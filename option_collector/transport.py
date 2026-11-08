"""Bounded async HTTP with lossless bodies and one paced quota."""
import asyncio
from email.utils import parsedate_to_datetime
import json
import random
import time
import aiohttp

from .common import body_fields, read_json, utc, write_json
from .schema import validate, quality

ALLOWED = {"public/" + x for x in ("get_instruments", "get_book_summary_by_currency", "get_index_price", "get_order_book", "get_delivery_prices")}
MINUTE_ALLOWED = ALLOWED | {"public/ticker"}


class Gate:
    def __init__(self, config, *, clock=time.monotonic, wall=time.time, sleep=asyncio.sleep):
        self.config, self.clock, self.wall, self.sleep = config, clock, wall, sleep
        self.lock = asyncio.Lock()
        self.next_request = self.next_discovery = 0.0
        self.path = config.data_root / "state" / "cooldown.json"
        state = read_json(self.path, {})
        self.cooldown = self.clock() + max(0, state.get("until_epoch", 0) - self.wall())

    async def acquire(self, discovery, deadline):
        async with self.lock:
            while True:
                earliest = max(self.next_request, self.next_discovery if discovery else 0, self.cooldown, self.clock())
                if earliest >= deadline:
                    raise TimeoutError("quota wait exceeds deadline")
                if earliest <= self.clock():
                    break
                await self.sleep(earliest - self.clock())
            now = self.clock()
            self.next_request = now + 1/self.config.rate
            if discovery:
                self.next_discovery = now + 1/self.config.discovery_rate

    def throttle(self, seconds):
        self.cooldown = max(self.cooldown, self.clock() + seconds)
        write_json(self.path, {"until_epoch": self.wall() + max(0, self.cooldown-self.clock())}, replace=True)


def retry_after(value, now):
    try:
        return max(0, float(value))
    except (TypeError, ValueError):
        try:
            return max(0, parsedate_to_datetime(value).timestamp() - now)
        except (TypeError, ValueError, OverflowError):
            return 0


class Client:
    def __init__(self, config, gate=None, *, clock=time.monotonic, wall=time.time, sleep=asyncio.sleep):
        self.config = config
        self.clock, self.wall, self.sleep = clock, wall, sleep
        self.gate = gate or Gate(config, clock=clock, wall=wall, sleep=sleep)
        self.semaphore = asyncio.Semaphore(config.workers)

    async def __aenter__(self):
        self.session = aiohttp.ClientSession(auto_decompress=False, trust_env=False,
            connector=aiohttp.TCPConnector(limit=self.config.workers), cookie_jar=aiohttp.DummyCookieJar(),
            headers={"Accept-Encoding": "identity", "Accept": "application/json", "User-Agent": "option-collector/0.1"})
        return self

    async def __aexit__(self, *args):
        await self.session.close()

    async def fetch(self, archive, logical_id, method, params, deadline, *, send_window=None):
        minute = archive.context["job_kind"] == "minute"
        if method not in (MINUTE_ALLOWED if minute else ALLOWED):
            raise ValueError("method not allowed")
        async with self.semaphore:
            previous=[r for r in archive.records if r["logical_request_id"]==logical_id]
            if previous and (previous[-1]["ok"] or len(previous)>=self.config.attempts):
                return previous[-1]
            for attempt in range(len(previous)+1, self.config.attempts + 1):
                if self.clock() >= deadline:
                    return None
                try:
                    await self.gate.acquire(method == "public/get_instruments", deadline)
                except TimeoutError:
                    return None
                start, sent = self.clock(), self.wall()
                if start >= deadline or (send_window is not None and not send_window[0] <= sent < send_window[1]):
                    return None
                rec = {k: archive.context[k] for k in ("schema_version", "run_id", "job_kind", "environment", "currency", "scheduled_slot_utc")}
                rec.update(logical_request_id=logical_id, attempt=attempt, method=method, parameters=params,
                           effective_url=self.config.base_url + method, instrument_name=params.get("instrument_name"),
                           request_started_utc=utc(sent), request_started_epoch_ms=sent*1000,
                           http_status=None, response_headers={}, api_error=None, transport_error=None,
                           validation_error=None, body_complete=False, ok=False, quality_flags=[])
                if minute:
                    rec["venue_server_epoch_us"] = None
                body, received, retryable, cancelled = None, None, False, False
                try:
                    async with asyncio.timeout(min(self.config.attempt_seconds, deadline-start)):
                        async with self.session.get(self.config.base_url + method, params={k: str(v).lower() if isinstance(v, bool) else v for k, v in params.items()}, allow_redirects=False) as response:
                            rec["http_status"] = response.status
                            rec["response_headers"] = {k.lower(): v for k,v in response.headers.items() if k.lower() in ("date", "content-type", "content-encoding", "retry-after")}
                            body = b""
                            if minute:
                                received = self.wall()
                            async for chunk in response.content.iter_chunked(65536):
                                if minute:
                                    received = self.wall()
                                remaining = self.config.body_cap-len(body)
                                if len(chunk) > remaining:
                                    body += chunk[:remaining]
                                    raise ValueError("body_cap")
                                body += chunk
                            rec["body_complete"] = True
                    retryable = rec["http_status"] == 429 or rec["http_status"] >= 500
                    if rec["response_headers"].get("content-encoding", "identity") != "identity":
                        raise ValueError("unexpected content encoding; raw bytes retained")
                    value = json.loads(body, parse_constant=lambda x: (_ for _ in ()).throw(ValueError("nonfinite JSON")))
                    if not isinstance(value, dict):
                        raise ValueError("invalid JSON-RPC response")
                    if minute and type(value.get("usOut")) is int:
                        rec["venue_server_epoch_us"] = value["usOut"]
                    rec["api_error"] = value.get("error")
                    throttled = rec["http_status"] == 429 or isinstance(rec["api_error"], dict) and rec["api_error"].get("code") == 10028
                    retryable |= throttled
                    if value.get("testnet") is not None and value["testnet"] != (self.config.environment == "testnet"):
                        raise ValueError("environment mismatch")
                    if rec["http_status"] == 200 and rec["api_error"] is None and "result" in value:
                        validate(method, params, value["result"])
                        rec["ok"] = True
                        if method == "public/get_order_book":
                            rec["quality_flags"] = quality(value["result"], params["depth"])
                except asyncio.CancelledError:
                    rec["transport_error"], cancelled = "cancelled", True
                except (TimeoutError, aiohttp.ClientError) as exc:
                    # Preserve the v1 error name. Minute indexes group timeout subclasses.
                    rec["transport_error"] = "TimeoutError" if minute and isinstance(exc, TimeoutError) else type(exc).__name__
                    retryable = not isinstance(exc, aiohttp.ClientConnectorCertificateError)
                except (ValueError, KeyError, TypeError) as exc:
                    rec["validation_error"] = type(exc).__name__ + ": " + str(exc)
                finished = self.wall()
                elapsed = self.clock() - start
                if minute:
                    received_ms = received * 1000 if received is not None else None
                else:
                    received = finished if body is not None else None
                    received_ms = finished * 1000
                rec.update(response_received_utc=utc(received) if received is not None else None,
                           response_received_epoch_ms=received_ms,
                           monotonic_elapsed_ms=elapsed*1000,
                           **body_fields(body))
                if abs((finished-sent)-elapsed) > 1:
                    rec["quality_flags"].append("clock_anomaly")
                rec["retry_reason"] = "retryable_response" if retryable else None
                record = archive.append(rec)
                if cancelled:
                    raise asyncio.CancelledError
                throttle = rec["http_status"] == 429 or isinstance(rec["api_error"],dict) and rec["api_error"].get("code") == 10028
                delay = max(2**(attempt-1)+random.uniform(0,.25), retry_after(rec["response_headers"].get("retry-after"), finished))
                if throttle:
                    self.gate.throttle(delay)
                if rec["ok"] or not retryable or attempt == self.config.attempts:
                    return record
                if self.clock()+delay >= deadline:
                    return record
                await self.sleep(delay)
        return None
