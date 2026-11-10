"""Bounded async HTTP with lossless bodies and one paced quota."""
import asyncio
from email.utils import parsedate_to_datetime
import json
import random
import re
import time
from urllib.parse import urlencode
import aiohttp
from yarl import URL

from .common import body_fields, read_json, utc, write_json
from .schema import validate, quality

ALLOWED = {"public/" + x for x in ("get_instruments", "get_book_summary_by_currency", "get_index_price", "get_order_book", "get_delivery_prices")}
MINUTE_ALLOWED = ALLOWED | {"public/ticker"}
BYBIT_ALLOWED = {"v5/market/" + name for name in (
    "tickers", "orderbook", "instruments-info", "risk-limit", "new-delivery-price",
)}


BINANCE_ALLOWED = {"fapi/v1/" + name for name in ("depth", "openInterest", "exchangeInfo")}
WEIGHT_HEADER = "x-mbx-used-weight-1m"


def valid_bybit_cursor(cursor):
    return (isinstance(cursor, str) and 0 < len(cursor) <= 4096
            and re.fullmatch(r"(?:[A-Za-z0-9._~!$'()*+,;=:@/-]|%[0-9A-Fa-f]{2})+", cursor) is not None)


def _binance_result(method, params, value):
    if method == "fapi/v1/depth":
        if (type(value.get("lastUpdateId")) is not int
                or ("symbol" in value and value["symbol"] != params["symbol"])):
            raise ValueError("Binance depth identity differs")
        for side in ("bids", "asks"):
            levels = value.get(side)
            if (not isinstance(levels, list) or len(levels) > params["limit"]
                    or any(not isinstance(level, list) or len(level) != 2
                           or not all(isinstance(item, str) for item in level) for level in levels)):
                raise ValueError("invalid Binance depth levels")
    elif method == "fapi/v1/openInterest":
        if value.get("symbol") != params["symbol"] or not isinstance(value.get("openInterest"), str):
            raise ValueError("Binance open interest identity or shape differs")
    else:
        symbols = value.get("symbols")
        if (not isinstance(symbols, list) or not symbols
                or any(not isinstance(row, dict) or not isinstance(row.get("symbol"), str) for row in symbols)):
            raise ValueError("invalid Binance exchange information")


def _bybit_result(method, params, result):
    if not isinstance(result, dict):
        raise ValueError("invalid Bybit result")
    if method == "v5/market/orderbook":
        if (result.get("s") != params["symbol"] or type(result.get("ts")) is not int
                or any(not isinstance(result.get(side), list) for side in ("b", "a"))):
            raise ValueError("Bybit order book identity or shape differs")
        return
    rows = result.get("list")
    if result.get("category") != params["category"] or not isinstance(rows, list):
        raise ValueError("Bybit response category or shape differs")
    if method != "v5/market/new-delivery-price" and not rows:
        raise ValueError("empty Bybit response")
    for row in rows:
        if not isinstance(row, dict):
            raise ValueError("invalid Bybit response row")
        symbol = row.get("symbol")
        if ("symbol" in params and symbol != params["symbol"]
                or "baseCoin" in params and method != "v5/market/new-delivery-price"
                and (not isinstance(symbol, str) or not symbol.startswith(params["baseCoin"] + "-"))):
            raise ValueError("Bybit response symbol differs")


def _rate_limited(record):
    error = record["api_error"]
    return (record["http_status"] == 429 or
            record.get("venue") == "binance" and record["http_status"] == 418) or isinstance(error, dict) and (
        error.get("code") == 10028 or error.get("retCode") in (10006, 10429)
    )


class Gate:
    def __init__(self, config, *, clock=time.monotonic, wall=time.time, sleep=asyncio.sleep, venue="deribit"):
        self.config, self.clock, self.wall, self.sleep = config, clock, wall, sleep
        self.lock = asyncio.Lock()
        self.next_request = self.next_discovery = 0.0
        filename = "cooldown.json" if venue == "deribit" else f"cooldown-{venue}.json"
        self.path = config.data_root / "state" / filename
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
    def __init__(self, config, gate=None, *, clock=time.monotonic, wall=time.time, sleep=asyncio.sleep,
                 bybit_base_url=None, binance_base_url=None):
        self.config = config
        self.clock, self.wall, self.sleep = clock, wall, sleep
        if binance_base_url is not None:
            self.venue, self.base_url = "binance", binance_base_url
        elif bybit_base_url is not None:
            self.venue, self.base_url = "bybit", bybit_base_url
        else:
            self.venue, self.base_url = "deribit", config.base_url
        self.gate = gate or Gate(config, clock=clock, wall=wall, sleep=sleep, venue=self.venue)
        self.semaphore = asyncio.Semaphore(config.workers)

    async def __aenter__(self):
        self.session = aiohttp.ClientSession(auto_decompress=False, trust_env=False,
            connector=aiohttp.TCPConnector(limit=self.config.workers), cookie_jar=aiohttp.DummyCookieJar(),
            headers={"Accept-Encoding": "identity", "Accept": "application/json", "User-Agent": "option-collector/0.1"})
        if self.venue == "binance":
            # aiohttp 3.14.5 otherwise retries some GETs without another admission.
            self.session._retry_connection = False
        return self

    async def __aexit__(self, *args):
        await self.session.close()

    async def fetch(self, archive, logical_id, method, params, deadline, *, send_window=None, weight_guard=None):
        minute = archive.context["job_kind"] == "minute"
        allowed = {
            "binance": BINANCE_ALLOWED if minute and weight_guard is not None else (),
            "bybit": BYBIT_ALLOWED if minute else (),
            "deribit": MINUTE_ALLOWED if minute else ALLOWED,
        }[self.venue]
        if method not in allowed:
            raise ValueError("method not allowed")
        url = self.base_url + method
        query = {k: str(v).lower() if isinstance(v, bool) else v for k, v in params.items()}
        if self.venue == "bybit" and "cursor" in query:
            cursor = query.pop("cursor")
            if not valid_bybit_cursor(cursor):
                raise ValueError("invalid native Bybit cursor")
            # The cursor is already escaped by the venue. Encode the other fields only.
            url = URL(f"{url}?{urlencode(query)}&cursor={cursor}", encoded=True)
            query = None
        async with self.semaphore:
            previous=[r for r in archive.records if r["logical_request_id"]==logical_id]
            if previous and (previous[-1]["ok"] or len(previous)>=self.config.attempts):
                return previous[-1]
            for attempt in range(len(previous)+1, self.config.attempts + 1):
                if weight_guard is not None and weight_guard.blocked(logical_id, method):
                    return None
                if self.clock() >= deadline:
                    return None
                try:
                    await self.gate.acquire(method == "public/get_instruments", deadline)
                except TimeoutError:
                    return None
                start, sent = self.clock(), self.wall()
                if start >= deadline or (send_window is not None and not send_window[0] <= sent < send_window[1]):
                    return None
                if weight_guard is not None and not weight_guard.reserve(logical_id, method):
                    return None
                rec = {k: archive.context[k] for k in ("schema_version", "run_id", "job_kind", "environment", "currency", "scheduled_slot_utc")}
                rec.update(logical_request_id=logical_id, attempt=attempt, method=method, parameters=params,
                           effective_url=self.base_url + method, instrument_name=params.get("instrument_name"),
                           request_started_utc=utc(sent), request_started_epoch_ms=sent*1000,
                           http_status=None, response_headers={}, api_error=None, transport_error=None,
                           validation_error=None, body_complete=False, ok=False, quality_flags=[])
                if self.venue == "binance":
                    rec.update(venue="binance", currency=None, instrument_name=params.get("symbol"),
                               venue_event_epoch_ms=None)
                elif self.venue == "bybit":
                    rec.update(venue="bybit", currency=params.get("baseCoin"), instrument_name=params.get("symbol"),
                               venue_server_epoch_ms=None)
                elif minute:
                    rec["venue_server_epoch_us"] = None
                body, received, retryable, cancelled = None, None, False, False
                try:
                    async with asyncio.timeout(min(self.config.attempt_seconds, deadline-start)):
                        async with self.session.get(url, params=query, allow_redirects=False) as response:
                            rec["http_status"] = response.status
                            rec["response_headers"] = {k.lower(): v for k,v in response.headers.items() if k.lower() in ("date", "content-type", "content-encoding", "retry-after", WEIGHT_HEADER)}
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
                        raise ValueError("invalid REST response object")
                    if self.venue == "binance":
                        timestamp = value.get("E" if method == "fapi/v1/depth" else "time")
                        if type(timestamp) is int:
                            rec["venue_event_epoch_ms"] = timestamp
                        if "code" in value:
                            rec["api_error"] = {key: value[key] for key in ("code", "msg") if key in value}
                        elif rec["http_status"] == 200:
                            _binance_result(method, params, value)
                            rec["ok"] = True
                    elif self.venue == "bybit":
                        if type(value.get("time")) is int:
                            rec["venue_server_epoch_ms"] = value["time"]
                        code = value.get("retCode")
                        if type(code) is not int:
                            raise ValueError("missing Bybit result code")
                        if code != 0:
                            rec["api_error"] = {key: value[key] for key in ("retCode", "retMsg") if key in value}
                        elif rec["http_status"] == 200:
                            _bybit_result(method, params, value.get("result"))
                            rec["ok"] = True
                    else:
                        if minute and type(value.get("usOut")) is int:
                            rec["venue_server_epoch_us"] = value["usOut"]
                        rec["api_error"] = value.get("error")
                        if value.get("testnet") is not None and value["testnet"] != (self.config.environment == "testnet"):
                            raise ValueError("environment mismatch")
                        if rec["http_status"] == 200 and rec["api_error"] is None and "result" in value:
                            validate(method, params, value["result"])
                            rec["ok"] = True
                            if method == "public/get_order_book":
                                rec["quality_flags"] = quality(value["result"], params["depth"])
                    retryable |= _rate_limited(rec)
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
                if weight_guard is not None:
                    weight_guard.observe(method, rec)
                record = archive.append(rec)
                if cancelled:
                    raise asyncio.CancelledError
                throttle = _rate_limited(rec)
                delay = max(2**(attempt-1)+random.uniform(0,.25), retry_after(rec["response_headers"].get("retry-after"), finished))
                if throttle:
                    self.gate.throttle(delay)
                if rec["ok"] or not retryable or attempt == self.config.attempts:
                    return record
                if self.clock()+delay >= deadline:
                    return record
                await self.sleep(delay)
        return None
