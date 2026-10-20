"""Bounded public Deribit feasibility experiment using only Python stdlib.

No authentication, orders, private endpoints, retries, or recurring services.
Stops on first throttle or network failure; does not discover a maximum API limit.
"""
import argparse
from collections import Counter
from concurrent.futures import FIRST_COMPLETED, ThreadPoolExecutor, wait
from datetime import datetime, timezone
import http.client
import json
import math
from pathlib import Path
import platform
import ssl
import threading
import time
from urllib.parse import urlencode

from archive_probe import Journal, atomic_json, decoded_body, digest, encoded_body, finalize, verify

HOST = "www.deribit.com"
CURRENCIES = ("BTC", "ETH")
VISIBLE = {"open", "settlement", "locked", "halted"}


def utc():
    return datetime.now(timezone.utc).isoformat(timespec="milliseconds")


def distribution(values):
    values = sorted(values)
    if not values:
        return {"n": 0}
    return dict(n=len(values), minimum=values[0], median=values[len(values) // 2],
                p95=values[min(len(values) - 1, math.ceil(len(values) * .95) - 1)], maximum=values[-1])


class Gate:
    def __init__(self, rate):
        self.rate = rate
        self.lock = threading.Lock()
        self.next_request = 0.0
        self.next_discovery = 0.0
        self.abort = threading.Event()

    def acquire(self, discovery=False, deadline=None):
        while not self.abort.is_set():
            with self.lock:
                now = time.monotonic()
                earliest = max(self.next_request, self.next_discovery if discovery else 0)
                delay = earliest - now
                if deadline and now + max(delay, 0) >= deadline:
                    return False
                if delay <= 0:
                    self.next_request = now + 1 / self.rate
                    if discovery:
                        self.next_discovery = now + 1.05
                    return True
            self.abort.wait(min(max(delay, .001), .2))
        return False


class Client:
    def __init__(self, gate):
        self.gate = gate
        self.local = threading.local()
        self.context = ssl.create_default_context()

    def get(self, method, params, currency=None, deadline=None):
        if not self.gate.acquire(method == "public/get_instruments", deadline):
            return None
        started = time.monotonic()
        sent_epoch = time.time()
        record = {"schema_version": 1, "method": method, "params": params,
                  "currency": currency, "request_started_utc": utc(),
                  "request_started_epoch_ms": sent_epoch * 1000, "attempt": 1}
        body = b""
        try:
            timeout = min(15, max(.1, deadline - started)) if deadline else 15
            conn = getattr(self.local, "connection", None)
            if conn is None:
                conn = http.client.HTTPSConnection(HOST, timeout=timeout, context=self.context)
                self.local.connection = conn
                self.local.uses = 0
            conn.timeout = timeout
            if conn.sock:
                conn.sock.settimeout(timeout)
            record["reused_connection"] = bool(conn.sock and self.local.uses)
            query = urlencode({key: str(value).lower() if isinstance(value, bool) else value for key, value in params.items()})
            conn.request("GET", "/api/v2/" + method + "?" + query,
                         headers={"User-Agent": "option-collector-feasibility/0.1", "Accept-Encoding": "identity", "Accept": "application/json"})
            response = conn.getresponse()
            record["http_status"] = response.status
            record["throttled"] = response.status == 429
            record["response_headers"] = {key.lower(): value for key, value in response.getheaders() if key.lower() in ("date", "content-type", "content-encoding", "retry-after")}
            body = response.read(16 * 1024 * 1024 + 1)
            if len(body) > 16 * 1024 * 1024:
                raise ValueError("response exceeds experiment 16MiB cap")
            self.local.uses += 1
            payload = json.loads(body)
            error = payload.get("error")
            record["api_error"] = error
            record["testnet"] = payload.get("testnet")
            record["ok"] = response.status == 200 and error is None and "result" in payload and payload.get("testnet") is not True
            if method == "public/get_order_book" and record["ok"]:
                result = payload["result"]
                record["ok"] = isinstance(result, dict) and result.get("instrument_name") == params["instrument_name"] and isinstance(result.get("timestamp"), int) and isinstance(result.get("bids"), list) and isinstance(result.get("asks"), list)
                if not record["ok"]:
                    record["validation_error"] = "book identity/timestamp/arrays invalid"
            record["throttled"] = response.status == 429 or isinstance(error, dict) and error.get("code") == 10028
            if not record["ok"]:
                self.gate.abort.set()
            if response.will_close:
                conn.close()
                self.local.connection = None
        except Exception as exc:
            record.update(ok=False, transport_error=type(exc).__name__ + ": " + str(exc), throttled=record.get("throttled", False))
            conn = getattr(self.local, "connection", None)
            if conn:
                conn.close()
            self.local.connection = None
            self.gate.abort.set()
        received_epoch = time.time()
        record.update(response_received_utc=utc(), response_received_epoch_ms=received_epoch * 1000,
                      latency_ms=(time.monotonic() - started) * 1000,
                      response_bytes=len(body), body_sha256=digest(body), **encoded_body(body))
        return record


def payload(record):
    return json.loads(decoded_body(record))


def probe_stats(records):
    result = {"requests": len(records), "successful_requests": sum(record.get("ok", False) for record in records),
              "throttles": sum(record.get("throttled", False) for record in records),
              "response_body_bytes": sum(record["response_bytes"] for record in records),
              "connection_reuse_requests": sum(record.get("reused_connection", False) for record in records),
              "latency_ms": distribution([record["latency_ms"] for record in records]),
              "errors": [{key: record.get(key) for key in ("currency", "method", "params", "http_status", "api_error", "transport_error", "validation_error")} for record in records if not record.get("ok")]}
    result["markets"] = {}
    for currency in CURRENCIES:
        books = [record for record in records if record["currency"] == currency and record["method"] == "public/get_order_book" and record.get("ok")]
        values = [payload(record)["result"] for record in books]
        if not books:
            continue
        result["markets"][currency] = {
            "books": len(books), "unique_instruments": len({value["instrument_name"] for value in values}),
            "observation_span_seconds": (max(record["response_received_epoch_ms"] for record in books) - min(record["request_started_epoch_ms"] for record in books)) / 1000,
            "exchange_timestamp_span_seconds": (max(value["timestamp"] for value in values) - min(value["timestamp"] for value in values)) / 1000,
            "response_minus_exchange_ms": distribution([record["response_received_epoch_ms"] - value["timestamp"] for record, value in zip(books, values)]),
            "both_sides_empty": sum(not value["bids"] and not value["asks"] for value in values),
            "one_or_both_sides_empty": sum(not value["bids"] or not value["asks"] for value in values),
            "at_depth_cap": sum(len(value["bids"]) == record["params"]["depth"] or len(value["asks"]) == record["params"]["depth"] for record, value in zip(books, values)),
            "missing_or_null_mark_iv": sum(value.get("mark_iv") is None for value in values),
            "missing_or_null_bid_iv": sum(value.get("bid_iv") is None for value in values),
            "missing_or_null_ask_iv": sum(value.get("ask_iv") is None for value in values),
            "zero_bid_iv": sum(value.get("bid_iv") == 0 for value in values),
            "states": dict(Counter(value.get("state") for value in values)),
        }
    return result


class Experiment:
    def __init__(self, root):
        self.root = root
        self.gate = Gate(2)
        self.client = Client(self.gate)
        self.stages = []
        self.initial_universe = None

    def save_stage(self, path, journal, records, elapsed, extra=None):
        journal.close()
        fsync_seconds = journal.fsync_seconds
        started = time.monotonic()
        successful = sum(bool(record.get("ok")) for record in records)
        status = "captured" if records and successful == len(records) and (extra or {}).get("complete", True) else "partial" if successful else "failed"
        archive = finalize(path, status=status)
        summary = dict(probe_stats(records), elapsed_seconds=elapsed, fsync_seconds=fsync_seconds,
                       finalize_seconds=time.monotonic() - started, archive=archive, **(extra or {}))
        atomic_json(path / "summary.json", summary)
        self.stages.append({"name": path.name, **summary})
        atomic_json(self.root / "summary.json", {"created_utc": utc(), "stages": self.stages})
        printed_extra = {key: value for key, value in (extra or {}).items() if key not in ("universe", "missing")}
        print(json.dumps({"event": "stage_complete", "name": path.name, "elapsed_seconds": round(elapsed, 3), "requests": len(records), "success": summary["successful_requests"], "throttles": summary["throttles"], "archive_bytes": archive["archive_bytes"], **printed_extra}), flush=True)
        return summary

    def discovery(self, name, full_references):
        path = self.root / name
        journal = Journal(path, {"kind": "discovery", "started_utc": utc(), "full_references": full_references})
        records, universe = [], {}
        started = time.monotonic()
        for currency in CURRENCIES:
            for kind in (("option", "future") if full_references else ("option",)):
                record = self.client.get("public/get_instruments", {"currency": currency, "kind": kind, "expired": False, "extended": True}, currency)
                if record is None:
                    break
                journal.append(record)
                records.append(record)
                if not record.get("ok"):
                    break
                if kind == "option":
                    instruments = payload(record)["result"]
                    universe[currency] = sorted([value for value in instruments if value.get("state") in VISIBLE], key=lambda value: (value["expiration_timestamp"], value["strike"], value["option_type"]))
                    if not universe[currency]:
                        self.gate.abort.set()
            if full_references and not self.gate.abort.is_set():
                for method, params in (("public/get_book_summary_by_currency", {"currency": currency, "kind": "option"}), ("public/get_book_summary_by_currency", {"currency": currency, "kind": "future"}), ("public/get_index_price", {"index_name": currency.lower() + "_usd"})):
                    record = self.client.get(method, params, currency)
                    if record is None:
                        break
                    journal.append(record)
                    records.append(record)
                    if not record.get("ok"):
                        break
        self.save_stage(path, journal, records, time.monotonic() - started,
                        {"universe_counts": {currency: len(values) for currency, values in universe.items()},
                         "universe": {currency: [value["instrument_name"] for value in values] for currency, values in universe.items()}})
        return universe

    @staticmethod
    def names(universe, count=None, rotation=0):
        sides = {}
        for currency, instruments in universe.items():
            if count:
                n = min(len(instruments), math.ceil(count / 2))
                selected = [instruments[index * len(instruments) // n] for index in range(n)]
            else:
                selected = instruments[:]
            # Sorting by hash disperses maturities and moneyness without random
            # hidden state; deterministic rotations avoid a fixed late tail.
            names = sorted((value["instrument_name"] for value in selected), key=lambda value: digest(value.encode()))
            shift = rotation % len(names) if names else 0
            sides[currency] = names[shift:] + names[:shift]
        output = []
        for index in range(max(map(len, sides.values()), default=0)):
            for currency in CURRENCIES:
                if index < len(sides.get(currency, [])):
                    output.append((currency, sides[currency][index]))
        return output[:count] if count else output

    def books(self, name, universe, rate, count=None, deadline_seconds=240, pairs=None, rotation=0, depth=20):
        self.gate.rate = rate
        names = self.names(universe, count, rotation)
        jobs = [(currency, instrument, depth) for currency, instrument in names] if pairs is None else pairs
        path = self.root / name
        journal = Journal(path, {"kind": "books", "started_utc": utc(), "rate": rate, "workers": 8, "deadline_seconds": deadline_seconds, "expected_jobs": jobs})
        started = time.monotonic()
        deadline = started + deadline_seconds
        records = []
        jobs_iter = iter(jobs)
        next_progress = started + 10
        with ThreadPoolExecutor(max_workers=8) as executor:
            pending = set()
            def fill():
                while len(pending) < 8 and not self.gate.abort.is_set() and time.monotonic() < deadline:
                    try:
                        currency, instrument, depth = next(jobs_iter)
                    except StopIteration:
                        return
                    pending.add(executor.submit(self.client.get, "public/get_order_book", {"instrument_name": instrument, "depth": depth}, currency, deadline))
            fill()
            while pending:
                done, pending = wait(pending, timeout=1, return_when=FIRST_COMPLETED)
                for future in done:
                    record = future.result()
                    if record is not None:
                        journal.append(record)
                        records.append(record)
                if time.monotonic() >= next_progress:
                    print(json.dumps({"event": "progress", "stage": name, "elapsed_seconds": round(time.monotonic() - started, 1), "received": len(records), "expected": len(jobs)}), flush=True)
                    next_progress = time.monotonic() + 10
                fill()
        elapsed = time.monotonic() - started
        successful = {(record["currency"], record["params"]["instrument_name"], record["params"]["depth"]) for record in records if record.get("ok")}
        missing = [job for job in jobs if tuple(job) not in successful]
        return self.save_stage(path, journal, records, elapsed, {"rate_limit_rps": rate, "expected_jobs": len(jobs), "unique_instruments": len(set((currency, instrument) for currency, instrument, _ in jobs)), "complete": not missing and bool(jobs), "missing": missing})

    def delivery(self):
        self.gate.rate = 2
        path = self.root / "delivery-history"
        journal = Journal(path, {"kind": "delivery-pagination", "started_utc": utc(), "page_size": 1000})
        records, coverage = [], {}
        started = time.monotonic()
        for currency in CURRENCIES:
            rows, totals, offset = [], [], 0
            for _ in range(1000):
                record = self.client.get("public/get_delivery_prices", {"index_name": currency.lower() + "_usd", "offset": offset, "count": 1000}, currency)
                if record is None:
                    break
                journal.append(record)
                records.append(record)
                if not record.get("ok"):
                    break
                result = payload(record)["result"]
                page = result["data"]
                rows.extend(page)
                totals.append(result.get("records_total"))
                # The observed production API clamps a requested count=1000 to
                # 100 rows. Never infer exhaustion from a short page alone.
                offset += len(page)
                if not page or isinstance(result.get("records_total"), int) and offset >= result["records_total"]:
                    break
            dates = sorted(row["date"] for row in rows)
            coverage[currency] = {"records": len(rows), "unique_dates": len(set(dates)), "oldest": dates[0] if dates else None, "newest": dates[-1] if dates else None, "advertised_totals": totals, "complete": bool(totals) and len(rows) == totals[-1] and len(set(dates)) == len(rows)}
        return self.save_stage(path, journal, records, time.monotonic() - started, {"coverage": coverage, "complete": len(coverage) == 2 and all(value["complete"] for value in coverage.values())})


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--pilot-seconds", type=int, default=30)
    parser.add_argument("--full-rounds", type=int, default=2)
    parser.add_argument("--delivery-only", action="store_true")
    parser.add_argument("--depth", type=int, choices=(20, 10000), default=20)
    parser.add_argument("--skip-pilots", action="store_true", help="only after successful 10rps calibration on the same IP")
    parser.add_argument("--sweeps-only", action="store_true")
    args = parser.parse_args()
    if args.output.exists():
        raise SystemExit("output must be new; do not overwrite prior evidence")
    args.output.mkdir(parents=True)
    atomic_json(args.output / "environment.json", {"started_utc": utc(), "python": platform.python_version(), "platform": platform.platform(), "host": HOST, "authenticated": False, "workers": 8, "maximum_test_rate_rps": 10, "code_sha256": {path.name: digest(path.read_bytes()) for path in (Path(__file__), Path(__file__).with_name("archive_probe.py"))}, "note": "Response-body bytes exclude HTTP/TLS overhead; no retry or claim of official public allowance."})
    source = args.output / "source"
    source.mkdir()
    for path in (Path(__file__), Path(__file__).with_name("archive_probe.py")):
        (source / path.name).write_bytes(path.read_bytes())
    experiment = Experiment(args.output)
    if args.delivery_only:
        experiment.delivery()
        return
    universe = experiment.discovery("initial-discovery", True)
    if experiment.gate.abort.is_set():
        raise SystemExit("initial API probe failed; inspect saved raw evidence")
    highest = 10 if args.skip_pilots else 0
    for rate in (() if args.skip_pilots else (2, 5, 8, 10)):
        summary = experiment.books("pilot-{}rps".format(rate), universe, rate, count=rate * args.pilot_seconds, deadline_seconds=args.pilot_seconds + 30)
        if experiment.gate.abort.is_set() or not summary["complete"]:
            break
        highest = rate
    if highest and not experiment.gate.abort.is_set():
        for index in range(args.full_rounds):
            universe = experiment.discovery("round-{}-discovery".format(index + 1), True)
            if experiment.gate.abort.is_set():
                break
            summary = experiment.books("round-{}-full".format(index + 1), universe, highest, rotation=index * 193, depth=args.depth)
            if experiment.gate.abort.is_set() or not summary["complete"]:
                break
    if not experiment.gate.abort.is_set() and not args.sweeps_only:
        # Enrich the depth sample with books that reached the cap, plus dispersed
        # names. Pair requests are nearby in time, never simultaneous.
        selected = experiment.names(universe, count=12)
        full_stages = [stage for stage in experiment.stages if stage["name"].endswith("-full")]
        if full_stages:
            _, records = verify(args.output / full_stages[-1]["name"])
            for currency in CURRENCIES:
                candidates = [record for record in records if record.get("ok") and record["currency"] == currency]
                candidates.sort(key=lambda record: max(len(payload(record)["result"]["bids"]), len(payload(record)["result"]["asks"])), reverse=True)
                selected.extend((currency, record["params"]["instrument_name"]) for record in candidates[:6])
        selected = list(dict.fromkeys(selected))
        pairs = [(currency, instrument, depth) for currency, instrument in selected for depth in (20, 10000)]
        experiment.books("depth-comparison", universe, min(highest or 2, 5), pairs=pairs)
    if not experiment.gate.abort.is_set() and not args.sweeps_only:
        experiment.delivery()
    atomic_json(args.output / "finished.json", {"finished_utc": utc(), "aborted_after_error": experiment.gate.abort.is_set(), "highest_successful_pilot_rps": highest, "stages": len(experiment.stages)})


if __name__ == "__main__":
    main()
