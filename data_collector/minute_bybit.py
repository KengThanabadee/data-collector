"""Bybit minute requests, hourly depth selections and native reference history."""
import asyncio
from decimal import Decimal, InvalidOperation
import gzip
import json
from pathlib import Path
from statistics import median

from option_collector.common import body_bytes, dumps, read_json, sha, utc, write_bytes, write_json


BASES = ("BTC", "ETH")
DEFAULT_COUNTS = (50, 50)
DEFAULT_EXPIRIES = 2
HOUR = 3600
DAY = 86400
DELIVERY_OFFSET = 8 * HOUR + 120
BASE_URLS = {"production": "https://api.bytick.com/", "testnet": "https://api-testnet.bybit.com/"}
TICKERS = "v5/market/tickers"
INSTRUMENTS = "v5/market/instruments-info"
RISK = "v5/market/risk-limit"
DELIVERY = "v5/market/new-delivery-price"
DEPTH = "v5/market/orderbook"
REFERENCE_NAMES = {
    *(f"{feed}-{base}" for feed in ("options", "perp", "risk") for base in BASES),
    *(f"settlement-{base}-{coin}" for base in BASES for coin in ("USDT", "USDC")),
}


def _selection_path(root, slot, base):
    return root / "selection" / f"{slot // HOUR * HOUR}-{base}.json"


def read_bybit_selection(root, slot, base):
    """Return the immutable hourly depth selection available at this minute."""
    if base not in BASES:
        raise ValueError("invalid Bybit selection base")
    path = _selection_path(Path(root) / "bybit", slot, base)
    saved = read_json(path)
    if saved is None or saved["selected_slot_epoch"] > slot:
        raise FileNotFoundError(path)
    return saved


def read_bybit_reference(root, name, slot, *, failed=False):
    """Return a Bybit reference revision or a failed reference check."""
    if name not in REFERENCE_NAMES or type(slot) is not int or slot % 60:
        raise ValueError("invalid Bybit reference name or slot")
    family = "reference-failures" if failed else "reference"
    return _read_reference(Path(root) / "bybit" / family / name / f"{slot:012d}.json.gz")


def bybit_requests(selection):
    return [
        (f"bybit:option-tickers:{base}", TICKERS, {"category": "option", "baseCoin": base})
        for base in BASES
    ] + [
        (f"bybit:perp-ticker:{base}USDT", TICKERS, {"category": "linear", "symbol": f"{base}USDT"})
        for base in BASES
    ] + [
        (f"bybit:depth:{symbol}", DEPTH, {"category": "option", "symbol": symbol, "limit": 25})
        for base in BASES for symbol in selection.get(base, ())
    ]


def _result(record):
    return json.loads(body_bytes(record), parse_float=str)["result"]


def _price(value):
    try:
        price = Decimal(value)
    except (InvalidOperation, TypeError, ValueError):
        return None
    return price if price.is_finite() and price > 0 else None


def _rank(rows, ticker, slot, limit, expiry_count):
    prices = [_price(row.get("indexPrice")) for row in _result(ticker)["list"]]
    prices = [price for price in prices if price is not None]
    if not prices:
        return None
    index = median(prices)
    candidates = []
    for row in rows:
        expiry = row.get("deliveryTime")
        strike = _price(row["symbol"].split("-")[2]) if row["symbol"].count("-") >= 3 else None
        if (row.get("status") == "Trading" and isinstance(expiry, str) and expiry.isascii()
                and expiry.isdigit() and int(expiry) > slot * 1000 and strike is not None):
            candidates.append((abs(strike - index), int(expiry), row["symbol"]))
    expiries = sorted({expiry for _, expiry, _ in candidates})[:expiry_count]
    symbols = [symbol for _, expiry, symbol in sorted(candidates) if expiry in expiries][:limit]
    return dict(index_price=str(index), expiries_epoch_ms=expiries, symbols=symbols)


class _ReferenceAttempts:
    def __init__(self, config, slot):
        self.context = dict(schema_version=1, run_id=f"bybit-reference-{slot}", job_kind="minute",
                            environment=config.environment, currency="bybit", scheduled_slot_utc=utc(slot))
        self.records = []

    def append(self, record):
        self.records.append(record)
        return record


def _read_reference(path):
    record = json.loads(gzip.decompress(path.read_bytes()))
    for attempt in record["responses"]:
        body = body_bytes(attempt)
        if ((sha(body) if body is not None else None) != attempt["body_sha256"]
                or len(body or b"") != attempt["body_byte_count"]):
            raise ValueError("Bybit reference response checksum differs")
    return record


class BybitMinute:
    def __init__(self, config, client, *, counts=DEFAULT_COUNTS, expiries=DEFAULT_EXPIRIES):
        if (len(counts) != len(BASES) or any(type(count) is not int or count < 0 for count in counts)
                or type(expiries) is not int or expiries < 1):
            raise ValueError("invalid Bybit depth selection limits")
        self.config, self.client = config, client
        self.counts, self.expiries = dict(zip(BASES, counts)), expiries
        self.root = config.data_root / "bybit"
        self.reference_heads = {}
        self.checked = {}

    def selection(self, slot):
        result = {}
        for base in BASES:
            saved = read_json(_selection_path(self.root, slot, base))
            if saved is not None and saved["selected_slot_epoch"] <= slot:
                result[base] = saved["symbols"]
        return result

    def _failed_reference(self, name, slot, archive, status, previous):
        path = self.root / "reference-failures" / name / f"{slot:012d}.json.gz"
        failure = dict(schema_version=1, captured_slot_utc=utc(slot), status=status, responses=archive.records)
        write_bytes(path, gzip.compress(dumps(failure), compresslevel=6, mtime=0))
        return previous

    async def _refresh(self, name, method, params, slot, deadline, *, daily=False):
        period = slot // (DAY if daily else HOUR)
        directory = self.root / "reference" / name
        if name not in self.reference_heads:
            latest = max(directory.glob("*.json.gz"), default=None)
            self.reference_heads[name] = _read_reference(latest) if latest else None
        previous = self.reference_heads[name]
        if self.checked.get(name) == period or daily and previous and previous["period"] == period:
            return previous

        archive = _ReferenceAttempts(self.config, slot)
        rows, cursors = [], set()
        for page in range(100):
            record = await self.client.fetch(archive, f"bybit:{name}:{page}", method, params, deadline)
            if record is None:
                return self._failed_reference(name, slot, archive, "missed", previous)
            if not record["ok"]:
                status = "timeout" if record["transport_error"] == "TimeoutError" else "error"
                return self._failed_reference(name, slot, archive, status, previous)
            result = _result(record)
            rows.extend(result["list"])
            cursor = result.get("nextPageCursor") or ""
            if not cursor:
                break
            if daily or not isinstance(cursor, str) or cursor in cursors:
                return self._failed_reference(name, slot, archive, "error", previous)
            cursors.add(cursor)
            params = dict(params, cursor=cursor)
        else:
            return self._failed_reference(name, slot, archive, "error", previous)

        if method == INSTRUMENTS and len({row["symbol"] for row in rows}) != len(rows):
            return self._failed_reference(name, slot, archive, "error", previous)
        digest = sha(dumps(sorted(dumps(row).decode() for row in rows)))
        if daily or previous is None or previous["content_sha256"] != digest:
            version = dict(schema_version=1, period=period, captured_slot_utc=utc(slot), status="ok",
                           content_sha256=digest, responses=archive.records)
            path = directory / f"{slot:012d}.json.gz"
            write_bytes(path, gzip.compress(dumps(version), compresslevel=6, mtime=0))
            self.reference_heads[name] = version
        self.checked[name] = period
        return self.reference_heads[name]

    async def capture(self, archive, slot, deadline, send_window, deribit_jobs, *, following_jobs=()):
        selection = self.selection(slot)
        base_tasks = {}
        # Place grid requests in the venue quota queue before hourly reference reads.
        async with asyncio.TaskGroup() as group:
            for logical_id, method, params in bybit_requests({}):
                base_tasks[logical_id] = group.create_task(
                    self.client.fetch(archive, logical_id, method, params, deadline, send_window=send_window)
                )
            options = {
                base: group.create_task(self._refresh(
                    f"options-{base}", INSTRUMENTS,
                    {"category": "option", "baseCoin": base, "limit": 1000}, slot, deadline,
                ))
                for base in BASES
            }
            for base in BASES:
                params = {"category": "linear", "symbol": f"{base}USDT"}
                group.create_task(self._refresh(f"perp-{base}", INSTRUMENTS, params, slot, deadline))
                group.create_task(self._refresh(f"risk-{base}", RISK, params, slot, deadline))
                if slot % DAY >= DELIVERY_OFFSET:
                    for coin in ("USDT", "USDC"):
                        group.create_task(self._refresh(
                            f"settlement-{base}-{coin}", DELIVERY,
                            {"category": "option", "baseCoin": base, "settleCoin": coin}, slot, deadline, daily=True,
                        ))

            # Freeze the selection in the minute's durable context before admitting depth.
            for base in BASES:
                if base in selection:
                    continue
                ticker = await base_tasks[f"bybit:option-tickers:{base}"]
                metadata = await options[base]
                if ticker is None or not ticker["ok"] or metadata is None:
                    continue
                rows = [row for page in metadata["responses"] if page["ok"] for row in _result(page)["list"]]
                ranked = _rank(rows, ticker, slot, self.counts[base], self.expiries)
                if ranked is None:
                    continue
                saved = dict(schema_version=1, base=base, selected_slot_epoch=slot, limit=self.counts[base],
                             expiry_count=self.expiries, ticker_body_sha256=ticker["body_sha256"],
                             metadata_sha256=metadata["content_sha256"], **ranked)
                write_json(_selection_path(self.root, slot, base), saved)
                selection[base] = ranked["symbols"]

            jobs = bybit_requests(selection)
            if archive.context["bybit_selection"] != selection:
                archive.update_context(bybit_selection=selection,
                                       required_jobs=[*deribit_jobs, *(job[0] for job in jobs), *following_jobs])
            for logical_id, method, params in jobs:
                if method == DEPTH:
                    group.create_task(self.client.fetch(archive, logical_id, method, params, deadline,
                                                        send_window=send_window))
