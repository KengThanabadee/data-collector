"""Fixed USDⓈ-M minute universe, IP weight admission, and native metadata history."""
import asyncio
from decimal import Decimal
import gzip
import json
from pathlib import Path

from option_collector.common import body_bytes, dumps, sha, utc, write_bytes
from option_collector.transport import WEIGHT_HEADER

from .minute_reference import ReferenceAttempts, read_reference, reference_json


BASE_URLS = {"production": "https://fapi.binance.com/", "testnet": "https://demo-fapi.binance.com/"}
DEFAULT_UNIVERSE_PATH = Path(__file__).with_name("binance-top-100.json")
DEFAULT_IP_LIMIT = 2400
DEFAULT_BOT_RESERVE = 300
DEFAULT_RATE = 25
DEPTH = "fapi/v1/depth"
OPEN_INTEREST = "fapi/v1/openInterest"
EXCHANGE_INFO = "fapi/v1/exchangeInfo"
WEIGHTS = {DEPTH: 2, OPEN_INTEREST: 1, EXCHANGE_INFO: 1}
REFERENCE_ID = "binance:exchange-info"
HOUR = 3600


def read_default_binance_universe():
    """Read the ranked build-time universe and its endpoint provenance."""
    return json.loads(DEFAULT_UNIVERSE_PATH.read_bytes())


def binance_requests(symbols):
    return [
        request for symbol in symbols
        for request in (
            (f"binance:open-interest:{symbol}", OPEN_INTEREST, {"symbol": symbol}),
            (f"binance:depth:{symbol}", DEPTH, {"symbol": symbol, "limit": 20}),
        )
    ]


class _WeightGuard:
    """One unit-weight probe, followed by the remote high-water mark and local reservations."""

    def __init__(self, ip_limit, bot_reserve):
        self.ip_limit, self.bot_reserve = ip_limit, bot_reserve
        self.ceiling = ip_limit - bot_reserve
        self.max_observed = None
        self.pending, self.unconfirmed = 0, 0
        self.probe_finished = False
        self.halted = False
        self.skipped = {}

    def blocked(self, logical_id, method, *, include_pending=False):
        if self.halted:
            reason = "rate_limited"
        elif self.probe_finished and self.max_observed is None:
            reason = "unobserved"
        else:
            used = (self.max_observed or 0) + self.unconfirmed
            if include_pending:
                used += self.pending
            reason = "budget" if used + WEIGHTS[method] > self.ceiling else None
        if reason is not None:
            self.skipped[logical_id] = reason
        return reason is not None

    def reserve(self, logical_id, method):
        if self.blocked(logical_id, method, include_pending=True):
            return False
        self.pending += WEIGHTS[method]
        return True

    def observe(self, method, response):
        weight = WEIGHTS[method]
        self.pending -= weight
        raw = response["response_headers"].get(WEIGHT_HEADER, "")
        if raw.isascii() and raw.isdigit() and len(raw) <= 10 and int(raw) >= weight:
            self.max_observed = max(self.max_observed or 0, int(raw))
        else:
            # A lost reply may still have consumed its reserved weight.
            self.unconfirmed += weight
        if response["http_status"] in (418, 429):
            self.halted = True

    def report(self):
        return dict(ip_limit=self.ip_limit, bot_reserve=self.bot_reserve,
                    max_observed=self.max_observed,
                    skipped={key: reason for key, reason in self.skipped.items() if key != REFERENCE_ID})


def _metadata_digest(response):
    value = json.loads(body_bytes(response), parse_float=Decimal)
    value.pop("serverTime", None)  # Venue documentation directs users to the dedicated time endpoint.
    value["symbols"].sort(key=lambda row: row["symbol"])
    assets = value.get("assets")
    if isinstance(assets, list) and all(isinstance(row, dict) for row in assets):
        assets.sort(key=lambda row: str(row.get("asset", "")))
    return sha(reference_json(value) + b"\n")


def read_binance_reference(root, slot, *, failed=False):
    """Read one exchangeInfo revision or failed check with its original response."""
    if type(slot) is not int or slot % 60:
        raise ValueError("expected a UTC minute boundary")
    directory = "reference-failures" if failed else "reference"
    return read_reference(Path(root) / "binance" / directory / f"{slot:012d}.json.gz")


class BinanceMinute:
    def __init__(self, config, client, symbols, *, ip_limit, bot_reserve):
        self.client = client
        self.environment = config.environment
        self.root = config.data_root / "binance"
        self.universe = read_default_binance_universe() if symbols is None else {"symbols": list(symbols)}
        self.symbols = self.universe["symbols"]
        if (not self.symbols or any(not isinstance(symbol, str) or not 2 <= len(symbol) <= 40
                                    or not symbol.isalnum() or symbol != symbol.upper() for symbol in self.symbols)
                or len(set(self.symbols)) != len(self.symbols)):
            raise ValueError("Binance symbols must be a nonempty, unique list of uppercase native symbols")
        if type(ip_limit) is not int or type(bot_reserve) is not int or not 0 <= bot_reserve < ip_limit:
            raise ValueError("Binance weight limit must exceed the nonnegative bot reserve")
        self.ip_limit, self.bot_reserve = ip_limit, bot_reserve
        latest = max((self.root / "reference").glob("*.json.gz"), default=None)
        self.head = _metadata_digest(read_reference(latest)["responses"][-1]) if latest else None
        self.checked_hour = None

    async def capture(self, archive, slot, deadline, send_window):
        guard = _WeightGuard(self.ip_limit, self.bot_reserve)
        first, *rest = binance_requests(self.symbols)

        async def fetch(request):
            return await self.client.fetch(archive, *request, deadline, send_window=send_window, weight_guard=guard)

        await fetch(first)
        guard.probe_finished = True
        async with asyncio.TaskGroup() as tasks:
            for request in rest:
                tasks.create_task(fetch(request))
        if self.checked_hour != slot // HOUR:
            await self._refresh(slot, deadline, send_window, guard)
        archive.update_context(binance_weight=guard.report())

    async def _refresh(self, slot, deadline, send_window, guard):
        archive = ReferenceAttempts(self.environment, "binance", slot)
        record = await self.client.fetch(archive, REFERENCE_ID, EXCHANGE_INFO, {}, deadline,
                                         send_window=send_window, weight_guard=guard)
        if record is None or not record["ok"]:
            status = "missed" if record is None else (
                "timeout" if record["transport_error"] == "TimeoutError" else "error"
            )
            self._publish("reference-failures", slot, archive, status,
                          skip_reason=guard.skipped.get(REFERENCE_ID))
            return
        digest = _metadata_digest(record)
        if digest != self.head:
            self._publish("reference", slot, archive, "ok", content_sha256=digest)
            self.head = digest
        self.checked_hour = slot // HOUR

    def _publish(self, directory, slot, archive, status, **fields):
        entry = dict(schema_version=1, captured_slot_utc=utc(slot), status=status,
                     responses=archive.records, **fields)
        path = self.root / directory / f"{slot:012d}.json.gz"
        write_bytes(path, gzip.compress(dumps(entry), compresslevel=6, mtime=0))
