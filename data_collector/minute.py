"""UTC minute capture and verified readback for the Deribit REST profile."""
import asyncio
from contextlib import asynccontextmanager
from datetime import datetime, timezone
import gzip
import json
import math
from pathlib import Path
import time

import aiohttp

from option_collector.archive import Archive, read_archive
from option_collector.collector import context
from option_collector.common import dumps, process_lock, read_json, sha, utc, write_bytes, write_json
from option_collector.transport import Client

from .minute_tickers import TickerSampler
from .minute_bybit import DEFAULT_COUNTS, DEFAULT_EXPIRIES, BybitMinute, bybit_requests, read_bybit_selection


PERIOD = 60
START_GRACE = 1
TICKER_ARCHIVE = "deribit-tickers.json.gz"
CURRENCIES = ("BTC", "ETH")
INDEX_NAMES = ("btc_usd", "eth_usd", "usdt_usd", "btc_usdt", "eth_usdt")


def _requests():
    return [
        (f"deribit:futures:{currency}", "public/get_book_summary_by_currency",
         {"currency": currency, "kind": "future"})
        for currency in CURRENCIES
    ] + [
        (f"deribit:index:{name}", "public/get_index_price", {"index_name": name})
        for name in INDEX_NAMES
    ] + [
        (f"deribit:dvol:{currency}", "public/get_index_price",
         {"index_name": f"{currency.lower()}dvol_usdc"})
        for currency in CURRENCIES
    ] + [
        (f"deribit:ticker:{currency}-PERPETUAL", "public/ticker",
         {"instrument_name": f"{currency}-PERPETUAL"})
        for currency in CURRENCIES
    ]


def _destination(slot):
    if type(slot) is not int or slot % PERIOD:
        raise ValueError("expected a UTC minute boundary in Unix seconds")
    return Path("minutes") / datetime.fromtimestamp(slot, timezone.utc).strftime("%Y/%m/%d/%H%M")


def _plan(selection):
    return _requests() + (bybit_requests(selection) if selection is not None else [])


def _index(slot, records, *, reason=None, ticker_hash=None, selection=None):
    expected = {logical_id: (method, params) for logical_id, method, params in _plan(selection)}
    statuses = dict.fromkeys(expected, "missed")
    for record in records:
        logical_id = record["logical_request_id"]
        if (expected.get(logical_id) != (record["method"], record["parameters"])
                or record["scheduled_slot_utc"] != utc(slot)
                or not slot * 1000 <= record["request_started_epoch_ms"] < (slot + PERIOD) * 1000):
            raise ValueError("unexpected request in minute archive")
        if record["ok"]:
            statuses[logical_id] = "ok"
        elif record["transport_error"] == "TimeoutError":
            statuses[logical_id] = "timeout"
        else:
            statuses[logical_id] = "error"
    gap = not records and ticker_hash is None
    index = dict(schema_version=1, scheduled_slot_utc=utc(slot), gap=gap,
                 gap_reason=(reason or "no_requests_sent") if gap else None, requests=statuses)
    if ticker_hash is not None:
        index["deribit_tickers_sha256"] = ticker_hash
    if selection is not None:
        index["bybit_selection"] = selection
    return index


def read_minute(root, slot):
    """Read one minute's index and lossless v1 response envelopes."""
    path = Path(root) / _destination(slot)
    data = (path / "index.jsonl").read_bytes()
    index = json.loads(data)
    records = []
    selection = index.get("bybit_selection")
    if selection is not None:
        for base, symbols in selection.items():
            if read_bybit_selection(root, slot, base)["symbols"] != symbols:
                raise ValueError("minute depth plan differs from the Bybit hourly selection")
    ticker_hash = index.get("deribit_tickers_sha256")
    if ticker_hash is not None and sha((path / TICKER_ARCHIVE).read_bytes()) != ticker_hash:
        raise ValueError("minute ticker archive checksum differs")
    if (path / "manifest.json").exists():
        manifest, records = read_archive(path, include_incomplete=True)
        context_selection = read_json(path / "context.json").get("bybit_selection")
        if (manifest["scheduled_slot_utc"] != utc(slot) or manifest["job_kind"] != "minute"
                or context_selection != selection
                or manifest["expected_jobs"] != [logical_id for logical_id, _, _ in _plan(selection)]):
            raise ValueError("minute archive identity mismatch")
        reason = "interrupted" if manifest["status"] == "interrupted" else None
    else:
        reason = index.get("gap_reason")
        if ticker_hash is None and reason not in ("missed_boundary", "interrupted"):
            raise ValueError("minute response archive is missing")
    if data != dumps(_index(slot, records, reason=reason, ticker_hash=ticker_hash, selection=selection)):
        raise ValueError("minute index does not match archived responses")
    return index, records


def read_ticker_samples(root, slot):
    """Read the ticker snapshot referenced by a verified minute index."""
    index, _ = read_minute(root, slot)
    if "deribit_tickers_sha256" not in index:
        return None
    path = Path(root) / _destination(slot) / TICKER_ARCHIVE
    data = path.read_bytes()
    if sha(data) != index["deribit_tickers_sha256"]:
        raise ValueError("minute ticker archive checksum differs")
    snapshot = json.loads(gzip.decompress(data))
    if snapshot["scheduled_slot_utc"] != utc(slot):
        raise ValueError("ticker snapshot belongs to another minute")
    return snapshot


@asynccontextmanager
async def _sampler_session(client, config, url, slot, window, wall, clock):
    if url is None:
        yield None
        return
    sampler = TickerSampler(config.data_root, url, config.environment, wall=wall)
    sampler.schedule(slot)
    async with (
        aiohttp.ClientSession(trust_env=False, cookie_jar=aiohttp.DummyCookieJar()) as socket_session,
        asyncio.TaskGroup() as tasks,
    ):
        running = tasks.create_task(sampler.run(socket_session))
        try:
            await sampler.refresh(client, clock() + window)
            yield sampler
        finally:
            running.cancel()


@asynccontextmanager
async def _bybit_session(config, url, counts, expiries, wall, clock, sleep):
    if url is None:
        yield None
        return
    async with Client(config, clock=clock, wall=wall, sleep=sleep, bybit_base_url=url.rstrip("/") + "/") as client:
        yield BybitMinute(config, client, counts=counts, expiries=expiries)


async def capture_minutes(config, *, minutes=None, request_window=20, ticker_ws_url=None, bybit_base_url=None,
                          bybit_depth=DEFAULT_COUNTS, bybit_expiries=DEFAULT_EXPIRIES, wall=time.time, clock=time.monotonic,
                          sleep=asyncio.sleep):
    """Run the UTC grid. An optional limit counts gaps as well as captured slots."""
    config.validate()
    if not START_GRACE < request_window < PERIOD:
        raise ValueError("request window must be greater than 1 and less than 60 seconds")
    if minutes is not None and (type(minutes) is not int or minutes < 1):
        raise ValueError("minutes must be a positive integer")
    root = config.data_root
    state_path = root / "minute-grid.json"
    with process_lock(root):
        state = read_json(state_path)
        if state is None:
            if (any(path.name != "state" for path in root.iterdir())
                    or any(path.name != "collector.lock" for path in (root / "state").iterdir())):
                raise ValueError("minute capture requires a separate, empty data root")
            state = dict(environment=config.environment, next_slot=math.ceil(wall() / PERIOD) * PERIOD)
            write_json(state_path, state)
        if state["environment"] != config.environment:
            raise ValueError("minute data root belongs to another environment")

        count = 0
        async with (
            Client(config, clock=clock, wall=wall, sleep=sleep) as client,
            _sampler_session(client, config, ticker_ws_url, state["next_slot"], request_window, wall, clock) as sampler,
            _bybit_session(config, bybit_base_url, bybit_depth, bybit_expiries, wall, clock, sleep) as bybit,
        ):
            while minutes is None or count < minutes:
                slot = state["next_slot"]
                destination = _destination(slot)
                if sampler is not None:
                    sampler.schedule(slot)
                path = root / destination
                index_path = path / "index.jsonl"
                if index_path.exists():
                    # A prior process published this minute before advancing its cursor.
                    read_minute(root, slot)
                else:
                    records, reason = await _capture_slot(config, client, slot, destination,
                                                          request_window, wall, clock, sleep, sampler, bybit)
                    ticker_path = path / TICKER_ARCHIVE
                    ticker_hash = sha(ticker_path.read_bytes()) if ticker_path.exists() else None
                    saved_context = read_json(path / "context.json")
                    if saved_context is not None:
                        selection = saved_context.get("bybit_selection")
                    else:
                        selection = bybit.selection(slot) if bybit is not None else None
                    index = _index(slot, records, reason=reason, ticker_hash=ticker_hash, selection=selection)
                    write_bytes(index_path, dumps(index))
                state["next_slot"] = slot + PERIOD
                write_json(state_path, state, replace=True)
                count += 1
    return count


async def _capture_slot(config, client, slot, destination, request_window, wall, clock, sleep, sampler, bybit):
    root = config.data_root
    run_id = str(slot)
    work = root / "work" / run_id / "deribit"
    path = root / destination

    if (work / "context.json").exists() and ((work / "journal.jsonl").exists()
                                           or (path / "manifest.json").exists()):
        Archive(root, {}, existing=work).finalize(interrupted=True)
    if (path / "manifest.json").exists():
        manifest, records = read_archive(path, include_incomplete=True)
        return records, "interrupted" if manifest["status"] == "interrupted" else None
    if work.exists():
        # A process stopped after creating the work directory and before opening the journal.
        return [], "interrupted"

    now = wall()
    while now < slot:
        await sleep(min(slot - now, PERIOD))
        now = wall()
    if now >= slot + START_GRACE:
        return [], "missed_boundary"
    deadline = clock() + (slot - now) + request_window
    if sampler is not None:
        ticker_path = path / TICKER_ARCHIVE
        if not ticker_path.exists():
            samples = sampler.snapshot(slot)
            write_bytes(ticker_path, gzip.compress(dumps(samples), compresslevel=6, mtime=0))
        sampler.schedule(slot + PERIOD)
    selection = bybit.selection(slot) if bybit is not None else None
    requests = _requests()
    capture_context = context(config, "deribit", "minute", utc(slot), run_id)
    capture_context.update(destination=str(destination), started_utc=utc(now),
                           request_window_seconds=request_window,
                           required_jobs=[logical_id for logical_id, _, _ in _plan(selection)])
    if selection is not None:
        capture_context["bybit_selection"] = selection
    files = capture_context["code"]["files"]
    files["data_collector/minute.py"] = sha(Path(__file__).read_bytes())
    if sampler is not None:
        files["data_collector/minute_tickers.py"] = sha(Path(__file__).with_name("minute_tickers.py").read_bytes())
    if bybit is not None:
        files["data_collector/minute_bybit.py"] = sha(Path(__file__).with_name("minute_bybit.py").read_bytes())
    capture_context["code"]["source_sha256"] = sha(dumps(files))
    archive = Archive(root, capture_context)
    async with asyncio.TaskGroup() as tasks:
        send_window = (slot, slot + request_window)
        if bybit is not None:
            tasks.create_task(bybit.capture(archive, slot, deadline, send_window,
                                           [logical_id for logical_id, _, _ in requests]))
        for logical_id, method, params in requests:
            tasks.create_task(client.fetch(archive, logical_id, method, params, deadline,
                                           send_window=send_window))
        if sampler is not None:
            tasks.create_task(sampler.refresh(client, deadline))
    archive.finalize()
    return archive.records, None
