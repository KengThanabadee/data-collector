import asyncio
from contextlib import redirect_stdout
from dataclasses import replace
import io
import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

from aiohttp import web

from data_collector.cli import main
from data_collector.minute import capture_minutes, read_minute
from option_collector.common import body_bytes
from option_collector.config import Config


SLOT = 1794122460
FIXTURE = Path(__file__).parent / "fixtures" / "dc_deribit_minute_rest.json"
REPLIES = {
    (row["method"], tuple(sorted(row["params"].items()))): row["body"].encode()
    for row in json.loads(FIXTURE.read_bytes())
}


def recorded_body(method, **params):
    return REPLIES[(method, tuple(sorted(params.items())))]


class Clock:
    def __init__(self):
        self.wall = SLOT - 15
        self.elapsed = 0.0
        self.delay_next_boundary = 0
        self.jump_next_request = 0

    async def sleep(self, seconds):
        if seconds >= 1:
            seconds += self.delay_next_boundary
            self.delay_next_boundary = 0
        elif self.jump_next_request:
            self.wall += self.jump_next_request
            self.jump_next_request = 0
        self.wall += seconds
        self.elapsed += seconds
        await asyncio.sleep(0)


class DeribitMinuteTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        tmp = tempfile.TemporaryDirectory()
        self.addCleanup(tmp.cleanup)
        self.config = Config(data_root=Path(tmp.name), attempts=1)
        self.clock = Clock()
        self.requests = []
        self.fail_futures = False
        self.block_futures = None
        self.futures_blocked = asyncio.Event()
        app = web.Application()
        app.router.add_get("/public/{method}", self.reply)
        runner = web.AppRunner(app)
        await runner.setup()
        self.addAsyncCleanup(runner.cleanup)
        site = web.TCPSite(runner, "127.0.0.1", 0)
        await site.start()
        port = runner.addresses[0][1]
        self.enterContext(patch.object(Config, "base_url", property(lambda _: f"http://127.0.0.1:{port}/")))

    async def reply(self, request):
        method = request.match_info["method"]
        params = dict(request.query)
        self.requests.append((method, params))
        if method == "get_book_summary_by_currency":
            if self.block_futures is not None and params["currency"] == "ETH":
                self.futures_blocked.set()
                await self.block_futures.wait()
            if self.fail_futures and params["currency"] == "ETH":
                await asyncio.sleep(.15)
            if self.fail_futures and params["currency"] == "BTC":
                return web.Response(body=b'{"jsonrpc":"2.0","error":{"code":11050},"usOut":1794122460000456}')
        body = REPLIES.get((method, tuple(sorted(params.items()))))
        if body is None:
            return web.Response(status=404)
        return web.Response(body=body, content_type="application/json")

    async def capture(self, **kwargs):
        await capture_minutes(
            self.config,
            wall=lambda: self.clock.wall,
            clock=lambda: self.clock.elapsed,
            sleep=self.clock.sleep,
            **kwargs,
        )

    async def test_deribit_profile_fires_on_utc_grid_and_reads_native_timed_bytes(self):
        await self.capture(minutes=1)
        index, records = read_minute(self.config.data_root, SLOT)
        self.assertEqual(index["scheduled_slot_utc"], "2026-11-08T07:21:00.000+00:00")
        self.assertFalse(index["gap"])
        self.assertEqual(sorted(index["requests"].items()), [
            ("deribit:dvol:BTC", "ok"), ("deribit:dvol:ETH", "ok"),
            ("deribit:futures:BTC", "ok"), ("deribit:futures:ETH", "ok"),
            ("deribit:index:btc_usd", "ok"), ("deribit:index:btc_usdt", "ok"),
            ("deribit:index:eth_usd", "ok"), ("deribit:index:eth_usdt", "ok"),
            ("deribit:index:usdt_usd", "ok"),
            ("deribit:ticker:BTC-PERPETUAL", "ok"), ("deribit:ticker:ETH-PERPETUAL", "ok"),
        ])
        expected = [
            ("get_book_summary_by_currency", {"currency": currency, "kind": "future"})
            for currency in ("BTC", "ETH")
        ] + [
            ("get_index_price", {"index_name": name})
            for name in ("btc_usd", "eth_usd", "usdt_usd", "btc_usdt", "eth_usdt")
        ] + [
            ("get_index_price", {"index_name": name})
            for name in ("btcdvol_usdc", "ethdvol_usdc")
        ] + [
            ("ticker", {"instrument_name": name}) for name in ("BTC-PERPETUAL", "ETH-PERPETUAL")
        ]
        self.assertCountEqual(self.requests, expected)

        self.assertEqual(len(records), 11)
        self.assertEqual(min(record["request_started_epoch_ms"] for record in records), 1794122460000)
        self.assertTrue(all(record["request_started_epoch_ms"] >= 1794122460000 for record in records))
        self.assertTrue(all(record["response_received_epoch_ms"] >= record["request_started_epoch_ms"]
                            for record in records))
        by_request = {record["logical_request_id"]: record for record in records}
        btc = by_request["deribit:index:btc_usd"]
        self.assertEqual(btc["venue_server_epoch_us"], 1794123384426780)
        for record in records:
            expected_body = recorded_body(record["method"].removeprefix("public/"), **record["parameters"])
            self.assertEqual(body_bytes(record), expected_body)
            self.assertEqual(record["venue_server_epoch_us"], json.loads(expected_body)["usOut"])

    async def test_index_distinguishes_errors_timeouts_and_unsent_requests(self):
        self.config = replace(self.config, rate=.5, attempt_seconds=.06)
        self.fail_futures = True
        await self.capture(minutes=1, request_window=5)
        index, records = read_minute(self.config.data_root, SLOT)
        self.assertFalse(index["gap"])
        self.assertEqual(index["requests"]["deribit:futures:BTC"], "error")
        self.assertEqual(index["requests"]["deribit:futures:ETH"], "timeout")
        self.assertEqual(index["requests"]["deribit:index:btc_usd"], "ok")
        self.assertEqual(list(index["requests"].values()).count("missed"), 8)
        self.assertEqual(len(index["requests"]), 11)
        self.assertEqual(len(records), 3)
        timeout = next(record for record in records if record["logical_request_id"] == "deribit:futures:ETH")
        self.assertIsNone(timeout["response_received_utc"])
        self.assertIsNone(timeout["response_received_epoch_ms"])
        self.assertIsNone(timeout["venue_server_epoch_us"])
        self.assertIsNone(body_bytes(timeout))
        error = next(record for record in records if record["logical_request_id"] == "deribit:futures:BTC")
        self.assertEqual(body_bytes(error), b'{"jsonrpc":"2.0","error":{"code":11050},"usOut":1794122460000456}')
        self.assertEqual(error["venue_server_epoch_us"], 1794122460000456)

    async def test_late_wakeup_and_restart_emit_gaps_without_backfilling(self):
        self.clock.delay_next_boundary = 120
        await self.capture(minutes=3)
        for slot in (SLOT, SLOT + 60):
            index, records = read_minute(self.config.data_root, slot)
            self.assertTrue(index["gap"])
            self.assertEqual(index["gap_reason"], "missed_boundary")
            self.assertEqual(set(index["requests"].values()), {"missed"})
            self.assertEqual(len(index["requests"]), 11)
            self.assertEqual(records, [])
        good, records = read_minute(self.config.data_root, SLOT + 120)
        self.assertFalse(good["gap"])
        self.assertEqual(len(records), 11)
        self.assertEqual(len(self.requests), 11)

        self.clock.wall = SLOT + 300
        await self.capture(minutes=3)
        self.assertEqual(len(self.requests), 22)
        self.assertEqual(read_minute(self.config.data_root, SLOT + 120), (good, records))
        for slot in (SLOT + 180, SLOT + 240):
            index, records = read_minute(self.config.data_root, slot)
            self.assertTrue(index["gap"])
            self.assertEqual(records, [])
        self.assertFalse(read_minute(self.config.data_root, SLOT + 300)[0]["gap"])

    async def test_forward_clock_step_does_not_send_for_an_expired_minute(self):
        self.clock.jump_next_request = 90
        await self.capture(minutes=2)
        index, records = read_minute(self.config.data_root, SLOT)
        self.assertEqual(list(index["requests"].values()).count("ok"), 1)
        self.assertEqual(list(index["requests"].values()).count("missed"), 10)
        self.assertEqual(len(records), 1)
        self.assertEqual(len(self.requests), 1)
        self.assertTrue(read_minute(self.config.data_root, SLOT + 60)[0]["gap"])

    async def test_restart_preserves_interrupted_attempts_without_reissuing_requests(self):
        self.config = replace(self.config, workers=1)
        self.block_futures = asyncio.Event()
        self.addCleanup(self.block_futures.set)
        run = asyncio.create_task(self.capture(minutes=1))
        try:
            await asyncio.wait_for(self.futures_blocked.wait(), timeout=2)
        finally:
            run.cancel()
            with self.assertRaises(asyncio.CancelledError):
                await run
            self.block_futures.set()

        await self.capture(minutes=1)
        index, records = read_minute(self.config.data_root, SLOT)
        self.assertFalse(index["gap"])
        self.assertEqual(index["requests"]["deribit:futures:BTC"], "ok")
        self.assertEqual(index["requests"]["deribit:futures:ETH"], "error")
        self.assertEqual(list(index["requests"].values()).count("missed"), 9)
        self.assertEqual(len(records), 2)
        self.assertEqual(body_bytes(records[0]),
                         recorded_body("get_book_summary_by_currency", currency="BTC", kind="future"))
        self.assertEqual(len(self.requests), 2)

    async def test_minute_read_command_exports_verified_index_and_responses(self):
        await self.capture(minutes=1)
        output = io.StringIO()
        with redirect_stdout(output):
            code = main(["minute-read", "--data-root", str(self.config.data_root),
                         "--slot", "2026-11-08T07:21:00Z"])
        self.assertEqual(code, 0)
        exported = json.loads(output.getvalue())
        index, records = read_minute(self.config.data_root, SLOT)
        self.assertEqual(exported, {"index": index, "records": records})
