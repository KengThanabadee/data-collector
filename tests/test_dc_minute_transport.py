"""Minute REST transport invariants against a loopback server."""
import asyncio
from dataclasses import replace
from email.utils import formatdate
import json
from pathlib import Path
import tempfile
import time
import unittest
from unittest.mock import patch

from aiohttp import web

from data_collector.minute_archive import Archive, verify
from data_collector.minute_config import Config, context
from data_collector.minute_files import body_bytes
from data_collector.minute_transport import Client, Gate, retry_after

SLOT = "2026-11-08T23:59:00.000+00:00"
INDEX = "public/get_index_price"


class MinuteHttpTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.config = Config(data_root=Path(self.tmp.name), require_ntp=False, attempt_seconds=.08, attempts=1)
        self.handler = None
        app = web.Application()
        app.router.add_get("/{tail:.*}", self.route)
        self.runner = web.AppRunner(app)
        await self.runner.setup()
        site = web.TCPSite(self.runner, "127.0.0.1", 0)
        await site.start()
        port = site._server.sockets[0].getsockname()[1]
        patcher = patch.object(Config, "base_url", property(lambda _: f"http://127.0.0.1:{port}/"))
        patcher.start()
        self.addCleanup(patcher.stop)

    async def asyncTearDown(self):
        await self.runner.cleanup()

    async def route(self, request):
        return await self.handler(request)

    def archive(self):
        return Archive(self.config.data_root, context(self.config, "deribit", "minute", SLOT, "run-1"))

    async def fetch(self, archive, method=INDEX, params=None, logical="index"):
        async with Client(self.config) as client:
            return await client.fetch(archive, logical, method, params or {"index_name": "btc_usd"},
                                      time.monotonic() + 2)

    async def test_http200_api_throttle_and_non_json_429_persist_cooldown(self):
        for api in (True, False):
            with self.subTest(api=api):
                async def handler(req):
                    return web.Response(body=b'{"error":{"code":10028}}' if api else b"not json",
                                        status=200 if api else 429, headers={"Retry-After": "10"})
                self.handler = handler
                rec = await self.fetch(self.archive_for(api))
                self.assertFalse(rec["ok"])
                self.assertTrue((self.config.data_root / "state/cooldown.json").exists())
                self.assertIsNotNone(rec["raw_body"])
                (self.config.data_root / "state/cooldown.json").unlink()

    def archive_for(self, api):
        return Archive(self.config.data_root, context(self.config, "deribit", "minute", SLOT, f"run-{api}"))

    async def test_drip_feed_total_timeout_retains_prefix(self):
        async def handler(req):
            resp = web.StreamResponse()
            await resp.prepare(req)
            try:
                for _ in range(10):
                    await resp.write(b"x")
                    await asyncio.sleep(.025)
            except (ConnectionResetError, RuntimeError):
                pass
            return resp
        self.handler = handler
        start = time.monotonic()
        rec = await self.fetch(self.archive())
        self.assertLess(time.monotonic() - start, .3)
        self.assertFalse(rec["body_complete"])
        self.assertGreater(rec["body_byte_count"], 0)
        self.assertEqual(rec["transport_error"], "TimeoutError")

    async def test_body_cap_including_single_final_chunk(self):
        self.config = replace(self.config, body_cap=1024)
        async def handler(req):
            return web.Response(body=b"x" * 1025)
        self.handler = handler
        rec = await self.fetch(self.archive())
        self.assertFalse(rec["ok"])
        self.assertFalse(rec["body_complete"])
        self.assertEqual(rec["body_byte_count"], 1024)

    async def test_inflight_cancellation_records_attempt(self):
        async def handler(req):
            await asyncio.sleep(.3)
            return web.json_response({"result": {"index_price": 1}})
        self.handler = handler
        archive = self.archive()
        async with Client(self.config) as client:
            task = asyncio.create_task(client.fetch(archive, "index", INDEX, {}, time.monotonic() + 2))
            await asyncio.sleep(.025)
            task.cancel()
            with self.assertRaises(asyncio.CancelledError):
                await task
        self.assertEqual(len(archive.records), 1)
        self.assertEqual(archive.records[0]["transport_error"], "cancelled")

    async def test_bounded_retry_reuses_logical_request_and_retains_errors(self):
        self.config = replace(self.config, attempts=3, attempt_seconds=1)
        calls = 0
        async def handler(req):
            nonlocal calls
            calls += 1
            return web.json_response({"error": {"code": -1}} if calls < 2 else {"result": {"index_price": 1}},
                                     status=503 if calls < 2 else 200)
        self.handler = handler
        archive = self.archive()
        with patch("data_collector.minute_transport.random.uniform", return_value=0), \
                patch.object(Gate, "acquire", lambda *_: asyncio.sleep(0)):
            async with Client(self.config, sleep=lambda _: asyncio.sleep(0)) as client:
                rec = await client.fetch(archive, "index", INDEX, {"index_name": "btc_usd"}, time.monotonic() + 5)
                again = await client.fetch(archive, "index", INDEX, {"index_name": "btc_usd"}, time.monotonic() + 5)
        self.assertTrue(rec["ok"])
        self.assertIs(again, rec)
        self.assertEqual([r["attempt"] for r in archive.records], [1, 2])
        self.assertEqual(calls, 2)

    async def test_minute_methods_are_validated_and_unknown_fields_preserved(self):
        async def handler(req):
            method = req.path.rsplit("/", 1)[-1]
            if method == "ticker":
                result = {"instrument_name": req.query["instrument_name"] + "-other", "timestamp": 1}
            elif method == "get_book_summary_by_currency":
                result = [{"instrument_name": "BTC-PERPETUAL", "unknown": "preserved"}]
            else:
                result = {"index_price": 100}
            return web.json_response({"result": result, "testnet": False, "usOut": 1234})
        self.handler = handler
        archive = self.archive()
        ticker = await self.fetch(archive, "public/ticker", {"instrument_name": "BTC-PERPETUAL"}, "ticker")
        self.assertFalse(ticker["ok"])
        self.assertIn("ticker identity", ticker["validation_error"])
        summary = await self.fetch(archive, "public/get_book_summary_by_currency",
                                   {"currency": "BTC", "kind": "future"}, "summary")
        self.assertTrue(summary["ok"])
        self.assertEqual(summary["venue_server_epoch_us"], 1234)
        self.assertIn("unknown", json.loads(body_bytes(summary))["result"][0])
        self.assertEqual(summary["quality_flags"], [])
        archive.update_context(required_jobs=["summary"])
        archive.finalize()
        self.assertEqual(verify(archive.destination)[0]["status"], "complete")

    async def test_unlisted_methods_and_other_run_kinds_are_refused(self):
        async def handler(req):
            raise AssertionError("no request may be sent")
        self.handler = handler
        archive = self.archive()
        async with Client(self.config) as client:
            for method in ("public/get_order_book", "public/get_delivery_prices", "private/get_positions"):
                with self.assertRaises(ValueError):
                    await client.fetch(archive, "x", method, {}, time.monotonic() + 1)
            other = Archive(self.config.data_root, context(self.config, "deribit", "snapshot", SLOT, "run-other"))
            with self.assertRaises(ValueError):
                await client.fetch(other, "x", INDEX, {}, time.monotonic() + 1)
        self.assertEqual(archive.records, [])

    async def test_environment_mismatch_is_a_validation_error(self):
        async def handler(req):
            return web.json_response({"result": {"index_price": 1}, "testnet": True})
        self.handler = handler
        rec = await self.fetch(self.archive())
        self.assertFalse(rec["ok"])
        self.assertIn("environment mismatch", rec["validation_error"])


class MinuteGateTests(unittest.IsolatedAsyncioTestCase):
    async def test_shared_pacing_discovery_and_persisted_cooldown(self):
        with tempfile.TemporaryDirectory() as d:
            now = [0.0]
            async def sleep(s):
                now[0] += s
            config = Config(data_root=Path(d))
            gate = Gate(config, clock=lambda: now[0], wall=lambda: 1000 + now[0], sleep=sleep)
            await gate.acquire(True, 10)
            await gate.acquire(False, 10)
            self.assertAlmostEqual(now[0], .1)
            await gate.acquire(True, 10)
            self.assertAlmostEqual(now[0], 1)
            gate.throttle(4)
            newer = Gate(config, clock=lambda: now[0], wall=lambda: 1000 + now[0], sleep=sleep)
            with self.assertRaises(TimeoutError):
                await newer.acquire(False, 4)
            await newer.acquire(False, 10)
            self.assertAlmostEqual(now[0], 5)

    def test_retry_after_httpdate(self):
        self.assertEqual(retry_after(formatdate(1010, usegmt=True), 1000), 10)
        self.assertEqual(retry_after("12", 1000), 12)
        self.assertEqual(retry_after("invalid", 1000), 0)


if __name__ == "__main__":
    unittest.main()
