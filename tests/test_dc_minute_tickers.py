import asyncio
from contextlib import redirect_stdout
import io
import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

from aiohttp import web

from data_collector.cli import main
from data_collector.minute import capture_minutes, read_minute, read_ticker_samples
from data_collector.minute_tickers import read_deribit_metadata
from option_collector.common import body_bytes
from option_collector.config import Config
from tests.test_dc_minute import REPLIES


FIXTURE = json.loads((Path(__file__).parent / "fixtures" / "dc_deribit_minute_tickers.json").read_bytes())
EXPIRING = "BTC-8NOV26-69000-C"
FUTURE = "BTC-13NOV26"
QUIET = "ETH-13NOV26-1800-C"
NEW = "BTC-13NOV26-65000-C"
FIRST_SLOT = 1794124740


class TickerMinuteTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        tmp = tempfile.TemporaryDirectory()
        self.addCleanup(tmp.cleanup)
        self.config = Config(data_root=Path(tmp.name), attempts=1)
        self.wall = FIRST_SLOT - 15
        self.clock = 0.0
        self.ready = asyncio.Event()
        self.metadata_reads = 0
        self.metadata_bodies = []
        self.requested = []
        self.retired = []
        self.connections = []
        self.reconnected = asyncio.Event()
        self.retirement_seen = asyncio.Event()
        self.lifecycle = False
        self.drop_first = True
        self.new_sent = set()
        self.new_seen = asyncio.Event()
        self.sleeps = 0
        app = web.Application()
        app.router.add_get("/public/{method}", self.rest)
        app.router.add_get("/ws/api/v2", self.socket)
        runner = web.AppRunner(app)
        await runner.setup()
        self.addAsyncCleanup(runner.cleanup)
        site = web.TCPSite(runner, "127.0.0.1", 0)
        await site.start()
        port = runner.addresses[0][1]
        self.ws_url = f"ws://127.0.0.1:{port}/ws/api/v2"
        self.enterContext(patch.object(Config, "base_url", property(lambda _: f"http://127.0.0.1:{port}/")))

    async def rest(self, request):
        method = request.match_info["method"]
        params = dict(request.query)
        if method == "get_instruments":
            self.metadata_reads += 1
            rows = FIXTURE["initial"]
            if self.lifecycle and self.metadata_reads >= 2:
                rows = [*rows, FIXTURE["new"]]
            data = json.dumps({"jsonrpc": "2.0", "result": rows,
                               "usOut": 1794124700000000 + self.metadata_reads}).encode()
            self.metadata_bodies.append(data)
            return web.Response(body=data)
        body = REPLIES.get((method, tuple(sorted(params.items()))))
        return web.Response(body=body, status=200 if body is not None else 404)

    async def socket(self, request):
        ws = web.WebSocketResponse(compress=True)
        await ws.prepare(request)
        self.connections.append(ws)
        connection = len(self.connections)
        async for message in ws:
            rpc = message.json()
            method = rpc["method"]
            if method == "public/subscribe":
                channels = rpc["params"]["channels"]
                self.requested.extend(channels)
                near = f"ticker.{EXPIRING}.agg2"
                future = f"ticker.{FUTURE}.agg2"
                new = f"ticker.{NEW}.agg2"
                if connection == 1 and near in channels:
                    await ws.send_str(FIXTURE["frames"][near])
                await ws.send_json({"jsonrpc": "2.0", "id": rpc["id"], "result": channels})
                if connection == 1 and future in channels:
                    await ws.send_str(FIXTURE["frames"][future].replace('"jsonrpc":', '"jsonrpc" :', 1))
                    await ws.send_str(FIXTURE["frames"][future])
                if new in channels:
                    await ws.send_str(FIXTURE["frames"][new])
                    self.new_sent.add(connection)
                await ws.send_json({"jsonrpc": "2.0", "method": "heartbeat", "params": {"type": "test_request"}})
            elif method == "public/unsubscribe":
                channels = rpc["params"]["channels"]
                self.retired.extend(channels)
                await ws.send_json({"jsonrpc": "2.0", "id": rpc["id"], "result": channels})
                self.retirement_seen.set()
            elif method == "public/test":
                (self.ready if connection == 1 else self.reconnected).set()
                if connection in self.new_sent:
                    self.new_seen.set()
        return ws

    async def sleep(self, seconds):
        if seconds > 10:
            self.sleeps += 1
            if self.sleeps == 1:
                await asyncio.wait_for(self.ready.wait(), 5)
            elif self.lifecycle and self.sleeps == 2:
                if self.drop_first:
                    await self.connections[0].close()
                    await asyncio.wait_for(self.reconnected.wait(), 5)
                else:
                    await asyncio.wait_for(self.new_seen.wait(), 5)
            elif self.lifecycle and self.sleeps == 3:
                await asyncio.wait_for(self.retirement_seen.wait(), 5)
        self.clock += seconds
        self.wall += seconds
        await asyncio.sleep(0)

    async def capture(self, minutes):
        await capture_minutes(
            self.config, minutes=minutes, ticker_ws_url=self.ws_url,
            wall=lambda: self.wall, clock=lambda: self.clock, sleep=self.sleep,
        )

    async def test_reads_last_raw_frame_and_distinguishes_pre_ack_and_missing(self):
        await self.capture(1)
        index, records = read_minute(self.config.data_root, FIRST_SLOT)
        self.assertEqual(len(records), 11)
        self.assertIn("deribit_tickers_sha256", index)
        tickers = read_ticker_samples(self.config.data_root, FIRST_SLOT)
        rows = tickers["samples"]
        self.assertEqual(set(rows), {EXPIRING, FUTURE, QUIET})
        self.assertCountEqual(self.requested, [f"ticker.{name}.agg2" for name in rows])
        self.assertEqual(rows[FUTURE]["status"], "ok")
        self.assertEqual(rows[EXPIRING]["status"], "stale")
        self.assertEqual(rows[QUIET]["status"], "missing")
        self.assertIsNone(rows[QUIET]["raw_body"])
        self.assertIsNotNone(rows[QUIET]["subscribe_ack_epoch_ms"])
        self.assertLess(rows[QUIET]["subscribe_ack_epoch_ms"], FIRST_SLOT * 1000)
        output = io.StringIO()
        with redirect_stdout(output):
            code = main(["minute-read", "--data-root", str(self.config.data_root),
                         "--slot", "2026-11-08T07:59:00Z"])
        self.assertEqual(code, 0)
        self.assertEqual(json.loads(output.getvalue())["deribit_tickers"], tickers)
        for name in (FUTURE, EXPIRING):
            native = FIXTURE["frames"][f"ticker.{name}.agg2"]
            self.assertEqual(body_bytes(rows[name]), native.encode())
            self.assertEqual(rows[name]["venue_epoch_ms"], json.loads(native)["params"]["data"]["timestamp"])
            self.assertLess(rows[name]["received_epoch_ms"], FIRST_SLOT * 1000)
            self.assertTrue(rows[name]["connection_id"])

    async def test_refreshes_listings_reconnects_and_retires_at_0800(self):
        self.lifecycle = True
        await self.capture(3)

        before = read_ticker_samples(self.config.data_root, FIRST_SLOT)
        expiry = read_ticker_samples(self.config.data_root, FIRST_SLOT + 60)
        after = read_ticker_samples(self.config.data_root, FIRST_SLOT + 120)
        self.assertEqual(expiry["scheduled_slot_utc"], "2026-11-08T08:00:00.000+00:00")
        self.assertIn(EXPIRING, before["samples"])
        self.assertNotIn(EXPIRING, expiry["samples"])
        self.assertNotIn(EXPIRING, after["samples"])
        self.assertEqual(self.retired, [f"ticker.{EXPIRING}.agg2"])
        self.assertNotIn(NEW, before["samples"])
        self.assertEqual(expiry["samples"][NEW]["status"], "ok")
        self.assertEqual(expiry["samples"][NEW]["connection_id"], expiry["current_connection_id"])
        self.assertEqual(expiry["samples"][FUTURE]["status"], "stale")
        self.assertEqual(expiry["samples"][FUTURE]["connection_id"], before["current_connection_id"])
        self.assertNotEqual(before["current_connection_id"], expiry["current_connection_id"])
        self.assertEqual(expiry["samples"][QUIET]["status"], "missing")

        self.assertEqual([before["metadata_version"], expiry["metadata_version"],
                          after["metadata_version"]], [1, 2, 2])
        self.assertEqual(self.metadata_reads, 4)
        self.assertEqual(body_bytes(read_deribit_metadata(self.config.data_root, 1)), self.metadata_bodies[0])
        self.assertEqual(body_bytes(read_deribit_metadata(self.config.data_root, 2)), self.metadata_bodies[1])
        with self.assertRaises(FileNotFoundError):
            read_deribit_metadata(self.config.data_root, 3)

    async def test_subscribes_new_listing_on_healthy_socket_before_next_cutoff(self):
        self.lifecycle = True
        self.drop_first = False
        await self.capture(2)
        before = read_ticker_samples(self.config.data_root, FIRST_SLOT)
        after = read_ticker_samples(self.config.data_root, FIRST_SLOT + 60)
        self.assertEqual(before["current_connection_id"], after["current_connection_id"])
        self.assertEqual(len(self.connections), 1)
        self.assertNotIn(NEW, before["samples"])
        new = after["samples"][NEW]
        self.assertEqual(new["status"], "ok")
        self.assertLessEqual(new["subscribe_ack_epoch_ms"], new["received_epoch_ms"])
        self.assertLess(new["received_epoch_ms"], (FIRST_SLOT + 60) * 1000)
