import asyncio
import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

from aiohttp import web

from data_collector.minute import capture_minutes, read_minute
from data_collector.minute_binance import _metadata_digest, read_binance_reference, read_default_binance_universe
from data_collector.minute_files import body_bytes, body_fields
from data_collector.minute_config import Config
from tests.helpers import BYBIT_MINUTE_REPLIES as BYBIT_FIXTURE, DERIBIT_MINUTE_REPLIES as REPLIES


FIXTURE = json.loads((Path(__file__).parent / "fixtures" / "dc_binance_minute_rest.json").read_bytes())
SLOT = 1794124740
MARKETS = ("BTCUSDT", "ETHUSDT")


class BinanceMetadataTests(unittest.TestCase):
    def test_revision_digest_distinguishes_precise_numbers_and_strings(self):
        def digest(value):
            return _metadata_digest(body_fields(b'{"symbols":[{"symbol":"BTCUSDT","filter":' + value + b'}]}'))

        self.assertNotEqual(digest(b"0.1"), digest(b'"0.1"'))
        self.assertNotEqual(digest(b"0.100000000000000000001"), digest(b"0.100000000000000000002"))


class BinanceMinuteTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        temp = tempfile.TemporaryDirectory()
        self.addCleanup(temp.cleanup)
        self.config = Config(data_root=Path(temp.name), attempts=1)
        self.wall = SLOT - 10
        self.clock = 0.0
        self.base_weight = 2097
        self.used = self.base_weight
        self.last_bucket = SLOT
        self.calls = []
        self.missing_first_minute = False
        self.delay_depth = False
        self.metadata_changes = False
        app = web.Application()
        app.router.add_get("/public/{method}", self.deribit)
        app.router.add_get("/fapi/v1/{method}", self.binance)
        app.router.add_get("/v5/market/{method}", self.bybit)
        runner = web.AppRunner(app)
        await runner.setup()
        self.addAsyncCleanup(runner.cleanup)
        site = web.TCPSite(runner, "127.0.0.1", 0)
        await site.start()
        self.url = f"http://127.0.0.1:{runner.addresses[0][1]}/"
        self.enterContext(patch.object(Config, "base_url", property(lambda _: self.url)))

    async def deribit(self, request):
        key = request.match_info["method"], tuple(sorted(request.query.items()))
        body = REPLIES.get(key)
        return web.Response(body=body, status=200 if body is not None else 404)

    async def binance(self, request):
        method, params = request.match_info["method"], dict(request.query)
        self.calls.append((method, params))
        bucket = int(self.wall) // 60 * 60
        if bucket != self.last_bucket:
            self.last_bucket, self.used = bucket, self.base_weight
        self.used += 2 if method == "depth" else 1
        observed = self.used
        if method == "depth" and self.delay_depth:
            await asyncio.sleep(.02)
        if method == "depth":
            body = FIXTURE["depth"]
        elif method == "openInterest":
            body = FIXTURE["open_interest"].replace("BTCUSDT", params["symbol"])
        elif method == "exchangeInfo":
            body = FIXTURE["metadata"]
            if self.metadata_changes and bucket != SLOT:
                value = json.loads(body)
                value["serverTime"] += bucket
                value["symbols"].reverse()
                if bucket >= SLOT + 3660:
                    value["symbols"][0]["status"] = "SETTLING"
                body = json.dumps(value)
        else:
            return web.Response(status=404)
        headers = {}
        if not (self.missing_first_minute and bucket == SLOT):
            headers["X-MBX-USED-WEIGHT-1M"] = str(observed)
        return web.Response(body=body.encode(), headers=headers)

    async def bybit(self, request):
        method, params = request.match_info["method"], dict(request.query)
        base = params.get("baseCoin", "").lower()
        if params["category"] == "option":
            body = BYBIT_FIXTURE[f"{'options' if method == 'tickers' else 'metadata'}-{base}"]
        else:
            fixture = {"tickers": "perp-btc", "instruments-info": "linear-meta-btc", "risk-limit": "risk-btc"}
            body = BYBIT_FIXTURE[fixture[method]].replace("BTCUSDT", params["symbol"])
        return web.Response(body=body.encode())

    async def sleep(self, seconds):
        self.wall += seconds
        self.clock += seconds
        await asyncio.sleep(0)

    async def capture(self, count=1, **kwargs):
        await capture_minutes(
            self.config, minutes=count, binance_base_url=self.url,
            binance_symbols=kwargs.pop("binance_symbols", MARKETS),
            wall=lambda: self.wall, clock=lambda: self.clock, sleep=self.sleep, **kwargs,
        )

    async def test_uses_ip_weight_and_marks_unsent_depth_and_open_interest_missed(self):
        await self.capture()
        index, records = read_minute(self.config.data_root, SLOT)
        self.assertEqual(index["binance_symbols"], list(MARKETS))
        self.assertEqual(index["requests"]["binance:open-interest:BTCUSDT"], "ok")
        self.assertEqual(index["requests"]["binance:depth:BTCUSDT"], "ok")
        self.assertEqual(index["requests"]["binance:open-interest:ETHUSDT"], "missed")
        self.assertEqual(index["requests"]["binance:depth:ETHUSDT"], "missed")
        self.assertEqual(self.calls, [("openInterest", {"symbol": "BTCUSDT"}),
                                     ("depth", {"symbol": "BTCUSDT", "limit": "20"})])
        self.assertEqual(index["binance_weight"]["ip_limit"], 2400)
        self.assertEqual(index["binance_weight"]["bot_reserve"], 300)
        self.assertEqual(index["binance_weight"]["max_observed"], 2100)
        self.assertEqual(index["binance_weight"]["skipped"], {
            "binance:depth:ETHUSDT": "budget", "binance:open-interest:ETHUSDT": "budget",
        })
        by_id = {record["logical_request_id"]: record for record in records}
        depth = by_id["binance:depth:BTCUSDT"]
        oi = by_id["binance:open-interest:BTCUSDT"]
        self.assertEqual(body_bytes(depth), FIXTURE["depth"].encode())
        self.assertEqual(body_bytes(oi), FIXTURE["open_interest"].encode())
        self.assertEqual(depth["response_headers"]["x-mbx-used-weight-1m"], "2100")
        self.assertEqual(depth["venue_event_epoch_ms"], json.loads(FIXTURE["depth"])["E"])
        self.assertEqual(oi["venue_event_epoch_ms"], json.loads(FIXTURE["open_interest"])["time"])
        self.assertGreaterEqual(depth["request_started_epoch_ms"], SLOT * 1000)
        self.assertGreaterEqual(depth["response_received_epoch_ms"], depth["request_started_epoch_ms"])

    async def test_missing_probe_header_blocks_this_minute_and_resets_next_minute(self):
        self.base_weight = self.used = 0
        self.missing_first_minute = True
        await self.capture(2, binance_ip_limit=8, binance_bot_reserve=2)
        first, _ = read_minute(self.config.data_root, SLOT)
        second, _ = read_minute(self.config.data_root, SLOT + 60)
        self.assertIsNone(first["binance_weight"]["max_observed"])
        self.assertEqual(set(first["binance_weight"]["skipped"].values()), {"unobserved"})
        self.assertEqual(len(first["binance_weight"]["skipped"]), 3)
        self.assertEqual(first["requests"]["binance:open-interest:BTCUSDT"], "ok")
        self.assertEqual(read_binance_reference(self.config.data_root, SLOT, failed=True)["skip_reason"],
                         "unobserved")
        self.assertEqual(second["binance_weight"]["max_observed"], 6)
        self.assertEqual(second["binance_weight"]["skipped"], {})
        self.assertEqual(second["requests"]["binance:depth:ETHUSDT"], "ok")

    async def test_reserves_inflight_weight_and_ignores_lower_late_header(self):
        self.base_weight = self.used = 2096
        self.delay_depth = True
        await self.capture()
        index, _ = read_minute(self.config.data_root, SLOT)
        self.assertEqual(index["binance_weight"]["max_observed"], 2100)
        self.assertEqual(index["binance_weight"]["skipped"], {"binance:depth:ETHUSDT": "budget"})
        self.assertEqual(index["requests"]["binance:open-interest:ETHUSDT"], "ok")
        self.assertEqual(index["requests"]["binance:depth:ETHUSDT"], "missed")
        self.assertEqual(sum(method == "depth" for method, _ in self.calls), 1)
        self.assertFalse(any(method == "exchangeInfo" for method, _ in self.calls))

    async def test_archives_native_exchange_info_only_on_instrument_change(self):
        self.base_weight = self.used = 0
        self.metadata_changes = True
        await self.capture(1)
        self.wall = SLOT + 60
        await self.capture(1)
        # Jump the grid over the rest of the hour. The next live slot changes a native symbol.
        self.wall = SLOT + 3660
        await self.capture(60)
        initial = read_binance_reference(self.config.data_root, SLOT)
        self.assertEqual(body_bytes(initial["responses"][0]), FIXTURE["metadata"].encode())
        with self.assertRaises(FileNotFoundError):
            read_binance_reference(self.config.data_root, SLOT + 60)
        changed = read_binance_reference(self.config.data_root, SLOT + 3660)
        self.assertNotEqual(initial["content_sha256"], changed["content_sha256"])
        self.assertIn(b"SETTLING", body_bytes(changed["responses"][0]))
        self.assertEqual(sum(method == "exchangeInfo" for method, _ in self.calls), 3)

    async def test_default_uses_fixed_ranked_universe_with_provenance(self):
        self.base_weight = self.used = 0
        await self.capture(binance_symbols=None)
        index, records = read_minute(self.config.data_root, SLOT)
        ranked = read_default_binance_universe()
        self.assertEqual(ranked["retrieved_at_utc"], "2026-11-08T07:30:48Z")
        self.assertEqual(len(ranked["endpoint_sha256"]), 2)
        self.assertEqual(len(ranked["symbols"]), 100)
        self.assertEqual(ranked["symbols"][:4], ["BTCUSDT", "ETHUSDT", "ZECUSDT", "ONEUSDT"])
        self.assertEqual(ranked["symbols"][-2:], ["ATOMUSDT", "HOTUSDT"])
        self.assertIn("牛来USDT", ranked["symbols"])
        self.assertEqual(index["binance_symbols"], ranked["symbols"])
        self.assertEqual(len(records), 211)
        self.assertEqual(index["binance_weight"]["max_observed"], 301)
        self.assertEqual(index["binance_weight"]["skipped"], {})
        self.assertFalse(any(method == "ticker" for method, _ in self.calls))

    async def test_three_venues_share_the_same_expected_request_index(self):
        self.base_weight = self.used = 0
        await self.capture(bybit_base_url=self.url, bybit_depth=(0, 0))
        index, records = read_minute(self.config.data_root, SLOT)
        self.assertEqual(index["bybit_selection"], {"BTC": [], "ETH": []})
        self.assertEqual(len(index["requests"]), 19)
        self.assertEqual(set(index["requests"].values()), {"ok"})
        self.assertEqual(len(records), 19)
        self.assertEqual({row["scheduled_slot_utc"] for row in records}, {index["scheduled_slot_utc"]})
