import asyncio
import json
from dataclasses import replace
from datetime import datetime, timezone
from pathlib import Path
import shutil
import tempfile
import unittest
from unittest.mock import patch

from aiohttp import web

from data_collector.minute import capture_minutes, read_minute
from data_collector.minute_bybit import read_bybit_reference, read_bybit_selection
from data_collector.minute_storage import seal_minute_day
from option_collector.common import body_bytes, read_json
from option_collector.config import Config
from tests.test_dc_minute import REPLIES


FIXTURE = json.loads((Path(__file__).parent / "fixtures" / "dc_bybit_minute_rest.json").read_bytes())
START = 1794124740
BTC_CALL = "BTC-9NOV26-76000-C-USDT"
BTC_PUT = "BTC-9NOV26-76000-P-USDT"
ETH_CALL = "ETH-9NOV26-2240-C-USDT"


class BybitMinuteTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        temp = tempfile.TemporaryDirectory()
        self.addCleanup(temp.cleanup)
        self.config = Config(data_root=Path(temp.name), attempts=1)
        self.wall = START - 10
        self.clock = 0.
        self.depth = []
        self.bybit_calls = []
        self.depth_faults = False
        self.change_hour = False
        self.fail_rollover_page = False
        self.reference_type_change = False
        self.late = 0
        app = web.Application()
        app.router.add_get("/public/{method}", self.deribit)
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

    async def bybit(self, request):
        method = request.match_info["method"]
        params = dict(request.query)
        self.bybit_calls.append((method, params))
        base = params.get("baseCoin", "").lower()
        symbol = params.get("symbol")
        changed = self.change_hour and self.wall >= START + 60
        if method == "tickers" and params["category"] == "option":
            body = FIXTURE[f"options-{base}"]
            if changed and base == "btc":
                value = json.loads(body)
                for row in value["result"]["list"]:
                    row["indexPrice"] = "75750.00"
                body = json.dumps(value)
        elif method == "instruments-info" and params["category"] == "option":
            body = FIXTURE[f"metadata-{base}"]
            if self.change_hour and base == "btc":
                value = json.loads(body)
                rows = value["result"]["list"]
                if changed:
                    rows[0]["priceFilter"]["tickSize"] = "7.5"
                if "cursor" in params:
                    if "cursor=second%3D" not in request.rel_url.raw_query_string.split("&"):
                        return web.json_response({"retCode": 10001, "retMsg": "cursor differs"})
                    if self.fail_rollover_page and START + 60 <= self.wall < START + 120:
                        return web.json_response({"retCode": 10001, "retMsg": "catalog page error", "result": {}})
                    value["result"].update(list=rows[4:], nextPageCursor="")
                else:
                    value["result"].update(list=rows[:4], nextPageCursor="second%3D")
                body = json.dumps(value)
            if self.reference_type_change and base == "btc":
                value = json.loads(body)
                value["result"]["list"][0]["fixtureValue"] = 0.1 if self.wall < START + 60 else "0.1"
                body = json.dumps(value)
        elif method == "tickers":
            body = FIXTURE["perp-btc"].replace("BTCUSDT", symbol)
        elif method == "instruments-info":
            body = FIXTURE["linear-meta-btc"].replace("BTCUSDT", symbol)
        elif method == "risk-limit":
            if self.depth_faults and symbol == "ETHUSDT":
                return web.Response(body=b'{"retCode":10001,"retMsg":"risk error","result":{},"time":1794124740220}')
            body = FIXTURE["risk-btc"].replace("BTCUSDT", symbol)
            if changed and symbol == "BTCUSDT":
                value = json.loads(body)
                value["result"]["list"][0]["riskLimitValue"] = "999"
                body = json.dumps(value)
        elif method == "new-delivery-price":
            fixture = "delivery-btc-usdc" if params["settleCoin"] == "USDC" else "delivery-btc"
            body = FIXTURE[fixture]
        elif method == "orderbook":
            self.depth.append(params)
            if self.depth_faults and symbol == ETH_CALL:
                await asyncio.sleep(.12)
            if self.depth_faults and symbol == BTC_PUT:
                return web.Response(body=b'{"retCode":10001,"retMsg":"fixture error","result":{},"time":1794124740200}')
            body = ('{"retCode":0,"retMsg":"OK","result":{"s":"%s","b":[["1.2300","0.50"]],'
                    '"a":[],"ts":1794124740123},"time":1794124740125}' % symbol)
        else:
            return web.Response(status=404)
        return web.Response(body=body.encode())

    async def sleep(self, seconds):
        if seconds > 10 and self.wall >= START and self.late:
            seconds += self.late
            self.late = 0
        self.wall += seconds
        self.clock += seconds
        await asyncio.sleep(0)

    async def capture(self, minutes, **options):
        await capture_minutes(
            self.config, minutes=minutes, bybit_base_url=self.url, bybit_depth=options.pop("bybit_depth", (2, 1)),
            wall=lambda: self.wall, clock=lambda: self.clock, sleep=self.sleep, **options,
        )

    async def test_shared_minute_preserves_bybit_bodies_times_and_configured_depth(self):
        await self.capture(1)
        index, records = read_minute(self.config.data_root, START)
        self.assertEqual(index["bybit_selection"], {"BTC": [BTC_CALL, BTC_PUT], "ETH": [ETH_CALL]})
        self.assertCountEqual(self.depth, [
            {"category": "option", "symbol": symbol, "limit": "25"}
            for symbol in (BTC_CALL, BTC_PUT, ETH_CALL)
        ])
        self.assertEqual(len(index["requests"]), 18)
        by_id = {record["logical_request_id"]: record for record in records}
        option = by_id["bybit:option-tickers:BTC"]
        self.assertEqual(body_bytes(option), FIXTURE["options-btc"].encode())
        self.assertEqual(option["venue_server_epoch_ms"], json.loads(FIXTURE["options-btc"])["time"])
        first = min(row["request_started_epoch_ms"] for row in records
                    if row["logical_request_id"].startswith("bybit:"))
        self.assertLessEqual(START * 1000, first)
        self.assertLess(first, (START + 1) * 1000)
        self.assertGreaterEqual(option["response_received_epoch_ms"], option["request_started_epoch_ms"])
        self.assertEqual(index["requests"][f"bybit:depth:{ETH_CALL}"], "ok")
        self.assertEqual(by_id[f"bybit:depth:{BTC_CALL}"]["venue_server_epoch_ms"], 1794124740125)

    async def test_reselects_on_the_hour_and_archives_only_changed_references(self):
        self.change_hour = True
        await self.capture(5)
        before, _ = read_minute(self.config.data_root, START)
        rollover, _ = read_minute(self.config.data_root, START + 60)
        after, _ = read_minute(self.config.data_root, START + 120)
        self.assertEqual(before["bybit_selection"]["BTC"], [BTC_CALL, BTC_PUT])
        self.assertEqual(rollover["bybit_selection"]["BTC"], [
            "BTC-9NOV26-75750-C-USDT", "BTC-9NOV26-75750-P-USDT",
        ])
        self.assertEqual(after["bybit_selection"], rollover["bybit_selection"])
        selected = read_bybit_selection(self.config.data_root, START + 120, "BTC")
        self.assertEqual(selected["selected_slot_epoch"], START + 60)
        self.assertEqual(selected["index_price"], "75750.00")
        self.assertEqual(selected["expiries_epoch_ms"], [1794211200000, 1794297600000])

        initial = read_bybit_reference(self.config.data_root, "options-BTC", START)
        changed = read_bybit_reference(self.config.data_root, "options-BTC", START + 60)
        self.assertEqual(len(initial["responses"]), 2)
        self.assertNotEqual(initial["content_sha256"], changed["content_sha256"])
        second_page = changed["responses"][1]
        self.assertEqual(second_page["parameters"]["cursor"], "second%3D")
        self.assertIsInstance(second_page["venue_server_epoch_ms"], int)
        self.assertIn(b"nextPageCursor", body_bytes(second_page))
        self.assertNotEqual(read_bybit_reference(self.config.data_root, "risk-BTC", START)["content_sha256"],
                            read_bybit_reference(self.config.data_root, "risk-BTC", START + 60)["content_sha256"])
        with self.assertRaises(FileNotFoundError):
            read_bybit_reference(self.config.data_root, "options-ETH", START + 60)
        with self.assertRaises(FileNotFoundError):
            read_bybit_reference(self.config.data_root, "risk-ETH", START + 60)

        settled = read_bybit_reference(self.config.data_root, "settlement-BTC-USDT", START + 180)
        self.assertEqual(body_bytes(settled["responses"][0]), FIXTURE["delivery-btc"].encode())
        self.assertEqual(sum(method == "new-delivery-price" for method, _ in self.bybit_calls), 4)

    async def test_failed_rollover_catalog_records_unavailable_depth_then_retries(self):
        self.change_hour = True
        self.fail_rollover_page = True
        await self.capture(3)

        rollover, records = read_minute(self.config.data_root, START + 60)
        self.assertEqual(rollover["bybit_selection_unavailable"], ["BTC"])
        self.assertEqual(rollover["bybit_selection"], {"ETH": [ETH_CALL]})
        self.assertEqual(rollover["requests"]["bybit:option-tickers:BTC"], "ok")
        self.assertEqual([row["logical_request_id"] for row in records
                          if row["logical_request_id"].startswith("bybit:depth:")], [f"bybit:depth:{ETH_CALL}"])
        failure = read_bybit_reference(self.config.data_root, "options-BTC", START + 60, failed=True)
        self.assertEqual(failure["status"], "error")
        self.assertEqual(len(failure["responses"]), 2)
        with self.assertRaises(FileNotFoundError):
            read_bybit_selection(self.config.data_root, START + 60, "BTC")

        recovered, _ = read_minute(self.config.data_root, START + 120)
        self.assertNotIn("bybit_selection_unavailable", recovered)
        self.assertEqual(recovered["bybit_selection"]["BTC"], [
            "BTC-9NOV26-75750-C-USDT", "BTC-9NOV26-75750-P-USDT",
        ])
        self.assertEqual(read_bybit_selection(self.config.data_root, START + 120, "BTC")["selected_slot_epoch"],
                         START + 120)

    async def test_recovery_materializes_selection_from_wal_before_readback(self):
        root = self.config.data_root

        def stop_after_plan(*_):
            context = read_json(root / "work" / str(START) / "deribit" / "context.json")
            self.assertEqual(context["bybit_selection"], {"BTC": [BTC_CALL, BTC_PUT], "ETH": [ETH_CALL]})
            self.assertFalse((root / "bybit" / "selection").exists())
            raise OSError("interrupted before publishing selection")

        with patch("data_collector.minute_bybit.publish_bybit_selections", side_effect=stop_after_plan):
            with self.assertRaises(ExceptionGroup):
                await self.capture(1)
        await self.capture(1)
        index, _ = read_minute(root, START)
        for base, symbols in index["bybit_selection"].items():
            self.assertEqual(read_bybit_selection(root, START, base)["symbols"], symbols)
            for symbol in symbols:
                self.assertEqual(index["requests"][f"bybit:depth:{symbol}"], "missed")
        self.assertEqual(index["bybit_selection"]["BTC"], [BTC_CALL, BTC_PUT])
        self.assertFalse(self.depth)

    async def test_sealed_copy_reads_bybit_selection_from_an_earlier_minute(self):
        first = int(datetime(2026, 11, 8, 23, 58, tzinfo=timezone.utc).timestamp())
        self.wall = first - 10
        await self.capture(2)
        root = self.config.data_root
        seal_minute_day(root, "2026-11-08")
        restored = root / "restored"
        shutil.copytree(root / "daily", restored / "daily")
        slot = first + 60
        self.assertEqual(read_minute(restored, slot), read_minute(root, slot))
        self.assertEqual(read_bybit_selection(restored, slot, "BTC")["selected_slot_epoch"], first)

    async def test_reference_revision_preserves_change_from_number_to_string(self):
        self.reference_type_change = True
        await self.capture(2)
        initial = read_bybit_reference(self.config.data_root, "options-BTC", START)
        changed = read_bybit_reference(self.config.data_root, "options-BTC", START + 60)
        self.assertNotEqual(initial["content_sha256"], changed["content_sha256"])
        self.assertIn(b'"fixtureValue": 0.1', body_bytes(initial["responses"][-1]))
        self.assertIn(b'"fixtureValue": "0.1"', body_bytes(changed["responses"][-1]))

    async def test_depth_outcomes_and_missed_grid_slot_remain_explicit(self):
        self.config = replace(self.config, attempt_seconds=.06)
        self.depth_faults = True
        self.wall = START + 60 - 10
        self.late = 60
        # The first boundary still runs. The following wakeup skips the next minute.
        original_sleep = self.sleep

        async def sleep(seconds):
            if self.wall < START + 60:
                self.wall += seconds
                self.clock += seconds
                await asyncio.sleep(0)
            else:
                await original_sleep(seconds)

        self.sleep = sleep
        await self.capture(3)
        index, records = read_minute(self.config.data_root, START + 60)
        self.assertEqual(index["requests"][f"bybit:depth:{BTC_PUT}"], "error")
        self.assertEqual(index["requests"][f"bybit:depth:{ETH_CALL}"], "timeout")
        error = next(row for row in records if row["logical_request_id"] == f"bybit:depth:{BTC_PUT}")
        self.assertEqual(error["venue_server_epoch_ms"], 1794124740200)
        risk = read_bybit_reference(self.config.data_root, "risk-ETH", START + 60, failed=True)
        self.assertEqual(risk["status"], "error")
        self.assertEqual(risk["responses"][0]["venue_server_epoch_ms"], 1794124740220)
        self.assertIn(b"risk error", body_bytes(risk["responses"][0]))
        gap, records = read_minute(self.config.data_root, START + 120)
        self.assertTrue(gap["gap"])
        self.assertEqual(gap["bybit_selection"], index["bybit_selection"])
        self.assertEqual(len(gap["requests"]), len(index["requests"]))
        self.assertEqual(set(gap["requests"].values()), {"missed"})
        self.assertEqual(records, [])

    async def test_default_depth_only_uses_two_nearest_expiries_per_base(self):
        await capture_minutes(
            self.config, minutes=1, bybit_base_url=self.url,
            wall=lambda: self.wall, clock=lambda: self.clock, sleep=self.sleep,
        )
        index, _ = read_minute(self.config.data_root, START)
        for base in ("BTC", "ETH"):
            selected = read_bybit_selection(self.config.data_root, START, base)
            self.assertEqual(selected["limit"], 50)
            self.assertEqual(selected["expiry_count"], 2)
            self.assertEqual(selected["expiries_epoch_ms"], [1794211200000, 1794297600000])
            self.assertEqual(len(index["bybit_selection"][base]), 8)
            self.assertTrue(all("-9NOV26-" in symbol or "-10NOV26-" in symbol
                                for symbol in index["bybit_selection"][base]))
