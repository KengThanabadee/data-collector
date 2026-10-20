"""Offline observable transport behavior; no live API requests."""
import json
import threading
import time
from pathlib import Path
import tempfile
from datetime import date, timedelta
import unittest
from unittest.mock import patch

from archive_probe import Journal, decoded_body, digest, encoded_body
from live_probe import Client, Experiment, Gate
from history_probe import chart_summary


class Response:
    will_close = False
    def __init__(self, status, body):
        self.status, self.body = status, body
    def getheaders(self):
        return [("Content-Type", "text/plain")]
    def read(self, _):
        return self.body


class FakeConnection:
    sock = None
    def __init__(self, response):
        self.response = response
    def request(self, *_args, **_kwargs):
        pass
    def getresponse(self):
        return self.response
    def close(self):
        pass


class LiveProbeTests(unittest.TestCase):
    def fake_request(self, status, body):
        gate = Gate(10)
        client = Client(gate)
        with patch("live_probe.http.client.HTTPSConnection", return_value=FakeConnection(Response(status, body))):
            record = client.get("public/example", {})
        return gate, record

    def test_jsonrpc_rate_limit_under_http200_stops(self):
        body = b'{"jsonrpc":"2.0","error":{"code":10028,"message":"too_many_requests"}}'
        gate, record = self.fake_request(200, body)
        self.assertFalse(record["ok"])
        self.assertTrue(record["throttled"])
        self.assertTrue(gate.abort.is_set())
        self.assertEqual(decoded_body(record), body)

    def test_http429_non_json_also_counts_as_throttling(self):
        body = b"temporarily rate limited"
        gate, record = self.fake_request(429, body)
        self.assertTrue(record["throttled"])
        self.assertTrue(gate.abort.is_set())
        self.assertEqual(decoded_body(record), body)

    def test_no_request_admitted_past_deadline(self):
        gate = Gate(1)
        self.assertFalse(gate.acquire(deadline=time.monotonic() - 1))
        gate.abort.set()
        self.assertFalse(gate.acquire())

    def test_missing_jobs_cannot_be_published_as_captured(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            experiment = Experiment(root)
            stage = root / "incomplete"
            journal = Journal(stage, {"expected_jobs": 2})
            record = {"ok": True, "method": "public/example", "currency": "BTC", "response_bytes": 0, "latency_ms": 1}
            journal.append(record)
            summary = experiment.save_stage(stage, journal, [record], 1, {"complete": False, "expected_jobs": 2})
            self.assertEqual(summary["archive"]["status"], "partial")

    def test_delivery_short_pages_continue_to_advertised_total(self):
        offsets = []
        class PagedClient:
            def get(self, method, params, currency):
                offset = params["offset"]
                offsets.append((currency, offset))
                dates = [(date(2020, 1, 1) + timedelta(days=index)).isoformat() for index in range(250)]
                data = [{"date": value, "delivery_price": 1} for value in dates[offset:offset + 100]]
                body = json.dumps({"result": {"data": data, "records_total": 250}}).encode()
                return dict(encoded_body(body), body_sha256=digest(body), ok=True, method=method, params=params, currency=currency, response_bytes=len(body), latency_ms=1)
        with tempfile.TemporaryDirectory() as temporary:
            experiment = Experiment(Path(temporary))
            experiment.client = PagedClient()
            summary = experiment.delivery()
            self.assertTrue(summary["complete"])
            self.assertEqual(offsets, [(currency, offset) for currency in ("BTC", "ETH") for offset in (0, 100, 200)])
            self.assertEqual(summary["coverage"]["BTC"]["records"], 250)

    def test_chart_counts_differ_from_span_when_there_is_a_gap(self):
        result = {"status": "ok", "ticks": [0, 60000, 180000], "volume": [1, 0, 1], "open": [1, 1, 1], "close": [1, 1, 1], "high": [1, 1, 1], "low": [1, 1, 1]}
        record = {"ok": True, "http_status": 200, "latency_ms": 1, "response_bytes": 1, "params": {"instrument_name": "test", "resolution": "1", "start_timestamp": 0, "end_timestamp": 239999}, "body_encoding": "utf-8", "raw_body": json.dumps({"result": result})}
        row = chart_summary(record, "fixture")
        self.assertEqual(row["bars"], 3)
        self.assertEqual(row["nominal_requested_intervals"], 4)
        self.assertEqual(row["gap_intervals"], 1)
        self.assertEqual(row["zero_volume_bars"], 1)
        self.assertAlmostEqual(row["observed_span_days"], 4 / 1440)
        self.assertAlmostEqual(row["nominal_bar_days"], 3 / 1440)

    def test_daily_0800_candle_can_cross_requested_midnight(self):
        from datetime import datetime, timezone
        start = int(datetime(2026, 10, 17, tzinfo=timezone.utc).timestamp() * 1000)
        tick = start + 8 * 3600000
        body = json.dumps({"result": {"status": "ok", "ticks": [tick], "volume": [0]}}).encode()
        record = dict(encoded_body(body), ok=True, http_status=200, latency_ms=1, response_bytes=len(body), response_received_epoch_ms=start + 2 * 86400000, params={"instrument_name": "test", "resolution": "1D", "start_timestamp": start, "end_timestamp": start + 86400000 - 1})
        row = chart_summary(record, "fixture")
        self.assertEqual(row["not_closed_at_requested_cutoff"], 1)
        self.assertEqual(row["not_closed_when_received"], 0)
        self.assertEqual(row["timestamp_offsets_mod_interval_ms"], [8 * 3600000])


if __name__ == "__main__":
    unittest.main(verbosity=2)
