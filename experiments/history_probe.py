"""Bounded candles/retention probe; run AFTER live_probe, not concurrently.

Reports observed per-call counts and tested older windows, never an undocumented
global retention promise. Requests use a previous-UTC-day cutoff, but returned
bars can overlap that cutoff (the exchange uses an 08:00 UTC anchor for daily
and some intraday bars). A backtest must check candle close time separately.
"""
import argparse
import csv
from datetime import datetime, timezone
import json
import math
from pathlib import Path
import time

from archive_probe import Journal, atomic_json, decoded_body, digest, finalize, verify
from live_probe import Client, Gate, payload, utc

RESOLUTIONS = ("1", "3", "5", "10", "15", "30", "60", "120", "180", "360", "720", "1D")
DAY = 86400000


def iso(milliseconds):
    return datetime.fromtimestamp(milliseconds / 1000, timezone.utc).isoformat() if milliseconds is not None else None


def interval_ms(resolution):
    return DAY if resolution == "1D" else int(resolution) * 60000


def chart_summary(record, label):
    row = {"label": label, **record["params"], "api_ok": record.get("ok"), "http_status": record.get("http_status"), "api_error": record.get("api_error"), "latency_ms": record["latency_ms"], "body_bytes": record["response_bytes"]}
    if not record.get("ok"):
        return row
    result = payload(record)["result"]
    ticks = result.get("ticks", [])
    step = interval_ms(record["params"]["resolution"])
    row.update(status=result.get("status"), bars=len(ticks), unique_timestamps=len(set(ticks)),
               nominal_requested_intervals=math.ceil((record["params"]["end_timestamp"] - record["params"]["start_timestamp"] + 1) / step),
               first_utc=iso(min(ticks)) if ticks else None, last_utc=iso(max(ticks)) if ticks else None,
               nominal_bar_days=len(ticks) * step / DAY,
               observed_span_days=(max(ticks) - min(ticks) + step) / DAY if ticks else 0,
               zero_volume_bars=sum(value == 0 for value in result.get("volume", [])),
               array_lengths={key: len(result[key]) for key in ("ticks", "open", "high", "low", "close", "volume", "cost") if key in result},
               gap_intervals=sum((right - left) // step - 1 for left, right in zip(sorted(set(ticks)), sorted(set(ticks))[1:]) if right - left > step),
               starts_before_requested_start=sum(tick < record["params"]["start_timestamp"] for tick in ticks),
               starts_after_requested_end=sum(tick > record["params"]["end_timestamp"] for tick in ticks),
               not_closed_at_requested_cutoff=sum(tick + step - 1 > record["params"]["end_timestamp"] for tick in ticks),
               not_closed_when_received=sum(tick + step > record["response_received_epoch_ms"] for tick in ticks) if "response_received_epoch_ms" in record else None,
               timestamp_offsets_mod_interval_ms=sorted({tick % step for tick in ticks}),
               last_close_utc=iso(max(ticks) + step) if ticks else None)
    return row


class HistoryProbe:
    def __init__(self, output, source):
        self.output = output
        output.mkdir(parents=True, exist_ok=False)
        self.journal = Journal(output / "raw", {"purpose": "history-window-probe", "started_utc": utc(), "source": str(source), "resolutions": RESOLUTIONS, "code_sha256": digest(Path(__file__).read_bytes())})
        self.gate = Gate(1)
        self.client = Client(self.gate)
        self.rows = []
        self.end = int(time.time() * 1000) // DAY * DAY - 1
        self.source_records = verify(source / "initial-discovery")[1]
        self.subjects = []
        for currency in ("BTC", "ETH"):
            future_metadata = next(payload(record)["result"] for record in self.source_records if record["method"] == "public/get_instruments" and record["params"]["kind"] == "future" and record["currency"] == currency)
            perp = next(value for value in future_metadata if value["instrument_name"] == currency + "-PERPETUAL")
            self.subjects.append({"instrument_name": perp["instrument_name"], "creation_timestamp": perp["creation_timestamp"], "kind": "perpetual"})
            option_metadata = next(payload(record)["result"] for record in self.source_records if record["method"] == "public/get_instruments" and record["params"]["kind"] == "option" and record["currency"] == currency)
            summary = next(payload(record)["result"] for record in self.source_records if record["method"] == "public/get_book_summary_by_currency" and record["params"]["kind"] == "option" and record["currency"] == currency)
            metadata = {value["instrument_name"]: value for value in option_metadata}
            candidates = [value for value in summary if value["instrument_name"] in metadata and metadata[value["instrument_name"]].get("state") == "open"]
            # Choose an older liquid option instead of a newly listed daily one.
            candidates.sort(key=lambda value: value.get("open_interest", 0), reverse=True)
            eligible = [value for value in candidates[:50] if metadata[value["instrument_name"]]["creation_timestamp"] < self.end - 30 * DAY]
            selected = (eligible or candidates)[0]
            instrument = metadata[selected["instrument_name"]]
            self.subjects.append({"instrument_name": instrument["instrument_name"], "creation_timestamp": instrument["creation_timestamp"], "expiration_timestamp": instrument["expiration_timestamp"], "kind": "option", "selection": "highest OI among top50 with >30day listing age, if present"})
        atomic_json(output / "subjects.json", {"end_utc": iso(self.end), "subjects": self.subjects})

    def call(self, instrument, resolution, start, end, label):
        params = {"instrument_name": instrument, "resolution": resolution, "start_timestamp": int(start), "end_timestamp": int(end)}
        record = self.client.get("public/get_tradingview_chart_data", params, instrument.split("-")[0])
        if record is None:
            return None
        self.journal.append(record)
        row = chart_summary(record, label)
        self.rows.append(row)
        atomic_json(self.output / "summary.json", {"updated_utc": utc(), "end_utc": iso(self.end), "subjects": self.subjects, "rows": self.rows})
        print(json.dumps({key: row.get(key) for key in ("label", "instrument_name", "resolution", "nominal_requested_intervals", "bars", "status", "first_utc", "last_utc", "api_error")}), flush=True)
        if not record.get("ok"):
            error = record.get("api_error") or {}
            # Window-size/argument rejection is an expected experimental result,
            # not permission to ignore throttling or transport failures.
            if not record.get("throttled") and not record.get("transport_error") and error.get("code") in (10020, -32602):
                self.gate.abort.clear()
        return row

    def run(self):
        # Escalating window sizes on one liquid market identifies a cap without
        # requesting an unbounded decade of minute candles in one response.
        for bars in (1000, 5000, 10000, 20000, 50000, 100000):
            row = self.call("BTC-PERPETUAL", "1", self.end + 1 - bars * 60000, self.end, "window-cap")
            if self.gate.abort.is_set() or row is None or not row.get("api_ok") or row.get("bars", 0) < bars:
                break
        if self.gate.abort.is_set():
            return self.finish()
        for subject in self.subjects:
            name = subject["instrument_name"]
            for resolution in RESOLUTIONS:
                step = interval_ms(resolution)
                # Maximum 20,000 requested bars, and no pre-2016 windows.
                floor_2016 = 1451606400000
                start = max(floor_2016, self.end + 1 - 20000 * step)
                row = self.call(name, resolution, start, self.end, "recent-wide")
                if self.gate.abort.is_set():
                    return self.finish()
                if row and row.get("api_ok") and row.get("bars"):
                    first = int(datetime.fromisoformat(row["first_utc"]).timestamp() * 1000)
                    # Probe immediately before the returned window. If this
                    # succeeds, a per-call cap is not a total-history boundary.
                    if first > max(start, subject["creation_timestamp"]) + 2 * step:
                        self.call(name, resolution, max(floor_2016, first - 1000 * step), first - 1, "older-adjacent")
                if self.gate.abort.is_set():
                    return self.finish()
                early = subject["creation_timestamp"] // step * step
                if early + step < self.end:
                    self.call(name, resolution, early, min(self.end, early + 1000 * step - 1), "near-listing")
                if self.gate.abort.is_set():
                    return self.finish()
        # Test the documented limited option-only mark-history capability.
        for subject in self.subjects:
            if subject["kind"] != "option":
                continue
            record = self.client.get("public/get_mark_price_history", {"instrument_name": subject["instrument_name"], "start_timestamp": self.end - 7 * DAY + 1, "end_timestamp": self.end}, subject["instrument_name"].split("-")[0])
            if record is not None:
                self.journal.append(record)
                result = payload(record).get("result", []) if record.get("ok") else []
                atomic_json(self.output / (subject["instrument_name"].split("-")[0] + "-mark-history.json"), {"instrument_name": subject["instrument_name"], "requested_days": 7, "points": len(result), "first_utc": iso(min(value[0] for value in result)) if result else None, "last_utc": iso(max(value[0] for value in result)) if result else None, "api_error": record.get("api_error")})
            if self.gate.abort.is_set():
                break
        return self.finish()

    def finish(self):
        self.journal.close()
        manifest = finalize(self.output / "raw", status="experiment-finished" if not self.gate.abort.is_set() else "experiment-stopped")
        atomic_json(self.output / "finished.json", {"finished_utc": utc(), "aborted": self.gate.abort.is_set(), "chart_requests": len(self.rows), "archive": manifest})
        if self.rows:
            keys = sorted({key for row in self.rows for key in row})
            with (self.output / "candles.csv").open("w", newline="") as output:
                writer = csv.DictWriter(output, fieldnames=keys)
                writer.writeheader()
                writer.writerows(self.rows)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--source", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    HistoryProbe(args.output, args.source).run()
