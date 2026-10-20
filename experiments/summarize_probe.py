"""Read saved evidence without making requests; generate inspectable metrics."""
import argparse
from collections import Counter
from datetime import datetime
import json
from pathlib import Path
import statistics

from archive_probe import atomic_json, verify
from live_probe import payload
from history_probe import chart_summary


def analyze(live, history=None, delivery_source=None):
    stages = json.loads((live / "summary.json").read_text())["stages"]
    full = [stage for stage in stages if stage["name"].endswith("-full")]
    if delivery_source:
        corrected = json.loads((delivery_source / "summary.json").read_text())["stages"]
        stages = [stage for stage in stages if stage["name"] != "delivery-history"] + corrected
    rounds = []
    for stage in full:
        prefix = stage["name"].removesuffix("-full")
        discovery = next(value for value in stages if value["name"] == prefix + "-discovery")
        _, books = verify(live / stage["name"])
        _, references = verify(live / discovery["name"])
        all_records = books + references
        book_values = [payload(record)["result"] for record in books if record.get("ok")]
        begin = min(record["request_started_epoch_ms"] for record in all_records)
        end = max(record["response_received_epoch_ms"] for record in all_records)
        rounds.append({"name": stage["name"], "books": stage["successful_requests"], "expected_books": stage["expected_jobs"],
                       "complete": stage["complete"], "errors": stage["errors"] + discovery["errors"],
                       "requests_with_references": len(all_records), "wall_span_with_references_seconds": (end - begin) / 1000,
                       "books_elapsed_seconds": stage["elapsed_seconds"], "markets": stage["markets"],
                       "response_body_bytes": sum(record["response_bytes"] for record in all_records),
                       "journal_bytes": stage["archive"]["journal_bytes"] + discovery["archive"]["journal_bytes"],
                       "gzip_bytes": stage["archive"]["archive_bytes"] + discovery["archive"]["archive_bytes"],
                       "fsync_seconds": stage["fsync_seconds"] + discovery["fsync_seconds"],
                       "finalize_seconds": stage["finalize_seconds"] + discovery["finalize_seconds"],
                       "unique_book_ids": len({record["params"]["instrument_name"] for record in books}),
                       "books_with_more_than20_levels": sum(max(len(value["bids"]), len(value["asks"])) > 20 for value in book_values),
                       "max_levels_per_side": max((max(len(value["bids"]), len(value["asks"])) for value in book_values), default=0),
                       "reused_requests": sum(record.get("reused_connection", False) for record in all_records)})
    result = {"experiment_label": "depth-10000-followup" if "depth10000" in str(live) else "depth-20-baseline", "local_rounds": rounds, "pilots": [{key: stage[key] for key in ("name", "requests", "elapsed_seconds", "rate_limit_rps", "throttles", "latency_ms", "connection_reuse_requests", "complete")} for stage in stages if stage["name"].startswith("pilot-")]}
    if rounds:
        averages = {key: statistics.mean(value[key] for value in rounds) for key in ("requests_with_references", "response_body_bytes", "journal_bytes", "gzip_bytes")}
        daily_delivery = next((stage for stage in stages if stage["name"] == "delivery-history"), None)
        result["extrapolation_not_measured_retention"] = {
            "basis_rounds": len(rounds), "rounds_per_day": 288,
            "requests_per_day": averages["requests_with_references"] * 288 + (daily_delivery["requests"] if daily_delivery else 0),
            "response_body_gib_per_day_lower_bound": (averages["response_body_bytes"] * 288 + (daily_delivery["response_body_bytes"] if daily_delivery else 0)) / 2**30,
            "archive_mib_per_day": (averages["gzip_bytes"] * 288 + (daily_delivery["archive"]["archive_bytes"] if daily_delivery else 0)) / 2**20,
            "archive_gib_per_year": (averages["gzip_bytes"] * 288 + (daily_delivery["archive"]["archive_bytes"] if daily_delivery else 0)) * 365 / 2**30,
            "note": "Short local samples. Body bytes exclude headers/TLS. No proof of future load, storage or VPS allowance. Context/manifests/filesystem blocks and logs are additional."}
    depth = next((stage for stage in stages if stage["name"] == "depth-comparison"), None)
    if depth:
        _, records = verify(live / depth["name"])
        pairs = {}
        for record in records:
            if record.get("ok"):
                book = payload(record)["result"]
                pairs.setdefault(book["instrument_name"], {})[str(record["params"]["depth"])] = {"bid_levels": len(book["bids"]), "ask_levels": len(book["asks"]), "body_bytes": record["response_bytes"], "latency_ms": record["latency_ms"], "exchange_timestamp": book["timestamp"]}
        result["depth_sample"] = {"pairs": pairs, "instruments": len(pairs), "sample_selection": "dispersed names enriched with observed most populated books; not representative of whole universe", "pairs_over_20_levels_in_deeper_response": sum(max(value.get("10000", {}).get("bid_levels", 0), value.get("10000", {}).get("ask_levels", 0)) > 20 for value in pairs.values()), "not_simultaneous": True}
    delivery = next((stage for stage in stages if stage["name"] == "delivery-history"), None)
    if delivery:
        result["delivery"] = delivery["coverage"]
    if history and (history / "summary.json").exists():
        chart = json.loads((history / "summary.json").read_text())
        # Recompute timestamp semantics from exact raw bodies after discovering
        # the 08:00 anchor. Preserve original summaries as experimental evidence.
        _, records = verify(history / "raw")
        rows = []
        for record in records:
            if record["method"] == "public/get_tradingview_chart_data":
                original = chart["rows"][len(rows)]
                assert all(record["params"][key] == original[key] for key in record["params"])
                rows.append(chart_summary(record, original["label"]))
        chart["rows"] = rows
        result["chart_subjects"] = chart["subjects"]
        result["candle_cap_probes"] = [row for row in chart["rows"] if row["label"] == "window-cap"]
        result["candle_recent_wide"] = [row for row in chart["rows"] if row["label"] == "recent-wide"]
        result["candle_near_listing"] = [row for row in chart["rows"] if row["label"] == "near-listing"]
        result["candle_older_adjacent"] = [row for row in chart["rows"] if row["label"] == "older-adjacent"]
        result["chart_analysis_sha256"] = __import__("hashlib").sha256(Path(__file__).with_name("history_probe.py").read_bytes()).hexdigest()
    return result


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--live", type=Path, required=True)
    parser.add_argument("--history", type=Path)
    parser.add_argument("--delivery", type=Path)
    parser.add_argument("--deep", type=Path)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    args.output.parent.mkdir(parents=True, exist_ok=True)
    result = analyze(args.live, args.history, args.delivery)
    if args.deep:
        result["depth10000_full_experiment"] = analyze(args.deep, delivery_source=args.delivery)
    atomic_json(args.output, result)
    print(json.dumps({key: value for key, value in result.items() if key in ("local_rounds", "extrapolation_not_measured_retention", "delivery")}, indent=2))
