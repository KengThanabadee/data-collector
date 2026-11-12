"""Response validation and coverage for the minute grid, independent of quote quality."""
from collections import Counter
import json

from .minute_files import body_bytes, dumps, sha


def payload(record):
    return json.loads(body_bytes(record))


def validate(method, params, result):
    if method == "public/get_instruments":
        if not isinstance(result, list):
            raise ValueError("invalid instrument list")
    elif method == "public/get_book_summary_by_currency":
        if not isinstance(result, list) or any(not isinstance(x, dict) or not isinstance(x.get("instrument_name"), str) for x in result):
            raise ValueError("invalid summaries")
    elif method == "public/get_index_price":
        if not isinstance(result, dict) or not isinstance(result.get("index_price"), (int, float)) or result["index_price"] <= 0:
            raise ValueError("invalid index")
    elif method == "public/ticker":
        if (not isinstance(result, dict) or result.get("instrument_name") != params["instrument_name"]
                or type(result.get("timestamp")) is not int):
            raise ValueError("ticker identity/timestamp mismatch")
    else:
        raise ValueError("method not allowed")


def capture_span(records, expected):
    successes = [r for r in records if r["ok"] and r["logical_request_id"] in expected]
    if not successes:
        return None
    discovery = [r for r in records if r["logical_request_id"] == "metadata:option"]
    starts = discovery or records
    return (max(r["response_received_epoch_ms"] for r in successes)
            - min(r["request_started_epoch_ms"] for r in starts))/1000


def coverage(context, records):
    if context["job_kind"] != "minute":
        raise ValueError("only minute archives are supported")
    successes = {}
    flags = Counter()
    for record in records:
        if record["ok"]:
            successes[record["logical_request_id"]] = record["record_id"]
        flags.update(record.get("quality_flags", []))
    expected = context.get("required_jobs", [])
    missing = sorted(set(expected) - successes.keys())
    status = "complete" if expected and not missing else "partial" if successes else "failed"
    times = [r for r in records if r["ok"]]
    span = capture_span(records, expected) if context.get("capture_span_version",1)>=2 else (
        (max(r["response_received_epoch_ms"] for r in times)-min(r["request_started_epoch_ms"] for r in times))/1000 if times else None)
    return dict(status=status, expected_jobs=expected, selected_success_ids={k: successes[k] for k in expected if k in successes},
                missing_jobs=missing, attempts=len(records), quality_counts=dict(flags),
                frozen_universe_sha256=sha(dumps(context.get("universe", []))),
                capture_span_seconds=span)
