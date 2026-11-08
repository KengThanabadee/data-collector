"""Validation and coverage are independent of quote quality."""
from collections import Counter
import json
from .common import body_bytes, dumps, sha
from .config import VISIBLE

KNOWN_STATES = set(VISIBLE) | {"inactive", "delivered", "archivized"}


def payload(record):
    return json.loads(body_bytes(record))


def universe(instruments, currency):
    if not isinstance(instruments, list) or not instruments:
        raise ValueError("empty/invalid discovery")
    names, selected, excluded = set(), [], []
    for item in instruments:
        if not isinstance(item, dict):
            raise ValueError("invalid instrument")
        name = item.get("instrument_name")
        if not isinstance(name, str) or not name or name in names:
            raise ValueError("missing/duplicate instrument name")
        names.add(name)
        if item.get("kind") != "option" or item.get("settlement_currency") != currency or item.get("state") not in KNOWN_STATES:
            raise ValueError("untrusted instrument kind/currency/state")
        if item["state"] not in VISIBLE:
            excluded.append({"instrument_name": name, "reason": item["state"]})
            continue
        if (not isinstance(item.get("expiration_timestamp"), int) or
                not isinstance(item.get("strike"), (int, float)) or item.get("option_type") not in ("call", "put")):
            raise ValueError("incomplete option metadata")
        selected.append(name)
    if not selected:
        raise ValueError("no visible options")
    return sorted(selected), excluded


def validate(method, params, result):
    if method == "public/get_order_book":
        if not isinstance(result, dict) or result.get("instrument_name") != params["instrument_name"] or not isinstance(result.get("timestamp"), int):
            raise ValueError("book identity/timestamp mismatch")
        for side in ("bids", "asks"):
            if not isinstance(result.get(side), list) or any(not isinstance(x, list) or len(x) != 2 or any(isinstance(v, bool) or not isinstance(v, (int, float)) for v in x) for x in result[side]):
                raise ValueError("invalid price levels")
    elif method == "public/get_instruments":
        if not isinstance(result, list):
            raise ValueError("invalid instrument list")
        if params["kind"] == "option":
            universe(result, params["currency"])
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
    elif method == "public/get_delivery_prices":
        if not isinstance(result, dict) or not isinstance(result.get("records_total"), int) or result["records_total"] < 0 or not isinstance(result.get("data"), list):
            raise ValueError("invalid delivery page")
        from datetime import date
        for x in result["data"]:
            date.fromisoformat(x["date"])
            if not isinstance(x["delivery_price"], (int, float)) or x["delivery_price"] <= 0:
                raise ValueError("invalid delivery price")
    else:
        raise ValueError("method not allowed")


def quality(result, depth):
    flags = []
    for side in ("bids", "asks"):
        if not result[side]:
            flags.append("empty_" + side)
        if len(result[side]) >= depth:
            flags.append("depth_cap_reached")
    for field in ("bid_iv", "ask_iv", "mark_iv"):
        if result.get(field) is None:
            flags.append(field + "_missing")
        elif result[field] == 0:
            flags.append(field + "_zero")
    if result["bids"] and result["asks"] and result["bids"][0][0] > result["asks"][0][0]:
        flags.append("crossed_book")
    return sorted(set(flags))


def capture_span(records, expected):
    successes = [r for r in records if r["ok"] and r["logical_request_id"] in expected]
    if not successes:
        return None
    discovery = [r for r in records if r["logical_request_id"] == "metadata:option"]
    starts = discovery or records
    return (max(r["response_received_epoch_ms"] for r in successes)
            - min(r["request_started_epoch_ms"] for r in starts))/1000


def coverage(context, records):
    successes = {}
    flags = Counter()
    for record in records:
        if record["ok"]:
            successes[record["logical_request_id"]] = record["record_id"]
        flags.update(record.get("quality_flags", []))
    expected = context.get("required_jobs", [])
    if context["job_kind"] == "snapshot":
        expected = expected + ["book:" + name for name in context.get("universe", [])]
        trusted = context.get("universe_trusted", False)
        if trusted:
            discovery = next((r for r in records if r["record_id"] == successes.get("metadata:option")), None)
            if discovery is None or universe(payload(discovery)["result"], context["currency"])[0] != context["universe"]:
                raise ValueError("frozen universe differs from raw discovery")
    elif context["job_kind"] == "minute":
        trusted = True
    else:
        trusted = context.get("pagination_complete", False)
        if trusted:
            from .delivery import progress
            offset,total=progress(records)
            if total is None or offset!=total or total!=context.get("records_total") or offset!=context.get("next_offset"):
                raise ValueError("delivery coverage mismatch")
    missing = sorted(set(expected) - successes.keys())
    status = "complete" if trusted and expected and not missing else "partial" if successes else "failed"
    times = [r for r in records if r["ok"]]
    span = capture_span(records, expected) if context.get("capture_span_version",1)>=2 else (
        (max(r["response_received_epoch_ms"] for r in times)-min(r["request_started_epoch_ms"] for r in times))/1000 if times else None)
    return dict(status=status, expected_jobs=expected, selected_success_ids={k: successes[k] for k in expected if k in successes},
                missing_jobs=missing, attempts=len(records), quality_counts=dict(flags),
                frozen_universe_sha256=sha(dumps(context.get("universe", []))),
                capture_span_seconds=span)
