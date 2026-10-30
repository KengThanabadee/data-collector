"""Offline quality checks for the bounded free historical ticker probe."""
from collections import Counter
from datetime import datetime, timezone
import gzip
import hashlib
import json
import math
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]


def positive(value):
    return isinstance(value, (float, int)) and math.isfinite(value) and value > 0


def analyze():
    base = ROOT / "experiments/results/2026-10-22-research-free-quotes"
    summary = json.loads((base / "summary.json").read_text())
    rows = []
    for request in summary["rows"]:
        raw = (base / f"{request['id']:02d}.body").read_bytes()
        assert hashlib.sha256(raw).hexdigest() == request["body_sha256"]
        assert request["ok"] and request["http_status"] == 200
        start = datetime.fromisoformat(request["params"]["from"].replace("Z", "+00:00")).timestamp()
        selected = set(json.loads(request["params"]["filters"])[0]["symbols"])
        counts = Counter()
        latest = {}
        option_missing_inputs = 0
        invalid_bbo = 0
        future_exchange_timestamps = 0
        negative_receive_lag = 0
        disconnects = 0
        for line in gzip.decompress(raw).decode().splitlines():
            if not line.strip():
                disconnects += 1
                continue
            local, payload = line.split(" ", 1)
            receipt = datetime.fromisoformat(local.replace("Z", "+00:00")).timestamp()
            assert start <= receipt < start + 60
            message = json.loads(payload)
            data = message["params"]["data"]
            symbol = data["instrument_name"]
            assert symbol in selected
            event = data["timestamp"] / 1000
            future_exchange_timestamps += int(event >= start + 60)
            negative_receive_lag += int(event > receipt)
            valid = (all(positive(data.get(k)) for k in ("best_bid_price", "best_ask_price", "best_bid_amount", "best_ask_amount")) and data["best_bid_price"] <= data["best_ask_price"])
            invalid_bbo += int(not valid)
            option = symbol.endswith(("-C", "-P"))
            counts["option_messages" if option else "hedge_messages"] += 1
            if option:
                option_missing_inputs += int(not all(positive(data.get(k)) for k in ("bid_iv", "ask_iv", "mark_iv", "underlying_price", "index_price")))
            latest[symbol] = {"symbol": symbol, "option": option, "two_sided": valid, "ticker_age_seconds_at_minute_end": round(start + 60 - event, 6)}
        assert set(latest) == selected
        assert future_exchange_timestamps == 0
        rows.append({"from": request["params"]["from"], "symbols": len(latest), **counts, "invalid_or_missing_bbo_messages": invalid_bbo, "option_messages_missing_positive_iv_or_underlying": option_missing_inputs, "negative_receipt_lag_messages": negative_receive_lag, "disconnect_markers": disconnects, "last_tickers_two_sided": sum(x["two_sided"] for x in latest.values()), "max_last_ticker_age_seconds_at_minute_end": max(x["ticker_age_seconds_at_minute_end"] for x in latest.values()), "last_tickers": list(latest.values()), "wire_bytes": len(raw), "sha256_checked": True})
    return {"executed_utc": datetime.now(timezone.utc).isoformat(), "requests": len(rows), "messages": sum(r["option_messages"] + r["hedge_messages"] for r in rows), "rows": rows, "caveats": ["Three disjoint one-minute samples on one free day, not continuous annual coverage or statistical adequacy.", "Sixteen options were selected from saved within-hour trades for availability checks; this is forward-looking for decisions inside that hour. Later research use requires an explicit prior-period liquidity filter and limited target population.", "Ticker timestamps measure observation age, not time since the last BBO change or a guaranteed executable fill.", "Event and capture times both matter; the raw minute is selected by provider capture time."]}


if __name__ == "__main__":
    metrics = analyze()
    output = ROOT / "docs/research_quote_metrics.json"
    output.write_text(json.dumps(metrics, indent=2) + "\n")
    print(json.dumps({k: v for k, v in metrics.items() if k != "rows"}, indent=2))
    for row in metrics["rows"]:
        print(json.dumps({k: v for k, v in row.items() if k != "last_tickers"}))
