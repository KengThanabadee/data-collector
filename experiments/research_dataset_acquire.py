"""Acquire a predeclared monthly panel, selecting a small cohort causally.

One full-channel seed minute per date establishes the historical BTC universe.
Two expiries near 14/28 days and nine nearest strikes each are selected at 08UTC.
Both option types and the actual inverse perpetual are replayed at later times.
This is a sampled-day experiment, not a continuous annual archive.
"""
import asyncio
from datetime import datetime, timezone
import gzip
import hashlib
import io
import json
import math
from pathlib import Path
import shutil
import time
import aiohttp
from research_panel_states import contract, extract

ROOT = Path(__file__).resolve().parents[1]
OUT = ROOT / "experiments/results/2026-10-22-research-monthly-panel"
PLAN = ROOT / "experiments/research_monthly_plan.json"
BASE = "https://api.tardis.dev/v1/data-feeds/deribit"


def utc():
    return datetime.now(timezone.utc).isoformat()


def write(path, value):
    path.write_text(json.dumps(value, indent=2) + "\n")


def select_cohort(state):
    groups = {}
    for name, item in state["quotes"].items():
        c = contract(name)
        if not c:
            continue
        days = (c["expiry"] - state["decision"]) / 86400
        forward = item["data"].get("underlying_price")
        if not 7 <= days <= 45 or not forward or not .8 <= c["strike"] / forward <= 1.2:
            continue
        groups.setdefault(c["expiry"], []).append((name, c["strike"], forward))
    selected_expiries = []
    for target in (14, 28):
        candidates = [x for x in groups if x not in selected_expiries]
        if candidates:
            selected_expiries.append(min(candidates, key=lambda x: abs((x - state["decision"]) / 86400 - target)))
    names = ["BTC-PERPETUAL"]
    for expiry in selected_expiries:
        entries = groups[expiry]
        ranked = sorted({k for _, k, _ in entries}, key=lambda k: min(abs(math.log(k / f)) for _, _, f in entries))[:9]
        names.extend(name for name, k, _ in entries if k in ranked)
    assert len(set(names)) <= 50, "single-filter budget"
    return sorted(set(names))


async def run():
    plan_bytes = PLAN.read_bytes()
    plan = json.loads(plan_bytes)
    OUT.mkdir(parents=True, exist_ok=True)
    context = OUT / "context.json"
    digest = hashlib.sha256(plan_bytes).hexdigest()
    if context.exists():
        assert json.loads(context.read_text())["plan_sha256"] == digest
    else:
        write(context, {"started_utc": utc(), "plan": plan, "plan_sha256": digest, "source_sha256": hashlib.sha256(Path(__file__).read_bytes()).hexdigest()})
        shutil.copyfile(Path(__file__), OUT / "acquisition-source.py")
    existing = {}
    for directory in plan["reuse_directories"]:
        for path in (ROOT / directory).glob("*.request.json"):
            meta = json.loads(path.read_text())
            if meta.get("ok"):
                existing[meta["params"]["from"]] = path
    total_wire = 0
    new_calls = 0
    rows = []
    last = 0
    async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=60), auto_decompress=False, trust_env=False) as client:
        for day in plan["dates"]:
            symbols = None
            for hour in ("07:59", "08:59", "15:59", "16:59"):
                start = day + "T" + hour + ":00.000Z"
                key = day.replace("-", "") + "T" + hour.replace(":", "")
                meta_path = OUT / (key + ".request.json")
                body_path = OUT / (key + ".body")
                if not meta_path.exists() and start in existing:
                    source = existing[start]
                    shutil.copyfile(source, meta_path)
                    shutil.copyfile(source.with_name(source.name.replace(".request.json", ".body")), body_path)
                if meta_path.exists():
                    rec = json.loads(meta_path.read_text())
                    assert rec["params"]["from"] == start
                    assert hashlib.sha256(body_path.read_bytes()).hexdigest() == rec["body_sha256"]
                    if not rec.get("ok"):
                        raise RuntimeError("saved failed response; inspect before any retry")
                else:
                    if new_calls >= plan["max_new_requests"] or total_wire >= plan["max_total_wire_bytes"]:
                        raise RuntimeError("plan budget")
                    filters = [{"channel": "ticker"}]
                    if symbols is not None:
                        filters[0]["symbols"] = symbols
                    params = {"from": start, "filters": json.dumps(filters), "compression": "gzip"}
                    await asyncio.sleep(max(0, 1 - (time.monotonic() - last)))
                    last = time.monotonic()
                    new_calls += 1
                    rec = {"endpoint": BASE, "params": params, "request_started_utc": utc()}
                    body = bytearray()
                    try:
                        async with client.get(BASE, params=params, headers={"Accept-Encoding": "gzip"}, allow_redirects=False) as response:
                            rec["http_status"] = response.status
                            rec["content_encoding"] = response.headers.get("Content-Encoding")
                            async for chunk in response.content.iter_chunked(65536):
                                if len(body) + len(chunk) > plan["max_response_wire_bytes"] or total_wire + len(body) + len(chunk) > plan["max_total_wire_bytes"]:
                                    raise ValueError("wire cap")
                                body.extend(chunk)
                        if rec["http_status"] != 200:
                            rec.update(ok=False, error_body=bytes(body[:1000]).decode(errors="replace"))
                        else:
                            decoded = 0
                            with gzip.GzipFile(fileobj=io.BytesIO(body)) as stream:
                                for line in stream:
                                    decoded += len(line)
                                    if decoded > plan["max_response_decoded_bytes"]:
                                        raise ValueError("decoded cap")
                            rec.update(ok=True, decoded_bytes=decoded)
                    except (aiohttp.ClientError, TimeoutError, ValueError, OSError) as exc:
                        rec.update(ok=False, error=str(exc))
                    rec.update(response_received_utc=utc(), body_bytes=len(body), body_sha256=hashlib.sha256(body).hexdigest())
                    body_path.write_bytes(body)
                    write(meta_path, rec)
                total_wire += rec["body_bytes"]
                rows.append(rec)
                write(OUT / "summary.json", {"requests": len(rows), "wire_bytes": total_wire, "rows": rows})
                if not rec.get("ok"):
                    raise RuntimeError("stop on request failure")
                if hour == "07:59":
                    state = extract(OUT, only=meta_path.name)[0]
                    symbols = select_cohort(state)
                    write(OUT / (day + ".cohort.json"), {"selection_time": state["decision"], "seed_sha256": state["source_sha256"], "symbols": symbols, "selection_rule": "historical seed only; expiry nearest14/28days within7-45; nearest9strikes each; both call/put"})
            print(json.dumps({"day": day, "samples": len(rows), "selected_symbols": len(symbols), "new_requests_this_run": new_calls, "wire_megabytes": round(total_wire / 1e6, 2)}), flush=True)
    write(OUT / "finished.json", {"requests": len(rows), "finished_utc": utc(), "wire_bytes": total_wire})


if __name__ == "__main__":
    asyncio.run(run())
