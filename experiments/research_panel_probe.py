"""Bounded, resumable public replay sampling for research-input sufficiency.

Every request is one complete raw ticker minute, without today's symbol list.
Use a predeclared JSON plan. Saves original gzip bytes and request metadata.
No retry, key, subscription, trading or production collector is involved.
"""
import argparse
import asyncio
from datetime import datetime, timezone
import gzip
import hashlib
import io
import json
from pathlib import Path
import time
import aiohttp

ROOT = Path(__file__).resolve().parents[1]
BASE = "https://api.tardis.dev/v1/data-feeds/deribit"


def utc():
    return datetime.now(timezone.utc).isoformat()


def write(path, value):
    path.write_text(json.dumps(value, indent=2) + "\n")


async def run(plan_path):
    plan_bytes = plan_path.read_bytes()
    plan = json.loads(plan_bytes)
    out = ROOT / plan["output"]
    out.mkdir(parents=True, exist_ok=True)
    context = out / "context.json"
    plan_hash = hashlib.sha256(plan_bytes).hexdigest()
    if context.exists():
        assert json.loads(context.read_text())["plan_sha256"] == plan_hash
    else:
        write(context, {"started_utc": utc(), "plan_sha256": plan_hash, "plan": plan, "source_sha256": hashlib.sha256(Path(__file__).read_bytes()).hexdigest()})
    rows = []
    total_bytes = 0
    last_start = 0
    async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=60), auto_decompress=False, trust_env=False) as client:
        for i, start in enumerate(plan["minutes"], 1):
            assert start[8:10] == "01", "free first-of-month samples only"
            body_path = out / f"{i:04d}.body"
            meta_path = out / f"{i:04d}.request.json"
            if meta_path.exists():
                rec = json.loads(meta_path.read_text())
                assert rec["params"]["from"] == start
                assert hashlib.sha256(body_path.read_bytes()).hexdigest() == rec["body_sha256"]
                rows.append(rec)
                total_bytes += rec["body_bytes"]
                if not rec.get("ok"):
                    raise RuntimeError("saved failed response; inspect rather than retry")
                continue
            if i > plan["max_requests"]:
                raise RuntimeError("request cap")
            if total_bytes >= plan["max_total_wire_bytes"]:
                raise RuntimeError("total byte cap")
            await asyncio.sleep(max(0, 1 - (time.monotonic() - last_start)))
            last_start = time.monotonic()
            params = {"from": start, "filters": json.dumps([{"channel": "ticker"}]), "compression": "gzip"}
            rec = {"id": i, "endpoint": BASE, "params": params, "request_started_utc": utc()}
            body = bytearray()
            try:
                async with client.get(BASE, params=params, headers={"Accept-Encoding": "gzip"}, allow_redirects=False) as response:
                    rec.update(http_status=response.status, content_encoding=response.headers.get("Content-Encoding"))
                    async for chunk in response.content.iter_chunked(65536):
                        if len(body) + len(chunk) > plan["max_response_wire_bytes"] or total_bytes + len(body) + len(chunk) > plan["max_total_wire_bytes"]:
                            raise ValueError("wire byte cap")
                        body.extend(chunk)
                if rec["http_status"] != 200:
                    rec.update(ok=False, error_body=bytes(body[:1000]).decode(errors="replace"))
                else:
                    messages = 0
                    decoded_bytes = 0
                    symbols = set()
                    disconnects = 0
                    with gzip.GzipFile(fileobj=io.BytesIO(body)) as stream:
                        for line in stream:
                            decoded_bytes += len(line)
                            if decoded_bytes > plan["max_response_decoded_bytes"]:
                                raise ValueError("decoded byte cap")
                            if not line.strip():
                                disconnects += 1
                                continue
                            value = json.loads(line.split(b" ", 1)[1])
                            data = value.get("params", {}).get("data", {})
                            name = data.get("instrument_name")
                            if name:
                                symbols.add(name)
                            messages += 1
                    rec.update(ok=True, messages=messages, symbols=len(symbols), decoded_bytes=decoded_bytes, disconnect_markers=disconnects)
            except (aiohttp.ClientError, TimeoutError, ValueError, OSError) as exc:
                rec.update(ok=False, error=str(exc))
            total_bytes += len(body)
            rec.update(response_received_utc=utc(), body_bytes=len(body), body_sha256=hashlib.sha256(body).hexdigest())
            body_path.write_bytes(body)
            write(meta_path, rec)
            rows.append(rec)
            write(out / "summary.json", {"requests": len(rows), "wire_bytes": total_bytes, "rows": rows})
            print(json.dumps({k: rec.get(k) for k in ("id", "http_status", "ok", "messages", "symbols", "body_bytes", "decoded_bytes", "error")}, sort_keys=True), flush=True)
            if not rec.get("ok"):
                raise RuntimeError("stop on first failed request")
    write(out / "finished.json", {"requests": len(rows), "finished_utc": utc(), "wire_bytes": total_bytes})


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("plan", type=Path)
    asyncio.run(run(parser.parse_args().plan))
