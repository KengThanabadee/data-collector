"""Three bounded, unauthenticated Tardis free-day ticker samples.

This is an availability/schema probe, not a research sample or downloader.
Symbols are selected from an already saved hour of trades; this selection uses
information within that hour and MUST NOT be reused as a backtest selection rule.
"""
import asyncio
from collections import Counter
from datetime import datetime, timezone
import gzip
import hashlib
import json
from pathlib import Path
import aiohttp

ROOT = Path(__file__).resolve().parents[1]
OUT = ROOT / "experiments/results/2026-10-22-research-free-quotes"
BASE = "https://api.tardis.dev/v1/data-feeds/deribit"


def utc():
    return datetime.now(timezone.utc).isoformat()


def write(path, value):
    path.write_text(json.dumps(value, indent=2) + "\n")


async def run():
    symbols = ["BTC-PERPETUAL", "ETH-PERPETUAL"]
    for file, limit in (("01.body", 10), ("02.body", 6)):
        trades = json.loads((ROOT / "experiments/results/2026-10-22-research-trade-chain" / file).read_bytes())["result"]["trades"]
        symbols.extend(name for name, _ in Counter(t["instrument_name"] for t in trades).most_common(limit))
    OUT.mkdir(parents=True, exist_ok=False)
    write(OUT / "context.json", {"started_utc": utc(), "max_requests": 3, "max_wire_bytes_per_response": 32 * 1024**2, "max_decoded_bytes": 64 * 1024**2, "retries": 0, "authenticated": False, "purpose": "availability/schema only; trade-selected symbols are not a backtest universe", "source_sha256": hashlib.sha256(Path(__file__).read_bytes()).hexdigest()})
    rows = []
    async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=45), auto_decompress=False, trust_env=False) as client:
        for hour in (12, 13, 23):
            params = {"from": f"2025-10-01T{hour:02d}:00:00.000Z", "filters": json.dumps([{"channel": "ticker", "symbols": symbols}]), "compression": "gzip"}
            rec = {"id": len(rows) + 1, "endpoint": BASE, "params": params, "request_started_utc": utc()}
            body = bytearray()
            try:
                async with client.get(BASE, params=params, headers={"Accept-Encoding": "gzip"}, allow_redirects=False) as response:
                    rec["http_status"] = response.status
                    rec["content_encoding"] = response.headers.get("Content-Encoding")
                    async for chunk in response.content.iter_chunked(65536):
                        if len(body) + len(chunk) > 32 * 1024**2:
                            raise ValueError("wire byte limit")
                        body.extend(chunk)
                if rec["http_status"] == 200:
                    import io
                    with gzip.GzipFile(fileobj=io.BytesIO(body)) as stream:
                        decoded = stream.read(64 * 1024**2 + 1)
                    if len(decoded) > 64 * 1024**2:
                        raise ValueError("decoded byte limit")
                    messages = []
                    disconnects = 0
                    for line in decoded.decode().splitlines():
                        if not line.strip():
                            disconnects += 1
                            continue
                        local, payload = line.split(" ", 1)
                        message = json.loads(payload)
                        messages.append((local, message))
                    records = [m[1].get("params", {}).get("data", {}) for m in messages]
                    rec.update(ok=True, messages=len(messages), disconnect_markers=disconnects, decoded_bytes=len(decoded), symbol_counts=dict(Counter(d.get("instrument_name") for d in records)), fields=sorted({k for d in records for k in d}), first_local_timestamp=messages[0][0] if messages else None, last_local_timestamp=messages[-1][0] if messages else None)
                else:
                    rec.update(ok=False, error_body=bytes(body[:1000]).decode(errors="replace"))
            except (aiohttp.ClientError, TimeoutError, ValueError, OSError) as exc:
                rec.update(ok=False, error=str(exc))
            rec.update(response_received_utc=utc(), body_bytes=len(body), body_sha256=hashlib.sha256(body).hexdigest())
            name = f"{rec['id']:02d}"
            (OUT / (name + ".body")).write_bytes(body)
            write(OUT / (name + ".request.json"), rec)
            rows.append(rec)
            write(OUT / "summary.json", {"requests": len(rows), "rows": rows})
            print(json.dumps(rec), flush=True)
            if rec.get("http_status") != 200:
                break
            await asyncio.sleep(1)
    write(OUT / "finished.json", {"requests": len(rows), "finished_utc": utc()})


if __name__ == "__main__":
    asyncio.run(run())
