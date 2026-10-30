"""Extract causally available BTC ticker states from saved replay responses."""
import argparse
from datetime import datetime, timezone
import gzip
import hashlib
import json
from pathlib import Path
import re

OPTION = re.compile(r"^BTC-(\d{1,2}[A-Z]{3}\d{2})-(\d+(?:\.\d+)?)-([CP])$")
ROOT = Path(__file__).resolve().parents[1]


def iso_seconds(value):
    return datetime.fromisoformat(value.replace("Z", "+00:00")).timestamp()


def contract(name):
    match = OPTION.fullmatch(name)
    if not match:
        return None
    expiry = datetime.strptime(match[1], "%d%b%y").replace(hour=8, tzinfo=timezone.utc).timestamp()
    return {"expiry": expiry, "strike": float(match[2]), "put": match[3] == "P"}


def extract(directory, only=None):
    states = []
    for meta_path in sorted(directory.glob("*.request.json")):
        if only is not None and meta_path.name != only:
            continue
        meta = json.loads(meta_path.read_text())
        if not meta.get("ok"):
            continue
        body_path = meta_path.with_name(meta_path.name.replace(".request.json", ".body"))
        digest = hashlib.sha256(body_path.read_bytes()).hexdigest()
        assert digest == meta["body_sha256"]
        cached = meta_path.with_name(meta_path.name.replace(".request.json", ".btc-state.json.gz"))
        if cached.exists():
            state = json.loads(gzip.decompress(cached.read_bytes()))
            assert state["source_sha256"] == digest
            states.append(state)
            continue
        start = iso_seconds(meta["params"]["from"])
        cutoff = start + 60
        latest = {}
        rejected_future = 0
        messages = 0
        disconnects = 0
        with gzip.open(body_path, "rt") as stream:
            for position, line in enumerate(stream):
                if not line.strip():
                    disconnects += 1
                    continue
                local, payload = line.split(" ", 1)
                message = json.loads(payload)
                data = message.get("params", {}).get("data", {})
                name = data.get("instrument_name", "")
                if name != "BTC-PERPETUAL" and not OPTION.fullmatch(name):
                    continue
                receipt = iso_seconds(local)
                event = data["timestamp"] / 1000
                assert start <= receipt < cutoff
                messages += 1
                if event > cutoff:
                    rejected_future += 1
                    continue
                latest[name] = {"receipt": receipt, "event": event, "position": position, "data": data}
        state = {"from": meta["params"]["from"], "decision": cutoff, "source_sha256": digest, "messages_examined": messages, "rejected_future": rejected_future, "disconnect_markers": disconnects, "quotes": latest}
        with gzip.open(cached, "wt") as stream:
            json.dump(state, stream)
        print(json.dumps({"from": state["from"], "btc_states": len(latest), "future_rejected": rejected_future}), flush=True)
        states.append(state)
    return sorted(states, key=lambda x: x["decision"])


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("directory", type=Path)
    args = parser.parse_args()
    states = extract(args.directory)
    print("Extracted", len(states), "saved snapshots; no network.")
