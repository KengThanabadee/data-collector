"""Golden fixtures prove that minute archives and event runs keep their on-disk formats.

The fixtures under tests/fixtures/golden were written by the code tagged
`pre-cleanup-2026-11` from recorded venue replies; see the README there.
"""
import base64
import gzip
import hashlib
import json
from pathlib import Path
import re
import shutil
import struct
import subprocess
import tempfile
import unittest

from tests.golden import DAY, FIRST_SLOT, GOLDEN, capture_event_run, capture_minute_archive, run_cli


# A fresh archive equals the golden one when every file has the same path and
# the same decoded content, apart from the fields named below. Compressed files
# are compared by their gzip header and decompressed content, files embedded in
# hourly journals and sealed days are decoded the same way, and journal frames
# are compared by record.

# The loopback port in each archived request URL.
LOOPBACK = re.compile(r"^http://127\.0\.0\.1:\d+/")
# Minute archives: each set names the keys excluded in files with that name.
MINUTE_EXCLUDED = {
    # Code identity, the temporary data root and the config hash that covers it.
    "context.json": {"code", "data_root", "config_sha256"},
    # Digests and sizes of response archives and contexts holding those fields.
    "manifest.json": {"archive_bytes", "archive_sha256", "context_sha256", "journal_sha256"},
    # Digests and sizes of the sealed day and its hourly journals.
    f"{DAY}.manifest.json": {"archive_bytes", "archive_sha256", "bytes", "sha256", "source_bytes",
                             "source_sha256"},
}
# Event runs: keys excluded wherever they appear.
EVENT_EXCLUDED = {
    # Code identity, the temporary data root and the config hash that covers it.
    "source_sha256", "data_root", "config_sha256",
    # Run IDs.
    "capture_id", "process_session_id", "session_id",
    # Wall and monotonic times, and chrony tracking output, which carries the sampled time.
    "utc_ns", "monotonic_ns", "receive_utc_ns", "started_utc_ns", "epoch_started_ns", "recovery_next_utc_ns",
    "uncertainty_ns",
    "elapsed_seconds", "date", "next_connect", "stdout_base64",
    # Record IDs, digests over records that carry times or run IDs, and compressed sizes.
    "record_id", "source_record_id", "last_record_id", "last_closed_record_id", "sha256", "raw_sha256",
    "evidence_sha256", "previous_sha256", "head_sha256", "records_sha256", "archive_sha256", "archive_bytes",
    "uncompressed_bytes", "manifest_sha256", "context_sha256", "session_sha256", "binding_sha256", "wal_sha256",
    # Write-ahead journal size, which follows the length of the records it holds.
    "wal_bytes",
}
# The same journal size inside each write-ahead journal seal.
WAL_SEAL_SCHEMA, WAL_SEAL_SIZE = "dc.wal-seal/2", "bytes"
# Control payloads are compared decoded, without their byte count: their content carries times.
CONTROL_PAYLOAD_SIZE = "bytes"
# Run IDs inside connection IDs, and run and segment directory names and capture dates in paths.
RUN_ID = re.compile(r"(?<=-)[0-9a-f]{32}\b")
HASH_PATH = re.compile(r"\b[0-9a-f]{64}\b")
DATE_PATH = re.compile(r"\b\d{4}-\d{2}-\d{2}\b")
WAL_MAGIC = b"DCW2\r\n\x1a\n"


def _lines(raw):
    return [json.loads(line) for line in raw.splitlines()]


def _minute_value(name, value):
    excluded = MINUTE_EXCLUDED.get(name, set())
    if isinstance(value, dict):
        result = {}
        for key, item in value.items():
            if key in excluded:
                continue
            if key in ("files", "references") and isinstance(item, dict):
                result[key] = {inner: _minute_file(inner, base64.b64decode(data)) for inner, data in item.items()}
            elif key == "effective_url" and isinstance(item, str):
                result[key] = LOOPBACK.sub("http://loopback/", item)
            else:
                result[key] = _minute_value(name, item)
        return result
    if isinstance(value, list):
        return [_minute_value(name, item) for item in value]
    return value


def _decompress(name, raw):
    if name.endswith(".gz"):
        return {"gzip_header": raw[:10].hex(), "content": gzip.decompress(raw)}
    if name.endswith(".zst"):
        content = subprocess.run(["zstd", "-q", "-d", "-c"], input=raw, capture_output=True, check=True).stdout
        return {"zstd_frame_magic": raw[:4].hex(), "content": content}
    return {"content": raw}


def _minute_file(name, raw):
    decoded = _decompress(name, raw)
    base = Path(name).name
    key = base.removesuffix(".gz").removesuffix(".zst")
    content = decoded.pop("content")
    if key.endswith((".json", ".jsonl")):
        decoded["lines"] = [_minute_value(base, line) for line in _lines(content)]
    else:
        decoded["bytes"] = content.hex()
    return decoded


def minute_snapshot(root):
    """Decoded minute archive with the excluded fields removed, keyed by relative path."""
    root = Path(root)
    if not root.is_dir():
        raise FileNotFoundError(root)
    return {str(path.relative_to(root)): _minute_file(path.name, path.read_bytes())
            for path in sorted(root.rglob("*")) if path.is_file() and path.suffix != ".lock"}


def _event_value(value):
    if isinstance(value, dict):
        if value.get("kind") == "control" and isinstance(value.get("payload"), dict):
            payload = dict(value["payload"])
            if payload.get("encoding") == "base64":
                payload["data"] = json.loads(base64.b64decode(payload["data"]))
                payload.pop(CONTROL_PAYLOAD_SIZE)
            value = dict(value, payload=payload)
        excluded = EVENT_EXCLUDED | ({WAL_SEAL_SIZE} if value.get("schema") == WAL_SEAL_SCHEMA else set())
        return {key: _event_value(item) for key, item in value.items() if key not in excluded}
    if isinstance(value, list):
        return [_event_value(item) for item in value]
    if isinstance(value, str):
        return _event_path(RUN_ID.sub("<id>", value))
    return value


def _event_path(text):
    return DATE_PATH.sub("<date>", HASH_PATH.sub("<hash>", text))


def _record_order(record):
    receipt = record.get("receipt", {})
    return receipt.get("connection_id", ""), receipt.get("receive_seq", 0)


def _event_file(path):
    raw = path.read_bytes()
    if path.suffix == ".wal":
        if not raw.startswith(WAL_MAGIC):
            raise ValueError(f"unknown journal header in {path}")
        frames, offset = [], len(WAL_MAGIC)
        while offset < len(raw):
            length, checksum = struct.unpack(">I32s", raw[offset:offset + 36])
            body = raw[offset + 36:offset + 36 + length]
            frames.append(dict(checksum_matches=hashlib.sha256(body).digest() == checksum,
                               record=_event_value(json.loads(body))))
            offset += 36 + length
        return {"magic": WAL_MAGIC.hex(), "frames": sorted(frames, key=lambda f: _record_order(f["record"]))}
    decoded = _decompress(path.name, raw)
    lines = [_event_value(line) for line in _lines(decoded.pop("content"))]
    decoded["lines"] = sorted(lines, key=_record_order) if path.name == "records.jsonl.gz" else lines
    return decoded


def event_snapshot(root):
    """Decoded event run without its frozen source, with the excluded fields removed."""
    root = Path(root)
    if not root.is_dir():
        raise FileNotFoundError(root)
    snapshot = {}
    for path in sorted(root.rglob("*")):
        relative = path.relative_to(root)
        if not path.is_file() or relative.parts[0] == "source" or path.suffix == ".lock":
            continue
        snapshot[_event_path(str(relative))] = _event_file(path)
    return snapshot


class GoldenFixtureTest(unittest.TestCase):
    def setUp(self):
        temp = tempfile.TemporaryDirectory()
        self.addCleanup(temp.cleanup)
        self.temp = Path(temp.name)

    def golden_copy(self, name):
        target = self.temp / "golden" / name
        shutil.copytree(GOLDEN / name, target)
        return target


class GoldenMinuteTests(GoldenFixtureTest):
    def test_minute_read_returns_the_archived_minutes(self):
        root = self.golden_copy("minute")
        for minute, slot in (("2358", "2026-11-08T23:58:00Z"), ("2359", "2026-11-08T23:59:00Z")):
            directory = GOLDEN / "minute" / "minutes" / "2026" / "11" / "08" / minute
            code, result = run_cli(["minute-read", "--data-root", root, "--slot", slot])
            self.assertEqual(code, 0, result)
            self.assertEqual(result, {
                "index": json.loads((directory / "index.jsonl").read_bytes()),
                "records": [json.loads(line) for line in
                            gzip.decompress((directory / "responses.jsonl.gz").read_bytes()).splitlines()],
            })
            self.assertFalse(result["index"]["gap"])
            self.assertEqual(len(result["records"]), 23)
            self.assertEqual(set(result["index"]["requests"].values()), {"ok"})

    def test_minute_verify_day_returns_the_sealed_manifest(self):
        root = self.golden_copy("minute")
        code, result = run_cli(["minute-verify-day", "--data-root", root, "--day", DAY])
        self.assertEqual(code, 0, result)
        self.assertEqual(result, json.loads((GOLDEN / "minute" / "daily" / f"{DAY}.manifest.json").read_bytes()))
        self.assertEqual((result["first_slot"], result["last_slot"], result["minutes"]),
                         (FIRST_SLOT, FIRST_SLOT + 60, 2))

    def test_capture_from_recorded_replies_matches_the_golden_minute_archive(self):
        root = self.temp / "minute"
        root.mkdir()
        (capture_code, captured), (seal_code, sealed) = capture_minute_archive(root)
        self.assertEqual((capture_code, captured), (0, {"minutes_processed": 2}))
        self.assertEqual(seal_code, 0, sealed)
        self.assertEqual(minute_snapshot(root), minute_snapshot(GOLDEN / "minute"))


class GoldenEventTests(GoldenFixtureTest):
    VERIFIED = {
        "binance": {
            "archive_verified": True, "records": 17, "ws_messages": 2, "metadata_versions_verified": 0,
            "feed_plan_initialization_verified": True, "capture_status": "raw_capture_complete",
            "topics_seen": ["!forceOrder@arr"], "liquidation_pushes_observed": True,
        },
        "bybit": {
            "archive_verified": True, "records": 23, "ws_messages": 4, "metadata_versions_verified": 0,
            "feed_plan_initialization_verified": True, "capture_status": "raw_capture_complete",
            "topics_seen": ["allLiquidation.ETHUSDT", "publicTrade.BTC"], "liquidation_pushes_observed": True,
        },
    }

    def test_verify_run_reports_the_recorded_event_runs(self):
        for venue, expected in self.VERIFIED.items():
            with self.subTest(venue=venue):
                code, result = run_cli(["verify-run", self.golden_copy(venue)])
                self.assertEqual((code, result), (0, expected))

    def test_capture_from_recorded_pushes_matches_the_golden_event_runs(self):
        for venue in self.VERIFIED:
            with self.subTest(venue=venue):
                directory = self.temp / venue
                directory.mkdir()
                code, result = capture_event_run(directory, venue)
                self.assertEqual((code, result["status"]), (0, "raw_capture_complete"), result)
                self.assertEqual(event_snapshot(result["root"]), event_snapshot(GOLDEN / venue))


if __name__ == "__main__":
    unittest.main()
