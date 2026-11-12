"""Shared attempt envelopes and lossless comparisons for minute references."""
from decimal import Decimal
import gzip
import json

from .minute_files import body_bytes, dumps, sha, utc


class ReferenceAttempts:
    def __init__(self, environment, venue, slot, *, run_id=None):
        self.context = dict(schema_version=1, run_id=run_id or f"{venue}-reference-{slot}",
                            job_kind="minute", environment=environment, currency=venue,
                            scheduled_slot_utc=utc(slot))
        self.records = []

    def append(self, record):
        self.records.append(record)
        return record


def read_reference(path, *, archived=None):
    entry = json.loads(gzip.decompress(path.read_bytes() if archived is None else archived))
    for response in entry["responses"]:
        raw = body_bytes(response)
        if ((sha(raw) if raw is not None else None) != response["body_sha256"]
                or len(raw or b"") != response["body_byte_count"]):
            raise ValueError(f"reference response checksum differs: {path}")
    return entry


def reference_json(value):
    """Canonical JSON without rounding decimals or treating them as strings."""
    if isinstance(value, Decimal):
        return str(value).encode("ascii")
    if isinstance(value, list):
        return b"[" + b",".join(reference_json(item) for item in value) + b"]"
    if isinstance(value, dict):
        pairs = (dumps(key).rstrip(b"\n") + b":" + reference_json(value[key]) for key in sorted(value))
        return b"{" + b",".join(pairs) + b"}"
    return dumps(value).rstrip(b"\n")
