"""Versioned, lossless application-message contracts; no network side effects."""
from __future__ import annotations

import base64
from dataclasses import asdict, dataclass
import hashlib
import json
import math
import re

SCHEMA = "dc.raw/2"
MAX_BODY_BYTES = 32 * 1024 * 1024
KINDS = frozenset({"ws_message", "rest_response", "control", "metadata_version",
                   "coverage_event", "checkpoint"})
VENUES = frozenset({"deribit", "binance", "bybit"})


def canonical(value) -> bytes:
    return json.dumps(value, sort_keys=True, separators=(",", ":"),
                      ensure_ascii=False, allow_nan=False).encode("utf-8")


def digest(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def _pairs(items):
    result = {}
    for key, value in items:
        if key in result:
            raise ValueError("duplicate JSON key: " + key)
        result[key] = value
    return result


def strict_json(data: bytes):
    def invalid(value):
        raise ValueError("non-finite JSON number: " + value)
    def finite_float(value):
        result = float(value)
        if not math.isfinite(result):
            raise ValueError("non-finite JSON number: " + value)
        return result
    return json.loads(data, object_pairs_hook=_pairs, parse_constant=invalid, parse_float=finite_float)


def text(value, name):
    if not isinstance(value, str) or not value or any(ord(c) < 32 for c in value):
        raise ValueError("nonempty text required: " + name)


def integer(value, name, minimum=0):
    if type(value) is not int or value < minimum:
        raise ValueError("integer out of range: " + name)


def sha256(value, name):
    if not isinstance(value, str) or re.fullmatch(r"[0-9a-f]{64}", value) is None:
        raise ValueError("SHA256 required: " + name)


@dataclass(frozen=True, order=True)
class InstrumentKey:
    venue: str
    environment: str
    native_product: str
    native_symbol: str
    listing_incarnation: str

    def __post_init__(self):
        for name, value in asdict(self).items():
            text(value, name)
        if self.venue not in VENUES:
            raise ValueError("unknown venue")

    @property
    def key_id(self):
        return digest(canonical(asdict(self)))


@dataclass(frozen=True)
class StreamKey:
    capture_id: str
    process_session_id: str
    connection_id: str

    def __post_init__(self):
        for name, value in asdict(self).items():
            text(value, name)


@dataclass(frozen=True)
class Receipt:
    capture_id: str
    process_session_id: str
    connection_id: str
    receive_seq: int
    receive_utc_ns: int
    monotonic_ns: int
    clock_epoch: str

    def __post_init__(self):
        for name in ("capture_id", "process_session_id", "connection_id", "clock_epoch"):
            text(getattr(self, name), name)
        for name in ("receive_seq", "receive_utc_ns", "monotonic_ns"):
            integer(getattr(self, name), name)

    @property
    def stream_key(self):
        return StreamKey(self.capture_id, self.process_session_id, self.connection_id)

    @property
    def record_id(self):
        # UTC/clock changes do not change identity or order within a connection.
        return digest(canonical([self.capture_id, self.process_session_id,
                                 self.connection_id, self.receive_seq]))


@dataclass(frozen=True)
class Provenance:
    source_sha256: str
    config_sha256: str
    universe_sha256: str
    metadata_sha256: str | None = None

    def __post_init__(self):
        for name in ("source_sha256", "config_sha256", "universe_sha256"):
            sha256(getattr(self, name), name)
        if self.metadata_sha256 is not None:
            sha256(self.metadata_sha256, "metadata_sha256")


@dataclass(frozen=True)
class RawRecord:
    receipt: Receipt
    venue: str
    environment: str
    native_product: str
    channel: str
    kind: str
    body: bytes
    provenance: Provenance
    instrument: InstrumentKey | None = None
    native_fields_json: bytes = b"{}"

    def __post_init__(self):
        if not isinstance(self.receipt, Receipt) or not isinstance(self.provenance, Provenance):
            raise ValueError("typed receipt and provenance required")
        for name in ("environment", "native_product", "channel"):
            text(getattr(self, name), name)
        if self.venue not in VENUES or self.kind not in KINDS:
            raise ValueError("unknown venue or record kind")
        if type(self.body) is not bytes or len(self.body) > MAX_BODY_BYTES:
            raise ValueError("invalid or oversized raw body")
        if type(self.native_fields_json) is not bytes:
            raise ValueError("immutable native fields required")
        if not isinstance(strict_json(self.native_fields_json), dict):
            raise ValueError("native fields must be an object")
        if self.instrument is not None:
            if not isinstance(self.instrument, InstrumentKey):
                raise ValueError("typed instrument required")
            if (self.venue, self.environment, self.native_product) != (
                    self.instrument.venue, self.instrument.environment, self.instrument.native_product):
                raise ValueError("instrument identity conflicts with envelope")

    @property
    def record_id(self):
        return self.receipt.record_id

    def to_bytes(self):
        return canonical(dict(schema=SCHEMA, record_id=self.record_id,
            receipt=asdict(self.receipt), venue=self.venue, environment=self.environment,
            native_product=self.native_product, channel=self.channel, kind=self.kind,
            provenance=asdict(self.provenance),
            instrument=asdict(self.instrument) if self.instrument else None,
            native_fields=strict_json(self.native_fields_json),
            payload=dict(encoding="base64", data=base64.b64encode(self.body).decode("ascii"),
                         bytes=len(self.body), sha256=digest(self.body))))

    @classmethod
    def from_bytes(cls, data, *, max_body_bytes=MAX_BODY_BYTES):
        integer(max_body_bytes, "max_body_bytes", 1)
        if max_body_bytes > MAX_BODY_BYTES:
            raise ValueError("body bound exceeds schema limit")
        # Bound before JSON parsing or base64 allocation. Envelope overhead is bounded too.
        if type(data) is not bytes or len(data) > 2 * max_body_bytes + 65536:
            raise ValueError("oversized record")
        value = strict_json(data)
        expected = {"schema", "record_id", "receipt", "venue", "environment", "native_product",
                    "channel", "kind", "provenance", "instrument", "native_fields", "payload"}
        if not isinstance(value, dict) or set(value) != expected or value["schema"] != SCHEMA:
            raise ValueError("unknown envelope schema/fields")
        payload = value["payload"]
        if not isinstance(payload, dict) or set(payload) != {"encoding", "data", "bytes", "sha256"}:
            raise ValueError("invalid payload envelope")
        integer(payload["bytes"], "payload.bytes")
        if payload["encoding"] != "base64" or payload["bytes"] > max_body_bytes:
            raise ValueError("unknown encoding or oversized payload")
        if not isinstance(payload["data"], str) or len(payload["data"]) > 4 * ((max_body_bytes + 2)//3):
            raise ValueError("oversized base64 payload")
        try:
            body = base64.b64decode(payload["data"], validate=True)
            receipt = Receipt(**value["receipt"])
            provenance = Provenance(**value["provenance"])
            instrument = InstrumentKey(**value["instrument"]) if value["instrument"] is not None else None
        except (TypeError, KeyError) as exc:
            raise ValueError("invalid nested envelope") from exc
        if len(body) != payload["bytes"] or digest(body) != payload["sha256"]:
            raise ValueError("body checksum/length mismatch")
        record = cls(receipt, value["venue"], value["environment"], value["native_product"],
                     value["channel"], value["kind"], body, provenance, instrument,
                     canonical(value["native_fields"]))
        if record.record_id != value["record_id"]:
            raise ValueError("record identity mismatch")
        return record
