"""Hash-linked validity ledger projection with explicitly pinned read versions.

Entries are immutable control payloads suitable for the durable raw journal. This
module is a pure, bounded projection; the writer/reader integration owns fsync and
raw-evidence availability. A gap closure never repairs its historical interval.
"""
from __future__ import annotations

from dataclasses import asdict, dataclass

from .contracts import InstrumentKey, canonical, digest, integer, sha256, strict_json, text

MAX_ENTRY_BYTES = 65536


@dataclass(frozen=True)
class LedgerStart:
    capture_scope: str
    policy: str = "receive-asof/2"

    def __post_init__(self):
        text(self.capture_scope, "capture_scope")
        if self.policy != "receive-asof/2":
            raise ValueError("unknown correction policy")


@dataclass(frozen=True)
class FeedTarget:
    instrument: InstrumentKey
    profile: str
    owner_epoch: str

    def __post_init__(self):
        if not isinstance(self.instrument, InstrumentKey):
            raise ValueError("typed instrument required")
        text(self.profile, "profile")
        text(self.owner_epoch, "owner_epoch")


@dataclass(frozen=True)
class GapCorrection:
    capture_scope: str
    target: FeedTarget
    first_cutoff_id: int
    recovery_cutoff_id: int | None
    evidence_record_id: str
    proof_record_id: str
    reason: str
    status: str = "invalid"

    def __post_init__(self):
        text(self.capture_scope, "capture_scope")
        text(self.reason, "reason")
        if not isinstance(self.target, FeedTarget) or self.status not in {"invalid", "unknown"}:
            raise ValueError("typed target and conservative correction status required")
        integer(self.first_cutoff_id, "first_cutoff_id")
        if self.recovery_cutoff_id is not None:
            integer(self.recovery_cutoff_id, "recovery_cutoff_id", self.first_cutoff_id + 1)
        sha256(self.evidence_record_id, "evidence_record_id")
        sha256(self.proof_record_id, "proof_record_id")

    @property
    def correction_id(self):
        return digest(canonical(asdict(self)))


@dataclass(frozen=True)
class CloseGap:
    correction_id: str
    recovery_cutoff_id: int
    recovery_anchor_record_id: str
    evidence_record_id: str

    def __post_init__(self):
        sha256(self.correction_id, "correction_id")
        sha256(self.recovery_anchor_record_id, "recovery_anchor_record_id")
        sha256(self.evidence_record_id, "evidence_record_id")
        integer(self.recovery_cutoff_id, "recovery_cutoff_id")


@dataclass(frozen=True)
class LedgerEntry:
    ledger_id: str
    sequence: int
    previous_hash: str | None
    payload: GapCorrection | CloseGap | LedgerStart

    def __post_init__(self):
        text(self.ledger_id, "ledger_id")
        integer(self.sequence, "sequence")
        if self.sequence == 0:
            if self.previous_hash is not None:
                raise ValueError("genesis has no predecessor")
        else:
            sha256(self.previous_hash, "previous_hash")
        if not isinstance(self.payload, (GapCorrection, CloseGap, LedgerStart)):
            raise ValueError("typed ledger payload required")
        if isinstance(self.payload, LedgerStart) and self.sequence != 0:
            raise ValueError("ledger start must be genesis")

    @property
    def kind(self):
        return "start" if isinstance(self.payload, LedgerStart) else "gap" if isinstance(self.payload, GapCorrection) else "close_gap"

    @property
    def entry_hash(self):
        return digest(canonical(dict(schema="dc.validity/2", ledger_id=self.ledger_id,
            sequence=self.sequence, previous_hash=self.previous_hash,
            kind=self.kind,
            payload=asdict(self.payload))))

    def to_bytes(self):
        result = canonical(dict(schema="dc.validity/2", ledger_id=self.ledger_id,
            sequence=self.sequence, previous_hash=self.previous_hash,
            kind=self.kind,
            payload=asdict(self.payload), entry_hash=self.entry_hash))
        if len(result) > MAX_ENTRY_BYTES:
            raise ValueError("ledger entry exceeds codec byte bound")
        return result

    @classmethod
    def from_bytes(cls, data):
        if not isinstance(data, bytes) or len(data) > MAX_ENTRY_BYTES:
            raise ValueError("bounded ledger payload required")
        value = strict_json(data)
        if not isinstance(value, dict) or set(value) != {
                "schema", "ledger_id", "sequence", "previous_hash", "kind", "payload", "entry_hash"
                } or value["schema"] != "dc.validity/2":
            raise ValueError("unknown ledger envelope")
        try:
            if not isinstance(value["payload"],dict):
                raise ValueError("ledger payload object required")
            attributes = dict(value["payload"])
            if value["kind"] == "gap":
                target = dict(attributes["target"])
                target["instrument"] = InstrumentKey(**target["instrument"])
                attributes["target"] = FeedTarget(**target)
                payload = GapCorrection(**attributes)
            elif value["kind"] == "close_gap":
                payload = CloseGap(**attributes)
            elif value["kind"] == "start":
                payload = LedgerStart(**attributes)
            else:
                raise ValueError("unknown ledger kind")
            entry = cls(value["ledger_id"], value["sequence"], value["previous_hash"], payload)
        except (TypeError, KeyError) as exc:
            raise ValueError("invalid ledger payload") from exc
        if entry.entry_hash != value["entry_hash"]:
            raise ValueError("ledger entry checksum mismatch")
        return entry


@dataclass(frozen=True)
class LedgerPin:
    ledger_id: str
    sequence: int
    head_hash: str

    def __post_init__(self):
        text(self.ledger_id, "ledger_id")
        integer(self.sequence, "sequence")
        sha256(self.head_hash, "head_hash")


class ValidityLedger:
    """Bounded, replayable projection. Unknown/missing prefixes never become valid."""
    def __init__(self, ledger_id, *, max_entries=10000, max_bytes=16 * 1024 * 1024):
        text(ledger_id, "ledger_id")
        integer(max_entries, "max_entries", 1)
        integer(max_bytes, "max_bytes", 1)
        self.ledger_id, self.max_entries, self.max_bytes = ledger_id, max_entries, max_bytes
        self._bytes = 0
        self._entries = []
        self._gaps = {}
        self._closed = set()

    @property
    def entry_count(self):
        return len(self._entries)

    def check(self, entry: LedgerEntry):
        """Validate without mutation; return encoded size, or zero for a retry."""
        if not isinstance(entry, LedgerEntry) or entry.ledger_id != self.ledger_id:
            raise ValueError("foreign ledger entry")
        if entry.sequence < len(self._entries):
            if self._entries[entry.sequence].entry_hash == entry.entry_hash:
                return 0
            raise ValueError("ledger fork/conflicting sequence")
        expected_previous = self._entries[-1].entry_hash if self._entries else None
        if entry.sequence != len(self._entries) or entry.previous_hash != expected_previous:
            raise ValueError("missing or broken ledger prefix")
        if len(self._entries) >= self.max_entries:
            raise ValueError("ledger projection bound reached; checkpoint/rotate required")
        encoded_size = len(entry.to_bytes())
        if self._bytes + encoded_size > self.max_bytes:
            raise ValueError("ledger projection byte bound reached; checkpoint/rotate required")
        payload = entry.payload
        if isinstance(payload, GapCorrection):
            if payload.correction_id in self._gaps:
                raise ValueError("correction already exists; retain original ledger identity")
        elif isinstance(payload, CloseGap):
            original = self._gaps.get(payload.correction_id)
            if original is None or original.recovery_cutoff_id is not None or payload.correction_id in self._closed:
                raise ValueError("close references missing/already closed correction")
            if payload.recovery_cutoff_id <= original.first_cutoff_id:
                raise ValueError("recovery must follow affected cutoff")
        return encoded_size

    def add(self, entry: LedgerEntry):
        encoded_size = self.check(entry)
        if not encoded_size:
            return entry.entry_hash
        payload = entry.payload
        # All validation precedes projection mutation; entries become visible atomically.
        self._entries.append(entry)
        self._bytes += encoded_size
        if isinstance(payload, GapCorrection):
            self._gaps[payload.correction_id] = payload
        elif isinstance(payload, CloseGap):
            self._closed.add(payload.correction_id)
        return entry.entry_hash

    def pin(self):
        if not self._entries:
            raise ValueError("empty ledger has no certified prefix")
        return LedgerPin(self.ledger_id, len(self._entries)-1, self._entries[-1].entry_hash)

    def verify_pin(self, pin):
        if (not isinstance(pin, LedgerPin) or pin.ledger_id != self.ledger_id or
                pin.sequence >= len(self._entries) or self._entries[pin.sequence].entry_hash != pin.head_hash):
            raise ValueError("pinned ledger prefix unavailable or mismatched")
        return pin

    def entries(self, pin):
        self.verify_pin(pin)
        for sequence in range(pin.sequence+1):
            yield self._entries[sequence]

    def correction(self, correction_id):
        sha256(correction_id, "correction_id")
        return self._gaps[correction_id]

    def assess(self, capture_scope, target, cutoff_id, validity_as_published, pin: LedgerPin):
        text(capture_scope, "capture_scope")
        integer(cutoff_id, "cutoff_id")
        if not isinstance(target, FeedTarget) or validity_as_published not in {"valid", "invalid", "unknown", "missing"}:
            raise ValueError("typed target and explicit original validity required")
        self.verify_pin(pin)
        gaps, closures = {}, {}
        for entry in self._entries[:pin.sequence+1]:
            payload = entry.payload
            if isinstance(payload, GapCorrection):
                gaps[payload.correction_id] = payload
            elif isinstance(payload, CloseGap):
                closures[payload.correction_id] = payload.recovery_cutoff_id
        applied = []
        assessed = validity_as_published
        for identity, gap in gaps.items():
            recovery = closures.get(identity, gap.recovery_cutoff_id)
            if gap.capture_scope == capture_scope and gap.target == target and cutoff_id >= gap.first_cutoff_id and (
                    recovery is None or cutoff_id < recovery):
                applied.append(identity)
                # Corrections may downgrade, never repair a missing/invalid original row.
                if assessed not in {"invalid", "missing"}:
                    assessed = "invalid" if gap.status == "invalid" else "unknown"
        return dict(validity_as_published=validity_as_published, assessed_validity=assessed,
                    pin=asdict(pin), corrections=sorted(applied))
