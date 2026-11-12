"""Immutable metadata projection using receipt positions, never UTC knowledge order."""
from dataclasses import asdict, dataclass

from .contracts import InstrumentKey, Receipt, StreamKey, canonical, digest, integer, sha256, strict_json


@dataclass(frozen=True)
class MetadataVersion:
    instrument: InstrumentKey
    receipt: Receipt
    effective_at_ns: int | None
    attributes_json: bytes
    supersedes: str | None = None

    def __post_init__(self):
        if not isinstance(self.instrument, InstrumentKey) or not isinstance(self.receipt, Receipt):
            raise ValueError("typed instrument and source receipt required")
        if self.effective_at_ns is not None:
            integer(self.effective_at_ns, "effective_at_ns")
        if self.supersedes is not None:
            sha256(self.supersedes, "supersedes")
        if type(self.attributes_json) is not bytes or not isinstance(strict_json(self.attributes_json), dict):
            raise ValueError("immutable metadata object required")

    @property
    def observed_at_ns(self):
        return self.receipt.receive_utc_ns

    @property
    def source_record_id(self):
        return self.receipt.record_id

    @property
    def version_id(self):
        return digest(canonical(dict(instrument=asdict(self.instrument), receipt=asdict(self.receipt),
            effective_at_ns=self.effective_at_ns, attributes=strict_json(self.attributes_json),
            supersedes=self.supersedes)))

    @property
    def attributes(self):
        return strict_json(self.attributes_json)


def _positions(value):
    if not isinstance(value, dict):
        raise ValueError("explicit stream admission vector required; UTC alone is not knowledge")
    for stream, bound in value.items():
        if not isinstance(stream, StreamKey):
            raise ValueError("typed stream key required")
        integer(bound, "admission bound")
    return value


def _eligible(version, positions):
    bound = positions.get(version.receipt.stream_key)
    return bound is not None and version.receipt.receive_seq <= bound


class MetadataHistory:
    """Rebuild in archived owner/receipt order; cross-owner ties are not guessed."""

    def __init__(self):
        self._versions = {}
        self._by_instrument = {}

    def add(self, version: MetadataVersion):
        if not isinstance(version, MetadataVersion):
            raise ValueError("typed metadata version required")
        identity = version.version_id
        if identity in self._versions:
            return identity
        versions = self._by_instrument.get(version.instrument, [])
        if versions:
            last = versions[-1]
            if last.receipt.stream_key != version.receipt.stream_key:
                raise ValueError("metadata owner changed: new anchored projection required")
            if version.receipt.receive_seq <= last.receipt.receive_seq:
                raise ValueError("metadata source positions conflict or are out of order")
        if version.supersedes is not None:
            previous = self._versions.get(version.supersedes)
            if previous is None or previous.instrument != version.instrument:
                raise ValueError("missing or foreign superseded version")
            if previous.effective_at_ns != version.effective_at_ns:
                raise ValueError("revision must retain its effective boundary")
        same_boundary = [v for v in versions if v.effective_at_ns == version.effective_at_ns]
        if same_boundary and version.supersedes != same_boundary[-1].version_id:
            raise ValueError("effective-boundary revision must supersede its current head")
        self._versions[identity] = version
        self._by_instrument.setdefault(version.instrument, []).append(version)
        return identity

    def at_version(self, version_id, admitted_positions):
        positions = _positions(admitted_positions)
        version = self._versions.get(version_id)
        if version is None or not _eligible(version, positions):
            raise ValueError("metadata version unavailable at admitted positions")
        return version

    def known_at(self, instrument, admitted_positions, *, effective_cutoff_ns):
        """Greatest effective boundary, then its eligible revision.

        Unknown effective boundaries require explicit version pins: do not silently
        select stale attributes or invent an earlier effective timestamp.
        """
        positions = _positions(admitted_positions)
        integer(effective_cutoff_ns, "effective_cutoff_ns")
        eligible = [v for v in self._by_instrument.get(instrument, ()) if _eligible(v, positions)]
        superseded = {v.supersedes for v in eligible if v.supersedes is not None}
        active = [v for v in eligible if v.version_id not in superseded]
        if any(v.effective_at_ns is None for v in active):
            raise ValueError("unknown effective boundary: explicit metadata version pin required")
        candidates = [v for v in active if v.effective_at_ns <= effective_cutoff_ns]
        return max(candidates, key=lambda v: v.effective_at_ns, default=None)
