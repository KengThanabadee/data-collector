"""Byte-bounded, exclusively owned framed WAL; fsync before acknowledging a record.

This layer never asserts feed continuity. Its positions describe collector receipt
order. Recovery preserves damaged tails before truncating the usable WAL prefix.
"""
from __future__ import annotations

from dataclasses import dataclass, field
import fcntl
import hashlib
import os
from pathlib import Path
import struct
import uuid

from .contracts import MAX_BODY_BYTES, RawRecord, StreamKey, canonical, digest, integer, sha256, strict_json

MAGIC = b"DCW2\r\n\x1a\n"
FRAME = struct.Struct(">I32s")
COPY_BUFFER = 65536


class JournalFull(ValueError):
    pass


class JournalDamaged(ValueError):
    pass


@dataclass(frozen=True)
class JournalLimits:
    max_record_bytes: int = 2 * MAX_BODY_BYTES + 65536
    target_bytes: int = 64 * 1024 * 1024
    max_streams: int = 4096

    def __post_init__(self):
        for name in ("max_record_bytes", "target_bytes", "max_streams"):
            integer(getattr(self, name), name, 1)
        if self.max_record_bytes > 2 * MAX_BODY_BYTES + 65536:
            raise ValueError("record bound exceeds schema maximum")


@dataclass
class Scan:
    records: int = 0
    valid_bytes: int = len(MAGIC)
    reason: str | None = None
    positions: dict[StreamKey, int] = field(default_factory=dict)


def sync_directory(path):
    fd = os.open(path, os.O_RDONLY)
    try:
        os.fsync(fd)
    finally:
        os.close(fd)


def _hash_file(path):
    result = hashlib.sha256()
    with Path(path).open("rb") as src:
        while chunk := src.read(COPY_BUFFER):
            result.update(chunk)
    return result.hexdigest()


def _publish_bytes(path, data):
    """Durable no-clobber metadata, including idempotent crash retries."""
    temp = path.with_name("." + path.name + "." + uuid.uuid4().hex)
    try:
        with temp.open("xb") as dst:
            dst.write(data)
            dst.flush()
            os.fsync(dst.fileno())
        try:
            os.link(temp, path)
        except FileExistsError:
            with path.open("rb") as existing:
                if existing.read(len(data) + 1) != data:
                    raise JournalDamaged("publication evidence conflict")
                os.fsync(existing.fileno())
        sync_directory(path.parent)
    finally:
        temp.unlink(missing_ok=True)


def _scan(src, limits, identity):
    src.seek(0)
    if src.read(len(MAGIC)) != MAGIC:
        raise JournalDamaged("unknown or truncated journal header")
    size = os.fstat(src.fileno()).st_size
    if size > len(MAGIC) + max(limits.target_bytes, limits.max_record_bytes + FRAME.size):
        raise JournalDamaged("journal exceeds declared byte bound")
    result = Scan()
    while header := src.read(FRAME.size):
        if len(header) != FRAME.size:
            result.reason = "truncated_frame_header"
            break
        length, expected_hash = FRAME.unpack(header)
        if length == 0 or length > limits.max_record_bytes:
            result.reason = "invalid_frame_length"
            break
        data = src.read(length)
        if len(data) != length:
            result.reason = "truncated_frame_body"
            break
        if hashlib.sha256(data).digest() != expected_hash:
            result.reason = "frame_checksum_mismatch"
            break
        try:
            record = RawRecord.from_bytes(data)
        except (ValueError, TypeError, UnicodeError):
            result.reason = "invalid_record"
            break
        if (record.venue, record.environment) != identity:
            raise JournalDamaged("journal identity mismatch")
        stream, seq = record.receipt.stream_key, record.receipt.receive_seq
        previous = result.positions.get(stream)
        if previous is not None and seq != previous + 1:
            result.reason = "receipt_sequence_conflict"
            break
        if previous is None and len(result.positions) >= limits.max_streams:
            raise JournalDamaged("journal stream count exceeds bound")
        result.positions[stream] = seq
        result.records += 1
        result.valid_bytes += FRAME.size + length
    return result


class Journal:
    def __init__(self, path, *, venue, environment, limits=JournalLimits(), create=False):
        self.path = Path(path)
        self.identity = (venue, environment)
        self.limits = limits
        self.poisoned = False
        self.closed = False
        self.seal_binding = None
        flags = os.O_RDWR | getattr(os, "O_NOFOLLOW", 0)
        if create:
            flags |= os.O_CREAT | os.O_EXCL
        fd = os.open(self.path, flags, 0o600)
        self._file = os.fdopen(fd, "r+b", buffering=0)
        try:
            fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
            if create:
                self._write_all(MAGIC)
            self.state = _scan(self._file, limits, self.identity)
            # A prior creator may have failed before fsyncing the directory. Also
            # re-establish durability for complete but unacknowledged crash survivors.
            os.fsync(fd)
            sync_directory(self.path.parent)
            marker = self.path.with_name(self.path.name + ".sealed.json")
            if marker.exists():
                with marker.open("rb") as src:
                    content = src.read(4097)
                if len(content) > 4096:
                    raise JournalDamaged("oversized seal marker")
                seal = strict_json(content)
                if self.state.reason or seal != self._seal_content(seal.get("binding_sha256")):
                    raise JournalDamaged("sealed journal content mismatch")
                self.seal_binding = seal["binding_sha256"]
            self._file.seek(0, os.SEEK_END)
        except BaseException:
            self._file.close()
            self.closed = True
            raise

    def _write_all(self, data):
        view = memoryview(data)
        while view:
            written = self._file.write(view)
            if not written:
                raise OSError("short journal write")
            view = view[written:]

    def _ready(self):
        if self.closed or self.poisoned:
            raise JournalDamaged("journal must be reopened after close/write failure")
        if self.state.reason:
            raise JournalDamaged("preserve/recover damaged tail before appending")
        if self.seal_binding is not None:
            raise JournalDamaged("journal is sealed; rotate to a new journal")

    def _seal_content(self, binding):
        sha256(binding, "binding_sha256")
        return dict(schema="dc.wal-seal/2", binding_sha256=binding,
                    wal_sha256=_hash_file(self.path), bytes=self.state.valid_bytes,
                    records=self.state.records, identity=list(self.identity))

    def freeze(self, binding_sha256):
        """Persist an append prohibition before publishing a derived segment."""
        if self.closed or self.poisoned or self.state.reason:
            raise JournalDamaged("cannot seal unusable journal")
        if self.seal_binding is not None and self.seal_binding != binding_sha256:
            raise JournalDamaged("journal already bound to another segment context")
        state = _scan(self._file, self.limits, self.identity)
        if state != self.state:
            raise JournalDamaged("journal changed before seal")
        os.fsync(self._file.fileno())
        marker = self.path.with_name(self.path.name + ".sealed.json")
        content = self._seal_content(binding_sha256)
        # Poison until the durable marker is known; failed publication requires reopen.
        self.poisoned = True
        _publish_bytes(marker, canonical(content))
        self.seal_binding = binding_sha256
        self.poisoned = False
        return content

    def encoded_records(self, *, allow_unsealed=False):
        """Stream the exclusively owned WAL; preflight may read before sealing.

        The owner must serialize append/read/seal calls (this is not a threaded API).
        """
        if self.closed or self.poisoned or self.state.reason or (
                self.seal_binding is None and not allow_unsealed):
            raise JournalDamaged("frozen usable journal required")
        self._file.seek(len(MAGIC))
        for _ in range(self.state.records):
            header = self._file.read(FRAME.size)
            if len(header) != FRAME.size:
                raise JournalDamaged("sealed frame missing")
            length, expected_hash = FRAME.unpack(header)
            if length < 1 or length > self.limits.max_record_bytes:
                raise JournalDamaged("sealed frame length invalid")
            data = self._file.read(length)
            if len(data) != length or hashlib.sha256(data).digest() != expected_hash:
                raise JournalDamaged("sealed frame changed")
            yield data
        if self._file.read(1):
            raise JournalDamaged("sealed journal has an unexpected suffix")

    def append(self, record: RawRecord):
        self._ready()
        if not isinstance(record, RawRecord) or (record.venue, record.environment) != self.identity:
            raise ValueError("journal identity mismatch")
        encoded = record.to_bytes()
        if len(encoded) > self.limits.max_record_bytes:
            raise ValueError("record exceeds journal bound")
        stream, seq = record.receipt.stream_key, record.receipt.receive_seq
        previous = self.state.positions.get(stream)
        if previous is not None and seq != previous + 1:
            raise ValueError("nonconsecutive collector receipt sequence")
        if previous is None and len(self.state.positions) >= self.limits.max_streams:
            raise JournalFull("stream count bound reached")
        frame_bytes = FRAME.size + len(encoded)
        payload_bytes = self.state.valid_bytes - len(MAGIC)
        if self.state.records and payload_bytes + frame_bytes > self.limits.target_bytes:
            raise JournalFull("rotate before next record")
        try:
            # Preflight/read iterators can stop mid-file. Never use their cursor as
            # an append position, and never hide unexpected external file changes.
            if self._file.seek(0, os.SEEK_END) != self.state.valid_bytes:
                raise JournalDamaged("journal length changed outside owned append")
            self._write_all(FRAME.pack(len(encoded), hashlib.sha256(encoded).digest()))
            self._write_all(encoded)
            os.fsync(self._file.fileno())
        except BaseException:
            self.poisoned = True
            raise
        # No acknowledgement or position advance before successful fsync.
        self.state.records += 1
        self.state.valid_bytes += frame_bytes
        self.state.positions[stream] = seq
        return record.record_id

    def recover_tail(self, *, checkpoint=lambda stage: None):
        if self.closed or self.poisoned:
            raise JournalDamaged("reopen journal before recovery")
        if self.state.reason is None:
            return None
        offset = self.state.valid_bytes
        temp = self.path.with_name("." + self.path.name + ".tail-" + uuid.uuid4().hex)
        hasher, count = hashlib.sha256(), 0
        try:
            prefix_hash = hashlib.sha256()
            self._file.seek(0)
            remaining = offset
            while remaining:
                chunk = self._file.read(min(COPY_BUFFER, remaining))
                if not chunk:
                    raise JournalDamaged("valid prefix changed during recovery")
                prefix_hash.update(chunk)
                remaining -= len(chunk)
            with temp.open("xb") as dst:
                self._file.seek(offset)
                while chunk := self._file.read(COPY_BUFFER):
                    dst.write(chunk)
                    hasher.update(chunk)
                    count += len(chunk)
                dst.flush()
                os.fsync(dst.fileno())
            tail_hash = hasher.hexdigest()
            target = self.path.with_name(self.path.name + ".tail-" + tail_hash + ".bin")
            evidence = dict(reason=self.state.reason, valid_bytes=offset, records=self.state.records,
                            prefix_sha256=prefix_hash.hexdigest(),
                            tail_bytes=count, tail_sha256=tail_hash, tail_file=target.name)
            try:
                os.link(temp, target)
            except FileExistsError:
                if _hash_file(target) != tail_hash:
                    raise JournalDamaged("existing tail evidence conflict")
                with target.open("rb") as existing:
                    os.fsync(existing.fileno())
            sync_directory(self.path.parent)
            checkpoint("after_tail")
            marker_data = canonical(evidence)
            marker = self.path.with_name(self.path.name + ".recovery-" + digest(marker_data) + ".json")
            _publish_bytes(marker, marker_data)
            checkpoint("after_marker")
            # Original tail and evidence are durable before the prefix is shortened.
            self._file.truncate(offset)
            os.fsync(self._file.fileno())
            checkpoint("after_truncate")
            self._file.seek(offset)
            self.state.reason = None
            return evidence
        except BaseException:
            self.poisoned = True
            raise
        finally:
            temp.unlink(missing_ok=True)

    def close(self):
        if not self.closed:
            self._file.close()
            self.closed = True

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        self.close()
