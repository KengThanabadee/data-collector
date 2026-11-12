"""Bounded streaming publication; a manifest is the final commit marker."""
from __future__ import annotations

from dataclasses import asdict, dataclass
import gzip
import hashlib
import os
from pathlib import Path
import re
import uuid

from .contracts import RawRecord, VENUES, canonical, digest, integer, sha256, strict_json, text
from .journal import COPY_BUFFER, FRAME, MAGIC, Journal, JournalLimits, _publish_bytes, sync_directory


@dataclass(frozen=True)
class SegmentContext:
    venue: str
    environment: str
    capture_id: str
    process_session_id: str
    segment_id: str
    source_sha256: str
    config_sha256: str

    def __post_init__(self):
        if self.venue not in VENUES:
            raise ValueError("unknown venue")
        for name in ("environment", "capture_id", "process_session_id", "segment_id"):
            text(getattr(self, name), name)
        sha256(self.source_sha256, "source_sha256")
        sha256(self.config_sha256, "config_sha256")

    @property
    def context_id(self):
        return digest(canonical(asdict(self)))


@dataclass(frozen=True)
class SegmentLimits:
    max_record_bytes: int = JournalLimits().max_record_bytes
    max_uncompressed_bytes: int = 128 * 1024 * 1024
    max_compressed_bytes: int = 128 * 1024 * 1024
    max_manifest_bytes: int = 8 * 1024 * 1024
    max_streams: int = 4096
    max_versions: int = 4096
    max_recoveries: int = 128
    max_evidence_bytes: int = 64 * 1024 * 1024

    def __post_init__(self):
        for name, value in asdict(self).items():
            integer(value, name, 1)
        if self.max_record_bytes > JournalLimits().max_record_bytes:
            raise ValueError("record bound exceeds schema maximum")


def _mkdir(path):
    if not path.exists():
        _mkdir(path.parent)
        path.mkdir(exist_ok=True)
    # Retry may see a directory whose creator died before syncing its name.
    sync_directory(path.parent)


def _read_json(path, limit):
    with path.open("rb") as src:
        data = src.read(limit + 1)
    if len(data) > limit:
        raise ValueError("metadata exceeds byte bound")
    return strict_json(data), data


def _file_hash(path, limit):
    result, count = hashlib.sha256(), 0
    with path.open("rb") as src:
        while chunk := src.read(min(COPY_BUFFER, limit - count + 1)):
            count += len(chunk)
            if count > limit:
                raise ValueError("file exceeds byte bound")
            result.update(chunk)
    return result.hexdigest(), count


def _publish_file(temp, target, expected_hash, limit):
    if _file_hash(temp, limit)[0] != expected_hash:
        raise ValueError("staged file checksum mismatch")
    try:
        os.link(temp, target)
    except FileExistsError:
        if _file_hash(target, limit)[0] != expected_hash:
            raise ValueError("immutable publication conflict: " + target.name)
        with target.open("rb") as src:
            os.fsync(src.fileno())
    sync_directory(target.parent)


class _Stats:
    def __init__(self, context, limits):
        self.context, self.limits = context, limits
        self.count = self.bytes = 0
        self.hash = hashlib.sha256()
        self.wal_hash = hashlib.sha256(MAGIC)
        self.wal_bytes = len(MAGIC)
        self.streams = {}
        self.versions = set()

    def add(self, encoded):
        if len(encoded) > self.limits.max_record_bytes:
            raise ValueError("record exceeds byte bound")
        record = RawRecord.from_bytes(encoded)
        if b"\n" in encoded:
            raise ValueError("WAL envelope must be single-line for JSONL publication")
        c = self.context
        if (record.venue, record.environment, record.receipt.capture_id,
                record.receipt.process_session_id, record.provenance.source_sha256,
                record.provenance.config_sha256) != (
                    c.venue, c.environment, c.capture_id, c.process_session_id,
                    c.source_sha256, c.config_sha256):
            raise ValueError("record conflicts with segment context")
        stream = record.receipt.stream_key
        current = self.streams.get(stream)
        seq = record.receipt.receive_seq
        if current is None:
            if len(self.streams) >= self.limits.max_streams:
                raise ValueError("stream count bound exceeded")
            self.streams[stream] = dict(first=seq, last=seq, records=1)
        else:
            if seq != current["last"] + 1:
                raise ValueError("receipt order mismatch")
            current["last"] = seq
            current["records"] += 1
        for version in (record.provenance.universe_sha256, record.provenance.metadata_sha256):
            if version is not None:
                self.versions.add(version)
                if len(self.versions) > self.limits.max_versions:
                    raise ValueError("provenance version bound exceeded")
        self.count += 1
        self.bytes += len(encoded) + 1
        if self.bytes > self.limits.max_uncompressed_bytes:
            raise ValueError("uncompressed archive exceeds bound")
        self.hash.update(encoded + b"\n")
        self.wal_hash.update(FRAME.pack(len(encoded), hashlib.sha256(encoded).digest()))
        self.wal_hash.update(encoded)
        self.wal_bytes += FRAME.size + len(encoded)
        return record

    def result(self):
        return dict(records=self.count, uncompressed_bytes=self.bytes,
            records_sha256=self.hash.hexdigest(),
            wal_sha256=self.wal_hash.hexdigest(), wal_bytes=self.wal_bytes,
            streams=[dict(stream=asdict(k), **v) for k, v in sorted(self.streams.items(),
                key=lambda pair: (pair[0].capture_id, pair[0].process_session_id, pair[0].connection_id))],
            provenance_versions=sorted(self.versions))


def _copy_evidence(journal, destination, limits):
    recoveries, copied, total = [], set(), 0
    markers = []
    with os.scandir(journal.path.parent) as entries:
        for entry in entries:
            if entry.name.startswith(journal.path.name + ".recovery-") and entry.name.endswith(".json"):
                if len(markers) >= limits.max_recoveries:
                    raise ValueError("recovery count exceeds bound")
                markers.append(Path(entry.path))
    for marker in sorted(markers):
        evidence, data = _read_json(marker, 65536)
        name = evidence["tail_file"]
        sha256(evidence["tail_sha256"], "tail_sha256")
        integer(evidence["tail_bytes"], "tail_bytes")
        if Path(name).name != name or name != journal.path.name + ".tail-" + evidence["tail_sha256"] + ".bin":
            raise ValueError("unsafe tail evidence name")
        if marker.name != journal.path.name + ".recovery-" + digest(data) + ".json":
            raise ValueError("recovery marker identity mismatch")
        total += len(data)
        if name not in copied:
            total += evidence["tail_bytes"]
        if total > limits.max_evidence_bytes:
            raise ValueError("recovery evidence exceeds byte bound")
        source = journal.path.parent/name
        actual_hash, size = _file_hash(source, limits.max_evidence_bytes)
        if (actual_hash, size) != (evidence["tail_sha256"], evidence["tail_bytes"]):
            raise ValueError("tail evidence checksum mismatch")
        if name not in copied:
            temp = destination/(".tail-" + uuid.uuid4().hex)
            try:
                with source.open("rb") as src, temp.open("xb") as dst:
                    while chunk := src.read(COPY_BUFFER):
                        dst.write(chunk)
                    dst.flush(); os.fsync(dst.fileno())
                _publish_file(temp, destination/name, actual_hash, limits.max_evidence_bytes)
            finally:
                temp.unlink(missing_ok=True)
            copied.add(name)
        _publish_bytes(destination/marker.name, data)
        recoveries.append(dict(marker=marker.name, marker_sha256=digest(data), **evidence))
    return recoveries


def seal_segment(journal: Journal, archive_root, context: SegmentContext, *,
                 limits=SegmentLimits(), checkpoint=lambda stage: None):
    """Idempotently commit a frozen journal; keep source WAL and recovery evidence."""
    if journal.identity != (context.venue, context.environment):
        raise ValueError("journal/segment identity mismatch")
    preflight = _Stats(context, limits)
    for encoded in journal.encoded_records(allow_unsealed=True):
        preflight.add(encoded)
    seal = journal.freeze(context.context_id)
    if seal["wal_sha256"] != preflight.result()["wal_sha256"]:
        raise ValueError("journal changed between preflight and freeze")
    checkpoint("after_seal")
    destination = Path(archive_root)/"segments"/context.context_id
    _mkdir(destination)
    _publish_bytes(destination/"context.json", canonical(asdict(context)))
    stats = _Stats(context, limits)
    temp = destination/(".records-" + uuid.uuid4().hex)
    try:
        with temp.open("xb") as packed:
            with gzip.GzipFile(filename="", mode="wb", fileobj=packed, mtime=0) as encoder:
                for encoded in journal.encoded_records():
                    stats.add(encoded)
                    encoder.write(encoded + b"\n")
                    if packed.tell() > limits.max_compressed_bytes:
                        raise ValueError("compressed archive exceeds bound")
            packed.flush(); os.fsync(packed.fileno())
        packed_hash, packed_size = _file_hash(temp, limits.max_compressed_bytes)
        checkpoint("after_compression")
        _publish_file(temp, destination/"records.jsonl.gz", packed_hash, limits.max_compressed_bytes)
        checkpoint("after_archive")
        recoveries = _copy_evidence(journal, destination, limits)
        manifest = dict(schema="dc.segment/2", context_sha256=context.context_id,
            archive_sha256=packed_hash, archive_bytes=packed_size, wal=seal,
            recovery_evidence=recoveries, **stats.result())
        encoded_manifest = canonical(manifest)
        if len(encoded_manifest) > limits.max_manifest_bytes:
            raise ValueError("manifest exceeds bound")
        _verify_contents(destination, manifest, limits)
        _publish_bytes(destination/"manifest.json", encoded_manifest)
        checkpoint("after_manifest")
        return destination
    finally:
        temp.unlink(missing_ok=True)


def _validated_rows(path, context, limits):
    stats = _Stats(context, limits)
    with gzip.open(path, "rb") as src:
        while line := src.readline(limits.max_record_bytes + 2):
            if len(line) > limits.max_record_bytes + 1 or not line.endswith(b"\n"):
                raise ValueError("oversized/incomplete segment record")
            yield stats.add(line[:-1])
    return stats.result()


def verify_segment(path, *, limits=SegmentLimits()):
    path = Path(path)
    manifest, _ = _read_json(path/"manifest.json", limits.max_manifest_bytes)
    return _verify_contents(path, manifest, limits)


def _verify_contents(path, manifest, limits):
    if not isinstance(manifest, dict) or manifest.get("schema") != "dc.segment/2":
        raise ValueError("unknown segment schema")
    context_value, context_data = _read_json(path/"context.json", 65536)
    context = SegmentContext(**context_value)
    if digest(context_data) != manifest["context_sha256"] or context.context_id != manifest["context_sha256"]:
        raise ValueError("segment context checksum mismatch")
    if _file_hash(path/"records.jsonl.gz", limits.max_compressed_bytes) != (
            manifest["archive_sha256"], manifest["archive_bytes"]):
        raise ValueError("compressed archive checksum mismatch")
    iterator = _validated_rows(path/"records.jsonl.gz", context, limits)
    while True:
        try:
            next(iterator)
        except StopIteration as result:
            for key, actual in result.value.items():
                if manifest[key] != actual:
                    raise ValueError("segment record statistics mismatch: " + key)
            break
    expected_seal = dict(schema="dc.wal-seal/2", binding_sha256=context.context_id,
        wal_sha256=manifest["wal_sha256"], bytes=manifest["wal_bytes"], records=manifest["records"],
        identity=[context.venue, context.environment])
    if manifest["wal"] != expected_seal:
        raise ValueError("WAL seal statistics mismatch")
    recoveries = manifest["recovery_evidence"]
    if not isinstance(recoveries, list) or len(recoveries) > limits.max_recoveries:
        raise ValueError("invalid recovery list")
    total, files = 0, set()
    for entry in recoveries:
        for field in ("marker", "tail_file"):
            if not isinstance(entry[field], str) or Path(entry[field]).name != entry[field]:
                raise ValueError("unsafe evidence path")
        evidence, data = _read_json(path/entry["marker"], 65536)
        if digest(data) != entry["marker_sha256"] or entry != dict(
                marker=entry["marker"], marker_sha256=digest(data), **evidence):
            raise ValueError("recovery marker mismatch")
        actual = _file_hash(path/entry["tail_file"], limits.max_evidence_bytes)
        if actual != (entry["tail_sha256"], entry["tail_bytes"]):
            raise ValueError("recovery tail mismatch")
        total += len(data) + (0 if entry["tail_file"] in files else actual[1])
        files.add(entry["tail_file"])
        if total > limits.max_evidence_bytes:
            raise ValueError("recovery evidence exceeds bound")
    return manifest


def iter_segment_records(path, *, limits=SegmentLimits()):
    """Verify whole committed segment before exposing any records to a consumer."""
    verify_segment(path, limits=limits)
    context_value, _ = _read_json(Path(path)/"context.json", 65536)
    return _validated_rows(Path(path)/"records.jsonl.gz", SegmentContext(**context_value), limits)
