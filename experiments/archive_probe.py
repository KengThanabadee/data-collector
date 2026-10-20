"""Experimental archive protocol; not the production collector.

Each received record is fsynced before acknowledgement. A committed manifest is
the publication boundary. Recovery never trusts a gzip file without validating
it against either the retained journal or an existing committed manifest.
"""
import base64
import gzip
import hashlib
import json
import os
from pathlib import Path


def encoded_body(body):
    try:
        return {"body_encoding": "utf-8", "raw_body": body.decode("utf-8")}
    except UnicodeDecodeError:
        return {"body_encoding": "base64", "raw_body": base64.b64encode(body).decode("ascii")}


def decoded_body(record):
    value = record["raw_body"]
    return value.encode("utf-8") if record["body_encoding"] == "utf-8" else base64.b64decode(value, validate=True)


def digest(data):
    return hashlib.sha256(data).hexdigest()


def sync_dir(path):
    fd = os.open(str(path), os.O_RDONLY)
    try:
        os.fsync(fd)
    finally:
        os.close(fd)


def atomic_json(path, value):
    path = Path(path)
    temporary = path.with_name(path.name + ".tmp")
    with temporary.open("wb") as output:
        output.write((json.dumps(value, ensure_ascii=False, sort_keys=True, indent=2) + "\n").encode())
        output.flush()
        os.fsync(output.fileno())
    os.replace(temporary, path)
    sync_dir(path.parent)


class Journal:
    def __init__(self, path, context):
        self.path = Path(path)
        self.path.mkdir(parents=True, exist_ok=False)
        atomic_json(self.path / "context.json", context)
        self.output = (self.path / "journal.jsonl").open("xb")
        sync_dir(self.path)
        self.count = 0
        self.fsync_seconds = 0.0

    def append(self, record):
        import time
        record = dict(record, record_id=self.count)
        data = (json.dumps(record, ensure_ascii=False, separators=(",", ":")) + "\n").encode()
        started = time.monotonic()
        self.output.write(data)
        self.output.flush()
        os.fsync(self.output.fileno())
        self.fsync_seconds += time.monotonic() - started
        self.count += 1

    def close(self):
        if not self.output.closed:
            self.output.close()


def journal_records(path):
    data = Path(path).read_bytes()
    cut = data.rfind(b"\n") + 1
    complete, tail = data[:cut], data[cut:]
    records = [json.loads(line) for line in complete.splitlines()]
    if [record["record_id"] for record in records] != list(range(len(records))):
        raise ValueError("non-contiguous or duplicate record IDs")
    for record in records:
        if "raw_body" in record and digest(decoded_body(record)) != record["body_sha256"]:
            raise ValueError("raw body checksum mismatch")
    return complete, records, tail


def verify(path):
    path = Path(path)
    manifest = json.loads((path / "manifest.json").read_text())
    packed = (path / "responses.jsonl.gz").read_bytes()
    if digest(packed) != manifest["archive_sha256"]:
        raise ValueError("archive checksum mismatch")
    data = gzip.decompress(packed)
    records = [json.loads(line) for line in data.splitlines()]
    if digest(data) != manifest["journal_sha256"] or len(records) != manifest["records"]:
        raise ValueError("journal checksum/count mismatch")
    if [record["record_id"] for record in records] != list(range(len(records))):
        raise ValueError("record ID mismatch")
    for record in records:
        if "raw_body" in record and digest(decoded_body(record)) != record["body_sha256"]:
            raise ValueError("raw body checksum mismatch")
    tail_bytes = manifest.get("truncated_tail_bytes", 0)
    if tail_bytes:
        tail = (path / "truncated-tail.bin").read_bytes()
        if len(tail) != tail_bytes or digest(tail) != manifest["truncated_tail_sha256"]:
            raise ValueError("preserved tail checksum/length mismatch")
    return manifest, records


def finalize(path, status="captured", checkpoint=None):
    path = Path(path)
    if (path / "manifest.json").exists():
        return recover(path)

    complete, records, tail = journal_records(path / "journal.jsonl")
    if tail:
        tail_path = path / "truncated-tail.bin"
        if tail_path.exists() and tail_path.read_bytes() != tail:
            raise ValueError("different truncated-tail evidence already exists")
        with tail_path.open("wb") as output:
            output.write(tail)
            output.flush()
            os.fsync(output.fileno())
        sync_dir(path)
        # Preserve the original journal until commit. Only complete lines enter
        # the published archive, and the tail remains a separate evidence file.
    if checkpoint:
        checkpoint("before_compression")

    temporary = path / "responses.jsonl.gz.tmp"
    with temporary.open("wb") as output:
        with gzip.GzipFile(fileobj=output, mode="wb", filename="", mtime=0, compresslevel=6) as compressor:
            compressor.write(complete[:max(1, len(complete) // 2)])
            compressor.flush()
            output.flush()
            os.fsync(output.fileno())
            if checkpoint:
                checkpoint("during_compression")
            compressor.write(complete[max(1, len(complete) // 2):])
        output.flush()
        os.fsync(output.fileno())
    packed = temporary.read_bytes()
    if gzip.decompress(packed) != complete:
        raise ValueError("compression round-trip failure")
    published = path / "responses.jsonl.gz"
    if published.exists():
        # Never overwrite an unexplained archive during recovery.
        if gzip.decompress(published.read_bytes()) != complete:
            raise ValueError("uncommitted archive differs from durable journal")
        temporary.unlink()
        packed = published.read_bytes()
    else:
        os.replace(temporary, published)
    sync_dir(path)
    if checkpoint:
        checkpoint("after_archive")
    manifest = {
        "schema_version": 1, "status": status, "records": len(records),
        "journal_bytes": len(complete), "archive_bytes": len(packed),
        "journal_sha256": digest(complete), "archive_sha256": digest(packed),
        "truncated_tail_bytes": len(tail),
        "truncated_tail_sha256": digest(tail) if tail else None,
        # Model normal commit only. This is not a measured filesystem peak and
        # excludes context/manifest/blocks. Recovery can also retain an archive
        # and another gzip.tmp simultaneously.
        "normal_commit_payload_disk_bytes_estimate": len(complete) + 2 * len(tail) + len(packed),
    }
    atomic_json(path / "manifest.json", manifest)
    if checkpoint:
        checkpoint("after_manifest")
    # A manifest can refer to complete lines plus preserved tail evidence. Its
    # commit makes removing the original journal safe.
    verify(path)
    (path / "journal.jsonl").unlink()
    sync_dir(path)
    if checkpoint:
        checkpoint("after_cleanup")
    return manifest


def recover(path):
    path = Path(path)
    if (path / "manifest.json").exists():
        manifest, _ = verify(path)
        if (path / "journal.jsonl").exists():
            complete, _, tail = journal_records(path / "journal.jsonl")
            if digest(complete) != manifest["journal_sha256"]:
                raise ValueError("journal does not match committed manifest")
            if len(tail) != manifest["truncated_tail_bytes"]:
                raise ValueError("tail does not match committed manifest")
            if tail and digest(tail) != manifest["truncated_tail_sha256"]:
                raise ValueError("tail checksum mismatch")
            (path / "journal.jsonl").unlink()
            sync_dir(path)
        return manifest
    return finalize(path, status="interrupted")
