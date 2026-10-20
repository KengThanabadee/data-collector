"""Journal -> verified archive -> manifest. No valid commit is overwritten."""
import gzip
import json
import os
from pathlib import Path
from .common import body_bytes, dumps, mkdir, read_json, sha, sync_dir, write_bytes, write_json
from .schema import coverage


def checked_records(data):
    if data and not data.endswith(b"\n"):
        raise ValueError("incomplete JSONL archive")
    records = [json.loads(line) for line in data.splitlines()]
    if [r["record_id"] for r in records] != list(range(len(records))):
        raise ValueError("non-contiguous record IDs")
    for r in records:
        body = body_bytes(r)
        if r["body_sha256"] != (sha(body) if body is not None else None) or r["body_byte_count"] != (len(body) if body is not None else 0):
            raise ValueError("raw body checksum/count mismatch")
    return records


def verify(path):
    path = Path(path)
    manifest = read_json(path / "manifest.json")
    if manifest is None:
        raise ValueError("not committed")
    context_bytes = (path / "context.json").read_bytes()
    packed = (path / "responses.jsonl.gz").read_bytes()
    if sha(context_bytes) != manifest["context_sha256"] or sha(packed) != manifest["archive_sha256"] or len(packed) != manifest["archive_bytes"]:
        raise ValueError("archive/context checksum mismatch")
    data = gzip.decompress(packed)
    records = checked_records(data)
    if sha(data) != manifest["journal_sha256"] or len(records) != manifest["records"]:
        raise ValueError("journal checksum/count mismatch")
    context = json.loads(context_bytes)
    for key in ("job_kind","run_id","currency","scheduled_slot_utc","environment"):
        if manifest[key]!=context[key]:
            raise ValueError("manifest identity mismatch")
    if sha(dumps(context["config"]))!=context["config_sha256"]:
        raise ValueError("config provenance mismatch")
    actual = coverage(context, records)
    for key, value in actual.items():
        if key != "status" and manifest[key] != value:
            raise ValueError("coverage mismatch: " + key)
    if manifest["status"] != "interrupted" and actual["status"] != manifest["status"]:
        raise ValueError("status/coverage mismatch")
    if manifest["tail_bytes"]:
        tail = (path / "truncated-tail.bin").read_bytes()
        if sha(tail) != manifest["tail_sha256"] or len(tail) != manifest["tail_bytes"]:
            raise ValueError("tail checksum mismatch")
    return manifest, records


class Archive:
    def __init__(self, root, context, *, existing=None):
        self.root = Path(root)
        self.path = Path(existing) if existing else self.root / "work" / context["run_id"] / context["currency"]
        if not existing:
            mkdir(self.path.parent)
            self.path.mkdir()
            sync_dir(self.path.parent)
            write_json(self.path / "context.json", context)
            write_bytes(self.path / "journal.jsonl", b"")
        self.context = read_json(self.path / "context.json")
        dest = Path(self.context["destination"])
        if dest.is_absolute() or ".." in dest.parts:
            raise ValueError("unsafe archive destination")
        self.destination = self.root / dest
        self.records = self.read_journal()[1] if (self.path / "journal.jsonl").exists() else []

    def read_journal(self):
        data = (self.path / "journal.jsonl").read_bytes()
        cut = data.rfind(b"\n") + 1
        return data[:cut], checked_records(data[:cut]), data[cut:]

    def update_context(self, **values):
        if (self.destination / "manifest.json").exists():
            raise ValueError("cannot alter committed context")
        self.context.update(values)
        write_json(self.path / "context.json", self.context, replace=True)

    def append(self, record):
        record = dict(record, record_id=len(self.records))
        with (self.path / "journal.jsonl").open("ab") as out:
            out.write(dumps(record))
            out.flush()
            os.fsync(out.fileno())
        self.records.append(record)
        return record

    def checkpoint(self, state):
        data = (self.path / "journal.jsonl").read_bytes()
        write_json(self.path / "lifecycle.json", dict(state=state, journal_bytes=len(data), journal_sha256=sha(data), context_sha256=sha((self.path / "context.json").read_bytes())), replace=True)

    def clean_yield(self):
        marker = read_json(self.path / "lifecycle.json")
        if marker and marker["state"] == "yielded":
            data = (self.path / "journal.jsonl").read_bytes()
            if marker["journal_sha256"] != sha(data) or marker["journal_bytes"] != len(data) or marker["context_sha256"] != sha((self.path / "context.json").read_bytes()) or self.read_journal()[2]:
                raise ValueError("clean-yield checkpoint conflict")
            return True
        return False

    def finalize(self, *, interrupted=False, checkpoint=lambda name: None):
        if (self.destination / "manifest.json").exists():
            manifest, _ = verify(self.destination)
            if (self.path / "journal.jsonl").exists():
                data, _, tail = self.read_journal()
                if (sha(data) != manifest["journal_sha256"] or (sha(tail) if tail else None) != manifest["tail_sha256"] or sha((self.path / "context.json").read_bytes()) != manifest["context_sha256"]):
                    raise ValueError("journal/context conflict with existing commit")
                (self.path / "journal.jsonl").unlink()
                sync_dir(self.path)
            self.cleanup()
            return manifest
        data, records, tail = self.read_journal()
        context_data = (self.path / "context.json").read_bytes()
        mkdir(self.destination)
        for name, content in (("context.json", context_data), ("truncated-tail.bin", tail)):
            if name == "truncated-tail.bin" and not tail:
                continue
            target = self.destination / name
            if target.exists():
                if target.read_bytes() != content:
                    raise ValueError("publication conflict: " + name)
            else:
                write_bytes(target, content)
        checkpoint("before_compression")
        packed = gzip.compress(data, compresslevel=6, mtime=0)
        if gzip.decompress(packed) != data:
            raise ValueError("compression round-trip failure")
        checkpoint("after_compression")
        target = self.destination / "responses.jsonl.gz"
        if target.exists():
            packed = target.read_bytes()
            if gzip.decompress(packed) != data:
                raise ValueError("uncommitted archive conflict")
        else:
            write_bytes(target, packed)
        checkpoint("after_archive")
        manifest = dict(schema_version=1, **{k: self.context[k] for k in ("job_kind", "run_id", "currency", "scheduled_slot_utc", "environment")},
                        **coverage(self.context, records), records=len(records), journal_sha256=sha(data),
                        archive_sha256=sha(packed), archive_bytes=len(packed), context_sha256=sha(context_data),
                        tail_bytes=len(tail), tail_sha256=sha(tail) if tail else None,
                        destination=self.context["destination"])
        if interrupted or tail:
            manifest["status"] = "interrupted"
        write_json(self.destination / "manifest.json", manifest)
        checkpoint("after_manifest")
        verify(self.destination)
        (self.path / "journal.jsonl").unlink()
        sync_dir(self.path)
        checkpoint("after_cleanup")
        self.cleanup()
        return manifest

    def cleanup(self):
        # Only known, redundant work files are removed after verifying a commit.
        for name in ("context.json","lifecycle.json"):
            (self.path/name).unlink(missing_ok=True)
        if self.path.exists():
            sync_dir(self.path)
            try:
                self.path.rmdir()
                sync_dir(self.path.parent)
                if not any(self.path.parent.iterdir()):
                    self.path.parent.rmdir()
                    sync_dir(self.path.parent.parent)
            except OSError:
                pass  # unknown temporary evidence is deliberately retained


def recover_all(root):
    results = []
    for path in sorted((Path(root) / "work").glob("*/*/context.json")):
        archive = Archive(root, {}, existing=path.parent)
        if not (path.parent / "journal.jsonl").exists():
            if (archive.destination / "manifest.json").exists():
                results.append(archive.finalize())
            continue
        if archive.context["job_kind"] == "delivery" and archive.clean_yield():
            continue
        results.append(archive.finalize(interrupted=True))
    return results


def read_archive(path, *, include_incomplete=False):
    manifest, records = verify(path)
    if manifest["status"] != "complete" and not include_incomplete:
        raise ValueError("incomplete archive: explicit include_incomplete required")
    return manifest, records
