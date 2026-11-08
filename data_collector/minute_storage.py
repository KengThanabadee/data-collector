"""Durable hourly minute journals and verified, immutable daily zstd seals."""
import asyncio
from base64 import b64decode, b64encode
from contextlib import contextmanager
from datetime import date, datetime, timedelta, timezone
import gzip
import hashlib
import json
import logging
import os
from pathlib import Path
import shutil
import subprocess
import tempfile
import time

from option_collector.common import dumps, mkdir, process_lock, read_json, sha, sync_dir, write_bytes, write_json


LOGGER = logging.getLogger(__name__)


PERIOD = 60
DAY = 86400
MINUTE_FILES = ("index.jsonl", "context.json", "responses.jsonl.gz", "manifest.json",
                "truncated-tail.bin", "deribit-tickers.json.gz")


def _utc(slot):
    return datetime.fromtimestamp(slot, timezone.utc)


def minute_path(root, slot):
    return Path(root) / "minutes" / _utc(slot).strftime("%Y/%m/%d/%H%M")


def _day_bounds(day):
    parsed = date.fromisoformat(day)
    if parsed.isoformat() != day:
        raise ValueError("expected a YYYY-MM-DD UTC day")
    start = int(datetime.combine(parsed, datetime.min.time(), tzinfo=timezone.utc).timestamp())
    return start, start + DAY


def _hour_path(root, slot):
    stamp = _utc(slot)
    return Path(root) / "hourly" / stamp.date().isoformat() / f"{stamp:%H}.jsonl"


def _daily_paths(root, day):
    directory = Path(root) / "daily"
    return directory / f"{day}.jsonl.zst", directory / f"{day}.manifest.json"


def _digest_file(path):
    with path.open("rb") as source:
        return hashlib.file_digest(source, "sha256").hexdigest()


def _decode_files(entry):
    return {name: b64decode(encoded, validate=True) for name, encoded in entry["files"].items()}


def _read_entry(line):
    if not line.endswith(b"\n"):
        raise ValueError("incomplete hourly journal record")
    entry = json.loads(line)
    if not isinstance(entry, dict):
        raise ValueError("invalid hourly minute record")
    slot = entry.get("slot")
    files = entry.get("files")
    if (entry.get("schema_version") != 1 or type(slot) is not int or slot % PERIOD
            or not isinstance(files, dict) or "index.jsonl" not in files
            or not files.keys() <= set(MINUTE_FILES) or not isinstance(entry.get("references"), dict)):
        raise ValueError("invalid hourly minute record")
    return entry


class HourlyJournal:
    def __init__(self, root):
        self.root = Path(root)
        self.path = None
        self.last_slot = None
        self.last_entry = None
        self.references = set()
        self.scheduled_day = None
        self.seal_task = None

    async def __aenter__(self):
        return self

    async def __aexit__(self, *_):
        if self.seal_task is not None:
            try:
                await asyncio.shield(self.seal_task)
            except asyncio.CancelledError:
                await self.seal_task
                raise

    async def seal_due(self, slot):
        if self.seal_task is not None and self.seal_task.done():
            await self.seal_task
            self.seal_task = None
        day = _utc(slot).date().isoformat()
        if day != self.scheduled_day and self.seal_task is None:
            self.scheduled_day = day
            self.seal_task = asyncio.create_task(asyncio.to_thread(self._seal_prior_days, day))

    def _seal_prior_days(self, day):
        for directory in sorted((self.root / "hourly").glob("????-??-??")):
            if directory.name >= day:
                continue
            try:
                if not _daily_paths(self.root, directory.name)[1].exists():
                    _seal(self.root, directory.name)
            except Exception:
                LOGGER.exception("Could not seal UTC minute day %s under %s", directory.name, self.root)

    def _open_hour(self, path):
        if self.path is None or self.path.parent != path.parent:
            self.references.clear()
        self.path, self.last_slot, self.last_entry = path, None, None
        if not path.exists():
            return
        with path.open("r+b") as file:
            while line := file.readline():
                if not line.endswith(b"\n"):
                    offset = file.tell() - len(line)
                    tail = path.with_name(f"{path.stem}.{offset}.{sha(line)}.partial")
                    if not tail.exists():
                        write_bytes(tail, line)
                    elif tail.read_bytes() != line:
                        raise ValueError("hourly journal tail evidence differs")
                    file.truncate(offset)
                    file.flush()
                    os.fsync(file.fileno())
                    break
                entry = _read_entry(line)
                slot = entry["slot"]
                if _hour_path(self.root, slot) != path or self.last_slot is not None and slot != self.last_slot + PERIOD:
                    raise ValueError("hourly journal minute sequence differs")
                self.last_slot, self.last_entry = slot, entry
                self.references.update(entry["references"])

    def append(self, slot, *, deribit_version=None):
        path = _hour_path(self.root, slot)
        if self.path != path:
            self._open_hour(path)
        directory = minute_path(self.root, slot)
        files = {name: b64encode((directory / name).read_bytes()).decode("ascii")
                 for name in MINUTE_FILES if (directory / name).is_file()}
        if "index.jsonl" not in files:
            raise ValueError("minute index must be committed before journaling")
        if self.last_slot == slot:
            if self.last_entry["files"] != files:
                raise ValueError("hourly journal conflicts with committed minute")
            return
        if self.last_slot is not None and slot != self.last_slot + PERIOD:
            raise ValueError("hourly journal minute sequence differs")

        references = self._minute_references(slot, directory, deribit_version)
        entry = dict(schema_version=1, slot=slot, files=files, references=references)
        mkdir(path.parent)
        created = not path.exists()
        with path.open("ab") as target:
            target.write(dumps(entry))
            target.flush()
            os.fsync(target.fileno())
        if created:
            sync_dir(path.parent)
        self.last_slot, self.last_entry = slot, entry
        self.references.update(references)

    def _minute_references(self, slot, directory, deribit_version):
        label = f"{slot:012d}.json.gz"
        paths = [
            *self.root.glob(f"bybit/reference/*/{label}"),
            *self.root.glob(f"bybit/reference-failures/*/{label}"),
            *self.root.glob(f"binance/reference/{label}"),
            *self.root.glob(f"binance/reference-failures/{label}"),
            *self.root.glob(f"deribit-retirements/{_utc(slot):%Y-%m-%d}/*.json.gz"),
        ]
        hour = slot // 3600 * 3600
        for path in self.root.glob(f"bybit/selection/{hour}-*.json"):
            if read_json(path)["selected_slot_epoch"] == slot:
                paths.append(path)
        versions = {deribit_version}
        ticker = directory / "deribit-tickers.json.gz"
        if ticker.exists():
            versions.add(json.loads(gzip.decompress(ticker.read_bytes()))["metadata_version"])
        paths.extend(self.root / "deribit-metadata" / f"{version:012d}.json.gz"
                     for version in versions if version is not None)
        result = {}
        for path in sorted(paths):
            relative = str(path.relative_to(self.root))
            if relative not in self.references:
                result[relative] = b64encode(path.read_bytes()).decode("ascii")
        return result


def _source_lines(paths):
    for path in paths:
        with path.open("rb") as file:
            for line in file:
                yield path.stem, line


@contextmanager
def _sealed_lines(path):
    with subprocess.Popen(["zstd", "-q", "-d", "-c", str(path)], stdout=subprocess.PIPE,
                          stderr=subprocess.DEVNULL) as process:
        yield ((None, line) for line in process.stdout)
        if process.wait():
            raise ValueError("daily zstd decompression failed")


def _stats(lines, day, *, visit=lambda entry: None):
    start, end = _day_bounds(day)
    source = hashlib.sha256()
    hours = {}
    first = last = None
    count = size = 0
    for declared_hour, line in lines:
        entry = _read_entry(line)
        slot = entry["slot"]
        if not start <= slot < end or last is not None and slot != last + PERIOD:
            raise ValueError("daily minute sequence differs")
        hour = f"{_utc(slot):%H}"
        if declared_hour is not None and hour != declared_hour:
            raise ValueError("minute belongs to a different hourly journal")
        if hour not in hours:
            hours[hour] = [hashlib.sha256(), 0, 0]
        hours[hour][0].update(line)
        hours[hour][1] += 1
        hours[hour][2] += len(line)
        source.update(line)
        if first is None:
            first = slot
        last = slot
        count += 1
        size += len(line)
        visit(entry)
    if first is None or last != end - PERIOD:
        raise ValueError("closed day has incomplete journal coverage")
    return dict(first_slot=first, last_slot=last, minutes=count, source_bytes=size,
                source_sha256=source.hexdigest(),
                hours={hour: dict(sha256=digest.hexdigest(), minutes=minutes, bytes=length)
                       for hour, (digest, minutes, length) in hours.items()})


def _sealed_stats(path, day, *, visit=lambda entry: None):
    with _sealed_lines(path) as lines:
        return _stats(lines, day, visit=visit)


def _compress(paths, target):
    with target.open("wb") as output:
        with subprocess.Popen(["zstd", "-q", "-c", "-3"], stdin=subprocess.PIPE, stdout=output,
                              stderr=subprocess.DEVNULL) as process:
            for path in paths:
                with path.open("rb") as source:
                    shutil.copyfileobj(source, process.stdin)
            process.stdin.close()
            if process.wait():
                raise ValueError("daily zstd compression failed")
        output.flush()
        os.fsync(output.fileno())


def _verified_day(root, day, *, visit=lambda entry: None):
    _day_bounds(day)
    archive, manifest_path = _daily_paths(root, day)
    manifest = read_json(manifest_path)
    if manifest is None:
        raise ValueError("daily archive has no manifest")
    if (manifest.get("schema_version") != 1 or manifest.get("utc_day") != day
            or manifest.get("archive_bytes") != archive.stat().st_size
            or manifest.get("archive_sha256") != _digest_file(archive)):
        raise ValueError("daily archive does not match manifest")
    stats = _sealed_stats(archive, day, visit=visit)
    if any(manifest.get(key) != value for key, value in stats.items()):
        raise ValueError("daily readback does not match manifest")
    return manifest


def verify_minute_day(root, day):
    """Verify both compressed bytes and the decoded minute/hour coverage."""
    return _verified_day(Path(root), day)


def _seal(root, day, *, checkpoint=lambda phase: None):
    _, end = _day_bounds(day)
    state = read_json(root / "minute-grid.json")
    if state is None or state["next_slot"] < end:
        raise ValueError("UTC day is still open")
    archive, manifest_path = _daily_paths(root, day)
    if manifest_path.exists():
        return verify_minute_day(root, day)
    paths = sorted((root / "hourly" / day).glob("??.jsonl"))
    stage_day = minute_path(root, end - PERIOD).parent
    staged_indexes = set(stage_day.glob("*/index.jsonl"))

    def compare_stage(entry):
        directory = minute_path(root, entry["slot"])
        index = directory / "index.jsonl"
        if index in staged_indexes:
            for name, content in _decode_files(entry).items():
                if (directory / name).read_bytes() != content:
                    raise ValueError("hourly journal differs from committed minute files")
            staged_indexes.remove(index)

    expected = _stats(_source_lines(paths), day, visit=compare_stage)
    if staged_indexes:
        raise ValueError("hourly journals omit committed minute indexes")
    mkdir(archive.parent)
    if archive.exists():
        if _sealed_stats(archive, day) != expected:
            raise ValueError("existing daily archive differs from hourly journals")
    else:
        fd, name = tempfile.mkstemp(prefix=f".{day}.", suffix=".zst", dir=archive.parent)
        os.close(fd)
        temporary = Path(name)
        try:
            _compress(paths, temporary)
            if _sealed_stats(temporary, day) != expected:
                raise ValueError("daily zstd round trip differs from hourly journals")
            checkpoint("before_archive")
            os.link(temporary, archive)
            sync_dir(archive.parent)
        finally:
            temporary.unlink(missing_ok=True)
    checkpoint("after_archive")
    manifest = dict(schema_version=1, utc_day=day, archive_bytes=archive.stat().st_size,
                    archive_sha256=_digest_file(archive), **expected)
    write_json(manifest_path, manifest)
    checkpoint("after_manifest")
    return verify_minute_day(root, day)


def seal_minute_day(root, day, *, checkpoint=lambda phase: None):
    """Publish or recover the immutable archive for a closed UTC day."""
    root = Path(root)
    with process_lock(root):
        return _seal(root, day, checkpoint=checkpoint)


def confirm_offsite_copy(root, day, archive_sha256):
    """Record the operator-supplied SHA-256 of the offsite copy."""
    root = Path(root)
    with process_lock(root):
        manifest = verify_minute_day(root, day)
        if manifest["archive_sha256"] != archive_sha256:
            raise ValueError("offsite copy checksum differs")
        path = root / "daily" / f"{day}.offsite.json"
        proof = dict(schema_version=1, archive_sha256=archive_sha256, manifest_sha256=sha(dumps(manifest)))
        previous = read_json(path)
        if previous is None:
            write_json(path, proof)
        elif previous != proof:
            raise ValueError("offsite confirmation differs")
        return proof


def _local_evidence(root, entry):
    slot = entry["slot"]
    directory = minute_path(root, slot)
    files = _decode_files(entry)
    for name in MINUTE_FILES:
        if name in files:
            yield directory / name, files[name]
    if "responses.jsonl.gz" in files:
        work = root / "work" / str(slot) / "deribit"
        yield work / "journal.jsonl", gzip.decompress(files["responses.jsonl.gz"]) + files.get("truncated-tail.bin", b"")
        yield work / "context.json", files["context.json"]


def prune_minute_day(root, day, *, now=None):
    """Remove redundant local evidence only after full verification and retention checks."""
    root = Path(root)
    today = _utc(time.time() if now is None else now).date()
    with process_lock(root):
        removable = []

        def inspect(entry):
            for path, expected in _local_evidence(root, entry):
                if path.is_file():
                    if path.read_bytes() != expected:
                        raise ValueError("local minute evidence differs from the sealed archive")
                    removable.append(path)

        manifest = _verified_day(root, day, visit=inspect)
        if date.fromisoformat(day) >= today - timedelta(days=2):
            proof = read_json(root / "daily" / f"{day}.offsite.json")
            if (not proof or proof.get("archive_sha256") != manifest["archive_sha256"]
                    or proof.get("manifest_sha256") != sha(dumps(manifest))):
                raise ValueError("recent journals require matching offsite confirmation")
        for hour, info in manifest["hours"].items():
            path = root / "hourly" / day / f"{hour}.jsonl"
            if path.is_file():
                if path.stat().st_size != info["bytes"] or _digest_file(path) != info["sha256"]:
                    raise ValueError("hourly journal differs from the sealed archive")
                removable.append(path)
        for path in removable:
            path.unlink()
            if path.name == "index.jsonl":
                sync_dir(path.parent)
        directories = {path.parent for path in removable}
        directories.update(path.parent for path in directories.copy() if path.parent.parent == root / "work")
        for directory in sorted(directories, key=lambda path: len(path.parts), reverse=True):
            try:
                directory.rmdir()
            except OSError:
                sync_dir(directory)
            else:
                sync_dir(directory.parent)
        return len(removable)


def read_minute_files(root, slot):
    """Resolve committed minute files from staging or verified daily readback."""
    root = Path(root)
    path = minute_path(root, slot)
    index = path / "index.jsonl"
    if index.is_file():
        files = {}
        for name in MINUTE_FILES:
            try:
                files[name] = (path / name).read_bytes()
            except FileNotFoundError:
                continue
        # Prune removes the index before any other staged file for this minute.
        if index.is_file():
            return files
    day = _utc(slot).date().isoformat()
    _, manifest = _daily_paths(root, day)
    if manifest.exists():
        found = {}

        def visit(entry):
            if entry["slot"] == slot:
                found.update(_decode_files(entry))

        _verified_day(root, day, visit=visit)
        if found:
            return found
    else:
        hourly = _hour_path(root, slot)
        if hourly.is_file():
            with hourly.open("rb") as source:
                for line in source:
                    entry = _read_entry(line)
                    if entry["slot"] == slot:
                        return _decode_files(entry)
    raise FileNotFoundError(path / "index.jsonl")
