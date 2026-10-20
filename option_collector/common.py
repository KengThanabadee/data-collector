"""Small durable filesystem primitives. Writers must hold the process lock."""
import base64
from contextlib import contextmanager
from datetime import datetime, timezone
import fcntl
import hashlib
import json
import os
from pathlib import Path
import tempfile
import time
import uuid


def utc(epoch=None):
    return datetime.fromtimestamp(time.time() if epoch is None else epoch, timezone.utc).isoformat(timespec="milliseconds")


def epoch(value):
    result = datetime.fromisoformat(value.replace("Z", "+00:00"))
    if result.tzinfo is None:
        raise ValueError("timestamp must include timezone")
    return result.timestamp()


def dumps(value):
    return (json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"), allow_nan=False) + "\n").encode()


def sha(data):
    return hashlib.sha256(data).hexdigest()


def sync_dir(path):
    fd = os.open(path, os.O_RDONLY)
    try:
        os.fsync(fd)
    finally:
        os.close(fd)


def mkdir(path):
    path = Path(path)
    if not path.exists():
        mkdir(path.parent)
        path.mkdir(exist_ok=True)
        sync_dir(path.parent)
    return path


def write_bytes(path, data, *, replace=False):
    path = Path(path)
    mkdir(path.parent)
    fd, tmp = tempfile.mkstemp(prefix="." + path.name, dir=path.parent)
    try:
        with os.fdopen(fd, "wb") as out:
            out.write(data)
            out.flush()
            os.fsync(out.fileno())
        if replace:
            os.replace(tmp, path)
        else:
            os.link(tmp, path)  # atomic no-clobber publication, unlike rename
        sync_dir(path.parent)
    finally:
        if os.path.exists(tmp):
            os.unlink(tmp)


def write_json(path, value, *, replace=False):
    write_bytes(path, dumps(value), replace=replace)


def read_json(path, default=None):
    try:
        return json.loads(Path(path).read_bytes())
    except FileNotFoundError:
        return default


def body_fields(body):
    if body is None:
        return dict(raw_body=None, body_encoding=None, body_sha256=None, body_byte_count=0)
    try:
        text = body.decode("utf-8", errors="strict")
        encoding = "utf8"
    except UnicodeDecodeError:
        text, encoding = base64.b64encode(body).decode("ascii"), "base64"
    return dict(raw_body=text, body_encoding=encoding, body_sha256=sha(body), body_byte_count=len(body))


def body_bytes(record):
    if record["raw_body"] is None:
        return None
    if record["body_encoding"] == "utf8":
        return record["raw_body"].encode("utf-8")
    if record["body_encoding"] != "base64":
        raise ValueError("unknown body encoding")
    return base64.b64decode(record["raw_body"], validate=True)


@contextmanager
def process_lock(root, name="collector.lock"):
    path = mkdir(Path(root) / "state") / name
    with path.open("a+b") as out:
        try:
            fcntl.flock(out, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            raise BlockingIOError("collector lock is busy") from None
        try:
            yield
        finally:
            fcntl.flock(out, fcntl.LOCK_UN)


def event(root, kind, **values):
    now = time.time()
    value = dict(event_id=str(uuid.uuid4()), kind=kind, observed_utc=utc(now), **values)
    folder = "lifecycle" if kind in ("pause", "resume") else utc(now)[:10].replace("-", "/")
    write_json(Path(root) / "events" / folder / (value["event_id"] + ".json"), value)
    return value
