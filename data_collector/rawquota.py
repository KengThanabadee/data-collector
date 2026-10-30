"""Shared one-subscription-per-second reservation for local raw workers."""
import fcntl
import time


def take_slot(path):
    if path is None:return True
    try:return _take(path)
    except OSError as exc:raise RuntimeError('shared subscription budget unavailable') from exc


def _take(path):
    with path.open('r+b',buffering=0) as state:
        try:fcntl.flock(state.fileno(),fcntl.LOCK_EX|fcntl.LOCK_NB)
        except BlockingIOError:return False
        try:
            deadline=int(state.read(64))
            now=time.monotonic_ns()
            if now<deadline:return False
            state.seek(0);state.write(str(now+1_000_000_000).encode());state.truncate()
            # A live-process pacing reservation, not durable capture evidence.
            return True
        finally:fcntl.flock(state.fileno(),fcntl.LOCK_UN)
