"""Byte-bounded volatile ingress queues; dequeue does not acknowledge durability.

Single event-loop API. Queued and leased payloads remain charged until ack after
successful durable writing. Overflow/backlog errors must trigger scoped loss
controls in the capture supervisor; these primitives never discard old payloads.
"""
import asyncio
from collections import deque
from dataclasses import dataclass
import time

from .contracts import VENUES, integer


class QueueCapacity(ValueError):
    pass


class QueueBacklog(ValueError):
    pass


class QueueClosed(ValueError):
    pass


@dataclass(frozen=True)
class QueueLimits:
    max_bytes: int = 64 * 1024 * 1024
    max_items: int = 4096
    max_item_bytes: int = 32 * 1024 * 1024
    max_age_ns: int = 5_000_000_000

    def __post_init__(self):
        for name, value in vars(self).items():
            integer(value, name, 1)
        if self.max_item_bytes + 512 > self.max_bytes:
            raise ValueError('queue must accommodate one maximum item and its overhead')


class ByteBudget:
    def __init__(self, limit):
        integer(limit, 'aggregate queue bytes', 1)
        self.limit, self.used = limit, 0

    def reserve(self, size):
        integer(size, 'reserved bytes', 1)
        if self.used + size > self.limit:
            raise QueueCapacity('aggregate ingress byte cap')
        self.used += size

    def release(self, size):
        integer(size, 'released bytes', 1)
        if size > self.used:
            raise ValueError('aggregate queue accounting underflow')
        self.used -= size


@dataclass(frozen=True, slots=True)
class QueueLease:
    payload: bytes
    ordinal: int
    enqueued_ns: int
    late: bool
    clock_valid: bool = True


@dataclass(frozen=True, slots=True)
class _Entry:
    payload: bytes
    ordinal: int
    enqueued_ns: int
    charge: int


class ByteQueue:
    def __init__(self, limits=QueueLimits(), *, budget=None, now_ns=time.monotonic_ns):
        if not isinstance(limits, QueueLimits):
            raise ValueError('typed queue limits required')
        self.limits, self.budget = limits, budget if budget is not None else ByteBudget(limits.max_bytes)
        if not isinstance(self.budget, ByteBudget) or self.budget.limit < limits.max_bytes:
            raise ValueError('shared budget smaller than local queue cap')
        self.now_ns = now_ns
        self._pending = deque()
        self._leased = self._entry = None
        self._used = self._ordinal = 0
        self._open, self._waiting = True, False
        self._last_now = None
        self._clock_failed = False
        self._event = asyncio.Event()

    def _now(self):
        try:
            now = self.now_ns()
            integer(now, 'queue monotonic observation')
            if self._last_now is not None and now < self._last_now:
                raise ValueError('queue monotonic clock regressed; stop this ingress epoch')
        except Exception:
            self._clock_failed = True
            self.close()
            raise
        self._last_now = now
        return now

    def _oldest_age(self, now):
        entry = self._entry if self._entry is not None else self._pending[0] if self._pending else None
        return 0 if entry is None else now - entry.enqueued_ns

    def offer(self, payload):
        if not self._open:
            raise QueueClosed('ingress queue closed')
        if type(payload) is not bytes:
            raise ValueError('immutable serialized payload bytes required')
        if len(payload) > self.limits.max_item_bytes:
            raise QueueCapacity('application/expanded record exceeds item cap')
        now = self._now()
        if self._oldest_age(now) >= self.limits.max_age_ns:
            raise QueueBacklog('ingress persist backlog reached hard deadline')
        cost = len(payload) + 512
        if self._used + cost > self.limits.max_bytes or len(self._pending) + int(self._leased is not None) >= self.limits.max_items:
            raise QueueCapacity('venue ingress queue cap')
        entry = _Entry(payload, self._ordinal, now, cost)
        self.budget.reserve(cost)
        try:
            self._pending.append(entry)
        except BaseException:
            self.budget.release(cost)
            raise
        self._used += cost
        self._ordinal += 1
        self._event.set()
        return entry.ordinal

    async def get(self):
        if self._waiting or self._leased is not None:
            raise ValueError('one serialized consumer/lease per ingress queue')
        self._waiting = True
        try:
            while not self._pending:
                if not self._open:
                    raise QueueClosed('ingress queue drained')
                self._event.clear()
                await self._event.wait()
            now = None
            if not self._clock_failed:
                try:
                    now = self._now()
                except Exception:
                    # Preserve accepted bytes even when timing cannot be trusted.
                    # The consumer must invalidate timing and persist the tail.
                    pass
            entry = self._pending.popleft()
            lease = QueueLease(entry.payload, entry.ordinal, entry.enqueued_ns,
                               now is None or now - entry.enqueued_ns >= self.limits.max_age_ns,
                               clock_valid=not self._clock_failed)
            self._entry, self._leased = entry, lease
            return lease
        finally:
            self._waiting = False

    def _check_lease(self, lease):
        if lease is not self._leased or lease is None:
            raise ValueError('foreign, expired or already acknowledged queue lease')

    def ack(self, lease):
        """Call only after the corresponding durable writer acknowledgement."""
        self._check_lease(lease)
        self.budget.release(self._entry.charge)
        self._used -= self._entry.charge
        self._entry = self._leased = None

    def nack(self, lease):
        """Retry the original FIFO item; no accounting or receipt identity changes."""
        self._check_lease(lease)
        self._pending.appendleft(self._entry)
        self._entry = self._leased = None
        self._event.set()

    def close(self):
        self._open = False
        self._event.set()

    def health(self):
        age = None if self._clock_failed else self._oldest_age(self._now())
        return dict(bytes=self._used, queued=len(self._pending), leased=int(self._leased is not None),
                    oldest_age_ns=age, clock_valid=not self._clock_failed, accepting=self._open,
                    warning=self._clock_failed or self._used * 4 >= self.limits.max_bytes * 3 or age >= self.limits.max_age_ns,
                    resubscribe_allowed=self._open and not self._clock_failed and self._used * 4 <= self.limits.max_bytes and age < self.limits.max_age_ns)


class VenueQueues:
    """Dedicated venue caps with enough aggregate capacity for all local caps.

    This reservation policy prevents one failed/full venue from consuming the
    queue allocation of another. Process/RSS isolation is a separate runtime gate.
    """
    def __init__(self, limits, *, aggregate_bytes, now_ns=time.monotonic_ns):
        if not isinstance(limits, dict) or not limits or not set(limits) <= VENUES or any(
                not isinstance(value, QueueLimits) for value in limits.values()):
            raise ValueError('explicit venue queue limits required')
        self.budget = ByteBudget(aggregate_bytes)
        if sum(value.max_bytes for value in limits.values()) > aggregate_bytes:
            raise ValueError('aggregate queue cap cannot isolate all venue allocations')
        self.queues = {venue: ByteQueue(value, budget=self.budget, now_ns=now_ns) for venue, value in limits.items()}
