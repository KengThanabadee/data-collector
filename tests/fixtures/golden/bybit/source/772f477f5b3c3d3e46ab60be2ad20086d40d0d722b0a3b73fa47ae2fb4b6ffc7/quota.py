"""Venue-local REST admission with shared rolling limits and explicit priority.

Every attempt/retry needs a grant. Granted cost is never refunded: cancellation
cannot prove that an exchange did not receive a request. This is conservative
rolling-window enforcement, not an invented exchange token-bucket/reset policy.
"""
import asyncio
from collections import deque
from dataclasses import dataclass
from enum import IntEnum
import time

from .contracts import VENUES, canonical, integer, text


class Priority(IntEnum):
    RECOVERY = 0
    LIVE = 1
    DISCOVERY = 2
    BACKFILL = 3


class QuotaCapacity(ValueError):
    pass


class QuotaClosed(ValueError):
    pass


class QuotaTimeout(TimeoutError):
    pass


@dataclass(frozen=True)
class QuotaWindow:
    name: str
    capacity: int
    window_ns: int

    def __post_init__(self):
        text(self.name, 'quota pool name')
        if len(self.name.encode('utf-8')) > 128:
            raise ValueError('quota pool name byte bound')
        integer(self.capacity, 'quota capacity', 1)
        integer(self.window_ns, 'quota rolling window', 1)


@dataclass(frozen=True, slots=True)
class Grant:
    venue: str
    ticket: int
    granted_ns: int
    waited_ns: int
    charges: tuple[tuple[str, int], ...]


@dataclass(slots=True)
class _Waiter:
    ticket: int
    charges: tuple[tuple[str, int], ...]
    priority: Priority
    deadline_ns: int
    submitted_ns: int
    bytes: int
    future: asyncio.Future


class _Pool:
    def __init__(self, rule):
        self.rule, self.history, self.used, self.cooldown_ns = rule, deque(), 0, 0

    def prune(self, now):
        while self.history and self.history[0][0] <= now:
            _, cost = self.history.popleft()
            self.used -= cost

    def available_at(self, cost, now, max_history):
        needed = self.used + cost - self.rule.capacity
        release = now
        if needed > 0:
            for expires, amount in self.history:
                needed -= amount
                if needed <= 0:
                    release = expires
                    break
        if len(self.history) >= max_history:
            release = max(release, self.history[0][0])
        return max(release, self.cooldown_ns)


class PriorityQuota:
    """Single event-loop scheduler; FIFO within strict venue-wide priority.

    A blocked higher-priority request reserves its turn; backfill may starve
    rather than consume recovery capacity. Endpoint weights/windows must come
    from the frozen enabled-feed configuration and current access context.
    """
    def __init__(self, venue, windows, *, now_ns=time.monotonic_ns, max_pending=1024,
                 max_pending_bytes=1024*1024, max_history_per_pool=10000):
        if venue not in VENUES:
            raise ValueError('known venue required')
        rules = []
        for rule in windows:
            if len(rules) >= 32:
                raise ValueError('quota pool count bound')
            if not isinstance(rule, QuotaWindow):
                raise ValueError('typed quota windows required')
            rules.append(rule)
        if not rules or len({row.name for row in rules}) != len(rules):
            raise ValueError('distinct nonempty quota pools required')
        for name, value in [('max_pending', max_pending), ('max_pending_bytes', max_pending_bytes),
                            ('max_history_per_pool', max_history_per_pool)]:
            integer(value, name, 1)
        self.venue, self.now_ns = venue, now_ns
        self.max_pending, self.max_pending_bytes, self.max_history = max_pending, max_pending_bytes, max_history_per_pool
        self.pools = {row.name: _Pool(row) for row in rules}
        self.pending, self.pending_bytes, self.next_ticket = {}, 0, 0
        self.closed, self.failure = False, None
        self.grants = self.timeouts = self.cancellations = 0
        self._last_now = None
        self._runner = None
        self._wakeup = asyncio.Event()

    def _now(self):
        now = self.now_ns()
        integer(now, 'quota monotonic observation')
        if self._last_now is not None and now < self._last_now:
            raise ValueError('quota monotonic clock regressed; stop venue acquisition')
        self._last_now = now
        return now

    def _remove(self, ticket):
        row = self.pending.pop(ticket, None)
        if row is not None:
            self.pending_bytes -= row.bytes
        return row

    def _fail(self, error):
        if not self.closed:
            self.failure = str(error)[:256]
        self.closed = True
        for ticket in list(self.pending):
            row = self._remove(ticket)
            if not row.future.done():
                row.future.set_exception(QuotaClosed(self.failure))
        self._wakeup.set()

    def notify(self):
        """Wake after a provider cooldown update or an injected clock advance."""
        self._wakeup.set()

    def defer_until(self, pool, until_ns):
        if self.closed:
            raise QuotaClosed('venue quota closed')
        if pool not in self.pools:
            raise ValueError('unknown quota pool')
        integer(until_ns, 'provider cooldown deadline')
        self.pools[pool].cooldown_ns = max(self.pools[pool].cooldown_ns, until_ns)
        self.notify()

    async def acquire(self, charges, *, priority=Priority.LIVE, deadline_ns):
        if self.closed:
            raise QuotaClosed('venue quota closed')
        if not isinstance(priority, Priority) or not isinstance(charges, dict) or not charges or not set(charges) <= self.pools.keys():
            raise ValueError('explicit priority and known nonempty pool charges required')
        frozen = tuple(sorted(charges.items()))
        for name, cost in frozen:
            integer(cost, 'endpoint quota weight', 1)
            if cost > self.pools[name].rule.capacity:
                raise ValueError('request weight exceeds quota window capacity')
        integer(deadline_ns, 'request admission deadline')
        try:
            now = self._now()
        except Exception as exc:
            self._fail(exc)
            raise
        if deadline_ns <= now:
            raise QuotaTimeout('request admission deadline elapsed')
        size = len(canonical(dict(charges=dict(frozen), priority=int(priority), deadline_ns=deadline_ns))) + 4096
        if len(self.pending) >= self.max_pending or self.pending_bytes + size > self.max_pending_bytes:
            raise QuotaCapacity('venue quota waiter count/byte cap')
        future = asyncio.get_running_loop().create_future()
        ticket = self.next_ticket
        self.next_ticket += 1
        self.pending[ticket] = _Waiter(ticket, frozen, priority, deadline_ns, now, size, future)
        self.pending_bytes += size
        self.notify()
        if self._runner is None or self._runner.done():
            self._runner = asyncio.create_task(self._run())
        try:
            return await future
        except asyncio.CancelledError:
            self.cancellations += 1
            raise
        finally:
            self._remove(ticket)
            self.notify()

    async def _run(self):
        try:
            while self.pending and not self.closed:
                self._wakeup.clear()
                now = self._now()
                for pool in self.pools.values():
                    pool.prune(now)
                for ticket, row in list(self.pending.items()):
                    if row.future.cancelled():
                        self._remove(ticket)
                    elif row.deadline_ns <= now:
                        self._remove(ticket)
                        self.timeouts += 1
                        row.future.set_exception(QuotaTimeout('request expired waiting for quota'))
                if not self.pending:
                    break
                row = min(self.pending.values(), key=lambda value: (value.priority, value.ticket))
                available = max(self.pools[name].available_at(cost, now, self.max_history) for name, cost in row.charges)
                if available <= now:
                    self._remove(row.ticket)
                    for name, cost in row.charges:
                        pool = self.pools[name]
                        pool.used += cost
                        pool.history.append((now + pool.rule.window_ns, cost))
                    self.grants += 1
                    row.future.set_result(Grant(self.venue, row.ticket, now, now-row.submitted_ns, row.charges))
                    continue
                deadline = min(available, min(value.deadline_ns for value in self.pending.values()))
                try:
                    await asyncio.wait_for(self._wakeup.wait(), timeout=min((deadline-now)/1_000_000_000, 60))
                except TimeoutError:
                    pass
        except asyncio.CancelledError:
            self._fail(QuotaClosed('quota scheduler stopped'))
            raise
        except Exception as exc:
            self._fail(exc)

    def health(self):
        return dict(venue=self.venue, closed=self.closed, failure=self.failure,
                    pending=len(self.pending), pending_bytes=self.pending_bytes,
                    grants=self.grants, timeouts=self.timeouts, cancellations=self.cancellations,
                    pools={name:dict(charged=pool.used, history=len(pool.history), cooldown_ns=pool.cooldown_ns)
                           for name, pool in self.pools.items()})

    async def close(self):
        self._fail(QuotaClosed('venue quota closed'))
        if self._runner is not None and not self._runner.done():
            self._runner.cancel()
            try:
                await self._runner
            except asyncio.CancelledError:
                pass
