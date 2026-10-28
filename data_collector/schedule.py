"""Bounded recurring local minute barriers, independently consumed for publication.

This owns timer allocations, not socket/clock sampling or publication workers.
One leased closure retains its plan until acknowledged by its consumer. Backlog
halts the venue visibly; it never drops a minute, grows a queue or reduces scope.
Requires a fresh local scope or acknowledged pinned startup recovery.
"""
import asyncio
from dataclasses import dataclass
import time

from .contracts import integer, text
from .cutoffs import MINUTE_NS
from .lane import ArchiveLane,DurableOwnerPlanAck
from .publisher import _charge
from .replay import SnapshotPlan


class ScheduleStopped(ValueError):
    pass


@dataclass(frozen=True, slots=True)
class ScheduledCutoff:
    cutoff_id: int
    close_record_id: str
    close_sha256: str
    deadline_ns: int
    plan: SnapshotPlan


class MinuteCutoffs:
    """One producer run, one outstanding consumer lease; no hidden replay queue."""
    def __init__(self, lane, *, first_cutoff_utc_ns, plan, healthy_streams, boot_id,
                 max_cutoffs=10000, max_plan_bytes=8*1024*1024, wait_until=None,
                 healthy_streams_for_plan=None):
        if not isinstance(lane,ArchiveLane) or not callable(plan) or not callable(healthy_streams):
            raise ValueError('local lane and synchronous plan/health providers required')
        if first_cutoff_utc_ns is not None:integer(first_cutoff_utc_ns,'first cutoff UTC')
        integer(max_cutoffs,'scheduled cutoff bound',1);integer(max_plan_bytes,'schedule plan byte bound',1)
        text(boot_id,'schedule boot identity')
        clock=lane.health()['clock']
        if ((first_cutoff_utc_ns is not None and (first_cutoff_utc_ns%MINUTE_NS or first_cutoff_utc_ns>2**63-1-MINUTE_NS)) or max_cutoffs>10000 or
                len(boot_id.encode())>128 or clock is None or clock['cutoff_scope'] is None or
                clock['cutoff_allocations_submitted']!=clock['cutoff_history_count'] or not clock['recovery_ready'] or
                (clock['recovery_next_utc_ns'] is not None and first_cutoff_utc_ns!=clock['recovery_next_utc_ns']) or
                not lane.health()['accepting']):
            raise ValueError('fresh or recovered configured local minute scope required')
        if wait_until is not None and not callable(wait_until):raise ValueError('cooperative async timer required')
        if healthy_streams_for_plan is not None and not callable(healthy_streams_for_plan):raise ValueError('callable allocated-plan health required')
        self.lane,self.plan,self.healthy_streams,self.boot_id=lane,plan,healthy_streams,boot_id
        self.healthy_streams_for_plan=healthy_streams_for_plan
        self._allocation_lock=asyncio.Lock()
        self.first_cutoff,self.max_cutoffs,self.max_plan_bytes=first_cutoff_utc_ns,max_cutoffs,max_plan_bytes
        self.wait_until=wait_until or self._sleep_until
        self._stop=asyncio.Event();self._available=asyncio.Event()
        self._started=self._terminal=self._cancelled=False
        self._slot=None;self._taken=False;self._observation=None
        self._allocated=self._failed=self._closed=self._acknowledged=0
        self.error=None

    async def _sleep_until(self, deadline):
        await asyncio.sleep(max(0,deadline-time.monotonic_ns())/1_000_000_000)

    def _stamp(self):
        stamp=self.lane._sample()
        if self._observation is not None and (stamp[2]!=self._observation[2] or stamp[1]<self._observation[1]):
            self.lane.halt_snapshot_schedule('snapshot_schedule_clock_invalid')
            raise ScheduleStopped('schedule process clock failed')
        self._observation=stamp
        return stamp

    def _cycle_plan(self,plan=None):
        if plan is None:plan=self.plan()
        if not isinstance(plan,SnapshotPlan) or len(plan.bindings)>self.lane.snapshot_limits.max_books:
            raise ValueError('bounded immutable snapshot plan required')
        _charge(plan,self.max_plan_bytes)
        streams=tuple(sorted({source for binding in plan.bindings
            for source in (binding.ws_source,binding.metadata_source,binding.seed_source) if source is not None},
            key=lambda s:s.connection_id))
        self.lane._cutoff_streams(streams)
        if plan.config_sha256!=self.lane.provenance.config_sha256:raise ValueError('schedule config differs from capture')
        return plan,streams

    async def replace_plan(self,plan):
        """Declare and select a managed plan atomically with the next allocation.

        A cutoff already allocated keeps its frozen plan. Native sources for the
        replacement must start at the exact next durable receipt. Use fresh
        connections when the old cutoff has not closed.
        """
        if not isinstance(plan,SnapshotPlan):raise ValueError('typed replacement owner plan required')
        async with self._allocation_lock:
            if self._stop.is_set() or self._terminal:raise ScheduleStopped('schedule stopped before owner cutover')
            self._cycle_plan(plan)
            utc,mono,_=self._stamp()
            try:ack=await self._owned(self.lane.submit_owner_plan(plan,utc_ns=utc,monotonic_ns=mono))
            except BaseException:
                self.stop();self.lane.halt_snapshot_schedule('snapshot_schedule_failure')
                raise
            if not isinstance(ack,DurableOwnerPlanAck) or not ack.timing_valid or ack.declaration.plan!=plan:
                self.stop();self.lane.halt_snapshot_schedule('snapshot_schedule_failure')
                raise ScheduleStopped('timely exact durable owner plan required')
            self.plan=lambda:plan
            if self._cancelled:raise asyncio.CancelledError
            return ack

    async def _owned(self, future):
        while True:
            try:return await asyncio.shield(future)
            except asyncio.CancelledError:
                self._cancelled=True;self._stop.set()
                if future.done():
                    if future.cancelled():raise ScheduleStopped('owned lane command cancelled')
                    return future.result()

    async def _wait(self, deadline):
        if self._stop.is_set():return False
        timer=asyncio.create_task(self.wait_until(deadline));stopped=asyncio.create_task(self._stop.wait())
        try:
            done,_=await asyncio.wait((timer,stopped),return_when=asyncio.FIRST_COMPLETED)
            if timer in done:timer.result()
            return not self._stop.is_set()
        except asyncio.CancelledError:
            self._cancelled=True;self._stop.set();return False
        finally:
            for task in (timer,stopped):
                if not task.done():task.cancel()
            await self._owned(asyncio.gather(timer,stopped,return_exceptions=True))

    async def _wait_deadline(self, deadline):
        if deadline>2**63-1:raise ScheduleStopped('schedule monotonic range exhausted')
        for _ in range(32):
            if self._stop.is_set():return False
            if self._stamp()[1]>=deadline:return True
            if not await self._wait(deadline):return False
        if not self._stop.is_set() and self._stamp()[1]>=deadline:return True
        raise ScheduleStopped('timer repeatedly woke before saved deadline')

    def stop(self):
        self._stop.set()

    async def take(self):
        """Lease a closed cutoff; ack only after bounded downstream handling."""
        if self._taken:raise ValueError('one cutoff lease must be acknowledged first')
        while self._slot is None:
            if self._terminal:raise StopAsyncIteration
            self._available.clear()
            await self._available.wait()
        if self._taken:raise ValueError('cutoff already leased by another consumer')
        self._taken=True
        return self._slot

    def ack(self, close_record_id):
        """Release handoff capacity. This is not snapshot/delivery certification."""
        if not self._taken or self._slot is None or self._slot.close_record_id!=close_record_id:
            raise ValueError('matching leased cutoff required')
        self._slot=None;self._taken=False;self._acknowledged+=1

    async def run(self):
        if self._started:raise ScheduleStopped('schedule can run once per local scope')
        self._started=True;active=None
        try:
            if self.first_cutoff is None:
                # A fresh runtime selects its first minute after clock warmup,
                # not before the >=60s alignment interval.
                utc,_,_=self._stamp()
                self.first_cutoff=(utc//MINUTE_NS+1)*MINUTE_NS
            for index in range(self.max_cutoffs):
                if self._stop.is_set():break
                label=self.first_cutoff+index*MINUTE_NS
                if label>2**63-1:raise ValueError('schedule UTC range exhausted')
                async with self._allocation_lock:
                    if self._stop.is_set():break
                    plan,streams=self._cycle_plan()
                    utc,mono,_=self._stamp()
                    # Pacing only; never used as a barrier or validity proof.
                    pacing=mono+min(MINUTE_NS,max(0,label-utc))
                    allocation=await self._owned(self.lane.submit_cutoff_allocation(label,streams,plan.dependencies,
                        utc_ns=utc,monotonic_ns=mono,boot_id=self.boot_id))
                self._allocated+=1
                if not allocation.timing_valid:raise ScheduleStopped('allocation durability timing failed')
                if allocation.event['kind']!='allocated':
                    self._failed+=1
                    if not await self._wait_deadline(pacing):break
                    continue
                active=allocation.event['payload']['cutoff_id']
                deadline=allocation.event['payload']['deadline_ns']
                if not await self._wait_deadline(deadline):break
                health=(self.healthy_streams() if self.healthy_streams_for_plan is None else
                    self.healthy_streams_for_plan(plan))
                self.lane._cutoff_streams(health)
                health=tuple(health)
                if not set(health)<=set(streams):raise ValueError('health includes streams outside this cycle')
                utc,mono,_=self._stamp() # same event-loop turn as receipt-stamping barrier admission
                closed=await self._owned(self.lane.submit_cutoff_close(active,health,
                    utc_ns=utc,monotonic_ns=mono,boot_id=self.boot_id))
                active=None
                if not closed.timing_valid:raise ScheduleStopped('cutoff durability timing failed')
                if closed.event['kind']!='closed':self._failed+=1;continue
                self._closed+=1
                if self._slot is not None:
                    self.lane.halt_snapshot_schedule('snapshot_handoff_backlog')
                    raise ScheduleStopped('publication handoff still leased at next close')
                self._slot=ScheduledCutoff(closed.event['payload']['cutoff_id'],closed.record_id,closed.raw_sha256,
                    deadline,plan)
                self._available.set()
        except Exception as exc:
            self.error=type(exc).__name__
            self.lane.halt_snapshot_schedule('snapshot_schedule_failure')
            raise
        finally:
            if active is not None and self.lane.health()['accepting']:
                try:
                    utc,mono,_=self._stamp()
                    await self._owned(self.lane.submit_cutoff_failure(active,utc_ns=utc,monotonic_ns=mono,boot_id=self.boot_id))
                    self._failed+=1
                except Exception as exc:
                    self.error=type(exc).__name__
                    self.lane.halt_snapshot_schedule('snapshot_schedule_failure')
            self._terminal=True;self._available.set()
        if self._cancelled:raise asyncio.CancelledError
        return self.health()

    def health(self):
        return dict(started=self._started,terminal=self._terminal,stopping=self._stop.is_set(),
            allocated=self._allocated,closed=self._closed,failed=self._failed,acknowledged=self._acknowledged,
            outstanding_close_id=None if self._slot is None else self._slot.close_record_id,
            leased=self._taken,error=self.error)
