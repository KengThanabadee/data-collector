"""One bounded Deribit group's clock/receiver/publication task ownership.

Caller supplies an already running raw lane, frozen metadata/plan and HTTP/quota
owners. Stop/drain this group before stopping its lane or closing HTTP/quota.
No CLI, discovery, cross-venue supervisor or process restart is implied.
"""
import asyncio

from .clocksource import ChronyClockSampler
from .contracts import integer
from .deribit import DeribitBookSession
from .pipeline import SnapshotPipeline


class CaptureStopped(RuntimeError):
    pass


class DeribitCapture:
    """Start only after a fresh aligned clock check and durable owner cutover.

    Any component's terminal state ends this bounded group. Admitted publication
    drains first while raw/clock owners remain available, then the socket, then
    clock sampling. Never cancel an admitted durable operation to shorten stop.
    """
    def __init__(self,sampler,receiver,pipeline,*,warmup_timeout_seconds=180):
        if (not isinstance(sampler,ChronyClockSampler) or not isinstance(receiver,DeribitBookSession) or
                not isinstance(pipeline,SnapshotPipeline)):
            raise ValueError('typed clock, Deribit receiver and snapshot pipeline required')
        integer(warmup_timeout_seconds,'clock warmup timeout',1)
        lane=sampler.lane
        if (warmup_timeout_seconds>600 or receiver.lane is not lane or pipeline.lane is not lane or
                receiver.schedule is not pipeline.schedule or pipeline.schedule.boot_id!=lane._last_observation[2] or
                not lane.health()['running'] or
                not lane.health()['accepting'] or sampler._started or receiver._started or pipeline._started or
                sampler._stop.is_set() or receiver._stop.is_set() or pipeline._stopping or
                any(pipeline.schedule.health()[key] for key in ('started','terminal','stopping')) or
                not pipeline.publisher.health()['accepting'] or
                pipeline.publisher.health()['busy'] or pipeline.publisher.health()['submitted'] or
                any(hasattr(owner,'_capture_owner') for owner in (sampler,receiver,pipeline))):
            raise ValueError('fresh exclusive components, matching boot and one running lane required')
        self.sampler,self.receiver,self.pipeline,self.lane=sampler,receiver,pipeline,lane
        self.warmup_timeout_seconds=warmup_timeout_seconds
        self._stop=asyncio.Event();self._started=self._terminal=self._cancelled=False
        self.phase='idle';self.error=None
        for owner in (sampler,receiver,pipeline):owner._capture_owner=self
        pipeline.schedule.healthy_streams_for_plan=receiver.healthy_streams_for_plan

    def stop(self):
        self._stop.set()
        if self.phase in {'idle','clock_warmup','owner_cutover'}:self.receiver.stop()

    async def _owned(self,future):
        while True:
            try:return await asyncio.shield(future)
            except asyncio.CancelledError:
                self._cancelled=True;self.stop()
                if future.done():return future.result()

    async def _warmup(self,clock):
        checked=-1;deadline=asyncio.get_running_loop().time()+self.warmup_timeout_seconds
        while not self._stop.is_set():
            self.sampler.updated.clear()
            if clock.done():
                clock.result()
                raise CaptureStopped('clock source ended before capture')
            remaining=deadline-asyncio.get_running_loop().time()
            if remaining<=0:raise TimeoutError('clock warmup deadline')
            ack=self.sampler.last_ack
            if ack is not None and ack.assessment.aligned and self.sampler.samples>checked:
                checked=self.sampler.samples
                utc,mono,boot=self.lane._sample()
                # Historical sample alignment alone does not certify now.
                fresh=await self._owned(self.lane.submit_clock_check(utc_ns=utc,monotonic_ns=mono,boot_id=boot))
                if self._cancelled:raise asyncio.CancelledError
                if asyncio.get_running_loop().time()>=deadline:raise TimeoutError('clock warmup deadline')
                if not fresh.timing_valid:raise CaptureStopped('clock check durability unconfirmed')
                if fresh.assessment.aligned:return True
            remaining=max(0,deadline-asyncio.get_running_loop().time())
            changed=asyncio.create_task(self.sampler.updated.wait());stopped=asyncio.create_task(self._stop.wait())
            try:
                done,_=await asyncio.wait((changed,stopped,clock),timeout=remaining,return_when=asyncio.FIRST_COMPLETED)
                if not done:raise TimeoutError('clock warmup deadline')
            finally:
                for task in (changed,stopped):
                    if not task.done():task.cancel()
                await self._owned(asyncio.gather(changed,stopped,return_exceptions=True))
        return False

    async def run(self,session):
        if self._started:raise CaptureStopped('capture group can run once')
        self._started=True;clock=feed=publication=None;failure=None
        try:
            if not self._stop.is_set():
                self.phase='clock_warmup';clock=asyncio.create_task(self.sampler.run())
                if await self._warmup(clock) and not self._stop.is_set():
                    if clock.done():clock.result();raise CaptureStopped('clock source ended before receiver start')
                    self.phase='owner_cutover';feed=asyncio.create_task(self.receiver.run(session))
                    while not self.receiver.declared.is_set() and not self._stop.is_set():
                        if feed.done():
                            feed.result();raise CaptureStopped('receiver ended before owner cutover')
                        if clock.done():
                            clock.result();raise CaptureStopped('clock source ended before owner cutover')
                        # Wait on both component owners, not a polling timer.
                        declared=asyncio.create_task(self.receiver.declared.wait())
                        stopped=asyncio.create_task(self._stop.wait())
                        try:await asyncio.wait((declared,stopped,feed,clock),return_when=asyncio.FIRST_COMPLETED)
                        finally:
                            for task in (declared,stopped):
                                if not task.done():task.cancel()
                            await self._owned(asyncio.gather(declared,stopped,return_exceptions=True))
                    if not self._stop.is_set():
                        if feed.done():feed.result();raise CaptureStopped('receiver ended before timer start')
                        if clock.done():clock.result();raise CaptureStopped('clock source ended before timer start')
                        self.phase='capturing';publication=asyncio.create_task(self.pipeline.run())
                        stopped=asyncio.create_task(self._stop.wait())
                        try:
                            done,_=await asyncio.wait((clock,feed,publication,stopped),return_when=asyncio.FIRST_COMPLETED)
                            for task in (clock,feed,publication):
                                if task in done:task.result()
                        finally:
                            stopped.cancel();await self._owned(asyncio.gather(stopped,return_exceptions=True))
        except asyncio.CancelledError:
            self._cancelled=True;self.stop()
        except Exception as exc:
            failure=exc;self.error=type(exc).__name__;self.stop()
        finally:
            self.phase='draining'
            # Keep feed/clock alive until a leased publication and report drain.
            self.pipeline.stop()
            if publication is None:
                self.receiver.stop()
                closing=asyncio.create_task(self.pipeline.publisher.close())
                outcomes=await self._owned(asyncio.gather(closing,return_exceptions=True))
            else:outcomes=await self._owned(asyncio.gather(publication,return_exceptions=True))
            self.receiver.stop()
            if feed is not None:outcomes+=await self._owned(asyncio.gather(feed,return_exceptions=True))
            self.sampler.stop()
            if clock is not None:outcomes+=await self._owned(asyncio.gather(clock,return_exceptions=True))
            for outcome in outcomes:
                if isinstance(outcome,BaseException) and failure is None:
                    failure=outcome;self.error=type(outcome).__name__
            self._terminal=True;self.phase='terminal'
        if self._cancelled:raise asyncio.CancelledError
        if failure is not None:raise failure
        return self.health()

    def health(self):
        return dict(started=self._started,terminal=self._terminal,phase=self.phase,error=self.error,
            clock_samples=self.sampler.samples,clock_failures=self.sampler.failures,
            receiver=self.receiver.health(),pipeline=self.pipeline.health())
