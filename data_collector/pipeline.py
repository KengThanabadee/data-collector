"""Fresh local timer -> seal -> publish -> archive-report ownership.

One leased cutoff is retained until exact publisher receipt bytes are durably
archived. Stop/cancellation drains admitted operations before publisher unlock.
The caller owns the raw lane and reference sampler; stop this pipeline before
stopping that lane. Hard hangs still need an external process supervisor.
"""
import asyncio

from .contracts import digest
from .deliveries import MAX_REPORT_BYTES, validate_delivery
from .lane import DurableDeliveryAck, DurableSnapshotInputs, SnapshotOrigin
from .publisher import SnapshotDelivery, SnapshotPublisher
from .schedule import MinuteCutoffs


class PipelineStopped(ValueError):
    pass


class SnapshotPipeline:
    """Exclusively owns a fresh timer and publisher for one raw-lane session."""
    def __init__(self, schedule, publisher):
        if not isinstance(schedule,MinuteCutoffs) or not isinstance(publisher,SnapshotPublisher):
            raise ValueError('typed local schedule and publisher required')
        lane=schedule.lane
        expected=SnapshotOrigin(*lane.writer.identity,lane.identity['capture_id'],lane.identity['process_session_id'],
            lane.health()['clock']['cutoff_scope'],lane._last_observation[2])
        state=publisher.health()
        if (schedule.health()['started'] or not lane.health()['running'] or not lane.health()['accepting'] or
                publisher.origin!=expected or not state['accepting'] or state['busy'] or state['submitted'] or
                publisher.max_receipt_bytes>MAX_REPORT_BYTES or hasattr(schedule,'_pipeline_owner') or
                hasattr(publisher,'_pipeline_owner')):
            raise ValueError('fresh exclusive local owners and raw-compatible receipt bound required')
        self.schedule,self.publisher,self.lane=schedule,publisher,lane
        schedule._pipeline_owner=publisher._pipeline_owner=object()
        self._started=self._terminal=self._stopping=self._cancelled=False
        self._producer=None;self._current=None;self._last_close=None
        self._received=self._reported=self._published=self._failed=self._unconfirmed=0
        self.phase='idle';self.error=None

    def stop(self):
        self._stopping=True
        self.schedule.stop()

    def _halt(self, exc):
        self.error=type(exc).__name__
        self.stop()
        self.lane.halt_snapshot_schedule('snapshot_publication_failure')

    async def _owned(self, future):
        while True:
            try:return await asyncio.shield(future)
            except asyncio.CancelledError:
                self._cancelled=True;self.stop()
                if future.done():
                    if future.cancelled():raise PipelineStopped('owned pipeline operation cancelled')
                    return future.result()

    async def _deliver(self, cutoff):
        self._current=cutoff;self._received+=1;self.phase='sealing'
        utc,mono,_=self.lane._sample()
        inputs=await self._owned(self.lane.submit_snapshot_inputs(cutoff.close_record_id,utc_ns=utc,monotonic_ns=mono))
        if (not isinstance(inputs,DurableSnapshotInputs) or not inputs.timing_valid or
                (inputs.close_record_id,inputs.close_sha256,inputs.cutoff_id,inputs.deadline_ns)!=(
                    cutoff.close_record_id,cutoff.close_sha256,cutoff.cutoff_id,cutoff.deadline_ns)):
            raise PipelineStopped('timely exact scheduled snapshot inputs required')
        self.phase='publishing'
        delivery=await self._owned(self.publisher.submit(inputs,cutoff.plan))
        if (not isinstance(delivery,SnapshotDelivery) or type(delivery.receipt_body) is not bytes or
                len(delivery.receipt_body)>MAX_REPORT_BYTES or digest(delivery.receipt_body)!=delivery.receipt_sha256):
            raise PipelineStopped('publisher receipt acknowledgement differs')
        value=validate_delivery(delivery.receipt_body)
        if (value['close_record_id'],value['outcome'],value['reason'],value['snapshot_id'])!=(
                cutoff.close_record_id,delivery.outcome,delivery.reason,delivery.snapshot_id):
            raise PipelineStopped('publisher receipt result differs')
        self.phase='archiving'
        utc,mono,_=self.lane._sample()
        ack=await self._owned(self.lane.submit_snapshot_delivery(inputs,delivery.receipt_body,utc_ns=utc,monotonic_ns=mono))
        if not isinstance(ack,DurableDeliveryAck) or not ack.timing_valid or ack.receipt_sha256!=delivery.receipt_sha256:
            raise PipelineStopped('delivery report archival unconfirmed or untimely')
        self._reported+=1
        if delivery.outcome=='published':self._published+=1
        else:self._failed+=1
        self.schedule.ack(cutoff.close_record_id)
        self._last_close=cutoff.close_record_id;self._current=None;self.phase='waiting'

    async def run(self):
        if self._started:raise PipelineStopped('pipeline can run once')
        self._started=True;failure=None;self.phase='waiting'
        self._producer=asyncio.create_task(self.schedule.run())
        try:
            while True:
                try:cutoff=await self._owned(asyncio.create_task(self.schedule.take()))
                except StopAsyncIteration:break
                await self._deliver(cutoff)
        except Exception as exc:
            failure=exc
            if self._current is not None:self._unconfirmed+=1
            self._halt(exc)
        finally:
            self.stop();self.phase='stopping'
            outcomes=await self._owned(asyncio.gather(self._producer,return_exceptions=True))
            if isinstance(outcomes[0],BaseException) and failure is None:
                failure=PipelineStopped('cutoff producer failed: '+type(outcomes[0]).__name__)
                self._halt(failure)
            try:await self._owned(asyncio.create_task(self.publisher.close()))
            except Exception as exc:
                if failure is None:failure=exc;self._halt(exc)
            self._terminal=True;self.phase='terminal'
        if failure is not None:raise failure
        if self._cancelled:raise asyncio.CancelledError
        return self.health()

    def health(self):
        return dict(started=self._started,terminal=self._terminal,stopping=self._stopping,
            phase=self.phase,error=self.error,received=self._received,reported=self._reported,
            published=self._published,failed=self._failed,unconfirmed=self._unconfirmed,
            current_close_id=None if self._current is None else self._current.close_record_id,
            current_cutoff_deadline_ns=None if self._current is None else self._current.deadline_ns,
            last_reported_close_id=self._last_close,schedule=self.schedule.health(),publisher=self.publisher.health())
