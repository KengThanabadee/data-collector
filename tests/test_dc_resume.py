import asyncio
from contextlib import redirect_stderr
from dataclasses import replace
from functools import partial
import io
import threading
import unittest
from unittest.mock import patch

from data_collector.clock import ClockSample
from data_collector.clocklog import SOURCE
from data_collector.contracts import Provenance,RawRecord,Receipt,StreamKey,canonical,digest,strict_json
from data_collector.cutoffs import MINUTE_NS
from data_collector.deliveries import MAX_REPORT_BYTES,replay_delivery_reports
from data_collector.journal import Journal,JournalLimits
from data_collector.lane import ArchiveLane,DurabilityUnknown,PENDING_CLOCK_EPOCH,SnapshotOrigin
from data_collector.metadata import MetadataVersion
from data_collector.pipeline import SnapshotPipeline
from data_collector.publisher import SnapshotPublisher
from data_collector.queues import ByteQueue,QueueLimits
from data_collector.recovery import RecoveryRequest,RecoverySegment
from data_collector.replay import BookBinding,SnapshotPlan
from data_collector.schedule import MinuteCutoffs
from data_collector.segments import SegmentLimits,iter_segment_records
from data_collector.validity import FeedTarget,GapCorrection
from data_collector.writer import SessionWriter,recover_session
from tests import test_dc_pipeline as fixtures
from tests import test_dc_publisher as publisher_fixtures
from tests.helpers import key

C=MINUTE_NS


class ResumeTests(unittest.IsolatedAsyncioTestCase):
    setUp=fixtures.PipelineTests.setUp
    observe=fixtures.PipelineTests.observe
    lane=fixtures.PipelineTests.lane
    start=fixtures.PipelineTests.start
    record=fixtures.PipelineTests.record
    received=fixtures.PipelineTests.received
    clock_request=fixtures.PipelineTests.clock_request
    clock_policy=fixtures.PipelineTests.clock_policy
    ready_cutoffs=fixtures.PipelineTests.ready_cutoffs
    ready=fixtures.PipelineTests.ready
    measured_wait=fixtures.PipelineTests.measured_wait
    records=fixtures.PipelineTests.records
    controls=fixtures.PipelineTests.controls
    schedule=fixtures.PipelineTests.schedule
    pipeline=fixtures.PipelineTests.pipeline
    finish_raw=fixtures.PipelineTests.finish_raw

    async def source(self,*,interrupted=True,gap=False):
        lane=await self.ready();pipeline=self.pipeline(lane)
        await pipeline.run()
        if gap:
            proof=lane._durable[StreamKey('capture','process','connection')]['record_id']
            await lane._disk(partial(lane._cutoffs.ledger.append,GapCorrection('capture',self.plan.bindings[0].target,
                0,None,proof,proof,'synthetic old gap'),utc_ns=self.observe()[0],monotonic_ns=self.mono[0],clock_epoch=lane._clock.assessment.anchor.clock_epoch))
        if interrupted:
            utc,mono,_=self.observe()
            await lane.submit_cutoff_allocation(2*C,[self.ws,self.meta],self.plan.dependencies,utc_ns=utc,monotonic_ns=mono,boot_id='boot')
        # Freeze a simulated crash prefix before fixture cleanup resolves its tail.
        pin=lane._cutoffs.coordinator.pin;ledger_pin=lane._cutoffs.ledger.pin()
        await lane._disk(lane.writer.rotate)
        segments=tuple(RecoverySegment(path,ref['context_sha256'],ref['manifest_sha256'])
            for path,ref in zip(lane.writer.committed,lane.writer.references))
        await self.finish_raw(lane)
        self.source_request=RecoveryRequest(segments,pin,ledger_pin)
        self.next_label=(3 if interrupted else 2)*C
        return self.source_request

    def new_observe(self):return self.new_utc+self.new_mono[0]-10,self.new_mono[0],'restart-process-clock'

    def resumed(self,request,*,provenance=None,max_recovery_ns=300_000_000_000):
        self.new_mono=[10];self.new_utc=self.next_label-100
        self.new_provenance=provenance or Provenance('d'*64,'e'*64,'c'*64)
        root=self.root/'restart';root.mkdir(exist_ok=True)
        writer=SessionWriter(root/'wal',root/'archive',venue='deribit',environment='production',capture_id='capture',
            process_session_id='restart',provenance=self.new_provenance,started_utc_ns=self.new_utc,now_ns=lambda:self.new_mono[0],
            limits=JournalLimits(max_record_bytes=262144,target_bytes=64000,max_streams=32),
            segment_limits=SegmentLimits(max_record_bytes=262144,max_uncompressed_bytes=1048576,max_compressed_bytes=1048576))
        emergency=Journal(root/'emergency.wal',venue='deribit',environment='production',create=True,
            limits=JournalLimits(max_record_bytes=262144))
        queue=ByteQueue(QueueLimits(max_bytes=262144,max_items=8,max_item_bytes=131072),now_ns=lambda:self.new_mono[0])
        lane=ArchiveLane(writer,queue,emergency_writer=emergency,observe=self.new_observe,max_connections=4,
            clock_policy=self.clock_policy(),cutoff_scope='capture',cutoff_ledger_id='ledger',recovery=request,max_recovery_ns=max_recovery_ns)
        self.addCleanup(writer.abort);self.addCleanup(emergency.close)
        lane.test_task=asyncio.create_task(lane.run())
        async def cleanup():
            lane.stop();await asyncio.wait_for(asyncio.gather(lane.test_task,return_exceptions=True),5)
        self.addAsyncCleanup(cleanup)
        return lane

    def recover(self,lane):
        utc,mono,_=self.new_observe()
        return lane.submit_recovery(utc_ns=utc,monotonic_ns=mono)

    async def sample(self,lane,seq,mono):
        self.new_mono[0]=mono;utc,_,_=self.new_observe()
        source=RawRecord(Receipt('capture','restart',SOURCE,seq,utc,mono,'measurement'),'deribit','production',
            'control','clock_measurement','control',b'new process reference '+str(seq).encode(),lane.provenance)
        return await lane.submit_clock(ClockSample(utc,mono,0,0,'new-boot',digest(source.to_bytes())),source)

    async def seed(self,lane):
        inst=key();utc,mono,_=self.new_observe()
        attrs=dict(base_currency='BTC',amount_unit='native',quote_currency='native')
        meta=RawRecord(Receipt('capture','restart','meta',0,utc,mono,PENDING_CLOCK_EPOCH),'deribit','production',
            'option','metadata','metadata_version',canonical(dict(schema='dc.metadata/2',effective_at_ns=utc,attributes=attrs,supersedes=None)),lane.provenance,inst)
        meta_ack=await lane.submit_received(meta)
        version=MetadataVersion(inst,replace(meta.receipt,clock_epoch=meta_ack.clock_epoch),utc,canonical(attrs))
        target=FeedTarget(inst,'deribit_full','restart-owner')
        plan=SnapshotPlan((BookBinding(target,StreamKey('capture','restart','ws'),StreamKey('capture','restart','meta'),version.version_id),),lane.provenance.config_sha256)
        channel=f'book.{inst.native_symbol}.100ms'
        body=canonical(dict(jsonrpc='2.0',method='subscription',params=dict(channel=channel,data=dict(type='snapshot',timestamp=1,
            instrument_name=inst.native_symbol,change_id=500,bids=[['new',200,2]],asks=[['new',201,3]]))))
        native=RawRecord(Receipt('capture','restart','ws',0,utc,mono,PENDING_CLOCK_EPOCH),'deribit','production','option',channel,'ws_message',body,lane.provenance,inst)
        book=await lane.submit_received(native)
        return plan,book

    async def publish_new(self,lane,plan):
        async def wait(deadline):self.new_mono[0]=deadline
        schedule=MinuteCutoffs(lane,first_cutoff_utc_ns=self.next_label,plan=lambda:plan,
            healthy_streams=lambda:(StreamKey('capture','restart','ws'),StreamKey('capture','restart','meta')),
            boot_id='new-boot',max_cutoffs=1,wait_until=wait)
        origin=SnapshotOrigin('deribit','production','capture','restart','capture','restart-process-clock')
        publisher=SnapshotPublisher(self.root/'restart-publisher',origin,observe=self.new_observe,max_receipt_bytes=MAX_REPORT_BYTES)
        self.addAsyncCleanup(publisher.close)
        pipeline=SnapshotPipeline(schedule,publisher)
        state=await asyncio.wait_for(pipeline.run(),5)
        return state,publisher

    async def test_resume_global_ids_new_clock_and_source_config_then_publish_and_replay(self):
        request=await self.source();lane=self.resumed(request)
        ack=await self.recover(lane)
        self.assertTrue(ack.timing_valid)
        self.assertEqual((ack.recovery.failed_cutoff_ids,ack.recovery.closed_cutoff_ids,ack.recovery.next_cutoff_id),((1,),(0,),2))
        self.assertEqual(ack.ledger_pin,request.ledger_pin)
        self.assertEqual(lane.health()['clock']['cutoff_history_count'],2)
        self.assertFalse((await self.sample(lane,0,20)).assessment.aligned)
        self.assertTrue((await self.sample(lane,1,80)).assessment.aligned)
        plan,_=await self.seed(lane);state,publisher=await self.publish_new(lane,plan)
        self.assertEqual((state['reported'],state['published']),(1,1))
        self.assertEqual(lane._cutoffs.coordinator.allocations[2]['payload']['utc_ns'],3*C)
        self.assertEqual(lane._clock.assessment.anchor.clock_epoch,'restart:clock:0')
        self.assertTrue(lane.health()['accepting'])
        await self.finish_raw(lane)
        local=self.records(lane)
        report=replay_delivery_reports(local,origin=publisher.origin)[0]
        self.assertEqual(report.outcome,'published')
        from data_collector.snapshots import read_snapshot
        receipt=next(r for r in local if r.record_id==report.record_id)
        value=strict_json(receipt.body)
        paths=[s.path for s in request.segments]+list(lane.segments)
        snapshot=read_snapshot(publisher.root/'snapshots',value['snapshot_id'],paths)
        self.assertEqual(strict_json(snapshot['payload'])['books'][0]['book']['bids'],[['200','2']])
        self.assertEqual(snapshot['descriptor']['cutoff_id'],2)

    async def test_ledger_prefix_and_new_process_receipt_sequences_are_preserved(self):
        request=await self.source(gap=True);lane=self.resumed(request)
        await self.recover(lane);await self.sample(lane,0,20);await self.sample(lane,1,80)
        plan,book=await self.seed(lane)
        self.assertEqual(lane._cutoffs.ledger.pin(),request.ledger_pin)
        utc,mono,_=self.new_observe()
        pin=await lane._disk(partial(lane._cutoffs.ledger.append,GapCorrection('capture',plan.bindings[0].target,2,None,
            book.record_id,book.record_id,'synthetic new-process gap'),utc_ns=utc,monotonic_ns=mono,clock_epoch=lane._clock.assessment.anchor.clock_epoch))
        self.assertEqual(pin.sequence,request.ledger_pin.sequence+1)
        state,publisher=await self.publish_new(lane,plan)
        self.assertEqual(state['published'],1)
        await self.finish_raw(lane)
        corrections=[r for r in self.records(lane) if r.kind=='coverage_event' and r.channel=='validity:capture']
        self.assertEqual(corrections[0].receipt.receive_seq,0)
        self.assertEqual(strict_json(corrections[0].body)['sequence'],2)
        from data_collector.snapshots import read_snapshot
        body=strict_json(next(r.body for r in self.records(lane) if r.channel=='snapshot_deliveries:capture'))
        snapshot=read_snapshot(publisher.root/'snapshots',body['snapshot_id'],[s.path for s in request.segments]+list(lane.segments))
        self.assertEqual(strict_json(snapshot['payload'])['books'][0]['validity_as_published'],'invalid')

    async def test_live_ingress_and_timer_are_gated_until_recovery_ack(self):
        request=await self.source();lane=self.resumed(request)
        with self.assertRaises(ValueError):await self.sample(lane,0,20)
        self.new_mono[0]=10
        with self.assertRaises(ValueError):MinuteCutoffs(lane,first_cutoff_utc_ns=self.next_label,plan=lambda:self.plan,
            healthy_streams=lambda:(),boot_id='new-boot',max_cutoffs=1)
        future=self.recover(lane)
        with self.assertRaises(ValueError):self.recover(lane)
        ack=await future;self.assertTrue(ack.timing_valid)
        with self.assertRaises(ValueError):MinuteCutoffs(lane,first_cutoff_utc_ns=C,plan=lambda:self.plan,
            healthy_streams=lambda:(),boot_id='new-boot',max_cutoffs=1)

    async def test_wrong_ledger_or_cutoff_pin_yields_no_ready_ack_and_no_live_capture(self):
        request=await self.source();bad=replace(request,ledger_pin=replace(request.ledger_pin,head_hash='f'*64))
        lane=self.resumed(bad)
        with redirect_stderr(io.StringIO()):
            future=self.recover(lane)
            with self.assertRaises(DurabilityUnknown):await future
            await lane.test_task
        self.assertFalse(lane.health()['clock']['recovery_ready'])
        self.assertIsNone(lane.health()['clock']['recovery_marker_id'])
        restored=recover_session(lane.writer.directory,lane.writer.directory.parent.parent/'archive')
        self.assertFalse(any(r.channel=='cutoff_recovery:capture' for p in restored for r in iter_segment_records(p)))

    async def test_wrong_cutoff_tip_is_rejected_after_ledger_validation_without_marker(self):
        request=await self.source();bad=replace(request,cutoff_pin=replace(request.cutoff_pin,last_record_sha256='f'*64))
        lane=self.resumed(bad)
        with redirect_stderr(io.StringIO()):
            with self.assertRaises(DurabilityUnknown):await self.recover(lane)
            await lane.test_task
        self.assertFalse(lane.health()['clock']['recovery_ready'])
        self.assertIsNone(lane.health()['clock']['recovery_marker_id'])

    async def test_old_clock_samples_cannot_align_a_new_process_with_one_sample(self):
        request=await self.source();lane=self.resumed(request)
        await self.recover(lane);self.assertFalse((await self.sample(lane,0,20)).assessment.aligned)
        plan,_=await self.seed(lane);state,_=await self.publish_new(lane,plan)
        self.assertEqual((state['published'],state['reported']),(0,0))
        self.assertEqual(lane._cutoffs.coordinator.resolutions[2]['payload']['reason'],'clock_alignment_unavailable')
        self.assertTrue(lane.health()['accepting'])

    async def test_all_closed_source_resumes_next_id_without_new_interruption(self):
        request=await self.source(interrupted=False);lane=self.resumed(request)
        ack=await self.recover(lane)
        self.assertEqual((ack.recovery.next_cutoff_id,ack.recovery.failed_cutoff_ids),(1,()))
        await self.sample(lane,0,20);await self.sample(lane,1,80)
        plan,_=await self.seed(lane);state,_=await self.publish_new(lane,plan)
        self.assertEqual(state['published'],1)
        self.assertEqual(lane._cutoffs.coordinator.allocations[1]['payload']['utc_ns'],2*C)

    async def test_cancelled_recovery_client_does_not_release_owned_startup_work(self):
        request=await self.source();lane=self.resumed(request)
        entered,release=threading.Event(),threading.Event();self.addCleanup(release.set)
        original=lane._resume_cutoffs
        def blocked(record):
            entered.set()
            if not release.wait(3):raise OSError('test gate timeout')
            return original(record)
        with patch.object(lane,'_resume_cutoffs',side_effect=blocked):
            future=self.recover(lane);self.assertTrue(await asyncio.to_thread(entered.wait,2))
            future.cancel()
            with self.assertRaises(ValueError):await self.sample(lane,0,20)
            release.set()
            async def ready():
                while not lane.health()['clock']['recovery_ready']:await asyncio.sleep(0)
            await asyncio.wait_for(ready(),5)
        self.assertTrue(lane.health()['accepting'])
        self.assertEqual(lane.queue.budget.used,0)

    async def test_cancelled_raw_owner_drains_recovery_but_cannot_resume_live(self):
        request=await self.source();lane=self.resumed(request)
        entered,release=threading.Event(),threading.Event();self.addCleanup(release.set)
        original=lane._resume_cutoffs
        def blocked(record):
            entered.set()
            if not release.wait(3):raise OSError('test gate timeout')
            return original(record)
        with patch.object(lane,'_resume_cutoffs',side_effect=blocked):
            future=self.recover(lane);self.assertTrue(await asyncio.to_thread(entered.wait,2))
            lane.test_task.cancel();await asyncio.sleep(0);self.assertFalse(lane.test_task.done());release.set()
            with self.assertRaises(asyncio.CancelledError):await asyncio.wait_for(lane.test_task,5)
        self.assertIsNotNone((await future).recovery.marker_record_id)
        self.assertFalse(lane.health()['accepting'])
        self.assertEqual(lane.queue.budget.used,0)

    async def test_startup_budget_is_separate_from_five_second_live_durability_budget(self):
        request=await self.source();lane=self.resumed(request)
        original=lane._resume_cutoffs
        def elapsed(record):
            result=original(record);self.new_mono[0]+=6_000_000_000;return result
        with patch.object(lane,'_resume_cutoffs',side_effect=elapsed):ack=await self.recover(lane)
        self.assertTrue(ack.timing_valid)
        self.assertEqual(ack.elapsed_ns,6_000_000_000)
        self.assertTrue(lane.health()['accepting'])

    async def test_startup_deadline_exhaustion_keeps_marker_but_blocks_live_ingress(self):
        request=await self.source();lane=self.resumed(request,max_recovery_ns=100)
        original=lane._resume_cutoffs
        def elapsed(record):
            result=original(record);self.new_mono[0]+=101;return result
        with patch.object(lane,'_resume_cutoffs',side_effect=elapsed):ack=await self.recover(lane)
        self.assertFalse(ack.timing_valid)
        self.assertIsNotNone(ack.recovery.marker_record_id)
        await lane.test_task
        self.assertFalse(lane.health()['clock']['recovery_ready'])
        self.assertEqual(lane.health()['venue_failure'],'cutoff_recovery_deadline_or_clock_invalid')


class HistoricalCloseTests(unittest.IsolatedAsyncioTestCase):
    setUp=publisher_fixtures.PublisherTests.setUp

    async def test_old_unbound_close_cannot_be_certified_in_new_process_clock_domain(self):
        from data_collector.journal import Journal
        from data_collector.cutoffs import CutoffCoordinator,IngressWriter,ClockAnchor
        from data_collector.ledger import DurableLedger
        from data_collector.clocklog import DurableClock,SOURCE
        from data_collector.clock import ClockSample
        from data_collector.contracts import RawRecord,Receipt,digest,strict_json
        from data_collector.segments import seal_segment,SegmentContext
        from data_collector.recovery import RecoverySegment,reconcile_cutoffs
        from data_collector.lane import DurableSnapshotInputs,SnapshotOrigin,SnapshotSegmentInput
        from data_collector.replay import ReplayLimits,SnapshotPlan
        from data_collector.metadata import MetadataVersion
        from data_collector.contracts import canonical
        from data_collector.publisher import SnapshotPublisher,_verify
        fixture=self
        root=fixture.root;prov=fixture.clock.provenance
        native=[]
        for encoded in fixture.wal.encoded_records(allow_unsealed=True):
            record=RawRecord.from_bytes(encoded)
            if not record.receipt.connection_id.startswith('control:'):
                native.append(replace(record,receipt=replace(record.receipt,clock_epoch='restart:clock:0'),native_fields_json=b'{}'))
        md=next(r for r in native if r.kind=='metadata_version');v=strict_json(md.body)
        version=MetadataVersion(md.instrument,md.receipt,v['effective_at_ns'],canonical(v['attributes']))
        plan=SnapshotPlan((replace(fixture.plan.bindings[0],metadata_version_id=version.version_id),),fixture.plan.config_sha256)
        old=Journal(root/'old-unbound.wal',venue='deribit',environment='production',create=True)
        self.addCleanup(old.close)
        ingress=IngressWriter(old,'capture','process')
        coord=CutoffCoordinator(old,capture_scope='capture',capture_id='capture',process_session_id='process',provenance=prov)
        ledger=DurableLedger(old,ledger_id='ledger',capture_scope='capture',capture_id='capture',process_session_id='process',provenance=prov)
        ledger.initialize(utc_ns=C-200,monotonic_ns=900,clock_epoch='restart:clock:0')
        anchor=ClockAnchor('process','restart:clock:0',C-100,1000,0,2)
        coord.allocate(C,[fixture.ws,fixture.meta],plan.dependencies,anchor=anchor,observed_utc_ns=C-100,monotonic_ns=1000,clock_epoch=anchor.clock_epoch)
        for record in native:ingress.append(record)
        barrier=ingress.barrier(anchor,C,now_monotonic_ns=1100,healthy_streams=[fixture.ws,fixture.meta])
        coord.close(0,barrier,ledger_pin=ledger.pin(),utc_ns=C,monotonic_ns=1100,clock_epoch=anchor.clock_epoch)
        close=coord.resolutions[0]
        op=seal_segment(old,root/'archive',SegmentContext('deribit','production','capture','process','old','a'*64,'b'*64))
        rs=RecoverySegment(op,digest((op/'context.json').read_bytes()),digest((op/'manifest.json').read_bytes()))
        new=Journal(root/'new.wal',venue='deribit',environment='production',create=True)
        self.addCleanup(new.close)
        reconcile_cutoffs(new,(rs,),coord.pin,capture_scope='capture',capture_id='capture',process_session_id='restart',provenance=prov,utc_ns=C-1000,monotonic_ns=10,clock_epoch='newclock')
        clock=DurableClock(new,capture_id='capture',process_session_id='restart',provenance=prov,policy=fixture.clock.policy)
        for seq,mono in enumerate((940,1000)):
            source=RawRecord(Receipt('capture','restart',SOURCE,seq,C-1100+mono,mono,'measurement'),'deribit','production','control','clock_measurement','control',b'x'+str(seq).encode(),prov)
            clock.sample(ClockSample(source.receipt.receive_utc_ns,mono,0,0,'newboot',digest(source.to_bytes())),source,ingress_ordinal=0)
        np=seal_segment(new,root/'archive',SegmentContext('deribit','production','capture','restart','new','a'*64,'b'*64))
        ns=SnapshotSegmentInput(np,digest((np/'context.json').read_bytes()),digest((np/'manifest.json').read_bytes()))
        inputs=DurableSnapshotInputs(close['record_id'],close['hash'],0,1100,SnapshotOrigin('deribit','production','capture','restart','capture','newclock'),(SnapshotSegmentInput(op,rs.context_sha256,rs.manifest_sha256),ns),ReplayLimits(),clock.pin,0,True)
        # The old unbound close deliberately shares the new clock epoch label,
        # monotonic deadline and provenance. Receipt origin must still reject it.
        with self.assertRaisesRegex(ValueError,'another process origin'):
            _verify(inputs,plan)
        publisher=SnapshotPublisher(root/'repro-publisher',inputs.origin,observe=lambda:(C,1100,'newclock'))
        self.addAsyncCleanup(publisher.close)
        result=await publisher.submit(inputs,plan)
        self.assertEqual((result.outcome,result.reason,result.snapshot_id),('failed','publication_failed',None))
        self.assertFalse((publisher.root/'snapshots').exists())
