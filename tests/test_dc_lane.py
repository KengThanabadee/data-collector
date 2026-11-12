import asyncio
from contextlib import redirect_stderr
from dataclasses import FrozenInstanceError, replace
import io
import json
from pathlib import Path
import tempfile
import threading
import unittest
from unittest.mock import patch

from data_collector.contracts import Provenance, RawRecord, Receipt, StreamKey, canonical, digest, strict_json
from data_collector.clockcutoffs import verify_clock_binding
from data_collector.cutoffs import MINUTE_NS
from data_collector.replay import BookBinding, ReplayLimits, SnapshotPlan
from data_collector.metadata import MetadataVersion
from data_collector.snapshots import publish_snapshot, read_snapshot
from data_collector.publisher import SnapshotPublisher
from data_collector.deliveries import delivery_base, replay_delivery_reports
from data_collector.validity import FeedTarget
from data_collector.journal import Journal, JournalLimits
from data_collector.lane import ArchiveLane, DurabilityUnknown, LaneStopped, PENDING_CLOCK_EPOCH
from data_collector.queues import ByteQueue, QueueCapacity, QueueLimits, VenueQueues
from data_collector.segments import SegmentLimits, iter_segment_records
from data_collector.writer import SessionWriter, recover_session
from data_collector.clock import ClockSample
from data_collector.clocklog import SOURCE, replay_clock
from tests.helpers import LaneHarness, key


class LaneTests(LaneHarness, unittest.IsolatedAsyncioTestCase):
    def cutoff_request(self,lane,cutoff=MINUTE_NS):
        utc,mono,_=self.observe()
        return lane.submit_cutoff_allocation(cutoff,[StreamKey('capture','process','connection')],
            dict(config='b'*64,universe='c'*64,metadata='d'*64,required_feeds='e'*64),
            utc_ns=utc,monotonic_ns=mono,boot_id='boot')

    def cutoff_close(self,lane):
        utc,mono,_=self.observe()
        return lane.submit_cutoff_close(0,[StreamKey('capture','process','connection')],
            utc_ns=utc,monotonic_ns=mono,boot_id='boot')

    async def ready_cutoffs(self,**kwargs):
        self.utc=MINUTE_NS-100
        lane,emergency=self.lane(clock_policy=self.clock_policy(),cutoffs=True,**kwargs);self.start(lane)
        await asyncio.wait_for(lane.submit_clock(*self.clock_request(lane)),5)
        self.mono[0]+=60
        await asyncio.wait_for(lane.submit_clock(*self.clock_request(lane,1)),5)
        return lane,emergency

    async def closed_cutoff(self,lane):
        await asyncio.wait_for(self.cutoff_request(lane),5)
        await asyncio.wait_for(lane.submit_received(self.received(lane)),5)
        self.mono[0]=1100
        return await asyncio.wait_for(self.cutoff_close(lane),5)

    def snapshot_input_request(self,lane,close_record_id):
        utc,mono,_=self.observe()
        return lane.submit_snapshot_inputs(close_record_id,utc_ns=utc,monotonic_ns=mono)

    async def test_sealed_inputs_publish_and_replay_real_book_while_lane_remains_open(self):
        lane,_=await self.ready_cutoffs()
        inst=key();ws=StreamKey('capture','process','connection');meta=StreamKey('capture','process','meta')
        attrs=dict(base_currency='BTC',amount_unit='native',quote_currency='native')
        metadata=replace(self.received(lane),kind='metadata_version',channel='metadata',
            receipt=replace(self.received(lane).receipt,connection_id='meta'),
            body=canonical(dict(schema='dc.metadata/2',effective_at_ns=MINUTE_NS-100,attributes=attrs,supersedes=None)))
        meta_ack=await asyncio.wait_for(lane.submit_received(metadata),5)
        version=MetadataVersion(inst,replace(metadata.receipt,clock_epoch=meta_ack.clock_epoch),
            MINUTE_NS-100,canonical(attrs))
        plan=SnapshotPlan((BookBinding(FeedTarget(inst,'deribit_full','owner'),ws,meta,version.version_id),),'b'*64)
        utc,mono,_=self.observe()
        await asyncio.wait_for(lane.submit_cutoff_allocation(MINUTE_NS,[ws,meta],plan.dependencies,
            utc_ns=utc,monotonic_ns=mono,boot_id='boot'),5)
        self.mono[0]=1090
        channel=f'book.{inst.native_symbol}.100ms'
        body=canonical(dict(jsonrpc='2.0',method='subscription',params=dict(channel=channel,data=dict(
            type='snapshot',timestamp=1,instrument_name=inst.native_symbol,change_id=10,
            bids=[['new',100,2]],asks=[['new',101,3]]))))
        await asyncio.wait_for(lane.submit_received(replace(self.received(lane),channel=channel,body=body)),5)
        self.mono[0]=1100;utc,mono,_=self.observe()
        closed=await asyncio.wait_for(lane.submit_cutoff_close(0,[ws,meta],utc_ns=utc,monotonic_ns=mono,boot_id='boot'),5)
        inputs=await asyncio.wait_for(self.snapshot_input_request(lane,closed.record_id),5)
        paths=[segment.path for segment in inputs.segments]
        store=self.root/'published-live-prefix'
        publisher=SnapshotPublisher(store,inputs.origin,observe=self.observe);self.addAsyncCleanup(publisher.close)
        entered,release=threading.Event(),threading.Event();self.addCleanup(release.set)
        def blocked(*args,**kwargs):
            entered.set()
            if not release.wait(3):raise OSError('test gate timeout')
            return publish_snapshot(*args,**kwargs)
        with patch('data_collector.publisher.publish_snapshot',side_effect=blocked):
            delivery=publisher.submit(inputs,plan)
            self.assertTrue(await asyncio.to_thread(entered.wait,2))
            self.mono[0]+=1
            raw_ack=await asyncio.wait_for(lane.submit_received(self.received(lane,1)),5)
            self.assertTrue(raw_ack.timing_valid)
            self.assertFalse(delivery.done())
            release.set();delivered=await asyncio.wait_for(delivery,5)
        self.assertEqual(delivered.outcome,'published')
        result=await asyncio.to_thread(read_snapshot,store/'snapshots',delivered.snapshot_id,paths)
        self.assertEqual(result['descriptor']['close_sha256'],inputs.close_sha256)
        self.assertEqual(strict_json(result['payload'])['books'][0]['validity_as_published'],'valid')
        self.assertEqual(strict_json(result['payload'])['books'][0]['book']['bids'],[['100','2']])
        self.assertEqual(await asyncio.to_thread(read_snapshot,store/'snapshots',result['snapshot_id'],paths),result)
        self.assertTrue(lane.health()['accepting'])
        self.assertFalse(lane.writer.closed)
        receipt=delivered.receipt_path.read_bytes()
        self.mono[0]+=1;utc,mono,_=self.observe()
        ack=await asyncio.wait_for(lane.submit_snapshot_delivery(inputs,receipt,utc_ns=utc,monotonic_ns=mono),5)
        self.assertTrue(ack.timing_valid)
        self.assertEqual(ack.receipt_sha256,delivered.receipt_sha256)
        lane.stop();await asyncio.wait_for(lane.test_task,5)
        records=self.records(lane)
        reports=replay_delivery_reports(records,origin=inputs.origin)
        self.assertEqual((reports[0].outcome,reports[0].receipt_sha256),('published',digest(receipt)))
        archived=next(r for r in records if r.record_id==ack.record_id)
        self.assertEqual(archived.body,receipt)
        self.assertGreater(archived.receipt.monotonic_ns,strict_json(receipt)['completed']['monotonic_ns'])

    async def delivery_fixture(self):
        lane,emergency=await self.ready_cutoffs()
        closed=await self.closed_cutoff(lane)
        inputs=await self.snapshot_input_request(lane,closed.record_id)
        stamp=dict(utc_ns=self.observe()[0],monotonic_ns=self.mono[0],process_timing_epoch='epoch',valid=True)
        body=canonical(dict(delivery_base(inputs),plan_sha256='e'*64,started=stamp,completed=stamp,
            outcome='failed',reason='publication_failed',snapshot_id=None,error_type='OSError'))
        return lane,emergency,inputs,body

    def delivery_request(self,lane,inputs,body):
        utc,mono,_=self.observe()
        return lane.submit_snapshot_delivery(inputs,body,utc_ns=utc,monotonic_ns=mono)

    async def test_delivery_reports_are_fifo_idempotent_and_exact_after_restore(self):
        lane,_,inputs,body=await self.delivery_fixture()
        first=self.delivery_request(lane,inputs,body)
        later=lane.submit_received(self.received(lane,1))
        ack,raw_ack=await asyncio.gather(first,later)
        self.mono[0]+=1
        retry=await self.delivery_request(lane,inputs,body)
        self.assertEqual((retry.record_id,retry.raw_sha256,retry.receive_seq),(ack.record_id,ack.raw_sha256,0))
        lane.stop();await asyncio.wait_for(lane.test_task,5)
        records=self.records(lane)
        reports=replay_delivery_reports(records,origin=inputs.origin)
        self.assertEqual(len(reports),1)
        self.assertLess(next(i for i,r in enumerate(records) if r.record_id==ack.record_id),
                        next(i for i,r in enumerate(records) if r.record_id==raw_ack.record_id))
        restored=recover_session(lane.writer.directory,lane.writer.directory.parent.parent/'archive')
        self.assertEqual(replay_delivery_reports((r for p in restored for r in iter_segment_records(p)),origin=inputs.origin),reports)
        self.assertEqual(lane.health()['clock']['delivery_handoffs_without_report'],0)

    async def test_bad_or_conflicting_delivery_is_rejected_before_queue_admission(self):
        lane,_,inputs,body=await self.delivery_fixture()
        before=lane.queue.health()['bytes']
        for data in (body+b' ',canonical(dict(strict_json(body),close_sha256='f'*64)),b'x'*65537):
            with self.assertRaises(ValueError):self.delivery_request(lane,inputs,data)
        with self.assertRaises(ValueError):self.delivery_request(lane,replace(inputs,timing_valid=False),body)
        with self.assertRaises(ValueError):self.delivery_request(lane,replace(inputs,segments=inputs.segments*1025),body)
        self.assertEqual(lane.queue.health()['bytes'],before)
        first=self.delivery_request(lane,inputs,body)
        with self.assertRaises(ValueError):self.delivery_request(lane,inputs,canonical(dict(strict_json(body),error_type='ValueError')))
        await first
        self.assertTrue(lane.health()['accepting'])

    async def test_delivery_admission_clock_and_request_bound_fail_visibly(self):
        lane,_,inputs,body=await self.delivery_fixture()
        for utc,mono in ((True,1100),(self.utc,-1),(2**63,1100),(self.utc,1099)):
            with self.assertRaises(ValueError):lane.submit_snapshot_delivery(inputs,body,utc_ns=utc,monotonic_ns=mono)
        self.assertEqual(lane.health()['clock']['delivery_requests_submitted'],0)
        lane._delivery_requests_submitted=10000
        with self.assertRaises(QueueCapacity):self.delivery_request(lane,inputs,body)
        await asyncio.wait_for(lane.test_task,5)
        self.assertEqual(lane.health()['venue_failure'],'snapshot_delivery_request_bound')

    async def test_cancelled_delivery_write_drains_and_retains_original_raw_report(self):
        lane,_,inputs,body=await self.delivery_fixture()
        entered,release=threading.Event(),threading.Event();self.addCleanup(release.set)
        append=lane.writer.append
        def gated(record):
            if record.channel.startswith('snapshot_deliveries:'):
                entered.set()
                if not release.wait(3):raise OSError('test gate timeout')
            return append(record)
        with patch.object(lane.writer,'append',side_effect=gated):
            report=self.delivery_request(lane,inputs,body)
            later=lane.submit_received(self.received(lane,1))
            self.assertTrue(await asyncio.to_thread(entered.wait,2))
            report.cancel();lane.test_task.cancel();await asyncio.sleep(0)
            self.assertFalse(lane.test_task.done());release.set()
            with self.assertRaises(asyncio.CancelledError):await asyncio.wait_for(lane.test_task,5)
        self.assertTrue((await later).timing_valid)
        reports=replay_delivery_reports(self.records(lane),origin=inputs.origin)
        self.assertEqual(reports[0].receipt_sha256,digest(body))
        self.assertEqual(lane.queue.budget.used,0)

    async def test_delivery_write_failure_is_unconfirmed_and_emergency_reports_missing_outcome(self):
        lane,emergency,inputs,body=await self.delivery_fixture();append=lane.writer.append
        def broken(record):
            if record.channel.startswith('snapshot_deliveries:'):raise OSError('report write failed')
            return append(record)
        with patch.object(lane.writer,'append',side_effect=broken),redirect_stderr(io.StringIO()):
            report=self.delivery_request(lane,inputs,body)
            await asyncio.wait_for(lane.test_task,5)
        with self.assertRaises(DurabilityUnknown):await report
        with Journal(emergency,venue='deribit',environment='production') as journal:
            event=strict_json(RawRecord.from_bytes(next(journal.encoded_records(allow_unsealed=True))).body)
        self.assertEqual(event['payload']['clock']['delivery_handoffs_without_report'],1)
        self.assertEqual(event['payload']['clock']['delivery_requests_submitted'],1)

    async def test_delivery_queue_overflow_stops_ingress_and_preserves_missing_report_count(self):
        lane,_,inputs,body=await self.delivery_fixture()
        with patch.object(lane.queue,'offer',side_effect=QueueCapacity('full')):
            with self.assertRaises(QueueCapacity):self.delivery_request(lane,inputs,body)
        await asyncio.wait_for(lane.test_task,5)
        self.assertEqual(lane.health()['venue_failure'],'snapshot_delivery_queue_admission_failed')
        self.assertEqual(lane.health()['clock']['delivery_handoffs_without_report'],1)

    async def test_snapshot_inputs_seal_before_later_raw_and_retry_pins_first_prefix(self):
        lane,_=await self.ready_cutoffs()
        closed=await self.closed_cutoff(lane)
        request=self.snapshot_input_request(lane,closed.record_id)
        with self.assertRaises(ValueError):self.snapshot_input_request(lane,closed.record_id)
        later=lane.submit_received(self.received(lane,1))
        inputs,later_ack=await asyncio.wait_for(asyncio.gather(request,later),5)
        self.assertEqual(inputs.close_sha256,closed.raw_sha256)
        self.assertEqual(inputs.cutoff_id,0)
        self.assertEqual(inputs.deadline_ns,1100)
        self.assertEqual(inputs.origin.process_timing_epoch,'epoch')
        self.assertEqual(inputs.origin.process_session_id,'process')
        self.assertEqual(inputs.origin.capture_scope,'capture')
        self.assertTrue(inputs.timing_valid)
        self.assertEqual(inputs.clock_pin,closed.clock_pin)
        self.assertTrue(inputs.segments)
        frozen=[]
        for segment in inputs.segments:
            encoded=(segment.path/'manifest.json').read_bytes()
            self.assertEqual(digest(encoded),segment.manifest_sha256)
            self.assertEqual(strict_json(encoded)['context_sha256'],segment.context_sha256)
            frozen.extend(iter_segment_records(segment.path))
            with self.assertRaises(FrozenInstanceError):segment.manifest_sha256='a'*64
        self.assertTrue(any(r.record_id==closed.record_id and digest(r.to_bytes())==inputs.close_sha256 for r in frozen))
        self.assertFalse(any(r.record_id==later_ack.record_id for r in frozen))
        self.mono[0]+=1
        await asyncio.wait_for(self.clock_check(lane),5)
        count=len(lane.writer.committed)
        retry=await asyncio.wait_for(self.snapshot_input_request(lane,closed.record_id),5)
        self.assertEqual(retry.segments,inputs.segments)
        self.assertEqual(retry.clock_pin,inputs.clock_pin)
        self.assertEqual(len(lane.writer.committed),count)
        self.assertGreater(lane._clock.pin.count,retry.clock_pin.count)
        lane.stop();await asyncio.wait_for(lane.test_task,5)
        self.assertTrue(any(r.record_id==later_ack.record_id for r in self.records(lane)))
        self.assertFalse(any(r.channel=='snapshot_input_request' for r in self.records(lane)))

    async def test_snapshot_inputs_require_closed_identity_and_bounded_observation(self):
        disabled,_=self.lane()
        with self.assertRaises(ValueError):self.snapshot_input_request(disabled,'a'*64)
        lane,_=await self.ready_cutoffs()
        with self.assertRaises(ValueError):self.snapshot_input_request(lane,'a'*64)
        closed=await self.closed_cutoff(lane)
        before=lane.queue.health()['bytes']
        with self.assertRaises(ValueError):self.snapshot_input_request(lane,'b'*64)
        for utc,mono in ((-1,1100),(True,1100),(self.utc,2**63)):
            with self.assertRaises(ValueError):lane.submit_snapshot_inputs(closed.record_id,utc_ns=utc,monotonic_ns=mono)
        self.assertEqual(lane.queue.health()['bytes'],before)
        self.assertEqual(lane.health()['clock']['snapshot_requests_submitted'],0)

    async def test_snapshot_input_seal_failure_is_unconfirmed_and_recoverable(self):
        lane,emergency=await self.ready_cutoffs()
        closed=await self.closed_cutoff(lane)
        with patch.object(lane.writer,'rotate',side_effect=OSError('injected seal failure')),redirect_stderr(io.StringIO()):
            request=self.snapshot_input_request(lane,closed.record_id)
            later=lane.submit_received(self.received(lane,1))
            await asyncio.wait_for(lane.test_task,5)
        for future in (request,later):
            with self.assertRaises(DurabilityUnknown):await future
        with Journal(emergency,venue='deribit',environment='production') as journal:
            event=strict_json(RawRecord.from_bytes(next(journal.encoded_records(allow_unsealed=True))).body)
        self.assertTrue(event['payload']['clock']['snapshot_inputs_pending'])
        self.assertEqual(event['payload']['clock']['last_closed_record_id'],closed.record_id)
        recovered=recover_session(lane.writer.directory,lane.writer.directory.parent.parent/'archive')
        records=[r for path in recovered for r in iter_segment_records(path)]
        close=next(r for r in records if r.record_id==closed.record_id)
        self.assertEqual(digest(close.to_bytes()),closed.raw_sha256)
        verify_clock_binding(lambda:iter(records),close)

    async def test_cancelled_snapshot_seal_drains_later_raw_without_exposing_mutable_wal(self):
        lane,_=await self.ready_cutoffs()
        closed=await self.closed_cutoff(lane)
        entered,release=threading.Event(),threading.Event();self.addCleanup(release.set)
        original=lane.writer.rotate
        def gated():
            entered.set()
            if not release.wait(3):raise OSError('test gate timeout')
            return original()
        with patch.object(lane.writer,'rotate',side_effect=gated):
            request=self.snapshot_input_request(lane,closed.record_id)
            later=lane.submit_received(self.received(lane,1))
            self.assertTrue(await asyncio.to_thread(entered.wait,2))
            request.cancel();lane.test_task.cancel();await asyncio.sleep(0)
            self.assertFalse(lane.test_task.done())
            release.set()
            with self.assertRaises(asyncio.CancelledError):await asyncio.wait_for(lane.test_task,5)
        later_ack=await later
        _,references,_=lane._snapshot_inputs
        frozen=[r for ref in references for r in iter_segment_records(ref.path)]
        self.assertTrue(any(r.record_id==closed.record_id for r in frozen))
        self.assertFalse(any(r.record_id==later_ack.record_id for r in frozen))
        self.assertFalse(lane.health()['clock']['snapshot_inputs_pending'])
        self.assertEqual(lane.queue.budget.used,0)

    async def test_snapshot_prefix_bounds_fail_before_extra_seal(self):
        for limits in (ReplayLimits(max_records=1),ReplayLimits(max_scan_bytes=1),ReplayLimits(max_segments=1)):
            with self.subTest(limits=limits):
                self.mono[0]=1000
                lane,_=await self.ready_cutoffs(snapshot_limits=limits)
                closed=await self.closed_cutoff(lane)
                await lane._disk(lane.writer.rotate)
                await asyncio.wait_for(lane.submit_received(self.received(lane,1)),5)
                count=len(lane.writer.committed)
                with redirect_stderr(io.StringIO()):
                    future=self.snapshot_input_request(lane,closed.record_id)
                    await asyncio.wait_for(lane.test_task,5)
                with self.assertRaises(DurabilityUnknown):await future
                self.assertEqual(len(lane.writer.committed),count)
                self.assertIsNone(lane._snapshot_inputs)

    async def test_snapshot_input_queue_overflow_drains_previously_accepted_raw(self):
        q=ByteQueue(QueueLimits(max_bytes=32768,max_items=1,max_item_bytes=8192),now_ns=lambda:self.mono[0])
        lane,_=await self.ready_cutoffs(queue=q)
        closed=await self.closed_cutoff(lane)
        later=lane.submit_received(self.received(lane,1))
        with self.assertRaises(QueueCapacity):self.snapshot_input_request(lane,closed.record_id)
        await asyncio.wait_for(later,5);await asyncio.wait_for(lane.test_task,5)
        self.assertEqual(lane.health()['venue_failure'],'snapshot_input_queue_admission_failed')
        self.assertEqual(lane.health()['clock']['snapshot_requests_submitted'],0)
        self.assertEqual(lane.queue.budget.used,0)

    async def test_cutoff_configuration_and_invalid_requests_do_not_consume_queue_state(self):
        base,emergency_path=self.lane()
        common=dict(emergency_writer=base.emergency_writer,observe=self.observe,max_connections=4)
        cases=[dict(cutoff_scope='scope'),dict(cutoff_scope='scope',cutoff_ledger_id='ledger'),
            dict(cutoff_scope='x'*129,cutoff_ledger_id='ledger',clock_policy=self.clock_policy()),
            dict(cutoff_scope='clock-assessment',cutoff_ledger_id='ledger',clock_policy=self.clock_policy()),
            dict(cutoff_scope='runtime:deribit',cutoff_ledger_id='ledger',clock_policy=self.clock_policy()),
            dict(cutoff_scope='validity:ledger',cutoff_ledger_id='ledger',clock_policy=self.clock_policy()),
            dict(cutoff_scope='scope',cutoff_ledger_id='ledger',clock_policy=dict(self.clock_policy(),max_offset_ns=100_000_001))]
        for kwargs in cases:
            with self.subTest(config=kwargs),self.assertRaises(ValueError):
                ArchiveLane(base.writer,base.queue,**common,**kwargs)
        with patch.object(base.writer,'limits',replace(base.writer.limits,max_streams=7)):
            with self.assertRaises(ValueError):
                ArchiveLane(base.writer,base.queue,**common,clock_policy=self.clock_policy(),
                            cutoff_scope='scope',cutoff_ledger_id='ledger')
        lane,_=await self.ready_cutoffs()
        stream=StreamKey('capture','process','connection')
        deps=dict(config='b'*64,universe='c'*64,metadata='d'*64,required_feeds='e'*64)
        utc,mono,_=self.observe()
        for cutoff,streams,dependencies,boot in [
                (MINUTE_NS+1,[stream],deps,'boot'),(MINUTE_NS,[],deps,'boot'),
                (MINUTE_NS,[stream,stream],deps,'boot'),
                (MINUTE_NS,[StreamKey('capture','other','connection')],deps,'boot'),
                (MINUTE_NS,[stream],{},'boot'),(MINUTE_NS,[stream],deps,'x'*129)]:
            with self.assertRaises(ValueError):
                lane.submit_cutoff_allocation(cutoff,streams,dependencies,
                    utc_ns=utc,monotonic_ns=mono,boot_id=boot)
        self.assertEqual(lane.health()['clock']['cutoff_commands_submitted'],0)
        self.assertEqual(lane.queue.budget.used,0)
        self.assertTrue(lane.health()['accepting'])

    async def test_async_cutoffs_share_fifo_with_equal_time_raw_and_verified_clock_links(self):
        lane,_=await self.ready_cutoffs()
        allocation=self.cutoff_request(lane)
        with self.assertRaises(ValueError):self.cutoff_close(lane)
        self.mono[0]=1099
        before=lane.submit_received(self.received(lane))
        allocated=await asyncio.wait_for(allocation,5)
        self.assertEqual(allocated.event['kind'],'allocated')
        self.assertTrue(allocated.timing_valid)
        self.mono[0]=1100
        equal_before=lane.submit_received(self.received(lane,1))
        close=self.cutoff_close(lane)
        with self.assertRaises(ValueError):self.cutoff_close(lane)
        equal_after=lane.submit_received(self.received(lane,2))
        self.mono[0]=1101
        after=lane.submit_received(self.received(lane,3))
        first,a,closed,b,c=await asyncio.wait_for(asyncio.gather(before,equal_before,close,equal_after,after),5)
        self.assertEqual([ack.ingress_ordinal for ack in (first,a,b,c)],[0,1,3,4])
        payload=closed.event['payload']
        self.assertEqual(closed.event['kind'],'closed')
        self.assertEqual(payload['validity'],'valid')
        self.assertEqual(payload['barrier']['ordinal'],2)
        self.assertEqual(payload['barrier']['deadline_ns'],1100)
        self.assertEqual(payload['streams'][0]['admitted_seq'],1)
        self.assertTrue(closed.timing_valid)
        self.assertEqual(closed.clock_pin.count,4)
        lane.stop();await asyncio.wait_for(lane.test_task,5)
        records=self.records(lane)
        close_record=next(r for r in records if r.record_id==closed.record_id)
        self.assertEqual(digest(close_record.to_bytes()),closed.raw_sha256)
        self.assertEqual(strict_json(close_record.body),closed.event)
        self.assertFalse(any(r.channel=='cutoff_request' for r in records))
        verify_clock_binding(lambda:iter(records),close_record)
        self.assertEqual(self.clock_replay(lane).pin,closed.clock_pin)
        self.assertEqual(lane.queue.budget.used,0)

    async def test_async_utc_step_fails_cutoff_but_continues_raw_in_new_epoch(self):
        lane,_=await self.ready_cutoffs()
        await asyncio.wait_for(self.cutoff_request(lane),5)
        before=await asyncio.wait_for(lane.submit_received(self.received(lane)),5)
        self.mono[0]=1100;self.utc+=500_000_000
        close=self.cutoff_close(lane)
        after=lane.submit_received(self.received(lane,1))
        failed,raw_ack=await asyncio.wait_for(asyncio.gather(close,after),5)
        self.assertEqual(failed.event['kind'],'failed')
        self.assertEqual(failed.event['payload']['reason'],'clock_changed_or_stale')
        self.assertTrue(failed.timing_valid)
        self.assertTrue(raw_ack.timing_valid)
        self.assertNotEqual(before.clock_epoch,raw_ack.clock_epoch)
        self.assertTrue(lane.health()['accepting'])
        lane.stop();await asyncio.wait_for(lane.test_task,5)
        self.assertFalse(any(strict_json(r.body).get('kind')=='closed'
                             for r in self.records(lane) if r.channel=='cutoffs:capture'))

    async def test_graceful_stop_fails_unresolved_async_allocation(self):
        lane,_=await self.ready_cutoffs()
        allocated=await asyncio.wait_for(self.cutoff_request(lane),5)
        with self.assertRaises(ValueError):self.cutoff_close(lane)
        with self.assertRaises(ValueError):self.cutoff_request(lane,MINUTE_NS*2)
        lane.stop();await asyncio.wait_for(lane.test_task,5)
        cutoff_events=[strict_json(r.body) for r in self.records(lane) if r.channel=='cutoffs:capture']
        self.assertEqual([event['kind'] for event in cutoff_events],['allocated','failed'])
        self.assertEqual(cutoff_events[1]['payload']['reason'],'capture_stopped_before_cutoff')
        self.assertEqual(cutoff_events[1]['payload']['allocation_hash'],allocated.raw_sha256)
        self.assertIsNone(lane.health()['clock']['active_cutoff'])

    async def test_unaligned_and_missed_allocations_fail_durably_without_reopening_labels(self):
        self.utc=MINUTE_NS-100
        lane,_=self.lane(clock_policy=self.clock_policy(),cutoffs=True);self.start(lane)
        await asyncio.wait_for(lane.submit_clock(*self.clock_request(lane)),5)
        failed=await asyncio.wait_for(self.cutoff_request(lane),5)
        self.assertEqual(failed.event['kind'],'failed')
        self.assertEqual(failed.event['payload']['reason'],'clock_alignment_unavailable')
        with self.assertRaises(ValueError):self.cutoff_request(lane)
        self.mono[0]+=60
        await asyncio.wait_for(lane.submit_clock(*self.clock_request(lane,1)),5)
        # A healthy clock cannot manufacture a schedule for a missed UTC label.
        self.mono[0]+=1
        self.utc+=MINUTE_NS*2
        await asyncio.wait_for(self.clock_check(lane),5)
        self.mono[0]+=1
        await asyncio.wait_for(lane.submit_clock(*self.clock_request(lane,2)),5)
        self.mono[0]+=60
        healthy=await asyncio.wait_for(lane.submit_clock(*self.clock_request(lane,3)),5)
        self.assertTrue(healthy.assessment.aligned)
        missed=await asyncio.wait_for(self.cutoff_request(lane,MINUTE_NS*2),5)
        self.assertEqual(missed.event['kind'],'failed')
        self.assertEqual(missed.event['payload']['reason'],'cutoff_schedule_unavailable')
        self.assertTrue(lane.health()['accepting'])
        lane.stop();await asyncio.wait_for(lane.test_task,5)
        allocations=[strict_json(r.body)['payload'] for r in self.records(lane)
                     if r.channel=='cutoffs:capture' and strict_json(r.body)['kind']=='allocated']
        self.assertEqual([a['cutoff_id'] for a in allocations],[0,1])
        self.assertTrue(all(a['clock_anchor'] is None and a['deadline_ns'] is None for a in allocations))

    async def test_cutoff_checks_share_clock_operation_cap_and_stop_resolves_open_allocation(self):
        lane,_=await self.ready_cutoffs();lane._clock.max_assessments=3
        allocated=await asyncio.wait_for(self.cutoff_request(lane),5)
        self.mono[0]=1100
        with self.assertRaises(QueueCapacity):self.cutoff_close(lane)
        await asyncio.wait_for(lane.test_task,5)
        self.assertEqual(lane.health()['venue_failure'],'clock_log_bound')
        self.assertEqual(lane.health()['clock']['pin']['count'],3)
        self.assertEqual(lane.health()['clock']['cutoff_commands_submitted'],1)
        self.assertEqual(lane._cutoffs.coordinator.resolutions[0]['payload']['allocation_hash'],allocated.raw_sha256)

    async def test_cancellation_during_cutoff_close_preserves_barrier_and_later_raw(self):
        lane,_=await self.ready_cutoffs()
        await asyncio.wait_for(self.cutoff_request(lane),5)
        await asyncio.wait_for(lane.submit_received(self.received(lane)),5)
        self.mono[0]=1100
        entered,release=threading.Event(),threading.Event();self.addCleanup(release.set)
        original=lane.writer.append
        def gated(record):
            if record.channel=='cutoffs:capture' and strict_json(record.body)['kind']=='closed':
                entered.set()
                if not release.wait(3):raise OSError('test gate timeout')
            return original(record)
        with patch.object(lane.writer,'append',side_effect=gated):
            future=self.cutoff_close(lane)
            later=lane.submit_received(self.received(lane,1))
            self.assertTrue(await asyncio.to_thread(entered.wait,2))
            future.cancel();lane.test_task.cancel();await asyncio.sleep(0)
            self.assertFalse(lane.test_task.done())
            release.set()
            with self.assertRaises(asyncio.CancelledError):await asyncio.wait_for(lane.test_task,5)
        later_ack=await later
        self.assertEqual(later_ack.ingress_ordinal,2)
        records=self.records(lane)
        closed=next(r for r in records if r.channel=='cutoffs:capture' and strict_json(r.body)['kind']=='closed')
        self.assertEqual(strict_json(closed.body)['payload']['streams'][0]['admitted_seq'],0)
        verify_clock_binding(lambda:iter(records),closed)
        self.assertEqual(lane.queue.budget.used,0)

    async def test_failed_async_close_is_unconfirmed_with_pinned_emergency_state(self):
        lane,emergency=await self.ready_cutoffs()
        await asyncio.wait_for(self.cutoff_request(lane),5)
        await asyncio.wait_for(lane.submit_received(self.received(lane)),5)
        self.mono[0]=1100
        original=lane.writer.append
        def fail(record):
            if record.channel=='cutoffs:capture' and strict_json(record.body)['kind']=='closed':
                raise OSError('injected close fsync failure')
            return original(record)
        with patch.object(lane.writer,'append',side_effect=fail),redirect_stderr(io.StringIO()):
            future=self.cutoff_close(lane)
            later=lane.submit_received(self.received(lane,1))
            await asyncio.wait_for(lane.test_task,5)
        for pending in (future,later):
            with self.assertRaises(DurabilityUnknown):await pending
        with Journal(emergency,venue='deribit',environment='production') as journal:
            event=strict_json(RawRecord.from_bytes(next(journal.encoded_records(allow_unsealed=True))).body)
        self.assertEqual(event['payload']['clock']['pin']['count'],4)
        self.assertEqual(event['payload']['clock']['active_cutoff'],dict(cutoff_id=0,state='closing'))
        self.assertEqual(event['payload']['unconfirmed_records'],2)
        recovered=recover_session(lane.writer.directory,lane.writer.directory.parent.parent/'archive')
        records=[r for path in recovered for r in iter_segment_records(path)]
        self.assertFalse(any(strict_json(r.body)['kind']=='closed' for r in records if r.channel=='cutoffs:capture'))

    async def test_cutoff_queue_overflow_stops_ingress_without_accepting_allocation(self):
        self.utc=MINUTE_NS-100
        q=ByteQueue(QueueLimits(max_bytes=32768,max_items=1,max_item_bytes=8192),now_ns=lambda:self.mono[0])
        lane,_=self.lane(queue=q,clock_policy=self.clock_policy(),cutoffs=True)
        sample=lane.submit_clock(*self.clock_request(lane))
        with self.assertRaises(QueueCapacity):self.cutoff_request(lane)
        self.assertEqual(lane.health()['clock']['cutoff_allocations_submitted'],0)
        self.start(lane);await asyncio.wait_for(sample,5);await asyncio.wait_for(lane.test_task,5)
        self.assertEqual(lane.health()['venue_failure'],'cutoff_queue_admission_failed')
        self.assertEqual(lane.queue.budget.used,0)

    async def test_measured_raw_capture_survives_forward_and_backward_utc_steps(self):
        for step in (-500_000_000,500_000_000):
            with self.subTest(step=step):
                lane,_=self.lane(clock_policy=self.clock_policy());self.start(lane)
                await asyncio.wait_for(lane.submit_clock(*self.clock_request(lane)),5)
                self.mono[0]+=60
                healthy=await asyncio.wait_for(lane.submit_clock(*self.clock_request(lane,1)),5)
                self.assertTrue(healthy.assessment.aligned)
                entered,release=threading.Event(),threading.Event();self.addCleanup(release.set)
                original_append=lane.writer.append
                def gated(value):
                    if value.kind=='ws_message':
                        entered.set()
                        if not release.wait(3):raise OSError('test gate timeout')
                    return original_append(value)
                original=self.received(lane)
                with patch.object(lane.writer,'append',side_effect=gated):
                    future=lane.submit_received(original)
                    self.assertTrue(await asyncio.to_thread(entered.wait,2))
                    self.utc+=step;self.mono[0]+=1
                    release.set();before=await asyncio.wait_for(future,5)
                self.assertTrue(before.timing_valid)
                self.assertEqual(before.receive_to_durable_ns,1)
                changed=await asyncio.wait_for(self.clock_check(lane),5)
                self.assertTrue(changed.timing_valid)
                self.assertFalse(changed.assessment.aligned)
                self.assertEqual(changed.assessment.reason,'wall_step')
                self.assertNotEqual(changed.assessment.anchor.clock_epoch,before.clock_epoch)
                after_record=self.received(lane,1)
                after=await asyncio.wait_for(lane.submit_received(after_record),5)
                self.assertTrue(after.timing_valid)
                self.assertEqual(after.clock_epoch,changed.assessment.anchor.clock_epoch)
                self.assertTrue(lane.health()['accepting'])
                self.mono[0]+=1
                warming=await asyncio.wait_for(lane.submit_clock(*self.clock_request(lane,2)),5)
                self.assertFalse(warming.assessment.aligned)
                self.mono[0]+=60
                recovered=await asyncio.wait_for(lane.submit_clock(*self.clock_request(lane,3)),5)
                self.assertTrue(recovered.assessment.aligned)
                self.assertEqual(recovered.assessment.anchor.clock_epoch,after.clock_epoch)
                lane.stop();await asyncio.wait_for(lane.test_task,5)
                native=[r for r in self.records(lane) if r.kind=='ws_message']
                self.assertEqual([r.receipt.receive_seq for r in native],[0,1])
                self.assertEqual(native[0].receipt,replace(original.receipt,clock_epoch=before.clock_epoch))
                self.assertEqual(native[1].receipt,replace(after_record.receipt,clock_epoch=after.clock_epoch))
                self.assertEqual([r.body for r in native],[original.body,after_record.body])
                self.assertEqual(self.clock_replay(lane).assessment,recovered.assessment)
                self.assertEqual(lane.health()['failure_controls_durable'],0)
                self.assertEqual(lane.health()['timing_basis'],'process_monotonic')
                self.assertTrue(all(event['timing_basis']=='process_monotonic' for event in self.controls(lane)))

    async def test_measured_observer_monotonic_regression_still_halts_raw_capture(self):
        stamp=[self.observe()]
        lane,_=self.lane(clock_policy=self.clock_policy(),observe=lambda:stamp[0]);self.start(lane)
        await asyncio.wait_for(lane.submit_clock(*self.clock_request(lane)),5)
        entered,release=threading.Event(),threading.Event();self.addCleanup(release.set)
        original=lane.writer.append
        def gated(record):
            if record.kind=='ws_message':
                entered.set()
                if not release.wait(3):raise OSError('test gate timeout')
            return original(record)
        with patch.object(lane.writer,'append',side_effect=gated):
            future=lane.submit_received(self.received(lane))
            self.assertTrue(await asyncio.to_thread(entered.wait,2))
            stamp[0]=(stamp[0][0],stamp[0][1]-1,stamp[0][2])
            release.set();ack=await asyncio.wait_for(future,5)
            await asyncio.wait_for(lane.test_task,5)
        self.assertFalse(ack.timing_valid)
        self.assertIsNone(ack.receive_to_durable_ns)
        self.assertFalse(lane.health()['accepting'])
        self.assertEqual(len([r for r in self.records(lane) if r.kind=='ws_message']),1)

    async def test_measured_processing_deadline_still_halts_raw_capture(self):
        lane,_=self.lane(clock_policy=self.clock_policy());self.start(lane)
        await asyncio.wait_for(lane.submit_clock(*self.clock_request(lane)),5)
        entered,release=threading.Event(),threading.Event();self.addCleanup(release.set)
        original=lane.writer.append
        def gated(record):
            if record.kind=='ws_message':
                entered.set()
                if not release.wait(3):raise OSError('test gate timeout')
            return original(record)
        with patch.object(lane.writer,'append',side_effect=gated):
            future=lane.submit_received(self.received(lane))
            self.assertTrue(await asyncio.to_thread(entered.wait,2))
            self.mono[0]+=5_000_000_000;release.set()
            ack=await asyncio.wait_for(future,5);await asyncio.wait_for(lane.test_task,5)
        self.assertFalse(ack.timing_valid)
        self.assertEqual(ack.receive_to_durable_ns,5_000_000_000)
        self.assertFalse(lane.health()['accepting'])
        self.assertEqual(len([r for r in self.records(lane) if r.kind=='ws_message']),1)

    async def test_received_epochs_follow_queued_clock_boundaries_without_waiting_for_disk(self):
        lane,_=self.lane(clock_policy=self.clock_policy())
        first_sample=lane.submit_clock(*self.clock_request(lane))
        originals=[self.received(lane)]
        first=lane.submit_received(originals[-1])
        self.mono[0]+=60
        second_sample=lane.submit_clock(*self.clock_request(lane,1))
        originals.append(self.received(lane,1))
        second=lane.submit_received(originals[-1])
        self.mono[0]+=61
        check=self.clock_check(lane)
        originals.append(self.received(lane,2))
        third=lane.submit_received(originals[-1])
        self.assertIsNone(lane._clock.assessment)
        self.assertEqual(lane._clock.pin.count,0)
        self.start(lane)
        a,raw_a,b,raw_b,c,raw_c=await asyncio.wait_for(
            asyncio.gather(first_sample,first,second_sample,second,check,third),5)
        self.assertTrue(b.assessment.aligned)
        self.assertFalse(c.assessment.aligned)
        self.assertEqual([ack.clock_epoch for ack in (raw_a,raw_b,raw_c)],
            [a.assessment.anchor.clock_epoch,b.assessment.anchor.clock_epoch,c.assessment.anchor.clock_epoch])
        self.assertNotEqual(raw_a.clock_epoch,raw_c.clock_epoch)
        self.assertEqual(c.assessment.anchor.epoch_started_ordinal,2)
        lane.stop();await asyncio.wait_for(lane.test_task,5)
        native=[r for r in self.records(lane) if r.kind=='ws_message']
        for original,stored,ack in zip(originals,native,(raw_a,raw_b,raw_c)):
            self.assertEqual(stored.receipt,replace(original.receipt,clock_epoch=ack.clock_epoch))
            self.assertEqual(stored.body,original.body)
            self.assertEqual(stored.record_id,original.record_id)
            self.assertEqual(digest(stored.to_bytes()),ack.raw_sha256)
            self.assertTrue(ack.timing_valid)
        self.assertEqual([ack.ingress_ordinal for ack in (raw_a,raw_b,raw_c)],[0,1,2])
        self.assertEqual(self.clock_replay(lane).assessment,c.assessment)
        self.assertTrue(all(r.receipt.clock_epoch!=PENDING_CLOCK_EPOCH for r in self.records(lane)))

    async def test_received_stamping_rejects_disabled_unmeasured_or_prelabelled_input(self):
        disabled,_=self.lane()
        with self.assertRaises(ValueError):disabled.submit_received(self.received(disabled))
        with self.assertRaises(ValueError):disabled.submit(self.received(disabled))
        lane,_=self.lane(clock_policy=self.clock_policy())
        with self.assertRaises(ValueError):lane.submit_received(self.received(lane))
        sample=lane.submit_clock(*self.clock_request(lane))
        before=lane.queue.health()['bytes']
        with self.assertRaises(ValueError):lane.submit(self.received(lane))
        with self.assertRaises(ValueError):lane.submit_received(self.record(lane))
        with self.assertRaises(ValueError):lane.submit_received(None)
        self.assertEqual(lane.queue.health()['bytes'],before)
        self.start(lane);await asyncio.wait_for(sample,5)

    async def test_failed_preceding_clock_write_never_acknowledges_pending_epoch(self):
        lane,emergency=self.lane(clock_policy=self.clock_policy())
        sample=lane.submit_clock(*self.clock_request(lane))
        future=lane.submit_received(self.received(lane))
        original=lane.writer.append
        def fail(record):
            if record.channel=='clock_assessments':raise OSError('injected clock assessment write failure')
            return original(record)
        with patch.object(lane.writer,'append',side_effect=fail),redirect_stderr(io.StringIO()):
            self.start(lane);await asyncio.wait_for(lane.test_task,5)
        for pending in (sample,future):
            with self.assertRaises(DurabilityUnknown):await pending
        self.assertIsNone(lane._clock.assessment)
        self.assertEqual(lane.health()['durable_records'],0)
        self.assertEqual(lane.health()['unconfirmed_records'],2)
        with Journal(emergency,venue='deribit',environment='production') as journal:
            event=strict_json(RawRecord.from_bytes(next(journal.encoded_records(allow_unsealed=True))).body)
        self.assertEqual(event['payload']['clock']['pin']['count'],0)
        self.assertEqual(event['payload']['unconfirmed_records'],2)

    async def test_cancelled_received_future_still_archives_committed_epoch(self):
        lane,_=self.lane(clock_policy=self.clock_policy())
        sample=lane.submit_clock(*self.clock_request(lane))
        original=self.received(lane)
        future=lane.submit_received(original);future.cancel()
        lane.stop();self.start(lane);await asyncio.wait_for(lane.test_task,5)
        assessment=(await sample).assessment
        native=[r for r in self.records(lane) if r.kind=='ws_message']
        self.assertEqual(len(native),1)
        self.assertEqual(native[0].receipt,replace(original.receipt,clock_epoch=assessment.anchor.clock_epoch))
        self.assertEqual(native[0].body,original.body)
        self.assertEqual(lane.queue.budget.used,0)

    async def test_received_admission_preflights_larger_final_clock_label(self):
        lane,_=self.lane(clock_policy=self.clock_policy())
        sample=lane.submit_clock(*self.clock_request(lane))
        record=self.received(lane)
        maximum_pending=replace(record,native_fields_json=canonical(dict(collector_ingress_ordinal=2**63-1)))
        original_limit=lane.writer.limits
        with patch.object(lane.writer,'limits',replace(original_limit,max_record_bytes=len(maximum_pending.to_bytes()))):
            with self.assertRaises(QueueCapacity):lane.submit_received(record)
        self.assertEqual(lane.health()['unconfirmed_records'],1)
        self.start(lane);await asyncio.wait_for(sample,5)

    async def test_received_stamping_does_not_claim_freshness_without_a_check(self):
        lane,_=self.lane(clock_policy=self.clock_policy());self.start(lane)
        await asyncio.wait_for(lane.submit_clock(*self.clock_request(lane)),5)
        self.mono[0]+=60
        aligned=await asyncio.wait_for(lane.submit_clock(*self.clock_request(lane,1)),5)
        self.mono[0]+=61
        # FIFO labelling alone uses the last committed assessment. A freshness
        # check at a cutoff must still downgrade this expired reference.
        raw=await asyncio.wait_for(lane.submit_received(self.received(lane)),5)
        self.assertEqual(raw.clock_epoch,aligned.assessment.anchor.clock_epoch)
        checked=await asyncio.wait_for(self.clock_check(lane),5)
        self.assertFalse(checked.assessment.aligned)
        self.assertNotEqual(raw.clock_epoch,checked.assessment.anchor.clock_epoch)
        self.assertTrue(raw.timing_valid)

    async def test_measured_epoch_recovery_does_not_change_process_timing_epoch(self):
        lane,_=self.lane(clock_policy=self.clock_policy());self.start(lane)
        first=await asyncio.wait_for(lane.submit_clock(*self.clock_request(lane)),5)
        self.mono[0]+=60
        healthy=await asyncio.wait_for(lane.submit_clock(*self.clock_request(lane,1)),5)
        self.assertTrue(healthy.assessment.aligned)
        epoch=healthy.assessment.anchor.clock_epoch
        self.assertNotEqual(epoch,self.observe()[2])
        record=self.record(lane)
        record=replace(record,receipt=replace(record.receipt,clock_epoch=epoch))
        ack=await asyncio.wait_for(lane.submit(record),5)
        self.assertTrue(ack.timing_valid)
        self.assertEqual(ack.receive_to_durable_ns,0)
        self.mono[0]+=61
        stale=await asyncio.wait_for(self.clock_check(lane),5)
        self.assertFalse(stale.assessment.aligned)
        self.assertNotEqual(stale.assessment.anchor.clock_epoch,epoch)
        record2=self.record(lane,1)
        record2=replace(record2,receipt=replace(record2.receipt,clock_epoch=stale.assessment.anchor.clock_epoch))
        self.assertTrue((await asyncio.wait_for(lane.submit(record2),5)).timing_valid)
        # Two later provider observations recover reference alignment. Raw
        # capture stays live throughout; this does not make stale data valid.
        self.mono[0]+=1
        recovering=await asyncio.wait_for(lane.submit_clock(*self.clock_request(lane,2)),5)
        self.assertFalse(recovering.assessment.aligned)
        self.mono[0]+=60
        recovered=await asyncio.wait_for(lane.submit_clock(*self.clock_request(lane,3)),5)
        self.assertTrue(recovered.assessment.aligned)
        self.assertTrue(lane.health()['accepting'])
        lane.stop();await asyncio.wait_for(lane.test_task,5)
        native=[r for r in self.records(lane) if r.kind=='ws_message']
        self.assertEqual([r.receipt.clock_epoch for r in native],
                         [epoch,stale.assessment.anchor.clock_epoch])
        self.assertEqual([r.body for r in native],[record.body,record2.body])
        self.assertEqual(self.clock_replay(lane).assessment,recovered.assessment)
        self.assertEqual(lane.health()['failure_controls_durable'],0)

    async def test_process_epoch_fault_still_invalidates_measured_raw_inflight(self):
        stamp=[self.observe()]
        lane,_=self.lane(clock_policy=self.clock_policy(),observe=lambda:stamp[0]);self.start(lane)
        measured=await asyncio.wait_for(lane.submit_clock(*self.clock_request(lane)),5)
        record=self.record(lane)
        record=replace(record,receipt=replace(record.receipt,clock_epoch=measured.assessment.anchor.clock_epoch))
        entered,release=threading.Event(),threading.Event();self.addCleanup(release.set)
        original=lane.writer.append
        def gated(value):
            if value.kind=='ws_message':
                entered.set()
                if not release.wait(3):raise OSError('test gate timeout')
            return original(value)
        with patch.object(lane.writer,'append',side_effect=gated):
            future=lane.submit(record)
            self.assertTrue(await asyncio.to_thread(entered.wait,2))
            stamp[0]=(stamp[0][0],stamp[0][1],'different-process-epoch')
            release.set()
            ack=await asyncio.wait_for(future,5);await asyncio.wait_for(lane.test_task,5)
        self.assertFalse(ack.timing_valid)
        self.assertIsNone(ack.receive_to_durable_ns)
        self.assertFalse(lane.health()['accepting'])
        native=[r for r in self.records(lane) if r.kind=='ws_message']
        self.assertEqual(len(native),1)
        self.assertEqual(native[0].receipt,record.receipt)

    async def test_clock_checks_share_fifo_and_preserve_independent_source_sequence(self):
        lane,_=self.lane(clock_policy=self.clock_policy())
        first=lane.submit(self.record(lane))
        sample=lane.submit_clock(*self.clock_request(lane))
        second=lane.submit(self.record(lane,1))
        check=self.clock_check(lane)
        third=lane.submit(self.record(lane,2))
        self.start(lane)
        results=await asyncio.wait_for(asyncio.gather(first,sample,second,check,third),5)
        self.assertEqual([results[i].ingress_ordinal for i in (0,2,4)],[0,1,2])
        self.assertTrue(results[3].timing_valid)
        self.assertFalse(results[3].assessment.aligned)
        self.mono[0]+=60
        later=await asyncio.wait_for(lane.submit_clock(*self.clock_request(lane,1)),5)
        self.assertTrue(later.assessment.aligned)
        lane.stop();await asyncio.wait_for(lane.test_task,5)
        records=self.records(lane)
        controls=[r for r in records if r.channel=='clock_assessments']
        self.assertEqual([r.receipt.receive_seq for r in controls],[0,1,2])
        query=next(r for r in controls if strict_json(r.body)['schema']=='dc.clock-check/2')
        self.assertEqual(strict_json(query.body)['observation']['ingress_ordinal'],2)
        self.assertEqual([r.receipt.receive_seq for r in records if r.channel=='clock_measurement'],[0,1])
        self.assertFalse(any(r.channel=='clock_check_request' for r in records))
        self.assertEqual(self.clock_replay(lane).assessment,later.assessment)
        self.assertEqual(lane.health()['clock']['checks_submitted'],1)

    async def test_stale_reference_check_remains_durable_while_raw_capture_continues(self):
        lane,_=self.lane(clock_policy=self.clock_policy());self.start(lane)
        await asyncio.wait_for(lane.submit_clock(*self.clock_request(lane)),5)
        self.mono[0]+=60
        await asyncio.wait_for(lane.submit_clock(*self.clock_request(lane,1)),5)
        self.mono[0]+=61
        check=await asyncio.wait_for(self.clock_check(lane),5)
        self.assertTrue(check.timing_valid)
        self.assertFalse(check.assessment.aligned)
        self.assertEqual(check.assessment.reason,'sample_stale')
        self.assertTrue(lane.health()['accepting'])
        await asyncio.wait_for(lane.submit(self.record(lane)),5)
        lane.stop();await asyncio.wait_for(lane.test_task,5)
        self.assertEqual(self.clock_replay(lane).assessment,check.assessment)
        self.assertEqual(lane.health()['durable_records'],1)

    async def test_async_clock_check_write_failure_preserves_prior_pin_in_emergency(self):
        lane,emergency=self.lane(clock_policy=self.clock_policy());self.start(lane)
        prior=await asyncio.wait_for(lane.submit_clock(*self.clock_request(lane)),5)
        original=lane.writer.append
        def fail(record):
            if strict_json(record.body).get('schema')=='dc.clock-check/2':raise OSError('check fsync failure')
            return original(record)
        with patch.object(lane.writer,'append',side_effect=fail),redirect_stderr(io.StringIO()):
            future=self.clock_check(lane)
            await asyncio.wait_for(lane.test_task,5)
        with self.assertRaises(DurabilityUnknown):await future
        self.assertEqual(lane.health()['clock']['pin']['count'],prior.pin.count)
        self.assertEqual(lane.health()['clock']['checks_submitted'],1)
        self.assertEqual(lane.health()['unconfirmed_records'],1)
        self.assertGreater(lane.queue.budget.used,0)
        with Journal(emergency,venue='deribit',environment='production') as journal:
            event=strict_json(RawRecord.from_bytes(next(journal.encoded_records(allow_unsealed=True))).body)
        self.assertEqual(event['payload']['clock']['pin']['count'],1)
        self.assertEqual(event['payload']['clock']['checks_submitted'],1)

    async def test_cancelled_clock_check_waits_for_owned_write_and_replays(self):
        lane,_=self.lane(clock_policy=self.clock_policy());self.start(lane)
        await asyncio.wait_for(lane.submit_clock(*self.clock_request(lane)),5)
        entered,release=threading.Event(),threading.Event();self.addCleanup(release.set)
        original=lane.writer.append
        def gated(record):
            if strict_json(record.body).get('schema')=='dc.clock-check/2':
                entered.set()
                if not release.wait(3):raise OSError('test gate timeout')
            return original(record)
        with patch.object(lane.writer,'append',side_effect=gated):
            future=self.clock_check(lane)
            self.assertTrue(await asyncio.to_thread(entered.wait,2))
            future.cancel();lane.test_task.cancel();await asyncio.sleep(0)
            self.assertFalse(lane.test_task.done())
            self.assertGreater(lane.queue.budget.used,0)
            release.set()
            with self.assertRaises(asyncio.CancelledError):await asyncio.wait_for(lane.test_task,5)
        self.assertEqual(self.clock_replay(lane).pin.count,2)
        self.assertEqual(lane.queue.budget.used,0)

    async def test_queue_fault_during_check_retains_historical_alignment_with_invalid_completion(self):
        lane,_=self.lane(clock_policy=self.clock_policy());self.start(lane)
        await asyncio.wait_for(lane.submit_clock(*self.clock_request(lane)),5)
        self.mono[0]+=60
        await asyncio.wait_for(lane.submit_clock(*self.clock_request(lane,1)),5)
        entered,release=threading.Event(),threading.Event();self.addCleanup(release.set)
        original=lane.writer.append
        def gated(record):
            if strict_json(record.body).get('schema')=='dc.clock-check/2':
                entered.set()
                if not release.wait(3):raise OSError('test gate timeout')
            return original(record)
        with patch.object(lane.writer,'append',side_effect=gated):
            future=self.clock_check(lane)
            self.assertTrue(await asyncio.to_thread(entered.wait,2))
            with patch.object(lane.queue,'now_ns',side_effect=ValueError('queue clock error')):
                with self.assertRaises(ValueError):lane.submit(self.record(lane))
            release.set()
            ack=await asyncio.wait_for(future,5);await asyncio.wait_for(lane.test_task,5)
        self.assertTrue(ack.assessment.aligned)
        self.assertFalse(ack.timing_valid)
        self.assertIsNone(ack.receive_to_durable_ns)
        self.assertFalse(lane.health()['accepting'])
        self.assertEqual(self.clock_replay(lane).pin.count,3)

    async def test_samples_and_checks_share_one_admission_cap_and_reject_invalid_requests(self):
        disabled,_=self.lane()
        with self.assertRaises(ValueError):self.clock_check(disabled)
        lane,_=self.lane(clock_policy=self.clock_policy())
        with self.assertRaises(ValueError):self.clock_check(lane)
        lane._clock.max_assessments=2
        sample=lane.submit_clock(*self.clock_request(lane))
        with self.assertRaises(ValueError):self.clock_check(lane,boot_id='b'*129)
        self.assertEqual(lane.health()['clock']['checks_submitted'],0)
        check=self.clock_check(lane)
        with self.assertRaises(QueueCapacity):lane.submit_clock(*self.clock_request(lane,1))
        self.start(lane)
        await asyncio.wait_for(asyncio.gather(sample,check),5)
        await asyncio.wait_for(lane.test_task,5)
        self.assertEqual(self.clock_replay(lane).pin.count,2)
        self.assertEqual(lane.health()['venue_failure'],'clock_log_bound')

    async def test_clock_writes_share_fifo_with_raw_and_replay_after_finalization(self):
        lane,_=self.lane(clock_policy=self.clock_policy())
        first=lane.submit(self.record(lane))
        clock=lane.submit_clock(*self.clock_request(lane))
        second=lane.submit(self.record(lane,1))
        self.start(lane)
        a,measurement,b=await asyncio.wait_for(asyncio.gather(first,clock,second),5)
        self.assertEqual([a.ingress_ordinal,b.ingress_ordinal],[0,1])
        self.assertEqual(measurement.assessment.anchor.epoch_started_ordinal,1)
        self.assertTrue(measurement.timing_valid)
        self.mono[0]+=60
        later=await asyncio.wait_for(lane.submit_clock(*self.clock_request(lane,1)),5)
        self.assertTrue(later.assessment.aligned)
        lane.stop();await asyncio.wait_for(lane.test_task,5)
        records=self.records(lane)
        self.assertEqual([r.receipt.connection_id for r in records[:4]],
                         ['connection',SOURCE,'control:clock-assessment','connection'])
        self.assertEqual(self.clock_replay(lane).assessment,later.assessment)
        self.assertEqual(lane.health()['clock']['pin']['count'],2)
        self.assertEqual(lane.health()['durable_records'],2)
        self.assertEqual(lane.queue.budget.used,0)

    async def test_clock_requests_require_enabled_policy_and_exact_evidence(self):
        disabled,_=self.lane()
        with self.assertRaises(ValueError):disabled.submit_clock(*self.clock_request(disabled))
        lane,_=self.lane(clock_policy=self.clock_policy())
        sample,source=self.clock_request(lane)
        with self.assertRaises(ValueError):lane.submit_clock(sample,replace(source,body=b'changed'))
        self.assertEqual(lane.queue.budget.used,0)
        self.assertEqual(lane.health()['clock']['submitted'],0)

    async def test_clock_control_write_failure_is_unconfirmed_with_emergency_clock_pin(self):
        lane,emergency=self.lane(clock_policy=self.clock_policy())
        future=lane.submit_clock(*self.clock_request(lane))
        original=lane.writer.append
        def fail(record):
            if record.channel=='clock_assessments':raise OSError('injected clock control failure')
            return original(record)
        with patch.object(lane.writer,'append',side_effect=fail),redirect_stderr(io.StringIO()):
            self.start(lane);await asyncio.wait_for(lane.test_task,5)
        with self.assertRaises(DurabilityUnknown):await future
        self.assertEqual(lane.health()['clock']['pin']['count'],0)
        self.assertEqual(lane.health()['unconfirmed_records'],1)
        self.assertGreater(lane.queue.budget.used,0)
        with Journal(emergency,venue='deribit',environment='production') as journal:
            event=strict_json(RawRecord.from_bytes(next(journal.encoded_records(allow_unsealed=True))).body)
        self.assertEqual(event['payload']['clock']['submitted'],1)
        self.assertEqual(event['payload']['clock']['pin']['count'],0)

    async def test_cancellation_during_clock_pair_never_interleaves_failure_controls(self):
        lane,_=self.lane(clock_policy=self.clock_policy())
        entered,release=threading.Event(),threading.Event();self.addCleanup(release.set)
        original=lane.writer.append
        def gated(record):
            if record.channel=='clock_assessments':
                entered.set()
                if not release.wait(3):raise OSError('test gate timeout')
            return original(record)
        future=lane.submit_clock(*self.clock_request(lane))
        with patch.object(lane.writer,'append',side_effect=gated):
            task=self.start(lane);self.assertTrue(await asyncio.to_thread(entered.wait,2))
            future.cancel();task.cancel();await asyncio.sleep(0)
            self.assertFalse(task.done());self.assertGreater(lane.queue.budget.used,0)
            release.set()
            with self.assertRaises(asyncio.CancelledError):await asyncio.wait_for(task,5)
        self.assertEqual([r.receipt.connection_id for r in self.records(lane)[:2]],
                         [SOURCE,'control:clock-assessment'])
        self.assertEqual(self.clock_replay(lane).pin.count,1)
        self.assertEqual(lane.queue.budget.used,0)

    async def test_queue_clock_fault_during_clock_write_invalidates_completion_timing(self):
        lane,_=self.lane(clock_policy=self.clock_policy())
        entered,release=threading.Event(),threading.Event();self.addCleanup(release.set)
        original=lane.writer.append
        def gated(record):
            if record.channel=='clock_assessments':
                entered.set()
                if not release.wait(3):raise OSError('test gate timeout')
            return original(record)
        future=lane.submit_clock(*self.clock_request(lane))
        with patch.object(lane.writer,'append',side_effect=gated):
            self.start(lane);self.assertTrue(await asyncio.to_thread(entered.wait,2))
            with patch.object(lane.queue,'now_ns',side_effect=ValueError('clock error')):
                with self.assertRaises(ValueError):lane.submit(self.record(lane))
            release.set()
            ack=await asyncio.wait_for(future,5);await asyncio.wait_for(lane.test_task,5)
        self.assertFalse(ack.timing_valid)
        self.assertIsNone(ack.receive_to_durable_ns)
        self.assertEqual(self.clock_replay(lane).pin.count,1)

    async def test_clock_overflow_halts_lane_and_drains_previously_accepted_raw(self):
        q=ByteQueue(QueueLimits(max_bytes=32768,max_items=1,max_item_bytes=8192),now_ns=lambda:self.mono[0])
        lane,_=self.lane(queue=q,clock_policy=self.clock_policy())
        raw_future=lane.submit(self.record(lane))
        with self.assertRaises(QueueCapacity):lane.submit_clock(*self.clock_request(lane))
        self.start(lane);await asyncio.wait_for(raw_future,5);await asyncio.wait_for(lane.test_task,5)
        self.assertEqual(lane.health()['venue_failure'],'clock_queue_admission_failed')
        self.assertEqual(lane.health()['clock']['submitted'],0)
        self.assertEqual(lane.health()['durable_records'],1)
        self.assertEqual(lane.queue.budget.used,0)

    async def test_raw_acknowledgement_matches_archived_bytes_and_ingress_order(self):
        lane,_ = self.lane()
        originals = [self.record(lane,n) for n in range(3)]
        futures = [lane.submit(record) for record in originals]
        self.start(lane)
        acks = await asyncio.wait_for(asyncio.gather(*futures),5)
        lane.stop('budget_complete'); report = await asyncio.wait_for(lane.test_task,5)
        native = [r for r in self.records(lane) if r.receipt.connection_id=='connection']
        self.assertEqual([r.body for r in native],[r.body for r in originals])
        self.assertEqual([a.ingress_ordinal for a in acks],[0,1,2])
        self.assertEqual([a.raw_sha256 for a in acks],[digest(r.to_bytes()) for r in native])
        self.assertTrue(all(a.timing_valid for a in acks))
        self.assertEqual(report['durable_records'],3)
        self.assertIsNone(report['writer_error'])
        self.assertEqual(report['failure_controls_durable'],0)
        self.assertEqual(self.controls(lane)[-1]['payload']['reason'],'budget_complete')
        self.assertEqual(lane.queue.budget.used,0)

    async def test_capacity_halts_only_rejected_connection_and_persists_open_loss_control(self):
        q = ByteQueue(QueueLimits(max_bytes=32768,max_items=1,max_item_bytes=8192),now_ns=lambda:self.mono[0])
        lane,_ = self.lane(queue=q)
        first = lane.submit(self.record(lane))
        with self.assertRaises(QueueCapacity): lane.submit(self.record(lane,1))
        with self.assertRaises(LaneStopped): lane.submit(self.record(lane,1))
        self.start(lane); await asyncio.wait_for(first,5)
        other = lane.submit(self.record(lane,connection='fresh-connection'))
        await asyncio.wait_for(other,5)
        lane.stop(); report = await asyncio.wait_for(lane.test_task,5)
        self.assertEqual(report['durable_records'],2)
        self.assertEqual(report['failed_connections'],1)
        self.assertIsNone(report['venue_failure'])
        failure = next(v for v in self.controls(lane) if v['kind']=='ingress_halted')['payload']
        self.assertEqual(failure['scope'],'connection')
        self.assertTrue(failure['interval_open'])
        self.assertEqual(failure['rejected']['receipt']['receive_seq'],1)
        self.assertEqual(failure['reason'],'queue_capacity')

    async def test_full_connection_failure_map_is_bounded_even_without_accepted_messages(self):
        q = ByteQueue(QueueLimits(max_bytes=32768,max_items=4,max_item_bytes=4096),now_ns=lambda:self.mono[0])
        lane,_ = self.lane(queue=q)
        for index in range(4):
            with self.assertRaises(QueueCapacity):lane.submit(self.record(lane,connection=str(index),body=b'x'*5000))
        with self.assertRaises(QueueCapacity):lane.submit(self.record(lane,connection='beyond',body=b'x'*5000))
        self.start(lane); report = await asyncio.wait_for(lane.test_task,5)
        self.assertEqual(report['failed_connections'],4)
        self.assertEqual(report['durable_records'],0)
        self.assertEqual(report['venue_failure'],'session_connection_bound')
        self.assertEqual(report['failure_controls_durable'],5)

    async def test_cancelled_client_future_does_not_drop_accepted_raw_message(self):
        lane,_ = self.lane()
        future = lane.submit(self.record(lane)); future.cancel()
        lane.stop(); self.start(lane)
        await asyncio.wait_for(lane.test_task,5)
        self.assertEqual([r.receipt.receive_seq for r in self.records(lane) if r.kind=='ws_message'],[0])
        self.assertEqual(lane.health()['unconfirmed_records'],0)
        self.assertEqual(lane.queue.budget.used,0)

    async def test_writer_failure_reports_uncertainty_and_uses_independent_control_wal(self):
        lane,emergency = self.lane()
        future = lane.submit(self.record(lane))
        stderr = io.StringIO()
        with patch.object(lane.writer,'append',side_effect=OSError('injected disk failure')),redirect_stderr(stderr):
            self.start(lane); report = await asyncio.wait_for(lane.test_task,5)
        with self.assertRaises(DurabilityUnknown):await future
        self.assertEqual(report['unconfirmed_records'],1)
        self.assertGreater(lane.queue.budget.used,0)
        self.assertIsNotNone(report['emergency_control_id'])
        with Journal(emergency,venue='deribit',environment='production') as journal:
            events=[strict_json(RawRecord.from_bytes(v).body)
                    for v in journal.encoded_records(allow_unsealed=True)]
        self.assertEqual(events[0]['kind'],'writer_unconfirmed')
        self.assertEqual(events[0]['payload']['watermarks'][0]['last_submitted_seq'],0)
        self.assertIsNone(events[0]['payload']['watermarks'][0]['last_durable'])
        self.assertIn('"emergency_durable":true',stderr.getvalue())

    async def test_complete_but_unacknowledged_frame_can_be_recovered_without_false_success(self):
        lane,_ = self.lane()
        original=lane.writer.append
        def fail_after_fsync(record):
            original(record)
            raise OSError('failed after actual durable append')
        future=lane.submit(self.record(lane))
        with patch.object(lane.writer,'append',side_effect=fail_after_fsync),redirect_stderr(io.StringIO()):
            self.start(lane); report=await asyncio.wait_for(lane.test_task,5)
        with self.assertRaises(DurabilityUnknown):await future
        self.assertEqual(report['durable_records'],0)
        base=lane.writer.directory.parent.parent
        recovered=recover_session(lane.writer.directory,base/'archive')
        records=[r for path in recovered for r in iter_segment_records(path)]
        self.assertEqual(len(records),1)
        self.assertEqual(records[0].record_id,self.record(lane).record_id)

    async def test_both_disks_failed_emit_health_without_claiming_durable_failure_control(self):
        lane,_=self.lane(); future=lane.submit(self.record(lane)); stderr=io.StringIO()
        with patch.object(lane.writer,'append',side_effect=OSError()),patch.object(lane.emergency_writer,'append',side_effect=OSError()),redirect_stderr(stderr):
            self.start(lane); report=await asyncio.wait_for(lane.test_task,5)
        with self.assertRaises(DurabilityUnknown):await future
        self.assertIsNone(report['emergency_control_id'])
        self.assertEqual(report['failure_controls_durable'],0)
        self.assertIn('"emergency_durable":false',stderr.getvalue())

    async def test_cancellation_waits_for_inflight_disk_operation_before_closing_owner(self):
        lane,_=self.lane(); entered=threading.Event(); release=threading.Event()
        self.addCleanup(release.set)
        original=lane.writer.append
        def gated(record):
            if record.kind=='ws_message':
                entered.set()
                if not release.wait(3):raise OSError('test gate timeout')
            return original(record)
        future=lane.submit(self.record(lane))
        with patch.object(lane.writer,'append',side_effect=gated):
            task=self.start(lane)
            self.assertTrue(await asyncio.to_thread(entered.wait,2))
            task.cancel(); await asyncio.sleep(0)
            self.assertFalse(task.done()); self.assertFalse(lane.writer.closed)
            self.assertFalse(future.done()); self.assertGreater(lane.queue.budget.used,0)
            task.cancel(); await asyncio.sleep(0)
            release.set()
            with self.assertRaises(asyncio.CancelledError):await asyncio.wait_for(task,5)
        self.assertTrue((await future).timing_valid)
        self.assertTrue(lane.writer.closed)
        self.assertEqual(len([r for r in self.records(lane) if r.kind=='ws_message']),1)
        self.assertEqual(lane.health()['venue_failure'],'worker_cancelled')

    async def test_receive_to_durable_deadline_failure_keeps_raw_and_reports_measured_latency(self):
        lane,_=self.lane(); entered=threading.Event(); release=threading.Event(); self.addCleanup(release.set)
        original=lane.writer.append
        def slow(record):
            if record.kind=='ws_message':entered.set(); release.wait(3)
            return original(record)
        future=lane.submit(self.record(lane))
        with patch.object(lane.writer,'append',side_effect=slow):
            self.start(lane); self.assertTrue(await asyncio.to_thread(entered.wait,2))
            self.mono[0]+=5_000_000_000; release.set()
            ack=await asyncio.wait_for(future,5)
            await asyncio.wait_for(lane.test_task,5)
        self.assertFalse(ack.timing_valid)
        self.assertEqual(ack.receive_to_durable_ns,5_000_000_000)
        self.assertEqual(lane.health()['venue_failure'],'receive_durable_deadline_or_clock_invalid')
        self.assertEqual(len([r for r in self.records(lane) if r.kind=='ws_message']),1)

    async def test_clock_sampler_failure_preserves_raw_with_explicit_unknown_control_time(self):
        broken=[False]
        def observe():
            if broken[0]:raise ValueError('injected clock error')
            return self.observe()
        lane,_=self.lane(observe=observe); future=lane.submit(self.record(lane)); broken[0]=True
        self.start(lane); ack=await asyncio.wait_for(future,5); await asyncio.wait_for(lane.test_task,5)
        self.assertFalse(ack.timing_valid)
        self.assertIsNone(ack.receive_to_durable_ns)
        for event in self.controls(lane):
            self.assertFalse(event['control_time_valid'])
            self.assertEqual(event['control_time_source'],'last_healthy_observation')

    async def test_faulty_venue_does_not_stop_another_lanes_thread_or_queue(self):
        limits=QueueLimits(max_bytes=131072,max_items=8,max_item_bytes=32768)
        fleet=VenueQueues({'deribit':limits,'bybit':limits},aggregate_bytes=262144,now_ns=lambda:self.mono[0])
        bad,_=self.lane(queue=fleet.queues['deribit'])
        good,_=self.lane(venue='bybit',queue=fleet.queues['bybit'])
        failed=bad.submit(self.record(bad)); saved=good.submit(self.record(good))
        with patch.object(bad.writer,'append',side_effect=OSError()),redirect_stderr(io.StringIO()):
            self.start(bad); self.start(good)
            await asyncio.wait_for(saved,5)
            good.stop(); await asyncio.wait_for(good.test_task,5)
            await asyncio.wait_for(bad.test_task,5)
        with self.assertRaises(DurabilityUnknown):await failed
        self.assertIsNone(good.health()['writer_error'])
        self.assertEqual(good.health()['durable_records'],1)
        self.assertEqual(fleet.budget.used,bad.queue.health()['bytes'])

    async def test_epoch_change_or_wall_step_invalidates_timing_but_preserves_raw(self):
        for change in ('epoch', 'wall'):
            with self.subTest(change=change):
                stamp = [self.observe()]
                lane,_ = self.lane(observe=lambda:stamp[0])
                future = lane.submit(self.record(lane))
                utc, mono, epoch = stamp[0]
                stamp[0] = (utc, mono, 'changed') if change == 'epoch' else (utc+100_000_001, mono, epoch)
                self.start(lane)
                ack = await asyncio.wait_for(future,5)
                await asyncio.wait_for(lane.test_task,5)
                self.assertFalse(ack.timing_valid)
                self.assertIsNone(ack.receive_to_durable_ns)
                self.assertFalse(lane.health()['accepting'])
                self.assertEqual(lane.health()['durable_records'],1)
                self.assertTrue(any(event['kind']=='ingress_halted' for event in self.controls(lane)))
                self.assertTrue(all(not event['control_time_valid'] for event in self.controls(lane)))
                self.assertEqual(len([r for r in self.records(lane) if r.kind=='ws_message']),1)

    async def test_observation_monotonic_regression_latches_even_with_nonnegative_latency(self):
        stamp = [self.observe()]
        lane,_ = self.lane(observe=lambda:stamp[0])
        future = lane.submit(self.record(lane))
        stamp[0] = (self.utc+1000,2000,'epoch')
        self.assertTrue(lane._observation()[1])
        stamp[0] = (self.utc+500,1500,'epoch')
        self.start(lane)
        ack = await asyncio.wait_for(future,5)
        await asyncio.wait_for(lane.test_task,5)
        self.assertFalse(ack.timing_valid)
        self.assertIsNone(ack.receive_to_durable_ns)
        stamp[0] = (self.utc+2000,3000,'epoch')
        self.assertFalse(lane._observation()[1])

    async def test_queue_clock_fault_during_write_invalidates_inflight_ack(self):
        lane,_ = self.lane()
        entered, release = threading.Event(), threading.Event()
        self.addCleanup(release.set)
        original = lane.writer.append
        def gated(record):
            if record.kind=='ws_message':
                entered.set()
                if not release.wait(3):raise OSError('test gate timeout')
            return original(record)
        future = lane.submit(self.record(lane))
        def broken():raise ValueError('injected queue clock failure')
        with patch.object(lane.writer,'append',side_effect=gated):
            self.start(lane)
            self.assertTrue(await asyncio.to_thread(entered.wait,2))
            with patch.object(lane.queue,'now_ns',side_effect=broken):
                with self.assertRaises(ValueError):lane.submit(self.record(lane,1))
            release.set()
            ack = await asyncio.wait_for(future,5)
            await asyncio.wait_for(lane.test_task,5)
        self.assertFalse(ack.timing_valid)
        self.assertIsNone(ack.receive_to_durable_ns)
        self.assertEqual(lane.health()['venue_failure'],'queue_clock_or_closed')
        self.assertEqual(lane.health()['durable_records'],1)
        self.assertEqual(lane.queue.budget.used,0)

    async def test_pending_connection_loss_control_survives_primary_control_write_failure(self):
        q=ByteQueue(QueueLimits(max_bytes=32768,max_items=1,max_item_bytes=8192),now_ns=lambda:self.mono[0])
        lane,emergency=self.lane(queue=q)
        future=lane.submit(self.record(lane))
        with self.assertRaises(QueueCapacity):lane.submit(self.record(lane,1))
        with patch.object(lane.writer,'append',side_effect=OSError()),redirect_stderr(io.StringIO()):
            self.start(lane); await asyncio.wait_for(lane.test_task,5)
        with self.assertRaises(DurabilityUnknown):await future
        with Journal(emergency,venue='deribit',environment='production') as journal:
            record=RawRecord.from_bytes(next(journal.encoded_records(allow_unsealed=True)))
        events=strict_json(record.body)['payload']['unpersisted_controls']
        self.assertEqual(events[0]['reason'],'queue_capacity')
        self.assertEqual(events[0]['rejected']['receipt']['receive_seq'],1)


if __name__=='__main__':unittest.main()
