import asyncio
from dataclasses import replace
from pathlib import Path
import tempfile
import threading
import unittest
from unittest.mock import patch

from data_collector.clock import ClockSample
from data_collector.clockcutoffs import ClockBoundCutoffs
from data_collector.clocklog import DurableClock,SOURCE
from data_collector.contracts import Provenance,RawRecord,Receipt,StreamKey,canonical,digest,strict_json
from data_collector.cutoffs import CutoffCoordinator,IngressWriter,MINUTE_NS
from data_collector.journal import Journal,_publish_bytes
from data_collector.lane import DurableSnapshotInputs,SnapshotOrigin,SnapshotSegmentInput
from data_collector.ledger import DurableLedger
from data_collector.metadata import MetadataVersion
from data_collector.publisher import SnapshotPublisher,PublisherBusy,PublicationUnconfirmed,DELIVERY_NS
from data_collector.replay import BookBinding,ReplayLimits,SnapshotPlan
from data_collector.segments import SegmentContext,seal_segment,iter_segment_records
from data_collector.snapshots import publish_snapshot,read_snapshot
from data_collector.validity import FeedTarget,GapCorrection
from tests.test_dc_contracts import key

C=MINUTE_NS


class PublisherTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        self.tmp=tempfile.TemporaryDirectory();self.addCleanup(self.tmp.cleanup)
        self.root=Path(self.tmp.name);self.counter=0;self._inputs=None
        self.stamp=[C,1100,'process-clock']
        inst=key();self.ws=StreamKey('capture','process','ws');self.meta=StreamKey('capture','process','meta')
        version=MetadataVersion(inst,Receipt('capture','process','meta',0,C-20,1080,'process:clock:0'),
            C-100,canonical(dict(base_currency='BTC',amount_unit='native',quote_currency='native')))
        self.plan=SnapshotPlan((BookBinding(FeedTarget(inst,'deribit_full','owner'),self.ws,self.meta,version.version_id),),'b'*64)
        provenance=Provenance('a'*64,'b'*64,self.plan.dependencies['universe'])
        self.wal=Journal(self.root/'active.wal',venue='deribit',environment='production',create=True);self.addCleanup(self.wal.close)
        ingress=IngressWriter(self.wal,'capture','process')
        coordinator=CutoffCoordinator(self.wal,capture_scope='capture',capture_id='capture',process_session_id='process',provenance=provenance)
        ledger=DurableLedger(self.wal,ledger_id='ledger',capture_scope='capture',capture_id='capture',process_session_id='process',provenance=provenance)
        ledger.initialize(utc_ns=C-200,monotonic_ns=900,clock_epoch='process:clock:0')
        self.clock=DurableClock(self.wal,capture_id='capture',process_session_id='process',provenance=provenance,
            policy=dict(sample_interval_ns=60,max_sample_age_ns=60_000_000_000,max_offset_ns=100,step_tolerance_ns=2,max_epochs=8))
        for seq,mono in enumerate((940,1000)):
            source=RawRecord(Receipt('capture','process',SOURCE,seq,C-1100+mono,mono,'measurement'),
                'deribit','production','control','clock_measurement','control',b'measurement '+str(seq).encode(),provenance)
            self.clock.sample(ClockSample(source.receipt.receive_utc_ns,mono,0,0,'boot',digest(source.to_bytes())),source,ingress_ordinal=0)
        bound=ClockBoundCutoffs(self.clock,ingress,coordinator,ledger)
        self.bound,self.ingress,self.coordinator,self.ledger=bound,ingress,coordinator,ledger
        bound.allocate(C,[self.ws,self.meta],self.plan.dependencies,utc_ns=C-100,monotonic_ns=1000,boot_id='boot')
        ingress.append(RawRecord(version.receipt,'deribit','production','option','metadata','metadata_version',
            canonical(dict(schema='dc.metadata/2',effective_at_ns=version.effective_at_ns,attributes=version.attributes,supersedes=None)),provenance,inst))
        channel=f'book.{inst.native_symbol}.100ms'
        body=canonical(dict(jsonrpc='2.0',method='subscription',params=dict(channel=channel,data=dict(type='snapshot',
            timestamp=1,instrument_name=inst.native_symbol,change_id=10,bids=[['new',100,2]],asks=[['new',101,3]]))))
        ingress.append(RawRecord(Receipt('capture','process','ws',0,C-10,1090,'process:clock:0'),
            'deribit','production','option',channel,'ws_message',body,provenance,inst))
        bound.close(0,utc_ns=C,monotonic_ns=1100,boot_id='boot',healthy_streams=[self.ws,self.meta])
        self.close=coordinator.resolutions[0]

    @property
    def inputs(self):
        if self._inputs is None:
            path=seal_segment(self.wal,self.root/'archive',SegmentContext('deribit','production','capture','process','0','a'*64,'b'*64))
            manifest=(path/'manifest.json').read_bytes()
            self._inputs=DurableSnapshotInputs(self.close['record_id'],self.close['hash'],0,1100,
                SnapshotOrigin('deribit','production','capture','process','capture','process-clock'),
                (SnapshotSegmentInput(path,strict_json(manifest)['context_sha256'],digest(manifest)),),
                ReplayLimits(),self.clock.pin,0,True)
        return self._inputs

    def publisher(self,**kwargs):
        self.counter+=1
        publisher=SnapshotPublisher(self.root/f'publisher-{self.counter}',self.inputs.origin,observe=lambda:tuple(self.stamp),**kwargs)
        self.addAsyncCleanup(publisher.close)
        return publisher

    async def delivered(self,publisher,inputs=None):
        return await asyncio.wait_for(publisher.submit(self.inputs if inputs is None else inputs,self.plan),5)

    def advance(self,amount):
        self.stamp[0]+=amount;self.stamp[1]+=amount

    def next_inputs(self, *, recover=False):
        previous=self.inputs
        wal=Journal(self.root/f'next-{previous.cutoff_id}.wal',venue='deribit',environment='production',create=True)
        self.addCleanup(wal.close)
        for segment in previous.segments:
            for record in iter_segment_records(segment.path):wal.append(record)
        for owner in (self.clock,self.ingress,self.coordinator,self.ledger):owner.journal=wal
        cutoff=(previous.cutoff_id+2)*C;deadline=cutoff-C+1100
        if recover:
            self.clock.check(utc_ns=cutoff,monotonic_ns=deadline-1000,boot_id='boot',ingress_ordinal=self.ingress.ordinal)
            self.clock.check(utc_ns=cutoff-900,monotonic_ns=deadline-900,boot_id='boot',ingress_ordinal=self.ingress.ordinal)
        for mono in ((deadline-300,deadline-200) if recover else (deadline-200,)):
            seq=self.clock._sample_count
            source=RawRecord(Receipt('capture','process',SOURCE,seq,cutoff+mono-deadline,mono,'measurement'),
                'deribit','production','control','clock_measurement','control',b'next '+str(seq).encode(),self.clock.provenance)
            self.clock.sample(ClockSample(source.receipt.receive_utc_ns,mono,0,0,'boot',digest(source.to_bytes())),
                source,ingress_ordinal=self.ingress.ordinal)
        self.bound.allocate(cutoff,[self.ws,self.meta],self.plan.dependencies,
            utc_ns=cutoff-100,monotonic_ns=deadline-100,boot_id='boot')
        identity=previous.cutoff_id+1
        self.bound.close(identity,utc_ns=cutoff,monotonic_ns=deadline,boot_id='boot',healthy_streams=[self.ws,self.meta])
        close=self.coordinator.resolutions[identity]
        path=seal_segment(wal,self.root/'archive',SegmentContext('deribit','production','capture','process',str(identity),'a'*64,'b'*64))
        manifest=(path/'manifest.json').read_bytes()
        self._inputs=replace(previous,close_record_id=close['record_id'],close_sha256=close['hash'],cutoff_id=identity,
            deadline_ns=deadline,clock_pin=self.clock.pin,segments=(
                SnapshotSegmentInput(path,strict_json(manifest)['context_sha256'],digest(manifest)),))
        self.stamp[:]=[cutoff,deadline,'process-clock']
        return self.inputs

    async def test_detected_step_persists_across_jobs_reopen_and_new_epoch_recovers(self):
        publisher=self.publisher()
        def step(*args,**kwargs):
            result=publish_snapshot(*args,**kwargs);self.stamp[0]+=1000;return result
        with patch('data_collector.publisher.publish_snapshot',side_effect=step):
            self.assertEqual((await self.delivered(publisher)).reason,'clock_alignment_unavailable')
        self.next_inputs()
        self.assertEqual((await self.delivered(publisher)).reason,'clock_alignment_unavailable')
        await publisher.close()
        reopened=SnapshotPublisher(publisher.root,self.inputs.origin,observe=lambda:tuple(self.stamp))
        self.addAsyncCleanup(reopened.close)
        self.next_inputs()
        self.assertEqual((await self.delivered(reopened)).reason,'clock_alignment_unavailable')
        self.next_inputs(recover=True)
        recovered=await self.delivered(reopened)
        self.assertEqual(recovered.outcome,'published',recovered.receipt_path.read_text())

    async def test_step_is_retained_even_when_publication_raises(self):
        publisher=self.publisher()
        def step(*args,**kwargs):
            self.stamp[0]+=1000;raise OSError('publication failure with step')
        with patch('data_collector.publisher.publish_snapshot',side_effect=step):
            self.assertEqual((await self.delivered(publisher)).reason,'publication_failed')
        self.next_inputs()
        self.assertEqual((await self.delivered(publisher)).reason,'clock_alignment_unavailable')

    async def test_historical_success_survives_later_epoch_invalidation(self):
        publisher=self.publisher();original=self.inputs
        first=await self.delivered(publisher)
        self.next_inputs();self.stamp[0]+=1000
        self.assertEqual((await self.delivered(publisher)).reason,'clock_alignment_unavailable')
        self.assertEqual(await self.delivered(publisher,original),first)

    async def test_invalidation_write_failure_stops_owner_without_success(self):
        publisher=self.publisher();self.stamp[0]+=1000
        with patch('data_collector.publisher._publish_bytes',side_effect=OSError('invalidation storage failed')):
            with self.assertRaises(PublicationUnconfirmed):await self.delivered(publisher)
        self.assertFalse(publisher.health()['accepting'])
        self.assertFalse((publisher.root/'snapshots').exists())

    async def test_successful_delivery_is_verified_and_retry_retains_original_timing(self):
        publisher=self.publisher()
        first=await self.delivered(publisher)
        self.assertEqual((first.outcome,first.reason),('published','on_time'))
        body=first.receipt_path.read_bytes();receipt=strict_json(body)
        self.assertEqual(first.receipt_body,body)
        self.assertEqual(digest(body),first.receipt_sha256)
        self.assertEqual(receipt['completed']['monotonic_ns'],1100)
        result=read_snapshot(publisher.root/'snapshots',first.snapshot_id,[s.path for s in self.inputs.segments])
        self.assertEqual(strict_json(result['payload'])['books'][0]['validity_as_published'],'valid')
        self.advance(DELIVERY_NS+1)
        self.assertEqual(await self.delivered(publisher),first)
        self.assertEqual(first.receipt_path.read_bytes(),body)
        self.assertEqual(publisher.health()['completed'],2)

    async def test_late_start_skips_artifact_and_late_completion_is_failed(self):
        publisher=self.publisher();self.advance(DELIVERY_NS+1)
        late=await self.delivered(publisher)
        self.assertEqual(late.reason,'publication_deadline_missed')
        self.assertFalse((publisher.root/'snapshots').exists())
        self.stamp[:]=[C,1100,'process-clock']
        another=self.publisher()
        def slow(*args,**kwargs):
            result=publish_snapshot(*args,**kwargs);self.advance(DELIVERY_NS+1);return result
        with patch('data_collector.publisher.publish_snapshot',side_effect=slow):
            completed=await self.delivered(another)
        self.assertEqual((completed.outcome,completed.reason),('failed','publication_deadline_missed'))
        self.assertIsNotNone(completed.snapshot_id)

    async def test_exact_deadline_is_allowed_with_fresh_alignment(self):
        publisher=self.publisher()
        def at_limit(*args,**kwargs):
            result=publish_snapshot(*args,**kwargs);self.advance(DELIVERY_NS);return result
        with patch('data_collector.publisher.publish_snapshot',side_effect=at_limit):
            result=await self.delivered(publisher)
        self.assertEqual(result.outcome,'published')

    async def test_utc_step_or_process_regression_during_publish_cannot_report_on_time(self):
        for fault in ('utc','monotonic','epoch'):
            with self.subTest(fault=fault):
                self.stamp[:]=[C,1100,'process-clock'];publisher=self.publisher()
                def changed(*args,**kwargs):
                    result=publish_snapshot(*args,**kwargs)
                    if fault=='utc':self.stamp[0]+=1000
                    elif fault=='monotonic':self.stamp[1]-=1
                    else:self.stamp[2]='different-process'
                    return result
                with patch('data_collector.publisher.publish_snapshot',side_effect=changed):
                    result=await self.delivered(publisher)
                self.assertEqual(result.outcome,'failed')
                self.assertEqual(result.reason,'clock_alignment_unavailable' if fault=='utc' else 'process_clock_invalid')

    async def test_clock_invalidation_after_close_prevents_publication(self):
        self.clock.check(utc_ns=C+1000,monotonic_ns=1101,boot_id='boot',ingress_ordinal=3)
        self.stamp[:]=[C+1000,1101,'process-clock']
        publisher=self.publisher();result=await self.delivered(publisher)
        self.assertEqual(result.reason,'clock_alignment_unavailable')
        self.assertFalse((publisher.root/'snapshots').exists())

    async def test_pinned_manifest_or_close_mismatch_yields_durable_failure(self):
        for bad in (replace(self.inputs,close_sha256='f'*64),
                replace(self.inputs,segments=(replace(self.inputs.segments[0],manifest_sha256='f'*64),))):
            publisher=self.publisher();result=await self.delivered(publisher,bad)
            self.assertEqual((result.outcome,result.reason),('failed','publication_failed'))
            self.assertFalse((publisher.root/'snapshots').exists())
            self.assertEqual(await self.delivered(publisher,bad),result)

    async def test_delivery_uses_publication_start_ledger_and_rejects_existing_older_knowledge(self):
        first=self.inputs.segments[0]
        native=next(r for r in iter_segment_records(first.path) if r.receipt.connection_id=='ws')
        with Journal(self.root/'correction.wal',venue='deribit',environment='production',create=True) as wal:
            ledger=DurableLedger(wal,ledger_id='ledger',capture_scope='capture',capture_id='capture',
                process_session_id='process',provenance=self.clock.provenance,
                prior_records=lambda:iter_segment_records(first.path))
            ledger.append(GapCorrection('capture',self.plan.bindings[0].target,0,None,native.record_id,native.record_id,'late_gap'),
                utc_ns=C,monotonic_ns=1100,clock_epoch='process:clock:0')
            path=seal_segment(wal,self.root/'archive',SegmentContext('deribit','production','capture','process','1','a'*64,'b'*64))
        manifest=(path/'manifest.json').read_bytes()
        corrected=replace(self.inputs,segments=self.inputs.segments+(
            SnapshotSegmentInput(path,strict_json(manifest)['context_sha256'],digest(manifest)),))
        publisher=self.publisher();fresh=await self.delivered(publisher,corrected)
        snapshot=read_snapshot(publisher.root/'snapshots',fresh.snapshot_id,[s.path for s in corrected.segments])
        self.assertEqual(snapshot['descriptor']['publication_ledger_pin']['sequence'],1)
        self.assertEqual(strict_json(snapshot['payload'])['books'][0]['validity_as_published'],'invalid')
        another=self.publisher()
        original=publish_snapshot(another.root/'snapshots',[first.path],self.inputs.close_record_id,self.plan)
        failed=await self.delivered(another,corrected)
        self.assertEqual((failed.outcome,failed.reason),('failed','publication_failed'))
        self.assertEqual(read_snapshot(another.root/'snapshots',original['snapshot_id'],[first.path]),original)

    async def test_corrupt_delivery_timing_is_not_repaired_or_reported_successfully(self):
        publisher=self.publisher();result=await self.delivered(publisher)
        saved=strict_json(result.receipt_path.read_bytes());saved['completed']['valid']=False
        altered=canonical(saved);result.receipt_path.write_bytes(altered)
        with self.assertRaises(PublicationUnconfirmed):await self.delivered(publisher)
        self.assertEqual(result.receipt_path.read_bytes(),altered)

    async def test_publication_exception_never_counts_existing_files_as_success(self):
        publisher=self.publisher()
        def after_commit(*args,**kwargs):
            publish_snapshot(*args,**kwargs);raise OSError('injected post-commit error')
        with patch('data_collector.publisher.publish_snapshot',side_effect=after_commit):
            result=await self.delivered(publisher)
        self.assertEqual((result.outcome,result.reason),('failed','publication_failed'))
        self.assertTrue((publisher.root/'snapshots'/'closures'/(self.inputs.close_record_id+'.json')).exists())

    async def test_receipt_failure_is_unconfirmed_and_retry_can_recover_durable_proof(self):
        publisher=self.publisher()
        def after_receipt(path,data):
            _publish_bytes(path,data);raise OSError('injected error after receipt fsync')
        with patch('data_collector.publisher._publish_bytes',side_effect=after_receipt):
            with self.assertRaises(PublicationUnconfirmed):await self.delivered(publisher)
        path=publisher.root/'deliveries'/(self.inputs.close_record_id+'.json');saved=path.read_bytes()
        self.advance(DELIVERY_NS+1)
        recovered=await self.delivered(publisher)
        self.assertEqual(recovered.outcome,'published')
        self.assertEqual(recovered.receipt_sha256,digest(saved))
        self.assertEqual(path.read_bytes(),saved)

    async def test_receipt_never_written_cannot_be_recovered_as_old_on_time(self):
        publisher=self.publisher()
        with patch('data_collector.publisher._publish_bytes',side_effect=OSError('disk write failed')):
            with self.assertRaises(PublicationUnconfirmed):await self.delivered(publisher)
        self.advance(DELIVERY_NS+1)
        self.assertEqual((await self.delivered(publisher)).outcome,'failed')

    async def test_bounded_worker_and_cancelled_close_retain_lock_until_owned_job_finishes(self):
        publisher=self.publisher();entered,release=threading.Event(),threading.Event();self.addCleanup(release.set)
        def blocked(*args,**kwargs):
            entered.set()
            if not release.wait(3):raise OSError('test gate timeout')
            return publish_snapshot(*args,**kwargs)
        with patch('data_collector.publisher.publish_snapshot',side_effect=blocked):
            future=publisher.submit(self.inputs,self.plan)
            self.assertTrue(await asyncio.to_thread(entered.wait,2))
            with self.assertRaises(PublisherBusy):publisher.submit(self.inputs,self.plan)
            future.cancel();closing=asyncio.create_task(publisher.close());await asyncio.sleep(0)
            closing.cancel();await asyncio.sleep(0)
            self.assertFalse(closing.done())
            with self.assertRaises(BlockingIOError):
                SnapshotPublisher(publisher.root,self.inputs.origin,observe=lambda:tuple(self.stamp))
            release.set()
            with self.assertRaises(asyncio.CancelledError):await asyncio.wait_for(closing,5)
        self.assertEqual(publisher.health()['completed'],1)
        replacement=SnapshotPublisher(publisher.root,self.inputs.origin,observe=lambda:tuple(self.stamp));self.addAsyncCleanup(replacement.close)
        self.assertEqual((await self.delivered(replacement)).outcome,'published')

    async def test_invalid_origin_metadata_and_job_caps_are_rejected_before_admission(self):
        publisher=self.publisher(max_jobs=1)
        with self.assertRaises(ValueError):publisher.submit(replace(self.inputs,origin=replace(self.inputs.origin,capture_id='foreign')),self.plan)
        bounded=self.publisher(max_input_bytes=64)
        with self.assertRaises(ValueError):bounded.submit(self.inputs,self.plan)
        self.assertEqual(bounded.health()['submitted'],0)
        await self.delivered(publisher)
        with self.assertRaises(ValueError):publisher.submit(self.inputs,self.plan)

    async def test_invalid_acquisition_timing_records_failure_and_receipt_byte_cap_fails_closed(self):
        publisher=self.publisher()
        result=await self.delivered(publisher,replace(self.inputs,timing_valid=False))
        self.assertEqual(result.reason,'input_timing_invalid')
        bounded=self.publisher(max_receipt_bytes=1)
        with self.assertRaises(PublicationUnconfirmed):await self.delivered(bounded)
