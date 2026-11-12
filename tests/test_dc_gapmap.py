import asyncio
from contextlib import redirect_stderr
from dataclasses import replace
from functools import partial
import io
import threading
import unittest
from unittest.mock import patch

from data_collector.contracts import RawRecord, Receipt, canonical, digest, strict_json
from data_collector.cutoffs import ClockAnchor, CutoffCoordinator, IngressWriter
from data_collector.exports import export_snapshots, read_export
from data_collector.gapmap import map_native_gap
from data_collector.journal import JournalLimits
from data_collector.ledger import DurableLedger
from data_collector.segments import SegmentLimits, iter_segment_records
from data_collector.snapshots import publish_snapshot, read_snapshot
from data_collector.writer import SessionWriter
from tests import test_dc_publisher as publisher_fixtures
from tests import test_dc_pipeline as pipeline_fixtures

C=publisher_fixtures.C


class GapMappingTests(unittest.TestCase):
    setUp=publisher_fixtures.PublisherTests.setUp

    def fixture(self,*,missing_ordinal=False):
        writer=SessionWriter(self.root/'wal',self.root/'archive',venue='deribit',environment='production',capture_id='capture',
            process_session_id='process',provenance=self.clock.provenance,started_utc_ns=C-200,now_ns=lambda:1000,
            limits=JournalLimits(max_record_bytes=262144,target_bytes=64000,max_streams=32),
            segment_limits=SegmentLimits(max_record_bytes=262144,max_uncompressed_bytes=1048576,max_compressed_bytes=1048576))
        self.addCleanup(writer.abort)
        ingress=IngressWriter(writer,'capture','process')
        coordinator=CutoffCoordinator(writer,capture_scope='capture',capture_id='capture',process_session_id='process',provenance=self.clock.provenance)
        ledger=DurableLedger(writer,ledger_id='ledger',capture_scope='capture',capture_id='capture',process_session_id='process',provenance=self.clock.provenance)
        ledger.initialize(utc_ns=C-200,monotonic_ns=900,clock_epoch='process:clock:0')
        anchor=ClockAnchor('process','process:clock:0',C-100,1000,0,2)
        native=[RawRecord.from_bytes(e) for e in self.wal.encoded_records(allow_unsealed=True)
                if not RawRecord.from_bytes(e).receipt.connection_id.startswith('control:')]
        closes=[]
        for identity in range(3):
            cutoff=(identity+1)*C;mono=identity*C+1100
            coordinator.allocate(cutoff,[self.ws,self.meta],self.plan.dependencies,anchor=anchor,
                observed_utc_ns=C-100 if not identity else identity*C,monotonic_ns=1000 if not identity else (identity-1)*C+1100,clock_epoch=anchor.clock_epoch)
            if not identity:
                for record in native:
                    if record.receipt.stream_key==self.ws:
                        record=replace(record,receipt=replace(record.receipt,receive_utc_ns=C,monotonic_ns=1100))
                    record=replace(record,native_fields_json=b'{}')
                    if missing_ordinal:writer.append(record)
                    else:ingress.append(record)
            barrier=ingress.barrier(anchor,cutoff,now_monotonic_ns=mono,healthy_streams=[self.ws,self.meta])
            coordinator.close(identity,barrier,ledger_pin=ledger.pin(),utc_ns=cutoff,monotonic_ns=mono,clock_epoch=anchor.clock_epoch)
            closes.append(coordinator.resolutions[identity]['record_id'])
        writer.rotate()
        snapshots=[] if missing_ordinal else [publish_snapshot(self.root/'snapshots',writer.committed,close,self.plan) for close in closes]
        self.missing_ordinal=missing_ordinal
        self.writer,self.owner,self.ledger_owner,self.ingress_owner=writer,coordinator,ledger,ingress
        self.snapshot_results=snapshots
        self.last_native=next(r for p in writer.committed for r in iter_segment_records(p) if r.receipt.stream_key==self.ws)
        return writer

    def discovery(self,*,control=False,duplicate=False,seq=1):
        receipt=replace(self.last_native.receipt,receive_seq=seq,receive_utc_ns=3*C+seq,monotonic_ns=2*C+1100+seq)
        value=strict_json(self.last_native.body)
        if control:value=dict(jsonrpc='2.0',id=1,result='ok')
        elif not duplicate:
            value['params']['data'].update(type='change',change_id=12,prev_change_id=999,bids=[],asks=[])
        record=replace(self.last_native,receipt=receipt,body=canonical(value),native_fields_json=b'{}')
        if self.missing_ordinal:
            self.writer.append(record);return record
        return self.ingress_owner.append(record)

    def mapped(self,discovery,**kwargs):
        return map_native_gap(self.ledger_owner,self.owner,self.plan,0,discovery.record_id,
            utc_ns=3*C+2,monotonic_ns=2*C+1102,clock_epoch='process-clock',**kwargs)

    def test_exact_proof_retained_and_later_quiet_cutoffs_downgraded_portably(self):
        writer=self.fixture();old=[(r['snapshot_id'],canonical(r['descriptor']),r['payload']) for r in self.snapshot_results]
        proof=self.discovery();writer.rotate() # discovery lives separately from its audit and ledger
        result=self.mapped(proof);self.assertEqual(result.first_cutoff_id,1)
        audit=next(RawRecord.from_bytes(e) for e in writer.encoded_records(allow_unsealed=True)
            if RawRecord.from_bytes(e).record_id==result.mapping_record_id)
        body=strict_json(audit.body)
        self.assertEqual(body['proof_boundary']['ordinal'],2)
        self.assertEqual(body['native_proof_record_id'],self.last_native.record_id)
        for identity,expected in [(0,'valid'),(1,'invalid'),(2,'invalid'),(100,'invalid')]:
            self.assertEqual(self.ledger_owner.assess('capture',self.plan.bindings[0].target,identity,'valid',result.ledger_pin)['assessed_validity'],expected)
        self.assertEqual([row['payload']['streams'][0]['admitted_seq'] for row in self.owner.resolutions.values()],[0,0,0])
        writer.rotate();paths=list(writer.committed)
        export=export_snapshots(self.root/'snapshots',self.root/'exports',[s[0] for s in old],paths)
        self.assertEqual(export['descriptor']['population']['invalid'],2)
        self.assertEqual(read_export(self.root/'exports',export['export_id'],paths),export)
        dependencies={d.record_id for d in self.ledger_owner.dependencies(result.ledger_pin)}
        self.assertTrue({self.last_native.record_id,proof.record_id,result.mapping_record_id}<=dependencies)
        self.assertIn(self.owner.resolutions[0]['record_id'],dependencies)
        for identity,descriptor,payload in old:
            original=read_snapshot(self.root/'snapshots',identity,paths)
            self.assertEqual((canonical(original['descriptor']),original['payload']),(descriptor,payload))
        without_discovery=[p for p in paths if not any(r.record_id==proof.record_id for r in iter_segment_records(p))]
        with self.assertRaises(ValueError):read_export(self.root/'exports',export['export_id'],without_discovery)

    def test_retry_and_ambiguous_audit_write_keep_first_identity_and_range(self):
        writer=self.fixture();proof=self.discovery();append=writer.append
        def after_audit(record):
            append(record)
            if record.channel=='gap_mapping:capture':raise OSError('audit durable, acknowledgement lost')
        with patch.object(writer,'append',side_effect=after_audit):
            with self.assertRaises(OSError):self.mapped(proof)
        first=next(RawRecord.from_bytes(e) for e in writer.encoded_records(allow_unsealed=True)
            if RawRecord.from_bytes(e).channel=='gap_mapping:capture')
        result=self.mapped(proof)
        self.assertEqual(result.mapping_record_id,first.record_id)
        self.assertEqual(result.mapping_sha256,digest(first.to_bytes()))
        before=writer.state.records
        self.assertEqual(self.mapped(proof),result);self.assertEqual(writer.state.records,before)

    def test_missing_ingress_proof_expands_to_zero_instead_of_exempting_quiet_rows(self):
        writer=self.fixture(missing_ordinal=True)
        # Native source without archived ordinal is deliberately legacy evidence.
        proof=self.discovery();result=self.mapped(proof)
        self.assertEqual(result.first_cutoff_id,0)
        self.assertEqual(self.ledger_owner.assess('capture',self.plan.bindings[0].target,0,'valid',result.ledger_pin)['assessed_validity'],'unknown')

    def test_duplicate_or_control_is_not_a_gap_and_bounds_do_not_write(self):
        writer=self.fixture();proof=self.discovery(duplicate=True);before=writer.state.records
        with self.assertRaisesRegex(ValueError,'does not establish'):self.mapped(proof)
        self.assertEqual(writer.state.records,before)
        # A dependency budget must fail before an audit/ledger write.
        with self.assertRaisesRegex(ValueError,'dependency bound'):self.mapped(proof,max_dependencies=1)
        self.assertEqual(writer.state.records,before)

    def test_native_control_ack_is_rejected_instead_of_converted_to_gap(self):
        writer=self.fixture();proof=self.discovery(control=True);before=writer.state.records
        with self.assertRaisesRegex(ValueError,'control delivery'):self.mapped(proof)
        self.assertEqual(writer.state.records,before)

    def test_mapping_proof_transitive_dependencies_are_required_on_restore(self):
        writer=self.fixture();proof=self.discovery();result=self.mapped(proof)
        records=[RawRecord.from_bytes(e) for e in writer.encoded_records(allow_unsealed=True)]
        from types import SimpleNamespace
        empty=SimpleNamespace(identity=writer.identity,encoded_records=lambda **kw:iter(()))
        prior=[r for r in records if r.record_id!=self.owner.resolutions[1]['record_id']]
        with self.assertRaisesRegex(ValueError,'mapping raw dependency'):
            DurableLedger(empty,ledger_id='ledger',capture_scope='capture',capture_id='capture',process_session_id='later',
                provenance=self.clock.provenance,prior_records=lambda:iter(prior),expected_pin=result.ledger_pin)
        audit=next(r for r in records if r.record_id==result.mapping_record_id)
        corrupted=strict_json(audit.body);corrupted['first_cutoff_id']=2
        prior=[replace(r,body=canonical(corrupted)) if r.record_id==audit.record_id else r for r in records]
        with self.assertRaisesRegex(ValueError,'differs from native mapping'):
            DurableLedger(empty,ledger_id='ledger',capture_scope='capture',capture_id='capture',process_session_id='later',
                provenance=self.clock.provenance,prior_records=lambda:iter(prior))

    def test_multiplexed_other_instrument_keeps_receipts_but_does_not_poison_target(self):
        writer=self.fixture()
        inst=replace(self.last_native.instrument,native_symbol='BTC-OTHER-C')
        channel=f'book.{inst.native_symbol}.100ms';value=strict_json(self.last_native.body)
        value['params']['channel']=channel;value['params']['data']['instrument_name']=inst.native_symbol
        other=replace(self.last_native,instrument=inst,channel=channel,body=canonical(value),native_fields_json=b'{}',
            receipt=replace(self.last_native.receipt,receive_seq=1,receive_utc_ns=3*C+1,monotonic_ns=2*C+1101))
        other=self.ingress_owner.append(other);proof=self.discovery(seq=2)
        result=self.mapped(proof)
        self.assertEqual(result.first_cutoff_id,1)
        self.assertIn(other.record_id,{r.record_id for r in self.ledger_owner.dependencies(result.ledger_pin)})

    def test_orphan_audit_blocks_certification_and_can_complete_exactly_after_restart(self):
        writer=self.fixture();proof=self.discovery();append=writer.append
        def lost(record):
            append(record)
            if record.channel=='gap_mapping:capture':raise OSError('audit acknowledgement lost')
        with patch.object(writer,'append',side_effect=lost):
            with self.assertRaises(OSError):self.mapped(proof)
        old=DurableLedger(writer,ledger_id='ledger',capture_scope='capture',capture_id='capture',process_session_id='process',
            provenance=self.clock.provenance)
        with self.assertRaisesRegex(ValueError,'unresolved native mapping'):old.pin()
        writer.finish();paths=tuple(writer.committed)
        prior=lambda:(r for p in paths for r in iter_segment_records(p))
        later=SessionWriter(self.root/'later-wal',self.root/'archive',venue='deribit',environment='production',capture_id='capture',
            process_session_id='later',provenance=self.clock.provenance,started_utc_ns=4*C,now_ns=lambda:5)
        self.addCleanup(later.abort)
        ledger=DurableLedger(later,ledger_id='ledger',capture_scope='capture',capture_id='capture',process_session_id='later',
            provenance=self.clock.provenance,prior_records=prior)
        coordinator=CutoffCoordinator(later,capture_scope='capture',capture_id='capture',process_session_id='later',
            provenance=self.clock.provenance,prior_records=prior)
        result=map_native_gap(ledger,coordinator,self.plan,0,proof.record_id,utc_ns=4*C,monotonic_ns=5,clock_epoch='later-process')
        self.assertEqual(result.first_cutoff_id,1)
        audit=next(r for r in prior() if r.channel=='gap_mapping:capture')
        self.assertEqual(result.mapping_sha256,digest(audit.to_bytes()))
        self.assertEqual(ledger.assess('capture',self.plan.bindings[0].target,1,'valid',result.ledger_pin)['assessed_validity'],'invalid')
        controls=[RawRecord.from_bytes(e) for e in later.encoded_records(allow_unsealed=True)]
        self.assertEqual(len(controls),1)
        self.assertEqual(controls[0].receipt.receive_seq,0)
        self.assertEqual(strict_json(controls[0].body)['sequence'],1)

    def test_unknown_correction_ack_reopens_to_same_mapping_without_second_entry(self):
        writer=self.fixture();proof=self.discovery();append=writer.append
        def lost(record):
            append(record)
            if record.channel=='validity:capture':raise OSError('correction fsync acknowledgement lost')
        with patch.object(writer,'append',side_effect=lost):
            with self.assertRaises(OSError):self.mapped(proof)
        self.ledger_owner=DurableLedger(writer,ledger_id='ledger',capture_scope='capture',capture_id='capture',process_session_id='process',
            provenance=self.clock.provenance)
        before=writer.state.records;result=self.mapped(proof)
        self.assertEqual(writer.state.records,before)
        self.assertEqual(result.ledger_pin.sequence,1)

    def test_buffered_owner_routes_other_symbol_before_starting_bootstrap_clock(self):
        from data_collector.journal import Journal
        from data_collector.replay import BookBinding,SnapshotPlan
        from data_collector.validity import FeedTarget
        from tests.helpers import key
        inst=key(venue='bybit',native_product='linear',native_symbol='BTCUSDT')
        other=replace(inst,native_symbol='ETHUSDT');channel='orderbook.full.BTCUSDT'
        ws=replace(self.ws,connection_id='ws');seed=replace(self.ws,connection_id='seed')
        plan=SnapshotPlan((BookBinding(FeedTarget(inst,'bybit_full','owner'),ws,self.meta,'a'*64,seed_source=seed),),'b'*64)
        with Journal(self.root/'buffered.wal',venue='bybit',environment='production',create=True) as wal:
            owner=IngressWriter(wal,'capture','process')
            def raw(instrument,source,seq,mono,payload,topic,kind='ws_message',fields=None):
                return owner.append(RawRecord(Receipt('capture','process',source.connection_id,seq,mono,mono,'epoch'),
                    'bybit','production','linear',topic,kind,canonical(payload),self.clock.provenance,instrument,
                    canonical(fields or {})))
            raw(other,ws,0,10,dict(topic='orderbook.full.ETHUSDT',type='delta',ts=1,
                data=dict(s='ETHUSDT',u=10,seq=100,b=[],a=[])),'orderbook.full.ETHUSDT')
            now=100_000_000_000
            raw(inst,ws,1,now,dict(topic=channel,type='delta',ts=1,data=dict(s='BTCUSDT',u=10,seq=100,b=[],a=[])),channel)
            anchor=raw(inst,seed,0,now+1,dict(retCode=0,result=dict(s='BTCUSDT',u=10,seq=100,b=[['1','2']],a=[])),
                '/v5/market/full_orderbook','rest_response',dict(request_category='linear',request_symbol='BTCUSDT'))
            gap=raw(inst,ws,2,now+2,dict(topic=channel,type='delta',ts=1,data=dict(s='BTCUSDT',u=12,seq=102,b=[],a=[])),channel)
            ledger=DurableLedger(wal,ledger_id='ledger',capture_scope='capture',capture_id='capture',process_session_id='process',provenance=self.clock.provenance)
            coordinator=CutoffCoordinator(wal,capture_scope='capture',capture_id='capture',process_session_id='process',provenance=self.clock.provenance)
            result=map_native_gap(ledger,coordinator,plan,0,gap.record_id,utc_ns=now+3,monotonic_ns=now+3,clock_epoch='epoch')
            audit=next(RawRecord.from_bytes(e) for e in wal.encoded_records(allow_unsealed=True)
                if RawRecord.from_bytes(e).record_id==result.mapping_record_id)
            self.assertEqual(strict_json(audit.body)['native_proof_record_id'],anchor.record_id)
            self.assertEqual(strict_json(audit.body)['reason'],'u_gap')


class GapLaneTests(unittest.IsolatedAsyncioTestCase):
    setUp=pipeline_fixtures.PipelineTests.setUp
    observe=pipeline_fixtures.PipelineTests.observe
    lane=pipeline_fixtures.PipelineTests.lane
    start=pipeline_fixtures.PipelineTests.start
    record=pipeline_fixtures.PipelineTests.record
    received=pipeline_fixtures.PipelineTests.received
    clock_request=pipeline_fixtures.PipelineTests.clock_request
    clock_policy=pipeline_fixtures.PipelineTests.clock_policy
    ready_cutoffs=pipeline_fixtures.PipelineTests.ready_cutoffs
    ready=pipeline_fixtures.PipelineTests.ready
    measured_wait=pipeline_fixtures.PipelineTests.measured_wait
    records=pipeline_fixtures.PipelineTests.records
    controls=pipeline_fixtures.PipelineTests.controls
    schedule=pipeline_fixtures.PipelineTests.schedule
    pipeline=pipeline_fixtures.PipelineTests.pipeline
    finish_raw=pipeline_fixtures.PipelineTests.finish_raw

    async def discover(self,lane):
        body=canonical(dict(jsonrpc='2.0',method='subscription',params=dict(channel=self.plan.bindings[0].native_channel,
            data=dict(type='change',timestamp=2,instrument_name=self.plan.bindings[0].target.instrument.native_symbol,
            change_id=12,prev_change_id=999,bids=[],asks=[]))))
        return await lane.submit_received(replace(self.received(lane,1),channel=self.plan.bindings[0].native_channel,body=body))

    def submit_gap(self,lane,discovery):
        utc,mono,_=self.observe()
        return lane.submit_native_gap(self.plan,0,discovery.record_id,utc_ns=utc,monotonic_ns=mono)

    async def test_fifo_mapping_after_published_quiet_rows_changes_assessment_not_original(self):
        lane=await self.ready();first=asyncio.Event();waits=[]
        async def wait(deadline):
            if waits:await first.wait()
            waits.append(deadline);await self.measured_wait(deadline)
        pipeline=self.pipeline(lane,max_cutoffs=2,wait_until=wait)
        original=pipeline.schedule.ack
        def ack(close):original(close);first.set()
        with patch.object(pipeline.schedule,'ack',side_effect=ack):await pipeline.run()
        gap=await self.discover(lane)
        ack=await self.submit_gap(lane,gap)
        self.assertTrue(ack.timing_valid);self.assertEqual(ack.mapping.first_cutoff_id,0)
        self.assertEqual((await self.submit_gap(lane,gap)).mapping,ack.mapping)
        self.assertTrue(lane.health()['accepting'])
        await self.finish_raw(lane)
        audit=[r for r in self.records(lane) if r.channel=='gap_mapping:capture']
        self.assertEqual(len(audit),1)
        reports=[strict_json(r.body) for r in self.records(lane) if r.channel=='snapshot_deliveries:capture']
        result=export_snapshots(pipeline.publisher.root/'snapshots',self.root/'exports',[r['snapshot_id'] for r in reports],lane.segments)
        self.assertEqual(result['descriptor']['population']['invalid'],2)

    async def test_cancelled_client_does_not_drop_admitted_audit_or_correction(self):
        lane=await self.ready();gap=await self.discover(lane)
        entered,release=threading.Event(),threading.Event();self.addCleanup(release.set)
        original=lane._execute_gap_mapping
        def blocked(request):
            entered.set()
            if not release.wait(3):raise OSError('test gate timeout')
            return original(request)
        with patch.object(lane,'_execute_gap_mapping',side_effect=blocked):
            future=self.submit_gap(lane,gap)
            self.assertTrue(await asyncio.to_thread(entered.wait,2));future.cancel();release.set()
            async def done():
                while lane.queue.health()['bytes']:await asyncio.sleep(0)
            await asyncio.wait_for(done(),5)
        self.assertEqual(lane._cutoffs.ledger.pin().sequence,1)
        self.assertTrue(lane.health()['accepting'])
        await self.finish_raw(lane)
        self.assertEqual(len([r for r in self.records(lane) if r.channel=='gap_mapping:capture']),1)
