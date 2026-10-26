import asyncio
from dataclasses import replace
import unittest
from unittest.mock import patch

from data_collector.contracts import RawRecord,canonical,digest,strict_json
from data_collector.cutoffs import ClockAnchor,CutoffCoordinator
from data_collector.exports import export_snapshots,read_export
from data_collector.gapmap import map_native_gap
from data_collector.gaprecovery import SCHEMA,map_native_recovery
from data_collector.journal import Journal
from data_collector.publisher import SnapshotPublisher
from data_collector.deliveries import MAX_REPORT_BYTES
from data_collector.ledger import DurableLedger
from data_collector.segments import SegmentContext,iter_segment_records,seal_segment
from data_collector.snapshots import publish_snapshot,read_snapshot
from data_collector.validity import CloseGap,LedgerEntry
from tests import test_dc_gapmap as gaps
from tests import test_dc_replay as replay_fixtures

C=gaps.C


class NativeRecoveryTests(unittest.TestCase):
    setUp=gaps.GapMappingTests.setUp
    fixture=gaps.GapMappingTests.fixture
    discovery=gaps.GapMappingTests.discovery
    mapped=gaps.GapMappingTests.mapped

    def start(self):
        self.fixture();self.proof=self.discovery();self.gap=self.mapped(self.proof)

    def native(self,*,stale=False,late=False):
        body=strict_json(self.last_native.body)
        body['params']['data'].update(change_id=10 if stale else 20,bids=[['new',100,9]])
        mono=(3 if late else 2)*C+(1101 if late else 1110)
        utc=(4 if late else 3)*C+(1 if late else 10)
        record=replace(self.last_native,body=canonical(body),native_fields_json=b'{}',
            receipt=replace(self.last_native.receipt,receive_seq=2,receive_utc_ns=utc,monotonic_ns=mono))
        return self.ingress_owner.append(record)

    def next_close(self,*,healthy=True):
        identity=len(self.owner.allocations);cutoff=(identity+1)*C;deadline=identity*C+1100
        anchor=ClockAnchor('process','process:clock:0',C-100,1000,0,2)
        self.owner.allocate(cutoff,[self.ws,self.meta],self.plan.dependencies,anchor=anchor,
            observed_utc_ns=identity*C+20,monotonic_ns=(identity-1)*C+1120,clock_epoch=anchor.clock_epoch)
        barrier=self.ingress_owner.barrier(anchor,cutoff,now_monotonic_ns=deadline,
            healthy_streams=[self.ws,self.meta] if healthy else [])
        self.owner.close(identity,barrier,ledger_pin=self.ledger_owner.pin(),utc_ns=cutoff,
            monotonic_ns=deadline,clock_epoch=anchor.clock_epoch)
        self.writer.rotate()
        return self.owner.resolutions[identity]['record_id']

    def recover(self,**kwargs):
        if self.writer.active.state.records:self.writer.rotate()
        return map_native_recovery(self.ledger_owner,self.owner,self.writer.committed,self.plan,0,self.gap.correction_id,
            utc_ns=6*C,monotonic_ns=5*C+1100,clock_epoch='process:clock:0',**kwargs)

    def reopen(self):
        self.ledger_owner=DurableLedger(self.writer,ledger_id='ledger',capture_scope='capture',capture_id='capture',
            process_session_id='process',provenance=self.clock.provenance)

    def test_first_recovery_closure_proof_and_historical_interval_export_portably(self):
        self.start();anchor=self.native();close=self.next_close();result=self.recover()
        self.assertEqual(result.recovery_cutoff_id,3)
        payloads=[e.payload for e in self.ledger_owner.entries(result.ledger_pin)]
        resolution=payloads[-1];self.assertIsInstance(resolution,CloseGap)
        self.assertEqual(resolution.recovery_anchor_record_id,anchor.record_id)
        for identity,expected in [(0,'valid'),(1,'invalid'),(2,'invalid'),(3,'valid'),(4,'valid')]:
            actual=self.ledger_owner.assess('capture',self.plan.bindings[0].target,identity,'valid',result.ledger_pin)
            self.assertEqual(actual['assessed_validity'],expected)
        self.writer.rotate()
        new=publish_snapshot(self.root/'snapshots',self.writer.committed,close,self.plan)
        self.assertEqual(strict_json(new['payload'])['books'][0]['validity_as_published'],'valid')
        ids=[s['snapshot_id'] for s in self.snapshot_results]+[new['snapshot_id']]
        exported=export_snapshots(self.root/'snapshots',self.root/'exports',ids,self.writer.committed)
        self.assertEqual(exported['descriptor']['population']['invalid'],2)
        self.assertEqual(read_export(self.root/'exports',exported['export_id'],self.writer.committed),exported)
        deps={e.record_id for e in self.ledger_owner.dependencies(result.ledger_pin)}
        self.assertTrue({anchor.record_id,close,result.mapping_record_id,self.proof.record_id}<=deps)
        # Recovery seed/deltas and earlier receipts are transitive dependencies.
        source=self.writer.committed[-2]
        omitted=[p for p in self.writer.committed if p!=source]
        with self.assertRaises(ValueError):read_export(self.root/'exports',exported['export_id'],omitted)

    def test_quiet_or_stale_snapshot_never_resolves_gap(self):
        self.start();before=self.ledger_owner.pin()
        self.assertIsNone(self.recover())
        self.native(stale=True);self.next_close()
        self.assertIsNone(self.recover())
        self.assertEqual(self.ledger_owner.pin(),before)
        self.assertFalse(any(strict_json(r.body).get('schema')==SCHEMA for p in self.writer.committed
            for r in iter_segment_records(p) if r.channel=='gap_mapping:capture'))

    def test_anchor_after_barrier_waits_for_later_healthy_closure(self):
        self.start();self.next_close();self.native(late=True)
        self.assertIsNone(self.recover())
        self.next_close(healthy=False);self.assertIsNone(self.recover())
        self.next_close();self.assertEqual(self.recover().recovery_cutoff_id,5)

    def test_lost_audit_and_resolution_ack_retry_without_duplicate(self):
        for channel in ('gap_mapping:capture','validity:capture'):
            with self.subTest(channel=channel):
                # New fixture root for each ambiguous acknowledgement stage.
                case=NativeRecoveryTests();case.setUp()
                try:
                    case.start();case.native();case.next_close();append=case.writer.append
                    def lost(record):
                        append(record)
                        if record.channel==channel:raise OSError('lost durable ack')
                    with patch.object(case.writer,'append',side_effect=lost):
                        with self.assertRaises(OSError):case.recover()
                    case.reopen();result=case.recover();count=case.writer.state.records
                    self.assertEqual(case.recover(),result);self.assertEqual(case.writer.state.records,count)
                    audits=[r for encoded in case.writer.encoded_records(allow_unsealed=True)
                            if (r:=RawRecord.from_bytes(encoded)).channel=='gap_mapping:capture' and strict_json(r.body)['schema']==SCHEMA]
                    self.assertEqual(len(audits),1)
                    self.assertEqual(result.ledger_pin.sequence,2)
                finally:case.doCleanups()

    def test_hash_valid_alternate_replay_cannot_replace_owned_native_bytes(self):
        self.start();anchor=self.native();close=self.next_close()
        body=strict_json(anchor.body);body['params']['data']['bids']=[['new',99,9]]
        alternate_anchor=replace(anchor,body=canonical(body))
        with Journal(self.root/'alternate.wal',venue='deribit',environment='production',create=True) as wal:
            for encoded in self.writer.encoded_records(allow_unsealed=True):
                record=RawRecord.from_bytes(encoded)
                if record.record_id==anchor.record_id:record=alternate_anchor
                if record.record_id==close:
                    value=strict_json(record.body)
                    for row in value['payload']['streams']:
                        if row['stream']==dict(capture_id='capture',process_session_id='process',connection_id='ws'):
                            row['record_hash']=digest(alternate_anchor.to_bytes())
                    record=replace(record,body=canonical(value))
                wal.append(record)
            alternate=seal_segment(wal,self.root/'alternate-archive',SegmentContext('deribit','production','capture','process','alt','a'*64,'b'*64))
        before=self.writer.state.records
        with self.assertRaisesRegex(ValueError,'differ from owned prefix'):
            map_native_recovery(self.ledger_owner,self.owner,[alternate],self.plan,0,self.gap.correction_id,
                utc_ns=6*C,monotonic_ns=5*C+1100,clock_epoch='process')
        self.assertEqual(self.writer.state.records,before)

    def test_unresolved_earlier_cutoff_blocks_later_closed_candidate(self):
        self.start();self.native()
        anchor=ClockAnchor('process','process:clock:0',C-100,1000,0,2)
        for identity in (3,4):
            self.owner.allocate((identity+1)*C,[self.ws,self.meta],self.plan.dependencies,anchor=anchor,
                observed_utc_ns=identity*C+20,monotonic_ns=(identity-1)*C+1120,clock_epoch=anchor.clock_epoch)
        barrier=self.ingress_owner.barrier(anchor,5*C,now_monotonic_ns=4*C+1100,healthy_streams=[self.ws,self.meta])
        self.owner.close(4,barrier,ledger_pin=self.ledger_owner.pin(),utc_ns=5*C,monotonic_ns=4*C+1100,clock_epoch=anchor.clock_epoch)
        self.assertIsNone(self.recover())
        barrier=self.ingress_owner.barrier(anchor,4*C,now_monotonic_ns=4*C+1100,healthy_streams=[self.ws,self.meta])
        self.owner.close(3,barrier,ledger_pin=self.ledger_owner.pin(),utc_ns=5*C,monotonic_ns=4*C+1100,clock_epoch=anchor.clock_epoch)
        self.assertEqual(self.recover().recovery_cutoff_id,3)

    def test_delayed_earlier_failure_is_required_by_portable_first_closure_proof(self):
        self.start();self.native()
        anchor=ClockAnchor('process','process:clock:0',C-100,1000,0,2)
        for identity in (3,4):
            self.owner.allocate((identity+1)*C,[self.ws,self.meta],self.plan.dependencies,anchor=anchor,
                observed_utc_ns=identity*C+20,monotonic_ns=(identity-1)*C+1120,clock_epoch=anchor.clock_epoch)
        barrier=self.ingress_owner.barrier(anchor,5*C,now_monotonic_ns=4*C+1100,healthy_streams=[self.ws,self.meta])
        self.owner.close(4,barrier,ledger_pin=self.ledger_owner.pin(),utc_ns=5*C,monotonic_ns=4*C+1100,clock_epoch=anchor.clock_epoch)
        self.writer.rotate()
        self.owner.fail(3,'late interruption outcome',utc_ns=5*C+1,monotonic_ns=4*C+1101,clock_epoch=anchor.clock_epoch)
        failure=self.owner.resolutions[3]['record_id'];self.writer.rotate();failure_segment=self.writer.committed[-1]
        result=self.recover();self.assertEqual(result.recovery_cutoff_id,4)
        self.assertIn(failure,{e.record_id for e in self.ledger_owner.dependencies(result.ledger_pin)})
        self.writer.rotate();close=self.owner.resolutions[4]['record_id']
        new=publish_snapshot(self.root/'snapshots',self.writer.committed,close,self.plan)
        exported=export_snapshots(self.root/'snapshots',self.root/'exports',[new['snapshot_id']],self.writer.committed)
        self.assertEqual(read_export(self.root/'exports',exported['export_id'],self.writer.committed),exported)
        without_failure=[p for p in self.writer.committed if p!=failure_segment]
        with self.assertRaises(ValueError):read_export(self.root/'exports',exported['export_id'],without_failure)

    def future_gap(self):
        self.fixture()
        body=strict_json(self.last_native.body);body['params']['data'].update(change_id=20,bids=[['new',100,9]])
        source=replace(self.last_native,body=canonical(body),native_fields_json=b'{}',
            receipt=replace(self.last_native.receipt,receive_seq=1,receive_utc_ns=4*C,monotonic_ns=3*C+1100))
        self.ingress_owner.append(source);self.next_close()
        body['params']['data'].update(type='change',change_id=21,prev_change_id=999,bids=[],asks=[])
        discovery=self.ingress_owner.append(replace(source,body=canonical(body),native_fields_json=b'{}',
            receipt=replace(source.receipt,receive_seq=2,receive_utc_ns=4*C+1,monotonic_ns=3*C+1101)))
        self.gap=map_native_gap(self.ledger_owner,self.owner,self.plan,0,discovery.record_id,
            utc_ns=4*C+2,monotonic_ns=3*C+1102,clock_epoch='process:clock:0')
        self.assertEqual(self.gap.first_cutoff_id,4)
        body['params']['data'].update(type='snapshot',change_id=30,bids=[['new',100,9]],asks=[['new',101,3]])
        self.ingress_owner.append(replace(source,body=canonical(body),native_fields_json=b'{}',
            receipt=replace(source.receipt,receive_seq=3,receive_utc_ns=4*C+10,monotonic_ns=3*C+1110)))
        return ClockAnchor('process','process:clock:0',C-100,1000,0,2)

    def test_changed_plan_at_first_affected_cutoff_blocks_recovery(self):
        anchor=self.future_gap()
        self.owner.allocate(5*C,[self.ws,self.meta],dict(self.plan.dependencies,required_feeds='f'*64),anchor=anchor,
            observed_utc_ns=4*C+20,monotonic_ns=3*C+1120,clock_epoch=anchor.clock_epoch)
        self.owner.fail(4,'different plan failed',utc_ns=5*C,monotonic_ns=4*C+1100,clock_epoch=anchor.clock_epoch)
        self.next_close();before=self.writer.state.records
        self.assertIsNone(self.recover());self.assertEqual(self.writer.state.records,before)

    def test_unresolved_first_affected_cutoff_cannot_be_skipped(self):
        anchor=self.future_gap()
        self.owner.allocate(5*C,[self.ws,self.meta],self.plan.dependencies,anchor=anchor,
            observed_utc_ns=4*C+20,monotonic_ns=3*C+1120,clock_epoch=anchor.clock_epoch)
        self.next_close();before=self.writer.state.records
        self.assertIsNone(self.recover());self.assertEqual(self.writer.state.records,before)

    def test_failed_cutoff_with_changed_plan_cannot_be_skipped(self):
        self.start();self.native()
        anchor=ClockAnchor('process','process:clock:0',C-100,1000,0,2)
        dependencies=dict(self.plan.dependencies,required_feeds='f'*64)
        self.owner.allocate(4*C,[self.ws,self.meta],dependencies,anchor=anchor,
            observed_utc_ns=3*C+20,monotonic_ns=2*C+1120,clock_epoch=anchor.clock_epoch)
        self.owner.fail(3,'synthetic plan interruption',utc_ns=3*C+21,monotonic_ns=2*C+1121,clock_epoch=anchor.clock_epoch)
        self.next_close();before=self.writer.state.records
        self.assertIsNone(self.recover())
        self.assertEqual(self.writer.state.records,before)
        self.assertEqual(self.ledger_owner.pin(),self.gap.ledger_pin)

    def test_bounds_and_different_plan_refuse_before_audit_write(self):
        self.start();self.native();self.next_close();count=self.writer.state.records
        for options in (dict(max_dependencies=1),dict(max_mapping_bytes=1)):
            with self.assertRaisesRegex(ValueError,'bound'):self.recover(**options)
            self.assertEqual(self.writer.state.records,count)
        plan=replace(self.plan,bindings=(replace(self.plan.bindings[0],ws_start_seq=2),))
        with self.assertRaisesRegex(ValueError,'exact native gap plan'):
            map_native_recovery(self.ledger_owner,self.owner,self.writer.committed,plan,0,self.gap.correction_id,
                utc_ns=6*C,monotonic_ns=5*C+1100,clock_epoch='process')
        self.assertEqual(self.writer.state.records,count)


class BufferedRecoveryTests(unittest.TestCase):
    setUp=replay_fixtures.ReplayTests.setUp
    fixture=replay_fixtures.ReplayTests.fixture

    def test_buffered_seed_bridge_recovers_and_restarts_with_exact_proof(self):
        for profile in ('bybit_full','binance_spot','binance_um'):
            with self.subTest(profile=profile):self.roundtrip(profile)

    def roundtrip(self,profile):
        segment,closes,plan=self.fixture(profile,native_gap=True,recover=True,second_cutoff=True)
        work=self.root/('owners-'+str(self.counter));work.mkdir()
        venue=plan.bindings[0].target.instrument.venue
        rows=tuple(iter_segment_records(segment));provenance=rows[0].provenance
        with Journal(work/'later.wal',venue=venue,environment='production',create=True) as wal:
            prior=lambda:iter_segment_records(segment)
            coordinator=CutoffCoordinator(wal,capture_scope='capture',capture_id='capture',process_session_id='later',
                provenance=provenance,prior_records=prior)
            ledger=DurableLedger(wal,ledger_id='ledger',capture_scope='capture',capture_id='capture',process_session_id='later',
                provenance=provenance,prior_records=prior)
            discovery=next(r for r in rows if r.receipt.connection_id=='ws' and r.receipt.receive_seq==1)
            gap=map_native_gap(ledger,coordinator,plan,0,discovery.record_id,utc_ns=3*C,monotonic_ns=5,clock_epoch='later')
            result=map_native_recovery(ledger,coordinator,[segment],plan,0,gap.correction_id,
                utc_ns=3*C,monotonic_ns=6,clock_epoch='later')
            self.assertEqual(result.recovery_cutoff_id,1)
            self.assertEqual(ledger.assess('capture',plan.bindings[0].target,0,'valid',result.ledger_pin)['assessed_validity'],'invalid')
            self.assertEqual(ledger.assess('capture',plan.bindings[0].target,1,'valid',result.ledger_pin)['assessed_validity'],'valid')
            later=seal_segment(wal,work/'later-archive',SegmentContext(venue,'production','capture','later','0','a'*64,'b'*64))
        with Journal(work/'third.wal',venue=venue,environment='production',create=True) as wal:
            prior=lambda:(r for p in (segment,later) for r in iter_segment_records(p))
            ledger=DurableLedger(wal,ledger_id='ledger',capture_scope='capture',capture_id='capture',process_session_id='third',
                provenance=provenance,prior_records=prior)
            coordinator=CutoffCoordinator(wal,capture_scope='capture',capture_id='capture',process_session_id='third',
                provenance=provenance,prior_records=prior)
            again=map_native_recovery(ledger,coordinator,[segment,later],plan,0,gap.correction_id,
                utc_ns=4*C,monotonic_ns=1,clock_epoch='third')
            self.assertEqual(again,result);self.assertEqual(wal.state.records,0)


class NativeRecoveryLaneTests(unittest.IsolatedAsyncioTestCase):
    setUp=gaps.GapLaneTests.setUp
    observe=gaps.GapLaneTests.observe
    lane=gaps.GapLaneTests.lane
    start=gaps.GapLaneTests.start
    record=gaps.GapLaneTests.record
    received=gaps.GapLaneTests.received
    clock_request=gaps.GapLaneTests.clock_request
    clock_policy=gaps.GapLaneTests.clock_policy
    ready_cutoffs=gaps.GapLaneTests.ready_cutoffs
    ready=gaps.GapLaneTests.ready
    measured_wait=gaps.GapLaneTests.measured_wait
    records=gaps.GapLaneTests.records
    controls=gaps.GapLaneTests.controls
    schedule=gaps.GapLaneTests.schedule
    pipeline=gaps.GapLaneTests.pipeline
    finish_raw=gaps.GapLaneTests.finish_raw
    discover=gaps.GapLaneTests.discover
    submit_gap=gaps.GapLaneTests.submit_gap

    async def test_fifo_gap_seed_closure_resolution_and_fresh_publication(self):
        lane=await self.ready();pipeline=self.pipeline(lane);await pipeline.run()
        self.mono[0]+=1;discovery=await self.discover(lane);gap=(await self.submit_gap(lane,discovery)).mapping
        async def recover():
            utc,mono,_=self.observe()
            return await lane.submit_native_recovery(self.plan,0,gap.correction_id,utc_ns=utc,monotonic_ns=mono)
        self.assertIsNone((await recover()).mapping)
        binding=self.plan.bindings[0]
        body=canonical(dict(jsonrpc='2.0',method='subscription',params=dict(channel=binding.native_channel,
            data=dict(type='snapshot',timestamp=3,instrument_name=binding.target.instrument.native_symbol,
                change_id=20,bids=[['new',100,9]],asks=[['new',101,3]]))))
        await lane.submit_received(replace(self.received(lane,2),channel=binding.native_channel,body=body))
        utc,mono,_=self.observe()
        await lane.submit_cutoff_allocation(2*C,[self.ws,self.meta],self.plan.dependencies,utc_ns=utc,monotonic_ns=mono,boot_id='boot')
        await self.measured_wait(C+1100);utc,mono,_=self.observe()
        closed=await lane.submit_cutoff_close(1,healthy_streams=[self.ws,self.meta],utc_ns=utc,monotonic_ns=mono,boot_id='boot')
        recovered=await recover()
        self.assertTrue(recovered.timing_valid);self.assertEqual(recovered.mapping.recovery_cutoff_id,1)
        self.assertEqual((await recover()).mapping,recovered.mapping)
        inputs=await lane.submit_snapshot_inputs(closed.record_id,utc_ns=utc,monotonic_ns=mono)
        publisher=SnapshotPublisher(self.root/'recovered-publisher',pipeline.publisher.origin,observe=self.observe,max_receipt_bytes=MAX_REPORT_BYTES)
        self.addAsyncCleanup(publisher.close)
        delivered=await publisher.submit(inputs,self.plan)
        self.assertEqual(delivered.outcome,'published')
        result=read_snapshot(publisher.root/'snapshots',delivered.snapshot_id,[s.path for s in inputs.segments])
        self.assertEqual(strict_json(result['payload'])['books'][0]['validity_as_published'],'valid')
        await lane.submit_snapshot_delivery(inputs,delivered.receipt_body,utc_ns=utc,monotonic_ns=mono)
        self.assertTrue(lane.health()['accepting']);await self.finish_raw(lane)
