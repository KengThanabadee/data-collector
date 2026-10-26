import asyncio
from dataclasses import replace
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

from data_collector.contracts import Provenance,RawRecord,Receipt,StreamKey,canonical,strict_json
from data_collector.cutoffs import ClockAnchor,CutoffCoordinator,IngressWriter
from data_collector.journal import Journal
from data_collector.metadata import MetadataVersion
from data_collector.ledger import DurableLedger
from data_collector.owners import OwnerPlans
from data_collector.replay import BookBinding,SnapshotPlan,materialize_snapshot
from data_collector.segments import SegmentContext,iter_segment_records,seal_segment
from data_collector.snapshots import publish_snapshot,read_snapshot
from data_collector.validity import FeedTarget
from tests.test_dc_contracts import key
from tests import test_dc_gapmap as gaps

C=gaps.C
E1='dc-owner/1:'+'1'*32
E2='dc-owner/1:'+'2'*32
E3='dc-owner/1:'+'3'*32


class OwnerTests(unittest.TestCase):
    setUp=gaps.GapMappingTests.setUp
    fixture=gaps.GapMappingTests.fixture

    def registry(self,**kwargs):
        return OwnerPlans(self.writer,capture_scope='capture',capture_id='capture',process_session_id='process',
            provenance=self.clock.provenance,**kwargs)

    def replacement(self,epoch=E1,connection='ws2',start=0,metadata=None):
        binding=self.plan.bindings[0]
        return SnapshotPlan((replace(binding,target=replace(binding.target,owner_epoch=epoch),
            ws_source=StreamKey('capture','process',connection),ws_start_seq=start,
            metadata_version_id=metadata or binding.metadata_version_id),),self.plan.config_sha256)

    def next_stamp(self):
        mono=max(RawRecord.from_bytes(e).receipt.monotonic_ns for e in self.writer.encoded_records(allow_unsealed=True))+1
        return C-1100+mono,mono

    def declare(self,plan,**kwargs):
        utc,mono=self.next_stamp()
        return self.registry(**kwargs).declare(plan,utc_ns=utc,monotonic_ns=mono,clock_epoch='process:clock:0')

    def native(self,plan,seq=0,*,snapshot=True,size=9):
        binding=plan.bindings[0];body=strict_json(self.last_native.body)
        body['params']['data'].update(type='snapshot' if snapshot else 'change',change_id=20+seq,
            bids=[['new' if snapshot else 'change',100,size]],asks=[['new',101,3]] if snapshot else [])
        if not snapshot:body['params']['data']['prev_change_id']=10
        utc,mono=self.next_stamp()
        return self.ingress_owner.append(replace(self.last_native,body=canonical(body),native_fields_json=b'{}',
            receipt=Receipt('capture','process',binding.ws_source.connection_id,seq,utc,mono,'process:clock:0')))

    def allocate(self,plan):
        identity=len(self.owner.allocations);anchor=ClockAnchor('process','process:clock:0',C-100,1000,0,2)
        streams=[plan.bindings[0].ws_source,plan.bindings[0].metadata_source]
        self.owner.allocate((identity+1)*C,streams,plan.dependencies,anchor=anchor,
            observed_utc_ns=identity*C+20,monotonic_ns=(identity-1)*C+1120,clock_epoch=anchor.clock_epoch)
        return identity,anchor,streams

    def close_allocated(self,allocation,plan):
        identity,anchor,streams=allocation;cutoff=(identity+1)*C;mono=identity*C+1100
        barrier=self.ingress_owner.barrier(anchor,cutoff,now_monotonic_ns=mono,healthy_streams=streams)
        self.owner.close(identity,barrier,ledger_pin=self.ledger_owner.pin(),utc_ns=cutoff,monotonic_ns=mono,clock_epoch=anchor.clock_epoch)
        self.writer.rotate();close=self.owner.resolutions[identity]['record_id']
        return publish_snapshot(self.root/'managed',self.writer.committed,close,plan)

    def test_overlap_uses_selected_connection_and_preserves_legacy_snapshots(self):
        self.fixture();plan=self.replacement();declaration=self.declare(plan)
        self.native(self.plan,1,snapshot=False,size=4) # old owner continues to be archived
        self.native(plan,size=9)
        result=self.close_allocated(self.allocate(plan),plan)
        row=strict_json(result['payload'])['books'][0]
        self.assertEqual(row['book']['bids'],[['100','9']]);self.assertEqual(row['validity_as_published'],'valid')
        self.assertEqual(row['target']['owner_epoch'],E1)
        self.assertEqual(result['descriptor']['owner_plan_dependencies'],[dict(record_id=declaration.record_id,raw_sha256=declaration.raw_sha256)])
        for old in self.snapshot_results:
            self.assertEqual(read_snapshot(self.root/'snapshots',old['snapshot_id'],self.writer.committed),old)

    def test_declaration_after_allocation_applies_only_to_next_cutoff(self):
        self.fixture();old_allocation=self.allocate(self.plan)
        plan=self.replacement();self.declare(plan);self.native(plan)
        legacy=self.close_allocated(old_allocation,self.plan)
        self.assertNotIn('owner_plan_dependencies',legacy['descriptor'])
        current=self.close_allocated(self.allocate(plan),plan)
        self.assertEqual(strict_json(current['payload'])['books'][0]['target']['owner_epoch'],E1)

    def test_same_socket_cutover_waits_for_open_cutoff_and_needs_fresh_snapshot(self):
        self.fixture();old_allocation=self.allocate(self.plan);plan=self.replacement(connection='ws',start=1)
        before=self.writer.state.records
        with self.assertRaisesRegex(ValueError,'await cutoff resolution'):self.declare(plan)
        self.assertEqual(self.writer.state.records,before)
        self.close_allocated(old_allocation,self.plan);self.declare(plan)
        self.native(plan,1,snapshot=False)
        invalid=self.close_allocated(self.allocate(plan),plan)
        self.assertEqual(strict_json(invalid['payload'])['books'][0]['validity_as_published'],'invalid')
        self.native(plan,2)
        valid=self.close_allocated(self.allocate(plan),plan)
        self.assertEqual(strict_json(valid['payload'])['books'][0]['validity_as_published'],'valid')

    def test_unused_declaration_burns_owner_epoch_and_retry_keeps_first_identity(self):
        self.fixture();plan=self.replacement();first=self.declare(plan);before=self.writer.state.records
        self.assertEqual(self.declare(plan),first);self.assertEqual(self.writer.state.records,before)
        second=self.replacement(E2);self.declare(second)
        with self.assertRaisesRegex(ValueError,'owner epoch reused'):self.declare(plan)
        self.assertEqual(self.registry().plans[-1].plan,second)

    def test_changed_native_fields_or_metadata_attributes_require_new_epoch(self):
        self.fixture();plan=self.replacement();self.declare(plan)
        with self.assertRaisesRegex(ValueError,'owner epoch reused'):
            self.declare(replace(plan,bindings=(replace(plan.bindings[0],signed_prices=True),)))
        version=self.metadata(dict(base_currency='BTC',amount_unit='changed',quote_currency='native'))
        changed=replace(plan,bindings=(replace(plan.bindings[0],metadata_version_id=version.version_id),))
        with self.assertRaisesRegex(ValueError,'owner epoch reused'):self.declare(changed)
        self.declare(replace(changed,bindings=(replace(changed.bindings[0],target=replace(changed.bindings[0].target,owner_epoch=E2)),)))

    def metadata(self,attrs):
        source=next(r for encoded in self.writer.encoded_records(allow_unsealed=True)
            if (r:=RawRecord.from_bytes(encoded)).kind=='metadata_version')
        utc,mono=self.next_stamp()
        receipt=replace(source.receipt,receive_seq=1,receive_utc_ns=utc,monotonic_ns=mono)
        version=MetadataVersion(source.instrument,receipt,utc,canonical(attrs))
        self.ingress_owner.append(replace(source,receipt=receipt,body=canonical(dict(schema='dc.metadata/2',
            effective_at_ns=version.effective_at_ns,attributes=attrs,supersedes=version.supersedes)),native_fields_json=b'{}'))
        return version

    def test_same_attributes_metadata_update_retains_native_owner(self):
        self.fixture();plan=self.replacement();self.declare(plan);self.native(plan)
        self.close_allocated(self.allocate(plan),plan)
        version=self.metadata(dict(base_currency='BTC',amount_unit='native',quote_currency='native'))
        updated=replace(plan,bindings=(replace(plan.bindings[0],metadata_version_id=version.version_id),))
        self.declare(updated)
        result=self.close_allocated(self.allocate(updated),updated)
        self.assertEqual(strict_json(result['payload'])['books'][0]['target']['owner_epoch'],E1)
        self.assertEqual(strict_json(result['payload'])['books'][0]['validity_as_published'],'valid')

    def test_exact_source_set_and_durable_cutover_are_enforced(self):
        self.fixture();plan=self.replacement();self.declare(plan);registry=self.registry()
        binding=plan.bindings[0]
        registry.check_next_allocation(plan.dependencies,[binding.ws_source,binding.metadata_source])
        with self.assertRaisesRegex(ValueError,'active owner plan'):
            registry.check_next_allocation(plan.dependencies,[binding.ws_source,binding.metadata_source,self.ws])
        bad=self.replacement(E2,connection='ws',start=0)
        with self.assertRaisesRegex(ValueError,'next durable receipt'):self.declare(bad)

    def test_lost_declaration_ack_and_limits_never_duplicate_owner_controls(self):
        self.fixture();plan=self.replacement();append=self.writer.append
        def lost(record):
            append(record)
            if record.channel=='owner_plans:capture':raise OSError('durable declaration ack lost')
        with patch.object(self.writer,'append',side_effect=lost):
            with self.assertRaises(OSError):self.declare(plan)
        before=self.writer.state.records;self.declare(plan);self.assertEqual(self.writer.state.records,before)
        with self.assertRaisesRegex(ValueError,'bound'):self.declare(self.replacement(E2),max_plans=1)
        self.assertEqual(self.writer.state.records,before)

    def test_restart_requires_current_process_owner_and_fresh_metadata(self):
        self.fixture();plan=self.replacement();self.declare(plan);self.writer.finish()
        prior=lambda:(r for p in self.writer.committed for r in iter_segment_records(p))
        with Journal(self.root/'restart.wal',venue='deribit',environment='production',create=True) as wal:
            registry=OwnerPlans(wal,capture_scope='capture',capture_id='capture',process_session_id='restart',
                provenance=self.clock.provenance,prior_records=prior)
            with self.assertRaisesRegex(ValueError,'current process'):registry.declare(plan,utc_ns=4*C,monotonic_ns=1,clock_epoch='restart')
            with self.assertRaisesRegex(ValueError,'current process'):
                registry.check_next_allocation(plan.dependencies,[plan.bindings[0].ws_source,plan.bindings[0].metadata_source])


class OwnerLaneTests(unittest.IsolatedAsyncioTestCase):
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

    async def test_fifo_cutover_after_pending_raw_and_cancelled_client_still_publishes(self):
        lane=await self.ready();binding=self.plan.bindings[0]
        plan=replace(self.plan,bindings=(replace(binding,target=replace(binding.target,owner_epoch=E1),ws_start_seq=2),))
        raw=lane.submit_received(self.received(lane,1))
        utc,mono,_=self.observe()
        declaration=lane.submit_owner_plan(plan,utc_ns=utc,monotonic_ns=mono)
        declaration.cancel();await raw
        async def drained():
            while lane.queue.health()['bytes']:await asyncio.sleep(0)
        await asyncio.wait_for(drained(),5)
        ack=await lane.submit_owner_plan(plan,utc_ns=utc,monotonic_ns=mono)
        self.assertTrue(ack.timing_valid);self.assertEqual(ack.declaration.effective_cutoff_id,0)
        self.assertEqual(ack.declaration.plan,plan)
        body=canonical(dict(jsonrpc='2.0',method='subscription',params=dict(channel=binding.native_channel,
            data=dict(type='snapshot',timestamp=2,instrument_name=binding.target.instrument.native_symbol,
                change_id=20,bids=[['new',100,9]],asks=[['new',101,3]]))))
        await lane.submit_received(replace(self.received(lane,2),channel=binding.native_channel,body=body))
        self.plan=plan;pipeline=self.pipeline(lane);state=await pipeline.run()
        self.assertEqual(state['published'],1);await self.finish_raw(lane)
        records=self.records(lane)
        self.assertEqual(len([r for r in records if r.channel=='owner_plans:capture']),1)
        report=next(strict_json(r.body) for r in records if r.channel=='snapshot_deliveries:capture')
        result=read_snapshot(pipeline.publisher.root/'snapshots',report['snapshot_id'],lane.segments)
        self.assertEqual(strict_json(result['payload'])['books'][0]['book']['bids'],[['100','9']])
        self.assertTrue(result['descriptor']['owner_plan_dependencies'])


class ManagedBufferedTests(unittest.TestCase):
    def replay(self,foreign_seed=False):
        with tempfile.TemporaryDirectory() as directory:
            root=Path(directory)
            inst=key(venue='bybit',native_product='linear',native_symbol='BTCUSDT')
            other=replace(inst,native_symbol='ETHUSDT')
            ws=StreamKey('capture','process','ws');meta=replace(ws,connection_id='meta');seed=replace(ws,connection_id='seed')
            mr=Receipt('capture','process','meta',0,1,1,'epoch')
            attrs=dict(base_currency='BTC',amount_unit='native',quote_currency='native')
            mv=MetadataVersion(inst,mr,0,canonical(attrs))
            binding=BookBinding(FeedTarget(inst,'bybit_full','dc-owner/1:'+'1'*32),ws,meta,mv.version_id,seed_source=seed)
            plan=SnapshotPlan((binding,),'b'*64);p=Provenance('a'*64,'b'*64,plan.dependencies['universe'])
            with Journal(root/'active.wal',venue='bybit',environment='production',create=True) as wal:
                ingress=IngressWriter(wal,'capture','process')
                def raw(instrument,source,seq,mono,payload,topic,kind='ws_message',fields=None):
                    return ingress.append(RawRecord(Receipt('capture','process',source.connection_id,seq,mono,mono,'epoch'),
                        'bybit','production','linear',topic,kind,canonical(payload),p,instrument,canonical(fields or {})))
                raw(inst,meta,0,1,dict(schema='dc.metadata/2',effective_at_ns=0,attributes=attrs,supersedes=None),'metadata','metadata_version')
                owners=OwnerPlans(wal,capture_scope='capture',capture_id='capture',process_session_id='process',provenance=p)
                owners.declare(plan,utc_ns=2,monotonic_ns=2,clock_epoch='epoch')
                coordinator=CutoffCoordinator(wal,capture_scope='capture',capture_id='capture',process_session_id='process',provenance=p)
                ledger=DurableLedger(wal,ledger_id='ledger',capture_scope='capture',capture_id='capture',process_session_id='process',provenance=p)
                ledger.initialize(utc_ns=3,monotonic_ns=3,clock_epoch='epoch')
                streams=[ws,seed,meta];anchor=ClockAnchor('process','epoch',0,0,0,2)
                owners.check_next_allocation(plan.dependencies,streams)
                coordinator.allocate(180_000_000_000,streams,plan.dependencies,anchor=anchor,observed_utc_ns=4,monotonic_ns=4,clock_epoch='epoch')
                raw(other,ws,0,99_999_999_999 if foreign_seed else 10,dict(topic='orderbook.full.ETHUSDT',type='delta',ts=1,data=dict(s='ETHUSDT',u=10,seq=100,b=[],a=[])),'orderbook.full.ETHUSDT')
                now=100_000_000_000
                raw(inst,ws,1,now,dict(topic='orderbook.full.BTCUSDT',type='delta',ts=1,data=dict(s='BTCUSDT',u=10,seq=100,b=[],a=[])),'orderbook.full.BTCUSDT')
                raw(inst,seed,0,now+1,dict(retCode=0,result=dict(s='BTCUSDT',u=10,seq=100,b=[['1','2']],a=[])), '/v5/market/full_orderbook','rest_response',dict(request_category='linear',request_symbol='BTCUSDT'))
                if foreign_seed:
                    raw(other,seed,1,now+2,dict(retCode=0,result=dict(s='ETHUSDT',u=10,seq=100,b=[['1','2']],a=[])),
                        '/v5/market/full_orderbook','rest_response',dict(request_category='linear',request_symbol='ETHUSDT'))
                barrier=ingress.barrier(anchor,180_000_000_000,now_monotonic_ns=180_000_000_000,healthy_streams=streams)
                coordinator.close(0,barrier,ledger_pin=ledger.pin(),utc_ns=180_000_000_000,monotonic_ns=180_000_000_000,clock_epoch='epoch')
                close=coordinator.resolutions[0]['record_id']
                segment=seal_segment(wal,root/'archive',SegmentContext('bybit','production','capture','process','segment','a'*64,'b'*64))
            result=materialize_snapshot([segment],close,plan)
            return strict_json(result['payload'])['books'][0]

    def test_other_symbol_does_not_start_managed_bootstrap_clock(self):
        book=self.replay()
        self.assertEqual(book['validity_as_published'],'valid')
        self.assertEqual(book['book']['bids'],[['1','2']])
        self.assertEqual(book['replay_diagnostics']['count'],0)

    def test_foreign_seed_does_not_clear_valid_managed_book(self):
        book=self.replay(foreign_seed=True)
        self.assertEqual(book['validity_as_published'],'valid')
        self.assertEqual(book['book']['bids'],[['1','2']])
        self.assertEqual(book['replay_diagnostics']['count'],0)
