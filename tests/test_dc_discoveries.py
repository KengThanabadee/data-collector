from dataclasses import replace
import unittest
from unittest.mock import patch

from data_collector.contracts import RawRecord,canonical,strict_json
from data_collector.discoveries import pending_native_discoveries
from data_collector.exports import export_snapshots
from data_collector.gapmap import map_native_gap
from data_collector.ledger import DurableLedger
from data_collector.lane import PENDING_CLOCK_EPOCH
from data_collector.recovery import iter_recovery_prefix,reconcile_cutoffs,verify_recovery_marker
from data_collector.cutoffs import CutoffCoordinator
from tests import test_dc_schedule_cutovers as scheduling
from data_collector.snapshots import read_snapshot
from data_collector.validity import GapCorrection,LedgerEntry
from tests import test_dc_owners as owners
from tests import test_dc_mapping_recovery as recovery


class DiscoveryTests(unittest.TestCase):
    setUp=owners.OwnerTests.setUp
    fixture=owners.OwnerTests.fixture
    registry=owners.OwnerTests.registry
    replacement=owners.OwnerTests.replacement
    next_stamp=owners.OwnerTests.next_stamp
    declare=owners.OwnerTests.declare
    native=owners.OwnerTests.native
    allocate=owners.OwnerTests.allocate
    close_allocated=owners.OwnerTests.close_allocated

    def open_ledger(self):
        return DurableLedger(self.writer,ledger_id='ledger',capture_scope='capture',capture_id='capture',
            process_session_id='process',provenance=self.clock.provenance)

    def start(self):
        self.fixture();self.managed=self.replacement();self.declare(self.managed)
        self.anchor=self.native(self.managed)
        self.original=self.close_allocated(self.allocate(self.managed),self.managed)
        self.gap=self.native(self.managed,1,snapshot=False)
        return self.writer

    def repair(self):
        utc,mono=self.next_stamp()
        return self.open_ledger().complete_native_discoveries(self.owner,utc_ns=utc,monotonic_ns=mono,clock_epoch='process:clock:0')

    def test_unmapped_raw_discovery_blocks_public_certification_and_repairs_exactly(self):
        self.start();ledger=self.open_ledger();source,_=ledger.recovery_state()
        self.assertEqual([d.record_id for d in pending_native_discoveries(ledger)],[self.gap.record_id])
        for operation in (lambda l:l.pin(),lambda l:l.entries(source),lambda l:l.dependencies(source),
                          lambda l:l.assess('capture',self.managed.bindings[0].target,3,'valid',source)):
            with self.assertRaisesRegex(ValueError,'unmapped native discovery'):operation(self.open_ledger())
        pin=self.repair();before=self.writer.state.records
        self.assertEqual(self.repair(),pin);self.assertEqual(self.writer.state.records,before)
        self.writer.rotate()
        self.assertEqual(read_snapshot(self.root/'managed',self.original['snapshot_id'],self.writer.committed),self.original)
        result=export_snapshots(self.root/'managed',self.root/'exports',[self.original['snapshot_id']],self.writer.committed)
        self.assertEqual(result['descriptor']['population']['invalid'],1)
        negative=list(self.open_ledger().entries(pin))[-1].payload
        self.assertIsInstance(negative,GapCorrection);self.assertEqual(negative.evidence_record_id,self.gap.record_id)
        self.assertEqual(negative.first_cutoff_id,3)

    def test_ambiguous_audit_then_exact_negative_completion_does_not_duplicate(self):
        self.start();append=self.writer.append
        def lost(record):
            append(record)
            if record.channel=='gap_mapping:capture':raise OSError('lost audit acknowledgement')
        with patch.object(self.writer,'append',side_effect=lost):
            with self.assertRaises(OSError):self.repair()
        ledger=self.open_ledger();tip,pending=ledger.recovery_state();self.assertEqual(len(pending),1)
        self.assertEqual(pending_native_discoveries(ledger),())
        utc,mono=self.next_stamp();ledger.complete_native_mappings(utc_ns=utc,monotonic_ns=mono,clock_epoch='process:clock:0')
        self.assertEqual(ledger.pin().sequence,tip.sequence+1)
        self.assertEqual(len([RawRecord.from_bytes(e) for e in self.writer.encoded_records(allow_unsealed=True)
            if RawRecord.from_bytes(e).channel=='gap_mapping:capture']),1)

    def test_second_gap_in_same_managed_path_replays_prior_audited_failure(self):
        self.start();self.repair();self.native(self.managed,2)
        second=self.native(self.managed,3,snapshot=False)
        ledger=self.open_ledger();self.assertEqual([d.record_id for d in pending_native_discoveries(ledger)],[second.record_id])
        pin=self.repair();gaps=[e.payload for e in self.open_ledger().entries(pin) if isinstance(e.payload,GapCorrection)]
        self.assertEqual([g.evidence_record_id for g in gaps],[self.gap.record_id,second.record_id])

    def test_same_source_new_epoch_does_not_inherit_retired_model(self):
        self.start();self.repair()
        plan=self.replacement(owners.E2,connection=self.managed.bindings[0].ws_source.connection_id,start=2)
        self.declare(plan);self.native(plan,2,size=6)
        self.assertEqual(pending_native_discoveries(self.open_ledger()),())
        result=self.close_allocated(self.allocate(plan),plan)
        row=strict_json(result['payload'])['books'][0]
        self.assertEqual(row['validity_as_published'],'valid');self.assertEqual(row['book']['bids'],[['100','6']])

    def test_native_projection_bounds_fail_closed(self):
        self.start();ledger=self.open_ledger()
        with self.assertRaisesRegex(ValueError,'aggregate level bound'):
            pending_native_discoveries(ledger,max_levels=1)
        with self.assertRaisesRegex(ValueError,'out of range'):
            pending_native_discoveries(ledger,max_paths=0)


class DiscoveryRecoveryTests(unittest.IsolatedAsyncioTestCase):
    setUp=recovery.MappingRecoveryTests.setUp
    fixture=DiscoveryTests.fixture
    registry=DiscoveryTests.registry
    replacement=DiscoveryTests.replacement
    next_stamp=DiscoveryTests.next_stamp
    declare=DiscoveryTests.declare
    native=DiscoveryTests.native
    allocate=DiscoveryTests.allocate
    close_allocated=DiscoveryTests.close_allocated
    start=DiscoveryTests.start
    store=recovery.MappingRecoveryTests.store
    prepare=recovery.MappingRecoveryTests.prepare
    def lane(self,store,request=None):
        lane=recovery.MappingRecoveryTests.lane(self,store,request)
        self.new_utc=5*owners.C-100;self.next_label=5*owners.C
        return lane
    recover=recovery.MappingRecoveryTests.recover
    records=recovery.MappingRecoveryTests.records
    new_observe=recovery.MappingRecoveryTests.new_observe
    sample=recovery.MappingRecoveryTests.sample
    seed=recovery.MappingRecoveryTests.seed
    publish_new=recovery.MappingRecoveryTests.publish_new

    def interrupted(self,*,preallocation=False):
        self.start();source=tuple(self.writer.encoded_records(allow_unsealed=True))
        if preallocation:
            selected=[]
            for data in source:
                record=RawRecord.from_bytes(data)
                if record.kind=='metadata_version' or record.receipt.stream_key==self.managed.bindings[0].ws_source:
                    selected.append(data)
                elif record.channel=='owner_plans:capture':
                    body=strict_json(record.body);body['effective_cutoff_id']=0
                    selected.append(replace(record,body=canonical(body)).to_bytes())
            source=tuple(selected)
        store=self.store(True);prepared=self.prepare(store)
        for data in source:prepared.writer.append(RawRecord.from_bytes(data))
        prepared.writer.abort();store.close()
        return self.store()

    async def test_startup_maps_raw_without_audit_before_marker_and_fresh_publication(self):
        store=self.interrupted();request=store.recover(next_process_session_id='restart')
        lane=self.lane(store,request);ack=await self.recover(lane);records=tuple(self.records(lane))
        self.assertEqual([r.channel for r in records],['gap_mapping:capture','validity:capture','cutoff_recovery:capture'])
        self.assertEqual(verify_recovery_marker(request.segments,records[-1],records),ack.recovery)
        result=lane._cutoffs.ledger.assess('capture',self.managed.bindings[0].target,3,'valid',ack.ledger_pin)
        self.assertEqual(result['assessed_validity'],'invalid')
        await self.sample(lane,0,20);await self.sample(lane,1,80)
        plan,_=await self.seed(lane)
        binding=plan.bindings[0]
        plan=replace(plan,bindings=(replace(binding,target=replace(binding.target,owner_epoch=owners.E3),ws_start_seq=1),))
        utc,mono,_=self.new_observe()
        await lane.submit_owner_plan(plan,utc_ns=utc,monotonic_ns=mono)
        saved=next(RawRecord.from_bytes(data) for data in lane.writer.encoded_records(allow_unsealed=True)
            if RawRecord.from_bytes(data).kind=='ws_message' and RawRecord.from_bytes(data).receipt.connection_id=='ws')
        body=strict_json(saved.body);body['params']['data']['change_id']=501
        await lane.submit_received(replace(saved,receipt=replace(saved.receipt,receive_seq=1,receive_utc_ns=utc,
            monotonic_ns=mono,clock_epoch=PENDING_CLOCK_EPOCH),body=canonical(body),native_fields_json=b'{}'))
        state,_=await self.publish_new(lane,plan)
        self.assertEqual(state['published'],1)

    async def test_preallocation_without_genesis_requires_negative_repairs_before_marker(self):
        store=self.interrupted(preallocation=True);request=store.recover(next_process_session_id='restart')
        self.assertIsNone(request.ledger_pin);self.assertEqual(request.cutoff_pin.count,0)
        lane=self.lane(store,request);ack=await self.recover(lane);records=tuple(self.records(lane))
        self.assertEqual([r.channel for r in records],['validity:capture','gap_mapping:capture',
            'validity:capture','cutoff_recovery:capture'])
        self.assertEqual(ack.ledger_pin.sequence,1)
        self.assertEqual(verify_recovery_marker(request.segments,records[-1],records),ack.recovery)
        with self.assertRaisesRegex(ValueError,'exactly the pending'):
            verify_recovery_marker(request.segments,records[-1],(records[-1],))

    async def test_marker_cannot_omit_or_change_generated_native_audit(self):
        store=self.interrupted();request=store.recover(next_process_session_id='restart')
        lane=self.lane(store,request);await self.recover(lane);records=tuple(self.records(lane));marker=records[-1]
        with self.assertRaisesRegex(ValueError,'exactly the pending'):
            verify_recovery_marker(request.segments,marker,records[1:])
        body=strict_json(records[0].body);body['first_cutoff_id']=0
        changed=replace(records[0],body=canonical(body))
        with self.assertRaisesRegex(ValueError,'exactly the pending'):
            verify_recovery_marker(request.segments,marker,(changed,)+records[1:])

    async def test_repair_prefix_with_earlier_stamps_verifies_after_later_marker_retry(self):
        store=self.interrupted();request=store.recover(next_process_session_id='restart')
        prepared=self.prepare(store,'restart')
        def prior():return iter_recovery_prefix(request.segments,venue='deribit',environment='production',
            capture_id='capture',process_session_id='restart')
        ledger=DurableLedger(prepared.writer,ledger_id='ledger',capture_scope='capture',capture_id='capture',
            process_session_id='restart',provenance=self.clock.provenance,prior_records=prior)
        coordinator=CutoffCoordinator(prepared.writer,capture_scope='capture',capture_id='capture',
            process_session_id='restart',provenance=self.clock.provenance,prior_records=prior)
        ledger.complete_native_discoveries(coordinator,utc_ns=5*owners.C,monotonic_ns=10,clock_epoch='retry')
        first=tuple(prepared.writer.encoded_records(allow_unsealed=True))
        result=reconcile_cutoffs(prepared.writer,request.segments,request.cutoff_pin,capture_scope='capture',
            capture_id='capture',process_session_id='restart',provenance=self.clock.provenance,
            utc_ns=5*owners.C+20,monotonic_ns=30,clock_epoch='retry')
        records=tuple(RawRecord.from_bytes(data) for data in prepared.writer.encoded_records(allow_unsealed=True))
        self.assertEqual(tuple(r.to_bytes() for r in records[:-1]),first)
        self.assertEqual(verify_recovery_marker(request.segments,records[-1],records),result)


class DiscoveryLaneTests(unittest.IsolatedAsyncioTestCase):
    setUp=scheduling.ScheduleCutoverTests.setUp
    observe=scheduling.ScheduleCutoverTests.observe
    lane=scheduling.ScheduleCutoverTests.lane
    start=scheduling.ScheduleCutoverTests.start
    record=scheduling.ScheduleCutoverTests.record
    received=scheduling.ScheduleCutoverTests.received
    clock_request=scheduling.ScheduleCutoverTests.clock_request
    clock_policy=scheduling.ScheduleCutoverTests.clock_policy
    ready_cutoffs=scheduling.ScheduleCutoverTests.ready_cutoffs
    ready=scheduling.ScheduleCutoverTests.ready
    measured_wait=scheduling.ScheduleCutoverTests.measured_wait
    records=scheduling.ScheduleCutoverTests.records
    controls=scheduling.ScheduleCutoverTests.controls
    schedule=scheduling.ScheduleCutoverTests.schedule
    pipeline=scheduling.ScheduleCutoverTests.pipeline
    finish_raw=scheduling.ScheduleCutoverTests.finish_raw
    replacement=scheduling.ScheduleCutoverTests.replacement
    native=scheduling.ScheduleCutoverTests.native

    async def test_cutoff_worker_maps_raw_gap_before_delayed_adapter_callback(self):
        lane=await self.ready();plan=self.replacement();discovery=[]
        async def wait(deadline):
            await self.measured_wait(deadline)
            binding=plan.bindings[0];raw=self.received(lane,1)
            body=canonical(dict(jsonrpc='2.0',method='subscription',params=dict(channel=binding.native_channel,
                data=dict(type='change',timestamp=2,instrument_name=binding.target.instrument.native_symbol,
                    change_id=21,prev_change_id=999,bids=[],asks=[]))))
            ack=await lane.submit_received(replace(raw,receipt=replace(raw.receipt,connection_id=binding.ws_source.connection_id),body=body))
            discovery.append(ack.record_id)
        pipeline=self.pipeline(lane,wait_until=wait,healthy_streams_for_plan=lambda p:(p.bindings[0].ws_source,self.meta))
        await pipeline.schedule.replace_plan(plan);await self.native(lane,plan)
        state=await pipeline.run();self.assertEqual(state['published'],1)
        self.assertTrue(lane.health()['accepting'])
        ledger=lane._cutoffs.ledger
        negative=list(ledger.entries(ledger.pin()))[-1].payload
        self.assertEqual(negative.evidence_record_id,discovery[0])
        await self.finish_raw(lane)
        report=next(strict_json(r.body) for r in self.records(lane) if r.channel=='snapshot_deliveries:capture')
        result=read_snapshot(pipeline.publisher.root/'snapshots',report['snapshot_id'],lane.segments)
        self.assertEqual(strict_json(result['payload'])['books'][0]['validity_as_published'],'invalid')
