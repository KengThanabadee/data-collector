from dataclasses import replace
import unittest
from unittest.mock import patch

from data_collector.contracts import RawRecord,Receipt,canonical,digest,strict_json
from data_collector.decoders import deribit_session_end
from data_collector.discoveries import pending_native_discoveries
from data_collector.exports import export_snapshots
from data_collector.gapmap import map_deribit_end,map_native_gap
from data_collector.gaprecovery import map_native_recovery
from data_collector.journal import Journal
from data_collector.ledger import DurableLedger
from data_collector.cutoffs import CutoffCoordinator
from data_collector.snapshots import read_snapshot
from data_collector.validity import GapCorrection
from tests import test_dc_discoveries as discoveries
from tests import test_dc_owners as owners
from tests import test_dc_deribit as receiver_fixtures


class DeribitEndTests(unittest.TestCase):
    setUp=discoveries.DiscoveryTests.setUp
    fixture=discoveries.DiscoveryTests.fixture
    registry=discoveries.DiscoveryTests.registry
    replacement=discoveries.DiscoveryTests.replacement
    next_stamp=discoveries.DiscoveryTests.next_stamp
    declare=discoveries.DiscoveryTests.declare
    native=discoveries.DiscoveryTests.native
    allocate=discoveries.DiscoveryTests.allocate
    close_allocated=discoveries.DiscoveryTests.close_allocated
    open_ledger=discoveries.DiscoveryTests.open_ledger
    repair=discoveries.DiscoveryTests.repair

    def start(self,*,anchor=True):
        self.fixture();self.managed=self.replacement();self.declare(self.managed)
        if anchor:self.anchor=self.native(self.managed)
        self.original=self.close_allocated(self.allocate(self.managed),self.managed)

    def end(self,*,socket_type=None,channel='deribit_session',connection=None):
        binding=self.managed.bindings[0];source=binding.ws_source
        if connection is not None:source=replace(source,connection_id=connection)
        seq=max((RawRecord.from_bytes(e).receipt.receive_seq for e in self.writer.encoded_records(allow_unsealed=True)
                 if RawRecord.from_bytes(e).receipt.stream_key==source),default=-1)+1
        utc,mono=self.next_stamp()
        body=(dict(event='socket_event',type=socket_type) if socket_type is not None else
              dict(schema='dc.deribit-session/2',event='stopped',reason='requested_stop',frames=1,bytes=100,corrections=0))
        return self.ingress_owner.append(replace(self.last_native,
            receipt=Receipt('capture','process',source.connection_id,seq,utc,mono,'process:clock:0'),
            instrument=None,kind='control',channel=channel,body=canonical(body),native_fields_json=b'{}'))

    def test_end_corrects_quiet_cutoff_and_preserves_original_bytes(self):
        self.start();end=self.end();pending=pending_native_discoveries(self.open_ledger())
        self.assertEqual([(p.kind,p.record_id) for p in pending],[('deribit_end',end.record_id)])
        with self.assertRaisesRegex(ValueError,'unmapped native discovery'):self.open_ledger().pin()
        pin=self.repair();before=self.writer.state.records;self.assertEqual(self.repair(),pin)
        self.assertEqual(self.writer.state.records,before)
        audit=next(RawRecord.from_bytes(e) for e in self.writer.encoded_records(allow_unsealed=True)
            if RawRecord.from_bytes(e).channel=='gap_mapping:capture')
        body=strict_json(audit.body)
        self.assertEqual(body['transport_policy'],'deribit-session-end/1')
        self.assertEqual(body['discovery_kind'],'deribit_end')
        self.assertEqual(body['native_proof_record_id'],self.anchor.record_id)
        self.assertEqual(body['first_cutoff_id'],3)
        self.writer.rotate()
        self.assertEqual(read_snapshot(self.root/'managed',self.original['snapshot_id'],self.writer.committed),self.original)
        exported=export_snapshots(self.root/'managed',self.root/'exports',[self.original['snapshot_id']],self.writer.committed)
        self.assertEqual(exported['descriptor']['population']['invalid'],1)

    def test_first_socket_end_wins_and_post_end_snapshot_cannot_heal(self):
        self.start();end=self.end(socket_type=257);self.end()
        self.assertEqual([p.record_id for p in pending_native_discoveries(self.open_ledger())],[end.record_id])
        pin=self.repair();self.ledger_owner=self.open_ledger()
        self.native(self.managed,3)
        self.assertEqual(pending_native_discoveries(self.open_ledger()),())
        result=self.close_allocated(self.allocate(self.managed),self.managed)
        book=strict_json(result['payload'])['books'][0]
        self.assertEqual(book['book']['state'],'invalid')
        self.assertEqual(book['validity_as_published'],'invalid')
        self.assertEqual(book['replay_diagnostics']['first_record_id'],end.record_id)
        self.assertEqual(len([e for e in self.open_ledger().entries(pin) if isinstance(e.payload,GapCorrection)]),1)
        negative=list(self.open_ledger().entries(pin))[-1].payload;utc,mono=self.next_stamp()
        self.assertIsNone(map_native_recovery(self.open_ledger(),self.owner,self.writer.committed,self.managed,0,
            negative.correction_id,utc_ns=utc,monotonic_ns=mono,clock_epoch='process:clock:0'))

    def test_stop_before_anchor_widens_to_unknown_from_zero(self):
        self.start(anchor=False);end=self.end();pin=self.repair()
        negative=list(self.open_ledger().entries(pin))[-1].payload
        self.assertEqual(negative.evidence_record_id,end.record_id)
        self.assertEqual((negative.first_cutoff_id,negative.status),(0,'unknown'))

    def test_native_gap_and_transport_end_remain_independent_open_causes(self):
        self.start();gap=self.native(self.managed,1,snapshot=False);end=self.end()
        self.assertEqual([(p.kind,p.record_id) for p in pending_native_discoveries(self.open_ledger())],
            [('native',gap.record_id),('deribit_end',end.record_id)])
        pin=self.repair();negatives=[e.payload for e in self.open_ledger().entries(pin) if isinstance(e.payload,GapCorrection)]
        self.assertEqual([n.evidence_record_id for n in negatives],[gap.record_id,end.record_id])

    def test_unrelated_channel_source_and_nonterminal_socket_do_not_end_binding(self):
        self.start();self.end(channel='other_control');self.end(connection='other_source');self.end(socket_type=9)
        self.assertEqual(pending_native_discoveries(self.open_ledger()),())
        self.open_ledger().pin()
        utc,mono=self.next_stamp();record=self.end(channel='other_control')
        with self.assertRaisesRegex(ValueError,'native data discovery|required|does not establish'):
            map_deribit_end(self.open_ledger(),self.owner,self.managed,0,record.record_id,
                utc_ns=utc,monotonic_ns=mono,clock_epoch='process:clock:0')

    def test_audit_ack_loss_completes_exact_correction(self):
        self.start();self.end();append=self.writer.append
        def lost(record):
            append(record)
            if record.channel=='gap_mapping:capture':raise OSError('lost transport audit acknowledgement')
        with patch.object(self.writer,'append',side_effect=lost):
            with self.assertRaises(OSError):self.repair()
        ledger=self.open_ledger();tip,pending=ledger.recovery_state();self.assertEqual(len(pending),1)
        with self.assertRaisesRegex(ValueError,'unresolved native mapping audit'):ledger.pin()
        utc,mono=self.next_stamp();ledger=self.open_ledger()
        ledger.complete_native_mappings(utc_ns=utc,monotonic_ns=mono,clock_epoch='process:clock:0')
        self.assertEqual(ledger.pin().sequence,tip.sequence+1)
        self.assertEqual(pending_native_discoveries(ledger),())

    def test_terminated_source_cannot_get_fresh_owner_epoch(self):
        self.start();self.end();self.repair()
        plan=self.replacement(owners.E2,connection=self.managed.bindings[0].ws_source.connection_id,start=2)
        before=self.writer.state.records
        with self.assertRaisesRegex(ValueError,'fresh connection identity'):self.declare(plan)
        self.assertEqual(self.writer.state.records,before)
        self.declare(self.replacement(owners.E2,connection='fresh-socket'))

    def test_native_discovery_after_end_cannot_be_mapped_as_recovery(self):
        self.start();self.end();self.repair();gap=self.native(self.managed,2,snapshot=False)
        utc,mono=self.next_stamp()
        with self.assertRaisesRegex(ValueError,'closed Deribit source'):
            map_native_gap(self.open_ledger(),self.owner,self.managed,0,gap.record_id,
                utc_ns=utc,monotonic_ns=mono,clock_epoch='process:clock:0')

    def test_legacy_native_audit_without_transport_policy_retains_exact_retry(self):
        self.start();gap=self.native(self.managed,1,snapshot=False);self.repair()
        with Journal(self.root/'legacy.wal',venue='deribit',environment='production',create=True) as writer:
            expected=None
            old_refs={RawRecord.from_bytes(e).record_id for e in self.writer.encoded_records(allow_unsealed=True)
                if RawRecord.from_bytes(e).channel=='cutoffs:capture' or
                RawRecord.from_bytes(e).receipt.stream_key==self.managed.bindings[0].ws_source}
            for data in self.writer.encoded_records(allow_unsealed=True):
                record=RawRecord.from_bytes(data)
                if record.channel=='gap_mapping:capture':
                    body=strict_json(record.body);body.pop('transport_policy')
                    body['raw_dependencies']=[row for row in body['raw_dependencies'] if row['record_id'] in old_refs]
                    record=replace(record,body=canonical(body));expected=record.to_bytes()
                writer.append(record)
            args=dict(capture_scope='capture',capture_id='capture',process_session_id='process',provenance=self.clock.provenance)
            ledger=DurableLedger(writer,ledger_id='ledger',**args);coordinator=CutoffCoordinator(writer,**args)
            before=tuple(writer.encoded_records(allow_unsealed=True));utc,mono=self.next_stamp()
            mapped=map_native_gap(ledger,coordinator,self.managed,0,gap.record_id,
                utc_ns=utc,monotonic_ns=mono,clock_epoch='process:clock:0')
            self.assertEqual(mapped.mapping_sha256,digest(expected))
            self.assertEqual(tuple(writer.encoded_records(allow_unsealed=True)),before)

    def test_owner_declaration_omission_cannot_restore_unchanged_terminal_pin(self):
        self.start();self.end();pin=self.repair()
        with Journal(self.root/'missing-owner.wal',venue='deribit',environment='production',create=True) as writer:
            for data in self.writer.encoded_records(allow_unsealed=True):
                record=RawRecord.from_bytes(data)
                if record.channel!='owner_plans:capture':writer.append(record)
            with self.assertRaisesRegex(ValueError,'native mapping raw dependency missing'):
                DurableLedger(writer,ledger_id='ledger',capture_scope='capture',capture_id='capture',
                    process_session_id='process',provenance=self.clock.provenance,expected_pin=pin)

    def test_unmanaged_terminal_mapping_is_rejected_before_writing(self):
        self.fixture();self.managed=self.plan;end=self.end();before=self.writer.state.records
        utc,mono=self.next_stamp()
        with self.assertRaisesRegex(ValueError,'durable managed owner plan required'):
            map_deribit_end(self.open_ledger(),self.owner,self.plan,0,end.record_id,
                utc_ns=utc,monotonic_ns=mono,clock_epoch='process:clock:0')
        self.assertEqual(self.writer.state.records,before)

    def test_invalid_transport_policy_and_selected_control_identity_fail_closed(self):
        self.start();end=self.end()
        with self.assertRaisesRegex(ValueError,'one instrument'):
            deribit_session_end(replace(end,instrument=self.managed.bindings[0].target.instrument))
        self.repair()
        with Journal(self.root/'bad-policy.wal',venue='deribit',environment='production',create=True) as writer:
            for data in self.writer.encoded_records(allow_unsealed=True):
                record=RawRecord.from_bytes(data)
                if record.channel=='gap_mapping:capture':
                    body=strict_json(record.body);body['transport_policy']='unknown/99'
                    record=replace(record,body=canonical(body))
                writer.append(record)
            with self.assertRaisesRegex(ValueError,'unknown Deribit transport replay policy'):
                DurableLedger(writer,ledger_id='ledger',capture_scope='capture',capture_id='capture',
                    process_session_id='process',provenance=self.clock.provenance)


class DeribitEndRecoveryTests(discoveries.DiscoveryRecoveryTests):
    # Reuse the same crash/startup fixtures and assertions with a connection end
    # in place of its unaudited native sequence error. Inherited cases exercise
    # omitted/changed prefix, partial retry and preallocation/no-genesis repair.
    def start(self):
        self.fixture();self.managed=self.replacement();self.declare(self.managed)
        self.anchor=self.native(self.managed)
        self.original=self.close_allocated(self.allocate(self.managed),self.managed)
        self.gap=DeribitEndTests.end(self)
        return self.writer


class DeribitEndReceiverTests(unittest.IsolatedAsyncioTestCase):
    setUp=receiver_fixtures.DeribitTests.setUp
    observe=receiver_fixtures.DeribitTests.observe
    lane=receiver_fixtures.DeribitTests.lane
    start=receiver_fixtures.DeribitTests.start
    record=receiver_fixtures.DeribitTests.record
    received=receiver_fixtures.DeribitTests.received
    clock_request=receiver_fixtures.DeribitTests.clock_request
    clock_policy=receiver_fixtures.DeribitTests.clock_policy
    ready_cutoffs=receiver_fixtures.DeribitTests.ready_cutoffs
    ready=receiver_fixtures.DeribitTests.ready
    measured_wait=receiver_fixtures.DeribitTests.measured_wait
    records=receiver_fixtures.DeribitTests.records
    controls=receiver_fixtures.DeribitTests.controls
    schedule=receiver_fixtures.DeribitTests.schedule
    pipeline=receiver_fixtures.DeribitTests.pipeline
    finish_raw=receiver_fixtures.DeribitTests.finish_raw
    receiver=receiver_fixtures.DeribitTests.receiver
    frame=receiver_fixtures.DeribitTests.frame
    launch=receiver_fixtures.DeribitTests.launch
    until=receiver_fixtures.DeribitTests.until

    async def test_requested_stop_maps_end_before_successful_return(self):
        lane=await self.ready();receiver,_=self.receiver(lane);task=self.launch(receiver)
        self.socket.put(self.frame(receiver))
        await self.until(lambda:receiver._acked and receiver.models[0].state=='live')
        receiver.stop();state=await task
        self.assertEqual(state['connection_end_corrections'],1)
        lane._cutoffs.ledger.pin();await self.finish_raw(lane)
        audit=next(strict_json(r.body) for r in self.records(lane) if r.channel=='gap_mapping:capture')
        self.assertEqual(audit['discovery_kind'],'deribit_end')

    async def test_cancelled_receiver_drains_end_correction_before_raising(self):
        import asyncio
        lane=await self.ready();receiver,_=self.receiver(lane);task=self.launch(receiver)
        self.socket.put(self.frame(receiver))
        await self.until(lambda:receiver._acked and receiver.models[0].state=='live')
        task.cancel()
        with self.assertRaises(asyncio.CancelledError):await task
        self.assertEqual(len(receiver.ends),1);lane._cutoffs.ledger.pin()
        await self.finish_raw(lane)


class DeribitEndLaneTests(discoveries.DiscoveryLaneTests):
    async def test_cutoff_worker_maps_raw_gap_before_delayed_adapter_callback(self):
        lane=await self.ready();plan=self.replacement();discovery=[]
        async def wait(deadline):
            await self.measured_wait(deadline)
            binding=plan.bindings[0];raw=self.received(lane,1)
            body=canonical(dict(schema='dc.deribit-session/2',event='stopped',reason='idle_timeout',
                frames=1,bytes=100,corrections=0))
            ack=await lane.submit_received(replace(raw,receipt=replace(raw.receipt,connection_id=binding.ws_source.connection_id),
                kind='control',channel='deribit_session',instrument=None,body=body))
            discovery.append(ack.record_id)
        pipeline=self.pipeline(lane,wait_until=wait,healthy_streams_for_plan=lambda p:(p.bindings[0].ws_source,self.meta))
        await pipeline.schedule.replace_plan(plan);await self.native(lane,plan)
        state=await pipeline.run();self.assertEqual(state['published'],1);self.assertTrue(lane.health()['accepting'])
        ledger=lane._cutoffs.ledger;negative=list(ledger.entries(ledger.pin()))[-1].payload
        self.assertEqual(negative.evidence_record_id,discovery[0])
        await self.finish_raw(lane)
        report=next(strict_json(r.body) for r in self.records(lane) if r.channel=='snapshot_deliveries:capture')
        result=read_snapshot(pipeline.publisher.root/'snapshots',report['snapshot_id'],lane.segments)
        self.assertEqual(strict_json(result['payload'])['books'][0]['validity_as_published'],'invalid')
