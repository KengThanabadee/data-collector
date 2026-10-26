"""Crash-to-startup tests: durable negatives finish before fresh live capture."""
from contextlib import redirect_stderr
from dataclasses import replace
from functools import partial
import io
import unittest
from unittest.mock import patch

from data_collector.contracts import RawRecord,canonical,strict_json
from data_collector.lane import DurabilityUnknown
from data_collector.ledger import DurableLedger
from data_collector.recovery import iter_recovery_prefix,reconcile_cutoffs,verify_recovery_marker
from data_collector.validity import LedgerEntry
from tests import test_dc_gapmap as gaps
from tests import test_dc_preallocation as preallocation
from tests import test_dc_publisher as publisher
from tests import test_dc_startup as startup

C=publisher.C


class MappingRecoveryTests(unittest.IsolatedAsyncioTestCase):
    setUp=publisher.PublisherTests.setUp
    fixture=gaps.GapMappingTests.fixture
    discovery=gaps.GapMappingTests.discovery
    mapped=gaps.GapMappingTests.mapped
    store=startup.StartupTests.store
    prepare=startup.StartupTests.prepare
    recover=preallocation.PreallocationTests.recover
    records=preallocation.PreallocationTests.records
    new_observe=startup.StartupTests.new_observe
    sample=startup.StartupTests.sample
    seed=startup.StartupTests.seed
    publish_new=startup.StartupTests.publish_new

    def interrupted(self,*,two=False,omit_discovery=False):
        writer=self.fixture();proof=self.discovery();append=writer.append
        def lost(record):
            append(record)
            if record.channel=='gap_mapping:capture':raise OSError('lost audit fsync ack')
        with patch.object(writer,'append',side_effect=lost):
            with self.assertRaises(OSError):self.mapped(proof)
        audit=next(RawRecord.from_bytes(e) for e in writer.encoded_records(allow_unsealed=True)
                   if RawRecord.from_bytes(e).channel=='gap_mapping:capture')
        if two:
            body={**strict_json(audit.body),'request_id':'f'*64}
            writer.append(replace(audit,receipt=replace(audit.receipt,receive_seq=1),body=canonical(body)))
        source=list(writer.encoded_records(allow_unsealed=True));self.original_snapshots=[(s['snapshot_id'],s['payload']) for s in self.snapshot_results]
        store=self.store(True);prepared=self.prepare(store)
        for encoded in source:
            record=RawRecord.from_bytes(encoded)
            if omit_discovery and record.record_id==proof.record_id:continue
            prepared.writer.append(record)
        prepared.writer.abort();store.close()
        store=self.store()
        self.next_label=4*C
        return store,audit

    def lane(self,store,request=None):
        lane=preallocation.PreallocationTests.lane(self,store,request)
        self.new_utc=4*C-100;self.next_label=4*C
        return lane

    def prior(self,request,process='restart'):
        return lambda:iter_recovery_prefix(request.segments,venue='deribit',environment='production',
            capture_id='capture',process_session_id=process)

    def restored_ledger(self,writer,request,process='restart'):
        return DurableLedger(writer,ledger_id='ledger',capture_scope='capture',capture_id='capture',
            process_session_id=process,provenance=self.clock.provenance,prior_records=self.prior(request,process))

    async def test_orphan_audit_is_completed_before_marker_and_new_snapshot_can_publish(self):
        store,audit=self.interrupted();request=store.recover(next_process_session_id='restart')
        # Source tip is a continuity assertion, not permission to certify old rows.
        self.assertEqual(request.ledger_pin.sequence,0)
        lane=self.lane(store,request);before=self.records(lane);self.assertEqual(before,[])
        with self.assertRaisesRegex(ValueError,'await durable cutoff recovery'):lane._require_recovery()
        ack=await self.recover(lane);records=self.records(lane)
        self.assertEqual([r.channel for r in records],['validity:capture','cutoff_recovery:capture'])
        correction=LedgerEntry.from_bytes(records[0].body).payload
        self.assertEqual((correction.proof_record_id,correction.first_cutoff_id),(audit.record_id,1))
        self.assertEqual(ack.ledger_pin.sequence,1)
        self.assertEqual(verify_recovery_marker(request.segments,records[-1],tuple(records)),ack.recovery)
        ledger=lane._cutoffs.ledger
        result=ledger.assess('capture',self.plan.bindings[0].target,2,'valid',ack.ledger_pin)
        self.assertEqual(result['assessed_validity'],'invalid')
        self.assertIn(audit.record_id,{r.record_id for r in ledger.dependencies(ack.ledger_pin)})
        await self.sample(lane,0,20);await self.sample(lane,1,80)
        plan,_=await self.seed(lane);state,_=await self.publish_new(lane,plan)
        self.assertEqual(state['published'],1)
        lane.stop();await lane.test_task
        # Old immutable snapshot payload bytes were never rewritten.
        from data_collector.snapshots import read_snapshot
        paths=self.writer.committed
        for identity,payload in self.original_snapshots:
            self.assertEqual(read_snapshot(self.root/'snapshots',identity,paths)['payload'],payload)

    async def test_multiple_pending_audits_complete_in_order_without_duplicate_on_restart(self):
        store,_=self.interrupted(two=True);lane=self.lane(store)
        ack=await self.recover(lane);records=self.records(lane)
        self.assertEqual([r.channel for r in records],['validity:capture','validity:capture','cutoff_recovery:capture'])
        self.assertEqual([LedgerEntry.from_bytes(r.body).sequence for r in records[:-1]],[1,2])
        self.assertEqual([r.receipt.receive_seq for r in records[:-1]],[0,1])
        self.assertEqual(ack.ledger_pin.sequence,2)
        lane.stop();await lane.test_task;store.close();store=self.store()
        request=store.recover(next_process_session_id='third');prepared=self.prepare(store,'third')
        ledger=self.restored_ledger(prepared.writer,request,'third')
        self.assertEqual(ledger.recovery_state(),(ack.ledger_pin,()))
        self.assertEqual(ledger.complete_native_mappings(utc_ns=5*C,monotonic_ns=5,clock_epoch='third'),ack.ledger_pin)
        self.assertEqual(prepared.writer.state.records,0)

    async def test_lost_correction_ack_and_second_pending_audit_resume_in_next_process(self):
        store,_=self.interrupted(two=True);lane=self.lane(store);append=lane.writer.append
        def lost(record):
            append(record)
            if record.channel=='validity:capture':raise OSError('correction stored; ack lost')
        with patch.object(lane.writer,'append',side_effect=lost),redirect_stderr(io.StringIO()):
            with self.assertRaises(DurabilityUnknown):await self.recover(lane)
            await lane.test_task
        self.assertFalse(lane.health()['clock']['recovery_ready'])
        self.assertIsNone(lane.health()['clock']['recovery_marker_id'])
        store.close();store=self.store();request=store.recover(next_process_session_id='third')
        self.assertEqual(request.ledger_pin.sequence,1)
        prepared=self.prepare(store,'third');ledger=self.restored_ledger(prepared.writer,request,'third')
        self.assertEqual(len(ledger.recovery_state()[1]),1)
        pin=ledger.complete_native_mappings(utc_ns=5*C,monotonic_ns=5,clock_epoch='third')
        self.assertEqual(pin.sequence,2)
        result=reconcile_cutoffs(prepared.writer,request.segments,request.cutoff_pin,capture_scope='capture',
            capture_id='capture',process_session_id='third',provenance=self.clock.provenance,
            utc_ns=5*C,monotonic_ns=5,clock_epoch='third')
        records=tuple(RawRecord.from_bytes(e) for e in prepared.writer.encoded_records(allow_unsealed=True))
        self.assertEqual(len(records),2)
        self.assertEqual(verify_recovery_marker(request.segments,records[-1],records),result)

    async def test_crash_after_repair_before_marker_keeps_negative_on_next_startup(self):
        store,_=self.interrupted();lane=self.lane(store);append=lane.writer.append
        def fail(record):
            if record.channel=='cutoff_recovery:capture':raise OSError('stopped before marker write')
            return append(record)
        with patch.object(lane.writer,'append',side_effect=fail),redirect_stderr(io.StringIO()):
            with self.assertRaises(DurabilityUnknown):await self.recover(lane)
            await lane.test_task
        self.assertFalse(lane.health()['clock']['recovery_ready'])
        store.close();store=self.store();request=store.recover(next_process_session_id='third')
        self.assertEqual(request.ledger_pin.sequence,1)
        prepared=self.prepare(store,'third');ledger=self.restored_ledger(prepared.writer,request,'third')
        self.assertEqual(ledger.recovery_state(),(request.ledger_pin,()))
        result=reconcile_cutoffs(prepared.writer,request.segments,request.cutoff_pin,capture_scope='capture',
            capture_id='capture',process_session_id='third',provenance=self.clock.provenance,
            utc_ns=5*C,monotonic_ns=5,clock_epoch='third')
        records=tuple(RawRecord.from_bytes(e) for e in prepared.writer.encoded_records(allow_unsealed=True))
        self.assertEqual([r.channel for r in records],['cutoff_recovery:capture'])
        self.assertEqual(verify_recovery_marker(request.segments,records[-1],records),result)

    async def test_missing_dependency_rejected_before_new_session_registration(self):
        store,_=self.interrupted(omit_discovery=True)
        catalog=(store.root/'registry.wal').read_bytes()
        with self.assertRaisesRegex(ValueError,'raw dependency missing'):
            self.prepare(store,'restart')
        self.assertEqual((store.root/'registry.wal').read_bytes(),catalog)

    async def test_marker_cannot_bypass_or_alter_pending_correction(self):
        store,_=self.interrupted();request=store.recover(next_process_session_id='restart')
        lane=self.lane(store,request)
        args=dict(capture_scope='capture',capture_id='capture',process_session_id='restart',provenance=lane.provenance,
            utc_ns=self.new_utc,monotonic_ns=10,clock_epoch='restart-process-clock')
        with self.assertRaisesRegex(ValueError,'exactly the pending'):
            await lane._disk(partial(reconcile_cutoffs,lane.writer,request.segments,request.cutoff_pin,**args))
        self.assertEqual(lane.writer.state.records,0)
        ack=await self.recover(lane);records=tuple(self.records(lane));marker=records[-1]
        entry=LedgerEntry.from_bytes(records[0].body)
        bad=replace(records[0],body=replace(entry,payload=replace(entry.payload,first_cutoff_id=2)).to_bytes())
        with self.assertRaisesRegex(ValueError,'exactly the pending'):
            verify_recovery_marker(request.segments,marker,(bad,marker))
        with self.assertRaises(ValueError):verify_recovery_marker(request.segments,marker,(marker,))
        self.assertEqual(verify_recovery_marker(request.segments,marker,records),ack.recovery)
