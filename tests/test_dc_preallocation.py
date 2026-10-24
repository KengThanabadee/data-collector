import asyncio
from contextlib import redirect_stderr
from dataclasses import replace
from functools import partial
import io
import unittest

from data_collector.clocklog import SOURCE, CONTROL
from data_collector.contracts import RawRecord, canonical, strict_json
from data_collector.cutoffs import CutoffPin
from data_collector.journal import Journal, JournalLimits
from data_collector.lane import ArchiveLane, DurabilityUnknown
from data_collector.ledger import DurableLedger
from data_collector.queues import ByteQueue, QueueLimits
from data_collector.recovery import reconcile_cutoffs, verify_recovery_marker
from data_collector.snapshots import read_snapshot
from data_collector.validity import GapCorrection
from tests import test_dc_startup as fixtures

C=fixtures.C


class PreallocationTests(unittest.IsolatedAsyncioTestCase):
    setUp=fixtures.StartupTests.setUp
    store=fixtures.StartupTests.store
    prepare=fixtures.StartupTests.prepare
    new_observe=fixtures.StartupTests.new_observe
    sample=fixtures.StartupTests.sample
    seed=fixtures.StartupTests.seed
    publish_new=fixtures.StartupTests.publish_new

    def interrupted(self,mode='native'):
        store=self.store(True);prepared=self.prepare(store)
        natives=[]
        for encoded in self.wal.encoded_records(allow_unsealed=True):
            record=RawRecord.from_bytes(encoded)
            if (mode in {'native','ledger','one'} and not record.receipt.connection_id.startswith('control:') and
                    (mode!='one' or not natives)) or (
                    mode=='clock' and record.receipt.connection_id in {SOURCE,CONTROL}):
                prepared.writer.append(record);natives.append(record)
        ledger_pin=None
        if mode=='ledger':
            ledger=DurableLedger(prepared.writer,ledger_id='ledger',capture_scope='capture',capture_id='capture',
                process_session_id='process',provenance=self.clock.provenance)
            ledger.initialize(utc_ns=C-1,monotonic_ns=1100,clock_epoch='process:clock:0')
            proof=natives[-1].record_id
            target=replace(self.plan.bindings[0].target,owner_epoch='restart-owner')
            ledger_pin=ledger.append(GapCorrection('capture',target,0,None,proof,proof,'gap observed before first cutoff'),
                utc_ns=C-1,monotonic_ns=1100,clock_epoch='process:clock:0')
        prepared.writer.abort();store.close()
        if mode=='corrupt_empty':
            with (prepared.writer.directory/'00000000.wal').open('ab') as out:out.write(b'lost first receive frame')
        store=self.store();request=store.recover(next_process_session_id='restart')
        self.assertIsNotNone(request)
        self.assertEqual(request.cutoff_pin,CutoffPin(0,None,None))
        self.assertEqual(request.ledger_pin,ledger_pin)
        return store,request,prepared.writer.directory

    def lane(self,store,request=None):
        self.new_mono=[10];self.new_utc=2*C-100;self.next_label=2*C
        prepared=self.prepare(store,'restart',started_utc_ns=self.new_utc,monotonic_ns=10,
            clock_epoch='restart-process-clock',now_ns=lambda:self.new_mono[0])
        if request is not None:prepared=replace(prepared,recovery=request)
        emergency=Journal(self.root/'restart-emergency.wal',venue='deribit',environment='production',create=True,
            limits=JournalLimits(max_record_bytes=262144))
        self.addCleanup(emergency.close)
        queue=ByteQueue(QueueLimits(max_bytes=262144,max_items=8,max_item_bytes=131072),now_ns=lambda:self.new_mono[0])
        lane=ArchiveLane(prepared.writer,queue,emergency_writer=emergency,observe=self.new_observe,max_connections=4,
            clock_policy=self.clock.policy,cutoff_scope='capture',cutoff_ledger_id='ledger',recovery=prepared.recovery)
        task=asyncio.create_task(lane.run());lane.test_task=task
        async def drain():
            lane.stop();await asyncio.wait_for(asyncio.gather(task,return_exceptions=True),5)
        self.addAsyncCleanup(drain)
        return lane

    def recover(self,lane):return lane.submit_recovery(utc_ns=self.new_utc,monotonic_ns=self.new_mono[0])

    def records(self,lane):
        return [RawRecord.from_bytes(e) for e in lane.writer.encoded_records(allow_unsealed=True)]

    async def test_native_prefix_resume_retains_uncertainty_then_publishes_first_fresh_book(self):
        store,request,_=self.interrupted();lane=self.lane(store)
        with self.assertRaises(ValueError):await self.sample(lane,0,20)
        self.new_mono[0]=10
        ack=await self.recover(lane)
        self.assertEqual((ack.recovery.next_cutoff_id,ack.recovery.next_cutoff_utc_ns,ack.ledger_pin),(0,None,None))
        marker=next(r for r in self.records(lane) if r.channel=='cutoff_recovery:capture')
        self.assertEqual(strict_json(marker.body)['preallocation_interruption'],dict(continuity='not_proven',
            last_closed_cutoff_id=None,first_affected_cutoff_id=None,time_bound='not_inferred'))
        self.assertFalse(any(r.channel=='validity:capture' for r in self.records(lane)))
        before=[(s.path,(s.path/'manifest.json').read_bytes(),(s.path/'records.jsonl.gz').read_bytes()) for s in request.segments]
        self.assertFalse((await self.sample(lane,0,20)).assessment.aligned)
        await self.sample(lane,1,80);plan,_=await self.seed(lane)
        state,publisher=await self.publish_new(lane,plan)
        self.assertEqual((state['published'],state['reported']),(1,1))
        local=self.records(lane)
        ledger=next(r for r in local if r.channel=='validity:capture')
        self.assertEqual((ledger.receipt.process_session_id,ledger.receipt.receive_seq),('restart',0))
        report=strict_json(next(r.body for r in local if r.channel=='snapshot_deliveries:capture'))
        snapshot=read_snapshot(publisher.root/'snapshots',report['snapshot_id'],
            [s.path for s in request.segments]+list(lane.writer.committed))
        payload=strict_json(snapshot['payload'])
        self.assertEqual(snapshot['descriptor']['cutoff_id'],0)
        self.assertEqual(payload['books'][0]['book']['bids'],[['200','2']])
        self.assertEqual(payload['books'][0]['validity_as_published'],'valid')
        for path,manifest,archive in before:
            self.assertEqual((path/'manifest.json').read_bytes(),manifest)
            self.assertEqual((path/'records.jsonl.gz').read_bytes(),archive)

    async def test_preexisting_open_correction_carries_to_first_new_cutoff(self):
        store,request,_=self.interrupted('ledger');lane=self.lane(store)
        ack=await self.recover(lane);self.assertEqual(ack.ledger_pin,request.ledger_pin)
        await self.sample(lane,0,20);await self.sample(lane,1,80)
        plan,_=await self.seed(lane);state,publisher=await self.publish_new(lane,plan)
        self.assertEqual(state['published'],1)
        local=self.records(lane)
        self.assertFalse(any(r.channel=='validity:capture' for r in local))
        report=strict_json(next(r.body for r in local if r.channel=='snapshot_deliveries:capture'))
        snapshot=read_snapshot(publisher.root/'snapshots',report['snapshot_id'],
            [s.path for s in request.segments]+list(lane.writer.committed))
        self.assertEqual(strict_json(snapshot['payload'])['books'][0]['validity_as_published'],'invalid')

    async def test_old_clock_history_still_requires_new_two_sample_alignment(self):
        store,_,_=self.interrupted('clock');lane=self.lane(store)
        await self.recover(lane);self.assertFalse((await self.sample(lane,0,20)).assessment.aligned)
        plan,_=await self.seed(lane);state,_=await self.publish_new(lane,plan)
        self.assertEqual(state['published'],0)
        self.assertEqual(lane._cutoffs.coordinator.resolutions[0]['payload']['reason'],'clock_alignment_unavailable')

    async def test_zero_record_corrupt_tail_remains_pinned_and_does_not_invent_labels(self):
        store,request,directory=self.interrupted('corrupt_empty')
        evidence=list(directory.glob('*.tail-*'));self.assertTrue(evidence)
        saved={p:p.read_bytes() for p in evidence}
        lane=self.lane(store);ack=await self.recover(lane)
        self.assertEqual((ack.recovery.failed_cutoff_ids,ack.recovery.closed_cutoff_ids),((),()))
        await self.sample(lane,0,20);await self.sample(lane,1,80)
        plan,_=await self.seed(lane);state,_=await self.publish_new(lane,plan)
        self.assertEqual(state['published'],1)
        for p,data in saved.items():self.assertEqual(p.read_bytes(),data)
        manifests=[strict_json((s.path/'manifest.json').read_bytes()) for s in request.segments]
        self.assertTrue(any(m['recovery_evidence'] for m in manifests))

    async def test_null_ledger_claim_cannot_hide_a_preexisting_correction_prefix(self):
        store,request,_=self.interrupted('ledger');lane=self.lane(store,replace(request,ledger_pin=None))
        with redirect_stderr(io.StringIO()):
            with self.assertRaises(DurabilityUnknown):await self.recover(lane)
            await lane.test_task
        self.assertFalse(lane.health()['clock']['recovery_ready'])
        self.assertIsNone(lane.health()['clock']['recovery_marker_id'])

    async def test_recovery_marker_is_retryable_and_readonly_verifiable_at_exact_record_bound(self):
        store,request,_=self.interrupted('one');lane=self.lane(store)
        ack=await self.recover(lane);marker=self.records(lane)[0]
        args=dict(capture_scope='capture',capture_id='capture',process_session_id='restart',provenance=lane.provenance,
            utc_ns=self.new_utc,monotonic_ns=10,clock_epoch='restart-process-clock',max_records=1)
        # Both the old raw prefix and target consist of exactly1 record.
        again=await lane._disk(partial(reconcile_cutoffs,lane.writer,request.segments,request.cutoff_pin,**args))
        self.assertEqual(again,ack.recovery)
        self.assertEqual(verify_recovery_marker(request.segments,marker,(marker,),max_records=1),again)
        bad=replace(marker,body=canonical({**strict_json(marker.body),'next_cutoff_utc_ns':C}))
        with self.assertRaises(ValueError):verify_recovery_marker(request.segments,bad,(bad,))

    async def test_two_preallocation_restarts_do_not_reset_or_fabricate_cutoff_labels(self):
        store,_,_=self.interrupted();lane=self.lane(store)
        await self.recover(lane);lane.stop();await lane.test_task;store.close()
        store=self.store();request=store.recover(next_process_session_id='third')
        self.assertEqual((request.cutoff_pin,request.ledger_pin),(CutoffPin(0,None,None),None))
        prepared=self.prepare(store,'third')
        marker=reconcile_cutoffs(prepared.writer,request.segments,request.cutoff_pin,capture_scope='capture',capture_id='capture',
            process_session_id='third',provenance=self.clock.provenance,
            utc_ns=3*C,monotonic_ns=5,clock_epoch='third-process')
        self.assertEqual((marker.next_cutoff_id,marker.next_cutoff_utc_ns),(0,None))
        prepared.writer.abort()

    async def test_nonempty_cutoff_prefix_cannot_configure_a_null_ledger_pin(self):
        store=self.store(True)
        fixtures.StartupTests.captured(self,store)
        request=store.recover(next_process_session_id='restart')
        self.assertGreater(request.cutoff_pin.count,0)
        with self.assertRaisesRegex(ValueError,'pinned cutoff/ledger'):
            self.lane(store,replace(request,ledger_pin=None))
