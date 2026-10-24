import asyncio
from dataclasses import replace
import shutil
import unittest
from unittest.mock import patch

from data_collector.contracts import RawRecord, digest
from data_collector.journal import Journal, JournalLimits
from data_collector.lane import ArchiveLane
from data_collector.queues import ByteQueue, QueueLimits
from data_collector.segments import SegmentLimits, iter_segment_records
from data_collector.startup import CaptureStore, StartupLimits
from tests import test_dc_publisher as publisher_fixtures
from tests import test_dc_resume as resume_fixtures

C=publisher_fixtures.C


class StartupTests(unittest.IsolatedAsyncioTestCase):
    setUp=publisher_fixtures.PublisherTests.setUp
    new_observe=resume_fixtures.ResumeTests.new_observe
    sample=resume_fixtures.ResumeTests.sample
    seed=resume_fixtures.ResumeTests.seed
    publish_new=resume_fixtures.ResumeTests.publish_new

    def store(self,create=False,**kwargs):
        store=CaptureStore(self.root/'store',venue='deribit',environment='production',capture_id='capture',
            capture_scope='capture',ledger_id='ledger',create=create,**kwargs)
        self.addCleanup(store.close)
        return store

    def prepare(self,store,process='process',**kwargs):
        args=dict(process_session_id=process,provenance=self.clock.provenance,started_utc_ns=C-200,
            monotonic_ns=900,clock_epoch='initial',now_ns=lambda:1000,
            limits=JournalLimits(max_record_bytes=262144,target_bytes=64000,max_streams=32),
            segment_limits=SegmentLimits(max_record_bytes=262144,max_uncompressed_bytes=1048576,max_compressed_bytes=1048576),
            max_segments=64)
        args.update(kwargs)
        result=store.prepare_session(**args)
        self.addCleanup(result.writer.abort)
        return result

    def captured(self,store,*,finish=False):
        prepared=self.prepare(store)
        self.assertIsNone(prepared.recovery)
        for encoded in self.wal.encoded_records(allow_unsealed=True):
            prepared.writer.append(RawRecord.from_bytes(encoded))
        if finish:prepared.writer.finish()
        else:prepared.writer.abort()
        return prepared

    async def test_automatic_inventory_selection_runs_new_lane_to_published_snapshot(self):
        store=self.store(True);old=self.captured(store);store.close()
        store=self.store();self.new_mono=[10];self.new_utc=2*C-100;self.next_label=2*C
        prepared=self.prepare(store,'restart',started_utc_ns=self.new_utc,monotonic_ns=10,
            now_ns=lambda:self.new_mono[0],clock_epoch='restart-process-clock')
        self.assertEqual(prepared.recovery.cutoff_pin,self.coordinator.pin)
        self.assertEqual(prepared.recovery.ledger_pin,self.ledger.pin())
        old_refs=prepared.recovery.segments
        self.assertTrue((old.writer.directory/'recovered.json').exists())
        emergency=Journal(self.root/'restart-emergency.wal',venue='deribit',environment='production',create=True,
            limits=JournalLimits(max_record_bytes=262144))
        self.addCleanup(emergency.close)
        queue=ByteQueue(QueueLimits(max_bytes=262144,max_items=8,max_item_bytes=131072),now_ns=lambda:self.new_mono[0])
        lane=ArchiveLane(prepared.writer,queue,emergency_writer=emergency,observe=self.new_observe,max_connections=4,
            clock_policy=self.clock.policy,cutoff_scope='capture',cutoff_ledger_id='ledger',recovery=prepared.recovery)
        task=asyncio.create_task(lane.run())
        async def drain():
            lane.stop();await asyncio.wait_for(asyncio.gather(task,return_exceptions=True),5)
        self.addAsyncCleanup(drain)
        ack=await lane.submit_recovery(utc_ns=self.new_utc,monotonic_ns=10)
        self.assertTrue(ack.timing_valid)
        await self.sample(lane,0,20);await self.sample(lane,1,80)
        plan,_=await self.seed(lane);state,publisher=await self.publish_new(lane,plan)
        self.assertEqual((state['reported'],state['published']),(1,1))
        await drain();self.assertTrue(prepared.writer.closed)
        report=next(r for path in lane.segments for r in iter_segment_records(path) if r.channel=='snapshot_deliveries:capture')
        from data_collector.contracts import strict_json
        from data_collector.snapshots import read_snapshot
        value=strict_json(report.body)
        snapshot=read_snapshot(publisher.root/'snapshots',value['snapshot_id'],[r.path for r in old_refs]+list(lane.segments))
        self.assertEqual(snapshot['descriptor']['cutoff_id'],1)
        store.close();again=self.store()
        request=again.recover(next_process_session_id='third')
        self.assertEqual(request.cutoff_pin.count,4)
        self.assertEqual(request.ledger_pin,prepared.recovery.ledger_pin)
        self.assertEqual(request,again.recover(next_process_session_id='third'))

    async def test_graceful_closed_prefix_and_empty_opened_session_are_recoverable(self):
        store=self.store(True);empty=self.prepare(store);empty.writer.finish();store.close()
        store=self.store();prepared=self.prepare(store,'second');self.assertIsNone(prepared.recovery)
        prepared.writer.abort();store.close()
        # Separate fresh fixture root, exercising complete+recovered evidence equality.
        shutil.rmtree(self.root/'store')
        store=self.store(True);self.captured(store,finish=True);store.close()
        store=self.store();first=store.recover(next_process_session_id='second')
        self.assertEqual(first,store.recover(next_process_session_id='second'))
        self.assertEqual(first.cutoff_pin,self.coordinator.pin)

    async def test_registry_reservation_and_writer_creation_failures_are_never_exposed(self):
        for stage in ('after_registry_reserved','after_writer_create','after_registry_opened'):
            with self.subTest(stage=stage):
                if (self.root/'store').exists():shutil.rmtree(self.root/'store')
                def crash(value):
                    if value==stage:raise OSError('synthetic startup crash')
                store=self.store(True,checkpoint=crash)
                with self.assertRaises(OSError):self.prepare(store)
                self.assertTrue(store.poisoned)
                with self.assertRaises(ValueError):store.recover(next_process_session_id='second')
                store.close();store=self.store()
                prepared=self.prepare(store,'second')
                self.assertIsNone(prepared.recovery)
                self.assertEqual(store.sessions[0]['phase'],'opened' if stage=='after_registry_opened' else 'abandoned')
                prepared.writer.abort();store.close()

    async def test_opened_session_deletion_and_unknown_session_cannot_shrink_history(self):
        store=self.store(True);old=self.captured(store);store.close()
        shutil.rmtree(old.writer.directory)
        store=self.store()
        with self.assertRaisesRegex(ValueError,'opened capture session missing'):store.recover(next_process_session_id='second')
        store.close()
        unknown=self.root/'store'/'sessions'/('f'*64);unknown.mkdir()
        with self.assertRaisesRegex(ValueError,'unregistered capture session'):self.store()

    async def test_unknown_archive_and_symlink_entries_are_rejected_before_recovery(self):
        store=self.store(True);self.captured(store);store.close()
        unknown=self.root/'store'/'archive'/'v2'/'deribit'/'production'/'2000-01-01'/('f'*64)
        unknown.mkdir(parents=True)
        with self.assertRaisesRegex(ValueError,'unregistered archive session'):self.store()
        shutil.rmtree(self.root/'store'/'archive')
        (self.root/'store'/'sessions'/'alias').symlink_to(self.root,target_is_directory=True)
        with self.assertRaisesRegex(ValueError,'symlinks'):self.store()

    async def test_registry_tail_damage_is_not_silently_discarded(self):
        store=self.store(True);self.captured(store);store.close()
        path=self.root/'store'/'registry.wal'
        with path.open('ab') as out:out.write(b'partial registry frame')
        original=path.read_bytes()
        with self.assertRaisesRegex(ValueError,'registry damaged'):self.store()
        self.assertEqual(path.read_bytes(),original)

    async def test_opened_descriptor_and_committed_index_conflicts_are_rejected(self):
        store=self.store(True);old=self.captured(store);store.close();store=self.store()
        request=store.recover(next_process_session_id='second');store.close()
        path=old.writer.directory/'committed'/'00000000.json';original=path.read_bytes()
        path.write_bytes(original.replace(request.segments[0].manifest_sha256.encode(),b'f'*64))
        store=self.store()
        with self.assertRaises(ValueError):store.recover(next_process_session_id='second')
        store.close();path.write_bytes(original)
        descriptor=old.writer.directory/'session.json';descriptor.write_bytes(descriptor.read_bytes()+b' ')
        store=self.store()
        with self.assertRaisesRegex(ValueError,'descriptor changed'):store.recover(next_process_session_id='second')

    async def test_root_lock_active_writer_and_reused_process_guards(self):
        store=self.store(True)
        with self.assertRaises(BlockingIOError):self.store()
        prepared=self.prepare(store)
        with self.assertRaisesRegex(ValueError,'stop current writer'):store.close()
        with self.assertRaisesRegex(ValueError,'stop current writer'):self.prepare(store,'second')
        prepared.writer.abort()
        with self.assertRaisesRegex(ValueError,'new process session'):self.prepare(store)
        store.close()
        with self.assertRaises(ValueError):CaptureStore(self.root/'store',venue='deribit',environment='testnet',capture_id='capture',capture_scope='capture',ledger_id='ledger')

    async def test_resource_bounds_reject_before_recovery_or_registration(self):
        store=self.store(True);self.captured(store);store.close()
        store=self.store(limits=StartupLimits(max_records=1))
        catalog=(self.root/'store'/'registry.wal').read_bytes()
        with self.assertRaisesRegex(ValueError,'scan bound'):self.prepare(store,'second')
        self.assertEqual((self.root/'store'/'registry.wal').read_bytes(),catalog)
        store.close()
        with self.assertRaisesRegex(ValueError,'disk byte scan bound'):self.store(limits=StartupLimits(max_disk_bytes=100))

    async def test_partial_raw_wal_tail_is_preserved_without_fabricating_a_close(self):
        store=self.store(True);old=self.captured(store);store.close()
        wal=old.writer.directory/'00000000.wal'
        with wal.open('ab') as out:out.write(b'partial market frame')
        store=self.store();request=store.recover(next_process_session_id='second')
        self.assertEqual(request.cutoff_pin,self.coordinator.pin)
        self.assertTrue(list(old.writer.directory.glob('*.tail-*')))
        self.assertTrue(list(old.writer.directory.glob('*.recovery-*.json')))
        self.assertEqual(request,store.recover(next_process_session_id='second'))

    async def test_missing_initial_and_rotated_opened_wals_cannot_erase_history(self):
        store=self.store(True);old=self.captured(store);store.close()
        wal=old.writer.directory/'00000000.wal';saved=wal.read_bytes();wal.unlink()
        store=self.store();catalog=(self.root/'store'/'registry.wal').read_bytes()
        with self.assertRaisesRegex(ValueError,'opened WAL dependency missing'):self.prepare(store,'second')
        self.assertEqual((self.root/'store'/'registry.wal').read_bytes(),catalog)
        self.assertFalse((old.writer.directory/'recovered.json').exists())
        store.close();wal.write_bytes(saved)
        # A normal rotation exposes a new WAL whose data has no commit index yet.
        shutil.rmtree(self.root/'store');store=self.store(True);old=self.prepare(store)
        records=list(self.wal.encoded_records(allow_unsealed=True))
        for encoded in records[:-1]:old.writer.append(RawRecord.from_bytes(encoded))
        old.writer.rotate();old.writer.append(RawRecord.from_bytes(records[-1]));old.writer.abort();store.close()
        (old.writer.directory/'00000001.wal').unlink()
        store=self.store()
        with self.assertRaisesRegex(ValueError,'opened WAL dependency missing'):store.recover(next_process_session_id='second')

    async def test_published_unindexed_archive_cannot_hide_a_missing_tail_wal(self):
        store=self.store(True);old=self.prepare(store)
        records=list(self.wal.encoded_records(allow_unsealed=True))
        for encoded in records[:-1]:old.writer.append(RawRecord.from_bytes(encoded))
        old.writer.rotate();old.writer.append(RawRecord.from_bytes(records[-1]))
        def fail(stage):
            if stage=='after_manifest':raise OSError('synthetic publish interruption')
        old.writer.checkpoint=fail
        with self.assertRaises(OSError):old.writer.rotate()
        old.writer.abort();store.close();(old.writer.directory/'00000001.wal').unlink()
        store=self.store()
        with self.assertRaisesRegex(ValueError,'opened WAL dependency missing'):store.recover(next_process_session_id='second')

    async def test_unexposed_initial_and_rotated_empty_wals_survive_proof_publication_failure(self):
        store=self.store(True)
        with patch('data_collector.writer._publish_opened',side_effect=OSError('initial proof fsync')):
            with self.assertRaises(OSError):self.prepare(store)
        store.close();store=self.store();prepared=self.prepare(store,'second')
        self.assertIsNone(prepared.recovery);prepared.writer.abort();store.close()
        shutil.rmtree(self.root/'store');store=self.store(True);old=self.prepare(store)
        for encoded in self.wal.encoded_records(allow_unsealed=True):old.writer.append(RawRecord.from_bytes(encoded))
        with patch('data_collector.writer._publish_opened',side_effect=OSError('rotated proof fsync')):
            with self.assertRaises(OSError):old.writer.rotate()
        old.writer.abort();store.close();store=self.store()
        request=store.recover(next_process_session_id='second')
        self.assertEqual(request.cutoff_pin,self.coordinator.pin)
        self.assertTrue((old.writer.directory/'opened'/'00000001.json').exists())

    async def test_orphan_archive_and_conflicting_opened_proof_are_rejected(self):
        store=self.store(True);old=self.captured(store);store.close();store=self.store()
        request=store.recover(next_process_session_id='second');store.close()
        segments=request.segments[0].path.parent;extra=segments/('f'*64);extra.mkdir()
        store=self.store()
        with self.assertRaisesRegex(ValueError,'unindexed immutable archive evidence'):store.recover(next_process_session_id='second')
        store.close();extra.rmdir()
        proof=old.writer.directory/'opened'/'00000000.json';proof.write_bytes(b'{}')
        store=self.store()
        with self.assertRaisesRegex(ValueError,'opened WAL proof differs'):store.recover(next_process_session_id='second')

    async def test_preallocation_raw_history_and_registry_session_cap_fail_visibly(self):
        store=self.store(True);prepared=self.prepare(store)
        native=next(RawRecord.from_bytes(e) for e in self.wal.encoded_records(allow_unsealed=True)
                    if RawRecord.from_bytes(e).receipt.connection_id=='meta')
        prepared.writer.append(native);prepared.writer.abort();store.close();store=self.store()
        with self.assertRaisesRegex(ValueError,'without cutoff allocation'):store.recover(next_process_session_id='second')
        store.close();shutil.rmtree(self.root/'store')
        store=self.store(True,limits=StartupLimits(max_sessions=1));old=self.captured(store)
        before=(self.root/'store'/'registry.wal').read_bytes()
        with self.assertRaisesRegex(ValueError,'session inventory bound'):self.prepare(store,'second')
        self.assertEqual((self.root/'store'/'registry.wal').read_bytes(),before)

    async def test_initial_proof_directory_creation_without_proof_can_be_abandoned(self):
        store=self.store(True)
        def fail(directory,descriptor,ordinal):
            (directory/'opened').mkdir()
            raise OSError('proof directory created, proof not linked')
        with patch('data_collector.writer._publish_opened',side_effect=fail):
            with self.assertRaises(OSError):self.prepare(store)
        store.close();store=self.store();prepared=self.prepare(store,'second')
        self.assertIsNone(prepared.recovery)
        self.assertEqual(store.sessions[0]['phase'],'abandoned')
        prepared.writer.abort()

    async def test_unopened_reservation_with_raw_data_cannot_be_abandoned(self):
        store=None
        native=next(RawRecord.from_bytes(e) for e in self.wal.encoded_records(allow_unsealed=True)
                    if RawRecord.from_bytes(e).receipt.connection_id=='meta')
        def fail(stage):
            if stage=='after_writer_create':
                store.writer.append(native)
                raise OSError('unregistered writer was incorrectly used')
        store=self.store(True,checkpoint=fail)
        with self.assertRaises(OSError):self.prepare(store)
        store.close();store=self.store()
        with self.assertRaisesRegex(ValueError,'unregistered raw data'):self.prepare(store,'second')
        self.assertEqual(store.sessions[0]['phase'],'reserved')

    async def test_reset_cutoff_history_in_later_registered_session_is_not_selected(self):
        store=self.store(True);self.captured(store)
        second=self.prepare(store,'second')
        self.assertIsNotNone(second.recovery)
        # Simulate a caller incorrectly ignoring the recovery gate and starting
        # a fresh cutoff ledger in the later process. Registration cannot bless it.
        for encoded in self.wal.encoded_records(allow_unsealed=True):
            record=RawRecord.from_bytes(encoded)
            second.writer.append(replace(record,receipt=replace(record.receipt,process_session_id='second')))
        second.writer.abort();store.close();store=self.store()
        before=(self.root/'store'/'registry.wal').read_bytes()
        with self.assertRaises(ValueError):self.prepare(store,'third')
        self.assertEqual((self.root/'store'/'registry.wal').read_bytes(),before)
