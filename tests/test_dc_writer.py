from dataclasses import asdict, replace
import json
import os
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest
from unittest.mock import patch

from data_collector.contracts import Provenance, RawRecord, Receipt, StreamKey, canonical
from data_collector.cutoffs import ClockAnchor, CutoffCoordinator, IngressWriter, MINUTE_NS
from data_collector.journal import Journal, JournalDamaged, JournalFull, JournalLimits
from data_collector.ledger import DurableLedger
from data_collector.segments import SegmentLimits, iter_segment_records, verify_segment
from data_collector.writer import SessionWriter, recover_session
from tests.test_dc_contracts import raw


class WriterTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.root = Path(self.tmp.name)
        self.clock = [1000]
        self.provenance = Provenance('a'*64, 'b'*64, 'c'*64)

    def writer(self, **overrides):
        kwargs = dict(venue='deribit', environment='production', capture_id='capture',
                      process_session_id='process', provenance=self.provenance, started_utc_ns=MINUTE_NS,
                      now_ns=lambda: self.clock[0], limits=JournalLimits(max_record_bytes=4096, target_bytes=3000, max_streams=8),
                      segment_limits=SegmentLimits(max_record_bytes=4096, max_uncompressed_bytes=16384, max_compressed_bytes=16384),
                      rotate_ns=60, max_segments=16)
        kwargs.update(overrides)
        writer = SessionWriter(self.root/'wal', self.root/'archive', **kwargs)
        self.addCleanup(writer.abort)
        return writer

    def records(self, paths):
        return [record for path in paths for record in iter_segment_records(path)]

    def test_group_commit_syncs_once_and_preserves_rotated_raw_prefix(self):
        writer=self.writer()
        expected=tuple(raw(seq) for seq in range(6))
        self.assertEqual(writer.append_batch(expected),tuple(r.record_id for r in expected))
        self.assertGreater(len(writer.committed),0)
        self.assertEqual([r.to_bytes() for r in self.records(writer.finish())],[r.to_bytes() for r in expected])

    def test_group_commit_fsync_failure_poisoning_prevents_successful_retry(self):
        writer=self.writer()
        with patch.object(writer.active,'sync',side_effect=OSError('disk full')):
            with self.assertRaises(OSError):writer.append_batch((raw(0),))
        self.assertTrue(writer.poisoned)
        with self.assertRaises(JournalDamaged):writer.append(raw(1))

    def test_byte_rotation_preserves_every_receipt_and_raw_body(self):
        writer = self.writer()
        expected = [raw(seq) for seq in range(6)]
        for record in expected:
            self.assertEqual(writer.append(record), record.record_id)
        self.assertGreater(len(writer.committed), 0)
        self.assertEqual(list(writer.encoded_records(allow_unsealed=True)), [record.to_bytes() for record in expected])
        segments = writer.finish()
        self.assertEqual([record.to_bytes() for record in self.records(segments)], [record.to_bytes() for record in expected])
        self.assertEqual(writer.state.positions[expected[0].receipt.stream_key], 5)
        for path in segments:
            verify_segment(path)
        self.assertTrue((writer.directory/'complete.json').exists())

    def test_quiet_age_rotation_does_not_create_empty_segments_on_every_tick(self):
        writer = self.writer()
        writer.append(raw())
        self.clock[0] += 60
        self.assertIsNotNone(writer.tick())
        self.clock[0] += 120
        self.assertIsNone(writer.tick())
        self.assertEqual(len(writer.committed), 1)
        writer.append(raw(1))
        self.assertEqual(len(self.records(writer.finish())), 2)

    def test_preflight_rejects_global_sequence_and_namespace_before_rotation(self):
        writer = self.writer()
        writer.append(raw())
        writer.rotate()
        with self.assertRaises(ValueError): writer.append(raw())
        with self.assertRaises(ValueError): writer.append(raw(2))
        with self.assertRaises(ValueError): writer.append(replace(raw(1), provenance=Provenance('d'*64, 'b'*64, 'c'*64)))
        self.assertEqual(writer.state.records, 1)
        writer.append(raw(1))
        self.assertEqual(len(self.records(writer.finish())), 2)

    def test_session_stream_bound_is_not_reset_by_segment_rotation(self):
        writer = self.writer(limits=JournalLimits(max_record_bytes=4096, target_bytes=3000, max_streams=1))
        writer.append(raw())
        writer.rotate()
        other = replace(raw(), receipt=replace(raw().receipt, connection_id='other'))
        with self.assertRaises(JournalFull): writer.append(other)
        self.assertEqual(len(self.records(writer.finish())), 1)

    def test_index_capacity_stops_before_new_record_but_allows_finishing_durable_prefix(self):
        writer = self.writer(max_segments=1, limits=JournalLimits(max_record_bytes=4096, target_bytes=100, max_streams=8))
        writer.append(raw())
        with self.assertRaises(JournalFull): writer.append(raw(1))
        self.assertFalse(writer.poisoned)
        self.assertEqual([r.receipt.receive_seq for r in self.records(writer.finish())], [0])

    def test_lock_and_existing_session_identity_prevent_duplicate_owners(self):
        writer = self.writer()
        with self.assertRaises(BlockingIOError): self.writer()
        writer.append(raw())
        writer.abort()
        with self.assertRaisesRegex(ValueError, 'already exists'): self.writer()
        recovered = recover_session(writer.directory, self.root/'archive')
        self.assertEqual(len(self.records(recovered)), 1)
        self.assertEqual(recover_session(writer.directory, self.root/'archive'), recovered)

    def test_partial_tail_is_preserved_and_recovery_does_not_resume_old_session(self):
        writer = self.writer()
        writer.append(raw())
        path = writer.path
        writer.abort()
        with path.open('ab') as dst:
            dst.write(b'partial frame')
            dst.flush()
            os.fsync(dst.fileno())
        segments = recover_session(writer.directory, self.root/'archive')
        self.assertEqual([r.record_id for r in self.records(segments)], [raw().record_id])
        manifest = json.loads((segments[0]/'manifest.json').read_bytes())
        self.assertEqual(len(manifest['recovery_evidence']), 1)
        with self.assertRaises(ValueError): self.writer()
        fresh = self.writer(process_session_id='fresh')
        fresh.append(replace(raw(), receipt=replace(raw().receipt, process_session_id='fresh')))
        self.assertEqual(len(self.records(fresh.finish())), 1)

    def test_recovery_rejects_cross_segment_receipt_gap(self):
        writer = self.writer()
        writer.append(raw())
        writer.rotate()
        path = writer.path
        writer.abort()
        with Journal(path, venue='deribit', environment='production', limits=writer.limits) as journal:
            journal.append(raw(2))
        with self.assertRaisesRegex(ValueError, 'receipt prefix'):
            recover_session(writer.directory, self.root/'archive')

    def test_monotonic_regression_poison_stops_acknowledgements(self):
        writer = self.writer()
        writer.append(raw())
        self.clock[0] -= 1
        with self.assertRaises(ValueError): writer.append(raw(1))
        self.assertTrue(writer.poisoned)
        writer.abort()
        self.assertEqual(len(self.records(recover_session(writer.directory, self.root/'archive'))), 1)

    def test_cutoff_and_ledger_controls_restore_across_automatic_rotations(self):
        writer = self.writer(limits=JournalLimits(max_record_bytes=32768, target_bytes=6000, max_streams=16),
                             segment_limits=SegmentLimits(max_record_bytes=32768, max_uncompressed_bytes=65536, max_compressed_bytes=65536))
        ingress = IngressWriter(writer, 'capture', 'process')
        coordinator = CutoffCoordinator(writer, capture_scope='capture', capture_id='capture',
                                       process_session_id='process', provenance=self.provenance)
        ledger = DurableLedger(writer, ledger_id='ledger', capture_scope='capture', capture_id='capture',
                               process_session_id='process', provenance=self.provenance)
        c = MINUTE_NS
        pin = ledger.initialize(utc_ns=c-100, monotonic_ns=1000, clock_epoch='epoch')
        anchor = ClockAnchor('process', 'epoch', c-100, 1000, 0, 2)
        ws = StreamKey('capture', 'process', 'connection')
        coordinator.allocate(c, [ws], {key:'d'*64 for key in ['config','required_feeds','universe','metadata']},
                             anchor=anchor, observed_utc_ns=c-100, monotonic_ns=1000, clock_epoch='epoch')
        for seq in range(4):
            record = replace(raw(seq, body=b'{"padding":"'+b'x'*2000+b'"}'),
                             receipt=Receipt('capture','process','connection',seq,c-10,1090,'epoch'))
            ingress.append(record)
        barrier = ingress.barrier(anchor, c, now_monotonic_ns=1100, healthy_streams=[ws])
        coordinator.close(0, barrier, ledger_pin=pin, utc_ns=c, monotonic_ns=1100, clock_epoch='epoch')
        resolution = coordinator.resolutions[0]
        self.assertGreater(len(writer.committed), 1)
        segments = writer.finish()
        factory = lambda: (record for path in segments for record in iter_segment_records(path))
        with Journal(self.root/'reader.wal', venue='deribit', environment='production', create=True) as empty:
            restored = CutoffCoordinator(empty, capture_scope='capture', capture_id='capture',
                                        process_session_id='reader', provenance=self.provenance, prior_records=factory)
            self.assertEqual(restored.resolutions[0], resolution)
            loaded_ledger = DurableLedger(empty, ledger_id='ledger', capture_scope='capture', capture_id='capture',
                                         process_session_id='reader', provenance=self.provenance, prior_records=factory, expected_pin=pin)
            self.assertEqual(loaded_ledger.pin(), pin)

    def test_rotation_failure_is_fail_stop_and_offline_recovery_is_idempotent(self):
        for index, stage in enumerate(['after_seal','after_compression','after_archive','after_manifest','after_index']):
            def injected(current):
                if current == stage: raise OSError('injected rotation interruption')
            writer = self.writer(process_session_id='fault-'+str(index), checkpoint=injected)
            record = replace(raw(), receipt=replace(raw().receipt, process_session_id='fault-'+str(index)))
            writer.append(record)
            with self.assertRaises(OSError): writer.rotate()
            with self.assertRaises(JournalDamaged): writer.append(replace(record, receipt=replace(record.receipt, receive_seq=1)))
            writer.abort()
            recovered = recover_session(writer.directory, self.root/'archive')
            self.assertEqual([r.record_id for r in self.records(recovered)], [record.record_id])
            self.assertEqual(recover_session(writer.directory, self.root/'archive'), recovered)

    def test_real_process_exit_at_rotation_stages_preserves_durable_record(self):
        code = '''import os,sys
from data_collector.writer import SessionWriter
from data_collector.contracts import Provenance
from tests.test_dc_contracts import raw
root,stage=sys.argv[1:]
def killed(current):
    if current==stage:os._exit(77)
w=SessionWriter(root+'/wal',root+'/archive',venue='deribit',environment='production',capture_id='capture',
process_session_id='process',provenance=Provenance('a'*64,'b'*64,'c'*64),started_utc_ns=60000000000,checkpoint=killed)
w.append(raw());w.rotate()
'''
        for stage in ['after_seal','after_compression','after_archive','after_manifest','after_index']:
            root = self.root/stage
            child = subprocess.run([sys.executable,'-c',code,str(root),stage], capture_output=True, timeout=15)
            self.assertEqual(child.returncode, 77, child.stderr.decode())
            directory = next((root/'wal').iterdir())
            recovered = recover_session(directory, root/'archive')
            self.assertEqual([r.record_id for r in self.records(recovered)], [raw().record_id])

    def test_missing_wal_proven_by_commits_cannot_be_reported_as_empty_recovery(self):
        writer = self.writer()
        writer.append(raw())
        segments = writer.finish()
        (writer.directory/'00000000.wal').unlink()
        with self.assertRaisesRegex(ValueError, 'committed WAL dependency missing'):
            recover_session(writer.directory, self.root/'archive')
        self.assertFalse((writer.directory/'recovered.json').exists())
        self.assertEqual([r.record_id for r in self.records(segments)], [raw().record_id])

    def test_graceful_empty_trailing_wal_does_not_change_committed_recovery(self):
        writer = self.writer()
        writer.append(raw())
        writer.rotate()
        expected = writer.finish()
        self.assertEqual(recover_session(writer.directory, self.root/'archive'), expected)
        self.assertEqual(recover_session(writer.directory, self.root/'archive'), expected)

    def test_terminal_and_index_conflicts_are_not_overwritten(self):
        writer = self.writer()
        writer.append(raw())
        writer.finish()
        path = writer.directory/'complete.json'
        original = json.loads(path.read_bytes())
        damaged = dict(original, records=2)
        path.write_text(json.dumps(damaged))
        with self.assertRaisesRegex(ValueError, 'terminal evidence'):
            recover_session(writer.directory, self.root/'archive')
        self.assertFalse((writer.directory/'recovered.json').exists())
        self.assertEqual(json.loads(path.read_bytes()), damaged)

    def test_incompatible_segment_configuration_is_rejected_before_session_creation(self):
        for limits in [SegmentLimits(max_record_bytes=4096, max_streams=1),
                       SegmentLimits(max_record_bytes=4096, max_compressed_bytes=1),
                       SegmentLimits(max_record_bytes=4096, max_manifest_bytes=1)]:
            with self.assertRaises(ValueError): self.writer(segment_limits=limits)
        self.assertFalse((self.root/'wal').exists())

    def test_provenance_version_cap_rotates_before_acknowledging_next_version(self):
        writer = self.writer(segment_limits=SegmentLimits(max_record_bytes=4096, max_versions=1))
        first = raw()
        second = replace(raw(1), provenance=replace(first.provenance, universe_sha256='d'*64))
        writer.append(first)
        writer.append(second)
        self.assertEqual(len(writer.committed), 1)
        segments = writer.finish()
        self.assertEqual([r.to_bytes() for r in self.records(segments)], [first.to_bytes(), second.to_bytes()])
        self.assertEqual(recover_session(writer.directory, self.root/'archive'), segments)

    def test_single_record_version_and_manifest_caps_reject_before_durability(self):
        writer = self.writer(segment_limits=SegmentLimits(max_record_bytes=4096, max_versions=1))
        two_versions = replace(raw(), provenance=replace(raw().provenance, metadata_sha256='e'*64))
        with self.assertRaisesRegex(ValueError, 'single record'): writer.append(two_versions)
        self.assertEqual(writer.state.records, 0)
        writer.finish()
        narrow = self.writer(process_session_id='narrow', segment_limits=SegmentLimits(max_record_bytes=4096,
            max_manifest_bytes=16384+128*1024+32))
        record = replace(raw(), receipt=replace(raw().receipt, process_session_id='narrow'))
        with self.assertRaisesRegex(ValueError, 'single record'): narrow.append(record)
        self.assertEqual(narrow.state.records, 0)

    def test_large_stream_array_delimiters_trigger_rotation_before_manifest_overflow(self):
        # Resource accounting test only: other tests exercise actual fsync/crash
        # boundaries. Suppress fsync latency here, retaining real frames and gzip.
        count = 18000
        streams = [StreamKey('capture', 'process', str(i)) for i in range(count)]
        rows = [dict(stream=asdict(stream), first=0, last=0, records=1) for stream in streams]
        budget = 16384 + 1024 + sum(len(canonical(row)) for row in rows) + 67
        with patch('data_collector.journal.os.fsync', return_value=None):
            writer = self.writer(limits=JournalLimits(max_record_bytes=4096, target_bytes=50*1024*1024, max_streams=count),
                segment_limits=SegmentLimits(max_record_bytes=4096, max_streams=count, max_recoveries=1, max_manifest_bytes=budget))
            original = raw()
            for stream in streams:
                writer.append(replace(original, receipt=replace(original.receipt, connection_id=stream.connection_id)))
            segments = writer.finish()
        self.assertGreater(len(segments), 1)
        total = 0
        for segment in segments:
            data = (segment/'manifest.json').read_bytes()
            self.assertLessEqual(len(data), budget)
            total += json.loads(data)['records']
        self.assertEqual(total, count)


if __name__ == '__main__': unittest.main()
