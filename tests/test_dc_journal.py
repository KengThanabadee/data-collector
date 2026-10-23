from dataclasses import replace
from pathlib import Path
import os
import tempfile
import unittest
import subprocess
import sys
import signal
from unittest.mock import patch

from data_collector.journal import FRAME, MAGIC, Journal, JournalDamaged, JournalFull, JournalLimits, sync_directory
from tests.test_dc_contracts import raw


class JournalTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory(); self.addCleanup(self.tmp.cleanup)
        self.path = Path(self.tmp.name)/"capture.wal"

    def open(self, **kwargs):
        return Journal(self.path, venue="deribit", environment="production", **kwargs)

    def test_durable_prefix_binary_reopen_and_exclusive_ownership(self):
        with self.open(create=True) as wal:
            self.assertEqual(wal.append(raw(body=b'\xff\x00')), raw().record_id)
            wal.append(raw(1))
            with self.assertRaises(BlockingIOError): self.open()
        with self.open() as wal:
            self.assertEqual(wal.state.records, 2)
            self.assertEqual(wal.state.positions[raw().receipt.stream_key], 1)
            wal.append(raw(2))

    def test_truncated_tail_retained_before_recovery_and_repeated_reopen(self):
        with self.open(create=True) as wal: wal.append(raw())
        prefix = self.path.read_bytes()
        tail = FRAME.pack(20, bytes(32)) + b'broken'
        with self.path.open('ab') as handle: handle.write(tail)
        with self.open() as wal:
            self.assertEqual(wal.state.reason, "truncated_frame_body")
            with self.assertRaises(JournalDamaged): wal.append(raw(1))
            evidence = wal.recover_tail()
            self.assertEqual((self.path.parent/evidence['tail_file']).read_bytes(), tail)
            self.assertEqual(self.path.read_bytes(), prefix)
            self.assertIsNone(wal.recover_tail())
            wal.append(raw(1))
        with self.open() as wal: self.assertEqual(wal.state.records, 2)

    def test_corrupt_complete_frame_is_not_silently_skipped(self):
        with self.open(create=True) as wal:
            wal.append(raw()); first_end=wal.state.valid_bytes; wal.append(raw(1)); wal.append(raw(2))
        data=bytearray(self.path.read_bytes());data[first_end+FRAME.size+5]^=1;self.path.write_bytes(data)
        with self.open() as wal:
            self.assertEqual(wal.state.records,1)
            self.assertEqual(wal.state.reason,"frame_checksum_mismatch")
            evidence=wal.recover_tail()
            self.assertEqual(evidence['tail_bytes'],len(data)-first_end)
            self.assertEqual((self.path.parent/evidence['tail_file']).read_bytes(),data[first_end:])

    def test_size_stream_and_receipt_bounds_checked_before_write(self):
        limits=JournalLimits(target_bytes=1,max_streams=1)
        with self.open(create=True,limits=limits) as wal:
            wal.append(raw()); before=self.path.read_bytes()
            with self.assertRaises(JournalFull): wal.append(raw(1))
            with self.assertRaises(ValueError): wal.append(raw(2))
            other=replace(raw(),receipt=replace(raw().receipt,connection_id='other'))
            with self.assertRaises(JournalFull): wal.append(other)
            self.assertEqual(self.path.read_bytes(),before)
        with self.open(limits=limits) as wal: self.assertEqual(wal.state.records,1)

    def test_failed_fsync_does_not_ack_and_poisoned_writer_requires_reopen(self):
        with self.open(create=True) as wal:
            with patch('data_collector.journal.os.fsync',side_effect=OSError('synthetic disk failure')):
                with self.assertRaises(OSError): wal.append(raw())
            self.assertEqual(wal.state.records,0)
            with self.assertRaises(JournalDamaged): wal.append(raw())
        # A complete unacknowledged record may survive. Recovery must not overwrite it.
        with self.open() as wal: self.assertEqual(wal.state.records,1)

    def test_failed_initial_directory_sync_is_reestablished_before_next_ack(self):
        with patch('data_collector.journal.sync_directory',side_effect=OSError('synthetic dir failure')):
            with self.assertRaises(OSError): self.open(create=True)
        self.assertEqual(self.path.read_bytes(),MAGIC)
        with patch('data_collector.journal.sync_directory',wraps=sync_directory) as sync:
            with self.open() as wal:
                sync.assert_called_once_with(self.path.parent)
                wal.append(raw())
        # A failed reopen must fail before it can return a writable owner.
        with patch('data_collector.journal.sync_directory',side_effect=OSError('synthetic dir failure')):
            with self.assertRaises(OSError): self.open()

    def test_early_closed_preflight_reader_cannot_move_next_append_into_prefix(self):
        with self.open(create=True) as wal:
            for seq in range(3):wal.append(raw(seq))
            prefix=self.path.read_bytes()
            iterator=wal.encoded_records(allow_unsealed=True)
            next(iterator);iterator.close()
            wal.append(raw(3))
            self.assertTrue(self.path.read_bytes().startswith(prefix))
        with self.open() as wal:self.assertEqual(wal.state.records,4)

    def test_partial_write_poison_and_preserve_tail(self):
        with self.open(create=True) as wal:
            real=wal._write_all
            def fail(data):
                real(data[:7]);raise OSError('synthetic short disk write')
            with patch.object(wal,'_write_all',side_effect=fail):
                with self.assertRaises(OSError): wal.append(raw())
        with self.open() as wal:
            self.assertEqual(wal.state.records,0)
            self.assertEqual(wal.state.reason,'truncated_frame_header')
            evidence=wal.recover_tail()
            self.assertEqual(evidence['tail_bytes'],7)
            self.assertEqual(self.path.read_bytes(),MAGIC)

    def test_header_identity_and_symlink_cannot_be_reinterpreted(self):
        with self.open(create=True) as wal: wal.append(raw())
        with self.assertRaises(JournalDamaged):
            Journal(self.path,venue='bybit',environment='production')
        alias=self.path.with_name('alias');alias.symlink_to(self.path)
        with self.assertRaises(OSError): Journal(alias,venue='deribit',environment='production')
        self.path.write_bytes(b'not a journal')
        with self.assertRaises(JournalDamaged): self.open()

    def test_repeated_identical_tail_at_new_offset_retains_distinct_recovery_markers(self):
        with self.open(create=True) as wal: wal.append(raw())
        for seq in (1,2):
            with self.path.open('ab') as dst: dst.write(b'partial')
            with self.open() as wal:
                wal.recover_tail(); wal.append(raw(seq))
        self.assertEqual(len(list(self.path.parent.glob('*.recovery-*.json'))),2)
        self.assertEqual(len(list(self.path.parent.glob('*.tail-*.bin'))),1)

    def test_kill_and_retry_all_tail_publication_stages(self):
        for stage in ('after_tail','after_marker','after_truncate'):
            with self.subTest(stage=stage):
                path=self.path.with_name(stage+'.wal')
                with Journal(path,venue='deribit',environment='production',create=True) as wal:
                    wal.append(raw())
                with path.open('ab') as dst: dst.write(b'partial')
                program='''import os,signal,sys
from data_collector.journal import Journal
with Journal(sys.argv[1],venue='deribit',environment='production') as wal:
 def checkpoint(stage):
  if stage==sys.argv[2]:os.kill(os.getpid(),signal.SIGKILL)
 wal.recover_tail(checkpoint=checkpoint)
'''
                child=subprocess.run([sys.executable,'-c',program,str(path),stage])
                self.assertEqual(child.returncode,-signal.SIGKILL)
                with Journal(path,venue='deribit',environment='production') as wal:
                    wal.recover_tail();wal.append(raw(1))
                self.assertEqual(len(list(path.parent.glob(path.name+'.recovery-*.json'))),1)
                self.assertEqual(next(path.parent.glob(path.name+'.tail-*.bin')).read_bytes(),b'partial')
