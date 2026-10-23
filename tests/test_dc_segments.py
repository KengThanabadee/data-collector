from dataclasses import replace
from pathlib import Path
import gzip
import json
import os
import signal
import subprocess
import sys
import tempfile
import unittest
from unittest.mock import patch

from data_collector.contracts import canonical, digest
from data_collector.journal import FRAME, MAGIC, Journal, JournalDamaged
from data_collector.segments import SegmentContext, SegmentLimits, iter_segment_records, seal_segment, verify_segment, _mkdir, sync_directory
from tests.test_dc_contracts import raw


class SegmentTests(unittest.TestCase):
    def setUp(self):
        self.tmp=tempfile.TemporaryDirectory();self.addCleanup(self.tmp.cleanup)
        self.root=Path(self.tmp.name)
        self.path=self.root/'capture.wal'
        self.context=SegmentContext('deribit','production','capture','process','segment-1','a'*64,'b'*64)

    def open(self,create=False):
        return Journal(self.path,venue='deribit',environment='production',create=create)

    def make(self):
        with self.open(True) as wal:
            wal.append(raw(body=b'\xff\x00'));wal.append(raw(1))
            result=seal_segment(wal,self.root/'archive',self.context)
        return result

    def test_publish_replay_original_bytes_stats_and_persistent_seal(self):
        with self.open(True) as wal:
            wal.append(raw(body=b'\xff\x00'));wal.append(raw(1))
            destination=seal_segment(wal,self.root/'archive',self.context)
            manifest=verify_segment(destination)
            self.assertEqual(manifest['records'],2)
            self.assertEqual([r.body for r in iter_segment_records(destination)],[b'\xff\x00',raw(1).body])
            with self.assertRaises(JournalDamaged):wal.append(raw(2))
            before={p.name:p.read_bytes() for p in destination.iterdir() if p.is_file()}
            self.assertEqual(seal_segment(wal,self.root/'archive',self.context),destination)
            self.assertEqual(before,{p.name:p.read_bytes() for p in destination.iterdir() if p.is_file()})
        with self.open() as wal:
            with self.assertRaises(JournalDamaged):wal.append(raw(2))
            self.assertEqual(seal_segment(wal,self.root/'archive',self.context),destination)
            with self.assertRaises(JournalDamaged):seal_segment(wal,self.root/'archive',replace(self.context,segment_id='wrong'))

    def test_process_kill_all_publication_boundaries(self):
        for stage in ('after_seal','after_compression','after_archive','after_manifest'):
            with self.subTest(stage=stage), tempfile.TemporaryDirectory() as tmp:
                path=Path(tmp)/'source.wal'
                with Journal(path,venue='deribit',environment='production',create=True) as wal:
                    wal.append(raw())
                program='''import os,signal,sys
from data_collector.journal import Journal
from data_collector.segments import SegmentContext,seal_segment
context=SegmentContext('deribit','production','capture','process','segment-1','a'*64,'b'*64)
with Journal(sys.argv[1],venue='deribit',environment='production') as wal:
 def checkpoint(stage):
  if stage==sys.argv[3]:os.kill(os.getpid(),signal.SIGKILL)
 seal_segment(wal,sys.argv[2],context,checkpoint=checkpoint)
'''
                archive=Path(tmp)/'archive'
                child=subprocess.run([sys.executable,'-c',program,str(path),str(archive),stage])
                self.assertEqual(child.returncode,-signal.SIGKILL)
                with Journal(path,venue='deribit',environment='production') as wal:
                    destination=seal_segment(wal,archive,self.context)
                    self.assertEqual(verify_segment(destination)['records'],1)
                    manifest=(destination/'manifest.json').read_bytes()
                    seal_segment(wal,archive,self.context)
                    self.assertEqual((destination/'manifest.json').read_bytes(),manifest)

    def test_recovered_tail_evidence_is_independent_of_source_wal(self):
        with self.open(True) as wal:wal.append(raw())
        with self.path.open('ab') as dst:dst.write(b'partial')
        with self.open() as wal:
            evidence=wal.recover_tail();wal.append(raw(1))
            destination=seal_segment(wal,self.root/'archive',self.context)
        self.path.unlink()
        (self.path.parent/evidence['tail_file']).unlink()
        manifest=verify_segment(destination)
        self.assertEqual(len(manifest['recovery_evidence']),1)
        self.assertEqual((destination/evidence['tail_file']).read_bytes(),b'partial')
        (destination/evidence['tail_file']).write_bytes(b'changed')
        with self.assertRaises(ValueError):verify_segment(destination)

    def test_corruption_context_and_seal_are_rejected_before_consumer_gets_rows(self):
        destination=self.make()
        manifest_path=destination/'manifest.json';original=manifest_path.read_bytes()
        manifest=json.loads(original);manifest['wal']['records']=999
        manifest_path.write_bytes(canonical(manifest))
        with self.assertRaises(ValueError):iter_segment_records(destination)
        manifest_path.write_bytes(original)
        context=destination/'context.json';context.write_bytes(context.read_bytes()+b' ')
        with self.assertRaises(ValueError):verify_segment(destination)

    def test_limits_and_conflicting_orphan_are_never_overwritten(self):
        with self.open(True) as wal:
            wal.append(raw())
            with self.assertRaises(ValueError):
                seal_segment(wal,self.root/'archive',self.context,limits=SegmentLimits(max_uncompressed_bytes=1))
            destination=self.root/'archive'/'segments'/self.context.context_id
            self.assertFalse((destination/'manifest.json').exists())
            destination.mkdir(parents=True,exist_ok=True)
            packed=destination/'records.jsonl.gz';packed.write_bytes(b'conflicting orphan')
            with self.assertRaises(ValueError):seal_segment(wal,self.root/'archive',self.context)
            self.assertEqual(packed.read_bytes(),b'conflicting orphan')
            self.assertFalse((destination/'manifest.json').exists())

    def test_compressed_and_record_bounds_and_no_incomplete_line(self):
        destination=self.make()
        with self.assertRaises(ValueError):verify_segment(destination,limits=SegmentLimits(max_compressed_bytes=1))
        with self.assertRaises(ValueError):verify_segment(destination,limits=SegmentLimits(max_record_bytes=10))
        packed=destination/'records.jsonl.gz'
        value=gzip.decompress(packed.read_bytes());packed.write_bytes(gzip.compress(value[:-1],mtime=0))
        manifest=json.loads((destination/'manifest.json').read_bytes())
        manifest['archive_sha256']=digest(packed.read_bytes());manifest['archive_bytes']=packed.stat().st_size
        (destination/'manifest.json').write_bytes(canonical(manifest))
        with self.assertRaises(ValueError):iter_segment_records(destination)

    def test_wrong_context_does_not_irreversibly_bind_acknowledged_wal(self):
        with self.open(True) as wal:
            for seq in range(3):wal.append(raw(seq))
            with self.assertRaises(ValueError):
                seal_segment(wal,self.root/'archive',replace(self.context,capture_id='wrong'))
            self.assertIsNone(wal.seal_binding)
            self.assertFalse(self.path.with_name(self.path.name+'.sealed.json').exists())
            wal.append(raw(3))
            destination=seal_segment(wal,self.root/'archive',self.context)
            self.assertEqual(verify_segment(destination)['records'],4)
        with self.open() as wal:
            self.assertEqual(wal.state.records,4)
        self.assertEqual([r.receipt.receive_seq for r in iter_segment_records(destination)],list(range(4)))

    def test_corruption_after_archive_does_not_publish_manifest(self):
        with self.open(True) as wal:
            wal.append(raw())
            destination=self.root/'archive'/'segments'/self.context.context_id
            def corrupt(stage):
                if stage=='after_archive':(destination/'records.jsonl.gz').write_bytes(b'corrupt')
            with self.assertRaises(ValueError):
                seal_segment(wal,self.root/'archive',self.context,checkpoint=corrupt)
            self.assertFalse((destination/'manifest.json').exists())
            self.assertTrue(self.path.exists())

    def test_multiline_wal_envelope_rejected_before_seal_or_commit(self):
        import hashlib
        encoded=json.dumps(json.loads(raw().to_bytes()),indent=2).encode()
        self.path.write_bytes(MAGIC+FRAME.pack(len(encoded),hashlib.sha256(encoded).digest())+encoded)
        with self.open() as wal:
            with self.assertRaisesRegex(ValueError,'single-line'):
                seal_segment(wal,self.root/'archive',self.context)
            self.assertIsNone(wal.seal_binding)
            self.assertFalse(list((self.root/'archive').glob('**/manifest.json')))

    def test_existing_directory_retry_reestablishes_entry_durability(self):
        target=self.root/'new-destination'
        def fail_final(path):
            if path==self.root and target.exists():raise OSError('synthetic mkdir-fsync interruption')
            sync_directory(path)
        with patch('data_collector.segments.sync_directory',side_effect=fail_final):
            with self.assertRaises(OSError):_mkdir(target)
        self.assertTrue(target.exists())
        with patch('data_collector.segments.sync_directory',wraps=sync_directory) as sync:
            _mkdir(target)
            sync.assert_called_once_with(target.parent)
