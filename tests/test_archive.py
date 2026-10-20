import gzip
import json
import os
from pathlib import Path
import signal
import subprocess
import sys
import tempfile
import unittest
from option_collector.archive import Archive, recover_all, verify, read_archive
from option_collector.collector import context
from option_collector.common import body_bytes, dumps, read_json, sha, write_json
from option_collector.config import Config
from option_collector.operations import export_archive
from tests.helpers import record


class ArchiveTests(unittest.TestCase):
    def setUp(self):
        self.tmp=tempfile.TemporaryDirectory();self.addCleanup(self.tmp.cleanup)
        self.config=Config(data_root=Path(self.tmp.name),require_ntp=False)

    def archive(self):
        a=Archive(self.config.data_root,context(self.config,"BTC","delivery"))
        a.update_context(required_jobs=["index"])
        record(a,"index",{},raw=b'{"result":{"index_price":1.234567890123456789,"unknown":"keep"}}')
        return a

    def test_lossless_decimal_binary_roundtrip_and_export(self):
        a=self.archive()
        record(a,"binary",{},raw=b'\xff\x00\xe0',ok=False)
        a.finalize()
        m,rows=read_archive(a.destination,include_incomplete=True)
        self.assertEqual(body_bytes(rows[0]),b'{"result":{"index_price":1.234567890123456789,"unknown":"keep"}}')
        self.assertEqual(body_bytes(rows[1]),b'\xff\x00\xe0')
        with self.assertRaises(ValueError):read_archive(a.destination)
        dest=Path(self.tmp.name)/"export"
        export_archive(a.destination,dest,True)
        self.assertEqual(verify(dest)[0],m)
        with self.assertRaises(ValueError):export_archive(a.destination,dest,True)

    def test_process_kill_at_all_publication_checkpoints(self):
        for point in ("before_compression","after_compression","after_archive","after_manifest","after_cleanup"):
            with self.subTest(point=point):
                a=self.archive()
                program="""import os,signal,sys
from option_collector.archive import Archive
a=Archive(sys.argv[1],{},existing=sys.argv[2])
def checkpoint(name):
 if name==sys.argv[3]:os.kill(os.getpid(),signal.SIGKILL)
a.finalize(checkpoint=checkpoint)
"""
                child=subprocess.run([sys.executable,"-c",program,str(a.root),str(a.path),point])
                self.assertEqual(child.returncode,-signal.SIGKILL)
                recover_all(a.root)
                first=(a.destination/"manifest.json").read_bytes()
                packed=(a.destination/"responses.jsonl.gz").read_bytes()
                recover_all(a.root)
                self.assertEqual(first,(a.destination/"manifest.json").read_bytes())
                self.assertEqual(packed,(a.destination/"responses.jsonl.gz").read_bytes())
                expected="partial" if point in ("after_manifest","after_cleanup") else "interrupted"
                self.assertEqual(verify(a.destination)[0]["status"],expected)

    def test_truncated_tail_retained_and_corruption_refused(self):
        a=self.archive()
        with (a.path/"journal.jsonl").open("ab") as f:f.write(b'{"truncated":')
        recover_all(a.root)
        m,_=verify(a.destination)
        self.assertEqual(m["status"],"interrupted")
        self.assertEqual((a.destination/"truncated-tail.bin").read_bytes(),b'{"truncated":')
        (a.destination/"truncated-tail.bin").write_bytes(b"changed")
        with self.assertRaises(ValueError):verify(a.destination)

    def test_mismatched_orphan_archive_not_overwritten(self):
        a=self.archive();a.destination.mkdir(parents=True)
        packed=gzip.compress(b"wrong\n",mtime=0)
        (a.destination/"responses.jsonl.gz").write_bytes(packed)
        with self.assertRaises(ValueError):a.finalize()
        self.assertEqual((a.destination/"responses.jsonl.gz").read_bytes(),packed)
        self.assertTrue((a.path/"journal.jsonl").exists())

    def test_clean_yield_is_resumable_but_active_crash_is_interrupted(self):
        a=self.archive();a.checkpoint("yielded")
        self.assertEqual(recover_all(a.root),[])
        b=Archive(a.root,{},existing=a.path)
        self.assertTrue(b.clean_yield())
        b.checkpoint("in_progress")
        self.assertEqual(recover_all(a.root)[0]["status"],"interrupted")

    def test_checkpoint_context_tamper_and_archive_corruption(self):
        a=self.archive();a.checkpoint("yielded")
        a.update_context(next_offset=999)
        with self.assertRaises(ValueError):recover_all(a.root)
        a.checkpoint("in_progress");a.finalize(interrupted=True)
        p=a.destination/"responses.jsonl.gz";p.write_bytes(p.read_bytes()+b"bad")
        with self.assertRaises(ValueError):verify(a.destination)
