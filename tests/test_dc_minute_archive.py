"""Minute response archive invariants, exercised with minute run contexts."""
import gzip
import json
from pathlib import Path
import signal
import subprocess
import sys
import tempfile
import unittest

from data_collector.minute_archive import Archive, read_archive, verify, verify_parts
from data_collector.minute_config import Config, context
from data_collector.minute_files import body_bytes, dumps, sha, write_json
from tests.helpers import record

SLOT = "2026-11-08T23:59:00.000+00:00"
BODY = b'{"result":{"index_price":1.234567890123456789,"unknown":"keep"}}'


class MinuteArchiveTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.config = Config(data_root=Path(self.tmp.name), require_ntp=False)

    def archive(self, run="run-1"):
        a = Archive(self.config.data_root, context(self.config, "deribit", "minute", SLOT, run))
        a.update_context(required_jobs=["index"])
        record(a, "index", {}, raw=BODY)
        return a

    def test_lossless_decimal_and_binary_roundtrip_with_incomplete_gate(self):
        a = self.archive()
        record(a, "binary", {}, raw=b"\xff\x00\xe0", ok=False)
        a.finalize()
        manifest, rows = read_archive(a.destination, include_incomplete=True)
        self.assertEqual(body_bytes(rows[0]), BODY)
        self.assertEqual(body_bytes(rows[1]), b"\xff\x00\xe0")
        self.assertEqual(manifest["status"], "complete")
        manifest["status"] = "partial"
        write_json(a.destination / "manifest.json", manifest, replace=True)
        with self.assertRaises(ValueError):
            verify(a.destination)

    def test_incomplete_archive_needs_explicit_opt_in(self):
        a = self.archive()
        a.update_context(required_jobs=["index", "missing"])
        a.finalize()
        with self.assertRaises(ValueError):
            read_archive(a.destination)
        manifest, _ = read_archive(a.destination, include_incomplete=True)
        self.assertEqual((manifest["status"], manifest["missing_jobs"]), ("partial", ["missing"]))

    def test_process_kill_at_publication_checkpoints_recovers_to_identical_bytes(self):
        for point, status in (("before_compression", "interrupted"), ("after_compression", "interrupted"),
                              ("after_archive", "interrupted"), ("after_manifest", "complete")):
            with self.subTest(point=point):
                a = self.archive(point)
                program = """import os,signal,sys
from data_collector.minute_archive import Archive
a=Archive(sys.argv[1],{},existing=sys.argv[2])
def checkpoint(name):
 if name==sys.argv[3]:os.kill(os.getpid(),signal.SIGKILL)
a.finalize(checkpoint=checkpoint)
"""
                child = subprocess.run([sys.executable, "-c", program, str(a.root), str(a.path), point])
                self.assertEqual(child.returncode, -signal.SIGKILL)
                # The minute restart path finalizes the surviving work directory as interrupted.
                Archive(a.root, {}, existing=a.path).finalize(interrupted=True)
                first = (a.destination / "manifest.json").read_bytes()
                packed = (a.destination / "responses.jsonl.gz").read_bytes()
                Archive(a.root, {}, existing=a.path).finalize(interrupted=True)
                self.assertEqual(first, (a.destination / "manifest.json").read_bytes())
                self.assertEqual(packed, (a.destination / "responses.jsonl.gz").read_bytes())
                self.assertEqual(verify(a.destination)[0]["status"], status)
                self.assertTrue((a.path / "journal.jsonl").exists())

    def test_truncated_tail_retained_and_corruption_refused(self):
        a = self.archive()
        with (a.path / "journal.jsonl").open("ab") as f:
            f.write(b'{"truncated":')
        Archive(a.root, {}, existing=a.path).finalize(interrupted=True)
        manifest, _ = verify(a.destination)
        self.assertEqual(manifest["status"], "interrupted")
        self.assertEqual((a.destination / "truncated-tail.bin").read_bytes(), b'{"truncated":')
        (a.destination / "truncated-tail.bin").write_bytes(b"changed")
        with self.assertRaises(ValueError):
            verify(a.destination)

    def test_mismatched_orphan_archive_not_overwritten(self):
        a = self.archive()
        a.destination.mkdir(parents=True)
        packed = gzip.compress(b"wrong\n", mtime=0)
        (a.destination / "responses.jsonl.gz").write_bytes(packed)
        with self.assertRaises(ValueError):
            a.finalize()
        self.assertEqual((a.destination / "responses.jsonl.gz").read_bytes(), packed)
        self.assertTrue((a.path / "journal.jsonl").exists())

    def test_committed_archive_refuses_context_change_and_conflicting_journal(self):
        a = self.archive()
        a.finalize()
        with self.assertRaises(ValueError):
            a.update_context(required_jobs=[])
        with (a.path / "journal.jsonl").open("ab") as f:
            f.write(b"unexpected")
        with self.assertRaises(ValueError):
            Archive(a.root, {}, existing=a.path).finalize()

    def test_archive_corruption_and_tampered_provenance_are_refused(self):
        a = self.archive()
        a.finalize()
        manifest, _ = verify(a.destination)
        context_bytes = (a.destination / "context.json").read_bytes()
        packed = (a.destination / "responses.jsonl.gz").read_bytes()
        verify_parts(manifest, context_bytes, packed)
        with self.assertRaises(ValueError):
            verify_parts(manifest, context_bytes, packed + b"bad")
        tampered = dict(manifest, currency="other")
        with self.assertRaises(ValueError):
            verify_parts(tampered, context_bytes, packed)
        changed = dumps(dict(json.loads(context_bytes), config={"rate": 1}))
        with self.assertRaises(ValueError):
            verify_parts(dict(manifest, context_sha256=sha(changed)), changed, packed)
        with self.assertRaises(ValueError):
            verify_parts(dict(manifest, attempts=99), context_bytes, packed)
        p = a.destination / "responses.jsonl.gz"
        p.write_bytes(p.read_bytes() + b"bad")
        with self.assertRaises(ValueError):
            verify(a.destination)

    def test_only_minute_contexts_are_archived(self):
        a = Archive(self.config.data_root, context(self.config, "deribit", "snapshot", SLOT, "run-2"))
        with self.assertRaises(ValueError):
            a.finalize()


if __name__ == "__main__":
    unittest.main()
