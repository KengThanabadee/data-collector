"""Offline fault injection with SIGKILL at explicit child checkpoints."""
import json
import os
from pathlib import Path
import select
import subprocess
import sys
import tempfile
import unittest

from archive_probe import Journal, decoded_body, digest, encoded_body, finalize, recover, verify

BODY = b'{"jsonrpc":"2.0","result":{"price":0.123456789012345678901,"unknown":null}}'


def child(path, target):
    journal = Journal(path, {"purpose": "offline-crash-test"})
    for index in range(12):
        body = BODY + (b" " * index)
        journal.append(dict(encoded_body(body), body_sha256=digest(body)))
    journal.close()
    if target in ("truncated_line", "truncated_after_manifest"):
        with (Path(path) / "journal.jsonl").open("ab") as output:
            output.write(b'{"record_id":12,"raw_body":"cut')
            output.flush()
            os.fsync(output.fileno())
    def checkpoint(stage):
        if stage == target or (target == "truncated_line" and stage == "before_compression") or (target == "truncated_after_manifest" and stage == "after_manifest"):
            print("CHECKPOINT " + stage, flush=True)
            # Parent kills the child after this observed checkpoint.
            sys.stdin.read()
    finalize(path, checkpoint=checkpoint)


class ArchiveProbeTests(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.root = Path(self.temporary.name)

    def tearDown(self):
        self.temporary.cleanup()

    def assert_records(self, path):
        manifest, records = verify(path)
        self.assertEqual(len(records), 12)
        for index, record in enumerate(records):
            self.assertEqual(decoded_body(record), BODY + b" " * index)
        return manifest

    def test_sigkill_recovery_matrix(self):
        for stage in ("before_compression", "during_compression", "after_archive", "after_manifest", "after_cleanup", "truncated_line", "truncated_after_manifest"):
            with self.subTest(stage=stage):
                path = self.root / stage
                proc = subprocess.Popen([sys.executable, __file__, "--child", str(path), stage], stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
                try:
                    ready, _, _ = select.select([proc.stdout], [], [], 15)
                    self.assertTrue(ready, "child did not reach checkpoint")
                    line = proc.stdout.readline()
                    self.assertTrue(line.startswith(b"CHECKPOINT"), line)
                    proc.kill()
                    proc.wait(timeout=10)
                    self.assertLess(proc.returncode, 0)
                    first = finalize(path) if stage == "truncated_after_manifest" else recover(path)
                    manifest_bytes = (path / "manifest.json").read_bytes()
                    archive_bytes = (path / "responses.jsonl.gz").read_bytes()
                    self.assert_records(path)
                    self.assertEqual(recover(path), first)
                    self.assertEqual((path / "manifest.json").read_bytes(), manifest_bytes)
                    self.assertEqual((path / "responses.jsonl.gz").read_bytes(), archive_bytes)
                    if stage in ("truncated_line", "truncated_after_manifest"):
                        self.assertGreater(first["truncated_tail_bytes"], 0)
                        self.assertEqual((path / "truncated-tail.bin").read_bytes(), b'{"record_id":12,"raw_body":"cut')
                finally:
                    if proc.poll() is None:
                        proc.kill()
                    proc.communicate(timeout=10)

    def seed(self, name):
        path = self.root / name
        journal = Journal(path, {})
        for _ in range(12):
            journal.append(dict(encoded_body(BODY), body_sha256=digest(BODY)))
        journal.close()
        finalize(path)
        return path

    def test_corruption_is_not_blessed_by_recovery(self):
        for mode in ("gzip", "checksum", "count"):
            with self.subTest(mode=mode):
                path = self.seed(mode)
                if mode == "gzip":
                    target = path / "responses.jsonl.gz"
                    body = bytearray(target.read_bytes())
                    body[len(body) // 2] ^= 0x20
                    target.write_bytes(body)
                else:
                    target = path / "manifest.json"
                    value = json.loads(target.read_text())
                    value["archive_sha256" if mode == "checksum" else "records"] = "wrong" if mode == "checksum" else 999
                    target.write_text(json.dumps(value))
                evidence = target.read_bytes()
                with self.assertRaises((ValueError, OSError)):
                    recover(path)
                self.assertEqual(target.read_bytes(), evidence)

    def test_binary_and_decimal_tokens_round_trip(self):
        for body in (BODY, b"\xff\x00\x80not utf8", b'{"text":"\xe0\xb9\x84\xe0\xb8\x97\xe0\xb8\xa2"}'):
            self.assertEqual(decoded_body(encoded_body(body)), body)

    def test_tail_evidence_cannot_be_lost_or_corrupted(self):
        for mode in ("missing", "corrupted"):
            with self.subTest(mode=mode):
                path = self.root / mode
                journal = Journal(path, {})
                journal.append(dict(encoded_body(BODY), body_sha256=digest(BODY)))
                journal.close()
                with (path / "journal.jsonl").open("ab") as output:
                    output.write(b'{"truncated":')
                recover(path)
                evidence = path / "truncated-tail.bin"
                if mode == "missing":
                    evidence.unlink()
                else:
                    evidence.write_bytes(b"different")
                with self.assertRaises((ValueError, OSError)):
                    recover(path)

    def test_uncommitted_archive_mismatch_is_preserved(self):
        path = self.root / "uncommitted"
        journal = Journal(path, {})
        journal.append(dict(encoded_body(BODY), body_sha256=digest(BODY)))
        journal.close()
        published = path / "responses.jsonl.gz"
        published.write_bytes(b"not the journal's archive")
        with self.assertRaises((ValueError, OSError)):
            recover(path)
        self.assertEqual(published.read_bytes(), b"not the journal's archive")
        self.assertTrue((path / "journal.jsonl").exists())


if __name__ == "__main__":
    if len(sys.argv) > 1 and sys.argv[1] == "--child":
        child(sys.argv[2], sys.argv[3])
    else:
        unittest.main(verbosity=2)
