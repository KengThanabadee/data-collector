import json
import unittest
from unittest.mock import patch

from data_collector.journal import FRAME, MAGIC, Journal, JournalDamaged
from data_collector.ledger import DurableLedger
from tests.test_dc_contracts import raw
from tests import test_dc_writer as writers
from tests import test_dc_discoveries as discoveries


class TypedRecordTests(unittest.TestCase):
    setUp = writers.WriterTests.setUp
    writer = writers.WriterTests.writer

    def test_typed_and_encoded_views_preserve_binary_bytes_across_rotation_and_active_wal(self):
        writer = self.writer()
        expected = [raw(i, body=b'\x00\xff'+bytes([i])) for i in range(3)]
        writer.append(expected[0]);writer.rotate()
        writer.append(expected[1]);writer.rotate();writer.append(expected[2])
        encoded = list(writer.encoded_records(allow_unsealed=True))
        for _ in range(2):
            self.assertEqual([r.to_bytes() for r in writer.raw_records(allow_unsealed=True)], encoded)
            self.assertEqual(encoded, [r.to_bytes() for r in expected])
        with self.assertRaises(JournalDamaged):list(writer.raw_records())
        writer.abort()
        with self.assertRaises(JournalDamaged):list(writer.raw_records(allow_unsealed=True))

    def test_typed_segment_read_still_verifies_entire_segment_before_first_yield(self):
        writer = self.writer();writer.append(raw());writer.rotate()
        path = writer.committed[0]/'manifest.json'
        # A prior successful typed read must not cache trust for the next read.
        self.assertEqual(len(list(writer.raw_records(allow_unsealed=True))), 1)
        manifest = json.loads(path.read_bytes());manifest['records'] = 2
        path.write_text(json.dumps(manifest))
        iterator = writer.raw_records(allow_unsealed=True)
        with self.assertRaisesRegex(ValueError, 'statistics mismatch'):next(iterator)

    def test_typed_active_wal_rechecks_frames_and_unexpected_suffix(self):
        with Journal(self.root/'typed.wal', venue='deribit', environment='production', create=True) as wal:
            wal.append(raw(body=b'\xff\x00'))
            self.assertEqual([r.to_bytes() for r in wal.raw_records(allow_unsealed=True)], [raw(body=b'\xff\x00').to_bytes()])
            with self.assertRaises(JournalDamaged):list(wal.raw_records())
            with wal.path.open('ab') as dst:dst.write(b'extra')
            with self.assertRaisesRegex(JournalDamaged, 'unexpected suffix'):list(wal.raw_records(allow_unsealed=True))
        with Journal(self.root/'changed.wal', venue='deribit', environment='production', create=True) as wal:
            wal.append(raw())
            self.assertEqual(len(list(wal.raw_records(allow_unsealed=True))), 1)
            with wal.path.open('r+b') as dst:
                dst.seek(len(MAGIC)+FRAME.size+5);byte = dst.read(1)
                dst.seek(-1, 1);dst.write(bytes([byte[0]^1]))
            with self.assertRaisesRegex(JournalDamaged, 'frame changed'):list(wal.raw_records(allow_unsealed=True))


class TypedCertificationTests(unittest.TestCase):
    setUp = discoveries.DiscoveryTests.setUp
    fixture = discoveries.DiscoveryTests.fixture
    replacement = discoveries.DiscoveryTests.replacement
    registry = discoveries.DiscoveryTests.registry
    next_stamp = discoveries.DiscoveryTests.next_stamp
    declare = discoveries.DiscoveryTests.declare
    native = discoveries.DiscoveryTests.native
    open_ledger = discoveries.DiscoveryTests.open_ledger

    def managed(self):
        self.fixture();plan = self.replacement();self.declare(plan);self.native(plan)
        return self.open_ledger()

    def test_ledger_and_owner_prefer_typed_view_but_keep_encoded_only_source_compatibility(self):
        ledger = self.managed();pin = ledger.pin()
        # These consumers no longer need the encode/decode round trip for a
        # SessionWriter. Segment parsing and WAL checks are still exercised.
        with patch.object(self.writer, 'encoded_records', side_effect=AssertionError('round trip')):
            self.assertEqual(ledger.pin(), pin)
            self.assertEqual(self.registry().plans[-1].plan, self.replacement())
        from types import SimpleNamespace
        source = SimpleNamespace(identity=self.writer.identity,
            encoded_records=self.writer.encoded_records)
        restored = DurableLedger(source, ledger_id='ledger', capture_scope='capture',
            capture_id='capture', process_session_id='process', provenance=self.clock.provenance)
        self.assertEqual(restored.pin(), pin)

    def test_missing_raw_after_prior_pin_poisoned_and_pending_negative_still_blocks(self):
        ledger = self.managed();ledger.pin()
        self.native(self.replacement(), 1, snapshot=False)
        with self.assertRaisesRegex(ValueError, 'unmapped native discovery'):ledger.pin()
        with self.assertRaisesRegex(ValueError, 'reopened'):ledger.pin()
        ledger = self.open_ledger()
        (self.writer.committed[0]/'manifest.json').unlink()
        with self.assertRaises(FileNotFoundError):ledger.pin()
        self.assertTrue(ledger.poisoned)
