import dataclasses
import json
from pathlib import Path
import tempfile
import unittest

from data_collector.contracts import InstrumentKey, Provenance, RawRecord, Receipt, canonical
from data_collector.metadata import MetadataHistory, MetadataVersion
from data_collector.legacy import legacy_observations, read_legacy_archive
from tests.helpers import key, raw


class ContractsTests(unittest.TestCase):
    def test_identity_text_rejects_c0_controls_and_preserves_unicode(self):
        for codepoint in range(32):
            with self.subTest(codepoint=codepoint),self.assertRaises(ValueError):
                key(native_symbol='BTC-'+chr(codepoint)+'-C')
        for value in ('BTC-31OCT26-100000-C','สัญญา','BTC-\u00a0-C','BTC-\x7f-C','BTC-\U0001f30f-C'):
            self.assertEqual(key(native_symbol=value).native_symbol,value)
        for value in ('',None,123,b'BTC'):
            with self.subTest(value=value),self.assertRaises(ValueError):key(native_symbol=value)

    def test_native_body_exact_decimal_unknown_and_binary(self):
        for body in [b'{"p":0.123456789012345678901234567890,"new_field":42}', b'\x00\xff\xe0', b'']:
            record = raw(body=body)
            decoded = RawRecord.from_bytes(record.to_bytes())
            self.assertEqual(decoded.body, body)
            self.assertEqual(decoded.record_id, record.record_id)

    def test_contract_family_settlement_symbol_reuse_and_venue_are_distinct(self):
        base = key()
        variants = [base, key(native_product="linear_option"), key(venue="bybit"),
                    key(environment="testnet"), key(listing_incarnation="listing-2")]
        self.assertEqual(len({v.key_id for v in variants}), 5)
        with self.assertRaises(ValueError):
            raw(instrument=key(venue="binance"))

    def test_receipt_identity_ignores_wall_clock_but_not_session(self):
        receipt = raw().receipt
        self.assertEqual(receipt.record_id, dataclasses.replace(receipt, receive_utc_ns=1).record_id)
        self.assertNotEqual(receipt.record_id, dataclasses.replace(receipt, process_session_id="next").record_id)
        for bad in [True, -1, 1.0]:
            with self.assertRaises(ValueError):
                dataclasses.replace(receipt, receive_seq=bad)

    def test_reject_corrupt_and_ambiguous_envelopes(self):
        original = json.loads(raw().to_bytes())
        for field, value in [("record_id", "0"*64), ("schema", "dc.raw/99")]:
            with self.assertRaises(ValueError):
                RawRecord.from_bytes(canonical(original | {field: value}))
        for value in [original["payload"] | {"data": "eA=="}, original["payload"] | {"bytes": True}]:
            with self.assertRaises(ValueError):
                RawRecord.from_bytes(canonical(original | {"payload": value}))
        with self.assertRaises(ValueError):
            RawRecord.from_bytes(b'{"schema":1,"schema":2}')
        with self.assertRaises(ValueError):
            RawRecord.from_bytes(raw().to_bytes(), max_body_bytes=1)
        for fields in (b'{"value":NaN}', b'{"value":1e9999}', b'{"a":1,"a":2}'):
            with self.assertRaises(ValueError):
                raw(native_fields_json=fields)

    def test_metadata_effective_intervals_and_receipt_pinned_knowledge(self):
        history = MetadataHistory()
        r0 = Receipt("capture", "process", "metadata", 0, 100, 100, "epoch")
        r1 = dataclasses.replace(r0, receive_seq=1, receive_utc_ns=200, monotonic_ns=200)
        r2 = dataclasses.replace(r0, receive_seq=2, receive_utc_ns=90, monotonic_ns=300, clock_epoch="next")
        first = MetadataVersion(key(), r0, 100, b'{"tick":"1"}')
        second = MetadataVersion(key(), r1, 200, b'{"tick":"2"}')
        revision = MetadataVersion(key(), r2, 100, b'{"tick":"0.5"}', first.version_id)
        for version in (first, second, revision): history.add(version)
        at = lambda position, effective: history.known_at(key(), {r0.stream_key:position}, effective_cutoff_ns=effective)
        self.assertIsNone(at(0, 99))
        self.assertEqual(at(0, 150), first)
        self.assertEqual(at(1, 150), first)
        self.assertEqual(at(2, 150), revision)
        self.assertEqual(at(2, 250), second)
        self.assertEqual(history.at_version(first.version_id, {r0.stream_key:2}), first)
        with self.assertRaises(ValueError): history.at_version(revision.version_id, {r0.stream_key:1})
        with self.assertRaises(ValueError): history.known_at(key(), 400, effective_cutoff_ns=250)
        attrs = revision.attributes; attrs["tick"]="invented"
        self.assertEqual(revision.attributes["tick"], "0.5")
        self.assertEqual(history.add(revision), revision.version_id)
        with self.assertRaises(ValueError):
            history.add(dataclasses.replace(revision, instrument=key(venue="bybit")))

    def test_metadata_tied_utc_future_effective_reuse_and_conflicts(self):
        history = MetadataHistory()
        r0 = Receipt("capture", "process", "metadata", 0, 100, 10, "epoch")
        r1 = dataclasses.replace(r0, receive_seq=1, monotonic_ns=11)
        first = MetadataVersion(key(), r0, 0, b'{"tick":"1"}')
        future = MetadataVersion(key(), r1, 1000, b'{"tick":"2"}')
        history.add(first); history.add(future)
        self.assertEqual(history.known_at(key(), {r0.stream_key:0}, effective_cutoff_ns=2000), first)
        self.assertEqual(history.known_at(key(), {r0.stream_key:1}, effective_cutoff_ns=900), first)
        self.assertEqual(history.known_at(key(), {r0.stream_key:1}, effective_cutoff_ns=1000), future)
        self.assertIsNone(history.known_at(key(listing_incarnation="reused"), {r0.stream_key:1}, effective_cutoff_ns=2000))
        with self.assertRaises(ValueError): history.add(dataclasses.replace(future, attributes_json=b'{}'))
        with self.assertRaises(ValueError): history.add(dataclasses.replace(future, receipt=dataclasses.replace(r1, receive_seq=2), attributes_json=b'{}'))

    def test_missing_effective_time_requires_explicit_pin_instead_of_stale_metadata(self):
        history = MetadataHistory()
        receipt = Receipt("capture", "process", "metadata", 0, 100, 10, "epoch")
        first = MetadataVersion(key(), receipt, 100, b'{"tick":"1"}')
        later = MetadataVersion(key(), dataclasses.replace(receipt, receive_seq=1), None, b'{"tick":"2"}')
        history.add(first); history.add(later)
        self.assertEqual(history.known_at(key(), {receipt.stream_key:0}, effective_cutoff_ns=200), first)
        with self.assertRaisesRegex(ValueError, "explicit metadata version pin"):
            history.known_at(key(), {receipt.stream_key:1}, effective_cutoff_ns=200)
        self.assertEqual(history.at_version(later.version_id, {receipt.stream_key:1}), later)


class LegacyCompatibilityTests(unittest.TestCase):
    def test_verified_existing_api_bytes_unchanged_and_no_atomic_snapshot_claim(self):
        from option_collector.archive import Archive
        from option_collector.collector import context
        from option_collector.config import Config
        from tests.helpers import record
        with tempfile.TemporaryDirectory() as tmp:
            archive = Archive(tmp, context(Config(data_root=Path(tmp), require_ntp=False), "BTC", "snapshot"))
            archive.update_context(required_jobs=["index"], universe=["BTC-SYNTHETIC-"+str(i)+"-C" for i in range(2000)], universe_trusted=False)
            record(archive, "index", {}, raw=b'{"result":{"index_price":1.234567890123456789}}')
            archive.finalize()
            self.assertGreater((archive.destination/"manifest.json").stat().st_size, 65536)
            with self.assertRaises(ValueError):
                read_legacy_archive(archive.destination, include_incomplete=True, max_manifest_bytes=65536)
            before = {p.name: p.read_bytes() for p in archive.destination.iterdir() if p.is_file()}
            with self.assertRaises(ValueError):
                read_legacy_archive(archive.destination)
            rows = list(legacy_observations(archive.destination, include_incomplete=True))
            self.assertEqual(rows[0]["profile"], "rolling-sweep")
            self.assertIsNone(rows[0]["shared_asof"])
            self.assertEqual(rows[0]["streaming_coverage"], "unknown")
            self.assertEqual(before, {p.name:p.read_bytes() for p in archive.destination.iterdir() if p.is_file()})
