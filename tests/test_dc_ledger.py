from dataclasses import replace
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

from data_collector.contracts import RawRecord
from data_collector.journal import Journal
from data_collector.ledger import DurableLedger
from data_collector.segments import SegmentContext,iter_segment_records,seal_segment
from data_collector.validity import CloseGap,LedgerEntry,LedgerStart
from tests.test_dc_contracts import raw
from tests.test_dc_validity import gap


class DurableLedgerTests(unittest.TestCase):
    def setUp(self):
        self.tmp=tempfile.TemporaryDirectory();self.addCleanup(self.tmp.cleanup)
        self.path=Path(self.tmp.name)/'active.wal'
        self.wal=Journal(self.path,venue='deribit',environment='production',create=True)
        self.addCleanup(lambda:self.wal.close())

    def ledger(self,session='process',**kw):
        return DurableLedger(self.wal,ledger_id='ledger',capture_scope='capture',capture_id='capture',
            process_session_id=session,provenance=raw().provenance,**kw)

    def initialize(self,ledger):return ledger.initialize(utc_ns=100,monotonic_ns=50,clock_epoch='epoch')

    def evidence(self):
        proof=raw(0);discovery=raw(1)
        self.wal.append(proof);self.wal.append(discovery)
        return gap(proof_record_id=proof.record_id,evidence_record_id=discovery.record_id)

    def append(self,ledger,payload):return ledger.append(payload,utc_ns=101,monotonic_ns=51,clock_epoch='epoch')

    def test_empty_known_prefix_then_gap_old_pin_and_recovery_are_durable(self):
        ledger=self.ledger();empty=self.initialize(ledger)
        self.assertEqual(empty.sequence,0);self.assertEqual(self.initialize(ledger),empty)
        correction=self.evidence();opened=self.append(ledger,correction)
        self.assertEqual(ledger.assess('capture',correction.target,4,'valid',empty)['assessed_validity'],'valid')
        self.assertEqual(ledger.assess('capture',correction.target,4,'valid',opened)['assessed_validity'],'invalid')
        recovery=raw(2);self.wal.append(recovery)
        closed=self.append(ledger,CloseGap(correction.correction_id,5,recovery.record_id,recovery.record_id))
        self.assertEqual(ledger.assess('capture',correction.target,4,'valid',closed)['assessed_validity'],'invalid')
        self.assertEqual(ledger.assess('capture',correction.target,5,'valid',closed)['assessed_validity'],'valid')
        self.wal.close();self.wal=Journal(self.path,venue='deribit',environment='production')
        restored=self.ledger('restart')
        self.assertEqual(restored.pin(),closed)
        self.assertEqual(restored.assess('capture',correction.target,5,'valid',opened)['assessed_validity'],'invalid')
        self.assertEqual(len(list(restored.entries(empty))),1)

    def test_missing_evidence_wrong_scope_or_venue_rejected_before_write(self):
        ledger=self.ledger();pin=self.initialize(ledger)
        before=self.path.read_bytes()
        with self.assertRaisesRegex(ValueError,'evidence missing'):self.append(ledger,gap())
        self.assertEqual(before,self.path.read_bytes());self.assertEqual(ledger.pin(),pin)
        correction=self.evidence();before=self.path.read_bytes()
        with self.assertRaisesRegex(ValueError,'scope mismatch'):self.append(ledger,replace(correction,capture_scope='foreign'))
        target=replace(correction.target,instrument=replace(correction.target.instrument,venue='bybit'))
        with self.assertRaisesRegex(ValueError,'venue differs'):self.append(ledger,replace(correction,target=target))
        self.assertEqual(before,self.path.read_bytes())

    def test_explicit_foreign_instrument_cannot_supply_proof_or_recovery(self):
        ledger=self.ledger();self.initialize(ledger);correction=self.evidence()
        foreign=raw(2,instrument=replace(raw().instrument,native_symbol='ETH-OTHER-C'))
        self.wal.append(foreign);before=self.path.read_bytes()
        with self.assertRaisesRegex(ValueError,'instrument differs'):
            self.append(ledger,replace(correction,proof_record_id=foreign.record_id))
        self.assertEqual(before,self.path.read_bytes())
        opened=self.append(ledger,correction);before=self.path.read_bytes()
        with self.assertRaisesRegex(ValueError,'instrument differs'):
            self.append(ledger,CloseGap(correction.correction_id,5,foreign.record_id,foreign.record_id))
        self.assertEqual(before,self.path.read_bytes())
        self.assertEqual(ledger.assess('capture',correction.target,5,'valid',opened)['assessed_validity'],'invalid')

    def test_fsync_failure_never_exposes_new_pin_and_survivor_replays(self):
        ledger=self.ledger();self.initialize(ledger);correction=self.evidence()
        with patch('data_collector.journal.os.fsync',side_effect=OSError('injected fsync')):
            with self.assertRaises(OSError):self.append(ledger,correction)
        with self.assertRaisesRegex(ValueError,'reopened'):ledger.pin()
        self.wal.close();self.wal=Journal(self.path,venue='deribit',environment='production')
        restored=self.ledger('restart');pin=restored.pin()
        self.assertEqual(pin.sequence,1)
        self.assertEqual(restored.assess('capture',correction.target,3,'valid',pin)['assessed_validity'],'invalid')

    def test_open_gap_and_unique_control_receipts_survive_segment_rotation(self):
        ledger=self.ledger();self.initialize(ledger);correction=self.evidence();opened=self.append(ledger,correction)
        context=SegmentContext('deribit','production','capture','process','first',
            raw().provenance.source_sha256,raw().provenance.config_sha256)
        segment=seal_segment(self.wal,Path(self.tmp.name)/'archive',context)
        controls=[r for r in iter_segment_records(segment) if r.kind=='coverage_event']
        self.wal.close();self.wal=Journal(Path(self.tmp.name)/'next.wal',venue='deribit',environment='production',create=True)
        restored=self.ledger(prior_records=lambda:iter_segment_records(segment))
        self.assertEqual(restored.pin(),opened)
        recovery=raw(2);self.wal.append(recovery)
        self.append(restored,CloseGap(correction.correction_id,5,recovery.record_id,recovery.record_id))
        new=[RawRecord.from_bytes(b) for b in self.wal.encoded_records(allow_unsealed=True) if RawRecord.from_bytes(b).kind=='coverage_event']
        self.assertEqual(new[0].receipt.receive_seq,2)
        self.assertNotIn(new[0].record_id,{r.record_id for r in controls})

    def test_restore_missing_prefix_and_evidence_fails_instead_of_valid_assessment(self):
        ledger=self.ledger();self.initialize(ledger);correction=self.evidence();opened=self.append(ledger,correction)
        records=[RawRecord.from_bytes(b) for b in self.wal.encoded_records(allow_unsealed=True)]
        self.wal.close();self.wal=Journal(Path(self.tmp.name)/'empty.wal',venue='deribit',environment='production',create=True)
        for omitted in [records[0].record_id,correction.proof_record_id]:
            with self.assertRaises(ValueError):
                self.ledger('restart',prior_records=lambda:iter([r for r in records if r.record_id!=omitted]))
        prefix=self.ledger('restart',prior_records=lambda:iter(records[:-1]))
        with self.assertRaises(ValueError):prefix.assess('capture',correction.target,4,'valid',opened)

    def test_no_cached_pin_when_verified_dependency_factory_loses_prefix(self):
        source=[];ledger=self.ledger(prior_records=lambda:iter(source));self.initialize(ledger)
        records=[RawRecord.from_bytes(b) for b in self.wal.encoded_records(allow_unsealed=True)]
        self.wal.close();self.wal=Journal(Path(self.tmp.name)/'empty.wal',venue='deribit',environment='production',create=True)
        source[:]=records
        restored=self.ledger('restart',prior_records=lambda:iter(source));restored.pin()
        source.clear()
        with self.assertRaises(ValueError):restored.pin()

    def test_known_head_and_external_restore_pin_cannot_silently_regress(self):
        ledger=self.ledger();self.initialize(ledger);correction=self.evidence();opened=self.append(ledger,correction)
        source=[RawRecord.from_bytes(b) for b in self.wal.encoded_records(allow_unsealed=True)]
        self.wal.close();self.wal=Journal(Path(self.tmp.name)/'empty.wal',venue='deribit',environment='production',create=True)
        restored=self.ledger('restart',prior_records=lambda:iter(source));self.assertEqual(restored.pin(),opened)
        source.pop() # Genesis and raw evidence survive, only final gap control lost.
        with self.assertRaisesRegex(ValueError,'pinned ledger prefix'):restored.pin()
        with self.assertRaisesRegex(ValueError,'pinned ledger prefix'):
            self.ledger('restart',prior_records=lambda:iter(source),expected_pin=opened)
        # A fresh older copy can state its older knowledge bound explicitly.
        older=self.ledger('older',prior_records=lambda:iter(source))
        self.assertEqual(older.pin().sequence,0)

    def test_pinned_dependencies_include_controls_and_exact_raw_evidence_only(self):
        ledger=self.ledger();empty=self.initialize(ledger);correction=self.evidence();opened=self.append(ledger,correction)
        old=ledger.dependencies(empty);new=ledger.dependencies(opened)
        self.assertEqual(len(old),1);self.assertEqual(len(new),4)
        self.assertTrue({correction.proof_record_id,correction.evidence_record_id}.issubset({d.record_id for d in new}))
        self.assertEqual(old,ledger.dependencies(empty))

    def test_bounds_are_checked_before_appending_and_legacy_gap_only_prefix_rejected(self):
        ledger=self.ledger(max_entries=1);self.initialize(ledger);correction=self.evidence();before=self.path.read_bytes()
        with self.assertRaisesRegex(ValueError,'projection bound'):self.append(ledger,correction)
        self.assertEqual(self.path.read_bytes(),before)
        with self.assertRaisesRegex(ValueError,'scan bound'):self.ledger(max_scan_records=1)
        with self.assertRaisesRegex(ValueError,'evidence projection bound'):self.ledger(max_evidence_records=1)
        self.assertEqual(LedgerEntry.from_bytes(LedgerEntry('x',0,None,LedgerStart('scope')).to_bytes()).payload,LedgerStart('scope'))

    def test_prospective_scan_limits_never_acknowledge_an_unreadable_new_pin(self):
        tiny=self.ledger(max_scan_bytes=1);before=self.path.read_bytes()
        with self.assertRaisesRegex(ValueError,'proposed control'):self.initialize(tiny)
        self.assertEqual(before,self.path.read_bytes())
        bounded=self.ledger(max_scan_records=3);self.initialize(bounded);correction=self.evidence()
        before=self.path.read_bytes();old=bounded.pin()
        with self.assertRaisesRegex(ValueError,'proposed control'):self.append(bounded,correction)
        self.assertEqual(before,self.path.read_bytes());self.assertEqual(bounded.pin(),old)
        restored=self.ledger('restart',max_scan_records=3);self.assertEqual(restored.pin(),old)


if __name__=='__main__':unittest.main()
