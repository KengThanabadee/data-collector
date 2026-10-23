from dataclasses import replace
import json
import unittest

from data_collector.contracts import canonical
from data_collector.validity import CloseGap, FeedTarget, GapCorrection, LedgerEntry, LedgerPin, LedgerStart, ValidityLedger
from tests.test_dc_contracts import key


def gap(**kwargs):
    values=dict(capture_scope='capture',target=FeedTarget(key(),'deribit_full','owner-1'),
        first_cutoff_id=2,recovery_cutoff_id=None,evidence_record_id='a'*64,
        proof_record_id='b'*64,reason='missing_native_predecessor')
    return GapCorrection(**(values|kwargs))


class ValidityTests(unittest.TestCase):
    def test_explicit_start_certifies_zero_corrections_and_prepare_never_mutates(self):
        ledger=ValidityLedger('ledger');start=LedgerEntry('ledger',0,None,LedgerStart('capture'))
        self.assertGreater(ledger.check(start),0)
        with self.assertRaises(ValueError):ledger.pin()
        ledger.add(LedgerEntry.from_bytes(start.to_bytes()));empty=ledger.pin()
        correction=LedgerEntry('ledger',1,start.entry_hash,gap())
        self.assertGreater(ledger.check(correction),0);self.assertEqual(ledger.pin(),empty)
        self.assertEqual(ledger.assess('capture',gap().target,3,'valid',empty)['assessed_validity'],'valid')
        with self.assertRaises(ValueError):LedgerEntry('ledger',1,start.entry_hash,LedgerStart('capture'))
        bad=json.loads(start.to_bytes());bad['payload']=list(bad['payload'].items())
        with self.assertRaises(ValueError):LedgerEntry.from_bytes(canonical(bad))

    def test_late_gap_downgrades_quiet_cutoffs_independent_of_admitted_position(self):
        ledger=ValidityLedger('ledger')
        correction=gap();ledger.add(LedgerEntry('ledger',0,None,correction));pin=ledger.pin()
        # Cutoffs2/3 may both admit the same last receipt100. Their cutoff IDs still
        # put them in the conservative interval: position==L is no exemption.
        for cutoff in (2,3,100):
            self.assertEqual(ledger.assess('capture',correction.target,cutoff,'valid',pin)['assessed_validity'],'invalid')
        self.assertEqual(ledger.assess('capture',correction.target,1,'valid',pin)['assessed_validity'],'valid')

    def test_recovery_closes_future_interval_not_missing_past_and_old_pin_reproducible(self):
        ledger=ValidityLedger('ledger');correction=gap()
        first=LedgerEntry('ledger',0,None,correction);ledger.add(first);old=ledger.pin()
        close=CloseGap(correction.correction_id,5,'c'*64,'d'*64)
        ledger.add(LedgerEntry('ledger',1,first.entry_hash,close));new=ledger.pin()
        self.assertEqual(ledger.assess('capture',correction.target,5,'valid',old)['assessed_validity'],'invalid')
        self.assertEqual(ledger.assess('capture',correction.target,5,'valid',new)['assessed_validity'],'valid')
        self.assertEqual(ledger.assess('capture',correction.target,4,'valid',new)['assessed_validity'],'invalid')
        self.assertEqual(ledger.assess('capture',correction.target,5,'missing',new)['assessed_validity'],'missing')

    def test_restored_prefix_retains_open_gap_and_cannot_claim_newer_pin(self):
        first=LedgerEntry('ledger',0,None,gap())
        original=ValidityLedger('ledger');original.add(first)
        original.add(LedgerEntry('ledger',1,first.entry_hash,CloseGap(gap().correction_id,5,'c'*64,'d'*64)))
        restored=ValidityLedger('ledger');restored.add(LedgerEntry.from_bytes(first.to_bytes()))
        self.assertEqual(restored.assess('capture',gap().target,10,'valid',restored.pin())['assessed_validity'],'invalid')
        with self.assertRaises(ValueError):restored.assess('capture',gap().target,10,'valid',original.pin())

    def test_prefix_fork_missing_duplicates_and_bounds_are_atomic(self):
        ledger=ValidityLedger('ledger',max_entries=1);entry=LedgerEntry('ledger',0,None,gap())
        ledger.add(entry);self.assertEqual(ledger.add(entry),entry.entry_hash)
        with self.assertRaises(ValueError):ledger.add(replace(entry,payload=gap(reason='conflicting')))
        with self.assertRaises(ValueError):ledger.add(LedgerEntry('ledger',2,'f'*64,gap(reason='later')))
        with self.assertRaises(ValueError):ledger.add(LedgerEntry('ledger',1,entry.entry_hash,gap(reason='later')))
        self.assertEqual(ledger.pin().head_hash,entry.entry_hash)

    def test_target_scope_owner_and_overlapping_gaps_do_not_leak_or_cancel(self):
        ledger=ValidityLedger('ledger');first=LedgerEntry('ledger',0,None,gap(recovery_cutoff_id=5))
        ledger.add(first);other=gap(first_cutoff_id=4,reason='second_gap')
        ledger.add(LedgerEntry('ledger',1,first.entry_hash,other));pin=ledger.pin()
        self.assertEqual(ledger.assess('capture',gap().target,5,'valid',pin)['assessed_validity'],'invalid')
        self.assertEqual(ledger.assess('another-capture',gap().target,5,'valid',pin)['assessed_validity'],'valid')
        self.assertEqual(ledger.assess('capture',replace(gap().target,owner_epoch='owner-2'),5,'valid',pin)['assessed_validity'],'valid')

    def test_codec_corruption_invalid_closure_and_empty_prefix_fail_closed(self):
        entry=LedgerEntry('ledger',0,None,gap());self.assertEqual(LedgerEntry.from_bytes(entry.to_bytes()),entry)
        value=json.loads(entry.to_bytes());value['payload']['first_cutoff_id']=999
        with self.assertRaises(ValueError):LedgerEntry.from_bytes(canonical(value))
        empty=ValidityLedger('ledger')
        with self.assertRaises(ValueError):empty.pin()
        empty.add(entry)
        with self.assertRaises(ValueError):empty.add(LedgerEntry('ledger',1,entry.entry_hash,CloseGap('e'*64,5,'c'*64,'d'*64)))
        with self.assertRaises(ValueError):empty.add(LedgerEntry('ledger',1,entry.entry_hash,CloseGap(gap().correction_id,1,'c'*64,'d'*64)))
        self.assertEqual(empty.pin().sequence,0)

    def test_oversized_entry_and_aggregate_byte_limit_cannot_create_unrestorable_pin(self):
        first=LedgerEntry('ledger',0,None,gap())
        ledger=ValidityLedger('ledger',max_bytes=len(first.to_bytes())+10)
        ledger.add(first);pin=ledger.pin()
        oversized=LedgerEntry('ledger',1,first.entry_hash,gap(reason='x'*65536))
        with self.assertRaisesRegex(ValueError,'codec byte bound'):oversized.to_bytes()
        with self.assertRaises(ValueError):ledger.add(oversized)
        self.assertEqual(ledger.pin(),pin)
        normal=LedgerEntry('ledger',1,first.entry_hash,gap(reason='second_gap'))
        self.assertEqual(LedgerEntry.from_bytes(normal.to_bytes()),normal)
        with self.assertRaisesRegex(ValueError,'projection byte bound'):ledger.add(normal)
        self.assertEqual(ledger.pin(),pin)
