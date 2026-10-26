"""Publication knowledge may differ from close knowledge; originals never change.

Explicit synthetic CloseGap controls isolate publication plumbing here. These
fixtures do not certify a native recovery selection/positive-proof mapper.
"""
from dataclasses import asdict
import unittest

from data_collector.contracts import digest,strict_json
from data_collector.exports import export_snapshots,read_export
from data_collector.journal import Journal
from data_collector.ledger import DurableLedger
from data_collector.replay import materialize_snapshot
from data_collector.segments import SegmentContext,iter_segment_records,seal_segment
from data_collector.snapshots import PublicationLimits,_commit,_publication,publish_snapshot,read_snapshot
from data_collector.validity import CloseGap,GapCorrection,LedgerEntry
from tests import test_dc_replay as fixtures

C=fixtures.C


class PublicationValidityTests(unittest.TestCase):
    def setUp(self):
        self.fixtures=fixtures.ReplayTests();self.fixtures.setUp();self.addCleanup(self.fixtures.doCleanups)
        self.root=self.fixtures.root;self.store=self.root/'snapshots'

    def fixture(self,**kwargs):
        options=dict(native_gap=True,known_gap=True,recover=True,second_cutoff=True)
        options.update(kwargs)
        return self.fixtures.fixture(**options)

    def resolution(self,segment):
        records=tuple(iter_segment_records(segment))
        gap=next(LedgerEntry.from_bytes(r.body).payload for r in records if r.channel=='validity:capture'
                 and isinstance(LedgerEntry.from_bytes(r.body).payload,GapCorrection))
        latest=next(r for r in reversed(records) if r.receipt.connection_id=='ws')
        close=next(r for r in reversed(records) if r.channel=='cutoffs:capture')
        work=self.root/('resolution-'+str(self.fixtures.counter));work.mkdir()
        with Journal(work/'resolution.wal',venue=close.venue,environment=close.environment,create=True) as wal:
            ledger=DurableLedger(wal,ledger_id='ledger',capture_scope='capture',capture_id='capture',
                process_session_id='resolution',provenance=close.provenance,prior_records=lambda:iter_segment_records(segment))
            pin=ledger.append(CloseGap(gap.correction_id,1,latest.record_id,close.record_id),
                utc_ns=2*C+10,monotonic_ns=10,clock_epoch='resolution')
            path=seal_segment(wal,work/'archive',SegmentContext(close.venue,close.environment,
                'capture','resolution','resolution','a'*64,'b'*64))
        return path,pin

    def book(self,result):return strict_json(result['payload'])['books'][0]

    def test_resolution_before_first_publication_uses_its_pin_without_repairing_history(self):
        segment,closes,plan=self.fixture();later,pin=self.resolution(segment);paths=[segment,later]
        result=publish_snapshot(self.store,paths,closes[1],plan)
        book=self.book(result)
        self.assertEqual((book['validity_at_close'],book['validity_as_published']),('invalid','valid'))
        self.assertEqual(book['publication_corrections'],[])
        self.assertEqual(book['book']['bids'],[['100','9']])
        self.assertEqual(result['descriptor']['publication_policy'],'ledger-at-start/2')
        self.assertEqual(result['descriptor']['publication_ledger_pin'],asdict(pin))
        historical=publish_snapshot(self.store,paths,closes[0],plan)
        self.assertEqual(self.book(historical)['validity_as_published'],'invalid')
        exported=export_snapshots(self.store,self.root/'exports',[result['snapshot_id'],historical['snapshot_id']],paths)
        self.assertEqual(exported['descriptor']['population'],dict(valid=1,invalid=1,unknown=0,missing=0))
        self.assertEqual(read_export(self.root/'exports',exported['export_id'],paths),exported)
        self.assertEqual(read_snapshot(self.store,result['snapshot_id'],paths),result)
        with self.assertRaises(ValueError):read_snapshot(self.store,result['snapshot_id'],[segment])

    def test_published_invalid_is_immutable_and_export_cannot_upgrade_it(self):
        segment,closes,plan=self.fixture()
        original=publish_snapshot(self.store,[segment],closes[1],plan)
        self.assertEqual(self.book(original)['validity_as_published'],'invalid')
        later,_=self.resolution(segment);paths=[segment,later]
        self.assertEqual(publish_snapshot(self.store,paths,closes[1],plan),original)
        self.assertEqual(read_snapshot(self.store,original['snapshot_id'],paths),original)
        exported=export_snapshots(self.store,self.root/'exports',[original['snapshot_id']],paths)
        self.assertEqual(exported['descriptor']['population']['invalid'],1)
        self.assertEqual(read_export(self.root/'exports',exported['export_id'],paths),exported)
        fresh=publish_snapshot(self.root/'fresh',paths,closes[1],plan)
        self.assertEqual(self.book(fresh)['validity_as_published'],'valid')
        self.assertNotEqual(original['snapshot_id'],fresh['snapshot_id'])

    def test_resolution_never_overrides_native_metadata_or_barrier_failures(self):
        for options,expected in [(dict(recover=False),'invalid'),(dict(new_metadata=True),'invalid'),(dict(healthy=False),'missing')]:
            with self.subTest(options=options):
                segment,closes,plan=self.fixture(**options);later,_=self.resolution(segment)
                store=self.root/('case-'+str(self.fixtures.counter))
                result=publish_snapshot(store,[segment,later],closes[1],plan)
                self.assertEqual(self.book(result)['validity_as_published'],expected)
                self.assertEqual(read_snapshot(store,result['snapshot_id'],[segment,later]),result)

    def test_legacy_bytes_and_mixed_publication_policy_exports_replay_exactly(self):
        segment,closes,plan=self.fixture()
        base=materialize_snapshot([segment],closes[1],plan)
        # Pinned from unmodified cba0881 code before this change.
        self.assertEqual(base['snapshot_id'],'0f6371bc806af4ddd2af8ca82ba53226a38923f2849921620a0c626a66c8b2cf')
        legacy=_publication(base,[segment],limits=PublicationLimits().replay)
        self.assertEqual(legacy['snapshot_id'],'1e2fa7473feaa24c3e343a7008e9c3deb8a46eb1b08af23df6a854104a1a33cf')
        self.assertEqual(digest(legacy['payload']),'9f6b38f750fcbe347103bd2f3c03a9789cb9d1dc223e893b5ddfbe20161191df')
        _commit(self.root/'legacy',legacy,PublicationLimits())
        self.assertEqual(read_snapshot(self.root/'legacy',legacy['snapshot_id'],[segment]),legacy)
        old_base=materialize_snapshot([segment],closes[0],plan)
        old=_publication(old_base,[segment],limits=PublicationLimits().replay)
        _commit(self.store,old,PublicationLimits())
        later,_=self.resolution(segment);paths=[segment,later]
        new=publish_snapshot(self.store,paths,closes[1],plan)
        self.assertEqual(read_snapshot(self.store,old['snapshot_id'],paths),old)
        for mode in ('as_published','assessed_at_ledger'):
            root=self.root/mode
            exported=export_snapshots(self.store,root,[old['snapshot_id'],new['snapshot_id']],paths,mode=mode)
            self.assertEqual(exported['descriptor']['population'],dict(valid=1,invalid=1,unknown=0,missing=0))
            self.assertEqual(read_export(root,exported['export_id'],paths),exported)
