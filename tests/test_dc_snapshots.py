from dataclasses import asdict,replace
from pathlib import Path
import json
import tempfile
import subprocess
import sys
import unittest
from unittest.mock import patch

from data_collector.contracts import canonical
from data_collector.journal import Journal
from data_collector.ledger import DurableLedger
from data_collector.segments import SegmentContext,iter_segment_records,seal_segment
from data_collector.snapshots import PublicationLimits,publish_snapshot,read_snapshot
from data_collector.validity import GapCorrection
from tests import test_dc_replay as fixtures
C=fixtures.C


class PublicationTests(unittest.TestCase):
    def setUp(self):
        self.fixtures=fixtures.ReplayTests();self.fixtures.setUp();self.addCleanup(self.fixtures.doCleanups)
        self.root=self.fixtures.root;self.store=self.root/'snapshots'

    def fixture(self,**kwargs):return self.fixtures.fixture(**kwargs)

    def correction(self,segment,plan,*,evidence=None):
        records=list(iter_segment_records(segment))
        native=[r for r in records if r.receipt.connection_id=='ws']
        close=next(r for r in records if r.kind=='control' and json.loads(r.body).get('kind')=='closed')
        sources=[segment]+([evidence] if evidence else [])
        observed=next(iter(iter_segment_records(evidence))) if evidence else native[-1]
        with Journal(self.root/'correction.wal',venue=close.venue,environment=close.environment,create=True) as wal:
            ledger=DurableLedger(wal,ledger_id='ledger',capture_scope='capture',capture_id='capture',
                process_session_id='later',provenance=close.provenance,
                prior_records=lambda:(record for path in sources for record in iter_segment_records(path)))
            pin=ledger.append(GapCorrection('capture',plan.bindings[0].target,0,None,observed.record_id,native[0].record_id,'late_gap'),
                utc_ns=C+100,monotonic_ns=100,clock_epoch='later-epoch')
            later=seal_segment(wal,self.root/'correction-archive',SegmentContext(close.venue,close.environment,
                'capture','later','later','a'*64,'b'*64))
        return later,pin

    def test_commit_and_raw_replay_preserve_exact_publication(self):
        segment,close,plan=self.fixture()
        first=publish_snapshot(self.store,[segment],close,plan)
        self.assertEqual(read_snapshot(self.store,first['snapshot_id'],[segment]),first)
        self.assertEqual(publish_snapshot(self.store,[segment],close,plan),first)
        self.assertEqual(json.loads(first['payload'])['books'][0]['validity_as_published'],'valid')
        self.assertEqual(first['descriptor']['publication_ledger_pin']['sequence'],0)

    def test_late_gap_before_publication_is_known_but_values_stay_original(self):
        segment,close,plan=self.fixture();later,pin=self.correction(segment,plan)
        result=publish_snapshot(self.store,[segment,later],close,plan)
        book=json.loads(result['payload'])['books'][0]
        self.assertEqual(book['validity_at_close'],'valid')
        self.assertEqual(book['validity_as_published'],'invalid')
        self.assertEqual(book['book']['bids'],[['100','4']])
        self.assertEqual(len(book['publication_corrections']),1)
        self.assertEqual(result['descriptor']['publication_ledger_pin']['head_hash'],pin.head_hash)

    def test_original_pin_and_bytes_survive_later_corrections_on_retry(self):
        segment,close,plan=self.fixture();first=publish_snapshot(self.store,[segment],close,plan)
        later,_=self.correction(segment,plan)
        self.assertEqual(read_snapshot(self.store,first['snapshot_id'],[segment,later]),first)
        self.assertEqual(publish_snapshot(self.store,[segment,later],close,plan),first)
        with self.assertRaises(ValueError):
            publish_snapshot(self.store,[segment,later],close,replace(plan,config_sha256='f'*64))

    def test_missing_raw_or_correction_prefix_cannot_certify_published_snapshot(self):
        segment,close,plan=self.fixture();later,_=self.correction(segment,plan)
        first=publish_snapshot(self.store,[segment,later],close,plan)
        with self.assertRaises(ValueError):read_snapshot(self.store,first['snapshot_id'],[segment])
        with self.assertRaises(ValueError):read_snapshot(self.store,first['snapshot_id'],[later])

    def test_interruption_after_each_durable_write_retries_idempotently(self):
        from data_collector.snapshots import _publish_bytes
        segment,close,plan=self.fixture()
        for stage in range(1,5):
            store=self.root/('interrupt-'+str(stage));calls=0
            def interrupted(path,data):
                nonlocal calls
                _publish_bytes(path,data);calls+=1
                if calls==stage:raise OSError('injected interruption after durable write')
            with patch('data_collector.snapshots._publish_bytes',side_effect=interrupted):
                with self.assertRaises(OSError):publish_snapshot(store,[segment],close,plan)
            if stage<4:self.assertFalse((store/'closures'/(close+'.json')).exists())
            result=publish_snapshot(store,[segment],close,plan)
            self.assertEqual(read_snapshot(store,result['snapshot_id'],[segment]),result)

    def test_tamper_and_conflicting_index_fail_without_overwrite(self):
        segment,close,plan=self.fixture();first=publish_snapshot(self.store,[segment],close,plan)
        payload=self.store/'objects'/first['snapshot_id']/'payload.json'
        payload.write_bytes(b'{}')
        with self.assertRaisesRegex(ValueError,'commit/hash'):read_snapshot(self.store,first['snapshot_id'],[segment])
        with self.assertRaises(ValueError):publish_snapshot(self.store,[segment],close,plan)
        self.assertEqual(payload.read_bytes(),b'{}')

    def test_publication_limits_fail_before_any_commit(self):
        segment,close,plan=self.fixture()
        with self.assertRaisesRegex(ValueError,'metadata bound'):
            publish_snapshot(self.store,[segment],close,plan,limits=PublicationLimits(max_descriptor_bytes=1))
        self.assertFalse(self.store.exists())

    def test_original_read_ignores_later_correction_with_unavailable_later_evidence(self):
        segment,close,plan=self.fixture();first=publish_snapshot(self.store,[segment],close,plan)
        original=next(r for r in iter_segment_records(segment) if r.receipt.connection_id=='ws')
        receipt=replace(original.receipt,process_session_id='later',connection_id='evidence',receive_seq=0)
        with Journal(self.root/'evidence.wal',venue=original.venue,environment=original.environment,create=True) as wal:
            wal.append(replace(original,receipt=receipt,body=b'{"later_evidence":true}'))
            evidence=seal_segment(wal,self.root/'evidence-archive',SegmentContext(original.venue,original.environment,
                'capture','later','evidence','a'*64,'b'*64))
        later,_=self.correction(segment,plan,evidence=evidence)
        self.assertEqual(read_snapshot(self.store,first['snapshot_id'],[segment,later]),first)
        self.assertEqual(publish_snapshot(self.store,[segment,later],close,plan),first)

    def test_retry_completes_directory_sync_after_index_link_survives(self):
        from data_collector.journal import sync_directory
        segment,close,plan=self.fixture()
        def fail_index_directory(path):
            if Path(path)==self.store/'closures':raise OSError('injected directory sync failure')
            sync_directory(path)
        with patch('data_collector.journal.sync_directory',side_effect=fail_index_directory):
            with self.assertRaises(OSError):publish_snapshot(self.store,[segment],close,plan)
        self.assertTrue((self.store/'closures'/(close+'.json')).exists())
        with patch('data_collector.journal.sync_directory',wraps=sync_directory) as sync:
            result=publish_snapshot(self.store,[segment],close,plan)
        sync.assert_any_call(self.store/'closures')
        self.assertEqual(read_snapshot(self.store,result['snapshot_id'],[segment]),result)

    def test_process_exit_after_each_commit_step_recovers_same_snapshot(self):
        segment,close,plan=self.fixture();plan_file=self.root/'plan.json';plan_file.write_bytes(canonical(asdict(plan)))
        code='''import json,os,sys
from pathlib import Path
from data_collector import snapshots as m
store,segment,close,plan_path,stage=sys.argv[1:]
plan=m.plan_from_descriptor(json.loads(Path(plan_path).read_bytes()))
original=m._publish_bytes
count=0
def killed(path,data):
    global count
    original(path,data)
    count+=1
    if count==int(stage):os._exit(77)
m._publish_bytes=killed
m.publish_snapshot(store,[segment],close,plan)
'''
        for stage in range(1,5):
            store=self.root/('killed-'+str(stage))
            child=subprocess.run([sys.executable,'-c',code,str(store),str(segment),close,str(plan_file),str(stage)],
                capture_output=True,timeout=15)
            self.assertEqual(child.returncode,77,child.stderr.decode())
            if stage<4:self.assertFalse((store/'closures'/(close+'.json')).exists())
            result=publish_snapshot(store,[segment],close,plan)
            self.assertEqual(read_snapshot(store,result['snapshot_id'],[segment]),result)


if __name__=='__main__':unittest.main()
