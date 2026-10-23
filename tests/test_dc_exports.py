from dataclasses import replace
import json
from pathlib import Path
import shutil
import unittest
from unittest.mock import patch

from data_collector.contracts import canonical
from data_collector.exports import ExportLimits,export_snapshots,read_export
from data_collector.snapshots import publish_snapshot
from data_collector.validity import LedgerPin
from tests import test_dc_snapshots as fixtures


class ExportTests(unittest.TestCase):
    def setUp(self):
        self.fixtures=fixtures.PublicationTests();self.fixtures.setUp();self.addCleanup(self.fixtures.doCleanups)
        self.root=self.fixtures.root;self.store=self.fixtures.store;self.exports=self.root/'exports'

    def fixture(self,**kwargs):
        segment,close,plan=self.fixtures.fixture(**kwargs)
        original=publish_snapshot(self.store,[segment],close,plan)
        return segment,plan,original

    def export(self,ids,segments,**kwargs):return export_snapshots(self.store,self.exports,ids,segments,**kwargs)

    def test_default_export_pins_latest_durable_ledger_without_changing_original_quotes(self):
        segment,plan,original=self.fixture()
        later,pin=self.fixtures.correction(segment,plan)
        result=self.export([original['snapshot_id']],[segment,later])
        self.assertEqual(result['descriptor']['ledger_pin']['head_hash'],pin.head_hash)
        self.assertEqual(result['descriptor']['population'],dict(valid=0,invalid=1,unknown=0,missing=0))
        row=json.loads(result['payload'])['observations'][0]
        self.assertEqual(canonical(row['original_payload']),original['payload'])
        self.assertEqual(row['assessments'][0]['assessed_validity'],'invalid')
        self.assertEqual(row['original_payload']['books'][0]['validity_as_published'],'valid')
        self.assertEqual(len(result['descriptor']['ledger_entries']),2)
        self.assertEqual(read_export(self.exports,result['export_id'],[segment,later]),result)

    def test_old_pinned_export_stays_identical_when_later_corrections_arrive(self):
        segment,plan,original=self.fixture()
        before=self.export([original['snapshot_id']],[segment])
        pin=LedgerPin(**before['descriptor']['ledger_pin'])
        later,_=self.fixtures.correction(segment,plan)
        self.assertEqual(self.export([original['snapshot_id']],[segment,later],ledger_pin=pin),before)
        self.assertEqual(read_export(self.exports,before['export_id'],[segment,later]),before)
        after=self.export([original['snapshot_id']],[segment,later])
        self.assertNotEqual(after['export_id'],before['export_id'])
        self.assertEqual(after['descriptor']['population']['invalid'],1)

    def test_original_mode_preserves_original_bytes_and_is_distinct_from_assessment(self):
        segment,plan,original=self.fixture();later,_=self.fixtures.correction(segment,plan)
        result=self.export([original['snapshot_id']],[segment,later],mode='as_published')
        row=json.loads(result['payload'])['observations'][0]
        self.assertEqual(canonical(row['original_payload']),original['payload'])
        self.assertEqual(row['assessments'],[])
        self.assertIsNone(result['descriptor']['ledger_pin'])
        self.assertEqual(result['descriptor']['population']['valid'],1)
        self.assertEqual(read_export(self.exports,result['export_id'],[segment]),result)

    def test_portable_export_needs_no_original_snapshot_directory(self):
        segment,_,original=self.fixture()
        result=self.export([original['snapshot_id']],[segment])
        shutil.rmtree(self.store)
        self.assertEqual(read_export(self.exports,result['export_id'],[segment]),result)

    def test_affected_quiet_cutoffs_remain_in_population_and_order_is_deterministic(self):
        segment,closes,plan=self.fixtures.fixture(second_cutoff=True)
        originals=[publish_snapshot(self.store,[segment],close,plan) for close in closes]
        later,_=self.fixtures.correction(segment,plan)
        ids=[row['snapshot_id'] for row in originals]
        result=self.export(ids,[segment,later])
        self.assertEqual(result,self.export(reversed(ids),[segment,later]))
        self.assertEqual(result['descriptor']['population']['invalid'],2)
        self.assertEqual(len(json.loads(result['payload'])['observations']),2)

    def test_original_invalid_rows_are_retained(self):
        segment,_,original=self.fixture(native_gap=True)
        result=self.export([original['snapshot_id']],[segment])
        self.assertEqual(result['descriptor']['population']['invalid'],1)
        self.assertEqual(len(json.loads(result['payload'])['observations']),1)

    def test_missing_prefix_and_forged_pin_fail_instead_of_returning_valid(self):
        segment,plan,original=self.fixture();later,pin=self.fixtures.correction(segment,plan)
        with self.assertRaises(ValueError):self.export([original['snapshot_id']],[segment],ledger_pin=pin)
        with self.assertRaises(ValueError):self.export([original['snapshot_id']],[segment,later],ledger_pin=replace(pin,head_hash='f'*64))
        result=self.export([original['snapshot_id']],[segment,later])
        with self.assertRaises(ValueError):read_export(self.exports,result['export_id'],[segment])

    def test_population_and_byte_limits_fail_before_commit(self):
        segment,_,original=self.fixture();ids=[original['snapshot_id']]
        for limit in [ExportLimits(max_payload_bytes=1),ExportLimits(max_descriptor_bytes=1)]:
            with self.assertRaises(ValueError):self.export(ids,[segment],limits=limit)
        self.assertFalse(self.exports.exists())
        with self.assertRaises(ValueError):self.export(ids+ids,[segment])
        with self.assertRaises(ValueError):self.export(ids,[segment],mode='ambiguous')

    def test_manifest_is_final_marker_and_retry_completes_identically(self):
        from data_collector.exports import _publish_bytes
        segment,_,original=self.fixture()
        for stage in range(1,4):
            root=self.root/('fault-'+str(stage));count=0
            def interrupted(path,data):
                nonlocal count
                _publish_bytes(path,data);count+=1
                if count==stage:raise OSError('injected after export durable write')
            with patch('data_collector.exports._publish_bytes',side_effect=interrupted):
                with self.assertRaises(OSError):export_snapshots(self.store,root,[original['snapshot_id']],[segment])
            result=export_snapshots(self.store,root,[original['snapshot_id']],[segment])
            self.assertEqual(read_export(root,result['export_id'],[segment]),result)

    def test_tamper_is_detected_and_not_repaired_by_overwrite(self):
        segment,_,original=self.fixture();result=self.export([original['snapshot_id']],[segment])
        path=self.exports/result['export_id']/'payload.json';path.write_bytes(b'{}')
        with self.assertRaises(ValueError):read_export(self.exports,result['export_id'],[segment])
        with self.assertRaises(ValueError):self.export([original['snapshot_id']],[segment])
        self.assertEqual(path.read_bytes(),b'{}')


if __name__=='__main__':unittest.main()
