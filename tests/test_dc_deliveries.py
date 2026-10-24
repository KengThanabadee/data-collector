from dataclasses import replace
import unittest

from data_collector.contracts import RawRecord,Receipt,canonical,digest,strict_json
from data_collector.deliveries import delivery_base,validate_delivery,replay_delivery_reports
from tests import test_dc_publisher as fixtures


class DeliveryTests(unittest.TestCase):
    def setUp(self):
        self.fixture=fixtures.PublisherTests();self.fixture.setUp()
        self.addCleanup(self.fixture.tmp.cleanup);self.addCleanup(self.fixture.wal.close)
        self.inputs=self.fixture.inputs
        observation=dict(utc_ns=fixtures.C,monotonic_ns=1100,process_timing_epoch='process-clock',valid=True)
        self.value=dict(delivery_base(self.inputs,self.fixture.plan),started=observation,completed=observation,
            outcome='published',reason='on_time',snapshot_id='f'*64,error_type=None)
        self.body=canonical(self.value)
        self.record=RawRecord(Receipt('capture','process','control:snapshot-deliveries:capture',0,fixtures.C,1100,'process:clock:0'),
            'deribit','production','control','snapshot_deliveries:capture','control',self.body,self.fixture.clock.provenance)

    def replay(self,records,**kwargs):
        return replay_delivery_reports(records,origin=self.inputs.origin,**kwargs)

    def test_exact_receipt_and_raw_hashes_are_reported_without_claiming_artifact_verification(self):
        self.assertEqual(validate_delivery(self.body),self.value)
        result=self.replay([self.record])[0]
        self.assertEqual(result.receipt_sha256,digest(self.body))
        self.assertEqual(result.raw_sha256,digest(self.record.to_bytes()))
        self.assertEqual(result.outcome,'published') # claimed; no artifact files needed

    def test_failed_reports_keep_invalid_or_absent_completion_observations(self):
        for observation in (dict(utc_ns=None,monotonic_ns=None,process_timing_epoch=None,valid=False),
                            dict(utc_ns=0,monotonic_ns=0,process_timing_epoch='changed',valid=False)):
            value=dict(self.value,completed=observation,outcome='failed',reason='process_clock_invalid',snapshot_id=None)
            body=canonical(value)
            self.assertEqual(self.replay([replace(self.record,body=body)])[0].reason,'process_clock_invalid')

    def test_contract_rejects_bad_shape_types_hashes_timing_and_unbounded_arrays(self):
        cases=[dict(self.value,extra=1),dict(self.value,cutoff_id=True),dict(self.value,close_sha256='x'),
            dict(self.value,input_segments=self.value['input_segments']*1025),dict(self.value,input_segments=[]),
            dict(self.value,publication_deadline_ns=1),dict(self.value,outcome='failed'),
            dict(self.value,completed=dict(self.value['completed'],monotonic_ns=1099)),
            dict(self.value,completed=dict(self.value['completed'],valid=False)),
            dict(self.value,reason=[]),dict(self.value,outcome={}),
            dict(self.value,input_clock_pin=dict(self.value['input_clock_pin'],count=0)),
            dict(self.value,origin=dict(self.value['origin'],capture_id='x'*129))]
        for value in cases:
            with self.subTest(value=value.keys()),self.assertRaises(ValueError):validate_delivery(canonical(value))
        for body in (self.body+b' ',self.body.replace(b'"schema":',b'"schema" :'),b'x'*65537):
            with self.assertRaises(ValueError):validate_delivery(body)

    def test_replay_rejects_duplicate_conflicting_reports_or_sequence_holes(self):
        for second in (self.record,replace(self.record,receipt=replace(self.record.receipt,receive_seq=1)),
                       replace(self.record,receipt=replace(self.record.receipt,receive_seq=2))):
            with self.assertRaises(ValueError):self.replay([self.record,second])

    def test_replay_checks_origin_namespace_and_archive_completion_order(self):
        variants=[replace(self.record,channel='wrong'),replace(self.record,kind='ws_message'),
            replace(self.record,receipt=replace(self.record.receipt,connection_id='wrong')),
            replace(self.record,receipt=replace(self.record.receipt,monotonic_ns=1099)),
            replace(self.record,venue='bybit'),replace(self.record,native_fields_json=b'{"extra":1}'),
            replace(self.record,body=canonical(dict(self.value,origin=dict(self.value['origin'],capture_scope='foreign'))))]
        for record in variants:
            with self.assertRaises(ValueError):self.replay([record])

    def test_replay_scan_count_and_retained_report_bounds_fail_without_partial_result(self):
        unrelated=replace(self.record,channel='other',receipt=replace(self.record.receipt,connection_id='other'))
        for records,limits in (([unrelated,self.record],dict(max_records=1)),([self.record],dict(max_scan_bytes=1)),
            ([self.record,replace(self.record,body=canonical(dict(self.value,close_record_id='e'*64)),
                receipt=replace(self.record.receipt,receive_seq=1))],dict(max_reports=1)),
            ([self.record],dict(max_report_bytes=1))):
            with self.assertRaises(ValueError):self.replay(records,**limits)
        self.assertEqual(self.replay([unrelated]),())

    def test_archive_monotonic_regression_or_overflow_is_rejected(self):
        first=replace(self.record,receipt=replace(self.record.receipt,monotonic_ns=1200))
        second=replace(self.record,body=canonical(dict(self.value,close_record_id='e'*64)),
            receipt=replace(self.record.receipt,receive_seq=1,monotonic_ns=1199))
        with self.assertRaises(ValueError):self.replay([first,second])
        with self.assertRaises(ValueError):self.replay([replace(self.record,
            receipt=replace(self.record.receipt,monotonic_ns=2**63))])
