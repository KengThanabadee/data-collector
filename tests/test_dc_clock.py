from dataclasses import replace
import unittest

from data_collector.clock import ClockMonitor, ClockRestartRequired, ClockSample
from data_collector.cutoffs import IngressWriter
from tests.test_dc_contracts import raw


class ClockTests(unittest.TestCase):
    def setUp(self):
        self.monitor = ClockMonitor('process', sample_interval_ns=60, max_sample_age_ns=60,
                                    max_offset_ns=100, step_tolerance_ns=2)

    def sample(self, mono, **kwargs):
        return ClockSample(1000+mono, mono, kwargs.get('offset',0), kwargs.get('uncertainty',0),
                           kwargs.get('boot','boot'), 'a'*64)

    def test_two_spaced_samples_preserve_exact_utc_and_offset_bound(self):
        first=self.monitor.sample(self.sample(10,offset=50,uncertainty=20),ingress_ordinal=0)
        self.assertFalse(first.aligned)
        second=self.monitor.sample(self.sample(70,offset=50,uncertainty=20),ingress_ordinal=4)
        self.assertTrue(second.aligned)
        self.assertEqual(second.anchor.utc_ns,1070)
        self.assertEqual(second.anchor.uncertainty_ns,70)
        self.assertEqual(second.anchor.boundary,(10,0))
        self.assertEqual(second.evidence_sha256,'a'*64)

    def test_fast_repeated_samples_do_not_count_as_two_health_checks(self):
        for mono in (10,11,20,69):
            self.assertFalse(self.monitor.sample(self.sample(mono),ingress_ordinal=0).aligned)
        self.assertTrue(self.monitor.sample(self.sample(70),ingress_ordinal=0).aligned)

    def test_wall_step_opens_new_epoch_and_needs_two_later_healthy_samples(self):
        self.monitor.sample(self.sample(10),ingress_ordinal=0)
        old=self.monitor.sample(self.sample(70),ingress_ordinal=1).anchor
        stepped=replace(self.sample(80),utc_ns=500)
        fault=self.monitor.sample(stepped,ingress_ordinal=2)
        self.assertEqual(fault.reason,'wall_step')
        self.assertEqual(fault.invalidated_epoch,old.clock_epoch)
        self.assertEqual(fault.anchor.boundary,(80,2))
        self.assertEqual(self.monitor.prior_anchors[0].healthy_samples,0)
        for mono in (90,150):
            result=self.monitor.sample(replace(self.sample(mono),utc_ns=500+mono-80),ingress_ordinal=3)
        self.assertTrue(result.aligned)
        self.assertNotEqual(result.anchor.clock_epoch,old.clock_epoch)

    def test_bad_reference_including_uncertainty_resets_recovery(self):
        self.monitor.sample(self.sample(10),ingress_ordinal=0)
        self.monitor.sample(self.sample(70),ingress_ordinal=0)
        fault=self.monitor.sample(self.sample(80,offset=-90,uncertainty=11),ingress_ordinal=1)
        self.assertFalse(fault.aligned)
        self.assertEqual(fault.reason,'reference_offset_or_uncertainty')
        self.assertIsNotNone(fault.invalidated_epoch)
        self.assertFalse(self.monitor.sample(self.sample(90),ingress_ordinal=1).aligned)
        self.assertTrue(self.monitor.sample(self.sample(150),ingress_ordinal=1).aligned)

    def test_stale_anchor_cannot_align_and_stale_gap_starts_new_recovery_epoch(self):
        self.monitor.sample(self.sample(10),ingress_ordinal=0)
        result=self.monitor.sample(self.sample(70),ingress_ordinal=0)
        self.assertEqual(self.monitor.anchor(now_monotonic_ns=130),result.anchor)
        self.assertEqual(self.monitor.anchor(now_monotonic_ns=131).healthy_samples,0)
        fault=self.monitor.sample(self.sample(140),ingress_ordinal=1)
        self.assertEqual(fault.reason,'sample_stale')
        self.assertFalse(fault.aligned)
        self.assertIsNotNone(fault.invalidated_epoch)

    def test_boot_regression_and_ingress_regression_require_fresh_session(self):
        for kind in ('boot','mono','ordinal'):
            with self.subTest(kind=kind):
                monitor=ClockMonitor('process')
                monitor.sample(self.sample(10),ingress_ordinal=2)
                sample=self.sample(9 if kind=='mono' else 20,boot='new' if kind=='boot' else 'boot')
                with self.assertRaises(ClockRestartRequired):
                    monitor.sample(sample,ingress_ordinal=1 if kind=='ordinal' else 2)
                with self.assertRaises(ClockRestartRequired):
                    monitor.sample(self.sample(100),ingress_ordinal=3)

    def test_current_monotonic_regression_is_sticky(self):
        self.monitor.sample(self.sample(10),ingress_ordinal=0)
        with self.assertRaises(ClockRestartRequired):self.monitor.anchor(now_monotonic_ns=9)
        with self.assertRaises(ClockRestartRequired):self.monitor.anchor(now_monotonic_ns=20)

    def test_regression_between_anchor_queries_cannot_restore_stale_alignment(self):
        self.monitor.sample(self.sample(10),ingress_ordinal=0)
        self.monitor.sample(self.sample(70),ingress_ordinal=0)
        self.assertEqual(self.monitor.anchor(now_monotonic_ns=131).healthy_samples,0)
        with self.assertRaises(ClockRestartRequired):self.monitor.anchor(now_monotonic_ns=130)
        with self.assertRaises(ClockRestartRequired):self.monitor.anchor(now_monotonic_ns=200)
        with self.assertRaises(ClockRestartRequired):self.monitor.sample(self.sample(200),ingress_ordinal=1)

    def test_sample_cannot_regress_past_a_more_recent_anchor_query(self):
        self.monitor.sample(self.sample(10),ingress_ordinal=0)
        self.monitor.sample(self.sample(70),ingress_ordinal=0)
        self.assertEqual(self.monitor.anchor(now_monotonic_ns=120).healthy_samples,2)
        with self.assertRaises(ClockRestartRequired):self.monitor.sample(self.sample(80),ingress_ordinal=1)
        with self.assertRaises(ClockRestartRequired):self.monitor.sample(self.sample(180),ingress_ordinal=1)
        with self.assertRaises(ClockRestartRequired):self.monitor.anchor(now_monotonic_ns=180)

    def test_invalid_measurement_and_duplicate_sample_do_not_advance_state(self):
        with self.assertRaises(ValueError):self.monitor.anchor(now_monotonic_ns=0)
        with self.assertRaises(ValueError):ClockSample(1,1,True,0,'boot','a'*64)
        with self.assertRaises(ValueError):ClockSample(1,1,0,0,'boot','bad')
        with self.assertRaises(ValueError):ClockSample(1,1,0,0,'b'*129,'a'*64)
        first=self.monitor.sample(self.sample(10),ingress_ordinal=0)
        with self.assertRaises(ValueError):self.monitor.sample(self.sample(10),ingress_ordinal=0)
        self.assertEqual(self.monitor.anchor(now_monotonic_ns=10),first.anchor)

    def test_epoch_history_cap_fails_closed(self):
        monitor=ClockMonitor('process',max_epochs=1)
        monitor.sample(self.sample(10),ingress_ordinal=0)
        with self.assertRaises(ClockRestartRequired):
            monitor.sample(replace(self.sample(20),utc_ns=9_000_000),ingress_ordinal=1)
        self.assertEqual(monitor.prior_anchors,())

    def test_measured_anchor_works_with_existing_ingress_barrier(self):
        class Journal:
            state=type('State',(),{'positions':{}})()
            def append(self,record):pass
        self.monitor.sample(self.sample(10),ingress_ordinal=0)
        anchor=self.monitor.sample(self.sample(70),ingress_ordinal=0).anchor
        ingress=IngressWriter(Journal(),'capture','process')
        record=raw(0)
        record=replace(record,receipt=replace(record.receipt,receive_utc_ns=1080,monotonic_ns=80,
                                             clock_epoch=anchor.clock_epoch))
        ingress.append(record)
        barrier=ingress.barrier(anchor,1100,now_monotonic_ns=100,healthy_streams=[record.receipt.stream_key])
        self.assertEqual(barrier['deadline_ns'],100)
        self.assertEqual(barrier['ordinal'],1)
        self.assertEqual(barrier['anchor']['healthy_samples'],2)

    def check(self,mono,**kwargs):
        return self.monitor.check(utc_ns=kwargs.get('utc',1000+mono),monotonic_ns=mono,
                                  boot_id=kwargs.get('boot','boot'),ingress_ordinal=kwargs.get('ordinal',0))

    def test_paired_checks_do_not_refresh_reference_or_count_as_healthy_samples(self):
        self.monitor.sample(self.sample(10),ingress_ordinal=0)
        self.assertFalse(self.check(70).aligned)
        self.assertTrue(self.monitor.sample(self.sample(70),ingress_ordinal=0).aligned)
        self.assertTrue(self.check(130).aligned)
        fault=self.check(131)
        self.assertFalse(fault.aligned)
        self.assertEqual(fault.reason,'sample_stale')
        self.assertEqual(fault.anchor.healthy_samples,0)
        self.assertEqual(len(self.monitor.prior_anchors),1)
        self.assertEqual(self.check(132).reason,'sample_stale')
        self.assertEqual(len(self.monitor.prior_anchors),1)
        self.assertFalse(self.monitor.sample(self.sample(140),ingress_ordinal=0).aligned)
        self.assertTrue(self.monitor.sample(self.sample(200),ingress_ordinal=0).aligned)

    def test_check_detects_wall_step_and_two_measured_samples_recover(self):
        self.monitor.sample(self.sample(10),ingress_ordinal=0)
        self.monitor.sample(self.sample(70),ingress_ordinal=0)
        fault=self.check(80,utc=500,ordinal=1)
        self.assertEqual(fault.reason,'wall_step')
        self.assertEqual(fault.anchor.boundary,(80,1))
        self.assertIsNotNone(fault.invalidated_epoch)
        first=self.monitor.sample(replace(self.sample(90),utc_ns=510),ingress_ordinal=1)
        second=self.monitor.sample(replace(self.sample(150),utc_ns=570),ingress_ordinal=1)
        self.assertFalse(first.aligned)
        self.assertTrue(second.aligned)
        self.assertEqual(first.anchor.clock_epoch,fault.anchor.clock_epoch)

    def test_checks_detect_cumulative_drift_relative_to_pinned_reference(self):
        self.monitor.sample(self.sample(10),ingress_ordinal=0)
        self.monitor.sample(self.sample(70),ingress_ordinal=0)
        self.assertTrue(self.check(71,utc=1072).aligned)
        self.assertTrue(self.check(72,utc=1074).aligned)
        self.assertEqual(self.check(73,utc=1076).reason,'wall_step')

    def test_interleaved_check_cannot_hide_cumulative_step_from_measured_sample(self):
        self.monitor.sample(self.sample(10),ingress_ordinal=0)
        old=self.monitor.sample(self.sample(70),ingress_ordinal=0)
        self.assertTrue(self.check(71,utc=1072).aligned)
        fault=self.monitor.sample(replace(self.sample(72),utc_ns=1075),ingress_ordinal=0)
        self.assertFalse(fault.aligned)
        self.assertEqual(fault.reason,'wall_step')
        self.assertEqual(fault.invalidated_epoch,old.anchor.clock_epoch)

    def test_sample_only_consecutive_stale_epochs_preserve_original_policy(self):
        epochs=[self.monitor.sample(self.sample(mono),ingress_ordinal=0).anchor.clock_epoch
                for mono in (10,70,140,210)]
        self.assertEqual(epochs,['process:clock:0','process:clock:0','process:clock:1','process:clock:2'])

    def test_check_order_faults_are_sticky_across_samples_and_queries(self):
        for kind in ('mono','ordinal','boot'):
            with self.subTest(kind=kind):
                self.setUp()
                self.monitor.sample(self.sample(10),ingress_ordinal=2)
                self.monitor.check(utc_ns=1020,monotonic_ns=20,boot_id='boot',ingress_ordinal=3)
                with self.assertRaises(ClockRestartRequired):
                    self.check(19 if kind=='mono' else 21,boot='other' if kind=='boot' else 'boot',
                               ordinal=2 if kind=='ordinal' else 3)
                with self.assertRaises(ClockRestartRequired):self.monitor.sample(self.sample(100),ingress_ordinal=4)
                with self.assertRaises(ClockRestartRequired):self.check(100,ordinal=4)

    def test_checks_cannot_invent_reference_and_epoch_bound_remains_enforced(self):
        with self.assertRaises(ValueError):self.check(1)
        monitor=ClockMonitor('process',sample_interval_ns=60,max_sample_age_ns=60,max_epochs=1)
        monitor.sample(self.sample(10),ingress_ordinal=0)
        with self.assertRaises(ClockRestartRequired):
            monitor.check(utc_ns=1071,monotonic_ns=71,boot_id='boot',ingress_ordinal=1)


if __name__=='__main__':unittest.main()
