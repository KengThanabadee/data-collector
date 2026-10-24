from dataclasses import replace
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

from data_collector.clock import ClockRestartRequired, ClockSample
from data_collector.clocklog import ClockPin, DurableClock, replay_clock, SOURCE
from data_collector.contracts import Provenance, RawRecord, Receipt, canonical, digest, strict_json
from data_collector.journal import Journal, JournalLimits
from data_collector.writer import SessionWriter
from data_collector.segments import iter_segment_records


class ClockLogTests(unittest.TestCase):
    def setUp(self):
        self.tmp=tempfile.TemporaryDirectory();self.addCleanup(self.tmp.cleanup)
        self.root=Path(self.tmp.name)
        self.provenance=Provenance('a'*64,'b'*64,'c'*64)
        self.policy=dict(sample_interval_ns=60,max_sample_age_ns=60,max_offset_ns=100,step_tolerance_ns=2,max_epochs=8)
        self.journal=Journal(self.root/'clock.wal',venue='deribit',environment='production',create=True)
        self.addCleanup(self.journal.close)
        self.owner=self.owner_for(self.journal)

    def owner_for(self,writer,**kwargs):
        return DurableClock(writer,capture_id='capture',process_session_id='process',provenance=self.provenance,
                            policy=self.policy,**kwargs)

    def measurement(self,sequence,mono,**kwargs):
        utc=kwargs.get('utc',1000+mono)
        source=RawRecord(Receipt('capture','process',SOURCE,sequence,utc,mono,'measurement'),
            'deribit','production','control','clock_measurement','control',
            b'original reference-provider bytes '+str(sequence).encode(),self.provenance)
        sample=ClockSample(utc,mono,kwargs.get('offset',0),0,'boot',digest(source.to_bytes()))
        return sample,source

    def records(self):
        return [RawRecord.from_bytes(data) for data in self.journal.encoded_records(allow_unsealed=True)]

    def replay(self,records=None,**kwargs):
        args=dict(venue='deribit',environment='production',capture_id='capture',process_session_id='process',
                  provenance=self.provenance,policy=self.policy,expected_pin=self.owner.pin)
        args.update(kwargs)
        return replay_clock(self.records() if records is None else records,**args)

    def test_fsynced_sources_and_controls_replay_to_same_alignment_and_pin(self):
        first=self.owner.sample(*self.measurement(0,10),ingress_ordinal=0)
        self.assertFalse(first.aligned)
        result=self.owner.sample(*self.measurement(1,70),ingress_ordinal=3)
        self.assertTrue(result.aligned)
        replay=self.replay()
        self.assertEqual(replay.assessment,result)
        self.assertEqual(replay.pin,self.owner.pin)
        self.assertEqual(self.records()[0].body,b'original reference-provider bytes 0')
        self.assertEqual(self.records()[2].receipt.receive_utc_ns,1070)

    def test_wall_fault_replays_invalidated_epoch_and_original_clock_bytes(self):
        for seq,mono in enumerate((10,70)):
            self.owner.sample(*self.measurement(seq,mono),ingress_ordinal=seq)
        result=self.owner.sample(*self.measurement(2,80,utc=500),ingress_ordinal=2)
        replay=self.replay()
        self.assertEqual(replay.assessment,result)
        self.assertEqual(replay.assessment.reason,'wall_step')
        self.assertEqual(len(replay.prior_anchors),1)
        self.assertEqual(replay.prior_anchors[0].healthy_samples,0)
        self.assertEqual(self.records()[-2].receipt.receive_utc_ns,500)

    def test_write_failure_never_advances_pin_or_allows_further_assessment(self):
        original=self.journal.append
        calls=[0]
        def fail_control(record):
            calls[0]+=1
            if calls[0]==2:raise OSError('control write failure')
            return original(record)
        with patch.object(self.journal,'append',side_effect=fail_control):
            with self.assertRaises(OSError):self.owner.sample(*self.measurement(0,10),ingress_ordinal=0)
        self.assertEqual(self.owner.pin,ClockPin(0,None,None))
        self.assertEqual(len(self.records()),1)
        with self.assertRaises(ValueError):self.owner.sample(*self.measurement(0,20),ingress_ordinal=0)
        with self.assertRaises(ValueError):self.replay()

    def test_write_completed_but_exception_keeps_result_unconfirmed(self):
        original=self.journal.append
        def failed_ack(record):
            original(record)
            if record.channel=='clock_assessments':raise OSError('post-fsync failure')
        with patch.object(self.journal,'append',side_effect=failed_ack):
            with self.assertRaises(OSError):self.owner.sample(*self.measurement(0,10),ingress_ordinal=0)
        self.assertTrue(self.owner.poisoned)
        self.assertEqual(self.owner.pin.count,0)
        records=self.records()
        recovered_pin=ClockPin(1,digest(records[-1].to_bytes()),records[-1].record_id)
        self.assertEqual(self.replay(expected_pin=recovered_pin).pin,recovered_pin)

    def test_tampered_control_missing_source_or_changed_policy_fails_replay(self):
        self.owner.sample(*self.measurement(0,10),ingress_ordinal=0)
        records=self.records();body=strict_json(records[-1].body)
        body['assessment']['aligned']=True
        corrupted=replace(records[-1],body=canonical(body))
        for candidate in ([records[0],corrupted],[records[-1]],[records[0]],[records[-1],records[0]]):
            with self.subTest(candidate=len(candidate)):
                with self.assertRaises((ValueError,KeyError)):self.replay(candidate)
        policy=dict(self.policy,step_tolerance_ns=3)
        with self.assertRaises(ValueError):self.replay(policy=policy)
        with self.assertRaises(ValueError):self.replay(expected_pin=ClockPin(0,None,None))

    def test_source_hash_identity_and_control_size_are_checked_before_writes(self):
        sample,source=self.measurement(0,10)
        for bad in (replace(source,body=b'changed'),replace(source,venue='bybit')):
            with self.assertRaises(ValueError):self.owner.sample(sample,bad,ingress_ordinal=0)
        self.assertEqual(self.records(),[])
        with patch.object(self.journal,'limits',JournalLimits(max_record_bytes=100)):
            with self.assertRaises(ValueError):self.owner.sample(sample,source,ingress_ordinal=0)
        self.assertEqual(self.records(),[])
        self.assertFalse(self.owner.poisoned)
        self.owner.sample(sample,source,ingress_ordinal=0)

    def test_log_bounds_reuse_and_scan_budgets_fail_closed(self):
        owner=self.owner_for(self.journal,max_assessments=1)
        owner.sample(*self.measurement(0,10),ingress_ordinal=0)
        with self.assertRaises(ValueError):owner.sample(*self.measurement(1,70),ingress_ordinal=0)
        with self.assertRaises(ValueError):self.owner_for(self.journal)
        with self.assertRaises(ValueError):self.replay(expected_pin=owner.pin,max_scan_records=1)
        with self.assertRaises(ValueError):self.replay(expected_pin=owner.pin,max_scan_bytes=1)
        with self.assertRaises(ValueError):ClockPin(True,None,None)

    def test_rejected_monotonic_sample_poison_is_not_false_recovery(self):
        self.owner.sample(*self.measurement(0,10),ingress_ordinal=0)
        with self.assertRaises(ClockRestartRequired):self.owner.sample(*self.measurement(1,9),ingress_ordinal=0)
        self.assertEqual(self.owner.pin.count,1)
        with self.assertRaises(ValueError):self.owner.sample(*self.measurement(1,70),ingress_ordinal=0)

    def test_session_rotation_preserves_clock_pair_replay(self):
        writer=SessionWriter(self.root/'sessions',self.root/'archive',venue='deribit',environment='production',
            capture_id='capture',process_session_id='process',provenance=self.provenance,started_utc_ns=1000,
            now_ns=lambda:100,limits=JournalLimits(target_bytes=1000))
        self.addCleanup(writer.abort)
        owner=self.owner_for(writer)
        for sequence,mono in enumerate((10,70)):
            owner.sample(*self.measurement(sequence,mono),ingress_ordinal=sequence)
        paths=writer.finish()
        records=[record for path in paths for record in iter_segment_records(path)]
        self.assertGreater(len(paths),1)
        self.assertTrue(self.replay(records,expected_pin=owner.pin).assessment.aligned)


if __name__=='__main__':unittest.main()
