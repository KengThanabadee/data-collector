from dataclasses import asdict, replace
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

from data_collector.contracts import Receipt, StreamKey, canonical
from data_collector.cutoffs import ClockAnchor, CutoffCoordinator, IngressWriter, MINUTE_NS
from data_collector.journal import Journal
from data_collector.segments import SegmentContext, iter_segment_records, seal_segment
from data_collector.validity import LedgerPin
from tests.test_dc_contracts import raw

C = MINUTE_NS
DEPS = dict(config='a'*64, universe='b'*64, metadata='c'*64, required_feeds='d'*64)
PIN = LedgerPin('ledger', 0, 'f'*64)  # Integration caller supplies verified prefix.


class CutoffTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory(); self.addCleanup(self.tmp.cleanup)
        self.path = Path(self.tmp.name)/'control.wal'
        self.wal = Journal(self.path, venue='deribit', environment='production', create=True)
        self.addCleanup(lambda: self.wal.close())
        self.ingress = IngressWriter(self.wal, 'capture', 'process')
        self.coordinator = self.coordinator_for()
        self.anchor = ClockAnchor('process','epoch',C-100,1000,0,2)
        self.stream = raw().receipt.stream_key

    def coordinator_for(self, session='process', **kwargs):
        return CutoffCoordinator(self.wal, capture_scope='local', capture_id='capture',
                                 process_session_id=session, provenance=raw().provenance, **kwargs)

    def append(self, seq, utc, mono, **kw):
        receipt = replace(raw(seq).receipt, receive_utc_ns=utc, monotonic_ns=mono, **kw)
        return self.ingress.append(raw(seq, receipt=receipt))

    def allocate(self, cutoff=C, **kwargs):
        return self.coordinator.allocate(cutoff,[self.stream],DEPS,observed_utc_ns=C-100,
                                          monotonic_ns=1000,clock_epoch='epoch',anchor=self.anchor, **kwargs)

    def barrier(self, now=1100, health=None):
        return self.ingress.barrier(self.anchor, C, now_monotonic_ns=now,
                                    healthy_streams=[self.stream] if health is None else health)

    def close(self, barrier):
        mono=barrier['observed_monotonic_ns']
        return self.coordinator.close(0,barrier,ledger_pin=PIN,utc_ns=self.anchor.utc_ns+mono-self.anchor.monotonic_ns,
                                      monotonic_ns=mono,clock_epoch='epoch')['payload']

    def test_equal_timestamp_barrier_tie_and_later_rollback_excluded(self):
        self.allocate()
        first = self.append(0,C,1100)
        barrier = self.barrier()
        # Same timestamp but later ingress ordinal cannot enter the closed prefix.
        self.append(1,C,1100)
        self.append(2,C-50,1101)
        result = self.close(barrier)
        self.assertEqual(result['validity'],'valid')
        self.assertEqual(result['streams'][0]['admitted_seq'],0)
        self.assertEqual(result['streams'][0]['record_hash'],__import__('hashlib').sha256(first.to_bytes()).hexdigest())
        old = canonical(result)
        self.wal.close()
        self.wal = Journal(self.path,venue='deribit',environment='production')
        restored = self.coordinator_for('restart')
        self.assertEqual(canonical(restored.resolutions[0]['payload']),old)
        with self.assertRaises(ValueError):
            restored.allocate(C,[self.stream],DEPS,anchor=ClockAnchor('restart','new',C,1,0,2),observed_utc_ns=C,monotonic_ns=1,clock_epoch='new')

    def test_delayed_timer_keeps_deadline_and_quiet_cutoff_health(self):
        self.allocate(); self.append(0,C-10,1090); self.append(1,C+1,1101)
        result = self.close(self.barrier(now=1200))
        self.assertEqual(result['barrier']['deadline_ns'],1100)
        self.assertEqual(result['streams'][0]['admitted_seq'],0)
        self.assertEqual(result['validity'],'valid')

    def test_utc_contradiction_does_not_skip_middle_to_admit_later_delta(self):
        self.allocate(); self.append(0,C-10,1090)
        self.append(1,C+5,1095); self.append(2,C-1,1099)
        result = self.close(self.barrier())
        self.assertEqual(result['validity'],'invalid')
        self.assertEqual(result['streams'][0]['admitted_seq'],0)

    def test_clock_step_after_deadline_before_barrier_invalidates_closure(self):
        self.allocate(); self.append(0,C-10,1090); self.append(1,C-50,1101)
        result=self.close(self.barrier(now=1200))
        self.assertEqual(result['validity'],'invalid')
        self.assertEqual(result['streams'][0]['admitted_seq'],0)

    def test_missing_receipt_or_barrier_health_and_unhealthy_clock(self):
        self.allocate()
        self.assertEqual(self.close(self.barrier())['validity'],'missing')
        self.coordinator.allocate(2*C,[self.stream],DEPS,anchor=self.anchor,observed_utc_ns=C,monotonic_ns=1100,clock_epoch='epoch')
        self.append(0,C+1,1101)
        barrier=self.ingress.barrier(self.anchor,2*C,now_monotonic_ns=C+1100,healthy_streams=[])
        result=self.coordinator.close(1,barrier,ledger_pin=PIN,utc_ns=2*C,monotonic_ns=barrier['observed_monotonic_ns'],clock_epoch='epoch')['payload']
        self.assertEqual(result['validity'],'missing')
        anchor=replace(self.anchor,healthy_samples=1)
        self.coordinator.allocate(3*C,[self.stream],DEPS,anchor=anchor,observed_utc_ns=2*C,monotonic_ns=C+1100,clock_epoch='epoch')
        barrier=self.ingress.barrier(anchor,3*C,now_monotonic_ns=2*C+1100,healthy_streams=[self.stream])
        result=self.coordinator.close(2,barrier,ledger_pin=PIN,utc_ns=3*C,monotonic_ns=barrier['observed_monotonic_ns'],clock_epoch='epoch')['payload']
        self.assertEqual(result['validity'],'invalid')

    def test_allocation_survives_before_close_and_failure_is_explicit(self):
        self.allocate(); self.wal.close()
        self.wal=Journal(self.path,venue='deribit',environment='production')
        restored=self.coordinator_for('restart')
        self.assertIn(0,restored.allocations); self.assertNotIn(0,restored.resolutions)
        restored.fail(0,'interrupted_before_close',utc_ns=C,monotonic_ns=1,clock_epoch='new')
        with self.assertRaises(ValueError):restored.fail(0,'duplicate',utc_ns=C,monotonic_ns=2,clock_epoch='new')
        restored.allocate(2*C,[StreamKey('capture','restart','new')],DEPS,
                          anchor=ClockAnchor('restart','new',C,2,0,2),observed_utc_ns=C,monotonic_ns=2,clock_epoch='new')

    def test_fsync_failure_does_not_advance_allocation_but_survivor_replays(self):
        with patch('data_collector.journal.os.fsync',side_effect=OSError('injected')):
            with self.assertRaises(OSError):self.allocate()
        self.assertEqual(len(self.coordinator.allocations),0)
        self.wal.close(); self.wal=Journal(self.path,venue='deribit',environment='production')
        restored=self.coordinator_for('restart')
        self.assertEqual(len(restored.allocations),1)

    def test_control_dependency_immutability_bounds_and_omitted_minute(self):
        result=self.allocate(); result['payload']['dependencies']['metadata']='0'*64
        self.assertEqual(self.coordinator.allocations[0]['payload']['dependencies'],DEPS)
        with self.assertRaises(ValueError):self.coordinator.allocate(3*C,[self.stream],DEPS,
            anchor=self.anchor,observed_utc_ns=C,monotonic_ns=1,clock_epoch='epoch')
        with self.assertRaises(ValueError):self.coordinator_for('restart',max_projection_bytes=1)
        with self.assertRaises(ValueError):self.coordinator_for('restart',prior_records=[])

    def test_missing_raw_prefix_and_wrong_epoch_are_invalid(self):
        self.allocate()
        receipt=replace(raw(5).receipt,receive_utc_ns=C-1,monotonic_ns=1099)
        self.wal.append(raw(5,receipt=receipt,native_fields_json=canonical({'collector_ingress_ordinal':0})))
        result=self.close(dict(anchor=__import__('dataclasses').asdict(self.anchor),deadline_ns=1100,
            ordinal=1,observed_monotonic_ns=1100,healthy_streams=[asdict(self.stream)],epoch_anchors=[asdict(self.anchor)]))
        self.assertEqual(result['validity'],'invalid')

    def test_two_healthy_samples_in_new_clock_epoch_resume_same_connection(self):
        self.allocate(); self.append(0,C-10,1090)
        self.close(self.barrier())
        new=ClockAnchor('process','new',2*C-100,C+900,0,2,epoch_started_ns=C+850)
        self.coordinator.allocate(2*C,[self.stream],DEPS,anchor=new,observed_utc_ns=2*C-100,monotonic_ns=C+900,clock_epoch='new')
        self.append(1,2*C-1,C+999,clock_epoch='new')
        barrier=self.ingress.barrier(new,2*C,now_monotonic_ns=C+1000,
                                    healthy_streams=[self.stream],prior_anchors=[self.anchor])
        result=self.coordinator.close(1,barrier,ledger_pin=PIN,utc_ns=2*C,monotonic_ns=barrier['observed_monotonic_ns'],clock_epoch='new')['payload']
        self.assertEqual(result['validity'],'valid')
        self.assertEqual(result['streams'][0]['admitted_seq'],1)

    def test_unhealthy_old_epoch_does_not_reinvalidate_healthy_future_cutoff(self):
        self.allocate();self.append(0,C-10,1090);self.append(1,C-50,1101)
        first=self.close(self.barrier(now=1200))
        self.assertEqual(first['validity'],'invalid')
        new=ClockAnchor('process','new',C+1000,2151,0,2,epoch_started_ns=1201)
        self.coordinator.allocate(2*C,[self.stream],DEPS,anchor=new,observed_utc_ns=C+1000,monotonic_ns=2151,clock_epoch='new')
        self.append(2,2*C-1,C+1150,clock_epoch='new')
        barrier=self.ingress.barrier(new,2*C,now_monotonic_ns=C+1151,
            healthy_streams=[self.stream],prior_anchors=[self.anchor])
        result=self.coordinator.close(1,barrier,ledger_pin=PIN,utc_ns=2*C,monotonic_ns=barrier['observed_monotonic_ns'],clock_epoch='new')['payload']
        self.assertEqual(result['validity'],'valid');self.assertEqual(result['streams'][0]['admitted_seq'],2)
        self.assertEqual(result['historical_clock_invalid'],['epoch'])
        self.assertEqual(self.coordinator.resolutions[0]['payload']['validity'],'invalid')

    def test_clock_epoch_boundary_uses_ordinal_when_monotonic_ties(self):
        self.allocate();self.append(0,C,1100);self.append(1,C-50,1100,clock_epoch='new')
        new=ClockAnchor('process','new',C-48,1102,0,2,epoch_started_ns=1100,epoch_started_ordinal=1)
        self.coordinator.fail(0,'clock_step',utc_ns=C,monotonic_ns=1102,clock_epoch='new')
        self.coordinator.allocate(2*C,[self.stream],DEPS,anchor=new,observed_utc_ns=C-48,monotonic_ns=1102,clock_epoch='new')
        barrier=self.ingress.barrier(new,2*C,now_monotonic_ns=C+1150,
            healthy_streams=[self.stream],prior_anchors=[self.anchor])
        result=self.coordinator.close(1,barrier,ledger_pin=PIN,utc_ns=2*C,monotonic_ns=barrier['observed_monotonic_ns'],clock_epoch='new')['payload']
        self.assertEqual(result['validity'],'valid');self.assertEqual(result['streams'][0]['admitted_seq'],1)

    def test_clock_step_cannot_move_previously_allocated_deadline(self):
        self.allocate();self.append(0,C-10,1090)
        self.append(1,C-49,1101,clock_epoch='new');self.append(2,C-1,1149,clock_epoch='new')
        new=ClockAnchor('process','new',C-48,1102,0,2,epoch_started_ns=1101,epoch_started_ordinal=1)
        barrier=self.ingress.barrier(new,C,now_monotonic_ns=1150,
            healthy_streams=[self.stream],prior_anchors=[self.anchor])
        with self.assertRaisesRegex(ValueError,'allocated clock/deadline changed'):
            self.coordinator.close(0,barrier,ledger_pin=PIN,utc_ns=C,monotonic_ns=1150,clock_epoch='new')
        self.assertNotIn(0,self.coordinator.resolutions)
        self.coordinator.fail(0,'clock_step_after_allocation',utc_ns=C,monotonic_ns=1150,clock_epoch='new')
        self.assertEqual(self.coordinator.allocations[0]['payload']['deadline_ns'],1100)

    def test_forward_jump_records_missing_minute_without_invented_deadline(self):
        self.allocate();self.coordinator.fail(0,'interrupted',utc_ns=3*C,monotonic_ns=2000,clock_epoch='new')
        self.coordinator.allocate_missing(2*C,[self.stream],DEPS,observed_utc_ns=3*C,
                                          monotonic_ns=2000,clock_epoch='new')
        with self.assertRaisesRegex(ValueError,'missing cutoff'):
            self.coordinator.close(1,{},ledger_pin=PIN,utc_ns=3*C,monotonic_ns=2000,clock_epoch='new')
        self.coordinator.fail(1,'forward_clock_jump',utc_ns=3*C,monotonic_ns=2001,clock_epoch='new')
        self.assertIsNone(self.coordinator.allocations[1]['payload']['deadline_ns'])

    def test_normal_allocation_requires_live_consistent_schedule_observation(self):
        self.append(0,C-10,1090)
        before=self.wal.path.read_bytes()
        for utc,mono in [(C+100,1200),(C-101,999),(C-1,1000)]:
            with self.assertRaisesRegex(ValueError,'allocate_missing'):
                self.coordinator.allocate(C,[self.stream],DEPS,anchor=self.anchor,
                    observed_utc_ns=utc,monotonic_ns=mono,clock_epoch='epoch')
            self.assertEqual(self.wal.path.read_bytes(),before)
            self.assertEqual(len(self.coordinator.allocations),0)

    def test_quiet_feed_close_clock_step_is_invalid_and_matching_pair_is_saved(self):
        self.allocate();self.append(0,C-10,1090);barrier=self.barrier()
        result=self.coordinator.close(0,barrier,ledger_pin=PIN,utc_ns=C-100,
                                      monotonic_ns=1100,clock_epoch='epoch')['payload']
        self.assertEqual(result['validity'],'invalid')
        self.assertEqual(result['streams'][0]['admitted_seq'],0)

    def test_close_has_separate_paired_clock_observation_after_barrier(self):
        self.allocate();self.append(0,C-10,1090);barrier=self.barrier()
        with self.assertRaisesRegex(ValueError,'precedes barrier'):
            self.coordinator.close(0,barrier,ledger_pin=PIN,utc_ns=C-1,monotonic_ns=1099,clock_epoch='epoch')
        result=self.coordinator.close(0,barrier,ledger_pin=PIN,utc_ns=C+10,monotonic_ns=1110,clock_epoch='epoch')['payload']
        self.assertEqual(result['validity'],'valid')
        self.assertEqual(result['barrier']['observed_monotonic_ns'],1100)

    def test_non_required_connection_clock_step_invalidates_whole_process_scope(self):
        self.allocate();self.append(0,C-10,1090)
        self.append(0,C-50,1095,connection_id='other')
        result=self.close(self.barrier())
        self.assertEqual(result['validity'],'invalid')
        self.assertEqual(len(result['streams']),1)
        self.assertEqual(result['streams'][0]['admitted_seq'],0)

    def test_non_required_post_barrier_rollback_does_not_change_original_membership(self):
        self.allocate();self.append(0,C-10,1090);barrier=self.barrier()
        self.append(0,C-50,1101,connection_id='other')
        result=self.close(barrier)
        self.assertEqual(result['validity'],'valid')

    def test_control_receipt_identity_continues_across_verified_segments(self):
        self.allocate()
        context=SegmentContext('deribit','production','capture','process','one',
            raw().provenance.source_sha256,raw().provenance.config_sha256)
        segment=seal_segment(self.wal,Path(self.tmp.name)/'archive',context)
        old=list(iter_segment_records(segment));self.wal.close()
        self.wal=Journal(Path(self.tmp.name)/'next.wal',venue='deribit',environment='production',create=True)
        restored=self.coordinator_for(prior_records=lambda:iter_segment_records(segment))
        restored.fail(0,'interrupted',utc_ns=C,monotonic_ns=1100,clock_epoch='epoch')
        from data_collector.contracts import RawRecord
        new=[RawRecord.from_bytes(data) for data in self.wal.encoded_records(allow_unsealed=True)]
        self.assertEqual(new[0].receipt.receive_seq,1)
        self.assertNotEqual(new[0].record_id,old[0].record_id)
        self.assertIn(0,self.coordinator_for('restart',prior_records=lambda:iter_segment_records(segment)).resolutions)

    def test_restore_rejects_control_receipt_prefix_hole(self):
        self.allocate()
        from data_collector.contracts import RawRecord
        original=[RawRecord.from_bytes(data) for data in self.wal.encoded_records(allow_unsealed=True)][0]
        broken=replace(original,receipt=replace(original.receipt,receive_seq=5))
        self.wal.close();self.wal=Journal(Path(self.tmp.name)/'empty.wal',venue='deribit',environment='production',create=True)
        with self.assertRaisesRegex(ValueError,'receipt prefix'):
            self.coordinator_for(prior_records=lambda:iter([broken]))

    def test_ingress_rejects_clock_regression_and_identity_reuse(self):
        self.append(0,C-10,1090)
        with self.assertRaises(ValueError):self.append(1,C-11,1089)
        with self.assertRaises(ValueError):self.barrier()
        with self.assertRaises(ValueError):IngressWriter(self.wal,'capture','process')


if __name__ == '__main__':
    unittest.main()
