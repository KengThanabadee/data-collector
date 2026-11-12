from dataclasses import replace
import unittest
from unittest.mock import patch

from data_collector.contracts import RawRecord,Receipt,StreamKey,canonical,digest,strict_json
from data_collector.cutoffs import ClockAnchor,CutoffCoordinator,CutoffPin,MINUTE_NS
from data_collector.journal import Journal,JournalLimits
from data_collector.recovery import RecoverySegment,reconcile_cutoffs
from data_collector.segments import SegmentContext,seal_segment,iter_segment_records
from data_collector.writer import SessionWriter,recover_session
from tests import test_dc_cutoffs as fixtures
from tests.helpers import raw

C=MINUTE_NS


class RecoveryTests(unittest.TestCase):
    setUp=fixtures.CutoffTests.setUp
    coordinator_for=fixtures.CutoffTests.coordinator_for
    append=fixtures.CutoffTests.append
    allocate=fixtures.CutoffTests.allocate
    barrier=fixtures.CutoffTests.barrier
    close=fixtures.CutoffTests.close

    def prefix(self):
        pin=self.coordinator.pin
        path=seal_segment(self.wal,self.path.parent/'archive',SegmentContext('deribit','production','capture','process','0','a'*64,'b'*64))
        return (RecoverySegment(path,digest((path/'context.json').read_bytes()),digest((path/'manifest.json').read_bytes())),),pin

    def target(self,session='restart',**kwargs):
        wal=Journal(self.path.parent/(session+'.wal'),venue='deribit',environment='production',create=True,**kwargs)
        self.addCleanup(wal.close)
        return wal

    def recover(self,target,segments,pin,session='restart',**kwargs):
        options=dict(capture_scope='local',capture_id='capture',process_session_id=session,provenance=raw().provenance,
            utc_ns=2*C,monotonic_ns=10,clock_epoch='restart-epoch')
        options.update(kwargs)
        return reconcile_cutoffs(target,segments,pin,**options)

    def records(self,target):return [RawRecord.from_bytes(b) for b in target.encoded_records(allow_unsealed=True)]

    def test_open_allocation_is_failed_before_marker_and_next_label_cannot_reopen(self):
        self.allocate();segments,pin=self.prefix();original=tuple(r.to_bytes() for r in iter_segment_records(segments[0].path))
        target=self.target();state=self.recover(target,segments,pin)
        self.assertEqual((state.failed_cutoff_ids,state.closed_cutoff_ids,state.next_cutoff_id,state.next_cutoff_utc_ns),((0,),(),1,2*C))
        records=self.records(target)
        self.assertEqual([strict_json(r.body).get('kind') for r in records],['failed',None])
        self.assertEqual(strict_json(records[0].body)['payload']['reason'],'interrupted_before_close')
        self.assertEqual(state.marker_sha256,digest(records[1].to_bytes()))
        self.assertEqual(tuple(r.to_bytes() for r in iter_segment_records(segments[0].path)),original)
        restored=CutoffCoordinator(target,capture_scope='local',capture_id='capture',process_session_id='restart',
            provenance=raw().provenance,prior_records=lambda:iter_segment_records(segments[0].path),expected_prior_pin=pin)
        self.assertEqual(restored.pin,state.recovered_pin)
        anchor=ClockAnchor('restart','new',2*C-100,1,0,2)
        with self.assertRaises(ValueError):restored.allocate(C,[StreamKey('capture','restart','new')],fixtures.DEPS,
            anchor=anchor,observed_utc_ns=2*C-100,monotonic_ns=1,clock_epoch='new')
        restored.allocate(2*C,[StreamKey('capture','restart','new')],fixtures.DEPS,
            anchor=anchor,observed_utc_ns=2*C-100,monotonic_ns=1,clock_epoch='new')
        self.assertIn(1,restored.allocations)
        with self.assertRaises(ValueError):self.recover(target,segments,pin)

    def test_closed_bytes_remain_unchanged_and_retry_keeps_first_marker(self):
        self.allocate();self.append(0,C-10,1090);self.close(self.barrier())
        old=canonical(self.coordinator.resolutions[0]);self.coordinator.allocate(2*C,[self.stream],fixtures.DEPS,
            anchor=self.anchor,observed_utc_ns=C,monotonic_ns=1100,clock_epoch='epoch')
        segments,pin=self.prefix();target=self.target()
        first=self.recover(target,segments,pin);encoded=tuple(r.to_bytes() for r in self.records(target))
        again=self.recover(target,segments,pin,utc_ns=3*C,monotonic_ns=900)
        self.assertEqual(again,first)
        self.assertEqual(tuple(r.to_bytes() for r in self.records(target)),encoded)
        self.assertEqual((first.failed_cutoff_ids,first.closed_cutoff_ids),((1,),(0,)))
        restored=CutoffCoordinator(target,capture_scope='local',capture_id='capture',process_session_id='restart',
            provenance=raw().provenance,prior_records=lambda:iter_segment_records(segments[0].path),expected_prior_pin=pin)
        self.assertEqual(canonical(restored.resolutions[0]),old)

    def test_closed_only_prefix_records_recovery_without_fabricating_delivery(self):
        self.allocate();self.append(0,C-10,1090);self.close(self.barrier())
        segments,pin=self.prefix();target=self.target();state=self.recover(target,segments,pin)
        self.assertEqual(state.recovered_pin,pin)
        self.assertEqual(state.failed_cutoff_ids,())
        self.assertEqual(len(self.records(target)),1)
        self.assertEqual(self.records(target)[0].channel,'cutoff_recovery:local')

    def test_wrong_expected_tip_is_rejected_before_writing_any_resolution(self):
        self.allocate();segments,pin=self.prefix();target=self.target()
        for bad in (replace(pin,count=pin.count+1),replace(pin,head_sha256='f'*64),replace(pin,last_record_sha256='f'*64),CutoffPin(0,None,None)):
            with self.assertRaises(ValueError):self.recover(target,segments,bad)
            self.assertEqual(target.state.records,0)

    def test_corrupt_pinned_manifest_and_reused_process_identity_fail_before_writes(self):
        self.allocate();segments,pin=self.prefix();target=self.target('process')
        with self.assertRaises(ValueError):self.recover(target,segments,pin,session='process')
        self.assertEqual(target.state.records,0)
        target=self.target();manifest=segments[0].path/'manifest.json';manifest.write_bytes(manifest.read_bytes()+b' ')
        with self.assertRaises(ValueError):self.recover(target,segments,pin)
        self.assertEqual(target.state.records,0)

    def test_omitted_control_prefix_cannot_match_expected_tip(self):
        self.allocate();self.append(0,C-10,1090);self.close(self.barrier());segments,pin=self.prefix()
        with Journal(self.path.parent/'omitted.wal',venue='deribit',environment='production',create=True) as wal:
            for record in iter_segment_records(segments[0].path):
                if record.channel=='cutoffs:local' and strict_json(record.body)['kind']=='allocated':continue
                wal.append(record)
            path=seal_segment(wal,self.path.parent/'archive',SegmentContext('deribit','production','capture','process','1','a'*64,'b'*64))
        omitted=(RecoverySegment(path,digest((path/'context.json').read_bytes()),digest((path/'manifest.json').read_bytes())),)
        target=self.target()
        with self.assertRaises(ValueError):self.recover(target,omitted,pin)
        self.assertEqual(target.state.records,0)

    def test_all_resource_bounds_reject_before_any_interruption_append(self):
        self.allocate();self.append(0,C-10,1090);self.close(self.barrier());segments,pin=self.prefix()
        target=self.target()
        for limits in (dict(max_records=1),dict(max_scan_bytes=1),dict(max_control_bytes=1),dict(max_projection_bytes=1)):
            with self.assertRaises(ValueError):self.recover(target,segments,pin,**limits)
            self.assertEqual(target.state.records,0)
        tiny=self.target('tiny',limits=JournalLimits(max_record_bytes=64))
        with self.assertRaises(ValueError):self.recover(tiny,segments,pin,session='tiny')
        self.assertEqual(tiny.state.records,0)

    def test_projected_record_bound_is_reserved_before_append_and_exact_bound_retries(self):
        self.allocate();segments,pin=self.prefix();target=self.target()
        with self.assertRaises(ValueError):self.recover(target,segments,pin,max_records=1)
        self.assertEqual(target.state.records,0)
        first=self.recover(target,segments,pin,max_records=2)
        self.assertEqual(target.state.records,2)
        self.assertEqual(self.recover(target,segments,pin,max_records=2),first)

    def test_control_stream_headroom_is_reserved_before_interruption_append(self):
        self.allocate();segments,pin=self.prefix()
        target=self.target(limits=JournalLimits(max_streams=1))
        with self.assertRaises(ValueError):self.recover(target,segments,pin)
        self.assertEqual(target.state.records,0)

    def test_retry_rejects_known_process_epoch_change_or_monotonic_regression(self):
        self.allocate();segments,pin=self.prefix();target=self.target()
        first=self.recover(target,segments,pin)
        before=tuple(r.to_bytes() for r in self.records(target))
        for changes in (dict(monotonic_ns=9),dict(clock_epoch='different-boot')):
            with self.assertRaises(ValueError):self.recover(target,segments,pin,**changes)
        self.assertEqual(tuple(r.to_bytes() for r in self.records(target)),before)
        self.assertEqual(self.recover(target,segments,pin),first)

    def test_ambiguous_failure_after_first_resolution_is_idempotently_reconciled(self):
        self.allocate();self.coordinator.allocate(2*C,[self.stream],fixtures.DEPS,anchor=self.anchor,
            observed_utc_ns=C,monotonic_ns=1100,clock_epoch='epoch')
        segments,pin=self.prefix();target=self.target();append=target.append
        def after_write(record):
            append(record)
            if record.channel=='cutoffs:local':raise OSError('after durable interruption')
        with patch.object(target,'append',side_effect=after_write):
            with self.assertRaises(OSError):self.recover(target,segments,pin)
        self.assertEqual(len(self.records(target)),1)
        state=self.recover(target,segments,pin)
        self.assertEqual(state.failed_cutoff_ids,(0,1))
        records=self.records(target)
        self.assertEqual([r.receipt.receive_seq for r in records],[0,1,0])
        self.assertEqual([strict_json(r.body)['sequence'] for r in records[:2]],[2,3])
        self.assertEqual(len(records),3)

    def test_ambiguous_marker_write_returns_no_success_until_retry_verifies_same_bytes(self):
        self.allocate();segments,pin=self.prefix();target=self.target();append=target.append
        def after_write(record):
            append(record)
            if record.channel=='cutoff_recovery:local':raise OSError('after durable marker')
        with patch.object(target,'append',side_effect=after_write):
            with self.assertRaises(OSError):self.recover(target,segments,pin)
        original=tuple(r.to_bytes() for r in self.records(target))
        state=self.recover(target,segments,pin,utc_ns=3*C,monotonic_ns=999)
        self.assertEqual(tuple(r.to_bytes() for r in self.records(target)),original)
        self.assertEqual(state.marker_sha256,digest(original[-1]))

    def test_changed_prefix_during_recovery_prevents_ready_marker(self):
        self.allocate();segments,pin=self.prefix();target=self.target();append=target.append
        def changed(record):
            append(record)
            if record.channel=='cutoffs:local':
                path=segments[0].path/'manifest.json';path.write_bytes(path.read_bytes()+b' ')
        with patch.object(target,'append',side_effect=changed):
            with self.assertRaises(ValueError):self.recover(target,segments,pin)
        self.assertEqual(len(self.records(target)),1)
        self.assertFalse(any(r.channel=='cutoff_recovery:local' for r in self.records(target)))

    def test_target_marker_conflict_or_non_recovery_work_is_not_overwritten(self):
        self.allocate();segments,pin=self.prefix();target=self.target()
        self.recover(target,segments,pin);records=self.records(target)
        conflict=self.target('conflict')
        for record in records:
            body=canonical(dict(strict_json(record.body),next_cutoff_id=999)) if record.channel=='cutoff_recovery:local' else record.body
            conflict.append(replace(record,body=body,receipt=replace(record.receipt,process_session_id='conflict')))
        before=tuple(r.to_bytes() for r in self.records(conflict))
        with self.assertRaises(ValueError):self.recover(conflict,segments,pin,session='conflict')
        self.assertEqual(tuple(r.to_bytes() for r in self.records(conflict)),before)
        dirty=self.target('dirty');dirty.append(raw(receipt=Receipt('capture','dirty','native',0,1,1,'new')))
        with self.assertRaises(ValueError):self.recover(dirty,segments,pin,session='dirty')
        self.assertEqual(dirty.state.records,1)

    def test_session_writer_marker_survives_restore_and_next_process_recovery(self):
        self.allocate();segments,pin=self.prefix()
        writer=SessionWriter(self.path.parent/'new-wal',self.path.parent/'new-archive',venue='deribit',environment='production',
            capture_id='capture',process_session_id='restart',provenance=raw().provenance,started_utc_ns=2*C)
        self.addCleanup(writer.abort)
        state=self.recover(writer,segments,pin)
        committed=writer.finish();restored=recover_session(writer.directory,self.path.parent/'new-archive')
        self.assertEqual(committed,restored)
        references=tuple(RecoverySegment(path,digest((path/'context.json').read_bytes()),digest((path/'manifest.json').read_bytes())) for path in restored)
        next_target=self.target('again')
        again=self.recover(next_target,segments+references,state.recovered_pin,session='again',monotonic_ns=1,clock_epoch='another-boot')
        self.assertEqual((again.next_cutoff_id,again.next_cutoff_utc_ns),(1,2*C))
        self.assertEqual(again.failed_cutoff_ids,())
        self.assertEqual(again.recovered_pin,state.recovered_pin)

    def test_cutoff_restore_rejects_namespace_and_venue_confusion(self):
        self.allocate();record=next(r for r in self.records(self.wal) if r.channel=='cutoffs:local')
        for changed in (replace(record,channel='wrong'),replace(record,kind='ws_message'),replace(record,venue='bybit'),
                        replace(record,native_fields_json=b'{"extra":1}')):
            target=Journal(self.path.parent/'empty.wal',venue='deribit',environment='production',create=True)
            try:
                with self.assertRaises(ValueError):CutoffCoordinator(target,capture_scope='local',capture_id='capture',
                    process_session_id='restart',provenance=raw().provenance,prior_records=lambda:iter([changed]))
            finally:target.close();(self.path.parent/'empty.wal').unlink()
