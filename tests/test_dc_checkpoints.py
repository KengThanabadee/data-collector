from copy import deepcopy
from dataclasses import asdict,replace
import unittest
from unittest.mock import patch

from data_collector.bookstate import BookStateLimits,restore_snapshot_state,snapshot_state
from data_collector.books import BookLimits,SnapshotBook
from data_collector.checkpoints import (NativeCheckpointLimits,make_native_checkpoint,publish_native_checkpoint,
    read_native_checkpoint,verify_native_checkpoint)
from data_collector.contracts import canonical,digest,strict_json
from data_collector.replay import ReplayLimits,_book_message,materialize_snapshot
from data_collector.segments import iter_segment_records
from tests.test_dc_books import event,levels
from tests.helpers import key
from tests import test_dc_replay as fixtures
from tests import test_dc_deribit_ends as ends


class NativeStateTests(unittest.TestCase):
    def clone(self,model):return restore_snapshot_state(strict_json(canonical(snapshot_state(model))))

    def test_hidden_observed_watermark_survives_equal_display_views(self):
        untouched=SnapshotBook(key(),'deribit_full');observed=SnapshotBook(key(),'deribit_full')
        observed.receive(event(kind='delta',update_id=100,previous_id=99))
        self.assertEqual(untouched.view(),observed.view())
        restored=self.clone(observed);candidate=event(update_id=50)
        self.assertEqual(untouched.receive(candidate),'applied')
        self.assertEqual(restored.receive(candidate),'stale')
        self.assertEqual(snapshot_state(restored),snapshot_state(observed))

    def test_duplicate_fingerprint_invalid_watermark_and_fresh_anchor_resume(self):
        model=SnapshotBook(key(),'deribit_full');model.receive(event(bids=levels(('100','2'))))
        delta=event(kind='delta',update_id=11,previous_id=10,bids=levels(('100','4')))
        model.receive(delta);restored=self.clone(model)
        for candidate,expected in ((replace(delta,source_record_id='b'*64),'duplicate'),
                (replace(delta,bids=levels(('100','99'))),'invalid'),
                (event(update_id=9),'stale'),(event(update_id=12,bids=levels(('100','8'))),'applied')):
            self.assertEqual(model.receive(candidate),expected);self.assertEqual(restored.receive(candidate),expected)
            self.assertEqual(snapshot_state(model),snapshot_state(restored))

    def test_bybit_reset_cross_watermarks_and_health_only_confidence(self):
        inst=key(venue='bybit');model=SnapshotBook(inst,'bybit_standard',depth=100)
        model.receive(event(inst,update_id=100,cross_sequence=1000))
        model.receive(event(inst,kind='delta',update_id=200,cross_sequence=900))
        restored=self.clone(model)
        for candidate in (event(inst,update_id=150,cross_sequence=1200),event(inst,update_id=1,cross_sequence=1),
                          event(inst,kind='delta',update_id=5,cross_sequence=8)):
            self.assertEqual(model.receive(candidate),restored.receive(candidate))
            self.assertEqual(snapshot_state(model),snapshot_state(restored))
        self.assertEqual(restored.view()['continuity_confidence'],'health_only')

    def test_exact_signed_prices_resource_cap_and_atomic_update_survive(self):
        inst=key(native_product='combo',native_symbol='COMBO')
        model=SnapshotBook(inst,'deribit_full',signed_prices=True,limits=BookLimits(1))
        model.receive(event(inst,bids=levels(('-1.1234567890123456789','2.1234567890123456789'))))
        restored=self.clone(model)
        candidate=event(inst,kind='delta',update_id=11,previous_id=10,
            bids=levels(('0','3','new'),('-1.1234567890123456789','0','delete')))
        self.assertEqual(model.receive(candidate),restored.receive(candidate))
        self.assertEqual(snapshot_state(model),snapshot_state(restored));self.assertEqual(restored.book.limits.max_levels_per_side,1)
        self.assertEqual(restored.book.levels()['bids'],[('0','3')])

    def test_invalid_and_reset_bootstrap_states_roundtrip(self):
        model=SnapshotBook(key(),'deribit_full');model.invalidate('decode_failed')
        self.assertEqual(snapshot_state(model),snapshot_state(self.clone(model)))
        model.reset('new_epoch');self.assertEqual(snapshot_state(model),snapshot_state(self.clone(model)))

    def test_malformed_incomplete_or_noncanonical_state_is_rejected(self):
        model=SnapshotBook(key(),'deribit_full');model.receive(event(bids=levels(('100','2'))))
        original=snapshot_state(model)
        for name,value in (('schema','unknown'),('last_source',None),('observed_id',0),('epoch',True),
                           ('last_fingerprint','bad'),('state','unknown'),('signed_prices',1)):
            changed=deepcopy(original);changed[name]=value
            with self.subTest(name=name):
                with self.assertRaises(ValueError):restore_snapshot_state(changed)
        for side in ([['100.00','2']],[['100','0']],[['100',2.0]],[['100','2'],['100','3']]):
            changed=deepcopy(original);changed['levels']['bids']=side
            with self.assertRaises(ValueError):restore_snapshot_state(changed)
        changed=deepcopy(original);changed['surprise']=1
        with self.assertRaises(ValueError):restore_snapshot_state(changed)
        model.unserialized_field=1
        with self.assertRaisesRegex(ValueError,'complete snapshot-book'):snapshot_state(model)

    def test_state_resource_limits_do_not_silently_resize_profile(self):
        model=SnapshotBook(key(),'deribit_full',limits=BookLimits(2));value=snapshot_state(model)
        with self.assertRaisesRegex(ValueError,'level cap exceeds'):restore_snapshot_state(value,limits=BookStateLimits(max_levels_per_side=1))
        with self.assertRaisesRegex(ValueError,'byte bound'):snapshot_state(model,limits=BookStateLimits(max_bytes=1))


class NativeCheckpointTests(unittest.TestCase):
    setUp=fixtures.ReplayTests.setUp
    fixture=fixtures.ReplayTests.fixture

    def test_raw_verified_state_resumes_with_identical_native_suffix(self):
        for profile in ('deribit_full','bybit_standard'):
            with self.subTest(profile=profile):
                segment,close,plan=self.fixture(profile);before=materialize_snapshot([segment],close,plan)
                encoded=make_native_checkpoint([segment],close,plan)
                restored=verify_native_checkpoint(encoded,digest(encoded),[segment]);binding=plan.bindings[0]
                model=SnapshotBook(binding.target.instrument,profile,depth=binding.depth)
                for record in iter_segment_records(segment):
                    if record.receipt.stream_key==binding.ws_source:
                        native=_book_message(record,binding)
                        if native is not None:model.receive(native)
                self.assertEqual(snapshot_state(model),snapshot_state(restored.models[0]))
                self.assertEqual(restored.stream_frontiers[0]['last_receipt']['receive_seq'],1)
                self.assertEqual(restored.frontier['ledger_pin'],before['descriptor']['ledger_pin'])
                self.assertEqual(restored.frontier['owner_plan_dependencies'] if 'owner_plan_dependencies' in restored.frontier else [],[])
                suffix=event(binding.target.instrument,kind='delta',update_id=20,previous_id=11,
                    cross_sequence=300 if profile=='bybit_standard' else None,bids=levels(('100','8')))
                self.assertEqual(model.receive(suffix),restored.models[0].receive(suffix))
                self.assertEqual(snapshot_state(model),snapshot_state(restored.models[0]))
                self.assertEqual(materialize_snapshot([segment],close,plan),before)

    def test_hash_consistent_wrong_hidden_state_is_rejected_by_raw_replay(self):
        segment,close,plan=self.fixture();encoded=make_native_checkpoint([segment],close,plan)
        value=strict_json(encoded);value['books'][0]['state']['observed_id']=1000
        changed=canonical(value)
        with self.assertRaisesRegex(ValueError,'differs from complete raw replay'):
            verify_native_checkpoint(changed,digest(changed),[segment])
        with self.assertRaisesRegex(ValueError,'identity/schema'):
            verify_native_checkpoint(changed,digest(encoded),[segment])

    def test_missing_or_substituted_frontier_dependencies_fail(self):
        segment,close,plan=self.fixture();encoded=make_native_checkpoint([segment],close,plan)
        with self.assertRaises(ValueError):verify_native_checkpoint(encoded,digest(encoded),[])
        value=strict_json(encoded);value['frontier']['ledger_pin']['head_hash']='0'*64;changed=canonical(value)
        with self.assertRaisesRegex(ValueError,'differs from complete raw replay'):
            verify_native_checkpoint(changed,digest(changed),[segment])
        value=strict_json(encoded);value['books'][0]['terminated']=True;changed=canonical(value)
        with self.assertRaisesRegex(ValueError,'differs from complete raw replay'):
            verify_native_checkpoint(changed,digest(changed),[segment])

    def test_immutable_publish_retry_manifest_last_and_no_mismatching_overwrite(self):
        import data_collector.checkpoints as checkpoints
        segment,close,plan=self.fixture();root=self.root/'checkpoints';publish=checkpoints._publish_bytes
        def interrupted(path,data):
            if path.name=='manifest.json':raise OSError('crash before commit marker')
            return publish(path,data)
        encoded=make_native_checkpoint([segment],close,plan);identity=digest(encoded)
        with patch.object(checkpoints,'_publish_bytes',side_effect=interrupted):
            with self.assertRaises(OSError):publish_native_checkpoint(root,[segment],close,plan)
        with self.assertRaises(FileNotFoundError):read_native_checkpoint(root,identity,[segment])
        self.assertEqual(publish_native_checkpoint(root,[segment],close,plan),identity)
        self.assertEqual(read_native_checkpoint(root,identity,[segment]).checkpoint_id,identity)
        (root/'objects'/identity/'checkpoint.json').write_bytes(b'corrupt')
        with self.assertRaises(ValueError):publish_native_checkpoint(root,[segment],close,plan)

    def test_aggregate_level_limit_is_checked_before_constructing_models(self):
        segment,close,plan=self.fixture();encoded=make_native_checkpoint([segment],close,plan)
        limits=NativeCheckpointLimits(replay=replace(ReplayLimits(),max_total_levels=1))
        with patch('data_collector.checkpoints.restore_snapshot_state') as restore:
            with self.assertRaisesRegex(ValueError,'aggregate level bound'):
                verify_native_checkpoint(encoded,digest(encoded),[segment],limits=limits)
            restore.assert_not_called()

    def test_limits_and_unimplemented_buffered_profile_fail_explicitly(self):
        segment,close,plan=self.fixture()
        with self.assertRaisesRegex(ValueError,'checkpoint byte bound'):
            make_native_checkpoint([segment],close,plan,limits=NativeCheckpointLimits(max_bytes=1))
        segment,close,plan=self.fixture('bybit_full')
        with self.assertRaisesRegex(ValueError,'snapshot-seeded'):
            make_native_checkpoint([segment],close,plan)


class TerminatedCheckpointTests(unittest.TestCase):
    setUp=ends.DeribitEndTests.setUp
    fixture=ends.DeribitEndTests.fixture
    registry=ends.DeribitEndTests.registry
    replacement=ends.DeribitEndTests.replacement
    next_stamp=ends.DeribitEndTests.next_stamp
    declare=ends.DeribitEndTests.declare
    native=ends.DeribitEndTests.native
    allocate=ends.DeribitEndTests.allocate
    close_allocated=ends.DeribitEndTests.close_allocated
    open_ledger=ends.DeribitEndTests.open_ledger
    repair=ends.DeribitEndTests.repair
    start=ends.DeribitEndTests.start
    end=ends.DeribitEndTests.end

    def test_checkpoint_keeps_terminal_latch_owner_and_open_negative_pin(self):
        self.start();end=self.end();pin=self.repair();self.ledger_owner=self.open_ledger()
        closed=self.close_allocated(self.allocate(self.managed),self.managed)
        close_id=closed['descriptor']['close_record_id']
        encoded=make_native_checkpoint(self.writer.committed,close_id,self.managed)
        restored=verify_native_checkpoint(encoded,digest(encoded),self.writer.committed)
        self.assertEqual(restored.terminated,(True,));self.assertEqual(restored.models[0].state,'invalid')
        self.assertEqual(restored.frontier['ledger_pin'],asdict(pin))
        self.assertTrue(restored.frontier['owner_plan_dependencies'])
        self.assertEqual(restored.last_applied[0]['record_id'],self.anchor.record_id)
        self.assertNotEqual(restored.last_applied[0]['record_id'],end.record_id)
