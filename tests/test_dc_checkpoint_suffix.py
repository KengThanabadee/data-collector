from dataclasses import asdict,replace
from pathlib import Path
import tempfile
import unittest

from data_collector.checkpoints import make_native_checkpoint,materialize_from_checkpoint
from data_collector.contracts import Provenance,RawRecord,Receipt,StreamKey,canonical,digest,strict_json
from data_collector.cutoffs import ClockAnchor,CutoffCoordinator,IngressWriter
from data_collector.ledger import DurableLedger
from data_collector.metadata import MetadataVersion
from data_collector.replay import BookBinding,SnapshotPlan,materialize_snapshot
from data_collector.validity import FeedTarget
from data_collector.writer import SessionWriter
from tests.helpers import key
from tests import test_dc_deribit_ends as ends

C=60_000_000_000


class CheckpointSuffixTests(unittest.TestCase):
    def setUp(self):
        self.tmp=tempfile.TemporaryDirectory();self.addCleanup(self.tmp.cleanup)
        self.root=Path(self.tmp.name);self.case=0

    def fixture(self,profile='deribit_full'):
        self.case+=1;root=self.root/str(self.case);self.profile=profile
        self.venue='deribit' if profile=='deribit_full' else 'bybit'
        self.inst=key(venue=self.venue,native_symbol='BTC-SYNTHETIC-C')
        self.ws=StreamKey('capture','process','ws');self.meta=replace(self.ws,connection_id='meta')
        attrs=canonical(dict(base_currency='BTC',amount_unit='native',quote_currency='native'))
        metadata=MetadataVersion(self.inst,Receipt('capture','process','meta',0,C-20,1080,'epoch'),C-100,attrs)
        binding=BookBinding(FeedTarget(self.inst,profile,'owner'),self.ws,self.meta,metadata.version_id,
            depth=100 if profile=='bybit_standard' else None)
        self.plan=SnapshotPlan((binding,),'b'*64);self.provenance=Provenance('a'*64,'b'*64,self.plan.dependencies['universe'])
        self.writer=SessionWriter(root/'wal',root/'archive',venue=self.venue,environment='production',capture_id='capture',
            process_session_id='process',provenance=self.provenance,started_utc_ns=C-100,now_ns=lambda:0)
        self.addCleanup(self.writer.abort);self.ingress=IngressWriter(self.writer,'capture','process')
        args=dict(capture_scope='scope',capture_id='capture',process_session_id='process',provenance=self.provenance)
        self.owner=CutoffCoordinator(self.writer,**args);self.ledger=DurableLedger(self.writer,ledger_id='ledger',**args)
        self.ledger.initialize(utc_ns=C-100,monotonic_ns=1000,clock_epoch='epoch')
        self.anchor=ClockAnchor('process','epoch',C-100,1000,0,2);self.streams=[self.ws,self.meta]
        self.allocate()
        self.ingress.append(RawRecord(metadata.receipt,self.venue,'production','option','metadata','metadata_version',
            canonical(dict(schema='dc.metadata/2',effective_at_ns=C-100,attributes=strict_json(attrs),supersedes=None)),self.provenance,self.inst))

    def allocate(self):
        identity=len(self.owner.allocations);mono=max(1000 if identity==0 else (identity-1)*C+1101,
            (self.ingress.last_mono or 0)+1)
        utc=C-100+mono-1000
        self.owner.allocate((identity+1)*C,self.streams,self.plan.dependencies,anchor=self.anchor,
            observed_utc_ns=utc,monotonic_ns=mono,clock_epoch=self.anchor.clock_epoch)
        return identity

    def native(self,update,*,previous=None,mono=1090,size='2'):
        binding=self.plan.bindings[0];snapshot=previous is None
        if self.profile=='deribit_full':
            data=dict(type='snapshot' if snapshot else 'change',timestamp=1,instrument_name=self.inst.native_symbol,
                change_id=update,bids=[['new' if snapshot else 'change','100',size]],asks=[['new','101','3']] if snapshot else [])
            if not snapshot:data['prev_change_id']=previous
            body=dict(jsonrpc='2.0',method='subscription',params=dict(channel=binding.native_channel,data=data))
        else:
            body=dict(topic=binding.native_channel,type='snapshot' if snapshot else 'delta',ts=1,
                data=dict(s=self.inst.native_symbol,u=update,seq=update*10,b=[['100',size]],a=[['101','3']] if snapshot else []))
        sequence=self.ingress.positions.get(self.ws,-1)+1;utc=C-100+mono-1000
        return self.ingress.append(RawRecord(Receipt('capture','process','ws',sequence,utc,mono,'epoch'),
            self.venue,'production','option',binding.native_channel,'ws_message',canonical(body),self.provenance,self.inst))

    def close(self):
        identity=len(self.owner.allocations)-1;deadline=identity*C+1100
        barrier=self.ingress.barrier(self.anchor,(identity+1)*C,now_monotonic_ns=max(deadline,self.ingress.last_mono),healthy_streams=self.streams)
        stamp=max(deadline,self.ingress.last_mono)
        self.owner.close(identity,barrier,ledger_pin=self.ledger.pin(),utc_ns=C-100+stamp-1000,
            monotonic_ns=stamp,clock_epoch=self.anchor.clock_epoch)
        self.writer.rotate();return self.owner.resolutions[identity]['record_id']

    def resume(self,encoded,target):
        stats={};actual=materialize_from_checkpoint(encoded,digest(encoded),iter(self.writer.committed),target,self.plan,stats=stats)
        self.assertEqual(actual,materialize_snapshot(self.writer.committed,target,self.plan))
        return actual,stats

    def test_supported_profiles_dispatch_only_native_suffix_and_preserve_artifact(self):
        for profile in ('deribit_full','bybit_standard'):
            with self.subTest(profile=profile):
                self.fixture(profile);self.native(10);first=self.close()
                encoded=make_native_checkpoint(self.writer.committed,first,self.plan)
                self.allocate();self.native(11,previous=10,mono=C+1000,size='7');second=self.close()
                artifact,stats=self.resume(encoded,second)
                self.assertEqual(stats['native_mode'],'checkpoint_suffix')
                self.assertEqual(stats['renderer_binding_dispatches'],1);self.assertEqual(stats['renderer_checkpoint_skips'],1)
                self.assertEqual(strict_json(artifact['payload'])['books'][0]['book']['bids'],[['100','7']])
                self.assertEqual(self.resume(encoded,second)[0],artifact)

    def test_renderer_dispatch_work_depends_on_suffix_not_prefix_length(self):
        self.fixture();self.native(10)
        for update in range(11,139):self.native(update,previous=update-1,mono=1091)
        first=self.close();encoded=make_native_checkpoint(self.writer.committed,first,self.plan)
        self.allocate();self.native(139,previous=138,mono=C+1000);self.native(140,previous=139,mono=C+1001)
        second=self.close();_,stats=self.resume(encoded,second)
        self.assertEqual(stats['renderer_binding_dispatches'],2);self.assertEqual(stats['renderer_checkpoint_skips'],129)
        self.assertEqual(stats['control_certification'],'full_raw_prefix')

    def test_quiet_cutoff_needs_no_native_suffix_dispatch(self):
        self.fixture();self.native(10);first=self.close();encoded=make_native_checkpoint(self.writer.committed,first,self.plan)
        self.allocate();second=self.close();_,stats=self.resume(encoded,second)
        self.assertEqual(stats['renderer_binding_dispatches'],0);self.assertEqual(stats['renderer_checkpoint_skips'],1)

    def test_newly_admitted_receipt_before_checkpoint_close_is_not_skipped(self):
        self.fixture();self.native(10);late=self.native(11,previous=10,mono=1101,size='8')
        first=self.close();encoded=make_native_checkpoint(self.writer.committed,first,self.plan)
        self.assertEqual(strict_json(encoded)['frontier']['admitted_positions'][0]['admitted_seq'],0)
        self.allocate();second=self.close();artifact,stats=self.resume(encoded,second)
        book=strict_json(artifact['payload'])['books'][0]
        self.assertEqual(book['book']['last_source'],late.record_id);self.assertEqual(book['book']['bids'],[['100','8']])
        self.assertEqual(stats['renderer_binding_dispatches'],1)

    def test_cumulative_diagnostics_survive_native_recovery(self):
        self.fixture();self.native(10);bad=self.native(11,previous=999,mono=1099);first=self.close()
        encoded=make_native_checkpoint(self.writer.committed,first,self.plan)
        self.allocate();self.native(12,mono=C+1000);second=self.close();artifact,stats=self.resume(encoded,second)
        row=strict_json(artifact['payload'])['books'][0]
        self.assertEqual(row['replay_diagnostics']['count'],1)
        self.assertEqual(row['replay_diagnostics']['first_record_id'],bad.record_id)
        self.assertEqual(row['book']['state'],'live');self.assertEqual(stats['native_mode'],'checkpoint_suffix')

    def test_observed_watermark_blocks_stale_anchor_after_checkpoint(self):
        self.fixture();self.native(100,previous=99);first=self.close()
        encoded=make_native_checkpoint(self.writer.committed,first,self.plan)
        self.allocate();self.native(50,mono=C+1000);second=self.close();artifact,_=self.resume(encoded,second)
        row=strict_json(artifact['payload'])['books'][0]
        self.assertEqual(row['book']['state'],'bootstrapping');self.assertIsNone(row['book']['update_id'])

    def test_future_or_changed_plan_checkpoint_is_rejected(self):
        self.fixture();self.native(10);first=self.close();encoded=make_native_checkpoint(self.writer.committed,first,self.plan)
        with self.assertRaisesRegex(ValueError,'must precede'):
            materialize_from_checkpoint(encoded,digest(encoded),self.writer.committed,first,self.plan)
        changed=replace(self.plan,bindings=(replace(self.plan.bindings[0],target=replace(self.plan.bindings[0].target,owner_epoch='other')),))
        self.allocate();second=self.close()
        with self.assertRaisesRegex(ValueError,'same frozen plan'):
            materialize_from_checkpoint(encoded,digest(encoded),self.writer.committed,second,changed)

    def test_new_clock_epoch_with_nonextending_admission_uses_raw_fallback(self):
        self.fixture();self.native(10);first=self.close();encoded=make_native_checkpoint(self.writer.committed,first,self.plan)
        self.anchor=ClockAnchor('process','epoch2',C+1000,2100,0,2,2100,self.ingress.ordinal)
        self.ingress.append(RawRecord(Receipt('capture','process','meta',1,C+1000,2100,'epoch2'),
            self.venue,'production','option','metadata_health','control',b'{}',self.provenance,self.inst))
        self.allocate();second=self.close();artifact,stats=self.resume(encoded,second)
        self.assertEqual(stats['native_mode'],'raw_admission_fallback');self.assertEqual(stats['renderer_checkpoint_skips'],0)
        row=strict_json(artifact['payload'])['books'][0]
        self.assertEqual(row['book']['state'],'bootstrapping');self.assertNotEqual(row['validity_as_published'],'valid')

    def test_public_entrypoint_reverifies_hash_consistent_changed_checkpoint(self):
        self.fixture();self.native(10);first=self.close();encoded=make_native_checkpoint(self.writer.committed,first,self.plan)
        self.allocate();second=self.close();value=strict_json(encoded);value['books'][0]['state']['observed_id']=100
        changed=canonical(value)
        with self.assertRaisesRegex(ValueError,'complete raw replay'):
            materialize_from_checkpoint(changed,digest(changed),self.writer.committed,second,self.plan)


class ManagedCheckpointSuffixTests(unittest.TestCase):
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

    def test_new_audit_policy_falls_back_without_reinterpreting_checkpoint_state(self):
        self.start();first=self.original['descriptor']['close_record_id']
        encoded=make_native_checkpoint(self.writer.committed,first,self.managed)
        self.end();self.repair();self.ledger_owner=self.open_ledger()
        second=self.close_allocated(self.allocate(self.managed),self.managed)['descriptor']['close_record_id'];stats={}
        actual=materialize_from_checkpoint(encoded,digest(encoded),self.writer.committed,second,self.managed,stats=stats)
        self.assertEqual(actual,materialize_snapshot(self.writer.committed,second,self.managed))
        self.assertEqual(stats['native_mode'],'raw_policy_fallback');self.assertEqual(stats['renderer_checkpoint_skips'],0)
        self.assertEqual(strict_json(actual['payload'])['books'][0]['book']['state'],'invalid')

    def test_terminal_latch_and_open_negative_remain_effective_on_suffix(self):
        self.start();self.end();pin=self.repair();self.ledger_owner=self.open_ledger()
        first=self.close_allocated(self.allocate(self.managed),self.managed)['descriptor']['close_record_id']
        encoded=make_native_checkpoint(self.writer.committed,first,self.managed)
        self.native(self.managed,2)
        second=self.close_allocated(self.allocate(self.managed),self.managed)['descriptor']['close_record_id'];stats={}
        actual=materialize_from_checkpoint(encoded,digest(encoded),self.writer.committed,second,self.managed,stats=stats)
        self.assertEqual(actual,materialize_snapshot(self.writer.committed,second,self.managed))
        self.assertEqual(stats['native_mode'],'checkpoint_suffix')
        self.assertEqual(actual['descriptor']['ledger_pin'],asdict(pin))
        row=strict_json(actual['payload'])['books'][0]
        self.assertEqual(row['book']['state'],'invalid');self.assertEqual(row['validity_as_published'],'invalid')
