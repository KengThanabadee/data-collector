from dataclasses import asdict,replace
from pathlib import Path
import json
import tempfile
import unittest
from unittest.mock import patch

from data_collector.clock import ClockSample
from data_collector.clocklog import DurableClock,SOURCE
from data_collector.clockcutoffs import ClockBoundCutoffs,verify_clock_binding
from data_collector.contracts import Provenance,RawRecord,Receipt,StreamKey,canonical,digest,strict_json
from data_collector.cutoffs import CutoffCoordinator,IngressWriter,MINUTE_NS
from data_collector.journal import Journal
from data_collector.ledger import DurableLedger
from data_collector.metadata import MetadataVersion
from data_collector.replay import BookBinding,SnapshotPlan,materialize_snapshot
from data_collector.segments import SegmentContext,seal_segment
from data_collector.snapshots import publish_snapshot,read_snapshot
from data_collector.validity import FeedTarget
from tests.test_dc_contracts import key

C=MINUTE_NS


class ClockCutoffTests(unittest.TestCase):
    def setUp(self):
        self.tmp=tempfile.TemporaryDirectory();self.addCleanup(self.tmp.cleanup)
        self.root=Path(self.tmp.name)
        self.inst=key();self.epoch='process:clock:0'
        self.ws=StreamKey('capture','process','ws');self.meta=StreamKey('capture','process','meta')
        self.version=MetadataVersion(self.inst,Receipt('capture','process','meta',0,C-20,1080,self.epoch),
            C-100,canonical(dict(base_currency='BTC',amount_unit='native',quote_currency='native')))
        self.plan=SnapshotPlan((BookBinding(FeedTarget(self.inst,'deribit_full','owner'),self.ws,self.meta,self.version.version_id),),'b'*64)
        self.provenance=Provenance('a'*64,'b'*64,self.plan.dependencies['universe'])
        self.wal=Journal(self.root/'active.wal',venue='deribit',environment='production',create=True)
        self.addCleanup(self.wal.close)
        self.ingress=IngressWriter(self.wal,'capture','process')
        self.coordinator=CutoffCoordinator(self.wal,capture_scope='capture',capture_id='capture',process_session_id='process',provenance=self.provenance)
        self.ledger=DurableLedger(self.wal,ledger_id='ledger',capture_scope='capture',capture_id='capture',process_session_id='process',provenance=self.provenance)
        self.ledger.initialize(utc_ns=C-200,monotonic_ns=900,clock_epoch=self.epoch)
        self.clock=DurableClock(self.wal,capture_id='capture',process_session_id='process',provenance=self.provenance,
            policy=dict(sample_interval_ns=60,max_sample_age_ns=1000,max_offset_ns=100,step_tolerance_ns=2,max_epochs=8))
        for sequence,mono in enumerate((940,1000)):
            source=RawRecord(Receipt('capture','process',SOURCE,sequence,C-1100+mono,mono,'measurement'),
                'deribit','production','control','clock_measurement','control',b'exact reference bytes '+str(sequence).encode(),self.provenance)
            sample=ClockSample(source.receipt.receive_utc_ns,mono,0,0,'boot',digest(source.to_bytes()))
            self.clock.sample(sample,source,ingress_ordinal=0)
        self.bound=ClockBoundCutoffs(self.clock,self.ingress,self.coordinator,self.ledger)

    def records(self):return [RawRecord.from_bytes(v) for v in self.wal.encoded_records(allow_unsealed=True)]

    def allocate(self,streams=None):
        return self.bound.allocate(C,streams or [self.ws],self.plan.dependencies,utc_ns=C-100,monotonic_ns=1000,boot_id='boot')

    def native(self,seq=0,mono=1090,body=b'{}'):
        return self.ingress.append(RawRecord(Receipt('capture','process','ws',seq,C-1100+mono,mono,self.epoch),
            'deribit','production',self.inst.native_product,f'book.{self.inst.native_symbol}.100ms','ws_message',body,self.provenance,self.inst))

    def close(self,mono=1100,utc=None,health=None):
        return self.bound.close(0,utc_ns=C-1100+mono if utc is None else utc,monotonic_ns=mono,boot_id='boot',
                                healthy_streams=[self.ws] if health is None else health)

    def close_record(self):
        identity=self.coordinator.resolutions[0]['record_id']
        return next(r for r in self.records() if r.record_id==identity)

    def verify(self,records=None,close=None,**kwargs):
        records=self.records() if records is None else records
        return verify_clock_binding(lambda:iter(records),self.close_record() if close is None else close,**kwargs)

    def test_checks_bind_allocation_and_closure_with_exact_deadline(self):
        allocated=self.allocate();self.native()
        result=self.close()['payload']
        self.assertEqual(result['validity'],'valid')
        self.assertEqual(result['streams'][0]['admitted_seq'],0)
        self.assertEqual(result['barrier']['deadline_ns'],1100)
        binding=self.verify()
        self.assertEqual(binding['close_clock_pin']['count'],4)
        self.assertEqual(allocated['payload']['clock_anchor']['healthy_samples'],2)
        self.assertEqual(result['ledger_pin'],asdict(self.ledger.pin()))

    def test_delayed_timer_excludes_post_deadline_raw_and_preserves_quiet_prefix(self):
        self.allocate();first=self.native()
        self.native(1,1101)
        result=self.close(1200)['payload']
        self.assertEqual(result['validity'],'valid')
        self.assertEqual(result['barrier']['deadline_ns'],1100)
        self.assertEqual(result['streams'][0]['admitted_seq'],0)
        self.assertEqual(result['streams'][0]['record_hash'],digest(first.to_bytes()))
        self.verify()

    def test_stale_or_changed_clock_resolves_as_failed_never_valid_close(self):
        for mono,utc in [(2101,C+1001),(1100,C-100)]:
            with self.subTest(mono=mono):
                fixture=ClockCutoffTests();fixture.setUp();self.addCleanup(fixture.doCleanups)
                fixture.allocate();fixture.native()
                result=fixture.close(mono,utc)
                self.assertEqual(result['kind'],'failed')
                self.assertEqual(result['payload']['reason'],'clock_changed_or_stale')
                self.assertNotIn('barrier',result['payload'])
                with self.assertRaises(ValueError):fixture.close(mono+1,utc+1)

    def test_unaligned_allocation_has_no_invented_deadline_and_cannot_reopen(self):
        result=self.bound.allocate(C,[self.ws],self.plan.dependencies,utc_ns=C-100+1001,monotonic_ns=2001,boot_id='boot')
        self.assertEqual(result['kind'],'failed')
        self.assertIsNone(self.coordinator.allocations[0]['payload']['deadline_ns'])
        with self.assertRaises(ValueError):self.bound.close(0,utc_ns=C+1000,monotonic_ns=2100,boot_id='boot',healthy_streams=[self.ws])

    def test_missing_health_retains_missing_rows_under_verified_clock_binding(self):
        self.allocate();self.native()
        result=self.close(health=[])['payload']
        self.assertEqual(result['validity'],'missing')
        self.assertEqual(result['streams'][0]['reason'],'barrier_health_missing')
        self.verify()

    def test_missing_or_tampered_link_clock_prefix_and_pin_fail_verification(self):
        self.allocate();self.native();self.close()
        records=self.records();close=self.close_record()
        link=next(r for r in records if r.channel=='clock_cutoff_links')
        for missing in (link,records[1]):
            with self.assertRaises(ValueError):self.verify([r for r in records if r is not missing],close)
        modified=replace(link,body=b'{}')
        with self.assertRaises(ValueError):self.verify([modified if r is link else r for r in records],close)
        body=strict_json(close.body)
        body['payload']['barrier']['clock_binding']['close_clock_pin']=dict(count=3,head_sha256='a'*64,last_record_id='b'*64)
        modified=replace(close,body=canonical(body))
        with self.assertRaises(ValueError):self.verify([modified if r.record_id==close.record_id else r for r in records],modified)
        with self.assertRaises(ValueError):self.verify(max_records=1)
        with self.assertRaises(ValueError):self.verify(max_scan_bytes=1)

    def test_interruption_between_allocation_and_link_cannot_yield_a_proven_close(self):
        original=self.wal.append
        def fail(record):
            if record.channel=='clock_cutoff_links':raise OSError('injected link fsync failure')
            return original(record)
        with patch.object(self.wal,'append',side_effect=fail):
            with self.assertRaises(OSError):self.allocate()
        self.assertTrue(self.bound.poisoned)
        self.assertEqual(len(self.coordinator.allocations),1)
        self.assertEqual(self.coordinator.resolutions,{})
        with self.assertRaises(ValueError):self.close()
        restored=CutoffCoordinator(self.wal,capture_scope='capture',capture_id='capture',process_session_id='restart',provenance=self.provenance)
        event=restored.fail(0,'interrupted_without_clock_link',utc_ns=C,monotonic_ns=1,clock_epoch='new')
        self.assertEqual(event['kind'],'failed')
        self.assertEqual(restored.allocations[0],self.coordinator.allocations[0])

    def test_link_projection_limit_fails_closed_and_prevents_followup_work(self):
        limited=ClockBoundCutoffs(self.clock,self.ingress,self.coordinator,self.ledger,max_link_bytes=1)
        with self.assertRaises(ValueError):limited.allocate(C,[self.ws],self.plan.dependencies,utc_ns=C-100,monotonic_ns=1000,boot_id='boot')
        self.assertTrue(limited.poisoned)
        with self.assertRaises(ValueError):limited.close(0,utc_ns=C,monotonic_ns=1100,boot_id='boot',healthy_streams=[self.ws])

    def snapshot_prefix(self):
        self.allocate([self.ws,self.meta])
        metadata=RawRecord(self.version.receipt,'deribit','production',self.inst.native_product,'metadata','metadata_version',
            canonical(dict(schema='dc.metadata/2',effective_at_ns=self.version.effective_at_ns,
                           attributes=self.version.attributes,supersedes=None)),self.provenance,self.inst)
        self.ingress.append(metadata)
        channel=f'book.{self.inst.native_symbol}.100ms'
        body=canonical(dict(jsonrpc='2.0',method='subscription',params=dict(channel=channel,data=dict(
            type='snapshot',timestamp=1,instrument_name=self.inst.native_symbol,change_id=10,
            bids=[['new',100,2]],asks=[['new',101,3]]))))
        self.native(body=body)
        self.close(health=[self.ws,self.meta])
        return self.close_record()

    def test_bound_closure_materializes_and_publishes_with_verified_clock_dependencies(self):
        close=self.snapshot_prefix()
        segment=seal_segment(self.wal,self.root/'archive',SegmentContext('deribit','production','capture','process','segment','a'*64,'b'*64))
        result=materialize_snapshot([segment],close.record_id,self.plan)
        self.assertEqual(json.loads(result['payload'])['books'][0]['validity_as_published'],'valid')
        published=publish_snapshot(self.root/'snapshots',[segment],close.record_id,self.plan)
        self.assertEqual(read_snapshot(self.root/'snapshots',published['snapshot_id'],[segment]),published)

    def test_materializer_refuses_bound_closure_with_clock_link_missing(self):
        close=self.snapshot_prefix();records=[r for r in self.records() if r.channel!='clock_cutoff_links']
        with Journal(self.root/'missing-link.wal',venue='deribit',environment='production',create=True) as writer:
            for record in records:writer.append(record)
            segment=seal_segment(writer,self.root/'bad-archive',SegmentContext('deribit','production','capture','process','segment','a'*64,'b'*64))
        with self.assertRaisesRegex(ValueError,'clock allocation/link/close evidence missing'):
            materialize_snapshot([segment],close.record_id,self.plan)

    def test_release_clock_policy_ceiling_rejects_relaxed_live_owner(self):
        for name,value in [('max_offset_ns',100_000_001),('max_sample_age_ns',60_000_000_001),
                           ('sample_interval_ns',60_000_000_001)]:
            policy=dict(self.clock.policy,**{name:value})
            if name=='sample_interval_ns':policy['max_sample_age_ns']=value
            with self.subTest(name=name),patch.object(self.clock,'_policy',policy):
                with self.assertRaisesRegex(ValueError,'clock policy exceeds'):
                    ClockBoundCutoffs(self.clock,self.ingress,self.coordinator,self.ledger)

    def test_standalone_verifier_rejects_previously_false_valid_relaxed_policy(self):
        fixture=json.loads((Path(__file__).parent/'fixtures'/'rejected_clock_policy_v2.json').read_text())
        self.assertTrue(fixture['previously_verified'])
        self.assertEqual(fixture['generator_source_sha256'],
                         '00bd5680f5f6b654ed3064bff65e83a5fa389eb29cc066135ffc27932621a4a4')
        records=[RawRecord.from_bytes(canonical(value)) for value in fixture['records']]
        close=next(r for r in records if r.record_id==fixture['close_record_id'])
        self.assertEqual(strict_json(close.body)['payload']['validity'],'valid')
        with self.assertRaisesRegex(ValueError,'clock policy exceeds'):
            self.verify(records,close)

    def test_standalone_verifier_rejects_foreign_close_control_namespace(self):
        self.allocate();self.native();self.close()
        close=self.close_record();records=self.records()
        for mutation in [dict(kind='ws_message',channel='book.fake'),dict(native_product='option'),
                         dict(native_fields_json=b'{"unexpected":1}')]:
            changed=replace(close,**mutation)
            prefix=[changed if r.record_id==close.record_id else r for r in records]
            with self.subTest(mutation=mutation),self.assertRaisesRegex(ValueError,'durable closed cutoff'):
                self.verify(prefix,changed)

    def test_standalone_verifier_rejects_rehashed_changed_allocation_deadline(self):
        self.allocate();self.native();self.close()
        records=self.records();close=self.close_record()
        allocation=next(r for r in records if r.record_id==self.coordinator.allocations[0]['record_id'])
        body=strict_json(allocation.body);body['payload']['deadline_ns']+=1
        changed_allocation=replace(allocation,body=canonical(body))
        link=next(r for r in records if r.channel=='clock_cutoff_links')
        link_body=strict_json(link.body);link_body['allocation_sha256']=digest(changed_allocation.to_bytes())
        changed_link=replace(link,body=canonical(link_body))
        close_body=strict_json(close.body)
        close_body['previous_hash']=digest(changed_allocation.body)
        close_body['payload']['allocation_hash']=digest(changed_allocation.to_bytes())
        close_body['payload']['barrier']['clock_binding']['allocation_link_sha256']=digest(changed_link.to_bytes())
        changed_close=replace(close,body=canonical(close_body))
        replacements={r.record_id:r for r in [changed_allocation,changed_link,changed_close]}
        changed=[replacements.get(r.record_id,r) for r in records]
        with self.assertRaisesRegex(ValueError,'allocated clock anchor/epoch/barrier changed'):
            self.verify(changed,changed_close)

    def test_clock_binding_replays_supported_live_log_beyond_default_ten_thousand(self):
        self.clock.max_assessments=10010
        # Stress the live/replay bound; other tests use actual fsync. Suppress
        # fsync only here to avoid ten thousand physical durability operations.
        with patch('data_collector.journal.os.fsync'):
            for _ in range(9998):
                self.clock.check(utc_ns=C-100,monotonic_ns=1000,boot_id='boot',ingress_ordinal=0)
            self.allocate();self.native();self.close()
        self.assertEqual(self.clock.pin.count,10002)
        self.verify()


if __name__=='__main__':unittest.main()
