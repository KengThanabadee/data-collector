from dataclasses import replace
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

from data_collector.contracts import Provenance,RawRecord,Receipt,StreamKey,canonical,digest,strict_json
from data_collector.cutoffs import ClockAnchor,CutoffCoordinator,IngressWriter
from data_collector.discoveries import pending_native_discoveries
from data_collector.exports import export_snapshots,read_export
from data_collector.gapmap import map_native_gap
from data_collector.journal import Journal
from data_collector.ledger import DurableLedger
from data_collector.metadata import MetadataVersion
from data_collector.owners import OwnerPlans
from data_collector.replay import BookBinding,SnapshotPlan,materialize_snapshot
from data_collector.segments import SegmentContext,seal_segment
from data_collector.recovery import RecoverySegment,iter_recovery_prefix,reconcile_cutoffs,verify_recovery_marker
from data_collector.validity import FeedTarget,GapCorrection
from data_collector.snapshots import publish_snapshot,read_snapshot
from tests.test_dc_contracts import key


class BufferedDiscoveryTests(unittest.TestCase):
    def setUp(self):
        self.tmp=tempfile.TemporaryDirectory();self.addCleanup(self.tmp.cleanup)
        self.root=Path(self.tmp.name);self.mono=0;self.positions={}

    def fixture(self,profile='bybit_full',symbols=('BTCUSDT',)):
        self.positions={};self.mono=0
        self.profile=profile;self.venue='bybit' if profile=='bybit_full' else 'binance'
        self.product='spot' if profile=='binance_spot' else 'linear'
        self.path=self.root/(profile+'.wal')
        self.wal=Journal(self.path,venue=self.venue,environment='production',create=True)
        self.addCleanup(self.wal.close)
        self.provenance=Provenance('a'*64,'b'*64,'c'*64)
        self.ingress=IngressWriter(self.wal,'capture','process')
        bindings=[]
        for i,symbol in enumerate(symbols):
            inst=key(venue=self.venue,native_product=self.product,native_symbol=symbol)
            ws=StreamKey('capture','process','ws');meta=replace(ws,connection_id='meta')
            seed=replace(ws,connection_id='seed')
            attrs=dict(base_currency=symbol[:-4],amount_unit='native',quote_currency='native')
            receipt=Receipt('capture','process','meta',i,i+1,i+1,'epoch')
            version=MetadataVersion(inst,receipt,0,canonical(attrs))
            self.positions[meta]=i+1;self.mono=i+1
            self.ingress.append(RawRecord(receipt,self.venue,'production',self.product,'metadata','metadata_version',
                canonical(dict(schema='dc.metadata/2',effective_at_ns=0,attributes=attrs,supersedes=None)),self.provenance,inst))
            bindings.append(BookBinding(FeedTarget(inst,profile,'dc-owner/1:'+str(i+1)*32),ws,meta,
                version.version_id,seed_source=seed))
        self.plan=SnapshotPlan(tuple(bindings),'b'*64)
        # Universe provenance is opaque source metadata; the managed declaration
        # itself carries the frozen plan's computed universe dependency.
        self.coordinator=CutoffCoordinator(self.wal,capture_scope='capture',capture_id='capture',
            process_session_id='process',provenance=self.provenance)
        registry=OwnerPlans(self.wal,capture_scope='capture',capture_id='capture',process_session_id='process',provenance=self.provenance)
        registry.declare(self.plan,utc_ns=self.mono+1,monotonic_ns=self.mono+1,clock_epoch='epoch')
        self.mono+=2
        self.open_ledger().initialize(utc_ns=self.mono,monotonic_ns=self.mono,clock_epoch='epoch')
        return self.plan.bindings[0]

    def open_ledger(self):
        return DurableLedger(self.wal,ledger_id='ledger',capture_scope='capture',capture_id='capture',
            process_session_id='process',provenance=self.provenance)

    def raw(self,binding,*,seed=False,update=10,seq=100,previous=None,levels=None,empty=False,wait=1,foreign=False,malformed=False):
        self.mono+=wait;inst=binding.target.instrument
        if foreign:inst=replace(inst,native_symbol='OTHERUSDT')
        symbol=inst.native_symbol;levels=levels if levels is not None else [['100','2']]
        source=binding.seed_source if seed else binding.ws_source
        position=self.positions.get(source,0);self.positions[source]=position+1
        fields={};kind='rest_response' if seed else 'ws_message'
        if self.profile=='bybit_full':
            if seed:
                route='/v5/market/full_orderbook';fields=dict(request_category=self.product,request_symbol=symbol)
                body=dict(retCode=0,result=dict(s=symbol,u=update,seq=seq,b=[] if empty else levels,a=[]))
            else:
                route='orderbook.full.'+symbol
                body=dict(topic=route,type='delta',ts=1,data=dict(s=symbol,u=update,seq=seq,b=levels,a=[]))
        else:
            if seed:
                route='/api/v3/depth' if self.profile=='binance_spot' else '/fapi/v1/depth'
                fields=dict(request_symbol=symbol);body=dict(lastUpdateId=update,bids=levels,asks=[])
            else:
                route=symbol.lower()+'@depth'
                body=dict(e='depthUpdate',E=1,s=symbol,U=update-1 if self.profile=='binance_spot' else update,u=update,b=levels,a=[])
                if self.profile=='binance_um':body['pu']=update-1 if previous is None else previous
        return self.ingress.append(RawRecord(Receipt('capture','process',source.connection_id,position,self.mono,self.mono,'epoch'),
            self.venue,'production',self.product,route,kind,b'{' if malformed else canonical(body),self.provenance,inst,canonical(fields)))

    def pending(self,**kwargs):return pending_native_discoveries(self.open_ledger(),**kwargs)

    def repair(self):
        self.mono+=1
        return self.open_ledger().complete_native_discoveries(self.coordinator,
            utc_ns=self.mono,monotonic_ns=self.mono,clock_epoch='epoch')

    def bootstrap(self,binding):
        update=11 if self.profile=='binance_spot' else 10
        self.raw(binding,update=update);return self.raw(binding,seed=True,update=10)

    def test_supported_profiles_gap_and_exact_idempotent_negative(self):
        for profile in ('bybit_full','binance_spot','binance_um'):
            with self.subTest(profile=profile):
                binding=self.fixture(profile);self.bootstrap(binding)
                gap=self.raw(binding,update=15,seq=105,previous=999)
                self.assertEqual([d.record_id for d in self.pending()],[gap.record_id])
                with self.assertRaisesRegex(ValueError,'unmapped native discovery'):self.open_ledger().pin()
                pin=self.repair();self.assertEqual(self.repair(),pin)
                negatives=[e.payload for e in self.open_ledger().entries(pin) if isinstance(e.payload,GapCorrection)]
                self.assertEqual([n.evidence_record_id for n in negatives],[gap.record_id])
                self.assertEqual(self.pending(),())

    def test_foreign_ws_and_seed_do_not_start_bootstrap_or_invalidate(self):
        binding=self.fixture();self.raw(binding,foreign=True);self.raw(binding,seed=True,foreign=True)
        self.raw(binding,wait=100_000_000_000);self.raw(binding,seed=True)
        self.assertEqual(self.pending(),())

    def test_timeout_ignored_return_is_still_a_gap_and_replay_diagnostic(self):
        binding=self.fixture();streams=[binding.ws_source,binding.seed_source,binding.metadata_source]
        anchor=ClockAnchor('process','epoch',0,0,0,2)
        self.coordinator.allocate(60_000_000_000,streams,self.plan.dependencies,anchor=anchor,
            observed_utc_ns=self.mono,monotonic_ns=self.mono,clock_epoch='epoch')
        self.raw(binding);timeout=self.raw(binding,update=11,seq=101,wait=30_000_000_000)
        self.raw(binding,seed=True,update=11,seq=101)
        self.assertEqual([d.record_id for d in self.pending()],[timeout.record_id])
        pin=self.repair()
        barrier=self.ingress.barrier(anchor,60_000_000_000,now_monotonic_ns=60_000_000_000,healthy_streams=streams)
        self.coordinator.close(0,barrier,ledger_pin=pin,utc_ns=60_000_000_000,monotonic_ns=60_000_000_000,clock_epoch='epoch')
        segment=seal_segment(self.wal,self.root/'archive',SegmentContext(self.venue,'production','capture','process','segment','a'*64,'b'*64))
        result=materialize_snapshot([segment],self.coordinator.resolutions[0]['record_id'],self.plan)
        row=strict_json(result['payload'])['books'][0]
        self.assertEqual(row['replay_diagnostics']['count'],1)
        self.assertEqual(row['replay_diagnostics']['first_record_id'],timeout.record_id)
        self.assertEqual(row['validity_as_published'],'unknown')

    def test_empty_bybit_reset_reavailability_then_gap_maps(self):
        binding=self.fixture();self.raw(binding,update=1,seq=1)
        self.raw(binding,seed=True,update=1,seq=1,empty=True)
        self.raw(binding,update=10,seq=100,wait=100_000_000_000)
        self.raw(binding,seed=True,update=10,seq=100)
        gap=self.raw(binding,update=12,seq=102)
        self.assertEqual([d.record_id for d in self.pending()],[gap.record_id])
        pin=self.repair();self.assertEqual(self.pending(),())
        audit=next(RawRecord.from_bytes(e) for e in self.wal.encoded_records(allow_unsealed=True)
            if RawRecord.from_bytes(e).channel=='gap_mapping:capture')
        self.assertEqual(strict_json(audit.body)['buffered_policy'],'reset-and-timeout/1')
        self.assertEqual(list(self.open_ledger().entries(pin))[-1].payload.evidence_record_id,gap.record_id)

    def test_repeated_gaps_and_malformed_seed_repair_in_raw_order(self):
        binding=self.fixture();self.bootstrap(binding)
        first=self.raw(binding,update=12,seq=102)
        self.raw(binding,seed=True,update=12,seq=102)
        second=self.raw(binding,update=14,seq=104)
        third=self.raw(binding,seed=True,malformed=True)
        self.assertEqual([d.record_id for d in self.pending()],[first.record_id,second.record_id,third.record_id])
        pin=self.repair();gaps=[e.payload for e in self.open_ledger().entries(pin) if isinstance(e.payload,GapCorrection)]
        self.assertEqual([g.evidence_record_id for g in gaps],[first.record_id,second.record_id,third.record_id])

    def test_aggregate_buffers_fail_closed_without_inventing_native_gap(self):
        self.fixture(symbols=('BTCUSDT','ETHUSDT'))
        for binding in self.plan.bindings:self.raw(binding)
        self.assertEqual(self.pending(),())
        with self.assertRaisesRegex(ValueError,'aggregate buffer event bound'):self.pending(max_buffer_events=1)
        with self.assertRaisesRegex(ValueError,'aggregate buffer byte bound'):self.pending(max_buffer_bytes=1)
        self.assertEqual(self.pending(),())

    def test_retained_seed_and_live_last_delta_are_charged(self):
        binding=self.fixture();self.raw(binding,seed=True,levels=[['100','2'],['99','3']])
        with self.assertRaisesRegex(ValueError,'aggregate level bound'):self.pending(max_levels=1)
        with self.assertRaisesRegex(ValueError,'aggregate buffer byte bound'):self.pending(max_buffer_bytes=1)
        self.raw(binding)
        self.assertEqual(self.pending(),())
        with self.assertRaisesRegex(ValueError,'aggregate buffer byte bound'):self.pending(max_buffer_bytes=1)

    def test_legacy_audit_without_policy_retries_without_changing_bytes(self):
        binding=self.fixture();self.bootstrap(binding);gap=self.raw(binding,update=12,seq=102)
        self.repair()
        with Journal(self.root/'legacy.wal',venue=self.venue,environment='production',create=True) as target:
            expected=None
            for data in self.wal.encoded_records(allow_unsealed=True):
                record=RawRecord.from_bytes(data)
                if record.channel=='gap_mapping:capture':
                    body=strict_json(record.body);body.pop('buffered_policy')
                    record=replace(record,body=canonical(body));expected=record.to_bytes()
                target.append(record)
            ledger=DurableLedger(target,ledger_id='ledger',capture_scope='capture',capture_id='capture',
                process_session_id='process',provenance=self.provenance)
            coordinator=CutoffCoordinator(target,capture_scope='capture',capture_id='capture',
                process_session_id='process',provenance=self.provenance)
            before=tuple(target.encoded_records(allow_unsealed=True))
            result=map_native_gap(ledger,coordinator,self.plan,0,gap.record_id,
                utc_ns=self.mono+1,monotonic_ns=self.mono+1,clock_epoch='epoch')
            self.assertEqual(tuple(target.encoded_records(allow_unsealed=True)),before)
            self.assertEqual(result.mapping_sha256,digest(expected))

    def test_restart_repairs_unreported_buffered_gap_before_verified_marker(self):
        binding=self.fixture();self.bootstrap(binding);gap=self.raw(binding,update=12,seq=102)
        cutoff_pin=self.coordinator.pin
        path=seal_segment(self.wal,self.root/'archive',SegmentContext(self.venue,'production','capture','process','segment','a'*64,'b'*64))
        segments=(RecoverySegment(path,digest((path/'context.json').read_bytes()),digest((path/'manifest.json').read_bytes())),)
        with Journal(self.root/'restart.wal',venue=self.venue,environment='production',create=True) as target:
            def prior():return iter_recovery_prefix(segments,venue=self.venue,environment='production',
                capture_id='capture',process_session_id='restart')
            ledger=DurableLedger(target,ledger_id='ledger',capture_scope='capture',capture_id='capture',
                process_session_id='restart',provenance=self.provenance,prior_records=prior)
            coordinator=CutoffCoordinator(target,capture_scope='capture',capture_id='capture',
                process_session_id='restart',provenance=self.provenance,prior_records=prior)
            with self.assertRaisesRegex(ValueError,'unmapped native discovery'):ledger.pin()
            ledger=DurableLedger(target,ledger_id='ledger',capture_scope='capture',capture_id='capture',
                process_session_id='restart',provenance=self.provenance,prior_records=prior)
            pin=ledger.complete_native_discoveries(coordinator,utc_ns=self.mono+1,monotonic_ns=1,clock_epoch='restart')
            result=reconcile_cutoffs(target,segments,cutoff_pin,capture_scope='capture',capture_id='capture',
                process_session_id='restart',provenance=self.provenance,utc_ns=self.mono+2,monotonic_ns=2,clock_epoch='restart')
            records=tuple(RawRecord.from_bytes(e) for e in target.encoded_records(allow_unsealed=True))
            self.assertEqual([r.channel for r in records],['gap_mapping:capture','validity:capture','cutoff_recovery:capture'])
            self.assertEqual(list(ledger.entries(pin))[-1].payload.evidence_record_id,gap.record_id)
            self.assertEqual(verify_recovery_marker(segments,records[-1],records),result)
            with self.assertRaisesRegex(ValueError,'exactly the pending'):
                verify_recovery_marker(segments,records[-1],records[1:])

    def test_original_managed_snapshot_stays_readable_but_current_certification_blocks(self):
        binding=self.fixture();streams=[binding.ws_source,binding.seed_source,binding.metadata_source]
        anchor=ClockAnchor('process','epoch',0,0,0,2)
        self.coordinator.allocate(60_000_000_000,streams,self.plan.dependencies,anchor=anchor,
            observed_utc_ns=self.mono,monotonic_ns=self.mono,clock_epoch='epoch')
        self.raw(binding);timeout=self.raw(binding,update=11,seq=101,wait=30_000_000_000)
        self.raw(binding,seed=True,update=11,seq=101)
        # Reproduce an archive saved before buffered native discovery existed.
        # No timeout policy was durable before this original cutoff.
        with patch('data_collector.discoveries.pending_native_discoveries',return_value=()):
            self.mono+=1
            pin=self.open_ledger().append(GapCorrection('capture',binding.target,0,None,
                timeout.record_id,timeout.record_id,'operational_failure','invalid'),
                utc_ns=self.mono,monotonic_ns=self.mono,clock_epoch='epoch')
            barrier=self.ingress.barrier(anchor,60_000_000_000,now_monotonic_ns=60_000_000_000,healthy_streams=streams)
            self.coordinator.close(0,barrier,ledger_pin=pin,utc_ns=60_000_000_000,monotonic_ns=60_000_000_000,clock_epoch='epoch')
            path=seal_segment(self.wal,self.root/'archive',SegmentContext(self.venue,'production','capture','process','segment','a'*64,'b'*64))
            original=publish_snapshot(self.root/'snapshots',[path],self.coordinator.resolutions[0]['record_id'],self.plan)
            historical_assessed=export_snapshots(self.root/'snapshots',self.root/'old-assessed',
                [original['snapshot_id']],[path])
        self.assertEqual(read_export(self.root/'old-assessed',historical_assessed['export_id'],[path]),historical_assessed)
        row=strict_json(original['payload'])['books'][0]
        self.assertEqual(row['book']['state'],'invalid');self.assertEqual(row['replay_diagnostics']['count'],0)
        self.assertEqual(read_snapshot(self.root/'snapshots',original['snapshot_id'],[path]),original)
        exported=export_snapshots(self.root/'snapshots',self.root/'exports',[original['snapshot_id']],[path],mode='as_published')
        self.assertEqual(read_export(self.root/'exports',exported['export_id'],[path]),exported)
        with self.assertRaisesRegex(ValueError,'unmapped native discovery'):
            export_snapshots(self.root/'snapshots',self.root/'assessed',[original['snapshot_id']],[path])
        with self.assertRaisesRegex(ValueError,'unmapped native discovery'):self.open_ledger().pin()
        with self.assertRaisesRegex(ValueError,'unmapped native discovery'):
            publish_snapshot(self.root/'new-snapshots',[path],self.coordinator.resolutions[0]['record_id'],self.plan)
