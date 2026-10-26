from dataclasses import replace
from pathlib import Path
import json
import tempfile
import unittest

from data_collector.contracts import Provenance,RawRecord,Receipt,StreamKey,canonical
from data_collector.cutoffs import ClockAnchor,CutoffCoordinator,IngressWriter,MINUTE_NS
from data_collector.journal import Journal
from data_collector.ledger import DurableLedger
from data_collector.metadata import MetadataVersion
from data_collector.replay import BookBinding,ReplayLimits,SnapshotPlan,materialize_snapshot
from data_collector.segments import SegmentContext,iter_segment_records,seal_segment
from data_collector.validity import FeedTarget,GapCorrection
from tests.test_dc_contracts import key

C=MINUTE_NS


class ReplayTests(unittest.TestCase):
    def setUp(self):
        self.tmp=tempfile.TemporaryDirectory();self.addCleanup(self.tmp.cleanup)
        self.root=Path(self.tmp.name);self.counter=0

    def fixture(self,profile='deribit_full',*,native_gap=False,known_gap=False,effective=None,
                malformed_count=0,extra_topic=False,recover=False,stale_pin=False,new_metadata=False,
                missing_header=False,unknown_revision=False,stale_recovery=False,observed_ahead=False,second_cutoff=False,healthy=True):
        self.counter+=1;root=self.root/str(self.counter);root.mkdir()
        venue='deribit' if profile=='deribit_full' else 'bybit' if profile.startswith('bybit') else 'binance'
        product='option' if profile in {'deribit_full','bybit_standard'} else 'spot' if profile=='binance_spot' else 'linear'
        inst=key(venue=venue,native_product=product,native_symbol='BTC-SYNTHETIC-C' if product=='option' else 'BTCUSDT')
        ws=StreamKey('capture','process','ws');meta=StreamKey('capture','process','meta')
        seed=None if profile in {'deribit_full','bybit_standard'} else StreamKey('capture','process','seed')
        meta_receipt=Receipt('capture','process','meta',0,C-20,1080,'epoch')
        attrs=canonical(dict(base_currency='BTC',amount_unit='native',quote_currency='native'))
        version=MetadataVersion(inst,meta_receipt,None if unknown_revision else C-100 if effective is None else effective,attrs)
        target=FeedTarget(inst,profile,'owner-1')
        binding=BookBinding(target,ws,meta,version.version_id,seed,100 if profile=='bybit_standard' else None)
        plan=SnapshotPlan((binding,),'b'*64)
        provenance=Provenance('a'*64,'b'*64,plan.dependencies['universe'])
        def raw(connection,seq,utc,mono,channel,kind,body,fields=None):
            return RawRecord(Receipt('capture','process',connection,seq,utc,mono,'epoch'),venue,'production',product,
                             channel,kind,canonical(body),provenance,inst,canonical(fields or {}))
        with Journal(root/'active.wal',venue=venue,environment='production',create=True) as wal:
            ingress=IngressWriter(wal,'capture','process')
            coordinator=CutoffCoordinator(wal,capture_scope='capture',capture_id='capture',process_session_id='process',provenance=provenance)
            ledger=DurableLedger(wal,ledger_id='ledger',capture_scope='capture',capture_id='capture',process_session_id='process',provenance=provenance)
            initial_pin=ledger.initialize(utc_ns=C-100,monotonic_ns=1000,clock_epoch='epoch')
            anchor=ClockAnchor('process','epoch',C-100,1000,0,2)
            streams=[ws,meta]+([seed] if seed else [])
            coordinator.allocate(C,streams,plan.dependencies,anchor=anchor,observed_utc_ns=C-100,monotonic_ns=1000,clock_epoch='epoch')
            ingress.append(raw('meta',0,C-20,1080,'metadata','metadata_version',dict(schema='dc.metadata/2',
                effective_at_ns=version.effective_at_ns,attributes=json.loads(attrs),supersedes=None)))
            if profile=='deribit_full':
                channel=f'book.{inst.native_symbol}.100ms'
                first=dict(jsonrpc='2.0',method='subscription',params=dict(channel=channel,data=dict(type='snapshot',timestamp=1,
                    instrument_name=inst.native_symbol,change_id=10,bids=[['new',100,2]],asks=[['new',101,3]])))
                second=dict(jsonrpc='2.0',method='subscription',params=dict(channel=channel,data=dict(type='change',timestamp=2,
                    instrument_name=inst.native_symbol,change_id=11,prev_change_id=99 if native_gap else 10,bids=[['change',100,4]],asks=[])))
                if observed_ahead:second['params']['data']['change_id']=30
            elif profile.startswith('bybit'):
                channel=f'orderbook.100.{inst.native_symbol}' if profile=='bybit_standard' else f'orderbook.full.{inst.native_symbol}'
                first=dict(topic=channel,type='snapshot' if profile=='bybit_standard' else 'delta',ts=1,
                           data=dict(s=inst.native_symbol,u=10,seq=100,b=[['100','2']],a=[['101','3']]))
                second=dict(topic=channel,type='delta',ts=2,data=dict(s=inst.native_symbol,u=11,seq=200,b=[['100','4']],a=[]))
                if native_gap and profile=='bybit_full':second['data']['u']=12
            else:
                channel=inst.native_symbol.lower()+'@depth'
                first=dict(e='depthUpdate',E=1,s=inst.native_symbol,U=10,u=11 if profile=='binance_spot' else 10,b=[['100','2']],a=[['101','3']])
                second=dict(e='depthUpdate',E=2,s=inst.native_symbol,U=12 if profile=='binance_spot' else 11,u=12 if profile=='binance_spot' else 20,b=[['100','4']],a=[])
                if native_gap:second.update(U=99,u=100)
                if profile=='binance_um':first['pu']=9;second['pu']=999 if native_gap else 10
            original=ingress.append(raw('ws',0,C-10,1090,channel,'ws_message',first))
            if seed:
                if profile=='bybit_full':
                    route='/v5/market/full_orderbook';body=dict(retCode=0,result=dict(s=inst.native_symbol,u=10,seq=100,b=[['100','2']],a=[['101','3']]))
                    fields=dict(request_symbol=inst.native_symbol,request_category=product)
                else:
                    route='/api/v3/depth' if profile=='binance_spot' else '/fapi/v1/depth'
                    body=dict(lastUpdateId=10,bids=[['100','2']],asks=[['101','3']]);fields=dict(request_symbol=inst.native_symbol)
                ingress.append(raw('seed',0,C-5,1095,route,'rest_response',body,fields))
            next_seq=1
            if extra_topic:
                extra=json.loads(canonical(first))
                if profile.startswith('bybit'):extra['topic']=f'orderbook.{25 if profile=="bybit_standard" else 50}.{inst.native_symbol}'
                else:extra['params']['channel']=f'book.{inst.native_symbol}.agg2'
                ingress.append(raw('ws',next_seq,C-4,1096,'multiplex','ws_message',extra));next_seq+=1
            if malformed_count:
                bad=json.loads(canonical(second))
                if profile=='deribit_full':
                    if missing_header:bad.pop('method')
                    else:bad['params']['data'].pop('instrument_name')
                elif profile.startswith('bybit'):
                    if missing_header:bad.pop('topic')
                    else:bad['data'].pop('s')
                else:bad.pop('s')
                for index in range(malformed_count):
                    latest=ingress.append(raw('ws',next_seq,C-2,1098,channel,'ws_message',bad));next_seq+=1
            else:
                latest=ingress.append(raw('ws',next_seq,C-(2 if recover else 1),1098 if recover else 1099,channel,'ws_message',second));next_seq+=1
            if recover:
                if profile=='deribit_full':
                    recovery=json.loads(canonical(first));recovery['params']['data'].update(change_id=12,bids=[['new',100,9]])
                    if stale_recovery:recovery=json.loads(canonical(first))
                    if observed_ahead:recovery['params']['data']['change_id']=20
                    ingress.append(raw('ws',next_seq,C-1,1099,channel,'ws_message',recovery))
                elif profile=='bybit_full':
                    recovery=json.loads(canonical(body));recovery['result'].update(u=12,seq=200,b=[['100','9']])
                    ingress.append(raw('seed',1,C-1,1099,route,'rest_response',recovery,fields))
                elif profile.startswith('binance'):
                    recovery=dict(lastUpdateId=100,bids=[['100','9']],asks=[['101','3']])
                    ingress.append(raw('seed',1,C-1,1099,route,'rest_response',recovery,fields))
                    bridged=dict(second,U=100,u=101,b=[['100','9']])
                    if profile=='binance_um':bridged['pu']=100
                    ingress.append(raw('ws',next_seq,C-1,1099,channel,'ws_message',bridged))
            if new_metadata or unknown_revision:
                ingress.append(raw('meta',1,C-1,1099,'metadata','metadata_version',dict(schema='dc.metadata/2',
                    effective_at_ns=None if unknown_revision else C-50,attributes=dict(base_currency='BTC',amount_unit='changed'),
                    supersedes=version.version_id if unknown_revision else None)))
            if known_gap or stale_pin:
                ledger.append(GapCorrection('capture',target,0,None,latest.record_id,original.record_id,'late_gap'),
                              utc_ns=C-1,monotonic_ns=1099,clock_epoch='epoch')
            barrier=ingress.barrier(anchor,C,now_monotonic_ns=1100,healthy_streams=streams if healthy else [])
            coordinator.close(0,barrier,ledger_pin=initial_pin if stale_pin else ledger.pin(),utc_ns=C,monotonic_ns=1100,clock_epoch='epoch')
            close_id=coordinator.resolutions[0]['record_id']
            if second_cutoff:
                coordinator.allocate(2*C,streams,plan.dependencies,anchor=anchor,observed_utc_ns=C,
                    monotonic_ns=1100,clock_epoch='epoch')
                quiet=ingress.barrier(anchor,2*C,now_monotonic_ns=C+1100,healthy_streams=streams if healthy else [])
                coordinator.close(1,quiet,ledger_pin=ledger.pin(),utc_ns=2*C,monotonic_ns=C+1100,clock_epoch='epoch')
                close_id=[close_id,coordinator.resolutions[1]['record_id']]
            context=SegmentContext(venue,'production','capture','process','segment','a'*64,'b'*64)
            segment=seal_segment(wal,root/'archive',context)
        return segment,close_id,plan

    def test_five_profiles_materialize_raw_and_metadata_at_saved_positions(self):
        for profile in ['deribit_full','bybit_standard','bybit_full','binance_spot','binance_um']:
            with self.subTest(profile=profile):
                segment,close,plan=self.fixture(profile)
                result=materialize_snapshot([segment],close,plan)
                book=json.loads(result['payload'])['books'][0]
                self.assertEqual(book['validity_as_published'],'valid')
                self.assertEqual(book['book']['bids'],[['100','4']])
                self.assertEqual(book['metadata_version'],plan.bindings[0].metadata_version_id)
                for row in result['descriptor']['admitted_positions']:
                    applied=next(r for r in result['descriptor']['applied_positions'] if r['stream']==row['stream'])
                    self.assertEqual(applied['receive_seq'],row['admitted_seq'])
                self.assertEqual(materialize_snapshot([segment],close,plan),result)

    def test_later_low_utc_segment_cannot_change_closed_membership_or_snapshot_id(self):
        segment,close,plan=self.fixture();original=materialize_snapshot([segment],close,plan)
        with Journal(self.root/'later.wal',venue='deribit',environment='production',create=True) as wal:
            inst=plan.bindings[0].target.instrument
            wal.append(RawRecord(Receipt('capture','process','ws',2,C-50,1101,'epoch'),'deribit','production','option',
                'multiplex','ws_message',b'{"later":"low UTC"}',Provenance('a'*64,'b'*64,plan.dependencies['universe']),inst))
            later=seal_segment(wal,self.root/'later-archive',SegmentContext('deribit','production','capture','process','later','a'*64,'b'*64))
        self.assertEqual(materialize_snapshot([segment,later],close,plan),original)

    def test_native_gap_and_already_known_ledger_gap_are_invalid_without_value_substitution(self):
        for params in [dict(native_gap=True),dict(known_gap=True)]:
            segment,close,plan=self.fixture(**params)
            result=materialize_snapshot([segment],close,plan)
            book=json.loads(result['payload'])['books'][0]
            self.assertEqual(book['validity_as_published'],'invalid')
            self.assertEqual(book['book']['bids'],[['100','2' if params.get('native_gap') else '4']])

    def test_future_metadata_or_changed_plan_cannot_be_silently_used(self):
        segment,close,plan=self.fixture(effective=C+1)
        with self.assertRaisesRegex(ValueError,'not effective'):materialize_snapshot([segment],close,plan)
        segment,close,plan=self.fixture()
        with self.assertRaisesRegex(ValueError,'allocated dependency'):
            materialize_snapshot([segment],close,replace(plan,config_sha256='f'*64))

    def test_missing_admitted_raw_record_fails_even_if_last_position_is_present(self):
        segment,close,plan=self.fixture()
        records=list(iter_segment_records(segment))
        with Journal(self.root/'missing.wal',venue='deribit',environment='production',create=True) as wal:
            for record in records:
                if record.receipt.connection_id=='ws' and record.receipt.receive_seq==0:continue
                wal.append(record)
            damaged=seal_segment(wal,self.root/'missing-archive',SegmentContext('deribit','production','capture','process','missing','a'*64,'b'*64))
        with self.assertRaisesRegex(ValueError,'raw prefix missing'):materialize_snapshot([damaged],close,plan)

    def test_universe_is_not_truncated_to_meet_resource_bounds(self):
        segment,close,plan=self.fixture()
        with self.assertRaisesRegex(ValueError,'aggregate L2'):materialize_snapshot([segment],close,plan,limits=ReplayLimits(max_total_levels=1))
        with self.assertRaisesRegex(ValueError,'payload byte'):materialize_snapshot([segment],close,plan,limits=ReplayLimits(max_payload_bytes=1))

    def test_malformed_bound_symbols_never_silently_retain_a_valid_stale_book(self):
        for profile in ['deribit_full','bybit_standard','bybit_full','binance_spot','binance_um']:
            segment,close,plan=self.fixture(profile,malformed_count=1)
            book=json.loads(materialize_snapshot([segment],close,plan)['payload'])['books'][0]
            self.assertEqual(book['validity_as_published'],'invalid',profile)
            self.assertEqual(book['replay_diagnostics']['count'],1)

    def test_legitimate_other_topics_on_same_owner_do_not_merge_or_invalidate(self):
        for profile in ['deribit_full','bybit_standard','bybit_full']:
            segment,close,plan=self.fixture(profile,extra_topic=True)
            book=json.loads(materialize_snapshot([segment],close,plan)['payload'])['books'][0]
            self.assertEqual(book['validity_as_published'],'valid',profile)
            self.assertEqual(book['book']['bids'],[['100','4']])
            self.assertEqual(book['replay_diagnostics']['count'],0)

    def test_accepted_recovery_anchor_restores_current_epoch_not_historical_gaps(self):
        for profile in ['deribit_full','bybit_full']:
            segment,close,plan=self.fixture(profile,native_gap=True,recover=True)
            book=json.loads(materialize_snapshot([segment],close,plan)['payload'])['books'][0]
            self.assertEqual(book['validity_as_published'],'valid',profile)
            self.assertEqual(book['book']['bids'],[['100','9']])
            self.assertEqual(book['replay_diagnostics']['count'],1)
            segment,close,plan=self.fixture(profile,native_gap=True,recover=True,known_gap=True)
            book=json.loads(materialize_snapshot([segment],close,plan)['payload'])['books'][0]
            self.assertEqual(book['validity_as_published'],'invalid',profile)

    def test_close_cannot_hide_a_correction_already_durable_before_it(self):
        segment,close,plan=self.fixture(stale_pin=True)
        with self.assertRaisesRegex(ValueError,'excludes corrections'):materialize_snapshot([segment],close,plan)

    def test_diagnostics_aggregate_repeated_failures_and_fail_before_budget_overrun(self):
        segment,close,plan=self.fixture(malformed_count=30)
        result=materialize_snapshot([segment],close,plan)
        diagnostics=json.loads(result['payload'])['books'][0]['replay_diagnostics']
        self.assertEqual(diagnostics['count'],30)
        self.assertIsInstance(diagnostics['first_reason'],str)
        with self.assertRaisesRegex(ValueError,'diagnostic projection bound'):
            materialize_snapshot([segment],close,plan,limits=ReplayLimits(max_diagnostic_bytes=1))

    def test_new_applicable_metadata_invalidates_stale_plan_instead_of_using_old_units(self):
        segment,close,plan=self.fixture(new_metadata=True)
        book=json.loads(materialize_snapshot([segment],close,plan)['payload'])['books'][0]
        self.assertEqual(book['validity_as_published'],'invalid')
        self.assertIn('metadata_changed_since_plan',book['reasons'])


    def test_missing_native_headers_are_not_silently_treated_as_control_ack(self):
        for profile in ['deribit_full','bybit_standard','bybit_full']:
            segment,close,plan=self.fixture(profile,malformed_count=1,missing_header=True)
            book=json.loads(materialize_snapshot([segment],close,plan)['payload'])['books'][0]
            self.assertEqual(book['validity_as_published'],'invalid',profile)
            self.assertEqual(book['replay_diagnostics']['count'],1)

    def test_explicit_unknown_boundary_revision_invalidates_obsolete_pin(self):
        segment,close,plan=self.fixture(unknown_revision=True)
        book=json.loads(materialize_snapshot([segment],close,plan)['payload'])['books'][0]
        self.assertEqual(book['validity_as_published'],'invalid')
        self.assertIn('metadata_superseded_since_plan',book['reasons'])

    def test_duplicate_old_anchor_does_not_recover_same_owner_gap(self):
        segment,close,plan=self.fixture(native_gap=True,recover=True,stale_recovery=True)
        book=json.loads(materialize_snapshot([segment],close,plan)['payload'])['books'][0]
        self.assertEqual(book['validity_as_published'],'invalid')
        self.assertEqual(book['book']['state'],'invalid')
        self.assertEqual(book['book']['update_id'],10)

    def test_recovery_must_reach_highest_observed_not_only_applied_position(self):
        segment,close,plan=self.fixture(native_gap=True,recover=True,observed_ahead=True)
        book=json.loads(materialize_snapshot([segment],close,plan)['payload'])['books'][0]
        self.assertEqual(book['validity_as_published'],'invalid')
        self.assertEqual(book['book']['update_id'],10)


if __name__=='__main__':unittest.main()
