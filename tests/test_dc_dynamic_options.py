"""Durable runtime fixtures for a universe that changes on a healthy socket."""
import asyncio
from dataclasses import replace
from pathlib import Path
import time
import unittest
from unittest.mock import AsyncMock,patch

from data_collector.app import run,verify_run
from data_collector.contracts import canonical,strict_json
from data_collector.dynamic_options import LIFECYCLE,GLOBALS,INDEXES,instrument_channels,native_push
from data_collector.rawrun import RawOwner
from data_collector.segments import iter_segment_records
from tests import test_dc_app as fixtures
from tests.test_dc_deribit import Socket
from tests.test_dc_discovery import Response,response_body


class LiveSession:
    _retry_connection=False
    def __init__(self):
        self.socket=Socket();self.rows=[];self.calls=0;self.subscribed=set();self.rejected=False
        self.push=True;self.malformed=False;self.delay=None;self.connects=0;self.bad_push=False;self.closed=False;self.book_push=True;self.before_ack=False;self.index_push=True;self.suppress_reconnect_options=False
        async def send(body):
            value=strict_json(body.encode());self.socket.sent.append(value)
            method=value['method']
            if method in {'public/subscribe','public/unsubscribe'}:
                channels=value['params']['channels']
                if self.suppress_reconnect_options and self.connects>=2 and LIFECYCLE not in channels:return
                if self.rejected and LIFECYCLE not in channels:
                    self.socket.put(dict(jsonrpc='2.0',id=value['id'],error=dict(code=10028,message='too_many_requests')))
                    self.socket.put(dict(jsonrpc='2.0',method='heartbeat',params=dict(type='heartbeat')))
                    return
                if self.before_ack and method=='public/subscribe':
                    for channel in channels:
                        if channel.startswith('book.'):
                            self.book(channel)
                self.socket.put(dict(jsonrpc='2.0',id=value['id'],result=channels))
                if method=='public/subscribe':
                    for channel in channels:
                        if channel in INDEXES and self.index_push:
                            self.socket.put(dict(jsonrpc='2.0',method='subscription',params=dict(channel=channel,
                                data=dict(index_name=channel.removeprefix('deribit_price_index.'),price=100,timestamp=1))))
                        elif channel.startswith('book.') and self.book_push and self.push:
                            self.book(channel)
                        if channel.startswith('ticker.'):
                            name=channel[7:-6];self.subscribed.add(name)
                            if self.bad_push:self.socket.put(b'{broken')
                            if self.push:self.socket.put(dict(jsonrpc='2.0',method='subscription',params=dict(
                                channel=channel,data=dict(instrument_name=name))))
            else:self.socket.put(dict(jsonrpc='2.0',id=value['id'],result=True))
        self.socket.send_str=send
    def book(self,channel):
        self.socket.put(dict(jsonrpc='2.0',method='subscription',params=dict(channel=channel,
            data=dict(instrument_name=channel[5:-14],timestamp=1,change_id=1,bids=[[100,1]],asks=[[101,2]]))))
    async def __aenter__(self):return self
    async def __aexit__(self,*args):self.closed=True
    async def ws_connect(self,*args,**kwargs):
        if kwargs.get('compress')!=15:raise AssertionError('negotiate transport compression')
        self.connects+=1
        if self.socket.closed:
            send=self.socket.send_str;self.socket=Socket();self.socket.send_str=send
        return self.socket
    def get(self,*args,**kwargs):
        self.calls+=1
        return Response(b'{broken' if self.malformed else response_body(self.rows),delay=self.delay)
    def notify(self,name,state='open'):
        self.socket.put(dict(jsonrpc='2.0',method='subscription',params=dict(channel=LIFECYCLE,
            data=dict(instrument_name=name,state=state,timestamp=time.time_ns()//1_000_000))))


class DynamicOptionsTests(unittest.IsolatedAsyncioTestCase):
    setUp=fixtures.AppTests.setUp
    observe=fixtures.AppTests.observe
    clock_read=fixtures.AppTests.clock_read
    async def clock_wait(self,deadline):await asyncio.Event().wait()

    def setup_live(self):
        self.config=replace(self.config,capture_mode='raw',all_options=True,rediscover_options=True,
            raw_processes=1,symbols=(),duration_seconds=0,max_frames=0)
        self.session=LiveSession();self.session.rows=[self.row('BTC-OLD-C',1)]
        self.alignment=patch.object(RawOwner,'aligned',new=AsyncMock())
        self.alignment.start();self.addCleanup(self.alignment.stop)

    def row(self,name,identity,*,expiry=None,created=None):
        now=time.time_ns()//1_000_000
        return dict(instrument_name=name,instrument_id=identity,kind='option',base_currency='BTC',settlement_currency='BTC',
            creation_timestamp=created or now-1000,expiration_timestamp=expiry or now+3600000,is_active=True)

    async def wait_for(self,predicate,seconds=10):
        async with asyncio.timeout(seconds):
            while not predicate():await asyncio.sleep(.02)

    async def start_live(self):
        task=asyncio.create_task(run(self.config,session_factory=lambda:self.session,observer=self.observe,
            clock_read=self.clock_read,clock_wait=self.clock_wait))
        self.addAsyncCleanup(self.cleanup_task,task)
        await self.wait_for(lambda:task.done() or bool(list(self.root.glob('runs/*/ready.json'))))
        if task.done():self.fail(str(task.result()))
        return task

    async def cleanup_task(self,task):
        if not task.done():task.cancel()
        await asyncio.gather(task,return_exceptions=True)

    async def finish(self,task):
        await asyncio.sleep(.3);task.cancel();return await asyncio.wait_for(task,10)

    def records(self,result,metadata=False):
        root=Path(result['root']);descriptor=result
        if metadata:root=root/'metadata';descriptor=result['metadata']
        return [r for name in descriptor['segments'] for r in iter_segment_records(root/name)]

    async def test_lifecycle_addition_on_same_socket_has_request_ack_push_and_all_versions_verify(self):
        self.setup_live();task=await self.start_live()
        self.session.rows.append(self.row('BTC-NEW-P',2));self.session.notify('BTC-NEW-P')
        await self.wait_for(lambda:'BTC-NEW-P' in self.session.subscribed)
        result=await self.finish(task)
        self.assertEqual(result['status'],'raw_capture_complete',result)
        self.assertEqual(result['connections'],1)
        verified=verify_run(result['root'])
        self.assertTrue(verified['changing_universe_coverage_verified'])
        self.assertGreaterEqual(verified['universe_versions_verified'],2)
        evidence=[strict_json(r.body) for r in self.records(result) if r.channel=='option_subscription']
        new=next(row for e in evidence for row in e['options'] if row['symbol']=='BTC-NEW-P')
        self.assertLessEqual(new['observed_monotonic_ns'],new['request_monotonic_ns'])
        self.assertTrue(new['creation_deadline_met']);self.assertIn('request_record_id',new)
        raw=self.records(result);self.assertTrue(any(r.kind=='ws_message' and b'BTC-NEW-P' in r.body for r in raw))
        metadata=self.records(result,metadata=True)
        self.assertGreaterEqual(sum(r.kind=='rest_response' for r in metadata),2)

    async def test_expiry_retires_instrument_and_unsubscribes_without_reconnect(self):
        self.setup_live();expiry=time.time_ns()//1_000_000+4500
        self.session.rows=[self.row('BTC-OLD-C',1,expiry=expiry),self.row('BTC-LATER-P',2)]
        task=await self.start_live()
        await self.wait_for(lambda:any(v['method']=='public/unsubscribe' for v in self.session.socket.sent),seconds=8)
        result=await self.finish(task)
        self.assertEqual(result['status'],'raw_capture_complete',result)
        checked=verify_run(result['root']);self.assertTrue(checked['changing_universe_coverage_verified'])
        self.assertEqual(result['retired_options'],1)
        revisions=[strict_json(r.body)['revision'] for r in self.records(result,metadata=True) if r.channel=='option_universe']
        retired=next(v['retirements'][0] for v in revisions if v['retired'])
        self.assertEqual(retired['symbol'],'BTC-OLD-C');self.assertGreaterEqual(retired['retirement_utc_ns'],expiry*1_000_000)

    async def test_ack_without_first_push_never_initializes(self):
        self.setup_live();self.session.push=False
        task=asyncio.create_task(run(self.config,session_factory=lambda:self.session,observer=self.observe,
            clock_read=self.clock_read,clock_wait=self.clock_wait))
        self.addAsyncCleanup(self.cleanup_task,task)
        await self.wait_for(lambda:bool(self.session.subscribed))
        result=await self.finish(task)
        self.assertEqual(result['status'],'failed',result)
        self.assertTrue(verify_run(result['root'])['archive_verified'])
        self.assertFalse(verify_run(result['root'])['changing_universe_coverage_verified'])

    async def test_rejected_subscription_is_retained_and_run_fails(self):
        self.setup_live();self.session.rejected=True
        result=await asyncio.wait_for(run(self.config,session_factory=lambda:self.session,observer=self.observe,
            clock_read=self.clock_read,clock_wait=self.clock_wait),10)
        self.assertEqual(result['status'],'failed',result)
        self.assertEqual(result['rejected_subscriptions'],1);self.assertEqual(result['throttled_subscriptions'],1)
        self.assertTrue(verify_run(result['root'])['archive_verified'])
        self.assertTrue(any(b'too_many_requests' in r.body for r in self.records(result)))

    async def test_reconnect_rebuilds_current_plan_and_keeps_timing_proof(self):
        import aiohttp
        self.setup_live();task=await self.start_live()
        self.session.socket.incoming.put_nowait(aiohttp.WSMessage(aiohttp.WSMsgType.CLOSE,1000,''))
        await self.wait_for(lambda:self.session.connects==2 and len(self.session.socket.sent)>=3)
        result=await self.finish(task)
        self.assertEqual(result['status'],'raw_capture_complete',result)
        self.assertEqual(result['connections'],2)
        self.assertTrue(verify_run(result['root'])['changing_universe_coverage_verified'])

    async def test_notification_during_rest_fetch_forces_followup_refresh(self):
        self.setup_live();task=await self.start_live()
        gate=asyncio.Event();self.session.delay=gate
        self.session.rows.append(self.row('BTC-NEW-A-C',2));self.session.notify('BTC-NEW-A-C')
        await self.wait_for(lambda:self.session.calls>=2)
        self.session.rows.append(self.row('BTC-NEW-B-P',3));self.session.notify('BTC-NEW-B-P')
        await asyncio.sleep(.1);self.session.delay=None;gate.set()
        await self.wait_for(lambda:'BTC-NEW-B-P' in self.session.subscribed)
        result=await self.finish(task)
        self.assertEqual(result['status'],'raw_capture_complete',result)
        self.assertGreaterEqual(self.session.calls,3)
        self.assertGreaterEqual(verify_run(result['root'])['universe_versions_verified'],3)

    async def test_failed_metadata_refresh_keeps_old_members_and_retries(self):
        self.setup_live();task=await self.start_live()
        self.session.malformed=True;self.session.notify('BTC-OLD-C')
        await self.wait_for(lambda:self.session.calls>=2)
        await asyncio.sleep(.1);self.session.malformed=False
        self.session.rows.append(self.row('BTC-NEW-P',2))
        await self.wait_for(lambda:'BTC-NEW-P' in self.session.subscribed)
        result=await self.finish(task)
        self.assertEqual(result['status'],'raw_capture_complete',result)
        self.assertTrue(verify_run(result['root'])['changing_universe_coverage_verified'])
        self.assertEqual(result['retired_options'],0)
        self.assertTrue(any(r.body==b'{broken' for r in self.records(result,metadata=True)))

    async def test_verifier_rejects_native_creation_delay_over_120_seconds(self):
        self.setup_live();task=await self.start_live()
        created=self.observe()[0]//1_000_000
        self.session.rows.append(self.row('BTC-LATE-P',2,created=created))
        self.offset+=121_000_000_000;self.session.notify('BTC-LATE-P')
        await self.wait_for(lambda:'BTC-LATE-P' in self.session.subscribed)
        result=await self.finish(task)
        self.assertEqual(result['status'],'raw_capture_complete',result)
        with self.assertRaisesRegex(ValueError,'120 seconds'):verify_run(result['root'])

    async def test_receive_error_drains_already_admitted_lifecycle_before_reconnect(self):
        self.setup_live();task=await self.start_live()
        original=self.session.socket.receive;calls=0
        async def receive():
            nonlocal calls
            calls+=1
            if calls==2:raise OSError('connection reset during burst')
            return await original()
        self.session.socket.receive=receive
        self.session.rows.append(self.row('BTC-NEW-P',2));self.session.notify('BTC-NEW-P')
        await self.wait_for(lambda:'BTC-NEW-P' in self.session.subscribed)
        result=await self.finish(task)
        self.assertEqual(result['status'],'raw_capture_complete',result)
        self.assertGreaterEqual(result['connections'],2)
        self.assertTrue(verify_run(result['root'])['changing_universe_coverage_verified'])

    async def test_expiry_after_bad_refresh_uses_last_successful_metadata(self):
        self.setup_live();expiry=time.time_ns()//1_000_000+4500
        self.session.rows=[self.row('BTC-OLD-C',1,expiry=expiry),self.row('BTC-LATER-P',2)]
        task=await self.start_live();self.session.malformed=True;self.session.notify('BTC-LATER-P')
        await self.wait_for(lambda:self.session.calls>=2)
        await self.wait_for(lambda:any(v['method']=='public/unsubscribe' for v in self.session.socket.sent),seconds=8)
        result=await self.finish(task)
        self.assertEqual(result['status'],'raw_capture_complete',result)
        self.assertTrue(verify_run(result['root'])['changing_universe_coverage_verified'])

    async def test_unknown_listing_at_stop_cannot_be_accepted(self):
        self.setup_live();task=await self.start_live()
        self.session.delay=asyncio.Event()
        self.session.rows.append(self.row('BTC-NEW-P',2));self.session.notify('BTC-NEW-P')
        await self.wait_for(lambda:self.session.calls>=2)
        task.cancel();self.session.delay.set();result=await asyncio.wait_for(task,10)
        self.assertEqual(result['status'],'failed',result)
        self.assertTrue(verify_run(result['root'])['archive_verified'])
        # A success label cannot hide the unresolved native lifecycle evidence.
        report=Path(result['root'])/'result.json';value=strict_json(report.read_bytes())
        value['status']='raw_capture_complete';report.write_bytes(canonical(value))
        with self.assertRaisesRegex(ValueError,'unresolved|ACK|first push'):verify_run(result['root'])

    async def test_duration_expires_during_dns_retry(self):
        import aiohttp
        self.setup_live();self.config=replace(self.config,duration_seconds=1)
        task=await self.start_live()
        async def unavailable(*args,**kwargs):raise OSError('DNS unavailable')
        self.session.ws_connect=unavailable
        self.session.socket.incoming.put_nowait(aiohttp.WSMessage(aiohttp.WSMsgType.CLOSE,1000,''))
        result=await asyncio.wait_for(task,5)
        self.assertEqual(result['status'],'raw_capture_complete',result)
        self.assertEqual(result['stop_reason'],'duration')
        self.assertTrue(verify_run(result['root'])['changing_universe_coverage_verified'])

    async def test_malformed_message_followed_by_admitted_frame_keeps_exact_counters(self):
        self.setup_live();self.session.bad_push=True
        result=await asyncio.wait_for(run(self.config,session_factory=lambda:self.session,observer=self.observe,
            clock_read=self.clock_read,clock_wait=self.clock_wait),10)
        self.assertEqual(result['status'],'failed',result)
        checked=verify_run(result['root']);self.assertTrue(checked['archive_verified'])
        self.assertEqual(checked['ws_messages'],sum(r.kind=='ws_message' for r in self.records(result)))
        self.assertTrue(any(r.body==b'{broken' for r in self.records(result)))

    async def test_final_control_failure_still_drains_metadata_before_session_closes(self):
        self.setup_live();task=await self.start_live()
        gate=asyncio.Event();self.session.delay=gate
        self.session.rows.append(self.row('BTC-NEW-P',2));self.session.notify('BTC-NEW-P')
        await self.wait_for(lambda:self.session.calls>=2)
        original=RawOwner.save;failed=asyncio.Event()
        async def save(owner,body,channel,kind,**kwargs):
            if channel=='option_control' and strict_json(body).get('event')=='capture_end':
                failed.set();raise OSError('simulated main writer failure')
            return await original(owner,body,channel,kind,**kwargs)
        with patch.object(RawOwner,'save',new=save):
            task.cancel();await asyncio.wait_for(failed.wait(),5)
            self.assertFalse(task.done());self.assertFalse(self.session.closed)
            gate.set();result=await asyncio.wait_for(task,10)
        self.assertEqual(result['status'],'failed',result)
        self.assertTrue(self.session.closed)
        responses=[r for r in self.records(result,metadata=True) if r.kind=='rest_response']
        self.assertEqual(len(responses),2)
        self.assertIn(b'BTC-NEW-P',responses[-1].body)
        self.assertTrue(verify_run(result['root'])['archive_verified'])

    async def test_usdc_linear_option_is_visible_but_never_subscribed(self):
        self.setup_live()
        linear=self.row('BTC_USDC-LINEAR-C',2);linear['settlement_currency']='USDC';linear['instrument_type']='linear'
        self.session.rows.append(linear);task=await self.start_live();result=await self.finish(task)
        self.assertEqual(result['status'],'raw_capture_complete',result)
        self.assertNotIn(linear['instrument_name'],self.session.subscribed)
        revisions=[strict_json(r.body)['revision'] for r in self.records(result,metadata=True) if r.channel=='option_universe']
        excluded=revisions[0]['exclusions'];self.assertEqual(excluded[0]['status'],'not_enabled')
        self.assertIn('inverse',excluded[0]['reason'])
        self.assertTrue(verify_run(result['root'])['changing_universe_coverage_verified'])

    async def test_book_before_ack_cannot_initialize_top20(self):
        self.setup_live();self.session.before_ack=True;self.session.book_push=False
        task=asyncio.create_task(run(self.config,session_factory=lambda:self.session,observer=self.observe,
            clock_read=self.clock_read,clock_wait=self.clock_wait))
        self.addAsyncCleanup(self.cleanup_task,task)
        await self.wait_for(lambda:bool(self.session.subscribed))
        result=await self.finish(task)
        self.assertEqual(result['status'],'failed',result)
        self.assertTrue(verify_run(result['root'])['archive_verified'])
        path=Path(result['root'])/'result.json';report=strict_json(path.read_bytes())
        report['status']='raw_capture_complete';path.write_bytes(canonical(report))
        with self.assertRaisesRegex(ValueError,'initialized|first push'):verify_run(result['root'])

    async def test_index_ack_without_initial_value_is_incomplete(self):
        self.setup_live();self.session.index_push=False
        task=asyncio.create_task(run(self.config,session_factory=lambda:self.session,observer=self.observe,
            clock_read=self.clock_read,clock_wait=self.clock_wait))
        self.addAsyncCleanup(self.cleanup_task,task)
        await self.wait_for(lambda:bool(self.session.subscribed))
        result=await self.finish(task)
        self.assertEqual(result['status'],'failed',result)
        path=Path(result['root'])/'result.json';report=strict_json(path.read_bytes())
        report['status']='raw_capture_complete';path.write_bytes(canonical(report))
        with self.assertRaisesRegex(ValueError,'initialized'):verify_run(result['root'])

    async def test_selected_topics_have_no_per_instrument_trade_or_linear_index(self):
        self.setup_live();task=await self.start_live();result=await self.finish(task)
        self.assertEqual(result['status'],'raw_capture_complete',result)
        topics={c for request in self.session.socket.sent if request['method']=='public/subscribe'
            for c in request['params']['channels']}
        self.assertEqual(topics,set(GLOBALS)|set(instrument_channels('BTC-OLD-C')))
        self.assertNotIn('trades.BTC-OLD-C.100ms',topics)
        books=[strict_json(r.body)['params']['data'] for r in self.records(result)
            if r.kind=='ws_message' and b'book.BTC-OLD-C.none.20.100ms' in r.body and b'instrument_name' in r.body]
        self.assertTrue(books);self.assertTrue(all('type' not in b for b in books))
        self.assertTrue(verify_run(result['root'])['changing_universe_coverage_verified'])

    def test_top20_shape_requires_complete_pairs_and_allows_empty_replacements(self):
        def push(data):return dict(method='subscription',params=dict(
            channel='book.BTC-OLD-C.none.20.100ms',data=data))
        first=dict(instrument_name='BTC-OLD-C',change_id=1,timestamp=1,
            bids=[[100-i,1] for i in range(20)],asks=[[101+i,2] for i in range(20)])
        self.assertEqual(native_push(push(first)),('book','BTC-OLD-C'))
        second=dict(first,change_id=2,bids=[],asks=[[102,3]])
        self.assertEqual(native_push(push(second)),('book','BTC-OLD-C'))
        for malformed in (dict(first,bids=[['new',100,1]]),dict(first,asks=[[i,1] for i in range(21)]),
                dict(first,instrument_name='ETH-WRONG-P')):
            with self.assertRaises(ValueError):native_push(push(malformed))

    def test_top20_and_index_reject_non_numeric_or_nonfinite_fields(self):
        for bad in ('bad','NaN','Infinity','1e','00',True,None):
            with self.subTest(value=bad):
                book=dict(method='subscription',params=dict(channel='book.BTC-OLD-C.none.20.100ms',
                    data=dict(instrument_name='BTC-OLD-C',change_id=1,timestamp=1,bids=[[bad,1]],asks=[])))
                with self.assertRaisesRegex(ValueError,'numeric'):native_push(book)
                index=dict(method='subscription',params=dict(channel=INDEXES[0],
                    data=dict(index_name='btc_usd',timestamp=1,price=bad)))
                with self.assertRaisesRegex(ValueError,'numeric'):native_push(index)

    async def test_duration_during_rebootstrap_preserves_prior_coverage_and_explicit_gap(self):
        import aiohttp
        self.setup_live();self.config=replace(self.config,duration_seconds=3)
        task=await self.start_live();self.session.suppress_reconnect_options=True
        self.session.socket.incoming.put_nowait(aiohttp.WSMessage(aiohttp.WSMsgType.CLOSE,1000,'server restart'))
        result=await asyncio.wait_for(task,8)
        self.assertEqual(result['status'],'raw_capture_complete',result)
        self.assertEqual(result['connections'],2)
        self.assertTrue(result['connection_incomplete_at_stop'])
        self.assertTrue(verify_run(result['root'])['changing_universe_coverage_verified'])
        events=[strict_json(r.body) for r in self.records(result) if r.channel=='socket_event']
        self.assertEqual(events[0]['reason'],'server restart')

    async def test_cli_rejects_dropped_futures_and_legacy_raw_profiles(self):
        self.setup_live()
        future=replace(self.config,all_options=False,rediscover_options=False,all_futures=True)
        with self.assertRaisesRegex(ValueError,'futures.*not_enabled'):await run(future)
        legacy=replace(self.config,rediscover_options=False)
        with self.assertRaisesRegex(ValueError,'legacy.*not_enabled'):await run(legacy)
        self.assertFalse(self.root.exists())

    def test_receive_ack_rejects_foreign_identity_and_late_durability(self):
        from data_collector.contracts import RawRecord,Receipt,Provenance
        from data_collector.lane import DurableAck
        record=RawRecord(Receipt('capture','process','socket',0,1,1,'pending'),
            'deribit','testnet','multiplex','multiplex','ws_message',b'{}',Provenance('a'*64,'b'*64,'c'*64))
        ack=DurableAck(record.record_id,'d'*64,0,1,True,'epoch')
        self.assertIs(RawOwner.check_ack(record,ack),record)
        with self.assertRaisesRegex(ValueError,'foreign'):RawOwner.check_ack(record,replace(ack,record_id='wrong'))
        with self.assertRaisesRegex(ValueError,'late'):RawOwner.check_ack(record,replace(ack,timing_valid=False))

    async def test_reconciled_lifecycle_receipts_do_not_accumulate_past_instrument_bound(self):
        self.setup_live();self.config=replace(self.config,max_instruments=2)
        task=await self.start_live()
        for _ in range(4):
            before=self.session.calls
            self.session.notify('BTC-OLD-C')
            await self.wait_for(lambda:self.session.calls>before)
            # Wait for the completed REST response to publish its revision.
            await asyncio.sleep(.3)
        result=await self.finish(task)
        self.assertEqual(result['status'],'raw_capture_complete',result)
        self.assertGreaterEqual(result['universe_versions'],5)
        self.assertTrue(verify_run(result['root'])['changing_universe_coverage_verified'])

    async def test_each_added_option_uses_its_own_first_native_observation(self):
        self.setup_live();task=await self.start_live()
        gate=asyncio.Event();self.session.delay=gate
        self.session.rows.append(self.row('BTC-NEW-A-C',2));self.session.notify('BTC-NEW-A-C')
        await self.wait_for(lambda:self.session.calls>=2)
        self.session.rows.append(self.row('BTC-NEW-B-P',3));self.session.notify('BTC-NEW-B-P')
        await asyncio.sleep(.1);self.session.delay=None;gate.set()
        await self.wait_for(lambda:'BTC-NEW-B-P' in self.session.subscribed)
        result=await self.finish(task);self.assertEqual(result['status'],'raw_capture_complete',result)
        raw=self.records(result)
        observed={}
        for record in raw:
            if record.kind!='ws_message':continue
            value=strict_json(record.body)
            if value.get('method')=='subscription' and value['params']['channel']==LIFECYCLE:
                observed[value['params']['data']['instrument_name']]=record
        claims=[v for r in self.records(result,metadata=True) if r.channel=='option_universe'
            for v in strict_json(r.body)['revision']['instrument_observations'] if v['symbol'] in observed]
        self.assertEqual(len(claims),2)
        for claim in claims:
            actual=observed[claim['symbol']]
            self.assertEqual(claim['record_id'],actual.record_id)
            self.assertEqual(claim['monotonic_ns'],actual.receipt.monotonic_ns)
        self.assertTrue(verify_run(result['root'])['changing_universe_coverage_verified'])

    async def test_slow_cached_refresh_does_not_resurrect_expired_members(self):
        self.setup_live();expiry=time.time_ns()//1_000_000+4500
        self.session.rows=[self.row('BTC-OLD-C',1,expiry=expiry),self.row('BTC-LATER-P',2)]
        task=await self.start_live();gate=asyncio.Event();self.session.delay=gate
        self.session.notify('BTC-LATER-P')
        await self.wait_for(lambda:self.session.calls>=2)
        await self.wait_for(lambda:any(r['method']=='public/unsubscribe' for r in self.session.socket.sent))
        self.session.delay=None;gate.set();await asyncio.sleep(.3)
        result=await self.finish(task);self.assertEqual(result['status'],'raw_capture_complete',result)
        self.assertEqual(result['retired_options'],1)
        revisions=[strict_json(r.body)['revision'] for r in self.records(result,metadata=True) if r.channel=='option_universe']
        self.assertFalse(any('BTC-OLD-C' in r['added'] for r in revisions[1:]))
        self.assertTrue(verify_run(result['root'])['changing_universe_coverage_verified'])

    async def test_delayed_settlement_of_retired_option_is_not_a_new_listing(self):
        self.setup_live();expiry=time.time_ns()//1_000_000+4500
        later=self.row('BTC-LATER-P',2)
        self.session.rows=[self.row('BTC-OLD-C',1,expiry=expiry),later]
        task=await self.start_live()
        await self.wait_for(lambda:any(r['method']=='public/unsubscribe' for r in self.session.socket.sent))
        self.session.rows=[later];self.session.notify('BTC-OLD-C','settlement')
        await self.wait_for(lambda:self.session.calls>=2)
        result=await self.finish(task);self.assertEqual(result['status'],'raw_capture_complete',result)
        self.assertEqual(result['retired_options'],1)
        self.assertTrue(verify_run(result['root'])['changing_universe_coverage_verified'])


if __name__=='__main__':unittest.main()
