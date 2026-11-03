"""Recorded public replies exercise the selected raw acquisition path."""
import asyncio
from dataclasses import replace
import json
from pathlib import Path
import unittest
from unittest.mock import AsyncMock,patch

import aiohttp

from data_collector.app import run,verify_run
from data_collector.binance_capture import plans,validate_event,LIQUIDATIONS
from data_collector.contracts import canonical,strict_json
from data_collector.rawrun import RawOwner
from data_collector.segments import iter_segment_records
from tests import test_dc_app as fixtures
from tests.test_dc_deribit import Socket
from tests.test_dc_discovery import Response

FIXTURE=json.loads((Path(__file__).parent/'fixtures/dc_binance_selected.json').read_text())


class Session:
    _retry_connection=False
    def __init__(self):
        self.calls=[];self.sockets=[];self.reject=False;self.no_book=False;self.before_ack=False
        self.closed=False;self.status=200;self.body=canonical(FIXTURE['metadata'])
    async def __aenter__(self):return self
    async def __aexit__(self,*args):self.closed=True
    def get(self,url,**kwargs):
        self.calls.append(url);return Response(self.body,status=self.status)
    async def ws_connect(self,url,**kwargs):
        socket=Socket();self.sockets.append((url,socket))
        async def send(body):
            value=strict_json(body.encode());socket.sent.append(value)
            if self.reject:
                socket.put(dict(id=1,code=-1003,msg='Too many requests'));return
            topics=value['params']
            async def push():
                for topic in topics:
                    if self.no_book and '@depth' in topic:continue
                    socket.put(FIXTURE['samples'][topic].encode())
            if self.before_ack:await push()
            socket.put(FIXTURE['samples']['ack'].encode())
            if not self.before_ack:await push()
        socket.send_str=send
        return socket


class BinanceCaptureTests(unittest.IsolatedAsyncioTestCase):
    setUp=fixtures.AppTests.setUp
    observe=fixtures.AppTests.observe
    clock_read=fixtures.AppTests.clock_read
    async def clock_wait(self,deadline):await asyncio.Event().wait()

    def setup_capture(self,**values):
        self.config=replace(self.config,venue='binance',environment='production',capture_mode='raw',symbols=(),
            duration_seconds=1,max_frames=0,bootstrap_timeout_seconds=2,timeout_seconds=1,**values)
        self.session=Session()
        p=patch.object(RawOwner,'aligned',new=AsyncMock());p.start();self.addCleanup(p.stop)
        for name in ('acquire','connection','observe'):
            p=patch('data_collector.binance_budget.Budget.'+name,new=AsyncMock(return_value=True));p.start();self.addCleanup(p.stop)

    async def capture(self):
        return await asyncio.wait_for(run(self.config,session_factory=lambda:self.session,observer=self.observe,
            clock_read=self.clock_read,clock_wait=self.clock_wait),10)

    def records(self,result):
        return [v for p in result['segments'] for v in iter_segment_records(Path(result['root'])/p)]

    async def test_recorded_replies_exact_bytes_routes_and_append_only_metadata(self):
        self.setup_capture()
        with patch('data_collector.binance_capture.METADATA_INTERVAL_SECONDS',.2):result=await self.capture()
        self.assertEqual(result['status'],'raw_capture_complete',result)
        self.assertGreater(result['metadata_versions'],1)
        self.assertEqual({url for url,_ in self.session.sockets},{p['url'] for p in plans(self.config.binance_symbols)})
        self.assertTrue(all(url=='https://fapi.binance.com/fapi/v1/exchangeInfo' for url in self.session.calls))
        self.assertEqual(result['topics_seen'],sorted(t for p in plans(self.config.binance_symbols) for t in p['topics']))
        bodies={v.body for v in self.records(result) if v.kind=='ws_message'}
        self.assertEqual(bodies,{v.encode() for v in FIXTURE['samples'].values()})
        verified=verify_run(result['root'])
        self.assertTrue(verified['feed_plan_initialization_verified'])
        self.assertTrue(verified['liquidation_pushes_observed'])
        report=Path(result['root'])/'result.json';value=strict_json(report.read_bytes())
        value['plans'][0]['topics'].pop();report.write_bytes(canonical(value))
        with self.assertRaisesRegex(ValueError,'plan differs'):verify_run(result['root'])

    async def test_missing_configured_symbol_stops_before_subscription(self):
        self.setup_capture();self.session.body=canonical(dict(symbols=FIXTURE['metadata']['symbols'][:1]))
        result=await self.capture();self.assertEqual(result['status'],'failed',result)
        self.assertFalse(self.session.sockets)
        self.assertTrue(verify_run(result['root'])['archive_verified'])

    async def test_rejection_is_archived_and_complete_claim_cannot_pass(self):
        self.setup_capture();self.session.reject=True
        result=await self.capture();self.assertEqual(result['status'],'failed',result)
        self.assertGreater(result['rejected_subscriptions'],0)
        self.assertTrue(any(b'Too many requests' in v.body for v in self.records(result)))
        verify_run(result['root'])
        report=Path(result['root'])/'result.json';value=strict_json(report.read_bytes());value['status']='raw_capture_complete'
        report.write_bytes(canonical(value))
        with self.assertRaises(ValueError):verify_run(result['root'])

    async def test_pre_ack_push_does_not_initialize_books(self):
        self.setup_capture();self.session.before_ack=True
        result=await self.capture();self.assertEqual(result['status'],'failed',result)
        self.assertFalse(verify_run(result['root'])['feed_plan_initialization_verified'])

    async def test_http_rate_rejection_is_retained_without_retry(self):
        self.setup_capture();self.session.status=429;self.session.body=b'{"code":-1003,"msg":"rate limit"}'
        result=await self.capture()
        self.assertEqual(result['status'],'failed',result)
        self.assertEqual(len(self.session.calls),1)
        self.assertFalse(self.session.sockets)
        self.assertTrue(verify_run(result['root'])['archive_verified'])

    async def test_failed_final_control_still_describes_durable_prefix(self):
        self.setup_capture();save=RawOwner.save
        async def failure(owner,body,channel,kind,**kwargs):
            if channel=='capture_control' and strict_json(body).get('event')=='capture_end':raise OSError('disk full')
            return await save(owner,body,channel,kind,**kwargs)
        with patch.object(RawOwner,'save',new=failure):result=await self.capture()
        self.assertEqual(result['status'],'failed',result)
        self.assertTrue(result['segments'])
        self.assertTrue(verify_run(result['root'])['archive_verified'])

    async def test_quiet_trades_and_liquidations_need_ack_not_first_push(self):
        self.setup_capture()
        original=self.session.ws_connect
        async def connect(url,**kwargs):
            socket=await original(url,**kwargs)
            if '/market/' in url:
                async def send(body):socket.put(FIXTURE['samples']['ack'].encode())
                socket.send_str=send
            return socket
        self.session.ws_connect=connect
        result=await self.capture();self.assertEqual(result['status'],'raw_capture_complete',result)
        self.assertFalse(verify_run(result['root'])['liquidation_pushes_observed'])

    async def test_socket_loss_reconnects_and_reacquires_first_books(self):
        self.setup_capture();self.config=replace(self.config,duration_seconds=2)
        task=asyncio.create_task(self.capture())
        try:
            async with asyncio.timeout(5):
                while len(self.session.sockets)<2:await asyncio.sleep(.02)
            socket=self.session.sockets[0][1]
            socket.incoming.put_nowait(aiohttp.WSMessage(aiohttp.WSMsgType.CLOSE,1000,'restart'))
            result=await task
        finally:
            if not task.done():task.cancel();await asyncio.gather(task,return_exceptions=True)
        self.assertEqual(result['status'],'raw_capture_complete',result)
        self.assertGreater(result['connections'],2)
        self.assertEqual(result['socket_events'],1)
        self.assertTrue(verify_run(result['root'])['archive_verified'])

    def test_all_symbol_liquidations_preserve_nonconfigured_and_cm_rows(self):
        plan=plans(('BTCUSDT','ETHUSDT'))[1]
        value=strict_json(FIXTURE['samples'][LIQUIDATIONS].encode())
        value['data']['o']['s']='UNCONFIGUREDUSD_PERP';value['data']['st']=2
        self.assertEqual(validate_event(value,plan),LIQUIDATIONS)
        value['stream']='!forceOrder@arrWrong'
        with self.assertRaises(ValueError):validate_event(value,plan)

    def test_wrong_um_identity_and_oversized_depth_fail(self):
        plan=plans(('BTCUSDT','ETHUSDT'))[0]
        value=strict_json(FIXTURE['samples']['btcusdt@depth20@100ms'].encode())
        value['data']['st']=2
        with self.assertRaises(ValueError):validate_event(value,plan)
        value['data']['st']=1;value['data']['b']*=2
        with self.assertRaises(ValueError):validate_event(value,plan)
