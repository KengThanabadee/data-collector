import asyncio
from dataclasses import replace
from email.utils import formatdate
import json
from pathlib import Path
import tempfile
import time
import unittest
from unittest.mock import patch
from aiohttp import web

from option_collector.archive import Archive, verify
from option_collector.collector import collect, context
from option_collector.common import body_bytes
from option_collector.config import Config
from option_collector.transport import Client, Gate, retry_after
from tests.helpers import instrument


class HttpTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.tmp=tempfile.TemporaryDirectory();self.addCleanup(self.tmp.cleanup)
        self.config=Config(data_root=Path(self.tmp.name),require_ntp=False,attempt_seconds=.08,attempts=1)
        self.handler=None
        app=web.Application();app.router.add_get('/{tail:.*}',self.route)
        self.runner=web.AppRunner(app);await self.runner.setup()
        site=web.TCPSite(self.runner,'127.0.0.1',0);await site.start()
        port=site._server.sockets[0].getsockname()[1]
        self.patch=patch.object(Config,'base_url',property(lambda _:f'http://127.0.0.1:{port}/'))
        self.patch.start();self.addCleanup(self.patch.stop)

    async def asyncTearDown(self):await self.runner.cleanup()

    async def route(self,request):return await self.handler(request)

    def archive(self):return Archive(self.config.data_root,context(self.config,'BTC','delivery'))

    async def fetch(self,a):
        async with Client(self.config) as client:
            return await client.fetch(a,'index','public/get_index_price',{'index_name':'btc_usd'},time.monotonic()+2)

    async def test_http200_api_throttle_and_non_json_429_persist_cooldown(self):
        for api in (True,False):
            with self.subTest(api=api):
                async def handler(req):
                    return web.Response(body=b'{"error":{"code":10028}}' if api else b'not json',status=200 if api else 429,headers={'Retry-After':'10'})
                self.handler=handler;a=self.archive();rec=await self.fetch(a)
                self.assertFalse(rec['ok']);self.assertTrue((self.config.data_root/'state/cooldown.json').exists())
                self.assertFalse(rec['raw_body'] is None)
                # Reset between independent synthetic throttle scenarios.
                (self.config.data_root/'state/cooldown.json').unlink()

    async def test_drip_feed_total_timeout_retains_prefix(self):
        async def handler(req):
            resp=web.StreamResponse();await resp.prepare(req)
            try:
                for _ in range(10):
                    await resp.write(b'x');await asyncio.sleep(.025)
            except (ConnectionResetError,RuntimeError):pass
            return resp
        self.handler=handler;a=self.archive();start=time.monotonic();rec=await self.fetch(a)
        self.assertLess(time.monotonic()-start,.3)
        self.assertFalse(rec['body_complete']);self.assertGreater(rec['body_byte_count'],0)
        self.assertEqual(rec['transport_error'],'TimeoutError')

    async def test_body_cap_including_single_final_chunk(self):
        self.config=replace(self.config,body_cap=1024)
        async def handler(req):return web.Response(body=b'x'*1025)
        self.handler=handler;rec=await self.fetch(self.archive())
        self.assertFalse(rec['ok']);self.assertFalse(rec['body_complete']);self.assertEqual(rec['body_byte_count'],1024)

    async def test_inflight_cancellation_records_attempt(self):
        async def handler(req):await asyncio.sleep(.3);return web.json_response({'result':{'index_price':1}})
        self.handler=handler;a=self.archive()
        async with Client(self.config) as client:
            task=asyncio.create_task(client.fetch(a,'index','public/get_index_price',{},time.monotonic()+2))
            await asyncio.sleep(.025);task.cancel()
            with self.assertRaises(asyncio.CancelledError):await task
        self.assertEqual(len(a.records),1);self.assertEqual(a.records[0]['transport_error'],'cancelled')

    async def test_bounded_retry_reuses_logical_request_and_retains_errors(self):
        self.config=replace(self.config,attempts=3,attempt_seconds=1)
        calls=0
        async def handler(req):
            nonlocal calls;calls+=1
            return web.json_response({'error':{'code':-1}} if calls<2 else {'result':{'index_price':1}},status=503 if calls<2 else 200)
        self.handler=handler;a=self.archive();rec=await self.fetch(a)
        self.assertTrue(rec['ok']);self.assertEqual([r['attempt'] for r in a.records],[1,2])

    async def test_full_snapshot_frozen_universe_quality_and_missing_target(self):
        async def handler(req):
            method=req.path.rsplit('/',1)[-1];currency=req.query.get('currency','BTC')
            if method=='get_instruments':result=[instrument(currency)] if req.query['kind']=='option' else []
            elif method=='get_book_summary_by_currency':result=[]
            elif method=='get_index_price':result={'index_price':100}
            else:result={'instrument_name':req.query['instrument_name'],'timestamp':1000,'bids':[],'asks':[],'bid_iv':0,'unknown':'preserved'}
            return web.json_response({'result':result,'testnet':False})
        self.handler=handler
        self.config=replace(self.config,attempt_seconds=1)
        async with Client(self.config) as client:
            results=await collect(self.config,client,time.monotonic()+8)
        for m in results:
            self.assertEqual(m['status'],'complete');self.assertEqual(len(m['expected_jobs']),6)
            _,records=verify(self.config.data_root/m['destination'])
            book=next(r for r in records if r['method']=='public/get_order_book')
            self.assertIn('empty_bids',book['quality_flags']);self.assertIn('bid_iv_zero',book['quality_flags'])
            self.assertIn('unknown',json.loads(body_bytes(book))['result'])


class GateTests(unittest.IsolatedAsyncioTestCase):
    async def test_shared_pacing_discovery_and_persisted_cooldown(self):
        with tempfile.TemporaryDirectory() as d:
            now=[0.0]
            async def sleep(s):now[0]+=s
            config=Config(data_root=Path(d))
            gate=Gate(config,clock=lambda:now[0],wall=lambda:1000+now[0],sleep=sleep)
            await gate.acquire(True,10);await gate.acquire(False,10);self.assertAlmostEqual(now[0],.1)
            await gate.acquire(True,10);self.assertAlmostEqual(now[0],1)
            gate.throttle(4)
            newer=Gate(config,clock=lambda:now[0],wall=lambda:1000+now[0],sleep=sleep)
            with self.assertRaises(TimeoutError):await newer.acquire(False,4)
            await newer.acquire(False,10);self.assertAlmostEqual(now[0],5)

    def test_retry_after_httpdate(self):
        self.assertEqual(retry_after(formatdate(1010,usegmt=True),1000),10)
        self.assertEqual(retry_after('12',1000),12)
        self.assertEqual(retry_after('invalid',1000),0)
