"""Public discovery keeps all native evidence and explicit disabled coverage."""
import asyncio
import unittest
from dataclasses import replace
from pathlib import Path

import aiohttp

from data_collector.app import run, verify_run
from data_collector.contracts import canonical, strict_json
from data_collector.segments import iter_segment_records
from tests.helpers import AppHarness, HTTP, Response


class BinanceDiscoveryTests(AppHarness, unittest.IsolatedAsyncioTestCase):

    async def clock_wait(self,deadline):await asyncio.Event().wait()

    def configure(self):
        self.config=replace(self.config,venue='binance',environment='production',capture_mode='raw',
            symbols=(),duration_seconds=0,max_frames=0)
        self.bodies=[canonical(dict(symbols=[
            dict(symbol='BTCUSDT',baseAsset='BTC',quoteAsset='USDT',status='TRADING',isSpotTradingAllowed=True),
            dict(symbol='ETHOLD',baseAsset='ETH',quoteAsset='OLD',status='BREAK',isSpotTradingAllowed=True),
            dict(symbol='BTCDOMUSDT',baseAsset='BTCDOM',status='TRADING')
        ])),canonical(dict(symbols=[dict(symbol='BTCUSDT',baseAsset='BTC',quoteAsset='USDT',
            contractType='PERPETUAL',status='TRADING',onboardDate=1000)])),
            canonical(dict(symbols=[dict(symbol='ETHUSD_261225',baseAsset='ETH',quoteAsset='USD',
                contractType='CURRENT_QUARTER',contractStatus='TRADING',onboardDate=2000,contractSize=10)]))]
        self.session=HTTP([Response(body) for body in self.bodies])
        # HTTP fixture does not otherwise own an aiohttp session context.
        class Session(HTTP):
            async def __aenter__(self):return self
            async def __aexit__(self,*args):pass
        self.session=Session(self.session.responses)

    async def discover(self):
        return await run(self.config,discover_only=True,session_factory=lambda:self.session,
            observer=self.observe,clock_read=self.clock_read,clock_wait=self.clock_wait)

    def records(self,result):
        return [r for p in result['segments'] for r in iter_segment_records(Path(result['root'])/p)]

    async def test_complete_discovery_projects_all_three_families_and_verifies(self):
        self.configure();result=await self.discover()
        self.assertEqual(result['status'],'discovery_complete',result)
        root=Path(result['root']);universe=strict_json((root/'universe.json').read_bytes())
        self.assertEqual(len(universe['selected']),4)
        self.assertEqual(universe['families']['spot']['feed_enabled'],1)
        selected={(r['family'],r['instrument']['native_symbol']):r for r in universe['selected']}
        self.assertIsNone(selected['spot','BTCUSDT']['instrument']['listing_incarnation'])
        self.assertEqual(selected['um','BTCUSDT']['instrument']['listing_incarnation'],'onboard:1000')
        self.assertEqual(selected['um','BTCUSDT']['instrument']['native_product'],'perpetual')
        self.assertEqual(selected['cm','ETHUSD_261225']['instrument']['native_product'],'future')
        self.assertFalse(selected['spot','ETHOLD']['feed_enabled'])
        self.assertEqual(universe['excluded'][0]['native_symbol'],'BTCDOMUSDT')
        self.assertTrue(verify_run(root)['universe_verified'])
        raw=[r for r in self.records(result) if r.kind=='rest_response']
        self.assertEqual([r.body for r in raw],self.bodies)
        self.assertEqual(self.session.calls[0][1]['params'],dict(showPermissionSets='false'))
        changed=dict(universe,selected=universe['selected'][:-1]);(root/'universe.json').write_bytes(canonical(changed))
        with self.assertRaisesRegex(ValueError,'projection differs'):verify_run(root)

    async def test_throttle_is_retained_and_stops_before_another_request(self):
        self.configure()
        body=b'{"code":-1003,"msg":"Too many requests"}'
        self.session.responses=[Response(body,status=429,headers=((b'Retry-After',b'60'),))]
        result=await self.discover()
        self.assertEqual(result['status'],'failed',result)
        self.assertEqual(len(self.session.calls),1)
        self.assertTrue(verify_run(result['root'])['archive_verified'])
        response=next(r for r in self.records(result) if r.kind=='rest_response')
        self.assertEqual(response.body,body)
        self.assertEqual(strict_json(response.native_fields_json)['headers'],[['Retry-After','60']])

    async def test_transport_payload_failure_archives_the_received_prefix(self):
        self.configure()
        self.session.responses=[Response(self.bodies[0],error=aiohttp.ClientPayloadError('truncated response'))]
        result=await self.discover()
        self.assertEqual(result['status'],'failed',result)
        response=next(r for r in self.records(result) if r.kind=='rest_response')
        self.assertEqual(response.body,self.bodies[0][:19])
        fields=strict_json(response.native_fields_json)
        self.assertFalse(fields['complete'])
        self.assertIn('ClientPayloadError',fields['error'])
        self.assertTrue(verify_run(result['root'])['archive_verified'])

    async def test_malformed_native_json_is_saved_before_projection_fails(self):
        self.configure();self.session.responses[0]=Response(b'{malformed')
        result=await self.discover()
        self.assertEqual(result['status'],'failed',result)
        self.assertTrue(verify_run(result['root'])['archive_verified'])
        self.assertEqual(next(r.body for r in self.records(result) if r.kind=='rest_response'),b'{malformed')
        self.assertEqual(len(self.session.calls),1)

    async def test_http200_api_error_stops_before_another_request(self):
        self.configure();body=b'{"code":-1003,"msg":"Too many requests"}'
        self.session.responses[0]=Response(body)
        result=await self.discover()
        self.assertEqual(result['status'],'failed',result)
        self.assertEqual(len(self.session.calls),1)
        self.assertTrue(verify_run(result['root'])['archive_verified'])
