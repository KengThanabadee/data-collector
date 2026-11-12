"""Raw future/index plans keep discovery scope and exact native bytes."""
import unittest
import asyncio
import time
from dataclasses import replace
from pathlib import Path
from unittest.mock import patch

from data_collector.app import verify_run
from data_collector.continuous import capture, verify, _future_plan
from data_collector.contracts import strict_json
from data_collector.segments import iter_segment_records
from tests import test_dc_app as fixtures
from tests import test_dc_continuous as continuous_fixtures
from tests.helpers import Socket
from tests.helpers import Response, response_body


class FuturesTests(unittest.IsolatedAsyncioTestCase):
    setUp=fixtures.AppTests.setUp
    observe=fixtures.AppTests.observe
    clock_read=fixtures.AppTests.clock_read
    clock_wait=continuous_fixtures.ContinuousTests.clock_wait

    def configure(self, *, missing_index=False, wrong_index=False):
        rows=[dict(instrument_name='BTC-PERPETUAL',kind='future',base_currency='BTC',
                   price_index='btc_usd',settlement_period='perpetual'),
              dict(instrument_name='ETH_USDC-25DEC26',kind='future',base_currency='ETH',
                   price_index='eth_usdc',settlement_period='month'),
              dict(instrument_name='BTC-TEST-C',kind='option',base_currency='BTC')]
        for i,row in enumerate(rows):row.update(instrument_id=i+1,creation_timestamp=1000,is_active=True)
        self.session.responses=[Response(response_body(rows))]
        self.config=replace(self.config,capture_mode='raw',all_futures=True,symbols=(),duration_seconds=0,
            max_frames=7 if missing_index else 8)
        async def send(body):
            await Socket.send_str(self.session.socket,body)
            request=strict_json(body.encode())
            if request['method']!='public/subscribe':return
            for channel in request['params']['channels']:
                if channel.startswith('book.'):
                    data=dict(type='snapshot',instrument_name=channel[5:-6])
                elif channel.startswith('ticker.'):
                    data=dict(instrument_name=channel[7:-6])
                elif channel.startswith('deribit_price_index.'):
                    if missing_index and channel.endswith('eth_usdc'):continue
                    data=dict(index_name='wrong' if wrong_index else channel[len('deribit_price_index.'):],
                              price=100,timestamp=2)
                else:continue
                self.session.socket.put(dict(jsonrpc='2.0',method='subscription',params=dict(channel=channel,data=data)))
        self.session.socket.send_str=send

    async def run_capture(self):
        return await capture(self.config,session_factory=lambda:self.session,observer=self.observe,
            clock_read=self.clock_read,clock_wait=self.clock_wait)

    async def test_all_futures_and_metadata_indices_are_initialized_and_verified(self):
        self.configure()
        result=await self.run_capture()
        self.assertEqual(result['status'],'raw_capture_complete',result)
        self.assertEqual(result['symbols'],['BTC-PERPETUAL','ETH_USDC-25DEC26'])
        self.assertEqual(result['index_names'],['btc_usd','eth_usdc'])
        self.assertEqual(result['subscribed_channels'],8)
        self.assertTrue(verify_run(result['root'])['initial_feed_coverage_verified'])
        records=[r for p in result['segments'] for r in iter_segment_records(Path(result['root'])/p)
            if r.kind=='ws_message']
        self.assertTrue(all(r.native_product=='multiplex' for r in records))
        bad=dict(result,channels=result['channels'][:-1])
        with self.assertRaisesRegex(ValueError,'all-future/index plan'):
            verify(Path(result['root']),bad)

    async def test_index_ack_without_initial_value_cannot_complete(self):
        self.configure(missing_index=True)
        result=await self.run_capture()
        self.assertEqual(result['status'],'failed',result)
        self.assertTrue(verify_run(result['root'])['archive_verified'])
        self.assertFalse((Path(result['root'])/'ready.json').exists())

    async def test_wrong_native_index_identity_fails_after_archiving(self):
        self.configure(wrong_index=True)
        result=await self.run_capture()
        self.assertEqual(result['status'],'failed',result)
        self.assertIn('index identity differs',result['error'])
        self.assertTrue(verify_run(result['root'])['archive_verified'])
        self.assertEqual(result['frames'],8)

    async def test_connect_failures_back_off_then_reacquire_the_complete_plan(self):
        self.configure()
        connect=self.session.ws_connect;calls=[]
        async def failing(url,**kwargs):
            calls.append(time.monotonic())
            if len(calls)<3:raise OSError('network unavailable')
            return await connect(url,**kwargs)
        self.session.ws_connect=failing
        with patch('data_collector.continuous.random.uniform',return_value=1):
            result=await asyncio.wait_for(self.run_capture(),15)
        self.assertEqual(result['status'],'raw_capture_complete',result)
        self.assertEqual(result['connections'],3)
        self.assertGreaterEqual(calls[1]-calls[0],1)
        self.assertGreaterEqual(calls[2]-calls[1],2)
        self.assertTrue(verify_run(result['root'])['initial_feed_coverage_verified'])

    def test_future_index_dependency_must_be_explicit(self):
        for name in (None,'','btc/usd','btc.usd'):
            with self.subTest(name=name),self.assertRaises(ValueError):
                _future_plan(dict(selected=[dict(instrument=dict(native_product='future',native_symbol='BTC-PERPETUAL'),
                    attributes=dict(native=dict(price_index=name)))]))
