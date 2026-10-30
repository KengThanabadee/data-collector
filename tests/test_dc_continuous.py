"""Exercise continuous raw capture against deterministic transport fixtures."""
import asyncio
import unittest
from dataclasses import replace
from pathlib import Path
from unittest.mock import patch

from data_collector.app import verify_run
from data_collector.continuous import capture, _routing_json
from data_collector.clocksource import ChronyClockSampler
from data_collector.writer import SessionWriter
from tests import test_dc_app as fixtures


class ContinuousTests(unittest.IsolatedAsyncioTestCase):
    setUp = fixtures.AppTests.setUp
    observe = fixtures.AppTests.observe
    clock_read = fixtures.AppTests.clock_read
    async def clock_wait(self, deadline):
        if self.reads < 5:
            self.offset += max(0, deadline-self.observe()[1])
            await asyncio.sleep(0)
        else:
            await asyncio.Event().wait()
    async def test_raw_capture_limits_and_independent_integrity(self):
        self.config = replace(self.config, capture_mode='raw', duration_seconds=0, max_frames=4,raw_segment_bytes=4*1024*1024)
        with patch('data_collector.continuous.ChronyClockSampler', wraps=ChronyClockSampler) as sampler, \
                patch('data_collector.continuous.SessionWriter',wraps=SessionWriter) as writer:
            result = await asyncio.wait_for(capture(self.config, session_factory=lambda:self.session,
                observer=self.observe, clock_read=self.clock_read, clock_wait=self.clock_wait), 15)
        self.assertIsNone(sampler.call_args.kwargs['max_samples'])
        self.assertEqual(writer.call_args.kwargs['limits'].target_bytes,4*1024*1024)
        self.assertIsNone(sampler.call_args.args[0]._clock.max_assessments)
        self.assertEqual(result['status'], 'raw_capture_complete', result)
        self.assertEqual(result['frames'], 4)
        verified = verify_run(result['root'])
        self.assertTrue(verified['archive_verified'])
        self.assertNotIn('book_replay_equal', verified)
        self.assertEqual(verified['ws_messages'], 4)
        self.assertTrue(self.session.socket.closed)

    async def test_all_discovered_options_subscribe_every_feed_and_verify(self):
        from data_collector.contracts import strict_json
        original=self.session.socket.send_str
        async def send(body):
            await original(body)
            request=strict_json(body.encode())
            if request['method']=='public/subscribe':
                for channel in request['params']['channels']:
                    if channel.startswith('ticker.'):
                        self.session.socket.put(dict(jsonrpc='2.0',method='subscription',params=dict(
                            channel=channel,data=dict(instrument_name=channel[7:-6]))))
        self.session.socket.send_str=send
        self.config=replace(self.config,capture_mode='raw',all_options=True,raw_processes=1,symbols=(),max_frames=5)
        result=await capture(self.config,session_factory=lambda:self.session,observer=self.observe,
            clock_read=self.clock_read,clock_wait=self.clock_wait)
        self.assertEqual(result['status'],'raw_capture_complete',result)
        self.assertEqual(result['symbols'],['BTC-TEST-C'])
        self.assertEqual(set(result['channels']),{'book.BTC-TEST-C.100ms','ticker.BTC-TEST-C.100ms','trades.BTC-TEST-C.100ms'})
        self.assertEqual(result['subscribed_channels'],3)
        self.assertTrue(verify_run(result['root'])['archive_verified'])

    async def test_all_feed_batches_are_paced_and_all_acks_required(self):
        from tests.test_dc_deribit import Socket
        from data_collector.contracts import strict_json
        async def send(body):
            await Socket.send_str(self.session.socket,body)
            request=strict_json(body.encode())
            if request['method']=='public/subscribe':
                for channel in request['params']['channels']:
                    symbol=channel.split('.')[1]
                    if channel.startswith('book.'):
                        self.session.socket.put(dict(jsonrpc='2.0',method='subscription',params=dict(
                            channel=channel,data=dict(type='snapshot',instrument_name=symbol))))
                    elif channel.startswith('ticker.'):
                        self.session.socket.put(dict(jsonrpc='2.0',method='subscription',params=dict(
                            channel=channel,data=dict(instrument_name=symbol))))
        self.session.socket.send_str=send
        self.config=replace(self.config,capture_mode='raw',all_options=True,raw_processes=1,symbols=(),max_frames=6,subscription_batch_size=1)
        result=await capture(self.config,session_factory=lambda:self.session,observer=self.observe,
            clock_read=self.clock_read,clock_wait=self.clock_wait)
        self.assertEqual(result['status'],'raw_capture_complete',result)
        requests=[r for r in self.session.socket.sent if r['method']=='public/subscribe']
        self.assertEqual(len(requests),3)
        self.assertEqual(result['subscribed_channels'],3)
        self.assertTrue(verify_run(result['root'])['archive_verified'])

    async def test_rejection_and_throttle_payload_is_retained(self):
        from data_collector.contracts import strict_json
        async def send(body):
            request=strict_json(body.encode())
            if request['method']=='public/subscribe':
                self.session.socket.put(dict(jsonrpc='2.0',id=request['id'],error=dict(code=10028,message='too_many_requests')))
        self.session.socket.send_str=send
        self.config=replace(self.config,capture_mode='raw')
        result=await capture(self.config,session_factory=lambda:self.session,observer=self.observe,
            clock_read=self.clock_read,clock_wait=self.clock_wait)
        self.assertEqual(result['status'],'failed')
        self.assertEqual(result['rejected_messages'],1)
        self.assertEqual(result['throttled_messages'],1)
        self.assertTrue(verify_run(result['root'])['archive_verified'])

    async def test_raw_cancel_while_idle_drains_and_verifies(self):
        self.config = replace(self.config, capture_mode='raw', duration_seconds=0, max_frames=0)
        task = asyncio.create_task(capture(self.config, session_factory=lambda:self.session,
            observer=self.observe, clock_read=self.clock_read, clock_wait=self.clock_wait))
        while not task.done() and not self.session.connected:
            await asyncio.sleep(.01)
        await asyncio.sleep(.1)
        task.cancel()
        result = await asyncio.wait_for(task, 5)
        self.assertEqual(result['status'], 'raw_capture_complete', result)
        self.assertTrue(verify_run(result['root'])['archive_verified'])

    async def test_reused_discovery_avoids_http_and_stops_when_supervisor_exits(self):
        from data_collector.app import run
        from data_collector.contracts import strict_json
        metadata=await run(self.config,discover_only=True,session_factory=lambda:self.session,
            observer=self.observe,clock_read=self.clock_read,clock_wait=self.clock_wait)
        self.session=fixtures.Session()
        original=self.session.socket.send_str
        async def send(body):
            await original(body)
            request=strict_json(body.encode())
            if request['method']=='public/subscribe':
                for channel in request['params']['channels']:
                    if channel.startswith('ticker.'):
                        self.session.socket.put(dict(jsonrpc='2.0',method='subscription',params=dict(
                            channel=channel,data=dict(instrument_name=channel[7:-6]))))
        self.session.socket.send_str=send
        self.config=replace(self.config,capture_mode='raw',data_root=self.root/'child',
            source_discovery_root=Path(metadata['root']),raw_feeds=('book','ticker','trades'),
            raw_processes=1,max_frames=0,duration_seconds=0,supervisor_pid=200)
        ready=self.config.data_root/'continuous'/'ready.json'
        with patch('data_collector.continuous.os.getppid',side_effect=lambda:201 if ready.exists() else 200):
            result=await asyncio.wait_for(capture(self.config,session_factory=lambda:self.session,
                observer=self.observe,clock_read=self.clock_read,clock_wait=self.clock_wait),15)
        self.assertEqual(result['status'],'raw_capture_complete',result)
        self.assertEqual(result['stop_reason'],'supervisor_exit')
        self.assertEqual(len(self.session.responses),1)  # No HTTP acquisition in worker.
        self.assertTrue(self.session.socket.closed)
        self.assertTrue(verify_run(result['root'])['initial_feed_coverage_verified'])

    def test_raw_routing_preserves_numeric_lexemes_and_rejects_ambiguous_envelopes(self):
        body=b'{"method":"subscription","params":{"data":{"price":1.234567890123456789,"vega":1e-300}}}'
        value=_routing_json(body)
        self.assertEqual(value['params']['data']['price'],'1.234567890123456789')
        self.assertEqual(value['params']['data']['vega'],'1e-300')
        for malformed in (b'{"method":"a","method":"b"}',b'{"x":NaN}',b'[]',b'\xff'):
            with self.subTest(body=malformed),self.assertRaises(ValueError):_routing_json(malformed)

    def test_unbounded_defaults_and_large_configured_limits(self):
        config = replace(self.config, capture_mode='raw', duration_seconds=0, max_frames=0, max_bytes=0)
        self.assertEqual((config.duration_seconds, config.max_frames, config.max_bytes),(0,0,0))
        replace(config,duration_seconds=86400,max_frames=1000000,max_bytes=2**40)

    async def test_ack_only_limit_is_failed(self):
        self.config = replace(self.config,capture_mode='raw',max_frames=1)
        result = await capture(self.config,session_factory=lambda:self.session,observer=self.observe,
            clock_read=self.clock_read,clock_wait=self.clock_wait)
        self.assertEqual(result['status'],'failed',result)
        self.assertIn('native snapshots',result['error'])
        self.assertTrue(verify_run(result['root'])['archive_verified'])

    async def test_transport_disconnect_reconnects_with_gaps(self):
        from aiohttp import WSMessage, WSMsgType
        from data_collector.contracts import strict_json
        from data_collector.segments import iter_segment_records
        second = fixtures.Session()
        first_socket = self.session.socket
        first_socket.incoming = asyncio.Queue()
        original_send = first_socket.send_str
        async def send(body):
            await original_send(body)
            first_socket.incoming = asyncio.Queue()
            first_socket.incoming.put_nowait(WSMessage(WSMsgType.CLOSE,1000,''))
        first_socket.send_str = send
        calls = 0
        async def connect(url, **kwargs):
            nonlocal calls
            calls += 1
            if calls==1:
                return first_socket
            return await second.ws_connect(url,**kwargs)
        self.session.ws_connect = connect
        self.config = replace(self.config,capture_mode='raw',max_frames=4)
        result = await asyncio.wait_for(capture(self.config,session_factory=lambda:self.session,
            observer=self.observe,clock_read=self.clock_read,clock_wait=self.clock_wait),10)
        self.assertEqual(result['status'],'raw_capture_complete',result)
        self.assertEqual(result['connections'],2)
        records = [r for name in result['segments'] for r in iter_segment_records(Path(result['root'])/name)]
        gaps = [strict_json(r.body)['event'] for r in records if r.channel=='capture_gap']
        self.assertIn('gap_end',gaps)
        self.assertGreaterEqual(gaps.count('gap_start'),3)
        socket_events=[strict_json(r.body) for r in records if r.channel=='socket_event']
        self.assertTrue(any(e['code']==1000 for e in socket_events))
        self.assertTrue(first_socket.closed)
        self.assertTrue(second.socket.closed)
        self.assertTrue(verify_run(result['root'])['archive_verified'])
