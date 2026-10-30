"""Exercise continuous raw capture against deterministic transport fixtures."""
import asyncio
import unittest
from dataclasses import replace
from pathlib import Path
from unittest.mock import patch

from data_collector.app import verify_run
from data_collector.continuous import capture
from data_collector.clocksource import ChronyClockSampler
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
        self.config = replace(self.config, capture_mode='raw', duration_seconds=0, max_frames=4)
        with patch('data_collector.continuous.ChronyClockSampler', wraps=ChronyClockSampler) as sampler:
            result = await asyncio.wait_for(capture(self.config, session_factory=lambda:self.session,
                observer=self.observe, clock_read=self.clock_read, clock_wait=self.clock_wait), 15)
        self.assertIsNone(sampler.call_args.kwargs['max_samples'])
        self.assertIsNone(sampler.call_args.args[0]._clock.max_assessments)
        self.assertEqual(result['status'], 'raw_capture_complete', result)
        self.assertEqual(result['frames'], 4)
        verified = verify_run(result['root'])
        self.assertTrue(verified['archive_verified'])
        self.assertNotIn('book_replay_equal', verified)
        self.assertEqual(verified['ws_messages'], 4)
        self.assertTrue(self.session.socket.closed)

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
        self.assertTrue(first_socket.closed)
        self.assertTrue(second.socket.closed)
        self.assertTrue(verify_run(result['root'])['archive_verified'])
