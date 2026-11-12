"""Exercise the shipped Bybit event routes through capture and raw readback."""
import asyncio
from dataclasses import replace
import json
from pathlib import Path
import unittest

from data_collector.app import run, verify_run
from data_collector.config import load
from data_collector.contracts import canonical, strict_json
from data_collector.segments import iter_segment_records
from tests.helpers import AppHarness, BybitEventSession as BybitSession, BYBIT_LIQUIDATION as LIQUIDATION, BYBIT_OPTION_TRADE as OPTION_TRADE


class BybitEventTests(AppHarness, unittest.IsolatedAsyncioTestCase):
    async def test_shipped_profile_archives_liquidation_and_option_trade_routes(self):
        shipped = load(Path(__file__).parents[1] / 'config/v2-bybit-options.toml')
        self.config = replace(shipped, data_root=self.root, duration_seconds=1)
        self.session = BybitSession()
        result = await asyncio.wait_for(self.capture(), 10)
        self.assertEqual(result['status'], 'raw_capture_complete', result)
        self.assertEqual({url: socket.sent for url, socket in self.session.sockets}, {
            'wss://stream.bytick.com/v5/public/linear': [{'op': 'subscribe', 'args': [
                'allLiquidation.BTCUSDT', 'allLiquidation.ETHUSDT']}],
            'wss://stream.bytick.com/v5/public/option': [{'op': 'subscribe', 'args': [
                'publicTrade.BTC', 'publicTrade.ETH']}],
        })
        bodies = {record.body for record in self.records(result) if record.kind == 'ws_message'}
        self.assertLessEqual({LIQUIDATION, OPTION_TRADE}, bodies)
        export = self.root.parent / 'events.jsonl'
        verified = verify_run(result['root'], export=export)
        expected = [record.to_bytes() for record in self.records(result)]
        self.assertEqual(export.read_bytes().splitlines(), expected)
        with self.assertRaises(FileExistsError):
            verify_run(result['root'], export=export)
        self.assertEqual(export.read_bytes().splitlines(), expected)
        self.assertTrue(verified['feed_plan_initialization_verified'])
        self.assertTrue(verified['liquidation_pushes_observed'])
        self.assertEqual(verified['topics_seen'], ['allLiquidation.ETHUSDT', 'publicTrade.BTC'])

    async def test_incomplete_option_ack_fails_with_raw_prefix_verifiable(self):
        shipped = load(Path(__file__).parents[1] / 'config/v2-bybit-options.toml')
        self.config = replace(shipped, data_root=self.root)
        self.session = BybitSession()
        self.session.option_ack_topics = ['publicTrade.BTC']
        result = await asyncio.wait_for(self.capture(), 5)
        self.assertEqual(result['status'], 'failed', result)
        self.assertEqual(result['rejected_subscriptions'], 1)
        verified = verify_run(result['root'])
        self.assertTrue(verified['archive_verified'])
        self.assertFalse(verified['feed_plan_initialization_verified'])
        self.assertIn(OPTION_TRADE, [record.body for record in self.records(result) if record.kind == 'ws_message'])

    async def test_failed_verification_leaves_no_export_and_retains_the_native_burst(self):
        shipped = load(Path(__file__).parents[1] / 'config/v2-bybit-options.toml')
        self.config = replace(shipped, data_root=self.root)
        self.session = BybitSession()
        self.session.linear_control = b'{"op":"heartbeat","success":true,"conn_id":"linear-test"}'
        result = await asyncio.wait_for(self.capture(), 5)
        self.assertEqual(result['status'], 'failed', result)
        linear_messages = [record.body for record in self.records(result) if record.kind == 'ws_message'
                           and record.receipt.connection_id.startswith('linear-')]
        self.assertEqual(linear_messages[-2:], [self.session.linear_control, LIQUIDATION])

        export = self.root.parent / 'events.jsonl'
        before = set(export.parent.iterdir())
        with self.assertRaisesRegex(ValueError, 'unplanned Bybit linear'):
            verify_run(result['root'], export=export)
        self.assertEqual(set(export.parent.iterdir()), before)

    async def test_transport_loss_retains_received_payload_and_reopens_configured_route(self):
        shipped = load(Path(__file__).parents[1] / 'config/v2-bybit-options.toml')
        self.config = replace(shipped, data_root=self.root, duration_seconds=3,
            bybit_liquidation_symbols=('ETHUSDT',))
        self.session = BybitSession()
        task = asyncio.create_task(self.capture())
        try:
            async with asyncio.timeout(5):
                while not list(self.root.glob('runs/*/ready.json')):
                    await asyncio.sleep(.02)
            first = next(socket for url, socket in self.session.sockets if url.endswith('/linear'))
            last_payload = LIQUIDATION.replace(b'2258.9400', b'2258.9300')
            first.put(last_payload)
            first.incoming.put_nowait(ConnectionResetError('peer reset during receive burst'))
            result = await asyncio.wait_for(task, 5)
        finally:
            if not task.done():
                task.cancel()
                await asyncio.gather(task, return_exceptions=True)
        self.assertEqual(result['status'], 'raw_capture_complete', result)
        linear = [socket for url, socket in self.session.sockets if url.endswith('/linear')]
        self.assertEqual([socket.sent for socket in linear], [[dict(op='subscribe', args=['allLiquidation.ETHUSDT'])]] * 2)
        records = self.records(result)
        self.assertIn(last_payload, [record.body for record in records if record.kind == 'ws_message'])
        controls = [(record.receipt.connection_id, strict_json(record.body))
                    for record in records if record.channel == 'capture_control']
        gaps = [(conn, value['event'], value.get('reason')) for conn, value in controls
                if value.get('route') == 'linear' and value['event'].startswith('gap_')]
        first_id, second_id = [conn for conn, event, reason in gaps if reason == 'connection_start']
        self.assertEqual(gaps, [(first_id, 'gap_start', 'connection_start'), (first_id, 'gap_end', None),
            (first_id, 'gap_start', 'connection_end'), (second_id, 'gap_start', 'connection_start'),
            (second_id, 'gap_end', None), (second_id, 'gap_start', 'connection_end')])
        self.assertTrue(verify_run(result['root'])['feed_plan_initialization_verified'])

    async def capture(self):
        return await run(self.config, session_factory=lambda: self.session, observer=self.observe,
            clock_read=self.clock_read, clock_wait=self.clock_wait)

    def records(self, result):
        return [record for name in result['segments'] for record in iter_segment_records(Path(result['root']) / name)]
