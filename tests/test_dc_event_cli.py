"""Operators run the Binance and Bybit event streams through the command line."""
import asyncio
from contextlib import redirect_stdout
import io
from pathlib import Path
import re
import unittest
from unittest.mock import AsyncMock, patch

from data_collector.app import run
from data_collector.cli import main
from data_collector.contracts import RawRecord, strict_json
from data_collector.rawrun import RawOwner
from data_collector.segments import iter_segment_records
from tests.helpers import (CONFIG, BINANCE_EVENTS, BYBIT_LIQUIDATION, BYBIT_OPTION_TRADE, AppHarness,
    BinanceEventSession, BybitEventSession)


BINANCE = CONFIG / 'v2-binance.toml'
BYBIT = CONFIG / 'v2-bybit-options.toml'


class EventCommandTests(AppHarness, unittest.TestCase):
    def command(self, *argv):
        output = io.StringIO()
        with redirect_stdout(output):
            code = main(list(argv))
        return code, strict_json(output.getvalue().encode())

    def test_config_check_reports_the_binance_event_config_without_network(self):
        code, report = self.command('config-check', '--config', str(BINANCE))
        self.assertEqual(code, 0)
        self.assertRegex(report['config_sha256'], '^[0-9a-f]{64}$')
        self.assertEqual(report['config']['venue'], 'binance')
        self.assertEqual(report['config']['environment'], 'production')
        self.assertTrue(report['config']['event_streams'])
        self.assertEqual(report['config']['capture_mode'], 'raw')
        self.assertEqual(report['config']['data_root'], str((CONFIG.parent / 'data/v2/binance-events').resolve()))

    def test_capture_and_verify_run_archive_the_recorded_binance_liquidations(self):
        self.session = BinanceEventSession()
        self.hold_binance_clock_and_budget()
        code, captured = self.capture(BINANCE)
        self.assertEqual(code, 0, captured)
        self.assertEqual(captured['status'], 'raw_capture_complete', captured)
        self.assertEqual([url for url, _ in self.session.sockets], ['wss://fstream.binance.com/market/stream'])

        export = self.root.parent / 'events.jsonl'
        code, verified = self.command('verify-run', captured['root'], '--export', str(export))
        self.assertEqual(code, 0, verified)
        self.assertTrue(verified['archive_verified'])
        self.assertTrue(verified['feed_plan_initialization_verified'])
        self.assertTrue(verified['liquidation_pushes_observed'])
        self.assertEqual(verified['topics_seen'], ['!forceOrder@arr'])
        self.assertIn(BINANCE_EVENTS['samples']['!forceOrder@arr'].encode(), self.exported_pushes(captured, export))

    def test_config_check_reports_the_bybit_event_config_without_network(self):
        code, report = self.command('config-check', '--config', str(BYBIT))
        self.assertEqual(code, 0)
        self.assertRegex(report['config_sha256'], '^[0-9a-f]{64}$')
        self.assertEqual(report['config']['venue'], 'bybit')
        self.assertEqual(report['config']['environment'], 'production')
        self.assertTrue(report['config']['event_streams'])
        self.assertEqual(report['config']['bybit_liquidation_symbols'], ['BTCUSDT', 'ETHUSDT'])
        self.assertEqual(report['config']['data_root'], str((CONFIG.parent / 'data/v2/bybit-events').resolve()))

    def test_capture_and_verify_run_archive_the_recorded_bybit_liquidations_and_option_trades(self):
        self.session = BybitEventSession()
        code, captured = self.capture(BYBIT)
        self.assertEqual(code, 0, captured)
        self.assertEqual(captured['status'], 'raw_capture_complete', captured)
        self.assertEqual(sorted(url for url, _ in self.session.sockets), [
            'wss://stream.bytick.com/v5/public/linear', 'wss://stream.bytick.com/v5/public/option'])

        export = self.root.parent / 'events.jsonl'
        code, verified = self.command('verify-run', captured['root'], '--export', str(export))
        self.assertEqual(code, 0, verified)
        self.assertTrue(verified['archive_verified'])
        self.assertTrue(verified['feed_plan_initialization_verified'])
        self.assertTrue(verified['liquidation_pushes_observed'])
        self.assertEqual(verified['topics_seen'], ['allLiquidation.ETHUSDT', 'publicTrade.BTC'])
        self.assertLessEqual({BYBIT_LIQUIDATION, BYBIT_OPTION_TRADE}, set(self.exported_pushes(captured, export)))

    def test_capture_exits_nonzero_when_bybit_rejects_a_subscription_and_the_prefix_still_verifies(self):
        self.session = BybitEventSession()
        self.session.option_ack_topics = ['publicTrade.BTC']
        code, captured = self.capture(BYBIT, duration_seconds='0')
        self.assertEqual(code, 1, captured)
        self.assertEqual(captured['status'], 'failed', captured)
        self.assertEqual(captured['rejected_subscriptions'], 1)

        code, verified = self.command('verify-run', captured['root'])
        self.assertEqual(code, 0, verified)
        self.assertTrue(verified['archive_verified'])
        self.assertFalse(verified['feed_plan_initialization_verified'])

    def capture(self, shipped, **overrides):
        """Run `capture` on a copy of a shipped config whose data root is this test's root."""
        values = dict(data_root='"data"', duration_seconds='1') | overrides
        text = shipped.read_text()
        for name, value in values.items():
            text, count = re.subn(rf'^{name} = .*$', f'{name} = {value}', text, flags=re.M)
            self.assertEqual(count, 1, name)
        config = self.root.parent / shipped.name
        config.write_text(text)

        async def local_run(value, **kwargs):
            return await run(value, session_factory=lambda: self.session, observer=self.observe,
                clock_read=self.clock_read, clock_wait=self.clock_wait, **kwargs)
        with patch('data_collector.cli.run', side_effect=local_run):
            code, report = self.command('capture', '--config', str(config))
        self.assertEqual(Path(report['root']).parent.parent, self.root)
        return code, report

    def hold_binance_clock_and_budget(self):
        """Binance capture starts without waiting for clock alignment or the shared request budget."""
        async def clock_wait(deadline):
            await asyncio.Event().wait()
        self.clock_wait = clock_wait
        patcher = patch.object(RawOwner, 'aligned', new=AsyncMock())
        patcher.start(); self.addCleanup(patcher.stop)
        for name in ('acquire', 'connection', 'observe'):
            patcher = patch('data_collector.binance_budget.Budget.' + name, new=AsyncMock(return_value=True))
            patcher.start(); self.addCleanup(patcher.stop)

    def exported_pushes(self, captured, export):
        """Check the export holds every committed record, and return the pushed venue frames."""
        root = Path(captured['root'])
        committed = [record for name in captured['segments'] for record in iter_segment_records(root / name)]
        exported = [RawRecord.from_bytes(line) for line in export.read_bytes().splitlines()]
        self.assertEqual([record.to_bytes() for record in exported], [record.to_bytes() for record in committed])
        return [record.body for record in exported if record.kind == 'ws_message']
