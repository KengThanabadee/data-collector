from dataclasses import replace
import unittest

from data_collector.bootstrap import BootstrapLimits, BufferedBook, BufferedDelta
from tests.test_dc_books import event, levels
from tests.helpers import key


class BootstrapTests(unittest.TestCase):
    def make(self, profile, **kw):
        instrument = key(venue='bybit' if profile == 'bybit_full' else 'binance',
                         native_product='spot' if profile == 'binance_spot' else 'linear')
        return BufferedBook(instrument, profile, now_ns=0, **kw), instrument

    def delta(self, instrument, u, U=None, pu=None, seq=None, source='b', **kw):
        return BufferedDelta(event(instrument, kind='delta', update_id=u, previous_id=pu,
                                   cross_sequence=seq, source=source, **kw), U)

    def test_spot_strict_documented_bridge_and_following_ranges(self):
        state, inst = self.make('binance_spot')
        state.receive(self.delta(inst, 9, 8), now_ns=1)
        state.receive(self.delta(inst, 12, 10, bids=levels(('100', '3'))), now_ns=2)
        seed = event(inst, update_id=10, bids=levels(('100', '1')), asks=levels(('101', '2')))
        self.assertEqual(state.supply_seed(seed, now_ns=3), 'live')
        self.assertEqual(state.view()['bids'], [('100', '3')])
        self.assertEqual(state.receive(self.delta(inst, 14, 13), now_ns=4), 'applied')
        self.assertEqual(state.receive(self.delta(inst, 16, 16), now_ns=5), 'invalid')
        self.assertEqual(state.view()['reason'], 'range_gap')

    def test_spot_old_seed_retries_and_equal_final_is_discarded(self):
        state, inst = self.make('binance_spot')
        delta = self.delta(inst, 12, 11)
        state.receive(delta, now_ns=1)
        self.assertEqual(state.supply_seed(event(inst, update_id=10), now_ns=2), 'retry_seed')
        # Conservative seed-in-range boundary, not a silently assumed seed+1 bridge.
        self.assertEqual(state.supply_seed(event(inst, update_id=12), now_ns=3), 'waiting_for_bridge')
        self.assertEqual(state.receive(self.delta(inst, 14, 12), now_ns=4), 'live')

    def test_um_inclusive_seed_and_pu_chain_different_from_spot(self):
        state, inst = self.make('binance_um')
        bridge = self.delta(inst, 10, 8, pu=7, bids=levels(('100', '4')))
        state.receive(bridge, now_ns=1)
        self.assertEqual(state.supply_seed(event(inst, update_id=10), now_ns=2), 'live')
        self.assertEqual(state.receive(self.delta(inst, 30, 15, pu=10), now_ns=3), 'applied')
        self.assertEqual(state.receive(self.delta(inst, 40, 31, pu=29), now_ns=4), 'invalid')
        self.assertEqual(state.view()['reason'], 'pu_gap')

    def test_bybit_exact_pair_is_not_applied_twice_and_seq_can_skip(self):
        state, inst = self.make('bybit_full')
        state.receive(self.delta(inst, 10, seq=100, bids=levels(('100', '99'))), now_ns=1)
        state.receive(self.delta(inst, 11, seq=400, asks=levels(('101', '2'))), now_ns=2)
        seed = event(inst, update_id=10, cross_sequence=100, bids=levels(('100', '1')))
        self.assertEqual(state.supply_seed(seed, now_ns=3), 'live')
        self.assertEqual(state.view()['bids'], [('100', '1')])
        self.assertEqual(state.view()['asks'], [('101', '2')])
        self.assertEqual(state.receive(self.delta(inst, 13, seq=500), now_ns=4), 'invalid')

    def test_bybit_old_mismatch_future_seed_and_unknown_deeper_levels(self):
        state, inst = self.make('bybit_full')
        state.receive(self.delta(inst, 10, seq=100), now_ns=1)
        self.assertEqual(state.supply_seed(event(inst, update_id=9, cross_sequence=99), now_ns=2), 'retry_seed')
        self.assertEqual(state.supply_seed(event(inst, update_id=9, cross_sequence=100), now_ns=3), 'retry_seed')
        seed = event(inst, update_id=11, cross_sequence=200)
        self.assertEqual(state.supply_seed(seed, now_ns=4), 'waiting_for_bridge')
        self.assertEqual(state.receive(self.delta(inst, 11, seq=200), now_ns=5), 'live')
        self.assertEqual(state.view()['untouched_deeper_levels'], 'unknown')
        self.assertEqual(state.view()['live_readiness'], 'unverified')

    def test_bybit_duplicates_conflicts_stale_reset_and_recovery_at_one(self):
        state, inst = self.make('bybit_full')
        delta = self.delta(inst, 10, seq=100)
        state.receive(delta, now_ns=1)
        self.assertEqual(state.receive(replace(delta, event=replace(delta.event, source_record_id='c'*64)), now_ns=2), 'duplicate')
        state.supply_seed(event(inst, update_id=10, cross_sequence=100), now_ns=3)
        self.assertEqual(state.receive(delta, now_ns=4), 'duplicate')
        self.assertEqual(state.receive(self.delta(inst, 9, seq=99), now_ns=5), 'stale')
        self.assertEqual(state.receive(self.delta(inst, 1, seq=1), now_ns=6), 'invalid')
        state.reset(now_ns=7)
        state.receive(self.delta(inst, 1, seq=1), now_ns=8)
        self.assertEqual(state.supply_seed(event(inst, update_id=1, cross_sequence=1, bids=levels(('100','2'))), now_ns=9), 'live')
        self.assertEqual(state.receive(self.delta(inst, 1, seq=1, bids=levels(('100','3'))), now_ns=10), 'invalid')

    def test_bybit_empty_reset_is_no_book_until_explicit_reseed(self):
        state, inst = self.make('bybit_full')
        state.receive(self.delta(inst, 1, seq=1), now_ns=1)
        self.assertEqual(state.supply_seed(event(inst, update_id=1, cross_sequence=1), now_ns=2), 'no_book')
        self.assertEqual(state.view()['continuity_confidence'], 'invalid')
        self.assertEqual(state.supply_seed(event(inst, update_id=1, cross_sequence=1), now_ns=2), 'ignored_until_reset')
        self.assertEqual(state.receive(self.delta(inst, 2, seq=2), now_ns=3), 'ignored_until_reset')
        state.reset(now_ns=4)
        state.receive(self.delta(inst, 2, seq=2), now_ns=5)
        self.assertEqual(state.supply_seed(event(inst, update_id=2, cross_sequence=2, bids=levels(('100','1'))), now_ns=6), 'live')

    def test_buffer_count_bytes_timeout_and_clock_regression(self):
        state, inst = self.make('binance_spot', limits=BootstrapLimits(max_events=1))
        state.receive(self.delta(inst, 10, 9), now_ns=1)
        self.assertEqual(state.receive(self.delta(inst, 11, 10), now_ns=2), 'invalid')
        self.assertEqual(state.view()['buffered_bytes'], 0)
        state, inst = self.make('binance_spot', limits=BootstrapLimits(max_bytes=1))
        self.assertEqual(state.receive(self.delta(inst, 10, 9), now_ns=1), 'invalid')
        state, inst = self.make('binance_spot', limits=BootstrapLimits(max_wait_ns=10))
        self.assertEqual(state.poll(now_ns=10), 'invalid')
        state.reset(now_ns=11)
        self.assertEqual(state.poll(now_ns=10), 'invalid')

    def test_buffer_gap_and_namespace_or_missing_native_fields(self):
        state, inst = self.make('bybit_full')
        state.receive(self.delta(inst, 10, seq=100), now_ns=1)
        self.assertEqual(state.receive(self.delta(inst, 12, seq=200), now_ns=2), 'invalid')
        with self.assertRaises(ValueError):state.receive(self.delta(key(), 13), now_ns=3)
        with self.assertRaises(ValueError):self.make('binance_options')
        state, inst = self.make('binance_um')
        with self.assertRaises(ValueError):state.receive(self.delta(inst, 10, 9), now_ns=1)
        with self.assertRaises(ValueError):BufferedDelta(event(inst, kind='delta', update_id=10), 11)


if __name__ == '__main__':
    unittest.main()
