from dataclasses import replace
from decimal import Decimal
import unittest

from data_collector.books import BookLimits, DepthEvent, L2Book, LevelChange, SnapshotBook, decimal_value
from tests.test_dc_contracts import key


def levels(*values):
    return tuple(LevelChange(*item) for item in values)


def event(instrument=None, kind='snapshot', update_id=10, source='a', **changes):
    return DepthEvent(instrument or key(), source*64, kind, update_id, **changes)


class DecimalBookTests(unittest.TestCase):
    def test_exact_decimal_no_float_and_atomic_rejected_update(self):
        book=L2Book(key())
        price='0.123456789012345678901234567890'
        book.apply(event(bids=levels((price,'100.12345678901234567890'))),replace=True)
        self.assertEqual(book.levels()['bids'],[(price.rstrip('0'),'100.1234567890123456789')])
        before=book.levels()
        with self.assertRaises(ValueError):
            book.apply(event(bids=levels(('1','2'),('0','1'))))
        self.assertEqual(book.levels(),before)
        for value in [1.2, True, 'NaN','Infinity','1e9999999']:
            with self.assertRaises(ValueError):decimal_value(value)

    def test_absolute_replacement_delete_duplicate_levels_and_resource_limit(self):
        book=L2Book(key(),limits=BookLimits(2))
        book.apply(event(bids=levels(('1','5'),('2','6'))),replace=True)
        book.apply(event(bids=levels(('1.0','7'),('2','0'))))
        self.assertEqual(book.levels()['bids'],[('1','7')])
        book.apply(event(bids=levels(('1','99','delete'))))
        self.assertEqual(book.levels()['bids'],[])
        with self.assertRaises(ValueError):
            book.apply(event(bids=levels(('1','1'),('1.00','2'))),replace=True)
        with self.assertRaises(ValueError):
            book.apply(event(bids=levels(('1','1'),('2','2'),('3','3'))))
        self.assertEqual(book.levels()['bids'],[])

    def test_spread_prices_require_explicit_convention_and_native_identity(self):
        spread=key(native_product='combo',native_symbol='SYNTHETIC-SPREAD')
        book=L2Book(spread,signed_prices=True)
        book.apply(event(spread,bids=levels(('-1','2')),asks=levels(('0','3'))),replace=True)
        self.assertEqual(book.levels()['bids'],[('-1','2')])
        with self.assertRaises(ValueError):book.apply(event())
        with self.assertRaises(ValueError):LevelChange('1','-2')


class SnapshotFeedTests(unittest.TestCase):
    def test_deribit_prev_change_gap_duplicate_and_fresh_recovery_anchor(self):
        state=SnapshotBook(key(),'deribit_full')
        seed=event(bids=levels(('100','2')),asks=levels(('101','3')))
        self.assertEqual(state.receive(seed),'applied')
        delta=event(kind='delta',update_id=20,previous_id=10,bids=levels(('100','4')))
        self.assertEqual(state.receive(delta),'applied')
        self.assertEqual(state.receive(seed),'stale')
        self.assertEqual(state.view()['update_id'],20)
        self.assertEqual(state.receive(replace(delta,source_record_id='b'*64)),'duplicate')
        self.assertEqual(state.view()['continuity_confidence'],'sequence_proven')
        self.assertEqual(state.receive(event(kind='delta',update_id=30,previous_id=21)),'invalid')
        self.assertEqual(state.receive(event(kind='delta',update_id=31,previous_id=30)),'ignored_until_snapshot')
        self.assertEqual(state.receive(event(update_id=50,bids=levels(('99','1')))),'applied')
        self.assertEqual(state.view()['bids'],[('99','1')])

    def test_deribit_snapshot_duplicate_conflict_and_bybit_replacement_are_distinct(self):
        state=SnapshotBook(key(),'deribit_full')
        seed=event(bids=levels(('100','2')))
        state.receive(seed);epoch=state.epoch
        self.assertEqual(state.receive(replace(seed,source_record_id='b'*64)),'duplicate')
        self.assertEqual(state.epoch,epoch)
        self.assertEqual(state.receive(replace(seed,bids=levels(('100','99')))),'invalid')
        self.assertEqual(state.view()['bids'],[('100','2')])
        instrument=key(venue='bybit')
        bybit=SnapshotBook(instrument,'bybit_standard',depth=100)
        bybit.receive(event(instrument,update_id=10,cross_sequence=10,bids=levels(('100','2'))))
        self.assertEqual(bybit.receive(event(instrument,update_id=1,cross_sequence=1,bids=levels(('99','2')))),'applied')
        self.assertEqual(bybit.view()['bids'],[('99','2')])

    def test_atomic_insertion_before_deletion_at_capacity_and_reset_clears_old_epoch(self):
        state=SnapshotBook(key(),'deribit_full',limits=BookLimits(1))
        state.receive(event(bids=levels(('100','2'))))
        delta=event(kind='delta',update_id=11,previous_id=10,bids=levels(('99','3','new'),('100','0','delete')))
        self.assertEqual(state.receive(delta),'applied')
        self.assertEqual(state.view()['bids'],[('99','3')])
        state.reset();view=state.view()
        self.assertEqual(view['bids'],[]);self.assertEqual(view['asks'],[])
        self.assertIsNone(view['anchor_source']);self.assertIsNone(view['update_id'])

    def test_bybit_standard_allows_nonconsecutive_ids_but_reset_before_stale(self):
        instrument=key(venue='bybit')
        state=SnapshotBook(instrument,'bybit_standard',depth=100)
        state.receive(event(instrument,update_id=100,cross_sequence=1000,bids=levels(('100','2'))))
        self.assertEqual(state.receive(event(instrument,kind='delta',update_id=110,cross_sequence=1500)),'applied')
        self.assertEqual(state.view()['continuity_confidence'],'health_only')
        self.assertEqual(state.receive(event(instrument,kind='delta',update_id=109,cross_sequence=1499)),'stale')
        self.assertEqual(state.receive(event(instrument,kind='delta',update_id=1,cross_sequence=1)),'invalid')
        self.assertEqual(state.receive(event(instrument,update_id=1,cross_sequence=1)),'applied')
        self.assertIn('missing_bid',state.view()['quality_flags'])
        with self.assertRaises(ValueError):SnapshotBook(instrument,'bybit_standard',depth=1000)
        self.assertEqual(state.receive(event(instrument,update_id=10)),'invalid')

    def test_invalid_quote_quality_is_separate_from_feed_continuity(self):
        state=SnapshotBook(key(),'deribit_full')
        state.receive(event(bids=levels(('102','2')),asks=levels(('101','3'))))
        view=state.view()
        self.assertEqual(view['state'],'live')
        self.assertIn('crossed_book',view['quality_flags'])
        self.assertEqual(view['asks'],[('101','3')])

    def test_bybit_initial_anchor_must_reach_already_observed_delta(self):
        instrument=key(venue='bybit')
        state=SnapshotBook(instrument,'bybit_standard',depth=100)
        self.assertEqual(state.receive(event(instrument,kind='delta',update_id=100,cross_sequence=1000)),'ignored_until_snapshot')
        self.assertEqual(state.receive(event(instrument,update_id=20,cross_sequence=200)),'stale')
        self.assertEqual(state.view()['state'],'bootstrapping')
        self.assertEqual(state.receive(event(instrument,update_id=100,cross_sequence=1000)),'applied')

    def test_conflict_capacity_and_reconnect_do_not_silently_apply(self):
        state=SnapshotBook(key(),'deribit_full',limits=BookLimits(1))
        state.receive(event(bids=levels(('100','2'))))
        self.assertEqual(state.receive(event(kind='delta',update_id=11,previous_id=10,bids=levels(('99','3')))),'invalid')
        self.assertEqual(state.view()['bids'],[('100','2')])
        self.assertEqual(state.receive(event(bids=levels(('100','2')))),'stale')
        self.assertEqual(state.view()['state'],'invalid')
        self.assertEqual(state.receive(event(update_id=11,bids=levels(('100','2')))),'applied')
        self.assertEqual(state.receive(event(kind='delta',update_id=11,previous_id=10,bids=levels(('100','5')))),'invalid')
        state.reset()
        self.assertEqual(state.receive(event(kind='delta',update_id=100,previous_id=99)),'ignored_until_snapshot')
        with self.assertRaises(ValueError):state.receive(event(key(venue='bybit')))
