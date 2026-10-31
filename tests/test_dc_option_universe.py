"""Lifecycle fixtures use durable-source IDs and deterministic receipt clocks."""
from dataclasses import replace
import unittest

from data_collector.contracts import strict_json
from data_collector.option_universe import (Option,OptionUniverse,bybit_options,
    deribit_lifecycle,DEADLINE_NS)

MS=1_000_000
NOW=1_800_000_000_000


def option(symbol='BTC-SYNTHETIC-C', *, venue='deribit', base='BTC', state='open', expiry=None):
    return Option(venue,symbol,base,'listing:1',state,expiry or NOW+3600000,
        NOW-10000 if venue=='bybit' else None,NOW-1000 if venue=='deribit' else None,{})


def page(rows, cursor='', *, base='BTC', request_cursor=''):
    params=dict(category='option',baseCoin=base,limit=1000)
    if request_cursor:params['cursor']=request_cursor
    return params,dict(retCode=0,result=dict(category='option',list=rows,nextPageCursor=cursor))


def native(symbol='BTC-SYNTHETIC-C',state='PreLaunch'):
    return dict(symbol=symbol,symbolId=1,baseCoin='BTC',quoteCoin='USDT',settleCoin='USDT',
        launchTime=str(NOW+10000),deliveryTime=str(NOW+3600000),status=state)


class OptionUniverseTests(unittest.TestCase):
    def request(self,state,symbols):
        state.requested(symbols,source_id='request-record',connection='socket',
            utc_ns=NOW*MS,monotonic_ns=100)

    def refresh(self,state,rows,**kwargs):
        return strict_json(state.refresh(rows,source_ids=['raw-metadata'],utc_ns=NOW*MS,
            monotonic_ns=100,**kwargs))

    def test_new_strike_ack_tracks_creation_and_observation_delay(self):
        state=OptionUniverse('deribit');first=self.refresh(state,[option()])
        self.assertEqual(first['added'],['BTC-SYNTHETIC-C'])
        self.request(state,first['added'])
        evidence=strict_json(state.acknowledge(first['added'],source_id='raw-subscribe-ack',
            utc_ns=(NOW+500)*MS,monotonic_ns=100+500*MS,connection='socket'))
        self.assertEqual(evidence['options'][0]['observed_to_ack_ns'],500*MS)
        self.assertEqual(evidence['options'][0]['creation_to_ack_ns'],1500*MS)
        self.assertTrue(evidence['options'][0]['observation_deadline_met'])
        self.assertFalse(state.pending)
        revision=self.refresh(state,[option(),option('BTC-NEW-P')])
        self.assertEqual(revision['added'],['BTC-NEW-P'])
        self.assertEqual(revision['parent_sha256'],evidence['universe_sha256'])

    def test_reconnect_cannot_reset_new_listing_deadline(self):
        state=OptionUniverse('deribit');self.refresh(state,[option()])
        state.reconnect(utc_ns=(NOW+110000)*MS,monotonic_ns=100+110000*MS)
        self.assertEqual(state.overdue(101+DEADLINE_NS),('BTC-SYNTHETIC-C',))
        self.request(state,['BTC-SYNTHETIC-C'])
        evidence=strict_json(state.acknowledge(['BTC-SYNTHETIC-C'],source_id='raw-ack',
            utc_ns=(NOW+121000)*MS,monotonic_ns=100+121000*MS,connection='socket'))
        self.assertFalse(evidence['options'][0]['observation_deadline_met'])

    def test_prelaunch_to_trading_keeps_subscription_and_records_transition(self):
        state=OptionUniverse('bybit');row=option(venue='bybit',state='PreLaunch')
        self.refresh(state,[row]);self.request(state,[row.symbol]);state.acknowledge([row.symbol],source_id='raw-ack',
            utc_ns=NOW*MS,monotonic_ns=101,connection='socket')
        revision=self.refresh(state,[replace(row,state='Trading')])
        self.assertEqual(revision['changed'],[row.symbol]);self.assertFalse(revision['added'])
        self.assertIn(row.symbol,state.acknowledged);self.assertFalse(state.pending)

    def test_incomplete_bad_or_empty_discovery_never_erases_members(self):
        state=OptionUniverse('deribit');self.refresh(state,[option()]);prior=state.revision_sha256
        for rows in ([],[option(),option()], [option(venue='bybit')]):
            with self.assertRaises(ValueError):self.refresh(state,rows)
            self.assertEqual(state.revision_sha256,prior)
            self.assertEqual(len(state.members),1)
        self.assertTrue(strict_json(state.stale('HTTP429'))['stale'])
        self.assertTrue(state.unhealthy)

    def test_partition_poll_does_not_retire_other_base_coin(self):
        state=OptionUniverse('bybit');btc=option(venue='bybit',state='PreLaunch')
        eth=option('ETH-SYNTHETIC-P',venue='bybit',base='ETH',state='Trading')
        self.refresh(state,[btc],base='BTC');self.refresh(state,[eth],base='ETH')
        revision=self.refresh(state,[replace(btc,state='Trading')],base='BTC')
        self.assertEqual(len(state.members),2);self.assertFalse(revision['retired'])

    def test_expiry_and_listing_incarnation_retire_pending(self):
        state=OptionUniverse('deribit');row=option();self.refresh(state,[row])
        revision=self.refresh(state,[replace(row,expiry_ms=NOW)])
        self.assertEqual(revision['retired'],[row.symbol]);self.assertFalse(state.pending)
        self.refresh(state,[row]);revision=self.refresh(state,[replace(row,incarnation='listing:2')])
        self.assertEqual(revision['added'],[row.symbol]);self.assertEqual(revision['retired'],[row.symbol])

    def test_delivering_keeps_settlement_tickers_until_expiry(self):
        state=OptionUniverse('bybit');row=option(venue='bybit',state='Delivering')
        self.refresh(state,[row]);self.assertIn(row.symbol,state.members)

    def test_settlement_does_not_mean_expiry(self):
        state=OptionUniverse('deribit');row=option(state='settlement')
        self.refresh(state,[row]);self.assertIn(row.symbol,state.members)
        self.assertFalse(deribit_lifecycle(dict(instrument_name=row.symbol,state='settlement',timestamp=NOW))['terminal'])
        self.assertTrue(deribit_lifecycle(dict(instrument_name=row.symbol,state='delivered',timestamp=NOW))['terminal'])

    def test_unknown_lifecycle_fails_without_guessing(self):
        with self.assertRaises(ValueError):
            deribit_lifecycle(dict(instrument_name='BTC-SYNTHETIC',state='magic',timestamp=NOW))

    def test_bound_fails_without_selecting_prefix(self):
        state=OptionUniverse('deribit',max_instruments=1);self.refresh(state,[option()])
        with self.assertRaises(ValueError):self.refresh(state,[option(),option('BTC-SECOND-P')])
        self.assertEqual(set(state.members),{'BTC-SYNTHETIC-C'})

    def test_many_revisions_keep_only_current_members_and_pending(self):
        state=OptionUniverse('deribit')
        for i in range(10005):
            row=option('BTC-STRIKE-'+str(i))
            self.refresh(state,[row]);self.request(state,[row.symbol]);state.acknowledge([row.symbol],source_id='raw-ack-'+str(i),
                utc_ns=NOW*MS,monotonic_ns=101,connection='socket')
        self.assertEqual(state.revision,10005);self.assertEqual(len(state.members),1)
        self.assertEqual(len(state.acknowledged),1);self.assertFalse(state.pending)

    def test_bybit_pagination_keeps_prelaunch_and_usdt_metadata(self):
        second=native('BTC-SECOND-P','Trading');second['symbolId']=2
        rows=bybit_options([page([native()],'next'),page([second],request_cursor='next')],base='BTC')
        self.assertEqual(len(rows),2);self.assertEqual(rows[0].state,'PreLaunch')
        self.assertEqual(rows[0].native['settleCoin'],'USDT');self.assertIsNone(rows[0].created_ms)
        self.assertEqual(rows[0].launch_ms,NOW+10000)

    def test_bybit_incomplete_repeated_cursor_or_status_filter_rejected(self):
        with self.assertRaises(ValueError):bybit_options([page([native()],'next')],base='BTC')
        with self.assertRaises(ValueError):
            bybit_options([page([native()],'next'),page([],'next',request_cursor='next')],base='BTC')
        params,body=page([native()]);params['status']='Trading'
        with self.assertRaises(ValueError):bybit_options([(params,body)],base='BTC')
        with self.assertRaises(ValueError):bybit_options([page([])],base='BTC')


if __name__=='__main__':unittest.main()
