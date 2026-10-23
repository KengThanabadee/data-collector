from dataclasses import replace
from decimal import Decimal
import unittest

from data_collector.books import SnapshotBook
from data_collector.bootstrap import BufferedBook
from data_collector.contracts import canonical
from data_collector.decoders import decode_depth, decode_seed, native_json
from tests.test_dc_contracts import raw, key


class NativeDecoderTests(unittest.TestCase):
    def record(self, instrument, body, channel, kind='ws_message', **kwargs):
        return raw(body=body if isinstance(body,bytes) else canonical(body), venue=instrument.venue,
                   native_product=instrument.native_product, instrument=instrument,channel=channel,kind=kind,**kwargs)

    def test_deribit_numeric_precision_original_bytes_and_native_actions(self):
        inst=key(); channel=f'book.{inst.native_symbol}.100ms'
        body=b'{"jsonrpc":"2.0","method":"subscription","params":{"channel":"'+channel.encode()+b'","data":{"type":"snapshot","timestamp":1,"instrument_name":"'+inst.native_symbol.encode()+b'","change_id":10,"bids":[["new",0.12345678901234567890123456789,2]],"asks":[]}}}'
        record=self.record(inst,body,channel)
        event=decode_depth(record,'deribit_full')
        state=SnapshotBook(inst,'deribit_full'); state.receive(event)
        self.assertEqual(state.view()['bids'],[('0.12345678901234567890123456789','2')])
        self.assertEqual(record.body,body)
        self.assertEqual(event.source_record_id,record.record_id)
        value=native_json(body);value['params']['data'].update(type='change',prev_change_id=10,change_id=11,bids=[['delete','0.12345678901234567890123456789',0]])
        delta=decode_depth(self.record(inst,value,channel),'deribit_full')
        self.assertEqual(state.receive(delta),'applied');self.assertEqual(state.view()['bids'],[])

    def test_deribit_wrong_channel_type_auth_or_missing_predecessor(self):
        inst=key();channel=f'book.{inst.native_symbol}.100ms'
        data=dict(type='snapshot',timestamp=1,instrument_name=inst.native_symbol,change_id=10,bids=[],asks=[])
        value=dict(jsonrpc='2.0',method='subscription',params=dict(channel=channel,data=data))
        for change in ['raw','wrong_symbol','missing_previous']:
            copy=native_json(canonical(value))
            if change=='raw':copy['params']['channel']=f'book.{inst.native_symbol}.raw'
            elif change=='wrong_symbol':copy['params']['data']['instrument_name']='FOREIGN'
            else:copy['params']['data']['type']='change'
            with self.assertRaises(ValueError):decode_depth(self.record(inst,copy,channel),'deribit_full')

    def test_bybit_option_native_schema_and_l1_snapshot_only(self):
        inst=key(venue='bybit');topic=f'orderbook.100.{inst.native_symbol}'
        payload=dict(topic=topic,type='snapshot',ts=1,data=dict(s=inst.native_symbol,u=10,seq=100,b=[['1','2']],a=[]))
        event=decode_depth(self.record(inst,payload,topic),'bybit_standard',depth=100)
        self.assertEqual(event.cross_sequence,100)
        with self.assertRaises(ValueError):decode_depth(self.record(inst,payload,topic),'bybit_full')
        spot=key(venue='bybit',native_product='spot',native_symbol='BTCUSDT')
        topic='orderbook.1.BTCUSDT'
        payload.update(topic=topic,type='delta',data=dict(s='BTCUSDT',u=10,seq=100,b=[],a=[]))
        with self.assertRaises(ValueError):decode_depth(self.record(spot,payload,topic),'bybit_standard',depth=1)

    def test_bybit_full_seed_pair_from_native_raw_messages(self):
        inst=key(venue='bybit',native_product='linear',native_symbol='BTCUSDT');topic='orderbook.full.BTCUSDT'
        data=dict(s='BTCUSDT',u=10,seq=100,b=[['1','9']],a=[])
        delta=decode_depth(self.record(inst,dict(topic=topic,type='delta',ts=1,data=data),topic),'bybit_full')
        rest=dict(retCode=0,result=dict(s='BTCUSDT',u=10,seq=100,b=[['1','2']],a=[]))
        archived=self.record(inst,rest,'/v5/market/full_orderbook','rest_response',
            native_fields_json=canonical(dict(request_category='linear',request_symbol='BTCUSDT')))
        seed=decode_seed(archived,'bybit_full')
        with self.assertRaises(ValueError):decode_seed(replace(archived,native_fields_json=canonical(
            dict(request_category='spot',request_symbol='BTCUSDT'))),'bybit_full')
        state=BufferedBook(inst,'bybit_full',now_ns=0);state.receive(delta,now_ns=1)
        self.assertEqual(state.supply_seed(seed,now_ns=2),'live')
        self.assertEqual(state.view()['bids'],[('1','2')])

    def test_binance_combined_seed_request_identity_and_um_predecessor(self):
        inst=key(venue='binance',native_product='linear',native_symbol='BTCUSDT');channel='btcusdt@depth@100ms'
        payload=dict(e='depthUpdate',E=1,s='BTCUSDT',U=9,u=10,pu=8,st=1,ps='BTCUSDT',b=[['1','3']],a=[])
        record=self.record(inst,dict(stream=channel,data=payload),channel)
        delta=decode_depth(record,'binance_um')
        self.assertEqual(delta.event.previous_id,8)
        rest=self.record(inst,dict(lastUpdateId=10,bids=[['1','2']],asks=[]),'/fapi/v1/depth',
                         'rest_response',native_fields_json=canonical(dict(request_symbol='BTCUSDT')))
        seed=decode_seed(rest,'binance_um')
        state=BufferedBook(inst,'binance_um',now_ns=0);state.receive(delta,now_ns=1)
        self.assertEqual(state.supply_seed(seed,now_ns=2),'live')
        self.assertEqual(state.view()['bids'],[('1','3')])
        with self.assertRaises(ValueError):decode_seed(replace(rest,native_fields_json=b'{}'),'binance_um')
        payload['st']=2
        with self.assertRaises(ValueError):decode_depth(self.record(inst,payload,channel),'binance_um')

    def test_spot_ranges_and_foreign_native_routing(self):
        inst=key(venue='binance',native_product='spot',native_symbol='BTCUSDT');channel='btcusdt@depth'
        payload=dict(e='depthUpdate',E=1,s='BTCUSDT',U=10,u=11,b=[],a=[])
        event=decode_depth(self.record(inst,payload,channel),'binance_spot')
        self.assertEqual(event.first_update_id,10)
        with self.assertRaises(ValueError):decode_depth(self.record(inst,payload,channel),'binance_options')
        with self.assertRaises(ValueError):decode_depth(self.record(inst,dict(stream='ethusdt@depth',data=payload),channel),'binance_spot')
        payload['s']='ETHUSDT'
        with self.assertRaises(ValueError):decode_depth(self.record(inst,payload,channel),'binance_spot')

    def test_native_json_duplicate_nonfinite_float_and_resource_rejection(self):
        parsed=native_json(b'{"p":0.12345678901234567890123456789}')
        self.assertEqual(parsed['p'],Decimal('0.12345678901234567890123456789'))
        for body in [b'{"u":1,"u":2}',b'{"p":NaN}',b'{"p":1e999999}',b'{"u":'+b'1'*129+b'}',b'[]',b'\xff']:
            with self.assertRaises(ValueError):native_json(body)
        with self.assertRaises(ValueError):native_json(b'{}',max_bytes=1)

    def test_error_seed_wrong_kind_numeric_schema_and_foreign_incarnation(self):
        inst=key(venue='bybit',native_product='spot',native_symbol='BTCUSDT')
        seed=self.record(inst,dict(retCode=100,result={}),'/v5/market/full_orderbook','rest_response')
        with self.assertRaises(ValueError):decode_seed(seed,'bybit_full')
        with self.assertRaises(ValueError):decode_depth(seed,'bybit_full')
        topic='orderbook.50.BTCUSDT'
        payload=dict(topic=topic,type='snapshot',ts=1,data=dict(s='BTCUSDT',u=10,seq=100,b=[[1,2]],a=[]))
        with self.assertRaises(ValueError):decode_depth(self.record(inst,payload,topic),'bybit_standard',depth=50)
        with self.assertRaises(ValueError):decode_depth(replace(self.record(inst,payload,topic),instrument=None),'bybit_standard',depth=50)


if __name__=='__main__':
    unittest.main()
