from pathlib import Path
from types import SimpleNamespace
import tempfile
import unittest
from unittest.mock import AsyncMock, patch

import aiohttp

from data_collector.contracts import canonical, digest
from data_collector.segments import iter_segment_records, verify_segment
from experiments import dc_deribit_probe as probe


class Content:
    def __init__(self, body):self.body=body
    async def iter_chunked(self, size):
        for start in range(0,len(self.body),size):yield self.body[start:start+size]


class Response:
    status=200
    def __init__(self,body):self.content=Content(body)
    async def __aenter__(self):return self
    async def __aexit__(self,*args):return False


class Socket(Response):
    def __init__(self,messages):self.messages=iter(messages);self.sent=[]
    async def send_str(self,data):self.sent.append(data)
    async def receive(self):return next(self.messages,SimpleNamespace(type=aiohttp.WSMsgType.CLOSED))


class ProbeTests(unittest.IsolatedAsyncioTestCase):
    async def capture(self, mode='normal'):
        source=b'synthetic official schema source fixture'
        bodies={}
        symbols=[]
        for currency in ('BTC','ETH'):
            symbol=currency+'-SYNTHETIC-C';symbols.append(symbol)
            bodies[currency]=canonical(dict(result=[dict(kind='option',is_active=True,base_currency=currency,
                expiration_timestamp=9_999_999_999_999,creation_timestamp=1,instrument_name=symbol,
                option_type='call',strike=100)]))
        channels=[f'book.{s}.100ms' for s in symbols]
        index_body=canonical(dict(result=dict(index_price=100)))
        payloads=[canonical(dict(jsonrpc='2.0',id=1,result=channels))]
        for symbol in symbols:
            for index in range(3):
                data=dict(type='snapshot' if index==0 else 'change',timestamp=index+1,
                    instrument_name=symbol,change_id=index+10,bids=[['new' if index==0 else 'change',100,index+1]],asks=[])
                if index:data['prev_change_id']=index+9
                payloads.append(canonical(dict(jsonrpc='2.0',method='subscription',
                                              params=dict(channel=f'book.{symbol}.100ms',data=data))))
        if mode=='duplicate':payloads[3]=payloads[2];payloads[6]=payloads[5]
        if mode=='pre_snapshot':payloads=[payloads[i] for i in [0,2,3,1,5,6,4]]
        malformed={
            'null_params':dict(method='subscription',params=None),
            'null_data':dict(method='subscription',params=dict(data=None,channel=channels[0])),
            'missing_channel':dict(method='subscription',params=dict(data=dict(instrument_name=symbols[0]))),
            'bad_ack':dict(id=1,result=[{}])}
        if mode in malformed:payloads.append(canonical(malformed[mode]))
        cap=sum(map(len,bodies.values()))+2*len(index_body)+len(payloads[0])+len(payloads[1])-1 if mode=='cap' else probe.MAX_TOTAL
        if mode=='bad_discovery':bodies['ETH']=b'{"jsonrpc":"2.0","error":{"code":-1}}'
        socket=Socket([SimpleNamespace(type=aiohttp.WSMsgType.TEXT,data=p.decode()) for p in payloads])
        class Session(Response):
            def __init__(self,*args,**kwargs):pass
            def get(self,url,*,params,allow_redirects):
                self.asserted=(url,allow_redirects)
                if url==probe.INDEX_HTTP:return Response(index_body)
                return Response(bodies[params['currency']])
            def ws_connect(self,url,**kwargs):return socket
        original_bytes=Path.read_bytes;original_text=Path.read_text
        def read_bytes(path,*args,**kwargs):
            return source if str(path).endswith('/D1.body') else original_bytes(path,*args,**kwargs)
        def read_text(path,*args,**kwargs):
            if str(path)=='docs/v2/source-evidence.json':return canonical(dict(sources=[dict(sha256=digest(source))])).decode()
            return original_text(path,*args,**kwargs)
        with tempfile.TemporaryDirectory() as directory, patch.object(probe.aiohttp,'ClientSession',Session), \
                patch.object(probe.asyncio,'sleep',AsyncMock()), patch.object(Path,'read_bytes',read_bytes), \
                patch.object(Path,'read_text',read_text),patch.object(probe,'MAX_TOTAL',cap):
            if mode=='bad_discovery':
                with self.assertRaises(ValueError):await probe.run(Path(directory))
                children=list(Path(directory).iterdir());self.assertEqual(len(children),1)
                for currency,body in bodies.items():
                    self.assertEqual((children[0]/f'discovery-{currency}.body').read_bytes(),body)
                    self.assertTrue((children[0]/f'discovery-{currency}.json').exists())
                return
            report=await probe.run(Path(directory))
            self.assertTrue(report['archive_verified'])
            if mode not in malformed:self.assertTrue(report['book_replay_equal'])
            self.assertEqual(report['frames'],2 if mode=='cap' else len(payloads))
            records=list(iter_segment_records(report['manifest']))
            received=[r.body for r in records if r.kind=='ws_message']
            self.assertEqual(received,payloads[:2] if mode=='cap' else payloads)
            self.assertEqual([r.body for r in records if r.channel=='source_bundle:D1'],[source])
            self.assertTrue(verify_segment(report['manifest']))
            self.assertEqual(report['payload_bytes'],sum(map(len,bodies.values()))+2*len(index_body)+sum(map(len,received)))
            return report

    async def test_native_capture_archive_and_replay_with_bounded_fake_exchange(self):
        report=await self.capture()
        self.assertTrue(report['initial_native_book_evidence'])
        for counts in report['counts'].values():self.assertEqual(counts['applied_deltas'],2)

    async def test_duplicate_or_pre_snapshot_deltas_cannot_pass_native_evidence(self):
        for mode in ['duplicate','pre_snapshot']:
            report=await self.capture(mode)
            self.assertFalse(report['initial_native_book_evidence'])
            for counts in report['counts'].values():self.assertLess(counts['applied_deltas'],2)

    async def test_crossing_message_is_archived_and_accounted_under_reserve(self):
        report=await self.capture('cap')
        self.assertEqual(report['stop_reason'],'total_byte_bound')
        self.assertGreater(report['payload_bytes'],report['config']['max_total_bytes'])
        self.assertLessEqual(report['payload_bytes'],report['config']['max_total_bytes']+report['config']['overflow_reserve_bytes'])

    async def test_http200_discovery_error_preserves_both_completed_responses(self):
        await self.capture('bad_discovery')

    async def test_malformed_subscription_or_ack_is_archived_sealed_and_reported(self):
        for mode in ['null_params','null_data','missing_channel','bad_ack']:
            report=await self.capture(mode)
            self.assertFalse(report['capture_schema_ok'])
            self.assertFalse(report['initial_native_book_evidence'])
            self.assertTrue(report['stop_reason'].startswith('ValueError:'))

    async def test_response_cap_stops_without_unbounded_read(self):
        with patch.object(probe,'MAX_RESPONSE',2):
            with self.assertRaisesRegex(ValueError,'byte cap'):await probe.bounded_response(Response(b'123'))

    def test_discovery_rejects_expired_inactive_or_missing_native_identity(self):
        for value in [dict(error={}),dict(result=[]),dict(result=[dict(kind='option',is_active=False)])]:
            with self.assertRaises(ValueError):probe.choose_instrument(value,'BTC',100)

    def test_selection_prefers_nearest_expiry_call_near_current_index(self):
        base=dict(kind='option',is_active=True,base_currency='BTC',expiration_timestamp=1000,
                  creation_timestamp=1,option_type='call')
        candidates=[dict(base,instrument_name=str(p),strike=p) for p in [10,90,100,110]]
        self.assertEqual(probe.choose_instrument(dict(result=candidates),'BTC',100,102)['strike'],100)
        with self.assertRaises(ValueError):probe.choose_instrument(dict(result=candidates),'BTC',100,-1)
        with self.assertRaisesRegex(ValueError,'no eligible calls'):
            probe.choose_instrument(dict(result=[dict(base,option_type='put',instrument_name='PUT',strike=100)]),'BTC',100,102)
        missing=dict(base,instrument_name='MISSING',strike=100);missing.pop('option_type')
        with self.assertRaisesRegex(ValueError,'no eligible calls'):
            probe.choose_instrument(dict(result=[missing]),'BTC',100,102)
