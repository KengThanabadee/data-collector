import asyncio
from dataclasses import replace
from unittest.mock import patch
import unittest

import aiohttp
from aiohttp import web
from data_collector.quota import Priority

from data_collector.contracts import canonical,strict_json
from data_collector.discovery import DeribitDiscovery,project_instruments,discovery_session
from data_collector.quota import PriorityQuota,QuotaWindow
from tests import test_dc_lane as base


def response_body(rows=None):
    if rows is None:
        return (b'{"jsonrpc":"2.0","unknown_root":true,"result":['
            b'{"instrument_name":"BTC-TEST-C","instrument_id":1,"creation_timestamp":1000,'
            b'"kind":"option","base_currency":"BTC","settlement_currency":"USDC",'
            b'"is_active":false,"tick_size":0.000000000000000000123,"new_field":{"x":1e-20}}]}')
    return canonical(dict(jsonrpc='2.0',result=rows))


class Response:
    def __init__(self,body,status=200,headers=(),delay=None,error=None):
        self.body=body;self.status=status;self.raw_headers=headers;self.content=self
        self.offset=0;self.delay=delay;self.error=error;self.closed=False
    async def __aenter__(self):return self
    async def __aexit__(self,*args):self.closed=True
    async def read(self,size):
        if self.delay is not None:await self.delay.wait()
        if self.error is not None and self.offset:raise self.error
        chunk=self.body[self.offset:self.offset+min(size,19)];self.offset+=len(chunk);return chunk


class HTTP:
    _retry_connection=False
    def __init__(self,responses):self.responses=list(responses);self.calls=[]
    def get(self,url,**kwargs):
        self.calls.append((url,kwargs));return self.responses.pop(0)


class DiscoveryTests(unittest.IsolatedAsyncioTestCase):
    setUp=base.LaneTests.setUp
    observe=base.LaneTests.observe
    lane=base.LaneTests.lane
    start=base.LaneTests.start
    clock_request=base.LaneTests.clock_request
    record=base.LaneTests.record
    received=base.LaneTests.received
    records=base.LaneTests.records

    def clock_policy(self):
        return dict(sample_interval_ns=60,max_sample_age_ns=60_000_000_000,max_offset_ns=100,step_tolerance_ns=2,max_epochs=8)
    async def discovery(self,**kwargs):
        lane,_=self.lane(clock_policy=self.clock_policy());self.start(lane)
        await lane.submit_clock(*self.clock_request(lane));self.mono[0]+=60
        await lane.submit_clock(*self.clock_request(lane,1))
        quota=PriorityQuota('deribit',[QuotaWindow('rest',1000,1_000_000_000),
            QuotaWindow('get_instruments',1,1_000_000_000)],now_ns=lambda:self.mono[0])
        self.addAsyncCleanup(quota.close)
        owner=DeribitDiscovery(lane,quota,charges={'rest':1,'get_instruments':1},max_response_bytes=4096,**kwargs)
        return lane,owner
    async def finish(self,lane):lane.stop();await asyncio.wait_for(lane.test_task,5)
    def raw(self,lane):
        return [r for r in self.records(lane) if r.native_product=='discovery']
    def advance(self,owner):self.mono[0]+=1_000_000_000;owner.quota.notify()

    async def test_exact_raw_query_and_decimal_metadata_preserve_linear_inactive_instrument(self):
        lane,owner=await self.discovery()
        native=response_body();http=HTTP([Response(native,headers=[(b'Set-Cookie',b'secret'),(b'Content-Type',b'application/json')])])
        result=strict_json(await owner.fetch(http))
        self.assertEqual(result['selected'][0]['instrument']['native_symbol'],'BTC-TEST-C')
        attrs=result['selected'][0]['attributes']['native']
        self.assertEqual(attrs['settlement_currency'],'USDC');self.assertFalse(attrs['is_active'])
        self.assertEqual(attrs['tick_size'],'1.23E-19');self.assertEqual(attrs['new_field']['x'],'1E-20')
        self.assertEqual(result['families']['spot']['observed'],0)
        self.assertEqual(http.calls[0][1]['params']['currency'],'any')
        self.assertFalse(http.calls[0][1]['allow_redirects']);self.assertFalse(http.calls[0][1]['auto_decompress'])
        self.assertEqual(owner.quota.health()['grants'],1)
        await self.finish(lane);raw=self.raw(lane)
        self.assertEqual([r.kind for r in raw],['control','rest_response'])
        self.assertEqual(raw[1].body,native)
        self.assertNotIn(b'secret',raw[1].to_bytes())
        self.assertEqual(strict_json(raw[1].native_fields_json)['request_record_id'],raw[0].record_id)
        self.assertEqual(project_instruments(raw[1]),owner.latest)

    async def test_complete_classification_has_explicit_combo_unknown_and_excluded_rows(self):
        lane,owner=await self.discovery()
        common=dict(instrument_id=2,creation_timestamp=1000,base_currency='BTC')
        rows=[dict(common,instrument_name='BTC-F',kind='future'),
            dict(common,instrument_name='BTC-COMBO',kind='option_combo'),
            dict(common,instrument_name='ALT-SPOT',kind='spot',base_currency='ALT'),
            dict(common,instrument_name='NEW',kind='unknown_kind')]
        result=strict_json(await owner.fetch(HTTP([Response(response_body(rows))])))
        self.assertEqual(len(result['selected']),1);self.assertEqual(len(result['excluded']),1)
        self.assertEqual([r['reason'] for r in result['blocked']],['combo_leg_scope_unverified','native_kind_unverified'])
        self.assertEqual(result['total_rows'],sum(len(result[n]) for n in ('selected','excluded','blocked')))
        self.assertEqual(result['families']['option']['selected'],0)

    async def test_empty_or_bad_refresh_retains_last_universe_as_stale(self):
        lane,owner=await self.discovery()
        good=await owner.fetch(HTTP([Response(response_body())]))
        for body in (response_body([]),b'{"jsonrpc":"2.0","result":[{}]}',b'{"error":{"code":1}}'):
            self.advance(owner)
            with self.assertRaises(ValueError):await owner.fetch(HTTP([Response(body)]))
            self.assertEqual(owner.latest,good);self.assertTrue(owner.health()['stale'])
        await self.finish(lane)
        self.assertEqual(len([r for r in self.raw(lane) if r.kind=='rest_response']),4)

    async def test_http_error_and_retry_after_preserve_response_and_defer_shared_pool(self):
        lane,owner=await self.discovery()
        response=Response(b'{"error":"rate limited"}',429,[(b'Retry-After',b'2')])
        with self.assertRaisesRegex(ValueError,'acquisition unsuccessful'):await owner.fetch(HTTP([response]))
        self.assertGreater(owner.quota.pools['get_instruments'].cooldown_ns,self.mono[0])
        self.assertGreater(owner.quota.pools['rest'].cooldown_ns,self.mono[0])
        await self.finish(lane);self.assertEqual(self.raw(lane)[1].body,b'{"error":"rate limited"}')

    async def test_overflow_and_partial_failure_are_explicit_incomplete_evidence(self):
        for error in (None,OSError('read failed')):
            with self.subTest(error=error):
                lane,owner=await self.discovery()
                response=Response(b'x'*6000,error=error)
                with self.assertRaises(ValueError):await owner.fetch(HTTP([response]))
                await self.finish(lane);record=self.raw(lane)[1];fields=strict_json(record.native_fields_json)
                self.assertFalse(fields['complete']);self.assertIsNotNone(fields['error'])
                self.assertEqual(record.body,response.body[:len(record.body)])
                self.assertLessEqual(len(record.body),4097)
                self.assertTrue(response.closed)

    async def test_cancel_during_http_archives_prefix_before_return(self):
        lane,owner=await self.discovery();entered=asyncio.Event();release=asyncio.Event()
        response=Response(response_body())
        original=response.read
        async def blocked(size):entered.set();await release.wait();return await original(size)
        response.read=blocked;http=HTTP([response])
        task=asyncio.create_task(owner.fetch(http));await asyncio.wait_for(entered.wait(),5);task.cancel()
        with self.assertRaises(asyncio.CancelledError):await task
        await self.finish(lane);record=self.raw(lane)[1];fields=strict_json(record.native_fields_json)
        self.assertEqual(fields['error'],'cancelled_during_acquisition');self.assertFalse(fields['complete'])
        self.assertTrue(response.closed)

    async def test_cancel_after_request_ack_never_dispatches_http(self):
        lane,owner=await self.discovery();entered=asyncio.Event();release=asyncio.Event()
        original=lane.submit_received
        def delayed(record):
            durable=original(record)
            async def ack():
                saved=await durable;entered.set();await release.wait();return saved
            return asyncio.create_task(ack())
        http=HTTP([Response(response_body())])
        with patch.object(lane,'submit_received',side_effect=delayed):
            task=asyncio.create_task(owner.fetch(http));await asyncio.wait_for(entered.wait(),5)
            task.cancel();await asyncio.sleep(0);self.assertFalse(task.done());release.set()
            with self.assertRaises(asyncio.CancelledError):await task
        self.assertEqual(http.calls,[])
        await self.finish(lane);fields=strict_json(self.raw(lane)[1].native_fields_json)
        self.assertEqual(fields['error'],'cancelled_before_dispatch');self.assertIsNone(fields['request_start'])

    async def test_unconfirmed_request_or_response_ack_poison_and_prevent_publication(self):
        for failed_kind in ('control','rest_response'):
            with self.subTest(kind=failed_kind):
                lane,owner=await self.discovery();original=lane.submit_received
                def unconfirmed(record):
                    durable=original(record)
                    if record.kind!=failed_kind:return durable
                    async def fail():
                        await durable;raise OSError('ack lost')
                    return asyncio.create_task(fail())
                http=HTTP([Response(response_body())])
                with patch.object(lane,'submit_received',side_effect=unconfirmed):
                    with self.assertRaisesRegex(OSError,'ack lost'):await owner.fetch(http)
                self.assertTrue(owner.poisoned);self.assertIsNone(owner.latest)
                with self.assertRaisesRegex(ValueError,'reopen'):await owner.fetch(http)
                if failed_kind=='control':self.assertEqual(http.calls,[])

    async def test_identity_duplicates_and_count_bounds_do_not_publish_subset(self):
        lane,owner=await self.discovery(max_instruments=1)
        common=dict(instrument_id=1,creation_timestamp=1000,base_currency='BTC',kind='option',instrument_name='X')
        with self.assertRaisesRegex(ValueError,'count bound'):
            await owner.fetch(HTTP([Response(response_body([common,dict(common,instrument_name='Y')]))]))
        self.assertIsNone(owner.latest)
        self.advance(owner)
        with self.assertRaisesRegex(ValueError,'native creation'):
            invalid=response_body([dict(common,creation_timestamp=None)])
            await owner.fetch(HTTP([Response(invalid)]))
        self.assertIsNone(owner.latest)
        await self.finish(lane)
        record=self.raw(lane)[1]
        body=response_body([common,common])
        with self.assertRaisesRegex(ValueError,'duplicate'):
            project_instruments(replace(record,body=body),max_instruments=10)

    async def test_cap_exhaustion_marks_previous_universe_stale(self):
        lane,owner=await self.discovery(max_requests=1)
        previous=await owner.fetch(HTTP([Response(response_body())]))
        with self.assertRaisesRegex(ValueError,'request bound'):await owner.fetch(HTTP([]))
        self.assertEqual(owner.latest,previous);self.assertTrue(owner.health()['stale'])

    async def test_shared_quota_is_granted_after_durable_intent_not_before(self):
        lane,owner=await self.discovery();entered=asyncio.Event();original=lane.submit_received
        def delayed(record):
            durable=original(record)
            if record.kind!='control':return durable
            async def ack():
                saved=await durable
                self.mono[0]+=2_000_000_000;owner.quota.notify()
                await owner.quota.acquire(owner.charges,priority=Priority.LIVE,deadline_ns=self.mono[0]+1_000_000_000)
                entered.set();return saved
            return asyncio.create_task(ack())
        http=HTTP([Response(response_body())])
        with patch.object(lane,'submit_received',side_effect=delayed):
            task=asyncio.create_task(owner.fetch(http));await asyncio.wait_for(entered.wait(),5)
            await asyncio.sleep(0);await asyncio.sleep(0);self.assertEqual(http.calls,[])
            self.advance(owner);await asyncio.wait_for(task,5)
        self.assertEqual(owner.quota.health()['grants'],2);self.assertEqual(len(http.calls),1)

    async def test_cancellation_after_429_headers_preserves_shared_cooldown(self):
        lane,owner=await self.discovery();entered=asyncio.Event()
        response=Response(b'provider error',429,[(b'Retry-After',b'2')])
        async def blocked(size):entered.set();await asyncio.Event().wait()
        response.read=blocked;task=asyncio.create_task(owner.fetch(HTTP([response])))
        await asyncio.wait_for(entered.wait(),5);task.cancel()
        with self.assertRaises(asyncio.CancelledError):await task
        self.assertGreater(owner.quota.pools['get_instruments'].cooldown_ns,self.mono[0])
        self.assertGreater(owner.quota.pools['rest'].cooldown_ns,self.mono[0])
        await self.finish(lane);fields=strict_json(self.raw(lane)[1].native_fields_json)
        self.assertEqual(fields['status'],429);self.assertFalse(fields['complete'])

    async def test_unknown_429_cooldown_stops_rest_pool_instead_of_guessing(self):
        lane,owner=await self.discovery()
        with self.assertRaises(ValueError):await owner.fetch(HTTP([Response(b'error',429)]))
        self.assertTrue(owner.quota.closed)
        await self.finish(lane);self.assertEqual(self.raw(lane)[1].body,b'error')

    async def test_real_transport_does_not_retry_disconnected_get(self):
        lane,owner=await self.discovery();requests=[]
        async def disconnect(reader,writer):
            try:
                await reader.readuntil(b'\r\n\r\n');requests.append(1)
            finally:
                writer.close();await writer.wait_closed()
        server=await asyncio.start_server(disconnect,'127.0.0.1',0)
        self.addAsyncCleanup(server.wait_closed);self.addCleanup(server.close)
        owner.endpoint='http://127.0.0.1:'+str(server.sockets[0].getsockname()[1])+'/api/v2/public/get_instruments'
        async with discovery_session() as session:
            with self.assertRaisesRegex(ValueError,'acquisition unsuccessful'):await owner.fetch(session)
        self.assertEqual(len(requests),1);self.assertEqual(owner.quota.health()['grants'],1)
        await self.finish(lane);self.assertEqual(strict_json(self.raw(lane)[1].native_fields_json)['error'],'ServerDisconnectedError')

    async def test_real_transport_overrides_status_errors_and_retry_middleware(self):
        lane,owner=await self.discovery()
        async def limited(request):return web.Response(status=429,body=b'exact rate error',headers={'Retry-After':'2'})
        app=web.Application();app.router.add_get('/instruments',limited)
        runner=web.AppRunner(app);await runner.setup();self.addAsyncCleanup(runner.cleanup)
        site=web.TCPSite(runner,'127.0.0.1',0);await site.start()
        owner.endpoint='http://127.0.0.1:'+str(site._server.sockets[0].getsockname()[1])+'/instruments'
        async def forbidden(request,handler):raise AssertionError('middleware must not run')
        async with discovery_session() as session:
            session._raise_for_status=True;session._middlewares=(forbidden,)
            with self.assertRaisesRegex(ValueError,'acquisition unsuccessful'):await owner.fetch(session)
        await self.finish(lane);self.assertEqual(self.raw(lane)[1].body,b'exact rate error')
        self.assertGreater(owner.quota.pools['get_instruments'].cooldown_ns,self.mono[0])

    async def test_default_retry_enabled_session_rejected_before_any_request(self):
        lane,owner=await self.discovery()
        async with aiohttp.ClientSession() as session:
            with self.assertRaisesRegex(ValueError,'no-retry'):await owner.fetch(session)
        self.assertEqual(owner.requests,0);self.assertEqual(owner.quota.health()['grants'],0)

    async def test_unsupported_encoding_and_unbounded_configuration_fail_closed(self):
        lane,owner=await self.discovery()
        with self.assertRaisesRegex(ValueError,'encoded discovery'):
            await owner.fetch(HTTP([Response(response_body(),headers=[(b'Content-Encoding',b'gzip')])]))
        with self.assertRaisesRegex(ValueError,'archive bounds'):
            DeribitDiscovery(lane,owner.quota,charges=owner.charges,max_response_bytes=1024*1024)
        with self.assertRaisesRegex(ValueError,'one request per second'):
            DeribitDiscovery(lane,owner.quota,charges={'rest':1},max_response_bytes=4096)
