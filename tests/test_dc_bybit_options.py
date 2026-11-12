"""Bybit option capture fixtures: sharded topics on a changing universe."""
import asyncio
from dataclasses import replace
import json
import hashlib
from pathlib import Path
import time
from types import SimpleNamespace
import unittest
from unittest.mock import AsyncMock,patch

import aiohttp

from data_collector import bybit_options
from data_collector.app import run,verify_run
from data_collector.bybit_options import CAP_CHARS,TRADES,Shard,ticker
from data_collector.config import DiscoveryConfig,load
from data_collector.contracts import canonical,strict_json
from data_collector.rawrun import RawOwner
from data_collector.segments import iter_segment_records
from tests.helpers import AppHarness, Response


def row(symbol,identity,*,status='Trading',expiry=None,launch=None):
    now=time.time_ns()//1_000_000
    return dict(symbolId=identity,symbol=symbol,status=status,baseCoin=symbol.split('-')[0],quoteCoin='USDT',
        settleCoin='USDT',optionsType='Call' if '-C-' in symbol else 'Put',launchTime=str(launch or now-86400000),
        deliveryTime=str(expiry or now+86400000),priceFilter=dict(tickSize='5'),lotSizeFilter=dict(qtyStep='0.01'))


class Socket:
    def __init__(self,venue):
        self.venue=venue;self.incoming=asyncio.Queue();self.sent=[];self.closed=False;self.topics=set()
    def put(self,value):
        body=value if isinstance(value,bytes) else canonical(value)
        self.incoming.put_nowait(aiohttp.WSMessage(aiohttp.WSMsgType.TEXT,body.decode(),None))
    async def send_str(self,body):
        value=strict_json(body.encode());self.sent.append(value);self.venue.requests.append(value)
        if self.venue.send_hook is not None and await self.venue.send_hook(self,value):return
        if value['op']=='ping':
            self.put(dict(op='pong',args=[str(time.time_ns()//1_000_000)]));return
        topics=value['args']
        if value['op']=='subscribe':
            for topic in topics:
                if topic.startswith('tickers.') and self.venue.push:
                    self.put(dict(topic=topic,ts=1,type='snapshot',id=topic+'-1',data=dict(symbol=topic[8:],bidPrice='0')))
            self.topics.update(topics)
        else:self.topics.difference_update(topics)
        fail=[t for t in topics if t in self.venue.reject]
        self.put(dict(success=True,conn_id='c',type='COMMAND_RESP',
            data=dict(failTopics=fail,successTopics=[t for t in topics if t not in fail])))
        if fail:self.put(dict(topic='publicTrade.BTC',ts=1,type='snapshot',data=[]))  # frame after the ACK
    async def receive(self):return await self.incoming.get()
    async def close(self):self.closed=True
    def drop(self):self.put_close()
    def put_close(self):self.incoming.put_nowait(aiohttp.WSMessage(aiohttp.WSMsgType.CLOSED,None,None))


class Venue:
    """Fake public session: paginated instruments-info and option sockets."""
    _retry_connection=False
    def __init__(self):
        self.rows={'BTC':[],'ETH':[]};self.sockets=[];self.requests=[];self.reject=set();self.push=True
        self.page_size=2;self.fail_next=0;self.urls=[];self.send_hook=None
    async def __aenter__(self):return self
    async def __aexit__(self,*args):pass
    async def ws_connect(self,url,**kwargs):
        self.ws_url=url;self.ws_options=kwargs;socket=Socket(self);self.sockets.append(socket);return socket
    def get(self,url,**kwargs):
        url=str(url);self.urls.append(url)
        if self.fail_next:
            self.fail_next-=1;return Response(b'{"retCode":10006,"retMsg":"busy","result":{}}')
        query=dict(part.split('=',1) for part in url.split('?',1)[1].split('&'))
        rows=self.rows[query['baseCoin']];start=int(query.get('cursor','0'))
        page=rows[start:start+self.page_size];more=start+self.page_size<len(rows)
        return Response(canonical(dict(retCode=0,retMsg='OK',result=dict(category='option',
            nextPageCursor=str(start+self.page_size) if more else '',list=page),retExtInfo={},time=1)))
    def subscribed(self):
        return set().union(*(s.topics for s in self.sockets if not s.closed)) if self.sockets else set()


class BybitOptionTests(AppHarness, unittest.IsolatedAsyncioTestCase):
    async def clock_wait(self,deadline):await asyncio.Event().wait()

    def setup_live(self,**overrides):
        values=dict(venue='bybit',environment='production',capture_mode='raw',all_options=True,
            rediscover_options=True,raw_processes=1,raw_feeds=('ticker','trades'),bybit_rest_host='api.bytick.com',bybit_ws_host='stream.bytick.com',
            discovery_interval_seconds=1,shard_max_args=3,subscription_batch_size=3)
        values.update(overrides)
        self.config=DiscoveryConfig(self.root,**values)
        self.venue=Venue()
        self.venue.rows['BTC']=[row('BTC-1NOV26-60000-C-USDT',1),row('BTC-1NOV26-60000-P-USDT',2),
            row('BTC-1NOV26-70000-C-USDT',3)]
        self.venue.rows['ETH']=[row('ETH-1NOV26-2000-C-USDT',11)]
        for name,value in (('aligned',AsyncMock()),):
            patcher=patch.object(RawOwner,name,new=value);patcher.start();self.addCleanup(patcher.stop)
        patcher=patch.object(bybit_options,'CONNECT_SPACING_SECONDS',0);patcher.start();self.addCleanup(patcher.stop)

    async def wait_for(self,predicate,seconds=10):
        async with asyncio.timeout(seconds):
            while not predicate():await asyncio.sleep(.02)

    async def start_live(self):
        task=asyncio.create_task(run(self.config,session_factory=lambda:self.venue,observer=self.observe,
            clock_read=self.clock_read,clock_wait=self.clock_wait))
        self.addAsyncCleanup(self.cleanup_task,task)
        await self.wait_for(lambda:task.done() or bool(list(self.root.glob('runs/*/ready.json'))))
        if task.done():self.fail(str(task.result()))
        return task

    async def cleanup_task(self,task):
        if not task.done():task.cancel()
        await asyncio.gather(task,return_exceptions=True)

    async def finish(self,task):
        await asyncio.sleep(.3);task.cancel();return await asyncio.wait_for(task,10)

    def records(self,result,metadata=False):
        root=Path(result['root']);descriptor=result
        if metadata:root=root/'metadata';descriptor=result['metadata']
        return [r for name in descriptor['segments'] for r in iter_segment_records(root/name)]

    def tickers(self):
        return {t for t in self.venue.subscribed() if t.startswith('tickers.')}

    async def test_sharded_capture_follows_prelaunch_listing_and_verifies(self):
        self.setup_live();task=await self.start_live()
        self.assertEqual(self.venue.ws_url,'wss://stream.bytick.com/v5/public/option')
        self.assertEqual(self.venue.ws_options['compress'],15)
        self.assertTrue(all(u.startswith('https://api.bytick.com/v5/market/instruments-info?category=option') for u in self.venue.urls))
        self.assertIn('cursor=2',' '.join(self.venue.urls))  # BTC needs a second page
        self.assertGreaterEqual(len(self.venue.sockets),2)
        self.assertEqual(self.tickers(),{ticker(r['symbol']) for b in ('BTC','ETH') for r in self.venue.rows[b]})
        self.assertEqual(set(TRADES)<=self.venue.subscribed(),True)
        self.venue.rows['ETH'].append(row('ETH-2NOV26-2100-P-USDT',12,status='PreLaunch'))
        await self.wait_for(lambda:'tickers.ETH-2NOV26-2100-P-USDT' in self.tickers())
        seen=len(self.venue.urls);await self.wait_for(lambda:len(self.venue.urls)>=seen+3)  # one unchanged poll
        result=await self.finish(task)
        self.assertEqual(result['status'],'raw_capture_complete',result)
        for shard in result['shards']:self.assertLessEqual(shard['args'],3)
        verified=verify_run(result['root'])
        self.assertTrue(verified['changing_universe_coverage_verified'],verified)
        self.assertEqual(verified['final_options'],5);self.assertGreaterEqual(verified['universe_versions_verified'],2)
        evidence=[row for r in self.records(result) if r.channel=='option_subscription'
            for row in strict_json(r.body)['options']]
        new=next(e for e in evidence if e['symbol']=='ETH-2NOV26-2100-P-USDT')
        self.assertTrue(new['first_ack']);self.assertTrue(new['deadline_met'])
        self.assertLessEqual(new['observed_monotonic_ns'],new['request_monotonic_ns'])
        self.assertLessEqual(new['request_monotonic_ns'],new['ack_monotonic_ns'])
        revisions=[strict_json(r.body)['revision'] for r in self.records(result,metadata=True) if r.channel=='option_universe']
        self.assertTrue(any('ETH-2NOV26-2100-P-USDT' in v['added'] for v in revisions))
        # Unchanged polls archive raw HTTP but add no universe version.
        self.assertGreater(result['polls'],len(revisions))
        raw=[r for r in self.records(result) if r.kind=='ws_message']
        self.assertTrue(all(r.body==canonical(json.loads(r.body)) for r in raw))

    async def test_expiry_retires_and_unsubscribes_on_open_socket(self):
        self.setup_live();expiry=time.time_ns()//1_000_000+3000
        self.venue.rows['BTC'][0]=row('BTC-1NOV26-60000-C-USDT',1,expiry=expiry)
        task=await self.start_live()
        await self.wait_for(lambda:any(v['op']=='unsubscribe' for v in self.venue.requests),seconds=8)
        await self.wait_for(lambda:'tickers.BTC-1NOV26-60000-C-USDT' not in self.tickers())
        result=await self.finish(task)
        self.assertEqual(result['status'],'raw_capture_complete',result)
        self.assertEqual(result['retired_options'],1)
        self.assertEqual(len(self.venue.sockets),len(result['shards']))
        self.assertTrue(verify_run(result['root'])['changing_universe_coverage_verified'])

    async def test_rejected_subscription_fails_run_with_raw_preserved(self):
        self.setup_live();self.venue.reject={'tickers.ETH-1NOV26-2000-C-USDT'}
        result=await asyncio.wait_for(run(self.config,session_factory=lambda:self.venue,observer=self.observe,
            clock_read=self.clock_read,clock_wait=self.clock_wait),10)
        self.assertEqual(result['status'],'failed');self.assertEqual(result['rejected_subscriptions'],1)
        self.assertIn('rejected',result['error'])
        self.assertTrue(any(b'failTopics":["tickers.ETH' in r.body for r in self.records(result) if r.kind=='ws_message'))
        checked=verify_run(result['root'])  # counters include frames admitted after the rejection
        self.assertTrue(checked['archive_verified']);self.assertFalse(checked['changing_universe_coverage_verified'])

    async def test_disconnect_reconnects_shard_and_resubscribes(self):
        self.setup_live();task=await self.start_live()
        first=self.venue.sockets[0];count=len(self.venue.sockets);first.put_close()
        await self.wait_for(lambda:len(self.venue.sockets)>count and len(self.venue.sockets[-1].topics)==len(first.topics))
        result=await self.finish(task)
        self.assertEqual(result['status'],'raw_capture_complete',result)
        self.assertEqual(result['connections'],count+1)
        self.assertTrue(verify_run(result['root'])['changing_universe_coverage_verified'])
        evidence=[row for r in self.records(result) if r.channel=='option_subscription' for row in strict_json(r.body)['options']]
        self.assertTrue(any(not e['first_ack'] and e['deadline_met'] is None for e in evidence))

    async def test_failed_poll_keeps_members_and_next_poll_recovers(self):
        self.setup_live();task=await self.start_live()
        self.venue.fail_next=1;self.venue.rows['BTC'].append(row('BTC-1NOV26-80000-C-USDT',4))
        await self.wait_for(lambda:'tickers.BTC-1NOV26-80000-C-USDT' in self.tickers(),seconds=8)
        result=await self.finish(task)
        self.assertEqual(result['status'],'raw_capture_complete',result)
        self.assertEqual(result['failed_polls'],1)
        self.assertTrue(verify_run(result['root'])['changing_universe_coverage_verified'])

    async def test_ack_without_first_push_never_initializes(self):
        self.setup_live(bootstrap_timeout_seconds=1);self.venue.push=False
        result=await asyncio.wait_for(run(self.config,session_factory=lambda:self.venue,observer=self.observe,
            clock_read=self.clock_read,clock_wait=self.clock_wait),10)
        self.assertEqual(result['status'],'failed');self.assertIn('bootstrap',result['error'])

    async def test_verifier_rejects_tampered_poll_classification(self):
        self.setup_live();task=await self.start_live();result=await self.finish(task)
        report=strict_json((Path(result['root'])/'result.json').read_bytes())
        report['universe_versions']+=1
        (Path(result['root'])/'result.json').write_bytes(canonical(report))
        with self.assertRaises(ValueError):verify_run(result['root'])

    def claim_complete(self,result):
        path=Path(result['root'])/'result.json';value=strict_json(path.read_bytes())
        value['status']='raw_capture_complete';path.write_bytes(canonical(value))

    async def test_late_first_request_fails_capture_and_independent_verifier(self):
        self.setup_live();task=await self.start_live();save=RawOwner.save;late='ETH-2NOV26-2100-P-USDT';shifted=False
        async def delayed(owner,body,channel,kind,**kwargs):
            nonlocal shifted
            if channel=='subscribe' and late.encode() in body and not shifted:
                self.offset+=121_000_000_000;shifted=True
            return await save(owner,body,channel,kind,**kwargs)
        with patch.object(RawOwner,'save',new=delayed):
            self.venue.rows['ETH'].append(row(late,12,status='PreLaunch'))
            await self.wait_for(lambda:ticker(late) in self.venue.subscribed())
            result=await self.finish(task)
        self.assertEqual(result['status'],'failed',result);self.assertEqual(result['late_first_acks'],1)
        self.assertTrue(verify_run(result['root'])['archive_verified'])
        self.claim_complete(result)
        with self.assertRaisesRegex(ValueError,'exceeded 120 seconds'):verify_run(result['root'])

    async def test_option_seen_near_stop_still_requires_first_push(self):
        self.setup_live();task=await self.start_live();self.venue.push=False
        late='ETH-2NOV26-2100-P-USDT';self.venue.rows['ETH'].append(row(late,12,status='PreLaunch'))
        await self.wait_for(lambda:ticker(late) in self.venue.subscribed())
        result=await self.finish(task)
        self.assertEqual(result['status'],'failed',result)
        self.assertIn('ACK and first push',result['error']);self.claim_complete(result)
        with self.assertRaisesRegex(ValueError,'ACK plus first push'):verify_run(result['root'])

    async def test_option_seen_near_stop_still_requires_ack(self):
        self.setup_live();task=await self.start_live();late='ETH-2NOV26-2100-P-USDT';requested=asyncio.Event()
        async def hold(socket,value):
            if value['op']=='subscribe' and ticker(late) in value['args']:
                requested.set();return True
            return False
        self.venue.send_hook=hold;self.venue.rows['ETH'].append(row(late,12,status='PreLaunch'))
        await asyncio.wait_for(requested.wait(),5);result=await self.finish(task)
        self.assertEqual(result['status'],'failed',result);self.claim_complete(result)
        with self.assertRaisesRegex(ValueError,'ACK plus first push'):verify_run(result['root'])

    async def test_retirement_does_not_erase_uncovered_short_lived_option(self):
        self.setup_live();task=await self.start_live();self.venue.push=False
        late='ETH-2NOV26-2100-P-USDT'
        self.venue.rows['ETH'].append(row(late,12,expiry=time.time_ns()//1_000_000+5000))
        await self.wait_for(lambda:ticker(late) in self.venue.subscribed())
        self.offset+=10_000_000_000
        await self.wait_for(lambda:any(v['op']=='unsubscribe' and ticker(late) in v['args'] for v in self.venue.requests))
        result=await self.finish(task)
        self.assertEqual(result['status'],'failed',result)
        self.assertIn('before retirement',result['error']);self.claim_complete(result)
        with self.assertRaisesRegex(ValueError,'before retirement'):verify_run(result['root'])

    async def test_reconnect_after_120_seconds_does_not_repeat_listing_deadline(self):
        self.setup_live();task=await self.start_live();count=len(self.venue.sockets)
        self.offset+=121_000_000_000;self.venue.sockets[0].put_close()
        await self.wait_for(lambda:len(self.venue.sockets)>count and bool(self.venue.sockets[-1].topics))
        result=await self.finish(task)
        self.assertEqual(result['status'],'raw_capture_complete',result)
        self.assertEqual(result['late_first_acks'],0)
        self.assertTrue(verify_run(result['root'])['changing_universe_coverage_verified'])

    async def test_stop_during_reconnect_retains_prior_coverage_and_explicit_gap(self):
        self.setup_live();task=await self.start_live();count=len(self.venue.sockets)
        async def offline(*args,**kwargs):raise OSError('network disconnected')
        self.venue.ws_connect=offline
        for socket in self.venue.sockets:socket.put_close()
        await asyncio.sleep(.1);result=await self.finish(task)
        self.assertEqual(result['status'],'raw_capture_complete',result)
        self.assertEqual(result['connections'],count)
        self.assertTrue(verify_run(result['root'])['changing_universe_coverage_verified'])

    async def test_stalled_close_is_bounded_and_other_shard_keeps_recording(self):
        self.setup_live(timeout_seconds=1);task=await self.start_live();count=len(self.venue.sockets)
        first,healthy=self.venue.sockets[:2];closing=asyncio.Event();aborted=asyncio.Event()
        async def stalled():closing.set();await asyncio.Event().wait()
        first.close=stalled;first._response=SimpleNamespace(close=aborted.set)
        first.put_close();await asyncio.wait_for(closing.wait(),3)
        topic=next(t for t in healthy.topics if t.startswith('tickers.'))
        healthy.put(dict(topic=topic,ts=12345,type='snapshot',data=dict(symbol=topic[8:],test_marker='healthy-during-close')))
        await self.wait_for(healthy.incoming.empty)
        await asyncio.wait_for(aborted.wait(),3)
        await self.wait_for(lambda:len(self.venue.sockets)>count and bool(self.venue.sockets[-1].topics))
        result=await self.finish(task)
        self.assertEqual(result['status'],'raw_capture_complete',result)
        self.assertTrue(any(b'healthy-during-close' in r.body for r in self.records(result)))
        self.assertTrue(verify_run(result['root'])['changing_universe_coverage_verified'])


class BybitConfigTests(unittest.TestCase):
    def test_recorded_option_protocol_and_native_metadata(self):
        fixture=json.loads((Path(__file__).parent/'fixtures/dc_bybit_options.json').read_text())
        for kind,item in fixture['samples'].items():
            body=item['body'].encode()
            self.assertEqual(hashlib.sha256(body).hexdigest(),item['sha256'])
            self.assertEqual(bybit_options.route(strict_json(body))[0],kind)
        from data_collector.option_universe import bybit_options as project
        for item in fixture['metadata']:
            page=dict(retCode=0,result=dict(category='option',list=[item['row']],nextPageCursor=''))
            rows=project([(bybit_options.page_params(item['base']),page)],base=item['base'])
            self.assertEqual(rows[0].native,item['row'])

    def test_shipped_config_uses_alternative_hosts(self):
        config=load(Path(__file__).parents[1]/'config'/'v2-bybit-options.toml')
        self.assertEqual((config.bybit_rest_host,config.bybit_ws_host),('api.bytick.com','stream.bytick.com'))
        self.assertLessEqual(config.shard_arg_chars,CAP_CHARS)

    def test_defaults_and_bounds(self):
        base=dict(venue='bybit',all_options=True,rediscover_options=True,raw_processes=1,raw_feeds=('ticker','trades'))
        config=DiscoveryConfig(Path('/tmp/x'),'production',**base)
        self.assertEqual((config.bybit_rest_host,config.bybit_ws_host),('api.bybit.com','stream.bybit.com'))
        for bad in (dict(bybit_ws_host='stream.bytick.com/v5'),dict(shard_arg_chars=21001),dict(shard_max_args=2001),
                    dict(raw_processes=2),dict(discovery_interval_seconds=121)):
            with self.assertRaises(ValueError):DiscoveryConfig(Path('/tmp/x'),'production',**dict(base,**bad))
        with self.assertRaises(ValueError):DiscoveryConfig(Path('/tmp/x'),'testnet',**base)
        with self.assertRaises(ValueError):DiscoveryConfig(Path('/tmp/x'),'production',**dict(base,raw_feeds=('book',)))

    def test_shard_respects_documented_caps(self):
        shard=Shard(0,TRADES)
        self.assertFalse(shard.fits('BTC-X-1-C-USDT',CAP_CHARS+5000,5000) and shard.load+30>CAP_CHARS)
        shard=Shard(1);name='BTC-27DEC26-100000-C-USDT';count=0
        while shard.fits(name+str(count),CAP_CHARS,5000):shard.assign(name+str(count),('bybit',name,'x'));count+=1
        self.assertLessEqual(len(canonical([ticker(n) for n in shard.assigned])),CAP_CHARS)
        shard=Shard(2)
        for i in range(2000):shard.assign(f'B{i}',None)
        self.assertFalse(shard.fits('B',CAP_CHARS*10,5000))


if __name__=='__main__':unittest.main()
