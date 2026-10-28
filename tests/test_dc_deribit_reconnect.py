import asyncio
import unittest
from unittest.mock import patch

import aiohttp

from data_collector.contracts import canonical, strict_json
from data_collector.deribit import DeribitLimits, DeribitStopped
from data_collector.snapshots import read_snapshot
from tests import test_dc_deribit as base


class HTTP:
    def __init__(self):self.sockets=[]
    async def ws_connect(self,url,**kwargs):
        socket=base.Socket();self.sockets.append(socket);return socket


class ReconnectTests(unittest.IsolatedAsyncioTestCase):
    setUp=base.DeribitTests.setUp
    observe=base.DeribitTests.observe
    lane=base.DeribitTests.lane
    start=base.DeribitTests.start
    record=base.DeribitTests.record
    received=base.DeribitTests.received
    clock_request=base.DeribitTests.clock_request
    clock_policy=base.DeribitTests.clock_policy
    ready_cutoffs=base.DeribitTests.ready_cutoffs
    ready=base.DeribitTests.ready
    measured_wait=base.DeribitTests.measured_wait
    records=base.DeribitTests.records
    controls=base.DeribitTests.controls
    schedule=base.DeribitTests.schedule
    finish_raw=base.DeribitTests.finish_raw
    receiver=base.DeribitTests.receiver
    frame=base.DeribitTests.frame
    until=base.DeribitTests.until

    def pipeline(self,lane,**kwargs):
        return base.DeribitTests.pipeline(self,lane,**dict(getattr(self,'_pipeline_kwargs',{}),**kwargs))

    async def running(self,pipeline_kwargs=None,**limits):
        self._pipeline_kwargs=pipeline_kwargs or {}
        lane=await self.ready()
        limits.setdefault('max_attempts',2);limits.setdefault('max_frames',8)
        limits.setdefault('retry_delay_ms',1)
        receiver,pipeline=self.receiver(lane,**limits);http=HTTP()
        task=asyncio.create_task(receiver.run(http))
        async def cleanup():
            receiver.stop();await asyncio.wait_for(asyncio.gather(task,return_exceptions=True),5)
        self.addAsyncCleanup(cleanup)
        await self.until(lambda:receiver._acked)
        return lane,receiver,pipeline,http,task

    def close_socket(self,socket):
        socket.incoming.put_nowait(aiohttp.WSMessage(aiohttp.WSMsgType.CLOSED,None,''))

    async def test_transport_reconnect_uses_fresh_owner_and_anchor_and_same_scheduler(self):
        lane,receiver,pipeline,http,task=await self.running()
        first=http.sockets[0];first.put(self.frame(receiver))
        await self.until(lambda:receiver.models[0].state=='live')
        old_plan=receiver.plan;old_source=receiver.source
        self.close_socket(first)
        await self.until(lambda:len(http.sockets)==2 and receiver._acked)
        self.assertTrue(first.closed);self.assertNotEqual(receiver.source,old_source)
        self.assertNotEqual(receiver.plan.bindings[0].target.owner_epoch,old_plan.bindings[0].target.owner_epoch)
        self.assertEqual(receiver.models[0].state,'bootstrapping')
        self.assertNotIn(receiver.source,receiver.healthy_streams_for_plan(old_plan))
        self.assertFalse(pipeline.schedule.health()['stopping']);self.assertTrue(lane.health()['accepting'])
        http.sockets[1].put(self.frame(receiver))
        await self.until(lambda:receiver.models[0].state=='live')
        state=await pipeline.run();self.assertEqual(state['published'],1)
        receiver.stop();result=await task
        self.assertTrue(result['terminal']);self.assertEqual(result['attempts'],2)
        self.assertTrue(http.sockets[1].closed)
        await self.finish_raw(lane)
        raw=list(self.records(lane))
        plans=[r for r in raw if r.channel=='owner_plans:capture']
        self.assertEqual(len(plans),2)
        # End proof for the old source precedes the replacement declaration.
        old_ends=[i for i,r in enumerate(raw) if r.receipt.stream_key==old_source and
            r.channel=='deribit_session' and strict_json(r.body).get('event')=='stopped']
        self.assertEqual(len(old_ends),1);self.assertLess(old_ends[0],raw.index(plans[1]))

    async def test_native_gap_reconnect_keeps_cumulative_frame_budget(self):
        lane,receiver,_,http,task=await self.running(max_frames=5)
        first=http.sockets[0];first.put(self.frame(receiver));first.put(self.frame(receiver,snapshot=False,gap=True))
        await self.until(lambda:len(http.sockets)==2)
        result=await task
        self.assertEqual(result['reason'],'session_frame_bound')
        self.assertEqual(result['frames'],5);self.assertEqual(result['attempts'],2)
        self.assertEqual(len(receiver.gaps),1);self.assertTrue(all(s.closed for s in http.sockets))
        self.assertTrue(lane.health()['accepting'])

    async def test_retry_exhaustion_is_terminal_and_preserves_both_end_records(self):
        lane,receiver,_,http,task=await self.running()
        self.close_socket(http.sockets[0]);await self.until(lambda:len(http.sockets)==2 and receiver._acked)
        self.close_socket(http.sockets[1])
        with self.assertRaisesRegex(DeribitStopped,'socket_closed_or_error'):await task
        self.assertEqual(receiver.health()['attempts'],2);self.assertFalse(lane.health()['accepting'])
        self.assertTrue(all(s.closed for s in http.sockets));self.assertTrue(receiver.schedule.health()['stopping'])

    async def test_unconfirmed_close_or_end_mapping_never_opens_replacement(self):
        for failure in ('close','mapping'):
            with self.subTest(failure=failure):
                lane,receiver,_,http,task=await self.running()
                target=http.sockets[0] if failure=='close' else lane
                name='close' if failure=='close' else 'submit_deribit_end'
                async def broken_close():raise OSError('unconfirmed close')
                effect=broken_close if failure=='close' else OSError('unconfirmed mapping')
                with patch.object(target,name,side_effect=effect):
                    self.close_socket(http.sockets[0])
                    with self.assertRaises(DeribitStopped):await task
                self.assertEqual(len(http.sockets),1);self.assertFalse(lane.health()['accepting'])

    async def test_stop_and_cancellation_during_backoff_open_no_replacement(self):
        for cancel in (False,True):
            with self.subTest(cancel=cancel):
                lane,receiver,_,http,task=await self.running(retry_delay_ms=60000)
                self.close_socket(http.sockets[0]);await self.until(lambda:receiver._retryable)
                if cancel:
                    task.cancel()
                    with self.assertRaises(asyncio.CancelledError):await task
                else:
                    receiver.stop();self.assertTrue((await task)['terminal'])
                self.assertEqual(len(http.sockets),1);self.assertTrue(http.sockets[0].closed)
                self.assertTrue(receiver.schedule.health()['stopping'])

    async def test_bytes_remain_cumulative_and_no_third_connection_after_bound(self):
        lane,receiver,_,http,task=await self.running(max_bytes=2000)
        first=http.sockets[0];first.put(self.frame(receiver));first.put(self.frame(receiver,snapshot=False,gap=True))
        await self.until(lambda:len(http.sockets)==2 and receiver._acked)
        frame=self.frame(receiver);frame['padding']='x'*1400
        # The replacement's own frames fit; the run-wide total does not.
        own_acks=sum(len(canonical(dict(jsonrpc='2.0',id=request['id'],result=(
            request['params']['channels'] if request['method']=='public/subscribe' else True))))
            for request in http.sockets[1].sent)
        self.assertLess(own_acks+len(canonical(frame)),receiver.limits.max_bytes)
        http.sockets[1].put(frame)
        with self.assertRaisesRegex(DeribitStopped,'session_byte_bound'):await task
        self.assertGreater(receiver.health()['bytes'],receiver.limits.max_bytes)
        self.assertEqual(len(http.sockets),2);self.assertFalse(lane.health()['accepting'])

    async def test_original_run_deadline_is_not_renewed_by_reconnect(self):
        lane,receiver,_,http,task=await self.running()
        self.close_socket(http.sockets[0]);await self.until(lambda:len(http.sockets)==2 and receiver._acked)
        # Fresh attempt start is later, but cannot extend the original budget.
        self.mono[0]=receiver._run_start_ns+receiver.limits.max_seconds*1_000_000_000
        http.sockets[1].put(dict(jsonrpc='2.0',id=88,result=True))
        with self.assertRaisesRegex(DeribitStopped,'session_or_bootstrap_deadline'):await task
        self.assertEqual(len(http.sockets),2);self.assertFalse(lane.health()['accepting'])

    async def test_allocated_old_plan_stays_missing_while_future_plan_publishes_fresh_book(self):
        second=asyncio.Event();release_second=asyncio.Event();third=asyncio.Event();release_third=asyncio.Event();waits=[]
        async def wait(deadline):
            index=len(waits);waits.append(deadline)
            if index==1:second.set();await release_second.wait()
            if index==2:third.set();await release_third.wait()
            await self.measured_wait(deadline)
        lane,receiver,pipeline,http,task=await self.running(
            pipeline_kwargs=dict(max_cutoffs=3,wait_until=wait),max_frames=12,
            max_seconds=300,idle_timeout_seconds=300)
        http.sockets[0].put(self.frame(receiver));await self.until(lambda:receiver.models[0].state=='live')
        old_plan=receiver.plan
        publication=asyncio.create_task(pipeline.run())
        async def cleanup_pipeline():
            release_second.set();release_third.set();pipeline.stop()
            await asyncio.wait_for(asyncio.gather(publication,return_exceptions=True),5)
        self.addAsyncCleanup(cleanup_pipeline)
        await self.until(lambda:second.is_set() and pipeline._reported==1)
        self.close_socket(http.sockets[0]);await self.until(lambda:len(http.sockets)==2 and receiver._acked)
        http.sockets[1].put(self.frame(receiver));await self.until(lambda:receiver.models[0].state=='live')
        fresh_plan=receiver.plan;self.assertNotEqual(old_plan,fresh_plan)
        release_second.set();await self.until(lambda:third.is_set() and pipeline._reported==2)
        release_third.set();state=await publication
        self.assertEqual(state['published'],3);receiver.stop();await task
        await self.finish_raw(lane)
        reports=[strict_json(r.body) for r in self.records(lane) if r.channel=='snapshot_deliveries:capture']
        books=[]
        for report in reports:
            artifact=read_snapshot(pipeline.publisher.root/'snapshots',report['snapshot_id'],lane.segments)
            books.append(strict_json(artifact['payload'])['books'][0])
        self.assertEqual(books[1]['target']['owner_epoch'],old_plan.bindings[0].target.owner_epoch)
        self.assertEqual(books[1]['validity_as_published'],'missing')
        self.assertEqual(books[2]['target']['owner_epoch'],fresh_plan.bindings[0].target.owner_epoch)
        self.assertEqual(books[2]['validity_as_published'],'valid')

    def test_retry_profile_is_bounded_and_default_remains_one_attempt(self):
        self.assertEqual(DeribitLimits().max_attempts,1)
        for values in ({'max_attempts':9},{'max_attempts':0},{'retry_delay_ms':60001}):
            with self.assertRaises(ValueError):DeribitLimits(**values)
