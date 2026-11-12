import asyncio
import threading
import unittest
from unittest.mock import patch

from data_collector.clocksource import ChronyClockSampler
from data_collector.runtime import DeribitCapture, CaptureStopped
from data_collector.contracts import strict_json
from data_collector.snapshots import publish_snapshot, read_snapshot
from tests import test_dc_deribit as base
from tests.helpers import tracking

C=60_000_000_000


class RuntimeTests(unittest.IsolatedAsyncioTestCase):
    setUp=base.DeribitTests.setUp
    lane=base.DeribitTests.lane
    start=base.DeribitTests.start
    record=base.DeribitTests.record
    received=base.DeribitTests.received
    clock_request=base.DeribitTests.clock_request
    clock_policy=base.DeribitTests.clock_policy
    ready_cutoffs=base.DeribitTests.ready_cutoffs
    ready=base.DeribitTests.ready
    records=base.DeribitTests.records
    controls=base.DeribitTests.controls
    schedule=base.DeribitTests.schedule
    finish_raw=base.DeribitTests.finish_raw
    receiver=base.DeribitTests.receiver
    frame=base.DeribitTests.frame
    until=base.DeribitTests.until

    def observe(self):
        utc,mono,_=base.DeribitTests.observe(self)
        return utc,mono,'boot'

    async def measured_wait(self,deadline):self.mono[0]=max(self.mono[0],deadline)
    def pipeline(self,lane,**kwargs):
        async def wait(deadline):
            await self.until(lambda:self.active._acked and self.active.models[0].state=='live')
            await self.measured_wait(deadline)
        return base.DeribitTests.pipeline(self,lane,first_cutoff_utc_ns=2*C,wait_until=wait,**kwargs)

    async def components(self,*,unhealthy=0,max_samples=20,read_error=None,warmup_timeout_seconds=180):
        self.mono[0]=1000
        lane=await self.ready();receiver,pipeline=self.receiver(lane,max_frames=8,max_seconds=180,idle_timeout_seconds=180)
        self.active=receiver;self.mono[0]+=1;calls=[]
        async def read(*args):
            calls.append(1)
            if read_error is not None:raise read_error
            if len(calls)<=unhealthy:
                self.assertIsNone(self.http.options)
                return 1,b'not synchronized',b''
            return 0,tracking(self.observe()[0]),b''
        async def wait(deadline):
            if len(calls)<unhealthy+3:
                self.mono[0]=deadline;await asyncio.sleep(0)
            else:await asyncio.Event().wait()
        sampler=ChronyClockSampler(lane,read=read,wait_until=wait,max_samples=max_samples)
        capture=DeribitCapture(sampler,receiver,pipeline,warmup_timeout_seconds=warmup_timeout_seconds)
        self.addCleanup(capture.stop)
        return lane,capture,calls

    def launch(self,capture):
        task=asyncio.create_task(capture.run(self.http))
        async def cleanup():
            capture.stop();await asyncio.wait_for(asyncio.gather(task,return_exceptions=True),5)
        self.addAsyncCleanup(cleanup)
        return task

    async def test_warmup_then_owner_cutover_capture_publish_report_and_ordered_drain(self):
        lane,capture,calls=await self.components(unhealthy=1)
        task=self.launch(capture);self.socket.put(self.frame(capture.receiver))
        result=await asyncio.wait_for(task,5)
        self.assertGreaterEqual(len(calls),4)
        self.assertTrue(result['terminal']);self.assertEqual(result['pipeline']['published'],1)
        self.assertTrue(self.socket.closed);self.assertTrue(capture.sampler._terminal)
        self.assertTrue(lane.health()['running']);self.assertTrue(lane.health()['accepting'])
        await self.finish_raw(lane)
        raw=self.records(lane)
        owner=next(i for i,r in enumerate(raw) if r.channel=='owner_plans:capture')
        allocation=next(i for i,r in enumerate(raw) if r.channel=='cutoffs:capture' and strict_json(r.body).get('kind')=='allocated')
        self.assertLess(owner,allocation)
        reports=[strict_json(r.body) for r in raw if r.channel=='snapshot_deliveries:capture']
        self.assertEqual(len(reports),1)
        artifact=read_snapshot(capture.pipeline.publisher.root/'snapshots',reports[0]['snapshot_id'],lane.segments)
        book=strict_json(artifact['payload'])['books'][0]
        self.assertEqual(book['validity_as_published'],'valid')
        self.assertEqual(book['target']['owner_epoch'],capture.receiver.plan.bindings[0].target.owner_epoch)
        self.assertEqual(book['book']['bids'],[['100','9']])
        with self.assertRaisesRegex(CaptureStopped,'once'):await capture.run(self.http)

    async def test_exhausted_unaligned_clock_never_opens_socket_and_closes_publisher(self):
        lane,capture,_=await self.components(unhealthy=5,max_samples=2)
        with self.assertRaisesRegex(CaptureStopped,'clock source ended'):await self.launch(capture)
        self.assertIsNone(self.http.options);self.assertFalse(capture.receiver._started)
        self.assertFalse(capture.pipeline.publisher.health()['accepting'])
        self.assertTrue(capture.sampler._terminal);self.assertTrue(lane.health()['accepting'])

    async def test_clock_task_failure_prevents_receiver_start_and_drains(self):
        lane,capture,_=await self.components(read_error=RuntimeError('provider crashed'))
        with self.assertRaisesRegex(RuntimeError,'provider crashed'):await self.launch(capture)
        self.assertIsNone(self.http.options);self.assertTrue(capture.sampler._terminal)
        self.assertFalse(lane.health()['accepting']);self.assertTrue(capture._terminal)

    async def test_receiver_connect_failure_stops_clock_and_publication_tasks(self):
        lane,capture,_=await self.components()
        async def connect(*args,**kwargs):raise OSError('connect failed')
        with patch.object(self.http,'ws_connect',side_effect=connect):
            with self.assertRaisesRegex(OSError,'connect failed'):await self.launch(capture)
        self.assertTrue(capture.receiver.health()['terminal']);self.assertTrue(capture.sampler._terminal)
        self.assertFalse(capture.pipeline.publisher.health()['accepting'])
        self.assertFalse(lane.health()['accepting'])

    async def test_stop_or_cancel_during_publication_drains_report_before_socket(self):
        for cancel in (False,True):
            with self.subTest(cancel=cancel):
                lane,capture,_=await self.components()
                entered,release=threading.Event(),threading.Event();self.addCleanup(release.set)
                def blocked(*args,**kwargs):
                    entered.set()
                    if not release.wait(3):raise OSError('test publisher wait')
                    return publish_snapshot(*args,**kwargs)
                with patch('data_collector.publisher.publish_snapshot',side_effect=blocked):
                    task=self.launch(capture);self.socket.put(self.frame(capture.receiver))
                    self.assertTrue(await asyncio.to_thread(entered.wait,2))
                    if cancel:task.cancel()
                    else:capture.stop()
                    await asyncio.sleep(0);await asyncio.sleep(0)
                    self.assertFalse(task.done());self.assertFalse(self.socket.closed)
                    release.set()
                    if cancel:
                        with self.assertRaises(asyncio.CancelledError):await task
                    else:self.assertTrue((await task)['terminal'])
                self.assertTrue(self.socket.closed);self.assertTrue(capture.sampler._terminal)
                self.assertEqual(capture.pipeline.health()['reported'],1)
                await self.finish_raw(lane)
                self.assertEqual(len([r for r in self.records(lane) if r.channel=='snapshot_deliveries:capture']),1)

    async def test_stop_during_admitted_owner_cutover_never_connects(self):
        lane,capture,_=await self.components()
        entered,release=asyncio.Event(),asyncio.Event();self.addCleanup(release.set)
        original=lane.submit_owner_plan
        def delayed(*args,**kwargs):
            durable=original(*args,**kwargs)
            async def acknowledgement():
                ack=await durable;entered.set();await release.wait();return ack
            return asyncio.create_task(acknowledgement())
        with patch.object(lane,'submit_owner_plan',side_effect=delayed):
            task=self.launch(capture);await asyncio.wait_for(entered.wait(),5)
            capture.stop();await asyncio.sleep(0);await asyncio.sleep(0)
            self.assertFalse(task.done());release.set()
            result=await asyncio.wait_for(task,5)
        self.assertTrue(result['terminal']);self.assertIsNone(self.http.options)
        self.assertTrue(lane.health()['accepting']);self.assertTrue(capture.receiver._terminal)

    async def test_stop_after_owner_ack_still_propagates_end_cleanup_failure(self):
        lane,capture,_=await self.components()
        original=capture.receiver.schedule.replace_plan
        async def selected(plan):
            result=await original(plan);capture.stop();return result
        with patch.object(capture.receiver.schedule,'replace_plan',side_effect=selected), \
                patch.object(lane,'submit_deribit_end',side_effect=OSError('end cleanup failed')):
            with self.assertRaisesRegex(OSError,'end cleanup failed'):await self.launch(capture)
        self.assertIsNone(self.http.options);self.assertFalse(lane.health()['accepting'])
        self.assertTrue(capture.receiver._terminal);self.assertTrue(capture.sampler._terminal)

    async def test_stopped_nested_components_are_rejected_before_acquisition(self):
        for target in ('sampler','receiver','pipeline','schedule'):
            lane,capture,_=await self.components()
            owner=capture.pipeline.schedule if target=='schedule' else getattr(capture,target)
            owner.stop()
            # Remove only the outer grouping marker to examine nested freshness;
            # components remain in their actual stopped state.
            for obj in (capture.sampler,capture.receiver,capture.pipeline):del obj._capture_owner
            with self.assertRaisesRegex(ValueError,'fresh exclusive'):
                DeribitCapture(capture.sampler,capture.receiver,capture.pipeline)
            self.assertIsNone(self.http.options)
            await capture.pipeline.publisher.close()
            await self.finish_raw(lane)

    async def test_warmup_timeout_drains_clock_without_opening_socket(self):
        lane,capture,_=await self.components(unhealthy=1,warmup_timeout_seconds=1)
        async def wait(deadline):await asyncio.Event().wait()
        capture.sampler.wait_until=wait
        with self.assertRaisesRegex(TimeoutError,'clock warmup'):await self.launch(capture)
        self.assertIsNone(self.http.options);self.assertTrue(capture.sampler._terminal)
        self.assertFalse(capture._cancelled);self.assertTrue(lane.health()['accepting'])

    async def test_stop_before_run_starts_no_component(self):
        lane,capture,_=await self.components();capture.stop()
        result=await capture.run(self.http)
        self.assertTrue(result['terminal']);self.assertFalse(capture.receiver._started)
        self.assertFalse(capture.sampler._started);self.assertIsNone(self.http.options)
