import asyncio
from contextlib import redirect_stderr
from dataclasses import replace
import io
import threading
import unittest
from unittest.mock import patch

from data_collector.deliveries import MAX_REPORT_BYTES,replay_delivery_reports
from data_collector.lane import DurabilityUnknown, SnapshotOrigin
from data_collector.pipeline import PipelineStopped,SnapshotPipeline
from data_collector.publisher import PublicationUnconfirmed,SnapshotPublisher,DELIVERY_NS
from data_collector.snapshots import publish_snapshot
from tests import test_dc_schedule as fixtures


class PipelineTests(unittest.IsolatedAsyncioTestCase):
    setUp=fixtures.ScheduleTests.setUp
    observe=fixtures.ScheduleTests.observe
    lane=fixtures.ScheduleTests.lane
    start=fixtures.ScheduleTests.start
    record=fixtures.ScheduleTests.record
    received=fixtures.ScheduleTests.received
    clock_request=fixtures.ScheduleTests.clock_request
    clock_policy=fixtures.ScheduleTests.clock_policy
    ready_cutoffs=fixtures.ScheduleTests.ready_cutoffs
    ready=fixtures.ScheduleTests.ready
    measured_wait=fixtures.ScheduleTests.measured_wait
    records=fixtures.ScheduleTests.records
    controls=fixtures.ScheduleTests.controls
    schedule=fixtures.ScheduleTests.schedule

    def pipeline(self,lane,**kwargs):
        schedule=self.schedule(lane,**kwargs)
        origin=SnapshotOrigin(*lane.writer.identity,lane.identity['capture_id'],lane.identity['process_session_id'],
            lane.health()['clock']['cutoff_scope'],lane._last_observation[2])
        publisher=SnapshotPublisher(self.root/f'publisher-{self.counter}',origin,observe=self.observe,max_receipt_bytes=MAX_REPORT_BYTES)
        self.addAsyncCleanup(publisher.close)
        pipeline=SnapshotPipeline(schedule,publisher);self.addCleanup(pipeline.stop)
        return pipeline

    async def finish_raw(self,lane):
        lane.stop();await asyncio.wait_for(lane.test_task,5)

    async def test_two_cycles_own_publish_archive_and_handoff_ack(self):
        lane=await self.ready();first=asyncio.Event();waits=[]
        async def wait(deadline):
            if waits:await first.wait()
            waits.append(deadline);await self.measured_wait(deadline)
        pipeline=self.pipeline(lane,max_cutoffs=2,wait_until=wait)
        original=pipeline.schedule.ack
        def ack(close):original(close);first.set()
        with patch.object(pipeline.schedule,'ack',side_effect=ack):
            state=await asyncio.wait_for(pipeline.run(),5)
        self.assertEqual((state['received'],state['reported'],state['published'],state['failed'],state['unconfirmed']),(2,2,2,0,0))
        self.assertEqual(state['schedule']['acknowledged'],2)
        self.assertFalse(state['publisher']['accepting'])
        self.assertTrue(lane.health()['accepting'])
        await self.finish_raw(lane)
        reports=replay_delivery_reports(self.records(lane),origin=pipeline.publisher.origin)
        self.assertEqual([r.outcome for r in reports],['published','published'])
        with self.assertRaises(PipelineStopped):await pipeline.run()

    async def test_stop_while_waiting_resolves_allocation_and_keeps_raw_open(self):
        lane=await self.ready();entered=asyncio.Event()
        async def wait(deadline):entered.set();await asyncio.Event().wait()
        pipeline=self.pipeline(lane,wait_until=wait)
        task=asyncio.create_task(pipeline.run());await asyncio.wait_for(entered.wait(),5)
        pipeline.stop();state=await asyncio.wait_for(task,5)
        self.assertEqual(state['reported'],0)
        self.assertEqual(state['schedule']['failed'],1)
        self.assertTrue(lane.health()['accepting'])
        await lane.submit_received(self.received(lane,1))

    async def test_cancel_during_publishing_drains_receipt_before_unlock_and_raw_continues(self):
        lane=await self.ready();pipeline=self.pipeline(lane)
        entered,release=threading.Event(),threading.Event();self.addCleanup(release.set)
        def blocked(*args,**kwargs):
            entered.set()
            if not release.wait(3):raise OSError('test gate timeout')
            return publish_snapshot(*args,**kwargs)
        with patch('data_collector.publisher.publish_snapshot',side_effect=blocked):
            task=asyncio.create_task(pipeline.run())
            self.assertTrue(await asyncio.to_thread(entered.wait,2))
            self.mono[0]+=1
            self.assertTrue((await lane.submit_received(self.received(lane,1))).timing_valid)
            task.cancel();await asyncio.sleep(0);task.cancel();await asyncio.sleep(0)
            self.assertFalse(task.done())
            with self.assertRaises(BlockingIOError):
                SnapshotPublisher(pipeline.publisher.root,pipeline.publisher.origin,observe=self.observe,max_receipt_bytes=MAX_REPORT_BYTES)
            release.set()
            with self.assertRaises(asyncio.CancelledError):await asyncio.wait_for(task,5)
        state=pipeline.health()
        self.assertEqual((state['reported'],state['published'],state['schedule']['acknowledged']),(1,1,1))
        self.assertIsNone(state['current_close_id'])
        self.assertTrue(lane.health()['accepting'])
        await self.finish_raw(lane)
        self.assertEqual(len(replay_delivery_reports(self.records(lane),origin=pipeline.publisher.origin)),1)

    async def test_cancel_during_seal_drains_full_chain_before_returning(self):
        lane=await self.ready();pipeline=self.pipeline(lane)
        entered,release=threading.Event(),threading.Event();self.addCleanup(release.set)
        original=lane.writer.rotate
        def blocked():
            if pipeline.phase=='sealing':
                entered.set()
                if not release.wait(3):raise OSError('test gate timeout')
            return original()
        with patch.object(lane.writer,'rotate',side_effect=blocked):
            task=asyncio.create_task(pipeline.run());self.assertTrue(await asyncio.to_thread(entered.wait,2))
            task.cancel();await asyncio.sleep(0);self.assertFalse(task.done());release.set()
            with self.assertRaises(asyncio.CancelledError):await asyncio.wait_for(task,5)
        self.assertEqual(pipeline.health()['reported'],1)
        self.assertEqual(pipeline.schedule.health()['acknowledged'],1)
        self.assertTrue(lane.health()['accepting'])

    async def test_cancel_during_raw_receipt_write_keeps_lease_until_durable_ack(self):
        lane=await self.ready();pipeline=self.pipeline(lane)
        entered,release=threading.Event(),threading.Event();self.addCleanup(release.set)
        original=lane.writer.append
        def blocked(record):
            if record.channel.startswith('snapshot_deliveries:'):
                entered.set()
                if not release.wait(3):raise OSError('test gate timeout')
            return original(record)
        with patch.object(lane.writer,'append',side_effect=blocked):
            task=asyncio.create_task(pipeline.run());self.assertTrue(await asyncio.to_thread(entered.wait,2))
            task.cancel();await asyncio.sleep(0)
            self.assertTrue(pipeline.schedule.health()['leased'])
            self.assertEqual(pipeline.health()['reported'],0)
            self.assertFalse(task.done());release.set()
            with self.assertRaises(asyncio.CancelledError):await asyncio.wait_for(task,5)
        self.assertEqual(pipeline.health()['reported'],1)
        self.assertFalse(pipeline.schedule.health()['leased'])

    async def test_failed_publication_receipt_is_archived_without_promoting_existing_artifact(self):
        lane=await self.ready();pipeline=self.pipeline(lane)
        def failed(*args,**kwargs):
            publish_snapshot(*args,**kwargs);raise OSError('error after artifact commit')
        with patch('data_collector.publisher.publish_snapshot',side_effect=failed):
            state=await asyncio.wait_for(pipeline.run(),5)
        self.assertEqual((state['reported'],state['published'],state['failed']),(1,0,1))
        self.assertTrue(lane.health()['accepting'])
        await self.finish_raw(lane)
        report=replay_delivery_reports(self.records(lane),origin=pipeline.publisher.origin)[0]
        self.assertEqual(report.reason,'publication_failed')

    async def test_late_start_archives_failed_receipt_and_no_artifact(self):
        lane=await self.ready();pipeline=self.pipeline(lane);original=pipeline.publisher.submit
        def late(*args,**kwargs):
            self.mono[0]+=DELIVERY_NS+1
            return original(*args,**kwargs)
        with patch.object(pipeline.publisher,'submit',side_effect=late):
            state=await asyncio.wait_for(pipeline.run(),5)
        self.assertEqual((state['published'],state['failed']),(0,1))
        self.assertFalse((pipeline.publisher.root/'snapshots').exists())
        await self.finish_raw(lane)
        self.assertEqual(replay_delivery_reports(self.records(lane),origin=pipeline.publisher.origin)[0].reason,'publication_deadline_missed')

    async def test_unconfirmed_publisher_receipt_halts_without_acknowledging_handoff(self):
        lane=await self.ready();pipeline=self.pipeline(lane)
        with patch('data_collector.publisher._publish_bytes',side_effect=OSError('receipt disk failure')):
            with self.assertRaises(PublicationUnconfirmed):await asyncio.wait_for(pipeline.run(),5)
        await lane.test_task
        state=pipeline.health()
        self.assertEqual((state['reported'],state['published'],state['unconfirmed']),(0,0,1))
        self.assertTrue(state['schedule']['leased'])
        self.assertIsNotNone(state['current_close_id'])
        self.assertEqual(lane.health()['venue_failure'],'snapshot_publication_failure')
        self.assertEqual(lane.health()['clock']['delivery_handoffs_without_report'],1)
        self.assertEqual(replay_delivery_reports(self.records(lane),origin=pipeline.publisher.origin),())

    async def test_raw_report_write_failure_remains_unconfirmed_despite_successful_snapshot(self):
        lane=await self.ready();pipeline=self.pipeline(lane);original=lane.writer.append
        def failed(record):
            if record.channel.startswith('snapshot_deliveries:'):raise OSError('raw receipt failed')
            return original(record)
        with patch.object(lane.writer,'append',side_effect=failed),redirect_stderr(io.StringIO()):
            with self.assertRaises(DurabilityUnknown):await asyncio.wait_for(pipeline.run(),5)
            await lane.test_task
        state=pipeline.health()
        self.assertEqual((state['reported'],state['published'],state['unconfirmed']),(0,0,1))
        self.assertTrue(state['schedule']['leased'])
        self.assertEqual(lane.health()['clock']['delivery_handoffs_without_report'],1)
        self.assertFalse(state['publisher']['busy'])

    async def test_producer_failure_is_observed_and_does_not_leave_consumer_waiting(self):
        lane=await self.ready();pipeline=self.pipeline(lane,plan=lambda:None)
        with self.assertRaises(PipelineStopped):await asyncio.wait_for(pipeline.run(),5)
        self.assertEqual(pipeline.health()['reported'],0)
        self.assertTrue(pipeline.health()['terminal'])
        self.assertFalse(pipeline.publisher.health()['accepting'])
        await lane.test_task

    async def test_constructor_rejects_foreign_used_or_incompatible_owners(self):
        lane=await self.ready();pipeline=self.pipeline(lane)
        with self.assertRaises(ValueError):SnapshotPipeline(pipeline.schedule,pipeline.publisher)
        origin=replace(pipeline.publisher.origin,process_session_id='foreign')
        foreign=SnapshotPublisher(self.root/'foreign',origin,observe=self.observe,max_receipt_bytes=MAX_REPORT_BYTES)
        self.addAsyncCleanup(foreign.close)
        with self.assertRaises(ValueError):SnapshotPipeline(pipeline.schedule,foreign)
        large=SnapshotPublisher(self.root/'large',pipeline.publisher.origin,observe=self.observe)
        self.addAsyncCleanup(large.close)
        with self.assertRaises(ValueError):SnapshotPipeline(pipeline.schedule,large)
        await pipeline.run()
        with self.assertRaises(ValueError):SnapshotPipeline(pipeline.schedule,pipeline.publisher)
