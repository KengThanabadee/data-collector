import asyncio
from dataclasses import replace
import threading
import unittest
from unittest.mock import patch

from data_collector.contracts import StreamKey,canonical,strict_json
from data_collector.schedule import ScheduleStopped
from data_collector.lane import DurabilityUnknown
from data_collector.snapshots import read_snapshot
from tests import test_dc_pipeline as fixtures


class ScheduleCutoverTests(unittest.IsolatedAsyncioTestCase):
    setUp=fixtures.PipelineTests.setUp
    observe=fixtures.PipelineTests.observe
    lane=fixtures.PipelineTests.lane
    start=fixtures.PipelineTests.start
    record=fixtures.PipelineTests.record
    received=fixtures.PipelineTests.received
    clock_request=fixtures.PipelineTests.clock_request
    clock_policy=fixtures.PipelineTests.clock_policy
    ready_cutoffs=fixtures.PipelineTests.ready_cutoffs
    ready=fixtures.PipelineTests.ready
    measured_wait=fixtures.PipelineTests.measured_wait
    records=fixtures.PipelineTests.records
    controls=fixtures.PipelineTests.controls
    schedule=fixtures.PipelineTests.schedule
    pipeline=fixtures.PipelineTests.pipeline
    finish_raw=fixtures.PipelineTests.finish_raw

    def replacement(self):
        binding=self.plan.bindings[0]
        return replace(self.plan,bindings=(replace(binding,
            target=replace(binding.target,owner_epoch='dc-owner/1:'+'1'*32),
            ws_source=StreamKey('capture','process','ws2'),ws_start_seq=0),))

    async def native(self,lane,plan):
        binding=plan.bindings[0]
        record=self.received(lane)
        body=canonical(dict(jsonrpc='2.0',method='subscription',params=dict(channel=binding.native_channel,
            data=dict(type='snapshot',timestamp=2,instrument_name=binding.target.instrument.native_symbol,
                change_id=20,bids=[['new',100,9]],asks=[['new',101,3]]))))
        return await lane.submit_received(replace(record,
            receipt=replace(record.receipt,connection_id=binding.ws_source.connection_id),body=body))

    def block_declaration(self,lane):
        entered,release=threading.Event(),threading.Event();self.addCleanup(release.set)
        original=lane._execute_owner_plan
        def blocked(request):
            entered.set()
            if not release.wait(5):raise OSError('test declaration gate timeout')
            return original(request)
        return entered,release,patch.object(lane,'_execute_owner_plan',side_effect=blocked)

    async def test_declaration_ack_and_plan_selection_are_atomic_with_allocation(self):
        lane=await self.ready();plan=self.replacement();data_ready=asyncio.Event()
        async def wait(deadline):
            await data_ready.wait();await self.measured_wait(deadline)
        pipeline=self.pipeline(lane,wait_until=wait,
            healthy_streams_for_plan=lambda p:(p.bindings[0].ws_source,self.meta))
        entered,release,blocked=self.block_declaration(lane)
        with blocked:
            cutover=asyncio.create_task(pipeline.schedule.replace_plan(plan))
            self.assertTrue(await asyncio.to_thread(entered.wait,2))
            running=asyncio.create_task(pipeline.run());await asyncio.sleep(0)
            self.assertEqual(lane.health()['clock']['cutoff_allocations_submitted'],0)
            self.assertNotEqual(pipeline.schedule.plan(),plan)
            release.set();ack=await asyncio.wait_for(cutover,5)
        self.assertEqual(ack.declaration.plan,plan)
        await self.native(lane,plan);data_ready.set()
        state=await asyncio.wait_for(running,5)
        self.assertEqual(state['published'],1)
        allocation=lane._cutoffs.coordinator.allocations[0]['payload']
        self.assertEqual(allocation['dependencies'],plan.dependencies)
        await self.finish_raw(lane)
        report=next(strict_json(r.body) for r in self.records(lane) if r.channel=='snapshot_deliveries:capture')
        result=read_snapshot(pipeline.publisher.root/'snapshots',report['snapshot_id'],lane.segments)
        self.assertEqual(strict_json(result['payload'])['books'][0]['book']['bids'],[['100','9']])

    async def test_open_old_cutoff_keeps_its_plan_health_and_next_uses_replacement(self):
        lane=await self.ready();old=self.plan;plan=self.replacement()
        allocated=asyncio.Event();release=asyncio.Event();reported=asyncio.Event();calls=[];health=[]
        async def wait(deadline):
            calls.append(deadline)
            if len(calls)==1:allocated.set();await release.wait()
            else:await reported.wait()
            await self.measured_wait(deadline)
        def for_plan(p):
            health.append(p);return (p.bindings[0].ws_source,self.meta)
        pipeline=self.pipeline(lane,max_cutoffs=2,wait_until=wait,
            healthy_streams=lambda:(plan.bindings[0].ws_source,self.meta),healthy_streams_for_plan=for_plan)
        original=pipeline.schedule.ack
        def ack(close):original(close);reported.set()
        with patch.object(pipeline.schedule,'ack',side_effect=ack):
            task=asyncio.create_task(pipeline.run());await asyncio.wait_for(allocated.wait(),5)
            await pipeline.schedule.replace_plan(plan);await self.native(lane,plan);release.set()
            state=await asyncio.wait_for(task,5)
        self.assertEqual(state['published'],2);self.assertEqual(health,[old,plan])
        await self.finish_raw(lane)
        reports=[strict_json(r.body) for r in self.records(lane) if r.channel=='snapshot_deliveries:capture']
        books=[]
        for report in reports:
            result=read_snapshot(pipeline.publisher.root/'snapshots',report['snapshot_id'],lane.segments)
            books.append(strict_json(result['payload'])['books'][0])
        self.assertEqual([b['book']['bids'] for b in books],[[['100','2']],[['100','9']]])
        self.assertEqual([b['validity_as_published'] for b in books],['valid','valid'])

    async def test_cancelled_admitted_cutover_drains_and_stops_before_any_allocation(self):
        lane=await self.ready();plan=self.replacement();schedule=self.schedule(lane)
        entered,release,blocked=self.block_declaration(lane)
        with blocked:
            task=asyncio.create_task(schedule.replace_plan(plan))
            self.assertTrue(await asyncio.to_thread(entered.wait,2));task.cancel();await asyncio.sleep(0)
            self.assertFalse(task.done());release.set()
            with self.assertRaises(asyncio.CancelledError):await asyncio.wait_for(task,5)
        self.assertEqual(schedule.plan(),plan);self.assertTrue(schedule.health()['stopping'])
        self.assertEqual(lane.health()['clock']['cutoff_allocations_submitted'],0)
        self.assertEqual(schedule.health()['allocated'],0)
        await self.finish_raw(lane)
        self.assertEqual(len([r for r in self.records(lane) if r.channel=='owner_plans:capture']),1)

    async def test_failed_durable_cutover_stops_without_switching_provider(self):
        lane=await self.ready();old=self.plan;schedule=self.schedule(lane)
        with patch.object(lane,'_execute_owner_plan',side_effect=OSError('test disk uncertainty')):
            with self.assertRaises(DurabilityUnknown):await schedule.replace_plan(self.replacement())
        self.assertEqual(schedule.plan(),old);self.assertTrue(schedule.health()['stopping'])
        self.assertEqual(lane.health()['clock']['cutoff_allocations_submitted'],0)

    async def test_invalid_or_stopped_cutover_never_writes_owner_declaration(self):
        lane=await self.ready();schedule=self.schedule(lane)
        with self.assertRaisesRegex(ValueError,'config'):
            await schedule.replace_plan(replace(self.replacement(),config_sha256='e'*64))
        self.assertFalse(schedule.health()['stopping']);schedule.stop()
        with self.assertRaises(ScheduleStopped):await schedule.replace_plan(self.replacement())
        await self.finish_raw(lane)
        self.assertFalse(any(r.channel=='owner_plans:capture' for r in self.records(lane)))

    async def test_submission_failure_keeps_original_error_and_halts_healthy_lane(self):
        lane=await self.ready();schedule=self.schedule(lane)
        with patch.object(lane,'submit_owner_plan',side_effect=RuntimeError('test admission failure')):
            with self.assertRaisesRegex(RuntimeError,'test admission failure'):
                await schedule.replace_plan(self.replacement())
        self.assertFalse(lane.health()['accepting']);self.assertTrue(schedule.health()['stopping'])
        self.assertEqual(schedule.plan(),self.plan)

    async def test_untimely_ack_halts_before_replacement_can_allocate(self):
        lane=await self.ready();schedule=self.schedule(lane);submit=lane.submit_owner_plan
        async def untimely(*args,**kwargs):
            ack=await submit(*args,**kwargs)
            return replace(ack,timing_valid=False)
        with patch.object(lane,'submit_owner_plan',side_effect=untimely):
            with self.assertRaisesRegex(ScheduleStopped,'timely exact'):
                await schedule.replace_plan(self.replacement())
        self.assertFalse(lane.health()['accepting']);self.assertTrue(schedule.health()['stopping'])
        self.assertEqual(schedule.plan(),self.plan)
        self.assertEqual(lane.health()['clock']['cutoff_allocations_submitted'],0)
