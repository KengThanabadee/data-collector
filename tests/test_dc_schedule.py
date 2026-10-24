import asyncio
from contextlib import redirect_stderr
from dataclasses import replace
import io
import unittest
from unittest.mock import patch

from data_collector.contracts import StreamKey,canonical,strict_json
from data_collector.cutoffs import MINUTE_NS
from data_collector.deliveries import replay_delivery_reports
from data_collector.metadata import MetadataVersion
from data_collector.publisher import SnapshotPublisher
from data_collector.replay import BookBinding,SnapshotPlan
from data_collector.schedule import MinuteCutoffs,ScheduleStopped
from data_collector.validity import FeedTarget
from tests import test_dc_lane as fixtures
from tests.test_dc_contracts import key

C=MINUTE_NS


class ScheduleTests(unittest.IsolatedAsyncioTestCase):
    setUp=fixtures.LaneTests.setUp
    observe=fixtures.LaneTests.observe
    lane=fixtures.LaneTests.lane
    start=fixtures.LaneTests.start
    record=fixtures.LaneTests.record
    received=fixtures.LaneTests.received
    clock_request=fixtures.LaneTests.clock_request
    ready_cutoffs=fixtures.LaneTests.ready_cutoffs
    records=fixtures.LaneTests.records
    controls=fixtures.LaneTests.controls

    def clock_policy(self):
        return dict(sample_interval_ns=60,max_sample_age_ns=C,max_offset_ns=100,step_tolerance_ns=2,max_epochs=8)

    async def ready(self):
        lane,_=await self.ready_cutoffs()
        self.lane_owner=lane;self.sample_seq=2
        inst=key();self.ws=StreamKey('capture','process','connection');self.meta=StreamKey('capture','process','meta')
        attrs=dict(base_currency='BTC',amount_unit='native',quote_currency='native')
        record=replace(self.received(lane),kind='metadata_version',channel='metadata',
            receipt=replace(self.received(lane).receipt,connection_id='meta'),
            body=canonical(dict(schema='dc.metadata/2',effective_at_ns=C-100,attributes=attrs,supersedes=None)))
        ack=await lane.submit_received(record)
        version=MetadataVersion(inst,replace(record.receipt,clock_epoch=ack.clock_epoch),C-100,canonical(attrs))
        self.plan=SnapshotPlan((BookBinding(FeedTarget(inst,'deribit_full','owner'),self.ws,self.meta,version.version_id),),'b'*64)
        channel=f'book.{inst.native_symbol}.100ms'
        body=canonical(dict(jsonrpc='2.0',method='subscription',params=dict(channel=channel,data=dict(
            type='snapshot',timestamp=1,instrument_name=inst.native_symbol,change_id=10,
            bids=[['new',100,2]],asks=[['new',101,3]]))))
        await lane.submit_received(replace(self.received(lane),channel=channel,body=body))
        return lane

    async def measured_wait(self,deadline):
        self.mono[0]=max(self.mono[0],deadline)
        await self.lane_owner.submit_clock(*self.clock_request(self.lane_owner,self.sample_seq))
        self.sample_seq+=1

    def schedule(self,lane,**kwargs):
        schedule=MinuteCutoffs(lane,first_cutoff_utc_ns=kwargs.pop('first_cutoff_utc_ns',C),
            plan=kwargs.pop('plan',lambda:self.plan),healthy_streams=kwargs.pop('healthy_streams',lambda:(self.ws,self.meta)),
            boot_id='boot',max_cutoffs=kwargs.pop('max_cutoffs',1),wait_until=kwargs.pop('wait_until',self.measured_wait),**kwargs)
        self.addCleanup(schedule.stop)
        return schedule

    async def test_two_real_cycles_publish_archive_and_replay_without_stopping_raw(self):
        lane=await self.ready();first_done=asyncio.Event();waits=[]
        async def wait(deadline):
            if waits:await first_done.wait()
            waits.append(deadline);await self.measured_wait(deadline)
        schedule=self.schedule(lane,max_cutoffs=2,wait_until=wait)
        task=asyncio.create_task(schedule.run())
        publisher=None;origins=[]
        for index in range(2):
            cut=await asyncio.wait_for(schedule.take(),5)
            utc,mono,_=self.observe()
            inputs=await lane.submit_snapshot_inputs(cut.close_record_id,utc_ns=utc,monotonic_ns=mono)
            if publisher is None:
                publisher=SnapshotPublisher(self.root/'publisher',inputs.origin,observe=self.observe)
                self.addAsyncCleanup(publisher.close)
            result=await asyncio.wait_for(publisher.submit(inputs,cut.plan),5)
            self.assertEqual(result.outcome,'published')
            utc,mono,_=self.observe()
            await lane.submit_snapshot_delivery(inputs,result.receipt_path.read_bytes(),utc_ns=utc,monotonic_ns=mono)
            origins.append(inputs.origin)
            schedule.ack(cut.close_record_id)
            if index==0:first_done.set()
        state=await asyncio.wait_for(task,5)
        self.assertEqual((state['closed'],schedule.health()['acknowledged']),(2,2))
        self.assertEqual(waits,[1100,C+1100])
        self.assertTrue(lane.health()['accepting'])
        lane.stop();await lane.test_task
        reports=replay_delivery_reports(self.records(lane),origin=origins[0])
        self.assertEqual([r.outcome for r in reports],['published','published'])
        with self.assertRaises(StopAsyncIteration):await schedule.take()

    async def test_delayed_wakeup_keeps_saved_deadline_and_excludes_later_raw(self):
        lane=await self.ready()
        async def delayed(deadline):
            await self.measured_wait(deadline)
            self.mono[0]+=1
            await lane.submit_received(self.received(lane,1))
        schedule=self.schedule(lane,wait_until=delayed)
        await schedule.run();cut=await schedule.take()
        closure=lane._cutoffs.coordinator.resolutions[0]['payload']
        self.assertEqual(cut.deadline_ns,1100)
        position=next(p for p in closure['streams'] if p['stream']['connection_id']=='connection')
        self.assertEqual(position['admitted_seq'],0)
        schedule.ack(cut.close_record_id)

    async def test_stop_during_wait_durably_fails_open_allocation_and_raw_continues(self):
        lane=await self.ready();entered=asyncio.Event()
        async def waiting(deadline):entered.set();await asyncio.Event().wait()
        schedule=self.schedule(lane,wait_until=waiting)
        task=asyncio.create_task(schedule.run());await asyncio.wait_for(entered.wait(),5)
        schedule.stop();state=await asyncio.wait_for(task,5)
        self.assertEqual(state['failed'],1)
        self.assertEqual(lane._cutoffs.coordinator.resolutions[0]['payload']['reason'],'snapshot_schedule_stopped')
        self.assertTrue(lane.health()['accepting'])
        await lane.submit_received(self.received(lane,1))
        with self.assertRaises(StopAsyncIteration):await schedule.take()

    async def test_cancellation_waits_for_allocated_command_then_records_failed_cutoff(self):
        lane=await self.ready();entered=asyncio.Event();release=asyncio.Event()
        original=lane._disk
        async def delayed(function,*args):
            result=await original(function,*args)
            if getattr(function,'__name__','')=='_execute_cutoff' and strict_json(args[0].body)['operation']=='allocate':
                entered.set();await release.wait()
            return result
        schedule=self.schedule(lane)
        with patch.object(lane,'_disk',side_effect=delayed):
            task=asyncio.create_task(schedule.run());await asyncio.wait_for(entered.wait(),5)
            task.cancel();await asyncio.sleep(0);self.assertFalse(task.done());release.set()
            with self.assertRaises(asyncio.CancelledError):await asyncio.wait_for(task,5)
        self.assertEqual(lane._cutoffs.coordinator.resolutions[0]['payload']['reason'],'snapshot_schedule_stopped')
        self.assertTrue(lane.health()['accepting'])

    async def test_handoff_backlog_halts_venue_and_preserves_both_closures(self):
        lane=await self.ready();schedule=self.schedule(lane,max_cutoffs=2)
        with self.assertRaises(ScheduleStopped):await asyncio.wait_for(schedule.run(),5)
        await lane.test_task
        self.assertEqual(lane.health()['venue_failure'],'snapshot_handoff_backlog')
        self.assertEqual(len(lane._cutoffs.coordinator.resolutions),2)
        cut=await schedule.take();self.assertEqual(cut.cutoff_id,0)
        schedule.ack(cut.close_record_id)
        self.assertEqual(schedule.health()['closed'],2)
        self.assertTrue(any(c['payload']['reason']=='snapshot_handoff_backlog' for c in self.controls(lane) if c['kind']=='ingress_halted'))

    async def test_backward_step_fails_and_paces_next_label_without_spinning(self):
        lane=await self.ready();waits=[]
        async def step(deadline):
            waits.append(deadline);self.mono[0]=max(self.mono[0],deadline)
            if len(waits)==1:self.utc-=1000
        schedule=self.schedule(lane,max_cutoffs=2,wait_until=step)
        state=await schedule.run()
        self.assertEqual((state['allocated'],state['failed'],state['closed']),(2,2,0))
        self.assertEqual(len(waits),2)
        self.assertGreater(waits[1],waits[0])
        self.assertTrue(lane.health()['accepting'])
        self.assertEqual([r['payload']['utc_ns'] for r in lane._cutoffs.coordinator.allocations.values()],[C,2*C])

    async def test_forward_step_allocates_explicit_missing_labels_without_relabeling(self):
        lane=await self.ready();calls=[]
        async def step(deadline):
            self.mono[0]=max(self.mono[0],deadline)
            if not calls:self.utc+=2*C
            calls.append(deadline)
        schedule=self.schedule(lane,max_cutoffs=3,wait_until=step)
        state=await schedule.run()
        self.assertEqual((state['failed'],state['closed']),(3,0))
        self.assertEqual([r['payload']['utc_ns'] for r in lane._cutoffs.coordinator.allocations.values()],[C,2*C,3*C])
        self.assertTrue(lane.health()['accepting'])

    async def test_plan_health_and_process_faults_leave_explicit_failure(self):
        for fault in ('plan','health','process'):
            with self.subTest(fault=fault):
                self.mono[0]=1000;lane=await self.ready()
                async def regressed(deadline):self.mono[0]=1059
                schedule=self.schedule(lane,
                    plan=(lambda:None) if fault=='plan' else (lambda:self.plan),
                    healthy_streams=(lambda:(StreamKey('capture','process','foreign'),)) if fault=='health' else (lambda:(self.ws,self.meta)),
                    wait_until=regressed if fault=='process' else self.measured_wait)
                with redirect_stderr(io.StringIO()):
                    with self.assertRaises((ValueError,ScheduleStopped)):await schedule.run()
                    await lane.test_task
                self.assertFalse(lane.health()['accepting'])
                self.assertIsNotNone(lane.health()['venue_failure'])

    async def test_single_lease_ack_validation_and_once_only_run(self):
        lane=await self.ready();schedule=self.schedule(lane)
        with self.assertRaises(ValueError):schedule.ack('x')
        await schedule.run();cut=await schedule.take()
        with self.assertRaises(ValueError):await schedule.take()
        with self.assertRaises(ValueError):schedule.ack('x')
        schedule.ack(cut.close_record_id)
        with self.assertRaises(ScheduleStopped):await schedule.run()

    async def test_early_timer_cannot_close_early_and_repeated_early_wakes_are_bounded(self):
        lane=await self.ready();calls=[]
        async def early_once(deadline):
            calls.append(deadline)
            if len(calls)>1:await self.measured_wait(deadline)
        schedule=self.schedule(lane,wait_until=early_once)
        await schedule.run();self.assertEqual(len(calls),2)
        self.assertEqual((await schedule.take()).deadline_ns,1100)
        lane.stop();await lane.test_task
        self.mono[0]=1000;another=await self.ready()
        async def always_early(deadline):await asyncio.sleep(0)
        broken=self.schedule(another,wait_until=always_early)
        with self.assertRaises(ScheduleStopped):await asyncio.wait_for(broken.run(),5)
        await another.test_task
        self.assertEqual(broken.health()['closed'],0)

    async def test_invalid_configuration_and_plan_memory_bound_fail_before_allocation(self):
        lane=await self.ready()
        for kwargs in (dict(first_cutoff_utc_ns=C+1),dict(max_cutoffs=10001),dict(max_plan_bytes=0)):
            with self.assertRaises(ValueError):self.schedule(lane,**kwargs)
        schedule=self.schedule(lane,max_plan_bytes=1)
        with self.assertRaises(ValueError):await schedule.run()
        self.assertEqual(schedule.health()['allocated'],0)
