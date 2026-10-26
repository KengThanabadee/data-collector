import asyncio
from dataclasses import replace
import threading
import unittest
from unittest.mock import patch

import aiohttp

from data_collector.contracts import canonical,strict_json
from data_collector.deribit import DeribitBookSession,DeribitLimits,DeribitStopped
from data_collector.exports import export_snapshots
from data_collector.metadata import MetadataVersion
from data_collector.validity import FeedTarget
from data_collector.quota import PriorityQuota,QuotaWindow
from data_collector.journal import JournalLimits
from data_collector.queues import QueueLimits
from data_collector.segments import SegmentLimits
from tests import test_dc_lane as lane_fixtures
from data_collector.snapshots import read_snapshot
from tests import test_dc_schedule_cutovers as fixtures


class Socket:
    def __init__(self):
        self.incoming=asyncio.Queue();self.sent=[];self.closed=False
    async def send_str(self,body):
        value=strict_json(body.encode());self.sent.append(value)
        result=value['params']['channels'] if value['method']=='public/subscribe' else True
        self.put(dict(jsonrpc='2.0',id=value['id'],result=result))
    def put(self,value):
        body=value if isinstance(value,bytes) else canonical(value)
        self.incoming.put_nowait(aiohttp.WSMessage(aiohttp.WSMsgType.BINARY,body,''))
    async def receive(self):return await self.incoming.get()
    async def close(self):self.closed=True


class HTTP:
    def __init__(self,socket):self.socket=socket;self.options=None
    async def ws_connect(self,url,**kwargs):
        self.options=(url,kwargs);return self.socket


class DeribitTests(unittest.IsolatedAsyncioTestCase):
    setUp=fixtures.ScheduleCutoverTests.setUp
    observe=fixtures.ScheduleCutoverTests.observe
    lane=fixtures.ScheduleCutoverTests.lane
    start=fixtures.ScheduleCutoverTests.start
    record=fixtures.ScheduleCutoverTests.record
    received=fixtures.ScheduleCutoverTests.received
    clock_request=fixtures.ScheduleCutoverTests.clock_request
    clock_policy=fixtures.ScheduleCutoverTests.clock_policy
    ready_cutoffs=fixtures.ScheduleCutoverTests.ready_cutoffs
    ready=fixtures.ScheduleCutoverTests.ready
    measured_wait=fixtures.ScheduleCutoverTests.measured_wait
    records=fixtures.ScheduleCutoverTests.records
    controls=fixtures.ScheduleCutoverTests.controls
    schedule=fixtures.ScheduleCutoverTests.schedule
    pipeline=fixtures.ScheduleCutoverTests.pipeline
    finish_raw=fixtures.ScheduleCutoverTests.finish_raw

    def receiver(self,lane,**limits):
        pipeline=self.pipeline(lane)
        quota=PriorityQuota('deribit',[QuotaWindow('control',1000,1_000_000_000)],now_ns=lambda:self.mono[0])
        self.addAsyncCleanup(quota.close)
        limits.setdefault('max_message_bytes',4096)
        receiver=DeribitBookSession(pipeline.schedule,quota,control_charges={'control':1},limits=DeribitLimits(**limits))
        pipeline.schedule.healthy_streams_for_plan=receiver.healthy_streams_for_plan
        self.socket=Socket();self.http=HTTP(self.socket)
        return receiver,pipeline

    def frame(self,receiver,*,snapshot=True,gap=False,symbol=None):
        binding=receiver.plan.bindings[0];symbol=symbol or binding.target.instrument.native_symbol
        return dict(jsonrpc='2.0',method='subscription',params=dict(channel=f'book.{symbol}.100ms',data=dict(
            type='snapshot' if snapshot else 'change',instrument_name=symbol,timestamp=2,
            change_id=20 if snapshot else 21,prev_change_id=999 if gap else 20,
            bids=[['new',100,9]] if snapshot else [['change',100,7]],asks=[])))

    async def until(self,condition):
        async def wait():
            while not condition():await asyncio.sleep(0)
        await asyncio.wait_for(wait(),5)

    def launch(self,receiver):
        task=asyncio.create_task(receiver.run(self.http))
        async def cleanup():
            receiver.stop()
            await asyncio.wait_for(asyncio.gather(task,return_exceptions=True),5)
        self.addAsyncCleanup(cleanup)
        return task

    async def test_durable_native_ingress_publishes_and_replays_exact_book(self):
        lane=await self.ready();receiver,pipeline=self.receiver(lane)
        task=self.launch(receiver)
        self.socket.put(self.frame(receiver));self.socket.put(self.frame(receiver,snapshot=False))
        await self.until(lambda:receiver._acked and receiver.models[0].last_id==21)
        state=await pipeline.run();self.assertEqual(state['published'],1)
        receiver.stop();await task;self.assertTrue(self.socket.closed)
        await self.finish_raw(lane)
        report=next(strict_json(r.body) for r in self.records(lane) if r.channel=='snapshot_deliveries:capture')
        result=read_snapshot(pipeline.publisher.root/'snapshots',report['snapshot_id'],lane.segments)
        book=strict_json(result['payload'])['books'][0]
        self.assertEqual(book['validity_as_published'],'valid')
        self.assertEqual(book['book'],strict_json(canonical(receiver.models[0].view())))
        self.assertEqual(book['book']['bids'],[['100','7']])
        self.assertEqual(self.http.options[1]['max_msg_size'],receiver.limits.max_message_bytes)
        self.assertEqual(receiver.quota.health()['grants'],2)

    async def test_native_gap_automatically_corrects_quiet_published_rows(self):
        lane=await self.ready();receiver,pipeline=self.receiver(lane)
        task=self.launch(receiver);self.socket.put(self.frame(receiver))
        await self.until(lambda:receiver._acked and receiver.models[0].state=='live')
        await pipeline.run()
        self.socket.put(self.frame(receiver,snapshot=False,gap=True))
        with self.assertRaisesRegex(DeribitStopped,'native_resync_required'):await task
        self.assertEqual(len(receiver.gaps),1);self.assertTrue(self.socket.closed)
        await self.finish_raw(lane)
        reports=[strict_json(r.body) for r in self.records(lane) if r.channel=='snapshot_deliveries:capture']
        original=read_snapshot(pipeline.publisher.root/'snapshots',reports[0]['snapshot_id'],lane.segments)
        self.assertEqual(strict_json(original['payload'])['books'][0]['validity_as_published'],'valid')
        export=export_snapshots(pipeline.publisher.root/'snapshots',self.root/'exports',[reports[0]['snapshot_id']],lane.segments)
        self.assertEqual(export['descriptor']['population']['invalid'],1)
        self.assertFalse(receiver.healthy_streams_for_plan(receiver.plan)==(receiver.source,))

    async def test_ack_does_not_create_native_anchor_and_heartbeat_reply_is_archived(self):
        lane=await self.ready();receiver,_=self.receiver(lane);task=self.launch(receiver)
        await self.until(lambda:receiver._acked)
        self.assertEqual(receiver.models[0].state,'bootstrapping')
        self.assertNotIn(receiver.source,receiver.healthy_streams_for_plan(receiver.plan))
        self.socket.put(dict(jsonrpc='2.0',method='heartbeat',params=dict(type='test_request')))
        await self.until(lambda:any(v['method']=='public/test' for v in self.socket.sent))
        receiver.stop();await task;await self.finish_raw(lane)
        controls=[strict_json(r.body) for r in self.records(lane) if r.channel=='public/test']
        self.assertEqual(len(controls),1);self.assertEqual(receiver.quota.health()['grants'],3)

    async def test_foreign_symbol_is_retained_without_mutating_selected_book(self):
        lane=await self.ready();receiver,_=self.receiver(lane);task=self.launch(receiver)
        foreign=self.frame(receiver,symbol='FOREIGN-C')
        self.socket.put(foreign);self.socket.put(self.frame(receiver))
        await self.until(lambda:receiver.models[0].state=='live')
        self.assertFalse(receiver.gaps);receiver.stop();await task;await self.finish_raw(lane)
        bodies=[r.body for r in self.records(lane) if r.kind=='ws_message']
        self.assertIn(canonical(foreign),bodies)
        self.assertEqual(receiver.models[0].view()['bids'],[('100','9')])

    async def test_raw_snapshot_is_not_applied_until_durable_ack_even_on_cancel(self):
        lane=await self.ready();receiver,_=self.receiver(lane)
        entered,release=threading.Event(),threading.Event();self.addCleanup(release.set)
        append=lane.writer.append
        def blocked(record):
            if record.kind=='ws_message' and b'"type":"snapshot"' in record.body:
                entered.set()
                if not release.wait(5):raise OSError('test gate timeout')
            return append(record)
        with patch.object(lane.writer,'append',side_effect=blocked):
            task=self.launch(receiver);self.socket.put(self.frame(receiver))
            self.assertTrue(await asyncio.to_thread(entered.wait,2))
            self.assertEqual(receiver.models[0].state,'bootstrapping')
            task.cancel();await asyncio.sleep(0);self.assertFalse(task.done());release.set()
            with self.assertRaises(asyncio.CancelledError):await asyncio.wait_for(task,5)
        self.assertEqual(receiver.models[0].state,'live');self.assertTrue(self.socket.closed)
        await self.finish_raw(lane)

    async def test_malformed_native_bytes_are_archived_and_mapped_before_stop(self):
        lane=await self.ready();receiver,_=self.receiver(lane);task=self.launch(receiver)
        self.socket.put(b'{invalid')
        with self.assertRaisesRegex(DeribitStopped,'native_resync_required'):await task
        self.assertEqual(len(receiver.gaps),1)
        await self.finish_raw(lane)
        self.assertIn(b'{invalid',[r.body for r in self.records(lane)])

    async def test_proof_bound_is_rejected_before_any_capture_or_subscription(self):
        lane=await self.ready()
        with self.assertRaisesRegex(ValueError,'proof headroom'):
            self.receiver(lane,max_frames=128)
        self.assertTrue(lane.health()['accepting'])
        await self.finish_raw(lane)
        self.assertFalse(any(r.channel=='owner_plans:capture' for r in self.records(lane)))

    async def test_rejected_ack_is_retained_and_socket_stops_unhealthy(self):
        lane=await self.ready();receiver,_=self.receiver(lane)
        self.socket.put(dict(jsonrpc='2.0',id=1,error=dict(code=1,message='test rejection')))
        task=self.launch(receiver)
        with self.assertRaisesRegex(DeribitStopped,'native_resync_required'):await task
        self.assertFalse(receiver._acked);self.assertTrue(self.socket.closed)
        await self.finish_raw(lane)
        self.assertTrue(any(b'test rejection' in r.body for r in self.records(lane)))

    async def two_bindings(self):
        lane=await self.ready();binding=self.plan.bindings[0]
        instrument=replace(binding.target.instrument,native_symbol='ETH-SYNTHETIC-C')
        attrs=dict(base_currency='ETH',amount_unit='native',quote_currency='native')
        raw=self.received(lane)
        record=replace(raw,instrument=instrument,kind='metadata_version',channel='metadata',
            receipt=replace(raw.receipt,connection_id='meta2'),
            body=canonical(dict(schema='dc.metadata/2',effective_at_ns=0,attributes=attrs,supersedes=None)))
        ack=await lane.submit_received(record)
        metadata=MetadataVersion(instrument,replace(record.receipt,clock_epoch=ack.clock_epoch),0,canonical(attrs))
        second=replace(binding,target=FeedTarget(instrument,'deribit_full','owner-eth'),
            metadata_source=record.receipt.stream_key,metadata_version_id=metadata.version_id)
        self.plan=replace(self.plan,bindings=(binding,second))
        return lane

    async def test_multiplexed_gap_corrects_only_affected_binding(self):
        lane=await self.two_bindings();instrument=self.plan.bindings[1].target.instrument
        receiver,pipeline=self.receiver(lane);task=self.launch(receiver)
        self.socket.put(self.frame(receiver));self.socket.put(self.frame(receiver,symbol=instrument.native_symbol))
        await self.until(lambda:receiver._acked and all(m.state=='live' for m in receiver.models))
        await pipeline.run()
        self.socket.put(self.frame(receiver,snapshot=False,gap=True))
        with self.assertRaisesRegex(DeribitStopped,'native_resync_required'):await task
        self.assertEqual(len(receiver.gaps),1)
        self.assertEqual([m.state for m in receiver.models],['invalid','live'])
        await self.finish_raw(lane)
        report=next(strict_json(r.body) for r in self.records(lane) if r.channel=='snapshot_deliveries:capture')
        result=export_snapshots(pipeline.publisher.root/'snapshots',self.root/'exports',[report['snapshot_id']],lane.segments)
        self.assertEqual(result['descriptor']['population']['invalid'],1)
        self.assertEqual(result['descriptor']['population']['valid'],1)

    async def test_byte_crossing_gap_is_classified_before_resource_stop(self):
        lane=await self.ready();receiver,pipeline=self.receiver(lane,max_bytes=2500)
        task=self.launch(receiver);self.socket.put(self.frame(receiver))
        await self.until(lambda:receiver._acked and receiver.models[0].state=='live')
        await pipeline.run()
        gap=self.frame(receiver,snapshot=False,gap=True);gap['padding']='x'*2200
        self.socket.put(gap)
        with self.assertRaisesRegex(DeribitStopped,'native_resync_required'):await task
        self.assertGreater(receiver.health()['bytes'],receiver.limits.max_bytes)
        self.assertEqual(len(receiver.gaps),1)

    async def test_catalogue_channel_and_archive_envelope_caps_fail_before_connect(self):
        lane=await self.ready();binding=self.plan.bindings[0]
        self.plan=replace(self.plan,bindings=tuple(replace(binding,target=replace(binding.target,
            instrument=replace(binding.target.instrument,native_symbol=f'OPTION-{i}'))) for i in range(51)))
        with self.assertRaisesRegex(ValueError,'bounded local'):self.receiver(lane)
        self.plan=replace(self.plan,bindings=(binding,));self.counter+=1
        with self.assertRaisesRegex(ValueError,'archive queue/writer'):
            self.receiver(lane,max_message_bytes=2*1024*1024)
        self.assertTrue(lane.health()['accepting'])

    async def test_frame_cap_stops_without_reading_an_unbudgeted_delivery(self):
        lane=await self.ready();receiver,_=self.receiver(lane,max_frames=1);task=self.launch(receiver)
        await task
        self.assertEqual(receiver.health()['frames'],1)
        self.assertEqual(receiver.health()['reason'],'session_frame_bound')
        self.assertGreater(self.socket.incoming.qsize(),0)
        self.assertTrue(self.socket.closed)

    async def test_closed_transport_is_recorded_without_fabricating_native_gap(self):
        lane=await self.ready();receiver,_=self.receiver(lane)
        self.socket.incoming.put_nowait(aiohttp.WSMessage(aiohttp.WSMsgType.CLOSED,None,''))
        task=self.launch(receiver)
        with self.assertRaisesRegex(DeribitStopped,'socket_closed_or_error'):await task
        self.assertFalse(receiver.gaps);self.assertTrue(self.socket.closed)
        await self.finish_raw(lane)
        self.assertTrue(any(b'socket_event' in r.body for r in self.records(lane)))

    async def test_health_excludes_idle_socket_before_timer_task_resumes(self):
        lane=await self.ready();receiver,_=self.receiver(lane);task=self.launch(receiver)
        self.socket.put(self.frame(receiver))
        await self.until(lambda:receiver._acked and receiver.models[0].state=='live')
        self.assertIn(receiver.source,receiver.healthy_streams_for_plan(receiver.plan))
        self.mono[0]+=receiver.limits.idle_timeout_seconds*1_000_000_000
        self.assertNotIn(receiver.source,receiver.healthy_streams_for_plan(receiver.plan))
        receiver.stop();await task

    async def test_slow_apply_stops_before_next_healthy_publication(self):
        lane=await self.ready();receiver,_=self.receiver(lane);apply=receiver._apply
        async def slow(record,value):
            await apply(record,value);self.mono[0]+=5_000_000_001
        with patch.object(receiver,'_apply',side_effect=slow):
            task=self.launch(receiver);self.socket.put(self.frame(receiver))
            with self.assertRaisesRegex(DeribitStopped,'receive_apply_deadline'):await task
        self.assertNotIn(receiver.source,receiver.healthy_streams_for_plan(receiver.plan))
        self.assertFalse(lane.health()['accepting'])

    async def test_close_failure_is_exposed_and_halts_capture(self):
        lane=await self.ready();receiver,_=self.receiver(lane);task=self.launch(receiver)
        await self.until(lambda:receiver._acked)
        async def close():raise OSError('test close failure')
        with patch.object(self.socket,'close',side_effect=close):
            receiver.stop()
            with self.assertRaisesRegex(OSError,'test close failure'):await task
        self.assertEqual(receiver.reason,'socket_close_unconfirmed')
        self.assertFalse(lane.health()['accepting'])

    async def test_global_error_with_book_channel_corrects_every_affected_binding(self):
        lane=await self.two_bindings();receiver,pipeline=self.receiver(lane);task=self.launch(receiver)
        self.socket.put(self.frame(receiver))
        self.socket.put(self.frame(receiver,symbol=self.plan.bindings[1].target.instrument.native_symbol))
        await self.until(lambda:receiver._acked and all(m.state=='live' for m in receiver.models))
        await pipeline.run()
        value=self.frame(receiver,snapshot=False)
        value['error']=dict(code=1,message='global subscription error')
        self.socket.put(value)
        with self.assertRaisesRegex(DeribitStopped,'native_resync_required'):await task
        self.assertEqual(len(receiver.gaps),2)
        await self.finish_raw(lane)
        report=next(strict_json(r.body) for r in self.records(lane) if r.channel=='snapshot_deliveries:capture')
        result=export_snapshots(pipeline.publisher.root/'snapshots',self.root/'exports',[report['snapshot_id']],lane.segments)
        self.assertEqual(result['descriptor']['population']['invalid'],2)

    async def test_receipt_is_admitted_in_socket_return_turn_before_other_callbacks(self):
        lane=await self.ready();receiver,_=self.receiver(lane);receive=self.socket.receive
        expected=self.mono[0]
        def advance():self.mono[0]+=1
        async def incoming():
            message=await receive()
            if b'"type":"snapshot"' in message.data:
                asyncio.get_running_loop().call_soon(advance)
            return message
        with patch.object(self.socket,'receive',side_effect=incoming):
            task=self.launch(receiver);body=self.frame(receiver);self.socket.put(body)
            await self.until(lambda:receiver._acked and receiver.models[0].state=='live')
            receiver.stop();await task
        await self.finish_raw(lane)
        saved=next(r for r in self.records(lane) if r.receipt.stream_key==receiver.source and r.body==canonical(body))
        self.assertEqual(saved.receipt.monotonic_ns,expected)
        self.assertGreater(self.mono[0],expected)

    async def test_full_final_message_overshoot_requires_scan_headroom_before_capture(self):
        # A small cumulative byte budget can still admit one full transport frame.
        # Use genuinely capable archive/queue limits to isolate replay headroom.
        with patch.object(lane_fixtures,'JournalLimits',side_effect=lambda **kw:JournalLimits(
                max_record_bytes=4*1024*1024,target_bytes=8*1024*1024,max_streams=16)), \
             patch.object(lane_fixtures,'SegmentLimits',side_effect=lambda **kw:SegmentLimits(
                max_record_bytes=4*1024*1024,max_uncompressed_bytes=32*1024*1024,max_compressed_bytes=32*1024*1024)), \
             patch.object(lane_fixtures,'QueueLimits',side_effect=lambda **kw:QueueLimits(
                max_bytes=16*1024*1024,max_items=8,max_item_bytes=4*1024*1024)):
            lane=await self.ready()
        lane.snapshot_limits=replace(lane.snapshot_limits,max_scan_bytes=1_500_000)
        with self.assertRaisesRegex(ValueError,'native replay scan headroom'):
            self.receiver(lane,max_bytes=10_000,max_message_bytes=2*1024*1024)
        self.assertTrue(lane.health()['accepting'])
        await self.finish_raw(lane)
        self.assertFalse(any(r.channel=='owner_plans:capture' for r in self.records(lane)))
