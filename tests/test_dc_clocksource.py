import asyncio
import base64
import gc
from decimal import Decimal
import os
import sys
import threading
import tracemalloc
import unittest
from unittest.mock import patch

from data_collector.clocksource import ChronyClockSampler, read_tracking, tracking_error_ns
from data_collector.clocklog import DEFAULT_POLICY, SOURCE, replay_clock
from data_collector.contracts import Provenance
from data_collector.journal import Journal, JournalLimits
from data_collector.lane import ArchiveLane
from data_collector.queues import ByteQueue
from data_collector.segments import SegmentLimits, iter_segment_records
from data_collector.writer import SessionWriter
from data_collector.contracts import strict_json
from tests import test_dc_lane as base


def tracking(utc,**changes):
    fields=['CB00710F','192.0.2.1','3',str(Decimal(utc)/1_000_000_000),'0','0','0','0','0','0','0','0','64.2','Normal']
    for key,value in changes.items():fields[int(key[1:])]=value
    return (','.join(fields)+'\n').encode()


class ClockSourceTests(unittest.IsolatedAsyncioTestCase):
    setUp=base.LaneTests.setUp
    observe=base.LaneTests.observe
    lane=base.LaneTests.lane
    start=base.LaneTests.start
    records=base.LaneTests.records
    clock_policy=base.LaneTests.clock_policy
    received=base.LaneTests.received
    record=base.LaneTests.record

    async def wait(self,deadline):self.mono[0]=deadline
    async def read(self,*args):return 0,tracking(self.observe()[0]),b''
    async def setup_sampler(self,**kwargs):
        lane,_=self.lane(clock_policy=self.clock_policy());self.start(lane)
        sampler=ChronyClockSampler(lane,read=kwargs.pop('read',self.read),wait_until=kwargs.pop('wait_until',self.wait),**kwargs)
        self.addCleanup(sampler.stop)
        return lane,sampler
    async def finish(self,lane):
        lane.stop();await asyncio.wait_for(lane.test_task,5)
    def replay(self,lane):
        return replay_clock(self.records(lane),venue='deribit',environment='production',capture_id='capture',
            process_session_id='process',provenance=lane.provenance,policy=self.clock_policy(),expected_pin=lane._clock.pin)

    async def test_recurring_sources_replay_two_spaced_healthy_observations(self):
        lane,sampler=await self.setup_sampler(max_samples=3)
        result=await sampler.run();self.assertEqual(result['samples'],3)
        self.assertTrue(sampler.last_ack.assessment.aligned)
        await self.finish(lane);replay=self.replay(lane)
        self.assertEqual(replay.assessment,sampler.last_ack.assessment)
        sources=[r for r in self.records(lane) if r.receipt.connection_id==SOURCE]
        self.assertEqual(len(sources),3)
        for source in sources:
            evidence=strict_json(source.body);self.assertTrue(evidence['output_complete'])
            self.assertEqual(base64.b64decode(evidence['stdout_base64']),tracking(source.receipt.receive_utc_ns))
        self.assertEqual(replay.assessment.anchor.healthy_samples,2)
        with self.assertRaisesRegex(ValueError,'once'):await sampler.run()

    async def test_unbounded_sampling_exceeds_old_cap_with_bounded_memory(self):
        # Exercise sampler, lane admission and durable clock log past all three
        # former 10,000-operation caps, with a stable clock and byte rotation.
        root=self.root/'long-clock'
        provenance=Provenance('a'*64,'b'*64,'c'*64)
        writer=SessionWriter(root/'wal',root/'archive',venue='deribit',environment='production',
            capture_id='capture',process_session_id='process',provenance=provenance,
            started_utc_ns=self.utc,now_ns=lambda:self.mono[0],rotate_ns=10**18,
            limits=JournalLimits(max_record_bytes=131072,target_bytes=16*1024*1024),
            segment_limits=SegmentLimits(max_record_bytes=131072,
                max_uncompressed_bytes=32*1024*1024,max_compressed_bytes=32*1024*1024))
        self.addCleanup(writer.abort)
        emergency=Journal(root/'emergency.wal',venue='deribit',environment='production',create=True)
        self.addCleanup(emergency.close)
        lane=ArchiveLane(writer,ByteQueue(),emergency_writer=emergency,observe=self.observe,
            clock_policy=DEFAULT_POLICY,clock_max_assessments=None,max_connections=4)
        raw=asyncio.create_task(lane.run())
        baseline=None
        owned_trace=not tracemalloc.is_tracing()
        if owned_trace:tracemalloc.start()
        async def wait(deadline):
            nonlocal baseline
            self.mono[0]=deadline
            if sampler.samples==100:
                gc.collect()
                baseline=tracemalloc.get_traced_memory()[0]
            if sampler.samples==10002:
                self.assertFalse(task.done())
                # Compare live retained allocations, excluding Python free lists
                # and garbage awaiting a collection, rather than allocator RSS.
                gc.collect()
                retained=tracemalloc.get_traced_memory()[0]-baseline
                self.assertLess(retained,512*1024)
                sampler.stop()
        sampler=ChronyClockSampler(lane,max_samples=None,read=self.read,wait_until=wait)
        task=asyncio.create_task(sampler.run())
        try:
            result=await task
            self.assertEqual(result['samples'],10002)
            self.assertEqual(lane._clock.pin.count,10002)
            self.assertEqual(lane._clock.prior_anchors,())
            self.assertTrue(sampler.last_ack.assessment.aligned)
            self.assertLessEqual(len(writer.state.positions),4)
        finally:
            sampler.stop()
            await asyncio.gather(task,return_exceptions=True)
            if owned_trace:tracemalloc.stop()
            lane.stop()
            await raw
            emergency.close();writer.abort()
        def records():
            for path in lane.segments:yield from iter_segment_records(path)
        replay=replay_clock(records(),venue='deribit',environment='production',capture_id='capture',
            process_session_id='process',provenance=provenance,policy=DEFAULT_POLICY,
            expected_pin=lane._clock.pin,max_assessments=10002)
        self.assertEqual(replay.assessment,sampler.last_ack.assessment)

    async def test_unavailable_measurement_is_durable_unaligned_then_recovers(self):
        calls=[]
        async def read(*args):
            calls.append(1)
            if len(calls)==1:return 1,b'provider refused\n',b'not running\n'
            return await self.read()
        lane,sampler=await self.setup_sampler(max_samples=4,read=read)
        result=await sampler.run();self.assertEqual(result['failures'],1)
        self.assertTrue(sampler.last_ack.assessment.aligned)
        await self.finish(lane);self.assertTrue(self.replay(lane).assessment.aligned)
        source=next(r for r in self.records(lane) if r.receipt.connection_id==SOURCE)
        body=strict_json(source.body);self.assertEqual(base64.b64decode(body['stderr_base64']),b'not running\n')
        assessments=[strict_json(r.body) for r in self.records(lane) if r.channel=='clock_assessments']
        self.assertFalse(assessments[0]['assessment']['aligned'])
        self.assertEqual(assessments[0]['assessment']['reason'],'reference_offset_or_uncertainty')

    async def test_ingress_during_command_does_not_regress_clock_admission(self):
        holder={};calls=[]
        async def read(*args):
            calls.append(1)
            if len(calls)==2:
                self.mono[0]+=1
                await holder['lane'].submit_received(self.received(holder['lane']))
            return await self.read()
        lane,sampler=await self.setup_sampler(max_samples=3,read=read);holder['lane']=lane
        await sampler.run();self.assertTrue(lane.health()['accepting'])
        await self.finish(lane);self.replay(lane)
        bodies=[strict_json(r.body) for r in self.records(lane) if r.receipt.connection_id==SOURCE]
        self.assertEqual(bodies[1]['completed']['monotonic_ns']-bodies[1]['start']['monotonic_ns'],1)
        self.assertGreaterEqual(bodies[1]['uncertainty_ns'],4)

    async def test_cancellation_waits_for_admitted_clock_pair(self):
        lane,sampler=await self.setup_sampler(max_samples=3)
        entered,release=threading.Event(),threading.Event();self.addCleanup(release.set)
        original=lane.writer.append
        def append(record):
            if record.channel=='clock_assessments':
                entered.set()
                if not release.wait(3):raise OSError('test timeout')
            return original(record)
        with patch.object(lane.writer,'append',side_effect=append):
            task=asyncio.create_task(sampler.run())
            self.assertTrue(await asyncio.to_thread(entered.wait,2))
            task.cancel();await asyncio.sleep(0);self.assertFalse(task.done())
            release.set()
            with self.assertRaises(asyncio.CancelledError):await task
        self.assertTrue(sampler._terminal);self.assertEqual(sampler.samples,1)
        await self.finish(lane);self.replay(lane)

    async def test_stop_during_timer_and_early_timer_failure(self):
        entered=asyncio.Event()
        async def wait(deadline):entered.set();await asyncio.Event().wait()
        lane,sampler=await self.setup_sampler(max_samples=3,wait_until=wait)
        task=asyncio.create_task(sampler.run());await asyncio.wait_for(entered.wait(),5)
        sampler.stop();self.assertEqual((await task)['samples'],1)
        await self.finish(lane);self.replay(lane)
        async def early(deadline):pass
        lane,sampler=await self.setup_sampler(max_samples=3,wait_until=early)
        with self.assertRaisesRegex(ValueError,'timer woke early'):await sampler.run()
        self.assertFalse(lane.health()['accepting'])

    def test_tracking_schema_staleness_local_reference_leaps_and_rounding(self):
        utc=self.utc
        valid=tracking(utc,f4='0.0000000001',f10='0.0000000001',f11='0.0000000001')
        self.assertEqual(tracking_error_ns(valid,utc_ns=utc,max_reference_age_ns=100),4)
        for body in (tracking(utc,f0='7F7F0101'),tracking(utc,f2='0'),tracking(utc,f13='Not synchronised'),
                tracking(utc,f4='NaN'),tracking(utc,f11='-1'),tracking(utc-1000),tracking(utc+1),
                tracking(utc)+tracking(utc),b'\xff'):
            with self.subTest(body=body),self.assertRaises((ValueError,UnicodeError)):
                tracking_error_ns(body,utc_ns=utc,max_reference_age_ns=100)

    async def test_real_subprocess_bounds_timeout_and_cancellation_reap(self):
        def executable(name,body):
            path=self.root/name;path.write_text('#!'+sys.executable+'\n'+body);path.chmod(0o700);return str(path)
        good=executable('good','import sys\nsys.stdout.buffer.write(b"exact bytes\\x00")\n')
        self.assertEqual(await read_tracking(good,1,128),(0,b'exact bytes\x00',b''))
        large=executable('large','import sys\nsys.stdout.buffer.write(b"x"*10000)\nsys.stdout.flush()\n')
        with self.assertRaises(ValueError):await read_tracking(large,1,128)
        pidfile=self.root/'pid'
        slow=executable('slow','import os,time\nfrom pathlib import Path\nPath('+repr(str(pidfile))+').write_text(str(os.getpid()))\ntime.sleep(60)\n')
        with self.assertRaises(TimeoutError):await read_tracking(slow,0.1,128)
        if pidfile.exists():
            pid=int(pidfile.read_text())
            with self.assertRaises(ProcessLookupError):os.kill(pid,0)
            pidfile.unlink()
        task=asyncio.create_task(read_tracking(slow,10,128))
        async def ready():
            while not pidfile.exists():await asyncio.sleep(0.001)
        await asyncio.wait_for(ready(),3);pid=int(pidfile.read_text());task.cancel()
        with self.assertRaises(asyncio.CancelledError):await task
        with self.assertRaises(ProcessLookupError):os.kill(pid,0)
