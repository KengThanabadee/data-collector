import asyncio
from dataclasses import replace
import threading
import unittest
from unittest.mock import patch

from data_collector.contracts import strict_json
from data_collector.lane import DurabilityUnknown
from tests.helpers import LaneHarness


class RawBatchTests(LaneHarness, unittest.IsolatedAsyncioTestCase):
    async def test_fsync_precedes_all_acks_and_clock_barrier_splits_batches(self):
        lane,_=self.lane(clock_policy=self.clock_policy());lane.raw_batch_size=512;self.start(lane)
        await lane.submit_clock(*self.clock_request(lane))
        self.mono[0]+=60
        await lane.submit_clock(*self.clock_request(lane,1))
        entered,release=threading.Event(),threading.Event();self.addCleanup(release.set)
        sync=lane.writer.active.sync
        def gated_sync():
            entered.set()
            if not release.wait(3):raise OSError('fixture gate timeout')
            return sync()
        with patch.object(lane.writer.active,'sync',side_effect=gated_sync):
            originals=(self.received(lane,0),self.received(lane,1))
            before=[lane.submit_received(r) for r in originals]
            self.utc+=10;self.mono[0]+=1
            boundary=self.clock_check(lane)
            last=self.received(lane,2);after=lane.submit_received(last)
            self.assertTrue(await asyncio.to_thread(entered.wait,2))
            self.assertFalse(any(f.done() for f in before+[boundary,after]))
            self.assertGreater(lane.queue.budget.used,0)
            release.set()
            a,b,clock,c=await asyncio.gather(*before,boundary,after)
        self.assertEqual(a.clock_epoch,b.clock_epoch)
        self.assertNotEqual(a.clock_epoch,c.clock_epoch)
        self.assertEqual(c.clock_epoch,clock.assessment.anchor.clock_epoch)
        lane.stop();await lane.test_task
        received=[r for r in self.records(lane) if r.kind=='ws_message']
        self.assertEqual([r.body for r in received],[r.body for r in (*originals,last)])
        self.assertEqual([strict_json(r.native_fields_json)['collector_ingress_ordinal'] for r in received],[0,1,2])
        self.assertEqual(lane.queue.budget.used,0)

    async def test_failed_group_fsync_acknowledges_nothing_and_keeps_charges(self):
        lane,_=self.lane(clock_policy=self.clock_policy());lane.raw_batch_size=512;self.start(lane)
        await lane.submit_clock(*self.clock_request(lane))
        with patch.object(lane.writer.active,'sync',side_effect=OSError('disk full')):
            futures=[lane.submit_received(self.received(lane,i)) for i in range(2)]
            outcomes=await asyncio.gather(*futures,return_exceptions=True)
            await lane.test_task
        self.assertTrue(all(isinstance(v,DurabilityUnknown) for v in outcomes))
        self.assertGreater(lane.queue.budget.used,0)
        self.assertEqual(lane.health()['durable_records'],0)
