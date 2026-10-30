import asyncio
from dataclasses import replace
import unittest

from data_collector.queues import ByteBudget, ByteQueue, QueueBacklog, QueueCapacity, QueueClosed, QueueLimits, VenueQueues


class QueueTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        self.clock = [100]
        self.limits = QueueLimits(max_bytes=4096, max_items=4, max_item_bytes=1024, max_age_ns=5_000_000_000)

    def queue(self, **kwargs):
        return ByteQueue(self.limits, now_ns=lambda:self.clock[0], **kwargs)

    async def test_group_lease_keeps_budget_and_stops_at_control_barrier(self):
        q=self.queue()
        for value in (b'a',b'b',b'barrier',b'c'):q.offer(value)
        first=await q.get();charged=q.budget.used
        leases=q.expand_lease(first,max_items=512,max_bytes=1024,accept=lambda ordinal:ordinal<2)
        self.assertEqual([l.payload for l in leases],[b'a',b'b'])
        self.assertEqual(q.budget.used,charged)
        self.assertEqual(q.health()['leased'],2)
        with self.assertRaises(QueueCapacity):q.offer(b'overflow')
        with self.assertRaises(ValueError):q.ack(first)
        with self.assertRaises(ValueError):q.ack_batch(tuple(list(leases)))
        q.ack_batch(leases)
        self.assertLess(q.budget.used,charged)
        barrier=await q.get();self.assertEqual(barrier.payload,b'barrier');q.ack(barrier)
        last=await q.get();self.assertEqual(last.payload,b'c');q.ack(last)
        self.assertEqual(q.budget.used,0)

    async def test_dequeue_keeps_bytes_and_count_reserved_until_durable_ack(self):
        q = ByteQueue(QueueLimits(max_bytes=1024, max_items=1, max_item_bytes=200), now_ns=lambda:self.clock[0])
        q.offer(b'x'*200)
        before = q.budget.used
        lease = await q.get()
        self.assertEqual(q.budget.used, before)
        with self.assertRaises(QueueCapacity): q.offer(b'next')
        with self.assertRaises(ValueError): await q.get()
        q.ack(lease)
        self.assertEqual(q.budget.used, 0)
        q.offer(b'next')
        q.ack(await q.get())

    async def test_lease_identity_and_retry_preserve_fifo_and_charge(self):
        q = self.queue()
        q.offer(b'first'); q.offer(b'second')
        first = await q.get()
        with self.assertRaises(ValueError): q.ack(replace(first))
        charged = q.budget.used
        q.nack(first)
        retry = await q.get()
        self.assertEqual((retry.payload,retry.ordinal), (first.payload,first.ordinal))
        self.assertEqual(q.budget.used, charged)
        with self.assertRaises(ValueError): q.ack(first)
        q.ack(retry)
        with self.assertRaises(ValueError): q.ack(retry)
        second = await q.get()
        self.assertEqual(second.payload,b'second')
        q.ack(second)
        self.assertEqual(q.budget.used,0)

    async def test_backlog_rejects_new_ingress_but_preserves_late_payload_for_archiving(self):
        q = self.queue()
        q.offer(b'original')
        self.clock[0] += 5_000_000_000
        with self.assertRaises(QueueBacklog): q.offer(b'would be lost')
        self.assertTrue(q.health()['warning'])
        self.assertFalse(q.health()['resubscribe_allowed'])
        lease = await q.get()
        self.assertEqual(lease.payload,b'original')
        self.assertTrue(lease.late)
        q.ack(lease)
        self.assertTrue(q.health()['resubscribe_allowed'])

    async def test_close_wakes_waiter_and_drains_accepted_items(self):
        empty = self.queue()
        waiting = asyncio.create_task(empty.get())
        await asyncio.sleep(0)
        empty.close()
        with self.assertRaises(QueueClosed): await waiting
        q = self.queue(); q.offer(b'kept'); q.close()
        with self.assertRaises(QueueClosed): q.offer(b'new')
        lease = await q.get(); self.assertEqual(lease.payload,b'kept'); q.ack(lease)
        with self.assertRaises(QueueClosed): await q.get()

    async def test_waiter_cancellation_does_not_consume_future_payload(self):
        q = self.queue()
        task = asyncio.create_task(q.get())
        await asyncio.sleep(0)
        with self.assertRaises(ValueError): await q.get()
        task.cancel()
        with self.assertRaises(asyncio.CancelledError): await task
        q.offer(b'next')
        lease = await q.get(); self.assertEqual(lease.payload,b'next'); q.ack(lease)

    async def test_venue_local_overflow_does_not_consume_other_venue_allocation(self):
        limits = QueueLimits(max_bytes=1024,max_items=1,max_item_bytes=200)
        fleet = VenueQueues({'deribit':limits,'bybit':limits},aggregate_bytes=2048,now_ns=lambda:self.clock[0])
        d,b = fleet.queues['deribit'],fleet.queues['bybit']
        d.offer(b'x'*200)
        with self.assertRaises(QueueCapacity): d.offer(b'overflow')
        b.offer(b'y'*200)
        self.assertEqual(fleet.budget.used,1424)
        d.close()
        lease = await b.get(); b.ack(lease)
        self.assertEqual(fleet.budget.used,712)
        lease = await d.get(); d.ack(lease)
        self.assertEqual(fleet.budget.used,0)
        with self.assertRaises(ValueError): VenueQueues({'deribit':limits,'bybit':limits},aggregate_bytes=1024)

    async def test_global_byte_budget_rejection_and_oversize_do_not_leak_reservations(self):
        budget = ByteBudget(4096)
        one,two = self.queue(budget=budget),self.queue(budget=budget)
        for _ in range(2): one.offer(b'x'*1024)
        with self.assertRaises(QueueCapacity): two.offer(b'y'*1024)
        before = budget.used
        with self.assertRaises(QueueCapacity): one.offer(b'x'*1025)
        with self.assertRaises(ValueError): one.offer(bytearray(b'mutable'))
        self.assertEqual(budget.used,before)
        for _ in range(2): one.ack(await one.get())
        self.assertEqual(budget.used,0)

    async def test_clock_regression_closes_ingress_without_releasing_volatile_tail(self):
        q = self.queue(); q.offer(b'received'); lease = await q.get()
        before = q.budget.used
        self.clock[0] -= 1
        with self.assertRaises(ValueError): q.health()
        with self.assertRaises(QueueClosed): q.offer(b'new')
        self.assertEqual(q.budget.used,before)
        q.ack(lease)
        self.assertEqual(q.budget.used,0)

    async def test_clock_failure_preserves_unleased_tail_for_conservative_fifo_drain(self):
        q = self.queue(); q.offer(b'first'); q.offer(b'second')
        self.clock[0] -= 1
        with self.assertRaises(ValueError): q.health()
        health = q.health()
        self.assertFalse(health['clock_valid'])
        self.assertIsNone(health['oldest_age_ns'])
        self.assertFalse(health['resubscribe_allowed'])
        self.assertTrue(health['warning'])
        for body in [b'first',b'second']:
            lease = await q.get()
            self.assertEqual(lease.payload,body)
            self.assertTrue(lease.late)
            self.assertFalse(lease.clock_valid)
            q.ack(lease)
        self.assertEqual(q.budget.used,0)
        with self.assertRaises(QueueClosed): await q.get()

    async def test_get_detects_fault_and_drains_without_fabricating_a_healthy_clock(self):
        q = self.queue(); q.offer(b'preserved')
        self.clock[0] -= 1
        lease = await q.get()
        self.assertEqual(lease.payload,b'preserved')
        self.assertFalse(lease.clock_valid)
        self.assertTrue(lease.late)
        q.nack(lease)
        retry = await q.get()
        self.assertFalse(retry.clock_valid)
        q.ack(retry)

    async def test_associated_waiter_metadata_stays_charged_through_lease(self):
        q=ByteQueue(QueueLimits(max_bytes=1024,max_items=2,max_item_bytes=200))
        with self.assertRaises(ValueError):q.offer(b'x',extra_bytes=-1)
        q.offer(b'x'*100,extra_bytes=400)
        self.assertEqual(q.budget.used,1012)
        lease=await q.get()
        with self.assertRaises(QueueCapacity):q.offer(b'y')
        self.assertEqual(q.budget.used,1012)
        q.ack(lease)
        self.assertEqual(q.budget.used,0)


if __name__ == '__main__': unittest.main()
