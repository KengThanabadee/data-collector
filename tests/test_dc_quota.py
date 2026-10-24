import asyncio
import unittest

from data_collector.quota import Priority, PriorityQuota, QuotaCapacity, QuotaClosed, QuotaTimeout, QuotaWindow

S = 1_000_000_000


class QuotaTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        self.clock = [0]

    def quota(self, windows=None, **kwargs):
        q = PriorityQuota('deribit', windows or [QuotaWindow('rest',1,10*S)], now_ns=lambda:self.clock[0], **kwargs)
        self.addAsyncCleanup(q.close)
        return q

    async def pending(self, q, count):
        for _ in range(50):
            if q.health()['pending'] == count:
                return
            await asyncio.sleep(0)
        self.fail(f'expected {count} pending requests, got {q.health()}')

    def request(self, q, charges=None, **kwargs):
        return asyncio.create_task(q.acquire(charges or {'rest':1}, deadline_ns=100*S, **kwargs))

    def advance(self, q, seconds):
        self.clock[0] += seconds*S
        q.notify()

    async def test_rolling_weight_never_replenishes_early_and_waits_exact_boundary(self):
        q = self.quota([QuotaWindow('rest',3,10*S)])
        first = await q.acquire({'rest':2},deadline_ns=100*S)
        self.assertEqual(first.charges,(('rest',2),))
        waiting = self.request(q,{'rest':2})
        await self.pending(q,1)
        self.advance(q,9)
        await asyncio.sleep(0)
        self.assertFalse(waiting.done())
        self.advance(q,1)
        grant = await asyncio.wait_for(waiting,1)
        self.assertEqual(grant.granted_ns,10*S)
        self.assertEqual(q.health()['pools']['rest']['charged'],2)

    async def test_recovery_precedes_earlier_backfill_and_priorities_share_one_pool(self):
        q = self.quota()
        await q.acquire({'rest':1},deadline_ns=100*S)
        backfill = self.request(q,priority=Priority.BACKFILL)
        live = self.request(q,priority=Priority.RECOVERY)
        await self.pending(q,2)
        self.advance(q,10)
        granted = await asyncio.wait_for(live,1)
        self.assertEqual(granted.granted_ns,10*S)
        self.assertFalse(backfill.done())
        self.advance(q,10)
        self.assertEqual((await asyncio.wait_for(backfill,1)).granted_ns,20*S)

    async def test_all_endpoint_constraints_are_charged_atomically(self):
        q = self.quota([QuotaWindow('rest',3,10*S),QuotaWindow('books',1,60*S)])
        await q.acquire({'rest':2,'books':1},deadline_ns=100*S)
        await q.acquire({'rest':1},deadline_ns=100*S)
        waiting = self.request(q,{'rest':1,'books':1})
        await self.pending(q,1)
        self.advance(q,10)
        for _ in range(5): await asyncio.sleep(0)
        self.assertFalse(waiting.done())
        self.assertEqual(q.health()['pools']['rest']['charged'],0)
        self.advance(q,50)
        await asyncio.wait_for(waiting,1)
        self.assertEqual(q.health()['pools']['rest']['charged'],1)
        self.assertEqual(q.health()['pools']['books']['charged'],1)

    async def test_deadline_expiry_does_not_charge_or_leave_waiter_metadata(self):
        q = self.quota()
        await q.acquire({'rest':1},deadline_ns=100*S)
        task = asyncio.create_task(q.acquire({'rest':1},deadline_ns=5*S))
        await self.pending(q,1)
        self.advance(q,5)
        with self.assertRaises(QuotaTimeout): await asyncio.wait_for(task,1)
        self.assertEqual(q.health()['pending_bytes'],0)
        self.assertEqual(q.health()['grants'],1)
        self.assertEqual(q.health()['timeouts'],1)

    async def test_cancelled_waiter_is_removed_without_refunding_existing_grant(self):
        q = self.quota()
        await q.acquire({'rest':1},deadline_ns=100*S)
        waiting = self.request(q)
        await self.pending(q,1)
        waiting.cancel()
        with self.assertRaises(asyncio.CancelledError): await waiting
        self.assertEqual(q.health()['pending_bytes'],0)
        self.assertEqual(q.health()['pools']['rest']['charged'],1)
        next_request = self.request(q)
        await self.pending(q,1)
        self.assertFalse(next_request.done())
        self.advance(q,10)
        await asyncio.wait_for(next_request,1)

    async def test_cancel_after_admission_conservatively_keeps_exchange_charge(self):
        q = self.quota()
        task = self.request(q)
        await asyncio.sleep(0)
        await asyncio.sleep(0)
        self.assertEqual(q.health()['grants'],1)
        self.assertFalse(task.done())
        task.cancel()
        with self.assertRaises(asyncio.CancelledError): await task
        self.assertEqual(q.health()['pools']['rest']['charged'],1)
        waiting = self.request(q)
        await self.pending(q,1)
        self.assertFalse(waiting.done())
        self.advance(q,10)
        await asyncio.wait_for(waiting,1)

    async def test_provider_cooldown_extends_all_lanes_and_cannot_be_shortened(self):
        q = self.quota()
        q.defer_until('rest',10*S)
        task = self.request(q,priority=Priority.RECOVERY)
        await self.pending(q,1)
        q.defer_until('rest',20*S)
        q.defer_until('rest',1*S)
        self.advance(q,10)
        for _ in range(5): await asyncio.sleep(0)
        self.assertFalse(task.done())
        self.advance(q,10)
        self.assertEqual((await asyncio.wait_for(task,1)).granted_ns,20*S)

    async def test_history_cap_bounds_memory_even_with_large_native_capacity(self):
        q = self.quota([QuotaWindow('rest',100,10*S)],max_history_per_pool=2)
        for _ in range(2): await q.acquire({'rest':1},deadline_ns=100*S)
        task = self.request(q)
        await self.pending(q,1)
        self.assertEqual(q.health()['pools']['rest']['history'],2)
        self.advance(q,10)
        await asyncio.wait_for(task,1)
        self.assertEqual(q.health()['pools']['rest']['history'],1)

    async def test_waiter_count_and_bytes_caps_reject_without_leaking(self):
        q = self.quota(max_pending=1)
        await q.acquire({'rest':1},deadline_ns=100*S)
        first = self.request(q)
        await self.pending(q,1)
        with self.assertRaises(QuotaCapacity): await q.acquire({'rest':1},deadline_ns=100*S)
        self.assertEqual(q.health()['pending'],1)
        first.cancel()
        with self.assertRaises(asyncio.CancelledError): await first
        small = self.quota(max_pending_bytes=1)
        with self.assertRaises(QuotaCapacity): await small.acquire({'rest':1},deadline_ns=100*S)
        self.assertEqual(small.health()['pending'],0)
        self.assertEqual(small.health()['pending_bytes'],0)

    async def test_clock_failure_closes_only_affected_venue_and_wakes_waiters(self):
        q = self.quota()
        self.clock[0] = 100
        await q.acquire({'rest':1},deadline_ns=100*S)
        waiting = self.request(q)
        await self.pending(q,1)
        self.clock[0] = 99
        q.notify()
        with self.assertRaises(QuotaClosed): await asyncio.wait_for(waiting,1)
        self.assertTrue(q.health()['closed'])
        self.assertEqual(q.health()['pending_bytes'],0)
        original_failure = q.health()['failure']
        self.assertIn('clock regressed',original_failure)
        await q.close()
        await q.close()
        self.assertEqual(q.health()['failure'],original_failure)
        other = PriorityQuota('bybit',[QuotaWindow('rest',1,10*S)],now_ns=lambda:self.clock[0])
        self.addAsyncCleanup(other.close)
        grant = await other.acquire({'rest':1},deadline_ns=100*S)
        self.assertEqual(grant.venue,'bybit')
        self.assertFalse(other.health()['closed'])

    async def test_waiter_uses_immutable_charges_and_input_validation_does_not_spend(self):
        q = self.quota()
        for charges in [{'rest':True},{'rest':2},{'unknown':1},{}]:
            with self.assertRaises(ValueError): await q.acquire(charges,deadline_ns=100*S)
        with self.assertRaises(QuotaTimeout): await q.acquire({'rest':1},deadline_ns=0)
        self.assertEqual(q.health()['grants'],0)
        await q.acquire({'rest':1},deadline_ns=100*S)
        charges = {'rest':1}
        waiting = asyncio.create_task(q.acquire(charges,deadline_ns=100*S))
        await self.pending(q,1)
        charges['rest'] = 2
        self.advance(q,10)
        self.assertEqual((await asyncio.wait_for(waiting,1)).charges,(('rest',1),))

    async def test_close_resolves_waiters_and_rejects_future_acquisitions(self):
        q = self.quota()
        await q.acquire({'rest':1},deadline_ns=100*S)
        pending = self.request(q)
        await self.pending(q,1)
        await q.close()
        with self.assertRaises(QuotaClosed): await pending
        with self.assertRaises(QuotaClosed): await q.acquire({'rest':1},deadline_ns=100*S)
        self.assertEqual(q.health()['pending_bytes'],0)


if __name__ == '__main__': unittest.main()
