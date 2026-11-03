import asyncio
from dataclasses import replace
import os
from pathlib import Path
import shutil
import time
import unittest
from unittest.mock import patch

from data_collector.app import run,verify_run,_freeze_source
from data_collector.config import load
from data_collector.contracts import canonical,strict_json
from data_collector.option_universe import option_partition
from data_collector.option_workers import capture_workers,worker_config
from tests import test_dc_dynamic_options as fixtures


def symbol(index,count=2,start=0):
    return next('BTC-'+str(i)+'-C' for i in range(start,start+1000)
        if option_partition('BTC-'+str(i)+'-C',count)==index)


class OptionWorkerTests(unittest.IsolatedAsyncioTestCase):
    setUp=fixtures.DynamicOptionsTests.setUp
    observe=fixtures.DynamicOptionsTests.observe
    clock_read=fixtures.DynamicOptionsTests.clock_read
    clock_wait=fixtures.DynamicOptionsTests.clock_wait
    setup_live=fixtures.DynamicOptionsTests.setup_live
    row=fixtures.DynamicOptionsTests.row
    wait_for=fixtures.DynamicOptionsTests.wait_for
    start_live=fixtures.DynamicOptionsTests.start_live
    cleanup_task=fixtures.DynamicOptionsTests.cleanup_task
    finish=fixtures.DynamicOptionsTests.finish
    records=fixtures.DynamicOptionsTests.records

    async def test_worker_follows_new_listings_in_its_partition_from_full_metadata(self):
        self.setup_live();self.config=replace(self.config,option_partition_count=2,option_partition_index=0)
        first,other=symbol(0),symbol(1)
        self.session.rows=[self.row(first,1),self.row(other,2)]
        task=await self.start_live()
        self.assertEqual(self.session.subscribed,{first})
        new=symbol(0,start=1000);foreign=symbol(1,start=1000)
        self.session.rows.extend([self.row(new,3),self.row(foreign,4)])
        self.session.notify(new);self.session.notify(foreign)
        await self.wait_for(lambda:new in self.session.subscribed)
        result=await self.finish(task)
        self.assertEqual(result['status'],'raw_capture_complete',result)
        self.assertEqual(self.session.subscribed,{first,new})
        revisions=[strict_json(r.body)['revision'] for r in self.records(result,metadata=True) if r.channel=='option_universe']
        self.assertTrue(all(r['worker_partition']==dict(count=2,index=0) for r in revisions))
        self.assertEqual({v['symbol'] for v in revisions[-1]['members']},{first,new})
        responses=[strict_json(r.body)['result'] for r in self.records(result,metadata=True) if r.kind=='rest_response']
        self.assertEqual(len(responses[-1]),4)
        self.assertTrue(verify_run(result['root'])['changing_universe_coverage_verified'])

    async def test_empty_local_partition_still_initializes_global_feeds(self):
        self.setup_live();self.config=replace(self.config,option_partition_count=2,option_partition_index=0)
        self.session.rows=[self.row(symbol(1),1)]
        task=await self.start_live();result=await self.finish(task)
        self.assertEqual(result['status'],'raw_capture_complete',result)
        self.assertFalse(self.session.subscribed)
        self.assertTrue(verify_run(result['root'])['changing_universe_coverage_verified'])

    async def test_metadata_and_subscriptions_share_request_reservations(self):
        self.setup_live();budget=self.root.parent/'budget.state';budget.write_bytes(b'0')
        self.config=replace(self.config,subscription_budget_file=budget)
        sent=[];original_get=self.session.get;original_send=self.session.socket.send_str
        def get(*args,**kwargs):sent.append(('http',time.monotonic()));return original_get(*args,**kwargs)
        async def send(body):
            if strict_json(body.encode()).get('method')=='public/subscribe':sent.append(('subscribe',time.monotonic()))
            await original_send(body)
        self.session.get=get;self.session.socket.send_str=send
        task=await self.start_live();result=await self.finish(task)
        self.assertEqual(result['status'],'raw_capture_complete',result)
        self.assertEqual([kind for kind,_ in sent],['subscribe','http','subscribe'])
        self.assertTrue(all(b[1]-a[1]>=.9 for a,b in zip(sent,sent[1:])),sent)

    async def _aggregate_fixture(self,stale_owner=False,late_foreign=False,already_expired=False,old_creation=False):
        self.setup_live();base=self.root;parent=replace(self.config,raw_processes=2)
        root=base/'runs'/'aggregate';root.mkdir(parents=True)
        source=_freeze_source(root);(root/'config.json').write_bytes(parent.to_bytes())
        report=dict(schema='dc.option-workers/1',status='raw_capture_complete',venue='deribit',
            environment=parent.environment,config_sha256=parent.config_sha256,source_sha256=source,
            original_root=str(root),physical_global_copies=2,workers=[])
        children=[];rows=[self.row(symbol(0),1),self.row(symbol(1),2)]
        extra=self.row(symbol(0 if late_foreign else 1,start=1000),3);subscribed=set()
        for index in range(2):
            self.root=root/'workers'/str(index)
            self.config=replace(worker_config(parent,root,index,root/'request-budget.state'),
                supervisor_pid=os.getppid(),subscription_budget_file=None)
            if late_foreign and index==1:
                if not old_creation:extra['creation_timestamp']=children[0]['started_utc_ns']//1_000_000+1
                if already_expired:
                    extra['expiration_timestamp']=children[0]['ended_utc_ns']//1_000_000-1
                    extra['is_active']=False
            include_extra=(stale_owner and index==0) or (late_foreign and index==1)
            self.session=fixtures.LiveSession();self.session.rows=rows+([extra] if include_extra else [])
            # The shared limiter is tested separately; retain its intended path
            # in frozen config while bypassing reservations in this fixture.
            self.config=replace(self.config,subscription_budget_file=root/'request-budget.state')
            with patch('data_collector.dynamic_options.take_slot',return_value=True):
                task=await self.start_live();child=await self.finish(task)
            self.assertEqual(child['status'],'raw_capture_complete',child)
            subscribed.update(self.session.subscribed)
            path=Path(child['root']);children.append(child)
            report['workers'].append(dict(index=index,root=str(path.relative_to(root))))
        for key in ('frames','bytes','connections','universe_versions','retired_options','rejected_subscriptions','throttled_subscriptions'):
            report[key]=sum(child[key] for child in children)
        (root/'result.json').write_bytes(canonical(report))
        if stale_owner or late_foreign:self.assertNotIn(extra['instrument_name'],subscribed)
        return root,report

    async def test_aggregate_requires_all_partitions_and_verifies_after_copy(self):
        root,report=await self._aggregate_fixture()
        checked=verify_run(root);self.assertTrue(checked['full_partition_coverage_verified'])
        copy=root.parent.parent/'copied-aggregate';shutil.copytree(root,copy)
        self.assertTrue(verify_run(copy)['full_partition_coverage_verified'])
        changed=dict(report,workers=[report['workers'][0],report['workers'][0]])
        (copy/'result.json').write_bytes(canonical(changed))
        with self.assertRaisesRegex(ValueError,'duplicate'):verify_run(copy)

    async def test_foreign_discovery_cannot_hide_a_missing_option_in_its_owners_cached_view(self):
        root,_=await self._aggregate_fixture(stale_owner=True)
        with self.assertRaisesRegex(ValueError,'cross-worker discovery lacks assigned'):verify_run(root)

    async def test_late_foreign_observation_still_checks_creation_during_shared_run(self):
        root,_=await self._aggregate_fixture(late_foreign=True)
        with self.assertRaisesRegex(ValueError,'cross-worker discovery lacks assigned'):verify_run(root)

    async def test_late_already_expired_option_created_during_run_cannot_be_ignored(self):
        root,_=await self._aggregate_fixture(late_foreign=True,already_expired=True)
        with self.assertRaisesRegex(ValueError,'cross-worker discovery lacks assigned'):verify_run(root)

    async def test_late_expired_option_born_before_run_still_needs_capture_and_retirement(self):
        root,_=await self._aggregate_fixture(late_foreign=True,already_expired=True,old_creation=True)
        with self.assertRaisesRegex(ValueError,'cross-worker discovery lacks assigned'):verify_run(root)

    async def test_delayed_foreign_and_usdc_settlement_are_not_new_unknown_listings(self):
        self.setup_live();self.config=replace(self.config,option_partition_count=2,option_partition_index=0)
        expiry=time.time_ns()//1_000_000+3500
        own=self.row(symbol(0),1);foreign=self.row(symbol(1),2,expiry=expiry)
        linear=dict(self.row('BTC_USDC-OLD-C',3,expiry=expiry),settlement_currency='USDC',instrument_type='linear')
        self.session.rows=[own,foreign,linear];task=await self.start_live()
        await self.wait_for(lambda:time.time_ns()//1_000_000>expiry+250)
        self.session.rows=[own]
        self.session.notify(foreign['instrument_name'],'settlement');self.session.notify(linear['instrument_name'],'settlement')
        await self.wait_for(lambda:self.session.calls>=2)
        result=await self.finish(task)
        self.assertEqual(result['status'],'raw_capture_complete',result)
        self.assertEqual(self.session.subscribed,{own['instrument_name']})
        self.assertTrue(verify_run(result['root'])['changing_universe_coverage_verified'])

    async def test_supervisor_stops_and_reaps_all_children_at_duration(self):
        self.setup_live();parent=replace(self.config,raw_processes=2,duration_seconds=1)
        processes=[]
        class Process:
            def __init__(self):self.returncode=None;self.done=asyncio.Event();self.signals=[]
            def send_signal(self,value):self.signals.append(value);self.returncode=0;self.done.set()
            async def wait(self):await self.done.wait();return self.returncode
        async def launch(*args,**kwargs):
            config=load(Path(args[-1]));index=config.option_partition_index
            path=config.data_root/'runs'/'fixture';path.mkdir(parents=True)
            (path/'ready.json').write_bytes(canonical(dict(options=1)))
            (path/'result.json').write_bytes(canonical(dict(status='raw_capture_complete')))
            self.assertEqual(config.option_partition_count,2);self.assertEqual(config.raw_processes,1)
            self.assertEqual(config.supervisor_pid,os.getpid())
            self.assertEqual(config.subscription_budget_file.parent.parent,self.root/'runs')
            process=Process();processes.append(process);return process
        with patch('data_collector.option_workers.asyncio.create_subprocess_exec',side_effect=launch):
            result=await capture_workers(parent,observer=self.observe)
        self.assertEqual(result['status'],'raw_capture_complete',result)
        self.assertEqual(len(processes),2);self.assertTrue(all(p.signals and p.returncode==0 for p in processes))
        self.assertGreaterEqual(result['elapsed_seconds'],1)

    async def test_worker_crash_fails_supervisor_and_drains_surviving_peer(self):
        self.setup_live();parent=replace(self.config,raw_processes=2,duration_seconds=1);processes=[]
        class Process:
            def __init__(self,failed):
                self.returncode=1 if failed else None;self.done=asyncio.Event();self.signals=[]
                if failed:self.done.set()
            def send_signal(self,value):self.signals.append(value);self.returncode=0;self.done.set()
            async def wait(self):await self.done.wait();return self.returncode
        async def launch(*args,**kwargs):
            config=load(Path(args[-1]));path=config.data_root/'runs'/'fixture';path.mkdir(parents=True)
            (path/'result.json').write_bytes(canonical(dict(status='failed')))
            process=Process(config.option_partition_index==0);processes.append(process);return process
        with patch('data_collector.option_workers.asyncio.create_subprocess_exec',side_effect=launch):
            result=await capture_workers(parent,observer=self.observe)
        self.assertEqual(result['status'],'failed');self.assertIn('worker ended',result['error'])
        self.assertEqual(processes[0].signals,[]);self.assertEqual(len(processes[1].signals),1)
        self.assertTrue(all(process.done.is_set() for process in processes))


if __name__=='__main__':unittest.main()
