from dataclasses import replace
from datetime import date,timedelta
import json
import io
from contextlib import redirect_stdout,redirect_stderr
from pathlib import Path
import tempfile
import time
import sys
from types import SimpleNamespace
import unittest
from unittest.mock import patch

from option_collector.archive import Archive, verify
from option_collector.collector import context
from option_collector.common import body_fields,dumps,epoch,event,process_lock,read_json,utc,write_json
from option_collector.config import Config,bind_root,load,retain_source
from option_collector.audit import audit
from option_collector.delivery import chunk,progress
from option_collector.operations import preflight,probe_clock,reconcile,schedule,terminal_events
from option_collector.cli import main,supervise
from option_collector.schema import coverage,universe
from tests.helpers import instrument,record


class OperationsTests(unittest.TestCase):
    def setUp(self):
        self.tmp=tempfile.TemporaryDirectory();self.addCleanup(self.tmp.cleanup)
        self.config=Config(data_root=Path(self.tmp.name),require_ntp=False)

    def test_bad_config_no_fallback_and_environment_binding(self):
        for c in (replace(self.config,rate=11),replace(self.config,workers=1.5),replace(self.config,attempts=True),replace(self.config,depth=25),replace(self.config,currencies=('BTC','BTC')),replace(self.config,attempt_seconds=float('nan'))):
            with self.assertRaises(ValueError):c.validate()
        bind_root(self.config)
        with self.assertRaises(ValueError):bind_root(replace(self.config,environment='testnet'))

    def test_process_lock_exclusion(self):
        with process_lock(self.config.data_root):
            with self.assertRaises(BlockingIOError):
                with process_lock(self.config.data_root):pass
            # Independent monitoring remains available during collection.
            with process_lock(self.config.data_root,'reconcile.lock'):pass

    def test_disk_stop_warning_and_clock_preflight(self):
        with patch('option_collector.operations.shutil.disk_usage',return_value=SimpleNamespace(free=1,total=100)),patch('option_collector.operations.clock_synced',return_value=False):
            health=preflight(replace(self.config,require_ntp=True))
            self.assertEqual(set(health['reasons']),{'disk_low','clock_unsafe'})
        with patch('option_collector.operations.shutil.disk_usage',return_value=SimpleNamespace(free=10*1024**3,total=100*1024**3)):
            h=preflight(self.config);self.assertTrue(h['ok']);self.assertTrue(h['disk_warning']);self.assertIsNone(h['clock_synced'])

    def test_preflight_terminal_event_not_double_counted_as_missing(self):
        start=epoch('2026-10-19T00:00:00Z')
        with patch('option_collector.operations.time.time',return_value=start):schedule(self.config,True)
        with patch('option_collector.operations.time.time',return_value=start+1):
            terminal_events(self.config,start,'disk_low')
        with patch('option_collector.operations.events_on',return_value=[read_json(p) for p in self.config.data_root.glob('events/*/*/*/*.json')]):
            results=reconcile(self.config,start+271)
        self.assertEqual([r['status'] for r in results],['failed','failed'])
        self.assertEqual(reconcile(self.config,start+271),[])

    def test_missing_slot_pause_and_resume(self):
        start=epoch('2026-10-19T00:00:00Z')
        with patch('option_collector.operations.time.time',return_value=start):schedule(self.config,True)
        with patch('option_collector.operations.time.time',return_value=start+300):schedule(self.config,False)
        rows=reconcile(self.config,start+600)
        self.assertEqual([r['status'] for r in rows],['missing','missing','paused','paused'])

    def test_universe_unknown_state_duplicate_and_missing_book_partial(self):
        i=instrument()
        with self.assertRaises(ValueError):universe([i,i],'BTC')
        with self.assertRaises(ValueError):universe([dict(i,state='future_unknown')],'BTC')
        a=Archive(self.config.data_root,context(self.config,'BTC','snapshot'))
        a.update_context(universe=[i['instrument_name']],universe_trusted=True,required_jobs=['metadata:option'])
        record(a,'metadata:option',[i],{'currency':'BTC','kind':'option'},'public/get_instruments')
        m=a.finalize()
        self.assertEqual(m['status'],'partial');self.assertEqual(m['missing_jobs'],['book:'+i['instrument_name']])

    def test_monotonic_clock_step_and_reboot_detection(self):
        with patch('option_collector.operations.boot_id',return_value='boot-one'),patch('option_collector.operations.time.time',return_value=1000),patch('option_collector.operations.time.monotonic',return_value=100):
            self.assertTrue(probe_clock(self.config))
        with patch('option_collector.operations.boot_id',return_value='boot-one'),patch('option_collector.operations.time.time',return_value=1012),patch('option_collector.operations.time.monotonic',return_value=110):
            self.assertFalse(probe_clock(self.config));self.assertIn('clock_unsafe',preflight(self.config)['reasons'])
        with patch('option_collector.operations.boot_id',return_value='boot-two'),patch('option_collector.operations.time.time',return_value=1020),patch('option_collector.operations.time.monotonic',return_value=1):
            self.assertTrue(probe_clock(self.config))

    def test_late_duplicate_and_preflight_fail_cli_never_collect(self):
        p=self.config.data_root/'config.toml';p.write_text('[collector]\ndata_root="."\nrequire_ntp=false\n')
        start=epoch('2026-10-19T00:00:00Z')
        args=['--worker','--config',str(p),'collect-once','--scheduled-slot',utc(start)]
        with redirect_stdout(io.StringIO()),patch('option_collector.cli.time.time',return_value=start+40):
            self.assertEqual(main(args),5)
        with patch('option_collector.operations.time.time',return_value=start):schedule(self.config,True)
        with redirect_stdout(io.StringIO()),patch('option_collector.cli.time.time',return_value=start+10),patch('option_collector.cli.preflight',return_value={'ok':False,'reasons':['disk_low']}):
            self.assertEqual(main(args),3)
            self.assertEqual(main(args),5)
        self.assertFalse((self.config.data_root/'snapshots').exists())

    def test_external_hard_stop_kills_process_ignoring_sigterm(self):
        start=time.monotonic()
        code=supervise([],2.4,command=[sys.executable,'-c','import signal,time;signal.signal(signal.SIGTERM,signal.SIG_IGN);time.sleep(30)'])
        self.assertEqual(code,4);self.assertGreater(time.monotonic()-start,2.1);self.assertLess(time.monotonic()-start,3.5)

    def test_audit_counts_missing_slots_and_source_bundle_is_immutable(self):
        r=audit(self.config,'2026-10-17T00:00:00Z','2026-10-18T00:00:00Z')
        self.assertFalse(r['data_gate_pass'])
        self.assertTrue(all(m['expected']==288 and m['statuses']['missing']==288 for m in r['markets'].values()))
        version=retain_source(self.config)
        p=self.config.data_root/'provenance'/version['source_sha256']/'option_collector/config.py'
        self.assertTrue(p.exists());p.write_text('corrupt')
        with self.assertRaises(ValueError):retain_source(self.config)


class DeliveryTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.tmp=tempfile.TemporaryDirectory();self.addCleanup(self.tmp.cleanup)
        self.config=Config(data_root=Path(self.tmp.name),require_ntp=False)

    async def test_52_clamped_pages_finish_with_snapshot_priority_and_yield(self):
        now=[184.0];totals={'BTC':2642,'ETH':2482};calls=[]
        class FakeClient:
            async def fetch(_,a,key,method,params,deadline):
                # Synthetic 2 rps, 0.5 s/page, clamped 100 rows despite count=1000.
                if now[0]+.5>deadline:return None
                now[0]+=.5;calls.append(now[0])
                offset=params['offset'];total=totals[a.context['currency']]
                rows=[{'date':str(date(2000,1,1)+timedelta(days=x)),'delivery_price':1} for x in range(offset,min(offset+100,total))]
                return record(a,key,{'records_total':total,'data':rows},params,method,at=1000000000+now[0])
        client=FakeClient();completed={};ticks=0
        with patch('option_collector.delivery.time',SimpleNamespace(monotonic=lambda:now[0])),patch('option_collector.collector.utc',return_value='2026-10-20T00:00:00.000+00:00'):
            for tick in range(30,7200,60):
                if tick%300<184:continue
                now[0]=float(tick);ticks+=1
                for m in await chunk(self.config,client,now[0]+15,day='2026-10-19'):
                    completed[m['currency']]=m
                # Lose the derived cursor after a clean yield; raw pages survive.
                (self.config.data_root/'state/delivery-2026-10-19.json').unlink(missing_ok=True)
                self.assertLessEqual(now[0],tick+15)
                if len(completed)==2:break
        self.assertLess(ticks,48);self.assertEqual(len(calls),52)
        self.assertLess(max(calls),7200)
        self.assertTrue(all(t%300>=184 and t%300<290 for t in calls))
        self.assertEqual({m['status'] for m in completed.values()},{'complete'})
        for m in completed.values():verify(self.config.data_root/m['destination'])

    def test_delivery_total_change_duplicate_and_empty_page_not_complete(self):
        a=Archive(self.config.data_root,context(self.config,'BTC','delivery'))
        def page(offset,total,dates):return record(a,'page:'+str(offset),{'records_total':total,'data':[{'date':d,'delivery_price':1} for d in dates]},dict(offset=offset),'public/get_delivery_prices')
        page(0,2,['2020-01-01'])
        page(1,3,['2020-01-02'])
        with self.assertRaises(ValueError):progress(a.records)
        a.records.pop();page(1,2,['2020-01-01'])
        with self.assertRaises(ValueError):progress(a.records)
        a.records.pop();page(1,2,[])
        with self.assertRaises(ValueError):progress(a.records)

    async def test_terminal_failed_delivery_starts_new_run_next_tick(self):
        config=replace(self.config,attempts=1)
        class FakeClient:
            failed=True
            async def fetch(client,a,key,method,params,deadline):
                return record(a,key,{'records_total':1,'data':[{'date':'2020-01-01','delivery_price':1}]},params,method,ok=not client.failed)
        client=FakeClient()
        first=await chunk(config,client,time.monotonic()+5,day='2026-10-19')
        self.assertEqual({m['status'] for m in first},{'failed'})
        client.failed=False
        second=await chunk(config,client,time.monotonic()+5,day='2026-10-19')
        self.assertEqual({m['status'] for m in second},{'complete'})
        self.assertTrue({m['run_id'] for m in first}.isdisjoint({m['run_id'] for m in second}))
