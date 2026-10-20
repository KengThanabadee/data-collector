"""Synthetic reproductions of the six independent implementation findings."""
from dataclasses import replace
from pathlib import Path
import tempfile
import time
import unittest
from unittest.mock import patch

from option_collector.archive import Archive, verify
from option_collector.audit import audit
from option_collector.cli import main, supervise
from option_collector.collector import context
from option_collector.common import epoch, read_json, utc, write_json
from option_collector.config import Config
from option_collector.delivery import chunk
from option_collector.operations import reconcile, schedule
from option_collector.schema import coverage
from tests.helpers import instrument, record


class ReviewRegressionTests(unittest.TestCase):
    def setUp(self):
        self.tmp=tempfile.TemporaryDirectory();self.addCleanup(self.tmp.cleanup)
        self.config=Config(data_root=Path(self.tmp.name),require_ntp=False,currencies=('BTC',))
        self.start=epoch('2026-10-19T09:00:00Z')

    def enable(self):
        with patch('option_collector.operations.time.time',return_value=self.start):
            schedule(self.config,True)

    def snapshot(self, slot=None, legacy=False):
        c=context(self.config,'BTC','snapshot',utc(self.start if slot is None else slot))
        if legacy:c.pop('capture_span_version')
        a=Archive(self.config.data_root,c)
        i=instrument()
        a.update_context(required_jobs=['metadata:option'],universe=[i['instrument_name']],universe_trusted=True)
        record(a,'metadata:option',None,ok=False,at=100)
        record(a,'metadata:option',[i],{'currency':'BTC','kind':'option'},'public/get_instruments',at=116)
        record(a,'book:'+i['instrument_name'],{},at=130)
        return a

    def test_reconcile_reverifies_cached_success_and_rebuilds_corrupt_evidence(self):
        self.enable();a=self.snapshot();a.finalize()
        self.assertEqual(reconcile(self.config,self.start+271)[0]['status'],'complete')
        self.assertEqual(reconcile(self.config,self.start+271),[])
        packed=a.destination/'responses.jsonl.gz';original=packed.read_bytes()
        packed.write_bytes(b'corrupt')
        self.assertEqual(reconcile(self.config,self.start+271)[0]['status'],'corrupt')
        # Repair only this temporary fixture, proving the derived index rebuilds.
        packed.write_bytes(original)
        self.assertEqual(reconcile(self.config,self.start+271)[0]['status'],'complete')
        packed.unlink()
        self.assertEqual(reconcile(self.config,self.start+271)[0]['status'],'corrupt')
        (a.destination/'manifest.json').unlink()
        self.assertEqual(reconcile(self.config,self.start+271)[0]['status'],'missing')

    def test_reconcile_rejects_corruption_before_first_index(self):
        self.enable();a=self.snapshot();a.finalize()
        (a.destination/'responses.jsonl.gz').write_bytes(b'bad')
        self.assertEqual(reconcile(self.config,self.start+271)[0]['status'],'corrupt')

    def test_same_boundary_lifecycle_order_survives_clock_ties_and_cursor_loss(self):
        # Deliberately reverse UUID ordering; sequence must carry command order.
        ids=['z','m','a']
        with patch('option_collector.operations.time.time',return_value=self.start),patch('option_collector.common.uuid.uuid4',side_effect=ids):
            for enabled in (True,False,True):schedule(self.config,enabled)
        self.assertEqual(reconcile(self.config,self.start+271)[0]['status'],'missing')
        (self.config.data_root/'state/reconcile.json').unlink()
        self.assertEqual(reconcile(self.config,self.start+271),[])
        self.assertEqual(read_json(self.config.data_root/'state/schedule.json')['sequence'],3)

    def test_capture_span_includes_failed_discovery_and_ignores_late_failed_book(self):
        a=self.snapshot()
        record(a,'unrelated',{},at=200)
        record(a,'book:'+instrument()['instrument_name'],{},ok=False,at=250)
        self.assertAlmostEqual(coverage(a.context,a.records)['capture_span_seconds'],30.1)
        a.finalize();self.assertEqual(verify(a.destination)[0]['status'],'complete')
        # Prior archives retain their recorded metric and remain verifiable.
        b=self.snapshot(self.start+300,legacy=True)
        self.assertAlmostEqual(b.finalize()['capture_span_seconds'],14.1)
        verify(b.destination)

    def test_delivery_parent_cutoff_includes_grace_and_delayed_start(self):
        p=self.config.data_root/'config.toml';p.write_text('[collector]\ndata_root="."\nrequire_ntp=false\n')
        with patch('option_collector.cli.time.time',return_value=self.start+281),patch('option_collector.cli.supervise',return_value=0) as parent:
            self.assertEqual(main(['--config',str(p),'delivery','--due']),0)
            self.assertEqual(parent.call_args.args[1],9)
        with patch('option_collector.cli.time.time',return_value=self.start+286),patch('option_collector.cli.supervise') as parent:
            self.assertEqual(main(['--config',str(p),'delivery','--due']),0)
            parent.assert_not_called()

    def test_delivery_hung_finalization_is_killed_before_cutoff(self):
        import sys
        # Real process ignores TERM as a stalled finalization could.
        # Short wall budget includes grace; no API or recurring job.
        before=time.monotonic()
        code=supervise([], 2.4, command=[sys.executable,'-c',
            'import signal,time,sys;from option_collector.common import process_lock;'
            'signal.signal(signal.SIGTERM,signal.SIG_IGN);'
            'lock=process_lock(sys.argv[1]);lock.__enter__();time.sleep(30)',str(self.config.data_root)])
        self.assertEqual(code,4)
        self.assertGreater(time.monotonic()-before,2.1)
        self.assertLess(time.monotonic()-before,3.5)
        from option_collector.common import process_lock
        with process_lock(self.config.data_root):pass

    def test_audit_detects_pause_entering_window_and_exact_resume_boundary(self):
        start=self.start
        with patch('option_collector.operations.time.time',return_value=start-600):schedule(self.config,True)
        with patch('option_collector.operations.time.time',return_value=start-300):schedule(self.config,False)
        with patch('option_collector.operations.time.time',return_value=start+600):schedule(self.config,True)
        # 286/288 complete would meet the fraction requirement without pause.
        for slot in range(int(start+600),int(start+86400),300):
            label=utc(slot).replace(':','').replace('+','_')
            write_json(self.config.data_root/'snapshots'/utc(slot)[:10].replace('-','/')/label/'run/BTC/manifest.json',{})
        def fake_verify(path):
            label=path.parents[1].name
            slot=next(s for s in range(int(start+600),int(start+86400),300)
                      if utc(s).replace(':','').replace('+','_')==label)
            return dict(scheduled_slot_utc=utc(slot),currency='BTC',environment='production',status='complete',archive_bytes=1,expected_jobs=['index']),[dict(ok=True,logical_request_id='index',request_started_epoch_ms=1000,response_received_epoch_ms=2000,body_byte_count=1)]
        with patch('option_collector.audit.verify',side_effect=fake_verify):
            r=audit(self.config,utc(start),utc(start+86400))
        self.assertGreaterEqual(r['markets']['BTC']['complete_fraction'],.99)
        self.assertTrue(r['intentional_pause']);self.assertFalse(r['data_gate_pass'])
        from option_collector.operations import pause_overlaps
        self.assertFalse(pause_overlaps(self.config.data_root,start+600,start+86400))
        self.assertTrue(pause_overlaps(self.config.data_root,start-300,start))


class DeliveryVersionRegressionTests(unittest.IsolatedAsyncioTestCase):
    async def test_clean_yield_restarts_on_source_or_schema_change(self):
        for change in ('code','schema_version','capture_span_version','config_sha256'):
            with self.subTest(change=change),tempfile.TemporaryDirectory() as tmp:
                config=Config(data_root=Path(tmp),require_ntp=False,currencies=('BTC',))
                c=context(config,'BTC','delivery');c.update(job_date='2026-10-19',pagination_complete=False,required_jobs=['page:0'],
                    destination=f"delivery/2026/10/19/{c['run_id']}/BTC")
                c[change]={'source_sha256':'old','files':{}} if change=='code' else 0 if change!='config_sha256' else c['config_sha256']
                # Config hash remains valid evidence; change the effective runtime
                # config rather than forging an invalid old context.
                runtime=replace(config,rate=9) if change=='config_sha256' else config
                a=Archive(config.data_root,c)
                record(a,'page:0',{'records_total':2,'data':[{'date':'2020-01-01','delivery_price':1}]},{'offset':0},'public/get_delivery_prices')
                a.checkpoint('yielded')
                class Client:
                    async def fetch(self,archive,key,method,params,deadline):
                        self.offset=params['offset']
                        return record(archive,key,{'records_total':1,'data':[{'date':'2020-01-02','delivery_price':1}]},params,method)
                client=Client()
                rows=await chunk(runtime,client,time.monotonic()+5,day='2026-10-19')
                self.assertEqual(client.offset,0)
                self.assertEqual(rows[0]['status'],'complete');self.assertNotEqual(rows[0]['run_id'],c['run_id'])
                old,_=verify(a.destination)
                self.assertEqual(old['status'],'interrupted')
                new_context=read_json(runtime.data_root/rows[0]['destination']/'context.json')
                self.assertEqual(new_context['code'],context(runtime,'BTC','delivery')['code'])
