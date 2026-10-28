import asyncio
from contextlib import redirect_stdout
from dataclasses import replace
import io
from pathlib import Path
import tempfile
import time
import unittest
from unittest.mock import patch

from data_collector.app import run, verify_run, _bind_root, _verify_report
from data_collector.cli import main
from data_collector.config import DiscoveryConfig
from data_collector.contracts import canonical, strict_json
from data_collector.runtime import DeribitCapture
from tests.test_dc_clocksource import tracking
from tests.test_dc_deribit import Socket
from tests.test_dc_discovery import HTTP, Response, response_body


class Session(HTTP):
    def __init__(self):
        body = response_body().replace(b'"is_active":false',b'"is_active":true')
        super().__init__([Response(body)])
        self.closed = False
        self.socket = Socket()
        self.connected = False
        original = self.socket.send_str
        async def send(body):
            await original(body)
            request = strict_json(body.encode())
            if request['method']=='public/subscribe':
                channel = request['params']['channels'][0]
                symbol = channel.removeprefix('book.').removesuffix('.agg2')
                for snapshot in (True,False):
                    self.socket.put(dict(jsonrpc='2.0',method='subscription',params=dict(channel=channel,data=dict(
                        type='snapshot' if snapshot else 'change',instrument_name=symbol,timestamp=2,
                        change_id=10 if snapshot else 11,prev_change_id=10,
                        bids=[['new' if snapshot else 'change',100,9 if snapshot else 7]],asks=[]))))
        self.socket.send_str = send

    async def __aenter__(self):return self
    async def __aexit__(self,*args):self.closed=True
    async def ws_connect(self,url,**kwargs):
        self.connected=True
        return self.socket


class AppTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name).resolve()/'data'
        self.offset = 0
        self.reads = 0
        self.session = Session()
        self.config = DiscoveryConfig(self.root,'testnet',symbols=('BTC-TEST-C',),max_frames=4)

    def observe(self):
        return time.time_ns()+self.offset,time.monotonic_ns()+self.offset,'test-boot'

    async def clock_read(self,*args):
        self.reads+=1
        return 0,tracking(self.observe()[0]),b''

    async def clock_wait(self,deadline):
        if self.reads<4:
            self.offset+=max(0,deadline-self.observe()[1])
            await asyncio.sleep(0)
        else:await asyncio.Event().wait()

    async def capture(self,**kwargs):
        return await run(self.config,session_factory=lambda:self.session,observer=self.observe,
            clock_read=self.clock_read,clock_wait=self.clock_wait,**kwargs)

    async def test_complete_capture_committed_readback_replay_and_raw_export(self):
        result = await asyncio.wait_for(self.capture(),15)
        self.assertEqual(result['status'],'bounded_capture_complete',result)
        self.assertTrue(self.session.connected)
        self.assertTrue(self.session.closed)
        self.assertTrue(self.session.socket.closed)
        self.assertEqual(result['runtime']['receiver']['reason'],'session_frame_bound')
        verified = verify_run(result['root'],export=Path(self.temp.name)/'raw.jsonl')
        self.assertTrue(verified['archive_verified'])
        self.assertTrue(verified['book_replay_equal'])
        self.assertEqual(verified['books'][0]['bids'],[['100','7']])
        self.assertEqual(verified['metadata_versions'],1)
        self.assertEqual(verified['ws_messages'],4)
        exported = [strict_json(row) for row in (Path(self.temp.name)/'raw.jsonl').read_bytes().splitlines()]
        self.assertEqual(len(exported),verified['records'])
        self.assertTrue(any(r['kind']=='rest_response' for r in exported))
        self.assertTrue((Path(result['root'])/'verification.json').exists())
        with self.assertRaises(FileExistsError):
            verify_run(result['root'],export=Path(self.temp.name)/'raw.jsonl')

    async def test_discovery_command_archives_metadata_without_opening_socket(self):
        result = await self.capture(discover_only=True)
        self.assertEqual(result['status'],'discovery_complete',result)
        self.assertEqual(result['available_symbols'],['BTC-TEST-C'])
        self.assertFalse(self.session.connected)
        self.assertEqual(verify_run(result['root'])['metadata_versions'],1)

    async def test_discovery_sized_response_and_proofs_use_the_frozen_pilot_budget(self):
        # Real discovery exceeded4MiB and response+metadata proof exceeded8MiB.
        # Preserve an unknown native field rather than weakening proof checks.
        body = self.session.responses[0].body.replace(b'"new_field":{"x":1e-20}',
            b'"new_field":{"x":1e-20,"fixture":"'+b'x'*(17*1024*1024//4)+b'"}')
        response = Response(body)
        async def read(size):
            chunk = response.body[response.offset:response.offset+size]
            response.offset+=len(chunk)
            return chunk
        response.read = read
        self.session.responses = [response]
        result = await self.capture(discover_only=True)
        self.assertEqual(result['status'],'discovery_complete',result)
        self.assertGreater(len(body),4*1024*1024)
        self.assertEqual(result['replay_limits']['max_metadata_bytes'],16*1024*1024)
        self.assertTrue(verify_run(result['root'])['archive_verified'])
        smaller = dict(result,replay_limits=dict(result['replay_limits'],max_metadata_bytes=8*1024*1024))
        with self.assertRaisesRegex(ValueError,'retained byte bound'):
            _verify_report(Path(result['root']),smaller)

    async def test_capture_publishes_a_minute_snapshot_after_clock_alignment(self):
        self.config = replace(self.config,max_frames=8,duration_seconds=120)
        components = []
        waits = []
        def capture(*args,**kwargs):
            value = DeribitCapture(*args,**kwargs)
            components.append(value)
            return value
        async def wait(deadline):
            while not components or components[0].receiver.models[0].state!='live':
                await asyncio.sleep(0)
            while components[0].pipeline.health()['reported']<len(waits):
                await asyncio.sleep(0)
            waits.append(deadline)
            self.offset+=max(0,deadline-self.observe()[1])
            await components[0].sampler._sample_once()
            self.session.socket.put(dict(jsonrpc='2.0',method='heartbeat',params=dict(type='heartbeat')))
        with patch('data_collector.app.DeribitCapture',side_effect=capture):
            result = await asyncio.wait_for(self.capture(schedule_wait=wait),15)
        self.assertEqual(result['status'],'bounded_capture_complete',result)
        self.assertGreaterEqual(len(result['snapshot_ids']),1,result)
        self.assertEqual(verify_run(result['root'])['snapshots_verified'],len(result['snapshot_ids']))

    async def test_ack_only_run_is_failed_and_verification_failure_is_not_saved_as_success(self):
        self.config = replace(self.config,max_frames=1)
        result = await self.capture()
        self.assertEqual(result['status'],'failed',result)
        self.assertIn('native snapshot',result['error'])
        self.config = replace(self.config,data_root=self.root/'another',max_frames=4)
        self.session = Session()
        self.reads = 0
        with patch('data_collector.app._verify_report',side_effect=ValueError('corrupt fixture')):
            result = await self.capture()
        self.assertEqual(result['status'],'failed',result)
        saved = strict_json((Path(result['root'])/'result.json').read_bytes())
        self.assertEqual(saved['status'],'failed')
        self.assertIn('verification failed',saved['error'])

    async def test_unknown_symbol_fails_with_discovery_raw_retained(self):
        self.config = replace(self.config,symbols=('UNKNOWN',))
        result = await self.capture()
        self.assertEqual(result['status'],'failed',result)
        self.assertIn('not an active discovered',result['error'])
        self.assertFalse(self.session.connected)
        self.assertGreater(verify_run(result['root'])['records'],0)
        self.assertTrue(self.session.closed)

    async def test_cancel_during_http_drains_raw_and_closes_owners(self):
        waiting = asyncio.Event()
        response = self.session.responses[0]
        original = response.read
        async def blocked(size):
            waiting.set()
            await asyncio.Event().wait()
            return await original(size)
        response.read = blocked
        task = asyncio.create_task(self.capture())
        await asyncio.wait_for(waiting.wait(),5)
        task.cancel()
        with self.assertRaises(asyncio.CancelledError):await asyncio.wait_for(task,5)
        root = next((self.root/'runs').iterdir())
        report = strict_json((root/'result.json').read_bytes())
        self.assertEqual(report['error'],'cancelled')
        self.assertGreater(verify_run(root)['records'],0)
        self.assertTrue(self.session.closed)

    def test_root_separation_environment_and_existing_content(self):
        import os
        owner = _bind_root(self.config)
        try:
            with self.assertRaises(BlockingIOError):_bind_root(self.config)
        finally:os.close(owner)
        with self.assertRaises(ValueError):_bind_root(replace(self.config,environment='production'))
        v1 = Path(self.temp.name)/'v1'
        v1.mkdir();(v1/'environment.json').write_text('{}')
        with self.assertRaisesRegex(ValueError,'v1'):
            _bind_root(replace(self.config,data_root=v1/'nested'))
        other = Path(self.temp.name)/'other'
        other.mkdir();(other/'keep.raw').write_bytes(b'untouched')
        with self.assertRaisesRegex(ValueError,'unbound'):
            _bind_root(replace(self.config,data_root=other))
        self.assertEqual((other/'keep.raw').read_bytes(),b'untouched')

    def test_config_check_cli_no_network_and_invalid_config_nonzero(self):
        example = Path(__file__).resolve().parents[1]/'config/v2-discovery.toml'
        output = io.StringIO()
        with redirect_stdout(output):self.assertEqual(main(['config-check','--config',str(example)]),0)
        self.assertEqual(strict_json(output.getvalue().encode())['config']['environment'],'testnet')
        output = io.StringIO()
        with redirect_stdout(output):self.assertEqual(main(['capture','--config',str(example)]),1)
        self.assertIn('explicit current option symbols',output.getvalue())


class CommandTests(unittest.TestCase):
    def test_capture_and_verify_commands_use_the_real_runtime_with_fixture_transport(self):
        fixture = AppTests()
        fixture.setUp()
        self.addCleanup(fixture.temp.cleanup)
        config = Path(fixture.temp.name)/'config.toml'
        config.write_text('[data_collector]\nschema="dc.config/2"\nenvironment="testnet"\n'
            f'data_root="{fixture.root}"\nsymbols=["BTC-TEST-C"]\nmax_frames=4\n')
        async def local_run(value,**kwargs):
            return await run(value,session_factory=lambda:fixture.session,observer=fixture.observe,
                clock_read=fixture.clock_read,clock_wait=fixture.clock_wait,**kwargs)
        output = io.StringIO()
        with patch('data_collector.cli.run',side_effect=local_run), redirect_stdout(output):
            self.assertEqual(main(['capture','--config',str(config)]),0)
        report = strict_json(output.getvalue().encode())
        exported = Path(fixture.temp.name)/'export.jsonl'
        output = io.StringIO()
        with redirect_stdout(output):
            self.assertEqual(main(['verify-run',report['root'],'--export',str(exported)]),0)
        self.assertTrue(strict_json(output.getvalue().encode())['book_replay_equal'])
        self.assertGreater(exported.stat().st_size,0)
