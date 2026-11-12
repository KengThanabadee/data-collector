"""Shared synthetic fixtures and fakes; never exchange observations.

Test modules import shared fixtures from here only, never from each other.
"""
import asyncio
from dataclasses import replace
from decimal import Decimal
import json
from pathlib import Path
import tempfile
import time

import aiohttp

from data_collector.clock import ClockSample
from data_collector.clocklog import SOURCE, replay_clock
from data_collector.config import DiscoveryConfig
from data_collector.contracts import InstrumentKey, Provenance, RawRecord, Receipt, canonical, digest, strict_json
from data_collector.journal import Journal, JournalLimits
from data_collector.lane import ArchiveLane, PENDING_CLOCK_EPOCH
from data_collector.queues import ByteQueue, QueueLimits
from data_collector.replay import ReplayLimits
from data_collector.segments import SegmentLimits, iter_segment_records
from data_collector.writer import SessionWriter
from data_collector.minute_files import body_fields, utc


FIXTURES = Path(__file__).parent / "fixtures"
CONFIG = Path(__file__).parents[1] / "config"

# Recorded venue replies.
DERIBIT_MINUTE_REPLIES = {
    (row["method"], tuple(sorted(row["params"].items()))): row["body"].encode()
    for row in json.loads((FIXTURES / "dc_deribit_minute_rest.json").read_bytes())
}
BYBIT_MINUTE_REPLIES = json.loads((FIXTURES / "dc_bybit_minute_rest.json").read_bytes())
BINANCE_EVENTS = json.loads((FIXTURES / "dc_binance_selected.json").read_text())
BYBIT_OPTION_TRADE = json.loads((FIXTURES / "dc_bybit_options.json").read_text())[
    "samples"]["trade"]["body"].encode()
# Representative hedge-perp push using the documented all-liquidation schema.
# https://bybit-exchange.github.io/docs/v5/websocket/public/all-liquidation
BYBIT_LIQUIDATION = (b'{"topic":"allLiquidation.ETHUSDT", "type":"snapshot", "ts":1739502303204, "data":['
                     b'{"T":1739502302929,"s":"ETHUSDT","S":"Sell","v":"0.34","p":"2258.9400"},'
                     b'{"T":1739502303040,"s":"ETHUSDT","S":"Buy","v":"0.01","p":"2258.9500"}]}')


def record(archive,key,result,params=None,method="public/get_index_price",ok=True,raw=None,at=None):
    at=time.time() if at is None else at
    body=raw if raw is not None else json.dumps({"jsonrpc":"2.0","result":result}).encode()
    r={k:archive.context[k] for k in ("schema_version","run_id","job_kind","environment","currency","scheduled_slot_utc")}
    r.update(logical_request_id=key,attempt=1,method=method,parameters=params or {},ok=ok,
             request_started_epoch_ms=at*1000,response_received_epoch_ms=(at+.1)*1000,
             request_started_utc=utc(at),response_received_utc=utc(at+.1),quality_flags=[],body_complete=True,
             http_status=200,**body_fields(body))
    return archive.append(r)


def instrument(currency="BTC"):
    return dict(instrument_name=currency+"-SYNTHETIC-C",kind="option",settlement_currency=currency,
                state="open",expiration_timestamp=1900000000000,strike=100000,option_type="call")


def key(**changes):
    return InstrumentKey(**(dict(venue="deribit", environment="production", native_product="option",
        native_symbol="BTC-SYNTHETIC-C", listing_incarnation="listing-1") | changes))


def raw(seq=0, body=b'{"price":0.123456789012345678901234567890}', **changes):
    values = dict(receipt=Receipt("capture", "process", "connection", seq, 100, 50, "epoch"),
        venue="deribit", environment="production", native_product="option", channel="synthetic",
        kind="ws_message", body=body, provenance=Provenance("a"*64, "b"*64, "c"*64), instrument=key())
    return RawRecord(**(values | changes))


def tracking(utc,**changes):
    """Synthetic `chronyc -c tracking` output at UTC nanoseconds `utc`."""
    fields=['CB00710F','192.0.2.1','3',str(Decimal(utc)/1_000_000_000),'0','0','0','0','0','0','0','0','64.2','Normal']
    for key,value in changes.items():fields[int(key[1:])]=value
    return (','.join(fields)+'\n').encode()


def response_body(rows=None):
    if rows is None:
        return (b'{"jsonrpc":"2.0","unknown_root":true,"result":['
            b'{"instrument_name":"BTC-TEST-C","instrument_id":1,"creation_timestamp":1000,'
            b'"kind":"option","base_currency":"BTC","settlement_currency":"USDC",'
            b'"is_active":false,"tick_size":0.000000000000000000123,"new_field":{"x":1e-20}}]}')
    return canonical(dict(jsonrpc='2.0',result=rows))


class Response:
    """Fake aiohttp response that streams its body in small chunks."""
    def __init__(self,body,status=200,headers=(),delay=None,error=None):
        self.body=body;self.status=status;self.raw_headers=headers;self.content=self
        self.offset=0;self.delay=delay;self.error=error;self.closed=False
    async def __aenter__(self):return self
    async def __aexit__(self,*args):self.closed=True
    async def read(self,size):
        if self.delay is not None:await self.delay.wait()
        if self.error is not None and self.offset:raise self.error
        chunk=self.body[self.offset:self.offset+min(size,19)];self.offset+=len(chunk);return chunk


class HTTP:
    """Fake REST session that serves queued responses in order."""
    _retry_connection=False
    def __init__(self,responses):self.responses=list(responses);self.calls=[]
    def get(self,url,**kwargs):
        self.calls.append((url,kwargs));return self.responses.pop(0)


class Socket:
    """Fake websocket that acknowledges JSON-RPC requests and queues pushed frames."""
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


class BinanceEventSession:
    """Fake Binance public session fed from the recorded selected-stream replies."""
    _retry_connection=False
    def __init__(self):
        self.calls=[];self.sockets=[];self.reject=False;self.no_book=False;self.before_ack=False
        self.closed=False;self.status=200;self.body=canonical(BINANCE_EVENTS['metadata'])
    async def __aenter__(self):return self
    async def __aexit__(self,*args):self.closed=True
    def get(self,url,**kwargs):
        self.calls.append(url);return Response(self.body,status=self.status)
    async def ws_connect(self,url,**kwargs):
        socket=Socket();self.sockets.append((url,socket))
        async def send(body):
            value=strict_json(body.encode());socket.sent.append(value)
            if self.reject:
                socket.put(dict(id=1,code=-1003,msg='Too many requests'));return
            topics=value['params']
            async def push():
                for topic in topics:
                    if self.no_book and '@depth' in topic:continue
                    socket.put(BINANCE_EVENTS['samples'][topic].encode())
            if self.before_ack:await push()
            socket.put(BINANCE_EVENTS['samples']['ack'].encode())
            if not self.before_ack:await push()
        socket.send_str=send
        return socket


class BybitEventSession:
    """Fake Bybit public session that acknowledges and pushes recorded event frames."""
    def __init__(self):
        self.sockets = []
        self.option_ack_topics = ['publicTrade.ETH', 'publicTrade.BTC']
        self.linear_control = None

    async def __aenter__(self):
        return self

    async def __aexit__(self, *args):
        pass

    async def ws_connect(self, url, **kwargs):
        socket = Socket()
        self.sockets.append((url, socket))

        async def send(body):
            value = strict_json(body.encode())
            socket.sent.append(value)
            if url.endswith('/linear'):
                socket.put(b'{"success":true,"ret_msg":"","conn_id":"linear-test","req_id":"","op":"subscribe"}')
                if self.linear_control is not None:
                    socket.put(self.linear_control)
                socket.put(BYBIT_LIQUIDATION)
            else:
                socket.put(canonical(dict(success=True, type='COMMAND_RESP', conn_id='option-test',
                    data=dict(failTopics=[], successTopics=self.option_ack_topics))))
                socket.put(BYBIT_OPTION_TRADE)

        async def receive():
            message = await socket.incoming.get()
            if isinstance(message, Exception):
                raise message
            return message

        socket.send_str = send
        socket.receive = receive
        return socket


class AppHarness:
    """Mixin for capture tests: a temporary data root and a deterministic app clock."""
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name).resolve()/'data'
        self.offset = 0
        self.reads = 0
        self.config = DiscoveryConfig(self.root,'testnet',symbols=('BTC-TEST-C',),max_frames=4,duration_seconds=90,capture_mode='snapshots')

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


class LaneHarness:
    """Mixin for lane tests: synthetic lanes, records and clock requests on a manual clock."""
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory(); self.addCleanup(self.tmp.cleanup)
        self.root = Path(self.tmp.name); self.counter = 0
        self.mono = [1000]
        self.utc = 1_700_000_000_000_000_000

    def observe(self):
        return self.utc+self.mono[0]-1000,self.mono[0],'epoch'

    def lane(self, *, venue='deribit', queue=None, observe=None, max_connections=4,clock_policy=None,cutoffs=False,snapshot_limits=None):
        self.counter += 1
        root = self.root/str(self.counter)
        provenance = Provenance('a'*64,'b'*64,'c'*64)
        writer = SessionWriter(root/'wal',root/'archive',venue=venue,environment='production',capture_id='capture',
            process_session_id='process',provenance=provenance,started_utc_ns=self.utc,now_ns=lambda:self.mono[0],
            limits=JournalLimits(max_record_bytes=131072,target_bytes=12000,max_streams=16),
            segment_limits=SegmentLimits(max_record_bytes=131072,max_uncompressed_bytes=262144,max_compressed_bytes=262144))
        emergency_path = root/'emergency.wal'
        emergency = Journal(emergency_path,venue=venue,environment='production',create=True,
                            limits=JournalLimits(max_record_bytes=131072))
        q = queue or ByteQueue(QueueLimits(max_bytes=131072,max_items=8,max_item_bytes=32768),now_ns=lambda:self.mono[0])
        lane = ArchiveLane(writer,q,emergency_writer=emergency,observe=observe or self.observe,max_connections=max_connections,
                           clock_policy=clock_policy,cutoff_scope='capture' if cutoffs else None,
                           cutoff_ledger_id='ledger' if cutoffs else None,
                           snapshot_limits=ReplayLimits() if snapshot_limits is None else snapshot_limits)
        lane.test_task = None
        self.addCleanup(writer.abort); self.addCleanup(emergency.close)
        async def cleanup():
            lane.stop()
            task = lane.test_task
            if task is None:task = asyncio.create_task(lane.run())
            await asyncio.wait_for(asyncio.gather(task,return_exceptions=True),5)
        self.addAsyncCleanup(cleanup)
        return lane,emergency_path

    def record(self, lane, seq=0, *, connection='connection', body=None):
        record = raw(seq, **({'body':body} if body is not None else {}))
        stamp = self.observe()
        return replace(record,venue=lane.identity['venue'],instrument=key(venue=lane.identity['venue']),
            receipt=Receipt('capture','process',connection,seq,*stamp))

    def start(self, lane):
        task = asyncio.create_task(lane.run()); lane.test_task=task
        return task

    def records(self, lane):
        return [record for path in lane.segments for record in iter_segment_records(path)]

    def controls(self, lane):
        return [strict_json(r.body) for r in self.records(lane) if r.channel=='runtime_events']

    def clock_policy(self):
        return dict(sample_interval_ns=60,max_sample_age_ns=60,max_offset_ns=100,step_tolerance_ns=2,max_epochs=8)

    def clock_request(self,lane,sequence=0):
        utc,mono,_=self.observe()
        source=RawRecord(Receipt('capture','process',SOURCE,sequence,utc,mono,'measurement'),
            lane.identity['venue'],'production','control','clock_measurement','control',b'original clock provider output',lane.provenance)
        return ClockSample(utc,mono,0,0,'boot',digest(source.to_bytes())),source

    def clock_replay(self,lane):
        return replay_clock(self.records(lane),venue=lane.identity['venue'],environment='production',capture_id='capture',
            process_session_id='process',provenance=lane.provenance,policy=self.clock_policy(),expected_pin=lane._clock.pin)

    def clock_check(self,lane,**kwargs):
        utc,mono,_=self.observe()
        return lane.submit_clock_check(utc_ns=kwargs.get('utc_ns',utc),monotonic_ns=kwargs.get('monotonic_ns',mono),
                                       boot_id=kwargs.get('boot_id','boot'))

    def received(self,lane,seq=0):
        record=self.record(lane,seq)
        return replace(record,receipt=replace(record.receipt,clock_epoch=PENDING_CLOCK_EPOCH))
