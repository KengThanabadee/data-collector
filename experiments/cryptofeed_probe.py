"""Isolated pinned Cryptofeed assessment; never imports the production collector.

Requires a separate Python >=3.13 environment with the inspected upstream source
installed. This first stage is synthetic/offline and makes no exchange requests.
Raw artifacts belong under ignored data/ or experiments/results/ directories.
"""
import argparse
import asyncio
from decimal import Decimal
import hashlib
import importlib.metadata
import inspect
import json
from pathlib import Path
import signal
import subprocess
import sys
from types import SimpleNamespace
from unittest.mock import patch

PIN = 'f2daed5c75f4514669e2da54a4cbb8d1f5037a47'
VERSION = '3.0.2'
FILES = ('connection.py','connection_handler.py','feedhandler.py','feed.py','exchange.py','symbols.py','_json.py','types.pyx',
         'exchanges/deribit.py','exchanges/bybit.py',
         'exchanges/binance.py','exchanges/binance_delivery.py','capture/recorder.py',
         'capture/pcap.py','capture/playback.py','capture/_zstd.py')


def sha(data):return hashlib.sha256(data).hexdigest()


def dump(path,value):
    path.write_text(json.dumps(value,sort_keys=True,indent=2,default=str)+'\n')


def environment(source):
    import cryptofeed
    if sys.version_info < (3,13) or importlib.metadata.version('cryptofeed') != VERSION:
        raise ValueError('isolated Python>=3.13 and pinned cryptofeed3.0.2 required')
    head = subprocess.check_output(['git','rev-parse','HEAD'],cwd=source,text=True,timeout=10).strip()
    if head != PIN:raise ValueError('upstream commit differs from evaluated pin')
    subprocess.run(['git','diff','--quiet','HEAD','--','pyproject.toml','LICENSE',
                    *('cryptofeed/'+name for name in FILES)],cwd=source,check=True,timeout=10)
    installed = Path(inspect.getfile(cryptofeed)).parent
    hashes = {}
    for name in FILES:
        expected=(source/'cryptofeed'/name).read_bytes()
        actual=(installed/name).read_bytes()
        if expected != actual:raise ValueError('installed source mismatch: '+name)
        hashes[name]=sha(actual)
    freeze=subprocess.check_output([sys.executable,'-m','pip','freeze','--disable-pip-version-check','--no-cache-dir'],text=True,timeout=15).splitlines()
    return dict(upstream_commit=head,version=VERSION,python=sys.version,installed_source_hashes=hashes,
        source_license_sha256=sha((source/'LICENSE').read_bytes()),license='AGPL-3.0-or-later with attribution term',
        dependencies=freeze)


async def roundtrip(output):
    from cryptofeed.capture import PcapRecorder,read_capture
    path=output/'roundtrip.pcap'
    recorder=PcapRecorder(str(path),compress=False,rotate_size=2048)
    connection=SimpleNamespace(id='DERIBIT.ws.synthetic',address='wss://www.deribit.com/ws/api/v2',subscription={})
    bodies=[' {"decimal":1.2300,"extra":"ไทย"}\n',b'\x00\xff\x10', '{"unknown":true}']
    expected=[]
    with patch('os.fsync') as fsync:
        try:
            for session in range(2):
                await recorder.on_ws_open(connection,1700000000.0+session)
                await recorder.on_ws_send(connection,'{"public":"subscription"}',1700000000.0+session+.01)
                for index,body in enumerate(bodies):
                    await recorder.on_ws_message(connection,body,1700000000.0+session+.1+index*.01)
                    expected.append((session+1,type(body).__name__,body))
                await recorder.on_close(connection)
        finally:
            await recorder.aclose()
    capture=read_capture(recorder.files)
    actual=[(session.connect_ordinal,type(body).__name__,body)
            for session in capture.ws for _,direction,body in session.messages if direction=='in']
    if actual!=expected:raise ValueError('roundtrip changed original application message bytes/types/order')
    return dict(exact_payloads=True,received_messages=len(actual),connection_epochs=len(capture.ws),
        rotations=len(recorder.files)-1,fsync_calls=fsync.call_count,
        durability_claim='callback completion is not a durable acknowledgement',
        payload_sha256=[sha(body.encode() if isinstance(body,str) else body) for _,_,body in actual],
        files=[dict(name=Path(p).name,sha256=sha(Path(p).read_bytes()),bytes=Path(p).stat().st_size) for p in recorder.files])


def book_view(book):
    levels=book.book.to_dict()
    return dict(symbol=book.symbol,sequence=book.sequence_number,
        bids=sorted([[str(p),str(q)] for p,q in levels['bid'].items()]),
        asks=sorted([[str(p),str(q)] for p,q in levels['ask'].items()]))


async def native_replay(output):
    from cryptofeed.capture import PcapRecorder,playback_async
    from cryptofeed.defines import DERIBIT,L2_BOOK
    from cryptofeed.exchanges import Deribit
    from cryptofeed.exceptions import MissingSequenceNumber
    from cryptofeed.symbols import Symbols
    standard='BTC-USD-PERP';native='BTC-PERPETUAL'
    Symbols.set(DERIBIT,{standard:native},{'instrument_type':{standard:'perpetual'},'tick_size':{standard:Decimal('0.5')}})
    observed=[]
    async def collect(book,received):observed.append(book_view(book))
    feed=Deribit(symbols=[standard],channels=[L2_BOOK],callbacks={L2_BOOK:collect},config={'log':{'disabled':True}})
    recorder=PcapRecorder(str(output/'deribit.pcap'),compress=False)
    recorder.register_feed(feed)
    conn=SimpleNamespace(id='DERIBIT.ws.synthetic',uuid='DERIBIT.ws.synthetic',
        address='wss://www.deribit.com/ws/api/v2',subscription=feed.subscription)
    async def write(body):await recorder.on_ws_send(conn,body,1700000000.01)
    conn.write=write
    channel='book.'+native+'.100ms'
    def message(kind,change,previous=None):
        data=dict(type=kind,timestamp=1,instrument_name=native,change_id=change,
                  bids=[['new' if kind=='snapshot' else 'change',100.0,2.0 if kind=='snapshot' else 4.0]],
                  asks=[['new',101.0,3.0]] if kind=='snapshot' else [])
        if previous is not None:data['prev_change_id']=previous
        return json.dumps(dict(jsonrpc='2.0',method='subscription',params=dict(channel=channel,data=data)))
    try:
        await recorder.on_ws_open(conn,1700000000.0)
        await feed.subscribe(conn)
        for index,body in enumerate([message('snapshot',10),message('change',11,10)]):
            ts=1700000000.1+index*.1
            await recorder.on_ws_message(conn,body,ts)
            await feed.message_handler(body,conn,ts)
        gap=False
        try:await feed.message_handler(message('change',12,999),conn,1700000000.3)
        except MissingSequenceNumber:gap=True
    finally:
        await recorder.aclose()
        await feed.http_conn.close()
    if not gap or len(observed)!=2:raise ValueError('native sequence-hole fixture was not detected')
    replayed=[]
    async def replay_collect(book,received):replayed.append(book_view(book))
    result=await playback_async(str(output/'deribit.pcap'),exchange=DERIBIT,callbacks={L2_BOOK:replay_collect},
        on_error='raise',config={'log':{'disabled':True},'symbol_cache_ttl':0})
    if observed!=replayed:raise ValueError('native book callbacks differ after upstream replay')
    return dict(equal_callback_states=True,states=len(observed),sequence_gap_detected=gap,
                replay_messages=result.messages_processed,order_regressions=result.order_regressions,
                final_book=observed[-1])


def crash_probe(output):
    path=output/'crashed.pcap'
    code='''import asyncio,os,sys
from types import SimpleNamespace
from cryptofeed.capture import PcapRecorder
async def main():
 r=PcapRecorder(sys.argv[1],compress=False)
 c=SimpleNamespace(id="DERIBIT.ws.crash",address="wss://www.deribit.com/ws/api/v2",subscription={})
 await r.on_ws_open(c,1700000000.0)
 await r.on_ws_message(c,'{"accepted":true}',1700000000.1)
 os._exit(23)
asyncio.run(main())
'''
    child=subprocess.run([sys.executable,'-c',code,str(path)],timeout=15,capture_output=True)
    if child.returncode!=23:raise ValueError('crash child did not reach the completed recorder callback')
    from cryptofeed.capture import read_capture
    try:
        result=read_capture(str(path))
        recovered=sum(direction=='in' for session in result.ws for _,direction,_ in session.messages)
        reader_error=None
    except ValueError as exc:
        recovered=0;reader_error=str(exc).split(':')[-1].strip()
    return dict(callback_completed_before_exit=True,exit_code=23,file_bytes=path.stat().st_size,
                recovered_messages=recovered,reader_error=reader_error,synthetic=True)


async def run(source,output):
    output.mkdir(parents=True,exist_ok=False)
    report=dict(schema='dc.cryptofeed-evaluation/1',mode='offline',synthetic=True,
                harness_sha256=sha(Path(__file__).read_bytes()),environment=environment(source),
                limits=dict(wall_seconds=60,artifact_bytes=64*1024*1024),status='running')
    dump(output/'report.json',report)
    try:
        async with asyncio.timeout(45):
            report['raw_roundtrip']=await roundtrip(output)
            report['native_replay']=await native_replay(output)
            report['crash']=crash_probe(output)
        total=sum(p.stat().st_size for p in output.iterdir() if p.is_file())
        if total>report['limits']['artifact_bytes']:raise ValueError('offline artifact budget exceeded')
        report['status']='passed'
    except Exception as exc:
        report['status']='failed';report['error']=type(exc).__name__+': '+str(exc)
        raise
    finally:
        dump(output/'report.json',report)
    return report


if __name__=='__main__':
    def hard_timeout(signum,frame):raise TimeoutError('offline assessment hard wall deadline')
    signal.signal(signal.SIGALRM,hard_timeout)
    signal.alarm(60)
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--source',type=Path,required=True)
    parser.add_argument('--output',type=Path,required=True)
    args=parser.parse_args()
    result=asyncio.run(run(args.source.resolve(),args.output.resolve()))
    signal.alarm(0)
    print(json.dumps(dict(status=result['status'],report=str(args.output/'report.json'))))
