"""Bounded synthetic model continuation probe; no network or archive certification.

Run from the repository root with .venv/bin/python. This measures native model
state only. It does not establish bounded controller/ledger scans or capture RSS.
"""
import argparse
from datetime import datetime,timezone
from pathlib import Path
import platform
import sys
from time import perf_counter

sys.path.insert(0,str(Path(__file__).resolve().parents[1]))
from data_collector.books import DepthEvent,LevelChange,SnapshotBook
from data_collector.bookstate import restore_snapshot_state,snapshot_state
from data_collector.contracts import InstrumentKey,canonical,digest


def delta(instrument,sequence):
    return DepthEvent(instrument,format(sequence,'064x'),'delta',sequence,
        (LevelChange('100',str(sequence%9+1)),),(),sequence-1)


def probe(count,interval):
    instrument=InstrumentKey('deribit','production','option','SYNTHETIC-C','synthetic-incarnation')
    model=SnapshotBook(instrument,'deribit_full')
    model.receive(DepthEvent(instrument,'0'*64,'snapshot',0,(LevelChange('100','1'),)))
    checkpoint=None;start=perf_counter()
    for sequence in range(1,count+1):
        model.receive(delta(instrument,sequence))
        if sequence==count-interval:checkpoint=snapshot_state(model)
    full_seconds=perf_counter()-start
    start=perf_counter();restored=restore_snapshot_state(checkpoint)
    for sequence in range(count-interval+1,count+1):restored.receive(delta(instrument,sequence))
    suffix_seconds=perf_counter()-start
    final=canonical(snapshot_state(model));same=final==canonical(snapshot_state(restored))
    if not same:raise RuntimeError('native continuation differs')
    return dict(events=count,prefix_events=count-interval,suffix_events=interval,
        state_bytes=len(canonical(checkpoint)),full_processing_seconds=full_seconds,
        restore_and_suffix_seconds=suffix_seconds,state_sha256=digest(final),equal=same)


def main():
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--counts',type=int,nargs='+',default=[10000,100000,1000000])
    parser.add_argument('--interval',type=int,default=1000)
    parser.add_argument('--output',type=Path,required=True)
    args=parser.parse_args()
    if (not 1<=args.interval<=10000 or not 1<=len(args.counts)<=4 or
            any(not args.interval<count<=1000000 for count in args.counts)):
        parser.error('use1–4 counts above interval and up to1million; interval1–10000')
    rows=[]
    for count in args.counts:
        row=probe(count,args.interval);rows.append(row)
        print(canonical(row).decode(),flush=True)
    root=Path(__file__).resolve().parents[1]
    sources=['data_collector/books.py','data_collector/bookstate.py','experiments/native_checkpoint_probe.py']
    result=dict(schema='dc.native-model-continuation-probe/1',run_utc=datetime.now(timezone.utc).isoformat(),scope='synthetic native model only; no archive/runtime acceleration claim',
        python=sys.version,platform=platform.platform(),machine=platform.machine(),
        source_sha256={name:digest((root/name).read_bytes()) for name in sources},rows=rows)
    args.output.parent.mkdir(parents=True,exist_ok=True);args.output.write_bytes(canonical(result)+b'\n')


if __name__=='__main__':main()
