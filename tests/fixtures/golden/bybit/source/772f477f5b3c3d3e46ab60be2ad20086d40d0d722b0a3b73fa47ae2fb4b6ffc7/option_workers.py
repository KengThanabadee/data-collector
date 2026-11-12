"""Supervise disjoint live option partitions; each keeps its complete metadata."""
import asyncio
from contextlib import closing
from dataclasses import replace
from functools import partial
import json
import os
from pathlib import Path
import signal
import sqlite3
import sys
import tempfile
import time
import uuid

from .contracts import canonical,digest,strict_json
from .journal import _publish_bytes
from .segments import _mkdir
from .option_universe import option_partition


def worker_config(config, root, index, budget):
    return replace(config,data_root=root/'workers'/str(index),raw_processes=1,
        option_partition_count=config.raw_processes,option_partition_index=index,
        duration_seconds=0,max_frames=0,max_bytes=0,supervisor_pid=os.getpid(),
        subscription_budget_file=budget)


def config_toml(config):
    values=strict_json(config.to_bytes())
    return ('[data_collector]\n'+''.join(f'{key}={json.dumps(value,ensure_ascii=False)}\n'
        for key,value in values.items() if value is not None)).encode()


def child_root(root,index):
    paths=list((root/'workers'/str(index)/'runs').glob('*'))
    if len(paths)>1:raise ValueError('multiple child captures in one worker root')
    return paths[0] if paths else None


async def capture_workers(config, *, observer):
    from .app import _bind_root,_drain,_freeze_source
    lock=_bind_root(config);root=config.data_root/'runs'/uuid.uuid4().hex
    processes=[];files=[];stopped=False;started=None;failure=None
    report=dict(schema='dc.option-workers/1',status='failed',venue='deribit',environment=config.environment,
        workers=[],elapsed_seconds=0,physical_global_copies=config.raw_processes,original_root=str(root),
        global_copy_semantics='independent native lifecycle/trade/index receipts; book/ticker symbols are disjoint')

    async def own():
        nonlocal started
        _mkdir(root)
        report['source_sha256']=_freeze_source(root)
        _publish_bytes(root/'config.json',config.to_bytes())
        report['config_sha256']=config.config_sha256
        budget=root/'request-budget.state';_publish_bytes(budget,b'0')
        for index in range(config.raw_processes):
            if stopped:break
            child=worker_config(config,root,index,budget)
            path=root/f'worker-{index}.toml';_publish_bytes(path,config_toml(child))
            out=(root/f'worker-{index}.stdout').open('xb');files.append(out)
            err=(root/f'worker-{index}.stderr').open('xb');files.append(err)
            process=await asyncio.create_subprocess_exec(sys.executable,'-m','data_collector','capture',
                '--config',str(path),stdout=out,stderr=err,cwd=Path(__file__).parent.parent)
            processes.append(process)
        deadline=time.monotonic()+config.warmup_timeout_seconds+config.bootstrap_timeout_seconds+60
        while not stopped:
            if any(process.returncode is not None for process in processes):
                raise ValueError('option worker ended before requested stop')
            roots=[child_root(root,index) for index in range(len(processes))]
            ready=len(processes)==config.raw_processes and all(path and (path/'ready.json').exists() for path in roots)
            if started is None and ready:
                started=time.monotonic()
                _publish_bytes(root/'ready.json',canonical(dict(utc_ns=observer()[0],workers=len(roots),
                    options=sum(strict_json((path/'ready.json').read_bytes())['options'] for path in roots))))
            if started is None and time.monotonic()>=deadline:raise TimeoutError('option workers initialization deadline')
            if started is not None and config.duration_seconds and time.monotonic()-started>=config.duration_seconds:break
            await asyncio.sleep(.1)

    task=asyncio.create_task(own())
    try:
        try:await asyncio.shield(task)
        except asyncio.CancelledError:
            stopped=True;await _drain(task)
    except Exception as exc:failure=type(exc).__name__+': '+str(exc)
    finally:
        stopped=True
        for process in processes:
            if process.returncode is None:
                try:process.send_signal(signal.SIGINT)
                except ProcessLookupError:pass
        await _drain(asyncio.gather(*(process.wait() for process in processes)))
        for file in files:file.close()
        report['elapsed_seconds']=0 if started is None else time.monotonic()-started
        children=[]
        try:
            for index,process in enumerate(processes):
                path=child_root(root,index)
                if path is None or not (path/'result.json').exists():
                    failure=failure or 'option worker result missing';continue
                child=strict_json((path/'result.json').read_bytes());children.append(child)
                report['workers'].append(dict(index=index,root=str(path.relative_to(root))))
                if process.returncode!=0 or child['status']!='raw_capture_complete':failure=failure or 'option worker capture failed'
            for key in ('frames','bytes','connections','universe_versions','retired_options','rejected_subscriptions','throttled_subscriptions'):
                report[key]=sum(child.get(key,0) for child in children)
            if failure is None and started is not None and len(children)==config.raw_processes:
                report['status']='raw_capture_complete'
            else:report['error']=failure or 'stopped before all workers initialized'
            if root.exists():_publish_bytes(root/'result.json',canonical(report))
        finally:os.close(lock)
    return dict(root=str(root),**report)


def _audit(ledger,bounds,owner,count,category,**value):
    if category=='bound':
        bounds[owner,value['event']]=(value['utc_ns'],value['monotonic_ns'])
    elif category=='universe':
        for row in value['rows']:
            expiry=row.expiry_ms*1_000_000
            if expiry>2**63-1:raise ValueError('native option expiry exceeds verification integer bound')
            live=expiry>value['utc_ns'] and row.state not in {'Closed','delivered','archivized','closed','terminated'}
            ledger.execute('INSERT INTO required VALUES(?,?,?,?,?,?,?) ON CONFLICT(symbol,incarnation) DO UPDATE SET seen=min(seen,excluded.seen),expiry=min(expiry,excluded.expiry),live=max(live,excluded.live)',
                (row.symbol,row.incarnation,option_partition(row.symbol,count),row.created_ms*1_000_000,expiry,value['monotonic_ns'],int(live)))
    elif category=='coverage':
        ledger.execute('INSERT INTO coverage VALUES(?,?,?,?,?,?) ON CONFLICT(symbol,incarnation,owner,kind) DO UPDATE SET utc=CASE WHEN excluded.mono<mono THEN excluded.utc ELSE utc END,mono=min(mono,excluded.mono)',
            (value['symbol'],value['incarnation'],owner,value['kind'],value['utc_ns'],value['monotonic_ns']))


def _cross_coverage(ledger,bounds,count):
    end_utc,end_mono=min((bounds[index,'capture_end'] for index in range(count)),key=lambda v:v[1])
    start_utc=min(bounds[index,'capture_start'][0] for index in range(count))
    query='''SELECT r.symbol,r.created,r.expiry,a.utc,a.mono,b.mono,t.mono,x.utc,x.mono
        FROM required r
        LEFT JOIN coverage a ON (r.symbol=a.symbol AND r.incarnation=a.incarnation AND r.owner=a.owner AND a.kind='ack')
        LEFT JOIN coverage b ON (r.symbol=b.symbol AND r.incarnation=b.incarnation AND r.owner=b.owner AND b.kind='book')
        LEFT JOIN coverage t ON (r.symbol=t.symbol AND r.incarnation=t.incarnation AND r.owner=t.owner AND t.kind='ticker')
        LEFT JOIN coverage x ON (r.symbol=x.symbol AND r.incarnation=x.incarnation AND r.owner=x.owner AND x.kind='retired')
        WHERE (r.live=1 AND (r.seen<=? OR r.created<=?)) OR r.created BETWEEN ? AND ?
        OR (r.created<=? AND r.expiry>? AND r.expiry<=?)'''
    checked=0
    for symbol,created,expiry,ack_utc,ack_mono,book_mono,ticker_mono,retired_utc,retired_mono in ledger.execute(query,(end_mono,end_utc,start_utc,end_utc,end_utc,start_utc,end_utc)):
        if None in (ack_mono,book_mono,ticker_mono):
            raise ValueError('cross-worker discovery lacks assigned ACK/book/ticker coverage: '+symbol)
        if start_utc<=created<=end_utc and ack_utc-created>120_000_000_000:
            raise ValueError('cross-worker creation-to-ACK exceeded 120 seconds: '+symbol)
        if expiry<=end_utc and retired_mono is None:
            raise ValueError('cross-worker expired option lacks retirement: '+symbol)
        if retired_mono is not None and max(ack_mono,book_mono,ticker_mono)>retired_mono:
            raise ValueError('cross-worker retirement precedes coverage: '+symbol)
        checked+=1
    return checked


def verify(root,report,*,export=None):
    # Cross-worker history spills to an owned temporary database, never an
    # ever-growing Python set or a mutation of the immutable archive.
    with tempfile.TemporaryDirectory(prefix='dc-option-coverage-') as scratch:
        with closing(sqlite3.connect(str(Path(scratch)/'coverage.sqlite'))) as ledger:
            ledger.execute('PRAGMA cache_size=-2048')
            ledger.executescript('''CREATE TABLE required(symbol TEXT,incarnation TEXT,owner INTEGER,created INTEGER,expiry INTEGER,seen INTEGER,live INTEGER,PRIMARY KEY(symbol,incarnation));
                CREATE TABLE coverage(symbol TEXT,incarnation TEXT,owner INTEGER,kind TEXT,utc INTEGER,mono INTEGER,PRIMARY KEY(symbol,incarnation,owner,kind));''')
            return _verify_workers(root,report,ledger,export=export)


def _verify_workers(root,report,ledger,*,export=None):
    from .dynamic_options import verify as verify_worker,PROFILE
    if export is not None:raise ValueError('export each option worker separately')
    root=Path(root).resolve();body=(root/'config.json').read_bytes();config=strict_json(body)
    if digest(body)!=report['config_sha256']:raise ValueError('worker supervisor config hash differs')
    count=config['raw_processes']
    original=Path(report['original_root'])
    if not original.is_absolute() or original.parent!=Path(config['data_root'])/'runs':
        raise ValueError('supervisor original root differs from configuration')
    if (not config['rediscover_options'] or not config['all_options'] or not 2<=count<=8 or
            report['physical_global_copies']!=count or len(report['workers'])!=count):
        raise ValueError('complete live partition set required')
    seen=set();roots=set();totals={key:0 for key in ('frames','bytes','connections','universe_versions',
        'retired_options','rejected_subscriptions','throttled_subscriptions')}
    records=0;complete=True;bounds={}
    for row in report['workers']:
        index=row['index'];path=(root/row['root']).resolve()
        if type(index) is not int or index not in range(count) or index in seen or path in roots or not path.is_relative_to(root):
            raise ValueError('duplicate or foreign worker partition')
        seen.add(index);roots.add(path)
        raw_config=(path/'config.json').read_bytes();child_config=strict_json(raw_config)
        expected=dict(config,data_root=str(original/'workers'/str(index)),raw_processes=1,
            option_partition_count=count,option_partition_index=index,duration_seconds=0,max_frames=0,max_bytes=0,
            subscription_budget_file=str(original/'request-budget.state'))
        # The parent PID is an operational guard, not a portable replay identity.
        expected['supervisor_pid']=child_config.get('supervisor_pid')
        if canonical(expected)!=raw_config:raise ValueError('worker configuration differs from supervisor plan')
        child=strict_json((path/'result.json').read_bytes())
        if child.get('schema')!='dc.dynamic-options/1' or child.get('profile')!=PROFILE or child['provenance']['source_sha256']!=report['source_sha256']:
            raise ValueError('worker capture profile/source differs')
        checked=verify_worker(path,child,audit=partial(_audit,ledger,bounds,index,count));records+=checked['records']
        complete=complete and checked['changing_universe_coverage_verified'] and child['status']=='raw_capture_complete'
        for key in totals:totals[key]+=child.get(key,0)
    if seen!=set(range(count)):raise ValueError('missing worker partition')
    if any(report.get(key)!=value for key,value in totals.items()):raise ValueError('worker aggregate counters differ')
    if report['status']=='raw_capture_complete' and not complete:raise ValueError('incomplete worker coverage')
    cross_checked=_cross_coverage(ledger,bounds,count) if complete else 0
    return dict(archive_verified=True,records=records,ws_messages=totals['frames'],
        universe_versions_verified=totals['universe_versions'],workers=count,
        changing_universe_coverage_verified=complete,full_partition_coverage_verified=complete,
        capture_status=report['status'],physical_global_copies=count,
        cross_worker_instruments_verified=cross_checked,
        verification_scope='every live partition and metadata version with ACK/first-push/retirement proof; physical global copies retained')
