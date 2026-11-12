"""Raw all-option workers sharing one frozen discovery and subscription budget."""
import asyncio
from dataclasses import replace
import os
from pathlib import Path
import signal
import sys
import time

from .contracts import canonical, digest, strict_json
from .journal import _publish_bytes
from .segments import _mkdir


def partition(symbols, count):
    symbols=tuple(sorted(symbols))
    return tuple(symbols[i::count] for i in range(min(count,len(symbols))))


async def capture_shards(config, *, session_factory, observer, clock_read=None, clock_wait=None):
    from .app import run, _bind_root, _drain
    discovered=await run(config,discover_only=True,session_factory=session_factory,observer=observer,
        clock_read=clock_read,clock_wait=clock_wait)
    if discovered['status']=='failed':return discovered
    metadata_root=Path(discovered['root'])
    universe_bytes=(metadata_root/'universe.json').read_bytes()
    universe=strict_json(universe_bytes)
    symbols=tuple(v['instrument']['native_symbol'] for v in universe['selected']
        if v['instrument']['native_product']=='option')
    groups=partition(symbols,config.raw_processes)
    if not groups:raise ValueError('no listed BTC/ETH options')
    owner=_bind_root(config)
    root=metadata_root/'all-options'
    processes=[];files=[];rows=[]
    stopped=False;started=None
    result=dict(schema='dc.raw-sharded-result/1',status='failed',all_options=True,
        metadata_ref='..',metadata_sha256=digest(universe_bytes),symbols=sorted(symbols),
        shards=[],elapsed_seconds=0)
    async def own():
        nonlocal started
        _mkdir(root)
        budget=root/'subscription-budget.state'
        _publish_bytes(budget,b'0')
        for index,group in enumerate(groups):
            if stopped:break
            data_root=root/'workers'/str(index)
            child=replace(config,data_root=data_root,all_options=False,raw_processes=1,
                symbols=group,raw_feeds=('book','ticker','trades'),duration_seconds=0,
                max_frames=0,max_bytes=0,source_discovery_root=metadata_root,
                subscription_budget_file=budget,supervisor_pid=os.getpid())
            # JSON effective config is converted to a minimal, literal TOML;
            # only the supervisor writes these fresh worker configurations.
            import json
            values=strict_json(child.to_bytes())
            def toml(value):
                if isinstance(value,bool):return 'true' if value else 'false'
                if isinstance(value,int):return str(value)
                return json.dumps(value,ensure_ascii=False)
            config_path=root/f'worker-{index}.toml'
            _publish_bytes(config_path,('[data_collector]\n'+''.join(
                f'{k}={toml(v)}\n' for k,v in values.items() if v is not None)).encode())
            out=(root/f'worker-{index}.stdout').open('xb');err=(root/f'worker-{index}.stderr').open('xb')
            files.extend((out,err))
            process=await asyncio.create_subprocess_exec(sys.executable,'-m','data_collector','capture',
                '--config',str(config_path),stdout=out,stderr=err,cwd=Path(__file__).parent.parent)
            processes.append(process)
            rows.append(dict(root=str((data_root/'continuous').relative_to(root)),symbols=list(group),pid=process.pid))
        result['shards']=rows
        deadline=time.monotonic()+config.warmup_timeout_seconds+config.bootstrap_timeout_seconds+60
        while not stopped:
            if any(p.returncode is not None for p in processes):
                raise ValueError('raw worker ended before requested stop; inspect worker stderr/results')
            ready=all((root/row['root']/'ready.json').exists() for row in rows)
            if started is None and ready:
                started=time.monotonic()
                _publish_bytes(root/'ready.json',canonical(dict(schema='dc.raw-sharded-ready/1',
                    utc_ns=observer()[0],workers=len(rows),symbols=len(symbols),channels=3*len(symbols))))
            if started is None and time.monotonic()>=deadline:
                raise TimeoutError('all-option worker initialization deadline')
            if started is not None and config.duration_seconds and time.monotonic()-started>=config.duration_seconds:
                break
            await asyncio.sleep(.1)
    task=asyncio.create_task(own())
    failure=None
    try:
        try:await asyncio.shield(task)
        except asyncio.CancelledError:
            stopped=True
            await _drain(task)
    except Exception as exc:failure=type(exc).__name__+': '+str(exc)
    finally:
        stopped=True
        for process in processes:
            if process.returncode is None:
                try:process.send_signal(signal.SIGINT)
                except ProcessLookupError:pass
        # Each worker drains admitted disk work; do not kill immutable evidence
        # just to shorten stop latency. A hung disk remains an operator fault.
        await _drain(asyncio.gather(*(p.wait() for p in processes)))
        for f in files:f.close()
        result['elapsed_seconds']=0 if started is None else time.monotonic()-started
        reports=[]
        for row,process in zip(rows,processes):
            path=root/row['root']/'result.json'
            if not path.exists():failure=failure or 'worker result missing';continue
            report=strict_json(path.read_bytes());reports.append(report)
            if process.returncode!=0 or report['status']!='raw_capture_complete':
                failure=failure or 'worker capture failed; inspect retained worker result'
        for key in ('frames','bytes','connections','rejected_messages','throttled_messages','socket_events','fully_initialized_connections'):
            result[key]=sum(r.get(key,0) for r in reports)
        result['subscribed_channels']=sum(r.get('subscribed_channels',0) for r in reports)
        result['subscribed_channels_peak']=sum(r.get('subscribed_channels_peak',0) for r in reports)
        if failure is None and started is not None and len(reports)==len(groups):result['status']='raw_capture_complete'
        else:result['error']=failure or 'stopped before all workers initialized'
        try:
            if root.exists():_publish_bytes(root/'result.json',canonical(result))
        finally:os.close(owner)
    return dict(root=str(root),**result)


def verify(root, report, *, export=None):
    from .app import verify_run
    from .continuous import verify as verify_worker
    root=Path(root).resolve()
    if export is not None:raise ValueError('export each raw worker separately')
    metadata_root=(root/report['metadata_ref']).resolve()
    body=(metadata_root/'universe.json').read_bytes()
    if digest(body)!=report['metadata_sha256']:raise ValueError('discovery identity differs')
    expected={v['instrument']['native_symbol'] for v in strict_json(body)['selected']
        if v['instrument']['native_product']=='option'}
    if set(report['symbols'])!=expected:raise ValueError('all-options universe differs')
    seen=set();records=frames=payload=planned=0;complete=True
    for row in report['shards']:
        path=(root/row['root']).resolve()
        if not path.is_relative_to(root):raise ValueError('worker outside run root')
        selected=set(row['symbols'])
        if not selected or seen & selected:raise ValueError('duplicate/empty worker assignment')
        seen.update(selected)
        child=strict_json((path/'result.json').read_bytes())
        channels={f'{feed}.{symbol}.100ms' for symbol in selected for feed in ('book','ticker','trades')}
        if set(child['symbols'])!=selected or set(child['channels'])!=channels:
            raise ValueError('worker feed plan differs')
        if (path/child['metadata_ref']).resolve()!=metadata_root:raise ValueError('worker discovery reference differs')
        checked=verify_worker(path,child)
        records+=checked['records'];frames+=checked['ws_messages'];payload+=child['bytes'];planned+=len(channels)
        complete=complete and checked['initial_feed_coverage_verified'] and child['status']=='raw_capture_complete'
    if seen!=expected:raise ValueError('worker union omits listed options')
    if frames!=report['frames'] or payload!=report['bytes']:raise ValueError('worker aggregate counters differ')
    if report['status']=='raw_capture_complete' and not complete:raise ValueError('incomplete worker coverage')
    return dict(archive_verified=True,records=records,ws_messages=frames,channels_planned=planned,
        initial_feed_coverage_verified=complete,workers=len(report['shards']),capture_status=report['status'],
        verification_scope='raw integrity, frozen-universe union and per-worker feed initialization; no derived replay')
