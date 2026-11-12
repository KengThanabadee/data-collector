"""Raw-first continuous Deribit capture with transport-driven reconnects."""
import asyncio
import json
import os
import random
import shutil
from pathlib import Path
import time
import uuid

import aiohttp

from .contracts import Provenance, RawRecord, Receipt, canonical, digest, strict_json, _pairs
from .journal import Journal, JournalLimits, _publish_bytes
from .lane import ArchiveLane, DurableAck, PENDING_CLOCK_EPOCH
from .queues import ByteQueue
from .writer import SessionWriter
from .clocklog import DEFAULT_POLICY
from .clocksource import ChronyClockSampler
from .segments import _mkdir, iter_segment_records


def _routing_json(body):
    """Classify opaque raw bytes without constructing/pricing Decimal values.

    Decimal lexemes stay strings only in this transient routing view. The
    original body is archived first and is never serialized from this view.
    Duplicate keys, nonfinite constants and non-object messages fail closed.
    """
    if type(body) is not bytes or len(body)>2*1024*1024:
        raise ValueError('native message byte bound')
    def invalid(value):raise ValueError('nonfinite native JSON number')
    try:value=json.loads(body,parse_float=str,parse_constant=invalid,object_pairs_hook=_pairs)
    except (UnicodeError,RecursionError) as exc:raise ValueError('invalid native JSON') from exc
    if not isinstance(value,dict):raise ValueError('native message must be an object')
    return value


def _future_plan(universe):
    rows=[v for v in universe['selected'] if v['instrument']['native_product']=='future']
    symbols=tuple(sorted(v['instrument']['native_symbol'] for v in rows))
    indices=set()
    for row in rows:
        name=row['attributes']['native'].get('price_index')
        if not isinstance(name,str) or not name or len(name)>128 or any(
                not (c.isascii() and (c.isalnum() or c in '_-')) for c in name):
            raise ValueError('explicit native future index dependency required')
        indices.add(name)
    if not symbols:raise ValueError('no discovered BTC/ETH futures')
    return symbols,tuple(sorted(indices))


async def capture(config, *, session_factory, observer, clock_read=None, clock_wait=None):
    from .app import run, _bind_root, _freeze_source, _drain
    # Preserve discovery validation and measured clock evidence through the
    # existing path. Its immutable result remains independently verifiable.
    if config.all_options and config.raw_processes>1:
        from .sharded import capture_shards
        return await capture_shards(config,session_factory=session_factory,observer=observer,
            clock_read=clock_read,clock_wait=clock_wait)
    if config.source_discovery_root is None:
        discovered = await run(config, discover_only=True, session_factory=session_factory,
            observer=observer, clock_read=clock_read, clock_wait=clock_wait)
    else:
        from .app import verify_run
        metadata_root=config.source_discovery_root.resolve()
        report=strict_json((metadata_root/'result.json').read_bytes())
        if report.get('mode')!='discovery' or report.get('status')!='discovery_complete' or report['environment']!=config.environment:
            raise ValueError('matching completed discovery required')
        verify_run(metadata_root)
        discovered=dict(root=str(metadata_root),**report)
    metadata_root=Path(discovered['root'])
    if discovered['status'] == 'failed':
        return discovered
    universe=strict_json((metadata_root/'universe.json').read_bytes())
    indices=()
    if config.all_futures:
        symbols,indices=_future_plan(universe)
    elif config.all_options:
        symbols=tuple(sorted(v['instrument']['native_symbol'] for v in universe['selected']
            if v['instrument']['native_product']=='option'))
    else:
        symbols=config.symbols
    listed={v['instrument']['native_symbol'] for v in universe['selected']
        if v['instrument']['native_product']==('future' if config.all_futures else 'option')}
    if not symbols or not set(symbols)<=listed:
        raise ValueError('select discovered BTC/ETH symbols in the chosen family')
    whole_family=config.all_options or config.all_futures
    feeds=('book','ticker','trades') if whole_family else config.raw_feeds
    batching=whole_family or len(symbols)>2 or feeds!=('book',)
    channels=tuple(f'{feed}.{symbol}.100ms' for symbol in symbols for feed in feeds)+tuple(
        'deribit_price_index.'+name for name in indices)
    channel_set=set(channels)
    books={c for c in channels if c.startswith('book.')}
    tickers={c for c in channels if c.startswith('ticker.')}
    index_channels={'deribit_price_index.'+name for name in indices}
    batches=[channels[i:i+config.subscription_batch_size] for i in range(0,len(channels),config.subscription_batch_size)]
    owner = _bind_root(config)
    root = config.data_root/'continuous' if config.source_discovery_root is not None else metadata_root/'continuous'
    try:
        _mkdir(root)
        source = _freeze_source(root)
        _publish_bytes(root/'config.json',config.to_bytes())
        provenance = Provenance(source, config.config_sha256, digest((metadata_root/'universe.json').read_bytes()))
    except BaseException:
        os.close(owner)
        raise
    result = dict(schema='dc.raw-capture-result/1', status='failed', segments=[],
        symbols=list(symbols), all_options=config.all_options, all_futures=config.all_futures,
        index_names=list(indices), metadata_ref=os.path.relpath(metadata_root,root), metadata_sha256=provenance.universe_sha256, channels=list(channels), frames=0, bytes=0, connections=0,
        rejected_messages=0, throttled_messages=0, subscribed_channels=0,
        subscribed_channels_peak=0, fully_initialized_connections=0, socket_events=0,
        limitations=['Raw capture with measured clock evidence; no derived snapshot or continuity across gaps'])
    start = time.monotonic()
    stopped = False
    acquired = set()
    # asyncio.Runner cancels the main task on first SIGINT. Shield the owner so
    # cancellation becomes a stop request and admitted disk operations drain.
    def supervisor_alive():
        if config.supervisor_pid is not None and os.getppid()!=config.supervisor_pid:
            result['stop_reason']='supervisor_exit'
            return False
        return True
    async def own():
        nonlocal stopped, start
        while not stopped and supervisor_alive():
            if config.duration_seconds and time.monotonic()-start >= config.duration_seconds:
                break
            identity = uuid.uuid4().hex
            writer = emergency = lane = raw = sampler = clock = None
            seq = 0
            connection = 'deribit-'+uuid.uuid4().hex
            def admit(body, channel, kind):
                nonlocal seq
                utc, mono, _ = observer()
                record = RawRecord(Receipt('capture-'+metadata_root.name, 'process-'+identity, connection,
                    seq, utc, mono, PENDING_CLOCK_EPOCH), 'deribit', config.environment, 'multiplex' if config.all_futures else 'option',
                    channel, kind, body, provenance, None, canonical({}))
                try:future=lane.submit_received(record)
                except Exception as exc:raise RuntimeError('archive admission failed') from exc
                seq+=1
                return future
            async def save(body, channel, kind):
                ack=await admit(body,channel,kind)
                if not isinstance(ack,DurableAck) or not ack.timing_valid:
                    raise ValueError('raw durability unconfirmed or late')
            try:
                writer = SessionWriter(root/'wal', root/'archive', venue='deribit', environment=config.environment,
                    capture_id='capture-'+metadata_root.name, process_session_id='process-'+identity,
                    provenance=provenance, started_utc_ns=observer()[0],
                    limits=JournalLimits(target_bytes=config.raw_segment_bytes))
                emergency = Journal(root/(identity+'.emergency.wal'), venue='deribit', environment=config.environment, create=True)
                lane = ArchiveLane(writer, ByteQueue(), emergency_writer=emergency, observe=observer, max_connections=128, clock_policy=DEFAULT_POLICY,
                    clock_max_assessments=None,raw_batch_size=512 if batching else 1)
                raw = asyncio.create_task(lane.run())
                sampler = ChronyClockSampler(lane, executable=config.chronyc, read=clock_read, wait_until=clock_wait, max_samples=None)
                clock = None
                await sampler.prime()
                clock = asyncio.create_task(sampler.run())
                async with asyncio.timeout(config.warmup_timeout_seconds):
                    while not stopped and supervisor_alive() and (sampler.last_ack is None or not sampler.last_ack.assessment.aligned):
                        sampler.updated.clear()
                        if clock.done():
                            clock.result()
                            raise ValueError('clock sampler ended during warmup')
                        try:
                            await asyncio.wait_for(sampler.updated.wait(), 1)
                        except TimeoutError:
                            pass
                if not supervisor_alive():stopped=True
                if result['connections'] == 0:
                    start = time.monotonic()
                async with session_factory() as session:
                    attempts = 0
                    retry_seconds = 1
                    # Rebind bounded stream state only after real connection loss;
                    # a healthy socket has no timer or attempt rollover.
                    while not stopped and attempts < 64:
                        if config.duration_seconds and time.monotonic()-start >= config.duration_seconds:
                            return
                        if clock.done():
                            clock.result()
                            raise ValueError('clock sampler ended')
                        connection = 'deribit-'+uuid.uuid4().hex
                        seq = 0
                        attempts += 1
                        result['connections'] += 1
                        await save(canonical(dict(event='gap_start', reason='connection_start')), 'capture_gap', 'control')
                        try:
                            endpoint = 'www.deribit.com' if config.environment=='production' else 'test.deribit.com'
                            async with asyncio.timeout(config.timeout_seconds):
                                socket = await session.ws_connect('wss://'+endpoint+'/ws/api/v2', max_msg_size=2*1024*1024,
                                    autoping=True, compress=0, timeout=aiohttp.ClientWSTimeout(ws_close=5))
                            try:
                                batch_index=0
                                pending={}
                                acknowledged=set()
                                async def send_batch():
                                    nonlocal batch_index
                                    request_id=1 if batch_index==0 else 10+batch_index
                                    batch=batches[batch_index]
                                    request=dict(jsonrpc='2.0',id=request_id,method='public/subscribe',params=dict(channels=batch))
                                    body=canonical(request)
                                    await save(body,'public/subscribe','control')
                                    await asyncio.wait_for(socket.send_str(body.decode()),config.timeout_seconds)
                                    pending[request_id]=set(batch)
                                    batch_index+=1
                                from .rawquota import take_slot
                                while not take_slot(config.subscription_budget_file):
                                    if stopped or not supervisor_alive():stopped=True;return
                                    await asyncio.sleep(.05)
                                await send_batch()
                                body=canonical(dict(jsonrpc='2.0',id=2,method='public/set_heartbeat',params=dict(interval=10)))
                                await save(body,'public/set_heartbeat','control')
                                await asyncio.wait_for(socket.send_str(body.decode()),config.timeout_seconds)
                                next_batch=time.monotonic()+1
                                acked = False
                                gap_closed = False
                                snapshots = set()
                                ticker_seen=set()
                                index_seen=set()
                                last_disk_check=0
                                connected_at = last_received = time.monotonic()
                                while not stopped:
                                    if not supervisor_alive():stopped=True;break
                                    now = time.monotonic()
                                    if now-last_disk_check>=1:
                                        disk = shutil.disk_usage(root)
                                        if disk.free < 5*1024**3 or disk.free/disk.total < .10:
                                            raise ValueError('disk free below v2 stop threshold')
                                        last_disk_check=now
                                    if batch_index<len(batches) and now>=next_batch and take_slot(config.subscription_budget_file):
                                        await send_batch()
                                        next_batch=time.monotonic()+1
                                    if (config.duration_seconds and now-start >= config.duration_seconds):
                                        break
                                    if clock.done():
                                        clock.result()
                                        raise ValueError('clock sampler ended')
                                    if now-connected_at > config.bootstrap_timeout_seconds and not gap_closed:
                                        raise TimeoutError('subscription bootstrap timeout')
                                    try:
                                        message = await asyncio.wait_for(socket.receive(), 1)
                                    except TimeoutError:
                                        # Heartbeats should arrive every ten seconds.
                                        if now-last_received > 30:
                                            raise TimeoutError('socket idle timeout')
                                        continue
                                    deliveries=[];native_bytes=0;closed=False;receive_error=None;closed_future=None
                                    deadline=time.monotonic()+.05
                                    try:
                                        while True:
                                            if message.type not in (aiohttp.WSMsgType.TEXT,aiohttp.WSMsgType.BINARY):
                                                closed=True
                                                event=canonical(dict(type=int(message.type),
                                                    code=message.data if type(message.data) is int else None,
                                                    detail=str(message.data)[:1024],reason=str(message.extra)[:1024]))
                                                closed_future=admit(event,'socket_event','control')
                                                break
                                            last_received=time.monotonic()
                                            body=message.data.encode() if message.type==aiohttp.WSMsgType.TEXT else message.data
                                            # Receipt and FIFO admission precede the next socket await;
                                            # clock commands remain ordered even inside a receive burst.
                                            deliveries.append((body,admit(body,'multiplex','ws_message')))
                                            native_bytes+=len(body)
                                            if (not batching or len(deliveries)>=512 or native_bytes>=1024*1024 or
                                                    (config.max_frames and result['frames']+len(deliveries)>=config.max_frames) or
                                                    (config.max_bytes and result['bytes']+native_bytes>=config.max_bytes)):
                                                break
                                            remaining=deadline-time.monotonic()
                                            if remaining<=0:break
                                            message=await asyncio.wait_for(socket.receive(),remaining)
                                    except TimeoutError:pass
                                    except Exception as exc:receive_error=exc
                                    acks=await asyncio.gather(*(future for _,future in deliveries),
                                        *((closed_future,) if closed_future is not None else ()))
                                    if closed_future is not None:result['socket_events']+=1
                                    if any(not isinstance(a,DurableAck) or not a.timing_valid for a in acks):
                                        raise ValueError('raw batch durability unconfirmed or late')
                                    result['frames']+=len(deliveries);result['bytes']+=native_bytes
                                    for body,_ in deliveries:
                                        value = _routing_json(body)
                                        if 'error' in value:
                                            result['rejected_messages']+=1
                                            if value['error'].get('code')==10028:
                                                result['throttled_messages']+=1
                                            raise ValueError('Deribit subscription/control error: '+str(value['error']))
                                        if type(value.get('id')) is int and value['id'] in pending:
                                            expected=pending.pop(value['id'])
                                            received=value.get('result')
                                            if not isinstance(received,list) or any(not isinstance(c,str) for c in received) or len(received)!=len(expected) or set(received)!=expected:
                                                result['rejected_messages']+=1
                                                raise ValueError('subscription ACK differs')
                                            acknowledged.update(expected)
                                            result['subscribed_channels']=len(acknowledged)
                                            result['subscribed_channels_peak']=max(result['subscribed_channels_peak'],len(acknowledged))
                                            acked=acknowledged==channel_set
                                        params = value.get('params', {})
                                        if value.get('method')=='subscription':
                                            channel=params.get('channel')
                                            if channel not in channel_set:
                                                raise ValueError('unexpected subscription channel')
                                            data=params.get('data')
                                            if channel in books:
                                                if not isinstance(data,dict) or data.get('instrument_name')!=channel[5:-6]:
                                                    raise ValueError('book identity differs')
                                                if data.get('type')=='snapshot':snapshots.add(channel)
                                            elif channel in tickers:
                                                if not isinstance(data,dict) or data.get('instrument_name')!=channel[7:-6]:
                                                    raise ValueError('ticker identity differs')
                                                ticker_seen.add(channel)
                                            elif channel in index_channels:
                                                if not isinstance(data,dict) or data.get('index_name')!=channel[len('deribit_price_index.'):]:
                                                    raise ValueError('index identity differs')
                                                index_seen.add(channel)
                                        if not gap_closed and acked and snapshots==books and ticker_seen==tickers and index_seen==index_channels:
                                            await save(canonical(dict(event='gap_end', reason='native_snapshots_acquired')), 'capture_gap', 'control')
                                            gap_closed = True
                                            retry_seconds = 1
                                            result['fully_initialized_connections']+=1
                                            acquired.update(books)
                                            if 'ready_utc_ns' not in result:
                                                result['ready_utc_ns']=observer()[0]
                                                _publish_bytes(root/'ready.json',canonical(dict(schema='dc.raw-ready/1',utc_ns=result['ready_utc_ns'],channels=len(channels))))
                                                if whole_family:start=time.monotonic()
                                        if value.get('method')=='heartbeat' and params.get('type')=='test_request':
                                            request = canonical(dict(jsonrpc='2.0',id=3,method='public/test',params={}))
                                            await save(request, 'public/test', 'control')
                                            await socket.send_str(request.decode())
                                        if ((config.max_frames and result['frames']>=config.max_frames) or
                                                (config.max_bytes and result['bytes']>=config.max_bytes)):
                                            stopped = True
                                    if receive_error is not None:raise receive_error
                                    if closed:raise ConnectionError('socket closed')
                                await save(canonical(dict(event='gap_start',reason=result.get('stop_reason','connection_stopped'))), 'capture_gap', 'control')
                            finally:
                                try:
                                    await save(canonical(dict(event='gap_start',reason='connection_end')), 'capture_gap', 'control')
                                finally:
                                    await asyncio.wait_for(socket.close(), 5)
                        except (aiohttp.ClientError, OSError, TimeoutError) as exc:
                            # Disk OSError must never be treated as transport loss.
                            # Check lane health before recording the transport gap.
                            if lane is not None and lane.health()['writer_error'] is not None:
                                raise
                            await save(canonical(dict(event='gap_start', reason=type(exc).__name__)), 'capture_gap', 'control')
                        if not stopped:
                            # Failed DNS/connect bursts observed in the hour run
                            # share the venue's bounded reconnect/backoff policy.
                            retry_until=time.monotonic()+random.uniform(.8,1)*retry_seconds
                            while not stopped and time.monotonic()<retry_until:
                                if not supervisor_alive():stopped=True;break
                                if config.duration_seconds and time.monotonic()-start>=config.duration_seconds:
                                    stopped=True;break
                                await asyncio.sleep(min(.25,max(0,retry_until-time.monotonic())))
                            retry_seconds=min(60,retry_seconds*2)
            finally:
                if sampler is not None:
                    sampler.stop()
                if clock is not None:
                    await _drain(asyncio.gather(clock, return_exceptions=True))
                if lane is not None:
                    lane.stop()
                    await _drain(raw)
                result['segments'].extend(str(p.relative_to(root)) for p in (() if lane is None else lane.segments))
                if emergency is not None:
                    emergency.close()
                if writer is not None:
                    writer.abort()
                if lane is not None and lane.health()['writer_error'] is not None:
                    raise ValueError('archive writer failed: '+str(lane.health()['writer_error']))
    task = asyncio.create_task(own())
    try:
        try:
            await asyncio.shield(task)
        except asyncio.CancelledError:
            stopped = True
            await _drain(task)
        if acquired != books or not result['fully_initialized_connections']:
            raise ValueError('capture ended before all selected native snapshots acquired')
        result['status'] = 'raw_capture_complete'
    except Exception as exc:
        result['status'] = 'failed'
        result['error'] = type(exc).__name__+': '+str(exc)
    finally:
        result['elapsed_seconds'] = time.monotonic()-start
        try:_publish_bytes(root/'result.json', canonical(result))
        finally:os.close(owner)
    return dict(root=str(root), **result)


def verify(root, report, *, export=None):
    from .app import verify_run
    metadata_root=(root/report.get('metadata_ref','..')).resolve()
    universe_hash=digest((metadata_root/'universe.json').read_bytes())
    if report.get('metadata_sha256',universe_hash)!=universe_hash:raise ValueError('metadata hash differs')
    metadata = verify_run(metadata_root)
    if not metadata['archive_verified']:
        raise ValueError('discovery archive invalid')
    planned=set(report.get('channels',()))
    if report.get('all_options'):
        universe=strict_json((metadata_root/'universe.json').read_bytes())
        expected_symbols={v['instrument']['native_symbol'] for v in universe['selected']
            if v['instrument']['native_product']=='option'}
        expected={f'{feed}.{symbol}.100ms' for symbol in expected_symbols for feed in ('book','ticker','trades')}
        if set(report['symbols'])!=expected_symbols or planned!=expected:
            raise ValueError('all-option plan differs from archived discovery')
    if report.get('all_futures'):
        universe=strict_json((metadata_root/'universe.json').read_bytes())
        symbols,indices=_future_plan(universe)
        expected={f'{feed}.{symbol}.100ms' for symbol in symbols for feed in ('book','ticker','trades')}
        expected.update('deribit_price_index.'+name for name in indices)
        if set(report['symbols'])!=set(symbols) or set(report.get('index_names',()))!=set(indices) or planned!=expected:
            raise ValueError('all-future/index plan differs from archived discovery')
    check_coverage=bool(planned) and report['status']=='raw_capture_complete'
    expected_books={c for c in planned if c.startswith('book.')}
    expected_tickers={c for c in planned if c.startswith('ticker.')}
    expected_indices={c for c in planned if c.startswith('deribit_price_index.')}
    active=None
    pending={}
    acked=set();book_seen=set();ticker_seen=set();index_seen=set()
    coverage_complete=False
    count = frames = size = 0
    positions = {}
    seen = set()
    output = None if export is None else Path(export).open('xb')
    try:
        for name in report['segments']:
            path = (root/name).resolve()
            if not path.is_relative_to(root):
                raise ValueError('segment outside run root')
            if path in seen:
                raise ValueError('duplicate segment')
            seen.add(path)
            for record in iter_segment_records(path):
                if report.get('metadata_sha256') and record.provenance.universe_sha256!=universe_hash:
                    raise ValueError('raw universe provenance differs')
                key = record.receipt.stream_key
                if record.receipt.receive_seq != positions.get(key,-1)+1:
                    raise ValueError('receipt sequence discontinuity')
                positions[key] = record.receipt.receive_seq
                if check_coverage and record.channel=='public/subscribe':
                    if key!=active:
                        active=key;pending={};acked=set();book_seen=set();ticker_seen=set();index_seen=set()
                    request=strict_json(record.body)
                    pending[request['id']]=set(request['params']['channels'])
                count += 1
                if output is not None:
                    output.write(record.to_bytes()+b'\n')
                if record.kind=='ws_message':
                    if report.get('all_futures') and record.native_product!='multiplex':
                        raise ValueError('mixed future/index socket product differs')
                    frames += 1
                    size += len(record.body)
                    if check_coverage and not coverage_complete and key==active:
                        value=strict_json(record.body)
                        if type(value.get('id')) is int and value['id'] in pending:
                            expected=pending.pop(value['id'])
                            reply=value.get('result')
                            if isinstance(reply,list) and all(isinstance(c,str) for c in reply) and len(reply)==len(expected) and set(reply)==expected and expected<=planned:
                                acked.update(expected)
                        if value.get('method')=='subscription':
                            params=value.get('params',{});channel=params.get('channel');data=params.get('data')
                            if channel in expected_books and isinstance(data,dict) and data.get('type')=='snapshot' and data.get('instrument_name')==channel[5:-6]:
                                book_seen.add(channel)
                            if channel in expected_tickers and isinstance(data,dict) and data.get('instrument_name')==channel[7:-6]:
                                ticker_seen.add(channel)
                            if channel in expected_indices and isinstance(data,dict) and data.get('index_name')==channel[len('deribit_price_index.'):]:
                                index_seen.add(channel)
                        if acked==planned and book_seen==expected_books and ticker_seen==expected_tickers and index_seen==expected_indices:
                            coverage_complete=True
        if planned and report['status']=='raw_capture_complete' and not coverage_complete:
            raise ValueError('selected feed acknowledgements/initial data incomplete')
        if frames!=report['frames'] or size!=report['bytes']:
            raise ValueError('raw counts differ from report')
    finally:
        if output is not None:
            output.close()
    return dict(archive_verified=True, records=count, ws_messages=frames,
        capture_status=report['status'], channels_planned=len(planned), initial_feed_coverage_verified=coverage_complete, verification_scope='raw integrity and counts; no derived book replay')
