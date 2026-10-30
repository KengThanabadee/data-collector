"""Raw-first continuous Deribit capture; hourly owners bound clock/stream state."""
import asyncio
import os
import shutil
from pathlib import Path
import time
import uuid

import aiohttp

from .contracts import Provenance, RawRecord, Receipt, canonical, digest, strict_json
from .decoders import native_json
from .journal import Journal, _publish_bytes
from .lane import ArchiveLane, DurableAck, PENDING_CLOCK_EPOCH
from .queues import ByteQueue
from .writer import SessionWriter
from .clocklog import DEFAULT_POLICY
from .clocksource import ChronyClockSampler
from .segments import _mkdir, iter_segment_records


async def capture(config, *, session_factory, observer, clock_read=None, clock_wait=None):
    from .app import run, _bind_root, _freeze_source, _drain
    # Preserve discovery validation and measured clock evidence through the
    # existing path. Its immutable result remains independently verifiable.
    discovered = await run(config, discover_only=True, session_factory=session_factory,
        observer=observer, clock_read=clock_read, clock_wait=clock_wait)
    if discovered['status'] == 'failed':
        return discovered
    if not config.symbols or any(s not in discovered['available_symbols'] for s in config.symbols):
        raise ValueError('select active discovered BTC/ETH option symbols')
    owner = _bind_root(config)
    root = Path(discovered['root']) / 'continuous'
    try:
        _mkdir(root)
        source = _freeze_source(root)
        provenance = Provenance(source, config.config_sha256, digest((root.parent/'universe.json').read_bytes()))
    except BaseException:
        os.close(owner)
        raise
    result = dict(schema='dc.raw-capture-result/1', status='failed', segments=[],
        symbols=list(config.symbols), frames=0, bytes=0, connections=0,
        limitations=['Raw capture with measured clock evidence; no derived snapshot or continuity across gaps'])
    start = time.monotonic()
    stopped = False
    acquired = set()
    # asyncio.Runner cancels the main task on first SIGINT. Shield the owner so
    # cancellation becomes a stop request and admitted disk operations drain.
    async def own():
        nonlocal stopped, start
        while not stopped:
            if config.duration_seconds and time.monotonic()-start >= config.duration_seconds:
                break
            identity = uuid.uuid4().hex
            writer = emergency = lane = raw = sampler = clock = None
            seq = 0
            connection = 'deribit-'+uuid.uuid4().hex
            async def save(body, channel, kind):
                nonlocal seq
                utc, mono, _ = observer()
                record = RawRecord(Receipt('capture-'+root.parent.name, 'process-'+identity, connection,
                    seq, utc, mono, PENDING_CLOCK_EPOCH), 'deribit', config.environment, 'option',
                    channel, kind, body, provenance, None, canonical({}))
                try:
                    ack = await lane.submit_received(record)
                except Exception as exc:
                    raise RuntimeError('archive admission failed') from exc
                if not isinstance(ack, DurableAck) or not ack.timing_valid:
                    raise ValueError('raw durability unconfirmed or late')
                seq += 1
            try:
                writer = SessionWriter(root/'wal', root/'archive', venue='deribit', environment=config.environment,
                    capture_id='capture-'+root.parent.name, process_session_id='process-'+identity,
                    provenance=provenance, started_utc_ns=observer()[0])
                emergency = Journal(root/(identity+'.emergency.wal'), venue='deribit', environment=config.environment, create=True)
                lane = ArchiveLane(writer, ByteQueue(), emergency_writer=emergency, observe=observer, max_connections=128, clock_policy=DEFAULT_POLICY)
                raw = asyncio.create_task(lane.run())
                sampler = ChronyClockSampler(lane, executable=config.chronyc, read=clock_read, wait_until=clock_wait, max_samples=10000)
                clock = None
                await sampler.prime()
                clock = asyncio.create_task(sampler.run())
                async with asyncio.timeout(config.warmup_timeout_seconds):
                    while not stopped and (sampler.last_ack is None or not sampler.last_ack.assessment.aligned):
                        sampler.updated.clear()
                        if clock.done():
                            clock.result()
                            raise ValueError('clock sampler ended during warmup')
                        try:
                            await asyncio.wait_for(sampler.updated.wait(), 1)
                        except TimeoutError:
                            pass
                if result['connections'] == 0:
                    start = time.monotonic()
                async with session_factory() as session:
                    # Each hourly owner deliberately reconnects; this bounds
                    # accumulated connection and clock-epoch state.
                    epoch_start = time.monotonic()
                    attempts = 0
                    while not stopped and time.monotonic()-epoch_start < 3600 and attempts < 64:
                        if config.duration_seconds and time.monotonic()-start >= config.duration_seconds:
                            return
                        if clock.done():
                            clock.result()
                            raise ValueError('clock sampler ended')
                        connection = 'deribit-'+uuid.uuid4().hex
                        seq = 0
                        attempts += 1
                        result['connections'] += 1
                        channels = ['book.'+s+'.agg2' for s in config.symbols]
                        await save(canonical(dict(event='gap_start', reason='connection_start')), 'capture_gap', 'control')
                        try:
                            endpoint = 'www.deribit.com' if config.environment=='production' else 'test.deribit.com'
                            async with asyncio.timeout(config.timeout_seconds):
                                socket = await session.ws_connect('wss://'+endpoint+'/ws/api/v2', max_msg_size=2*1024*1024,
                                    autoping=True, compress=0, timeout=aiohttp.ClientWSTimeout(ws_close=5))
                            try:
                                for request in (dict(jsonrpc='2.0',id=1,method='public/subscribe',params=dict(channels=channels)),
                                        dict(jsonrpc='2.0',id=2,method='public/set_heartbeat',params=dict(interval=10))):
                                    body = canonical(request)
                                    await save(body, request['method'], 'control')
                                    await socket.send_str(body.decode())
                                acked = False
                                gap_closed = False
                                snapshots = set()
                                connected_at = last_received = time.monotonic()
                                while not stopped:
                                    now = time.monotonic()
                                    disk = shutil.disk_usage(root)
                                    if disk.free < 5*1024**3 or disk.free/disk.total < .10:
                                        raise ValueError('disk free below v2 stop threshold')
                                    if (config.duration_seconds and now-start >= config.duration_seconds) or now-epoch_start >= 3600:
                                        break
                                    if clock.done():
                                        clock.result()
                                        raise ValueError('clock sampler ended')
                                    if now-connected_at > 30 and (not acked or len(snapshots)!=len(channels)):
                                        raise TimeoutError('subscription bootstrap timeout')
                                    try:
                                        message = await asyncio.wait_for(socket.receive(), 1)
                                    except TimeoutError:
                                        # Heartbeats should arrive every ten seconds.
                                        if now-last_received > 30:
                                            raise TimeoutError('socket idle timeout')
                                        continue
                                    last_received = time.monotonic()
                                    if message.type not in (aiohttp.WSMsgType.TEXT, aiohttp.WSMsgType.BINARY):
                                        raise ConnectionError('socket closed')
                                    body = message.data.encode() if message.type==aiohttp.WSMsgType.TEXT else message.data
                                    await save(body, 'multiplex', 'ws_message')
                                    result['frames'] += 1
                                    result['bytes'] += len(body)
                                    value = native_json(body)
                                    if 'error' in value:
                                        raise ValueError('Deribit subscription/control error')
                                    if value.get('id')==1:
                                        if not isinstance(value.get('result'),list) or sorted(value['result'])!=sorted(channels):
                                            raise ValueError('subscription ACK differs')
                                        acked = True
                                    params = value.get('params', {})
                                    if value.get('method')=='subscription' and params.get('channel') in channels:
                                        if params.get('data',{}).get('type')=='snapshot':
                                            snapshots.add(params['channel'])
                                        if acked and len(snapshots)==len(channels) and not gap_closed:
                                            await save(canonical(dict(event='gap_end', reason='native_snapshots_acquired')), 'capture_gap', 'control')
                                            gap_closed = True
                                            acquired.update(snapshots)
                                    if acked and len(snapshots)==len(channels) and not gap_closed:
                                        await save(canonical(dict(event='gap_end', reason='native_snapshots_acquired')), 'capture_gap', 'control')
                                        gap_closed = True
                                        acquired.update(snapshots)
                                    if value.get('method')=='heartbeat' and params.get('type')=='test_request':
                                        request = canonical(dict(jsonrpc='2.0',id=3,method='public/test',params={}))
                                        await save(request, 'public/test', 'control')
                                        await socket.send_str(request.decode())
                                    if ((config.max_frames and result['frames']>=config.max_frames) or
                                            (config.max_bytes and result['bytes']>=config.max_bytes)):
                                        stopped = True
                                await save(canonical(dict(event='gap_start',reason='connection_stopped')), 'capture_gap', 'control')
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
                            await asyncio.sleep(1)
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
        if len(acquired) != len(config.symbols):
            raise ValueError('capture ended before all selected native snapshots acquired')
        result['status'] = 'raw_capture_complete'
    except Exception as exc:
        result['status'] = 'failed'
        result['error'] = type(exc).__name__+': '+str(exc)
    finally:
        result['elapsed_seconds'] = time.monotonic()-start
        _publish_bytes(root/'result.json', canonical(result))
        os.close(owner)
    return dict(root=str(root), **result)


def verify(root, report, *, export=None):
    from .app import verify_run
    metadata = verify_run(root.parent)
    if not metadata['archive_verified']:
        raise ValueError('discovery archive invalid')
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
                key = record.receipt.stream_key
                if record.receipt.receive_seq != positions.get(key,-1)+1:
                    raise ValueError('receipt sequence discontinuity')
                positions[key] = record.receipt.receive_seq
                count += 1
                if output is not None:
                    output.write(record.to_bytes()+b'\n')
                if record.kind=='ws_message':
                    frames += 1
                    size += len(record.body)
        if frames!=report['frames'] or size!=report['bytes']:
            raise ValueError('raw counts differ from report')
    finally:
        if output is not None:
            output.close()
    return dict(archive_verified=True, records=count, ws_messages=frames,
        capture_status=report['status'], verification_scope='raw integrity and counts; no derived book replay')
