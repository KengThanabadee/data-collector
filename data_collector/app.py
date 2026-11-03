"""A bounded Deribit command assembled from the existing v2 components."""
import asyncio
from dataclasses import asdict, replace
import os
from pathlib import Path
import shutil
import time
import uuid

from .books import SnapshotBook
from .clocklog import DEFAULT_POLICY
from .clocksource import ChronyClockSampler
from .config import DiscoveryConfig
from .contracts import Provenance, StreamKey, canonical, digest, strict_json
from .cutoffs import MINUTE_NS
from .decoders import decode_depth
from .deliveries import MAX_REPORT_BYTES
from .deribit import DeribitBookSession, DeribitLimits
from .deribit_metadata import verify_metadata_sources
from .discovery import DeribitDiscovery, discovery_session
from .journal import Journal, _publish_bytes
from .lane import ArchiveLane, SnapshotOrigin
from .pipeline import SnapshotPipeline
from .publisher import SnapshotPublisher
from .queues import ByteQueue
from .quota import PriorityQuota, QuotaWindow
from .replay import BookBinding, ReplayLimits, SnapshotPlan, _book_message
from .runtime import DeribitCapture
from .schedule import MinuteCutoffs
from .segments import _mkdir, iter_segment_records
from .snapshots import PublicationLimits, plan_from_descriptor, read_snapshot
from .validity import FeedTarget
from .writer import SessionWriter, _lock

# Sized from the retained raw discovery response plus the complete derived batch.
# Library defaults stay unchanged; each run freezes this bounded pilot profile.
PILOT_REPLAY_LIMITS = ReplayLimits(max_metadata_bytes=16*1024*1024)


def observe():
    # The command targets Linux with local chrony. A boot identity is required
    # for measured cutoffs; a wall-clock-only fallback would make a false claim.
    boot = Path('/proc/sys/kernel/random/boot_id').read_text().strip()
    return time.time_ns(), time.monotonic_ns(), boot


async def _drain(future):
    """Do not interrupt already admitted disk work during cancellation cleanup."""
    while True:
        try:return await asyncio.shield(future)
        except asyncio.CancelledError:
            if future.done():return future.result()


def _bind_root(config):
    root = config.data_root.resolve()
    if any((parent/'environment.json').exists() for parent in (root, *root.parents)):
        raise ValueError('v2 cannot write inside a v1 archive root')
    owner = _lock(root)
    try:
        marker = root/'dc-root.json'
        if not marker.exists() and set(p.name for p in root.iterdir()) != {'owner.lock'}:
            raise ValueError('nonempty unbound v2 root; choose a separate data_root')
        _publish_bytes(marker, canonical(dict(schema='dc.root/2', environment=config.environment)))
    except BaseException:
        os.close(owner)
        raise
    return owner


def _freeze_source(root):
    package = Path(__file__).parent
    contents = {p.name:p.read_bytes() for p in sorted(package.glob('*.py'))}
    manifest = canonical(dict(schema='dc.collector-source/1',
        files={name:digest(body) for name,body in contents.items()},
        scope='collector-python-only; vendor-schema release gate pending'))
    identity = digest(manifest)
    target = root/'source'/identity
    _mkdir(target)
    for name,body in contents.items():_publish_bytes(target/name, body)
    _publish_bytes(target/'manifest.json', manifest)
    return identity


def verify_run(root, *, export=None):
    """Read committed raw afresh, rebuild final books, and optionally export raw."""
    root = Path(root).resolve()
    report = strict_json((root/'result.json').read_bytes())
    if report.get('schema')=='dc.option-workers/1':
        from .option_workers import verify
        return verify(root,report,export=export)
    if report.get('schema')=='dc.dynamic-options/1':
        from .dynamic_options import verify
        return verify(root,report,export=export)
    if report.get('schema')=='dc.public-discovery/1' and report.get('venue')=='binance':
        from .binance import verify
        return verify(root,report,export=export)
    if report.get('schema') == 'dc.raw-sharded-result/1':
        from .sharded import verify
        return verify(root,report,export=export)
    if report.get('schema') == 'dc.raw-capture-result/1':
        from .continuous import verify
        return verify(root, report, export=export)
    return _verify_report(root,report,export=export)


def _verify_report(root, report, *, export=None):
    if report.get('schema') != 'dc.capture-result/2':raise ValueError('v2 capture result required')
    limits = ReplayLimits(**report.get('replay_limits',{}))
    paths = []
    for name in report['segments']:
        path = (root/name).resolve()
        if not path.is_relative_to(root):raise ValueError('segment outside run root')
        paths.append(path)
    def records():
        for path in paths:yield from iter_segment_records(path)
    metadata = [r for r in records() if r.kind=='metadata_version']
    verify_metadata_sources(records, metadata,max_records=limits.max_records,
        max_scan_bytes=limits.max_scan_bytes,max_source_bytes=limits.max_metadata_bytes)
    plan = None if report['plan'] is None else plan_from_descriptor(report['plan'])
    models = [] if plan is None else [SnapshotBook(b.target.instrument,b.target.profile) for b in plan.bindings]
    count = messages = 0
    output = None
    try:
        if export is not None:output = Path(export).open('xb')
        for record in records():
            count += 1
            if output is not None:output.write(record.to_bytes()+b'\n')
            if record.kind!='ws_message':continue
            messages += 1
            if plan is None:continue
            for binding,model in zip(plan.bindings,models):
                if record.receipt.stream_key!=binding.ws_source:continue
                if _book_message(record,binding) is not None:
                    model.receive(decode_depth(replace(record,instrument=binding.target.instrument,
                        channel=binding.native_channel),binding.target.profile))
        if output is not None:output.flush();os.fsync(output.fileno())
    finally:
        if output is not None:output.close()
    books = [strict_json(canonical(model.view())) for model in models]
    equal = books==report['books']
    for snapshot_id in report['snapshot_ids']:
        read_snapshot(root/'published'/'snapshots',snapshot_id,paths,limits=PublicationLimits(replay=limits))
    return dict(records=count,ws_messages=messages,metadata_versions=len(metadata),
        archive_verified=True,book_replay_equal=equal,books=books,
        snapshots_verified=len(report['snapshot_ids']),capture_status=report['status'])


async def _capture_for(capture, session, seconds):
    running = asyncio.create_task(capture.run(session))
    declared = asyncio.create_task(capture.receiver.declared.wait())
    timer = None
    try:
        await asyncio.wait((running,declared),return_when=asyncio.FIRST_COMPLETED)
        if not running.done():
            timer = asyncio.create_task(asyncio.sleep(seconds))
            await asyncio.wait((running,timer),return_when=asyncio.FIRST_COMPLETED)
            if not running.done():capture.stop()
        return await asyncio.shield(running)
    finally:
        capture.stop()
        for task in (declared,timer):
            if task is not None and not task.done():task.cancel()
        await _drain(asyncio.gather(*(t for t in (declared,timer) if t is not None),return_exceptions=True))
        await _drain(asyncio.gather(running,return_exceptions=True))


async def run(config, *, discover_only=False, session_factory=discovery_session,
              observer=observe, clock_read=None, clock_wait=None, schedule_wait=None):
    """Own one declared pilot. Internal clock/transport seams are for fixtures."""
    if isinstance(config,DiscoveryConfig) and config.venue=='binance':
        from .binance import run as binance_run
        return await binance_run(config,discover_only=discover_only,session_factory=session_factory,
            observer=observer,clock_read=clock_read,clock_wait=clock_wait)
    if not discover_only and config.all_futures:
        raise ValueError('Deribit futures feeds are not_enabled: downloadable history; inverse options are the selected live scope')
    if (not discover_only and config.capture_mode=='raw' and not config.rediscover_options
            and (config.symbols or config.all_options)):
        raise ValueError('legacy Deribit raw capture is not_enabled; use the live inverse-options configuration with rediscover_options=true')
    if not discover_only and config.rediscover_options:
        from .dynamic_options import capture
        return await capture(config,session_factory=session_factory,observer=observer,clock_read=clock_read,clock_wait=clock_wait)
    if not discover_only and not config.symbols and not (config.all_options or config.all_futures):
        raise ValueError('set explicit current option symbols before capture')
    if not discover_only and config.capture_mode == 'raw':
        from .continuous import capture
        return await capture(config, session_factory=session_factory, observer=observer, clock_read=clock_read, clock_wait=clock_wait)
    if not isinstance(config,DiscoveryConfig):raise ValueError('typed v2 config required')
    if not discover_only and not config.symbols and not (config.all_options or config.all_futures):raise ValueError('set explicit current option symbols before capture')
    if observer is observe:
        observe()
        if shutil.which(config.chronyc) is None:raise ValueError('install/configure local chrony before capture')
    owner = _bind_root(config)
    writer = emergency = lane = raw_task = sampler = capture = quota = publisher = None
    root = config.data_root/'runs'/uuid.uuid4().hex
    failure = None
    result = dict(schema='dc.capture-result/2',status='failed',mode='discovery' if discover_only else 'capture',
        environment=config.environment,symbols=list(config.symbols),plan=None,books=[],snapshot_ids=[],segments=[],
        replay_limits=asdict(PILOT_REPLAY_LIMITS),
        limitations=['Bounded Deribit option books, agg2 cadence; other feeds/venues pending',
                     'No full-universe, historical sufficiency, off-host restore or production acceptance claim'])
    try:
        _mkdir(root)
        disk = shutil.disk_usage(root)
        if disk.free<5*1024**3 or disk.free/disk.total<.10:raise ValueError('disk free below v2 stop threshold')
        source = _freeze_source(root)
        pending = canonical(dict(schema='dc.pending-universe/1',scope='discovery not yet acquired'))
        _publish_bytes(root/'config.json',config.to_bytes())
        _publish_bytes(root/'pending-universe.json',pending)
        provenance = Provenance(source,config.config_sha256,digest(pending))
        utc,_,boot = observer()
        capture_id,process = 'capture-'+root.name, 'process-'+uuid.uuid4().hex
        writer = SessionWriter(root/'wal',root/'archive',venue='deribit',environment=config.environment,
            capture_id=capture_id,process_session_id=process,provenance=provenance,started_utc_ns=utc)
        emergency = Journal(root/'emergency.wal',venue='deribit',environment=config.environment,create=True)
        lane = ArchiveLane(writer,ByteQueue(),emergency_writer=emergency,observe=observer,max_connections=16,
            clock_policy=DEFAULT_POLICY,cutoff_scope=capture_id,cutoff_ledger_id='ledger-'+root.name,
            snapshot_limits=PILOT_REPLAY_LIMITS)
        raw_task = asyncio.create_task(lane.run())
        await asyncio.sleep(0)
        sampler = ChronyClockSampler(lane,executable=config.chronyc,read=clock_read,wait_until=clock_wait,
            max_samples=1000)
        await sampler.prime()
        quota = PriorityQuota('deribit',[QuotaWindow('public',1,1_000_000_000),
            QuotaWindow('get_instruments',1,1_000_000_000)],now_ns=lambda:observer()[1])
        async with session_factory() as session:
            discovery = DeribitDiscovery(lane,quota,charges={'public':1,'get_instruments':1},max_requests=1,
                max_response_bytes=config.max_response_bytes,max_instruments=config.max_instruments,
                timeout_seconds=config.timeout_seconds)
            universe = await discovery.fetch(session)
            _publish_bytes(root/'universe.json',universe)
            utc,mono,_ = observer()
            ack_task = lane.submit_deribit_metadata(universe,utc_ns=utc,monotonic_ns=mono)
            try:ack = await asyncio.shield(ack_task)
            except asyncio.CancelledError:
                await _drain(ack_task)
                raise
            if not ack.timing_valid:raise ValueError('metadata durability late')
            result['available_symbols'] = [v.instrument.native_symbol for v in ack.versions
                if v.instrument.native_product=='option' and v.attributes['native'].get('is_active') is True]
            if not discover_only:
                by_symbol = {v.instrument.native_symbol:v for v in ack.versions}
                bindings = []
                for symbol in config.symbols:
                    version = by_symbol.get(symbol)
                    if version is None or symbol not in result['available_symbols']:
                        raise ValueError('selected symbol is not an active discovered BTC/ETH option: '+symbol)
                    bindings.append(BookBinding(FeedTarget(version.instrument,'deribit_full','initial-owner'),
                        StreamKey(capture_id,process,'pending-books'),version.receipt.stream_key,version.version_id,
                        channel=f'book.{symbol}.agg2'))
                plan = SnapshotPlan(tuple(bindings),config.config_sha256)
                schedule = MinuteCutoffs(lane,first_cutoff_utc_ns=None,plan=lambda:plan,
                    healthy_streams=lambda:(),boot_id=boot,max_cutoffs=max(1,(config.duration_seconds+59)//60),
                    wait_until=schedule_wait)
                origin = SnapshotOrigin('deribit',config.environment,capture_id,process,capture_id,boot)
                publisher = SnapshotPublisher(root/'published',origin,observe=observer,max_receipt_bytes=MAX_REPORT_BYTES)
                pipeline = SnapshotPipeline(schedule,publisher)
                receiver = DeribitBookSession(schedule,quota,control_charges={'public':1},
                    limits=DeribitLimits(max_frames=config.max_frames,max_seconds=min(7200,config.duration_seconds+5)))
                capture = DeribitCapture(sampler,receiver,pipeline,
                    warmup_timeout_seconds=config.warmup_timeout_seconds)
                result['plan'] = asdict(receiver.plan)
                try:result['runtime'] = await _capture_for(capture,session,config.duration_seconds)
                finally:
                    result['books'] = strict_json(canonical([m.view() for m in receiver.models]))
                if not all(m.state=='live' for m in receiver.models):
                    raise ValueError('capture ended before all selected books acquired a native snapshot')
        result['status'] = 'discovery_complete' if discover_only else 'bounded_capture_complete'
    except asyncio.CancelledError:
        failure = asyncio.CancelledError();result['error']='cancelled'
    except Exception as exc:
        failure = exc;result['error']=type(exc).__name__+': '+str(exc)
    finally:
        if sampler is not None:sampler.stop()
        if publisher is not None:await _drain(asyncio.create_task(publisher.close()))
        if quota is not None:await _drain(asyncio.create_task(quota.close()))
        if lane is not None:
            lane.stop()
            await _drain(raw_task)
            result['segments'] = [str(p.relative_to(root)) for p in lane.segments]
            if lane.health()['writer_error'] is not None:
                result['status']='failed';result['archive_error']=lane.health()['writer_error']
        if emergency is not None:emergency.close()
        if writer is not None:writer.abort()
        os.close(owner)
    if root.exists():
        if lane is not None:
            result['snapshot_ids'] = [strict_json(r.body)['snapshot_id'] for path in lane.segments
                for r in iter_segment_records(path) if r.channel.startswith('snapshot_deliveries:')
                and strict_json(r.body).get('outcome')=='published']
        if result['status']!='failed':
            try:
                verified = _verify_report(root,result)
                if not verified['book_replay_equal']:raise ValueError('book replay differs from capture')
                _publish_bytes(root/'verification.json',canonical(verified))
            except Exception as exc:
                result['status']='failed';result['error']='verification failed: '+str(exc)
        _publish_bytes(root/'result.json',canonical(result))
    if isinstance(failure,asyncio.CancelledError):raise failure
    return dict(root=str(root),**result)
