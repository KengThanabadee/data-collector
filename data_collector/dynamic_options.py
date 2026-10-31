"""Raw Deribit option capture with durable, changing universe membership.

This lifecycle path initially retains the existing per-instrument raw feeds.
The selected depth-20/per-kind/index profile is the subsequent implementation
step. Old captures continue through their original verifier.
"""
import asyncio
from dataclasses import replace
import os
from pathlib import Path
import random
import shutil
import time
import uuid

import aiohttp

from .contracts import canonical,digest,strict_json
from .continuous import _routing_json
from .discovery import DeribitDiscovery,project_instruments,PARAMS,METHOD
from .journal import _publish_bytes
from .option_universe import OptionUniverse,deribit_options,deribit_scope,deribit_lifecycle
from .quota import PriorityQuota,QuotaWindow
from .rawrun import RawOwner,verified_records

LIFECYCLE='instrument.state.option.any'


def instrument_channels(symbol):
    return tuple(f'{feed}.{symbol}.100ms' for feed in ('book','ticker','trades'))


def first_push(value, members):
    """Return a selected instrument with a native push, not an invented trade."""
    if value.get('method')!='subscription':return None
    params=value.get('params',{});channel=params.get('channel');data=params.get('data')
    if channel==LIFECYCLE:return None
    if not isinstance(channel,str):raise ValueError('native subscription channel required')
    if channel.startswith(('book.','ticker.')):
        if not isinstance(data,dict):raise ValueError('native option push required')
        symbol=data.get('instrument_name')
        if channel not in instrument_channels(symbol):raise ValueError('native option push identity differs')
        return symbol if symbol in members else None
    if channel.startswith('trades.'):
        if not isinstance(data,list):raise ValueError('native option trades required')
        for trade in data:
            if not isinstance(trade,dict) or channel not in instrument_channels(trade.get('instrument_name')):
                raise ValueError('native option trade identity differs')
        return None  # Quiet trade channels are covered by the subscription ACK.
    raise ValueError('unrecognized option subscription channel')


async def capture(config, *, session_factory, observer, clock_read=None, clock_wait=None):
    from .app import _bind_root,_drain
    if not config.all_options or config.source_discovery_root is not None:
        raise ValueError('live rediscovery requires all_options with fresh metadata')
    lock=_bind_root(config)
    root=config.data_root/'runs'/uuid.uuid4().hex
    anchor=canonical(dict(schema='dc.dynamic-option-anchor/1',venue='deribit',environment=config.environment))
    main=RawOwner(root,config,observer,universe=anchor,batch_size=512,clock_read=clock_read,clock_wait=clock_wait)
    meta=RawOwner(root/'metadata',config,observer,universe=anchor,clock_read=clock_read,clock_wait=clock_wait)
    state=OptionUniverse('deribit',max_instruments=config.max_instruments)
    report=dict(schema='dc.dynamic-options/1',venue='deribit',environment=config.environment,status='failed',
        profile='per-instrument-100ms/1',metadata={},frames=0,bytes=0,connections=0,universe_versions=0,
        rejected_subscriptions=0,throttled_subscriptions=0,started_utc_ns=observer()[0],
        elapsed_seconds=0,ready_utc_ns=None,retired_options=0)
    stop=False;fetch_task=None;quota=None;discovery=None;latest=None
    initial_ready=None;next_audit=0;retry_metadata=0;trigger=None;refresh_due=True
    last_disk=0;generation=0;fetch_generation=0;unresolved={}

    def should_stop():
        if stop:return True
        if config.supervisor_pid is not None and os.getppid()!=config.supervisor_pid:
            report['stop_reason']='supervisor_exit';return True
        if initial_ready is not None and config.duration_seconds and time.monotonic()-initial_ready>=config.duration_seconds:
            report['stop_reason']='duration';return True
        return False

    async def control(value, channel='option_control', *, connection='controls'):
        return await main.save(canonical(value),channel,'control',connection=connection)

    async def apply_manifest(manifest, *, observed=None, reason='refresh'):
        nonlocal latest
        native_universe=strict_json(manifest)
        classified={v['instrument']['native_symbol'] for v in native_universe['selected']}
        classified.update(v['native_symbol'] for kind in ('excluded','blocked') for v in native_universe[kind])
        for name in classified:unresolved.pop(name,None)
        response=discovery.last_response if reason=='refresh' else latest[1]
        receipt=response.receipt if observed is None else observed.receipt
        # Expiry filtering uses current UTC, while the first durable observation
        # is retained separately for new-listing deadlines below.
        utc,mono=(observer()[:2] if reason=='expiry' else (receipt.receive_utc_ns,receipt.monotonic_ns))
        selected,excluded=deribit_scope(strict_json(manifest))
        encoded=state.refresh(selected,source_ids=[response.record_id],
            utc_ns=utc,monotonic_ns=mono,exclusions=excluded)
        value=strict_json(encoded)
        envelope=canonical(dict(schema='dc.option-revision-source/1',reason=reason,
            observation_record_id=None if observed is None else observed.record_id,
            response_record_id=response.record_id,response_raw_sha256=digest(response.to_bytes()),
            revision=value))
        saved=await meta.save(envelope,'option_universe','control',connection='universe')
        await control(dict(event='universe_version',record_id=saved.record_id,raw_sha256=digest(saved.to_bytes()),
            revision_sha256=digest(encoded),revision=state.revision))
        report['universe_versions']+=1;report['retired_options']+=len(value['retired'])
        latest=(manifest,response)

    async def own():
        nonlocal stop,fetch_task,quota,discovery,latest,initial_ready,next_audit,retry_metadata
        nonlocal trigger,refresh_due,last_disk,generation,fetch_generation
        await main.start();await meta.start()
        started=await control(dict(event='capture_start'))
        report['started_utc_ns']=started.receipt.receive_utc_ns
        await main.aligned(lambda:stop);await meta.aligned(lambda:stop)
        quota=PriorityQuota('deribit',(QuotaWindow('public',1,1_000_000_000),
            QuotaWindow('get_instruments',1,1_000_000_000)))
        discovery=DeribitDiscovery(meta.lane,quota,charges={'public':1,'get_instruments':1},max_requests=None,
            max_response_bytes=config.max_response_bytes,max_instruments=config.max_instruments,
            timeout_seconds=config.timeout_seconds)
        endpoint='www.deribit.com' if config.environment=='production' else 'test.deribit.com'
        backoff=1;initial_deadline=time.monotonic()+config.bootstrap_timeout_seconds
        async with session_factory() as session:
            try:
                while not stop:
                    if should_stop():stop=True;break
                    if initial_ready is None and time.monotonic()>=initial_deadline:
                        raise TimeoutError('initial option bootstrap deadline')
                    socket=None;connection='socket-'+str(report['connections'])
                    pending={};active={};pushed=set();request_id=10;next_control=0;life_ack=False
                    connected=time.monotonic();last_received=connected
                    opened=await control(dict(event='gap_start',connection=connection,reason='connection_start'))
                    state.reconnect(utc_ns=opened.receipt.receive_utc_ns,monotonic_ns=opened.receipt.monotonic_ns)
                    try:
                        async with asyncio.timeout(config.timeout_seconds):
                            socket=await session.ws_connect('wss://'+endpoint+'/ws/api/v2',max_msg_size=2*1024*1024,
                                autoping=True,compress=0,timeout=aiohttp.ClientWSTimeout(ws_close=5))
                        report['connections']+=1
                        async def request(method,channels,assignments):
                            nonlocal request_id,next_control
                            request_id+=1
                            body=canonical(dict(jsonrpc='2.0',id=request_id,method=method,params=dict(channels=channels)))
                            saved=await main.save(body,method,'control',connection=connection,
                                fields=dict(direction='outbound',universe_sha256=state.revision_sha256))
                            eligible=[name for name,key in assignments.items()
                                if name in state.pending and state.members[name].key==key]
                            if method=='public/subscribe' and eligible:
                                state.requested(eligible,source_id=saved.record_id,connection=connection,
                                    utc_ns=saved.receipt.receive_utc_ns,monotonic_ns=saved.receipt.monotonic_ns)
                            pending[request_id]=dict(method=method,channels=tuple(channels),assignments=assignments,
                                sent=time.monotonic(),record=saved)
                            await asyncio.wait_for(socket.send_str(body.decode()),config.timeout_seconds)
                            next_control=time.monotonic()+1
                        await request('public/subscribe',[LIFECYCLE],{})
                        heartbeat=canonical(dict(jsonrpc='2.0',id=2,method='public/set_heartbeat',params=dict(interval=10)))
                        await main.save(heartbeat,'public/set_heartbeat','control',connection=connection)
                        await socket.send_str(heartbeat.decode())
                        gap_closed=False
                        while not stop:
                            now=time.monotonic();main.check();meta.check()
                            if now-last_disk>=1:
                                disk=shutil.disk_usage(root)
                                if disk.free<5*1024**3 or disk.free/disk.total<.10:raise ValueError('disk free below stop threshold')
                                last_disk=now
                            if should_stop():stop=True;break
                            if now-connected>config.bootstrap_timeout_seconds and not gap_closed:
                                raise TimeoutError('changing-universe bootstrap timeout')
                            if any(now-p['sent']>config.timeout_seconds for p in pending.values()):
                                raise TimeoutError('subscription ACK timeout')
                            if fetch_task is not None and fetch_task.done():
                                current=fetch_task;fetch_task=None
                                try:
                                    manifest=current.result()
                                except (ValueError,OSError,aiohttp.ClientError,TimeoutError) as exc:
                                    meta.check()
                                    await control(dict(event='discovery_unhealthy',reason=type(exc).__name__))
                                    retry_metadata=now+min(60,backoff);refresh_due=True
                                else:
                                    await apply_manifest(manifest,observed=trigger)
                                    refresh_due=generation!=fetch_generation or bool(unresolved)
                                    if not refresh_due:trigger=None
                                    next_audit=now+300
                            if life_ack and fetch_task is None and (refresh_due or now>=next_audit) and now>=retry_metadata:
                                fetch_generation=generation
                                fetch_task=asyncio.create_task(discovery.fetch(session))
                            if latest and any(row.expiry_ms*1_000_000<=observer()[0] for row in state.members.values()):
                                await apply_manifest(latest[0],reason='expiry')
                            if life_ack and not pending and now>=next_control:
                                retiring={name:key for name,key in active.items()
                                    if name not in state.members or state.members[name].key!=key}
                                adding={name:row.key for name,row in state.members.items() if active.get(name)!=row.key}
                                assignments=dict(list((retiring or adding).items())[:max(1,config.subscription_batch_size//3)])
                                if assignments:
                                    channels=[c for name in assignments for c in instrument_channels(name)]
                                    await request('public/unsubscribe' if retiring else 'public/subscribe',channels,assignments)
                            current_ready=(state.revision>0 and not state.pending and
                                set(state.members)<=pushed and all(active.get(n)==row.key for n,row in state.members.items()))
                            if current_ready and not gap_closed:
                                await control(dict(event='gap_end',connection=connection,revision_sha256=state.revision_sha256))
                                gap_closed=True;backoff=1
                                if initial_ready is None:
                                    initial_ready=now;report['ready_utc_ns']=observer()[0]
                                    _publish_bytes(root/'ready.json',canonical(dict(utc_ns=report['ready_utc_ns'],options=len(state.members))))
                            try:message=await asyncio.wait_for(socket.receive(),.25)
                            except TimeoutError:
                                if now-last_received>30:raise TimeoutError('socket idle timeout')
                                continue
                            deliveries=[];size=0;closed=False;end=time.monotonic()+.02
                            receive_error=None
                            try:
                                while True:
                                    if message.type not in (aiohttp.WSMsgType.TEXT,aiohttp.WSMsgType.BINARY):
                                        body=canonical(dict(type=int(message.type),code=message.data if type(message.data) is int else None,
                                            detail=str(message.data)[:1024]))
                                        item,future=main.admit(body,'socket_event','control',connection=connection)
                                        deliveries.append((item,future));closed=True;break
                                    body=message.data.encode() if message.type==aiohttp.WSMsgType.TEXT else message.data
                                    item,future=main.admit(body,'multiplex','ws_message',connection=connection)
                                    deliveries.append((item,future));size+=len(body);last_received=time.monotonic()
                                    if len(deliveries)>=512 or size>=1024*1024 or time.monotonic()>=end:break
                                    try:message=await asyncio.wait_for(socket.receive(),end-time.monotonic())
                                    except TimeoutError:break
                            except Exception as exc:receive_error=exc
                            acknowledgements=await asyncio.gather(*(future for _,future in deliveries))
                            confirmed=[main.confirm(record,ack) for (record,_),ack in zip(deliveries,acknowledgements)]
                            report['frames']+=sum(record.kind=='ws_message' for record in confirmed)
                            report['bytes']+=sum(len(record.body) for record in confirmed if record.kind=='ws_message')
                            for record in confirmed:
                                if record.kind!='ws_message':continue
                                value=_routing_json(record.body)
                                if 'error' in value:
                                    if value.get('id') in pending:
                                        report['rejected_subscriptions']+=1
                                        if value['error'].get('code')==10028:report['throttled_subscriptions']+=1
                                    raise ValueError('Deribit control rejected; raw preserved')
                                response=pending.pop(value.get('id'),None)
                                if response is not None:
                                    if value.get('result')!=list(response['channels']):
                                        # Venue ordering is not significant; duplicate or missing channels are.
                                        got=value.get('result')
                                        if not isinstance(got,list) or len(got)!=len(response['channels']) or set(got)!=set(response['channels']):
                                            report['rejected_subscriptions']+=1;raise ValueError('subscription ACK differs')
                                    if response['channels']==(LIFECYCLE,):life_ack=True
                                    elif response['method']=='public/unsubscribe':
                                        for name,key in response['assignments'].items():
                                            if active.get(name)==key:active.pop(name);pushed.discard(name)
                                    else:
                                        active.update(response['assignments'])
                                        eligible=[name for name,key in response['assignments'].items()
                                            if name in state.pending and state.members[name].key==key]
                                        if eligible:
                                            evidence=state.acknowledge(eligible,source_id=record.record_id,connection=connection,
                                                utc_ns=record.receipt.receive_utc_ns,monotonic_ns=record.receipt.monotonic_ns)
                                            await main.save(evidence,'option_subscription','control',connection='controls')
                                params=value.get('params',{})
                                if value.get('method')=='subscription' and params.get('channel')==LIFECYCLE:
                                    event=deribit_lifecycle(params.get('data'))
                                    if not event['terminal'] and event['symbol'] not in state.members:
                                        unresolved.setdefault(event['symbol'],record.receipt)
                                        if len(unresolved)>config.max_instruments:raise ValueError('unresolved option listing bound')
                                    if trigger is None:trigger=record
                                    generation+=1;refresh_due=True
                                else:
                                    symbol=first_push(value,state.members)
                                    if symbol is not None:pushed.add(symbol)
                                if value.get('method')=='heartbeat' and params.get('type')=='test_request':
                                    body=canonical(dict(jsonrpc='2.0',id=3,method='public/test',params={}))
                                    await main.save(body,'public/test','control',connection=connection)
                                    await socket.send_str(body.decode())
                            if (config.max_frames and report['frames']>=config.max_frames) or (config.max_bytes and report['bytes']>=config.max_bytes):
                                stop=True
                            if receive_error is not None:raise receive_error
                            if closed:raise ConnectionError('socket closed')
                        if unresolved:raise ValueError('capture ended with unresolved option listing observations')
                        if state.pending or not set(state.members)<=pushed:
                            raise ValueError('capture ended before changing-universe ACK and first push')
                    except (aiohttp.ClientError,OSError,TimeoutError) as exc:
                        main.check();meta.check()
                        await control(dict(event='gap_start',connection=connection,reason=type(exc).__name__))
                        refresh_due=True
                    finally:
                        if socket is not None:await asyncio.wait_for(socket.close(),5)
                        await control(dict(event='gap_start',connection=connection,reason='connection_end'))
                    if not stop:
                        until=time.monotonic()+random.uniform(.8,1)*backoff
                        while not stop and time.monotonic()<until:
                            if should_stop():stop=True;break
                            await asyncio.sleep(.1)
                        backoff=min(60,backoff*2)
            finally:
                stop=True
                try:
                    if latest and any(row.expiry_ms*1_000_000<=observer()[0] for row in state.members.values()):
                        await apply_manifest(latest[0],reason='expiry')
                    ended=await control(dict(event='capture_end'))
                    report['ended_utc_ns']=ended.receipt.receive_utc_ns
                finally:
                    try:
                        if fetch_task is not None:
                            await _drain(asyncio.gather(fetch_task,return_exceptions=True))
                    finally:
                        if quota is not None:await quota.close()


    task=asyncio.create_task(own())
    try:
        try:await asyncio.shield(task)
        except asyncio.CancelledError:
            stop=True;await _drain(task)
        if initial_ready is None:raise ValueError('stopped before option initialization')
        if unresolved:raise ValueError('capture ended with unresolved option listing observations')
        report['status']='raw_capture_complete'
    except Exception as exc:report['error']=type(exc).__name__+': '+str(exc)
    finally:
        stop=True
        try:
            for owner in (meta,main):
                try:await owner.close()
                except Exception as exc:
                    report['status']='failed';report['error']=type(exc).__name__+': '+str(exc)
            if main.provenance is not None:report.update(main.descriptor())
            if meta.provenance is not None:report['metadata']=meta.descriptor()
            report['elapsed_seconds']=0 if initial_ready is None else time.monotonic()-initial_ready
            report.setdefault('ended_utc_ns',observer()[0])
            if root.exists():_publish_bytes(root/'result.json',canonical(report))
        finally:os.close(lock)
    return dict(root=str(root),**report)


class MetadataReplay:
    """Stream metadata dependencies in order, keeping one HTTP response only."""
    def __init__(self, root, report):
        self.records=iter(verified_records(root,report));self.request=None;self.response=None;self.accepted=None
        self.count=0

    def next_revision(self):
        for record in self.records:
            self.count+=1;fields=strict_json(record.native_fields_json)
            if fields.get('schema')=='dc.deribit-discovery/1':
                if fields.get('direction')=='outbound':
                    if self.request is not None:raise ValueError('overlapping metadata requests')
                    expected=('https://www.deribit.com/api/v2/' if record.environment=='production'
                        else 'https://test.deribit.com/api/v2/')+METHOD
                    if strict_json(record.body)!=dict(method='GET',endpoint=expected,params=PARAMS):
                        raise ValueError('metadata request differs from discovery profile')
                    self.request=record
                elif fields.get('direction')=='inbound':
                    if (self.request is None or fields.get('request_record_id')!=self.request.record_id or
                            fields.get('request_raw_sha256')!=digest(self.request.to_bytes())):
                        raise ValueError('metadata response request proof differs')
                    self.response=record;self.request=None
            if record.channel=='option_universe':
                value=strict_json(record.body)
                candidates=[r for r in (self.response,self.accepted) if r is not None and r.record_id==value.get('response_record_id')]
                source=candidates[0] if candidates else None
                if (value.get('schema')!='dc.option-revision-source/1' or source is None or
                        value['response_raw_sha256']!=digest(source.to_bytes())):
                    raise ValueError('universe version lacks raw metadata proof')
                manifest=project_instruments(source);self.accepted=source
                return record,value,manifest,source
        return None


def verify(root, report, *, export=None):
    """Verify raw integrity, every metadata version, ACKs and native first pushes.

    This proves raw capture coverage with explicit connection gaps; it does not
    replay a derived book or certify a complete network history.
    """
    if export is not None:raise ValueError('export of multiple raw owners is not enabled')
    if report.get('profile')!='per-instrument-100ms/1':raise ValueError('unsupported dynamic option profile')
    root=Path(root).resolve();config=strict_json((root/'config.json').read_bytes())
    anchor=canonical(dict(schema='dc.dynamic-option-anchor/1',venue='deribit',environment=config['environment']))
    for descriptor in (report,report['metadata']):
        if descriptor['provenance']['universe_sha256']!=digest(anchor):raise ValueError('option anchor differs')
    if not config.get('rediscover_options') or not config.get('all_options'):
        raise ValueError('changing-universe configuration required')
    state=OptionUniverse('deribit',max_instruments=config['max_instruments'])
    metadata=MetadataReplay(root/'metadata',report['metadata'])
    complete=report['status']=='raw_capture_complete';semantic_error=False
    pending={};active={};pushed=set();current_connection=None;life_ack=False
    observations={};evidence={};ever={};unresolved={};records=frames=payload=versions=retired=rejected=throttled=0
    connection_count=0;bounds={}

    def coverage(names):
        return all(ever.get(state.members[name].key,{}).get('ack') and
            ever.get(state.members[name].key,{}).get('push') for name in names)

    def check_record(record):
        nonlocal pending,active,pushed,current_connection,life_ack,versions,retired,rejected,throttled,connection_count
        fields=strict_json(record.native_fields_json)
        if record.channel=='option_control':
            value=strict_json(record.body)
            if value.get('event') in {'capture_start','capture_end'}:
                event=value['event']
                if event in bounds:raise ValueError('duplicate capture boundary')
                bounds[event]=record.receipt.receive_utc_ns
            elif value.get('event')=='gap_start' and value.get('reason')=='connection_start':
                current_connection=value['connection'];pending={};active={};pushed=set();life_ack=False
                state.reconnect(utc_ns=record.receipt.receive_utc_ns,monotonic_ns=record.receipt.monotonic_ns)
                connection_count+=1
            elif value.get('event')=='universe_version':
                acquired=metadata.next_revision()
                if acquired is None:raise ValueError('universe reference has no metadata record')
                source,wrapped,manifest,response=acquired;revision=wrapped['revision']
                if source.record_id!=value['record_id'] or digest(source.to_bytes())!=value['raw_sha256']:
                    raise ValueError('universe source hash differs')
                observed=wrapped['observation_record_id']
                if observed is None and wrapped['reason']=='refresh':
                    if (revision['observed_at_utc_ns']!=response.receipt.receive_utc_ns or
                            revision['observed_at_monotonic_ns']!=response.receipt.monotonic_ns):
                        raise ValueError('metadata observation time differs')
                elif observed is not None:
                    receipt=observations.get(observed)
                    if receipt is None or (revision['observed_at_utc_ns'],revision['observed_at_monotonic_ns'])!=(
                            receipt.receive_utc_ns,receipt.monotonic_ns):
                        raise ValueError('lifecycle observation time differs')
                    observations.clear();observations[observed]=receipt
                elif wrapped['reason']=='expiry':
                    if revision['observed_at_monotonic_ns']>source.receipt.monotonic_ns:
                        raise ValueError('expiry retirement is later than its raw control')
                else:raise ValueError('unrecognized universe observation proof')
                native_universe=strict_json(manifest)
                classified={v['instrument']['native_symbol'] for v in native_universe['selected']}
                classified.update(v['native_symbol'] for kind in ('excluded','blocked') for v in native_universe[kind])
                for name in classified:unresolved.pop(name,None)
                previous=dict(state.members)
                selected,excluded=deribit_scope(strict_json(manifest))
                encoded=state.refresh(selected,source_ids=[response.record_id],
                    utc_ns=revision['observed_at_utc_ns'],monotonic_ns=revision['observed_at_monotonic_ns'],exclusions=excluded)
                if canonical(revision)!=encoded or digest(encoded)!=value['revision_sha256'] or value['revision']!=state.revision:
                    raise ValueError('universe projection or revision chain differs')
                for name in revision['retired']:
                    key=previous[name].key
                    if complete and not (ever.get(key,{}).get('ack') and ever.get(key,{}).get('push')):
                        raise ValueError('retired option lacked ACK plus first push before retirement')
                    ever.pop(key,None);pushed.discard(name)
                versions+=1;retired+=len(revision['retired'])
            elif value.get('event')=='gap_end':
                if not life_ack or state.pending or not set(state.members)<=pushed:
                    raise ValueError('gap ended before current option initialization')
            return
        if record.channel=='option_subscription':
            value=strict_json(record.body);expected=evidence.pop(value.get('ack_record_id'),None)
            if expected is None or canonical(value)!=expected:raise ValueError('option subscription timing differs from raw request/ACK')
            return
        if record.kind=='control' and record.channel in {'public/subscribe','public/unsubscribe'}:
            if record.receipt.connection_id!=current_connection:
                current_connection=record.receipt.connection_id;pending={};active={};pushed=set();life_ack=False
                state.reconnect(utc_ns=record.receipt.receive_utc_ns,monotonic_ns=record.receipt.monotonic_ns)
                connection_count+=1
            value=strict_json(record.body);channels=value.get('params',{}).get('channels')
            if value.get('method')!=record.channel or not isinstance(channels,list) or not channels:
                raise ValueError('explicit option subscription request required')
            if len(set(channels))!=len(channels):raise ValueError('duplicate subscription channel')
            assignments={}
            if channels!=[LIFECYCLE]:
                names=list(dict.fromkeys(c.split('.')[1] for c in channels))
                if set(channels)!={c for name in names for c in instrument_channels(name)}:
                    raise ValueError('option request omits a required feed')
                for name in names:
                    if record.channel=='public/subscribe':
                        if name not in state.members:raise ValueError('subscription outside current universe')
                        assignments[name]=state.members[name].key
                    else:
                        if name not in active:raise ValueError('unsubscribe lacks active incarnation')
                        assignments[name]=active[name]
                if fields.get('universe_sha256')!=state.revision_sha256:
                    raise ValueError('subscription references another universe version')
                if record.channel=='public/subscribe':
                    eligible=[name for name in assignments if name in state.pending]
                    if eligible:state.requested(eligible,source_id=record.record_id,connection=current_connection,
                        utc_ns=record.receipt.receive_utc_ns,monotonic_ns=record.receipt.monotonic_ns)
            if value.get('id') in pending:raise ValueError('duplicate pending request id')
            pending[value['id']]=dict(method=record.channel,channels=channels,assignments=assignments)
            return
        if record.kind!='ws_message':return
        value=_routing_json(record.body)
        if 'error' in value:
            if value.get('id') in pending:
                rejected+=1
                if value['error'].get('code')==10028:throttled+=1
            raise ValueError('rejected subscription/control in raw capture')
        request=pending.pop(value.get('id'),None)
        if request is not None:
            result=value.get('result')
            if not isinstance(result,list) or len(result)!=len(request['channels']) or set(result)!=set(request['channels']):
                rejected+=1;raise ValueError('raw subscription ACK differs')
            if request['channels']==[LIFECYCLE]:life_ack=True
            elif request['method']=='public/unsubscribe':
                for name,key in request['assignments'].items():
                    if active.get(name)==key:active.pop(name);pushed.discard(name)
            else:
                active.update(request['assignments'])
                eligible=[name for name,key in request['assignments'].items()
                    if name in state.pending and state.members[name].key==key]
                if eligible:
                    encoded=state.acknowledge(eligible,source_id=record.record_id,connection=record.receipt.connection_id,
                        utc_ns=record.receipt.receive_utc_ns,monotonic_ns=record.receipt.monotonic_ns)
                    evidence[record.record_id]=encoded
                    for name in eligible:
                        row=state.members[name];prior=ever.setdefault(row.key,{})
                        if not prior.get('ack') and row.created_ms is not None:
                            created=row.created_ms*1_000_000
                            if report['started_utc_ns']<=created<=report['ended_utc_ns'] and record.receipt.receive_utc_ns-created>120_000_000_000:
                                raise ValueError('Deribit created option subscription exceeded 120 seconds')
                        prior['ack']=True
        params=value.get('params',{})
        if value.get('method')=='subscription' and params.get('channel')==LIFECYCLE:
            event=deribit_lifecycle(params.get('data'));observations[record.record_id]=record.receipt
            if not event['terminal'] and event['symbol'] not in state.members:
                unresolved.setdefault(event['symbol'],record.record_id)
            if len(observations)>config['max_instruments']:
                raise ValueError('unconsumed lifecycle observation bound')
        else:
            name=first_push(value,state.members)
            if name is not None:
                pushed.add(name);ever.setdefault(state.members[name].key,{})['push']=True

    for record in verified_records(root,report):
        records+=1
        if record.kind=='ws_message':frames+=1;payload+=len(record.body)
        if semantic_error:continue
        try:check_record(record)
        except (ValueError,TypeError,KeyError,IndexError):
            if complete:raise
            semantic_error=True
    # Finish the independent owner's integrity scan, including an HTTP attempt
    # that completed during shutdown without publishing a membership change.
    extra=metadata.next_revision()
    if complete and extra is not None:raise ValueError('unreferenced universe version')
    while extra is not None:extra=metadata.next_revision()
    if (frames,payload)!=(report['frames'],report['bytes']):raise ValueError('raw option counters differ')
    if complete:
        if bounds!=dict(capture_start=report['started_utc_ns'],capture_end=report['ended_utc_ns']):
            raise ValueError('capture time bounds differ from raw controls')
        if versions!=report['universe_versions'] or retired!=report['retired_options']:
            raise ValueError('universe version or retirement counters differ')
        if rejected or report['rejected_subscriptions']:raise ValueError('subscription rejection')
        if evidence:raise ValueError('ACK timing evidence missing')
        if unresolved:raise ValueError('unresolved option listing observation')
        if not state.revision or not coverage(state.members):raise ValueError('option lacks ACK plus first push')
        if any(row.expiry_ms*1_000_000<=report['ended_utc_ns'] for row in state.members.values()):
            raise ValueError('expired option not retired')
    return dict(archive_verified=True,records=records+metadata.count,ws_messages=frames,
        universe_versions_verified=versions,changing_universe_coverage_verified=complete,
        capture_status=report['status'],verification_scope='raw integrity, append-only metadata and every universe version with ACK/first-push/retirement evidence; explicit gaps retained')
