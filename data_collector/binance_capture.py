"""Selected WebSocket events and the historical Binance top-20 profile."""
import asyncio
import os
from pathlib import Path
import random
import shutil
import time
import uuid

import aiohttp

from .binance import ENDPOINTS,fetch,_native_values
from .binance_budget import Budget
from .bybit_options import IDLE_SECONDS, PING_SECONDS, TRADES, route as bybit_option_route
from .contracts import canonical,digest,strict_json,integer
from .continuous import _routing_json
from .journal import _publish_bytes, sync_directory
from .rawrun import RawOwner,verified_records

PROFILE='binance-um-top20/1'
EVENT_PROFILE='minute-grid-events/1'
LIQUIDATIONS='!forceOrder@arr'
METADATA_INTERVAL_SECONDS=60


def project(response,symbols,environment,max_instruments):
    fields=strict_json(response.native_fields_json)
    url,params=ENDPOINTS['um']
    if (response.venue!='binance' or response.environment!=environment or response.kind!='rest_response'
            or response.channel!='um:exchange_info' or fields.get('endpoint')!=url or fields.get('params')!=params
            or fields.get('status')!=200 or fields.get('complete') is not True or fields.get('error') is not None):
        raise ValueError('complete successful UM exchangeInfo required')
    value=strict_json(response.body)
    if not isinstance(value,dict) or 'code' in value or not isinstance(value.get('symbols'),list):
        raise ValueError('native UM symbols required')
    if len(value['symbols'])>max_instruments:raise ValueError('instrument count bound')
    found={};seen=set()
    for index,row in enumerate(value['symbols']):
        if not isinstance(row,dict) or not isinstance(row.get('symbol'),str):raise ValueError('native symbol required')
        symbol=row['symbol']
        if symbol in seen:raise ValueError('duplicate native UM symbol')
        seen.add(symbol)
        if symbol not in symbols:continue
        if row.get('status')!='TRADING':raise ValueError('configured symbol is not trading: '+symbol)
        if row.get('contractType') not in {'PERPETUAL','CURRENT_QUARTER','NEXT_QUARTER','CURRENT_MONTH','NEXT_MONTH'}:
            raise ValueError('unsupported native contract type')
        integer(row.get('onboardDate'),'native onboard time',1)
        found[symbol]=dict(symbol=symbol,incarnation='onboard:'+str(row['onboardDate']),
            product='perpetual' if row['contractType']=='PERPETUAL' else 'future',native=_native_values(row),row_index=index)
    if set(found)!=set(symbols):raise ValueError('configured symbols missing from exchangeInfo')
    return dict(schema='dc.binance-selected-universe/1',environment=environment,
        source=dict(record_id=response.record_id,raw_sha256=digest(response.to_bytes())),
        selected=[found[s] for s in symbols])


def plans(symbols):
    return [dict(id=route,url='wss://fstream.binance.com/'+route+'/stream',topics=topics)
        for route,topics in (
            ('public',[s.lower()+'@'+feed for s in symbols for feed in ('bookTicker','depth20@100ms')]),
            ('market',[s.lower()+'@aggTrade' for s in symbols]+[LIQUIDATIONS]))]


def capture_plans(config):
    if not config.get('event_streams', False):
        return plans(config['binance_symbols'])
    if config['venue']=='binance':
        return [dict(id='market', url='wss://fstream.binance.com/market/stream', topics=[LIQUIDATIONS])]
    return [dict(id=route, url=f"wss://{config['bybit_ws_host']}/v5/public/{route}", topics=topics)
        for route,topics in (
            ('linear',['allLiquidation.'+symbol for symbol in config['bybit_liquidation_symbols']]),
            ('option',list(TRADES)))]


def subscription(plan):
    if plan['id'] in ('linear','option'):
        return dict(op='subscribe',args=plan['topics'])
    return dict(method='SUBSCRIBE',params=plan['topics'],id=1)


def classify_message(value, plan):
    """Classify native pushes and connection controls without changing the payload."""
    route = plan['id']
    if route=='option':
        kind,payload=bybit_option_route(value)
        if kind=='ack':
            ok,topics=payload
            return ('ack' if ok and sorted(topics)==sorted(plan['topics']) else 'rejected'),None
        if kind=='trade' and payload in plan['topics']:
            return 'event',payload
        if kind=='pong':return kind,None
        raise ValueError('unplanned Bybit option topic')
    if route=='linear':
        if value.get('op')=='pong' or (value.get('op')=='ping'
                and value.get('ret_msg')=='pong' and value.get('success') is True):
            return 'pong',None
        if value.get('op')=='subscribe':
            return ('ack' if value.get('success') is True else 'rejected'),None
        topic=value.get('topic');rows=value.get('data')
        if (topic not in plan['topics'] or not isinstance(rows,list) or not rows
                or any(not isinstance(row,dict) or row.get('s')!=topic.split('.',1)[1] for row in rows)):
            raise ValueError('unplanned Bybit linear liquidation or native symbol differs')
        return 'event',topic
    if 'code' in value:return 'rejected',None
    if value.get('id')==1 and 'result' in value:
        if value['result'] is not None:raise ValueError('unexpected subscription ACK')
        return 'ack',None
    data=value.get('data',value)
    if isinstance(data,dict) and data.get('e')=='serverShutdown':return 'shutdown',None
    return 'event',validate_event(value,plan)


def required(plan):
    # Trades/liquidations may be quiet; their ACK is required, a made-up push is not.
    return set(plan['topics']) if plan['id']=='public' else set()


def validate_event(value,plan):
    topic=value.get('stream')
    if topic not in plan['topics']:raise ValueError('unplanned Binance topic')
    data=value.get('data')
    if topic==LIQUIDATIONS:
        rows=data if isinstance(data,list) else [data]
        if not rows:raise ValueError('empty liquidation push')
        for row in rows:
            if (not isinstance(row,dict) or row.get('e')!='forceOrder' or not isinstance(row.get('o'),dict)
                    or not isinstance(row['o'].get('s'),str)):
                raise ValueError('native liquidation identity required')
        # The native all-market feed can include CM after migration. Keep every
        # row unchanged, including st/ps; never label all rows as UM instruments.
        return topic
    if not isinstance(data,dict) or data.get('s')!=topic.split('@',1)[0].upper():
        raise ValueError('native Binance symbol differs from topic')
    if data.get('st',1)!=1:raise ValueError('configured UM topic delivered a non-UM instrument')
    if '@depth20' in topic:
        if data.get('e')!='depthUpdate':raise ValueError('native depth event required')
        for side in ('b','a'):
            levels=data.get(side)
            if not isinstance(levels,list) or len(levels)>20 or any(not isinstance(v,list) or len(v)!=2 for v in levels):
                raise ValueError('native top-20 depth required')
    elif topic.endswith('@bookTicker'):
        if any(k not in data for k in ('b','B','a','A','u')):raise ValueError('native top-of-book with size required')
    elif data.get('e')!='aggTrade':raise ValueError('native aggregate trade required')
    return topic


async def capture(config,*,session_factory,observer,clock_read=None,clock_wait=None):
    from .app import _bind_root,_drain
    lock=_bind_root(config);root=config.data_root/'runs'/uuid.uuid4().hex
    owner=RawOwner(root,config,observer,batch_size=512,clock_read=clock_read,clock_wait=clock_wait)
    routes=capture_plans(strict_json(config.to_bytes()))
    stopped=asyncio.Event();tasks=[];started=None;failure=None
    initialized=set();topics_seen=set();current=None
    report=dict(schema='dc.event-streams/1' if config.event_streams else 'dc.binance-selected/1',
        profile=EVENT_PROFILE if config.event_streams else PROFILE,venue=config.venue,environment=config.environment,
        status='failed',plans=routes,frames=0,bytes=0,connections=0,initialized_connections=0,
        rejected_subscriptions=0,throttled_subscriptions=0,metadata_versions=0,socket_events=0)

    def stopping():
        if config.supervisor_pid is not None and os.getppid()!=config.supervisor_pid:
            stopped.set();report['stop_reason']='supervisor_exit'
        return stopped.is_set()

    async def wait(seconds):
        end=time.monotonic()+seconds
        while not stopping() and time.monotonic()<end:await asyncio.sleep(min(.2,max(0,end-time.monotonic())))

    async def control(event,*,connection='controls',**fields):
        return await owner.save(canonical(dict(event=event,**fields)),'capture_control','control',connection=connection)

    async def publish_ready():
        nonlocal started
        if started is None and initialized=={p['id'] for p in routes}:
            started=time.monotonic();await control('capture_ready')
            _publish_bytes(root/'ready.json',canonical(dict(utc_ns=observer()[0],channels=sum(len(p['topics']) for p in routes))))

    async def metadata(session):
        nonlocal current
        if not await budget.acquire('um','exchange_info',stopping):return
        response=await fetch(owner,session,'um')
        fields=strict_json(response.native_fields_json)
        await budget.observe('um',fields['headers'],stopping)
        # Full failed replies/prefixes stay in the archive. Common transport
        # failures retry with gaps, rate/API rejections stop without hammering.
        if fields['status'] in (418,429):raise ValueError('Binance REST rate rejection')
        if not fields['complete'] or fields['error'] is not None:
            await control('metadata_gap',error=fields['error']);return False
        projected=project(response,config.binance_symbols,config.environment,config.max_instruments)
        if current is not None and [(r['symbol'],r['incarnation']) for r in projected['selected']]!=[(r['symbol'],r['incarnation']) for r in current['selected']]:
            raise ValueError('configured listing incarnation changed; restart with new plan')
        await control('metadata_version',universe=projected)
        if current is None:_publish_bytes(root/'universe.json',canonical(projected))
        current=projected;report['metadata_versions']+=1
        return True

    async def route(session,plan):
        delay=1
        while not stopping():
            if budget is not None and not await budget.connection(stopping):return
            connection=plan['id']+'-'+uuid.uuid4().hex
            await control('gap_start',connection=connection,route=plan['id'],reason='connection_start')
            ws=None
            try:
                async with asyncio.timeout(config.timeout_seconds):
                    ws=await session.ws_connect(plan['url'],compress=15,autoping=True,
                        heartbeat=30 if config.venue=='binance' else None,
                        max_msg_size=2*1024*1024,timeout=aiohttp.ClientWSTimeout(ws_close=5))
                report['connections']+=1
                async def send(value, channel):
                    payload=canonical(value)
                    await owner.save(payload,channel,'control',connection=connection,fields=dict(route=plan['id']))
                    await asyncio.wait_for(ws.send_str(payload.decode()),config.timeout_seconds)

                await send(subscription(plan),'SUBSCRIBE')
                acked=False;seen=set();connected=last_received=time.monotonic();complete=False
                next_ping=connected+PING_SECONDS
                while not stopping():
                    owner.check()
                    if not acked and time.monotonic()-connected>config.timeout_seconds:raise TimeoutError('subscription ACK timeout')
                    if not complete and time.monotonic()-connected>config.bootstrap_timeout_seconds:raise TimeoutError(f'{plan["id"]} route bootstrap timeout')
                    if config.venue=='bybit':
                        now=time.monotonic()
                        if now-last_received>IDLE_SECONDS:raise TimeoutError('Bybit socket idle timeout')
                        if now>=next_ping:
                            await send(dict(op='ping'),'ping');next_ping=now+PING_SECONDS
                    try:message=await asyncio.wait_for(ws.receive(),.25)
                    except TimeoutError:continue
                    deliveries=[];native_bytes=0;deadline=time.monotonic()+.05;closed=None;receive_error=None
                    while True:
                        if message.type not in (aiohttp.WSMsgType.TEXT,aiohttp.WSMsgType.BINARY):
                            closed=dict(type=int(message.type),data=str(message.data)[:1024],reason=str(message.extra)[:1024]);break
                        body=message.data.encode() if isinstance(message.data,str) else message.data
                        record,future=owner.admit(body,'multiplex','ws_message',connection=connection,fields=dict(route=plan['id']))
                        deliveries.append((body,record,future));native_bytes+=len(body)
                        last_received=time.monotonic()
                        if len(deliveries)>=512 or native_bytes>=1024*1024 or time.monotonic()>=deadline:break
                        try:message=await asyncio.wait_for(ws.receive(),max(.0001,deadline-time.monotonic()))
                        except TimeoutError:break
                        except (aiohttp.ClientError,OSError) as exc:receive_error=exc;break
                    acks=await asyncio.gather(*(future for _,_,future in deliveries))
                    for (_,record,_),ack in zip(deliveries,acks):owner.check_ack(record,ack)
                    report['frames']+=len(deliveries);report['bytes']+=native_bytes
                    for body,_,_ in deliveries:
                        value=_routing_json(body)
                        kind,topic=classify_message(value,plan)
                        if kind=='rejected':
                            report['rejected_subscriptions']+=1
                            report['throttled_subscriptions']+=int(value.get('code') in (-1003,-1008))
                            receive_error=ValueError(config.venue+' subscription rejected')
                        elif kind=='ack':
                            if acked:raise ValueError('unexpected subscription ACK')
                            acked=True
                        elif kind=='shutdown' and receive_error is None:receive_error=ConnectionError('venue serverShutdown')
                        elif kind=='event':
                            topics_seen.add(topic)
                            if acked:seen.add(topic)
                    if closed is not None:
                        await control('socket_event',connection=connection,route=plan['id'],**closed)
                        report['socket_events']+=1
                        if receive_error is None:receive_error=ConnectionError('socket closed')
                    if receive_error is None and acked and required(plan)<=seen and not complete:
                        await control('gap_end',connection=connection,route=plan['id'])
                        complete=True;delay=1;initialized.add(plan['id']);report['initialized_connections']+=1
                        await publish_ready()
                    if config.max_frames and report['frames']>=config.max_frames:stopped.set();report['stop_reason']='max_frames'
                    if config.max_bytes and report['bytes']>=config.max_bytes:stopped.set();report['stop_reason']='max_bytes'
                    if receive_error is not None:raise receive_error
            except (aiohttp.ClientError,OSError,TimeoutError) as exc:
                owner.check();await control('transport_error',connection=connection,route=plan['id'],reason=type(exc).__name__)
            finally:
                initialized.discard(plan['id'])
                try:await control('gap_start',connection=connection,route=plan['id'],reason='connection_end')
                finally:
                    if ws is not None:
                        try:await asyncio.wait_for(ws.close(),5)
                        except TimeoutError:ws._response.close()  # pinned aiohttp connection cleanup
                        except (aiohttp.ClientError,OSError):pass
            if not stopping():await wait(random.uniform(.8,1)*delay);delay=min(60,delay*2)

    async def audits(session):
        while not stopping():
            await wait(METADATA_INTERVAL_SECONDS)
            if stopping():return
            delay=1
            while not stopping() and not await metadata(session):
                await wait(delay);delay=min(60,delay*2)

    async def own():
        nonlocal budget
        await owner.start();await owner.aligned(stopping)
        if stopping():return
        if config.venue=='binance':budget=Budget(root/'budget.json',observer()[2])
        async with session_factory() as session:
            deadline=time.monotonic()+config.bootstrap_timeout_seconds
            delay=1
            while not config.event_streams and not stopping() and not await metadata(session):
                if time.monotonic()>=deadline:raise TimeoutError('initial metadata deadline')
                await wait(delay);delay=min(60,delay*2)
            if stopping():return
            tasks.extend(asyncio.create_task(route(session,p)) for p in routes)
            if not config.event_streams:tasks.append(asyncio.create_task(audits(session)))
            while not stopping():
                owner.check()
                for task in tasks:
                    if task.done():task.result();raise ValueError('raw acquisition task ended')
                if started is None and time.monotonic()>=deadline:raise TimeoutError(f'{config.venue} initialization deadline')
                if started is not None and config.duration_seconds and time.monotonic()-started>=config.duration_seconds:
                    report['stop_reason']='duration';stopped.set();break
                disk=shutil.disk_usage(root)
                if disk.free<5*1024**3 or disk.free/disk.total<.10:raise ValueError('disk free below stop threshold')
                await asyncio.sleep(.2)

    budget=None;task=asyncio.create_task(own())
    try:
        try:await asyncio.shield(task)
        except asyncio.CancelledError:stopped.set();report['stop_reason']='operator';await _drain(task)
    except Exception as exc:failure=exc
    finally:
        stopped.set()
        if tasks:
            results=await _drain(asyncio.gather(*tasks,return_exceptions=True))
            failure=failure or next((r for r in results if isinstance(r,Exception)),None)
        try:
            if owner.lane is not None:
                await control('capture_end');await owner.close();report.update(owner.descriptor())
            if failure is not None:report['error']=type(failure).__name__+': '+str(failure)
            elif started is None:report['error']='stopped before initialization'
            else:report['status']='raw_capture_complete'
        except Exception as exc:
            report['error']=type(exc).__name__+': '+str(exc)
            try:await owner.close()
            except Exception:pass
        if hasattr(owner,'provenance'):report.update(owner.descriptor())
        report['elapsed_seconds']=0 if started is None else time.monotonic()-started
        report['topics_seen']=sorted(topics_seen)
        try:
            if root.exists():_publish_bytes(root/'result.json',canonical(report))
        finally:os.close(lock)
    return dict(root=str(root),**report)


def verify(root,report,*,export=None):
    root=Path(root);config=strict_json((root/'config.json').read_bytes());routes=capture_plans(config)
    event_streams=config.get('event_streams', False)
    if (report['schema']!=('dc.event-streams/1' if event_streams else 'dc.binance-selected/1')
            or report['profile']!=(EVENT_PROFILE if event_streams else PROFILE) or report['plans']!=routes
            or (report['venue'],report['environment'])!=(config['venue'],config['environment'])):
        raise ValueError('capture plan differs from config')
    by_route={p['id']:p for p in routes};states={};requests={};metadata_responses={}
    count=frames=byte_count=versions=rejected=throttled=connections=initialized=socket_events=0
    topics_seen=set();ready=False;ended=False;first=None
    target=None if export is None else Path(export)
    staged=None if target is None else target.with_name('.dc-export-'+uuid.uuid4().hex)
    output=None if staged is None else staged.open('xb')
    try:
        for record in verified_records(root,report):
            count+=1
            if output is not None:output.write(record.to_bytes()+b'\n')
            fields=strict_json(record.native_fields_json);connection=record.receipt.connection_id
            if record.channel=='um:exchange_info':
                if record.kind=='control':
                    # A failed previous response needs no projection; its raw
                    # HTTP proof has already been checked. Retain one poll only.
                    metadata_responses.clear();requests[record.record_id]=record
                elif record.kind=='rest_response':
                    request=requests.pop(fields.get('request_record_id'),None)
                    if (request is None or digest(request.to_bytes())!=fields.get('request_raw_sha256')
                            or strict_json(request.body)!=dict(method='GET',endpoint=ENDPOINTS['um'][0],params={})):
                        raise ValueError('metadata HTTP request proof differs')
                    metadata_responses[record.record_id]=record
                continue
            if record.channel=='capture_control':
                value=strict_json(record.body);event=value['event']
                if event=='metadata_version':
                    universe=value['universe'];source=metadata_responses.pop(universe['source']['record_id'],None)
                    if source is None or project(source,config['binance_symbols'],config['environment'],config['max_instruments'])!=universe:
                        raise ValueError('metadata projection differs from raw')
                    if first is None:first=universe
                    elif [(r['symbol'],r['incarnation']) for r in first['selected']]!=[(r['symbol'],r['incarnation']) for r in universe['selected']]:
                        raise ValueError('listing incarnation changed')
                    versions+=1
                elif event=='gap_start':
                    if connection not in states:
                        if value['route'] not in by_route:raise ValueError('unplanned route')
                        states[connection]=dict(route=value['route'],requested=False,ack=False,seen=set(),initialized=False,closed=False)
                    elif value.get('reason')=='connection_end':states[connection]['closed']=True
                elif event=='gap_end':
                    state=states[connection];plan=by_route[state['route']]
                    if not state['ack'] or not required(plan)<=state['seen'] or state['closed'] or state['initialized']:
                        raise ValueError('route initialized without ACK and first books')
                    state['initialized']=True;initialized+=1
                elif event=='capture_ready':
                    if {s['route'] for s in states.values() if s['initialized'] and not s['closed']}!=set(by_route):raise ValueError('capture ready before all routes')
                    ready=True
                elif event=='capture_end':ended=True
                elif event=='socket_event':socket_events+=1
            elif record.channel=='SUBSCRIBE':
                state=states[connection];plan=by_route[state['route']]
                if state['requested'] or strict_json(record.body)!=subscription(plan):
                    raise ValueError('subscription request differs from plan')
                state['requested']=True;connections+=1
            elif record.kind=='ws_message':
                frames+=1;byte_count+=len(record.body);state=states[connection];plan=by_route[state['route']]
                if not state['requested'] or state['closed'] or fields.get('route')!=state['route']:raise ValueError('message outside live route')
                value=_routing_json(record.body)
                kind,topic=classify_message(value,plan)
                if kind=='rejected':
                    rejected+=1;throttled+=int(value.get('code') in (-1003,-1008))
                elif kind=='ack':
                    if state['ack']:raise ValueError('invalid subscription ACK')
                    state['ack']=True
                elif kind=='event':
                    topics_seen.add(topic)
                    if state['ack']:state['seen'].add(topic)
        actual=dict(frames=frames,bytes=byte_count,metadata_versions=versions,rejected_subscriptions=rejected,
            throttled_subscriptions=throttled,connections=connections,initialized_connections=initialized,socket_events=socket_events)
        if any(report[k]!=v for k,v in actual.items()) or report['topics_seen']!=sorted(topics_seen):raise ValueError('capture counters differ from raw')
        complete=report['status']=='raw_capture_complete'
        if complete:
            if not ready or not ended or rejected or (not event_streams and first is None):
                raise ValueError('complete capture lacks initialization/metadata or has rejected subscriptions')
            if event_streams and any(not state['closed'] for state in states.values()):
                raise ValueError('complete event capture lacks terminal gap records')
            if not event_streams and canonical(first)!=(root/'universe.json').read_bytes():
                raise ValueError('initial universe differs')
        if output is not None:
            output.flush()
            os.fsync(output.fileno())
            output.close()
            os.link(staged,target)  # Publish without replacing an existing export.
            sync_directory(target.parent)
        return dict(archive_verified=True,records=count,ws_messages=frames,metadata_versions_verified=versions,
            feed_plan_initialization_verified=complete,capture_status=report['status'],topics_seen=sorted(topics_seen),
            liquidation_pushes_observed=any(t==LIQUIDATIONS or t.startswith('allLiquidation.') for t in topics_seen))
    finally:
        if output is not None:
            try:output.close()
            finally:staged.unlink(missing_ok=True)
