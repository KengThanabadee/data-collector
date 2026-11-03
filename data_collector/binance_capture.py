"""Selected USDⓈ-M raw streams; books are native top-20 replacements."""
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
from .contracts import canonical,digest,strict_json,integer
from .continuous import _routing_json
from .journal import _publish_bytes
from .rawrun import RawOwner,verified_records

PROFILE='binance-um-top20/1'
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
    routes=plans(config.binance_symbols);stopped=asyncio.Event();tasks=[];started=None;failure=None
    initialized=set();topics_seen=set();current=None
    report=dict(schema='dc.binance-selected/1',profile=PROFILE,venue='binance',environment=config.environment,
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
            if not await budget.connection(stopping):return
            connection=plan['id']+'-'+uuid.uuid4().hex
            await control('gap_start',connection=connection,route=plan['id'],reason='connection_start')
            ws=None
            try:
                async with asyncio.timeout(config.timeout_seconds):
                    ws=await session.ws_connect(plan['url'],compress=15,autoping=True,heartbeat=30,
                        max_msg_size=2*1024*1024,timeout=aiohttp.ClientWSTimeout(ws_close=5))
                report['connections']+=1
                payload=canonical(dict(method='SUBSCRIBE',params=plan['topics'],id=1))
                await owner.save(payload,'SUBSCRIBE','control',connection=connection,fields=dict(route=plan['id']))
                await asyncio.wait_for(ws.send_str(payload.decode()),config.timeout_seconds)
                acked=False;seen=set();connected=time.monotonic();complete=False
                while not stopping():
                    owner.check()
                    if not acked and time.monotonic()-connected>config.timeout_seconds:raise TimeoutError('subscription ACK timeout')
                    if not complete and time.monotonic()-connected>config.bootstrap_timeout_seconds:raise TimeoutError('Binance route bootstrap timeout')
                    if acked and required(plan)<=seen and not complete:
                        await control('gap_end',connection=connection,route=plan['id'])
                        complete=True;delay=1;initialized.add(plan['id']);report['initialized_connections']+=1
                    try:message=await asyncio.wait_for(ws.receive(),.25)
                    except TimeoutError:continue
                    deliveries=[];native_bytes=0;deadline=time.monotonic()+.05;closed=None
                    while True:
                        if message.type not in (aiohttp.WSMsgType.TEXT,aiohttp.WSMsgType.BINARY):
                            closed=dict(type=int(message.type),data=str(message.data)[:1024],reason=str(message.extra)[:1024]);break
                        body=message.data.encode() if isinstance(message.data,str) else message.data
                        record,future=owner.admit(body,'multiplex','ws_message',connection=connection,fields=dict(route=plan['id']))
                        deliveries.append((body,record,future));native_bytes+=len(body)
                        if len(deliveries)>=512 or native_bytes>=1024*1024 or time.monotonic()>=deadline:break
                        try:message=await asyncio.wait_for(ws.receive(),max(.0001,deadline-time.monotonic()))
                        except TimeoutError:break
                    acks=await asyncio.gather(*(future for _,_,future in deliveries))
                    for (_,record,_),ack in zip(deliveries,acks):owner.check_ack(record,ack)
                    report['frames']+=len(deliveries);report['bytes']+=native_bytes
                    for body,record,_ in deliveries:
                        value=_routing_json(body)
                        if 'code' in value:
                            report['rejected_subscriptions']+=1
                            report['throttled_subscriptions']+=int(value['code'] in (-1003,-1008))
                            raise ValueError('Binance subscription rejected')
                        if value.get('id')==1 and 'result' in value:
                            if value['result'] is not None or acked:raise ValueError('unexpected subscription ACK')
                            acked=True;continue
                        data=value.get('data',value)
                        if isinstance(data,dict) and data.get('e')=='serverShutdown':raise ConnectionError('venue serverShutdown')
                        topic=validate_event(value,plan);topics_seen.add(topic)
                        if acked:seen.add(topic)
                    if config.max_frames and report['frames']>=config.max_frames:stopped.set();report['stop_reason']='max_frames'
                    if config.max_bytes and report['bytes']>=config.max_bytes:stopped.set();report['stop_reason']='max_bytes'
                    if closed is not None:
                        await control('socket_event',connection=connection,route=plan['id'],**closed)
                        report['socket_events']+=1;raise ConnectionError('socket closed')
            except (aiohttp.ClientError,OSError,TimeoutError) as exc:
                owner.check();await control('transport_error',connection=connection,route=plan['id'],reason=type(exc).__name__)
            finally:
                initialized.discard(plan['id'])
                if ws is not None:
                    try:await asyncio.wait_for(ws.close(),5)
                    except (aiohttp.ClientError,OSError,TimeoutError):pass
                await control('gap_start',connection=connection,route=plan['id'],reason='connection_end')
            if not stopping():await wait(random.uniform(.8,1)*delay);delay=min(60,delay*2)

    async def audits(session):
        while not stopping():
            await wait(METADATA_INTERVAL_SECONDS)
            if stopping():return
            delay=1
            while not stopping() and not await metadata(session):
                await wait(delay);delay=min(60,delay*2)

    async def own():
        nonlocal started,budget
        await owner.start();await owner.aligned(stopping)
        if stopping():return
        budget=Budget(root/'budget.json',observer()[2])
        async with session_factory() as session:
            deadline=time.monotonic()+config.bootstrap_timeout_seconds
            delay=1
            while not stopping() and not await metadata(session):
                if time.monotonic()>=deadline:raise TimeoutError('initial metadata deadline')
                await wait(delay);delay=min(60,delay*2)
            if stopping():return
            tasks.extend(asyncio.create_task(route(session,p)) for p in routes)
            tasks.append(asyncio.create_task(audits(session)))
            while not stopping():
                owner.check()
                for task in tasks:
                    if task.done():task.result();raise ValueError('raw acquisition task ended')
                if started is None and initialized=={p['id'] for p in routes}:
                    started=time.monotonic();await control('capture_ready')
                    _publish_bytes(root/'ready.json',canonical(dict(utc_ns=observer()[0],channels=sum(len(p['topics']) for p in routes))))
                if started is None and time.monotonic()>=deadline:raise TimeoutError('Binance initialization deadline')
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
    root=Path(root);config=strict_json((root/'config.json').read_bytes());routes=plans(config['binance_symbols'])
    if report['profile']!=PROFILE or report['plans']!=routes:raise ValueError('capture plan differs from config')
    by_route={p['id']:p for p in routes};states={};requests={};metadata_responses={}
    count=frames=byte_count=versions=rejected=throttled=connections=initialized=socket_events=0
    topics_seen=set();ready=False;ended=False;first=None
    output=None if export is None else Path(export).open('xb')
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
                if state['requested'] or strict_json(record.body)!=dict(method='SUBSCRIBE',params=plan['topics'],id=1):
                    raise ValueError('subscription request differs from plan')
                state['requested']=True;connections+=1
            elif record.kind=='ws_message':
                frames+=1;byte_count+=len(record.body);state=states[connection];plan=by_route[state['route']]
                if not state['requested'] or state['closed'] or fields.get('route')!=state['route']:raise ValueError('message outside live route')
                value=_routing_json(record.body)
                if 'code' in value:
                    rejected+=1;throttled+=int(value['code'] in (-1003,-1008));continue
                if value.get('id')==1 and 'result' in value:
                    if value['result'] is not None or state['ack']:raise ValueError('invalid subscription ACK')
                    state['ack']=True;continue
                data=value.get('data',value)
                if isinstance(data,dict) and data.get('e')=='serverShutdown':continue
                topic=validate_event(value,plan);topics_seen.add(topic)
                if state['ack']:state['seen'].add(topic)
        actual=dict(frames=frames,bytes=byte_count,metadata_versions=versions,rejected_subscriptions=rejected,
            throttled_subscriptions=throttled,connections=connections,initialized_connections=initialized,socket_events=socket_events)
        if any(report[k]!=v for k,v in actual.items()) or report['topics_seen']!=sorted(topics_seen):raise ValueError('capture counters differ from raw')
        complete=report['status']=='raw_capture_complete'
        if complete:
            if not ready or not ended or first is None or rejected:raise ValueError('complete capture lacks initialization/metadata or has rejected subscriptions')
            if canonical(first)!=(root/'universe.json').read_bytes():raise ValueError('initial universe differs')
        return dict(archive_verified=True,records=count,ws_messages=frames,metadata_versions_verified=versions,
            feed_plan_initialization_verified=complete,capture_status=report['status'],topics_seen=sorted(topics_seen),
            liquidation_pushes_observed=LIQUIDATIONS in topics_seen)
    finally:
        if output is not None:output.close()
