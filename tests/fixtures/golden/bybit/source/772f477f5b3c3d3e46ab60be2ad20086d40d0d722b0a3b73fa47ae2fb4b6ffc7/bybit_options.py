"""Raw Bybit BTC/ETH option tickers and trades with a changing universe.

Discovery walks every page of /v5/market/instruments-info?category=option for
both base coins (PreLaunch included) on a fixed interval and archives each HTTP
exchange append-only. Topics are sharded across public option connections
below the documented 21,000 args-array characters and 2,000 args per
connection. Inbound frames are archived exactly as received before they are
interpreted. Bybit option command responses carry no request id, so each
connection has at most one subscribe/unsubscribe request in flight.
"""
import asyncio
import os
from pathlib import Path
import random
import shutil
import time
import uuid

import aiohttp
from yarl import URL

from .contracts import canonical,digest,strict_json
from .continuous import _routing_json
from .journal import _publish_bytes
from .option_universe import DEADLINE_NS,OptionUniverse,bybit_options
from .rawrun import RawOwner,verified_records

SCHEMA='dc.bybit-options/1'
PROFILE='bybit-option-tickers-publictrade/1'
BASES=('BTC','ETH')
TRADES=tuple('publicTrade.'+base for base in BASES)
CAP_CHARS,CAP_ARGS=21000,2000
HEADERS={'content-type','content-length','date','retry-after','x-bapi-limit','x-bapi-limit-status',
         'x-bapi-limit-reset-timestamp','traceid','timenow'}
PING_SECONDS=20
IDLE_SECONDS=30
CONNECT_SPACING_SECONDS=5
MAX_PAGES=100


def ticker(symbol):
    return 'tickers.'+symbol


def topic_chars(topic):
    return len(topic)+3  # two quotes and one separator in the JSON args array


def page_params(base, cursor=''):
    params=dict(category='option',baseCoin=base,limit=1000)
    if cursor:params['cursor']=cursor
    return params


def page_url(host, params):
    # The native cursor is already URL-encoded (for example 0%2C2); send it as returned.
    query='&'.join(f'{k}={v}' for k,v in params.items())
    return f'https://{host}/v5/market/instruments-info?{query}'


def page_body(response):
    fields=strict_json(response.native_fields_json)
    if fields.get('status')!=200 or fields.get('complete') is not True or fields.get('error') is not None:
        raise ValueError('complete successful Bybit discovery response required')
    value=strict_json(response.body)
    if not isinstance(value,dict) or value.get('retCode')!=0 or not isinstance(value.get('result'),dict):
        raise ValueError('successful Bybit option discovery body required')
    return value


def next_cursor(value):
    cursor=value['result'].get('nextPageCursor') or ''
    if not isinstance(cursor,str) or len(cursor)>4096 or not cursor.isascii() or any(c in cursor for c in '&#? \t\r\n'):
        raise ValueError('bounded native cursor required')
    return cursor


def project_poll(responses, *, max_instruments):
    """Rows from one complete BTC-then-ETH walk; raises unless every page is valid."""
    rows=[];index=0
    for base in BASES:
        pages=[]
        while True:
            if index>=len(responses):raise ValueError('incomplete Bybit discovery walk')
            response=responses[index];index+=1
            params=strict_json(response.native_fields_json).get('params')
            body=page_body(response);pages.append((params,body))
            if not next_cursor(body):break
        rows.extend(bybit_options(pages,base=base,max_instruments=max_instruments))
    if index!=len(responses):raise ValueError('responses after completed discovery walk')
    if len(rows)>max_instruments:raise ValueError('option count bound; no prefix selected')
    return tuple(rows)


def route(value):
    """Classify one decoded option-socket message; unknown shapes fail closed."""
    if value.get('op')=='pong':return 'pong',None
    if 'success' in value:
        data=value.get('data')
        topics=data.get('successTopics') if isinstance(data,dict) else None
        ok=(value.get('success') is True and isinstance(data,dict) and data.get('failTopics')==[]
            and isinstance(topics,list) and all(isinstance(t,str) for t in topics))
        return 'ack',(ok,topics)
    topic=value.get('topic')
    if isinstance(topic,str) and topic.startswith('tickers.'):
        data=value.get('data')
        if not isinstance(data,dict) or not isinstance(data.get('symbol'),str) or ticker(data['symbol'])!=topic:
            raise ValueError('native ticker identity differs')
        return 'ticker',topic
    if topic in TRADES:
        if not isinstance(value.get('data'),list):raise ValueError('native option trades required')
        return 'trade',topic
    raise ValueError('unrecognized Bybit option message')


def evidence(keys, observed, request, ack, connection, first):
    """Observation, request and ACK timing for each ticker incarnation in one ACK."""
    rows=[]
    for topic,key in keys.items():
        if key is None:continue
        utc,mono=observed[key]
        rows.append(dict(symbol=key[1],incarnation=key[2],connection=connection,
            observed_utc_ns=utc,observed_monotonic_ns=mono,
            request_record_id=request.record_id,request_utc_ns=request.receipt.receive_utc_ns,
            request_monotonic_ns=request.receipt.monotonic_ns,
            ack_record_id=ack.record_id,ack_utc_ns=ack.receipt.receive_utc_ns,ack_monotonic_ns=ack.receipt.monotonic_ns,
            first_ack=key in first,observed_to_ack_ns=ack.receipt.monotonic_ns-mono,
            deadline_met=(ack.receipt.monotonic_ns-mono<=DEADLINE_NS) if key in first else None))
    return canonical(dict(schema='dc.bybit-subscription-evidence/1',ack_record_id=ack.record_id,options=rows))


class Shard:
    """One public option connection's desired topics, with fixed trade topics on shard 0."""
    def __init__(self, index, fixed=()):
        self.index=index;self.fixed={topic:None for topic in fixed};self.assigned={}
        self.load=2+sum(topic_chars(t) for t in fixed)
        self.revision=0;self.ready=False;self.task=None;self.attempts=0
        self.pending_keys=set()

    def fits(self, name, chars, args):
        return (self.load+topic_chars(ticker(name))<=min(chars,CAP_CHARS) and
            len(self.fixed)+len(self.assigned)+1<=min(args,CAP_ARGS))

    def assign(self, name, key):
        self.assigned[name]=key;self.load+=topic_chars(ticker(name))

    def unassign(self, name):
        self.assigned.pop(name);self.load-=topic_chars(ticker(name))

    def desired(self):
        return dict(self.fixed,**{ticker(name):key for name,key in self.assigned.items()})

    def summary(self):
        return dict(index=self.index,args=len(self.fixed)+len(self.assigned),args_chars=self.load,
            connection_attempts=self.attempts)


async def fetch_page(meta, session, config, poll, params):
    url=page_url(config.bybit_rest_host,params)
    request=await meta.save(canonical(dict(method='GET',url=url,params=params)),'instruments-info','control',
        product='discovery',fields=dict(schema='dc.bybit-http/1',direction='outbound',poll=poll))
    fields=dict(schema='dc.bybit-http/1',direction='inbound',poll=poll,url=url,params=params,
        request_record_id=request.record_id,request_raw_sha256=digest(request.to_bytes()),
        status=None,headers=[],complete=False,error=None,body_decoded=True)
    utc,mono,_=meta.observer();fields['request_start']=dict(utc_ns=utc,monotonic_ns=mono)
    body=bytearray()
    try:
        if getattr(session,'_retry_connection',True) is not False:raise ValueError('disable implicit HTTP retries')
        async with asyncio.timeout(config.timeout_seconds):
            async with session.get(URL(url,encoded=True),allow_redirects=False) as response:
                fields['status']=response.status
                fields['headers']=[[k.decode('latin1'),v.decode('latin1')] for k,v in response.raw_headers
                    if k.decode('latin1').lower() in HEADERS][:32]
                while True:
                    chunk=await response.content.read(min(65536,config.max_response_bytes-len(body)+1))
                    if not chunk:break
                    body.extend(chunk)
                    if len(body)>config.max_response_bytes:raise ValueError('HTTP response byte bound')
                fields['complete']=True
    except (aiohttp.ClientError,OSError,TimeoutError,ValueError) as exc:
        fields['error']=type(exc).__name__+': '+str(exc)[:512]
    utc,mono,_=meta.observer();fields['request_end']=dict(utc_ns=utc,monotonic_ns=mono)
    return await meta.save(bytes(body),'instruments-info','rest_response',product='discovery',fields=fields)


async def capture(config, *, session_factory, observer, clock_read=None, clock_wait=None):
    from .app import _bind_root,_drain
    lock=_bind_root(config)
    root=config.data_root/'runs'/uuid.uuid4().hex
    anchor=canonical(dict(schema='dc.bybit-option-anchor/1',venue='bybit',environment=config.environment,
        rest_host=config.bybit_rest_host,ws_host=config.bybit_ws_host))
    main=RawOwner(root,config,observer,universe=anchor,batch_size=512,clock_read=clock_read,clock_wait=clock_wait)
    meta=RawOwner(root/'metadata',config,observer,universe=anchor,clock_read=clock_read,clock_wait=clock_wait)
    state=OptionUniverse('bybit',max_instruments=config.max_instruments)
    report=dict(schema=SCHEMA,venue='bybit',environment=config.environment,status='failed',profile=PROFILE,
        rest_host=config.bybit_rest_host,ws_host=config.bybit_ws_host,metadata={},frames=0,bytes=0,connections=0,
        connection_attempts=0,universe_versions=0,retired_options=0,polls=0,failed_polls=0,
        rejected_subscriptions=0,subscribed_options=0,first_acks=0,late_first_acks=0,max_first_observed_to_ack_ns=0,
        started_utc_ns=observer()[0],ready_utc_ns=None,elapsed_seconds=0,shards=[])
    shards=[];observed={};covered={};first_pending=set()
    stop=False;initial_ready=None;latest=None;session=None;last_connect=[0.0];missed_retirement=False
    connect_lock=asyncio.Lock()

    def should_stop():
        if stop:return True
        if config.supervisor_pid is not None and os.getppid()!=config.supervisor_pid:
            report['stop_reason']='supervisor_exit';return True
        if initial_ready is not None and config.duration_seconds and time.monotonic()-initial_ready>=config.duration_seconds:
            report['stop_reason']='duration';return True
        if (config.max_frames and report['frames']>=config.max_frames) or (config.max_bytes and report['bytes']>=config.max_bytes):
            report['stop_reason']='frame_or_byte_limit';return True
        return False

    async def control(value):
        return await main.save(canonical(value),'option_control','control',connection='controls')

    def prune_history():
        keep={row.key for row in state.members.values()}
        keep.update(key for shard in shards for key in shard.pending_keys)
        for mapping in (observed,covered):
            for key in list(mapping):
                if key not in keep:mapping.pop(key)
        first_pending.intersection_update(keep)

    def assign():
        for shard in shards:
            for name,key in list(shard.assigned.items()):
                if name not in state.members or state.members[name].key!=key:shard.unassign(name)
        placed={name for shard in shards for name in shard.assigned}
        for name in sorted(state.members):
            if name in placed:continue
            fitting=[s for s in shards if s.fits(name,config.shard_arg_chars,config.shard_max_args)]
            if fitting:shard=min(fitting,key=lambda s:s.load)
            else:
                shard=Shard(len(shards),() if shards else TRADES);shards.append(shard)
                if not shard.fits(name,config.shard_arg_chars,config.shard_max_args):
                    raise ValueError('shard limits cannot hold one option topic')
            shard.assign(name,state.members[name].key)
        for shard in shards:shard.revision=state.revision

    async def apply(rows, ids, poll, reason, utc, mono):
        nonlocal missed_retirement
        encoded=state.refresh(rows,source_ids=ids,utc_ns=utc,monotonic_ns=mono)
        value=strict_json(encoded)
        for item in value['retirements']:
            if covered.get(('bybit',item['symbol'],item['incarnation']),set())!={'ack','push'}:
                missed_retirement=True
        for name in value['added']:
            key=state.members[name].key;observed[key]=(utc,mono);first_pending.add(key)
        envelope=canonical(dict(schema='dc.bybit-option-revision/1',reason=reason,poll=poll,revision=value))
        saved=await meta.save(envelope,'option_universe','control',connection='universe')
        # Shards see the new assignment only after its control is admitted, so
        # every request is archived after the universe version it relies on.
        record,future=main.admit(canonical(dict(event='universe_version',record_id=saved.record_id,
            raw_sha256=digest(saved.to_bytes()),revision_sha256=digest(encoded),revision=state.revision)),
            'option_control','control',connection='controls')
        assign()
        main.confirm(record,await future)
        prune_history()
        report['universe_versions']+=1;report['retired_options']+=len(value['retired'])

    async def poll():
        """One complete discovery walk. Returns seconds until the next poll."""
        nonlocal latest
        report['polls']+=1;number=report['polls'];responses=[]
        failed=False
        for base in BASES:
            cursor=''
            while not failed:
                response=await fetch_page(meta,session,config,number,page_params(base,cursor))
                responses.append(response)
                try:cursor=next_cursor(page_body(response))
                except ValueError:failed=True;break  # no further requests while the venue is failing
                if not cursor:break
                if len(responses)>=MAX_PAGES:failed=True
        ids=[r.record_id for r in responses];last=responses[-1].receipt
        try:rows=project_poll(responses,max_instruments=config.max_instruments);error=None
        except (ValueError,TypeError,KeyError) as exc:rows=None;error=type(exc).__name__+': '+str(exc)[:512]
        changed=rows is not None and (state.revision==0 or state.changes(rows,utc_ns=last.receive_utc_ns))
        await meta.save(canonical(dict(schema='dc.bybit-poll/1',poll=number,response_record_ids=ids,
            ok=rows is not None,error=error,changed=changed)),'poll_result','control',connection='universe')
        if rows is None:
            report['failed_polls']+=1;state.stale(error)
            await control(dict(event='discovery_unhealthy',poll=number,reason=error))
            status=strict_json(responses[-1].native_fields_json).get('status')
            return 600 if status==403 else min(60,config.discovery_interval_seconds)  # Bybit 403 is an IP ban
        latest=(rows,ids,number)
        if changed:await apply(rows,ids,number,'refresh',last.receive_utc_ns,last.monotonic_ns)
        return config.discovery_interval_seconds

    async def pace_connect():
        async with connect_lock:
            wait=last_connect[0]+CONNECT_SPACING_SECONDS-time.monotonic()
            while wait>0 and not stop:
                await asyncio.sleep(min(.1,wait));wait=last_connect[0]+CONNECT_SPACING_SECONDS-time.monotonic()
            last_connect[0]=time.monotonic()

    async def run_shard(shard):
        backoff=1
        while not stop:
            connection=f'shard-{shard.index}-{shard.attempts}';shard.attempts+=1;shard.ready=False
            socket=None;active={};inflight=None;pushed=set();gap_closed=False;sequence=0
            report['connection_attempts']+=1
            await control(dict(event='gap_start',connection=connection,shard=shard.index,reason='connection_start'))
            try:
                await pace_connect()
                if stop:break
                async with asyncio.timeout(config.timeout_seconds):
                    # permessage-deflate is transport framing like TLS; the archive keeps
                    # each decompressed message payload exactly as Bybit sent it.
                    socket=await session.ws_connect(f'wss://{config.bybit_ws_host}/v5/public/option',max_msg_size=2*1024*1024,
                        autoping=True,compress=15,timeout=aiohttp.ClientWSTimeout(ws_close=5))
                report['connections']+=1
                await control(dict(event='connection_open',connection=connection,shard=shard.index,
                    compress=getattr(socket,'compress',0)))
                connected=last_received=time.monotonic();next_ping=connected+PING_SECONDS

                async def send(op, topics=None, keys=None):
                    nonlocal inflight,sequence
                    sequence+=1;message=dict(op=op,req_id=f'{connection}-{sequence}')
                    if topics is not None:message['args']=topics
                    body=canonical(message)
                    if op!='ping':
                        shard.pending_keys={key for key in keys.values() if key is not None} if op=='subscribe' else set()
                    saved=await main.save(body,op,'control',connection=connection,
                        fields=dict(direction='outbound',shard=shard.index,revision=shard.revision))
                    if op!='ping':
                        inflight=dict(op=op,topics=topics,keys=keys,record=saved,sent=time.monotonic())
                    await asyncio.wait_for(socket.send_str(body.decode()),config.timeout_seconds)

                while not stop:
                    now=time.monotonic()
                    if not gap_closed and now-connected>config.bootstrap_timeout_seconds:
                        raise TimeoutError('option shard bootstrap timeout')
                    if inflight is not None and now-inflight['sent']>config.timeout_seconds:
                        raise TimeoutError('subscription ACK timeout')
                    if now>=next_ping:
                        await send('ping');next_ping=now+PING_SECONDS
                    if inflight is None:
                        desired=shard.desired()
                        remove=[t for t,key in active.items() if t not in desired or desired[t]!=key]
                        add=[t for t in desired if t not in active]
                        if remove:
                            part=remove[:config.subscription_batch_size]
                            await send('unsubscribe',part,{t:active[t] for t in part})
                        elif add:
                            part=add[:config.subscription_batch_size]
                            await send('subscribe',part,{t:desired[t] for t in part})
                        elif not gap_closed and all(t in pushed for t in active if t.startswith('tickers.')):
                            await control(dict(event='gap_end',connection=connection,shard=shard.index,revision=shard.revision))
                            gap_closed=True;shard.ready=True;backoff=1
                    try:message=await asyncio.wait_for(socket.receive(),.25)
                    except TimeoutError:
                        if time.monotonic()-last_received>IDLE_SECONDS:raise TimeoutError('socket idle timeout')
                        continue
                    deliveries=[];size=0;closed=False;end=time.monotonic()+.02;receive_error=None
                    try:
                        while True:
                            if message.type not in (aiohttp.WSMsgType.TEXT,aiohttp.WSMsgType.BINARY):
                                body=canonical(dict(type=int(message.type),code=message.data if type(message.data) is int else None,
                                    detail=str(message.extra if message.extra is not None else message.data)[:1024]))
                                deliveries.append(main.admit(body,'socket_event','control',connection=connection))
                                closed=True;break
                            body=message.data.encode() if message.type==aiohttp.WSMsgType.TEXT else message.data
                            deliveries.append(main.admit(body,'multiplex','ws_message',connection=connection))
                            size+=len(body);last_received=time.monotonic()
                            if len(deliveries)>=512 or size>=1024*1024 or time.monotonic()>=end:break
                            try:message=await asyncio.wait_for(socket.receive(),end-time.monotonic())
                            except TimeoutError:break
                    except Exception as exc:receive_error=exc
                    acknowledgements=await asyncio.gather(*(future for _,future in deliveries))
                    confirmed=[main.confirm(record,ack) for (record,_),ack in zip(deliveries,acknowledgements)]
                    # Count the whole durable batch first, so a failure mid-batch keeps
                    # counters equal to the retained raw that verify-run checks.
                    for record in confirmed:
                        if record.kind=='ws_message':report['frames']+=1;report['bytes']+=len(record.body)
                    for record in confirmed:
                        if record.kind!='ws_message':continue
                        kind,payload=route(_routing_json(record.body))
                        if kind=='ack':
                            ok,topics=payload
                            if inflight is None:raise ValueError('Bybit command response without a request')
                            if not ok or len(topics)!=len(inflight['topics']) or set(topics)!=set(inflight['topics']):
                                report['rejected_subscriptions']+=1
                                raise ValueError('Bybit subscription rejected or differs; raw preserved')
                            if inflight['op']=='subscribe':
                                active.update(inflight['keys'])
                                first={key for key in inflight['keys'].values() if key in first_pending}
                                encoded=evidence(inflight['keys'],observed,inflight['record'],record,connection,first)
                                await main.save(encoded,'option_subscription','control',connection='controls')
                                for row in strict_json(encoded)['options']:
                                    if row['first_ack']:
                                        report['first_acks']+=1;report['late_first_acks']+=not row['deadline_met']
                                        report['max_first_observed_to_ack_ns']=max(report['max_first_observed_to_ack_ns'],row['observed_to_ack_ns'])
                                for key in inflight['keys'].values():
                                    if key is not None:covered.setdefault(key,set()).add('ack');first_pending.discard(key)
                            else:
                                for topic in inflight['topics']:active.pop(topic,None);pushed.discard(topic)
                            inflight=None
                            shard.pending_keys.clear();prune_history()
                        elif kind=='ticker':
                            key=active.get(payload)
                            if key is None and inflight is not None and inflight['op']=='subscribe':key=inflight['keys'].get(payload)
                            if key is not None:pushed.add(payload);covered.setdefault(key,set()).add('push')
                    if receive_error is not None:raise receive_error
                    if closed:raise ConnectionError('socket closed')
            except (aiohttp.ClientError,OSError,TimeoutError) as exc:
                main.check()
                await control(dict(event='gap_start',connection=connection,shard=shard.index,reason=type(exc).__name__))
            finally:
                shard.ready=False
                if socket is not None:
                    # aiohttp's own 5-second close timeout releases the connection; on a
                    # stalled link the close frame itself can block, so abort after that.
                    try:await asyncio.wait_for(socket.close(),config.timeout_seconds)
                    except TimeoutError:socket._response.close()  # pinned aiohttp 3.14.5
                await control(dict(event='gap_start',connection=connection,shard=shard.index,reason='connection_end'))
                shard.pending_keys.clear();prune_history()
            if not stop:
                until=time.monotonic()+random.uniform(.8,1)*backoff
                while not stop and time.monotonic()<until:await asyncio.sleep(.1)
                backoff=min(60,backoff*2)

    async def own():
        nonlocal stop,initial_ready,session
        await main.start();await meta.start()
        started=await control(dict(event='capture_start'))
        report['started_utc_ns']=started.receipt.receive_utc_ns
        await main.aligned(lambda:stop);await meta.aligned(lambda:stop)
        deadline=time.monotonic()+config.bootstrap_timeout_seconds
        async with session_factory() as session:
            try:
                next_poll=0;last_disk=0
                while not stop:
                    if should_stop():stop=True;break
                    now=time.monotonic();main.check();meta.check()
                    if now-last_disk>=1:
                        disk=shutil.disk_usage(root)
                        if disk.free<5*1024**3 or disk.free/disk.total<.10:raise ValueError('disk free below stop threshold')
                        last_disk=now
                    for shard in shards:
                        if shard.task is not None and shard.task.done():
                            shard.task.result();raise ValueError('option shard ended')
                    if initial_ready is None and now>=deadline:raise TimeoutError('initial option bootstrap deadline')
                    if now>=next_poll:next_poll=now+await poll()
                    utc,mono,_=observer()
                    if latest and any(row.expiry_ms*1_000_000<=utc for row in state.members.values()):
                        await apply(*latest,'expiry',utc,mono)
                    for shard in shards:
                        if shard.task is None:shard.task=asyncio.create_task(run_shard(shard))
                    if initial_ready is None and shards and all(s.ready for s in shards):
                        initial_ready=time.monotonic();report['ready_utc_ns']=observer()[0]
                        report['subscribed_options']=len(state.members)
                        _publish_bytes(root/'ready.json',canonical(dict(utc_ns=report['ready_utc_ns'],
                            options=len(state.members),shards=len(shards))))
                    await asyncio.sleep(.2)
                stop=True
                await _drain(asyncio.gather(*(s.task for s in shards if s.task is not None),return_exceptions=True))
                for shard in shards:
                    if shard.task is not None and not shard.task.cancelled():shard.task.result()
                utc,mono,_=observer()
                if latest and any(row.expiry_ms*1_000_000<=utc for row in state.members.values()):
                    await apply(*latest,'expiry',utc,mono)
                missing=[name for name,row in state.members.items() if covered.get(row.key,set())!={'ack','push'}]
                if missing:raise ValueError(f'capture ended before ACK and first push for {len(missing)} options')
                if report['late_first_acks']:raise ValueError('first option subscription exceeded 120 seconds')
                if missed_retirement:raise ValueError('retired option lacked ACK plus first push before retirement')
            finally:
                stop=True
                try:await _drain(asyncio.gather(*(s.task for s in shards if s.task is not None),return_exceptions=True))
                finally:
                    ended=await control(dict(event='capture_end'))
                    report['ended_utc_ns']=ended.receipt.receive_utc_ns
                    report['shards']=[s.summary() for s in shards]

    task=asyncio.create_task(own())
    try:
        try:await asyncio.shield(task)
        except asyncio.CancelledError:
            stop=True;await _drain(task)
        if initial_ready is None:raise ValueError('stopped before option initialization')
        report['status']='raw_capture_complete'
    except Exception as exc:report['error']=type(exc).__name__+': '+str(exc)
    finally:
        stop=True
        try:
            for owner in (meta,main):
                try:await owner.close()
                except Exception as exc:
                    report['status']='failed';report['error']=type(exc).__name__+': '+str(exc)
            if hasattr(main,'provenance'):report.update(main.descriptor())
            if hasattr(meta,'provenance'):report['metadata']=meta.descriptor()
            report['elapsed_seconds']=0 if initial_ready is None else time.monotonic()-initial_ready
            report.setdefault('ended_utc_ns',observer()[0])
            if root.exists():_publish_bytes(root/'result.json',canonical(report))
        finally:os.close(lock)
    return dict(root=str(root),**report)


class MetadataReplay:
    """Re-derive every poll result and universe revision from archived HTTP bytes."""
    def __init__(self, root, report, state, max_instruments):
        self.records=iter(verified_records(root,report));self.state=state;self.max_instruments=max_instruments
        self.requests={};self.responses={};self.latest=None;self.expected=None;self.count=0

    def next_revision(self):
        for record in self.records:
            self.count+=1;fields=strict_json(record.native_fields_json)
            if fields.get('schema')=='dc.bybit-http/1':
                if fields.get('direction')=='outbound':self.requests[record.record_id]=record;continue
                request=self.requests.pop(fields.get('request_record_id'),None)
                if request is None or digest(request.to_bytes())!=fields.get('request_raw_sha256'):
                    raise ValueError('HTTP request proof missing or differs')
                if strict_json(request.body)!=dict(method='GET',url=fields['url'],params=fields['params']):
                    raise ValueError('HTTP request identity differs')
                self.responses[record.record_id]=record;continue
            if record.channel=='poll_result':
                value=strict_json(record.body)
                if self.expected is not None:raise ValueError('changed poll lacks its universe version')
                responses=[self.responses.pop(i,None) for i in value['response_record_ids']]
                if None in responses or self.responses:raise ValueError('poll response list differs')
                try:rows=project_poll(responses,max_instruments=self.max_instruments)
                except (ValueError,TypeError,KeyError):rows=None
                if (rows is not None)!=value['ok']:raise ValueError('poll success differs from raw')
                if rows is not None:
                    last=responses[-1].receipt
                    changed=self.state.revision==0 or self.state.changes(rows,utc_ns=last.receive_utc_ns)
                    if changed!=value['changed']:raise ValueError('poll change classification differs')
                    self.latest=(rows,value['response_record_ids'],value['poll'],last)
                    if changed:self.expected=value['poll']
                continue
            if record.channel=='option_universe':
                value=strict_json(record.body);revision=value['revision']
                if value.get('schema')!='dc.bybit-option-revision/1' or self.latest is None:
                    raise ValueError('universe version lacks raw metadata proof')
                rows,ids,poll,last=self.latest
                if value['poll']!=poll or revision['source_record_ids']!=ids:raise ValueError('universe source differs')
                if value['reason']=='refresh':
                    if self.expected!=poll or (revision['observed_at_utc_ns'],revision['observed_at_monotonic_ns'])!=(
                            last.receive_utc_ns,last.monotonic_ns):
                        raise ValueError('refresh observation differs from raw poll')
                    self.expected=None
                elif value['reason']!='expiry' or revision['observed_at_monotonic_ns']>record.receipt.monotonic_ns:
                    raise ValueError('unrecognized or late universe observation')
                encoded=self.state.refresh(rows,source_ids=ids,utc_ns=revision['observed_at_utc_ns'],
                    monotonic_ns=revision['observed_at_monotonic_ns'])
                if encoded!=canonical(revision):raise ValueError('universe projection differs from raw metadata')
                return record,revision
        return None


def verify(root, report, *, export=None):
    """Verify raw integrity, every poll and universe version, ACKs and first pushes.

    Proves raw coverage with explicit connection gaps; it does not certify
    native continuity between consecutive pushes.
    """
    if export is not None:raise ValueError('export of multiple raw owners is not enabled')
    if report.get('profile')!=PROFILE:raise ValueError('unsupported Bybit option profile')
    if 'provenance' not in report:raise ValueError('no raw owner established; inspect the failed result')
    root=Path(root).resolve();config=strict_json((root/'config.json').read_bytes())
    anchor=canonical(dict(schema='dc.bybit-option-anchor/1',venue='bybit',environment=config['environment'],
        rest_host=config.get('bybit_rest_host') or config.get('rest_host') or 'api.bybit.com',
        ws_host=config.get('bybit_ws_host') or config.get('ws_host') or 'stream.bybit.com'))
    for descriptor in (report,report['metadata']):
        if descriptor['provenance']['universe_sha256']!=digest(anchor):raise ValueError('option anchor differs')
    state=OptionUniverse('bybit',max_instruments=config['max_instruments'])
    metadata=MetadataReplay(root/'metadata',report['metadata'],state,config['max_instruments'])
    complete=report['status']=='raw_capture_complete';semantic_error=False
    connections={};observed={};first_pending=set();covered={};expected={};bounds={}
    counts=dict(records=0,frames=0,bytes=0,versions=0,retired=0,rejected=0,attempts=0)

    def gap(key):
        return covered.get(key,set())!={'ack','push'}

    def prune_history():
        keep={row.key for row in state.members.values()}
        keep.update(key for conn in connections.values() if conn['inflight'] is not None
            for key in conn['inflight']['keys'].values() if key is not None)
        for mapping in (observed,covered):
            for key in list(mapping):
                if key not in keep:mapping.pop(key)
        first_pending.intersection_update(keep)

    def check_record(record):
        fields=strict_json(record.native_fields_json)
        if record.channel=='option_control':
            value=strict_json(record.body);event=value.get('event')
            if event in {'capture_start','capture_end'}:
                if event in bounds:raise ValueError('duplicate capture boundary')
                bounds[event]=record.receipt
            elif event=='gap_start' and value.get('reason')=='connection_start':
                if value['connection'] in connections:raise ValueError('duplicate connection identity')
                connections[value['connection']]=dict(active={},inflight=None,pushed=set())
                counts['attempts']+=1
            elif event=='gap_end':
                conn=connections[value['connection']]
                if conn['inflight'] is not None or any(t not in conn['pushed'] for t in conn['active'] if t.startswith('tickers.')):
                    raise ValueError('gap ended before shard initialization')
            elif event=='universe_version':
                acquired=metadata.next_revision()
                if acquired is None:raise ValueError('universe reference has no metadata record')
                source,revision=acquired
                if (source.record_id!=value['record_id'] or digest(source.to_bytes())!=value['raw_sha256'] or
                        value['revision']!=state.revision or value['revision_sha256']!=state.revision_sha256):
                    raise ValueError('universe version reference differs')
                for item in revision['retirements']:
                    key=('bybit',item['symbol'],item['incarnation'])
                    if complete and gap(key):
                        raise ValueError('retired option lacked ACK plus first push before retirement')
                for name in revision['added']:
                    key=state.members[name].key
                    observed[key]=(revision['observed_at_utc_ns'],revision['observed_at_monotonic_ns']);first_pending.add(key)
                prune_history()
                counts['versions']+=1;counts['retired']+=len(revision['retired'])
            return
        if record.channel=='option_subscription':
            value=strict_json(record.body)
            if expected.pop(value.get('ack_record_id'),None)!=record.body:
                raise ValueError('subscription timing differs from raw request/ACK')
            return
        if record.kind=='control' and record.channel in {'subscribe','unsubscribe','ping'}:
            conn=connections.get(record.receipt.connection_id)
            value=strict_json(record.body)
            if conn is None or value.get('op')!=record.channel or fields.get('direction')!='outbound':
                raise ValueError('outbound request outside a recorded connection')
            if record.channel=='ping':return
            topics=value.get('args')
            if conn['inflight'] is not None:raise ValueError('overlapping Bybit command requests')
            if not isinstance(topics,list) or not topics or len(set(topics))!=len(topics):
                raise ValueError('explicit distinct topics required')
            if record.channel=='subscribe':
                if fields.get('revision')!=state.revision:raise ValueError('subscription references another universe version')
                keys={}
                for topic in topics:
                    if topic in TRADES:keys[topic]=None;continue
                    name=topic[8:] if topic.startswith('tickers.') else None
                    if name not in state.members:raise ValueError('subscription outside current universe')
                    keys[topic]=state.members[name].key
                if conn['active'].keys()&set(topics):raise ValueError('duplicate active subscription')
            else:
                if any(t not in conn['active'] for t in topics):raise ValueError('unsubscribe lacks active topic')
                keys={t:conn['active'][t] for t in topics}
            conn['inflight']=dict(op=record.channel,topics=topics,keys=keys,record=record)
            return
        if record.kind!='ws_message':return
        conn=connections.get(record.receipt.connection_id)
        if conn is None:raise ValueError('message outside a recorded connection')
        kind,payload=route(_routing_json(record.body))
        inflight=conn['inflight']
        if kind=='ack':
            ok,topics=payload
            if inflight is None:raise ValueError('command response without a request')
            if not ok or len(topics)!=len(inflight['topics']) or set(topics)!=set(inflight['topics']):
                counts['rejected']+=1;raise ValueError('rejected or differing Bybit subscription in raw capture')
            if inflight['op']=='subscribe':
                conn['active'].update(inflight['keys'])
                first={key for key in inflight['keys'].values() if key in first_pending}
                expected[record.record_id]=evidence(inflight['keys'],observed,inflight['record'],record,
                    record.receipt.connection_id,first)
                if complete and any(row['first_ack'] and not row['deadline_met']
                        for row in strict_json(expected[record.record_id])['options']):
                    raise ValueError('first option subscription exceeded 120 seconds')
                for key in inflight['keys'].values():
                    if key is not None:covered.setdefault(key,set()).add('ack');first_pending.discard(key)
            else:
                for topic in inflight['topics']:conn['active'].pop(topic,None);conn['pushed'].discard(topic)
            conn['inflight']=None
            prune_history()
        elif kind=='ticker':
            key=conn['active'].get(payload)
            if key is None and inflight is not None and inflight['op']=='subscribe':key=inflight['keys'].get(payload)
            if key is not None:conn['pushed'].add(payload);covered.setdefault(key,set()).add('push')

    for record in verified_records(root,report):
        counts['records']+=1
        if record.kind=='ws_message':counts['frames']+=1;counts['bytes']+=len(record.body)
        if semantic_error:continue
        try:check_record(record)
        except (ValueError,TypeError,KeyError,IndexError):
            if complete:raise
            semantic_error=True
    extra=metadata.next_revision()
    if complete and extra is not None:raise ValueError('unreferenced universe version')
    while extra is not None:extra=metadata.next_revision()
    if (counts['frames'],counts['bytes'])!=(report['frames'],report['bytes']):raise ValueError('raw option counters differ')
    if complete:
        if set(bounds)!={'capture_start','capture_end'} or (bounds['capture_start'].receive_utc_ns,
                bounds['capture_end'].receive_utc_ns)!=(report['started_utc_ns'],report['ended_utc_ns']):
            raise ValueError('capture time bounds differ from raw controls')
        if (counts['versions'],counts['retired'],counts['attempts'])!=(report['universe_versions'],
                report['retired_options'],report['connection_attempts']):
            raise ValueError('universe, retirement or connection counters differ')
        if counts['rejected'] or report['rejected_subscriptions']:raise ValueError('subscription rejection')
        if expected:raise ValueError('ACK timing evidence missing')
        if metadata.expected is not None:raise ValueError('changed poll lacks its universe version')
        end=bounds['capture_end'].monotonic_ns
        if not state.revision or any(gap(row.key) for row in state.members.values()):
            raise ValueError('option lacks ACK plus first push')
        if any(row.expiry_ms*1_000_000<=report['ended_utc_ns'] for row in state.members.values()):
            raise ValueError('expired option not retired')
    return dict(archive_verified=True,records=counts['records']+metadata.count,ws_messages=counts['frames'],
        payload_bytes=counts['bytes'],universe_versions_verified=counts['versions'],
        final_options=len(state.members),changing_universe_coverage_verified=complete and not semantic_error,
        capture_status=report['status'],verification_scope='raw integrity, every discovery poll and universe '
        'version re-derived from archived HTTP bytes, ACK plus first ticker push per option incarnation, '
        'retirement exemptions; explicit gaps retained')
