"""Durable Deribit REST discovery over the existing venue lane and quota.

One caller-owned acquisition at a time. No subscription/lifecycle mutation or
implicit retry. A projection is an index into raw, not an enabled-feed manifest.
"""
import asyncio
from dataclasses import asdict,replace
from decimal import Decimal
import uuid

import aiohttp

from .contracts import InstrumentKey,RawRecord,Receipt,StreamKey,canonical,digest,integer,strict_json
from .decoders import native_json
from .lane import ArchiveLane,DurableAck,PENDING_CLOCK_EPOCH
from .quota import Priority,PriorityQuota

METHOD='public/get_instruments'
PARAMS=dict(currency='any',kind='any',expired='false',extended='true')
HEADERS={'content-type','content-encoding','content-length','date','retry-after',
         'x-ratelimit-limit','x-ratelimit-remaining','x-ratelimit-reset'}


def discovery_session():
    """Qualified public HTTP owner; callers close it with async with.

    aiohttp3.14.5 retries idempotent GET internally by default. Its pinned private
    switch is deliberately disabled and requalification is required on upgrade.
    """
    if aiohttp.__version__!='3.14.5':raise ValueError('qualify discovery transport for installed aiohttp')
    session=aiohttp.ClientSession(cookie_jar=aiohttp.DummyCookieJar(),trust_env=False,raise_for_status=False)
    session._retry_connection=False
    return session


def _native_values(value):
    if isinstance(value,Decimal):return str(value)
    if isinstance(value,dict):return {k:_native_values(v) for k,v in value.items()}
    if isinstance(value,list):return [_native_values(v) for v in value]
    return value


def project_instruments(record,*,max_instruments=10000):
    """Complete classification; no prefix selection and no inferred combo legs."""
    integer(max_instruments,'instrument count',1)
    if max_instruments>100000:raise ValueError('bounded discovery projection required')
    if (not isinstance(record,RawRecord) or record.venue!='deribit' or record.kind!='rest_response' or
            record.native_product!='discovery' or record.channel!=METHOD or record.instrument is not None):
        raise ValueError('Deribit discovery raw response required')
    fields=strict_json(record.native_fields_json)
    if (fields.get('schema')!='dc.deribit-discovery/1' or fields.get('params')!=PARAMS or
            fields.get('status')!=200 or fields.get('complete') is not True or fields.get('error') is not None):
        raise ValueError('complete successful discovery response required')
    value=native_json(record.body,max_bytes=16*1024*1024)
    if 'error' in value or value.get('jsonrpc')!='2.0' or not isinstance(value.get('result'),list):
        raise ValueError('successful native discovery result required')
    rows=value['result']
    if len(rows)>max_instruments:raise ValueError('instrument count bound; no subset selected')
    raw_sha256=digest(record.to_bytes())
    selected=[];excluded=[];blocked=[];symbols=set()
    families={kind:dict(observed=0,selected=0,excluded=0,blocked=0)
        for kind in ('spot','future','option','future_combo','option_combo')}
    for index,row in enumerate(rows):
        if not isinstance(row,dict):raise ValueError('discovery row must be an object')
        name=row.get('instrument_name');kind=row.get('kind');base=row.get('base_currency')
        if not isinstance(name,str) or not name or not isinstance(kind,str) or not isinstance(base,str):
            raise ValueError('unrecognized discovery identity; previous universe retained')
        if name in symbols:raise ValueError('duplicate discovery symbol')
        symbols.add(name);reference=dict(row_index=index,native_symbol=name,native_product=kind)
        family=families.setdefault(kind,dict(observed=0,selected=0,excluded=0,blocked=0));family['observed']+=1
        if kind in {'future_combo','option_combo'}:
            family['blocked']+=1;blocked.append(dict(**reference,reason='combo_leg_scope_unverified'));continue
        if kind not in {'spot','future','option'}:
            family['blocked']+=1;blocked.append(dict(**reference,reason='native_kind_unverified'));continue
        if base not in {'BTC','ETH'}:
            family['excluded']+=1;excluded.append(dict(**reference,reason='outside_BTC_ETH_underlying_scope'));continue
        created=row.get('creation_timestamp');identity=row.get('instrument_id')
        integer(created,'native creation ms',1);integer(identity,'native instrument id',1)
        if created>2**63//1_000_000 or identity>2**63-1:raise ValueError('native identity integer bound')
        key=InstrumentKey('deribit',record.environment,kind,name,f'id:{identity}:creation:{created}')
        family['selected']+=1
        selected.append(dict(instrument=asdict(key),effective_at_ns=None,attributes=dict(
            native=_native_values(row),native_decimal_encoding='decimal-as-string/1',
            source_record_id=record.record_id,source_raw_sha256=raw_sha256,row_index=index)))
    return canonical(dict(schema='dc.deribit-universe/1',response_record_id=record.record_id,
        response_raw_sha256=raw_sha256,request_record_id=fields['request_record_id'],
        params=PARAMS,observed_at_utc_ns=record.receipt.receive_utc_ns,total_rows=len(rows),
        selected=selected,excluded=excluded,blocked=blocked,families=families))


class DeribitDiscovery:
    """Archive request intent and complete/explicitly incomplete response first.

    Exceptions leave latest unchanged. Empty results cannot erase the previous
    universe. The owner is poisoned after any unconfirmed archive operation.
    """
    def __init__(self,lane,quota,*,charges,max_requests=10,max_response_bytes=1024*1024,
                 max_instruments=10000,timeout_seconds=30,before_dispatch=None):
        if (not isinstance(lane,ArchiveLane) or not isinstance(quota,PriorityQuota) or
                quota.venue!='deribit' or lane.writer.identity[0]!='deribit' or
                lane.writer.identity[1] not in {'production','testnet'} or not lane.health()['accepting'] or
                not lane.health()['running'] or not lane._clock_submitted):
            raise ValueError('Deribit lane and shared quota required')
        if max_requests is not None:integer(max_requests,'requests',1)
        for name,value in (('response bytes',max_response_bytes),
                           ('instruments',max_instruments),('timeout',timeout_seconds)):
            integer(value,name,1)
        if ((max_requests is not None and max_requests>10000) or max_response_bytes>16*1024*1024 or max_instruments>100000 or timeout_seconds>60 or
                not isinstance(charges,dict) or not charges or len(charges)>32):
            raise ValueError('bounded discovery configuration required')
        for name,cost in charges.items():
            integer(cost,'discovery quota cost',1)
            if name not in quota.pools or cost>quota.pools[name].rule.capacity:raise ValueError('admissible discovery quota required')
        if ('get_instruments' not in charges or quota.pools['get_instruments'].rule.capacity!=1 or
                quota.pools['get_instruments'].rule.window_ns<1_000_000_000):
            raise ValueError('shared get_instruments pool must enforce at most one request per second')
        self.lane,self.quota=lane,quota;self.charges=dict(charges)
        self.max_requests,self.max_response_bytes,self.max_instruments=max_requests,max_response_bytes,max_instruments
        self.timeout_seconds=timeout_seconds;self.requests=self._seq=0;self.latest=None;self.last_error=None
        self.poisoned=self._busy=self._cancelled=False
        if before_dispatch is not None and not callable(before_dispatch):raise ValueError('dispatch callback required')
        self.before_dispatch=before_dispatch
        identity=lane.identity
        self.source=StreamKey(identity['capture_id'],identity['process_session_id'],'deribit-discovery:'+uuid.uuid4().hex)
        self.endpoint=('https://www.deribit.com/api/v2/' if lane.writer.identity[1]=='production' else
                       'https://test.deribit.com/api/v2/')+METHOD
        # Reserve expansion of the complete bounded body plus one overflow byte,
        # response fields, envelope and FIFO clock/ordinal bookkeeping.
        envelope=4*((max_response_bytes+3)//3)+24000
        if envelope>min(lane.writer.limits.max_record_bytes,lane.queue.limits.max_item_bytes) or envelope+4096>lane.queue.limits.max_bytes:
            raise ValueError('discovery response exceeds archive bounds')

    async def _archive(self,body,kind,fields):
        utc,mono,_=self.lane._sample()
        record=RawRecord(Receipt(self.source.capture_id,self.source.process_session_id,self.source.connection_id,
            self._seq,utc,mono,PENDING_CLOCK_EPOCH),*self.lane.writer.identity,'discovery',METHOD,kind,body,
            self.lane.provenance,None,canonical(fields))
        self.poisoned=True
        future=self.lane.submit_received(record);self._seq+=1
        while True:
            try:ack=await asyncio.shield(future);break
            except asyncio.CancelledError:
                self._cancelled=True
                if future.done():ack=future.result();break
        if not isinstance(ack,DurableAck) or not ack.timing_valid or ack.record_id!=record.record_id:
            raise ValueError('discovery durability unconfirmed or late')
        saved=replace(record,receipt=replace(record.receipt,clock_epoch=ack.clock_epoch),
            native_fields_json=canonical(dict(fields,collector_ingress_ordinal=ack.ingress_ordinal)))
        if digest(saved.to_bytes())!=ack.raw_sha256:raise ValueError('discovery acknowledgement hash differs')
        self.poisoned=False
        return saved

    async def fetch(self,session):
        if self.poisoned or self._busy or self._cancelled:raise ValueError('discovery owner requires reopen')
        if self.max_requests is not None and self.requests>=self.max_requests:
            self.last_error='request_bound';raise ValueError('discovery request bound')
        if getattr(session,'_retry_connection',True) is not False:
            self.last_error='unqualified_transport';raise ValueError('discovery requires a qualified no-retry HTTP session')
        self._busy=True
        try:
            self.requests+=1
            request=await self._archive(canonical(dict(method='GET',endpoint=self.endpoint,params=PARAMS)),
                'control',dict(schema='dc.deribit-discovery/1',direction='outbound',params=PARAMS,dispatched=False))
            body=bytearray();status=None;headers=[];complete=False;error=None;start=None
            if self._cancelled:error='cancelled_before_dispatch'
            else:
                try:
                    _,mono,_=self.lane._sample()
                    await self.quota.acquire(self.charges,priority=Priority.DISCOVERY,deadline_ns=mono+self.timeout_seconds*1_000_000_000)
                    if self.before_dispatch is not None:await self.before_dispatch()
                    utc,mono,_=self.lane._sample();start=dict(utc_ns=utc,monotonic_ns=mono)
                    async with session.get(self.endpoint,params=dict(PARAMS),allow_redirects=False,auto_decompress=False,
                            headers={'Accept-Encoding':'identity'},raise_for_status=False,middlewares=(),
                            timeout=aiohttp.ClientTimeout(total=self.timeout_seconds)) as response:
                        status=response.status;header_bytes=0
                        for name,value in response.raw_headers:
                            if name.decode('ascii').lower() not in HEADERS:continue
                            header=[name.decode('ascii'),value.decode('latin1')]
                            header_bytes+=len(canonical(header))+1
                            if len(headers)>=32 or header_bytes>8192:error='header_bound';break
                            headers.append(header)
                        self._cooldown(status,headers)
                        while True:
                            chunk=await response.content.read(min(65536,self.max_response_bytes+1-len(body)))
                            if not chunk:complete=True;break
                            body.extend(chunk)
                            if len(body)>self.max_response_bytes:error='response_byte_bound';break
                except asyncio.CancelledError:
                    self._cancelled=True;error='cancelled_during_acquisition' if start is not None else 'cancelled_before_dispatch'
                except (OSError,aiohttp.ClientError,TimeoutError,ValueError,UnicodeError) as exc:error=type(exc).__name__
            response=await self._archive(bytes(body),'rest_response' if status is not None else 'control',
                dict(schema='dc.deribit-discovery/1',direction='inbound',params=PARAMS,endpoint=self.endpoint,
                    request_record_id=request.record_id,request_raw_sha256=digest(request.to_bytes()),
                    request_start=start,status=status,headers=headers,complete=complete,error=error))
            self.last_response=response
            if self._cancelled:raise asyncio.CancelledError
            if error is not None or not complete or status!=200:raise ValueError('discovery acquisition unsuccessful; raw retained')
            if any(k.lower()=='content-encoding' and v.lower()!='identity' for k,v in headers):
                raise ValueError('encoded discovery body requires an explicitly qualified decoder')
            manifest=project_instruments(response,max_instruments=self.max_instruments)
            if not strict_json(manifest)['selected']:raise ValueError('empty target discovery; previous universe retained')
            self.latest=manifest;self.last_error=None
            return manifest
        except BaseException as exc:
            self.last_error=type(exc).__name__;raise
        finally:self._busy=False

    def _cooldown(self,status,headers):
        if status!=429:return
        values=[v.strip() for k,v in headers if k.lower()=='retry-after']
        if (len(values)!=1 or not 1<=len(values[0])<=4 or not values[0].isascii() or
                not values[0].isdigit() or int(values[0])>3600):
            self.quota._fail('HTTP429 requires a qualified Retry-After cooldown')
            return
        now=self.lane._sample()[1]
        for name in self.charges:self.quota.defer_until(name,now+int(values[0])*1_000_000_000)

    def health(self):
        return dict(requests=self.requests,poisoned=self.poisoned,busy=self._busy,last_error=self.last_error,
            universe_id=None if self.latest is None else digest(self.latest),
            stale=self.latest is not None and self.last_error is not None)
