"""Public Binance discovery, bound to exact raw request/response evidence."""
import asyncio
import os
import uuid

import aiohttp

from .contracts import canonical, digest, integer, strict_json
from .discovery import _native_values
from .journal import _publish_bytes
from .rawrun import RawOwner, verified_records
from .segments import _mkdir

ENDPOINTS={
    'spot':('https://api.binance.com/api/v3/exchangeInfo',{'showPermissionSets':'false'}),
    'um':('https://fapi.binance.com/fapi/v1/exchangeInfo',{}),
    'cm':('https://dapi.binance.com/dapi/v1/exchangeInfo',{}),
}
HEADERS={'content-type','content-encoding','content-length','date','retry-after',
         'x-mbx-used-weight','x-mbx-used-weight-1m'}
DATED={'CURRENT_QUARTER','NEXT_QUARTER','CURRENT_MONTH','NEXT_MONTH'}


async def fetch(owner, session, family):
    url,params=ENDPOINTS[family]
    request=await owner.save(canonical(dict(method='GET',endpoint=url,params=params)),
        family+':exchange_info','control',product='discovery',fields=dict(
            schema='dc.binance-http/1',family=family,direction='outbound',dispatched=False))
    fields=dict(schema='dc.binance-http/1',family=family,direction='inbound',endpoint=url,
        params=params,request_record_id=request.record_id,request_raw_sha256=digest(request.to_bytes()),
        status=None,headers=[],complete=False,error=None,body_decoded=True)
    utc,mono,_=owner.observer();fields['request_start']=dict(utc_ns=utc,monotonic_ns=mono)
    body=bytearray()
    try:
        if getattr(session,'_retry_connection',True) is not False:
            raise ValueError('disable implicit HTTP retries')
        async with asyncio.timeout(owner.config.timeout_seconds):
            async with session.get(url,params=params) as response:
                fields['status']=response.status
                fields['headers']=[[k.decode('latin1'),v.decode('latin1')] for k,v in response.raw_headers
                    if k.decode('latin1').lower() in HEADERS]
                while True:
                    chunk=await response.content.read(min(65536,owner.config.max_response_bytes-len(body)+1))
                    if not chunk:break
                    body.extend(chunk)
                    if len(body)>owner.config.max_response_bytes:raise ValueError('HTTP response byte bound')
                fields['complete']=True
    except (aiohttp.ClientError,OSError,TimeoutError,ValueError) as exc:
        fields['error']=type(exc).__name__+': '+str(exc)[:512]
    # aiohttp transport errors are separate from archive admission errors.
    # The public owner always writes the observed prefix before interpretation.
    utc,mono,_=owner.observer();fields['request_end']=dict(utc_ns=utc,monotonic_ns=mono)
    return await owner.save(bytes(body),family+':exchange_info','rest_response',product='discovery',fields=fields)


def project(responses, *, environment, max_instruments):
    rows_out=[];excluded=[];sources={};families={};observed=0;identities=set()
    for response in responses:
        fields=strict_json(response.native_fields_json);family=fields.get('family')
        if family not in ENDPOINTS or family in sources:
            raise ValueError('one response for each Binance family required')
        url,params=ENDPOINTS[family]
        if (response.venue!='binance' or response.environment!=environment or response.kind!='rest_response'
                or response.channel!=family+':exchange_info' or fields.get('schema')!='dc.binance-http/1'
                or fields.get('endpoint')!=url or fields.get('params')!=params
                or fields.get('status')!=200 or fields.get('complete') is not True or fields.get('error') is not None):
            raise ValueError('complete successful Binance discovery response required')
        value=strict_json(response.body)
        if not isinstance(value,dict) or not isinstance(value.get('symbols'),list) or 'code' in value:
            raise ValueError('native Binance exchangeInfo symbols required')
        rows=value['symbols'];observed+=len(rows)
        if observed>max_instruments:raise ValueError('instrument count bound; no subset selected')
        source=dict(record_id=response.record_id,raw_sha256=digest(response.to_bytes()))
        sources[family]=source;counts=dict(observed=len(rows),selected=0,feed_enabled=0,excluded=0)
        families[family]=counts
        for i,row in enumerate(rows):
            if not isinstance(row,dict):raise ValueError('native instrument object required')
            symbol=row.get('symbol');base=row.get('baseAsset')
            if not isinstance(symbol,str) or not symbol or not isinstance(base,str):
                raise ValueError('native instrument identity required')
            identity=(family,symbol)
            if identity in identities:raise ValueError('duplicate Binance instrument identity')
            identities.add(identity)
            if base not in ('BTC','ETH'):
                excluded.append(dict(family=family,native_symbol=symbol,reason='outside_BTC_ETH_base_scope'))
                counts['excluded']+=1;continue
            status=row.get('status') if family!='cm' else row.get('contractStatus')
            if not isinstance(status,str) or not status:raise ValueError('native instrument status required')
            incarnation=None;product='spot'
            if family!='spot':
                kind=row.get('contractType')
                if kind!='PERPETUAL' and kind not in DATED:raise ValueError('unverified native future contract type')
                integer(row.get('onboardDate'),'native onboard time',1)
                incarnation='onboard:'+str(row['onboardDate'])
                product='perpetual' if kind=='PERPETUAL' else 'future'
            enabled=status=='TRADING' and (family!='spot' or row.get('isSpotTradingAllowed') is True)
            reason=None if enabled else 'native_status:'+status if status!='TRADING' else 'spot_trading_disabled'
            rows_out.append(dict(family=family,instrument=dict(venue='binance',environment=environment,
                native_product=product,native_symbol=symbol,listing_incarnation=incarnation),
                native=_native_values(row),source=dict(**source,row_index=i),
                feed_enabled=enabled,disabled_reason=reason))
            counts['selected']+=1;counts['feed_enabled']+=int(enabled)
    if set(sources)!=set(ENDPOINTS):raise ValueError('Binance discovery family missing')
    if not rows_out:raise ValueError('empty BTC/ETH Binance universe')
    return canonical(dict(schema='dc.binance-universe/1',environment=environment,sources=sources,
        selected=rows_out,excluded=excluded,families=families))


async def run(config, *, discover_only, session_factory, observer, clock_read=None, clock_wait=None):
    from .app import _bind_root, _drain
    if not discover_only:
        from .binance_capture import capture
        return await capture(config,session_factory=session_factory,observer=observer,
            clock_read=clock_read,clock_wait=clock_wait)
    lock=_bind_root(config);root=config.data_root/'runs'/uuid.uuid4().hex
    owner=RawOwner(root,config,observer,clock_read=clock_read,clock_wait=clock_wait)
    stopped=False
    report=dict(schema='dc.public-discovery/1',venue='binance',environment=config.environment,
        status='failed',segments=[],responses=[],max_instruments=config.max_instruments)
    async def own():
        records=[]
        try:
            await owner.start()
            async with session_factory() as session:
                for family in ENDPOINTS:
                    if stopped:break
                    response=await fetch(owner,session,family);records.append(response)
                    report['responses'].append(response.record_id)
                    # A failed/throttled response remains raw evidence. Do not
                    # make additional requests while the venue is rejecting us.
                    fields=strict_json(response.native_fields_json)
                    if fields['status']!=200 or not fields['complete'] or fields['error'] is not None:
                        raise ValueError('Binance discovery acquisition failed: '+family)
                    value=strict_json(response.body)
                    if not isinstance(value,dict) or 'code' in value or not isinstance(value.get('symbols'),list):
                        raise ValueError('Binance discovery API error or malformed response: '+family)
                    if family!='cm':await asyncio.sleep(1)
            if stopped:raise ValueError('discovery stopped before completion')
            universe=project(records,environment=config.environment,max_instruments=config.max_instruments)
            _publish_bytes(root/'universe.json',universe)
            report['universe_sha256']=digest(universe)
            report['families']=strict_json(universe)['families']
            report['status']='discovery_complete'
        except Exception as exc:
            report['error']=type(exc).__name__+': '+str(exc)
        finally:
            try:await owner.close()
            except Exception as exc:
                report['status']='failed';report['error']=type(exc).__name__+': '+str(exc)
            if hasattr(owner,'provenance'):report.update(owner.descriptor())
    task=asyncio.create_task(own())
    try:
        try:await asyncio.shield(task)
        except asyncio.CancelledError:
            stopped=True
            await _drain(task)
    finally:
        try:
            if root.exists():_publish_bytes(root/'result.json',canonical(report))
        finally:os.close(lock)
    return dict(root=str(root),**report)


def verify(root, report, *, export=None):
    from pathlib import Path
    root=Path(root);responses=[];requests={};count=0
    if 'provenance' not in report:raise ValueError('no raw owner established; inspect the failed discovery result')
    output=None if export is None else Path(export).open('xb')
    try:
        for record in verified_records(root,report):
            count+=1
            if output is not None:output.write(record.to_bytes()+b'\n')
            if record.native_product!='discovery':continue
            fields=strict_json(record.native_fields_json)
            if fields.get('direction')=='outbound':requests[record.record_id]=record
            elif record.kind=='rest_response':
                request=requests.pop(fields.get('request_record_id'),None)
                if request is None or digest(request.to_bytes())!=fields.get('request_raw_sha256'):
                    raise ValueError('HTTP request proof missing or differs')
                url,params=ENDPOINTS[fields['family']]
                if strict_json(request.body)!=dict(method='GET',endpoint=url,params=params):
                    raise ValueError('HTTP request identity differs')
                responses.append(record)
        if [r.record_id for r in responses]!=report['responses']:raise ValueError('discovery response list differs')
        projected=False
        if report['status']=='discovery_complete':
            body=project(responses,environment=report['environment'],max_instruments=report['max_instruments'])
            if body!=(root/'universe.json').read_bytes() or digest(body)!=report['universe_sha256']:
                raise ValueError('discovery projection differs from raw')
            projected=True
        return dict(archive_verified=True,records=count,raw_responses=len(responses),
            universe_verified=projected,capture_status=report['status'])
    finally:
        if output is not None:output.close()
