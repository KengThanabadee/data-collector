"""Bounded read-only Deribit research feasibility probe; never starts collection.

At most 16 sequential requests, one request per second, no retries. Retain
response bytes and request metadata in a new ignored experiment directory.
"""
import argparse
import asyncio
import base64
from datetime import datetime, timezone
import gzip
import hashlib
import json
from pathlib import Path
import time

import aiohttp

DAY=86400000
HOUR=3600000
BASE='https://www.deribit.com/api/v2/'


def iso(ms):
    return datetime.fromtimestamp(ms/1000,timezone.utc).isoformat()


def write(path,value):
    path.write_text(json.dumps(value,ensure_ascii=False,indent=2,allow_nan=False)+'\n')


def saved_markets(root):
    markets={}
    for p in sorted((root/'data/production/snapshots').rglob('manifest.json')):
        m=json.loads(p.read_bytes());d=p.parent
        packed=(d/'responses.jsonl.gz').read_bytes()
        if hashlib.sha256(packed).hexdigest()!=m['archive_sha256']:raise ValueError('source checksum')
        records=[json.loads(line) for line in gzip.decompress(packed).splitlines()]
        if m['currency'] in markets:continue
        def result(key):
            return json.loads(next(r['raw_body'] for r in records if r['logical_request_id']==key and r['ok']))['result']
        markets[m['currency']]=(result('metadata:option'),{r['instrument_name']:r for r in result('summary:option')})
    return markets


async def run(output):
    output.mkdir(parents=True,exist_ok=False)
    script_hash=hashlib.sha256(Path(__file__).read_bytes()).hexdigest()
    write(output/'context.json',dict(purpose='hedging-research-feasibility',started_utc=iso(time.time()*1000),source_sha256=script_hash,max_requests=16,rate_per_second=1,retries=0))
    n=0;last=0;rows=[]
    async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=15),trust_env=False,auto_decompress=False,headers={'Accept-Encoding':'identity'}) as session:
        async def get(label,method,params):
            nonlocal n,last
            if n>=16:raise RuntimeError('request budget exhausted')
            await asyncio.sleep(max(0,1-(time.monotonic()-last)))
            n+=1;last=time.monotonic();start=time.time();body=b''
            rec=dict(id=n,label=label,method=method,params=params,request_started_utc=iso(start*1000))
            try:
                async with session.get(BASE+method,params={k:str(v).lower() if isinstance(v,bool) else v for k,v in params.items()},allow_redirects=False) as response:
                    rec['http_status']=response.status
                    async for chunk in response.content.iter_chunked(65536):
                        if len(body)+len(chunk)>8*1024**2:raise ValueError('body cap')
                        body+=chunk
                parsed=json.loads(body)
                rec['api_error']=parsed.get('error')
                result=parsed.get('result')
                rec['ok']=rec['http_status']==200 and rec['api_error'] is None and result is not None
            except (aiohttp.ClientError,TimeoutError,ValueError) as exc:
                rec.update(ok=False,error=type(exc).__name__);result=None
            rec.update(response_received_utc=iso(time.time()*1000),body_sha256=hashlib.sha256(body).hexdigest(),body_bytes=len(body))
            (output/f'{n:02d}.body').write_bytes(body)
            write(output/f'{n:02d}.request.json',rec)
            row=dict(rec)
            if rec['ok']:
                if method.endswith('get_tradingview_chart_data'):
                    ticks=result.get('ticks',[]);vol=result.get('volume',[])
                    row.update(status=result.get('status'),bars=len(ticks),first_utc=iso(min(ticks)) if ticks else None,last_utc=iso(max(ticks)) if ticks else None,zero_volume=sum(x==0 for x in vol),gap_hours=sum(max(0,(b-a)//HOUR-1) for a,b in zip(ticks,ticks[1:])))
                elif method.endswith('get_last_trades_by_instrument_and_time'):
                    trades=result.get('trades',[])
                    row.update(trades=len(trades),has_more=result.get('has_more'),trade_fields=sorted({k for t in trades for k in t}),first_utc=iso(min(t['timestamp'] for t in trades)) if trades else None,last_utc=iso(max(t['timestamp'] for t in trades)) if trades else None)
                elif method.endswith('get_instruments'):
                    row.update(instruments=len(result),first_expiry_utc=iso(min(r['expiration_timestamp'] for r in result)) if result else None,last_expiry_utc=iso(max(r['expiration_timestamp'] for r in result)) if result else None,states=sorted({r.get('state','unknown') for r in result}))
                elif method.endswith('get_instrument'):
                    row.update(instrument_name=result.get('instrument_name'),expiry_utc=iso(result['expiration_timestamp']),state=result.get('state'),units={k:result.get(k) for k in ('base_currency','quote_currency','counter_currency','settlement_currency','contract_size','instrument_type')})
                elif method.endswith('get_funding_rate_history'):
                    row.update(points=len(result),fields=sorted({k for r in result for k in r}),first_utc=iso(min(r['timestamp'] for r in result)) if result else None,last_utc=iso(max(r['timestamp'] for r in result)) if result else None)
                elif method.endswith('get_mark_price_history'):
                    row.update(points=len(result))
            rows.append(row);write(output/'summary.json',dict(requests=n,rows=rows))
            print(json.dumps({k:v for k,v in row.items() if k not in ('params','body_sha256','trade_fields','fields','units')},ensure_ascii=False),flush=True)
            if rec.get('http_status')==429 or (rec.get('api_error') or {}).get('code')==10028:raise RuntimeError('throttle: stop without retry')
            return result if rec['ok'] else None
        now=time.time()*1000
        for currency,(metadata,summary) in sorted(saved_markets(Path(__file__).resolve().parents[1]).items()):
            await get(currency+'-recent-expired-list','public/get_instruments',dict(currency=currency,kind='option',expired=True,extended=True))
            expired=[r for r in metadata if r['expiration_timestamp']<now]
            if expired:
                chosen=max(expired,key=lambda r:(summary.get(r['instrument_name'],{}).get('open_interest',0),r['expiration_timestamp']))
                name=chosen['instrument_name'];expiry=chosen['expiration_timestamp']
                await get(name+'-expired-hourly','public/get_tradingview_chart_data',dict(instrument_name=name,resolution='60',start_timestamp=expiry-7*DAY,end_timestamp=expiry-1))
                await get(name+'-expired-trades','public/get_last_trades_by_instrument_and_time',dict(instrument_name=name,start_timestamp=expiry-DAY,end_timestamp=expiry-1,count=100,sorting='asc'))
            start=int(datetime(2025,10,1,tzinfo=timezone.utc).timestamp()*1000)
            await get(currency+'-year-old-funding','public/get_funding_rate_history',dict(instrument_name=currency+'-PERPETUAL',start_timestamp=start,end_timestamp=start+DAY-1))
        # BTC name is the example in official trades documentation; ETH is a
        # candidate. A missing name is not evidence that all expired data fails.
        for name in ('BTC-24APR26-72000-C','ETH-26JUN26-3000-C'):
            meta=await get(name+'-old-metadata','public/get_instrument',dict(instrument_name=name))
            if not meta:continue
            expiry=meta['expiration_timestamp']
            await get(name+'-old-hourly','public/get_tradingview_chart_data',dict(instrument_name=name,resolution='60',start_timestamp=expiry-7*DAY,end_timestamp=expiry-1))
            await get(name+'-old-trades','public/get_last_trades_by_instrument_and_time',dict(instrument_name=name,start_timestamp=expiry-DAY,end_timestamp=expiry-1,count=100,sorting='asc'))
            await get(name+'-old-mark','public/get_mark_price_history',dict(instrument_name=name,start_timestamp=expiry-7*DAY,end_timestamp=expiry-1))
    write(output/'finished.json',dict(finished_utc=iso(time.time()*1000),requests=n))


if __name__=='__main__':
    p=argparse.ArgumentParser();p.add_argument('--output',type=Path,required=True)
    asyncio.run(run(p.parse_args().output))
