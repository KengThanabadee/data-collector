"""Follow-up for the separate public history host; <=9 requests, no retries."""
import asyncio
from datetime import datetime, timezone
import hashlib
import json
from pathlib import Path
import time

import aiohttp

ROOT=Path(__file__).resolve().parents[1]
OUTPUT=ROOT/'experiments/results/2026-10-22-research-history-host'
BASE='https://history.deribit.com/api/v2/'
DAY=86400000

def stamp(ms):return datetime.fromtimestamp(ms/1000,timezone.utc).isoformat()
def write(p,o):p.write_text(json.dumps(o,indent=2,ensure_ascii=False)+'\n')

async def run():
    OUTPUT.mkdir(parents=True,exist_ok=False)
    write(OUTPUT/'context.json',dict(base=BASE,max_requests=9,rate_per_second=1,retries=0,source_sha256=hashlib.sha256(Path(__file__).read_bytes()).hexdigest(),started_utc=stamp(time.time()*1000)))
    source=json.loads((ROOT/'experiments/results/2026-10-22-research-feasibility/summary.json').read_text())['rows']
    requests=[]
    for r in source:
        if r['method']=='public/get_last_trades_by_instrument_and_time':requests.append((r['label'],'public/get_last_trades_by_instrument_and_time',r['params']))
    for name in ('BTC-24APR26-72000-C','ETH-26JUN26-3000-C'):
        requests.append((name+'-first-sequences','public/get_last_trades_by_instrument',dict(instrument_name=name,start_seq=1,count=100,sorting='asc')))
    old=next(r for r in source if r['label']=='BTC-24APR26-72000-C-old-mark')
    requests.extend([('old-mark','public/get_mark_price_history',old['params']),('old-book','public/get_order_book',dict(instrument_name='BTC-24APR26-72000-C',depth=1))])
    rows=[];last=0
    async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=15),trust_env=False,auto_decompress=False,headers={'Accept-Encoding':'identity'}) as session:
        for i,(label,method,params) in enumerate(requests,1):
            if i>9:raise ValueError('request limit')
            await asyncio.sleep(max(0,1-(time.monotonic()-last)));last=time.monotonic()
            rec=dict(id=i,label=label,url=BASE+method,params=params,request_started_utc=stamp(time.time()*1000));body=b'';result=None
            try:
                async with session.get(BASE+method,params=params,allow_redirects=False) as response:
                    rec['http_status']=response.status
                    async for part in response.content.iter_chunked(65536):
                        if len(body)+len(part)>2*1024**2:raise ValueError('body cap')
                        body+=part
                value=json.loads(body);rec['api_error']=value.get('error');result=value.get('result');rec['ok']=rec['http_status']==200 and rec['api_error'] is None and result is not None
            except (aiohttp.ClientError,TimeoutError,ValueError) as e:rec.update(ok=False,error=type(e).__name__)
            rec.update(response_received_utc=stamp(time.time()*1000),body_sha256=hashlib.sha256(body).hexdigest(),body_bytes=len(body))
            (OUTPUT/f'{i:02d}.body').write_bytes(body);write(OUTPUT/f'{i:02d}.request.json',rec)
            row=dict(rec)
            if rec['ok'] and isinstance(result,dict) and 'trades' in result:
                trades=result['trades'];row.update(trades=len(trades),has_more=result.get('has_more'),fields=sorted({k for t in trades for k in t}),first_utc=stamp(min(t['timestamp'] for t in trades)) if trades else None,last_utc=stamp(max(t['timestamp'] for t in trades)) if trades else None,first_seq=min((t['trade_seq'] for t in trades),default=None),last_seq=max((t['trade_seq'] for t in trades),default=None))
                if label=='BTC-24APR26-72000-C-first-sequences' and trades and result.get('has_more'):
                    requests.append((label+'-next-page',method,dict(instrument_name=params['instrument_name'],start_seq=max(t['trade_seq'] for t in trades)+1,count=100,sorting='asc')))
            elif rec['ok'] and isinstance(result,list):row['points']=len(result)
            rows.append(row);write(OUTPUT/'summary.json',dict(requests=len(rows),rows=rows));print(json.dumps({k:v for k,v in row.items() if k not in ('url','params','body_sha256','fields')},ensure_ascii=False),flush=True)
            if rec.get('http_status')==429 or (rec.get('api_error') or {}).get('code')==10028:break
    write(OUTPUT/'finished.json',dict(requests=len(rows),finished_utc=stamp(time.time()*1000)))

if __name__=='__main__':asyncio.run(run())
