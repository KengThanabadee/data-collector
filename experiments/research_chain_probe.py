"""Four bounded historical option-trade samples; no paging or retries."""
import asyncio
from datetime import datetime,timezone
import hashlib
import json
from pathlib import Path
import time
import aiohttp

ROOT=Path(__file__).resolve().parents[1]
OUT=ROOT/'experiments/results/2026-10-22-research-trade-chain'
BASE='https://history.deribit.com/api/v2/'
def utc():return datetime.now(timezone.utc).isoformat()
def write(p,v):p.write_text(json.dumps(v,indent=2)+'\n')
async def run():
    OUT.mkdir(parents=True,exist_ok=False)
    write(OUT/'context.json',dict(started_utc=utc(),max_requests=4,rate_per_second=1,source_sha256=hashlib.sha256(Path(__file__).read_bytes()).hexdigest()))
    rows=[];last=0
    async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=15),trust_env=False,headers={'Accept-Encoding':'identity'}) as client:
        for day in ('2025-10-01','2026-04-17'):
            start=int(datetime.fromisoformat(day+'T12:00:00+00:00').timestamp()*1000)
            for currency in ('BTC','ETH'):
                await asyncio.sleep(max(0,1-(time.monotonic()-last)));last=time.monotonic();i=len(rows)+1
                params=dict(currency=currency,kind='option',start_timestamp=start,end_timestamp=start+3600000-1,count=1000,sorting='asc')
                rec=dict(id=i,method='public/get_last_trades_by_currency_and_time',params=params,request_started_utc=utc());body=b''
                try:
                    async with client.get(BASE+rec['method'],params=params,allow_redirects=False) as response:
                        rec['http_status']=response.status
                        async for part in response.content.iter_chunked(65536):
                            if len(body)+len(part)>2*1024**2:raise ValueError('body cap')
                            body+=part
                    value=json.loads(body);rec['api_error']=value.get('error');rec['ok']=response.status==200 and rec['api_error'] is None
                    if rec['ok']:
                        result=value['result'];trades=result['trades'];names={t['instrument_name'] for t in trades};groups={}
                        for name in names:
                            parts=name.split('-');groups.setdefault(parts[1],set()).add(parts[2])
                        rec.update(trades=len(trades),has_more=result.get('has_more'),instruments=len(names),expiries=len(groups),distinct_strikes_per_expiry={k:len(v) for k,v in groups.items()},fields=sorted({k for t in trades for k in t}),first_trade_utc=datetime.fromtimestamp(min(t['timestamp'] for t in trades)/1000,timezone.utc).isoformat() if trades else None,last_trade_utc=datetime.fromtimestamp(max(t['timestamp'] for t in trades)/1000,timezone.utc).isoformat() if trades else None)
                except (aiohttp.ClientError,TimeoutError,ValueError) as exc:rec.update(ok=False,error=type(exc).__name__)
                rec.update(response_received_utc=utc(),body_bytes=len(body),body_sha256=hashlib.sha256(body).hexdigest())
                (OUT/f'{i:02d}.body').write_bytes(body);write(OUT/f'{i:02d}.request.json',rec);rows.append(rec);write(OUT/'summary.json',dict(requests=len(rows),rows=rows));print(json.dumps(rec),flush=True)
                if rec.get('http_status')==429 or (rec.get('api_error') or {}).get('code')==10028:raise RuntimeError('stop on throttle')
    write(OUT/'finished.json',dict(requests=len(rows),finished_utc=utc()))
if __name__=='__main__':asyncio.run(run())
