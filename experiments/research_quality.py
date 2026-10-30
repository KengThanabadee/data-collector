"""Offline quality checks for the saved hourly research evidence. No networking."""
from collections import defaultdict
from datetime import datetime, timezone
import gzip
import hashlib
import json
from pathlib import Path

HOUR=3600000
FIELDS=('open','high','low','close','volume','cost')

def sha(data):return hashlib.sha256(data).hexdigest()
def iso(ms):return datetime.fromtimestamp(ms/1000,timezone.utc).isoformat()

def analyze(root):
    folder=root/'experiments/results/2026-10-19-history/raw'
    manifest=json.loads((folder/'manifest.json').read_bytes())
    packed=(folder/'responses.jsonl.gz').read_bytes();raw=gzip.decompress(packed)
    assert sha(packed)==manifest['archive_sha256']
    assert sha(raw)==manifest['journal_sha256']
    records=[json.loads(x) for x in raw.splitlines()]
    assert len(records)==manifest['records']==137
    by_symbol=defaultdict(dict);wide={};conflicts=defaultdict(int);old_rows=[]
    for r in records:
        assert sha(r['raw_body'].encode())==r['body_sha256']
        if r['method']!='public/get_tradingview_chart_data' or r['params']['resolution']!='60':continue
        result=json.loads(r['raw_body'])['result'];ticks=result.get('ticks',[])
        assert all(len(result.get(k,[]))==len(ticks) for k in FIELDS)
        name=r['params']['instrument_name'];bars=[]
        for i,t in enumerate(ticks):
            value={k:result[k][i] for k in FIELDS};value['timestamp']=t
            assert t%HOUR==0
            assert value['volume']>=0
            assert value['low']<=min(value['open'],value['close'])<=max(value['open'],value['close'])<=value['high']
            existing=by_symbol[name].get(t)
            if existing is not None and existing!=value:conflicts[name]+=1
            by_symbol[name][t]=value;bars.append(value)
        if ticks and r['params']['end_timestamp']==1792281599999:wide[name]=bars
        old_rows.append(dict(record_id=r['record_id'],instrument=name,bars=len(ticks),first_utc=iso(min(ticks)) if ticks else None,last_utc=iso(max(ticks)) if ticks else None))
    profiles=[]
    for name,bars in wide.items():
        count=len(bars);zeros=sum(x['volume']==0 for x in bars)
        longest=streak=0
        for b in bars:
            streak=streak+1 if b['volume']==0 else 0;longest=max(longest,streak)
        flat_zero=sum(b['volume']==0 and b['open']==b['high']==b['low']==b['close'] for b in bars)
        profile=dict(instrument=name,bars=count,first_utc=iso(bars[0]['timestamp']),last_utc=iso(bars[-1]['timestamp']),zero_volume=zeros,zero_volume_fraction=zeros/count,flat_zero=flat_zero,longest_zero_volume_hours=longest,duplicate_timestamps=count-len({b['timestamp'] for b in bars}),overlap_value_conflicts=conflicts[name])
        if 'PERPETUAL' not in name:
            options={b['timestamp']:b for b in bars};hedge=by_symbol[name[:3]+'-PERPETUAL']
            aligned=[t for t in options if t in hedge]
            starts=[t for t in options if t+24*HOUR in options and t in hedge and t+24*HOUR in hedge]
            active=[t for t in starts if options[t]['volume']>0 and options[t+24*HOUR]['volume']>0]
            # Strict diagnostic only: every hourly option bar has trades. This
            # does NOT establish an executable quote or a necessary research rule.
            all_active=[t for t in starts if all(options.get(t+k*HOUR,{}).get('volume',0)>0 for k in range(25))]
            profile.update(perpetual_timestamp_matches=len(aligned),holding_24h_candidates=len(starts),holding_24h_positive_volume_endpoints=len(active),holding_24h_all_25_bars_positive_volume=len(all_active))
        profiles.append(profile)
    probes={}
    for folder_name in ('2026-10-22-research-feasibility','2026-10-22-research-history-host','2026-10-22-research-trade-chain'):
        p=root/'experiments/results'/folder_name
        if not (p/'finished.json').exists():raise ValueError('probe not finished')
        data=json.loads((p/'summary.json').read_bytes())
        for row in data['rows']:
            body=(p/f'{row["id"]:02d}.body').read_bytes()
            assert sha(body)==row['body_sha256']
        probes[folder_name]=data
    chain_rows=[]
    chain_folder=root/'experiments/results/2026-10-22-research-trade-chain'
    for r in probes['2026-10-22-research-trade-chain']['rows']:
        if not r['ok']:continue
        trades=json.loads((chain_folder/f'{r["id"]:02d}.body').read_bytes())['result']['trades']
        assert all(r['params']['start_timestamp']<=t['timestamp']<=r['params']['end_timestamp'] for t in trades)
        keys=[(t['instrument_name'],t['trade_id']) for t in trades]
        assert len(keys)==len(set(keys))
        last={}
        for t in trades:
            key=t['instrument_name']
            if key not in last or t['timestamp']>last[key]['timestamp']:last[key]=t
        ages=[(r['params']['end_timestamp']+1-t['timestamp'])/60000 for t in last.values()]
        chain_rows.append(dict(currency=r['params']['currency'],start_utc=iso(r['params']['start_timestamp']),end_utc=iso(r['params']['end_timestamp']),trades=len(trades),instruments=len(last),expiries=r['expiries'],has_more=r['has_more'],missing_iv=sum(t.get('iv') is None for t in trades),missing_mark=sum(t.get('mark_price') is None for t in trades),missing_index=sum(t.get('index_price') is None for t in trades),last_trade_older_than_5min=sum(a>5 for a in ages),max_last_trade_age_minutes=max(ages,default=None),duplicate_trade_keys=len(keys)-len(set(keys))))
    page_folder=root/'experiments/results/2026-10-22-research-history-host'
    first=json.loads((page_folder/'05.body').read_bytes())['result']['trades']
    second=json.loads((page_folder/'09.body').read_bytes())['result']['trades']
    seq=[t['trade_seq'] for t in first+second]
    assert sorted(seq)==list(range(1,201))
    return dict(chain_hour_profiles=chain_rows,pagination_checked_sequences=[1,200],pagination_response_sequence_sorted=(seq==sorted(seq)),executed_utc=iso(__import__('time').time()*1000),assumptions=dict(currencies=['BTC','ETH'],rebalance_hours=1,holding_hours=24,desired_history_months=12,assumptions_are_not_final_research_requirements=True),old_raw_records=137,old_raw_archive_sha256=manifest['archive_sha256'],hourly_profiles=profiles,hourly_requests=old_rows,live_probes=probes,caveats=['Samples are purposive, not a representative universe.','Perpetual timestamp matches are candle-grid matches, not synchronized fresh quotes.','A positive-volume hour does not establish a quote at the rebalance time.','24-hour candidate starts overlap and are not independent observations.','Recent-wide samples contain only one option per currency and cannot estimate a smile.','No research model has been trained or evaluated.'])

if __name__=='__main__':
    root=Path(__file__).resolve().parents[1];result=analyze(root)
    (root/'docs/research_feasibility_metrics.json').write_text(json.dumps(result,ensure_ascii=False,indent=2)+'\n')
    for row in result['hourly_profiles']:print(json.dumps(row))
