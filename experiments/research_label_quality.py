"""Inspect label uncertainty and a causal OTM population, without new requests.

OTM is defined at ENTRY, not from future liquidity. The subset assessment is an
exploratory design iteration: the original test dates have already been viewed.
"""
from collections import Counter, defaultdict
from datetime import datetime, timezone
import gzip
import hashlib
import json
from pathlib import Path
import numpy as np
from research_panel_analysis import FEATURES, train_and_assess

ROOT=Path(__file__).resolve().parents[1]
DATA=ROOT/'experiments/results/2026-10-22-research-monthly-panel'


def stats(rows):
    intervals=defaultdict(set)
    counts=Counter(r['entry_utc'] for r in rows)
    for r in rows:
        intervals[r['date']].add(r['entry_utc'])
    total=wide=0.0
    for r in rows:
        w=1/(len(intervals[r['date']])*counts[r['entry_utc']])
        loss=(r['dq']-r['n_bs']*r['hedge_gain_z'])**2
        total+=w*loss
        wide+=w*loss*(r['exit_relative_spread']>.5)
    return {'rows':len(rows),'dates':len(intervals),'market_intervals':len(counts),
        'wide_exits_above_50pct':sum(r['exit_relative_spread']>.5 for r in rows),
        'share_bs_squared_error_from_wide_exits':wide/total,
        'slope_band_contains_zero':sum(r['slope_band_contains_zero'] for r in rows),
        'feature_missing_values':sum(not np.isfinite(r[k]) for r in rows for k in FEATURES)}


def run():
    rows=[json.loads(line) for line in gzip.open(DATA/'model_rows.jsonl.gz','rt')]
    otm=[r for r in rows if bool(r['put'])==(r['log_m']<0)]
    itm=[r for r in rows if bool(r['put'])!=(r['log_m']<0)]
    # Compare raw observed mark changes against mid changes; never substitute
    # marks silently or call their agreement proof of executable prices.
    states={}
    for p in DATA.glob('*.btc-state.json.gz'):
        state=json.loads(gzip.decompress(p.read_bytes()))
        states[state['decision']]=state
    differences=[]
    for r in rows:
        t=datetime.fromisoformat(r['entry_utc']).timestamp()
        before=states[t]['quotes'][r['instrument']]['data']
        after=states[t+3600]['quotes'][r['instrument']]['data']
        a,b=before.get('mark_price'),after.get('mark_price')
        if a is not None and b is not None and a>0 and b>0:
            differences.append({'instrument':r['instrument'],'entry_utc':r['entry_utc'],
                'mid_change':r['dq'],'mark_change':b-a,'difference':r['dq']-(b-a),
                'wide_exit':r['exit_relative_spread']>.5})
    m={'executed_utc':datetime.now(timezone.utc).isoformat(),
        'input_rows_sha256':hashlib.sha256((DATA/'model_rows.jsonl.gz').read_bytes()).hexdigest(),
        'all':stats(rows),'entry_otm':stats(otm),'entry_itm':stats(itm),
        'marks_present_at_both_endpoints':len(differences),
        'largest_mid_minus_mark_change_differences':sorted(differences,key=lambda x:abs(x['difference']),reverse=True)[:5],
        'entry_otm_model_assessment_exploratory':train_and_assess(otm),
        'caveats':['The OTM rule uses entry moneyness, not exit liquidity; outcome availability is still complete-case.',
        'OTM design was investigated after inspecting the broad panel; these model scores are exploratory, not a newly untouched final test.',
        'Marks define a different valuation process and can be circular with exchange-model IV; no automatic mark replacement was made.',
        'Broad-panel midpoint model scores are materially exposed to very wide future quotes; fitting successfully does not certify their labels.',
        'All outcomes remain gross coin risk under declared inverse-hedge basis assumptions, without funding/fees or execution guarantees.']}
    with gzip.open(DATA/'entry_otm_rows.jsonl.gz','wt') as stream:
        for r in otm: stream.write(json.dumps(r)+'\n')
    out=ROOT/'docs/research_label_quality_metrics.json'
    out.write_text(json.dumps(m,indent=2)+'\n')
    print(json.dumps({k:v for k,v in m.items() if k not in ('entry_otm_model_assessment_exploratory','largest_mid_minus_mark_change_differences')},indent=2))
    print(json.dumps(m['entry_otm_model_assessment_exploratory']['partitions'],indent=2))


if __name__=='__main__':run()
