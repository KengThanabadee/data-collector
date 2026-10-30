"""Bounded public first-day CSV audit of an intraday BTC option hedge path.

Uses the pre-existing 08UTC cohort; never chooses contracts from later outcomes.
Downloads at most twenty public files, without retries/authentication. Raw gzip
is preserved. Analyze separately with bundled numpy and no network requests.
"""
import argparse
import bisect
from collections import Counter
import csv
from datetime import datetime, timezone
import gzip
import hashlib
import json
import math
from pathlib import Path
import time
import urllib.error
import urllib.request

ROOT = Path(__file__).resolve().parents[1]
OUT = ROOT / "experiments/results/2026-10-22-research-daily-quotes"
COHORT = ROOT / "experiments/results/2026-10-22-research-monthly-panel/2025-10-01.cohort.json"
METRICS = ROOT / "docs/research_daily_quote_metrics.json"
FILE_CAP = 32*1024**2
TOTAL_CAP = 128*1024**2
BASE_US = int(datetime(2025,10,1,tzinfo=timezone.utc).timestamp()*1e6)
DECISIONS = [BASE_US+h*3600*1000000 for h in range(8,24)]


def now():
    return datetime.now(timezone.utc).isoformat()


def write(path, value):
    path.write_text(json.dumps(value, indent=2)+"\n")


def jobs():
    cohort = json.loads(COHORT.read_text())
    assert cohort["selection_time"]*1e6 == DECISIONS[0]
    options = [s for s in cohort["symbols"] if s.startswith("BTC-17OCT25-")]
    assert len(options) == 18 and len(set(options)) == 18
    return [("options_chain", s) for s in options] + [("quotes","BTC-PERPETUAL"), ("derivative_ticker","BTC-PERPETUAL")]


def download():
    OUT.mkdir(parents=True, exist_ok=True)
    context = OUT / "context.json"
    if not context.exists():
        write(context, {"started_utc":now(),"public_unauthenticated":True,"retries":0,
            "file_cap_bytes":FILE_CAP,"total_cap_bytes":TOTAL_CAP,"max_requests":20,
            "cohort_sha256":hashlib.sha256(COHORT.read_bytes()).hexdigest(),
            "cohort_source":str(COHORT.relative_to(ROOT)),"selection_time_utc":"2025-10-01T08:00:00Z",
            "source_sha256":hashlib.sha256(Path(__file__).read_bytes()).hexdigest(),
            "purpose":"intraday input/label coverage, no model-superiority claim"})
    total = sum(p.stat().st_size for p in OUT.glob("*.csv.gz"))
    if total > TOTAL_CAP:
        raise RuntimeError("existing raw bytes exceed total cap")
    for dataset, symbol in jobs():
        name = dataset+"__"+symbol
        path, meta_path = OUT/(name+".csv.gz"), OUT/(name+".request.json")
        if meta_path.exists():
            previous = json.loads(meta_path.read_text())
            if previous.get("ok"):
                assert path.exists() and hashlib.sha256(path.read_bytes()).hexdigest()==previous["sha256"]
            print(json.dumps({"cached":name,"ok":previous.get("ok")}),flush=True)
            continue  # Failed requests are not automatically retried either.
        url=f"https://datasets.tardis.dev/v1/deribit/{dataset}/2025/10/01/{symbol}.csv.gz"
        record={"dataset":dataset,"symbol":symbol,"url":url,"method":"GET","started_utc":now()}
        started=time.monotonic()
        raw_bytes=0
        digest=hashlib.sha256()
        try:
            request=urllib.request.Request(url,headers={"User-Agent":"option-collector-research-audit/1.0"})
            with urllib.request.urlopen(request,timeout=60) as response:
                record["http_status"]=response.status
                record["content_type"]=response.headers.get("Content-Type")
                record["content_encoding"]=response.headers.get("Content-Encoding")
                with path.open("wb") as stream:
                    while True:
                        block=response.read(65536)
                        if not block:
                            break
                        if raw_bytes+len(block)>FILE_CAP or total+len(block)>TOTAL_CAP:
                            raise RuntimeError("bounded-download byte cap reached")
                        stream.write(block);digest.update(block)
                        raw_bytes+=len(block);total+=len(block)
            record.update(ok=True,raw_gzip_bytes=raw_bytes,sha256=digest.hexdigest())
        except Exception as exc:
            record.update(ok=False,error_type=type(exc).__name__,error=str(exc),raw_gzip_bytes=raw_bytes,sha256=digest.hexdigest())
            if isinstance(exc,urllib.error.HTTPError):
                record["http_status"]=exc.code
        record.update(completed_utc=now(),elapsed_seconds=time.monotonic()-started)
        write(meta_path,record)
        print(json.dumps({k:record.get(k) for k in ("dataset","symbol","ok","raw_gzip_bytes","http_status","error")}),flush=True)
        if total>=TOTAL_CAP or record.get("error")=="bounded-download byte cap reached":
            break
        time.sleep(1)
    write(OUT/"download_finished.json",{"completed_utc":now(),"raw_gzip_bytes":total})


def number(value):
    try:
        x=float(value)
        return x if math.isfinite(x) else None
    except (ValueError,TypeError):
        return None


def audit_file(meta_path):
    meta=json.loads(meta_path.read_text())
    path=meta_path.with_name(meta_path.name.replace(".request.json",".csv.gz"))
    assert meta["ok"] and hashlib.sha256(path.read_bytes()).hexdigest()==meta["sha256"]
    states=[None]*len(DECISIONS)
    n=0;negative_lag=0;out_of_order=0;gap=0;prev=None
    first_local=None;last_local=None;first_event=None;last_event=None
    empty=Counter();fields=[]
    with gzip.open(path,"rt",newline="") as stream:
        reader=csv.DictReader(stream)
        fields=reader.fieldnames
        assert "timestamp" in fields and "local_timestamp" in fields
        for row in reader:
            n+=1
            local=int(row["local_timestamp"]);event=int(row["timestamp"])
            first_local=local if first_local is None else min(first_local,local)
            last_local=local if last_local is None else max(last_local,local)
            first_event=event if first_event is None else min(first_event,event)
            last_event=event if last_event is None else max(last_event,event)
            if prev is not None:
                out_of_order+=int(local<prev)
                gap=max(gap,local-prev)
            prev=local;negative_lag+=int(event>local)
            for k in ("underlying_price","index_price","bid_price","ask_price","bid_amount","ask_amount"):
                if k in row and not row[k]:
                    empty[k]+=1
            begin=bisect.bisect_left(DECISIONS,max(local,event))
            for i in range(begin,len(DECISIONS)):
                if states[i] is None or local>=states[i]["local_timestamp"]:
                    states[i]={**row,"local_timestamp":local,"timestamp":event}
    info={"dataset":meta["dataset"],"symbol":meta["symbol"],"rows":n,"columns":fields,
        "raw_gzip_bytes":meta["raw_gzip_bytes"],"sha256":meta["sha256"],
        "first_local_us":first_local,"last_local_us":last_local,"first_event_us":first_event,"last_event_us":last_event,
        "negative_receipt_lag_rows":negative_lag,"local_time_out_of_order_rows":out_of_order,
        "max_adjacent_local_gap_seconds":gap/1e6,"empty_field_counts":dict(empty)}
    print(json.dumps({"audited":meta["symbol"],"dataset":meta["dataset"],"rows":n}),flush=True)
    return info,states


def valid_quote(row,decision,max_spread=.5):
    if row is None:
        return None,"no_prior_record"
    if row["timestamp"]>decision or row["local_timestamp"]>decision:
        raise AssertionError("future information")
    age=(decision-row["timestamp"])/1e6
    local_age=(decision-row["local_timestamp"])/1e6
    if max(age,local_age)>60:
        return None,"stale_over_60s"
    values={k:number(row.get(k)) for k in ("bid_price","ask_price","bid_amount","ask_amount")}
    if any(x is None or x<=0 for x in values.values()):
        return None,"missing_or_nonpositive_bbo"
    if values["bid_price"]>values["ask_price"]:
        return None,"crossed_bbo"
    bid,ask=values["bid_price"],values["ask_price"]
    mid=(bid+ask)/2
    spread=(ask-bid)/mid
    if spread>max_spread:
        return None,"relative_spread_over_limit"
    return {"bid":bid,"ask":ask,"mid":mid,"spread":spread,"event_age_seconds":age,"local_age_seconds":local_age,"raw":row},None


def analyze():
    from research_hedge_math import (MathDomainError,black_greeks,implied_volatility_band,
        inverse_hedge_normalized,local_quadratic_smile,smile_adjusted_delta)
    file_audits=[];states={};failures=[]
    for meta_path in sorted(OUT.glob("*.request.json")):
        meta=json.loads(meta_path.read_text())
        if not meta.get("ok"):
            failures.append(meta);continue
        info,state=audit_file(meta_path)
        file_audits.append(info);states[(meta["dataset"],meta["symbol"])]=state
    options=[s for d,s in jobs() if d=="options_chain"]
    strikes=sorted({float(s.split("-")[2]) for s in options})
    focal_strikes=set(strikes[2:-2])
    focals=[s for s in options if float(s.split("-")[2]) in focal_strikes]
    expiry=datetime(2025,10,17,8,tzinfo=timezone.utc).timestamp()
    hourly=[];option_states={};counter=Counter();paths={s:[] for s in options}
    for i,decision in enumerate(DECISIONS):
        ts=decision/1e6
        counts=Counter(selected_options=len(options),selected_focal_options=len(focals),selected_guard_options=len(options)-len(focals))
        hedge,reason=valid_quote(states.get(("quotes","BTC-PERPETUAL"),[None]*16)[i],decision,1)
        derivative=states.get(("derivative_ticker","BTC-PERPETUAL"),[None]*16)[i]
        S=number(derivative.get("index_price")) if derivative else None
        derivative_fresh=derivative and max(decision-derivative["timestamp"],decision-derivative["local_timestamp"])<=60e6
        if hedge is None or not S or S<=0 or not derivative_fresh:
            counts["missing_hedge_or_fresh_index"]+=len(options)
            hourly.append({"utc":datetime.fromtimestamp(ts,timezone.utc).isoformat(),**counts,"hedge_failure":reason})
            counter.update(counts)
            continue
        prepared=[]
        for symbol in options:
            row=states.get(("options_chain",symbol),[None]*16)[i]
            q,reason=valid_quote(row,decision)
            if q is None:
                counts[reason]+=1;continue
            counts["valid_two_sided_fresh_quotes"]+=1
            F=number(row.get("underlying_price"));K=float(symbol.split("-")[2]);put=symbol.endswith("-P")
            if not F or F<=0:
                counts["missing_forward"]+=1;continue
            tau=(expiry-ts)/(365*86400)
            try:
                band=implied_volatility_band(q["bid"]*S,q["ask"]*S,F,K,tau,"P" if put else "C")
                if band["mid_iv"]<=0:raise MathDomainError("zero IV")
            except MathDomainError:
                counts["iv_domain_failure"]+=1;continue
            prepared.append({"symbol":symbol,"F":F,"S":S,"K":K,"m":K/F,"tau":tau,"put":put,**q,**band})
        # Underlying values should agree within this expiry; preserve/report range
        # and use a common median F, reinverting IVs to avoid mixed m coordinates.
        if prepared:
            import statistics
            common_F=statistics.median(p["F"] for p in prepared)
            counts["forward_range_bps_x1000"]=round((max(p["F"] for p in prepared)-min(p["F"] for p in prepared))/common_F*1e7)
            recentered=[]
            for p in prepared:
                try:
                    band=implied_volatility_band(p["bid"]*S,p["ask"]*S,common_F,p["K"],p["tau"],"P" if p["put"] else "C")
                except MathDomainError:
                    counts["common_forward_iv_failure"]+=1;continue
                recentered.append({**p,**band,"F":common_F,"m":p["K"]/common_F})
            prepared=recentered
        points={p["K"]:p for p in prepared if p["put"]==(p["K"]<p["F"])}
        ordered=[points[k] for k in sorted(points)]
        counts["otm_strike_points"]=len(ordered)
        for p in prepared:
            option_states[(i,p["symbol"])]=p
            if p["symbol"] not in focals:
                continue
            try:
                fit=local_quadratic_smile([x["m"] for x in ordered],[x["mid_iv"] for x in ordered],
                    [x["bid_iv"] for x in ordered],[x["ask_iv"] for x in ordered],p["m"])
            except MathDomainError:
                counts["smile_support_failure"]+=1;continue
            g=black_greeks(p["F"],p["K"],p["tau"],p["mid_iv"],"P" if p["put"] else "C")
            delta=smile_adjusted_delta(g["delta_forward"],g["vega"],fit["slope"],p["m"],p["F"])
            p.update(n_sm=inverse_hedge_normalized(p["F"],delta,p["mid"]*S,S),
                n_bs=inverse_hedge_normalized(p["F"],g["delta_forward"],p["mid"]*S,S),
                H=hedge["mid"],smile_slope=fit["slope"],slope_band_contains_zero=fit["slope_lower"]<=0<=fit["slope_upper"])
            counts["smile_and_hedge_available"]+=1
            counts["slope_band_contains_zero"]+=int(p["slope_band_contains_zero"])
            paths[p["symbol"]].append(i)
        hourly.append({"utc":datetime.fromtimestamp(ts,timezone.utc).isoformat(),**counts,
            "index_S":S,"hedge_H":hedge["mid"],"index_event_age_seconds":(decision-derivative["timestamp"])/1e6})
        counter.update({k:v for k,v in counts.items() if k!="forward_range_bps_x1000"})
    pairs=[];pair_counts=Counter()
    for i in range(15):
        hedge0,reason=valid_quote(states.get(("quotes","BTC-PERPETUAL"),[None]*16)[i],DECISIONS[i],1)
        hedge1,reason=valid_quote(states.get(("quotes","BTC-PERPETUAL"),[None]*16)[i+1],DECISIONS[i+1],1)
        for symbol in focals:
            p=option_states.get((i,symbol))
            q0,_=valid_quote(states.get(("options_chain",symbol),[None]*16)[i],DECISIONS[i])
            q1,_=valid_quote(states.get(("options_chain",symbol),[None]*16)[i+1],DECISIONS[i+1],math.inf)
            if q0 is None or q1 is None:
                pair_counts["missing_valued_endpoint"]+=1;continue
            if hedge0 is None or hedge1 is None:
                pair_counts["missing_hedge_endpoint"]+=1;continue
            pair_counts["valued_same_contract_pairs"]+=1
            if p is None or "n_sm" not in p:
                pair_counts["no_entry_smile_hedge"]+=1;continue
            z=1-hedge0["mid"]/hedge1["mid"]
            dq=q1["mid"]-q0["mid"]
            pairs.append({"instrument":symbol,"entry_utc":hourly[i]["utc"],"exit_utc":hourly[i+1]["utc"],
                "q_entry_coin":q0["mid"],"q_exit_coin":q1["mid"],"exit_relative_spread":q1["spread"],
                "dq_coin":dq,"hedge_gain_z":z,"n_bs":p["n_bs"],"n_sm":p["n_sm"],
                "bs_gross_error_coin":dq-p["n_bs"]*z,"sm_gross_error_coin":dq-p["n_sm"]*z})
            pair_counts["sm_hedge_pairs"]+=1
    with gzip.open(OUT/"hourly_pairs.jsonl.gz","wt") as stream:
        for p in pairs:stream.write(json.dumps(p)+"\n")
    complete=[s for s in focals if len(paths[s])==16]
    complete_pairs=[s for s in focals if sum(p["instrument"]==s for p in pairs)==15]
    dynamic=[]
    for symbol in complete_pairs:
        path=sorted((p for p in pairs if p["instrument"]==symbol),key=lambda p:p["entry_utc"])
        dq=path[-1]["q_exit_coin"]-path[0]["q_entry_coin"]
        assert abs(sum(p["dq_coin"] for p in path)-dq)<1e-12
        dynamic.append({"instrument":symbol,"hourly_rebalances":15,"option_change_coin":dq,
            "bs_hedge_gain_coin":sum(p["n_bs"]*p["hedge_gain_z"] for p in path),
            "sm_hedge_gain_coin":sum(p["n_sm"]*p["hedge_gain_z"] for p in path),
            "bs_gross_error_coin":sum(p["bs_gross_error_coin"] for p in path),
            "sm_gross_error_coin":sum(p["sm_gross_error_coin"] for p in path)})
    metrics={"executed_utc":now(),"date":"2025-10-01","cohort_selection_utc":"2025-10-01T08:00:00Z",
        "cohort_sha256":hashlib.sha256(COHORT.read_bytes()).hexdigest(),"files_successful":len(file_audits),"files_failed":len(failures),
        "raw_gzip_bytes":sum(f["raw_gzip_bytes"] for f in file_audits),"raw_csv_rows":sum(f["rows"] for f in file_audits),
        "decision_hours_utc":list(range(8,24)),"selected_contracts":len(options),"focal_contracts":len(focals),"guard_contracts":len(options)-len(focals),"potential_option_states":18*16,"potential_focal_states":len(focals)*16,"potential_hourly_pairs":len(focals)*15,
        "counts":dict(counter),"pair_counts":dict(pair_counts),"contracts_with_sm_hedge_at_all_16_decisions":complete,
        "contracts_with_all_15_hourly_sm_pairs":complete_pairs,
        "gross_dynamic_15hour_ledgers":dynamic,
        "per_contract_sm_available_hours":{s:[8+i for i in indices] for s,indices in paths.items()},"hourly_audits":hourly,"file_audits":file_audits,"failures":failures,
        "derived_pairs_file":str((OUT/"hourly_pairs.jsonl.gz").relative_to(ROOT)),
        "caveats":["One ex-ante cohort on one first-of-month session; not continuous annual data or ML adequacy.",
        "08-23UTC decisions establish at most a 15-hour within-day path, not a 24-hour overnight path.",
        "As-of requires both event and receipt time <= decision; 60-second event/receipt freshness.",
        "Coin gross price-risk benchmark, zero discounting, SM and parallel percentage F/S/H basis assumption; no funding, fees or execution claim.",
        "Ticker freshness does not establish execution or the time since the last BBO change.",
        "Contracts with exit records are a complete-case target population; missing endpoints are counted."]}
    write(METRICS,metrics)
    print(json.dumps({k:metrics[k] for k in ("files_successful","files_failed","raw_gzip_bytes","raw_csv_rows","counts","pair_counts","contracts_with_sm_hedge_at_all_16_decisions")},indent=2),flush=True)


if __name__=="__main__":
    parser=argparse.ArgumentParser();parser.add_argument("mode",choices=["download","analyze"])
    args=parser.parse_args()
    download() if args.mode=="download" else analyze()
