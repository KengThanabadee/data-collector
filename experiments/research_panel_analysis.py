"""Actual BS/SM inputs and residual-ML fitability on saved sampled sessions.

Illustrative zero-discounting SM, one-hour gross BTC risk, parallel percentage
basis assumption. This does not freeze the user's final research specification.
Requires bundled numpy; no network, scipy, sklearn or production dependencies.
"""
import argparse
from collections import Counter, defaultdict
from datetime import datetime, timezone
import gzip
import json
import math
from pathlib import Path
import statistics
import numpy as np
from research_panel_states import contract, extract
from research_hedge_math import (MathDomainError, black_greeks, fit_residual_ridge,
    implied_volatility_band, inverse_hedge_normalized, local_quadratic_smile,
    smile_adjusted_delta)

ROOT = Path(__file__).resolve().parents[1]
FEATURES = ["put", "log_m", "sqrt_tau", "mid_iv", "delta_bs", "vega_coin", "smile_slope", "relative_spread", "hedge_index_basis"]


def positive(value):
    return isinstance(value, (int, float)) and math.isfinite(value) and value > 0


def quote(item, decision, max_age, max_spread):
    if item is None:
        return None, "no_ticker_in_minute"
    d = item["data"]
    if not (item["receipt"] <= decision and item["event"] <= decision):
        return None, "future_timestamp"
    if decision - item["event"] > max_age:
        return None, "stale_ticker"
    if not all(positive(d.get(k)) for k in ("best_bid_price", "best_ask_price", "best_bid_amount", "best_ask_amount")):
        return None, "missing_or_nonpositive_bbo"
    bid, ask = d["best_bid_price"], d["best_ask_price"]
    if bid > ask:
        return None, "crossed_bbo"
    mid = (bid + ask) / 2
    spread = (ask - bid) / mid
    if spread > max_spread:
        return None, "wide_spread"
    return {"bid": bid, "ask": ask, "mid": mid, "spread": spread, "data": d,
        "age": decision-item["event"]}, None


def build(directory, max_age=60, max_spread=.5):
    states = extract(directory)
    by_time = {s["decision"]: s for s in states}
    cohorts = {p.name[:10]: json.loads(p.read_text())["symbols"] for p in directory.glob("*.cohort.json")}
    rows, summaries = [], []
    for entry in states:
        timestamp = entry["decision"]
        dt = datetime.fromtimestamp(timestamp, timezone.utc)
        if dt.hour not in (8, 16) or dt.minute != 0:
            continue
        exit_state = by_time.get(timestamp + 3600)
        if exit_state is None:
            continue
        day = dt.date().isoformat()
        if day not in cohorts:
            raise ValueError("missing predeclared past-only cohort for " + day)
        selected = cohorts[day]
        # The four outer strikes are guards for interpolation, not failed focal
        # observations: five-point fits cannot bracket an extreme guard strike.
        selected_strikes = defaultdict(set)
        for name in selected:
            c = contract(name)
            if c:
                selected_strikes[c["expiry"]].add(c["strike"])
        focal_strikes = {expiry: set(sorted(strikes)[2:-2]) for expiry,strikes in selected_strikes.items()}
        counts = Counter(selected_options=sum(contract(n) is not None for n in selected))
        counts["selected_focal_options"] = sum((c:=contract(n)) is not None and c["strike"] in focal_strikes[c["expiry"]] for n in selected)
        counts["selected_guard_options"] = counts["selected_options"]-counts["selected_focal_options"]
        hedge0, reason0 = quote(entry["quotes"].get("BTC-PERPETUAL"), timestamp, max_age, 1)
        hedge1, reason1 = quote(exit_state["quotes"].get("BTC-PERPETUAL"), timestamp+3600, max_age, 1)
        if hedge0 is None or hedge1 is None or not positive((hedge0 or {}).get("data", {}).get("index_price")):
            counts["missing_hedge_or_index"] += counts["selected_options"]
            summaries.append({"date": day, "entry_utc": dt.isoformat(), **counts})
            continue
        S, H0, H1 = hedge0["data"]["index_price"], hedge0["mid"], hedge1["mid"]
        groups = defaultdict(list)
        for name in selected:
            c = contract(name)
            if not c:
                continue
            q, reason = quote(entry["quotes"].get(name), timestamp, max_age, max_spread)
            if q is None:
                counts["entry_"+reason] += 1
                continue
            if not positive(q["data"].get("underlying_price")):
                counts["missing_forward"] += 1
                continue
            tau = (c["expiry"] - timestamp) / (365*86400)
            if not 7 <= tau*365 <= 45:
                counts["outside_maturity"] += 1
                continue
            groups[c["expiry"]].append({"name": name, **c, **q, "tau": tau})
        for expiry, items in groups.items():
            F = statistics.median(x["data"]["underlying_price"] for x in items)
            prepared = []
            for item in items:
                try:
                    band = implied_volatility_band(item["bid"]*S, item["ask"]*S, F, item["strike"], item["tau"], "P" if item["put"] else "C")
                    if band["mid_iv"] <= 0:
                        raise MathDomainError("zero IV")
                except MathDomainError:
                    counts["iv_model_bound_failure"] += 1
                    continue
                prepared.append({**item, **band, "m": item["strike"] / F})
            # One OTM quote per strike, all available before the entry decision.
            smile = {}
            for item in prepared:
                if item["put"] == (item["strike"] < F):
                    smile[item["strike"]] = item
            points = sorted(smile.values(), key=lambda x: x["strike"])
            for item in prepared:
                if item["strike"] not in focal_strikes[expiry]:
                    counts["guard_options_not_targets"] += 1
                    continue
                if not .9 <= item["m"] <= 1.1:
                    counts["outside_focal_moneyness"] += 1
                    continue
                counts["focal_entry_candidates"] += 1
                try:
                    fit = local_quadratic_smile([p["m"] for p in points], [p["mid_iv"] for p in points], [p["bid_iv"] for p in points], [p["ask_iv"] for p in points], item["m"])
                except MathDomainError:
                    counts["smile_support_failure"] += 1
                    continue
                counts["fitted_entry_smiles"] += 1
                # Selection and smile fitting above do not inspect the exit.
                # Gross midpoint-risk valuation must not drop an entry merely
                # because its spread later widens. Report exit spread instead.
                q1, exit_reason = quote(exit_state["quotes"].get(item["name"]), timestamp+3600, max_age, math.inf)
                if q1 is None:
                    counts["exit_"+exit_reason] += 1
                    continue
                kind = "P" if item["put"] else "C"
                greeks = black_greeks(F, item["strike"], item["tau"], item["mid_iv"], kind)
                delta = greeks["delta_forward"]
                sm = smile_adjusted_delta(delta, greeks["vega"], fit["slope"], item["m"], F)
                B = item["mid"] * S
                n_bs = inverse_hedge_normalized(F, delta, B, S)
                n_sm = inverse_hedge_normalized(F, sm, B, S)
                dq = q1["mid"] - item["mid"]
                z = 1-H0/H1
                slope_unidentified = fit["slope_lower"] <= 0 <= fit["slope_upper"]
                data = {"date": day, "entry_utc": dt.isoformat(), "instrument": item["name"], "expiry_utc": datetime.fromtimestamp(expiry,timezone.utc).isoformat(),
                    "q_entry": item["mid"], "q_exit": q1["mid"], "index_S": S, "pricing_forward_F": F, "hedge_H_entry": H0, "hedge_H_exit": H1,
                    "put": int(item["put"]), "log_m": math.log(item["m"]), "sqrt_tau": math.sqrt(item["tau"]), "mid_iv": item["mid_iv"], "delta_bs": delta,
                    "vega_coin": greeks["vega"]/S, "smile_slope": fit["slope"], "relative_spread": item["spread"], "hedge_index_basis": H0/S-1,
                    "n_bs": n_bs, "n_sm": n_sm, "dq": dq, "hedge_gain_z": z, "entry_age_seconds": item["age"], "exit_age_seconds": q1["age"], "exit_relative_spread": q1["spread"],
                    "smile_slope_lower": fit["slope_lower"], "smile_slope_upper": fit["slope_upper"], "slope_band_contains_zero": bool(slope_unidentified),
                    "n_sm_quote_band_lower": n_bs-greeks["vega"]*item["m"]/S*fit["slope_upper"], "n_sm_quote_band_upper": n_bs-greeks["vega"]*item["m"]/S*fit["slope_lower"],
                    "smile_rmse": fit["rmse"], "smile_condition_number": fit["condition_number"], "bs_price_fit_error_usd": greeks["price"]-B,
                    "entry_raw_sha256": entry["source_sha256"], "exit_raw_sha256": exit_state["source_sha256"]}
                assert all(math.isfinite(data[k]) for k in FEATURES+["dq","hedge_gain_z","n_bs","n_sm"])
                rows.append(data)
                counts["usable_pairs"] += 1
                counts["slope_band_contains_zero"] += int(slope_unidentified)
        summaries.append({"date": day, "entry_utc": dt.isoformat(), **counts})
    return rows, summaries


def date_losses(rows, errors):
    groups = defaultdict(lambda: defaultdict(list))
    for row, error in zip(rows, errors):
        groups[row["date"]][row["entry_utc"]].append(float(error)**2)
    return {day: float(np.mean([np.mean(v) for v in intervals.values()])) for day, intervals in sorted(groups.items())}


def arrays(rows):
    f = np.array([[r[k] for k in FEATURES] for r in rows])
    dq = np.array([r["dq"] for r in rows])
    z = np.array([r["hedge_gain_z"] for r in rows])
    base = np.array([r["n_sm"] for r in rows])
    counts = Counter(r["entry_utc"] for r in rows)
    intervals = defaultdict(set)
    for row in rows:
        intervals[row["date"]].add(row["entry_utc"])
    weights = np.array([1/(len(intervals[r["date"]])*counts[r["entry_utc"]]) for r in rows])
    return f, dq, z, base, weights


def train_and_assess(rows):
    partitions = {"train": [r for r in rows if r["date"] < "2024-01-01"], "validation": [r for r in rows if "2024-01-01" <= r["date"] < "2025-01-01"], "test": [r for r in rows if r["date"] >= "2025-01-01"]}
    sizes = {k: {"rows": len(v), "dates": len({r["date"] for r in v}), "market_intervals": len({r["entry_utc"] for r in v})} for k,v in partitions.items()}
    if any(sizes[k]["dates"] < 2 for k in sizes):
        return {"status": "insufficient_calendar_partitions", "partitions": sizes}
    train, val, test = (partitions[k] for k in ("train", "validation", "test"))
    x, y, z, base, w = arrays(train)
    xv, yv, zv, bv, _ = arrays(val)
    alphas = (1e-10,1e-9,1e-8,1e-7,1e-6,1e-5,1e-4)
    candidates = []
    for alpha in alphas:
        model = fit_residual_ridge(x, y-base*z, z, alpha=alpha, sample_weight=w)
        losses = date_losses(val, yv-(bv+model.predict(xv))*zv)
        candidates.append({"alpha": alpha, "validation_date_mean_mse": float(np.mean(list(losses.values())))})
    chosen = min(candidates, key=lambda c: c["validation_date_mean_mse"])["alpha"]
    model = fit_residual_ridge(x, y-base*z, z, alpha=chosen, sample_weight=w)
    # An explicitly calibrated alternative adjusted delta, not the paper's rolling
    # 90-observation reproduction. These three parameters use TRAINING labels only.
    def hw_basis(part):
        delta = np.array([r["delta_bs"] for r in part])
        g = np.array([r["vega_coin"]/r["sqrt_tau"] for r in part])
        return g[:,None]*np.column_stack([np.ones(len(part)),delta,delta*delta])
    hw_design = z[:,None]*hw_basis(train)
    sw = np.sqrt(w/w.sum())
    hw_coefficients, _, hw_rank, _ = np.linalg.lstsq(hw_design*sw[:,None], (y-np.array([r["n_bs"] for r in train])*z)*sw, rcond=None)
    results = {}
    paired_by_split = {}
    for name, part in partitions.items():
        xp, yp, zp, bp, _ = arrays(part)
        bs_hedge = np.array([r["n_bs"] for r in part])
        errors = {"unhedged": yp, "bs": yp-bs_hedge*zp, "sm": yp-bp*zp, "hw_static_fit": yp-(bs_hedge+hw_basis(part)@hw_coefficients)*zp, "ml": yp-(bp+model.predict(xp))*zp}
        losses = {method: date_losses(part, error) for method,error in errors.items()}
        means = {method: float(np.mean(list(values.values()))) for method,values in losses.items()}
        paired = [{"date": day, "bs_mse": losses["bs"][day], "sm_mse": losses["sm"][day], "hw_mse": losses["hw_static_fit"][day], "ml_mse": losses["ml"][day], "difference_sm_minus_ml": losses["sm"][day]-losses["ml"][day], "difference_hw_minus_ml": losses["hw_static_fit"][day]-losses["ml"][day]} for day in losses["sm"]]
        paired_by_split[name] = paired
        results[name] = {"mean_daily_mse_btc_squared": means, "ml_improvement_vs_sm": 1-means["ml"]/means["sm"], "daily_losses": paired}
    test_d = np.array([x["difference_sm_minus_ml"] for x in paired_by_split["test"]])
    rng = np.random.default_rng(20261022)
    bootstrap = np.mean(rng.choice(test_d, size=(4000,len(test_d)), replace=True),axis=1)
    val_d = np.array([x["difference_sm_minus_ml"] for x in paired_by_split["validation"]])
    sd = float(np.std(val_d,ddof=1))
    reference = results["validation"]["mean_daily_mse_btc_squared"]["sm"]
    planning = {"validation_daily_difference_sd": sd, "test_dates": len(test_d), "approx_80pct_power_detectable_difference": 2.8*sd/math.sqrt(len(test_d)), "detectable_fraction_of_validation_sm_mse": 2.8*sd/math.sqrt(len(test_d))/reference,
        "approx_required_test_days": {str(frac): math.ceil(7.84*(sd/(frac*reference))**2) for frac in (.05,.1,.2)}}
    comparison_intervals = {}
    for baseline, key in (("sm","difference_sm_minus_ml"),("hw_static_fit","difference_hw_minus_ml")):
        differences = np.array([r[key] for r in paired_by_split["test"]])
        samples = rng.choice(differences,size=(4000,len(differences)),replace=True).mean(axis=1)
        comparison_intervals[baseline] = {"mean_daily_mse_difference":float(differences.mean()),"day_bootstrap95_interval":np.quantile(samples,[.025,.975]).tolist()}
    curve = []
    days = sorted({r["date"] for r in train})
    for size in sorted({min(n,len(days)) for n in (6,12,24,len(days))}):
        part = [r for r in train if r["date"] in set(days[-size:])]
        xx, yy, zz, bb, ww = arrays(part)
        # Each prefix tunes its own alpha on the same validation dates, never test.
        losses = []
        for alpha in alphas:
            fitted = fit_residual_ridge(xx,yy-bb*zz,zz,alpha=alpha,sample_weight=ww)
            l = date_losses(val,yv-(bv+fitted.predict(xv))*zv)
            losses.append((float(np.mean(list(l.values()))),alpha))
        best, alpha = min(losses)
        curve.append({"training_dates": size, "training_rows": len(part), "first_training_date": min(r["date"] for r in part), "last_training_date":max(r["date"] for r in part), "selected_alpha": alpha, "validation_mean_daily_mse": best})
    return {"status": "fitted_and_evaluated_chronologically", "partitions": sizes, "features": FEATURES, "selected_alpha": chosen, "validation_search": candidates, "coefficients": model.coefficients.tolist(), "training_feature_mean": model.mean.tolist(), "training_feature_scale": model.scale.tolist(), "training_design_rank": model.design_rank, "hw_static_training_coefficients": hw_coefficients.tolist(), "hw_static_training_rank": int(hw_rank),
        "results": results, "test_daily_difference_mean": float(test_d.mean()), "test_day_bootstrap95_interval": np.quantile(bootstrap,[.025,.975]).tolist(), "test_comparison_intervals":comparison_intervals, "power_planning_from_validation": planning, "learning_curve": curve,
        "inference_caveats": ["Calendar-sampled first-of-month days, not random or continuous sessions.", "Bootstrap treats sampled days as independent and has few clusters; it is a diagnostic, not a certification.", "Power calculation is a normal-planning approximation using validation dispersion, not guaranteed power or an economic significance threshold.", "Learning curve uses trailing windows with a fixed end date; sample size and included historical regimes still change together.", "Features and hedge conversion reflect an illustrative SM benchmark, not a user-approved final specification."]}


def run(directory, output_prefix):
    directory = directory.resolve()
    rows, summaries = build(directory)
    prefix = ROOT / output_prefix
    rows_path = directory / "model_rows.jsonl.gz"
    with gzip.open(rows_path, "wt") as stream:
        for row in rows:
            stream.write(json.dumps(row)+"\n")
    counts = Counter()
    for summary in summaries:
        counts.update({k:v for k,v in summary.items() if isinstance(v,int)})
    metrics = {"executed_utc": datetime.now(timezone.utc).isoformat(), "rows": len(rows), "dates": len({r["date"] for r in rows}), "market_intervals": len({r["entry_utc"] for r in rows}), "derived_rows_file": str(rows_path.relative_to(ROOT)), "filters": {"max_ticker_age_seconds":60,"max_relative_spread":.5,"maturity_days":[7,45],"focal_moneyness":[.9,1.1],"smile":"five OTM distinct strikes, two strictly each side; local quadratic; bid/ask-IV sensitivity"}, "counts": dict(counts), "interval_audits": summaries, "model_assessment": train_and_assess(rows)}
    Path(str(prefix)+"_metrics.json").write_text(json.dumps(metrics,indent=2)+"\n")
    print(json.dumps({k:v for k,v in metrics.items() if k not in ("interval_audits","model_assessment")},indent=2),flush=True)
    print(json.dumps(metrics["model_assessment"],indent=2),flush=True)


if __name__ == "__main__":
    parser=argparse.ArgumentParser()
    parser.add_argument("directory",type=Path)
    parser.add_argument("--output-prefix",default="docs/research_model_panel")
    args=parser.parse_args()
    run(args.directory,args.output_prefix)
