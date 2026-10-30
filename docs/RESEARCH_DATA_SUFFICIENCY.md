# BSAD/ML data sufficiency: measured results

## Decision

**มีข้อมูลจริงพอสำหรับเริ่มงานวิจัย BTC hedging แบบจำกัดขอบเขต และประกอบ features/targets เพื่อฟิตโมเดลขนาดเล็กได้แล้ว จึงยังไม่มีเหตุผลให้เปลี่ยนโปรเจกต์เพราะคิดว่าไม่มีข้อมูลย้อนหลัง** อย่างไรก็ตาม ชุดฟรีที่ตรวจนี้เป็นวันตัวอย่างแยกกัน ไม่ใช่ข้อมูลต่อเนื่องทุกวันหลายปี และไม่ได้รองรับข้อสรุปสำหรับทุกตลาด ทุกโมเดล หรือกำไรสุทธิจากการซื้อขายจริง

ข้อสรุปนี้มาจากการประกอบข้อมูล ฟิตสมการ และทดลอง ML จริง ไม่ใช่เพียง HTTP200 หรือความเห็นตรงกันของ agent รายงานก่อนหน้าที่แนะนำให้ตรวจต่อยังไม่ใช่หลักฐานความเพียงพอ; การปิด goal ในช่วงนั้นเร็วเกินไป

## What was actually constructed

- Predeclared 72 first-of-month dates, October 2020–September 2026. Four endpoint minutes per date give two non-overlapping one-hour outcomes: 08–09 and 16–17 UTC. Both exchange and receipt timestamps must precede each decision.
- 288 saved replay responses, 276 newly requested and 12 reused, **361,943,142 compressed bytes**. Historical cohorts were selected from each date's 08UTC snapshot, never today's open instruments or later trading activity.
- Two expiries near 14/28 days, nine nearby strikes per expiry, both calls/puts. The central five strikes are focal contracts; four outer strikes provide interpolation support. Guards are not failed observations.
- Positive two-sided prices and sizes, entry relative spread at most50%, ticker observation age at most60seconds, expiry7–45days, focal moneyness0.9–1.1. Exits require fresh positive non-crossed BBO; widening exit spreads are retained and audited rather than silently discarded.

| Evidence | Actual result |
|---|---:|
| Preselected focal observations | 2,718 |
| Focal entries with usable smile inputs | 2,539 |
| Same-contract one-hour pairs | **2,513 (92.46% of preselected focal observations)** |
| Observed exits / fitted entries | 2,513/2,539 =98.98% |
| Calendar dates / shared market intervals | **72 /144** |
| Smile support failures among accepted focal entries | 0/2,539 |
| Slope quote-band includes zero | 1,196/2,513 =47.59% |

These counts are not independent ML sample counts. Contracts at the same timestamp share the underlying move; BTC/ETH would not double the number of independent dates. The26 missing/invalid exits are reported, not imputed; performance on complete cases does not establish performance on every original entry.

## Required columns and actual fit

| Need | Construction demonstrated | Boundary |
|---|---|---|
| Option value and contract | Historical bid/ask and sizes, stable instrument ID, strike/type/expiry; BTC premium units | Standard BTC inverse options only; no ETH sufficiency claim. |
| Pricing and conversion | Recorded expiry underlying/forward F, contemporaneous index S, actual perpetual BBO H | F, S and H are preserved separately. A synthetic forward is not an executable hedge. |
| BS inputs | Midpoint-price IV inversion, maturity, moneyness, forward delta and vega | Zero discounting is a declared benchmark assumption. Greeks use own midpoint IV, while slope comes from the fitted smile. |
| Smile-adjusted baseline | Five-point local quadratic on OTM strike IVs, two points on either side; observed slope and quote-band sensitivity | Demonstration uses explicitly named sticky-moneyness adjustment; BSAD is not one uniquely specified formula. Quote-band bounds are not statistical confidence intervals. |
| A calibrated adjustment | Three training-only Hull–White-style coefficients fitted to signed hedge risk | Static calibration demonstration, not a replication of the paper's rolling90-observation procedure. |
| ML features | Put flag, log-moneyness, sqrt maturity, midpoint IV, BS delta, scaled vega, smile slope, relative spread, hedge/index basis | Nine complete numeric features; order flow, GARCH and long lag windows are not in this panel. |
| ML target/loss | Observed same-contract value change and actual inverse-perpetual price change | Gross BTC price risk; no funding, fees, fills or collateral claim. Avoid dividing by near-zero hedge returns to construct an unstable oracle-delta label. |

For premium q in BTC and normalized inverse hedge n=N/H at entry, the observed gross risk loss is based on:

`error = (q_exit - q_entry) - n * (1 - H_entry/H_exit)`

The conversion from a USD forward delta to n assumes parallel percentage changes of F, S and H:

`n = (F * delta_adjusted - USD_option_value) / S`.

The residual ML minimizes this risk using entry features. Normalization and coefficient fitting use training rows only; regularization is chosen on validation dates. Every contract on a date stays in one chronological partition. Tests of the math include36 price/IV roundtrips, six finite-difference hedge checks, quote-band propagation and synthetic residual recovery.

## Label quality changes the recommendation

An important defect in using the broad panel indiscriminately: **22 very wide exit quotes account for83.07% of its date/interval-weighted BS squared error**. Some midpoint values jump while the underlying barely moves. Thus “2,513 complete rows” does not mean2,513 equally trustworthy economic labels. Original broad-panel ML scores must not be sold as a final model comparison.

A causal alternative is to define the research population as **OTM at entry**, consistent with the smile construction. This is not a filter on future exit liquidity:

| Entry-OTM subset | Rows | Dates | Market intervals |
|---|---:|---:|---:|
| Training: Oct2020–Dec2023 | 675 |39|78|
| Validation: Jan–Dec2024 |240|12|24|
| Later evaluation: Jan2025–Sep2026 |420|21|42|
| Total | **1,335** | **72** | **144** |

All nine features are finite. None of these1,335 observed exits has a spread over50%. Smile slope quote-bands still include zero for632/1,335; the adjustment's economic signal remains uncertain even though it can be computed. Quote age, midpoint inversion and support were checked rather than assumed.

BS, the smile adjustment, a calibrated adjustment and a small residual ridge were actually fitted/evaluated on this subset. Its ML-versus-SM and ML-versus-calibrated-adjustment day-level intervals include zero. The run supports a modest exploratory comparison, **not a claim that ML is better**. A valid negative or inconclusive result remains research.

The OTM design was investigated after examining broad-panel labels. Its scores are therefore exploratory, not a newly untouched final test. A subsequent formal model comparison needs a frozen design and fresh holdout. The trailing-window learning curves also vary regime coverage, not sample size alone. Validation loss scale differs from later evaluation, so the earlier44/174-day power estimates must not be treated as universal minimum data requirements.

## Continuous intraday path, not just isolated endpoints

The public CSV route was also tested end-to-end on1 October2025: **20 files,43,921,277 compressed bytes,2,951,029 CSV rows**. Eighteen options (nine strikes, one expiry) plus perpetual BBO and derivative ticker supply pricing forward, conversion index and hedge observations.

- Decisions08–23UTC:16 consecutive hourly states after cohort selection.
- Ten focal contracts have all16 smile/hedge states and all15 one-hour outcomes: **150/150 focal pairs**.
- Ten complete15-hour gross dynamic hedge ledgers were constructed. Option changes telescope to the end-minus-start value; hedge cash flows use the actual inverse price formula.
- The20 overlapping pairs with the monthly replay experiment have exactly matching option endpoint prices and normalized hedge gains. CSV and native replay routes therefore reconcile in this checked intersection.

This establishes that a continuous within-day experiment is technically possible. It does not establish an overnight24-hour path, every session's continuity or sufficient independent dates for a complex ML model. Raw file checksums/request metadata are preserved. The original daily capture script hash is recorded, but the agent refined that script before its exact acquisition source was archived; this source-copy limitation is documented separately. The analysis source and raw-body hashes remain inspectable.

## Available expansion routes

| Route | Evidence and practical consequence |
|---|---|
| Free first-of-month Tardis data | Actual72-date panel and full intraday session now verified. Provider nominal CSV availability gives91 isolated monthly days Apr2019–Oct2026; that is not91 universally intact days or continuous annual data. Raw ticker coverage starts later than earliest CSV coverage. Expanding to more hourly endpoints increases within-day intervals, not independent dates. |
| Select contracts, not the whole market | Full-chain examples are6.78GB/day in Oct2025 and11.72GB/day in Sep2026. Individual contract files were actually downloaded; the18-contract plus hedge full-day audit is43.9MB. This makes a targeted panel much more practical. Those measured sizes are not a guarantee for another date. |
| Trial, if eligible | Documented10 consecutive days,20GB, no payment details, account required. No trial was activated. Full-market files would exceed the allowance; selected-contract data are the plausible route. A published gap on1Sep2026 from18:51–21:39UTC must be excluded from affected paths. |
| Tardis broader history | Actual daily-file interface works. Current Academic Derivatives rate is$450/month billed quarterly/yearly, nominal$1,350/quarter before tax with12-month history on quarterly billing. Options-only entitlement does not establish hedge-perpetual access. No subscription was purchased or annual panel certified. |
| Lower-cost continuous BTC lead: OptionsDX | ListsJun2021–Sep2024; selectedJan2022 UI prices were$5 per month of data at30-minute frequency and$0 EOD. Twelve months would be about$60 only if other monthly prices match. **Actual sample download failed**, and matching executable hedge/index history was not established. This is a lead to qualify, not an approved or verified complete dataset. |

Sources/prices/access terms are current observations in [research_source_access.json](research_source_access.json), including bounded sample prefixes, metadata and known recording incidents. No payment, account creation or deployment occurred.

## Recommended scope and remaining choices

Continue this project, with a BTC, entry-selected OTM, small-model gross hedging-risk benchmark as the **evidence-backed starting scope**. Freeze which adjusted-delta equation, horizon, accounting and valuation define the final question before another formal score comparison. Do not equate successful fitting with proven generalization, or choose filters to make ML win.

The current data support baseline calibration, feature/target construction and a limited empirical comparison. They do **not** establish deep-model sufficiency, all-trading-day conclusions, ETH adequacy, overnight paths, order-flow/GARCH features or net execution performance. If these are mandatory, acquire and audit the matching broader data before committing to those claims; changing the entire topic is not yet necessary.

## Inspect and reproduce

- [Companion notebook](../experiments/research_data_sufficiency.ipynb): saved outputs; no network.
- [Monthly exact counts and chronological fits](research_monthly_panel_metrics.json).
- [Label-quality and exploratory OTM fits](research_label_quality_metrics.json).
- [Full-session raw fields, hourly audits and ledgers](research_daily_quote_metrics.json).
- Source code: `experiments/research_panel_analysis.py`, `research_label_quality.py`, `research_hedge_math.py`, `research_daily_quote_probe.py`.
- The analysis uses Python3.12.14 and NumPy2.3.5. Production collector dependencies/code are unchanged. Raw/derived market rows stay under ignored `experiments/results/`; none were published.
- Native Jupyter/nbformat and plotting packages are absent. Five notebook code cells were executed sequentially in CPython and saved as a table-based diagnostic; its static HTML summary and result tables were visually inspected. Native kernel validation was not performed.
- Primary precedent: [Alexander and Imeraj (2023), university-hosted full paper](https://ndownloader.figshare.com/files/41202854), equations7/10 and data construction. Our benchmark is not a replication.
- [Provider access terms](https://docs.tardis.dev/faq/billing-and-subscriptions), [CSV schemas](https://docs.tardis.dev/downloadable-csv-files/data-types), [OptionsDX product](https://www.optionsdx.com/product/btc-option-chains/).
