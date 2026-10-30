# Research decision after independent agent consultation — 22 October 2026

**Superseded by actual dataset validation:** the consultation alone did not prove sufficiency. We subsequently constructed72 sampled dates, fitted baselines/ML, audited label defects and completed a continuous intraday ledger. Read [measured data-sufficiency results](RESEARCH_DATA_SUFFICIENCY.md). The status and next-step lists below describe the earlier consultation stage, not current completion.

## Decision

**Continue the options-hedging project, starting with BTC and a bounded historical quote audit. Do not switch projects because of the trade-data limitation. Do not yet claim that a final ML dataset is sufficient.**

The earlier trade-first recommendation is superseded by a newly verified free quote route. Historical quotes exist and are accessible without waiting for this collector to accumulate a year. The remaining decision is whether a correctly specified experiment can be constructed and expanded within the user's eventual budget and deadline.

The user has no fixed horizon, hedge cadence, asset universe, data budget or research requirements yet. Hourly rebalancing, a 24-hour holding period and a year of data were diagnostic assumptions, not requirements. A defensible null result in which ML does not improve hedging is a successful research outcome.

## New direct evidence

Three unauthenticated Tardis raw `ticker` requests for 1 October 2025, at 12:00, 13:00 and 23:00 UTC, each returning one minute. The sample contains ten BTC options, six ETH options and both currency perpetuals. These were deliberately small availability checks, not a research population.

| Sample minute | Option ticker messages | Perpetual ticker messages | Symbols returned / requested | Invalid or absent two-sided price/size observations |
|---|---:|---:|---:|---:|
| 12:00 UTC | 772 | 3,477 | 18/18 | 0 |
| 13:00 UTC | 705 | 1,950 | 18/18 | 0 |
| 23:00 UTC | 482 | 2,362 | 18/18 | 0 |

There are **9,748 messages** in total. The 1,959 option messages all contain positive bid IV, ask IV, mark IV, underlying and index prices. The checks verified raw-body SHA256 values, selected symbols, positive prices and sizes, non-crossed BBO, and timestamps within the requested provider-receipt minute. No negative exchange-to-receipt lag or disconnect marker appeared in these samples. The last ticker for every selected symbol in each minute had valid two-sided prices and sizes; maximum last-ticker observation age at minute end was 2.563, 3.957 and 4.628 seconds respectively.

**Scope:** these are three disjoint minutes on one day, with symbols chosen from an already saved hour of trades. This diagnostic selection is not a predeclared research universe. It is forward-looking for decisions inside the source hour; any later use would require an explicitly defined prior-period liquidity filter and a correspondingly limited research population. This proves availability of historical ticker BBO, not all-instrument coverage, stable smile estimation, full-day continuity, fills or statistical adequacy. Ticker observation age is not time since the BBO last changed. Coexisting ticker fields do not establish identical update triggers.

Reproduce without network: `.venv/bin/python -B experiments/research_quote_quality.py`. Exact metrics: [research_quote_metrics.json](research_quote_metrics.json). The raw probe, hashes, parameters and UTC request/receipt times are saved under ignored `experiments/results/2026-10-22-research-free-quotes/`. `research_quote_sample.py` is the three-request experiment, not a backfill CLI. The companion feasibility notebook contains an offline quote-check cell.

## Access routes and what each permits

| Route | Verified access terms or observations | Appropriate use / remaining limitation |
|---|---|---|
| Free first day of each month | Provider documents every exchange/data type, CSV and raw replay, no key; the three minute requests above succeeded | Audit a complete day first. Multiple years of such days could support explicitly calendar-sampled intraday research, not continuous annual paths or unrestricted generalization. |
| Free trial | Provider documents one month of access to days 1–10 of the previous UTC calendar month, all exchanges, 20GB, no payment details; account required and eligibility limited | Could audit consecutive hedge paths. In October 2026 the documented window is 1–10 September 2026. No account or trial was created, and its actual downloads/quality are unverified. |
| Academic subscription | University-email eligibility, CSV only, quarterly/yearly billing. Current displayed Options rate $350/month ($1,050/quarter); Derivatives $450/month ($1,350/quarter), before tax | Significant expense; not recommended before the audit. Options scope does not establish perpetual entitlement. Derivatives documents all Deribit instruments. Quarterly access starts 12 months before subscription start; confirm exact dates, entitlement and total at checkout. No purchase or paid access was attempted. |
| Free Deribit historical trades | Previously verified expired trades, IV and event mark/index on the separate history host | Useful supplementary event data or a separately scoped event study; not the primary substitute for a quote-based hedging experiment. |

Daily files and all-symbol streams can be large. A first-day minute probe does not establish the cost of retrieving a complete day or staying within a trial's transfer cap. Paid Academic CSV access is a different interface from the free raw-API probe and needs its own audit. A source's exchange-message preservation does not grant permission to redistribute its raw archive.

## Reconciled independent reviews

Three fresh agents reviewed methodology, source options and the strongest reasons to reject the project. Their agreement is advisory; the observed API responses and source documentation control factual claims.

- **Methodology reviewer:** retain the intended smile-adjusted baseline and begin with a simple BTC comparison. Do not quietly replace BSAD with a different formula just to fit available trades. A small residual ML model is more defensible than starting with deep learning. Split all contracts by calendar time and estimate uncertainty by time blocks.
- **Source reviewer:** free first-of-month quotes and the documented ten-day trial are the best next routes. A free older university archive contains trades, so it does not repair the quote limitation. Broader paid access is a later decision, not a prerequisite for starting.
- **Skeptical reviewer:** freeze the research contract before bulk acquisition. Three minutes and sixteen selected options do not prove a historical universe or local smile slope. A year and a 24-hour horizon are not inherently necessary; terminal hedge error does not require option valuation every intermediate hour unless the chosen hedge rule needs those inputs.

We changed the earlier plan accordingly: quote-first feasibility replaces the proposed two-week trade backfill. We did not infer that three agreeing agents or a working endpoint establish final research sufficiency.

## Recommended plan and stopping rules

1. **Specify the experiment before bulk data.** Recommend BTC first and a simple one-period intraday hedge comparison as the initial experiment, with BS, one explicitly defined smile-adjusted delta and a small ML correction using identical information. Fix the BSAD equation, hedge contract, coin/USD accounting, holding/decision times, entry-time universe selection, valuation and cost assumptions. If the intended question requires dynamic hedging, retain that requirement and test its whole decision path. Do not silently narrow it to suit free data.
2. **Run a bounded quote feasibility gate.** Allocate up to two working days of research effort before expanding acquisition. Choose one free session in advance and one or two suitable expiries using only information available at entry; retrieve the needed smile strikes, initialization data and hedge observations. Construct one complete as-of BS/BSAD comparison and cash-flow ledger. Repeat on another preselected session before claiming repeatability. No ML training is needed for this gate.
3. **Assess the actual mandatory inputs.** Both timestamps must precede each decision under the declared availability rule. Audit historical metadata/units, BBO validity and displayed size, stale states, underlying/forward basis, smile slope stability, funding if relevant, and observed exits. Keep all entry-selected contracts in the denominator; report missing exits instead of silently discarding them. Choose freshness/fit/coverage tolerances before seeing model performance, publish sensitivity and failed cases. No universally correct numerical coverage threshold was established here.
4. **Choose expansion after the gate.** If sampled intraday sessions answer the user's academic question, audit free first-of-month days across several years and explicitly limit conclusions to that sampling scheme. If consecutive holding paths or general daily claims are essential, verify a broader continuous source, starting with the documented trial if eligible. Only then consider university access or a funded subscription. Do not join unobserved days, forward-fill long gaps or treat twenty-four sampled days as a continuous two-year panel.
5. **Separate statistical adequacy from pipeline success.** Train/tune/test chronologically across entire dates, purge overlapping outcomes and keep the test period untouched. Quantify uncertainty using time blocks and inspect regime coverage; BTC/ETH and multiple strikes on the same dates do not multiply independent market histories. Reduce model complexity if only a limited sample is available. Neither ten days nor a nominal year automatically proves sufficient data, and ML superiority is not a pass condition.

**Stop the current empirical specification** if its mandatory inputs cannot be reconstructed causally, if no traceable ledger can be produced within the feasibility cap, or if a sample matching the intended claim cannot be obtained within the actual budget/deadline. Repair a concrete design error before abandoning the topic.

**Fallback ranking:** (1) quote-based empirical study matching the intended claim; (2) explicitly calendar-sampled intraday empirical study if academically acceptable; (3) simulation of the same hedging question across several dynamics, with real quotes as a limited sanity check; (4) another project only when empirical evidence is required and neither affordable access nor an acceptable revised design exists. Simulation demonstrates behavior in specified models, not real-market ML superiority. A trade-event study is another explicit research redesign, not an undisclosed replacement for the original quote experiment.

The collector is a separate prospective archive investment. It remains useful, but neither deploying it nor waiting a year is required to begin this feasibility gate. VPS readiness and the collector's existing acceptance requirements are unchanged.

## Status and sources

- Completed: three independent consultations, reconciliation of objections, primary-source access checks, three actual free quote requests and offline checks, updated research decision and implementation plan.
- Not completed: a complete-day quote audit, frozen research formula, a BS/BSAD ledger, final dataset, model training, statistical adequacy assessment, deployment or data purchase. This is a decision to proceed through explicit gates, not a certificate that the eventual study will pass them.
- [Tardis free/trial access, plan definitions, academic eligibility and history windows](https://docs.tardis.dev/faq/billing-and-subscriptions).
- [Current displayed pricing](https://tardis.dev/).
- [Raw replay API: minute boundaries use provider receipt time](https://docs.tardis.dev/api/http-api-reference).
- [Deribit channels and collection details](https://docs.tardis.dev/historical-data-details/deribit).
- [CSV schemas: options chain, BBO and derivative ticker](https://docs.tardis.dev/downloadable-csv-files/data-types).
- [Alexander and Imeraj (2023), university publication record](https://sussex.figshare.com/articles/journal_contribution/Delta_hedging_bitcoin_options_with_a_smile/23494724): a related study uses a two-year hourly dataset and several smile-adjusted deltas; its design is not replicated by our trade samples.
