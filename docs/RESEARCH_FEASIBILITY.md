# Data feasibility for BSAD vs ML hedging — 22 October 2026

**Current result:** see [actual BSAD/ML dataset validation](RESEARCH_DATA_SUFFICIENCY.md), which goes beyond the earlier API/trade checks below and includes chronological fitting, label-quality diagnosis and a continuous intraday quote audit.

## Conclusion

**Update after independent consultation:** a subsequent three-request experiment successfully retrieved historical option and perpetual ticker bid/ask and sizes from the free Tardis first-of-month API. The next step is now a quote-first feasibility gate, superseding the trade-first pilot below. See [research decision and new evidence](RESEARCH_DECISION.md). This report preserves the preceding trade/candle assessment; its statement that no vendor sample was downloaded applied at that earlier stage.

**A trade-based exploratory research pilot is feasible without waiting for the collector to accumulate a year. A complete, synchronized, executable hourly option-chain dataset has not been established.** The separate public History API provides substantially more than the OHLCV-only evidence examined previously. It supplies actual historical trades, including trade IV, mark price and index price, even for expired instruments. This changes the available starting point, but does not make trade observations equivalent to quotes.

Keng has not fixed the research specification. This assessment uses provisional BTC/ETH, hourly rebalance, a 24-hour same-contract holding diagnostic and a desired 12-month history. These are assessment scenarios, not approved final research requirements. The precise BSAD formula, hedge instrument and P&L currency remain undecided. No model was trained and no hedging-performance conclusion is made.

## New experiment

29 public, unauthenticated, sequential requests on 22 October 2026, approximately 12:10–12:13 Asia/Bangkok, capped at one request per second with no retries. No trading, notification, timer, production capture, deployment, commit or push was performed. Bodies, request parameters, timing, status and checksums were saved under three ignored directories:

- `experiments/results/2026-10-22-research-feasibility/`: 16 normal-host requests.
- `experiments/results/2026-10-22-research-history-host/`: nine separate-history-host requests.
- `experiments/results/2026-10-22-research-trade-chain/`: four one-hour, currency-wide option-trade samples.

Probe scripts are `experiments/research_feasibility.py`, `research_history_probe.py` and `research_chain_probe.py`. Re-running them makes network requests into a new directory; the existing directories are intentionally not overwritten. `experiments/research_quality.py` and its companion notebook only read saved evidence.

### The host matters

The normal `https://www.deribit.com/api/v2/` returned zero trades for all four tested expired-instrument/day requests. The same parameters on `https://history.deribit.com/api/v2/` returned 83, 98, 67 and one trade respectively. An empty normal-host result is therefore not evidence that the historical trades do not exist.

For `BTC-24APR26-72000-C`, the History API returned sequences 1–100 from 2–5 February 2026, and the next request using `start_seq=101` returned sequences 101–200. There were no sequence gaps or duplicates across those two sample pages. Response order was not always increasing by sequence, despite `sorting=asc`; a downloader must inspect actual sequences, handle ties, advance from the maximum sequence and verify pagination, not trust the last row blindly. This is a two-page check, not a full download.

For `ETH-26JUN26-3000-C`, its first 100 trades span 28 June 2025 to 11 January 2026, illustrating highly irregular activity. This single contract is not a representative liquidity estimate.

### Currency-wide historical sample

Each query requested one hour, 12:00–13:00 UTC, options only, count=1000. All four returned `has_more=false`; no additional page was requested. All returned trade timestamps were within the requested interval; no duplicate `(instrument_name, trade_id)` keys were found. Every returned trade had IV, mark price and index price. These checks apply only to the returned samples and do not prove the exchange archive has no missing records.

| Date | Currency | Trades | Traded instruments | Expiries | Instruments whose last trade was >5 minutes before 13:00 |
|---|---|---:|---:|---:|---:|
| 2025-10-01 | BTC | 923 | 220 | 12 | 197/220 |
| 2025-10-01 | ETH | 528 | 152 | 11 | 143/152 |
| 2026-04-17 | BTC | 733 | 220 | 12 | 182/220 |
| 2026-04-17 | ETH | 334 | 122 | 11 | 82/122 |

This directly demonstrates both BTC and ETH option trades more than one year before the probe. Some expiries had many traded strikes; others had very few. These are traded instruments during an hour, not the complete listed universe or simultaneous option-chain quotes. Five minutes is a diagnostic reference, not a required pass threshold. Last-trade age is not quote age; an old trade does not imply that no fresh quote existed.

### Expired instruments, candles, funding and marks

- Normal-host `get_instrument` resolved the old BTC and ETH examples as `archivized`, with units/contract metadata.
- Each old contract returned 168 hourly candles over its last seven days. BTC had 120 zero-volume hours; ETH had 167. Two recently expired options also returned 94 and 96 hourly bars. Availability of expired candles is now positively verified for these four examples.
- Normal-host `get_instruments(expired=true)` returned 54 BTC and 60 ETH instruments, all expiring 21 October 2026, whereas historical instrument examples expired months earlier. It is not an all-time listing catalog. Official documentation explains the recently delivered limitation.
- `get_funding_rate_history` on 1 October 2025 returned 23 hourly points per perpetual (01:00–23:00 UTC) for the requested 00:00–23:59:59.999 interval. Its interval/end-boundary interpretation needs a bracketing test; do not silently equate this result to all 24 funding intervals or a year of complete funding data.
- Mark history for the two archived examples returned `instrument is not active` on the normal host; the tested history-host mark and order-book methods returned HTTP400 non-JSON `bad_request`. These particular attempts did not establish a historical quote/mark-snapshot route. They do not prove that every alternative source or method is unavailable.

## Rechecked old candle quality

The saved 19 October experiment contains 137 raw responses. Archive and individual raw-body hashes were checked. The hourly recent-wide sample has zero timestamp duplicates and no conflicting values in overlapping sampled pages; numeric OHLC and array-length checks passed.

| Instrument | Hourly bars | Zero-volume bars | Longest consecutive zero-volume run | 24h same-contract start/end candidates | Candidates with positive volume at both endpoint hours |
|---|---:|---:|---:|---:|---:|
| BTC-30OCT26-95000-C | 1,909 | 1,338 (70.09%) | 40 hours | 1,885 | 272 |
| ETH-25DEC26-3200-C | 5,001 | 4,754 (95.06%) | 259 hours | 4,977 | 24 |

All zero-volume bars in those recent-wide samples had O=H=L=C. A stricter diagnostic requiring positive volume in all 25 hourly bars of each 24-hour path left seven overlapping BTC paths and zero ETH paths. This is not a necessary criterion for research: valid quotes can exist without trades. It demonstrates that trade-candle density cannot be assumed merely because the timestamp grid is complete.

Option/perpetual candle timestamps matched at every sampled option hour. That only aligns candle buckets; it does not synchronize the underlying trades or establish executable prices at rebalance times. Candidate windows overlap and are not independent ML observations. These are two purposively selected contracts, not estimates for the whole market.

The earlier 5,001-bar observation is about **208.375 nominal days per hourly response**, not a total retention limit. Adjacent pages and near-listing hourly samples exist, but the complete intervening years have not been downloaded or verified.

## Research requirement matrix

| Requirement | Evidence now | Consequence / remaining work |
|---|---|---|
| Historical option trades, IV, event mark/index | Verified in expired contracts and BTC/ETH samples dated October 2025 and April 2026 | Suitable basis for a bounded trade-based pilot; not proof of continuous annual coverage. |
| Strike/expiry/type and units | Archived metadata lookup succeeds for two old contracts; names also encode strike/expiry | Build historical universe from historical evidence, not today's open contracts. Contract ID must remain stable over each holding period. |
| Same-time smile across strikes | Many strikes traded within each sample hour, asynchronously | Use only data observed before each decision, impose declared freshness rules, record fit coverage and failures. Never treat all trades in an hour as available at its start. |
| Option valuation at every hedge/exit | Sparse trades and flat zero-volume candles | Define valuation methodology; trade marks occur at trade times only. Nearest future trade introduces look-ahead. Long forward fills can invent low-risk paths. |
| Hedge instrument prices and pricing basis | Perpetual candle history works; event trade index prices and sample funding exist | Select spot/future/perpetual and correct coin/USD accounting. Underlying index is not automatically the same-expiry forward or an executable hedge price. |
| Bid/ask spread, sizes, slippage | No historical top-of-book established by tested public routes | An empirical execution-cost backtest is not supported yet. Assumed spreads/fees are scenarios, not measured execution. Full depth is optional for small-size work; top-of-book is the first target. |
| Historical funding and fees | One day of old funding sampled; metadata commissions are not a historical fee schedule | Test boundary semantics and full coverage; match the fee tier, timestamp and contract type. |
| ML sample adequacy | Only sparse selected contracts and four sampled currency-hours audited | Build a panel, report independent time blocks/regimes and out-of-time folds. Purge overlapping holding windows. Twelve months alone is not a statistical sufficiency certificate. |
| BSAD definition | Several smile-adjusted delta variants are possible | Fix formula and hedge numeraire before deciding which forward, IV and smile inputs are mandatory. |

## Earlier proposed next step — superseded by the quote audit

Start with a **BTC trade-based data pilot in two separated one-week windows**, before downloading a full year or training models. This is a proposed next task, not a job enabled by this assessment.

1. Download historical option and chosen hedge-instrument trades with proven pagination; preserve raw, block/combo flags and trade IDs. Recover contract metadata for the historical universe.
2. Construct hourly as-of panels with explicit valuation/freshness policy and the same contract at t and t+24h. Measure 1/5/15/60-minute freshness sensitivity; do not choose a favorable cutoff after seeing model scores. Include fees/funding as available and label unobserved costs as scenarios.
3. Report hourly retained contracts, strike/expiry coverage, smile-fit failures, usable same-contract outcomes, missing exits and market regimes. Compare trade-IV and event-mark valuation methods without silently mixing them.
4. If the aim requires execution-realistic comparison, audit a permitted historical quote sample for options plus the chosen hedge instrument, then decide whether a paid archive is necessary. Tardis documents Deribit archives and options-chain/top-of-book datasets, but no vendor dataset was downloaded or quality-certified here and no purchase or access claim is made.
5. Freeze BSAD formula, P&L currency and sample-selection rules. Only then size the full download, fit the BS/BSAD baseline and train an ML model with leakage-safe time splits.

Do not wait a year to begin exploration, and do not claim the current sparse candles or trade archive already support the final executable hourly-chain study. The prospective collector remains useful for future quote/depth archives; starting it today does not repair the past.

## Reproduction and sources

- Offline checks and exact metrics: `experiments/research_quality.py`, `experiments/research_feasibility.ipynb`, [research_feasibility_metrics.json](research_feasibility_metrics.json). Run `.venv/bin/python -B experiments/research_quality.py`; it makes no network requests.
- The companion notebook uses only the standard library plus the local analysis module. Its cells were executed sequentially in CPython and outputs saved. Jupyter kernel/nbformat packages are absent in the available environment, so native Jupyter execution/validation was not performed. A static HTML rendering was inspected separately.
- [Deribit trade-by-time schema](https://docs.deribit.com/api-reference/market-data/public-get_last_trades_by_instrument_and_time): event prices, IV, mark/index and trade identifiers. Actual historical-host responses are the controlling evidence for the host behavior measured here.
- [Deribit instrument listing semantics](https://docs.deribit.com/api-reference/market-data/public-get_instruments): recently delivered versus archivized instruments.
- [Deribit funding history](https://docs.deribit.com/api-reference/market-data/public-get_funding_rate_history): hourly perpetual funding.
- [Deribit mark history](https://docs.deribit.com/api-reference/market-data/public-get_mark_price_history): limited option coverage.
- [Tardis Deribit coverage](https://docs.tardis.dev/historical-data-details/deribit): provider documentation, not a verified dataset.
