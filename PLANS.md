# Data Collector (DC) — plan

V1 follows [SPEC v1](docs/SPEC_V1.md). V2 follows [SPEC.md](SPEC.md). The chunk-by-chunk log up to 28 October 2026 is archived in [docs/v2/history/](docs/v2/history/PLANS_2026-10.md).

## Current state — 8 November 2026

Keng changed the [collection scope](docs/v2/COLLECTION_SCOPE.md) on 8 November 2026: one 1-minute grid fired at :00 UTC across Deribit, Bybit and Binance, plus three event streams, at about 0.2–0.55 GB/day. The 100 ms path is superseded. Its [31 October scope](docs/v2/history/COLLECTION_SCOPE_2026-10-31.md) and the [3 November state and steps](docs/v2/history/PLANS_2026-11-08.md), including every measured run, are kept as history.

- V1: Deribit REST snapshot collector with CLI, archive reader and systemd units. One full public-API capture ran locally (BTC 884/884, ETH 766/766). Its REST transport and archive are the base for the minute-snapshot service.
- V2: raw WebSocket capture with WAL, immutable segments, changing-universe verification and per-venue adapters. Steps 1–4 of the 100 ms plan are committed: rediscovery, Deribit top-20 options, Binance USDⓈ-M feeds with liquidations, and Bybit option tickers/publicTrade (755 tests). Only the event streams stay in scope: Binance `!forceOrder@arr` and Bybit option `publicTrade` already run inside the V2 Binance and Bybit configs; Bybit `allLiquidation` is not built.
- Not built: the minute-snapshot service, the Deribit `ticker.<name>.agg2` sampler, the Binance weight guard, daily zstd sealing and the minute acceptance run.
- Historical 100 ms measurements (not acceptance of the new scope): the Deribit all-options hour produced 7.15 GB/hour of native payload, about 160 GiB/day; a ten-minute Deribit probe crossed 08:00 UTC with 118 retirements and zero rejected subscriptions.

## Next

Goal: build the minute-snapshot service on a new branch, from V1's REST transport and archive, with V2 kept for the three event streams.

```
Goal: Record every feed with no free history on one 1-minute grid shared by Deribit, Bybit and Binance, plus three small event streams, at about 0.2–0.55 GB/day, replacing the 100 ms capture.
Done when: the build steps below are committed on a new branch and the one-hour run across 08:00 UTC in COLLECTION_SCOPE.md passes, with bytes per day, peak RSS and CPU, and Binance weight recorded; PLANS.md and README.md describe the new path.
Out of scope: choosing or setting up a VPS (decided last); trading-bot changes (bot.convert needs a minute-snapshot reader, which trading-bot builds); derived Parquet layers; Bybit option depth beyond about 100 options; any 100 ms stream.
When unsure: pick the simplest option that keeps raw bytes and explicit gaps, add one line to PLANS.md Notes, continue.
```

1. Minute-snapshot service from V1's REST transport and archive: all requests fire at :00 UTC and store raw bytes with send, receive and venue server times. Each minute writes one index line listing every expected request as ok, error, timeout or missed; a missed minute is a recorded gap, never backfilled.
2. Deribit: WS sampler on `ticker.<name>.agg2` for every BTC/ETH option and dated future, writing the last raw value per instrument each minute with venue timestamp, receive time and connection ID, marked stale if received before the current connection's subscribe ACK; futures book summary; indexes `btc_usd`, `eth_usd`, `usdt_usd`, `btc_usdt`, `eth_usdt`; DVOL; BTC/ETH-PERPETUAL ticker.
3. Bybit: option tickers per coin; order book limit 25 for about 100 near-ATM short-dated options; BTC/ETH perp ticker.
4. Binance USDⓈ-M: depth limit 20 and open interest for about 100 symbols, guarded on `x-mbx-used-weight-1m`.
5. Metadata stored on change for all venues; Bybit risk-limit tiers on change; Bybit option settlement from `/v5/market/new-delivery-price` daily. The Deribit sampler subscribes new listings from the metadata and retires options expiring at 08:00 UTC.
6. Event streams on V2: Binance `!forceOrder@arr` (`/market`), Bybit `allLiquidation` for the hedge perps, Bybit option `publicTrade`. Remove the not_enabled 100 ms subscriptions from the shipped V2 configs.
7. Storage: hourly journal, daily zstd seal with manifest; the journal and WAL are deleted only after the sealed file reads back and matches the manifest; the last 2 days of journals stay until an offsite copy is confirmed.
8. Acceptance: one run of at least an hour spanning 08:00 UTC in `dc-acceptance`: minute completeness with gaps recorded, new listings subscribed, expired options retired. Record bytes per day, peak RSS and CPU, and Binance weight used.

The VPS is decided last and is out of scope for this goal.

## Notes

- 8 November scope wording: "BTC/ETH option" on Deribit means currency BTC/ETH (inverse) instruments, so USDC linear options stay not_enabled; the Bybit hedge perps for `allLiquidation` start as BTCUSDT and ETHUSDT; a sampler instrument with no value since its subscribe is written as missing; a Binance request skipped by the weight guard is recorded as missed.
- 8 November scope: the orchestrator's contracts index still needs the new date; that file lives in the orchestrator repository, not here.

Notes below this line were written under the superseded 100 ms scope. They are kept for their decisions and evidence locations; where one mentions step 5, the 120-second listing deadline or the expiry-window run, it no longer applies.

- Reviewed Bybit step-4 integration: exp/bybit-a BTC/ETH option tickers/publicTrade now run from this repo with the shipped config/v2-bybit-options.toml, strict 120-second first-ACK deadline, ACK/first-push coverage through stop and reconnect gaps, and ticker/trades recorded in the run config; 755 tests pass. Live capacity and step 5 await the VPS.
- Bybit review note: assign() may place a new listing on a reconnecting shard; an outage like the observed 150/70-second ones would then miss the 120-second deadline. Prefer ready shards if this appears on the VPS.
- Bybit review note: in an unbounded run one late first ACK or one unrecognized message fails the whole capture at stop (raw kept); revisit per-shard failure if continuous VPS runs hit it.
- Repository renamed to data-collector on 3 November 2026 (GitHub and local folder); the v1 option_collector package, CLI, service and user names stay unchanged for v1 compatibility.
- Step-4 decision: follow the local scope strictly: an option still active at stop needs ACK and first push even when observed in the last 120 seconds; only archived retirement ends its coverage requirement. Keep long reconnects as explicit gaps, and enforce the first-listing deadline independently.

- Reviewed Binance selected-feed chunk: configured USDⓈ-M books/trades and /market all-symbol liquidations now run raw with append-only metadata, exact recorded-reply fixtures and per-route ACK/first-book verification; the final five-minute run above passed readback, and delivery stops after step 3.

- Binance choice: the configured list is explicit and fixed for a run, full UM exchangeInfo is appended every 60 seconds, and quiet trades/liquidations require ACK without a fabricated push. Keep all native all-market liquidation rows, including possible CM st/ps identities, in multiplex raw envelopes; no COIN-M subscription is added.
- Binance exact source bundle: official SDK revision 13fda514bb2a4358712d849aa3163a0505d96da0 schema/models and official llms-full documentation are archived under experiments/results/binance-selected-sources and dc-acceptance ~/dc-binance-selected-evidence/sources-20261103; hashes, retrieval details and recorded-reply fixture provenance are in docs/v2/binance-selected-source-evidence.json. Empty catalogue replies were not treated as schema evidence.

- Reviewed worker capacity: two stable live partitions with negotiated WebSocket deflate completed 300.429 post-ready seconds in dc-acceptance under caffeinate -i -s; verify-run passed 520,070 records, six universe versions and cross-worker coverage of all 1,630 inverse options. Two connections stayed open, no rejection/throttle or too-slow close, index lag medians 136.502/126.871 ms and peaks 6.949/7.260 s returned to about 100 ms at stop; peak summed RSS 305,152 KiB and CPU 196.93%. Evidence: ~/dc-top20-evidence/20261103T080456Z, raw root ~/dc-top20-data/runs/a3288661fd7144638d8e99f4c592a31c. This meets the development capacity brief, not the later expiry-window acceptance.

- Worker transport diagnosis: the isolated two-worker probe at ~/dc-top20-evidence/20261031T135524Z verified 1,770 options but reached 75.509/57.252-second index lag; 60-second stack sampling found each receiver idle in select for over 57 seconds and socket queues near empty. Enable negotiated WebSocket deflate to reduce network demand while preserving decoded native payload bytes.

One line per deferred idea or choice made while working.

- Unmerged branch `docs/consolidate` (eb15a8b, worktree `~/Developer/data-collector-docs`, 1 Nov 2026) reorganises docs, moving docs/v2/* into docs/history/v2/. Before merging after this goal, carry over doc edits made on main since then: AGENTS.md, docs/OPERATIONS.md, the docs/v2/RUNNING.md export section, docs/v2/COLLECTION_SCOPE.md and docs/v2/history/data-collector-spec-review.md. Experiment worktrees were removed after step 4; branches exp/bybit-a and exp/bybit-b remain.

- Scope correction: Binance all-symbol liquidations are now selected on /market as a throttled stat-arb feature; the earlier not_enabled decision is superseded. Bybit option depth remains not_enabled pending known order sizes, with its lack of an archive recorded under COLLECTION_SCOPE.md Open.
- Reviewed identity-validation optimization preserves C0 rejection and accepted Unicode; its unprofiled 300.457-second full-universe run passed verify-run for 422,155 WebSocket messages and two versions, but still had one connection-too-slow close and 16.497-second peak index lag (303.716 ms median). Use workers rather than keep tuning the serialized path.
- Worker capacity choice: start with two stable symbol partitions and a shared REST/subscription pacing file; keep lifecycle, trades and index as independently received raw copies per worker, explicitly report the physical copies, and verify each partition against its full archived discovery rather than freeze a startup list.
- Optimized development evidence: dc-acceptance `~/dc-top20-data/runs/e681535761c44d96969cbf3e805e46e2`, reports under `~/dc-top20-evidence/20261031T125137Z`; 1,770 initialized inverse options, 304,067,667 native bytes, zero rejected/throttled subscriptions, peak RSS 141,472 KiB and CPU 105.42%; this is not expiry-window acceptance.
- Bybit network correction: configure REST and WebSocket hosts independently; acceptance must use api.bytick.com and stream.bytick.com because api.bybit.com and stream.bybit.com do not resolve on this network. Binance endpoints work normally. Bybit transport wiring remains step 4.
- Throughput measurement attempt at `~/dc-top20-evidence/20261031T123931Z` ended at the unchanged 600-second clock-warmup deadline with zero WebSocket frames; its 87-record failed archive verified for raw integrity only. Reset stale chrony source history and requested fresh samples without a clock step, but root-distance uncertainty remains too high for a new capacity claim.
- Collection decision clarification carried over: option features use Deribit, perp features use Binance even for Deribit option research, and costs use trading-venue quotes subject to the documented Bybit perp proxy; selected live feeds are unchanged.
- Step-2 development evidence: dc-acceptance `~/dc-top20-data/runs/7907085f02bf4add8f797231cc95fe90`, reports under `~/dc-top20-evidence/20261031T075603Z`; 600.567 post-ready seconds, peak sampled RSS 193,468 KiB and CPU 108.51%, index receipt lag up to 45.137 seconds; profile the receiver before choosing further parallelism.
- Reviewed step-2 timing corrections: reconcile eligibility at the current receipt clock, retain each new instrument's own first observation, and briefly remember retired identities so delayed settlement notifications do not become false new listings; prune consumed verifier lifecycle receipts after reconciliation.
- Step-2 CPU profile: the 90.332-second post-ready development run at `~/dc-top20-evidence/20261031T123036Z` passed verify-run for 103,723 WebSocket messages and two versions/connections; cProfile attributed 46.193 cumulative seconds to identity text validation across capture initialization and recording, so remove its repeated Python character loop before adding workers.
- Final acceptance needs closing current/recently-delivered metadata reconciliation to detect a listing created during a transport gap that reaches the end of the run; per-version coverage alone cannot prove its absence.
- Expired-instrument source correction: history.deribit.com get_instruments?expired=true returns expired instruments (124,759 BTC options verified 31 October); include_old=true returned only 985 active options.
- Independent-review correction: prior committed docs incorrectly treated Bybit settlement as unavailable and its character cap as per request; new-delivery-price supplies settlement, and the plan now shards connections with headroom rather than splitting requests on one connection.
- Independent-review correction: old all-options measurements included broader currency variants; they do not certify the selected inverse-only scope. The dynamic projection is restricted by native base/settlement metadata, with USDC option/index feeds explicitly disabled.
- Depth-20 correction: the selected book push has no type field; step 2 must require its first push after ACK, while the old diff-feed snapshot check remains historical only.

- Reviewed-scope update: transition timing, optional intraday strikes, the Bybit live cap probe, resource sizes and throttling are record-only; rejection and the explicit lifecycle/verification failures determine acceptance.
- Metadata acquisition remains append-only across Deribit instruments, Binance exchangeInfo and Bybit instruments-info; Bybit funding history is a separate downloadable source with 200 rows per request.
- Rediscovery foundation: config/v2-deribit-live-options.toml runs the changing-universe Deribit path with append-only metadata and per-version verification; Bybit PreLaunch/pagination policy is shared, and its transport wiring remains in step 4. The live path still uses the preceding per-instrument feeds until step 2; no expiry-window acceptance claim.
- Rediscovery recovery choice: preserve the last successful metadata dependency through failed refreshes, fail unresolved listing observations at stop, and drain admitted receive tails and HTTP acquisition before closing their owners.
- Rediscovery integration constraints: retain Bybit Delivering tickers until deliveryTime; preserve the earliest lifecycle observation through REST lookup; bind ACKs to exact requested channels, connection and listing incarnation.
- Revised-scope timing conflict addressed in the live rediscovery path: archive observation/request/ACK/retirement records and check Deribit creation-to-ACK within 120 seconds as a conservative subscription bound; retain request latency separately. Expiry-window acceptance remains pending.
- Scope documentation chunk: selected feeds, disabled-feed reasons and expiry-window acceptance now agree across SPEC.md, FEEDS.md and this plan; runtime implementation remains pending.
- Scope alignment: copied the full 31 October decision into docs/v2/COLLECTION_SCOPE.md; historical recipes and measurements remain visible, with dropped feeds disabled rather than erased.
- Acceptance planning: a non-expiry-window probe is development evidence only; the final run must start by 07:25 UTC and continue past 08:00 UTC across all selected venues.

- Lima dc-acceptance resources: 6 vCPU, 6 GiB RAM and a 100 GiB sparse disk (root filesystem 96 GiB) for measured all-options raw throughput and storage; only this VM is used.
- Lima dc-acceptance setup: chrony is the sole clock setter; remove CAP_SYS_TIME from lima-guestagent via its chrony-clock-owner.conf systemd drop-in after observed host-agent steps repeatedly prevented alignment; collector clock policy remains unchanged.
- Lima chrony reference freshness: cap source maxpoll at 7 (128 seconds), persisted in chrony.conf with its prior version retained, after the adaptive 1,024-second poll interval exceeded the collector's 300-second reference-age limit; no policy relaxation or clock step.
- Full-universe scaling: one-process CPU saturation caused documented Deribit heartbeat/too-slow closes; use four disjoint CLI workers from one verified frozen discovery with a shared one-request-per-second subscription budget, supervisor-death guards and exhaustive aggregate verification.
- Full-universe probe: per-message fsync and repeated whole-plan set comparisons were throughput bottlenecks; bounded FIFO group commit preserves receipt order, clock barriers and fsync-before-ack, and plan equality is skipped after initialization.
- Full-universe rotation: the first hour attempt stopped on the 5-second durability deadline during large-segment sealing; raw segments now target a configurable 8 MiB rather than 64 MiB, with the deadline unchanged.
- Historical all-options plan freezes every listed option in the startup discovery (including returned inactive states); uses book/ticker/trades 100ms, batches at most 500 channels and at most one subscription request per second on one healthy socket; quiet trades require ACK, not a fabricated initial trade.
- All-options hour evidence: `~/dc-all-options-data/runs/467b3e8616744e1cb69dff8e2f6855b0/all-options` and its parent discovery in dc-acceptance; config, 1-second resources, chrony reports and summary under `~/dc-all-options-evidence/20261030T172150Z/`.
- Historical futures/index selection: freeze every discovered native future, including perpetual and coin/linear variants, plus each metadata price_index dependency; shared raw envelopes use multiplex product and retain exact native topics/data rather than labelling index messages as futures.
- Futures/index hour evidence: `~/dc-futures-data/runs/4c65d28370dd43038df8dd800801b234/continuous` and its parent discovery in dc-acceptance; config, chrony reports, resources, summary and successful verification under `~/dc-futures-evidence/20261030T185115Z/`.
- Reconnect pacing follows FEEDS.md after the observed DNS outage: exponential 1/2/4…60-second backoff with jitter, reset after full initialization; stop and supervisor-death checks continue during the wait.
- Binance discovery: request showPermissionSets=false after measuring a 17.8 MB spot response; preserve all BTC/ETH rows and native filters, mark non-trading rows explicitly, and leave unavailable spot listing incarnation null rather than infer it.
- Binance discovery evidence: `~/dc-binance-data/runs/de94c7c122b949bd883599c49fbea549` in dc-acceptance; config, output and successful verification under `~/dc-binance-evidence/20261030T201903Z/`.

- Continuous clock sampling: remove both sampler and durable-assessment count caps in the raw path; preserve finite library defaults and bounded clock epoch history.
- Continuous capture: keep healthy sockets open; rebind archive ownership after 64 closed/failed connections, between retries.
- One-hour acceptance on 30 October 2026: first/last WebSocket receipt 18:18:56–19:18:55 Bangkok time; 1,338 BTC and 946 ETH book messages. Collector RSS sampled every 30 seconds stayed at 118,276–118,292 KiB after startup (16 KiB increase); startup peak was 172,540 KiB.
- Evidence stays inside `dc-acceptance`: `~/dc-data/runs/a46be2c3649e4d7d910b13e94b1abb59/continuous`; retain its parent discovery run. Config, chrony reports, memory samples and verification are under `~/dc-acceptance-evidence/20261030T111739Z/`; the local non-raw summary is `experiments/results/lima-acceptance-20261030/summary.json`.
- Timing evidence: 123 archived clock assessments included 91 aligned and 32 unaligned (warmup, wall steps or reference uncertainty). These labels are retained in raw capture; the one-hour result does not certify continuous UTC alignment.
- Separate shutdown probes: SIGINT during warmup stopped cleanly with a failed/incomplete result and a verified raw prefix; a later startup timed out at 180 seconds after measured wall steps and uncertainty above 100 ms. Clock policy was preserved; live SIGINT after book acquisition was not observed, while cancellation/reconnect fixtures passed.

## Research

Research feasibility now has actual constructed data and fitted models, superseding the earlier API-only consultation. See [measured sufficiency and limits](docs/RESEARCH_DATA_SUFFICIENCY.md), [label-quality evidence](docs/research_label_quality_metrics.json) and the [companion notebook](experiments/research_data_sufficiency.ipynb).

- [x] Inspect primary-paper equations and reconcile independent methodology/source/skeptical reviews.
- [x] Predeclare a 72-date historical sample and chronological partitions; preserve 288 replay responses and causal historical cohorts.
- [x] Construct 2,513 same-contract one-hour observations and calculate explicit BS/SM plus a training-calibrated HW-style benchmark.
- [x] Fit a small residual ML model with training-only normalization and chronological validation/evaluation.
- [x] Investigate surprising labels: 22 wide exit quotes dominate broad-panel error. Construct an entry-OTM subset of 1,335 observations, with scores explicitly exploratory after the broad evaluation was viewed.
- [x] Download/audit a full free session: ten focal contracts, 16 consecutive hourly states, 150 pairs and ten complete 15-hour gross dynamic ledgers. Reconcile overlapping prices with replay data.
- [x] Assess effective time blocks, learning-window sensitivity, quote-band uncertainty and source access/size constraints. No ML-superiority or universal sample-sufficiency claim.
- [ ] Freeze the user's final adjusted-delta equation, horizon, accounting, valuation and research population before formal model comparison.
- [ ] If broader claims require continuous everyday data, qualify the corresponding source/hedge history and access terms; current free dates are isolated sessions.
- [ ] Reserve a fresh final holdout after the exploratory design choices; add funding/fees/execution assumptions only with matching evidence.

The investigation supports a scoped BTC small-model study; it does not complete the eventual research dataset or prove adequacy for every ML specification. No account, trial, subscription, publication or deployment was performed. These research steps remain separate from collector production acceptance.
