# Data Collector (DC) — plan

V1 follows [SPEC v1](docs/SPEC_V1.md). V2 follows [SPEC.md](SPEC.md). The chunk-by-chunk log up to 28 October 2026 is archived in [docs/v2/history/](docs/v2/history/PLANS_2026-10.md).

## Current state — 31 October 2026

The [local collection scope](docs/v2/COLLECTION_SCOPE.md) supersedes the earlier full-product queue. Existing measurements below describe historical profiles, not acceptance of the new profile. New-profile runtime and expiry-window acceptance remain pending.

- V1: Deribit REST snapshot collector with CLI, archive reader and systemd units. One full public-API capture ran locally (BTC 884/884, ETH 766/766). It has not run on a Linux host yet.
- V2: raw Deribit book, ticker and trades at 100ms ran for 3,601.137 seconds after all 4 workers initialized, covering all 2,800 BTC/ETH options in the startup discovery (8,400 channels). The Ubuntu 24.04 Lima `dc-acceptance` run used chrony, its separate Python 3.12 venv and `caffeinate -i -s`; capture completed with 10,993,156 WebSocket messages and 7,149,854,234 native payload bytes. Full `verify-run` passed with 10,995,639 records and exhaustive feed-plan initialization coverage. Continuous limits and bounded snapshot mode are described in [running instructions](docs/v2/RUNNING.md).
- Capture resource samples every second: peak summed process RSS 492,284 KiB (480.75 MiB), peak CPU 403.44% (100% is one vCPU; VM has 6). Summed RSS was 456,100 KiB near startup and 452,836 KiB before shutdown.
- Venue/transport results: zero RPC rejections or throttles; 3 server closes with code 4000 (`heartbeat close`), 5 CLOSED events without a code, and 94 DNS-failed connection attempts. All workers recovered and reacquired their feed plans. Recorded DNS gap windows were 218.30–224.55 seconds per worker, plus shorter reconnect gaps; timeout detection can follow actual loss. The run has explicit coverage gaps.

Observed linear projections divide all captured bytes, including initialization and network gaps, by the post-ready elapsed time. Retained worker data includes WAL and archive; it excludes shared discovery/aggregate metadata. These are logical file sizes, not allocated filesystem blocks.

| Byte basis | Bytes per hour | Projected GiB per day |
|---|---:|---:|
| Native WebSocket payload | 7,147,595,853 | 159.76 |
| Compressed immutable archive and manifests | 2,755,437,912 | 61.59 |
| All retained worker data, including WAL and archive | 24,208,069,497 | 541.09 |

- V2 futures/index: all 46 discovered BTC/ETH native futures (4 perpetuals and 42 dated contracts) and their 4 metadata index dependencies (142 channels) passed a 3,600.635-second post-initialization run in dc-acceptance under `caffeinate -i -s`; `verify-run` passed 823,276 records, including 822,679 WebSocket messages and 492,094,910 native payload bytes. All book/ticker/trade 100ms channels and native index topics were initialized. Three connections initialized successfully after two code 4000 heartbeat closes; recorded gap windows were 2.604 and 2.560 seconds. RPC rejections and throttles were zero.
- Futures/index 1-second resource samples: peak summed RSS 150,504 KiB (146.98 MiB), peak CPU 101.15% (100% is one vCPU); RSS was 113,004 KiB after initialization and 113,024 KiB before shutdown. The same observed-rate basis applies; retained capture data excludes the parent discovery.

| Futures/index byte basis | Bytes per hour | Projected GiB per day |
|---|---:|---:|
| Native WebSocket payload | 492,008,069 | 11.00 |
| Compressed immutable archive and manifests | 220,160,167 | 4.92 |
| All retained capture data, including WAL and archive | 1,771,514,629 | 39.60 |

- Binance discovery ran in dc-acceptance and passed raw archive/projection verification for spot, UM and CM: 69 BTC/ETH spot rows (31 feed-enabled), 13 UM and 6 CM futures. Its broad continuous capture prototype is unfinished and superseded by the configured USDⓈ-M profile; new-profile acceptance is not complete.

## Next

Option universe rediscovery:
```
Goal: Keep BTC/ETH option subscriptions current during a run, including new listings, Bybit PreLaunch and expiry retirement.
Done when: Deterministic capture fixtures show archived lifecycle/discovery changes, subscription within 120 seconds, retirement, reconnect recovery and verify-run coverage of changing universe versions; the final cross-venue expiry-window run confirms live timing.
Out of scope: derived snapshots/replay, VPS setup and dropped feeds.
When unsure: pick the simplest option consistent with SPEC.md, add one line to PLANS.md Notes, continue.
```

1. Implement option rediscovery with Deribit instrument.state plus 300-second REST audit, Bybit 60-second discovery including PreLaunch, and append-only universe/subscription timing evidence; review and commit.
2. Deribit options top-20 book snapshots, ticker, per-kind trades and BTC/ETH index; drop futures. Write own brief, review and commit.
3. Binance USDⓈ-M configured list initially BTCUSDT/ETHUSDT: bookTicker, depth20@100ms and aggTrade. Reuse applicable unfinished Binance work; drop spot parts. Write own brief, review and commit.
4. Bybit BTC/ETH option tickers and publicTrade, one connection per base coin with native args/count limits and settlement fields. Write own brief, review and commit.
5. Run the selected feeds together in dc-acceptance under caffeinate -i -s for at least one hour, starting by 07:25 UTC and continuing past 08:00 UTC; pass verify-run and record every Acceptance item in COLLECTION_SCOPE.md. Prior runs do not satisfy this gate.
6. Wait for a VPS before the later 24-hour production gate.

## Notes

One line per deferred idea or choice made while working.

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
