# Data Collector (DC) — plan

V1 follows [SPEC v1](docs/SPEC_V1.md). V2 follows [SPEC.md](SPEC.md). The chunk-by-chunk log up to 28 October 2026 is archived in [docs/v2/history/](docs/v2/history/PLANS_2026-10.md).

## Current state — 30 October 2026

- V1: Deribit REST snapshot collector with CLI, archive reader and systemd units. One full public-API capture ran locally (BTC 884/884, ETH 766/766). It has not run on a Linux host yet.
- V2: continuous raw Deribit option capture passed a one-hour run on Ubuntu 24.04 in Lima `dc-acceptance`, using chrony and a separate Python 3.12 venv under `caffeinate -i -s`. BTC-13NOV26-74000-P and ETH-13NOV26-2000-P ran for 3,601.3 seconds with one connection: 2,961 WebSocket messages, 60 immutable segments, and 3,543 verified records. `verify-run` passed; duration, frame and byte limits remain configurable and unbounded by default. Bounded snapshot mode remains available; see [running instructions](docs/v2/RUNNING.md).

## Next

1. Run that capture for 24 hours on the intended Linux host with chrony, and record what failed.
2. Add the remaining SPEC.md venues and feeds as raw capture with gap records, one feed at a time.

## Notes

One line per deferred idea or choice made while working.

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
