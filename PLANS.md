# Data Collector (DC) — plan

V1 follows [SPEC v1](docs/SPEC_V1.md). V2 follows [SPEC.md](SPEC.md). The chunk-by-chunk log up to 28 October 2026 is archived in [docs/v2/history/](docs/v2/history/PLANS_2026-10.md).

## Current state — 30 October 2026

- V1: Deribit REST snapshot collector with CLI, archive reader and systemd units. One full public-API capture ran locally (BTC 884/884, ETH 766/766). It has not run on a Linux host yet.
- V2: `config-check`, `discover`, `capture` and `verify-run` work for Deribit option books; see [docs/v2/RUNNING.md](docs/v2/RUNNING.md). A real discovery run captured 2,653 metadata rows and read them back. `capture` now has a raw mode with unbounded default limits, reconnect gaps and hourly owner rollover; bounded snapshot mode remains available. Live one-hour Linux acceptance is still pending.

## Next

1. Continuous Deribit option capture.
   ```
   Goal: Collect Deribit option books continuously.
   Done when: `capture` runs for one hour on selected BTC/ETH options with duration, frame and byte limits configurable and unbounded by default, reconnects after connection loss, stops cleanly on Ctrl-C, and `verify-run` passes on the result.
   Out of scope: other venues and feeds, new snapshot or correction features, clock handling beyond what capture already does.
   When unsure: pick the simplest option consistent with SPEC.md, add one line to PLANS.md Notes, continue.
   ```
2. Run that capture for 24 hours on the intended Linux host with chrony, and record what failed.
3. Add the remaining SPEC.md venues and feeds as raw capture with gap records, one feed at a time.

## Notes

One line per deferred idea or choice made while working.

- Next 1 implementation: raw capture reuses the measured clock/archive components, reconnects with gap records and rolls owners hourly; synthetic validation and review recorded in the implementation commit.
- Next 1 remains open until a real one-hour BTC/ETH capture on Linux with local chrony finishes and `verify-run` passes; this macOS workspace has no chrony or Docker.
- Raw verification covers immutable segment integrity, receipt ordering and payload counts; derived replay stays in bounded snapshot mode.

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
