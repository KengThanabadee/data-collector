# Data Collector (DC) — progress

V1 implementation follows the preserved [SPEC v1](docs/SPEC_V1.md). V2 design follows [SPEC.md](SPEC.md). Source repository: [KengThanabadee/option-collector](https://github.com/KengThanabadee/option-collector) (private). Deployment and recurring collection have not been performed.

## V2 runtime implementation — active

The user authorized chunked implementation with a fresh **high-reasoning** agent review after each chunk. Follow [chunk progress and exact review evidence](docs/v2/IMPLEMENTATION.md).

- [x] Chunk1 foundational contracts/metadata/v1 compatibility: implemented, tested and approved after review fixes.
- [x] Chunk2a bounded durable WAL/tail recovery: implemented, tested and approved after review fixes.
- [x] Chunk2b immutable segment publication/verification and persistent seal: implemented,29scoped tests pass, high-reasoning review approved.
- [x] Chunks3a–3c: native decimal book projections, bounded bootstrap and depth decoders for reviewed profiles; fresh high-reasoning reviews approved.
- [x] Chunks4a–4b: pinned correction projection and durable local allocation/closure barriers; fresh high-reasoning reviews approved.
- [x] Chunk4c: durable correction ledger with exact raw evidence and verified restore; high-reasoning review approved after fixes.
- [x] Chunk4d: local deterministic book snapshot materialization;121 v2 tests pass; high-reasoning review approved after fixes.
- [x] Chunk4e: immutable local snapshot publication, pinned dependency reads and process-exit recovery; high-reasoning review approved after fixes.
- [x] Chunk4f: portable original/assessed exports with pinned ledger replay; high-reasoning review approved.
- [x] Chunk4g: session-wide WAL rotation/recovery with pre-admission resource bounds; high-reasoning review approved after fixes.
- [x] Chunk5a: retained-byte venue queues and priority shared REST quotas; high-reasoning review approved after fixes.
- [x] Chunk5b: serialized per-venue archive worker, durable raw acknowledgements, emergency loss controls and clock-fault regressions; high-reasoning re-review approved.
- [x] Chunk5c: measured clock-health epochs, freshness and two-sample recovery; shared monotonic regression guards reviewed and approved.
- [x] Chunk5d: fsynced clock measurement/assessment pairs and pinned deterministic replay; independent high-reasoning review and real fsync-failure probes approved.
- [x] Chunk5e: measured clock requests share the bounded async raw FIFO and disk owner; independent high-reasoning review approved cancellation, failure, overflow and replay tests.
- [x] Chunk5f: durable paired clock freshness checks, cumulative-drift protection and legacy log replay fixture; independent high-reasoning re-review approved both repairs.
- [x] Chunk5g: async paired clock checks through the raw FIFO, shared operation caps and failure pin reporting; independent high-reasoning review approved.
- [x] Chunk5h: synchronous clock-bound local cutoffs and snapshot clock-dependency verification; independent high-reasoning re-review approved policy/deadline/namespace/bounds fixes.
- [x] Chunk5i: separate process integrity timing from measured alignment epochs; independent high-reasoning review approved,261 v2 tests pass.
- [x] Chunk5j: stamp received raw epochs from preceding committed FIFO clock controls; independent high-reasoning review approved.
- [x] Chunk5k: continue measured-mode raw capture through UTC wall steps with monotonic integrity/deadline guards; independent high-reasoning review approved.
- [x] Chunk5l: clock-bound cutoff allocation/closure commands share the raw FIFO; independent high-reasoning review approved,315 combined v1/v2 tests pass.
- [x] Chunk5m: seal and pin immutable snapshot inputs through the raw FIFO while capture stays open; independent high-reasoning review approved,287 v2 tests pass.
- [x] Chunk5n: bounded separate snapshot publisher and durable deadline/clock/knowledge outcome receipts; persistent UTC-step invalidation repaired, high-reasoning re-review approved,341 combined tests pass.
- [x] Chunk5o: archive canonical publisher reports through raw FIFO with strict replay and missing-report accounting; fresh high-reasoning review approved,354 combined tests pass.
- [x] Chunk5p: bounded recurring minute cutoffs with leased publication handoff and durable stop/backlog evidence; fresh high-reasoning review approved,365 combined tests pass.
- [x] Chunk5q: own timer/seal/publish/raw-report consumer with durable lease acknowledgement and deferred cancellation; fresh high-reasoning review approved,376 combined tests pass.
- [x] Chunk5r: pinned cross-process cutoff reconciliation, interrupted allocation resolution and durable recovery marker; output-headroom repair independently approved,392 combined tests pass.
- [ ] Startup prefix selection, runtime cutoff resume and automatic correction mapping remain pending.
- [x] Chunks7a–7c: reviewed bounded Deribit raw/archive/book replay experiments; first two20s runs kept as failed delta gates;90s sample passed with BTC12/ETH27 advancing deltas and equal raw replay.
- [ ] Remaining feed models/trades/references/owner transitions, aggregate snapshot integration/checkpoints, continuous capture/CLI, backup/restore and full acceptance remain in the original plan.

Current392 combined tests pass:357 v2 and35 v1. This is actual partial v2 code, not full collection readiness. No deployment or unbounded capture is running.

## Cryptofeed reuse evaluation — decision recorded

- [x] CF01: isolate pinned Cryptofeed3.0.2, verify exact offline raw/book replay and abrupt-exit behavior; fresh high-reasoning review approved.
- [x] Retain our collector as the production v2 core; no Cryptofeed dependency or module replacement. See [decision](docs/v2/CRYPTOFEED_EVALUATION.md).
- CF02 live qualification is deferred. Preserve the offline experiment as reference; continue the original runtime implementation and full acceptance scope.

## V2 specification and implementation handoff — 23 October 2026

- [x] Adopt the reviewed three-exchange Data Collector/DC scope; keep downstream BSAD/direct-ML research separate.
- [x] Preserve the exact v1 specification and keep v1 package/CLI/services/archive paths unchanged.
- [x] Write SPEC v2: raw/identity/point-in-time metadata, feed state, shared-time replay, bounded durability, gaps, backup/restore and migration contracts.
- [x] Freeze the official-documentation [feed catalogue](docs/v2/FEEDS.md), capability states and per-feed evidence gates. Current Binance options routes/schema identified; live seed/bridge/recovery readiness remains gated.
- [x] Freeze proposed [resource configuration](docs/v2/pilot-budget.toml) and measurable [acceptance criteria](docs/v2/ACCEPTANCE.md). Values are test targets, not capacity results.
- [x] Repair the reviewed Bybit full-book sequence/seed rules and define durable cutoff closure plus append-only validity corrections; two independent agents re-reviewed the saved fixes and approved the specification; see [closure evidence](docs/v2/REVIEW.md).
- [ ] Complete remaining coverage/replay integrations; raw contracts, journal/segments and v1 compatibility foundations are implemented and reviewed.
- [ ] Complete venue-isolated adapters and quota coordination; bounded journal/segment writer/recovery and local fault harness are implemented and reviewed.
- [ ] Resolve enabled-feed schema/identity/bootstrap gates with bounded public captures; archive exact source/schema versions and raw fixture evidence. Binance options, CM, Bybit full/RPI/spread profiles require their own evidence.
- [ ] Measure the complete BTC/ETH target universe; freeze full-release host/storage/network configuration without silently reducing scope.
- [ ] Complete faults, independent backup/restore and at least 24-hour intended-host soak before production claims.

No v2 runtime, account, trial, purchase, service activation, deployment, repository rename, commit or push was performed in the spec phase. Documentation approval does not certify adapter readiness. The existing v1 and research progress below is retained as historical/current evidence, not streaming-v2 acceptance.

## V1 implementation

- [x] Core: validated configuration, async transport, frozen universe, bounded collection, reader.
- [x] Durability: journal, verified publication, recovery, lock, slot events and gap accounting; local fault tests.
- [x] Daily delivery: pagination, clean yield/resume, shared quota and snapshot priority; simulated52-page workload and cursor-loss recovery.
- [x] Operations prepared: CLI, monitoring, export, systemd units, installer and short operations guide. Linux runtime validation remains pending.
- [x] Local validation: fixtures, HTTP fault server, crash tests and one full production public-API snapshot capture (BTC884/884,ETH766/766).
- [x] Fix the six independent implementation review findings; add regression coverage and verify existing archives unchanged. See [fix evidence](docs/IMPLEMENTATION_REVIEW_FIXES.md).
- [ ] Linux validation and actual VPS: pending a Linux host; no production-readiness claim before the specified 24-hour soak.

Optional candle backfill follows the core snapshot rollout; it does not block starting prospective collection.

See [local validation evidence](docs/LOCAL_VALIDATION.md) for the exact tests, captured archives and remaining acceptance work. The reviewed spec is preserved unchanged; these checkboxes describe implementation progress rather than a production-readiness certificate.
