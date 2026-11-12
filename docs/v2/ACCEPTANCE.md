# DC v2 acceptance and resource release contract

Current goal (8 November 2026): the acceptance for the 1-minute grid is the one-hour run across 08:00 UTC defined in [COLLECTION_SCOPE.md](COLLECTION_SCOPE.md). It replaces the 31 October expiry-window gate. The gates below are the later production release contract. On the minute grid, coverage is counted per expected request per minute (ok, error, timeout or missed), and the receive→durable, snapshot-publication, ACK and bootstrap targets apply only to the V2 event streams. Off-host backup and the soak wait for the VPS, which is decided last.

Profile `dc-v2-bounded-pilot-20261023.1` in [pilot-budget.toml](pilot-budget.toml) is a **proposed test configuration**, not an installed/runnable config for the v1 application. No throughput, disk/day, streaming-host size or recovery result is certified yet. Production cannot use this disabled pilot unchanged: freeze a full-universe release configuration with concrete endpoint limits, symbol list, host, storage/backup destination and known gates resolved. Keep all prior runs and configuration hashes.

## 1. Scope and measurements

A bounded pilot stops at the first of2h,5GiB ingress,10GiB archive growth or any hard safety threshold; commits what it can and reports stop reason. Trial at most100instruments/venue with separate caps64options/4spot/16outright derivatives/8spreads; these caps sum92 and leave headroom for dependent references within100. It is not an acceptance sample for full BTC/ETH coverage. Freeze concrete symbols from current discovery and all profile variants under test before each run. A missing product/expiry is an explicitly untested path.

Capacity gate later measures the **entire** target universe and dependent references, with no silent selection/depth/cadence change. Measure at1s resolution: ingress messages/bytes, compressed and uncompressed durable bytes, REST weights/latency, CPU/user/system, RSS, queue bytes/age, bootstrap buffers, disk/write/fsync rates, receive→apply, receive→durable and snapshot publish lag. Report per connection/feed, venue/product, and aggregate p50/p95/p99/max. Native-event→receive latency is a separate diagnostic affected by clocks/feed cadence; it is not receive→persist lag.

Candidate hard bounds: aggregate RSS4GiB, venue896MiB each, writer512MiB and replay512MiB within aggregate; queues192MiB aggregate/64MiB per venue; bootstrap16MiB per connection/64MiB aggregate;16MiB application message and32MiB REST body;64MiB aggregate in-flight REST. These overlap in accounting, not additive exemptions from RSS. Include parser expansion, compression, Python allocations and in-memory book state in RSS. Enforce whole service/cgroup limits as well as component limits; on a limit, loss evidence and invalidation are required. A full book too large for limits blocks that profile and requires a new sized configuration.

CPU candidate ceiling p95≤70% of allocated host CPU; RSS must stay within hard bounds without monotonic leak over soak; queue p99≤50% and no unreported overflow. Warning at75% queue; stop/resync at cap or backlog5s. A load increase may cause a failed test; do not silently drop topics. Recovery must reach≤25% queue before resubscription. Source identities, configurations and explicit reconnect epochs stay fixed for reproducibility.

Storage warning20% free; stop if free<10% **or**<5GiB. Resume only≥20% and≥10GiB with journal+compression temporary files+anchors/control reserve headroom. Stream verify without loading a complete segment into RAM. Predict disk days from measured peak rolling-hour compressed rate, manifests/control/checkpoints, temporary amplification and backlog; require≥7days local headroom in the chosen full-profile storage allocation. Also report the uncertainty of extrapolating a day/week from a short sample. No raw deletion to satisfy the test.

## 2. Coverage denominators and pass criteria

Define expected universe/feed intervals from archived discovery, lifecycle and profile selections. Keep time spent bootstrapping/resyncing, local overload, provider outage and access failure in coverage denominators. A feed documented as unsupported or intentionally disabled is outside the *enabled-feed* denominator but remains visible in the full target-scope report. Predeclared exchange no-book/closed states are separate unavailable states, not good quotes or unexplained loss; publish both calendar and eligible denominators. Listing discovery uncertainty is separately visible.

- `raw_integrity`: all committed file/body hashes and record counts verify; no loss of acknowledged durable records.
- `subscription_coverage`: acknowledged expected subscriptions / expected selected subscriptions, plus unacknowledged reasons and durations.
- `reconstructable_coverage`: eligible instrument-feed seconds with available anchor and permitted continuity confidence / expected eligible instrument-feed seconds. Report sequence-proven and health-only separately; do not merge them under a claim of proven gaplessness.
- `snapshot_validity`: per-cutoff/per-instrument/feed status and depth/reference availability, plus the strict intersection across all required feeds. Never replace missing quotes by marks or exclude failed contracts from denominators.
- Target full-profile soak≥24h; reconstructable coverage≥99.5% **per enabled product/profile and overall**, snapshot publication≤5s for≥99%cutoffs, receive→apply p99≤1s and receive→durable p99≤2s. Any>5s episode invalidates the affected interval and counts as a threshold breach to investigate; no unexplained loss is acceptable regardless of the percentage.
- ACK≤10s and bootstrap≤30s per attempt after connection established; incomplete attempts are invalid and retry within the bounded circuit breaker. Two pending healthy clock checks after a step precede restored time alignment. Measured |offset|≤100ms is required for time-aligned snapshots.

These are chosen acceptance targets, not exchange SLAs or results. A blocked feed is reported blocked and cannot pass by omission. If all-three-venue readiness is claimed, every target row must have passing evidence or an explicitly documented unsupported capability; `not_verified` or unavailable required credentials keep that claim blocked.

## 3. Mandatory test evidence

| Gate | Required demonstration and exact pass condition |
|---|---|
| A0 Source/config freeze | Capability manifest expands every target family, cadence/depth/timestamps/limits complete; frozen source/schema bytes+hashes, versioned universe and no unresolved enabled row. Record actual auth/network/access context. |
| A1 Native bootstrap | Each chosen adapter/profile has saved ACK, initial anchor, first delta and ≥one later delta; expected books from known fixtures. BO/YF and CM bridge gates in FEEDS must pass before activation. |
| A2 Fault continuity | Inject loss, duplicate, conflicting duplicate, out-of-order, reset, re-list/symbol reuse, parser failure and disconnect. Exactly the intended stream scope becomes invalid from last proven position; no silent healing or cross-venue restart. |
| A3 Trade semantics | Fixtures for maker/aggressor mapping, aggregate-vs-individual IDs, shared seq batches, block/RPI and UM/CM double delivery. Raw preserves all; derived includes expected unique native events and flags. Unsupported side stays unknown. |
| A4 Clock/as-of | Execute every closure/clock/crash case in [SNAPSHOTS.md](SNAPSHOTS.md): persist cutoff-close barriers and admission vectors, backward step across published cutoff, delayed timer, quiet feed and restart. No later low-UTC arrival enters an old cutoff; raw/checkpoint hashes and original membership agree. Incomplete closure is missing/invalid. |
| A5 Durability | Kill before/after fsync, compression, rename and manifest commit; corrupt/truncate tail, disk-full/slow writer and oversized payload. All durable prefixes survive, bad tails preserved, no mismatched overwrite, repeat recovery idempotent. Test on intended filesystem; power-loss claim requires separate evidence. |
| A6 Bounded resources | Saturate queues/bootstrap and verify configured RAM/RSS enforcement, per-venue isolation, emergency logging and invalidation. Replay/compression/verification fit limits, including largest selected snapshot. |
| A7 Replay/metadata | Raw/checkpoint hashes identical across segments/sessions/reset at equal closure/ledger bounds, in both as_published and assessed_at_ledger modes. Late-gap correction, open gap across restart, old/new pinned exports, owner transition and correction conflict fixtures in SNAPSHOTS.md pass. Missing dependencies/prefix fail assessed read. Point-in-time metadata/symbol reuse pass; exports do not modify original archives. |
| A8 Legacy read | Preserve original v1 fixture/export SHA256; old reader behavior and compatibility `legacy_observation` output verified, no invented shared-time/stream continuity. Original v1 spec hash matches. |
| A9 Off-host restore | Copy committed data/anchors/metadata plus cutoff allocation/close records, owner transitions and pinned correction-ledger prefix/open gaps to independent storage; on clean root without capture disk read back SHA256 and replay equal snapshots.≤10GiB predeclared restore≤4h, resume new capture anchor≤1h; backup committed-data lag≤15min. |
| A10 Full release soak |≥24h on intended Linux host and frozen full-target configuration after fault gates; publish all rates/coverage/limits/gaps/reset evidence. List untested families and measure projected storage/network. No aggregate metric masks a failed product/profile. |

Fixtures can represent listings/resets where live occurrence is unavailable; label simulated vs actual separately. Small live public captures confirm routing and schema only, not complete live lifecycle/production readiness. Zero trading orders are necessary for all gates.

## 4. Release artefacts and remaining work

Implementation must produce `release-config.toml`, `feed-manifest.json`, `universe.json`, exact `source-bundle/`, fixture expectations and results, gap/coverage/correction ledger, cutoff-close records, resource summaries, backup/restore report and soak report. Hash these into archive provenance; strip secrets. Any enabled profile's unresolved bridge/limits/identity field is a hard preflight failure, not a warning.

The v2 spec phase delivers these contracts and the frozen documentation catalogue. It does not deliver live fixtures, a chosen paid machine/storage account, feed credentials, measured full-universe capacity or acceptance results. Next implementation work starts with common raw/coverage contracts and compatibility fixtures, then bounded adapters and the enumerated feed verification gates. Direct BSAD versus direct ML remains a downstream research specification.
