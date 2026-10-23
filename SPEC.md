# Data Collector (DC) — Specification v2

Specification baseline: 23 October 2026, Asia/Bangkok. Status: implementation-ready contracts with explicit adapter evidence gates; **v2 runtime is not implemented or production-certified**. Scope approval comes from the [reviewed handoff snapshot](docs/v2/HANDOFF_BASELINE.md). This specification governs future work; it does not enable any capture, deploy a service or change the running v1 program.

## 1. Outcome and scope

Build a reusable public market-data archive for **Deribit, Binance and Bybit**, initially underlying BTC/ETH. Continuously capture WebSocket messages, REST bootstrap/recovery/reference/history where available, reconstruct books and produce market states at a shared reference time. Preserve native payloads, provenance, units, historical metadata, capture gaps and reproducible replay.

Target products: spot, perpetuals, dated futures, options, and exchange-listed combos/spreads. Include inverse/linear, quote/settlement currency variants discovered under BTC/ETH; do not equate BTC-underlying with BTC-settled. Include a spread when all underlying legs are within BTC/ETH scope and public metadata identifies them. Reference assets needed for those contracts may be collected with an explicit dependency reason. A missing product/feed remains visible as unsupported, access-required or unverified, rather than silently reducing the target scope.

Books, trades, tickers, mark/index/pricing forwards, IV/Greeks, open interest, lifecycle, funding and settlement/delivery are target families. Public liquidation/risk statistics and RPI are separately labelled profiles. Each venue/product has independent supervision and loss accounting. Analytics, BSAD/direct ML model design, feature fitting, trading, private account data and execution simulation remain downstream.

The [feed catalogue](docs/v2/FEEDS.md) is normative for routes, identity, continuity and capability. The [acceptance profile](docs/v2/ACCEPTANCE.md) and [candidate configuration](docs/v2/pilot-budget.toml) freeze measurable trial budgets. Live access/fixtures are subsequent implementation gates, including Binance options; documentation support does not mean adapter readiness.

## 2. Compatibility and project name

The desired product name is **Data Collector**, abbreviation **DC**. Keep the current checkout and GitHub repository `option-collector` during this specification phase. Preserve the exact [v1 specification](docs/SPEC_V1.md), SHA256 `ff908b923069d7bd213d1065a073e5d62edf412576946fd22b811d6e0020ec34`. Relative links inside that historical copy are retained as original text; current navigation uses README and this specification.

| Surface today | Planned v2 change and compatibility |
|---|---|
| `pyproject.toml`: distribution `option-collector`, package `option_collector` | Add `data_collector` and `data-collector` entry point in implementation; keep `option_collector`, `python -m option_collector` and `option-collector` as v1 compatibility paths. No import rename in this task. |
| `collect-once`, `delivery`, `backfill-candles`, `status`, `verify`; `read_archive`/`export_archive` | Preserve existing semantics and schema-v1 reader. New commands `capture`, `discover`, `replay`, `snapshot`, `verify-stream`, `backup`, `restore-check`, `coverage` are versioned v2 contracts, not available commands yet. |
| `config/local.toml`, `config/vps.toml`, `/etc/option-collector/collector.toml` | Read v1 with its existing parser. Separate v2 config version and future `/etc/data-collector/collector.toml`; reject cross-version use clearly. No silent reinterpretation. |
| `data/production`, `/var/lib/option-collector` | Keep old archives read-only in place. New archive root `/var/lib/data-collector/v2` only when deployed explicitly; version dispatcher opens both. No mass move or recompression. |
| `/opt/option-collector/current`, service user/group and journald namespace | Remain valid for v1. Plan separate `/opt/data-collector/current`, `data-collector` user, log namespace and state directory; explicit permissions/copy verification before cutover. |
| `deploy/systemd/option-collector.service` + timer; delivery/monitor units; installer/health config | Retain v1 units. Add `data-collector@<venue>.service` continuous supervisor, backup and monitoring timers. Do not repoint the oneshot timer to streaming. Explicitly disable duplicate owners before cutover. |
| GitHub/checkout name | Optional later separate migration; verify remotes, links, CI and installer paths then. No repository rename requested as part of this task. |

Do not have v1 and v2 write the same root. Old rolling REST sweeps retain `rolling-sweep` meaning and observation span: they cannot become atomic snapshots by changing a reader label. A migration acceptance test reads an unchanged v1 fixture/export and verifies original hashes and returned values. Compatibility adapter emits `legacy_observation` with native timing and absent streaming coverage marked unknown.

## 3. Universe, instruments and metadata

Instrument key is `(venue, environment, native_product, native_symbol, listing_incarnation)`. Alias/base/quote/settlement/underlying are attributes, not the key. Record routing/actual matching venue, contract size/multiplier, inverse or linear, price and quantity units, tick/lot, expiry/strike/right, combo legs/ratios, states and supported feed profiles. All numerics retain native strings/lexemes in raw; derived decimal arithmetic uses explicit units.

Discovery stores raw pages/cursors/status and a versioned universe with selected/excluded/reason lists. Bootstrap complete pagination, then reconcile lifecycle notifications immediately. Poll discovery every 60s where no authoritative lifecycle feed exists; Deribit uses `instrument.state.{kind}.{currency}` with a slower 300s REST audit to respect its documented collection guidance. No listing stream means `poll_observed`, not instant discovery. Empty/unrecognized discovery cannot erase the last known universe; mark discovery unhealthy, retain the previous list as stale and report uncertainty. Rate-budget inability to finish a poll becomes a visible overdue event. New listings subscribe within 120s of observation; that is not a guarantee relative to true exchange listing time. Delist/state/tick changes retire or reset the affected stream at the observed time. Unknown symbol or product identity is quarantined, never guessed from host or symbol substring alone.

Point-in-time metadata is append-only bitemporal: `observed_at_receive`, `effective_at` when supplied, acquisition time, source raw ID, revision, superseded version. Historical REST imports keep their later acquisition time. Receive-time replay cannot treat them as information known before acquisition. Maintain settlement/funding revision histories; distinguish predicted, currently published and realized rates. Record null for unavailable fields and provide a field-capability reason.

## 4. Capture contract and adapter boundary

Adapters implement `discover`, `plan_subscriptions`, `classify_message`, `bootstrap`, `apply`, `health`, `reconcile` and bounded `backfill`. They emit native identity/timestamps/sequence information without mutating the raw payload. The common supervisor owns quotas, write acknowledgements, clock state and lifecycle; no adapter bypasses the durable writer or writes directly to analytics tables. One failing venue has no global restart authority.

State machine per feed/instrument: `discovering → subscribing → bootstrapping → live`; failures lead to `invalid → resyncing → bootstrapping`, with `retired` for delisting. A subscription ACK is necessary but not a book snapshot. New connection/session or reset always has its own epoch; a book is live only after the feed's specific anchor and continuity rules pass. No-sequence feeds have `health_only` continuity confidence, never `sequence_proven`.

Archive these records:

- `ws_message`: original application-message bytes, including combined wrapper/arrays and within-message order, opcode/text-or-binary, body hash, stream route and arrival identity. This is lossless application payload, not a claim to record TLS packets or WebSocket fragmentation/compression on the wire.
- `rest_response`: exact response bytes, request method/path/params, start/receive/acquisition times, attempt, HTTP/API status, relevant headers and body hash. All attempts/errors remain present.
- `control`: connect/disconnect, ACK/reject, heartbeat/test response, subscription/universe changes, cutoff-close/failure and validity-correction records, reset/resync, capacity/clock/disk events, writer loss/recovery and graceful stop.
- `metadata_version`, `coverage_event`, `checkpoint`: indexed derivations that point to raw sources. Indexes may be rebuilt; they are not substitutes for source bodies.

Envelope schema `dc.raw/2` requires capture/process/session IDs, connection ordinal and monotonically increasing receive sequence, UTC receive time, local monotonic nanoseconds and clock epoch, actual native timestamp fields with units/precision, native sequence/checksum fields, venue/environment/product/profile, universe/metadata/config/source-bundle hashes and raw-byte encoding. A multi-symbol message has connection/channel identity plus child identities in an index. Retain whole messages outside requested universe if a shared stream delivers them; expose routing exclusions in derived coverage. Secrets/auth tokens/notification URLs never enter payload journals: archive a redacted auth control record rather than credentials. Public subscription payloads remain exact.

## 5. Ordering, bootstrap and failure rules

Feed-specific rules in [FEEDS.md](docs/v2/FEEDS.md) override any generic assumption. Absolute L2 quantities replace a level; zero deletes where the native feed specifies it. Never sum duplicate snapshots or treat a truncated top-N snapshot as a delta. Preserve duplicate/out-of-order native messages in raw; only the derived state machine rejects/deduplicates them with a recorded reason.

Book reset clears the previous epoch. A sequence hole invalidates from the last provably continuous position to a new accepted anchor. Reconnect does not repair missing history. A REST seed obtained later is valid for recovery from its own acquisition/bridge point, not for retroactively filling the gap. Archive REST request and response plus buffered WS positions and chosen bridge. Timeout, bounded-buffer overflow, conflicting same-ID payloads or inability to prove a bridge causes another resync, never speculative application. If the venue lacks an enforceable predecessor rule, use health-only confidence plus reconciliation; do not invent consecutive sequence semantics.

Trades use native IDs scoped to native product/instrument/incarnation, event type and sequence domain as documented. Store native side fields, maker flags, RPI/block/liquidation flags and map aggressor only when supported by a versioned rule. Unknown side remains unknown. Bybit `seq` alone is not a trade ID. Binance merged arrays can contain UM and CM; derive identity from `st`, metadata and symbol, using `ps` as pair attribute. Raw keeps both delivery paths; derived dedup is by validated native event identity, not matching timestamps/prices. Conflicting duplicates are evidence, not overwritten. Do not pair trades to book deltas one-to-one or infer cancellations exactly from L2 changes.

Backfill is a separate acquisition lane using the same venue quota pool, raw contracts and lower priority than live bootstrap/recovery. Save exact page bounds/cursors, overlaps, no-progress checks and imported time ranges. It can improve event-time historical analysis but cannot assert a live subscription was continuous. Candles, last trades, marks and midpoints have distinct provenance; no synthetic past quote fills.

## 6. Shared-time snapshots and replay

Default policy **`receive-asof/2`** uses UTC minute cutoffs with durable, immutable cutoff-close records and per-connection admission-position vectors. The normative [snapshot finalization contract](docs/v2/SNAPSHOTS.md) defines monotonic barriers, clock epochs, receipt order, crash recovery and publication. A timestamp predicate alone cannot reconstruct a published snapshot. Messages arriving after its barrier never enter that closed snapshot, even if a backward clock step gives them an earlier UTC label. Future exchange-time reconstruction is a distinct profile.

Every snapshot references its close record, admitted/applied positions, original raw/anchor dependencies, metadata/universe version, replay policy and clock state. Replaying raw plus these control records or replaying a checkpoint must produce identical canonical hashes. Within each connection use `receive_seq`, then native child order; never reorder its messages by wall-clock or exchange timestamps. Cross-connection merges use the saved deterministic dependency/tie policy, without claiming exchange causal order. Missing close records, anchors or dependencies cannot yield a valid reconstruction.

Published snapshot bytes are immutable **as-published observations**, not a promise that validity can never be corrected. A subsequently discovered gap appends a durable `validity_correction` with affected stream/position/cutoff bounds, discovery provenance and reason. Verified reads/exports pin a coverage-ledger high-watermark and overlay all corrections through it; they retain invalid/missing rows and expose original and assessed validity. Original bodies are not rewritten or silently removed. Detailed closure, revision and export rules are in SNAPSHOTS.md.

A research publication references only committed, hash-verified raw and close records. A live preview is provisional. Publish by5s after cutoff or record an explicit failed/missing closure; never recreate missing historical cutoffs after restart using current market data. Checkpoints every300s and after resync preserve closure and ledger dependencies. An unchanged book can be valid under its declared continuity confidence; absent bid/ask stays null, reference age is independent and marks/last prices never replace quotes.

Preserve raw UTC and monotonic receipt clocks. Sample offset/uncertainty every60s; any wall-clock step, monotonic regression/reboot or |offset|>100ms invalidates alignment. Recover after two healthy observations in a new clock epoch. Do not reuse a closed UTC cutoff, shift its admission vector or recompute original timestamps. Late discovery of a clock error uses the same correction mechanism; snapshots for missed/ambiguous cutoffs are invalid. Processing lag uses monotonic time within its valid process epoch.

## 7. Durable archive and bounded resources

Partition by schema/venue/environment/date/session, not by mutable instrument attributes. Journal append records with length, checksum and sequence; durable position advances only after fsync, at most 1s between flushes under the candidate profile. The allowed received-but-not-durable tail is explicitly at risk on host power failure; the goal is zero loss of acknowledged durable records. No claim that fsync makes the entire machine infallible.

Rotate at 60s or 64MiB uncompressed journal payload, whichever first; allow one record of bounded maximum size across the threshold. Stream compression/verification with bounded buffers. Fsync files, atomically rename within one filesystem, then fsync parent directories. Commit marker/manifest is published last and references raw SHA256, uncompressed record hash/count, time/sequence ranges, receive epochs, coverage/metadata/universe versions, config/source hashes and replay anchors. A crash between data and marker leaves an orphan to reconcile, never an apparently complete archive. No overwrite of a mismatching object.

On startup scan committed objects and journals; recover valid framed prefix, preserve partial/corrupt tail as evidence, append interruption/gap controls and verify before publication. Interrupted compression/publication can be repeated idempotently. Store manifests/control log with the same durability guarantees. Keep a 256MiB emergency control reserve outside normal quota; if all writes fail, stop ingestion, signal stderr/service health and on restart record uncertainty from last durable watermark. It is impossible to promise a durable error record on a fully failed disk.

Queues are byte bounded, including decoded messages, bootstrap buffers, compression and in-flight REST. On overflow, stop/resync affected connection and mark loss beginning at its last proven continuous position; if streams cannot be identified, invalidate all streams on that connection. Preserve other venues. Oversized frames/responses are capture failures with known bounds and gap evidence, not silently truncated raw. No automatic depth/cadence/universe reduction to stay within budget. A new workload requires a new versioned profile and acceptance run.

## 8. Independent backup and restore

Local checksum verification detects corruption but does not protect against machine loss. Before long-running production, configure an independently administered/off-host object-storage destination, credentials by environment/secret file, encryption/access policy, backup cadence, retention and available storage. Destination must pass an off-host identity check; a second directory on the capture disk does not qualify.

Upload committed immutable segments, manifests, metadata, gap/correction ledger, cutoff-close records, indexes and anchor dependencies at least every 5min; read back/verify SHA256 before marking a remote commit. Do not rely on multipart ETags as SHA256. Snapshot publication does not wait for remote upload, but backup lag over 15min makes archive protection unhealthy. Target RPO ≤15min of committed data; received volatile tail remains separately bounded by local durability. Target RTO ≤4h for a predeclared ≤10GiB restore slice and ≤1h to resume capture from a new anchor on a replacement host. Whole-archive RTO must be measured/sized separately.

No automatic deletion of raw locally or remotely. Storage pressure pauses capture and alerts. Retention cleanup requires a separately approved policy with verified independent copies and replay dependencies protected; age alone cannot delete anchors. Restore to a clean root without the capture disk, verify object hashes, v1 readability where included and canonical replay/snapshot equality. Production gate fails without an actual independent restore test. This spec does not provision storage or purchase anything.

## 9. Acceptance, delivery and readiness

The target universe is the entire discovered BTC/ETH scope, not the bounded trial cohort. Trial caps prove code paths; full-universe capacity must be measured independently. Proposed resource settings and pass/fail rules are frozen in [ACCEPTANCE.md](docs/v2/ACCEPTANCE.md); update configuration version before rerunning, never relax thresholds after seeing a soak result and call the original run passed.

Implementation order:

1. Common schema/identity, v1 reader dispatcher, metadata, coverage/gap ledger and deterministic archive/replay fixtures.
2. Bounded durable writer/recovery/restore and fault harness; then adapter interfaces and venue quota supervisors.
3. One small public capture per feed family, archive fixtures including ACK/bootstrap/reconnect; resolve feed gates. Start with Deribit and Binance spot, then futures/options and Bybit profiles without changing the accepted scope.
4. Measure complete target-universe workload/depth/reference fan-out; allocate a host/storage/network profile. Authentication-gated feeds stay visibly disabled until credentials are explicitly available.
5. Fault tests, independent restore and ≥24h full-profile soak on the intended Linux host. Publish resource/validity/gap report per feed/product, including excluded/blocked feeds.

A release may be labelled partial with a coverage report; it cannot claim all-three-venue/all-product readiness with unresolved rows. Binance options requires current route + contract discovery + REST/WS sequence bridge + reset fixtures, not a legacy example. Bybit full/RPI/spreads each have their own gate. Operations guide remains concise: start/stop/status, coverage/gaps, export, backup status, restore, upgrade/rollback. No runtime change, service activation, model fitting or deployment is authorized by completion of this spec phase.
