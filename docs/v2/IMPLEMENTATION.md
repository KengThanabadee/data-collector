# DC v2 implementation chunks

User-directed workflow: implement a bounded chunk, execute relevant tests, spawn a fresh reviewer at **high reasoning**, repair findings and obtain re-review before proceeding to the next dependent chunk. Do not equate a chunk's local acceptance with all-venue production readiness.

| Chunk | Deliverable | State |
|---|---|---|
| 1 | Lossless raw contracts, stable native identities, point-in-time metadata projection, v1 reader compatibility, package inclusion | Approved after fixes; [evidence](implementation/chunk-01.json) |
| 2a | Bounded framed WAL, durable prefix, tail recovery and exclusive ownership | Approved after fixes; [evidence](implementation/chunk-02a.json) |
| 2b | Streamed immutable segment publication/verification and sealed-journal recovery | Approved after fixes; [evidence](implementation/chunk-02b.json) |
| 3a | Decimal L2, Deribit full and Bybit standard snapshot state | Approved after fixes; [evidence](implementation/chunk-03a.json) |
| 3b | Bounded REST/WS bootstrap: Binance spot/UM and Bybit full | Approved after fixes; [evidence](implementation/chunk-03b.json) |
| 3c | Native lossless depth decoders and REST request namespace binding | Approved after fixes; [evidence](implementation/chunk-03c.json) |
| 3 remaining | Feed-specific books/bootstrap/sequence/identity models and fixtures for Deribit, Binance and Bybit profiles | Pending |
| 4a | Pinned, bounded validity-correction projection | Approved after fixes; [evidence](implementation/chunk-04a.json) |
| 4b | Durable minute allocations, local ingress barriers and immutable closure vectors | Approved after fixes; [evidence](implementation/chunk-04b.json) |
| 4c | Durable correction ledger, zero-correction genesis, pinned raw evidence and restore | Approved after fixes; [evidence](implementation/chunk-04c.json) |
| 4d | Local deterministic book snapshot materialization at durable admitted positions | Approved after fixes; [evidence](implementation/chunk-04d.json) |
| 4e | Immutable local snapshot objects, final close index and pinned publication knowledge | Approved after fixes; [evidence](implementation/chunk-04e.json) |
| 4f | Portable immutable exports with original and pinned assessed validity | Approved; [evidence](implementation/chunk-04f.json) |
| 4g | Session-wide rotating WAL owner and offline prefix recovery | Approved after fixes; [evidence](implementation/chunk-04g.json) |
| 4 remaining | Owner transitions, aggregate venue closures, reference/trade projections and checkpoint recovery | Pending |
| 5a | Byte-bounded venue queue allocations and shared priority REST quota pools | Approved after fixes; [evidence](implementation/chunk-05a.json) |
| 5b | Serialized per-venue archive worker, durable raw acknowledgements and emergency loss controls | Approved after fixes; [evidence](implementation/chunk-05b.json) |
| 5c | Measured clock-health epochs, freshness and two-sample alignment recovery | Approved after fixes; [evidence](implementation/chunk-05c.json) |
| 5d | Fsynced clock measurement/assessment pairs, pinned bounded replay and ambiguous-write poisoning | Approved; [evidence](implementation/chunk-05d.json) |
| 5e | Shared raw/clock async FIFO and serialized durable clock pairs with completion timing | Approved; [evidence](implementation/chunk-05e.json) |
| 5f | Durable paired clock freshness checks and legacy sample-log replay compatibility | Approved after fixes; [evidence](implementation/chunk-05f.json) |
| 5g | Async paired clock checks on raw FIFO with shared operation bounds and durable pin reporting | Approved; [evidence](implementation/chunk-05g.json) |
| 5h | Clock-bound local allocations/closures and independently verified snapshot replay dependencies | Approved after fixes; [evidence](implementation/chunk-05h.json) |
| 5i | Separate raw durability timing from measured UTC alignment epochs | Approved; [evidence](implementation/chunk-05i.json) |
| 5j | FIFO assignment of received raw alignment labels from committed clock controls | Approved; [evidence](implementation/chunk-05j.json) |
| 5k | Continue measured-mode raw capture through UTC steps while retaining process timing faults | Approved; [evidence](implementation/chunk-05k.json) |
| 5l | Clock-bound cutoff allocation/closure commands through the shared raw FIFO | Approved; [evidence](implementation/chunk-05l.json) |
| 5m | FIFO seal and immutable pinned snapshot inputs while raw capture remains open | Approved; [evidence](implementation/chunk-05m.json) |
| 5n | Separate bounded snapshot publisher with immutable deadline/clock/knowledge delivery receipts | Approved after fixes; [evidence](implementation/chunk-05n.json) |
| 5o | Canonical publisher reports in raw FIFO, idempotent durable archival and bounded report replay | Approved; [evidence](implementation/chunk-05o.json) |
| 5p | Bounded recurring local minute cutoffs with leased publication handoff and explicit stop/backlog failures | Approved; [evidence](implementation/chunk-05p.json) |
| 5 remaining | Venue-isolated async capture, REST discovery/recovery, quotas, bounded configuration, CLI and health | Pending |
| 6 | Independent backup/restore interfaces, archive dependency verification, operational integration and compatibility packaging | Pending |
| 7a | Bounded Deribit BTC/ETH native-book evidence harness | Harness approved after fixes; [evidence](implementation/chunk-07a.json); first live run archived/replayed successfully; quiet-contract delta gate failed ([result](implementation/deribit-probe-quiet.json)) |
| 7b | Near-index option selection with preserved public index reference | Harness approved; [evidence](implementation/chunk-07b.json); second20s capture archived/replayed equally, BTC1/ETH4 advancing deltas; gate remains failed ([result](implementation/deribit-probe-near-index.json)) |
| 7c | Extend only the bounded probe duration to90s; retain byte/frame/request caps | Harness approved; [review](implementation/chunk-07c.json); 90s sample captured BTC12/ETH27 advancing deltas with equal native book replay ([result](implementation/deribit-probe-90s.json)); independent high-reasoning live-evidence audit approved |
| 7 remaining | Bounded public adapter verification, resource evidence, full-scope readiness report and remaining intended-host acceptance | Pending |

Original scope remains SPEC.md and the full feed catalogue. Each chunk may be split further if necessary; every implemented subchunk also gets its own fresh high-reasoning review. Disabled/unsupported/access-required/not-verified feed states remain visible. No unbounded capture, purchase, deployment, repository rename or push is implied by implementation work.

Chunk1 initial tests:7 new tests pass. Existing35 tests:29 passed in sandbox;6 loopback HTTP tests initially could not bind due to sandbox policy, and all6 passed under approved local-test execution. Total42 passing tests across those runs. Final chunk1:8 new tests pass,43 total with unchanged35 v1 tests. High-reasoning reviewer approved all repairs; exact hashes/outcomes are in the closure evidence.

Current v2 verification includes native decoding, archive verification, durable correction-ledger restore, local book materialization fixtures, fake-exchange capture/replay and one bounded live Deribit snapshot capture with equal raw-replayed book state. The first quiet-contract experiment did not meet the two-advancing-delta gate. The continuous multi-venue runtime, shared-time snapshot publication, exports/checkpoints, operations/backup integration and live acceptance are not yet complete.

Latest bounded live sample: Deribit90s,42 application messages including ACK, BTC12 and ETH27 advancing native deltas, no sequence invalidation in that sample, verified raw segment and equal full book replay. Root independently reconstructed Decimal levels/native predecessor chains from raw without the capture reducer; [checks](implementation/probe-independent-checks.json). This establishes only the sampled native book path. Current365 combined tests pass:330 v2 plus35 v1 including the local HTTP fault server. These are local results, not intended-host acceptance.

The90s sample evidence was independently audited by `dc_chunk7c_duration_review`:50raw records,42WSframes,41native book messages, exact predecessor chains and independently reconstructed Decimal books match. The report hash is pinned in [chunk7c review evidence](implementation/chunk-07c.json).

## Cryptofeed reuse assessment

CF01 offline evaluation is approved; see [assessment and decision](CRYPTOFEED_EVALUATION.md), [review](implementation/chunk-CF01.json), and [independent synthetic evidence](implementation/cryptofeed-offline.json). On24October the user retained our collector as the production core. CF02 live qualification is deferred; no production dependency or module replacement is introduced. Full v2 scope stays unchanged.

Chunk5i separates the stable observer process/monotonic integrity epoch from a
raw receipt's measured alignment epoch when measured clocks are enabled. A
durable raw acknowledgement alone does not certify snapshot validity.

Chunk5j adds `ArchiveLane.submit_received` for exact receive clocks with an
explicit pending alignment label. The serialized disk owner assigns that label
from the last preceding fully committed clock assessment before raw append;
the acknowledgement returns the stored epoch and full stored-envelope hash.
Clock-write failure prevents dependent raw success, and admission preflights the
larger eventual envelope. The existing `submit` API continues to accept labels
owned by its caller. Neither API checks reference freshness per message; cutoff
checks, late clock corrections and alignment validity remain separate. Live
adapters must use `submit_received` when they rely on this FIFO assignment.
Async cutoff commands are implemented in5l below; recurring timers and automatic
correction ranges remain incomplete and require review before continuous publication.

Chunk5k removes the inherited UTC-step ingress stop for measured-clock lanes.
Processing deadlines use monotonic time within the stable observer process epoch;
forward/backward UTC steps preserve original receipt clocks and raw bytes. Paired
clock checks still invalidate reference alignment and require two later healthy
measurements before alignment recovers. Process epoch changes, monotonic
regression, queue faults, missing clock observations and hard latency failures
still halt ingress. Legacy lanes retain paired UTC/monotonic validation.

Health and newly written runtime controls expose `timing_basis` as
`process_monotonic` or `paired_utc_monotonic`. A runtime control's
`control_time_valid` means its local observation is usable under that timing
basis; it does not certify UTC/reference alignment. Snapshot eligibility must
use the committed clock assessment/check dependencies. Continuous cutoff
checks and automatic correction mapping remain necessary runtime integration.

Chunk5l adds optional `cutoff_scope`/`cutoff_ledger_id` to `ArchiveLane` and
`submit_cutoff_allocation`/`submit_cutoff_close`. Exact paired command observations
are admitted into the same byte queue as raw and measurements; the dedicated
disk owner writes ledger genesis, checks, allocations/links and closures. One
unresolved minute is allowed per lane; callers await a successful allocation
before queuing one barrier at or after its saved deadline. Equal-time raw behind
the close command is excluded by the barrier ordinal, while a delayed timer
retains the original deadline. A healthy clock cannot invent a missed or
uncertain schedule: that allocation resolves durably as failed/missing.

Implicit cutoff checks share the measured clock-operation cap. Queue overflow
halts ingress with failure evidence; cancellation drains owned writes and raw
already accepted behind a barrier. Ambiguous failures expose no successful
closure acknowledgement and retain the clock pin/open-command state in emergency
evidence. Graceful stop explicitly fails an unresolved allocation. Temporary
queue request envelopes are not archived as native messages.

This is the serialized command path, not the recurring timer/snapshot publisher.
Successful durable closure alone does not prove publication by the5s deadline.
Automatic correction mapping, aggregate venue publication, checkpoint recovery
and full-universe capacity/intended-host acceptance remain incomplete. Existing
bounded full-prefix scans must be replaced or checkpointed before they become a
continuous-runtime resource bottleneck; no capacity claim follows from fixtures.

Chunk5m adds `submit_snapshot_inputs(close_record_id, utc_ns=..., monotonic_ns=...)`
after a durable local close. Its FIFO command seals the active WAL on the existing
disk owner before returning frozen segment paths, committed context/manifest
hashes, exact close hash/cutoff/deadline, replay limits and clock pin. Raw admitted
behind that command stays outside the input set. The lane remains open and an
independent publisher can read these immutable inputs without accessing mutable
WAL state. A retry for the same close retains the first input set and clock pin.

Only one input request is outstanding; bounded request counts, segment/record
counts and a conservative framed-byte cap fail visibly before another seal.
Prefix seals do not consume a clock assessment or change ingress/barrier order.
Cancellation drains owned writes; seal failure returns no input acknowledgement
and emergency evidence identifies the closed cutoff and pending request. One
integration fixture publishes and verifies a real metadata/book snapshot from
the returned inputs while the lane continues accepting raw.

These inputs are not a runtime publication or proof of the5s publication deadline.
The separate publisher below consumes this handoff; raw-control integration,
recurring scheduling, correction mapping, checkpoint acceleration and
intended-host/full-universe acceptance remain pending.

Chunk5n adds `SnapshotPublisher`: one retained job on a dedicated worker, exclusive
output-root ownership, input-metadata/job/receipt byte bounds, and shutdown that
drains an owned job before releasing its lock. Caller cancellation cannot turn an
unfinished write into success. `SnapshotOrigin` binds inputs to the configured
local process/clock domain. This is not a cross-host/process publisher protocol.

Before publishing, it verifies the pinned segment/context hashes, exact close,
complete clock pin and publication-start correction knowledge. Newly invalidated
or stale alignment, process-clock faults and late starts prevent successful
delivery. Publication also pins the expected close hash before new or repeated
commit. Existing snapshots that exclude newly known corrections are not certified
as current delivery, while their original bytes remain unchanged.

An immutable `dc.snapshot-delivery/2` receipt records the observed completion of
snapshot commit plus verification against the original deadline+5s. That artifact
completion observation precedes the receipt's own fsync. Files can survive a late
or failed attempt; their presence alone is not on-time delivery evidence. A receipt
write error is unconfirmed, not success. Retries retain a verified first receipt
and its original timestamps; no receipt means an old artifact cannot retrospectively
prove on-time delivery. The synthetic open-lane integration proves raw continues
while this publisher is blocked, then verifies the delivered snapshot.

Delivery receipts live in the publisher store; chunk5o below also archives exact
reports in raw. Backup/coverage integration and recurring orchestration remain
pending. A process
supervisor must enforce hard bounds for a permanently hung worker; Python threads
are not forcibly stopped. Full-prefix replay/checkpoint capacity and intended-host
acceptance remain unproven; this chunk does not enable unbounded live capture.

Publisher-detected UTC steps persist an immutable bounded marker keyed by local
origin and raw clock epoch. Restoring UTC or reopening the same output root
cannot recertify that epoch. A different epoch must have two replay-proven
healthy reference samples before fresh aligned delivery. Every verified job
observation is checked, including completion after a publication exception.
Previously successful historical receipts keep their original proof. Marker
I/O uncertainty stops this owner; recovery must preserve the output root and
its invalidation markers. Raw-control correction mapping remains pending.


Chunk5o adds `ArchiveLane.submit_snapshot_delivery(inputs, receipt_bytes, ...)`.
It accepts canonical `dc.snapshot-delivery/2` bytes matching an issued immutable
handoff, retains first-report identity across retries, and queues one dedicated
raw control stream in FIFO order with capture. New archive-admission timestamps
remain distinct from original publisher start/completion. Reports are archived
unchanged in `snapshot_deliveries:<scope>`; duplicate retries return the original
record identity without appending another report. Conflicting reports fail before
queue admission. Queue, receipt/envelope, total report bytes, request counts and
handoff identities are bounded. Cancellation drains accepted writes; uncertain
writes use the existing emergency path with missing-report counts.

`deliveries.py` shares canonical receipt validation with the publisher and provides
bounded raw report replay with strict namespace/origin/order checks. Reports retain
publisher claims; report replay alone does not prove referenced artifact contents,
closure/clock dependencies, or historical five-second timeliness. The integration
fixture verifies an actual published snapshot separately, archives its exact
receipt while raw remains open, and restores/replays the raw report afterward.
Reports now travel in committed raw segments, but no independent backup service
or end-to-end receipt reconciliation/coverage CLI has been implemented. Publisher
clock invalidation markers remain in its store and must be retained on recovery;
automatic raw clock correction mapping and recurring orchestration remain pending.


Chunk5p adds `MinuteCutoffs` over a fresh configured local lane. Each cycle freezes
a bounded snapshot plan, allocates the next consecutive UTC minute, waits for its
saved monotonic deadline, then samples health and submits the barrier in the same
event-loop turn as receipt admission. A delayed wake never moves the barrier.
Forward jumps preserve missing labels; clock-unhealthy future labels use capped
monotonic pacing only, never a fabricated admission deadline. Early wakeups are
rechecked and bounded. Clock reference sampling remains the capture supervisor's
responsibility; timer checks do not replace measured reference samples.

One closed cutoff/plan can be leased by a publisher consumer until acknowledgement.
The current cycle may retain one more bounded plan. Handoff acknowledgement frees
capacity, not proof of delivery. An outstanding handoff at the next close visibly
halts only that venue and retains both durable closures. The scheduler does not
drop oldest work, grow a queue, or reduce the declared universe/cadence. Independent
raw capture remains active through ordinary waiting, publication and clean timer
stop. The consumer must keep the lease through its bounded downstream handling.

`submit_cutoff_failure` durably resolves a cleanly stopped allocated timer without
inventing a barrier. Cancellation drains admitted allocation/closure/failure
commands before returning. Process-clock/callback/timer faults halt the lane with
runtime/emergency evidence. This is a fresh-scope timer, not restart allocation
recovery. Production still requires supervisor ordering: stop timers, drain owned
publication, then stop the lane; hard hung operations need process supervision.
A two-cycle synthetic integration consumes closures, seals inputs, publishes real
book snapshots and archives/replays exact delivery reports while raw remains open.
The production publication-consumer loop, restart allocation resume, automatic
clock/gap correction mapping, checkpoint capacity and intended-host acceptance
remain pending; no live collection or deployment is started by these tests.
