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
| 5q | Owned timer/seal/publish/raw-report pipeline with deferred cancellation and durable handoff acknowledgement | Approved; [evidence](implementation/chunk-05q.json) |
| 5r | Pinned cross-process cutoff reconciliation with durable interruption resolutions and recovery marker | Approved after fixes; [evidence](implementation/chunk-05r.json) |
| 5s | Explicit pinned startup resume through raw lane, minute timer and historical snapshot publication | Approved after fixes; [evidence](implementation/chunk-05s.json) |
| 5t | Owned capture inventory and automatic pinned startup selection with opened-WAL evidence | Approved after fixes; [evidence](implementation/chunk-05t.json) |
| 5u | Pinned preallocation restart with explicit uncertainty, absent-ledger verification and fresh first-cutoff publication | Approved; [evidence](implementation/chunk-05u.json) |
| 5 remaining | Venue-isolated async capture, REST discovery/recovery, quotas, bounded configuration, CLI and health | Pending |
| 6 | Independent backup/restore interfaces, archive dependency verification, operational integration and compatibility packaging | Pending |
| 7a | Bounded Deribit BTC/ETH native-book evidence harness | Harness approved after fixes; [evidence](implementation/chunk-07a.json); first live run archived/replayed successfully; quiet-contract delta gate failed ([result](implementation/deribit-probe-quiet.json)) |
| 7b | Near-index option selection with preserved public index reference | Harness approved; [evidence](implementation/chunk-07b.json); second20s capture archived/replayed equally, BTC1/ETH4 advancing deltas; gate remains failed ([result](implementation/deribit-probe-near-index.json)) |
| 7c | Extend only the bounded probe duration to90s; retain byte/frame/request caps | Harness approved; [review](implementation/chunk-07c.json); 90s sample captured BTC12/ETH27 advancing deltas with equal native book replay ([result](implementation/deribit-probe-90s.json)); independent high-reasoning live-evidence audit approved |
| 7 remaining | Bounded public adapter verification, resource evidence, full-scope readiness report and remaining intended-host acceptance | Pending |

Original scope remains SPEC.md and the full feed catalogue. Each chunk may be split further if necessary; every implemented subchunk also gets its own fresh high-reasoning review. Disabled/unsupported/access-required/not-verified feed states remain visible. No unbounded capture, purchase, deployment, repository rename or push is implied by implementation work.

Chunk1 initial tests:7 new tests pass. Existing35 tests:29 passed in sandbox;6 loopback HTTP tests initially could not bind due to sandbox policy, and all6 passed under approved local-test execution. Total42 passing tests across those runs. Final chunk1:8 new tests pass,43 total with unchanged35 v1 tests. High-reasoning reviewer approved all repairs; exact hashes/outcomes are in the closure evidence.

Current v2 verification includes native decoding, archive verification, durable correction-ledger restore, local book materialization fixtures, fake-exchange capture/replay and one bounded live Deribit snapshot capture with equal raw-replayed book state. The first quiet-contract experiment did not meet the two-advancing-delta gate. The continuous multi-venue runtime, shared-time snapshot publication, exports/checkpoints, operations/backup integration and live acceptance are not yet complete.

Latest bounded live sample: Deribit90s,42 application messages including ACK, BTC12 and ETH27 advancing native deltas, no sequence invalidation in that sample, verified raw segment and equal full book replay. Root independently reconstructed Decimal levels/native predecessor chains from raw without the capture reducer; [checks](implementation/probe-independent-checks.json). This establishes only the sampled native book path. Current430 combined tests pass:395 v2 plus35 v1 including the local HTTP fault server. These are local results, not intended-host acceptance.

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
Chunk5q below supplies the local publication-consumer loop. Restart allocation
resume, automatic clock/gap correction mapping, checkpoint capacity and intended-
host acceptance remain pending; these tests start no live collection/deployment.


Chunk5q adds `SnapshotPipeline` owning a fresh local `MinuteCutoffs` and a matching
exclusive `SnapshotPublisher`, with its receipt cap configured at or below the raw
report64KiB bound. It runs the timer and consumer together. Each consumer lease
is held through exact scheduled input sealing, publication, receipt validation
and a timely durable raw-report acknowledgement. Only then does it release the
handoff slot. Failed publisher receipts are archived as failed, never promoted by
artifact presence; unconfirmed operations retain the lease/current-close identity
and visibly halt the venue. Health separates reported/published/failed/unconfirmed
counts and exposes the active deadline. There is no accumulated result queue.

`SnapshotDelivery` now returns its exact immutable, already-bounded receipt bytes
with the hash/path. The consumer therefore neither rereads files nor performs
publication I/O on the event loop. It does not fabricate a substitute receipt
when publication or raw archival is unconfirmed. Existing runtime/emergency loss
controls and missing-report counts remain authoritative, with ambiguous artifact
or receipt files retained for later recovery.

Stop/cancellation stops future timer work, drains any admitted seal/publication/
report write and its handoff acknowledgement, joins the timer, then closes the
publisher before returning or raising cancellation. The raw lane and measured
clock sampler stay externally owned and must remain available until the pipeline
has drained. A worker that never returns still requires a hard process-supervisor
bound; deferred cancellation is not a thread-kill timeout. Constructor ownership
prevents sharing the same timer/publisher across two pipelines.

Synthetic integration covers consecutive published/archived reports, raw capture
while publishing is blocked, cancellation during each owned write stage, late and
post-artifact failed publication, producer failure and unconfirmed receipt/raw
writes. This is the local publication path, not a deployed full capture service.
Chunk5r below supplies serialized restart reconciliation. Startup/runtime resume,
automatic clock/gap corrections, checkpoints, all-feed transport supervision,
operations/backup and intended-host acceptance remain pending. No live capture
or deployment is started by this chunk.


Chunk5r adds `CutoffPin` (control count, body-chain head and last raw-envelope
hash) plus prior-prefix verification before a coordinator reads the target WAL.
Cutoff restore rejects control namespace/venue confusion. `reconcile_cutoffs`
consumes explicitly pinned verified segment references and a nonempty
authoritative expected tip into a new process/session writer. It never guesses
that tip from an incomplete directory listing or compares clocks across boots.

Only unresolved allocations become `interrupted_before_close`, in cutoff order.
Existing resolution/close bytes remain unchanged. A separate fsynced
`dc.cutoff-recovery/2` marker records source/result pins, portable input hashes,
failed/closed IDs and the next consecutive cutoff ID/UTC label. Retrying the same
recovery verifies and retains its first marker and timestamps. Errors after an
individual resolution or marker fsync return no successful recovery until retry
verifies the owned WAL; a poisoned writer still needs normal WAL recovery first.
The marker is not a snapshot or a retrospective delivery/timeliness certificate.

Record/count/projection/metadata/marker bounds fail visibly. Target output records,
control streams and serialized envelope bytes are reserved before any interruption
append, so a successful recovery remains readable under the same limits on retry.
Known target-process epoch changes or monotonic regressions require a fresh process
instead of another write into that session. Prefix scans reserve
conservative compressed/raw/evidence/metadata bytes before whole-segment reads;
verification and exposed-row passes are bounded, and pins are checked again
before new ready-marker publication. A recovery target may contain only its own
interruption suffix and final marker, never newly started capture work. Both
Journal and rotating SessionWriter fixtures preserve markers through restore and
subsequent new-process reconciliation.

This is the serialized recovery primitive and durable state handoff. Automatic
startup selection of the authoritative prefix, raw-lane/clock-bound timer resume,
late snapshot/report reconciliation and automatic clock/gap correction mapping
remain pending. Full capture, checkpoint capacity, independent backup/restore and
intended-host acceptance remain unfinished; no live service is started here.


Chunk5s integrates an explicit `RecoveryRequest` into the serialized raw lane.
The caller supplies pinned immutable old-process segments and authoritative
cutoff and validity-ledger tips. Live ingress, measured clock requests and minute
timers remain gated until a durable startup acknowledgement verifies both tips,
resolves interrupted allocations and records the recovery marker. Startup has its
own bounded300s timing budget while live ingress is closed; this does not relax
the normal raw receive-to-durable deadline. Unconfirmed or late recovery retains
its evidence and halts the lane visibly.

The resumed timer requires the exact next UTC label and continues global cutoff
IDs. The new process must acquire two new measured clock samples; old healthy
samples do not align a different boot. Ledger entries retain their global chain,
while receipt sequences restart per process/control stream. Historical segment
references remain pinned in new snapshot inputs. The publisher verifies the
recovery marker against that exact prefix without writing, allows historical
process source/config to differ from the current process, enforces current-process
source/config consistency, and uses only the new process clock pin.
Old quote owners are not silently promoted into the new process's native book.

Synthetic integration covers interrupted and all-closed histories, new owner/book
publication, carried gap corrections, incorrect cutoff/ledger pins, startup gates,
new clock warmup, startup timing limits and cancellation of both client and raw
owner during recovery. Independent review reproduced an old unbound close being
certified under a new process origin when provenance and clock labels collided.
The publisher now explicitly rejects a selected close whose venue/environment/
capture/process differs from its origin. A regression verifies direct rejection
and a failed publisher receipt without a snapshot artifact. Final full-suite and
independent re-review results are recorded in the evidence file. These tests start
no live capture or deployment.

Automatic authoritative prefix/root selection, lost publication/report recovery,
automatic correction mapping, bounded checkpoint capacity, complete feeds and
transport supervision, independent backup/restore and intended-host24h acceptance
remain pending. Explicit pinned runtime resume does not establish those gates.


Chunk5t adds a local `CaptureStore` with an exclusive root lock and fsynced framed
registry. Every new session has an ordered reservation followed by an opened
entry before its writer is returned. Crash-surviving reservations may only become
abandoned after proving their descriptor/WAL is empty and has no archive data.
Opened sessions cannot disappear. Root identity, descriptor identities, registry
hash chain/receipt order and filesystem inventory are checked before selecting
history; unknown sessions, foreign archive roots, symlinks, damaged registry tails
and conflicting metadata stop startup instead of guessing the latest timestamp.

For opened sessions, existing WAL recovery preserves corrupt tails, completes
immutable publication and verifies original committed references. The store derives
cutoff and validity-ledger tips from the entire ordered registered prefix and
returns the exact pinned `RecoveryRequest` together with its next fresh writer.
Clean empty history permits a fresh scope. Nonempty preallocation history and
empty corrupt-tail uncertainty stop visibly pending explicit gap reconciliation.
No raw evidence, original close or original publication is rewritten.

Independent review reproduced lost initial and rotated WAL files being treated
as shorter history. New writers now fsync an immutable per-ordinal opened proof
before the initial or rotated WAL is exposed to append. Recovery checks those
proofs and requires all proven WAL dependencies; it also rejects orphan WAL loss/
seal evidence and unindexed immutable archive objects. An unproved clean empty
rotated suffix can be recovered after a failed proof publication because it was
never exposed to append. Registered roots require these proofs; legacy standalone
writer recovery remains supported without requiring absent historical proofs.

This is bounded serialized startup work; its caller must keep the root owner
alive while the writer runs, pass the returned request into the reviewed lane
startup gate, run startup work outside the event loop and stop/drain before close.
Synthetic integration exercises automatic inventory selection through real lane,
clock warmup, timer, publisher, raw report and subsequent cross-process selection.
Fault tests cover registration boundaries, missing WALs, proof failures, raw tail
preservation, foreign/conflicting dependencies, ownership and resource limits.
422 combined tests pass (387v2+35v1), including18 startup tests. An independent
high-reasoning reviewer approved the repaired source/tests and passed65 focused
tests with ResourceWarning treated as an error. Exact hashes are recorded in the
chunk evidence.

Automatic adoption of legacy unregistered roots, nonempty preallocation gap
mapping, lost publication/report reconciliation, automatic corrections,
checkpoint/index capacity, full feeds/CLI supervision, independent backup/restore
and intended-host24h acceptance remain pending. This adds no live collection or
deployment, and the default startup bounds are limits rather than capacity proof.


Chunk5u permits a verified empty cutoff tip without treating unknown history as a
fresh empty capture. A nonempty raw/clock prefix or even a zero-row preserved
corrupt-tail prefix remains pinned in the automatic startup request. The fsynced
recovery marker explicitly records `preallocation_interruption`: continuity is
not proven, no last close or first affected cutoff is inferred, and UTC bounds
are not invented. Its next global ID is0 and next UTC label is null; the caller
chooses the first label from the new process clock. No historical cutoff or
successful delivery is fabricated. Clean empty history still needs no recovery.

A nullable ledger pin is an exact absence claim, independently compared against
the entire verified prior prefix before the marker is written. It cannot omit
existing ledger entries. A nonempty cutoff tip still requires a real ledger pin.
A ledger initialized before the first cutoff, including an open correction, is
carried forward unchanged. When no ledger exists, initialization occurs through
the serialized lane at the actual first allocation; the normal ledger pin API
continues to reject empty knowledge unless startup explicitly requests absence.

The new process still needs two new clock samples and new process book/metadata
inputs. Earlier clock health or quote owners cannot supply its alignment or book
state. The marker's uncertainty describes the interrupted prior process; it is
not a resolved gap, a cutoff-range correction, a UTC interval certificate or a
substitute for existing correction rules. A carried open correction continues to
invalidate its matching first new book until an explicit resolution.

Eight new synthetic tests exercise automatic root selection through lane, timer,
real snapshot publication and verified readback; old raw bytes/tail evidence and
open-correction effects remain intact. They also cover old-clock warmup refusal,
null-ledger omission rejection, nonempty-tip constructor refusal, exact one-record
marker retry/read-only verification, forged label rejection and repeated restart
before any allocation. Independent high-reasoning review approved the scope with
54 focused tests passed. Final full-suite results and exact hashes are recorded
in the chunk evidence.

Mapping interruption/clock/feed gaps onto existing cutoff ranges, lost snapshot/
report reconciliation, checkpoint capacity, all-feed transport/CLI supervision,
legacy root adoption, independent backup/restore and intended-host24h acceptance
remain pending. No live capture or deployment is started by these tests.
