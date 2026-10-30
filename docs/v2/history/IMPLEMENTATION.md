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
| 5v | Native gap replay, quiet-cutoff range mapping, durable audit and FIFO correction hook | Approved after fixes; [evidence](implementation/chunk-05v.json) |
| 5w | Automatic exact native-audit correction before startup recovery marker/live gate | Approved; [evidence](implementation/chunk-05w.json) |
| 5x | Separate close knowledge from first-publication validity; byte-compatible legacy policy replay | Approved; [evidence](implementation/chunk-05x.json) |
| 5y | Native recovery closure selection, portable positive proof and FIFO resolution | Approved after fixes; [evidence](implementation/chunk-05y.json) |
| 5z | Durable managed owner plans, source cutover fences and allocation-pinned replay | Approved after fixes; [evidence](implementation/chunk-05z.json) |
| 5aa | Atomic managed scheduler cutovers and allocated-plan health | Approved after fixes; [evidence](implementation/chunk-05aa.json) |
| 5ab | Bounded durable Deribit shared-socket receiver and automatic native gap mapping | Approved after fixes; [evidence](implementation/chunk-05ab.json) |
| 5ac | Recover managed snapshot-book native failures directly from raw before certification/startup | Approved after fixes; [evidence](implementation/chunk-05ac.json) |
| 5ad | Bounded buffered native discovery, timeout/reavailability repair and frozen artifact compatibility | Approved after fixes; [evidence](implementation/chunk-05ad.json) |
| 5ae | Managed Deribit connection-end negatives, durable shutdown and non-healing replay | Approved after fixes; [evidence](implementation/chunk-05ae.json) |
| 5af | Verified snapshot-book continuation state and immutable native checkpoint prerequisite | Approved; [evidence](implementation/chunk-05af.json) |
| 5ag | Checkpoint-to-cutoff native continuation with exact raw artifact equivalence | Approved; [evidence](implementation/chunk-05ag.json) |
| 5 remaining | Venue-isolated async capture, REST discovery/recovery, quotas, bounded configuration, CLI and health | Pending |
| 6 | Independent backup/restore interfaces, archive dependency verification, operational integration and compatibility packaging | Pending |
| 7a | Bounded Deribit BTC/ETH native-book evidence harness | Harness approved after fixes; [evidence](implementation/chunk-07a.json); first live run archived/replayed successfully; quiet-contract delta gate failed ([result](implementation/deribit-probe-quiet.json)) |
| 7b | Near-index option selection with preserved public index reference | Harness approved; [evidence](implementation/chunk-07b.json); second20s capture archived/replayed equally, BTC1/ETH4 advancing deltas; gate remains failed ([result](implementation/deribit-probe-near-index.json)) |
| 7c | Extend only the bounded probe duration to90s; retain byte/frame/request caps | Harness approved; [review](implementation/chunk-07c.json); 90s sample captured BTC12/ETH27 advancing deltas with equal native book replay ([result](implementation/deribit-probe-90s.json)); independent high-reasoning live-evidence audit approved |
| 7 remaining | Bounded public adapter verification, resource evidence, full-scope readiness report and remaining intended-host acceptance | Pending |

Original scope remains SPEC.md and the full feed catalogue. Each chunk may be split further if necessary; every implemented subchunk also gets its own fresh high-reasoning review. Disabled/unsupported/access-required/not-verified feed states remain visible. No unbounded capture, purchase, deployment, repository rename or push is implied by implementation work.

Chunk1 initial tests:7 new tests pass. Existing35 tests:29 passed in sandbox;6 loopback HTTP tests initially could not bind due to sandbox policy, and all6 passed under approved local-test execution. Total42 passing tests across those runs. Final chunk1:8 new tests pass,43 total with unchanged35 v1 tests. High-reasoning reviewer approved all repairs; exact hashes/outcomes are in the closure evidence.

Current v2 verification includes native decoding, archive verification, durable correction-ledger restore, local book materialization fixtures, fake-exchange capture/replay and one bounded live Deribit snapshot capture with equal raw-replayed book state. The first quiet-contract experiment did not meet the two-advancing-delta gate. The continuous multi-venue runtime, shared-time snapshot publication, exports/checkpoints, operations/backup integration and live acceptance are not yet complete.

Latest bounded live sample: Deribit90s,42 application messages including ACK, BTC12 and ETH27 advancing native deltas, no sequence invalidation in that sample, verified raw segment and equal full book replay. Root independently reconstructed Decimal levels/native predecessor chains from raw without the capture reducer; [checks](implementation/probe-independent-checks.json). This establishes only the sampled native book path. Current503 combined tests pass:468 v2 plus35 v1 including the local HTTP fault server. These are local results, not intended-host acceptance.

The90s sample evidence was independently audited by `dc_chunk7c_duration_review`:50raw records,42WSframes,41native book messages, exact predecessor chains and independently reconstructed Decimal books match. The report hash is pinned in [chunk7c review evidence](implementation/chunk-07c.json).

## Cryptofeed reuse assessment

CF01 offline evaluation is approved; see [assessment and decision](../CRYPTOFEED_EVALUATION.md), [review](implementation/chunk-CF01.json), and [independent synthetic evidence](implementation/cryptofeed-offline.json). On24October the user retained our collector as the production core. CF02 live qualification is deferred; no production dependency or module replacement is introduced. Full v2 scope stays unchanged.

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


Chunk5v adds the adapter-invoked `submit_native_gap(plan, binding_index,
discovery_record_id)` command through the existing raw FIFO/disk owner. The
mapper independently replays the selected native owner with reviewed snapshot/
buffered book reducers, derives its last applied continuous proof and requires the
reported raw delivery to establish a native invalidation. Duplicate/stale/control
messages do not extend that proof or become a fabricated hole. Messages for
another instrument on a multiplexed stream retain receipt/dependency accounting
but are routed before target validation or bootstrap-clock initialization.

The first affected cutoff follows saved barrier order, including quiet closures
whose admitted prefix still equals the proof. The exact post-message proof
boundary `(monotonic_ns, collector_ingress_ordinal +1)` can retain its state;
strictly later barriers are affected. Missing or incompatible proof/epoch/barrier
knowledge widens conservatively and records unknown status. Cross-process clocks
are never compared. Bybit standard health-only semantics do not become a native
sequence proof. New/future cutoff IDs inherit the resulting open correction for
the matching scope, instrument/profile and owner epoch.

A fsynced audit pins the plan, native discovery/proof, cutoff knowledge, receipt
observations and required raw hashes before its exact negative ledger correction.
UTC interval bounds are explicitly not inferred. Ledger restore validates audit
namespace/order and dependencies, requires the correction to match the audit and
expands raw dependencies transitively for portable snapshot/export verification.
Retries retain the first audit identity/timestamps and its original cutoff prefix,
including exact completion in a later process with a lower monotonic origin.

Independent review repaired two findings: legitimate multiplexed instruments were
rejected before routing, and a durable audit without its correction could allow an
old valid assessment after crash. Pending audits now block public ledger pins,
assessments and startup certification. A ledger owner may restore their bytes for
repair; only the exact audit-derived correction completes pending knowledge. The
explicit mapper retry can finish across process restart. Automatic supervisor
selection/repair of pending jobs is still pending, so ordinary startup visibly
stops on an unresolved audit instead of certifying unaffected history.

The hook has bounded request/plan/raw/dependency/audit projections and normal
receive-to-durable timing. It remains callable while raw capture is open, preserves
cancelled-client admitted work and does not modify old raw or published snapshots.
Twelve new synthetic tests cover exact vs later quiet barriers, real assessed
exports with immutable originals, transitive dependency loss, malformed/control/
duplicate discoveries, bounds, FIFO publication integration, cancelled clients,
multiplexed snapshot/buffered owners, audit/correction ambiguity and new-process
repair.442 combined tests pass (407v2+35v1); independent high-reasoning re-review
passed23 gap/ledger tests. Exact bytes and closed findings are recorded separately.

This is the negative native mapping hook, not a live all-feed monitor or positive
recovery certificate. Adapters/supervision must invoke it; first recovery-closure
selection, clock/feed/process-fault range mapping, owner transitions, automatic
pending-job/publication/report recovery, checkpoint capacity, complete feed/CLI
supervision, independent backup/restore and intended-host24h acceptance remain
pending. No live capture or deployment was started. The user requested a pause
after this reviewed chunk; no subsequent chunk is started.


## Chunk5w — complete durable native audits at startup

Startup now carries the source ledger chain tip as a continuity assertion even
when a native-gap audit lacks its correction. This is not a validity certificate:
ordinary ledger pins/assessments still reject pending audits. The serialized
startup command completes only exact audit-derived negative corrections, then
writes the existing cutoff recovery marker before allowing fresh live work.
No new repair session, storage format, worker, or positive continuity claim is
introduced. An interrupted/lost-ack repair is replayed from durable bytes on the
next process and completed entries are not duplicated.

Recovery-marker verification independently reconstructs the source ledger and
requires the exact ordered pending-correction prefix. The snapshot publisher
accepts that verified prefix before the marker. Missing/mismatched dependencies,
missing corrections, unrelated ledger controls, and altered correction ranges
cannot pass the marker gate. Original raw records/publications stay immutable.

Six new integration tests cover repair-to-fresh-publication, multiple audits,
lost correction acknowledgement with another audit pending, crash between repair
and marker, missing dependency before session registration, and marker bypass/
range-tampering rejection. All448 repository tests pass with ResourceWarning
treated as errors, including35 v1 tests and the local HTTP fixtures. Fresh
high-reasoning reviewer approved the chunk after94 focused test executions
(overlapping suites) and three additional independent adversarial probes. Automatic
job/publication/report reconciliation, recovery closure, feed/process/clock
fault mapping and intended-host production acceptance remain unfinished.


## Chunk5x — publication validity after recovery knowledge

A confirmed prerequisite for native recovery closure: an open correction known
at cutoff was previously folded into the starting validity used for first
publication. Even if a resolution arrived before publication, that invalid
starting value could never recover. Replay now retains its intrinsic native,
metadata and closure status as transient side data. Standalone materialization
bytes/identities remain unchanged.

New immutable publications use `ledger-at-start/2`: `validity_at_close` still
reports the close-pinned assessment; `validity_as_published` assesses the
intrinsic replay status with the first publication ledger pin. Native/metadata/
barrier failures cannot be upgraded by a resolution. `/1` publications retain
exact legacy replay. Snapshot and portable-export readers dispatch the saved
policy; later exports still start from immutable published validity and cannot
upgrade an already-published invalid row.

Four regression tests cover pre-publication resolution and historical ranges,
unchanged published-invalid bytes/assessments, native/metadata/missing-barrier
failure protection, and mixed-policy portable exports with pinned legacy
materialization/publication/payload hashes captured before this change. Synthetic
CloseGap controls test publication semantics only; accepted-native-anchor
selection and positive-proof certification remain pending. All452 repository
tests pass with ResourceWarning treated as errors (417 v2 plus35 v1, including
local HTTP fixtures). Fresh high-reasoning reviewer approved after41 focused
tests, a HEAD-vs-current comparison across15fixture variants (17materializations
and17legacy publications byte-identical), and adversarial same-segment
resolution, pinned later-gap, portable export and invalid-policy/pin probes.


## Chunk5y — native recovery closure proof

The adapter-invoked `submit_native_recovery` shares the existing bounded mapping
FIFO and disk owner. It seals inputs, replays the frozen native plan, and selects
the first healthy closed cutoff incorporating an accepted snapshot/REST seed
acquired after native gap discovery. It does not infer recovery from quiet health,
stale retransmissions, a post-barrier anchor, incomplete bridge, missing metadata
or an unhealthy closure. Unresolved earlier cutoffs or intervening plan changes
stop selection until separately resolved; owner/plan transitions remain pending.

A versioned positive audit records the chosen closure/anchor, exact materialized
snapshot descriptor, source cutoff pin, full bounded replay-prefix raw hashes,
and all cutoff controls needed for selection, including delayed earlier outcomes.
Supplied sealed replay bytes must equal the owned durable prefix. The audit and
CloseGap resolution share the existing mapping control namespace; transitive
ledger dependencies carry the full proof into portable exports. An orphan
positive audit leaves the negative gap open and can be retried exactly, including
after restart. Positive audit completion does not reuse negative-audit startup
repair. Original historical missing intervals/publications remain unchanged.

Thirteen tests cover native and buffered recovery (Deribit, Bybit full, Binance spot
and UM), a complete async capture/gap/recovery/publication/report path, stale and
post-barrier anchors, unhealthy/unresolved/out-of-order closures, alternate
hash-valid replay inputs, bounded proof admission, ambiguous audit/resolution
acknowledgements, portable export dependencies and cross-process exact retries.
Two P2 review findings were repaired: failed/first-affected changed-plan or
unresolved boundaries cannot be skipped; selection cutoff controls appended
after the chosen close are retained in portable proof dependencies. All465
repository tests pass with ResourceWarning treated as errors (430 v2 plus35 v1,
including loopback HTTP fixtures). Fresh high-reasoning re-review approved after
13 focused native-recovery tests and90 supporting tests. Automatic adapter scheduling,
owner/plan transitions, remaining fault mapping, checkpoints, remaining feeds,
CLI/supervision, independent backup/restore and intended-host acceptance remain
unfinished. No live exchange capture or deployment was performed.


## Chunk5z — durable managed owner-plan cutovers

`OwnerPlans` stores versioned full frozen SnapshotPlans on the existing raw
journal, hash-links declarations across sessions, and binds the active declaration
to each allocation. `ArchiveLane.submit_owner_plan` uses the existing disk FIFO;
cutover starts are checked after earlier queued raw writes. Replacement paths
need fresh managed epochs from `new_owner_epoch()` and exact next WS/seed receipt
positions. Raw overlap stays archived while snapshot replay applies one selected
source. A new path never inherits an old book anchor.

Same-source resets wait for all open cutoffs using that source to resolve;
different connections can overlap before the previous cutoff closes. Metadata
observations may retain a native owner only when attribute bytes are identical.
Changed metadata attributes/native fields/config require a fresh owner/bootstrap;
source prefixes containing another config require fresh source identities.
Allocations match the exact selected source set and plan dependencies. A later
declaration affects the next allocation, never an already allocated cutoff.

Managed snapshot descriptors pin the declaration chain selected at allocation.
Legacy allocations preceding the first declaration retain exact legacy bytes.
Epoch reuse is rejected over known declaration/correction history; first adoption
uses the fresh managed UUID namespace rather than claiming discovery of every
free-form epoch ever used by legacy snapshots. Unused durable declarations burn
epochs; multiple declarations before one allocation keep the last durable plan,
and restart requires current-process sources and freshly acquired metadata.

Twelve tests cover overlap/no double application, exact legacy readback, declaration
between allocation and close, same-socket closure fences/fresh anchors, unused
owner retirement/idempotent retries, metadata/native reset rules, exact source
sets, bounded writes, lost acknowledgement, restart guards, and a cancelled
client/pending-raw FIFO path through fresh snapshot publication. Two managed
buffered regressions cover foreign-symbol WS bootstrap timing and unrelated REST
seeds. The P2 review finding was fixed by routing before native book creation or
mutation, gated on managed declarations to preserve legacy snapshot bytes.
Owner control admission uses independent bounded scan defaults; snapshot scan
limits still govern publication. This also preserves legacy cutoff allocation
when deliberately tiny snapshot limits reject later publication. All477 tests
pass with ResourceWarning treated as errors (442 v2 plus35 v1). Fresh high-reasoning
re-review approved after40 focused/supporting tests. Automatic lifecycle/resets,
production capture/CLI, broader feed qualification, remaining fault mappings,
checkpoints, independent backup/restore and intended-host acceptance remain
unfinished; this is the explicit cutover mechanism, not a continuous supervisor.


## Chunk5aa — scheduled managed cutovers

`MinuteCutoffs.replace_plan` shares a local allocation lock with the existing
minute allocator. It waits for the exact durable managed declaration before
switching the plan provider. Already allocated cutoffs retain their original
frozen plan; later allocations select the replacement. This closes the race
where an old plan could be queued behind a newly durable owner declaration.
An optional `healthy_streams_for_plan(plan)` callback evaluates closure health
against that allocated plan; existing fixed-plan health callbacks keep working.

Seven async tests exercise a blocked disk declaration racing allocation,
old/new source overlap across two published/replayed cutoffs, deferred cutover
cancellation, uncertain disk acknowledgement, pre-admission failure, untimely
acknowledgement and invalid/stopped requests. The review found an unsupported
halt-reason label that masked errors and skipped lane shutdown; using the existing
schedule-failure reason and precise failure assertions repaired it. Fresh
high-reasoning re-review approved after41 focused/supporting tests. Full-suite
results are recorded in the chunk evidence. This connects existing owner and
scheduler mechanisms; it adds no socket framework. Automatic feed supervision,
transport/clock fault mapping, proof checkpoints, continuous runtime/CLI,
independent backup/restore and intended-host acceptance remain unfinished.

The next concrete integration is the Deribit session driver using these cutovers.
Its native proof history is currently bounded to256 dependencies/64KiB per gap
audit; checkpoint/proof-prefix integration remains necessary for sustained capture.


## Chunk5ab — bounded Deribit socket ingress

`DeribitBookSession` installs fresh managed owners through the existing minute
scheduler, opens one size-bounded public socket and subscribes to at most50
explicit channels of one native product. Exact inbound bytes (including ACKs,
foreign symbols and malformed messages) and outbound requests share the existing
raw FIFO. Receipt stamping/admission precede task-cleanup awaits; book decoding
and application follow a timely, hash-checked durable acknowledgement. Subscription
ACK and native snapshot health remain distinct. Shared quota grants precede
subscription, heartbeat setup and public test responses.

The first native failure freezes the plan/discovery, marks the source unhealthy,
stops scheduling and durably maps only affected bindings. Original snapshot bytes
remain immutable; verified exports overlay the correction. Transport faults stop
visibly without inventing a native sequence proof. Cancellation drains admitted
writes/mappings and closes the socket. Idle/bootstrap/ACK/session/receive-apply,
message/byte/frame, aggregate-book and outbound/close waits have explicit bounds.

Before capture, the receiver checks conservative source-receipt/cutoff dependency
headroom against the existing256-entry/64KiB native proof cap, scan headroom and
expanded-message writer/queue capacity. This is a bounded integration path,
not continuous qualification. Nineteen synthetic async tests cover exact pipeline
publication/replay, automatic quiet-cutoff correction, multi-symbol isolation,
raw-before-apply cancellation, ACK/heartbeat, malformed/foreign input, resource
preflight, byte-crossing gaps, frame exhaustion, idle/slow processing and socket
failures. The review found an initial64-channel request exceeding the frozen
50-channel batch cap and scan headroom missing one full byte-crossing message.
The receiver now caps its group at50 and reserves the final maximum frame;
a capable-writer regression rejects insufficient scan headroom before capture.
Fresh high-reasoning re-review approved all19 receiver tests. Full-suite results
are recorded in the chunk evidence.

Late/uncertain raw durability acknowledgement stops before native apply. A writer
clock/deadline failure can halt admission before a negative mapping is queued;
those unresolved raw discoveries require the pending fault/reconciliation work.
This receiver alone does not certify historical coverage after such failures.

Automatic reconnect/lifecycle discovery, transport/clock/process correction
mapping, durable proof checkpoints, broader feeds, continuous runtime/CLI,
independent backup/restore and intended-host acceptance remain unfinished.
No network capture or deployment was performed for this chunk.


## Chunk5ac — recover native failures from raw

Managed Deribit full and Bybit standard snapshot-book paths now derive missing
native negatives from existing owner declarations and immutable raw records.
Public ledger pins, assessments and dependency reads refuse certification while
an unaudited failure remains. Existing native audit/correction records repair it;
there is no additional durable schema or journal. Legacy undeclared paths keep
their existing behavior and published snapshot bytes remain immutable.

The disk owner performs this reconciliation before cutoff operations and snapshot
input sealing, covering raw admission that races the receiver's mapping callback.
Startup completes interrupted audits and newly discovered failures before its
recovery marker. Independent marker verification replays the exact negative-only
repair prefix, including preallocation history without an existing ledger genesis.
Earlier receipt timestamps from a partial repair remain verifiable on retry.
Internal continuity tips do not bypass the public certification gate.

Ten synthetic tests cover certification/export, interrupted audit acknowledgement,
repeated gaps, owner resets, resource bounds, raw-only startup, omitted/altered
repair prefixes, partial retries, missing genesis and the cutoff callback race.
Review prompted source-only startup pin validation, explicit handling of remaining
uncertified siblings and restricting this projection to snapshot-book profiles.
The aggregate level cap matches the existing replay default of200000. Full-suite
results and exact reviewed hashes are recorded in the chunk evidence.

Buffered native profiles, transport/clock/process fault mapping, proof checkpoints,
automatic lifecycle/reconnect, broader feeds, continuous runtime/CLI, independent
backup/restore and intended-host acceptance remain pending. No live capture or
deployment was performed for this chunk.


## Chunk5ad — buffered native discovery and historical replay

The existing raw-derived certification gate now also replays managed Binance
spot/UM and Bybit full books. It uses their existing buffered bootstrap models,
seed decoders and negative mapping path. Foreign-symbol WS/REST input is routed
before model creation. Per-path native limits stay unchanged; aggregate paths,
book/seed levels, buffered event count and retained bootstrap bytes are bounded.
Byte accounting includes seeds and last deltas outside the buffer, checked after
each path transition. Resource exhaustion stops certification instead of creating
a synthetic exchange gap.

Buffered timeouts can set invalid state while returning ignored_until_reset;
this state now establishes a negative discovery. Bybit empty-book reavailability
resets before the next matching WS event, matching managed replay. New buffered
negative audits retain schema /2 and name optional buffered_policy
reset-and-timeout/1. Existing audits without this field retain their old mapper
semantics and exact retry bytes. Unknown policy values are rejected.

Replay activates the corrected timeout behavior per exact target only when its
validated policy audit precedes the selected close. This preserves older managed
snapshot bytes. A private frozen-ledger view is used only to reproduce an existing
snapshot or its embedded export: it checks the exact pinned chain, dependencies
and orphan audits, while leaving newly discovered raw negatives to assessment.
Current ledger certification, new materialization/publication and latest assessed
exports still require exact repair. Original archives remain unchanged.

Ten new tests cover all three buffered profiles, foreign input, timeout,
empty-book reavailability, repeated failures, malformed seeds, aggregate bounds,
legacy audit retries, cross-process startup/marker verification and historical
snapshot/export reproduction with current certification blocked. Review identified
the historical managed replay regression; the policy-bound replay and separate
frozen read path address it. Exact hashes and full-suite results are in the evidence.

Quiet bootstrap expiry without a new receipt still needs transport watchdog
integration. Transport/clock/process faults, proof checkpoints, automatic feed
lifecycle, remaining feed models/runtime/CLI, independent backup/restore and
intended-host acceptance remain pending. No live capture or deployment occurred.


## Chunk5ae — Deribit connection termination

Existing receiver socket CLOSE/CLOSING/CLOSED/ERROR controls and its stopped
control now establish connection termination. Managed raw discovery retains the
first end per target, including bootstrapping and already native-invalid targets.
Native sequence gaps and connection loss remain independent causes: a fresh
snapshot can repair a native reset on an open socket, but cannot resurrect an
ended source. Shared-socket termination affects every binding while a native gap
still targets only the instruments whose sequence failed.

The terminal mapper reuses native book replay and the existing last-proof cutoff
boundary rule. It emits an open negative with discovery_kind deribit_end and
transport_policy deribit-session-end/1 in the existing audit schema. Controls and
other instruments never advance the target's proof; no anchor widens to unknown
from cutoff0. A validated exact managed plan must precede the terminal record.
Transport-aware audits retain the declaration chain and its metadata hashes.
This prevents omitted ownership evidence from silently selecting legacy replay.
Existing native audits without this policy retain their exact retry semantics.

The receiver remembers the earliest durable socket end, archives its stopped
record and awaits each correction through the existing FIFO before returning or
raising cancellation. An interrupted acknowledgement remains discoverable from
raw during startup or before cutoff/seal. Stopped-record correction count remains
the count at serialization; final health separately reports connection-end
corrections. Preflight reserves owner/metadata proof headroom conservatively.

Managed replay activates termination handling only from validated exact-target
policy audits before the selected close. Later frames do not heal that source,
and native recovery cannot select them. New owner declarations require a fresh
connection identity after termination; historical declarations/artifacts retain
their original read semantics. Existing frozen snapshot/export reads remain
separate from current certification.

Nineteen new tests plus the updated multiplexed receiver test cover quiet-cutoff
corrections, earliest-end deduplication, no anchor, independent causes, post-end
snapshot/recovery, source/identity/policy bounds, legacy audit retries, omitted
owner dependencies, crash/startup/marker repair, disk-owner callback races,
requested stop and cancellation. Review identified unmanaged terminal API use and
missing ownership proof dependencies; both were repaired and re-reviewed.
Full-suite results and exact hashes are recorded in the chunk evidence.

This handles saved managed Deribit termination controls only. Failure before any
such control can be written still requires runtime/clock/process fault mapping.
Automatic reconnect/lifecycle, durable proof checkpoints, other feed models,
continuous runtime/CLI, independent backup/restore and intended-host acceptance
remain pending. No network capture or deployment was performed.


## Chunk5af — verified native checkpoint prerequisite

`bookstate.py` saves/restores every existing SnapshotBook continuation field,
including observed watermarks and last fingerprint omitted by display views.
Native price precision, configured level cap, epoch, invalid/bootstrapping state
and raw source identities survive restoration. Unknown state shapes and resource
excess are rejected. The existing materializer exposes final native state through
a private optional sink after all validation; its normal output bytes are unchanged.

`checkpoints.py` derives an immutable checkpoint at an exact closed raw frontier,
retaining its full snapshot descriptor/ledger pin/owner and raw references,
source receipt frontiers, last-applied raw proof and terminal latches. Marker-last
publication reuses existing immutable file writes. Readers check identity/bounds
and independently replay raw to compare the entire checkpoint; recomputing a
checksum around altered hidden state cannot pass. Ledger history/open negatives
stay referenced and unchanged, not silently reset or hydrated by this module.

Fourteen tests cover hidden state with identical views but different future
acceptance, exact suffix state, duplicate/conflict/stale/reset rules, native
precision/caps, hash-consistent semantic tampering, missing/pinned dependencies,
pre-allocation resource checks, interrupted marker publication, immutable retry,
and terminal/owner/open-negative preservation. A bounded synthetic experiment
at10k/100k/1m native events demonstrates equal full state after restoring and
applying a1000-event suffix. See [scope and measurements](NATIVE_CHECKPOINTS.md).
Exact reviewed hashes and full-suite results are recorded in the chunk evidence.

This is a prerequisite for acceleration, not continuous capture qualification.
Only snapshot-seeded Deribit/Bybit-standard models are supported. Buffered state,
native suffix orchestration, compact raw-prefix proofs and incremental ledger/
owner/cutoff/metadata/clock projections remain pending. Existing256-dependency/
64KiB proof, scan and projection limits remain in force; reading a checkpoint
still replays full raw. Other feeds/lifecycle/runtime/CLI, independent backup/
restore and intended-host acceptance remain unfinished. No network/deployment.


## Chunk5ag — checkpoint-to-cutoff native replay

`materialize_from_checkpoint` re-verifies encoded checkpoint bytes itself and
renders a later cutoff for the same exact frozen snapshot-book plan. The target
prefix must contain the old close/hash in the same capture/scope and extend the
checkpoint ledger knowledge. Full control/owner/discovery/clock/metadata/receipt
certification stays on its existing path. Only native binding dispatch resumes
from admitted per-stream sequences; records newly admitted by a later cutoff
are included even when they precede the checkpoint close in raw file order.

The existing checkpoint verification replay now returns transient cumulative
diagnostics and effective policy context. Checkpoint /1 bytes remain unchanged.
The renderer restores native model state, diagnostics and terminal latches; it
falls back to raw native replay when audit/owner policy changes or target
admissions cease to extend the checkpoint (for example after a clock epoch
change). Descriptor, payload, snapshot ID and ledger pin equal raw-only results.
Public callers cannot supply a mutable self-labelled verified model to bypass
checkpoint validation.

Eleven tests cover both profiles, quiet and populated suffixes, repeated calls,
129-receipt prefix/2-receipt suffix dispatch counts, newly admitted pre-close
receipts, cumulative diagnostics/recovery, hidden observed watermarks, changed
plan/future frontier and recomputed-hash tampering, new-policy fallback, clock
admission fallback, and ended-source/open-negative preservation. Fresh high-
reasoning re-review approved the clock fallback extension. Full-suite results
and exact hashes are in the chunk evidence.

The statistics count renderer binding dispatch only: checkpoint verification
and control/discovery certification still replay full prefixes. This does not
remove capture proof/scan caps or establish end-to-end acceleration. Incremental
control projections, compact raw-prefix proofs, runtime checkpoint scheduling,
buffered state, other feeds/lifecycle/CLI, independent backup/restore and actual
host acceptance remain pending. No network capture or deployment occurred.


## Chunk5ah — measure managed ledger cost before adding state

Added a bounded, network-free `experiments/ledger_refresh_probe.py`, using an
actual fsynced SessionWriter and rotated immutable segment. Three unchanged
managed ledger pins at 100/1,000/10,000 native events have median durations
0.034/0.329/3.207 seconds on this local machine. Separate profiles assert exact
native dispatch counts, and every call checks unchanged ledger identity.
Machine details, raw sizes, profile attribution and exact source hashes are
saved in `docs/v2/experiments/ledger_refresh_probe.json`.

The one-instrument, two-level healthy fixture establishes repeated old native
work but also shows larger raw decoding/verification costs. Defer the proposed
native-discovery-only cache; investigate reuse of verified immutable input
within one certification operation first. No runtime module, schema, dependency,
scan cap or certification rule changed. Nested profile timings overlap and
include instrumentation overhead; this is neither a cache speedup estimate nor
capture/publication/full-universe/host acceptance. See NATIVE_CHECKPOINTS.md for
method, limits and the revised next step. Focused discovery/owner/termination
regressions pass (41 tests); independent high-reasoning review includes separate
probe smoke and argument-bound checks. Exact approval is in chunk evidence.


## Chunk5ai — avoid segment encode/reparse during certification

Journal and SessionWriter now offer a streaming `raw_records()` view. Ledger
and owner scans prefer it while retaining the encoded-only source fallback.
Committed segments still undergo complete verification before exposing records;
the active WAL still checks frame bytes and unexpected suffixes. The typed view
passes existing decoded segment records directly to projections instead of
serializing them only for the next layer to decode them again. The encoded API
is unchanged. No records are retained between calls, no persistent cache is
introduced, and all full-prefix scans and resource limits remain in force.

Five regression tests cover typed/encoded binary byte equality across rotations
and an active WAL, repeated whole-segment verification before first yield, bad
WAL frames/suffixes, sealed/closed restrictions, encoded-only source fallback,
missing raw and pending-native-negative poisoning. Independent high-reasoning
review additionally checked exact pin/evidence/control/position/owner-plan
identity, lazy reads, generator close propagation and append after early close.
Full repository suite:572 tests passed in53.317s with ResourceWarning as errors;
reviewer independently passed78 focused tests. Measurement evidence is in
`experiments/ledger_typed_comparison.json`; interpretation is documented in
NATIVE_CHECKPOINTS.md. This reduces redundant work only: it does not make scans
incremental, remove continuous-capture bounds or establish host readiness.


## Chunk5aj — bounded reconnect inside the existing Deribit owner

`DeribitBookSession.run()` can now replace a failed socket without restarting its
exclusive MinuteCutoffs/SnapshotPipeline or clearing a lane failure. The default
`DeribitLimits.max_attempts=1` preserves terminal single-attempt behavior. An
explicit profile can request up to8 attempts with `retry_delay_ms` from1 to60000;
existing proof/scan/queue limits may reject that profile before connection. The
same quota, metadata/channel group and cumulative frame/byte/run-time budget
apply to all attempts. Headroom accounts conservatively for future owner
controls, terminal evidence and corrections; attempts cannot renew capacity.

Only `socket_closed_or_error` and successfully mapped `native_resync_required`
failures are retry candidates. The old attempt first archives stopped evidence,
confirms every binding's connection-end mapping and confirms socket close. Any
unconfirmed cleanup, cancellation, storage/clock/quota error, deadline, resource
bound, stopped scheduler or exhausted retry budget remains terminal. Idle or
connect timeouts are not automatically retried in this increment. Backoff is
interruptible. No scheduler or lane halt is reset, and no new supervisor
framework is introduced.

A replacement gets fresh source/owner identities, sequence zero, ACK state and
empty native models. Existing `replace_plan()` provides the durable atomic
cutover. Previously allocated cutoffs keep their frozen plan and cannot borrow
a replacement's health; that connection needs its own snapshot before becoming
healthy. Native/end proof histories remain immutable. Terminal records report
per-attempt frames/bytes/corrections, while receiver health exposes cumulative
counters and attempt count.

Nine new tests cover fresh identity/anchor and successful publication, cumulative
frame/byte/time bounds, retry exhaustion, failed close/end acknowledgement,
stop/cancellation during backoff, profile bounds and three actual publications
across a cutover: the already allocated old plan stays missing and the following
fresh plan publishes valid. Focused47 tests pass; the complete581-test suite
passes in55.416s with ResourceWarning errors (546v2+35v1). Exact independent
review/source hashes are recorded in the chunk evidence.

This is a bounded frozen-group reconnect path, not continuous-capacity or host
acceptance. Universe/listing/expiry changes, more retryable transport classes,
automatic reference sampling/reconciliation, scalable checkpoint integration,
other venues/feeds, CLI, backup/restore and full production acceptance remain
in the original scope. No live network capture or deployment occurred.


## Chunk5ak — local chrony reference evidence and recurring admission

`ChronyClockSampler` in `data_collector/clocksource.py` reads the local daemon
using fixed read-only `chronyc -c -h 127.0.0.1 tracking`. It neither installs nor
configures chrony, adjusts the clock, or implements NTP. The subprocess has a
runtime timeout and bounded stdout/stderr; cancellation and overflow kill/reap
the owned child and drain pipes. No shell is used. Raw successful/failed command
outputs are base64-preserved in `dc.chrony-source/1` measurement bodies. When
acquisition cannot return complete output, `output_complete=false` explicitly
marks that absence; no complete measurement is claimed.

The strict14-field parser follows the [chrony4.8 client source](https://raw.githubusercontent.com/mlichvar/chrony/4.8/client.c).
It rejects local/unknown references, unsynchronized/leap states, invalid numeric
fields, and stale/future upstream reference times. The independently pinned
reference-age limit defaults300s, capped3600s; it is separate from collector
sample freshness. Decimal arithmetic and upward rounding preserve a conservative
nanosecond interval. [Chrony's documented error bound](https://chrony-project.org/doc/4.8/chronyc.html)
is conditional on a correct external stratum-1 source. We use its absolute bound
as zero-centred uncertainty (`offset_ns=0` is not a directional zero-offset
measurement), add3ns formatting allowance, acquisition age and wall/monotonic
mismatch. The actual reported correction remains in raw stdout. This does not
authenticate the daemon or reference source.

After asynchronous acquisition the sampler takes fresh paired clocks and
admits source/sample to the existing raw FIFO in the same event-loop turn.
Intervening native ingress therefore cannot put a newly admitted sample behind
its old command-start timestamp. Failed/unusable readings create a durable
unaligned sample with maximum uncertainty, never healthy zero. Exact source
hash and timely durable acknowledgement are checked. Admitted clock pairs drain
before cancellation returns, and replay uses the unchanged ClockSample,
assessment and ClockPin contracts.

The bounded single-owner loop defaults100 samples, at half the monitor's sample
interval (30s under the default policy). This leaves freshness margin while the
existing monitor still requires two healthy observations separated by one full
policy interval before alignment. Stop/cancellation interrupts timer waits;
early timers, process-epoch faults, queue/durability failures stay fail-closed.
Caller owns the raw lane and must stop/drain the sampler before stopping it.
There is no continuous supervisor/CLI wiring in this increment.

Seven new tests cover exact source replay and warm-up, durable provider failure
and recovery, ingress during acquisition, cancellation during clock-pair fsync,
stop/early-timer behavior, parser rejection/rounding, and real subprocess output,
oversize, timeout and cancellation with PID reaping. Full588 tests pass in58.094s
with ResourceWarning as errors (553v2+35v1); independent high-reasoning review
approved both source hashes and independently ran the focused tests.

Intended Linux installation/version, pinned chrony configuration/external UTC
source trust, service-user read access, root-distance behavior during ordinary
slewing and the configured reference-age limit remain host acceptance gates.
The local subprocess tests use fake executables; no live daemon was queried and
no system time/service configuration changed. Clock negative-ledger integration,
supervisor wiring, remaining feeds/lifecycle/checkpoint/CLI/backup and full
production acceptance remain pending. Exact hashes/review are in chunk evidence.


## Chunk5al — bounded clock/feed/publication task ownership

`DeribitCapture` in `data_collector/runtime.py` owns one unstarted clock sampler,
Deribit receiver and snapshot pipeline on the same already-running raw lane.
The constructor rejects mismatched lanes/schedules/boot identities, previously
stopped/started components, stale nested timer/publisher state and competing
capture owners. It installs the receiver's allocated-plan health provider.
The caller still owns the raw lane task, HTTP session, shared quota and frozen
metadata/universe acquisition; drain this group before closing those resources.
This is neither the standalone CLI nor a cross-venue/process supervisor.

Startup waits for new sampler evidence and a fresh timely durable clock check.
Historical sample alignment alone cannot start the socket. An ended or failed
clock source, stopped group or warmup deadline prevents further startup. The
receiver then commits its managed owner cutover before the timer can allocate.
Two small event notifications expose committed clock progress/termination and
owner selection; there is no polling loop or mutable replay cache.

A component's completion, failure, user stop or cancellation ends the bounded
group. Admitted publication/report handling drains while feed/clock owners are
still available, then the receiver archives its end mappings and closes its
socket, then the clock sampler drains. Caller cancellation is re-raised only
after this ownership chain finishes. Warmup deadlines do not cancel an admitted
clock write; hard stuck I/O still requires the eventual process supervisor.
Raw lane shutdown/recovery and HTTP/quota cleanup remain caller obligations.

Fresh high-reasoning review caught two startup-stop issues: propagating stop to
the receiver only in the finalizer could open a socket after owner acknowledgement,
and a return inside the receiver's protected try could suppress an end-cleanup
failure stored by finally. Startup stop now synchronously stops the receiver;
active publication/draining retains the prior ordered behavior. The receiver
honors stop before an attempt and conditionally skips connect after owner ack,
while always propagating cleanup failures. No stopped owner is rearmed.

Ten new integration tests cover healthy warmup through raw capture/publication/
durable report and exact valid-book readback, unaligned source exhaustion,
provider/connect failure, stop-before-run, warmup timeout, stopped nested owners,
stop/cancel during publication, and stop during owner acknowledgement with both
successful and failed cleanup. Independent re-review approved all repairs and
the strengthened artifact assertion. Full598 tests pass in59.896s with
ResourceWarning as errors (563v2+35v1); focused45 tests pass in10.392s. Exact
source hashes and review are recorded in chunk evidence.

This integrates bounded frozen-group components only. Release configuration,
universe/discovery/lifecycle, raw lane/process supervision, remaining venues/
feeds, checkpoint capacity, standalone CLI, independent backup/restore and actual
Linux chrony/full-target soak acceptance remain pending. No live capture,
system-service change or deployment occurred.


## Chunk5am — durable Deribit discovery and raw-backed universe index

`DeribitDiscovery` in `data_collector/discovery.py` captures bounded public
`get_instruments` attempts through the existing measured raw lane and shared
venue quota. [Current primary API documentation](https://docs.deribit.com/api-reference/market-data/public-get_instruments)
qualifies `currency=any` across settlement currencies and a distinct1request/s
limit. The fixed query also uses `kind=any`, `expired=false`, `extended=true`.
Derivatives are selected by explicit BTC/ETH base metadata, including contracts
settled in another currency; `is_active=false` is retained, not silently omitted.
This is discovery evidence, not atomic market-state data or feed enablement.

Each call archives request intent, then obtains its final quota grant immediately
before starting HTTP acquisition. A mandatory shared `get_instruments` pool
has capacity1 and a window of at least1s, alongside the caller's frozen venue
cost pools. No archive await can age a grant before application dispatch.
`discovery_session()` qualifies pinned aiohttp3.14.5 by disabling its internal
idempotent-request retry; default retry-enabled sessions are rejected. Per-call
middleware and status-error overrides retain HTTP errors for raw capture.
Dependency upgrades must requalify this private transport switch. The factory
has no cookies, environment-proxy trust or credentials; callers own its closure.

Exact bounded response bodies, public request parameters, request source/hash,
request-start/receipt clocks, HTTP status and allowlisted relevant headers are archived
before parsing. Redirects/decompression are disabled. Partial reads, timeout,
oversize and cancellation preserve the observed prefix with explicit incomplete
status; they never publish a partial universe. Received429 cooldown is applied
at headers before body awaits or cancellation. A single qualified numeric
Retry-After up to3600s defers every charged pool; absent/ambiguous/unsupported
cooldown fails that venue's REST quota closed rather than guessing a delay.
Admitted archive writes drain before cancellation; an unconfirmed ack poisons
the discovery owner. No automatic HTTP retry is introduced.

`project_instruments` returns an immutable `dc.deribit-universe/1` index with
selected/excluded/blocked rows, source/row pointers, native IDs plus creation
incarnations and explicit zero counts for known families. Native decimal values
become declared decimal strings in metadata; untouched raw preserves original
lexemes and unknown fields. Metadata effective time remains unknown, never
backdated to contract creation. Combo candidates remain blocked until all legs
are verified; unknown native kinds remain visible. Identity/schema/row/byte
bounds reject the whole candidate instead of choosing a prefix. Empty or bad
refreshes retain the last accepted universe with stale health, including an
exhausted refresh budget. A repeatable raw projection does not certify missing
request dependencies or publish `metadata_version`/owner transitions itself.

Seventeen new tests cover raw/decimal/linear/inactive fidelity, complete family
classification, empty/bad refresh retention, partial/oversized errors, cancel
before dispatch and during reads, lost acknowledgements, count/identity caps,
transport/config gates and the four independent-review regressions. Real local
HTTP servers verify exactly one request on disconnect and preservation of429
body/cooldown despite inherited status errors or retry middleware. Full615 tests
pass in61.370s with ResourceWarning errors (580v2+35v1); reviewer independently
ran15 non-loopback tests and inspected all17 passing focused results.

The source/index are implemented; frozen release configuration, dependency-
complete bootstrap/metadata emission, lifecycle subscription/audit scheduling,
full-universe sharding/capacity, CLI and the remaining original multi-venue/
feed/checkpoint/backup/host acceptance scope remain pending. No exchange HTTP
request or deployment occurred in this chunk. Exact source hashes, closed
findings and approval are recorded in chunk evidence.


## Chunk5an — discovery-backed metadata through the disk FIFO

`ArchiveLane.submit_deribit_metadata()` queues a complete universe for metadata
publication. The existing single disk owner revalidates the exact manifest
against its raw request/response, including official query/endpoint, checksums,
physical order, acquisition position and process/provenance context. It then
emits the complete batch as existing `dc.metadata/2` records through IngressWriter;
it returns `DurableMetadataAck` only after every write. Timeliness is explicit
and must be checked by callers. Canceling the client future does not cancel an
admitted disk batch. No adapter reads the active WAL outside the disk owner.

The stable per-process `derived:deribit-metadata` stream has consecutive receipt
positions and correct superseded-version chains. Ordinary raw adapters cannot
write the reserved derived namespace. Economic/native attributes contain a
static derivation policy and exact decimal-string metadata; acquisition pointers
live in native fields. Identical later discovery therefore leaves attributes
identical and allows a metadata-only plan refresh to retain its native owner.
Effective time remains unknown, rather than being backdated to listing creation.
No subscription/owner plan is automatically enabled by this API.

`deribit_metadata.py` recomputes the universe from bounded raw and checks each
row/key/body against it. Owner declaration, raw snapshot replay and correction
audits retain the source dependency contract; a native-gap audit includes both
REST records and any predecessor metadata/source dependencies. Proof graphs are
direct edges with bounded expansion, not copies of whole ancestry per version.
The receiver's conservative proof headroom includes metadata batches. Legacy
metadata without this derivation policy/reserved namespace keeps its old path
and artifact bytes. Source and universe lookup caches exist only for a bounded
verification pass; every operation still revalidates raw.

Re-review fixed two issues: source proofs are required for the reserved stream
even if a body marker is removed, and batch preflight now runs the same downstream
verifier over history plus exact would-be persisted records before the first
append. This includes ingress-ordinal expansion, source dependencies, recovered
prefixes and existing metadata budgets. Known source/proof and budget failures are rejected
before metadata writes. Partial fsync/ack failures still require existing
WAL recovery; they cannot return an acknowledged complete batch.

Eight new tests cover durable metadata/raw equivalence, unchanged attributes and
owner preservation across refresh, missing/mutated/future/stripped proof,
forged-manifest zero-write rejection, combined-budget zero-write rejection,
client cancellation, namespace exclusion, and valid snapshot materialization
followed by a portable invalid assessment after a late gap. Independent high-
reasoning re-review ran59 supporting tests and both original reproducers, then
reran the strengthened snapshot/export test and final8 metadata tests. Full623
tests pass in59.008s with ResourceWarning errors (588v2+35v1). Exact hashes and
approval are in the chunk evidence.

This connects discovery evidence to usable verified metadata. Full release
configuration/bootstrap orchestration, multi-group sharding, lifecycle updates,
CLI, scalable checkpoints, remaining venues/feeds, independent backup/restore
and intended-host acceptance remain pending. No exchange request, live daemon
query, service change or deployment occurred in this chunk.


## Chunk5ao — runnable bounded Deribit capture and readback

The next delivery is a usable vertical slice rather than another infrastructure
refinement. `python -m data_collector` and the packaged `data-collector` entry
point expose `config-check`, `discover`, `capture`, and `verify-run`. The existing
v1 entry point/configuration remain unchanged. Strict versioned TOML normalizes
effective settings and hashes them; the root is separately bound by environment
and exclusively owned during collection. V1 roots and unbound existing contents
are rejected. Collector Python source and effective config are retained.

`app.py` composes the existing writer, raw lane, quota, discovery, metadata,
receiver, schedule and publisher. A narrow clock `prime()` admits actual evidence
before discovery; recurring sampler ownership and measured warmup stay with
DeribitCapture. A fresh schedule can choose its first future minute after
warmup. One or two explicit active BTC/ETH option symbols use declared agg2 book
channels. This profile stops within120 seconds or64 incoming frames, with existing
byte/proof limits retained; it can stop before a minute snapshot is available.
A duration timer requests normal drain. Ctrl-C drains admitted raw work, and
failed/cancelled runs keep their durable prefix and terminal error.

Independent committed reads verify segments and metadata sources, replay selected
books and compare final state, verify available snapshots, and optionally export
lossless original raw envelopes as a new JSONL file. Review found and fixed three
usability issues: ACK-only capture cannot succeed; final success is published only
after readback verification; known-inadmissible long durations are rejected by
config before discovery. The reviewer considers this thin composition appropriate
and found no need for an additional orchestration framework.

All639 tests pass in66.908 seconds with ResourceWarning errors (604v2+35v1).
Final16 focused tests pass in6.768 seconds; the independent reviewer reran them
in6.395 seconds. They cover actual CLI-to-runtime wiring with fixture transport,
raw/discovery/metadata capture, snapshot publication, replay/export, cancellation,
root safety and the two capture-truth regressions. The distributable wheel builds
and includes both entry points. These use synthetic exchange/clock fixtures, not
live Linux/chrony or intended-host evidence. Exact hashes are recorded below.

The full SPEC remains the final scope. Real exchange operation of this CLI on a
Linux/chrony host, long-run proof horizon/capacity, automatic crashed-session
reconciliation, all remaining feeds/venues, complete vendor source/schema freeze,
independent backup/restore and production acceptance remain pending. This host is
macOS; no live service, deployment, clock modification or production-readiness
claim occurred. See RUNNING.md for the actual commands and current boundaries.


## Chunk5ap — size the runnable profile from actual Deribit discovery

A real public get_instruments experiment found two blockers in the initial CLI
profile: the4MiB response cap stopped a successfulHTTP200 response, and the8MiB
retained metadata-proof budget rejected the complete2653-row derivation before
writes. The failed runs and their raw/WAL evidence remain unchanged on disk.
Read-only reconstruction measured about14.17MB of combined raw response and derived
metadata evidence; the same proof fails at8MiB and verifies at16MiB.

The CLI default response cap is now8MiB and its explicit pilot replay profile has
a16MiB metadata-proof budget. The profile is frozen in result.json and passed
consistently through archive admission, independent metadata verification and
snapshot readback. Reports without it retain the original8MiB library default.
Generic replay/library defaults, proof verification and other resource bounds
are unchanged. This is a measured sizing fix, not a new orchestration layer.

The bounded live rerun completed:4,421,429 response bytes,5134 native instrument
rows,2653 selected BTC/ETH metadata versions,2606 active option symbols,2658
committed records. A separate verify-run invocation successfully reread the
archive and verified source dependencies. The small public metrics and exact
hashes are in chunk-05ap-live.json; raw archives remain ignored.

This experiment used actual macOS UTC/monotonic clocks and a process identity via
the existing observer seam, with the real absent chronyc invocation retained as
unhealthy FileNotFoundError evidence. It performed REST discovery only; no healthy
UTC alignment, WebSocket capture, minute snapshot, Linux CLI qualification,
intended-host capacity or production acceptance is claimed. No preflight was
relaxed in the public CLI. A Linux VM can qualify that path before a VPS purchase.

All640 tests pass in71.064 seconds with ResourceWarning errors. The independent
reviewer ran17 app/config tests, including a response above4MiB whose full proof
fails under8MiB but succeeds under the frozen16MiB profile. The reviewer approved
the measured fix and found no actionable issue or unnecessary abstraction. Full
SPEC scope, remaining feeds/venues, long-running operation, Linux/chrony capture,
independent backup/restore and intended-host acceptance remain pending.
