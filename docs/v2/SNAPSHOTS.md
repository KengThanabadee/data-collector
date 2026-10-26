# DC v2 snapshot finalization and validity contract

Normative policy `receive-asof/2`, 23 October 2026. This specifies future implementation. It does not claim the v2 runtime or fixtures have been implemented. It replaces the earlier timestamp-only `receive-asof/1` draft. Payload schema remains `dc.raw/2`; the replay-policy version is a separate field.

## 1. Identity and receipt order

A raw record ID is `(capture_id, process_session_id, connection_id, receive_seq)`; child order is its original array index. Connection IDs are never reused after reconnect. Each connection's receive sequence increases strictly, including its connection control records. Coordinator allocation/closure/correction records use a dedicated `control:<scope>` stream ID in place of connection_id with its own increasing sequence; they are not socket messages or members of the cutoff admission vector they describe. Keep the original UTC receive timestamp and monotonic receive time. Local monotonic values are comparable only inside one process/boot epoch; no comparison across hosts or reboots is implied.

Each instrument/feed epoch has one owning connection. During rotation or redundancy, raw includes all deliveries; a versioned owner-transition control record chooses the state-building path and cutover positions. Apply native order within that path. Do not apply the same book delta twice through cross-connection merging. Shared trade dedup follows native identity; identical prices/times alone are not an ID. Metadata/reference state uses its declared owner/version and source raw positions as well.

## 2. Closing a UTC cutoff

A coordinator allocates each UTC minute cutoff once per capture scope with a strictly increasing `cutoff_id`, its UTC label and expected universe/required-feed versions. A restart resumes this durable allocation ledger. Closed labels are never issued a second time; unobserved minutes become explicit missing cutoffs. Across multiple venues, each capture process supplies a separate local closure; the aggregate descriptor pins all required local closures or marks that venue missing.

In a healthy clock epoch, convert the future UTC cutoff once to a local monotonic deadline using a saved clock observation `(anchor_utc, anchor_monotonic, uncertainty)`. An ingress barrier is ordered against the same receipt-stamping mechanism used by the socket callbacks: messages stamped at or before that deadline can be admitted; messages stamped after it cannot. A delayed timer or busy processor does not move the deadline later. A race with identical monotonic time uses the local ingress ordinal saved by that mechanism. This order is reproducible, not claimed to be physical nanosecond precision.

Admission for each connection is an immutable contiguous prefix ending at the last eligible `receive_seq`. It must satisfy both the saved monotonic barrier and `receive_utc <= cutoff`; if UTC observations contradict the prefix/clock anchor, mark the closure clock-invalid rather than skip a middle delta or sort messages differently. Later positions cannot be admitted by a UTC predicate alone. Save the actual admission bound explicitly; replay does not recalculate it from clocks. A quiet healthy connection closes at its last received position with a fresh barrier-health observation, not a fabricated update. A connection lacking a barrier/health response by the publication deadline is explicitly missing/invalid; a null bound is not a zero-length valid feed.

Durably append `cutoff_closed` before publishing the corresponding snapshot. It contains:

- `cutoff_id`, scope, UTC label, policy/config version, expected universe/required feeds and metadata-version dependencies;
- per-process session/boot/clock epoch, saved monotonic deadline, barrier observation/ordinal and health;
- per-connection admitted-prefix positions, ordered owner-transition/dependency controls and missing/invalid reasons;
- positions/counts/hashes of committed raw/metadata/anchor dependencies plus the ledger version known at closure.

Apply every admitted message necessary for each feed in `receive_seq` order, preserving child order, using its native bootstrap/reset rules. The snapshot's applied positions must equal admitted positions for the required streams, or be invalid/degraded with the actual lag/shortfall; do not publish a valid partial prefix. For shared dependencies use their saved owner and version. Independent components can be merged in `(venue, native_product, connection_id, receive_seq, child_index)` order; no book updates are reordered within an owning connection.

Finalize the snapshot descriptor by a content-addressed snapshot ID that includes `cutoff_closed` hash, applied-position vector, dependency hashes, policy and payload hash. Duplicate attempts with the same content are idempotent; differing contents for one close record are a failure, not an overwrite. The publication commit references committed raw and closure. An apply failure can produce an invalid descriptor for that closure; it must not silently substitute a different admission vector.

## 3. Clock changes, restart and missing cutoffs

A detected step or unreliable offset closes the affected clock epoch. Preserve the message order and original timestamps; suspend valid UTC-aligned publication until two healthy samples in a new epoch. Keep receiving/durably archiving within budgets. Missed or ambiguous scheduled cutoffs receive an explicit invalid/missing record; they cannot be backfilled by a later wall-clock label or by dividing a later book state into earlier snapshots. A backward step never reopens a previously allocated cutoff. A forward jump likewise creates missing cutoff evidence. Physical UTC accuracy is bounded by the recorded clock uncertainty, not guaranteed by these admission rules.

After crash, a fully durable close record can be replayed to finish its snapshot with original admission bounds. A journal prefix without a complete close record cannot prove the historical barrier: append `cutoff_failed`/interruption evidence; do not infer a valid close from the latest journal position. If the coordinator allocation record alone survived, that cutoff remains failed/missing until resolved as such. A restart is a new process/clock epoch and new connection identities.

For example, close12:00 at position100. A later position101 gets UTC11:59:59 after a backward clock step. Replaying the closed12:00 snapshot still stops at100. Position101 is archived in the later epoch; it cannot enter the earlier snapshot. If a clock problem is discovered only later, its validity may be downgraded through the correction ledger while the original membership/value remains reproducible.

## 4. Late gap discovery and append-only corrections

A snapshot's `validity_as_published` describes information known at publication. It is immutable alongside its original values. Later evidence may prove that an earlier interval was unreliable; append a `validity_correction` to a scope-wide ordered, durable ledger. Each record requires a monotonic `ledger_seq`, previous-record hash, source raw/control IDs, discovery/acquisition timestamp and receive positions, affected instrument/feed/owner epoch, reason/evidence, bounds and new assessed status. Replica copies keep the same ledger identity, not new independent sequences.

New local publications use `ledger-at-start/2`. Replay independently computes
native/metadata/closure validity, assesses it at the close ledger pin for
`validity_at_close`, and at the first publication pin for
`validity_as_published`. A gap resolution learned between close and first
publication can make the recovered cutoff eligible only if intrinsic replay
is healthy and the cutoff is outside the resolved gap. Earlier affected cutoffs
remain downgraded. This does not upgrade an already-published invalid row;
assessed exports still start from its immutable published validity. Existing
`ledger-at-start/1` publications retain their exact historical replay semantics.
Intrinsic replay validity is transient and does not change standalone
materialization bytes.

For a sequence hole discovered at positionP after last provably continuous messageL, save L's ingress proof boundary (process/clock epoch, monotonic time and ingress ordinal), and define the affected interval in **closure/barrier order**, not solely by admitted message position. Every relevant cutoff whose saved barrier is strictly after that proof boundary and before the first closure that incorporates an accepted recovery anchorR is downgraded, including cutoffs whose admitted prefix is **still exactly L** because the feed was quiet. The latter equality must never exempt a later cutoff. An exact closure at L's proof boundary may retain that exactly-proven state; later barrier health alone does not extend L's proof across a subsequently discovered sequence hole. The first qualifying recovery closure and later ones can be eligible only after all required recovered messages are applied. Recovery never restores earlier missing history.

The correction stores an explicit inclusive `first_affected_cutoff_id` and exclusive `recovery_cutoff_id` (null/open until recovery) for each affected capture scope, plus native L/P/R positions and conservative UTC bounds. Resolve first_affected_cutoff_id from durable allocation/closure barriers, including quiet cutoffs; reference the proof raw ID and saved barrier evidence. New cutoff IDs monotonically allocated while the interval is open inherit its invalid status even across process restart; do not compare monotonic clocks from different epochs. Close it only with a durable resolution identifying the first recovery closure. If an epoch/proof boundary cannot be mapped, conservatively expand to all cutoffs after the last supported anchor and record uncertainty. Missing IDs or missing barrier evidence cannot produce a valid assessed row. Previously published and not-yet-published descriptors use the same cutoff-range rule. These cutoff bounds, rather than an admission>L predicate or ambiguous wall-clock time, are normative.

Corrections can be expressed by scope/epoch/position or cutoff ranges without enumerating every snapshot ID, but the matching rule must identify affected published and future descriptors unambiguously. Correction application is deterministic and idempotent by ledger ID/seq/hash. Conflicting records are not resolved by timestamp last-write-wins; mark assessed validity unknown/invalid and append an explicit resolution referencing every superseded correction. No correction can fabricate quote values or change original membership. Positive reinstatement needs recorded evidence and an explicit superseding resolution; a later healthy book alone cannot reinstate a gap.

## 5. Read, export, backup and replay

The initial reader exposes two named modes; there is no ambiguous unversioned `verified=true`:

- `as_published`: reproduce original bytes, admitted/applied positions and validity; label this as the historical observation, not current archive certification.
- `assessed_at_ledger`: require a pinned `(ledger_id, high_watermark_seq, head_hash)`, verify the complete ledger prefix, overlay all matching corrections through that position and return both original and assessed validity. Default research export pins the latest fully durable prefix available at export start, states that high-watermark in its manifest, and retains invalid/missing rows. Incomplete ledger/dependencies fail assessed export instead of returning valid rows. A previous pinned export stays reproducible when later corrections arrive.

The export includes raw/closure/checkpoint/metadata dependencies, pinned correction-ledger prefix and matching rules. Its assessed payload/hash differs from the original snapshot's immutable hash. `Latest assessed` means known through that ledger position, not immune to future discoveries. Applying later corrections to select a research sample can use future information; record the chosen ledger policy and population effects separately from causal input features. It does not put later metadata/quotes into an earlier receive-time snapshot.

Backup and restore copy closure/allocation records, owner transitions, full pinned correction prefix and open gap intervals together with raw/anchors/metadata. A remote commit advertises the ledger high-watermark it actually contains. Restore cannot claim current assessed validity from an older ledger copy; it labels the restored knowledge bound. Checkpoints likewise carry close/ledger references, never silently reset the ledger or omit an open gap. Verify raw-only vs checkpoint replay in both reader modes at the same high-watermark.

## 6. Required fixtures with expected outcomes

These are acceptance requirements to implement, not a statement of tests already run:

| Case | Required expected result |
|---|---|
| Normal cutoff, quiet connection and equal timestamps | Stable admission prefixes; native receipt/child order preserved; healthy unchanged book remains eligible. |
| Backward step after published cutoff | Later low-UTC record excluded from original cutoff; raw-only/checkpoint hashes equal; allocated cutoff not reused. |
| Clock step before barrier or delayed timer | Original monotonic deadline retained; contradictory epoch closure invalid; no late admissions. |
| Crash before/after durable close | Before: failed/missing cutoff; after: same membership and descriptor can be recovered, no current-data replacement. |
| Hole discovered after several published quiet cutoffs | L=position100 at10:00; cutoffs10:01/10:02 still admit100; gap discovered10:03. Both quiet cutoffs must be invalid in assessed view through before the recovery cutoff, although their admission equals L. Original descriptors stay unchanged; exact L proof boundary stays reproducible. |
| Open gap spans restart, segments and backup | Open range remains in restored ledger and covers later affected closures until explicit recovery anchor. |
| Same export before/after a later correction | Old pinned export reproducible; new high-watermark shows downgrade and records new assessed hash/population status. |
| Correction conflict or missing prefix | Assessed read fails or returns explicitly unknown/invalid; never silently chooses one valid claim. |
| Rotation overlap and metadata update around cutoff | One book owner path, no duplicate apply, dependency/metadata version follows saved positions. |
