# Native checkpoints and continuation

This is the first native-state component of the checkpoint work required by
SPEC.md. It supports Deribit full and Bybit standard snapshot-seeded books.
Buffered bootstrap and continuous capture integration remain unfinished. Native
cutoff continuation is available through `materialize_from_checkpoint`.

## Stored state and trust boundary

`bookstate.py` preserves exact L2 quantities/prices, native profile/depth/resource
cap, applied and observed watermarks, last fingerprint, epoch/state/reason, and
anchor/last source identities. A display `view()` is insufficient: an ignored
pre-anchor delta can raise the observed watermark without changing that view,
then make a later snapshot stale.

`checkpoints.py` builds state only after the existing raw materializer validates
an immutable closed cutoff. It retains the full frontier descriptor, including
plan/config/metadata/universe, raw manifests, owner dependencies and ledger pin;
per-stream last receipts; last-applied raw hashes/receipts/ingress boundaries; and
terminal latches. A last-applied receipt does not establish sequence proof for
health-only Bybit feeds. The ledger pin references retained open-gap history;
this module does not replace or restore the ledger projection.

Publication uses immutable data writes and a manifest committed last. Read/verify
checks canonical bytes, identity and bounds, reconstructs models, then rebuilds
the complete checkpoint from pinned raw and compares every byte. An altered
watermark with a newly computed checksum still fails this comparison. Raw and
existing snapshot/export files remain unchanged.

## Replay to a later cutoff

`materialize_from_checkpoint(encoded, expected_id, segments, target_close, plan)`
first re-verifies the saved checkpoint against raw, then renders a later cutoff
for the same exact frozen plan. The target must contain the checkpoint close in
its verified prefix and extend its ledger knowledge. The resulting descriptor,
payload and snapshot ID match ordinary raw-only materialization exactly.

The native renderer carries forward hidden model state, cumulative diagnostics
and terminal latches. It skips only binding receipts already admitted at the
checkpoint's per-stream sequence frontier. A later-admitted receipt can precede
the old close physically; it must still be dispatched. Metadata, receipt/hash/
config checks and all owner/clock/cutoff/ledger/discovery validation remain full
prefix. A newly introduced audit policy or changed owner context can reinterpret
older native input, so it uses raw native fallback. Nonextending admissions after
clock changes also fall back; checkpoint state cannot improve an invalid cutoff.

Optional statistics explicitly count only renderer binding dispatches/skips.
A regression with129 admitted native receipts and2 suffix receipts reports
129 skips and2 dispatches, with the complete artifact equal to raw-only replay.
Checkpoint verification and certification scans are excluded from those counters
and still perform full-prefix work. This is not end-to-end replay acceleration.
Persisted checkpoint /1 bytes are unchanged; extra diagnostic/policy context is
recomputed during existing raw verification and kept transiently.

## Bounded synthetic experiment

Run `.venv/bin/python experiments/native_checkpoint_probe.py --output experiments/results/native-checkpoint-scaling.json`.
The saved measurement is [native_checkpoint_probe.json](experiments/native_checkpoint_probe.json),
with runtime/platform and exact source hashes. One constant-size synthetic
Deribit book was replayed from its anchor, saved1000 events before the end,
restored and given the same suffix. Full native state hashes matched.

| Native events | State bytes | Full processing seconds | Restore +1000 events seconds | Equal state |
|---|---|---|---|---|
| 10,000 | 692 | 0.0839 | 0.0083 | True |
| 100,000 | 694 | 0.8190 | 0.0081 | True |
| 1,000,000 | 696 | 8.4509 | 0.0083 | True |

These are single-run native model measurements on the local development host,
not raw archive replay timings, capture throughput, RSS or production acceptance.
The checkpoint reader still verifies the full raw prefix; it does not use the
fast model-only path measured above.

## Remaining integration

Keep current256-dependency/64KiB audit and full-prefix scan limits in force.
Before enabling long-running capture, integrate the native suffix renderer into
runtime checkpoint selection, compact exact raw-prefix proofs, and incremental ledger/owner/cutoff/metadata/
clock projections without losing pending audits or open negatives. Add buffered
seed/buffer/clock state. A terminated source must remain terminated; a checkpoint
cannot make a new connection inherit old health or an old clock become current.

Then measure10k/100k/1m raw-event histories with gaps around checkpoint boundaries,
quiet cutoffs, owner/clock changes and open negatives. At identical close/ledger
pins, raw-only and checkpoint replay must agree in both reader modes, while
native suffix work stays bounded by checkpoint interval. This experiment and
intended-host acceptance have not yet passed.


## Managed ledger measurement before further optimization

Run `.venv/bin/python experiments/ledger_refresh_probe.py --output experiments/results/ledger-refresh-scaling.json`.
The [saved result](experiments/ledger_refresh_probe.json) records source hashes,
machine/Python details, three unprofiled samples per size and a separate profile.
It uses the actual `DurableLedger.pin()` path over a real rotated immutable
segment, one managed Deribit instrument, two retained price levels and one owner
declaration. Setup/fsync costs are excluded. Repeated filesystem reads are warm;
there are no gaps, corrections, cutoff publications, concurrent capture or
network requests in the measured interval.

| Native events | Physical records | Encoded raw bytes | Median unchanged pin |
|---|---|---|---|
| 100 | 103 | 138,719 | 0.034 s |
| 1,000 | 1,003 | 1,353,720 | 0.329 s |
| 10,000 | 10,003 | 13,559,921 | 3.207 s |

Every repeat preserves the ledger pin. The separate profiled call dispatches
all 10,000 old native messages again. At that size, profiled cumulative pin time
is 9.850 s, `RawRecord.from_bytes` 5.509 s, `_book_message` 0.523 s and
`SnapshotBook.receive` 0.245 s. These are nested cumulative timings with profiler
overhead: do not add them or compare their seconds directly with unprofiled
samples. Generator call counts include resumptions, not just records. The
100,036 `from_bytes` calls expose repeated decoding/verification work in this
specific immutable-segment path.

Decision: defer the proposed native-discovery-only in-memory cache. The
measurement does not support prioritizing another mutable projection while
full-prefix raw/owner/evidence scans remain dominant. Next investigate reuse of
already verified immutable input within one certification operation, with raw
mutation/removal and pending-negative regressions, before introducing persistent
cache state. Preserve all existing verification gates and scan limits. This is
an ordering decision, not proof of a safe reuse design, predicted speedup or a
waiver of continuous-capture capacity requirements. Measure full-universe load
and actual host acceptance separately.
