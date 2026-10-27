# Native checkpoint prerequisite

This is the first native-state component of the checkpoint work required by
SPEC.md. It supports Deribit full and Bybit standard snapshot-seeded books.
Buffered bootstrap and continuous capture integration remain unfinished.

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
Before enabling long-running capture, integrate verified native suffix replay,
compact exact raw-prefix proofs, and incremental ledger/owner/cutoff/metadata/
clock projections without losing pending audits or open negatives. Add buffered
seed/buffer/clock state. A terminated source must remain terminated; a checkpoint
cannot make a new connection inherit old health or an old clock become current.

Then measure10k/100k/1m raw-event histories with gaps around checkpoint boundaries,
quiet cutoffs, owner/clock changes and open negatives. At identical close/ledger
pins, raw-only and checkpoint replay must agree in both reader modes, while
native suffix work stays bounded by checkpoint interval. This experiment and
intended-host acceptance have not yet passed.
