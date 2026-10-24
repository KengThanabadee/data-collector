# Cryptofeed adoption assessment

Evaluate reuse against the unchanged production v2 specification. Existing code
is retained until a measured adoption decision identifies the replacement boundary.
No exchange account, credentials, subscription purchase or deployment is involved.

## Pinned candidate and isolation

- Repository: https://github.com/bmoscon/cryptofeed
- Commit: `f2daed5c75f4514669e2da54a4cbb8d1f5037a47`, package 3.0.2.
- Inspected `pyproject.toml`: Python >=3.13; experiment uses isolated Python3.14.6.
  Production collector remains on Python3.12 with unchanged dependencies.
- License declares AGPL-3.0-or-later and an additional attribution term. Preserve
  attribution and evaluate the intended integration against these actual terms;
  a process boundary is not by itself a licensing conclusion.
- Fetch source with `git clone --depth 1 --filter=blob:none --sparse`; select only
  `cryptofeed examples tests`. A full shallow clone includes large sample captures.

## Decision gates

1. Confirm exact payload/type/order roundtrip across reconnect and rotation.
2. Confirm native snapshot/delta replay, sequence-hole behavior, and final-book
   equality using sampled data. Synthetic tests and live evidence remain separate.
3. Measure abrupt process exit and distinguish callback completion, flush and
   durable acknowledgement. Require the existing v2 fsync/immutable evidence
   contract regardless of which feed handler is selected.
4. Run a reviewed bounded public Deribit BTC/ETH options/perpetual capture,
   preserving full raw ticker fields, native discovery and subscription receipts.
   Force one reconnect; replay and compare sampled callback/book states. Hard
   time, per-message, total-byte, request and shutdown budgets are required.
5. Map the complete FEEDS.md catalogue before deciding full replacement versus
   partial reuse: native identifiers, unsupported profiles, original REST error
   bodies, lifecycle, historical metadata, shared quotas and timestamp provenance.
6. Record a decision with executable evidence and implementation boundary. Then
   implement that decision in reviewed chunks; do not silently reduce v2 scope.

## Stage CF01: offline probe

Run from this repository using the separate evaluated environment:

```sh
/tmp/dc-cryptofeed-venv-20261024/bin/python -B experiments/cryptofeed_probe.py \
  --source /tmp/dc-cryptofeed-source-20261024 \
  --output data/cryptofeed-evaluation/offline-03
```

The output directory must be new. The harness verifies the Git pin, clean inspected
source files, installed source equality, dependency versions and license hash.
It records synthetic text/binary bodies, two connection epochs and file rotation;
checks upstream Deribit book/replay equality and a predecessor-hole exception;
and executes a child exiting abruptly after its recorder callback completes.
A failed native fixture with string-valued prices was retained in `offline-01`;
`offline-02` corrected prices to native JSON numbers; `offline-03` additionally
verifies the expanded source bundle. No live traffic occurred in these runs.

Observed `offline-03`: six exact payloads, two connection epochs, one rotation,
matching two native book states, detected sequence hole, zero recorder fsync calls.
The abrupt-exit child completed one callback, then left a zero-byte PCAP; no
message was recoverable. This establishes that this recorder callback is not a
v2 durable acknowledgement, not a claim that Cryptofeed promises that contract.
The selected upstream unit suite also passed 12 tests (61 non-unit tests deselected).

## Preliminary source coverage

| v2 family/contract | Evidence at pinned source | Current conclusion |
|---|---|---|
| Deribit single-leg book/trade/ticker | `exchanges/deribit.py` public100ms subscriptions and predecessor check | Candidate for live reuse test |
| Deribit combo discovery | Parser explicitly skips option/future combos | Custom extension remains necessary |
| Binance spot, UM and CM | Separate Binance/BinanceFutures/BinanceDelivery adapters | Source-present; not live-qualified here |
| Binance options | No options/eapi adapter among pinned Binance classes | Unimplemented by this candidate |
| Bybit spot/linear/inverse | Separate public endpoints in `exchanges/bybit.py` | Source-present; not live-qualified here |
| Bybit options/full/RPI/spreads | Pinned endpoints/topics do not implement these v2 profiles | Custom extensions remain necessary |
| Exact REST acquisition | GET uses `response.text()`; error handling precedes raw callback | Byte fidelity/error-body qualification needed |
| v2 durability/as-of/corrections | PCAP flush and replay exist; no recorder fsync or v2 close/ledger contract found | Existing archive/snapshot logic remains required |

## Decision — 24 October 2026

Continue with the existing collector as the production v2 core. Do not adopt
Cryptofeed as a production dependency or replace existing modules now. This
decision follows the user's latest direction after comparing reuse costs; it is
not a claim that our unfinished live adapters outperform Cryptofeed.

The pinned candidate provides useful exchange transports, subscriptions, native
book handling and capture/replay. However, adoption would still retain our
durable archive, shared-time closure, provenance and correction contracts, require
extensions for missing catalogue profiles, and introduce Python-version and
license integration work. Offline evidence does not establish a net reduction
in implementation or maintenance effort for this repository.

Defer CF02 live qualification rather than adding a second integration track.
Keep the approved offline experiment and exact pin as reference evidence. A
future proposal to reuse a specific adapter must name the code it replaces,
qualify that feed's native semantics and raw preservation, and assess its actual
license terms. No production implementation has been removed and the complete
v2 feed catalogue and acceptance gates remain unchanged.
