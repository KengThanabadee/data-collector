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
| 5 remaining | Venue-isolated async capture, REST discovery/recovery, quotas, bounded configuration, CLI and health | Pending |
| 6 | Independent backup/restore interfaces, archive dependency verification, operational integration and compatibility packaging | Pending |
| 7a | Bounded Deribit BTC/ETH native-book evidence harness | Harness approved after fixes; [evidence](implementation/chunk-07a.json); first live run archived/replayed successfully; quiet-contract delta gate failed ([result](implementation/deribit-probe-quiet.json)) |
| 7b | Near-index option selection with preserved public index reference | Harness approved; [evidence](implementation/chunk-07b.json); second20s capture archived/replayed equally, BTC1/ETH4 advancing deltas; gate remains failed ([result](implementation/deribit-probe-near-index.json)) |
| 7c | Extend only the bounded probe duration to90s; retain byte/frame/request caps | Harness approved; [review](implementation/chunk-07c.json); 90s sample captured BTC12/ETH27 advancing deltas with equal native book replay ([result](implementation/deribit-probe-90s.json)); independent high-reasoning live-evidence audit approved |
| 7 remaining | Bounded public adapter verification, resource evidence, full-scope readiness report and remaining intended-host acceptance | Pending |

Original scope remains SPEC.md and the full feed catalogue. Each chunk may be split further if necessary; every implemented subchunk also gets its own fresh high-reasoning review. Disabled/unsupported/access-required/not-verified feed states remain visible. No unbounded capture, purchase, deployment, repository rename or push is implied by implementation work.

Chunk1 initial tests:7 new tests pass. Existing35 tests:29 passed in sandbox;6 loopback HTTP tests initially could not bind due to sandbox policy, and all6 passed under approved local-test execution. Total42 passing tests across those runs. Final chunk1:8 new tests pass,43 total with unchanged35 v1 tests. High-reasoning reviewer approved all repairs; exact hashes/outcomes are in the closure evidence.

Current v2 verification includes native decoding, archive verification, durable correction-ledger restore, local book materialization fixtures, fake-exchange capture/replay and one bounded live Deribit snapshot capture with equal raw-replayed book state. The first quiet-contract experiment did not meet the two-advancing-delta gate. The continuous multi-venue runtime, shared-time snapshot publication, exports/checkpoints, operations/backup integration and live acceptance are not yet complete.

Latest bounded live sample: Deribit90s,42 application messages including ACK, BTC12 and ETH27 advancing native deltas, no sequence invalidation in that sample, verified raw segment and equal full book replay. Root independently reconstructed Decimal levels/native predecessor chains from raw without the capture reducer; [checks](implementation/probe-independent-checks.json). This establishes only the sampled native book path. Current259 v2 tests pass; existing35 v1 tests were verified earlier as described above.

The90s sample evidence was independently audited by `dc_chunk7c_duration_review`:50raw records,42WSframes,41native book messages, exact predecessor chains and independently reconstructed Decimal books match. The report hash is pinned in [chunk7c review evidence](implementation/chunk-07c.json).
