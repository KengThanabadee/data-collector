# DC v2 specification review closure

Date: 23 October 2026 (Asia/Bangkok). Scope: repair and independently re-review the saved v2 specification. No runtime implementation, live data capture, deployment, account, trial, commit or push was performed in this repair phase.

## Findings and verification

| Finding | Saved repair | Evidence and outcome |
|---|---|---|
| P1 Bybit full-book bootstrap/continuity | [FEEDS.md YF](FEEDS.md#yf-bybit-full-book-y3y4): exact seq+u seed match, then consecutive u, seq may skip, duplicate/conflict handling, reset-before-stale, seeded-depth limits. | Re-opened [current official procedure](https://bybit-exchange.github.io/docs/v5/websocket/public/full-ob); both reviewers confirm original finding closed. |
| P1 Backward-clock replay membership | [SNAPSHOTS.md sections1–3](SNAPSHOTS.md): immutable durable cutoff allocation/close records, monotonic barriers, admission vectors, no timestamp-only replay or reopened cutoffs. | Both reviewers confirm late low-UTC messages cannot enter previously closed snapshots under the revised contract. Required fixtures cover clock rollback, delayed barrier and crash. |
| P2 Late-discovered gap validity, escalated to P1 for quiet-boundary counterexample | [SNAPSHOTS.md sections4–6](SNAPSHOTS.md): append-only corrections, explicit half-open cutoff interval, pinned ledger reads/exports/restore, open intervals across restart. | First repair review found admission>L would omit quiet cutoffs. Corrected to closure/barrier order including admission==L, with exact position100/quiet10:01–10:02 expected downgrade. Both reviewers re-read final repair and approve. |
| Coordinator self-reference clarification | Dedicated control:<scope> stream, outside the socket admission vector it describes. | Fresh reviewer explicitly confirms closure self-reference removed. |

## Independent final reviews

- `review_dc_v2_spec`: **APPROVE** the revised SPEC v2. Original findings closed; no additional actionable defect found within reviewed scope.
- `dc_v2_replay_final_review`: **APPROVE** revised specification contracts. Quiet-cutoff correction, restart behavior and missing-mapping failure now explicit; no remaining actionable implementation-blocking contradiction found within reviewed scope.

The first reviewer revisited the entire repaired contract, and the second was a fresh independent review of the saved revision. Neither edited the files. Source/catalogue version bookkeeping and this closure note were updated after their approval; the approved normative behavior was not changed.

## Completion boundary

This closes the **specification findings**, not runtime acceptance. Live adapter evidence, executed fault/clock/ledger fixtures, full-universe capacity, independent backup/restore and intended-host24h soak remain future gates in [ACCEPTANCE.md](ACCEPTANCE.md). No claim of a bug-free future implementation or already-passing runtime tests is made. Broad Deribit/Binance/Bybit BTC/ETH product scope and v1 compatibility remain intact.

Local delivery verification: current-document links resolve; candidate TOML parses and stays disabled; queue budget is consistent; v1 specification bytes equal Git HEAD:SPEC.md; no runtime/package/deployment/config changes; git diff whitespace checks pass. Hashes below pin the saved normative documents at closure.

| File | SHA256 |
|---|---|
| `SPEC.md` | `b4118a506380963991cc22f1c4c674be6593b17d40d951b44414fe173ec55111` |
| `docs/v2/SNAPSHOTS.md` | `b43836fd168e4ca267e3b0ebcef3856cee81b1e0a827dd95bc95d3d97ae377ff` |
| `docs/v2/FEEDS.md` | `cafc961c958ab1cb2b5ab80117f2665994fa19bd742296a313c90b3c568af539` |
| `docs/v2/ACCEPTANCE.md` | `e6f79fbc4e91c9715e31480fd2c314a654c92b7ec8cc415a10e49df4f330d234` |
| `docs/v2/pilot-budget.toml` | `e28030a75390331ddf06802512bf5bd73ce8006364ca53b171a2099831477560` |
