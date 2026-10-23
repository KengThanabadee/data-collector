# DC v2 feed catalogue

Frozen documentation review: **23 October 2026, Asia/Bangkok**, catalogue `dc-feeds/2.1`. This is a documentation freeze, not a live capture report. Sources below were opened during the review. Binance redirects and inaccessible documentation are recorded explicitly. [Source acquisition evidence](source-evidence.json) separates the successful web-reader review from bounded direct-download attempts; four direct source bodies were saved and other direct downloads failed DNS/TLS. This is not a claim that every original page/schema body is already bundled. Before shipping an adapter, archive the exact applicable schema/document body, retrieval time, redirect chain and SHA256 with its fixture evidence; do not substitute a mutable URL for a release source bundle.

Capability vocabulary: `supported` = current official documentation establishes a public capability; `unsupported` = explicitly excluded by that feed; `access_required` = needs entitlement/authentication; `not_verified` = evidence incomplete; `not_enabled` = available profile intentionally not selected. Capability and activation are separate columns in the release manifest. **All v2 runtime activation is currently `not_enabled`; all capture readiness is `unverified`.** A documented capability can have a mandatory implementation evidence gate.

Every release manifest expands the family rows below into venue/environment/product/native-symbol/feed records and freezes: capability/reason/source IDs; selected profile; endpoint/route/topic; seed/depth limit; cadence/aggregation; native time units; sequence/ID scope and resets; trade side/exclusions; rate-pool/connection/subscription budgets; bootstrap/recovery rule; metadata/universe versions; evidence and readiness. Startup fails if any enabled row lacks a resolved value or gate evidence. Null/unknown values are permitted only for explicitly disabled rows.

## 1. Product and currency coverage

| Venue / native family | BTC/ETH discovery and variants | Book profile / capability | Trades + references / capability | Mandatory boundary |
|---|---|---|---|---|
| Deribit spot native matching | `get_instruments`, currency variants and routing metadata | Full L2 `book.<id>.100ms`: supported [D1,D2] | Public trades/ticker: supported for native instruments; index dependencies by metadata [D3,D4] | Unit mapping is per contract, not the derivatives USD amount rule applied to spot. |
| Deribit spot routed | Same discovery; retain actual venue/routing flags | Deribit-routed book/quote/ticker: supported [D3] | Routed public trades/candles through Deribit: unsupported [D3] | Missing trade feed is a declared exclusion, not silent capture failure or native Deribit flow. |
| Deribit perpetual / dated future | `kind=future` plus settlement/contract metadata; coin and linear variants | Full L2 at100ms supported; raw access_required [D1,D4] | `trades.<id>.100ms`, ticker, index, perpetual funding fields and settlement references supported [D4,D5] | Retain USD vs coin amount units and predicted/current/realized funding semantics. |
| Deribit options | Single-leg discovery by underlying and currencies, all listed strikes/expiries/states | Same full L2 supported; raw access_required [D1,D5] | Option trades/ticker, IV/Greeks/OI and pricing underlying supported [D5] | Never treat mark IV as midpoint IV. |
| Deribit combos | `get_combo_ids/get_combos/get_combo_details`, separate namespace/legs | Combo-book public availability per actual returned ID: not_verified for this release; explicit fixture gate [D5] | Public combo trade-tape coverage: not_verified; private `user.combo_trades` is access_required and outside DC public scope [D5] | A documented private channel is not a public market tape. Do not manufacture combo books from leg sums. |
| Binance spot | `/api/v3/exchangeInfo`, underlying/base BTC/ETH variants, native filters | Diff L2, REST seed5000/side,100ms supported [B1] | Native `@trade`, `@bookTicker`, ticker supported; metadata REST [B1] | Seed does not establish untouched levels beyond5000. |
| Binance UM perpetual / dated future | `/fapi/v1/exchangeInfo`, native contract type/settlement, verify product classification | Diff L2+seed1000/side;100ms candidate supported [B2,B3] | `@aggTrade`, mark/index/funding + ticker/contract info supported subject to schema fixtures [B3,B4] | UM/CM merged routing does not merge instrument identity or budgets incorrectly. |
| Binance CM perpetual / dated future | `/dapi/v1/exchangeInfo`; preserve inverse, contract multiplier, `st` and `ps` | Updated integrated routing documented; exact CM seed/bridge gate not_verified until schema/capture evidence [B3] | Shared-host/merged arrays supported; per-stream identity fixture mandatory [B3] | Do not inherit UM depth budget or bridge without checking current CM response/schema. |
| Binance options | `/eapi/v1/exchangeInfo`, BTC/ETH `optionSymbols`, exact contract filters | Current public diff stream documented; seed1000,100/500ms; supported documentation, gated runtime [B5,B6] | `@optionTrade`, option ticker; market route index/mark/OI/new-symbol supported documentation [B5,B7] | Gate BO below resolves current routes and recovery; legacy `nbstream` is not an approved route. |
| Binance combos/spreads | No confirmed public listed-combo discovery/book schema in reviewed sources | not_verified, not_enabled | not_verified | Never infer Deribit combo semantics or use private block negotiation as public books. |
| Bybit spot | `/v5/market/instruments-info?category=spot` [Y1] | Full L2 delta200ms+seed≤10000; standard depth1000/200ms alternative supported [Y2,Y3,Y4] | `publicTrade.<symbol>`, ticker supported [Y5,Y6] | Standard/full excludes RPI; full seed cap still limits untouched depth. |
| Bybit linear perpetual / dated future | category linear, native `contractType`, settlement and price units [Y1] | Same full or standard profile supported [Y2,Y3,Y4] | Trades, ticker mark/index/OI/funding; REST realized history supported [Y5,Y6] | Prelaunch/auction state has explicit unavailable book semantics. |
| Bybit inverse perpetual / dated future | category inverse, multiplier and inverse units [Y1] | Same full/standard profiles supported [Y2,Y3,Y4] | Same trade/reference family supported where fields exist [Y5,Y6] | Do not equate inverse and linear sizes/hedge P&L. |
| Bybit options | category option, baseCoin BTC/ETH, expiry/strike/right [Y1] | Standard L2 depth100 at100ms supported; full-depth profile unsupported for options [Y2,Y3] | `publicTrade.BTC/ETH` and per-symbol ticker/IV/Greeks supported [Y5,Y6] | Option ticker is a separate schema, not a derivatives delta ticker. |
| Bybit listed spreads | Spread instrument/leg discovery must be verified separately | Standard spread L2 depth25/20ms documented supported, route/discovery gate [Y9] | Public spread trade/ticker documented, exact metadata/IDs gate [Y10] | Use spread identity/legs and separate feed coverage, not duplicated outright quantities. |

All target families remain in scope even when a feed is disabled pending evidence. Release reports list blocked and empty-by-discovery families separately. Discovery records a family with zero matching contracts rather than pretending the exchange cannot support it.

## 2. Supplementary families and inclusions

| Family | Profile and capability | Granularity, exclusions and acceptance |
|---|---|---|
| Bybit RPI book | Separate `orderbook.rpi.<symbol>`, depth50/100ms, spot/perp/futures supported; options unsupported [Y7] | Levels contain non-RPI and RPI size components. Retain both, never add entire RPI-profile sizes to standard/full sizes. Off in initial core trial; dedicated RPI acceptance later. |
| Bybit liquidation | `allLiquidation.<symbol>`, derivatives public500ms batching, supported [Y8] | Provider describes all-liquidation family, not legacy throttled liquidation. Keep liquidation side/bankruptcy price semantics separate from matched trades. No guarantee through a capture outage. |
| Bybit insurance / ADL / price-limit statistics | not_verified exact per-product schema/cadence; not_enabled | Catalogue and fixture needed before activation. Aggregate public risk statistics are not individual participant orders. |
| Binance futures liquidation | `forceOrder` snapshot family, not_verified current canonical schema in this review; not_enabled | Candidate is throttled/latest-per-window, not an exhaustive liquidation tape. Require current schema/cadence/filter source before activation; merged identity rules apply. |
| Binance futures agg trades | `@aggTrade`, supported family [B3] with granularity fixture gate | Save aggregate ID and first/last native trade IDs when present, maker side and time. Insurance/ADL exclusions must be established from current schema before enabling; cannot claim every matching-engine fill. |
| Binance futures RPI | Mentioned integrated `@rpiDepth` family [B3]; exact components/cadence/coverage not_verified, not_enabled | Separate profile; do not union/sum with standard book. |
| Binance spot risk / options liquidation / combos | not_verified, not_enabled | No inferred parity with futures. |
| Deribit liquidation/risk | Native trade liquidation flags if present; separate public risk-feed completeness not_verified [D5] | Preserve original flags; do not infer exhaustive liquidations from them. |
| Deribit Starbase / institutional L3 | access_required; private connectivity/selected-client entitlement, not_enabled [D2] | Current DC internet-WebSocket profile is L2. No hidden/L3/queue-position claim; adding institutional capture requires a separate entitlement/transport profile. |
| Funding/settlement/history, all venues | REST complement supported as family; exact endpoint rows below | No funding for a spot or expiring option unless metadata actually establishes it. Predicted funding ≠ realized funding; historical acquisition time ≠ time known to collector. |

Reference acquisition allowlist: Deribit `get_instruments`, `get_combo_*`, `get_book_summary_by_currency`, `get_index_price`, `get_delivery_prices`, `get_funding_rate_history`, public trade-history methods; Binance spot `exchangeInfo/depth/trades/aggTrades/klines`, UM/CM `exchangeInfo/depth/premiumIndex/fundingRate/openInterest` and documented delivery/contract fields, options `exchangeInfo/depth/index/mark/openInterest/exerciseHistory/trades`; Bybit `/v5/market/instruments-info`, `orderbook/full_orderbook`, tickers, funding/history, delivery-price and public recent-trade endpoints. **These are intended families, not permission to call arbitrary private endpoints:** release expands exact path/params/weights/pagination/retention per profile from official schema. Unresolved history bounds or absent pagination keep that backfill job disabled; live capture need not wait for optional backfill. Deribit History API trades are trades, not historical book snapshots.

## 3. Identity, timestamps and bootstrap recipes

### D: Deribit [D1–D5]

JSON-RPC `wss://www.deribit.com/ws/api/v2`; production/testnet roots are distinct. Default public interval100ms; raw is authenticated and never silently requested. Initial `book.<id>.<interval>` full snapshot, then `[action,price,amount]` updates; require `prev_change_id == prior change_id` per book/epoch. Duplicate same-ID/hash is a derived no-op; conflict or broken predecessor invalidates and resubscribes for a fresh snapshot. Preserve full initial depth and native action semantics. Timestamps are native milliseconds; trade IDs/sequences stay scoped to native instrument, not timestamps or currency alone. Preserve native `direction`; validate its taker mapping separately. Ticker fields are own native values, not recomputed Greeks.

Record subscribe responses, heartbeat and connection-scoped state. Initial proposal heartbeat10s, watchdog30s, control budget1request/s, at most8WS plus4HTTP sockets across this DC instance; other clients share the documented32connections/IP limit [D4]. Public REST credit costs and max subscription capacity are release gates: freeze the observed/current account/IP credit policy and shard plan before full universe. Pilot max64symbols/connection and50channels/subscribe is a collector cap, not a vendor guarantee. Reset/reconnect refreshes subscriptions; `.raw` credentials never enter archive.

### BS: Binance spot [B1]

Use `wss://stream.binance.com:9443`, lower-case topics; default milliseconds, optional microsecond mode must be a separate feed config. Buffer diff messages, fetch `/api/v3/depth?limit=5000`, reject a seed older than first buffered `U`, discard messages through seed, bridge its update range. Thereafter a future `U > local_u+1` is a hole; discard old ranges and apply absolute sizes/zero deletions. Freeze exact inclusive boundary fixtures from current official algorithm; no UM `pu` assumption. Native trade `t` is scoped to symbol/incarnation; `m=true` maps buyer-maker to sell aggressor only under the validated native rule. Maintain `trade` and `aggTrade` as distinct sources, never summed.

Book coverage is seeded5000 plus updated known levels; unknown deeper levels stay unknown even if best prices later approach the seed boundary. Partial depth snapshots are a separately labelled alternative, not a replacement full seed. BookTicker has its own update semantics; it cannot anchor an L2 book.

### BF: Binance UM/CM [B2–B4]

Public depth/trades use `.../public/...`, mark/reference use `.../market/...` on approved fstream/dstream route. A bare unrouted connection does not establish mark subscription. Buffer then REST seed; UM seed1000, discard `u < lastUpdateId`, first accepted range spans seed, subsequent `pu == previous u`, zero deletes. CM has its own gated schema/seed fixture; do not assume identical depth from an old API. Native `E/T` times are ms unless the exact schema says otherwise.

Merged all-market streams on fstream/dstream contain the same UM+CM universe. Choose one primary route per logical stream; redundancy is opt-in with both raw copies and derived native-ID dedup. `st=1/2` distinguishes UM/CM where provided, `ps` is pair metadata. If `st` is absent, require unambiguous versioned discovery mapping; conflict quarantines the record. Shared fapi/dapi REST pool is2400weight/min [B3], so candidate collector budget1200 shared (not1200 each), weighted by endpoint/depth. No trading quota use. Header usage from other clients reduces available budget.

### BO: Binance options — resolved documentation, gated runtime [B5–B8]

The current official catalogue specifies **fstream `/public`** for contract diff `{symbol}@depth@100ms` (500ms alternative), `U/u/pu`, and `{underlying}@optionTrade` (50ms batch cadence, native `t`, `X=MARKET/BLOCK`, maker flag). Partial book profile is5/10/20levels; it is not the diff stream. REST seed is **eapi `/eapi/v1/depth`**, max1000, `lastUpdateId/T`. Market route documents index, option mark, OI and new symbol. Native timestamps ms. This resolves the earlier uncertainty about which documentation describes the current public stream; legacy examples are not sufficient.

**BO gate before enabling:** save current exchangeInfo and choose one actually listed BTC and ETH option; prove subscription ACK and contract-vs-underlying stream naming/case; pair REST seed IDs with buffered diff IDs; establish the exact first-event inclusive bridge and subsequent `pu` chain with a fixture; force reconnect/resync and verify book vs independent REST at an aligned position; verify reference schemas/cadences and connection limits applicable to options on shared host. The catalogue gives fields but no independently validated current option-specific bootstrap run here. A proposed futures-like bridge is a hypothesis until proven, not copied into a ready adapter. Access/geo restrictions must be reported with exact response. **No live options request or capture was made in this spec task.**

### YS: Bybit standard/options/spread [Y2,Y5,Y6,Y9,Y10]

Public v5 connections separate spot/linear/inverse/option; spread route/discovery remains a gate because its connect page was inaccessible in this review. Standard/options/spread initial WS snapshot replaces state; then absolute deltas/zero deletes. New snapshot or `u=1` resets the book epoch. Store `u`, cross-sequence `seq`, message `ts` and engine `cts` when supplied (ms). **Monotonic does not prove consecutive:** do not demand `u+1` or use `seq+1` without a documented per-feed rule. Standard fixtures must cover snapshot replay, duplicate/reorder/reset; confidence stays health-only where no predecessor/checksum proof exists. Reconcile with REST periodically without treating a later snapshot as repair of earlier history.

`publicTrade.<symbol>` uses per-trade `i`, native taker `S`, `T`, RPI/block flags; option subscription uses baseCoin and its message ID is preserved. Multiple messages can share `seq`; keep every child and dedup only with validated native trade identity. For options, validate the child-ID scope from fixtures before derived dedup. Ticker spot/option snapshots and futures snapshot/delta must use separate schemas: missing delta field means unchanged only for a documented delta channel, otherwise missing.

Reference/lifecycle cadence freeze: Bybit `tickers.<symbol>` uses100ms derivatives/options and50ms spot [Y6]; options/spot are snapshots, futures can be deltas. Binance options `/market` index and optionMarkPrice use1000ms, new-symbol50ms, OI60s by underlying+expiry [B7]. Deribit lifecycle uses `instrument.state.{kind}.{currency}` [D2]; ticker100ms candidate and index/funding notification intervals require exact channel-schema fields in the release manifest. Funding-history polling candidate300s plus settlement-boundary reconciliation must save realized event/effective timestamps separately. Other unresolved reference cadences stay disabled until their current schema is frozen.

### YF: Bybit full book [Y3,Y4]

Use `orderbook.full.<symbol>` with REST `/v5/market/full_orderbook` (seed≤10000/side, delta200ms). Follow the official **Full Order Book Synchronization Procedure** [Y3]: buffer WS first, reject decreasing `seq`, restart buffering on broken `u`; refetch an old seed and discard buffered entries below its `seq`. Initialize only at a buffered delta matching **both seed `seq` and `u`**; that matching delta is already represented by the seed and is not applied twice. A same-`seq`/different-`u` seed requires refetch. If the matching delta is not yet buffered, wait only within bootstrap/byte bounds, otherwise restart. Archive the matched pair and apply only subsequent deltas.

Within the same book epoch, the documented next delta must have `u == local_u + 1`; `seq` may skip. A future `u` gap invalidates and resyncs; an old delta is ignored in derived state. A repeated `u` with identical normalized native book payload is a no-op; conflicting content invalidates rather than overwrites. Check `u=1` reset **before** the stale-ID branch; clear and reseed. Delist/auction empty reset becomes `NO_BOOK`. Full-book continuity may be `sequence_proven` once the matched seed and consecutive chain are verified; this rule must not be generalized to standard/options/RPI/spread books. Seeded depth still excludes unknown untouched levels beyond10000, and standard/full excludes RPI. **YF gate:** fixtures must prove exact seed match, old/mismatched seed retry, `u+1`, skipped `seq`, duplicates/conflicts, `u=1` and gap-triggered resync; live capture/reconciliation remains required before readiness.

## 4. Connection/rate plan

| Pool | Official constraint / source | Collector candidate cap and recovery |
|---|---|---|
| Binance spot |5incoming control frames/s,1024streams/connection,300connection attempts/5min/IP [B1] |2JSONcontrols/s with token reserve for pong,256streams/socket,4sockets; max1newconnection/5s. 24h rotation candidate23h50; protocol heartbeat tracked separately. |
| Binance futures |10incoming/s,1024streams,24h lifetime, server ping3min/pong timeout10min [B4] |2JSONcontrols/s,256streams/socket,8sockets total UM/CM; rotate23h50; pong immediately. Split public/market sockets and dedup overlap. |
| Binance options | Current shared-host catalogue verified, options-specific limits not independently frozen [B5,B7,B8] | Disabled until BO gate resolves effective limit. Proposed2controls/s/128streams/socket are collector caps only, not proof of permission. |
| Deribit |32connections/IP inclHTTP, authenticated raw, account/IP credit policy [D4,D5] | Bounds in D; public REST initial1request/s + per-method cost limit, shared across live/recovery/backfill. Unknown credit cost disables that job. |
| Bybit |HTTP600requests/5s/IP;≤500connection creations/5min;≤1000marketdata connections/category [Y11] |HTTP5requests/s total plus endpoint limits,4sockets/category,≤1connect/5s; ping20s/watchdog40s candidate. Args/request and message-size caps must be frozen from current connect schema before activation. |

All connections use bounded exponential reconnect1/2/4…60s plus jitter, respect Retry-After/ban delays, and retry only within capture budgets. Bybit HTTP403frequency ban waits at least10min per source; do not hammer reconnect. On Binance429 pause the shared pool,418stop until declared expiry. Heartbeat reserves bypass only the collector's JSON-control allowance, never an exchange total limit. Rotation overlap cannot be declared valid until new bootstrap/identity checks complete. Discovery, REST recovery, metadata/history and legacy v1 on the same IP must share budgets or run on isolated IPs explicitly; a second process is not a second quota.

## 5. Official source register

Each ID below means documentation opened23Oct2026; no live feed certification. Store this catalogue's hash in release config. Binance source pages report modifications22Oct2026 in several cases. Source body archival/hashes are mandatory in implementation, not claimed present here.

- D1 [Deribit full book](https://docs.deribit.com/subscriptions/orderbook/bookinstrument_nameinterval).
- D2 [Market-data best practices](https://docs.deribit.com/articles/market-data-collection-best-practices).
- D3 [Spot routing](https://docs.deribit.com/articles/spot-trading-venues).
- D4 [Connection management](https://docs.deribit.com/articles/connection-management-best-practices).
- D5 [Options collection, combos and settlement](https://docs.deribit.com/articles/options-data-collection-best-practices).
- B1 [Official spot streams and local-book algorithm](https://github.com/binance/binance-spot-api-docs/blob/master/web-socket-streams.md).
- B2 [UM local-book algorithm](https://developers.binance.com/docs/derivatives/usds-margined-futures/websocket-market-streams/How-to-manage-a-local-order-book-correctly).
- B3 [UM/CM integration, identity and shared pool](https://developers.binance.com/docs/derivatives/coin-margined-futures/Important-CM-UM-Integration-Notice).
- B4 [Futures connect/routing](https://developers.binance.com/docs/derivatives/usds-margined-futures/websocket-market-streams/Connect).
- B5 [Current options public WS catalogue](https://developers.binance.com/en/docs/catalog/core-trading-derivatives-trading-options/api/ws-streams/public).
- B6 [Current options REST market data](https://developers.binance.com/en/docs/catalog/core-trading-derivatives-trading-options/api/rest-api/market-data).
- B7 [Current options market WS catalogue](https://developers.binance.com/en/docs/catalog/core-trading-derivatives-trading-options/api/ws-streams/market).
- B8 [Options general info and units of time](https://developers.binance.com/docs/derivatives/options-trading/general-info).
- Y1 [Instrument discovery](https://bybit-exchange.github.io/docs/v5/market/instrument).
- Y2 [Standard/options books](https://bybit-exchange.github.io/docs/v5/websocket/public/orderbook).
- Y3 [Full WS book](https://bybit-exchange.github.io/docs/v5/websocket/public/full-ob).
- Y4 [Full REST seed](https://bybit-exchange.github.io/docs/v5/market/full-ob).
- Y5 [Trades and flags](https://bybit-exchange.github.io/docs/v5/websocket/public/trade).
- Y6 [Ticker schemas](https://bybit-exchange.github.io/docs/v5/websocket/public/ticker).
- Y7 [RPI book](https://bybit-exchange.github.io/docs/v5/websocket/public/orderbook-rpi).
- Y8 [All liquidation](https://bybit-exchange.github.io/docs/v5/websocket/public/all-liquidation).
- Y9 [Spread book](https://bybit-exchange.github.io/docs/v5/spread/websocket/public/orderbook).
- Y10 [Spread trades](https://bybit-exchange.github.io/docs/v5/spread/websocket/public/trade).
- Y11 [Rate limits](https://bybit-exchange.github.io/docs/v5/rate-limit).

Documentation failures: old Binance options `Diff-Book-Depth-Streams` and some futures legacy trade/liquidation URLs redirected to the generic home page, so those responses were not counted as schema evidence. Bybit standard and spread connect URLs were inaccessible through the documentation reader; connection/argument/route gates remain explicit. No failure was reinterpreted as evidence that the product itself is unsupported.
