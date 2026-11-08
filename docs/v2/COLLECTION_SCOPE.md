# Collection scope

Decided by Keng on 8 November 2026. This file is the contract for the current collection goal. It replaces the 31 October 100 ms scope, which is kept unchanged in [history/COLLECTION_SCOPE_2026-10-31.md](history/COLLECTION_SCOPE_2026-10-31.md). The minute-snapshot service that implements it is not built yet; see [PLANS.md](../../PLANS.md).

## Principle

- Record live everything that has no free history, on one 1-minute grid shared by Deribit, Bybit and Binance. Anything a venue lets us download later is left out.
- Keep raw bytes and explicit gaps. A minute that cannot be recorded is written as a gap and never backfilled.
- Research covers options and volatility on Deribit BTC/ETH and stat arb on Binance USDⓈ-M perps; features may cross between them. Trading uses Bybit options (portfolio margin) hedged with Bybit perps, and Binance perp stat arb, at a 5–15 minute horizon (maybe 1 minute later).
- Trading cost comes from Bybit and Binance; research cost comes from Binance and Deribit.
- Order-book microstructure (imbalance and similar signals) is out of scope, so no 100 ms stream is recorded.

## Consumers

| Consumer | Uses |
|---|---|
| trading-bot | features and cost data from the minute grid and event streams |
| crypto-options-analytics | the recorded history, together with the downloadable history listed under "Not recorded" |

## Recorded every minute

All requests fire at :00 UTC. Each response keeps its raw bytes, its send and receive times, and the venue server time.

| Venue | Data | Source |
|---|---|---|
| Deribit | Ticker for every BTC/ETH inverse option and dated future | WS sampler (below) |
| Deribit | Futures book summary, BTC and ETH | REST |
| Deribit | Indexes `btc_usd`, `eth_usd`, `usdt_usd`, `btc_usdt`, `eth_usdt` | REST |
| Deribit | DVOL, BTC and ETH | REST |
| Deribit | `BTC-PERPETUAL` and `ETH-PERPETUAL` ticker | REST |
| Bybit | Option tickers per base coin (BTC, ETH) | REST |
| Bybit | Order book, limit 25, for about 100 near-ATM short-dated options | REST |
| Bybit | BTC and ETH perp ticker | REST |
| Binance USDⓈ-M | Depth (limit 20) and open interest for about 100 symbols | REST |

"BTC/ETH option" means the Deribit instruments listed under currency BTC or ETH (the inverse options). Deribit USDC linear options stay not_enabled, as in the 31 October scope.

About 313 REST requests per minute: Binance about 200, Bybit about 103, Deribit about 10.

**Deribit WS sampler.** Subscribe to `ticker.<name>.agg2` for every BTC/ETH inverse option and dated future. At each minute the sampler writes the last raw value received for every instrument, with its venue timestamp, receive time and connection ID. A value received before the current connection's subscribe ACK is marked stale. An instrument with no value since it was subscribed is written as missing. The sampler receives about 1 MB/s.

## Recorded continuously (event streams)

These stay on the V2 WebSocket capture path.

| Venue | Stream | Note |
|---|---|---|
| Binance USDⓈ-M | `!forceOrder@arr` on the `/market` route | all symbols; Binance sends at most one liquidation per symbol per second. On 31 October the `/public` route ACKed it and delivered nothing. |
| Bybit linear | `allLiquidation.<symbol>` for the hedge perps | configured list, initially BTCUSDT and ETHUSDT |
| Bybit option | `publicTrade.<baseCoin>` for BTC and ETH | no free option trade history |

## Recorded on change or daily

- Instrument metadata for all venues (Deribit instruments, Binance `exchangeInfo`, Bybit `instruments-info`), stored when it changes. The Deribit sampler subscribes new listings from it and retires expired options at 08:00 UTC.
- Bybit risk-limit tiers, stored when they change.
- Bybit option settlement from `/v5/market/new-delivery-price`, once a day. The endpoint keeps only the latest 200 days.

## Not recorded (downloadable)

| Data | Source |
|---|---|
| Deribit option, perp, block and combo trades | history.deribit.com |
| Deribit funding, 1-minute bars, delivery prices | `get_funding_rate_history`, `get_tradingview_chart_data`, `get_delivery_prices` |
| Deribit expired instruments | history.deribit.com `get_instruments?expired=true` (verified 31 October 2026: 124,759 expired BTC options) |
| Deribit DVOL, long history | `get_volatility_index_data` (the 1-minute series reaches only about 4–6 months back, which is why DVOL is also recorded each minute) |
| Binance klines, aggTrades, funding, mark/index/premium, 5-minute metrics, bookDepth | data.binance.vision |
| Bybit perp L2 | quote-saver.bycsi.com (ob200) |
| Bybit perp trades | public.bybit.com |
| Bybit klines | `/v5/market/*kline`, 1,000 rows per request, back to 2020 |
| Bybit funding | `/v5/market/funding/history`, 200 rows per request |

## Dropped

Every 100 ms book and ticker stream is dropped. Each stays visible in [FEEDS.md](FEEDS.md) as `not_enabled` with its reason:

| Feed (31 October scope) | Status and reason |
|---|---|
| Deribit `book.<id>.none.20.100ms` for options | not_enabled: order-book microstructure is out of scope; the minute ticker carries best bid/ask with size |
| Deribit `ticker.<id>.100ms` for options | not_enabled: replaced by the minute sampler on `ticker.<name>.agg2` |
| Deribit `trades.option.<currency>.100ms` | not_enabled: option trades are downloadable from history.deribit.com |
| Deribit `deribit_price_index.btc_usd` / `eth_usd` stream | not_enabled: replaced by the minute REST index reads |
| Binance `bookTicker` (`/public`) | not_enabled: 100 ms stream; the minute depth-20 read carries top of book |
| Binance `depth20@100ms` (`/public`) | not_enabled: 100 ms stream; replaced by minute REST depth, limit 20 |
| Binance `aggTrade` (`/market`) | not_enabled: downloadable from data.binance.vision |
| Bybit option `tickers.<symbol>` WebSocket | not_enabled: 100 ms stream; replaced by minute REST tickers per base coin |
| Bybit option depth beyond the about 100 selected options | not_enabled: cost data is needed only near ATM and short-dated |

Other families dropped earlier (Deribit spot and combos, Binance spot, COIN-M and options, Bybit spot, inverse, spreads and RPI, full-depth books on all venues) stay not_enabled with the reasons in FEEDS.md.

## Requirements

- **One grid.** Every venue uses one 1-minute grid fired at :00 UTC. A minute that cannot be recorded is written as a gap and never backfilled.
- **Raw and timed.** Each response keeps its raw bytes, send time, receive time and venue server time.
- **Minute index.** Each minute writes one index line listing every expected request as `ok`, `error`, `timeout` or `missed`.
- **Listings.** The Deribit sampler subscribes new listings found in the metadata and retires expired options at 08:00 UTC.
- **Stale marking.** A sampler value received before the current connection's subscribe ACK is marked stale.
- **Binance weight guard.** Binance REST is guarded on the `x-mbx-used-weight-1m` response header. The weight budget is per IP and shared with the trading bot (about 300 of 2,400 per minute). A request skipped by the guard is recorded as missed.
- **Storage.** Append each minute to an hourly journal. Seal each day as one zstd file with a manifest. Delete the journal and WAL only after the sealed file reads back and matches the manifest. Keep the last 2 days of journals until the offsite copy is confirmed.

## Sizes

About 0.55 GB/day with gzip per file, and about 0.2 GB/day projected after daily zstd. Neither is yet measured at 60-second spacing.

## Acceptance

One run of at least an hour that spans 08:00 UTC. Until a VPS is chosen, run it in the Lima VM `dc-acceptance`.

**The run fails if any of these is true:**
- a minute in the run is neither recorded nor written as a gap;
- a minute index line does not list every expected request;
- an option listed during the run is not subscribed by the sampler;
- an option that expired at 08:00 UTC is not retired.

**Record (these do not fail the run):**
- bytes per day;
- peak RSS and CPU;
- Binance weight used per minute.

## Open

- VPS, decided last: Ubuntu 24.04, chrony, a reproducible install so the host can change within an hour, an external dead-man's switch to Keng's phone, and a nightly offsite backup with a monthly restore test. Candidates: Oracle Free (unconfirmed), Contabo, Vultr Tokyo.
- Trading-bot host: a separate paid VPS with a reserved IP and a watchdog that flattens positions, decided later.
- Bybit API trading from a foreign server with a Thai-KYC account (Keng expects it works; unverified), and the Thai tax and legal view.
- Bybit depth selection for production. Ship configurable defaults of 50 BTC and 50 ETH options from the two nearest expiries, using the strikes nearest the corresponding index. Re-select hourly and record each selection.
- Deribit new-listing deadline. By default, check metadata every minute and subscribe newly discovered instruments before the next minute.
- Binance symbol universe for production. Default to a fixed config list of the top 100 USDT perps by 24-hour volume at build time.

## Affected elsewhere

- **trading-bot:** `bot.convert` reads V2 exports of the 100 ms feeds. It needs a reader for the minute snapshots; trading-bot builds that itself.
- **crypto-options-analytics:** reads the recorded history together with the downloadable sources above.
- **orchestrator:** its contracts index gets the 8 November date.
