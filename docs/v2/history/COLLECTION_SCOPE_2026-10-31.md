# Collection scope, 31 October 2026 (superseded)

Archived on 8 November 2026. The [8 November scope](../COLLECTION_SCOPE.md) replaces this 100 ms profile. Decided by Keng on 31 October 2026. The text below is unchanged; it is kept to look up past decisions and the measurements made under it.

## Principle

- Live-record what cannot be downloaded later.
- Also record cheap feeds when they make use simpler.
- Features come from the deepest market for each instrument type, wherever the trade happens: Deribit for options, Binance for perps. They use 20 book levels.
- Cost modelling uses the trading venue's own top of book with size: Binance perps, Bybit options and Bybit perps.

## Consumers

| Use | Venue | Data |
|---|---|---|
| Stat arb (trade) | Binance USDⓈ-M perps | features + cost |
| Options strategy (trade) | Bybit options, hedged with Bybit perps (portfolio margin) | cost |
| Options research | Deribit options, hedged with the Deribit perp | features; perp book features come from the Binance perp book |

Bybit perp prices and costs may be proxied by Binance perp prices plus Bybit funding and fees. On a quiet 1m sample, Bybit sat 2.5 bp (BTC) and 1.3 bp (ETH) below Binance, with a 1m return-difference sd of about 0.6 bp. Bybit's own downloadable data can replace the proxy at any time.

## Live-recorded

| Venue | Feeds | Why live |
|---|---|---|
| Deribit BTC/ETH inverse options (USDC linear options excluded) | `book.<id>.none.20.100ms`, `ticker.<id>.100ms`, `trades.option.<currency>.100ms` | no free book/ticker history |
| Deribit | BTC/ETH index | reference for options |
| Binance USDⓈ-M | `bookTicker` and `depth20@100ms` on the `/public` route; `aggTrade` on the `/market` route. Symbols come from a configured list (BTCUSDT, ETHUSDT, alts TBD). Liquidations: `!forceOrder@arr` (all symbols) on the `/market` route; on 31 Oct the `/public` route ACKed it and delivered nothing. | bookTicker archive ended 2024-03-30; no depth-20 archive; no liquidation archive. aggTrade is kept for alignment with the book. Liquidations are a stat-arb feature; Binance sends at most one per symbol per second. |
| Bybit BTC/ETH options | `tickers.<symbol>`, `publicTrade.<baseCoin>` | no option book, ticker or trade history |
| All three venues | discovery and instrument metadata (Deribit instruments, Binance `exchangeInfo`, Bybit `instruments-info`), append-only | tick/lot, strikes and expiries are current-only on the venues |

## Requirements

- **New listings.** The option universe must follow listings during a run. Subscribe new options within 120 s and retire expired ones (../../../SPEC.md discovery rules). Before this change, the collector froze the startup discovery.
  - Deribit creates new expiries at 08:00 UTC and adds strikes intraday. Use the `instrument.state` stream: the 300 s REST audit alone cannot meet 120 s.
  - Bybit pre-lists the next expiry hours before 08:00 UTC with status `PreLaunch` and zero bid/ask. Subscribe `PreLaunch` instruments when they first appear in discovery.
- **Timing evidence.** For every instrument, archive:
  - when it was observed (Deribit `creation_timestamp`/`instrument.state` receipt, or the discovery poll time);
  - the subscribe request time and the subscribe-ACK receipt time;
  - the retirement time.
  The 120 s requirement is measured from these records.
- **Coverage with a changing universe.** verify-run checks each universe version. An option added mid-run must have an ACK and a first push. A retired option is exempt after its retirement time.
- **Deribit book.** Verify that each depth-20 push on an option is a full top-20 snapshot. These pushes have no `type` field (keys: `asks`, `bids`, `change_id`, `instrument_name`, `timestamp`). A book counts as initialized at its first push after the subscribe ACK. The old verifier's `type == 'snapshot'` check does not apply.
- **Bybit subscription limits.** Bybit's docs cap the `args` array at 21,000 characters per public connection, and at 2,000 option args per connection. BTC tickers alone come to about 26k characters as JSON, so a single coin does not fit on one connection.
  - Shard topics across connections so each connection stays under the cap, with headroom for new listings.
  - Test once, live, whether the cap is enforced per connection or per request, and record the result. Shard per connection unless the test shows otherwise.
  - On 31 Oct, 1,378 options were listed (730 BTC, 648 ETH), all USDT-settled.
- **Dropped feeds.** Each is marked `not_enabled` with its reason, never removed silently (../../../SPEC.md).

## Not recorded

| Data | Source |
|---|---|
| Deribit perp/futures 1m bars, trades, funding | `get_tradingview_chart_data`, history.deribit.com, `get_funding_rate_history` |
| Deribit option settlement, expired instruments | `get_delivery_prices`; history.deribit.com `get_instruments?expired=true` (verified 31 Oct 2026: 124,759 expired BTC options; `include_old=true` returns only the 985 active ones) |
| Binance klines, aggTrades, funding, mark/index/premium | data.binance.vision (from 2020-01) |
| Bybit perp book and trades | quote-saver.bycsi.com (ob200, 100 ms), public.bybit.com |
| Bybit klines, mark/index/premium | `/v5/market/*kline`, 1000 rows per request, back to 2020 |
| Bybit funding | `/v5/market/funding/history`, 200 rows per request |
| Bybit option settlement prices | `/v5/market/new-delivery-price?category=option&baseCoin=<coin>&settleCoin=USDT` (daily history; the old `/delivery-price` returns an empty list) |

**Dropped entirely:** Deribit futures feeds; Deribit USDC linear options (`BTC_USDC`, `ETH_USDC`), since nearly all liquidity is in the inverse options; Binance spot, COIN-M, options, diff book and REST seed; Bybit spot, inverse, spreads, RPI and liquidation (Binance liquidations are the feature); Bybit option depth books for now (see Open); full-depth books on all venues.

## Acceptance

One Linux run under the collector's acceptance setup. Both venues list and expire options daily at 08:00 UTC, weekends included, so any day works. The run must:
- be at least one hour long;
- start by 07:25 UTC (14:25 Bangkok time);
- continue past 08:00 UTC.

**The run fails if any of these is true:**
- verify-run fails, including coverage of every universe version;
- a Deribit option created during the run was subscribed more than 120 s after its creation, or was never subscribed;
- a Bybit `PreLaunch` option for the next expiry was not subscribed before 08:00 UTC;
- an option that expired at 08:00 UTC was not retired;
- any subscription was rejected.

**Record only (these do not fail the run):**
- Bybit: the `PreLaunch`→`Trading` change, seen within one discovery interval of `launchTime`, and the first ticker with bid/ask after 08:00;
- strikes added during the run on either venue (none is acceptable);
- the live test of Bybit's character cap: per connection or per request;
- bytes per hour and GiB per day (native, archive, and including WAL);
- peak RSS and CPU;
- throttled subscriptions.

Bybit options are pre-listed before 07:25, so this run does not exercise Bybit's mid-run listing path. That path is covered only by an intraday strike addition, if one occurs.

## Open

- The Binance alt list. Book data for an alt starts only from the day it is added.
- Bybit Thai-KYC access from a foreign IP. This concerns trading only; public data needs no account.
- Bybit option depth books (`orderbook.25`, 20 ms; no smaller depth exists). Revisit once order sizes show whether fills go past the top level that `tickers` carries. They have no archive, so data before that decision is not recoverable.
- Optional: Deribit DVOL (`deribit_volatility_index`). Its 1m history is downloadable only about 4–6 months back.

## Affected elsewhere

- **trading-bot:**
  - `bot.convert` assumes Deribit diff books and needs a snapshot path.
  - `bot.convert` builds instruments only from the startup `universe.json`. Options listed mid-run need the later metadata versions.
  - Update PLANS.md: Bybit options are USDT-settled.
- **VPS sizing:** wait for the acceptance sizes. The earlier full-book Deribit run measured 541 GiB/day including WAL.
