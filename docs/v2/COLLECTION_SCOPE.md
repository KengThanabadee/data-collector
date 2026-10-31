# Collection scope

Decided by Keng on 31 Oct 2026. This repository copy is normative for the current collection goal. Copied from the decision in `orchestrator/docs/collection-scope.md`; runtime implementation and acceptance remain pending. The repo is self-contained and does not require that external file.

## Principle

- Live-record what cannot be downloaded later.
- Also record cheap feeds when they make use simpler.
- Features use 20 book levels.
- Cost modelling uses top of book with size.

## Consumers

| Use | Venue | Data |
|---|---|---|
| Stat arb (trade) | Binance USDⓈ-M perps | features + cost |
| Options strategy (trade) | Bybit options, hedged with Bybit perps (portfolio margin) | cost |
| Options research | Deribit options, hedged with the Deribit perp | features |

Bybit perp prices and costs may be proxied by Binance perp prices plus Bybit funding and fees. On a quiet 1m sample, Bybit sat 2.5 bp (BTC) and 1.3 bp (ETH) below Binance, with a 1m return-difference sd of about 0.6 bp. Bybit's own downloadable data can replace the proxy at any time.

## Live-recorded

| Venue | Feeds | Why live |
|---|---|---|
| Deribit BTC/ETH options | `book.<id>.none.20.100ms`, `ticker.<id>.100ms`, `trades.option.<currency>.100ms` | no free book/ticker history |
| Deribit | BTC/ETH index | reference for options |
| Binance USDⓈ-M | `bookTicker`, `depth20@100ms`, `aggTrade` for a configured list (BTCUSDT, ETHUSDT, alts TBD) | bookTicker archive ended 2024-03-30; no depth-20 archive. aggTrade is kept for alignment with the book. |
| Bybit BTC/ETH options | `tickers`, `publicTrade` | no option history at all, settlement price included |

## Requirements

- **New listings.** The option universe must follow listings during a run. Subscribe new options within 120 s and retire expired ones (SPEC.md discovery rules). Today the collector freezes the startup discovery (PLANS.md Notes).
  - Deribit creates new expiries at 08:00 UTC and adds strikes intraday. Use the `instrument.state` stream: the 300 s REST audit alone cannot meet 120 s.
  - Bybit pre-lists the next expiry hours before 08:00 UTC with status `PreLaunch` and zero bid/ask. Subscribe `PreLaunch` instruments when they first appear in discovery.
- **Deribit trades.** Use the per-kind channel `trades.option.<currency>.100ms`.
- **Deribit book.** Verify that each depth-20 push is a full top-20 snapshot. FEEDS.md's book recipe assumes diffs.
- **Bybit settlement price.** It cannot be downloaded: `/v5/market/delivery-price?category=option` returns an empty list. It comes from the recorded ticker (`predictedDeliveryPrice`, `indexPrice`). Verify that `predictedDeliveryPrice` becomes non-zero before an expiry. Fallback: Bybit 1m index-price klines.
- **Bybit subscription limits.** Use one connection per base coin. Per connection: at most 2,000 option args, and at most 21,000 characters per subscribe args array. On 31 Oct, 1,378 options were listed (730 BTC, 648 ETH), all USDT-settled.
- **Dropped feeds.** Each is marked `not_enabled` with its reason, never removed silently (SPEC.md).

## Not recorded

| Data | Source |
|---|---|
| Deribit perp/futures 1m bars, trades, funding | `get_tradingview_chart_data`, history.deribit.com, funding API |
| Deribit option settlement, expired instruments | `get_delivery_prices`; history.deribit.com `get_instruments?include_old=true` |
| Binance klines, aggTrades, funding, mark/index/premium | data.binance.vision |
| Bybit perp book and trades | quote-saver.bycsi.com (ob200, 100 ms), public.bybit.com |
| Bybit klines, mark/index/premium, funding | `/v5/market/*`, 1000 rows per request, back to 2020 |

**Dropped entirely:** Deribit futures feeds; Binance spot, COIN-M, options, diff book and REST seed; Bybit spot, inverse, spreads, RPI, liquidation, and option depth books; full-depth books on all venues.

## Acceptance

One Linux run under the collector's acceptance setup passes verify-run. The run is at least one hour long, starts by 07:25 UTC (14:25 Bangkok time) and continues past 08:00 UTC. Option expiry is 08:00 UTC on both venues, and Bybit's settlement averaging runs 07:30–08:00. Record:
- Deribit: options created at 08:00 UTC subscribed during the run, with the delay from creation to subscription;
- Bybit: the `PreLaunch`→`Trading` change seen at about `launchTime`, the first ticker with bid/ask after 08:00, and any strike added during the run;
- whether Bybit `predictedDeliveryPrice` filled before the expiry;
- bytes per hour and GiB per day (native, archive, and including WAL);
- peak RSS and CPU;
- rejected or throttled subscriptions.

## Open

- The Binance alt list. Book data for an alt starts only from the day it is added.
- Bybit Thai-KYC access from a foreign IP. This concerns trading only; public data needs no account.
- Optional: Deribit DVOL (`deribit_volatility_index`). Its 1m history is downloadable only about 4–6 months back.

## Affected elsewhere

- **trading-bot:**
  - `bot.convert` assumes Deribit diff books and needs a snapshot path.
  - PLANS.md says Bybit options are USDC-settled; they are USDT-settled.
- **VPS sizing:** wait for the acceptance sizes. The earlier full-book Deribit run measured 541 GiB/day including WAL.
