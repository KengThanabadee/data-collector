# Data Collector

Data Collector (DC) records public crypto market data from Deribit, Bybit and Binance that cannot be downloaded later, for options, volatility and stat-arb research.

## Language

**Data Collector**:
The single collector this repository builds; there is no earlier or parallel version of it.
_Avoid_: v1, v2, Option Collector

**Minute grid**:
The shared schedule of UTC minute boundaries on which every venue's market state is recorded together.
_Avoid_: Snapshot grid, polling

**Minute slot**:
One UTC minute boundary on the minute grid and everything recorded for it.
_Avoid_: Tick, bar

**Gap**:
A minute slot or stretch of a stream with no usable market data, recorded explicitly with its reason.
_Avoid_: Missing row, hole

**Event stream**:
A continuous venue feed of individual events, such as liquidations or option trades, recorded as it arrives rather than on the minute grid.
_Avoid_: Continuous capture, WebSocket capture
