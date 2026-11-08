# Run the Deribit collector

> **Scope change, 8 November 2026.** The [collection scope](COLLECTION_SCOPE.md) now records one 1-minute grid plus three event streams. The 100 ms profiles below (Deribit option book/ticker/trades, Binance bookTicker/depth20@100ms/aggTrade, Bybit option tickers WebSocket) are superseded and kept as history; do not start new captures with them. The runnable V2 event paths that stay in scope are the Binance all-symbol `!forceOrder@arr` feed and Bybit option `publicTrade`, which still run inside the shipped Binance and Bybit configs; Bybit `allLiquidation` is not implemented yet.

## Deribit and Bybit minute grid

Use a fresh data root dedicated to minute capture. A bounded run counts scheduled
slots, including explicit gaps. Omit `--minutes` to keep the grid running.

```sh
.venv/bin/python -m data_collector minute-capture --data-root data/minute-grid --minutes 1
.venv/bin/python -m data_collector minute-read --data-root data/minute-grid --slot 2026-11-08T07:21:00Z
```

For readback, use the UTC minute of your run. The grid dispatches the profile at
:00 UTC, with the v1 REST transport pacing individual sends. The first run starts
at the next boundary. A wakeup one second or more after a boundary records a
gap. On restart the same root resumes its grid and indexes any elapsed slots
without reissuing old requests. `--request-window` sets the request deadline in
seconds after the boundary and defaults to 20.

Each slot has one `index.jsonl` line under `minutes/YYYY/MM/DD/HHMM/`. The
index lists the expected requests and distinguishes `ok`, `error`, `timeout`
and `missed`. A slot without a REST attempt or ticker snapshot is a gap.
Readback verifies the index against the v1 response archive, which retains raw
bytes and local send and receive times. Deribit `usOut` is stored as
`venue_server_epoch_us` and Bybit `time` as `venue_server_epoch_ms` when
available. Native market timestamps remain in the raw body.

The profile uses `public/get_index_price` for the live BTC and ETH DVOL indexes,
`btcdvol_usdc` and `ethdvol_usdc`.

The CLI enables the Deribit `ticker.<name>.agg2` sampler by default. Add
`--rest-only` to run just the REST profile. At each cutoff, `minute-read`
returns the last ticker per BTC/ETH inverse option or dated future in
`deribit_tickers`. Each entry contains its native frame, venue and local
receipt timestamps, and connection ID. The snapshot also carries the current
connection and each instrument's subscription ACK time when available.
A frame from an earlier connection or one received before its ACK is
`stale`. An instrument without a frame is `missing`.

Discovery runs at startup and once per captured minute. The snapshot names
the most recent valid metadata version and reports the last discovery
outcome. Changed replies are kept as native response envelopes under
`deribit-metadata/`. The sampler reconciles new listings on the current
connection and retires listings at their native expiry, including options
expiring at 08:00 UTC.

The Bybit profile requests BTC and ETH option ticker lists, BTCUSDT and ETHUSDT
perp tickers, and option depth at 25 levels. Use `--deribit-only` to skip
Bybit. Depth defaults to 50 options per base from the two nearest active
expiries. Adjust the defaults with `--bybit-depth-btc`,
`--bybit-depth-eth` and `--bybit-expiries`.

The first successful selection in each UTC hour uses the median available
option ticker index for that base. It ranks strikes by distance to that index
and breaks ties by expiry then symbol. When a selection is due, its ticker and
metadata must arrive before depth requests can start. The minute index lists
the selected symbols for each base with a selection. The hourly evidence under
`bybit/selection/` records the index, expiries, selected symbols, and source
checksums.

Hourly reference checks preserve changed option and hedge-perp instruments
and risk tiers under `bybit/reference/`. Failed checks keep their outcomes and
available native bytes in `bybit/reference-failures/`. The first successful daily settlement
read at or after 08:02 UTC keeps each BTC and ETH USDT and USDC reply as returned.
`read_bybit_selection(root, slot, base)` and
`read_bybit_reference(root, name, slot)` in `data_collector.minute_bybit`
read the stored evidence by UTC Unix slot. Pass `failed=True` to read a failed
reference check.

## Deribit 100 ms inverse-option profile (superseded 8 November 2026)

This was the first runnable v2 path: public discovery → raw metadata → selected
option books → committed archive → independent replay/export. It reuses the v2
writer, lane, clock sampler, receiver and publication pipeline. The selected scope is defined in
COLLECTION_SCOPE.md; the older frozen profiles below are historical paths. This command is a **partial pilot**, not a production service.

The live path selects BTC/ETH inverse options by native base and settlement currency; USDC linear options remain visible as not_enabled in each revision and are never subscribed.

Use `config/v2-deribit-options.toml` (or `config/v2-deribit-live-options.toml`) with `rediscover_options = true`,
`all_options = true`, `raw_processes = 2` and a fresh writable Linux data root.
Choose testnet or production explicitly. Run `capture --config <your-copy.toml>`,
then `verify-run <printed-root>` with the Linux Python 3.12 venv. Healthy sockets
stay open while `instrument.state.option.any` triggers immediate complete REST
reconciliation; the periodic audit is 300 seconds. Raw discovery attempts and
universe revisions are append-only under each worker's `metadata/`. Failed/empty discovery
retains the preceding membership and records unhealthy state. Known expiry
retires options independently of a successful new REST response.

Workers use stable symbol partitions and independently acquire complete metadata
throughout the run. The supervisor starts the duration after all workers initialize,
stops and drains them together, and `verify-run` checks every partition/version.
A shared file paces subscriptions and metadata requests across workers. Lifecycle,
per-kind trades and indexes are independent physical raw copies in each worker;
book/ticker symbols are disjoint. Resource and byte totals include those copies.
`raw_processes` can be set from one to eight; internal `option_partition_count`
and `option_partition_index` identify child scope. A child archive verifies its
own partition; use the supervisor root to verify full coverage.

Each instrument has archived observation, subscribe request, ACK receipt and
retirement timing. Verification binds requests to native channel ACKs and the
current incarnation, checks every universe revision and requires a first native
push for added options. Recorded listing observations still unresolved at stop
cannot produce successful coverage. Deribit creation-to-ACK over 120 seconds
fails verification for options created during the run; both request and ACK
latencies are retained, with ACK as the conservative subscription-time bound.

The profile uses `book.<id>.none.20.100ms` and `ticker.<id>.100ms` for
current inverse options, plus `trades.option.BTC.100ms`,
`trades.option.ETH.100ms`, `deribit_price_index.btc_usd` and
`deribit_price_index.eth_usd`. Each book push is a complete top-20 replacement
with price/amount pairs; it has no type field. The first book and ticker after
that instrument's ACK initialize it. A consolidated trade channel requires its
ACK, not an invented initial trade. Both index values are required after their
ACK. Reconnect opens an explicit coverage gap and repeats initialization;
stopping in a later gap does not erase earlier proven coverage of that same
incarnation. New options still require their own ACK and first pushes.

Bybit transport is described below. The cross-venue expiry-window run of the
31 October scope will not be run; the 8 November acceptance replaces it. Old archive formats remain verifiable, while new CLI futures capture
is not_enabled because its history is downloadable. No multi-owner raw export
is enabled for this path. A verified failed prefix is integrity evidence, not a
passed capture.

## Requirements

Use Python 3.12 and the pinned requirements on Linux with a configured local
chrony daemon and `chronyc` on PATH. The command only reads chrony; it does not
install software or change the system clock. Allow about a minute for measured
clock alignment. `config-check` and archive verification also work on macOS.
Keep at least 5 GiB and 10% free disk space. No API key is required.

## Start and inspect

Copy `config/v2-deribit-options.toml`, explicitly choose production or testnet,
and set a fresh writable Linux `data_root`. Relative paths resolve against the
configuration file. In Lima use its Python 3.12 venv and a data root inside the
VM; the macOS repository mount and `.venv` are not writable Linux runtime roots.
The command rejects a v1 root, environment mismatch, unbound nonempty directory
and a second owner of the same root.

```sh
python -m data_collector config-check --config YOUR_OPTIONS_CONFIG
python -m data_collector discover --config YOUR_OPTIONS_CONFIG
python -m data_collector capture --config YOUR_OPTIONS_CONFIG
```

Discovery retains complete native metadata, including explicitly excluded
products. Capture selects inverse options from native base and settlement
currency. It uses at most 500 channels per request and one subscription request
per second. The duration starts when all current options and both indexes are
initialized. Set `duration_seconds = 3600` for an hour after initialization;
zero duration/frame/byte limits mean unbounded. Positive frame/byte limits stop
after a bounded receive burst, so they can include its admitted tail. Ctrl-C
stops ingress, drains admitted writes and in-flight metadata HTTP, then prints
the archive root. Listing observations unresolved at stop fail coverage.

Receive bursts are bounded by 512 frames, 1 MiB of native bytes plus at most one
message, or 20 ms. FIFO group commit retains queue charges and clock barriers
until fsync. Raw segments target a configurable 8 MiB, with the 5-second
receive-to-durable deadline unchanged. Writer failures retain their durable
prefix and fail the run. Rejections and throttling are archived before failure.

Healthy sockets stay open. Real connection loss opens a recorded gap, closes
the socket and reconnects with exponential 1/2/4…60-second backoff and jitter.
Close codes/reasons are retained. A recovered connection repeats ACK, book,
ticker and index initialization. A previously covered incarnation remains
covered before its later gap; a new incarnation never inherits that coverage.
The recorded gaps remain gaps, without a native sequence-continuity claim.

## Read back

Keep the entire printed root, including its `metadata/` owner, source/config
bundles, WAL and immutable segments. Run:

```sh
python -m data_collector verify-run RUN_ROOT
```

Verification rereads native bodies, checks source/config hashes and receipt
ordering, rebuilds every universe version from raw discovery, and binds timing
evidence to raw requests and ACKs. It validates full top-20 pair shape on every
book push and requires post-ACK book/ticker/index initialization. This is raw
coverage with explicit gaps, not a derived book replay. A failed prefix may pass
integrity verification while reporting `changing_universe_coverage_verified`
as false; that does not pass capture acceptance.

### Export for other repositories

`python -m data_collector verify-run RUN_ROOT --export FILE` is the interface
other repositories use to read a V2 capture (trading-bot's `bot.convert` calls it).
The minute snapshots will need their own reader, which trading-bot builds.
It verifies the run as above and writes the raw envelopes losslessly as JSONL to
`FILE`, which must not already exist. It prints the result JSON on stdout, which
includes `archive_verified`, and exits nonzero when verification fails.

Historical single-owner/frozen-shard/snapshot archives remain readable. Existing
exports remain exact raw envelopes; multi-owner live-option export is disabled.
The old frozen raw capture and futures CLI paths are not_enabled for new runs.
The bounded historical snapshot path still uses `capture_mode = "snapshots"`,
at most 120 seconds and 64 frames, with explicit symbols and its original
snapshot/replay semantics. It is separate from the selected live raw profile.

## Current boundary

Binance discovery is available with
`python -m data_collector discover --config config/v2-binance.toml`, using an
explicit writable Linux data root. It archives spot, UM and CM exchangeInfo,
requests spot `showPermissionSets=false`, and verifies the BTC/ETH projection
against those exact responses. Non-trading rows remain visible as disabled.
Spot listing incarnation is null because exchangeInfo does not supply it.
Binance continuous capture uses the 31 October USDⓈ-M profile. Under the
8 November scope only its `!forceOrder@arr` feed stays selected; bookTicker,
depth20@100ms and aggTrade are not_enabled and move to the minute REST depth read:

```sh
.venv/bin/python -m data_collector capture --config config/v2-binance.toml
.venv/bin/python -m data_collector verify-run /absolute/data/root/runs/<run-id>
```

Set `binance_symbols = ["BTCUSDT", "ETHUSDT"]` to the complete desired native list;
a missing or non-trading configured symbol fails startup. The profile always
subscribes bookTicker and depth20@100ms on `/public`, aggTrade and the all-symbol
`!forceOrder@arr` feed on `/market`. The feed plan is archived explicitly;
`raw_feeds`, `raw_processes` and the Deribit selection settings do not choose
Binance feeds or workers. No spot/COIN-M subscription, REST seed or reference
polling is used. Full UM exchangeInfo replies are appended every 60 seconds;
changes to configured listing identities stop capture so a fresh plan is required.
Filters and other metadata changes remain recorded without stopping.

Each connection archives its subscribe request, ACK and native pushes, plus
transport gap start/end controls. Public initialization requires first bookTicker
and depth20 pushes after ACK for every configured symbol. aggTrade and
liquidations require ACK; quiet feeds do not require an invented event. The
verifier reports whether a liquidation push was observed. All liquidation rows
are preserved even for unconfigured symbols or native CM rows after Binance's
UM/CM migration; raw envelopes retain multiplex identity, and native `st`/`ps`
remain untouched. This all-market stream is sampled by Binance at most once per
symbol per second and is not an exhaustive liquidation tape.

Failed runs retain raw prefixes. The shared raw owner still has a 128-connection
identity bound; reaching it stops with retained evidence rather than growing
memory indefinitely. Healthy sockets have no collector rollover. Server close,
serverShutdown and connection loss trigger paced reconnect and fresh ACK/book
initialization. No REST backfill repairs the earlier transport gap.

Failed/cancelled runs retain their durable raw prefix; inspect their error before
using data. Each command starts a new capture identity, so separate runs do not
claim uninterrupted coverage. Crashed sessions remain on disk for the existing
recovery APIs; automatic CLI restart reconciliation is pending. The initial source
bundle freezes collector Python only; full vendor-schema release qualification is
pending. Other feeds/venues, long-running capacity, resource enforcement,
independent backup/restore and intended-host acceptance remain required by SPEC.

Continuous clock sampling and its durable log have no sample/assessment cap;
they run until stopped and retain the latest sample/acknowledgement and counters.
Clock epoch history remains bounded by the existing clock policy. Writer sessions
still allow 10,000 segments; configured unbounded limits do not remove that guard.

Observed acceptance runs and their evidence locations are recorded in
[PLANS.md](../../PLANS.md).

## Bybit option raw capture (31 October profile)

Use the single-process sharded profile with a writable Linux data root and the
Linux Python 3.12 environment:

```sh
.venv/bin/python -m data_collector config-check --config config/v2-bybit-options.toml
.venv/bin/python -m data_collector capture --config config/v2-bybit-options.toml
.venv/bin/python -m data_collector verify-run /absolute/data/root/runs/<run-id>
```

It selects BTC/ETH option `tickers.<symbol>` and `publicTrade.BTC/ETH`, including
PreLaunch instruments from a complete unfiltered paginated discovery walk every
60 seconds. Each full HTTP response and poll result is archived append-only in
`metadata/`; changed metadata and expiry produce referenced universe versions.
Expired instruments are retired and unsubscribed on their current connection.
The effective config must explicitly select `raw_feeds = ["ticker", "trades"]`;
other selections are rejected.

Default shard targets are 14,000 args-array characters and 1,000 topics, below
the documented 21,000-character / 2,000-arg per-connection caps. Set
`shard_arg_chars`, `shard_max_args` and `discovery_interval_seconds` to change
these targets within their validation bounds. New listings use available shard
headroom or a new shard, with at least five seconds between connection attempts.
One command is in flight per connection because the observed option ACK has no
request id. Requests and exact COMMAND_RESP topics bind ACK evidence to their
connection and listing incarnation. A ticker may push before its ACK; it counts
once requested, while the ACK remains independently required. publicTrade needs
its ACK without a fabricated initial trade.

`verify-run` re-derives every metadata poll and universe version from raw bytes.
Every option incarnation needs an ACK and a first ticker push by retirement or
stop; there is no last-120-second exemption. A first subscription ACK more than
120 seconds after observation fails, using the ACK as a conservative bound and
retaining the request time separately. Reconnects of already covered options
retain explicit gaps and do not repeat that listing deadline. Stopping during a
later gap preserves earlier coverage; it does not prove that the gap recovered.
Raw prefixes from failed runs remain verifiable for integrity.

The independent [cap probe readback](bybit-cap-evidence.json) found no rejection
in its tested 22,887-character cases; sharding still follows the documented caps.
[Official source hashes](bybit-source-evidence.json), recorded-reply fixtures and
the [corrected historical experiment](history/BYBIT_EXPERIMENT_A.md) retain the
evidence and its limits. Under the 8 November scope only `publicTrade.BTC/ETH` stays selected; the
`tickers.<symbol>` WebSocket is not_enabled and moves to the minute REST read.
The home connection could not keep up with the full ticker feed in the experiment.

Public origins are configured independently with `bybit_rest_host` and
`bybit_ws_host`; values are bare DNS hostnames. The selected production profile
uses these overrides on the acceptance network:

```toml
bybit_rest_host = "api.bytick.com"
bybit_ws_host = "stream.bytick.com"
```

Without overrides the production Bybit profile freezes api.bybit.com and
stream.bybit.com. The WebSocket path remains `/v5/public/option`, REST uses HTTPS,
and venue identity remains Bybit. The legacy multi-owner export restriction and
shared raw-owner connection/segment guards also apply to this profile.

Live Deribit workers negotiate WebSocket per-message deflate and archive the negotiated window in `transport_ready` controls. Raw bodies remain the exact decoded native WebSocket payloads.
