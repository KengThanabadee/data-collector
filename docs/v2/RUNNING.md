# Run the Deribit collector

> **Scope change, 8 November 2026.** The [collection scope](COLLECTION_SCOPE.md) now records one 1-minute grid plus three event streams. The 100 ms profiles below (Deribit option book/ticker/trades, Binance bookTicker/depth20@100ms/aggTrade, Bybit option tickers WebSocket) are superseded and kept as history; do not start new captures with them. The shipped [continuous event profiles](#continuous-event-streams) use the V2 WebSocket archive. The minute-snapshot service is separate.

This is the first runnable v2 path: public discovery → raw metadata → selected
option books → committed archive → independent replay/export. It reuses the v2
writer, lane, clock sampler, receiver and publication pipeline. The selected scope is defined in
COLLECTION_SCOPE.md; the older frozen profiles below are historical paths. This command is a **partial pilot**, not a production service.

## Deribit 100 ms inverse-option profile (superseded 8 November 2026)

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

## Continuous event streams

Copy the shipped configs, choose a fresh writable Linux `data_root` for each,
and run the commands as separate collector processes. Both use `event_streams = true`.

```sh
.venv/bin/python -m data_collector capture --config config/v2-binance.toml
.venv/bin/python -m data_collector capture --config config/v2-bybit-options.toml
.venv/bin/python -m data_collector verify-run RUN_ROOT
```

The Binance config opens `/market/stream` for `!forceOrder@arr`. All native
liquidation rows stay in the raw archive, including rows outside configured
minute-grid symbols and any COIN-M rows from the shared stream. The native
`st` and `ps` fields are retained. Binance sends at most one liquidation per
symbol per second. The Bybit config opens `/v5/public/linear` for
`allLiquidation.<symbol>` and `/v5/public/option` for `publicTrade.BTC` and
`publicTrade.ETH`. Set `bybit_liquidation_symbols` to the native linear symbols
of the hedge perps. The default is `BTCUSDT` and `ETHUSDT`.

Each run records subscription requests, ACKs, exact inbound messages and
transport gaps. The duration starts after every route acknowledges its
subscriptions. `verify-run` checks the stored plan and ACKs against the raw
archive and reports the observed push topics in `topics_seen`. A quiet topic
can be subscribed successfully without appearing in that list. Reconnects
repeat the subscription and leave explicit gaps. The event profiles make no
REST requests, so minute metadata collection and the Binance REST weight
budget are handled by the minute service.

## Historical Binance discovery and 100 ms capture

Binance discovery is available with
`python -m data_collector discover --config config/v2-binance.toml`, using an
explicit writable Linux data root. It archives spot, UM and CM exchangeInfo,
requests spot `showPermissionSets=false`, and verifies the BTC/ETH projection
against those exact responses. Non-trading rows remain visible as disabled.
Spot listing incarnation is null because exchangeInfo does not supply it.
The 31 October USDⓈ-M capture profile used `binance_symbols` for a fixed
native list. A missing or non-trading configured symbol failed startup. The
historical profile subscribes bookTicker and depth20@100ms on `/public`, aggTrade and the all-symbol
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

The single-process sharded profile selected BTC/ETH option `tickers.<symbol>` and `publicTrade.BTC/ETH`, including
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

The historical Bybit profile used independent `bybit_rest_host` and
`bybit_ws_host` overrides on the acceptance network:

```toml
bybit_rest_host = "api.bytick.com"
bybit_ws_host = "stream.bytick.com"
```

Without overrides the production Bybit profile freezes api.bybit.com and
stream.bybit.com. The WebSocket path remains `/v5/public/option`, REST uses HTTPS,
and venue identity remains Bybit. The legacy multi-owner export restriction and
shared raw-owner connection/segment guards also apply to this profile.

Live Deribit workers negotiate WebSocket per-message deflate and archive the negotiated window in `transport_ready` controls. Raw bodies remain the exact decoded native WebSocket payloads.
