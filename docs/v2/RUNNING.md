# Run the Deribit collector

This is the first runnable v2 path: public discovery → raw metadata → selected
option books → committed archive → independent replay/export. It reuses the v2
writer, lane, clock sampler, receiver and publication pipeline. The selected scope is defined in
COLLECTION_SCOPE.md; the older frozen profiles below are historical paths. This command is a **partial pilot**, not a production service.

## Current inverse-option profile

The live path selects BTC/ETH inverse options by native base and settlement currency; USDC linear options remain visible as not_enabled in each revision and are never subscribed.

Use `config/v2-deribit-options.toml` (or `config/v2-deribit-live-options.toml`) with `rediscover_options = true`,
`all_options = true`, `raw_processes = 1` and a fresh writable Linux data root.
Choose testnet or production explicitly. Run `capture --config <your-copy.toml>`,
then `verify-run <printed-root>` with the Linux Python 3.12 venv. Healthy sockets
stay open while `instrument.state.option.any` triggers immediate complete REST
reconciliation; the periodic audit is 300 seconds. Raw discovery attempts and
universe revisions are append-only under `metadata/`. Failed/empty discovery
retains the preceding membership and records unhealthy state. Known expiry
retires options independently of a successful new REST response.

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

Bybit transport integration and the cross-venue expiry-window run remain
pending. Old archive formats remain verifiable, while new CLI futures capture
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
Binance continuous capture is not enabled yet.

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
