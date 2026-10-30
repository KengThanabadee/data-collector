# Run the Deribit collector

This is the first runnable v2 path: public discovery → raw metadata → selected
option books → committed archive → independent replay/export. It reuses the v2
writer, lane, clock sampler, receiver and publication pipeline. The full scope in
SPEC.md is unchanged. This command is a **partial pilot**, not a production service.

## Requirements

Use Python 3.12 and the pinned requirements on Linux with a configured local
chrony daemon and `chronyc` on PATH. The command only reads chrony; it does not
install software or change the system clock. Allow about a minute for measured
clock alignment. `config-check` and archive verification also work on macOS.
Keep at least 5 GiB and 10% free disk space. No API key is required.

## Start and inspect

Copy `config/v2-discovery.toml` and select `testnet` or `production` explicitly.
Relative `data_root` paths resolve against the config file. Use a new v2 root;
the command rejects a v1 root, a different bound environment, an unbound nonempty
directory, and a second command owning that same root.

```sh
.venv/bin/python -m data_collector config-check --config config/v2-discovery.toml
.venv/bin/python -m data_collector discover --config config/v2-discovery.toml
```

Discovery prints `available_symbols` and an archive `root`. Set `symbols` in the
TOML to current active option names from that list, or set `all_options = true`
with `symbols = []` to freeze every listed BTC/ETH option in discovery, then run:

```sh
.venv/bin/python -m data_collector capture --config config/v2-discovery.toml
```

For the full options universe, `config/v2-deribit-options.toml` provides the
all-options settings; select the environment and a writable data root explicitly.

For perpetuals and dated futures with their index dependencies, use
`config/v2-deribit-futures.toml`. `all_futures = true` selects every native future
in the frozen BTC/ETH discovery, including coin and linear currency variants,
and each distinct metadata `price_index`. It is exclusive with `all_options`
and explicit symbols. One socket captures book/ticker/trades at 100ms plus
`deribit_price_index.<index_name>` updates at their native cadence. ACKs,
native book snapshots, initial tickers and initial index values are required
before the duration starts. Shared future/index raw envelopes use product
`multiplex`; native topic/data and the pinned discovery retain their identities.
Verification checks the complete future/index plan and its initialization.

Capture defaults to raw mode (`capture_mode = "raw"`) on explicit
`book.<symbol>.100ms` channels. With `all_options = true`, raw capture also
subscribes `ticker.<symbol>.100ms` and `trades.<symbol>.100ms` for every listed
option, including returned inactive states. The universe is frozen at startup. All-options mode defaults to
`raw_processes = 4` (at most 8), partitioning that one discovery into disjoint
worker symbol sets. Workers reuse the verified frozen discovery without extra
REST calls, and share one subscription-per-second reservation across processes.
Each worker owns one socket, measured clock sampler and durable raw lane. The
supervisor starts the duration only when every worker is initialized, propagates
Ctrl-C, and drains workers before reporting completion. A worker also stops if its
supervisor PID disappears, preventing orphan captures after a controller crash.
Subscriptions use batches of at most `subscription_batch_size` (default 500)
channels paced at one request per second. All batches must be acknowledged,
every book must receive its native snapshot and every ticker an initial value
within `bootstrap_timeout_seconds` (default 180). Quiet trade streams need an
ACK, not an artificial initial trade. Raw receive bursts are bounded at 512
frames, 1 MiB of native payload (plus one message), or 50 ms. FIFO raw write
groups keep queue charges until fsync, stop at clock/control barriers and sync
before segment sealing; acknowledgements never precede durability. Raw WAL
segments target `raw_segment_bytes` (default 8 MiB, configurable from 1 to 64 MiB)
to shorten synchronous sealing pauses; the 5-second receive-to-durable deadline
is unchanged. Native close codes/reasons are retained as socket-event controls. The all-options duration starts once the
whole plan is initialized. Rejections and throttle messages are retained verbatim
and counted, then fail the run; they are never silently skipped. See
[Deribit usage policy](https://docs.deribit.com/articles/api-usage-policy) and
[subscription rate limits](https://docs.deribit.com/articles/rate-limits). `duration_seconds`, `max_frames`, and `max_bytes`
default to 0, meaning unbounded. Positive values stop at the first limit in single-process mode; positive frame/byte
budgets require `raw_processes = 1` rather than being silently ignored. Multi-worker
mode supports the global duration and explicit stop. Byte
limits count native payload bytes and may overshoot by one message. For a one-hour
run set `duration_seconds = 3600`, leaving the other limits at 0. Ctrl-C requests
a stop, closes the socket, drains admitted disk work, and prints the run root.

Raw mode reconnects after transport loss with a one-second delay and a fresh
connection identity. Gap controls identify connection start/end and acquisition
of all selected native snapshots plus the subscription ACK. They do not certify
native sequence continuity between messages. Archive or malformed-message errors
fail the run and retain the durable prefix. Connections remain open until a real transport failure or a requested stop.
After 64 failed/closed connections, a fresh archive owner bounds connection state
before retrying; a healthy socket is never closed for owner rollover.
Reconnect failures use exponential backoff up to 60 seconds with jitter; a
fully initialized connection resets it. Stop requests remain responsive during
the wait.
The owner uses the existing chrony sampler and waits for measured alignment. Disk free space
is checked during capture against the existing 5 GiB and 10% stop thresholds.

Single-process raw capture is stored under the discovery run's `continuous/`
directory. Multi-worker runs are under `all-options/`, with immutable worker
configuration, stdout/stderr, the aggregate `result.json`, and raw data under
`workers/<n>/continuous/`. Keep the whole parent discovery run when moving data.
`verify-run ALL_OPTIONS_ROOT` validates every worker and the disjoint, exhaustive
union against discovery. Export individual worker roots with `--export`; aggregate
export is intentionally rejected. The mutable subscription-budget file is pacing
state, not raw evidence. `verify-run` checks committed segment
integrity, receipt ordering, payload counts and the parent discovery archive.
It explicitly reports raw verification scope, without claiming derived book
replay. Snapshot mode remains available with `capture_mode = "snapshots"`, positive
`duration_seconds` at most 120 and `max_frames` at most 64; that mode retains its
minute snapshot publication and final book replay checks.

The default REST response bound is8MiB. This pilot freezes a16MiB metadata-proof
budget in each run result and uses it consistently for capture and readback.
These values were sized after an actual public discovery response exceeded4MiB
and its complete response-plus-derived-metadata proof exceeded8MiB. Larger future
universes still fail closed; this is not a full-universe capacity certificate.

## Read back and export

Replace `RUN_ROOT` with the printed run directory:

```sh
.venv/bin/python -m data_collector verify-run RUN_ROOT
.venv/bin/python -m data_collector verify-run RUN_ROOT --export raw.jsonl
```

Snapshot-mode verification rereads committed segments, validates metadata source
dependencies, replays selected books, compares final states and verifies published snapshots.
Export creates a new JSONL file of the original versioned raw envelopes, including
lossless payload encoding. It never overwrites an existing export. This preserves
data for downstream processing; it is not an ML feature table.

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
