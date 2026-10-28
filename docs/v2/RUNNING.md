# Run the bounded Deribit collector

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
TOML to one or two current active option names from that list, then run:

```sh
.venv/bin/python -m data_collector capture --config config/v2-discovery.toml
```

Capture uses explicit `book.<symbol>.agg2` channels. The first shared minute is
selected after alignment. Stop with Ctrl-C, or let the first duration/frame/byte
bound end the run. `duration_seconds` is at most 120 and `max_frames` at most 64;
these preserve the existing proof horizon. A frame limit may stop before any
minute snapshot is produced. A run receiving only ACKs fails; it must acquire
native book snapshots for all selected symbols. Inspect `runtime`, `books`,
`snapshot_ids`, `status` and any `error` in the printed result and `result.json`.

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

Verification rereads committed segments, validates metadata source dependencies,
replays selected books, compares final states and verifies any published snapshots.
Export creates a new JSONL file of the original versioned raw envelopes, including
lossless payload encoding. It never overwrites an existing export. This preserves
data for downstream processing; it is not an ML feature table.

## Current boundary

Failed/cancelled runs retain their durable raw prefix; inspect their error before
using data. Each command starts a new capture identity, so separate runs do not
claim uninterrupted coverage. Crashed sessions remain on disk for the existing
recovery APIs; automatic CLI restart reconciliation is pending. The initial source
bundle freezes collector Python only; full vendor-schema release qualification is
pending. Other feeds/venues, long-running capacity, resource enforcement,
independent backup/restore and intended-host acceptance remain required by SPEC.
