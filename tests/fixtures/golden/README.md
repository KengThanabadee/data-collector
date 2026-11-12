# Golden archives

These archives were written by the code at tag `pre-cleanup-2026-11` (commit `777b279`). `tests/test_golden.py` reads and verifies them through `python -m data_collector`, and checks that a fresh capture from the same replies matches them.

- `minute/`: a minute-grid data root with the slots 2026-11-08 23:58 and 23:59 UTC and the sealed day 2026-11-08. The command was `minute-capture --rest-only --minutes 2 --binance-symbols BTCUSDT,ETHUSDT --bybit-depth-btc 2 --bybit-depth-eth 2 --bybit-expiries 1`, followed by `minute-seal --day 2026-11-08`. It holds the minute indexes, response archives and manifests, the Bybit selections and references, the Binance reference, the hourly journal, the work journals, and the sealed day with its manifest. The Deribit ticker sampler is off.
- `binance/` and `bybit/`: one-second `capture` runs with the Binance and Bybit event configs. Each run holds its config, result, ready marker, write-ahead journal, one committed segment and the frozen collector source that `verify-run` checks.

## How they were generated

`tests/golden.py` drives `data_collector.cli.main(argv)`:

- Minute replies come from `tests/fixtures/dc_deribit_minute_rest.json`, `dc_bybit_minute_rest.json` and `dc_binance_minute_rest.json`, served on loopback with fixed `Date` and `Server` headers. Wall and monotonic clocks are discrete-event fakes: HTTP exchanges run one at a time and the clocks only advance while no exchange is pending.
- Event pushes come from `tests/fixtures/dc_binance_selected.json` and `dc_bybit_options.json` through fake WebSocket sessions, with a fake chrony reader.

To regenerate them, export the tagged tree, copy `tests/golden.py` into it and run the generator from there:

```sh
git archive -o /tmp/pre-cleanup.tar pre-cleanup-2026-11
mkdir /tmp/pre-cleanup && tar -xf /tmp/pre-cleanup.tar -C /tmp/pre-cleanup
cp tests/golden.py /tmp/pre-cleanup/tests/golden.py
(cd /tmp/pre-cleanup && "$OLDPWD/.venv/bin/python" -m tests.golden /tmp/golden-out)
```

Then replace `minute/`, `binance/` and `bybit/` here with the directories in `/tmp/golden-out`.

## Fields the comparison excludes

`tests/test_golden.py` names each excluded field in `MINUTE_EXCLUDED`, `EVENT_EXCLUDED`, `WAL_SEAL_SIZE` and `CONTROL_PAYLOAD_SIZE`. Everything else must match after decompression.

- Minute archive: `code` in `context.json` (code identity); `data_root` and `config_sha256` in `context.json` (temporary data root); the loopback port in `effective_url`; `archive_bytes`, `archive_sha256`, `context_sha256` and `journal_sha256` in minute manifests; `archive_bytes`, `archive_sha256`, `source_bytes`, `source_sha256` and each hour's `bytes` and `sha256` in the daily manifest. These digests and sizes cover files that hold the excluded fields.
- Event runs: the `source/` directory and `source_sha256` (code identity); `data_root` and `config_sha256`; run IDs (`capture_id`, `process_session_id`, `session_id`, the random suffix of connection IDs, and the run and segment directory names); times (`utc_ns`, `monotonic_ns`, `receive_utc_ns`, `started_utc_ns`, `epoch_started_ns`, `recovery_next_utc_ns`, `uncertainty_ns`, `elapsed_seconds`, `date`, the capture date in paths, `next_connect` and the chrony output `stdout_base64`); and record IDs, digests and sizes over records that carry times or run IDs (`record_id`, `source_record_id`, `last_record_id`, `last_closed_record_id`, `sha256`, `raw_sha256`, `evidence_sha256`, `previous_sha256`, `head_sha256`, `records_sha256`, `archive_sha256`, `archive_bytes`, `uncompressed_bytes`, `manifest_sha256`, `context_sha256`, `session_sha256`, `binding_sha256`, `wal_sha256`, `wal_bytes`, the `bytes` size in write-ahead journal seals, and the `bytes` count of control payloads). Journal frames are checked against their own checksums instead of compared byte for byte. Event records are compared in stream order, because records from separate connections interleave by arrival.
