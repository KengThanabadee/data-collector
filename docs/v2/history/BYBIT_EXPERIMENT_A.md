# Bybit options experiment A — corrected historical report

Corrected on 3 November 2026 during step-4 integration. These measurements describe the old experimental code, not new live qualification. The original branch and its archives remain unchanged.

Branch `exp/bybit-a` at e4301c4, based on option-collector commit 2813b78. Runs on 31 October 2026 in the Lima VM `dc-acceptance`, under `~/exp-bybit-a/` only.

## What was built

- `data_collector/bybit_options.py`: raw capture and `verify-run` for Bybit BTC/ETH options. `config/v2-bybit-options.toml` is the shipped profile.
- Feeds: `tickers.<symbol>` for every BTC/ETH option and `publicTrade.BTC`/`publicTrade.ETH` on `wss://<ws_host>/v5/public/option`.
- Discovery: every page of `/v5/market/instruments-info?category=option&baseCoin=<coin>&limit=1000`, with no status filter, so PreLaunch options are included. Discovery runs every 60 seconds (`discovery_interval_seconds`) and reuses the existing `bybit_options` projection and `OptionUniverse` policy. Each HTTP request and response, and a per-poll result, are archived append-only under `metadata/`. A new universe version is written when membership or native metadata changes, or when an option passes its `deliveryTime`. Then the option is retired and unsubscribed on its open socket. A failed poll keeps the previous members; after an HTTP 403, the next poll waits 10 minutes.
- Timing evidence: each universe version records observation and retirement times. Each subscription ACK writes per-option observed, request and ACK times, plus a `first_ack` flag and a 120-second deadline result.
- Sharding: topics are packed onto connections up to `shard_arg_chars` (default 14,000) args-array characters and `shard_max_args` (default 1,000) args. Both settings are capped at Bybit's documented 21,000 and 2,000 per connection. A new listing goes to the least-loaded shard with room, or opens a new connection. New connections are spaced at least 5 seconds apart.
- Hosts: `rest_host` and `ws_host` are configurable. The defaults are `api.bybit.com` and `stream.bybit.com`; the shipped config uses `api.bytick.com` and `stream.bytick.com`.
- Raw bytes: every inbound WebSocket message is archived exactly as received before it is parsed. Connections request permessage-deflate, and the decompressed message payload is what gets archived. Each connection records whether compression was negotiated.
- Protocol facts, observed live:
  - Option command responses (`type: COMMAND_RESP`) carry no `req_id`, so each connection has one subscribe or unsubscribe in flight.
  - The first ticker snapshot can arrive before the ACK, so a push counts once its topic has been requested.
  - Any `failTopics` entry, or `success` other than true, fails the run; the raw response is kept.
- `verify-run`:
  - checks raw integrity;
  - re-derives every poll and every universe version from the archived HTTP bytes, and checks that each unchanged poll really was unchanged;
  - binds every ACK to its single in-flight request on the same connection;
  - re-computes every timing-evidence record byte for byte;
  - requires an ACK and a first ticker push for every option incarnation.

  The original experimental verifier also exempted options observed less than 120 seconds before retirement or stop and did not reject late first subscription ACKs. These were defects under COLLECTION_SCOPE.md. The integrated reader removes both exemptions: each incarnation needs ACK plus first push before retirement or stop, and a first ACK over 120 seconds after observation fails. A later reconnect of an already covered option remains a recorded gap. `publicTrade` needs only its ACK.
- Small shared change: `OptionUniverse.changes()` previews whether a refresh would change anything, so unchanged polls do not add universe versions.

## Live runs (dc-acceptance, `api.bytick.com` / `stream.bytick.com`)

Both 10-minute runs were `duration_seconds = 600` after full initialization. Both ended `raw_capture_complete` and `verify-run` passed with `changing_universe_coverage_verified: true`.

| | Run 1 (13:42 UTC, uncompressed) | Run 3 (14:21 UTC, permessage-deflate) |
|---|---:|---:|
| Options subscribed (BTC 658, ETH 582) | 1,240 | 1,240 |
| Options with ACK and first push (verified) | 1,240 | 1,240 |
| Shards (args / args characters) | 400/13,968, 410/13,996, 416/13,994, 16/528 | same |
| Connections (initial 4 + reconnects) | 13 | 14 |
| Socket closes by Bybit / client timeouts | 9 / 0 | 5 / 5 |
| WebSocket messages | 592,404 | 421,300 |
| Native payload bytes | 417,174,618 | 296,678,345 |
| Ticker / publicTrade / other messages | 592,065 / 233 / 106 | 421,053 / 152 / 95 |
| Subscription rejections | 0 | 0 |
| Discovery polls (failed) | 11 (0) | 11 (0) |
| Universe versions | 1 | 1 |
| Start to ready | 77.0 s | 114.5 s |
| Max first observed-to-ACK | 16.4 s | 53.6 s |
| Records verified | 592,797 | 421,716 |
| Peak RSS | 65,120 KiB | 63,100 KiB |
| CPU time / wall | 311 s / 676 s | 208 s / 712 s |
| Retained data: total / archive / WAL / metadata | 1.36 GB / 166 MB / 1.18 GB / 10.1 MB | 0.97 GB / 116 MB / 844 MB / 10.1 MB |

"Other" messages are command responses and pongs. Byte figures are logical file sizes. A 60-second smoke run before these runs also passed `verify-run` (96,264 messages, 1,240 options, no reconnects). One failed attempt (run 2) hit the 180-second bootstrap deadline while the link was down to about 50 KB/s; its raw prefix is retained.

**Network-limited throughput.** The experiment estimated demand around 1,240 messages and 0.85 MB of payload per second. This is an observed-rate estimate, not the documented nominal 100 ms ticker cadence or a guaranteed future rate.

- The host downlink measured about 0.75 MB/s at best and 0.05 to 0.16 MB/s at worst during the session.
- The collector process was mostly idle: a stack sampler showed 98% of the main thread in `select`, and fsync took about 1 ms.
- On the 400-topic connections, receive lag grew steadily. In run 1 it reached about 90 seconds, then Bybit dropped those connections (TCP close without a close frame). The 16-topic shard kept sub-second lag.
- Compression roughly doubled throughput on the host in a side-by-side test (1,274 compared with 505 messages per second). During run 3, however, the link was worse again: median lag ranged from 13 to 125 seconds per minute.
- Every reconnect is recorded as an explicit gap. Independent readback on 3 November found only 11 of run 3's 14 opened connection epochs reached gap_end; shard-1-2, shard-1-3 and shard-2-2 never fully initialized. Two recovery outages lasted 150.270 and 70.225 seconds, and native-to-receipt lag peaked at 173.492 seconds. These archives prove historical per-incarnation coverage with gaps, not recovery of every attempt or continuous low latency. See [readback evidence](../bybit-experiment-readback.json). Bybit live capacity runs await the VPS; the exact cause of upstream closes is unproved.

## Character-cap probe

The original report claimed that one connection accepted 658 BTC tickers over three requests and a single request accepted 1,240 tickers (42,443 characters). The raw artifacts for that larger experiment-A claim were not identified, so it is not treated as verified evidence.

The independently retained protocol-only probe was checked directly against its raw request and COMMAND_RESP bytes on 3 November: 658 topics / 22,887 args characters succeeded in one request, and the same topics succeeded across two requests on one connection (11,602 and 11,286 chars; 22,887 combined). Every requested topic was ACKed with no failTopics. The documented cap was not enforced by rejection in those cases; this does not establish an unlimited allowance. Keep the documented per-connection sharding with headroom. [Exact evidence and hashes](../bybit-cap-evidence.json) identify this separate probe; its storage under exp-bybit-b does not mean the rejected adapter was integrated.

## Tests

- `.venv/bin/python -W error -m unittest discover -s tests`: 711 tests, OK (701 before this change), with no warnings.
- New `tests/test_dc_bybit_options.py` (10 tests) covers:
  - sharding across connections with a paginated BTC walk;
  - a PreLaunch listing added mid-run and verified with its timing evidence;
  - expiry retirement with an unsubscribe on the open socket;
  - a rejected subscription failing the run with the raw response kept;
  - disconnect and re-subscribe;
  - a failed poll keeping members, followed by recovery;
  - an ACK without a first push never initializing;
  - verifier counter tampering;
  - config defaults and bounds;
  - documented cap packing.
- One full-suite run under VM load hit a 1-second subprocess timeout in the untouched `test_dc_clocksource`. It passed on rerun, and the final full run was clean.

## Known limitations

- Not exercised live, because nothing listed or expired during the runs:
  - a PreLaunch option (none was listed at 13:00 to 14:35 UTC; Bybit pre-lists hours before 08:00 UTC);
  - a mid-run listing;
  - expiry retirement and unsubscribing an expired topic.

  Fixtures cover all three; the expiry-window acceptance run must confirm them, including whether Bybit accepts unsubscribing a delivered topic.
- If a PreLaunch ticker sends no snapshot, its shard cannot initialize and reconnects at the bootstrap timeout. This is unverified.
- All shards run in one process and share one archive lane. The process was not the bottleneck here, but its message-rate ceiling at full Bybit rate on a fast link is unmeasured.
- Shards are not merged after retirements.
- At shutdown after stalled connections, run 3 printed 3 "unclosed transport" ResourceWarnings for stream sockets; the exit code and archive were unaffected. A close that exceeds `timeout_seconds` now aborts the connection, but SSL teardown on a stalled link can outlive the process.

## Review

After commit 1b13aff, one fresh reviewer agent reviewed the chunk with the prompt in `docs/agents/REVIEW.md`. Its scope included the paths that never ran live: PreLaunch, new listings, expiry and retirement, reconnects and failed polls. It also checked capture and verify-run for ordering races. Outcome: **Approved, no BLOCKERs.**

- **Fixed (a NOTE):** when a rejection arrived mid-batch, the frames after it were archived but not counted. verify-run on that failed run then stopped with "raw option counters differ" instead of verifying the retained raw. Counters now include the whole durable batch first. The rejection test sends a frame after the ACK; it fails without the fix and passes with it.
- **Recorded in PLANS.md notes:**
  - unverified response shapes for unsubscribing a delivered topic and subscribing a PreLaunch topic;
  - a millisecond stop window that could fail verification;
  - inline polls delaying other checks;
  - pagination not exercised live;
  - an option that disappears from one poll and returns overwriting its observation time.
- **Already listed under Known limitations above:** a PreLaunch ticker that never pushes.

## Evidence locations (inside dc-acceptance)

- Run 1: `~/exp-bybit-a/data-live/runs/842880a33b854ac489d6f50936c05676`, with `live.out`, `live-verify.json`, `live-stats.json` and `live-resources.txt` in `~/exp-bybit-a/`.
- Run 3: `~/exp-bybit-a/data-live3/runs/e17d001e3f784f7b908cc1f1373da441`, with `live3.out`, `live3-verify.json`, `live3-stats.json` and `live3-resources.txt`.

## Integration corrections

The shipped main profile uses `bybit_rest_host` / `bybit_ws_host` and explicitly freezes `raw_feeds = ["ticker", "trades"]`. The historical archives' incorrect `raw_feeds = ["book"]` configuration is retained as evidence. New fixtures cover late first subscriptions, strict coverage at stop and retirement, reconnect after 120 seconds, stop during a reconnect, and bounded close isolation. Current validation is recorded in the integration commit; the old test counts above are historical.
