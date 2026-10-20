# Local validation

All fixture instruments, quotes and delivery rows in this directory are **synthetic**. The HTTP test server binds only `127.0.0.1` on an ephemeral port and never calls Deribit. Run with Python3.12:

```sh
.venv/bin/python -m unittest discover -s tests -v
```

The suite checks lossless text/binary bodies, decimal lexemes/unknown fields, body caps, drip-feed total deadlines, in-flight cancellation, HTTP200/API errors, non-JSON429, persisted cooldown, retries, frozen universe, completeness, locks, clock steps, slot events, clean yield/crash recovery, pagination and export. SIGKILL tests exercise five actual publication checkpoints; a separate subprocess ignores SIGTERM to test the external hard stop.

The 52-page delivery case uses a fake clock, 2 requests/second and 0.5-second page latency with 100-row pages despite count1000. Snapshot occupancy is184seconds in each300second slot. This is a simulation of scheduling correctness, not measured VPS throughput.

VPS/Linux/systemd/reboot, real Telegram alerts and24-hour soak remain separate acceptance work. Existing `experiments/test_*.py` cover the earlier experimental archive independently.
