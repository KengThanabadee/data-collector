# Implementation review fixes — 20 October 2026 (Asia/Bangkok)

The six P2 findings from the independent implementation review have been addressed. This report records implementation and local verification; a second independent review of the patch has not been performed.

| Finding | Change | Regression evidence |
|---|---|---|
| Reconciliation trusted manifests and cached complete outcomes | Verify archive/context/raw checksums, coverage and slot/currency/environment identity before deriving an outcome. Recheck recent outcomes and replace stale derived indexes. Report corrupt evidence instead of complete. | Corruption before first reconciliation; corruption/deletion after caching; fixture repair and cursor loss. |
| Commands at one effective boundary replayed in filesystem order | Persist an increasing lifecycle sequence under a schedule lock; replay by effective boundary and sequence. | Resume/pause/resume with identical timestamps and deliberately reverse UUID ordering. |
| Delivery parent could overrun snapshot cutoff | Parent and worker share the same budget calculation. Parent hard kill, including TERM grace, ends ten seconds before the next snapshot boundary; skip when <=5 seconds remain. Worker also respects the parent's propagated monotonic deadline. | Delayed 09:04:41 start receives nine seconds total; later start skips. Real subprocess ignores TERM while holding collector lock, is killed at the bound, and releases the lock. |
| Delivery resumed across source releases with old provenance | Compare source fingerprint/file map, schema, metric version and config before resume. Finalize the old attempt as interrupted and create a new run from offset zero. | Clean-yield fixtures with changed source/schema/metric/config all start a distinct run while preserving old attempt evidence. |
| Soak audit missed pauses beginning before the window | Replay lifecycle state at window start and every boundary inside the half-open audit window. Same-boundary commands collapse to their final state. | Pause overlaps the first two slots; 286/288 complete still fails the gate. Resume exactly at window start is allowed. |
| Capture span omitted failed discovery attempts | New contexts carry `capture_span_version=2`. Measure from first discovery attempt, including failures, to last required successful response. Audit computes this corrected span from verified raw records even for prior archives. | Failed discovery at 100, retry at 116, final book at 130.1 yields 30.1 seconds; unrelated successes and later failed attempts do not extend the end. |

The raw envelope schema remains 1. Contexts without the metric marker retain the previous manifest calculation during verification, so existing committed manifests remain verifiable and are not rewritten. The soak audit uses the corrected timing independently of that compatibility rule.

Additional issue exposed by rerunning the original pagination test on a different calendar day: delivery destinations now use `job_date`, rather than the process's current date. The 52-page cursor-loss simulation explicitly fixes the process date to the following day, so this case remains covered. A terminal-event fixture also now uses its simulated slot date rather than the actual test execution date.

## Validation

Run `.venv/bin/python -B -m unittest discover -s tests`: **35 tests pass**, including eight new tests in `tests/test_review_regressions.py`. Exact source fingerprint, counts and elapsed time are in [review_fix_test_result.json](review_fix_test_result.json). The HTTP fault tests need permission to bind an ephemeral localhost port; an initial sandboxed run could not bind, and the complete run passed with that restriction lifted. No exchange API, notifications, recurring jobs or deployment were used.

The new reader verifies all six existing live-validation archives: four snapshot archives under `data/production/snapshots` and two delivery archives under `data/validation-delivery/delivery`. No raw/context/manifest migration was performed. `SPEC.md` remains SHA-256 `ff908b923069d7bd213d1065a073e5d62edf412576946fd22b811d6e0020ec34`.

Reconciliation prioritizes new/backlogged slots and uses the remaining budget to recheck recent slots, up to 288 slots per invocation. This is a bounded recent integrity check, not a recurring scan of the entire historical archive. Older ranges can be checked with `audit-soak` or individual `verify` calls. Legacy lifecycle events lacking sequence numbers use observed time and a deterministic ID tie-break; identical legacy timestamps cannot establish the original command order retroactively. New events record it explicitly.

A read-only local check of 20 existing archive verifications took about 0.25 seconds (approximately seven seconds when extrapolated to 576 archives). This is a local performance estimate, not evidence of monitor timing on the VPS.

Linux/systemd behavior, actual VPS resource usage, Telegram delivery and the required >=24-hour soak remain pending. Historical live API measurements in `LOCAL_VALIDATION.md` belong to the earlier source release, not this patch.
