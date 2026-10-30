# Repository Guidelines

## Project Structure & Module Organization

- `option_collector/`: working v1 Deribit REST collector, CLI, archive reader, and operations.
- `data_collector/`: v2 raw contracts, journals, immutable segments, native books, cutoff coordination, ledger, replay, and snapshots. Continuous multi-venue capture remains under development.
- `tests/`: synthetic fixtures and `unittest` suites; v2 tests use `test_dc_*.py`.
- `config/` and `deploy/`: local/VPS configuration and Linux service templates.
- `experiments/`: bounded probes and research scripts. Generated captures belong in ignored `data/` or `experiments/results/`.
- `SPEC.md` and `docs/v2/`: authoritative v2 contracts and acceptance gates. Preserve `docs/SPEC_V1.md` and v1 compatibility.
- `PLANS.md`: current status, next steps and one-line notes. `docs/agents/`: task brief format and review prompt. `docs/v2/history/` holds the archived chunk log from October 2026 for looking up past decisions.

## Build, Test, and Development Commands

Use Python 3.12 from the repository root. Create the environment and install pinned dependencies:

```sh
python3.12 -m venv .venv
.venv/bin/python -m pip install -r requirements.lock
```

Run these with `.venv/bin/python`:

- `-m pip wheel --no-deps . -w dist`: build a distributable wheel.
- `-m unittest discover -s tests -v`: run all repository tests.
- `-m unittest tests.test_dc_snapshots -v`: run a focused suite.
- `-m option_collector status --json`: inspect local collector status.

The v1 `collect-once` command performs a real public-API capture. V2 `python -m data_collector` exposes `config-check`, `discover`, `capture`, and `verify-run`; see `docs/v2/RUNNING.md`.

## Coding Style & Naming Conventions

Use four-space indentation, `snake_case` functions/modules, `PascalCase` classes, and `UPPER_CASE` constants. Follow surrounding code; avoid unrelated formatting changes. No formatter or linter is configured. Keep native numeric precision and exact raw bytes; use explicit typed identities and versioned schemas.

## Testing Guidelines

Name tests `test_*.py` and methods `test_<behavior>`. Use synthetic data, temporary directories, and deterministic clocks. HTTP tests require loopback access; live exchange probes remain separate. Cover changed invariants, failure paths, replay determinism, and v1 compatibility. There is no percentage coverage threshold; production acceptance follows `docs/v2/ACCEPTANCE.md`.

## Commit & Pull Request Guidelines

Write imperative subjects that name the behavior change, such as `Reconnect Deribit book sessions after connection loss`; no Conventional Commits prefix is required. PRs should explain the problem, behavior, validation, remaining limitations, and relevant spec/issue links.

Implement bounded chunks that each keep a runnable end-to-end path working. Obtain one fresh agent review per chunk using the prompt in `docs/agents/REVIEW.md`, scoped to defects that can realistically lose, corrupt or mislabel captured data, or break the runnable path. Fix those; re-review only when a fix is non-trivial. Git history is the review record: put the review outcome in the commit message and add one line per chunk to `PLANS.md`. Commit only approved versions; keep unfinished work separate.

Handle an edge case only when it is realistic: observed in live capture or logs, documented by the venue as normal behavior, or a common operational failure (disconnect, reconnect, process crash/restart, disk full, rate limit, malformed message). Record hypothetical scenarios that need unlikely combinations of failures as one line in `PLANS.md` and move on. A review finding below this bar is a note, not a blocker or a new chunk. Choose the simplest design that works for the venues and feeds being captured now.

## Scope & Delivery

- Keep the full SPEC.md venue and product scope as the capture target: the archive is meant for future research ideas. Grow it raw-first: add venues and feeds as raw capture with gap records, starting with Deribit options. Build derived layers (snapshots, validity corrections, replay proofs) when a concrete research use needs them.
- Make limits configurable, with defaults sized for real use. The collector runs continuously, so duration, frame and byte limits default to unbounded or to values measured from live data.
- Start every goal from a brief in the format of `docs/agents/BRIEF.md`.
- Work autonomously toward the goal; the user is usually away. When a choice is unclear, pick the simplest option consistent with SPEC.md, record it as one line in `PLANS.md`, and continue.
- Measure progress by what the user can run. When three consecutive chunks have not improved it, re-plan: take the shortest path to the goal's runnable result and move the remaining ideas to one-line notes in `PLANS.md`.
- State each limitation once, in the document where it belongs. Write plain sentences with normal spacing, such as `at most 120 seconds` and `16 MiB`.
- Count tests as passing only when they finish without errors or warnings, including `ResourceWarning`.

## Data & Configuration Safety

Never commit credentials, `.env` files, private healthcheck URLs, or raw archives. Keep v1/v2 data roots separate. Never overwrite immutable evidence, delete raw data to satisfy quotas, or substitute marks for missing quotes. Describe readiness by what was actually run and observed.
