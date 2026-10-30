# Repository Guidelines

## Project Structure & Module Organization

- `option_collector/`: working v1 Deribit REST collector, CLI, archive reader, and operations.
- `data_collector/`: v2 raw contracts, journals, immutable segments, native books, cutoff coordination, ledger, replay, and snapshots. Continuous multi-venue capture remains under development.
- `tests/`: synthetic fixtures and `unittest` suites; v2 tests use `test_dc_*.py`.
- `config/` and `deploy/`: local/VPS configuration and Linux service templates.
- `experiments/`: bounded probes and research scripts. Generated captures belong in ignored `data/` or `experiments/results/`.
- `SPEC.md` and `docs/v2/`: authoritative v2 contracts, acceptance gates, and review evidence. Preserve `docs/SPEC_V1.md` and v1 compatibility.

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

The v1 `collect-once` command performs a real public-API capture. V2 `python -m data_collector` exposes `config-check`, `discover`, bounded `capture`, and `verify-run`; see `docs/v2/RUNNING.md`. Continuous production and the remaining spec commands are pending.

## Coding Style & Naming Conventions

Use four-space indentation, `snake_case` functions/modules, `PascalCase` classes, and `UPPER_CASE` constants. Follow surrounding code; avoid unrelated formatting changes. No formatter or linter is configured. Keep native numeric precision and exact raw bytes; use explicit typed identities and versioned schemas.

## Testing Guidelines

Name tests `test_*.py` and methods `test_<behavior>`. Use synthetic data, temporary directories, and deterministic clocks. HTTP tests require loopback access; live exchange probes remain separate. Cover changed invariants, failure paths, replay determinism, and v1 compatibility. There is no percentage coverage threshold; production acceptance follows `docs/v2/ACCEPTANCE.md`.

## Commit & Pull Request Guidelines

History uses descriptive imperative subjects, such as `Add reviewed rotating WAL session writer`; no Conventional Commits prefix is required. PRs should explain the problem, behavior, validation, remaining limitations, and relevant spec/issue links.

Implement bounded chunks that each keep a runnable end-to-end path working. Obtain one fresh agent review per chunk, scoped to defects that can realistically lose, corrupt or mislabel captured data, or break the runnable path. Fix those; re-review only when a fix is non-trivial. Record approval and source hashes in `docs/v2/implementation/`. Commit only approved versions; keep unfinished work separate.

Handle an edge case only when it is realistic: observed in live capture or logs, documented by the venue as normal behavior, or a common operational failure (disconnect, reconnect, process crash/restart, disk full, rate limit, malformed message). Do not implement hypothetical scenarios that require unlikely combinations of failures; note them in one line in `PLANS.md` instead. A review finding that does not meet this bar is not a blocker and must not start a new chunk. Prefer the simplest design that works for the current scope; add no abstractions for venues or feeds that are not yet captured.

## Data & Configuration Safety

Never commit credentials, `.env` files, private healthcheck URLs, or raw archives. Keep v1/v2 data roots separate. Never overwrite immutable evidence, delete raw data to satisfy quotas, or substitute marks for missing quotes. Local tests do not establish deployment or production readiness.
