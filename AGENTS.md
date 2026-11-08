# Repository Guidelines

## Project Structure & Module Organization

- `option_collector/`: working v1 Deribit REST collector, CLI, archive reader, and operations.
- `data_collector/`: v2 raw contracts, journals, immutable segments, native books, cutoff coordination, ledger, replay, and snapshots. Continuous multi-venue capture remains under development.
- `tests/`: synthetic fixtures and `unittest` suites; v2 tests use `test_dc_*.py`.
- `config/` and `deploy/`: local/VPS configuration and Linux service templates.
- `experiments/`: bounded probes and research scripts. Generated captures belong in ignored `data/` or `experiments/results/`.
- `SPEC.md` and `docs/v2/`: authoritative v2 contracts and acceptance gates. Preserve `docs/SPEC_V1.md` and v1 compatibility.
- `PLANS.md` records readiness and measured evidence. `docs/agents/` configures the engineering skills. `docs/v2/history/` archives earlier scope and run evidence.

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

Name tests `test_*.py` and methods `test_<behavior>`. Use synthetic data, temporary directories, and deterministic clocks; venue-facing code is tested against recorded venue replies. HTTP tests require loopback access; live exchange probes remain separate. Cover changed invariants, failure paths, replay determinism, and v1 compatibility. There is no percentage coverage threshold; production acceptance follows `docs/v2/ACCEPTANCE.md`.

## Commit & Pull Request Guidelines

Write imperative subjects that name the behavior change, such as `Reconnect Deribit book sessions after connection loss`; no Conventional Commits prefix is required. PRs should explain the problem, behavior, validation, remaining limitations, and relevant spec/issue links.

Implement bounded vertical slices that each keep a runnable end-to-end path working.

Review for realistic failures that can lose, corrupt or mislabel data or break a runnable path. Treat events observed in capture or logs, documented venue behavior, and common operational failures as realistic. Track unlikely combinations as issue notes. Choose the simplest design that works for the selected venues and feeds.

## Scope & Delivery

- Follow [the collection scope](docs/v2/COLLECTION_SCOPE.md) for the current capture target. Grow it raw-first with explicit gap records. Build derived layers when a concrete research use needs them.
- Make limits configurable, with defaults sized for real use. The collector runs continuously, so duration, frame and byte limits default to unbounded or to values measured from live data.
- Live runs on the Lima VM `dc-acceptance` (6 CPU, 6 GiB): when the run ends or stalls, check `limactl list` and run `limactl stop dc-acceptance` if it is idle. The disk is kept, and `limactl start` restores it in under a minute.
- State each limitation once, in the document where it belongs. Write plain sentences with normal spacing, such as `at most 120 seconds` and `16 MiB`.
- Count tests as passing only when they finish without errors or warnings, including `ResourceWarning`.

## Agent skills

### Issue tracker

Use GitHub Issues for feature specs, implementation tickets and dependencies. See [issue tracker](docs/agents/issue-tracker.md).

### Triage labels

Use the five default roles. See [triage labels](docs/agents/triage-labels.md).

### Domain docs

Use a single glossary and ADR directory. See [domain docs](docs/agents/domain.md).

### Delivery

- Use `grill-with-docs` for unresolved design decisions, `to-spec` for the agreed feature spec and `to-tickets` for runnable vertical slices with explicit blockers.
- Use `implement` for one slice and `implement-spec` for a ticket graph. Reuse the integration branch named in the spec. Use TDD at existing public seams and link commits and PRs to the relevant issues.
- Review the branch against its base with `code-review`. The Standards review applies the data integrity and runnable-path rules above. The Spec review uses the originating issue and the contracts it references. Resolve findings that block the agreed behavior before merging.
- Keep current readiness and measured evidence in `PLANS.md`. Track work and planning decisions in GitHub Issues. Record durable domain decisions in ADRs.

## Data & Configuration Safety

Never commit credentials, `.env` files, private healthcheck URLs, or raw archives. Keep v1/v2 data roots separate. Never overwrite immutable evidence, delete raw data to satisfy quotas, or substitute marks for missing quotes. Describe readiness by what was actually run and observed.
