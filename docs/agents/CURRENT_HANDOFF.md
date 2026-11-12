# Current handoff

Continue the minute-grid delivery on branch `minute-grid`. Keep replies in Thai unless the user switches languages.

## Repository state

- The integration commit is `bae997b5ed89a64637849d7663bf0c75cc773593`.
- PR #9 is open and ready for review. Do not merge it or commit to `main` without explicit user approval.
- `docs/consolidate` must not be merged. Arm B remains measurement-only on `ab-matt-dc`.
- Preserve untracked `.scratch/` evidence. In particular, `.scratch/workflow-ab/design-dc.md` is the uncommitted A/B experiment report and `.scratch/workflow-ab/acceptance-20261110/` contains copied evidence from the prior live run.

## What is complete

- Tickets #2 through #7 were implemented on `minute-grid` through the controlled A/B experiment. Arm A is integrated. Arm B is not for merging.
- Ticket #8 passed a one-hour local Lima acceptance run on earlier commit `76fbe15`.
- Follow-up fixes for realistic runtime and recovery failures are in `bae997b`. They passed 44 focused tests and fresh Standards and Spec review, including a targeted legacy-seal recovery review.
- The earlier live result is recorded in `PLANS.md`, PR #9 and `docs/v2/RUNNING.md`.

## Next requested work

Rerun ticket #8 local acceptance against `bae997b` before deciding whether to merge PR #9. The user must explicitly ask to start a run. No run or schedule is active.

Run locally with Lima VM `dc-acceptance`, never in Cloud. Cloud exchange access was blocked during preflight. Use a fresh data root for each attempt and preserve raw captures and partial evidence.

Before starting, verify the branch, exact SHA, clean tracked working tree, Lima status, target UTC crossing and current Bangkok time. The target must be recalculated when a prior time window has passed.

At the previous handoff, the local launcher was in `/tmp/dc-workflow-ab.FylsqY/launch-acceptance.sh`. Inspect it before use. If it is still valid, invoke it with the current UTC crossing:

```sh
/usr/bin/caffeinate -i -s /bin/bash /tmp/dc-workflow-ab.FylsqY/launch-acceptance.sh bae997b5ed89a64637849d7663bf0c75cc773593 YYYY-MM-DDT08:00:00
```

The launcher is expected to stage the exact tree in a new guest directory, check two healthy chrony sources, capture 62 slots with the Binance and Bybit event profiles, audit readback, copy reports, and stop Lima.

Check actual coverage, gaps, expected request outcomes, Deribit listing acknowledgements and retirement evidence, event archive readback, clock health, Binance rate weight, CPU, RSS and bytes. Scale storage using the observed duration of each profile. Record the result against the tested SHA in `PLANS.md` and PR #9. Commit and push any authorized follow-up only to `minute-grid`.
