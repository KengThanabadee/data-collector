# Task brief

Every goal gets a brief before work starts. When the user gives a goal without one, write the brief yourself under `## Next` in `PLANS.md`, then start. Keep it to these four parts:

```
Goal: <one sentence: what the user can do afterwards>
Done when: <a command the user can run and the result they will see>
Out of scope: <work that belongs to a later goal>
When unsure: pick the simplest option consistent with SPEC.md, add one line to PLANS.md Notes, continue.
```

Write "Done when" as something observable: a command, a duration, a file that opens, a check that passes. Test counts and review approvals belong in the commit message, not here.

The goal is finished when "Done when" is true. Then update `PLANS.md` Current state and move to the next item.

## Example

```
Goal: Collect Deribit option books continuously.
Done when: `capture` runs for one hour on selected BTC/ETH options, stops cleanly on Ctrl-C, and `verify-run` passes on the result.
Out of scope: other venues and feeds, new snapshot or correction features, clock handling beyond what capture already does.
When unsure: pick the simplest option consistent with SPEC.md, add one line to PLANS.md Notes, continue.
```
