# GitHub Issues

Specs and tickets for this repository live in GitHub Issues on `KengThanabadee/data-collector`. Use `gh` with this repository selected.

- Read an issue and its discussion with `gh issue view <number> --repo KengThanabadee/data-collector --json number,title,body,labels,comments`.
- List work with `gh issue list --repo KengThanabadee/data-collector` and filter by state or label as needed.
- Publish a spec with `gh issue create --repo KengThanabadee/data-collector --title "..." --body-file <file> --label ready-for-agent`.
- Publish a ticket the same way. Use `--parent <number>` for a native sub-issue and `--blocked-by <number>,<number>` for its direct blockers. Use `gh issue edit` with `--add-sub-issue` or `--add-blocked-by` when linking existing issues.
- For standalone work, a ticket is ready when it is open, unassigned and every blocker is closed. Use `gh issue view <number> --json blockedBy` to inspect dependency states. Claim with `gh issue edit <number> --add-assignee @me`.
- During `implement-spec`, a blocker clears for scheduling once its verified changes land on the integration branch. GitHub closes the issues when the integration PR merges.
- Apply or remove triage labels with `gh issue edit` and `--add-label` or `--remove-label`. See [triage labels](triage-labels.md).
- Resolve implementation tickets through closing references in the integration PR. For work resolved without a PR, record the outcome with `gh issue comment <number> --body-file <file>` and close the issue with `gh issue close <number>`.
- For wayfinding, label the parent issue `wayfinder:map` and add child issues with `wayfinder:research`, `wayfinder:prototype`, `wayfinder:grilling` or `wayfinder:task` as appropriate. Add the result to the child and link the decision from the map before closing the child.

**PRs as a request surface: no.**
