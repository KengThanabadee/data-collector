# Review prompt

Give the reviewer agent this prompt, the task brief and the diff for the chunk.

```
Review this chunk against its task brief. Answer three questions:

1. Data: can a realistic event lose, corrupt or mislabel captured data?
   Realistic means observed in live capture or logs, documented by the venue
   as normal behavior, or a common operational failure: disconnect, reconnect,
   process crash or restart, disk full, rate limit, malformed message.
2. Run: does the chunk break a command the user can run today?
3. Done: does the chunk move the brief's "Done when" closer, or meet it?

Report each finding as:
- BLOCKER: <question number> <the concrete event> -> <what goes wrong> <file:line>
- NOTE: <anything else worth one line>

A BLOCKER needs a concrete event from the realistic list above. Scenarios that
need several unlikely failures at once, style, structure and future features
are NOTEs. If there are no BLOCKERs, say "Approved".
```

Fix BLOCKERs, then commit with the review outcome in the commit message. Add NOTEs worth keeping to `PLANS.md` Notes as one line each. Re-review only when a BLOCKER fix is non-trivial.
