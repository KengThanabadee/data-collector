# Implementation progress

Based on the reviewed [SPEC.md](SPEC.md). Source repository: [KengThanabadee/option-collector](https://github.com/KengThanabadee/option-collector) (private). Deployment and recurring collection have not been performed.

- [x] Core: validated configuration, async transport, frozen universe, bounded collection, reader.
- [x] Durability: journal, verified publication, recovery, lock, slot events and gap accounting; local fault tests.
- [x] Daily delivery: pagination, clean yield/resume, shared quota and snapshot priority; simulated52-page workload and cursor-loss recovery.
- [x] Operations prepared: CLI, monitoring, export, systemd units, installer and short operations guide. Linux runtime validation remains pending.
- [x] Local validation: fixtures, HTTP fault server, crash tests and one full production public-API snapshot capture (BTC884/884,ETH766/766).
- [x] Fix the six independent implementation review findings; add regression coverage and verify existing archives unchanged. See [fix evidence](docs/IMPLEMENTATION_REVIEW_FIXES.md).
- [ ] Linux validation and actual VPS: pending a Linux host; no production-readiness claim before the specified 24-hour soak.

Optional candle backfill follows the core snapshot rollout; it does not block starting prospective collection.

See [local validation evidence](docs/LOCAL_VALIDATION.md) for the exact tests, captured archives and remaining acceptance work. The reviewed spec is preserved unchanged; these checkboxes describe implementation progress rather than a production-readiness certificate.
