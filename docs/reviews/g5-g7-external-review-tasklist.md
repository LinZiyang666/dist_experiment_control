# G5/G7 External Review Tasklist

- [x] Read `CLAUDE.md`, core architecture docs, cluster docs, and prior review/tasklist style.
- [x] Rebuild the review boundary from the current unstaged/untracked worktree (empty staged baseline).
- [x] Coarsely inspect the G5/G7 plans and internal Stage-C reviews without treating their conclusions as authority.
- [x] Inspect the G5 rolling-upgrade implementation end to end: signed trigger, auth/ACL, planner, driver, broker re-exec, agent re-exec, version correlation, and docs.
- [x] Inspect the G7 data-plane implementation end to end: proxy home rendering, remote homes/seeds/status views, auto-rebalance gates, and JS health signaling.
- [x] Check protocol/wire compatibility: ProtoVersion/ClusterHealth schema, command version exposure, new subjects, permissions, old-broker behavior, and mixed-version fallbacks.
- [x] Check concurrency/lifecycle hazards: broker Run/re-exec shutdown ordering, NATS subscription wiring, admin handle races, observability loop state, and repeated/partial roll idempotency.
- [x] Check ACL/security hazards: account-signed upgrade trigger replay/window/targeting, member permissions, `--homes --remote` aggregate leakage, and untrusted self-reported fields.
- [x] Check user-facing CLI/docs consistency: flags, exit-code taxonomy, hidden environment switches, cluster manual updates, and backup/staging warnings.
- [x] Add focused external-review tests for confirmed gaps when cheap and independent.
- [x] Run focused Go tests for touched packages and review-added tests.
- [x] Decide whether simcluster is needed; if not, document why. If needed, read server/runbook info and run only the relevant drill.
- [x] Write the external review report with a leading Fail/Pass verdict, doubts, issues, recommendations, and verification.
- [x] Stage all files after the external review.
