# G1-G7 Audit External Review Tasklist

Scope: external review of the current unstaged and untracked worktree for the G1-G7 cross-cutting audit fixes. Treat the internal plan/review as an index only; verify from code, tests, docs, and live checks.

- [x] Read `CLAUDE.md`, architecture/cluster docs, simcluster operating constraints, and prior external-review report style.
- [x] Rebuild the review boundary from `git status`, unstaged `git diff --stat`, cached diff, and untracked files.
- [x] Read the internal G1-G7 audit plan/review without trusting its conclusions.
- [x] Audit CLI grow/add driver changes: dry-run webhook suppression, join-op resume classification, BLOCKED auto-confirm edge counting, timeout diagnostics, and seam comments.
- [x] Audit cluster upgrade planning changes: node-list fail-closed behavior, signed-roster retry/fallback warning, mixed-version roster consistency, and no-op version correlation.
- [x] Audit broker grow cutover changes: tolerant /varz probe, restart decision, hard restart timeout, backup-path idempotency, and deploy-tier implications.
- [x] Audit broker upgrade trigger, proxy status/rebalance, transfer bucket sizing, seed convergence comments, adminsock/proto ledgers.
- [x] Audit offline force-single seed pruning and warning behavior.
- [x] Check for new races, deadlocks, lock ordering regressions, authorization gaps, and fail-open paths around all touched code.
- [x] Run focused Go tests for changed cmd/tether, broker, and clusteroffline surfaces plus `git diff --check`.
- [x] Attempt the appropriate simcluster deploy-tier drill for the grow cutover surface; blocked before staging current binary because SSH to `weilandserver` closed during key exchange.
- [x] Write final external review report with leading Pass/Fail verdict, findings, doubts, recommendations, and verification log.
- [x] Stage all files after the review is complete.
