# G5/G7 External Re-review Round 2 Tasklist

Scope: re-review the developer's response and unstaged fixes after the first external review. Treat the
main-process reply as untrusted; verify behavior from code and focused tests.

- [x] Re-read project workflow and relevant docs/review conventions (`CLAUDE.md`, `docs/reviews/*`) and identify the
  modified surface from `git diff --stat`.
- [x] Reconstruct the developer delta against the first review: G5 cluster upgrade orchestration, upgrade
  trigger security/retry behavior, replicated upgrade lock, whole-host agent re-exec, G7 status/proxy signals,
  docs/tests.
- [x] Verify B1/B2/M1/M2/M3 and minor findings were actually closed, including recovery paths rather than only
  happy-path tests.
- [x] Add or run independent focused tests for the highest-risk regressions: planner ambiguity, lock release,
  stale/stuck lock self-heal, membership operation overlap, and status signal aggregation.
- [x] Inspect security and protocol invariants: account-signed trigger, SHA requirements, session-scoped agent
  re-exec, roster source trust, replay guard, and NATS permission assumptions.
- [x] Run targeted Go tests and static checks that fit the changed surface; record failures and skipped heavier
  deploy-tier/simcluster tests with rationale.
- [x] Write the round-2 external review report with a leading Pass/Fail verdict, explicit doubts, findings,
  recommendations, and verification log.
- [x] Stage all files after the review is complete.
