# G3 External Review Tasklist

- [x] Rebuild the review boundary from unstaged tracked diffs, untracked files, and the empty staged baseline.
- [x] Re-read project authority/process docs and prior external-review style: `CLAUDE.md`, architecture/cluster docs, G3 plan/review, and recent external reports.
- [x] Review G3 #1 seed auto-convergence design: derive rules, host-match ownership, VIP/custom endpoint behavior, deterministic gating, bootstrap preservation, and first-publish safety.
- [x] Review online membership trigger points: join, retire, online force-single, recovery node remove/ghost remove, and leadership backstop.
- [x] Review offline force-single seed drop-only path: transaction boundary, generation bump, empty-set floor, host matching, and replay/idempotency.
- [x] Review G3 #17 ctl roster-pull path: live NATS responder, permissions, actor scoping, PIN/no-TOFU gate, fallback behavior, TTL throttling, and cache write/no-poison semantics.
- [x] Review subject namespace and ACL implications against the broker-only `cluster.*` boundary and mixed-version behavior.
- [x] Review changed docs for operational honesty, especially VIP/LB clobbering, IP fallback limitations, and first-publish/manual seed expectations.
- [x] Audit added tests for false confidence and add independent reviewer regressions/probes for uncovered high-risk paths.
- [x] Run focused Go tests for touched packages and reviewer-added regressions.
- [x] Run static checks and, if feasible, broader package compilation/gates.
- [x] Decide whether simcluster deploy-tier drills are required for this change and record the rationale or blocker.
- [x] Write the final external review report with Fail/Pass conclusion, doubts, findings, recommendations, and verification.
- [x] Stage all files after the review, per user request.
