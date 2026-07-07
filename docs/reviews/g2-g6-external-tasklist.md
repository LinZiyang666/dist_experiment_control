# G2/G6 External Review Tasklist

- [x] Rebuild the review boundary from `git status`, unstaged tracked diffs, untracked tests/docs, and verify the staged baseline.
- [x] Re-read project process and authority docs: `CLAUDE.md`, cluster architecture/runbook, simcluster mandate, and prior external review style.
- [x] Review G2 force-single roster pruning: online best-effort prune, offline direct-SQL prune, generation bumps, idempotency, restart/snapshot implications, and client roster convergence.
- [x] Review ghost voter removal and migration guard: leader-only proof, raft config freshness, resource ownership guard, topology peer filtering, NATS mesh render safety, and false self-only risks.
- [x] Review offline de-cluster rendering: identity source, multi-user auth parsing, store_dir preservation, already-standalone no-op, validation/swap path, operator messages, and online runbook parity.
- [x] Review DATA-PLANE-DEGRADED status banner and cold-start diagnostics for correctness, non-fatal behavior, and stale/unsafe operator guidance.
- [x] Review G6 tier-B/Object Store capacity logic: ceiling source, free-space fallback, clamp/refuse invariants, bucket create/update behavior, transfer admission, and NATS config preflight.
- [x] Review changed docs and simcluster drills against the mandate: no product-gap masking, correct GREEN/RED semantics, deploy-tier signatures, and operator steps.
- [x] Add independent reviewer tests or probes for uncovered high-risk paths.
- [x] Run focused Go tests for touched packages and new reviewer regressions.
- [x] Run static checks (`git diff --check`, shell syntax) and selected broader gates if feasible.
- [x] Run or attempt the relevant deploy-tier simcluster drill(s) for force-single/natsconf and small-disk tier-B; record exact outcome or blocker.
- [x] Write the final external review report with Pass/Fail conclusion, doubts, findings, recommendations, and verification.
- [x] Stage all files after the review, per user request.
