# G1-G7 Audit External Review Round 2 Tasklist

Scope: re-review the developer response to `docs/reviews/g1-g7-audit-external-review.md`, especially F1 and the residual-risk follow-ups.

- [x] Rebuild the current unstaged/staged boundary from `git status`, `git diff --stat`, and cached diff.
- [x] Read the developer reply appended to the round-1 external review.
- [x] Re-check F1 implementation: failed `confirm-op` must not spend budget or arm the BLOCKED edge, and the next BLOCKED poll must retry.
- [x] Re-check F1 regression tests, including the live NATS retry case.
- [x] Re-check the C3 sentinel-only backup-path adjustment.
- [x] Run focused cmd/tether, broker, and clusteroffline tests plus `git diff --check`.
- [x] Run `make test`, `make lint`, and `go test -race ./internal/broker`.
- [x] Run deploy-tier simcluster `10-grow-to-3` against the current binary.
- [x] Write the round-2 external review report with leading Pass/Fail verdict, doubts, recommendations, and verification log.
- [x] Stage all files after the review is complete.
