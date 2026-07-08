# G5/G7 External Review Round 3 Tasklist

Scope: re-review the developer's round2 fixes over the already-staged G5/G7 change set. Treat the
main-process reply as a map only; verify behavior from code and tests.

- [x] Re-read the project/review workflow context and rebuild the unstaged round3 delta with `git status`
  and `git diff --stat`.
- [x] Re-read the round2 external report and the main-process reply appended to it.
- [x] Verify B1: release-lock failure reporting and no-op stale-lock recovery.
- [x] Verify B2: lock acquisition rejects existing membership ops and freezes operation driving while held.
- [x] Verify B3: signed-roster verification and stale manifest fail-closed behavior, including mixed-version
  brokers that do not report `IsVoter`.
- [x] Verify M1: socket status aggregates peer `JetStreamUnavailable`.
- [x] Run focused tests, including the previous external regressions and any new reviewer test needed for
  residual risks.
- [x] Write the round3 external review report with a leading Pass/Fail verdict, doubts, findings,
  recommendations, and verification log.
- [x] Stage all files after the review is complete.
