# G5/G7 External Review Round 4 Tasklist

Scope: re-review the developer's round3 fixes over the staged G5/G7 change set. Treat the appended
main-process reply as an index only; verify from code and tests.

- [x] Rebuild the round4 boundary from `git status`, unstaged `git diff --stat`, and cached diff.
- [x] Read the round3 external report and the main-process reply appended to it.
- [x] Verify the round3 blocker fix: responder-to-roster consistency no longer depends on additive
  `IsVoter`, and the pre-G5 responder regression passes.
- [x] Verify no-op stale-lock UX now warns when `--account-seed` is absent.
- [x] Re-check NATS ACL assumptions around cluster-health responder spoofing.
- [x] Run focused cmd/tether, broker, agent, clusterupgrade tests and `git diff --check`.
- [x] Re-check the simcluster access path via local `tether exec` to `weilandserver` and run the
  isolated `00-skeleton` drill as a deploy-tier smoke.
- [x] Write the round4 external review report with a leading Pass/Fail verdict, doubts, recommendations,
  and verification log.
- [x] Stage all files after the review is complete.
