# S3–S5 (G-A) external re-review round 9 tasklist

Scope: independently review the developer's five-file unstaged response over the staged round-8 Fail baseline.
The response admits the prior oracle/control-flow defects and claims stricter gates; verify executable behavior
without trusting its prose. Correct product RED remains evidence, not a reason to weaken or skip locked criteria.

## Boundary and closure matrix

- [x] Reconstruct the exact staged-to-worktree delta, response claims, deleted/untracked state, executable inputs,
  and the locked plan requirements implicated by R8-M1/M2/M3.
- [x] Build an R8 closure matrix separating causal gate closure, exact identity closure, runtime branch coverage,
  product RED, and documentation accuracy.
- [x] Check that new NOT-COVERED branches preserve the prerequisite RED and do not silently convert a required
  behavior into an accepted skip or early GREEN.

## Drill 74 SKEW and B

- [x] Verify reconstruction plus every exact per-nid SKEW flow baseline is adjacent, complete, validated, and gates
  all destructive SKEW/RETURN/A/B/C paths on failure with a correct final rc and cleanup.
- [x] Audit B pre-flow aggregation and branch scope: all expected distinct nids, validated fresh home snapshot,
  dry-run/real command ordering, and no injection when any pre-flow or snapshot prerequisite fails.
- [x] Audit exact manual-move derivation: nonempty DP_A, unique old/new nid mapping, old!=KTGT, new==KTGT, no natural
  drift ambiguity, and the post-move test must invoke `_ss_via_agent "$DP_A"` rather than a dynamic home selector.
- [x] Audit ordinary-expose control gates: create rc, valid home, pre-serving, exact unchanged home, post-serving,
  failed partial-row cleanup, and whether a failed control prevents dependent Arm C claims where required.

## Drill 74 Arm C

- [x] Verify C setup/env, positive-home target, exact pre-flow nid, kill/rehome-away, return, still-zero-before-auto,
  and auto-before snapshot form one fail-closed `C_EDGE` gate with no vacuous zero-home kill.
- [x] Verify auto effect cannot pass from an already-even or invalid snapshot and does not invoke any manual verb;
  inspect how non-terminal ops are diagnosed versus asserted root cause.
- [x] Audit exact auto-move derivation across before/after validated per-nid snapshots, require a real move onto KTGT,
  and prove post-auto bytes through the derived nid with `_ss_via_agent`.
- [x] Verify the ordinary expose genuinely spans the automatic transaction only when its pre-control was valid, and
  that cleanup/default-env restoration is correct across every early gate and failure branch.
- [x] Run realistic branch probes for failed SKEW-flow, B pre-flow, partial neg-control, zero-home C target, failed
  C return/still-skew, no auto move, ambiguous/missing nid and successful exact move.

## Drill 71 and documentation

- [x] Verify both post-return recoveries gate Arm E; exact `assert_refuses` requires rc!=0 plus the precise refusal
  text, preserves `_AS_RC/_AS_OUT` diagnostics, aborts any partial drain, and leaves the B fixture valid.
- [x] Audit interaction with global assertion capture state: any diagnostic/cleanup after `assert_refuses` must not
  overwrite evidence or accidentally change the refusal verdict.
- [x] Verify B command/migration behavior is unchanged and remains meaningful after E success, #31 interception,
  E skip, and abort cleanup.
- [x] Reconcile README, gotcha ledger, drill warning and D7 source for rebuild-OFF refusal versus the uniquely
  deploy-tier rebuild-ON migration data plane; reject any new overclaim.

## Verification and report

- [x] Run shell syntax, cached/uncached whitespace checks, ShellCheck if installed, focused branch/adversarial probes,
  and relevant D7/broker hermetic tests; immediately retry environment-only failures with exact selectors/caches.
- [x] Via tether CLI only, verify local/server hashes and run fresh isolated 71/74 plus relevant 73 regression
  concurrently; retry failed/noisy branches concurrently where useful and retain logs, rc, counts and grow attempts.
- [x] Scan complete logs for continued execution after failed prerequisites, false PASS/NOT-COVERED attribution,
  missing cleanup, stale inputs, infra retry visibility, contradictory diagnostics and obvious secret material.
- [x] Write a round-9 report beginning with Pass or Fail, including closure matrix, findings, doubts/questions,
  exact local/runtime evidence and explicit release disposition.
- [x] Stage every file with `git add -A`; verify no unstaged/untracked content and both cached/uncached whitespace
  checks. Do not commit or push.
