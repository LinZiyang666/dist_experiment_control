# S3–S5 (G-A) external re-review round 4 tasklist

Scope: independently re-review the developer's eight-file follow-up over the fully staged round-3 tree. The
developer's new self-review narrative is evidence to investigate, not an authority. The release threshold remains
the user's instruction: release only when no Major remains. Honest `NOT-COVERED` wording does not by itself remove
a locked acceptance criterion; absence of an explicit owner-approved scope change leaves that release gate open.

## Boundary and prior-finding closure

- [x] Reconstruct the exact eight-file unstaged delta, deleted/untracked state, and effective `HEAD`→worktree
  tree; locate every developer response/claim addressing R3-M1–M5.
- [x] Build a closure matrix for R3-M1–M5. Separate implemented fixes, factual/documentation corrections,
  newly admitted NOT-COVERED gaps, and any requested scope change; do not treat self-downgrade as approval.
- [x] Compare every changed runtime drill, vendored input, and sim image with `weilandserver`; transfer/rebuild
  only what is required and verify exact hashes before execution.

## Drill 73 / R3-M1

- [x] Audit the redesigned non-tunnel selection/construction for leader-vs-tunnel correctness, observable
  proxy-home eligibility, deterministic termination, and avoidance of the round-3 150-second construction RED.
- [x] Audit the new #33 observe-measure oracle for prerequisite validity, anti-vacuity, exact instant being
  observed, exit/process reuse, recovery-latency measurement, command/exit-code handling, and false PASS when
  setup/control/data-plane state is missing for unrelated reasons.
- [x] Audit every new 240-second wait and all destructive-arm gates. Confirm steady-state, pre-kill, rehome,
  post-heal, survivor, and dead-leg baselines fail fast before their corresponding injection and are not merely
  accumulated `assert_ok` failures.
- [x] Re-audit Q 1+1 construction after OFF/ON for actual eligible placement, two live flowing legs, exact
  killed/survivor homes, quorum proof causality, cleanup, and bounded diagnostics.
- [x] Verify #33 claims in drill comments, gotcha ledger, README, and inventory match only measured behavior;
  reject unsupported min/max/eventual/root-cause claims and stale #32/readiness language.

## Drills 71/72/32/74 and R3-M2–M4

- [x] Audit drill 71's replacement outcome accounting and four-sample dwell. Prove failures cannot be silently
  counted as an accepted outcome, journal evidence is attempt-scoped, success is data-plane verified, allocation
  state is causally established, and documentation does not convert an un-attributed race into completed S3.
- [x] Audit drill 72's executable delta and revised honesty boundary. Determine whether persistent in-flight
  revoke and OFF listener/port reclaim were implemented, explicitly owner-rescoped, or remain release-blocking.
- [x] Audit drill 32's manifest changes for numeric uid/gid, symlink lstat metadata, path safety, error
  propagation, type completeness, stable ordering, byte-exact self-test restoration, and false-pass resistance.
- [x] Determine whether real agent/ctl install/never-start and usage §8.4 were implemented or owner-rescoped;
  an unchanged NOT-COVERED warning is not closure by itself.
- [x] Audit drill 74's 60-second executable/document reconciliation and ensure no historical 180-second or
  recovery-through-73 claim remains presented as current evidence.

## Cross-cutting static and documentation review

- [x] Reconcile README, inventory, gotcha ledger, locked plan, and dated external reports for current verdicts,
  assertion counts, timeout values, historical-vs-current labels, event coverage, and release status.
- [x] Review all effective changes for shell portability, word/glob/newline splitting, quoting/expansion,
  exit-code loss, stale variables, subshell state loss, race windows, cleanup, destructive safety, secrets, and
  misleading GREEN/RED/NOT-COVERED semantics.
- [x] Run shebang-aware `sh -n`/`bash -n`, whitespace checks, ShellCheck if available, and focused adversarial
  probes for outcome classifiers, manifest sensitivity/error handling, and the #33 helper.

## Independent runtime verification

- [x] Run every materially changed executable drill needed to decide its claim on fresh `weilandserver`
  instances with automatic retry disabled. At minimum run 71 and 73 twice strict-serial and retain full logs.
- [x] Require drill 73 to complete every arm in at least two clean current-hash runs before considering R3-M1
  closed; any timeout/early die/branch-dependent prerequisite failure is evidence, not an automatic retry flake.
- [x] Scan retained logs for unexpected errors, skipped arms, contradictory outcomes, secrets, and exact live
  assertion counts; compare those facts back to README/inventory.

## Disposition

- [x] Write a round-4 report beginning with Pass or Fail, clearly separating release-blocking Major findings,
  closed findings, non-blocking advice, doubts/questions, exact tests/logs/hashes, and the G-A release decision.
- [x] Complete every task, stage all files with `git add -A`, and verify no unstaged/untracked content remains;
  both cached and uncached whitespace checks must pass. Do not commit or push.
