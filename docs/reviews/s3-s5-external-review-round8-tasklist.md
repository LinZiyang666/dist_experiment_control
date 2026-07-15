# S3–S5 (G-A) external re-review round 8 tasklist

Scope: independently review the developer's seven-file unstaged response over the staged round-7 Fail baseline.
The appended response and claimed concurrent run are hypotheses, not trusted evidence. Prefer reachable production
paths and repeatable failures; do not block on contrived states or require a cosmetic all-GREEN result.

## Boundary and response

- [x] Reconstruct the exact staged-to-worktree delta, untracked/deleted state, response claims, and executable
  inputs; read the locked plan and the prior review sections implicated by R7-M1/M2/M3.
- [x] Build an R7 closure matrix separating code closure, runtime closure, product RED, and documentation accuracy.
- [x] Confirm no acceptance criterion or owner boundary was silently weakened while converting prerequisites into
  gates or conditional branches.

## Drill 74 / R7-M1

- [x] Audit `_construct_111` and reconstruction as a true causal gate: command rc, snapshot validity, exit path,
  cleanup, final verdict, and absence of dependent SKEW/A/B/C claims after failure.
- [x] Audit B moved-exit attribution end to end: stable per-nid snapshot, exact old/new home, same-nid pre-flow,
  real move, post-flow, natural-drift exclusion, and behavior when no exit moves.
- [x] Audit Arm C pre-flow, auto-effect and post-flow ordering; ensure C-dp cannot pass through a non-moved or stale
  exit and that auto failure remains a hard RED rather than an accepted skip.
- [x] Audit the ordinary expose across the entire automatic transaction: valid pre-home/serve baseline, non-target
  placement assumptions, unchanged exact home, continued serving, command failures, and cleanup.
- [x] Probe realistic failure branches (failed reconstruct, no moved nid, failed/late auto, pre/post flow failure)
  for contradictory PASS/NOT-COVERED output or accidental continuation.

## Drill 71 / R7-M2

- [x] Audit Arm E ordering and fixture: both exposes genuinely live, exact rebuild-OFF refusal signature, rc and
  command transport handling, drain cancellation/cleanup, and preservation of the rebuild-ON fixture for B.
- [x] Audit Arm B's command/result split: documented #31 refusal versus success versus unknown failure, and ensure
  neither command nor migration receives a misleading PASS in the blocked path.
- [x] Verify G/F dependency claims and documentation against the locked plan and source; confirm the corrected
  hermetic-coverage statements by searching actual `_test.go` coverage.

## Drill 73 / R7-M3

- [x] Verify Q endpoint cross-check gates every destructive action and every separation/diagnosis claim; on mismatch
  require an honest RED plus no kill, no vacuous black-hole assertion, and no contradictory matched-endpoint text.
- [x] Verify the matched branch still proves target down, leg black-hole, `/sub` availability, separation, and write
  fencing without a stale home or endpoint race crossing the kill boundary.

## Shared documents and verification

- [x] Reconcile README, gotcha ledger, coverage inventory and developer response with executable branch behavior,
  assertion counts, observed-versus-inferred root causes, and current release status.
- [x] Run shell syntax, cached/uncached whitespace checks, ShellCheck if installed, focused static/adversarial probes,
  and relevant hermetic Go tests; retry sandbox/environment failures with a writable cache or exact selector.
- [x] Via tether CLI only, verify local/server input hashes and run fresh isolated relevant drills concurrently;
  retry failed drills concurrently where useful, retaining logs, rc, assertion counts, grow attempts and branches.
- [x] Scan full logs for false continuation, skipped required arms, contradictory diagnostics, infra retries, stale
  inputs and obvious secret material; distinguish stable product RED from harness or topology noise.
- [x] Write a round-8 report beginning with Pass or Fail, including findings, closures, doubts/questions, exact
  tests and runtime evidence, and an explicit release disposition.
- [x] Stage every file with `git add -A`; verify no unstaged/untracked content and both cached/uncached whitespace
  checks. Do not commit or push.
