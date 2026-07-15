# S3–S5 (G-A) external re-review round 2 tasklist

Scope: independently re-review the developer response and unstaged fixes layered over the staged S3–S5
change set.  The response and its claimed server runs are leads, not trusted evidence.  The effective review
target is `HEAD` through the current worktree, with special attention to whether the first external review's
seven Major findings and one Minor finding are actually closed without weakening or vacating the oracles.

## Boundary and evidence

- [x] Reconstruct staged, unstaged, deleted, and untracked scope; read the developer response and all affected
  roadmap/inventory/README/gotcha claims against the executable drills.
- [x] Reconcile every original M1–M7 and m1 closure claim to exact code and identify any newly introduced
  behavior, coverage claim, timeout, inverted assertion, or destructive transition.
- [x] Inspect retained/local sim-cluster configuration and independently rebuild the exact binaries/image when
  the effective runtime inputs changed or cannot be proven identical.

## Static and adversarial review

- [x] M1/M7: audit drill 73 topology construction and every destructive precondition for non-empty targets,
  correct home selection, mandatory independent survivor control, and absence of vacuous shell success.
- [x] M2: audit drill 71's deliverability gate and precise journal signature, including cursor/time scoping,
  stale-log resistance, probe causality, fixed-tunnel proof, and post-probe healthy control.
- [x] M3: audit drill 72's revoke/off data-plane matrix, HTTP status gates, per-sub key isolation, socket/process
  lifecycle, and logs/artifacts for token, PSK, password, or rendered-config disclosure.
- [x] M4: audit drill 31's fleet enumeration, OFFLINE exclusion, dispatch evidence, timeout semantics, exit status,
  summary oracle, and the honesty of success-path NOT-COVERED claims.
- [x] M5: audit drill 32's content+metadata manifest for regular files, directories, symlinks, ownership/mode,
  special files, deterministic ordering, error propagation, real-user execution, all three roles, uninstall,
  and §8.4 claims.
- [x] M6: audit drill 74's returned-voter eligibility proof, quiet window, dry-run mutation assumptions,
  per-run auto-rebalance accounting, and consistency of README/inventory/gotcha claims.
- [x] Audit new gotcha #32 and drill 73's inverted recovery assertion: independently verify the product defect,
  define a non-vacuous latency boundary, ensure eventual recovery is not confused with permanent failure, and
  ensure the off/on heal does not erase the state required by the quorum-separation claim.
- [x] Review all effective scripts/libraries for shell quoting, pipeline/exit-code mistakes, race windows,
  cleanup/idempotence, fixed-node assumptions, destructive safety, secret hygiene, and cross-drill interference.

## Independent verification

- [x] Run syntax/static hygiene (`sh -n`/`bash -n`, ShellCheck if available, `git diff --check`) plus focused
  adversarial local probes for any oracle that can falsely pass without a live cluster.
- [x] Run relevant focused Go tests and the shared sim-cluster regression set (61/62/80/82) without trusting
  the developer's reported outcomes.
- [x] Run all eight G-A drills on the required N=1/N=3 topologies; run 71/73/74 in at least two isolated fresh
  clusters each, retain logs, and investigate every retry, timeout, branch-dependent count, or unexpected GREEN.

## Disposition

- [x] Write a round-2 report beginning with Pass or Fail, with evidence, doubts, findings, suggestions, coverage
  corrections, and an explicit release recommendation consistent with prior `docs/reviews` practice.
- [x] Mark every task above complete, stage all files with `git add -A`, and verify no unstaged/untracked content
  remains and both cached/uncached whitespace checks are clean.  Do not commit or push.

## Completion notes

- Effective drill/vendor hashes on `weilandserver` matched the local tree exactly; the developer follow-up
  touched no binary or image input, so an additional rebuild would not change the runtime artifact.
- ShellCheck was not installed.  Shebang-aware `sh -n`/`bash -n`, both whitespace checks, focused Go tests,
  and three adversarial oracle probes were run instead.
- Server logs are retained under `/tmp/s3s5-external-r2/{base,solo1,solo2}`.  Automatic retry was disabled.
  Two strict-serial fresh-cluster rounds were run for 71/73/74; 73 was GREEN once and RED once, in addition to
  a separate valid-N=3 Q-construct RED in the base matrix.
- Final disposition and exact evidence are in `s3-s5-external-review-round2.md`.  No product or drill fix was
  made by the external reviewer.
