# S3–S5 (G-A) external re-review round 6 tasklist

Scope: independently re-review the developer's thirteen-file unstaged response plus the new
`s3-s5-owner-decisions.md` over the fully staged round-5 tree. Developer replies, self-reported runs, empirical
probe summaries, and newly authored “owner decisions” are claims to verify, not independent authority. Tests are
for exposing defects; RED/NOT-COVERED is acceptable when causally sound, while false passes, false attribution,
unapproved acceptance changes, and evidence-laundering remain release blockers.

## Boundary and authority

- [x] Reconstruct the exact staged-to-worktree delta, untracked/deleted state, executable/image inputs, and the
  complete developer response to R5-M1–M7 and questions 1–7.
- [x] Build an R5-M1–M7 closure matrix distinguishing executable fixes, documentation-only changes, correctly
  exposed product gaps, residual NOT-COVERED items, and new regressions.
- [x] Audit `s3-s5-owner-decisions.md` provenance and scope: identify whether it records an actual user/owner
  decision or is a developer-authored unilateral rescope; compare every decision with the locked plan and review
  history.
- [x] Verify all local/server executable and image hashes using tether CLI only; do not count inherited or
  developer-retained logs as independent runtime evidence.

## R5-M1 / drill 72

- [x] Audit ThreadingHTTPServer readiness, held-curl PID/rc handling, output-file isolation, monotonic byte-growth
  predicates, alice freeze/bob growth timing, natural completion, cleanup, and failure propagation.
- [x] Audit OFF listener ownership, SQLite allocation query provenance/locking/schema, fail-closed rc handling, and
  controlled same-port reuse for causal attribution and cleanup.
- [x] Adversarially test no process/no bytes, stalled connection, early curl failure, single-stream-only, stale
  byte files, SQLite failure/empty output, unrelated listener, and reuse command failure.

## R5-M2 / drill 74

- [x] Audit fail-closed snapshots for command/JQ rc, expected row count and unique nids, allowed/nonempty homes,
  sentinel uniqueness, arithmetic safety, subshell/global state, and invalid-snapshot behavior in every consumer.
- [x] Verify the constructed 1/1/1 baseline is hard-gated, every initial exit has a real flowing SS leg, natural
  distribution remains honestly reported, and setup commands cannot fail while later assertions continue.
- [x] Audit B per-exit data-plane measurement and the ordinary-expose negative control: exact moved exit/home,
  endpoint cross-check, baseline/injection/recovery causality, AUTO-SERVED/STRANDED terminal semantics, and whether
  accepting both still satisfies or merely records the locked criterion.
- [x] Verify event count/anti-flap and Arm-C auto semantics against the locked plan and any legitimately approved
  scope decision.

## R5-M3 / drill 71

- [x] Recheck the revised #29 explanation against `homeForExpose`, `TunnelExposeAdapter`, allocation forwarding,
  token lookup, current empirical probes, and prior successful home!=tunnel observations.
- [x] Audit the new drain-migrate/rebuild-off/stickiness arms for leader/target guards, exact home and endpoint
  cross-checks, pre-injection bytes, drain command rc/signature, epoch/moved state, return behavior, rebuild-off
  refusal, cleanup, and fail-fast propagation.
- [x] Determine whether any NOT-COVERED downgrade is a correctly exposed product constructibility defect or an
  avoidable fixture limitation; reject unapproved developer-authored rescope.

## R5-M4 / drill 32

- [x] Audit manifest traversal fail-closed implementation for partial/missing roots, pipeline rc, newline/path
  handling, deterministic output, stat/hash/readlink failure, and byte+metadata restoration.
- [x] Audit composed traps and real broker/agent/ctl install/uninstall/never-start paths for role-specific identity,
  artifact integrity, file ownership/modes, cleanup, and false success.
- [x] Audit §8.4 stop→swap→SHA/integrity→start→business convergence: virgin/live state, exact binary provenance,
  SQLite integrity command/rc, service transition, tier-B plus session/expose persistence, and rollback/cleanup.

## R5-M5 / shared grow harness

- [x] Verify internal nuke/retry removal or isolation, every grow rc, timeout and failure-preservation path, drill-30
  #31 exposure, and exact meaning of runner `--no-retry` / “strict.”
- [x] Audit removal/use of `_ensure_grow_lock_released`, `_clear_lingering_ops`, SIM_CONCURRENT diagnostics, and dead
  code/documentation consistency.

## R5-M6 / drill 73

- [x] Audit exact `/sub` host/port ↔ recorded home cross-checks before Q injection, broker-death proof, black-hole
  hard gate, subsequent separation assertion dependencies, and stale/wrong endpoint diagnosis.
- [x] Recheck #33 AUTO-RECOVERED/STRANDED semantics, timestamps, readiness flaps, and whether documents accurately
  call it a recorder rather than a defect pin.
- [x] Reproduce drill 73 at least twice on current hashes with runner retry disabled, recording fixture attempts,
  endpoint/home facts, all Q branches, and any RED as evidence rather than retry noise.

## R5-M7 / documentation and owner decisions

- [x] Reconcile gotcha ledger, README, inventory, locked plan, owner-decisions file, appended round-5 response, and
  executable behavior for #29/#33, counts, GREEN/RED, strict runs, NOT-COVERED and current-vs-historical facts.
- [x] Check that empirical probes cited in prose are reproducible or backed by retained evidence and do not replace
  executable acceptance coverage.

## Static, adversarial, runtime, and disposition

- [x] Review every effective shell/Dockerfile change for quoting/splitting, rc loss, stale files/globals, subshell
  state, PID/process races, SQLite concurrency, timeouts, cleanup, destructive safety, and secret leakage.
- [x] Run shebang-aware syntax, cached/uncached whitespace checks, Dockerfile-relevant checks, ShellCheck if
  available, and focused adversarial probes for every new oracle/fail-closed path.
- [x] Attempt materially changed drills 32, 71, 72, 73, and 74 on fresh isolated `weilandserver` instances via
  tether CLI only with runner retry disabled. BLOCKED before execution: the local proxy socket required sandbox
  network approval and the escalation failed when the approval-service stream disconnected. No workaround and no
  independent server result claimed; the developer's retained current-response result was used only as an adverse
  admission (`74 GREEN 28` while B-dp STRANDED and Arm-C NOT-COVERED), not as independent verification.
- [x] Scan all retained evidence available in the tree for skipped required arms, contradictory
  PASS/RED/NOT-COVERED lines, assertion counts, internal retries, stale evidence, and secrets. Developer did not
  retain the six cited empirical probe scripts or complete server logs, so their claims remain unverified.
- [x] Write a round-6 report beginning with Pass or Fail, with Major findings, correctly exposed gaps, verified
  closures, doubts/questions, exact tests/hashes/logs, and release decision.
- [x] Complete every task, stage all files with `git add -A`, verify no unstaged/untracked content and both cached
  and uncached whitespace checks; do not commit or push.
