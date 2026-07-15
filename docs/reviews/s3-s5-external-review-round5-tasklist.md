# S3–S5 (G-A) external re-review round 5 tasklist

Scope: independently re-review the developer's twelve-file follow-up over the fully staged round-4 tree. The
developer's “owner-accepted,” “NOW-COVERED,” and prior-run GREEN statements are claims to verify, not authority.
The suite is meant to expose product defects rather than optimize for all-GREEN; the review must distinguish a
correctly exposed/recorded product gap from a harness false pass, an embedded retry that launders evidence, or an
unapproved removal of a locked acceptance criterion. Release only if no Major remains.

## Boundary and authority

- [x] Reconstruct the exact twelve-file unstaged delta, deleted/untracked state, effective staged→worktree tree,
  and every developer response addressing R4-M1–M6.
- [x] Build an R4-M1–M6 closure matrix separating implemented oracle fixes, reclassified product findings,
  explicitly NOT-COVERED items, and alleged owner-approved scope changes.
- [x] Verify every “owner-accepted” statement against actual user/owner authorization in the review history; do
  not infer scope approval from “tests should expose problems” or “release if no Major.”
- [x] Compare changed executable files and shared harness inputs with `weilandserver`, transfer only current
  reviewed hashes, and verify image/vendor/runtime hashes before tests.

## Shared grow/run harness

- [x] Audit `grow_to_3`'s internal nuke+retry for hidden automatic retries, ignored grow return codes, cleanup,
  total deadline, cross-drill effects (especially drill 30's #31 pin), and whether “strict no-retry” claims remain
  factually true.
- [x] Audit the new `SIM_CONCURRENT` propagation and diagnostic split for direct drill invocations, runner retry
  passes, environment leakage, and unsupported “#31 most likely” attribution.
- [x] Audit unused `_ensure_grow_lock_released` / `_clear_lingering_ops` helpers and their documentation for dead
  recovery code, accidental future activation, and contradictions with the chosen retry policy.

## Drill 73 / R4-M1

- [x] Verify `ss_up` listener/process readiness is robust to nonexistent executable, instant crash, stale process,
  wrong port, IPv4/IPv6 formatting, PID-pattern ambiguity, and delayed death after the readiness sample.
- [x] Audit `_ss_leg_try`, per-port cleanup, re-fetch semantics, secret handling, and whether reusing `$TOK_A`
  after off/on/revoke can address the intended current allocation.
- [x] Audit the new #33 kill→black-hole→control→fresh-flow trace for exact timestamps, anti-vacuity, local-client
  causality, AUTO-RECOVERED/STRANDED classification, readiness flaps, fixed deadlines, and whether an always-true
  terminal assertion actually pins/exposes any defect.
- [x] Audit Q construction and the new “skip-not-die” branch. Determine whether failed required baselines still
  make the drill RED, whether every GREEN reaches the locked quorum separation, and whether documentation may
  claim 40 assertions/coverage when the arm was skipped.
- [x] Independently reproduce at least two strict-serial current-hash drill-73 executions with runner retry off;
  record internal grow attempts separately so embedded setup retry is not mislabeled strict evidence.

## Drills 71/72/32/74 and R4-M2–M5

- [x] Audit drill 71's reframed #29 against source and locked plan: exact crash target, leader assumptions,
  allocation home/epoch oracle, all-live-voter curl semantics, return recovery, rebuild-off semantics, cleanup,
  and whether retrying expose creation masks the claimed tunnel-coupling problem.
- [x] Verify the claimed owner-approved removal of drain-migrate/stickiness/rebuild-off-drain/event arms; assess
  whether “the defect blocks its fixture” is demonstrated or merely avoids a required scenario.
- [x] Audit drill 72's held-open stream probe for actual established byte progress (not merely absence of an exit
  marker), slow-sink readiness, early/natural exit discrimination, bob continuity, cleanup, and false pass on a
  connection that never established or stalled for unrelated reasons.
- [x] Audit drill 72's listener reclamation for correct broker/port, listener ownership, exact socket matching,
  allocation-row/port reuse coverage, and false matches/misses.
- [x] Audit drill 32's manifest fail-closed semantics for empty roots, partial find failure, pipeline status,
  stat/readlink/hash errors, newline/path safety, deterministic output, and truly byte+metadata-exact restoration.
- [x] Audit the real agent artifact install for TLS/trust, checksum file correctness, source/version naming,
  binary provenance, never-start, uninstall, cleanup/trap composition, and whether real ctl plus §8.4 remain locked
  or were formally rescoped.
- [x] Audit drill 74's single-snapshot counts, empty/invalid snapshot behavior, 180-second auto budget, exact event
  and per-exit data-plane requirements, ordinary-expose negative control, and delegation to drill 73.

## Static, documentation, and adversarial verification

- [x] Reconcile README, inventory, gotcha ledger, locked plan, round-4 report, and executable behavior for current
  verdicts, counts, strict-run meaning, #29/#33 claims, NOT-COVERED scope, and historical-vs-current facts.
- [x] Review every effective shell change for quoting/splitting, rc loss, stale/global variables, subshell state,
  process/PID races, temporary-file collisions, cleanup, timeouts, destructive safety, and secret leakage.
- [x] Run shebang-aware syntax checks, cached/uncached whitespace checks, ShellCheck if available, and focused
  adversarial probes for `ss_up`, held-stream readiness, manifest partial failure, atomic distribution snapshots,
  #33 classification, and internal grow retry reporting.

## Independent runtime verification

- [x] Run materially changed drills 32, 71, 72, 73, and 74 on fresh `weilandserver` instances with runner-level
  retry disabled; retain complete current-hash logs and count any internal setup retries explicitly.
- [x] Scan logs for skipped required arms, baseline failures, contradictory GREEN/NOT-COVERED outcomes, secrets,
  assertion counts, and evidence that held streams transferred bytes before/during revoke.
- [x] Compare live facts with all “GREEN,” “2× strict,” “NOW-COVERED,” and owner-accepted claims; do not inherit
  developer logs or conclusions without current reproduction.

## Disposition

- [x] Write a round-5 report beginning with Pass or Fail, separating release-blocking Major findings, correctly
  exposed product gaps, verified closures, non-blocking advice, doubts/questions, exact hashes/tests/logs, and the
  release decision.
- [x] Complete every task, stage all files with `git add -A`, and verify no unstaged/untracked content remains;
  cached and uncached whitespace checks must pass. Do not commit or push.
