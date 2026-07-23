# R16 + G67 + G69 external re-review tasklist

Date: 2026-07-23
Reviewer role: independent external reviewer
Base: `b602fc7`
Prior verdict: `Fail` (`docs/reviews/r16-g67-g69-external-review.md`)
Initial re-review boundary: 9 unstaged developer-modified files, approximately `+149/-33`,
layered on top of the previously staged 79-file candidate/review index.

Developer changed the worktree again during the re-review. The third-round re-review boundary is the
effective `HEAD + index + second response`, whose second response touched 14 product/test/report
files (approximately `+437/-39`) before reviewer-owned follow-up tests. Results from processes
started before that change are explicitly classified as intermediate evidence and are not used
to certify the final candidate.

The re-review accepts neither comments naming the prior findings nor new green tests as proof.
Each prior finding is re-derived against the effective worktree (`HEAD + index + unstaged
developer response`) and, where applicable, against an independent counterexample.

## Boundary and response mapping

- [x] Freeze the post-review Git boundary and distinguish the 9 developer-response files from the
  previously staged candidate and reviewer evidence.
- [x] Read the complete developer response diff before running tests.
- [x] Map every changed hunk to F1–F9; identify findings not addressed, deliberately deferred,
  weakened in a drill, or contradicted by docs/comments.
- [x] Inspect the effective source, not only the unstaged response diff, for cross-file invariants
  and unchanged call sites.
- [x] Detect the concurrent second response, re-freeze its 14-file delta, and re-run prior
  conclusions instead of trusting tests from the earlier snapshot.
- [x] Receive the developer's appended conclusion as a third-round input and compare every claimed
  fix with the actual diff; record the Drill 96/expected-verdict claims that were not implemented.

## F1: terminal audit commit-before-ledger-delete

- [x] Trace watchdog, `ev.transfer`, prepare cleanup, and pull finalize through
  `emitTerminalTransferAudit`, async retry/give-up, shutdown-draining, and single-mode fallback.
- [x] Verify callback fires exactly once and only after durable Raft commit; cover marshal failure,
  permanent forward failure, repeated not-leader, panic/process exit, and callback/remove failure.
- [x] Audit both crash windows: before commit must retain recovery evidence; after commit but before
  ledger unlink must not synthesize a contradictory failed terminal on restart.
- [x] Verify object/tracker removal before audit commit cannot destroy fields or evidence needed for
  recovery and cannot leak live objects indefinitely.
- [x] Re-run the prior reviewer counterexample and add/adjust independent tests for any remaining
  at-least-once/exactly-once terminal gap.
- [x] Exercise a real cluster DB lookup failure and a deterministic terminal-stage filesystem
  failure; verify unknown commit state and failed staging cannot delete/overwrite recovery evidence.

## F2: cross-home GC safe floor

- [x] Verify production YAML rejects every value below the derived safe floor, accepts unset and
  safe raises, and cannot bypass the bound through flags, direct parsing, negative/overflow values,
  aliases, or alternate config paths.
- [x] Verify the duplicated serveconf constant cannot drift from broker timeout/default and that
  public docs no longer advertise unsafe compression.
- [x] Inspect drill 96 classification/non-vacuity after removal of the 5s seam; ensure it neither
  pre-books a gap before constructibility nor continues to claim observation of a reap that cannot
  occur.
- [x] Re-run the prior F2 counterexample and focused GC/config tests.

## F3–F9 regression and disposition

- [x] F3: re-run stale/offline JS assignment counterexample and decide whether the current
  placement contract remains falsely satisfied.
- [x] F4: re-run JS store root-error and symlink counterexamples.
- [x] F5: re-run stale-sentinel and backup-name collision counterexamples.
- [x] F6: re-check ledger fsync, required-field validation, corrupt-file collision, and observability.
- [x] F7: re-run drill-41 false-oracle shell check.
- [x] F8: verify whether stable transient code 10023 is now structurally classified.
- [x] F9: run `git diff --check` on the effective candidate.

## Regression, concurrency, and deploy-tier evidence

- [x] Run all reviewer-owned Go and shell tests and classify every failure.
- [x] Run focused package tests and affected-package `-race`; include audit-forward shutdown and
  recovery finalization paths.
- [x] Run `go vet`, lint, candidate test gate, and tagged E2E; distinguish deliberate review reds
  from unrelated regressions.
- [x] Run simcluster hermetic tests and the smallest deploy-tier drill set needed for changed
  behavior; record that Drill 96 ran on the intermediate image but its unchanged oracle judges
  #57 before recovery and cannot certify the final image.
- [x] Verify final simcluster cleanup and no persistent nodes.

## Deliverable and staging

- [x] Write `docs/reviews/r16-g67-g69-external-rereview-round3.md` beginning with `Fail` or `Pass`, with
  prior-finding disposition, new findings, doubts, recommendations, and exact test evidence.
- [x] Complete every task or record a concrete blocker/coverage limit.
- [x] Stage all files, including developer changes and re-review artifacts, and verify no unstaged
  or untracked files remain.

## Final evidence summary

- Unfiltered `make test`: FAIL only on the two new F1 reviewer counterexamples.
- Filtered `make test`: PASS.
- Affected-package race, vet, lint, simcluster hermetic, and final tagged E2E: PASS.
- Drill 96: `PRODUCT-RED rc=3 pass=38 product_red=1 not_covered=6`, but the #57 oracle
  fires before the recovery finalizer can run; F arm was not covered due cross-arm residue.
- Final sim server status: no nodes or containers.
