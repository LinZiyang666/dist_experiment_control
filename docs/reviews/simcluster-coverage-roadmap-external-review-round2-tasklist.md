# Simcluster Coverage Roadmap External Review Round 2 Tasklist

Scope: independent re-review of the unstaged rev3 changes relative to the staged rev2 baseline. The
maintainer response appended to the round-1 report is a claim index, not evidence. This remains a
roadmap-only stage: do not implement S0-S9 or product fixes.

## Boundary and response accounting

- [x] Re-read `CLAUDE.md` review/Git/simcluster contracts and reconstruct staged vs unstaged scope.
- [x] Rough-read the complete rev2→rev3 diff and maintainer F1-F11/doubt/recommendation responses.
- [x] Verify that only the roadmap and maintainer response changed before this review, with no hidden
  product, harness, test, generated artifact, or unrelated documentation mutation.
- [x] Build an exact response matrix for F1-F11 and doubts D1-D4: claimed edit, actual roadmap
  location, source/architecture evidence, closure status, and any new risk.

## Load-bearing remediation checks

- [x] F1/D1: prove S0-ingress preserves both loopback-only listeners, is reachable from consumers,
  shares the correct network namespace with the upstream loopback socket, protects token/PSK traffic
  with an explicit TLS/trust model, and does not smuggle a plaintext bridge exposure back in.
- [x] F2: verify every tunnel/data-plane and shared fixture consumer has an accurate S0 dependency;
  validate first-open/reordering ownership without forcing unrelated prerequisites or leaving typed
  confirm, ingress, backup, artifact, layout, or fault primitives ownerless.
- [x] F3/D2: independently inventory current command paths, Hidden bits, deployment-significant flags,
  operator-facing events and persistent alerts; diff them against §3/§4/§4.6 and NOT-COVERED rows.
- [x] F4: validate all three remote-fs arms, watchdog ownership, default-auto semantics, and strict
  separation of real NFS D-state from FUSE approximation.
- [x] F5/D4: validate CONNECT rejection, bidirectional NATS pub/sub ACL, within-session owner errors,
  PIN attempt rate-limit oracle, per-IP controllability, time window, and RED classification.
- [x] F6/D3: validate leader/follower backup semantics, bundle identity oracle, off-cluster repository
  survival and final cleanup, original-secret recovery, total-loss boundary, and foreign/torn/kill
  negative arms.
- [x] F7: verify G3 A/C/D fixture independence, exact broker identities, pre-failure connected-server
  proof, destructive ordering, rebuild/reset point, and existence of the claimed survivor.
- [x] F8: prove the revised orphan state is reachable and stable under actual SQLite/Raft restart
  behavior, simulates a legitimate failure class, preserves the agent process, and asserts the exact
  `killed_orphan` no-RC audit contract without corrupting unrelated state.
- [x] F9: validate PTY control/data paths through the named broker, connected-server observation,
  netns DROP/tc silent-partition fidelity, quorum-side assertions, anti-split-brain oracle, scoped
  cleanup, and parallel-instance isolation.
- [x] F10: validate `/proc` measurements, goroutine observability wording, PID re-resolution, settle
  points, tolerances/slope ownership, restart discontinuities, and background-client cleanup.
- [x] F11: compare the exact branch/PR wording and example branch name against current `CLAUDE.md §6`.

## Cross-cutting rev3 consistency and new-risk review

- [x] Audit S0 as a coherent dependency registry: default/first owner, prerequisites vs consumers,
  lifecycle, secrets, instance namespacing, teardown, parallel safety, and Mandate ①-④ justification.
- [x] Check §3 drill specs, §4 mappings, §4.6 event table, totals (~30 new/~37 overall), topology counts,
  dependency graph, OQ decisions, DOC/gotcha numbering, and acceptance exits for contradictions.
- [x] Verify every new event row names the real emitted contract and has a deterministic trigger plus
  authoritative observation path, rather than treating side effects or generic audit rows as events.
- [x] Recheck destructive five-element discipline on every modified restore/orphan/PTY/partition/soak
  arm; ensure the roadmap itself supplies enough constraints for leaf plans to be safe and non-flaky.
- [x] Review the maintainer response for factual accuracy, overclaims, incorrect file:line evidence,
  unresolved statements, and disagreement with the actual rev3 text.

## Independent verification and closure

- [x] Run exact source/flag/event searches, current command-tree inspection, focused static/Go checks
  where needed, Markdown/diff consistency checks, and record why no live sim drill is or is not needed.
- [x] Record residual doubts and recommendations separately from release-blocking roadmap defects.
- [x] Write a round-2 external review report beginning with `Pass` or `Fail`, with evidence, closure
  matrix, new findings, doubts, recommendations, and verification limits.
- [x] Re-read roadmap/response/report/tasklist as a coherent roadmap-only stage; truthfully close all
  boxes, stage every file with `git add -A`, and verify the cached diff contains only review-stage docs.
