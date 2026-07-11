# Simcluster Coverage Roadmap External Review Tasklist

Scope: external review of the current unstaged/untracked roadmap stage. The product artifact under
review is `docs/reviews/simcluster-coverage-roadmap.md`; the internal review is an index of claims,
not trusted evidence. This is a roadmap-only stage: review may add review artifacts and run
independent checks, but must not implement S1-S9 or product fixes.

## Review boundary and governing contracts

- [x] Read `CLAUDE.md`, identify the roadmap-only phase boundary, external-review role, test
  discipline, simcluster mandate, and staging requirement.
- [x] Rebuild the boundary from `git status`, unstaged/cached diff stats, and all untracked files.
- [x] Rough-read the roadmap structure, its internal review, relevant simcluster plans/reviews, and
  prior external-review report/tasklist style before detailed review.
- [x] Read the roadmap in full and read the authoritative architecture, user, broker, cluster,
  runbook, device/server, gotcha, and simcluster harness documentation relevant to every claim.
- [x] Confirm that the roadmap is the sole product/planning deliverable for this special stage and
  does not silently authorize implementation, product fixes, or a competing requirements source.

## Scope and source-of-truth completeness

- [x] Reconstruct the current shipped command tree from the executable/source (`--help`, including
  hidden/debug commands) and diff it against roadmap §4; check flags that materially create distinct
  deploy-tier behavior.
- [x] Cross-check `usage.md`, `broker-ops.md`, `cluster.md`, `cluster-runbook.md`, architecture
  milestone/leaf registers, B/C/D/G plans, and known gotcha ledgers for omitted user/operator journeys.
- [x] Verify every `NOT-COVERED` decision has a technically accurate reason, an explicit owner or
  future trigger where appropriate, and does not misuse the hermetic/deploy-tier boundary.
- [x] Verify every §4 mapping resolves to a concrete §3 drill assertion (not merely a command smoke
  invocation), and every §3 drill is represented in §4 without contradictory ownership.
- [x] Check that the roadmap's stated totals (existing/new/overall drills, N>=2 drills, batches,
  dependencies, dates/releases) are internally and factually consistent.

## Mandate fidelity and simulation realism

- [x] Audit every proposed harness change against Mandate ①-④: provisioning vs product action,
  no workaround laundering, no environment distortion, explicit GAP behavior, and faithful
  install.sh paths/ownership/systemd units.
- [x] Verify topology assumptions for N=1/N=2/N=3, agent/ctl/laptop/artifact/webhook roles,
  auth_callout, route mTLS, tunnel addressing, Caddy non-goal, user services, private-destination
  policy, PTY, and remote-filesystem spike.
- [x] Inspect current `test/simcluster` code and existing seven drills to validate claimed verbs,
  fixtures, coverage, gaps, numbering, assertion primitives, cleanup/isolation, and auto-discovery.
- [x] Read the dedicated sim-cluster server information and decide whether any live read-only check
  or existing drill is evidentially necessary; do not run expensive drills merely to test a roadmap.

## Per-batch executability and false-green resistance

- [x] S1: validate ctl/user journey, PTY/process lifecycle, transfer policy/error boundaries,
  history assertions, G.3 meaning, and remote-fs feasibility/exit routing.
- [x] S2: validate cross-session/auth_callout isolation, PIN/admin/session teardown semantics,
  invite/config refresh/doctor/seeds flows, user-service feasibility, and credential residuals.
- [x] S3: validate expose allocation/data flow/state persistence, explicit remote ports, rehome,
  failover, tunnel provisioning, and positive/negative endpoint proofs.
- [x] S4: validate proxy subscription token/PSK lifecycle, SS data path, private-destination split,
  HA policy/rehome/rebalance, G7a debt, and no-Caddy boundaries.
- [x] S5: validate agent and broker upgrade reachability, URL allowlist/TLS/artifact assumptions,
  `syscall.Exec` PID semantics, cluster upgrade lock/fencing/skew/failure recovery, and install lifecycle.
- [x] S6: validate drain/retire/ops/reconcile/shrink/rejoin/live-data migration, online force-single
  safety gates/protection mode/dwell-bounce, interruption-resume, and rollback proofs.
- [x] S7: validate backup content identity, torn/interrupted restore fail-closed behavior, full-loss
  DR, expose continuity, incident export, key/certificate rotation ordering, and rollback.
- [x] S8: validate G3/G7b debt closure by arm, alert lifecycle/ack/banner semantics, status remote vs
  offline observability, metrics/readiness/webhook behavior, and soft-dependency fallbacks.
- [x] S9: validate G.1/G.2/G.5 reconciliation semantics, clean-exit Restart proof, deterministic
  mid-flight/network-partition/double-fault assertions, soak value, and leak/resource evidence.

## Cross-cutting safety, feasibility, and program control

- [x] Check security coverage for identity isolation, NATS ACL enforcement, invite/token tampering,
  SSRF/private targets, secret leakage, backup/incident-export confidentiality, admin socket access,
  cert/key rotation, replay/revocation, and post-eviction access.
- [x] Check destructive-operation safety: precondition baselines, fail-closed gates, typed confirms,
  single-use arms, quorum/write fences, locks, resumability, rollback, and cleanup after partial failure.
- [x] Check determinism: signature-guarded REDs, positive/negative controls, authoritative data source,
  eventual polling, meaningful exit/status/content assertions, and absence of exact-timing coupling.
- [x] Validate batch dependency graph and ordering, shared-deliverable ownership when reordered,
  cross-batch fixture reuse, soft-dependency degraded exits, and whether any allegedly independent batch
  actually requires an earlier product/harness change.
- [x] Validate runtime/resource claims and `run-drills.sh` scheduling: concurrency hazards, inotify,
  grow flake treatment, retry classification, drill isolation, `-j`/wave strategy, three-run baseline,
  and realistic release-gate cost.
- [x] Validate gotcha/DOC numbering, ledger creation ownership, RED-to-GREEN lifecycle, evidence fields,
  and prevention of findings being lost between S batches and later fix roadmaps.

## Independent verification and closure

- [x] Run document/link/reference consistency checks, exact command/flag searches, shell/Go static
  checks where they validate roadmap claims, and `git diff --check`.
- [x] Record all doubts, unverified environmental claims, non-blocking recommendations, and exact
  verification limits; distinguish product defects from roadmap defects and future implementation risk.
- [x] Write an external review report whose first heading begins with `Pass` or `Fail`, with severity,
  evidence, questions/doubts, recommendations, and verification log following `docs/reviews` practice.
- [x] Re-read the final roadmap/report/tasklist as a coherent roadmap-only stage, update all tasklist
  boxes truthfully, stage every file with `git add -A`, and verify the cached diff contains no accidental
  implementation changes.
