# Codex All-Green Remediation External Review Tasklist

Date: 2026-07-20
Reviewer role: independent external reviewer
Implementation boundary: the 177 files staged against `fec3bfa` when this review began

The staged implementation spans roughly 22,005 added and 1,104 removed lines. Internal plans,
findings, synthesis documents, and claimed green runs are context only; they are not trusted as
proof. Every release-significant conclusion below must be re-derived from staged source, an
independent test/probe, or preserved deploy-tier evidence.

## Boundary and governing contracts

- [x] Read `CLAUDE.md`; record reviewer authority, implementation/test boundary, language rules,
  phase gates, and the simcluster mandate.
- [x] Freeze the staged boundary with `git status`, `git diff --cached --name-status`, stat, branch,
  and base commit; keep later `claude`-prefixed artifacts outside the evidence set.
- [x] Rough-read the full modification map and group it into CLI, broker/cluster, agent/auth,
  offline recovery, provisioning, simcluster, tests, and documentation surfaces.
- [x] Re-read the requirements baseline, current architecture/distributed-broker invariants,
  cluster runbook, broker operations guide, simcluster mandate, remediation roadmap, and prior
  external-review/tasklist style.
- [x] Reconcile staged claims and command/help/doc changes with the authoritative architecture and
  operator runbooks; flag stale, contradictory, unsafe, or unverifiable guidance.

## Replicated state, Raft, and membership safety

- [x] Audit every new/changed cluster command for deterministic Plan/Apply behavior, stable SQL
  literals, idempotency, monotonicity, correct RowsAffected/no-op semantics, and snapshot/replay
  compatibility.
- [x] Audit grow/upgrade lock leases end to end: atomic acquire+lease, ownership predicates,
  renewal across leadership moves, expiry boundaries and clock skew, old-version/no-lease behavior,
  reaper safety, explicit unlock fail-closed behavior, and keeper shutdown/leak properties.
- [x] Audit the reconcile registry: registration freeze, cadence and leader-only policy, backoff,
  cancellation, panic/error isolation, last-tick visibility, fake-clock determinism, and no duplicate
  or omitted legacy loops.
- [x] Audit each reconciler pass against the one-vote-veto rule: read expected state, compare actual
  state, call an existing idempotent command path, and invent no hidden business policy.
- [x] Audit cluster add/upgrade/retire/drain/force-single/rotation flows for mixed-version behavior,
  partial success, lock release, nonzero-on-nonconvergence semantics, quorum/catch-up fences, and
  safe retry/resume.

## Active convergence and data-plane correctness

- [x] Trace home/roster convergence from replicated write through broker delivery to agent apply;
  verify silent-agent convergence, epoch/generation fencing, retry/backoff, bounded fan-out,
  leadership changes, stale acknowledgements, and restart recovery.
- [x] Verify drain/retire/upgrade cannot report success before required data-plane convergence;
  inspect expose/proxy/transfer interactions and old-home teardown ordering.
- [x] Review agent reconnect, roster replacement, run/exec/expose/proxy handlers, and home-push
  lifecycle for races, goroutine/channel leaks, stale state resurrection, unbounded retry, and
  fail-open behavior during broker silence.

## Security, identity, and denial-of-service resistance

- [x] Audit auth-callout PIN rate limiting: trusted client-IP source, normalization, sharding,
  thresholds, expiry/eviction, concurrency safety, memory bounds, multi-broker behavior, successful
  login reset semantics, and bypass/false-positive cases.
- [x] Audit all new NATS/admin operations for authorization, signed-trigger binding, replay/skew
  protection, actor/session isolation, leader routing, and error/result disclosure.
- [x] Audit secrets/config/install changes for permissions, symlink/TOCTOU hazards, heredoc expansion,
  root-to-service-user privilege boundaries, secret leakage, and upgrade compatibility.
- [x] Audit externally reachable observability/webhook/event payloads for sensitive data exposure,
  stable schema, bounded work, and malformed-input handling.

## Recovery, CLI, and operator safety

- [x] Audit backup/restore/doctor/bundle-scope changes for correct database identity, corrupted or
  missing DB handling, provenance checks, atomicity, partial restore, JetStream loss disclosure,
  config seam application, and fresh-host runbook executability.
- [x] Audit CLI flags, defaults, interactive confirmations, JSON schema, stdout/stderr separation,
  error hints, and exit-code classification; ensure automation cannot receive rc=0 on unknown,
  partial, unreachable, or unconverged state.
- [x] Audit admin events/runtime commands and broker runtime introspection for Unix-socket trust,
  pagination/limits, timeouts, cancellation, resource accounting correctness, and stable output.
- [x] Verify node/broker/agent upgrade version decisions, agentless and colocated cases, rollback,
  settle windows, and help/runbook parity.

## Simcluster truthfulness and test rigor

- [x] Audit all changed drills and shared shell helpers against the mandate: no product workaround,
  no weakened predicate, no hidden manual lifecycle step, correct SETUP/ASSERT/PRODUCT/INCOMPLETE
  classification, and no false GREEN.
- [x] Verify verdict ledger, kept-sites baseline, rollup/rc cross-check, runtime-guard handling,
  timeout/retry logic, parallel isolation, and anti-waiver/anti-dilution gates are fail-closed.
- [x] Inspect each changed/added assertion for non-vacuity, quoting/word-splitting, pipeline/subshell
  status loss, stale logs, empty grep/jq needles, and wrong process/container attribution.
- [x] Run shell syntax and hermetic simcluster harness tests (`tests/run-all.sh` and focused negative
  controls); independently perturb representative oracles where practical.
- [x] Inspect current simcluster server prerequisites/state and run relevant deploy-tier drills for
  any disputed high-risk product/deployment behavior; preserve exact command, instance, logs, verdict,
  and cleanup result.

## Independent regressions and release gates

- [x] Compare new tests with production branches to find assertion-free, same-implementation,
  unreachable, timing-only, or shared-DB false proofs; add reviewer-owned regressions for confirmed
  high-risk gaps without modifying implementation.
- [x] Run `gofmt`/format checks, `git diff --check`, shell syntax, `go vet`, focused package tests,
  static build, and lint; classify environment failures separately from product failures.
- [x] Run affected concurrency/Raft/agent/broker tests under `-race` plus available goroutine/fd leak
  gates; repeat timing-sensitive reviewer regressions.
- [x] Run `make test`, `make e2e`, and `make lint` once as the release hard gate when prerequisites
  permit; do not infer PASS from internally recorded results.
- [x] Check worktree/staging integrity at the end: no implementation edits by the reviewer, no
  accidental inclusion of concurrent `claude` artifacts, and all reviewer-owned files staged.

## Deliverable

- [x] Write `docs/reviews/codex-allgreen-external-review.md` beginning with `Fail` or `Pass`, with
  scope, methods, severity-ranked findings, exact source/test evidence, doubts/questions,
  recommendations, verification results, server/drill evidence, and remaining coverage limits.
- [x] Mark every task above complete or explicitly blocked with a reason; stage the tasklist,
  reviewer tests (if any), and final report.

## Completion record

All review tasks were executed. The resulting release decision is **Fail**. Product counterexamples are
pinned by five reviewer regressions across four files. The full unit/e2e gates were run; their only
new package failures were those counterexamples. Lint, vet, static build, shell syntax, focused race,
and lifecycle/leak checks passed. The hermetic simcluster ledger and kept-site checks failed, while the
verdict-contract test exposed undefined assertion helpers despite returning zero. The current product
was remotely built, drill 80 ran GREEN at N=1 with 44 passes, and automatic instance cleanup was
verified. That deploy-tier run is explicitly coverage-limited: it cannot establish multi-broker PIN
budget behavior, which is instead disproved by the deterministic two-handler regression and production
queue-subscription topology.

No product implementation was changed by the reviewer. Concurrent `claude`-prefixed review artifacts
were not used as evidence. Final details, questions, and remediation requirements are in
`docs/reviews/codex-allgreen-external-review.md`.
