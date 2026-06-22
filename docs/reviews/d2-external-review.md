# Pass — D2 external review

## Tasklist

- [x] Scope census: confirmed every unstaged tracked and untracked file, split docs / implementation / tests.
- [x] Requirements/architecture alignment: checked D2 plan/docs against the distributed-broker baseline, especially ops-only, Plan/Apply determinism, LitTime, op-set boundaries, and no线上 cutover.
- [x] SQL literal surface: audited `sqlbake` for byte equivalence, JSON/log safety, injection behavior, and test coverage.
- [x] Op renderers: audited `port`, `proc`, `node`, `session`, and `agentprov` Plan functions for parity with live mutators, all-literal commands, business-error prechecks, and secret non-replication.
- [x] Cluster apply surface: audited `Command`, `knownOps`, `defaultAppliers`, `ExecCommand`, and applied-index behavior for D1 regressions and D2 op coverage.
- [x] Reconcile extraction: checked `resolveReconcileMarks`, batch ordering, and equivalence against live G.1 behavior.
- [x] Determinism and equivalence tests: inspected multi-FSM/differential tests, CHA reachability lint, liveness-column assertions, and non-vacuity controls.
- [x] E2E wiring and dependency drift: inspected `TestD2Matrix`, `go.mod/go.sum`, and Go directive/dependency implications.
- [x] Independent verification: ran focused tests and added reviewer-owned regression tests.
- [x] Main-process reply: reviewed the F1 fix, re-audited the corrected architecture text, and reran the D2 gates.
- [x] Report and staging: report written; all files staged after final verification.

## Findings

No remaining blocker / major findings after the main-process fix.

### Resolved F1 — stale `LitTime=t.UTC().String()` architecture contract【BLOCKER → FIXED】

Initial external review failed because `docs/distributed-broker-architecture.md` still had two stale references to `LitTime=t.UTC().String()` even though D2's real contract is `LitTime(t)=t.String()` and callers decide whether to pass raw `now` or `now.UTC()`.

Main-process response accepted the finding and fixed both stale architecture references:
- §5 D2 amendment now says `LitTime(=t.String())`, not forced UTC, and documents the caller split.
- §19-D2 status now says implementation is complete / internally reviewed and repeats the corrected `LitTime=t.String()` contract.

Reviewer recheck:
- `rg -n 'UTC\(\)\.String\(\)|t\.UTC\(\)\.String\(\)|LitTime=t\.UTC|实现进行中' docs/distributed-broker-architecture.md` returns no matches.
- The fixed wording now matches `internal/cluster/sqlbake.go`, `internal/session/plan.go`, and `docs/reviews/d2-plan.md`.

## Reviewer Tests Added

Added `test/cluster/d2_command_shape_review_test.go`.

This asserts every real D2 Plan command (`SessionCreate`, `MemberJoin`, `NodeRegister`, `NodeEvict`, `ProcCreate`, `ProcMarkExited`, `ReconcileBatch`, `PortAllocate`, `PortFree`, `PortRevoke`, `AgentProvision`, `SessionTombstone`, `SessionHardDelete`) has non-empty SQL and no `Statement.Args`. It directly guards the D2 "all leader-baked SQL literals, Args only for inert D1 ClusterMetaSet" contract.

Added `test/cluster/d2_error_parity_review_test.go`.

This closes the coverage gap I found during the stricter re-review: `docs/reviews/d2-review.md` claimed broad typed-error parity coverage, while the existing `TestPlanError_Parity` only exercised a subset. The reviewer test compares live mutators vs Plan paths for tombstone `ErrDeleting`, member join missing/deleting/invalid PIN, agent provisioning missing/deleting/invalid PIN/already-provisioned, and port exhaustion. All pass, so this was a test/report coverage gap rather than an implementation bug.

## Verification

Passing:
- `env GOCACHE=/tmp/tether-gocache go test ./test/cluster -run TestD2RealOpsDoNotUseStatementArgs_Review -count=1 -v`
- `env GOCACHE=/tmp/tether-gocache go test ./test/cluster -run TestD2PlanErrorParity_Review -count=1 -v`
- `env GOCACHE=/tmp/tether-gocache go test ./test/cluster -count=1`
- `env GOCACHE=/tmp/tether-gocache go test -tags e2e_matrix ./test/e2e -run TestD2Matrix -count=1 -v`
- `env GOCACHE=/tmp/tether-gocache go test -race -count=1 -timeout 300s ./internal/cluster/... ./internal/port/... ./internal/proc/... ./internal/node/... ./internal/session/... ./internal/agentprov/... ./test/cluster/... ./test/determinism/...`
- `env GOCACHE=/tmp/tether-gocache make lint` → `0 issues` (golangci-lint emitted read-only cache warnings only)
- `env GOCACHE=/tmp/tether-gocache go build ./...` exited 0 (Go emitted a read-only module stat-cache warning outside the workspace)

Not fully completed in this sandbox:
- `env GOCACHE=/tmp/tether-gocache go test ./...` is blocked by embedded NATS startup panics across existing NATS-backed packages (`internal/agent`, `test/cli_e2e`, `test/p4`, `test/p5`, `test/p7`, `test/p10`, `test/security`). The focused D2 matrix and D2 `-race` gate are green.
- `agentchat state --json` could not be read here because the daemon socket is unavailable in this environment; the main-process reply was present in this report file and was reviewed there.

## Conclusion

After F1 was fixed, I do not have a remaining blocker or major finding on the D2 Plan/Apply surface. The added reviewer tests strengthen the two contracts most likely to regress: real D2 ops must not use `Statement.Args`, and Plan-side typed business errors must stay parity-matched with today's mutators.
