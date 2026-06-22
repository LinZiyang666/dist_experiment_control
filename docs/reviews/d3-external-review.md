# Fail — D3 external review

Reviewer role: external reviewer. Scope: all unstaged / untracked D3 changes, including
`docs/distributed-broker-architecture.md`, `docs/reviews/d3-{plan,review}.md`,
`internal/cluster`, `internal/authcallout`, `internal/auth`, `internal/natscluster`,
`internal/broker/authcallout.go`, `internal/proto/subjects.go`, and `test/d3`.

结论：D3 不能按当前形态通过外审。信任面的大部分实现与测试方向正确，但 PIN write
seam 存在一个会在 D9 cutover 时暴露的真实逻辑缺陷：按当前 D3 文档/测试建议把
`Handler.ProvisionAgentWrite` 接成 `node.Propose(PlanProvisionWithPIN)` 时，follower 会先在
本地 stale DB 上执行 Plan，可能返回永久业务错误，而不是 D3-R3 要求的 transient
`not_leader` deny。

## Tasklist

- [x] Scope census: confirmed there was no staged baseline and reviewed all unstaged tracked plus untracked D3 files.
- [x] Requirements / architecture alignment: checked D3 against D2 outputs, distributed-broker §3.2/§6.2/§8.4/§13.8/§19-D3, and the D3 plan/review reports.
- [x] Raft transport / Node audit: reviewed mTLS `NetworkTransport`, static `BootstrapPeers`, timeout knobs, `LeaderContactStale`, `IsNotLeader`, and transport cleanup on `New` failure / `Shutdown`.
- [x] Auth callout audit: reviewed fail-closed local reads, nil-seam zero regression, PIN write seams, not-leader classification, and `QueueSubscribe`.
- [x] NATS / RF1 ACL audit: reviewed subject SSOT, `PermissionsForBroker`, user-template exclusion of `cluster.*`/`$SYS.*`, renderer completeness, and behavioral tests.
- [x] Test rigor audit: checked fake-vs-real composition, vacuity controls, route mTLS / cross-server callout tests, and D3 e2e matrix wiring.
- [x] Independent reviewer test: added a focused in-memory regression test for the follower PIN write seam.
- [x] Verification: ran non-socket focused tests; recorded sandbox-blocked socket/NATS gates separately from the real failing reviewer test.
- [x] Report: this report written as `docs/reviews/d3-external-review.md`.

## Findings

### F1 — High: follower PIN write seam can return permanent business errors before it reaches raft not-leader

Locations:
- `internal/cluster/node.go:296` — `Node.Propose` runs the `plan(n.db)` closure before `n.Apply(cmd)`.
- `internal/agentprov/plan.go:22` — `PlanProvisionWithPIN` reads `sessions` from the local DB and returns `ErrSessionMissing` before proposing.
- `internal/authcallout/handler.go:76` — the D3 seam contract says clustered PIN writes must return `authcallout.ErrNotLeader` when this broker is not leader.
- `docs/distributed-broker-architecture.md:164` / `docs/reviews/d3-plan.md` D3-R3 — follower / just-deposed leader must deny as transient `not_leader`, never as false allow or unreplicated direct write.

Why this fails:

The planned seam shape is effectively:

```go
h.ProvisionAgentWrite = func(...) error {
    return node.Propose(func(db *sql.DB) (*cluster.Command, error) {
        return agentprov.PlanProvisionWithPIN(db, ...)
    })
}
```

But `Node.Propose` does not establish leadership before planning. It first runs `PlanProvisionWithPIN` against the local replica. If this broker is a follower whose local DB has not yet applied the session row, the plan returns `agentprov.ErrSessionMissing`. The handler then emits a permanent auth deny instead of a transient `not_leader` deny.

Impact:

This does not create a false allow or an unreplicated local write, so the security half is fail-closed. It still violates D3-R3’s load-bearing classification contract. In D9, a real agent currently treats auth-deny as terminal, so a follower / lagging-replica PIN bootstrap can kill bootstrap instead of surfacing the intended retriable not-leader path.

Reviewer repro added:

- `test/d3/follower_pin_review_test.go`
- `go test ./test/d3 -run TestD3FollowerPINWriteStaleReplicaReturnsNotLeader_Review -count=1 -v`

Observed failure:

```text
follower PIN write must classify as transient not_leader before local Plan business errors; got *errors.errorString agentprov: session does not exist
```

Expected fix direction:

The clustered PIN seam must gate leadership before any local Plan business decision on a follower, or `Node` should expose a leader-only proposal helper that returns `raft.ErrNotLeader` before invoking the planner. The existing `cluster.IsNotLeader` mapping is useful, but it only helps after a raft not-leader error is actually reached.

### N1 — Low: D3 architecture status line still says implementation is in progress

Location: `docs/distributed-broker-architecture.md:531`.

The same change set includes `docs/reviews/d3-review.md`, which states Stage C internal review completed and all internal fixes/gates were green. The architecture status still says `实现进行中`. This is process-document drift, not the reason for FAIL, but it should be corrected when the main-process reply updates the report.

## Reviewer Additions

Added `test/d3/follower_pin_review_test.go`.

The test uses in-memory Raft transport, so it does not depend on local socket permissions. It models the exact stale-follower case: leader DB has the session, follower DB has not applied it, and the follower seam calls `Node.Propose(PlanProvisionWithPIN)`. Current code returns `ErrSessionMissing`; the D3 contract requires transient not-leader.

## Verification

Passing:

- `env GOCACHE=/tmp/tether-gocache go test ./internal/authcallout -run TestD3 -count=1`
- `env GOCACHE=/tmp/tether-gocache go test ./internal/auth -run TestD3RF1ClusterACLOnlyBroker -count=1`
- `env GOCACHE=/tmp/tether-gocache go test ./internal/natscluster -run TestD3 -count=1`
- `env GOCACHE=/tmp/tether-gocache go test ./internal/cluster -run 'TestD3FailClosedBoundary|TestD3MultinodeRaftConfigValid' -count=1`
- `env GOCACHE=/tmp/tether-gocache go test ./test/d3 -run 'TestD3ProductionWiresNoClusterNode|TestD3SeamMapsRaftNotLeaderToTransientDeny' -count=1`
- `git diff --check`

Failing for the real product issue:

- `env GOCACHE=/tmp/tether-gocache go test ./test/d3 -run TestD3FollowerPINWriteStaleReplicaReturnsNotLeader_Review -count=1 -v`

Sandbox-blocked, not treated as product findings here:

- `env GOCACHE=/tmp/tether-gocache go test ./internal/cluster -run 'TestD3TransportTwoNodeReplicate|TestD3LeaderContactStaleLive|TestD3FollowerProposeReturnsNotLeader' -count=1`
- `env GOCACHE=/tmp/tether-gocache go test ./test/d3 -run 'TestD3CrossServerCalloutAuthorizes|TestD3RF1ApplyACL|TestD3RenderedConfCrossServerCalloutDedupe|TestD3RouteMTLSRejectsBadCert' -count=1`

Both blocked because this sandbox denies local listening sockets (`listen tcp 127.0.0.1:0: socket: operation not permitted` / embedded NATS startup panic). The reviewer repro above avoids sockets and still fails.

## Required Before Re-review

1. Fix the PIN write seam contract so non-leader brokers classify before local follower Plan can leak stale-replica business errors.
2. Add a passing regression covering follower `Node.Propose(PlanProvisionWithPIN)` or the replacement leader-only helper, with the stale-local-replica shape in this reviewer test.
3. Update `docs/distributed-broker-architecture.md` D3 status after the main-process response.

---

## 主进程回复（逐条处置 · 2026-06）

外审**正确**——F1 是真 bug，且比 Stage C 内审更深：内审证了「seam 返回 not-leader → handler transient deny」与「真 Node follower `Apply` 返 `ErrNotLeader`」，但**没** compose「follower `Node.Propose(PlanProvisionWithPIN)` 在陈旧本地副本上先跑 leader-only Plan」这条真实接线——而 `Propose` 恰恰在 `raft.Apply` **之前**跑 `plan(n.db)`，故 follower 副本落后时漏出永久 `ErrSessionMissing`。**全部采纳并修。**

### F1【已修】— `Node.Propose` 在跑 Plan 前未门 leadership
- **根因确认**（亲手核验）：`internal/cluster/node.go` `Propose` 先 `plan(n.db)` 再 `Apply`；`PlanProvisionWithPIN` 读本地 `sessions`，follower 副本未复制该 session 时返 `ErrSessionMissing`（永久），never 到达 `raft.Apply` 的 `ErrNotLeader`。违反 D3-R3「follower/刚下台 leader 返 transient `not_leader`」+ §3.3「Plan 仅 leader」。
- **修复**（采纳外审建议方向 (b)「Node 暴露 leader-only proposal helper，跑 planner 前先返 `raft.ErrNotLeader`」）：`Propose` 在 `applyMu` 内、`plan` 之前加门——`if n.raft.State() != raft.Leader { return raft.ErrNotLeader }`。**非 leader 绝不在陈旧副本上跑 leader-only Plan**；leader 与「门后→Apply 间瞬时失主」由 `raft.Apply` 的 `ErrLeadershipLost` 兜底（同 transient，`cluster.IsNotLeader` 识别）。门在 `applyMu` 内，与 `{gate→Plan→Apply}` 同序列窗口（不破 d2 的 PA-5/PA-8 并发不变量）。
- **验证**：reviewer 复现 `test/d3/follower_pin_review_test.go` 的 `TestD3FollowerPINWriteStaleReplicaReturnsNotLeader_Review` **现通过**（follower `Propose(PlanProvisionWithPIN)` 返 `raft.ErrNotLeader`、`cluster.IsNotLeader`==true、非 `ErrSessionMissing`）。单节点 leader 的 `Propose`（D2/D3 既有测试）不受影响（State==Leader→门通过→Plan 照跑）。
- **回归**：D2 等价/determinism/`TestD2Matrix` + D3 全面 -race + `make test`(clean) + `make e2e` 全 phase 全绿，无回归。

### N1【已修】— §19-D3 状态行仍写"实现进行中"
- **处置（采纳·已修）**：`docs/distributed-broker-architecture.md` §19-D3 状态行更新为「实现完成 + 内审过 + 外审 round-1（F1）已修」，并摘记 F1 修复要点 + 指向本报告。

### 复验门（修完 · 全绿）
`go build ./...` ✓ / `go clean -testcache && make test` ✓（含 reviewer repro + `TestRaftConfinedToClusterPackage` raft-confinement）/ `make lint` **0 issues** ✓ / **`TestD3Matrix` -race** ✓ / `test/d3` 全包 -race ×5 稳定绿（修了一处我自加的 T5 渲染-conf 测试在重负载 -race 下的端口 flake，与外审 finding 无关）/ **`make e2e` 全 phase** ✓。

→ **请复审。** 复审通过后我 commit/push（step 7）。

---

## 外部复审 round 2（2026-06）

**PASS.** F1 / N1 均已修复；未发现新的 blocker / major finding。

### Re-review tasklist

- [x] Re-read main-process response and checked the unstaged fix diff.
- [x] Re-audited `Node.Propose` leader gate placement against D3-R3 and D2 `applyMu` invariants.
- [x] Re-ran the reviewer-owned stale-follower PIN regression.
- [x] Re-ran focused D2 Propose / Plan parity tests to check the new leader gate did not regress leader-side `Propose`.
- [x] Rechecked D3 seam / RF1 / natscluster non-socket tests and `git diff --check`.

### F1 re-review — Fixed

`internal/cluster/node.go` now checks `n.raft.State() != raft.Leader` under `applyMu`
and before running the planner, returning raw `raft.ErrNotLeader`. This closes the exact
bug: follower-local stale replica state can no longer leak as `ErrSessionMissing` before
the not-leader classification path. A leadership loss after the gate still surfaces via
`raft.Apply` as `ErrLeadershipLost`, which `cluster.IsNotLeader` also classifies.

The reviewer regression now passes:

```text
go test ./test/d3 -run TestD3FollowerPINWriteStaleReplicaReturnsNotLeader_Review -count=1 -v
```

### N1 re-review — Fixed

`docs/distributed-broker-architecture.md` §19-D3 now says D3 implementation is complete,
internal review passed, and external round-1 F1 was fixed, with a pointer back to this
report.

### Verification

Passing in this environment:

- `env GOCACHE=/tmp/tether-gocache go test ./test/d3 -run TestD3FollowerPINWriteStaleReplicaReturnsNotLeader_Review -count=1 -v`
- `env GOCACHE=/tmp/tether-gocache go test ./test/d3 -run 'TestD3FollowerPINWriteStaleReplicaReturnsNotLeader_Review|TestD3ProductionWiresNoClusterNode|TestD3SeamMapsRaftNotLeaderToTransientDeny' -count=1`
- `env GOCACHE=/tmp/tether-gocache go test ./test/cluster -run 'TestConcurrentPropose|TestD2RealOpsDoNotUseStatementArgs_Review|TestD2PlanErrorParity_Review|TestPlanError_Parity|TestPlanAllocate_DesiredPortCollision' -count=1`
- `env GOCACHE=/tmp/tether-gocache go test ./internal/authcallout ./internal/auth ./internal/natscluster -run TestD3 -count=1`
- `env GOCACHE=/tmp/tether-gocache go test ./internal/cluster -run 'TestD3FailClosedBoundary|TestD3MultinodeRaftConfigValid' -count=1`
- `git diff --check`

Still sandbox-blocked here, same as round 1:

- mTLS / embedded-NATS tests that need local listening sockets fail with
  `listen tcp 127.0.0.1:0: socket: operation not permitted` or NATS startup panic.
- `TestD2Matrix` in this sandbox also fails because its `./internal/cluster/...` package
  now includes the D3 mTLS socket tests; the non-socket D2 Propose / parity subset above
  passed.

### Conclusion

D3 external review passes after the F1 fix. The remaining full-gate evidence that depends
on local sockets is taken from the main-process run in a non-sandboxed environment; the
socket-independent regression that failed round 1 now passes locally.
