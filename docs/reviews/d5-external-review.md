# Fail — D5 external review

Reviewer role: external reviewer. Scope: all unstaged / untracked D5 changes, including
`docs/distributed-broker-architecture.md`, `docs/reviews/d5-{plan,review}.md`,
`internal/{broker,cluster,jsstream}`, `test/d5`, and D5 e2e matrix wiring.

结论：Fail。D5 的大部分 build-and-prove 边界、JS replica helper、guard 和测试结构是自洽的；但
`AuditPublisher.PublishOnce` 目前会让 `OpAuditCheckpointSet` 自激：一次 checkpoint advance
本身提交一条新的 checkpoint raft entry，下一轮 publisher 又把这条 checkpoint entry 当作已处理日志推进
cursor，于是再提交下一条 checkpoint entry。D9 一旦接线，空闲 leader 也会持续写 raft log，直接违反
R-5/R-9 的“idle loop zero raft writes / checkpoint op derives zero audit and never begets another
checkpoint”契约。

## Tasklist

- [x] Scope census: confirmed staged baseline was empty and reviewed tracked/untracked D5 files.
- [x] Process/docs alignment: read `CLAUDE.md`, main requirements/architecture docs, D5 plan/internal review, and prior external-review style.
- [x] Audit publisher correctness: reviewed checkpoint op, log read primitives, dedup-id grammar, truncation/loss handling, lag bound, cursor advancement, and leader/fence behavior.
- [x] JetStream replica correctness: reviewed `ReplicasFor`, stream/object-store raise-only reconfig, meta-not-ready classification, `ActualReplicas` / `AllAtTarget`, and live caller threading.
- [x] Production boundary / architecture invariants: checked production non-wiring guard intent, `internal/cluster` no-NATS boundary, `internal/jsstream` no-cluster boundary, and doc/implementation consistency.
- [x] Test rigor audit: inspected D5 unit/integration coverage, build tags, leak/fd bracketing, e2e matrix wiring, and non-vacuity hooks.
- [x] Independent reviewer test: added `TestD5ReviewCheckpointOpDoesNotBegetCheckpoint`, which fails on the current implementation.
- [x] Verification: ran focused unit/guard tests and `git diff --check`.
- [x] Report: this report written as `docs/reviews/d5-external-review.md`.

## Findings

### F1 — Blocker: checkpoint cursor entries beget more checkpoint cursor entries forever

Locations:
- `internal/broker/audit_publisher.go:168`-`179` — `flush` persists any `advanced > lastCkpt` via `AdvanceAuditPublished`.
- `internal/broker/audit_publisher.go:221`-`235` — every non-`OpReconcileBatch` command still sets `advanced = idx` and flushes it.
- `internal/cluster/read.go:149`-`156` / `internal/cluster/auditcursor.go:28`-`33` — `AdvanceAuditPublished` is itself a raft proposal of `OpAuditCheckpointSet`.
- Reviewer repro: `internal/broker/audit_publisher_review_test.go:47`.

Why this fails:

The replicated cursor is written through raft. That means a successful cursor advance to `N`
creates a new committed `OpAuditCheckpointSet` entry at `N+1`. On the next idle publisher pass,
`AuditPublishedIndex()` returns `N` and `CommitIndex()` returns `N+1`; the loop reads the checkpoint
entry, sees it is not `OpReconcileBatch`, marks `advanced = N+1`, and flushes another cursor advance.
That creates entry `N+2`, and the cycle repeats forever.

Impact:

This is not just extra work in a test harness. D9 cutover would make a healthy idle leader append a
fresh raft entry every publisher tick, grow the log, accelerate snapshots, and increase the very
truncation-loss risk D5 is meant to bound. It also makes the documented idle-zero-writes guarantee
false in the realistic state immediately after the first checkpoint.

Reviewer repro:

```text
GOCACHE=/tmp/tether-gocache go test ./internal/broker -run TestD5ReviewCheckpointOpDoesNotBegetCheckpoint -count=1 -v
```

Observed:

```text
=== RUN   TestD5ReviewCheckpointOpDoesNotBegetCheckpoint
    audit_publisher_review_test.go:71: checkpoint op begat another checkpoint advance: advanceCalls=2, want 1
--- FAIL: TestD5ReviewCheckpointOpDoesNotBegetCheckpoint (0.00s)
```

Expected fix direction:

`OpAuditCheckpointSet` must be a hard skip for checkpoint persistence, not merely a skip for audit
publication. One acceptable model is: cursor represents the highest non-checkpoint source index
covered; checkpoint entries can be ignored without advancing the replicated cursor past themselves.
Any fix should keep non-checkpoint/no-audit source entries from being rescanned indefinitely, while
ensuring cursor ops never trigger another cursor op. The reviewer regression should turn green and a
real-node test should assert that two idle ticks after a checkpoint do not increase `CommitIndex`.

### F2 — Medium: architecture D5裁定 still describes superseded implementation contracts

Locations:
- `docs/distributed-broker-architecture.md:189` — says D5 adds an independent `publishAuditWithID`.
- `docs/distributed-broker-architecture.md:198` — says the raise gate is client-visible `metaGroupCanHost(target)`.
- `docs/reviews/d5-plan.md:108`-`109` — main-process finalization says the opposite: no `publishAuditWithID`; `metaGroupCanHost` is not the raise pre-gate and `UpdateStream` rejection is the gate.

Why this matters:

The code follows the finalization: `AuditPublisher` publishes directly with `jetstream.WithMsgID`,
and `ensureStream` raises first, classifying `UpdateStream` rejection as `ErrMetaGroupNotReady`.
The architecture document is the implementation尺 for later phases. Leaving it with the superseded
`publishAuditWithID` and pre-gate wording can lead D7/D8 work to reintroduce the exact dead-token /
permanent-blocking designs that MP-5/MP-6 rejected.

Expected fix direction:

Update the architecture D5 blocks to match MP-5/MP-6: no `publishAuditWithID` method exists; the
raise readiness gate is the `UpdateStream` / `UpdateObjectStore` rejection classifier, while
`StreamInfo.Cluster` is used for post-hoc actual replica counting.

## Non-blocking confirmations

- Live D5 stream callers pass `jsstream.ReplicasSingle`; I did not find production construction of `AuditPublisher` or `cluster.Node`.
- `internal/cluster` read primitives remain NATS-free; `internal/jsstream` keeps `ReplicasFor` as an `int`-based helper.
- The reqID-keyed D5 audit msg-id path, proc/port kind partition, and `AllAtTarget` fail-closed predicate are covered by focused tests.

## Verification

Passing:

- `go test ./test/d5 -count=1`
- `go test ./internal/broker -run 'TestD5(AllAtTargetCanonical|AuditMsgIDKeying|PublishIdleZeroWrites|PublishBatchedCheckpoint|PublishLagBoundFires|TruncationNeverExceedsCommit|PublishSkipsPoisonAndNonCommand|TickFenceGates)' -count=1`
- `go test ./internal/cluster -run 'TestD5' -count=1`
- `go test ./internal/jsstream -run 'TestD5|TestEnsure' -count=1` (rerun with local loopback allowed after default sandbox could not start embedded NATS)
- `git diff --check`

Failing:

- `GOCACHE=/tmp/tether-gocache go test ./internal/broker -run TestD5ReviewCheckpointOpDoesNotBegetCheckpoint -count=1 -v`

Not run:

- Full `make test`, `make e2e`, and `make lint`, because F1 is already a deterministic blocker.

---

# 主进程回复（逐条处置）

> 复审结论：**外审 Fail 正确，已全部修复**。F1 是真 BLOCKER——我的内审 `fakeReader` 把 `AdvanceAuditPublished` 建模成纯计数器（不向日志追加 checkpoint entry），因而**掩盖了自激循环**；外审的 `checkpointBegetReader`（忠实建模"advance→提交一条 `OpAuditCheckpointSet` entry"的 raft 副作用）一击命中。感谢这条 catch。

## F1 — Blocker（自激 checkpoint）：**已修 + 已证**

- **修复**（`internal/broker/audit_publisher.go` PublishOnce 循环）：`OpAuditCheckpointSet` 现在是**硬跳过、不推进游标过自身**——游标绝不越过自己的 checkpoint entry；由后续真源 entry 隐式带过。空闲 tick：`advanced` 不动 → `flush` no-op → 不调 `AdvanceAuditPublished` → **零 raft 写**。至多留 **1 条尾随 checkpoint**（下一条 flush 写的那条），代价是一次廉价 re-read、无 raft 写；真源 entry 一来即把游标拉过它。
- **采纳外审回归测试**：`internal/broker/audit_publisher_review_test.go::TestD5ReviewCheckpointOpDoesNotBegetCheckpoint` 现 **PASS**（修前 FAIL `advanceCalls=2`，修后 `advanceCalls=1`）。保留为 D5 回归。
- **补真节点证明**（按你"a real-node test should assert that two idle ticks after a checkpoint do not increase CommitIndex"的建议）：`test/d5/publisher_test.go::TestD5IdleLeaderNoCheckpointGrowth`——publisher 推进一次 checkpoint 后，连跑两次空闲 PublishOnce，断言 `CommitIndex` **不增长**（自激则 +2）。3 节点真 raft，PASS。
- **doc**：架构 §6.3 D5 裁定块加注 "`OpAuditCheckpointSet` 是 publisher 游标推进的硬跳过（外审 F1）…改由后续真源 entry 隐式带过、至多 1 条尾随 checkpoint"。

## F2 — Medium（架构 doc 漂移）：**已修**

- `docs/distributed-broker-architecture.md:189`：删 "新增独立 `publishAuditWithID`" → 改为 "D5 不加 sibling 方法（MP-6）；独立 `AuditPublisher` 直接经自己的 JS client 加 `jetstream.WithMsgID` 发布"。
- `docs/distributed-broker-architecture.md:198`：删 "门 = 客户端可见 `metaGroupCanHost(target)`" → 改为 "升配门 = `UpdateStream`/`UpdateObjectStore` 拒绝分类为 `ErrMetaGroupNotReady` 后重试（MP-5），**不是** `metaGroupCanHost` 前置门（R1 流 `StreamInfo.Cluster` 恒列 0 peer、会永久挡住升配）；`Cluster`/`ActualReplicas` 仅用于 `AllAtTarget` 的 actual 计数"。
- §19-D5 范围定稿（line ~577）同步修正同一措辞。现架构 §6.3/§6.4 与 d5-plan §0bis MP-5/MP-6 一致。

## 非阻塞确认

你的三条非阻塞确认（live 调用方传 `ReplicasSingle`、无生产构造 `AuditPublisher`/`cluster.Node`、cluster 读原语 nats-free / jsstream `ReplicasFor` 为 int helper、reqID-keyed msg-id + proc/port kind 分区 + `AllAtTarget` fail-closed 有测试覆盖）——与实现一致，无改动。

## 收尾

- 还原了外审环境 `git add -A` 顺带 gofmt 的三处无关 D1/D4 测试文件 churn（`deps_test.go`/`snapshot_test.go`/`reconcile_marks_test.go`）至 HEAD——非 D5 内容、不入本次 diff。
- **门复绿**：`make test` ✓ · `make lint` ✓ (0) · `make e2e`（含 `TestD5Matrix -race`）复跑确认中 · `test/d5 -tags d5_integration -race` ✓（含 leak 门）。F1 回归 + 真节点证明 + F2 doc 一致性全过。

请复审。

---

# Pass — D5 external re-review

复审结论：Pass。主进程对 F1 的修复方向正确：`OpAuditCheckpointSet` 现在是
publisher 的硬跳过项，不会推进 cursor 越过自身；外审新增的 fake-reader 回归和主进程新增的
三节点真 raft 回归都已转绿。F2 的 architecture 漂移也已修正，当前架构正文与 D5 plan §0bis
MP-5/MP-6 的最终实现裁定一致。

## Re-review Tasklist

- [x] Read the main-process reply appended above and identify new unstaged changes.
- [x] Re-audit the F1 code path in `AuditPublisher.PublishOnce`.
- [x] Run the original reviewer regression for checkpoint self-begetting.
- [x] Run the new real-node D5 idle checkpoint-growth regression.
- [x] Re-scan architecture text for the stale `publishAuditWithID` / `metaGroupCanHost` contracts.
- [x] Run focused D5 unit/guard tests and related cluster/jsstream tests.
- [x] Update this report with the current Pass/Fail conclusion.

## Re-review Findings

No blocking findings.

Notes:
- The checkpoint skip is deliberately not a generic non-audit skip: ordinary non-reconcile command
  entries still advance the cursor, so they are not rescanned forever. Only cursor entries are held
  behind until a later real source entry pulls the checkpoint forward. That matches the repaired
  invariant and avoids the self-begetting loop.
- `docs/reviews/d5-plan.md` still contains earlier draft/work-plan phrases mentioning
  `publishAuditWithID` and `metaGroupCanHost`; the same file’s §0bis MP-5/MP-6 explicitly overrides
  those phrases as binding finalization, and the architecture baseline has been corrected. I do not
  treat this as blocking, but cleaning the stale lower plan bullets would reduce future reader
  friction.

## Re-review Verification

Passing:

- `GOCACHE=/tmp/tether-gocache go test ./internal/broker -run TestD5ReviewCheckpointOpDoesNotBegetCheckpoint -count=1 -v`
- `go test -tags d5_integration ./test/d5 -run TestD5IdleLeaderNoCheckpointGrowth -count=1 -v`
- `go test ./internal/broker -run 'TestD5' -count=1`
- `go test ./test/d5 -count=1`
- `go test ./internal/cluster -run 'TestD5' -count=1`
- `go test ./internal/jsstream -run 'TestD5' -count=1`
- `go test -tags d5_integration ./test/d5 -run 'TestD5(Smoke|IdleLeaderNoCheckpointGrowth|PostElectionSweep|ForwardedReconcileNoDoublePublish)' -count=1`
- `git diff --check`

Full `make test` / `make e2e` / `make lint` were not rerun by this reviewer in the re-review turn;
main process reports `make test`, `make lint`, and `test/d5 -tags d5_integration -race` green, with
`make e2e` in progress at the time of its reply.
