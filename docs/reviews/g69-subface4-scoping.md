> # ⛔ SUPERSEDED — 本文的核心主张已被证伪，请勿据此行动
>
> **本文第 3 节主张「对称复用 `clusterStreamsReady`」——该主张经 G69 的 plan workflow 与主进程复核，
> 判定为 WRONG。** `ObserveReplicas` 枚举 `events` + 每个 `history-<sid>` + **每个活的 `OBJ_xfer-*`**，
> 而 `AllAtTarget` 依赖的 `ActualReplicas` **只数 `Current && !Offline` 的 peer**
> （`internal/jsstream/replicas.go`）。照本文实现会让**每次 grow 等到所有流（含多 GiB 对象存储）
> 字节复制完**；`audit.go` 自己就记着滞留的 under-target `OBJ_xfer-*` 会「blocks the D7 retire gate forever」。
> **那会做出一个让正常扩容卡死的门。**
>
> **以 `docs/reviews/g69-plan.md` 为准**（判据改为「放置」而非「追平」，且只看 `events` 一条流）。
> 本文仅作调研轨迹保留。

# #67 sub-face 4 — `cluster add` 过早宣告成功 — 选型稿（SCOPING ONLY，非 plan）

> 主进程调研产物，**未写任何产品代码**。目的是把这条"剩余唯一属于主进程的发版前遗留项"从
> 「设计风险不明」降到「可决策」。正式 plan 仍需按 CLAUDE.md §3 走对抗性起草。
>
> 上游：`docs/deploy-tier-gotchas.md` `### #67` open sub-face 4；G67 内审把它列为
> **"the item most likely to bite a real operator on the first push after a grow"**。

## 1. 问题（G67 **没有**修掉的那半）

G67 让「grow 完立刻传文件」的失败变**诚实且可重试**（`jetstream_not_ready` + 明确重试指引 + 有界重试），
但**没有消除失败本身**。实测：

- **空载**部署层：grow 后第一次 tier-B push **1.66s 成功、零重试**；
- **多 drill 并发**（单机 6–9 个 clustered-JS 集群）：3 次尝试跨 8s **全部超时**，运维需按提示重试一次。

根因不在 push 侧而在 grow 侧：**`cluster add` 在 JS meta 尚不能放置 R=N 资产时就把 join op 判为终态 SERVING**。

## 2. 现状：终态门把关的是什么

`internal/broker/cluster_operation_controller.go` 的 `OpStateNatsRolledOut` 分支，
在 `transition(op, OpStateServing, true, …)` 之前只过 `topoAdvance(op, sub, true)`：

- `topoAdvance` → `topoConvergedForOp`：把关的是**拓扑代收敛**（每个 voter 都报告目标 `topology_generation`，
  即 nats.conf 已 rollout 并被活进程加载）；
- **它不检查 JetStream meta 是否已经能承载 R=N 资产。** conf 加载完 ≠ meta 能放置资产。

## 3. 关键发现：不需要发明新探针，仓库里已有一个 fail-closed 的现成信号

`internal/broker/clusterwrite.go:466` 已有 **生产级** 判据：

```go
// clusterStreamsReady is the production stream-readiness gate (external-review F1): retire is
// refused unless EVERY JS stream is at its target replica count (ReplicaReport.AllAtTarget is
// fail-closed — an incomplete observation reports NOT ready).
func (b *Broker) clusterStreamsReady(string) (bool, error)
```

- 它已被 **retire** 路径使用（external-review F1 的产物），**join 路径没用**。
- `AllAtTarget()` 本身就是 **fail-closed**：未观测到 / 空集合一律判 not ready
  （`audit_publisher.go:448`，注释明写「a fresh cluster / an errored pass must never falsely permit a D7 retire」）。
- 对 grow 到 N=2 而言，「events/history 已真正达到 R=2」**正是** meta 能在 R=2 放置资产的直接证据——
  因为它刚刚就放置成功了。

⇒ 修法是**把同一条生产判据对称地用到 join 终态**，而不是新造一个 `/jsz` 判定机
（后者正是 R16 A5 当初刻意推迟的东西）。

## 4. 主要风险与既有缓解

**风险 = gotcha #45 的形状**：fail-closed 的门 + 不可靠信号，曾把 op 永久钉在 `NATS_ROLLED_OUT`，
`assertNoActiveOp` 随即拒绝下一次成员变更，整个 grow/shrink 脊柱 wedge 住且运维无状态可操作。

**但缓解已经在同一个函数里现成**：`topoAdvance` 已带 `CatchupDeadline` → 超时转 `OpStateBlocked`
（`cluster ops confirm/abort` 可处置），并有 `recordOpError` 写明等待原因。新增合取项若接进**同一条**
deadline/BLOCKED 通道，就继承有界性，不会复现 #45。这一点必须是 plan 的硬约束，而不是实现时的自觉。

**第二个风险（必须在 plan 里裁决）**：`ObserveReplicas` 是一次网络观测，接进每 tick 的控制器循环会增加
raft-leader 侧负载；retire 路径是**低频**调用，join 终态门是**每 tick**。需要缓存/节流，或只在
`topoConvergedForOp` 已通过后才求值（后者更省，且语义上也对：拓扑没收敛时问副本没意义）。

## 5. 影响面与验收

- **产品**：`cluster_operation_controller.go` 一处合取 + 复用 `clusterStreamsReady`；无 wire 变更、无新码。
- **hermetic**：join 终态门的 RED-first（副本未达标时不得进 SERVING）、超时必须进 BLOCKED 而非永久等待、
  以及「拓扑已收敛但副本未达标」这一具体组合。
- **deploy-tier**：drill 67 的 `CONTROL(before)` 现在无条件记 `not_covered … sub-face 4` —— 修好之后
  这条 gap 应当**自然消失**（nc_gap 从 1 降到 0 之外的部分不变），这就是它的具名 oracle；
  drill 42/51（grow 家族）作回归。

## 6. 为什么没有直接开工

工作树已承载 **R16 + G67 两个未外审增量（44 改 + 17 新）**。再叠第三个**HA 状态机**增量会：

1. 扩大外审边界并引入跨增量耦合——某一处 finding 可能迫使另外两处返工；
2. 违背 R16 定的调子（HA 关键路径「不 rush」，值得单独 plan→实现→内审→外审）。

因此本文只做到**可决策**为止，把开工与否留给用户。
