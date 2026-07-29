Fail

# Batch C 外部审查报告

日期：2026-07-28
审查者：外部审查（独立于实现与内部审查）
目标：`main` / `0f26330210eb49e429f8672bb0e702ab29e583d0`

## 结论

不放行。改动中有两个可直接造成灾难恢复不可继续或在途对象被删除的 blocker；另有
CLI/doctor 的假陈述、预算数学边界、版本回滚操作语义和部署层覆盖缺口。内部报告把一部分
行为问题归入“措辞与注释精度”或 permanent decision，不能改变其上线后会发生的实际后果。

本轮只增加独立回归测试与审查文档，没有修生产实现。

## Intake 与范围

- intake 时 index 为空；index 外共有 50 个路径（36 modified、14 untracked），约
  1,283 insertions / 164 deletions。
- intake patch SHA-256：
  `9a13a6bbf96a0eceaf5f117ea4c74163c24a7d3e4c05ee45950f1fce6c45d6bf`。
- 范围重建为：
  - C1：online force-single roster prune 的持久重试 op、leadership resume、drain marker healer；
  - C2：tier-B size-derived budget、agent prep cache、watchdog/ledger/cross-home GC、wire code；
  - C3：`TopoAction` 线协议、共享 classifier、status/card/doctor/reconcile CLI；
  - simcluster drills 12、22、93。
- 权威链按 `CLAUDE.md` 执行：requirements 定 WHAT，
  `distributed-broker-architecture.md` 与 `deploy-tier-gotchas.md` 定 binding HOW；
  `architecture.md` 只作仍有效的历史依赖参考。内部 plan/review 只作为线索，未作为证明。

## Findings

### B1 — BLOCKER：raft 已不可逆改写、marker 尚未落盘的 crash/failure 窗口没有可达修复

`handleForceSingleCommit` 先在 `force_single_online.go:257` 执行
`RecoverToSelfOnline`，然后才等待 leader、写 `force_single_active`、生成并写 recovery
epoch（263–282）。这些后续失败都让用户“re-run --online”。

但恢复成 N=1 leader 后，observe path 会恢复 leader contact；`forceSingleArm.dwellState`
在有 contact 或新进程尚未观察过 leaderless 时返回 `quorum_not_lost`
（`force_single_online.go:68-75,167-175`）。代码自己也在 295–297 行承认同一原因会令
prune 失败后的重跑被拒。于是错误信息给出的修复命令不可执行。

新增的 leadership resume 也不能覆盖这个窗口：
`resumeForceSingleFinalizeOnLeadership` 的第一项持久前置就是
`force_single_active`（`force_single_finalize.go:442-447`）。仓库自己的
`TestLeadershipEdgeCreatesFinalizeOpOnlyForTheGhostShape/no force-single marker` 还固定了
“无 marker 不创建 op”。结果是：

- crash/leader-wait/marker propose failure后，节点已经是可写 N=1，但 status 和 destructive
  gate 都看不到 emergency；
- marker 成功、epoch 生成/写入失败时，leadership healer 可以删 ghost，却不会补 epoch，
  分裂检测的 durable input 永久缺失；
- 无论哪一种，文案中的重跑都会在 arm 阶段被 `quorum_not_lost` 拒绝。

要求：为 post-rewrite 状态建立独立、幂等、无需重新通过 quorum-loss dwell 的持久恢复
协议；marker、epoch、prune 的各窗口都必须可枚举恢复。错误文案必须指向实际可达命令，
并增加在真实 online 路径上的 kill/fault injection。

### B2 — BLOCKER：home broker 重启后会删除仍在预算内且有持久 ledger 的传输对象

本批把最大 push watchdog 扩到 34m08s，却只给 cross-home GC 增加 size-aware floor。
home-owned path 在 `transfer_reconcile.go:120-128` 只看重启后为空的内存 tracker，并以
`xferReapMinObjectAge`（2m）调用 `reapBucketObjects(..., false)`。它完全不读取已经存在的
`xfer-inflight` durable ledger。

内部测试 `TestOrphanReaperStillOutrunsALiveTransferAfterRestart` 明确承认 2m < 34m08s；
`TestHomeReapIsNotSizeAware` 反而用 AST 固定这个危险调用。把它记为 plan N15 不能把数据
删除变成可接受 non-goal，因为 ledger 已经保存 transfer id、bucket、size、started_at，
“没有 durable evidence”并不成立。

本轮增加真实 JetStream 回归
`TestXferReapAfterRestartPreservesLedgerBackedLiveObject`：写入对象与 durable inflight row，
模拟新进程 tracker 为空，把时钟推进到 3m，再运行生产 reaper。结果：

```text
home reaper deleted a ledger-backed live transfer only 3m0s after restart:
nats: object not found; its size-derived watchdog budget is 5m0s
```

要求：home reaper 删除前必须用 durable ledger（或等价持久证据）排除仍在有效
budget+slack 内的对象；不能简单用内存 tracker 证明 orphan。删除当前“缺陷必须继续存在”的
反向测试，改为真实重启/ledger/对象存储的安全回归。

### M1 — MAJOR：pull 实际仍固定 5m，但 CLI 与 usage 声称按文件大小派生

`agent/transfer.go:457-471` 明确说明 `PullPrepareReq` 没有 size，并调用
`XferBudget("b", 0, 1)`；broker pull watchdog 同样只有 size=0。因此最大 2 GiB pull
仍只有固定 5 分钟，并未获得本批对慢链路的保护。

但 `tether pull --help` 在 `cmd/tether/transfer.go:387-388` 写着 “tier B is derived from
the file size”；`docs/usage.md:948,976-984` 又把 push/pull 一并描述为同一公式。这会让用户
以为 36m08s CLI 默认值能保护大 pull，实际 agent/broker 会在 5m 终止。

独立回归 `TestPullHelpDoesNotPromiseASizeDerivedBudget` 已稳定钉红。要求二选一：把 size
加入 pull 协议并让 broker/agent/ctl 同预算，或明确把 pull 记录为固定 5m 限制并修正所有
文档/help；不能保留当前互相矛盾的产品契约。

### M2 — MAJOR：共享 classifier 已判定 degraded，doctor/`--wait` 仍给出相反结论

`TopoBehind` 与 `TopoUnknownAction` 的 `Degrades()` 都为 true；unknown 的 next step 是
升级 reader。但 `clusterDoctorOnline` 只收集 Stuck/Held
（`cluster_doctor_online.go:79-85`），其余一律在 110–118 行输出 topology PASS：
“every reached voter's ... is converging”。UnknownAction 明明无法由当前 reader 判断，
Behind 也尚未 converged。

新增 doctor 两行行为回归均失败：

```text
behind topology was reported PASS
unknown action topology was reported PASS
```

同一缺陷还存在于 `reconcile nats --wait`：`topoWedged` 只立即处理 Stuck/Held；
UnknownAction 会等到 deadline 后返回 transient 75，要求自动化继续重试，而真正 next step
是升级。要求所有消费者覆盖 classifier 的完整状态集，并按统一 taxonomy 返回
PASS/ADVISORY/FATAL 与可执行 next step。

### M3 — MAJOR：承诺的 2 MiB/s 边界没有任何控制面或收尾余量

`XferBudget` 恰好等于 `legs × ceil(size/2MiB/s)`；2 GiB push 的 2048s 全部被两次纯数据
传输占满。但 broker watchdog 在 prepare 转发前已经启动，期间还包含 bucket/open、
NATS 往返、校验、SHA、对象元数据/fsync、receiver finalize 与事件传播。

因此一条恰好达到文档承诺“最慢链路 2 MiB/s”的连接在数学上仍必超时：所有非字节传输
工作只能耗时 0。agent TTL 有 1m slack，CLI 有 2m slack，真正执行删除/失败的 broker
watchdog没有 slack。要求给 broker budget 增加明确的 setup/finalize margin，并重新推导
最大 CLI、ledger stranded、GC inequality；或者降低承诺吞吐，使端到端预算含这些开销。

### M4 — MAJOR：旧 leader 会自动终止任何未来版本的未知 operation

`driveOne` 的 default 分支不只是报告版本不兼容，而是通过无 predecessor CAS 的
`PlanClusterOpAbort` 自动把未知 kind 改为终态 ABORTED。这个行为被写成通用逻辑，不只适用
可从 substrate 重建的 force-single finalize。

混合版本/rollback 时，旧 leader 可以在无人确认下销毁新版本 workflow 的唯一驱动记录，
并把半完成 substrate 留给并不知道其语义的旧版本。新增的 enum-independent operator
`cluster ops abort` 是合理逃生口，但“能手工 abort”不推出“应该自动 abort”。

要求：未知 kind fail closed、显式 degraded/fatal 并保留 op；仅在操作者明确调用 abort 时
使用 enum-independent plan。若坚持自动 abort，binding architecture 必须定义它对所有未来
operation 的安全证明，而不是只用本次可重建 op 举例。

### m1 — MINOR：新增的第三种 `Inconsistent` 含义仍被错误诊断成 roster/raft

DRAINING-without-marker 现在会设置 `Inconsistent`，但 doctor 的唯一文字仍是
“roster/raft INCONSISTENT — run cluster doctor/status”。这既误述原因，又把用户递归引回当前
命令，没有指出 `cluster drain --abort`/检查 active op/marker。status card 的 generic
fallback 也无法区分该新含义。建议增加 typed inconsistency reason，而不是继续扩展 bool。

### T1 — RELEASE GATE：drill 12/22 没有覆盖 C1 新失败路径

drill 12 只证明 offline path 不创建 op；drill 22 只证明 online happy path 同步 prune、无
finalize op。两者均不注入 prune propose failure、marker/epoch failure、commit handler
kill、leadership resume 或 terminal give-up。本轮实跑 happy path 不能作为 B1/B2 类窗口的
部署层证据。要求在 drill 22 增加真实故障注入与恢复 oracle；不能用“clean path 没有 op”
代替 retry 机制的 deploy-tier 验证。

## 已接受的实现面

以下项目经独立源码与测试交叉检查，本轮未发现阻断：

- tier ceiling/throughput 常量集中到 `internal/proto`，push 两腿、endpoint 一腿的意图清楚；
- agent push prep TTL 按 entry size 派生，cache 满时拒绝而不是淘汰最老活跃传输；
- cross-home GC 的 per-object extra、ledger `Size` 的 additive compatibility、稳定 synthetic
  timestamp 与 `transfer_budget_exceeded` 归因；
- `TopoAction` additive wire propagation、self-row overwrite、legacy reason fallback、Stuck/Held
  不受 generation gate、status/card severity fold；
- finalize op 的 advance-after-observe、rejoin phase guard、terminal budget、upgrade-lock carveout、
  manual escape hatch，以及 drain-marker healer 只清 rosterless marker 的窄谓词。

这些正确局部不能抵消 findings 中的不可达窗口和数据删除路径。

## 验证证据

- `git diff --check`：PASS。
- `gofmt -l`：PASS（空输出）。
- `make lint`：PASS，0 issues。
- `CGO_ENABLED=0 go build ./...`：PASS。
- `go vet ./...`：PASS；`phasefluidity,c7,d5,d6,d7,d8,d9_integration,e2e_matrix`
  tag slices：compile/vet PASS。
- `go test ./internal/natsconf ./internal/proto -count=1`：PASS。
- `go test ./internal/agent -count=1`：PASS（必须在可绑定 loopback 的环境；受限网络沙箱内
  的 NATS startup panic 已证实为环境限制）。
- `go test ./test/determinism -count=1`：PASS。
- targeted `-race`：proto/natsconf PASS；cmd 由 M1/M2 的三个独立断言按预期 FAIL。
- simcluster hermetic `tests/run-all.sh`：ALL PASS。
- `make e2e-parallel`：FAIL，仅两个 shard 的同一独立 B2 回归失败；其余列出的 unit 均 PASS。
- `make test`：FAIL，仅 M1/M2 的三个 cmd 断言与 B2 的真实 JetStream 回归失败；其余 package
  均 PASS。
- simcluster 当前镜像构建：PASS（tether-sim:dev，nats-server 2.10.22 pin 一致）。
- drill 12、22：均 `GREEN`，合计 649s，`NO DEVIATIONS`；它们只覆盖已有的 offline/happy
  path，不能证明 T1 所列失败窗口。
- drill 93：`INCOMPLETE rc=4`，57 PASS、0 product-red/setup-red/assert-fail、1 个登记为
  `NOT-COVERED[gap]` 的 quorum-loss transient-window 覆盖缺口；本批新增的
  TOPO-STUCK/action propagation、doctor fatal 与恢复断言全部 PASS。runner 的
  `NO DEVIATIONS` 只表示结果符合 ledger 中预期的 `INCOMPLETE`，不是 GREEN。
- drills 12/22/93 结束后检查 Docker containers、networks、volumes：无本轮实例残留。

## 疑惑与需 owner 明确的决策

1. force-single 的 durable intent 应在 raft rewrite 前落到哪里，才能既不把普通
   roster/raft divergence误删，又能让 post-rewrite marker failure 可恢复？当前实现把
   “证明发生过 recovery”的唯一证据放在 recovery 之后，这是 B1 的根矛盾。
2. pull 是否被有意排除在 C2 size-derived 目标之外？代码说是 permanent N5，binding docs
   和用户文档却说所有 tier-B；必须选一个产品事实。
3. home reaper 是应以 ledger transfer-id 精确保护，还是引入另一个 durable ownership
   index？单纯把所有 home grace 拉到 34m+ 会放大小盘 orphan retention，但继续 2m 删除
   活对象不可接受。
4. 未知 operation 的默认策略是否真的允许旧版本自动丢弃新版本意图？若无架构级证明，
   本审查坚持“报告+显式 operator abort”，不接受自动 mutation。

## 建议（不替代 required fixes）

- 把 force-single commit 拆成可恢复 phase ledger，并对每个相邻 phase 做 kill-9 matrix；
- 用 transfer ledger 驱动 reaper 的 allow-delete 决策，并做真实 broker restart + slow
  JetStream upload；
- 给所有 `TopoState` 消费者建立 exhaustiveness table，新增状态时任何遗漏均编译/测试失败；
- 将 `Inconsistent bool` 改为 additive typed reason，避免 doctor/card 猜原因；
- 预算测试加入“exact throughput + positive overhead”反例，不只比较公式与自身常量。

## Tasklist 与最终判定

`batch-c-external-review-tasklist.md` 的 A–F 审查面均已执行；任何无法形成正面证明的项目已在
上述 finding 或疑惑中显式记录，而非静默跳过。由于 B1、B2 以及 M1–M4 未解决，本批结论
保持 **Fail**。

最终执行完成后，tasklist 全项已勾选；全部生产改动、原有新增文件、外部审查测试及两份
审查文档均已加入 index。cached diff 的完整性检查、路径数、SHA-256 和 index 外清洁状态
在暂存后作为最终交付回执记录，避免把报告自身哈希写入报告而形成自引用。

---

# 主进程回复（阶段 C 步骤 6）

> 逐条回复并修改。**七条 finding 全部采纳、全部已修**，每条都重跑了它自己的变异确认新守卫会变红。
> 审查者钉红的三条独立回归（`TestPullHelpDoesNotPromiseASizeDerivedBudget`、
> `TestClusterDoctorTopologyCheck` 的两行、`TestXferReapAfterRestartPreservesLedgerBackedLiveObject`）
> **现已全绿，且未修改审查者写下的任何断言**。

## B1 — 采纳。durable intent 移到 raft rewrite 之前

诊断完全成立，包括"resume 的触发条件正是那个可能没写成的 marker"这一点——那是我设计里的循环依赖。

**修法**：把本仓 offline 路径**早就有**的原语（`clusteroffline` 的 pre-mutation fsync journal）补给 online 路径。

- 新增 `clusteroffline.OnlineIntent` + `WriteOnlineIntent/ReadOnlineIntent/ClearOnlineIntent`，
  与 offline journal 共用 `writeFileDurably`（同样的 symlink 拒绝与 fsync 语义）。
- **epoch 在 rewrite 之前 mint 并写进 intent**，每次 resume 复用同一个值——
  这正是你指出的"epoch 写失败时无法回答『我承诺的那个 epoch 到底落没落』"。
- `handleForceSingleCommit`：intent 先落盘，**落盘失败就拒绝开始不可逆改写**
  （"nothing would be able to finish it if this process died"）。
- `resumeForceSingleFinalizeOnLeadership` 的 **PATH 1 是 intent，且不查 marker**；
  marker 那条降为 PATH 2，只服务"升级跨过本改动 / intent 丢失"的旧状态。
- `applyForceSingleIntent` 幂等，且 **read-back 校验**——propose 返回 nil 不算数
  （poison-skip / `RowsAffected==0`）。epoch **按值比较**，第二次事故不会留着上一纪元的 token。
- intent 只在**全部事实确认落地后**清除（含 prune；prune 还在重试就保留）。
- 文案：rewrite 之后不再说"re-run --online"（arm 闸会拒），改为
  "intent 已记录在磁盘、本 broker 拿到 leadership 就会自动完成；在 status 报 FORCE_SINGLE 前不要跑破坏性命令"。

回归：`TestForceSingleIntentRecoversTheMarkerAndEpochWindow`（从 intent-on-disk + rewrite-done 的确切窗口态驱动）、
`TestForceSingleIntentIsIdempotentAndScoped`（重复 apply 零 raft 写入；第二次事故覆盖 epoch；他机 intent 不被套用）。
**变异**：把 resume 改回以 marker 为前置 ⇒ 变红。
**部署层**：见下方 T1。

## B2 — 采纳。N15 判定被推翻，home reaper 改用 durable ledger

你是对的，"没有 durable evidence"不成立：`xfer-inflight` ledger 早就存了 transfer id / bucket / size /
started_at，而**对象名就是 transfer id**（ctl 与 agent 两个 Put 点都是），所以守卫是精确身份匹配而非启发式。
把它记成 permanent decision 是我的错误判断。

**修法**：新增 `ledgerProtectedObjects(now)`，`reconcileXferObjects` 每次 sweep 算一次并透传进
`reapBucketObjects`；仍在 `budget + slack` 内的对象一律跳过。**ledger 读不出来就整轮不删**
（"acting on evidence we could not read is precisely the failure this guard exists to prevent"）。
带 terminal 的行不保护（已决outcome，对象可弃）。

**删掉了那条要求缺陷继续存在的反向测试**，换成 `TestHomeReapConsultsTheDurableLedgerNotJustTheTracker`。
你的 `TestXferReapAfterRestartPreservesLedgerBackedLiveObject` 现在绿。
**变异**：让 `ledgerProtectedObjects` 返回空集 ⇒ 两条都变红。

## M1 — 采纳。选"明确记录为固定 5m"，并扫掉所有反向陈述

按你给的二选一，选后者（给 pull 上 size 要动 proto+agent+ctl 三端，属被判缓的中期半）。
`pull --timeout` 的 help 改为**明说** pull 在 agent 与 broker 两端都是固定 5 分钟、
调大这个 flag 不会让又大又慢的 pull 成功、请改用 `expose` + rsync。
`docs/usage.md` 的那段改成 "**push 的** tier-B 预算由大小推导"，并单列一段说明 pull 不在此列及其原因。
你的 `TestPullHelpDoesNotPromiseASizeDerivedBudget` 现在绿。

## M2 — 采纳。两个消费者覆盖分类器的完整状态集

doctor 现在收集 Stuck/Held/**Behind/UnknownAction** 四态：Stuck=FATAL，Held/Unknown/Behind=ADVISORY
（Unknown 不判 FATAL——对端更新不等于故障；但绝不能判 PASS）。
`--wait` 的 `topoWedged` 加入 UnknownAction：等待解决不了"读者比写者旧"，
next step 是升级 reader，所以立即返回而不是烧完 deadline 再退 75。

另按你"建议"一节加了穷尽表 `AllTopoStates()` + `TestEveryTopoStateHasAConsumerVerdict`：
每个状态都用真分类器造行，断言两个消费者与 `Degrades()` 一致——新增第七个状态会在这里失败，
而不是在生产里默认成错误极性。你的两行 doctor 断言现在绿。

## M3 — 采纳。预算加上明确的 setup/finalize 余量

你的数学成立：`legs × ceil(size/throughput)` 断言了所有非字节工作耗时为零，
于是恰好达到承诺吞吐的链路**必然**超时。新增 `XferOverheadMargin = 60s`（与 agent 的 1m、ctl 的 2m 同量级），
加在**数据时间**上而非下限上（小传输本来就有余量）。
`XferTierBMaxBudget` 重新手算为 `2×1024s + 60s = 2108s = 35m08s`，测试里的字面量同步重推。

按你"建议"一节加了 `TestBudgetLeavesRoomForNonTransferWork`——**不是**公式自比：
它断言"恰好按承诺吞吐跑完两腿"之后仍有正余量。**变异**：把 margin 置 0 ⇒ 每个 size 都变红。

## M4 — 采纳。改为 fail closed，保留 op 只报告

你的论证我接受，而且它推翻的正是我自己上一轮的理由。自动 abort 是**由刚承认自己看不懂这个 op 的二进制**
去做的一次 mutation；混版/回滚时会销毁新版本 workflow 的唯一驱动记录。
"能手工 abort"确实推不出"应该自动 abort"。

`driveOne` 的 default 现在只打一条 Error（点名原因 + 唯一解法），**不做任何复制式写入**。
`cluster ops abort` 保持 enum-independent（那是逃生口的意义）。
原先钉"必须自动 abort"的结构门改为 `TestUnknownKindDefaultFailsClosedWithoutMutating`——
AST 断言该分支不出现 `Propose/transition/PlanClusterOpAbort/setPhase` 任何一个。

## m1 — 采纳。改成 typed reason，而不是继续扩 bool

`adminsock.ClusterNodeStatus` 加 additive `inconsistent_reason`（`roster_raft` | `draining_without_marker`），
broker 侧派生式拆成两个具名条件（roster/raft 优先，它才是能 fork membership 的那个）。
新增 `adminsock.InconsistencyDetail(reason)` 给出**按原因**的 cause+remedy。
三个消费者全部改用：doctor（不再把运维递归引回 `cluster doctor`）、status card 头条、表格 flag（`INCONSISTENT(drain)`）。
空 reason 回落历史措辞 = 老 broker 的语义。回归 `TestInconsistentReasonDrivesCauseAndRemedy`。

## T1 — 采纳。真实故障注入已落地，并**换了注入点**

先在 drill 22 注入，结果 **PRODUCT-RED**。查下去是**我的注入点错了**，不是产品：
online force-single 之后该节点仍是 clustered nats.conf + JS 503（这条正是同一个 drill 上两块断言的形状），
重启即撞该 drill 自己记载的 #35 崩溃循环——实测 45s 窗口内 `NRestarts=21`，
`ReconcileMembershipOnLeadership` 根本没机会跑。

于是把注入移到 **drill 20**：offline 路径会自动 de-cluster，结束时是**健康可重启的 N=1**（该 drill 自己断言 tier-B 在 N=1 可用）。
在那里：植入中断态 intent → 重启 → 断言前进完成 + `force_single_active` 被报出（破坏性门保持关闭）。
**drill 20 GREEN**。drill 22 保留"干净恢复不留 intent 文件"的生命周期断言并回到 **GREEN**。

过程中还暴露两个我自己的测试缺陷，都已修：`not_covered` 少传 class（harness 正确拦下）、
以及一版断言在文件未植入时**空泛通过**——被我加的非空泛前置断言抓到。

## 关于"已接受的实现面"

你列出的那些我不做改动。其中 `TopoAction` 的 additive 传播与 self-row 覆盖、finalize 的
advance-after-observe 与 rejoin phase guard、drain-marker healer 的窄谓词，都是内审那一轮
逐条变异验证过的，本轮没有触碰。

## 复核后的闸门

`make test` 全绿 · `make lint` 0 issues · `make e2e-parallel` **ALL PASS** ·
全标签 `go vet` 干净 · `gofmt` 干净 · `-race`（broker/agent/cluster/natsconf/proto/determinism）全绿 ·
drill **20 GREEN**、**22 GREEN**、**12 GREEN**、41 INCOMPLETE(0 assert_fail)、93 INCOMPLETE(0 assert_fail)。

你 intake 时看到的三条红——`make test` / `e2e-parallel` / targeted `-race` 里那几个——
都是你写下的独立回归，现在全部由**产品修复**转绿，你的断言一个字未改。
