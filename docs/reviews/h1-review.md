# h1 增量 — 内审报告（多专家对抗审查 + 主进程裁决）

日期：2026-08-04 · 增量：h1（ps 有界回包 / raft 存储 GC / proc-exit 可靠投递 / ctl 存活契约 / 热循环退避 / 日志封顶）
plan：[`h1-plan.md`](h1-plan.md)

## 审查方法

6 个视角的 reviewer 并行读**未提交工作区 diff**（~67 文件 / +2649 行），随后 6 个 verifier
**一一对应**独立复核（每个 verifier 只看一个 reviewer 的 findings，被要求「无法证实即判 REFUTED」）。
reviewer 视角：raft/FSM 确定性、并发与生命周期、wire 兼容与混版、失败模式与误报、
运维与首跳部署、闸门合规与测试质量。

共产出 **38 条 finding**（3 BLOCKER / 15 MAJOR / 20 MINOR）。verifier 判定：绝大多数 CONFIRMED，
1 条 BLOCKER 被降为 MAJOR（OVERSTATED），另有多条被指出「reviewer 读的是审查开始时的树，
主进程已在审查进行中修好」。

**交叉确认是本轮最有价值的信号**：`proxy_bind_stalled` 重启后无法清除这一条，被 raft、
并发两个互不通气的 lens 各自独立发现——这类重合基本可以直接当真。

---

## 一、已采纳并修复（正确性缺陷）

### 1. `proxy_bind_stalled` 告警在 broker 重启后永远无法清除 — MAJOR，2 lens 独立发现

- **缺陷**：唯一的「节点已恢复」清除路径 `proxyRotateReady` 需要 leader 本地内存里的
  `proxyRotate` tracker；tracker 随进程死亡。而 `sweepProxyBindAlerts` 当时只清除
  **session 已消失**的告警。于是：告警 ACTIVE → broker 重启（**下一个升级跳就会发生**）→
  节点其间已恢复 → 没有 tracker → 第一行就 return → 告警永远 ACTIVE，
  `tether alert ls` 报告一个健康节点「proxy 卡住」。节点被 evict 同理，且 sync.Map 泄漏。
- **修复**：把 sweep 改成真正的**电平触发**——每 tick 由 `driveProxyReconcile` 计算
  `stalledNow`（本 tick 自己判定为卡住的 sid/nid 集合），不在集合里就清除。一次覆盖三种丢失
  场景（重启后恢复、节点驱逐、session 消失）；OFFLINE 节点显式保留告警（不可达的 agent 确实
  是卡住的 proxy），`nodeKnownButOffline` 用 `ErrNotFound` 区分「已驱逐」与「离线」。
- **测试**：`TestSweepProxyBindAlertsDecisions` 五个 case 表驱动，含修复前必然失败的重启场景。
  为可测，把 clear 动作提成参数（与 `gcDrain` 提 propose 是同一手法）。

### 2. 告警 raise 是边沿触发，一次 Propose 失败即永久静音 — MAJOR

- **缺陷**：`if tr.Fails() == proxyBindAlertAfter` 只在**恰好第 3 次**轮转时尝试 raise，
  而 `proposeProxyBindAlert` 的错误路径只打日志。leader 抖动/无 quorum 导致那一次 Propose
  失败，整段卡死期间告警再也不会出现。
- **修复**：改成 `>=`。ACTIVE 时重复 propose 代价为零——转换门在 plan closure 内部，
  已 ACTIVE 时返回 nil command，**不产生任何 raft entry**。
- **测试**：`TestProxyBindAlertRaiseIsLevelTriggered`。

### 3. 被 park 的 `started` 会让同 pid 的 `exit` 永不投递 — MAJOR，2 lens 独立发现

- **缺陷**：`deliverRound` 的 per-PID 顺序保证把**所有** pending started（含 parked）计入
  `startedPending`，而 parked 条目永不重试 → 该 pid 的 exit 被永久扣住。对旧 broker 而言
  这比 v0.4.7 的 fire-and-forget **更差**（exit 根本不会发出）。
- **修复**：只有**未 park** 的 started 扣住 exit。
- **测试**：`TestCourierParkedStartedDoesNotBlockExit`。

### 4. parked 的 `started` 没有 replay 通道 → 下次 register 会杀掉用户的活进程 — 原报 BLOCKER，verifier 降 MAJOR

- **缺陷**：park 条件是「连接正常但连续 3 次请求超时」。一个**新** broker 在 1 vCPU 上被 GC
  积压拖慢（正是首跳场景）完全可能触发。started 被 park → broker 没有该行 →
  agent 的 register 快照报告该 pid 在跑 → G.1 孤儿判定 → **命令 agent 杀掉用户的活进程**。
  （`pendingExitSnapshot` 只带 exit，started 没有 replay 通道。）
- **修复**：**只 park exit，永不 park started**。started 重试在两个方向都无害
  （`INSERT OR IGNORE` + broker 的去重预读抑制重复审计），而 park 它的下场是杀进程。
- **测试**：`TestCourierNeverParksStarted`。

### 5. parked 条目豁免于「永不无限等待」上限 — MINOR

- **缺陷**：`courierMaxLifetime` 检查在 `ev.parked` 跳过之后，而 parked 正是最可能常驻的条目。
- **修复**：上限检查前移到跳过之前。测试：`TestCourierMaxLifetimeReleasesParkedEntries`。

### 6. 单模式 `proc.GCExited` 与三个同族路径不一致 — MINOR

- **缺陷**：另外三条（`PlanGCExited` / `port.GCTerminated` / `PlanGCTerminated`）都用
  `COALESCE(end, start)` + UTC cutoff，唯独单模式 proc GC 保持 `ended_at < ?` 原样——
  于是 `ended_at IS NULL` 的 EXITED 行在单模式**永生**，且单/集群行为分叉。
- **修复**：补 COALESCE + `.UTC()`。

### 7. logrotate 三处 — MINOR ×3

- `Close` 不 latch：后续 Write 会在 reopen 节流窗口后**复活**已关闭的文件。→ 加 `closed` 闩。
- Write 短写时报告成功并**吞掉记录尾部**。→ 未写入的尾部 spill 到 stderr 并计入返回值。
- rename 失败时每次轮转都写一行 marker，**marker 自身**再次撑过阈值触发下一次轮转，
  文件在 marker+记录之间震荡。→ marker 按 `remindEvery` 限流（与 degraded 提醒同一纪律）。
- 测试：`TestWriterCloseLatches` / `TestWriterRenameFailureTruncatesInPlace` /
  `TestWriterDegradedReopenIsRateLimited`，三条均**变异验证**过（删 fallback、删限流各自变红）。

---

## 二、已采纳并补齐（测试覆盖缺口）

闸门/测试质量 lens 指出：D、E2、F 三个 workstream **交付了机制却没交付 plan 点名的对抗测试**，
而 A、B 交付了。逐条补齐，且每条都做了变异验证：

| 缺口 | 补的测试 | 变异验证 |
|---|---|---|
| D 的误报防护三层（`IsConnected` 守卫 / 往返探测 / 二次打击）三条变异全部逃逸 | `TestCtlLivenessReaperNeverReapsWithoutAProvenConn`（两个子测试**分别隔离**两个守卫） | 删探测 → 红；折叠二次打击 → 红 |
| `proxy_bind_stalled` 生命周期零覆盖 | `TestSweepProxyBindAlertsDecisions` + `TestProxyBindAlertRaiseIsLevelTriggered` | 见上文 1、2 |
| logrotate 自称的「反事故不变量」未测 | 上文 7 的三条 | 已验 |
| dup2 panic sink（F 的头号机制）零覆盖 | `TestRedirectStderrToLandsPanicText`（**子进程真 panic**）+ `TestResolveAgentLogSinkPrecedence` + `TestAgentDaemonArmsPanicSink`（接线断言） | 删调用点 → 接线断言变红 |

**这里有个方法论教训值得记下**：D 的两个守卫在生产中是纵深防御，于是**互相掩护**——
关掉 server 会让 nats.go 翻成 reconnecting，`IsConnected` 守卫先挡住，探测的删除便隐形。
真正只有探测能救的状态（socket 在、nats.go 仍报 CONNECTED、但无法往返）在进程内 server
上造不出来。处置是把探测**提成可注入的 var**并写明理由——让那个危险状态获得真测试，
而不是一句乐观的注释。

---

## 三、审查期间已被修好的（reviewer 读的是较早的树）

ops lens 的 BLOCKER「workstream F 在 racknerd 上是死的——plan 要求的迁移 runbook 不存在」
与 raft lens 的 MAJOR「X-1/Q2 的补偿文档从未写」，都指向同一批文档。这些在审查 workflow
运行**期间**由主进程完成，ops verifier 明确指出了这个时序差（"the reviewer's stated diff
size does not match the actual working tree… the two docs files the reviewer claims don't exist"）：

- `usage.md`：§9.13.1（`reply_too_large` 是 70 里唯一不该重试的）、§5.11 的 ps 有界与截断脚注、
  §5.13 的 ctl 存活契约（含挂起 >3min 会被挂断这一明确取舍）。
- `broker-ops.md`：§7.5 重写（进程内封顶，**删掉 logrotate 配置**）、新增 §8.8 racknerd 迁移
  runbook（含「必须 `rm` 而非 truncate 那两个 root 属主日志文件」这一步及其原因）。
- `deploy-tier-gotchas.md`：新增 #74（新 FSM op + 新告警种类要求 broker 锁步；
  日志重放式回滚被封死；为何不做能力门及其反转条件）。

同理，闸门 lens 的 BLOCKER「structural budget 是红的」在审查期间已手改账本（Agent 126→127）。

---

## 四、已知残留（不在本增量修，登记在案）

1. **simcluster deploy-tier 清扫未做**（ops lens MAJOR）：约 25 个 drill/harness 文件仍断言
   h1 之前的日志契约（`broker.err` 存在、journald 契约方向相反、`_agent_journal_after` 家族）。
   deploy-tier drill 按 CLAUDE.md §5 是**按需运行**的第三层测试，不进 `go test`/CI；
   这批清扫应与真机跑 drill 一起做，属于部署验证而非本增量的代码正确性。
2. **`distributed-broker-architecture.md` 的 op catalog 未补 `OpProcGC`/`OpPortGC` 行**
   （raft lens MAJOR 的一半）。第 2 层文档，应补；本轮已把运维约束写进 gotchas #74 与
   broker-ops §8.8，锁步规则不至于失载。
3. **cluster 模式下 `unknown_pid` 跨 forward 的身份未测**（MINOR）：`proc_not_found` 的
   `forwardErrKind` 条目存在且逻辑正确，但只有单模式测试覆盖。失败面有界（退化成
   `store_error` NACK → courier 退避重试到 register 清算）。
4. **`classifyJSErrClass`、AlertReconciler 阻尼、courier round budget、`drainStarted`、
   ctl 侧 A 表层（截断脚注/psJSON 字段/hint 路由）** 无直接测试（各 MINOR）。
5. **`reply_egress_test.go` 的 census 不覆盖 `RespondMsg`**（MINOR）：当前全仓零处使用，
   属守卫缺口而非活缺陷。
6. **二次打击间隔是 5s tick 而非 plan 写的「一个完整 ka 间隔」**（MINOR）：实际语义更保守
   （更早发现），但与 plan 文字不符；plan 文字与实现应择一对齐。
7. **`resolveReconcile` 的 doc 注释被 `exitStamp` 截断**（MINOR，注释完整性）。
8. **agent.yaml 模板未写注释掉的 log 键**（MINOR）：ops verifier 确认 v0.4.7 用
   `KnownFields(true)` 严格解码，**手工加这些键会让回滚后的 agent 拒绝启动**——所以
   写成注释是安全的、写成实键是危险的。当前模板不写，行为正确但可发现性差。

---

## 五、硬闸状态（内审修复后重跑）

| 闸门 | 结果 |
|---|---|
| `make test` | **PASS**（65 包） |
| `make lint` | **PASS**（0 issues，golangci-lint v2） |
| `make e2e-parallel` | **ALL PASS**（3m40s） |
| `-race` + 内建泄漏门 | PASS（pty / agent / broker / logrotate 全部触碰面） |

本轮为通过闸门而做的**账本手改**（每条都是对不变量的编辑，理由随 commit message）：

- `type-methods internal/broker.Broker` 281 → **282**（内审新增 `nodeKnownButOffline`）
- `type-methods internal/agent.Agent` 126 → **127**（D 新增 4 个方法 − C 删除 3 个旧 pub\*）
- `main-noncli-code-lines cmd/tether` 1100 → **1200**、`pkg-files cmd/tether` 52 → **56**（F 的 sink 解析 + 平台文件）
- `cfgdb` ratchet：`exec.go:handlePsReq` 3 → **2**，总数 119 → **118**（A 把 ports 读改走 `b.read()`，**下调**）
- wire inventory / wire-freeze / enum families / CLI golden / migration list / error-code allowlist：见 plan 的 gate 表

---

## 六、结论

内审的三个 BLOCKER 与全部 CONFIRMED 的 MAJOR 已处置完毕：正确性缺陷 7 条已修并各自带回归测试，
测试缺口 4 组已补并全部变异验证，文档三份已落地。残留 8 条均为 MINOR 或明确的范围外事项，
逐条登记在第四节。

**下一步：提交用户外审。** 外审报告写入 `docs/reviews/h1-external-review.md`，
主进程在报告文件内逐条回复并修改；**外审不过不算 done**（CLAUDE.md §3 step 6）。
