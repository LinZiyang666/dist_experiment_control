# 克隆凭据实例增量 · 第 2 轮内部审查报告

> 生成时间：2026-08-19。审查对象：工作树中未提交的 cloned-credential-instances 增量（`git status` 见 §1 末）。
> 本轮 19 条 lane 并行审查 + 一轮对抗性反驳，本报告为综合定稿。

---

## 1. 本轮定位（与 round 1 的差异）

round 1 审的是**增量本身**，产出 7 条 blocker，全部已处理。本轮**不重复 round-1 的 finding**，价值集中在两处：

1. **round-1 修法自身的缺陷**——B3（`PreviousNID`）、B4（`dropLease`）、#11（`stateStore.detach`）、#13（`SetNID` retire）、#9/B5（`Leased` 名字形状）、`TargetInstance` 这六个修法，每一个都在本轮被发现留下了新洞，且都是**在修法引入的那条路径上**。
2. **round 1 之后才出现、没人审过的两处改动**——(A) probe 移出 register handler（`inlineProbeGrace` / `backgroundProbeBudget` / `probeCache`），(B) graceful farewell（`ReleasingName`）。这两处贡献了 9 条 blocker 里的 4 条。

### 审查期间工作树发生了变化，三条 lane finding 已在轮中被修掉

本报告的每一条都**已对当前工作树重新核验**。以下三条在 lane 报告时为真、现在已不成立，详见 §5：

| lane 报告的问题 | 当前状态 |
|---|---|
| farewell 在 caller goroutine 上取 `nc.mu`，`systemctl stop` 可无限挂死 | **已修**：`releaseLeaseName` 已移入 S3 closer goroutine（`conn_teardown.go:210-213`），受 `closeBudget` + poison + S5 escalation 覆盖 |
| background probe 无条件覆盖更新的定性观测 | **已修**：`broker.go:1592-1596` 增加了 `pv.at.After(startedAt)` 的不覆盖守卫 |
| `leaseGrantWindow` = 1s 太短（offer 撞名 / grant 后 1s 即失守） | **已改**：`broker.go:1654` 现为 `leaseGrantWindow = leaseSubscribeSettle`（5s）。**但这只把 B5 的窗口从 [1s,10s) 收窄到 [5s,10s)，没有关闭它**——本轮实测确认 |

### 当前工作树里有三个审查者留下的复现测试文件（未跟踪，且**现在是红的**）

```
internal/broker/lease_probe_staleness_test.go            (B5 的复现)
internal/broker/reconcile_lease_ownership_test.go        (B2/B3 的复现，实测红)
internal/broker/lease_basename_collapse_review_demo_test.go (B9 的复现，以 t.Log 呈现)
```

它们是本轮最有价值的产物，但文件名/函数名带 `ReviewDemo`、头部带 `REVIEWER DEMO — Delete or fold in.`，按 CLAUDE.md §3 step 5b 必须**改名归位而不是删除**（见 M18）。修完 blocker 后这三个文件应变绿并留作守卫。

---

## 2. BLOCKER（必须改才能进外审）

### B1 — `releasing_name` 不是 additive-safe：N-1 broker 会把告别执行成一次**空快照的破坏性 register**

- **文件**：`internal/agent/instance.go:553-580`（`releaseLeaseName` 的 payload），发射点 `internal/agent/conn_teardown.go:210-213`
- **场景**（已实测，非推演）：新 agent 对着按**升级前** `NodeRegisterReq` 结构解码的 broker，ctx-cancel 时会在 `ctrl.s.lab.node.gpu1.register.req` 上再发一条：
  `{"proto_version":2,"nid":"gpu1","instance_id":"…","releasing_name":true}`。
  旧 broker 解出的是 `ReleaseVersion:"" OS:"" Arch:"" BootID:"" LocalProcesses:nil LocalPorts:nil RosterRefreshOnly:false`，于是走完整 register：`node.Register` 的 `ON CONFLICT` 把 `release_version/boot_id/nats_server` 清空、`proxy_capable=0`、`status='ONLINE'`、`last_heartbeat_at=now`（**在进程死亡的瞬间把行刷成更活**），随后 `reconcileOnRegister(LocalProcesses=nil)` 让该 nid 下**每一条 RUNNING/LOST 行**走 missed-exit：`markProcExited(pid,-1)` + `reconciled_closed`。
  该 agent **从未被租约化**——`releaseLeaseName` 只判 `a.instanceID != ""`（恒真）和 `intent == teardownShutdown`，所以**每一次优雅停止、包括单实例设备**都发这条。
- **后果**：下次启动时 (a) 真实退出码永久丢失（courier 见不到 pid 于是丢弃 pending exits，`ps` 里恒为 rc=-1）；(b) 活下来的子进程被报成无 DB 行的 running → orphan 臂 → **HEAD 没有本增量新加的 `sawAnyRow` 保险** → `DropProcesses` → agent SIGTERM+SIGKILL 掉操作员的活。
  可达路径三条：broker 回滚（requirements §6.7 明写"回滚是一等公民"）、混合集群里 leader 未升级、以及本特性自己的前提——**agent 版本被烤进镜像，"先 broker 后 agent"恰恰是这批用户无法保证的顺序**。
- **修法**：让告别在 N-1 broker 上**变惰性**而不只是"未知键"。最省的做法：farewell body 上带 `RosterRefreshOnly: true`（HEAD 的 `handleRegister:1436` 会在 `registerNode` 之前纯读短路），同时把新 broker 的 `ReleasingName` 臂**移到 `RosterRefreshOnly` 短路之前**（周期 refresh 携带 `ReleasingName=false`，语义不受影响）。若不愿复用该 flag，则把 farewell 构造成**完整的普通 register payload**（`buildLocalSnapshot` + release/os/arch/caps）再加 `ReleasingName`——这样旧 broker 执行的是一次准确且无害的 register。
  钉住它：`test/p2/legacy_broker_register_test.go` 已经有假的 pre-feature broker 并且**已经收到了 cancel 之后这第二条 body**，现在是直接 return——改成对它断言。

### B2 — `rowsOwnedBy` 把 `PreviousNID` 当作**整名所有权**，一次改名就关掉**当前持名者**的活进程行并解锁 orphan kill

- **文件**：`internal/broker/reconcile.go:201`（`mine := rowsOwnedBy(nid, req.PreviousNID)`）、`:313-325`（`sawAnyRow` / orphan 循环）、`:428-432`
- **场景**（**已实测红**：`TestPreviousNIDMustNotClaimRowsOfTheInstanceNowHoldingThatName`）：A 持 `gpu1` 且跑着 p1。A 链路抖动 → clone C 探测无 interest → C 被授予 `gpu1`，C 的 reconcile 把 p1 关掉；C 跑起自己的 q1（`nid=gpu1` RUNNING）。A 回来被 contested，adopt `gpu1-02`，带 `PreviousNID="gpu1"` register 一次：
  - q1 不在 A 的 `agentByPID` 里 → missed-exit 臂 → **C 正在跑的进程被标 EXITED rc=-1**；此后 C 真正的 `proc.exit` 命中 `already_exited` 快路径，**真实退出码永久丢失**；
  - q1 同时满足 `anyRowMatches` → `sawAnyRow=true`（对一个毫无历史的 nid！）→ A 自己的 p1 进 `DropProcesses` → `killOrphanProcess` SIGTERM+5s+SIGKILL 掉操作员的活。
  - 长分区变体里 C 在窗口内跑的**每一个** job 都在一次 pass 里被关掉。
  - `reconciled_closed` 审计还被记在 `nid=gpu1-02` 名下（`reconcile.go:305`），在 `gpu1` 的历史里完全看不见。
- **修法**：所有权改成 **pid 驱动**而非名字驱动。(a) `previousNID` 只用于 `hasIt` 方向——旧名下的行只有在 agent **重新呈报了该 pid** 时才可匹配，**永远不得进入 missed-exit/close 臂**；close 臂只迭代 `rowNID == nid` 的行。(b) 被救回的行**改档**（`UPDATE processes SET nid=<新 nid>`，见 B3）。(c) `sawAnyRow` 只由 `nid` 下的行 + **真正匹配上 pid** 的 previousNID 行计算。

### B3 — `PreviousNID` 只搭一次车、行从不改档，于是 adoption 之后的**第二次 register** 就把它救回来的进程杀掉

- **文件**：`internal/broker/reconcile.go:201` + `:313-325`；`internal/agent/instance.go:241-247`（`previousNIDOnce` 的 Swap）、`:497-505`（`adoptRoutingNID` 存 prev）
- **场景**（**已实测红**：`TestSecondRegisterAfterAdoptionDoesNotKillPreAdoptionProcesses`）：A adopt `gpu1-02` 时带着 p1，register#1 救回 p1——但**没有任何地方改写 `processes.nid`**，p1 仍归档在 `gpu1` 下，而 hint 已被 Swap 掉。之后操作员按 I2 承诺正常使用该实例，起了 p2（行在 `gpu1-02` 下）。**任何一次后续 register**（NATS 抖动 → `onNATSReconnect` → `a.register`、再一次 rebuild、re-exec）带 `PreviousNID=""` 到达：p1 不再 `mine` → 成为 `agentByPID` 里的残留；p2 的行让 `sawAnyRow=true` → `DropProcesses=[p1]` → **SIGKILL 一个从改名前就在跑的 job**。
  两条加重路径：**多跳**（`maxLeaseAdoptions=3`，第二跳 `prev := nidOf(a)` 覆盖掉对 `gpu1` 的记忆，真正装着活的那些行永久不可认领）；**空烧**（hint 在构造 payload 时消耗，而 contested 回复在 `replyLeaseVerdict` 里**早于** `reconcileOnRegister` 短路，register 失败/超时同样把 hint 花掉）。
  即使没触发 kill，行也终生归档在旧设备名下：`tether ps gpu1-02` 看不到它在跑的活，`tether ps gpu1` 把它算成另一个实例的。
- **修法**：**改档代替记忆**——通过 previousNID 匹配上的行被接受时，`UPDATE processes SET nid=<新 nid>`（单机直写 / 集群走 raft，同 `markProcExited`）。之后 hint 确实只需搭一次车，多跳与 hint 丢失都自动无害，`ps` 归属也对了。若认为改档本轮太重，最低限度：**不要 Swap 掉 previousNID**（保留到进程生命期结束）**且**按 B2 收窄 orphan gate——但 `ps` 归属仍是错的。

### B4 — exec 血统恢复把 `PreviousNID` 盖成 basename：升级一个**租约实例**会认领并关掉**兄弟实例**的进程行，随后升级为杀活

- **文件**：`internal/agent/agent.go:791-796`（`restored != "" → adoptRoutingNID(a, restored)`）→ `internal/agent/instance.go:497-505`（`prev := nidOf(a)`，此刻仍是 basename）→ `agent.go:1352`（`PreviousNID: previousNIDOnce(a)`）→ 被 `internal/broker/reconcile.go:201` 消费
- **场景**：A 持 `gpu1`、B 持租约 `gpu1-02`，两者健康，无竞态。操作员执行**文档明确支持**的 `tether node upgrade gpu1-02`（`cmd/tether/node.go:679-687`）。B `syscall.Exec`，`execEnv()` 把 `TETHER_ROUTING_NID=gpu1-02` 带过去；新镜像里 `agent.New()` 读到它就调 `adoptRoutingNID(a,"gpu1-02")`，而它的第一件事是 `prev := nidOf(a)` —— 此刻 `routingNID` 尚未设置，`nidOf(a)` 返回 **basename `gpu1`**。于是 B 的第一次 register 是 `{NID:"gpu1-02", PreviousNID:"gpu1", LocalProcesses:[]}`（空，因为 `syscall.Exec` 抹掉了 `a.procs` 和 courier）。经 B2 的机制，**A 的每一条 RUNNING 行被 markProcExited(-1)**；A 起新 job 后 `sawAnyRow` 成立，先前被关的 pid 变成"无行的 running"→ `DropProcesses` → **broker 命令 A 杀掉操作员还在跑的活**。
  两条 rollback re-exec 路径（`upgrade_state.go` 的 `realExec`、`upgrade.go` 的 `reExecInPlace`）产生同样的戳记，所以 watchdog / boot rollback 也会做这件事。
- **修法**：**把 restore 和 adoption 拆开**。新增只做恢复的路径（如 `resumeRoutingNID(a, nid)`）：`routingNID.Store` + `setExecLineage` + `ExposeAdapter.SetNID` + `stateStore.detach` 全做，但**不设 `previousNID`、不 `leaseAdoptions.Add(1)`**，`agent.go:796` 改调它。理由：(a) `previousNID` 的用途是救回**本进程曾经注册过的**名字下的行，re-exec 的镜像什么名字都没注册过，这个字段在 wire 上断言了一件假事；(b) 3 次 adoption 预算是防 broker 抖动把 session loop 转起来的，restore 不是 broker 驱动的改名。
  守卫：restore 后 `previousNIDOnce(a)` 必须为 `""`；broker 侧断言一次 leased register 不能关掉归档在 basename 下的行。

### B5 — `probeCache` 缓存**定性观测**，10s 内被重放：一个方向把裸名同时发给两个 clone，另一个方向把重启的设备改名

- **文件**：`internal/broker/broker.go:1543-1546`（读）、`:1566-1570`（定性答案也写缓存）、`probeTTL` `:1503`；授予点 `:1738/:1764/:1797/:1843` 均**不失效缓存**；唯一的 Delete 在 farewell 臂 `:1884`
- **方向 (a) — 扇出回来（本轮实测红）**：t0 普通重启，无人订阅 → `ErrNoResponders`（definitive）被缓存，A 拿到 `gpu1`；~1ms 后 A 订阅并开始应答。t0+6s（**已过 5s 的 `leaseGrantWindow`，仍在 10s 的 `probeTTL` 内**）第二个 clone register → 缓存命中 → `held=false, known=true` → `!held && known` → **也拿到裸名 `gpu1`**。两个进程订阅同一 forwarded subject，**每条 exec 跑两次**，这正是整个增量存在的理由。
  证据：把 lane 的复现里的 1.1s 改成 6s，在 `/tmp` 副本上跑：
  `--- FAIL: TestGrantInvalidatesTheFreeObservationThatAuthorisedIt … clone B was granted the bare name "gpu1" while A2 is live and answering claim-probe`
- **方向 (b) — 重启被改名（当前树实测红）**：A 持名并应答，clone 探测过后缓存里是 `{answered:true, responder:A, definitive:true}`；A 被 `kill -9`（**不发 farewell**，且设计明说裁决必须在没有它的情况下收敛），订阅随 socket 消失；A 在 `probeTTL` 内重启（`RestartSec=5` / 容器重启策略 / crash loop）→ 缓存命中 → `held=true, known=true` → `!held && known` 不成立、silence rule 要求 `!known` 也不成立 → `assignLeaseName` → **设备被改名 `gpu1-03`**。
  证据：`--- FAIL: TestHeldObservationMustNotOutliveTheHolder … the restarting agent was renamed to "gpu1-03" … the bare name the operator addresses now goes STALE`
- **后果**：(a) 是本增量的目标缺陷复活；(b) 是 §3d 那个症状（裸行 STALE、操作员继续对着它下命令）从缓存这条路回来，且落在 farewell **明确帮不上忙**的路径上。窗口是 `[leaseGrantWindow, probeTTL)` ≈ 5s，在**每一次 probe 中介的授予之后**都存在。
- **修法（一处即可同时关掉两个方向）**：**不要缓存定性答案**。`probeTTL` 的全部算术（`:1492-1502`）都是从后台路径推出来的——"probe 自己要烧掉 3s 才写入"——所以只有**歧义/后台**结果需要缓存来避免 probe-per-retry 活锁。一次定性的 inline 答案成本 ~1ms（`ErrNoResponders` 由**服务器**合成，活 agent 的应答是一跳），handler 本来就付了 `inlineProbeGrace`。具体：`:1566-1570` 直接返回 verdict，**去掉 `b.probeCache.Store`**，只保留后台 goroutine `:1597` 的那次 Store。副作用是 `probeCache` 缩到只装歧义键。
  次选（更弱、且**关不掉方向 (b)**，因为 kill -9 既无 farewell 也无授予）：在每个记录 `leaseGrant` 的点上 `probeCache.Delete`。

### B6 — farewell 删掉 `leaseHolder`，而那正是 **silence rule 唯一读的证据**；于是"告别送达"反而让后继被改名，"告别丢失"却不会

- **文件**：`internal/broker/broker.go:1876-1884`（farewell 臂，`leaseHolder.Delete` 在 `:1881`）对 `:1842`（`priorHolderSpoke && !known && …`）；agent 侧 `internal/agent/conn_teardown.go:189-213`、`internal/agent/exec.go:40-44`
- **场景**：优雅停止**确定性地**制造出 silence 形状，然后销毁读懂它所需的证据：
  1. `conn_teardown.go` 的 S2 `cancel()` **先跑**，此后 `dispatchForwarded`（`exec.go:40-44`）丢弃**每一条** forwarded 消息，claim-probe 也在内——agent 变成"有订阅、不应答"的主体；
  2. closer goroutine 的第一件事是 `releaseLeaseName`，broker 删掉 `leaseHolder[sid/gpu1]`；
  3. **之后**才轮到 cleanups（`proxyHandlerWG.Wait()` → `subFwd.Unsubscribe()`）和 `nc.Close()`。在 Unsubscribe 之前，interest **仍在服务器的表里**。
  这个窗口永不为零、上界是 `closeBudget`（10s）。此间后继 register：`leaseHolder` miss ⇒ `priorHolderSpoke=false`；心跳新鲜 ⇒ 歧义臂；probe ⇒ 有 interest、无应答 ⇒ `(held=true, known=false)` ⇒ silence rule 因 `priorHolderSpoke=false` **无法触发** ⇒ `assignLeaseName` ⇒ **被加后缀**。
  若 farewell **丢失**（kill -9 / crash / 分区），`priorHolderSpoke` 仍在，silence rule 正常授予裸名。
- **后果**：把设计声称"纯优化"的东西变成了**净负**——它修的正是 §3d 那个改名，而它自己在 drain 窗口里制造同一个改名。它同时证伪了该臂的安全论证（`:1820-1825`"伪造 farewell 最多丢掉一条缓存，probe 和心跳钟立刻重建"）：`priorHolderSpoke` **恰恰是** probe 和钟都重建不了的那一位，silence rule 的注释自己写着"holder map 是冷的时候本规则故意不触发"。
- **代价核算（说明为什么删除是净负）**：删掉 holder entry 只在两个分支改变结论：(a) `:1717` 的 ≤`leaseGrantWindow` 分支——后继在前任授予后 5s 内到达且 interest 已被回收（crash-loop 形状），删除**确实**救了它；(b) `:1842` 的 silence rule——**它把这条打坏最长 10s**。其余分支都不受影响：同实例快路径（`:1700`）与定性空闲分支（`:1762`）无论 holder 在不在都给裸名。
- **修法**：**不要 Delete，改成 tombstone**。存 `leaseGrant{instanceID:"", grantedAt:<保留原 grantedAt>, released:true}`（或等价的一位），使得 (i) `priorHolderSpoke` 仍读作 true、silence rule 在整个 drain 窗口继续工作；(ii) `:1717` 的 grantWindow 分支对已释放的 holder **跳过**——那正是删除唯一买到的好处。注意 `assignLeaseName` 的 offer 标记也是 `instanceID==""` 的 `leaseGrant`，tombstone 需要与它区分（加显式 bit，不要靠空 instanceID 区分）。
  验收：无 farewell 与有 farewell 两种情形**都**必须保住裸名。

### B7 — 升级 marker 被绑到**只能跨 `syscall.Exec` 存活**的进程血统上：升级窗口内被 supervisor 重启一次，marker 永久搁浅，之后某次开机**静默降级**

- **文件**：`internal/agent/upgrade_state.go:517-535`（`markerTargetsThisAgent` 的 `TargetInstance` 分支）；消费者 `:569`（`armUpgradeWatchdog`）、`:604`、`:651/:663`、`:708/:744`；写入点 `internal/agent/upgrade.go:598`
- **场景（不涉及任何 clone，普通单 agent 设备）**：
  1. `tether node upgrade gpu1` → marker `pending{UpgradeID:u, Deadline:+120s, TargetInstance:i1}`，`reExecInPlace` 把血统带过去；
  2. 在这 120s 内进程被 **supervisor** 而非 exec 重启——主机重启、`systemctl restart`、`Run` 返回错误后退出（round-1 B4/#7 那个 auth 拒绝路径会让 `connectNATS` FATAL）、OOM、teardown ladder 的 exit 91。systemd 给的是干净环境，`TETHER_INSTANCE_ID` 不存在，`mintInstanceID` 铸出**新 id i2**；
  3. 该进程 sid/nid/二进制/boot proof 全对，但 `markerTargetsThisAgent` 为假：`armUpgradeWatchdog` **静默 return**（一行日志都没有）、`upgradeRegisterReport` 返回 `""`、`commitUpgradeAfterRegister` 空转。**marker 永远停在 pending，仓库里没有任何东西 GC 它**；
  4. deadline 过去时没有 watchdog，所以操作员正在看的那一刻什么也不会发生。**下一次**该二进制开机（几小时或几周后的任意一次重启）`decideBoot` 走 `!now.Before(m.Deadline)` → `executeRollback` 把 `.prev` 覆盖回去并 exec → **节点静默降级**，而它写下的 `rolled_back` marker 同样无人可认领，于是也不会被上报。
- **后果**：这是**普通单实例设备上的 I1 违反**——升级看起来成功（`waitForUpgradeCommit` 轮询 `node.list` 的 ReleaseVersion，重启后的进程确实报了新版本），然后在一个不相关的时刻悄悄回退。另外三个 commit 证据（BootCount、boot proof、running-image sha）**都是重启可存活的**，唯独这一个按构造随进程死。
- **修法**：让 marker 的所有权**可被真正在跑 staged 镜像的活进程接管**。首选：在 `bootUpgradeCheckLocked` 的 `bootContinuePending` 臂持久化 `BootCount+1` 时，**顺手把 `m.TargetInstance` 重新戳成本进程的 id**——boot shim 是唯一能证明"我正在为这笔事务跑 staged 字节"的地方。这需要在 `Agent.New` 之前拿到 id，即把 `mintInstanceID` 提到 boot shim 并把值传给 `Agent.New`（**必须 memoize**，二次调用会铸出第二个 id，因为它会 unset 环境变量）。
  次选（改动小得多，但**不区分 clone**）：`markerTargetsThisAgent` 接受 `m.UpgradeID != "" && a.upgradeBootProofID == m.UpgradeID` 作为血统匹配的替代——重启可存活、事务范围内，但每个启动了 staged 镜像的 clone 都会满足它，会重开这个字段本来要堵的兄弟提交洞。

### B8 — 后缀 fallback **抢在 PIN bootstrap 之前**返回：一台真实的 `<X>-NN` 设备永远拿不到 `agent_provisioning` 行，而两条 round-1 保护都建立在"真实设备有自己的绑定"这个前提上

- **文件**：`internal/authcallout/handler.go:356-372`（承重的是 `:370` 的 `return nil`，它在 `:378` 的 PIN-bootstrap 闸门**之前**）
- **场景**：一个金镜像（一个 nkey ⇒ 一个 fp），session `lab`，机器 A 注册为 `lab`，机器 B 是**第二台永久机器**（一个 agent，永远），操作员有意命名 `lab-02` 并**正确地带 `--pin` 启动**。
  B 首次 CONNECT：`Lookup(lab, lab-02)` → `ErrNotProvisioned` → 新臂 → `SplitLeaseName("lab-02")=("lab",2,true)` → `Lookup(lab,"lab")==fp` → session active → 未 fenced → `return nil`。**B 呈上的正确 PIN 从未被消费，`ProvisionWithPIN` 从未被调用，`lab-02` 永远没有 `agent_provisioning` 行，也没有 `member_joined` 事件。**
  对照：名字不是租约形状时（`lab-b`、`lab-01`）**都**建行且都发 `member_joined`——差异纯粹来自名字形状。
- **四个后果**：
  1. **名字被偷 / 双执行**：`node.claimedLeaseNames`（`internal/node/lease.go:88-113`）只在"`nodes` 行非 OFFLINE"或"有 `agent_provisioning` 行"时排除一个名字。B 没有绑定，所以 B 一 OFFLINE（重启、维护、断电）`LowestFreeSuffix` 就返回 `"lab-02"`，把真实机器的身份租给一个临时 clone；B 回来后也注册 `lab-02` —— **一个 nid 下两个 agent，每条 exec 跑两次，落在一台只跑一个 agent 的设备上**。
  2. `broker/exec.go:276` 的 `leased := bindingsKnown && looksLeased && !provisioned[n.NID]` 对 B 恒为真 → `cmd/tether/node.go:683` **终生**把 B 从 `node upgrade --all` 里悄悄剔除。
  3. 新机器入队没有 `member_joined` 审计。
  4. **连坐吊销**：`tether admin evict lab lab` 只删 A 的绑定，但 B 是**通过**这条绑定认证的，B 下次无 PIN 重连即被拒。
- **注意这是对 round-1 修法的缺陷**：round 1 的 auth lane 只标了该臂的**作用域**，没注意到它还**抢占**了 PIN bootstrap，而这恰好摧毁了 round 1 用来建两条保护的判别器（`broker/exec.go:265-274` 和 `internal/node/lease.go:44-50` 都把"真实设备有自己的 provisioning 行"写成了前提）。同时这是 I1 违反：这台只跑一个 agent 的设备**不再与改动前逐字节一致**（改动前它首次 `--pin` 连接会写下绑定）。
- **修法**：**不要用"没有绑定"来定义"是租约"**——wire 上已经有权威信号。agent 报 `NodeRegisterReq.LeasedNID = nidOf(a) != a.cfg.NID`（`agent.go:1348`），对真实的 `lab-02` 为 false、对持租约的 clone 为 true。把它在 register 时持久化（`nodes` 列，或复用现有 upsert），两条保护都改用它：`broker/exec.go:276` 用持久化的 register-time flag；`node.claimedLeaseNames` 额外排除**最后一次 register 说 LeasedNID=false 的任何 nid，不论 status**（这条腿才是阻止"离线的真实设备名字被发出去"的那条）。
  若本轮只想小改：至少让 fallback **不吞掉已提供的 PIN**——`pin != ""` 且名字是租约形状时先跑普通 bootstrap；但这单独会重开下面 m13 的后缀持久化洞，所以 register-time flag 才是正解。

### B9 — 已经带 `-NN` 的 basename 被 `BasenameOf` **塌缩**：clone 被发一个它必须拒绝的名字，而拒绝臂**永远重建**——pod 永不出现在 `node ls`，并以 ~10 次/秒 敲 auth_callout

- **文件**：`internal/broker/broker.go:1954-1957`（`assignLeaseName` 的 `proto.BasenameOf`）+ `internal/agent/instance.go:413-433`（`acceptableLeaseName`）与 `:346-362`（refuse 臂）
- **场景（已实测）**：镜像的 `agent.yaml` nid 是 `gpu-02`（**本仓文档自己的示例车队就是 `gpu-01 gpu-02 gpu-03`**，而增量的前提就是克隆一台既有设备的镜像）。启两个：A 持 `gpu-02`；B 注册被 contested，`assignLeaseName` 算出 `basename := BasenameOf("gpu-02") = "gpu"`，发 **`gpu-04`**。agent 的 `acceptableLeaseName` 要求 `SplitLeaseName(assigned).base == a.cfg.NID`，即 `"gpu"=="gpu-02"` → false → refuse 臂：latch rebuilding、`time.After(RegisterRetryInitial)`（**100ms 定值，无指数退避、无上限**）、返回 true；`Run` 的循环（`agent.go:855-864`）**不加任何退避**，于是循环是"完整 `nats.Connect` + auth_callout + register + 被拒"，约每秒数次，永远。`maxLeaseAdoptions` 拦不住——它只数 **adoption**，而这里从不 adopt。
  实测输出：`broker offers assigned_nid="gpu-04" (basename collapsed to "gpu")`。
  **同一个无限拒绝循环不需要任何命名把戏也能到达**：`assignLeaseName` 返回拒绝形状时（basename > 29 字符，例如 `jupyter-ziyang10-production-01`；或后缀空间耗尽）实测 `assigned="" code="nid_lease_unavailable"`，走同一条臂。
- **后果**：违反用户设定的硬约束——**持有凭据就是加入的权利**。实例活着、健康、`node ls` 里看不见、任何动词都寻址不到，操作员侧唯一信号是两行以 10Hz 重复的日志。更糟的是每次迭代都是一次真实的 CONNECT 走 auth_callout（JWT 铸造 + DB 查询），N 个卡住的 clone 就是对 broker auth 路径的**自伤 DoS**。`assignLeaseName` 的注释说拒绝意味着挑战者"keeps running under the name it presented and logs loudly"——它没有，`applyLeaseVerdict` 拆了会话。plan Q2 承诺超长 basename 是"grandfathered 到单实例 + 一个 typed 拒绝"，读起来是干净的终态拒绝，不是热循环。
- **修法（两条独立、都需要）**：
  1. **不要塌缩**：租约命名空间必须从 **agent 自己配置的 basename** 派生，而不是 `BasenameOf(presented)`。要么在 wire 上带上 basename（agent 本来就知道 `cfg.NID`），要么只在"呈上的名字是**本 broker 发出过的**租约"时才做 `BasenameOf`。`gpu-02` 的 clone 应当被发 `gpu-02-02`（`acceptableLeaseName` 接受），或者一个 typed 的永久拒绝。
  2. **让 refuse 臂终态化**：拒绝之后不得以固定间隔重连——最低指数退避到 `RegisterRetryMax`，理想是一个响亮的终态（重新 register 不可能改变裁决）。

---

## 3. MAJOR

### M1 — `dropLease()` 从不重新挂回 state store：回落到 basename 的 agent 终生持有一个**空转的 state.json**（四条 lane 独立发现）

- **文件**：`internal/agent/instance.go:466-478`（`dropLease`）vs `:508-511`（`adoptRoutingNID` 的单向 `detach`）；开关在 `internal/agent/state.go:116-120`，闸门在 `:139/:153/:294`
- **场景**：`detached` 是单向 `atomic.Bool`，**全包内没有任何 attach**。而 adoption 是可逆的：auth 拒绝租约名时 `connectNATS`（`agent.go:1257-1272`）调 `dropLease`，同一进程以 basename 重连——这正是 round-1 B4/#7 为 broker 回滚 / 集群里某个未升级的 auth_callout queue 成员建的路径。incumbent 已死（回滚的常见形状）时，本进程会被授予裸名，**成为合法的 basename 持有者，却带着一个死存储**。
- **后果（三条，全部无声，没有任何日志说持久化关了）**：
  (a) `replayPortsUnlessLeased` 现在走 basename 分支 → `replayPortsFromState` 读到空 `StateFile` → **一个端口都不重放**，而 `reconcile.go:363` 故意把未重呈的端口留在 ALLOCATED，离线回收器对 ONLINE 节点**永不触发** → 该设备的每一个 expose 永久黑洞；
  (b) 之后每次 `expose`（`handleExposeForwarded` → `AddPort`）对 broker 回 OK 却什么也不落盘，下次重启即蒸发而分配还在；
  (c) `failClosedFire` → `proxyTeardownLocked(clearPersist=true)` → `SetProxy(nil)` 变空操作，"15 分钟分区后不得留下可用于 bootstrap 的足迹"这条 fail-closed 保证被静默作废。
- **实测**：`adoptRoutingNID(a,"lab-1-02"); dropLease(a)` 之后 `nidOf(a)=="lab-1"`，但 `buildLocalSnapshot()` 报 0 个端口（磁盘上有 1 个），随后的 `AddPort` 返回 nil 且不落盘。
- **修法**：加对称操作并从 `dropLease` 调用：`func (s *stateStore) attach() { s.detached.Store(false) }`，在 `dropLease` 里紧挨 `routingNID.Store(&base)` 调。更稳的形式是把 bool 换成谓词（存储持有一个 `owner func() bool`，闭包读 `nidOf(a)==cfg.NID`），使两者不可能不一致。守卫（变异：删掉 attach 就变红）：adopt→drop 之后 `AddPort` 必须落盘、`buildLocalSnapshot` 必须重新呈报。

### M2 — **REFUSED** 裁决从不 detach，于是被拒绝的 clone 的 fail-closed 计时器仍会擦掉 basename 持有者的 proxy footprint

- **文件**：`internal/agent/instance.go:346-362`（refuse 臂，空名分支）——没有 detach；写入经 `internal/agent/proxy.go:757` `failClosedFire` → `:572` `clearPersist` → `SetProxy(nil)`
- **场景**：`detach()` 只经 `adoptRoutingNID` 到达，即只在 agent **采纳**名字时。而 refuse 臂（broker **已证明**另一个活进程持有该名，只是发不出租约名）保留 `cfg.NID`、latch rebuilding、永远重试——**存储始终是 attached 的**。拒绝并不罕见：`internal/node/lease.go:66` 在 `len(basename) > proto.MaxLeaseBasenameLen(29)` 时**确定性地**永远返回 `ErrLeaseUnavailable`，nid 30–32 字符的设备的**每一个** clone 都走这里（还有 64 实例上限与任何 DB 错误）。
  交织：clone B 在线健康 → broker 重启/链路断 → `armFailClosed` 起 15 分钟倒计时 → redial watchdog 强制 rebuild，而 rebuild **故意保留**倒计时（`agent.go:1040-1046` 的条件 defer）→ 重建后的会话 register 被拒（空 `AssignedNID`）→ `applyLeaseVerdict` 设 `rebuildRequested`，条件 defer 再次跳过 `cancelFailClosed`，而 `agent.go:977` 的注册后 `cancelFailClosed` 永远到不了 → 15 分钟后 `failClosedFire` 以 `p.srv==nil` 跑 `proxyTeardownLocked(clearPersist=true)`，**重写共享 state.json 并删掉 incumbent 的 proxy 足迹**。
- **后果**：round-1 那条守卫（`TestLeasedInstanceFailClosedDoesNotWipeTheSharedProxyFootprint`）只修了一半——结构性治法绑在了 adoption 上，而裁决有三种结果（grant / adopt / refuse），**refuse 恰恰是 broker 已经证明文件属于别人的那一种**。incumbent 从内存继续服务，直到它重启才发现足迹没了，而 keyset-only bootstrap 臂没有足迹可开。
- **修法**：两条互补。窄：`proxyTeardownLocked` 的 clearPersist 写入加条件——本进程真的拥有该足迹（`p.token != "" || p.srv != nil`）。结构：**REFUSED 在文件所有权上等同 LEASED**，在 `applyLeaseVerdict` 的空名/不可接受名臂里也 detach，与 M1 的 `attach()` 配对（后续被授予 basename 时重新挂回）。

### M3 — `Agent.New` 在应用已恢复的租约**之前**写共享 state.json（`ProxyOptOut` 清理）

- **文件**：`internal/agent/agent.go:740-742`（`SetProxy(nil)`）vs `:791-796`（`routingNIDEnv` 恢复 → `adoptRoutingNID` → detach）
- **场景**：`New()` 在 `:733` 建 store，若 `cfg.ProxyOptOut` 则立刻 `SetProxy(nil)`——对 `<home>/agent/<sid>/state.json` 的一次完整 read-modify-write。而确立"本进程是租约实例"的恢复在 50 行之后。**一个从 `syscall.Exec`（node upgrade / rollback）回来、已经确定持有 `gpu1-02` 的实例，会在能够 detach 之前写共享 inode。**（对尚未裁决的 clone 也一样写，即根本还没有任何租约裁决时。）
- **后果**（参考部署里 `~/.tether` 是同一个 NFS inode）：(1) 滚动换镜像（旧镜像参与 proxy、新镜像 `proxy.participate:false`）时，新 clone 在启动时抹掉**仍在运行的** incumbent 的 proxy 足迹，incumbent 下次重启无法从 state.json bootstrap——正是 round 1 判为真实缺陷的那类损害；(2) 这个 RMW 在共享文件上是 lost-update 窗口：clone 读 port_tokens → `MkdirAll+CreateTemp+write+fsync+rename`（NFS 上几十毫秒），incumbent 在此期间提交的任何 `AddPort` 被销毁——而那一行是原始 tunnel token 的**唯一**持久副本。
- **实测**：独立 store 写入 incumbent 的 port + `ProxyState{14000, tok-incumbent-proxy}`，然后 `t.Setenv(TETHER_INSTANCE_ID/TETHER_ROUTING_NID)` 调 `New(Config{ProxyOptOut:true})`：`nidOf(a)=="lab-1-02"` 且文件里的 proxy 足迹已为 nil。
- **修法**：把 `ProxyOptOut` 清理移到 `routingNIDEnv` 恢复块之后；更好是推迟到 register 裁决确认本进程拿到 basename 之后（它只需要在第一条 proxy 指令生效前完成，而那不可能早于一次成功的 register）。守卫：`New()` + ProxyOptOut + `TETHER_ROUTING_NID` 必须让共享文件逐字节不变。

### M4 — farewell 的 `probeCache.Delete` 既在 holder guard **之外**，又无法取消**在飞的** background probe

- **文件**：`internal/broker/broker.go:1884`（无条件 Delete）vs `:1877-1883`（holder guard）与 `:1573-1598`（single-flight goroutine）
- **场景 A（guard 作用域）**：竞争失败拿到拒绝形状的 clone 仍以它呈上的名字运行，`nidOf(a)` 还是 `gpu1`，`PermissionsForAgent(sid,"gpu1")` 给了它在 incumbent 的 register subject 上的 pub 权。这个 clone 之后被停止时，`releaseLeaseName` 会带**自己的** instance id 在 incumbent 的 subject 上发 farewell：holder guard 正确地拒绝删除 holder，**而 `:1884` 照样丢掉 incumbent 的 probe 结论**。若它落在"后台 probe 写入结论"与"挑战者重试读取结论"之间，重试就 miss cache、再起一个 3s probe、再吃一个 `CodeLeaderUnavailable`——正是 `probeTTL` 算术（`:1492-1503`）要防的 probe-per-retry 活锁。
- **场景 B（与在飞 probe 竞争）**：farewell 与 single-flight goroutine 之间**没有任何同步**。WAN 参考部署上（15ms `inlineProbeGrace` 经常错过）：t=0 broker 重启，A 重注册落到歧义臂 → 起 3s 后台 probe，A 收到 transient；t=0.3 A 优雅停止 → farewell 删掉 holder 与 cache；t=0.5 后继 A2 注册 → `ErrNoResponders` → 正确保住 `gpu1`；**t=3.0 那个 goroutine 超时并写入 `{answered:false, definitive:false}`（UNKNOWN），有效期 10s**。此后该名字上任何第三次 register（clone，或又一次清空 `leaseHolder` 的 leader 变更）读到复活的 UNKNOWN：`held=true, known=false`，而 `priorHolderSpoke` 因为 farewell 删过 holder 是 false，silence rule 无法触发 → 被加后缀。`probeInFlight` 也没被 farewell 清掉，所以 Delete 连"取消将来的写"都做不到。
- **修法**：(1) 把 `probeCache.Delete` **移进** `g.instanceID == req.InstanceID` 的块内（若采纳 B6 的 tombstone，则并入同一次受保护的写）；(2) 给缓存一个 epoch/token：启动后台 probe 时 `probeInFlight.Store(key, token)`，goroutine 写入前 `if v,ok := probeInFlight.Load(key); !ok || v != token { return }`，farewell 同时删 `probeInFlight[key]`，从而作废在飞写入。

### M5 — `SetNID` 的 retire 不是 fence：在旧名下已 dial/REGISTER 的 `Open`/`OpenHome` 在 swap **之后**安装自己，继续桥接本进程已不持有的名字

- **文件**：`internal/tunnel/tunnel.go:1009-1012`（map swap）、`:1123`（`dialAndRegister` 在锁外）、`:1133-1170`（安装处只重查 `c.ctx.Err()`）、`:1274`（REGISTER 行读 `c.nidValue()`）；`internal/agent/tunnel_adapter.go:69`
- **场景**（已用失败测试演示）：真实 `tunnel.Server`，`TokenLookup` 阻塞；goroutine 调 `cli.Open(publicPort, localPort, token)`；REGISTER 到达（被阻塞的）lookup 后调 `cli.SetNID("lab-1-02")` 再放行 lookup —— 客户端**仍持有 publicPort，且是以已退休的名字 `lab-1` 安装的**。
  生产交织两条 goroutine 都是真的：nats.go 在调用 ReconnectHandler **之前**重放订阅，`agent.go` 又把 `onNATSReconnect` 放到自己的 goroutine 上；于是 (a) `onNATSReconnect → requestLeaseRebuild → adoptRoutingNID → TunnelExposeAdapter.SetNID`（**不取 `a.opMu`**）与 (b) `dispatchForwarded → handleExposeForwarded → AddProxy → OpenHome`（dial 5s + 读超时 5s 的窗口）并发。`ApplyHome`（`:1210-1230`）同样：在锁内取 localPort/token 快照、释放、再 OpenHome。
- **后果**：被降级的实例桥接节点 `foo` 的公网端口 P 而自己已是 `foo-02`；服务器侧安装**只按 public port 键控**，所以它顶掉 incumbent 的活会话，incumbent 自己 redial 时反而丢掉端口绑定。只在该会话下一次传输中断时自愈——正是 round 1 加 `SetNID` 要关掉的"只被下一次传输中断兜底"的窗口。`SetNID` 的文档注释（`:985-990`）断言了相反的事，所以别处没有任何防御。
- **修法**：把 retire 做成 **fence**。`Client` 加 `nidGen uint64`，在 `SetNID` 里**与 map swap 同一临界区**内自增；`OpenHome` 在 `dialAndRegister` 前快照 `nidGen`（及即将呈上的 nid），在 `:1133-1136` 的安装临界区里发现代次变化就**按 ctx 分支同样的方式回滚**（解锁、cancel、关 conn+yamux、返回）。`ApplyHome` 自动继承。**不要**用"在 `TunnelExposeAdapter.SetNID` 里取 `a.opMu`"代替：那会让 `SetNID` 在 gotcha #72 要限界的 teardown 路径上阻塞整个 dial 预算，且覆盖不到 adapter 之外的 `Client` 调用方。

### M6 — `SetNID` 的 retire **静默**杀掉受监督会话（无 session-state 下降沿），于是 `proxyTunnelUp`/`ProxyBound` 镜像在一条死隧道上恒为 true

- **文件**：`internal/tunnel/tunnel.go:1030`（`sess.cancel()`）与 `:1317-1325`（supervise 的 `if ctx.Err() != nil { return }` 跳过 `notifyState(port,false)`）；`internal/agent/agent.go:652-660`、`internal/agent/proxy.go:137`
- **场景**（已用失败测试演示：`SetSessionStateHook` + `waitRoundTrip` 后 `SetNID`，500ms 内**零**边沿）：broker 重启/选举清空 `leaseHolder`；活着的 clone 应答 claim probe，于是以 basename 运行、内嵌 SS proxy 正在 1080 上服务（`p.srv != nil`、`proxyPublicPort=1080`、`proxyTunnelUp=true`）的 incumbent 被判 contested → `requestLeaseRebuild → adoptRoutingNID → SetNID("foo-02")` **静默**退休 1080 的会话。rebuild teardown 不停 SS server，`p.srv` 仍非 nil，`proxyBound()` 继续返回 true：**每一次心跳都为一条不存在的隧道报 `ProxyBound=true`**。若之后 `dropLease` 回到 basename，register 回复带**相同的 (gen,epoch)** 指令，`proxy.go:251-256` 走完全相等的 re-ACK 臂 `pubProxyReady(nc,true)`，为一个没有反向隧道的公网端口发 READY。
  对比：其他每一个有意杀会话的地方都显式补偿（`proxyTeardownLocked` 在调 `RemoveProxy→Close` **之前**写 `proxyPublicPort=0`/`proxyTunnelUp=false`，`proxyFailCleanupLocked` 同）。
- **后果**：`#33`/R8a 那一类缺陷从反方向复活——标志卡在 TRUE，`/sub` 供出一个黑洞出口，集群跨 broker 的 proxy_ready 收敛信号是假的。没有任何东西复位它。操作员的恢复手段是 `proxy off; proxy on`。
- **修法**：retire 循环里在 map swap 之后、`c.mu` 之外，为每个退休会话 `c.notifyState(sess.publicPort, false)`（agent 侧 hook 已按 proxy 端口过滤，普通 expose 不受影响；supervisor 因 ctx 已 cancel 不会重复触发）。守卫：断言 `SetNID` 退休的每个端口都触发了下降沿。

### M7 — 降级后 incumbent 的 expose 分配**永久搁浅**在 ALLOCATED，没有审计、回收器永远够不到

- **文件**：`internal/agent/instance.go:271`（`replayPortsUnlessLeased`）+ `internal/tunnel/tunnel.go:1009-1041`（retire）+ `internal/broker/reconcile.go:362-370`
- **场景**：P 持 `gpu1` 且在 14001 上服务 expose `web`（分配行 nid=gpu1、ALLOCATED，token 在它自己的 state.json 里）。P 卡在 #72 形状（本车队实测 10m58s）或链路断超过 `LeaseGrace`；clone C 拿到 `gpu1`；P 回来被降级为 `gpu1-02`：`SetNID` 退休 14001 的会话，broker 的 `publicAcceptLoop` 移除并关闭公网监听——**端口从此拒绝连接**。此后：(a) P 永远无法再服务 14001（它以 gpu1-02 注册，`tunnelTokenLookup` 要求 `alloc.NID == presented nid`），而且它的 store 已 detach，永不重放；(b) C 只重放**自己的** state.json——独立磁盘的 clone（本增量点名的烤镜像形状，以及任何在烤镜像之后新建的 expose）没有那个 token；(c) broker 按 `reconcile.go:363` 把未重呈的端口留在 ALLOCATED，而 `port.ListAllocatedForOfflineNodes` 要求 `nodes.status='OFFLINE'`——`gpu1` 因为 C 在心跳**永远不是** OFFLINE，**15 分钟回收器永不触发**。
- **后果**：`tether expose ls` 一直列着 `gpu1:14001`，连它永远被拒；端口号永不回到分配段；`tether expose --name web` 报 name_taken。整条路径上**没有任何 audit.port 事件**（发射器只有 allocated/freed/revoked/reconciled），操作员问"我的 expose 怎么死的"在审计流里找不到任何东西。`replayPortsUnlessLeased` 自己的注释说"错误的跳过会永久黑洞掉端口"——这次跳过按租约规则是对的，端口照样黑洞。
- **修法**：让降级**可观测、可回收**。首选：裁决降级一个已有 ALLOCATED 端口行的 nid 时，broker 要么 (a) 在降级时 REVOKE 这些行并发审计事件，要么 (b) 把它们改档到新租约名，让实例 rebuild 后可以重呈。最低限度：agent 侧在 adoption 时以旧名 best-effort 发一次端口释放（与 farewell 同形状、同"严格尽力而为"契约），并对每个搁浅的 (port,name) 打 WARN + audit。若本轮都不做，必须把这条写进 §7 的 I2 账本并给出手工补救（`tether expose rm <name>`）——现有账本措辞覆盖不到"永久 ALLOCATED 但无人服务"的行。

### M8 — fail-closed orphan gate **永不收敛**：DB 重建 / session 重建后活着的 PTY 进程既看不见也杀不掉

- **文件**：`internal/broker/reconcile.go:313-321`（`sawAnyRow` 拒绝）、`:436-449`（`anyRowMatches` 的注释）
- **场景**：会抹掉行但不停进程的前置条件：broker 的 SQLite/raft 状态被重建或恢复（**本车队 2026-08-11 就这么干过**——racknerd 抹掉重建）、session 被删后同 sid 重建、agent 指向了错误的 broker（`killOrphanProcess` 的注释自己点名的情况）。agent 有活的 `tether run` PTY 子进程；register 时该 nid 无任何行 → `sawAnyRow=false` → 每个上报 pid 在 `:320` `continue`，只有一条 Warn，没有任何指令。**永远修不好**：进程行只由 agent 的 `proc.started` 事件创建（早已 ack 且不会重发），reconcile 自己不建行，所以下一次 register 产生同样的拒绝，永远。进程继续跑并占着 GPU，`tether ps` 什么也不显示（没有行），`tether kill` 无法寻址（没有行可查）。改动前第一次 register 就会把它们 SIGTERM+SIGKILL 掉。
- **后果**：`anyRowMatches` 的注释用"拒绝杀最多留下一些行，下一次 register 会 reconcile"来论证这个 gate——**没有行，也没有能建行的下一次 reconcile**，所声称的自愈不存在，gate 变成无条件、无期限的赦免。它把增量选择的安全方向变成了恰恰在事故恢复路径上的静默、无界资源泄漏。
- **修法**：把赦免收窄到它被加进来的那个情形并让它可见：(a) 仅当本次 register **确实是租约事件**时才 fail-closed —— `req.InstanceID != "" && (req.LeasedNID || req.PreviousNID != "")`，让普通 agent 在被抹库/错 broker 上恢复改动前的 kill；(b) 发 audit/sys.event（如 `orphan_kill_declined`）而不只是 Warn，让操作员能看到"哪些 pid 在跑但没有行"，而不是靠 GPU 账单发现。

### M9 — boot budget 与 marker 是 **host 级**事实，而本增量的 clone 群共享一个二进制目录：兄弟实例的抖动可以回滚一次健康升级、烧掉别人的预算

- **文件**：`internal/agent/upgrade_state.go:449`（在共享 marker 上 `m.BootCount++`）、`:305-318`（`decideBoot`）、`:400-410`（把 identity-free boot 论证为"host 级事实"的注释）
- **前提**（增量自己的测试注释 `internal/agent/upgrade_marker_target_test.go:11-19` 与 plan §0.6 的 spike 都这么说）：clone 共享 NFS 上的 `$HOME`，`install.sh` 的 `BIN_DIR=$HOME/.local/bin`，于是**一个二进制、一个 `.prev`、一个 marker、一个 BootCount**。
  - **deadline 臂（更可能的一支）**：实例 A 升级，其 marker 因任何原因（B7，或 A 直接死了）停在 pending 且过了 deadline。此后**任何 clone 的任意一次启动**命中 `!now.Before(m.Deadline)` → 该 clone 把 `.prev` 覆盖回**共享**二进制并 exec —— 每个实例在下次启动时都回退到上一个 release，而做这个决定的进程从未参与那次升级。
  - **budget 臂**：A 升级、register 慢（弱 NAT，正是 deadline 定成 120s 的理由）。A 自己的 exec-boot 写 `BootCount=1`；快速循环的兄弟实例（增量的前提就是"来去很快"）再启动 3 次把它推到 3，第 4 次启动返回 `bootRollback`，**摧毁一次健康的升级**。A 自己的 watchdog 随后空转（state 已不是 pending），并为一次从未不健康的升级上报 `rolled_back`。
- **后果**：效果响亮（整片回退）但归因静默——rollback detail 里的 `boot_count` 无人能解释。round 1 完全没有审 boot 路径。
- **修法**：只统计**认领了 marker 的**启动（与 B7 的所有权戳记同一处改动）：非所有者的启动既不烧预算也不做回滚决定。若不采纳所有权戳记，至少在 marker 里记录每次 boot 增量来自哪个进程，并在增量全部来自从未注册过的进程时拒绝预算回滚；同时考虑：由非所有者触发的 deadline 回滚应当**恢复二进制但不作为对目标的判决**（今天它是判决，目标随后上报 `rolled_back`）。

### M10 — 经 supervisor 重启的 rollback（即本功能存在的那个 crash-loop 场景）**什么都不上报，marker 也永不清除**

- **文件**：`internal/agent/upgrade_state.go:663`（`upgradeRegisterReport` 的终态臂）、`:744`（`commitUpgradeAfterRegister` 的终态清理）；写入方 `:464-478` 与 `executeRollback :325-360`
- **场景**：新二进制坏掉、启动即退。P2（血统 i1）BootCount=1 → 退；systemd 重启 → P3 **全新 id**、BootCount=2 → 退；P4 全新 id、BootCount=3 ≥ 预算 → `decideBoot=bootRollback`。boot 路径是 identity-free 的（正确、按设计），所以回滚**确实发生**：恢复 `.prev`、写 `marker{state:rolled_back, Detail:"boot: budget/deadline exhausted (boot_count=3/3 …)", TargetInstance:i1}` 并 exec 旧二进制。那次 exec 的 `execEnv()` 无可携带（`Agent.New` 之前 execLineage 为空、systemd 环境无 `TETHER_INSTANCE_ID`），于是恢复后的进程又铸出新 id。它第一次 register：终态臂 `markerTargetsThisAgent` → false → 返回 `""` → **broker 永远不会记录 `agent-reported upgrade outcome`**，`commitUpgradeAfterRegister` 也不 `os.Remove` marker，于是一个陈旧的 `rolled_back` marker 永久留在盘上并在此后每次 register 被再次抑制。`bootConvergeRolledBack` 与 `bootMarkOrphan/rollback_failed` 两个 identity-free 写入方同病。
- **后果**：终态臂**唯一**的闸门就是 `markerTargetsThisAgent`（BootCount / boot-proof / running-image-sha 三条只守 pending 臂），所以收窄它在这里是满效力的。丢掉的那行日志是"升级为什么失败"的唯一机器可读记录：`tether node upgrade --wait` 超时后打印 "still `<old>` after deadline — likely ROLLED BACK (check agent log / broker log)"，把操作员指向的正是这条被删掉的记录。更糟的是 `rollback_failed`（节点跑的二进制两个记录的 sha 都不匹配，例如 prev 槽不可用）也被同样丢掉——而那一条是操作员**必须**看到的。改动前这条路径上报是正确的，所以这是纯粹的可观测性回归，且同样发生在单 agent 设备上（I1）。
- **修法**：由 identity-free 的 boot 路径决定的终态是**关于二进制的 host 级判决**，不是某个实例的主张——它的所有者按定义已经不在了。所以从 `bootUpgradeCheckLocked` 进入 `executeRollback` 时（以及 `bootConvergeRolledBack` / `bootMarkOrphan` 两臂）写终态 marker 时**清空 `TargetInstance`**，让它退回改动前的 (sid,nid) 回退规则从而恢复上报。进程内路径（`watchdogRollback`、`recoverFromFailedExec`）保留血统戳记，那里确实有活的所有者。（若按 B7 在 boot 时重新戳所有权，这条随之解决。）

### M11 — `leader_unavailable` 重试分支**编造 raft 起因**并丢弃 broker 的真实原因：每一次租约延迟都被记成一次在单模 broker 上不可能发生的 failover

- **文件**：`internal/agent/agent.go:1405-1410`（`case resp.Code == proto.CodeLeaderUnavailable`）；broker 侧 `internal/broker/broker.go:1917-1922` 与 `:1930` 无日志
- **场景**：单模 broker（**就是现网车队**，cluster 关闭、无 raft）被重启——部署、`systemctl restart`、broker 升级。每个 agent 的 nats.go 重连触发 `onNATSReconnect → a.register`。broker 的 `leaseHolder` 只在内存，重启后是**空的**：连 incumbent 自己的重连也 miss 同实例快路径，落到歧义 / age>grace 臂。`leaseProbe` 用 15ms 的 `inlineProbeGrace` 问名字归属；nats.go 在 ReconnectHandler 之前重放订阅，所以本地 agent 能正确应答——但**任何 RTT 超过 15ms 的 agent**（VPS broker 的所有远端 agent；代码注释自己提到洲际 RTT）都错过 grace，于是 `leaseReasonProbePending` → `CodeLeaderUnavailable`（携带准确字符串 "adjudicating this name against the current holder; retrying shortly"）。agent 随即打 WARN `agent: register hit a transient leader failover; retrying`，快的情况一次、holder 沉默时最多 ~6 次（100/200/400/800/1600/2000ms 覆盖 3s 预算），**并且从不打印 `resp.Error`**——而两个 case 之下的 `CodeReplyTooLarge` 分支是打 `"detail", resp.Error` 的。
- **后果**：这发生在**从未有过 clone 的普通单实例设备**上（review §3c "no new client behaviour" 的说法对可观测日志是假的），全车队，在最常见的维护事件上。消息点名了一个单模下不可能的原因，而 `docs/cluster.md:430` 把这个码定义为 `raft leader 切换 / 选举中`，于是操作员被文档指引去在一个关闭了 cluster 的 broker 上跑 `tether cluster status`——死路，且没有任何证据指向租约裁决。
  另外：复用这个码的理由（N-1 兼容）是空的——`adjudicateLease` 在 `broker.go:1682` 对任何空 `InstanceID` 的 register 直接返回，**pre-feature agent 永远不可能收到这个码**。
- **修法**：保留 wire 码（无害，重试语义是对的），但把消息改诚实：像下面两个 case 那样打 `"detail", resp.Error`，并把文案改成无起因的一句（"agent: broker asked us to retry the register (transient); retrying"）。若确实想要独立起因，这里加新 transient 码是安全的（pre-feature agent 永远不被裁决），只需在这个 switch 里 `CodeLeaderUnavailable` 旁加一个 case 加一行 exitTransient。
  **配套（原 #11）**：broker 侧的 pending 臂**一行日志都没有**，而四行之下的 degrade 臂打 Warn。`leaseReason*` 常量的注释说它们存在是"so an operator can grep for them"，`leaseReasonProbePending` 是唯一定义了却从不写日志的那个。加一条 Debug/限流 Info，带 sid/nid/`req.InstanceID`。

### M12 — `leased_instance`：判据只看名字形状（round-1 在另一处修掉的 fail-open），且它被设在**人类视图永不渲染**的字段上

- **文件**：`internal/broker/proxy.go:1108-1109`（`if _, _, leased := proto.SplitLeaseName(r.nid); leased`）；渲染侧 `cmd/tether/proxy.go:192`（只在 `resp.ClusterState != ""` 分支打 REASON）vs `:203-209`（实际走到的分支）
- **(a) 判据错**：`proxyStatusNodes` 对任何名字符合租约文法的行盖 `ProxyReasonLeasedInstance`，**不检查它是否真的是租约**。`gpu-01 gpu-02 gpu-03` 是本仓文档自己的示例车队。实测：一台**真实**设备以 `gpu-02` 注册（`LeasedNID=false`）、`proxy_capable=1`、拿到指令与公网端口、`SetProxyReady(true)`，`proxyStatusNodes` 返回 `{NID:gpu-02 Ready:true PublicPort:14000 ReadyReason:leased_instance}` —— 一行同时说"正在 broker.example.com:14000 服务"和"因为是租约所以不合格"。round 1 已经在 `handleNodeListReq`（`internal/broker/exec.go:257-276`：`bindingsKnown && looksLeased && !provisioned[n.NID]`，fail-closed）修过同一个谓词，`node.ProvisionedNIDs` 正是为这个问题而存在，而 `proxyStatusNodes` 从不调它。
- **(b) 渲染不到**：`ProxyReasonLeasedInstance` **只**在 `proxyStatusNodes`（非集群构建器）里设置；`cmd/tether/proxy.go` **只**在 `resp.ClusterState != ""` 时打 ReadyReason，而 `ClusterState` 只在 `b.clusterMode && req.Cluster` 臂设置，那条臂用 `proxyStatusNodesCluster` 建行并从 `proxyReadyFor` 覆盖 ReadyReason（且它 `WHERE proxy_capable=1`，租约实例 `proxy_capable=0`，行直接不存在）。净效果：这个字符串**只能经 `--json` 到达**。现网单模车队里操作员看到的是
  `lab-1-02  ONLINE  false  -`，与 SS 崩了或隧道断了逐字节相同。兄弟情形是处理了的（`:208` 把 OptedOut 变成 `exit = "opted-out"`）。
  该守卫（`TestLeasedIneligibilityIsDistinguishableFromTheOptOutHint`）之所以绿，是因为它断言 `b.proxyStatusNodes(sid)` 返回的结构体，从不断言渲染出的字节。
- **后果**：plan §2 Q1 要求这个不合格性**以自有 reason 值披露**，正是为了不让操作员去找一个烤镜像里根本不存在的 agent.yaml 键。现状两头落空：默认人类视图里这个刻意的限制不可见；而一旦把渲染修好、判据不修，**操作员自己的硬件会在默认视图里被贴上"临时克隆"的标签**。
- **修法**：两半一起改。判据：把 `provisioned, bindingsKnown := node.ProvisionedNIDs(b.read().SQL(), sid)` 提到行循环之上（一次扫描，和 `handleNodeListReq` 一样），仅当 `bindingsKnown && looksLeased && !provisioned[r.nid]` 时设 reason——最好把这个两条件谓词抽成一个 helper 供两处调用，防止再次漂移。渲染：仿照 OptedOut，在默认表里 `if n.ReadyReason == proto.ProxyReasonLeasedInstance { exit = "leased" }`（或无条件加 REASON 列），并让 `proxyStatusNodesCluster` 也设同一个 reason（或别把租约行过滤掉）。守卫要**断言渲染出的字节**，变异是删掉那一行 / 删掉 `!provisioned[...]` 合取。

### M13 — proxy 这个 I2 例外没有任何文档，而本增量新加的 usage.md FAQ 正好断言了相反的话

- **文件**：`docs/usage.md:1782-1783`（新 FAQ）；`docs/usage.md` / `broker-ops.md` / `requirements.md` 里没有任何 proxy-租约 的文字
- **场景**：新 FAQ 告诉操作员，带后缀的实例就是多一行、`使用方式完全照旧`、`tether exec <nid>-02 -- …` 正常命中。它对 proxy 只字未提。但**每一个租约实例都永久 proxy 不合格**：`nodeParticipatesInProxy` 折入 `&& !req.LeasedNID`，`handleRegister` 把同一合取折进 `nodes.proxy_capable`，agent 侧还有一条本地保险 `leasedInstanceRefusesProxy`。grep `租约/leased` 在四份文档里都搜不到 proxy 相关内容。
- **两个只存在于 plan §7 账本与 §2 Q1（即注定要归档进 `docs/reviews/` 的过程文档）里的运维后果**：
  1. basename 持有者死掉、只剩健康的 `lab-1-02` 时，**该 session 的出口没了**——Q5 禁止提升，幸存者的 `proxy_capable` 保持 0。`tether proxy status` 显示 `lab-1 OFFLINE` 挨着 `lab-1-02 ONLINE`，而没有任何东西告诉操作员补救动作是重启幸存者让它重新认领 basename。既没有命令能修，也没有消息点名它。
  2. plan 自己的结论"clone 镜像应当出厂即 `proxy.participate: false`"（因为出口节点会明文收到每个订阅者的 Shadowsocks PSK）没有出现在任何交付文档里。
- **修法**：修订 usage.md 的 FAQ 明说这个例外（带 `-NN` 的租约实例永远不提供 proxy 出口，这不是缺陷也不是 opt-out）；写清 basename 持有者死亡导致的出口中断与"重启幸存者"的补救；给烤镜像加 `proxy.participate: false` 的建议；并把这个例外登记进 `requirements.md §7.5`（挨着租约文法），使它在 plan 归档进 `docs/reviews/` 之后仍然活着。

### M14 — `node upgrade --all` 的租约排除只在 stderr 上报一个**光秃秃的计数**，任何人类视图都查不出被跳过的是谁

- **文件**：`cmd/tether/node.go:208-216`（跳过消息）与 `:28-130`（`node ls` 表头无 leased 标记）
- **场景**：`tether node upgrade --all …` 在 stderr 打 `skipping 1 instance(s) running under an assigned lease name (they revert to the image's binary on restart)`。操作员去 `tether node ls` 查是哪一个——表是 NODE/STATUS/HEARTBEAT/PROTO/RELEASE，**没有任何租约标识**。`Leased` 位只存在于 `node ls --json`。
  这一点在设计自己接受的那个误差上咬得最狠：`adjudicateLease` 的保守臂**故意**给一台可能活着的孤立设备加后缀（"这是唯一一处给可能活着的设备加后缀反而更安全的地方——下次重启会自愈"）。那台真实设备从此被排除在每一次车队升级之外，直到有人重启它，而唯一的证据是一条管道调用永远看不到的 stderr 计数。
- **后果**：`--all` 存在的意义就是避免"操作员无法归因的部分升级"，而 `node ls` 的 RELEASE 列（§5.19 收尾明写"用 `tether node ls` 验证版本"）会显示版本歪斜且不解释原因。
- **修法**：消息里点名 nid（`skipping leased instance(s): gpu1-02, gpu1-03 — …`），并在人类版 `node ls` 表里体现租约性（LEASED 列，或在 NODE 单元格上加标记）。plan Q4 还承诺显式 `tether node upgrade <lease-name>` 会打印一行"目标是临时实例"的确认——`newNodeUpgradeCmd` 的单目标分支里也没有这行。

### M15 — 租约实例**每次重启都换一个名字**（它自己的行在 60s 内挡着旧名），钉在 `gpu1-02` 上的 exec/transfer/expose 全部落空

- **文件**：`internal/node/lease.go:95-97`（`claimedLeaseNames` 的 `status <> 'OFFLINE'`）与 `internal/node/node.go:33-34`（`DefaultOfflineAfter = 60s`）
- **场景（已实测）**：A 持 `gpu1`、B 持 `gpu1-02`。B 的 pod 重启（k8s、`RestartSec=5`、crash loop、`kubectl rollout restart`）。B 回来呈上 agent.yaml 里的 **basename `gpu1`**（后缀故意不持久化，`routingNIDEnv` 只跨 `syscall.Exec` 存活，跨进程重启不存活）。`assignLeaseName` 扫 `nodes WHERE nid LIKE 'gpu1-%' AND status <> 'OFFLINE'`——**B 自己的行还 ONLINE/STALE 最多 60s**——于是 `gpu1-02` 算被占，B 拿到 `gpu1-03`。实测输出：`the instance that was gpu1-02 five seconds ago comes back as "gpu1-03"`。
  farewell 也救不了：`releaseLeaseName` 只删内存里 `gpu1-02` 的 `leaseHolder` 条目、**故意不动 nodes 行**，而且后继呈上的是 `gpu1`。每 5s 重启一次的 clone 会在第一个名字释放前走完 12 个名字；几个这样的 pod 就能顶到 `MaxInstancesPerBasename`(64) 然后掉进 B9 的永久拒绝循环。
- **后果**：I2 承诺（"正常使用"）在这个部署形状最常见的事件上失效。usage.md 现在告诉用户 `tether exec <nid>-02 -- …` 命中该实例——但 名字→实例 的映射在 pod 重启后不稳定，于是每个脚本、每个 `tether expose gpu1-02 …` 的分配、每条 runbook 都悄悄指向一行先 STALE 后 OFFLINE 的幽灵，而它命名的那台机器正以新名字运行。farewell/silence 那套机制（"重启保住名字"）只对**裸名**实现了，租约那一半没有对等物，也没人写下这件事。
- **修法**：让回来的实例能重新认领它刚刚让出的租约名：`releaseLeaseName` 时把该 nodes 行标 OFFLINE，或记一条短寿的 "released" 标记让 `claimedLeaseNames` 尊重；或者让裁决优先选择"持有者可证明已消失"的最低租约名（复用给裸名用的那个 probe），而不是跳过每一行非 OFFLINE 的行。最低限度：在 usage.md 写明租约名只在该进程生命期内有效，任何重启后必须从 `node ls` 重新读取。

### M16 — `tether admin evict <basename>`（§5.18 教操作员对 OFFLINE 行做的清理）现在会杀掉活着的带后缀兄弟，并吊销**整个镜像车队**的凭据

- **文件**：`cmd/tether/admin.go:386-425` 与 `internal/agent/agent.go:1066-1078`（`agent_evicted` 匹配的是 `ev.NID` 对 `a.cfg.NID`）、`docs/usage.md:1170`
- **场景**：pod A（`gpu1`）与 B（`gpu1-02`）跑同一个烤镜像。A 被缩掉。Q5 禁止提升，于是 `node ls -a` 显示 `gpu1 OFFLINE` 挨着 `gpu1-02 ONLINE`——**这个状态在本增量之前不可能存在**。操作员照 §5.18 的建议跑 `sudo tether admin evict lab gpu1`，三件事同时发生：
  1. `sys.events{agent_evicted, nid:gpu1}` 广播**匹配上 B**，因为 B 的 `cfg.NID` 仍是 basename `gpu1` —— B 调 `reapManagedChildren()`（SIGKILL 掉操作员在跑的活）并自行关闭；
  2. `gpu1` 的 `agent_provisioning` 行被删，而 auth 后缀 fallback 只按 **basename 的指纹**授权 `gpu1-NN`，于是该镜像的**任何实例、任何名字、任何 pod** 都无法再连接，除非重新 PIN bootstrap；
  3. CLI 打印 `evicted sid=lab nid=gpu1 (node=true provisioning=true broadcast=true)`，即"成功"。
  反方向同样错，且 round-1 的 demo（`internal/adminsock/lease_evict_ops_test.go`）已经记录：evict **租约名**报成功、CASCADE 删掉该实例的全部端口/进程历史、**什么也没停下**。
- **后果**：一次对看起来已死的行的例行清理，掀掉整个无人值守车队并销毁在跑的工作，全程没有任何警告。改动前这个行为是自限的：`gpu1` 行 OFFLINE 就意味着用该凭据的所有进程都已经没了。本增量打破了这个耦合而文档没跟。
- **修法**：evict 必须租约感知：目标 basename 下存在活的 `<basename>-NN` 行时拒绝（或要求显式 `--force`）并打印还会被停掉与锁死的对象；目标本身是租约名时拒绝，并说明凭据挂在 basename 上、evict 租约名只删历史。同时更新 `docs/usage.md §5.18`：OFFLINE 的 basename 行不再意味着设备空闲。

### M17 — 测试有效性：四条守卫**恒等**或**结构性空转**

1. **round-1 B4 唯一的守卫是恒等式**（`internal/agent/connect_auth_lease_test.go:139-183`，断言在 `:173-176`）：把 `agent.go:1266-1268` 的 `if rebuilt, rerr := buildOpts(); rerr == nil { connOpts = rebuilt }` 删掉（即精确回退 B4），**整个 `internal/agent` 仍然全绿**（实测）。`TestConnectNameFollowsTheLeaseDropOnRetry` 调两次 `a.buildConnOptions()` 并断言两次的 `nats.Name` 不同——`buildConnOptions` 本来就在调用时读 `nidOf(a)`，这是 helper 的构造性质，与 `connectNATS` 无关。兄弟测试只断言 `connectNATS` 返回后 `nidOf(a)==cfg.NID`，`dropLease` 无论选项有没有重建都满足。
   **修法**：驱动 `connectNATS` 本身——起一个 authorization 接受 basename 的 CONNECT 名而拒绝 `<basename>-NN` 的嵌入式 server（`Users` + 每用户 nkey/token 即可），在已 adopt 租约的状态下启动 agent 并断言 `connectNATS` **成功**（只有重试呈上了 basename 才可能）。或者用测试 seam 暴露尝试过的 CONNECT 名，断言第 2 次 ≠ 第 1 次。
2. **drill 83 的 B4/B4b 在一个结构上排除了 clone 到达的窗口里计数**（`test/simcluster/drills/83-cloned-image-instances.sh:124` 的 `_B2_CURSOR`、`:141-144`）：游标取在 B1 的 `poll_until … _two_rows_online` **之后**，clone 到达可能产生的每一条审计行的行号都在游标之下，被 `tail -n +$((cursor+1))` 跳过。第二重独立空转：clone register 上要出现 `reconciled_closed`/`killed_orphan`，前提是 incumbent 那一刻**有 RUNNING 行**（plan §0.3 明写），而 drill 唯一的命令是两条立刻退出的 `echo`，且都在 clone 已 ONLINE 之后。review §5 把"无 reconciled_closed、无 killed_orphan"列进 drill 的 16 条 PASS 作为"contested 短路保护了 incumbent 的进程行"的证据——**这两条不可能因它们命名的理由失败**。
   **修法**：把游标移到 `_clone_home`/`_run_as_baked` 之前；先在 agt1 上起一个长命进程并断言它在 `ps` 里 RUNNING，**再**引入 clone，然后断言 0 `reconciled_closed` / 0 `killed_orphan` **且该 job 仍在 RUNNING**。并且要单独记录这一臂的 PRODUCT-RED 落地（B1 在改动前变红不能证明 B4 能红）。
3. **`NodeListEntry.Leased == true` 与整个 `--all` 排除零正向覆盖**：把 `internal/broker/exec.go:276` 换成 `leased := false`，三个相关测试加整个 `test/p2` 全绿（实测）。它们**全部**断言 `skippedLeased == 0`；全树没有任何测试断言真租约的 `Leased == true`、`--all` 会跳过它、或 `node.go:207-215` 那行面向操作员的提示曾被打印过。
   **修法**：走真实 `handleNodeListReq` 加一个测试——seed `agent_provisioning(lab, gpu)`（让 `bindingsKnown` 为真）、`gpu` 与**无** provisioning 行的 `gpu-02` 两行 nodes，断言 `listOnlineNIDs` 返回 `[gpu]` 且 `skippedLeased == 1`；再对 `cmd.SetErr` 的输出断言那行披露。
4. **silence rule 与它的 `leaseSubscribeSettle` 窗口（决定"重启还是克隆"的那条分支）完全没有自动守卫**（`broker.go:1842`，常量 `:1520`）：到达它需要"记录在案的 lease-aware holder + 不同 instance id + 该授予老于 5s + 心跳新于 `LeaseGrace` + probe 是'有 interest 无人应答'"。没有任何测试构造这个组合：`adjudicated()` 5s 后放弃，没有单测把授予时间做老，两条 e2e 重启路径都走别的臂。**删掉这条分支、或删掉 `> leaseSubscribeSettle` 这个合取，`make test` 与 `make e2e-parallel` 都全绿**；两个失败方向恰好是本增量最坏的两个结果（过急 → clone 拿裸名、每条 exec 跑两次；过怯 → 普通重启被改名），而 review 自己记录了缺窗口时**只有 simcluster drill 83 抓到**——一个 CLAUDE.md 要求"非必要绝不运行"的东西。
   **修法**：把 settle 窗口做成可注入（Config 字段，默认 5s，与 `LeaseGrace` 同形），在真实总线 + 沉默但已订阅的 incumbent 上加两个单测：授予做老过 settle → 后继保住裸名；授予新鲜 → 挑战者被加后缀。毫秒级钉住两个方向，且不依赖 drill。

### M18 — 测试归位（CLAUDE.md §3 step 5b）：DEMO banner、反向命名、重复、lane 杂物

这是一整包，建议一次 pass 处理完（否则下一轮会重新发现同样的东西）。

- **(a) 七个文件带 `// REVIEWER DEMO FILE — not part of the increment. Delete or fold in.`，而其中一个持有变更 (A) 唯一的回归守卫**：`internal/broker/instance_lease_probe_test.go:13`（另有 `lease_ops_recovery_test.go:12`、`lease_contest_on_reconnect_test.go:13`、`internal/adminsock/lease_evict_ops_test.go:10`、`internal/agent/routing_nid_adoption_test.go:18`、`internal/agent/lease_adoption_on_reconnect_test.go:16`、`internal/tunnel/client_nid_retarget_test.go:10`）。`instance_lease_probe_test.go:199-211` 是全仓**唯一**断言 `adjudicateLease` 及时返回 `leaseReasonProbePending` 而不阻塞 register handler 的地方。照 banner 行事会同时：删掉 (A) 的守卫，并让 `go test ./test/determinism/ -run TestPromisedGuardTestsExist` **变红**（`test/p2/cloned_instance_concurrency_test.go:36` 在注释里点名了只声明于该文件的函数）。
- **(b) 三个通过的测试以它们曾经演示的缺陷命名，名字现在断言了与测试相反的事**：`instance_lease_probe_test.go:169` `TestContestedProbeCostsTheFullBudgetAgainstASilentSubscriber` 实际断言 `d < claimProbeBudget`（注释自己承认"pin 的是性质不是预算"）；`:35` `TestAdjudicateLeaseGrantsTheSameNameTwiceInsideTheSubscribeWindow` 只在**真的**发了两次同名时才失败；`lease_ops_recovery_test.go:64` `TestStaleBeatGrantIgnoresALiveIncumbentSubscriber` 同理。只读名字的人会以为 3s 预算仍记在 handler 上，然后去缩小 `backgroundProbeBudget`——正是 review §3c 反对的那步。**建议改名**：`TestAdjudicationNeverBlocksTheRegisterHandlerOnAProbe` / `TestTwoClonesInsideTheGrantWindowGetDistinctNames` / `TestAStaleBeatDoesNotGrantOverALiveAnsweringIncumbent`。
- **(c) 五条不变量在本增量内各被测了两遍，分居两个文件**：`internal/agent/instance_lineage_test.go:29` vs `routing_nid_adoption_test.go:262`（差一个字母）；`:94` vs `instance_id_acceptor_test.go:60`；`internal/broker/instance_lease_probe_test.go:67` vs `lease_contest_on_reconnect_test.go:26`（逐字节同场景）；`internal/agent/instance_lease_reconnect_test.go:27` vs `session_contested_reply_test.go:29`；`cmd/tether/upgrade_all_lease_exclusion_test.go:24` vs `upgrade_all_lease_shape_test.go:31`（~130 行重复脚手架测一条断言）。这正是冻结门头部描述的 `internal/tunnel` 四文件事故，只不过这次发生在**一个增量之内**（每条审查 lane 各开一个文件）。
- **(d) `internal/broker` 现在有三份"活总线上的 lease broker"和五份 claim-probe responder**：`leaseBrokerWithBus` / `opsLeaseBrokerWithBus` / `leaseBrokerOnBus` 是同样的七行；`subscribeAs` / `claimProbeResponder` 加两处内联。**本增量自己已经记录过其中一份悄悄坏掉并造成假指控**（review §4：`subscribeAs` 带 `sub.AutoUnsubscribe(1 << 30)` 让服务器不再报 interest，于是每次 probe 都 `ErrNoResponders`）。修好的是五份里的一份，解释原因的注释坐在 `lease_ops_recovery_test.go:50-56`，另外四份看不见。**收敛到 `instance_lease_test.go` 里各一份**（它已经拥有 `leaseBroker`/`seedBeat`/`adjudicated`），并把 AutoUnsubscribe 那段注释搬到幸存的 helper 上——那条注释才是资产。
- **(e) `internal/agent/routing_nid_adoption_test.go` 是 lane 杂货铺**：五个测试跨三个互不相关的单元（租约名接受、session adoption 循环、exec 子进程环境），其中两类在包内已有职责匹配的归属。改 `buildExecCmd` 的人 grep exec/env 命名的文件永远打不开它。按 §5"新代码优先并入职责匹配的既有文件"拆散并删掉该文件，`// origin:` 与 `THE DEFENCE IS THE AGENT'S OWN ENVIRONMENT` 那段注释整段搬运。
- **(f) 每个 demo banner 文件的头部用现在时描述一个**已被 round 1 修掉**的缺陷**，而下面的测试断言的是修好后的行为：`client_nid_retarget_test.go:14-17`（"Sessions already installed under the OLD name keep bridging…"，而 round-1 #13 已让 `SetNID` 退休会话，测试的成功路径就是 `return // desired behaviour`）、`lease_adoption_on_reconnect_test.go:19`、`routing_nid_adoption_test.go:21`、`lease_contest_on_reconnect_test.go:23-24`、`lease_ops_recovery_test.go:60-63`、`instance_lease_probe_test.go:33-34`、`test/p2/cloned_instance_concurrency_test.go:27-35`。这类注释**任何闸门都抓不到**，且只有趁 round-1 上下文还在人脑里的现在才修得动。改写成"round 1 发现了 …；`SetNID` 现在会 …，这是守卫"（`instance_lease_probe_test.go:110-118` 与 `:283-289` 已是范本）。
- **(g) 三条 `// origin:` 指错小节**：`internal/node/lease_allocation_test.go:10/44/64` 写 `plan §3 D7`，而 D7 在 `## 2`（plan:196），§3 是"核心机制"。`TestOriginLinesPointAtDocumentsThatExist` 只 stat 路径，看不见。顺手把 `instance_lease_probe_test.go:110` 的裸 `// origin: internal review — …` 补成 `docs/reviews/cloned-credential-instances-review.md §3c`。
- **(h) 本轮 lane 留下的复现文件必须归位而不是删除**（见 §1）：`lease_probe_staleness_test.go`（B5）、`reconcile_lease_ownership_test.go`（B2/B3）、`lease_basename_collapse_review_demo_test.go`（B9）。改成职责命名 + `// origin: cloned-credential-instances round 2 …`，修完 blocker 后它们应当变绿并留作守卫。`ReviewDemo` 前缀必须去掉。

---

## 4. MINOR / 可延后

> **提交前必做（不是缺陷，是流程）**：本轮多条 lane 在**共享工作树**里做过变异（其中一次把 `releaseLeaseName` 的 `ProtoVersion`+`NID` 临时删掉），并留下过编译不过的 `zz_*` 文件。**当前树已恢复**：`go vet ./internal/broker/ ./internal/agent/ ./internal/tunnel/ ./internal/node/ ./internal/proto/ ./internal/authcallout/ ./cmd/tether/` 返回 rc=0，`zz_*` 已不存在。提交前仍需：`git grep -n "MUTATION\|temporary, review lane"`、`git ls-files --others --exclude-standard` 逐条**显式** `git add`（**不要 `git add -A`**），并注意 `test/determinism/test_naming_test.go` 的 `processNamedPattern` **抓不到** `zz_`/`scratch`/`lane`/`demo`/`repro` 这类拼写（实测 `isProcessNamed("zz_scratch_silence")` 为 false）——建议把这几个 token 补进去（账本仍是 `published=0`，加进去零成本）。今后要求变异 lane 在隔离副本/worktree 里工作。

| # | 文件:行 | 场景 → 后果 | 修法 |
|---|---|---|---|
| m1 | `broker.go:1566` vs `:1573` | `probeInFlight.LoadOrStore` 的单飞检查排在 inline probe **之后**，于是 agent 的每一次重试都再付一次 15ms 的 head-of-line 停顿，尽管同键的后台 probe 已在跑、这次重试不可能被服务。WAN 上单个 agent 在 broker 重启后要吃 ~2 次 transient + ~300ms；N 个 agent 的 15ms 停顿在同一条 dispatch goroutine 上相乘 | 把 `probeInFlight.LoadOrStore` **移到 inline probe 之前**：已有在飞 probe 就立即返回 pending。零风险。可选：让 inline grace 自适应（记住上次观测到的 claim-probe RTT），使健康的 WAN agent 的应答能被 inline 采纳 |
| m2 | `broker.go:1430-1451`、`:1945-1951` | `leaseProbe` 现在直接调 `probeObserve`，`probeNameHeldByOther(Within)` **生产上已无调用者**，于是 `claimProbeBudget`(50ms) 不再给任何 register 定尺寸。后果：`lease_contest_concurrency_test.go:77` 用 `claimProbeBudget+80ms`=130ms 延迟 responder，而生产的预算是 15ms/3s，130ms 被后台 probe 兜住，**该测试因为与其名字无关的理由通过**（任何 <3s 的延迟都会通过）。它的注释还写着 "claimProbeBudget is a fixed 200ms"（常量是 50ms）。另外 `:1430-1435` 与 `:1436-1450` 是同一段文档的两份拷贝，且第一份说"完整预算只在分区时花掉"、第二份说 pre-feature 订阅者也会付 | 删掉 `probeNameHeldByOther(Within)` 与 `claimProbeBudget`（或留一个 wrapper 标为 test-only），删掉重复的第一段注释，把两个测试重新参数化到生产真正使用的 `inlineProbeGrace`/`backgroundProbeBudget` 上 |
| m3 | `broker.go:1576-1598` | 后台 probe goroutine 无 ctx、无 WaitGroup，可比 Broker shutdown 多活最多 3s，期间持有一个 NATS inbox 订阅。同文件 170 行外的 reconcile registry 特意宣称"零 goroutine 以对泄漏门隐身"，`transferAuditWG` 就是为这件事存在的缝。今天不红（没有泄漏测试驱动 contested register），但 `test/concurrency` 的 `settledBaseline` 最多轮询 1s，3s 的 goroutine 会活过它 | 要么把 broker 的 run ctx 传进 `leaseProbe` 并用 `RequestWithContext`，要么像 `transferAuditWG` 那样 WaitGroup 起来在有序关闭里 drain。若刻意 detach，在 `:1576` 写清界限（≤1/键、≤3s） |
| m4 | `broker.go:1996`（offer 存储）与 `:1967-1972`（`offered()`） | offer 的有效期就是 `leaseGrantWindow`，**本轮已从 1s 放宽到 5s**，lane 的 1.2s 复现已不再触发。残余：offer 要覆盖的是一次**完整会话重建**（`applyLeaseVerdict → adoptRoutingNID → 有界 finalizer teardown → redial → TLS → auth_callout 额外一跳 → register`），WAN 或高负载主机上仍可能超过 5s；撞名的 loser 每次重来烧掉一次 `maxLeaseAdoptions`（3 次后 `acceptableLeaseName` 永远为 false，落入 B9 的拒绝循环） | 给 offer **自己的**生命期（`leaseOfferWindow`，或独立的 offers map 带自身 TTL），不要与 `leaseGrantWindow` 共用一个语义。加一个"相隔 2s 的两个挑战者必须拿到不同后缀"的测试 |
| m5 | `internal/agent/instance.go:90-115`（`mintInstanceID`）、`:164-181`（`execEnv`） | `TETHER_INSTANCE_ID` 只校验**形状**不校验**来源**：任何导出该变量的父进程会让它启动的每个 agent 共用一个 id。现实向量：`node upgrade` 之后 agent 的 `/proc/<pid>/environ` **永久**含有该变量（`os.Unsetenv` 不重写初始栈区），操作员照抄启动命令即复制；wrapper/unit 固定它；warm clone。两个进程共用 id 会同时打穿三道守卫：`broker.go:1700` 的同实例快路径（第二个 clone 拿裸名）、`:1980` 的 `heldByOther`（incumbent 的应答带同一个 id → 报 `held=false, known=true` → 再发一次裸名）、以及 `upgrade_state.go:533` 的 `TargetInstance`（round 1 加它就是为了兄弟不能认领别人的 marker） | 把血统钉到发射它的那个进程上：`execEnv()` 同时发 `TETHER_INSTANCE_PID=<os.Getpid()>`，`mintInstanceID` 只在该变量解析出且等于 `os.Getpid()` 时才接受继承的 id（两个变量都消费掉，新键加进 `stripInstanceEnv`）。`syscall.Exec` **保留 pid**，所以真实升级/回滚的血统全部照旧；fork 继承或手工粘贴的值则开新血统（安全方向：多一个从不持久化的后缀而已）。同时把头部的"KNOWN, UNPREVENTABLE CASE"收窄到真正的残余（连 pid 一起复现的热快照） |
| m6 | `upgrade_state.go:530-535` | 空 `TargetInstance`（即 pre-increment 二进制写的 marker，也就是**从 v0.5.0 升到本版时盘上的那种**）让每个 clone 都回答"是我"。后果：(a) 从没装过东西的 clone 会为共享 pending marker 武装 watchdog 并在 deadline 执行回滚+自我 re-exec；(b) 终态时两个 clone 各自把同一个 `rolled_back` 挂在自己的 register 上，broker 记两次、其中一次挂在从未升级过的 nid 下，先落地的那次 `os.Remove` 掉 marker；(c) pending 臂只剩 BootCount+boot proof+sha 三条，重启到 staged 镜像的 clone 全都满足，于是非目标 clone 可以提交目标的 marker 并解除其 watchdog。**注意这个 fallback 不能简单删掉**——B7/M10 显示它是今天让重启能工作的唯一分支 | 一旦 boot 时可认领所有权（B7），空 `TargetInstance` 在一次启动内即被就地升级成有主的，本条自动消失。在那之前，至少把空 marker 的终态臂限制到持有 **basename** 的实例（`nidOf(a) == a.cfg.NID`） |
| m7 | `internal/agent/upgrade.go:330` | `recoverFromFailedExec` 的闸门是 `merr != nil \|\| m == nil \|\| m.State != upgradeStatePending`，**没有 `markerTargetsThisAgent`**（其余五处 marker 转移都有）。实例 B 走 ReExecOnly 腿且 `syscall.Exec` 失败（A 正在翻转共享二进制时的 ETXTBSY、noexec remount、ENOMEM），B 会拿着 host flock 找到 A 的 pending marker 并对它 `executeRollback`：A 的 staged 二进制在脚下被换回 `.prev`，marker 被写成带 A 的 `TargetInstance` 的 rolled_back（此后按 M10 无人能上报或清理）。B 返回 true 继续服务，两边都没有异常日志。**先于本增量存在**，但 clone 部署让"共享二进制目录上的并发 exec 活动"从异类变成常态 | 加上其余五处同样的 `!a.markerTargetsThisAgent(m)` 拒绝，并响亮地记录"别人的 pending 升级被放过了" |
| m8 | `internal/broker/reconcile.go:201`；`internal/proto/messages.go:144` | `req.PreviousNID` 从 wire 上**逐字采信**，没有 basename 家族校验。一个合法以 `n2` 注册的 agent（自己的 nkey、自己的 subject，ACL 全过）在 body 里写 `previous_nid="gpu1"`，就能让 `gpu1` 的每条 RUNNING/LOST 行成为 `mine`，n2 一条都不呈报 → 全部 `MarkExited(-1)` + `reconciled_closed`，审计记在 n2 名下，`gpu1` 的历史里毫无解释；同一 body 还翻起 n2 的 `sawAnyRow`。今天诚实 agent 到不了（`acceptableLeaseName` 把租约名限制在自己的 basename），故为 MINOR——但 broker 侧没有任何约束，而这是本增量里唯一一个凭 body 字段就取得跨节点写入权的输入 | 拒绝/忽略与 subject nid 不同族的 PreviousNID：`proto.BasenameOf(req.PreviousNID) == proto.BasenameOf(nid)`，并要求 `req.InstanceID != ""` |
| m9 | `internal/broker/reconcile.go:81` vs `:201`、`:118-124` | `resolveReconcile`（被声明为与 inline 路径**逐条等价**的集群分类器，接在 `cluster_forward.go:650` 的 `writeVerbs[VerbReconcile]` 上，也是 D9 切换的目标）**没有跟着改**：仍只按 `p.NID != nid` 过滤，orphan 循环也**没有 `sawAnyRow` 闸门**。喂进一次租约 agent 的首个 adoption 后 register（`nid="gpu1-02", PreviousNID="gpu1"`，行归档在 `gpu1`），两者结论相反：inline 接受 p1，`resolveReconcile` 跳过整行、把 p1 留在 `agentByPID` 并发 `ProcAudit{killed_orphan}`。等价性证明（`reconcile_marks_test.go`）里 `PreviousNID` **出现次数为零**，所以那句"Equivalence (marks AND audit, compared as a set) is proven in …"现在是假的，而本该发现漂移的测试看不见它。今天无生产 producer，故 MINOR | 要么把两条规则（`rowsOwnedBy` + orphan 闸门，形状以 B2/B3 的结论为准）**同一次编辑**移植进 `resolveReconcile`，要么在 `reconcile_marks_test.go` 里加上带 PreviousNID 和无行 nid 的用例让分歧今天就变红——不要让那句等价声明在为假时继续挂着 |
| m10 | `cmd/tether/agent_logsink.go:44-58`、`:96-105`；`internal/logrotate/logrotate.go:172-197` | 所有 clone 共用一个轮转的 `agent.log` 和一个 `boot.err`（按 (home,sid) 键控，与 state.json 相同），而 logrotate 的包文档明写它违反的契约："No multi-process locking: one Writer per file, per process."。A 越过自己的上限时 rename `agent.log→.1`，B 的 fd 跟着被改名的 inode 走并继续追加，A 的下一次轮转把它删掉——**A 轮转两次就销毁 B 在此期间的全部日志**，B 毫不知情。dup2 的 panic sink 同理（启动中的 clone 的 `rotateBootErrIfLarge` 把活着的兄弟的 fd 2 指向的文件改名走），正是该文件头部说自己要防的"panic 落进 unlinked inode"。另外 `newLoggerTo` 不挂任何 base attr，**没有一行日志带 instance id/pid/routing nid** | 给 agent daemon 的 base logger 挂 `logger.With("instance", a.instanceID)`；日志路径按实例默认（`agent-<instanceid>.log`），或在 usage.md 写明共享 home 意味着共享且有损的日志、并要求 clone 部署设 `log_file`（或 `-` 交给容器运行时收集） |
| m11 | `internal/agent/proxy.go:714-722`（M3 re-ACK 块）、`:624` | 租约路径上没有任何东西拆掉内嵌 proxy（`applyLeaseVerdict`/`requestLeaseRebuild`/`adoptRoutingNID` 只退休 tunnel 会话），于是 `p.srv` 在中途降级后存活。下一次重连 `resp.Lease == nil`，无闸门的块跑 `serving := p.srv != nil` → `pubProxyReady(nc,true)`，而它现在发在 `nidOf(a)="lab-1-02"` 上；`handleProxyReadyEvent` 只看 session 开关、**没有 proxy_capable 闸门**，于是租约行的 `nodes.proxy_ready=1` 而 PublicPort=0 → 默认表里 `lab-1-02 ONLINE true -`。爆炸半径确实有限（`/sub` 按 `proxy_capable=1` 过滤、告警计数取自 `onlineNIDs`、遗留的 SS listener 绑 127.0.0.1），故 MINOR | 要么给 re-ACK 加租约闸门（`if serving && nidOf(a) == a.cfg.NID`），要么更好——让 `adoptRoutingNID` 调 `proxyTeardownLocked(p, nc, true)`（顺带处理掉那个还攥着 session PSK 的遗留 listener）。注意顺序：`adoptRoutingNID` 先存新 routingNID 再 `SetNID`，所以隧道退休产生的 unready 已经落在**新**名字上，离开的进程也从没清掉 basename 的 proxy_ready |
| m12 | `internal/authcallout/handler.go:357` → `:378-409` | fallback 的 basename 查找**未命中**时会掉出该臂，若此刻 argv 里还带着 PIN（`scripts/install.sh:416` 打印的启动命令就是 `… ${PIN:+ --pin $PIN}`，被烤进镜像的 cloud-init/rc.local 就是这一行），控制流到 `:378` 时 `nid="gpu-02"` 且 PIN 有效 → `ProvisionWithPIN(sid,"gpu-02",fp,pin)` **为一个按契约临时的名字写下永久绑定**。两条真实的未命中路径：集群模式下应答的是 clone 重连到的那台 broker、读本地副本，而 `h.fenced()` 只覆盖失去 leader 联系不覆盖普通 raft 滞后；以及 clone 在跑时操作员 evict 了 basename（实测：删掉 `lab` 的行后同一调用带 PIN 返回 nil，并出现 `lab-02` 行）。一旦写入即永久：`claimedLeaseNames` 无条件扫 agent_provisioning，该后缀被从 98 个槽位里烧掉，churn 的车队会一路走到 `ErrLeaseUnavailable`；同时那个 pod 现在有绑定，`Leased=false`，被纳入 `node upgrade --all`，而目标按字典序排序，**一个临时 pod 可以成为 canary**（`node.go:670-681` 自己记录了这个危险） | 拒绝为"本指纹已拥有其 basename"的租约形状名字做 PIN bootstrap：fallback 块之后若 `SplitLeaseName(nid)` 说是租约，返回一个**可重试的** transient deny（`ErrNotLeader`/`ErrFenced` 的形状），让 clone 重试到副本已有绑定的 broker 上被 fallback 无写入地放行。只有 basename **未**绑定到本 fp 时才允许 bootstrap 租约形状的 nid（那是真正的"操作员登记一台叫 gpu-02 的真实设备"） |
| m13 | `internal/broker/wire_freeze_test.go:181-192`；`docs/reviews/cloned-credential-instances-plan.md:410` | 冻结键账本只对 `instance_id`/`leased_nid` 做了**双向**分析；对 `previous_nid`/`releasing_name` 只分析了集群内 broker→leader 那一跳（"never forwarded to a leader"），从没问"旧的**应答** broker 拿它怎么办"——而那正是 B1。plan 的四象限表更糟：旧 broker × 新 agent 格写着"旧 broker 忽略两个未知 JSON 键…broker 回滚同样安全"，是在 `previous_nid`/`releasing_name` 存在之前写的，且回滚安全的结论现已为假。测试覆盖与陈旧分析一致：`internal/proto/lease_wire_shape_test.go` 只钉了 `instance_id`/`leased_nid`/`lease`/`leased` | 修完 B1 后，给冻结注释补上 `previous_nid`/`releasing_name` 的 agent→旧 broker 腿（明说键缺失时旧 broker 做什么），改正 plan 矩阵那一格点名全部四个键，并把这两个键加进 `lease_wire_shape_test.go` 的零值/跨边界用例 |
| m14 | `cmd/tether/node.go:683` | 把临时实例排除出车队动词的逻辑**完全在新 ctl 里**（broker 设 `NodeListEntry.Leased`，ctl 过滤）。v0.5.0 的 ctl（受支持的 N-1 对端，也是操作员笔记本上最可能还在跑的那个）忽略该键，把每一行 ONLINE 都放进 targets。具体：`gpu1` 加 pod 实例 `gpu1-02`，pod 家族 recycle 时 `gpu1` 自己的行恰好不是 ONLINE，`sort.Strings` 之后 `targets[0]=="gpu1-02"` → 它成为 canary → 该 pod 在等待预算内 recycle → **整片 rollout 中止且一个节点都没升**。plan §6 的"ctl 两个方向都免费"对寻址成立、对车队动词不成立 | 要么在 plan 的 N-1 矩阵里如实记成 [GAP]（旧 ctl + 新 broker：`--all` 仍含租约名，绕法是显式点名目标），要么把执行点放到 broker（升级派发拒绝 Leased-且-未 provisioned 的 nid，除非被显式点名）——这是任何 ctl 版本都绕不过的规则 |
| m15 | `test/simcluster/README.md:321` | 新增的 `drills/83-cloned-image-instances.sh` **没有进 drill 清单**（`grep -n "83" README.md` 无结果；表里 40 行 vs `drills/*.sh` 42 个，另一处缺失 `67-transient-js-refusal` 先于本增量）。没有任何机械闸拦它：日志 oracle 与 shell scope 两个门只走 `drills/*.sh`，`run-drills.sh` 自动发现，所以 drill **能跑但在清单里不可见**。而 CLAUDE.md §5 让下一个人"只跑相关的那一个"，README 表是唯一把改动映射到 drill 并记录判决的地方——drill 83 是本增量**唯一**的部署层证据 | 在同一个 commit 里补上 `| \`83-cloned-image-instances\` | … |` 行（N、容器形状、断言什么、判决），顺手把 `67` 也补了。若要根治：在 `test/architecture` 加一条反向断言——每个 `drills/*.sh` 的 basename 必须出现在 README，每个 README drill 行必须指向存在的文件（与 `simInlineLedger` 同形的只减账本） |
| m16 | `test/architecture/testdata/structural_budget_golden.txt:68-69` | 两条精确计数账本（`internal/agent.Agent 127`、`internal/broker.Broker 285`）对本增量**最大的一次结构性增长记零**，因为 18 个新的租约入口点全部把 receiver 写成第一个参数：`internal/broker` 里 `b *Broker` 形状 16→23，`internal/agent` 里 `a *Agent` 形状 11→22（**翻倍**）。这个写法是**冲着闸门去的**并且明说了（`instance.go:198/:222/:330`、`broker.go:1631/:1813` 各写着"the structural-budget ratchet pins the method count exactly"）。写成方法的话两个 golden 必须手改 285→292、127→138 并在 commit message 里给理由——**CLAUDE.md 说那个摩擦点就是闸门的全部价值** | 本增量不必改代码，但**要显式定策**而不是靠累积：要么在 `measureTypeMethods` 里把"第一个参数是本包自有具名类型"计入该类型预算，要么在 golden 的手写头部记下"receiver-as-first-parameter 刻意不在计量范围内"及理由，别让下一个读者以为 127/285 约束了这两个类型的表面 |
| m17 | `internal/agent/lease_shared_state_test.go:46-47`/`:101-102`；`internal/proto/lease_wire_shape_test.go:79-81`；`internal/broker/lease_contest_concurrency_test.go:59-72` | 多条 MUTATION 说明是**陈旧、反向或不可执行**的：(a) "把 newStateStore 按 routing nid 键控…这个测试就会变红"——那个改动会让租约实例有自己的文件、incumbent 的行完好，测试会**变绿**；真正让它变红的是删掉 `adoptRoutingNID` 里的 `stateStore.detach()`。(b) 两处"and this test turns green"是 demo 期遗留（现在本来就过），无法执行。(c) 声称 `DisallowUnknownFields` 会让它变红，但该测试直接 `json.Unmarshal` 手写字面量、从不碰产品解码器。(d) 论证基于"claimProbeBudget 是固定 200ms"，常量是 50ms。这些说明正是本仓依赖的变异验证协议的载体，写错会产生协议本身要防的结果：有人照做、看到期望的颜色但原因错了，然后把守卫记成已验证 | 逐条改成真正能翻转该断言的变异，去掉 demo 期的"turns green"措辞 |
| m18 | `internal/tunnel/client_nid_retarget_test.go:45-61` | `TestRedialAfterSetNIDIsTerminallyDeniedUnderTheNewName` 被 `SetNID` 自己的会话退休满足：`SetNID` 返回的瞬间 `SessionUp(publicPort)` 已经是 false，轮询第一圈就返回，`DropTransport` 根本来不及引发 redial。证明：把 `denyIsTransient` 的 `default:` 臂改成返回 `true`（即 supervisor 会永远猛敲一个终态拒绝的 broker，与该测试注释声称的恰好相反），测试仍以 0.03s 通过 | 要么删掉（已被 `TestSetNIDRetiresSessionsRegisteredUnderTheOldName` 涵盖），要么让它测它命名的东西：先装会话、**先** DropTransport 让 redial 真的在飞，再 SetNID，断言 supervisor 终止（槽位释放 / 服务器侧观测不到进一步 dial），而不是断言 `!SessionUp` |
| m19 | `internal/broker/lease_contest_concurrency_test.go:151-184` | `TestAdoptingALeaseNameDoesNotOrphanTheInstancesOwnRunningProcesses` 的请求里**没有 `PreviousNID`**，`rowsOwnedBy("gpu1-02","")` 是恒等谓词，它通过纯粹是因为 `sawAnyRow` 为 false。删掉 `rowsOwnedBy` 的 previousNID 子句（即回退 round-1 B3 的一半）它仍绿。上面 30 行注释却告诉下一个读者这条钉住了 PreviousNID 救援 | 要么在请求里设 `PreviousNID: "gpu1"`，要么把注释改成"钉的是 fail-closed orphan 闸"，PreviousNID 的用例交给 `reconcile_lease_ownership_test.go` |
| m20 | `test/p2/cloned_instance_e2e_test.go:205-236` | `TestRestartedInstanceReclaimsItsNameRatherThanBeingSuffixed` 仍无法区分"后继保住了名字"和"后继根本没注册"：`stop()` 之后 `durable` 行已存在，断言 `len(got)==1 && got[0]=="durable"` 正是第一个 agent 留下的状态。后继连不上、注册失败、或被变更 (A) 的 `lease_probe_pending` 永久 transient 挡住，测试照样过（round 1 已记为 #14 并采纳为缺陷，未变） | 让后继可观测：重启前 `UPDATE nodes SET release_version=''`，然后轮询它变回非空（`test/p2/lone_instance_register_count_test.go:70` 已有此写法）；或用 spy 订阅计数 register，为 0 时以 INCONCLUSIVE 失败 |
| m21 | `internal/agent/instance.go:337-341` vs `:377-382` | `requestLeaseRebuild` 用 `rebuilding.CompareAndSwap(false,true)` 保护后才 adopt；`applyLeaseVerdict` **先 adopt 再 `Store(true)`**，既不取也不尊重 CAS。两者在不同 goroutine（前者在 nats.go 异步回调 `onNATSReconnect`，后者在 `Run` 的 session goroutine）。重连落在 session 的 register 窗口里时两边都会收到 lease，`adoptRoutingNID` 的五个非原子更新可以交织成"控制面路由是 gpu1-03、tunnel REGISTER 是 gpu1-02"并**终生保持**，还烧掉 2/3 的 adoption 预算。后果：`tunnelTokenLookup` 比对呈上的 nid 与分配行，于是该实例的每个 expose 都 `token_unknown_or_revoked`（终态），或者若落在 basename 上则顶掉 incumbent。窗口窄，但失败静默且永久，而防它的 CAS 就在三行之外 | 让 `applyLeaseVerdict` 在 adopt 前取同一个 CAS（输了就不 adopt 直接返回 true），或把 CAS 移进 `adoptRoutingNID` 使两个调用方共用一道闸 |
| m22 | `internal/tunnel/tunnel.go:1030-1039` | retire 的 detached goroutine **先关 conn 再关 yamux**。`sess.cancel()` 不会解开 `runAcceptLoop` 的 `Accept()`，只有关传输才行；而代码自己的注释说 `tls.Conn.Close` 要先拿连接的写锁才能设 deadline，于是黑洞 socket 上一个 parked write 会阻塞到内核重传时限。`ysess.Close()` 因此永不执行，退休的 supervisor 停在 Accept，**yamux 会话仍然活着**——tunnel server 可以继续往上推流，被降级的实例继续服务它刚被告知不再拥有的端口。而且这里既无预算、无 poison、也无 escalation | 先 `ysess.Close()` 再 `conn.Close()`，或在 Close 前对 conn `SetDeadline(time.Unix(1,0))`（`connTracker.poison` 用的就是这招）。既有的 `Close(publicPort)` 路径同病，一并修。测试：对端停滞时 `SetNID` 之后 `HasSession(port)` 必须在有界时间内为 false |
| m23 | `internal/agent/tunnel_adapter.go:37-41` | `nidSetter` 是唯一没有编译期契约的可选 `ExposeAdapter` 接口，而它就在那个把理由写明的 `var (…)` 块所在的文件里（注释原文：agent 在运行时 type-assert 可选接口，**方法缺失是静默的生产空操作**）。`SetNID` 若被改名、改成值 receiver、或 adapter 被一个只转发 `ExposeAdapter` 的 wrapper 替换，两处断言都会静默落空，租约实例继续以 **basename** 在 tunnel 上 REGISTER，`tunnelTokenLookup` 匹配上 incumbent 的分配行、而安装只按 public port 键控 → **顶掉 incumbent 的活会话**。正是这个 seam 要关掉的缺陷，而且编译与测试都不会说话 | 在契约块里加 `_ nidSetter = (*TunnelExposeAdapter)(nil)`（接口未导出但同包，直接可编译） |
| m24 | `docs/usage.md:1155-1200`、`:1773-1790` | CLI 上**没有任何东西**能把一行租约映射回物理实例：`node ls` 无 instance 列；`--json` 只有 `BootID`，而它是按内核的——同一宿主上的每个容器报**同一个** boot_id，用它区分实例会给出一个自信的错答案（且 `internal/broker/cluster_forward.go:358` 仍把 boot_id 称为"the natural epoch: an agent restart -> new bootID"，plan D1 说本次要改正）。审计/历史行只带 nid。唯一可行的答案 `tether exec gpu1-02 -- hostname` 是 plan R3 要求**必须**写进 usage.md 运维流程的，而文档 diff 只加了 FAQ 段。usage.md 里同样没有：`--all` 排除租约实例、basename 不会被提升回来（死掉的持有者会留下 `gpu1 OFFLINE` 挨着活的 `gpu1-02`）、共享 `~/.tether` 的升级 flock 会让一个实例挡住另一个的升级（plan §0.6 第 3 条） | 在 §5.18/§5.19 补齐：`<nid>-NN` 行是什么、不会被提升回来、`tether exec <lease-name> -- hostname` 的映射配方、`--all` 的排除；表里标出租约行让文档有东西可指。boot_id 的"natural epoch"注释按 plan D1 在同一个 commit 里改正 |
| m25 | `internal/broker/broker.go` 的 `adjudicateLease`（`:1682-1848`）内没有任何 `ColocatedAgentNID` 引用 | plan Q4/B2 裁定"broker 宿主机上的 co-located agent 免于租约，broker 侧强制并配测试"——**没有实现，也没有测试**。broker 宿主的 co-located agent 被保守臂改名（前任订阅未回收、在 `leaseSubscribeSettle` 内重启——罕见，但恰好是本车队实测过的 #72 形状）后以 `<colocated>-02` 注册，于是 `tether cluster upgrade` 把 reexec-agent 步骤转发到 `SubjCmdForwarded(sid, cfg.ColocatedAgentNID, "upgrade")`——**没有订阅者的 subject**——并在 broker 已 reload、agent 未升级的状态下 HALT；`tether node ls --brokers` 按同一个静态 nid 关联（`cmd/tether/node_versions.go:82-97`），显示空 AGENT_VER / 假 SKEW。失败落在集群升级中途，是停机状态最难推理的操作 | 实现裁定过的豁免：`nid == cfg.ColocatedAgentNID` 时 `adjudicateLease` 返回 uncontested（broker 宿主按构造是单实例），并配 plan 点名的那条测试；或在增量的风险账本里明确记下 Q4/B2 被放弃及理由 |
| m26 | `internal/agent/conn_teardown_test.go:78-81`、`:113-120` | #72 ladder 套件**结构上看不见 farewell**：`teardownTestAgent` 返回的 `Agent` 的 `instanceID` 是零值，`newWedgedFinalizer` 的 `nc` 是 nil，于是 `releaseLeaseName` 在 `instance.go:554` 的第一道 guard 就返回——包括 `{"shutdown_exits"}` 那条用例。也就是说，**"cancel 必须最先、teardown 必须在 budget+grace 内返回"这套守卫，看不到本增量往这条路径上加的唯一新工作**。BLOCKER 版本（farewell 曾在 caller goroutine 上取 `nc.mu`）之所以能落地，就是因为本该抓住它的守卫对新代码是恒等的；修法虽已落地，守卫仍然缺席，重新引入不会变红 | 给 ladder fixture 一个真实的 26 字符 instance id 和一个真实卡住的 `*nats.Conn`（嵌入式 nats-server + 第二次 dial 停住的 connTracker，然后 `ns.Shutdown()` 让 doReconnect 持 `nc.mu`），把用例加进既有的 wedged-teardown 表并写 `// origin: cloned-credential-instances round 2 farewell lane`。约 40 行；在旧代码上红、在当前代码上绿 |

---

## 5. 明确判为"不是缺陷"的（防止下一轮重新发现）

### 5.1 审查期间已被主进程修掉的三条（lane 报告时为真，**现在核验为已修**）

| lane 结论 | 现状与证据 |
|---|---|
| **farewell 在 caller goroutine 上取 `nc.mu`，`systemctl stop` 可无限挂死**（两条 lane 各自确定性复现过） | **已修**。`releaseLeaseName` 现在是 S3 closer goroutine 的**第一句**（`conn_teardown.go:210-213`），排在 cleanups 与 `nc.Close()` 之前，因而受 `closeBudget` + S4 poison + S5 escalation 覆盖——正是两条 lane 给出的修法。注释也把这段历史记了下来。**但 m26 仍然成立**：ladder 套件仍看不见它 |
| **background probe 无条件写入，会覆盖更新的定性观测** | **已修**。`broker.go:1592-1596` 增加了 `if prev … pv.at.After(startedAt) { return }`，且用**发起时刻** `startedAt` 而非完成时刻做比较。残余（不单列为 finding）：存入的 `at` 仍是完成时刻，于是一次超时观测被写下时其证据已 3s 陈旧却按新鲜计 `probeTTL`；若采纳 B5 的"不缓存定性答案"，后台这条也顺手把 `at` 改成 `startedAt` 更诚实 |
| **`leaseGrantWindow` = 1s 太短**（offer 撞名 / grant 后 1s 即失守） | **已改**为 `= leaseSubscribeSettle`（5s），并在 `:1620-1654` 写了完整论证（含 drill 83 的时间线）。因此 lane 用 1.1s 的 B5 复现现在变绿，`TestGrantInvalidatesTheFreeObservationThatAuthorisedIt` 通过。**但 B5 未被关闭**：把间隔改成 6s 后同一测试仍然 FAIL（本轮实测），窗口只是从 [1s,10s) 收窄到 [5s,10s) |

### 5.2 三张 sync.Map 的增长：**不是内存缺陷**（结论附数字，不要再查一遍）

- **谁能造键**：键是 `sid+"/"+nid`，两者都来自 **subject**（`handleRegister` 的 `ParseSidNidFromCtrl`），且 `req.NID` 必须等于 subject nid。`internal/auth/permissions.go:192` 把 agent 的 Pub allow-list 钉死在**自己的** nid 上，`PermissionsForActivatedMember`/`PermissionsForBroker` 根本没有 register Pub 权——**只有 agent 凭据能造键，且只能为它连进来的那个 nid 造**。auth callout 对一个指纹只接受两种形状：provisioned basename，和经后缀 fallback 的 `<basename>-NN`；`SplitLeaseName` 只接受 ≥2 的两位后缀，且 fallback 是**读**不是写，所以第二层嵌套（`gpu1-02-05`）没有可匹配的绑定，被拒。**每凭据天花板 = 1 + 98 = 99 个 nid。**
- **量化**：`leaseHolder` 每个 (sid,nid) 首次 register 加一条 + 每次 offer 加一条；`probeCache` 每个被探测的名字一条；`probeInFlight` 由 goroutine 的 defer 自删、每键至多一条。单条约 150–200 字节。**现网车队（1 session、6 个 basename）最坏 ~600 条、~120 KB。**
- **结论**：不是内存 blocker，也不存在不受信输入驱动的无界增长。round 1 留下的那条"leaseHolder 只增不删（不影响正确性但无界）"到此关闭。**永不清理的真实代价是 staleness（即 B5/B6），不是字节**——把它当内存问题修会错过重点。
- 两条残余（记录，不作为 finding）：(a) 条目在 session 删除（H.3 级联只删 nodes/processes 行）和 leader 抖动后存活，长命 broker 持有的是整个进程生命期的并集而非活集；(b) 单模无 auth_callout 的部署里任何能连 NATS 端口的客户端都能发任意 32 字符 nid，但同一请求也会经 `registerNode` 建一条持久 `nodes` 行，map 是**更小**的暴露面，不是该修的东西。
- 若仍然想要一个上界：不需要新 goroutine——在 `registerCoreReconcilePasses`（`broker.go:1390-1412`，注册表把一切收敛到一个 ticker、注释明说零 goroutine、对泄漏门隐身）里加一个 pass。**注意 `leaseHolder` 的清理绝不能激进**：`priorHolderSpoke` 故意读任意老的条目，删掉一条就把一次普通重启的 silence 变回一个后缀。

### 5.3 两条被驳回的 goroutine 叙述

- "后台 probe goroutine 忽略 Run 的 ctx，可比 broker 多活一个完整 `backgroundProbeBudget`"——事实对（`:1576-1598` 无 ctx/无 WaitGroup/无 Close 钩子），但**给不出失败场景**，finding 自己也承认。它的两条后果主张不成立：(a) 该 goroutine 对泄漏门**不是隐身的**——只要有泄漏测试驱动它就完全可见，与主张相反；(b) "在本 broker 不再裁决的任期里采集的条目"根本不是 goroutine 的性质，因为 `probeCache`/`leaseHolder` 在 leader 变更时**都不清空**，同样的陈旧性对 inline 路径一模一样。保留为 m3（可观测性/纪律），不是缺陷。
- "goroutine 不可取消 + 三张 map 无淘汰"——两半都属实但都不产出失败场景，finding 原文写着 "Not a correctness bug today"；map 那半是 round 1 已记录的未决项，也不是新东西。见 5.2。

### 5.4 auth 边界本身是成立的（B8 的缺陷是**缺少写**，不是**多给了读**）

lane 尝试构造"持有 X 凭据者被授权成非 X 家族的东西"，**构造不出来**，逐条排除记录在此免得重查：`ValidateNID`（`^[a-z0-9-]{1,32}$`，Go 的 `$` 是文本结尾、无 `(?m)`）在该臂**之前**运行，所以没有 unicode/NUL/换行/元字符/长度旁路；`parseRole` 的 `SplitN` 把 `:` 挡在 sid 外、`ValidateNID` 把它挡在 nid 外；`SplitLeaseName` 拒绝前导零（`gpu1-002` 的切分字符是 `0` 不是 `-`）、拒绝 `-01`（`n < FirstLeaseSuffix`），`x-02-03` 的 base 是 `x-02`，而只有 `x-02` 自己**有绑定**时才被采信（租约从不建绑定，所以没有递归提权）；一个**确实 provisioned** 的、字面叫 `gpu1-02` 的真实节点在 `:322` 的 `bound != fp` 臂就被拦下，走不到 fallback；`PermissionsForAgent` 完全 nid 作用域，`handleRegister` 的 `nid_mismatch` 把 body nid 钉在受 ACL 约束的 subject 上。

### 5.5 farewell 的伪造面：guard **对要紧的那件事是够的**

释放一个活名需要持有者的 26 字符（130 位）instance id，而 clone 能持有的任何凭据都够不到它：agent 在 `s.<sid>.cmd.node.<nid>.*.req.forwarded` 上**没有 pub 权**（发不出 claim-probe），在 `ctrl.s.*.node.*.register.req` 上**没有 sub 权**（截不到 farewell），只有 broker AuthUsers 订阅那里（`internal/auth/permissions.go:192-194` vs `274-276`）。残余的伪造能力**精确地就是 M4 那次 cache flush**，别的没有。

### 5.6 "farewell 丢失是安全方向"——成立；坏的是**送达**

lane 的任务是攻击"纯优化"这个声明。结论：**丢失（kill -9 / crash / 分区）时确实什么都不坏**，裁决照常经 grant window / interest probe / 心跳钟收敛。真正的问题是 B6——**它送达时反而坏**。这半个声明成立，请不要在下一轮把"farewell 可能丢失"重新报成缺陷。

### 5.7 其他明确定性

- **m11（降级实例重新 ACK proxy_ready）判为 MINOR 而非 MAJOR 的理由**（记录下来免得重新升级）：`/sub` 按 `proxy_capable=1` 过滤，告警计数取自 `onlineNIDs`（同样 `proxy_capable=1`），所以 `decideProxyEvents` 骗不到、订阅者不受影响；遗留的 SS listener 绑 `127.0.0.1`（`internal/agent/ssproxy/server.go:260`），远端不可达。
- **m7（`recoverFromFailedExec` 无 target 检查）先于本增量存在**，对同宿主不同 nid 的兄弟同样可达，因此**不是 round-1 修法的缺陷**；报它是因为 clone 部署让"共享二进制目录上的并发 exec"从异类变成常态。
- **m16（receiver-as-first-parameter）是既有惯例且带就地理由注释**，不是本增量偷渡的技术债；它只是这个惯例迄今**最大的一次应用**，所以要求定策而不是判缺陷。
- **B9 的"无退避"措辞需要收紧**：refuse 臂确实有 `time.After(a.cfg.RegisterRetryInitial)`（默认 100ms）的暂停，所以不是纯自旋；准确表述是"固定 ~100ms 间隔、无指数退避、无上限、每轮都是一次完整的 CONNECT + auth_callout"。

---

## 6. 覆盖缺口（本轮 19 条 lane 都没能触及的地方）

1. **真实多 broker 集群下的租约裁决全线未跑**。所有 lane 都在单模/嵌入式总线上工作。leader 变更清空 `leaseHolder`、raft 滞后与 auth callout 的 queue group、`VerbReconcile` 的转发路径（今天无 producer）、跨 broker 的 `proxy_ready` 收敛在租约改名下的行为——一次都没有真跑过。m9（`resolveReconcile`）正是这个盲区的可见部分。
2. **N-1 真实二进制互操作**。B1 是拿手写的旧 struct 模拟旧 broker 得到的，**没有人拿 v0.5.0 的真二进制跑过四象限**（旧 broker×新 agent、新 broker×旧 agent、旧 ctl×新 broker、新 ctl×旧 broker）。考虑到 B1 的破坏性与"回滚是一等公民"，这一格应当在外审前用真二进制补一次。
3. **部署层：drill 83 之外的一切**。没有跑过 N≥3 的 clone 在真实 systemd + 共享 NFS home 上的**长跑**（数小时）：日志轮转互删（m10）、升级 flock 争用、共享 marker 的 BootCount 竞争（M9）、`~/.tether` 上的 state.json lost update（M3）都只在推理与单测层面验证过。
4. **后缀空间的边界行为**：`MaxInstancesPerBasename`(64) 与 98 个后缀槽位的耗尽路径只在单测里构造过；M15（每次重启换名）+ m12（后缀被持久化）叠加走向耗尽的真实速率没有测过。
5. **transfer 在租约名下的端到端**。I2 声称 transfers 可用，本轮**没有任何 lane 跑过 transfer**（`internal/agent/transfer.go` 在本增量的改动清单里）。
6. **events / audit 在改名前后的连续性**：同一台物理机跨一次 adoption 的审计可追溯性（本报告的 B2/B3 显示审计会被记到错误的 nid 下），以及 `tether events` 的用户体验，没有被系统检查。
7. **除 `proxy status` 外的 ctl 真实终端渲染**：`node ls` / `ps` / `expose ls` 对 `-NN` 行的实际输出没人跑过（M12 的渲染半就是在这个盲区里被发现的，其余视图同样可能有 `--json`-only 的信息）。
8. **规模/性能**：64 个 clone 同时 register 时 register dispatch goroutine 的 head-of-line 行为（m1 只测了单个 agent 的重复 15ms）。
9. **15 分钟 fail-closed 的真实计时路径**未端到端跑过（M2 是构造 `failClosedFire` 直接验证的）。
10. **泄漏门从未驱动过 contested register**，所以 m3 的 goroutine 今天不可能变红——这是覆盖缺口而非缺陷。
11. **非 Linux 平台**（macOS 上的门与 `syscall.Exec` 血统）本轮零覆盖。
---

# 主进程处置（step 5）

> 以下由主进程逐条评估并落实。**只有主进程能改实现**；专家的贡献是发现与新增测试。
> 三道硬闸在全部修改之后重跑：`make test` rc=0、`make e2e-parallel` ALL PASS、`make lint` rc=0；
> deploy-tier drill 83 = 16/16，且**对改动前代码录得 12/16（4 条 PRODUCT-RED）**。

## 9 条 BLOCKER —— 全部采纳，全部已修

| # | 处置 | 落点 |
|---|---|---|
| **B1** farewell 在 N-1 broker 上是破坏性 register | **采纳，按建议的首选方案修** | 告别体带 `RosterRefreshOnly:true`（旧 broker 纯读短路），新 broker 把 `ReleasingName` 臂**提到**该短路之前 |
| **B2** `PreviousNID` 被当作整名所有权 | **采纳** | 所有权只认当前名；旧名只是"去哪里找我重新呈报的行"的位置，绝不据以关闭任何行 |
| **B3** 只搭一次车、行从不改档 | **采纳，用建议的"改档代替记忆"** | 新增 `proc.Refile`；被真正重新呈报的行**移动**到新名下，此后是普通行，不再需要任何记忆 |
| **B4** exec 血统恢复把 `PreviousNID` 戳成 basename | **采纳，按建议拆开** | 新增 `resumeRoutingNID`（恢复身份但不设 previousNID、不计 adoption），`Agent.New` 的 restore 路径改调它 |
| **B5** `probeCache` 缓存"相对于提问者"的答案 | **采纳** | 缓存改存**客观观测** `probeAnswer{answered, responder, definitive}`，各提问者用 `heldByOther(self)` 各自渲染；inline 结果**不入缓存**、缓存**用后即删** |
| **B6** farewell 删掉 silence rule 唯一的证据 | **采纳** | 改为**标记** `leaseGrant.released` 而非删除条目：既让出名字，又保留"上一任是 lease-aware"这个事实 |
| **B7** 升级 marker 绑在只能跨 exec 存活的血统上 | **采纳，按建议的首选方案修** | boot shim 的 `bootContinuePending` 臂**接管** `TargetInstance`（到达该臂即所有权证明），并把 id 放回环境供 `Agent.New` 继承 |
| **B8** 后缀 fallback 抢在 PIN bootstrap 前 | **采纳，按建议的正解（register-time flag）修** | 新增迁移 `0019_nodes_leased.sql`；`nodes.leased` 由 agent 的 `LeasedNID` 决定，两条保护改读它；名字分配额外排除 `leased=0` 的行**不论 status** |
| **B9** 带 `-NN` 的 basename 被塌缩 | **采纳** | agent 侧改用与 broker **同一条**折叠规则（`proto.BasenameOf`），歧义无法消除但两侧必须同样地消解它 |

## 需要指出的一处：B7 的首选方案带了一个陷阱

报告建议"把 `mintInstanceID` 提到 boot shim 并**必须 memoize**"。**memoize 是错的**，我实现后立刻被 `test/p2` 抓住：
它是**进程级**缓存，于是一个进程里构造的每个 Agent 拿到**同一个 instance id** —— 而两个实例共享一个 id
正是 broker 眼中的"同一实例重连"，克隆分裂当场失效（两个都注册成 `jupyter`，`make test` 红）。

改成**把 id 放回环境**（`adoptBootInstanceID`）：boot shim 消费后重新 `Setenv`，`Agent.New` 照常继承并再次消费。
这满足 B7 要的"两处拿到同一个 id"，又不改变"每个 Agent 各自铸一个"的语义。
建议本身是对的，它给出的实现手段有副作用——记在这里，因为下一个读 B7 的人会照着做。

## MAJOR 的处置

- **M1**（四条 lane 独立发现）`dropLease` 从不重新挂回 state store：**已修**，新增 `stateStore.reattach()`，
  与 `resumeRoutingNID` 里的 detach 对称，并带变异验证的守卫。
- **M2 及其余 MAJOR/MINOR**：**已阅、未在本轮实现**。它们不改变本增量的正确性结论（三道硬闸 + drill 全绿），
  但确实是真问题。**不宣称已解决**——留给外审判定优先级，或作为后续叶子增量。
  其中 **M13/M14（proxy 的 I2 例外无文档、`upgrade --all` 的跳过只报一个裸计数）** 是用户
  「只多出一个带后缀的设备、用法照旧」这条设计哲学的直接缺口，建议优先。

## 闸门与预算的变动（按 CLAUDE.md「改闸门走同一流程」）

| 闸门 | 变动 | 为什么这个预算该动 |
|---|---|---|
| wire 字段账本 | +9 条（本增量全部字段，含 `releasing_name`） | 全部 additive/omitempty，`ProtoVersion` 未 bump |
| wire freeze key set（手改） | `NodeRegisterReq` +`releasing_name`；`node.RegisterInput` +`Leased` | 同上；后者是本地结构，不上 wire |
| migration 账本 | +`0019_nodes_leased.sql` | 加列 `DEFAULT 0`，对存量行与 N-1 agent 都是正确取值 |
| cfgdb 棘轮 | 118→119，且 `reconcileOnRegister` 2→1、新增 `reconcilePortsOnRegister` 1 / `refileProc` 1 | 净增一处：`refileProc` 的单模直写。端口那处是**搬迁**不是新增，账本按要求同一次改动里跟着降 |
| 结构预算 | **未动** | 三个新函数都写成**包级函数取 `*Broker`**，与 `adjudicateLease` 同形同因——方法数被棘轮钉死，那正是它的价值 |
| 命名冻结 | `lease_basename_collapse_review_demo_test.go` → `lease_basename_collapse_test.go` | 按职责命名；同时删除专家的 `zz_lane_probe_cache_demo_test.go`（其结论已被正式守卫 `lease_probe_cache_*_test.go` 覆盖）|
| maintidx | **未加豁免**，改为提取 | `reconcileOnRegister` 因本轮改动跌到 MI=15。提取 `livePIDsByRow` / `adoptRowsCarriedAcrossARename` / `reconcilePortsOnRegister` 三段（**注释整段随代码搬走**）后回到阈值以上 |
