# proxy-lifecycle — 内审报告（阶段 C step 4 + 主进程处置 step 5）

> 编排：6 个视角 reviewer（并发／安全／状态机／测试有效性／契约兼容／仓库纪律）→ 每 lane 一个**无条件 spawn** 的
> 对抗性 verifier（输入为空也 spawn，返回空 verdicts）→ 1 个综合。共 13 agent，全部继承会话主模型。
> 专家只读实现、可提测试建议，**不改实现代码**；本文的采纳/驳回由主进程定夺（CLAUDE.md §4）。

**规模**：50 findings（3 BLOCKER / 18 MAJOR / 20 MINOR / 9 NIT）→ verifier 确认 **37**、驳回 **12**。

---

## 1. 本轮最重要的结论

**这一轮内审抓到的，几乎全是我自己在阶段 B 引入的缺陷**，而不是原设计的问题。其中三条是**我加的守卫本身就是它要防的形状**：

1. 我加 `agentExiting` 闩来防「fail-closed 与 agent 退出竞态擦掉 footprint」——而我把它实现成了 **TOCTOU**：检查在一个 `p.mu` 临界区、teardown 在另一个，退出路径整个挤得进中间。**闩存在，窗口照旧**。
2. 我写了停止者表来钉住「session 重建不得杀 SS」——而那两行是**恒等式**：ctx 在 server 建好之后才安装，cancel 的是 server 从未用过的那个。reviewer 把事故本身重新注入代码，**整个仓库仍然全绿**。
3. 我把 `proxyBound` 改成诚实谓词以终结 READY 谎言——却**漏掉了 `onNATSReconnect` 的 re-ACK**，它仍用 `p.srv != nil` 发布 `ready=true`。四个 lane 独立发现了同一处。

第 2 条尤其值得记住：**一条声称覆盖事故的回归测试，在事故被完整重放时不会失败**。它通过的原因与被测行为无关。这不是"测试不够严"，是"测试测的不是它写着要测的东西"。

## 2. 已修复（逐条，全部带变异验证）

| 级别 | finding | 处置 |
|---|---|---|
| BLOCKER | `failClosedFire` 的 `agentExiting` 检查与 teardown 不原子（两个 lane 独立发现，其一用阻塞 slog handler 实测复现） | 合并为**一个**临界区，日志移进锁内（有界，一条 Warn） |
| BLOCKER | 停止者表两个 ctx 行是恒等式 | 新增 `setup` 钩子，ctx 在 server **构建前**安装；**变异 11** = 重新注入事故本身，两行都红 |
| MAJOR ×4 | `onNATSReconnect` 残留 READY 谎言站点 | 改用 `servingLocked()`；注明可达路径（`d==nil` 提前 return 不 reap，corpse 存活至此） |
| MAJOR | `refootprint` 绕过 round-6 F7：fail-closed 后无 token 推送即可重开出口 | 加 `!p.needsReestablish` 项；**变异 15** 确认。这是 ⚠R2 同一条原则我自己没贯彻到底的地方 |
| MAJOR | `applyProxyDirective` 忽略 `agentExiting`：退出后迟到的 directive 能建一个**无人能停**的 server | 头部加 shutdown 栅栏；**变异 16** |
| MAJOR | `applyProxyKeysetLocked` 的 ErrStopped 终态臂 **0% 覆盖**（被 `!servingLocked()` 分支拦在前面，端到端几乎不可达） | 直接单元测试该臂；**变异 12** |
| MAJOR | 无测试钉住 `authoritativelyOff` 在 re-enable 时清除（卡住的闩＝永久禁用自愈） | 加测试并断言自愈恢复；**变异 13** |
| MAJOR | 「agent exit」行测的是**函数**不是**接线**——删掉 `Run` 的 defer 全绿 | 加 AST 接线闸门；**变异 14** |
| MAJOR | 停止者表只钉 4 个停止者中的 3 个 | 补 teardown-then-rebuild 行，断言旧实例被 **停止** 而非遗弃 |
| MAJOR | `acceptExited`（`Serving()` 的新半边）无测试，删掉全绿 | 加 `CloseListenerForTest` 构造该状态 + 测试；**变异 17** |
| MAJOR | `allConns` 新纳入 upstream 但无 drain 断言，漏 untrack 即每连接泄漏一个 map 条目 | 加 drain 测试；**变异 18**（`6 entries after 6 relays`） |
| MAJOR | `reapProxyCorpseLocked` 借用 `configWarnAnnounced`，corpse 的 WARN 会**吃掉** one-shot，把随后「无持久 footprint」的失败告警降级为 Debug | 拆出独立的 `corpseWarnAnnounced` |
| MINOR ×3 | `shutdown()` 注释仍描述已删除的 ctx-watch goroutine；`Serving()` 文档谎称 SetKeys 报同一条件（对 `acceptExited` 为假）；backoff 门注释仍写 "no ACK" 而我刚在那里加了 ACK | 三处注释全部改正——本仓视注释为资产，描述已删机制的注释比没有更坏 |
| MINOR | 新闸门未登记进 CLAUDE.md 闸门清单 | 已补一行 |

## 3. 驳回 / 部分驳回（记录在案，避免下轮重复提出）

- ~~**「single-broker 模式下修复无效」——部分驳回。**~~
  **⚠ 这条驳回是错的，已被外审 F4 推翻。三位 lane reviewer 都是对的，错的是我。**
  我当时的论证是：「corpse 期间 broker 仍认为 `ready=true`，所以 `repairProxy` 的『仅因 !ready 而 nudge 就抑制』守卫
  不触发，keyset 推送照常到达」。我读了 Fix D 那道守卫，却**没有读它上面的 CONVERGENCE-FIRST 早退**
  （`internal/broker/proxy.go`：`if on && ready && agentGen == brokerGen && agentEpoch == epoch { return }`）——
  而 corpse 恰好**完全匹配**它：DB 里 `ready` 仍是 true，且 `p.srv != nil` 时 `proxyGenEpoch` 报的正是真实序对。
  于是根本走不到 Fix D，推送永远不会发出。我不但据此否决了三个独立 lane，还把这个错误结论写进了
  `proxyBound` 的代码注释和本报告。
  **真正的修法**（外审后补上）：`reapProxyCorpseOnHeartbeat` 在心跳发布**之前**运行，使该次心跳同时携带
  `ProxyBound=false` **和** `(0,0)`——这个组合两道早退都不匹配，推送才得以发出。
  **方法论教训**：我在一条早退链上只读了自己预期会命中的那一段，并用它去否决多人一致的意见。
  当多个独立 lane 指向同一处而我准备驳回时，举证责任在我，且必须读完整条路径而不是我认为相关的那一段。
- **「⚠R1 该反转成 (0,0)」——维持原裁决。** 提出者的论证只覆盖 ON 路径。反转会让 corpse 收不到权威 OFF
  （`repairProxy` 的 disable 仅在 `agentEpoch > 0` 时推），而 reap 挂在 directive 路径上——收不到 directive 就永不 reap，死锁。
  ON 路径的自愈已由 head reap + refootprint 覆盖，不需要牺牲 OFF 通道。
- **「⚠R3 只落地了一半，`.srv`-nil 账本被丢弃」——维持 ⚠R3。** 那正是 plan 阶段的明确裁决：精确计数账本会在无关改动上误报，
  训练人反射性重置闸门。代之以三条不可反射满足的机械断言。**但提出者有一点对**：残留的 `onNATSReconnect` 站点
  正是账本本会抓到的——所以我用**测试**而非账本覆盖了它。
- **`docs/devices.md` 与 `docs/devices.md.bak.20260820` 的无关漂移（3 个 lane 提）**——不是本增量引入的，会话开始前的 git status
  快照里就有。外审阶段主进程不 `git add`（暂存是外审者的工作），此处仅登记提醒。

## 4. 待外审裁定 / 明确不做

- **`Stop()` 对 in-flight upstream 拨号／DNS 仍非严格有界 —— 我的初判被实测推翻，已升级为外审首要议题。**
  我原本判定「`dialTimeout=10s`，所以有界但慢」。reviewer 给出实测：对黑洞地址 **`Stop() took 9.547s`**，
  并指出两段在 `trackConn(remote)` **之前**、`shutdown()` 够不到的阻塞——`destAllowed` 的 `net.LookupIP`
  （**无 deadline**，受 resolv.conf 支配；agent 默认 `DenyPrivateDestinations`，所以每个域名目标必经此路）
  与 `dialTarget` 的 10s。因为 `Stop()` 在 `p.mu` 下、而 `heartbeatLoop` 每 5s 取两次该锁，15–20s 持锁
  会停心跳，触发 broker `proxyRehomeDwell = 3`（15s）的**端口轮换**——正是 plan §6.3 承诺不发生的事。
  更糟的是**我的测试无法证伪它**：deadline 写成 10s，恰等于 `dialTimeout`。
  处置：①测试阈值改 3s（现在真的有证伪力）；②plan §6.3 的「公网端口不变」**收回无条件承诺**，
  限定为「upstream 无慢拨号时」；③真正的修复（给 dial/DNS 引入可被 shutdown 打断的机制）是独立设计，
  列为外审议题而非本增量的静默债。
  **⚠ 外审后更新**：③也被推翻了——外审 F1 不仅确认必要性，还给出了不重引入事故的修法（server 自有 `stopCtx`），
  已在本增量内实现并由 `TestStopCancelsInFlightDestinationResolution` 钉住。我两次低估了这条：
  先判成"有界但慢"，再判成"独立设计、本增量不做"。**剩余**：`Stop` 在 `p.mu` 下的总上界仍不是显式契约。
- **rebuild 失败可能撞 EADDRINUSE 形成 reap→rebuild-fails 循环**：需要一个"前任仍持有本地端口"的构造才能证伪或证实，
  且 `Start(0, …)` 让 OS 选端口时不复现。登记为待外审判断项。
- **§6.2/§6.3 承诺的「同一次自愈调用内恰好一次 unready 再一次 ready」无断言**：需要一个能捕获 `pubProxyReady` 的
  测试夹具（现有测试一律传 `nil` nc）。承认是缺口，本轮未补。
- **闸门只扫 `ssproxy` 顶层目录**，未来子包可逃逸——真实但今天无子包，登记。

## 5. 变异验证账本（18 条，全部实际执行）

阶段 B 的 10 条见 `proxy-lifecycle-plan.md` §10.2。本轮新增 8 条：

| # | 注入 | 目标 | 结果 |
|---|---|---|---|
| 11 | 在 `proxyStartLocked` 重新注入 ctx-watch goroutine（**事故本身**） | 停止者表两个 ctx 行 | ✅ 两行都红 |
| 12 | ErrStopped 臂回退为 WARN+return | `TestKeysetArmRebuildsWhenSetKeysReportsStopped` | ✅ |
| 13 | `authoritativelyOff` 永不清除 | `TestAuthoritativeOffLatchClearsOnReEnable` | ✅ |
| 14 | 删 `Run` 的 `defer stopProxyOnRunExit` | `TestRunWiresTheAgentExitProxyTeardown` | ✅ |
| 15 | 去掉 `refootprint` 的 F7 栅栏 | `TestFailClosedIsNotUndoneByATokenlessPush` | ✅ |
| 16 | 去掉 shutdown 栅栏 | `TestLateDirectiveAfterAgentExitBuildsNothing` | ✅ |
| 17 | `Serving()` 丢掉 `acceptExited` | `TestServingIsFalseWhenAcceptLoopDiesWithoutClose` | ✅ |
| 18 | 去掉 upstream `untrackConn` | `TestRelayedConnectionsDrainFromTheTrackingMap` | ✅ 精确报 `6 entries after 6 relays` |

**18 条里有 3 条一开始打不红**（阶段 B 的特权端口空转、泄漏门覆盖不足，本轮的恒等式 ctx 行）。若只跑测试不做变异，这三条守卫都会以绿色的样子进外审。
