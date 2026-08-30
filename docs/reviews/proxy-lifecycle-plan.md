# proxy-lifecycle — 把 agent 的 SS proxy 生命周期从 per-session runCtx 解耦（plan）

> 状态：主进程定稿（阶段 A step 2）。起草经 9 专家 workflow（4 视角草拟 → 4 视角对抗互评 → 综合）。
> 本文是实现的唯一尺。实现中若发现设计问题，**先改本文再改代码**。

---

## 1. 事故（2026-08-21，weilandserver，生产）

`weilandserver` 的 agent 对外服务了一个**死出口 7 小时 40 分**，而 broker 全程报告它健康：

- `tether proxy status` → `weilandserver ONLINE READY=true`，出口 `linziyang.top:14004`
- 同一时刻 `ss -tlnp | grep pid=2324` → **agent 进程零 listener**
- `agent.log` 里 `agent: proxy SetKeys err="ssproxy: server stopped"` **5416 条**，每 5s 一条，7.5 小时，从未恢复，写入 4.3 MB
- 每一个被 `/sub` 导到该出口的订阅者都必然连接失败

恢复只能靠 session 级 `tether proxy off && tether proxy on --yes`——它重排了**全部 8 个节点**的公网端口（14004 → 14007，全车队重编号），并强制唯一的活跃订阅者重拉配置。对"一个节点的 SS 死了"而言，这个爆炸半径是荒谬的。

**同日另一现象（同源，见 §3.4）**：拔 wifi 插网线后 agent 静默 5m48s 才重连。二者共享同一条 session-rebuild 路径。

## 2. 根因（已逐条对照源码验证，非推测）

1. `proxyStartLocked` 把调用方的 ctx 交给 SS server：`srv.Start(ctx, wantLocal, keys)`，`internal/agent/proxy.go:396`。
2. 该 ctx 来自 `internal/agent/agent.go:1137` 的 `runCtx`。`runCtx` 在 `session()` 顶部创建（`agent.go:920`），且 `agent.go:394` 的注释明写 **"the C1 session loop REWRITES runCtx every session"**。
3. NATS session 重建（stuck-disconnect watchdog `fireRedial`，`internal/agent/roster.go:620`，`redialAfter = 20s`）触发 `cancelRun()`。
4. ssproxy 的 ctx-watch goroutine（`internal/agent/ssproxy/server.go:285-291`）收到 `ctx.Done()` → `s.shutdown()` → `s.closed = true`（单向 latch，`server.go:352-369`），listener 关闭。
5. agent 侧 `p.srv` **仍指向这具尸体**——`proxyRuntime` 跨 session 存活，无人清理。
6. 新 session 的 register 回包是 **keyset-only（无 Token）**，而这是**正确的 broker 行为**：`internal/broker/proxy.go:462-465` 注释 *"Token empty → agent reuses its persisted token"*。broker 没有做错任何事。
7. `applyProxyDirective` 因 `p.srv != nil` 走 `default:` 分支（`internal/agent/proxy.go:315-320`），`p.srv.SetKeys()` 撞 `s.closed` 返回 `ssproxy: server stopped`，函数**只打一条 WARN 就 `return`**——不清理、不重建。
8. 此后 broker 每 ~5s 推一次 keyset，每次撞同一堵墙。永不自愈。

### 2.1 这是策略违背，不只是漏了一个 nil 检查

`armFailClosed`（`internal/agent/proxy.go:771-791`）是 SS server 的**既定 teardown 策略**：分区超过 `ProxyFailClosedGrace`（默认 15 分钟）才主动停 SS，以免被吊销的订阅者绕过 broker 的 OFFLINE→port-REVOKE 窗口继续 egress。而 runCtx 耦合让**一次普通的 session 重建在 ~20s 内就杀掉 SS**——这条 15 分钟宽限在该路径上等同死代码。控制面事件正在拆毁策略明确规定它不该拆的数据面。

### 2.2 不对称是 bug 本身

数据面的**另一半**——tunnel client——锚在**进程 ctx** 上（`cmd/tether/agent.go` 交给 `a.Run` 的那个 ctx），跨 session 重建存活。同一个数据面的两半锚在不同生命周期上，这个不对称就是缺陷的形状。仓库里已有同款先例并写明了理由：proc-event courier 绑 parent ctx，`agent.go:846-849` 注释 *"outlives session rebuilds … exactly one goroutine per agent lifetime (leak-gate covered)"*。

## 3. 核心设计决策

**SS server 不接受任何 ctx。**

不是"换一根绳子"（锚到 `Run` 的 ctx 或新造一个 `lifeCtx`），而是移除 `ssproxy.Server.Start` 的 ctx 参数。理由：

- 换锚只是把"谁能杀它"从一个 ctx 换成另一个，**停止者集合仍然是隐式的**——任何未来持有该 ctx 的人都能意外杀死它，正如今天。
- 去掉 ctx 后 `Stop()` 成为**唯一**停止者，停止者集合变成一份闭合、可 grep、可被闸门钉住的清单：
  1. 权威 OFF（`proxyTeardownLocked` clearPersist=true）
  2. fail-closed 触发（`failClosedFire`）
  3. teardown-then-rebuild（`d.Token != ""` 分支）
  4. agent 退出（新增 `Run` 的 defer）
- `internal/agent/ssproxy` 包在此之后**完全不 import context**——这是"控制面 ctx 不能绑定数据面对象生命周期"的机械形式，可由架构闸门断言。
- 不引入任何新的 `context.Background()` 站点（CLAUDE.md §5 要求逐站点标注 `ctx-root:`/`ctx-none:`）。

**无 wire 改动**：`internal/proto` 不增删改任何字段，不 bump `ProtoVersion`。`ProxyBound`（`internal/proto/messages.go`）只是开始说真话。

## 4. 主进程裁决（对专家 finding 的逐条处置）

四位 critic 一致推举 Draft 2（recovery 视角）为核心方案。以下是我**采纳、修正、驳回**的记录。凡与 Draft 2 不同者标 ⚠。

### 4.1 采纳（已独立验证）

| # | finding | 我的验证 |
|---|---|---|
| A1 | `ssproxy.Server.Start` 对已停止的 server 返回**假成功**：`if s.ln != nil { return s.localPort, nil }`(`server.go:254-256`) 排在 `if s.closed`(`:257-259`) **之前**，而 `shutdown()` 关闭 `s.ln` 却**不置 nil**(`:362-364`) | ✅ 读码确认。今天不可达仅因 `proxyStartLocked` 每次 `ssproxy.New()`；本增量新增重建路径后**会**踩上 |
| A2 | `Stop()` 无界，且在 `p.mu` 下调用。`allConns` 注释自陈是 "every **accepted** conn"(`server.go:50`)——**upstream `remote` 不在其中**；`relay` 的 `wg.Wait()`(`:564-578`) 需双向 copy 都结束，而 `io.Copy(w, remote)` 阻塞在 `remote.Read()`，关闭 client 不影响它 | ✅ 读码确认。upstream 若不响应 half-close 即无限期挂住 `proxyTeardownLocked`(`proxy.go:565`)。与 gotcha #72（无界 teardown）同类，且本增量让该路径**变常走** |
| A3 | maintidx 边界：`applyProxyDirective` 与 `session` 均 MI=20，阈值 `under: 20`，且**都不在** `.golangci.yml:409-416` 的 god-function 豁免名单里 | ✅ 实测：`applyProxyDirective` CC=40 / MI=**20**；`session` CC=19 / MI=**20**。**加任何代码即红** |
| A4 | 测试应是**一张 `{verb, killFn}` 表**而非散落用例 | ✅ 采纳。这正是本仓自己的教训：`test/determinism/test_naming_test.go` 头部记载 tunnel fence 因未写成表而被 round2/5/6 重复发现三次 |
| A5 | 缺 N-1 四象限与 rollback 声明 | ✅ 采纳，见 §8 |
| A6 | 变异验证成本未计价 | ✅ 采纳，见 §7.3 |
| A7 | 自愈的用户可见爆炸半径未声明 | ✅ 采纳，见 §6.3 |
| A8 | `SetKeys` 那条 WARN 是 `applyProxyDirective` 里**最后一处无预算的 per-push WARN**（兄弟分支都走 `logConfigWarnOnce` 或 once-then-Debug） | ✅ 采纳，见 step 5 |

### 4.2 ⚠ 修正 Draft 2（三处）

**⚠ R1 — `proxyGenEpoch` 不得因 corpse 归零。**
Draft 2 step 2 要把 `proxyGenEpoch`(`proxy.go:118`) 的 `if p.srv == nil` 改成 `if !p.servingLocked()`，使 corpse 报 `(0,0)`。**驳回。**
理由：broker 的 `repairProxy` 在 proxy 已 OFF 的分支上，**只有 `agentEpoch > 0` 才推 disable**（`internal/broker/proxy.go:739-741`，已核）。corpse 报 `(0,0)` 会让该节点**永远收不到权威 OFF**；而 head-reap 挂在 directive 处理路径上，收不到 directive 就永远不被 reap——**死锁**。
裁决：`proxyGenEpoch` 的语义是「我最后成功应用的 directive 序对」，是**排序信息**，不是活性断言，**保持报真实 (gen, epoch)**。活性断言归 `proxyBound`(`proxy.go:133`)，那里改用 `servingLocked()`。于是 corpse 对 broker 呈现为「应用过 epoch N **且** ProxyBound=false」——既保住 OFF 通道，又如实暴露不健康。

**⚠ R2 — 安全栅栏不得建立在未检查的磁盘写上。**
Draft 2 step 4 的 `refootprint` 放行论证依赖「权威 OFF 会擦掉 footprint」，但那个擦除是 `_ = a.stateStore.SetProxy(nil)`（`proxy.go:572-574`，**错误被丢弃**），且 `loadProxyStateSafe` 同样吞读错误。一次失败的磁盘写就让被吊销的出口可被无 token 的 push 复活。
裁决：新增**纯内存 latch** `p.authoritativelyOff bool`，在 clearPersist teardown 时置位、在任何成功 (re)build 时清位；`refootprint` 放行必须 `!p.authoritativelyOff`。内存 latch 不依赖磁盘写成功，且进程重启后本就没有活着的 SS server 可复活。

**⚠ R3 — 架构闸门不用精确站点计数。**
Draft 2 step 9 要对 `.srv != nil` 站点做**精确数目**钉死。critic 正确指出这会在无关改动上误报（TLS 配对门那种精确计数之所以成立，是因为那些站点本身就该逼人去读）。
裁决：闸门只保留**两条机械断言**：(a) `internal/agent/ssproxy` 的非测试文件**不得 import `context`**；(b) 非测试文件中对 `.srv` 与 nil 的比较，其**所在函数**必须落在一份**只减不增的账本**里（键 = `file:function`，与 §5b 的命名冻结账本同构）。不钉总数，钉集合。

### 4.3 驳回

- **改 `ProxyFailClosedGrace` 默认值（15min）**：本增量让它从死代码变成真正的 teardown deadline，确有讨论价值，但调整它是**独立的安全策略决策**，需要单独论证与外审。本增量**不动**该值，只在 §6.2 记录这一后果。混进来会让本增量的安全论证与一个未经论证的数值绑定。
- **让 broker 侧新增 per-node 修复动词**：真实缺口（§9 记录），但那是 broker 面的独立叶子增量，且需要 wire/CLI 表面变更。本增量是 agent-only 且零 wire 改动，混入会毁掉这个性质。

## 5. 实施步骤

> 顺序即依赖顺序。每步单独可编译。

**S1 — `ssproxy.Server` 去 ctx。**`internal/agent/ssproxy/server.go`
`Start(ctx, wantLocalPort, keys)` → `Start(wantLocalPort, keys)`；删 ctx-watch goroutine(`:285-291`)、`s.ctx/s.cancel` 字段与赋值(`:281`)、`shutdown` 里的 cancel 块；删 `context` import（此后全包不 import context）。**整段搬运既有注释**（CLAUDE.md §5：注释是资产）。

**S2 — 修 A1 的假成功。**`internal/agent/ssproxy/server.go`
把 `if s.closed` 检查**移到** `if s.ln != nil` 早退**之前**，使对已停止 server 的 `Start` 返回明确错误而非 `(oldPort, nil)`。原地留 `// origin:` 注释说明这个顺序是载重的。

**S3 — 修 A2 的无界 Stop。**`internal/agent/ssproxy/server.go`
`relay` 的 upstream `remote` 也纳入 `shutdown()` 的关闭集（track 之，或在 relay 内注册 cancel 钩子），使双向 copy 在 shutdown 后**确定性**返回、`Stop()` 有界。**必须带一个"upstream 静默不响应 half-close"的回归测试**，否则这条修复无法证明。

**S4 — 诚实的活性谓词。**`internal/agent/ssproxy/server.go` + `internal/agent/proxy.go`
ssproxy 加 `Serving() bool`（`s.ln != nil && !s.closed`，acceptLoop 退出亦计入）与 `var ErrStopped = errors.New("ssproxy: server stopped")`（**保留原字符串**，drill 日志 oracle 与运维 grep 习惯不破）。proxy.go 加 `func (p *proxyRuntime) servingLocked() bool`。
改 `proxyBound`(`:133`) 用 `servingLocked()`。**`proxyGenEpoch`(`:118`) 按 ⚠R1 保持不变。**

**S5 — corpse 收割 + SetKeys 终态化，全部提取为独立函数（A3 强制）。**`internal/agent/proxy.go`
- 新增 free function `reapProxyCorpseLocked(a, p, nc) bool`：`p.srv != nil && !p.srv.Serving()` 时 teardown（clearPersist=false，保留 footprint）、publish unready、返回 true。
- 新增 free function `bootstrapProxyFromFootprintLocked(...)`：把现有 `case p.srv == nil` 分支体(`:302-313`)原样搬出。
- `applyProxyDirective` 内**只增加两处调用**，其余逻辑一律在新函数里——`applyProxyDirective` 的 MI 必须**不降**（实测门，见 §7.4）。
- `default:` 分支：`errors.Is(err, ssproxy.ErrStopped)` → 收割并在**同一次调用内**重建，而非 `return`。WARN 走 once-then-Debug（A8）。

**S6 — `authoritativelyOff` 内存 latch（⚠R2）+ `refootprint` 放行。**`internal/agent/proxy.go`

**S7 — backoff 门早退时 publish unready。**`internal/agent/proxy.go:365-369`
该处注释当前断言 *"no SS server, no dial, no ACK — the node simply stays unready"*，但代码并未 ACK unready。补上并同步注释。

**S8 — agent 退出时显式停 SS。**`internal/agent/agent.go`
`Run` 中在 `defer a.stopRedialWatchdog()` / `defer a.cancelFailClosed()` 旁加 `defer stopProxyOnRunExit(a)`。⚠ 必须防 `Timer.Stop()` **不 join 正在运行的 AfterFunc**：`failClosedFire` 可能在退出 teardown 之后拿到 `p.mu` 并执行 clearPersist teardown，擦掉 footprint。需一个 latch 使退出后的 fire 成为 no-op。

**S9 — 调用点更新。**去掉 `applyProxyDirective` / `proxyStartLocked` 的 ctx 参数（`unparam` 已启用，留着未用参数会被报）。生产调用点三处 + 测试调用点若干。

**S10 — 架构闸门（⚠R3 形式）。**`test/architecture/` 下按职责命名的新文件。

**S11 — 文档。**`docs/deploy-tier-gotchas.md`：本机制与 **#33**（proxy exit crash-rehome 后 SS 数据面恢复滞后，**根因未归因**，`:134-164`）的症状/workaround/非确定性完全吻合，登记为**候选归因**（不宣称已闭合——#33 的首个根因假说已被撤回过一次）。同时新登记本次生产事故本身。

## 6. 风险

### 6.1 安全：egress 窗口变宽（诚实登记）

今天 runCtx 耦合在 ~20s 内杀掉 SS，属**意外的保守**。修复后，唯一的分区 teardown deadline 是 `ProxyFailClosedGrace`（15min）。被吊销的订阅者在 agent 与 broker 分区期间的可用窗口从 ~20s 扩到 15min。
缓解：这正是该策略**本来写明的意图**；key 轮换经 `SetKeys` 每 ~5s 推达仍然有效（连接侧硬撤销：`setKeysLocked` 强关不在新 key 集里的连接）；`ProxyFailClosedGrace` 的调值按 §4.3 留作独立增量。

### 6.2 自愈可能触发 M3 换端口

自愈路径会先 publish unready。cluster 模式下连续 3 次 unready 会触发 reaper 的 `proxyRehomeDwell` 轮换并**重新铸端口**。缓解：收割与重建在**同一次 `applyProxyDirective` 调用内**完成，unready 窗口是单次 push 间隔量级，不足以累计 3 次；须有测试钉住"收割后同一调用内重建成功则不发生第二次 unready"。

### 6.3 用户可见爆炸半径（契约化）

一次成功自愈 = **该节点每个在途订阅者一次连接重置**（`shutdown()` 关闭全部被跟踪连接）+ 重开隧道，**公网端口不变**。这是 plan 承诺的契约，须有断言。

> **⚠ 契约收窄（内审 MAJOR，带实测数据）**：「公网端口不变」**只在 upstream 无慢拨号时成立**。
> `Stop()` 是在 `p.mu` 下调用的（`proxyTeardownLocked`），而 `handleConn` 里有两段在 `trackConn(remote)`
> **之前**、`shutdown()` 关不掉的阻塞：`destAllowed` 的 `net.LookupIP`（**无任何 deadline**，受 resolv.conf 支配，
> 典型 30s；且 agent 默认 `DenyPrivateDestinations`，所以每个域名目标都走这里）与 `dialTarget` 的
> `dialTimeout = 10s`（黑洞 SYN 会烧满）。内审实测：对黑洞地址拨号时 **`Stop() took 9.547s`**。
> 后果链是实的：`heartbeatLoop` 每 5s 要取两次 `p.mu`，15–20s 持锁即停心跳，而 broker 的
> `proxyRehomeDwell = 3` × 5s tick 会在 15s 不 ready 后**轮换分配、铸新端口**——恰是本节承诺不发生的事。
> 本增量**不修**（修复要给 dial/DNS 路径引入可被 shutdown 打断的机制，是独立设计），
> 但**收回无条件承诺**并列为外审议题。测试阈值已从 10s（== dialTimeout，无证伪力）改为 3s。
>
> **⚠ 外审后更新（F1，已修）**：上面那句「本增量不修」被外审推翻——它给出了修法且证明了必要性。
> `Server` 现在持有**自己创建的** `stopCtx`，`shutdown()` 在关闭 socket 前 cancel，`destAllowed`/`dialTarget`
> 改用 `LookupIPAddr(ctx,…)`/`DialContext(ctx,…)`，两段 pre-track 阻塞都可被 `Stop()` 打断。
> 这同时纠正了本 plan §3 的一个过度推广：「SS server 不接受任何 ctx」对**生命周期所有权**成立，
> 但我把它推广成了对**取消能力**的禁令，而后者恰恰是修复 hang 所需要的机制。闸门已按语义改写（外审 F5）。
> **仍未闭合**：`Stop()` 在 `p.mu` 下的**总**上界还不是显式契约——取消已到位，但没有一个"teardown 预算"
> 把 relay 收尾 + 连接关闭的总时间钉住。这是外审留下的、我认可的下一个议题。

### 6.4 `applyProxyDirective` 的 MI 边界

见 A3。缓解 = S5 的强制提取 + §7.4 的实测门。

## 7. 测试

### 7.1 一张表（A4）

单一表驱动测试覆盖**停止者闭集**：`{authoritative OFF, fail-closed fire, agent exit, teardown-then-rebuild}` × `{SS 是否应存活}`，外加**反向**用例：`{session rebuild, NATS reconnect, roster refresh, lease rename}` 必须**全部不杀** SS。新停止者出现时加一行，而不是新开一个文件。

### 7.2 必须存在的断言

1. session 重建后 SS server 仍 `Serving()`，且 keyset-only directive 的 `SetKeys` 成功（**事故的直接回归**）
2. 人为制造 corpse → 一次 keyset-only push 后恢复 `Serving()`，公网端口不变
3. 权威 OFF 后，`refootprint` **不**放行重建（⚠R2 的内存 latch；并模拟 `SetProxy` 写失败仍不放行）
4. 对已停止的 `ssproxy.Server` 调 `Start` 返回错误，**不**返回 `(oldPort, nil)`（S2）
5. upstream 静默不响应 half-close 时 `Stop()` 仍在界内返回（S3）
6. `Run` 退出后并发触发的 `failClosedFire` 不擦 footprint（S8）
7. corpse 状态下 `proxyGenEpoch` 仍报真实 (gen, epoch)、`proxyBound` 报 false（⚠R1）
8. 全部并发面带 `-race` + 仓库内建 NumGoroutine/fd 泄漏门（**非 goleak**）

### 7.3 变异验证预算（A6）

每条新守卫必须注入其声称能抓的缺陷并确认变红。预估 8–10 个变异 × `go test -race ./internal/agent/`（实测基线 23s）≈ 4–6 分钟纯跑测时间，**在隔离工作树中执行**，避免半应用的变异污染主树。

### 7.4 闸门

`make test` + `make e2e-parallel` + `make lint` 全绿；另加实测 `applyProxyDirective` 的 MI **不低于 20**（A3）。

## 8. 升级与回滚（A5）

- **agent-only，零 wire 改动**：不改 `internal/proto`，不 bump `ProtoVersion`，`internal/proto/wire_inventory_test.go` 账本不动。
- **四象限**：(新 agent, 旧 broker) — broker 行为不变，新 agent 只是 `ProxyBound` 更诚实、能自愈；(旧 agent, 新 broker) — broker 未改，恒等；其余两象限平凡。
- **顺序**：无 broker 前置。可任意顺序滚动。
- **回滚**：退回前一 release 的 agent 即恢复今天的行为（含本缺陷），**不卡任何路径**。
- **retrofit**：事故期间 OFFLINE 超过 `PortRevokeAfter` 的节点其 `__proxy__` 行可能已被 REVOKE；升级后首次 register 会重新分配。无需人工步骤。

## 9. 显式不做

- 不改 `ProxyFailClosedGrace` 默认值（§4.3）
- 不新增 broker 侧 per-node proxy 修复动词（§4.3，登记为后续叶子增量）
- 不碰 #29/#34（cluster home eligibility，独立增量）
- 不改 nats.go 的 `PingInterval`/`MaxPingsOut`（§1 提到的 5m48s 重连窗口是**同源但不同面**的问题，单独登记）

---

## 10. 实现回执（阶段 B 完成后回写）

### 10.1 计划外但必要的改动

- **S9 的延伸：删掉一段死代码。** 去掉 `applyProxyDirective` 的 ctx 参数后，
  `handleProxyKeysForwarded` 里的 `ctx := a.loadRunCtx(); if ctx == nil { ctx = context.Background() }`
  整块失去了唯一消费者。`make lint` 对同一行同时报出 `ineffassign` + `SA4006` + `SA4017` + `wastedassign`
  四条。这不是负担而是**印证**：连"取一个 session ctx"这件事本身都不再需要，说明解耦是彻底的而非表面的。
  （`onNATSReconnect` 里另一处同形代码**保留**——那里的 ctx 有真实消费者，lint 也没报它。）

### 10.2 变异验证账本（10 条，全部实际执行）

| # | 注入的缺陷 | 目标守卫 | 结果 |
|---|---|---|---|
| 1 | 去掉 `handleConn` 对 upstream conn 的 track | `TestStopIsBoundedWhenUpstreamIgnoresHalfClose` | ✅ 挂满 10s 后红 |
| 2 | `Start` 的 `closed` 检查移回 `ln` 检查之后 | `TestStartOnStoppedServerFailsInsteadOfReturningDeadPort` | ✅ 红，报出 `(35263, nil)` |
| 3 | 移除 `applyProxyDirective` 头部的 corpse 收割 | `TestCorpseIsRebuiltByTheNextKeysetPush` | ✅ 红 |
| 4 | 移除 `refootprint` 的 `!p.authoritativelyOff` 条件 | `TestAuthoritativeOffIsNotUndoneByAKeysetPush` | ⚠️ **首次未红** → 见 §10.3 |
| 5 | `proxyBound` 退回 `p.srv != nil` | `TestCorpseReportsUnboundButKeepsItsAppliedPair` | ✅ 红 |
| 6 | `failClosedFire` 忽略 `agentExiting` | `TestFailClosedDuringAgentExitKeepsTheFootprint` | ✅ 红 |
| 7 | `stopProxyOnRunExit` 不做 teardown | 停止者表的 `agent exit` 行 | ✅ 红（精确到那一行） |
| 8 | 给 `internal/agent/ssproxy` 加 `context` import | `TestSSProxyPackageTakesNoContext` | ✅ 红 |
| 9 | 给 `applyProxyDirective` 加回 ctx 参数 | `TestProxyDirectivePathTakesNoContext` | ✅ 红 |
| 10 | `proxyTeardownLocked` 不调 `srv.Stop()` | `TestRepeatedCorpseRebuildDoesNotLeakGoroutines` | ⚠️ **首次未红** → 见 §10.3 |

### 10.3 变异揪出的两个空转测试（本轮最有价值的产出）

**10 条变异里有 2 条一开始打不红。若只跑测试不做变异，这两条守卫都会以"绿色"的样子进外审。**

- **#4（安全守卫空转）**：测试给 footprint 播种了 `LocalPort: 1`——特权端口，`net.Listen("127.0.0.1:1")`
  直接权限拒绝。于是重建**因启动失败而没发生**，测试断言"没在服务"恒真，与 latch 在不在毫无关系。
  改 `LocalPort: 0` 并加前置断言后，同一变异让被吊销的出口真的复活了，守卫成立。
- **#10（泄漏门覆盖不足）**：第一版循环只走"手动 `Stop()` 制造 corpse → 自愈重建"。corpse 本就已停，
  所以 teardown 漏不漏 `Stop()` 都不泄漏。扩成每轮**同时**走「corpse→自愈」与「权威 OFF→全量重建」
  两条路径后，同一变异给出 `before=3 after=9`（6 轮各泄漏一个 accept loop）。
  教训与 §4.1 A4 同源：**一条路径的覆盖不能替另一条路径背书**。

### 10.4 闸门结果

- `make test`：**rc=0**，零 FAIL（含 `test/architecture` 的两个新闸门与 `test/determinism`）。
- `make lint`：**rc=0**，0 issues。
- `go test -race ./internal/agent/...`：**rc=0**。
- `applyProxyDirective` MI：改前 **20** → 阶段 B 结束 **21** → **内审整合后回落到 20**。
  ⚠️ **它正贴在阈值上**（`under: 20` 表示 MI<20 才报，20 恰好通过）。内审采纳的修复往这个函数里加了 shutdown 栅栏与两段
  论证性注释，MI 一度掉到 **19 并让 `make lint` 变红**；处置方式是把 shutdown 栅栏提取成 `proxyRefusedDuringShutdown`、
  把 `refootprint` 的论证移到 `needsReestablish` 的字段注释上——**注释没有被删，只是搬了家**。
  这正是 `.golangci.yml` 头部记的那条实测性质：maintidx 的 LOC 项**计入函数体内的注释行**，所以在这个函数里
  "把为什么写下来"和"通过闸门"是直接竞争的。CC=41 是根因，但拆解 `applyProxyDirective` 是独立的重构，不属本增量。
  **对下一个改这个函数的人**：你几乎肯定会撞红，正确做法是继续往外提取（本增量已提取三个 free function 作先例），
  而不是删注释、也不是把它加进 `.golangci.yml` 的 god-function 名单。
- `make e2e-parallel`：留到内审整合完成后跑（编辑中途跑全量闸只会得到一棵不存在的树的红）。

---

## 11. 外审后（step 6）的变更登记

外审 **Fail**，7 条 finding（4 Major / 3 Minor）**全部采纳并已修**，逐条回复写在
`docs/reviews/proxy-lifecycle-external-review.md` 的「主进程回复」一节。外审新增的三条反例测试
全部由修复转绿，未被改写或放宽。这里只登记对**本 plan 结论**的修正：

| plan 原结论 | 外审后 |
|---|---|
| §3「SS server 不接受任何 ctx」是核心设计决策 | **过度推广**。对**生命周期所有权**成立，但被我推广成了对**取消能力**的禁令；后者恰是修复 hang 所需。现改为「不接受 CALLER 的 ctx，但持有自己创建的」（F1/F5） |
| §4.2 ⚠R3「闸门不用精确计数」 | 保留；但闸门的**语义**是错的，已从「包级禁止 import context」改为「导出方法签名不得接收 ctx」+ 递归扫描（F5） |
| §4.3 驳回「让 broker 新增 per-node 修复动词」 | 维持驳回，但**理由更新**：不需要新动词——心跳侧 reap 让现有的 `repairProxy` 推送路径自己活过来（F4） |
| §6.3「公网端口不变」无条件契约 | 先被内审收窄为「upstream 无慢拨号时」，外审 F1 修复后**取消已到位**；`Stop` 的**总**上界仍非显式契约，登记为下一个议题 |
| §9「显式不做：不修 Stop 的 dial/DNS 中断」 | **被推翻并已修**（F1）。外审不仅指出必要性，还给了不重引入事故的修法 |
| 内审 §3「single-broker 修复无效」的部分驳回 | **驳回本身是错的**（F4）。三个 lane 都对，我漏读了 `repairProxy` 的 CONVERGENCE-FIRST 早退。已加心跳侧 reap 边并更正两处代码注释 |

### 变异账本累计 23 条

阶段 B 10 条（§10.2）+ 内审整合 8 条（review §5）+ 外审整合 5 条：

| # | 注入 | 目标 | 结果 |
|---|---|---|---|
| 19 | `shutdown` 不再 cancel `stopCtx` | `TestStopCancelsInFlightDestinationResolution` | ✅ |
| 20 | upstream 不绑定到 key | `TestRevocationReclaimsSilentUpstreamConnections` | ✅ |
| 21 | 去掉 EADDRINUSE 重试 | `TestCorpseRebuildSurvivesPersistedLocalPortCollision` | ✅ |
| 22 | 删 `heartbeatLoop` 里的 reap 调用 | `TestHeartbeatLoopWiresTheCorpseReap` | ✅ **（先暴露了我的行为测试测不到接线）** |
| 23 | 给 `Server` 加接收 ctx 的导出方法 | `TestSSProxyEntryPointsAcceptNoCallerContext` | ✅ |

**23 条里有 4 条一开始打不红**（阶段 B 2 条、外审整合 1 条 + F4 接线）。最后这条尤其说明问题：
外审 F15 刚指出「测函数不测接线」，我为 F4 写的测试**立刻又犯了同一个错**——直接调用被测函数，
所以删掉 `heartbeatLoop` 里的调用它照样绿。靠纪律记住这件事显然不管用，所以改成 AST 接线闸门。
