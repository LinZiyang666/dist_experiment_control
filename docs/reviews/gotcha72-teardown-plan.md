# gotcha #72 修复 · plan（定稿）

> 2026-08-01。阶段 A：3 drafter（teardown/seam/verify）→ 3 critic（nofake/race/feasible）→ 1 synth
> 对抗性 workflow 综合，主进程定稿。upgrade-safety 遗留三件中的第 3 件。
> **定稿批注**：综合稿全部裁决采纳，无修订。特别背书两处：①机理修正（真凶=doReconnect 持 nc.mu
> 的无 deadline dial/握手链，非台账原判的 close-frame flush——落地时同步修正 #72 条目并保持
> "高置信不升格"）；②drill 98 定位为"修复后恢复回归 + [GAP #72] wss 面如实呈现"，不冒充 pre-fix
> 复现，ledger owner 行开工即落使 crosscheck 转绿。实施顺序按 §7；变异 M1–M7 逐条注入验证。

## 实施记录（主进程，2026-08-01）

已落地：`internal/agent/conn_teardown.go`（connTracker + 粘性毒化 + 单飞 finalizer + S2–S5 ladder +
escalate 分叉）、`roster.go` 两处重排（fireRedial / rebuildOntoVoter）、`agent.go`（tracker dialer
接线、session defer 换 finalizer、Run 复位 `rebuilding` 的顺序论证注释）、`usage.md §9.9`（退出码 91
与恢复上界）、drill 98 + 四处台账（ledger-crosscheck 由此转绿）。

**变异验证如实记录**：
- **M1（恢复 close-before-cancel）→ 精确红** ✓（`TestTeardownCancelsBeforeClosing` 报出的正是
  "the session ctx was NOT cancelled before the close began"）。
- **M2（删毒化）→ 红，形态是"永不返回→包超时"** ✓（这正是缺陷本体：没有毒化，join 就是无界的）。
- **M3（join 改遗弃 goroutine）→ 不是两侧的，如实降级**：毒化一旦生效，closer 会立刻返回，
  "join 还是遗弃"在行为上不可区分——泄漏门抓不到它。M3 只在毒化**也**失效时才有区别，而那条路径
  生产上走 escalate（exec/exit），测试里进程不死、goroutine 按设计仍在park，无法断言"无泄漏"。
  故本增量**不声称** M3 被变异钉住；`TestWedgedTeardownRepeatsWithoutLeaking` 的真实价值是
  "重复 20 次楔死 teardown 后 goroutine/fd 回基线"，这条它确实两侧。外审可复核该降级是否接受。
- M4–M7（裸 Drain 回退 / dial 不绑 ctx / rebuildOntoVoter 旧序 / 删 rebuilding gate）随内审轮补，
  记入待办而不预先声称。

已核实关键锚点（roster.go:470-484 `rebuildOntoVoter` 确有同型 Close-before-cancel、cmd/tether/agent.go:472 已有 `Restart=on-failure`、upgrade.go:280-307 `reExecInPlace`"不许 exit"契约、agent.go:732 `defer nc.Drain()`）。以下为综合后的候选 plan。

# #72 修复候选 plan（叶子增量：agent teardown 有界化）

## 0. 范围与刻意不做

**范围**：把 agent 的 NATS 连接终结（rebuild 与优雅关停两类入口）改造成"cancel 先行 + 有界 close + 粘性毒化 + 升级梯子"的单一状态机；修复面覆盖 **`fireRedial` 与 `rebuildOntoVoter`（roster.go:476/610，同型缺陷，评审 C1）** 及 `session()` 的 `defer nc.Drain()`；落 ledger owner 行；修正台账机理段。

**刻意不做**（每条注明理由）：
- fork/patch nats.go、反射碰私有 transport（台账明令）。可另给上游报 issue（makeTLSConn ws 路径无握手 deadline、req.Write 无写 deadline），不阻塞。
- goroutine 遗弃式 timeout（台账点名的骗绿形态；变异 M3 专钉）。
- 方向 4 的 readiness 面（sd_notify/健康端点/CLI 字段）——独立叶子；本增量只保证日志锚点（§6）。
- **WSS drill 本增量不交付**：simcluster 现无 websocket listener/证书（已核，全目录无 ws 配置），"黑洞 WSS 写路径"按现状写不出来（评审对 teardown 草案的致命缺陷 2）。补 ws 前端是独立成本，列为 #72 的 flip 条件排期（§5）。
- 调小 `FlusherTimeout` 当修复（管不住 `nc.mu` 被 dial 持有的层）；改 `redialAfter=20s`。

## 1. teardown 状态机与预算（含 setsid 裁决）

```
S0 RUNNING ──(fireRedial / rebuildOntoVoter / 优雅关停)──▶
S1 PINNED+ARMED   CAS rebuilding；pin 旧 nc+tracker（绝不事后从 ncBox 重读）；先布防 watchdog
S1 ──立即──▶ S2 CANCELLED   sessCancel()（纯指针+chan，不碰 nc.mu）
S2 ──立即──▶ S3 CLOSING     closer goroutine 跑真 nc.Close()/Drain；closeBudget=10s
S3 ──closer join──▶ CLOSED   Run 复位 rebuilding、起 successor（唯一成功终态）
S3 ──预算耗尽──▶ S4 POISONED 粘性毒化（§2）；poisonGrace=10s
S4 ──closer join──▶ CLOSED
S4 ──grace 耗尽──▶ S5 ESCALATE  按入口意图分叉（见下）
```

关键裁决（各对应一条评审致命缺陷）：
- **先布防再关**（nofake 对 teardown 草案缺陷 1）：watchdog 在任何可能碰 `nc.mu` 的调用之前架起，安全网不依赖 close 不卡。
- **Run 复位 `rebuilding` 必须 gate 在 closer join 之后**（race 缺陷 1）：cancel-first 会让 session defers 先于 close 完成，若过早清零，旧 conn 一次迟到的 doReconnect 会绕过 ReconnectHandler 的 `rebuilding` 挡 → resendSubscriptions → double-subscribe；且迟到的 Close 从 ncBox 重读会误杀 successor 的新 conn。故：fire 时刻 pin 死旧 nc/旧 tracker，session 的 rebuild 路径**等待本 session 的 finalizer 完成**（毒化保证有界）才返回，Run 才复位。这是 verify 草案的骨架，三评审一致认定为唯一封死竞态的结构。
- **per-session 单飞 finalizer**（race 对 seam 缺陷 1）：fireRedial 侧与 session defer 侧对同一 conn 的终结收敛到同一个 `sync.Once` 型 finalizer，毒化/登记表操作幂等。
- **预算**：`closeBudget=10s`、`poisonGrace=10s`，常数集中定义、进日志与文档。恢复硬上界 ≈ redialAfter(20s)+10+10+重连 ≈ **≤60s**，写进台账替换"无硬上界"。
- **S5 是可达路径，不是理论兜底**（评审 C2）：`net.LookupHost`（nats.go:2329）在 `nc.mu` 下无 ctx、无 fd 可登记，毒化结构上够不到——escalate 的存在理由必须如实写为"DNS 层与未预见持锁层的唯一出口"。
- **S5 分叉（setsid 裁决，正面解决三草案冲突）**：
  - **rebuild 入口**：systemd 与 setsid **统一走 PID 保持的 self-exec**（复用 `reExecInPlace` 语义，exePath 取 /proc/self/exe 并沿用 " (deleted)" 处理，绝不用 `os.Args[0]`）。理由：① setsid 下裸 Exit=永久 down，与 upgrade.go:284-289 的既有裁决直接冲突（verify 草案缺陷 3）；② exec 丢 `a.procs` 名册的代价（verify 的反对）与 systemd 下 Exit 被 cgroup 收割子进程的代价**同级**——exec 不比 exit 更具破坏性，却在 setsid 下严格更优；③ 与 upgrade 路径同构，boot shim/G.1 reconcile 收敛链已存在。exec 失败时：**若 pending upgrade marker 存在，遵守 `reExecInPlace` 的"不许 exit"契约走 `recoverFromFailedExec`**（race 对 seam 缺陷 3 的显式裁决）；无 marker 则 `os.Exit(专用非零码)`——systemd 被 `Restart=on-failure`（**已存在**，无需补，两草案的事实错误）拉起，setsid 接受"诚实 down"残余窗口，写入 usage.md。
  - **优雅关停入口**：escalate = `os.Exit(专用非零码)`，**不 exec**——进程本来就要死，卡死的 closer 不许阻塞死亡；对 setsid 修掉"operator stop 撞 stuck conn 永久挂死"（nofake 对 verify 草案缺陷 2）。

## 2. seam 设计（含 nats.go 锁路径核实结论）

**锁路径核实结论（三评审逐行复核一致，定稿采 seam 草案 §0，并据此修正台账）**：
- **帧 A（无界，真凶）**：`doReconnect` 持 `nc.mu` 执行 `createConn`/`processConnectInit`（nats.go ≈:3146/:3344/:3360）。其中 `net.LookupHost`(:2329) 无 ctx；`makeTLSConn` 的 `Handshake()` 无 deadline（SetDeadline 在 processConnectInit:2727 才设）；ws `wsInitHandshake` 的 `req.Write`（ws.go:655）无写 deadline（读侧 :662 才有）。半死 NAT 下握手读在锁下无限期阻塞，`Close`(:5938)/`wsClose`(ws.go:706)/`Drain`（对 RECONNECTING 直接转 Close，:6188-91，且先拿锁）全部排队。10m58s ≈ 中间盒最终判死握手 socket。
- **帧 B（有界或 no-op，非真凶）**：close-frame 的 `bw.flush` 走 timeoutWriter（`FlusherTimeout` 默认 1min），且 RECONNECTING 时 `pending!=nil` 使 flush 直接返回。台账"close-frame flush 无 deadline"主嫌疑**必须修正**（不升格为已证明——无 dump，但帧结构已核实）。
- 推论：`*nats.Conn` 任何方法都可能拿 `nc.mu`，teardown 路径一律视为可无界阻塞；**仅重排 fireRedial 不够**。

**seam 形态：`CustomDialer` 包装器 = 生产功能 + 测试 seam 二合一**（公共 API，零私有面）：
- 每 session 一个 `connTracker`：包装现有 dialer（proxydial 装了就包它，否则包等价默认 dialer），内部用 `DialContext(runCtx,…)`——cancel-first 直接打断在途 dial，且 teardown 后新 dial 立即失败（teardown 草案的结构性亮点，封死"reconnect 再拨新 conn 重新卡死"窗口）。
- **粘性毒化**（race 对 seam 缺陷 1 的补强）：进入 POISONED 后，登记表内全部 raw conn `SetDeadline(过去)+Close()`；此后**新登记的 conn 到手即毒、新拨号直接拒绝**——毒化是状态不是一次性动作，closer 结构上必然返回。
- **两个已核实的坑**：① nats.go 对 CustomDialer 跳过 Timeout/len(hosts) 切分（:2343-49），包装器自带等价 2s timeout；② `skipTLSDialer` 断言（:2376）——**防御性**条件转发 `SkipTLSHandshake`（"否则 proxydial TLS 双包"的说法不成立：proxydial 刻意不实现该接口，三评审一致订正）。
- **驳回**：`closeFn` 包 Close 的 Config 字段（平行假路径）；毒化能覆盖一切的表述（DNS 盲区必须明写，评审 C2）。
- 测试注入（保真度最高，三评审一致推荐）：首拨真嵌入 server（带 `websocket{}` 块、无 TLS），后续拨号返回 `net.Pipe` 一端——`req.Write` 无读者永久阻塞，**100% 真实 nats.go 帧造出确定性 Close 阻塞**；嵌入 server 不便配 ws 时退路 `RunServerWithConfig` 手写最小 conf。辅 seam：`TeardownExecFn`/`exitFn`（记录型注入，不真 exec/exit）。

## 3. 改动文件清单

- `internal/agent/conn_teardown.go`（新，按职责命名）：connTracker、dialer 包装、单飞有界 finalizer、状态机与预算常量、粘性毒化、escalate 梯子挂钩。
- `internal/agent/roster.go`：`fireRedial` **与** `rebuildOntoVoter` 重排（pin→布防→cancel→finalizer；注释整段搬运并改写顺序论证，`// origin: gotcha #72`）。
- `internal/agent/agent.go`：connectNATS/buildConnOptions 装 tracker dialer；session() 的 `defer nc.Drain()` 换有界 finalizer（rebuild 与优雅关停共用 ladder，escalate 终点按入口分叉）；Run 复位 `rebuilding` gate 在 closer join；全仓扫其余 `nc.Close/Drain` 站点统一归口。
- `cmd/tether/agent.go`：escalate 默认实现装配；专用退出码入 exit-class 双向表（闸门）。
- `docs/deploy-tier-gotchas.md` #72：机理段按 §2 修正、修复定案、恢复上界 ≤60s、保持 OPEN + flip 条件。
- `docs/usage.md`：setsid 残余窗口一句。
- `test/simcluster/drills/98-stuck-redial-recovery.sh`（新）+ `expected-verdicts.tsv` 行 + `expected-verdicts-log.md` note + `drill-costs.tsv` 行。
- 测试：`internal/agent/conn_teardown_test.go`、`roster_runtime_test.go` 增函数、`test/concurrency/redial_rebuild_leak_test.go`（新，按被测单元命名）。

## 4. 测试与变异计划

包内确定性测试（嵌入式 NATS + net.Pipe 注入，`// origin: gotcha #72`）：
- **A 调序回归**：黑洞注入→直调 `a.fireRedial()`（既有先例 roster_runtime_test.go:213 等，**不引入** redialAfter 可配置项）→断言 session ctx 在预算内先 cancel、heartbeatLoop 退出、Run 抵达 successor；顺序断言用 tracker 暴露的 close-开始/归位时刻，**不 sleep 猜时序**。
- **A′ rebuildOntoVoter 同型回归**（评审 C1）：broker-silence 路径同套注入与断言。
- **B 有界归位**：closer 经毒化真实 join（WaitGroup 收割）；NumGoroutine/fd 回基线。
- **C 重复 rebuild**：同注入循环 ≥20 轮 disconnect→rebuild，`-race` 全程；任一时刻至多 1 个 successor（服务端订阅计数断 double-subscribe，复用 `TestRebuildNoDoubleForwardedDispatch` 形态）；NumGoroutine + `/proc/self/fd` 差 ≤容差（`test/concurrency` 泄漏门手法，非 goleak）。
- **D escalate 梯子**：对抗性 conn（Write 永阻且**无视 Close**）→毒化无效→断言 grace 后 escalate fn 以正确码被调、无假绿恢复；**pending upgrade marker 场景**：exec 走 boot shim 语义、exec-fail+marker 走 recoverFromFailedExec 不 exit（race 缺陷 3 钉住）。
- **E 优雅关停**：stop 撞 stuck conn 在预算内返回；stop 路径 escalate=exit 非 exec。
- **F proxydial wiring 回归**：包装组合下 raw conn 登记/毒化语义正确。

**变异验证**（每条注入后确认精确红再复原；成本预告 ≈ **7 × 包测时长**，隔离 worktree 跑）：M1 恢复"Close 后 cancel"→A 红；M2 删毒化→B/C 红；M3 join 改遗弃 goroutine→B/C 的 NumGoroutine 断言结构性红（台账点名的骗绿形态）；M4 有界 finalizer 改回裸 Drain→E 红；M5 dial 不绑 runCtx→在途 dial 打断测试红；M6 恢复 rebuildOntoVoter 旧序→A′ 红；M7 删"rebuilding gate 在 join 后"→C 的 double-subscribe 断言红。

## 5. ledger owner 行落法

- **开工即落**（不依赖修复完成）：tsv 加行 `98-stuck-redial-recovery	INCOMPLETE	1	-	#72 (WSS write-path blackhole not constructable until simcluster fronts wss://)	98-stuck-redial-recovery`——6 字段、owner 列与 ledger-crosscheck 的 `NF==6 && $2!="GREEN"` 对账吻合；crosscheck 立即 UNOWNED→ok。
- drill 98（nats:// 版）是**修复后的恢复回归，不是 #72 的 pre-fix 复现**——脚本与 note 明写（feasible 对 verify 缺陷 2）：纯 TCP 无 ws 握手面，DROP 下旧代码未必红。断言：黑洞注入后 ≤60s 书面预算内 agent 心跳在另一 voter `node ls` 恢复推进；`MainPID` 三类显式判别（同 PID rebuild / exec / systemd restart）；末尾 `not_covered "[GAP #72] …wss listener…"` 如实呈现。
- **不**把 #72 挂到 94/95（账面洗绿）。**#72 保持 OPEN**，flip 条件 = simcluster 补 wss 前端（listener+证书，独立增量、成本另计）后 WSS 黑洞 drill 多轮 GREEN；此排期写进台账条目。

## 6. observability 裁决

只做最小切：S1–S5 每次状态迁移一条稳定结构化日志（drill 与运维 grep 的锚点），现有三条日志锚点保持不动。sd_notify/WatchdogSec/健康端点/CLI 字段一概不做（避免 wire/command-tree golden 无关扰动），立为独立叶子候选。

## 7. 实施顺序

1. 台账机理段修正 + tsv owner 行（crosscheck 先绿）。
2. `conn_teardown.go` 核心（tracker/finalizer/毒化/预算）+ dialer 接线。
3. fireRedial + rebuildOntoVoter + session defer + Run gating 重排。
4. escalate 梯子 + 退出码入表 + cmd 装配。
5. 包内测试 A–F + 变异 M1–M7（隔离 worktree）。
6. `test/concurrency` 泄漏测试。
7. drill 98 编写并在 weilandserver 跑 ≥3 轮 + tsv/log/costs 收口。
8. 硬闸：`make test` + `make e2e-parallel` + `make lint`（改动含并发：`-race`+内建泄漏门）。
9. #72 条目更新（保持 OPEN）、INDEX 追加，停下等外审。

## 8. 风险与外审重点

- **DNS 盲区**：毒化够不到 LookupHost，escalate 是可达路径——文档与代码注释均如实写；poisonGrace 的取值请外审校 resolver 最坏时延。
- **rebuilding gate 竞态**：cancel-first 后 double-subscribe/误杀 successor 的封堵全系于"pin + join 后复位"，是外审第一重点（变异 M7 钉）。
- **escalate × upgrade marker**：exec 撞 pending marker 的契约合流（§1 裁决），外审核对与 upgrade.go F3 契约无冲突。
- **exec 丢 `a.procs` 名册**：已裁决接受（与 upgrade 同构、escalate 是双重故障级罕见路径、Exit 替代方案破坏性同级），外审可推翻。
- **CustomDialer 两坑**：timeout 切分被跳过、skipTLSDialer 防御性转发（"双包"理由已订正）。
- **ws 嵌入 server 测试脚手架**为新面，退路已列；nats.go 升级会动内部帧——注释点名 v1.52.0 行号证据链，依赖升级按 CLAUDE.md 条款重验。
- 归因未逐帧证明到 dump 级：修复对三候选层全覆盖 + escalate 兜底，不依赖归因收窄；台账保持"高置信"不升格。
