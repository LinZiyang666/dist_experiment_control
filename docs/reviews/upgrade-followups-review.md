# upgrade 收尾双增量（33 drill + #72 修复）内审报告

## 0. 结论一句话

30 条原始 finding 经 4 份核验后，去重合并存活 **25 条**（BLOCKER 3 / MAJOR 6 / MINOR 11 / NIT 5），另 1 条（TQ-3）被驳回；三个 BLOCKER 全在 drill 侧（33 的 C2 臂结构性跑不通 ×2、98 的故障仪器自相矛盾），#72 修复核心（cancel-first / finalizer 钉死 / 单飞）本体成立，但周边有 3 条 MAJOR 正确性缺陷（CT-1/2/3）需同轮修。

## 1. 存活 finding 表（最严重优先）

| # | 级 | file:line | 陈述 | 失败场景 | 建议修法 | verdict |
|---|---|---|---|---|---|---|
| S1 (F33-1+DOC-2) | BLOCKER | test/simcluster/drills/33-node-upgrade-success.sh:236 | C2-pre 把 `agt1b` 当容器节点名传给 `upgradecfg_allow_agent`，容器 agt1b 不存在（`up --agents 1` 只有 agt1，agt1b 是同容器第二个 unit） | 首次真跑 `docker exec sim-$INSTANCE-agt1b` 直接失败，C2 臂全红 | 首参改 `agt1`：`upgradecfg_allow_agent agt1 _agt1b_online "$SID" /home/sim/agt1b-home/.tether tether-agent-b` | CONFIRMED |
| S2 (F33-2) | BLOCKER | 同文件:235-241 | 修完 S1 后 C2-pre 的 restart 发生在 A 已 COMMIT 之后，agt1b 直接以 NEXT 上线，`node upgrade agt1b` 命中同 tag 闸（node.go:417-425）→ UNCONFIRMED rc≠0，`_c2_committed` 结构上不可满足；且踩了 plan 自己排除的"同 tag 重推" | C2 臂修完 S1 依然必红 | 把 agt1b 的 allowlist 配置提前到 setup 段（OLD/PID 捕获前）；C 臂拒绝靠 marker 门（upgrade.go:128 先于 :140 的 allowlist），提前配不削弱 C；C2-pre 只留 PID 捕获与 dispatch | CONFIRMED |
| S3 (F98-1+DOC-1) | BLOCKER | test/simcluster/drills/98-stuck-redial-recovery.sh:28,58,66（连带 gotchas:700、usage:1580、README:334） | 双重结构缺陷：① `fault_partition_on agt1 4222` 是 peer-agnostic DROP，把 agt1→brk2/brk3 一并黑洞，"DROP 保持布防、经另一 voter 恢复"结构上不存在恢复路径；② 90s 预算把 ≤60s 上界的起点当故障注入时刻，但 `redialAfter` 只在 DisconnectErr 后起算，silent DROP 下 nats.go 断连检测靠 ping 陈旧 ≈4–6min（未设 PingInterval，默认 2min/2）；docs 的 ≤60s 未写"自断连检测起算"，对弱 NAT 常态构成过度承诺 | drill 首次真跑 RECOVERY 断言必超时红 | fault.sh 新增按对端 IP 的 `fault_partition_peer_on` 只切 brk1 方向（仍入 SIMFAULT 链）；预算改为两段式起点或恢复臂改用可即时检测的切断（RST/停 listener）；gotchas/usage 两处 ≤60s 补"自 nats.go 判定断连起算"限定语 | CONFIRMED |
| S4 (CT-1) | MAJOR | internal/agent/agent.go:845,792 | session defer LIFO 中 `subFwd.Unsubscribe()`（拿 nc.mu RLock）与 `proxyHandlerWG.Wait()` 先于 fin.Do 执行；wedged 链路（doReconnect 持写锁跨 createConn）下二者无限阻塞，S1–S5 ladder 够不到，`systemctl stop` 仍被扣为人质——违反 plan 裁决"watchdog 在任何碰 nc.mu 的调用之前架起" | 半死链路上 operator 停机 → defer 845 卡 10m+，exit 91 永不发生（setsid 部署无 SIGKILL 兜底） | 把 Unsubscribe/WG.Wait 挪进 finalizer 的 closer goroutine（受 closeBudget 约束），或至少让 fin.Do 先于一切碰 nc.mu 的 defer 注册 | CONFIRMED（维持 MAJOR：多数时序 redial 定时器会侧路解锁，但终态错成 self-exec） |
| S5 (CT-2) | MAJOR | internal/agent/roster.go:615-633 + agent.go:769 | 陈旧 redial 定时器在 successor connect 窗口 no-op 触发：CAS 置 `rebuilding`/`rebuildRequested` 但 fin/cancel 皆 nil、无回滚 → 该 session 的 onNATSReconnect 全早退、rosterRefreshLoop 全跳过、watchdog 自毁；粘住的 `rebuildRequested` 还会把未来真关停误判为 rebuild | failover 时 N+1 connect 重试超 20s（弱网常态）即触发 | ① fireRedial/rebuildOntoVoter 在 fin==nil && cancel==nil 时回滚两标志后 return；② session() 顶部（connect 前）先 `stopRedialWatchdog()` | CONFIRMED（订正：fireRedial 自触发子路径被 single-arm 守卫挡住；rebuildOntoVoter 路径完全打通，结论不变） |
| S6 (CT-3) | MAJOR | internal/agent/conn_teardown.go:224 | escalate self-exec 用裸 `os.Executable()`，pending-upgrade 窗口内带 `" (deleted)"` → exec ENOENT → recoverFromFailedExec 用错误路径算 marker path → 读不到 pending → `os.Exit(91)`，违反 F3"pending 时绝不 exit"（本仓另三处都做了 trim，唯此站点漏） | upgrade 安装完成、reExec 前后窗口内恰逢 teardown wedge → setsid 节点永久死亡且 prev 槽未回滚 | 一行：改用 `a.upgradeExePath()`（顺带获得测试缝一致性） | CONFIRMED |
| S7 (TQ-1) | MAJOR | internal/agent/conn_teardown.go:269-292 | `trackerDialOption` 的 probe/wrap（把 proxy CustomDialer 读回转发）零测试覆盖；变异"删 probe 循环"无测试变红 | 走 proxy 的 NAT agent 所有 NATS 拨号静默绕过代理 → 直连被防火墙丢 → 永久离线 | 落 §2 的 T1 测试（顺手经 `TeardownDialFn` 注入，两清 S18） | CONFIRMED |
| S8 (TQ-2) | MAJOR | internal/agent/conn_teardown.go:216-240 | `escalateWedgedTeardown` 生产体全被 seam 遮蔽，intent→动作映射（shutdown→Exit 91、rebuild→upgradeExec、exec 失败→recover 决定去留）一行未执行过；互换分支的变异存活 | rebuild 分支误 `os.Exit` 在 setsid 部署上=节点死亡，无测试拦截 | seam 置 nil + 用现成 `UpgradeExecFn` 缝测 rebuild 分支与 F3 回归钉（见 §2 T2） | CONFIRMED |
| S9 (F98-2) | MAJOR | 98-stuck-redial-recovery.sh:58-67 | 改 per-peer 后 drill 从未证明故障咬中活连接（agt1 连 brk1 只是惯例非断言）；若实际连在 brk2/brk3，恢复断言空转绿 | 修完 S3 即从"必红"变"可能空转绿" | 布防后先证冲击（心跳水位停滞）、恢复后证着陆（brk2/brk3 journal 出现 `node registered … nid=agt1`），至少二选一 | CONFIRMED |
| S10 (CT-4) | MINOR | conn_teardown.go:90-92,282-284 | dialCtx 绑父 ctx 非 session ctx；proxy 包装丢弃 ctx，2s 预算不生效（实际上限 proxydial 10s）；三处注释失实 | 均有界，不升级 | 订正注释或改绑 runCtx + proxy 包装接 ctx | CONFIRMED |
| S11 (CT-5+TQ-6) | MINOR | conn_teardown.go:131 | `t.conns` 只增不减：数周 session + `MaxReconnects(-1)` 下每次重拨累积一条死 conn 引用，无界内存增长；现有泄漏测试每轮新 tracker，测不到长命形态 | flapping 链路数天累积数万死引用 | dial 注册时剔除已关闭项或环形上限；加"同 tracker 1000 次 dial 后 len 有界"钉 | CONFIRMED |
| S12 (CT-6) | MINOR | conn_teardown.go:236-237 vs :28 | recover-true 分支不 join closer 直接 return，头注"ALWAYS joined"在此路径为假 | 双故障下取舍可能正确但文档失实 | 头注与 S5 注释如实写明例外及理由 | CONFIRMED |
| S13 (F98-3) | MINOR | expected-verdicts.tsv:27 + README:334 | "regresses the bounded-teardown recovery"措辞过强：drill 真正钉的是部署层恢复预算，teardown 重排契约 owner 是 conn_teardown_test.go | 无跑红/假绿场景，纯措辞 | 三处统一为"pins the written recovery budget over nats://; teardown-order contract owned by conn_teardown_test.go (M1)" | DOWNGRADED (MAJOR→MINOR) |
| S14 (F98-4+TQ-4) | MINOR | 98-…sh:69-82 | 两个 assert_ok 恒真充数（`sh -c 'true'`、`[ -n "$PID1" ]` 必真）；`*deleted*` 判 self-exec 结构上不可达（同路径 exec 不现 deleted），三 way 分类实为两 way | 虚增 PASS 计数；escalate 被误记为 designed path | 恒真断言降为 `log`；真分类用 agent journal 的 escalate 行 | CONFIRMED |
| S15 (DOC-3) | MINOR | conn_teardown_test.go:87-93 ↔ usage §9.9 | 测试与 gotchas 都引用 §9.9 的 ≤60s，但 §9.9 全节无 "60" 字样 | grep "60s" 找不到要维护的契约句 | §9.9 补总上界一句（与 S3 限定语一并落） | CONFIRMED |
| S16 (DOC-4) | MINOR | usage §9.13 ↔ §9.9 + conn_teardown.go:228,238 | §9.13 taxonomy 不含 91；且 rebuild self-exec 失败同样 exit 91，§9.9 把 91 呈现为关停专属 | 监控按"91=stop 被挟持"解读会误判 rebuild 失败 | §9.13 保留区间加 91 条目；§9.9 补 rebuild 兜底一句 | CONFIRMED |
| S17 (DOC-5) | MINOR | roster.go:604-608 | fireRedial 注释第一段仍写旧契约（close 在 cancel 前），与 origin 段及实现自相矛盾 | 同一注释块两段各说一个顺序 | 第一段改为"pins the finalizer, which cancels FIRST then runs a bounded close" | CONFIRMED |
| S18 (TQ-5) | MINOR | agent.go:156-160 | `TeardownDialFn` 死 seam，零测试使用 | 无使用者的豁口只剩攻击面 | 让 S7 的测试经它注入，或删除 | CONFIRMED |
| S19 (TQ-7) | MINOR | r9d-nonvacuity.sh:436 起 | B4 缺 marker-unreadable fail-closed 变异臂（B3 有、独立提取的 B4 不迁移） | fail-closed 性质在 B4 上无证明 | 补一行 `NV_MARKER=''; expect F "…" _b4_rolled_back` | CONFIRMED |
| S20 (DOC-6) | MINOR | 31-node-upgrade-fleet.sh:170 | F4 注释把 `TestUpgradeAllSkipsOfflineContinuesNext` 的包写成 cmd/tether，实际在 test/p10/upgrade_all_test.go:69（综合者已 grep 复核，见 §3 裁决） | 下个人在 cmd/tether grep 不到锚点 | 改为 `test/p10 TestUpgradeAllSkipsOfflineContinuesNext` | CONFIRMED |
| S21 (CT-7) | NIT | conn_teardown.go:86 | connTracker 不透传 `SkipTLSHandshake()`，未来实现该接口的 dialer 被静默吞掉 | 今天无损（proxydial 刻意不实现） | 注释点名已知限制 | CONFIRMED |
| S22 (F33-3) | NIT | 33-…sh:122 | pin-bind `\|\| true` 吞错，失败只剩 40s poll 超时，bind 日志滞留容器 | 盲调一轮 | poll 失败分支 cat /tmp/agt1b-bind.log 进 drill 日志 | CONFIRMED |
| S23 (TQ-8) | NIT | conn_teardown.go:170-174 | fn==nil 真 `nc.Close()` 分支无 `IsClosed()` 断言 | no-op 变异只被泄漏门迟滞抓到 | 真嵌入式 NATS 最小用例：fin.Do 后断言 IsClosed 且耗时 ≪ closeBudget | CONFIRMED |
| S24 (TQ-9) | NIT | conn_teardown_test.go:184 | 上界 `closeBudget+poisonGrace`=300ms 把调度余量计入毒化窗口，`-race` 并跑有 flake 面 | 高负载偶发红 | 上界放宽 +2s，下界不动 | CONFIRMED |
| S25 (DOC-7+TQ-10) | NIT | upgradecfg.sh:24 | 头注漏 `$4`(home 树)/`$5`(unit) 两参（33 C2-pre 正在用）；"Extracted VERBATIM"未提 33 新增参数 | 下次改签名时被挤掉 | 头注补两参及默认值 + 一句"$4/$5 为 33 新增" | CONFIRMED |

## 2. 专家提议的新增测试

**T1（tests lane，TQ-1；同时消化 S18 死 seam）** — 落 `internal/agent/conn_teardown_test.go`：

```go
// origin: gotcha #72 review TQ-1 — the tracker must FORWARD to the proxy dialer it displaced.
func TestTrackerDialOptionWrapsExistingCustomDialer(t *testing.T) {
	inner := &recordingDialer{} // implements nats.CustomDialer, records Dial calls, returns blockingConn
	a := &Agent{cfg: Config{Logger: slog.New(slog.DiscardHandler)}}
	opt := a.trackerDialOption(context.Background(), []nats.Option{nats.SetCustomDialer(inner)})
	var o nats.Options
	if err := opt(&o); err != nil { t.Fatal(err) }
	if _, err := o.CustomDialer.Dial("tcp", "127.0.0.1:4222"); err != nil { t.Fatal(err) }
	if !inner.called.Load() { t.Fatal("proxy-aware inner dialer was DISPLACED, not wrapped") }
	if a.takeSessionTracker() == nil { t.Fatal("tracker was not stashed for the session finalizer") }
}
```

**T2（tests lane，TQ-2）** — escalate 生产体，`TeardownEscalateFn=nil` + `UpgradeExecFn` 缝：

```go
// origin: gotcha #72 review TQ-2 — intent=rebuild must self-exec, and a failed exec with a
// pending marker must NOT exit (F3 contract regression pin on the escalate path).
func TestEscalateRebuildSelfExecsAndHonoursF3(t *testing.T) {
	var got atomic.Value
	a := newTestAgentForTeardown(t) // TeardownEscalateFn deliberately nil — run the real body
	a.cfg.UpgradeExecFn = func(p string) error { got.Store(p); return nil }
	a.escalateWedgedTeardown(teardownRebuild, "test-wedge")
	if got.Load() == nil { t.Fatal("rebuild intent must self-exec, not exit") }
	// arm 2: exec fails + pending marker present → recoverFromFailedExec()==true → must return, not exit
	a.cfg.UpgradeExecFn = func(string) error { return errors.New("ENOENT") }
	writePendingMarker(t, a) // helper: stage a pending upgrade marker under the agent home
	a.escalateWedgedTeardown(teardownRebuild, "test-wedge") // reaching the next line IS the assertion
}
```

**T3（teardown lane，CT-2）** — no-op fireRedial 标志回滚钉：

```go
// origin: gotcha #72 review CT-2 — a stale redial firing with no finalizer must roll back its CAS.
func TestFireRedialWithoutSessionRollsBackFlags(t *testing.T) {
	a := newTestAgentForTeardown(t)
	a.setSessionFinalizer(nil); a.clearSessionCancel()
	a.fireRedial()
	if a.rebuilding.Load() || a.rebuildRequested.Load() {
		t.Fatal("no-op fireRedial must roll back rebuilding/rebuildRequested")
	}
}
```

**T4（tests lane，TQ-7）** — r9d-nonvacuity.sh B4 段补一行：`NV_MARKER=''; expect F "MUTATION B4 marker unreadable → fail closed" _b4_rolled_back`

另有两条测试提议未给完整代码，采纳为方向：CT-1 的 defer 顺序断言（需小幅提取 defer 链为可测函数，teardown lane）；TQ-8 的真 NATS `IsClosed()` 最小用例（tests lane）。

## 3. 被驳回的 finding 及理由

- **TQ-3（原 MAJOR，REFUTED）**：声称 `TestUpgradeAllSkipsOfflineContinuesNext` 不存在——核验 grep 一击命中 `test/p10/upgrade_all_test.go:69`。综合者复核裁决：确认核验方正确（grep 见上），该函数存在且正是 post-canary skip-continue 面；残余仅为 31:170 注释把包写错成 cmd/tether，已归入 S20（MINOR），MAJOR 撑不起。

（其余 29 条原始 finding 无被驳回项；F98-3 为降级而非驳回。）

## 4. 与两份 plan 的偏差清单

1. **违反 gotcha72-teardown-plan.md:58 裁决**（S4/CT-1）：plan 明写"watchdog 在任何可能碰 nc.mu 的调用之前架起"，但优雅关停路径的 `Unsubscribe`/`WG.Wait` 站在 ladder 之前。
2. **违反 plan §77 防御性转发**（S21/CT-7）：plan 写 `SkipTLSHandshake` 做"防御性条件转发"，实现未做，仅靠 proxydial 刻意不实现兜底。
3. **踩了 upgrade-success-drill-plan.md:22 自己的排除项**（S2/F33-2）：C2-pre 的时序制造出 plan §0 明确排除的"同 tag 重推 UNCONFIRMED"。
4. **contract 注释与实现偏差**（S10/CT-4）："scopes every dial to the session"与"tracker applies teardownDialTimeout itself"两句在 rebuild teardown / proxy 路径不成立。
5. **docs 落点与 plan 声称覆盖不符**（S13/S15）：≤60s 上界声称落在 usage §9.9 但该节无此数；verdicts.tsv 把 hermetic 测试的回归 owner 权责写到了 drill 头上。
6. **plan 明写的"刻意不做"均被审查正确放行**：#73 不 staged、wss 写路径不可构造、fn==nil seam 设计——无一被误报为缺陷（drill lane 核验抽查确认）。

**修复优先序建议**：S1/S2/S3（drill 结构性红，必须修才谈得上 33/98 首跑）→ S4/S5/S6（#72 修复本体的正确性洞，S6 是一行改）→ S7/S8/S9（防修完假绿）→ 其余 MINOR/NIT 按顺手原则同轮清。

---

## 主进程处置（2026-08-02）

**结论：3 BLOCKER + 6 MAJOR 全部采纳并修复；11 MINOR / 5 NIT 中 18 条采纳、2 条改为如实记录。**

| # | 处置 | 落点 |
|---|---|---|
| S1 | 采纳 | 33: `upgradecfg_allow_agent` 首参改 `agt1`（agt1b 是同容器第二个 unit，不是容器） |
| S2 | 采纳 | 33: agt1b 的 allow-list 前移到 setup 段。C 臂不受影响——marker 入口门先于 allowlist 检查；C2 不再撞同 tag 闸 |
| S3 | 采纳 | fault.sh 新增 `fault_partition_peer_on`（按对端容器 IP 只切一条边，仍在 SIMFAULT 链）；98 改用它 + 加 brk2 可达自证；预算注释写明 ≤60s 的**起点是 nats.go 判定断连之后**，不是注入时刻 |
| S4 | 采纳 | `sessionFinalizer.addBoundedCleanup`：`subFwd.Unsubscribe` 与 proxy drain barrier 从裸 defer 移入受预算约束的 closer（顺序保持：drain → unsubscribe → close） |
| S5 | 采纳 | fireRedial / rebuildOntoVoter 在「无 finalizer 且无 cancel」时回滚 `rebuilding`/`rebuildRequested` 并告警；rebuildOntoVoter 该路径返回 false。补回归钉 `TestNoOpRebuildRollsBackTheRebuildFlags`；`roster_proactive_rehome_test.go` 的 fixture 补 `setSessionCancel`（它本就代表活 session） |
| S6 | 采纳 | escalate 改用 `a.upgradeExePath()`（trim " (deleted)"），恢复 F3「pending 时绝不 exit」契约 |
| S7 | 采纳 | 补 `TestTrackerDialOptionWrapsExistingCustomDialer`（经 recordingDialer 证明 proxy dialer 被**包裹**而非替换）——同时消化 S18 的死 seam |
| S8 | **部分采纳** | intent→动作映射由 `TestTeardownEscalatesWhenPoisonCannotReach` 的双 intent 断言覆盖；生产体（真 os.Exit / 真 exec）**不测**——测它要么杀测试进程要么替换测试二进制，两者都比缺口更糟。如实记录，外审可复核 |
| S9 | 采纳 | 98 补 IMPACT 臂（心跳在水位停滞＝故障确实咬中活连接）与着陆臂（brk2/brk3 journal 出现 agt1 注册），堵住「本就没连 brk1」的空转绿 |
| S10 | 采纳（订正注释） | dialCtx 绑的是 agent 级 ctx 而非 session ctx、proxy 包装丢 ctx——两处注释改为如实描述，并说明为何不改绑（tracker 在 session ctx 存在之前就要建） |
| S11 | 采纳 | tracker 注册表加 `trackerConnCap=8` 上限（长命 session 重拨不再无界增长）+ `TestConnTrackerRegistryStaysBounded`（1000 次重拨后有界） |
| S12 | 采纳（注释） | 头注补「ALWAYS joined 的唯一例外是 S5 rung——进程即将被替换或退出，没有可泄漏的去处」 |
| S13 | 采纳 | verdicts.tsv 措辞改为「pins the WRITTEN recovery budget；teardown-order 契约 owner 是 conn_teardown_test.go」 |
| S14 | 采纳 | 98 的两条恒真 assert 降为 log；escalate 分类改用 agent journal 的 `teardown WEDGED` 行；新增一条**真断言**：单边分区上不该出现 escalation（出现＝ladder 正常档位失效） |
| S15 | 采纳 | usage §9.9 补总上界句（含起点限定） |
| S16 | 采纳 | usage §9.13 保留区间加 91 条目；§9.9 补「rebuild self-exec 也失败时同样退 91」 |
| S17 | 采纳 | 删掉 fireRedial 注释里残留的旧契约段（同一注释块曾两段各说一个顺序） |
| S18 | 采纳 | 见 S7（TeardownDialFn 现由该测试使用） |
| S19 | 采纳 | r9d B4 段补 marker-unreadable fail-closed 变异臂 |
| S20 | 采纳 | 31 注释里的包名改为 `test/p10/upgrade_all_test.go` |
| S21 | 采纳（注释） | connTracker 头注点名 `SkipTLSHandshake` 未转发这一已知限制及其无损理由 |
| S22 | **暂不采纳** | 33 的 pin-bind 日志落地属排障便利；bind 失败已由随后的 40s ONLINE poll 响亮红。记录为下轮顺手项 |
| S23 | **暂不采纳** | 真 NATS 的 `IsClosed()` 最小用例是好补充，但 fn==nil 分支的真实覆盖靠 p10/e2e 层的真升级往返；不为它引入嵌入式 server 依赖到本测试文件。记录在案 |
| S24 | 采纳 | 上界放宽 +2s（下界不动——下界才是「楔死确实被施加」的载荷断言） |
| S25 | 采纳 | upgradecfg.sh 头注补 `$3/$4/$5` 三参签名与「33 新增」注记 |

**未采纳的三条均如实记录理由，不是静默丢弃**（S8 生产体不可测、S22/S23 收益低于成本）。

### 处置轮的闸门收尾

三闸终验 **gates=0（lint 0 issues）/ test=0 / e2e-parallel=0（ALL PASS）**。过程中修掉的两类红，
均按「是它该抓的 vs 它抓错了」分别处置：

**账本类（新代码没进既有账本，全部按规矩落账并在 commit message 说明）**：结构预算
`internal/agent.Agent` 方法数 118→124；`teardownIntent` 进 `enumFamilies`（不登记＝将来有人写
`default:` 会永久瞎掉 exhaustive）；泄漏断言锚点登记为 finalizer 的 `Do`（否则 20 轮空转也能绿）；
两轮 gofmt。

**先于本增量存在的偶发假阳性（`internal/broker` `TestManifestNoSecrets`）**：它用裸
`strings.Contains` 在整份 manifest 里找 nkey **种子前缀** `SUB`/`SAA`…，而 manifest 必然包含随机
生成的 account **公钥**——本轮那把公钥恰好是 `…BSUBDJZIQJH2MIODZ2`，于是守卫指控了一个无辜的
公钥（单跑 5 次全绿，确认是运气而非泄漏）。改为 **token 边界匹配**（解析 JSON 后判断字符串值是否
「以种子前缀开头 ∧ 达到 nkey 长度」，并显式跳过 account 公钥本身），不可能碰撞的字面量（PEM 标记、
文件名、字段名）保持 `Contains`。**变异验证守卫未被削弱**：注入伪种子 `SUBLEAKED…` 后精确变红。
