# gotcha #81 修复（remote-fs stale-health）内审报告

> Date: 2026-08-30。CLAUDE.md §3 阶段 C step 4 的产物（对抗性审查 workflow `wf_521da9e7-b6b`：
> 6 视角审查 → 6 对抗性验证 → 1 综合），**§-1 是主进程在 step 5 的逐条裁决**。
> 汇总 6 个视角（状态机 / 调用点 / 恒等式 / 风险 / 文档契约 / 闸门质量）的 findings 与验证。
> 只保留 CONFIRMED 与 PLAUSIBLE；被驳回的单列 §5。
> **行号会随修改漂移**——凡能用函数名锚定处已同时给出函数名。

## §-1 主进程裁决与处置（step 5）

审查抓到一条**我自己的验证造假**，这条比它掩盖的洞更该先记：我把变异按 `-run` **正则分组**跑，
`TestMountHealthy_reArmSurvivesConcurrentLauncherWakeup` 的"红"其实来自同组的另一条测试。
逐条单测复核后确认：该测试对「`invalidateHealthy` 整体变空操作」是绿的，即对它命名的那件事零断言。
**「按组跑变异会互相掩蔽」是一种可复发的方法论错误**，已写进 gotcha #81 正文，不只留在这里。

### 全部采纳并已落地

| finding | 处置 | 验证 |
|---|---|---|
| **F-1** E3（`boundedHomeRead`）零覆盖 | 新增 `TestBoundedHomeRead_stallExpiresCachedMountHealth`（`internal/agent/remotefs_test.go`） | 删掉 `agent.go` 那行 ⇒ 该测试 RED；同一变异下 `internal/spawnsafe` 与架构闸门**都绿**，确认审查者「结构上够不到」属实 |
| **F-1** 注释夸大闸门覆盖面 | `invalidateHealthy` 的 doc 改成逐站点写明**各自由谁钉住**，并明说闸门只覆盖两个铸造点 | — |
| **F-2** re-arm 测试是恒等式 + `close(stop)` 位置错 | worker 单独 WaitGroup（`workers.Wait()` → `close(stop)`）；探针改成"慢但健康"让 launcher 真进死线臂；**按探测次数**断言重新武装真的发生 | 未变异 `-race -count=50` 绿；空操作变异 RED（"probed 1 times across 341 invalidation passes"）；原地重置变异 RED |
| **F-2** 换指针设计零机械守卫 | 新增确定性白盒守卫 `TestMountHealthy_reArmReplacesGenerationPointer`（T10/T11 两条路径 + 孤立代不得被改写） | 原地重置变异 RED；空操作变异 RED |
| **F-2** 账本记假 | gotcha #81 正文改写成诚实版本，写明三条具体问题与修法 | — |
| **F-3** `--safe` 作废**位置**零守卫 | 新增 `TestPrepare_safeInvalidatesBeforeCwdCheck` | 把该 block 挪到 cwd 检查之后 ⇒ RED |
| **F-4** 闸门 clause ③ 按函数名匹配 | 换成**锁支配检查**（`p.mu` 持有期间的作废调用一律红），并做**一层间接的传递闭包** | 抽 helper + 锁内调用 ⇒ RED（旧闸门 GREEN 且真死锁）；`Prepare` 锁内作废 ⇒ RED |
| **F-7** 缺 OQ-1 的等价前置测试 | 新增 `TestRunStartWithCleanup_reapsBeforeReleasingTheWedgeSlot` | 对调 reap/release ⇒ RED |
| **F-8** `fmt.Errorf("%w")` 铸造点看不见 | 加宽铸造谓词（排除 `errors.Is` 比较） | 在 `internal/agent` 新增 `%w` 包装的第三个 watchdog ⇒ RED |
| **F-9** 同键合并偏宽松 | 改成**冲突即报**，并新增 `blockInvalidated` | — |
| **F-15** 账本只 enforce 一半 | 双向化：给 ceiling 站点接线 ⇒ 红 | 接线变异 ⇒ RED |
| **F-16** clause ② 接受臂内 func literal | `callsInvalidateDirectly` 遇 `*ast.FuncLit` 停止下降 | 藏进 `go func(){}` ⇒ RED |
| **F-17** 对 `time.NewTimer` 重构误报 | `isDeadlineArm` 接受 `<-tm.C` | 正控：timer 重构 + 保留作废 ⇒ GREEN；**负控**：timer 重构 + 删作废 ⇒ 仍 RED（加宽没打瞎 clause ②） |
| **F-18** KNOWN BLIND SPOTS 点名了假洞 | 重写：删掉"import alias"（实测能看见），写上四个真洞 | — |
| **F-10** #82「前半段已消失」过强 | 改成量化表述（O(命令数) → O(1)，但**没有消灭**），并点明两半各由哪条测试钉住 | — |
| **F-11** `--safe` 契约只扫了一半 | 补 `docs/usage.md` 两行 flag 表；**并新增 `test/determinism/safe_flag_contract_test.go`** 把三处绑死 | 恢复任一行旧文案 ⇒ RED；两个 help 串分叉 ⇒ RED |
| **F-5** 注释说 targeted 实为 global | 改成明写 global 并给出为什么这是**有意的取舍**（证据不携带"是哪个挂载"的信息） | — |
| **F-13** D 态预算写成无条件定理 | 改成 per (挂载, **代**)，并写明它**不**覆盖"反复重挂"这一面 | — |
| **F-14** 引信注释用现在时 | 改成虚拟语气，并注明第一条引信已在同一次提交里被拆除 | — |
| **F-19** 错误串点名不存在的 agent.yaml 键 | 改成 `spawnsafe.Config.HealthTTL`；`TestNew_rejectsBadConfig` 补一格 + 一条"不得点名不存在的键"的反向断言 | — |
| **F-22** 订正 (a) 歪曲冻结 plan | 重写：原文的 **still-dead** 限定词是承重的，当年的推理**对它的指称对象是真的**；本批换的是指称对象 | 对照 `remote-fs-resilience-plan.md` §B 原文逐字核过 |
| **F-23** ceiling 测试从不断言 dropped | 补齐：释放槽位后断言 `Outage` 且死目录已剔出子进程 PATH | — |
| **F-24** C1 掉了 `WedgedCount` 归零 | 补上（轮询式，不裸断言——释放发生在 reaper goroutine 里） | — |
| **F-26** drill 1S-4 可空过 | 加 re-prime 判别式（健康态下 `--cwd` 必须成功；死判定会快速拒绝） | 真跑 drill 62 复核 |

### 采纳但**不在本批**做（登记，理由写明）

| finding | 裁决 |
|---|---|
| **F-6** 误判窗口不是"一条命令"而是那次被放弃的 statfs 的剩余延迟 | **接受该订正**，已在 usage §7.7 写成"通常一条命令内恢复"而非承诺。把它变成断言需要控制探针延迟与 drain 时机的新夹具，属独立小增量 |
| **F-12** 无墙钟预算守卫；TTL 到期是同刻的（惊群） | **登记**。当前所有守卫都数探测次数、不测阻塞窗口。加墙钟断言在 CI 上天然易 flake，需要先想清楚阈值怎么定，不塞进本批 |
| **F-20 / F-21** plan R13 的 O(1) 表述、`probe_timeout` 不是运行时旋钮 | **接受**，已在 #82 更新里改成精确表述；旋钮化是独立增量 |
| **F-25** `startBounded` 指路注释指向的 doc 没有那段"为什么" | 已部分处置（`TryAcquireSpawnSlot` 的过期注释已订正）。完整搬运留待动那块代码时做 |
| **F-27** `smokeVersion` 的 execve 不在任何有效看门狗下 | **登记项，非本批缺陷**——那是升级路径，与 remote-fs 无关；不在 #81 范围内扩大 |
| **T7 / T8 / T9** 每挂载去重、joiner 广播、per-generation 探针预算 | 都是好守卫，但都属于"加固既有正确行为"而非"防止本批引入的回归"。本批已从 13 条增到 18 条守卫，继续加会把外审面撑得过大 |

### step 5 期间新发现（不是审查报的，是复核 drill 时撞出来的）

**drill 62 Arm 1S-3 曾以 `rc=124` 失败一次，未归因。** 三轮实测：r1 绿 → r2 **红（rc=124，
`timeout 25` 砍掉，`took=25008ms`，当时 `loadavg 10.69` / `fsync_4k_ms 15.2`）** → r3 绿
（`assert_fail=0 pass=35`）。r2 与 r1/r3 之间我只改了注释和一条错误串，**没有行为改动**。

处置：把该臂的 ctl 预算从 25s 提到 45s。**这不是把它调绿的手段**——期望答案 ~2s，45s 远在其外；
提预算是为了让**下一次**失败交出一个**码**而不是 `rc=124`。`rc=124` 区分不了"判定没被作废"与
"机器慢"，而我**没有证明**它是后者。若该臂再红且报 `remote_fs_spawn_timeout`，那就是"证据驱动作废
在这条路径上没生效"的直接诊断，应当立案而不是重跑。这一段已同样写进 drill 的行内注释。

### 驳回

- **REF-1**（"删掉 drill 1S-4 的 re-prime 该臂会静默变绿"）：审查自己的 §5 已驳回，我同意——预测为红不是绿。判别式仍然加了，但理由是 F-26 的那个（同码、错理由），不是 REF-1 的那个。

---

## 0. 总体判断

**ship-with-fixes。**

没有任何一条 finding 说明**已落盘的实现在生产上是错的**：证据驱动作废 + healthy-only TTL + dead 绝对 sticky 的状态机被两个视角用动态断言（INV-2 panic 探针 `-race -count=30` 全包 + 并发压测）与静态阅读双向核过，没找到 close-of-closed / close(nil) / joiner 永久 parked / dead 重探泄 D 态线程的路径。deploy-tier 的双向证据（修复前 `assert_fail=2`、修复后 `assert_fail=0 pass=34`）也站得住。

**但主进程宣称的验证有一条是假的**，且这条正好落在本批风险最高的那个设计决策上：

> gotcha #81 写「13 条…每条都做过变异验证」、任务书写「M7 原地重置代替换指针 ⇒ 确认变红」。
> **M7 在当前树上不变红**：两个视角各自独立施加 M7 的两种忠实形态（保留 `close(done)` 的 M7-lite、连 `close(done)` 一起回退的 M7-full），`./internal/spawnsafe` 在 `-race -count=200` 与 `-race -count=40`、`./test/concurrency` 在 `-count=5`/`-count=10` 全绿。

按 CLAUDE.md「每条新增守卫都要注入它声称能抓的那个缺陷并确认变红」，**这条假记录本身比它掩盖的洞更该先修**——账本一旦记了假，下一轮就不会重新推导。

**ship 前必修（§1 的 F-1 ~ F-4 + F-10 + F-11）**：

| # | 一句话 | 类型 |
|---|---|---|
| F-1 | 调用点 E3（`boundedHomeRead` 超时臂）零覆盖，删掉全绿；且 `invalidateHealthy` 的 doc 宣称闸门钉住了它 | 无守卫 + 注释不实 |
| F-2 | 换指针（epoch swap）设计零机械守卫；`TestMountHealthy_reArmSurvivesConcurrentLauncherWakeup` 是恒等式（对 `invalidateHealthy` 整体 no-op 都绿）；M7 账本条目为假 | 恒等式 + 假账本 |
| F-3 | `--safe` 作废**位置**（必须早于 `pathOnDeadMount(cwd)`）零守卫，挪到 cwd 检查之后全绿 | 无守卫 |
| F-4 | 闸门 clause ③ 只按函数名匹配 `mountHealthy`；抽个 helper 就瞎，而那个形态**实测硬死锁** | 闸门盲区（后果比 #81 更坏）|
| F-10 | gotcha #82 宣称「前半段已消失」，与它自己援引的那条测试的控制断言直接矛盾，且 plan §7 R13 明令禁止这么写 | 文档不实 |
| F-11 | `--safe` 契约只扫了一半：help 串改了，`docs/usage.md:704/:749` 两行 flag 表未改（plan §5.2 第 5 条逐字点名） | 契约漏扫 |

其余 29 条是 **ship 后可跟**（或并入本轮一起改，代价都很低）的守卫加固与文档订正。

---

## 1. 必修

### F-1 · 调用点 E3 零覆盖，且注释宣称它被钉住

**独立发现：5 个视角**（状态机 / 调用点 / 恒等式 / 风险 / 闸门质量）——本轮最高复现度的一条。

- **位置**：`internal/agent/agent.go:1576`（`boundedHomeRead` 超时臂里的 `a.spawnPolicy.InvalidateHealthy()`）
- **变异（5 个视角各自独立跑过，结论一致）**：把 :1576 换成注释/no-op ⇒
  - `go test -count=1 ./internal/agent/` → **ok**（有视角跑的是整包 18–24s，不只是三条测试）
  - `go test -count=1 ./internal/spawnsafe/` → ok
  - `go test -run TestSpawnTimeoutMintSitesNoteEvidence ./test/architecture/` → ok
- **闸门为什么结构上够不到**：账本键是哨兵铸造点（`test/architecture/spawn_stall_evidence_test.go:65-70`，4 条全在 `internal/spawnsafe/spawnsafe.go`），而 `boundedHomeRead` 超时臂返回 `(nil, false)`、**不铸造任何哨兵**，clause ① 看不见它、clause ② 无从运行。
- **既有测试为什么够不到**：唯一走这条分支的 `internal/agent/remotefs_test.go:329-361`（`TestBoundedHomeRead_singleFlightAbandon`）用 `fakeMountinfo([2]string{"/", "ext4"})` 构造 agent——**一个 hangable 挂载都没有**，`p.health` 是空的，作废调用在那里按构造就是 no-op。
- **注释不实**：`internal/spawnsafe/spawnsafe.go:772-778` 写 “Call sites (all four, and the gate in test/architecture/spawn_stall_evidence_test.go … pins them)”。实际闸门只钉住 4 个里的 **2 个**（`boundedResolveInDirs:ReasonSpawnTimeout`、`RunStartWithCleanup:ErrSpawnTimeout`）；`--safe` 那处靠 `TestPrepare_safeForcesRevalidation` 行为钉，E3 **什么都没钉**。
- **为什么重要**：plan §2.6 自己把 E3 标为「唯一不需要任何人跑命令就能触发的路径」（`loadStateBounded` 由 `agent.go:1629` 的 register snapshot 走），删掉它 = 空闲 agent 失去唯一的自愈触发。而 plan 的测试表 H1–H13 里**没有 E3 那一行**，这正是主进程 M1–M9 里没有 E3 变异的原因。

**建议修法**：加 §6 T1 的行为守卫；同时把 :772-778 那句改成「闸门钉住两个 watchdog 铸造点；`--safe` 由 `TestPrepare_safeForcesRevalidation` 钉；`boundedHomeRead` 由 `<T1 的测试名>` 钉」。一条**夸大闸门覆盖面的注释**比没有注释更坏——它让下一个人不再去验证。

---

### F-2 · 换指针设计零守卫；reArm 并发测试是恒等式；M7 账本条目为假

**独立发现：2 个视角**（状态机 / 恒等式），两边各自从零复现，且各自写出了会变红的替代测试。

**(a) 事实：M7 不变红。**
把 T10（`invalidateHealthy`，`spawnsafe.go:783-794`）与 T11 臂（`spawnsafe.go:671-679`）的换指针改成原地重置
`h.launched=false; h.result=nil; h.done=nil; h.state=stUnprobed; h.decidedAt=time.Time{}`：

| 变体 | 命令 | 结果 |
|---|---|---|
| M7-lite（保留 `close(done)`） | `go test -race -count=200 -run 'TestMountHealthy\|TestPrepare_*' ./internal/spawnsafe/` | **ok**（63s） |
| M7-lite | `go test -race -count=60 ./internal/spawnsafe/` / `-count=10 ./...` | **ok** |
| M7-lite | `go test -race -count=10 ./test/concurrency/` | **ok** |
| M7-full（+ `close(done)`→`close(h.done)`） | 同上两包 | **ok** |
| M7-full | replay `spawnsafe_stress_test.go` `-race -count=40` | **ok** |

⇒ plan §0 A2 / §2.5 记的两条引信（stale launcher 把 sticky-dead 盖到新一代 / close-of-closed）**在树上都不可达**。

**(b) 直接病因：`spawnsafe_test.go:1235` 的 `close(stop)` 位置。**
它紧跟在 16 个 worker 的 spawn 循环（:1226-1234）之后、`wg.Wait()`（:1236）之前。实测：invalidator goroutine 只活 113µs–1.39ms（0–3 圈），而 16 worker 的 3200 次 consult 要跑 12.5–15.6ms。把 `close(stop)` 挪到 worker 跑完之后，同一 body：invalidator 287–605 圈、`rearmInvalidate` 22–29、`launcherOrphan` 2–5。
**修好之后 M7 立刻变红**（两个视角各自验过）：
- M7-lite ⇒ 该测试自己的失败串 `healthy mount was demoted by re-arm churn alone`（20 次里 3 次红 / 另一视角 ~每次都红）
- M7-full ⇒ `panic: close of closed channel` / `close of nil channel`
- 未变异的 shipped 实现，`-race -count=100`/`-count=200` 全绿（3.1s / 5.4s），不引入 flake。

**(c) 更硬的事实：该测试对 `invalidateHealthy` 整体 no-op 也绿。**
把 `invalidateHealthy` 的整个 body 换成 no-op，`-race -count=20 -run TestMountHealthy_reArmSurvivesConcurrentLauncherWakeup` → **ok**；**同一个二进制**让另外 6 条测试变红（`TestPrepare_staleHealthyMountDroppedAfterSpawnTimeout`、`...RevalidatesViaBlockingProbe`、`TestMountHealthy_reprobeNeverDoublesInFlightProbes`（`:1195 re-arm launched 0 probes for 200 consults`）、`TestPrepare_safeForcesRevalidation`、`TestPrepare_wedgeCeilingSaturated...`、`TestPrepare_cwdOnStaleHealthy...`）。⇒ 这条测试当前**对它命名的那件事零断言**。
第二个病因：fixture 的探针在既不 `setBlocking` 也不 `setRet` 时**同步返回 true**（`spawnsafe_test.go:100-117`），probeTimeout=1ms 下 launcher 基本进不了 `time.After` 臂——它命名的「stale launcher」状态从未被进入。

**(d) 注释与 plan 的次生问题**：
- `spawnsafe_test.go:1205-1206` 写「Historical precedent (external review F6) needed `-count=1000` … hence the repetitions」——测试里既没有钉住的 `-count`，也没有 plan §4 H6 / §7 R6 要求的那套 choreography（阻塞探针 + 另一个 caller drain + launcher 超时后再 re-arm，`-race -count≥200`）。
- H2（`spawnsafe_test.go:1046`）、H5（`:1154`）、H6（`:1203`）三条的 doc 注释都声称能抓原地重置变异；**都抓不到**。

**建议修法**：三件一起做，缺一不可——
1. 移动 `close(stop)`（把 worker 单独用一个 WaitGroup，`workers.Wait()` → `close(stop)` → `wg.Wait()`），并按 H6/R6 补上 `-race -count` 与阻塞探针的 choreography；
2. 另加一条**确定性白盒守卫**（§6 T3），不要把这个设计决策的唯一防线压在并发调度上；
3. 修正 H2/H5/H6 三条注释说它们**真正**抓的是什么，并订正 `docs/deploy-tier-gotchas.md:899-900` 的「13 条…每条都做过变异验证」。

---

### F-3 · `--safe` 作废的**位置**零守卫

**独立发现：2 个视角**（调用点 / 恒等式），两边都写出了会变红的测试并跑过。

- **位置**：`internal/spawnsafe/spawnsafe.go:878-885`（`if requestedSafe { … p.invalidateHealthy() }`）必须早于 `:897` 的 `if cwd != "" && p.pathOnDeadMount(cwd)`。
- **契约来源（两处都明写）**：plan `docs/reviews/remote-fs-stale-health-plan.md:150`（§2.6 S 行）「必须在 :711 的 `pathOnDeadMount(cwd)` 之前」；代码注释 `spawnsafe.go:882-883`「before the cwd/argv[0] checks below consult any of them」。
- **变异**：把该 block 挪到 cwd 检查之后 ⇒ `./internal/spawnsafe` ok、`./internal/agent` ok、`./cmd/tether` ok、`TestSpawnTimeoutMintSitesNoteEvidence` ok。**全绿。**
- **为什么承重**：`pathOnDeadMount`（:603-611）末尾是 `!p.mountHealthy(...)`，而 `mountHealthy` 只在 `stUnprobed` 时重探。先作废，才使**第一条** `--safe --cwd <stale-healthy 死挂载>` 快速失败为 `ReasonUnsafeCwd`，而不是照付 30s execve 看门狗——这正是 `--safe` 被文档承诺的逃生口。
- **两轴为什么都漏**：H12（`spawnsafe_test.go:1493-1518`）直接调 `InvalidateHealthy()` 且 `safe=false`；H7（`:1250-1285`）`safe=true` 但 `cwd==""`。新 drill 的 1S-4 用的是绝对 argv[0]、不是 `--cwd`。⇒ 两条轴从未相交。
- 顺带核实（调用点视角）：`boundedTouch` 被排除是对的，且理由比 plan 写的更强——它的两个 caller（`validSafeDir`、`localFallbackDirs`）都先 `IsHangablePath` 过滤，根本到不了 hangable path。

**建议修法**：§6 T4（两个视角各自写过、各自验证过红绿两向）。

---

### F-4 · 闸门 clause ③ 按函数名匹配 ⇒ 抽 helper 即瞎，而那个形态实测硬死锁

**位置**：`test/architecture/spawn_stall_evidence_test.go:123`（`fn.Name.Name == "mountHealthy" && callsInvalidate(fn.Body)`），报告点 `:145-147`。

**复现（用未改动的真闸门，两次独立跑）**：
```go
func (p *Policy) mountHealthyReArm() { p.invalidateHealthy() }
// 并在 mountHealthy 的 p.mu.Lock() 之后第一行调用它
```
- `go build ./...` 干净
- `go test -run TestSpawnTimeoutMintSitesNoteEvidence ./test/architecture/` → **ok 0.26s（绿）**
- `timeout 90 go test -run 'TestPrepare_staleHealthy|TestMountHealthy' ./internal/spawnsafe/` → **超时被杀（exit 124/143）**，`p.mu` 不可重入，agent 上每一次 spawn 都会硬死锁。

**同族第二个洞（同一次验证顺手跑出来的）**：把 `Prepare` 里的 `p.invalidateHealthy()`（:884）挪到它下面一行的 `p.mu.Lock()`（:885）**之后** ⇒ 闸门 **ok 0.298s**，而 `TestPrepare_safeForcesRevalidation` 挂死。

`mountHealthy` 现在 112 行（`spawnsafe.go:647-758`），`.golangci.yml` 又故意禁用 `funlen`——「抽个 helper」是**阻力最小的下一次编辑**，而后果（进程级 spawn 死锁）严格坏于 #81 本身。

**建议修法**：把 clause ③ 从名字匹配换成它真正代表的不变量——在 `internal/spawnsafe` 的**任何**函数里，`invalidateHealthy`/`InvalidateHealthy` 的调用不得被同函数内一个没有配对 `Unlock`（且其前没有 `defer p.mu.Unlock()`）的 `p.mu.Lock()` 支配。名字匹配可作为廉价二检保留，但不能是唯一一道。变异方案见 §6 T9（两种形态都已实测「今天绿 + 真死锁」）。

---

### F-10 · gotcha #82「前半段已消失」与它自己援引的测试直接矛盾，且 plan 明令禁止

**独立发现：2 个视角**（调用点 / 文档契约）。

- **文案**：`docs/deploy-tier-gotchas.md:981`「**前半段已消失**——stale-healthy 不再存在，所以 `exec --cwd <死挂载>` 会在 `Prepare` 的 lexical 检查处快速失败为 `remote_fs_unsafe_cwd`」。
- **被援引的那条测试的控制断言恰恰相反**：`internal/spawnsafe/spawnsafe_test.go:1503-1506`
  ```go
  // Stale-healthy: the lexical cwd check consults the cached verdict and lets a chdir into a dead mount through
  if _, err := f.prepare(t, []string{"true"}, staleMount+"/nas", false); err != nil {
      t.Fatalf("while still stale-healthy the cwd check cannot fire: %v", err)
  }
  ```
  实跑 `go test -race -count=1 -run TestPrepare_cwdOnStaleHealthy ./internal/spawnsafe/` → ok，即**这条「放行」断言今天是通过的**。函数名自己也带 `OnceInvalidated`。
- **plan 明令**：`docs/reviews/remote-fs-stale-health-plan.md:368`（§7 R13）「把这类事件的频率从『每条命令一次』降到『每次 healthy→dead 转换一次』…但没有消灭它。**不要在 gotcha 里宣称 #82 已解决**」。
- 同一过强表述还出现在 `spawnsafe_test.go:1487-1488` 的函数头注释（“disappears with it”），与其下 6 行的断言互相打架。

**建议修法**：改成量化表述——前半段的**频率**从 O(命令数) 降到 O(1)：同一个 healthy→dead 窗口里**第一条** `--cwd` 仍会放行、仍付一次 30s 看门狗、仍 fork 出一个 pre-execve D 态子进程；此后由证据/TTL/`--safe` 关闭。同步改 `spawnsafe_test.go:1488`。（`--safe` 让第一条就快速失败这一半，由 F-3 的新测试钉住。）

---

### F-11 · `--safe` 契约只扫了一半（plan §5.2 第 5 条逐字点名）

**位置**：`docs/usage.md:704`、`docs/usage.md:749` 两行 flag 表格。
`git diff -U0 docs/usage.md` 的**最早 hunk 在 @@ -1473**——这两行**逐字节未动**，仍只写「用去掉疑似挂死网络挂载的 PATH 解析 argv[0]」，只字不提作废/重探。
而 help 串已改：`cmd/tether/exec.go:157`、`cmd/tether/run.go:308` 都含 “re-probe mount health (discarding cached verdicts)”。
plan `docs/reviews/remote-fs-stale-health-plan.md:309` 逐字要求这两行与 help 串同步。

这正是 memory `feedback-contract-change-sweep` 记的那个反复复发形状：改了动词契约，漏掉用户实际会 grep 的那份手抄文案。缓解项：§7.7 散文（`usage.md:1489-1493`）确实写了新行为，所以契约不是无文档，只是查表的人读到的是旧的。

**建议修法**：两行各补一句「**并强制作废已缓存的挂载健康判定、立即重探**（per-call 成本：最坏 `probe_timeout` × 被咨询的健康网络挂载数）」，并加 §6 T18 的 determinism 门把三处串绑死。

---

## 2. 重要

### F-5 · `invalidateHealthy` 是**全局**的，而注释写着 `targeted`

- **位置**：`internal/spawnsafe/spawnsafe.go:786` `for mp, h := range p.health`（无任何 mountpoint 过滤）；两个 watchdog 调用点都不传任何被牵连的路径。注释 `:626` 写 “Cheap, **targeted**, self-correcting”。
- **复现**（scratch，跑完已删）：`/nfs1 /nfs2 /nfs3`（nfs4）+ `PATH=/nfs1/bin:/nfs2/bin:/nfs3/bin:/usr/bin`，`ProbeTimeout=50ms`，先播种 healthy 再把探针改成 500ms-but-healthy；一次 `invalidateHealthy()` + 一次 `Prepare` ⇒
  `outage=true`，`warn="dropped 3 unresponsive network $PATH dir(s) [/nfs1/bin, /nfs2/bin, /nfs3/bin]"`，`child PATH="/usr/bin:/usr/local/bin:/bin:/usr/sbin:/sbin"`，`cwd="/tmp"` —— **三块盘全程活着**。
- 「N 倍成本未记录」对 plan §7 R2/R4 成立（它们只量化了单挂载）；R5 与 `usage.md:1493` 已把 N×probe_timeout 写给了 `--safe` 那条路径。
- 全仓没有多挂载爆炸半径断言：`staleFixture`（`spawnsafe_test.go:963-983`）与 `spawnsafe_stress_test.go` 都只有一个 hangable 挂载。

**建议修法**：二选一并写进文档——(a) 让证据变成 targeted（`boundedResolveInDirs` 知道 `dirs`，`RunStartWithCleanup` 可被喂 argv[0]+cwd，只作废被牵连的 mountpoint，无法指名时才退化为全局）；或 (b) 承认全局是刻意的，把 `:626` 的 `targeted` 删掉、改成「every healthy verdict, because the stall does not say which mount caused it」，并把 N 倍放大写进 R2/R4 + 一条爆炸半径断言。

### F-6 · 误判窗口不是「一条命令」，而是那次被放弃的 statfs 的剩余延迟

- **名实不符**：`internal/spawnsafe/spawnsafe_test.go:1444` 函数名 `...SelfHealsWithinOneCommand`，body 是 `:1471` 的 `for deadline := time.Now().Add(2*time.Second)` + `:1477 time.Sleep(time.Millisecond)` 轮询（上界约 2000 次 `Prepare`），失败串 `:1480` 仍写 “within one command”。
- **同口径出现在另外两处**：`docs/usage.md:1496`「通常一条命令内恢复」；`spawnsafe.go:88` `DefaultHealthTTL` 的注释 “heals it within one command”。
- **实测**（两次独立 scratch）：单挂载、`ProbeTimeout=50ms`、探针 400ms-but-healthy、一次作废后按固定间隔发命令 ⇒ 拿到 fallback PATH 的命令数 **12（30ms 间隔）/ 34（10ms 间隔）**，与 statfs 延迟成正比，与「一条」无关。
- **run 路径上这次降级是不可见的**：`internal/agent/run.go:211-216`（`d.Outage` 只换 env/cwd + `Logger.Warn`，注释说 PTY 横幅会毁 vim/tmux）+ `cmd/tether/run.go:303` 的 `--cwd` 默认空 ⇒ `d.Cwd = p.safeDir`。交互 shell / 12h 作业可能以错的 `python3` 起来且零信号。（plan §7 R1/R2 已把这条残留登记过，所以这里可修的是**名实**与**上界口径**。）

**建议修法**：测试改名为 `TestPrepare_slowMountFalseDemotionRestampsFreshnessOnSelfHeal`（并改失败串），注释写清真实上界 = 命令速率 × statfs 剩余延迟；`usage.md:1495-1497` 同改。另建议评估**两击迟滞**（re-armed 那一代首次超时先返回 last-known-good，连续两次才降级）——它比「调大 TTL」严格更优，且不在 §8 OQ-2/OQ-3 权衡过的选项里。

### F-7 · `startBounded` 塌缩缺了 plan OQ-1 的**硬性前置**等价测试

- **plan 原文**（`remote-fs-stale-health-plan.md:20`，OQ-1）：「附加硬性前置：先补一条对旧实现与新实现都绿的等价测试（`onAbandon` 同步调用、`reapOnReturn` **先于** `ReleaseSpawnSlot`），再做替换」。**该测试没有被写。**
- **变异**：把 `spawnsafe.go:1126-1131` 里 `p.ReleaseSpawnSlot()` 与 `reapOnReturn(err)` 对调 ⇒ `./internal/spawnsafe`、`./internal/agent`、`./test/concurrency` **全绿**。
- **这是写下来的契约**：`spawnsafe.go:1098-1099`「reapOnReturn runs after the abandoned start eventually returns **and before the wedge slot is released**」。顺序承重：先放槽，新 spawn 就能在被回收的子进程 reap、管道写端关闭之前抢到槽——正是这次塌缩搬走的那笔 fd 账（review M4）。
- 既有测试为什么分不出：`internal/agent/remotefs_test.go:289-298` 把 `reaped != 0 && WedgedCount() == 0` 放在同一个轮询里；`spawnsafe_test.go:822-860` 先读 reap channel 再单独轮询 `WedgedCount` 到 0。两者都不定序。`remotefs_test.go` 本轮未被触碰。
- 塌缩的其余部分核过是行为保持的：`a.spawnTimeout()` 提前到实参求值（纯配置读）、两个回调新增 nil 容忍（两个调用点都传非 nil）。

**建议修法**：§6 T5（已写过并双向验证）。

### F-8 · 闸门的铸造点谓词看不见 `fmt.Errorf("%w", …)` 包装

- **位置**：`spawn_stall_evidence_test.go:222-231`（`ReturnStmt` 的 result 必须是裸 Ident/SelectorExpr）与 `:312-354`。
- **复现（真闸门）**：往 `internal/agent/exec.go` 追加第三个 watchdog，超时臂写 `return fmt.Errorf("%w: futureWatchdog", spawnsafe.ErrSpawnTimeout)`、不接线作废 ⇒ `go build ./...` 干净、`go vet` 静默、闸门 **ok 0.28–0.31s（绿）**。
- **这是最可能的下一种拼法**：同一函数往上 9 行就在用它——`spawnsafe.go:1022 return "", fmt.Errorf("%w: %s", exec.ErrDot, r.path)`；且下游 `internal/agent/exec.go:405` 用 `errors.Is` 归类，包装与否语义等价。
- 另外三种形态也实测不可见（编译进 `internal/spawnsafe` 后闸门仍 ok）：中间变量（`e := ErrSpawnTimeout; return e`）、具名返回值赋值、`&FSError{Code: "remote_fs_spawn_timeout"}` 裸字面量、本地类型别名 `type E = FSError`。

**建议修法**：`ReturnStmt` 臂加宽到「result 里任一 CallExpr 是 `fmt.Errorf`/`errors.Join`/`errors.New` 时扫其实参」；`AssignStmt` 到具名返回值也算铸造；`Code:` 值为等于哨兵字符串的 `BasicLit` 也算。然后按 F-18 重写 KNOWN BLIND SPOTS。

### F-9 · 闸门的同键合并是**宽松方向**的

**独立发现：3 个视角**（状态机 / 调用点 / 闸门质量）。

- **位置**：`spawn_stall_evidence_test.go:148-160` 的去重 + `:245/:253` 的键 `<file>:<func>:<sentinel>`（不含位置）。`if prev.invalidated { continue }`，否则被覆盖 ⇒ **两种源码顺序下都是「已接线的那个赢」**。
- **复现（真闸门，三次独立）**：在 `RunStartWithCleanup` 的 wedge-slot 检查之后插入一个**未接线**的早退（`if timeout <= 0 { return ErrSpawnTimeout }` / `if timeout < 0 { … }` / `if p.mode == ModeOff { … }`）⇒ 闸门 **ok 0.25–0.29s（绿）**：clause ① 仍数到 4，clause ② 被错误的那个 occurrence 满足。
- 与文件自己的头部宣称（`:41-43`「a new watchdog added anywhere in the tree changes the count and turns this red, which is the whole point」）和去重注释（`:153-154`「keeps the wired one so clause ② still reports honestly」）直接冲突——**留下已接线的那个，恰恰是压制不诚实情形的那一步**。
- 盲区范围（一个视角的订正，值得记）：仅限「同函数内、同哨兵的第二次铸造」；换函数仍会红，所以主进程做过的那两种闸门变异确实会红。

**建议修法**：键里加位置/序号，或保留单键但在两个 occurrence 的 `(inTimeoutCase, invalidated)` 不一致时报冲突并打印两处位置。

---

## 3. 次要

### F-12 · 没有任何墙钟预算守卫；TTL 到期是**同刻**的（惊群）

- 串行事实：`sanitizePATH`（`spawnsafe.go:951-970`）逐 dir 调 `pathOnDeadMount`，launcher 在 select 里阻塞（`:729-757`）。
- 零墙钟断言：`grep -n time.Since internal/spawnsafe/spawnsafe_test.go test/concurrency/spawnsafe_stress_test.go` 只命中 `spawnsafe_test.go:642`（既有 m9 joiner 用例）。号称的稳态成本守卫 `TestPrepare_healthyHangableMountZeroProbesWithinTTL`（`:1525-1544`）只断言**探针计数**。
- 实测：3 挂载 × 300ms statfs ⇒ `first=904ms / cached=0s / after-one-invalidation=902ms`；4 挂载 + 注入时钟正好推进一个 TTL ⇒ **那条命令阻塞 1.202s、一次重探 4 个挂载**（全部同刻过期，因为它们的 `decidedAt` 是同一次 `Prepare` 盖的）；3–5 次 `--safe` 在健康 3–4 挂载上共 3.0–3.6s。
- 订正（验证者提出，需照收）：「perfectly healthy box 上最坏 8s」是**最坏界**，不是常态——真健康的 statfs 是微秒级；要凑满 N×probe_timeout 需要每块盘都慢到撞 probe_timeout，那已经不叫健康。

**建议修法**：加 §6 T13 的 per-command 墙钟预算守卫；并给 `decidedAt` 加**每 mountpoint 抖动**（`hash(mp) mod TTL/4`）或每次 `Prepare` 最多 re-arm 一个挂载，把断崖摊平。同时把 TTL tick 的同形状成本写进 R5 / `usage.md:1492`（今天只写了 `--safe` 的）。

### F-13 · D-STATE 预算注释被写成**无条件定理**（plan §2.4 明令订正过）

**独立发现：2 个视角**（状态机 / 文档契约）。

- **plan 原文**（`remote-fs-stale-health-plan.md:106`）：「**界的精确表述（订正三份草案共同的口误）**：是 **每 (mountpoint, mount 代) ≤1** … 本轮不动，但必须如实写进注释，别把它论证成无条件定理。」
- **落盘的注释没照做**：`spawnsafe.go:637-638`「⇒ at most ONE probe goroutine per mountpoint **at any instant**, exactly as before this change」、`:642-643`「⇒ steady state is ≤1 abandoned probe per mountpoint, **forever**」。而同函数 `:614` 自己写的是 “per mountpoint **generation**”——**文件内自相矛盾**，且承重的预算段落丢了限定。
- **动态反例已复现（3/3 与 5/5 两次独立）**：探针阻塞时改**该挂载自身**的 mountinfo signature（`srv:/old`→`srv:/new`）并 `refreshIfChanged()` ⇒ `applyMounts`（`:483-490` 的 `old.signature == cur.signature` 继承条件）丢弃 entry ⇒ 下一次 `mountHealthy` 起第 2 个探针，`max concurrent probes for one mountpoint = 2`。
- 仓库自己的证人：`spawnsafe_test.go:782-796 TestProbe_resetsWhenMountInstanceChanges` 断言 `probes==2`。

**建议修法**：两条 bullet 都加 `per (mountpoint, mount generation)` 限定，写明 `applyMounts` 例外，并点名 `TestProbe_resetsWhenMountInstanceChanges` 作为证人，让下一个人不必重新推导。

### F-14 · `close(h.done)` 引信注释在同一次提交里被自己作废，却仍用现在时陈述

**独立发现：2 个视角**（恒等式 / 文档契约）。

- `spawnsafe.go:178-179`：「the launcher closes the **FIELD** h.done, not the local it captured, so a reset that swaps in a fresh channel gets a close-of-closed panic」
- 同文件 `:724-726`：「Closing the captured `done` (**NOT h.done, which used to be read as a field here**)」；`grep -n 'close(h.done)' internal/spawnsafe/spawnsafe.go` = **0 命中**，实际关闭点是 `:744`/`:754` 的 `close(done)`；`git show HEAD:` 确认 `close(h.done)` 正是本增量删掉的。
- `docs/deploy-tier-gotchas.md:894-895` 复制了同一句已死的论证（「原地重置会 close-of-closed panic」）。实测 M7-lite 跨 `-race -count=10/200` **不 panic**。
- 一处补充订正（恒等式视角）：`:180-182` 的第二条 bullet 措辞对 in-place 情形是**反的**——原地重置后新一代就是 `stUnprobed`，超时臂 `:749-752` 会**降级**它，而不是「拒绝降级」。两条 bullet 都要重写，不只是删第一条。
- 真正幸存的理由（也是新守卫该钉的）：旧 launcher 关的是旧局部 `done`，parked 在新 struct `done` 上的 joiner 无人唤醒，只能各等满一个 `probeTimeout`（m9 回归形状）。

### F-15 · 闸门只强制账本的 `Wired=true` 一半

- **位置**：`spawn_stall_evidence_test.go:186-188` 的 `if !wantWired { continue }` 在任何断言之前短路。
- **复现（真闸门，两次）**：在 `boundedResolveInDirs` 的 `return "", &FSError{Code: ReasonTooManyWedged}` 之前加 `p.invalidateHealthy()`（= 静默推翻账本注释 `:61-64` 自称的「a decision, not an oversight」的 §0 A9 决策）⇒ 闸门 **ok 0.26–0.28s（绿）**。
- 洞比 finding 描述的略宽：ceiling 铸造点不在任何 select 臂里，`newMintSite`（`:252-264`）根本不给它们填 `invalidated`——双向检查要看外围语句，不能只看臂。

### F-16 · clause ② 接受「臂内 func literal 里」的作废调用

- **复现**：把 `p.invalidateHealthy()` 从 `case <-time.After(timeout):` 臂顶挪进被放弃的 reaper `go func(){ err := <-done; … }()` ⇒ 闸门 **ok 0.275s（绿）**；而行为守卫正确变红：`spawnsafe_test.go:1363`「probe count 1: the abandoned start did not expire the cached healthy verdict」。
- 病因：`callsInvalidate`（`:288-308`）是裸 `ast.Inspect`，会下降进 FuncLit body。
- 语义上这个位置是错的：那个闭包只在被卡住的 start **最终返回**时才跑——恰恰不是修复所针对的那段 outage。
- 缓解：闸门自己的 KNOWN BLIND SPOTS（`:51-54`）已声明「it cannot prove the call does the right thing」，且真有行为守卫兜底，所以是**收紧项**而非无守卫洞。

### F-17 · 闸门对 `time.NewTimer` 重构**误报**

- **复现**：把 `RunStartWithCleanup` 改成 `tm := time.NewTimer(timeout); defer tm.Stop(); … case <-tm.C:`，`p.invalidateHealthy()` 原地不动 ⇒ 闸门 **FAIL at `:190`**，而 `go test -count=1 ./internal/spawnsafe/` **ok 0.606s**。
- 病因：`isTimeAfterArm`（`:267-286`）要求 Comm 字面上是 `<-time.After(...)`。而 `time.After`→`time.NewTimer`+`defer Stop` 正是标准的定时器保留修复。
- 反向核实（同视角）：给 `internal/agent/exec.go` 加一个无关的 `select { case <-ch: case <-time.After(d): }`，闸门仍绿——它不乱咬无关超时。
- 措辞订正（验证者提出）：闸门印的是「the mint is not inside a `case <-time.After(...)` arm」，**字面属实**，所以指控点是**形状脆**而非「诊断误导」。

**建议修法**：接受同函数内由 `time.NewTimer`/`NewTicker` 赋值的 `<-tmVar.C`；或保留严格形状但把消息改成「watchdog 不再用 `time.After`：请重读本闸门、确认作废仍在死线臂内，然后更新 `isTimeAfterArm`」。

### F-18 · KNOWN BLIND SPOTS 段落唯一点名的那个洞是**假的**，四个真洞一个没写

- `:50` 写「a caller that builds the value through a variable, a helper, or an **import alias** renamed at the call site would slip past」。
- **实测证伪（真闸门）**：新增 `internal/agent/zz_alias_probe.go`，以 `ss "…/spawnsafe"` 别名写 `return ss.ErrSpawnTimeout` 与 `&ss.FSError{Code: ss.ReasonSpawnTimeout}` ⇒ 闸门**变红**并列出两个新键。因为 `sentinelName`/`fsErrorCodeSentinel` 匹配的是 `SelectorExpr.Sel.Name`，别名改不动它；dot-import 同样被看见。「a helper」也不是盲区——minting helper 会造出自己的账本键、clause ① 直接红。
- 三个里只有「through a variable」为真；真正的四个洞见 F-8。
- 一个自称「stated, not papered over」的段落在唯一点名处编造了一个洞、又漏掉四个真洞，是它自己所反对的那种失败。

### F-19 · `HealthTTL<0` 的错误串点名了一个**不存在的 agent.yaml 键**，且该分支既不可达也无测试

**独立发现：3 个视角**（状态机 / 风险 / 文档契约）。

- `spawnsafe.go:267-269` 返回 `remote_fs.health_ttl: must not be negative`；而 `cmd/tether/agent.go:70-76` 的 `remoteFSConfig` 只有 `mode/safe_dir/probe_timeout/spawn_timeout/wedge_ceiling`；`grep -rn health_ttl`（排除 docs/reviews）只命中这一行。
- 唯一生产构造点 `internal/agent/agent.go:773-779` 从不设 `HealthTTL` ⇒ **从 agent.yaml 出发永远不可达**；而兄弟错误串（`:262/:265/:271`）点的都是真键，这个形状会被运维读成「有这个键、我配错了」。
- 无测试：`TestNew_rejectsBadConfig`（`spawnsafe_test.go:889-907`）只覆盖 `WedgeCeiling:-1` / `ProbeTimeout:-1` / 相对 `safe_dir`，没有 `HealthTTL:-1`；全仓没有任何地方构造负 `HealthTTL`。
- 关联：`usage.md:1479` 写「最多 5 分钟过期一次」却没说它**不可配置**，`usage.md:250-252` 的配置表也没有它；而 plan §7 R3（`:358`）把「调大 `DefaultHealthTTL`」列为车队误判后的第一动作。

**建议修法**：错误串改成不点 YAML 键（`spawnsafe: HealthTTL must not be negative`），或删掉这条不可达校验；`Config.HealthTTL` 注释写明「仅测试/嵌入注入，刻意不接 agent.yaml——R12：不落键才能纯二进制回滚」；`TestNew_rejectsBadConfig` 补一格。

### F-20 · plan R13 的「O(命令数) → O(1)」对它自己点名的那一类是**假的**

- `mountForPath`（`:571-585`）纯字面最长前缀匹配；`pathOnDeadMount`（`:601-610`）对 `kindLocal`/`kindAutomount` 直接 `return false`、零探针；denylist（`:125-152`）刻意保守。
- **复现**：`/shared` 判死且 sticky 后，连做 5 轮 `invalidateHealthy()` + `Prepare(argv=["echo"], cwd="/home/u/work")`（字面本地、实际是指向死挂载的 symlink）⇒ **5/5 返回 nil error**（probes=2：播种 1 + 首次作废 1，此后 sticky），即调用方每次都带着落在死挂载上的 `cmd.Dir` 去 fork，30s 看门狗照付、频率与修复前**完全一致**。
- ⇒ 享受 O(1) 的只有「字面命名的死 cwd/argv[0]」与真 TOCTOU 子类；symlink / denylist 外 fstype 那两类**残留未变**。
- 好消息：已发布的 gotcha #82 更新没有继承这个 O(1) 说法（它有别的问题，见 F-10），所以只需订正 plan §7 R13，别让下一个读者继承。

### F-21 · R3 的逃生口不是运行时旋钮；`probe_timeout` 与 Home 读死线的耦合零文档

- 无旋钮：唯一生产构造点 `internal/agent/agent.go:773-779` 不设 `HealthTTL`；`grep -rn HealthTTL --include=*.go internal/ cmd/ | grep -v _test` 只命中 `spawnsafe.go`。⇒ plan R3 的「第一动作是调大 `DefaultHealthTTL`」实际要求**改常量 + 发版 + 车队升级**，而 R12 又把「零新键、回滚 = 纯二进制回滚」写成本批卖点。两条并读会让人以为 TTL 可运维。
- **耦合且任何文档都没写**：`p.probeTimeout` 同时是挂载探针死线（`:729-757`）、resolve 死线（`:1025`）、以及经 `Policy.ProbeTimeout()`（`:1074`）成为 agent **Home 读死线**（`internal/agent/agent.go:1571`，全仓唯一消费者）。`usage.md:1497` 与 plan R2 mitigation③ 都建议调大它、都没提这层——调大它以压制误判，等价于延长挂死 Home 下重连的卡顿。
- 订正：finding 里「误判不是 1/5200 掷骰而是近乎必然」混淆了真降级与误降级，不要照抄这句。

### F-22 · #81 订正 (a) 歪曲了冻结 plan 的 TTL 否决理由

- 原文（`docs/reviews/remote-fs-resilience-plan.md:180-183`）：否掉的是「a plain ~5s TTL, which re-issues a fresh `statfs` **against a still-dead mount** every window and leaks one D-state goroutine per window」。
- 转述（`docs/deploy-tier-gotchas.md:903-906`）删掉了 `still-dead` 这个唯一承重的限定，然后把一个**条件成立**的命题记成「假命题」。
- 危害被验证者正确下调为 minor：同一警告在落盘代码里写了两遍（`spawnsafe.go:200-202` 与 `:669-672`，都紧贴 `state == stHealthy` 守卫），所以它不是 R7 唯一的文字防线；且订正文本同句里确实带了「在 dead 保持 sticky 的前提下」。
- **建议修法**：改成「当年否掉的是**无差别 TTL**（对已判死的挂载也每窗口重探）。那条理由**对它成立**；错的是外推成『任何 TTL 都不行』。healthy-only TTL + dead 绝对 sticky 不在该论证射程内。⚠ 反过来，把 `state == stHealthy` 守卫放宽到 dead，原论证立刻重新成立且更坏——见 plan §7 R7。」

### F-23 · `TestPrepare_wedgeCeilingSaturatedStillDropsDeadPathDirs` 从不断言 dropped（plan H9 ② 缺失）

- 函数体 `spawnsafe_test.go:1338-1387` 的全部断言：`errors.Is(ErrSpawnTimeout)`（:1351）、`mountHealthy`（:1359/:1381）、探针计数（:1362/:1374/:1384）、`errors.As(ReasonTooManyWedged)`（:1371）。**没有** `d.Warn` / `d.Env` / `envGet` / dropped 检查。
- plan §4 H9（`remote-fs-stale-health-plan.md:193`）明写「② TTL 到期后仍能重探并 `dropped` 非空」。⇒ 落盘时掉了一半被指定的断言。
- 注意这条测试是**承重的**（它正是抓 F-16 那个变异的那条），所以修法是补断言或改名，不是删。

### F-24 · C1 压测掉了 plan §4.2 的 `WedgedCount` 归零断言

- `test/concurrency/spawnsafe_stress_test.go:139` 之后只有 `assertNoGoroutineLeak`；plan §4.2（`:203`）要求「无 race、无 panic、`assertNoGoroutineLeak` 回基线、**`WedgedCount` 归零**」。`WedgedCount()` 已导出（`spawnsafe.go:1168`），当时就可用。
- 订正两点（照收）：(a)「非 `--safe` 路径从未被压」说过头了——`:99` 有专门的 `InvalidateHealthy` goroutine、`:85` 的时钟驱动每 ms 推进 20s，T11 确实会触发，只是每次都伴随 per-Prepare 作废；(b) 补的 `WedgedCount()==0` 必须**轮询**而非一次断言——放弃的槽是在 reaper goroutine 里释放的（`:1125-1131`），裸断言会 flake。

### F-25 · `startBounded` 的指路注释指向一份**没有那段「为什么」**的 doc

**独立发现：2 个视角**（调用点 / 文档契约）。

- `internal/agent/exec.go:375-378` 写「Contract (… and **why each step exists**) lives on `spawnsafe.RunStartWithCleanup`」；被指向的 `spawnsafe.go:1096-1099` 与 `git show HEAD:` 逐字节相同——**只有 4 行、无 fd、无 pipe、无 review M4 的「否则长时间 outage 下 fd 无界泄漏」论证**。
- plan §3 B1（`:159`）要求那段按「注释是资产，整段搬运」搬到 `RunStartWithCleanup` 的 doc；实际是**删掉没搬**（幸存副本只在 `exec.go:180-183` 的调用点，run.go 的调用点没有）。
- 附带：`:1097` 的括注仍写 hooks 是给 “resources that live outside the generic policy (**the PTY Session**)”——塌缩后 exec 侧（`exec.go:185-193`）也是钩子调用方，括注已不完整。
- 缓解：同一 plan bullet 的另一半（订正 `TryAcquireSpawnSlot` 的过时建议）**做了**（`spawnsafe.go:1141-1147`）。

### F-26 · drill 62 Arm 1S-4 缺判别式（PLAUSIBLE，强形式已被驳回，见 §5）

- **成立的部分**：`test/simcluster/drills/62-remote-fs-safe.sh:137` 的 re-prime 是 `>/dev/null 2>&1 || true`，结果与退出码全丢；1S-4 期望的 `remote_fs_unhealthy` 无法区分「cached-healthy 被 `--safe` 丢弃」与其它到达同一码的状态。对照第一半（`:127-133`）自带判别式，其注释直说「Getting `remote_fs_spawn_timeout` here (rather than `remote_fs_unhealthy`) is what proves the verdict really was cached-healthy going in」——**作者显然知道需要判别式，只在一半上加了**。Arm 3（`:148`）之后 reprovision agent，后面的臂也补不上。
- **不成立的部分见 §5**：所提的「删掉 re-prime ⇒ 该臂静默变绿」经两个验证者独立推演为**变红**。
- **建议修法（含修正后的变异方案）**：在 `:137` 与 `:138` 之间加一条只有「重新缓存为 HEALTHY」才能满足的正判别（例如断言此刻普通 `RFS agt1 -- "$MNT/probe"` 失败于 ENOENT/exec_failed 而**不是** `remote_fs_unhealthy`）。**变异用 T7 drain**（把 `spawnsafe.go:655` 的 `h.state != stHealthy` 改成 `h.state == stUnprobed`，让迟到的成功永远治不好）——1S-4 单独仍绿（同一个码、错的理由），新判别式必须红。**必须在 weilandserver 上真跑**（`cd test/simcluster && ./local.sh drill 62-remote-fs-safe`），`bash -n` 看不见（memory `feedback-drill-asserts-must-run-not-lint`）。

### F-27 · `smokeVersion` 的 execve 不在任何**有效**看门狗之下（登记项，非本批缺陷）

- `internal/agent/upgrade.go:786-789` 用 `exec.CommandContext(ctx, binPath, "version")`；`binPath` 由 `os.MkdirTemp(filepath.Dir(dst), ".tether-upgrade-*")`（`:531-537`）暂存，`dst` 源自 `os.Executable()`。
- ctx 结构上无法抢占卡在 execve 的 fork/exec：`$(go env GOROOT)/src/os/exec/exec.go` 的 `go c.watchCtx(resultc)`（:775）在 `os.StartProcess`（:725）**返回之后**才启动，`WaitDelay` 只在进程被 reap 后生效。（`:531-536` 的 MkdirTemp/解包其实会更早卡住。）
- 节点前提是**推断而非实测**：`docs/deploy-tier-gotchas.md:1013` 记 timan107「无 sudo / 无 systemd 自启」、`:1001` 记其 Home 在 NFS `/home/zixuans8`——家目录安装可信但没人量过二进制实际位置。
- 这不使 #81 的修复变错（它不铸造哨兵、没有超时臂可接线），但它证伪了 `spawnsafe.go:775-776` 那句话周围的**穷尽性语感**（该句字面为真）。

**建议修法**：按仓库惯例**登记**而非静默省略——在 `docs/deploy-tier-gotchas.md` #81 下加一行 `[GAP]`，写明 `smokeVersion` 是刻意在看门狗之外的 execve、理由（跑本地暂存产物、不产生挂载证据）、以及残留风险（安装目录在挂死挂载上时无界挂起）。真修它是另一个叶子增量。

---

## 4. 细节

| # | 一句话 | 证据 |
|---|---|---|
| F-28 | 把未导出的 `invalidateHealthy` 合并成只留 `InvalidateHealthy`（一个合理化简，clause ②/③ 本就两种拼法都收）会让闸门对**完全正确的代码** `t.Fatalf` | `spawn_stall_evidence_test.go:55` 的 `const invalidateFn` + `:120-121/:137-139`；实测报 “no invalidateHealthy function found”。修法：存在性检查也接受导出名 |
| F-29 | 闸门遍历的两条事实没写下来：(a) `:213-221` 只走 `CommClause.Body`、从不走 `.Comm`，所以 `case out <- ErrSpawnTimeout:` 不可见（实测 0 sites）；(b) 臂内 FuncLit 里的铸造被算到**外层具名函数**头上且 `inTimeoutCase=true`（实测：把该键当 wired 加进账本即 ok）。两者都可接受，但该记进 KNOWN BLIND SPOTS。好消息：混合 hand-walk + `ast.Inspect` 的结构经构造性验证**无重复计数、无漏节点**（嵌套 select / switch / for / `return "", &FSError{}` 均恰好 1–2 sites） | `spawn_stall_evidence_test.go:212-244` |
| F-30 | INV-1 写「`h.result == nil` ⟺ 该 struct 的探针 goroutine **provably exited**」强于代码所立：`result` 是 buffered(1)（`:693`），goroutine body 就是 `ch <- probe(mp)`（`:697`），发送无需接收者，drain/launcher 置 nil 时它只是**已从 `probe()` 返回**、尚在收尾。设计需要的也只是「不再卡在 statfs 里」 | `spawnsafe.go:186-189` vs `:693/:697/:658/:735` |
| F-31 | `cmd/tether/error_hints.go:229` 的溯源 `(spawnsafe.go:812-814)` 已指错——本轮 +190~228 行后该区间落在 `type Decision struct` 中间，ceiling 的两个铸造点现在在 `:999-1000` 与 `:1106-1107`。该文件本轮**被改过**（`remote_fs_spawn_timeout` 那条 hint），顺手没改这条 | 按 CLAUDE.md「溯源用稳定锚点」换成函数名（新闸门账本正是按函数名键控的） |
| F-32 | gotcha 里两句关于 drill 62 覆盖面的陈述被本轮新增的 Arm 1S 当场作废：`:1023`「**不能**在现有 `62-remote-fs-safe` 里复现」——1S-0 造的正是那个前置态；订正 (c) `:912`「现有 62 的断言**全部**发生在 `sanitizePATH` 之前」——1S-2 断言的 `remote_fs_spawn_timeout` 由 `RunStartWithCleanup` 铸造，在 `sanitizePATH`（`:907`）之后。(c) 想说的实质结论（`outage=true` 码路零 deploy-tier 覆盖）**仍然成立**，只是论据措辞失真 | 改成「本轮起可复现前置态；仍不可复现的是后半段（真 uninterruptible-D + 隔离宿主，OQ-2 冻结为 NOT-COVERED）」/「62 的断言里没有一条落在 `outage=true` 之后的行为上」 |
| F-33 | **PLAUSIBLE**：`usage.md:1484-1485`「要**跳过**这一次，用 `--safe`」偏强——`--safe` 压掉的是那次**慢**失败（30s→一次探测）与子进程继承中毒 PATH，不是失败本身（1S-4 断言的正是第一条 `--safe` 仍失败），且 `--safe` 自身要付 `probe_timeout`（同页 `:1492-1493` 已正确写明，与「跳过」口径不一致）。缓解：`:1497-1500`「做不到的事」与 `:1503-1504` 已给出限定，所以是**措辞松**而非事实错，且对主要生产形状（相对 argv[0] 可在死盘外解析，即 timan107 的 `nvidia-smi`）`--safe` 确实跳过了那次献祭 | 建议改成「把这一次的**代价**从 30s 压到一次探测（并避免子进程继承中毒 PATH）……如果 argv[0] 真在那块死盘上，`--safe` 只是让你更快看到 `remote_fs_unhealthy`」 |
| F-34 | #81 标了 FIXED 但全文仍在正文（`docs/deploy-tier-gotchas.md:886-974`）、末尾索引表也没有 #81 行，违反文件头 `:15-17` 自定的剥离规矩（#28/#45/#58/#68/#75/#76/#77 都是已剥离先例）。**这大概率是刻意推迟到 CLAUDE.md §3 step 7 的归档步骤**——提出来是为了它别被忘。剥离时注意订正 (c) 的 `[GAP]` 是**未了结**内容，别随全文沉进 closed 文件 | — |
| F-35 | **审查环境注意（非本批引入，但会误导下一次 `make gates`）**：`.claude/worktrees/wf_*` 下的 review worktree 被 repoRoot 全树扫描器当成第二份仓库，计数**精确翻倍**。实测当前工作树：`TestNolintDirectivesNameEnabledLinters` 报 “found 70 … expected exactly 35”、`TestInsecureSkipVerifyIsAlwaysPairedWithChainVerification` 报 “8 … expected exactly 4”，offender 直接印成 `.claude/worktrees/wf_.../internal/tunnel/tls.go`。新闸门免疫**纯属偶然**（它走 `root/internal` + `root/cmd` 而不是 root 全树，`spawn_stall_evidence_test.go:96-98`）。跑硬闸前先删 review worktree；或给共享 walker 的 skip 表加 `.claude` | `test/architecture/nolint_directive_test.go:185-194` 的 skip 表、`build_tags_test.go:101-117` 的 `repoRoot` |

---

## 5. 被驳回的 finding（REFUTED）

> 保留在此是为了下一轮不必重新推导。

### REF-1 · 「删掉 drill 1S-4 的 re-prime，该臂会静默变绿（恒等式通过）」

**报告方**：调用点视角 R1-F6（其验证者判 REFUTED）；风险视角 R1-F8（其验证者判 PLAUSIBLE 并给出同一反驳）；状态机视角 R1-F4 判 CONFIRMED，**但其论证漏了 T7 drain**。

**为什么被驳回**：断言链忽略了 `mountHealthy` 最前面的自愈 drain（`spawnsafe.go:655-668`）。真实序列是：

1. 1S-3 之后 `$MNT` 是 `stUnhealthy`，但 `h.result` 被**刻意保留**给自愈（`spawnsafe.go:748-751`，注释 “retain h.result for self-heal”）；
2. `_heal`（`62-remote-fs-safe.sh:56`）是 `pkill -CONT` **加** `poll_until 10 1 … _statfs_healthy`——它本身就是 `assert_ok`，宿主 statfs 被证实恢复才继续，于是被放弃的探针早已把 `true` 送进 buffered channel；
3. 即使删掉 `:137` 的 re-prime，drain 也只是**推迟到 1S-4 自己那次 `--safe` 调用**发生：`Prepare:884` 的 `invalidateHealthy` 对非 `stHealthy` 条目是 no-op（`:787`），随后 `pathOnDeadMount:610 → mountHealthy:655-668` drain 掉那个 `true`、置 `stHealthy`、重盖 `decidedAt`；
4. ⇒ 死 cwd/死 argv[0] 的快速失败**不触发**，agent 对已重新 wedge 的挂载 execve，而 `RFS` 的 `timeout 25`（`:60`）短于 30s 看门狗 ⇒ `assert_refuses "…remote_fs_unhealthy"` **变红**，不是变绿。

要让 1S-4 真的恒等式通过，需要「被放弃的 statfs 在整个 heal 窗口内一次都没被调度」——而 `_heal` 的轮询断言恰恰排除了它。

**幸存下来的弱形式**已记为 F-26（判别式缺失），但**变异方案必须换**：删 re-prime 不可用，改用 T7 drain 变异。另注意三个视角里**没有一个真跑过这个 drill**——F-26 的任何修改都必须在 weilandserver 上实跑验证。

---

## 6. 新增测试建议（每条带变异验证方案）

> 标 ✅ 的表示**已在本轮审查中写出并实测过红绿两向**（在 scratch 副本里，原仓库未动）。

### 6.1 行为守卫（`internal/spawnsafe` / `internal/agent`）

| # | 测试 | 断言 | 变异验证 |
|---|---|---|---|
| **T1** | `TestBoundedHomeRead_stallExpiresCachedMountHealth`（`internal/agent/remotefs_test.go`）**4 个视角各自提出** | 用 `fakeMountinfo({"/nfs","nfs4"},{"/","ext4"})` + 计数探针建 agent（`newTestAgent` 已支持注入）；先经 `a.spawnPolicy.Prepare` 在含 `/nfs/bin` 的 PATH 上播种 healthy（probe=1）；再用阻塞 loadFn 驱动 `boundedHomeRead` 撞死线；断言下一次 `Prepare` **重探**（probe=2）。**时钟不推进**，所以通过只可能来自作废，不可能来自 TTL | 删 `internal/agent/agent.go:1576` ⇒ 今天 `./internal/agent`（整包）、`./internal/spawnsafe`、`TestSpawnTimeoutMintSitesNoteEvidence` **全绿**（已实测）；加了本测试必须红在 probe count |
| **T2** ✅ | 修 `TestMountHealthy_reArmSurvivesConcurrentLauncherWakeup`（**不新开函数**） | 给 16 个 consult worker 单独 WaitGroup，`workers.Wait()` → `close(stop)` → `wg.Wait()`；并按 plan H6/R6 补 `-race -count≥200` 与阻塞探针 choreography（launcher 撞超时期间另一 caller drain，再 re-arm）。最终断言不变 | M7-lite（两处换指针→原地重置，保留 `close(done)`）⇒ 修改前 `-race -count=200` 绿、修改后 3/20~每次红（`healthy mount was demoted by re-arm churn alone`）；M7-full（再回退 `close(done)`）⇒ `panic: close of closed channel` / `close of nil channel`；未变异实现 `-race -count=100/200` 绿（3.1s/5.4s） |
| **T3** ✅ | `TestMountHealthy_reArmReplacesGenerationPointer`（确定性白盒） | 播种 healthy → 在 `p.mu` 下快照 `p.health[mp]` → `InvalidateHealthy()` → 断言 map 里现在是**不同指针**，且被孤立的旧 struct 仍读到 `state==stHealthy && launched==true`（迟醒的 launcher 写给没人读的对象）；再跨 `clock.advance(ttl)` 边界为 T11 重复一遍 | 同 T2 的原地重置变异 ⇒ 红（`T10 re-armed IN PLACE: the generation pointer must be REPLACED`）。**这条是把设计决策从并发调度里解耦出来的那一道**，与 T2 互补而非替代 |
| **T4** ✅ | `TestPrepare_safeInvalidatesBeforeCwdCheck` | 播种 healthy → `setVerdict(staleMount,false)` → 时钟冻结、零证据 → **第一次** `Prepare(argv:["true"], cwd:staleMount+"/nas", safe:true)` 必须返回 `*FSError{Code: ReasonUnsafeCwd}`；并断言注入的 Resolver **零调用**（证明 cwd 快速失败早于 argv[0] 解析） | 把 `if requestedSafe { p.invalidateHealthy() }` 从 `spawnsafe.go:878` 挪到 `:897` 的 cwd 检查之后 ⇒ 今天 `./internal/spawnsafe` + arch gate **全绿**；本测试红（`got <nil>, want remote_fs_unsafe_cwd`）。整块删除已被 `TestPrepare_safeForcesRevalidation` 覆盖 |
| **T5** ✅ | `TestRunStartWithCleanup_reapsBeforeReleasingTheWedgeSlot`（补上 plan OQ-1 的硬性前置） | `WedgeCeiling: 1`；`RunStartWithCleanup(blockingStart, 20ms, nil, reap)`，`reap` 把当时的 `p.WedgedCount()` 写进 buffered channel。调用须返回 `ErrSpawnTimeout`；放开 start 后 `reap` 观测到的值必须是 **1**（槽在 reap 期间仍被持有） | 对调 `spawnsafe.go:1126-1131` 的两句 ⇒ 今天 `./internal/spawnsafe`、`./internal/agent`、`./test/concurrency` 全绿；本测试红（`reapOnReturn saw WedgedCount=0`） |
| **T6** | `TestNew_rejectsBadConfig` 补 `HealthTTL:-1` 一格；另可加 `TestNew_configErrorsNameOnlyRealAgentYamlKeys`（收集所有 `New` 错误串里的 `remote_fs.<key>` token，对账 `remoteFSConfig` 的 yaml tag 集合） | 见左 | 删 `spawnsafe.go:267-269` ⇒ `New` 返回 nil ⇒ 红。第二条今天**就是红的**（`remote_fs.health_ttl` 不在任何 tag 里）；改成不点 YAML 键后转绿；再把 `probe_timeout` 故意写成 `probe_timeoutt` ⇒ 重新变红（证明非恒等式） |
| **T7** | `TestPrepare_oneCommandProbesEachMountAtMostOnce`（**墙钟预算**，今天零覆盖） | 4 个 hangable 挂载、每挂载 2 个 PATH 目录（8 条），探针阻塞 `d=30ms` 后健康，`ProbeTimeout ≫ d`；播种 healthy → `invalidateHealthy()` → **一次** `Prepare`。断言恰好 4 次探针（按 mountpoint 去重，不按 PATH 条目）**且** 总耗时 < 6·d | 把 `pathOnDeadMount`（`:610`）改成 `p.mountHealthy(path)` 而非 `p.mountHealthy(m.mountpoint)` ⇒ 探针 4→8、耗时翻倍 ⇒ 两条子句都红 |
| **T8** | `TestMountHealthy_reArmedGenerationStillBroadcastsToJoiners` | `ProbeTimeout=1s`、探针 ~30ms 后健康；播种 healthy → `InvalidateHealthy()` → 64 个并发 `mountHealthy`。断言 (a) 探针只 +1（单飞在 re-arm 之后仍成立）**且** (b) `wg.Wait()` < 300ms（新一代的 joiner 仍被唤醒、而不是各等满一个 probeTimeout） | 删 launcher 成功臂的 `close(done)`（`spawnsafe.go:744`）⇒ 探针计数仍是 1（既有守卫全绿），但每个 joiner 落到自己的 `time.After` ⇒ 耗时 ≈1s ⇒ (b) 红。**这条同时是 F-14 那条「真正幸存的理由」的机械形式** |
| **T9** | `TestMountHealthy_abandonedProbeBudgetIsPerMountGeneration`（把 F-13 的真界写成断言） | 探针对该挂载永久阻塞 ⇒ 首探超时判死、留 1 个被放弃的 goroutine；翻转该挂载**自身**的 signature 并 `refreshIfChanged()` ⇒ 断言 `probe.count(mp)==2` 且相对基线多出 **2** 个在途探针；最后放开并 `assertGoroutinesReturn` 回基线 | 把 `applyMounts` 的 signature 比较改成恒真继承 ⇒ count 停在 1 ⇒ 红（同时也让既有 `TestProbe_resetsWhenMountInstanceChanges` 红，互为佐证）。反向变异：把 T13 的丢弃条件放宽到**任何**代际变化（F4 回归形状）⇒ 既有 `TestProbe_survivesUnrelatedMountChurn` 红 ⇒ 证明本测试没顶掉 F4 的门 |
| **T10** | `TestPrepare_wedgeCeilingSaturatedStillDropsDeadPathDirs` 补齐 plan H9 ②（或改名） | `clock.advance(ttl)` 后释放 wedge 槽（`close(release)`，轮询 `WedgedCount()==0`），再跑一次 `prepare`，断言 `d.Outage` 且 `/shared/bin` 不在 `envGet(d.Env,"PATH")` 里 | 让 TTL 到期后不重探（M2 形态）⇒ dropped 为空 ⇒ 红 |
| **T11** | `test/concurrency/spawnsafe_stress_test.go` 补 plan §4.2 的缺项 | `wg.Wait()` 后**轮询**（带短 deadline，不要裸断言）`WedgedCount()==0`；并把一半迭代改成 `requestedSafe=false`，让 T11 在无显式作废时也被压 | 让 `RunStartWithCleanup` 的放弃路径漏掉 `ReleaseSpawnSlot` ⇒ 轮询超时 ⇒ 红 |

### 6.2 闸门加固（`test/architecture/spawn_stall_evidence_test.go`）

| # | 改法 | 断言 | 变异验证 |
|---|---|---|---|
| **T12** | **用锁支配关系取代 clause ③ 的名字匹配**（名字匹配可留作廉价二检） | `internal/spawnsafe` 的任何非测试函数里，`invalidateHealthy`/`InvalidateHealthy` 的调用不得被同函数内未配对的 `p.mu.Lock()` 支配（其前也无 `defer p.mu.Unlock()`）；报错点名函数与 Lock 位置 | (1) 抽 `func (p *Policy) mountHealthyReArm() { p.invalidateHealthy() }` 并在 `mountHealthy` 的 `p.mu.Lock()` 后第一行调用 ⇒ 今天闸门绿、`./internal/spawnsafe` **硬死锁**（`timeout 90` exit 124/143，已实测）；必须红。(2) 把 `Prepare` 的 `p.invalidateHealthy()`（:884）挪到 `p.mu.Lock()`（:885）之后 ⇒ 今天绿、`TestPrepare_safeForcesRevalidation` 挂死；必须红 |
| **T13** | **加宽铸造谓词** | 额外识别：`return` 里 `fmt.Errorf`/`errors.Join`/`errors.New` 的实参含哨兵；把哨兵赋给具名返回值的 `AssignStmt`；`Code:` 为等于哨兵值的 `BasicLit` | 往 `internal/agent/exec.go` 追加第三个 watchdog，超时臂 `return fmt.Errorf("%w: futureWatchdog", spawnsafe.ErrSpawnTimeout)`、不接线 ⇒ 今天闸门 **ok 0.28–0.31s**（已实测，`go build`/`go vet` 均静默）；修好后必须报账本漂移并点名 `internal/agent/exec.go`。第二变异：把 `spawnsafe.go:1031` 的 `Code:` 改成裸字符串 ⇒ 修好后必须仍被找到且判为 wired |
| **T14** | **不再合并同键**（或合并但报冲突） | 键加位置/序号；或单键下两个 occurrence 的 `(inTimeoutCase, invalidated)` 不一致时报错并打印**两处**位置 | 在 `RunStartWithCleanup` 的 wedge-slot 检查后插入未接线早退（`if timeout <= 0 { return ErrSpawnTimeout }`）⇒ 今天 **ok 0.25–0.29s**（三次独立实测）；必须红。**两种源码顺序都要试**——当前实现在两种顺序下都宽松 |
| **T15** | **双向化账本** | `wantWired == false` 的条目改成断言 `!s.invalidated`（消息引用 §0 A9 的理由）；注意 ceiling 铸造点不在任何 select 臂里，需检查外围语句而非只看臂 | 在 `boundedResolveInDirs` 的 `return "", &FSError{Code: ReasonTooManyWedged}` 前加 `p.invalidateHealthy()` ⇒ 今天 **ok 0.26–0.28s**（两次实测）；必须红 |
| **T16** | **臂内扫描遇 `*ast.FuncLit` 停止下降**（收紧 clause ②） | 死线臂里被 defer/goroutine 藏起来的作废调用不再算数 | 把 `p.invalidateHealthy()` 从 `time.After` 臂顶挪进被放弃的 reaper `go func(){ err := <-done; … }()` ⇒ 今天闸门 **ok 0.275s**（行为守卫 `spawnsafe_test.go:1363` 会红）；修好后闸门也必须红 |
| **T17** | **别对 timer 形状误报** | 接受同函数内由 `time.NewTimer`/`NewTicker` 赋值变量的 `<-tmVar.C` | 正控：把 `RunStartWithCleanup` 改成 `tm := time.NewTimer(timeout); defer tm.Stop(); case <-tm.C:`，保留作废 ⇒ 今天 **FAIL at :190** 而 `./internal/spawnsafe` ok 0.606s（已实测）；修好后必须绿。**负控**：同一重构 + 删掉作废 ⇒ 仍须红（否则加宽把 clause ② 打瞎了） |
| **T18** | 存在性检查接受两种拼法 | `fn.Name.Name == invalidateFn \|\| fn.Name.Name == "InvalidateHealthy"` | 删掉未导出方法、四个调用点全走导出名 ⇒ 今天 `t.Fatalf("no invalidateHealthy function found")`（已实测）；修好后必须绿，而**真删掉作废**时仍须 fatal |
| **T19** | 重写 KNOWN BLIND SPOTS | 删掉「import alias」这条假洞（实测别名与 dot-import 都被看见、minting helper 会造新键直接变红），写上四个真洞（中间变量 / 具名返回值 / 裸字符串 `Code` / `fmt.Errorf %w`，最后一条在 T13 落地后标为已闭合），再加 F-29 的两条遍历事实 | 段落改动本身无变异；但 T13 落地后应把「%w 包装」那行标闭合并跑一次 T13 的变异确认 |

### 6.3 文档 / 契约门

| # | 测试 | 断言 | 变异验证 |
|---|---|---|---|
| **T20** | `test/determinism`: `TestSafeFlagContractStatedInBothHelpAndUsage` | ① `cmd/tether/exec.go` 与 `run.go` 的 `--safe` help 串逐字相同（今天相同，防再分叉）；② `docs/usage.md` 里每一行以 `` | `--safe` | `` 开头的 flag 表格行（今天 :704/:749）必须同时含「重探/重新探测」与「健康」两个 token；③ §7.7 的 `--safe` 段落同样含这些 token。任缺其一即红，报错点名「改 `--safe` 契约要三处一起改（`feedback-contract-change-sweep`）」 | **今天这道门在树上就是红的**（②，即 F-11 的现状），先修文档才转绿。正控：把 `exec.go:157` 改回旧文案 ⇒ ① 红。反向变异：把断言弱化成「usage.md 任意位置含 token 即可」⇒ 删掉 :704/:749 两行仍绿 ⇒ 证明必须按行匹配 |

### 6.4 deploy-tier

| # | 改法 | 断言 | 变异验证 |
|---|---|---|---|
| **T21** | `62-remote-fs-safe.sh` Arm 1S-4 判别式（见 F-26） | 在 `:137` 与 `:138` 之间加只有「重新缓存为 HEALTHY」才能满足的正判别 | **不要用「删 re-prime」**（见 §5：预测为红，不是绿）。用 T7 drain 变异：把 `spawnsafe.go:655` 的 `h.state != stHealthy` 改成 `h.state == stUnprobed` ⇒ 1S-4 单独仍绿（同码、错理由），新判别式必须红。**必须在 weilandserver 上真跑该 drill**，`bash -n` 看不见 |

---

## 7. 结论

**ship-with-fixes。**

**不需要 block 的理由**：实现本身没有被发现的正确性缺陷。状态机的两条不变量（INV-2 的 `stHealthy ⇒ result==nil && !decidedAt.IsZero()`、`launched ⇒ done!=nil`）经动态 panic 探针在全包 `-race -count=30` 与并发压测下未被违反；dead 绝对 sticky 的守卫在代码里写了两遍警告；deploy-tier 双向证据成立。

**必须在 commit 前落地的**（否则这一轮留下的是一份**记了假的验证账本** + 三个承重不变量零守卫）：

1. **F-2**：修 `close(stop)` 位置 + 加确定性指针身份守卫（T2 + T3），并**订正 gotcha #81 里「13 条…每条都做过变异验证」的表述**。这条是 block 与否的分界——按 CLAUDE.md 的账本纪律，一条被证伪的验证记录比它掩盖的洞更该先修。
2. **F-1**：加 T1，并把 `spawnsafe.go:772-778` 那句夸大闸门覆盖面的注释改成实情。
3. **F-3**：加 T4（`--safe` 作废位置）。
4. **F-4**：把闸门 clause ③ 换成锁支配不变量（T12）——它今天放行的那个形态**实测是进程级硬死锁**。
5. **F-10 / F-11**：两处文档订正（#82 的「前半段已消失」、`usage.md:704/749` 的 `--safe` 契约），代价极低且都是 plan 明文要求。

**建议同批捎带**（都是几行）：F-13（预算注释加 generation 限定）、F-14（`close(h.done)` 引信注释两条 bullet 重写）、F-19（错误串不点不存在的 yaml 键 + 补一格 `TestNew_rejectsBadConfig`）、F-23（补 H9 ②）、F-31（error_hints 溯源换函数名）。

**可以留到下一个叶子增量**：F-5（targeted vs 全局的设计取舍）、F-6 的两击迟滞、F-12 的抖动与墙钟预算门、F-27（`smokeVersion` 登记 `[GAP]`）、F-35（给共享 walker 加 `.claude` skip）。

**操作提醒**：重跑硬闸前先删掉本轮审查留下的 `.claude/worktrees/wf_*`，否则 `TestNolintDirectivesNameEnabledLinters` 与 `TestInsecureSkipVerifyIsAlwaysPairedWithChainVerification` 会因扫描面翻倍而红，且红得指向没人碰过的代码（F-35）。