# remote-fs stale-health 修复计划（#81，附 #82 范围裁决）

> Date: 2026-08-30。CLAUDE.md §3 阶段 A 产物，**主进程定稿**。
> 来源：step 1 的对抗性 workflow（4 视角起草 → 4 个对抗性 critic → 1 综合，run `wf_7c86faa2-a2f`）。
> 本文只写计划，不含实现代码。file:line 对当前工作树核实（`internal/spawnsafe/spawnsafe.go` 1015 行）。

## §-1 主进程定稿裁决（step 2）

综合稿的核心语义与状态机设计**整体采纳**——它用 INV-1/INV-2 证明了 `h.result == nil` 是空条款
（三份草案把 D 态线程界挂在一条恒真条件上，落地即恒等式测试），并用**换指针**同时解掉
`close(h.done)` 的所有权与 launcher 超时分支 `:609` 那条 `stUnprobed` 守卫。这两点比我定稿前
自己的方案（在 `mountHealth` 上加一个显式 `inflight` 位）更省：INV-2 已经蕴含"无在途探针"，
加字段是净增面。**采纳综合稿，放弃我原来的 `inflight` 方案。**

以下是我在综合稿之上做的改动，逐条列明（未列出的部分即原样采纳）：

| # | 项 | 定稿裁决 |
|---|---|---|
| **F1** | `agent_provision_yaml` 调用点数 | 综合稿写 34，**实测 36**（`grep -rn "agent_provision_yaml " test/simcluster/drills/*.sh \| wc -l`）。数字改正；§4.5"该走独立增量"的结论不变，反而更强 |
| **OQ-1** | `startBounded` 收敛是否同批 | **同批收敛**。留两个看门狗家族＝第三条 spawn 路径必然第三次漏接线，与 §3 step 5b 记载的 tunnel fence 三轮返工同形。**附加硬性前置**：先补一条对旧实现与新实现**都绿**的等价测试（`onAbandon` 同步调用、`reapOnReturn` 先于 `ReleaseSpawnSlot`），再做替换——R11 动的是泄漏账，不能凭"逐行同构"目测过关 |
| **OQ-2** | `DefaultHealthTTL` | **维持 5 min**。我独立再权衡一次：取短（30s）把误判掷骰次数抬一个数量级，换来的只是"零证据负载愈合更快"——而零证据负载恰恰是**只跑绝对路径 argv[0] 的用户，他们本来就没被 #81 打到**（命令照常能跑）。为一类不受损的用户抬高另一类用户的误判风险，是错的取舍。取长 |
| **OQ-3** | 作废限速 / 探针超时与 resolve 超时解耦 | **都不做**，同意综合稿。两个旋钮保留在本文备查 |
| **OQ-4** | `Config.OnDemote` 降级日志回调 | **不做**。#81 无声活 18 天的原因是 `outage=false` 导致连既有 `dropped` 横幅都没发出，修复后那条横幅本身就会出现；再加回调是净增面 |
| **OQ-5** | drill Arm 1S-2 的新增宿主暴露是否需要重新裁定 OQ-2 | **我签字：不需要**。OQ-2 冻结的是**无界** chdir/exec；1S-2 是 30s 看门狗管着的有界 execve、FUSE 近似 SIGCONT 可逆。**但把两条降级为可选的东西升为强制**：1S-2 之后必须有 `_heal`，1S-5 的 alive-control 是强制项。少任何一条，这一臂不许合入 |
| **OQ-6** | `outage=true` 全链路的 deploy-tier 覆盖 | **独立叶子增量**，本批以 `[GAP]` 登记进 #81 的 FIXED 条目。R1 严重但它的缓解是分级上线，不是把一个 36 调用点的 fixture 契约变更塞进状态机修复 |
| **OQ-7** | 网络 Home 的写持锁毒化后续读，写进 usage 还是 gotcha | **拆开，两边都写**。已核实的**源码事实**（`state.go:190/217/240/258/279` 全在 `s.mu` 下调用无界 `saveLocked:302`，一次落在死挂载上的写会永久持锁、此后每个读都堵在它后面）进 `docs/usage.md §7.7` 的已知边界——它是真实的用户可见边界，与 #82 是否归因无关；"这可能是 #82 的根因"作为**假说**只进 gotcha #82。这样两边都诚实，也不会被读成 #82 已归因 |

**外审门**：本增量按 CLAUDE.md §3 走到 step 5b（测试归位）后**停在 step 6 外审**，不 commit、不 push、不 `git add`。

---

## §0 裁决摘要（四份草案的核心分歧，逐条定案）

| # | 分歧 | 裁决 | 依据（含被击穿的一方） |
|---|---|---|---|
| **A1** | 证据驱动作废 vs TTL vs 两者叠加 | **两者叠加：证据为主，惰性 TTL 为兜底** | 证据单独不够，有**两类可核实的零证据故障**（下详）。TTL 单独不该做（把误判骰子从"一生一次"放大数万倍）。lens=callsite / lens=adversary 的「TTL 一点额外能力都不提供」被证伪；lens=testing / lens=statemachine 的「TTL 是主路径」也被否 |
| **A2** | 重新武装用「原地重置字段」还是「换一个新 `*mountHealth`」 | **换指针（epoch swap），绝不原地重置** | 原地重置同时引爆两条现存引信：`:604`/`:613` 关的是**字段** `h.done`（不是 `:573` 捕获的局部量）⇒ close-of-closed / close(nil) panic；`:609` 的 `if h.state == stUnprobed` 会让掉队 launcher **拒绝降级**一个 stale-healthy 条目（lens=adversary 的 fatal #1 就是这条，它的草案正因为保持 `state=stHealthy(stale)` 而被自己击穿）。换新指针后 `state` 是 `stUnprobed`，:609 正常生效，`done` 由且仅由自己的 launcher 建/关一次 |
| **A3** | 重探守卫要不要 `h.result == nil` 这一项 | **丢弃**。守卫只保留 `h.state == stHealthy` | lens=statemachine 的草案把「不泄 D 态线程」的全部论证挂在 `result == nil` 上，而它的批评者证明该项**恒真**（`:543→:545`、`:595-596→:599` 两处写 stHealthy 前都先把 result 置 nil）⇒ 是一条拦不住任何东西的空条款，且它的两条"变异验证"因此是恒等式。真正承重的是 `state == stHealthy` 那半，必须把注释写对——lens=callsite 的草案要求把**错的**论证写成永久注释，明确丢弃 |
| **A4** | 作废的表示：时间戳归零 / `stale bool` / 直接换结构体 | **直接换结构体（eager replace）**，不引入 `stale` 位 | lens=testing 的「healthyAt 归零当哨兵」被击穿：注入时钟基准若靠近零值，`now.Sub(zero) >= TTL` 为假 ⇒ 正确实现在它自己的两条"时钟一步不推进"测试下变红。作废是 O(#mounts) 纯内存、可在 `p.mu` 下直接 `p.health[mp] = &mountHealth{}`，比"打标记 + 惰性换"少一个字段和一条边 |
| **A5** | 新增状态成员 / 新字段 | **状态集不变**（`stUnprobed/stHealthy/stUnhealthy`），只加 `decidedAt time.Time` | `test/determinism/enum_switch_default_test.go:83` 有 `"spawnsafe.probeState": {"st"}` 家族项，新增 `st*` 成员会连带那道门。`decidedAt` 只服务 TTL |
| **A6** | 收敛 `startBounded`（exec.go）到 `RunStartWithCleanup` | **采纳**，且是闸门可精确化的前提 | 两份实现逐行同构（`exec.go:380-399` vs `spawnsafe.go:917-950`，差别只有 nil 检查与 `a.spawnTimeout()`）。收敛后 `ErrSpawnTimeout` 的**铸造点**从 3 处降到 2 处，闸门可从"文本绊线"升级成"铸造点精确计数"。带回退方案（见 §3 B1 / §8 OQ-1） |
| **A7** | 闸门谓词用 `time.After` 还是 sentinel 铸造点 | **sentinel 铸造点** | 实测 `internal/agent` 非测试文件有 **10** 处 `time.After(`（8 处是退避/心跳/拆链），`internal/spawnsafe` 有 **5** 处（`:313` boundedTouch、`:582` joiner、`:607` launcher 自身——在 `:607` 之后调作废会 self-deadlock）。以 `time.After` 为谓词的门要么是恒等式、要么是哭狼机器（lens=statemachine / lens=testing 的门设计被这条击穿） |
| **A8** | drill 用 `--cwd <死挂载>` 还是显式 argv[0] 造 stale-healthy | **显式 argv[0]（`$MNT/probe`）**，不做 `--cwd` 灌缓存 | `--cwd` 路径会把一次**真实 chdir** 打进 wedge（`cmd.Dir` 落在死挂载上，fork 后子进程在 FUSE 里等），正是 62 头部与 OQ-2 定格 NOT-COVERED 的宿主危险；`_fuse_stopped` 读的是 **daemon** 状态，与 agent fork 出的那个子进程无关，所以 lens=testing / lens=adversary 给的"安全性论证"测错了对象。显式 argv[0] 只做一次 statfs + 一次 ENOENT execve，零 chdir |
| **A9** | ceiling（`ReasonTooManyWedged`）算不算证据 | **不算**，但必须显式登记并测 | 槽位耗尽是既往超时的后果，且高并发也能耗尽。**关键事实（三份草案都漏了或说反了）**：ceiling 打满时 `sanitizePATH`（`Prepare:719`）仍在 `boundedResolveInDirs`（`:735`）**之前**跑，而 `mountHealthy` 的探针**不占** wedge 槽（`TryAcquireSpawnSlot` 只在 `:812`/`:921` 被调）⇒ ceiling 下仍能重探并剔 PATH。但 ceiling 下**不产生任何证据** ⇒ 这一状态**只能靠 TTL 自愈**。这是 A1 的第二条硬理由 |

---

## §1 缺陷与证据（简）

**#81 一句话**：`mountHealthy`（`spawnsafe.go:552-554`）的 `if h.state == stHealthy { return true }` 是终态——无 TTL、无再验证；`:540` 的 self-heal drain 又被 `h.state != stHealthy` 挡在门外，所以状态机只设计了 `dead→healthy`，没有 `healthy→dead`。`applyMounts`（`:381-410`）按 signature **原样继承指针**，而挂死的 NFS 恰恰 umount 不掉、signature 恒定 ⇒ 继承链永不断。

**现网证据（timan107，2026-08-29，已确认，不再质疑）**：18 天长寿命 agent；`exec -- echo ok` → 2.14s `remote_fs_spawn_timeout`（命中 `boundedResolveInDirs` 的 2s 死线）；`exec -- /shared/.../python -V` → **30.18s**（`pathOnDeadMount` 零延迟返回 false ⇒ `/shared` 被判活）；**零** `remote-fs: dropped` 警告 ⇒ `outage=false`，子进程继承中毒 PATH；`--safe` 无效；重启 agent 立刻恢复。

**本轮新查出的、决定方案形状的两件事**：

1. **存在整类零证据故障**（这是否掉"只做证据驱动"的硬理由）：
   - **(a) 显式 argv[0] + 本地 cwd**（`exec -- /bin/bash -c '…'`）：`Prepare` 的 `explicit` 分支（`:715-718`）直接 `abs = name`，**不走** `boundedResolveInDirs` ⇒ 无 E1；`/bin/bash` 的 execve 秒成 ⇒ 无 E2。而 `sanitizePATH`（`:719`）**照样**走了一遍 PATH 并拿到 stale-healthy ⇒ `outage=false` ⇒ 子进程拿 legacy env（`exec.go:325-338`）⇒ **子进程自己**在 D 态卡死。这正是 107 上留下多个 D 态 bash 的形态。agent 侧零超时、零证据。
   - **(b) wedge ceiling 打满**：`boundedResolveInDirs:813` / `RunStartWithCleanup:923` 在 select **之前**早退 ⇒ 之后每一条命令零证据。
2. **原 plan 否掉 TTL 的前提是假命题**（值得回写文档）：`docs/reviews/remote-fs-resilience-plan.md` 记的否决理由是「TTL 每窗口泄一个 D-goroutine」。在 **dead 保持 sticky** 的前提下，一个挂载**一生**最多泄一个（详见 §2 的 D 态线程界证明）。当年就是这条假命题让 TTL 被否掉，才留下 #81。

**#81 自己已经开出的、之前被误读的一条**：gotcha `#81`「钉住它的测试」原文就写着「**先跑一次成功的 exec 把 healthy 灌进缓存**」。「不能在现有 62 里复现」出现在 **#82**、且限定的是 #82 自己的后半段。任何把「62 复现不了 #81」当成新发现写回文档的动作都是污染事故记忆——**明确禁止**。

---

## §2 新的 health 状态机语义

### 2.1 数据

- `mountHealth`（`spawnsafe.go:148-153`）**新增一个字段**：`decidedAt time.Time` —— 最近一次**由探针裁决**写入 `state` 的时刻。`launched/result/done` 语义一字不改。
- `Policy`（`:157-178`）新增 `healthTTL time.Duration`、`now func() time.Time`。
- `Config`（`:181-197`）新增 `HealthTTL time.Duration`（0 ⇒ `DefaultHealthTTL`）、`Now func() time.Time`（nil ⇒ `time.Now`，与既有 `MountSource/Probe/Resolver` 同一种注入缝，且与 `internal/agent/agent.go`、`internal/port/port.go` 的既有时钟缝惯例一致）。
- 新常量 `DefaultHealthTTL = 5 * time.Minute`（选值论证见 §7 R3 与 §8 OQ-2）。
- **不新增状态成员、不新增 `stale` 位、不新增 agent.yaml 键、不动 wire。**

### 2.2 两条不变量（写成 `mountHealth` 上方的注释，它们是全部正确性论证的载体）

- **INV-1** `h.result == nil` ⟺ 该 struct 的探针 goroutine 已确证退出。
  *证明*：`result` 只在 `:543`、`:596` 被置 nil，两处都发生在**已经从 ch 收到值之后**；超时分支 `:610` 刻意保留 result 正因为 goroutine 可能还在 D 里。探针的唯一动作是 `ch <- probe(mp)` 后退出。
- **INV-2** `state == stHealthy` ⇒ `result == nil`。
  *证明*：写 stHealthy 只有 `:545` 与 `:599`，两处都在同一临界区内先置 `result = nil`；超时分支从不写 stHealthy。

> **INV-2 是 A3 的全部内容**：因为它成立，重探守卫写 `state == stHealthy` 就已经蕴含"没有在途探针"，**再加 `result == nil` 是空条款**。注释必须这样写；写成「靠 `result != nil` 挡住 dead」是错的（`:601` 的 launcher 快速 false 分支产出 `stUnhealthy && result == nil`），会诱导下一个人删掉承重的那半。

### 2.3 状态转换表（全部在 `p.mu` 下；`now()` 是注入时钟）

| # | from | 触发 | 守卫 | to | 副作用 |
|---|---|---|---|---|---|
| T1 | (不在 map 中) | `mountHealthy(mp)` | — | stUnprobed | 建 entry |
| T2 | stUnprobed | 同上，`!launched` | — | stUnprobed(launched) | 建 `result`/`done`；起 **1** 个探针 |
| T3 | stUnprobed | 同上，`launched` | — | 不变 | 成为 joiner，等 `done` 或自超时（`:577-587`，m9 语义不变） |
| T4 | stUnprobed(launched) | 探针在时返回 true（`:592-599`） | — | **stHealthy** | `result=nil`；**`decidedAt=now()`**；`close(done)` |
| T5 | stUnprobed(launched) | 探针在时返回 false（`:601`） | — | stUnhealthy | `result=nil`；`decidedAt=now()`；`close(done)` |
| T6 | stUnprobed(launched) | `probeTimeout` 到（`:607-611`） | `state == stUnprobed` | stUnhealthy | **保留 result**（self-heal 见证）；`decidedAt=now()`；`close(done)` |
| T7 | stUnhealthy | drain 到迟到的 true（`:540-548`） | `result != nil` | **stHealthy** | `result=nil`；**`decidedAt=now()`**（不刷新则刚自愈就立刻过期） |
| T8 | stUnhealthy | drain 到迟到的 false | `result != nil` | stUnhealthy | `result=nil`；`decidedAt=now()` |
| T9 | stUnhealthy | 其它任何情况 | — | stUnhealthy | **sticky：永不重探。TTL 与作废都不作用于它。** |
| **T10** | **stHealthy** | **`invalidateHealthy()`**（证据 / `--safe`） | **`state == stHealthy`** | **stUnprobed（全新 struct）** | **`p.health[mp] = &mountHealth{}`**；不起探针（下次 `mountHealthy` 落到 T2） |
| **T11** | **stHealthy** | **`mountHealthy(mp)`** | **`now()-decidedAt >= healthTTL`** | **stUnprobed（全新 struct）** | 同上，随后落 T2 |
| T12 | stHealthy | `mountHealthy`，TTL 未到 | — | stHealthy | 直接 `return true`，**零 syscall**（今天的快路径原样保留） |
| T13 | 任意 | `applyMounts` 判定 signature 变/挂载消失（`:402-404`） | — | entry 被丢弃 | 下次调用回 T1（既有 F4 行为，不动） |

### 2.4 D 态线程预算的完整论证（回答「重探死挂载 = 再泄一个 D 态线程」这条硬约束）

- **瞬时并发界**：起探针的唯一处是 T2，要求 `!launched`（新 struct 必然满足）。T10/T11 只从 `stHealthy` 出发，由 INV-2 ⇒ 作废时**不存在在途探针**。⇒ **任意时刻每挂载点至多 1 个探针 goroutine**——与今天完全一致，重探没有放宽这个界。
- **生命周期界**：作废后的那次重探只有两种结局：(i) 在时返回 ⇒ goroutine 正常退出、零泄漏；(ii) 超时 ⇒ T6 ⇒ stUnhealthy ⇒ **T9 终态、永不重探** ⇒ 恰好泄漏 1 个。要泄第二个，必须先经 T7 回到 healthy，而 T7 成立的前提正是**前一个泄漏的 statfs 已经返回**。⇒ **稳态下每挂载点常驻 ≤1 个被放弃的探针，永远。**
- **界的精确表述（订正三份草案共同的口误）**：是 **每 (mountpoint, mount 代) ≤1**。`applyMounts:402-404` 在该挂载**自己**的 signature 变化时丢弃 entry，旧探针在途时新代会再起一个——既有行为，`TestProbe_resetsWhenMountInstanceChanges` 断言 `count==2` 就是它。本轮不动，但必须如实写进注释，别把它论证成无条件定理。
- **因此不需要**给健康探针加 wedge slot 或独立 ceiling（见 §7 R7 的否决理由）。

### 2.5 换指针（T10/T11）为什么必须是换指针

原地把 `h.launched=false; h.done=make(...); h.state=stUnprobed` 会同时引爆两条**今天就在代码里**的引信：

1. **close of closed / close(nil) ⇒ panic 在 spawn 热路径上。** launcher 在 `:604`/`:613` 关的是**字段** `h.done`，不是 `:573` 捕获的局部 `done`。可达交错：launcher L1 起探针后释放锁 → 另一个调用者的 drain 抢先消费 `ch` 并置 stHealthy/result=nil → L1 的 `<-ch` 永不就绪、走 `time.After` → 若此时发生原地重置，`h.done` 已换成新纪元的 channel，L1 关掉**别人的** done，新 launcher 再关一次 ⇒ panic。
2. **僵尸 launcher 覆写新纪元裁决。** 同一交错里 L1 的 `:609` `if h.state == stUnprobed { h.state = stUnhealthy }` 看到的是**新纪元刚重置出来的 stUnprobed**，于是把一个健康挂载判成 sticky-dead。

换指针后两者结构上消失：每个 `mountHealth` 实例的 `done` 由且仅由它自己的 launcher 建与关，恰好一次；掉队的 L1 只会去改一个已脱钩、无人再读的 struct。
**顺带收紧（必做）**：把 `:604` 与 `:613` 的 `close(h.done)` 改成 `close(done)`（关 `:573` 捕获的局部量）。换指针后二者等价，但用局部量让"恰好一次"成为**局部可验证**的性质。
**已接受的代价**：旧纪元的 joiner 醒来读到旧 struct 的裁决，至多过期一次 spawn。不可避免（让 joiner 重入 `mountHealthy` 会引入重入死锁风险），写进注释。

### 2.6 作废触发器（`invalidateHealthy` 的调用点，恰好 4 处）

| 站点 | 位置 | 类型 |
|---|---|---|
| E1 | `spawnsafe.go:839-840` `boundedResolveInDirs` 的 `case <-time.After(p.probeTimeout)` | 有界解析超时（107 那条 2.14s） |
| E2 | `spawnsafe.go:933-945` `RunStartWithCleanup` 的 `case <-time.After(timeout)` | execve 看门狗（107 那条 30.18s；A6 收敛后 exec/run 共用） |
| E3 | `internal/agent/agent.go:1571` `boundedHomeRead` 的 `case <-time.After(...)` | 网络 Home 有界读超时——**唯一不需要任何人跑命令**就能触发的路径 |
| S | `spawnsafe.go` `Prepare`：`refreshIfChanged()`（`:699`）之后、`ensurePaths()` 之前，`if requestedSafe { p.invalidateHealthy() }` | `--safe` 强制作废 |

**明确不接线（登记在闸门账本里，带理由）**：
- `boundedResolveInDirs:813` / `RunStartWithCleanup:923` 的 **ceiling 早退**——是既往超时的后果不是新证据，且高并发也能耗尽；ceiling 下的自愈由 TTL 承担（§0 A9）。
- `boundedTouch:313`——只在 `ensurePaths` 内跑，那时 health 表几乎全是 stUnprobed，作废是空操作。
- **`mountHealthy` 自身的 `:582`/`:607`**——在那里调 `invalidateHealthy()` 会 **self-deadlock**（`:607` 之后立即 `p.mu.Lock()`，`sync.Mutex` 不可重入）。这一条必须在闸门账本里显式写成"禁止接线"，否则闸门会把人推进死锁。

**E3 的准确说明（勿夸大）**：`boundedHomeRead` 有 `homeReadInFlight` 单飞闩（`agent.go:1539-1545`，注释自述"stays set until the read returns"），所以一次 outage 内它**只触发一次**。一次足够——而且是最早的那次。

---

## §3 逐文件改动清单

### B0 — `internal/spawnsafe/spawnsafe.go`（核心）

| 锚点 | 改动 |
|---|---|
| `const (:69-72)` | 新增 `DefaultHealthTTL = 5 * time.Minute`，注释写死选值论证（§7 R3）：证据驱动覆盖常见路径且一条命令内自愈，TTL 只是**零证据类**的兜底 ⇒ 取长不取短，把误判掷骰次数压低一个数量级 |
| `type mountHealth (:148-153)` | 加 `decidedAt time.Time`；**结构体上方注释扩写成 INV-1 / INV-2 的完整论证**，并点名它们是 T10/T11 守卫的正确性依据、以及"`state == stHealthy` 是承重的那半、`result == nil` 是空条款" |
| `type Policy (:157-178)` | 加 `healthTTL time.Duration`、`now func() time.Time` |
| `type Config (:181-197)` + `New (:199-250)` | 加 `HealthTTL`、`Now`；`New` 里补 `HealthTTL < 0` 的 loud 校验（与既有 `WedgeCeiling`/`SafeDir` 校验同形）并填默认值。**不接 agent.yaml** |
| `mountHealthy (:532-617)` | ① 在 self-heal drain（`:539-551`）之后、`:552` 的 stHealthy 快速返回**之前**插入 T11：`if h.state == stHealthy && p.now().Sub(h.decidedAt) >= p.healthTTL { h = &mountHealth{}; p.health[mp] = h }`（换指针，绝不原地重置）。② 四处裁决写入（`:545`、`:547`、`:599`、`:601`、`:610`）统一补 `h.decidedAt = p.now()`。③ `:604`/`:613` 的 `close(h.done)` → `close(done)`，上方一行注释点名"关字段会在换纪元时误关别人的 channel"。④ 函数头注释重写：sticky-dead 终态 + healthy 可失效（证据 / TTL）+ 每 (挂载, 代) 在途探针恒 ≤1 |
| **新增** `func (p *Policy) invalidateHealthy()` / `func (p *Policy) InvalidateHealthy()`（紧邻 `mountHealthy`） | 持 `p.mu` 遍历 `p.health`，对 `state == stHealthy` 的 entry 执行 `p.health[mp] = &mountHealth{}`；**绝不触碰 stUnprobed / stUnhealthy**，**不在此处起任何探针**（O(#mounts) 纯内存，可安全放在超时分支里）。doc 注释列全 4 个调用点 + 3 个"禁止接线"点及其理由（含 `:607` 的 self-deadlock） |
| `Prepare (:699 之后)` | `if requestedSafe { p.invalidateHealthy() }`，必须在 `:711` 的 `pathOnDeadMount(cwd)` 之前 |
| `boundedResolveInDirs (:839-840)` | 返回 `&FSError{Code: ReasonSpawnTimeout}` **之前**调 `p.invalidateHealthy()` |
| `RunStartWithCleanup (:933-945)` | 在 `onAbandon()` **之前**调 `p.invalidateHealthy()` |
| `applyMounts (:381-410)` | **仅改注释**：在既有 F4 段落后补——(a) healthy 判定的失效**不在这里**，在 `mountHealthy` 的 T10/T11；不要为修 stale-healthy 放宽 signature 继承，那是 F4 的原路返回（无关 churn 丢掉 dead 的单飞状态 ⇒ O(generations) D 态线程）；(b) 继承的是**指针**，`decidedAt` 随之带过来，**绝不得在此刷新**，否则 TTL 被无关 bind/容器 churn 无限续期；(c) 挂死的挂载恰恰 umount 不掉、mountinfo 行最稳定，用挂载表变动当重探触发器与故障发生**反相关** |
| `IsPathDead (:885-891)` | **删除**（全仓零调用点，已核实；Home 守卫走的是 `IsHangablePath`）。**理由只写"零调用点的死 API"**——不得援引 structural budget：`spawnsafe.Policy`（今日 23 个方法，阈值 40）与 `internal/spawnsafe`（1015 行，`pkg-code-lines` 阈值 2000）**都不在** `test/architecture/testdata/structural_budget_golden.txt` 里，用不存在的预算约束论证会给闸门制造假历史 |

### B1 — `internal/agent/exec.go`（看门狗收敛，A6）

- `startBounded (:380-399)` 整体塌缩为 `return a.spawnPolicy.RunStartWithCleanup(start, a.spawnTimeout(), onAbandon, reapOnReturn)`。**签名一字不改**（`remotefs_test.go:263/276` 的注入式测试继续编译并通过）。
- `:370-379` 那段解释 onAbandon/reapOnReturn 语义的注释按「注释是资产，整段搬运」移到 `RunStartWithCleanup` 的 doc（`spawnsafe.go:913-916`）；wrapper 只留一行指路。
- 顺带订正 `spawnsafe.go:954-955` 那句已过期的注释（「review M4: RunStart cannot close a caller's StdoutPipe/StderrPipe」写于 `RunStartWithCleanup` 存在之前，正是它让下一个人以为必须另写一份看门狗）。
- `runChild` 的 `startBounded` 错误分支（`:193-195`）加一条 `Warn`：run 侧已有 `run.go:244` 的 Warn，exec 侧一条都没有——107 上那次 30s 超时在 `agent.log` 里**完全无痕**，是 #81 归因慢的直接原因之一。
- **回退方案**（若主进程判断塌缩会放大爆炸半径）：不塌缩，改为 `exec.go:390` 超时分支直接调 `a.spawnPolicy.InvalidateHealthy()`；代价是闸门账本要允许 3 个铸造点，且"第三处漏接线"的风险回来。见 §8 OQ-1。

### B2 — `internal/agent/agent.go`

- `boundedHomeRead` 的 `case <-time.After(a.spawnPolicy.ProbeTimeout())`（`:1571`）：在既有 Warn 之前加 `a.spawnPolicy.InvalidateHealthy()`。
- **不加** `OnDemote` 回调 / 不给 spawnsafe 引 logger。降级的可观测性由既有的 `Decision.Warn`（`remote-fs: dropped …`，会流到 exec 的 stderr）+ B1 新增的 exec 侧 Warn 承担。理由：spawnsafe 至今零 logger 依赖，为一条日志加一个 Config 回调 + agent 侧闭包是净增面；而 `test/architecture/layering_test.go` 的规则表里**没有任何一条**涉及 `internal/spawnsafe`——两份草案援引的"会撞 layering 门"是不存在的闸门，不能拿它当理由。若主进程认为降级仍需独立日志行，见 §8 OQ-4。

### B3 — `internal/spawnsafe/spawnsafe_test.go`（夹具，纯附加）

- `fakeProbe`（`:87-115`）新增 `setVerdict(mp string, healthy bool)` / `setBlocking(mp string, ch chan bool)` / `waitCount(t, mp, n)` 三个方法（改**当前真值**，不是 per-call 脚本）。`fn`（`:99-115`）本来就每次调用持 `f.mu` 重读 `block`/`ret`，所以是纯附加；既有 25 个测试**零改动**（10 处直写 `ret`/`block` 全在 `New` 之前，合法）。
- 结构体注释加一行硬规矩：`New` 之后只准经 setter 改，直写 map 会被 `-race` 抓到。
- **不用 per-call 序列**：那会把断言绑死在探测**次数**上，而探测次数（single-flight 有没有生效、作废了几次）恰恰是被测对象本身。

---

## §4 测试矩阵（每条含变异验证）

> 全部按被测单元命名，无 `p<N>_*` / `*_round<N>_*`。溯源写成被测项上方一行 `// origin: docs/deploy-tier-gotchas.md #81 (timan107, 2026-08-29)`。

### 4.1 hermetic — `internal/spawnsafe/spawnsafe_test.go`

| # | 测试 | 断言 | 变异验证 |
|---|---|---|---|
| **H1** | `TestPrepare_staleHealthyMountDroppedAfterSpawnTimeout` **（头条，镜像 107）** | `PATH=/shared/bin:/usr/bin`，probe 首次 true。① `Prepare(["echo"])` ⇒ `Outage==false`、`Warn==""`、dropped 空（复刻"一条警告都没有"）。② 注入会阻塞过 `probeTimeout` 的 `Resolver` ⇒ `Prepare` 返回 `ReasonSpawnTimeout`（复刻 2.14s）。**时钟一步不推进**。③ `setBlocking("/shared", 永不发送)`；第三次 `Prepare` ⇒ `Outage==true`、dropped 含 `/shared/bin`、`Warn` 非空、`Env` 的 PATH 已清洗并追加 fallback | 删掉 `:839` 分支的 `invalidateHealthy()` ⇒ ③ 仍 `Outage==false` ⇒ 红（这就是今天的行为）。**时钟不动是关键**：保证测的是证据触发器而不是被 TTL 顺手救回来 |
| **H2** | `TestPrepare_staleHealthyRevalidatesViaBlockingProbe` | 与 H1 同布景，但第二轮探测用 **blocking** probe（`setBlocking`）而非快速 false ⇒ 走 `:607` 的 `time.After` 分支。断言重探后 state 真的翻成 unhealthy、PATH 被剔 | 把 T10/T11 改成"原地重置 `h.launched/h.done/h.state`"⇒ `:609` 的 `if h.state == stUnprobed` 在掉队 launcher 里把新纪元判死 / 或 close-of-closed panic ⇒ 红。**这条补的是四份草案共同的盲区：现有 25 个用例全是同步返回的 fake，而 #81 的现网路径 100% 是 statfs D-hang** |
| **H3** | `TestMountHealthy_healthyVerdictExpiresAfterTTL` | 注入时钟。t0 → `true`、`count==1`；推进 `TTL-1ns`，连调 50 次 ⇒ 仍 true、`count` 仍 1（**零 syscall 快路径未破**）；推进过 TTL ⇒ `count==2`，`setVerdict(false)` 后返回 false | ① 删 T11 ⇒ `count` 恒 1、永远 true ⇒ 红。② 反向：把守卫写成 `>= 0`（每次都重探）⇒ TTL 内那 50 次把 `count` 抬到 51 ⇒ 红（这一半守的是零开销） |
| **H4** | `TestMountHealthy_deadVerdictStaysStickyThroughTTLAndInvalidation` | probe 立即 false ⇒ stUnhealthy。随后推进 `10×TTL`、调 `InvalidateHealthy()` 三次、hammer `mountHealthy` 100 次。断言 `count("/dead")` **恒为 1**、始终 false。**`probeTimeout` 必须设成微秒级**，否则放宽守卫的变异会先撞测试超时而不是先在 count 上变红 | 把 `invalidateHealthy` 的守卫从 `state == stHealthy` 放宽成"所有 entry"（或去掉 T11 的 `state == stHealthy`）⇒ 死挂载被重探、`count>1` ⇒ 红。**这是 dead-sticky 硬约束的机械形式** |
| **H5** | `TestMountHealthy_reprobeNeverDoublesInFlightProbes` **（D 态线程界的唯一真守卫）** | 布景必须是 **slow-but-healthy 的振荡**，不是 dead：probe 阻塞到 `3×probeTimeout` 后返回 **true**。① 首探超时 ⇒ stUnhealthy + result 保留。② 迟到 true 被 drain ⇒ T7 回 stHealthy。③ 此时 `InvalidateHealthy()` + 并发 200 次 `mountHealthy`。断言任意时刻在途探针 ≤1（`count` 增量恰为 1，不是 200），cleanup 后 `assertGoroutinesReturn` 回基线 | 在作废时顺手重置发射闸（`h.launched = false` 的原地重置版本）⇒ `count` 涨到 ~201、goroutine 线性增长 ⇒ 红。**注意**：三份草案把这条守卫放在 dead 布景上，那里 `:556` 的 sticky 分支先返回、变异**不可达**，是恒等式——本条把它挪到唯一能红的地方 |
| **H6** | `TestMountHealthy_reArmSurvivesConcurrentLauncherWakeup` `-race -count=200` | 构造 launcher/drain/re-arm 三方交错：`probeTimeout=1ms`，探针值在 launcher 超时**之后**才被另一个调用者 drain（launcher 醒来时 state 已 stHealthy 而 ch 已空）；同时另一 goroutine 反复 `InvalidateHealthy()`。断言无 panic、无 race、每次调用在 `2×probeTimeout` 内返回、健康挂载没被掉队 launcher 判死 | ① 原地重置 ⇒ close-of-closed panic ⇒ 红。② 只把 `close(h.done)` 改回、保留原地重置 ⇒ 掉队 launcher 的 `:609` 把新纪元 stUnprobed 覆写成 sticky stUnhealthy ⇒ "健康挂载没被判死"变红。历史先例：F6 那条同类竞态在 `-count=1000` 下才复现，故必须带 count |
| **H7** | `TestPrepare_safeForcesRevalidation` | 同布景、**不制造任何 spawn timeout、时钟不推进**。`Prepare(safe=false)` 先缓存 healthy；再 `Prepare(safe=false)` ⇒ **仍** `Outage==false` 且 `count==1`（反证没有偷偷加"每次重探"）；`Prepare(safe=true)` ⇒ `count==2`、`Outage==true`。另断言 `--safe` 不重探已 stUnhealthy 的挂载 | 删掉 `Prepare` 的 `if requestedSafe { … }` ⇒ `--safe` 与不加同构、`Outage` 仍 false ⇒ 红（钉 usage §7.7 的产品承诺 + 107 实测"--safe 同样无效"那一行） |
| **H8** | `TestPrepare_explicitArgv0WorkloadHealsOnlyViaTTL` **（A1 的支点，四份草案都只有散文）** | `PATH=/shared/bin:/usr/bin`、`argv=["/bin/echo"]`（explicit、本地）、cwd 本地。① 灌 healthy。② `setVerdict("/shared", false)`（挂载死了）。③ 连跑 20 次 `Prepare` ⇒ 每次 `Outage==false`、`Resolver` **零次**被调（断言 `boundedResolveInDirs` 根本没走）、`count` 恒 1 ⇒ **证明这一类零证据**。④ 推进过 TTL，再 `Prepare` ⇒ `Outage==true`、`d.Env` 的 PATH 已剔除 `/shared/bin` | 删掉 T11（TTL）⇒ ④ 仍 `Outage==false` ⇒ 红。**这条把"为什么证据驱动不够"从散文变成断言**；没有它，A1 的裁决没有守卫 |
| **H9** | `TestPrepare_wedgeCeilingSaturatedStillDropsDeadPathDirs` | `WedgeCeiling=1`，先占满槽（一个被放弃的 start）。此后 `Prepare` ⇒ `boundedResolveInDirs` 在 `:813` 早退回 `ReasonTooManyWedged`，**但** `sanitizePATH`（`:719`，在 `:735` 之前）仍应剔掉死目录。断言：ceiling 下 ① 零证据产生（`invalidateHealthy` 未被调用）② TTL 到期后仍能重探并 `dropped` 非空 | 把 TTL 删掉 ⇒ ② 永远拿不到重探 ⇒ 红。**这条钉住 §0 A9 的裁决**：ceiling 下唯一的自愈路径是 TTL |
| **H10** | `TestApplyMounts_carryOverPreservesHealthyFreshness` | 灌 healthy 于 t0；用不断变号的**无关** bind mount 制造 12 代 mountinfo churn（目标挂载 signature 恒定 = F4 场景 = #81 机理③）；推进过 TTL ⇒ 被继承的 entry 仍判过期并被重探 | 在 `applyMounts:402-404` 的继承分支写 `h.decidedAt = p.now()` ⇒ 挂载在 churn 下永不重探 ⇒ 红。同时确认既有 `TestProbe_survivesUnrelatedMountChurn` / `TestProbe_resetsWhenMountInstanceChanges` 仍绿 |
| **H11** | `TestPrepare_slowMountFalseDemotionSelfHealsWithinOneCommand` | probe 阻塞 `3×probeTimeout` 后返回 true（健康但慢）。断言：作废后第一次判 dead（`Outage=true`、PATH 被剔）、探针发射数恒 1、迟到 true 可 drain 后下一次 `Prepare` 恢复 `Outage==false` 且 PATH 完整、`decidedAt` 已刷新为当前时刻 | 删掉 T7 的 `h.decidedAt = p.now()` ⇒ 刚自愈就立刻过期、下一次调用又重探 ⇒ 探针计数超标 ⇒ 红（三份草案都写了这条语义、无一条测它）。第二个变异：把 self-heal drain 的 `h.state != stHealthy` 条件去掉/改错 ⇒ 迟到 true 被门在外、挂载永久 dead ⇒ 红 |
| **H12** | `TestPrepare_cwdOnStaleHealthyMountFailsFastOnceInvalidated`（**#82 前半**） | healthy 缓存就位 → 触发一次证据作废 → `Prepare(argv=["true"], cwd="/shared/nas")` 必须返回 `*FSError{Code: ReasonUnsafeCwd, Detail:"/shared/nas"}`，且**不**进入 argv[0] 解析（`Resolver` 计数断言零调用） | 回退 T10 ⇒ `pathOnDeadMount(cwd)` 拿到 stale-healthy ⇒ `Prepare` 返回 nil error ⇒ 红 |
| **H13** | `TestPrepare_localMachineZeroSyscallPerSpawn`（**既有**，`:31`） | 一字不改必须绿。**并新增一条真正的稳态守卫**（既有那条只数 mountinfo 读次数、`markHealthyStale` 不读 mountinfo，所以它抓不到本轮的回归）：`TestPrepare_healthyHangableMountZeroProbesWithinTTL` —— 有 hangable 且健康、时钟冻结，连跑 50 次 `Prepare` ⇒ probe `count` 恰为 1；推进一个 TTL 后第 51 次 ⇒ 恰为 2 | 把 T11 或 `invalidateHealthy` 误放到 `Prepare` 的 `bootHangable` 短路（`:693-698`）之前 ⇒ 既有 H13 红；把失效条件写成恒真 ⇒ 新那条 `count` 变 50 ⇒ 红 |

### 4.2 并发 / 泄漏门 — `test/concurrency/spawnsafe_stress_test.go`

| # | 测试 | 断言 | 变异验证 |
|---|---|---|---|
| **C1** | 扩展**既有** `TestSpawnsafePolicy_concurrentGenSwap`（不新开函数） | 在既有「12 worker × Prepare + 挂载表 gen 翻转」上加两维：一个 goroutine 随机步长推进注入时钟越过 TTL，另一个周期性 `InvalidateHealthy()`；并让其中一个挂载处于 H5 的 slow-but-healthy 振荡态（≥5 轮，满足 N≥5）。断言无 race、无 panic、`assertNoGoroutineLeak` 回基线、`WedgedCount` 归零 | H5/H6 的两个变异在 `-race` 下分别以 close-of-closed panic 与 goroutine 基线抬升变红。**注入的假时钟必须并发安全**（原子或带锁），否则 `-race` 会红在一个与被测不变量无关的地方 |

> **归位说明（必须照办）**：扩展既有函数可避开 `test/determinism/leak_assert_shape_test.go:143` 的 `leakExerciseAnchors` 新增条目（anchor 仍是 `"Prepare"`）。若最终另开函数，**必须**同步加账本条目，否则 `make test` 直接变红。
> **不要**把泄漏证明只放进 `internal/spawnsafe` 的 `assertGoroutinesReturn`（`spawnsafe_test.go:117-131`）：它容差是 `before+1`、不受 N≥5 门管辖，每轮泄 1 个可以永绿。

### 4.3 架构闸门 — `test/architecture/spawn_stall_evidence_test.go`（新文件，按不变量命名）

`TestSpawnTimeoutMintSitesNoteEvidence`：go/ast 扫 `internal/` + `cmd/` 非测试文件，**枚举 `ReasonSpawnTimeout` / `ErrSpawnTimeout` / `ReasonTooManyWedged` / `ErrTooManyWedged` 的「铸造点」**（`&FSError{Code: …}` 复合字面量、以及 `return <ident>` 且 ident 解析到那两个 sentinel var），**排除**声明块（`:654-666`）、`errors.Is` 比较、`switch` 分类（`exec.go` 的 `remoteFSFailReason`、`run.go`）与字符串字面量（`cmd/tether/error_hints.go`）。

断言两条，对着一份**精确计数**的账本：

| 铸造点 | 位置 | 是否接线 | 理由（写在账本里） |
|---|---|---|---|
| `ReasonSpawnTimeout` | `spawnsafe.go:840` `boundedResolveInDirs` | ✅ | E1 |
| `ErrSpawnTimeout` | `spawnsafe.go:945` `RunStartWithCleanup` | ✅ | E2（A6 收敛后是全进程唯一的 execve 看门狗） |
| `ReasonTooManyWedged` | `spawnsafe.go:814` | ❌ | ceiling 是既往超时的后果，非新证据；ceiling 下自愈由 TTL 承担 |
| `ErrTooManyWedged` | `spawnsafe.go:924` | ❌ | 同上 |

- 子句①：铸造点集合必须**逐字**等于账本（总数 4）。多一处、少一处、挪一个函数都变红——与 `tls_verify_pairing_test.go` 的钉死站点数同形。
- 子句②：`wired == true` 的每一处，`invalidateHealthy` 的调用必须出现在**同一个 `case <-time.After(...)` 的 CommClause 内**（不是"函数体内某处"）——这直接堵住"把调用挪到成功分支、门照样绿"的批评。
- 子句③（禁止接线）：`mountHealthy`（`:532-617`）函数体内**不得**出现 `invalidateHealthy` 调用（`:607` 之后立即 `p.mu.Lock()`，非重入 ⇒ self-deadlock）。

**为什么谓词是 sentinel 而不是 `time.After`**：实测 `internal/agent` 非测试文件有 **10** 处 `time.After(`（8 处是退避/心跳/拆链），`internal/spawnsafe` 有 **5** 处（`:313`/`:582`/`:607` 都必须不接线）。以 `time.After` 为谓词的门要么是恒等式，要么在任何人加一个无关退避定时器时变红（哭狼）。

**文件头必须写 KNOWN BLIND SPOTS**（照 `dataplane_lifetime_test.go` 的诚实段落体例）：`FSError` 与 `ReasonSpawnTimeout` 都是**导出**的，包外任何人都能构造 `&spawnsafe.FSError{Code: spawnsafe.ReasonSpawnTimeout}` 绕过子句①的"只许在 spawnsafe 内 return sentinel"直觉——所以账本按**铸造点**枚举而不是按包边界，且 import 别名仍可绕过。承重守卫是 H1/H2 那组行为测试。

**变异验证**：① 删掉 `:840` 的调用 ⇒ 子句① 配对失败 ⇒ 红。② 把 `:945` 的调用挪到 `case err := <-done:` ⇒ 子句② 红。③ 在 `internal/agent` 新造一个返回 `ErrSpawnTimeout` 的函数（哪怕本身合规）⇒ 计数 5 ⇒ 红。④ 在 `mountHealthy:607` 后加一行调用 ⇒ 子句③ 红（不加这条门，闸门本身会把人推进死锁）。⑤ 把 `invalidateHealthy` 改名而不改门 ⇒ 存在性断言 `t.Fatal`（门会**大声**瞎掉而不是静默瞎掉）。

### 4.4 deploy-tier drill — `test/simcluster/drills/62-remote-fs-safe.sh` 新增 **Arm 1S**

> 插在 Arm1 的 `_heal`（现 `:96`）之后、Arm3 setup（现 `:99`）之前。**必须是独立一臂**：Arm1a 今天测的是"从未探过"态（`Arm1 control` 跑的是 `exec agt1 -- true`，裸 argv[0]，而 agent PATH 不含 `$MNT` ⇒ `sanitizePATH` 从不咨询 `/mnt/hung` 的 health——**这就是现有 drill 结构上够不到 #81 的确切原因**）。先灌 healthy 会把 Arm1a 悄悄变成另一个测试。

灌缓存用**显式 argv[0]**（`$MNT/probe`），**不用 `--cwd`**（§0 A8）：它只做一次 statfs（健康 ⇒ 快）+ 一次对不存在路径的 execve（立即 ENOENT），**零 chdir、零无界暴露**。

```
# 新增一个长外部死线的包装（30s agent 看门狗 > 现有 RFS 的 timeout 25）
RFS40() { timeout 45 "$SIM" ctl -- exec "$@"; }

# 1S-0 prime：挂载健康时用显式 argv[0] 让 pathOnDeadMount 探测 $MNT 并缓存 healthy。
#        期望失败（ENOENT），但 health 判定已进缓存 —— 这一步是 Arm1S 与 Arm1 的唯一结构差别。
"$SIM" ctl -- exec agt1 -- "$MNT/probe" >/dev/null 2>&1 || true
assert_ok "1S prime discriminator: statfs still healthy after priming" _statfs_healthy

# 1S-1 事后才挂死（从未被测过的那个转换）
assert_ok "1S inject: SIGSTOP hangfs → 挂载在【已被判 healthy 之后】才死"  _wedge
assert_ok "1S discriminator: 仍是 T/S 态可回收近似，不是真 D"             _fuse_stopped
assert_ok "1S discriminator: statfs 现在阻塞"                            _statfs_blocks

# 1S-2 控制项：stale-healthy 下第一条命令必然付 30s execve 看门狗（这是已知代价，不是 bug）
assert_refuses "1S control: stale-healthy 下首条 abs-argv0 以 spawn_timeout 失败（它同时是自愈证据）" \
    "remote_fs_spawn_timeout" RFS40 agt1 -- "$MNT/probe"

# 1S-3 ★载荷：第二条必须 2s 内快速失败为 remote_fs_unhealthy（修前恒为 30s spawn_timeout）
assert_refuses "1S ★证据驱动重探: 第二条 abs-argv0 快速失败 remote_fs_unhealthy" \
    "remote_fs_unhealthy" RFS agt1 -- "$MNT/probe"

# 1S-4 ★--safe 逃生口：heal → 重灌 healthy → 再 wedge → --safe 必须【第一条】就快速失败
assert_ok "1S heal"     _heal
"$SIM" ctl -- exec agt1 -- "$MNT/probe" >/dev/null 2>&1 || true   # re-prime
assert_ok "1S re-wedge" _wedge
assert_refuses "1S ★--safe 作废缓存判定: 首条 --safe 即 remote_fs_unhealthy（修前恒 spawn_timeout）" \
    "remote_fs_unhealthy" RFS --safe agt1 -- "$MNT/probe"

# 1S-5 存活控制 + 收尾
assert_ok "1S alive-control: 普通 exec 仍可用" timeout 25 "$SIM" ctl -- exec agt1 -- true
assert_ok "1S heal（把挂载还给 Arm3）"        _heal
```

- **不断言 TTL**：TTL 靠假时钟在 hermetic 层测（H3/H8/H9）；drill 只测两个与墙钟无关的触发器。这既避免把 drill 绑死在包内常量上，也省掉一次 5 分钟 sleep。
- **变异验证 = 用修复前的二进制跑这一臂**（deploy-tier 唯一诚实的变异形式）：1S-3 与 1S-4 必须红，且红的形态是 `remote_fs_spawn_timeout` 而不是 `remote_fs_unhealthy`。这是一次性人工步骤，须记录 `first_fail_ord`。
- **纪律**：新增断言必须**真跑**该 drill（`hostname -I` 含 `192.168.0.200` ⇒ 直接 `cd test/simcluster && ./local.sh drill 62-remote-fs-safe`，不 ssh、不 `remote.sh`）；`bash -n` 无效（把 harness 函数塞进 `sh -c` / `timeout N <fn>` 是 runtime not-found，静态检查看不见——`test/simcluster/tests/lint-drills.sh:77`/`:84` 的 noshc / timeout-fn 两条规则）。上面的写法全部把 timeout 加在真实二进制（`$SIM`）前，符合规则。
- **1S-2 引入的新暴露（必须如实登记）**：它是本 drill 中**第一次**让 agent 真的对一个 wedge 上的路径做 execve 并被放弃 ⇒ 一个 wedge 槽被占到 `_heal` 为止 + 一个被放弃的 goroutine。1S-5 的 alive-control 是它的护栏。这**不需要**重新裁定 OQ-2（OQ-2 冻结的是**无界** chdir/exec；这里是有界的、30s 看门狗管着的、SIGCONT 可逆的）。若主进程判断仍需重新裁定，见 §8 OQ-5。

### 4.5 本轮**明确不做**的 drill 臂（登记，不静默丢弃）

`outage=true` 全链路（childEnv PATH 重写 / PWD 注入 / cwd→safe_dir / fallback PATH / `dropped` 横幅）在部署层**从未跑过一次**——现有 62 的三条断言全部发生在 `sanitizePATH` **之前**。要覆盖它必须给 `drills/lib/agentyaml.sh` 加一个 `remotefspath:<dir>` token（写 `Environment=PATH=<dir>:…` 到 unit heredoc），因为 `buildExecCmd` 用的是 `os.Getenv("PATH")`（**agent 进程**的 PATH，review F2），不是子进程 env 的。

**本轮不做**，理由：`agent_provision_yaml` 的调用面是 **36 处 / 20 个 drill 文件**（定稿实测：`grep -rn "agent_provision_yaml " test/simcluster/drills/*.sh | wc -l` = **36**，文件数 20；综合稿写 34、三份草案分别写成 12/13 处——枚举的宇宙就是错的，这正是不该顺手改它的理由），且新变量若忘了在函数入口复位（现有 `_apy_root=""` 就是为这件事存在的），会污染**同一 shell 内后续臂**的 fixture。这是一次独立的 fixture 契约变更，该走自己的全调用点扫描 + 回归重跑，不该和状态机修复挤在一批。

⇒ **必须在 gotcha #81 的 FIXED 条目里登记为 `[GAP]`**：「`outage=true` 码路仍无 deploy-tier 覆盖；本次修复会让它第一次在现网点亮，上线按 §7 R1 分级」。见 §8 OQ-6。

---

## §5 闸门与文档改动

### 5.1 闸门

| 动作 | 位置 | 说明 |
|---|---|---|
| 新增 | `test/architecture/spawn_stall_evidence_test.go` | §4.3 |
| **必做的连带** | `CLAUDE.md` §5 闸门清单加一行 | `test/architecture/gate_registry_test.go` 会校验该行反引号路径**真在 `make gates` 里跑**（`./test/architecture/...` 已在 Makefile 的 recipe 里 ⇒ 加行即被覆盖）。漏了这一步，加门本身会让 `make gates` 变红 |
| 不动 | `internal/proto/wire_inventory_test.go` | 零 wire 改动 ⇒ 账本不动、不触发 N-1 四象限、`ProtoVersion` 不变 |
| 不动 | `cmd/tether/command_tree_inventory_test.go` | 零 CLI 表面改动（flag help 串不进 golden） |
| 不动 | `test/architecture/testdata/structural_budget_golden.txt` | `spawnsafe.Policy` 23→24 个方法（+`InvalidateHealthy`，−`IsPathDead`）远低于阈值 40 且不在 golden；`internal/spawnsafe` 1015 行远低于 `pkg-code-lines` 量化档 2000 且不在 golden；`internal/agent` 已在账本（6000，量化 2000），本轮净减行 |
| 不动 | `test/determinism/enum_switch_default_test.go` | 不新增 `st*` 成员 |
| 条件 | `test/determinism/leak_assert_shape_test.go` | 只要 §4.2 扩展的是既有 `TestSpawnsafePolicy_concurrentGenSwap` 就不动；另开函数则**必须**加账本行 |

**提交前硬闸**：`make test` + `make e2e-parallel` + `make lint` 全绿；并发改动另过 `-race` + 内建 NumGoroutine/fd 泄漏门；改了闸门自身 ⇒ 跑一次 `make gates`（以 `vet-tags` 开头、`make lint` 收尾）。⚠ golangci-lint 有全局锁，`make gates` **不得**与另一个 lint 并行；跑硬闸**绝不用 `| tail`** 取结果（退出码会变成 tail 的 0）。

### 5.2 `docs/usage.md` §7.7

1. **「已知边界」新增一条**（这是 #81 点名的那条）：
   > **启动时健康、之后才挂掉的旧挂载**：这才是生产上真正会发生的那个。本版本前**永不重探**（gotcha #81：timan107 实测 18 天长寿命 agent，整套保护静默退化成只剩 2s/30s 两条死线）。现已改为**证据驱动重探 + TTL 兜底**：一次 `remote_fs_spawn_timeout` 本身就是"缓存的健康判定已过期"的证据，agent 立刻作废并在**下一条**命令重探；即便完全没有证据（例如只跑绝对路径 argv[0] 的负载），健康判定也最多 5 分钟过期一次。**代价：命中该挂载的第一条命令仍会付一次 2s 或 30s 的失败**——要跳过这一次，用 `--safe`。
2. **「手动强制」段扩写**：`--safe` 现在除了绕过 `mode: off` 与本地机短路，还**强制作废已缓存的健康判定并立即重探**；它同样不重探已判死的挂载。代价：一次 `--safe` 最坏 `probe_timeout` × 被咨询的健康网络挂载数（**per-call，不是 per-process**——脚本里循环 `--safe` 每条都付）。
3. **`:1489-1490` 那句括注**（「启动后新挂载 … 用显式 `--safe`(每次都重新探测挂载)」）：**保留、不加"本版本前不成立"的自我否定**。它在自己的语境里（一块**新挂**的盘 health 本来就是 unprobed）一直是真的；两份草案要往已发布文档里插入一句错误的自我否定，明确驳回。仅把括注收紧成「（强制重读挂载表**并作废已缓存的健康判定**）」，让它在全局也成立。
4. **「做不到的事」补一句**：第一次失败不是"做不到"，是"正在重新学"——重跑或加 `--safe` 即可区分。
5. `:704` / `:749` 两行 flag 表格描述补「并强制重新探测挂载健康」；`cmd/tether/exec.go`/`run.go` 的 `--safe` help 串同步（契约全局扫，`feedback-contract-change-sweep`）。
6. **误判逃生口**：健康但慢的网络盘若被误判，调大**既有键** `remote_fs.probe_timeout`；并说明误判会随那次迟到的 statfs 返回而自动翻回（T7）。

### 5.3 `cmd/tether/error_hints.go`

`runFailureReasons["remote_fs_spawn_timeout"]` 文案改为点名新契约：「…agent 刚刚作废了缓存的挂载健康判定 —— **原样重跑同一条命令**即会重探并剔除死掉的 `$PATH` 目录；`--safe` 可立即强制重探。」**exit class 不改**（`exitTransient`，其注释「heals when the mount does」在修复后依然成立且更强）。新增一条 hermetic 断言钉住这句话（含 "again"/"retry"）——docs 会腐化，`error_hints` 是用户实际读到的那一句。

### 5.4 `docs/deploy-tier-gotchas.md`

**#81 → FIXED**，正文追记三条订正：
- (a) 原 `remote-fs-resilience-plan.md` 否掉 TTL 所依据的「TTL 每窗口泄一个 D-goroutine」是**假命题**（dead sticky ⇒ 一生一个，证明见 §2.4）。**冻结的 plan 文档本身不改**（`docs/reviews/` 下的报告不追溯改），订正写在这里。
- (b) 单靠 spawn-timeout 证据修不了两类零证据负载（显式 argv[0]；wedge ceiling 打满），必须叠 TTL。
- (c) 登记 `[GAP]`：`outage=true` 码路仍无 deploy-tier 覆盖（§4.5）。
- **不得**写「62 复现不了 #81 是错的」——#81 原文本来就开出了 pre-cache 那一臂（§1 末）。

**#82 → 保持 OPEN**，按 §6 追记。

---

## §6 #82 的范围裁决

### 采纳（零边际实现成本）

**#82 的前半段** —— stale-healthy 下 `exec --cwd <死挂载>` 不返回 `remote_fs_unsafe_cwd`。机理清楚、是 #81 的直接后果、修 #81 后自动消失。**不写一行额外实现代码，只补断言**（H12），否则"修完后本条前一半会自动消失"这句话下一轮又只是散文。

### 不采纳（后半段：agent 转 S 态停止处理消息 + 两个 pre-execve D 态子进程 + 30s 看门狗那次没回）

三条理由，每条可核查：

1. **根因未归因，且现有证据自相矛盾**：同一台机同一批实验里 `exec -- /shared/.../python` **确实**在 30.18s 正常回了 `remote_fs_spawn_timeout`，证明看门狗当时是活的。在没有定位的情况下改看门狗，是拿一个没有 oracle 的改动去换一个没有复现的现象。
2. **本仓规矩不允许**：「每条新增守卫必须有变异验证」——对未定位的缺陷，你注入不了"它声称能抓的那个缺陷"，只能写出恒等式测试（批次 B 真实翻过这个车）。
3. **复现前提被 OQ-2 冻结为 NOT-COVERED**：需要**真 uninterruptible-D**（kernel nfsd + hard mount）与隔离宿主；FUSE 近似是 T/S 态、SIGCONT 可逆。`weilandserver` 仍是共享宿主且跑着实验负载，理由一个字没变。

### 本轮必须交付的、零风险的 #82 产出（写进 gotcha #82，让下一个人不必重走）

- **假说 A「`--cwd` 路径没接上 30s 看门狗」——由代码阅读证伪。** `internal/agent/exec.go:184` 的 `startBounded` 在 `decision.Active` 下包住 `cmd.Start`，与是否设 `cmd.Dir` 无关；而 `cmd.Dir` 在更早的 `buildExecCmd`（`:325`/`:330`）就已设好；`run.go:226` 的 `RunStartWithCleanup` 同理。
- **假说 B「一条卡死的 exec head-of-line block 了订阅的消息循环」——证伪。** `exec.go:57-67` 每个 forwarded verb 都以 `go a.handleXForwarded(...)` 独立派发（该处注释本身就是为 `tether run` + Ctrl-C 写的）。
- **假说 C（本轮新查出，最强的一条，只登记不修）**：`internal/agent/state.go` 的**写**路径无死线且**持锁**——`AddPort(:190)` / `UpdatePortHome(:217)` / `RemovePort(:240)` / `SetProxy(:258)` / `SetRosterCache(:279)` 全都在 `s.mu` 下调用无界的 `saveLocked(:302)`；只有**读**在 `agent.go:1533-1576` 被做成有界 + 单飞。Home 落在死挂载上的一次写会**永久持有 `s.mu`**，此后每个 `load()`/`GetProxy()`/`GetRosterCache()` 全部堵在它后面。这与「原 agent 进程 S 态活着但停止处理消息」比任何其它候选都吻合；`roster.go:282` 的 `SetRosterCache` 提供了一条不需要用户操作就能触发的路径，可解释节点转 OFFLINE。**这是假说不是结论**（timan107 的 Home 是 `/home/zixuans8`，实测**健康**——所以要成立需要该 NFS 后来也出问题，或另有写路径落在 `/shared`；这一点必须在下一个增量里先验证再动手）。`docs/usage.md §7.7` 已把"网络 Home 的写会阻塞该次操作"写成已知边界，但**没写它会持锁毒化后续所有读**——这句该补。
- **假说 D「wedge slot 耗尽」——考虑过，弱。** `TryAcquireSpawnSlot` 失败在 `spawnsafe.go:813` 与 `exec.go:381` 都是**第一行 return**，产生的是**立即返回的错误**（`too_many_wedged_spawns`），不是沉默；解释不了 #82 记录的"没有任何返回"。登记为已考虑并否决，避免下一轮重新推导。
- **两个 pre-execve D 态 fork 子进程**（cmdline 仍是父进程 argv、etime 与那条命令吻合）依然**无解释、无修复**，保持 OPEN，并保留「复现请用隔离宿主 + hangfs，不要在共享/生产机器上跑」的定格。

---

## §7 风险与回滚面

| # | 风险 | 量化 / 缓解 |
|---|---|---|
| **R1** | **最大的一条，方向上真的更坏**：修复会**第一次在现网点亮 `outage=true` 这条码路**。今天现网长寿命 agent 因 #81 几乎永远拿不到 `Outage=true`（107 上"一条 dropped 警告都没有"就是铁证）⇒ `sanitizePATH` 之后的全部行为（childEnv PATH 重写、PWD 注入、cwd 未指定时替换成 `safe_dir=/tmp`、fallback PATH 追加）在生产上**从未真正跑过**，deploy-tier 也未覆盖（§4.5 的 `[GAP]`）。具体可见变化：(a) `tether run <node>`（不带 `--cwd`）在挂载死时把用户丢进 `/tmp`；(b) 子进程 PATH 被换成 kept+fallback | **分级上线**：weilandserver → 单台 timan → 全车队；观察 journald 里 `remote-fs: dropped` 与 B1 新增的 exec 侧 Warn。这条风险**必须写进 commit message 和 #81 的 FIXED 条目** |
| **R2** | **健康但慢的 NFS 被误判死的爆炸半径**：代价不是"fail"而是**静默换工具链**——timan 的 PATH[0..1] 是 `/shared` 上的 conda；误判后裸名解析落到 fallback，`python`/`torchrun` 多半 `remote_fs_not_found`（可见），但 `python3`/`pip3`/`bash`/`nvidia-smi` 会**解析到系统版本并真的跑起来**。对 exec 用户能在 stderr 看到 dropped 横幅；对 `run`，警告只进 `agent.log`（往 PTY 注横幅会毁掉 vim/tmux）⇒ **对 run 完全静默**。一条 12 小时训练任务用错解释器是本方案最坏的产出 | 三重缓解：① 证据为主，TTL 只是兜底且默认 5 min（把掷骰次数从 30s 档的 ~52k/18 天降到 ~5.2k）；② T7 self-heal 把误判窗口钉在**探针延迟 + 一次 spawn**，不是永久（H11 是它的机械形式）；③ 运维逃生口是调大既有键 `remote_fs.probe_timeout`。**残留缺口（本批不修，登记）**：run 的降级对用户仍不可见；要修得给 `RunChunk` 加新 Kind/字段 = wire 变更，须过 N-1 四象限与 append-only 账本，不该和状态机改动挤同一批 |
| **R3** | **TTL 值本身是取舍**：18 天 × 5 min ≈ 5.2k 次掷骰，比今天的 1 次高 3 个数量级 | 常量旁写死论证。若车队上线后真观测到误判，第一动作是**调大 `DefaultHealthTTL`**（比如 30 min），而不是回退整个修复——因为证据驱动才是主路径，TTL 只覆盖零证据类 |
| **R4** | **与挂载无关的 spawn 停滞会误作废**（GPU 打满时 2s 死线可能纯因调度饥饿错过；memory cgroup 回收期的 fork；冷缓存下 execve 一个 300MB conda python） | 误作废本身代价近似为零（O(1) 纯内存、无 I/O），代价要等到下一次真的查询某挂载健康时才发生，而健康挂载的 statfs 是微秒级。**真正的危险是"误作废 ∧ 探测也超时"——而制造误作废的 CPU 饥饿恰好也会让 2s 探针超时，两者正相关不是独立事件。** 唯一护栏是 R2 的 self-heal + 日志。备选旋钮（探针超时与 resolve 超时解耦）见 §8 OQ-3 |
| **R5** | **`--safe` 的成本从零变成 per-call**：每次 `--safe` 作废并重探全部被咨询的健康挂载。脚本里循环 `tether exec --safe` 的用户每条命令都付 N×statfs（健康时快、慢盘最坏 N×`probe_timeout`） | 已写进 usage §7.7 与 flag help。**不做并行化**（会引入 N 个并发探针，直接撞掉每挂载 ≤1 在途探针的界）。H7 断言 `--safe` 不重探已 dead 的挂载 |
| **R6** | **最高风险的单点编辑是 `mountHealthy` 的 joiner/launcher 路径**：m9（joiner 不各等一个 probeTimeout）与 F6（先清标志再发布结果）两条历史外审结论都落在这个函数上 | 换指针让两条引信结构上消失（§2.5），但仍必须 `-race -count≥200`（F6 那条同类竞态当年在 `-count=1000` 下才复现）。H6 是它的守卫 |
| **R7** | 有人日后"顺手"放宽 T10/T11 的 `state == stHealthy` 守卫 ⇒ 重探死挂载 ⇒ **无界** D 态线程（探针**不占** wedge 槽，`wedgeCeiling=64` 管不到；Go 运行时 10000 线程上限 ⇒ agent panic）——**比 #81 本身严重得多** | 唯一防线是 H4 + H5，删不得。闸门只管调用存在、不管守卫内容，这一点必须写在注释里。**明确否决**给探针加 wedge slot / 独立 ceiling：§2.4 已给出完整的界；且共享槽会让 64 个卡住的 spawn 把探针饿死，agent 就永远学不到"那个挂载死了"这个能让它自愈的事实 |
| **R8** | **旧纪元的 joiner 返回一次过期裁决**（至多一次 spawn 用了旧判定） | 不可避免（让 joiner 重入 `mountHealthy` 会引入重入死锁风险）。已接受并写进注释 |
| **R9** | **既有 25 个测试的探针计数会漂移**：任何「反复 `--safe` + 健康挂载」的用例在新语义下会多出重探 | 实现时必须**逐个复核 `internal/spawnsafe` 全部 25 个测试**（不是 32——`grep -c '^func Test'` = 25），不能只看 `make test` 是否绿（绿也可能是断言太松）。`TestProbe_survivesUnrelatedMountChurn` 用的是死挂载、按 sticky 规则不受影响，它反而成了"作废不许碰 unhealthy"的现成守卫 |
| **R10** | **drill 62 的新臂在打补丁前必然红**（1S-3/1S-4） | 补丁与 drill 改动**必须同一次提交**落盘，否则中间任何一次 `./run-drills.sh` 都会给出一个看似 harness 坏了的红。另：`test/architecture/simcluster_log_oracle_test.go` 的铁律——任何读 broker/agent 日志的断言必须经 `drills/lib/logs.sh` 并 source 之（本臂不读日志，故不触发，但若后续加日志断言必须遵守） |
| **R11** | **B1 的看门狗收敛动的是泄漏账**（管道 fd 回收 + 子进程 reap） | `remotefs_test.go:263/276` 覆盖 abandon/reap 计数，必须继续绿；另建议先补一条对**旧实现绿、新实现也绿**的等价测试（`onAbandon` 同步调用、`reapOnReturn` 先于 `ReleaseSpawnSlot`）再做替换 |
| **R12** | **回滚面（好消息，写进 commit message）** | 零 wire、零 `agent.yaml` 新键、零 CLI golden、零磁盘状态 ⇒ 回滚 = **纯二进制回滚**，不会有"写了新键就起不来旧二进制"的砖化，也不触发 N-1 四象限或 `wire_inventory` 账本。新旧 agent 与任何 broker 混跑均无影响。**这正是坚决反对本批引入 `remote_fs.health_ttl` 之类旋钮的原因**：一旦落键，回滚就变成"先改配置再降级"的两步操作，而 timan107 这类节点（无 sudo、无 systemd 自启、靠 NFS `.bashrc` 登录自启）恰恰最难做两步运维 |
| **R13** | **#82 的残留在修复后仍存在**：lexical 快速失败是纯字符串工作，绕不过 cwd 是**符号链接**指向死挂载（`mountForPath` 按字面路径匹配）、fstype 不在保守 denylist 里、以及检查与 fork 之间的 TOCTOU。这些仍会走到 30s 看门狗并留下 pre-execve D 态子进程 | 修复把这类事件的频率从「每条命令一次」降到「每次 healthy→dead 转换一次」（O(命令数) → O(1)），但没有消灭它。**不要在 gotcha 里宣称 #82 已解决** |

---

## §8 未决问题（留给主进程定稿）

| # | 问题 | 备选与倾向 |
|---|---|---|
| **OQ-1** | B1 的 `startBounded` 收敛是否与本轮同批？ | 倾向**同批**：不收敛就有两个铸造点家，第三条 spawn 路径必然第三次忘记接线（CLAUDE.md §3 step 5b 的 tunnel fence 三轮返工同形）。回退方案已在 §3 B1 写明（3 个铸造点账本）。若主进程选回退，§4.3 的账本要相应改成 5 条 |
| **OQ-2** | `DefaultHealthTTL` 取 5 min 还是 30s / 30 min？ | 计划取 **5 min**（§2.1）。论证：证据驱动覆盖常见路径且一条命令内自愈，TTL 只是零证据类（显式 argv[0] 负载、ceiling 饱和）的兜底 ⇒ 取长不取短，把 R2/R3 的掷骰次数压低一个数量级。**这是一个真正的判断题，主进程应自己再权衡一次** |
| **OQ-3** | 要不要给作废加限速（同一 `probeTimeout` 窗口内最多作废一次）？要不要把探针超时与 resolve 超时解耦（探针给 `probeTimeout×2`）？ | 计划**都不做**。限速的失败模式（抑制一次真实作废）比它省下的成本（重试风暴期每挂载每次 spawn 一次快 statfs）更坏，且 `--safe` 必须绕过它；解耦会把死挂载的首次判定时间翻倍（现网实测 2.14s → 4s+），而误判已被 T7 钉在一条命令内。两个旋钮都记在这里，供主进程再权衡 |
| **OQ-4** | 是否需要一条独立的"健康判定被作废/降级"日志行（`Config.OnDemote` 回调）？ | 计划**不做**（§3 B2）。#81 在现网无声活了 18 天，理由不是缺一条降级日志，而是 `outage=false` 导致连既有的 `dropped` 横幅都没发出——修复后那条横幅本身就会出现。若主进程认为仍需（例如为了在**没有** outage 的作废上也留痕），加一个 nil-safe 的 `Config.OnDemote` 是 3 行改动、无分层障碍（`layering_test.go` 的规则表里没有 `internal/spawnsafe`） |
| **OQ-5** | drill Arm 1S-2 让 agent 真的对 wedge 上的路径做一次被放弃的 execve —— 需要重新裁定 OQ-2 的 NOT-COVERED 吗？ | 计划**不需要**（OQ-2 冻结的是**无界** chdir/exec；这里是 30s 看门狗管着的有界暴露 + SIGCONT 可逆的 FUSE + 1S-5 的 alive-control）。但这条边界值得主进程明确签字，因为它是本轮唯一新增的宿主暴露 |
| **OQ-6** | `outage=true` 全链路的 deploy-tier 覆盖（§4.5）作为紧随其后的独立叶子增量，还是并入本批？ | 计划**独立增量**（`agent_provision_yaml` 有 **36** 个调用点 / 20 个文件的契约面，须自带全调用点扫描 + 回归重跑）。本批以 `[GAP]` 登记。**定稿裁决见 §-1 OQ-6：独立增量，不并入本批。** |
| **OQ-7** | `docs/usage.md §7.7` 是否补一句「网络 Home 的**写**会持 `s.mu` 并毒化后续所有读」（§6 假说 C）？ | 计划**补**（是一条已核实的源码事实，与是否修 #82 无关）。但若主进程认为它会被读成"#82 已归因"，则移到 gotcha #82 里、不进 usage |