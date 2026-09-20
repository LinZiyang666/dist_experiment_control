# simcluster 全套 drill 压到"几分钟"的可行性研究

> 研究报告，非 plan。基于五份草案（L1 产品时间常量 / L2 夹具供给 / L3 拆分与调度 / L4 oracle 形态 / L5 基础替代）与四份批评（C1 保真度 / C2 正确性与洗白 / C3 可行性与成本 / C4 证据严谨性）综合，并对决定地板的十余处引用在工作树 `a3431a1` 上重新核对（标 `[核]`）。数字来源标注：`实测`（用户 2026-09-18 给定）/ `file:line` / `推导` / `估计` / `Lx`·`Cx`（来自草案或批评、本轮未独立复核）。

---

## 1. 一句话回答与三张地板表

**一句话：在"每个 drill 的 claim 不变、不出假绿假红、不弱化断言"的前提下，全套 ≤5 min wall 做不到；诚实的地板是主 sweep ≈7–10 min（估计，需要 ≥3 个产品增量 + 修掉 #33/#34/#70 + 重写半个 harness），而今天 HEAD 上的真实 wall 不是 20.7 min 而是 45 min——drill 67 自 2026-08-19 起在每次全量 sweep 里撞 `DRILL_TIMEOUT=2700` INFRA-ABORT，在分诊它之前任何加速手段都改不了 wall。**

### 1.1 表 A · 严格现行 Mandate（Docker+systemd、一 drill 一集群、真时间、零产品改动、不动 drill 结构）

| 量 | 值 | 来源 / 推导 |
|---|---|---|
| wall（主 sweep，HEAD 今天） | **45 min** | `run-drills.sh:97` `DRILL_TIMEOUT=2700`；`docs/reviews/prerelease-audit-external-review.md:1018,1034` 与 `test/simcluster/expected-verdicts-log.md:425` 记 67 为 INFRA-ABORT rc=124（HEAD 基线同）`[核]` |
| wall（67 分诊后） | 20.7–22.3 min | `-j12` 实测 20.7（`simcluster-accel-dispositions.md`，C4 复核）；`drill-costs.tsv` 96=1337 s |
| wall（含 M4 串行归因） | 40–63 min | 修正树 `-j6` 含归因实测 62.6 min（dispositions，C4 复核） |
| sum | ≈216 min（40 行）→ ≈225 min（43 drill，估计） | `drill-costs.tsv` 40 行合计 12944 s `[核]`；78/83/84 未登记（78 含 255 s 纯 sleep，`78:89-91,141`，L4/C4） |
| 最长单元 | 96-mid-flight-chaos（1337 s，2026-07 种子） | 在等：N=3 spine 305 s（实测）+ **F 臂前置门 `poll_until 360`（`96:711`，账本记 in-sim >360 s，每次付满；L3/C3）** + D3 JS meta 重组 ≤300（`96:569`）+ A 臂 0 或 390 s（`96:396`，条件性，见 §1.4-R2）+ canary 0–60 + brk2 重启/reap ≤210 |
| 次长 | 67（777，来源不明）/ 90（775）/ 98（700）/ 74（653） | `drill-costs.tsv`；98 的 4–6 min 是 nats.go ping 检测（`98:31-34`）；74 的 180/240 s 只在 #34 显形时付满（`expected-verdicts.tsv` 74 行 band 在 `c-ss-preflow`，C4） |

⚠ 表 A 的"今日 p50"是 **2026-07-23 种子**（pre-lever、-j6 负载），且与 2026-09-18 实测的单次 grow 176/114 s 不相容（`drill-costs.tsv` 11-grow-gaps=152 s 含一次 grow brk2，`[核]`）——要么种子期 grow 快得多、要么 grow 路径此后回归（C4-X2）。**所有"节省"数字在一次 instrumented sweep 之前都是两个纪元的混算。**

### 1.2 表 B · "tether 真跑起来"宽松前提（sim 全可重构：模板夹具 + 拆臂 + lane 调度；允许修产品**驱动器缺陷**；产品常量不动；claim 不变）

| 量 | 值（估计） | 推导 |
|---|---|---|
| wall（主 sweep） | **≈11–13 min** | = max(P_max ≈ 10–12, W_grow ≈ 8–13, Σ/j ≈ 8–9 @ j=16)，三者同量级 |
| sum | ≈130–150 min | 225 − 47（grace，47 次 grow × 60 s，`cluster_add_drive.go:692`）− 26–35（模板，消费者名单按 C1 修正后仅 8–10 个 drill）− 6（96-F 前置门）+ 拆臂多付的夹具 |
| 最长单元及其在等什么 | **74-C（live，6–12 min）**：spine′ 190 + eligibility 0–240（产品观测，README:417）+ settle + C-auto 0–180（#34 open）；**41（live，7.7–11.7 min）**：spine′ + 2×retire + JS reset + S-survival ≤300（3×`RosterRefreshInterval`，`41:277-282`）；**98（模板，6.8–8.8 min）**：nats.go 检测 (4,6] min + 梯子 40 + heal ≤60；**91-A（live，≈7.6 min）**：两次 grow 是 claim 本身 | 全部为估计；spine′ = 305 − 120（grace）+ 0（[env] 重叠 −10）≈ 185–195 s |
| 第二 wall 设定者 | **grow lane**：≈30–37 次 live grow/sweep（模板只替掉 10–16 次），#70 在 5 个并发 grow 时打穿 VOTER 150 s 窗（`simcluster:363-372`，README），经验上限 g≈5–6 → W_grow ≈ (20 个含 grow 单元 × ≈3.2 min)/g ≈ 8–13 min | L3 模型 + C1 的模板消费者修正；g>6 未测 |

### 1.3 表 C · 激进（B + 有独立产品价值的常量/行为变更 + 修掉开放缺陷；仍不弱化任何 claim）

| 量 | 值（估计） | 推导 |
|---|---|---|
| wall（主 sweep） | **≈7–10 min** | 98 降到 ≈3–4 min（ping 两侧 20 s）；74/73 的 180/240 s 红路径随 #34/#33 修复消失；#70 修掉后 grow lane 不再是约束；剩 41（7.7–11.7 live / 5.5–9.5 模板）、51（≈7–8）、91-A（≈7.6）、97（≈5.5–8）、30（≈6.5） |
| sum | ≈90–110 min | B 再 −98 的 4 min、−74/73 的 7 min、−41 部分 |
| 最长单元及其在等什么 | **41**：retire 链（N=3→2→1 不可拆）+ `RosterRefreshInterval` 3 min 的 full-jitter 逃逸（不可压——压了会洗掉 `41:205-218` 记录的 proactive 唤醒缺口，见 §2）；**51 / 91-A / 30**：真 grow 链（raft AddNonvoter/catch-up/AddVoter + nats JS meta 1→2→3 形成，第三方状态机，去掉 grace 后每次 grow 仍 ≈2 min）；**97**：6 cycle 是 leak 斜率的结构地板（`97:358`，`LEAK_MIN_N=6`） | 估计 |
| 附加成本 | 含 M4 归因：+≥1 个最长单元 | 归因按单元 solo 重跑（§4.4） |

### 1.4 三张表依赖的分歧裁定（草案/批评冲突，逐条给裁定）

| # | 分歧 | 裁定 | 依据 |
|---|---|---|---|
| R1 | 今日 wall 20.7 min（五份草案）vs 45 min（C3） | **C3 对**。67 是 HEAD 上的 INFRA-ABORT，未分诊 | `[核]` audit review :1034、verdicts-log :425 |
| R2 | 96 的 390 s 是健康路径必付（L1/L2/L5）还是条件性（L3/L4/C2/C4） | **条件性**：`poll_until 390 … \|\| true` 首采样真即返回；四分支里只有①（audit 不可读，home 死）和③（只有 start 行）付满；登记世界（`expected-verdicts.tsv` 96 行"in-sim 1 GiB interruption not constructable"）对应分支②，0 s。**今天走哪个分支未定**（88 vCPU 空闲时 1 GiB 在 kill 前传完；-j6 负载下未知）| `[核]` `96:114-117,396-423` |
| R3 | 96-A 的 R16 年龄门是 6 min（L4/C2/C4）还是 19 min（C1 F0-2 担心 `Size`=1 GiB） | **6 min**。pull 路径的 tracker entry 不带 size（`transfer.go:1349-1355` 无 `size:` 字段）→ ledger `Size=0` → `XferBudget` 取 floor 5 min（`xfer.go:104-106,120-123`）+ `xferStrandedSlack` 60 s（`xfer_inflight.go:723`） | `[核]` |
| R4 | 98 只改客户端 `PingInterval` 即可（L3/L4/L5）vs 必须两侧（L1/C1/C2/C3/C4） | **必须两侧**。IMPACT 谓词是被切 broker 的 `/connz` 消失（`98:66-72`），DROP 双向对称（`fault.sh:85-92`），服务端踢连接由 nats-server `ping_interval/ping_max` 决定；这两键**不在** natsconf passthrough 表（`preflight.go:42-56`，未知键 fail-closed）。只改客户端会让 RECOVERY 断言在 IMPACT 之前就恒真（C2 L1-2） | `[核]` |
| R5 | `joinerBootGrace` 跳过条件：L1"本次调用跑了 P2 init" / L3"走了 render 分支" / L5"`opID` 为空或 socket ECONNREFUSED" / L2 不改产品 | **L1 的条件**。render 分支 `!joinerBrokerUpLocal` 在每次调用都重新求值（`:177`），drill 42 的 invocation 2 里 joiner 正在 crash-restart 时也会取到 → L3/L5 变体会把 grace 注释里 3/4 失败的场景修回去（`:697-704`）；只有"本次调用执行了 `runSelfInit`"（`:118-124`，仅 raft/ 不存在时为真）与 #I1 不变量一起证明此前不可能有 cluster-mode broker 在跑 | `[核]` |
| R6 | #31 的绑定常量是 `LockLeaseTTL` 15 min（L1/L2/L5）还是 `GrowLockReapInterval` 30 s（C1） | **C1 对**：`growLockDecision` 在 lease 未过期时仍走"(a) joiner 有 terminal op ⇒ clear"（`reconcile_grow_lock.go:99-125`），marker 最多活 30 s；且 #31 台账 2026-08-11 已记"产品行为已对"（`gotchas.md:95-100`）。**所以没有任何 drill 今天能看见 P9-release 单点回归；30/40/41 保持 live 的理由是"回归采样率"（每 sweep 多少次真 grow）而非"TTL 内鉴别力"** | `[核]` |
| R7 | 97 的 cycle 等 `OfflineAfter` 60 s（L3）还是 STALE 5 s（L1/C4） | **STALE**：`_agt1_offline() { ! _agt_online agt1; }`（`97:123`）。L3 的"97 ≈ 8–10 min、P_max 由 97 设定"下修为 5.5–8 min | `[核]` |
| R8 | 73/74 可用模板集群（L2/L3/L5）vs 必须 live（C1 BLOCKER） | **live**。post-grow proxy-eligibility 恢复窗（≤240 s，典型 60–90 s，`74:290`/`73:222`）是 README:417-421 登记的候选产品观测缺陷，模板让它永不可见；74 的 #34 band 是在 live grow 上校准的，模板化后 74 变绿 ≠ #34 修了。同理 20/92/12（`setup_forcesingle_n2` 的"grow 后首推"是 kept site，#67 residual 登记在 -j6 下出现）、95（首次 post-grow 重启的 G.2）都 live | C1 §2 delta 表；C2 L2-2(iii) |
| R9 | 拆臂后"计数器求和 = 串行"（L3）| **只在 verdict 枚举层等价**：串行时后臂跑在前臂改变过的集群上（96-F 在 heal 后、73-Q 在 rehome 后、33-A 在回滚后），并行拆臂后在新鲜集群上跑；且 abort 语义变化让 enum **上升**（A SETUP-RED + D ASSERT-FAIL → ASSERT-FAIL），expected 表要逐条改并写理由 | C1 §3、C2 L3-7 |
| R10 | drill 78 测的退避常量：L1 `agent.go:1939-1941`（500 ms/30 s/2 min）vs L4 `proxy.go:701-702`（5 s→5 min） | **L4 对**（前者是 `applyOneHome` 的 expose-home 退避）。(d) 结论不变 | C2 §0、C4 |
| R11 | 阻塞 sleep 总量 175 s（brief/L1）vs ≈433 s（L4/C4） | **433 s / 40 站点**：78（2026-08-12）晚于 accel plan（2026-07-24）| C4-X5 |
| R12 | grow 次数 47（L1/C3/C4）/ 41（L5）/ 13 或 9（L2/L3 模板后） | **47**（静态站点，含 90 的 5 次、51 的 3 次、22 的 2 次、42-F）；模板化后按 §1.2 名单剩 ≈30–37 | C4 逐 drill 复核 |
| R13 | `cutoverBroker` 重试环上限 18 s（L1）| **≈78 s**：每轮 `sendGrowTrigger` 带 `growTriggerTimeout` 10 s（`cluster_add.go:29,224`）+ `growConvergePoll` 3 s，6 轮；nats 被 SIGKILL 期间请求一般等满 10 s。这也是 invocation 2 的 67 s 之谜的候选解释之一（6 轮可跨两次调用）| `[核]` `cluster_add_drive.go:535-563` |
| R14 | `RosterRefreshInterval` 3 min→30 s 安全（L1）/ 需裁定（L4）/ 会洗白（C2） | **会洗白，拒**：41 的两条 gap 明写候选根因是"`nats_topology_*` 唤醒 best-effort、被丢弃后**重画 jitterDur(3 min)**"（`41:205-218`）；interval 压到 30 s 后 proactive 失败会被常规 refresh 兜住，`poll_until 60` 稳定通过，gap 被"交易回"断言——drill 由 INCOMPLETE 变 GREEN 而缺陷被隐藏。这是 drill 83 试金石的反向实例。只有当 fast-path 臂能按**机制**（日志行区分"由 topology 事件唤醒"与"定时 refresh"）区分时才可压 | `[核]` |
| R15 | invocation 2 的 67 s 构成（brief：含 cutover+JS reset+nats restart；L1/L2：代码里 cutover 在 invocation 1 的 P5；L5：lone-JS fail-stop + auth retry；C4：cutover 6 轮跨调用）| **未定，无日志不可裁**。`→ former-N1 cutover` 行每次调用都打印（`:186-193`），SIGKILL 只在 invocation 1 发出（C1 F0-4）。它决定 grace 跳过净省 55 s 还是更多，是 instrumented 测量的首项 | `[核]` 代码顺序 :177→:193→:206 |

---

## 2. "≤5 min wall" 的可达性判定

### 2.1 判定

**不可达**。三个各自独立的下界任何一个都把主 sweep 钉在 5 min 之上，且它们都不是 harness 的属性：

1. **真 grow 链的 claim 单元**（91-A、51-I、30、42-F、10、13）：去掉 60 s grace 后单次 grow 仍 ≈2 min（raft AddNonvoter→catch-up→AddVoter + nats JS meta 1→2→3 形成 + G69 placement 观测 ≤30 s，`clusterwrite.go:673-689`，L1），两次 grow + 臂 ≈ 6–8 min（估计）。压到 5 min 以下只能删臂（10 的 kill/quorum、91 的 D-floor）——那是 claim 变更。
2. **41 的 S-survival 逃逸**：`RosterRefreshInterval` 3 min full-jitter ×(1+2 silent) ≈ 225 s 最坏（`41:277-282`，`roster.go:24,27,41`），加 retire 链 ≈ 7–12 min；压 interval 会洗白 `41:205-218` 的 proactive gap（R14）。
3. **97 的 6 cycle**：`LEAK_MIN_N=6` 是斜率 oracle 的结构地板（`97:358`），每 cycle ≥ STALE 5 s + 返回 + brk3 restart / 分区 heal（≤180+60）+ settle 25 → ≈5.5–8 min。

再加两条**过程**下界：grow lane（#70 未归因，g≈5–6 经验上限 → W_grow ≈ 8–13 min）与 M4 归因（solo 重跑 ≥ 1 个最长单元）。

### 2.2 要"凑到" 5 min 需要哪套让步集合，以及每条的保真度代价与正确性风险

| 让步 | 才能压掉什么 | 保真度代价（C1） | 正确性风险（C2） | 是否在前提内 |
|---|---|---|---|---|
| 压 `RosterRefreshInterval` 到 30 s（41） | 41 −150–240 s | sim 与生产取不同值即失去生产值下的竞态覆盖（drill 83 原则） | **洗白 41 的两条 proactive gap**（R14）；`rosterStaleGrace` 推导关系需机械守住否则健康车队被判 `agent_roster_stale` | **否**（隐藏缺陷） |
| `SOAK_CYCLES` 6→3（97） | 97 −2–3 min | — | 97 整条变 INCOMPLETE，`97:358-360` runtime-guard | **否**（弱化断言，C2 L5-3） |
| 把 91-A/51/30 的 grow 链拆掉或迁 hermetic | 各 −3–4 min | grow 本身是 claim；d7 是嵌入式 nats，跨进程 cutover/nats.conf 漂移/SIGKILL 复活只有 deploy tier 有 | R8 删除门要求 Go 后继 + 具名保留真栈探针 | **否**（claim 变更） |
| 压 `DefaultOfflineAfter` 60→20 s | 94/96-F −40 s | -j≥12 下一次 >20 s 心跳延迟让 G.1 把活进程 reconcile 成 `EXITED(-1)`——**假的 G.1 触发被 94/96-F 正向断言计为 pass**（C1 (b)3） | 71/73/74 的 tunnel、97 cycles 都会看见 sim 制造的假红（C2 L1-3） | 否 |
| 压 `upgradeRegisterDeadline` 120→30 s | 33-B −90 s | `upgrade_state.go:63-66` "no tunables without a use case"；33-A 的 re-exec+注册绝对耗时不变，deadline 提前制造生产没有的 register-vs-rollback 竞态 | 33 A1 逐字断言 `deadline 120s`（`33:264`）会假红 | 否 |
| `XferTimeoutTierBFloor`→30 s knob / build tag 压常量 | 96-A | build tag = 非真二进制（前提 (1)）；`xfer.go` 是 ctl/agent/broker 共用 SSOT，单侧压 floor 让 watchdog 先于客户端预算开火（`xfer.go:29-33` 记录的事故） | 96-A 负窗测的变成 watchdog 不是 crash | **否（BLOCKER）** |
| 96-A 换"归属 oracle"压到 3 min | 96-A | — | age <6 min 时 terminal **不可能存在**（R3），3 min 时只能看到"无 terminal"→ 假 PRODUCT-RED | 否 |
| j 拉到 ≥24 / 单波 300 容器 | Σ/j | 争用是传感器（#66/#67/#70/52-rc77 全在争用下发现）；-j6 已出 3 条 LOAD-SENSITIVE 偏离（audit review §13.2，`fsync_4k_ms=15.3`） | Docker 默认地址池 ≈28–30 个 bridge（无 `daemon.json`，`docker network create` 不带 `--subnet`，`docker.sh:23-27` `[核]`）→ instance 数超限即 `network create` 失败 | 技术上可解（sim 侧 `--subnet`），但 #70 无根因 |
| 归因改并行重跑 | 归因 pass | — | LOAD-SENSITIVE 失去定义，dispositions 里六条偏离的定性依据消失（C2 L5-5） | 否 |

**结论**：五份草案里没有一条 ≤5 min 路径不依赖至少一项上表"否"的让步。能背书的最好结果是表 C 的 ≈7–10 min。

---

## 3. 杠杆目录

列：节省（来源）/ 保真度代价 / 正确性风险 / 工作量 / 前置 / 依赖·互斥 / 四位批评者裁定（C1 保真 · C2 正确 · C3 可行 · C4 证据）/ **本报告裁定**。

| # | 杠杆 | 节省 | 保真度代价 | 正确性风险 | 工作量 | 前置 | 依赖·互斥 | C1/C2/C3/C4 | 裁定 |
|---|---|---|---|---|---|---|---|---|---|
| 0a | **分诊 67 的 45 min wedge**（`_g67_push` 无 timeout 包装，`67:52`；CLI 默认 `cliTransferTimeoutDefault` 35m08s+2m，`transfer.go:737-742`——机理为 C3 推测） | wall 45→≈21 min | 无 | 可能是产品回归（G67 面 A）| 0.5–1 天 | — | 一切之前 | —/—/**第 0 步**/— | **先做** |
| 0b | 日志行尾时间戳 + `run_one` 保 `setsid timeout` 语义 + 一次 `-j6` instrumented sweep | 0；换来真实 Σ/臂时长/add2 分摊 | 无 | 行首加戳会打断三个 parser + 1325 行 hermetic fixture（`run-drills.sh:435-457`；`log.sh:12-15`） | 0.5 天 + 1 次 sweep（≈45–60 min） | 0a | 所有估计的前提 | —/—/**非零成本**/**归因先于优化** | **先做** |
| A | **产品：`joinerBootGrace` 在本次调用执行了 `runSelfInit` 时跳过**（`initRanThisInvocation`） | −60 s/grow × 47 = **−47 min sum**；N=3 关键路径 −120 s；真实运维首 grow 少等 60 s | 非零：joiner daemon 比今天早 60 s 起，更贴近 former-N1 nats 复活与 lone-clustered-JS 等待窗（`:686-691`）——**改变可见 bug 集的时序包络**，#70/#47 频率可能变 | 需 B1 式 A/B；禁止为新红加 FLAKE_SIG | 20 行 Go + 单测 + 一轮审查；`cmd/tether` 有 DEBT CEILING（`structural_budget_golden.txt:104-108`），并入既有文件 | 0b | 支配 A''；与 A' 可叠 | 条件对但"代价零"错（MAJOR）/ 成立+2 条注（MINOR）/ **ROI 最高** / 成立 | **采纳**（A/B 保护） |
| A' | 产品：HALT 提示在 grace 开始前打印（`startJoinerHint` 前移） | 运维 UX；对 sim 0 | 零 | 无 | 极低 | — | 与 A 叠加 | 同意/同意/同意/— | 采纳（顺手） |
| A'' | sim 侧：后台跑 invocation 1、tail 到 `:707` 进度行即 `systemctl start` | 同 A | resume 路径覆盖从 47/sweep 降到 1；sim 把产品 stdout 当控制协议；比 HALT→start 更"省力"（Mandate ④） | rc≠0 时无契约（`simcluster:309-313` 的 rc=75 契约要改）| 150–200 行 sh + 双模式 + tests | — | **被 A 支配**；仅当产品不可改时的退路 | MAJOR/MAJOR/MAJOR/— | **不采纳** |
| B | joiner 的 [env] 供给与前一 grow 重叠 | −10 s/N=3 | 非零：`admit_creator`/`session create` 是 raft 写，落在前一 grow 的 membership 变更期间——更强争用形态 | 需 A/B，登记为争用变化 | 低 | — | 顺手 | MINOR/可做/—/— | 采纳（A/B 后） |
| C | **模板集群克隆**（每 sweep 由 tether 真 grow 一次 N=3/N=2，并发 SIGRTMIN+3 清停，每节点 `docker commit` + volume 拷贝，出生证明 + 交接门） | 修正后消费者仅 N=3：71/93/96/97/98（+51 主体、90-a 有条件）；N=2：22-main/50/52/42-setup → **−26–35 min sum**（估计）；**wall 单独为零**（模板构建 ≈195 s 在 96/97/98 关键路径上，C3） | post-restart ≠ post-grow：term/leader/JS meta 重选、同步 return 边沿（撞 `autoRebalanceCooldownTicks`=60 tick≈5 min，`proxy_auto_rebalance.go:26-27`，五份草案均未列）、停机窗口落入 raft 的 `broker_down`、争用形态 47→≈33 次 grow、#23/#47 采样稀释 | **live-fallback 是洗白向量（BLOCKER）**：fixture 层系统性失败被回退吞掉；交接门失败必须 = SETUP-RED + 产品证据行；每个 clone 断言 `alert ls` 空 + settle ≥ dwell+quiet+cooldown 相位；事件基线在交接后取；出生证明含 panic/bad-sig 全流扫描；陈旧门用 `sha256sum /usr/local/bin/tether` 不用可伪造文本 | 800–1500 行 + 16 次真栈交接验证 + 2 次 A/B sweep（C3） | 0b；与 D 同一增量 | **与 D 互为前提**；单做无 wall 收益 | 多条 MAJOR + 1 BLOCKER / MAJOR / 工作量低估 3–4× / 25 s 未测 | **有条件采纳，且为第二序**：只在 D 落地后、且 instrumented 数据证明 96/97/98 的夹具份额仍是关键路径时做 |
| C' | CRIU / docker checkpoint | — | 恢复态是 post-restart + 时钟跳变 + 跨容器 TCP 全断 | — | 需 criu + experimental + 重启 dockerd（sudo） | — | — | 否决/否决/否决/否决 | **否决** |
| D | **拆臂为独立执行单元**（96→A/D/F+B0、74→SRAB/C、73→REHOME/Q、90→a1/a2/a3/M6/M8、40→a/b/c、33→B/A、22→main/#35、71→CD/B、93→a/b/c、52→A/B/D、50→gates/spine）| wall 由最长**臂**决定：96 的 F 前置门 360 s（跨臂残留）结构性消失；p_max 从 96 移到 74-C/41/98 | 后臂失去前臂状态（96-F 在 heal 后、73-Q 失去 R7-M3 传感器、33-A 失去"回滚后再升级"槽状态）——逐 drill 写进 manifest；D 臂须收作"3 VOTER ∧ 两 agent ONLINE ≤360 s"终态断言否则 agent 重注册回归无人看 | abort 语义变化让 enum 上升（expected 逐条改）；结构性 `not_covered` 归 owner 臂（静态声明，**不做运行时去重**——带运行值的描述串不会相等，合法重复会被判 CONTRACT-ERROR）| runner 1000+ 行含测试（run-drills.sh 1041 行被 1325 行 hermetic 测试逐字钉住）+ 15 个 drill `case "$ARM"` + 每臂真栈跑，3–4 周 | 0b | 与 C/E/F 同一增量 | MAJOR（状态残留）/ MAJOR（聚合规则）/ **重构半个 harness** / 97 估计错 | **采纳，分两步**：先 96-F 独立夹具（1–2 天，p_max −6 min），再 96/98/74/90 |
| E | **lane 调度**：j≈16；grow lane 30 s 错峰 + 上限 g（默认 5–6，未测 g>6）；bring-up 信号量 ≤8 个 `up`；sim 侧 per-instance `--subnet` | 让 Σ/j 不成为约束 | 默认 sweep 降低争用 → **必须带 `--contended`/`--live-grow` stress lane（周/release gate）与 C1 registry**（#66/#67/#70/52-rc77/`GROW-ATTEMPTS`/`DEGRADED-WRITABLE`/eligibility 窗/G67 首推） | j 每上调一次是一轮 V7 门（偏离集 ⊆ 旧集 + 逐条 disposition）；j>≈24 撞地址池 | 80 行 + 地址簿 | 0b；C3 §0.3 | 与 D 绑定 | 未声明 C1 义务（MAJOR）/—/ j≈16 反证据 / — | 采纳，j 作为 V7 门控实验 |
| F | **裁决聚合**：臂 verdict 行字节不变 + `DRILL-ARM` 行；drill 级 = 计数器 join + 同优先级函数；expected 父行 `match` 由子行**派生**（全 MATCH→MATCH；≥1 MATCH-BAND→MATCH-BAND(ids)；否则 DEVIATION）；**INFRA-ABORT/CONTRACT-ERROR 作独立阻塞列 `arms_missing=n` 进 exit code，不覆盖 verdict**；kept-sites 按臂计数并双向对账 manifest | — | 否则 R1 反模式（96-A INFRA-ABORT 遮住 96-D 的真 ASSERT-FAIL）；manifest 删一臂门仍绿 | 含在 D | D | D | —/ MAJOR ×3（L3-1/2/4）/ 含在 D / — | 采纳（按 C2 修正） |
| G | 单元级超时 = 声明 worst × 2（非全局 1200）；M4 归因按单元 solo 重跑，重跑**重建**模板（否则 LOAD-SENSITIVE 失去 fixture 维度） | 归因从 22 min/条降到 3–8 min/条 | — | 撞线即 INFRA-ABORT 且不重试，#34 band 首手证据丢失（C2 L3-5） | 含在 D | D | D | —/ MINOR / — / — | 采纳 |
| H | **产品：agent `nats.PingInterval/MaxPingsOut`（如 20 s×2）+ natsconf passthrough 加 `ping_interval/ping_max` + install.sh nats.conf 模板写入默认值** | 98：detection (4,6] min → ≤60 s，单元 ≈7–9 → ≈3–4 min；98 预算按 T7 重推导（当前 330 s < 360 s 上界，本就是相位 flake 候选，C4） | 若只在 sim conf 设 passthrough 键 → sim conf ≠ 生产 conf（#20/#12/#22 类 re-render 测在生产从不写的形状上）→ **必须由 install.sh 写默认值使生产同形**；20 s×3 容忍度改变 #80/#82 类触发频率与解冻路径（两者皆合法，生产同改则仍保真） | 单侧改 → RECOVERY 恒真 + IMPACT 耗尽预算假红（R4） | 三触点中型产品增量（agent 选项 + natsconf + install.sh，存量车队走 orderly-update retrofit）| — | 独立 | MAJOR（服务端半）/ MAJOR / 三触点 / X3 | **采纳为产品增量（有独立运维价值：NAT 后 agent 4–6 min 才发现 broker 死是 #72 的一部分）**，用户裁定 |
| I | 产品：roster refresh 唤醒来源日志行（观测面）→ 41 fast-path 臂按机制区分 → 之后才可议 `RosterRefreshInterval` YAML 化 | 41 −150–240 s（仅在两步都做后） | interval 只作 operator 用例（大车队错峰/小车队快收敛）| 不做机制区分直接压 = 洗白（R14） | 小（日志行）+ 中（yaml 管道 + `rosterStaleGrace` 推导守卫） | — | 独立 | MINOR / **MAJOR 接近 BLOCKER** / 并入产品增量 B / — | 第一步可做；第二步用户裁定 |
| J | 其余 (b) 常量：`OfflineAfter`、`upgradeRegisterDeadline`、`forceSingleArmTTL`、`opCatchupTimeout`、`growConvergePoll`、`RestartSec`、`jsDownThreshold`、`observeTickInterval`、dwell/quiet | 合计 ≈5–8 min sum，wall 几乎无 | 每条都是"sim 与生产取不同值"；`RestartSec` 压缩把 crash-loop 变 StartLimit 终态（`install.sh:1224-1226` 明写不设 `StartLimitIntervalSec=0`）；`growConvergePoll` 压到 nats 冷启以下让 cutover 证据被吞（外审 M2） | `jsDownThreshold` 10 s 在每次 JS meta 重组升 severe 告警 → 不相关 drill 的 `run/push` 被拒假红、96-B0 假阳性；`opCatchupTimeout` 短则慢 joiner 进 auto-confirm（产品动作） | 每键 = serveconf 管道 + 文档 + 外审论证 + **bash drill 引用不到 Go 常量，全套预算静默漂移（T7）** | 常量暴露面 | — | 多条 MAJOR / MAJOR（jsDown、opCatchup）/ 计价错位 / — | **全部不做** |
| K | `SOAK_CYCLES` 6→3；build tag 压常量；`XferTimeoutTierBFloor` knob | — | 非真二进制 / SSOT 单侧破坏 | 97 变 INCOMPLETE；96-A 测 watchdog | — | — | — | BLOCKER/BLOCKER/—/— | **BLOCKER，出局** |
| L | 90-M6 用 `disk_check_interval=5s`（已有 yaml 键）加一条**周期采样臂**，保留启动采样臂 | 90-M6 −60–90 s；新增覆盖 | 替换而非追加会丢启动采样 claim（`90:180-198`） | — | 小 | — | — | MINOR/MINOR/—/— | 采纳（追加臂） |
| M | 产品：`proxy status --json` 加 `dial_fails/next_dial_at`；78 的几何检查作**追加** corroborator，桶总量 oracle 保留（可缩到 2 桶×45 s，前提基线重推数 ≥18） | 78 −1–2 min sum，不在 p_max | wire additive，过 `wire_inventory` append-only 账本 | 35 s 三间隔替代总量窗会漏"每 N 秒退避被复位"类缺陷（C2 L4-4）| 产品字段 + golden + 78 重写 + 真栈 | — | — | MAJOR（"更强"不成立）/ MAJOR / 低优先级 / — | 低优先级，仅作追加 |
| N | poll 网格事件化（inotify / `nats sub` / admin runtime 直读） | wall ≤1 min（L4/C4：只剩 p_max drill 内部 overshoot） | inotify 再撞 per-UID 上限；`logs.sh` 单一映射门（`simcluster_log_oracle_test.go`）；稳定窗必须留 `poll_until_fixed`（#46 flap bank） | O1 早判需含年龄门否则假 PRODUCT-RED；O4 flap 早判对 #80 形状恒 0 | — | — | — | MINOR/MAJOR（O1/O4）/不做/— | **不是杠杆，不做**；O3 的 `Runs` 只作证据 |
| O | 把 90 的 72 次 alert 生命周期、93 的 /metrics 取值、40 的 OPS 状态机迁 hermetic（R8：Go 后继 + 具名保留真栈探针）| sum −15–20 min，wall 0 | `kept-sites` per-drill 地板下降需带理由 coverage-trade 行；**74 的 dry-run/spread 臂是 #34 的"已证 A"，不可迁** | — | 4 个独立增量 | — | — | MAJOR（74）/—/放最后/— | 可选，最后 |
| P | 修 #33 / #34 / #70（产品缺陷） | 74/73 的 180/240 s 红路径消失；grow lane 解除约束 | — | — | 各自 leaf 增量 | — | 表 C 的前提 | — | 不是加速工作，但决定地板 |
| Q | 基础替换（去 systemd / podman / nspawn / LXD / Firecracker / `unshare` / KVM TSC / 产品 `Now` seam） | 容器启动仅 ≈3.6 min sum（1.6%） | grow cutover 依赖 supervisor `Restart=always` 复活 SIGKILL 的 nats（`cluster_grow_cutover.go:20-26` + `install.sh:1225-1226`）；journald 四流、MainPID、install.sh 属主全在 systemd 上 | — | 本机全部未装 / 需 root；`sudo -n` 失败 | — | — | 同意/同意/同意/同意 | **否决**（Firecracker 保留为将来"真断电"选项） |
| R | tmpfs 放 store | ≈2 s/grow | 保护的其实是 fsync 延迟这个传感器 | — | — | — | 仅在 fsync 饱和实测出现时作争用杠杆重开 | 同意 | 否决（作速度杠杆） |
| S | 96-A 的 390 s 负窗事件化 / 归属 oracle | 0（年龄门 6 min，R3） | 负窗覆盖"未知机制也没写"，pass 计数只证已知机制 | 见 K 行末 | — | — | — | —/BLOCKER/—/— | 否决；仅把 `_a57_try` 环改为"`Runs`≥1 ∧ 年龄 ≥ timeout+slack ∧ `Skips`=0 才早判" |

---

## 4. simcluster 新基础的设计草图（若做 D+E+F+G，与 C 同一增量）

**值得做的判断**：拆臂 + lane 调度是唯一能把 wall 从 ≈21 min 压到 ≈11–13 min 的手段（表 B），且不触碰任何产品常量；模板克隆在消费者名单缩到 8–10 个 drill 后是第二序，随拆臂一起设计接口、延后实现。

### 4.1 拓扑供给
- **默认：每单元自建**（今天的 `up/init/grow`，加 A 后 N=3 spine′ ≈190 s）。grow 是 47 次里 ≈30–37 次的传感器（#23/#47/#70/G67 首推/eligibility 窗），**不稀释**。
- **模板（延后）**：每 sweep 由 `tether cluster add` 真 grow `tpl-n3`/`tpl-n2` 各一次；并发 SIGRTMIN+3 清停；出生证明 = 3 VOTER ∧ ops 全 terminal ∧ JS meta size==N ∧ `alert ls` 空 ∧ panic/bad-sig 四流扫描空 ∧ `sha256sum /usr/local/bin/tether` ∧ 冻结时刻；`docker commit` 每节点（unit/`tether` 用户/journal/`/home/sim` 在 rootfs）+ `docker run --rm -v … cp -a` 拷两卷/节点（零 sudo）；host stash 按 instance 复制。
- **交接门**（每个消费者，全部是既有 poll/产品动词）：`transfer-leader brk1 --wait`（失败 = PRODUCT-RED）→ JS meta size==N → `alert ls` 空 → settle ≥ dwell 30 s + quiet 60 s + cooldown 相位 → 事件基线在此之后取 → 年龄 ≤10 min（超龄 SETUP-RED）。**无 live-fallback**。
- 消费者名单（C1 修正后）：N=3 71/93/96/97/98（51 主体、90-a1..a3 有条件）；N=2 22-main/50/52/42-setup。**live**：10/11/13/91/67/82/42-F/30/40/41/73/74/20/92/12/95/90-M6/M8/22-#35/51-I。
- `--live-grow` stress lane（全 live、-j6 争用档）每周 + release gate；C1 registry 登记每个"依赖 live-grow 次数"的传感器。

### 4.2 执行单元 = 臂
- `drills/<name>.sh` 头部 `# arms: A D F` + `# fixture: N3 live|N3 tpl|N2 …` + **`# forgoes: <本臂放弃的 post-grow 传感器>`**；同文件 `case "$ARM"` 分派（`r9d-nonvacuity`/`kept-sites` 按文件扫描不受影响；kept-sites 改按臂计数）。
- `simcluster drill <name> --arm <A>`：instance `drill-<name>-<A>`，`SIM_DRILL_ID=<name>.<A>`。
- lint 双向对账：文件内每个 `case` 标签 ∈ manifest，manifest 每臂可达 `drill_end`；结构性 `not_covered` 站点须在 owner 臂分支内。

### 4.3 裁决聚合
- 臂：`drill_end` 不变，emit 字节不变的 `DRILL-VERDICT` + 追加 `DRILL-ARM arm=<A> of=<drill>`。
- drill：四计数器逐项求和 + `assert.sh:495-501` 的同一优先级函数；`arms_missing`（INFRA-ABORT/CONTRACT-ERROR/超时）为独立阻塞列进 exit code，**不覆盖 verdict**。
- `expected-verdicts.tsv`：父行 `drill` 保留 claim 集合；子行 `drill/arm` 承载 `expected_nc_gap` 与 bands；父行 `match` 由子行派生；`validate-verdicts.sh` 校验子行 nc_gap 之和 == 父行、父行 bands 为 `-`；`ledger-crosscheck.sh` 读父行 owner 列不动。拆臂的正面收益：banded 前臂失败不再遮住后臂新红（`_first_fail_sig` 只看第一条）。
- 首次拆臂 sweep 后 expected 表逐条手改并在 commit message 写理由（abort 语义变化让 enum 上升）。

### 4.4 runner
- 三 lane：`tpl`（并发 j）、`grow`（30 s 错峰 + 上限 g，默认 5，`--grow-cap`）、`N1`（并发 j）；bring-up 信号量 ≤8 个 `up`（`SIM_UP_SEM` 文件锁）；sim 侧 per-instance `--subnet 10.<i>.0.0/24` 地址簿（不改 `daemon.json`，零 sudo）。
- 推荐运行点 j≈16（≈65 单元 × 4.2 容器 ≈ 60–70 容器；inotify 8192 够）；j 每上调一档过 V7 门。
- 单元超时 = 声明 worst × 2；M4 归因按单元 solo（重建模板）；`drill-costs.tsv` 键改单元；`DRILL-POLL-WAIT` 行尾时间戳。
- hermetic 门跟随：新脚本进 `tests/run-all.sh` 循环（`simcluster_gate_set_test.go` 双向对账）；`verdict-contract-test.sh` 加"两臂求和 == 合成行"正负控制 ≥6 条变异。

### 4.5 存储隔离
- 保持 docker named volume（ext4 on NVMe）；Phase-4 per-drill LVM 槽位（一次 `sudo -S`）只在 j≈16 的 M3 遥测显示 fsync p99 持续 >10 ms 且 ≥2 条偏离归因 fsync 时触发（accel plan R6 条件仍成立，分母变了）。
- tmpfs 不用于任何有持久性/重启存活断言的 store。

### 4.6 与 hermetic 层的分工
deploy tier 只保留六类 hermetic 结构上够不到的东西：跨进程 nats-server（cutover/nats.conf 漂移/SIGKILL 复活/JS reset）、systemd/install.sh/属主/journald、跨容器真数据面（tunnel/SS/S0-ingress/iptables）、真 fsync/备份、真 PTY、真时间接线探针。可迁走的（R8 门：Go 后继 + 具名保留探针，sum −15–20 min，不改 wall）：90 的 alert 生命周期（保留 `broker_down`/`below_quorum` 拓扑臂）、93 的 metrics 取值（保留 webhook 真 HTTP）、40 的 OPS 状态机（保留 drain/retire 真栈）。**74 的 dry-run/spread 臂不可迁**（#34 已证 A）。

### 4.7 保留的"真时间探针"名单（每个产品 timer 家族一个，其余同族测试留 hermetic）

| 探针 | drill/臂 | 绑定常量 | 真时间下界 |
|---|---|---|---|
| arm token 过期 | 22-main `sleep 61`（`22:154`）| `forceSingleArmTTL` 60 s | 61 s |
| 升级注册 deadline 整组回滚 | 33-B | `upgradeRegisterDeadline` 120 s（`upgrade_state.go:67`）| 120 s + re-exec |
| lease grant window 双执行 | 83/84 | `leaseGrantWindow` 5 s | 试金石本体 |
| roster silence rebuild | 41 S-survival | `RosterRefreshInterval` 3 min ×3 | ≤225 s |
| auto-rebalance quiet window | 74-C | dwell 30 s + quiet 60 s | 90–180 s |
| nats.go ping + teardown 梯子 | 98 | `PingInterval`×`MaxPingsOut` + 20+10+10 | (4,6] min 今天 / ≤2 min（增量 H 后）|
| tier-B 超时 + finalize-on-recovery | 96-A | floor 5 min + slack 60 s | 0 或 ≥6 min（分支①/③） |
| `RestartSec` 复活 | 95 | 2 s | 秒级 |

---

## 5. 需要的产品侧改动

### 5.1 有独立产品价值（可作 leaf 增量）

| 改动 | 独立价值 | 对 sim 的收益 | 备注 |
|---|---|---|---|
| **A** `joinerBootGrace` 在本次调用执行了 `runSelfInit` 时跳过（`cluster_add_drive.go:118-124,206,692`）+ A' HALT 提示前移 | operator 首 grow 少等 60 s 才看到"start the joiner"提示 | −47 min sum | 由 #I1 不变量证明安全；resume 路径（raft/ 已存在）保留 60 s；需 A/B 观察 #70/#47 频率 |
| **H** agent `PingInterval/MaxPingsOut` + natsconf passthrough `ping_interval/ping_max` + install.sh 模板默认值 | NAT 后 agent 对半死链路的**发现**从 4–6 min 降到 ≤1 min（#72 事故的发现半）| 98 −4 min | 三触点；存量车队 retrofit；98 预算按 T7 重推 |
| **I₁** roster refresh 唤醒来源日志行（`rosterRequiresReconnect` 由 topology 事件还是定时触发） | 运维回答"agent 为什么没换 broker" | 让 41 的 fast-path gap 可按机制判 | 暴露型面，零风险 |
| **G67 面 A / 67 wedge 分诊** | 若是产品回归则是缺陷 | wall 45→21 min | 第 0 步 |
| **#33 / #34 / #70 修复** | 产品缺陷 | 表 C 前提 | 各自增量 |
| （可选）`admin runtime --json` 增 `constants` 块（暴露被 drill 引用的时间常量） | 运维可读生效值 | 消除 bash 预算对 Go 常量的静默漂移（T7） | 仅当未来真要动任何常量时才值得 |
| （可选）`proxy status` 加 `dial_fails/next_dial_at` | 回答"节点为什么不拨号"（#78 现场问题）| 78 −1–2 min sum | 低优先级；78 总量 oracle 保留 |

### 5.2 仅为测试（原则上不做）

`DefaultOfflineAfter` / `upgradeRegisterDeadline` / `forceSingleArmTTL` / `opCatchupTimeout` / `growConvergePoll` / `RestartSec` / `jsDownThreshold` / `observeTickInterval`+dwell/quiet / `XferTimeoutTierBFloor` / `MinXferCrossHomeReapAge`（外审 F2 raise-only）/ `LockLeaseTTL` / raft 三常量（T2）/ `closeBudget`·`poisonGrace`（#72 契约）/ build tag 变体 / `--joiner-boot-grace` flag（把知识推给调用方的 test-only seam）/ `RosterRefreshInterval` 在未做 I₁ 前的压缩。

---

## 6. 动手前该先做的廉价测量 / 实验（明确不是跑全套 drill）

| # | 测量 | 回答什么 | 预计耗时 |
|---|---|---|---|
| 1 | 读 67 上次 INFRA-ABORT 的 log（若无归档则 solo 跑一次 `67`，带 `--drill-timeout 900`），定位卡在 `_g67_push` 还是 after-loop | 0a 的机理（产品回归 vs drill 缺陷） | 15–45 min |
| 2 | `log.sh` 行尾时间戳原型 + `tests/run-all.sh`（hermetic，≈2 min）确认三个 parser 不破 | 0b 可行性 | 0.5 天 |
| 3 | solo `10-grow-to-3` 一次，在 `cmd_grow` 的 add1/env/add2 前后打 `date +%s`，并 grep joiner journal 里 lone-clustered-JS fail-stop 次数 | R15（add2 的 67 s 分摊）、A 的净省、`topoRestartBaseDelay` 是否在路径上 | 6 min |
| 4 | 同上在 `c6b9c9e` 种子镜像上再跑一次（`docker images` 若还有）| C4-X2：grow 是否回归（152 s vs 176 s）| 6 min ×2 |
| 5 | solo `96` 一次（instrumented）| 22 min 花在哪：A 走分支①还是②、F 前置门实付、D3 实付 | 22 min |
| 6 | solo `98` 两次，记录 DROP 时刻到 `/connz` absent 的相位 | 检测窗是否落在 (4,6] 且 330 s 预算是否相位 flake | 2 × 8 min |
| 7 | 手工模板原型：`up 3` → init → grow ×2 → 并发 SIGRTMIN+3 停 → commit ×4 + 卷拷 → 起新 instance → 计时到 3 VOTER + JS meta + `alert ls` 空；顺带看 volume 根属主与 `journalctl -b` | τ（20–90 s 之争）、交接门形状、C 是否值得 | 40 min |
| 8 | `for i in $(seq 40); do docker network create t$i; done` 再删 | Docker 默认地址池上限（≈28–30 之推导）| 2 min |
| 9 | 把 `[env]` 供给与前一 grow 重叠（B）在 `10` 上 A/B 各 2 次 | B 的争用变化是否可见 | 25 min |
| 10 | 一次 `-j6` instrumented sweep（在 1–2 之后）| 真实 Σ、每臂时长、R 类窗实付率、grow lane 曲线（g 与错峰）| ≈45–60 min + 归因 |

第 10 项是唯一的"全套"，但它是所有地板数字的收据；在它之前，本报告的表 B/C 全部是估计。

---

## 7. 留给用户的决策点

| # | 决策 | 选项 | 建议 |
|---|---|---|---|
| D1 | 目标口径 | (a) 坚持 ≤5 min 主 sweep；(b) 改为"主 sweep ≤12 min（表 B）→ ≤9 min（表 C）"；(c) 含归因 pass 的口径 | **(b)**：5 min 在前提内不可达（§2）；写死 5 会诱导弱化断言 |
| D2 | 第 0 步 | 先分诊 67 + 时间戳 + instrumented sweep，再决定任何杠杆 | **是**；不做则所有数字是两个纪元混算 |
| D3 | 产品增量 A（grace 跳过） | 做 / 不做（退回 A'' sim 侧） | **做**，带 A/B 与"禁止为新红加 FLAKE_SIG"条款 |
| D4 | 产品增量 H（ping 两侧 + install.sh 默认值） | 做（生产行为变更，20 s×2 或用户定值）/ 不做（98 保持 7–9 min 单元）| **做**，它本身是 #72 的发现半；值由用户定 |
| D5 | harness 增量范围 | (a) 只拆 96-F（1–2 天）；(b) 拆臂 + lane + 聚合（3–4 周）；(c) (b)+模板 | **先 (a)，instrumented 数据后决定 (b)；(c) 延后**（模板消费者缩到 8–10 个 drill 后 sum −26–35 min、wall 单独为 0） |
| D6 | 41 的 `RosterRefreshInterval` | (a) 保持 stock（41 ≈ 7–12 min，是表 C 的 wall 设定者）；(b) 先做 I₁ 机制区分再 YAML 化并压到 30–60 s | **(a)** 直到 I₁ 落地；(b) 需要外审确认 fast-path gap 仍可见 |
| D7 | 争用政策 | 默认 sweep 降争用（错峰/信号量/模板）+ 每周 `--live-grow` stress lane + C1 registry / 不设 lane | **设 lane**，否则 #66/#67/#70/52-rc77 类传感器静默退役 |
| D8 | 模板消费者的边界 | 41/40/51-body/90-a 是否可模板（各失去 grow 残留/in-flight op 状态）| **41/40 live**（#45 分支、retire 链依赖 grow 残留）；51-body/90-a 待第 7 项原型 |
| D9 | j 与地址池 | sim 侧 `--subnet`（零 sudo）/ `daemon.json` 池扩容（sudo + 重启 dockerd 杀正在跑的容器） | **sim 侧** |
| D10 | 96-A 的 390 s | 保持（两个世界，分支①/③付满）/ 改 `_a57_try` 环为年龄门早判 | **保持**；早判仅加年龄门条件，不删负窗 |
| D11 | 宿主副作用 | L5 探测 `lxc version` 触发 Ubuntu socket 激活安装器，**已装 snap `lxd 5.21.7-1018661`**（`snap list` `[核]`；仓库树未动；未 `init`，`docker network ls` 干净）| 若不需要：`sudo snap remove lxd`；并把"探测命令先 `dpkg -S`/`file`"写成只读任务规则 |
| D12 | hermetic 迁移（杠杆 O） | 做 4 个独立增量（sum −15–20 min，wall 0）/ 不做 | 最后再议 |

---

## 8. 附录：重审上次 accel plan §3 的每条否决

| §3 否决项 | 当初理由 | 理由是否依赖"Mandate 不可动" | 新前提下的结论 |
|---|---|---|---|
| 1 加速时钟层（libfaketime / CLONE_NEWTIME / 确定性模拟器） | Go 走 vDSO；时间 ns 只能偏移且分裂 monotonic/realtime；Shadow 类要替掉 OS/网络/盘 | **否**，纯技术 | **仍否决**。补两个分支：KVM TSC scaling 是唯一整栈快钟，但 I/O 不跟着变快（6.4 ms fdatasync 在 10× guest 里是 64 ms，改写 raft election 1 s 与 fsync 尾部的比例——drill 83 试金石），且本机无 root/tap；产品 `Now` seam 只管比较不管 `time.Sleep/After`，碰不到 nats-server JS meta、nats.go ping、hashicorp/raft 三个第三方计时源 |
| 2 测试专用产品 seam（fast-clock 旋钮） | 外审 F2 把 `xfer_cross_home_reap_age` 钉 ≥15 min（生产数据安全论证）；`upgrade_state.go:63-66` "no tunables without a use case" | **否**，是生产安全 + 用例纪律 | **仍否决，但当初过宽**：它把"暴露型观测面"（admin runtime pass 计数、events、ops timeline、status 字段——`adminsock/protocol.go:31-34` 明确是 operator verb 非 backdoor）与"行为型 knob"混为一谈。暴露型面从未被 F2 否决；行为型 knob 按**独立运维价值**个案评估（PingInterval 是；tier-B floor 30 s 不是——它是 2 MiB/s 链路承诺的反面） |
| 3 第六态 ENV-RED | 五态是三处解析的契约；自报环境 = 洗白向量 | **否** | **仍对，且更重要**：高并发下洗白压力更大；模板交接失败也必须落在 SETUP-RED 而非新态 |
| 4 放宽 FLAKE_SIG / 自动重试 / 重跑换裁决 | 20/91 那次就会被重试成脚注 | **否** | **仍对**；A 增量后 invocation 2 的 lone-clustered-JS 新红、模板 live-fallback 都是它的新形态 |
| 5 tmpfs 放需持久性断言的 store | 95/96/97 重启存活 claim；`docker kill` 本就丢不掉 page cache | 一半错位 | **仍拒作为速度杠杆**（一次 grow 几百次 raft commit × 6.4 ms ≈ 2 s），理由修正为"它保护的是 fsync 延迟这个传感器"；只在 fsync 饱和实测出现时作为争用杠杆重开 |
| 6 userns-remap | 破坏 `--privileged`+systemd 与 install.sh 属主语义 | 部分（Docker 特定） | 理由过窄（podman/nspawn/LXD 下 userns 是机制本身、属主经 subuid 完整保留），但基础替换本身零速度收益（容器启动 ≈1.6% sum；grow cutover 依赖 supervisor 复活 SIGKILL 的 nats）→ **无需重开** |
| 7 fixture 快照 | "制造 grow 的产物"（Mandate ①/③/④） | **是** | **有条件重开**：若快照由同一 sweep 内 tether 真 grow 产出、从不 check in，就不是"制造"；当初论证把"谁做的"与"每次都做"混在一起。真正丢的是每 drill 在自己并发条件下的 grow（争用传感器）、post-grow 瞬态（eligibility 窗、G67 首推、首次 G.2）、以及"post-restart ≠ post-grow"的 delta。裁决：仅给 claim 不引用 grow 且不隐含 post-grow 前提的 drill（§4.1 名单），交接门显式重建前提，无 fallback，保留 live-grow lane |
| 8 迁 drill 断言进 Go 层（R8 未采纳） | 22 的 `sleep 61` 有 Go 后继却仍是唯一真栈接线探针 | **否** | **仍对**；新前提下它成了主要 wall 杠杆候选，所以更需要 R8 的删除门（Go 后继 + 具名保留一个真栈探针，§4.7 名单） |

---

### 本轮核对范围声明
本报告独立复核的站点：`cmd/tether/cluster_add_drive.go:118-124,171-180,200-210,535-563,686-724`；`cmd/tether/cluster_add.go:29,31,224`；`test/simcluster/drills/96-mid-flight-chaos.sh:110-120,390-425`；`internal/broker/xfer_inflight.go:312-325,715-728`；`internal/proto/xfer.go:100-127`；`internal/broker/transfer.go:1349-1356`；`test/simcluster/drills/97-soak-cycles.sh:121-125,277-282`；`internal/broker/reconcile_grow_lock.go:95-128`；`internal/natsconf/preflight.go:40-58`；`test/simcluster/drills/98-stuck-redial-recovery.sh:28-47,64-73`；`test/simcluster/drills/lib/fault.sh:80-92`；`test/simcluster/simcluster:296-330`；`test/simcluster/lib/docker.sh:20-28`；`test/simcluster/drill-costs.tsv`；`test/simcluster/expected-verdicts.tsv`；`test/simcluster/expected-verdicts-log.md:415-430`；`docs/reviews/prerelease-audit-external-review.md:1005-1040`；`docs/deploy-tier-gotchas.md:95-100,165-186`；`test/simcluster/drills/lib/cluster.sh:8-28`；`test/simcluster/drills/41-shrink-to-standalone.sh:205-218,270-284`；`/etc/docker/daemon.json`（不存在）；`docker network ls`（4）；`snap list lxd`。未跑任何 drill/测试/构建；未改任何文件。其余引用沿用草案/批评并注明来源。
