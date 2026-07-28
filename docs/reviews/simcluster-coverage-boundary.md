# simcluster 覆盖边界（R15 收官披露）

> 本文件登记 deploy-tier 模拟集群（`test/simcluster/`）**结构性无法在-sim 内忠实观测**的覆盖项。
> 铁律（`test/simcluster/README.md` Mandate + CLAUDE.md §5）：只有**结构性不可测**的项才进本边界；
> 凡产品可修的缺陷一律修复、凡 drill 可构造的 fixture 一律构造，绝不用"披露"掩盖能修的问题。
> 每一项给出**为什么 in-sim 结构上不可能**的可核验理由，并指明其覆盖在别处（hermetic 单测 / 别的 drill / 源码确定性）如何补足。

分类：
- **§1 真结构性**（需真硬件 / 真内核 / 真 per-user systemd / 亚秒采样 / 物理不可复制的状态）——INCOMPLETE 保留，本边界登记。
- **§2 待 live 定夺**（R15 批量 re-run 用 SIM_KEEP 现场确认后，翻成"产品修复"或落入 §1）。
- **§3 可构造 fixture**（非结构性——列出以示未遗漏；按 roadmap 决定构造或显式 deferred）。

---

## §1 真结构性（in-sim 结构上不可观测）

### 62 · OQ-2 — 真·不可中断-D 态 + `mode:off` 无 safe 的 legacy 挂死
- **drill**：`62-remote-fs-safe` arm 2（INCOMPLETE，nc_gap=1）。
- **为什么结构性**：真正的 uninterruptible sleep（TASK_UNINTERRUPTIBLE，`ps` state `D`）只能由**真实阻塞 I/O 在真实存储/网络栈**上产生（如硬件 NFS server 无响应）。容器内用 FUSE daemon 近似出的挂起态是 **T/S-state、`kill -9` 可回收**的——与真 D 态在"能否被信号打断"这一决定性维度上不同。要观测真 D 态需**专用硬件**（真 NFS/iSCSI target 中途拔线），Docker 容器结构上给不出。
- **别处覆盖**：`mode:auto`/`mode:off-with-safe` 的可回收挂起路径在本 drill arm 1 已实测；hung-FS spawn 逻辑有 hermetic 单测。真 D 态留给实机（`docs/reviews` 已记 OQ-2）。

### 82 · U1–U4 — `systemd --user` / linger 冷启动 onboarding
- **drill**：`82-agent-onboarding-invite`（INCOMPLETE，nc_gap=1）。
- **为什么结构性**：容器的 PID1 是系统级 systemd，**没有 per-user systemd manager**（`systemd --user`）、也没有 `loginctl enable-linger` 所需的 logind/session 基础设施。usage §6.1 的 `systemd --user` onboarding 路径需要一个真正的多用户登录会话栈，容器镜像结构上不提供（起 `systemd --user` 会因无 user D-Bus session 失败）。
- **别处覆盖**：agent.yaml 落盘、trust-anchor、account_pub pin、bootstrap 刷新等 onboarding 的**其余**面在 82 的 J/T 臂已实测；仅 `--user` 单元管理留给实机。

### 51 · H1a — DR 后 agent 的瞬时 offline 窗口
- **drill**：`51-full-dr`（INCOMPLETE，nc_gap 之一；已在 drill 内 signature-guarded 登记）。
- **为什么结构性**：同一身份 DR 后 agent 的 tunnel cert **字节不变**，broker 一答应它就**亚秒级重连**。要断言"restore 后 agent 曾短暂 offline"需在这个亚秒窗口内采样，而 shell 轮询**可靠地慢于**重连（2026-07-19 + 本轮实测每次都被抢先）。唯一"观测到"的办法是人为拖住 agent offline——那是**篡改环境规避真实时序**，Mandate 明禁。
- **别处覆盖**：restore 后 DB 带 STALE `last_seen` 使 H1b 的 ONLINE 非空；H2 证 served terminus——END 态完整覆盖，缺的只是这个物理上抢不到的 transient。

### 96 · #57 — 被杀 home broker 上的 in-flight transfer audit
- **drill**：`96-mid-flight-chaos` arm A（nc_gap 之一）。
- **为什么结构性**：crashed transfer 的 audit 行**只存在于被 kill 的 home broker（brk2）本地**、**不复制到幸存者**。窗口内 brk2 全程 down，从幸存者侧**物理上查不到**这条 audit（数据随被杀进程一起不可达）。这不是"再跑一次就能抓到"的 runtime-guard，是被杀节点带走的状态。
- **别处覆盖**：#57 机制源码确定（watchdog 挂在 broker runCtx `transfer.go:593/:704`；tracker 重建为空 `broker.go:602`）；hermetic owner = transfer 单元测试。A0c/A1b 已证 transfer 的 start/complete 控制对与 start 行可读。

### 96 · #65 — 少数派 stale 写（partitioned-minority durable write）
- **drill**：`96-mid-flight-chaos` arm D6b（nc_gap 之一）。
- **为什么结构性**：真正的 #65 变体需要**一个在分区前就已认证、且跨分区长活的写客户端**（condition Y）。tether 的 CLI 是**短命连接**——每次命令新建认证连接，无法持有一条"分区前建立、分区后仍活"的写通道。R6 已实测：台账里"5/6 durable minority writes"实为 `--nats-url` dial 到多数派的**正确多数派提交被误归因**，非真少数派 stale 写。真 #65 需长活 pre-partition client，CLI 结构上提供不了。
- **别处覆盖**：R6 关键发现（committer attribution）已证多数派提交路径正确；96-D6b 已 signature-guard 登记该结构性不可达。

### 30 · (c) — 滚动升级中 leader-hop 的 mid-run HALT/resume
- **drill**：`30-rolling-upgrade`（INCOMPLETE，nc_gap 之一）。
- **为什么结构性**：本 drill 构造的 HALT 刻意打在**第一跳**——那是唯一 per-hop 期望确定的部分状态。leader-hop 的 HALT 还得先熬过 plan 先做的 leadership transfer，而 **sim 无法在那一瞬（leader 交接的确切 instant）注入中断**——没有"在子操作的精确时刻掐断"的注入面。
- **别处覆盖**：G5 roll 顺序（leader-last）、per-hop advance、whole-host dual-version、(b)-态 HALT + skew 判定、cluster unlock 恢复 + terminal re-roll、PID-preserving re-exec、raft-write 连续性（失败+完成两条 roll）——fixture 落地时全覆盖；仅 leader-hop 那一瞬的中断结构上抓不到。

---

## §2 深层产品缺陷（R15 live-确认；非结构性，可修但值一个专属批次，R15 诚实披露为 PRODUCT-RED，不在 HA 关键路径 rush）

### 42+51 · grow-onto-RECOVERED-broker 缺陷族（racknerd/pc732 生产事故家族）
**live 证据（re-run r15v1，2026-07-20）**：从一个**恢复过的 survivor** 上 re-grow 回 N≥2 会死锁，两个变体：

- **42（grow-onto-force-single）**：survivor brk1 是 force-single'd + 仍 clustered nats.conf → JS 503 的 lone voter；`cluster add brk2` 的 cutover 把 brk1 保持 clustered，returning brk2 起来时本地 raft config 是单-voter {brk2}（init --from-manifest，leader 的 AddNonvoter 还没经 raft 到达）+ JS 不可用 → 命中 `broker.go:1063 n1ClusteredJetStreamFatal` crash-loop → 永不 mesh → join op 卡 CATCHING_UP（`reachable:false`）。drill 42 误标已从 #47 更正为 #GROW-ONTO-FORCE-SINGLE（force-single 根因优先判）。
- **51（grow-onto-restored/resnapshot'd）**：survivor brk1 **干净**——已 de-cluster 成 standalone、无 force_single、JS 健康（drill G3c 断言证）。但 `cluster add brk2` 的 cutover 把 brk1 从 standalone **re-cluster** 成 lone-clustered-voter，1→2 clustered-JS meta 对 joiner 永不形成、join op 卡死。这是 pc732 "grow-onto-resnapshot'd-broker" 事故同型（memory project_cluster_ha_realmachine_test）。drill 51 归因已从 [#31/#45] 更正为 [#GROW-ONTO-RECOVERED]。

**为什么可修但不在 R15 修**：这是 tether 最关键的 HA 路径（clustered-JS meta 在 1→2 grow-after-recovery 的形成时序）。候选修复（42-agent 诊断）：(1) `broker.go:1063` 对 mid-grow joiner 给有界 catch-up 宽限，别立即 FATAL；(2) recovery/resnapshot 把 survivor 数据面也 normalize 到真 grow-ready；(3) cluster_add cutover 重排 JS-meta 形成时序。任一都需**全 e2e clustered-JS 回归**（drills 10/20/22/40/43/95 全涉 cluster grow/JS）+ 专属验证。R15 不 rush——rush 会在 HA 路径引入比暴露缺陷更坏的回归，违背"经得起测试"。**外审门呈报，由用户定夺是否 greenlight 专属修复批。**

**与 R15 已修的区分**：#31/#45 mid-grow-**bundle** 残留（bundle 在 membership-op 中途拍摄→leaked lock/stalled op 被带进 restored origin）是**另一回事**，已由 R15 restore-侧修复闭合（`normalizeRestoreStaging` 清 grow/upgrade marker+lease+非终态 op；hermetic verifier `TestRestoreClearsStaleGrowUpgradeAndOpResidue`）；51 的 bundle 取自健康 N=3，不含该残留，故非此路径所阻。

### 41 · first-retire 主动疏散 fast-path（`rosterRequiresReconnect`）— **仍 OPEN；理由已换（2026-07-28，B2 债务清理）**
- **verdict 不变（INCOMPLETE / 2 gaps），但两条 gap 的**理由**全部重写**：旧理由是猜的且已被证伪，新理由是实测的。
- **旧理由错在哪**：原条目从"meshed retire 下 silence-rebuild 路径**结构上不能**触发"（真）推出"所以 agt1 不会在窗口内物理离开"（假），并把原因归给一个"疑似 host/IP 匹配缺口"、自标"未经确认"。实测：agent journal 里 proactive 路径**命中**，`connected_url=nats://brk2:4222`（**hostname，不是 IP**），**62 ms** 后 `agent: registered`，同刻 `/connz`：brk2（retiring）0、brk1（voter）1。**疏散确实发生，怀疑的机制不是真凶。**
- **新理由（实测五次）**：run1/run2 在注册后 ~57s 移动、相差 88 ms；run3 在 `poll_until 60 3` 下 PASS；**run4 在 60s 超时**；**run5 在 210s 超时**——210s 是 180s 全抖动上限 + 余量，也正是这条臂历史上带的窗口。⇒ **移动本身已确认，延迟无界**。再往上放窗口就是为了把红变绿而发明 SLA，harness 绝不做这件事。
- **候选机制（记为候选，不下结论）**：疏散搭的是 roster 刷新循环，定时器 `jitterDur(3 min)` = **UNIFORM(0, 180s]**、每次唤醒后**重抽**；能提前唤醒它的 `nats_topology_*` sys.event 是 **best-effort 且不重试**（缓冲 1、合并，唤醒时若 `reconnectInFlight`/`rebuilding` 为真则**丢弃**并重置为新的全抖动抽样）——**连丢两次即超过 210s**。第二个候选是字符串同一性盲（已在 `internal/agent` 写成可执行断言）。两者都未确认，**这正是前三轮在这一块犯的错**：把候选当结论转述。
- **"不 launder" 全程生效，并且这次是它抓住了我。** 88 ms 的重合是关于**那两次运行**的证据、不是关于**机制**的证据；据此发明一个 60s 的 SLA，与当年那两条 gap 是同一类错误、方向相反。
- **正确性不变量不受影响**且仍在断言：agt1 跨 retire 保持功能可达，真正下线时经 #48 silence-rebuild 逃出——**上述每一次运行都通过**，包括快路径没赶上窗口的那两次。这条 gap 只关于**延迟**，不关于 agent 是否存活。

---

## §3 可构造 fixture（非结构性；未遗漏登记，按 roadmap 决定构造 or 显式 deferred）

- **30 · (b)** N=2 write-fence NEGATIVE 控制（证 write-probe 能在 raft 写真停时转 RED）——可构造（刻意 stall raft 写）。positive 已覆盖；negative 控制为 nice-to-have。
- **71 · G/F** home-return stickiness（需 drained-then-RETURNED home）+ `rehome_stalled{no_eligible_target}`（需 N=1-eligible 拓扑）——可构造的 topology fixture，本 drill 未构造。
- **H2 · N≥3 分布式 PIN 限速 drill — DEFERRED（v2 验收）**（外审 codex H2 / claude N-2）。v1 的 PIN 限速**明确是 per-broker best-effort**（architecture §E.6 已改正为诚实语义：N-broker 集群 ≈ N×10/min 的猜测预算，argon2id 是主防线）；集群一致的全局计数须在**未认证 connect 路径**上引入分布式写，是更差的 DoS 放大面，v1 不做。因此**不存在**要构造的 drill——一个"故意不存在的语义"的 drill 是虚构，不建。**若/当** cluster-consistent 限速在 v2 落地，这条 N≥3 分布 callout drill 即成其验收门（现 drill 80 的 N=1 GREEN 只证 N=1 拒绝、不据以推断多-broker 行为）。无 tsv 行（无 drill 存在）。
- 以上均**非结构性**：列此以证"未遗漏"，最终构造与否由 roadmap 定；在构造前保持 INCOMPLETE/gap 如实呈现，绝不当作已覆盖。
