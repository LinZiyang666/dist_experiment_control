# simcluster 提速研究 — 五份草案与四份批评（原文附录）

> 附属于 `simcluster-speed-research.md`（综合报告）。本文件是 10-agent 研究 workflow（`wf_4d7156ce-c5a`，2026-09-18）各 lane 的原始返回，未经主进程编辑；综合报告里的裁定以报告为准，这里只作溯源。



---

# 草案 L1 产品时间常量清单

## 常量清单表

> 读法：**等它的 drill/站点**一列只列健康路径上真会等到它的地方；只在失败时才等满的预算另标"预算"。生产值均取自源码，行号为 2026-09-18 工作树（`a3431a1`）。类别定义见下一节：(a) 已是 operator 配置项 / (b) 可合法变配置项且小值是合法生产配置 / (c) 缩短对产品有独立价值 / (d) 内在不可压缩。

### A · `cluster add` 驱动器（grow 的 47 次调用全部经过这里）

| 常量 | 生产值 | 位置 | 等它的 drill / 站点 | 类别 |
|---|---|---|---|---|
| `joinerBootGrace` | 60 s | `cmd/tether/cluster_add_drive.go:692`（poll 在 `:694-720`，2 s grain） | **每个 fresh grow 的第 1 次调用无条件等满**（joiner daemon 被设计停掉，`simcluster:263`；第 2 次调用前 sim 已用 `_grow_ctl_ready` 60/2 轮询证明就绪 `simcluster:327`，故第 2 次立即通过）。全套 **47 次 grow**（统计见 §健康路径）= 2820 s | **(c)** |
| `growConvergePoll` | 3 s | `cmd/tether/cluster_add.go:31`；用于 `waitJoinServing :440`、`waitOpCatchingUp :517`、`cutoverBroker :555`（6 次×3 s=18 s 上限） | 每次 grow 约 4–6 个 poll 点的 grain 超调；cutover 重试环 | (b) |
| `growTriggerTimeout` / `growConvergeTimeout` | 10 s / 3 min | `cluster_add.go:29-30` | 预算（单次 NATS 请求 / 收敛） | 预算 |
| `growConnectAuthRetryWindow` / pause | 30 s / 1 s | `cluster_add.go:36-37` | 只在 cutover 后撞到 Authorization Violation 才等 | (d) |
| `waitOpCatchingUp` 硬编码 | 2 min | `cluster_add_drive.go:501` | 预算 | 预算 |
| `--timeout`（`waitJoinServing`） | 默认 10 min；sim 传 **4m** | `cluster_add.go:134`；`simcluster:306,337` | 42 的 F 再 grow 走 CATCHING_UP 分支时等满（红路径） | (a) |
| `growLockReleaseAttempts`/`Backoff` | 4 / 1 s（~7 s） | `cluster_add_drive.go:578-581` | 只在 release 失败时 | 预算 |
| `growLeaseRenewInterval` = `cluster.LockLeaseRenewInterval` | 5 min（=TTL/3） | `cluster_upgrade_drive.go:207`；`internal/cluster/lock_lease.go:121` | 无人等 | — |
| `LockLeaseTTL` | 15 min | `internal/cluster/lock_lease.go:115`（推导 `:95-113`：≥ 8 min/(2/3)→12→15） | 30 的 `cluster unlock` 用 `--force`，不等；**drill 30 依赖 #31 泄漏的 marker 在 TTL 内存活**（`drills/lib/cluster.sh:19-20`） | **(d)** |
| `GrowLockReapInterval`/`UpgradeLockReapInterval`/`DrainMarkerReapInterval` | 30 s | `internal/broker/broker.go:907-914` | 过期 lease 的 marker 回收节拍；74 C-auto 的 "no in-flight op" 门 | (b) |
| `opCatchupTimeout`（#7 按 DB 大小放大，上限 30 min） | 2 min | `internal/broker/cluster_operation_controller.go:343,362,898-901` | **40 的 OPS-CONFIRM fixture 健康路径等满**（不可达 nonvoter → BLOCKED，`40-drain-retire.sh:183` poll 175） | (b)，带地板 |
| `opTopoConvergeTimeout` / `opRehomeConvergeTimeout` | 5 min / 10 min | 同文件 `:351,:357` | retire 的 NATS_ROLLED_OUT / REHOME 门，只在卡住时等 | 预算 |
| `topoReconcileInterval` | 5 s | `internal/broker/topology_reconcile.go:28` | reconciler 节拍 | (b) |
| `topoRestartBaseDelay` + `rank×topoRestartSpacing` | 12 s + rank×12 s | `topology_reconcile.go:35-36,195-197` | 不可 reload 的 route/auth 变更触发的 **错峰硬重启**（grow/retire 期间；是否在 grow 关键路径上——见"不确定"） | **(d)**（错峰即安全属性） |
| `clusteredJetStreamBootWait` / `Retry` | 90 s / 2 s | `internal/broker/broker.go:3057-3058` | joiner 冷启动等 JS：预算 / 2 s grain | 预算 / (b) |
| `cutoverGraceTimeout` | 45 s | `internal/broker/cluster_grow_cutover.go:26` | former-N1 SIGKILL nats 后等复活为 clustered（预算） | 预算 |
| `clusterReplicaObserveBase/PerStream/Ceiling`（G69 placement 证明） | 3 s / 250 ms / 30 s | `internal/broker/clusterwrite.go:673-689` | 每次 grow 的终态 SERVING 前；只在 UNOBSERVED 时打满 | (d) |
| `defaultCatchUpPoll` / `MaxWait` | 50 ms / 30 s | `internal/broker/clusteradmin.go:39-40` | leader 侧 catch-up 观测 | — |
| `ReconcileInterval` | 1 s | `broker.go:886-887` | 状态机 tick（node-states/ports…，`reconcile_passes.go:129-168`） | (b) |
| `HomeDeliverInterval` | 5 s | `broker.go:916-917` | 71 的 home 重投递（B-silent 数 ≥1 行，与节拍无关） | (b) |
| `homesConvergeTimeout` | 2 min | `cmd/tether/cluster_upgrade_drive.go:436` | drain 收敛预算（71 B-cmd rc=75 分支） | 预算 |
| systemd `RestartSec` | 2 s（nats-server、tether-broker、tether-agent） | `scripts/install.sh:1226,1261`；`image/units/tether-agent.service:19` | 每次 SIGKILL/crash 复活（grow cutover、95、97、33…） | (a)（部署件） |
| nats.go `DefaultReconnectWait` | 2 s | `nats.go@v1.52.0:56`（tether 未覆盖） | grow cutover 期间 CLI 连接掉线重连；95 的 agent 重连 | (b) |

### B · agent 存活 / 重连 / 身份

| 常量 | 生产值 | 位置 | 等它的 drill / 站点 | 类别 |
|---|---|---|---|---|
| `HeartbeatInterval` | 5 s | `internal/agent/agent.go:748-749` | 所有 ONLINE 判定的基础 | (b) |
| `DefaultStaleAfter` / `DefaultOfflineAfter` | 5 s / 60 s | `internal/node/node.go:33-34`（`broker.go:889-894` 只能由 Config 覆盖，无 yaml） | **94 A1c、96 F 健康路径各等满 60 s**；60/31/97 只等 STALE（5 s） | (b)，地板 ≥4×heartbeat |
| nats.go `DefaultPingInterval` / `DefaultMaxPingOut` | 2 min / 2 | `nats.go@v1.52.0:60-61`；tether **未设置**（`agent.go:2221-2260` `buildConnOptions`）；stale 判定 `processPingTimer`：第 3 个未答 ping 才断（`nats.go@v1.51.0:5765-5786`）→ **4–6 min**（drill 98 头注写"up to 4min" `98-stuck-redial-recovery.sh:31`，实际上限是 6 min） | **98 的 detection 段健康路径必等** | **(c)** |
| nats-server `ping_interval` / `ping_max`（服务端） | 2 min / 2（nats 默认） | **不在** `internal/natsconf/preflight.go:42-56` 的 passthrough 表 → clustered 节点的 nats.conf 不能设 | 98 的 impact 谓词读 `/connz`（`98:66-72`），由**服务端** ping 决定 | (b)，需先改产品 |
| `redialAfter` | 20 s | `internal/agent/roster.go:31` | 98（detection 之后） | (b) |
| `closeBudget` / `poisonGrace` | 10 s / 10 s | `internal/agent/conn_teardown.go:54,57`（"VALUES are a contract … #72 / usage.md §9.9"） | 98 | **(d)** |
| `defaultRosterRefreshInterval` / `FailBackoff` / `maxSilentRosterRefreshes` | 3 min（全抖动）/ 20 s / 3 | `roster.go:24,27,41` | **41 的 #48 沉默逃逸健康路径 ≤225 s**（`41:270-282`，poll 300）；82 的 `roster_gen` 收敛 | (b) |
| `rosterStaleGrace` | 6 min | `internal/broker/roster_stale.go:24`（=5 min SLA+1） | 82 `agent_roster_stale` **NOT-COVERED**（没人等 6 min） | (b)，随上项派生 |
| `RegisterTimeout`/`RetryInitial`/`RetryMax` | 10 s / 100 ms / 2 s | `agent.go:751-756` | 注册重试 | (d) |
| `leaseSubscribeSettle` = `leaseGrantWindow`；`probeTTL`；`backgroundProbeBudget` | 5 s；10 s；3 s | `broker.go:1609,1822,1592,1597` | 83/84（drill 83 试金石本体，`broker.go:1790-1810`） | **(d)** |
| `leaseRefusalBackoffMax` | 60 s | `internal/agent/instance.go:544` | 83/84 clone 拒绝退避 | (d) |
| proxy 首拨退避 base/max/deadline | 500 ms / 30 s / 2 min | `agent.go:1939-1941` | **78 Arm A 用 netfilter 计数器测这条曲线** | **(d)**（压了就在测另一条曲线） |
| `ProxyFailClosedGrace` | 15 min | `internal/agent/proxy.go:1013-1016`（Config 字段，无 yaml） | 无 drill 等它 | (b) |
| courier backoff / lifetime | 2 s–60 s / 24 h | `internal/agent/proc_delivery.go:57-61` | — | (d) |
| `agentProxyDialTimeout` / ssproxy handshake·dial | 10 s / 10 s / 10 s | `agent.go:1252`；`ssproxy/server.go:22-23` | 72/73/74 的 SS 腿只在失败时等 | 预算 |

### C · 升级

| 常量 | 生产值 | 位置 | 等它的 drill / 站点 | 类别 |
|---|---|---|---|---|
| `upgradeRegisterDeadline` | 120 s（"A constant, not a flag"） | `internal/agent/upgrade_state.go:62-67` | **33 Arm B 健康路径等满 120 s**（`33:244` poll 210） | (b)，地板 ≥30 s |
| `upgradeWaitBudget` / `upgradeWaitPoll` | 150 s / 3 s（var，"120 s + slack"） | `cmd/tether/node.go:400-409` | 33（`--wait` 路径）；31 `--timeout 0` | (b)，随上项派生 |
| `upgradeFetchTimeout` / `upgradeSmokeTimeout` | 30 s / 5 s | `internal/agent/upgrade.go:56,760` | 33 的 staged 线前 | 预算 |
| `upgradeConvergeTimeout`/`Poll`/`TriggerTimeout` | 3 min / 3 s / 10 s | `cmd/tether/cluster_upgrade.go:32-34` | 30 的 roll | (b) poll |

### D · force-single / 告警 / 观测 / 回收

| 常量 | 生产值 | 位置 | 等它的 drill / 站点 | 类别 |
|---|---|---|---|---|
| `forceSingleDwell` | 15 s（= max(15 s, 10×election)） | `internal/broker/force_single_online.go:24-27` | 22 Arm-0 与 #35 fixture、92 各等 ~20 s（dwell + observe tick；`simcluster:552-556`、`92:88`） | **(d)** |
| `forceSingleArmTTL` | 60 s | `force_single_online.go:30` | **22 `sleep 61`**（`22:154`，唯一真时钟过期接线探针） | (b) |
| `forceSingleLeaderWait` | 5 s | `:33` | 预算 | 预算 |
| `observeTickInterval` / `observePollWindow` | 5 s / 2 s | `internal/broker/observability.go:224-225` | 90 broker_down/below_quorum 的升/清；74 的 return dwell 以 tick 计 | (b) |
| `autoRebalanceReturnDwellTicks` / `autoRebalanceQuietWindow` | 6 tick（=30 s）/ 60 s | `internal/broker/proxy_auto_rebalance.go:23-26,97` | **74 Arm C**（`74:555-572`：30 s dwell + 60 s quiet + eligibility ≈ 180 锁定窗；今天 #34 开放 → 等满） | (b)，dwell 地板 > `proxyRehomeDwell`=3 tick |
| `jsDownThreshold` | 60 s | `internal/broker/alert_reconcile.go:95` | 92(b) sustained-503 banner（不出现即 not_covered） | (b) |
| `DiskCheckInterval` | 5 min | `internal/broker/disk.go:23`；yaml `disk_check_interval`（地板 1 s，`serveconf.go:66`）；flag `serve.go:371` | **90 M6 用 `systemctl restart` 触发启动采样来绕过它**（`90:196-203`）——周期采样路径在 deploy tier **从未被测** | **(a)** |
| `ProcGCInterval` | 5 min | `broker.go:898-899`；yaml `proc_gc_interval`（`serveconf.go:173`） | 94 G.5 端口回收面 | (a) |
| `XferReapInterval` | 5 min | `broker.go:901-902`；yaml `xfer_reap_interval`（地板 1 s、上限 24 h，`serveconf.go:256-282`） | **96 已设 8 s**（`96:331-349`） | (a) |
| `XferCrossHomeReapAge` | 3×tierB = 15 min，yaml **只能上调** | `internal/broker/transfer.go:81`；`serveconf.go:287-310`（外审 F2） | 96 的 split-home 分支因此不可观测（`96:497-509`） | **(d)**（唯一明文裁决先例） |
| `xferReapMinObjectAge` | 2 min | `internal/broker/transfer_reconcile.go:15-19` | 96 #58：今天靠 390 s 窗自然满足；**压缩 96 后它会成为绑定项** | (d)（in-flight 保护） |
| `XferTimeoutTierA` / `TierBFloor` / `OverheadMargin` / `TierBMaxBudget` | 30 s / 5 min / 60 s / 35m08s | `internal/proto/xfer.go:58,61,76,80` | 96 的 390 s 窗 = 5 min + 90 s（`96:393-396`）；`cliTransferTimeoutDefault` = 35m08s+2m（`cmd/tether/transfer.go:742`） | (d) |
| `xferProvisionBudget`/`CreateAttemptTO`/`SizingTimeout` | 8 s / 2.5 s / 1.5 s | `internal/broker/xfer_provision.go:35-46` | 67 的 `jetstream_not_ready` 拒绝（8 s 内返回） | (d) |
| `xferStrandedSlack` | 60 s | `internal/broker/xfer_inflight.go:37` | 96 finalize-on-recovery | (d) |

### E · raft 与读围栏

| 常量 | 生产值 | 位置 | 等它的 drill / 站点 | 类别 |
|---|---|---|---|---|
| `MultinodeHeartbeatTimeout` / `ElectionTimeout` / `LeaderLeaseTimeout` | 1000 ms / 1000 ms / 500 ms | `internal/cluster/node.go:66-73` | 96 D2 新 leader、97 heal 后 one-leader、90 kill follower（各 1–2 s） | **(d)**（testing-standards T2） |
| `TFence` / `defaultApplyTimeout` | 10 s / 10 s | `internal/cluster/read.go:18`；`node.go:22` | 读围栏 / apply 预算 | (d) |

### F · 不是产品常量、但健康路径同样等满的 sim/drill 侧固定等待（供对照）

| 等待 | 值 | 位置 |
|---|---|---|
| agent-join 绑定固定 `timeout 6` | 6 s × ~48 次 ≈ 288 s | `simcluster:501`（accel plan V4(b) 明确"保留固定等待"） |
| 96 的 #57 负窗 | 390 s（brk2 整窗宕机，**任何结果都不可能出现**） | `96:393-396` |
| 96 canary3 settle（`\|\| true` 负轮询） | 60 s | `96:647` |
| 97 `SOAK_SETTLE` × (1 baseline + 6 cycles + 1 quiesce) | 25 s × 8 = 200 s | `97:10,108,247,322,380` |
| 95 StartLimitBurst 错峰 | 20 s × 2 | `95:176,200` |
| 全套阻塞 `sleep` | 175 s（36 处，accel plan §1.1） | 其中 22 占 73 s |

## 分类与试金石论证

试金石（drill 83）：**压缩一个时间常量是否让某类 bug 从可见变不可见。** 对每个 (b)/(c) 候选给出判断；(d) 只给一句为什么。

### (c) — 缩短对产品有独立价值

**1. `joinerBootGrace`（60 s → fresh joiner 直接 HALT）。** 注释自述该 grace 只覆盖两种 *daemon 已被 provisioning 启动过* 的瞬态（`cluster_add_drive.go:686-691`）。而对一个 **fresh joiner**，本次调用刚在 P2 跑过 `runSelfInit`（`:117-124`，`raft/` 之前不存在）；产品自己的 #I1 不变量（cluster-mode `serve` 无 raft 状态即拒绝，drill 11 `assert_refuses 'no raft state exists'`）保证此前**不可能**有 cluster-mode broker 在跑，standalone broker 即使在跑也只会答 `cluster_not_enabled` 而永远不满足 `adminStatusIsClustered`（`:722-724`）。所以"本次调用 bootstrap 了 raft/ ⇒ 跳过 grace 立即 HALT"是由产品不变量证明的，不是猜测。试金石：grace 覆盖的 bug 类是"returning node 正在 crash-restart"（drill 42 3/4 次失败的那条，`:697-704`）——它只在 `raft/` 已存在的路径上出现，该路径**保留 60 s**。独立价值：operator 每次首 grow 少等 60 s 才看到 HALT 提示。代价：零；一条 `initRanThisInvocation` 布尔。备选做法 `--joiner-boot-grace`（(b)）也合法但把知识推给调用方。

**2. agent 侧 `nats.PingInterval`/`MaxPingsOut`（未设 → 例如 20 s / 2，detection ≤ 60 s）。** 独立价值：#72 事故的本质是半死链路，产品修的是 teardown 有界（≤60 s），但**发现**半死仍要 4–6 min——这是运维可见的延迟。试金石：98 要看的 bug 类（teardown 楔死、redial watchdog、在存活 voter 上重注册）**全部在 detection 之后**，detection 快慢不改变它们的可见性；反过来，一个"agent 自身无应用层活性探测、完全依赖库 ping"的设计缺陷在两种取值下都同样可见。新增风险（生产真实）：>3×interval 的 GC/网络停顿会被判 stale 触发一次重连+重注册，20 s×3=60 s 容忍度在 WAN 上可接受，属产品取舍。**注意**：98 的 impact 谓词是服务端 `/connz` 消失（`98:66-72`），由 nats-server 的 `ping_interval`/`ping_max` 决定；那两个键**不在** natsconf passthrough 表（`preflight.go:42-56`，未知键 fail-closed 拒绝 takeover）——要压 98 必须两侧一起，服务端那半是先改产品再变 (b)。

### (b) — 可合法变配置项，小值是合法生产配置

**3. `DefaultOfflineAfter`（60 s；建议地板 ≥4×heartbeat=20 s）。** 94/96 各等满 60 s。试金石：missed-exit / orphan 两个 G.1 方向的 bug 类与阈值无关；但阈值贴近 heartbeat 时，`-j 12` 下一次 >阈值 的心跳延迟会把活 agent 判 OFFLINE，G.1 会把它的活进程 reconcile 成 `EXITED(-1)`——这是 **sim 制造的、生产不存在的假红**（不是隐藏 bug，是制造 bug）。20 s 保留 4 个心跳余量。

**4. `upgradeRegisterDeadline`（120 s；建议 ≥30 s）。** 33 Arm B 的分支由 SYN-block 保证注册不可能成功，阈值只决定 rollback 何时发生，claim（whole-conjunction rollback、`.prev` consumed、同 PID）不变。试金石：注释里"honest binary loses the race on a weak NAT"这类假回滚在小值下**更容易出现**而非被隐藏；风险在 Arm A（真成功）：re-exec 三次 + 注册必须在阈值内完成，负载下 30 s 有余量、10 s 没有。`upgradeWaitBudget` 是它的派生（`node.go:400-405`），必须同步。

**5. `forceSingleArmTTL`（60 s）。** 22 的 claim 是"token 过期后 commit 被拒 + 单次消费"，与 TTL 值无关；生产下 TTL 要容纳人工 TTY 确认，30 s 仍合法。`sleep 61` 变 `sleep TTL+1`。它是 accel plan R8 点名的"唯一真时钟过期接线探针"——保留探针，缩短 TTL 不改变探针性质。

**6. `defaultRosterRefreshInterval`（3 min）+ `rosterStaleGrace`（6 min，派生）。** 41 的 #48 逃逸 ≤3×interval 健康路径必等（drill 注释明说"stock 3-min … NOT shortened — the mandate forbids tuning"，`41:277-278`——本次前提下可重审）。试金石：41 用**机制**（`rosterRequiresReconnect` / `rebuildOnBrokerSilence` 日志行）区分 fast path 与 silence path，不靠时序，故安全；82 的 `agent_roster_stale` 今天 NOT-COVERED 正是因为没人等 6 min——两常量一起压到 30 s/60 s 反而**新增覆盖**。风险：refresh 是对 broker 的请求，30 s 节拍在大车队上是真实负载，属可配置而非默认。

**7. `autoRebalanceReturnDwellTicks`/`autoRebalanceQuietWindow`（30 s/60 s）。** dwell 的语义是 tick 数（flap 取消逻辑按 tick，`proxy_auto_rebalance.go:71-82`），压 `observeTickInterval` 5 s→2 s 即等比缩短且保留 flap 过滤；quiet window 需 ≥ rehome 结算时间（否则在测"两个 mover 打架"这条另外的分支）。今天 74 C-auto 因 #34 等满 180 s，属红路径。

**8. `growConvergePoll`（3 s）。** 纯节拍。**必须**把 `cutoverBroker` 的 "6 次×grain" 改成时间窗（≥ RestartSec 2 s + nats 冷启），否则 500 ms×6=3 s 内 SIGKILL 的 nats 还没复活，重试环耗尽后按 transport-error 放行，把 cutover 的证据推给后面的 catch-up 预算——外审 M2 的"稳定拒绝不得被吞"依赖 ≥2 次 poll，仍成立。

**9. `opCatchupTimeout`（2 min；建议 ≥60 s）。** 40 的 OPS-CONFIRM claim 与值无关。风险：它同时是所有 grow 中 stuck joiner 的 BLOCKED 后备；30 s 会让 `-j 12` 下一个慢而健康的 joiner 进 BLOCKED → C1 auto-confirm 路径 → 假红。#7 已因假 BLOCK 把它按 DB 大小放大过一次，方向相反的教训在案。

**10. `observeTickInterval`/`HomeDeliverInterval`/`ReconcileInterval`/`ReconnectWait`/`GrowLockReapInterval`。** 全是节拍，无 claim 依赖具体值；`GrowLockReapInterval` 只回收**过期** lease，#31 的可见性由 `LockLeaseTTL` 决定（见 (d) 11），压它不隐藏 #31。

**11. `jsDownThreshold`（60 s）。** 92(b) 因 sustained-503 不出现而 not_covered；10 s 阈值更可能观测到 → 覆盖增加。地板 ≥ meta re-form 抖动（数秒）。

### (a) — 今天已是配置项（且用它反而增覆盖）

`xfer_reap_interval`（96 已用 8 s）、`disk_check_interval`（**90 今天用重启触发启动采样，等于从未在 deploy tier 测过周期采样路径**；设 5 s 后 M6④/⑤ 不再需要 restart，且测到真正的 monitor 循环）、`proc_gc_interval`、`RestartSec`、`cluster add --timeout`、`SOAK_CYCLES`/`SOAK_SETTLE`（结构地板 `LEAK_MIN_N`，`97:358`）。

### (d) — 内在不可压缩

- **`LockLeaseTTL` 15 min**：drill 30 **依赖** #31 泄漏的 marker 在 TTL 内存活来钉 upgrade-blocked（`cluster.sh:19-20`）；压到 1 min 则 marker 在 30 的 roll 前就自愈——**试金石失败的教科书例**。
- **`forceSingleDwell`**：=10×election 的瞬时分区防御；压它就在测更弱的守卫。
- **`leaseGrantWindow`/`probeTTL`**：试金石本体（`broker.go:1790-1822` 两个方向各一次事故）。
- **raft 三常量**：T2 规则；改了就改 stale-leader 窗（§3.2）。
- **`closeBudget`/`poisonGrace`**：已发布契约 #72。
- **`XferCrossHomeReapAge`**：外审 F2 raise-only。
- **`XferTimeoutTierBFloor` 等 transfer 预算**：安全预算；96 的 390 s 是 drill 从它派生的死窗，但那 390 s 里 brk2 整窗宕机，**产品在窗内什么也不会做**——它是 drill 预算不是产品常量（归 L4）。
- **`xferReapMinObjectAge` 2 min**：in-flight 保护；今天被 390 s 掩盖，96 一旦压缩它就是绑定项。
- **proxy 首拨退避曲线**：78 用包计数器测的就是这条曲线的形状。
- **`topoRestartBaseDelay`/`Spacing`**：错峰本身是"永不同时弹所有 voter"这条安全属性；sim 里压到 3 s 会制造生产不会有的 JS-meta 失 quorum 假红。

## 两张地板表

**约定与推导基元**（用户 2026-09-18 实测 + 源码）：
- 今日 spine：`up` 5 s / `init` 10 s / `grow brk2` 176 s（add1 97 = **60 grace** + 37；env restart+ctl-ready ≈12；add2 67）/ `grow brk3` 114 s（add1 80 = 60 + 20；≈12；add2 22）。S3（N=3）= 305 s；S2（N=2）= 191 s；S1 ≈ 15 s + 7 s/agent。
- 只压 (c)：每 grow −60 → S3 = 185 s，S2 = 131 s（**推导，非估计**）。
- 压 (a)+(b)+(c)：再压 `growConvergePoll`/cutover 重试窗/`clusteredJetStreamBootRetry`/`RestartSec`/`_grow_ctl_ready` grain；残余是 nats 冷启 + 2 节点 JS meta 选举 + raft AddNonvoter/catch-up/AddVoter + G69 placement 观测。**估计** grow brk2 ≈ 40 s、brk3 ≈ 25 s → S3 ≈ 80 s，S2 ≈ 55 s（这一步是估计：add2 的 67 s 内部无日志时间戳可分摊，见"不确定"）。
- 今日 p50 取 `drill-costs.tsv`（2026-07-23 种子，**已陈旧**：67 是 pre-G67 的 777 s；用户实测的 grow 176/114 与种子时代不同），33/98 取 README 实测。
- "非 L1 残余" = drill 自身的死窗 / 红路径窗，L1 手段够不到，单列出来以免被当成产品地板。

### 表 1 · 只压 (c)（`joinerBootGrace` fresh-joiner 跳过 + agent `PingInterval` 20 s/2 + 服务端 `ping_interval` passthrough）

| drill | 今日 p50 | 关键组成（健康路径必等） | (c) 后地板 | 推导 |
|---|---|---|---|---|
| 96 | 1337 | S3 305 + 2 AJ 14 + 1 GiB 推送 + **390 死窗** + brk2 重启 + **#57 判定环 ≤180（红路径）** + #58 + D 臂（选举/300 窗内 JS 重组/heal）+ **canary settle 60** + F 臂 **OfflineAfter 60** | **≈1217（20.3 min）** | −120（2 grace）；ping 不在路径 |
| 98 | ≈700 | S3 305 + AJ 7 + **detection 240–360** + 梯子 40 + 重注册 + heal 60 poll | **≈340（5.7 min）** | −120；detection 300→≤60（−240） |
| 90 | 775 | 三个 spine：S3 + S2(M6) + S3-like(M8) = **5 grow** + 臂 ~60 | **≈475** | −300 |
| 74 | 653 | S3 + 3 AJ + eligibility(~60–90) + 1/1/1 构造 + 3×SS 基线 + skew/return + **B-dp 240 与 C-auto 180 今天因 #33/#34 等满** | **≈533** | −120 |
| 41 | 572 | S3 + 2 retire + **#48 逃逸 ≤225** + JS reset + tier-B | **≈452** | −120 |
| 97 | 568 | S3 + 2 AJ + **settle 200** + 6 cycle（~30 s/cycle） | **≈448** | −120 |
| 40 | 565 | S3 + AJ + drain + **opCatchupTimeout 120** + retire spine | **≈445** | −120 |
| 33 | 540 | S1 + 双 agent unit + 工件 + **watchdog 120** + Arm A/C2 | **540** | 无 (c) 项 |
| 22 | 477 | S2 ×2（Arm-0 + #35 fixture）+ 2×dwell ~20 + **sleep 61** | **≈357** | −120 |
| 51 | 467 | S3 + 备份/全失/fresh box + I-prep grow（3 grow） | **≈287** | −180 |
| 71 | 428 | S3 + AJ + tunnel 建立 + drain + kill/start 恢复 | **≈308** | −120 |
| 73 | 419 | S3 + 2 AJ + eligibility + **#33 180 今天等满** + quorum 臂 | **≈299** | −120 |
| 93 / 30 / 91 / 10 / 13 | 392 / 378 / 376 / 305 / 266 | S3 主导 | 272 / 258 / 256 / 185 / 146 | −120 |
| 42 | 344 | S2 + 修复序列 + F 再 grow（红路径：grace 60 + ctl-ready 60） | **≈224** | −120（含红路径的那 60） |
| 92 / 95 / 52 / 50 / 82 / 20 / 12 / 11 / 67 | 291 / 274 / 234 / 227 / 196 / 186 / 174 / 152 / (777 陈旧) | S2 主导 | 231 / 214 / 174 / 167 / 136 / 126 / 114 / 92 / ≈190 | −60 |
| N=1 家族（00 21 31 32 43 60 61 62 70 72 78 80 81 83 84 94） | 32–133 | 无 grow | **不变**（94 含 OfflineAfter 60，是 (b)） | — |
| **Σ** | ≈13060 s（218 min，含 83/84 各估 60 s） | | **≈10000 s（167 min）** | −2820 grace − 240 (98) |
| **wall 地板 = p_max** | 96 ≈ 21 min | | **96 ≈ 20 min** | (c) 对 wall 几乎无效 |

### 表 2 · 压 (a)+(b)+(c)

| drill | (c) 地板 | 额外压缩项 | (a)(b)(c) 地板 | 其中非 L1 残余（drill 死窗 / 红路径） |
|---|---|---|---|---|
| 96 | 1217 | S3 −105；OfflineAfter 60→20（−40） | **≈1070（17.8 min）** | 390 死窗 + 60 canary + 180 #57 判定环 = **630**；扣除后产品绑定 ≈ 440 s（**估计 ~7 min**） |
| 98 | 340 | S3 −105 | **≈235（3.9 min）** | 梯子 40（契约） |
| 90 | 475 | 三 spine → 80+55+80；`disk_check_interval` 替代 restart | **≈295** | 0 |
| 74 | 533 | S3 −105；dwell+quiet 90→30 | **≈370**（#33/#34 开放）；产品修好后 **≈250** | B-dp 240 + C-auto 180 红路径 |
| 41 | 452 | S3 −105；refresh 3 min→30 s（#48 逃逸 225→~70，−155） | **≈190** | retire 错峰重启（(d)，未量化） |
| 97 | 448 | S3 −105 | **≈343**；`SOAK_SETTLE`=10 再 −120 → ≈220 | settle 是 (a) 但受 leak 斜率结构地板约束 |
| 40 | 445 | S3 −105；opCatchup 120→30（−90） | **≈250** | 0 |
| 33 | 540 | watchdog 120→30 + wait budget 派生（−105） | **≈435** | 工件/双 unit 供给是 sim 侧 |
| 22 | 357 | 2×S2 −152；armTTL 60→30（−30） | **≈175** | dwell (d) |
| 51 | 287 | S3 −105，S2-like −76 | **≈150–200（估计）** | DR 手工步骤是真活 |
| 71 / 73 | 308 / 299 | S3 −105 | **≈200 / ≈195** | 73 的 #33 180 红路径（健康时 ≈40） |
| 93 / 30 / 91 / 10 / 13 | 272 / 258 / 256 / 185 / 146 | S3 −105 | 167 / 153 / 151 / 80 / ~60 | 0 |
| 42 | 224 | S2 −76；再 grow 也 −40 | **≈120** | ctl-ready 60 红路径 |
| 92 / 95 / 52 / 50 / 82 / 20 / 12 / 11 / 67 | 231 / 214 / 174 / 167 / 136 / 126 / 114 / 92 / 190 | S2 −76（11 −40） | 155 / 138 / 98 / 91 / 60 / 50 / 40 / 50 / ~115 | 0 |
| 94 | 133 | OfflineAfter −40 | ≈93 | 0 |
| **Σ** | 167 min | 26 个 N≥2 drill × 平均 ~95 s | **≈125 min（估计）** | |
| **wall 地板 = p_max** | 20 min | | **96 ≈ 18 min**；若 L4 拆掉 96 的 630 s 死窗 → **≈7 min**；再把 96 按臂拆成独立 drill（共享 N=3 fixture）→ p_max ≈ S3 80 + 单臂 ≈ **3–4 min** | |

**结论（L1 视角）**：L1 全部手段把 Σ 从 ~218 min 压到 ~125 min（−43%），但 wall 只从 ~21 min 到 ~18 min，因为 p_max=96 的大头是 drill 自身死窗（390+60+180）而非产品常量。"≤5 min wall" 在 L1 内**做不到**；它同时需要 (i) L4 拆死窗、(ii) 拆 96/98/33/74 为按臂独立 drill、(iii) `j ≥ 20`（Σ/j = 125/20 ≈ 6.3 min，仍略超 5 min；且 `-j 12` 已多两条 load-sensitive 偏离，#70 grow 并发未解）。即便全做到，单臂产品绑定地板 ≈ 3–4 min（S3 ≈ 80 s 是估计中最不确定的一项）。

## 健康路径必等窗口

区分三类：**P 产品常量健康路径必等**（正常也要等满）、**D drill 自身固定窗**（正常也等满，但不是产品常量）、**R 红路径**（只因缺陷开放才等满；`poll_until` 的 timeout 只在红时等满——**这一类 `poll` 预算不计入**：全套 351 个 poll 站点的 timeout 总和是失败上限，不是健康成本）。

| 类 | 窗口 | 单次 | 次数 | 合计 | 出处 |
|---|---|---|---|---|---|
| P | `joinerBootGrace` | 60 s | **47 次 grow**（10:2 11:1 12:1 13:2 20:1 22:2 30:2 40:2 41:2 42:2 50:1 51:3 52:1 67:1 71:2 73:2 74:2 82:1 90:**5** 91:2 92:1 93:2 95:1 96:2 97:2 98:2；71/73/74 的 retry=1 失败重试再 +2） | **2820 s** | `cluster_add_drive.go:692` |
| P | nats.go ping detection + 梯子 | 240–360 + 40 | 1（98） | ≈340 s | `nats.go:60-61`；`conn_teardown.go:54,57` |
| P | #48 沉默逃逸（3 min 抖动 + 2×20 s） | ≤225 | 1（41） | ≈200 s | `roster.go:24,27,41` |
| P | `upgradeRegisterDeadline` | 120 | 1（33） | 120 s | `upgrade_state.go:67` |
| P | `opCatchupTimeout` | 120 | 1（40） | 120 s | `cluster_operation_controller.go:343` |
| P | `DefaultOfflineAfter` | 60 | 2（94、96） | 120 s | `node.go:34` |
| P | auto-rebalance dwell+quiet（产品修好后） | 90 | 1（74） | 90 s | `proxy_auto_rebalance.go:26,97` |
| P | `forceSingleDwell` + observe tick | ~20 | 3（22×2、92） | ≈60 s | `force_single_online.go:27` |
| P | `RestartSec` | 2 | ~30 次复活（估计） | ≈60 s | `install.sh:1226,1261` |
| P | observe tick 升/清 | 5–7 | ~4（90） | ≈25 s | `observability.go:224-225` |
| P | `growConvergePoll` grain 超调 + cutover 重试环 | 5–10（估计） | 47 | ≈250–450 s（估计） | `cluster_add.go:31` |
| **P 小计** | | | | **≈4000–4200 s ≈ 68–70 min（占 Σ 218 min 的 ~32%）** | |
| D | 96 #57 死窗 + canary settle | 390 + 60 | 1 | 450 s | `96:396,647` |
| D | agent-join 固定 `timeout 6` | 6 | ~48 | 288 s | `simcluster:501` |
| D | 97 `SOAK_SETTLE`×8 | 25 | 8 | 200 s | `97:247,322,380` |
| D | 阻塞 `sleep`（含 22 的 61） | — | 36 | 175 s | accel plan §1.1 |
| D | 95 StartLimit 错峰 | 20 | 2 | 40 s | `95:176,200` |
| **D 小计** | | | | **≈1150 s ≈ 19 min** | |
| R | 96 #57 判定环（30×6 s） | ≤180 | 1 | 180 s | `96:418-423` |
| R | 74 B-dp 240 + C-auto 180（#33/#34） | 420 | 1 | 420 s | `74:407,572` |
| R | 73 #33 measure-and-record | 180 | 1 | 180 s | `73:298` |
| R | 42 F 再 grow（grace 60 + ctl-ready 60） | 120 | 1 | 120 s | `42:222`；`simcluster:327` |
| **R 小计** | | | | **≈900 s ≈ 15 min** | |

**健康路径必等总量 ≈ P + D ≈ 87–89 min，约占今日 Σ 的 40%；其中 joinerBootGrace 一项占 47 min（P 的 ~68%）。** 单 drill 关键路径上的必等最大值：90（300 s grace）、98（120 + 340 = 460 s）、96（120 + 60 + D 450 + R 180 = 810 s）。

## 我不确定的地方

1. **add2 的 67 s（brk2）/ 22 s（brk3）内部分摊。** 用户给的是整段数字；`waitJoinServing` 内 joiner 冷启（`clusteredJetStreamBootWait` 循环）、2 节点 JS meta 选举（nats-server 内部计时，非 tether 常量）、G69 placement 观测（≤30 s）、错峰 nats 重启（`topoRestartBaseDelay` 12 s + rank×12 s，是否在 grow 路径上触发我没有证据）各占多少无日志时间戳可证。表 2 的 S3 ≈ 80 s 是**估计**，误差可能 ±40 s；这直接决定"拆臂后单臂 3–4 min"这一句的成立与否。
2. **用户所述"第二次调用含 former-N1 cutover + JS reset + nats restart"与源码顺序不符**：`cutoverBroker` 在 P5（`cluster_add_drive.go:186-193`）、grace 在 P3b（`:203`）之后才是 `waitJoinServing`，即 cutover 应落在第 1 次调用的 37 s 里。可能是 cutover 的 SIGKILL 回复丢失后 nats 复活跨越了两次调用的边界；不改变 grace 的结论，但影响 add2 的分摊。
3. **drill 98 的 detection 上限**：源码算出 4–6 min（第 3 个未答 ping），drill 头注写 "up to 4min"、预算 330 s。若真实分布落在 5–6 min，330 s 预算会按相位随机失败——这是对 98 稳定性的一个未经实测的推断（用户说 700 s 全程、未说失败率）。
4. **`drill-costs.tsv` 陈旧**（2026-07-23 种子）：67 的 777 s 是 pre-G67；96 是 pre-G69/R3-F3 改造前；83/84 不在表里。表 1/2 的"今日 p50"应以下一次 sweep 的 `rollup.tsv duration_s` 重置。
5. **grow 次数 47** 按源码站点静态数出（`grow_to_3`/`grow_to_2`/`setup_forcesingle_n2` 展开），未计 71/73/74 retry=1 的失败重试与 90 M6 在 `--cap-store` 下的差异；±2 次量级。
6. **agent-join 次数 ~48** 是站点数，个别站点在循环里执行多次（81、82），实际执行数可能更高。
7. **(b) 项的"合法生产配置"判断**是我按常量注释与外审先例推的，唯一有明文裁决的先例是 `XferCrossHomeReapAge`（F2，raise-only）与 `upgradeRegisterDeadline` 的 "no tunables without a use case"（plan §0）；`OfflineAfter`/`opCatchupTimeout` 的地板值（20 s / 60 s）没有实测支撑，只是心跳倍数推理。
8. **服务端 `ping_interval` passthrough** 是否会被 natsconf 的 takeover/reconcile 在 re-render 时保留，我只读了 `bucketOf` 表；`BuildMergedConf` 对 passthrough 键的重发路径未逐行核对。
9. 表 2 的 Σ ≈ 125 min 是把 26 个 N≥2 drill 的 spine 节省按平均值叠加得到的**估计**；每个 drill 的 body 部分我没有逐臂计时依据，只对 96/98/90/74/41/40/33/22 做了逐项分解。


---

# 草案 L2 集群夹具供给

## 并行 grow

**结论：brk2 与 brk3 的 `cluster add` 不能并行，串行是产品不变量，不建议放松；但 grow 内部有 ~55 s/次的纯死等可以诚实地去掉，且 joiner 的 [env] 供给步骤可与前一个 grow 重叠。**

1. **产品层的串行化是硬的、且是有意的。**
   - `PlanSetGrowActive`（`internal/cluster/membership_ops.go:503-517`）：acquire 是**条件写**——`NOT upgradeMarkerExists AND NOT growMarkerHeldByOther(joiner)`，外审 H1 专门把 check-then-write 竞争关掉；同一 joiner 幂等（resume），不同 joiner 直接拒。`driveAdd` 在 P1 就 acquire（`cmd/tether/cluster_add_drive.go:96-101`），P9 才 release（`:230`）。brk3 的第一次调用在 brk2 release 前必被拒在 `acquire-lock`。
   - 除锁之外还有结构性顺序依赖：former-N1 cutover 只在 `votersBefore == 1` 时做（`:192-199`），JS meta 是 1→2→3 逐步形成，raft 也只允许一个 pending 配置变更。两个 joiner 同时 AddNonvoter 意味着 cutover/R3 committed-config gate 的语义要重写。
   - 这把锁本身就是被测对象：drill 30/40/41 的 #31/#38/#45 暴露臂钉的正是「grow 后残留 lock 阻塞 upgrade/retire」。放松它 = 删掉一个传感器，换来的收益只有 brk3 那一次 grow（去掉死等后 ≈59 s）能与 brk2 重叠。**不值。**

2. **可以重叠的是 [env] 部分（Mandate ③ 允许）**：brk3 的 secrets mint/distribute、standalone 首启建 tether.db（`simcluster:262` 的 20 s poll）、broker.yaml seam、`admit_creator`+`session create`+`login`，合计约 12 s（= 176 − 97 − 67），可在 brk2 的 `cluster add` 期间做完。估计 −10 s/N=3。

3. **真正的大头：`joinerBootGrace` 的 60 s 死等（`cluster_add_drive.go:692`）。**
   - 机理：invocation 1 在 P5 已因 `!joinerBrokerUpLocal` 渲染了 joiner 的 clustered conf（`:176-191`），两步后进 P3b `awaitJoinerBrokerUpLocal`（`:709-729`），每 2 s 探一次本地 admin socket，满 60 s 才打印 `startJoinerHint` 并 HALT rc=75。注释说 grace 是为「provisioning **已经**启动了 daemon、但它正在 crash-restart」的 resume 场景加的（drill 42 3/4 次翻车的修法）。第一次调用时 daemon 是被设计停掉的（cluster add 从不跑 systemctl），所以这 60 s 每个 grow 都白等。
   - **诚实的 sim 侧修法（不改产品、不做 tether 的活）**：把 invocation 1 放后台、tail 它的输出；产品在进入 grace 前恰好打印一行进度 `… waiting up to 60s for <j>'s broker to serve cluster status (provisioning just restarted it)`（`:717`）——这一行**只在 P5 完成之后**出现（conf 已渲染、former-N1 已 cutover），是「现在可以启 daemon」的精确信号。sim 见此行即 `systemctl restart nats-server && start tether-broker`（今天 `simcluster:321-322` 做的同一件事，只是提前了 55 s）。driver 的 2 s 轮询随即看到 clustered status → 同一调用继续 P6 catch-up → SERVING → release → rc=0。这正是注释里 grace 为之设计的那种「daemon 已被 provisioning 启动」情形；产品行为路径没有新增任何东西。
   - 代价与收益：估计 −55 s/grow（60 s 减去 daemon 起到 socket 应答的几秒），N=3 −110 s，N=2 −55 s。第二次调用（67 s / 22 s）随之消失或退化为「already a VOTER with no live join op — grow complete」的幂等路径（`:77-92`）。
   - **必须保留的覆盖**：`cmd_grow` 今天把 rc=75 + `PAUSED at start-joiner` 签名当作唯一合法结果（`simcluster:311`）。改成双模式（`halt-resume` 与 `concurrent-start`）后，HALT→resume 契约（它自己就是 deploy-tier 发现的 drill 42 修法的产物）只能由**一个**指定 drill（建议 10 或 11 加一臂）显式走 `halt-resume` 模式来钉住；其他 grow 全走 `concurrent-start`。drill 11 的 gate A（cmd_grow 不得手跑 init/join/reconcile）不受影响。
   - **产品侧的可选小增量（非测试专用 seam）**：把 `:717` 那行升格为明确的操作员提示「你现在可以启动 joiner 的 daemon，我会在它应答后继续」——今天这个提示在等满 60 s 之后才出现（`startJoinerHint`，`:281`），真实运维同样白等 60 s。这是 UX 改进，不是 fast-clock。另一个思路「driver 自己刚渲染完 conf 且 socket 文件不存在就立刻 HALT」我**不推荐**：admin socket 在 crash-restart 周期里是否存在我没读到，分不清「从未启动」与「正在重启」就会把 drill 42 的 3/4 flake 修回去。

4. **对 invocation 2 的 67 s 我不能精确归因**（见「不确定」）：按代码，former-N1 cutover 在 invocation 1 的 P5 就发了（`cutoverBroker`，`:535-563`，通过 transport error 重试最多 6×`growConvergePoll`=3 s），invocation 2 里的 cutover 应是 `AlreadyDone` 秒回；67 s 更像是 catch-up + JS meta 1→2 + VOTER 晋升 + CLI 对被 SIGKILL 重启的 leader nats 的重连。这些在 concurrent-start 模式下仍要付，只是并入同一次调用。

## 模板集群克隆（含状态 delta 表）

### 三种机制的裁决

| 机制 | 本机现状（只读核实） | 裁决 |
|---|---|---|
| **docker checkpoint/restore (CRIU)** | `docker info`: Experimental=false，`criu` 未安装，Docker 29.6.1，Storage Driver=`overlayfs`（containerd image store），kernel 6.8 | **否决。** (a) 要装 criu + daemon.json experimental + 重启 dockerd，均是 host 级 sudo 改动；(b) checkpoint 是按容器的，恢复到不同 instance（不同 bridge/IP/容器名）不是支持的路径，而且 CRIU 会恢复 netns 里的旧 IP；(c) raft/route/agent 的跨容器 TCP 连接无法原子 dump，恢复后全部断连 → 重连风暴 + 选举；(d) Go 走 vDSO 的单调时钟在恢复后跳变（除非 timens 配齐），nats ping/raft heartbeat 定时器一齐到期 → 仍是选举。**即使一切能跑，得到的也是 post-restart 态，不是 post-grow 态**，却付出最高的脆弱度。containerd image store 下 checkpoint 是否支持我也不确定。 |
| **named volume 快照 + 容器重建 + 进程重启** | volume = `/var/lib/docker/volumes/<n>/_data`，root/tether 属主，host 用户读不到；但 `docker run --rm -v src:/from:ro -v dst:/to <img> cp -a` 只需 docker 组权限（零 sudo，满足 R4） | **主方案的一半。** 每 drill N=3+ctl 要克隆 8 个卷（run_node 对每个节点都挂 etc+lib，`lib/docker.sh:110-135`）。大小是估计：grow 后 raft bolt + tether.db + JS store 每 broker ≲30 MB → 每 clone ≲100 MB，NVMe 上 1–2 s。 |
| **overlay 层 commit** | `docker commit` 对已停容器即时生效；commit 不含 volume（正好） | **主方案的另一半，且是必需的。** install.sh 写的 systemd unit、`tether` 用户（uid！）、`/var/log/tether/broker.log`、journald、`/home/sim`（agent state.json、ctl 登录态）都在容器 rootfs 而不在卷里（`image/provision-node.sh` + `scripts/install.sh`）。只克隆卷、用基础镜像重建容器 → 没有 unit、没有 tether 用户、卷内文件属主 uid 对不上。所以每个模板节点各 commit 一个镜像（N=3+ctl = 4 个），clone 用该镜像 + 克隆卷 `docker run`；lower layer 由 overlayfs 共享，零拷贝。`check_image_or_die`（`simcluster:675-680`）比的是镜像内 `/usr/local/bin/tether` 的 sha，派生镜像继承同一二进制，仍能过；但要加一道「模板派生自哪个 `tether-sim:dev` image id」的陈旧门，否则中途 rebuild 后 clone 会静默用旧产品跑。 |

### 模板的形状与冻结协议

- **模板只含 brokers + ctl，不含 agent、不含 drill 会话。** agent 的 `agent-join` 只要 7 s（其中 6 s 是 `simcluster:501` 的固定 `timeout 6` bind），而各 drill 的 agent 数不同（74 要 3、96/97 要 2）、且 agent 状态新鲜度是多条 claim 的前提（roster_gen、state.json、node ls 计数、proxy 分布），所以 agent 一律 live join。session 同理（5 s）。模板里唯一残留的会话是 `cmd_grow` 自己创建的 `grow-brk2`/`grow-brk3`——真实 grow 过的集群也有。
- **两个模板**：`tpl-n3`（up 3 + ctl → init brk1 → grow brk2 → grow brk3）与 `tpl-n2`（up 2 + ctl → init → grow brk2）。用 lever 1 后估计构建 wall ≈ 5+10+121+59 ≈ 195 s 与 ≈ 140 s，两者并行、与 N=1 drill 及 live-grow drill 同时跑，不进关键路径。
- **出生证明（birth certificate）**：冻结前用 tether 自己的观测面记录并断言——3 VOTER、`cluster ops` 全 terminal SERVING、JS meta cluster_size==N、`alert ls` 为空、grow lock 已释放（或记下残留）、leader id、tether/nats 版本、基础 image id、冻结时刻。
- **停机必须并发**：`cmd_down` 今天是串行 `docker stop`（`simcluster:649`）；leader 的 observe 循环每 5 s 一轮、2 s 窗口（`internal/broker/observability.go:224-225`），串行停会让 leader 把先停的 peer 记成 `broker_down` 并经 raft 落盘。三个 broker 后台并发 stop（SIGRTMIN+3 干净关机——Dockerfile 注释明说 SIGTERM 10 s 会砸坏 SQLite/Bolt），把窗口压到亚秒，并在出生证明里核 `alert ls` 为空。
- **日志窗口**：`drills/lib/logs.sh` 的 `sim_broker_slog_grep`/`sim_broker_panic_journal` 是整文件/整 journal grep（`logs.sh:59,85`），模板期的行（grow 时 lone-clustered-JS 的预期 crash-restart、干净关机的 `Deactivated successfully`）会进 clone。写得好的 drill 已用时间戳游标（95 的 `_jcursor_b`，`95-broker-selfheal.sh:67-69`，"drill 94's lesson"），但不是全部。容器内 `journalctl -b` 无法按容器启动切分（boot_id 不是 namespaced 的，待核）。建议冻结时 `journalctl --rotate && --vacuum-time=1s` + 截断 `broker.log`（标 `[env]` housekeeping，等价 logrotate），并把 logs.sh 加一个「自交接时刻起」的窗口参数。
- **host 侧 secrets stash 按 instance 键**（`lib/secrets.sh:13-16`），clone 要复制 `secrets/tpl-n3/` → `secrets/drill-x/`，否则 drill 里再 mint 节点（51 的 D-freshbox）拿不到共享 CA/account。

### 状态 delta 表：克隆体（post-clean-stop + boot）vs 模板（post-grow）

| 状态 | 模板（grow 刚完成） | 克隆体 | 谁的 claim 相关 / 处置 |
|---|---|---|---|
| raft term / leader | term=grow 后的值，leader 几乎总是 brk1（init 在 brk1） | 三节点同时起 → 新选举，term+k，leader 随机 | 50/52（grow_to_2 硬断言 leader==brk1，`cluster.sh:78`）、51/96/97（已有 `_ensure_leader_brk1` → `tether cluster transfer-leader brk1 --wait`，产品动词）。处置：clone 交接时统一用 transfer-leader 钉 brk1；没有 claim 依赖 term 的具体值 |
| JS meta / stream leader | 1→2→3 形成后的首任 | 重新选举，R=3 stream resync，短暂 503 窗（#67 类） | 20/92/12 的 `setup_forcesingle_n2` 假绿守卫已 poll `cluster_size==N`，交接门复用它；但**#67 瞬态出现的时点会变**（从 grow 后变为 restart 后），G67 的 retry 契约覆盖它，需在 stress lane 保留 grow 后形态 |
| `cluster_grow_active`/lease | 正常已释放；若泄漏，lease 每 TTL/3 续（`lock_lease.go:121`），TTL=15 min（`:115`） | 泄漏的 marker 在模板年龄 >15 min 后被 reaper（30 s，`broker.go:908`）清掉 | **30/40/41 的 #31/#38/#45 暴露臂**：clone 只在模板年龄 <15 min 时保住鉴别力。裁决：这三条 live grow（#31 自 2026-08-11 已修，臂走 else，但鉴别力不能因 fixture 丢失） |
| ops 表 | join ops terminal SERVING，时间戳=刚才 | 同内容，时间戳老了模板年龄 | 40 的 ops-schema 断言、30 的 roll 读 ops；未见按年龄回收 terminal ops 的逻辑（未核） |
| alerts | 空（若并发停） | 启动瞬间可能 raise→clear `broker_down`（若某 broker 慢几秒）；历史行留下 | **90**（absence predicates fail-closed）：只能在出生证明证 alert 表空 + 交接后 `alert ls` 空时才可 clone，否则 live |
| agent | 无（live join） | 无 | — |
| sessions | `grow-brk2/3` + admitted creator 指纹 | 同 | 与今天 live grow 后完全一致 |
| events/history JS 流 | grow 期 sys.events | 同 + 重启期 events | 计数型断言（60/61）都是 N=1、不克隆 |
| `jetstream.grow-bak.<epoch>` | 存在（former-N1 reset） | 同字节 | 11（live）断言它 |
| nats.conf / broker.yaml / secrets / cert_fp | clustered conf、seam、3650 天证书 | 同字节 | 20/12 的「force-single 自动去集群 conf」claim 基于同样的 conf 字节 → 可 clone |
| broker.log / journald | grow 期行（含预期 crash-restart） | 模板行 + 干净关机行 + 新启动行 | 整文件 grep 的 drill（97 bad-sig 扫描等）：冻结时 vacuum/截断 |
| `/etc/machine-id`、sshd hostkey | 模板首启生成 | **所有 clone 相同**（像克隆 VM） | tether 明确不用 machine-id（`internal/agent/instance.go:29`）；instance id 每进程随机、永不落盘（`:78-87`） |
| 容器 IP / 网络 | bridge `sim-tpl-*` | 新 bridge、新 IP，hostname 相同 | 设计上无 IP 记账（raft addr `brk1:7400`、route `nats://brk2:6222` 都按名解析）|
| 磁盘时间戳年龄 | 0 | = 模板年龄（分钟级） | 所有按年龄触发的产品逻辑（xfer reap ≥15 min、roster 6-min grace、lock TTL）：**必须给模板设年龄上限**（例如每 sweep 重建、交接时拒绝 >10 min 的模板） |
| 并发形态 | 与其他 grow 争用（#70/#67 的发现条件） | 一次零争用 grow，之后 16 个 clone 同时冷启动 | C1「争用即传感器」：live-grow stress lane 必须保留（见下节） |
| `--cap-store` tmpfs、`--shared-agent-home`、hangfs | — | 无法克隆 | 21、90-M6、84、62 保持 live（除 90-M6 全是 N=1） |

### 哪些 drill 必须 live grow，哪些可拿克隆

- **必须 live（grow 或 grow 残留就是 claim）**：10、11、13、91（A2 seeds 随 grow 自动收编）、67（G69 grow 的 op timeline）、82（C1 grow 后 roster_gen 递增）、42-F（returning node re-grow；其 N=2 fixture 可克隆）、30/40/41（#31 残留鉴别力）、90-M6（tmpfs）、22-#35 第二套 N=2、51-I 第二次 grow。
- **可克隆 N=3（8）**：71、73、74、93、96、97、98、51 主体（90 主体有条件）。
- **可克隆 N=2（8）**：12、20、92（`setup_forcesingle_n2` 的核心）、22 主体、50、52、95、42 的 setup。
- **N=1（17）不建议模板化**：init+join+session ≈ 22 s，而 60/80/81/82/94/61 的 claim 依赖「第一天」的新鲜状态。

### 收益估算（估计，基于给定实测 + drill-costs.tsv）

- N=3 克隆消费者：今天 setup ≈ 305 s（≈ drill 10 全程 p50），clone 交接估计 ≈ 25 s（8 卷并行拷 2–5 s + systemd 起 ≈ 3 s + 选举/JS meta/socket 就绪 10–20 s）→ −280 s × 8 ≈ 37 min。
- N=2 克隆消费者：今天 ≈ 5+10+176 = 191 s → ≈ 20 s → −171 s × 8 ≈ 23 min。
- lever 1 作用于剩余 live grow：N=3 live 6 条 × 110 s ≈ 11 min；单次 grow live 7 条 × 55 s ≈ 6 min。
- **合计 ≈ 77 min，sum 216 min（drill-costs.tsv 40 行合计 12944 s；78/83/84 未登记）→ ≈ 140 min（−36%）**；模板构建 ≈ 3.3 min 与首波并行。
- **wall：96 从 1337 s → ≈ 1057 s（≈ 17.6 min），这就是 L2 单独能给的 wall 地板（-j≥12 时 20.7 → ≈ 17.6 min）。** L2 无法再低，因为它只碰 setup。
- **L2 的战略价值是让分片变便宜**：把 96 拆成 5 个 per-claim shard，今天每 shard 要付 5 min 的 live grow（拆了白拆），有了 clone 每 shard 只付 25 s。若其他 lane 把长 drill 按 claim 分片，本 lane 视角的诚实地板 = 最长**原子**窗口 + 25 s：96 的 390 s #57 负窗 ≈ 7 min；其次 98 的 ≈4 min nats.go ping 检测（tether 未设 PingInterval）、33 的 120+210 s、74 的 240 s。**≤5 min 需要 #57 修掉（或对其负窗口另作裁决）并接受 98 的 4 min 作为下限；那都不是 L2 的活。**

## 烤进镜像

| 项 | 能否在 build 期做 | drill 可观测差异 | 收益 | 裁决 |
|---|---|---|---|---|
| secrets 铸造（host openssl + nk，`lib/secrets.sh`） | 能（但会让所有 instance 共享 CA/账户） | 无（网络隔离），82 的 untrusted-CA 负例自铸外部 CA | <1 s/节点 | 不做；模板克隆时随 stash 复制即可 |
| install.sh 落盘树（unit、用户、目录、broker.yaml、nats.d/nats.conf） | 能：build 期跑真 install.sh，docker 在**空**named volume 首次挂载时会把镜像该路径内容连属主拷进卷 | (a) `public_host: <node>`（`provision-node.sh:30-36`）按节点不同，需首启一行 root `sed` 的 [env] 覆盖；(b) **32** 要求「fresh un-provisioned container」→ 必须保留未供给的基础镜像，双 tag；(c) 13 的属主断言在 build 期 install 后仍成立（卷填充保 uid） | `up` 只有 3.8–5 s 且已并行，省 ≈3 s/drill ≈ 2 min sum | **不做**：保真成本（一条新的 root 覆盖、双镜像、静默的「卷非空则不拷」语义）> 2 min |
| standalone 首启建 tether.db（`simcluster:262/409` 各 ≤20 s poll，实测秒级） | 可能——但要先证明标准库首启只写 schema、不写任何每节点身份 | 43 的 `init --from-existing` 拿的是「真进程建的 DB」 | ≈3 s/init | 不做；模板化后 N≥2 的 init 只付一次 |
| N=1+agents 模板 | 能 | 破坏 60/80/81/94/61 的「第一天」前提 | ≈20 s × 17 ≈ 5–6 min sum | 不做 |

一句话：provisioning 本来就是 sim 的活（Mandate ③），烤进去合法，但它已经便宜到不值得任何保真妥协；时间全在 grow 和 drill 本体。

## 重评"制造 grow 产物"裁决

上一次 plan §9 的原话：「a fixture-snapshot speed-up would manufacture the output of tether's own init/grow (Mandate ①/③/④)」——在「Mandate 不可动、每 drill 自建集群」前提下，"snapshot" 意味着 sim 用非 tether 的手段（或过期产物）伪造 grow 结果，Mandate ④「越省力越可疑」直接命中。逐字重评：

1. **「manufacture」不再成立**——如果模板是本次 sweep 里、同一二进制、由 `tether cluster add` 真做出来的，克隆体承载的是 tether 自己的输出，只是做一次用 N 次。没有任何一个字节是 sim 写的（除 [env] 日志 housekeeping，须标注）。Mandate ① 不被触犯：环境仍按生产方式建，克隆是「重启后的同一台机器」，生产每次 reboot 都到达这个状态。
2. **在 grow 本身是 claim 的 drill 上，原裁决仍然成立**：10/11/13/91/67/82/42-F 用克隆等于用回放代替测量。它们必须 live——这一点没有变。
3. **在「grow 残留」是 claim 的 drill 上，原裁决部分成立**：30/40/41 的 #31 鉴别力受 lock TTL 约束；克隆只在年龄 <15 min 时等价。裁决：live。
4. **原裁决没说出口、但今天必须补上的一条：争用形态**。C1 政策记录 #67/#66/#70 都是在多 grow 同时进行的争用下被发现的；模板化把 ~30 次并发 grow 压成 1 次零争用 grow，**可见 bug 集会变**——这正是 drill 83（leaseGrantWindow 压到 1 s 才看见双执行）在并发维度上的同构。处置不是否决克隆，而是 C1 自己已经要求的：保留 `--live-grow`（今天的全 live sweep）作为标注的 stress lane（release gate / 每周），其偏离仍可 mint gotcha、不可静默改 expected-verdicts；C1 的 registry 里给 #67/#70/#66 记上「依赖 live-grow lane 复现」。
5. **克隆体是 post-restart 而非 post-grow**——这不是 manufacture，但是**另一个**合法部署态。原裁决把两者混在一起否决；新裁决要求：每个 fixture 消费者把它隐含的 post-grow 前提（leader==brk1、JS meta 已成、无 alert、lock 已释放）显式地在交接门重新建立（都是既有的 poll/产品动词），并把 delta 表写进 README 的 Mandate 旁边。
6. **Mandate ④ 的倒置判据仍然有效且更锋利**：克隆脚本本身不能有任何「修一修再交接」的分支——交接门失败即 SETUP-RED（或回退 live grow 并在 rollup 里打 `FIXTURE=live-fallback`），绝不 chown、不改 conf、不清 alert。

## 方案矩阵

| 方案 | 节省（每 drill / 全套 sum） | 状态 delta | 受影响 drill | 实现复杂度 | 前置条件 |
|---|---|---|---|---|---|
| **L1 并发启动 joiner daemon（键在 `:717` 进度行上）** | −55 s/grow；N=3 −110 s；sum ≈ −17 min（13 条 live grow）+ 模板构建 −110 s | 零（同一产品路径，只是 provisioning 动作提前；grace 注释明说为此设计） | 所有走 `cmd_grow` 的 drill；HALT/resume 契约需由 1 个 drill 专臂钉住 | 低：cmd_grow 后台化 + tail 触发 + 双模式契约 ≈ 60 行 sh；一个 hermetic test 钉触发行文本 | 无（产品那行文字成了契约，需 `tests/` 守它） |
| **L1' 产品侧：grace 开始前就打印 start-joiner 提示** | 同上，且真实运维少等 60 s | 零 | 同上 | 极低（一行输出前移） | 一个小产品增量；非测试专用 seam |
| **L1'' 产品侧：driver 自己刚渲染 conf 且 socket 不存在则跳过 grace** | 同上 | 可能把 drill 42 的 3/4 flake 修回去 | 42 | 中（要读 adminsock 生命周期） | 不推荐 |
| **L2 模板集群克隆（volume 拷 + 每节点 commit）** | N=3 −280 s、N=2 −171 s；sum ≈ −60 min；wall 20.7 → ≈17.6 min；**使分片可行** | 见 delta 表：post-restart 态、term/leader/JS 重选、模板年龄、日志残留、争用形态变化 | 16 条消费者；30/40/41/90 有条件或 live | 中高：新 verb `template freeze` / `clone --from`，改 `grow_to_3`/`grow_to_2`/`setup_forcesingle_n2` 三个 helper，run-drills.sh 模板阶段 + 陈旧门 + 年龄门 + 交接门 + 出生证明 + live-fallback，logs.sh 窗口参数，`--live-grow` stress lane；估计 300–400 行 sh + 若干 hermetic test | 仅 docker 组权限（零 sudo）；磁盘 ≈100 MB/clone（估计）；inotify 计数不变 |
| **L2-CRIU** | 理论同 L2 | 仍是重连+选举（时钟跳变），且 IP/netns 不可迁 | — | 极高 | 装 criu、experimental、重启 dockerd（sudo）；containerd store 支持存疑 | **否决** |
| **并行 grow（放松锁）** | N=3 −59 s（仅 live 路径） | 删掉 #31 传感器；cutover/R3 语义重写 | 30/40/41 | 高（产品重设计） | — | **否决** |
| **joiner [env] 步骤与前一 grow 重叠** | −10 s/N=3 | 零 | 无 | 低 | 无 | 可做，顺手 |
| **烤 install.sh 树进镜像** | −3 s/drill ≈ −2 min | 一条新 root 覆盖 + 双镜像 | 13/32 | 中 | 无 | 不做 |
| **N=1 模板** | ≈ −20 s × 17 | 破坏「第一天」前提 | 60/61/80/81/82/94 | 中 | 无 | 不做 |

**本 lane 的诚实结论**：L1 + L2 把 sum 压 ~36%、wall 压到 ≈17.6 min（96 的本体）。「几分钟」在 L2 单独下做不到；L2 的意义是把每个 shard 的固定成本从 5 min 降到 25 s，让长 drill 按 claim 分片后 wall 才能真的下来。分片后的地板由最长原子窗口决定：#57 的 390 s 负窗（≈7 min）→ 若 #57 修掉则 98 的 ≈4 min ping 检测 → ≈4.5–5 min。

## 我不确定的地方

1. **invocation 2 的 67 s 构成**：给定事实说含 former-N1 cutover + JS reset + nats restart；代码顺序（`:176-199` 在 `:207` 之前）显示 cutover 在 invocation 1 发出。我推测 67 s 主要是 catch-up/JS meta 1→2/VOTER 晋升 + CLI 对 SIGKILL 重启的 leader nats 的重连超时。**不重测就无法归因**；这决定 L1 能省 55 s 还是更多。
2. **clone 交接 25 s 是估计**：卷大小（未见现存 sim 卷，`docker system df -v` 为空）、三节点冷启的选举 + JS meta 重组 + R=3 stream resync 时长、16 个 clone 同时冷启时 JS 重组的争用（比 grow 轻得多，但 #70 的同类敏感性可能以更小幅度出现）。需 B1 式 A/B。
3. **`journalctl -b` 在容器内是否按容器启动切分**：我认为 boot_id 不是 namespaced（所有容器共享 host boot），所以只能靠 vacuum/时间戳窗口；未在本机核实。
4. **`docker commit` 在 containerd image store（`Storage Driver: overlayfs`）下的行为细节**（含 STOPSIGNAL/CMD 继承）我按 24.x+ 的文档假设成立；`docker checkpoint` 在该 store 下大概率不支持，未核。
5. **admin socket 在 crash-restart 周期里的存在性**（决定 L1'' 是否可行）未读 `adminsock` 生命周期。
6. **standalone 首启 tether.db 是否只含 schema**（决定「烤 DB」是否零 delta）未读 `serve` 首启路径；我已裁决不做，所以不影响结论。
7. **terminal ops / 事件流是否有按年龄的回收**：影响模板年龄上限该设多少（我建议 ≤10 min，且每 sweep 重建）。
8. **90 主体能否克隆**取决于出生证明能否证明 alert 表为空且交接后无 raise/clear 历史行——若 `broker_down` 的 raise/clear 会留下可被 absence predicate 读到的行，90 必须 live。
9. **hermetic 门的联动**：`tests/lint-drills.sh`、`kept-sites`、`r9d-nonvacuity`、`test/architecture/simcluster_gate_set_test.go` 对 `grow_to_3`/`cmd_grow` 结构是否有隐含断言我只粗扫（未见直接引用），改动后必须跑 `sh tests/run-all.sh`。


---

# 草案 L3 长 drill 拆分与调度模型

## 各 drill 臂结构表

约定与来源：
- 夹具基线（solo，用户给定实测）：`up` 3.8–5 s / `init` 10 s / `grow brk2` 176 s / `grow brk3` 114 s / `session` ≈3 s / `agent-join` ≈7 s。记 **F1**（N=1+agt+ctl）≈ 25 s、**F2**（N=2）≈ 195 s、**F3**（N=3）≈ 310 s。
- **F′** = 同上但 `joinerBootGrace`（`cmd/tether/cluster_add_drive.go:692`）在"driver 本次调用自己刚走了 render 分支（`!joinerBrokerUpLocal`，:171-185）"时不再等满 60 s：F2′ ≈ 140 s、F3′ ≈ 200 s（每次 grow 省 ≈55 s，估计）。
- **T** = L2 模板集群（由一次真实 `tether cluster add` 产出、按 image sha 缓存）恢复到新 instance：τ ≈ 45–60 s（估计：up 5 s + 卷恢复 + 三 daemon 启动 + leader 选举 + 每 agent 7 s）。本表按 τ=60 s 取保守值。
- `drill-costs.tsv` 的 p50 是 2026-07-23 的 -j6 数据（文件头注释），**对之后长了新臂的 drill 已过期**（90 的 M6/M8/M9、41 的 S-survival、50 的 K/L、42 的 F 都是之后加的——90 的 775 s 甚至小于它今天三个夹具的 solo 和 ≈ 804 s）。因此下表的臂时长是"夹具 + 臂内 typical 等待"的**估计**，必等窗口给 file:line；真值必须由一次带 M3 `DRILL-POLL-WAIT`/evidence 的 instrumented sweep 替换（本机 `/tmp/simdrills*` 归档已不存在，我无法读到）。
- "必等"= 绑在产品常量/库默认值上的窗口，不改产品就压不掉；"可等"= 通常远早于预算返回的 poll。

| drill | 夹具 | 臂（→ 拆后单元） | 臂内必等窗口（file:line，产品常量） | 臂间依赖 | 拆后单元时长（T 制 / F′ 制，估计） |
|---|---|---|---|---|---|
| **96** mid-flight-chaos | F3 + 2 agt + 2 份 agent.yaml + brk1 knob 重启 + 重立 leader（:310-357，≈ +60 s） | **A**（#57/#58 tier-B 中断）· **B0**（`run --ack-alerts`，kill 的副产品）· **D**（leader 分区）· **F**（双故障） | A：`poll_until 390`（:396，= `proto.XferTimeoutTierBFloor` 5 min + 90 s，`internal/proto/xfer.go:61`）**只在 #57 钉住（仅 start 行）时耗满**；expected-verdicts 记录的世界是"1 GiB 在 kill 前传完"（nc_gap=5），此时该 poll 立刻返回。A 随后 brk2 重启 ≤120（:416）+ 30×6 s（:418-422）+ reap ≤90（:479）。D：新 leader ≤120（:560）、存活侧写 ≤300（:569，JS meta 2/3 重组）、heal 收敛 ≤180（:626）、readback ≤120（:629）。F：前置门 ≤360（:711）、reconcile ≤180（:737）、audit ≤120（:741） | D0 依赖 A 已把 brk2 拉回并重回 VOTER（:539）；F 依赖 D 完全恢复（:711 那道门正是"跨臂残留"，拆臂后**结构性消失**，F 的 `not_covered` gap（:745）不再需要）；B0 依赖任一 kill 留下的 severe alert | 96-A：T 120 + A ≈ 90 → **≈3.5 min**（若 #57 钉住：120+390+120+180+90 ≈ **15 min**）；96-D：120 + ~215（worst 800）→ **≈5.5 min**；96-F(+B0)：120 + ~120 → **≈4 min**。F′ 制各 +140 s |
| **98** stuck-redial | `up 3` + `init/grow/grow`（:154-155，直接调 `$SIM grow`，非 grow_to_3）+ agt | 单臂：DROP agt1→其当前 broker:4222 → IMPACT → RECOVERY → heal | `RECOVERY_BUDGET=330`（:41）；真正的必等是 **nats.go ping 检测**：tether 不设 `PingInterval/MaxPingsOut`（:31-34），默认 2 min × MaxPingsOut 2，`processPingTimer` 在第 3 个未应答 tick 判 stale ⇒ 检测落在 **4–6 min**（推导：切断时刻相对 ping tick 的相位），非 drill 注释写的"≤4 min"；随后 redialAfter 20 s（`internal/agent/roster.go:35`）+ closeBudget/poisonGrace 各 10 s | 无 | **≈6.5–9 min**（T 60 + 检测 240–360 + 恢复 ≤60 + heal/steady ≤70）。这是拆臂/模板都碰不到的地板 |
| **67** transient-js-refusal | `up 2 + init + grow brk2` + agt（:74-78） | **CTRL-before**（含 G69 正向 oracle）· **INJECT/RESTORE**（stop/start brk2 nats）· **CTRL-after** | meta 重组 ≤120（:155）；after-loop ≤12×(push+5 s)（:164-168）；push 自身的 G67 预算 ≈ 8 s + sizing 1.5 s（`internal/broker/xfer_provision.go:35-46`） | 三段是一条因果链（before→inject→after 才构成"transient"证明），不可拆 | **≈2.5–3 min**（T 60 + 5 + 10 + ~40 + ~30）。⚠ p50=777 s 与可见等待（≤190+120+180）不符，来源未明——见"不确定" |
| **90** alerts-lifecycle | ①grow_to_3 1 1 0（:81）+ agt；②`up 2 --cap-store 3g` + init + grow brk2（:184-186）；③`up 3` + init + grow brk2 + grow brk3（:211-216） | **90-a1** M1–M4+M7（manual raise/ack/clear，无故障）· **90-a2** M5/M5⑤（kill follower → broker_down → return）· **90-a3** M9（raft_lag 分区）· **90-M6**（disk_pressure，capped N=2）· **90-M8**（below_quorum 在 N=2 起、grow brk3 后清） | a2：return ≤120（:154）；a3：45/45/90（:165-171）；M6：ballast+restart 后 ≤90/≤90（:196/203）；M8：45/45（:215/217）+ **grow brk3 是 claim 本身（必须真 grow）** | a1/a2/a3 今天串在一个集群上但互不依赖（各自有 clean-baseline 门）；M6/M8 已是独立夹具 | a1 ≈ **2 min**；a2 ≈ **3.5 min**；a3 ≈ **4 min**；M6（无 cap-store 模板则 live）≈ F2′ 140 + 60 + 90 + 90 → **≈6 min**（若做 capped 模板 ≈ 4 min）；M8：T(N=2) 60 + 45 + grow brk3′ ≈60 + 45 → **≈3.5 min**（全 live′ ≈ 5.2 min） |
| **74** rebalance-on-return | grow_to_3 3 1（:265）+ 3 agt + `_proxy_ready 3` ≤40（:275） | **74-SRAB**：eligibility ≤240（:290）→ 构造 1/1/1 ≤120（:291,:298，effectful `poll_until_fixed`）→ 三条 SS 流 ≤120×3（:311）→ SKEW（:189，≤40）→ RETURN（:217，≤90）→ A（≤240，:353）→ B（settle 45 :359、preflow 90 :369、real ≤180 :212、B-dp ≤240 :407、negctrl）· **74-C**：env 重启三 broker + ≤60（:223-225）→ settle 90（:474）→ 重建 1/1/1 → pre-kill 流 ≤90（:505）→ skew/return → C-auto **≤180 锁定窗**（:572）→ C-dp ≤240（:599）→ negctrl | 这是 **RED-EXPOSING** drill：#34 open ⇒ A-elig 240、C-auto 180 这类"必须在 W 内发生"的窗口**每次耗满**——这是揭露型断言的固有成本，不是 harness 浪费。`_ge3_eligible` 的 240 s 是 grow 后 proxy-eligibility 恢复；模板集群已稳态运行，该窗预计 ≈0 | C 依赖 SRAB 的 SKEW/RETURN edge？否——C 自己重建 skew/return（:474-572）；C 唯一依赖是同一夹具+env 开关 ⇒ 可独立 | SRAB：T 80 + ~200（p50 残差推得；worst 全窗耗满 ≈ 1100）→ **≈4.7 min（worst ≈ 20 min）**；C：T 80 + 60 + 90 + 130 + 180 → **≈9 min**（C-auto 在 #34 open 下恒耗满 180） |
| **41** shrink-to-standalone | grow_to_3 1 1 0（:67）+ agt 直连将退休 broker（:80-91） | 单条 spine：negatives（:99-107）→ retire brk_a（`--wait --timeout 3m`，:61；voters 差分 ≤90 :133）→ retire brk_b（NATS_ROLLED_OUT ≤30 :139）→ to-standalone（stop/restart :262-266）→ S-survival ≤300（:282，3×RosterRefreshInterval，`internal/agent/roster.go:24` = 3 min） | 每步依赖上一步的拓扑 ⇒ **不可拆**；negatives 可剥成独立小单元但收益 ≈30 s | **≈4.5–6.5 min**（T 70 + 30 + 2×~60 + 60 + 逃逸 60–300） |
| **97** soak-cycles | grow_to_3 2 1（:234）+ 2 agt + warm-up `SOAK_SETTLE`=25（:102,:247） | 6 个 cycle 轮转 4 种注入（:272-322），每 cycle 后 settle 25 s；然后 leak/goroutine oracle | 每 cycle：agt kill → `agt1 leaves ONLINE` ≤90（:279，实际 = `node.DefaultOfflineAfter` 60 s，`internal/node/node.go:34`）+ back ≤120（:281）；brk3 restart ≤90（:287）；partition heal ≤180+60（:295-297）；transfer ≤60（:314） | **oracle 就是序列本身**（同一进程跨 ≥6 个样本的斜率，PID-generation 守卫 :328）⇒ **不可拆**，也不能分到 6 个集群 | **≈8–10 min**（T 80 + 25 + 6×(60–120) + 25 + oracle）。地板由 `OfflineAfter` 60 s 与 cycle 数 6 决定 |
| **40** drain-retire | grow_to_3 1 1 0（:89）+ agt | **40-a**：D-drain（≤20/≤30 :104/106）+ RECONCILE-plan + SAFETY negatives · **40-b**：OPS-CONFIRM/ABORT（不可达 nonvoter → BLOCKED **≤175**，:183，自适应 catch-up deadline 产品常量）· **40-c**：RETIRE spine（#31 分支 :214-227；R-resume kill 旧 leader :237 + return ≤60 :255；`_retire_gone` ≤150 :83） | a/b/c 互不依赖（都在 N=3 上从干净状态起） | a ≈ **2.5 min**；b ≈ **T 70 + 175 + 60 ≈ 5 min**；c ≈ **T 70 + 60 + 150 ≈ 4.7 min** |
| **33** node-upgrade-success | F1（:177-182）+ agt1b 兄弟 unit ≤40（:165）+ artifact 容器 + url_allow 重启 | **33-B(+C)**：syn-block → upgrade → staged ≤60（:227）→ pending ≤45（:229）→ C 兄弟被拒（嵌在 B 窗内 :236-239）→ **watchdog 回滚 ≤210**（:244；产品 120 s 注册 deadline "waiting for re-register (deadline 120s)" :263）→ heal → ≤120/60/60（:253-257）· **33-A(+C2)**：真升级成功 + 域释放 | A 今天依赖 B 回滚后的 dst==OLD；给 A 独立夹具则直接成功路径 | B ≈ **F1 25 + 60 + 60 + 120 + 90 ≈ 6 min**；A ≈ **25 + 60 + ~60 + ~40 ≈ 3 min** |
| **22** forcesingle-online | ①`setup_forcesingle_n2`（`drills/lib/setup-forcesingle.sh`：F2 + agt + 12 MB push 基线）；②#35 夹具：nuke + `up 2` + init + grow brk2（:213-216） | **22-main**：DRY/GATE 负向 → Arm-0（kill brk2 + 24×5 s 观察 dwell :110-113）→ PROT → GATE-d（含 **`sleep 61`** :154 = arm token 60 s TTL 的唯一真时钟到期接线探针，accel-plan R8 保留）→ POS commit · **22-#35**：fresh N=2 → kill → survivor restart → 观察 | dwell ≤120 是产品的 ~15 s 连续 quorum-loss dwell + 检测；61 s 是 TTL | 两臂各自夹具，今天串行 | main ≈ **T 60 + 30 + 60 + 61 + 30 ≈ 4 min**；#35 ≈ **T 60 + 30 + 60 ≈ 2.5 min** |
| **51** full-dr | grow_to_3 1 1（:161）+ vault + agt + expose | 单条 spine：backup → 总损失（rm 容器+卷）→ D fresh box `up 1`（真 install.sh :251）→ 文档化 DR 步骤 → ≤90（:415/425）→ H agt 自愈 ≤90×2（:448/451）→ **I 再 grow brk2**（:496-498，claim 本身，真 grow）+ ≤90 + ≤60 | 每步依赖上一步 ⇒ 不可拆 | **≈7–8 min**（T 70 + 60 + 20 + 90 + 90 + grow′ 140 + 60）。备份/负向门可能剥出但主 spine 不变 |
| **71** expose-rehome-failover | grow_to_3 1 1（:197）+ agt（tunnel=brk3）+ 两个 expose；**夹具门 ≤200**（:81，agt→brk3 tunnel 间歇，R6-M2 硬断言） | **71-CD(+A,E)**：kill brk3 → seen down ≤60（:244）→ strand 断言 → return ≤45（:253）→ recover ≤180/≤120（:256/259）→ E · **71-B**：drain 迁移 ≤240（:309）+ #30 事件 + B-silent | B 依赖 C 的 recover 成功（:342）⇒ 给 B 独立夹具（新建 expose 后直接 drain） | CD ≈ **T 70 + (0–200) + 60 + 45 + 60 ≈ 4–7 min**；B ≈ **T 70 + 60 + 240 ≈ 6 min（worst）/ ~3.5 typical** |
| **73** proxy-cluster-ha | grow_to_3 2 1（:177）+ 2 agt + ready ≤60（:190）+ eligibility ≤240（:222，模板下预计≈0）+ SS-construct（`_ss_egress` ≤240 :129） | **73-REHOME(+sub/revoke)**：kill home → rehome ≤90（:286）→ ready → **#33 测量 180**（:298，measure-and-record，未恢复即耗满）· **73-Q**：heal + 1+1 构造 → kill 第二个 → 冻结分离证明 | Q 今天依赖 REHOME 已杀 1 个 broker（"2 of 3 down"）；独立夹具下 Q 需自己先 kill 一个（`REHOME-skip` :318 已有此形状） | REHOME ≈ **T 80 + 60 + 90 + 180 ≈ 7 min（若 #33 STRANDED）/ ≈4 min（AUTO-RECOVERED ~26 s，INDEX 2026-08-29 实测）**；Q ≈ **T 80 + 120 + 60 ≈ 4.5 min** |
| **93** metrics-observability | grow_to_3 1 1 0（:128）+ observability 滚动重启 + ≤60（:160） | **93-a**：/metrics、/healthz、/readyz（≤30 :261）、MET-degraded（stop/start ≤30/≤60 :425-431）· **93-b**：webhook（leader 稳定 ≤90 fixed :277 + `sleep 8` :285 + 3×≤20）+ BAD-url 重启 ≤60（:365）· **93-c**：TOPO-STUCK（≤45/≤90 :210/224）+ N75 mis-nest ≤60（:468）+ all-down | 互不依赖（各自 restart 后都 poll VOTER 回归） | a ≈ **3 min**；b ≈ **4 min**；c ≈ **4 min** |
| **30** rolling-upgrade | grow_to_3 0 1 **0**（:319，retry=0，30 是 #31 owner）+ colocated agents（:357-359，含 broker 重启）+ staging | 单条 spine：#31 probe → dry-run → stop declared agent → roll → HALT → per-hop → UNLOCK → repair → resume → PHASE-2（≤90/≤60 :460/470） | 全程依赖 | **不宜模板**（#31 的采样点就是这次 grow）：F3′ 200 + 60 + 120 → **≈6.3 min** |
| **91** client-converge | `up 3` + init（:29-30）；**A2 的两次 grow 是 claim**（:44,:49） | **91-A**：A1 publish + A2 grow brk2 (+≤60 :46) + grow brk3 (+≤90/≤120 :57/59) + D floor kill/return ≤90（:98）+ A3 · **91-C**：offline force-single survivor-only（:129-169，≤60/≤90） | C 依赖 N=3（模板可用）；A 必须真 grow | A ≈ **20 + grow′ 140 + 60 + grow′ 60 + 90 + 90 ≈ 7.7 min**（live′；这就是 grow-claim 单元的形状）；C ≈ **T 60 + 30 + 90 ≈ 3 min** |
| **42** rejoin-returning | `setup_forcesingle_n2` + kill brk2（:82） | 单条 spine：H stop brk1 → offline force-single `--reset-js` → restart（:116 + `sleep 5`）→ node_start brk2 → rejoin 诊断 ≤90（:128）→ rejoin-prepare 门族 → G resnapshot（:182-198，≤60）→ **F 再 grow brk2**（:222，claim，真 grow）→ 后续 | 全程依赖 | **≈6 min**（T(N=2) 60 + 60 + 90 + 60 + grow′ 140 + 30） |
| **10** grow-to-3 | 无夹具可模板：**up/init/grow×2 就是 claim**（:22-37） | 单臂：JS meta ≤18（:62）、transfer-leader ≤24（:85）、agt、kill follower → ≤36/≤36（:100-101）、写 ≤30（:109） | — | **live′ 200 + 90 ≈ 4.8 min** |
| **52** credential-rotation | grow_to_2 1 1（:275）+ agt + expose | **52-A**（tunnel-cert：A7 partition 7000 ≤80/≤100 :378/387；A8 restart/restore ≤25/≤120 :407/420）· **52-B**（account.nk 轮换 ≤45 :485；把 auth 弄坏）· **52-D**（C7 retire spine：rebuild ≤120 :524、retire :557、≤30/≤20） | D 依赖"B 之后能重建集群"（:524-525）⇒ 给 D 独立夹具后该依赖消失 | A ≈ **T 60 + 100 + 150 ≈ 5 min**；B ≈ **T 60 + 120 ≈ 3 min**；D ≈ **T 60 + 60 + 60 ≈ 3 min** |
| **50** backup-restore | grow_to_2 1 1（:109）+ agt + expose（home 钉 brk1） | **50-gates**：backup ×3（G2 offline：stop brk2 ≤90 :334）+ restore 门 I1–I4/M1（需一个 bundle，backup 秒级）· **50-spine**：backup → disaster（:357）→ J2 真 restore → K（#64 ~73 s crash-loop，≤90/≤120 :533/535）→ L（≤180/≤180/≤120/≤180/≤120 :596-645） | spine 内全依赖；gates 可剥 | gates ≈ **T 60 + 120 ≈ 3 min**；spine ≈ **T 60 + 30 + 60 + 120 + 300 ≈ 9.5 min worst / ~5–6 typical** |

不在"20 个长 drill"里但影响调度的 23 个短 drill：全是 F1（30–130 s）或 F2/forcesingle_n2（12/20/92/95/82：180–290 s，均可模板化，claim 不是 grow）；11/13 的 grow 是 claim（真 grow，各 ≈150–270 s）。

## 最长单臂

按上表，拆臂 + 模板 + joinerBootGrace 修正三者都到位后，仍 > 5 min 的单元（估计，按 typical 排序）：

1. **97-soak** ≈ 8–10 min —— 等的是 6 个 cycle × `DefaultOfflineAfter`(60 s)/分区收敛(≤180)/`SOAK_SETTLE`(25 s)。oracle 是跨 cycle 的斜率，结构上不可拆。
2. **74-C** ≈ 9 min（C-auto 180 s 锁定窗在 #34 open 下恒耗满）；**74-SRAB** typical ≈ 5 min、worst 20 min。
3. **98** ≈ 6.5–9 min —— nats.go ping 检测 4–6 min（库默认，tether 未设 `PingInterval`）。
4. **51** ≈ 7–8 min、**30** ≈ 6.3 min、**42** ≈ 6 min、**33-B** ≈ 6 min、**91-A** ≈ 7.7 min（三个 grow-claim/regrow spine + 一个 120 s 产品 watchdog）。
5. **96-A** ≈ 3.5 min（记录的 expected 世界）但若 #57 钉住 ≈ 15 min（390 s 窗 + 恢复）。

**新的 wall 地板 ≈ 8–10 min（估计）**，由 97 与 74-C 设定；它们等的是产品计时器（`OfflineAfter`、C-auto 锁定窗、#34 未修）而非 harness。要把它们压到 ≤5 min 只有两条路，且都不在 harness 侧：
- 改产品计时常量或把它做成 deploy knob（`OfflineAfter` 60 s、nats `PingInterval` 2 min、`XferTimeoutTierBFloor` 5 min、upgrade 注册 deadline 120 s、adaptive catch-up deadline、`RosterRefreshInterval` 3 min）。每一条都要过 drill-83 试金石：`leaseGrantWindow` 压到 1 s 后只有真栈 drill 看见双执行——**压缩产品时间常量改变的是可见 bug 集，不是测试速度**。`PingInterval` 是唯一我认为可以作为*生产改进*而非测试 seam 来论证的（#72 台账自己写着"检测最多 4 min"是缺陷面），但它同样会改变 98 测得的 detection+teardown 之和，必须重新校准 330 s 预算。
- 修 #34（74 的窗口不再耗满）。这是产品缺陷，不是速度问题。

结论：**"全套 ≤5 min wall"在"每个 drill 的 claim 不变"前提下做不到**；诚实地板 ≈ 8–10 min（模板缓存命中）/ ≈ 11–13 min（新镜像需先花 ≈3.5 min 造模板，且它在关键路径上）。

## 调度模型（含公式与数字）

### 单元与容器计数

- 拆臂后单元数：20 个长 drill → 约 40 单元（96×3、90×5、74×2、40×3、33×2、22×2、71×2、73×2、93×3、52×3、50×2，其余 1）；23 个短 drill → 23（个别可再剥）；合计 **U ≈ 63–66**。
- 每单元容器 = 该臂拓扑（brk+agt+ctl，+artifact/ingress sidecar）：平均 ≈ 4.2（由 43 个 drill 的拓扑求和 ≈178 容器 / 43 推得；拆臂后每个臂自带夹具 ⇒ 总量 ≈ 65 × 4.2 ≈ **270–300 容器**（若全并行）。
- 对照 README:249 的 ~600 容器 CPU boot-storm 上限：全并行 ≈ 一半。inotify：`fs.inotify.max_user_instances`=8192（本机已读），2026-07-09 实测 40 容器打穿 128 ⇒ ≈3.2 实例/容器起步；300 × ~5–8 ≈ 1500–2400 ≪ 8192。pid_max 4194304、threads-max 2061280、file-max 无限、conntrack 262144（本机 sysctl 已读）——M5 表里没有一条会在 300 容器下触顶。真正的 t=0 风暴是 65 个 `up` 同时起 ≈300 个 systemd+install.sh：加一个 bring-up 信号量（同时 ≤8 个 `up`）把它摊成 ≈80 s 的斜坡即可（V4 已在单元内并行）。

### wall 公式

```
wall(j) = T_pre + max( P_max , (Σ/j)·(1+δ) , W_grow ) + T_post
```
- `Σ` = 单元时长之和；`P_max` = 最长单元；`δ` ≈ 5–10 % LPT 装箱损耗（单元数 65、时长离散度大，greedy 接近下界，accel-plan §1 实测 32.5→32.7）。
- `T_pre` = `check-image` + **模板构建**（新 image sha 时 ≈ F3′ 200 s ≈ 3.5 min，且在关键路径：所有模板消费者必须等它；按 sha 缓存命中时 0）。
- `T_post` = infra-flake 串行重试（改为按单元、可并行）+ 汇总；**M4 归因 pass 另算**（默认开、串行 solo、预算 60 min——按单元重跑后每条 3–8 min 而非 22 min，但仍在主 wall 之外）。
- `W_grow` = **grow lane 的 wall**：README:274-277 + `simcluster:345-352` 记录"5 个 grow 同时起打穿 VOTER 150 s 窗"、"SOLO / 30 s stagger 下 GREEN"；#70 台账把它定为 first-class flake（不 band）。模板化后剩下的**真 grow 单元** ≈ 9 个（10、11、13、91-A、90-M8、42、51、82-C1、30；模板构建自身再 +1）。两种控制：
  - 并发上限 g：`W_grow ≈ ceil(9/g) × ~4.5 min` → g=3 ⇒ 13.5 min（**比 P_max 还长**）；g=5 ⇒ 9 min。
  - 30 s 错峰起跑、不限并发：`W_grow ≈ 9 × 30 s + 4.5 min ≈ 9 min`。
  - 两者都落在 ≈9 min，与 P_max 同量级。**grow lane 是第二个 wall 设定者**，且它对 fsync 尾延迟敏感（#70 归因是 raft/JS-meta 形成期）。

### 数字

假设（估计）：模板制 Σ ≈ 110–130 min（今天 43 drill 在 -j6 的 Σ ≈ 160 min（accel-plan §6.4）→ 拆臂不减 Σ 反而每臂多付一份夹具 τ，但 F3 310 s → τ 60 s 每处省 ≈4 min，约 25 处 ⇒ −100 min；再加 joinerBootGrace 省 ≈1–2 min/真 grow；净 ≈ 110–130 min）；P_max ≈ 9 min；W_grow ≈ 9 min（30 s 错峰）。

| j | Σ/j（Σ=120 min） | wall ≈ max(P_max 9, Σ/j·1.07, W_grow 9) | 并发容器 ≈ j×4.2 |
|---|---|---|---|
| 6 | 20 | ≈ 21 min | 25 |
| 12 | 10 | ≈ 11 min | 50 |
| **14–16** | 7.5–8.6 | **≈ 9 min（P_max/W_grow 绑定）** | 60–70 |
| 24 | 5 | ≈ 9 min | 100 |
| 65（全并行） | 1.8 | ≈ 9 min | 270–300 |

关键结论：**j 超过 ≈14–16 后 wall 不再下降**（被 P_max 与 grow lane 钉死），所以 270 容器的全并行既无收益也无必要——它只会把 fsync 尾延迟推向未测区（33 容器时 p99 13–15 ms；300 容器未知）并放大 #70。推荐运行点：j≈16 + grow lane 30 s 错峰 + bring-up 信号量 8。Phase 4 的 per-drill store slot pool（一次 `sudo -S`）只在 j≈16 的 M3 telemetry 显示 p99 持续 >10 ms 且有 ≥2 条按 disposition 归因于 fsync 的偏离时才触发（accel-plan R6 的触发条件仍然成立，只是分母变了）。

对照今天：-j6 27–28 min、-j12 20.7 min（= 96 的长度）。新地板 ≈9 min 是 **−55 %**，不是 −80 %。

## 裁决契约跨臂聚合

契约（`lib/assert.sh:6-24`）的 landing verdict 是四个计数器的纯函数（`drill_end`，:488-494：ASSERT-FAIL > SETUP-RED > PRODUCT-RED > INCOMPLETE > GREEN）。这给了一个**精确**的聚合法：

1. **臂级不改契约**：每个单元仍是 `simcluster drill <name>`（带 `ARM=<arm>` env）跑在自己的 instance 上，`drill_begin/drill_end` 照旧，emit 一条**字节不变**的 `DRILL-VERDICT … -- <drill name>`。追加一行 `DRILL-ARM arm=<A> of=<drill>`（与 `DRILL-EVIDENCE`/`DRILL-POLL-WAIT` 同款"追加行"，:498-512 的三个 parser 不动）。
2. **drill 级 = 计数器逐项求和 + 同一优先级函数**：`assert_fail/setup_red/product_red/not_covered/nc_gap/nc_guard/pass` 各求和，verdict 按 `drill_end` 的同一条 case 重算；`rc` 由重算的 verdict 派生。数学上它等于把各臂顺序跑在一个 drill 里得到的结果——**除了 abort 语义**：串行时一次 `assert_setup` abort 会让后续臂根本不记账，并行时后续臂照跑并记账。差异只会让计数器 ≥ 串行值，verdict enum 因取 max 不变，但 `expected_nc_gap` 可能变。这是**信息增加**（被 abort 遮住的后臂红现在可见），要在 expected 表里显式登记，不能悄悄发生。
3. **结构性 gap 归一个 owner 臂**：96 的两条无条件 `not_covered`（:346 "#58 cross-home GC"、:368 "arm B + arm C"）今天登记一次；拆成三臂若各自都跑到这两行就变成三次。规则：结构性 gap 只在 `[ "$ARM" = <owner> ]` 时登记；`tests/lint-drills.sh` 加一条 "同一 `not_covered` 描述串不得在多个臂可达"（AST 级不可判，退化成：在 manifest 里声明 owner，运行时对 nc 描述串去重并把重复计为 CONTRACT-ERROR）。
4. **INFRA-ABORT / CONTRACT-ERROR**：按单元判定（`effective_verdict`，`run-drills.sh:402-431` 的严格 parser 对每个单元 log 各跑一次）；drill 级：任一臂 CONTRACT-ERROR ⇒ drill CONTRACT-ERROR；任一臂 INFRA-ABORT ⇒ drill INFRA-ABORT（其余臂的计数器仍进 rollup 供人读，不进 verdict）。两者都不可重试，与今天一致。
5. **exit-code law 不变**：仍数"未豁免的非 GREEN **drill** 数，饱和 125"（run-drills.sh:44-46）。豁免 flag 也仍按 drill。
6. **expected-verdicts.tsv**：主键保持 `drill`（claim 集合是 drill 的），新增可选子行 `drill/arm`（如 `96/A`）承载：该臂的 `expected_nc_gap`、该臂的 bands（band 签名本来就是"第一条失败行"的性质，天然是臂级的——`_fail_context` 读的是单元 log :455-465）。`tests/validate-verdicts.sh` 加两条：子行的 nc_gap 之和 == 父行；父行有子行时父行 bands 必须为 `-`（band 只许挂臂）。`ledger-crosscheck.sh` 读 owner 列 —— 留在父行，不动。
7. **FLAKE_SIG 重试 / M4 归因 / `--drill-timeout`**：全部按单元。归因的 solo 重跑从此是"重跑那一臂"，而非 22 min 的整 drill。`DRILL_TIMEOUT` 2700 s 可降到 ≈1200 s（最长单元 ≈10 min 的 2×）。
8. **drill-costs.tsv**：键改成单元（`96/A`），仍只影响 LPT 顺序、不影响裁决；缺失的单元仍"排最前、按最大成本"。
9. **hermetic 门跟随**：`tests/r9d-nonvacuity.sh` 按函数名从 drill 文件逐字抽 oracle——臂放在**同一文件**、用 `case "$ARM"` 分派时它不受影响；`verdict-contract-test.sh` 加"两臂 verdict 行求和 == 合成 drill 行"的正负控制；`poll-mode-test`/`kept-sites`/`lint-drills` 按文件扫描，不动；`test/architecture/simcluster_gate_set_test.go` 对账的是 `tests/*.sh` 与 `run-all.sh`，新 hermetic 用例进 `run-all.sh` 循环即可；`simcluster_log_oracle_test.go` 不受影响（臂仍经 `drills/lib/logs.sh`）。

## runner 改动

| 项 | 改动 | 估计工作量 |
|---|---|---|
| 单元清单 | `drills/<name>.sh` 头部 `# arms: A D F`（无则单臂 `-`）+ `# fixture: N3 tpl|N3 live|N2 tpl|N1|N2 cap3g`；runner 解析成单元表；`tests/lint-drills.sh` 验证每个声明臂在文件内可达 `drill_end` | 40 行 bash + lint 30 行 |
| `simcluster drill <name> --arm <A>` | 注入 `ARM` env；instance 名 `drill-<name>-<A>`（`drill_begin` 的 `drill-*` 前缀检查 :469-473 仍成立）；`SIM_DRILL_ID=<name>.<A>`（evidence 文件名跟 :92-99 的约定） | 20 行 |
| 模板生命周期（L2 的接口） | `simcluster template build|restore <class>`；runner 在 sweep 开头按 image sha 命中缓存则跳过构建；未命中则构建单元先跑，模板消费者作为**依赖**等待（今天的 launch 循环 :731-741 没有依赖概念，需加 ready-file 门） | 60 行（不含 L2 自身） |
| lane 调度 | 三类：`tpl`（并发 j）、`grow`（30 s 错峰起跑 + 上限 g，默认 g=∞ 错峰、可 `--grow-cap`）、`live-N1`（并发 j）；bring-up 信号量（同时 ≤8 个 `up`）经 `SIM_UP_SEM` 文件锁实现 | 80 行 |
| `run_one` 按单元 | 现有 :372-400 几乎原样，多一个 arm 参数；`.log/.rc/.secs/.timeout/.runpid/evidence` 全按 `<name>.<A>` | 20 行 |
| 聚合 | 新 `aggregate_drill()`：读各臂 verdict 行，求和 + 重算优先级，产出 rollup.tsv 的 drill 行与 `ARM` 子行（parser 已被告知"有更短的 WAIVER/ATTRIBUTION 行"，加一种行型不破坏契约） | 60 行 |
| `classify_match` | 父/子行；子行 nc_gap 校验；band 只在子行匹配 | 40 行 + validator 40 行 + selftest 变异 ≥6 条 |
| M4 归因 | 队列元素改为单元 | 15 行 |
| drill 侧 | 15 个 drill 加 `case "$ARM"` 分派 + 各臂自建前置（96-F 的 seeds、73-Q 的先 kill、33-A 的独立夹具、52-D 的独立夹具…）+ 结构性 gap 归 owner 臂 | 每个 drill 半天到一天 **并且必须在真栈上跑那一臂**（memory：drill 断言必须真跑不是 lint；predicate 用 `sh -c` 包函数是 runtime not-found）。合计 ≈ 2 周 |
| 产品侧（可选但 ROI 最高的一条） | `awaitJoinerBrokerUpLocal`：driver 本次调用走了 render 分支（joiner 在 P5 已证不在线且 tether 从不跑 systemctl）时立即 HALT 而不等 60 s；resume 路径（未走 render 分支）保留 60 s grace 以覆盖注释描述的 crash-restart 周期 | 20 行 Go + 单测；每次 grow −55 s，影响全部真 grow 与模板构建 |
| 文档/闸门 | README drill 表加"臂"列；`test/README.md`/`simcluster_gate_set_test` 不变；`drill-costs.tsv` 重新播种 | 半天 |

总量：runner ≈ 350 行 bash + 2–3 周 drill 侧真栈验证 + 两次 instrumented sweep（B1 式 A/B）。

## 我不确定的地方

1. **所有臂时长都是估计**。`drill-costs.tsv` 对 90/41/42/50/71 等已过期（有的 p50 小于今天夹具的 solo 和），本机没有 `/tmp/simdrills*` 归档可读，我拿不到 `DRILL-POLL-WAIT`。第一件事应是一次 `-j6` instrumented sweep，把每条 `poll_until: condition met after Ns` 归到臂上，再决定拆哪些。
2. **67 的 777 s 从可见等待推不出来**（夹具 ≤190 + meta ≤120 + after-loop ≤180）。要么 restore 后的 push 在"JS 已 formed 但 tether-broker 因 nats 丢失退出/重启"期间每次拖到长超时，要么 p50 含了并发污染。这直接决定 67 是 2.5 min 还是 13 min 的单元。
3. **98 的检测窗到底是 4 还是 6 min**：我按 nats.go `processPingTimer` 推导为 4–6 min（取决于切断相位），drill 注释写"up to ~4min"、预算 330 s；若 6 min 成立，330 s 预算本身就偶发不够。需要读 nats.go 源码版本核对 `MaxPingsOut` 的比较是 `>` 还是 `>=`。
4. **模板与 grow 残留状态**：30/40/41/52 分支在 `#31` grow-lock 泄漏（间歇、每次 grow 独立采样）。模板化后 #31 每 sweep 只在模板构建时采样一次，且所有消费者看到同一份残留——采样率从 ~30 次/sweep 降到 ~10 次。30 我已建议保持真 grow；40/41/52 是否也保持，是覆盖率 vs 时间的裁决，不是技术问题。同理 grow 后的 `DEGRADED-WRITABLE` 观测标签（README:417-421）、74/73 的 proxy-eligibility 恢复窗——模板集群是"已稳态"的，这些**grow 后的瞬态**在模板消费者里将永远不可见。
5. **#70 在新宽度下的形状**：README 说 5 个同时 grow 打穿 150 s 窗、30 s 错峰 GREEN，但没有"g 与错峰间隔的曲线"。我给的 W_grow ≈ 9 min 建立在"30 s 错峰后并发不受限"这一未验证外推上；若必须 g≤3，grow lane 变成 13.5 min 并成为唯一的 wall 设定者，那时值得做的反而是把 grow-claim 单元本身拆细（10 的 kill/quorum 臂可以放到模板上，只留"grow 到 VOTER"那一段）。
6. **fsync 尾延迟在 60–70 容器（j≈16）下的数值**未测；M3 sampler 每 60 s 采一次，够用。若 p99 明显 >15 ms，raft 多节点 timeouts 是秒级不受影响，但 JS-meta 形成（#70 根因）会。
7. **96-A 的 390 s 窗与 1 GiB 载荷的真实预算不一致**：`proto.XferBudget("b", 1 GiB, legs)` 可能大于 5 min floor（`internal/proto/xfer.go:120`），drill 的"transfer timeout + 90 s"注释按 5 min 写，若 #57 真钉住，390 s 可能不够长——这是 drill 正确性问题，不是速度问题，但它决定 96-A 是 3.5 还是 15 min。
8. **并行臂的 abort 语义变化**（聚合第 2 条）会让若干 drill 的 `expected_nc_gap` 变化（后臂原本被前臂 abort 遮住）。这些变化必须逐条对照 expected 表手改并在 commit message 写理由，我无法在不跑的情况下预知有多少条。
9. **τ=60 s 的模板恢复时间**是 L2 lane 的数字，我只是借用；若 L2 的方案是 `docker commit`+卷快照，raft `ServerID`/cert CN/nats route 都按 hostname 寻址（README:76-78）所以跨 instance 可移植，但每 instance 独立铸造的 CA/account.nk 会变成全 sweep 共享——隔离仍由独立 bridge 保证，`82`/`72` 的 S0-ingress test-CA 复用逻辑要核一次。


---

# 草案 L4 oracle 形态：抽象 claim vs 时间编码

## 墙钟 oracle 清单（claim / 编码 / 为什么这么长）

先给一个总账（全部来自本次静态读码；drill 内的行号以当前树为准）：

| # | 站点 | 类型 | 当前编码 | 绑定的产品/库常量（file:line） | 抽象 claim（不含时间） | 为什么当前编码需要这么长 |
|---|---|---|---|---|---|---|
| O1 | `drills/96-mid-flight-chaos.sh:396` | **负窗口 + 老化前置** | `poll_until 390 30 … \|\| true` | `proto.XferTimeoutTierBFloor = 5m`（`internal/proto/xfer.go:61`；pull 路径 size==0 → 必取 floor，`xfer.go:104-106,120-123`）+ `xferStrandedSlack = 60s`（`internal/broker/xfer_inflight.go:37`）| "home broker 死亡期间，没有任何幸存者侧路径会为在飞传输写 terminal；terminal 只能由 home 回来后的 finalize-on-recovery 写" | 两个原因叠加：①负窗口要覆盖 watchdog 预算（5 min）之后"再无任何代码路径会写"；②**R16 的 finalize pass 自己就要求 start 行年龄 ≥ timeout+slack = 6 min**（`ledgerRowDisposition`，`xfer_inflight.go:723`），所以即便去掉负窗口，后面的 `_a57_try` 30×6 s 循环也要等到 6 min 才可能看到合成 terminal。注意：这 390 s **只在 kill 真落在飞行中时才付**——若 1 GiB 在 kill 前完成（drill 自述在 88 vCPU 上的常态，`96:114-117`），`_a1_terminal_row` 第一采样即真，0 s |
| O2 | `96:340-348` | 结构性 not_covered | 无等待，直接 gap | `serveconf.MinXferCrossHomeReapAge = 15m`（`internal/serveconf/serveconf.go:287`）| "leader 跨 home GC 会回收 split-home 的孤儿对象" | 外审 F2 把 floor 钉在 ≥3×tier-B；drill 窗口内不可观测 → 已放弃观测 |
| O3 | `96:478-479` | reaper 窗（正向） | `poll 60 3` agt1 ONLINE → `poll 90 3` 计数回到 floor | `xfer_reap_interval=8s`（YAML，最小 1 s，`serveconf.go:269-272`；drill 在 `96:349` 设）+ `xferReapMinObjectAge = 2m`（`transfer_reconcile.go:19`）+ `reaperCaughtUp`（`clusterwrite.go:896`）| "home 回来并追平 raft 后，其周期 reap 会把自己的孤儿对象回收到 tombstone floor" | 正向 poll，首次成功即返回；90 s 是 brk2 crash 后 raft 追平 + 8 s cadence 的余量。2 min 最小年龄在此不咬（O1 已让对象老化） |
| O4 | `73-proxy-cluster-ha.sh:298` | **测量-记录窗** | `if poll_until 180 6 … _exit_flows_fresh` | 无产品常量；gotcha #33 明写"不声称固定 lag"（`docs/deploy-tier-gotchas.md:146-152`）| "crash-rehome 后，控制面 ready 之后数据面是否自愈；记录 AUTO-RECOVERED(lag) 或 STRANDED" | 180 s 是"愿意称之为自愈"的运维上限；每次采样本身是 /sub 拉取 + ss-local 起 + 10 s listener poll + 8 s curl（`drills/lib/proxy.sh:51-52,68`），单样本可达 ~20 s。**只要 #33 仍 STRANDED，这 180 s 每次都付满** |
| O5 | `74-rebalance-on-return.sh:572` | **锁定窗（硬断言）** | `poll_until 180 5 "auto-even … (locked >=180s)"` | return dwell 6 tick×5 s = 30 s（`proxy_auto_rebalance.go:23-27`）+ `autoRebalanceQuietWindow = 60s`（`:97`）+ fire-gate "no in-flight op"（`:57-58`）| "在合法的 skew+return 边沿上，auto-rebalance 无需人工即发火" | 30 s dwell + 60 s quiet + observe 节拍 + 余量 ≈ 180，R6-M1 锁定为 release-blocking 窗；正向 poll，**只在不发火（#34/#31 挂起 op 关掉 gate）时付满** |
| O6 | `74:407`（B-dp）| 正向 poll，负结局付满 | `poll_until 240 5 … _ss_via_agent` | 无常量（#33 家族，根因未归因）| "被 rebalance 移动的那个 exit 的 SS 数据面在 home 变更后闭合" | 同 O4 的采样成本；240 s 只在搁浅时付满，然后 `_ss_fail_stage` 区分 harness/product |
| O7 | `74:200,359,474`；`74:185` | **稳定窗** | `_dist_stable`（两读相隔 3 s 相同）套在 `poll_until_fixed 45 3` / `90 3`；`_ktgt_stable_empty` 6×5 s | 无 | "分布停止漂移之后再选 kill 目标 / 再量 dry-run" | 稳定窗 = "在一段时间内没变"，本质是负窗口；固定网格是 #46 flap-bank 的防线（`lib/log.sh:75-80`；`tests/poll-mode-test.sh` 门） |
| O8 | `93-metrics-observability.sh:275-277` | 稳定窗 | `_wh_leader_stable`（两读相隔 3 s）套 `poll_until_fixed 90 3` | raft lease 抖动 | "webhook 臂开始前 leader 不在换届" | 同 O7 |
| O9 | `97-soak-cycles.sh:247,322,380` | **settle 定时器** | `poll_until_fixed "$SOAK_SETTLE" 5 -- false` ×(1+6+1)，`SOAK_SETTLE=25`（`97:102`）, `GOR_QUIESCE=SOAK_SETTLE`（`:108`）| 无 | "在每轮注入后的稳态采样 fd/RSS/goroutine" | 8×25 s = 200 s 纯等待；"稳态"没有被定义成可观测量 |
| O10 | `95-broker-selfheal.sh:176,200` | settle 定时器 | `poll_until_fixed 20 5 -- false` ×2 | systemd `StartLimitBurst=5/10s`（install.sh，drill 注释 `95:173`）| "下一臂的 restart 不会撞 start limit" | 环境（systemd）常量，不是 tether 的 |
| O11 | `33-node-upgrade-success.sh:243-244` | **watchdog 窗（等触发）** | `poll_until 210 5 "B4 conjunction"` | `upgradeRegisterDeadline = 120s`（`internal/agent/upgrade_state.go:67`；`UpgradeNowFn` 仅 Go Config 注入，`agent.go:136-139`）| "无法 re-register 的升级在 deadline 到期时整组回滚（marker/dst/.prev/MainPID/exe 五元合取）" | 必须**等触发**——回滚事件本身在 t+120 s 才发生；210 s 是 120 + re-exec/boot shim 余量。之后 `B6b poll 120 3`（`:253`）是正向 |
| O12 | `98-stuck-redial-recovery.sh:33-47` | **检测窗** | `RECOVERY_BUDGET=330` 一个共享 deadline | nats.go v1.52.0 默认 `PingInterval=2m`、`MaxPingsOut=2`（`nats.go:60-61`），stale 判定在第 3 个 tick（`processPingTimer`，`nats.go:5783-5784`）；tether 未设二者（grep 全仓无 `PingInterval`）；之后 `redialAfter=20s`（`roster.go:31`）+ `closeBudget/poisonGrace 10s`（`conn_teardown.go:54,57`）| "静默 DROP 后，连接被观测到离开被切 broker，且 heartbeat 在另一 voter 上越过 post-impact 水位——在写明的预算内" | 检测 ∈ (4, 6] min 取决于 DROP 落在 ping 周期的相位（我的推导，见"不确定"）；预算里 4 min 那一项是库默认，不是 tether 的 SLA |
| O13 | `22-forcesingle-online.sh:154` | **prod-ttl** | `sleep 61` | `forceSingleArmTTL = 60s`（`force_single_online.go:30`）| "超过 TTL 的 arm token 被以 `arm_expired` 精确拒绝" | 真时钟过期接线探针（R8 明确保留）|
| O14 | `22:110-114`；`22:236-240` | 观测循环 / **固定负窗口** | Arm-0 24×5 s（正向早退）；#35 12×3 s = 36 s 全跑 | `forceSingleDwell = 15s`（`:27`）；`RestartSec=2` | Arm-0："dwell 在 survivor 不重启时可达 WouldProceed"；#35："survivor 重启后 36 s 内每个 3 s 采样都不 proceed ∧ ≥2 个 crash PID" | #35 是"足够长时间里没发生"的负窗口，且要求**每个采样**非 proceed（不能早退）|
| O15 | `78-proxy-dial-backoff.sh:88-91`；`:141` | **测量桶（纯 sleep）** | `sleep 65`×3 = 195 s；`sleep 60`（D2 零拨号）| `proxyDialPolicy` 5 s base → 5 min cap（`internal/agent/proxy.go:701-710`）；broker home-delivery 5 s 重推（`broker.go:917`，`reconcile_passes.go:343`）| A："拨号次数总量 ≤ 无退避基线一半且逐桶递减"（fault-speed 解耦）；D2："opt-out 节点在 repair loop 持续推送下零拨号" | 桶宽要装下几个倍增周期（尝试落在 t=0,5,15,35,75,155,…）；D2 的"零"是负窗口。**注意：这 255 s 不在 accel plan §1.1 的 175 s 里——78 是 g75-g78 之后新增的**（我的 grep 全仓字面 sleep 合计 433 s，差值几乎全是 78）|
| O16 | `41-shrink-to-standalone.sh:282` | 正向 poll，SLA 绑产品抖动 | `poll_until 300 3`（3×RosterRefreshInterval）| `defaultRosterRefreshInterval = 3m`（`roster.go:24`）+ `jitterDur` UNIFORM(0,d]（`agent.go:2135-2137`）；`RosterRefreshInterval` 仅 Go Config（`agent.go:263`），无 YAML | "agt1 通过 silence-rebuild 逃离被 retire 的孤岛并在幸存者上有真实命令通路" | 最坏 jitter(180)+2×jitter(20) ≈ 225 s；`41:170-190` 记录 5 次实测 57 s～>210 s，且 210 s 窗口曾超时后被换成 gap（`41:215-217`）|
| O17 | `30-rolling-upgrade.sh:433,480` | **事件化负窗口（已是范本）** | 常驻写探针 + scene watcher，断"roll 期间无 not_leader/503/no-responders" | — | "HALT 留下的部分状态是可写的" | 窗口 = roll 自身时长，不是额外等待；这是本清单里**已经**用"事件流上没有 X"而非"T 秒"编码的负窗口 |
| O18 | `52-credential-rotation.sh:378` | 正向 poll，绑 keepalive | `poll_until 80 3` 数据面 DOWN | tunnel 30 s keepalive（drill 注释）| "旧 yamux session 在 keepalive 之后确实死亡（不只是新连接被挡）" | 必须等 keepalive 到期 |
| O19 | `40-drain-retire.sh:235` | 稳定游标 | `poll_until 45 2 "stable pre-removal cursor"` | — | "op 已持久到 pre-RAFT_REMOVED 游标，可以 kill leader 验 resume" | 正向，早退 |
| O20 | `96:569`（D3 300 s）、`96:711`（F 前置 360 s）、`96:626/737`（180 s）、`simcluster:347`（VOTER 150 s）、`67:79/155`（JS meta 60/120 s）| 正向收敛 poll | 首次成功即返回 | raft 选举 / JS meta 重组 / `OfflineAfter=60s`（`node/node.go:34`）| "集群在故障后重新收敛" | 这些不是 oracle 形态问题——花的是产品真实收敛时间（96:706-710 实测 F 前置 >240 s）；属 L3 |

**观察**：清单里真正"跑满预算"的只有 O1（条件性）、O4（当 #33 STRANDED）、O5/O6（当 #34 显形）、O9、O11、O12、O13、O14-#35、O15。其余是首次成功即返回的正向 poll，它们的墙钟是产品收敛时间，不是 oracle 编码的错。

## 事件化替代方案与等价性判定

判定标准：**等价** = 新 oracle 在每个可能的产品行为下给出与旧 oracle 相同的 verdict；**弱化** = 存在产品行为让旧 oracle 变红而新 oracle 绿（或反之，制造假红）；**等触发 vs 等超时后再验** = 事件在 t+T 才发生的，事件化不省 T。

| # | 产品已有 / 可合法拥有的观测面 | 事件化编码 | 判定 | 省多少（估计） |
|---|---|---|---|---|
| O1 | `tether admin runtime --json` 的 `reconcilers[]`：每个 pass 的 `Runs/Skips/LastTick/LastErr`（`internal/broker/runtime_introspect.go:39-75`；`adminsock/protocol.go:34`），含 `xfer-inflight-finalize` 与 `xfer-orphan-reap`（`reconcile_passes.go:281,296`）| "brk1/brk3 上 `xfer-inflight-finalize.Runs` 各 ≥2 且无 terminal 行" 替代 390 s | **弱化，且不省时间**：①finalizer 只扫**自己**的 `<ClusterDataDir>/xfer-inflight/` 账目，幸存者结构上不可能写——pass 计数证明的是"已知机制没写"，负窗口覆盖的是"未知机制也没写"（如 `handleEvTransfer` 的晚到 finalization）；②更根本：R16 的合成 terminal 要求年龄 ≥ 6 min（`xfer_inflight.go:723`），这是**产品的**老化前置，不是 drill 的选择。事件化可做的只有把 `_a57_try` 的 30×6 s 盲循环换成"`Runs` 自 brk2 重启后 ≥1 且 `LastErr` 为空 → 立即判"，省 ≤ 1 个 8 s cadence | ≈ 0（真时间地板 6 min）|
| O3 | 同上 `xfer-orphan-reap.Runs` + `/jsz` 对象计数（drill 已用）| "自 brk2 重启后 `Runs` ≥ 2 ∧ `reaperCaughtUp`（可从 `cluster status` 的 applied/commit 推）∧ 计数 ≤ floor" | **等价**（正向 claim，计数是效果本身；`Runs` 只把"reaper 跑过"从时间推断改成直读）| 每次 ≤ 8 s；但 drill 的 `poll 90 3` 本就早退 |
| O4 | `proxy_node_unready` sys.event（`pubSysEvent("proxy_node_unready")`）+ `admin events --kind`（`cmd/tether/admin.go:58`，有 `--since/--kind/-n`，**无 `--follow`**）+ `/sub` 渲染 | "rehome 后 flap ≥K 次 ∧ /sub 不渲染该 exit ⇒ STRANDED" | **弱化**：STRANDED 的旧定义是"180 s 内字节不通"，包含"ready 稳定但黑洞不 flap"这一形态；flap 计数只覆盖已观测到的机制（#33 明写根因未归因，`gotchas.md:151`）。可作为**早判 corroborator**（flap 已 ≥K 且 /sub 不渲染 → 提前记 STRANDED），但不能删掉字节探针 | STRANDED 分支从 180 s 降到 flap 周期×K（估计 30–60 s）；AUTO-RECOVERED 分支本就早退 |
| O5 | `proxy_auto_rebalanced` sys.event（drill 已作 C-event 读）；`cluster ops ls --json` 的 in-flight op（drill 已作诊断，`74:568-570`）| 正向：事件到即判（已然如此）。负向早退："gate 被非终态 op 关闭 ⇒ 立即 RED" | **负向早退是弱化/假红**：gate 状态是瞬时的，op 可能在窗内终结然后 auto 在 160 s 发火——时间 oracle 判绿、gate oracle 判红。只有"op 的预计终结时间 > 窗口"可证明时才等价，而 #31 泄漏的 op 恰恰没有终结时间 | 0（保持 180 s；这是 R6-M1 锁定的 release 语义）|
| O6 | 无（#33 根因未归因）| — | 不可替代 | 0 |
| O7/O8 | 无需新面：稳定窗本身已是"两读相同"的采样式 oracle | 保持 `poll_until_fixed` | 事件驱动是**反向工具**（0 间隔 = 最密采样 = 最易 bank 瞬态；`lib/log.sh:75-80`）| 0 |
| O9 | `admin runtime` 的 `Goroutines/OpenFDs/RSSBytes`（同一 verb）| "连续两次采样（间隔 5 s）goroutine 差 ≤ε ⇒ 稳态" 替代固定 25 s | **近等价但有采样偏差**：leak-slope oracle 要求各周期在**可比相位**采样；"停止变化"可能 bank 一个平台期；固定 settle 简单诚实。可接受的折中：`min(25s, 稳态检出)` 只在 GOR_QUIESCE 用 | ≤ 200 s → 估计 60–100 s（单 drill）|
| O10 | `systemctl show -p ExecMainStartTimestampMonotonic` | "距上次 start ≥ 10 s 即放行" | 等价（同一环境常量的直读）| ≤ 20 s，不值得 |
| O11 | agent slog `agent: upgrade watchdog armed (pending register deadline)`（`upgrade_state.go:634`）+ marker 文件的 deadline 戳 | 等**触发**：仍 120 s。事件化只能把"合取成立"的确认从 5 s 网格改成 marker 变更即读 | **等触发**，不可压；把 deadline 做成 YAML 可调 = 只有测试想要更短（`upgrade_state.go:63-66` 明写"no tunables without a use case"）→ 测试专用 seam | ≤ 5 s |
| O12 | nats.go `DisconnectErrHandler` 日志行 + `/connz`（drill 已用权威源）| 事件化不省检测时间。**唯一能压的是产品设 `nats.Options.PingInterval/MaxPingsOut`**（如 20 s×2 → 检测 ≤ 60 s）| 这是**行为变更**而非观测面；但它有独立运维价值（NAT 后 agent 4–6 min 才发现 broker 死亡是 #72 事故的一部分），且 drill 的 claim 是"写明的预算内"——预算会随之改写，claim 不弱化。需产品裁决 | 若产品改：330 s → 估计 ≤ 120 s；不改：0 |
| O13 | 无 | 无 | 真时钟 | 0 |
| O14-#35 | journal 的 `_PID` 去重（drill 已用）+ `systemctl show NRestarts` | "≥2 个 crash PID 出现即可判"——但 claim 还含"36 s 内**每个**采样 DRY 不 proceed"，这是负半 | 正半可早判；负半是真负窗口（dwell 15 s 的 2 倍余量）| 36 s → 估计 ≥ 20 s |
| O15 | **今天没有**：agent 侧退避状态只在 Debug 日志（`proxy.go:558,727-729`；WARN 只首发），`ProxyDialRetryBase/Cap` 仅 Go Config（`agent.go:239-240`），`proxy status --json` 无退避字段。**可合法新增**：`proxy status` 节点行加 `dial_fails / next_dial_at`（secret-free 标量）；broker 侧 `home-delivery.Runs`（`reconcile_passes.go:343`）已可读 | A："next_dial_at−now 逐次几何增长 ∧ 包计数器在各 next_dial_at 处恰好 +1" ≥3 个间隔（5+10+20 = 35 s + 余量）替代 3×65 s；D2："`home-delivery.Runs` 前进 ≥12 ∧ 包计数 Δ=0" 替代盲 60 s | A：**等价且更强**（同时验证 agent 的自述与 netfilter 的事实；旧 oracle 只有事实）；D2：等价（"12 次真推送"比"60 s"更贴 claim）| 255 s → 估计 80–110 s |
| O16 | `agent_registered` sys.event + `/connz`（drill 已用 journal 捕获）| 正向早退已然；SLA 是产品 jitter | 不可替代；把 `RosterRefreshInterval` 暴露成 agent.yaml 是**有运维用例的**（大车队错峰 / 小车队快收敛）但 drill 注释明写"stock 3-min NOT shortened — the mandate forbids tuning the env"（`41:277-278`）——新前提下这条要重新裁定，见下节 | 若可配：300 s → 估计 ≤ 60 s |
| O18 | tunnel keepalive 是产品常量 | — | 等触发 | 0 |

## 观测面合法性判据

以外审 F2（`xfer_cross_home_reap_age` 钉 ≥15 min）与 R8（Go 后继 ≠ 接线证明）为界，我把"能进产品的运维观测特性"与"测试专用 seam"分成五条可机械问的问题；一个面要全部通过才算运维特性：

1. **无测试时运维会不会问这个问题？** `admin runtime` 的 pass `Runs/LastTick` 回答"reconciler 卡了吗"（`runtime_introspect.go:3-6` 就是这么写的）；`proxy status` 的 `next_dial_at` 回答"我的节点为什么不拨号"（#78 现场正是这个问题）。反例：`upgradeRegisterDeadline` 可调——运维只会想要**更长**（弱 NAT），更短只有测试要。
2. **它只暴露状态还是改变行为？** 暴露型（runtime/events/ops timeline/status 字段/metrics）零风险；行为型（PingInterval、RosterRefreshInterval、tier-B floor）必须有独立于测试的用例，且不能碰**安全 floor**——F2 的理由是"更低的 floor 让 leader 删掉另一 home 仍活着的对象"，这是生产数据安全论证，与 Mandate 无关，**新前提下仍然成立**。
3. **能否让测试改看产品的"自述"而不看"效果"？** 允许作 corroborator，禁止作唯一 oracle——`drills/lib/events.sh:34-38` 已写"CORROBORATION. Never gate a drill's spine on it"。O15 的方案之所以等价，是因为它把自述与 netfilter 事实**配对**。
4. **是不是隐藏路径？** `adminsock/protocol.go:31-33`：OpRuntime 特意是"normal operator verb… NOT a build-tag / env / hidden path (that would be a test backdoor)"。任何 `TETHER_TEST_*` 环境变量都不过关。
5. **是否新增攻击面或泄密？** pprof 被拒（`runtime_introspect.go:8-16`）、events payload 全是 allow-list 标量（`rehome_events.go:9-13`）——新增字段沿用同一纪律。

按这五条：`admin runtime` pass 计数、`admin events`、`cluster ops` timeline、`/connz`、marker 文件、audit 行——**都已存在且合法**；`proxy status` 加退避字段——**可进产品**；`PingInterval` 默认值、`RosterRefreshInterval` YAML 化——**行为变更，需产品裁决，有真实用例**；`upgradeRegisterDeadline`、`forceSingleArmTTL`、`XferTimeoutTierBFloor`、`MinXferCrossHomeReapAge` 可压——**测试专用，拒**。

上次 plan §3 那七条否决的重审（只列与本视角相关的）：
- **fast clock / 时间 namespace**：技术理由（vDSO、只能偏移、split 单调/实时钟）与 Mandate 无关，仍成立。
- **测试专用产品 seam**：理由是 F2 的生产安全 + "no tunables without a use case"，与 Mandate 无关，仍成立；但它被过度概括了——**暴露型面**（判据 2）从未被 F2 否决，只是没人区分。
- **fixture 快照**：Mandate ①/③/④ 理由，新前提下 Mandate 可动——但快照绕过的是 spine（L1），不是 oracle。
- **tmpfs**：对有持久性断言的 store 仍不行（是 claim 本身的要求，不是 Mandate 的）。
- **ENV-RED 第六态 / 放宽 FLAKE_SIG**：与本视角无关，且理由（洗白向量）不依赖 Mandate。

## poll 网格与事件驱动

**现状**（`lib/log.sh:66-80,122-131`）：363 个 `poll_until` 站点（上次 plan 写 315；我 grep `drills/ drills/lib/ lib/ simcluster` 得 363），间隔直方图 1 s:35 / 2 s:99 / 3 s:171 / 4 s:19 / 5 s:33 / 6 s:5 / 30 s:1；数值 timeout 之和 22 713 s（这是**预算**总和，不是实付）。fast-start 后：首个 interval 内每 1 s 采样，之后按 interval；`date +%s` 整秒量化。

**overshoot 推导（估计）**：
- 首采样即真：overshoot = 谓词延迟（`docker exec` ≈ 0.05–0.1 s；`$SIM ctl --` = docker exec + tether CLI + NATS RTT ≈ 0.3–1 s；SS 探针 8–20 s）。
- interval 内成真：≤ 1 s。
- interval 后成真：U[0, interval) → 平均 ≈ 1.5 s（直方图均值 ≈ 2.9 s，去掉 30 s 那个）。
- 每次 sweep 实际执行的 poll 数：估计 43 drill × 20–30 = 900–1300 次；假设 30–40% 落在第三类 → 300–500 次 × 1.5 s ≈ **7–12 min 的 sum**；再加所有采样的谓词延迟（每次 poll 平均 5–10 个采样 × 0.3 s）≈ 3–5 min。合计 **≈ 10–17 min / ~216 min sum（4–8%）**，与上次 plan 的 5–11 min 同量级偏高（站点多了 48 个）。-j 12 下摊到墙钟 ≈ **1–1.5 min**。
- 无法更准：机器上没有任何 `DRILL-POLL-WAIT` / `rollup.tsv` 归档（`/tmp/simdrills*`、`find` 全盘无果），而且 `poll_wait_total` 自述**漏计**所有在 `assert_ok` 子壳里跑的 poll（`lib/log.sh:145-150`）——即便有归档也是下界。

**事件驱动三种机制的收益/风险**：

| 机制 | 能替代哪类站点 | 省 | 风险 |
|---|---|---|---|
| inotify（容器内 `inotifywait` 或宿主看 volume 路径）| `sim_broker_slog_grep`/agent.log 类日志谓词（估计占站点 20–25%）| 该子集的 grid overshoot（≈ 2–3 min sum）| ①**再次撞 `fs.inotify.max_user_instances`**——正是 README 记录的那个 per-UID 计数（已提 8192，43 drill × 每 drill 几个实例够用，但它是同一根火柴）；②broker.log 是进程内封顶轮转（`internal/logrotate`），watch 文件会在轮转时丢，必须 watch 目录；③宿主侧 volume 是 root-owned，sim 用户读不到；④h1 把四条流拆开的教训（`simcluster_log_oracle_test.go`）——事件源一多，映射漂移面也多 |
| `nats sub tether.v2.sys.events`（已有 `events.sh`）| `proxy_*`/`home_reassign_*`/`agent_registered`/`disk_pressure` 类事件谓词 | 对应站点的 grid overshoot（≈ 1–2 min sum）| 只见订阅后发布的事件；nats-server 重启即掉（`events.sh:34-36`）——所以只能 corroborate 不能做 spine 门；`admin events --since` 持久但又是 poll |
| `/connz` `/jsz` `admin runtime` HTTP/socket 直读 | 已是 poll，延迟 ms 级 | 无额外收益 | — |

**#46 类 flap 被 bank 的风险是结构性的**：事件驱动 = 0 间隔采样，`poll_until` 首次成功即返回（`log.sh:83`），所以任何"稳定/收敛形"谓词（O7/O8 及 `_dist_stable` 家族）必须留在 `poll_until_fixed`；这已被 `tests/poll-mode-test.sh` 门住（accel plan §8 MAJOR-1 的教训：第一版全局翻 fast 漏了 6 个 effectful 谓词）。事件驱动改造要么把这道门原样带上，要么会重演。

**结论**：网格层最多再省 ≈ 1–1.5 min 墙钟（-j 12），不到目标差距的十分之一，且带着 inotify 上限与日志映射两条已知伤疤。不是这一层的杠杆。

## 只能用真时间证明的 claim 名单

假设 L1（spine）、L2（并发/调度）、L3（产品收敛常量）全部上齐，oracle 形态层**仍然**要付的真时间（每项给出绑定的常量与最小值；"估计"标明）：

| 编号 | drill | claim | 绑定常量 | 真时间下界 |
|---|---|---|---|---|
| T1 | 96-A | home 死亡期间无 terminal；home 回来后 finalize 只在 start 行年龄 ≥ timeout+slack 时合成 | `XferTimeoutTierBFloor 5m` + `xferStrandedSlack 60s` | **≥ 360 s**（在 kill 真落飞行中的 run；否则 0）|
| T2 | 98 | 静默 DROP 后连接离开被切 broker 并在他处恢复，预算 = 检测 + ≤60 s | nats.go `PingInterval 2m × MaxPingsOut 2`，stale 在第 3 tick | **(240, 360] s 检测 + ≤ 60 s**；唯一压法是产品设 PingInterval（行为变更）|
| T3 | 33-B | register deadline 到期即整组回滚 | `upgradeRegisterDeadline 120s` | **≥ 120 s** + re-exec 余量 |
| T4 | 22 | 超 TTL 的 token 精确拒绝 / 15 s dwell 可达 / survivor 重启下 36 s 内 dwell 不可达 | `forceSingleArmTTL 60s`、`forceSingleDwell 15s`、`RestartSec 2` | **≥ 61 + 15 + 36 ≈ 112 s** |
| T5 | 41 | agt1 经 silence-rebuild 逃离孤岛 | `RosterRefreshInterval 3m` full-jitter ×3 | **最坏 ≈ 225 s，实测 57–>210 s**；YAML 化可降但需裁决 |
| T6 | 74-C / 74-B-dp / 73-#33 | auto-rebalance 在锁定窗内发火 / moved exit 数据面闭合 / crash-rehome 数据面自愈 | dwell 30 s + quiet 60 s；#33 无归因 | **只在 #34/#33 显形时付满 180/240/180 s**——而它们今天就显形；这是"产品红 = 时间贵"的诚实代价，不是 oracle 的错 |
| T7 | 78-A / D2 | 退避几何加深 / opt-out 零拨号 | base 5 s 倍增；home-delivery 5 s | 今天 255 s；加观测面后 **≥ 35 s（三个倍增间隔）+ 12 次推送 60 s → 估计 80–110 s** |
| T8 | 52-A7 | 旧 tunnel session 过 keepalive 后死亡 | 30 s keepalive | **≥ 30 s** |
| T9 | 96-#58 cross-home GC | leader 跨 home GC 回收 | `MinXferCrossHomeReapAge 15m` | 已放弃观测（not_covered）；要观测就是 **≥ 15 min** |
| T10 | 97 | 稳态采样 | 无常量 | 可压到估计 60–100 s，但"稳态"定义会引入采样偏差 |

**对"≤ 5 min 墙钟"的地板判断**：墙钟 = max(drill)。T1（6 min + spine）与 T2（4–6 min + spine）**各自单独就超过 5 min**，且都绑在产品/库常量上、不由 oracle 形态决定。因此：
- 不改任何产品常量：oracle 层的诚实地板 ≈ **6.5–7 min（96-A 拆成独立 drill 后）或 7–8 min（98）+ 各自的 spine**；96 不拆时整条 drill ≈ 8–10 min（估计，因 F 前置实测 >240 s）。
- 若产品把 PingInterval 设为运维合理值（行为变更，独立价值）：T2 降到 ≤ 2 min；剩 T1 一根柱子。
- T1 唯一的绕法是承认 96-A 在 sim 里**本来就构造不出**（drill 自述 1 GiB 在 kill 前完成，`96:404-406`）——那 390 s 今天多半根本没付，96 的 22 min 花在别处（F 前置、D3、spine、#58 臂）；但这是归因问题，见下。

## 我不确定的地方

1. **96 的 22 min 究竟花在哪**：没有任何带 `DRILL-POLL-WAIT`/`condition met after` 的归档日志可读，`poll_wait_total` 又漏计子壳 poll。我按代码推 A 臂 390 s 是条件性的（常态 0 s），但无法证实 F 前置 360 s、D3 300 s 实付多少。这直接影响"96 拆臂后 max 臂多长"的估计——上次 plan §1.2 说的"归因先于优化"在这里仍然成立。
2. **98 的检测窗上界**：按 nats.go `processPingTimer` 读码，stale 在第 3 个 tick，最坏 6 min；drill 写"up to ~4min"、预算 330 s < 360 s。要么我的相位推导有误（例如 PONG 处理重置了 timer——我看到 `processPong` 只清 `pout`，未 Reset 定时器），要么该 drill 存在一个未被记录的最坏相位 flake。**只读，未验**。
3. **drill 78 的 255 s 未被计入 accel plan 的 175 s**：我用字面 sleep 求和得 433 s，含循环体内的 sleep（96:422、22:112、74:185 等按单次计），所以 433 不是实付；但 78 的 255 s 是确定的纯阻塞。
4. **`proxy status` 加退避字段是否会被视作 N-1 wire 变更**：它是 additive/omitempty 标量（judgement：合规），但我没核 `wire_inventory` 账本对 `ProxyStatus` 的覆盖范围。
5. **O5 负向早退的"op 预计终结时间"**：`cluster ops` 是否暴露 op 的 deadline/预计终结——我没读 op 控制器的 schema；如果暴露且可证明 > 180 s，负向早退可以等价。
6. **RosterRefreshInterval YAML 化的裁决**：`41:277-278` 把"不缩短"归于 Mandate；新前提下 Mandate 可动，但它同时也是"生产 3 min 才是被测对象"的保真论证——两个理由我分不开，需要用户裁定。
7. **poll 数量估计**（900–1300 次/sweep、30–40% 晚成真）是拍的，误差可能 2×；不影响结论（网格层不是杠杆）。
8. **grep 计数的可复现性**：363 站点用的模式是 `poll_until(_fixed)? +[^ ]+ +[0-9]+ `，会漏掉 timeout 以变量给出且 interval 非字面的站点（如 `97:247` 的 `"$SOAK_SETTLE"` 被计入、但完全变量化的可能漏）；上次 plan 的 315 用的是什么模式我不知道。


---

# 草案 L5 基础架构替代 + 怀疑者

## 基础替代评估表

**先给结论，再给表：容器/虚拟化基础不是时间去处。** 实测 `00-skeleton` 的 `up`（3 容器 + systemd 启动 + install.sh）只要 3.8 s，N=3 主干的 `up` 5 s；而 N=3 主干共 309 s，其中 `grow brk2`+`grow brk3` = 290 s（94%）。换掉 Docker+systemd 最多省下每个 drill ≈3–5 s（全套 sum ≈3.6 min，1.6%），却要付出下面表里列的保真度代价。时间在 **tether 自己的 `cluster add` 驱动器**（`joinerBootGrace` 60 s/次，`cmd/tether/cluster_add_drive.go:692`）、**产品时间常量**与**开放缺陷的满预算等待**里，不在基础层。

一个决定性的依赖必须先说明：**`tether cluster add` 的 former-N1 cutover 自己依赖 systemd `Restart=always`**——`internal/broker/cluster_grow_cutover.go:20-22` 对 nats-server 做同 uid SIGKILL（`nats-server --signal stop`）然后靠 systemd 复活成 clustered（`scripts/install.sh:1225-1226` `Restart=always`/`RestartSec=2`），并以 `cutoverGraceTimeout = 45 s`（`:26`）轮询复活。所以"去掉 systemd"不是丢掉 drill 95 的被测对象那么简单：**26 个 N≥2 drill 的 grow 主干在没有 supervisor 的环境里根本走不通**。

| 方案 | 节点冷启动 | 丢掉什么 | 得到什么 | 迁移成本 | 判定 |
|---|---|---|---|---|---|
| **Docker + systemd-in-container（现状）** `--privileged --cgroupns=host`（`Dockerfile:10-11`、`lib/docker.sh:117-121`） | 实测 `up` 3 节点 3.8 s（含 install.sh）；`wait_sysd` 判据是 `is-system-running` ∈ {running,degraded,starting}（`simcluster:45-51`） | — | 43/43 drill 的全部依赖（systemd 单元语义、journald、install.sh 真路径与属主、in-netns iptables、/dev/fuse、tmpfs `--cap-store`、`docker kill` 当断电近似） | 0 | **保留**。它不是瓶颈 |
| Docker 去 systemd（tini/supervisord） | 估计 0.3–0.5 s/节点，省 ≈1–2 s/节点 | `Restart=always`（95 的被测对象、**grow cutover 的前提**）、`systemctl` 存在性（install.sh `--no-enable` 之外会失败 → Mandate ①）、journald（`logs.sh` 四流之二：95 的 `Deactivated successfully` vs `code=killed` 判别）、`MainPID`/`ExecMainStartTimestamp`（33、98 的恢复路径分类）、cgroup 收割（81 的 #26 反证） | ≈1.5% 的 sum | 中（15 个 drill 直接调 docker/sctl，367 处引用） | **否决**：grow 走不通，且省的时间可忽略 |
| podman rootless + systemd | 与 Docker 同量级（估计 1–2 s；crun 比 runc 快 ≈0.1 s） | `--privileged` 语义不同：rootless 下 iptables/nft 在容器 userns 内可用（需 `--cap-add NET_ADMIN`）、`/dev/fuse` 可透传，但 `--cgroupns=host` 挂 `/sys/fs/cgroup:rw` 的形态没了（cgroup v2 委派：本机 `user@1000` `Delegate=no`，只委派 cpu/memory/pids） | 每 UID 计数器（inotify）改由 subuid 承担；无 root 守护进程 | 中：未安装（`apt-cache` 有 4.9.3 候选）、`newuidmap` 缺失、每次装都要一次 sudo | **不值**：零速度收益，换一套 privileged 语义的坑 |
| systemd-nspawn | 估计 ≈1 s（比 containerd 路径轻） | 无本质丢失（privileged nspawn：`--bind /dev/fuse`、netns 内 iptables OK、journald 原生、`--tmpfs`）；隔离网络要 `--network-veth`+bridge | machinectl 原生 journald 聚合 | 高：`systemd-container` 未装；**每次启动需 root**（`sudo -n` 失败） | **本机不可行**；即便可行也只省 ≈2 s/节点 |
| LXD/LXC 系统容器 | 估计 1–2 s（`dir` 存储池要 rsync 358 MB 根 → 反而 5–10 s；zfs/btrfs 池才是秒级 CoW 克隆） | privileged 模式下同 Docker；unprivileged 下 FUSE 需 `security.syscalls.intercept`；`docker kill` → `lxc stop --force` | **原生快照/克隆**（`lxc copy` CoW）——"grow 一次、克隆 N 份"的载体；lxd 组成员无需 sudo | 高：`lib/docker.sh` 183 行 + 15 个 drill 里 367 处 docker 直接引用 + run-drills 实例命名 | 唯一有"结构性新能力"（快照）的方案，但快照本身可用 docker volume tar 实现；**不推荐为此迁移** |
| Firecracker / cloud-hypervisor microVM | VMM ≈125 ms（Firecracker 官方数）；带 systemd 的 Ubuntu guest 估计 2–5 s | 无 `docker exec`（要 vsock agent 或走已有的真 sshd）；每 VM 内存 ≥512 MB（单波 ≈200 VM ≈100 GB，本机 251 GB 勉强） | **唯一能做真断电**（page cache 随 VM 丢失——accel plan §3 承认 `docker kill` 做不到）、virtio-blk 真 fsync、每节点独立内核；Firecracker 有运行态快照但**无跨 VM 一致快照**，且恢复后 guest 时钟跳变会制造 raft/lease 的"假长停顿" | 很高：二进制未装；tap 网络需 root；镜像/内核/vsock 全套新建 | **保真度更高、速度不更快**；以后若要"真断电"再谈 |
| 宿主裸进程 + `unshare -Urn`（userns/netns/veth，overlayfs 根，install.sh 装进"假 root"） | ≈0（进程启动 50–200 ms） | 与"Docker 去 systemd"同一份损失清单 + install.sh 的 `systemctl` 调用无处可去；属主语义靠 subuid 映射（`/etc/subuid` 有 `weiland:100000:65536`，但 `newuidmap` 缺失） | 最轻；无 root 也能建私有 veth/bridge（都在同一 userns 里） | 高 | **否决**：grow 走不通 |

**关于"fast clock"类基础**（上次 §3 第一条的两个新分支，补进本表）：
- **KVM TSC scaling / DieCast 式时间加速**：唯一能让 Go 二进制（vDSO 读 guest TSC/kvmclock）+ nats-server + hashicorp/raft **整栈**看到更快时钟的机制。代价：I/O 不会跟着变快——6.4 ms 的 fdatasync 在 10× 加速的 guest 眼里是 64 ms，raft `MultinodeElectionTimeout=1000ms`（`internal/cluster/node.go`）与 fsync 尾部的比例被改写，这正是 drill 83 试金石说的"改变可见 bug 集"。DieCast 文献里保真的方向是**放慢**（dilation），不是加速。本机还缺 root（tap）。**否决，理由是保真度而非可行性。**
- **产品内 `Now` 注入 seam 当 fast clock**：`broker.Config.Now`/`agent.Config.Now` 已存在（10 个生产文件用 `cfg.Now()`），但它只影响比较，不影响 `time.Sleep`/`time.After`（如 `cluster_grow_cutover.go:222`），更碰不到 nats-server 的 JS meta raft、nats.go 的 ping（98 的 4 min）、hashicorp/raft 的选举计时。**结构上不完整**，见"重审"第 2 条。

## 各方案 drill 兼容性

按"claim 依赖被丢掉的哪一部分"分组（drill 编号；依据 README drill 表与各 drill 头注）：

| 被丢掉的部分 | 依赖它的 drill | 备注 |
|---|---|---|
| systemd `Restart=always` 复活 nats（grow cutover 前提） | **所有 N≥2 drill**：10 11 12 13 20 22 30 40 41 42 50 51 52 67 71 73 74 82 90 91 92 93 95 96 97 98（26 个） | `cluster_grow_cutover.go:20-22`；无 supervisor 则 `cluster add` 在 mesh-cutover 处 `growCutoverRevivalFailed` |
| systemd 单元语义本身（MainPID、ExecMainStartTimestamp、`Deactivated successfully` 措辞、`enable --now`） | 95（#23 行为证明）、33（三次 exec 同 PID）、30（colocated agent 系统单元 + HALT/resume）、31、98（恢复路径按 MainPID 分类）、32（install.sh 装/卸单元）、13（`User=tether` reconciler 写 nats.d/） | 95 的两句 journal 措辞互为判别器，journald 不可替代 |
| journald（`logs.sh` 的 broker panic / boot 流） | 95、96、97、98（panic 扫描）、30（roll 日志） | `test/architecture/simcluster_log_oracle_test.go` 守四流 |
| install.sh 真路径 + root/tether 属主 | 13、32、doctor；间接：全部（provision-node.sh 走真 install.sh） | userns 方案下容器内属主语义保留，宿主侧看到的是 subuid |
| cgroup 收割 | 81（#26 反证："under systemd the cgroup reaps it"） | |
| in-netns iptables（需 CAP_NET_ADMIN） | 96 97 98（silent DROP 分区）、78（`fault_reject_on`）、33（`fault_synblock_on`） | rootless 需 `--cap-add`，nspawn/LXD/VM 原生 |
| `/dev/fuse` | 62（hangfs） | |
| tmpfs `--cap-store` | 21、90-M6 | |
| `docker kill` 作断电近似 | 12 20 22 95 96 97（`node_kill`）、94 | VM 会更真（真丢 page cache）；容器类方案等价 |
| 跨进程 route mTLS | 全部 N≥2 | 任何多进程方案都保留 |
| 真 sshd | 无 drill 断言它 | 只是调试路径 |

结论：只有 **Docker+systemd（现状）/ nspawn / LXD-privileged / VM** 四种是 43/43 兼容；后三者本机都要一次性 root 或未安装，且都不比现状快。

## 本机能力（只读检查结果）

| 项 | 结果 |
|---|---|
| Docker | 29.6.1（API 1.55），`overlayfs`，cgroup v2 + systemd driver，runc v1.3.6，`Experimental: false`（无 CRIU checkpoint） |
| CRIU | 未安装 |
| systemd-nspawn / podman / lxc-start / firecracker / cloud-hypervisor / crun / gVisor / tini / supervisord | 全部未安装（`apt-cache` 有 `systemd-container 255.4` 与 `podman 4.9.3` 候选，装需 sudo） |
| qemu-system-x86_64 | 已装；`/dev/kvm` 存在且用户在 `kvm` 组 |
| unshare / nsenter / bwrap / ip | 已装；`user.max_user_namespaces=1030640`，`unprivileged_userns_clone=1`；`/etc/subuid` 有 `weiland:100000:65536` 但 **`newuidmap` 缺失** |
| cgroup 委派 | `user@1000.service` `Delegate=no`，用户 slice 只有 `cpu memory pids` |
| sudo -n | **失败**（`a password is required`，与 accel plan R4 一致） |
| 内核 / CPU / 内存 / 盘 | 6.8.0-139，88 vCPU，251 GB（free 194 GB），`/` ext4 on `ubuntu--vg` 913 GB 用 8%；`/dev/shm` 126 GB |
| sysctl | `fs.inotify.max_user_instances=8192`、`max_user_watches=1048576`、`pid_max=4194304` |
| 镜像 | `tether-sim:dev` 358 MB（2 周前）；vendor: tether/tether-next（09-04）、nats-server v2.14.6 |
| Go | go1.26.8 |
| **LXD** | ⚠ **副作用披露**：我以为 `/usr/sbin/lxc` 是 LXC，执行了 `timeout 10 lxc version` 做探测；Ubuntu 24.04 的 `/usr/sbin/lxc` 是 `lxd-installer` 的 socket 激活 shim，它以 root 启动了 `snap install lxd --channel=5.21/stable/ubuntu-24.04`（`snap changes` #2，已 Done：`lxd 5.21.7-1018661`）。我没有 sudo，无法中止/回滚。**这违反了本任务"不改任何东西"的约束**——虽然仓库树未动、LXD 未 `init`、对 Docker 与 drill 无影响，但宿主上多了一个 snap。若不需要，`sudo snap remove lxd` 可清除。 |

## 三张地板表

**计量前提**（引用与推导）：
- 现状 sum：`drill-costs.tsv` 40 行合计 12,944 s（215.7 min，2026-07-23 `-j 6` 负载下的 p50）；未列的 78/83/84 估计合计 ≈600 s → **sum ≈ 226 min（43 drill）**。最长 = 96 = 1337 s。
- 主干份额（按各 drill 的 `grow_to_3`/`grow_to_2`/`grow brkN` 调用数分类）：两次 grow 15 个（10 13 30 40 41 42 71 73 74 90 91 93 96 97 98）、一次 grow 11 个（11 12 20 22 50 51 52 67 82 92 95）、N=1 17 个。以 2026-09-18 空闲实测（N=3 主干 305 s、N=2 191 s、N=1 ≈20 s）算：15×305 + 11×191 + 17×20 = **7,016 s ≈ 117 min ≈ sum 的 52%**。
- 其中 `joinerBootGrace` = 41 次 grow × 60 s = **2,460 s ≈ 41 min ≈ 18%**（`cluster_add_drive.go:692`；第一次调用时 joiner daemon 按设计是停的——`simcluster:264` 先 `sctl stop tether-broker`，驱动器在 `:171` 刚因 `!joinerBrokerUpLocal` 渲染了 conf，`:203` 仍无条件等满）。
- 容器启动份额 ≈ 43 × 5 s ≈ 3.6 min（1.6%）。
- 真阻塞 sleep：175 s（accel plan §1.1）。
- 长 drill 的产品常量窗口（file:line）：96 `poll_until 390`（`drills/96:396`；= `XferTimeoutTierBFloor` 5 min `internal/proto/xfer.go:61` + 90 s）、`poll_until 300` JS meta 重组（`:569`）、`poll_until 360` 全恢复门（`:711`）；98 `RECOVERY_BUDGET=330`（`drills/98:41`；nats.go 默认 PingInterval 2 min × MaxPingsOut 2，tether 未设置——`internal/agent/agent.go:2228` 只设 `MaxReconnects(-1)`）；33 `upgradeRegisterDeadline=120s`（`internal/agent/upgrade_state.go:67`）+ `poll_until 210`（`drills/33:244`）；22 `sleep 61`（`drills/22:154`；`forceSingleArmTTL=60s` `internal/broker/force_single_online.go`）；41 `poll_until 300` 且注释明说不缩 3-min `defaultRosterRefreshInterval`（`drills/41:277-282`，`internal/agent/roster.go`）；74 `240`（eligibility `:290`）/`240`（B-dp `:407`）/`180`（C-auto `:572`，`autoRebalanceQuietWindow=60s` `internal/broker/proxy_auto_rebalance.go`）；73 `240`/`180`（`:222`/`:298`）；40 `175` BLOCKED（`:183`；`opCatchupTimeout=2min` `internal/broker/cluster_operation_controller.go`）。
- 现行 runner：`JOBS=0`=不限（`run-drills.sh:76`），文档建议 `-j 6`；`DRILL_TIMEOUT=2700`（`:97`）；M4 归因是**串行 solo 重跑**，`ATTR_BUDGET=3600`（`:89`）——修正树 `-j 6` 一轮含归因实测 62.6 min（dispositions "Corrected-tree acceptance"）。

### 表 A — 严格现行 Mandate（Docker+systemd、一 drill 一集群、真时间、零产品改动、不动 drill 结构）

| 量 | 值 | 来源/推导 |
|---|---|---|
| wall（主 sweep） | **20.7–22.3 min** | = drill 96 自身（B1 `-j 12` 实测 20.7；costs 1337 s） |
| wall（含 M4 串行归因） | 40–63 min | 一个偏离若是 96 就再 +22 min；实测 62.6 min |
| sum | ≈226 min | 上文 |
| 最长单元 | 96-mid-flight-chaos | 等：390 s #57 负窗口（产品 5 min tier-B 超时 + 90 s）+ ≤300 s JS meta 重组 + ≤360 s 全恢复门 + 305 s grow 主干 + 两次 brk2 重启 |
| 次长 | 67（777）/90（775）/98（700）/74（653） | 67：quorum 丢失下的 tier-B push 撞 5 min 底线；98：4 min ping 检测；74：#33/#34 开放 → B-dp 240 + C 180 每次等满 |
| 只做 sim 侧拆臂（96→A/D/F、74→B/C 各自建集群，仍各自 grow） | wall ≈ **14 min** | 96-A = 305 主干 + ≈40 s 1 GiB 在飞 + 390 + ≈150 s 恢复/reap ≈ 885 s（估计） |

### 表 B — "tether 真跑起来"宽松前提（产品**驱动器缺陷**可修、常量不动；sim 侧全可重构：快照主干、拆臂、单波并行、归因并行）

| 杠杆 | 省多少 | 性质 |
|---|---|---|
| `joinerBootGrace` 只在 resume（`opID != ""`，`:141-142` 分支）或"socket 有应答但未 clustered"时才等；fresh 且 ECONNREFUSED/ENOENT 立即 HALT | −60 s/grow：sum −41 min；N=3 关键路径 −120 s | cmd/tether 驱动器改动，无 wire、无 daemon 语义、drill 契约（rc=75 + resume 签名，`simcluster:293-297`）不变。**严格 A 下的替代**：sim 在看到 `→ render` 行后立刻 `systemctl start`（驱动器注释 `:694-700` 明说 grace 就是为"provisioning 已启动"的 joiner 设的），一次调用直达 SERVING——但 `cmd_grow` 自己钉着"必须 rc=75"，要改契约 |
| 快照主干：每轮 sweep 由 tether **真 grow 一次** N=3/N=2，干净停机（SIGRTMIN+3）后 tar 两个 volume，仅供**claim 不引用 grow 的**drill 复用（40 41 71 73 74 90 93 96 97 98、12 20 92 的 N=2） | 每个受益 drill −250…−290 s；sum 估计 −50 min | 不是"制造 grow 产物"（产物是 tether 做的），但每个 drill 看到的是"重启后的集群"而非"刚 grow 完的集群"；own-grow drill（10 11 13 30 42 50 52 67 82 91 95）不适用——30 owns #31、67 的"grow 后首推"、82 的 `roster_gen` 都以自己的 grow 为前提 |
| 拆臂成独立单元 | wall 由最长**臂**决定 | 96-A/D/F、74-base/B/C、73-steady/Q、90-alerts/topology、33-A/B、40-drain/ops/retire；41（N=3→2→1 链）、97（soak 顺序）、98、67 不可拆 |
| 单波并行（≈60 单元、≈300 容器） | Σ/j ≈2 min ≪ 最长臂 | inotify 300×8 ≈2400 < 8192；README 实测 600 容器上限；**#70 grow 风暴**：own-grow 11 个同时 grow → 需分 3 波（每波 ≈2 min） |
| 归因改为并行重跑 | 不再 +一个最长单元 | 但"solo 重跑"的定义被破坏——并行重跑不 solo，LOAD-SENSITIVE 标签失去含义；需要新的归因设计 |

| 量 | 值 |
|---|---|
| wall | **≈10–12 min**（估计） |
| sum | ≈120–140 min（估计：226 − 41 − 50 + 拆臂多出的主干） |
| 最长单元 | **96-A**：快照主干 ≈40 s（估计，未测）+ 在飞 1 GiB ≈40 + **390 s 负窗口** + brk2 重启/agt1 重注册 ≈60 + reap ≈30 ≈ 560–620 s；**67**：own N=2 主干 131 + 控制推 + quorum 丢失下 tier-B 撞 5 min 底线 ≈ 8–10 min；**74-B**：#33 开放 → 240 s 满等 ≈ 7–8 min；**98**：4 min ping + 40 s teardown + 60 s heal ≈ 6–7 min |
| 它在等什么 | `XferTimeoutTierBFloor`（5 min）、nats.go PingInterval 默认（2 min×2）、#33/#34 的满预算 RED、`opCatchupTimeout`（2 min）|

### 表 C — 激进（连产品常量也压：以带生产地板的 operator knob 或 build tag 压 `XferTimeoutTierBFloor`→30 s、`upgradeRegisterDeadline`→30 s、`forceSingleArmTTL`→10 s、`defaultRosterRefreshInterval`→30 s、`autoRebalanceQuietWindow`→15 s、`opCatchupTimeout`→30 s；产品设 `nats.PingInterval(20s)`；`SOAK_CYCLES` 6→3）

| 量 | 值 |
|---|---|
| wall | **≈6–8 min**（估计）；若 #33/#34 也修好 → **≈5–6 min** |
| sum | ≈80–100 min（估计） |
| 最长单元 | **74-B/73-#33**（开放缺陷满等 240/180，与常量无关）；其次 own-grow 单元 4–6 min（raft/JS meta 形成是第三方计时，压不动：`MultinodeElectionTimeout` 1 s 本就很小，慢在 JS meta 形成 + 1→2 期间 leader 的 lone-clustered-JS fail-stop 重启，即 brk2 第二次调用比 brk3 多的 ≈45 s）；41 的 retire 链 ≈4–5 min；97 即使 3 cycles ≈3–4 min |
| 它在等什么 | 开放产品缺陷 + 第三方状态机形成时间 + 顺序链（retire、soak） |
| 代价 | (1) drill 83 试金石：30 s 的 tier-B 超时可能在 1 GiB 在飞被 kill **之前**就先开火（loopback 上 1 GiB 进 JS 估计 5–20 s），96-A 的负窗口失去含义甚至变假红；(2) T7 纪律要求 ≈40 个 drill 预算全部按新常量重推导；(3) 外审 F2 先例（`MinXferCrossHomeReapAge=15min` 钉地板，`serveconf.go:287`）是明确反对 test-only seam 的，knob 必须是真的 operator 价值（PingInterval 是；tier-B 超时 30 s 不是——那是 2 MiB/s 链路承诺的反面，`xfer.go:19-40`） |

## ≤5 min 可达性判定

**在任何一套让步集合下，"≤5 min wall"都不是一个稳态可达的目标；它最多是表 C 全部让步 + 修掉 #33/#34 + 拆臂 + 快照主干 + grow 分波 + 归因重设计之后的 5±1 min。** 逐层说：

1. **严格 Mandate**：地板 = drill 96 = 20.7–22 min（已实测），sim 侧拆臂可到 ≈14 min。5 min 不可达，差 3–4×。
2. **"tether 真跑起来"**：地板 ≈10–12 min，由三个各自独立的 5 分钟级产品常量钉死——tier-B 5 min 底线（96-A、67）、nats.go 4 min ping 检测（98）、#33/#34 开放导致的 240/180 s 满等（73/74）。这三者任何一个不动，5 min 就不可达。
3. **激进**：地板 ≈6–8 min，此时钉死 wall 的已经不是常量而是**开放缺陷的满预算 RED**（74-B 240 s、73 180 s、40 的 BLOCKED 175 s）和**第三方状态机**（nats JS meta 形成、hashicorp/raft 晋升——grow 即便去掉 60 s grace 仍 ≈2 min）。修掉 #33/#34 后才有 5–6 min 的可能。
4. **结构性反作用**（即使数字凑到 5 min）：
   - 单波并行 = 最大并发 = C1 政策里"contention as a sensor"的最强档；#70 在 `-j 6` 就让 30/96 时红时不红，单波下 own-grow 单元会常态化红 → 操作者体验是"5 分钟拿到一个红，再花 20 分钟 solo 复核"。要么接受偏离集永不稳定，要么给 grow 分波（+4–6 min），要么先给 #70 定根因（今天没有）。
   - M4 归因的"solo 重跑"是串行定义；5 min 的 sweep 后面接一个 ≥最长单元的归因，wall 翻倍。
   - 拆臂让集群数从 43 涨到 ≈60–80（容器 300–400），fsync 尾部在 33 容器时 p99 已 13–15 ms（accel plan §1），400 容器时未知——#70 的根因如果就是 fsync 尾部，单波会更糟。
5. **不弱化断言的红线检验**：把 96-A 的 390 s 负窗口换成"brk2 日志里的 finalize 行归属 oracle"（证明终态行是 recovery 写的而非某个超时写的）能把该臂压到 ≈3 min **且 claim 的证据力不降**——但 claim 的**措辞**从"在产品的完整超时窗口内不出现终态行"变成"终态行由 recovery 写入"，这属于用户说的"抽象逻辑不变"与否的裁定边界，我不替用户判。同类可疑窗口：98 的"WRITTEN budget 330 s"本身就是从 nats.go 默认推出来的（`drills/98:28-41`），产品若设 PingInterval，它按 T7 重推导即可，不是弱化。

**诚实的地板一句话**：不改产品 ≈14 min（拆臂后）；改驱动器缺陷 + 快照 ≈10 min；压常量 ≈6–8 min；≤5 min 需要额外修两条开放缺陷并接受 bug 可见集改变。

## 重审上次否决

| §3 否决项 | 当初理由 | 新前提下 | 判定 |
|---|---|---|---|
| 1. 加速时钟层（libfaketime / CLONE_NEWTIME / 确定性模拟器） | Go 走 vDSO；时间 ns 只能偏移且分裂 monotonic/realtime；Shadow/FDB 类要替掉 OS/网络/盘 | 前两条与 Mandate 无关，**仍成立**。第三条的"删掉 systemd/install.sh/nats/fsync"在新前提下不再是禁区，但一个真 nats-server + hashicorp/raft + Go 二进制在 Shadow 下能否跑（Go 运行时裸 syscall、BoltDB fsync 变 no-op）**没人验证过**；它是一个新的 L3.5 层，不是更快的 L6。补充两个当初没评的分支：KVM TSC scaling（唯一整栈快钟，但 I/O 比例失真——83 试金石；本机无 root/tap）、产品 `Now` seam（只管比较不管 timer，且碰不到三个第三方计时源） | **仍对**；理由从"Mandate"换成"保真度/不完整性"，结论不变 |
| 2. 产品内 fast-clock 旋钮 | 外审 F2 把 `xfer_cross_home_reap_age` 钉 ≥15 min，test-only seam 已被拒 | 权威论证减弱（用户允许激进政策），但出现新的**技术**论证：seam 只覆盖 tether 自己的 timer，98/96-D/grow 等主导 wall 的等待都在 nats.go/nats-server/raft 里。当初把"带生产地板的 operator knob"与"test-only seam"混为一谈是**过宽**：PingInterval 是真产品价值（4 min 才发现 silent DROP 对一个"必须可达"的 agent 是慢的），值得作为产品增量提出并让 98 按 T7 重推导 | **部分过宽**：test-only seam 仍拒；operator knob 按产品价值个案评估 |
| 3. 第六态 ENV-RED | 五态是三处解析的契约；自报环境=洗白向量 | 与 Mandate 无关；高并发下洗白压力**更大** | **仍对，且更重要** |
| 4. 放宽 FLAKE_SIG / 自动重试 / 重跑换裁决 | 20/91 那次就会被重试成脚注 | 同上 | **仍对** |
| 5. tmpfs 放需持久性断言的 store | 95/96/97 的重启存活 claim；`docker kill` 本就丢不掉 page cache | 当初论证有一半是错位的：保护的其实是 **fsync 延迟这个传感器**而非持久性。但作为**速度**杠杆它几乎无效——一次 grow 几百次 raft commit × 6.4 ms ≈ 2 s；#70 若与 fsync 尾部有关，tmpfs 反而会掩盖它 | **仍拒作为速度杠杆**（理由修正）；只在 fsync 饱和实测出现时作为**争用**杠杆重开 |
| 6. userns-remap | 破坏 `--privileged`+systemd 与 install.sh 属主语义 | 理由是 Docker 特定的；podman/nspawn/LXD 下 userns 是机制本身，容器内属主语义经 subuid 完整保留 | **理由过窄**，但因基础替换本身无速度收益而**无需重开** |
| 7. fixture 快照 | "制造 grow 的产物"（Mandate ①/③/④） | 若快照由**同一轮 sweep 内 tether 真 grow**产出、从不 check in，就不是制造。真正丢的是：每个 drill 在**自己**的并发条件下再跑一次 grow（C1 传感器；67 的"grow 后首推"类缺陷正是这样被抓到的）、以及"重启后的集群"≠"刚 grow 完的集群"。当初的论证**不足**：它把"谁做的"与"每次都做"混在一起 | **有条件重开**：只给 claim 不引用 grow 的 drill；own-grow drill 保留；每轮 sweep 仍有 ≥11 个真 grow 作 #70 传感器 |
| 8. 迁 drill 断言进 Go 层（R8 未采纳） | 22 的 `sleep 61` 有 Go 后继却仍是唯一真栈 wiring 探针 | R8 的"状态机 ≠ 接线"论证与 Mandate 无关，**完全成立**；新前提下它反而成了主要的 wall 杠杆候选，所以更需要 R8 要求的删除门（Go 后继 + 保留一个具名真栈探针） | **仍对**；见下节 |

## 与 hermetic 层的分工

`make e2e-parallel`（99 单元、3–4 min、注入时钟）已覆盖 d7 的进程内三节点 grow/transfer-leader/retire、d5 的集群 JetStream、d6 的 rehome、p10 的升级状态机、p13 的 proxy 隧道重连。simcluster 压到几分钟之后，它**剩下且只剩下**六类 hermetic 结构上够不到的东西，每类应保留的"真时间/真栈"探针如下（R8 删除门：Go 后继存在 + 具名保留一个真栈探针）：

| 独占能力 | 保留的探针（drill/臂） | 可迁往 hermetic 的部分 |
|---|---|---|
| 跨进程 nats-server：cutover、nats.conf 漂移、SIGKILL 复活、JS store 重置 | 10、11、13、20、12、67、42 | 无（d7 是嵌入式 nats） |
| systemd/install.sh/属主/journald | 32、13、95（`Restart=always` + 两句 journal 措辞）、33-A（三次 exec 同 PID）、30（colocated 单元 + HALT/unlock） | 30 的 roll 排序逻辑（Go 已有） |
| 跨容器真数据面（reverse tunnel、SS、S0-ingress、iptables 分区/SYN block） | 70、71、72、73、74、78、96-D、97、98、33-B | 73/74 的分布算法性质（`proxy_rebalance_test.go` 已有） |
| 真 fsync/真盘/备份 | 21、50、51、43 | 无 |
| 真 PTY 跨容器 | 60 | 无 |
| **真时间接线探针**（每个产品 timer 家族一个，其 hermetic 后继走注入时钟） | 22（arm TTL 60 s）、33-B（register deadline 120 s）、83（`leaseGrantWindow`——试金石本身）、41（roster silence rebuild 3×3 min）、74-C（auto-rebalance quiet window）、98（nats.go ping + teardown ladder）、96-A（tier-B 超时/finalize-on-recovery + 8 s reap 节奏）、95（RestartSec） | 同一 timer 家族的**其他**测试全部留在 hermetic |

这层最终应保留 **8 个真时间探针**；它们的常量之和 ≈ 60+120+5+225+180+240+390+2 ≈ 1,220 s（≈20 min 的 sum），但分散在不同单元里，所以 wall 只取最大者 = 96-A 的 390 s 窗口 → **这就是"不动产品常量"前提下 simcluster 的 wall 物理下界 ≈ 6.5 min + 主干**。若用户接受 96-A 换成归属 oracle（见判定第 5 条），下界降到 98 的 4 min + 主干。

**该迁走的**（有 Go 后继、真栈只剩"再跑一遍"价值）：90 的 72 次 `tether alert` 生命周期（d8 replicated alerts；保留 90 的真 `broker_down` + `below_quorum` 拓扑臂）、93 的 /metrics /healthz 取值（保留 webhook 真 HTTP 臂）、74 的 spread/dry-run 零变更臂（`proxy_rebalance_test.go`；保留 B-dp/C 真数据面）、40 的 OPS-CONFIRM/ABORT 状态机（保留 drain/retire 真栈）。这些迁移不改 wall（它们不在关键路径），但把 sum 再砍 ≈15–20 min，并让"这个 drill 存在的理由"重新可解释。

## 我不确定的地方

1. **快照主干的真实成本没测**：一个干净停机的 N=3 集群冷启动到"3 VOTER + JS meta 形成 + agent ONLINE"我估计 30–90 s，但 96-D 的注释说 JS meta 重组"legitimately slow, and slower still on a loaded host"（`drills/96:569`），单波并行下可能接近它的 300 s 预算——那快照就省不到 250 s。必须先测一次再定。
2. **#70 无根因**：所有单波/高并发方案都押在"grow 风暴可通过分波缓解"上；若根因是 fsync 尾部或 JS meta 形成的全局争用，分波也不够。accel plan 的 Phase-4 触发条件（fsync p99 >10 ms 持续）在 33 容器时未触发，300–400 容器时未知。
3. **grow 第二次调用 brk2 多出的 ≈45 s** 我归因为 1→2 期间 leader 的 lone-clustered-JS fail-stop + `RestartSec=2` + `growConnectAuthRetryWindow=30s`（`cluster_add.go`），是从代码读出的推断，任务给定的实测只说"i/o timeout"。若它其实是 `cutoverGraceTimeout=45s`（`cluster_grow_cutover.go:26`）整段等满，那就是另一个可修的驱动器/broker 侧延迟。
4. **96-A 的 390 s 与产品现状可能已失配**：`xfer.go:19-40` 把 tier-B 预算改成了按尺寸推导（1 GiB / 2 MiB/s × 2 legs ≈ 1024 s + 余量），而 drill 注释仍写"the transfer timeout (5 min)"。如果这个窗口的本意是"覆盖 watchdog 可能开火的时刻"，它今天已经覆盖不到；如果本意是"给 recovery 一个观察窗"，那它比需要的长得多。这是一条 T7 类失配，需要 drill 作者裁定。
5. **73/74 的"post-grow proxy-eligibility 恢复 ≤240 s"**（`drills/74:290`，注释说典型 60–90 s）：我没有找到对应的产品常量，`eligibleProxyHomes()`（`proxy_rebalance.go:196`）依赖 `homeReachable`。README 记过"grow 后健康标签滞留 DEGRADED-WRITABLE，因为 `config_load_time` 在字节相同的 reload 上不前进"——若 eligibility 与之相关，这 60–240 s 是一条**产品可观测性缺陷**而非 sim 预算，修了会直接缩短两个长 drill。未证实。
6. **estimates 的方差**：表 B/C 里所有单元估计都是"预算 vs 典型"两头取中；74/73 在 #33/#34 开放期间取的是满预算，一旦修复会整体下移 3–4 min。
7. **LXD 副作用**：见"本机能力"。它对本报告的分析没有影响，但它是本次会话唯一的宿主状态改变，需要用户知情。


---

# 批评 C1 保真度

# 保真度批评 —— 五份草案逐手段核对

> 视角：**tether 是否仍"真跑起来"（真二进制、真进程、真集群操作）；每个手段让哪些"只有 deploy tier 抓到过"的 bug 类变得不可见。** 所有引用行号为工作树 `a3431a1`。我核对源码的地方标 `[核]`，只按草案文字推断的地方标 `[推]`。

---

## 0. 四条被多份草案共享、且**错误**的事实前提（先纠正，后面的裁决都依赖它们）

| # | 共享前提 | 实情（`[核]`） | 受影响草案 |
|---|---|---|---|
| **F0-1** | "#31 泄漏 marker 的可见性由 `LockLeaseTTL` 15 min 决定；模板/压常量只要在 15 min 内就保住 30/40/41 的鉴别力"（L1 (d)、L2 delta 表、L5 表 B） | `internal/broker/reconcile_grow_lock.go:99-125` `growLockDecision`：lease 未过期时**仍**走"(a) joiner 有 terminal op ⇒ clear"子句，节拍 `GrowLockReapInterval=30 s`（`broker.go:907-908`）。grow 完成即 op terminal，所以一个 P9 release 失败留下的 marker **最多存活 30 s**，与 TTL 无关。drill 30 的 `#31` 探针在 `grow_to_3` + `colocated.sh`（含 broker 重启）之后才跑，早已 >30 s。**结论：今天没有任何 drill 能看见 P9-release 单点回归；三份草案争论的 15 min 边界不是绑定量。** L1 (b)10 "压 `GrowLockReapInterval` 不隐藏 #31" 恰好**反了**——它就是唯一的绑定量。 | L1 (b)10、(d)1；L2 delta 表"cluster_grow_active/lease"行；L5 表 B |
| **F0-2** | "96-A 的负窗 = tier-B 超时 5 min + 90 s；真时间地板 ≥6 min"（L1 (d)、L4 O1/T1） | `internal/proto/xfer.go:58-61,120-127`：batch C 后 `XferBudget("b", 1 GiB, 2) = 2×512 s + 60 s = 1084 s`；`xfer_inflight.go:723` finalize-on-recovery 的年龄门 = `transferTimeoutFor(tier, size) + 60 s`。若 ledger 记录的 `Size` 是 1 GiB，门在 **1144 s ≈ 19 min**；只有 `Size==0`（L4 假设的 pull 路径）才回到 6 min。`xfer_inflight.go:319-321` 第一写者取 `e.size`（tracker entry）——pull 的 tracker entry 是否为 0 **无人核实**。drill 96:393 的注释"the transfer timeout (5 min)"是 batch C 之前的话。**结论：96-A 的 `_a57_try` 30×6 s（:418-422）在 A1f-pre 之后 ~510–690 s 处采样，若门是 19 min 则这段"post-recovery 判定"结构上永远看不到合成终态——这是 drill 正确性缺陷，任何"压 96-A"方案都不该把它当地板继承。** | L1 (d)、L4 O1/T1、L3 表（96-A 3.5/15 min）、L5 表 A/B |
| **F0-3** | "drill 30 的 unlock 臂依赖 lease 在未来（默认 clear 拒绝）；压 TTL 会破坏它"（L1 (d) 隐含） | `a3431a1`（2026-09-04）：H-2 `PlanExpireUpgradeLease` 让 HALT 的 orchestrator 自己把 lease 标成已过期；drill 30 的 `UNLOCK-safety`（拒绝）已改为 `UNLOCK-halt-admits`（默认 clear 成功），"live lease 的拒绝"迁到 `cmd/tether/cluster_unlock_test.go` 并登记 `not_covered … gap`。**drill 30 的 unlock 语义今天与 `LockLeaseTTL` 无关。** | L1 (d)、L2 delta 表 |
| **F0-4** | "第二次 `cluster add` 调用含 former-N1 cutover + JS reset + nats restart"（用户给定事实；L1 §不确定 2、L2 §不确定 1、L5 §不确定 3 都在猜） | `cluster_add_drive.go:186-193`：`→ former-N1 cutover` 那行在**每次**调用都打印（`votersBefore==1` 在 joiner 成 VOTER 前恒真）；SIGKILL 只在 invocation 1 发出；invocation 2 的 `cutoverBroker` 走 `AlreadyDone`，或——若 brk1 的 tether-broker 正因 nats 丢失自重启——走 6×`growConvergePoll`=18 s 的 transport-error 重试后 `return nil`（`:535-563`）。所以 add2 的 67 s 里最多 18 s 是这段，其余是 catch-up/JS meta 1→2/VOTER 晋升。这不改变 grace 结论，但 L2"第二次调用随之消失"要改成"其中 18 s 的 cutover 重试仍会付、只是并入同一调用"。 | L1、L2、L5 |

---

## 1. 草案 L1（产品时间常量清单）

### (c)1 `joinerBootGrace` fresh-joiner 跳过（`initRanThisInvocation`）
- **问题 A（代价"零"不成立）**：跳过 60 s 后，joiner 的 daemon 启动时刻比今天**早 60 s**，即更贴近 former-N1 的 nats SIGKILL/revive 与"lone-clustered-JS 等待"窗（`cluster_grow_cutover.go:20-26`、`joinerBootGrace` 注释 `:686-691`）。JS meta 1→2 形成正是 #70 与 #47 的时序敏感区（README:249-256；`deploy-tier-gotchas-closed.md:426`）。这不是"隐藏 bug"，但是**改变可见 bug 集的时序包络**——试金石要求 A/B，不能写"代价：零"。`[核]`
- **问题 B（触发条件是对的，其他草案的不是）**：L1 用"本次调用跑了 P2 init"作条件，这与 drill 42 的 3/4 失败（invocation 2、raft/ 已存在、init 被跳过）兼容。对照 L3 的"走了 render 分支"条件（见 §3）——那条会把 42 的 flake 修回去。
- **严重度**：MINOR（条件正确）+ MAJOR（"代价零"须改为"需 A/B，观察 #70/#47 频率"）。
- **修正**：把"跳过"作为一个受 B1 式 A/B 保护的产品增量；同时保留 `--joiner-boot-grace` 备选的否决理由——它是 `upgrade_state.go:63-66` "no tunables without a use case" 意义上的测试专用 seam，不是"合法但把知识推给调用方"。

### (c)2 agent `PingInterval` 20 s/2 + 服务端 `ping_interval` passthrough
- **问题 A（服务端那半是保真度断裂）**：`internal/natsconf/preflight.go:42-56` `bucketOf` 不含 `ping_interval`/`ping_max`，未知键 fail-closed 拒绝 takeover。要让 98 的 `/connz` 谓词提速，必须把这两个键加进 `TetherPassthrough`，然后**只有 sim 的 nats.conf 会带它们**。deploy tier 的核心 bug 类之一是 nats.conf 漂移/re-render（#20/#12：force-single 去集群化 conf；#22 reconciler 写 nats.d/；drill 20/12/13/50-#64 全押在"re-render 保真"上）。从此这些 drill 在一份**生产从不写**的 conf 形状上测 re-render——passthrough 重发路径（`BuildMergedConf`）被 sim 键触发，而生产 install.sh 永远不会。`[核]`
- **问题 B（客户端那半改变 #80 类的触发频率）**：#80（SS proxy 锚在 per-session `runCtx`，一次普通 NATS 重连即杀数据面，INDEX 2026-08-29）的触发器就是 session rebuild。20 s×3 的容忍度让 WAN 上 30 s 抖动的 agent 更频繁重连 → 更频繁触发 #80 类。若这是**生产**值的改变，sim 仍保真（且更敏感）；若只在 sim 设（Config-only），则 sim 跑的是生产没有的配置。L1 没说清是哪一种。
- **问题 C（#82 类恢复路径改变）**：CLONE_VFORK/GC-STW 冻结的 agent（INDEX 2026-09-01）在生产（2 min×2）解冻时多半发现连接仍活；服务端 ping 20 s 下 40–60 s 就被服务端踢掉，解冻后走"socket 已关 → reconnect → re-register"。两条路径都合法，但生产命中的是前者。
- **严重度**：MAJOR（A）、MINOR（B、C）。
- **修正**：客户端 `PingInterval` 可作产品增量（含生产值变更、98 按 T7 重推预算）；服务端 passthrough **不做**——98 的 impact 谓词改用客户端侧证据（`DisconnectErrHandler` 日志 + agent slog），保留 `/connz` 作 corroborator。

### (b)3 `DefaultOfflineAfter` 60→20 s
- L1 自己指出 -j12 下会制造假红（G.1 把活进程 reconcile 成 `EXITED(-1)`）。补一条反例：这不只是"假红"，是**假的 G.1 触发**——94 的 missed-exit 方向要求 `reconciled_closed` 只在真死时写；一次 20 s 心跳延迟造成的 `EXITED(-1)` 会被 94/96-F 的正向断言当作"G.1 正常工作"计入 pass。假绿与假红同源。
- **严重度**：MAJOR（若 Config-only 且在 -j≥12 用）。
- **修正**：只在 N=1 家族（94）用，且 YAML 化后写明地板 ≥4×heartbeat 的**推导**须由 `test/determinism` 的产品计时守卫机械守住（今天该守卫只看 `leaseGrantWindow/probeTTL/DefaultLeaseGrace…`）。

### (b)4 `upgradeRegisterDeadline` 120→30 s
- 反例：33-A 的"三次 exec 同 PID + 注册"绝对耗时不变，deadline 相对提前 ⇒ sim 里制造生产不会有的 register-vs-rollback 竞态（commit 与 rollback 的 TOCTOU）。它会以 ASSERT-FAIL 出现，看起来像 33 抓到了 bug。这是"制造"不是"暴露"。
- **严重度**：MINOR（有地板 ≥30 s 时可控），但 `upgrade_state.go:63-66` 明文"no tunables without a use case"——L1 把它归 (b) 与仓库自己的裁决相悖。
- **修正**：归 (d) 或要求先给出运维用例。

### (b)5 `forceSingleArmTTL` 60→30 s
- 同意 claim 不变。但 R8 保留 `sleep 61` 的理由是"唯一真时钟过期**接线**探针"；把 TTL YAML 化后，探针测的是"YAML 读到的值"而非"编译进去的默认值"——如果 YAML 键解析出错回落到 60 s 默认，`sleep 31` 会看到 `arm_expired` 缺席而 ASSERT-FAIL（好），但如果 YAML 值被读成 0（零值合法？）则 TTL 立即过期，`sleep 31` 后仍 `arm_expired` ⇒ 假绿。
- **严重度**：MINOR。**修正**：探针同时断言 `forceSingleArmTTL` 的生效值（`cluster status` 或日志回显）。

### (b)6 `RosterRefreshInterval` 3 min→30 s + `rosterStaleGrace` 6→1 min
- `roster_stale.go:20-24`：grace 的推导是"超过 agent 的（抖动 ≤3 min）刷新周期 + 5 min SLA + 1 min"。压缩时**推导关系**必须同时机械守住（≥ interval×(1+jitter)×(silent+1)），否则健康车队被判 `agent_roster_stale`——这正是 drill 83 的形状（把常量压到真栈周转以下）。L1 说"反而新增覆盖"，前提是推导守卫存在；今天没有。
- **严重度**：MINOR。

### (b)7 `autoRebalanceReturnDwellTicks` / `QuietWindow`
- #34 的根因（`deploy-tier-gotchas.md:181-186`，源码+运行时双证）是 **return 留下的 in-flight join op 关掉 fire-gate**。压 dwell/quiet 不影响这个 gate，但 74 C-auto 的 180 s 锁定窗（R6-M1 release-blocking）是**外审锁定的 release 语义**；L1 把它当"红路径"可压，需外审重裁。
- **严重度**：MINOR（需注明外审锁定）。

### (b)8 `growConvergePoll` 3 s→更小
- L1 自己抓到 `cutoverBroker` 6×grain 的耦合——正确。补：这条正是 drill 83 的同构（poll 粒度压到 `RestartSec`+nats 冷启之下 ⇒ 6 次 transport error 后 `return nil`，cutover 证据推给 catch-up 预算，外审 M2 的"稳定拒绝不得被吞"失效）。**改成时间窗前不得压。**
- **严重度**：MAJOR（若未先改 cutoverBroker）。

### (b)9 `opCatchupTimeout` 120→30/60 s
- 同意 L1 的风险描述（#7 教训方向相反）。补：40-b 的 BLOCKED 是靠"不可达 nonvoter"构造的，claim 与值无关；但同一常量是**所有** grow 的 stuck-joiner 后备，压它等于让 -j12 下慢而健康的 joiner 进 C1 auto-confirm 路径——auto-confirm 是一条**产品动作**（confirm-op），它被错误触发后集群状态与生产不同。
- **严重度**：MAJOR（若 <60 s）。

### (b)10 节拍类（含 `GrowLockReapInterval`）
- 见 **F0-1**：`GrowLockReapInterval` 是 #31 marker 可见性的唯一绑定量；L1 的裁决反了。
- **严重度**：MAJOR（事实错误）。

### (a) `RestartSec` 2 s（"部署件，可压"）
- `scripts/install.sh:1224-1226,1259-1261`：`Restart=always` + `RestartSec=2` + **默认 StartLimitBurst=5/10 s，注释明写"deliberately do NOT set StartLimitIntervalSec=0 (that would mask a wedged broker)"**。`growCutoverRevivalFailed`（`cluster_grow_cutover.go:38`）的成因之一就是 StartLimit。2026-09-04 的 boot-probe 死循环（`NRestarts 137`，INDEX）在 RestartSec=2 下**没**触 StartLimit；压到 0.5 s 后同一瞬时态会在 2.5 s 内触 StartLimit → unit 进 failed → 一个不同的、生产不会出现的终态。`[核]`
- **严重度**：MAJOR。**修正**：`RestartSec` 归 (d)；或压缩时同步按比例改 StartLimit 并**记录**"这与生产 unit 不同"。

### (a) `disk_check_interval` 5 s 替代 90-M6 的 restart
- 同意（用 YAML 键替代 restart 触发的启动采样反而测到周期路径）。但 M6④/⑤ 今天用 `systemctl restart` 也顺带测了"重启后启动采样"这条路径；替换后那条路径不再被测。MINOR，需注明。

### (d) `LockLeaseTTL`
- 见 **F0-1/F0-3**：两条依据（30 依赖 marker 存活；30 的 unlock 依赖 lease 在未来）都已失效。归 (d) 的结论可以保留，但理由要换成 R7b 的 acquire-lock 窗口（P2/P3 HALT 后无人可判）——**没有 drill 构造那个窗口**，所以它今天是纯 hermetic 覆盖。
- **严重度**：MAJOR（事实错误导致的错误论证）。

### (d) `XferTimeoutTierBFloor` / 96 的 390 s
- 见 **F0-2**。L1 写"96 的 390 s 窗 = 5 min + 90 s… 产品在窗内什么也不会做"——若 1 GiB 的 budget 是 1084 s，"产品什么也不会做"对，但**理由不同**（watchdog 根本不会在 390 s 内开火），且 post-recovery 判定环也够不到。
- **严重度**：MAJOR（地板数字错）。

### 总前提"(b) 项的小值是合法生产配置"
- L1 §不确定 7 已承认地板值无实测。补一条结构性反例：**drill 83 的教训不是"小值不合法"，而是"sim 跑生产值才抓到那 2 s"**。任何 (b) 项一旦在 sim 与生产取不同值，sim 就失去了对"生产值下才出现的竞态"的覆盖——这与 §5 的 T2 规则（raft 三常量不动）是同一条原则，L1 只对 raft 常量承认了它。

---

## 2. 草案 L2（集群夹具供给）

### 并行 grow（否决）
- 同意，且理由更强：`PlanSetGrowActive` 条件写 + cutover 只在 `votersBefore==1` + R3 committed-config gate。无补充。

### joiner [env] 步骤与前一 grow 重叠（"零 delta"）
- **反例**：重叠的 [env] 含 `cmd_start_broker`（standalone 首启建 tether.db）、`admit_creator`（**leader 上的 raft 写**，`simcluster:281`）、`session create`（raft 写）。今天这些写发生在两次 grow 之间；重叠后它们落在前一个 grow 的 membership 变更**期间**（AddNonvoter/JS meta 1→2/VOTER 晋升）。这是更强的争用形态，不是零 delta——可能暴露也可能新增 flake，且 #70 的根因未定。
- **严重度**：MINOR（可做，但须 A/B 并登记为争用形态变化）。

### L1 concurrent-start（tail `:717` 进度行即启 daemon）
- **问题 A（resume 路径覆盖从 47/sweep 降到 1/sweep）**：今天每次 grow 的 invocation 2 都是一个**新进程**走 `joinerIsVoter` B1 捷径（`:75-92`）→ `findJoinOp` resume（`:141-142`）→"raft/ present"（`:126`）→ `joinerBrokerUpLocal` 跳 render → `cutoverBroker` AlreadyDone/transport 重试 → `waitJoinServing`。这条路径的历史：drill 42 的 3/4 失败、外审 B1（single-mode 误判）、M3（terminal-op resume）、B1（VOTER-but-live-op 假绿）。concurrent-start 让**同一进程**继续 P6——产品注释说它支持，但**运维手册只写 HALT→start→re-run**。46/47 的 grow 改走一条无 runbook 的流程，而 runbook 那条只剩 1 个采样点。这与 L2 自己对 #31 采样率的论证（30→10 次）是同一类损失，且更大。`[核]`
- **问题 B（harness 把产品 stdout 当控制协议）**：sim 对一行进度文本做出**动作**（不是分类）。产品改一个字，sim 静默退回等 60 s 然后 rc=75 → concurrent-start 契约红。够响亮，但把一行注释文字升格成契约需要 `tests/` 守它——L2 提到了，但没提这与 Mandate ④"越省力越可疑"的关系：sim 现在在 tether 操作**中途**插入 provisioning 动作，比 HALT→start 更"省力"。
- **严重度**：MAJOR（A）。
- **修正**：反转比例——默认保持 HALT→resume（每次 grow 都采样 resume 路径），concurrent-start 只给模板构建（2 次/sweep）用；或走 L1' 的产品侧提示前移，那样 HALT→resume 一次都不少。

### L1'（产品侧提示前移）—— 同意，零 delta。
### L1''（driver 自跳）—— 同意不推荐；理由与 §3 对 L3 的批评相同。

### 模板克隆：`docker commit` + volume 拷贝
- **问题 A（最重要：clone 交接把产品 bug 变成 infra 失败，live-fallback 则整条吞掉）**：模板消费者的**第一件事**是一次"grow 之后的首次冷启动"——这正是 2026-09-04 抓到的 bug 类（boot 路径对 JS 只探一次、exit 70、`Restart=always` 变死循环，INDEX）与 #64 类（restore 后 nats.conf 仍 clustered → crash-loop）的**唯一触发点**。今天每个 N≥2 drill 各自在 grow 后重启 broker（cmd_grow 的 `sctl restart nats-server`；drill 内的 kill/return），bug 落在 drill 断言里、按 drill 归因。模板化后，同一 bug 让 16 个消费者同时"交接门失败"→ SETUP-RED，归因文字是"template handover"而非产品；而 L2 写的 **"或回退 live grow 并打 `FIXTURE=live-fallback`"** 会让这个产品回归**完全消失**（fallback 成功 ⇒ 全绿）。这是 Mandate ④ 的教科书违反，也是 accel plan §3"verdict substitution by re-run"被拒的同一类。`[核]`
- **问题 B（清停 + 冷启是一条新的、无 oracle 的产品路径）**：三 broker 并发 SIGRTMIN+3 → 每个 `serve` 收 SIGTERM 清退（drill 95 T1 路径）→ leader 在 followers 同时关机时 step-down → JS meta 失 quorum 的尾部。每 sweep 跑 2 次，成功即沉默、失败即 SETUP。Mandate ④："靠脚本才成功的操作是缺陷不是成就"——这里是"没人看的操作"。
- **问题 C（16 次 `transfer-leader` 在 drill 开始前）**：钉 brk1 用 `tether cluster transfer-leader brk1 --wait`——一个有 #66（leader-hop 写不可用窗）历史的产品动词，每 sweep 多跑 16 次，失败归入交接门（同 A）。
- **问题 D（volume 根属主）**：`docker run … cp -a /from/. /to/` 对**内容**保属主；`/to` 挂载点本身是 docker 新建的 root:root。`/var/lib/tether` 必须 tether-owned（install.sh 语义，drill 13/32 断言）。GNU cp 的 `src/.` 形式会把 src 目录属性套到 dst，**应当**没问题，但 L2 没有给收据；而"绝不 chown"的承诺一旦在这里破例就是 Mandate ① 的字面违反。
- **问题 E（争用形态）**：L2 自己承认并给了 `--live-grow` lane。补：#23（broker 在 nats 丢失时清退，**无确定性 drill**，"grow labels + recovers the strand it does hit"，README:393-399）与 #47（CATCHING_UP strand，`cluster.sh:12-14` 靠 `grow_to_3` retry=1 的 `GROW-ATTEMPTS` 证据采样）的**唯一传感器就是 grow 次数**。47→13 次/sweep 是 3.6× 的采样稀释，`--live-grow` 只在 release gate 跑则是每周一次。
- **严重度**：BLOCKER（A 的 live-fallback）、MAJOR（A 的分类、B、C、E）、MINOR（D）。
- **修正**：删除 live-fallback（交接失败 = 该消费者 SETUP-RED + 一条**产品**证据行：交接门必须把 journal/slog 的首个失败签名打进 evidence，与 M1 同款）；给清停一个 oracle（birth certificate 含 3×`Deactivated successfully` ∧ 0×`code=`）；`transfer-leader` 失败按 PRODUCT-RED 分类；#23/#47 进 C1 registry 并注明"依赖 live-grow 次数"。

### delta 表遗漏的行
| 状态 | 模板 vs 克隆 | 谁的 claim |
|---|---|---|
| `lastObserve` 可达性缓存（内存，`proxy_reconcile.go:588-598`："未观测视为可达"） | 克隆冷启后所有 peer 默认可达 → `eligibleProxyHomes` 立即含全部 voter | **73/74 的 post-grow eligibility 等待（≤240 s，"典型 60–90 s"）≈0**——但那 60–240 s 是 README:417-421 登记的候选**产品**观测缺陷（`config_load_time` 不前进 → DEGRADED-WRITABLE），L5 §不确定 5 也怀疑它与 eligibility 相关。模板让它**永远不可见**，同时让 74 的 `SETUP-ss-brkN`/`SKEW-reconstruct`（#34 "拿到 eligibility → 又丢"的漂移，`gotchas:176-180`）在一个从未经历 grow 后 eligibility 抖动的集群上跑。**74 是 RED-EXPOSING #34 的 drill，它的 band（`sig:c-ss-preflow`/`b-negctrl-create`）是在 live grow 上校准的；模板化后 74 变绿不等于 #34 修了。** → 73/74 **必须 live**，L2 的"可克隆 N=3（8）"名单要去掉这两个。**BLOCKER**（对 74）。|
| raft log 回放 | 克隆冷启对模板的 raft log 做一次 FSM **replay**（live grow 是 Apply） | 每个消费者都在 drill 前跑一次 replay——FSM 确定性（`.golangci.yml` 约束 3）的免费传感器，但 replay 分歧同样落在交接门（同 A 的分类问题）|
| 20/92 的 #67 时点 | #67-B "no drill oracle, OPEN"（`gotchas:301-315`）；dispositions 记录 20/92 在 -j6 下以 `#67 residual` PRODUCT-RED **如预期**出现 | 模板化后 20/92 不再经历 grow 期 JS 形成，#67 在这两个 drill 上的出现点消失。这是一个**登记在册的 OPEN 缺陷**的可见集变化 → 20/92 不该进 N=2 可克隆名单，或 C1 registry 必须记"#67 依赖 live-grow"。**MAJOR** |
| 95 的 G.2 | 95 的 claim 之一是"G.2 restart reconciliation"（README:354） | 克隆冷启已经跑过一次 G.2；drill 注入的是**第二次**重启。首次 post-grow 重启的 G.2 缺陷被交接门消费。**MAJOR**（95 应 live 或交接门把首次 G.2 的可观测量写进 birth certificate）|

### 哪些 drill 可克隆 —— 名单修正
- 从"可克隆"中移出：**73、74**（eligibility/#34，BLOCKER）、**20、92**（#67 时点，MAJOR）、**95**（G.2 首次重启，MAJOR）。
- L2 已正确保留 live 的：10/11/13/91/67/82/42-F/30/40/41/90-M6/22-#35/51-I。

### 烤进镜像 —— 同意全部否决。
### 重评"制造 grow 产物" —— 第 4、5 条正确且是全篇最好的部分；但第 1 条"没有任何一个字节是 sim 写的"不严格：`journalctl --vacuum` + 截断 `broker.log` 是 sim 写（删）的字节，且 95/97 的 bad-sig 扫描（整文件 grep）依赖这些流的完整性——L2 自己在日志窗口段承认了，第 1 条应引用它。MINOR。

---

## 3. 草案 L3（长 drill 拆分与调度）

### F′：`joinerBootGrace` 跳过条件 = "driver 本次调用走了 render 分支"
- **BLOCKER**。render 分支的条件是 `!joinerBrokerUpLocal(socketPath)`（`:171`）。drill 42 的 3/4 失败恰在 invocation 2、brk2 "mid boot/crash-restart"（`:697-704`）——此时 socket 不答 ⇒ render 分支**会**再跑（幂等）⇒ L3 的条件跳过 grace ⇒ 立即 HALT ⇒ **把 grace 为之存在的那个 flake 原样修回去**。L2 的 L1'' 段与 L1 的 `initRanThisInvocation` 都识别了这一点；L3 没有。`[核]`
- **修正**：改用 L1 的条件（本次调用跑了 P2 init）或 L5 的 resume 判据（`findJoinOp` 非空 ⇒ 保留 grace）。

### 拆臂：96 → A/D/F
- **F 臂**：L3 说":711 那道门是跨臂残留，拆后结构性消失，F 的 not_covered 不再需要"。对 F 的 claim（G.1×G.2 node-scoped）而言正确（drill 注释 `:698-704` 明说前置是"cross-arm damage, not a finding"）。**但那 360 s 门本身是一条产品观测**：":706-710 measured r14d… full health legitimately takes >240s on a loaded host"——分区→heal→brk1 重回 VOTER→agt2 在刚 heal 的 home 上重注册，实测 >240 s。拆臂后这条"分区后全恢复时长"的测量没有归属：D 臂的收尾是 `:626/:629`（heal 收敛 ≤180 + readback ≤120），不含 agt2 ONLINE。**MAJOR**：D 臂必须把"3 VOTER ∧ 两 agent ONLINE ≤360 s"收作自己的终态断言，否则一次 agent 重注册回归在拆臂后无人看见。
- **A 臂**：见 F0-2；"3.5 min（expected 世界）/15 min（#57 钉住）"两个数字都建立在 5 min 假设上。

### 拆臂：73 → REHOME / Q；74 → SRAB / C
- 前提"模板集群已稳态，eligibility 窗 ≈0"——见 §2 delta 表第一行：那个窗是产品观测，不是 harness 浪费。L3 §不确定 4 已自己写出"grow 后的瞬态在模板消费者里将永远不可见"，但结论表仍把 73/74 放在 T 制上。**MAJOR**（自相矛盾）。
- 73-Q "独立夹具下 Q 需自己先 kill 一个（`REHOME-skip :318` 已有此形状）"——`REHOME-skip` 是 #33 STRANDED 时的**降级**形状，不是等价夹具：Q 的 causal gate（R5-M6："dead-homed exit 的 /sub-vended server == 即将被杀的 broker"）在 REHOME 之后的集群上有一个已经 rehome 过一次的 exit，在新鲜集群上是首次分布。claim（数据面分离）不变，但 R7-M3 那条 control/data endpoint mismatch（dispositions run2 抓到的 #34 显形）**只在 rehome 后的 exit 上出现**。拆开后 Q 看不到它。MINOR（需注明 Q 失去 R7-M3 传感器，REHOME 保留）。

### 拆臂：33 → B / A
- "A 今天依赖 B 回滚后的 dst==OLD；给 A 独立夹具则直接成功路径"——A 的 claim 含"同 PID 三次 exec + `.prev` consumed 后的干净成功"。B→A 的串行让 A 在一个**刚回滚过**的 marker/`.prev` 槽上升级（33 README:329："C2 domain release — wholesale-replaces the shared marker"）。独立夹具下 A 在干净槽上跑。#73（非 tether 产物过冒烟门 → 无 boot shim）与 upgrade-safety 的"四件套提交证明 + 有序 fsync"都是**槽状态**敏感的。MINOR（需注明 A 失去"回滚后再升级"形状；或 A 的独立夹具先自己制造一次回滚）。

### 调度：grow lane 30 s 错峰 + bring-up 信号量 8 + j≈16
- **未声明 C1 义务**。accel plan §C1："每个 lever 都降低争用，stress regime 必须带义务调度"，#66/#67/#70/52-rc77 全是争用下发现的。L3 的默认 sweep 同时降低 grow 争用（错峰）与 boot 争用（信号量），却没写 `--contended`/`--live-grow` lane 与 registry 更新。**MAJOR**。
- "j 超过 14–16 后 wall 不再下降，所以 270 容器全并行无必要"——同意，且这与 L5"单波会让 #70 常态化"一致；但两者对 **默认** 争用档的取向相反（见 §7）。

### 聚合契约
- 第 2 条"计数器求和 = 串行结果，除 abort 语义"——正确。补一条保真度反例：**串行时后臂跑在前臂改变过的集群上**（96-F 在 heal 后、73-Q 在 rehome 后、33-A 在回滚后），并行拆臂后每臂在新鲜（或模板）集群上跑。"计数器逐项求和"在**语义**上不等于串行，只在**verdict 枚举**上等价。L3 只登记了 abort 一处差异；应把"前臂状态残留是不是 claim 的一部分"逐 drill 写进 manifest（96-F 否、73-Q 部分、33-A 部分）。MAJOR。
- M4 归因按单元：solo 重跑一臂用的仍是同一模板（模板在争用下建）。LOAD-SENSITIVE 的定义（首跑争用 vs solo）失去 fixture 这一维。MINOR，需注明。

### 数字
- 借用 L2 的 τ=60 s 与 L2 自己的 25 s 矛盾（见 §7）。

---

## 4. 草案 L4（oracle 形态）

### O1（96-A 负窗事件化）
- 结论"弱化且不省时间"正确；**地板数字错**（F0-2）。T1 "≥360 s" 应写成"6 min（Size=0）或 19 min（Size=1 GiB），取决于 `writeXferInflight` 的 `e.size`；未核"。**MAJOR**（它是 L4 全篇 wall 结论的支柱）。

### O3（reaper `Runs` 直读）
- "等价"成立的前提是 `admin runtime` 的 `Runs` 只在 pass **完整跑完**后递增。若 `Runs` 在 pass 开始时计数，"Runs ≥2 ∧ 计数 ≤ floor"可在 reaper 半途被采样——正向 claim 本身（计数）仍是效果，所以不假绿，只是 `Runs` 那半不加信息。MINOR。

### O4（#33 用 flap 计数早判 STRANDED）
- L4 已正确说"不能删字节探针"。补一个**具名反例**：#80（INDEX 2026-08-29）的形状正是 **READY=true、零 flap、数据面死**——`p.srv` 指着尸体、register 回包 keyset-only、agent 自己 re-ACK READY。flap 计数对 #80 类恒为 0 ⇒ 早判永不触发 ⇒ 省时 0。L4 估计的"180→30–60 s"对生产真实发生过的那一种形状不成立。MINOR（结论对，估算对错误形状）。

### O5（74 C-auto 负向早退）—— 同意"假红"；补：gate 关闭的那个 in-flight op 恰是 #34 根因（`gotchas:181-186`），早退会把 #34 的显形改成另一条签名，破坏 D4 登记的 band。MINOR。

### O9（97 稳态检出替代 25 s settle）
- 采样偏差已承认。补：97 的 PID-generation 守卫（`:328-333`）与 `LEAK_MIN_N=6`（`leak.sh:40`）要求各 cycle **可比相位**；`min(25 s, 稳态检出)` 让相位随负载变化，斜率的方差上升——"thresholds UNCALIBRATED"的 drill 再引入一个未校准源。MINOR。

### O11 / O12 / O13 / O14 —— 同意。O12 的"产品设 PingInterval"见 §1 (c)2 的 A/B/C。

### O15（78 加 `proxy status` 退避字段 + 35 s 三间隔）
- **"等价且更强"不成立**。78-A 的 claim（`78:76-86`）是**窗口总量 ≤ 无退避基线一半**（39→≤20）∧ **三桶递减**。#78 的机理是 broker 每 5 s 重推 home directive、agent 每次推送都拨（`gotchas:785`）。
  - 反例 1：退避在第 3 步封顶为平的 20 s（bug）——三个间隔 5/10/20 几何增长 ✓，L4 oracle 绿；今天的 oracle：每桶 ≈3 次、桶 3 不小于桶 1 ⇒ 红。
  - 反例 2：agent 在**推送到达时**额外拨一次（#78 原始 bug 的一半）——L4 只在 `next_dial_at` 处检查 +1；除非同时断言"两次调度之间计数器 Δ=0"，否则看不见。而那正需要在推送点之间持续采样，即回到墙钟。
- **严重度**：MAJOR。**修正**：保留桶总量 oracle（可缩到 2 桶×45 s，前提是基线重推数 ≥18）；新字段只作 corroborator。

### O16（`RosterRefreshInterval` YAML 化）—— 见 §1 (b)6 的推导守卫要求。

### 观测面合法性五问 —— 全部同意；补一条：`proxy status --json` 加字段是 wire additive，须过 `internal/proto/wire_inventory_test.go` 的 append-only 账本（L4 §不确定 4 已提，应升为必做项）。

### poll 网格
- "网格层最多再省 1–1.5 min"——同意。inotify 方案的四条风险都对；补：`test/architecture/simcluster_log_oracle_test.go` 的"四条流只经 `logs.sh` 一份映射"闸门会把任何 inotify 监听路径判红，除非 helper 进 `logs.sh`。MINOR。

---

## 5. 草案 L5（基础架构替代 + 怀疑者）

### 基础替代表
- 全部同意；"grow cutover 依赖 systemd `Restart=always`"（`cluster_grow_cutover.go:20-22`）是决定性的，`[核]`。Firecracker 一行"唯一能做真断电"正确但漏了一条：guest 内核**仍**要 `--privileged` 等价的 iptables/FUSE，且 `docker exec` 换 vsock/sshd 后 `dexec` 183 行 + 367 处调用点全改——L5 已在"迁移成本"里写了。无补充。

### KVM TSC scaling / 产品 `Now` seam —— 否决理由（I/O 比例失真；seam 不管 timer 与三个第三方计时源）正确。

### 表 B 快照主干
- **40/41 进快照名单——MAJOR**（L2 把它们留 live）。`cluster.sh:77-80`："the drills that use this OWN their #31/#45 exposure arms, and a silent nuke-and-retry would launder exactly the leftover-op state they exist to pin"。虽然按 F0-1 marker 本身 30 s 内就被 reap，但 40 的 `#45`/`#38` 分支（`40:262-271`：retire 卡 `NATS_ROLLED_OUT`）与 41 的 retire 链依赖的是 **in-flight op / cluster_nodes 状态**，那些不被 reap；快照（清停+冷启）后 op 表相同但 `lastObserve`、JS meta leader、term 全变——41 的"retire 时 JS reset + broker active"链在一个刚重组过 JS 的集群上跑。
- **95 在 own-grow 名单**（与 L2 相反）——L5 这边对（见 §2 delta 表 95 行），但理由没写。
- **12/20/92 N=2 进快照**——#67 时点问题（§2）。MAJOR。

### 单波并行（≈300 容器）
- L5 自己在"结构性反作用"里写了 #70 常态化——对。但表 B 的 wall ≈10–12 min 仍按单波算，与其反作用段矛盾（自我矛盾，MINOR）。

### 归因改并行
- "solo 重跑的定义被破坏"——对，且比 L5 写的更严重：M4 的 LOAD-SENSITIVE 标签是 dispositions 里**全部**六条偏离的定性依据；没有 solo 就没有 D2/D3 那类裁决。BLOCKER 级的流程缺口，L5 只标"需要新的归因设计"。

### 表 C（激进）
- **`SOAK_CYCLES` 6→3 — BLOCKER**：`97:8` "the default is the STRUCTURAL FLOOR of the slope test"；`leak.sh:40,92` `LEAK_MIN_N=6` 以下直接 `not_covered`。这一行不是"压时间"，是把 97 的 leak oracle 变成 INCOMPLETE——弱化断言，用户红线。L1 的 (a) 段已指出该地板，L5 没看。`[核]`
- **`XferTimeoutTierBFloor`→30 s**：L5 自己在"代价 (3)"里否了（2 MiB/s 承诺的反面）。但表 C 的 wall 6–8 min 仍含这一项，数字与裁决不一致。MINOR。
- **`upgradeRegisterDeadline`→30 s / `forceSingleArmTTL`→10 s**：与 L4 的"测试专用，拒"矛盾（§7）。
- **`opCatchupTimeout`→30 s**：见 §1 (b)9。

### "96-A 换成归属 oracle（终态行由 recovery 写入）"
- L5 正确地把它交给用户裁定。补一条技术事实：按 F0-2，若年龄门是 19 min，"归属 oracle"仍要等 19 min 才有终态行可归属——它压的是**负窗**，压不了**正向合成**。所以"≈3 min 且证据力不降"只在 Size=0 世界成立。MAJOR（数字前提未核）。

### 与 hermetic 层分工 —— 8 个真时间探针的名单合理；"该迁走的"四项里，**74 的 spread/dry-run 零变更臂**是 #34 的"已证 A"（`gotchas:171-173`：1/1/1 → 漂回 brk1=3 的 home 计数直读）——那是 deploy tier 独家的产品证据，`proxy_rebalance_test.go` 测的是算法不是漂移。**不可迁**。MAJOR。

### LXD 副作用 —— 已如实披露；与分析无关。

---

## 6. 逐 bug 类可见性矩阵（"在该手段下会变得不可见/变形"的具名条目）

| bug 类（历史上只有 deploy tier 抓到） | L1 压常量 | L2 模板 | L2 concurrent-start | L3 拆臂+错峰 | L4 事件化 | L5 表 C |
|---|---|---|---|---|---|---|
| **#22 /etc/tether 属主**（13/32） | — | 13/32 live；clone 的 volume 根属主需收据（§2-D） | — | — | — | — |
| **#24 route cert SAN** | — | 全 sweep 共享一套证书；仍真 mTLS | — | — | — | — |
| **#80 SS 死出口（READY 谎言）** | `PingInterval` 变短 ⇒ 触发更频（若生产同改则更敏感） | 73 若模板化：失去 rehome 前的 eligibility 抖动 | — | 73-Q 失去 R7-M3 | O4 flap 早判对 #80 形状恒 0 | — |
| **#81/#82 stale mount / CLONE_VFORK** | 服务端 ping 变短改变解冻后路径（§1 C） | 62 live | — | — | — | — |
| **drill 83 leaseGrantWindow** | **原则本身**：sim 与生产取不同值即失去生产值竞态覆盖 | — | — | — | — | 同左 |
| **nats-server pin / deny 未装载** | 服务端 passthrough 键 ⇒ sim conf ≠ 生产 conf | 模板陈旧门须含 image id（L2 已写） | — | — | — | — |
| **drill 30 unlock（H-2）** | TTL 无关（F0-3） | 30 live | — | — | — | — |
| **#66/#67/#70/52-rc77 并发缺陷** | — | 47→13 次 grow；20/92 的 #67 时点消失 | resume 路径 47→1 | 错峰+信号量降争用、未声明 C1 lane | — | 单波反向拉满 |
| **drill 95 Restart=always / StartLimit** | `RestartSec` 压缩改写 crash-loop vs StartLimit | 95 模板化：首次 G.2 被交接消费 | — | — | — | — |
| **drill 32 install.sh 生命周期** | — | live（烤镜像已否决） | — | — | — | — |
| **#23 nats 丢失清退（无确定性 drill）** | — | 采样 3.6× 稀释 | — | grow 单元 ≈9 | — | — |
| **#47 CATCHING_UP strand** | grace 跳过改时序包络 | `GROW-ATTEMPTS` 证据在 8 个消费者消失 | — | 同 L2 | — | — |
| **2026-09-04 boot-probe 死循环（JS 单探→exit 70）** | `RestartSec` 压缩 ⇒ 变 StartLimit 终态 | **交接门吞掉 + live-fallback 完全隐藏** | — | — | — | — |
| **post-grow DEGRADED-WRITABLE / eligibility（README:417）** | — | **73/74 模板化 ⇒ 永不可见；74 的 #34 band 失效** | — | 同 L2 | — | — |
| **#57/#58 in-flight（96-A）** | 390 s 前提错（F0-2） | 96 模板化：#58 baseline 仍 live 算，OK | — | A 臂数字错 | O1 地板错 | 归属 oracle 前提错 |

---

## 7. 草案之间的矛盾

1. **`joinerBootGrace` 跳过的触发条件**：L1 = "本次调用跑了 P2 init"；L3 = "本次调用走了 render 分支"；L5 = "`opID` 为空的非 resume"；L2 = 不改产品、sim 看 `:717` 行。**L3 的条件会修回 drill 42 的 3/4 flake**（render 分支在 invocation 2 也会跑）；L1/L5 等价且安全。
2. **每 sweep grow 次数**：L1 47（静态数站点）；L5 41；L2 模板化后剩 13 live；L3 剩 ≈9 单元。分母不同，节省量不可比。
3. **模板交接时长**：L2 ≈25 s；L3 借用 τ=60 s；L5 30–90 s 未测。三份都拿它算 wall。
4. **drill 98 检测窗**：L1/L3/L4 (4,6] min（`nats.go:5783-5784` pout>2，`[核]`）；L2 "≈4 min"；L5 "4 min"。330 s 预算相位性不足的风险只有 L1/L3/L4 写出。
5. **96-A 的真时间地板**：L4 6 min；L3/L5 "可能超过 5 min"；L1 沿用 5 min+90 s。源码是 1084 s budget + 60 s（Size=1 GiB）或 6 min（Size=0），**无人核实 pull 路径的 Size**。
6. **#31 的绑定常量**：L1/L2/L5 都说 `LockLeaseTTL` 15 min；源码是 `GrowLockReapInterval` 30 s 的 terminal-op 子句。L1 (b)10 与 (d)1 互相矛盾（前者说压 reaper 安全，后者说 TTL 是试金石）。
7. **哪些 drill 可模板/快照**：L2 保留 30/40/41 live、模板 95/20/92/12；L5 快照 40/41/12/20/92、own-grow 含 95。两份都把 73/74 放进模板消费者，两份都错（§2）。
8. **争用政策**：L2 要 `--live-grow` stress lane；L3 默认降争用（错峰+信号量）且未提 lane；L5 说单波会让 #70 常态化却仍按单波算 wall；L1 未涉及。只有 L2 履行了 accel plan §C1 的义务。
9. **`SOAK_CYCLES`**：L5 表 C 6→3；L1 (a) 指出 `LEAK_MIN_N=6` 结构地板。L5 那行等于把 97 判 INCOMPLETE。
10. **测试专用 seam 的边界**：L4 明确"`upgradeRegisterDeadline`/`forceSingleArmTTL`/`XferTimeoutTierBFloor` 可压 = 测试专用，拒"；L1 把前两者归 (b)"合法生产配置"；L5 表 C 三者全压。三份对同一条外审 F2 先例给出三种读法。
11. **阻塞 sleep 总量**：L1 表 F 引 accel plan "175 s（36 处）"；L4 指出 78 的 255 s 在 g75-g78 之后新增、不在 175 s 内（全仓字面和 433 s）。L1 的 D 小计因此少计 ≈4 min。
12. **drill 30 与 lease**：L1 (d) 把 30 的 unlock 与 TTL 绑定；`a3431a1`（H-2）已让 HALT 自动过期 lease、unlock 臂改写。L2 delta 表也沿用旧前提。
13. **concurrent-start 的保真度**：L2 称"零 delta（同一产品路径）"；L2 自己的另一段承认 HALT→resume 契约只剩一个 drill 钉。这两句不能同时成立——resume 是一条**不同的**产品路径（新进程、B1 捷径、findJoinOp resume），不是同一条。
14. **拆臂 = 串行**：L3 说计数器求和数学上等于串行；同一份草案的臂结构表列出 96-F/73-Q/33-A/52-D 都"依赖前臂状态"。等价只在 verdict 枚举层成立。


---

# 批评 C2 正确性与洗白

# 批评报告 — 视角：正确性与洗白（laundering）

> 所有 file:line 均为工作树 `a3431a1`；引用的合约文本以 `test/simcluster/lib/assert.sh:6-24,488-494` 与 `run-drills.sh:402-431,514-548,962-1020` 为准。每条批评格式：**手段 → 问题 → 场景/证据 → 严重度 → 修正**。

## 0. 核对过的共同基线（供后续 agent 直接引用）

| 事实 | 出处 | 影响哪些草案 |
|---|---|---|
| 五态 verdict 是四个计数器的**格上确界**：`drill_end` 按 `_AS_FAIL → _AS_SETUP → _AS_PRODUCT_RED → _AS_NC` 顺序取第一个非零 | `lib/assert.sh:488-494` | L3 的跨臂求和在数学上成立（join 单调），但 INFRA-ABORT / CONTRACT-ERROR **不在这个格里**，它们是 runner 侧分类（`run-drills.sh:402-431`） |
| `poll_until` **首次成功即返回**；96 的 `poll_until 390 30 … \|\| true`（`96:396`）在"1 GiB 在 kill 前传完"的分支（期望表记录的世界：`expected-verdicts.tsv` 96 行 `INCOMPLETE 5`，gap 文本 `96:408` 明写 "in-sim interruption not reliably constructable"）**第一采样即真、0 s** | `lib/log.sh:83`；`96:396-408` | L1 表 F / L5 表 A 把 390 s 记为"健康路径必付"是错的；L3/L4 正确 |
| R16 finalizer 只在 `now − StartedAt ≥ transferTimeoutFor(tier,size) + xferStrandedSlack` 时合成 terminal；pull 路径 size==0 ⇒ 取 floor 5 min ⇒ **6 min** | `internal/broker/xfer_inflight.go:723`；`internal/proto/xfer.go:104-106,120-123` | L4 正确；L5 "96-A 换归属 oracle 压到 ≈3 min" 不可能；L3/L5 担心的"1 GiB 预算 >5 min"对 pull 不成立 |
| nats.go stale 在第 3 个 tick（`pout > MaxPingsOut`），检测 ∈ (4, 6] min；tether 未设 `PingInterval`；服务端 `ping_interval` **不在** natsconf passthrough 表 | `nats.go@v1.52.0:5783-5784`；`internal/natsconf/preflight.go:42-56` | 98 的 IMPACT 谓词是**服务端** `/connz` 消失（`98:66-72,185`），DROP 是双向的（`fault.sh:80-90,158`），所以只压客户端 ping **压不动 IMPACT** |
| `cmd_grow` 把 rc=75 + PAUSED 签名当作 invocation 1 的**唯一**合法结果，并明写"generic I/O/auth/internal failure must not be laundered merely because raft promotion happens later" | `simcluster:305-313` | L2 的 concurrent-start 必须给出替代契约 |
| `#31` 产品行为**已正确**（"产品行为已对，台账挂 OPEN 仅是 drill 翻不了绿"） | `docs/deploy-tier-gotchas.md:95-100` | L1/L2 把 30/40/41 的 TTL 论证建立在"#31 仍在泄漏"上，应改写为回归传感器论证 |
| `autoRebalanceCooldownTicks = 60`（≈5 min）——**五份草案都没列**的常量 | `internal/broker/proxy_auto_rebalance.go:26-27` | 任何"return 边沿"（含模板恢复、observe tick 缩短）都会撞它 |
| 97 的 `LEAK_MIN_N` 守卫：`SOAK_CYCLES<6` ⇒ `not_covered … runtime-guard` ⇒ INCOMPLETE | `97:358-360` | L5 表 C 的 `SOAK_CYCLES 6→3` 得到的是一个**什么都不判**的 drill |
| drill 78 测的退避曲线是 `proxyDialPolicy`（5 s base → 5 min cap，且已有 `ProxyDialRetryBase/Cap` Config seam） | `internal/agent/proxy.go:701-710`；`agent.go:231-240` | L1 引用的 `agent.go:1939-1941`（500 ms/30 s/2 min）是 `applyOneHome` 的 expose-home 退避，**引错对象** |

---

## 1. 草案 L1（产品时间常量清单）

### L1-1 (c)#1 `joinerBootGrace` fresh-joiner 跳过 → MINOR（论证成立，但要补两条）
- **问题 1**：`initRanThisInvocation` 的不变量论证我核过（`cluster_add_drive.go:117-124` 的 P2 与 `:670-683` 的 clustered 判定），在 sim 里安全：`cmd_grow` 在 invocation 1 前**总是** `sctl stop tether-broker`（`simcluster:264`），42-F 的 returning node 也走这条路（`42:222` 经 `$SIM grow`）。生产上有一个 grace 曾覆盖、跳过后不再覆盖的形态：provisioning 先写了 seam **又**启动了 daemon（crash-loop "no raft state"），init 落盘 raft/ 后下一次 `Restart=always` 就会以 clustered 起来——跳过 grace 会多打一次 HALT 提示，operator 执行 `restart` 无害。不改 claim，但要写进注释。
- **问题 2（与 L2-1d 同源）**：今天 sim 的 60 s 空等**恰好**给 former-N1 的 nats 复活留了 slack；跳过后 joiner 会在 former-N1 仍在 revive 时以 clustered 起来 → 每次 grow 都真走 `n1ClusteredJetStreamFatal` crash-restart（grace 注释 `:686-691` 描述的那个瞬态）。这是**可见 bug 集的改变**（更多覆盖，也更多红）。
- **修正**：落地前一次 instrumented A/B；明文禁止为 "invocation 2 的 lone-clustered-JS 失败"新增 `FLAKE_SIG`（`run-drills.sh:105-114` 的 R2-F1 原则），新红走 disposition。

### L1-2 (c)#2 `PingInterval` 20 s/2 → MAJOR
- **问题**：L1 已指出服务端那半，但**没有推出后果**：若只改客户端，98 的 IMPACT（`/connz` absent，由服务端 ping 决定）仍需 4–6 min，而 agent 已在 ~1.5 min 内 re-register 到幸存者 → `HB0` 在 IMPACT 之后才重读（`98:189-197`）→ 此时心跳早已在走 → `_hb_advanced` **第一采样即真**：RECOVERY 断言退化为恒真（F5.1 当年修的正是这个形状）。同时 IMPACT 可能耗尽 330 s 共享预算 → **假红**。
- **修正**：两侧一起改（服务端需先给 natsconf 加 passthrough 键并过 N-1 re-render），或都不改；98 的 IMPACT/RECOVERY 顺序与水位重读必须在新常量下重新推导（T7），并在 drill 头注里重写预算来源。

### L1-3 (b)#3 `DefaultOfflineAfter` 60→20 s → MINOR（L1 已自陈风险，补一条）
- **问题**：影响面不是 94/96，而是**全部** drill 的非 claim 路径：G.1 会把被误判 OFFLINE 的活 agent 的 proc reconcile 成 `EXITED(-1)`，71/73/74 的 tunnel、96-F、97 cycles 全都会看见由 sim 负载制造的假红。
- **修正**：只在 `-j 16` 的 M3 遥测给出心跳 p99 之后决定值；"20 s = 4×heartbeat"是推理不是测量。

### L1-4 (b)#4 `upgradeRegisterDeadline` 120→30 s → MINOR
- **证据**：33 A1 **逐字**断言 `waiting for re-register (deadline 120s)`（`33:264`）；改常量必须同步 drill 文本，否则 A1 假红。L4 把同一项裁为"测试专用 seam，拒"，L5 压到 30 s——三份草案三个答案（见 §6）。

### L1-5 (b)#6 `defaultRosterRefreshInterval` 3 min→30 s → **MAJOR（接近 BLOCKER）**
- **问题**：L1 说"41 用机制区分 fast path 与 silence path，故安全"——区分的对象错了。41 的两条 `not_covered gap`（`41:207-215`）说的是 **PROACTIVE 唤醒不可靠**，候选根因是"`nats_topology_*` nudge best-effort、被丢弃后**重画 jitterDur(3 min)**"。把 interval 压到 30 s，proactive 失败会被 30 s 内的常规 refresh **兜住**，`poll_until 60 3` 稳定通过，两条 gap 会被"交易回"断言——drill 由 INCOMPLETE(2) 变 GREEN，而被隐藏的正是 nudge 丢失这个产品缺陷。这是 drill 83 试金石的反向实例。
- **修正**：只有当 fast-path 臂能**按机制**区分"由 topology 事件唤醒"与"由定时 refresh 唤醒"（journal 行 + 到达时刻 < 最小 refresh 间隔）时才可压；否则 41 的 interval 保持生产值。同一论证适用于 L4-O16。

### L1-6 (b)#7 `observeTickInterval` 5→2 s "等比缩短" → MINOR
- **问题**：dwell 的注释是 wall-time 动机（"long enough for the outbound crash-rehome to settle"，`proxy_auto_rebalance.go:23-25`），不是 tick 语义；2 s×6=12 s 在负载下可能短于一次 OpenHome 结算 → 在测更弱的守卫。且 `autoRebalanceCooldownTicks=60` 随之从 5 min 变 2 min，五份草案均未列。

### L1-7 (b)#8 `growConvergePoll` → MINOR（L1 已抓住 cutover 6×grain）
- 补：外审 M2 的"稳定拒绝不得被吞"依赖 refusal 之后**再有 ≥1 次 poll 观察到它仍是 refusal**（`cluster_add_drive.go:530-546` 的 `lastRefusal` 清零逻辑）；改成时间窗时要保留"最后一次采样仍是 refusal 才 HALT"的语义。

### L1-8 (b)#9 `opCatchupTimeout` → MINOR（L1 已自陈；L5 表 C 给 30 s，矛盾见 §6）
- 补：假 BLOCKED 不只影响 40，它让 `waitJoinServing` 报错 → `cmd_grow` 失败 → 26 个 N≥2 drill **SETUP-RED**，且 `grow_to_3 retry=1` 的 nuke+retry（`cluster.sh:20-27`）会把它记成 `GROW-ATTEMPTS: 2`——有证据但整轮 sweep 变慢一倍。

### L1-9 (b)#11 `jsDownThreshold` 60→10 s → **MAJOR**
- **问题**：`js_down` 是 severe 家族告警，而 `run` / `push` 有告警门（96-B0 "run --ack-alerts gate"、41 `push --ack-alerts`）。10 s 阈值在**任何** JS meta 重组（每次 broker 重启、96-D 的 300 s 窗、负载下更长）都会升告警 → 不相关 drill 的 `run`/`push` 被拒 → 假红；更糟：96-B0 今天 `not_covered "run was NOT refused under the alert state"`（`96:528`）可能因一个**虚假**告警而"证明门存在"→ 假阳性 PASS。
- **修正**：不作为速度杠杆；若为 92(b) 增覆盖，只在 92 自己的 fixture 上以 YAML 设（若产品愿意开这个 knob），并保证其它 drill 保持默认。

### L1-10 (a) `disk_check_interval` 替代 restart → MINOR
- **问题**：90-M6③ 的 claim 明写是 "documented **startup** disk sample (disk.go:102)"（`90:180-198`）。改成周期采样是**替换**不是**追加**——原 claim（启动采样路径）消失。修正：新加一臂测周期路径，保留启动臂。

### L1-11 表 F / P-D 账目 → MAJOR（因为它喂了 L1 的 wall 地板结论）
- 390 s 在期望世界不付（§0）；78 的 255 s 纯 sleep 未计入（L4 抓到）；"D 小计 ≈1150 s"与"96 wall 地板 ≈20 min 由 390+60+180 死窗决定"两个结论都建立在未付的窗口上。96 的 22 min 真正花在哪，没有任何草案有 `DRILL-POLL-WAIT` 证据（L4 unc.1 诚实）。
- **修正**：把 L1 表 1/2 的 96 行标为"待 instrumented sweep"，结论改成条件式。

### L1-12 事实错误：drill 78 的常量 → MINOR
- `agent.go:1939-1941` 是 `applyOneHome` 的 expose-home 退避；78 测的是 `proxyDialPolicy`（`proxy.go:701-710`，且已有 Config seam）。(d) 分类结论不变，但依据要换。

### L1-13 (d) `LockLeaseTTL` "教科书例" → MINOR
- `#31` 已修（§0）；30 的 `:326-334` 探针在 grow 后**立即**做，与 TTL 无关；只有 `:409` 的 re-fence 路径对 TTL 敏感。应改写为"回归传感器：若泄漏复发，短 TTL 会让它在 roll 前自愈"。

---

## 2. 草案 L2（集群夹具供给）

### L2-1 Lever 1：以 `:717` 进度行触发 concurrent-start → **MAJOR**
- **(a) 契约空洞**：今天 rc=75 契约同时拦住"invocation 1 的一般性失败被 resume 洗掉"（`simcluster:309-313`）。concurrent-start 下 invocation 1 可能返回 0、75、或任意错误——L2 没说 rc≠0 时怎么办。若 sim 在 rc≠0 后仍跑 invocation 2，就是**用 resume 洗白 invocation 1 的失败**。
- **(b) 覆盖坍缩**：HALT→resume 路径从 47 次/sweep 降到 1 次；那条路径就是 42 在 3/4 次运行里翻车的地方（`cluster_add_drive.go:697-704`）。
- **(c) 每次 grow 都走 crash-restart 瞬态**（同 L1-1 问题 2）。
- **(d) 触发文本成契约**：sim grep 不到就等满 60 s → rc=75 → concurrent 模式下判契约违背 → SETUP-RED。fail-closed，可接受，但要 hermetic test 同时钉 `cluster_add_drive.go:717` 的字面与 sim 的 grep。
- **修正**：concurrent 模式合约 = "invocation 1 **必须** rc=0 且**不存在** invocation 2；任何其它 rc 是硬失败并 dump ops/status"；halt-resume 模式由 10 或 11 的一臂**具名保留**（R8 规则）；禁止新增 FLAKE_SIG。

### L2-2 模板克隆：post-restart ≠ post-grow 的**具体**漏项 → **MAJOR**
- **(i) 同步 return 边沿**：恢复 = 三个 voter 同时"回来"。若 leader 先起、follower 晚几秒，observe 循环会 raise→clear `broker_down`，形成 return 边沿 → dwell 30 s + quiet 60 s 后 **auto-rebalance 可能发火**（74 设 `TETHER_AUTO_REBALANCE=on`）→ 再进入 **5 min cooldown**（§0 未列常量）。场景：74-C 在交接后 ~2 min 构造 skew+return，C-auto 180 s 锁定窗因 cooldown 永不发火 → 归因给 #34 的**假红**；或反过来 `_auto_tick`(=spread≤1) 被模板触发的那次 fire 满足 → **假 PASS**（`74:568-572`）。今天 74-C 自己也重启三 broker，但随后 `settle 90`（`74:474`）。**交接门必须包含 ≥ dwell+quiet+observe 的 settle，并且所有 `_par_count`/`_pkc_count` 类事件基线必须在交接之后取。** L2 的 delta 表没有这一行。
- **(ii) 停机期间落入 raft 的告警**：出生证明在停机**前**做，检查不到停机窗口 raise 的 `broker_down`（即便并发 stop 也是亚秒窗，不是零）。带着一条 severe 告警的模板会改变告警门动词的行为：96-B0 的 `not_covered "run was NOT refused"` 可能翻成"门被证明存在"（假阳性），41/96 的 `push` 被拒（假红）。**交接门必须对每个 clone 断言 `alert ls` 为空**，不只是 90。
- **(iii) kept-sites 盲区**：12/20/92 的 `setup_forcesingle_n2` 里 `assert_ok "grow brk2"` 是一个计入 `kept_sites` 的站点（`tests/kept-sites.sh`）。换成 restore 后站点数不变但 claim 从"grow 后 tier-B 可服务（G67 契约，`setup-forcesingle.sh:22-30`）"变成"restart 后可服务"。要作为**显式交易**写进 `kept-sites.baseline.tsv` 的备注，而不是让计数器替你说没变。

### L2-3 `FIXTURE=live-fallback` 自动回退 → **MAJOR（新的洗白向量）**
- **场景**：某产品回归让干净停机后的三节点冷启动不再收敛（95/97 测的就是这类性质）→ 16 个 clone 交接门全失败 → 全部静默回退 live grow → sweep 全绿。fixture 层的系统性失败被回退吞掉。
- **修正**：回退必须是 sweep 级**阻塞**行（与 INFRA-ABORT 同等地位、进 exit code），不是 rollup 里的一个标签；或干脆不回退，交接失败 = SETUP-RED。

### L2-4 冻结时 vacuum journal + 截断 broker.log → MINOR
- 出生证明缺 panic / bad-sig 扫描（`drills/lib/logs.sh` 四流）。模板 grow 期间的 panic 被 vacuum 抹掉，之后没有任何 drill 能看见。修正：出生证明先跑一次 `sim_broker_panic_journal`/`sim_agent_panic_journal` 全流扫描，非空即拒绝冻结。

### L2-5 陈旧门 → MINOR
- "记录派生自哪个 image id"是可伪造文本（外审 re-review Medium 4 删掉过一个可伪造 bypass，`simcluster:684-690`）。修正：对模板镜像跑同一个 `d run --entrypoint sha256sum` 比对 `/usr/local/bin/tether`，且卷内 `raft/`/`tether.db` 的产出二进制 sha 写入出生证明并与之比对。

### L2-6 模板年龄 ≤10 min → MINOR
- LPT + j≈16 下消费者启动时刻不可控；"每 sweep 重建"不等于"交接时年龄 <10 min"。修正：交接门读取冻结时刻并**拒绝**超龄模板（SETUP-RED），并把年龄作为 `[env]` 证据写进 drill log。

### L2-7 C1 stress lane 的义务清单不全 → MINOR
- 除 #67/#66/#70，还有：`GROW-ATTEMPTS`（#47 CATCHING_UP 搁浅）、`DEGRADED-WRITABLE` 标签、73/74 的 post-grow eligibility 恢复窗、G67 的 post-grow 首推。注册表漏一项 = 那项传感器静默退役。

### L2-8 关于 "L1'' 不推荐"
- L2 与 L1/L3/L5 的判断冲突（见 §6）。就洗白视角而言 L1'' 是**安全**的（L1-1 论证成立）；L2 的顾虑（分不清"从未启动"与"正在重启"）在 L1 的谓词下不存在——谓词看的是 `raft/` 是否由本次调用创建，不看 socket。

---

## 3. 草案 L3（拆分与调度）

### L3-1 INFRA-ABORT 聚合规则 → **MAJOR（R1 反模式）**
- 规则 4 "任一臂 INFRA-ABORT ⇒ drill INFRA-ABORT，其余臂计数器不进 verdict"：场景 96-A 因 inotify 起不来 INFRA-ABORT，96-D 记录了真 ASSERT-FAIL（#65 候选）→ drill 报 INFRA-ABORT → 若 A 的重试再 INFRA-ABORT，D 的 ASSERT-FAIL 从 verdict 里**消失**。这正是 `assert.sh:452-462` R1 写下的"real finding relabelled as our bug"。
- **修正**：drill verdict = 有 verdict 的臂的 join；缺臂/CONTRACT-ERROR 作为独立的阻塞列（`arms_missing=n`），进 exit code，但**不覆盖** verdict。

### L3-2 父/子期望行 → MAJOR（设计缺口）
- 规则 6 "父行 bands 必须为 `-`"：52 期望 GREEN + band `ASSERT-FAIL@#69@sig:retire-not-leader`。拆成 A/B/D 后 band 归 D；D 失败 MATCH-BAND(#69)，父行聚合 = ASSERT-FAIL ≠ GREEN，父 bands 为 `-` → **DEVIATION**。子行说 MATCH-BAND、父行说 DEVIATION，M4 归因队列按谁走？
- **修正**：父行 `match` 由子行**派生**：全 MATCH → MATCH；≥1 MATCH-BAND 且其余 MATCH → MATCH-BAND(ids)；否则 DEVIATION。父行不再独立匹配。附带：拆臂让"第一条失败行"从 drill 级变臂级，这是**改进**（今天前臂的 banded 失败会遮住后臂的新红，因为 `_first_fail_sig` 只看第一条）——值得写进 plan 作为拆臂的正面收益。

### L3-3 运行时按描述串去重 `not_covered`、重复计 CONTRACT-ERROR → MINOR
- 96-A 与 96-A2 各有一个 "count unreadable" runtime-guard（`96:449,495`）；带运行值的描述（`count $_C_ORPHAN <= baseline $_B58`）根本不会相等；而合法的重复 runtime-guard 会被判成 CONTRACT-ERROR（永不重试的 blocker）——又一次把 INCOMPLETE 改标成 harness 错误。修正：所有权在 manifest 里静态声明，lint 检查每个结构性 `not_covered` 站点位于其 owner 臂的 `case` 分支内；不做运行时去重。

### L3-4 manifest `# arms:` 成为调度输入 → **MAJOR（kept-sites 盲区）**
- `kept-sites.sh` 数的是文件里的静态站点。把 `case "$ARM"` 里的一个臂从 `# arms:` 删掉，站点数不变、门绿，臂永不运行。修正：lint 双向对账（文件内每个 `case` 标签 ∈ manifest，manifest 每项在文件内可达 `drill_end`），且 `kept-sites` 改为**按臂**计数并进 baseline。

### L3-5 `DRILL_TIMEOUT` 2700→1200 → MINOR
- L3 自己给 74-SRAB worst ≈20 min = 1200 s；撞线即 INFRA-ABORT、不重试、evidence 是"KILLED BY --drill-timeout"——#34 band 的第一手证据丢失，且 "a trip means wedged, never slow host"（`run-drills.sh:97-99`）不再成立。修正：单元级超时 = 该单元声明的 worst × 2。

### L3-6 33-A 独立夹具 → MINOR
- 今天 A 在 B 回滚之后跑，隐含 claim "回滚后的 agent 仍可升级"（A3 的 `.prev==OLD`、A2 的 "same PID through THREE execs"，`33:266-273`）。独立夹具丢掉这条顺序 claim。修正：保留一个 B→A 变体或显式交易。

### L3-7 "verdict enum 因取 max 不变" → MINOR（表述错误）
- 串行时 A 臂 `assert_setup` abort → drill SETUP-RED；并行时 A SETUP-RED + D ASSERT-FAIL → **ASSERT-FAIL**。enum 会**上升**。信息增加，但 L3 声称"不变"是错的；expected 表要逐条改并写 commit 理由。

### L3-8 98 只提客户端 `PingInterval` → MAJOR（同 L1-2）

### L3-9 归因 solo 重跑单元 + 模板 → MINOR
- 重跑用同一模板还是 solo 重建模板？前者把模板构建期争用造成的缺陷标成 REGRESSION（确定性）、后者标成 LOAD-SENSITIVE——语义要写死。

### L3-10 模板消费者永远看不到 post-grow 瞬态 → MINOR（L3 unc.4 已诚实）
- 要求 manifest 的 `# fixture:` 行同时列出"本臂放弃的 post-grow 传感器"，与 L2-7 的注册表对账。

---

## 4. 草案 L4（oracle 形态）

### L4-1 O1 "Runs≥1 ∧ LastErr 空 → 立即判" → **MAJOR**
- L4 自己在同一行记录了 6 min 年龄门（`xfer_inflight.go:723`），却提议 brk2 重启后 pass 一跑完就判。场景：brk2 在 kill 后 ~4 min 回来，finalizer 第一轮 `Runs=1`、`LastErr=""`、裁决 `ledgerLeave "younger than its tier timeout + slack"` → drill 立即判 → 无 terminal → **假 PRODUCT-RED #57**。修正：早判条件必须包含 `now − StartedAt ≥ timeout + slack`（从 ledger/start 行 ts 读），否则保留 30×6 s 循环。

### L4-2 O4 早判 STRANDED corroborator → **MAJOR（改变测量值）**
- 73 REHOME 是 measure-and-record（`73:298-311`，两向接受）；INDEX 2026-08-29 实测 AUTO-RECOVERED ≈26 s，说明恢复确实发生。"flap≥K 且 /sub 不渲染 → 提前记 STRANDED"会把 150 s 时恢复的运行记成 STRANDED——记录的**数值**变了，尽管 verdict enum 不变。修正：corroborator 只能**标注**（log 行），不得终止 180 s 探针。

### L4-3 O14 (#35) 36 s→≥20 s → **MAJOR（假红）**
- `22:236-249`：正分支要求窗口内 `DRY_EVER_PROCEEDED=yes`，而 dwell 是 15 s 且要先经 boot；20 s 窗内多半到不了 proceed → 落入 `NRestarts≥2 ∧ ERR_PIDS≥2` 的 `product_red "#35 reproduced"`（假 PRODUCT-RED）或 `_as_fail`（假 ASSERT-FAIL）。修正：窗口 ≥ boot + dwell + 余量；只有正半可早退，且负半的"每个采样都不 proceed"必须保持固定网格。

### L4-4 O15 (78) 35 s 替代 3×65 s → **MAJOR（"等价且更强"不成立）**
- A3 的 claim 是 195 s 窗口内总拨号 ≤ 无退避基线一半（`78:78-83,105-111`）。它能抓"每 N 秒某个产品事件把退避复位"这类缺陷（N 可达 roster refresh 3 min、epoch 重推等）；35 s 的几何检查抓不到 N>35 s 的复位。修正：几何增长 + 包计数配对是**追加**；总量窗口保留，长度 ≥ 任何可能复位退避的产品节拍。

### L4-5 O16 `RosterRefreshInterval` YAML 化 → MAJOR（同 L1-5）
- L4 说"两个理由分不开，需用户裁定"。不需要裁定：41 的 gap 文本已经把它写成时序问题（`41:190-199`），压 interval 会以时钟而非修复关闭 gap，这是机械可判的。

### L4-6 O12 只提客户端 → MAJOR（同 L1-2）

### L4-7 O3 用 `Runs` 判 → MINOR
- pass 跑了但 `Skips`（`reaperCaughtUp` 为假）也算 `Runs≥2` → 早判 → 假红。加 `Skips` 条件或仍以效果（count≤floor）为唯一 oracle，`Runs` 只作证据。

### L4-8 O9 (97) 可变 settle → MINOR（L4 已自陈采样相位偏差）
- 补：`SOAK_SETTLE`/`GOR_QUIESCE` 已是 env（`97:102-108`）；改默认值等于改所有历史 series 的可比性，需重播 3-run baseline。

### L4 值得肯定的部分
- O5 负向早退自否、O7/O8 稳定窗必须留 `poll_until_fixed`、events.sh "never gate on corroboration"、五条观测面判据——都是正确的洗白防线。

---

## 5. 草案 L5（基础替代 + 怀疑者）

### L5-1 表 C "build tag 压常量" → **BLOCKER**
- 违反不可谈判前提 (1)"真二进制"：sim 跑的是带测试 tag 的另一份产物；`check_image_or_die` 比对的 `vendor/tether` 也会是那份。L5 列为选项却未标它出局。

### L5-2 表 C `XferTimeoutTierBFloor`→30 s 的 operator knob → **BLOCKER**
- `internal/proto/xfer.go` 是 ctl/agent/broker 共用 SSOT（文件头 `:15-17`）。broker 侧单独压 floor 会让 watchdog 先于客户端预算开火 → 删对象、写错码——`xfer.go:29-33` 记录的正是这个事故。96-A 的负窗口在 30 s floor 下测的是 watchdog 不是 crash（L5 自己也承认）。L5 一边写"tier-B 30 s 不是 operator 价值"一边把它放进表 C 算地板，等于用一个自己否决的手段报数。

### L5-3 表 C `SOAK_CYCLES` 6→3 → MAJOR（事实错误）
- `97:358-360`：<6 样本 ⇒ `not_covered … runtime-guard` ⇒ INCOMPLETE。"3 cycles ≈3–4 min"是一个不判任何 leak 的 drill 的时长。

### L5-4 "96-A 换 finalize 归属 oracle → ≈3 min 且证据力不降" → **BLOCKER（as stated）**
- 见 §0：terminal 在 age <6 min 时**不可能存在**（`xfer_inflight.go:723`），"归属 oracle"在 3 min 时只能看到"无 terminal"→ 要么等满 6 min（没省）要么假 PRODUCT-RED。这不是"抽象逻辑变不变"的裁定边界，是产品年龄门。

### L5-5 表 B 归因并行重跑 → MINOR（L5 已自陈）
- 并行归因下 LOAD-SENSITIVE 无定义，一个只在负载下复现的红会被随机贴 UNSTABLE/REGRESSION——标签本身成了洗白（"不是 load 的问题"没有证据）。与 L3 的"solo 按单元"直接冲突。

### L5-6 快照消费者集合含 41 → MINOR
- 41 的 retire 链有 `NATS_ROLLED_OUT|still in flight` 分支（#45，`41:218-220`）——它依赖 grow 残留的 in-flight op。模板上这条分支结构性不可达，是一条 L5 未记录的覆盖损失；L2 把 41 判 live，L3 判可模板（τ 70）。

### L5-7 表 A 的 390 s → MINOR（同 L1-11）

### L5-8 LXD 副作用 → 过程记录
- 只读任务中改了宿主（snap 安装 lxd 5.21.7）。与 drill 无关、已披露；但下一份报告应把"探测命令也要先 `file`/`dpkg -S`"写成规则，因为这份草案的可信度要靠它的"本机能力表"。

### L5 值得肯定的部分
- "26 个 N≥2 drill 的 grow 在无 supervisor 下走不通"我核过（`cluster_grow_cutover.go:20-26` + `install.sh` `Restart=always`），成立；tmpfs "保护的是 fsync 传感器而非持久性"的重审也对。

---

## 6. 草案之间的矛盾

| 议题 | 冲突 | 我核对后的裁定 |
|---|---|---|
| `joinerBootGrace` 怎么去掉 | L1：产品侧 `initRanThisInvocation`；L2：sim 侧 tail 触发，**不推荐**产品侧；L3：产品侧"本次走了 render 分支"；L5：产品侧"resume(opID) 或 socket 有应答但未 clustered 才等" | 四个不同谓词。L1 的最安全（由 #I1 不变量证明）；L5 的对"standalone 仍在跑"仍白等 60 s；L2 的 sim 侧方案需要 L2-1 的契约补丁；L2 对 L1'' 的顾虑不成立（L2-8） |
| invocation 2 的 67 s 花在哪 | 用户事实说含 cutover+JS reset；L1/L2 指出代码里 cutover 在 invocation 1（`:186-193` 在 `:203` 之前）；L5 猜 `cutoverGraceTimeout` 45 s 或 lone-JS fail-stop | 三份都承认未测；任何"grow 压到 40 s"的估计都押在这 67 s 上 |
| 96 的 390 s 是否健康路径必付 | L1 表 F / L5 表 A：必付；L3/L4：条件性，期望世界 0 s | L3/L4 对（§0）。L1/L5 的 wall 地板结论因此失据 |
| 1 GiB 的 tier-B 预算 | L3 unc.7 / L5 unc.4：可能远大于 5 min；L4：pull size==0 → floor | L4 对（`xfer.go:104-106`） |
| 98 检测能否只改客户端 | L1：必须两侧；L3/L4/L5：只提客户端 | L1 对；且后果比 L1 写的更重（L1-2） |
| `upgradeRegisterDeadline` | L1：(b) ≥30 s 合法；L4：测试专用 seam，拒；L5 表 C：30 s | 33 A1 逐字断言 "deadline 120s"；三方未提 |
| `XferTimeoutTierBFloor` | L1/L4：(d) 不可压；L5 表 C：压到 30 s | L1/L4 对（L5-2） |
| `RosterRefreshInterval` | L1：安全；L4：需裁定；41 自己的 gap 文本：时序会掩盖 | 机械可判：会掩盖（L1-5） |
| `SOAK_CYCLES` | L5 表 C：3；L1：结构地板 `LEAK_MIN_N`=6 | L1 对 |
| 97 的地板 | L3：8–10 min，wall 设定者；L4 T10：settle 可压到 60–100 s；L5 表 C：3 cycles 3–4 min | L3 的估计里 6 cycle × OfflineAfter 才是大头，L4 只压了 settle；两者不是同一量 |
| 模板消费者集合 | L2：30/40/41 live、90 有条件；L3：41 可模板；L5：40/41/90/93 可快照 | 41 的 #45 分支依赖 grow 残留（L5-6）；90 的 M6/M8 是 live（三方一致） |
| grow 次数 | L1：47；L5：41；L2："13 条 live grow"（模板化后） | 都是静态数、口径不同（是否含 retry/展开），无 instrumented 证据 |
| poll 站点数 | accel plan 315 / L1 351 / L4 363 | grep 模式不同；L4 已自陈 |
| 74 eligibility 240 s | L1：60–90 s 且 (b)；L3：模板上 ≈0；L5：疑似产品可观测性缺陷（`config_load_time` 不前进） | 无常量对应（L5 unc.5 诚实）；模板会让它**永远不可见** |
| drill 78 的退避常量 | L1：500 ms/30 s/2 min；L4：5 s/5 min | L4 对（§0） |
| M4 归因 | L3：solo 按单元；L5：并行重跑 | L5 方案使 LOAD-SENSITIVE 失义（L5-5） |
| 最终 wall 地板 | L1 ≈18 min（拆臂后 3–4 min 单臂）；L2 ≈17.6；L3 8–10；L4 6.5–8；L5 14 / 10–12 / 6–8 | 五个数五套前提；共同的诚实内核是：**不改产品常量时，由 96-A 的 6 min 年龄门（若 #57 钉住）与 98 的 4–6 min 检测各自单独超过 5 min**；改常量的方案里，L5 表 C 有两条 BLOCKER、L1 的 #6/#11 会隐藏或制造缺陷 |

---

## 7. 总评（按洗白风险归档）

**可用、且不改 claim 语义的手段**：L1-1/L3 的 grace 跳过（带 L1-1 修正）；L2 的 [env] 步骤与前一 grow 重叠；L3 的拆臂 + 计数器求和（带 L3-1/2/4 修正）；L4 的"暴露型观测面作追加 corroborator"（O3、O15 的几何检查作为**追加**）；L2 模板克隆**仅限**交接门重建全部 post-grow 前提（leader、JS meta、`alert ls` 空、settle ≥ dwell+quiet+cooldown 相位、事件基线在交接后取）且无自动回退。

**需重大修正后才可用**：L1-2/L3-8/L4-6（98 两侧 ping）；L1-9（jsDown）；L2-1（concurrent-start 契约）；L2-2/2-3（模板 delta 与回退）；L3-1/3-2/3-4（聚合与 manifest）；L4-1/4-2/4-3/4-4。

**不可用（会隐藏或制造缺陷 / 违反前提）**：L1-5 与 L4-5（roster interval，除非按机制区分唤醒来源）；L5-1（build tag）；L5-2（tier-B floor knob）；L5-4（96-A 3 min）。

**关于"几分钟"的诚实答案（本视角能背书的部分）**：在"每个 claim 不变"的约束下，没有一份草案给出的 ≤5 min 路径不依赖至少一条上面标 BLOCKER/MAJOR 的手段；能背书的下界形状是 L4 的 T1/T2 两根柱子（6 min / 4–6 min，各加 spine），而且它们的"是否真的被付"本身还缺 instrumented 证据——第一步该是一次带 `DRILL-POLL-WAIT` 的 sweep，而不是再一轮估计。


---

# 批评 C3 可行性与成本

# 可行性与成本批评（对五份草案 · 逐份逐手段）

> 视角：在 weilandserver 上**建不建得出来**、**要多少工**（对照上次 accel 增量的真实规模）、手段之间的**互斥/依赖/顺序**、哪些"看起来简单其实是重构半个 harness"。所有 file:line 为 `a3431a1` 工作树；本机只读检查结果在 §0。

---

## 0. 五份草案共同缺失的六条前提（先于任何手段）

| # | 事实 | 证据 | 对草案的影响 |
|---|---|---|---|
| **0.1 今天的 wall 不是 20.7 min，是 45 min** | 2026-09-04 全量 `-j6` sweep：`67-transient-js-refusal` **INFRA-ABORT（rc=124，撞 `DRILL_TIMEOUT=2700`）**，且 HEAD 单跑基线同为 INFRA-ABORT——自 2026-08-19 起未分诊 | `docs/reviews/prerelease-audit-external-review.md:1018,1034`；`expected-verdicts-log.md:425`；`run-drills.sh:97`。机理推测：`_g67_push`（`67:52`）无 timeout 包装，quorum 丢失下的 push 走 CLI 默认 `cliTransferTimeoutDefault = 35m08s+2m`（`cmd/tether/transfer.go:742`）而非 G67 的 8 s 有界拒绝 | 五份草案的"今日 p_max = 96 ≈ 21 min"全部是 2026-07 的数字。**在 67 分诊前，任何加速手段都改不了 wall = 45 min。**这是排在所有杠杆之前的第 0 步，成本约半天，可能是产品回归（G67 面 A 退化）也可能是 drill 缺陷 |
| **0.2 `-j6` 已经在负载敏感区** | 同一 sweep：40 SETUP-RED / 74 ASSERT-FAIL / 95 UNSTABLE 三条 LOAD-SENSITIVE，evidence 里 `fsync_4k_ms=15.321` | 同文档 §13.2/13.4 | L3 推荐 `j≈16`、L5 表 B 的"单波 ≈300 容器"都跑在**上一次真实证据的反方向**；accel plan R6 的 Phase-4 触发条件（p99 >10 ms 持续 + ≥2 条归因 fsync 的偏离）在 `-j6` 已接近满足，而 Phase-4 需要一次 `sudo -S` 建 LVM 槽位 |
| **0.3 Docker 默认地址池 ≈ 30 个 bridge** | 每个 instance 一个 `docker network create --driver bridge`（不指定 subnet，`lib/docker.sh:23-26`）；dockerd 无 `daemon.json`，默认池 = 172.17–31.0.0/16（15 个，`bridge` 占 1）+ 192.168.0.0/16 按 /20（16 个，本机 LAN 192.168.0.0/24 会让至少 1 个因路由重叠被跳过） | 本机 `docker info`/`/etc/docker/daemon.json` 不存在 | **并发 instance 数 ≳28 即 `docker network create` 失败**（drill instance + 模板 instance + M4 归因 instance 同时计数）。L3 的 `j=24` 行接近边界、`65 全并行`和 L5 的"单波"是 BLOCKER。两条出路：`daemon.json default-address-pools`（**需 sudo + 重启 dockerd，会杀掉正在跑的所有容器**）或 sim 侧自己按 instance 分配 `--subnet 10.x.y.0/24`（零 sudo，但要写一段地址簿——README 明说"no IP bookkeeping"指的是容器 IP，per-instance 子网不违反） |
| **0.4 日志没有时间戳；三个 parser 锚定行首** | `lib/log.sh:12-15`（`log/ok/warn/err` 无时间戳）；`run-drills.sh:435-457`（`is_flake` 的排除 grep、`_first_fail_sig` 锚 `^\[err \] (FAIL|…`）；`poll_wait_total` 明文承认漏计 `assert_ok` 子壳内的 poll（`log.sh:145-150`） | — | L3/L4 都把"先跑一次 instrumented sweep"当零成本前提。**它不是**：要按臂归因必须给每行加时间戳，而行首加戳会打断三个 parser + 1325 行 hermetic 测试的 fixture（`tests/deviation-report-test.sh` 等）。可行做法是行尾戳或 `run_one` 用 `ts` 管道（但 `setsid timeout` 进程树语义要保）——约半天，且要先做 |
| **0.5 产品侧改动的真实计价单位是"一个 leaf 增量"** | CLAUDE.md §3：plan workflow → 实现 → 审查 workflow → 外审。上次 **sim-only** 的 accel 增量：1 个 commit（`7748665`，47 文件 +5262/−146）、**2 轮内审 + 3 轮外审**（9 份 review 文档，全部 2026-07-24）、5 次 sweep（含 62.6 min 的修正树 sweep），产出 −42%/−56% | `git log`、`docs/reviews/simcluster-accel-*` | 五份草案合计提出 **≥10 项产品改动**（grace、PingInterval ×2 侧、~8 个常量变 knob、proxy status 字段、HALT 提示前移…），每项单独走流程的审查开销远大于代码量；**不合并成 1–2 个产品增量就做不完** |
| **0.6 宿主已被改动：LXD snap 已装** | `snap list` 显示 `lxd 5.21.7-1018661`；用户在 `lxd` 组（=root 等价） | 本机 | L5 已如实披露；需要一次 `sudo snap remove lxd`。对本报告结论无影响，但它是一处新增的 root 等价面 |

---

## 1. 草案 L1（产品时间常量清单）

| 手段 | 问题 | 证据 | 严重度 | 修正 |
|---|---|---|---|---|
| **(c)-1 `joinerBootGrace` 在"本次调用刚跑过 `runSelfInit`"时跳过** | **正确且是全套里 ROI 最高的一条**：47 次 grow × 60 s = 47 min sum，零 sim 改动，`rc=75 + PAUSED` 契约不变（`simcluster:311`），drill 42 的 invocation-2（raft/ 已存在 → 不 init → 保留 grace）覆盖不丢。三份草案给了三种产品变体（L1 布尔 / L3 "走了 render 分支即 HALT" / L5 "`opID != ""` 即 resume"），**只有 L1 的变体是由 #I1 不变量证明安全的**：render 分支在 drill 42 的 crash-restart 瞬态（`:177` 时 socket 正好没答）也会走到，L3 变体会把 3/4 flake 修回去；`opID` 在 fresh 路径 P4 之后也非空（`:157`），L5 变体判不出 fresh/resume | `cluster_add_drive.go:121,177,206,692`；`:697-704` 注释 | MINOR（L1 本身）| 采纳 L1 变体；额外注意：`cmd/tether` 在结构预算 golden 里挂着"**neither entry may rise again**"的 DEBT CEILING（`test/architecture/testdata/structural_budget_golden.txt:104-108`）——机械门不会因 +20 行变红（`pkg-code-lines 12000`，实测约 12.2k），但必须并入既有文件（`pkg-files 57` 精确）并在 commit message 里说明 |
| **(c)-2 agent `nats.PingInterval 20s / MaxPingsOut 2`** | 单独做**压不动 drill 98**：98 的 impact 谓词读的是**被切 broker 的 `/connz`**（`98:66-72`），DROP 是**双向**的（`fault.sh:165-166`），服务端何时踢掉连接由 nats-server 自己的 `ping_interval/ping_max`（默认 2m/2）决定，与 agent 侧无关；而 `ping_interval` **不在** natsconf passthrough 表（`preflight.go:42-56`，未知键 fail-closed 拒绝 takeover）。L1 自己在正文里点到了"两侧一起"，但表 1 的"98：−240"按单侧算 | 同左；`nats.go@v1.52.0:60-61,5783-5784` | **MAJOR** | 计价成 **3 个产品触点**：agent 连接选项 + `natsconf` 新 passthrough 键（含 `BuildMergedConf` 重发路径，L1 §不确定 8 未核）+ `install.sh` 的 nats.conf 模板（存量车队要走 orderly-update retrofit）；再加 98 的 `RECOVERY_BUDGET` 按 T7 重推导。是一个中型产品增量，不是"一行 option" |
| **(b) 类 ~10 个常量"可合法变配置项"** | 计价错位：把"改成 yaml 键"当成本，真成本是每个键 = serveconf/agent config 管道 + usage/broker-ops 文档 + 外审对"no tunables without a use case"（`upgrade_state.go:63-66` 明文）的逐条论证。L1 给出的 use case 大多是"测试想要更短"——按本仓先例这是 test-only seam。**且 bash drill 引用不到 Go 常量**：常量一变，`poll_until 390/300/210/175`、`sleep 61`、`RECOVERY_BUDGET=330` 全部按 T7 静默漂移（`docs/testing-standards.md:177-190`），除非产品先暴露一个常量读取面（例如 `admin runtime --json` 的 constants 块）——没有任何草案给这一项计价 | 同左 | **MAJOR** | 只保留有真实运维用例的 2–3 个（`RosterRefreshInterval` 车队规模、`disk_check_interval` 已存在、`jsDownThreshold` 可议），其余从"(b)"降为"(d)/拒"；先做常量暴露面，再谈任何压缩 |
| **表 2 "S3 ≈ 80 s"** | 估计过于乐观：add2 的 67 s（brk2）vs 22 s（brk3）差的 ~45 s 很可能是 joiner 在 1→2 期间的 lone-clustered-JS crash-restart（`:690` 注释描述的就是这个）+ 第二次调用又付一部分 grace——`_grow_ctl_ready`（`simcluster:379-381`）证明的是 joiner 能连**leader** 的 nats，不是 joiner 自己的 admin socket 已 clustered，所以 L1 说"第 2 次立即通过"没有证据；另有 `topoRestartBaseDelay 12 s + rank×12 s` 是否在 grow 路径上未证 | `simcluster:379-381`；`topology_reconcile.go:35-36,197` | MINOR | S3 写成 120–180 s（估计）；把 add2 的分摊列为 instrumented sweep 的首要测项 |
| **"wall 地板 = p_max = 96 ≈ 20 min"** | 见 §0.1：HEAD 上是 67 撞 45 min | — | **MAJOR**（跨草案）| 报告首句先写 67 |
| **健康路径 P/D/R 表** | 漏了 96 的 F 前置门 `poll_until 360`（`96:711`）——账本明文"in-sim >360 s，每次都 gate 成 gap"，即**每次付满 360 s**，应入 D 类；L1 只在文首列表提了一句 | `96:711,745`；`expected-verdicts-log.md` 96 行"F-arm … >360s in-sim" | MINOR | D 类 +360 s；96 的死窗合计 390+360+60 = 810 s |
| **grow 次数 47** | 静态数法正确（我按站点复核 = 47），但与 L5 的 41、L2 的"13 条 live"、L3 的"~9 个真 grow 单元"互不一致（见 §7） | — | MINOR | 全报告统一用 47（含 90 的 5 次、51 的 3 次、22 的 2 次） |

---

## 2. 草案 L2（集群夹具供给）

| 手段 | 问题 | 证据 | 严重度 | 修正 |
|---|---|---|---|---|
| **并行 grow 否决** | 正确；`PlanSetGrowActive` 条件写 + 锁本身是 30/40/41 的被测对象 | `membership_ops.go:503-517`；`cluster.sh:19-20` | — | — |
| **L1 lever：后台跑 invocation 1、tail 到 `:717` 那行就启动 daemon** | (1) **被 L1 的 (c)-1 严格支配**：同样 −55 s/grow，但 (c)-1 零 sim 改动、真实运维同样受益、且每次 grow 都继续走 HALT→resume 契约；L2 方案要把 `cmd_grow` 改成双模式 + 一个"driver 在那行出现前就退出"的回退分支（产品那行文字成了跨仓契约，正是 contract-change-sweep 事故类）+ 指定一条 drill 保留 HALT 路径——**"≈60 行 sh"是下界，加回退与 tests 约 150–200 行**；(2) Mandate ④ 反向判据：sim 读产品进度行来对时 provisioning，比今天更像"精心编排的 bash"；(3) `_add1=$(dexec …)` 的捕获形式要改成后台 + 文件 tail，`sh`（非 bash）下可做但要处理 `set -e` + 后台 rc | `simcluster:299-311`；`cluster_add_drive.go:707` | **MAJOR** | 放弃，改采 (c)-1；若产品不能改，才退回此方案 |
| **模板克隆（volume 拷 + 每节点 `docker commit`）** | 机制**可行且零 sudo**（docker 组即可；containerd snapshotter 下 commit 支持；rootfs 里的 unit/`tether` 用户/journal/`/home/sim` 只能靠 commit——L2 这点判断正确，L5 的"tar 两个 volume"方案不成立，见 §5）。**但工作量估计（300–400 行 sh）低估 3–4×**：新 verb ×2、`up` 的 clone 路径（跳过 `provision-node.sh`、per-node image、`check_image_or_die` 的派生镜像陈旧门）、stash 复制、出生证明、年龄门、交接门、live-fallback、`logs.sh` 窗口参数（该文件受 `simcluster_log_oracle_test.go` 守）、`--live-grow` lane、C1 registry、README Mandate 改写、hermetic 测试；对照 accel 增量 +5262 行/47 文件。**再加 16 个消费者每个必须在真栈跑交接门**（memory：drill 断言必须真跑不是 lint）| `lib/docker.sh:110-135`；`simcluster:675-680` | **MAJOR** | 估 800–1500 行 + 16 次真栈验证 + B1 式 A/B 两次 sweep；作为独立 harness 增量，不与 L3 的拆臂分开做（见 §6） |
| **"wall 20.7 → ≈17.6 min"** | **错**：模板构建（lever 1 后 ≈195 s）在 96 这个模板消费者的关键路径上，96 完成 ≈ 195 + 1057 ≈ **20.9 min**，≥ 96 直接 live-grow + (c)-1 的 ≈20.3 min。L2 自己又要求"模板年龄 ≤10 min、每 sweep 重建"，所以不存在跨 sweep 的缓存命中。**L2 单独只减 sum，wall 为零收益** | L2 §模板形状 + §收益 | **MAJOR** | 把 wall 一行改为"不变"；模板的价值只在与拆臂联动时体现 |
| **clone 交接 ≈25 s** | 未测；3 节点同时冷启 = raft 选举 + JS meta 重选 + `clusteredJetStreamBootWait` 2 s 轮询 + 可能的 lone-clustered fail-stop 重启 + agent-join 7 s + session 3–5 s；16 个 clone 同时冷启 = 64 容器 fsync 突发。L3 给 45–60 s | `broker.go:3057-3058` | MINOR | 写成 25–60 s（估计），列为 A/B 测项 |
| **烤 install.sh / DB / N=1 模板：不做** | 同意 | — | — | — |
| **状态 delta 表** | 完整；补两条：(a) `docker commit` 后所有 clone 共享 `/etc/machine-id` + `/var/log/journal/<mid>/`（Ubuntu 24.04 systemd 默认持久 journal，`Dockerfile:13` 只删了首启前的 id）——L2 已提 vacuum；(b) 并发 instance 数触 §0.3 的地址池上限（模板 instance 也占一个 bridge） | — | MINOR | — |
| **CRIU 否决** | 正确：`criu` 未装、`Experimental: false`、需 sudo 改 daemon + 重启 dockerd；且 systemd-in-container + `cgroupns=host` + 已建立 TCP 的 dump/restore 本就不在支持面内 | 本机 `docker info` | — | — |

---

## 3. 草案 L3（长 drill 拆分与调度模型）

| 手段 | 问题 | 证据 | 严重度 | 修正 |
|---|---|---|---|---|
| **臂结构表 / 96-F 前置门"拆臂后结构性消失"** | 这是 L3 最有价值的发现：`96:711` 的 360 s 每次付满，且是**跨臂残留**而非产品收敛；给 F 独立夹具即消失。已核实 | `96:700-745` | — | 采纳；作为拆臂的第一个（也可能是唯一值得先做的）目标 |
| **runner 改动 "≈350 行 bash"** | **这就是"重构半个 harness"**：run-drills.sh 1041 行，被 **1325 行** hermetic 测试按 `NF>=15` 的 rollup 行形、`ATTRIBUTION` 行、LPT 顺序、`duration_s` 逐字钉住（`tests/deviation-report-test.sh:86-219`、`verdict-contract-test.sh`、`validate-verdicts*.sh`、两份 accel review test）；新增 lane 调度 / ready-file 依赖 / 信号量 / `aggregate_drill` / 父子行 / 按单元 M4 / `--replay` 兼容，每一项都要连带改测试与 selftest 变异。350 行是代码下界，含测试/文档 2–3×；再加 15 个 drill 的 `case "$ARM"` 分派与各臂自建前置（L3 自己诚实给了 2–3 周真栈验证）| `tests/*.sh` 行数；`run-drills.sh:372-431` | **MAJOR** | 总量按 1000+ 行 + 3–4 周 + ≥2 轮内审 + 外审计价；分两步：先只拆 96/98/74（p_max 设定者），runner 只加"单元 = 文件+ARM"最小支持，聚合与父子行后置 |
| **调度模型的 Σ ≈ 120 min（"accel-plan §6.4 Σ≈160 at -j6"）** | 160 是 plan 的**验收目标**，不是测量值；dispositions 文档只记 wall。今天唯一的 Σ 数据是 `drill-costs.tsv`（**pre-lever** 种子，40 行合计 12944 s = 215.7 min，缺 78/83/84） | `drill-costs.tsv` 头注；`simcluster-accel-plan.md §6.4` | MINOR→影响所有 wall(j) 行 | 用 instrumented sweep 的 `rollup.tsv duration_s` 重置后再算 |
| **推荐运行点 j≈16 + "j 超过 14–16 后不再下降"** | 逻辑对，但 (a) 与 §0.2 证据相反：`-j6` 已出 3 条负载敏感偏离；按 accel plan V7 门（新宽度的偏离集 ⊆ 旧集 + 逐条 disposition）每提一次 j 都是一轮"人肉一晚"的归因；(b) `j=24` 行接近 §0.3 的地址池上限，`65 全并行`行不可建 | 同左 | **MAJOR** | 把 j 的选择写成"V7 门控的实验"，并在 plan 里预置地址池方案 |
| **W_grow：30 s 错峰、不限并发** | README 的"30 s stagger GREEN"是 5 个 grow 时代的观察；`run-drills.sh` 只有全局 `--stagger`（`:77`），per-lane 错峰是新代码；grow 单元 9–10 个 + 2 个模板构建同时 `cluster add` → #70 无根因 | `README.md` CAVEAT；`run-drills.sh:77` | MINOR | 先测 g 与间隔的曲线（instrumented），再定 |
| **模板缓存"按 image sha 命中则跳过"** | 与 L2 的"年龄 ≤10 min、每 sweep 重建"矛盾；且 deploy-tier 的典型用法是"改代码 → rebuild → 跑"，sha **必然**不命中。L3 括号里的 11–13 min 才应是 headline | — | MINOR | headline 改为 12–13 min |
| **产品项：走了 render 分支即立即 HALT** | 会把 drill 42 的 3/4 flake 修回去（§1 已述）| `cluster_add_drive.go:177,697-704` | **MAJOR** | 改用 L1 的"init 在本次调用运行过"布尔 |
| **`DRILL_TIMEOUT` 2700 → 1200** | 合理，但先修 67 | — | MINOR | — |
| **聚合契约（计数器求和 + 同优先级函数）** | 数学正确；`effective_verdict` 的"进程 rc == 行 rc"对父行不存在进程，需要新的行类型 + 三处 parser + `validate-verdicts` 父子校验 + selftest ≥6 条变异——已在上一行计入 | `run-drills.sh:396-427`；`assert.sh:488-494` | MINOR | — |
| **hermetic 门跟随** | `kept-sites` 按文件计数、`r9d-nonvacuity` 按函数名抽取，同文件 `case "$ARM"` 都不受影响——核实无误；但漏了 `simcluster_gate_set_test.go` 对 `run-all.sh` 循环的双向对账（新脚本要进循环）——L3 已提 | — | — | — |

---

## 4. 草案 L4（oracle 形态）

| 手段 | 问题 | 证据 | 严重度 | 修正 |
|---|---|---|---|---|
| **O1 判定"不省时间，地板 6 min"** | 正确：pull 路径 size==0 → floor 5 min（`xfer.go:104-106`），R16 年龄门 `timeout+slack`（`xfer_inflight.go:723`）→ 360 s。L5 §不确定 4 担心的"1 GiB 按尺寸推导 1024 s"只对 push 成立，96-A 用的是 pull | 同左 | — | 在矛盾节澄清 |
| **O12 "唯一能压的是产品设 PingInterval"** | 同 §1 (c)-2：impact 谓词由**服务端** ping 决定，DROP 双向；单设客户端不改 98 的 detection 段。需要 natsconf passthrough + install.sh | `98:66-72`；`fault.sh:165-166`；`preflight.go:42-56` | **MAJOR** | 改为"两侧 + natsconf + install.sh 的三触点产品增量" |
| **O15 `proxy status` 加 `dial_fails/next_dial_at`** | 合法（additive/omitempty，wire 清单 append-only 允许），但成本是：产品字段 + `wire_inventory` golden + drill 78 的配对 oracle 重写 + r9d 式非空性证明 + 真栈跑；收益只在 sum（78 ≈5 min，不在 p_max 上）| `internal/proto/wire_inventory_test.go` | MINOR | 低优先级；若做，并入 §6 的产品增量 B |
| **O16 `RosterRefreshInterval` YAML 化** | 同意有真实用例；成本 = agent config 管道 + 文档；41 的 300 s 与 82 的 6-min grace 同步派生 | `roster.go:24`；`roster_stale.go:24` | MINOR | 并入产品增量 B |
| **O20 把 `96:711` 归为"首次成功即返回"** | 错：账本记录它在 sim 里从不成功（>360 s），每次付满 | `expected-verdicts-log.md` 96 行；`96:745` | MINOR | 移到 O1 同类"必付窗"；这也回答了 L4 §不确定 1 的一部分（22 min 里至少 360 s 在这里） |
| **poll 网格 / 事件驱动（inotify、`nats sub`）** | 结论对（≤1–1.5 min wall，不是杠杆）；inotify 风险判断正确——容器内每个 `inotifywait` 都是 uid 0 下的一个 instance；`logs.sh` 是 `simcluster_log_oracle_test.go` 守的单一映射，加事件源等于给那道门加账本 | `test/architecture/simcluster_log_oracle_test.go` | — | 不做 |
| **"地板 6.5–8 min"** | 漏了 97（6 cycle × `OfflineAfter` 结构不可拆）与 74-C（#34 今天就显形 → 180 s 必付）；L4 的 T6 行自己写了"它们今天就显形"，但没进地板 | `97:272-322`；`74:572` | MINOR | 地板与 L3/L5 对齐为 ≈8–10 min |
| **观测面合法性五问** | 好框架；缺"计价"：每个暴露型面也是一个产品增量的审查周期（§0.5）| — | MINOR | 合并到 1 个增量 |

---

## 5. 草案 L5（基础替代 + 怀疑者）

| 手段 | 问题 | 证据 | 严重度 | 修正 |
|---|---|---|---|---|
| **基础替代表（nspawn/podman/LXD/VM/unshare）** | 本机核实一致：`sudo -n` 失败、`criu`/`podman`/`nspawn`/firecracker 未装、`newuidmap` 缺失、`/dev/kvm` + kvm 组可用。结论"保留 Docker+systemd"正确 | 本机 | — | — |
| **"去掉 systemd 则 26 个 N≥2 drill 的 grow 走不通"** | 过强：cutover 需要的是**任何**会 `Restart` 的 supervisor（supervisord `autorestart` 也行），不是 systemd 本身；真正锁死 systemd 的是 install.sh 的 `systemctl`、journald 四流、MainPID/`Deactivated successfully` 判别（95/33/30/98）。结论不变，理由要改 | `cluster_grow_cutover.go:20-26,205-222` | MINOR | — |
| **VM："tap 网络需 root"** | 半对：QEMU `-netdev socket,mcast=` / slirp 无需 root，可做 VM 间私网；但 L5 的最终判定（更保真不更快）不受影响 | — | MINOR | — |
| **快照主干："干净停机后 tar 两个 volume"** | **机制不成立**：unit、`tether` 用户、`/var/log/tether`、journal、`/home/sim` 都在 rootfs 不在 volume（`run_node` 只挂 `/etc/tether` + `/var/lib/tether`，`lib/docker.sh:110-124`）；只换 volume 再 `up` 会让 `provision-node.sh` 重跑 install.sh 覆盖 broker.yaml。L2 的 commit+volume 才是可行形态 | 同左 | **MAJOR** | 改引 L2 的机制 |
| **表 B 消费者含 40/41/90** | 与 L2（30/40/41 因 #31 残留必须 live、90 有条件）矛盾；L5 自己也说 "30 owns #31" 却把 40/41 放进去 | `cluster.sh:19-20` | MINOR | 与 L2 对齐 |
| **表 A/B/C 的 wall** | 全部建立在 20.7 min 的 7 月数字上（§0.1） | — | **MAJOR** | 先修 67 |
| **表 C 激进：以 knob 压 `upgradeRegisterDeadline`→30 s 等** | 与其自己的判据（"knob 必须是真的 operator 价值"）冲突；且未算 T7 的 bash 漂移成本 | — | MINOR | 表 C 只保留 PingInterval（两侧）+ Roster；其余删 |
| **"5±1 min"可达性** | 未计 §0.3 地址池、未计 M4 串行归因（它自己在第 4 条提了归因翻倍）——即使主 sweep 5 min，"sweep + 归因"仍 ≥ 5 + 最长单元 | `run-drills.sh:89`（`ATTR_BUDGET=3600`）| MINOR | 明确目标口径是"主 sweep"还是"含归因" |
| **迁走 90 的 72 次 alert / 93 metrics / 74 dry-run / 40 ops 状态机** | 每一项都会降低 `kept-sites` 的 per-drill 地板（`tests/kept-sites.baseline.tsv`：90=49、93=46、74=58、40=35）→ 门红 → 需要带理由的"coverage trade"行 + R8 删除门（Go 后继 + 具名保留探针）；是 4 个独立增量，不改 wall | `tests/kept-sites.sh` 头注 | MINOR | 列为可选、放最后 |
| **LXD 副作用** | 已核实（§0.6）；如实披露值得肯定 | `snap list` | MINOR | 报告里给用户一条 `sudo snap remove lxd` |

---

## 6. 互斥 / 依赖 / 顺序，以及"看起来简单其实是重构半个 harness"

**互斥（二选一）**
- L1 (c)-1（产品 20 行）**vs** L2 concurrent-start（sim 150–200 行 + 契约变更）：同样 −55~60 s/grow，前者支配后者。
- L1 变体 **vs** L3 变体 **vs** L5 变体的 grace 判据：只有 L1 的不变量成立。

**依赖链（缺一环则收益归零）**
1. 修 67 → 否则 wall = 45 min（§0.1）。
2. 时间戳 + 一次 `-j6` instrumented sweep → 否则 Σ 与每臂时长全是 7 月的估计。
3. 模板（L2）与拆臂（L3）**互为前提**：只模板不拆臂 → wall 不变（§2）；只拆臂不模板 → 每臂各自 grow，47 次 grow 变 ~70 次，#70 lane 更挤、Σ 不降反升。两者都要 runner 有 lane/依赖调度（L3）——**三者是同一个 harness 增量**，不能分三次做。
4. 任何常量压缩（L1 (b)/L5 表 C）依赖"产品暴露常量给 bash drill 读"（T7），否则是静默漂移。
5. 98 的 detection 依赖 agent 选项 + natsconf passthrough + install.sh 三者同增量。
6. `j` 的任何上调依赖 §0.3 的地址池方案 + V7 门的逐条 disposition；j 上调又会把 R6 的 Phase-4（一次 `sudo -S` LVM）从"未触发"推到"触发"。

**"看起来简单其实是重构半个 harness"的项**
- L3 的 arm split + lane 调度 + 聚合（runner 1041 行 + 1325 行 hermetic 测试）。
- L2 的模板克隆（新 verb、`up` 分叉、镜像派生、四道门、`logs.sh`、`--live-grow` lane、README/Mandate 改写）。
- L2 的 concurrent-start（表面 60 行；实为 `cmd_grow` 双模式 + 回退 + 跨仓文本契约 + 指定 HALT drill）。
- L1 的 "(b) 变配置项"（表面改常量；实为 ~10 个 yaml 键 × 管道/文档/审查 + 常量暴露面 + 全套 drill 预算重推导）。
- L4 的 O15（表面加两个字段；实为 wire golden + 78 的 oracle 重写 + 非空性证明）。
- "先跑一次 instrumented sweep"（表面免费；实为绕开三个 parser 的时间戳通道 + 子壳 poll 捕获）。

**建议顺序（按边际收益/成本）**
| 步 | 内容 | 成本（估计） | 收益 |
|---|---|---|---|
| 0 | 分诊 67 的 45 min wedge；`log/ok/err` 行尾时间戳（parser 安全）；一次 `-j6` instrumented sweep | 1–2 天 | wall 45 → ~21 min（回到 7 月水平）+ 真实 Σ/臂时长 |
| 1 | **产品增量 A**：`joinerBootGrace` init-ran 跳过（+ 可选：HALT 提示前移） | 20 行 Go + 单测 + 一轮审查 | sum −47 min；每条 N=3 路径 −2 min；真实运维少等 60 s |
| 2 | 96 的 F 臂独立夹具（先只拆这一条）| 1–2 天 + 真栈跑 | 96 −360 s（p_max −6 min） |
| 3 | **harness 增量**：模板 + 拆臂（96/98/74/90 优先）+ lane 调度 + sim 侧 `--subnet` 分配 | 1000+ 行、3–4 周、≥2 内审 + 外审、2 次 A/B sweep | wall → 12–13 min（估计，缓存不命中口径） |
| 4 | **产品增量 B**：98 两侧 ping（agent 选项 + natsconf + install.sh）+ `RosterRefreshInterval` yaml + 常量暴露面（T7）| 中型，一轮流程 | 98 −4 min；41 −2~3 min |
| 5 | 修 #33/#34/#70（产品缺陷，非加速）| 各自增量 | 74/73 的 180/240 s 红路径消失，地板 → 8–10 min |

**诚实地板（本视角）**：不动产品常量、不修 #33/#34 → **≈12–13 min**（步 0–3 后）；加步 4–5 → **≈8–10 min**（97 的 6-cycle 与 96-A 的 6 min 年龄门钉死）。"≤5 min wall"在"claim 不变"前提下不可达；五份草案里只有 L1 表 2 的"3–4 min"声称可达，它依赖 j≥20（§0.3 需先解决）、S3≈80 s（§1 判乐观）、以及把 96 按臂拆到 <4 min（96-A 单臂 ≥6 min 年龄门 + 夹具）。

---

## 7. 草案之间的矛盾

| 主题 | L1 | L2 | L3 | L4 | L5 | 裁定 |
|---|---|---|---|---|---|---|
| 全套 grow 次数 | 47 | "13 条 live grow line" | 模板后 ~9 个真 grow 单元 | — | 41 | 47（静态站点，我复核） |
| 今日 Σ | ≈218 min | ≈216 | 120–160（引 plan 目标） | ~216 | ≈226 | 无人有 post-lever 测量；tsv 是 pre-lever 种子 215.7 min |
| 今日 wall | 96 ≈20–21 min | 20.7 | 20.7 | — | 20.7–22.3 | HEAD 实际 45 min（67 INFRA-ABORT） |
| 98 的杠杆 | 两侧（agent + 服务端 passthrough） | — | 产品设 PingInterval 即可 | 同 L3 | 同 L3 | L1 对；单侧无效 |
| 模板恢复 τ | — | ≈25 s | 45–60 s | — | 30–90 s | 未测 |
| 模板缓存 | — | 每 sweep 重建（年龄 ≤10 min） | 按 sha 跨 sweep 命中 | — | 每轮 sweep 真 grow 一次 | L2/L5 一致；L3 的 9 min headline 依赖不存在的命中 |
| 模板消费者含 40/41/90？ | — | 否（#31）/90 有条件 | 是（40-b/41 用 T） | — | 是 | L2 对；L3/L5 需改 |
| 96-A 的 390 s 是否常付 | D 类必付 | — | 常态 0 s | 常态 0 s | 必付 | 账本把"audit unreadable（home 死）"记为常驻 gap（`expected-verdicts-log.md` r14d），该分支下 390 s 付满；无日志前按"多数付满"计 |
| 96-A 年龄门 | — | — | 担心 1 GiB 按尺寸 1024 s | pull → floor 5 min | 同 L3 | L4 对（`xfer.go:104-106`） |
| `96:711` 360 s | 列出未归类 | — | 拆臂后消失 | 首次成功即返回（错） | 列出 | L3 对 |
| grace 跳过判据 | init 本次运行过 | 不推荐 socket 判据；建议 sim 侧 | 走了 render 分支 | — | `opID != ""` | 只有 L1 变体安全 |
| 阻塞 sleep 总量 | 175 s | — | — | 433 s 字面（含 78 的 255 s，plan 之后新增） | 175 s | L4 对：78 晚于 plan |
| `poll_until` 站点数 | — | — | — | 363（我复核 = 363） | — | 363 |
| 单臂/wall 地板 | 3–4 min（拆臂 + j≥20） | 4.5–5 | 8–10（命中）/11–13 | 6.5–8 | 6–8（激进）/10–12 | 分歧根源：97 不可拆、74-C 今天必付、模板在关键路径、Σ 输入陈旧；本视角取 12–13 / 8–10 |
| 拆臂后集群数与并发 | — | — | 65 单元 / 270–300 容器 | — | 60–80 / 300–400 | 都撞 §0.3 地址池（≈28 instance）|

---

## 8. 我不确定的地方

1. **67 wedge 的机理**是我从 `_g67_push` 无 timeout + CLI 默认 37 min 推的，未读日志；也可能是 `_g67_meta_formed` 之后的 after-loop 卡在别处。分诊前不要按我的推测改产品。
2. **Docker 默认地址池数**按 moby `ipamutils` 的文档默认（15 × /16 + 16 × /20）写；本机没跑过 >12 个 instance，未实测上限；`docker network create --subnet` 的 sim 侧方案未验证。
3. **`cmd/tether` 的 12000 行预算**是我用 grep 近似（≈12.2k），门用 `go/scanner`；+20 行不会越 2000 量子，但精确值未跑门。
4. **96-A 390 s 的实付率**：账本记录了 unreadable 分支存在，但没有分支频率；L3/L4 的"常态 0 s"与用户事实清单相反，需 instrumented sweep 裁定。
5. **`topoRestartBaseDelay` 是否在 grow 关键路径上**（影响 S3 估计 ±36 s）未证。
6. **模板冷启 τ** 与 **16 个 clone 同时冷启的 JS meta 争用**未测；09-04 的"SIGKILL → ready 2.8 s"只是 N=2 单 broker 自愈。
7. **LXD 安装是否改变了 Docker 网络**（`lxdbr0` 只在 `lxd init` 后创建，L5 说未 init）——我只查了 `docker network ls`（干净）。
8. accel 增量的"2 天 / 9 份 review"是按 git 日期推的；实际人时未知，仅作规模对照。


---

# 批评 C4 证据严谨性

# 证据严谨性批评：五份草案逐手段核对

核对基线：工作树 `a3431a1`，只读命令（`sed`/`grep`/`git log`/`docker info`/`snap list`/`sysctl`）。共抽查约 90 处 file:line 引用，其中约 85 处逐字成立、十余处行号偏差 ≤10 行（不影响论断，下文只列有实质影响的）。整体引用质量高；**问题集中在少数几个被多份草案共同复用、且决定 wall 地板的数字上**。

## 0. 跨草案的顶级证据问题（先于逐份批评）

| # | 问题 | 证据 | 严重度 | 修正 |
|---|---|---|---|---|
| X1 | **drill 96 的 390 s "死窗" 在已记录的世界里并不支付。** L1/L2/L5 把它当"健康路径必等"（L1 记入 D 类、L2 当"最长原子窗口 ≈7 min"、L5 表 A/B 的 96-A 都含 390）。 | `drills/96-mid-flight-chaos.sh:396` 是 `poll_until 390 30 … _a1_terminal_row \|\| true`——首次采样为真即返回（fast-start 首采样 ~0 s）。R-EXHAUST 四分支（:398-423）里只有分支①（history 不可读）和分支③（只有 start 行）会耗满 390；`expected-verdicts.tsv:55` 记录的 96 描述文字 "in-sim 1 GiB interruption not constructable" 逐字对应分支②（:408，"transfer completed before the crash"）——该分支 `_a1_terminal_row` 首采样即真。同理 `_a57_try` 30×6 s 环（:418-422）只在分支③运行。 | **MAJOR**（对 L1 的 D/R 小计、L2 的地板句、L5 的表 A/B 各是 MAJOR；跨草案看是本轮最大的数字失真） | 所有 96 的分解改为"390 s 与 180 s 判定环为条件性（仅分支①/③）"；今日 p50 1337 s 的去向未知（L4 §不确定 1 是对的：**归因先于优化**）。任何把 p_max 写成"96-A ≈ 15 min / 7 min 原子窗"的句子删除。 |
| X2 | **grow 基线两个纪元混用，且今天的 grow 比种子期慢得无法解释。** 五份草案都以用户实测（grow brk2 176 s / brk3 114 s）为主干基线，同时用 `drill-costs.tsv`（2026-07-23 -j6）做"今日 p50"再相减得"节省"。但种子期数据与 176/114 不相容。 | `drill-costs.tsv`: `11-grow-gaps 152`（up+init+**一次 grow brk2**+臂）、`10-grow-to-3 305`（两次 grow+臂）、`13 266`；若单次 grow brk2 = 176 s，11 不可能 152 s。种子镜像 `c6b9c9e` 已含 `joinerBootGrace` 与 `adminStatusIsClustered`（`git log -S` 两者均首现于 c6b9c9e），所以不是"当时没有 60 s grace"。种子期单次 grow 估计 ≤100 s；今天 176/114。此后触碰 grow 路径的提交：`0f26330`/`54f125a`（JS observation budget by streams）、`92e01a4`（batch-C size-derived budget）、`808552d`、`1e9d32a`。L3 注意到了症状（"90 的 775 s < 今天三夹具之和"）但归因错误（见 L3-2）。 | **MAJOR**（影响每一张"节省/地板"表；L5 的"主干占 52%"直接建立在混用上） | 在算任何地板前先做一件事：**一次带 `DRILL-POLL-WAIT`/`condition met after` 的 instrumented sweep**（accel plan M3 已实现），或至少对 `10-grow-to-3` 在 c6b9c9e 镜像与当前镜像各跑一次 solo 对比；把 grow 176/114 标为"单次实测、来源纪元与 costs 种子不同、可能是回归"。 |
| X3 | **drill 98 的压缩杠杆需要两侧 ping。** L3/L4/L5 只说"产品设 nats.go `PingInterval`"；只有 L1 指出 IMPACT 门是服务端 `/connz`。 | `98:173` 用 `fault_partition_peer_on agt1 CUT_BROKER 4222`；`drills/lib/fault.sh:85-92` 明说 SIMFAULT 链是 INPUT+OUTPUT 双向、`--dport`+`--sport`——对称黑洞，agent 侧 close 的 FIN/RST 也到不了服务端。`98:183-184`：IMPACT（`/connz` 上 agt1 消失）与 RECOVERY 共用一个绝对预算且 IMPACT 先断言。`/connz` 消失由 **nats-server** 的 `ping_interval`/`ping_max`（默认 2 min/2）决定；这两个键不在 `internal/natsconf/preflight.go:42-56` 的 passthrough 表（未知键 fail-closed）。 | **MAJOR**（L3 98 行、L4 O12/T2、L5 表 C 的"≤120 s"估计均不成立） | 98 的检测地板只有"客户端 `PingInterval` + 服务端 `ping_interval` passthrough（产品增量：natsconf 表加两键）"一起做才能压；单做客户端只压 RECOVERY 不压 IMPACT。 |
| X4 | **没有任何 run log 可读，所有"今天等满"是推断。** L3/L4 诚实声明了（`/tmp/simdrills*` 不存在，我复核为空）；L1/L5 却把 73/74/96 的红路径窗写成确定支付。 | 本机无 rollup/evidence 归档；`poll_wait_total` 漏计子壳 poll（`lib/log.sh:146-150`）。 | MAJOR | 所有 R 类（红路径）数字标"推断，未见日志"，并给出上下界（0 ~ 预算）。 |
| X5 | **brief 自己的"175 s 阻塞 sleep"已过期。** | accel plan §1.1 定稿于 2026-07-24（`7748665`），drill 78 于 2026-08-12（`e9a6ff7`）新增 `sleep 65×3 + sleep 60` = 255 s（`78:89-91,141`）。我按 L4 的排除规则复算：40 站点、432.8 s（循环体内按单次计）。L4 抓到了；L1 照抄 175。 | MINOR | 引用 "175 s" 时注明"不含 78 的 255 s；当前字面合计 ≈433 s"。 |
| X6 | **L5 在"只读"任务里改了宿主**：探测 `lxc version` 触发 Ubuntu 的 socket 激活安装器。 | `snap list`: `lxd 5.21.7-1018661`，`snap changes` #2/#3 "Install lxd" 今天 21:31 CDT Done。仓库树未动（`git status` clean）。 | **BLOCKER**（对流程，不对手段；L5 自己已披露） | 报告必须原样携带这条披露；用户决定是否 `sudo snap remove lxd`。同时说明"廉价只读命令"边界在 Ubuntu 上是多孔的（`/usr/sbin/lxc` 是安装器 shim）。 |

## 1. 草案 L1（产品时间常量清单）

| 手段/条目 | 问题 | 证据 | 严重度 | 修正 |
|---|---|---|---|---|
| (c)1 `joinerBootGrace` fresh-joiner 跳过 | **成立。** 引用与代码顺序一致。 | 常量 `cluster_add_drive.go:692` ✓；poll 循环实际在 :707-720（2 s grain ✓）；`simcluster:265`（非 :263）`sctl stop tether-broker`；`:117-124` `!dirExists(raft)` → `runSelfInit` ✓；`adminStatusIsClustered` :722-724 ✓。 | — | 行号微调。它的条件（"本次调用跑过 init"）是四份草案里最安全的（见矛盾 §8-7）。 |
| 表 A `cutoverBroker` "6 次×3 s = 18 s 上限" 与 (b)8 "500 ms×6=3 s 内 nats 还没复活" | **上限算错。** 每次尝试是一次 `sendGrowTrigger`，带 `growTriggerTimeout` 10 s 的 `RequestWithContext`；nats 被 SIGKILL 期间连接处于 RECONNECTING，请求一般等满 10 s。真实上限 6×(10+3)=78 s，且主导项是请求超时不是 grain。 | `cluster_add.go:224` `context.WithTimeout(ctx, growTriggerTimeout)`；`:29` = 10 s；`cluster_add_drive.go:537-561` 循环。 | MAJOR | (b)8 的"必须改成时间窗"结论要按"每轮 ≥10 s"重推；同时这 78 s 上限是 invocation 1 的 37 s 与 invocation 2 的 67 s 之谜的候选解释之一（cutover 的 6 轮可能跨越两次调用）。 |
| 健康路径表 D 类："96 #57 死窗 390 + canary settle 60 = 450 s"、R 类 "96 判定环 180" | 见 X1。另 canary settle 是正向 poll：`_c3_gone_everywhere` 为真即返回。 | `96:396`、`:418-422`、`:647` + `:217`。 | MAJOR | D 小计 1150 → ≈700 s（在记录世界）；"96 关键路径必等 810 s" → ≈180–240 s。 |
| (d) "proxy 首拨退避 base/max/deadline 500 ms/30 s/2 min（`agent.go:1939-1941`），78 Arm A 测这条曲线" | **指错常量。** 那三个值属于 `applyOneHome`（expose home 应用重试）。drill 78 测的是 `proxyDialPolicy`：5 s base 倍增至 5 min cap。 | `internal/agent/agent.go:1937-1941` 函数名 `applyOneHome`；`internal/agent/proxy.go:698-702` `Base: 5s, Cap: 5min`；`78:88-91` 桶宽 65 s 对应 5 s 倍增序列。 | MAJOR | 换成 `proxyDialPolicy`（L4 O15 的引用是对的）；(d) 结论不变但论据换掉。 |
| (d) `LockLeaseTTL`："drill 30 **依赖** #31 泄漏的 marker 在 TTL 内存活……试金石失败的教科书例" | **依赖已不存在。** #31 台账 2026-08-11 记"已修"（grow-lock release 现已可靠）；`a3431a1` 又改了 30 的 unlock 臂（H-2 让 HALT 的 orchestrator 自己把 lease 标过期）。今天 30 走的是 `[GAP #31]` 为零的路径。 | `docs/deploy-tier-gotchas.md:97-100`；`git show a3431a1` 提交正文。 | MAJOR（这是 L1 试金石论证的样板例，例子本身失效） | TTL 仍归 (d)，理由改为它自己的推导（`lock_lease.go:95-115`，≥ 8 min/(2/3)）和 H-2 的"HALT 后可立即重跑"语义；把 30 从"依赖泄漏"改为"对 #31 的回归采样"。 |
| R 类 "74 B-dp 240 + C-auto 180（#33/#34）= 420 s 每次等满"、"73 #33 180" | **无日志支撑且与台账相反。** 74 的 band 签名是 `c-ss-preflow` 与 `b-negctrl-create`，B-dp 不是失败点；#34 台账 2026-08-11 复核记"auto-rebalance-on-return 在 return edge 发火（C-dp/C-event PASS）"。73 期望 GREEN 且注"#34 not manifesting"。 | `expected-verdicts.tsv:43-44`；`gotchas.md:165-169`。 | MAJOR | R 类 74/73 改为 "0 ~ 420 s，取决于本次 #34 形态；无日志"。 |
| F 表 "阻塞 sleep 175 s（36 处）" | 过期（X5）。 | — | MINOR | 改 ≈433 s / 40 站点，并注明 78 占 255 s。 |
| P 小计 "≈4000–4200 s" | 表内各行相加 = 4205–4405 s；且把 74 "产品修好后" 的 90 s 记进今天的账。 | 表 P 各行。 | MINOR | 重算；今天的账里 74 dwell+quiet 是 R 不是 P。 |
| "全套 351 个 poll 站点" | 未给计数模式。我按 L4 的模式得 363（含变量超时）/ 355（纯数字超时）。 | `grep -E 'poll_until(_fixed)? +[^ ]+ +[0-9]+ '` | MINOR | 统一用 363 并写出模式。 |
| nats.go 引用 | 同一草案对 :60-61 引 v1.52.0、对 `processPingTimer` 引 v1.51.0:5765-5786；go.mod 是 v1.52.0，该函数在 5774-5795。推导（第 3 个未答 tick、(4,6] min）**成立**：`nc.pout++; if nc.pout > MaxPingsOut`。 | `go.mod:15`；`nats.go@v1.52.0:5783-5784`。 | MINOR | 统一版本号。 |
| Σ ≈ 13060 s | 含 83/84 各估 60 s，不含 78。 | costs 40 行 = 12944 ✓。 | MINOR | 加 78（≥255 s sleep + F1 + 臂）。 |
| (c)2 服务端 ping 观察 | **唯一说对 98 的草案**（X3）。 | 见 X3。 | — | 保留并作为 98 的裁定依据。 |
| 表 1/2 的 47 次 grow | **计数正确**（我按 `grow_to_3`×2/`grow_to_2`/`setup_forcesingle_n2`/裸 `$SIM grow` 逐 drill 数得 47，含 22 的 #35 夹具第二次 grow `22:216`、51 的 I 臂 `51:497`、90 的 3 次裸 grow `90:186,213,216`、42-F `42:222`）。 | — | — | L5 的 41 应改为 47。 |
| "97 只等 STALE（5 s）" | 正确。 | `97:123` `_agt1_offline() { ! _agt_online agt1; }`。 | — | L3 相反的说法错（见 L3-1）。 |

## 2. 草案 L2（集群夹具供给）

| 手段/条目 | 问题 | 证据 | 严重度 | 修正 |
|---|---|---|---|---|
| 并行 grow 否决 | 成立；条件写守卫逐字核实。 | `internal/cluster/membership_ops.go:517-530`（草案写 503-517）：`guard := NOT upgradeMarkerExists AND NOT growMarkerHeldByOther`；`driveAdd :97-100` acquire、`:230` release。 | — | 行号微调。 |
| L1 concurrent-start（在 :717 进度行上触发） | 机制被代码顺序支持：render :177 → JS reset :189 → cutover :193 → `awaitJoinerBrokerUpLocal` :206（进度行打印于 :707）。但 "−55 s/grow" 与 "第二次调用随之消失" 两句并列会误导：invocation 2 的 67/22 s 工作量（catch-up→VOTER、G69 placement 观测 ≤30 s、JS meta 形成）**搬进** invocation 1 的 `waitJoinServing`，不消失；§4 自己承认了。 | `cluster_add_drive.go:165-215`。 | MINOR | 矩阵行写成"净省 = 60 s grace − joiner daemon 就绪时间（估计 3–8 s）；invocation 2 工作量不变"。 |
| "驱动器可能吞 cutover 拒绝 / 6 轮后放行" 的归因段 | 与 L1 同：每轮 10 s 请求超时是主导（见 L1 第 2 行）。 | 同上。 | MINOR | — |
| CRIU/checkpoint 否决 | 本机核实：Docker 29.6.1、`Experimental: false`、`criu` 未装、overlayfs。 | `docker info`；`command -v criu` 空。 | — | — |
| "`journalctl -b` 是否按容器切分——待核" | 可以升级为已证：仓库自己的注释说 boot_id 是 per-kernel、所有容器共享。 | `internal/agent/instance.go:29-31`。 | MINOR（正向） | 去掉"待核"。 |
| "clone 交接 ≈25 s" | 纯估计，与 L3 的 45–60 s 相差 2×；两者都无测量。 | — | MINOR | 标"估计，区间 20–90 s；上界取 96-D 注释 'JS meta re-forms legitimately slow'（`96:569`）"。 |
| "本 lane 地板 = 最长原子窗口 + 25 s：#57 的 390 s ≈ 7 min" | X1。 | — | MAJOR | 删除 390 作为原子窗口；改为"98 的服务端+客户端 ping 检测 (4,6] min 是当前最长原子窗口"。 |
| 30/40/41 "live 以保 #31 鉴别力" | #31 已修（08-11）；"鉴别力"应改写为"回归采样率"（每 sweep 从 ~30 次 grow 降到模板 1 次）。L3 §不确定 4 已提。 | `gotchas.md:97-100`。 | MINOR | 改措辞；裁定不变。 |
| 模板年龄上限 "<15 min（lock TTL）" | 同上：TTL 只在有泄漏 lease 时才是绑定项；更硬的年龄约束是 `xferReapMinObjectAge` 2 min、`rosterStaleGrace` 6 min、`MinXferCrossHomeReapAge` 15 min（草案 delta 表末行已列）。 | `transfer_reconcile.go:19`、`roster_stale.go:24`、`serveconf.go:287`。 | MINOR | 年龄上限的论据换成这三条。 |
| 数字：sum 216 min、−77 min、"8 卷"、"[env] 重叠 12 s = 176−97−67" | 复核成立（12944 s；2 卷/节点 × 4；算术对）。 | `lib/docker.sh:110-125` 挂 `vol_lib`（etc 另一处）。 | — | — |
| `cmd_down` 串行 stop | ✓ `simcluster:648-649`。 | — | — | — |

## 3. 草案 L3（拆分与调度模型）

| 手段/条目 | 问题 | 证据 | 严重度 | 修正 |
|---|---|---|---|---|
| 97 行："`agt1 leaves ONLINE` ≤90 实际 = `DefaultOfflineAfter` 60 s"；最长单臂 "97-soak ≈ 8–10 min，地板由 OfflineAfter 60 s 决定" | **错。** 谓词是"非 ONLINE"，STALE（5 s）即满足。每 cycle 的 agent 臂 ≈ 5 s + 返回注册几秒，不是 60–120 s。 | `97:123` `_agt1_offline() { ! _agt_online agt1; }`；`97:279`。 | MAJOR | 97 的模板制估计降到 ≈ τ + 25 + 6×(30–45) + 25 ≈ 5–6 min（估计）；"P_max 9 min 由 97 设定"改为由 74-C/98 设定；wall 公式的数字表随之下修。 |
| "`drill-costs.tsv` 对 90 已过期——M6/M8/M9 是之后加的（775 s 甚至小于今天三夹具的 solo 和 804 s）" | **归因错。** 种子提交 `7748665` 里的 90 与今天字节相同（218 行、3 次裸 grow、M6/M8/M9 全在）。775 < 804 的真正含义是 X2：种子期每次 grow 明显快于今天的 176/114。 | `git show 7748665:test/simcluster/drills/90-alerts-lifecycle.sh \| wc -l` = 218 = 今天。 | MAJOR | 改为 X2 的表述；这条比"costs 过期"严重得多——它意味着草案用今天的 spine 去减种子期的 body。 |
| 98 行只提客户端 `PingInterval` | X3。 | — | MAJOR | 补服务端 `ping_interval` passthrough 前提。 |
| §不确定 7 "`XferBudget("b", 1 GiB, legs)` 可能 >5 min，390 s 可能不够" | 可以关闭：96-A 是 `tether pull`（`96:118`），pull 路径无声明尺寸 → size==0 → 取 floor 5 min；R16 finalize 的年龄门也用同一函数（`transferTimeoutFor(tier,size)`）→ 6 min。 | `internal/proto/xfer.go:104-106,120-123`；`xfer_inflight.go:723`。 | MINOR | 从不确定项删除；L4 O1 的读法正确。 |
| 产品侧建议："driver 本次调用走了 render 分支 → 立即 HALT" | **条件不安全。** `!joinerBrokerUpLocal` 在每次调用都重新求值（:177）；drill 42-F 的 invocation 2 里 joiner 正在 crash-restart 时该分支同样被取到（render 幂等），立即 HALT 会把 grace 注释里 3/4 失败的那个场景修回去。 | `cluster_add_drive.go:177,697-704`。 | MAJOR | 换成 L1 的条件（"本次调用执行了 `runSelfInit`"，:117-124），它只在 raft/ 不存在时为真。 |
| "今天 43 drill 在 -j6 的 Σ ≈ 160 min（accel-plan §6.4）" | 那是 plan 对 38 drill 的**目标**（"sum 195.2 → ≤160 min"），不是 43 drill 的实测。 | `simcluster-accel-plan.md:438`。 | MINOR | 用 12944 s（40 行）+ 78/83/84 估计。 |
| 74-C 估计含 "settle 90" 整段 | `poll_until_fixed 90 3 _dist_stable` 只固定网格，仍首次为真即返回（两读相隔 3 s 相同即可）。 | `lib/log.sh:78-83`；`74:474`。 | MINOR | settle 记 6–90 s。 |
| 74/73 "eligibility 240 s 模板下预计 ≈0" | 无产品常量支撑（我查到 `eligibleProxyHomes` = VOTER ∧ cert_fp ∧ public_host ∧ `homeReachable`（读 observe 5 s tick 的 lastObserve）；drill 注释说典型 60–90 s，来源未归因）。L5 §不确定 5 把它列为候选产品可观测性缺陷。 | `internal/broker/proxy_rebalance.go:196-221`、`proxy_reconcile.go:588-597`。 | MINOR | 标"估计；需在模板集群上实测一次"。 |
| 裁决聚合的优先级函数 | 核实成立。 | `lib/assert.sh:495-501`。 | — | — |
| 行号：`roster.go:35`→:31；`run-drills.sh:455-465` `_fail_context` 实际 :503；`assert.sh:469-473` drill_begin 前缀检查实际 :481-484；`simcluster:345-352` #70 注释实际 :363-372。 | — | — | 微 | — |
| 单元/容器数（U≈63–66、4.2/单元、270–300）、τ、W_grow、δ | 全是估计且已标注；但 "30 s 错峰后并发不受限 → W_grow ≈ 9 min" 是未验证外推（草案自认）。 | — | — | 给区间（g≤3 时 13.5 min）。 |

## 4. 草案 L4（oracle 形态）

| 手段/条目 | 问题 | 证据 | 严重度 | 修正 |
|---|---|---|---|---|
| O1（390 条件性、R16 年龄门 6 min） | **正确**，是五份里对 96 最准的读法。 | 见 X1；`xfer_inflight.go:723`。 | — | — |
| O12/T2 "唯一能压的是产品设 `nats.Options.PingInterval`；若改：330 s → ≤120 s" | X3：漏掉服务端。 | — | MAJOR | 加服务端 `ping_interval`（需 natsconf passthrough 增量）；否则 IMPACT 门不动。 |
| O15 常量与 78 的 255 s | 正确（`proxy.go:701-702`）；"字面 sleep 合计 433 s"我复算为 432.8 s/40 站点 ✓；78 不在 175 里 ✓（日期证明）。 | — | — | — |
| 网格 overshoot "10–17 min sum → -j 12 下墙钟 ≈1–1.5 min" | 除以 j 只在 Σ/j 绑定时成立；-j 12 的 wall = 96 本体（20.7 min），墙钟收益只有 96 自己内部的 overshoot。结论（"不是杠杆"）不变，推导要改。 | `simcluster-accel-dispositions.md:17,23`。 | MINOR | 写成"墙钟收益 ≤ p_max drill 内部的 overshoot（估计 <1 min）"。 |
| 数字：363 站点、timeout 之和 22 713 s、直方图 | 22 713 s 逐字复算成立；363 ✓；直方图 3 s:169、5 s:27（草案 171/33，含变量超时站点）。 | — | — | — |
| O4/O5/O6 "只在 #33/#34 显形时付满" | 与 `expected-verdicts.tsv:43-44` 一致，措辞诚实。 | — | — | — |
| 观测面合法性五问 | 判断类；`adminsock/protocol.go:31-34`、`runtime_introspect.go:1-16`、`rehome_events.go:9-13`、`events.sh:34-38` 引用逐字成立。 | — | — | — |
| T 表 "≥ 360 s（T1）" | 条件同 X1（仅 kill 真落飞行中）；草案已写"否则 0"。 | — | — | — |
| §不确定 2（98 的 6 min 上界 vs 330 预算） | 我的读码结论相同：`processPong` 只清 `pout`、不 Reset 定时器；最坏 (4,6] min；对称黑洞下服务端同样 (4,6]。330 s 预算在相位落在后 25% 时不够——这是**未被记录的相位 flake 候选**。 | `nats.go@v1.52.0:2755,5783-5793`。 | MINOR（正向确认） | 在 98 的 T7 重推导里把预算改成 ≥360 + 60。 |

## 5. 草案 L5（基础替代 + 怀疑者）

| 手段/条目 | 问题 | 证据 | 严重度 | 修正 |
|---|---|---|---|---|
| "去 systemd → 26 个 N≥2 drill 的 grow 走不通" | **成立且是本轮最硬的一条结构论证。** | `cluster_grow_cutover.go:18-23`（SIGKILL 靠 `Restart=always` 复活）、`:26` 45 s；`install.sh:1225-1226`。 | — | — |
| "`joinerBootGrace` = 41 次 grow × 60 s = 2,460 s ≈ 18%" | **计数少 6。** 15×2 + 11×1 漏了 90 的 +3、51 的 +1、42 的 +1、22 的 +1 → 47 次 = 2820 s（≈22% of 12944）。 | 见 L1 末行的逐 drill 计数。 | MAJOR | 改 47/2820。 |
| 表 A "96-A = 305 + 40 + 390 + 150 ≈ 885 s → 严格 Mandate 拆臂后 wall ≈ 14 min"；表 B "96-A 560–620 s" | X1。去掉 390 后 96-A ≈ 305 + 40 + (A2 #58: ONLINE 60 + reap ≤90) ≈ 7–8 min；此时 p_max 更可能是 98（700 s）或 96-D/F。 | — | MAJOR | 表 A 的 14 min 改为"≈12 min（98 绑定，估计）"；表 B 相应下修。 |
| "主干份额 7,016 s ≈ 52%" | X2：用今天的 solo spine 乘以种子期的 drill 数，再除以种子期 sum；种子期 grow 更快，份额被高估。 | `drill-costs.tsv` 11=152 < 176。 | MAJOR | 标"上界估计"，或等 instrumented sweep。 |
| "`user@1000` `Delegate=no`，只委派 cpu/memory/pids" | 前半错、后半对。 | `systemctl show user@1000.service`: `Delegate=yes`, `DelegateControllers=cpu memory pids`。 | MINOR | 改。 |
| 表 A "67：quorum 丢失下的 tier-B push 撞 5 min 底线" | 写成事实，实为假设：G67 修复后拒绝在 `xferProvisionBudget` 8 s 内返回（`xfer_provision.go:46`）；777 s 无解释（L3 §不确定 2 同）。 | — | MINOR | 标假设并给两种候选（push 挂到 CLI 超时 / -j6 污染）。 |
| 表 C 把 `XferTimeoutTierBFloor`→30 s 列为杠杆并计代价 | 记录世界里 96 不付 390，压 floor 对 96 零收益，只剩风险。 | X1。 | MINOR | 从杠杆表删除（保留为"不要动"的注）。 |
| 产品修法 "fresh 且 ECONNREFUSED/ENOENT 立即 HALT" | 这是 L2 明确不推荐的 socket-absence 变体（分不清"从未启动"与"crash-restart 周期中"，admin socket 生命周期未读）。 | `cluster_add_drive.go:697-704`。 | MAJOR | 换 L1 的"本次调用跑了 init"条件。 |
| "8 个真时间探针常量之和 ≈1,220 s → wall 物理下界 ≈6.5 min + 主干（96-A 390）" | 含 390（条件性）；去掉后最大者是 98 的 (240,360]。 | — | MINOR | 下界句改为"98 的 ping 检测 + 梯子 ≈ 5–7 min + 主干"。 |
| 本机能力表其余项 | 复核：criu/podman/nspawn/firecracker 未装 ✓、qemu 已装 ✓、`/dev/kvm` + kvm 组 ✓、`newuidmap` 缺 ✓、`/etc/subuid` 有 ✓、`sudo -n` 失败 ✓、inotify 8192 ✓、88 vCPU/251 GB ✓、`Experimental: false` ✓。 | — | — | — |
| LXD 副作用 | X6。 | — | BLOCKER（流程） | 原样上报。 |
| "修正树 -j6 含归因 62.6 min"、"-j12 20.7"、"B1 27.3/28.2" | ✓ `dispositions.md:17,158`；`plan.md:502`。 | — | — | — |

## 6. "引用了但源码/台账不支持"清单

1. L1：proxy 首拨退避 = `agent.go:1939-1941` 的 500 ms/30 s/2 min → 实为 `applyOneHome`；drill 78 测 `proxy.go:701-702` 的 5 s→5 min。
2. L1：`cutoverBroker` 上限 "6×3 s = 18 s" → 每轮含 10 s 请求超时（`cluster_add.go:224`）。
3. L1：drill 30 "依赖 #31 泄漏 marker" → #31 台账 2026-08-11 已修。
4. L1/L2/L5：96 的 390 s 为固定支付 → 条件性（`96:396-423` + `expected-verdicts.tsv:55`）。
5. L1/L3/L4/L5：74 C-auto 180 "每次等满" → 台账 #34 2026-08-11 记发火；band 在 `c-ss-preflow`；无日志。
6. L3：97 等 `OfflineAfter` 60 s → 谓词是 `! _agt_online`（STALE 5 s）。
7. L3：90 的 M6/M8/M9 "之后加的" → 种子提交里已存在（文件字节相同）。
8. L3：Σ ≈ 160 min "accel-plan §6.4 实测" → 是 plan 目标值。
9. L3/L4/L5：客户端 `PingInterval` 单独可压 98 → IMPACT 门在服务端 `/connz`，fault 对称（`fault.sh:85-92`），`ping_interval` 不在 passthrough 表。
10. L5：grow 41 次 → 47 次。
11. L5：`Delegate=no` → `Delegate=yes`（controllers 那半对）。
12. L1（转引 brief）：阻塞 sleep 175 s → ≈433 s（78 未计）。
13. brief 本身："第二次调用含 former-N1 cutover + JS reset + nats restart" → 代码顺序 cutover 在 invocation 1 的 P5（:193）先于 grace（:206）；L1/L2 已标疑，L5 给了另一假设（lone-JS fail-stop + `growConnectAuthRetryWindow` 30 s）。**无日志不可裁**，但它决定 L2 的净省是 55 s 还是更多。

## 7. 必须标"估计"并给上下界的项

| 项 | 出处 | 建议区间 |
|---|---|---|
| 压 (a)(b)(c) 后 S3 ≈ 80 s | L1 表 2 | 60–140 s（add2 的 67 s 内部无法分摊；G69 观测上限 30 s + JS meta 1→2 未知） |
| clone/模板恢复 τ | L2 25 s / L3 45–60 s | 20–90 s，上界见 `96:569` 注释 |
| 拆臂后 Σ | L1 125 / L3 110–130 / L5 120–140 min | 依赖 X2 未解；先不给点估计 |
| 97 拆后单元 | L3 8–10 min | 5–6 min（改 STALE 后） |
| 74-C 单元 | L3 ≈9 min | 5–9 min（settle 6–90、C-auto 0–180） |
| 96 的 22 min 去向 | 全部草案 | 未知；F 前置实测 >240 s（`96:706-710`）、D3 ≤300、canary 0–60、A2 ≤150；390 记 0/390 双世界 |
| 67 的 777 s | L3/L5 | 2.5–13 min 两个假设 |
| W_grow（grow lane） | L3 | 9 min（30 s 错峰不限并发）~13.5 min（g≤3） |
| poll overshoot | L4 | 10–17 min sum；墙钟 <1 min |
| 98 检测窗 | 全部 | (4,6] min 双侧；330 s 预算存在相位 flake 候选 |

## 8. 草案之间的矛盾

1. **96 的 390 s**：L1/L2/L5 视为必付；L3/L4 视为条件性。**L3/L4 对**（§X1）。
2. **98 的压法**：L1 要求客户端+服务端；L3/L4/L5 只提客户端。**L1 对**（§X3）。
3. **97 的等待对象**：L1 "只等 STALE 5 s"；L3 "OfflineAfter 60 s"。**L1 对**（`97:123`）。
4. **drill 78 测的常量**：L1 `agent.go:1939`（500 ms/30 s/2 min）；L4 `proxy.go:701`（5 s→5 min）。**L4 对**。
5. **grow 次数**：L1 47；L5 41；L3 "≈25 处 spine 节省 / 9 个 own-grow 单元"（不同口径）。**L1 对**。
6. **`joinerBootGrace` 修法的触发条件**（四种）：L1 "本次调用跑了 init"；L2 sim 侧在 :707 进度行上并发启动（不改产品）；L3 "走了 render 分支"；L5 "socket ECONNREFUSED/ENOENT"。L3/L5 的条件在 drill 42 resume 场景下会重现 grace 注释所述的 3/4 失败；L1 的条件最强；L2 的方案零产品改动但要改 `cmd_grow` 的 rc=75 契约（`simcluster:311`）并留一个 drill 钉 HALT/resume。
7. **invocation 2 的 67 s 构成**：brief 说含 cutover；L1/L2 按代码说 cutover 在 invocation 1；L5 归因于 lone-JS fail-stop + auth retry 30 s；我补一个候选：cutover 6 轮×(10+3) s 可跨两次调用。无日志不可裁。
8. **τ**：L2 25 s vs L3 45–60 s（都是估计）。
9. **阻塞 sleep 总量**：L1 175 s（转引 brief）vs L4 433 s。**L4 对**。
10. **poll 站点数**：L1 351 vs L4 363（无模式 vs 有模式）。
11. **今日 Σ**：L1 218 / L2 216 / L5 226 min——78/83/84 的处理不同；都没有 78 的实测。
12. **#31 与 LockLeaseTTL**：L1 把 #31 泄漏当活依赖；L2 记 #31 已修。**L2 对**。
13. **#34 与 74 的 180/240 s**：L1/L3/L4/L5 "每次耗满" vs 台账 2026-08-11 "发火"；band 在 `c-ss-preflow`。无日志不可裁，但"每次耗满"缺支撑。
14. **96-A 的预算是否够 1 GiB**：L3 §7 / L5 §4 担心 `XferBudget` >5 min；L4 指出 pull 路径 size==0 取 floor。**L4 对**。
15. **wall 地板**：L1 "L1 内做不到；拆臂+j≥20 ≈6.3 min"；L2 "≈4.5–5 min（#57 修掉后）"；L3 "8–10 min"；L4 "6.5–8 min"；L5 "10–12（B）/ 6–8（C）min"。杠杆集合不同不算矛盾，但 L1/L2/L5 的数字都被 X1（390）抬高，L3 的被 97 误读抬高；修正后五份的"不改产品常量"地板会向 **98 的 (4,6] min 检测 + 主干** 收敛，而不是 96。
16. **模板化与 grow 后瞬态**：L2 说 clone 是"重启后的合法部署态"并要求交接门重建 post-grow 前提；L3 §不确定 4 指出 `DEGRADED-WRITABLE` 标签、proxy-eligibility 恢复窗等 grow 后瞬态在模板消费者里永远不可见。两者相容但没人量化"哪些 claim 隐含 post-grow 而非 post-restart"。
17. **hermetic 分工**：L5 主张把 90/93/74/40 的部分臂迁走（sum −15–20 min）；L3/L4 未评估；与 R8 删除门（Go 后继 + 保留一个具名真栈探针）的关系只有 L5 提到。

## 9. 本次核对的局限

- 未跑任何 drill/测试；没有 run log，所以"今天到底等了多少"对每份草案都只能核"预算与条件"，不能核"实付"。
- `docker info` 的 `Experimental` 与 `snap` 状态为本机即时读取；LXD 已装事实以 `snap list` 为准。
- 我复算的 sleep（432.8 s/40 站点）与 poll（363/22 713 s）用的是 L4 给出的模式；改模式数字会变，但量级结论不变。
