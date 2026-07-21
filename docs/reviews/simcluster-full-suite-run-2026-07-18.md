# simcluster 全量 drill 实跑缺陷报告（2026-07-18）

Date: 2026-07-18 · 被测 commit: `fec3bfa`（S1–S9 补完收官，工作树干净）· 定稿人：主进程
运行环境：`weilandserver`（192.168.1.150，88 vCPU / 251 GiB / Ubuntu 24.04 / docker 29.6.1 / cgroup v2）

> **这是什么**：`test/simcluster/` deploy-tier 测试补完（S1–S9，37 个 drill）落地后的**第一次全量实跑**，
> 外加 hermetic 两层与 harness 自检的完整回归。本报告是这次实跑的**缺陷清单与发布判定**。
> **这不是**新一批 drill 的 plan/review——本轮**只测不修**（roadmap §0.2），产品与 harness 的修复各自另立增量。
>
> **证据分级**（本报告每条结论都带标记）：
> `[主]` = 主进程亲自复核过源码/日志/复跑；`[专]` = 多专家工作流产出并经对抗核验；`[开]` = 未定案的开放问题。
> 多专家原始综合稿存档于同目录 `simcluster-full-suite-run-2026-07-18-expert-synthesis.md`（13 agent、
> 6 视角分析 + 6 对抗核验 + 1 综合，全部 Opus 4.8）。

---

## 0. 结论先说

### 0.1 发布判定：**不可放行**

三条独立理由，任一条单独成立即足以阻塞：

1. **确证的发布级产品缺陷**：`tether cluster drain` 返回 **rc=0「drain ok」**、控制面显示 expose 的 home
   已迁走，而**公网端口在新 home 上从不监听**（P1）。"成功退出码 + 数据面静默丢失"是最危险的形态——
   运维会据此继续 retire/关机，用户的 expose 永久失联。这是 HA 面上**唯一那条"被支持的"计划内迁移路径**。
2. **灾备面整条不可执行，且至今从未被端到端证明过一次**：runbook §5.2 第 1 步就 permission denied（D2/DOC-27），
   第 3 步结构性 FATAL（P2/#51），恢复文案指向必然 crash-loop 的下一步（P4/#64），唯一预检动词对
   "DB 根本不在那里"完全失明（P5/#50）。而本轮 DR 尾段（#52 / #53 / terminus）因 **harness 自己**
   seam 写不全而**完全没跑**（H2）。
3. **本轮的 "25 GREEN" 不能作为放行依据**：至少 4 条 drill 的关键 oracle 是**结构性空绿或恒失败**
   （H1/H2/H6/H13），**24 处覆盖缺口用 `warn` 绕过了 `not_covered` 计数器**（H4），三条已登记缺陷
   （#25/#26/#27）用"倒置 assert_ok"钉住因而**计入 GREEN**（H4c），静态闸 `lint-drills.sh` 的 BATCH
   白名单不覆盖 **21/37** 个 drill（H4）。汇总数字系统性地比真实情况乐观。

### 0.2 必须同时写明的阴性结论（避免过度恐慌）

**问题集中在元数据层与聚合层，不在断言层。** 抽查的 25 个 GREEN **无一空绿**——断言密度普遍 28–51 条、
带 NON-VACUITY 正控与真实 sha256/sentinel 往返；`50/51/52` 三条备份/灾备/凭据 drill 的落地 verdict 与
README 期望**逐条相符**、`assert_fail=0`、无 APPEARS-FIXED 分支触发，排除了"该 family 有缺陷被悄悄修好
或悄悄破坏"。hermetic 两层与 harness 自检**全绿**。**单 drill 的工艺水平很高；坏的是记账与聚合。**

### 0.3 最小修复集（修这 5 条就能堵住最危险的面）

| 优先级 | 动作 | 关闭 | 可复用的现成代码 |
|---|---|---|---|
| P0-1 | `cluster drain` 在 `migrateExposes` 后主动弹开受影响 agent 的 NATS 连接（或新增 home-push 通道）；在此之前**对含 rebuild-ON expose 的节点不得返回 0**。**修法见 §3.4——把 home 投递挂到既有的 `ReconcileInterval` 循环上，可一并覆盖同族的 #48** | **P1（发布级）** | `internal/agent/agent.go:1499-1516` ReconnectHandler；`proxy_reconcile.go:360` 已有 push 形态；`broker.go:1096` 现成 ticker |
| P0-2 | `recovery restore` 加 `--config`，落地即调 `applyClusterSeam` | **P2 / #51** | `cmd/tether/cluster.go:880`（自带 fail-closed decode 校验） |
| P0-3 | restore 完成文案补两句（lone-voter + clustered conf 需 `reconcile nats --to-standalone --confirm-single`；fresh box 需 `reconcile nats --manual`） | **P4 / #64** + #52 提示半边 | `internal/broker/clusterstatus.go:354` 已有现成 remedy 文案 |
| P0-4 | `cluster upgrade` 的 `AtTarget` 对"该主机无 colocated agent"短路为 broker-only 判据；补 stale-lock 清除动词 | **P3（NEW）** | `internal/clusterupgrade/plan.go` |
| P0-5 | doctor 的 DB 检查加一行 `db.Ping()`；check 列表接上已存在的 `readClusterPublicIdentities` | **P5 / #50 + P6 / #54 facet2** | `internal/clusteroffline/doctor.go:82`、`cmd/tether/cluster_secrets.go:32-47` |

harness 侧另有 7 条 P0（见 §4.2）——**不修则本层测试结果不能作为发布依据**。

---

## 1. 实验怎么跑的（可复现）

| 层 | 命令 | 结果 |
|---|---|---|
| hermetic 单测（无缓存） | `go test -count=1 ./...` | **rc=0，0 失败** |
| e2e 矩阵 | `make e2e`（`-tags e2e_matrix`，串行） | **rc=0，490.4s，26 子测试全 PASS** |
| lint | `make lint`（golangci-lint v2） | **rc=0** |
| harness 自检 | `sh tests/lint-drills.sh` / `sh tests/verdict-contract-test.sh` | **batch OK（16 契约 drill，0 违规）** / **ALL PASS** |
| deploy-tier 全量 | 见下 | **37/37 有 verdict** |

deploy-tier 分两波（按 README 的 family 调度策略，避开峰值 grow 并发）：

```sh
# 波 1：N=1 家族（13 drill）全并行
./remote.sh drill-all --logdir /tmp/simdrills-w2 00-skeleton 21-smalldisk-tierb 31-node-upgrade-fleet \
  32-install-lifecycle 43-migrate-live-data 60-user-journey 61-transfer-edges 62-remote-fs-safe \
  70-expose-journey 72-proxy-subscription 80-session-isolation 81-admin-evict-session-rm 94-agent-reconcile
# 波 2：grow / force-single 家族（24 drill）-j 4
./remote.sh drill-all -j 4 --logdir /tmp/simdrills-w1 10-grow-to-3 … 97-soak-cycles
# 取证：对 5 个可疑 RED 串行单跑，排除并发干扰
./remote.sh drill-all -j 1 --no-retry --logdir /tmp/simdrills-solo \
  30-rolling-upgrade 40-drain-retire 42-rejoin-returning 93-metrics-observability 96-mid-flight-chaos
# 定位：保留现场重跑 30（第三次复现），尸检 roll.log 与各 broker 版本
SIM_KEEP=1 ./simcluster drill 30-rolling-upgrade
```

宿主 `fs.inotify.max_user_instances=8192`（已按 README 调高），全程 load ≤ 4.3/88 vCPU，
资源**不是**任何一次 RED 的成因。全套 wall-clock ≈ 36 分钟（波 1 141s；波 2 ~26min）。
**总断言数 1198 条**。

> **实测订正 README**：各 drill 头注标称 Runtime（~11min / ~22min）是**并行占位窗口**而非单跑耗时，
> 二者不矛盾（96 实测 ~22min、97 ~16min 与标称吻合）。中性观察：37 个 drill 中 30 个缺耗时元数据。

---

## 2. 全量结果

### 2.1 五态汇总

| verdict | 数量 | 含义 | 处置 |
|---|---|---|---|
| GREEN | **25** | 全部断言为 KEPT 不变量 | 见 §0.2 的可信度限定 |
| ASSERT-FAIL | **5** | 不变量破裂 / 签名守卫触发（**新信号，优先深挖**） | 30 / 71 / 74 / 93 / 96 |
| PRODUCT-RED | **4** | 已登记缺陷按文档化原因复现（预期内，**仍是发布阻塞**） | 31 / 50 / 51 / 52 |
| SETUP-RED | **2** | 前置/fixture 失败 | 40 / 42 |
| INCOMPLETE | **1** | 登记的覆盖缺口 | 95 |

**无 INFRA-ABORT、无 CONTRACT-ERROR**：37 个 drill 全部产出了唯一、良构的 `DRILL-VERDICT` 行——
verdict 契约本身工作正常。

### 2.2 逐 drill 结果

| drill | verdict | af/sr/pr/nc | pass | 与 README 期望 |
|---|---|---|---|---|
| 00-skeleton | GREEN | 0/0/0/0 | 13 | ✓ |
| 10-grow-to-3 | GREEN | 0/0/0/0 | 19 | ✓ |
| 11-grow-gaps | GREEN | 0/0/0/0 | 13 | ✓ |
| 12-ghost-voter | GREEN | 0/0/0/0 | 13 | ✓ |
| 13-inbroker-reconcile-perm | GREEN | 0/0/0/0 | 11 | ✓ |
| 20-forcesingle-natsconf | GREEN | 0/0/0/0 | 14 | ✓ |
| 21-smalldisk-tierb | GREEN | 0/0/0/0 | 8 | ✓ |
| 22-forcesingle-online | GREEN | 0/0/0/0 | 34 | 期望 INCOMPLETE → **GREEN**（#35 未复现） |
| **30-rolling-upgrade** | **ASSERT-FAIL** | 1/0/0/0 | 13 | 期望 GREEN → **RED**（**P3 + H1**） |
| 31-node-upgrade-fleet | PRODUCT-RED | 0/0/1/0 | 27 | ✓（#28） |
| 32-install-lifecycle | GREEN | 0/0/0/0 | 37 | ✓ |
| **40-drain-retire** | **SETUP-RED** | 0/1/0/0 | 32 | 期望 INCOMPLETE → **SETUP-RED**（**H7**，单跑转 GREEN） |
| 41-shrink-to-standalone | GREEN | 0/0/0/0 | 30 | ✓ |
| **42-rejoin-returning** | **SETUP-RED** | 0/1/0/0 | 27 | 期望 INCOMPLETE → **SETUP-RED**（**H8**，确定性复现） |
| 43-migrate-live-data | GREEN | 0/0/0/0 | 38 | 期望 INCOMPLETE → **GREEN**（outcome b） |
| 50-backup-restore | PRODUCT-RED | 0/0/3/0 | 68 | ✓（#50/#64/DOC-27） |
| 51-full-dr | PRODUCT-RED | 0/0/1/1 | 53 | ✓（#51；**尾段未跑，见 H2**） |
| 52-credential-rotation | PRODUCT-RED | 0/0/4/2 | 37 | ✓（#54/#56/DOC-23） |
| 60-user-journey | GREEN | 0/0/0/0 | 38 | ✓ |
| 61-transfer-edges | GREEN | 0/0/0/0 | 41 | ✓ |
| 62-remote-fs-safe | GREEN | 0/0/0/0 | 23 | ✓（**但 Arm2 缺口被写成 PASS，H4b**） |
| 70-expose-journey | GREEN | 0/0/0/0 | 28 | ✓ |
| **71-expose-rehome-failover** | **ASSERT-FAIL** | 1/0/0/0 | 22 | ✓ 设计即 RED-EXPOSES（**P1**） |
| 72-proxy-subscription | GREEN | 0/0/0/0 | 47 | ✓ |
| 73-proxy-cluster-ha | GREEN | 0/0/0/0 | 42 | ✓ |
| **74-rebalance-on-return** | **ASSERT-FAIL** | 2/0/0/0 | 38 | ✓ 设计即 RED-EXPOSES（**但本次归因不成立，H10/H11**） |
| 80-session-isolation | GREEN | 0/0/0/0 | 42 | ✓（**含 #25 未限速，被记成 GREEN，H4c**） |
| 81-admin-evict-session-rm | GREEN | 0/0/0/0 | 40 | ✓（同上 #26） |
| 82-agent-onboarding-invite | GREEN | 0/0/0/0 | 29 | ✓（同上 #27） |
| 90-alerts-lifecycle | GREEN | 0/0/0/0 | 49 | 期望 INCOMPLETE → **GREEN** |
| 91-client-converge | GREEN | 0/0/0/0 | 37 | 期望 INCOMPLETE → **GREEN**（#46 未复现） |
| 92-js503-remote-alert | GREEN | 0/0/0/0 | 34 | 期望 INCOMPLETE → **GREEN** |
| **93-metrics-observability** | **ASSERT-FAIL** | 3/0/0/0 | 36 | 期望 INCOMPLETE → **RED**（**H13**，确定性复现） |
| 94-agent-reconcile | GREEN | 0/0/0/0 | 51 | ✓（**但 ps LOST 覆盖是 overclaim，D6**） |
| 95-broker-selfheal | INCOMPLETE | 0/0/0/1 | 34 | ✓（**该缺口很可能是假缺口**，§5-L2-7） |
| **96-mid-flight-chaos** | **ASSERT-FAIL** | 4/0/1/3 | 38 | 期望 PRODUCT-RED → **ASSERT-FAIL**（**H6/H14 + Q2/Q3/Q4**） |
| 97-soak-cycles | GREEN | 0/0/0/0 | 42 | ✓ |

### 2.3 单跑取证（排除并发伪红）

| drill | 并发跑 | 串行单跑 | 判定 |
|---|---|---|---|
| 30-rolling-upgrade | ASSERT-FAIL | **ASSERT-FAIL**（+ 保留现场第三次复现） | **确定性，非并发伪红** |
| 40-drain-retire | SETUP-RED | **GREEN** | 时序竞态触发（叠加 harness 缺陷 H7） |
| 42-rejoin-returning | SETUP-RED | **SETUP-RED** | **确定性**（harness 漏传 flag，H8） |
| 93-metrics-observability | ASSERT-FAIL | **ASSERT-FAIL** | **确定性** |
| 96-mid-flight-chaos | ASSERT-FAIL | **ASSERT-FAIL**（4 条一致） | **确定性** |

---

## 3. 产品缺陷

### 3.1 发布阻塞级

#### P1 — `cluster drain` 返回成功，但被迁移的 expose 数据面永不跟随 `[专]`
- **类别**：产品缺陷 · **严重度**：**blocker** · **gotcha**：#29 族**新面**（建议开子条目，不发新号）
- **暴露**：`71-expose-rehome-failover` 臂 B-migrate
- **证据**：`71.log:198` `Arm B cluster drain brk3 rc=0 out=[drain brk3 ok]` → `:199` PASS「drain-migrate 路径本轮可达」
  → `:200` **FAIL**「wstrand migrates to a survivor voter + SERVES within 180s」→ `:201` 180s 超时。
  台账登记的三堵墙**本轮全未命中**：`:184` 隧道 FIXTURE PASS、`:197` Arm E 拒绝签名 PASS（证 #31 未拦）、drain rc=0。
- **机理**：`internal/broker/clusterdrain.go:142` `DrainNode` → `:677` `migrateExposes` 只做一次
  `port.PlanReassignHome` raft 写（改 `port_allocations.home_broker`），`:151-152` `if !retire { return nil }`
  ——**不停 nats-server、不断开 agent、不发任何通知**。而 home 指令**唯一**投递通道是 register 回复：
  `internal/broker/home.go:121` `homeForRegister`（`:119` 注释自陈 drives a rehome on the **next reconnect**）
  → `internal/agent/agent.go:1170` `applyHomeDirectives`。agent 侧 `agent.go:1493-1499` 明写
  "registers ONCE and heartbeat is fire-and-forget"，只有 `nats.ReconnectHandler` 才触发 re-register。
  **投递通道唯一性已核验**：全仓非测试代码中 `applyHomeDirectives` 仅一处调用者，`a.register(` 仅两处。
  rc=0 反证 home 确已改写（migrateExposes 出错时 DrainNode 必 return err）。
- **运维后果**：执行 `tether cluster drain <brk>` 准备下线一台 broker → CLI exit 0 + `expose explain` 显示新 home
  → 运维继续 retire/关机 → **用户 expose 永久失联，且全程无任何告警**。
- **动作**：见 §0.3 P0-1。回归时必须同时抓 `expose explain --json` 终态与旧 home 的 curl（现在两者都不落盘，H9）。

#### P2 — `recovery restore` 无 `--config`，DR runbook §5.2 逐字执行结构性不可完成 `[专]`
- **类别**：产品缺陷 · **严重度**：**blocker** · **gotcha**：**#51（live-confirmed）**
- **暴露**：`51-full-dr` 臂 G1 · **证据**：`51.log:61` PRODUCT-RED，签名 `data_dir is unset|refusing to silently downgrade a cluster DB to single mode`；前置 `:33` 已证 fresh box 的 broker.yaml 无 cluster seam。
- **机理**：`newClusterRestoreCmd` 的 flag 集实测仅 `--data-dir/--db/--secrets-dir/--confirm-node-id/--raft-addr`，
  **无 `--config`** ⇒ 结构上不可能写 seam；兄弟路径 `cmd/tether/cluster.go:799` → `applyClusterSeam`（定义 :880，
  5 字段模板 :905-906）自动写全。`scripts/install.sh:548-556` 把 `cluster:` 整段注释掉 ⇒ 启动 FATAL。
- **运维后果**：全灭 DR 走到第 3 步必卡死。错误串只说 `data_dir is unset`，不给完整 seam 形状；正确的 5 字段形状
  **只存在于产品源码里**——runbook 唯一提 seam 的 `docs/cluster-runbook.md:493` 在另一节且只列 3 字段、漏 `nats_conf_path`。
  MTTR 从"照单执行"退化为"读源码逆向"。

### 3.2 major

#### P3 — 无 colocated agent 的 broker 主机上 `cluster upgrade` 结构性不可完成，且 HALT 后保留升级锁 `[主]`
- **类别**：产品缺陷 · **严重度**：**major** · **gotcha**：**NEW**（与 #31 相邻但不同：#31 卡在 acquire-lock 之前，本条卡在其后的 agent leg）
- **暴露**：`30-rolling-upgrade`（**3 次复现**：并发、串行单跑、保留现场重跑）
- **一手证据 `[主]`**：保留现场（`SIM_KEEP=1`）后从 leader 侧取到 `cluster upgrade` 的真实输出：
  ```
  rolling upgrade plan → v0.0.0-simcluster-next (3 host(s) to upgrade):
    UPGRADE brk2 / UPGRADE brk3 / UPGRADE brk1 (leader — transfer to brk2 first)
  → reload brk2 into v0.0.0-simcluster-next
  → re-exec brk2's co-located agent into v0.0.0-simcluster-next
  error: cluster upgrade HALTED at brk2: agent re-exec refused: agent_no_responders
         nats: no responders available for request (the cluster is left in a safe partial state; fix and re-run to resume)
  ```
  尸检确认：三台 broker **磁盘上**都是新版本，但 `node ls --brokers` 显示
  `brk1=v0.0.0-simcluster / brk2=v0.0.0-simcluster-next / brk3=v0.0.0-simcluster` ⇒ **只升了 brk2 一台，
  集群停在混合版本**。AGENT_NID 列全是 `brkN (assumed)`——编排器**假定**每台 broker 主机都有同 nid 的同机 agent。
- **机理 `[主]`**：`internal/clusterupgrade/plan.go` 的 `AgentVer string // "" = unknown/unpaired → treated as not-at-target`
  ⇒ 无同机 agent 时 AgentVer 恒 `""` ⇒ `AtTarget` 恒 false ⇒ `cmd/tether/cluster_upgrade_drive.go:82` 的
  `agentVersionOf` 返回 ok=false ⇒ **无条件**发 `reexec-agent`（`:86`）⇒
  `internal/broker/cluster_upgrade_trigger.go:122-125` 在 `ColocatedAgentNID == ""` 时**回落到 b.selfID**
  ⇒ 向一个不存在的 agent 发请求 ⇒ `:133` 返回 `agent_no_responders` ⇒ `:91` `haltUpgrade`。
  **锁**：`releaseUpgradeLock` 只在干净完成时调用（drive 顶部注释明写 "a HALT/cancel deliberately LEAVES it held"），
  唯一自愈路径在 `plan.Upgrades()==0` 内，而 AgentVer 恒 "" 使其**永不可达**；`upgradeActive` 被
  `cluster_operation_controller.go` 与 `cluster_grow_trigger.go:73` 用于拒绝成员变更 ⇒ **join/retire 全被阻塞**。
- **运维后果**：`serve` 的 `--colocated-agent-nid` **默认为空、明确可选**，因此"专用 broker 机、不跑 agent"
  是合法且常见的生产形态（也正是本项目现网 racknerd/pc732 的形态）。在这种部署上，文档承诺的
  **一条命令滚动升级完全不可用**：升一台就停，集群留在混合版本，且升级锁不释放导致后续 grow/retire 全被拒。
- **收窄（对抗核验采纳）**：并非绝对无解——在 broker 主机上以正确 SID 起一个 agent 再重跑即可走完并释放锁。
  缺陷在于**这条逃生路径未文档化，且与"colocated agent 可选"的设计承诺直接冲突**。
- **动作**：见 §0.3 P0-4；文档补 agentless 部署下的 upgrade 语义。

#### P4 — `recovery restore` 剪到单 voter 却不去集群化 nats.conf，照它自己印的 NEXT 做必 crash-loop `[专]`
- **gotcha**：**#64（live-confirmed）** · **暴露**：`50-backup-restore` 臂 K · **证据**：`50.log:66`，实测 ~4 轮 crash-loop
- **机理**：`cmd/tether/cluster_backup.go:115-119` 完成文案仅 `NEXT: start tether-broker, then 'cluster join approve'`；
  `internal/clusteroffline/restore.go:159` 剪 roster 到 self、`:317` 归零 applied/audit 游标；nats.conf 不在 restore 写入面内（与 #51 同根）。
  **产品在崩溃时刻自己会印出缺的那一步**（`internal/broker/clusterstatus.go:354` 的 de-cluster remedy）——它完全知道该说什么，只是说晚了。
- **运维后果**：lib 卷丢失后的单节点恢复（**比全灭常见得多**）照文档做必进 crash-loop。drill 里 ~73s 自愈**只因幸存 peer 存在**；
  真实全灭 DR 无此 peer，届时是**永久 crash-loop**。drill 已如实记录该机理而非假称 reconciler 收敛——这一点工艺正确。

#### P5 — `cluster doctor --offline --db <不存在路径>` 报 0 fatal 并 exit 0 `[专]`
- **gotcha**：**#50（live-confirmed）** · **暴露**：`50-backup-restore` 臂 R3（R-EXHAUST 四态，非空绿）
- **机理**：`internal/clusteroffline/doctor.go:81-87` 用 `storage.OpenReadOnly`，而 `internal/storage/storage.go:105-111`
  是裸 `sql.Open` —— `database/sql` 的 Open **惰性、从不建连、从不 Ping**。同轮对照 `--conf <nonexistent>` **确实**报 FATAL
  ⇒ 排除"doctor 整体失效"，问题特定于 db 那格。
- **运维后果**：doctor 是**不可逆** restore 前的唯一预检动词，其绿灯对"迁移源根本不存在"完全失明。

#### P6 — account.nk 轮换后无任何动词能看见 issuer skew；`reconcile nats --all --wait` 报 false all-clear `[专]`
- **gotcha**：**#54（两 facet 均 live-confirmed）** · **暴露**：`52-credential-rotation` 臂 B0/B2/B3
- **机理**：`cmd/tether/cluster_reconcile.go:78-79` 自陈 `It NEVER bumps a generation … just polls`；
  `cmd/tether/serve.go:203-218` 的 auth_callout seeds **只在 serve 启动时加载一次**；唯一能打印该 skew 的
  `cluster_secrets.go:46-47` 只挂在 init 与 rotation guide 上，`doctor.go:70-88` 的 check 列表**不含它**。
- **严重度上调依据（本报告采纳核验者意见）**：`docs/cluster-runbook.md:243-248` §2.1 **逐字**把 reconcile 指定为
  轮换的 re-render 动词。这使 #54 从"一个动词报错绿灯"升格为**文档化的凭据轮换流程整条走不通**——
  旧 account.nk 一删，下次 broker 重启即全集群认证失败且无回滚材料。

#### P7 — PIN CONNECT 无 per-IP 限速，architecture §E.6 承诺未实现 `[专]`
- **gotcha**：**#25（本轮再次实证）** · **暴露**：`80-session-isolation` R 臂三点判别
- **证据**：`80.log:39/40/41` —— 10 次同源错误 PIN 全部被处理（各发 `pin_failed`），**第 11 次同源正确 PIN 仍成功**。三臂正控排除"被前置拦截"。
- **机理**：`internal/authcallout/` 下 rate-limit/token-bucket/per-IP 相关 grep **零命中**；`client_info.host` 可得但从不读。
- **风险叠加**：与 DOC-6（evict ≠ revocation，只删 provisioning 行、不封 nkey）叠加时升高——重入门槛就是这把无限速的 PIN。
- **⚠ 呈现问题**：因 H4c，本缺陷在汇总里显示为 **GREEN**（`product_red=0`）。

#### P9 — install.sh 未加引号 heredoc 使正文反引号被以 root 做命令替换 `[主]`
- **类别**：产品缺陷 · **严重度**：**major** · **gotcha**：**NEW**
- **一手证据 `[主]`**：`scripts/install.sh:707` `cat > "$sysd/nats-server.service" <<EOF`（**未加引号**），
  正文 `:717` 含 `` # G4 §B / #23: Restart=always … The `cluster add` grow cutover … ``。
  实测日志中每台 broker 安装各打印一行 `/opt/sim/install.sh: 1: cluster: not found`
  （6 个未抑制 provisioning 输出的 drill 各 3 次 ⇒ 非环境噪声）。
- **三重后果**：① 每台 broker 安装打印伪错误（运维手册无此输出、会淹没真错误）；
  ② 写出的 unit 注释里 `cluster add` 二词被替换成空，G4 §B/#23 的 Restart=always 依据在实机上读不到；
  ③ **结构性注入面**：install.sh 以 root 运行，若目标机存在名为 `cluster` 的可执行文件，安装时会**以 root 真跑 `cluster add`**。
- **未泛化的第二面**：全文 11 处 heredoc **只有一处**（`:429`）用了加引号的 `<<'EOF'`，其余（agent.yaml / broker.yaml /
  Caddyfile / nats.conf / 两个 unit …）**全部未加引号**，正文一律做参数扩展，且无任何断言保证那些 `$…` 都是**有意**展开的。
- **动作**：不需展开的 heredoc 一律改 `<<'EOF'`；需展开的逐个审计 `$` 与反引号；`32-install-lifecycle`
  补一条产物**内容**级断言（现有 zero-write manifest 是自比对，对本类缺陷结构性失明）。

#### P10 — 非-leader home broker 崩溃后 tier-B transfer 的 orphan object 永不回收 `[专]`
- **gotcha**：**#58（live-confirmed）** · **暴露**：`96-mid-flight-chaos` transfer 臂（`96.log:25`，OBJ_xfer 计数停在 2，干净基线 1）
- **机理**：非-leader ⇒ `reaperMayDelete()==false`（`transfer_reconcile.go:34-36` / `clusterwrite.go:478-486`）；
  boot reconciler 仅 `broker.go:942` 启动时跑一次、无周期 pass。
- **运维后果**：orphan 累积顶满 8 GiB/session 桶——**与 racknerd 现网事故同型**（memory `project_racknerd_forcesingle_js_incident`）。

### 3.3 minor

| # | 缺陷 | gotcha | 要点 |
|---|---|---|---|
| P8 | `node ls --brokers --json` 输出 **PascalCase 裸字段名**，顶层键是 `brokers` 不是 `nodes` | **NEW** | `cmd/tether/node_versions.go:25-33` 的 `brokerVersionRow` **7 个字段全无 json tag**；全 CLI 唯一键名风格断裂的 JSON 面，而 `docs/cluster.md:290` 把它宣传为 G5 #19 whole-host 判据的单一可信来源。**空值静默是最坏失败模式**——jq 不报错、退出 0、返回空串 ⇒ oracle 恒真或恒假（本轮就发生了，见 H1）`[主]` |
| P11 | `rotate-tunnel-cert` 的 follower 侧 leader-redirect 对 self-only 动词给出**误导指引** | #56（**须收窄**） | 真实要求在 `clusterdrain.go` 的 `RotateTunnelCert` 里已完整表述并给出可执行出路（transfer leadership 后再做），**不存在死锁**。缺陷是 `clusterstatus.go:649-657` 的**通用** mutating-verb redirect 对该动词给了错指引。**台账 #56 与 drill 文案里的"死循环"措辞须订正** |
| P12 | tunnel-cert pin-mismatch 砖化态下产品建议的补救命令**结构上不可达** | DOC-23 | `wireClusterEarly` 在 `broker.go:691` 返错即退，admin socket 到 `:1060` 才建 ⇒ 该态下走 admin socket 的命令必 dial 失败。唯一出路是手工恢复旧 cert 文件（drill 已证有效），产品从不提 |

### 3.4 根因族：状态收敛依赖一次性事件，而周期对账不覆盖这些面 `[主]`

> **为什么单列**：下面几条在 §3 里是各自独立的条目，但它们**共享同一个结构性成因**。
> 只按条目逐个打补丁（比如给 drain 加一个特判），同族的其它条目会继续存在。

**成因**：tether 的状态收敛靠**一次性事件**驱动——agent 重连（re-register 回包）、broker 启动、命令的成功路径。
仓库里**确实存在**周期性对账循环（`internal/broker/broker.go:1096` 的 `ReconcileInterval` ticker、
`topology_reconcile.go:65`、`alert_reconcile.go:96`、`audit_publisher.go:120`），
但 `broker.go:1102-1125` 的主循环**只覆盖 node liveness 状态迁移 + OFFLINE 节点的端口回收（leader-only）**。
下列状态面**全部不在任何周期对账的覆盖范围内**，因此一旦那个一次性事件没发生，状态就**永久停在中间态，且调用方拿到 rc=0**。

| 条目 | 依赖的一次性事件 | 事件没发生时 | 周期对账是否覆盖 |
|---|---|---|---|
| **P1** `cluster drain` 后 expose 数据面不跟随 | agent **重连**（`home.go:121` `homeForRegister`，注释自陈 "on the next reconnect"） | drain 只做一次 raft 写、**不断开任何 agent 连接**（`clusterdrain.go:151-152` `if !retire { return nil }`）⇒ 新 home 永不投递 | **否** |
| **#48** agent 黏在已退役 broker 的 NATS 孤岛 | agent 从**当前 broker** 收到 signed roster 说 leaving/removed | 已退出 mesh 的旧 broker 保持着本地 client 连接、继续供着 stale VOTER roster ⇒ 无重连、无 roster 更新 | **否** |
| **#58** 非-leader 上 orphan xfer object 永不回收 | **broker 启动**（`broker.go:942` 调一次 `reconcileXferObjectsOnBoot`） | 首门 `if !b.reaperMayDelete() { return }` 在 cluster 模式非-leader 恒为 false ⇒ 启动那一次也被跳过，此后再无第二次 | **否**（无周期 pass，且 leader-only） |
| **#31** grow lock 泄漏 | `cluster add` 成功路径末尾的 **best-effort** `releaseGrowLock` | 释放失败不重试、不告警，`cluster_grow_active` 永久残留 ⇒ 阻塞后续 grow/retire/upgrade | **否** |
| **P3** upgrade HALT 后升级锁不释放 | `releaseUpgradeLock` **只在干净完成时**调用 | HALT 后锁保持；唯一自愈路径在 `plan.Upgrades()==0` 内，而 agentless 主机上 `AtTarget` 恒 false ⇒ 该路径**永不可达** | **否** |
| **#45** retire op 停滞在 `NATS_ROLLED_OUT` | rehome/migrate 完成后推进 op 状态机 | rehome/migrate 未完成即停滞、永不到 terminal RETIRED，下一个 retire 被 `already in flight` 拒 | **否** |

**共同的失败形态**：控制面把意图写进了 raft（home 改了、roster 改了、锁置位了），**数据面/清理面永远不执行**，
而 CLI 返回 **rc=0**。运维据此继续下一步（retire、关机、再 grow），故障在更晚、更远的地方以别的面目出现。

**统一修复方向（比逐条打补丁更根本）**：把这些状态面纳入**已经存在**的周期对账循环，或给 home/roster
变更加一条**不依赖 agent 重连**的主动投递通道（`internal/broker/proxy_reconcile.go:360` 已有 push 形态可复用）。
`broker.go:1096` 那个 ticker 是现成的挂载点——这使 P1 的修复成本远低于"新建一套推送机制"。

**明确排除（避免叙事驱动的过度归并）**：
- **#34（auto-rebalance-on-return 不发火）不属于本族**。台账 R7 订正的实测记录显示，C-auto 窗口时刻 leader
  的 `cluster ops ls` 有非终态 in-flight op（`brk3 join in_progress`），auto 的 fire-gate
  （`proxy_auto_rebalance.go:57`）据此**正确 DEFER**——这是门在按设计工作，不是自动化失灵。#34 已证的是
  **分布漂移不稳定**（1/1/1 → brk1=3/brk2=0/brk3=0），机制本身尚未独立归因。把它并入本族会犯本报告
  §4.2 H10 批评的同一类错误。
- 本族**不包含**非计划故障（进程崩溃、网络分区、agent 掉线）——那些路径本来就会产生重连或 leadership
  变更事件，本轮实测中收敛正常。**断层专属于"计划内的运维动作"**（drain / retire / upgrade / grow），
  恰恰是这些操作不产生任何重连事件。

---

## 4. 测试自身的缺陷（决定本轮结果的可信度）

> **为什么这一节和产品缺陷同等重要**：CLAUDE.md §5 铁律②要求"有问题就暴露、绝不替 tether 弥补"。
> 但**反方向同样有害**——把 harness 自己的 bug 记成产品缺陷，或让缺口悄悄不计数，会污染发布判定的地面真相。
> 本轮 5 个 ASSERT-FAIL 里有 **3 个**（74、96 的一半、93 的诊断面）的**归因**站不住。

### 4.1 结构性问题（使汇总数字系统性偏乐观）

| # | 缺陷 | 严重度 | 要点 |
|---|---|---|---|
| **H1** | drill 30 的版本 oracle `_ver_of` 查了**不存在的 JSON schema** ⇒ 恒返回空串 `[主]` | **blocker** | `30.sh:49` 查 `.nodes[]?\|select(.nid==$n)\|.release`，产品实际输出 `.brokers[].NodeID/.BrokerVer`（P8）。**双重后果**：(a) `_all_on_next`(:51) **恒 false**——无论产品升没升成，该断言永不可能通过；(b) `_dryrun_no_touch`(:52) **恒 true** ⇒ `:185` 那条"dry-run 没碰任何主机"的 PASS 是**永久空绿**，且在历次 MECH=0 运行里同样每次都跑 ⇒ **长期空绿，不止本次**。对照：同批作者在 `32-install-lifecycle.sh:181` 用对了 schema ⇒ 笔误级 bug。**为何拖到今天才炸**：#31 grow-lock 此前 3/3 次把 real roll 挡在 MECH=0 分支，MECH=1 分支本次是史上第一次真正执行 |
| **H2** | drill 51 手写的 cluster seam 只有 **3/4 字段**（还含一个无效键 `nats_route`），DR 尾段被 not_covered 短路 | **blocker** | `51.sh:325-333`。产品对不完整 seam 的 fail-closed 是**正确**的（`cutover.go:181`），但失败被 README:314 与 not_covered 文案**误标为"产品的两个 GAP 不能组合"**——而 `_dr_gap "#52-natsconf"` 分支本轮**从未执行**（ledger 输出 `undocumented=1 gaps=[#51]`）。⇒ **#52 / #53 / DR terminus 三块本轮全未测，主因是 harness 不是 tether** |
| **H4** | 覆盖记账契约被系统性旁路 | major | **24 处** `warn "…NOT-COVERED"` 不进 `not_covered` 计数器（30×3/31/62/71×4/73×3/74×10/82×2），`run-drills.sh` 的五列汇总与 `--allow-incomplete` 豁免对它们**完全失明**；`tests/lint-drills.sh:27` 的 `BATCH` 只有 16 个 drill ⇒ **未覆盖 21/37**（工具自陈豁免、提供 `--all` advisory，但不 fail-closed）；`62.sh:118` 把缺口写成 `assert_ok`（契约明写 not_covered "NOT a PASS"）；**#25/#26/#27 三条已登记缺陷用倒置 assert_ok 钉住 ⇒ 三个 drill 全落 GREEN、`product_red=0`**——`lib/assert.sh:169` 早已提供不要求特定形状的 `product_red()` 记录器，故这是**可避免的**假绿，且撞 round-4 policy「连绿 discipline is ABOLISHED」 |
| **H5** | `poll_until` **不可重入**：全局 `_pu_*` 变量被嵌套调用覆写 | major | `lib/log.sh:26-38`（POSIX sh 无 local）。**模式 A**：内层失败 ⇒ 外层第 1 次迭代即判超时并**冒用内层的 desc/timeout 报错**（`74.log:225` 现场签名）。**模式 B（危害更大）**：内层每次成功都把全局 `_pu_end` 重置 ⇒ **外层 deadline 无限延后 = 无界挂起**。嵌套源已系统枚举 8 处，其中 **`lib/tether.sh:42 wait_phase` 被广泛用于集群相位等待** ⇒ 波及 40/41/42/43 等 grow/retire 家族 |
| **H12** | 静态闸 `lint-drills.sh` 对 drill 内嵌 jq / 描述串完全失明 | major | 不做 jq 可编译性检查 ⇒ H1（坏 jq path）与 H13（jq 括号错）**两条都从闸门下溜过**；`--all` advisory 另报出 8 处 combined-signal-trap 与 3 处 sigpipe-truncation |

### 4.2 使个别 drill 结论失效的问题

| # | drill | 缺陷 | 后果 |
|---|---|---|---|
| **H3** | 30 | `_do_roll` 的 rc 被丢弃、roll.log **从不落盘**，终末 warn **硬编码**了与本次运行相反的叙事（日志 :186 说"grow lock 干净、roll 直接跑"，:191 却说"#31 阻塞了升级 (MECH=0)"） | 与 H1 叠加 ⇒ MECH=1 路径上**没有任何断言能证明升级发生过**；剩余两条 PASS 恰是该 drill 自己头注点名的假绿。**本轮 RED 在事后本不可归因**——是我用保留现场手工取到 roll.log 才定的案 |
| **H6** | 96 | D6b 的 GREEN 是**空绿**：canary3 从未写入却落进"已回滚"分支；日志把 rc=69 记成 rc=0 | **#65（分区少数派 stale-leader 写有时持久）的地面真相被污染**——本轮既不能算复现也不能算未复现 |
| **H7** | 40 | 杀 leader 后用两个**不一致**的 leader 判据，且 `sim_leader` 的 fallback **硬编码到刚被杀的 brk1** | 时序竞态触发 SETUP-RED（单跑 GREEN 佐证）；#45/#37 的 mid-retire 面本轮**未触达** |
| **H8** | 42 | fixture 的 `tether push` **漏传 `--ack-alerts`**，被 `force_single_active` 严重告警门确定性拦下（rc=70） | 该拦截是**设计如此**——`gateDestructive`（`cmd/tether/d8_alerts.go:71`）覆盖 push/pull/run/expose/session rm `[主]`。⇒ 这是 harness 缺陷，**不是**产品缺陷；rejoin/resnapshot 整段（audit-window、`--accept-audit-loss`、不复活陈旧 peer、join approve 并行）本轮**全未测** |
| **H10** | 74 | 把 harness 前置失败叙述成 `#33 族 moved-exit 搁浅、release-blocking` | 唯一失败理由行是 `ss-local SOCKS listener 未就绪`，其来源 `drills/lib/proxy.sh:51-52` 的注释**写死了** "a HARNESS/setup failure — NEVER count it as a product black-hole"。⇒ **本次对 #33 的归因不成立** |
| **H11** | 74 | Arm C 前置门：滚动重启后不 settle，`_count_on` 的 -1 fail-closed 让 **0-home broker 中选** | Arm C（auto-rebalance-on-return，G7a m11）本轮**未测**；#34 的产品侧半边（分布不稳）倒是如实再现了 |
| **H13** | 93 | 三条 RED **零诊断**：两条 webhook FAIL 只有 `poll_until: timed out after 20s`（谓词内部会 cat hook.log，但失败路径从不 dump）；CARD/JSON 是四段 `&&` 复合断言、失败无法定位；card 诊断的 `head -8` **恰好切掉**被 grep 的 `(exit N)` 行（产品打在卡片末尾） | **93 的三条确定性 RED 目前无法归因**——既可能是 webhook wire schema 真缺陷，也可能是 jq 括号错。**且 webhook 的 on-the-wire JSON 契约（schema/schema_version、transition=cleared、no-secret 键白名单）三层测试全无覆盖，而它触及安全承诺** |
| **H14** | 96 | 注释承诺的 F0c 门控**在代码里不存在**（写成普通 `assert_ok` 而非 `assert_setup`），F0c 与 F4 断言**同一谓词** | 一个根因被计成两条 ASSERT-FAIL ⇒ `assert_fail=4` 而真实互异信号为 **3** |
| **H15** | 全套 | rollup **只走 stdout、无落盘产物**，经 `ssh -t`（无 ConnectTimeout / ServerAliveInterval）投递 `[主]` | 本轮实测症状吻合：服务器端 runner 已退出、24 个 `.rc` 全部写好，**本地 ssh 仍不返回，summary 一次都没打印**——我是从 `.rc` + 日志手工重建的汇总。降级为 minor 仅因 rollup 可完全重建 |

### 4.3 harness 侧最小修复集

| # | 动作 | 关闭 |
|---|---|---|
| 1 | 30：`_ver_of` 改 `.brokers[]\|select(.NodeID==$n)\|.BrokerVer`；判 `_do_roll` 的 rc 并在失败时 dump roll.log；终末 warn 按 MECH 参数化 | H1/H3 |
| 2 | 51：G1-clear 照抄 `cluster.go:905-906` 的完整 5 字段 seam，**并加 decode 后置断言**（产品自己在 `applyClusterSeam` 里就做了这个校验） | H2 —— **一次修复解锁 #52/#53/terminus 三块覆盖** |
| 3 | 93：修两处 jq 括号；失败路径 dump hook.log；card 诊断去 `head -8`；拆四段复合断言 | H13 —— 并使 webhook wire schema **首次**获得覆盖 |
| 4 | 42 加 `--ack-alerts`；40 改 poll 重试 + `sim_leader` fallback 遍历存活节点 | H7/H8 —— 各恢复一整段核心覆盖 |
| 5 | `poll_until` 改唯一变量名或显式保存/恢复；审计 8 处嵌套点（尤其 `wait_phase`） | H5 —— 含 grow/retire 家族的潜在无界挂起 |
| 6 | lint：BATCH 默认全量；增加 jq 可编译性 / 空针 / 描述串反引号三条规则 | H4/H12 |
| 7 | 24 处 `warn "…NOT-COVERED"` 改 `not_covered`；`62:118` 改 `not_covered` 并关联 OQ-2；#25/#26/#27 旁加 `product_red`；waiver 改 per-drill 粒度 | H4 |

---

## 5. 开放问题（**不得**当作已定结论写入台账）

| # | 问题 | 若成立的严重度 | 定案实验 |
|---|---|---|---|
| **Q1** | 恢复 gen-1 account.nk + 重启双 broker 后 120s 内认证仍未恢复；drill 直接归因 #54「unrecoverable」但**零诊断证据**（全日志无 broker.err / journal / doctor 输出） | (A) 若"account.nk 换过就无法就地换回"成立 ⇒ **blocker（凭据轮换单向砖化）**；另有 (B) 纯计时、(C) C3 reconciler 重渲 conf 造成**反向 skew** 两个同样自洽的假说 | 定向复跑，**必须同时采 nats.conf 的 issuer 与 broker.err**（仅采其一无法区分 A/C）。**在此之前台账不得保留"unrecoverable"这条未测量的产品指控** |
| **Q2** | 被分区的少数派 broker 对客户端表现为**连接黑洞**（TCP 通、CONNECT 零应答、i/o timeout），而非设计中的 fail-closed 拒绝 | major | 设计意图侧已正向核实：`authcallout/handler.go:102-104` `fenced()` → `h.deny(...)`，**少数派应当明确拒绝**。候选机理最强的一个：`authcallout.go:101-104` 的回调在 `h.Handle` 出错时**直接 return 而不 Respond**。可验证预测：brk1 journal 应有 `authcallout: handle failed`。运维后果严重——错误提示写着 `verify the broker is running and --nats-url is correct`，而 broker 在跑、URL 也对，运维会去查网络而不是查 quorum |
| **Q3** | `exec` 报成功（rc=0），进程表 30s 内**从未**把该进程记为 RUNNING | 候选产品缺陷 | F 臂前置门（3 VOTER + 两 agent ONLINE，240s）**通过**，"arm-D 残留"这一现成借口已被部分排除。需带 `ps -a` 全量 + agt2 journal 复跑 |
| **Q4** | `session create` **写已提交却向调用方报失败**，且无幂等出口 | minor | **可观测事实成立**：D6 反证 canary2 已在多数派提交并可从 brk1 读回，而 D3 的 `session create canary2` 无一次返回 0。机理三候选未定（谓词 `>/dev/null 2>&1` 丢弃了 stderr 与 rc）。**产品侧可独立核验成立的部分**：create 非幂等（sid==name，已存在直接 `ErrAlreadyExists`），且 `error_hints.go` 里 **grep 不到任何 `already_exists` 条目** ⇒ 连提示都没有 |

> **方法论订正（我自己的一次误读，记录在案）**：我在初查时把 96 的 D3 失败读成"**多数派侧写不进去**"这一
> 可用性级结论。**对抗核验阶段推翻了它**——同 drill 的 D6 断言 PASS，明确证明"分区期间在多数派写下的行，
> 愈合后可从 ex-少数派 brk1 读回"，即那次写**确实提交了**。D3 的真实成因是"写成功但客户端未拿到成功
> + 非幂等重试"（Q4）。**若无这一步对抗核验，本报告会输出一条严重的假指控。**

---

## 6. 本轮实际**未**覆盖的面（不得计入发布判定）

| 面 | 未测原因 | 归属 |
|---|---|---|
| G5 滚动升级机制（版本是否真翻、dry-run 是否真未触碰主机） | H1 oracle 恒空 + H3 丢 rc | **净覆盖为零** |
| whole-host 升级判据（broker + 同机 agent 双到版） | OQ-6 供给从未实现 | 结构性未覆盖 |
| retire 中途换主后的收敛（#45 `NATS_ROLLED_OUT` / #37） | H7 提前 abort | 空白 |
| rejoin resnapshot audit-window + `--accept-audit-loss` + 不复活陈旧 peer + join approve 并行 | H8 提前 abort | 空白 |
| **全灭 DR 尾段**：#52 fresh box 无 auth_callout / #53 JetStream 不随 bundle 回来 / DR terminus | H2 seam 写不全 | **tether 的全灭 DR 至今从未被端到端证明过一次** |
| 52 D-group（C7 guided rotation 的 retire/alert 生命周期） | Q1 未定案 | 空白 |
| 74 Arm C：auto-rebalance-on-return（G7a m11） | H11 前置门失败 | 空白 |
| **webhook on-the-wire JSON 契约**（schema/schema_version、transition=cleared、no-secret 键白名单） | H13 jq 坏 + hermetic 从不读 body | **三层全无覆盖，且触及安全承诺** |
| 95-D：DELETING 会话的 boot-resume（G.2(1)b） | 谓词过严（硬钉 `leader_id=="brk1"`，而注入前 brk1 的 broker 已被两次干掉） | **很可能是假缺口**，被误记为结构性不可达 |
| 62 Arm 2：true-D + mode:off-without-safe | OQ-2（已登记）+ H4b 记成 PASS | 已登记缺口，**记账错误** |
| `ps` 的 LOST 派生态 | 94 声称覆盖、实际零断言 | **overclaim** |

---

## 7. 文档 / 台账需要订正的地方

| # | 内容 |
|---|---|
| D1 | **台账三处与本轮实测不一致**（台账是发布闸 SSOT）：#52 的状态、#54 的 facet 1 描述、#63 的 A7d 结论（本轮 re-pin **正面成立**）。#56 的"死循环"措辞须按 P11 收窄 |
| D2 | runbook **§5.2 缺 seam 步骤**（且 §4 那处只列 3 字段、漏 `nats_conf_path`）；**§5:524 备份示例路径 `/var/backups/…` 在 stock 装机上不可写**（DOC-27，本轮再次实证）；DR 章节从不告知 bundle **不含 JetStream** ⇒ history/audit 全失（DOC-19） |
| D3 | `cluster status` 的 exit code 在 voter 重启后会**抖动数秒**，文档未提示需去抖 |
| D4 | DOC-12：architecture H.1 承诺的 `kicked`/`agent_unregistered`/`rotated_pin` **三个事件无 writer**，且台账声称由 80/81 owns 的断言在两个 drill 里**零实现** |
| D5 | README drill 表：verdict 漂移（22/40/42/43/90/91/92/93 八处），以及更危险的 **scenario 列内容腐化** |
| D6 | 覆盖 overclaim：94 声称覆盖 `ps` LOST 实为零断言；webhook wire schema 三层全无覆盖却未登记 |

> **对 README 期望表漂移的定性**：README `:296-300` 的免责声明**已预告**这批结果（"historical round-3 snapshot …
> **it is not an acceptance target** … judged solely by its actual unique verdict"），因此 22/43/90/91/92 从
> INCOMPLETE 变 GREEN **不构成"期望写错"**——但表本身该刷新，否则读者每次都要先读免责声明才能用。

---

## 8. 方法与局限

- **实验先行、结论后置**：本报告的每条产品缺陷都先有实跑证据，再回源码定机理；纯源码推演的条目已标注。
- **三段式**：主进程实跑 → 6 视角专家并行分析（每 lane 固定 1 agent）→ 6 名对抗核验者（**无条件**逐 lane spawn，
  含空输入 lane）→ 1 名综合 → **主进程定稿**。对抗核验驳回/收窄了 **14 条**主张（含我自己的一条误读，见 §5 脚注），
  这些被驳回的条目连同反证一并存档在专家综合稿 §E，供复核。
- **局限**：(1) 5 个 ASSERT-FAIL 中 93 与 96-F 的**根因尚未定案**，受制于 harness 诊断缺失（H13/H14），
  已列为 §5 开放问题而非结论；(2) `-j 4` 的并发档使 40 出现一次时序伪红——已用串行单跑排除，
  但不能排除其它 drill 存在同类未被发现的时序敏感点；(3) 本轮**只测不修**，所有修复动作均为建议，
  未改动任何产品或 harness 代码。
