# 模拟集群测试补完 — 总纲 Roadmap（S1–S9）

Date: 2026-07-10（rev7；本文件后移至 `docs/`，审查史/清单附录仍在 `docs/reviews/`）。修订史：
rev2 = 61 条内审 findings 裁决（`docs/reviews/simcluster-coverage-roadmap-review.md`）；
rev3/rev4/rev5 = 外审 round1（11 findings + 4 doubts）/round2（8 findings + 3 questions）/round3
（2 Major + 1 Minor）/round4（1 Major + 2 Minor）逐轮全采纳，逐条回复见各轮
`docs/reviews/simcluster-coverage-roadmap-external-review*.md` 文件尾；rev5 把清单附录
`docs/reviews/simcluster-coverage-inventory.md` 完成全量生成，rev6 补安全门 flag 面，
**rev7**（round5：2 Major → round6 **Pass**）用构造后 Cobra 树的完整遍历系统性重建附录 §2
并修正 restore 的 never-escapable 安全模型。
Status: **ROADMAP（总纲）；S1 已落地（S0-pty + S0-台账 + 60/61/62 drills；commit `10e9a5f`，外审 3 轮 Pass）、S2–S9 未开工**。本文件**不是**单批 plan、**不**进入实现——它把「**已发布至 v0.4.7
的全部产品功能面中尚无 deploy-tier 覆盖者**（含 G 系列 plan 已认账未落地的 sim 验收欠账；simcluster
本体登场于 f460148, 2026-07-05）」的模拟集群测试补完，按内聚场景族 + 依赖顺序 + 使用频率拆成
**9 个独立叶子批次 S1–S9**。每批开工时各自按 CLAUDE.md §3 走 3 阶段 7 步（Workflow 对抗草拟 →
主进程定稿 `docs/reviews/s<N>-plan.md` → 实现 → 对抗内审 → 外审 → commit），彼此不阻塞主线。
**范围以本文件为总纲，精度以各批 plan 为准。**

> 使命一句话：**像一个真实团队那样去「用」和「运维」一个真实部署的 tether 集群**——把每一条
> 使用者旅程、每一条运维剧本在真 systemd + 真独立 nats-server + 真 install.sh 路径上原样走一遍，
> 让 tether 在真实部署下的一切缺陷露出来。**不是让操作「跑通」，而是让问题「暴露」**
> （`test/simcluster/README.md` Mandate ①–④，逐字约束本 roadmap 的每一个 drill）。
>
> 功能面真相源：`docs/usage.md`（使用者手册）、`docs/broker-ops.md`（broker 运维）、
> `docs/cluster.md` + `docs/cluster-runbook.md`（集群命令 + 运维剧本）、
> `docs/architecture.md` Part II（P0–P13 + post-1.0 叶子登记）——**外加源码级命令树与事件清单
> 对照**（`docs/reviews/simcluster-coverage-inventory.md`，单一真相源附录；cobra 的
> `Hidden: true` 命令在 `--help` 里**不可见**，故文本 help 不得作为清单来源——见 §4 闸门规则；
> 已知手册即漏 `recovery diagnose`/`resnapshot`，登记为 DOC 缺陷，§5）。
> 既有缺陷台账：`docs/reviews/v0.4.5-ha-grow-ops-gotchas.md`（#1–#24，SSOT）；本工程新发现从 **#25** 起
> 记入新台账 `docs/deploy-tier-gotchas.md`（见 §5）。

---

## 0. 目标与边界（先读）

### 0.1 什么算「补完」

simcluster 目前只覆盖它的诞生动机——**HA grow/force-single/deploy 缺陷族**（7 个 drill，全部
围绕集群 membership 与 G1–G7 修复回归）。但 tether 是一个完整产品：P0–P11 主线 + 十余个
post-1.0 叶子（push/pull、expose `--remote-port`、proxy 订阅、remote-fs-resilience、
cli-failover…）+ B1–B7 易用性 + C1–C8 自动化 + D0–D9 分布式 + G1–G7 整治。**这些面在
deploy-tier 一片空白**：没有任何 drill 在真实部署栈上跑过一次 `tether run`、一次 expose 真流量、
一次 node upgrade、一次备份恢复。补完 = 把 §4 总表里的每一行功能/流程要么归入某批 drill、要么
显式登记 NOT-COVERED（附理由），**一行不漏**。「功能面」**显式包含 operator-facing 的
`sys.events` 事件族与持久告警**（架构把它们当运维契约；清单见 §4.6），不止命令与 flag。

### 0.2 只测不修（本工程的硬边界）

- **S 系列只交付 drill + 暴露缺陷，不交付产品修复。** 发现 tether 缺陷 → 登记 gotcha（#25+）→
  用 `assert_bug` 签名钉成 RED（或 `GREW-…` 式 trailer 标注）→ 修复**另立独立叶子增量**
  （如 G 系列先例），不混入 S 批。这保证每批外审面只有「测试是否忠实」一个维度。
- 例外：**harness 自身的 bug/保真度债**（sim 脚手架）随批修——它们不是产品代码。**反掩盖细则**
  （防「以 harness 债名义把 tether 需要的 workaround 洗进环境」）：任何归类为 harness bug 的
  修复若为环境**新增了真实生产供给之外的行为**，必须在 drill 头注 / 批内审 checklist 里给出
  Mandate ④ 说明（为何不是替 tether 省事）；给不出即改走 (a) 路登 gotcha。
- 产品**文档**缺陷（如 usage §5.15 尾段「cluster 不支持 proxy」与 C5 现实相悖的残留旧文）同样
  只登记不顺手改；批量修文档是独立小增量。

### 0.3 与 hermetic 两层的关系：深度闸门（防 scope 爆炸的第一道闸）

`make test`（hermetic 单测/包测）+ `make e2e`（每 phase 子进程矩阵）+ `dN_integration` 已经把
**纯逻辑面**（flag 解析、错误码映射、状态机转移、wire 编解码、ACL 模板…）覆盖得很密。simcluster
的**唯一增量**是 hermetic 结构上够不到的部署维度。故立此闸门——**每个候选断言必须回答
「部署层新增了什么信息」**，答不出的不进 drill。部署层敏感面清单（drill 断言应密集于此）：

1. **权限/属主/文件系统**：root vs `User=tether` vs 普通用户的真实写权限；install.sh 铺出的真实
   目录树；state.json/agent.yaml/broker.yaml 在真实路径上的持久性（#22 类）。**Caveat：权限/属主
   类断言必须镜像 install.sh 的真实布局**（如 agent 角色装在用户可写 `~/.local/bin`）——烘焙布局
   偏离真实（如共用 root-owned `/usr/local/bin`）会在两个方向失真：制造真实部署不存在的假墙
   （false-RED），也可能跳过真实存在的墙（false-GREEN）。
2. **systemd 行为**：unit Restart 语义、clean-exit 陷阱、`syscall.Exec` 原地重启后 PID/unit 状态、
   enable/--now 时序（#23 类）。
3. **真独立 nats-server**：auth_callout 在真 JWT 链上的放行/拒绝、nats.conf 渲染与 `-t` 校验、
   SIGHUP vs restart、clustered JS meta 的真实形成/降级/503（#20/#24 类）。
4. **跨进程/跨容器网络**：真 yamux 隧道字节流、真公网带监听、真 route mTLS、连接断开与重连的
   真实时序（非注入的 fake transport）。
5. **多进程编排时序**：daemon 冷启动顺序、操作跨越 broker 重启、`--wait`/dwell/watchdog 在真实
   延迟下的行为。
6. **真实故障注入**：docker kill（整机断电级）、进程 kill -9、**网络分区（静默丢包 ≠ 连接拒绝）**、
   磁盘打满、二进制换版本——以及之后的**自愈是否真的发生**。
7. **完整旅程的组合效应**：单命令 hermetic 全绿、串成运维剧本却卡死（gotchas #1–#24 几乎全是
   这一类——组合、顺序、跨机才暴露）。

### 0.4 Drill 工艺：journey 式 + 探索→定格

- **journey 式**：一个 drill = 一条完整的真实使用/运维旅程（一次组建拓扑，串 10–20 个断言），
  不是每 flag 一个 drill——既贴合「模拟真实使用」的使命，又摊薄 2–10 min 的拓扑组建成本。
  现有 7 个 drill 全部是这个形态，沿用。
- **探索→定格**：新面 drill 先按**产品文档承诺**写成 GREEN 期望 → 上真 server 跑 → 通过 = GREEN
  回归入库；失败 = 分诊——(a) 产品缺陷 → gotcha #25+ 登记 + `assert_bug` 签名钉 RED；(b) harness
  bug → 随批修（受 §0.2 反掩盖细则约束）；(c) 环境不可行（如内核依赖）→ 显式 NOT-COVERED +
  理由。**永远不许**第四种出路「改环境迎合 tether 让它过」（Mandate ①）。个别面**依据代码证据可
  预判为 (a)**（如 S5-31 的 agent 侧升级白名单，见该批），此时 drill 直接按「预期落 gotcha」设计，
  但仍须真跑定格、不许凭静态阅读跳过。
- **断言纪律**沿用 `lib/assert.sh` 三原语 + `drill_begin` 的 throwaway-instance 安全门 +
  每个 RED 必须 signature-guarded（只为文档化原因通过）+ 每个 drill 在头注写明 false-green 风险
  （它可能因什么错误原因而绿）。**阳性/阴性对照纪律**：破坏性/恢复性断言先建立「注入前基线」
  （先证明要破坏的东西活着/要恢复的内容存在），负例臂与正例臂成对（先例：
  `drills/lib/setup-forcesingle.sh` 的 JS-meta-formed 前证）。**破坏性臂五要素**（每条破坏性/
  混沌臂在 plan 里必须写全）：① 命名的注入前数据/连接基线；② 权威观测源（读哪里、为什么可信）；
  ③ 精确的注入边界（对什么、在哪个状态点）；④ 注入后语义 oracle（断什么结果，非退出码）；
  ⑤ 无条件清理（trap，故障规则/后台客户端必回收）。**安全限流/撤销类断言另加一条**：必须带
  「来自不同 actor/源的成功对照」——「全部请求都失败」永远不能作为限流/撤销机制存在的证据
  （被限者失败 + 对照源成功 + 窗口过后恢复，三点齐才算证明）。
- **拓扑最小化**：断言不涉及集群语义的旅程用 N=1（组建 ~2 min）；涉及 rehome/quorum/滚动的才
  用 N=2/3。每 drill 独立 `--instance` 隔离；全套并发的真实天花板见 §1.1 与 OQ-8。

---

## 1. 现状盘点

### 1.1 harness 既有能力（够 S 系列直接复用的）

`simcluster` 19 verbs（up/init/grow[真 `cluster add`]/force-single[online]/session/agent-join/
ctl/exec/logs/nats-conf/status --json[leader 权威视图]/doctor/drill/down/nuke…）、
`lib/{log,docker,tether,secrets,assert}.sh`（poll_until 反固定 sleep、SAN 证书铸造、RED/GREEN
harness）、`image/pty-confirm.py`（typed-confirm 喂 TTY 的先例）、`run-drills.sh`（inotify
preflight + 并发 + infra-flake 重跑，drills/*.sh 自动发现——新 drill 落盘即入套）、
`--cap-store`（tmpfs 小盘）、真实 sshd 调试通道、`doctor`（#22/#23/#21 drift 检查）。

**容量现实**（校准，勿乐观引用）：inotify 修复后容器并发容量 ~600（README 实测），但**并发
grow 的 timing flake 是另一道独立天花板**——7 drill 全并发同时 grow 已可让 VOTER promotion 错过
150s 窗口（README「CAVEAT (grow-concurrency)」+ `cmd_grow` DO-NOT-RE-INVESTIGATE 注）。S 系列
新增 ~19 个组 N≥2 集群的 drill 后并发 grow 数约 ×3，**flake 面必然扩大**：每批收尾的 drill-all
从一开始就应按族分波 / `-j` 限并发跑（非 S9 才考虑），见 OQ-8。

### 1.2 既有 7 drill 已覆盖（S 系列勿重复）

| drill | 已覆盖 |
|---|---|
| `00-skeleton` | N=1 cutover + agent-join(--pin 路径) + node ls + push/pull 两档 round-trip |
| `10-grow-to-3` | N=1→2→3 真 grow、3 VOTER、R=3、JS meta、transfer-leader --wait、follower-kill 下读+写（session create）存活 |
| `11-grow-gaps` | `cluster add` 编排回归（workaround 全清）+ #I1 serve fail-closed 不变量 + #4 move-not-delete |
| `12/20` | G2 force-single（OFFLINE 路径）auto-de-cluster + auto-prune 回归、N=1 tier-B 恢复 |
| `13` | G1 #22 nats.d/ 属主与 in-broker reconciler 写权 |
| `21` | G6 盘感知 OBJ_xfer（4g tmpfs tier-B 通） |

### 1.3 已认账未落地的 G 系列 sim 验收欠账（归属各批收债）

G3/G5/G7 的 plan 都承诺了 sim drill 作为验收出口，内审明确记账「owed per plan」，但
`drills/` 至今只有上表 7 个：

- **G5**：`drills/30-rolling-upgrade.sh`（`g5-plan.md` §3.3 已写规格；`g5-review.md` #8 记账）→ 归 **S5**。
- **G7a**：`NN-g7a-rebalance-return.sh`（`g7-plan.md:133`；`g7-review.md` M2/m11 记「absent」）→ 归 **S4**。
- **G7b**：`NN-g7b-js503-alert.sh`（`g7-plan.md:134`）→ 归 **S8**。**注**：plan 原配方「复用
  drill 20 造 JS-503」已被 G2 修复淘汰（20 现为 OFFLINE auto-de-cluster GREEN、不再 503）——
  以本 roadmap 92 号规格为准（两 leg 拆分，见 S8）。
- **G3**：`NN-g3-client-converge.sh`（`g3-plan.md:236`，规格为 A/B/C/D 四臂：A=grow 收敛、
  B=online force-single 后 seeds 收敛仅 survivor、C=offline force-single prune drop-only、
  D=ctl 经 survivor 刷新）→ A/C/D 归 **S8-91**；B 臂骑 92 的 force-single leg（软依赖 S6-22
  结论，不可达成则如实登记）。

### 1.4 harness 保真度债与覆盖债（S 系列随批清偿/定格）

**保真度债**（sim 偏离真实部署，随批修，受 §0.2 反掩盖细则约束）：

- **agent 入群路径**：`provision-node.sh` 注释宣称 "Agents onboard via the real product path:
  `tether agent join <invite>`"，实际 `cmd_agent_join` 走的是 `--pin` 首连 + sim 手写 system
  unit。`--pin` 首连本身**也是**产品一等路径（usage §5.8），保留；但 C2 invite 入群
  （`agent join`/`config refresh`/`agent doctor`/`seeds publish`）从未演练 → S2 补 + 修注释漂移。
- **agent 反向隧道未接线**：sim 的 agent unit（烘焙与 `cmd_agent_join` 手写两处）均不带
  `--tunnel-addr`、也不写 agent.yaml——`tunnel_addr` 停在默认 `127.0.0.1:7000` 在 agent 容器本地
  空拨，**expose 数据面必死**；而真实 install.sh 本就把 `tunnel_addr` 写进 agent.yaml。→
  **S0-隧道**（§2 公共前置包；S2-81/S4 全数据面/S6-40/S7-50/51/S9 均依赖，非 S3 私有——由
  首开需要它的批落地，按 install.sh 形态写 agent.yaml，Mandate ③ 正当供给）。
- **agent 二进制布局失真**：sim 全角色共用烘焙的 root-owned `/usr/local/bin/tether`，而真实
  install.sh agent 角色装在**用户可写** `~/.local/bin`——`node upgrade` 的原子替换在 sim 布局下
  会撞一堵真实部署不存在的假墙。→ S5 harness 增量把 agent 布局对齐 install.sh 真实形态。
- **README 漂移**：README drill 表仍把 `11-grow-gaps` 描述为 RED（#8/#I1 + `GREW-VIA-WORKAROUNDS`
  trailer），盘上 drill 11 已是 G4 反转后的 GREEN（trailer 为 `GREW-VIA-TETHER-CLUSTER-ADD`）
  → S1 README 重构显式清偿。

**覆盖债（分诊未定，探索→定格）**：

- **online force-single**：drill 12/20 因 dwell 被 survivor 的 `Restart=always` bounce（peer 死后
  发生）反复重置而改走 OFFLINE 路径——ONLINE 路径（cluster.md 标注「首选」）在 deploy-tier 无
  覆盖。**注意 sim 的 broker 跑的就是真 install.sh 产品 unit**：bounce 若在真实产品 unit 下持续
  不止，即「dwell 结构性不可达成」的**产品缺陷**而非环境特异。S6-22 按顺序约束分诊（先判 bounce
  的生产可达性，再谈驯服），两种出口（可驱动 / 定格 gotcha）同权预期。见 OQ-4。

**基础设施注记**：`fs.inotify.max_user_instances=8192` 已调（8752afe）；S 系列新增容器角色
（artifact server、webhook 接收器）沿用同一 instance 命名/网络约定。

---

## 2. 分批原则与依赖图

三条拆批依据（仿 G-roadmap 的三准则结构）：**内聚场景族**（一批 = 一族旅程，外审能一次看懂）、
**harness 增量就近**（需要新基础设施的批自带它）、**使用频率×风险排序**（最常用的用户面先行，
深水区殿后）。

### S0 — 公共保真前置包（非批次、无独立 drill，跨批共享的 harness 增量注册表）

多个批共用的保真前置**集中登记于此**。**唯一归属规则**：每批开工时落地「本批所需且尚未落地」
的 S0 项（不多不少——不吸收无关项）；**整个工程的首开批**（无论按序还是重排）额外落地
S0-台账（全批共用底座）。每项落地受 §0.2 反掩盖细则约束（Mandate ④ 说明入 plan）；落地后在
本表「状态」列登记（落地批 + commit），后续批的 plan 据此判定「已就绪」而非重复实现。
**每项在其落地批的 plan 里必须写全生命周期元组**：归属批 / 消费批 / 实例作用域命名 /
创建预检（fresh/权限）/ 密钥与信任材料 / 健康检查 / 最终清理路径。

| # | 前置 | 内容（含生命周期要点） | 默认落地批 | 依赖它的批 | 状态 |
|---|---|---|---|---|---|
| S0-隧道 | agent 反向隧道接线 | 按 install.sh 形态把 `tunnel_addr: <broker>:7000` 写进 agent.yaml（§1.4 债） | S3→**S2**（唯一归属规则，早开落地） | **S3-70/71**、S2-81、S4 全数据面、S6-40、S7-50/51、S9-94/95/96 | **已落地（S2；agentyaml.sh 已写 tunnel_addr + provision-node.sh `--domain <node>` 使 public_host 可解析；活体实测 expose 数据面跨容器 curl 通。T0-T6 折入 81 setup 作回归门）** |
| S0-ingress | 公网 ingress 反代替身（**同 netns + 测试 TLS**） | 产品两 listener **loopback-only 系有意安全边界**（`subhttp.go:34-46`、`clustermanifest/manifest.go:19-33` fail-closed——**永不改绑，也不设 `TETHER_DEV_NO_AUTH` 削弱**）。普通 bridge 容器**到不了他容器的 127.0.0.1**（Docker 需共享网络栈），且 invite 拒明文 bootstrap URL、`/sub` URL 默认 https——故替身 = **每 broker 一个共享其 netns 的 HTTPS 反代**（sidecar `--network container:<brk>` 或 broker 容器内进程，terminate TLS→loopback，与生产 Caddy 同宿主同拓扑）；**实例作用域测试 CA**（与 S0-artifact 共用铸造设施）+ SAN=该 broker 主机名；CA 注入 ctl/agent trust store（部署职责）；**正向证书校验断言 + 错误/不受信证书负例**；N=1/N=3 下按 `https://<brkN>` 寻址；随 instance 拆除 | S4→**S2**（唯一归属规则；**S2=实例-CA facility owner**） | S4-72/73、S2-82（manifest 跨容器腿） | **已落地（S2；`image/ingress-proxy.py` python-stdlib TLS 反代 sidecar〔`--network container:<brk>`〕+ `drills/lib/ingress.sh`〔secrets_mint_ingress/ingress_enable_manifest/ingress_up/ingress_trust_inject〕；复用 lib/secrets.sh 实例 CA；活体实测 agent 真 Go-x509 fetch 经反代验签通、wrong-SAN/未信任-CA 负例拒）** |
| S0-布局 | agent 二进制布局对齐 | install.sh 真实形态：agent 装用户可写 `~/.local/bin`（§1.4 债） | S5 | S5-31 | 未落地 |
| S0-artifact | 发布制品库 | https 静态服务 + 实例作用域自签 CA 入容器 trust（OQ-3；CA 设施与 S0-ingress 共用） | S5 | S5-30/31 | 未落地 |
| S0-备份库 | 离簇备份库 | **实例命名空间化的 host 侧目录**：在 `rm_node --vols`（灾难注入）作用域**之外**、但在 `simcluster nuke` 作用域**之内**（nuke 学会删它——不跨轮泄漏状态）；fresh/空预检、0700、bundle 名唯一、trap 清理 | S7 | S7-50/51 | 未落地 |
| S0-故障原语 | 故障注入原语 | 目标 netns 内 nftables/iptables DROP / tc（分区）、SIGSTOP、卷级灾难（保留-secrets 变体）——每件带 Mandate ④ 说明模板 + 无条件清理 trap | S9（分区）/S7（卷级） | S9-96/97、S7-50/51 | 未落地 |
| S0-pty | 通用交互 pty 驱动 | `image/pty-run.py`（交互式 `run` 会话驱动；**typed-confirm 臂用既有 `pty-confirm.py`，不依赖本项**） | S1 | S1-60、S9-96（PTY 混沌臂） | **已落地（S1；`image/pty-run.py` 烘焙进镜像；commit `10e9a5f`）** |
| S0-台账 | 台账 + README 底座 | `docs/deploy-tier-gotchas.md` 建档 + README drill 表/编号族重构（含 11 号行漂移清偿）+ 清单附录（`simcluster-coverage-inventory.md`，rev4 建档、rev5 全量生成）维护权移交 | S1（或首开批） | 全部批 | **已落地（S1；`docs/deploy-tier-gotchas.md` 建档 + README 编号族/drill 表重构（含 11 号行漂移清偿）+ 提交入仓命令树 golden gate `cmd/tether/command_tree_inventory_test.go`；commit `10e9a5f`）** |

```
S1 用户平面核心旅程 (exec/run/ps/history/node ls/transfer 边界)      ── 无依赖；落 S0-pty/S0-台账
S2 session·多租户安全·admin·agent 入群 (C1/C2)                      ── 81 需 S0-隧道；82 manifest 跨容器腿需 S0-ingress
S3 expose 数据面 + rehome/failover (P6/P12/B4/C6/D6)                ── 落 S0-隧道（默认首个数据面批）
S4 proxy 订阅面 (P13/C5) + G7a 债                                    ── 需 S0-隧道；落 S0-ingress；OQ-1 选型
S5 升级与版本运维 (node upgrade/G5 债 cluster upgrade/install 生命周期) ── 落 S0-artifact/S0-布局 + staged 二进制
S6 拓扑收缩与回归 (drain/retire/shrink/rejoin/带数据割接/online force-single) ── 40 的 expose fixture 需 S0-隧道
S7 备份·灾备·凭据轮换 (backup/restore/DR/rotation/C7)               ── 需 S0-隧道；落 S0-备份库；软依赖 S6（fixture 经验）
S8 客户端视图与可观测告警 (G3 债/G7b 债/alert/metrics/--remote)      ── 92 的 #20③ 专属 leg 软依赖 S6-22 结论（其余 leg 独立）
S9 混沌对账与长稳 (G.1/G.2/#23 行为级/分区/中断注入/soak)            ── 需 S0-隧道；落 S0-故障原语；最后
```

**推荐推进顺序 = S1→S9**（先轻后重、先高频后深水）。除上述依赖外各批独立，用户可按优先级
重排（例如想先收 G 欠账，可把 S4/S5/S8 提前）。重排时归属规则不变（见 S0 节的唯一归属规则）：
**每批只落地自己所需且尚未落地的 S0 项**，工程首开批额外落 S0-台账——首开批**不**吸收与它
无关的 S0 项。

---

## 3. 逐批规格

> drill 编号**自本 roadmap 起确立**「十位 = 场景族」约定（既有 00/10/11 恰与其兼容；12/13 为
> 历史例外——12/20/21 的号来自 gotcha 编号、13 为顺延号——保持原号不改，S1 README 编号节注记）：
> 0x 骨架 / 1x grow / 2x force-single·容量 / 3x 升级 / 4x 收缩·回归·割接 / 5x 备份·灾备·轮换 /
> 6x 用户平面 / 7x expose·proxy 数据面 / 8x session·安全·入群 / 9x 观测·客户端视图·混沌。
> 编号与 drill 名为提案，各批 plan 定稿。**§4 表中标注「顺带」的面必须在本节对应 drill 规格里
> 有落点句**（§3 是批 plan 起草的直接底稿，§4 只是核对清单）。

### S1 — 用户平面核心旅程（+ S 系列工艺底座）
- **动机**：使用者每天在跑的命令（exec/run/ps/history/node ls）在 deploy-tier 零覆盖；这也是
  建立 journey 工艺、gap 台账、README 编号规范的第一批。
- **依赖**：无。
- **drill**：
  - `60-user-journey`（GREEN，**N=1** + 2 agent + ctl——全部断言不涉集群语义，按 §0.4 拓扑最小化
    原则用 N=1；集群态的用户面行为〔severe banner、--ack-alerts 门〕归 90/92）：完整「新使用者
    第一天」旅程——login/ctx/logout 往返（G.3：ctl 重连取最新快照，断连后再 login 状态一致）→
    `node ls`（ONLINE/HEARTBEAT/PROTO/RELEASE 列真值）→ `exec`（exit 0/非 0 透传、stdout 流式、
    `--cwd`、信号杀 →128）→ `run`（**真 PTY**：pty 驱动下断言 `stty size` 行列、交互 shell 往返、
    Ctrl-C 中断 1s 内进程组退出、attach 全程无孤儿）→ `ps`/`ps -a`（RUNNING→EXITED、PORTS 节）→
    `history`（`-n`、`--kind call/proc/transfer`、`--follow` 烟测）→ `version`/`completion` 顺带
    各跑一次。部署层价值：真 auth_callout JWT 模板下全命令面 + 真跨容器 PTY 字节流 + 真 User 隔离
    （hermetic 全部在同宿主嵌入 NATS 上跑）。
  - `61-transfer-edges`（GREEN，N=1）：push/pull 的部署敏感边界——`allow_roots` 收紧/显式 `[]`
    禁用（改 agent.yaml + 重启 agent = 部署职责）→ **push 写侧与 pull 读侧双向执法各一臂**
    （两条独立执法路径：`path_outside_roots`/`transfer_disabled` 双向、`path_parent_missing`
    〔push〕vs `path_not_found`〔pull〕）；**叶子软链拒 `not_a_regular_file` 双向**（agent 真盘
    `ln -s`，机制级加固、所有模式生效）；`dst_exists`→`--force`；>2 GiB 拒 `too_large`（truncate
    稀疏文件，不真传）；tier 边界（默认 nats `max_payload` 下 ~500 KiB inline 上限 → 1 MiB 文件
    自动升 tier-B）；`history --kind transfer` 出 start/complete/failed 对。**显式裁剪**（留
    hermetic，§0.3 闸门）：`sha_mismatch`/`path_race` 的注入类负例。
  - `62-remote-fs-safe`（**feasibility spike**，允许结论「环境不可行」）：容器内忠实复现网络盘
    挂死（NFS hard mount D 状态需内核 nfsd；备选 fuse+SIGSTOP **语义非 D**——spike 必须显式区分
    真不可中断 D 态与 FUSE 近似，不得静默等同）。若可行，**三臂**（v0.3.3 起缺省已是
    `remote_fs.mode: auto` 自动快速失败——「默认卡死 vs --safe」的对照不成立）：① 忠实缺省
    `auto` → 快速失败带 `remote_fs_*` reason；② 显式 `mode: off` 遗留风险基线 → 观察真实卡死
    形态（**外部有界 watchdog** 包裹，drill 自身不许挂死）；③ 同 `mode: off` + `--safe` →
    证明手动覆写。不可行 → NOT-COVERED 登记 + 留实机。
- **harness 增量**：落地 **S0-pty**（`image/pty-run.py`，pty-confirm 的一般化——ctl 用户本来
  就有终端，环境职责非弥补）与 **S0-台账**（`docs/deploy-tier-gotchas.md` 建档；README drill
  表/编号节重构，含 §1.4 的 11 号行漂移清偿）。
- **铁律风险点**：sim 容器恒 `--privileged`（PID1 systemd 前提，且 usage §9.5 本就把
  `--device=/dev/ptmx`/`--privileged` 列为容器化 agent 的文档化前提），故 **PTY 权限面在 sim
  恒可用**——run/PTY 断言的部署层价值在真跨容器字节流/信号/resize，而非 ptmx 权限；反向风险是
  **烘焙布局偏离 install.sh 真实布局**造成双向失真（§0.3-1 caveat），权限/属主类断言一律对照
  install.sh 真实形态。
- **验收出口**：60/61 GREEN 入 run-drills 全套；62 三分诊出结论；台账/README 落盘。

### S2 — session·多租户安全·admin·agent 入群
- **动机**：session 是产品的核心隔离单位、auth_callout 是安全承诺的执行点——**只有真独立
  nats-server 上的 ACL 拒绝才是真拒绝**；C1/C2 的 agent 自动发现/入群从未在真栈演练。
- **依赖**：**S0-隧道**（81 的活跃 expose 归宿臂）、**S0-ingress**（82 的 manifest 跨容器腿）
  ——先于 S3/S4 开工则本批落地之。
- **drill**：
  - `80-session-isolation`（GREEN，N=1 + 2 agent + ctl）：双 session（lab/ops）双身份
    （TETHER_HOME 隔离）。**隔离 oracle 三分**（auth_callout 下非成员在 CONNECT 期即拒、
    session-A 凭据到不了 session-B 应用层——「每动词返 not_a_member」不是可达 oracle）：
    ① **非成员激活在 CONNECT 被拒**（login 对 sidB 无 PIN 激活 → 连接期拒、不落
    current_session）；② **裸 NATS 跨 session ACL 双向双操作拒**（vendored `nats` CLI +
    A-session 凭据对 `s.<sidB>.…` 的 **publish 与 subscribe** 各一臂，B 对 A 对称）；
    ③ **session 内非 owner 达 broker 的应用层拒**（member 跑 `session rm`/`node upgrade` →
    `not_owner`）。**错误 PIN 拒 → 同身份正确 PIN 立即成功**（负-正对照，防误锁）；
    **PIN 限速探针**（架构 §E.6 承诺「每 IP 每分钟 ≤10 次尝试」是权威契约；**连发错误 PIN 全被
    拒无法区分「本来就错」与「被限」——假阳性 oracle，禁用**）：黑盒判别设计——同一 ctl 容器
    （同源 IP）用**正确 PIN** 起 11 个全新身份（TETHER_HOME×11）在一分钟内依次 join → 至多
    10 个成功、**第 11 个必须被拒**；**对照源**：第二源 IP（另一容器）同窗内新身份 join 成功
    （证明是按 IP 限流而非全局故障，§0.4 限流对照规则）；窗口过后被扣身份重试成功（窗口重置
    规则、源 IP 观测点、成员/事件计数由 s2 plan 命名）。若第 11 个也成功 → **限速缺失**，
    signature-guarded RED 登产品 gotcha（#25+ 候选；owner 若判定产品有意放弃该承诺，走独立
    文档修订改 §E.6 并显式接受爆破风险——不许静默豁免）。`pin_failed` 审计可见性保留为独立
    oracle；`TETHER_SESSION` 双 shell 互不串台。
  - `81-admin-evict-session-rm`（GREEN，N=1；**活跃 expose 归宿臂需 S0-隧道**）：broker 本机
    `admin sessions/nodes/audit`（`sudo -u tether`，socket 0600 属主语义：非授权用户拒）→
    `admin evict` → 在线 agent ~1s
    自杀 + 重连被拒（provisioning 已删）→ **evict 时名下有 RUNNING 进程 + 活跃 expose 的归宿**
    （文档未定义清理语义——探索→定格：进程/公网口何去何从如实钉住，缺口登 gotcha）→ **被踢
    nkey 经新 `--pin` 重入群的语义**（"nkey 泄漏立即吊销" 是文档明列用例，重入是否被拒直接关乎
    其成色——定格）→ `session rm` 三阶段：DELETING 期间新调用拒 `session_not_found_or_deleting`、
    `nats stream ls` 终态无 `history-<sid>`、SQLite 无残留；**`session_deleting` 广播 probe**
    （usage §5.10 承诺 broadcast ⇒ agent 拒新调用，但源码未见对应 `pubSysEvent` writer——验证
    agent 拒调用的真实机制路径；承诺与实现不符则登 gotcha/DOC 候选，**不因 rm 旅程跑通而记
    covered**，清单附录 §1.2）。
  - `82-agent-onboarding-invite`（GREEN，N=2 集群 + fresh agent）：真 C2 旅程——leader
    `cluster seeds publish --bootstrap …`（首次手动 = 产品语义）→ `cluster invite` 铸 OOB
    invite（inline `seed=nats://…`）→ 新 agent 容器 `tether agent join '<invite>' --start` →
    ONLINE；`agent config refresh --once`；`agent doctor` 全绿；C1 收敛：grow 加 broker 后
    agent roster 缓存刷新（refresh --once 加速断言）；well-known manifest：broker 本机 curl
    loopback 验签 + **经 S0-ingress 反代的跨容器 bootstrap-URL 腿**（invite 强制 https——
    `https://<brk>/.well-known/…` 经同 netns 反代 + 实例 CA 信任；产品 listener 保持
    loopback-only，见 OQ-5）；`agent_roster_stale`
    事件测点（refresh 前滞后可见、refresh 后消除，§4.6）。**信任锚负例**：
    伪造 invite（错 pin）→ join 端到端拒且**无半 onboard 残留**（无半写 unit/agent.yaml）；篡改
    invite（未知参数/错 scheme）→ parse 拒。**user-service spike 臂**：`loginctl enable-linger
    sim` 后走 install.sh `--role agent` + `tether agent --install-user-service` 真路径（产品文档
    的首日主路径，usage §6.1 步骤 2）——可行则收编为常驻臂，确证不可行才落 NOT-COVERED（理由=
    实测结论）。修 §1.4 的 provision-node.sh 注释漂移。
- **harness 增量**：多身份 ctl 约定（TETHER_HOME）；`agent join` 路径接线（保留 --pin 路径）。
- **铁律风险点**：evict/rm 的清理断言要「验结果不验退出码」（三阶段是异步的，poll_until）；
  隔离断言必须双向，防单向 ACL 缺陷漏网。
- **验收出口**：3 drill 入套；C2 旅程可执行文档化（等同 `simcluster grow` 之于 G4 的「可执行规格」地位）。

### S3 — expose 数据面 + rehome/failover
- **动机**：expose 是数据面主打；P12 `--remote-port`、B4 `--on-broker`/`--no-rebuild`、C6
  `expose explain`、D6 rehome 全部无 deploy-tier 覆盖；**真隧道字节流 + 真公网带监听 + 真
  state.json 重连**是 hermetic fake transport 够不到的核心。
- **依赖**：无外部前置；本批落地 **S0-隧道**（默认归属——先于本批开工的数据面批按 S0 规则代落）。
- **drill**：
  - `70-expose-journey`（GREEN，N=1 + agent + ctl）：agent 起 `python3 -m http.server` →
    `expose --local` → **从 ctl 容器 curl 公网带端口取回真实体**（数据面铁证）→ `ps` PORTS 节 →
    `expose explain`（home/epoch/rebuild）→ `--remote-port` 指定成功 / `port_taken` /
    `port_out_of_band` → `name_taken` → `expose rm` → 立即断流 + 端口立刻复用 → **agent
    `systemctl restart`** → state.json token 自动重连、URL 不变、curl 仍通（P6 出口断言的真栈版）。
  - `71-expose-rehome-failover`（GREEN 主体，N=3）：`--on-broker` 钉 home（`on_broker_unknown`
    负例）→ **docker kill home broker** → rehome 到幸存 voter：epoch 递增、explain 标 moved、
    `ps` HOME 列变、**新 home 上同号公网端口 curl 通**（agent 按 home directive + cert pin 重拨）
    → `--no-rebuild` expose：home 挂后**不**迁（断言不可达 + 状态如实）→ `cluster drain` 遇
    rebuild-OFF expose 拒静默迁移（runbook §2 语义）→ home 回归 → 分布保持粘性（#18 默认关，
    与 S4 的 74 呼应）。顺带捕获 rehome 生命周期事件族（`home_reassign_*`/`rehome_stalled`，
    §4.6）。
- **harness 增量**：落地 **S0-隧道**——按 install.sh 真实形态把 `tunnel_addr: <broker>:7000`
  写进 agent.yaml（清偿 §1.4 保真度债；install.sh 在真实安装里本就写它，Mandate ③ 正当供给，
  绝非替 tether 弥补）。**没有这步 70/71 的数据面正向断言必 RED**（agent 默认空拨本地
  127.0.0.1:7000）；S2-81/S4/S6-40/S7/S9 同样依赖此项（S0 表）。
- **铁律风险点**：rehome 断言必须以「curl 真流量恢复」收口，不以 status 字段为终点（防「视图
  收敛而数据面死」的假绿——正是 #20 的教训形态）。
- **验收出口**：2 drill 入套；发现的缺口按 #25+ 登记。

### S4 — proxy 订阅面（P13 + C5）+ G7a 债
- **动机**：proxy 订阅是 tether 首个对外 HTTP 面 + 唯一「非成员消费者」面；C5 cluster 化
  （rehome/quorum-loss 策略）与 G7 rebalance 的 sim 验收都欠着。
- **依赖**：**S0-隧道**（proxy 数据面骑真隧道）；本批落地 **S0-ingress**。OQ-1 选型本批 plan 定。
- **drill**：
  - `72-proxy-subscription`（GREEN，N=1 + 2 agent）：`proxy on --yes`（owner-only：member 拒）→
    `proxy status`（member 可读、无密钥泄漏）→ `sub create alice`（URL 只印一次；**重复
    create 同名 → 探索→定格钉冲突语义**）→ curl `/sub/<token>` 订阅体：每在线 agent 一节点、
    host/port 真值；**`/sub/<junk>` 伪造 token 负例**（拒且无信息泄漏——订阅面唯一对外公网
    HTTP 门）→ **真 SS 流量分工**（docker 桥上一切目标都是 RFC1918：**agent-A 的 agent.yaml
    显式 `proxy.allow_private_destinations: true`**〔文档化部署配置，高危注记照抄〕承载正向
    SS 腿；**agent-B 保持默认**承载「私网目的地默认拒」负例——同一 agent 上两者互斥，必须分工）
    → AEAD 门（错误 PSK 被断、无凭据连不进）→ `sub revoke alice`：在飞连接断、其它订阅不受
    影响、**被撤销 PSK 的新连接被拒**（"重放 salt 立即失效" 的撤销传播承诺）→ `proxy off`
    全停 + 端口回收。
  - `73-proxy-cluster-ha`（GREEN，N=3）：cluster 模式 `proxy on --ha-policy freeze-on-quorum-loss`
    → 杀 exit 的 home broker → **exit 自动 rehome**（C5）、`proxy status` 每 exit 标**它自己
    home 的 public_host**（G7 #2）+ **`proxy status --cluster`** 视图（per-agent ready reason/
    home/degraded 态）+ rehome 期间捕获 no-ready/partial 过渡事件（§4.6）→ quorum-loss
    （杀至 1/3）：freeze 语义 = `/sub` 继续服务；对照 `disable-on-quorum-loss` = 404
    （plan 裁剪二选一或双臂）。
  - `74-rebalance-on-return`（GREEN，N=3；**G7a 债**，按 `g7-plan.md:133` 规格落地）：堆 proxy
    homes → 重启一 broker → 断言分布倾斜（粘性 = 好性质）→ 默认 off：回归后**保持**倾斜；
    `TETHER_AUTO_REBALANCE=on`：回归自动均摊 `max−min≤1` + **`proxy_auto_rebalanced` 事件
    恰一次**（g7-plan 案 6）；`cluster rebalance proxy --dry-run` 预览与实跑（手动 verb 的
    deploy-tier 落点）。
- **harness 增量**：落地 **S0-ingress**（规格见 S0 表行——**每 broker 一个共享其 netns 的
  HTTPS 反代**〔sidecar `--network container:<brk>` 或容器内进程；普通 bridge 容器到不了他容器
  的 127.0.0.1〕+ 实例作用域测试 CA/SAN + ctl/agent trust 注入 + 证书正/负例；产品 listener
  loopback-only **绝不改绑/削弱**，不设 `TETHER_DEV_NO_AUTH`；无 ACME/无真域名，「测试 TLS ≠
  免 TLS」）。72/73 的跨容器订阅消费一律经反代 `https://<brk>/sub/…`，broker 本机 loopback
  断言保留（handler 正确性与公网 ingress 行为分立——真 Caddy+ACME 公网面仍属 staging/实机）。
  另：OQ-1 的 SS 客户端。
- **铁律风险点**：cluster 下 proxy 若仍有 `proxy_unsupported` 类残余行为（usage §5.15 尾段
  与 C5 矛盾处）→ 如实定格：是代码缺陷登 gotcha、是文档残留登 DOC 缺陷，**不得**用 harness
  绕出一个「可用」假象。
- **验收出口**：3 drill 入套；G7a 债清（`g7-review.md` m11 行翻绿）。

### S5 — 升级与版本运维（+ G5 债）
- **动机**：升级是最高频的 day-2 运维；`node upgrade` 的下载/校验/`syscall.Exec` 原地重启、
  `cluster upgrade` 的滚动编排**只有真 systemd + 真换二进制**才测得动；G5 的
  `30-rolling-upgrade.sh` 是记账欠债。
- **依赖**：无（artifact-server/staged 二进制/agent 布局对齐均本批自建）。
- **drill**：
  - `31-node-upgrade-fleet`（**探索→定格，预期落 gotcha**，N=1 + 2 agent）：env 起 artifact
    server（真 https tarball + SHA256SUMS）+ broker.yaml `upgrade.url_allow` 指向它 →
    **代码证据预判**：agent 侧白名单硬编码 GitHub 前缀且**无任何操作员接线**（无 flag/yaml 键/
    env——而 usage §9.3 与 error_hints 都声称存在 `--upgrade-url-allow` agent flag），自托管
    镜像升级将被 agent 以 `url_not_allowed_local` 拒 → **gotcha #25 候选**（agent 白名单不可配 +
    usage/error_hints 指向不存在的 flag = 并发 DOC 缺陷），`assert_bug` 签名钉 RED；**严禁**
    /etc/hosts 假冒 github.com + 自签证书强推 GREEN（Mandate ①）。仍可 GREEN 的臂：broker 侧
    白名单外 URL 拒 `url_not_allowed`、伪 sha256 拒 `sha256_invalid`/`sha256_mismatch`。
    `--all` 分类臂（**按实现校准**：已 OFFLINE 的 agent 在枚举期即被静默排除、不产生 skip 行——
    正确写法是**枚举后失能**：SIGSTOP/docker kill 一台使其仍在 ONLINE 心跳窗内被列入 → dispatch
    期 `agent_no_responders`/timeout → 断言 stderr skip 行 + "(N node(s) skipped…)" 汇总；另一臂
    钉「已 OFFLINE 者被静默排除、不出现在汇总」的语义分界）。**randomly-kill-mid-upgrade 崩溃
    一致性臂**（原子 rename 保旧二进制）登记为「随 #25 修复解锁」。成功升级 + MainPID 不变的
    GREEN 断言同样待 #25 修复后翻绿。
  - `30-rolling-upgrade`（GREEN，N=3；**G5 债**，规格已在 `g5-plan.md` §3.3）：staged 新版
    二进制（特权步，运维职责、如实分立）→ `cluster upgrade --dry-run` 预览（followers-first/
    leader-last）→ 实跑（`--expect-sha256`/`--account-seed`/`--backup-taken`）→ **背景写探针
    全程无 `not_leader`/JS-503**（M1）→ whole-host 判据：broker+同机 agent 双到版
    （OQ-6 colocated agent 供给）→ `node ls --brokers` skew 显示（#19）→ **B2 滚动锁真互斥**：
    持锁期间发起 `cluster join`/`retire` 被拒（不是只挡入口）→ 中途 HALT/重跑续点一臂 +
    **stale-lock 主动清除路径**（全升完但清锁失败 → 重跑同 `--to-version` 打印 "cleared a
    stale upgrade lock"）→ **N=2 写栅栏臂**：降到 N=2 重跑须显式 `--ack-writefence`。
  - `32-install-lifecycle`（GREEN，N=1）：install.sh 生命周期——`--dry-run` 零写入；装完
    `pgrep tether` 空（「永不启动」不变量）；`--uninstall` 后文件清单归零、重装幂等；
    sentinel/provision 与真 install.sh 的属主表对照（doctor 复检）；**§8.4 单机 broker 手动
    升级臂**（stop → 换二进制 → `integrity_check` → start → G.2 收敛——单机 broker 现网唯一
    升级路径；带 DB forward-migration 的真实版本对 plan 时定）。
- **harness 增量**：`remote.sh` 双版本构建（`vendor/tether-next`，SIM_VERSION 区分）；落地
  **S0-artifact**（https 静态服务 + 自签 CA 注入容器 trust store = 部署职责，OQ-3）与
  **S0-布局**（agent 二进制对齐 install.sh 真实形态：用户可写 `~/.local/bin`，清偿 §1.4 布局
  失真债——即便如此 agent 白名单墙独立存在，31 的定格结论不受影响）。
- **铁律风险点**：`cluster upgrade` 的二进制 staging 是**文档化的特权前置**（v1 设计）——drill
  必须把它作为显式分立步骤呈现（G5 plan 原话「asserted to be a distinct privileged step」），
  绝不能让 sim 的 staging 让编排看起来比实际更自动。
- **验收出口**：3 drill 入套；G5 债清（`g5-review.md` #8 行翻绿）；31 的 gotcha 定格落台账。

### S6 — 拓扑收缩与回归（drain/retire/shrink/rejoin/带数据割接/online force-single）
- **动机**：grow 有覆盖，**收缩方向全空**——retire/drain、N=1 去集群化、returning-node rejoin、
  set-raft-addr rebind 都是 runbook 正文却零 drill；online force-single（文档标「首选」）在
  deploy-tier 从未走通（§1.4 覆盖债）。
- **依赖**：**S0-隧道**（40 的 expose fixture）。
- **drill**：
  - `40-drain-retire`（GREEN，N=3 + expose fixture〔需 S0-隧道〕）：`cluster drain`（`--abort` 往返回 VOTER）→
    `cluster retire`：expose rehome 走、streams 副本收敛、roster/raft 双移除、`--wait` 语义、
    `ops ls/show` 全程可观测 + **`ops confirm/abort` 臂**（制造 BLOCKED/STALLED 态驱动之——
    drain/retire 中断天然可造）→ **mid-retire 中断臂**：retire 进行中 kill leader → 重启 →
    resume 收敛（"可 resume" 的文档承诺）→ N=2 再 retire（F==0 typed confirm，pty 喂）→ retire
    后提示「retire ≠ 凭据撤销」如实出现（B3）。顺带：`cluster apply -f`（仅 plan 断言，不执行）
    + `reconcile nats --plan`（dry-run 打印将改动 + **不写任何文件**的零写断言）+
    `cluster add --dry-run`（零改动打印 grow plan）+ **安全门负例组**：`drain --retire`（Hidden
    REMOVED-redirect）报错导向 `cluster retire`；`recovery node remove --yes` 被拒（Tier-2
    无 `--yes` override）；`node remove --confirm-node-id` 无 `$TETHER_CONFIRM_NODE_ID` 时拒
    （双因子 machine-confirm）；`--force` 的孤儿化语义与 `--auto-confirm-catchup` 边界由
    s6 plan 定臂（清单附录 §2.2）。
  - `41-shrink-to-standalone`（GREEN，N=3→1）：retire×2 → `reconcile nats --to-standalone
    --confirm-single`（有 peer 时硬拒的负例先行）→ operator JS reset → 重启 → tier-B 通 +
    **reconciler 保持 standalone 跨重启/gen-bump**（R3 持久 desired-state 断言）→ **§1.0 rebind
    臂**：regrow 前先走 loopback-advertise → `cluster set-raft-addr` 在线改绑（grow-ready 前置，
    语义与 regrow 天然吻合）→ regrow N=2（mode-preserving，d9 hermetic 的真栈版）。
  - `42-rejoin-returning`（GREEN，N=2 force-single fixture 复用）：被弃节点冷启动 →
    **actionable 报错**指明 rejoin（G2 #15 修复的真栈回归，不再退 70 崩溃循环）→
    `recovery rejoin prepare`（dump-divergent 0600 + wipe + `--emit-manifest`）→
    `init --from-manifest` → `join approve --wait`（`--wait` 变体的 deploy-tier 落点——grow
    路径 G4 后有意非阻塞）→ 回 N=2、名册收敛。顺带：`recovery diagnose`
    （离线诊断，与 22 的 online `--dry-run` 是不同代码路径）+ `recovery force-single --guided`
    （B7：只读诊断 + 打印真值替换的精确命令）+ **resnapshot 变体**（raft/ 丢失、
    DB 完好 → `recovery resnapshot` 重建单 voter——现网实战过的恢复路径；**`--accept-audit-loss`
    两臂**：有未发布 audit 且不带 → 拒，带 → 显式截断继续〔数据损失开关，定格真实语义〕）+
    typed-confirm 臂顺带 `assert_refuses --yes`（rejoin prepare/resnapshot 的 Tier-2 拒绝面）
    与 machine-confirm 缺 env 拒的负例（**仅 resnapshot**——它是 `machineEscapable=true` 者；
    rejoin prepare 无 env 逃生、restore 是 never-escapable，见清单附录 §2.2）。
  - `43-migrate-live-data`（GREEN，N=1→cluster）：**带活数据的 runbook §4 割接**——init 前先建
    session/expose/history（既有业务行）→ `cluster init --from-existing` 割接（forward
    migrations + home_broker backfill）→ 割接后 session/expose/agent 存活断言 → **回滚臂**：
    restore `tether.db.bak` + 去 cluster 化 → 回 v2 单机、业务行完好（runbook §4 Rollback 段的
    真栈版；既有 `simcluster init`/00 只割接过**空库**）。顺带：`init --check`（dry-run 别名
    `--dry-run`）零改动断言 + `init --yes` 被拒（Tier-2）+ init 的 machine-confirm 缺 env 拒
    负例（init 是 `machineEscapable=true` 者）。
  - `22-forcesingle-online`（探索→定格，N=2）：**顺序约束**——先在真产品 unit 下复现 dwell-bounce
    并判定其生产可达性（bounce 若在真实 `Restart=always` 产品 unit 下持续不止 = dwell 结构性
    不可达成 = 产品 gotcha #25+，**非环境特异**）；仅当确证 bounce 为容器 timing 特异才允许
    harness 侧驯服（杀后等 bounce 级联平息 + dwell-gate poll 窗参数化加长）。两种出口（可驱动 /
    定格 gotcha）同权预期。正路径若可达：`--dry-run` 健康集群排练（零改动）→ 杀 peer → dwell
    满足 → typed confirm → `force_single_active` + **broker PID 不变** → R3 语义：conf 留
    clustered、status 响亮提示 operator `--to-standalone`。**拒绝门四臂**（无论正路径是否可达
    都要落，安全闸零覆盖是 false-green 面）：(a) 健康集群**真** arm（非 dry-run）→ 拒，钉
    quorum-not-lost 签名；(b) 杀 peer 后 dwell 未满窗内真 arm → 拒，钉 dwell-remaining 签名；
    (c) **peer-alive 拒**：停 peer 的 tether-broker 但保其端口应答（模拟 merely-partitioned-
    but-alive），dwell 满后真 arm → 拒，钉 peer-alive 签名（runbook 防脑裂承诺的直测）；
    (d) arm token 单发/过期重放拒；(e) `--yes` 被拒（Tier-2 无 override，B3 设计语义）。
    **保护模式臂**：dwell 窗口内（quorum-lost 态）`retire`/
    `set-raft-addr` 等 routine 命令全拒（runbook §2.3 语义）。另：12/20 共享 setup 在杀 peer
    **之前**各加一条 OFFLINE force-single 的 assert_refuses（offline 无 dwell，钉的就是
    peer-alive 拒）。
- **harness 增量**：无重大新件；forcesingle fixture 参数化复用。
- **铁律风险点**：shrink/rejoin/割接的每个 operator 手动步（JS reset、wipe、restore .bak）是
  **产品文档规定**的运维动作——drill 照剧本执行并标注 `[operator per runbook §x]`，与「sim 替
  tether 干活」严格区分；若发现剧本里某步**本应自动**（对照 §B 零手动目标）→ 登 gotcha。
- **验收出口**：5 drill 入套；online force-single 得到「可达成/不可达成+原因」的定格结论。

### S7 — 备份·灾备·凭据轮换
- **动机**：备份是「破坏性操作前必做」的全剧本前置，restore/DR 是最后防线——从未在真盘上
  演练过一次；C7/runbook §2.1 的凭据轮换同样零覆盖。**灾备演练在真实运维里就该定期做**。
- **依赖**：**S0-隧道**（50/51 的 expose 业务态）；本批落地 **S0-备份库** 与 S0-故障原语的
  卷级件；软依赖 S6（复用其 fixture 经验）。
- **drill**：
  - `50-backup-restore`（GREEN，N=2；expose 业务态需 S0-隧道）：**备份前先种业务态 X/Y**
    （session/expose/history 行）→ online `cluster backup`（**在 leader 上取权威 bundle**——
    X/Y/Z 同一性 oracle 一律用 leader bundle）+ **follower 语义成对臂**：follower 上默认
    **拒**（refuses, re-run on the leader）→ 显式 `--allow-stale-follower` 放行且标
    possibly-stale（stale bundle 不用于同一性 oracle）→ manifest 无密钥断言 → **备份后再造 Z**
    （新 session）→ 灾难注入（wipe brk1 卷 = env 模拟）→ `recovery restore`（typed confirm）→
    **内容同一性对照：X/Y 在且 Z 不在**（恢复的确是备份时刻，不是空库/错库——阳性+阴性对照）→
    单 voter 起（exit 1 DEGRADED、**非** exit 3）→ re-grow → 数据面回。负例三臂：**foreign
    bundle 拒**（用 brk2 secrets 恢复 brk1 bundle）；**torn/edited bundle 拒**（篡改 manifest
    一字节 → 拒于任何磁盘写之前，断言原 tether.db 字节未动）；**kill-9 中断 restore** →
    `restore_in_progress` 下 `systemctl start tether-broker` 被拒 → 重跑 restore 续完。
    顺带：`recovery incident export`（脱敏 best-effort 断言 + **`--force` 的 O_EXCL 负例**：
    不带 --force 时目标已存在 → 拒）+ `cluster doctor --offline` 正向 preflight 臂（secrets
    全绿 + **preflight 输入面**：错 `--db/--conf` 指向时不得判绿，plan 定格）+ **restore
    确认语义三断言（never-escapable，R5-F2）**：① `--confirm-node-id` 与 bundle
    manifest/provenance 不符 → 拒；② `--yes` 必拒；③ **flag 与 `$TETHER_CONFIRM_NODE_ID`
    同时正确、非交互执行仍拒**（restore 是 `machineEscapable=false` 的 typed-confirm——
    **不属于** machine-confirm 双因子面，勿写「缺 env 拒」这类假阳性负例）。offline backup
    变体一臂。
  - `51-full-dr`（GREEN，N=3 全灭；需 S0-隧道 + S0-备份库）：**bundle 先导出到离簇备份库**
    （实例命名空间化 host 目录：在卷灾难〔`rm_node --vols`〕之外、**在 `simcluster nuke` 之内**
    ——存活于模拟灾难、但绝不跨轮泄漏，S0-备份库生命周期）+
    **灾前记录一个活 expose 的公网端点** → 杀全部 broker 容器+卷 → **先证 bundle 在备份库仍
    可读** → fresh 容器 + **从运维密钥库恢复原节点 secrets**（restore 的 provenance 锚是原
    tunnel-cert；新铸信任材料=非恢复，明确区分）→ restore 该 bundle → re-grow N≥2 →
    **agents 自动重连 re-pin、灾前 expose 以 curl 真流量收口恢复**（runbook §5.2 的四步承诺
    逐条断言，rehome 不以 status 字段为终点）。
  - `52-credential-rotation`（GREEN 主体，N=2）：`rotate-tunnel-cert`（pin 轮换窗口、agent
    re-pin 可见性 B5）→ runbook §2.1 全剧本：铸新 account.nk + 新 CA（**能用产品工具处铸之**：
    `cluster keygen` 铸 nkey 面——它是 in-binary 唯一 node-ident minter，deploy-tier 落点在此）
    → 分发 → `reconcile nats --all --wait` → 滚动重启 → **旧 route leaf 被新 CA 拒**（负例）+
    车队在新信任锚下全活；**C7 命令面**：`cluster retire <n> --compromised
    --require-credential-rotation`（引导式轮换 + 持久 `manual:credrot:<node>` NOT-SAFE 告警的
    raise→轮换完成→clear 生命周期）；其余 staging 演练工具面 plan 时核 `cluster_rotation.go`。
- **harness 增量**：落地 **S0-备份库**（实例命名空间化 host 目录——**`rm_node --vols` 之外、
  `simcluster nuke` 之内**，与 S0 表/51 一致）+ 卷级灾难注入原语（`rm_node --vols` 已有，加
  保留-secrets 变体；S0-故障原语的卷级件）。
- **铁律风险点**：restore/DR 剧本步数多，**每一步靠人**都要问「§B 零手动目标下这步该谁干」——
  照剧本执行 + 计步呈现，剧本外的额外绕过 = gotcha。
- **验收出口**：3 drill 入套；DR 剧本获得「按文档可完成/何处卡壳」的定格结论。

### S8 — 客户端视图与可观测告警（+ G3/G7b 债）
- **动机**：运维的眼睛。G3（成员变→客户端视图自动收敛）与 G7b（JS-503 远程告警）的 sim 验收
  欠债都在此；alert 面、metrics、`--remote`/`--watch` 从未在真栈上验过。
- **依赖**：92 的 #20③ 专属 leg 软依赖 S6-22 结论；其余无依赖。
- **drill**：
  - `91-client-converge`（GREEN，N=1→2→3；**G3 债 A/C/D 臂，臂间隔离、D 先于破坏性 C**）：
    首次 `seeds publish` → grow → `seeds show` **自动**含新成员（change-gated auto-publish，
    A 臂）→ retire → 死端点自动消失 → **D 臂（健康多 broker 拓扑上执行）**：显式命名 ctl 钉定
    的 floor broker（`cluster pin` 后指认）、**先证 pre-failure 路径**（ctl 当前确经 floor 连接）
    → 杀 floor → **ctl 经非-floor 幸存者刷新 roster**（`cluster_endpoints.json` 收敛，#17）+
    broker_url 指死 broker 时 `node ls` 自动 failover（cli-failover v0.4.6 真栈回归）→
    **C 臂（最后、独立 fixture/重建后执行——offline force-single 后仅剩单 survivor，若先做 C
    则 D 无「非-floor 幸存者」可用）**：offline force-single（复用 12/20 fixture）→
    `seeds show` 收敛仅 survivor（prune 的 drop-only 收敛）→ **信任锚负例**：错锚签名的
    roster 被拒、ctl 缓存不被毒化。**B 臂**（online force-single 后 seeds 收敛）骑 92 的
    (b) leg，见下。
  - `92-js503-remote-alert`（GREEN，N=2；**G7b 债**，两 leg 拆分）：**健康基线先行**（`--remote`
    无 DEGRADED banner——防告警因错误原因预先在场）→ **(a) 22-独立 leg**：N=2 杀 peer（不
    force-single、conf 自然滞留 clustered）→ JS meta 1/2 失 quorum、幸存 NATS 仍应答 →
    `cluster status --remote` 出**泛化 sustained-503 告警**（保底 G7b 覆盖）；此态下顺带复验
    `session rm` 默认拒 + `--ack-alerts` 强推臂 → **(b) #20③ 专属 leg**（软依赖 S6-22 的
    online force-single：产品的 #20③ 专属 banner + `--to-standalone` remedy 提示门在
    `force_single_active` && 磁盘 conf 仍 clustered 上，G2 后 OFFLINE 路径 auto-de-cluster、
    原生只剩 ONLINE 可达）：online force-single → `DATA-PLANE DEGRADED — JetStream UNAVAILABLE`
    专属 banner + **G3 B 臂**（seeds 收敛仅 survivor）→ `--to-standalone` 恢复 → 告警
    auto-clear；22 若定格不可达成 → 本 leg 登 NOT-COVERED-in-sim（banner 逻辑 hermetic 已测），
    不阻塞 (a)。→ `--remote` force_single **exit 3**（#16）→ `--homes`/`seeds show` 的
    `--remote` 变体从 ctl 容器（无 SSH）可用。
  - `90-alerts-lifecycle`（GREEN，N=2）：**注入前洁净基线**（无 active `broker_down`——防
    setup/grow 瞬态预拉的告警让「杀后存在告警」因错误原因而绿）→ `alert raise --kind manual
    --severity severe`（operator socket）→ member `alert ls` 见 + severe banner 上 `ps`/
    `node ls` stderr（stdout 可解析不变）→ `alert ack`（store-backed 团队 ack 语义）→
    `alert clear` 幂等 → 真触发系统 kind：杀 follower → `broker_down` + 与
    `broker_down_rehome_summary` 成对断言（§4.6）；灌 JS store → `disk_pressure`
    （复用 --cap-store）→ 对 `quorum_lost` 合成条件 `alert ack` 被解释拒绝
    （B3 正交开关语义）。
  - `93-metrics-observability`（GREEN，N=3）：`--metrics-listen` 私网口 `/metrics`
    （cluster_mode/is_leader/voters/quorum_margin/peer_lag 真值）、`/healthz`、`/readyz`
    （CATCHING_UP joiner 503 → 摘除语义）→ alert webhook（env 接收器容器捕 POST、无密钥断言）
    → `status --watch` 烟测 + `status --card`（B7 glance 卡片：headline/what's wrong/what to do
    渲染，JSON 不变）→ 在线 vs `--offline` 的 exit-code 语义分立断言（0/1/2/3 vs
    ROSTER_UNREACHABLE，B2）→ `--log-json` 日志行可解析断言。
- **harness 增量**：webhook 接收器角色容器；`laptop` 视角 = 复用 ctl 容器。
- **铁律风险点**：91 的「自动收敛」断言必须是**无 operator 命令**的收敛（G3 精髓）；任何一步
  需要手动 publish 才收敛（首次除外）→ 缺陷。
- **验收出口**：4 drill 入套；G7b/G3 债按臂如实清账（B 臂随 22 结论，不可达成则在验收
  记录里显式登记而非宣称全清；G7a 债在 S4 清）。

### S9 — 混沌对账与长稳（收官）
- **动机**：P8 reconciliation 是产品对「崩了能自愈」的承诺；hermetic 已测状态机，**真 systemd
  + 真 kill -9 + 真时序**下的自愈从未验证；#23 只有 doctor 静态 drift 检查、无行为级证明。
- **依赖**：**S0-隧道**（94/95/96 的端口/expose/PTY 面）；本批落地 **S0-故障原语**（分区件）
  与 **S0-pty** 若 S1 未先行（96 的 PTY 混沌臂）。
- **drill**：
  - `94-agent-reconcile`（GREEN，N=1 + 2 agent）：`exec sleep 9999`×N → **docker kill agent**
    → 重启 → G.1：进程全翻 EXITED(-1)、`ps` 收敛 + **G.5 审计痕迹**（`history --kind proc/port`
    出 `kind=reconciled/reconciled_closed` 记录——运维回答「进程为何突然 EXITED(-1)」的唯一
    依据）；**orphan 方向（产品路径造法——simcluster 的 broker 全部是单 voter 集群模式，
    直接删 SQLite 行会 fork raft 复制态〔`broker.go:1092-1096` 明文禁止〕，禁用）**：
    ① 在进程启动**前**取 leader `cluster backup`；② 正常起管理进程 + **证明进程活着**；
    ③ 停 broker → `recovery restore` 该 bundle（产品级「回滚到更旧已提交态」，raft 一致：
    restore 重置游标 + 重 bootstrap）→ 起 broker、等其健康；④ **显式制造一次「保留托管进程」
    的 NATS 断连→重连**——只重启 tether-broker **不会**打断 agent 的 NATS 连接（两个独立
    进程），而 agent 只在初连或 NATS `ReconnectHandler`（`onNATSReconnect`，本体在
    `internal/agent/proxy.go`、注册于 agent.go）时才 register、G.1 的 `reconcileOnRegister`
    只挂在 broker 的注册 handler 上（heartbeat 对未知节点静默丢弃、`tetherd_restarted` 事件
    agent 不消费）——故此步 `systemctl restart nats-server`（或有界网络规则断/恢复该 agent
    连接；**不许**用重启 agent unit 代替——systemd/cgroup 会顺手杀掉被测子进程）；**断连前
    再证 orphan 进程仍活**；⑤ 以**断连后新的 `agent_registered` 事件（或 agent 的
    re-register 日志）**为重注册证据——**roster 回 ONLINE 不算**（restore 的旧 bundle 已含
    该 node，heartbeat 本身就能把它翻回 ONLINE）；re-register 与最终 kill 各给**独立超时**
    （区分 broker 未恢复 / agent 未注册 / directive 未执行）→ 该进程对恢复态是未知的 →
    orphan 被 kill；**断言 `killed_orphan` 审计记录（含 no-RC 语义）+ 返回给 agent
    的 drop directive + 无关行（bundle 内 X/Y）完好**——不以进程消失为终点。（architecture
    P8 原型的「SIGSTOP broker 期间起进程」经产品路径不可达〔start 是 broker 转发的〕，登
    DOC 候选。）
    心跳翻转：STALE→OFFLINE 窗口 + `ps` LOST 合成标签。
  - `95-broker-selfheal`（GREEN，N=2）：**#23 判别性行为证明**——对 MainPID **直接发 SIGTERM**
    （绕过 systemctl；broker 经 NotifyContext 优雅退出 **exit 0** = clean-exit）→
    `Restart=always` 拉起（`on-failure` 对 exit 0 **不会**拉起——这才判别得了 #23；kill -9 是
    unclean、两种 Restart 都救，判别不了）→ 另一臂 kill -9（G.2 崩溃恢复方向）→ G.2：SQLite
    快照恢复、agent 自动重连、ALLOCATED 端口存活（revoke 计时保留）；`systemctl restart
    nats-server` → broker 存活或被单元救活（#23 全链路）；DELETING session 断点续跑（G.2 ①b）。
  - `96-mid-flight-chaos`（探索→定格，N=3；需 S0-隧道/S0-故障原语）：tier-B 大传输中杀 home
    broker →（bucket watchdog 兜底清理 + audit failed）；`run` PTY 会话中杀 broker →
    **先钉定路径**（ctl/agent 显式 `--nats-url` 指到受害 broker + 注入前验证 connected server
    ——否则杀无关 broker 也全绿，false-green）→ 杀之 → 断言预期的重连/续活行为；expose 真流量
    中 rehome → 断流-重连窗口计量；**网络分区臂（静默丢包语义）**：目标 netns 内
    nftables/iptables DROP（或 tc）——`docker network disconnect` 的文档契约只保证摘接口、
    **不**保证静默半开，不能当分区替身——注入前正流量基线、故障期进程/本地端口存活证明、
    无条件清理 trap；分区 leader → 幸存侧选举 + 分区侧只读、愈合后收敛无脑裂；
    `network disconnect` 仅在专测其真实 reset/路由移除语义时另立一臂；**双故障臂**：agent 与其
    home broker 同亡 → 双双回归 → G.1×G.2 交织收敛。**预期高产出 gotcha**——断言只钉确定性
    信号（OQ-7），每臂过 §0.4 破坏性五要素。
  - `97-soak-cycles`（参数化，GREEN）：P8 出口原型「24h + 每小时 chaos」的**缩放替身**——
    N 轮循环注入（agent kill / broker restart / 分区 / 传输并发），轮数与时长参数化（默认短、
    发布前可拉长）。**泄漏 oracle**（journal 串抓不到慢泄漏）：broker/agent 的
    `/proc/<pid>/fd` 计数 + `/proc/<pid>/status`（RSS/Threads）在起点建基线、每轮稳态后采样、
    按**有界高水位/斜率容差**判定；**goroutine 数无产品级观测口（无 expvar/pprof 端点），显式
    NOT-COVERED**（将来产品加 metrics 再收）；journal 的 panic/FK 检查保留为**独立的崩溃/
    完整性 oracle**；重启后 PID 经 systemd MainPID **重解析**；故障规则/后台客户端在每轮末
    **无条件清理**。**与 P8 原始 24h 出口的差距显式登记**（按需 dev 工具不常驻 24h soak；
    发布前可手动拉满）。
- **harness 增量**：落地 **S0-故障原语**分区件（netns 内 nft/iptables DROP、tc、SIGSTOP——
  每件带 Mandate ④ 说明 + 清理 trap）；本批同时做**全套并发基线**：`run-drills.sh` 全量
  （~37 drill）按 OQ-8 的分波/`-j` 策略跑 3 次，固化 wall-clock/flake 基线进 README。
- **验收出口**：4 drill 入套；全套并发 3 连绿（已知 RED 除外）基线落档。

---

## 4. 全功能面 → 批映射总表（无遗漏闸）

> 闸门规则：下表每行必须落一个 S 批或 NOT-COVERED；各批 plan 收工时对照本表勾行，**并以
> 源码级命令树清单对本表做一次 diff**——从 `cmd/tether` 源码枚举 cobra 命令树（**含 `Hidden`
> 位与产生独立部署行为的 flag**；`--help` 文本看不见 Hidden 命令〔如 `takeover-natsconf`〕，
> 不得作为清单来源）**外加架构事件/告警清单**（§4.6）对照。清单字段：command path / hidden 位 /
> 行为 flag / 事件族 / 归属 drill / 断言或 NOT-COVERED 理由。清单**已随 rev5 全量生成**于附录
> （命令树 + 事件面零推迟）——各叶 plan **消费并增量更新附录**（重枚举 → diff → 补行），
> **不得**各自另落盘全量清单。发现表外项 = 闸门失效，先补行再收工；泛泛的命令 smoke 调用
> **不算**覆盖，必须映射到有部署层含义的断言。全程结束时不得有未勾且未登记的行。
> **§4 只是核对清单，每个「顺带」都在 §3 对应 drill 规格里有落点句。**

### 4.1 使用者面（usage.md）

| 功能 / 命令 | 批 | 备注 |
|---|---|---|
| `login` / `logout` / `ctx` / PIN 首连 | S1/S2 | 60 旅程（含 logout/G.3 重连臂）+ 80 隔离 |
| `session create/ls/rm`（三阶段、`--ack-alerts`） | S2/S8 | 81；`--ack-alerts` 强推臂在 92(a) 的告警态复验 |
| `exec`（流式/exit/信号/`--cwd`/`--timeout`） | S1 | 60 |
| `run`（PTY/raw/resize/Ctrl-C/attach） | S1 | 60；failover 中会话存活归 96 |
| `ps` / `ps -a`（LOST 合成、PORTS/HOME 列） | S1/S3 | 60 + 70/71 |
| `history`（-n/--kind/--follow） | S1/S9 | 60/61 + 94（G.5 reconciled 记录） |
| `push`/`pull` 全部错误码与档位边界（双向执法） | S1 | 61（round-trip 已在 00；sha_mismatch/path_race 显式留 hermetic） |
| `--safe` / remote-fs-resilience | S1 | 62 spike，允许 NOT-COVERED 定格 |
| `node ls`（-a、PROTO/RELEASE、--brokers skew） | S1/S5 | 60 + 30/31 |
| `node upgrade`（单台/--all/白名单/sha/PID 不变） | S5 | 31（**探索→定格**：GREEN 升级路径预期被 agent 侧白名单 gotcha 候选挡下，负例臂先行；升级成功臂随修复翻绿） |
| `expose`（P6 全语义 + P12 `--remote-port` + B4 两 flag + explain + rm + state.json 重连） | S3 | 70/71 |
| `proxy on/off/status/sub create/ls/revoke`（P13） | S4 | 72（含 revoked-PSK 新连接拒 + /sub 伪 token 负例） |
| proxy cluster HA（C5：rehome/ha-policy）+ `proxy status --cluster` | S4 | 73（--cluster 视图：per-agent ready reason + home + degraded 态） |
| `alert ls/ack` | S8 | 90 |
| `cluster pin` / `cluster invite` / cli-failover | S8 | 91 |
| `cluster status --remote`（含 exit 3） | S8 | 92 |
| `version` / `completion` | S1 | 60 顺带；纯本地逻辑，hermetic 已密 |
| 控制面 proxy-aware dial（env 代理） | **NOT-COVERED** | 需外部代理拓扑；fake-ip 场景是 WSL/Clash 特异，hermetic proxydial 已测 |

### 4.2 agent 面

| 功能 | 批 | 备注 |
|---|---|---|
| `agent` daemon（注册/心跳/重连） | S1/S9 | 60 + 94 |
| `agent join <invite> --start`（C2） | S2 | 82（含伪造/篡改 invite 负例 + 无残留断言） |
| `agent config refresh --once` / `agent doctor`（C2） | S2 | 82 |
| C1 在线 roster 自动发现 | S2 | 82 |
| agent 自升级（`syscall.Exec`）+ G.1 保状态 | S5 | 31（随 #25 候选修复翻绿） |
| `--install-user-service` / `--uninstall` | S2 | 82 的 **user-service spike 臂**（enable-linger + install.sh 真路径；确证不可行才 NOT-COVERED，理由=实测） |
| `agent.yaml` 策略面（allow_roots/remote_fs/proxy.allow_private_destinations） | S1/S4 | 61 + 72 + 62（remote_fs） |

### 4.3 broker / 单机运维面（broker-ops.md）

| 功能 | 批 | 备注 |
|---|---|---|
| `serve`（真 systemd 下启停/优雅退出） | 既有+S9 | 00/10 组建即覆盖；95 行为级 |
| `admin sessions/nodes/audit/evict`（socket 权限） | S2 | 81（含 evict 时 RUNNING 进程/活跃 expose 归宿 + 被踢 nkey 重入语义） |
| `alert raise/clear`（operator socket） | S8 | 90 |
| `--metrics-listen`（/metrics /healthz /readyz） | S8 | 93 |
| alert webhook（B6） | S8 | 93 |
| `--log-level/--log-json` | S8 | 93 顺带断言 JSON 行可解析 |
| install.sh（三角色/uninstall/dry-run/永不启动） | S5 | 32（broker 角色组建已日常覆盖） |
| **§8.4 单机 broker 手动升级**（stop→换二进制→integrity_check→start→G.2） | S5 | 32 的 §8.4 臂（单机现网唯一升级路径） |
| §8.5 v0.3.0 proxy 配置迁移 / §8.6 G1 现网迁移剧本 | **NOT-COVERED** | 需旧版本安装布局基线（与 flag-day 行同类）；机械步已被 32/13/doctor 的终态断言覆盖 |
| auth_callout 手动启用剧本（§3.4 单机） | 既有 | init 路径即真 auth_callout；孤儿 conf 陷阱（#22 注）由 13/doctor 盯 |
| disk pressure（§7.2） | S8 | 90（--cap-store 灌盘） |
| 备份（§7.4 单机 = cluster backup 同机制） | S7 | 50 |
| 日志轮转（§7.5） | **NOT-COVERED** | OS logrotate 配置，非 tether 行为 |
| Caddy/ACME/wss 入口 | **NOT-COVERED** | README NON-GOAL；跨机 staging/实机责任 |

### 4.4 集群命令与运维剧本（cluster.md / cluster-runbook.md / usage.md §8）

| 功能 / 剧本 | 批 | 备注 |
|---|---|---|
| `init --from-existing` 空库割接（runbook §4 机械步） | 既有 | `simcluster init` 即该路径 |
| **runbook §4 带活数据割接 + 回滚**（restore tether.db.bak） | S6 | 43（新）——业务行存活断言 + 回滚臂 |
| grow 全剧本 / `cluster add`（§1） | 既有 | 10/11/13 |
| `set-raft-addr` rebind（§1.0）/ loopback 前置 | S6 | 41 的 rebind 臂（regrow 前置） |
| `join prepare/approve` / `keygen` | 既有+S6/S7 | grow 内已用（**approve 走非阻塞**——`--wait` 变体 G4 后有意不用，42 rejoin 收尾用 `--wait` 落点）；keygen → 52 的产品铸钥臂 |
| `transfer-leader --wait` | 既有 | 10 |
| `drain`（--now/--abort）/ `retire`（F==0 confirm、可 resume） | S6 | 40（含 mid-retire 中断-resume 臂） |
| `cluster ops ls/show` / **`ops confirm/abort`** | S6 | 40（confirm/abort 由 BLOCKED/STALLED 态驱动） |
| `cluster apply -f`（仅 plan）/ `reconcile nats --plan`（零写） | S6 | 40 顺带（两者分立断言） |
| `recovery node remove --manual` | 既有 | 12（ghost passthrough hermetic） |
| shrink `--to-standalone --confirm-single`（§2.2） | S6 | 41（20 已覆盖 offline-FS 自动路径） |
| force-single OFFLINE（§3.1） | 既有+S6 | 12/20；**杀 peer 前的 peer-alive 拒臂**补进共享 setup |
| force-single ONLINE（§3.0，含 --dry-run） | S6 | 22（§1.4 覆盖债；顺序约束见规格） |
| **force-single 拒绝门**（quorum-not-lost / dwell / peer-alive / typed-confirm / arm token 单发） | S6 | 22 的四拒臂 + 12/20 setup 臂（安全闸零覆盖=false-green 面） |
| **保护模式**（quorum-lost 下 routine 命令全拒，runbook §2.3） | S6 | 22 的保护模式臂 |
| `recovery diagnose`（离线诊断）/ `recovery force-single --guided`（B7 只读引导） | S6 | 42 顺带（与 online --dry-run 不同代码路径） |
| `recovery resnapshot`（raft 丢失重建） | S6 | 42 的 resnapshot 变体 |
| `cluster node-pub`（hidden debug） | **NOT-COVERED** | 纯本地 debug 输出，同 version 类；keygen 兄弟命令已有落点 |
| returning node rejoin（§3 末）/ `rejoin prepare` / `--from-manifest`（§5.3） | S6 | 42（§5.3 全流程） |
| `backup`（online/offline；**follower 默认拒 + `--allow-stale-follower`**）/ `recovery restore`（§5） | S7 | 50（leader 权威 bundle + follower 语义成对臂 + 内容同一性对照 + torn/kill-9 负例） |
| 全灭 DR（§5.2） | S7 | 51 |
| `rotate-tunnel-cert` / account.nk+CA 轮换（§2.1、C7）/ **`retire --compromised --require-credential-rotation`** + `manual:credrot:<node>` NOT-SAFE 告警生命周期 | S7 | 52 |
| `recovery incident export` | S7 | 50 顺带（脱敏 best-effort 断言） |
| `reconcile nats --all/--manual` | 既有 | 13 + init/grow 路径 |
| `cluster upgrade`（G5 全剧本 + 滚动锁互斥 + stale-lock 清除 + skew） | S5 | 30（债） |
| N=2 写栅栏（`--ack-writefence`）/ N≥3 推荐 | S5 | 30 的 N=2 臂 |
| `cluster rebalance proxy [--dry-run]`（手动 verb） | S4 | 74 |
| `seeds publish/show`（含 --remote）/ G3 自动收敛 A/B/C/D | S2/S8 | 82 首次 publish + 91（A/C/D）+ 92(b)（B 臂，软依赖 22） |
| `cluster status`（--json/--watch/**--card**/--offline/--homes/exit taxonomy） | 既有+S8 | 00/10 + 92/93（--card=B7 glance 渲染） |
| `cluster doctor`（secrets 预检 / `--offline`） | S7 | 50 的正向 preflight 臂（**既有 init/grow 路径并不跑 doctor**——先前记「既有」有误） |
| 跨 proto 重装（flag-day，usage §8.3 + runbook §6/§4） | **NOT-COVERED（stretch）** | v1 二进制 GitHub Release 直接可下（v0.3.x 线、install.sh --version 锁版），真实成本=v1 车队基线组建 + 双 proto 兼容矩阵；需求出现再立批 |

### 4.5 横切承诺

| 承诺 | 批 |
|---|---|
| G.1 agent 重连对账 | S9（94） |
| G.2 broker 重启对账（含 DELETING 续跑） | S9（95） |
| G.3 ctl 重连 | S1（60 的 logout/重连臂） |
| **G.5 对账审计**（kind=reconciled/reconciled_closed 落 history） | S9（94）；G.4 为触发矩阵元表（由 94/95 的行覆盖，不单列） |
| #23 Restart=always **判别性**行为证明（clean-exit SIGTERM 直发） | S9（95） |
| 中断注入（传输/PTY/expose 飞行中）+ 网络分区 + 双故障 | S9（96） |
| 长稳 soak（P8 24h 原型的参数化缩放替身，差距显式登记） | S9（97） |
| exit-code taxonomy（B2）抽查 | S8（93） |
| 多 broker 视图一致性（leader 权威） | 既有（10 + status --json fail-closed） |

### 4.6 事件 / 告警面（operator-facing `sys.events` 族与持久告警——架构视为运维契约）

> **完整清单的单一真相源 = `docs/reviews/simcluster-coverage-inventory.md`**（rev4 建档、
> rev5 全量生成；round2 R2-F2 + round3 R3-F2 要求）：§1.1 `pubSysEvent` 全部现有 kind（源码枚举，含 `session_created/
> destroyed`、`member_joined`、`tetherd_restarted`、`agent_registered/evicted/roster_stale`、
> `grow_cutover_revival_failed`、`nats_topology_<action>`、proxy 五 kind…）逐行归批；§1.2
> **架构承诺但无 writer / 命名漂移**的 probe 行（`rotated_pin`/`kicked`/`agent_unregistered`/
> `session_deleting` 广播——probe 而非 covered）；§1.3 rehome/drain 通道 kind；§1.4 `/sub`
> 可用性过渡 **event kind**（`sub_render_empty`/`proxy_no_ready_nodes`/`proxy_partial`——经
> `pubSysEvent` 发射、payload 无 `reason` 字段，断言按 kind 匹配）；§1.5
> store-backed 告警——**kind 枚举与 dedup key 分列**（`manual:credrot:<node>` 是 kind=`manual`
> 的 dedup key，不是独立 kind）。边界声明（round2 遗留问题 2 的裁定）
> 在附录 §0：= H.1 契约 ∪ 全部现有 `pubSysEvent` kind ∪ 专用通道 kind ∪ 告警 kind。本表不再
> 内联复制（避免双份漂移）；各叶 plan 消费并更新附录，收工闸以附录 §3 的生成法重枚举 + diff。

---

## 5. 新 gap 台账与编号

- 新台账：**`docs/deploy-tier-gotchas.md`**，编号自 **#25** 起**全局连续**（#1–#24 留在
  `docs/reviews/v0.4.5-ha-grow-ops-gotchas.md`，两文件头部互链）。全局连续保证 `assert_bug` 的
  gotcha token 与 `[GAP #N]` 标注跨文件唯一。
- **#I\* 过渡族收编**：既有 `#I1`（drill 11 断言的 serve fail-closed 不变量，只登记在 sim
  README）迁册/互链进新台账序言并**关族**——此后不再新增 `#I*` 号，一律用 #25+。
- 每条新 gotcha 记：现象 / 机理（file:line）/ 「该怎么自动化或修」/ 钉住它的 drill 与签名。
- 文档缺陷单列 `DOC-n` 小节，不占 gotcha 号。**预登记**（roadmap 研究期已发现，S 批核实后
  正式立项）：DOC-1 usage §5.15 尾段「cluster 不支持 proxy」与 C5 相悖的残留旧文；DOC-2
  `recovery diagnose`/`resnapshot` 未入 cluster.md/runbook 命令文档；DOC-3 usage §9.3 与
  error_hints 指向不存在的 agent `--upgrade-url-allow` flag（随 S5-31 的 gotcha 一并定格）；
  DOC-4 architecture P8 测试原型的「SIGSTOP broker 期间 agent 起进程」经产品路径不可达
  （start 是 broker 转发的——随 S9-94 的可达造法一并修订原型文本）。
- S 系列结束时，台账即下一个「修复 roadmap」（H 系列?）的输入——与
  `v0.4.5-…-gotchas.md` 之于 G1–G7 完全同构。

## 6. 开工约定

- **每批独立走 CLAUDE.md §3 的 3 阶段 7 步**：Workflow 对抗草拟（固定 agent 数、**模型一律
  ≥ Opus 4.8**——推荐省略 `model` 继承会话主模型）→ 主进程定稿 `docs/reviews/s<N>-plan.md` →
  主进程实现（drill + harness 增量）→ 对抗内审（`s<N>-review.md`，多轮则 `-round2`/`-round3`）
  → 用户外审（`s<N>-external-review.md`；按 G 系列惯例常伴随 `-tasklist` 与多轮 `-round<K>`
  文件）→ **git 收尾按 CLAUDE.md §6 执行**（分支命名与 PR 要求以 §6 原文为准，本文不复制
  具体模式以免与之漂移；若 owner 另行修订 §6 以匹配仓库实践，以届时 CLAUDE.md 为准——本
  roadmap 不越权改流程）。**外审不过不算 done。**
- **实现期在真 server 迭代**：drill 开发用 `./remote.sh drill <name>`（单跑）；批收尾
  `./remote.sh drill-all`（按 OQ-8 的 `-j`/分波策略）一次全绿（已知 RED 除外）。服务器/凭据见
  `docs/devices-ops.local.md §6`。**S 批不触碰 Go 产品代码 → `make test`/`make e2e`/`make lint`
  提交前各跑一次守恒即可**（无产品 diff 时三件套理应零变化——硬闸三件套不因 diff 类型而削减）。
- **每批同步物**：README drill 表 + 编号族注册；`doctor` 若有新 drift 检查；台账新条目；
  `run-drills.sh` 无需注册（自动发现），但新 drill 的预计时长/资源写进头注。
- **RED→GREEN 翻转纪律**（继承 G 系列）：某 gotcha 将来修好 → 对应 `assert_bug` 翻
  `assert_ok`、trailer token 移除——由修复批负责，S 系列 drill 头注写明翻转条件。
- **运行结果术语**（消除「全绿（已知 RED 除外）」歧义）：**已知缺陷运行** = drill 整体
  harness-GREEN、内部以 signature-guarded `assert_bug` 复现已登记 gotcha（这是合格态）；
  区别于 **infra flake**（run-drills 白名单签名，可重跑）与 **unexpected failure**（真回归/
  未登记缺陷，必须停下分诊）。「N 连绿」一律指前者 + 纯 GREEN，绝不含后两者。
- **不可违背**：Mandate ①–④ 全文适用（含 §0.2 反掩盖细则）；drill 一律 `drill_begin`
  throwaway-instance 门；poll_until 不许固定 sleep（既有纪律）；断言「验结果不验退出码」用于
  一切异步收敛；破坏/恢复断言先立注入前基线（§0.4 对照纪律）。

## 7. 开放问题（各批 plan 阶段裁定）

- **OQ-1（S4）**：SS 真流量客户端选型——镜像烘 `shadowsocks-libev`（apt 有）vs vendored Go
  客户端 vs 降级为「AEAD 门 + TCP 连通」断言。倾向 apt 烘入（env 职责，等价真实用户的 Clash）。
  **前提**：正向出口腿必须落在显式 `proxy.allow_private_destinations: true` 的 agent 上
  （桥内目标全是 RFC1918，默认配置按设计必拒——双 agent 分工见 72 规格）。
- **OQ-2（S1）**：容器内忠实复现网络盘 D 状态挂死的可行性（内核 nfsd 依赖宿主模块；fuse 挂起
  非 D 语义）。spike 结论三选一：可行 / 降级语义可接受并显式标注 / NOT-COVERED 留实机。
- **OQ-3（S5）**：https artifact server 的自签 CA 注入容器 trust store 是否越界——预判不越界
  （真实世界 GitHub 的 CA 也在系统 roots 里，信任锚供给=部署职责），plan 里定稿论证。
  **注意**：CA 信任解决的是 TLS 层；agent 侧 URL 白名单是独立的一堵墙（31 的定格对象），
  二者勿混。
- **OQ-4（S6）**：online force-single 的 dwell-bounce（peer 死后 survivor 被 `Restart=always`
  反复拉起、每次 bounce 重置 dwell——12/20 头注记载；**bounce 发生在杀之后**，故「等稳定后再杀」
  不成立）。处置顺序：先判 bounce 在真产品 unit 下的生产可达性——生产可达 → dwell 结构性不可
  达成 → 登 gotcha #25+（产品缺陷）；确证容器特异 → harness 驯服（杀后等 bounce 级联平息 +
  dwell-gate poll 窗参数化加长）。两种出口同权。
- **OQ-5（S2/S4，已裁定）**：well-known manifest 与 `/sub` 两个 HTTP listener **均为产品有意
  的 loopback-only 安全边界**（`clustermanifest/manifest.go:19-33`、`subhttp.go:34-46`
  fail-closed 拒非 loopback；「可配」仅指端口，**绝不改绑/削弱**——rev2 的绑桥地址方案作废）。
  跨容器消费一律经 **S0-ingress 反代替身**——**必须共享 broker 的网络命名空间**（普通 bridge
  容器访问不到他容器的 127.0.0.1；sidecar `--network container:<brk>` 或容器内进程）且
  **terminate 测试 TLS**（invite 拒明文 bootstrap、/sub URL 默认 https；实例 CA + SAN + trust
  注入 + 错误/不受信证书负例；无 ACME/无真域名 ≠ 免 TLS）；handler 正确性（loopback 本机断言）
  与公网 ingress 行为（反代腿）分立，真 Caddy+ACME 公网面仍属 staging/实机（§4.3 行）。
- **OQ-6（S5）**：G5 whole-host 判据需要 broker 容器同机跑 agent（`colocated_agent_nid`）——
  供给方式（brk 容器加 agent unit；sim 用户/身份目录全角色已备）与 session 归属（G5 的
  session-scoped agent leg 前提）。
- **OQ-7（S9）**：96/97 混沌 drill 的确定性——只钉确定性信号（watchdog 清理、audit failed 行、
  重连最终成功、无 panic/FK 签名），时序窗口一律宽容 poll，不断言精确秒数。
- **OQ-8（横切）**：全套 ~37 drill 的并发策略。**约束**：并发 grow 的 timing flake 在 7 drill
  时已存在且随并发 grow 数扩大（§1.1）；新套件组 N≥2 集群的 drill ~19 个。**从 S1 起**每批
  drill-all 即按场景族分波或 `-j` 限并发（如 `-j 6`）；`run-drills.sh` 的 infra-flake 重跑
  只认既有签名，grow-timing RED 按 README CAVEAT 单跑复核。S9 的 3 连基线固化推荐参数。
