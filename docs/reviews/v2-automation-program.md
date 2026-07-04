# v2 自动化补全计划（C1–C7）——落实 proposals 全部非拒绝需求

> 输入：`v2-usability-proposals.md`（原始 7 条建议）+ `v2-usability-proposals-gap.md`（落差，标了每条 ✅/🟡/❌）。
> 目标：把 gap 里**所有 ❌ 和 🟡（非拒绝）需求**拆成独立可交付阶段 C1–C7。**C1–C7 全部完成后，gap 文件里每一项 → ✅，一条不漏。**
> 边界：「需明确拒绝的自动化」5 条**保持拒绝、永不实现**——它们做对的方式就是拒绝。任何阶段都不得把这 5 条变成自动化。

## 完成定义（DoD，全程唯一验收尺）

逐个阶段做完后，重跑 gap 核对（grep 命令/字段/事件真实存在 + 跑验收测试）；**全程结束时 `v2-usability-proposals-gap.md` 的 7 张建议表 + 成功指标表里不得再有 ❌ 或 🟡**（命名差异类 🟡 允许用「别名 + 文档映射」闭合，但功能必须达到验收标准）。5 条拒绝表保持 ✅（拒绝）。

## 工作原则（每阶段都遵守）

1. **一次一个阶段、先父后子**：按 C1→C7 序，每阶段只用前序已完成产物。
2. **每阶段走 CLAUDE.md §3 三阶段**：多专家对抗 plan（`docs/reviews/c<N>-plan.md`）→ 主进程实现+测试 → 多专家对抗内审（`c<N>-review.md`）→ 用户外审（`c<N>-external-review.md`）。外审不过不算 done。
3. **不变量硬约束**（继承 architecture.md / distributed-broker-architecture.md）：
   - 所有自动分发**必须签名 + 单调 generation + 审计事件**（产品原则 3）；agent/ctl 只接受 signed roster，拒随机列表（已就绪的 `clusterroster.VerifyAt` 是基线）。
   - 涉及分裂脑 / 丢弃旧时间线 / credential rotation 的动作**保留 typed confirmation**，永不静默自动化（产品原则 2 + 拒绝表）。
   - 无多数派时进入只读保护，**绝不自动选 broker 写控制面**（拒绝表 #1）。
   - wire 破坏走跨版本路径；非集群 broker 字节等价；新面默认 opt-in 惰性。
4. **每阶段闭合后**：在本文件勾掉对应溯源矩阵行，并在 gap 文件把对应项翻 ✅。

---

## C1 — Agent 在线自动发现（消费 signed roster）

**为什么先做**：生产者侧（broker 签发 `ClusterRoster` + `VerifyAt` + TTL + generation）已在 v0.4.0 就绪，seam 最接近能落；直接闭合成功指标 #2「加 broker 后无需改 agent 配置」——这是用户最痛的一条。

**范围**：
- agent 消费 register-reply 里的 `ClusterRoster`，经 `clusterroster.VerifyAt`（pin account_pub + 单调 generation + 未过期 + identity 匹配）校验后采纳。
- agent 用 roster 的 broker 列表**自动 relearn 自己的 NATS 重连 URL 集**（VOTER 优先，draining 末位/排除）。
- 在线 agent 周期性刷新（加 broker 后 ≤5 分钟收敛）；离线 agent 受 TTL 约束，不无限期阻塞 retire。
- retire/drain 时 roster 把节点标 `draining`，agent 停止对其新连接偏好。
- broker 侧补 `agent_roster_stale` 事件（agent 上报其 roster generation，broker 检测滞后）。

**验收（建议 3）**：加一台 broker 后在线 agent ≤5 分钟自动刷新 roster；离线 agent 不无限阻塞 retire（明确 TTL）；sig 失败/generation 倒退/identity 不符拒更新（消费端真正生效）。**关闭成功指标 #2、#7（消费端接上）。**

**依赖**：无（生产者已就绪）。

## C2 — Agent 入群：bootstrap URL + invite

**范围**：
- `broker_url` 从「完整静态列表」降级为 **bootstrap URL**；agent 存 bootstrap URL + 最近一次有效 seed cache；启动顺序 **cached seeds → bootstrap URL → install-time fallback**。
- broker 暴露 **HTTP well-known signed roster manifest** 端点（`/.well-known/tether/cluster.json`，复用 C1 的签名 roster）。
- `tether cluster seeds publish --bootstrap <url>`（Raft 记 `seed_generation` + public WSS/NATS endpoints）。
- `tether agent join <invite-url> --start`、`tether agent config refresh --once`、`tether agent doctor`。

**验收（建议 3）**：新 agent 只拿一个 invite/bootstrap URL 即可入群，不需手写逗号分隔 broker list；`agent doctor` 能报 roster/连接健康。**关闭成功指标 #3。**

**依赖**：C1（agent 已能消费 + 验证 roster）。

## C3 — Topology Reconciler（自动收敛 NATS 拓扑）

**范围**：
- Raft 保存**期望拓扑 generation**：peer set + 每 broker 期望 NATS route 配置 + public endpoint + 服务状态（`cluster_generation`）。
- leader **只提交 intent**，不远程 ssh/root。
- 每台 broker 本机 **reconciler** 监听 generation，执行本机 `nats.conf` 渲染 → `nats-server -t` 校验 → 原子替换 → 滚动重启（复用现有 `internal/natsconf` 渲染器 + takeover 的 fail-closed 校验）。
- `cluster status` 显示 `desired_generation` / `applied_generation` / `observed_generation`，卡住时给明确下一步。
- 任一 broker 未完成 topology apply 时，集群**不显示 HEALTHY-HA**。

**验收（建议 1）**：新 broker 加入后**不需要人工登录所有 broker 重跑 NATS 配置**；任一 broker 未 apply 不显 HEALTHY-HA；unknown `nats.conf` 指令继续 fail-closed（保持，不得被自动覆盖）。**关闭成功指标 #1 的「拓扑/route 自动收敛」部分。**

**依赖**：无（broker 侧，可与 C1/C2 并行设计；按序列在 C2 后实现）。

## C4 — Cluster Operation Controller（join/retire 可恢复 operation）

**范围**：
- `cluster plan add b4 …` → 生成可审计 plan；`cluster apply <plan-id> --wait` → **执行**（驱动 raft membership + C3 reconciler 收敛）；`cluster reconcile nats --all --wait`。
  - **C4 实施裁定（descope，监工 #2 核实诚实）**：`cluster plan add` / `cluster apply <plan-id>`（建议1 的「可审计解耦」前端）**descope 到 post-C7 backlog**——成功指标 #1「一条 prepare + 一条 approve」已由 ergonomic `cluster join prepare`/`approve` 满足；plan/apply 是同一 operation record 上的可审计性补充（非新能力）。死的 `PLAN_DRAFTED` 面已删，两 gap 行如实留 ❌/🟡。**两行的真实归宿 = 此 backlog**（详 `docs/reviews/c4-review.md` §M10 + `docs/reviews/c-overseer-2.md`）；C-program「一条不漏」DoD 在末尾统一外审时核对此 backlog。
- `cluster ops show <op-id> --json` 升级为**真 operation 日志**（非派生视图），`cluster ops ls` 保留。
- **加入 operation 状态机**：`cluster join prepare`（新机）+ `cluster join approve --wait`（leader），状态 `PREPARED → PREFLIGHT_OK → JOIN_PROOF_VERIFIED → ROSTER_COMMITTED → RAFT_ADDING → CATCHING_UP → NATS_ROLLED_OUT → SERVING`（复用 D7 phase 机 + C3 reconciler 的 `NATS_ROLLED_OUT`）。
- **退役 operation 状态机**：`cluster retire b2 --wait`，状态 `DRAIN_REQUESTED → NO_NEW_HOME → REHOME_EXPOSES → STREAMS_AT_TARGET → SEED_WITHDRAWN → LEADER_TRANSFERRED → RAFT_REMOVED → NATS_ROLLED_OUT → RETIRED`（包住已有的 drain/retire 安全门：F=0 typed-confirm、副本 target、rebuild-OFF 枚举——这些**不弱化**）。
- operation 可恢复（kill-9 后 resume），全程审计事件。

**验收（建议 1+2）**：加 broker = **一条 prepare + 一条 approve**；retire 是可恢复 operation 而非一次性命令块；所有安全边界保留。**关闭成功指标 #1 的「一条 prepare + 一条 approve」。**

**依赖**：C3（reconciler 提供 `NATS_ROLLED_OUT` 收敛 + observed generation）。

## C5 — Proxy Cluster 化

**范围**（建议 4 整条）：
- **控制面入 Raft**：`proxy_enabled`、subscriber set、keyset generation、per-agent proxy allocation 纳入 Raft Apply；写操作要求多数派（无多数派不能新增 subscriber / 开关 proxy / 轮换 keyset）；leader 权威控制面，但不强制数据流量过 leader。
- **数据面**：每 agent 的 `__proxy__` allocation 带 `home_broker` / `epoch` / cert pins（复用 D6 home/epoch 基座）；`/sub/<token>` 可由任意 broker 服务但返回当前权威 home（`server=<home public_host>, port=<proxy public_port>`）；home down 后**自动 rehome `__proxy__`**，订阅客户端下次刷新拿新 home（既有 TCP 可中断，不承诺无损）。
- **命令**：`proxy on --ha-policy freeze-on-quorum-loss`、`proxy status --cluster`（解释每 agent 为何进/不进订阅：`ready`/`tunnel_down`/`keyset_stale`/`no_home`/`catching_up`）、`proxy sub <name>`、`cluster rebalance proxy --wait`。
- **降级语义**：`ACTIVE` / `FROZEN_READONLY`（无多数派冻结只读）/ `DISABLED_NO_QUORUM`（fail-closed）/ `FORCE_SINGLE`（事故模式）。
- **proxy 可观测事件**：`proxy_node_ready/unready`、`sub_render_empty`、`proxy_partial`、`proxy_no_ready_nodes`。
- **拒绝边界**（守住）：无多数派**不自动**「临时选一个中心」继续写 proxy 状态；事故向导（force-home）须 incident 记录 + typed confirmation。

**验收（建议 4）**：cluster 下 `proxy on/sub/status` 可用；杀掉 proxy home broker 后订阅短暂消失或自动切新 home，无需 `proxy off/on`；`proxy status --cluster` 能解释每 agent 的就绪原因。**关闭成功指标 #4。**

**依赖**：D6 home/epoch（已就绪）；建议复用 C3/C4 的 intent+reconciler 与 operation 模式。

## C6 — Expose/Rehome 可观测补全 + 状态命名对齐 + 恢复命令别名

**范围**（建议 5 剩余 + 建议 6/7 收尾）：
- **建议 5 事件补全**：`home_reassign_started/succeeded/failed`、`rehome_stalled`、`broker_down_rehome_summary`（`expose_rehomed` 已有；proxy 相关事件在 C5）。全部脱敏（无 token/PSK/private key/session secret）。
- **建议 5 字段补全**：`last_rehome_at`、`reconnects`（`home_broker/epoch/state/public_url/ready_reason` 已有）。
- **建议 5 命令**：`cluster status --homes`（聚合所有 expose 的 home/epoch/ready_reason）。
- **建议 6 状态命名对齐**：把现有 `HEALTHY_HA/DEGRADED/QUORUM_LOST/FORCE_SINGLE` 对齐到文档 `HEALTHY-HA / DEGRADED-WRITABLE / READ-ONLY / FORCE-SINGLE / NOT-HA`（NOT-HA = N≤2 能工作无生产 HA，从 verdict 提升为一等状态）；保持 `--json` schema 稳定（加映射、不破坏旧 consumer）。
- **建议 7 命令别名**：`cluster recovery diagnose --offline` / `recovery force-single --confirm-peers-dead` / `recovery rejoin prepare --dump-divergent` 作为现有 `force-single --guided` / `recover --guided` / `recover --emit-manifest` 的别名（功能已达，仅 CLI 命名/分组对齐文档），并在 usage/runbook 写明映射。

**验收**：建议 5 的 8 类事件齐全且脱敏；`cluster status --homes` 可用；建议 6 五状态齐备；建议 7 命令名与文档一致（或别名 + 映射文档）。

**依赖**：C5（proxy 事件先落，C6 补 expose/rehome 非 proxy 事件 + 命名收尾）。

## C7 — Compromised-node credential rotation + staging 演练工具

**范围**（建议 2 剩余 + P2 #3/#4）：
- `cluster retire <node> --compromised --require-credential-rotation`：把 `--compromised` 从「一句文字提醒」升级为**真流程**——进入 account.nk / cluster-CA / route-key / tunnel-cert rotation 助手（typed confirmation，不把 retire 当安全边界）。守住拒绝表 #5（retire compromised 后**不默认**安全已恢复）。
- **staging 演练工具**：定期/按需验证 quorum loss → recover 流程（注入 broker kill、断 quorum、跑 force-single/recover 向导、断言 severe alert 直到 N≥3 + streams at target），作为回归网。

**验收**：`--compromised` 触发完整 rotation 流程（不复制私钥到别机——拒绝表 #2）；演练工具能复现 quorum-loss→recover 并断言安全不变量。

**依赖**：无（独立，置末）。

---

## 全需求溯源矩阵（防漏闸——每个 gap 的 ❌/🟡 必须指到一个阶段）

> ✅ 项（已实现）不在此表。下表覆盖 gap 里**全部 ❌ + 🟡**；执行时每行做完就勾。

| gap 来源 | 需求项 | gap 状态 | 落实阶段 |
|---|---|---|---|
| 建议1 | `cluster plan add` | ❌ | **post-C7 backlog**（C4 descope，见下注）|
| 建议1 | `cluster apply <plan-id> --wait` 执行 | 🟡 | **post-C7 backlog**（C4 descope，见下注）|
| 建议1 | `cluster reconcile nats --all --wait` | ❌ | C3/C4 |
| 建议1 | `cluster ops show` 真 operation 日志 | 🟡 | C4 |
| 建议1 | Raft 存期望 NATS route / generation | 🟡 | C3 |
| 建议1 | leader 提交 intent + 本机 reconciler 执行 | ❌ | C3 |
| 建议1 | 每 broker 本机 reconciler 自动渲 nats.conf + 滚动重启 | ❌ | C3 |
| 建议1 | status 显示 desired/applied/observed_generation | ❌ | C3 |
| 建议1 | 验收：加 broker 不需人工重跑 NATS | ❌ | C3 |
| 建议1 | 验收：未 apply 不显 HEALTHY-HA | 🟡 | C3 |
| 建议2 | `cluster join prepare` / `approve` | ❌ | C4 |
| 建议2 | 加入状态机 PREPARED→…→SERVING | 🟡 | C4 |
| 建议2 | `cluster retire --wait` operation 化 | 🟡 | C4 |
| 建议2 | 退役状态机 DRAIN_REQUESTED→…→RETIRED | 🟡 | C4 |
| 建议2 | `retire --compromised --require-credential-rotation` | ❌ | C7 |
| 建议3 | broker_url → bootstrap URL | ❌ | C2 |
| 建议3 | `cluster seeds publish --bootstrap` | ❌ | C2 |
| 建议3 | `agent join <invite> --start` | ❌ | C2 |
| 建议3 | `agent config refresh --once` | ❌ | C2 |
| 建议3 | `agent doctor` | ❌ | C2 |
| 建议3 | HTTP well-known signed roster manifest 端点 | 🟡 | C2 |
| 建议3 | agent 存 bootstrap+cache，启动 cached→bootstrap→fallback | ❌ | C2 |
| 建议3 | 在线 agent 自动刷新 roster | ❌ | C1 |
| 建议3 | retire 标 draining + agent 消费 | 🟡 | C1 |
| 建议3 | 验收：新 agent 只拿 invite 入群 | ❌ | C2 |
| 建议3 | 验收：加 broker 后 agent 5 分钟自动刷新 | ❌ | C1 |
| 建议3 | 验收：离线 agent 不无限阻塞 retire | 🟡 | C1 |
| 建议4 | proxy 控制面纳入 Raft Apply | ❌ | C5 |
| 建议4 | 数据面 proxy home_broker/epoch/cert pins | ❌ | C5 |
| 建议4 | `/sub/<token>` 任意 broker 返回 home | ❌ | C5 |
| 建议4 | home down 自动 rehome `__proxy__` | ❌ | C5 |
| 建议4 | `proxy on --ha-policy` / `status --cluster` / `sub` / `cluster rebalance proxy` | ❌ | C5 |
| 建议4 | 降级 ACTIVE/FROZEN_READONLY/DISABLED_NO_QUORUM/FORCE_SINGLE | ❌ | C5 |
| 建议4 | 验收：cluster proxy 可用 + 杀 home 自动切 | ❌ | C5 |
| 建议5 | `cluster status --homes` | ❌ | C6 |
| 建议5 | 字段 last_rehome_at / reconnects | 🟡 | C6 |
| 建议5 | 事件 home_reassign_*/rehome_stalled/broker_down_rehome_summary | ❌ | C6 |
| 建议5 | 事件 agent_roster_stale | ❌ | C1 |
| 建议5 | 事件 proxy_node_ready/unready、sub_render_empty、proxy_partial、proxy_no_ready_nodes | ❌ | C5 |
| 建议6 | 状态命名 HEALTHY-HA/DEGRADED-WRITABLE/READ-ONLY/FORCE-SINGLE/NOT-HA | 🟡 | C6 |
| 建议7 | `cluster recovery diagnose --offline` 命令名 | 🟡 | C6 |
| 建议7 | `recovery force-single --confirm-peers-dead` 命令名 | 🟡 | C6 |
| 成功指标#1 | 加 broker = 一条 prepare + 一条 approve | ❌ | C3+C4 |
| 成功指标#2 | 加 broker 后无需改 agent 配置 | ❌ | C1 |
| 成功指标#3 | 新 agent 只需一个 invite/bootstrap URL | ❌ | C2 |
| 成功指标#4 | cluster 下 proxy 可用 + 降级 | ❌ | C5 |
| 成功指标#7 | 自动分发有 generation/签名/审计（消费端） | 🟡 | C1 |

**核验：gap 里的每个 ❌/🟡 都在上表出现一次。上表全部勾掉 = 所有需求落实。**

## 阶段 ↔ 成功指标 ↔ 建议 覆盖总览

| 阶段 | 主要关闭 | 关闭成功指标 |
|---|---|---|
| C1 Agent 在线自动发现 | 建议3 消费端 | #2、#7 |
| C2 Agent 入群 bootstrap/invite | 建议3 入群端 | #3 |
| C3 Topology reconciler | 建议1 reconciler | #1（收敛部分） |
| C4 Operation controller | 建议1 commands + 建议2 operation | #1（一条 prepare/approve） |
| C5 Proxy cluster 化 | 建议4 全 | #4 |
| C6 可观测补全 + 命名对齐 | 建议5 剩余 + 建议6/7 收尾 | —（验收类） |
| C7 Credential rotation + 演练 | 建议2 剩余 + P2 | —（安全/回归） |

执行序列：**C1 → C2 → C3 → C4 → C5 → C6 → C7**（先父后子；C1/C2 是 agent 线，C3/C4 是 broker 拓扑线，C5 是 proxy，C6/C7 收尾）。每阶段完成后停下等用户外审/确认再进下一阶段。
