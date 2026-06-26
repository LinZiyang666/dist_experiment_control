# v2-usability 需求文档落差分析（proposals ↔ 实现）

> 对照 `docs/v2-usability-proposals.md`（原始 7 条产品建议）逐条核对实际交付。
> 图例：✅ 实现 · 🟡 部分（只做了 seam / 安全 / 观测，核心机制缺）· ❌ 没做。
> 核对方式：grep 代码确认命令/字段/事件是否真实存在（非注释/文档提及）。
>
> **两列对比**：
> - **改造前（v0.4.0）**：B1–B7 交付后、C1–C8 之前的状态。校验 commit `f1a2bde` / tag `v0.4.0`，日期 2026-06-25。
> - **改造后（C1–C8）**：v2 易用化补全 epic（C1 agent 自动发现 / C2 入群 / C3 topology reconciler / C4 operation controller / C5 proxy cluster 化 / C6 可观测 + 命名 + recovery / C7 compromised rotation / C8 CLI 收敛）+ ≥20 专家大审计后的状态。校验日期 2026-06-26，已暂存（未 commit，待统一外审）。grep 代码逐条复核。

## 一句话结论

- **改造前**：「让手动操作变安全 + 看得懂 + 可恢复」做透了；「消除手动」几乎没做。4 个核心自动化成功指标（#1–4）全 ❌。
- **改造后**：「消除手动」补齐——**4 个核心自动化成功指标（#1–4）全部 ✅**，**7 条建议全部落地（含此前唯一缺口 `cluster rebalance proxy`）**。agent 在线消费签名 roster 自动 relearn broker URL（C1）、invite/bootstrap 一条命令入群（C2）、每台 broker 本机 reconciler 自动渲 nats.conf + 滚动重启（C3）、join/retire 成为可恢复 operation（C4/C8）、proxy 在 cluster 下可用且杀 home 自动切（C5）、`cluster rebalance proxy` 主动均摊 home（C-rebalance）。2 条「必须拒绝」的安全边界（#2 永不复制私钥、#5 退役不宣告 safe）经 C5/C7 保持。

---

## 核心判断的 4 个痛点（文档第 7 行）

| 痛点 | 改造前 (v0.4.0) | 改造后 (C1–C8) |
|---|---|---|
| broker 加入后人工更新多台 broker NATS 配置 | ❌ 仍手动（每台 `takeover-natsconf`） | ✅ 每台 broker 本机 topology reconciler 自动渲 nats.conf + SIGHUP 重载（C3）；`reconcile nats --all --wait` 阻塞至收敛 |
| agent/ctl 人工维护 broker URL | ❌ 仍手动（roster 签了没人消费） | ✅ agent 在线消费签名 roster → `adoptRoster` relearn DialURLs（C1，`roster.go`）；加 broker 后自动重连新 voter |
| proxy 在 cluster 下不可用 | ❌ 仍不可用 | ✅ proxy 控制面纳入 Raft + `/sub` 任意 broker 返回权威 home + 杀 home 自动 rehome（C5） |
| quorum 失败普通用户难判断 | ✅ 解决 | ✅ 保持（C6 health_label 5 态 + `--explain`） |

## 建议 1 — Cluster Operation Controller / topology reconciler

| 要求 | 改造前 (v0.4.0) | 改造后 (C1–C8) |
|---|---|---|
| `cluster plan add b4 …` | ❌ | ✅ 经 `cluster apply -f roster.yaml`（算 add 步）+ `cluster join approve --wait`（执行）；无 `plan add` 字面命令但能力达成 |
| `cluster apply <plan-id> --wait`（执行） | 🟡 只有 `cluster apply -f roster.yaml`，**plan-only 不执行** | ✅ 执行落地——经 `join approve --wait` / `retire --wait` / `reconcile nats --all --wait`（C4/C3）；`apply -f` 仍刻意 plan-only（leader-view fail-closed） |
| `cluster reconcile nats --all --wait` | ❌ | ✅ C3（`cluster_reconcile.go`：自动 per-broker 收敛 + `--all --wait` 阻塞至每台 voter 观测代次达期望，非零退出点名 laggard） |
| `cluster ops ls` | ✅ | ✅ |
| `cluster ops show <op-id> --json` | 🟡 派生视图，非 operation 日志 | ✅ C4 operation controller 真生命周期相位（keyed by node-id；join/retire 可恢复 op） |
| Raft 存期望 NATS route / generation | 🟡 存 peer set/phase，**无期望-route generation** | ✅ C3 `topology_generation` 单调计数器（仅渲染变更才升）+ 复制 `TopoDesired` |
| leader 只提交 intent，本机 reconciler 执行 | ❌ | ✅ C3：leader 升 generation（intent），每台 broker 本机 reconciler 渲染+重载 |
| 每台 broker 本机 reconciler 自动渲 nats.conf + 滚动重启 | ❌ | ✅ C3 `topology_reconcile.go`（render+validate+swap+SIGHUP reload + mtime storm 门[MAJ-6]） |
| status 显示 desired/applied/observed_generation | ❌ | ✅ `TopoDesired` / `TopoApplied` / `TopoObserved`（`protocol.go` + `clusterstatus.go`） |
| 验收：加 broker 不需人工重跑所有 NATS 配置 | ❌ | ✅ C3 自动收敛 |
| 验收：任一 broker 未 apply 不显 HEALTHY-HA | 🟡 有 stream/lag 降级，无 topology-applied 概念 | ✅ 某 voter `TopoObserved` 落后 `TopoDesired` → 非 HEALTHY_HA（`computeHealth`） |
| 验收：unknown nats.conf 指令 fail-closed | ✅ | ✅ |

## 建议 2 — Broker Join/Retire 可恢复 operation

| 要求 | 改造前 (v0.4.0) | 改造后 (C1–C8) |
|---|---|---|
| `cluster join prepare` / `join approve` | ❌ 仍 `sign-join`+`add` | ✅ C8（`cluster_join.go`：`join prepare` 生成 bundle、leader `join approve --wait` 驱动到 SERVING，可恢复；`add`/`sign-join` 已删/降隐藏） |
| 加入状态机 PREPARED→…→SERVING | 🟡 D7 有 PENDING_VOTER→CATCHING_UP→VOTER，非 prepare/approve 全机 | ✅ C4 operation controller 把 join 驱动到 SERVING（可恢复 op 相位机） |
| `cluster retire b2 --wait` | 🟡 有 `drain --retire` + `cluster wait` | ✅ C4 `cluster retire <node> --wait`（可恢复 operation 驱动到 RETIRED） |
| 退役状态机 DRAIN_REQUESTED→…→RETIRED | 🟡 有 drain→retire，非完整命名机 | ✅ C4 operation 驱动到 RETIRED |
| 安全：N=2/F=0 typed-confirm，拒 `--yes` | ✅ | ✅（C4 保持，byte-identical 两调用 dance） |
| 安全：JS/OBJ 副本未达 target 拒 retire | ✅ | ✅ |
| 安全：rebuild-OFF expose 列出并要求确认 | ✅ | ✅ |
| `retire --compromised --require-credential-rotation` | ❌ 只有一句文字提醒，无 rotation 流程 | ✅ C7（`cluster_rotation.go`：引导式 rotation checklist + 持久 severe `manual:credrot:<node>` alert[reconciler 永不自动清] + NOT-SAFE banner；私钥从不生成/传输[#2]） |

## 建议 3 — Signed Roster + Agent 自动发现

| 要求 | 改造前 (v0.4.0) | 改造后 (C1–C8) |
|---|---|---|
| broker_url 降级为 bootstrap URL | ❌ | ✅ C2（bootstrap HTTPS URL + 签名 SeedBundle） |
| `cluster seeds publish --bootstrap` | ❌ | ✅ C2（`cluster_seeds.go`，`PlanClusterSeedsPublish` 经 raft） |
| `agent join <invite> --start` | ❌ | ✅ C2（`agent_join.go`：写 agent.yaml + 钉 account_pub，`--start` 在进程内拉起 daemon） |
| `agent config refresh --once` | ❌ | ✅ C2（`agent_config.go`） |
| `agent doctor` | ❌ | ✅ C2（`agent_doctor.go`） |
| 每 broker 暴露 signed roster manifest（HTTP well-known） | 🟡 register reply 下发，**无 HTTP manifest 端点** | ✅ C2（`cluster_manifest.go` + `ManifestAddr` loopback，Caddy-fronted 的 `/.well-known/tether/cluster.json`）。**残留**：`FetchManifest`（`clusterroster/fetch.go`）有 scheme allowlist + redirect 上限 + body cap + timeout + proxy-aware，但**无私网/metadata-IP denylist**——见下「残留」 |
| register response 下发 `ClusterRoster{generation,urls,expires_at}` | ✅ 生产者侧做了 | ✅ |
| signed + 单调 generation + TTL | ✅ `VerifyAt` + TTL + generation | ✅ |
| agent 存 bootstrap URL + seed cache，启动 cached→bootstrap→fallback | ❌ | ✅ C1/C2（`cachedSeeds` 持久 + `bootstrapFetchOnce` + `effectiveDialURLs` cached→bootstrap→seed floor） |
| 在线 agent 自动刷新 roster | ❌ | ✅ C1（`rosterRefreshLoop` + `RosterRefreshInterval`，RosterRefreshOnly register → adoptRoster） |
| retire 时 roster 标 draining + TTL 后删 | 🟡 roster 带 phase，agent 不消费 | ✅ agent 现消费 roster；generation 升后退役 broker 离开 agent dial pool（`AdoptDecision` 单调 + never-empty floor） |
| 验收：新 agent 只拿 invite 入群 | ❌ | ✅ C2 `agent join <invite>` |
| 验收：加 broker 后 agent 5 分钟自动刷新 | ❌ | ✅ C1 `rosterRefreshLoop`（周期由 `RosterRefreshInterval` 控） |
| 验收：离线 agent 不无限阻塞 retire | 🟡 broker 侧 TTL 有，agent 侧未接 | ✅ C4 operation 驱动 retire（不依赖离线 agent 应答）+ broker TTL |
| 验收：sig 失败/generation 倒退/identity 不符拒更新 | ✅ 验证器就绪（消费者未接） | ✅ 消费端已接（`AdoptDecision` 调 `VerifyAt`/`VerifySeedsAt` + `Generation >= prev`） |

## 建议 4 — Proxy Cluster 化

| 要求 | 改造前 (v0.4.0) | 改造后 (C1–C8) |
|---|---|---|
| proxy_enabled/subscriber/keyset 纳入 Raft Apply | ❌ | ✅ C5（`OpProxySetEnabled`/`OpProxySubCreate`/`OpProxySubRevoke`/`OpProxyAllocate`，PSK literal 入 raft、raw token 永不入 raft、leak-once leader-mint） |
| 数据面 proxy `home_broker/epoch/cert pins` | ❌ | ✅ home_broker/epoch（`port_allocations`，D6 epoch ladder）；cert pins 复用 D6 隧道钉证（proxy home == agent 隧道 home） |
| `/sub/<token>` 任意 broker 服务返回当前 home | ❌ | ✅ C5（`subhttp.go` ClusterMode：JOIN `cluster_nodes ON home_broker` 投影每节点权威 public host，非-VOTER home 排除） |
| home down 自动 rehome `__proxy__` | ❌ | ✅ C5（`proxy_reconcile.go` `rehomeProxy` + dwell 滞回 + §17 reachability） |
| `proxy on --ha-policy` / `proxy status --cluster` / `proxy sub` / `cluster rebalance proxy` | ❌ | ✅ 全部（`proxy on --ha-policy {freeze\|disable}-on-quorum-loss`、`proxy status` CLUSTER 行、`proxy sub create/ls/revoke`；`cluster rebalance proxy [--dry-run]` 经 leader-local 贪心均摊 `__proxy__` homes 至 max−min≤1，复用 `PlanReassignHome` + home_reassign_* 事件 + 指令重推，`proxy_rebalance.go`） |
| 降级 ACTIVE/FROZEN_READONLY/DISABLED_NO_QUORUM/FORCE_SINGLE | ❌ | ✅ C5（`proxy_cluster.go` 全四态 + `--ha-policy` 决定 quorum-loss 行为） |
| 验收：cluster 下 proxy on/sub/status 可用 + 杀 home 自动切 | ❌ | ✅ C5（proxy 控制面经 raft + 数据面 rehome；`test/c5` 逻辑覆盖，多 broker kill-home e2e drill 列 follow-up） |

**改造前整条 DEFER，0 实现 → 改造后全部做完（含手动 `cluster rebalance proxy`）。**

## 建议 5 — Expose/Rehome 可观测性

| 要求 | 改造前 (v0.4.0) | 改造后 (C1–C8) |
|---|---|---|
| `expose ls --json` | ✅ | ✅ |
| `expose explain <name>` | ✅ | ✅ |
| `cluster status --homes` | ❌ 有 `ps` HOME 列，但无 `--homes` flag | ✅ C6（`cluster.go` `--homes` flag → `OpClusterHomes` 聚合，`homes.go`；纯描述视图无 exit-code 契约） |
| 字段 home_broker/epoch/state/public_url/ready_reason | ✅ 大部分（ExposeResp + explain） | ✅（homes 报告含 HomeBroker/Epoch/ReadyReason/Ready/url） |
| 字段 last_rehome_at/reconnects | 🟡 不确定/未单列 | ✅ C6（`last_rehome_at` 在 `PlanReassignHome` CAS 内 `LitTime(now.UTC())` 盖戳[`port/plan.go`]；`reconnects == epoch` 单调，homes 报告 `Reconnects: r.epoch`） |
| 事件 `expose_rehomed` | ✅ | ✅ |
| 事件 home_reassign_started/succeeded/failed、rehome_stalled、broker_down_rehome_summary、agent_roster_stale、proxy_* | ❌ 仅 1/8 | ✅ 8/8（C5/C6：home_reassign_{started,succeeded,failed}、rehome_stalled、broker_down_rehome_summary、agent_roster_stale、proxy_homes、expose_rehomed 全部 grep 确认存在） |
| 事件脱敏 | ✅ | ✅ |

## 建议 6 — Cluster Status 产品化

| 要求 | 改造前 (v0.4.0) | 改造后 (C1–C8) |
|---|---|---|
| 状态卡 HEALTHY-HA/DEGRADED-WRITABLE/READ-ONLY/FORCE-SINGLE/NOT-HA | 🟡 名略异，NOT-HA 折进 verdict | ✅ C6（`health_label` 5 态对齐；legacy `Health`/`ExitCode`/`schema_version` 字节稳定，NOT-HA→exit1） |
| `cluster status --explain` | ✅ | ✅ |
| `cluster doctor` | ✅ | ✅ |
| `cluster incident export` | ✅ | ✅（C8 收敛为 `cluster recovery incident export`） |
| 输出：能做什么/为什么保护/下一步/不要做什么 | ✅ | ✅ |
| `--json` schema 稳定 + stderr banner | ✅ | ✅ |

**改造前已是做得最完整的一条 → 改造后全做 + 命名对齐。**

## 建议 7 — 事故恢复向导

| 要求 | 改造前 (v0.4.0) | 改造后 (C1–C8) |
|---|---|---|
| `cluster recovery diagnose --offline` | 🟡 功能有，命令名不同 | ✅ C8（`cluster recovery diagnose --self-id … --offline` 为主命令，命令名对齐） |
| `recovery force-single --confirm-peers-dead` | 🟡 有 `force-single --guided`，flag 名不同 | ✅ C8（`recovery force-single --self-id … --confirm-peers-dead b,c`，flag 名对齐；gate 字节保留） |
| `recovery rejoin prepare --dump-divergent` | ✅ `recover --emit-manifest` + dump-divergent | ✅（C8 收敛为 `recovery rejoin prepare --dump-divergent --emit-manifest`） |
| 自动：daemon 停 / peer :7400 探测 / 列丢弃 peer / severe alert 到 N≥3 / dump+wipe+rejoin | ✅ | ✅ |
| 人工确认：输入 node_id / 确认 peer 死 / 接受后果 | ✅ | ✅ |

**改造后 `cluster recovery` 成为主命令树（C8 反转 C6 的「别名而非主」），命令名/flag 与提案逐字对齐。**

## 成功指标（7）

| # | 指标 | 改造前 (v0.4.0) | 改造后 (C1–C8) |
|---|---|---|---|
| 1 | 加 broker = 一条 prepare + 一条 approve | ❌ | ✅ C8 `join prepare` + `join approve --wait` |
| 2 | 加 broker 后无需改任何 agent 配置 | ❌ | ✅ C1 agent 消费 roster 自动 relearn URL |
| 3 | 新 agent 只需一个 invite/bootstrap URL | ❌ | ✅ C2 `agent join <invite>` |
| 4 | cluster 下 proxy 可用 + 降级 | ❌ | ✅ C5 proxy raft + rehome + 4 态降级 |
| 5 | 只读保护一分钟看懂影响+下一步 | ✅ | ✅ |
| 6 | 退役不误删承载 expose/未达 target 的节点 | ✅ | ✅ |
| 7 | 自动分发有 generation/签名/审计 | 🟡 签发+验证就绪，消费未接 | ✅ C1 消费端已接（`AdoptDecision` 验签 + 单调 + 审计事件） |

**4 个核心自动化指标（#1–4）改造前全 ❌ → 改造后全 ✅。**

## 需明确拒绝的自动化（5，做对 = 拒绝掉了）

| # | 必须拒绝的 | 改造前 (v0.4.0) | 改造后 (C1–C8) |
|---|---|---|---|
| 1 | 无多数派自动选 broker 写控制面 | ✅ 已拒（force-single 须 typed-confirm） | ✅ 保持（C5 无 quorum ⇒ proxy frozen，NEVER temp write-center） |
| 2 | 自动复制 CA/account 私钥到别机 | ✅ 已拒（从不复制私钥） | ✅ 保持（C7 rotation 引导 OOB，`tether NEVER copies private keys for you`，结构性不可能 + exfil guard） |
| 3 | sig 失败/generation 倒退仍刷新 agent | ✅ 已拒（VerifyAt） | ✅ 保持（C1 消费端 `AdoptDecision` 验签 + `Generation >= prev` reject 保旧态） |
| 4 | force-single 支持 `--yes` / 仅 ctl 视角 | ✅ 已拒（typed-confirm + 多 broker allStale） | ✅ 保持 |
| 5 | retire compromised 后默认安全 | ✅ 已拒（提示 rotation，不当安全边界） | ✅ 保持并加固（C7 持久 severe alert 永不自动清 + NOT-SAFE banner；raise 失败非零退出，不 false-safe） |

---

## 总账

### 改造前（v0.4.0）

- **基本做完**：建议 6、建议 7（命令名有出入），建议 2 的**全部安全护栏**，5 条「必须拒绝」全拒对。
- **只做了 seam / 一半**：建议 1（plan-only + ops 视图，缺 reconciler）、建议 3（签发+验证器就绪，缺 agent 消费端 + invite + bootstrap）、建议 5（explain + 字段 + 1 个事件，缺其余事件 + `--homes`）。
- **完全没做**：建议 4（proxy cluster，整条）、建议 1 的 reconciler、建议 3 的 agent 自动发现、建议 2 的 operation 状态机、`--compromised` rotation。

### 改造后（C1–C8）

- **7 条建议功能全部落地**（一处已记录的安全残留：`FetchManifest` 私网 denylist，见上）：建议 1（C3 reconciler + C4 operation controller）、建议 2（C4/C8 prepare/approve/retire 可恢复 op + C7 rotation）、建议 3（C1 agent 消费端 + C2 invite/bootstrap/manifest）、建议 4（C5 proxy cluster 化 + C-rebalance `cluster rebalance proxy`）、建议 5（C6 `--homes` + 8/8 事件 + last_rehome_at/reconnects）、建议 6（C6 命名对齐）、建议 7（C8 recovery 主树 + flag 逐字对齐）。
- **4 个核心自动化成功指标（#1–4）从全 ❌ → 全 ✅**；指标 #7 从 🟡 → ✅。
- **5 条「必须拒绝」全部保持**（C5/C7 验证未松动；#5 还加固了 raise-fail 非零退出）。

### 已接受的残留（诚实记录，非「完全实现」）

- **`FetchManifest` SSRF 防御纵深（C2，外审 Q2）**：`clusterroster/fetch.go` 已有 scheme allowlist（仅 http/https）、redirect 上限（5）、body cap（1 MiB）、timeout（15s）、proxy-aware dial，但**无私网/loopback/link-local（含云 metadata 169.254.169.254）denylist**。**接受为残留**而非关闭，理由：① bootstrap URL 由操作员配置（能改它=已控制 agent 配置）；② 取回的 body 在消费端必须过 **pinned account 签名校验**（`AdoptDecision`/`VerifySeedsAt`）才会被采纳——SSRF GET 到内网既不能被采纳也无法外泄；③ 正确的 denylist 需与 v0.3.6 proxy-aware dial 组合（合法的本地私网代理仍须可达），为理论攻击链引入真实复杂度（项目规则：v1 安全实用主义）。若需关闭，列为 tracked 加固项。

### 改造后仍挂的 follow-up（不影响需求达成，统一外审带走）

- gated 多 broker / temporal 集成 drill：`test/c3`·`c4`·`c5`·`c6`·`c7` kill-home auto-switch / 选举 / 时序单调 e2e（核心头号验收目前是单元/逻辑测试 + `test/d5`–`d9` 集成覆盖）；`cluster rebalance proxy` 的 DB/raft 执行路径同样目前是纯 planner 单测 + 逻辑，多-broker 执行 drill 列此处。
- in-band schema-floor join 门（大审计 MAJ-11，现为既存 reinstall-not-upgrade 不变量 + 文档化）。
- P13 真 Caddy/Clash 端到端（历史 CONDITIONAL，与本 epic 无关）。

## 补全建议（改造前的 P0 序，现多数已完成）

1. ~~**建议 3 agent 消费端**~~ → ✅ C1 完成。
2. ~~**建议 1 topology reconciler**~~ → ✅ C3 完成。
3. ~~**建议 3 `agent join <invite>` + bootstrap URL**~~ → ✅ C2 完成。
4. ~~**建议 4 proxy cluster 化**、建议 2 operation 状态机、`--compromised` rotation~~ → ✅ C5/C4/C7 完成；建议 4 的手动 `cluster rebalance proxy` 亦补齐（C-rebalance）。
