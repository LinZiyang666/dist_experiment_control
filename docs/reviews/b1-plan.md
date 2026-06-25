# B1 plan（定稿）— 用户可见的集群状态 + 安全错误叙事

> Stage-A：9×Opus 对抗 workflow（4 起草 → 4 互评 → 1 综合）。主进程定稿人已独立据实核验全部中心前提（`permissions.go:68` member 已授 `cluster-health.req`、`cluster.apply.>` 仅 broker → 零 ACL 改动；`ClusterHealthResp` 无 voter count/roster；`home_catching_up`/`leader_unavailable` 实为 agent 内部消费、不到 ctl；`try_again` 确从 `expose.go:72,97` 到 ctl；alert.go 拨号模板存在）。**采纳综合 plan**，无重大改动。

## 中心设计抉择（与定稿人读码结论一致）

- **ctl-status 复用 `tether.v2.ctrl.by.<actor>.cluster-health.req`**——无新 subject、无新 ACL、不碰 auth_callout（不触发"本地先证"门）。
- **ctl 只能拿轻量 `ClusterHealthResp`**（writable-leader / stale / force-single / NodeID / AppliedIndex），**没有 voter count/roster/streams**。故 verdict 分两处、两套数据源：socket 路给**权威 voter-count 判定**；ctl 路给**reachability 判定**（"N 台 broker 应答"），绝不冒充权威 voter 数。
- ctl 摘要结构体 **ctl-local（`cmd/tether`）**，不上 wire（`internal/proto` 是 wire SSOT）。
- 新增字段全 additive，**`statusSchemaVersion` 不 bump**；`is_leader_view` **无 omitempty**（false=非 leader 必须序列化）；`verdict`/`view_host` 用 omitempty。
- 富 roster 表 + 操作员逃生命令仍 **socket-gated**（broker host）；ctl 拿"用户级摘要"。
- **N=1 字节等价**：单 broker 不挂 responder → `probeClusterHealth` 零应答 → ctl 输出"no cluster detected / 单机受支持"、exit 0；guard 测试在 `make test` 里（非 gated）。

## Item 1 — ctl 可达 `cluster status`（over NATS）
- `cmd/tether/cluster.go newClusterStatusCmd`：加 `--remote`（+ `--nats-url`/`--home`，对齐 alert.go）。**不自动探测**（拒绝 `--via auto` 静默降级 operator 路）。RunE 分支：`offline` → 旧 offline；`remote` → 新 `clusterStatusRemote`；else → 旧 `callAdmin`（**operator 路字节等价**）。
- 新文件 `cmd/tether/cluster_status_nats.go`，纯 `os.Exit`-free cores：
  - `type ctlClusterSummary struct{ BrokersSeen int; WritableLeaderSeen bool; LeaderID string; ForceSingleActive bool; AllStale bool; AnyReply bool }`
  - `summarizeClusterHealth(replies) ctlClusterSummary`（**按非空 NodeID 去重**；fold ForceSingle/WritableLeader/AllStale[镜像 EvalDestructiveGate]/LeaderID）
  - `ctlVerdictLine(s) string` / `ctlExitCode(s) int` / `renderCtlStatus(w, s, jsonMode)`（无 os.Exit）
  - `clusterStatusRemote`（唯一调 os.Exit）：alert.go 模板拨号 → `probeClusterHealth(nc, id.PublicKey)` → summarize → render → `os.Exit(ctlExitCode(s))`。
- **ctl exit-code（比 socket 薄；ctl 无 roster → 永不 exit 1/DEGRADED）**：零应答→**0**；WritableLeaderSeen→0；ForceSingleActive→3；有应答&&!writable&&AllStale→2(只读)；有应答&&!writable&&!AllStale(选举中)→0+瞬时提示。

## Item 2 — 列图例（仅 operator socket 表）
- `renderClusterStatus` `tw.Flush()` 后、health 行前，打图例块（纯 render，无 struct 改）。标注"broker-host operator view"。`ACCT.NK` 诚实注明"当前恒 Y、per-node 校验尚未接线"（真校验 DEFERRED）。

## Item 3 — 白话判定（按 voter 数）
- **A. socket 路（权威）**：`ClusterStatusReport` 加 `Verdict string json:"verdict,omitempty"`；`StatusReport` 在 `computeHealth` 后据真实 `voters` + `streamsAtTarget`（所有行 `StreamTarget==0 || StreamActual>=StreamTarget`）算：
  - `voters<=1`：`1 broker — NO redundancy: if it dies, the cluster stops until restored.`
  - `voters==2`：`2 brokers — NO fault-tolerant writes: losing either makes the cluster read-only (quorum needs both).`
  - `voters>=3 && streamsAtTarget && health==HEALTHY_HA`：`HA active — writes are replicated; the cluster survives <F> broker failure(s).`（F=`ProjectQuorum(voters).FaultTolerance` 字面数）
  - `voters>=3 && (!streamsAtTarget||degraded)`：`HA configured but DEGRADED right now — see the table; full redundancy is not in place.`
  - `voters==0`：空。
  `renderClusterStatus` 在 health banner 下渲染。
- **B. ctl 路（reachability，非 voter 数）**：`ctlVerdictLine(s)`——零应答/force-single/writable≥3/writable(1–2)/!writable&&AllStale(只读)/!writable&&!AllStale(选举中) 各一句（措辞见综合 plan，强调"权威计数在 leader 上")。

## Item 4 — `view_host` + `is_leader_view`
- `internal/adminsock/protocol.go` `ClusterStatusReport` 加 `ViewHost string json:"view_host,omitempty"` + `IsLeaderView bool json:"is_leader_view"`（**无 omitempty**）。additive、不 bump schema。
- `StatusReport` 填 `rep.ViewHost = a.node.SelfID()`、`rep.IsLeaderView = (leaderID == a.node.SelfID())`。
- `renderClusterStatus` verdict 行后：leader→`view: authoritative (leader <host>).`；else→`view: this is <host>'s local view; re-run on the leader (<leader|unknown>) for the authoritative verdict.`
- ctl 路无 struct 字段，render 恒打"remote view aggregated from N brokers; leader holds authoritative verdict"。

## Item 5 — gateDestructive 改写
- 抽纯函数 `gateBlockMessage(gate proto.DestructiveGate) string`；`gateDestructive` 改为 `EvalDestructiveGate`→`Blocked()||ackAlerts` 早返→`errors.New(gateBlockMessage(gate))`。
- 新文案：transient/persistent 分列、**不含 `force-single`/`recover`**、不引用 runbook 节标题为可点命令、用 "condition" 非 "alert"（避免误导去 `alert ack`）、每条给 `→` 下一步（quorum_lost→等 30s 重试 / 查 `tether cluster status` + runbook §3；force_single_active→等待无用、需在 broker host 恢复冗余）。`--ack-alerts` 7 处 call site 不变。

## Item 6 — 泄漏 token 词条（诚实范围）
- `cmd/tether/error_hints.go brokerCodeHints` 加 3 条：`home_catching_up`/`leader_unavailable`（**注明当前不到 ctl、为防御性预注册 + 读 log 友好**）+ `try_again`（**真到 ctl 的那条**）。措辞 failover 框架、绝不暗示用户操作了什么。不加进任何"每 code 必映射"guard。
- 文档：§9.4 expose 表 + 新 §9.7.1 瞬时码小节。

## Item 7/8 — 文档
- §1/§2.3：**单机 broker 是默认且完整支持**的显著 callout（中文）。
- `cluster-runbook.md` 新 §0 + usage §5.6：平面语言"什么是 cluster / quorum"（英文 runbook / 中文 usage）。
- §5.6 另加：列图例表 + 判定表（标 socket 权威）+ view-source 说明 + ctl `--remote` 说明（用户摘要、非 8 列表、operator verb 仍 socket-gated）+ offline exit-code 厘清。

## 测试（对抗）
- **`make test` 单测**：`summarizeClusterHealth` 去重表（含**重复 NodeID 不冒充 quorum**、空 NodeID、not-stale-mixed=选举中非 quorum-lost）；`ctlExitCode` 表（零应答→0 是头条）；`ctlVerdictLine`/`renderCtlStatus`（含 --json）；`gateBlockMessage` golden（regex 拒 `force-single|recover`、require `cluster status`+runbook §3+`--ack-alerts`+"condition"；ackAlerts override 仍 nil）；socket verdict（voters∈{0,1,2,3,5} + streams-below-target 接线案例）；`is_leader_view` 序列化（false 出现、view_host omitempty、v1 前向兼容 unmarshal、schema_version 仍 1）；follower `is_leader_view=false` 分支（若 `RaftConfiguration()` 不可 stub 非自 leader，则扩 harness 属 B1）；`brokerCodeHints` 内容；**ACL 回归**（`PermissionsForActivatedMember` 含 `cluster-health.req`、不含 `cluster.apply.>`/`cluster.cursor.req`）。
- **`make test` 字节等价 guard**：嵌入式 NATS 无 responder → ctl core 打"no cluster detected"、exit 0、无 panic（**必须在 `make test`**）。
- **gated `test/d9`（`-tags d9_integration`，复用、不新建 tag）**：真 routed cluster `--remote` writable 判定；transfer-leader 选举窗口→"选举中"非假 HA（不用固定 sleep）；minority 分区→只读非误 force-single；force_single_active→ctl flag + gateBlockMessage + `--ack-alerts` 端到端。

## 实现顺序
1. 抽 `gateBlockMessage`（解锁 A4）。
2. adminsock `ClusterStatusReport` +3 字段 → `StatusReport` 填 ViewHost/IsLeaderView/Verdict（最底层）。
3. ctl 纯 cores（`cluster_status_nats.go`）。
4. `--remote` RunE 接线（消费 3）。
5. `renderClusterStatus` 图例/verdict/view footer（消费 2）。
6. `brokerCodeHints` +try_again（独立叶子）。
7. 文档（**末位、与 CLI 串同 commit**，逐字引用）。
8. 测试随码落 + gated 末位。

## DEFERRED（明确）
- NATS 上的完整 per-node roster（需 `ClusterHealthResp` 加字段 + schema bump + 新 responder 数据 = 真 wire 改）。
- `statusSchemaVersion` bump / 监控契约改 → B2。
- ctl 更细 exit taxonomy（ctl 不出 DEGRADED）→ B2。
- `home_catching_up`/`leader_unavailable` 真 ctl 暴露路径（B1 仅 docs + 防御 hint）。
- 真 ACCT.NK 账户密钥校验（当前恒 true）。
- status `--watch`/poll → B5。
- `--via auto` socket 可拨自动探测（拒绝，静默降级 operator 路）。

## 触碰文件
**码**：`cmd/tether/cluster.go`、新 `cmd/tether/cluster_status_nats.go`、`cmd/tether/d8_alerts.go`、`cmd/tether/error_hints.go`、`internal/adminsock/protocol.go`、`internal/broker/clusterstatus.go`。
**测**：`cmd/tether/cluster_status_nats_test.go`(新)、`cmd/tether/d8_alerts_test.go`、`cmd/tether/error_hints_test.go`、`internal/broker/clusterstatus_test.go`、`internal/adminsock/*_test.go`、`internal/auth/permissions_test.go`、`test/d9/clusterstatus_remote_test.go`(新, d9_integration)。
**档**：`docs/usage.md`(§1/§2.3/§5.6/§9.4/§9.7.1)、`docs/cluster-runbook.md`(新 §0)。
**不碰（字节等价/ACL 稳定证明）**：`internal/proto/{subjects,alerts,messages}.go`、`internal/auth/permissions.go`、`internal/broker/{cluster_health,expose}.go`。无新 subject、无 ACL/auth_callout 改、无 proto wire-break、无 schema bump。
