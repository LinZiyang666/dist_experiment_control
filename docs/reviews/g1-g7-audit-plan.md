# G1–G7 收官横切审计 — 定稿 remediation plan

> 范围：G1–G7 五个 commit 的生产代码（`177e3d4^..d93d842`）。这是各 G 各自过审后的**横切**质量审计（找残余 bug / 异味 / 复杂逻辑 / 效率 / 注释 / stale / 安全）。
> 阶段 A（本文件）：多专家对抗性审计 Workflow（10 finder + 逐条 refute-verify，28 agent）→ 主进程综合定稿。
> 产物：27 唯一 finding → **14 actionable + 9 cosmetic + 4 refuted**。主进程逐条裁定如下。
> **约束尺**：核验器已把多条"建议修法会引入回归"钉死——本 plan 一律采纳**核验器修正后的安全修法**，不用 finder 原始建议。

日期：2026-07-09。定稿人：主进程。

---

## 0. 裁定总表

| # | 文件:行 | 类别/严重度(校准) | 裁定 | 安全修法（已避开核验器指出的回归） |
|---|---|---|---|---|
| A1 | broker/cluster_grow_cutover.go:48 | correctness / medium | **FIX** | Stage-A 探测容忍瞬时错误（重试）；Stage-B SIGKILL 前三态复探（clustered→不 bounce / up-standalone→SIGKILL / down→只等 systemd 复活） |
| A2 | tether/cluster_upgrade.go:208 | correctness / medium | **FIX** | 签名 roster fetch 重试数次；落到 responder fallback 且 >1 broker 时**响亮 WARN**（不 fail-closed——会废掉 pre-G3 集群升级） |
| A3 | broker/alert_reconcile.go:216 | test-gap / medium | **FIX(test)** | 补 non-10008 清除路径的测试（force-single N=1 永不降级的唯一 JS-503 清除路径） |
| A4 | tether/cluster_add_drive.go:247 | correctness / low | **FIX** | findJoinOp 改 (string,error)；仅 `Code==CodeNodeUnknown` 返回 ""；瞬时错误 HALT-retry，不 fork 出新 nonce |
| A5 | tether/cluster_add_drive.go:44 | correctness / low | **FIX** | dry-run 抑制 webhook（只影响两处 preflight halt；保留只读 preflight 以打印计划） |
| A6 | broker/cluster_grow_cutover.go:245 | concurrency / low | **FIX** | hardRestartNatsServer 用 exec.CommandContext+超时（从 Background 派生，回调链无 ctx） |
| A7 | tether/cluster_upgrade.go:172 | correctness / low | **FIX** | node-list RPC/decode 失败 **fail-closed**（否则全机误判 stale→整轮空转+多余 leader transfer）；保留"agent 缺席"→not-at-target 语义 |
| A8 | clusteroffline/offline.go:477 | correctness / low | **FIX** | 离线 force-single seed drop 若把集合清空（全指向死 peer）→**响亮 WARN**（与在线 sibling 打平；不改 INV-2 floor 行为） |
| A9 | broker/proxy_rebalance.go:196 | correctness / low | **FIX** | rehome/rebalance 目标集排除 `broker_draining` 标记节点（对齐 allocatePort 的 DrainingNodes 门；读错→按无 draining 处理，不全停） |
| A10 | broker/proxy_auto_rebalance.go:115 | efficiency / low | **FIX** | 仅当 `returned>0 或 pending 非空`才算 gatesClear（省掉稳态每 tick 3 次 DB 读） |
| A11 | broker/transfer.go:543 | efficiency / low | **FIX** | push 准入把 ceiling 算一次传入 bucket（消除每次 push 两次 AccountInfo 往返）；pull 路径不变 |
| A12 | broker/cluster_upgrade_trigger.go:116 | correctness / low | **FIX** | reexec-agent 在 `ColocatedAgentNID==""` 时回退 `b.selfID`（与 detect/plan 的 node_id==nid 约定一致；selfID=本机已验证 node_id，非广告字段，符合 g5 安全注记）；同步改门禁测试 |
| C1 | tether/cluster_add_drive.go:277 | cli-ux / low | **FIX** | --auto-confirm-catchup 仅在**进入** BLOCKED 边沿花预算（跟踪 prevBlocked），不再每 poll 烧一次 |
| C2 | tether/cluster_add_drive.go:535 | comment / low | **FIX** | 删掉 verifyClusterSeam 被取代的旧首段 doc（"presence-only"误导） |
| C3 | broker/cluster_grow_cutover.go:205 | cli-ux / low | **FIX** | moveAside 幂等 no-op 分支返回已算出的 `backup` 路径而非 ""（resume 保留 restore 提示） |
| C4 | clusteroffline/offline.go:676 | stale / low | **FIX** | dumpTable 注释改指 `dumpableTables`/sqlite_master（`applyOwnedTables` 已不存在） |
| C5 | broker/seed_converge.go:37 | stale / low | **FIX** | DeriveSeedEndpoints doc 改指 `cluster.MaxSeedEndpoints`（`maxDerivedSeedEndpoints` 已不存在） |
| C6 | broker/proxy.go:1022 | smell / low | **FIX** | 默认视图 cluster 模式把 Ready 也按 home 健康门控（消除 Ready=true 却无 endpoint 的自相矛盾行） |
| C7 | tether/node_versions.go:57 | deadcode / low | **FIX** | 删死代码 `if !ok { row.AgentVer="?" }`（orQ 已产出 "?"） |
| C8 | proto/alerts.go:8 | comment / low | **FIX** | 版本账目补 v3（G5/G7b 字段批）——现从 2 跳到 4，v3 缺失 |
| C9 | adminsock/protocol.go:42 | comment / low | **FIX** | 更正 OpBrokerUpgradeReload 的"Local-socket-only"注释（实际只经进程内签名 NATS 触发，不在 clusterOps 路由表） |
| A13 | broker/cluster_upgrade_trigger.go:156 | concurrency / low(uncertain) | **DEFER** | 见 §2。已是文档化的 follow-up（single-active-op lock）；reject-when-set 会废掉 resume，owner/nonce 需 wire 改动；当前行为安全（幂等收敛、绝不去集群化） |
| A14 | broker/seed_converge.go:177 | correctness / low(uncertain) | **DEFER(+test)** | 见 §2。行为修法会回归 VIP 保护；唯一正解（provenance KV）反转文档化 OQ-B 设计=maintainer call；可达性窄。**只加一条钉住当前"已知取舍"的测试，不改行为** |
| R1 | tether/cluster_offline.go:195 | — | **REJECT** | 无自动化路径（confirmTypedNodeID 拒非交互 stdin）；人工在场必见 WARN。severity 不成立 |
| R2 | tether/serve.go:143 | — | **REJECT(*)** | "静默分叉"不成立（Preflight ENOENT 会响亮报出，Applied 不前进）。仅"unrecognized directive"措辞对缺文件不精确=可选小改，见 §2 |
| R3 | broker/alert_reconcile.go:221 | — | **REJECT** | 是 g7-review §M1 刻意的 inv-6 no-stuck-ACTIVE 决策（4 轮外审 Pass）；建议修法会重引已修的 stuck-ACTIVE bug |
| R4 | cluster/membership_ops.go:338 | — | **REJECT** | 两个 live 调用方都结构性排除 self（ReadRoster `!= ?`；RemoveNode 对 self inCfg=true 先拒）；不可达。加 self-exclusion 需 selfID 参数=为理论链增复杂度，违安全实用主义 |

合计：**21 处 FIX（12 actionable + 9 cosmetic）+ 1 DEFER-with-test（A14）+ 1 DEFER（A13）+ 4 REJECT**。

---

## 1. FIX 逐条实现规格

### A1 — cutover 对健康 clustered broker 的误 SIGKILL（medium）
`performGrowCutover` Stage-A 只在 `probeNatsClusterName()` `err==nil && name!=""` 才判 AlreadyDone；一次瞬时 /varz 探测错误（3s 超时 / DisableKeepAlives / reload 抖动）就落到 Stage-B，仅凭**盘上** `own.IsClusteredJetStream()` 无条件 SIGKILL 活着的 clustered nats（cutoverBroker 设计上重试 6 次，第 2 次极易撞进 systemd 复活窗）。
- 新增 `probeNatsClusteredTolerant() (clustered, reachable bool)`：重试 `cutoverProbeRetries=3` 次、间隔 `cutoverProbeRetryDelay=500ms`；`err==nil`→立即返回 `(name!="", true)`（clustered/standalone 都不重试），持续 error→`(false,false)`。
- Stage-A 用它：clustered→AlreadyDone。
- `restartAndVerifyClustered` **SIGKILL 前**三态复探：clustered→`OK{AlreadyDone}`（不 bounce）；reachable-standalone→SIGKILL（真复活）；unreachable→**跳过 SIGKILL**、只 poll `cutoverGraceTimeout` 等 systemd `Restart=always` 复活（永不静默返回 OK：不 clustered 就响亮 `growCutoverRevivalFailed`）。
- 保住三条既有语义：真 cutover（apply 落盘、nats 仍跑旧 standalone=reachable）照 SIGKILL 复活；revival-failed（down）照 poll+响亮报错；健康 clustered 不再被瞬时错误弹掉。
- **测试**：新增对 `restartAndVerifyClustered`/probe 三态决策的单测（把决策抽成可测纯函数 `cutoverRestartDecision(clustered, reachable)`）。**部署面**：改动 cutover 路径，release 前建议跑 `sim drill 10-grow-to-3`/`11-grow-gaps` 复核（happy path 行为不变）。

### A2 — 升级 roster fetch 瞬时失败降级到 fail-OPEN（medium）
`buildUpgradeNodes` 仅当 `fetchManifestOverNATS` 返回非空 roster 才走签名/absent-voter-fail-closed 安全路；fetch 对任意瞬时失败返回 nil → 落到 responder-only fallback（不验签、不 fail-closed、按应答者数算 N2WriteFence）。**不能**按"多 broker 应答就 fail-closed"（会废掉真 pre-G3 集群升级——核验器）。
- 安全修法：`buildUpgradeNodes` 里对 `fetchManifestOverNATS` **重试**（3 次、间隔短）再判；仍拿不到 roster 且 `len(replies)>1`（真多 broker）→ 向 `out` 打**响亮 WARN**："signed roster unavailable, planning over responders; a momentarily-absent voter cannot be detected — verify all voters present before proceeding"。
- `buildUpgradeNodes` 加 `out io.Writer` 形参；RunE 里把 `out := cmd.OutOrStdout()` 上移到调用前。
- **测试**：对 responder-fallback 的 WARN 触发做 table 断言（抽 `shouldWarnResponderFallback(replies)` 纯判定）。

### A3 — JS-503 non-10008 清除路径无测试（test-gap）
`alert_reconcile.go:216-223` 的 else 分支（non-10008 error → 清 jsDownSince + SetJSUnavailable(false)）是 force-single N=1 leader（永不降级）唯一的 JS-503 清除路径，现有测试从不驱动它。补一个独立 case：sustained-10008 raise 后喂 non-10008 error（如 `&jetstream.APIError{ErrorCode:10059}`）、`leader` 全程 true，断言 `jsFlag==false && jsDownSince.IsZero()`。纯补测，零行为改动。

### A4 — findJoinOp 把瞬时探测失败当"无 op"（low）
`findJoinOp` 现 `err!=nil || resp==nil || !resp.OK → ""`，把 transport 错误与真无 op 混同；resume-after-cutover（leader nats 刚 SIGKILL 重连）时误返 "" → 重新 prepare 出新 nonce → StartJoinOperation 以"另有 op 在飞、请先 abort"拒绝，误导操作员销毁健康进度。
- 改 `findJoinOp(...) (string, error)`：仅 `resp!=nil && resp.Code==adminsock.CodeNodeUnknown` 返回 `("",nil)`（真无 op，走正常 prepare）；`err!=nil` 或其它非 OK → 返回 error。
- caller（driveAdd:102）把该 error 经 `haltAdd(webhook,"find-join-op",...)` 转成"retry"，不 fork。

### A5 — dry-run 仍可 POST halt webhook（low）
P0 preflight 在 dryRun 短路之前；preflight 失败经 `haltAdd`→`notifyGrow(webhook,"halt")` 发真 HTTP POST，违背 --dry-run"touching nothing"。修：driveAdd 入口 `if dryRun { webhook = "" }`（dry-run 下唯一可达的 webhook 就是这两处 preflight halt；保留只读 preflight 以打印计划——不能把 dryRun 短路提到 preflight 前，那会丢掉 leader/formerN1 计划渲染）。

### A6 — hardRestartNatsServer 无 context/超时（concurrency low）
对齐 sibling `reloadNatsServer`（C3-m9 有超时）：`exec.CommandContext(ctx, bin, "--signal","stop")`，`ctx` 从 `context.WithTimeout(context.Background(), topoReloadTimeout)` 派生（回调链无 ctx 可用）+ `defer cancel()`。只界定发信号；45s verify poll 仍按设计同步（注释标注）。

### A7 — node-list RPC 失败→全机误判 stale（low）
`buildUpgradeNodes` 吞掉 node-list 的 marshal/RPC/unmarshal 全部错误→agentRelease 空→每机 AgentVer="" → AtTarget=false → 本已全 at-target 的集群被规划成整轮（多余 leader transfer + 锁抖动）。修：对 **transport/decode 失败 fail-closed**（返回 `unavailErr("node-list RPC failed (%v) — refusing to plan on incomplete data ...")`）。**保留** node-list 成功但某 agentNID 缺席→""→not-at-target（这是 plan.go:16 记载的合法"未配对 agent 触发升级"语义，不动）。

### A8 — 离线 force-single seed 可只剩死 peer（low）
`convergeSeedsDropHosts` 的 INV-2 floor（`len(filtered)==0` 保留原集）在"所有已发布 seed 恰好都是被弃 peer"时保留整套死端点，冷启动客户端只拨死主机；在线 sibling（seed_converge.go:183）此时有 WARN，离线无。修：`len(filtered)==0 && len(endpoints)>0`（真把集合 drop 空）时 `logger.Warn` 明示"published seeds now point only at departed brokers; run `cluster seeds publish` on the survivor"。仅告警、不改 floor 行为（synthesize-self 在 self 无 dialable host 时不完备，弃）。**需给 pruneRosterPeers/convergeSeedsDropHosts 传 logger**（现签名无 logger——见实现注意）。

### A9 — rehome/rebalance 目标不排除 draining 节点（low）
`eligibleProxyHomes`/`pickProxyRehomeTarget` 只按 `phase='VOTER' AND cert_fp!='' AND public_host!=''` 选目标，不查 `cluster.DrainingNodes`；DrainNode 先升 `broker_draining` 标记+migrateExposes、后翻 phase，窗口内节点仍 VOTER，auto-rebalance 的 gatesClear（只查 NonTerminalOperations，plain drain 非 operation）不挡→可能 rehome 到刚清空的 draining 节点。修：两处目标选择都过滤 `cluster.DrainingNodes`（对齐 allocatePort:550 的门）；**读错→按"无 draining 信息"放行不过滤**（目标集若全 fail-closed 会因一次瞬时读错停掉所有 rehome——核验器）。

### A10 — gatesClear 稳态每 tick 3 次无用 DB 读（efficiency low）
`driveAutoRebalanceOnReturn` 每 5s tick 无条件算 `forceSingleActive+noInflightOps+recentProxyRehome`，但 gatesClear 只在 `ready`（有 pending dwell 满足）时被读。修：`gatesClear := false; if len(returned)>0 || len(b.autoRebalanceArm.pending)>0 { gatesClear = ... }`（arm 单 goroutine 独占，读 pending 安全；不能只按 `returned>0`——armed 节点跨 tick 无新 return 也要评估门）。默认关（TETHER_AUTO_REBALANCE=on 才进），影响本就微，纯省。

### A11 — push 准入每次两次 AccountInfo 往返（efficiency low）
`handlePushReq` 先 `xferBucketMaxBytes(ctx)` 做 size 准入、再 `ensureXferBucket` 内部**又**算一次（各含一次 `AccountInfo` 活网往返）。修：handlePushReq 算一次 ceiling→做准入→把 maxBytes 传入新内部 helper `ensureXferBucketSized(ctx,sid,replicas,maxBytes)`（跳过内部再算）；`ensureXferBucket` 保留给 pull 路径（不做准入、不应被迫算/传 ceiling），可委托 `ensureXferBucketSized`。保住 G6 #21 refuse-on-too-small 语义。

### A12 — reexec-agent 对缺 colocated_agent_nid 硬拒→中断整轮（low）
detect/plan 两侧对空 `colocated_agent_nid` 都回退 `node_id==nid`（serveconf 文档记载可选），action 侧硬拒 CodeBadRequest → 文档化合法配置的一键 roll 中途 HALT（broker 已 reload、agent 仍旧版、锁仍持）。修：handler `ColocatedAgentNID==""` 时回退 `b.selfID`（本机已验证 node_id、非广告字段，符合 g5"绝不按广告 nid fan-out"注记）转发到 `SubjCmdForwarded(sid, b.selfID, "upgrade")`；**同步改** `TestUpgradeTriggerReexecAgentNoNID`——从"断言 CodeBadRequest"改为"断言已过配置门、无 nc 时得 CodeClusterNotEnabled(broker not connected)"（证明不再在配置门硬拒）。加注释说明 selfID 安全性。真正无 agent 的纯 broker 主机仍会在 agent 腿 `agent_no_responders` HALT（比原 bad_request 不差，且明示；part-2 driver-skip 有掩盖瞬时之虞，不做，记为考量）。

### C1 — --auto-confirm-catchup 按 poll 而非按 block 事件烧预算（cli-ux low）
`waitJoinServing` 每次 poll 见 BLOCKED 就 `confirms++`+发 confirm-op；同一卡住状态几秒烧光 N。修：循环外 `prevBlocked bool`；仅 `OpState=="BLOCKED" && !prevBlocked`（**进入** BLOCKED 边沿）才 confirm+计数；每轮末更新 prevBlocked。语义回到 flag 文档的"N 次不同 catch-up stall"。

### C2 — verifyClusterSeam 叠了两段 doc，首段描述被取代的旧行为（comment）
删 535-538 那段"carries a broker.cluster seam (raft_addr set)"（被 539-545 的全元组 fail-closed 描述取代），只留准确段。

### C3 — resume 的 cutover 报空 BackupPath（cli-ux low）
`moveAsideJetStreamStore` 幂等 no-op 分支（backup 已存在 / sentinel 存在）返回 `("",nil)`，丢失 restore 提示。修：这两个分支返回已算出的 `backup`（`storeDir+".grow-bak."+epoch`），resp.BackupPath 始终指向真实备份目录。

### C4 — dumpTable 注释引不存在的 applyOwnedTables（stale）
改注释指向 `dumpableTables`（运行时枚举 sqlite_master）；安全性质（表名非用户输入）不变。

### C5 — DeriveSeedEndpoints doc 引不存在的 maxDerivedSeedEndpoints（stale）
改指 `cluster.MaxSeedEndpoints`（Stage-C n23 已统一到该 SSOT）。

### C6 — 默认 proxy status 渲染 Ready=true 却无 endpoint（smell）
默认视图（proxyStatusNodes）cluster 分支 `Ready: r.ready==1` 用裸列，而 PublicPort/PublicHost 按 `proxyHomeHealthy(home)` 门控；home 不健康时出 Ready=true/无端点的矛盾行（与 --cluster 视图、/sub 渲染都不一致）。修：cluster 模式且 `home!="" && !proxyHomeHealthy(home)` 时把该行 Ready 置 false，使 Ready 与本视图内 vended 的 port/host 自洽（"可达出口"语义；不重构去调 proxyReadyFor 以免扩大 Ready 语义）。

### C7 — node_versions 死代码（deadcode）
删 `if !ok { row.AgentVer = "?" }`——上一行 `orQ(av)` 对 `av==""`（含 !ok）已产出 "?"。补一个 table 测试钉住 not-present→"?"。

### C8 — ClusterHealthSchemaVersion 版本账目缺 v3（comment）
头注释从 v2 直跳 v4，六个 `(schema v3)` 字段无账目。补一行枚举 v3（G5/G7b 批：ProxyHomeCount/ProxyHomeReported/JetStreamUnavailable/CommandVer/ColocatedAgentNID/IsVoter）。无 consumer gate 于 SchemaVersion（仅 omitempty additive 解码），纯文档。

### C9 — OpBrokerUpgradeReload 注释谎称"Local-socket-only"（comment）
它不在 `clusterOps` 路由表，socket 发它得 `unknown op`；实际只经进程内账号签名 NATS 触发（cluster_upgrade_trigger 直调 HandleCluster）。更正注释为"reachable only via the in-process signed NATS upgrade trigger（not the raw socket）"。

---

## 2. DEFER / REJECT 论证（供外审复核）

- **A13（升级锁不互斥两个并发 roll）DEFER**：`cluster_upgrade.go:27-28` 已明记"single-active-op lock"为 follow-up。核验器：reject-when-set 会废掉"HALT 后再跑 resume"（drive 故意留锁靠 re-run UPSERT 自愈）；owner/nonce 需给 ClusterUpgradeReq+marker 加 per-roll 身份（wire 改动，升级无 grow 的 joiner-id 那样的天然身份）。当前行为安全：两 roll 收敛到同 target、reload/roll 幂等、绝不去集群化、account-seed 门控。**不改**，记为已知 follow-up。
- **A14（provenance-free clobber guard 全死集）DEFER + 加钉测**：可达性窄（`DeriveSeedEndpoints` 常态含 self，force-single survivor=self 恒在集合内，≤8 broker 正常路径已由 `TestG3SeedShrinkConvergesDropsStale` 证明正确）；触发需运维手动发布不含未来 survivor 的子集且无中途收敛，或 >8 voter（本项目 2–5 voter 不现实）。行为修法（全死集就 rebuild）会**回归 VIP 保护**（VIP floor 定义上就是 host 不匹配任何 broker，与全死集 host-match 不可区分）；唯一正解=provenance KV 反转文档化 OQ-B 决策=maintainer call。**只加一条钉住当前"已知取舍"的 table 测试 + KNOWN-TRADEOFF 注释，不改行为**；是否引入 provenance 留外审/后续定夺。
- **R1/R3/R4 REJECT**：见 §0 表。R4 若加 self-exclusion 需给 `PlanClusterNodePrune` 加 selfID 参数——为不可达的理论链增复杂度，违[安全实用主义]。
- **R2 REJECT(\*)**：核验器澄清"静默分叉"不成立（Preflight 对缺文件 ENOENT 会以 `ActionUnknownDirective` 响亮报出、Applied 不前进）。唯一真缺陷是**措辞**——缺文件被报成"unrecognized directive"。**可选**小改：若 `natsconf.Preflight` 的 ReadFile 命中 `os.IsNotExist` 则给出"nats.conf not found at <path>（install.sh 是否跑过？nats_conf_path 是否正确？）"的清晰 reason。实现阶段若干净则顺手做，否则略（不进硬门）。

---

## 3. 测试与硬闸

- 每处 FIX 就近补/扩 table 或纯函数测试（A1 决策纯函数、A2 warn 判定、A3 清除路径、A4 findJoinOp 分类、A5 dry-run 抑制、A7 fail-closed、A8 warn、A9 draining 排除、A10 gatesClear 门、A11 单次 ceiling、A12 门禁测试改写、C1 边沿计数、C6 Ready 门、C7 not-present、A14 钉测）。
- 提交前硬闸：`make test` + `make lint` 全绿；并发面（A6/A9/A10）过 `-race` + 内建 NumGoroutine/fd 泄漏门。**A1 触碰 cutover 部署面**：release 前建议 `sim drill 10-grow-to-3`（happy path 行为不变，属分内 deploy-tier 复核）。
- 阶段 C：多专家对抗性复核这批修改（是否落地正确、有无回归/新异味）→ 主进程评估修整 → 外审停下。
