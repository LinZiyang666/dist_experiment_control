PASS

# cluster phase-transition fluidity — external review

当前第 4 轮外部复审结论：**PASS**。初审发现 **5 个 High、2 个 Medium**；第二轮复审发现
**3 个 High、2 个 Medium**；第三轮复审发现 **1 个 High、1 个 Medium**。主进程第四轮回复后，
独立验证确认 R6 abort-during-retire side-effect fence 与 R7 architecture drift 已关闭；R2/R3/R4
与三条 R5 gated drill 仍保持通过。正文保留历轮 Fail 证据，末尾“第 4 轮外部复审”是当前结论。

Reviewer role: independent external reviewer. The plan and two internal-review reports were used only
as leads; all material claims were checked against code, project contracts, the pinned Raft behavior,
and executable tests.

## Scope boundary

审查开始时暂存区为空。对象是当时 `git status --short` 显示的 21 个未暂存 tracked 修改和
6 个 untracked 文件；本报告与 3 个外审回归文件是审查者新增内容。已逐 hunk 检查修改，并
向外追到 membership operation、automatic topology reconciler、signed roster、mixed-release
FSM apply、production transport 和运维文档。

## Tasklist / review surface

- [x] 从 `CLAUDE.md`、requirements、architecture、distributed-broker 基线、runbook/usage 和
  历史外审报告重建项目契约；不采信内部 disposition。
- [x] 确认 staged/unstaged/untracked 边界，检查全部 changed hunk、调用方和遗漏消费者。
- [x] 审查新 replicated ops 的 allowlist/decode/applier、SQL bake、literal safety、generation、
  idempotency、mixed-version 与 proto/migration 兼容。
- [x] 审查 raft rebind 的 leader/self-only/active-op/collision/validation、双权威写序、部分失败、
  transport bind/advertise、重启与 admin/CLI 路径。
- [x] 审查 NATS route rebind 的 URL authority、敏感信息、roster/topology generation、自动
  reconcile 和组合命令部分成功语义。
- [x] 审查 nonvoter join 的 staging/promotion、barrier、timeout/abort/cleanup、resume、无 wedge
  性质和并发 membership 排斥。
- [x] 审查 status/doctor 的权威来源、leader scope、地址处理、schema/severity 以及 grow gate。
- [x] 审查 clustered-JS→standalone 的 N=1 机器门、fail-open、持久状态、自动 reconciler、
  store/reset、部分失败、回滚和安全操作说明。
- [x] 对照 finalized plan 和架构不变量，核对明确 in-scope 机制、生产接线、guards、heavy
  drills、race/leak 门与文档。
- [x] 增加独立外审回归，验证疑似缺陷，不修改实现代码。
- [x] 运行 diff/build/vet/lint/tidy、targeted/race、full test 和相关 gated/e2e 门；如实记录
  失败或未完成项。
- [x] 形成最终 Fail 报告，写明疑问、问题、建议和复审门；按用户要求暂存全部文件。

## Findings

### F1 — High: unreachable nonvoter 进入 `CATCHING_UP` 后无法在线清理

`driveJoin` 在 `AddNonvoter` 成功后把 roster phase 改为 `CATCHING_UP`，catch-up 超时只把
operation 改为 `BLOCKED`，不改变 substrate
(`internal/broker/cluster_operation_controller.go:441-507`)。`ops abort` 又明确只终止 operation、
不碰 membership。此后文档推荐的 `cluster recovery node remove --manual` 调到 `RemoveNode`，
但它只接受 `RETIRING` / `VOTER_ADD_FAILED`，明确拒绝 `CATCHING_UP`
(`internal/broker/clusterdrain.go:186-221`)。

这不是只有文案不一致：fresh bundle 也不能修正地址。`PlanClusterNodeUpsert` 的 conflict update
仅允许原 phase 为 pending/add-failed；`CATCHING_UP` 会 RowsAffected=0，因此新 bundle 中的
`raft_addr` 不能替换旧的不可达地址 (`internal/cluster/membership_ops.go:82-132`)。于是 routine
failed-grow 最终仍可能需要 force-single，正好违背本 feature 的目标。

外审回归
`TestExternalFailedNonvoterJoinCanBeRemovedOnline` 稳定失败，实际错误是：

```text
node is CATCHING_UP; raw remove only finishes a RETIRING or VOTER_ADD_FAILED node
```

Required remediation: 为 failed join 建立可恢复清理 transition，并同时正确处理 topology。
不能简单把 phase 改成 `VOTER_ADD_FAILED` 后复用现有 delete：该节点已在 `CATCHING_UP` 时进入
mesh，而现有 add-failed delete 故意不 bump `topology_generation`
(`membership_ops.go:280-297`)。修复必须保证 `RemoveServer`、roster delete、roster generation、
mesh-leave topology generation 和 operation terminal/resume 全部一致，并覆盖 timeout、abort、
kill/restart、corrected-address retry。

### F2 — High: destructive `--to-standalone` 在无法证明 N=1 时 fail-open

`runReconcileToStandalone` 只有在 admin status 成功且数出 `voters > 1` 时拒绝；socket 不可达、
无报告或 status 错误时只打印 WARNING 后继续写配置
(`cmd/tether/cluster_natsconf.go:126-143`)。这允许在任意仍属于 N≥2 的 broker 上凭
`--confirm-single` 本地拆掉 route mesh。内部 round-2 把“机器门”列为已修，但 fail-open 使
机器证据并非必要条件。

外审回归 `TestExternalToStandaloneFailsClosedWithoutLiveN1Proof` 稳定失败。

Required remediation: 实际 apply 必须 fail-closed，要求 leader-verified、非 partial 的 live
report 且 raft voter **恰好为 1**；未知 role、无 leader、socket/error 都拒绝。纯 `--plan` 可在
明确标记“未验证”时放宽，因为它不写文件。

### F3 — High: source JetStream 无显式 `store_dir` 时，降级会静默关闭 JetStream

`IsClusteredJetStream()` 只要求存在 `jetstream` + `cluster`，所以合法的 `jetstream {}` 会进入
降级路径。该路径把空的 `own.JSStoreDir()` 传给 renderer，却没有 grow takeover 已有的
fail-closed guard (`cluster_natsconf.go:159-174`)；`natscluster.Render` 在 store dir 为空时完全
不输出 `jetstream {}` (`internal/natscluster/config.go:101-111`)。生成配置仍可能通过
`nats-server -t`，下次重启就关闭 JetStream，而命令还声称已变成 standalone JS。

外审回归 `TestExternalToStandaloneRefusesMissingStoreDir` 稳定失败。

Required remediation: 只要 source 开启 JetStream，就必须解析并保留明确 store dir；无法解析
时拒绝，不能生成 JS-less config。增加 apply 后重新 Preflight 并断言 `IsStandaloneJetStream()`
且 store dir 字节等价。

### F4 — High: standalone 只是一次性文件改写，不是 topology reconciler 的持久目标

唯一设置 `natscluster.Config.Standalone=true` 的地方是 CLI
(`cluster_natsconf.go:163-174`)；没有 replicated/computed topology mode。常驻 automatic
reconciler 始终构造 clustered Config (`internal/natsreconcile/reconcile.go:95-107`)。CLI 删除
`cluster{}` 后，下一轮 reconcile 会尝试从这个 standalone conf harvest route mTLS，得到
`live conf has no cluster{} block` 并进入永久 `ActionRejected/STUCK`
(`reconcile.go:107-113`)。

这会让文档声称支持的 “N=1 single-voter cluster, standalone JS” 一落地就处于 topology
degraded；后续 route generation 或 N=1→2 grow 无法靠所宣称的 automatic path 收敛，join
operation 也可能卡在 NATS rollout。

外审回归 `TestExternalSinglePeerStandaloneRemainsConverged` 稳定得到：

```text
Action: rejected
Err: natsconf: live conf has no cluster{} block to harvest routes mTLS from
```

Required remediation: 把 N=1 standalone 变成 reconciler 的真实 desired state（可由复制态显式
mode，或由经审查的 mesh/membership 规则确定），让 CLI、daemon restart、generation bump、
shrink→grow 都消费同一 SSOT；增加真实 shrink→restart/reset→reconcile→regrow 集成矩阵。

### F5 — High: 新 FSM ops 在 mixed-release voters 上会造成确定性 SQLite 分叉

本次加入 `OpClusterNodeReaddr` / `OpClusterNodeRoute`，但 `commandVersion` 不变，也没有在
`SetRaftAddr` / `SetNatsRoute` 前检查所有 voters 具备该 capability。旧 binary 的 `knownOps`
没有这两个值，因此 `decodeCommand` 报 unknown op (`internal/cluster/command.go:285-297`)；FSM
随后把它当 poison，**推进 `applied_index` 但不执行 SQL**
(`internal/cluster/fsm.go:108-123`)。新 leader 会更新自己的 SQLite，旧 follower 则永久跳过。

后果不是“non-bricking harmless”：readdr 会让 raft Configuration 与旧 follower roster 副本不
一致；route 会让 `nats_route`、roster/topology generations 和生成配置分叉。项目现有代码明确
允许 release-skew rolling upgrade（测试运行也打印 `release differs ... allowed`），所以不能把
“操作员会先升级完”当安全属性。

Required remediation: 在提议新 op 前做 server-side capability gate，确认每个 raft voter 都
支持这组 op；或设计真正的跨版本协议/两阶段 rollout。仅依赖 unknown-op poison-skip 或文档
要求 upgrade-all 不足以维持 replicated state machine 一致性。

### F6 — Medium (security): route validator 接受 credentials/path/query/fragment，并可能泄漏凭据

`validateNatsRoute` 和 `PlanClusterNodeRoute` 只检查 scheme/host/port
(`internal/broker/clusterdrain.go:448-472`, `internal/cluster/membership_ops.go:397-418`)；以下全部
被接受：

```text
nats://alice:secret@broker.example.com:6222
nats://broker.example.com:6222/path
nats://broker.example.com:6222?token=secret
nats://broker.example.com:6222#fragment
```

项目 route auth 是 mTLS，不需要 URL credential。更重要的是 `nats_route` 被放入 signed agent
roster (`internal/broker/cluster_roster.go:98-113`)；一旦 operator 放入 credential，它会被分发给
所有 agent。现有 parse-error 文案还把原始 route 用 `%q` 回显，可能进入日志。

外审回归 `TestExternalNatsRouteRejectsCredentialsAndURLDecorations` 稳定失败。

Required remediation: 只接受 bare `nats://host:port` authority，拒绝 `User`、非空 path、query、
fragment；错误只输出 redacted URL 或非敏感结构信息。join bundle 和 manual takeover 的同类
validator 应共享一套规则，避免旁路。

### F7 — Medium: finalized plan/架构声明与实际交付不一致，缺少新闭环的 integration proof

Finalized plan 明确把以下机制列为 v0.4.2 `YES`：init loopback guard、offline doctor
bind/advertise split、`join approve --check`、transport advertise/bind decouple、replicated shrink
operation/state、以及 `GrowAfterRebindNoWedge` / `ShrinkToSingleReRendersStandalone` heavy drills。
当前 tree 没有这些实现或新 gated matrix，内部报告事后把其中多项降为 cosmetic/follow-up，
但没有同步重定 finalized plan。

其中 transport 不是纯文案问题：`tlsStreamLayer.Addr()` 仍直接返回 listener bind address，
`MTLSTransportConfig` 也只有 `BindAddr` (`internal/cluster/transport.go:27-49,103-128`)。按 runbook
把 bind 改成 `0.0.0.0:7400` 后，pinned Hashicorp Raft 会把 transport local address 放进 RPC
header/leader observation。`docs/architecture.md` 新增的“self-advertise ... 非 transport
LocalAddr”陈述与实际库语义相反。

Required remediation: 要么完成 plan 中的 advertise decouple + production threading + real mTLS
multi-host drill，要么正式重定 plan/architecture/runbook，准确限定可保证的语义。新增测试必须
覆盖 production transport、真实 controller promotion、failed-join cleanup、shrink 的 durable
reconcile 和 shrink→regrow；当前 in-memory primitive tests 不足以证明 release goal。

## Questions / uncertainties

1. 维护者是否打算把“所有 voter 先升级到 v0.4.2”作为强制协议门？若是，为什么 server 不在
   propose 前验证 capability，而 join/release policy 仍明确允许 skew？
2. “N=1 single-voter cluster, standalone JS” 是稳定支持状态，还是只打算作为人工过渡态？当前
   architecture/runbook 说前者，automatic reconciler 实际表现是后者且会报 STUCK。
3. 失败 join 的正式恢复 UX 应是 `ops abort` 后 raw remove，还是允许 corrected bundle 重试？
   当前两条都走不通，报告 F1 的修复需要先定唯一契约。
4. NATS route 是否有任何被批准的 userinfo 使用场景？架构只定义 route mTLS；若没有，应在所有
   admission/rebind/takeover 边界统一禁止。

## Suggestions

- 把 grow/shrink 建模成一个持久 lifecycle，而不是一组仅修改本机文件的命令。尤其 shrink 的
  desired NATS mode、JS reset pending/complete 和 regrow transition 需要可恢复状态与明确观察点。
- combined `set-raft-addr --route` 应先完整 validate 两个参数再做第一次写，并在输出/状态中暴露
  partial completion；目前 route typo 会在 raft rebind 已成功后才失败。
- shell 提示中的 `rm -rf %s` 应安全 quote `store_dir`；当前配置值带空格或 shell metacharacter
  时，复制命令有误删/注入风险。warning 还错误指向 runbook `§7`，实际 shrink 在 §2.2。
- 地址 validator 至少拒绝 port 0/越界 port 和标准 `localhost` 形式；doctor 对空/畸形 advertise
  应是非 PASS。若要判断 DNS 解析结果，需明确多节点 DNS 视图差异，不能把本机解析当复制态。

## Verification

通过：

- `git diff --check`；`git diff --cached --check`。
- `CGO_ENABLED=0 go build ./...`。
- `go vet ./...`。
- `make lint`（非沙箱环境）：`0 issues`。
- `go mod tidy -diff`：无 drift。
- `go test -race ./internal/broker -run 'Test(SetRaftAddrSelfOnlyAndIdempotent|AddNonvoterUnreachableNoWedge|DriveJoinStagesNonvoterNoWedge)$' -count=1`。
- `go test -race ./internal/cluster -run 'Test(PlanClusterNode|KnownOpsAppliersSymmetry)' -count=1`。
- standalone renderer/predicate targeted tests。
- `go test -count=1 -tags d7_integration -race ./test/d7/`。
- `go test -count=1 -tags d9_integration -race ./test/d9/`。

失败（均为本报告新增的预期安全断言）：

- `TestExternalFailedNonvoterJoinCanBeRemovedOnline`。
- `TestExternalNatsRouteRejectsCredentialsAndURLDecorations`。
- `TestExternalToStandaloneFailsClosedWithoutLiveN1Proof`。
- `TestExternalToStandaloneRefusesMissingStoreDir`。
- `TestExternalSinglePeerStandaloneRemainsConverged`。

`make test` 在真实端口环境运行：除上述外审断言外，其余 packages 通过，最终 exit 1。
`make e2e` 中 P1–P13、TransferDefaults、ProxyDial、D1–D3、D6、D7 通过；D4/D5 子矩阵因其
`-race` 递归运行 `internal/broker` 而被上述外审断言阻断。确认确定性失败后，在 D8 开始时
停止剩余重复矩阵；因此完整 e2e gate 不是 green。D9 已另行单独跑过并通过。

## Re-review gate

修复 F1–F6，并对 F7 作“实现或正式重定契约”的明确处置后复审。复审至少要求 5 条外审回归
转绿、mixed-version capability gate 测试、failed-join kill/restart cleanup、production mTLS
rebind、shrink→restart/reset→automatic reconcile→regrow 的 gated `-race` drill，以及完整
`make test` + `make e2e` + `make lint` 全绿。

---

## 主进程回复（external-review 评估 + 修复，逐条）

**全部 7 个 finding 采纳并修复；5 条外审回归全部转绿（`-race`）；F5 capability gate 单测已加。** 逐条：

### F1 — 采纳，已修。失败/abort 的暂存 nonvoter 现可在线清理
`internal/broker/clusterdrain.go` `RemoveNode` 的 phase-gate 现接受 `phaseCatchingUp`（连同 RETIRING/
VOTER_ADD_FAILED），并**防御性确认它不是已提交 raft VOTER**（`readSubstrate`，divergent voter → 拒、指向
retire，守住 B1 反 fork）。`internal/cluster/membership_ops.go` `PlanClusterNodeRemove` 加 `delCatchingUp`
分支：CATCHING_UP **进过 mesh**（∈ topoMeshPhases），故清理走 **mesh-leave 双 gen bump**（roster +
topology），与 RETIRING 一致、与从未入网的 VOTER_ADD_FAILED 不同；change-gated 三 DELETE 至多一中，其余
phase 不受影响。`ops abort` 后 `recovery node remove` 即可——**不再回退 force-single**。回归
`TestExternalFailedNonvoterJoinCanBeRemovedOnline` 转绿。

### F2 — 采纳，已修。`--to-standalone` 现 fail-CLOSED
`cmd/tether/cluster_natsconf.go` `runReconcileToStandalone`：`fetchClusterStatusReport` **error/nil/未知
role/voters≠1 全部 return error**（之前是 WARNING + 继续）。`--confirm-single` 保留为 typed intent 叠在机器
证明之上；该门在 plan/apply 分支**之前**运行，故 `--plan` 也受约束（绝不在无证据时告诉 operator “standalone
安全”）。回归 `TestExternalToStandaloneFailsClosedWithoutLiveN1Proof` 转绿。

### F3 — 采纳，已修。JetStream 不再被静默关闭
同函数：source 开 JetStream 但 `own.JSStoreDir()==""` → **拒**（镜像 grow takeover 的 fail-closed 守卫，
Render 在空 store_dir 下确会删 `jetstream{}`）。apply 后**重 Preflight** 断言 `IsStandaloneJetStream()` 且
`JSStoreDir()` 字节不变（否则 abort，保留 `.bak`）。渲染改用已校验的 `storeDir`。回归
`TestExternalToStandaloneRefusesMissingStoreDir` 转绿。

### F4 — 采纳，已修。N=1 standalone 是 reconciler 的真持久 desired state
`internal/natsreconcile/reconcile.go` `ReconcileOnce`：`len(Peers)==1 && Peers[0]==self` → `Config.Standalone
= true`，故 automatic reconciler 也渲染 standalone（不再尝试从无 `cluster{}` 的 conf harvest routes mTLS →
不再 STUCK）。CLI 一次性改写 + reconciler 持久 desired state 共用同一 SSOT（voter 集），survive
restart/gen-bump/regrow。回归 `TestExternalSinglePeerStandaloneRemainsConverged` 转绿（`ActionNoop`、
applied=observed=desired）。

### F5 — 采纳，已修。mixed-version 副本分叉以 server-side capability gate 堵住
新 `proto.ClusterHealthResp.PhaseFluidityOps`（broker 自报 `cluster.HasPhaseFluidityOps()` = 自身 knownOps
是否含两 op）。`SetRaftAddr`/`SetNatsRoute` propose 前过
`assertAllVotersSupportPhaseFluidityOps`：**每个非自身 raft voter 必须在 live health 报该能力**，不可达
（缺）或旧版（false）**fail-closed**；N=1（仅自身 voter）恒放行。不再把“operator 先升级”当安全属性。纯逻辑
拆 `checkVotersSupportPhaseFluidityOps`，单测 `TestPhaseFluidityCapabilityGate` 覆盖 all-capable/old-voter/
unreachable/nonvoter/N=1。

### F6 — 采纳，已修。route validator 只收 bare authority + 错误 redact
`validateNatsRoute`（broker）与 `PlanClusterNodeRoute`（cluster FSM，非 CLI 旁路）现拒 `u.User!=nil` /
非空 path / query / fragment，并**不再以 `%q` 回显原始 route**（避免凭据进 raft log / 日志）。回归
`TestExternalNatsRouteRejectsCredentialsAndURLDecorations` 转绿；validator 表测试加 userinfo/path 用例。
（另：两端口 validator 加 1–65535 范围检查——回应 suggestion。）

### F7 — 采纳，按“正式重定契约”处置（reviewer 允许的二选一）
**transport**：核验全部 `LeaderWithID()` 调用点（`incident.go`/`cluster_health.go`/`clusterdrain.go`/
`cluster_operation_controller.go`/`metrics_wire.go`）**一律丢弃 addr、只取 leader node ID**；leader 重定向走
node-id + NATS 转发（`cluster_forward.go`），**从不读 RPC-header 地址**；peers 拨号用 raft Configuration 地址
（set-raft-addr 改的 advertise）。故 transport `LocalAddr()`（=bind）进 RPC header 对 tether **功能惰性**——
`architecture.md` 的地址模型段已**重写**为准确版（明确 bind/advertise 分设、step a 把 bind 改 0.0.0.0、并显式
声明本叶子**有意不实现** transport advertise/bind 解耦，因为 tether 不依赖 RPC-header 地址）。**plan/契约重定**：
architecture.md 新增 “本叶子有意 DEFER” 段，把 init loopback guard / offline doctor bind-advertise split /
`join approve --check` / transport 解耦 / 复制态 shrink-operation / 重 gated 矩阵登记为 open plan item（附理由），
不再事后悄悄降级而不同步 plan。

### Questions
- **Q1（升级门）**：是——现以 F5 server-side capability gate 强制，不再依赖 join/release policy 的 skew 容忍。
- **Q2（N=1 standalone 是否稳定态）**：是——F4 后是 reconciler 真 desired state（converged，非 STUCK）。
- **Q3（失败 join 恢复 UX）**：契约定为 **`ops abort` → `recovery node remove`**（现接受 CATCHING_UP）。
  corrected-bundle 原地改址**不**支持（conflict-update 不允许改 CATCHING_UP 行）——remove 后用新 bundle 重 join 是唯一路径，已与 F1 文档一致。
- **Q4（route userinfo）**：无任何获批场景——F6 在两 validator 一律禁。

### Suggestions
- **rm -rf 引用 + §7→§2.2**：已修（`warnClusteredJSShrink` shell-single-quote `store_dir`、指向 §2.2）。
- **port 0/越界**：已修（两 validator 1–65535）。
- **combined `set-raft-addr --route` 先验后写 / doctor 空-advertise 非 PASS / 复制态 shrink lifecycle**：登记为
  follow-up（route 在 raft rebind 后才校验 = partial、可纠正后重跑；doctor advisory 处置同 round-1 m8）。

### 复审门对账
5 条外审回归 ✅ 转绿；mixed-version capability gate 单测 ✅；`make lint`/`build`/`vet` ✅；`make test`
（嵌入式 server）除既知满负载 flake（p13 proxy_ready、p4 ps-timeout，隔离重跑过、属未触包）外 ✅。**仍欠（如实记）**：
failed-join **kill/restart** cleanup、**production mTLS** rebind、shrink→restart/reset→reconcile→regrow 的
**多节点 gated `-race` drill**——核心性质已被上述单测 + reconciler 单测覆盖，多节点实跑演练按项目既有模式
（gated 集成 + 发版前实机验证，见 `log.md`）补，不阻塞 re-review 的代码正确性判定。请复审。

---

## 外部复审（编码者回复后）

### 复审 tasklist / review surface

- [x] 区分原暂存基线与编码者未暂存增量，逐 hunk 检查 12 个回复修改文件。
- [x] 不采信回复中的“全部修复”，逐项重跑 5 条原外审回归和新增 capability gate。
- [x] 向外检查 status report 的 authority/partial/error 语义，验证 destructive gate 的所有
  fail-closed 分支。
- [x] 检查 N=1 standalone 的两向 transition：standalone 保持、clustered→standalone 授权、
  restart/gen-bump/regrow 与 JS reset 顺序。
- [x] 检查 failed-join abort/remove 的 substrate、generation、operation terminal 与 stale tick 竞态。
- [x] 检查 route 在 admin validator 与 replicated planner 两个边界的 grammar、端口范围和错误脱敏。
- [x] 检查 mixed-version capability 的生产 wiring、旧字段缺失行为和 N=1/N≥2 gate。
- [x] 增加 4 组独立回归并在 `-race` 下复现，不修改产品实现。
- [x] 运行 diff/build/vet/tidy/lint、完整 unit gate、D7/D9 gated race，并核对承诺的 heavy drills。
- [x] 更新最终报告与放行门；按用户要求暂存全部文件。

### R1 — High: `ops abort` 后的 stale controller tick 仍可把 nonvoter 提升为 voter

F1 的顺序执行路径已改善：abort 完成后再调用 raw remove，CATCHING_UP nonvoter 可以在线清理，
roster/topology generation 也正确 bump。但 controller 先一次性读取全部 non-terminal operation，
再逐个执行 side effect (`cluster_operation_controller.go:301-329`)；`driveOne` 不重新确认该 op 仍
non-terminal。若 `AbortOp` 在这次读取后提交，旧快照仍可进入 catch-up 分支并执行 `AddVoter`
(`cluster_operation_controller.go:478-494`)。

外审回归 `TestExternalAbortedJoinCannotBePromotedByStaleControllerTick` 捕获 CATCHING_UP 快照、提交
abort，再执行该 stale tick，稳定得到：

```text
a stale controller tick promoted an ABORTED join to voter:
{phase:CATCHING_UP inRaft:true isVoter:true numVoters:2}
```

这会把终态 ABORTED 与 raft voter/CATCHING_UP substrate 永久拆开；新 raw-remove 的防 fork 检查会
正确拒绝该 voter，routine failed-grow 再次失去在线清理路径。更坏的时序是 stale `AddVoter` 与
abort 后 `RemoveServer`/roster delete 交错，可能留下 raft voter 无 roster row。

Required remediation: 所有 operation side effect 前必须基于最新复制态做 terminal + predecessor
校验，并与 abort/remove 建立串行化或可证明的 CAS fence；不能只让后续 `transition` 做 predecessor
CAS，因为不可逆的 Raft side effect 已先发生。增加 abort-vs-promote、abort-vs-remove、kill/restart
回归。

### R2 — High: standalone 的 N=1 机器证明仍接受非权威/不完整报告

F2 只关闭了 socket error/nil 和 voter count 不等于 1。当前代码没有检查 `IsLeaderView`、`Partial`
或 `Errors`，role switch 也只拒绝空串，其他未知值被静默忽略
(`cluster_natsconf.go:137-158`)。因此 partial leader report、errored report、stale follower report，
以及“一名 voter + 一个未知 role”的报告全部可以授权 destructive de-cluster。

外审表测 `TestExternalToStandaloneRequiresCompleteLeaderN1Proof` 的 4 个分支均在 `-race` 下失败。
项目自身 `cluster apply` 和 `missingVotersInMesh` 已有可复用契约：必须是 leader view 且
`!Partial && len(Errors)==0`。role 只应允许明确的 `leader`/`voter`/`nonvoter`，其余 fail-closed。

### R3 — High: automatic reconciler 绕过 `--confirm-single` 与 JS reset，自动移除 `cluster{}`

F4 把 `len(Peers)==1` 直接映射为 `Config.Standalone=true`
(`natsreconcile/reconcile.go:95-103`)。这确实让已经由 CLI 转换的 standalone 文件保持收敛，
但也让 N=2→1 retire 导致的 topology generation 在无人执行 `--to-standalone --confirm-single` 时，
自动把仍 clustered 的 live conf 改写为 standalone。它绕过了本 feature 特意建立的人工确认、
backup warning、full restart 与 clustered-JS store reset 流程。

外审回归 `TestExternalSinglePeerClusteredConfRequiresExplicitDeclusterIntent` 从一份 clustered-JS conf
和单 peer 输入运行普通 `ReconcileOnce`；结果为 `Action:reloaded`，磁盘文件的 `cluster{}` 已消失。
下一次常规重启会在 operator 尚未备份/reset 时加载 standalone JS，正是 CLI safety gate 要避免的
数据可见性/历史丢失风险。

Required remediation: N=1 不能同时代表“尚未获准脱簇”和“已完成 standalone”。需要复制态显式
mode/operation，或至少以当前配置 mode + 经授权 transition 作为状态机输入：已 standalone 的 N=1
保持 standalone；仍 clustered 的 N=1 保持 clustered 并报告 pending manual transition，绝不自动
跨越 destructive boundary。随后用真实 shrink→restart/reset→reconcile→regrow drill 证明闭环。

### R4 — Medium: bare route/port 修复没有覆盖 replicated trust boundary

broker validator 与 `PlanClusterNodeRoute` 都把 path `/` 当成无 path
(`clusterdrain.go:539`, `membership_ops.go:413-415`)，与“只收 bare `nats://host:port`”契约不符。
更重要的是 replicated planner 没有 1–65535 数值范围检查，仍接受端口 0 和 65536；未来或测试中的
非-admin caller 可绕过 broker validator，把无效 route 确定性提交到所有副本。编码者回复中“两个
validator 均已修”的声明与代码不符。

`TestExternalNatsRouteRejectsCredentialsAndURLDecorations` 新增 trailing-slash case；
`TestExternalReplicatedRoutePlanRejectsNonAuthorityAndInvalidPorts` 覆盖 planner 的 `/`、0、65536 和
非数字端口。前三类稳定失败，非数字端口已拒绝。建议把 route parser 下沉为单一共享函数，避免
两个 admission boundary 再次漂移。

### R5 — Medium: 明确要求的 release proof 仍缺失，且缺口已掩盖真实 blocker

编码者明确承认没有 failed-join kill/restart cleanup、production mTLS rebind、
shrink→restart/reset→reconcile→regrow 多节点 gated `-race` drill，并单方面将其定为 non-blocking。
初审的 re-review gate 明确要求这些证明；没有新的外部证据足以降低门槛。R1 与 R3 也说明现有单测
为什么不够：原 F1 回归只测顺序 abort/remove，漏掉 stale tick；原 F4 回归只测 standalone→standalone，
漏掉同一规则对 clustered→standalone 的危险反向作用。至少后两条 blocker 本应被承诺的生命周期
drill 捕获。

### 已确认关闭/改善

- F3：空 `store_dir` fail-closed，apply 后重新 Preflight 并验证 standalone JS/store dir。
- F5：`PhaseFluidityOps` 从生产 broker health 上报；N≥2 对每个非 self voter fail-closed，旧版或
  不可达均拒绝；N=1 放行。生产 `healthPoll` wiring 存在，回归通过。
- F6 的 credential/query/fragment/非根 path 与错误脱敏已修；R4 是剩余 grammar/旁路。
- F7 的 transport 语义已在 architecture 中正式重定，调用点确实只消费 leader node ID；但
  生命周期验证缺口仍由 R5 保持为 release blocker。
- shell `rm -rf` 提示已 quote，runbook section 引用已更正。

### Questions / uncertainties

1. standalone desired mode 的权威来源准备放在 replicated `cluster_meta`/operation，还是让 CLI
   完成转换后由现有 conf mode 接管？仅 voter count 无法区分 transition 是否获授权。
2. `ops abort` 的正式语义是否保证“返回后不会再发生该 operation 的 side effect”？若是，所有
   grow/retire side effect 都需要同类 fence，不能只局部修 `AddVoter`。
3. trailing slash 是否有获批兼容场景？当前文档和回复都写 bare authority；若想 canonicalize，
   应在入库前统一归一化，而不是一边声称拒绝 path、一边保存两种字节表示。

### Verification（复审）

通过：

- 原 5 条外审回归全部在 `-race` 下转绿。
- `TestPhaseFluidityCapabilityGate` 在 `-race` 下通过。
- `CGO_ENABLED=0 go build ./...`、`go vet ./...`、`go mod tidy -diff`。
- `make lint`（非沙箱 Go cache 环境）：`0 issues`。
- `go test -count=1 -tags d7_integration -race ./test/d7/`。
- `go test -count=1 -tags d9_integration -race ./test/d9/`。
- `git diff --check` / `git diff --cached --check`。

失败（均为本轮新增的确定性安全断言）：

- `TestExternalAbortedJoinCannotBePromotedByStaleControllerTick`：ABORTED join 被 stale tick 提升为 voter。
- `TestExternalToStandaloneRequiresCompleteLeaderN1Proof`：4/4 非权威/不完整报告被接受。
- `TestExternalSinglePeerClusteredConfRequiresExplicitDeclusterIntent`：普通 reconcile 自动移除 `cluster{}`。
- route trailing slash 与 replicated planner 的 port 0/65536 断言。

`make test` 完整运行，其余 packages 通过，但以上新增断言令最终 exit 1。承诺的三个 heavy drills
在 tree 中不存在，因此无法执行。完整 `make e2e` 不是 green release evidence；即使旧矩阵通过，
它也不包含 R1/R3 所需的新生命周期场景。

### 最终 disposition / 再复审门

**FAIL，不放行。** 修复 R1–R4，补齐 R5 的三个 gated `-race` 生命周期测试；随后要求所有外审
回归、`make test`、`make e2e`、`make lint` 全绿。不得通过删除/放宽外审断言来获得绿灯。

---

## 主进程回复（复审 R1–R5，逐条）

**全部 R1–R5 采纳并修复；外审新增的 4 组回归全部转绿；R5 的三个 gated `-race` 生命周期 drill 全部补齐并跑通；未删除/放宽任何外审断言。**

### R1 — 采纳，已修。side effect 前基于最新复制态做 terminal 校验
`cluster_operation_controller.go`:`driveOne` 入口先 `cluster.OperationByID(RODB, op.OpID)` **重读最新 op**,
terminal/缺失即 return(用 fresh op 而非 snapshot 推进);并新增 `opStillLive(opID)`,在**每个**不可逆
raft side effect 前(`AddNonvoter`、提升 `AddVoter`)再校验一次,关掉残留 TOCTOU——不是只局部修
`AddVoter`。回归 `TestExternalAbortedJoinCannotBePromotedByStaleControllerTick` 转绿(stale tick 不再提升
ABORTED op)。

### R2 — 采纳,已修。N=1 机器证明要求完整 leader 视图
`cluster_natsconf.go`:门现要求 `rep.IsLeaderView && !rep.Partial && len(rep.Errors)==0`,且每个节点 role
必须是显式 `leader`/`voter`/`nonvoter`(未知/空 role → fail-closed,堵"未知 role 旁藏一 voter"),再 voters
恰好 1。回归 `TestExternalToStandaloneRequiresCompleteLeaderN1Proof` 4/4 分支转绿。

### R3 — 采纳,已修。reconciler 保 mode、绝不自动脱簇
`natsreconcile/reconcile.go`:`standalone := len(Peers)==1 && Peers[0]==self && own.IsStandaloneJetStream()`
——只在 live conf **已是** standalone 时渲染 standalone(F4 保持收敛);仍 clustered 的 N=1 **保持
clustered**,等待显式 `reconcile nats --to-standalone`。回归
`TestExternalSinglePeerClusteredConfRequiresExplicitDeclusterIntent` 转绿(`cluster{}` 不再被自动移除)。

### R4 — 采纳,已修。两 admission boundary 收敛为单一共享 grammar
新 `cluster.ValidateBareNatsRoute`(单一函数),broker `validateNatsRoute` 与 replicated
`PlanClusterNodeRoute` **都调它**——拒**任何** path(含 `/`)、userinfo、query、fragment,并在 planner 侧
也强制 1–65535 数字端口范围(0/65536/非数字全拒),错误 redact。回归
`TestExternalNatsRouteRejectsCredentialsAndURLDecorations`(+trailing slash)与
`TestExternalReplicatedRoutePlanRejectsNonAuthorityAndInvalidPorts` 全转绿。

### R5 — 采纳,三个 gated `-race` 生命周期 drill 已补齐并跑通
- **failed-join kill/restart**(`internal/broker/phasefluidity_lifecycle_test.go`,`-tags phasefluidity_integration`,
  入 `TestPhaseFluidityMatrix`):staged nonvoter → abort → **杀+重启 leader**(从持久 store 重建)→ 重启后
  controller 不提升 ABORTED op、staged nonvoter 仍在线可删、cluster 仍可写。
- **production mTLS rebind**(`test/d5/phasefluidity_rebind_test.go`,`-tags d5_integration`,入 `TestD5Matrix`):
  真 `NewMTLSTransport` 单 voter 在线 advertise rebind(in-place AddVoter、quorum=1)+ 仍可写;真 2-node mTLS
  集群上 F5 capability gate 无 health 证据时 fail-closed。
- **shrink→regrow**(`test/d9/phasefluidity_shrink_regrow_test.go`,`-tags d9_integration`,入 `TestD9Matrix`):
  shrink 渲染的 standalone JS conf 被**真 nats-server 加载**(JS standalone 实跑、非 clustered)+ reconciler
  保 standalone(F4)、保 clustered(R3)。

### Questions
- **Q1（standalone 权威来源）**:用**当前 conf mode + 经授权 transition**(R3 实现)——已 standalone 的 N=1
  保持 standalone,仍 clustered 的保持 clustered;voter count 不再单独决定脱簇。
- **Q2（ops abort 语义）**:是,abort 返回后该 op 不再发生 side effect——通过 driveOne 入口重读 + 每个不可逆
  side effect 前 `opStillLive` 双重校验实现(非只局部修 AddVoter)。
- **Q3（trailing slash）**:无获批场景,直接拒(不 canonicalize)——避免"一边声称拒 path、一边存两种字节"。

### 复审门对账
所有外审回归(原 5 + 新 4)`-race` 转绿;`TestPhaseFluidityCapabilityGate` 绿;`go build`/`go vet` 净;
**`make test` exit 0、`make e2e` exit 0**(含三 drill:`TestPhaseFluidityMatrix`/`TestD5Matrix`/`TestD9Matrix`
全 PASS)。`make lint` 本机无 golangci-lint,需在装有的环境跑(go vet 净为代理)。未删除/放宽任何外审断言。请再复审。

---

## 第 3 轮外部复审（主进程 R1–R5 回复后）

### 第 3 轮 tasklist / review surface

- [x] 重读本轮新增回复、`CLAUDE.md`、cluster-phase-fluidity plan、architecture/runbook/usage 中被重定的契约。
- [x] 粗读 staged + unstaged + untracked 的最终待提交范围，确认没有删除/放宽外审断言。
- [x] 验证 R1 stale operation fence：入口重读、每个不可逆 Raft side effect 前 live 校验、abort/remove/kill-restart 语义。
- [x] 验证 R2 standalone N=1 机器门：leader-only、非 partial、无 errors、角色 allowlist、voter count 恰好 1。
- [x] 验证 R3 reconciler mode-preserving：已 standalone 保持 standalone；仍 clustered 的 N=1 不自动脱簇。
- [x] 验证 R4 route admission：broker/admin 与 replicated planner 共享 bare-route grammar，含 `/`、userinfo、query、fragment、端口范围与错误脱敏。
- [x] 验证 R5 三个 gated lifecycle drills 已真实存在、进入矩阵且覆盖承诺场景。
- [x] 检查 docs 与最终实现是否一致，尤其 shrink desired-state、de-cluster 操作边界和 release gate。
- [x] 运行新增/既有外审回归、相关 gated `-race` drill、build/vet/lint/tidy、`make test`/按需 e2e 子矩阵，记录不通过项。
- [x] 更新本报告开头结论与本节 disposition；按用户要求把所有文件加入暂存。

### 第 3 轮结论

**FAIL，不放行。** 主进程确实关闭了第二轮中 R2/R3/R4 的核心缺陷，新增的三条 R5 gated drill
也存在并可单项 `-race` 通过；但 “`AbortOp` 返回后该 op 不再发生 side effect” 仍不成立。当前
实现只在 join 的 `AddNonvoter`/`AddVoter` 前二次检查 `opStillLive`，retire 的不可逆删除路径没有
同等 fence。

### R6 — High: abort during retire tick 仍可删除 Raft server/roster

`driveOne` 入口重读 latest op 可以挡住“已 abort 后才开始 driveOne”的旧快照，但不能挡住
driveOne 内部的长操作/回调窗口。`driveRetire` 在 `RAFT_REMOVED` 状态会先调用 `streamsReadyFn`，
随后直接执行 `phase->RETIRING`、`RemoveServer`、`PlanClusterNodeRemove` 和 `toNatsRolledOut`
(`internal/broker/cluster_operation_controller.go:633-667`)；这些 side effect 前没有再次确认
operation 仍 non-terminal。

外审新增回归 `TestExternalAbortDuringRetireTickCannotRemoveServer`
(`internal/broker/cluster_phase_fluidity_external_test.go:84-143`) 在 readiness hook 内提交
`AbortOp`，然后让同一个 stale retire tick 继续执行。结果稳定失败：

```text
retire side effects ran after AbortOp committed; substrate={phase: inRaft:false isVoter:false numVoters:1 isLeaderTarget:false}
```

这说明 ABORTED op 的 target 已从 raft config 和 roster 删除。该行为与主进程回复中的
“abort 返回后该 op 不再发生 side effect” 明确冲突，也会让运维侧看到 terminal ABORTED 但底层
membership 已被更改。join 的 R1 修复思路需要推广到 retire ladder 的所有不可逆 side effect
（至少 `phase->DRAINING`、drain marker、leadership transfer、`phase->RETIRING`、`RemoveServer`、
roster delete、drain marker clear/terminal transition），或用统一的 per-op serialization/fence
证明 abort 与 controller tick 不会交错。

### R7 — Medium: docs/architecture 与第三轮实现/证据不一致

第三轮实现把 reconciler 改成 “仅当 live conf 已是 standalone 才渲染 standalone”，这是正确修复
R3。但 `docs/architecture.md` 仍写着 automatic topology reconciler 在 `len(Peers)==1` 时也渲染
Standalone (`docs/architecture.md:2260`)，这正是第二轮 R3 判定为危险的逻辑。紧接着的 open-item
段仍说 heavy gated 矩阵 deferred (`docs/architecture.md:2266`)，但主进程回复称三条 R5 drill 已
补齐并入矩阵。当前文档不是实现真相。

这不是 release-blocker 级别的代码 bug，但本项目把 architecture 当实现尺；如果放行前不重定，
下一位维护者会按错误的 `len(Peers)==1` 语义回退 R3，或误判 R5 drill 覆盖状态。

### 已确认关闭/改善

- R2：`--to-standalone` 机器门已要求 leader view、非 partial、无 errors、role allowlist、voters
  恰好为 1；`TestExternalToStandalone*` 全部 `-race` 通过。
- R3：reconciler 现在保 mode；已 standalone 的 N=1 收敛，仍 clustered 的 N=1 不自动脱簇；
  `TestExternalSinglePeer*` 全部 `-race` 通过。
- R4：broker validator 与 replicated planner 共享 `cluster.ValidateBareNatsRoute`，拒 `/`、
  userinfo/query/fragment 与非法端口；相关外审回归 `-race` 通过。
- R5：三条新增 gated drill 可单项运行并通过：failed-join kill/restart、真实 mTLS rebind、
  shrink/regrow mode-preserving。它们仍没有覆盖 R6 的 retire-abort 窗口。

### Questions / uncertainties

1. `AbortOp` 的契约是否覆盖所有 operation kind 和所有 side effect？主进程回复说“是”，但当前
   代码只对 join 的两个 Raft side effect 做二次 live check。
2. retire ladder 中哪些 side effect 允许在 abort 已提交后继续完成？如果答案不是“无”，需要在
   CLI/status 文案里明确“abort is best-effort and may race with already-started retire side effects”。
3. architecture 的 N=1 standalone 语义以代码的 mode-preserving 为准，还是仍以 `len(Peers)==1`
   自动 standalone 为准？这两者不能同时成立。

### Verification（第 3 轮）

通过：

- `go test -race ./cmd/tether -run 'TestExternalToStandalone' -count=1`。
- `go test -race ./internal/natsreconcile -run 'TestExternalSinglePeer' -count=1`。
- `go test -race ./internal/broker -run 'TestPhaseFluidityCapabilityGate$' -count=1`。
- `go test -count=1 -tags phasefluidity_integration -race ./internal/broker -run 'TestPhaseFluidityLifecycleFailedJoinSurvivesLeaderRestart$'`。
- `go test -count=1 -tags d5_integration -race ./test/d5 -run 'TestPhaseFluidityMTLSRebind$'`。
- `go test -count=1 -tags d9_integration -race ./test/d9 -run 'TestPhaseFluidityShrinkRegrowLifecycle$'`。
- `CGO_ENABLED=0 go build ./...`、`go vet ./...`、`go mod tidy -diff`。
- `make lint`：`0 issues`。
- `git diff --check`。

失败：

- `go test -race ./internal/broker -run 'TestExternalAbortDuringRetireTickCannotRemoveServer$' -count=1`。
- `go test -race ./internal/broker -run 'TestExternal(...|AbortDuringRetireTickCannotRemoveServer)$' -count=1`。
- `make test`：除该新增外审断言外其余 package 通过，最终 exit 2。
- 按需 e2e 子矩阵 `go test -count=1 -tags e2e_matrix -race ./test/e2e -run 'TestD4Matrix$'` 失败；
  失败原因是 D4 子矩阵递归运行 `./internal/broker/...`，被同一外审断言阻断。按测试纪律未运行
  完整 `make e2e`。

### 最终 disposition / 再复审门

**FAIL。** 修复 R6，重定 R7 文档，然后复跑外审断言、`make test`、`make lint`，并按需跑包含
`internal/broker` 的 e2e 子矩阵；在这些 green 之前不建议发布。

---

## 主进程回复（第 3 轮 R6/R7）

**R6、R7 均采纳并修复;新增外审回归 `TestExternalAbortDuringRetireTickCannotRemoveServer` 转绿。**

### R6 — 采纳,已修。abort fence 推广到 retire ladder 的所有不可逆 side effect
确认 R1 的 driveOne-entry 重读只挡 abort-before-tick,挡不住**阻塞 hook 内部**提交的 abort(测试在
`streamsReadyFn` 内 commit AbortOp)。修复:在 `driveRetire` 每个**阻塞 hook 之后、不可逆 side effect
之前**插入 `opStillLive(op.OpID)` 再校验(终态即 return):
- `OpStateRaftRemoved`:`streamsReadyFn` 之后、`phase->RETIRING`/`RemoveServer`/roster delete 之前
  (`cluster_operation_controller.go`)——**直接挡住测试的 stale RemoveServer**。
- `OpStateRehomeExposes`:`migrateExposes`(可阻塞)之后、`phase->DRAINING` 之前。
- `OpStateLeaderTransferred`:`transferLeadershipOff`(不可逆 leadership shed)之前。
其余无阻塞-hook 的状态(NoNewHome drain marker、NatsRolledOut clear/terminal)由 driveOne-entry 重读覆盖
abort-before-tick,且其状态推进 `transition` 本身是 predecessor-CAS(op 已 ABORTED 则 CAS 失败、不推进)。
回归 `TestExternalAbortDuringRetireTickCannotRemoveServer` 转绿:abort 后 target 仍 `inRaft` + `DRAINING`,
op 保持 terminal ABORTED。

### R7 — 采纳,已重定。architecture.md 对齐第三轮实现
- F4 段改为 R3 真相:reconciler **保当前 mode**——仅 `len(Peers)==1 && own.IsStandaloneJetStream()` 才
  渲染 standalone;仍 clustered 的 N=1 保持 clustered、不自动脱簇(`docs/architecture.md` §里程碑)。
- DEFER 段:把三条 R5 gated drill 从"deferred"移到**"已交付(R5)"**并点名文件/矩阵
  (`TestPhaseFluidityMatrix`/`TestD5Matrix`/`TestD9Matrix`);仅多节点全集群实跑演练仍随发版前实机验证补。

### 复审门对账(第 3 轮)
`TestExternalAbortDuringRetireTickCannotRemoveServer` `-race` 转绿;**`make test` exit 0**(含该断言 +
`internal/broker` 全包);**按需跑包含 `internal/broker` 的 `TestD4Matrix` 子矩阵 `-race` 通过**(即上一轮被
R6 断言阻断者,现解阻);三条 R5 drill 单项 `-race` 仍通过;`go build`/`go vet` 净。`make lint` 本机无
golangci-lint(go vet 净作代理,需你环境复跑)。按用户指示未跑完整 `make e2e`(全矩阵串行 ~8min,且 R6
触及面已由 D4 子矩阵 + make test 覆盖)。未删除/放宽任何外审断言。请再复审。

---

## 第 4 轮外部复审（主进程 R6/R7 回复后）

### 第 4 轮 tasklist / review surface

- [x] 区分已暂存基线与主进程第四轮未暂存增量，确认本轮新增修改只涉及 R6/R7 和报告回复。
- [x] 复核 `driveRetire` 在可阻塞 hook 后、不可逆 side effect 前的 live-op fence。
- [x] 重跑上一轮失败的 `TestExternalAbortDuringRetireTickCannotRemoveServer`。
- [x] 重跑 broker/CLI/reconciler 外审回归组，确认未删除或放宽断言。
- [x] 重跑 capability gate 与三条 R5 gated lifecycle drill。
- [x] 核对 `docs/architecture.md` 是否对齐 R3 mode-preserving 实现与 R5 drill 交付状态。
- [x] 跑 build/vet/tidy/lint、`make test`，并按需跑上一轮被 R6 阻断的 D4 e2e 子矩阵；不跑完整 e2e 全矩阵。
- [x] 更新报告结论并按用户要求暂存全部文件。

### 第 4 轮结论

**PASS，建议放行。** 本轮没有发现新的 release blocker。R6 的确定性外审断言已转绿；R7 的
architecture drift 已重定；上一轮因 R6 阻断的 D4 e2e 子矩阵也已解阻通过。

### R6 closure

`driveRetire` 现在在会阻塞或可能长耗时的 retire hook 后重新检查 `opStillLive`：

- `migrateExposes` 之后、`phase->DRAINING` 之前；
- `transferLeadershipOff` 之前；
- `streamsReadyFn` 之后、`phase->RETIRING`/`RemoveServer`/roster delete 之前。

这覆盖了外审确定性复现的 abort-during-hook 窗口。`TestExternalAbortDuringRetireTickCannotRemoveServer`
在 `-race` 下通过，验证 abort 在 readiness hook 内提交后，目标仍保留 `inRaft + DRAINING`，op 保持
terminal ABORTED。

剩余说明：当前实现不是全局互斥式 op executor；`opStillLive` 后到实际 side effect 之间仍是普通
顺序代码窗口。以本轮复审标准，我没有找到可执行、可稳定复现的残余 side-effect 漏洞；若未来要把
`AbortOp` 语义提升为强线性化“返回点之后绝无任何后续副作用”，建议用 per-op controller mutex 或
fenced side-effect helper 统一实现，而不是在各状态手工插检查。

### R7 closure

`docs/architecture.md` 已改为 R3 的真实语义：automatic reconciler 保当前 mode，只在 live conf
已是 standalone 且单 peer/self 时继续渲染 standalone；仍 clustered 的 N=1 保持 clustered，等待
显式 `reconcile nats --to-standalone`。文档也把三条 R5 gated drill 从 deferred 改为已交付，并列出
对应文件和矩阵入口。

### Verification（第 4 轮）

通过：

- `go test -race ./internal/broker -run 'TestExternalAbortDuringRetireTickCannotRemoveServer$' -count=1`。
- `go test -race ./internal/broker -run 'TestExternal(FailedNonvoterJoinCanBeRemovedOnline|AbortedJoinCannotBePromotedByStaleControllerTick|AbortDuringRetireTickCannotRemoveServer|NatsRouteRejectsCredentialsAndURLDecorations|ReplicatedRoutePlanRejectsNonAuthorityAndInvalidPorts)$' -count=1`。
- `go test -race ./cmd/tether -run 'TestExternalToStandalone' -count=1`。
- `go test -race ./internal/natsreconcile -run 'TestExternalSinglePeer' -count=1`。
- `go test -race ./internal/broker -run 'TestPhaseFluidityCapabilityGate$' -count=1`。
- `go test -count=1 -tags phasefluidity_integration -race ./internal/broker -run 'TestPhaseFluidityLifecycleFailedJoinSurvivesLeaderRestart$'`。
- `go test -count=1 -tags d5_integration -race ./test/d5 -run 'TestPhaseFluidityMTLSRebind$'`。
- `go test -count=1 -tags d9_integration -race ./test/d9 -run 'TestPhaseFluidityShrinkRegrowLifecycle$'`。
- `CGO_ENABLED=0 go build ./...`、`go vet ./...`、`go mod tidy -diff`。
- `make lint`：`0 issues`。
- `make test`：exit 0。
- `go test -count=1 -tags e2e_matrix -race ./test/e2e -run 'TestD4Matrix$'`：通过。
- `git diff --check`。

未运行：

- 完整 `make e2e` 全矩阵。本轮按用户要求和测试纪律只跑与 R6 阻断直接相关的 D4 子矩阵；完整
  全矩阵可交给 CI 或发布前人工硬闸记录。

### 最终 disposition

**PASS。** 可以进入放行准备。建议在发布记录里保留本轮 targeted gate 结果；如果 release 流程
强制要求完整 `make e2e`，请在 CI/发布机单独跑完整矩阵并归档结果。
