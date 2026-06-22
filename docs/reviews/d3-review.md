# D3 内审报告（Stage C · 多专家对抗 + 主进程处置）

> **来源**：Stage C 审查 workflow（5 个 **Opus 4.8** 对抗审查员 + 1 综合，~644k tok，run `wf_bcfb7634-b02`）。审查员只读实现 + 提测试条目，**不改实现**。本文件是**主进程逐条处置**（采纳/驳回 + 修复）。
> **总评**：**无安全 fail-open、无先父后子违规、无 false-allow、无未复制直写**——信任面正确，核心安全测试（fail-closed 谓词边界、mTLS 双向拒外来 CA、RF1 broker-only ACL、PIN 不假放行）经 5 审查员独立变异/源码核验**确实咬住**。BLOCKER 非逻辑洞：2 个 doc-vs-code 漂移 + 1 个错误路径资源泄漏 + 一批集成测试缺口（handler↔真 Node、leader-commit-through-raft 仅用 fake 各证一半）。**全部采纳并修。**

---

## 真问题（BLOCKER，必修）

### B1 — doc 称 handler 映射 `raft.ErrNotLeader`/`ErrLeadershipLost`→transient deny，代码只匹配 sentinel【已修】
- **缺陷**：§6.2 line 164 称 handler 映射裸 raft 错误，但 `handler.go` 只匹配 `authcallout.ErrNotLeader`。安全半边成立（裸 raft 错误落 default→仍 DENY、不假放行、不写库），但 R3 要求下台-leader deny **被分类 transient**；D9 seam 返回裸 `raft.ErrLeadershipLost` 会得 generic deny。
- **处置（采纳·已修，但修法纠偏）**：审查的初版建议「handler 也匹配裸 raft 错误」**会违反架构 L-2**（raft 仅限 `internal/cluster`，由 `TestRaftConfinedToClusterPackage` 守卫——我首版误 import raft 进 handler，被该 lint 抓到）。**正确修法**：handler 保持 raft-free，由 **seam（broker/D9 构建，可 import raft）经新增 `cluster.IsNotLeader(err)` 映射裸 raft 错误→`authcallout.ErrNotLeader`**；doc line 164 改述为「seam 经 `cluster.IsNotLeader` 映射、handler 据 sentinel 分类、handler 保持 raft-free（L-2）」。新增 `cluster.IsNotLeader` 作可复用 seam 原语。
- **验证**：`internal/cluster/node.go` `IsNotLeader`；`TestD3SeamMapsRaftNotLeaderToTransientDeny`（test/d3：`cluster.IsNotLeader` 识别两 raft 错误 + seam 映射→handler transient deny + 不写库）；`TestRaftConfinedToClusterPackage` 复绿。

### B2 — plan 修订 5 未落 §18.2.16（line 422）：route 叶子仍硬称"须 nkey-钉"无 D7 caveat【已修】
- **缺陷**：§18.2.16 item 16 仍写"route mTLS 叶子也须 nkey-钉到 node-identity"无 D7 caveat，与 D3 的 CA-only routes 实现直接矛盾；plan amendment 5 要求中和此句（§1.3/§18.1 已改、此处漏）。外审持 line 422 对照 CA-only 代码会读出"doc 说须/代码没"矛盾。
- **处置（采纳·已修）**：line 422 追加"（**D3 实现修正**：D3 仅发 CA-only X.509 routes/raft 传输，nkey 叶子钉式 = D7 join-PoP；与 §1.3/§18.1 一致）"。

### B3 — `cluster.New()` 错误路径泄漏注入的 mTLS transport（fd + accept goroutine）【已修】
- **缺陷**：`node.New` 三个错误分支关 store/ro/db 但**不关 `cfg.Transport`**；New 返回 `(nil, err)` 调用方无 Node 可 Shutdown → `tls.Listener` fd + `go listen()` goroutine 孤儿。`TestD3TransportShutdownReapsListenerNoLeak` 只测成功路 → 泄漏对套件不可见；D9 cutover 在生产调 New，瞬时启动错误每次重试漏一份。
- **处置（采纳·已修）**：New 在 Transport nil-check 后加 `success` flag + defer：任何非成功退出关闭注入的 transport；成功前置 `success=true`（Node 接管，Shutdown 关）。
- **验证**：`TestD3FailingNewReapsTransportNoLeak`（5× 失败 New——Lease>Heartbeat 使 ValidateConfig 在 BootstrapCluster 内拒——fd/goroutine 基线门）。

---

## 集成测试缺口（MAJOR，加测加固）

### M1 — 无测试把 handler 接**真** `cluster.Node` 谓词；§13.8-MANDATORY 的 live-allow 路缺失【已加测】
- 审查实证：现有只各证一半——`TestD3LeaderContactStaleLive` 直调 `follower.LeaderContactStale`（真 node 但纯数学，不经 Handler）；`TestD3FailClosedDeniesAlreadyProvisioned` 经 handler 但用 **fake** 谓词且只测 deny 侧。seam 合约（`Handler.fenced()`→真 raft `State()/LastContact()`）从未作为整体测过；「失 quorum 不锁死」的 ALLOW 侧零覆盖。
- **处置（采纳·加测）**：`TestD3HandlerRealNodeFenceLive`（test/d3）——真 2-node mTLS 集群、`h.LeaderContactStale = follower.LeaderContactStale`、注入 `h.Now`：在线 allow → 杀 leader（失 quorum）→ **仍 allow within T_fence 且 < 200ms 快**（失 quorum 不锁死 + 无 VerifyLeader 往返）→ 注入 now 过 T_fence → fenced deny。

### M2 — `TestD3PINSeamLeaderSucceeds` 用本地写 fake，未证真 `Node.Propose`→commit→allow【已加测】
- **处置（采纳·加测）**：`TestD3HandlerRealNodeLeaderCommitAllows`（test/d3）——单节点真集群，`h.ProvisionAgentWrite = node.Propose(PlanProvisionWithPIN)`，PIN bootstrap 经真 raft 提交→handler allow + 本地副本可见 agent_provisioning 行。

### M3 — `Multinode*` 常量从未喂 `raft.ValidateConfig`，仅算术不等式断言【已加测】
- **处置（采纳·加测）**：`TestD3MultinodeRaftConfigValid`——`raft.ValidateConfig(raftConfig(...Multinode...))` == nil，钉 `Lease≤Heartbeat≤Election` 顺序（防未来编辑只在 D9 启动炸）。

### M4+M5+M6 — 渲染 conf 从未启真服务器 / callout 从未经 mTLS routes / QueueSubscribe 去重无回归【已加测：一测合并三】
- 审查实证：`config_test` 只 `ProcessConfigFile` **解析**渲染 conf；行为套件用手搭 `Options`（非 `Render` 产物）；跨服务器 callout 跑**明文** routes；QueueSubscribe 去重每测仅一响应器。
- **处置（采纳·加测）**：`TestD3RenderedConfCrossServerCalloutDedupe`（test/d3）——2 个服务器**从真渲染 conf 启动**（`ProcessConfigFile`+`NewServer`）形成 **mTLS routes 网**，跨服务器 callout 授权通，且 `tether-authcallout` queue group **恰一应答**（去重）。一测闭合 M4+M5+M6。配合：渲染器 jetstream 块改条件式（无 store_dir 不启 JS，让该测跳过 2-node JS 集群复杂度）。

---

## minor（采纳轻修或登记narrowing）

- **m1**（采纳·加）：RF1 负向加 §13.8 正控制——同连接 pub 允许 subject(`_INBOX.*`)成功，证 deny 是 subject-scoped 非死连。
- **m2**（采纳·加）：`TestD3RF1ClusterACLOnlyBroker` 补「用户模板无 `$SYS.*`」精确断言。
- **m6**（采纳·已修）：`TestD3LeaderContactStaleLive` 删除时序脆弱的「just-under-T_fence live」断言（真 lastContact 在 teardown 中后退，-race/慢 CI 会 flake）；边界精度由纯函数表证。
- **m8**（采纳·加）：fail-closed 表加 `Candidate` + zero-contact 行（首次选举冷启）。
- **m9**（采纳·已修）：`isPermViolation` 改精确匹配 "permissions violation"（非裸 "permission" 子串）。
- **m10**（采纳·加）：渲染器测试加基数断言（`auth_users`/`Nkeys` 长度 == roster，无重复/泄漏 nkey）。
- **m12**（采纳·已注）：cross_server "no responder" 行加注释——断言的是非授权（callout 无应答）、非显式 DENY 理由。
- **F2**（采纳·已改注）：`read.go` 注释改「LastContact 在**成功** AppendEntries / 准予投票 / InstallSnapshot 刷新」。
- **m5**（驳回·已被现实现覆盖）：审查担心 `wrongGE` 仅靠单行判别、删行则 vacuous；但现实现的 per-predicate「≥1 行 disagree else t.Fatal」循环已自保（删判别行→disagreed=false→fatal），非 silent。
- **m3 / m4 / m7 / m11**（登记 narrowing，非 D3 欠账）：
  - m3：RF1 正控制为同连接内（非跨路由投递）——跨路由转发是 **D4**，D3 narrowing。
  - m4：泄漏门 goroutine 基线带 `+8` slack 可能掩盖 1–2 周期部分泄漏；**fd `>4` 门更强、总泄漏必被抓**，接受。
  - m7：`test/d3` routed-NATS 测试无 NumGoroutine/fd 门——服务器均 t.Cleanup、TestD3Matrix 跑 -race（抓竞争）；transport 泄漏门在 cluster 包已覆盖。登记为可接受 narrowing。
  - m11：R1 源码扫描 guard 可被 import-alias/位置式 struct-init/第 4 文件绕过——作为绊线接受（生产真接线会同时触发行为变化）。

---

## 复验门（修完 · 全绿）

`go build ./...` ✓ / `go clean -testcache && make test` ✓（含 p1–p13/security/chaos/cli_e2e/concurrency + 新 d3/natscluster/cluster，**无回归**）/ `make lint` **0 issues** ✓（含 `TestRaftConfinedToClusterPackage` 复绿）/ **D3 全面 -race** ✓ / **`TestD3Matrix` -race**（30s）✓ / **`make e2e` 全 phase 矩阵** ✓。

## 主进程结论

审查**高质量**：抓到 2 个会让外审 FAIL 的 doc 漂移 + 1 个 D9 才暴露的真泄漏 + 一批「两半各用 fake 证、整体未composed」的集成 vacuity。**全部采纳**（仅 m5 因现实现已自保而无需改、m3/m4/m7/m11 登记为合理 narrowing）。修复 = B1（纠偏为 seam-owns-mapping 守 L-2）/B2/B3 三 blocker + M1–M6 六集成测试 + m6 flake + 一批 minor，均主进程亲手做，**逐项验证 + 全门复绿**。→ **交付外审**。

> **D9-staged（非 D3 欠账，本报告确认边界）**：seam 在生产 broker 的接线（`broker.New` 内嵌 `cluster.Node` + 用 `cluster.IsNotLeader` 构建 PIN/fence seam + 渲染器接管 `nats.conf`）= D9 cutover；`apply.*` 跨路由转发 = D4；route 叶子 nkey-钉式 = D7 join-PoP。
