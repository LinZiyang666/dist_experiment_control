# tether — 项目工作说明（CLAUDE.md）

> 本文件每个会话加载进上下文，定义**怎么推进本项目**。需求/架构/用法细节在 `docs/`，本文件不重复。
> 工作流移植自 `auto_daemon`，按 tether（Go-only、已发布、phase 序列推进）的实际情况改写。

## 1. 项目与文档地图

- 一句话：**"SSH + 端口暴露" 的 NAT 穿透控制面**——NAT 后的 agent 经 NATS 反连公网 broker，使用者（ctl）经同一 NATS 把命令路由到 agent。三角色同一二进制 `tether`，子命令切换。
- `docs/requirements.md` — 需求基线（唯一需求真相）。
- `docs/architecture.md` — 架构基线（实现尺）。关键：**「里程碑映射」（P0→P11 出口 + post-1.0 叶子增量登记）**、**「关键依赖警告」（先父后子硬约束）**、**「每进入新 phase 的 checklist」**。
- `docs/usage.md` — 全量用户/运维手册（怎么用、怎么部、怎么排错）。
- `docs/devices.md` — 实机/设备清单。
- `docs/reviews/` — 每个 phase / feature 的 plan 与各轮 review 报告（`p<N>-plan.md`、`p<N>-review.md`(+`-round2`/`-round3`)、`p<N>-external-review.md`；历史 feature 增量用 `<feature>-plan.md`）。
- `docs/reviews/quality-audit/` — 横切质量审计（concurrency / security / storage-protocol / cli-ux / tests / deadcode）。
- `log.md` — 实机测试日志（真 broker + 真 agent 的人工验证记录）。

## 2. 工作单元：一次一个 phase

- 按 `architecture.md`「里程碑映射」的 **P<N> 序列**推进，**一次只做一个 phase**。主线 P0–P11 已带到 **v0.1.0**；其后改为 **post-1.0 叶子增量**模式（各自独立 plan→实现→内审→外审，不在线性 P 序内、不阻塞主线），P12（expose `--remote-port`）/P13（proxy 订阅）等均按此走，当前已发布到 **v0.3.4**。
- **新工作**：除非用户明确要延续线性里程碑（则取下一个未做的 P 号），否则一律当作**新的叶子 feature 增量**——范围先与用户敲定，再按 §3 的 3 阶段 7 步开工。
- 依赖"**先父后子**"：任何 phase 只用已完成的前序产物，绝不超前——严格遵守 `architecture.md`「关键依赖警告」里的不可跳序约束。
- 每进入新 phase 先过 `architecture.md`「每进入新 phase 的 checklist」（前一 phase 出口断言全过、翻状态、开分支、**实现中发现设计问题先改文档再改代码**、单测+e2e 同 PR 落盘）。

## 3. 每个 phase 的工作流（3 阶段 · 7 步）

### 阶段 A — 规划
1. **多专家对抗性草拟 plan（用 Workflow 工具）**：主进程**为当前 phase 现场草拟一个 workflow 脚本**（按该 phase 的范围/风险定制 fan-out），并行多个不同视角的专家起草 → 对抗性互评 → 综合出候选 plan。
2. **主进程审核并修改 plan**：主进程是 plan 的**唯一定稿人**；定稿写入 `docs/reviews/p<N>-plan.md`。

### 阶段 B — 实现
3. **主进程按 plan 编写代码 + 测试**：连续块；遵守 §5 约定与 `architecture.md` 的不变量。

### 阶段 C — 审查与收尾
4. **多专家对抗性审查代码（用 Workflow 工具）**：主进程为当前 phase 现场草拟一个审查 workflow 脚本，并行多视角专家对抗性审查。**专家只读实现、可自行新增测试条目，但绝不修改实现代码。** 报告写入 `docs/reviews/p<N>-review.md`（多轮则 `-round2`/`-round3`）。
5. **主进程评估审查正确性并修改**：逐条采纳/驳回 finding；整合专家新增的测试；**只有主进程能改实现**。
6. **外部审查（用户本人）**：提交给用户做最终人工外审；用户出报告 `docs/reviews/p<N>-external-review.md`，主进程评估后**在报告文件内逐条回复**并修改。**外审不过不算 done。**
7. **phase 结束**：`git commit` + `git push`（见 §6）。

> **Workflow 不预置固定文件**：步骤 1、4 的多专家编排**每个 phase 自己即时草拟脚本**（用 Workflow 工具的 inline `script` 跑），不维护复用的 `.claude/workflows/` 文件。fan-out 的专家维度按当前 phase 现定。每个 phase 完成后**停下等用户外审/确认**再进下一个。
>
> **Workflow 模型硬约束**：所有 `agent()` 调用（drafter / critic / synth 等任意 subagent）**一律不得低于 Opus 4.8**——**禁 Haiku、禁 Sonnet**。做法：在 `agent()` 上**省略 `model`**，继承会话主模型（= Opus 4.8 `claude-opus-4-8[1m]`，最稳）；同理 `meta.phases[].model` 不设。fan-out 的 agent 数为**静态常量**（不由上一阶段动态决定）。若误用了低于 4.8 的模型跑出结果，**弃用并改 Opus 重跑**（resume 时改 `model` opt 会让缓存失效、自动重跑）。

## 4. 角色边界（不可越界）

| 谁 | 能做 | 不能做 |
|---|---|---|
| **主进程** | 定稿 plan、编写/修改实现、采纳审查、整合测试、commit/push | — |
| **专家**（workflow 内 agent） | 草拟 plan 建议（step1）；审查 + **新增测试**（step4） | **改实现代码**、定稿 plan、commit |
| **外部审查**（用户本人） | 独立人工外审、出报告 | 改代码 |

## 5. 编码与测试约定

- **语言**：团队讨论用中文；**代码 / 标识符 / 注释 / commit / 日志 / 错误串一律英文**。
- **Go-only**：`CGO_ENABLED=0` 静态二进制；工具链锁 **Go 1.25**（由 `nats-io/jwt/v2 ≥ v2.8.1` 拉高，**升级依赖前必验 go directive**）。
- **不变量**：以 `architecture.md` 为准（控制面/数据面分离、auth_callout nkey 身份、G.1/G.2 reconcile、proto wire 版本一致性、session ACL 隔离、port 分配带 …）。实现与审查都以它为尺。
- **wire 协议**：`internal/proto.ProtoVersion` 是 SSOT；任何破坏性 wire 变更走整次跨版本路径（`tether.v1.*` → `v2.*`），**不兼容则必须重装而非 upgrade**。
- **测试纪律**：
  - 表驱动；快测 `make test`（`go test ./...`，用嵌入式 `nats-server/v2/test`，**不需要本机 nats-server**）。
  - 端到端矩阵 `make e2e`（`-tags e2e_matrix`，`test/e2e/all_phases_test.go` 每 phase 一个子进程子测试；单 phase 用 `go test ./test/pX/...`）。**新 phase 的 e2e 进矩阵，作为跨 phase 回归网。**
  - 并发安全：`-race` + **仓库内建泄漏门**（`runtime.NumGoroutine` poll-with-tolerance + fd 基线，见 `test/concurrency/helpers_test.go`；**刻意不用 `go.uber.org/goleak`**）；触碰隧道/PTY/reconcile/传输/Raft 等并发面必须带 race + leak 门。
  - lint：`make lint`（golangci-lint **v2**；v1 在 Go 1.25 模块上会拒跑）。
- **提交前硬闸**：`make test` + `make e2e` + `make lint` 全绿，并发改动另过 `-race` + 内建 NumGoroutine/fd 泄漏门（非 goleak）；**任一不过不算 done**。

## 6. Git

- 只在 phase 收尾（step 7）、内审+外审通过后才 `commit`/`push`；在默认分支（`main`）上**先开分支**（`phase/<N>-<slug>`，每个 phase 至少一个 PR）。
- commit message：conventional commits `<type>(scope): <imperative summary>`（如 `feat(ps): retention-bounded ps RPC`、`fix(auth): grant $JS.API.DIRECT.GET`）。
- **绝不添加 `Co-Authored-By: Claude` 或任何 AI 署名**（全局规则；已推送的若混入，用户会 rebase 移除）。作者/协作者只保留用户本人。

## 7. 当前状态（截至 2026-06）

> **⚠ 双线地雷**：`main` 顶端已是 **proto v2**（distributed-broker **D0** 已 commit），**连不上现网 proto-v1 车队**。面向用户的 patch 走 **v1 线**（从 `v0.3.5` 切 release 分支 + cherry-pick），**绝不从 main 发**给现网；详见 memory `project-release-lines-v1-v2`。

- **主线**：P0–P11 → **v0.1.0**（GitHub release，里程碑全达成）。
- **已发布的 post-1.0 叶子增量**（v1 线，按 tag 序，proto 始终 v1）：
  - file-transfer（`push`/`pull`，tier-A inline + tier-B JetStream Object Store）— **v0.2.0**（后 tier-B 上限提到 2 GiB）
  - run heartbeat watchdog、retention-bounded `ps` RPC + processes GC — **v0.2.8**
  - **P12** expose `--remote-port`（带内唯一索引仲裁） — **v0.2.9**
  - **P13** session-scoped proxy 订阅（内嵌纯 Go shadowsocks + 试解密多密钥，broker 托管 HTTPS 订阅 URL） — **v0.3.0**
  - compliance-cleanup 审计加固 — **v0.3.1**
  - transfer-unrestrict（`file_transfer.allow_roots` 可选收紧、缺省全盘） — **v0.3.2**
  - remote-fs-resilience（网络盘挂死时 `exec`/`run` 不卡死，`--safe`） — v0.3.3（随 v0.3.4 发布）
  - proxy-tunnel-reconnect（反向隧道自愈重连 + 就绪 liveness） — **v0.3.4**
  - history-snapshot-race（NumPending 驱动快照完成，修远程 broker 空输出） — **v0.3.5**
  - proxy-aware-dial（ctl/agent NATS 经本地代理：HTTP CONNECT + SOCKS5h，解 WSL fake-ip hang；默认关、零回归） — **v0.3.6**（WSL 实机验证过）
- **distributed-broker HA epic（D0–D9，proto v2，在 main）**：架构基线 `docs/distributed-broker-architecture.md` 过 4 轮外审 PASS；分解见 §19。
  - **D0**（前置门 + proto v2 SSOT + migrations 0008–0010 + PreVote 合并门）— **已 commit 进 main**。
  - **D1**（状态层：单节点 Raft FSM + SQLite Apply 同 txn `applied_index` + 幂等重投 + online-backup 快照/恢复 + kill-9 矩阵）— **DONE**（内审 + 外审 PASS，commit `bc181c1`）。报告 `docs/reviews/d1-{plan,review,external-review}.md`。
  - **D2**（op 集 + 全 mutator Plan/Apply 移植 → **N=1 功能等价**）— **DONE**（内审 + 外审 PASS，commit `38e2576` + pushed main）。**ops-only 不切线上 broker**（cutover=D9）；全 13 op + sqlbake(`LitTime=t.String()` 不强制 UTC、禁 `Args`、拒 NUL/非 UTF-8) + `Node.Propose` applyMu 接缝 + CHA §13.1 lint + DIFF-1 差分(UTC+非UTC) + TestD2Matrix。报告 `docs/reviews/d2-{plan,review,external-review}.md`。
  - **D3**（NATS 集群层 ≥2 节点：routes mTLS + auth_callout 本地读 + fail-closed `T_fence` + broker-only ACL）— **DONE**（内审 + 外审 round-2 PASS，commit `d15472d` + pushed main）。**build-and-prove 不切线上 broker**（cutover=D9，同 D2）：真 mTLS raft `NetworkTransport`(无 `NewTLSTransport`)+静态 `BootstrapPeers`(动态 AddVoter=D7) + 单一 `LeaderContactStale` 谓词(leader 无状态/follower `LastContact`，读路不调 VerifyLeader) + RF1 `tether.v2.cluster.*` ACL(SSOT proto，仅 `PermissionsForBroker`) + `internal/natscluster` conf 渲染器 + auth_callout 本地读 fail-closed + PIN 写经 raft-free `Node.Propose` seam(`cluster.IsNotLeader` 映射)。生产 `serve.go` 不构造 `cluster.Node`、seam 缺省 no-op(guard 测试锁)。**外审 F1 修**：`Propose` 跑 leader-only Plan 前先门 leadership(非 leader 返 `raft.ErrNotLeader`，不在 follower 陈旧副本跑 Plan)。报告 `docs/reviews/d3-{plan,review,external-review}.md`。
  - **D5**（审计可重导单写 + JS 流副本重配）— **DONE**（内审 6×Opus CONDITIONAL→全修 + 外审 round-2 re-review PASS，commit `0c2ec45` + pushed main）。**build-and-prove 不切线上**（cutover=D9；`serve.go` 字节不动、生产 `Broker` 不构造 `cluster.Node`、publisher loop 永不启动、live `publishAudit`/`reconcileOnRegister` 字节不变，guard `TestD5ProductionWiresNoClusterNode` 锁死）。**机制 A**=`OpAuditCheckpointSet`(FSM 烤单调守卫 UPSERT)推进复制游标 `audit_published_index`，**游标 op 是 publisher 硬跳过、不推过自身**(外审 F1 自激修：后续真源 entry 隐式带过、空闲零 raft 写) + 6 个 raft-free `cluster.Node` 读原语(`CommitIndex`/`LogFirstIndex`/`CommittedCommandAt`/`AuditPublishedIndex`/`AdvanceAuditPublished`/`NumVoters`)+`TrailingLogs` 常量 + `internal/broker/audit_publisher.go` 合一 leader-only `Run`(天花板 `CommitIndex`、地板 `max(checkpoint,LogFirstIndex)` 每轮重夹、批量 checkpoint、截断 loud-loss no-wedge、R-7 lag 门、R-22 排队不丢、sid 注入守) + reqID-keyed 去重 id(`q<reqID>:…` 收 D4 `appliedDedup` 重试双发，否则 `r<idx>:…`)；仅 `OpReconcileBatch` 可重导(`proc.ReplayReconcileAudit`)、单写者=跨选举同 id 收拢。**机制 B**=`jsstream.ReplicasFor`(1,2,3 封顶3)+`Duplicates` 窗口+create-or-RAISE `ensureStream`(只升不降)+`IsMetaGroupNotReady`(排除永久 JS-10074 non-clustered)+`ActualReplicas`/`AllAtTarget` canonical 预测语；`transfer.go` OBJ_xfer `UpdateObjectStore` 重配。**升配门=`UpdateStream` 拒分类重试**(非 `metaGroupCanHost` 前置门，R1 流 Cluster 恒列 0 peer)，`Cluster` 仅算 actual。live 调用方传 `jsstream.ReplicasSingle`(R=1 字节等价)。首个**路由 NATS + mTLS raft + clustered JetStream** 三合一 `test/d5`(重 harness gated `//go:build d5_integration`、跑在 `TestD5Matrix -race`；廉价 guard/window + cluster/broker/jsstream 单测在 `make test`)。三道 guard：build-and-prove token-scan、`internal/cluster` no-NATS、`internal/jsstream` no-cluster。报告 `docs/reviews/d5-{plan,review,external-review}.md`。
  - **D4**（写转发 `apply.*` follower→leader + 跨重试幂等 + ReconcileBatch leader 权威自足）— **DONE**（内审 2B+8M+12m + 外审 round-2 re-review PASS，commit `4ca8891` + pushed main）。**build-and-prove 不切线上**（cutover=D9）：**forward wire**=broker-owned `internal/broker/cluster_forward.go`，broadcast `Subscribe`+**仅 leader 应答**(follower 静默；queue-group 无 leader 亲和、follower not_leader 会抢 leader ok) over broker-only `tether.v2.cluster.apply.<verb>`；typed `{ok,not_leader,error}`，超时/malformed/未知=可重试，`ForwardBusinessError` 保 sentinel(`errors.Is`→转发坏 PIN 发 `pin_failed`+canonical deny)。**幂等**=migration 0011 `cluster_reqid_ledger` 同-Apply-txn 去重(`appliedDedup` 推进非回滚 + 确定性 in-txn GC)；**只 reconcile 带键**(bootID epoch、按原始请求推、护 D5 发布)；**provision/join 不带键**(`INSERT OR IGNORE`+绑定可 evict 删→纯内容键会误去重 evict 后重 provision)，**wire boundary 拒非空键**(`ErrReqIDNotAllowed`，外审 RF1)；`ProposeWithReqID` 提交前校验键(外审 F2，非法键不成 poison 假 ok)。**ReconcileBatch**=`commandVersion`→2 + apply-inert `Aux` 有序审计元组(killed_orphan 无 rc、Ts 剥单调) + 纯 `proc.ReplayReconcileAudit`(**D4 发布零、D5 边界**)；live `reconcileOnRegister` 字节不变(零回归)。`internal/cluster` nats-free(L-2)；生产 `serve.go` 不 wire(guard 测试锁)。报告 `docs/reviews/d4-{plan,review,external-review}.md`。
  - **D6**（数据面分布式：expose/tunnel 跨 home broker 故障切换）— **DONE**（内审 6×Opus CONDITIONAL→全修 + 外审 round-2 re-review PASS，commit `d403f38` + pushed main）。**build-and-prove 不切线上**（cutover=D9；`serve.go` 字节不动、生产 `Broker` 不构造 cluster seam、tunnel server 不给稳定 cert，guard `TestD6ProductionWiresNoClusterNode` 扫 serve.go+broker+agent 锁死，含字段写/结构字面量 token + 剥注释）。七机制：(1) **server-id 桥接** agent 报 `nc.ConnectedServerName()`(确定性 server_name、非易变 NUID)→ 复制态 `nodes.nats_server`(migration 0012、live `node.Register`+FSM `PlanRegister` 双写保 DIFF-1)→ `internal/clusternodes`(纯 SQL leaf、L-2) 解析 home；(2) per-expose `home_broker/epoch` + `OpPortReassignHome`(leader 烤 epoch+1 字面 + 单调 CAS)；(3) `tunnelTokenLookup` 二维 home/epoch ladder(`home==''` inert)；(4) `home_catching_up` transient(共享 `proto.ReasonHomeCatchingUp`、broker 发+agent `denyIsTransient` 同改) + REGISTER 第 6 字段 `<epoch>`(收正好 6)；(5) catch-up = **epoch-as-local-row-epoch**(非 `applied_index>=raft-commit`、域不兼容)；(6) agent 自驱 rehome(**每口去重重试 loop** + goroutine/leak 门 + `rehomeSeq` 序号判定重应用 + 单调持久 + **deferred replay** 缺 pins 延迟、tri-state `openHomeFromState`)；(7) 稳定 cert + `cert_pins{current,previous,valid_until}` 钉证(`VerifyConnection` resumption-safe) + **same-epoch pure-pin 轮换原地更不撕**(redial gen-fenced 读会话 pins)。`internal/broker/home.go` build-and-prove 文件;新建多 broker+agent failover `test/d6`(gated `//go:build d6_integration`、跑在 `TestD6Matrix -race`)。**外审 Fail→Fail→Pass**：F1 `handleRegister` 漏接 `req.ServerID`(home bridge 整条死)、F2 deferred replay 不重开、F3 stale terminal 删更新 directive、F4/RF2 same-epoch pure-pin 丢、RF1 不可读 state 误清 deferred——全修。过程抓修一**真死锁**(嵌套查询 under `MaxOpenConns(1)`)。报告 `docs/reviews/d6-{plan,review,external-review}.md`。
  - **D7**（集群生命周期 + membership 两阶段 + force-single 逃生）— **DONE**（Stage C 6×Opus 5B+10M+12m 全修 + 外审 Fail→Pass，commit `a0704c3` + pushed main）。**build-and-prove 不切线上**（cutover=D9；`serve.go` 字节不动、生产不构造 `cluster.Node`、online cluster 子命令 backend nil 时 fail-fast "cluster mode not enabled"，guard `TestD7ProductionWiresNoCluster` 扫 + 双向 self-check）。核心：(1) **membership 两阶段**=`OpClusterNodeUpsert`(唯一自定义 applier、follower Apply 复算 join-PoP ed25519、**伪签/约束失败=POISON-SKIP via `errAppliedRejected` 推 applied_index 不 panic**、位置化 Aux↔Body 交叉校验)→ 仅 committed 后 `raft.AddVoter`；半成功 phase 机(`JOIN_VERIFIED_PENDING_VOTER`→`CATCHING_UP`→`VOTER`/`VOTER_ADD_FAILED`)+ **leader-startup reconciliation pass**(no-silent-fork 真保证=提交序+调和、非 status 渲染)。(2) catch-up = **command-domain `AppliedIndex` barrier**(非 raft CommitIndex)。(3) Node membership 包装取/返纯字符串→raft 限 `internal/cluster`(L-2，broker/clusteroffline/adminsock/cmd 不直接 import raft)。(4) drain/retire = serving-set quorum 投影守卫(F==0 typed confirm 拒 `--yes`)+AllAtTarget+只迁 rebuild-ON(rebuild-OFF 枚举拒)+targeted transfer-leader-first+拒 retire 最后 voter。(5) **force-single/recover offline**(`internal/clusteroffline`→`cluster.RecoverSingleNode`)=flock+BoltDB 活 daemon 探测+空态拒+peer TCP-liveness HARD-REFUSE+`RecoverCluster` 自驱两存储重放(恢复点=本地 LastIndex、不双应用)+dump 全 Apply-owned 表 O_EXCL+fsync 先于 wipe；`force_single_active` 复 HA 时清。(6) `rotate-tunnel-cert`=`OpClusterCertRotate`(cert_fp_prev=cert_fp,cert_fp=new,valid_until)。(7) status 双视图=per-broker self-report(versioned `schema_version`、`reach_source`、NATS 不可达=UNKNOWN 非 DEAD、exit 2 仅来自肯定无 leader 自报)+offline disk-roster 快照；**多 broker NATS 聚合+:7400 raft-ping=D9**。adminsock cluster ops + 可选 `ClusterAdminBackend`(nil=未启用)+非 leader fail-fast 指名 leader；cobra cluster 组(destructive 无 `--yes`)。migration 0013。新建 gated `test/d7`(`//go:build d7_integration`、`TestD7Matrix -race` 8 drill)。**外审 Fail→Pass**：F1 transfer-leader 忽略目标/F2 rotate-cert stub/F3 drain 静默迁 rebuild-OFF/F4 status 缺 schema_version/F5 runbook recover 缺 --self-id——全修，5 reviewer regression 转绿。另修 2 pre-existing flake(D6 freePort TOCTOU retry、D5 sweep retry)。报告 `docs/reviews/d7-{plan,plan-synth,review,external-review}.md` + `docs/cluster-runbook.md`。
- 实机环境与历史验证见 `log.md`（broker `pc732.emulab.net` / `weiland.top`，多 agent）。

### 已知未收口 / 缺口（接手时优先处理）

- **P13 阶段出口仍是 CONDITIONAL PASS**：真 Caddy/WSS + 真 Clash 端到端验证未跑（外审不过不算 done）。
- **e2e 矩阵覆盖洞**：`test/e2e/all_phases_test.go` 覆盖 p1–p10 + p13 + 叶子矩阵（TransferDefaults / RemoteFS / ProxyTunnelReconnect / **ProxyDial** / **D1** / **D2** / **D3** / **D4** / **D5** / **D6** / **D7**）；**p11 及 post-1.0 的 file-transfer / ps-retention / P12 仍未进矩阵**。新增量进矩阵时一并回填。注：`TestD5/D6/D7Matrix` 各加 `-tags d{5,6,7}_integration`（重 clustered-JS/routed-NATS/raft 集成套件 gated 出 `make test`，只在专用 `-race` 子进程跑——`make test` 并行下会饿死超时；廉价 guard/unit 仍在 `make test`）。**重 -race matrix 时序敏感**：`make e2e` 满负载下偶发 flake（D6 freePort TOCTOU、D5 选举后 sweep），已对相关测试加 retry/更宽窗口；新写重集成测试避免固定窗口端口/选举等待。
- **D2 D9-staged 项（非欠账，已在 d2-review 确认边界）**：禁 FSM 外 INSERT lint（live 直连 mutator grandfathered）、leader-local 不碰身份列反向断言、`reconcileOnRegister`↔`resolveReconcileMarks` 真正共用重构、broker 切 FSM——全是 D9 cutover。
- **`README.md` 为空**（0 字节）——对外门面缺失。

### 下一步

- **D7 DONE（外审 re-review PASS，已 push main，`a0704c3`）**；下一步 **D8**（文件传输分布式 ‖ 告警系统，两叶子可并行）：**D8a transfer**=§9 终结于 home + `push/pull` 经 §4.1 转发 + audit start/complete/failed 经 leader Apply；tier-B `ensureXferBucket` `Replicas=replicasFor(nVoters)` + 重配；in-flight tracker/watchdog 在 home best-effort（rehome 不保在途、客户端重启）。**D8b alerts**=§10 leader 复制 `alerts`/`alert_acks`（集群级单 ack + 严重每新会话重现）+ 三条 severe 客户端合成 gating（server 佐证）+ §10.2 目录（含桥接 `disk_pressure`/新 `raft_lag`）+ §10.3 banner（所有人、不脱敏分层）+ §10.4 destructive 闸枚举。**依赖**：D8a tier-B 重配依赖 D5 `replicasFor`/`AllAtTarget`（已就位）；D8b 复制 alert store 是 D5 留的 D8b 项（force_single_active/quorum_lost 仍客户端合成、非 alerts.kind）。出口：tier-B 对象杀 home 存活；severe 告警正确 gate destructive 且不误伤。注意 proto v2 不发现网 v1 车队。D8 需用户新 /goal 启动。
