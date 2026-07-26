# 批次 A — 定稿 plan（结构性质量债 · 低风险高收益档）

> **流程位置**：CLAUDE.md §3 step 2。本文件是**主进程定稿**，是实施的唯一依据。
> **输入**：`docs/batch-a-roadmap.md`（执行路线）、`docs/reviews/quality-audit/2026-07-25-structural/S1-refactor-roadmap.md` §4（15 项论证）、
> 6 份专家草案 + 6 份对抗性批判（step 1，`$JOB/tmp/batch-a-drafts/D1..D6-*.md`）、主进程独立建立的 ground truth。
> **定稿人**：主进程。专家的建议已逐条采纳/驳回，理由见 §7。

## 0. 本次定稿推翻了 roadmap 的两条内容

写 roadmap 时的两个判断，在 step 1 之后被证据推翻，**以本 plan 为准**：

1. **验证门 L3「不适用、不跑 simcluster drill」不成立**（详见 §2）。A1 的产出就是改退出码，而 deploy-tier drill 里有对退出码的**数值硬断言**。
2. **A1 Step 1 的「二进制字节等价」验收门不可执行**。Go 把 build ID（由全部输入文件计算）嵌进二进制，新增 `internal/proto/codes.go` **必然**改变 build ID，`cmp` 必然失败。改用 §4-A1 给的替代验证。

## 1. 范围裁决

15 项**全部保留在批次 A**，无一推迟或丢弃——但 **9 项改动了做法**。改动均来自对抗性批判中被证据支持的攻击。

| 项 | 裁决 | 相对 S1 的改动要点 | 编码人日 |
|---|---|---|---|
| A1 | **proceed-modified** | 码宇宙不写死数字；扫描器三段规格 + 逐形态自检；新增 drill/e2e 退出码断言扫描；换 Step 1 验收方法 | 2.0 → **4.0** |
| A2 | **proceed-modified** | **反向做**：删 `handleCapsReq` 的 `Error: err.Error()` 去调现有 gate，**不改 gate 签名** | 0.25 |
| A3 | **proceed** | 加 `cluster-runbook.md` 进文档 sweep | 1.5 |
| A4 | **proceed-modified** | `port.Revoke` 改为"只删假 godoc、保留函数"；删 `subhttp.Serve` 须同 commit 重定向护栏测试 | 1.0 |
| A5 | **proceed** | 加 `admin runtime --json` 的 schema bump 判定 | 1.0 |
| A6 | **proceed-modified** | **只取第一方案**（kind 校验），签发路径一字节不动；同 PR 删生产零引用的 `IssueUserJWT`/`AccountPublicKey` | 0.5 |
| A7 | **proceed-modified** | 落地前必须先回答 revert 语义三问 + 查 JetStream subject filter | 1.0 → **1.5** |
| A8 | **proceed** | — | 1.5 |
| A9 | **proceed** | `s.closed` 不进 `fenceChangedLocked` | 0.5 |
| A10 | **proceed-modified** | 范围收窄到 2 处；删除两条经证伪的"风险" | 0.5 → **0.25** |
| A11 | **proceed-modified** | 是**四份**不是三份；`proxysub.HashToken` 已导出 | 0.25 |
| A12 | **proceed-modified** | 是**三个** Bind 不是两个 | 0.75 |
| A13 | **proceed** | release note 证据方法订正 | 0.5 |
| A14 | **proceed** | — | 0.5 |
| A15 | **proceed-modified** | 无条件加去重/限速窗口（不依赖 racknerd raft config 的未知状态） | 1.0 |

**编码合计 ≈ 14.75 人日；增量数 = 1（一次外审）。**

> **工作量口径**（采纳批判 D6-c 的意见）：本仓真正的成本大头是**增量数 × 7 步流程**，不是编码人日。
> 本批次刻意保持 **1 个叶子增量**——任何"把某项移到批次 B"的提议都必须说明它是否减少增量数；
> 若不减少，只是给 B 批增加带上下文债的尾巴，是净增成本。这是本批次不做任何裁剪的**主要理由**。

## 2. 硬边界（修订版）

1. **零 wire 变更** —— `ProtoVersion` 不动；所有错误码/subject 的**字符串值一个字节不改**。
2. **零不变量变更** —— 周围带 `INVARIANT` / `DELIBERATELY` 注释的代码不动。
   特别点名：`internal/cluster/fsm.go:80-89` 的 committed-OUTCOME FSM 结果类型（10 行 INVARIANT 明令禁止给它们加 `Error()`）——**A4 不得触碰**。
3. **不碰部署面** —— 不改 `install.sh` / `nats.conf` 渲染 / systemd unit / 集群生命周期动作。

### 2.1 ⚠ 验证门 L3 修订：A1 必须先过退出码断言扫描

roadmap 写的「本增量不碰部署面 ⇒ 不需要 drill」**对 A1 不成立**。实测 deploy-tier drill 里有对退出码的数值硬断言：

```
test/simcluster/drills/50-backup-restore.sh:197    _doc_db_rc64() { …; [ "$_drc" = 64 ]; }
test/simcluster/drills/82-agent-onboarding-invite.sh:90   [ "$_drc" -eq 77 ]
test/simcluster/drills/71-expose-rehome-failover.sh:101,289   assert 挂在 rc == 75
test/simcluster/drills/61-transfer-edges.sh       按 code=<X> grep（roadmap 已知）
```

**新增强制验收项 V1（零成本，且是兑现硬边界 3 的唯一证据）**：

> A1 Step 2 定下每个改类码之后，逐码 `rg` 一遍 `test/simcluster/drills/` 与 `test/e2e/`，
> 确认**没有任何 drill/e2e 对该码的旧 exit class 做数值断言**。
> - 扫描全绿 ⇒ L3「不跑 drill」成立，写进 plan 出口断言。
> - 有命中 ⇒ 必须跑**对应的那一个** drill（按 CLAUDE.md §5「只跑相关的那一个」），不是全套。

同理，A12 若改动 `/sub` 的 loopback 语义，相关 drill 是 **72-proxy-subscription**（`grep -rln "sub-http-listen\|/sub/" test/simcluster/drills/` → 72/73/74），**不是 93-metrics-observability**（93 跑的是 `--metrics-listen` 私网口，失败模式是启动即挂，响亮而非静默）。

## 3. 实施顺序与 checkpoint

顺序基本沿用 roadmap 的四组，**但有一处必须调整**：

> **A4 删 `subhttp.Serve` 与 A12 存在顺序陷阱**：`internal/subhttp/p13_external_review_test.go:15-22`
> 的 `TestExternalReviewServeRejectsNonLoopbackAddress` 喂 `"0.0.0.0:0"` 给 `Serve`，是**唯一守住
> /sub loopback 边界的护栏**。组 2（A4）在组 3（A12）之前 ⇒ A4 先删掉护栏的入口，A12 才动 Bind，
> 中间存在一个**无护栏窗口**。
> **约束**：删 `subhttp.Serve` 的**同一个 commit** 内必须把该测试重定向到 `Bind`（A12 落地后再改指 `httplisten.BindLoopback`）。A4 的出口断言显式点名这条测试必须仍在且仍可翻红。

```
组 1  A1 → A2 → A3                     （错误面收口；A1 最先，其余两项都要往它的分类表加条目）
组 2  A4 → A6 → A7 → A13 → A11         （删除面）
组 3  A9 → A10 → A12 → A14 → A5 → A15  （结构收口）
组 4  A8                                （文档；必须最后，它同步前三组落定的结果）
```

**安全 checkpoint（工作树一致、测试全绿、可安全中断）**：C1 = A1 完成后；C2 = 组 1 结束；C3 = 组 2 结束；C4 = 组 3 结束；C5 = 组 4 结束（= L2 硬闸）。

**部分落地策略**（采纳批判 D3-盲区#5）：本仓直落 main、无分支兜底。外审若否掉个别项，**默认工具是「该项先不落」而非 revert**——因此实施时每项自成 commit、不跨项混合，使任一项可被单独摘除而不需要顺序表。

## 4. 逐项施工卡

仅记录**相对 S1/roadmap 的增量决策**；未列出的部分按 S1 §4 执行。

### A1 — 错误码 exit-class SSOT（4.0 人日）

**码宇宙的真实规模**（主进程 AST 实测 + 批判者独立复核）：

| 口径 | 数 |
|---|---|
| S1 的规格（`Code:` 字面量）能看见 | 39 |
| 主进程扫描器（+ 具名常量 + 12 个自动发现的 helper 实参） | **95** |
| 批判者另发现的形态（函数返回码、`Reason:` 字段） | 使真实宇宙 **更大** |

**已确认的发射形态（≥7 种）**：① `Code:` 字面量 ② `Code:` 具名常量 ③ helper 实参（`replyExposeErr`/`replyPushErr`/`fsRefuse`/`pathErr` 等 12 个）④ `return namedConst` ⑤ `return "literal"`（`transferGate`/`proxyGate`/`forwardErrKind`/`xfer_provision.go:122`）⑥ `return fmt.Errorf("literal")` ⑦ broker 透传 `Code: ar.Code` ⑧ **`RunChunk.Reason` 是另一个字段名**（`agent/run.go:81,87,100,120,139`）。

**⇒ 决策 D1：静态枚举全部错误码不可判定，plan 不写死任何数字。**
扫描器规格分三段，**每段各配一个合成的未归类自检样例**（仿 `test/determinism/lint_skeleton_test.go:262` 的 `TestNoStrayVersionLiteral` 自检范式）：

1. `Code:` / `Reason:` 的 KeyValueExpr（值为字面量，或用 `go/types` 求常量值）
2. 已知 code-carrying helper 的实参位 —— **helper 名单本身要有断言**（新增 helper 未登记则失败）
3. `Code:` 指向**变量或函数返回值**时 —— **硬失败并要求显式豁免**，否则"把码提成变量"就能静默逃逸整条守门

**动态拼接码**（`expose.go:315,316`、`upgrade.go:131`、`cluster_upgrade_trigger.go:161` 的 `"agent_rejected:" + ar.Code`）单列为 **report-only** 输出，不硬失败。
注：`agent_rejected:` 前缀剥离**已经实现且有测试钉住**（`error_hints.go:158-166` + `error_hints_test.go:80`），不是缺陷。

**⇒ 决策 D2：Step 1 的验收方法**（原「二进制 `cmp`」不可执行）：
改为 `go build ./... && go test ./cmd/tether/ ./internal/broker/` 全绿 + **人工 diff 审查**确认新建的 `codes.go` 只有常量声明、无逻辑。常量值与原字面量的一致性由 Step 3 的守门测试保证。

**⇒ 决策 D3：只把跨包共享的码搬进 `proto`**。判据：同时出现在 `internal/broker/*.go` 和 `cmd/tether/error_hints.go`。broker 包内私有的码留在原地——搬过去会制造新的跨包同步点。

**⇒ 决策 D4：`dataplaneNotConvergedCode` 可以搬**。批判者已核实 `error_hints_test.go:181` 是 `const wire = "dataplane_not_converged"` 写死在测试内的字面量，搬到 proto 后该断言变成"proto 常量必须等于测试内写死的 wire 字节"——**比今天更强**（今天只钉 cmd/tether 一侧）。

**⇒ 决策 D5：`home_broker_restart` 进豁免白名单并注明 audit-only**（`xfer_inflight.go:504` 是 `schema.AuditTransfer` 而非 ctl reply）。豁免白名单**每条必须带理由字段**。

**⇒ 决策 D6：不写"所有退出码都来自分类器"式的全局断言**。`docs/usage.md:1539` 定义了第二套正交契约：`0..3` 仅 `cluster status` 健康码、`exec`/`run` 透传远端退出码（任意 0..255）。守门断言必须排除这两个保留区间。

**⇒ 决策 D7：Step 4 的 40 处 `"<code>: "+err` 拼接**——改之前先分类哪些进 stderr、哪些只进日志。**运维可见的字符串一律保持字节不变**（`transferRefusalErr` 已有豁免，理由是 drill 61 按 `code=<X>` grep）。

**验收**：V1（§2.1 的 drill/e2e 退出码扫描）+ 守门测试从红转绿 + 三段自检各自能抓到合成样例。

### A2 — `handleCapsReq` 收口（0.25 人日，-17 行）

**⇒ 决策 D8：反向做，不改 `transferGate` 签名。**

S1 的原方案会丢 `Error: err.Error()` 详情；草案提议改签名为 `(code, detail)` 以保住详情——但批判者证明该改法有外溢面：`transferGate` 现有 **4 个**调用点（`transfer.go:567/719/852/1144`）**今天全部丢弃 err 明细**，加上 detail 后会给 push/pull/commit/finalize 四条 RPC **新增**原本不存在的裸 SQLite 错误串，发给任意 session member（DB 路径、表名、约束名外泄给非 owner）。

**采纳做法**：删掉 `handleCapsReq` 的两处 `Error: err.Error()`，改调现有 `transferGate`。真·零外溢，错误码逐字不变。
**并在 plan 中确立一条通则**：`store_error` 的 DB 明细**一律只进 broker 日志、不进 wire**。

### A4 — 死符号与误导性 godoc 扫除（1.0 人日）

**⇒ 决策 D9：`port.Revoke` 改为「删假 godoc、保留函数」。**

理由：批判者查出 `port.Revoke` 有 **6 处引用**（`port_test.go:150/180/232/342/370` + **跨包的 `test/cluster/equiv_test.go:422`**），且 equiv_test 里它不是随手用的——`run(false)` 调 `port.Revoke`、`run(true)` 调 `PlanRevoke + cluster.ExecCommand`，然后断言 `logicalHash` 相等。**它是单机路径与集群路径的等价性对照臂。**

同时主进程实测：`Revoke` 幂等（`RowsAffected==0` 回查存在则返回 nil），`RevokeAllocation` 直接 `ErrNotFound` —— **语义不等价，不能 sed 全替**，`port_test.go:180/232/342/370` 依赖的正是幂等语义。

**真正的危害是 godoc 而非函数本身**：两份 godoc **互相矛盾**——`Revoke` 自称 *"used by the broker reconciler"*，`RevokeAllocation` 自称 *"**Offline-node reconciliation** uses the full row identity … so a delayed revoke cannot affect a later allocation that reused the same port"*。而 `Revoke` 的 `WHERE port=?`（无行身份）**正是后者明说要防的那个 race**。

**采纳做法**：删掉 `Revoke` godoc 里 *"used by the broker reconciler when the owning node has been OFFLINE long enough (architecture D.4 / F.3)"* 这句假指引，改写为明确的"**仅供等价性测试对照臂使用；生产路径一律用 `RevokeAllocation`（行身份谓词，防端口复用 race）**"。函数保留，`equiv_test.go` 不动。净删量相应下修。
`planPortStateChange` 被 `PlanFree` 共用，**不随 `PlanRevoke` 删**。

**⇒ 决策 D10：`subhttp.Serve` 的删除必须同 commit 重定向护栏测试**（见 §3）。

**⇒ 决策 D11：`internal/auth/permissions.go:42-46` 加进本项的"误导性 godoc 订正"清单**——它承诺 *"tetherd performs the owner check on the application side and replies `admin_denied` for non-owners"*，而 `kick`/`rotate-pin` **根本没有 handler**，不可能回 `admin_denied`。这比删 grant 更符合批次 A 的性质。

**⇒ 决策 D12：`deadcode` 重跑的两个已知撒谎方式**（roadmap §6 已记）：必须带全部 8 个 build tag；`go/packages` 要处理 `pkg [pkg.test]` 变体。

### A6 — 接线 account seed kind 校验（0.5 人日）

**⇒ 决策 D13：只取第一方案。签发路径一个字节不动。**

批判者证明第二方案（`AuthCalloutConfig` 持 `*AccountSigner`、`uc.Encode` 改调 `IssueUserJWT`）会在**唯一生产 broker 的 auth 热路径上丢字段**：
- `internal/authcallout/handler.go:428-447`：设 `uc.Audience = target`（空则 `"$G"`），用可注入时钟 `h.Now()`
- `internal/auth/jwt.go:48-69` 的 `IssueUserJWT`：**没有 Audience 概念**，写死 `time.Now()`
- nats-server 用 Audience 决定把连接放进哪个 account ⇒ 少这个字段 = **所有 agent/ctl 全连不上，单 broker 无处可切**

**采纳做法**：
1. `cmd/tether/serve.go:437` 的 `loadAuthCalloutSeeds` 里调 `auth.LoadAccountSigner(accountSeed)` 做 kind 校验，**丢弃返回的 signer**（或只留公钥用于启动日志）。
2. **同一 PR 内**删 `IssueUserJWT` + `AccountPublicKey`（生产引用为 0，S2-deletion-inventory §2.1 P1 已独立核实 `internal/auth/jwt.go` 整文件 prod=0）。把 `jwt_test.go` / `test/p1/foundation_risk_test.go` 里针对这两个符号的断言清掉，**把 seed-kind 断言重定向到 `loadAuthCalloutSeeds`**。
3. "消掉第二份 JWT 实现"**推迟到批次 B**，前置条件是先给 `IssueUserJWT` 补 audience + Now 注入并证明两条路径产出的 claim 集合逐字相同。

**现网风险（主进程穷举，为零）**：`loadAuthCalloutSeeds` 仅当 `authSeedsSource != ""` 才调用（显式 flag 或 clusterMode），而 `install.sh` 中 `broker.nk`/`account.nk` 只出现在第 566 行**注释**里。三种情况——未启用 auth_callout（路径不执行）/ 合法 account seed（校验通过）/ wrong-kind seed（当前已是"broker 起得来、全员被静默拒绝"的坏状态，fail fast 是严格改善）——**无一会把正常现网搞坏**。

**表述订正**：malformed seed 今天**已经**启动失败、A6 不改变它；A6 新增的只是 **wrong-kind** 那一支（`SU…` 能被 `nkeys.FromSeed` 正常解析、`authcallout.go:70` 放行）。S1 描述的正是这一支，S1 没判错。

**⇒ 决策 D14（新增，批判者盲区#3）**：A6 改的是**启动期对部署产物的校验**。落地后须确认 `test/simcluster/lib/secrets.sh` 铸密钥路径产出的确实是 account seed，否则 deploy-tier drill 会立刻起不来。这是一次**静态检查**（读那个脚本），不需要跑 drill。

### A7 — ACL 双向对账 + 删死授权（1.5 人日）

**⇒ 决策 D15：落地前必须先回答三个问题，答案写进 plan 的实施记录。**

批判者指出 A7 是**全批次唯一 `git revert` 撤不回效果**的一项（授权模板变化只体现在**新签发**的 JWT 上）：

1. 已签发 JWT 的有效期多长（`h.JWTTTL`）？
2. revert 之后，持旧权限的长连接要不要重连才回滚？
3. 新旧两套权限的客户端能否共存？

**⇒ 决策 D16：删授权前必须验证"NATS core 即刻丢弃"的前提。**
检查这 5 个 subject 是否落在**任何 JetStream stream 的 subject filter** 内——若被 stream 捕获，publish 就是持久化写入（磁盘 + R-7 lag 预算），"无订阅者即丢弃"的前提整个垮掉。

**已确认的有利证据**：`internal/broker/audit.go:36-39` 已有代码注释承认 *"`session kick`/`rotate-pin` **are not implemented**"*，且 architecture H.1 已被 **DOC-12 修正过**。A7 是把 DOC-12 做完，有明确先例。
**豁免表只引 `architecture.md:1198-1205`（DOC-12）这一条**——批判者查明 `distributed-broker-architecture.md:435(k)` 讲的是 destructive gate（`--ack-alerts` 门）该 gate 哪些动词，与 ACL pub 模板无关。

### A10 — `finalizeTransfer()`（0.25 人日，-20 行）

**⇒ 决策 D17：范围收窄到 2 处。**

S1 点的 5 个站点不是同一种东西：

| 站点 | 实际是什么 | 并入 |
|---|---|---|
| `transfer.go:429`（watchdog） | emitTerminal → deleteObject，**无 cancel** | ✅ |
| `transfer.go:948` | emitTerminal → deleteObject → `if ent.cancel != nil` | ✅ |
| `transfer.go:916` / `:1183` | **构造 `rec`**，不是终态处置 | ❌ 另一件事 |
| `xfer_inflight.go:511` | **#57 崩溃恢复**：M1 不变量要求 *"ledger 只能在 synthetic terminal **durably COMMITTED** 之后删"*，删除时机与另两处**相反** | ❌ **绝不合并** |

**⇒ 决策 D18：删掉两条经证伪的"风险"，不写进 plan。**
- ✗「提取时把 cancel 加进 watchdog 会取消正在写对象的 ctx」——假。`deleteXferObject` 显式传 `context.Background()`（`transfer.go:437`），watchdog 路径上调 `cancel()` 是**纯 no-op**。正确表述：*watchdog 那份不调 cancel 是因为它就是 cancel 的所有者*。
- ✗「`cleanupEntry` 的 `ent` 与入参 `entry` 可能不是同一指针」——假。`transferTracker.put`（`transfer.go:128-139`）存的就是调用方传入的 `*transferEntry` 本体，`get`/`claimFinalize` 返回同一指针。

**真实差异只有三条**（提取时须显式建模）：① watchdog 无 `cancel()`；② `handleEvTransfer` 在 remove 后多打一条 `Logger.Info`（`transfer.go:934-936`）；③ `rec` 装配来源不同。

### A11 — `internal/tokenhash` 叶子包（0.25 人日）

**⇒ 决策 D19：是四份不是三份，且论证要改。**

函数体逐字节相同确认：`internal/port/port.go:470`（未导出）、`internal/tunnel/tunnel.go:1274`（未导出）、`internal/proxysub/proxysub.go:205`（**已导出** `HashToken`）、外加 **`cmd/tether/transfer.go:876-879` 的 `hexSHA256`，其 doc 自称 *"wraps the **canonical** sha256.Sum256+hex.EncodeToString pair"***。

- **新包的理由不是"不存在现成的"**（`proxysub.HashToken` 是导出的），而是**"现成的那份住在错误的包里"**：`proxysub` 带 `database/sql` + `crypto/rand`，`tunnel` import 它会失去 dep-graph leaf 地位。
- 收口范围 **4 处**，且 `hexSHA256` 的 "canonical" 措辞必须一并订正（留着就是下一个假保证）。
- 全仓其余 11 处 `sha256.Sum256`（证书指纹、内容寻址 key、带前缀 audit key）**输入域与语义不同，不要顺手合并**。
- **验收**：`hex(sha256(x))` 不变 ⇒ 存量 `port_allocations.token_hash` 全部继续匹配（须有一条测试证明）。

### A12 — `internal/httplisten`（0.75 人日）

**⇒ 决策 D20：是三个 Bind 不是两个。**
`internal/clustermanifest/manifest.go:21-28` 也有一个 Bind，且同样 loopback fail-closed，在 `broker.go:926-931` 接线。

AST 断言必须覆盖三个包：`subhttp → BindLoopback`、`clustermanifest → BindLoopback`、`brokermetrics → BindAny`；并断言 `internal/httplisten` 之外不存在任何直接 `net.Listen("tcp", …)` 的 HTTP 监听点。

**已确认的三处真实漂移**（`manifest.go:46-56`）：① `go func(){ <-ctx.Done(); srv.Close() }()` 裸 goroutine（另两份用 `context.AfterFunc`）——若 `Serve` 先返回，该 goroutine **活过函数返回**；② `srv.Close()` 硬关掐断在途请求（另两份 `Shutdown(3s)` 优雅排空）；③ `err != http.ErrServerClosed` 用 `!=` 而非 `errors.Is`。
**⇒ A12 的理由是"消除已发生的语义漂移"，不是"消除重复"**——三份里有一份行为不同，而没人决定过这个差异。

**⇒ 决策 D21：删掉「A12 会把 shutdown 推过 `nc.Drain`」这条编造的耦合。**
三个 listener 的 `ServeListener` 都跑在 `broker.go:890-936` 里**无人 join 的裸 goroutine** 中，`Run` 不等它们，`clusterShutdownOrdered` 也不碰它们；3s 优雅关本身还发生在 `context.AfterFunc` 起的**另一个** goroutine 里。结构上无法推迟 `nc.Drain`。
（`clusterwrite.go:132-137` 的 publish-after-Drain 耦合是**真的**，但只涉及 A5 + A15。）

### A13 — 删 `DrainNode` retire 分支（0.5 人日）

前提确认：`cmd/tether/cluster.go:524` 硬编码 `Retire: false`（产品不可达）；`adminsock/protocol.go:207` 的 `Retire bool` 仍在（socket 可达）；已有正确替代 `OpClusterRetire`（*"create+drive a **recoverable** retire op"*）。

**⇒ 决策 D22：release note 的证据方法订正。**
批判者指出草案的取证方法是坏的：`git log -S "Retire: true"` 返回空是因为代码写的是 `Retire: retire`；且 v0.1.0/v0.2.0/v0.3.0 三个 tag 根本没有 `cmd/tether/cluster.go`，`grep -c` 为 0 是空转。
正确取证：`git log -S "Retire: retire" -- cmd/` → `a0704c3`（D7 加入）与 `9b99c0e`（C8 删除），说明历史上 `drain --retire` **确实透传过**。
**结论仍成立**（无已发布 tag 发过 `true`），但 release note 须写明真实残余面：**`a0704c3..9b99c0e` 之间自建的二进制仍会发 `true`**。

### A15 — raft 日志 + 计数器进 /metrics（1.0 人日）

**⇒ 决策 D23：无条件加去重/限速窗口，不依赖现网未知状态。**

批判者用 raft 源码证伪了"日志写满 racknerd 小盘"的具体后果：raft 只为 `configurations.latest.Servers` 中**除自己以外**的 server 起 followerReplication（`raft@v1.7.3/raft.go:582-590`）⇒ N=1 时**零 replication goroutine ⇒ 零条 heartbeat 日志**；且原估速率算错最多 22 倍（忽略了 `internal/cluster/transport.go:127-134` 默认 10s dial timeout，主机宕机/分区下每次尝试阻塞至多 10s ⇒ ≤0.1 条/秒）。

**但存在一个未解的现网变量**：项目记忆记载 racknerd 处于 force-single N=1 且带一个**删不掉的 ghost pc732 VOTER**。若该 ghost 确实在 raft configuration 里，replication goroutine 就存在，日志是**永久**而非仅 drain 期间。

**⇒ 采纳做法（不需要读现网即可安全）**：WARN+ 转发**自带 30s 去重窗口 + 速率上限**。这样无论 ghost 在不在 raft config 里，日志预算都有界。理由写成"便宜的保险"，**不写成"防止磁盘写满"**（后者在 N=1 下不成立）。

**⇒ 决策 D24：`admin runtime --json` 与 `export-incident` 的 schema bump 判定。**
A5 新增的 `last_tick`/`runs`/`last_err` 与 A15 的三个计数器都走 `--json` 载体，受 `docs/usage.md:1554-1556` 的 bump 政策管：**加 omitempty 字段不 bump**。两项都按"加 omitempty 字段"处理，`schema_version` 不动。

## 5. 需主进程拍板的决策（已拍，列此备外审复核）

| # | 问题 | 决定 | 理由 |
|---|---|---|---|
| 1 | A1 要不要追求"枚举全部错误码" | **不追求** | 静态不可判定（≥7 种发射形态，可经任意返回值链传播）。改为三段规格 + 逐形态自检 + **测试显式声明覆盖边界**。一个宣称守住全部实则覆盖 41% 的闸门比没有闸门更危险 |
| 2 | A2 改不改 `transferGate` 签名 | **不改** | 改签名会给 4 条 RPC 新增 SQLite 错误串外泄给非 owner 成员 |
| 3 | A4 删不删 `port.Revoke` | **不删，只删假 godoc** | 它是 `equiv_test.go:422` 的等价性对照臂；且幂等语义与 `RevokeAllocation` 不等价 |
| 4 | A6 用哪个方案 | **只用第一方案** | 第二方案丢 `uc.Audience`，单 broker 下 = 全员连不上 |
| 5 | A15 要不要先读现网 raft config | **不读，改为无条件限速** | 限速让结论不依赖未知状态，比一次现网查询更稳 |
| 6 | 批次 A 要不要裁剪 | **不裁剪** | 真实成本是"增量数 × 7 步"，裁剪不减少增量数，只给 B 批留下带上下文债的尾巴 |
| 7 | 外审否掉个别项怎么办 | **该项先不落，非 revert** | 因此每项自成 commit、不跨项混合 |

## 6. 已知陷阱（实施时逐条对照）

- A1 分类表是**产品决策不是机械工作**，逐条再判，不照抄 S1 的建议表。
- 只把跨包共享的码搬进 `proto`（判据：同时出现在 broker 与 `error_hints.go`）。
- AST 守门测试必须带自检，且**三段规格各配一个合成样例**。
- `deadcode` 重跑必须带全部 8 个 build tag；`go/packages` 须处理 `pkg [pkg.test]` 变体。
- banner 文案（A14）与错误码字符串（A1）一律**字节保持**，不顺手改文案。
- `internal/cluster/fsm.go:80-89` 的 INVARIANT 禁区，A4 不得触碰。
- A3 落地后 `docs/cluster-runbook.md:815-819`（"abandoned lock 约 15 分钟自释放"）**前提改变**，必须进文档 sweep。
- 文档 sweep 范围限**规范性文档**（usage/architecture/cluster/broker-ops/requirements），**排除** `docs/reviews/` 与 gotchas 的历史段落；判据是"只有描述**当前**应当如何重试的句子才算漂移"。
- A5/A9/A10/A12 触碰 goroutine 生命周期 ⇒ `-race` + 仓库内建 NumGoroutine/fd 泄漏门。同时留意：本批次新增的 400–600 行测试可能改变泄漏门的 tolerance 基线。

## 7. 被驳回的建议

| 来源 | 主张 | 驳回理由 |
|---|---|---|
| D6 反对派 | 裁剪到 8 人日（A5/A13/A8-3 拆一半到 B 批等） | 只算编码时间，漏掉"增量数 × 7 步流程"这个成本大头。5 处切分全落在耦合边上，且它没重画依赖图 |
| D6 反对派 | `port.Revoke` 唯一引用是 `port_test.go:150`，是"本批次唯一无保留支持的删除项" | 事实错误——实际 6 处引用含跨包 `equiv_test.go:422` |
| D5 production-ops | A6 用第二方案，工作量 **-0.25 天** | 丢 `uc.Audience`，方向和量级都错（是正工作量） |
| D5 production-ops | A10 提取时加 cancel 会取消正在写对象的 ctx | `deleteXferObject` 传 `context.Background()`，调 cancel 是 no-op |
| D5 production-ops | A12 需跑 drill `93-metrics-observability` | 选错 drill；93 跑的是 `--metrics-listen`，`/sub` 在 72/73/74 |
| D5 production-ops | "S1 的现网后果描述是错的" | 打稻草人——用 malformed seed（A6 不改的那支）反驳 wrong-kind（A6 唯一改的那支） |
| D3 regression-risk | A15 会以 172,800 条/天写满 racknerd | N=1 时零 replication goroutine；且速率算错最多 22 倍 |
| D3 regression-risk | `agent_rejected:` 前缀码恒退 70 | 前缀剥离已实现且有测试钉住（`error_hints.go:158-166` + `error_hints_test.go:80`） |
| D3 regression-risk | `cleanupEntry` 的 `ent` 与 `entry` 可能不同指针 | `transferTracker` 存的就是本体，恒等 |
| D1 sequencing | A1 的规则会误收 `transfer.go:425/427` 的 audit 码，27 码清单须重跑（+0.5 天） | 那两处是对局部变量赋值，Key 不是 Ident `Code`，规则不会命中。真正需豁免的只有 `home_broker_restart` 一条 |
| D1 sequencing | 搬走 `dataplaneNotConvergedCode` 会让 wire-stability 测试退化成自比 | 该测试比的是写死在测试文件里的 `const wire`，搬过去后**更强** |
