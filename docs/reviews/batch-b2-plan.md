# 批次 B 余项（B5–B9）实施 plan

> 范围：`docs/reviews/quality-audit/2026-07-25-structural/S1-refactor-roadmap.md` 的 **B5 / B6 / B7 / B8 / B9**
> —— 即批次 B 的全部剩余工作。B1–B4 已于 `808552d` 落地并通过两轮外审。
>
> **本轮的定稿规则（操作者指令）**：B5–B9 必须在**这一个流程内全部完成**。
> 不允许延后 / 推迟 / 悄悄缩范围。任何判定为"不该做"的条目，其含义是**此后永不做**，
> 必须落在 §7「永不做」表里并附 file:line 依据；「大」「难」「要前置条件」都不是合法依据。
>
> 主进程是本 plan 的唯一定稿人（CLAUDE.md §4）。专家（workflow 内 agent）只提建议、只审查、可新增测试，**不改实现**。

---

## 0. 主进程独立核实的地面真相（本节是本 plan 的尺）

本节全部由主进程亲自打开文件读出，**不是从注释或路线图推断**。与专家草案冲突时以本节为准；
与路线图冲突时以本节为准（路线图是 2026-07-25 的快照，部分数字已过时）。

### 0.1 路线图计数订正

| 路线图原文 | 实测（2026-07-27） |
|---|---|
| B6「141 / 499（28%）测试文件按开发过程事件命名」 | **537** 个 `*_test.go`；按过程命名 **149** |
| B8「4 处手抄 `in.Barrier=` / `in.TopoTargetGen=`」 | **5 处**：`internal/broker/cluster_operation_controller.go:269-272 / 570-573 / 761-764 / 792-795 / 1257-1260` |
| B9「`test/d3,d4,d5,d8,d9/setup_test.go` 共 1,712 行」 | d3=191 / d4=468 / d5=468 / **d6=280（路线图漏列）** / d8=327 / d9=258 = **1992** |
| B6「`internal/tunnel/p13_external_review_round{2,5,6}_test.go`」 | 实际是 **round2 / round4 / round5 / round6** 四个文件 |
| B9「`silentLog`×13」 | 13 个包是 `return testharness.SilentLog()` 的一行 shim；**另有 5 处自带函数体**（`internal/tunnel/tunnel_test.go:31`、`internal/broker/broker_test.go:68` 名为 `silentLogger`、`test/security/harness_test.go:38`、`test/concurrency/helpers_test.go:37`、`test/p3/setup_test.go:178`） |

### 0.2 build tag 的权威清单（B9 的 lane 验证命令依赖它）

`test/e2e/all_phases_test.go` 里 `exec.Command("go","test",…,"-tags",X)` 的 X 全集：
`d5_integration` `d6_integration` `d7_integration` `d8_integration` `d9_integration` `phasefluidity_integration`，
加矩阵入口自己的 `e2e_matrix`（`Makefile:69`）。另有 `test/concurrency/` 的 `//go:build linux`。

**`test/d3/` 与 `test/d4/` 没有任何 build tag** —— 它们进 `make test`；矩阵里的
`TestD3Matrix`（`all_phases_test.go:228`）/ `TestD4Matrix`（`:248`）是**不带 tag** 的 `-race` 子进程，只换了包 glob。
⇒ B9「一个 lane 一个 commit，各自 `go test -tags dN_integration -race ./test/dN/`」**对 d3/d4 不适用**，
它们是 `go test -race ./test/d3/...`。plan 的每条 lane 必须写对，否则 lane 验证是空跑。

全 tag 编译核验命令（本轮采用，不新增 Makefile 门）：

```
go vet -tags 'd5_integration,d6_integration,d7_integration,d8_integration,d9_integration,phasefluidity_integration,e2e_matrix' ./...
```

> 上一批已被这个咬过一次：`test/d8/integration_test.go` 在 `d8_integration` 后面，
> 改签名后 `go build` / `go vet` 全绿而 `make e2e-parallel` 编译失败。

### 0.3 B5：`cluster_grow_cutover.go:233-235` 的 "byte-identical" 是**错的注释**，不是"碰巧收敛"

路线图说"今天碰巧收敛，但没有任何测试钉住"。实测更明确：`cluster_grow_cutover.go:263-266` 有一段 `m2` 注释
**明确记录 MonitorListen 是故意强制的**（`restartAndVerifyClustered` 只探 `127.0.0.1:8223`，
harvest 一个可能缺失的 http 块会把健康的复活误报成 `cutover_revival_failed`，45s connection-refused）。

所以分歧属**设计**（§7.3「注释解释了为什么不能改」那一类），而 233-235 那句总结注释本身是假的。
⇒ B5 要做的不是"让两者变 byte-identical"，而是 **(1)** 订正 233-235 写出真实契约；
**(2)** 用测试钉住这个**带例外**的契约。

#### 0.3.1 两条渲染路径的完整逐字段分歧（`cluster_grow_cutover.go:256-272` vs `natsreconcile/reconcile.go:117-143`）

| 字段 | cutover | reconciler | 判定 |
|---|---|---|---|
| `Standalone` | 硬编码 `false` | 算出（`reconcile.go:116`） | cutover 要求 `len(Peers)>=2`（`:241`），reconciler 的 standalone 要求 `len(Peers)==1` ⇒ **该路径上等价** |
| `Local` / `Peers` / `AccountIssuer` / `JSStoreDir` / `ClientListen` | 同源 `buildTopologyInputs` + `own` | 同 | **等价** |
| `Account` | 不设 | `in.Account` —— **死字段**（见 0.3.2） | 今天等价，**靠巧合** |
| `JSDomain` | 不设 | `in.JSDomain` —— **死字段** | 今天等价，**靠巧合** |
| `MonitorListen` | 强制 `topoMonitorListen` | 不设 ⇒ `BuildMergedConf` harvest | **故意分歧**（m2, `:263-266`） |
| `CAFile`/`CertFile`/`KeyFile`/`ClusterListen` | 总是从 `SecretsDir` 设 | 仅当 `clusteredOverStandalone && SecretsDir!=""`（`reconcile.go:136-143`） | cutover 路径上 live conf 必是 standalone JS ⇒ **该路径上等价** |
| `ClusterName` | 不设 | 不设（`:141-142` 注释说明依赖 Render 默认 `"tether"`） | **等价** |

`natsconf.BuildMergedConf`（`takeover.go:60-109`）的 harvest 规则逐字确认：
`:67` **仅当** `!Standalone && CAFile=="" && CertFile=="" && KeyFile==""` 才 harvest mTLS，
并**仅在调用方留空时**补 `ClusterListen`（`:73-75`）/ `ClusterName`（`:76-78`）；
`:83-85` **仅当** `MonitorListen==""` 才 `own.MonitorHTTP()`（注释：绝不热加 http，SIGHUP 会被拒）。
代入两条路径 ⇒ cutover 两个 harvest 全跳过；reconciler 只跳过 mTLS harvest、走 monitor harvest。
**该路径上唯一分歧确实就是 MonitorListen。**

#### 0.3.2 `natsreconcile.Inputs.Account` 与 `Inputs.JSDomain` 是**死输入字段**（有读者、生产无写者）

- 读者：`reconcile.go:122`（`Account: in.Account`）、`:123`（`JSDomain: in.JSDomain`）。
- 生产唯一写者本应是 `internal/broker/topology_reconcile.go:212` `buildTopologyInputs` ——
  它只设 `SelfServerName / Peers / AccountIssuer / ConfPath / NatsServerBin / DesiredGen / SecretsDir`（`:231-243`），
  **两个都不设**。
- `natscluster/config.go:106-107` 只在 `JSDomain != ""` 时 emit `domain: %q`；`Account` 空则默认 `$G`（`:83-86`）。
  所以今天两者都不影响输出。

**这与 `internal/broker/loopset.go:53` 记录过的那次「RuntimeReport 有读者、全仓无写者」是同一缺陷类**，
也是批次 A（A4 死符号）清扫的同一类，只不过这次落在**部署面渲染器**上。

**它同时指明了 B5 的真实风险载体**：路线图担心"任何人加一个字段都会静默打破 byte-identity"——
指名道姓就是这两个字段。谁把 `JSDomain` 接进 `buildTopologyInputs`
（layer-2 `distributed-broker-architecture.md:31/:162`「一个 JS domain」字面上会诱使人这么做），
reconciler 渲染带 `domain:` 而 cutover 不带 ⇒ **刚重启完的 broker 上一次计划外 swap + SIGHUP**。

> 注意不要混淆：`scripts/install.sh:540` 的 `domain: $DOMAIN` 是 **broker.yaml 的 `broker.domain`（公网主机名）**，
> 与 JetStream domain 无关（`cmd/tether/serve.go:79,372` 佐证）。

### 0.4 B7：路线图给的"必须改执行模型"理由，指向的是它自己禁止做的那一半

路线图 B7（line 490-491）举了三个阻塞证据。逐个核实它们在哪：

| 阻塞源 | 实际位置 | 在 B7 要移动的清单里？ |
|---|---|---|
| `observePollWindow = 2s`（`observability.go:219`） | `observeOnce`，由 `runObserveLoop` 调（`:290`） | **不在** |
| `waitNatsLoaded`（`topology_reconcile.go:164`） | `runTopologyReconcileLoop` | **不在** |
| `pub.tick` 阻塞 JS 发布 | `audit-publisher` 循环（`clusterwrite.go:441` `pub.Run`） | **不在** |

五条集群循环是 `clusterwrite.go:441-448`（`audit-publisher` / `reconciler` / `observe` / `topology-reconcile` /
`alert-webhook`）。**三个阻塞源全在这五条里**，而把这五条并进 registry 正是路线图 line 494 自己的**明确不做**。
⇒ **own-goroutine-with-timeout 执行模式只有那个被禁止的合并才需要。**

而 `reconcile_registry.go:68-71` 明文禁止 registry 起 goroutine：

> The registry starts **NO goroutines** and owns **NO timers**. … which is what lets the equivalence proof run in
> microseconds of fake time and what keeps it **invisible to the repo's NumGoroutine/fd leak gate**.

加 own-goroutine 会同时打破：**(a)** 假时钟等价性证明（`reconcile_registry_test.go` 十余处
`r.runDue(ctx, clk.advance(...))` 全部断言「runDue 返回后副作用已发生」，pass 跑进自己的 goroutine ⇒ 全变竞态）；
**(b)** 仓库内建 NumGoroutine/fd 泄漏门（CLAUDE.md §5 明确"刻意不用 goleak"）；**(c)** §7.3 判据。

**替代设计（本 plan 采用）**：给 `reconcilePass` 加 `budget time.Duration`，
`runDue` 用 `context.WithDeadline(ctx, now.Add(budget))`（deadline 由**注入时钟的 tick 瞬时**推出，不读 `time.Now()`），
pass 返回后用注入时钟量 elapsed、超预算记入 `reconcilePassStatus` + 一条 Warn。
⇒ ctx-aware 的阻塞真能被打断；不 ctx-aware 的至少**超时可观测**；零 goroutine；假时钟等价性保住。

#### 0.4.1 cadence 保持可精确证明，但有三处不能动

- `observeTickInterval = 5s`（`observability.go:221`）；registry granularity = `min(interval)` = `ReconcileInterval` = **1s**
  （`reconcile_passes.go:106` 注释确认）。5s 是 1s 的整数倍 ⇒ 按 `reconcile_registry.go:58-66` 的**锚定**语义，
  移进 registry 并注册 `interval=5s` **触发瞬时与原 ticker 逐拍相同**。这是精确论证，不是"大概等价"。
- **`b.cl.fsArm.observeLeadership`（`observability.go:247-250`）不动**：注释明确
  "a quorum-lost survivor is never leader, so this is the **only** loop that can observe the sustained loss
  the force-single gate requires"。它是现网唯一逃生闸（racknerd 靠 force-single 活着）的前置观测。
- **`ReconcileMembershipOnLeadership`（`:264-268`）不动**：它只在 leader-**ACQUIRED 边沿**跑一次，
  注释说 "nothing ELSE in production calls it"。registry 是 **level-triggered**（`:63-66` 明确
  "replaying missed edges is both pointless and, for anything that writes, actively harmful"）——
  边沿触发的东西不能变成 level-triggered pass。
- `reconcile_registry.go:23` 的 `THE TUPLE IS (…) — NOT NEGOTIABLE` 与 `:145` 的
  `isLeader == nil ⇒ 单机 ⇒ 永远是 leader`：改元组必须整段搬运并订正该注释；
  单机语义必须保住（现网全是单机模式，只保集群语义 = **全车队回归**）。
- `driveLeaderMaintenance` 自述（`cluster_operation_controller.go:365-366`）
  "adds **NO goroutine** (it runs **inline** on the existing observe ticker)" —— 它本来就是 inline 的，
  移进 registry 不改变这一点，进一步说明该做的那一半不需要新执行模型。

### 0.5 B8：fail-OPEN 已核实；三指针的真实价值是「忘了 = 不写」而非「忘了 = 清零」

- `cluster_operation_controller.go:1079-1102` `topoConvergedForOp` 逐字确认：
  `:1080-1082` `if op.TopoTargetGen == 0 { return true, "" }` ⇒ **fail-OPEN**。
  其余三支全 fail-CLOSED（`:1084` status 不可用、`:1094` 未上报/不可达、`:1097` gen 落后）。
  **唯一的 fail-open 洞就是 0 这一支。**
- `jsGateExpiryReserve`（`:1104-1120`，`= 30 * time.Second`）注释原文
  "(internal review G-1, **a BLOCKER I shipped and did not see**)" —— 复用已咬过一次，属实。
- **今天的机制**：`SetBarrier bool` 一个门管三列（`operation_ops.go:149-152` 无条件拼三列 SQL）。
  设了 `SetBarrier=true` 却忘抄 `in.TopoTargetGen` ⇒ 用 **0 覆盖库里已有值** ⇒ 下次 `topoConvergedForOp` 见 0 ⇒ 宣布收敛。
  改三指针后忘传 = **该列不进 SQL = 保留原值**。缺陷类别从「静默毁数据 + 绕过拓扑门」降为「不更新」。
- 结构体自己的 doc（`operation_ops.go:112-114`）写着
  「Barrier/CatchupDeadline/TopoTargetGen … **0 = leave unchanged is NOT expressible**」——
  **这是作者写下的限制**，三指针正是把不可表达变成可表达。B8 半 A 不是口味问题，是填一个文档承认的缺口。

#### 0.5.1 wire 判定：成立，但正确论证是"wire 上跑的是 SQL 文本"

`OpTransitionInput` 全部出现处：`operation_ops.go:115/131`、`cluster_operation_controller.go:268/504/506/569/760/791/1256`、
`operation_ops_test.go`。**无任何序列化点。** 真正上 raft wire 的是
`NewCommand(OpClusterOpTransition, Stmt(sql))` —— **一段字面 SQL 文本**。
所以三指针改的是**生成出来的 SET 子句包含哪几列**；少几列仍是合法 SQL，任何版本 follower 应用后语义相同
⇒ 不需要 `ProtoVersion` 跨版本路径、不需要重装、混版滚动升级安全。
**「结构体没上 wire」这个理由不够——`cluster.apply.<verb>` 是跨版本 broker↔broker 面，必须论到 SQL 文本这一层。**

### 0.6 B9：泄漏门缺陷错在**样本量**不在容差；harness 不是孪生

- `test/concurrency/helpers_test.go:136-153`：`if last-before <= 2 { return }`。
  `:124-128` 论证 ±2 是经验噪声地板（GC / scavenger / dial retry timer）——**容差本身站得住**。
- `test/concurrency/goroutine_leak_test.go:168-199` `TestTunnelServerCloseWithActiveSessionNoLeak`
  只开 **1 个** session（`:188`），而其 doc（`:161-164`）自称覆盖
  "handleAgent + publicAcceptLoop + **the yamux watcher goroutine** + streamAcceptLoop" —— 全是 per-session 形状。
  1 session × per-session 漏 1~2 ⇒ `<= 2` ⇒ **结构上永远绿**。兄弟测试 `TestBrokerRepeatedRunNoGoroutineLeak`（`:242`）跑 5 轮，做对了。
  ⇒ **修法是把样本量提到 N≥5，不是收紧容差**（收紧会引入 ±2 注释论证过的假阳性）。
- **d4/d5 harness 不是孪生**：都恰好 468 行，但规范化包名后仍有 578 行 diff，
  `comm -12` 共同行只 **251 行**（≈54%）。d4 = §13.7 写转发；d5 = 审计发布 + JS 副本 reconfig（三层，且 NATS 不开 auth_callout）。
  ⇒ B9 能收的是那 251 行共同核心，**不是 1992 行整体**。plan 必须给**诚实的行数目标**，
  否则收尾时"没达到 -800 行"会被误判为没做完。
- `internal/testharness` 属 §7.1 点名「几乎永远该保留」的叶子包（roadmap:583：被 15 个测试包引用，
  替代方案是 15 份拷贝）⇒ B9 的方向（**扩**它，加 `testharness/cluster`）与 §7.1 一致。

### 0.7 B6：改名风险的实测结论——大多数门"跳过测试文件"，真正扫测试文件的只有两个

- **安全**（walker 显式 `strings.HasSuffix(p,"_test.go") { return nil }`，只扫生产文件）：
  `cmd/tether/error_code_coverage_test.go:502`、`internal/auth/acl_reconcile_test.go:143/166/554`、
  `internal/httplisten/policy_test.go:46/150`、`internal/proto/rehome_directive_test.go:43`、
  `internal/proto/codes_registry_test.go:88`、`internal/clusteroffline/force_single_callsite_round6_test.go:227`、
  `test/determinism/lint_skeleton_test.go:40/334`、`test/determinism/cfgdb_ratchet_test.go:213`。
- **会扫测试文件的只有 2 个**：
  1. `test/determinism/raft_timing_guard_test.go:80` + `:148`，有一个 `exempt` map 按 **"测试文件相对路径:行号"** 作 key。
     **实测只有 2 条 key，都在 `internal/cluster/transport_test.go:509/510`**，该文件不是过程命名文件
     ⇒ **B6 不会碰它 ⇒ 这条风险实际为零**。它另有非空性地板（`countProductionConstantUses < 10` → `t.Fatalf`，
     注释点名 `docs/testing-standards.md` G2），即使被打破也是**变红不是变绿**。
  2. `test/determinism/lint_skeleton_test.go` 的 `notTest`（`:40`）是用来**排除**测试文件的，同样安全。
- **需要手工跟的自引用**：`cmd/tether/error_code_coverage_test.go:817`
  `os.ReadFile(… "error_code_coverage_test.go")` **读自己的文件名**；
  `internal/broker/admit_test.go:394` 的错误串里手抄了该文件名（属 §A4 那一类）。两者都不在 B6 改名集内，但需登记。
- ⇒ **B6 的真实风险不在门禁**，而在 build tag 头、包内共享的未导出 helper、`// origin:` 溯源、
  以及**测试函数名集合 diff 必须为空**（只改文件名不改函数名 ⇒ 该 diff 天然为空，
  唯一例外是 tunnel kill-fence 那处**有意合并**）。

---

## 0.8 本会话的工具链事实（所有门禁命令必须带前缀）

`which go` → **rc=1**：`go` 不在本会话 PATH 上，工具链在 `/usr/local/go/bin/go`（go1.25.0）。
而 `Makefile:18/21/69/117/132` 调的是**裸 `go`** ⇒ 直接 `make test` 会失败。
`$(go env GOPATH)/bin/golangci-lint` 存在且是 **v2.5.0**（与 `Makefile:10` 的钉版一致）。

```
cd /home/weiland/dist_experiment_control && PATH=/usr/local/go/bin:$PATH make test
cd /home/weiland/dist_experiment_control && PATH=/usr/local/go/bin:$PATH make lint
cd /home/weiland/dist_experiment_control && PATH=/usr/local/go/bin:$PATH make e2e-parallel
```

---

## 1. 执行顺序

顺序由三件事强制，不是偏好：

1. **B6 的改名必须最后**——它的验收判据是「`(目录, 测试函数名)` 恒等多重集 diff 为空」，
   而 B5/B7/B8/B9 都会**新增测试函数**。改名在前 = 新测试生在旧名下再被改一次（白做）；
   改名在后 = 一次覆盖全部。
2. **B6 的「冻结门」必须最先**（与改名分离）——先落"新文件不许用过程命名"，
   后面四项新增的测试文件才会天生合规。S1 与 S3 的分歧（`S3:171`/`S3:691` 认为改名是可选清理、
   不该做成闸门前置）正是用这个次序化解的：门不被改名阻塞，改名反过来自动收紧门
   （`S3:475` 的反向断言：冻结表里已不存在的名字必须删掉）。
3. **B8 必须在 B7 之前**——两者都改 `internal/broker/cluster_operation_controller.go`（1292 行）。
   B8 会**删掉约 10 行**（每处 3 行回写 → 1 个指针）从而全文件后移行号；
   B7 的编辑区窄（`:357-378` 附近），后做重定位便宜。**不得交错。**

```
 0.  前置特征化测试（在未改动树上必须绿；B8 的那条今天必红，作为缺陷凭证）
 1.  B6-a  只做「冻结门」（test/determinism/test_naming_test.go）+ CLAUDE.md §3 step 5b
 2.  B8    半 A 三指针 → 半 B outbox 提纯 → 行级普查守卫（单独 commit）
 3.  B7    先限界（穿 ctx + 堵无界点）→ 再搬进 registry inline → authority → GoEvery
 4.  B5    natsconf 合并 + RenderDesired + BUG-1/BUG-2 + 注释订正
 5.  B9    lane 0..N，一 lane 一 commit
 6.  B6-b  158 个文件改名（154 git mv + 4 文件合成一张 fence 表）  ← 数字订正见 §2.2
 7.  三门全绿 + B5 的四个 simcluster drill（带 --build 且证明跑的是新二进制）
```

`BEFORE-testfuncs.txt`（**2304** 个测试函数 @ `808552d`）用于**全批次总账**；
B6-b 自己的 diff 用**它开工那一刻**重取的快照。

---

## 2. 逐项 plan

### 2.1 B5 — `natsconf` 收口（R=3，唯一碰部署面的一项）

**目标形状**：三包（`natsconf` 1775 行 + `natsreconcile` 538 + `natscluster` 534）合并为 `natsconf`，
`natscluster.Config` 的**唯一**装配点变成一个导出函数：

```go
// package natsconf
type RenderIntent int
const (
    IntentPreserve RenderIntent = iota // reconciler：保留 live conf 的模式，绝不跨 cluster↔standalone 边界
    IntentForceStandalone              // --to-standalone / offline force-single
    IntentForceClustered               // grow cutover
    IntentStandaloneIfLone             // 手工 takeover：len(peers)==1 ⇒ standalone
)
type RenderOverride struct {
    Intent        RenderIntent
    MonitorListen string // 非空 = 强制 monitor 而不是从 live conf harvest
    SecretsDir    string // 非空 = 从 secrets 目录派生 routes mTLS（跳过 harvest）
    ClusterListen string
}
func RenderDesired(in Inputs, own *Ownership, ov RenderOverride) (string, error)
```

**为什么是 4 个 Intent 而不是 3 个**（采纳 critique B5-C2）：
`cmd/tether/cluster_natsconf.go:359-363` 的 takeover 渲染 `Standalone: len(peers) == 1`，
按 `:360-362` 的 audit-D 注释这是**刻意**的第四种意图。
用 `IntentPreserve` 时，lone-peer takeover 打在**已 clustered** 的 conf 上会渲染成"clustered 但零 routes"，
撞 `Render` 的 fail-closed（`config.go:144-147`）⇒ **今天是脱簇、改完变硬拒**。
四个 drill 都盖不到这个形状（drill 43 用 `--manual` **带** `--peer`，`len>1`）。
算术订正为：**5 个装配点 → 4 种意图**（#3 与 #5 合并）。

**顺带修的三个真缺陷**：
- **BUG-1**：JS `store_dir` 的 fail-closed 守卫只在 5 个装配点中的 3 个
  （有：`cluster_natsconf.go:382-385`、`:139-142`、`cluster_offline.go:80-82`；
  **无：`natsreconcile/reconcile.go:124`、`cluster_grow_cutover.go:261`——恰好是两条自动路径**）。
  conf 带 `jetstream {}` 而无 `store_dir` 时，5 秒自动循环会渲染出**完全没有 JetStream** 的 conf
  并 swap（`nats-server -t` 语法上通过）。守卫**移进 `RenderDesired`**，五个调用方一次全得到。
  可达性：需手工编辑过的 conf（`install.sh:714-716` 总写 `store_dir`）⇒ 潜伏非现行。
- **BUG-2**：`Render` 能 emit 它自己的 `Preflight` **拒绝**的指令——
  `Render` 在 `JSDomain != ""` 时 emit `jetstream{ domain: … }`（`config.go:106-108`），
  而 `jetstreamSafeSubkeys = {"store_dir"}`（`preflight.go:66`，`:154` 强制）。
  `Inputs.JSDomain`（`reconcile.go:44`→`:123`）与 `Inputs.Account`（`:43`→`:122`）
  **有读者、生产无写者**（`buildTopologyInputs`（`topology_reconcile.go:230-241`）只设 7 个字段）。
  谁接上它，reconciler 就 swap 一个**它下一 tick 自己解析不了**的 conf ⇒ 永久 `ActionUnknownDirective`，
  **且是在 swap 之后才 fail-closed**。⇒ **删这三个字段**，并把
  `internal/natscluster/g6_render_test.go:12` `TestRenderNeverEmitsMaxFileStore` **泛化成整个 allow-list**。
- **BUG-3**：`cluster_grow_cutover.go:233-235` 那句 "byte-identical" 是**错的注释**——
  `:263-266` 的 `m2` 明确记录 MonitorListen 是**故意**强制的。订正措辞为真实契约
  （"除 monitor 外一致；cutover 强制它因为它必须探测它"），**不靠改策略来"修"**。

**测试（行为的）**：
- **T0（现网对照，最重要）**：golden 覆盖 **`BuildMergedConf` 的完整 merged 文本**，
  放 `internal/natsconf/`（采纳 critique B5-C3：真正落盘的字节是
  `Render` **加** mTLS/listen/name harvest（`takeover.go:67-79`）+ monitor harvest（`:83-85`）
  + 重发的 `websocket{}`（`:98-101`）+ 排序 passthrough（`:104-107`），
  而 B5 动的**恰恰是那层兜底逻辑** ⇒ `Render` 级 golden 结构上看不见它）。
  输入 = 真实落盘 conf fixture（`installSHConf`、`g4StandaloneConf`、一个 clustered conf、
  **一个 racknerd 形状的手工脱簇 conf**）× 每种生产意图，逐字节断言 `testdata/*.conf`。
  **必须在改动前写好并全绿。**
- **T1**：`TestCutoverRenderEqualsReconcilerRenderModuloMonitor`，两个 case——
  (a) live conf 带 `http: 127.0.0.1:8223` ⇒ 两条渲染**逐字节相同**；
  (b) live conf **不带** `http:` ⇒ 仅 monitor 块不同。
  变异 = 把 cutover 侧的 `RenderOverride{MonitorListen: topoMonitorListen}` 改成 `RenderOverride{}` ⇒ 必红。
- **T4**：Intent #4 的表驱动——(i) standalone conf + 1 peer、(ii) **clustered** conf + 1 peer、
  (iii) clustered conf + 3 peers，钉住结果模式。
- **T5**：泛化的 render allow-list 守卫（BUG-2）。
- **T6**：offline force-single 打在**已 standalone** 的 conf 上执行**零次写**（无 `.bak.*`、mtime 不变）。
- **结构性守卫（与行为测试并列，不替代——§S1）**：AST 断言
  `natscluster.Config{...}` 复合字面量在生产代码里**只出现在 `RenderDesired` 内部一处**，
  带**非空性地板**（扫到的字面量数 ≥1，否则改名会让门变绿失明）。

**step 6 从 MECHANICAL 改判 JUDGEMENT**（采纳 critique B5-C4，这是潜在**丢数据**的一条）：
`cluster_offline.go:76-78` 的 `buildStandaloneConf` 在 `!own.IsClusteredJetStream()` 时
**渲染之前**就 `already=true` 返回；`deClusterStandaloneConf:122-124` 随即短路，注释原文
*"no-op — already standalone: no render, no swap, **NO destructive JS-store reset**"*。
丢掉或重排 ⇒ offline `force-single` 打在**已手工脱簇的存活节点**（**= racknerd 的现网形状**）
会重新渲染、重新 swap、走进 **JS-store reset**。drill 20 从 **clustered** 存活节点起步 ⇒
这条臂在部署层**也没覆盖**。必须逐字保留并在 diff 里点名的守卫清单：
- #5 `cluster_offline.go:76-78` + `:122-124`
- #3 `cluster_natsconf.go:105-131`（已 standalone + `--reset-js` 前向补全）、`:132-133`（`--confirm-single`）、
  `:139-142`（F3）、`:149-177`（leader 视图 / 非 partial / role / voters==1）
- #4 `:315-328`、`:348-357`（`--allow-partial-mesh`）、`:397-408`（`--plan` 不改任何东西——drill 43 的假绿守卫依赖它）

**Blast radius**：不动 `ProtoVersion`、不需重装、不需 agent 升级
（`grep -rn 'natsconf|natscluster|natsreconcile' internal/proto/` 为空；
`internal/agent/`、`internal/node/` 对这三个包零引用；wire 上唯一 conf 形状的字段是
`ClusterStatusReport.NatsConfPath`——一个**路径**，不动）。
最坏情况是 reconciler 变 `ActionRejected` + `cluster status` STUCK，**旧 conf 继续服务**，
逃生闸是已文档化的 `tether cluster reconcile nats --manual`。

### 2.2 B6 — 测试文件按主题归位

**范围**：**158** 个过程命名文件（正则随 plan 一并公布，因为路线图的 141/499 已过时）。
其中 **154** 个走"改到**未被占用**的名字"的字面 `git mv`，**4** 个 tunnel round 文件走**唯一的有意合并**。

> **数字订正（内审 · mutation-honesty audit §3.6）**：本节起草时写的是 165，那是某个起草 agent 的计数，
> **没有公布正则、无法复现**。落地后以 `test/determinism/test_naming_test.go` 的 `processNamedPattern`
> 实测为准 = **158**。审计里前后出现过 141 / 134 / 123 / 155 / 158 / 165 六个数字，只有 158 附带了
> 可复现的正则；其余五个一律作废。完整对账见 `test/determinism/legacy_process_named_list.go`
> §「ON THE NUMBER 158」。**本 plan 内其余提到 165 的地方同此订正。**

**"纯 `git mv`、零风险"是假的**：L07 例表（`L07:103-113`）提的 6 个目标名里 **5 个已存在**，
其中 `internal/clusteroffline/r10_doctor_db_test.go`（`package clusteroffline_test`）
→ `doctor_test.go`（`package clusteroffline`）**跨包子句、合并不编译**
（该目录确实同时有 14 个 `package clusteroffline` + 7 个 `package clusteroffline_test`）。
⇒ **决策：绝不合并进已存在文件**，改到 `doctor_db_test.go` 这类自由名。
于是 161 个是字面 `git mv`：包子句、import 块、文件内行号全不动，编译风险归零。

**目录名一律不动**：`test/e2e/all_phases_test.go` 用**字符串字面量**传包路径
（`:144/:194/:213/:233/:253/:278/:316/:338/:362/:386/:415/:420/:456/:473`），
`test/e2e/parallel/split.go:60` 硬编码并 AST 解析 `all_phases_test.go`，
`shard.go:76` 硬编码 `test/d4`/`test/d5`。改目录会打破**唯一的全矩阵闸**。

**`test/e2e/parallel/external_review_test.go` 改名**（采纳 critique BL3——原计划自相矛盾地
既把它算进那 158 又宣布它不可动）：它是 `package main`，`split.go:60` 解析的是
`all_phases_test.go` 而非自己包里的测试文件，全仓对这个 basename 零引用 ⇒ 改名安全。

**唯一的有意合并**：`internal/tunnel/p13_external_review_round{2,4,5,6}_test.go` → `kill_fence_test.go`
一张 `{verb, killFn, dims}` 表，覆盖 **4 个** verb：
`CloseProxy:518` / `CloseProxyIf:542` / `ForgetSession:609` / `CloseSession:712`。
`Close()` **不算**——`tunnel.go:670-671` 明说 `s.closed` 是服务生命周期、不是 fence 维度。
round4 是**不同的不变量**（kill 且不读 DB）且**定义了两个包级 helper**
（`waitSessionsInstalled:77`、`waitListening:98`，包内别处没有）⇒ 合表时不能当成一行删掉。

**冻结门**（放 `test/determinism/`，已在矩阵里 `all_phases_test.go:213`；
不放 S3 提的 `test/architecture/`，那对 `make e2e-parallel` 不可见除非改矩阵契约）：
- 断言：新增/现存文件名不匹配过程命名形态；
- **非空性必须跑在合成的内存字符串上**（采纳 critique BL4，并见下方 §4 的通用原则）：
  每种禁止形态一个正样本（`p13_external_review_round6`、`g4_external_review_fixes`、`b6_skew`、
  `g1g7_audit`、`megaaudit`、`ops11`、`codex_allgreen`、`d2_command_shape_review`）
  + ≥2 个必须**不**匹配的反例（`proxy_generation_fencing`、`home_delivery`）。
- 允许的残留集**与 NEVER-DO 清单字面相等**，**不接受散文豁免**（采纳 critique BL2）。

**`TestFenceDimensionsAreAllTabled` 必须断言映射而非基数**（采纳 critique BL1）：
原设计 `reflect.TypeOf(fenceSnap{}).NumField() != len(killVerbs())` 是 3≠4 当场就红，
而它自己的变异（给 `fenceSnap` 加第 4 个字段）会让它 4==4 **变绿**——**在它要抓的场景下变绿**。
改成：每行带 `dims []string`，断言 `union(dims) == reflect 的字段名集合`，
并对每行在 `s.mu` 下 snapshot `fenceSnapLocked` 前后、要求**恰好**声明的维度动了。

**合表的变异证明要收窄**（采纳 critique BL5）：删 `tunnel.go:549`（`killGenAllocation[key]++`）
今天就会让 `TestD9CloseProxyIfFencesPortReuse`（`d6_test.go:115,139`）变红 ⇒
"改动前绿"是**假的**。可主张的命题是"**不存在** `CloseProxyIf` 的**黑盒 in-flight-REGISTER** 覆盖"
（全仓对它的测试调用只有 `d6_test.go:106/121/133/142`，**全白盒**）。
证明范围收窄到新行：变异下 `-run '…/CloseProxyIf'` 必须 RED、`-run '…/CloseProxy'` 保持 GREEN，
并写明既有白盒测试同时变红是**预期**。

**诚实行数**：**≈ −190**（仅来自合表，`L07:188`）+ 改名 ≈ 0（`L07:139` 自己说改名 0 净减行）。
**不承诺 −500。**

**CLAUDE.md §3 step 5b**：新测试按被测单元命名；**不允许新建 `*_external_review_*_test.go`**；
新增测试与被测单元同名文件；违反由冻结门在 `make test` 拦下。

**验收**：`(目录, 测试函数名)` 恒等多重集 diff 为空（除有意合并）+ **九个矩阵 `-run` 过滤集逐字节相同**。

### 2.3 B7 — registry 执行边界 + 收敛调度合一

**永不做 `own-goroutine-with-timeout`**（见 §7），**改做下面这条**——顺序即依赖序：

1. **先限界，后搬迁**（critique 逼出来的正确次序，路线图没给）：
   两个要限界的职责**都不收 ctx**（`proxy_reconcile.go:28` 与 `cluster_operation_controller.go:368` 皆零参），
   所以任何 budget 若不穿 ctx 就是**装饰品**。
   - `driveProxyReconcile(ctx)` → `activeProxySessions(ctx)` / `reconcileProxyTeardown(ctx, nc)` 用 `QueryContext`；
   - `driveLeaderMaintenance(ctx)` → `driveInFlightOperations(ctx)` → `streamsReady` / `caughtUpFn`；
   - 限界 `clusterwrite.go:477` 的 `ObserveReplicas(context.Background())`
     （它现有的 `return false, err` 已 fail-closed ⇒ 加界严格安全）。
     **动手前先 `grep -n "context.Background()" internal/broker/clusterwrite.go` 列全该路径上每一个无界点**
     ——只堵一个等于没堵。
   - **删掉编造的 fail-closed 链**：`topoConvergedForOp`（`:1079-1102`）**从不读副本态**，
     它只在 `StatusReport` 本身报错时 fail-closed（`:1084`）；而 `StatusReport` 在 `streamObserve`
     出错时把 `streamActual = 0`（`clusterstatus.go:238-241`）并返回**非 nil 报告 + 无错误**。
     据 `streamActual` 的真实消费者（`clusterstatus.go:241`、`computeHealth`、`actual/target` 渲染）重新论证，
     并回答"超时的观测能否与真实的 0 缺口区分"——**今天不能**，而按 §S1（missing == unsafe）
     `0` 会被读成**已测得的**缺口。这条极性要修。
2. **再搬进 registry，作为 inline pass、`interval = 5s`**：
   `observeTickInterval = 5s`（`observability.go:221`），registry granularity = `min(interval)` =
   `ReconcileInterval` = **1s**；5s 是 1s 的整数倍 ⇒ 按 `reconcile_registry.go:58-66` 的**锚定**语义
   **触发瞬时与原 ticker 逐拍相同**（精确论证，非"大概等价"）。因为第 1 步已限界，inline 串行不再有
   "慢 pass 冻住 `runMu`"的风险。
3. **`ReconcileMembershipOnLeadership` 放进同一个 pass**：pass 函数体自持 `wasLeader`、内部识别
   leader-ACQUIRED 边沿 ⇒ **严格顺序仍在一个 goroutine 上**，且边沿语义不被 level-triggered 的 registry 破坏
   （`:63-66` 明说 "replaying missed edges is … actively harmful"）。
4. **`fsArm.observeLeadership`（`observability.go:247-250`）不动**：必须每 tick 跑且**不能** leader-gated
   （注释：*"a quorum-lost survivor is never leader, so this is the **only** loop that can observe the
   sustained loss the force-single gate requires"*）——现网 racknerd 靠它活着。
5. **`leaderOnly bool` → `authority func() bool`，但每 sweep 只评估一次**：
   今天 `runDue:239-242` 整个 sweep 只调一次 `isLeader()`，`:145-147` 的注释说明理由是
   "every leaderOnly pass in one tick sees a **consistent view**"。改成 per-pass 逐个调用会
   **打破这条一致性**（sweep 中途 leadership 翻转 ⇒ 同一 tick 内前后半 pass 视图分裂，
   而 `driveInFlightOperations` 是 membership op 推进器 ⇒ 正是 §8.1 no-silent-fork 要防的）。
   ⇒ sweep 开始时把所有**不同的** authority 函数各评估一次并缓存。
   保住 `isLeader == nil ⇒ 单机 ⇒ 永远 true`（`:239`）——现网全单机，只保集群语义 = **全车队回归**。
   测试：计数 authority + 多个 leaderOnly pass ⇒ 断言调用次数 **== 1**；变异 = 改成逐 pass 调用 ⇒ 必红。
6. **per-iteration 活性 `loopSet.GoEvery`**：`loopStat` 加 `Cadence/LastIter/Iters`，
   四个 ticker 循环从 `Go` 改 `GoEvery`（`clusterwrite.go:441-449`）。
   **不能复活 `Runs` 计数器**——批次 A 的 review F-03 删它的理由是它对"起来了但第一行就 return"
   与"活着"给出**相同**读数。（路线图第 492 行把这条归功于 A5 是**错的**：A5 做的是骨架 + 接线，
   每迭代活性字段被 F-03 删了。覆盖账本记 **NO（路线图记错）→ 本轮补做**。）
7. **订正两处承诺该合并的已发布注释**：`loopset.go:58-60`、`adminsock/protocol.go:393-394`——它们是错的。
8. **保留 `register` 的两个 panic**（`:171-180`：重名 / 非正 interval 是编程错误，
   *"a silently dropped reconciliation pass is precisely the failure mode R7 exists to eliminate"*），
   并把 `:23` 的 `THE TUPLE IS (…) — NOT NEGOTIABLE` 整段搬运 + 订正为新元组。

### 2.4 B8 — `OpTransitionInput` 三指针 + outbox 提纯

**半 A**：`SetBarrier bool` 一个门管三列（`operation_ops.go:149-152` 无条件拼三列 SQL）
→ **三个指针的三态语义**：

| 传法 | 语义 |
|---|---|
| `nil` | 该列**不进** SET 子句（保留原值） |
| `&v`（v≠0） | 写 v |
| `&zero` | **显式清零** |

**路线图 `S1:506`「`ConfirmOp` 的手动清零变成传 nil」是错的**：nil 语义是"保留"，
照做会**保留过期的 rehome deadline、让 `cluster ops confirm` 退化成 no-op**，
重新打开 `cluster_operation_controller.go:262-273` 那个缺陷。清零必须是**显式非 nil 指针指向 0**。
⇒ 三指针不是机械替换：**每个站点都要判定它要的是"保留"还是"清零"**。

站点计数订正：**5 个站点 / 8 处单独拷贝**（路线图说 4）：
`:269-272` / `:570-573` / `:761-764` / `:792-795` / `:1257-1260`，经 `transition` helper（`:504-506`）。

**安全论证**：今天忘抄 `in.TopoTargetGen` ⇒ 用 **0 覆盖**库里已有值 ⇒
`topoConvergedForOp:1080-1082`（`if op.TopoTargetGen == 0 { return true }`，**唯一的 fail-open 支**）
宣布收敛 ⇒ 在拓扑未收敛时把 SERVING/RETIRED 宣布出去。
改后忘传 = **该列不写 = 保留原值** ⇒ 缺陷类别从「静默毁数据 + 绕过拓扑门」降为「不更新」。
结构体自己的 doc（`:112-114`）写着 *"0 = leave unchanged is **NOT expressible**"* ——
这是作者写下的限制，本项正是把它变成可表达。

**wire 判定**：`OpTransitionInput` 无任何序列化点；上 raft wire 的是
`NewCommand(OpClusterOpTransition, Stmt(sql))` = **一段字面 SQL**（`operation_ops.go:156`）。
改的是 SET 子句包含哪几列；少几列仍是合法 SQL、任何版本 follower 应用后语义相同
⇒ 不动 `ProtoVersion`、不需重装、混版滚动升级安全（老 leader 发全三列、新 leader 只发变化列）。
**「结构体没上 wire」不够——`cluster.apply.<verb>` 是跨版本 broker↔broker 面，必须论到 SQL 文本层。**
racknerd 的存量状态**逐位不变** ⇒ **B8 不跑 drill**。

**半 B**：`xfer_inflight.go:81-92` 只存在于注释里的 5 行表 → 纯函数。
路线图的签名 `ledgerRowDisposition(primary, outbox, live, now)` **缺三个输入**：
- **`OutboxCensusOK`** —— `R6-F1` 明写 *"a failed outbox census is UNKNOWN, not empty"*；
  照抄会把"扫不动"当成"空"，正是 §S1 禁止的极性。
- **`Site`** —— 两个 pass 检查门的**顺序不同**（pass 2 的 staged-terminal 分支**不查** `outboxOwned`，
  只有 start-only 分支查），且同一 transfer 可在两目录各有一行 ⇒ disposition 是 **(transfer, site)** 级。
- 第三个见 `/tmp/tether-b8/plan-B8.md`（实现时逐条核）。

同时**订正那张注释表**——它今天缺 live / 新鲜度 / 普查失败三个维度，而这三个正是决定是否 synthesize 的东西。
表驱动测试覆盖 5 个 (primary,outbox) 状态 × {live,!live} × {fresh,stale} × {censusOK,censusFailed}
的非退化组合，其中 **`censusFailed + start-only ⇒ leaveAlone`** 是 R6-F1 的回归门、**必须有**。

**行级普查守卫（新的活缺陷，本轮修，单独 commit `fix(xfer):`）**：
`forEachLedgerRecord`（`:350-367`）把**读不动的行隔离并返回 nil** ⇒ 一个**不可读的 outbox 行**
既不进 `outboxOwned` 也不被 replay ⇒ pass 2 认为 outbox 里没有该 transfer 的终态，
对 primary 的 start-only 行合成 `failed/home_broker_restart`——而那个不可读的行里装的可能是 `complete`
⇒ 审计对同一 transfer 同时声称 complete 与 failed（`:388-391` 论证这比悬空 start 行更糟）。
注意它与 `obErr` 是**不同粒度**：`obErr` 管"整个目录扫不动"，这条管"目录能扫、**其中一行**读不动"。
**修的支点**：ledger 文件名**就是** `sha256(transfer_id)`（`:114`）⇒ 内容读不动也知道是哪个 transfer，
可计入 `outboxOwned`（"这里有一个我读不懂的终态"）从而阻止合成。
变异 = 注入一个读不动的 outbox 行 + 一个 primary start 行，断言**不**合成 failed，
**该断言在今天的代码上必须是红的**。
新行为是**少合成**一条终态（更保守）；最坏后果是真 stranded 的 transfer 因其 outbox 行读不动而
**一直不被 finalize**。

> **内审 B8-1 订正（这一段原先写错了，逐字保留以记录教训）**：原文写的是
> 「fail-closed 方向，且 `xfer-inflight-finalize` pass 每轮重试 + 日志暴露」。
> **重试那半是真的但没用**（每轮只是把 `leave` 重新判一遍），**"日志暴露"那半是假的**：
> `ledgerLeave` 分支是裸 `return`，而 `reason` 只挂在**成功**那条 Info 日志上，
> 所以这个状态**每轮产出零行日志**。运维唯一拿到过的信号，是隔离那一轮的一条 Warn——
> 可能在几周前，可能已被日志轮转掉。
>
> 而这条 disposition 的正当性恰恰建立在 `replayStagedTerminal` 那句
> *"a dangling start is **detectable and repairable**"* 上——**可检测性是要有人提供的性质**，
> 不是自动成立的。所以本轮补上：`finalizeStrandedXfers` 每轮以一条 Warn 报告
> 「有 N 个 transfer 永远无法 finalize」，点名 transfer_id（截断到 5 个）、
> 两个目录路径、以及运维该做什么（修复 `.corrupt` 文件让下一轮重放，或同时删掉
> `.corrupt` 与对应的 in-flight 行让合成器接手）。
> 由 `TestUnfinalizableTransferIsReportedOnEveryPass` 钉住——它跑**三轮**并断言每一轮都报告，
> 因为运维实际会看的是第 2、3 轮，而那正是原实现沉默的地方。

**既有测试已围住 5 个站点中的 4 个**（`TestRetireRehomeHoldIsBoundedToBlocked`，
`r8_home_delivery_test.go:758`，第 4 步 `:806-816` 会在照抄路线图时**变红**）⇒
该测试**扩展**而非新写；真正未覆盖的唯一断言是「`ConfirmOp` 重置 deadline 的同时**保留** barrier 与 topo_target_gen」。

### 2.5 B9 — 泄漏门统一 + 测试脚手架去重（一 lane 一 commit）

- **真重复约 23 处函数体**收口（`openDB` 10 / `startNATS` 4 / `silentLog` 5 / `waitFor` 4），
  **31 个一行 shim 不动**——它们让本包测试直接写 `openDB(t)` 而不必在每个调用点 import testharness，
  删掉是把 churn 摊到几百个调用点换 31 行，正是 §7.5 禁止的交易。
- **不能盲收**：每个真函数体先 diff 再决定；签名/行为不同的要么适配、要么保留并写明为什么不同。
- **`test/concurrency` 的三个本地 helper 保留**（`helpers_test.go:56-64` 有明写的拒绝理由，§7.3），
  **但订正那段注释**——它有一半是事实错误（`testharness.StartNATS` 不收 logger、不多起 goroutine）；
  真正成立的是 `SilentLog()` 按 `TETHER_TEST_VERBOSE` 返回 **stderr** handler ⇒
  在一个整包目的就是数 goroutine/fd 的包里，那会**让门本身依赖环境**。
- **`waitGoroutineBaseline`（`wal_concurrency_test.go:184`）保留**：它是**上限**断言 + 每轮 `runtime.GC()`，
  两个调用点（`transport_test.go:167`/`:486`）用它做**取真基线之前的静默前置**——不同的原语。
  它的 `fdCount` 孪生（`:176`）去重。**`transport_test.go` 不得增删任何一行**
  （`raft_timing_guard_test.go:53/:59` 按 `file:line` 豁免它）。
- **fd 容差永久保留参数**（4/5/10 各有推导：`fd_leak_test.go:6-13`、`:204-209`）。
- **harness 不能放 `internal/`**（`lint_skeleton_test.go:124` + `:181-187` 的 raft 禁闭门只白名单
  `internal/cluster`，`TestRaftConfinementSelfCheck:191` 钉住谓词）⇒ 放 `test/clusterharness/`。
  **同一次编辑放宽两处 `_test.go` 过滤**：offender walk（`:80`）**与** `countProductionConstantUses`（`:148`）——
  只放宽一处会造出"同一个门的两半对存在哪些代码意见不一致"。
  搬迁前先记录今天的 `n`（约 40 处 / 约 18 个 `_test.go`）。
- **泄漏门四份合一**：`test/d4:423` / `d5:430` / `d8:313` / `concurrency/helpers_test.go:136`
  **功能完全相同**（差异只有变量名 `n`/`nb`、注释有无、换行）⇒ 合并零损失。
  `fdCount` 的解释注释（d4 那份，点名 CLAUDE.md §5 的 NumGoroutine+fd 双门）**整段搬运**（§7.4）。
- **修 `TestTunnelServerCloseWithActiveSessionNoLeak`**（`goroutine_leak_test.go:168-199`）：
  1 session（`:188`）→ **N≥5**。容差 ±2 本身站得住（`helpers_test.go:124-128` 的噪声地板论证），
  错的是**样本量**。变异 = 往 `tunnel.go` 的 per-session 路径注入不退出的 goroutine，
  断言**旧版保持绿、新版变红**，然后用 Edit 撤销注入（**不用 `git checkout --`**，工作树有未提交改动）。
- **同时把矩阵过滤放宽到 `Spawnsafe|Leak|FDStable`**（`all_phases_test.go:415`）——否则是制造抖动：
  该包 26 个测试里 **11 个是泄漏/fd 门**，而矩阵只跑 `-run Spawnsafe`（唯一匹配 1 个），
  `make test` 又无 `-race` ⇒ **11 个泄漏门从未在任何门禁下跑过 `-race`**，
  而 CLAUDE.md §5 要求并发改动必须过 race + 泄漏门。这是**增加**覆盖，§7.6 只禁静默**减少**。
  提交前另跑 `-race -count=5 ./test/concurrency/`。
- **AST 泄漏门按"赋值 + 差值"形状建**，不是单表达式：
  原设计（比较式一侧**包含** `NumGoroutine()`、另一侧 `<ident>+<INT>`）在 73 个 `NumGoroutine()` 行里
  只命中约 6 处，**看不见它要删的四份拷贝的形状**（它们是 `last = runtime.NumGoroutine()` /
  `if last-before <= 2` 两行），而 plan 的变异取自已匹配的子集 ⇒ **变异过、门全盲**。
  正确形态：(a) 找所有 `NumGoroutine()` 的 `CallExpr` 并解析所属函数；
  (b) 在该函数内找操作数**传递地**引用"由它赋值的 ident"或其 `±` 的比较式；
  (c) 要求所属测试函数含循环 / `rounds>=5`，否则要豁免条目；(d) 另按名字扫任何泄漏轮询 helper 的调用。
  **两个变异都必须变红**：当代形状 + **逐字复现的历史形状**。历史形状不红 ⇒ 门还没建成。
- **诚实行数**：d4/d5 **不是孪生**（都 468 行但规范化后仍 578 行 diff，`comm -12` 共同行仅 **251**）。
  可收的是那 251 行共同核心 + 四份泄漏门 + 约 23 处函数体，**不是 1992 行整体**。
- **每条 lane 的验证命令按 tag 事实写对**：`test/d3`/`test/d4` **无 build tag** ⇒
  `go test -race ./test/d3/...`；`d5..d9` 有 ⇒ `go test -tags dN_integration -race ./test/dN/`。

---

## 3. 前置特征化测试（重构前必须写、且在未改动树上必须绿）

按书写顺序：

| # | 测试 | 位置 | 钉住什么 | 在今天的树上 |
|---|---|---|---|---|
| P1 | `BuildMergedConf` 完整 merged 文本 golden（每种意图 × 4 个真实 conf fixture，含 racknerd 形状） | `internal/natsconf/` + `testdata/*.conf` | B5 的**现网字节中立性**——真正落盘的字节，不是 `Render` 的输出 | **必须绿** |
| P2 | `renderClusteredCutoverConf` 对"缺 `http:`"的 conf 的当前输出 | `internal/broker/` | cutover 渲染在重构后逐字节不变 | **必须绿** |
| P3 | 「只设 barrier 的 transition **不改** `topo_target_gen`」 | `internal/cluster/operation_ops_test.go` | B8 半 A 的安全属性 | **必须红**（今天会清零）——这就是缺陷凭证 |
| P4 | 「不可读的 outbox 行 + primary start 行 ⇒ **不**合成 failed」 | `internal/broker/xfer_inflight_test.go` | B8 行级普查守卫 | **必须红** |
| P5 | registry：`authority` 在一个 sweep 内**只被调用一次** | `internal/broker/reconcile_registry_test.go` | B7 的 per-sweep 一致性 | **必须绿**（今天只调一次 `isLeader`） |
| P6 | `(目录, 测试函数名)` 恒等多重集 + 九个矩阵 `-run` 过滤集快照 | 脚本产物，非测试 | B6 的验收基线 | 快照 |

P3/P4 必须红——一条**在改动前就绿**的"特征化测试"若声称覆盖一个缺陷，那它没覆盖。

---

## 4. 会被扰动的 line-keyed / AST 门（实测全集）

| 门 | key 形态 | 谁扰动 | 处理 |
|---|---|---|---|
| `test/determinism/cfgdb_ratchet_test.go:158` | **精确相等** `total != cfgDBBaselineTotal`（=119）+ per-function 表按 `file.go:funcName` | **B5/B7/B8/B9** 全部 | 新代码走 `b.read()`/`b.livenessDB()`/`b.singleWriter()`；**任何**增减都要同 commit 改常量与表。表扛得住行号漂移，**扛不住函数改名/跨文件搬迁** |
| `cmd/tether/error_code_coverage_test.go:92` | 散文里写 `xfer_inflight.go:504` | **B8** | key 是错误码、门不会红，但散文要同步（§A4 精神） |
| `cmd/tether/error_code_coverage_test.go:236` | map key `internal/broker/topology_reconcile.go:149` | **B5** | B5 动 `:212-242`（在 `:149` 之后）⇒ 不移位。**若在 `:149` 前插行必断**，实现时核 |
| `internal/auth/acl_reconcile_test.go:611` | map key `internal/broker/observability.go:81` | **B7** | B7 动 `:247-290`（在 `:81` 之后）⇒ 不移位。同上 |
| `internal/broker/proxy_cluster_guard_test.go:62` | 错误串手抄文件名 `proxy_reconcile.go` | **B7** | 若把 `driveProxyReconcile` 搬离该文件则同步该串 |
| `test/determinism/raft_timing_guard_test.go:53/:59` | `internal/cluster/transport_test.go:509/:510` | **B9** | **不得在 `transport_test.go` 增删任何一行** |
| `test/determinism/raft_timing_guard_test.go:80` + `:148` | 两处 `_test.go`-only 过滤 | **B9** | **同一次编辑放宽两处**，否则门的两半互相矛盾 |
| `cmd/tether/error_code_coverage_test.go:817` | `os.ReadFile(… "error_code_coverage_test.go")` **读自己的名字** | 无（不在那 158 内） | 登记，将来改它要同步这一行 |
| `internal/broker/admit_test.go:394` | 错误串手抄 `cmd/tether/error_code_coverage_test.go` | 无 | 登记 |

**另有一个必须修的遗留缺陷（上一批 B3 的）**：
`cfgdb_ratchet_test.go` 的**指导文本**在三处（`:161`、`:187`、`:262`）叫人用 `b.liveness()`，
而 `internal/broker/dbrole.go:82-84` 明写 *"THERE IS DELIBERATELY NO liveness() ACCESSOR HERE"* ——
**那个方法上一批被删了**。撞上棘轮的人会被这个门自己指去调一个编译不过的方法，
而它下一句还写着 "see internal/broker/dbrole.go for which"。⇒ 三处改成 `b.livenessDB()`；
`:262` 是自检 fixture，改后确认自检仍过。

### 通用原则（本轮新立，写进 `docs/testing-standards.md`）

**非空性地板不能对"目标是清空的那棵树"计数。**
门 G 的目的是"让树上不再有 X"，却给它加"必须还能在树上找到 ≥N 个 X"的地板
⇒ **G 成功之日即 G 失败之日**，于是产生"别把 X 清干净"的结构性激励。
正确形态：非空性跑在**合成样本**上（每种形态一个正样本 + 反例控制），
证明"扫描器还认得 X"而不是"树上还有 X"（`docs/testing-standards.md:129-135` G2 已有此意）。
反例（合法地对活树计数）：`raft_timing_guard_test.go:120-126` 的
`countProductionConstantUses < 10` —— 它成立**只因为**那个门的成功状态里目标物仍然存在。
**判据：非空性对活树计数，仅当该门的成功状态里目标物仍然存在。**

---

## 5. 门禁与 checkpoint 时序、commit 序列

**纪律**：编辑中途**不跑全量闸**（它编译的是一棵不存在的树，红得没有信息量还会误导归因）。

| checkpoint | 命令（全部带 `PATH=/usr/local/go/bin:$PATH`） |
|---|---|
| 每个 step 后 | 只跑碰过的包：`go test ./internal/natsconf/ ./internal/broker/ …` |
| 触 tag 后的包后 | `go vet -tags 'd5_integration,d6_integration,d7_integration,d8_integration,d9_integration,phasefluidity_integration,e2e_matrix' ./...` |
| 每个 item 收尾 | `make test` + `make lint` |
| B9 每条 lane | d3/d4：`go test -race ./test/dN/...`；d5..d9：`go test -tags dN_integration -race ./test/dN/` |
| 并发改动 | `-race` + 内建 NumGoroutine/fd 门；`test/concurrency` 另跑 `-count=5` |
| 全批次收尾 | `make e2e-parallel`（唯一全矩阵闸，**并行全绿即通过，不得再串行复核**） |
| B5 收尾 | 四个 simcluster drill，见 §6 |

**commit 序列**（单开发者，直接提 `main`，无 phase 分支）：

| # | commit | 范围 |
|---|---|---|
| 1 | `test(quality): pin pre-refactor behaviour for batch-B remainder` | P1/P2/P5 + P3/P4（带 `t.Skip` 说明它们在下一 commit 转绿？**不**——P3/P4 与其修复同 commit，见 #3/#4） |
| 2 | `test(naming): freeze process-event test filenames` + `docs: CLAUDE.md §3 step 5b` | B6-a |
| 3 | `refactor(cluster): three-pointer OpTransitionInput` | B8 半 A |
| 4 | `refactor(broker): pure ledgerRowDisposition + corrected precedence table` | B8 半 B |
| 5 | `fix(xfer): an unreadable outbox row must not license a synthetic terminal` | B8 行级守卫（**行为改动，单独**） |
| 6 | `refactor(broker): bound the leader-maintenance and proxy-reap paths` | B7 步 1 |
| 7 | `refactor(broker): move the two leader duties into the reconcile registry` | B7 步 2-3 |
| 8 | `refactor(broker): per-pass authority + per-iteration loop liveness` | B7 步 5-8 |
| 9 | `refactor(natsconf): single RenderDesired for all five assembly points` | B5 |
| 10 | `fix(natsconf): fail closed on jetstream without store_dir; drop the unreachable JS domain` | B5 BUG-1/BUG-2 |
| 11..N | `test(harness): …`（一 lane 一 commit） | B9 |
| N+1 | `test: move process-event-named test files to topic names` | B6-b |

---

## 6. simcluster drill 计划（只有 B5 需要）

**先修两个假绿陷阱**：
1. `local.sh` **只在传 `--build` 时**才 staging 二进制（`:49-51`；`:54` 只在 `vendor/tether` 完全缺失时报错），
   `README.md:129` 明写要在 "once, **or after a code change**" 重建 ⇒
   不带 `--build` 跑出的 GREEN **什么都没证明**。
2. `sim_stage_binaries` 以 `command -v go` 开头，而本机 `go` **不在 PATH** ⇒ `--build` 本身会 abort。

```
cd /home/weiland/dist_experiment_control/test/simcluster
PATH=/usr/local/go/bin:$PATH ./local.sh --build build     # 用 B5 之后的树重建 image
PATH=/usr/local/go/bin:$PATH ./local.sh drill 10-grow-to-3
PATH=/usr/local/go/bin:$PATH ./local.sh drill 20-forcesingle-natsconf
PATH=/usr/local/go/bin:$PATH ./local.sh drill 41-shrink-to-standalone
PATH=/usr/local/go/bin:$PATH ./local.sh drill 43-migrate-live-data
```

**并且必须有一步证明 drill 跑的是新二进制**（实例内取 version / 比对二进制哈希）——
**未经证明的重建 = 假绿**。这是定位铁律（"靠复杂脚本才成功的操作是缺陷不是成就"）在**验证侧**的镜像。

| drill | 它能抓什么 |
|---|---|
| `10-grow-to-3` | cutover 渲染与 reconciler 渲染的真实一致性；`IntentForceClustered` 路径 |
| `20-forcesingle-natsconf` | `IntentForceStandalone`；BUG-1 守卫在真 conf 上不误伤 |
| `41-shrink-to-standalone` | 脱簇路径；`cluster_operation_controller.go:1152-1166` 那段 "DELIBERATELY *NOT* FIXED" 的形状 |
| `43-migrate-live-data` | 手工 takeover（`IntentStandaloneIfLone` 的 `len>1` 分支）；`--plan` 不改任何东西 |

### 6.1 实际运行结果（2026-07-27，weilandserver，image 已从本轮树重建）

**先证明跑的是新二进制**（`local.sh:49-51` 不传 `--build` 就复用 `vendor/tether`；未经证明的重建 = 假绿）：
baked 二进制含三条**本轮才存在**的字符串——B8 的 `"stranded past timeout + slack"` 与
`"outbox census failed"`、B5 的 `"refusing to render a conf with NO JetStream block"`——
且 mtime（10:21）晚于最后一次源码改动（09:55）。

| drill | 结果 | 对 B5 的意义 |
|---|---|---|
| `10-grow-to-3` | **GREEN，19 断言，0 gap** | grow cutover 的渲染路径（收口进 `ApplySecretsDirIdentity` 之后）在真部署栈上正确；含 leadership transfer 与 follower-kill 的 HA 证明 |
| `20-forcesingle-natsconf` | **GREEN，16 断言，0 gap** | **BUG-1 的新 fail-closed 守卫没有误伤**真实的 offline force-single 脱簇路径：`--reset-js` 移走 clustered JS store、tier-B 在 N=1 上恢复工作、废弃 peer 被 prune（无幽灵 VOTER） |
| `41-shrink-to-standalone` | **31 pass / product_red=0 / assert_fail=0 / 2 GAP** | 见下 |
| `43-migrate-live-data` | **GREEN，38 断言，0 gap** | 手工 takeover（`IntentStandaloneIfLone` 的 `len>1` 分支）+ `--plan` 不改任何东西 + **逐字节精确回滚**（`tether.db` md5 一致、`nats.conf`/`broker.yaml` 原样） |

合计 **104 条断言通过，零产品失败**。

### 6.2 内审后重跑（同日 13:25，四个 drill 全部重来）

上表那一轮跑的是 **10:21 的镜像**，而 B5 的**四装配点收口**发生在之后（内审 completeness audit 判 B5-2
NOT DONE 之后才补的）。收口改的正是 `takeover` / `force-single` / `grow cutover` 三条**真实部署渲染路径**，
所以拿旧镜像的绿当数，就是本节开头警告的那种**假绿**。四个 drill 全部对新镜像重跑。

**新的"证明跑的是新二进制"方法**——上一轮用的字符串探针**这次不成立**，如实记下为什么：
`RenderIntent.String()` 返回的 `"standalone-if-lone"` / `"force-clustered"` 只被测试调用，
Go 链接器把死代码连同字面量一起裁掉了 ⇒ **strings 探不到它们不代表二进制是旧的**。
改用两条无歧义的证据：
① `find <repo> -name '*.go' -newer vendor/tether` **为空**（二进制比每一个 `.go` 都新）；
② 用 `local.sh` 同样的命令行（`CGO_ENABLED=0 GOOS=linux GOARCH=amd64 make build VERSION=v0.0.0-simcluster`）
从当前树重编一份，与 baked 的 `vendor/tether` **sha256 完全相同**。②是等值证明，比字符串探针强。

| drill | 收口前（10:21） | 收口后（13:25） | 结论 |
|---|---|---|---|
| `10-grow-to-3` | GREEN 19 / 0 gap | **GREEN 19 / 0 gap** | 一致 |
| `20-forcesingle-natsconf` | GREEN 16 / 0 gap | **GREEN 16 / 0 gap** | 一致 |
| `43-migrate-live-data` | GREEN 38 / 0 gap | **GREEN 38 / 0 gap** | 一致 |
| `41-shrink-to-standalone` | 31 pass / 2 GAP | **31 pass / 2 GAP（同两条）** | 一致 |

四轮合计仍是 **104 断言、`product_red=0`、`assert_fail=0`**，drill 41 的两个 GAP 逐字相同。
**逐项一致本身就是这次要的结论**：把四处手写装配换成一次 `RenderDesired` 调用，
在真实部署栈上**没有改变任何一条渲染路径的行为**。（单元侧的对应证据是四份 `BuildMergedConf` 全文 golden 未动。）

**drill 41 的两个 GAP 与本轮无关**，逐字记录以免被误当成回归：
它们是 agent 在「**非终态** retire、被退役 broker 的 nats-server 仍在网格中」时，
主动快速路径 `rosterRequiresReconnect`（`roster.go:409-437`）未在窗口内把 agt1 物理迁走。
drill 自己写明这不是正确性失败：leader 仍能应答 agt1 的 roster 刷新，
所以 silence-rebuild 路径按设计不触发（它只在**真隔离**时触发，而同一 drill 的 N=1 孤岛臂证明了它会触发），
agt1 全程**功能可达**（下方断言证明），真正退役时经 ~20s 断连看门狗完成迁移。
**与 nats.conf 渲染、与 B5 的任何改动无关。**

**已知未覆盖（写下来而不是假装覆盖）**：
- `IntentStandaloneIfLone` 的 **`len(peers)==1` 打在已 clustered conf 上**这条臂（drill 43 带 `--peer`，`len>1`）
  ⇒ 由 §2.1 的 T4 表驱动测试覆盖，**不由 drill 覆盖**。
- offline force-single 打在**已 standalone** conf 上的幂等臂（drill 20 从 clustered 起步）
  ⇒ 由 T6 的"零次写"hermetic 测试覆盖。

---

## 7. 永不做（永久决定，各带 file:line 依据）

> 本节的含义是**此后永不做**。凡因"大 / 难 / 要前置条件"而入列的，都已被移出。

| # | 永不做 | 依据 |
|---|---|---|
| N0 | **把 `driveProxyReconcile` / `driveLeaderMaintenance` 搬进 registry** —— 实现时发现**两条可行路径都是真回归**，路线图没有预见这一点 | **(a)** 只搬这两个而 `observeOnce` 留在 observe 循环 ⇒ 二者落到**两个 goroutine**：`ReconcileMembershipOnLeadership`（前向补全半完成的 AddNode）会与 `driveInFlightOperations`（推进同一个 op）交错，成为同一节点 membership phase 迁移的**两个并发提议者**（§8.1 no-silent-fork 要防的正是这个）；`observeOnce` 的 `driveAutoRebalanceOnReturn` 与 `driveProxyReconcile` 也会并发移动 `__proxy__` home。幂等论证（phase 写是 `WHERE phase IN (preds)`）覆盖的是**重复应用**、不是**两个不同迁移的交错**。**(b)** 连 observe tick 整体搬进来以保住顺序 ⇒ 它的 2s scatter-gather（`observePollWindow`）落到 `runMu` 之下，而 `node-states`/`ports`/`tunnel-sessions` 是 **1s** 节奏 ⇒ 每 5s 被一个无关职责卡住最多 2s，是收敛延迟的真回归。**今天的形状拥有两个替代方案都会失去的性质**：observe ticker 是**一个** goroutine，`fsArm.observeLeadership` → leadership 边沿 → `ReconcileMembershipOnLeadership` → `driveLeaderMaintenance` → `driveProxyReconcile` → `observeOnce` 按此顺序执行且不与任何东西共享锁——**那就是串行性论证本身**。论证已整段写入 `reconcile_registry.go` 的 "WHY THE TWO LEADER DUTIES STAYED OUT" |
| N1 | **registry 的 `own-goroutine-with-timeout` 执行模式** | ① `reconcile_registry.go:68-71` 明文 "starts **NO goroutines** and owns **NO timers** … keeps it **invisible to the repo's NumGoroutine/fd leak gate**"；② 假时钟等价性证明（`reconcile_registry_test.go` 十余处 `runDue(ctx, clk.advance(...))` 全断言"返回后副作用已发生"）会全变竞态；③ 它直接制造 **gate/执行 TOCTOU**（`seed_converge.go:160` 自身无 leader 门，今天的门是紧挨着的 `observability.go:277`）与**串行域丢失**（`ReconcileMembershipOnLeadership` 与 `driveInFlightOperations` 变成同一节点 membership 迁移的两个并发提议者）；④ 路线图为它给的三个阻塞理由（`observePollWindow`/`waitNatsLoaded`/`pub.tick`）**全部落在它自己第 494 行禁止合并的那五条循环里** ⇒ 它要解决的问题只存在于那个被禁止的合并中。**替代动作（限界 + inline 搬迁）本轮全做。** |
| N2 | **把五条集群循环并进 registry sweep** | `audit-publisher` poll 默认 **100ms**（`audit_publisher.go:106-108`）而 `granularity()` 驱动 broker 主 `Run` 循环 ticker（`broker.go:1282-1287`）⇒ 合并把核心循环调快 10×；一次 `pub.tick` 可阻塞 `N × 5s`（`audit_publisher.go:39,132-144,156-189`）而 `runMu` 全程持有（`reconcile_registry.go:134-137`）⇒ 冻住其余 pass **与 `ctx.Done()`**；`topology-reconcile` 刻意 per-broker 且 `:60` 明说"single-goroutine so `reloadNatsServer` calls are serial (no single-flight mutex needed)"；`alert-webhook` **根本没有 cadence**（`alert_webhook.go:157-166` 是 `select{ctx.Done; <-p.ch}`）**无法表达为 pass** |
| N3 | **把 `driveAutoRebalanceOnReturn` 搬进 registry** | 它不是周期职责——吃的是同一 tick scatter-gather 算出的 `broker_down` 边沿（`observability.go:157-175`、`:201`）；且 `autoRebalanceEnabled()` 默认 off（`proxy_auto_rebalance.go:104`）⇒ 注册它会让 `runs` 为一个什么都不做的机制爬计数，正是 A5/F-03 那个缺陷再来一次 |
| N4 | **给 `ClusterAdmin.StatusReport` 加 `context.Context`** | 2 个生产调用者（`cluster_operation_controller.go:1083`、`clusterstatus.go:688`）vs **~15 个测试调用者跨 7 文件**，其中数个在 B6 改名清单上 ⇒ B7×B6×B9 三方冲突；而界在 `clusterwrite.go:416-418` 的注入点已有。将来若真需要，该改 `streamObserve` seam 类型（`clusteradmin.go:68`） |
| N5 | **用 `goleak` 换掉内建泄漏门** | 它比较 goroutine **栈**对忽略表，对本轮那个缺陷无能为力（缺陷是**练习次数太少**，不是计数机制）；内建门另有 **fd** 轴（`/proc/self/fd`），而"泄漏的 NATS reply-inbox 订阅是被池化 goroutine 掩盖的 fd 泄漏"（`test/d4/setup_test.go:445-448`）。CLAUDE.md §5 已写"刻意不用 goleak" |
| N6 | **删 `xfer_inflight.go` 的 outbox** | 路线图 §2.3 / §7.3；它是关闭第二个崩溃窗口的机制（`:51-60`） |
| N7 | **合并 `natsconf` 与 `broker`/`cluster`/`clusteroffline`** | `preflight.go:9-10`（"It is a leaf: NO raft / NATS-runtime imports"）、ex-`natsreconcile` 包注释（"NO nats, NO raft, NO broker → L-2 clean"）、`lint_skeleton_test.go:124` 的 raft 禁闭门。B5 的合并建立在**相反**证据上：`natsreconcile` 同时 import `natsconf` 与 `natscluster`，且被迫导出 `SynthesizeClusterListen` 纯粹为了跨包同步 |
| N8 | **顺手合并其余小包**（`clustermanifest`/`clusterupgrade`/`xferaudit`/`testharness`） | §7.1 `:578-585`：判据是 import 面而非行数；`internal import = 0` 的叶子包几乎永远该保留（`testharness` 被 **37 个测试文件**引用） |
| N9 | **删/"治愈" `filterGhostPeers` / `filterSelfOnly`**（`topology_reconcile.go:244-293`） | 15 行注释记录了 G2 #12 双重故障与刻意的 fail-SAFE-not-fail-open 取舍；**它是让现网 racknerd（幽灵 `pc732`）渲染 standalone 的机制**。§7.3 字面适用 |
| N10 | **把 `Preflight` 的 fail-closed 拒绝软化成警告** | `preflight.go:116-168` + §S2（安全默认不能用警告替代）。BUG-2 的修法是把 **Render 对齐到 Preflight**（删 emitter），绝不反向 |
| N11 | **重命名 `Config`→`Desired` / `Render`→`RenderConf` / 任何 `Action*` 常量** | 5 个生产调用点 + 44 个测试函数 + `docs/architecture.md:2295`，零行为/编译期收益。§7.5 的纪律对标识符同样适用 |
| N12 | **把 `buildTopologyInputs`（`topology_reconcile.go:212-242`）搬进 `natsconf`** | 它读 SQLite roster（`clusternodes.ListPeersForTopology`）与 committed raft 配置 ⇒ 会破坏 N7。包边界是"已解析的输入进、conf 文本出" |
| N13 | **把 `catchup_deadline` 拆成四列** | 见 `/tmp/tether-b8/plan-B8.md`；四种语义分时复用由两段人工论证维持，拆列要改 schema + 迁移，而三指针已把**咬人的那个机制**（忘抄⇒清零⇒绕过拓扑门）消掉 |
| N14 | **改 `test/pN` / `test/dN` 目录名** | 矩阵契约靠字符串字面量（`all_phases_test.go` 13 处、`split.go:60`、`shard.go:76`）+ 24 个 `//go:build` tag 名按约定镜像目录 |
| N15 | **把测试文件改名合并进已存在的文件** | L07 例表 6 个目标名里 5 个已存在，其中 `clusteroffline` 那个跨 `package` 子句 ⇒ 不编译 |
| N16 | **把 `waitGoroutineBaseline` 并进统一泄漏门** | 它是**上限**断言 + `runtime.GC()` 静默原语，两个调用点用它做取真基线前的前置（`transport_test.go:167`/`:486`）——不同的原语。强并会么丢 GC、要么把 GC 塞进全仓每条泄漏断言 |
| N17 | **把 fd 容差统一成一个常量** | 4/5/10 三个数各有写下来的推导（`fd_leak_test.go:6-13`、`:204-209`） |
| N18 | **把集群 harness 放 `internal/`** | raft 禁闭门（`lint_skeleton_test.go:124`、`:181-187`，`TestRaftConfinementSelfCheck:191`）；harness 必须 import raft |
| N19 | **重命名带过程前缀的未导出测试 helper**（如 `g3AdminWithSelf`） | 既非 S1 B6 亦非 L07 F1/F2 所提；B6 是文件名 + 一次合并。列此以防中途扩范围 |
| N20 | **把 `test/c7/drill_test.go` 的清理算进本轮** | 它是死文件（`c7_integration` tag 全仓无 runner 引用、函数体无条件 `t.Skip`），但归 `S2-deletion-inventory.md` 行 T2，**不在 B5–B9 范围**。不擅自扩范围 |
| N21 | **给每条 `reconcilePass` 加 `budget time.Duration` + `context.WithDeadline`**——本 plan §0.4 曾写"**本plan采用**"，实现时改为不做，**这是把那次改主意补记为永久决定**（内审 completeness audit B7-2：丢掉一个 plan 已采纳的设计而不入 §7，是账本失真） | 它会成为**装饰性超时**：`runDue` 顺序执行、全程持 `runMu`（`reconcile_registry.go:134-137`），deadline 到期只能让 pass 自己的 ctx 变 Done，**不能抢占它**——一个不检查 ctx 的 pass 照样把整个 sweep 连同其余 pass 一起冻住，而会检查 ctx 的 pass 本来就该被调用方限界。要让超时真能取消东西，必须给每条 pass 一个 goroutine，那正是 **N1** 已永久拒绝的执行模型（含泄漏门不可见、假时钟等价性全变竞态、gate/执行 TOCTOU 三条独立理由）。**替代动作本轮全做**：真会无限挂的两处 `ObserveReplicas` 在**各自 seam** 上限界（deadline 在那里真能取消 NATS 请求）+ AST 门防第三处；可观测性用 `lastDur`/`maxDur`/`overruns` 交付，其中 `maxDur` 的**高水位**性质经变异验证（内审 §3.1 曾指出该断言 VACUOUS，已重写为"第二轮恢复"形态并证明变红） |

---

## 8. 覆盖账本（路线图每条义务 → YES / DECLINED）

> **本表在内审后被逐行重核（2026-07-27）。** 内审的 completeness audit 发现它有 **7 处方向一致的虚报**
> ——都是"计划要做、账本记 YES、代码里没有"。这不是笔误的方向：账本是运维会拿来**代替读 diff** 的东西，
> 单向虚报正是它最坏的失效形态。处置：**不改账本去迁就代码，而是把代码补到账本说的样子**，
> 只有 `B9 一 lane 一 commit` 一条无法在外审前满足，已如实标注。
> 每条重核过的行都带 **✔核** 与证据；无标记的行是内审未质疑、本次未复查的。

| 项 | 路线图义务（原文摘） | 处置 |
|---|---|---|
| B5 | 导出 `RenderDesired(in, own, override)`，5 装配点只提供真正不同的意图 | **YES ✔核**（4 种意图，非 3）。内审时此项为 **NOT DONE**（`grep RenderDesired` → 0 命中），现已补齐：`internal/natsconf/render_desired.go`，**四个装配点全部收口**（reconciler / grow cutover / `--to-standalone` / takeover），全树 `natsconf.Config{…}` 只剩 `RenderDesired` 一处，由**两把互补扫描器**看守（字面量 + 赋值式；`TestTheTwoScannersHaveDifferentBlindSpots` 保证二者不退化成同一把）。第四种意图 `IntentStandaloneIfLone` 有真实生产调用者（takeover），其必要性由 `render_intent_test.go` 的三情形表证明——把它折进任一相邻意图都必红 |
| B5 | `natscluster` + `natsreconcile` 合并进 `natsconf` | **YES** |
| B5 | 钉住 `cluster_grow_cutover.go:233-235` 的 byte-identity 声称 | **YES ✔核**——收口后这条不再是"注释声称 + 测试钉住"，而是**两条路径调用同一个函数**，字段不可能一有一无。原注释还挂着一句假的 "Pinned by TestCutoverRenderMatchesReconcilerRenderExceptTheMonitor"（该测试用自带 fixture，够不到本函数；删掉被钉的行仍全绿），已订正并补上真正的 characterization test（`internal/broker/cutover_render_test.go`，plan §3 行 P2 承诺却从未落地的那个） |
| B5 | `SwapIntent` 那半 | **YES**（不拆到下一轮） |
| B5 | 四个 simcluster drill | **YES**（带 `--build` + 证明新二进制） |
| B6 | 141/499 测试文件归位 | **YES**（实测 **158**，正则可复现；见 §2.2 数字订正） |
| B6 | 逐字保留 doc comment + `// origin:` 行 | **YES ✔核**。doc comment 逐字保留一直成立；`// origin:` 行内审时为 **NOT DONE**（全仓 10 处，且全在本轮新写的文件里，160 个改名文件**一个都没有**）——而这正是**承重的那一半**：改名删掉的是文件名这个唯一溯源载体。现已补齐：逐个读过 160 个改名文件，**66 个正文本就写明来历**（不重复加行），**94 个没有、全部补上**，共 103 处。每行尽量指向**真实存在的评审文档**（44 行带 `docs/reviews/*.md`），由 `test/determinism/origin_line_test.go` 看守——文档被移走/改名即变红，变异验证过 |
| B6 | tunnel round 合成一张表 | **YES**（4 个 verb） |
| B6 | CLAUDE.md §3 step 5b | **YES** |
| B6 | 前后测试函数名集合 diff 为空 | **YES**（`(目录, 函数名)` 恒等多重集 + 九个矩阵 `-run` 集） |
| B6 | −500 行 | **诚实修正为 ≈ −190**（`L07:139` 自己说改名 0 净减行）——不是缩范围，是路线图数字错 |
| B7 | `reconcilePass` 加执行模式 inline / own-goroutine-with-timeout | **DECLINED（N1，永久）** |
| B7 | per-pass state slot | **YES ✔核**：`lastDur` / `maxDur` / `overruns`，用注入时钟量，落进 `reconcilePassStatus` → adminsock → CLI 的 `SLOWEST`/`OVERRUNS` 两列。这是路线图 line 492-493 的"最小闭合"（把「op 驱动器卡住」从不可观测变成可观测）在**能真正做到**的形态下的交付。**两处内审订正**：(a) 本 plan §0.4 曾写"采用"的 `budget` + `context.WithDeadline` 被放弃却没入 §7，现补为 **N21**；(b) `maxDur` 的高水位断言原本 **VACUOUS**——`slow` pass 每轮都把假时钟推 3s，所以第二轮又是 3s，把 `if dur > p.maxDur` 改成无条件赋值也全绿。已重写成"第二轮**恢复**"形态（同时断言 `LastDur == 0 && MaxDur == 3s`）并变异验证必红 |
| B7 | `driveProxyReconcile` / `driveLeaderMaintenance` 移进 registry | **DECLINED（N0，永久）**——实现时发现两条可行路径都是真回归，见 N0。**替代交付**：把它们**真正会无限挂**的两处 `ObserveReplicas(context.Background())`（`clusterwrite.go:417` streamObserve seam、`:477` `clusterStreamsReady`）在各自 seam 上限界（那里的 deadline 真能取消东西），并加 AST 门 `TestNoUnboundedJSObservationOnTheLeaderTick` 防止第三个出现（带非空性地板，扫不到 2 个站点就 Fatal）。第二处是 critique 抓到的——只堵一处等于没堵 |
| B7 | `leaderOnly bool` → `authority func() bool` | **YES**（每 sweep 只评估一次） |
| B7 | 「4 个游离循环的 lastTick/runs/lastErr 塞进 RuntimeReport（已在 A5 做）」 | **路线图记错 ⇒ 本轮补做**：A5 做的是骨架 + 接线，每迭代活性字段被批次 A 的 review F-03 删了。本轮用 `GoEvery` 交付**每迭代**证据（不复活 `Runs`） |
| B7 | 明确不做：五条循环并进单 goroutine | **DECLINED（N2，永久）**，附四条独立证据 |
| B8 | `SetBarrier` bool 门三列 → 三个指针 | **YES**（三态：nil / &v / &zero；路线图说的"清零传 nil"是错的） |
| B8 | 5 行优先级表 → `ledgerRowDisposition` 纯函数 + 表驱动测试 | **YES**（签名补 `OutboxCensusOK` / `Site` 等三个输入，并订正那张注释表） |
| B8 | 只提纯、不删 outbox | **YES（N6）** |
| B8 | 不动 schema | **YES**（论到 SQL 文本层） |
| B9 | `testharness` 扩两层 | **YES ✔核**（`test/clusterharness/`，不放 `internal/`——N18）。内审时为 **NOT DONE**（该目录不存在，251 行共同核心一行未收）。现已落地 141 行共享包，四个 setup 文件净减 **192 行**（d3 −3 / d4 −63 / d5 −78 / d8 −51）。收的是**真正逐字相同**的三样：`RouteCA`（三份，仅 CN 串不同）、`WaitForCond`（四份，d8 拼作 `waitFor`）、`FreePort`（两份）。**顺带修掉一个真缺陷**：d8 那份 CA 把两处 `x509.CreateCertificate` 的 error 丢了（`der, _ :=`），失败时产出 nil-DER 证书，报错要到几秒后的 raft TLS 握手层才出现；共享实现就地 `t.Fatalf`。**每套件本地名保留**（`newRouteCA`/`waitForCond`/`freePort`），~65 个调用点不动。d3/d4/d5/d8 均已 `-race` 单跑全绿 |
| B9 | `assertNoGoroutineLeak`×4 → 1 | **YES**（四份功能完全相同，零损失） |
| B9 | `openDB`×11 / `startNATS`×6 / `silentLog`×13 换成已有导出 | **YES ✔核**。内审时为 **PARTIAL**——只有 `openDB` 收了（9 处），`startNATS` 还剩 4 份、`silentLog`/`silentLogger` 还剩 5 份完整重复体。现已全收：`startNATS` 四份（`internal/broker`、`internal/agent`、`test/concurrency`、`test/p2`）与 `testharness.StartNATS` **逐字相同**，`silentLog` 五份中两份连 `TETHER_TEST_VERBOSE` 逃生口都是逐字复制的。另有 `waitForCond`×4 / `freePort`×2 收进 `test/clusterharness`（见上一行）。本地名一律保留成一行委托，调用点零改动 |
| B9 | 顺带修 `TestTunnelServerCloseWithActiveSessionNoLeak` | **YES**（N≥5）**并**放宽矩阵过滤，否则是制造抖动 |
| B9 | 每个泄漏断言把被测对象练习 N≥5 次 | **YES ✔核**（AST 门强制）。内审时该门是 **PARTIAL / 无牙**：合规判据只是"函数体里有没有 `for`"，于是最常见的假形状——`for _, tc := range cases { t.Run(... 取基线、练一次、断言 ...) }`——直接通过（那是循环**子用例**，不是循环**练习**）。判据已收紧为两个合取：(1) 循环**不得包含**泄漏断言本身；(2) 轮数必须**静态可解析**且 ≥5。收紧后立刻抓出 4 个站点，逐个查证**全是判据太窄误报**（`for range K`、局部 const 界、`range make([]int, N)`）⇒ 修判据而非改被测代码，并把这三种真实写法加进合成自检表。变异验证：注入审计点名的 sub-case 形状必红。另清掉两条死条目（`singleExerciseExemptions` 唯一的 key 指的是 helper 不是 Test 函数、永不可能匹配；`leakAssertHelpers` 列了全仓不存在的 `assertNoGoroutineGrowth`），并加 `TestSingleExerciseExemptionsAreLiveAndSiteScoped` 防止再出现 |
| B9 | 明确不做：换 goleak | **DECLINED（N5，永久）** |
| B9 | 一 lane 一 PR | **⚠ 外审前无法满足，如实标注**。内审 completeness audit 记 **NOT DONE**，属实：整批仍是**一棵未提交的工作树**。但这不是拖延——CLAUDE.md §3 的 7 步流程把 `commit`/`push` 放在 **step 7（外审通过之后）**，而记忆里的用户规则又要求"外审阶段主进程不要 `git add`"。两者合起来意味着**任何**分 lane 提交都必须发生在外审之后。**处置**：外审通过后按 §5 的序列切分提交，本行届时才可能翻 YES；在此之前它是**已知且已声明**的未完成项，不得记成 YES |

**本轮额外交付（不在路线图里，但不做就是留着已知缺陷）**：
BUG-1（JS store_dir 守卫补齐两条自动路径）、BUG-2（删不可达且会自伤的 JS domain 管路 + 泛化 render allow-list 守卫）、
`xfer_inflight` 行级普查守卫、泄漏门测试从未跑过 `-race` 的矩阵缺口。
以下两条内审时被点名为"账本声称已交付、实际没有"，现已补上：
`cfgdb_ratchet_test.go` 三处指向已删 `b.liveness()` 的订正（改为 `b.livenessDB()`，含自检 fixture）、
`docs/testing-standards.md` **G2b**「非空性地板不得对目标是清空的那棵树计数」——原先只以注释形态存在于两个**本就做对**的门里，
而这条原则的全部价值就是让**下一个人**找得到，所以它必须在文档里。

---

## 9. 内审判定（2026-07-27）

内审用 15 个 agent 跑了 6 条 lane 的对抗审查 + 逐条 verify + 两个横切审计
（**完整性/反拖延** 与 **变异诚实性**），报告在 `/tmp/tether-b2-review/`。主进程逐条裁决的结果：

**变异诚实性审计**（28 次真实变异注入，全部用 `Edit` 还原并逐文件 sha256 校验）判定
**25 REAL PROOF · 2 OVERSTATED · 1 VACUOUS**。三条全部采纳并修复：

| 编号 | 判定 | 处置 |
|---|---|---|
| §3.1 | **VACUOUS** — `maxDur` 高水位性质从未被证明 | 重写测试形态（第二轮恢复）+ 变异验证必红。见 §8 B7 行 |
| §3.3 | **OVERSTATED** — plan §2.1 T1 把证明归给了一个够不到该代码的测试；plan §3 行 P2 的 characterization test 从未写 | 订正归属注释；补写 `internal/broker/cutover_render_test.go`（含三条 pre-render 守卫的表） |
| §3.4 | **OVERSTATED** — decluster 幂等对在**生产形状**下抓不到守卫被删 | 换成**真实 seeded DB**，守卫删除现在能走到三条文件系统断言（变异验证：`.bak` 出现、conf 被重写、mtime 移动全部触发）；"缺 DB 也要 no-op"拆成独立测试，不再兼任探测器 |

**非空性地板审计**：九条地板全部合规，无一违反 G2b。

**A4 类（注释里写了不存在的标识符）**：三条全部订正——发明的 `unreadableOutboxNames`、
`fence_dimension_coupling_test.go` 头部对姊妹文件的过期描述、以及同一集合的三个互相矛盾的公开数字
（165 / 158 / 0 ⇒ 统一为**唯一可复现**的 158）。补记：A4 门在本轮**抓到了我自己**新写的一处笔误
（注释写 `RoutePort` 拼成 `Routeport`），这正是它存在的理由。

**完整性/反拖延审计**：56 条原子义务判 **DONE 38 / PARTIAL 4 / NOT DONE 11 / DECLINED 2**。
除 `B9 一 lane 一 commit`（流程上必须在外审之后，已在 §8 如实标注）外，
其余 PARTIAL 与 NOT DONE **全部在本轮补齐**，逐条证据见 §8 带 ✔核 的行。
两条 DECLINED（N0、N1）审计认定"有证据支撑，不是'这个很难'"，维持。

---

## 10. 外审判定（2026-07-27，`docs/reviews/batch-b2-external-review.md`）

**结论 FAIL**：5 MAJOR + 2 MINOR + 1 个 deploy-tier INCOMPLETE，每条都带**独立构造的反例测试**。
全部处置见外审报告内的原位回复；此处只记对本 plan 的影响。

**它抓到了内审两个审计都没抓到的一类东西**：内审的 completeness audit 问"承诺的东西做了没有"，
mutation audit 问"守卫真的有牙吗"，两者都**以本批的意图为前提**。外审不接受这个前提，
于是抓到三条"做了、但做出来的东西是错的"：

| finding | 类别 | 为什么内审看不见 |
|---|---|---|
| B2-1 takeover 仍用 key-presence | **修了一个、漏了它的副本** | 内审核对的是"HasJetStream 是否 value-aware"（是），没有去找**同一判据的内联复制** |
| B2-3 恢复指引不可执行且会毁数据 | **产品告诉运维怎么做，而那个做法是错的** | 内审验的是守卫行为，没人**照着日志文字实际操作一遍** |
| B2-4 webhook 把投递成功当活性 | **内审自己的修法引入的** | 它是内审 B7-01 的产物；同一轮审查不会推翻自己刚下的结论 |

B2-4 尤其值得记：**内审 B7-01 与外审 B2-4 的结论正好相反，而两者都对**——
一个整数被问了两个问题。这类缺陷只能由**不共享前提的第二双眼睛**发现。

**对 §7 / §8 的影响**：
- §8 的 B5 行（"5 装配点只提供真正不同的意图"）保持 YES，但**唯一性门本身被外审证明有洞**
  （B2-6：带类型参数的 helper 完全不可见）。门已加强为参数/返回值/receiver 感知，
  并因此暴露出包内两处合法的管线变更者，规则由"绝对唯一"精确化为"包外零容忍 + 包内白名单带理由"。
  **原状态下"只有一个装配点"只对扫描器恰好能匹配的形状成立**——这句话本该出现在 §8 里，现在补上。
- §8 的 B7 per-pass 行不变；B2-4 让 `alert-webhook` 的 `iterations` 语义**回到**共享契约，
  并新增 `RuntimeReport.AlertWebhook` 三计数器。
- 新增一次 **schema bump**（cluster status v1→v2），这是本批唯一的破坏性 wire 变更，
  契约写在 `adminsock.ClusterStatusSchemaVersion` 与 `docs/cluster.md`。
- `Config.Account` 删除完成——§8 曾随 `JSDomain` 一并声称删了，实际只删了一个。
  **"声称删除不等于删除"已作为教训写进两处注释。**

### 10.1 外审复核轮（同日，FAIL：2 MAJOR + 2 MINOR）

复核轮抓到的东西比第一轮更值得记，因为**它证伪的是我自己的驳回**。

**RB2-1 推翻了我上一轮唯一一条"驳回并附证据"的裁决。** 我当时拒绝把
`IsStandaloneJetStream`/`IsClusteredJetStream` 一起改，理由是那会让 `jetstream: false` + `cluster{}`
脱不了簇。复核指出：**那个形状本来就脱不了簇**——key presence 让它进 de-cluster 分支，
然后被要求一个被禁用的 JetStream 根本没有的 `store_dir`。我描述的是"从一个从未工作过的状态退化"。
第二条论据同样错：只有把拓扑折进 `HasJetStream` 才会出现零路由 clustered，而那不是它要求的。

**教训（写进 §7 的判断纪律，不是写进 §8 的账本）**：我把"把两个事实分开"读成了"合并成一个判据"，
然后去反驳那个合并。**驳回一条 finding 时，先确认自己反驳的是它写的东西，而不是自己重述的版本**——
并且要**实测**被驳回路径当前的行为，而不是从代码结构推断它"本来是对的"。
这次实测只花了一个探针测试，就翻掉了整条论据。

其余三条（RB2-2 泄漏门控制流三洞、RB2-3 Config 门别名与整文件豁免、RB2-4 撕裂快照）全部采纳实修，
并连带修了三处它未点名的同类问题；唯一未采纳的是 RB2-3 建议的 `go/types` 全解析（取舍已记录）。

**一个方法论收获**：全 tag `go vet` 抓到了 `make test` 看不见的两处遗漏引用
（`test/d9` 受 build tag 门控）。改动跨包 API 时，`make test` 绿**不代表**树是完整的——
tag 门控的套件只有 `make e2e-parallel` 或显式 `-tags` 才会编译。

### 10.2 发布后技术债清理（同日，`0e9c5d5` 之后）

外审 Pass 之后清掉两笔在报告里已声明的技术债。**两笔都不是"补做漏掉的活"，而是修掉两个
结构性错配**；每笔都做了双向变异验证。

**债 1 — 观测预算无视 `OBJ_xfer-*` 流（外审复核轮疑问 3）**

原先按 `COUNT(*) FROM sessions WHERE state='ACTIVE'` 定标。这个项**对 history 流是精确的**
（`ListSIDs` 就是这条查询），但把 xfer 流算成了**零** —— 而 `ObserveReplicas` 经 `ListXferStreams`
枚举每一个，且它们可以比创建它的 session 活得久（孤儿桶）。于是 transfer 密集的 broker
拿到的是"只够一小部分往返"的 deadline，"routinely unobserved" 是可预期的结果。

改为按**上一轮观测自己的流数**定标（`events + history + xfer` 全含，取得零成本），
首轮回退到 `活跃session + 1`。常量 `PerSession` 改名 `PerStream` —— 旧名字正是错配藏身之处：
**项看起来对，是因为它乘的那个量被叫成了同一个名字**。
新增 `TestObserveBudgetCountsXferStreamsNotJustSessions`，含一条容易被忽略的性质：
**UNOBSERVED 的报告不得把已缓存的流数覆盖成 0** —— 否则一次失败观测会缩小下一次的 deadline，
在集群已经吃力时形成加速失败的反馈环。变异（不记录流数）验证必红。

**仍未刻画的部分照旧声明**：每流 250ms 这个系数需要一个实测的 transfer 密集车队。
编一个数字出来正是本批一直在删的那种"看起来像刻画"的产物。**这次修的是结构性错配，不是常量。**

**债 2 — 行号锚定的豁免表（历史 11 次漂移）**

`unresolvedCodeSites` 的 51 条 key 从 `file:line` 改为 **`file:FUNCTION#序号`**，
序号是该站点在**所属函数内未解析站点**中的 1-based 源序。三条轴上都成立：
*站点精确*（`HandleCluster` 一个函数就有 10 个站点，函数级 key 会退化成旧的文件级黑盖）、
*免疫漂移*（函数上方的任何插入都动不了它 —— 11 次漂移的成因**全部**是"上方加注释"）、
*诚实*（同函数内增删未解析站点时它**会**失效，而那正是周围豁免该被重读的时刻）。
只数未解析站点，所以同函数加一个可解析字面量不影响任何 key。

> **中途改了方案，理由记下**：先做的是"把豁免搬到站点写成 `// unresolved:` 标记"，
> 已改完 `internal/agent/run.go` 4 处后发现这张表里除 51 条短理由外还夹着**几段长的共享论证**
> （`admit()` 那段 30 行记录了两次被驳回的错误声称），分散到 6 个站点要么复制 6 份要么丢掉。
> 已回退那 4 处，改用序号 key —— 它对**实测到的**漂移成因完全免疫，且共享论证留在原处不受损。
>
> **两个方向都做了变异验证**：在 `clusterstatus.go` 每个站点上方插 5 行 ⇒ 三门**全绿**
> （旧 key 在同样操作下 12 条全废）；在 `HandleCluster` 内插一个**新的**未解析站点 ⇒ **变红**。
> 顺带修掉 3 处理由文本里**自己按行号交叉引用**的地方（`…same value as :169`），
> 改为"本函数内 #N"。

**并且发现了同一疾病的第三张表**：`internal/auth` 的 `dynamicSubscriptionExemptions`
也按 `internal/broker/broker.go:1034/1036` 锚定 —— 而它是被**债 1 给 `broker.go` 加的那个字段**
撑长后触发失效的。刚修完一张就留下它的孪生，正是外审两次抓到我的那个模式
（B2-1 的判据副本、RB2-4 的自检副本），所以一并改成同样的 `file:FUNCTION#序号` 并同样验证免疫。
这条本身是个教训：**同一个坏形状在仓库里往往不止一处，修的时候要去找它的兄弟。**

**仍未闭合的一项，如实登记**：simcluster `41-shrink-to-standalone` 保持 INCOMPLETE。
两个 GAP 是 agent 在非终态、被退役 broker 仍在网格中的 retire 上不走主动快路径，
**先于本批存在**（B5–B9 不碰 agent 重连路径；我与外审两轮 drill 给出逐字相同的两个 GAP）。
不请求豁免、不为让 drill 变绿去改产品——按 simcluster 定位铁律保持 recorded，
留给独立的 agent-rehome 增量。
