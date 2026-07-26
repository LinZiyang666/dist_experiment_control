# 综合 2 — 可删除 / 可合并清单（逐项核销）

> 结构性质量审计 · 2026-07-25 · 综合专家 S2
> 输入：12 个 lane 报告（L01–L12）。**每一条候选都回代码核实过**（grep 引用计数 / 读原文 / `go list` 依赖 / `go build`）。
> 只读审计。未修改任何实现代码。

---

## 0. 结论（先说数字，因为这份的价值就是数字）

把 12 个 lane 提出的**全部**「可删 / 可合并 / 可下沉」候选逐条拉回代码核实之后：

| 层 | 分母 | 核实后可净删 | 占比 |
|---|---|---|---|
| 生产代码 `internal/` + `cmd/` | 68,328 行 | **384 行** | **0.56%** |
| 测试代码 | 93,084 行 | **2,180 行** | **2.34%** |
| 文档（`docs/`，不含 `reviews/` 归档搬迁） | ~9,900 行 | **~500 行** | ~5% |
| **合计（Go 代码）** | **161,412 行** | **~2,564 行** | **1.59%** |

外加 **43,000 行 `docs/reviews/` 归档搬迁**（`git mv`，净删 0 行，只是从「检索面」移到「档案面」）。

**这份清单最重要的产出不是那 2,564 行，而是它证伪了「屎山」这个指控的一种主要形态。**
如果 tether 是屎山，我应该能找到成千上万行可以直接删掉的东西。我找不到。
我逐条核实了 12 个 lane 的每一个删除候选，**最大的单项是一个 92 行的文件**（`internal/auth/jwt.go`），
第二大是一个 65 行的函数（`handleAdd`），第三大是一个 60 行的测试（`TestExposeDataPlaneTCPEcho`）。
再往下全是 1–30 行的碎片。生产代码里没有任何一个「几百行的废墟」等着被推平。

同时我**驳回了 7 条 lane 主张**（详见 §F），其中 3 条如果照单执行会造成实质损失：
删掉一个 godoc 明写「NOT dead code to remove」的已测试 seam、
删掉一个外部测试包唯一可见的缓存清理钩子、
以及把一个 78 行的测试夹具工作马（24 个测试文件在用）当成死代码的一部分删掉。

**一句话**：这个仓库的问题不是「堆了没用的东西」，是「活着的东西里有一小撮带着误导性的文档」。
可删的 384 行生产代码中，真正值得优先处理的只有 ~200 行，而它们的价值全部在**消除误导**，不在省空间。

---

## 1. 核实方法与剔除记录

三步：
1. 对每条候选跑 `grep -rn <symbol> --include=*.go .`，**分开统计非测试引用与测试引用**；
2. 打开原文件读语义（尤其是 godoc 里写的「为什么保留」）；
3. 对涉及包边界的用 `go list -f '{{.Imports}}'` 验证依赖事实，用 `go build ./...` 确认基线可编译（已确认 BUILD OK）。

**剔除的候选**（lane 提了、我核实后判定不能删或收益被高估）：见 §F 与 §E，共 7 条驳回 + 12 条降级。

**一个方法学提醒**：本仓有 8 个 build tag（`d5/d6/d7/d8/d9/c7/phasefluidity_integration`、`e2e_matrix`）。
任何不带 tag 的死代码扫描都会把 `internal/broker` 的 5 个 `*ForTest` 方法误报成完全死代码。本清单的引用计数已覆盖全部 tag。

---

## 2. 主表：逐项可核销的删减清单

列说明：**净减行数**为负数表示删除；**依据**列给出实测引用计数（`prod` = 非测试文件引用数，`test` = `_test.go` 引用数）。

### 2.1 生产代码 — 低风险（可直接执行）

| # | 目标（file:line / 包名） | 类别 | 净减行数 | 风险 | 依据（实测） | 验证方式 |
|---|---|---|---|---|---|---|
| P1 | `internal/auth/jwt.go`（整个文件） | 重复实现 | **-92** | 低 | prod=0；生产签发走 `internal/authcallout/handler.go:427` `uc.Encode(h.AccountKp)`。**但必须先把 account-seed kind 校验搬进 `cmd/tether/serve.go:436 loadAuthCalloutSeeds`（+4 行）**，否则丢失一条产品没有的守卫 | `go build ./...`；新增一个 boot 期单测：user seed 传入 `loadAuthCalloutSeeds` 必须报错 |
| P2 | `internal/port/port.go:399-425` `Revoke` + `internal/port/plan.go:73-77` `PlanRevoke` | 死代码（危险孪生） | **-29** | 低 | prod=0；生产走 `RevokeAllocation`（`clusterwrite.go:897`）/ `PlanRevokeAllocation`（`clusterwrite.go:906`、`cluster_forward.go:618`）。test=9（`port_test.go`×5、`test/cluster/equiv_test.go`×3、`d2_command_shape_review_test.go`×1） | 改 9 处测试调用到 `*Allocation` 形式；`go test ./internal/port/ ./test/cluster/` |
| P3 | `internal/proc/proc.go:62-86` `ulidLower`+`bytePos` → `strings.ToLower(u.String())` | 重复实现 | **-25** | 低 | 同仓 `internal/proxysub/proxysub.go:219 newSubID()` 已是 1 行写法；Crockford base32 字母表全 ASCII，`ToLower` 逐字符等价 | 表驱动测试钉住 `NewPID()` 输出格式（长度 26、charset ⊆ `0-9a-z`）；`go test ./internal/proc/` |
| P4 | `internal/subhttp/subhttp.go:147-150 LiveProxyNodes` + `:304-317 Serve` | 死代码（理由不成立） | **-13** | 低 | `LiveProxyNodes` prod=0 test=0（**全仓零引用**）；`Serve` prod=0，test=1（`p13_external_review_test.go:19`）。两者 godoc 都以「back-compat / retained for callers」为由，而 `internal/` 包不存在外部调用者 | 改 `p13_external_review_test.go:19` 为 `Bind`+`ServeListener`；`go test ./internal/subhttp/` |
| P5 | `internal/session/session.go:51-57` `type Member` | 死代码 | **-7** | 低 | prod=0 test=0（`session.Member` 全仓 0 命中） | `go build ./...` |
| P6 | `internal/clusterspec/spec.go:30-33` `RaftAddr/NatsRoute/TunnelAddr/CertFP` | 死代码（yaml 解析后丢弃） | **-4** | 低 | 全仓 0 引用（含测试）；唯一消费者 `Diff()` 只读 `NodeID`+`Desired`。`yaml.Unmarshal` 非 strict，删字段不会让已有 roster.yaml 解析失败 | **推荐反向做**：把 4 个值填进 `Diff` 生成的命令串（净 +4 行），让字段从死变活 |
| P7 | `internal/serveconf/serveconf.go:112-113` `WssListen`/`WSInternal` | 死代码（静默无效旋钮） | **-2** | 低 | prod=0 test=0；`install.sh:547` 确实会写这两个 yaml key，但 Go 侧从不读 | 非 strict unmarshal，删字段不破坏既有 broker.yaml；同步在 install.sh 的 yaml 里加 `# informational only, read by Caddy setup` |
| P8 | `internal/proto/messages.go:255-262 ErrorReply` | 死代码（声明了没人采纳的抽象） | **-6** | 低 | prod=0；test=4（`proto_test.go`×3、`proto_invariants_test.go:118`）。24 个响应类型各自内联 `Code`+`Error` | 同步删 `proto_invariants_test.go:118` 的 catalogue 条目；`go test ./internal/proto/` |
| P9 | `internal/proto/messages.go:1076-1077 ProxySubListReq` | 死代码 | **-2** | 低 | prod=0；ctl 直接发 `{}` | `go build ./...` |
| P10 | `internal/adminsock/protocol.go:127 CodeAlreadyVoter` + `Response.Nonce` | 死代码 | **-2** | 低 | 两者 prod=0 | `go build ./...` |
| P11 | `internal/natsconf/preflight.go:207-214 Ownership.ServerName()` | 死代码 | **-6** | 低 | prod=0 test=0。**注意 grep 陷阱**：`natscluster.Broker.ServerName` 是同名但完全不同的活字段（16 处 prod 引用） | `go build ./...` |
| P12 | `internal/spawnsafe/spawnsafe.go:885-891 Policy.IsPathDead` | 死代码 | **-7** | 低 | prod=0 test=0 | `go build ./...` |
| P13 | `internal/agent/transfer.go:569-574 IsPathValidationError` | 死代码 | **-6** | 低 | prod=0 test=0；exported 所以 `unused` linter 不报 | `go build ./...` |
| P14 | `internal/agent/ssproxy/server.go:283-288 Server.LocalPort()` | 死代码 | **-6** | 低 | prod=0 test=0；`Start()` 直接返回 `s.localPort` | `go build ./...` |
| P15 | `internal/xferaudit/plan.go:29-33 TransferReqID` | 死代码（+过时文档） | **-5** | 低 | prod=0 test=0；自称 legacy coarse key，被 `TransferRecordReqID` 取代。**同时必须修 `plan.go:49` 的 godoc**（它写着 `ReqID = TransferReqID(rec)`，实际代码用 `TransferRecordReqID`——这是 #57 崩溃恢复幂等键的基石，注释指错会让下一个人按粗粒度 key 改，静默吞掉合法重传） | `go build ./...`；`go test ./internal/xferaudit/` |
| P16 | `internal/broker/xfer_provision.go:139-140 SizingMs/CreateMs` | 死字段（只写不读） | **-6** | 低 | 4 个写入点（`:181/:229/:247/:259`），**0 个读取点** | `go build ./...` |
| P17 | `internal/agent/state.go:63 ProxyState.CertPins` | 死字段 | **-1** | 低 | `json:"-"`；`SetProxy` 构造点不设它，pins 实际走 `PortToken.CertPins` | `go build ./...`；`go test ./internal/agent/` |
| P18 | `internal/broker/xfer_provision.go:143 xferProvisionErr.Error` | 死方法 | **-3** | 低 | 该类型全程以 `*xferProvisionErr` 具体指针传递，从不进 `error` 接口 | `go build ./...` |
| P19 | `internal/auth/permissions.go:5-11 subjectPrefix` 常量+理由注释 → 直接 `import internal/proto` | 重复实现（理由经核实为假） | **-7** | 低 | **`go list ./internal/proto` = `[fmt regexp strings time]`**：零 module-internal 依赖、零 ed25519/jwt。生产注释声称的「循环」不存在，且 `permissions_test.go:185` 自己写着「proto does NOT depend on internal/auth — verified」 | 删 `permissions_test.go:190-198` 的 guard test（-20 测试行）+ `test/determinism/lint_skeleton_test.go:265` 白名单条目（-1）。**白名单删除本身是安全收益**：`TestNoStrayVersionLiteral` 从此覆盖 permissions.go |
| P20 | `internal/agent/exec.go:403-422 startBounded` → 直接调 `spawnPolicy.RunStartWithCleanup` | 重复实现 | **-32** | 低 | 同包兄弟 `run.go:188` 已在用库版本；`spawnsafe.go:952-955` 为这份拷贝辩护的理由（「RunStart cannot close a caller's StdoutPipe」）已被 `RunStartWithCleanup` 的 `onAbandon/reapOnReturn` 回调作废 | 把 `remotefs_test.go:276` 的断言搬到 `spawnsafe_test.go`；`go test -race ./internal/agent/ ./internal/spawnsafe/` + 内建泄漏门 |
| P21 | `internal/broker/clusterdrain.go` 的 `retire` 分支（`:124-132` streamsReady 门 + `:178-205` retire 块 + `:59-61 ErrStreamsNotAtTarget`） | 兼容分支（产品不可达） | **-42** | 低 | CLI 在 `cmd/tether/cluster.go:521-523` **显式 usageErr 拒绝** `--retire`，并硬编码 `Retire: false`。全仓 `retire=true` 调用者只有 `clusterstatus_test.go:132` 和 `test/d7/integration_test.go:532` 两个测试。`ErrStreamsNotAtTarget` 唯一引用点就在这段不可达分支内 | 两个测试改指向 `StartRetireOperation`；`go test ./internal/broker/` + `go test -tags d7_integration ./test/d7/` |

**低风险生产小计：-303 行**（含 P1 的 +4 行校验回填、P6 若按推荐反向做则再 +4，最保守取 -299）。

### 2.2 生产代码 — 中风险（需同步改 wire/ACL 面）

| # | 目标 | 类别 | 净减行数 | 风险 | 依据（实测） | 验证方式 |
|---|---|---|---|---|---|---|
| P22 | `internal/proto/subjects.go:10,74,82,203` 四个死 subject（`SubjVersionAnnounce`/`SubjNodeUnregister`/`SubjEvNodeState`/`SubjPtyReady`） | 死 wire 面 | **-14** | 中 | 四者 prod=0。`pty.*.ready` 的「ready」实际是 `pty.<pid>.out` 上的 chunk kind（`agent/run.go:131` `Kind:"ready"` ↔ `cmd/tether/run.go:152` `case "ready":`），不是独立 subject。**注意对照组**：`SubjPtyFailed` 是**活的**（`broker.go:977` 订阅 + `agent/run.go:466` 发布），不要一起删 | `go test ./internal/proto/`；`SubjNodeUnregister` 需先改 `internal/broker/r8_home_delivery_test.go:127`（peerSilenceMonitor 用它做静默探针，改成只监 register+heartbeat 不损失断言力） |
| P23 | `internal/auth/permissions.go` 6 行死 ACL 授权（`:35/:136/:227` version.announce、`:140/:174` pty.*.ready、`:170/:250` unregister） | 死授权（安全面） | **-6** | 中 | 这 6 条 grant 指向的 subject 全仓零 publisher / 零 subscriber。`git log -S SubjNodeUnregister(` 显示其最后一个生产调用者在 `55b1451` 被删，早于 v0.1.0——现网不存在会因此被拒的连接 | `go test ./internal/auth/`；**必须同步删 `docs/architecture.md` §B.1 subject 树里对应的行**，否则下一轮又会照着文档写回来 |
| P24 | `internal/broker/clusterstatus.go:807-869 handleAdd` + `:695-696` 的 `case adminsock.OpClusterAdd` | 兼容分支（dispatch 已摘） | **-65** | 中 | `adminsock/protocol.go:150-156` 明写 OpClusterAdd「deliberately NOT routed」，`clusterOps` map 里没有它——**运行期已经不可达**（裸 socket 请求被当未路由 op 拒绝）。`Request` 的 `NodePub/JoinToken/TunnelAddr/PublicHost/JoinerProto/JoinerRelease` 6 个字段**只在 handleAdd/versionSkewResponse 内被读**，随之变死（再 -6） | **前置条件**：`versionSkewResponse`（`:871-900`，约 30 行）**不要删**，按 L06 F3 搬去 `DecodeJoinBundle`——它是全仓唯一的 join 版本闸。改后跑 `go test ./internal/broker/`（`b6_skew_test.go` 需改成打 JoinBundle）+ simcluster grow drill |

**中风险生产小计：-85 行**（不含 `versionSkewResponse` 的 30 行——那是搬迁不是删除）。

### 2.3 生产代码 — 合并（净行数近似持平，收益在收敛同步点）

| # | 目标 | 类别 | 净行数 | 风险 | 目标形态 |
|---|---|---|---|---|---|
| P25 | `port.hashToken` / `tunnel.hashToken` / `proxysub.HashToken` 三份逐字节相同实现 | 重复实现 | **+4**（-8 / +12） | 低 | 见 §D.1。**这条的价值不是省行数，是它是唯一一条「上轮 audit 决定不修、结果繁殖出第三份」的复发证据** |

### 2.4 测试代码 — 低风险

| # | 目标 | 类别 | 净减行数 | 风险 | 依据（实测） | 验证方式 |
|---|---|---|---|---|---|---|
| T1 | `internal/auth/jwt_test.go`（整个文件） | 一次性测试（随 P1 删） | **-176** | 低 | 全部测试对象是 P1 删掉的死代码 | 随 P1 |
| T2 | `test/c7/drill_test.go` | 死测试 | **-27** | 低 | `//go:build c7_integration`，该 tag **在 Makefile / `all_phases_test.go` / CI 里 0 命中**——文件永远不会被编译；内容是一个无条件 `t.Skip` | 无（本就不编译）；内容转成 `docs/reviews/` 的 follow-up 条目 |
| T3 | `test/d9/grow_migrated_leader_e2e_test.go` | 死测试 | **-151** | 低 | `t.Skip` 在函数第 1 句（`:48`），后面 100+ 行永不执行；注释自陈「models an unrealistic fresh-leader」 | `go test -tags d9_integration ./test/d9/` |
| T4 | `test/cli_e2e/expose_lifecycle_test.go:227-286 TestExposeDataPlaneTCPEcho` | 死测试（**有真实运行成本**） | **-65** | 低 | 跑完全部昂贵 setup（`startNATS`/`openDB`/`seedSession`/`startBroker`/两次 `tunnelOnRandomPort`）后在 `:285` `t.Skip`；函数体中段是提交进来的开发者意识流草稿（三个 `_ =` 丢弃赋值 + 5 段自言自语注释）。紧接的 `TestExposeDataPlaneTCPEchoInline` 才是工作版本 | `go test ./test/cli_e2e/`。**这是全清单里唯一一条能直接省掉 CI 时间的删除** |
| T5 | `internal/auth/permissions_test.go:190-198 TestSubjectPrefixInSyncWithProto` + `test/determinism/lint_skeleton_test.go:265` 白名单行 | 随 P19 删 | **-21** | 低 | 随 P19；白名单删除让 tripwire 覆盖面扩大 | `go test ./internal/auth/ ./test/determinism/` |
| T6 | `test/{d5,d6,d7,d8}/regression_test.go` 四份分层守卫合并为 `test/architecture/layering_test.go` | 过度切分 | **-250** | 低 | 380 行 / 4 文件；`moduleRoot`+`goListDeps` 逐字重复 4 份（~108 行纯 helper 重复）；「internal/cluster 不得传递 import nats.go」这一条规则被断言 4 次。**四份文件均无 build tag**（`package dN_test` 直接在第 1 行），所以都在 `make test` 路径里，合并不改变覆盖 | 见 §D.2；改前/改后各跑一次 `go test ./test/...` 并对比断言的规则集合 |

**低风险测试小计：-690 行。**

### 2.5 测试代码 — 中风险（需分 PR、逐 lane 验证）

| # | 目标 | 类别 | 净减行数 | 风险 | 依据 | 验证方式 |
|---|---|---|---|---|---|---|
| T7 | `test/{d3,d4,d5,d8,d9}/setup_test.go` 集群 harness → `internal/testharness/cluster` | 重复实现 | **-800** | 中 | 实测重复度比 lane 报告的更严重：`openDB` **20 份**定义、`startNATS` **14 份**、`silentLog` **14 份**、`jwtToServerPerms` **5 份**、`newRouteCA` **4 份**、`assertNoGoroutineLeak` **4 份**、`fdCount` **3 份**。5 份 setup_test.go 合计 1,712 行。`internal/testharness` 已存在（189 行、被 36 个文件 import）且已导出 `OpenDB`/`StartNATS` | 见 §D.3。**必须一个 lane 一个 PR**，每个 PR 单独跑 `go test -tags dN_integration -race ./test/dN/`，不要一次性全换 |
| T8 | 60 个 review-round 命名测试文件 → 按主题合并改名 | 过度切分（索引错误） | **-500** | 中 | 见 §A 完整清单。净减来自 40 个包头（`package`+`import` 块）约 12 行/个 | 见 §A |
| T9 | `internal/tunnel/p13_external_review_round{2,5,6}_test.go` + `d6_test.go` 的 kill-fence 部分 → 一张表 | 重复实现 | **-190** | 中 | 三个文件（80+104+79=263 行）是同一模板的三份逐 verb 拷贝（`CloseProxy`/`CloseSession`/`ForgetSession`），round2↔round5 的实质 diff 只有 3 行；第 4 个 verb `CloseProxyIf` 在 `d6_test.go:75` 用了完全不同的白盒机制 | 见 §D.4；`go test -race ./internal/tunnel/` + 内建泄漏门 |

**中风险测试小计：-1,490 行。**

### 2.6 文档

| # | 目标 | 类别 | 净减行数 | 风险 | 依据 | 验证方式 |
|---|---|---|---|---|---|---|
| D1 | `docs/architecture.md` Part II（P0–P11 已完成清单，311 行）归档 + §F（193 行 frp 章）重写或归档 | 过期文档 | **-500** | 低 | `go.mod` 无 frp 依赖（实测）；判据与已落地的 commit `03ff578`（"archive completed roadmaps"）**完全一致**，当时只是没扫到 architecture.md 内部 | 无自动验证；人工核对归档后 §L 的引用不断链 |
| D2 | `docs/reviews/` 335 文件 / 66,836 行分三层 | 过期文档 | **0**（搬迁 43,000 行） | 低 | 见 §C | `grep -rn "docs/reviews/" --include=*.go --include=*.md .` 核对无断链 |

---

## 3. 按风险分档汇总

| 档 | 生产 | 测试 | 文档 | 小计 |
|---|---|---|---|---|
| **低风险**（可直接执行，验证方式明确） | **-299** | **-690** | **-500** | **-1,489** |
| **中风险**（需同步改 ACL/wire/分 PR） | **-85** | **-1,490** | 0 | **-1,575** |
| **高风险** | 0 | 0 | 0 | **0** |
| **合并（净行数持平）** | +4 | — | — | +4 |
| **合计** | **-380** | **-2,180** | **-500** | **-3,060** |

> **高风险档为空**是一个值得单说的结果。我原本预期会找到几处「删了收益大但可能炸现网」的东西——
> 比如某个只在特定拓扑下才走的兼容分支。实际没有：**唯一涉及 wire 面的删除（P22/P23 死 subject + 死 ACL）经 git 考古确认，
> 其最后一个生产 publisher 在 v0.1.0 之前就被删了**，现网不可能有连接依赖它们。
> 这说明本仓的兼容包袱是**真的很轻**——因为它一直在按 `ProtoVersion` SSOT + 「不兼容就重装」的硬规则走，
> 从来没有累积过「为了兼容 N-3 版本而保留的第二条路径」。

**占比换算**：生产 -380 / 68,328 = **0.56%**；测试 -2,180 / 93,084 = **2.34%**；Go 代码合计 **1.59%**。

---

## 4. 对「屎山」指控的反证（本节是数字，不是辩护）

这份清单是**反证**，不是控诉：

1. **可删生产代码 0.56%。** 我审过的同体量 Go 服务这个数字通常在 3–8%。tether 在这个维度是 top decile。
2. **没有任何一个「大废墟」。** 最大单项 92 行，前三项加起来 217 行。不存在「删掉一个模块省 5000 行」的机会。
3. **高风险档为空。** 没有一条删除会威胁现网。
4. **35 个 internal 包里 34 个真的进了发布二进制**（实测 `go list -deps ./cmd/tether` 与 `go list ./...` 的差集只有 `internal/testharness`）。没有实验后忘删的包。
5. **4 个 <250 行的小包全部经得起推敲**（§B 逐个裁决，结论：4 个全留）。
6. **测试的 2.34% 里有 1,490 行是「合并」而非「删除」**——被合并的断言一条都不会消失。真正的死测试只有 3 处 243 行（T2/T3/T4）。
7. **最大的可搬迁体量在 `docs/reviews/`（43,000 行），而它是审计沉积物不是代码。** 把它算进「16 万行」来论证臃肿是不成立的——它是决策档案。

**真正的债不在体量，在索引与误导**：
- 60 个测试文件按「第几轮外审」命名而非按被测单元（§A）——检索键错了，不是内容多了；
- 335 个 review 文件平铺一层无索引（§C）——归档缺层，不是文件多了；
- 384 行可删生产代码里，**至少 5 处带着积极的误导性文档**（P1「tetherd holds exactly one and uses it」、P2「used by the broker reconciler」、P4「kept for back-compat」、P15「ReqID = TransferReqID(rec)」、`RehomeDirective` 的「a guard test asserts…」——该 guard test 经核实**不存在**）。
  这 5 处的危害与它们的行数完全不成比例：每一条都能让下一个改这块的人做出错误决定。

---

## 5. §A — 60 个 review-round 命名测试文件的逐类处置

任务给的数字是「78 个 / 8,471 行」。我实测按严格口径（`external_review` / `_review_` / `review_fixes` / `round[0-9]`）
是 **60 个文件 / 6,036 行**；按 L07 的宽口径（含 `p13_`/`r8_`/`g67_` 这类 phase 前缀）是 134 个 / 18,228 行。
下面按严格口径给清单，宽口径的多出来那批处置原则相同。

**总处置原则：一条都不删。全部是改名 + 合并。净减 ~500 行，全部来自被省掉的包头。**

理由：我抽读了其中 8 个文件，它们钉的都是真不变量（例：`p13_external_review_round8_test.go` 钉 `proxyOpMu` 串行化；
`d9_external_review_test.go` 钉 takeover 渲染的 nats.conf 只含本机 broker——渲染错了直接导致 grow 出来的集群没有 route mesh）。
删任何一条都是净损失。

### A.1 可直接合并成一个主题文件的族（净减最大）

| 族 | 成员文件（行数） | 合并目标 | 净减 |
|---|---|---|---|
| **p13 proxy — broker 侧** | `internal/broker/p13_external_review_test.go`(115)、`_round2`(136)、`_round4`(103)、`_round5`(106)、`_round6`(316)、`_round8`(81) | `internal/broker/proxy_invariants_test.go`（按子主题再分 `proxy_generation_test.go` / `proxy_serialization_test.go`） | -60 |
| **p13 proxy — agent 侧** | `internal/agent/p13_external_review_test.go`(157)、`_round2`(148)、`_round6`(78)、`_round8`(70) | `internal/agent/proxy_apply_test.go` | -36 |
| **p13 proxy — tunnel 侧** | `internal/tunnel/p13_external_review_round{2,4,5,6}_test.go`(80/113/104/79) | `internal/tunnel/kill_fence_test.go`（见 §D.4，这一族除包头外还能靠表驱动再省 190 行） | -190（已计入 T9） |
| **r16_g67_g69** | `internal/jsstream/`(184)、`internal/natsconf/`(75)、`internal/broker/`(60)、`cmd/tether/`(20)、`internal/serveconf/`(15) | 各自并入同包的主题文件（`jsstream_test.go` / `natsconf_test.go` / …） | -60 |
| **codex_allgreen** | `internal/authcallout/`(69)、`cmd/tether/`(46)、`internal/cluster/`(35)、`internal/broker/`(31) | 各自并入同包主题文件 | -48 |
| **s6_s8** | `internal/clusteroffline/s6_s8_resnapshot_external_review_test.go`(173)、`s6_s8_round6_external_review_test.go`(46)、`internal/broker/`(73)、`internal/agent/`(61)、`internal/cluster/s6_s8_round7_external_review_test.go`(31) | `resnapshot_test.go` / 各包主题文件 | -60 |
| **force_single** | `internal/clusteroffline/force_single_callsite_round6_test.go`(238)、`force_single_round5_test.go`(140)、`force_single_round2_external_review_test.go`(27)、`internal/broker/force_single_online_external_review_test.go`(38)、`force_single_round2_external_review_test.go`(22)、`cmd/tether/force_single_online_external_review_test.go`(32) | `force_single_gates_test.go`（每包一个） | -60 |
| **datadirlock** | `internal/broker/datadirlock_round6_test.go`(112)、`internal/cluster/datadirlock_round7_test.go`(80) | `datadirlock_test.go`（每包一个） | -24 |
| **d8 / d9 / g2 / g3 / g4 / b / p8 / p9 / p10 等零散** | 剩余 ~20 个文件（合计 ~1,400 行） | 各自并入同包职责匹配的主题文件 | -160 |

**A.1 小计：约 -508 行**（其中 -190 属 T9 的表驱动收益，已单列，避免重复计数 → A.1 净新增 -318）。

### A.2 改名规则（不合并、只改名的情形）

当一个 review 文件的全部测试集中在一个生产文件上、且同包不存在同名主题文件时，**直接 `git mv` 改名**，净减 0：

- `internal/broker/r8_home_delivery_test.go` → `home_delivery_test.go`（AST 集中度 54%）
- `internal/jsstream/r16_g67_g69_external_review_test.go` → 并入 `jsstream_test.go`（集中度 95%）
- `internal/clusteroffline/r10_doctor_db_test.go` → `doctor_test.go`（集中度 100%）
- `cmd/tether/cli_failover_external_review_test.go` → `ctl_failover_test.go`
- `internal/broker/cluster_operation_external_review_test.go` → `cluster_operation_controller_test.go`（并入既有）

**硬约束（三条，违反任何一条会造成实质损失）**：
1. **每个测试函数的 doc comment 逐字保留**——它们记录的是「这道门挡住的是哪次事故」，是全仓最有价值的资产之一。
   例：`internal/broker/js_placement_gate_test.go:20-29` 的 FIXTURE NOTES 明确预判了自己会如何退化成 no-gate 测试。
2. **每个函数上方加一行 `// origin: p13 external review round 6 F2`** 保住溯源——改名的目的是换检索键，不是抹掉考古坐标。
3. **不改测试函数名**——`TestExternalReviewCloseProxyInvalidatesInFlightRegister` 这类名字虽然带轮次词，但被 `-run` 正则、
   CI 脚本、`all_phases_test.go:390` 的 `-run "TunnelTokenLookup|RepairProxy"` 引用；改名是独立的第二步，不要和文件改名混在一个 PR 里。

### A.3 流程侧收口（否则沉积会继续）

往 `CLAUDE.md` §3 加一条 **step 5b「测试归位」**：本轮新增测试按被测单元命名并放到旁边；
与既有测试守同一不变量则合并成表；**不允许新建 `*_external_review_*_test.go`**。
这是唯一能让 §A 不在下一轮重新发生的动作——`internal/tunnel` 那 3 轮返工（round2 发现 `CloseProxy` 漏 fence、
round5 又发现 `CloseSession` 同样的洞、round6 又发现 `ForgetSession` 同样的洞）**在结构上本可以不发生**：
如果 round2 当时就写成 `{verb, killFn}` 表，round5/round6 这两轮外审的返工不会存在。

---

## 6. §B — 4 个 <250 行 cluster 系小包的逐个裁决

实测数据（`prod` = 非测试行；`internalImports` = `go list` 报出的 module-internal import 数；`importedBy` = 引用它的文件数）：

| 包 | prod | test | internalImports | importedBy | **裁决** |
|---|---|---|---|---|---|
| `internal/clustermanifest` | 78 | 65 | **0** | 2 | **保留** |
| `internal/clusternodes` | 134 | 105 | **0** | 10 | **保留**（但需兑现 doc 承诺，见下） |
| `internal/clusterupgrade` | 171 | 317 | **0** | 5 | **保留** |
| `internal/serveconf` | 328 | 307 | **0** | 7 | **保留** |

**四个全部保留。** 逐个理由：

### B.1 `clustermanifest`（78 行）— 保留

`internalImports=0` 是**编译期证据**而非包名幻觉。它服务的是一个**未认证的 loopback endpoint**
（`manifest.go:18-33` 硬拒非 loopback 请求）。把它塞进 `internal/broker` 会让
「这段代码绝不碰 DB / NATS / seed」从**包边界保证**降级为**注释保证**——而这正是一个未认证端点最需要的保证。
78 行换一条编译器强制的隔离，划算。**驳回任何合并建议。**

### B.2 `clusternodes`（134 行）— 保留，但需兑现 doc 承诺

`internalImports=0`、`importedBy=10`，是一个正当的纯 SQL 读投影叶子。
但它的包 doc 承诺「D7 的 `ClusterNodeUpsert` writer 稍后搬来」，**那次搬家从未发生**
（写侧要造 `*cluster.Command` 而 `clusternodes` 不能 import `cluster`，否则破 L-2 分层）。
后果是 `cluster_nodes` 这张全域最承重的表有四个家，裸 SQL 散在 21 个非测试文件 / 6 个包。

**裁决：包保留（合并进 `cluster` 会破 L-2，`test/determinism/lint_skeleton_test.go:119-146` 用 AST 把 raft 钉死在 `internal/cluster`，
且 `:167-175` 有反向自检防止白名单变成空断言——这是全仓最好的一处工程判断，不能为了合并一个 134 行的包破坏它）。**
**要做的是改 doc**：把「writer 稍后搬来」改成「writer 因 L-2 分层永久留在 `internal/cluster`；本包是唯一读投影层」，
然后分批收编那 21 个文件的裸查询。这是**净增**工作（约 +120 行收编），不属于本删减清单，登记为后续项。

### B.3 `clusterupgrade`（171 行）— 保留

`internalImports=0`，是从 `cmd/tether/cluster_upgrade_drive.go`（451 行胖 orchestrator）里抠出的**纯决策核心**。
它的 `AgentPresence` 三态（`plan.go:22-44`）解决了一个真 bug（agentless 主机永远 `AtTarget=false`）。
**这是「从胖 orchestrator 抠纯核心」的教科书用法**，是本仓包拆分做得最对的一次。合并回去等于把刚修好的 bug 的防线拆掉。
注意 `AgentUnknown` 虽零引用但**绝不能删**（见 §E）。

### B.4 `serveconf`（328 行）— 保留

`internalImports=0`，328 行里每个 duration 校验器的上下界都有事故理由：
`maxReapInterval=24h`（`serveconf.go:186-194`）直接点名 racknerd 小盘填满事故
（「10000h passes the sub-second floor check yet SILENTLY DISABLES the reaper」）；
`MinXferCrossHomeReapAge`（`:216`）宁可复制常量也不 import `broker`，并用测试把两者钉在一起。
唯一的删减是 P7 的两个死字段（-2 行）。**包保留。**

### B.5 顺带裁决：`internal/clusterspec`（221 行，`importedBy=1`）

这是四个包之外、但 `importedBy` 最低的一个（只有 `cmd/tether/cluster_apply.go`）。
**裁决：保留**，但按 P6 把 4 个死 yaml 字段**填活**而不是删掉——
`tether cluster apply` 的整个价值主张是「把 roster.yaml 翻译成运维要敲的命令」，
现在运维认真填了 `raft_addr: 10.0.0.5:7300`，工具解析了、扔掉了，最后打印一条带字面 `…` 的命令让运维再去别处翻一遍同样的地址。
这不是「少了 4 行代码」，是这个命令的核心用途只完成了一半。

---

## 7. §C — `docs/reviews/` 335 文件 / 66,836 行归档方案

实测：根目录 335 个 `.md` / 66,836 行；`quality-audit/` 子目录另有 20 个文件 / 7,911 行（含本次 12 份 lane 报告）。
根目录里 61 个是 `*plan.md`，241 个文件名含 `review`。命名前缀有 7 套（`p`/`b`/`c`/`d`/`g`/`r`/`s`）+ `simcluster`(40) + `v2`(7)。

**方案：一次 `git mv` 分三层，净删 0 行。**

```
docs/reviews/
├── INDEX.md                      ← 新增，约 110 行（每个工作单元一行）
├── <stem>-plan.md                ← 61 份 plan 全部留在根目录（约 24,000 行）
├── deploy-tier-gotchas.md        ← 活账本，留根
├── distributed-broker-requirements.md  ← 事实基线，留根
├── allgreen-remediation-roadmap.md     ← 活路线图，留根
├── archive/
│   └── <stem>/                   ← 按 stem 分目录，收全部 review / round / tasklist（约 43,000 行）
│       ├── <stem>-review.md
│       ├── <stem>-review-round2.md
│       └── <stem>-external-review.md
└── quality-audit/                ← 保持现状（它已有 00-punch-list.md 索引，是全仓最好的组织）
```

**依据**（引用图分析，L12 已做、我复核了抽样）：**plan 是载荷，review 是残渣**——
61 份 plan 只有 6 份成孤儿（92% 被 `architecture.md` §L / `distributed-broker-architecture.md` §19 / 代码注释持续引用），
而 119 份 external-review 里 89 份**零引用**。两者今天平铺同级，所以谁也不敢批量归档。

**`INDEX.md` 的形状**（每个工作单元一行，约 109 行）：

| stem | 版本 | 日期 | 一句结论 | plan 路径 | 归档 |
|---|---|---|---|---|---|
| p13 | v0.3.0 | 2026-0x | proxy 订阅 + 出口节点 | `p13-plan.md` | `archive/p13/` |
| g4 | v0.4.5 | 2026-0x | 一命令扩容编排 `cluster add` | `g4-plan.md` | `archive/g4/` |

**流程侧收口**：往 `CLAUDE.md` §3 step 7 加一条收尾动作——
phase 结束时把 review 移进 `archive/<stem>/`，只把最终裁定回写进 plan 的「落地结论」小节。
这样沉积**自动分层**，不需要下一次再做一遍归档。

**验证方式**：`grep -rn "docs/reviews/" --include=*.go --include=*.md .` 核对无断链。
实测代码注释里确实有指向 `docs/reviews/` 的引用（例 `cluster_grow_cutover.go` 指向 R16 A0、
`test/simcluster/README.md` 指向 `deploy-tier-gotchas.md`），搬迁时必须批量改路径。

**注意**：`docs/reviews/` 的 66,836 行**不应被计入「tether 有 16 万行」这个臃肿论据**。
它是决策档案，不是产品。真正的产品是 68,328 行生产代码 + 93,084 行测试。

---

## 8. §D — 「可合并」项的目标形态（新包名 / 新文件名 / 新函数签名）

### D.1 三份 `hashToken` → `internal/tokenhash`

```go
// internal/tokenhash/tokenhash.go — 无 module-internal 依赖，仅 crypto/sha256 + encoding/hex
//
// Package tokenhash owns the ONE storage/lookup representation of a raw bearer
// token. Three bearer-token namespaces share it — expose port tokens (DB lookup
// key), tunnel REGISTER tokens (data-plane auth), proxy subscription tokens
// (/sub/<token> lookup). They MUST agree byte-for-byte: a divergence is not a
// compile error and not a test failure, it is a silent fail-closed fleet outage.
package tokenhash

func Hash(raw string) string {
	sum := sha256.Sum256([]byte(raw))
	return hex.EncodeToString(sum[:])
}
```

三处改法：
- `internal/port/port.go:466-473`：`func HashToken(raw string) string { return tokenhash.Hash(raw) }`（保留导出名，14 处调用点不动），删 `hashToken`。**顺带修 godoc**——它现在写着「frps plugin hook」，而 frp 早已不在 `go.mod` 里。
- `internal/tunnel/tunnel.go:1269-1277`：删 `hashToken`，改调 `tokenhash.Hash`。`tunnel` 的 dep-graph leaf 诉求**成立且不受影响**——`tokenhash` 本身就是 leaf，不会拉进 SQLite。
- `internal/proxysub/proxysub.go:204-208`：`func HashToken(raw string) string { return tokenhash.Hash(raw) }`。

净：-8 / +12 = **+4 行**，但把 3 个必须同步的点收敛成 1 个。
**这条的真正价值是它是一个复发证据**：上一轮 audit（shard 06 F10）识别出这个风险、评为 low、
选择了「加注释说明是故意的」。结果注释生效了（`tunnel` 那份现在有很清楚的说明），但之后 `proxysub` 又独立写了第三份，
而第三份的注释只写「same scheme as port tokens」，不知道还有第二份。
**「用注释固定重复」在这个仓库被实证为不稳定解。**

### D.2 四份分层守卫 → `test/architecture/layering_test.go`

```go
// test/architecture/layering_test.go — 无 build tag，进 make test
package architecture_test

type rule struct {
	pkg      string
	banned   []string // 传递 import 集合中不得出现
	required []string // 必须出现（今天为空，留给将来）
	why      string   // 违反时打印，指向建立这条规则的那次 review
}

var layeringRules = []rule{
	{pkg: "internal/cluster", banned: []string{"github.com/nats-io/nats.go"},
		why: "L-2: raft engine must not reach the wire layer (D5)"},
	{pkg: "internal/jsstream", banned: []string{".../internal/cluster"}, why: "D5/D8"},
	{pkg: "internal/clusternodes", banned: []string{".../internal/cluster"}, why: "pure-SQL leaf (D6/D7)"},
	{pkg: "internal/xferaudit", banned: []string{".../internal/cluster"}, why: "D8"},
	{pkg: "internal/proto", banned: []string{".../internal/auth"}, why: "S2 P19: keeps auth→proto acyclic"},
}

func TestLayering(t *testing.T) { /* 一份 goListDeps + 一份 moduleRoot */ }
```

380 行 → 约 130 行。**新规则的边际成本从「新开一个 dN 文件」降到「加一行」**——
这正是为什么今天这条规则被断言了 4 次却没人知道「tether 现在总共有几条分层规则」。
注意最后一条 `proto ⊅ auth` 是我新增的：P19 删掉 `auth` 的 subjectPrefix 副本后，
需要这条断言防止将来 `proto` 反向 import `auth` 造成真环。

### D.3 集群测试 harness → `internal/testharness/cluster`

`internal/testharness` 已存在（189 行、6 个函数、被 36 个文件 import、`internalImports=2`），
边界画在**单机原语**上（`StartNATS`/`StartJSNATS`/`OpenDB`/`SilentLog`/`WaitNodeOnline`/`WaitConnect`/`FreshUserPub`）。
缺的是**集群那一层**——也就是最贵、被抄了 5 遍的那层。

```go
// internal/testharness/cluster/cluster.go
package cluster

type CA struct{ /* newRouteCA 的产物 */ }
func NewRouteCA(t testing.TB) *CA
func (c *CA) Leaf(t testing.TB, name string) tls.Certificate

type Cluster struct{ Nodes []*Node; LeaderIdx func() int }
func Start(t testing.TB, n int, opts Options) *Cluster   // 收 startRoutedJS + attemptRoutedJS 的重试
func (c *Cluster) Stop()

// 统一泄漏门（今天有 4 个略有差异的实现，CLAUDE.md 的核心并发口径其实并不统一）
func AssertNoLeak(t testing.TB, base Baseline)
func Snapshot(t testing.TB) Baseline    // 收 fdCount ×3
```

同时把已有导出用起来：`openDB` **20 份** → `testharness.OpenDB`；`startNATS` **14 份** → `testharness.StartNATS`；
`silentLog` **14 份** → `testharness.SilentLog`。

**执行纪律（重要）**：一个 lane 一个 PR，每个 PR 单独跑 `go test -tags dN_integration -race ./test/dN/`。
**不要一次性全换**——`make e2e` 刻意串行（`Makefile:22-34` 和 `all_phases_test.go:32-38` 记录了完整的试→测→退过程：
D8 在 2-way 挂、D5 在 4-way 挂、GOMAXPROCS 封顶无效），一次性大改会让 flake 与真回归无法区分。

### D.4 tunnel kill-fence 四份拷贝 → 一张表

```go
// internal/tunnel/kill_fence_test.go
func TestKillFenceInvalidatesInFlightRegister(t *testing.T) {
	for _, tc := range []struct {
		name   string
		kill   func(*Server)          // CloseProxy / CloseSession / ForgetSession / CloseProxyIf
		origin string                 // "p13 external review round 2 F1" …
	}{
		{"CloseProxy",    func(s *Server) { s.CloseProxy(publicPort) },        "round2 F1"},
		{"CloseSession",  func(s *Server) { s.CloseSession("lab") },           "round5 F1"},
		{"ForgetSession", func(s *Server) { s.ForgetSession("lab") },          "round6 F4"},
		{"CloseProxyIf",  func(s *Server) { s.CloseProxyIf(publicPort, alloc) },"d6"},
	} { /* 统一用「真 Server + in-flight REGISTER 竞态」机制 */ }
}
```

263 行（三个 round 文件）+ `d6_test.go` 的白盒 map 探查 → 约 90 行，**净减 190 行**。
`CloseProxyIf` 从「手搓 `Server{}` 字面量 + 直接读 `srv.killGenAllocation` map」的白盒机制**降级为表的一行**——
这是收益最大的部分：白盒探查是这四个测试里唯一一个在 `tunnel.go` 内部结构变化时会假绿的。

**必须保留三段各自的 doc comment**——它们解释了三种竞态的**因果差异**，合并后作为表项的注释存在。

### D.5 `versionSkewResponse` 搬迁（配合 P24，不是删除）

```go
// internal/cluster/join_bundle.go — additive omitempty 字段
type JoinBundle struct {
	// …既有字段…
	ProtoVer       int    `json:"proto_ver,omitempty"`       // S2/L06 F3
	ReleaseVersion string `json:"release_version,omitempty"`
}

// internal/broker/clusteradmin.go
func (a *ClusterAdmin) StartJoinOperation(bundle string) (string, error) {
	b, err := cluster.DecodeJoinBundle(bundle)
	// …
	if resp, reject := versionSkew(b.ProtoVer, b.ReleaseVersion); reject { return "", resp }
	// …
}
```

**这是全清单里唯一一条「删除的前置条件是先补一个功能」的项**。
原因：`versionSkewResponse` 是全仓唯一的 join 版本闸，但它被绑在 `OpClusterAdd` 这个 v0.4.2 已停用的 transport op 上，
所以 C8 换 grow 入口（`OpClusterAdd` → `OpClusterJoinApprove`/`driveJoin`）时**闸静默掉了**；
而 `b6_skew_test.go` 直接调 `versionSkewResponse` 所以一直绿——**测试覆盖了一条 CLI 到不了的路径**。
不先搬就删 = 把唯一的版本闸连同死路径一起删掉。

---

## 9. §E — 安全网：不能删但看起来像死代码

**这一节是本报告最重要的部分。** 下面每一条在 `deadcode` / 零引用扫描 / 朴素 grep 下都会显示为「可删」，
但删了会造成实质损失。照单执行删除清单的人**必须先读这一节**。

### E.1 零引用但语义载荷（删了会静默改变行为）

| 符号 | 位置 | 为什么不能删 |
|---|---|---|
| `clusterupgrade.AgentUnknown` | `internal/clusterupgrade/plan.go:24` | **`iota` 零值 + fail-closed 语义是载荷**。godoc 明写「A caller that forgets to classify therefore gets the conservative pre-P3 behaviour, never the broker-only shortcut」。删掉它会让后面所有枚举值下移一位，且让「忘记分类」从 fail-closed 变成 fail-open |
| `cluster` 的 5 个 `applied*.index` 字段 | `internal/cluster/fsm.go:74-78` | 字段确实**只写不读**（全部以位置字面量 `appliedOK{l.Index}` 构造），但类型本身载荷极重：`fsm.go:80-89` 有一段 10 行 INVARIANT 注释**禁止**给这些类型加 `Error()` 方法，并记录了曾经有人加过、导致 D7 forged-sig poison-skip 路径**静默失效**。删字段会诱发「那这个类型是不是也没用」的下一步 |
| `proto.RehomeDirective` | `internal/proto/messages.go:192-206` | 12 行 godoc 明确说明它是**有意未接线**的 D7 备份 rehome 触发器、主路径是什么、将来接线时为什么 wire 已稳定。**但**：同一段 godoc 最后一句「a guard test asserts it has no live publisher」——**我实测该 guard test 不存在**（全仓 `RehomeDirective` 只有 3 处命中，全在 `messages.go` 自己）。处置：**保留类型，删掉那句假保证，或真把 guard test 写出来** |
| `clusterroster.Verify` / `Select` | `internal/clusterroster/roster.go:96,145` | **包 godoc 逐字写着「They are intentionally present-but-unconsumed — NOT dead code to remove; removing them would force re-deriving + re-testing the verifier when the consumer lands」**。而且 `Select` 在 `roster_test.go:93` 有测试。见 §F.1 |
| `proto.SubjClusterWildcard` | `internal/proto/subjects.go:27` | 看起来「只被 `permissions_test.go` 用」，但它的**用途就是当 ACL 字面量的 SSOT 锚点**（`permissions_test.go:246-247` 断言 `SubjClusterWildcard == subjectPrefix+".cluster.>"`）。删它 = 拆掉一条 ACL 同步守卫 |
| `internal/broker` 的 5 个 `*ForTest` 方法 | `ClusterAdminForTest` / `ClusterStateForTest` / `AppliedIndexForTest` / `RODBForTest` / `TunnelTokenLookupForTest` | **不带 build tag 扫描会全部误报为死代码**。它们被 `test/d9/` 与 `test/d6/` 的 gated 测试使用，且因为跨包测试拿不到 `export_test.go` 的可见性，**结构上被迫导出** |
| 8 个「测试非空转」计数器 | `AuditPublisher.TruncationLossCount`/`LagExceededCount`/`DeletedStreamLossCount`、`webhookPoster.Drops`、`cluster.Node.DedupCount`、`spawnsafe.Policy.WedgedCount`、`lockKeeper.Renewals`、`broker.homeDeliveryStats` | 服务于一条明确的测试质量原则（断言非空转）。`fsm.go:60-70` 解释得很直白：「the in-scope writes are SQL-idempotent so a row-count assertion alone is vacuous」。成本每个 1 行。**保留**，但建议统一加 `// test-observability accessor` 标记，让下一个 auditor 一眼分类。（L05 F4 建议把其中 3 个接进 `/metrics`——那是净增，不属本清单） |

### E.2 grep 陷阱（同名但不同物，朴素 grep 会误删）

| 陷阱 | 说明 |
|---|---|
| `Revoke` / `PlanRevoke` | `internal/port` 的这一对是死的（P2）；**`internal/proxysub.Revoke` / `proxysub.PlanRevoke` 是活的**（`broker/proxy.go:619`、`proxy_cluster_wire.go:106`）。全仓 `grep Revoke` 会同时命中两组 |
| `ServerName` | `natsconf.Ownership.ServerName()` 是死方法（P11）；**`natscluster.Broker.ServerName` 是活字段**（16 处 prod 引用、渲染进每一份 nats.conf）。全仓 `grep ServerName` 有 78 处命中，绝大多数是后者 |
| `Free` / `PlanFree` vs `Revoke` / `PlanRevoke` | L08 F2 的论证暗示「port 单键形式 = 有 race = 该删」。**这个推论对 `Free` 不成立**：`port.Free`（`proxy.go:115`、`:183`）与 `port.PlanFree`（`proxy_reconcile.go:93`、`cluster_forward.go:600`）都是**活的生产路径**，仓库有意用端口单键形式管 proxy 出口端口。删 `PlanRevoke` **不会**连带删掉 `planPortStateChange`（它被 `PlanFree` 共享） |
| `pty.*.ready` vs `pty.*.failed` | `SubjPtyReady` 是死的（P22）；**`SubjPtyFailed` 是活的**（`broker.go:977` 订阅 + `agent/run.go:466` 发布）。ACL 里 `:175` 和 `:254` 的 `pty.*.failed` grant **不能删** |
| `HashToken` | `port.HashToken`（`agent.go:1162,1173` 活）、`proxysub.HashToken`（`proxysub.go:77`、`plan.go:29`、`subhttp.go:92` 活）、`tunnel.hashToken`（包内活）。三份都有真调用者，**是重复不是死代码**——处置是合并（D.1）不是删除 |
| `adminsock.Request` 的 6 个字段 | `NodePub`/`JoinToken`/`TunnelAddr`/`PublicHost`/`JoinerProto`/`JoinerRelease` **只在 `handleAdd`+`versionSkewResponse` 内被读**。它们**不是独立可删的**——只有在执行 P24 之后才变死。而 `.TunnelAddr` / `.PublicHost` 这两个字段名在全仓有 30/39 处命中，绝大多数是**别的 struct 上的同名活字段** |

### E.3 通过字符串 / 反射 / 构建标签间接使用

| 类别 | 说明 |
|---|---|
| **NATS subject 字符串** | broker 的 27 条订阅声明（`broker.go:958-1030`）用**字面 subject 模式串**注册 handler，handler 函数本身没有 Go 侧调用者。任何「零引用函数」扫描都会把它们报出来。同理 agent 的 forwarded verb switch（`exec.go:57`）按字符串分发 |
| **`clusterwrite.go:59-79` 的 `HasSuffix` 路由表** | 14 条字符串后缀决定 broadcast vs queue-group，与订阅声明分处两个文件。看起来是「一堆无引用的字符串常量」，实际是 leader 路由策略 |
| **JWT ACL 模板** | `internal/auth/permissions.go` 的每一条 grant 都是字符串，与 subject builder 之间**没有编译期链接**。P22/P23 是我逐条核实过 publisher/subscriber 之后才敢删的；**其余 grant 一条都不要凭「grep 不到」删** |
| **8 个 build tag** | `d5/d6/d7/d8/d9/c7/phasefluidity_integration` + `e2e_matrix`。不带 tag 的扫描会误报 5 个 `*ForTest` 方法 + 一批 harness helper。`c7_integration` 是唯一一个**没有任何 runner 引用**的 tag（这正是 T2 可删的依据） |
| **`raft.FSM` / `raft.SnapshotSink` / `raft.StreamLayer` 等接口实现** | AST 扫描报出的 28 个「零引用方法」**全部**是真接口实现，经动态派发调用。`internal/cluster` 用 `var _ Applier = clusterNodeUpsertApplier{}`（`membership_ops.go:168`）、`var _ raft.FSM = (*fsm)(nil)`（`fsm.go:409`）等 4 处编译期断言把契约钉死 |
| **`internal/testharness`** | 唯一一个不在 `cmd/tether` import closure 内的 internal 包（189 行）。**这是正确的**——它被 36 个测试文件 import，替代方案是 36 份拷贝。任何「不在主二进制里 = 死包」的判据会误杀它 |

### E.4 注释也不能删

全仓注释密度 29.3%（生产 68,328 行中 ~20,000 行），局部高达 60–67%
（`Broker` struct 231 行 span 里 155 行是注释、`reconcile_upgrade_lock.go` 60%、`reconcile_passes.go` 59%）。

**这些注释绝大多数不是复述代码，而是记录「为什么是这个数」「不这么写会出什么事」「哪次事故改的」。**
例：`clusterwrite.go:611` 的 `reaperMayDelete` 用 30 行论证「caught-up 必须在 raft 域而非 command 域度量，
因为 SQLite command cursor 在 `LogNoop` 上不前进、跨域比较结构上永不为真，这曾静默禁用了整个闸门」。

**在一个「wire 破坏 = 现网必须重装」的生产工具里，这类注释比测试更耐久。任何重构必须整段搬运，不得当作清理删掉。**
本清单里所有「净减行数」**都不包含删注释**——如果有人靠删注释去凑行数，那是在销毁本仓最有价值的资产。

---

## 10. §F — 我驳回 / 修正的 lane 主张

| # | lane 主张 | 我的判定 | 依据 |
|---|---|---|---|
| F.1 | **L02** F7：「`clusterroster.Select` 在生产与测试中均无调用者（0/0），删悬空的 Select」 | **驳回** | 两处事实错误：(a) `roster_test.go:93` **有**调用（`routes := Select(r)`，断言 VOTER 优先排序），所以不是 0/0；(b) 包 godoc `roster.go:6-9` **逐字写着**「Verify/Select are the TESTED seam for the DEFERRED post-v2 agent-discovery consumer … **NOT dead code to remove**; removing them would force re-deriving + re-testing the verifier when the consumer lands」。这是一条有明确记录的「故意不接线」，删掉它正是本仓做得最好的那类纪律（把「为什么这段代码看起来该删但不能删」写下来）的反面 |
| F.2 | **L08** F11 / **L05**：「把 `ClearCompletionCacheForTest` 搬进 `internal/cli/export_test.go`，生产减 6 行」 | **驳回** | 它被**两类**测试用：`internal/cli/completion_test.go`（同包）**和** `test/cli_e2e/completion_test.go:43,79,105,118,137`（外部包）。`export_test.go` 只对本包测试二进制可见，外部测试包看不到。搬迁会直接编译失败 |
| F.3 | **L08** F11：「把 `NewTestNATSTransport` + stub 搬进 `test/cli_e2e/`，生产减 26 行」 | **降级为不建议** | 技术上可行（`Transport` 是导出接口），但会**丢失覆盖**：该函数构造的是**包内未导出的 `natsTransport`**（字段 `cctx`/`id`/`connect` 全未导出），其存在理由写在注释里——「The connection-Name selection (`CtlNameUnactivated` vs `CtlNameForSession`) is still exercised — that's the whole point of the e2e regression test for the High #1 stale-SID fix」。外部自写 stub 无法触达 `natsTransport` 的 Name 选择逻辑，等于把这条回归测试变成空转 |
| F.4 | **L06** F6：「OpClusterAdd 那条约 225 行的死 op 路径，整体删」 | **修正为 -65 行 + 一次搬迁** | 实测：真正不可达的只有 `handleAdd`（63 行）+ `case` 分支（2 行）。`versionSkewResponse`（~30 行）**必须搬迁不能删**（全仓唯一 join 版本闸，见 D.5）。而 `AddNode`（`clusteradmin.go:227-304`，78 行）**有 24 个测试文件在用它建多节点夹具**（`clusteradmin_test.go` 一个文件就 9 处），删 `handleAdd` 后它变成 100% 测试夹具，属于「降级为 test-only」而非「可删」。原估 225 行里至少 108 行不能按删除处理 |
| F.5 | **L08** F2：暗示「port 单键形式有 race ⇒ 该删」 | **限缩** | 该推论对 `Revoke`/`PlanRevoke` 成立（P2 保留），但**对 `Free`/`PlanFree` 不成立**——两者都是活的生产路径（`proxy.go:115,183`、`proxy_reconcile.go:93`、`cluster_forward.go:600`），仓库有意用端口单键形式管 proxy 出口端口。且 `planPortStateChange` 被 `PlanFree` 共享，不随 `PlanRevoke` 一起删 |
| F.6 | **L10** F3：「删 `internal/auth/jwt.go` + `jwt_test.go`（268 行），把 `test/p1` 两个断言重定向到 `Handler.allow()`」 vs **L08** F1：「不要删，要接线」 | **裁决：按 L10 删，但按 L08 的理由先补校验** | 两 lane 的诊断都只对一半。实测：(a) `IssueUserJWT` 的 `IsValidPublicUserKey` 守卫在生产**确实存在**（`handler.go:169,172` 在调 `allow()` 之前就校验了 `jwtSubject` 和 `clientNkey`）——所以 `TestIssueUserJWTRejectsEmptySubjectWithoutPanic` 是重复覆盖，不是假信心，L08 高估了这一半；(b) `LoadAccountSigner` 的 `IsValidPublicAccountKey` 守卫在生产**确实不存在**（`broker/authcallout.go:70` 直接 `nkeys.FromSeed`，`serve.go:436 loadAuthCalloutSeeds` 只读文件不校验 kind）——所以 `TestAccountSignerRejectsUserSeed` 是**真的**假信心，L10 若直接删而不补校验会丢掉一条产品没有的守卫。**最终处置 = P1：删文件（-92 prod / -176 test），同时把 account-seed kind 校验加进 `loadAuthCalloutSeeds`（+4 行），并把 `test/p1` 的两个测试重定向到那个新校验点** |
| F.7 | **L12** F5：「`docs/reviews/` 归档可净减 42,000 行」 | **修正为搬迁 43,000 行、净减 0** | `git mv` 不删除任何内容。把它记成「净减」会让这份清单的总数虚高一个数量级，正好是我要避免的那种论证 |
| F.8 | 任务前提：「78 个 review-round 测试文件（8,471 行）」 | **修正口径** | 严格口径（`external_review`/`_review_`/`review_fixes`/`round[0-9]`）实测 **60 个文件 / 6,036 行**；宽口径（含 `p13_`/`r8_`/`g67_` phase 前缀）是 134 个 / 18,228 行（L07 口径）或 154 个 / 20,710 行（L12 口径）。三套口径都对，取决于「process-named」怎么定义。**但无论哪套口径，处置都是改名+合并，可净删只有 ~500 行（包头）** |

---

## 11. 执行顺序建议（如果要落地）

按「收益 / 风险」排序，前三批可以合成一个 PR：

**批 1（零风险，一次 `go build` + `go test ./...` 即可验证，-100 行）**
P5 / P9 / P10 / P11 / P12 / P13 / P14 / P16 / P17 / P18 + P15 的注释订正 + `RehomeDirective` 假保证注释订正。
全是零引用叶子 + 误导性文档订正，删除不可能改变任何运行行为。

**批 2（低风险，需改测试调用点，-350 行）**
P1（+校验回填）/ P2 / P3 / P4 / P7 / P8 / P19 / T1 / T2 / T3 / T4 / T5。
**T4 是唯一能直接省 CI 时间的一条**，建议单独先做。

**批 3（低风险但要跑集群测试，-74 行）**
P20（`-race` + 内建泄漏门）/ P21（`go test -tags d7_integration`）。

**批 4（中风险，需同步文档与 ACL，-85 行）**
P22 / P23 —— **必须同步删 `docs/architecture.md` §B.1 subject 树里的对应行**，否则下一轮又会照着文档写回来
（本次发现的 11 行死 ACL 就是上一轮照那棵树写出来的）。
P24 —— **前置条件是先做 D.5 的 `versionSkewResponse` 搬迁**，且改后必须跑 simcluster grow drill。

**批 5（结构性，分多个 PR，-1,940 行）**
T6 → T9 → T7（按此顺序：T6 最独立，T7 最需要逐 lane 验证）。T8/§A 的改名可与任何批并行。

**批 6（文档，-500 行 + 43,000 行搬迁）**
D1 / D2 / §C。与代码 PR 完全解耦。

---

## 12. 一句话总结

**核实之后，tether 可删的代码是 2,564 行 / 1.59%，没有一条属于高风险，最大单项 92 行。
「屎山」如果指的是「堆积了大量无用代码」，那么就本清单的证据而言，这个指控不成立。
真正值得动手的是那 5 处带着误导性文档的幸存者（合计不到 200 行）——
它们的危害与行数完全不成比例，因为每一条都能让下一个改这块的人做出错误决定。**
