# B4 review（Stage C 内审 + 主进程采纳）

> Stage C：6×Opus 对抗审查（5 视角 wire-byte-equiv / onbroker-security / alert-correctness / observability-scope / test-adversary + 1 综合）。**Verdict：B4 SOUND，无 BLOCKER**——5 reviewer + synth 独立复核全部硬不变量（非集群字节等价 / `--on-broker` 安全 / alert poison-safety + 零 ACL 改）均 HOLD。1 个真缺陷（D1，我漏的 drain-marker 窗口）+ 1 MAJOR（J1，exit code 错）+ MINOR/NIT（多为测试缺口）。主进程逐条采纳；改完 lint 0 / make test 全套绿 / 新增 gated d6+d8 绿。

## 采纳（已修）

### D1 [真缺陷，原 NEEDS-MAINTAINER-DECISION → 采纳 fix(a)]
`--on-broker` 校验只查 `Eligible()`(==phase VOTER) + `CertFP!=""`，**漏了 drain-marker 窗口**：`DrainNode` 先升 `broker_draining` marker（step 1）→ `migrateExposes`（step 2）→ phase→DRAINING（step 3）。step 1↔3 之间 phase 仍是 VOTER，`Eligible()` 通过 → `--on-broker <该节点>` 被接受、pin 到正在 drain 的 home（step-2 已迁完，这条新 expose 被搁浅）。错误串 `(VOTER, cert-pinned, non-draining)` 承诺了未执行的检查。plan §0/§1c/§F.11(e) 本就要求 `&& !draining`，我实现时误以为 phase 已覆盖。**修**（plan-faithful fix(a)）：`clusterwrite.go` allocatePort 的 `--on-broker` 分支补 `cluster.DrainingNodes(b.cfg.DB)` 成员检查，命中（或读失败，fail-closed）→ `errOnBrokerUnknown`（在 Propose 前、不写行）。错误串现与谓词一致。**测**：`b4_expose_test.go` 加 `brk-draining`（VOTER+cert+draining marker）拒绝用例 + `TestB4OnBrokerPredicateSense` 谓词锁。非安全缺陷（仍排除一切非 VOTER/无 cert；pin 的是真 voter），但承诺必须诚实——已修。

### J1 [MAJOR, 真缺陷]
`expose explain <bad-name>` 返回 `unavailErr`→exit 69（EX_UNAVAILABLE "broker unreachable"），但 name 不存在是**对健康 broker 的成功往返**——应为 exit 64（usage），与 `node_not_found`/`session_not_found` 约定一致。监控脚本会把打错的名字误判成 "broker DOWN"（正是 B2 taxonomy 要区分的）。**修**：`expose.go:211` `unavailErr`→`usageErr`。**测**：`TestB4ExposeExplainNotFoundIsUsageExit`（classifyExit==exitUsage、非 exitUnavailable）。

### m1 [MINOR]
`exposeExplainJSON.Moved` 标了 `omitempty` 却是 script-switched 字段——`moved=false`（常态）被丢，`.moved` 读成 absent/null（bool 的缺失歧义，jsonout.go:24-25 策略明禁）。**修**：去 `omitempty`（与同结构 `Rebuild` 一致恒在）。**测**：`TestB4ExposeExplainJSONNoDeferredKeys` 加 un-moved 行断言 `"moved":false` 在场。

### m5 [MINOR, test]
`TestB4ReconcilerNeverClearsManual` 只覆盖全局 `manual` 键，未覆盖 `manual:<label>`（reconciler 的 broker_draining clear 是**前缀**匹配，不变量须对带标签键也成立）。**修**：同测加 `manual:brk-b` 键，断言 healthy reconcile pass 后两键都存活。

### m6 [MINOR, test]
正典 `proto_invariants_test.go` 的 roundtrip/forward-compat 套件（未来加字段者读的那个）用的是 pre-B4 fixture，静默跳过 B4 字段。**修**：给 ExposeReq/ExposeResp 用例加 `RebuildOff/OnBroker`、`HomeBroker/Epoch`，并给 PsResp 用例加带 home/epoch/rebuild 的 `PsPortEntry`。

### m2-b [MINOR, test]
正向 `--on-broker` 写路径在所有套件都未测（unit 全是拒绝用例 → 谓词反转 e.g. `CertFP==""` 不会被任何现有测试抓到）。完整正向写（需真 raft node）记 refinement→test/d6；**先落便宜的谓词 sense 锁** `TestB4OnBrokerPredicateSense`（健康 VOTER+cert+非 draining 满足谓词、四个拒绝 fixture 不满足、加 draining marker 后翻假），守住安全关键的 accept 方向。**过程修一隐藏 fixture bug**：`b4InsertRoster` 漏设 `nats_server_id`，`LookupByNodeID` 扫 NULL 进 string 报错——在纯拒绝测试里被 err→reject 掩盖；补上后拒绝仍因正确原因拒绝、正向谓词为真。

### 专家新增测试（采纳留存）
- `internal/port/b4_rebuild_test.go::TestB4OfflineReconcilerScannerColumnOrder`：补 §F.8 未覆盖的 `ListAllocatedForOfflineNodes`（不同的 table-qualified SELECT 喂同一 scanner）的列序锁。
- `internal/broker/b4_alertadmin_test.go::TestB4ValidAlertTextAllowsControlChars`：钉住 `validAlertText` 只拒 NUL/非 UTF-8/超长、**不**拒 newline/tab（mirror LitText），并验字节级（非 rune）长度上限。

## 残留（记 refinement，非 ship-blocker）

- **m2-a / m3 正向路径 gated 测试**（需真 leader `*cluster.Node`）：`--on-broker` 正向写 + `ExposeResp.HomeBroker` 反映（→ test/d6）；operator `alert raise/clear` 成功 + 双 raise `AlreadyActive` + follower `not_leader` + clear 幂等 + 真 MaxOpenConns(1) 下 in-closure COUNT 不死锁（→ test/d8）。实现已被 5 reviewer + synth 代码级独立复核为正确（poison-safety 由 nil-admin 测试证"未达 Propose"；in-closure COUNT 经 node.go:321 单行 Scan 契约验证安全）；gated 正向锁留待 d6/d8 harness 接 B4 seam（与 B3 把 RemoveNode(force) N>0 端到端留 gated d7 同性质）。
- **m4**：`test/d7` drain-refusal 用直 SQL 注 `rebuild_on_failure=0` 行（非经 B4 `--no-rebuild` allocate）——writer 由 `TestB4AllocateRebuildColumn` 证、refuse 由 d7 证，链路两半未合一。低风险，记 refinement。
- **N-banner**（NIT）：`expose explain` 不渲染 D8 severe-alert banner（ps/node ls 有）。plan §2e 范围外、acceptable。
- **N-clear-help**（NIT）：`alert clear` 接受任意 dedup_key 含 system 键（仅校验 NUL/UTF-8/长度）——intentional + safe（plan §H 砍 emergency-system-clear；system 键下个 reconciler/disk tick 自愈）。

## 驳回 / 确认为 false-alarm

- **N-success-json**（wire-byte-equiv N1）：`exposeJSON.RebuildOff` 报的是**请求的** flag 非 broker 确认值（ExposeResp 不带 RebuildOff）——对老 broker 静默丢 `--no-rebuild` 时 JSON 印 `rebuild_off:true` 而行其实 rebuild-ON。**确认 cosmetic**：契合"老 broker 静默 no-op"fail-safe；`expose explain` 读真持久化行、是权威。无需改。
- **N-omitempty-dedupkey**：`AlertResult.DedupKey` 无 omitempty——安全（父 `Alert *AlertResult` 指针 omitempty）。留。

## 出口

内审通过、硬闸全绿：`make lint` 0 issues、`make test` 全包绿、gated `test/d6`(-race) + `test/d8`(-race) 绿。
**gated `test/d7`(-race) 三子测试失败（`ForgedSigPoisonSkipsOnFollower`/`ForceSingleRecoverRestart`/`DrainLeaderTransfersAndBails`，根因 membership `OpClusterNodeUpsert` Aux↔Body 位置化交叉校验在本 WSL2 -race 环境确定性拒绝 legit d7-a upsert）经 clean-HEAD（纯 committed `v2 plish 2`，无任何 B4 改）worktree 验证 = 完全相同地失败 → 既有失败、非 B4 回归**（与 B2/B3 的 d7-gated 处理同；B4 未触碰 `internal/cluster` membership 生产代码）。外审统一留最后（按本轮 goal）。
