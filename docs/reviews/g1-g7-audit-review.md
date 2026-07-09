# G1–G7 收官横切审计 — 阶段 C 对抗复核报告 + 主进程处置

> 阶段 C：对**阶段 B 的 21 处 FIX**（unstaged 工作树 diff）做多专家对抗性复核（6 reviewer 按修复簇 + 逐条 refute-verify，21 agent），检出**回归 / 修复不完整 / 缺测 / 漏约束 / 新异味**。
> 结论：**15 finding，全部 confirmed，无 blocker**——1 个实质 incorrect-fix（A7）、1 个诊断退化 regression（C1）、1 个文档误挂（C8），其余为**缺测**与微效率/注释。主进程逐条处置如下（专家只读+可建议测试，绝不改实现；本轮修改全部由主进程落地）。
> 定稿人：主进程。日期：2026-07-09。

---

## 处置总表（15 finding）

| # | fix | 类别/严重度(校准) | 处置 | 落地 |
|---|---|---|---|---|
| 1 | A7 | incorrect-fix / minor | **FIX** | node-list 应用层错误码（`nlResp.Code!=""`，store_error/not_a_member/…）也 fail-closed——原修复只挡 transport/decode，同"全机误判 stale"症状仍可达。加 `Code!=""` guard + nats 测试 `TestBuildUpgradeNodesFailsClosedOnNodeListError` |
| 2/7 | C1 | test-gap / minor | **FIX** | 抽纯函数 `blockedConfirmDecision(confirms,budget,prevBlocked)` + 表测 `TestBlockedConfirmDecision`（budget0 立即错、边沿计数、同 stall 不复算、distinct stall 复算、预算耗尽）|
| 3 | C1 | regression / minor | **FIX** | budget≥2 的持久 BLOCKED（confirm 清不掉）原退化成泛化超时——现超时消息**暴露最后 BLOCKED 原因 + actionable 提示**（`lastBlockedErr`），不再"decay into generic timeout that hides cause" |
| 4/15 | A5 | test-gap / minor·nit | **FIX** | dry-run 抑制 webhook 加回归测试 `TestDriveAddDryRunSuppressesWebhook`（preflight halt 路径断言 0 POST，httptest+nats harness）|
| 5 | C6 | test-gap / minor | **FIX** | Ready 门加断言 `TestG7DefaultProxyStatusReadyGatedOnHomeHealth`（**seed proxy_ready=1** 才有效——核验器指出现测因默认 0 而 Ready 本就 false）：unhealthy-home→Ready=false，healthy→true |
| 6 | A9 | test-gap / minor | **FIX** | draining 排除加 `TestA9RebalanceTargetsExcludeDraining`（brk-b 可达健康、只因 draining marker 被 `eligibleProxyHomes`+`pickProxyRehomeTarget` 双双排除，钉住不掉 filter）|
| 8 | C3 | smell / nit | **NO-OP** | sentinel-only no-op 分支返回的 backup 路径可能不存在（仅"运维手动 rename 备份+留 sentinel"这一近不可达分支触发）；`resp.BackupPath` 是**未被读**的休眠 wire 字段。加 Stat/fallback=为不可观测字段的不可达分支增复杂度，违安全实用主义。**不改**（采纳核验器 no-fix）|
| 9 | A1 | smell / nit | **NO-OP(+注释)** | live up-standalone 但 loopback 探测持续失败→误判 down→跳 SIGKILL→报 revival_failed：罕见、响亮、经 STAGED-idempotent driver retry 自愈；行为修改会**重引三态探测意在消除的歧义**。仅在 `cutoverAwaitRevival` 分支加一行澄清注释（采纳核验器）|
| 10 | A12 | test-gap / nit | **FIX** | `TestUpgradeTriggerReexecAgentNoNID` 加断言 `Error contains "broker not connected"`——`CodeClusterNotEnabled` 与 account-key 门共用，只验 code 可能因签名/seed 坏而误 pass |
| 11 | C6 | smell / nit | **FIX** | 每行两次 `proxyHomeHealthy`（各一次 DB lookup）——hoist `homeHealthy` 算一次复用两门（保留 Ready 门的 `clusterMode&&homeBroker` 守卫，核验器纠正过 naive hoist 会误伤单 broker 行）|
| 12 | A10/A11 | test-gap / nit | **WAIVE** | 见 §2。两者均**行为等价 no-op**，底层逻辑已直接测（A11 的 `xferMaxBytesForCeiling`=`TestXferMaxBytesForCeiling`；A10 的 `arm.tick`=多个 `TestAutoRebalanceArm*`）；集成路径（driveAutoRebalanceOnReturn 真跑 rebalance / ensureXferBucketSized 需活 JS）代价与 nit 收益不匹配。核验器明允"explicit waiver 可替代" |
| 13 | C8 | doc / nit | **FIX** | 我的 C8 修复把 `UpgradeLockActive` 误挂 v4——`git show` 证它随 v3 bump 于 7a16f72（G5+G7）引入；已移回 v3 clause，v4 只剩 `GrowLockActive` |
| 14 | A2 | test-gap / nit | **FIX** | fallback WARN 加 `TestBuildUpgradeNodesWarnsOnResponderFallback`（2 health responder + 无 roster responder→fallback 成功且 WARN，验"不 fail-closed 废掉 pre-G3"+"响亮告警"两面）|

合计：**11 FIX（6 代码 + 5 补测）+ 2 WAIVE（A10/A11）+ 2 NO-OP（C3 sentinel / A1 机制，A1 加注释）**。

---

## 1. 实质项详解

- **A7（唯一 incorrect-fix）**：阶段 B 的 A7 只对 node-list 的 transport/decode 失败 fail-closed，但 broker `handleNodeListReq` 对 store_error/not_a_member/session_not_found_or_deleting/actor_invalid 会回**结构良好、Code 非空、Nodes 空**的 reply——decode 干净、两道 guard 都不触发，agentRelease 仍空→全机 AtTarget=false→已在目标版的集群被规划成整轮（多余 leader transfer + agent re-exec）。这正是 A7 意在防的症状。修：decode 后 `if nlResp.Code != "" { fail-closed }`（合法空集回 Code==""，不误伤）。
- **C1 regression**：budget≥2 且单个持久 BLOCKED（confirm 发了但清不掉、op 不再 transition）时，边沿只触发一次 confirm，`confirms<budget` 故不报预算耗尽错、`prevBlocked` 故不再 confirm→空转到 deadline 出**泛化** `did not reach SERVING`。这与本文件 cutover 的 `stableCutoverRefusal` 刻意避免的"decay into generic timeout that hides the cause"同类。修：超时分支若 `lastBlockedErr != ""` 则输出"last state BLOCKED (%s) … `cluster ops confirm`"actionable 消息。功能结局不变（grow 失败需人工），仅诊断质量恢复。
- **C8 doc**：`git show 7a16f72`（G5+G7）同时 bump SchemaVersion 2→3 并加 `UpgradeLockActive`（其结构注释即"External-review round2 B1"=G5/G7 轮）；`d93d842`（G4）3→4 只加 `GrowLockActive`。我原 C8 把 UpgradeLockActive 挂到 v4=错，已改回 v3。无运行时影响（无 consumer gate 于该值），但 C8 的验收标准就是账目准确，故修。

## 2. WAIVE 论证（A10/A11 集成测试）

两处均为**已验证的行为等价 no-op**，plan §3 列了对应测试，主进程判定用**等价性 + 底层已测**豁免集成 pin（核验器允许 explicit waiver）：

- **A11（push 准入单次 ceiling）**：`ensureXferBucketSized` 只是把 `xferBucketMaxBytes` 算出的值透传 + 沿用原 create/raise/错误路径，错误串（`xfer bucket sizing: %w`）与 too-small 语义不变；**磁盘感知定值数学**（唯一有判定的部分）由 `TestXferMaxBytesForCeiling` 直接覆盖。建桶路径需活 JetStream，现有 d8 push 测试本就不触及（在 home gate 前 bail）。
- **A10（gatesClear 仅有活时算）**：只改**何时计算** gatesClear，不改 `arm.tick` 的开火逻辑；`returned==0 && pending==0` 时 tick 不可能产出 ready==true，gatesClear 永不被读=可证等价。`arm.tick` 的开火/dwell/cooldown 语义由多个 `TestAutoRebalanceArm*` 覆盖；`driveAutoRebalanceOnReturn` 真跑 `rebalanceProxyHomes`（需全套 DB+proxy 状态），为一个等价 no-op 写集成测试代价与 nit 收益不匹配。

## 3. 硬闸

阶段 C 修改后重跑：`make test`（全量 `go test ./...`）+ `make lint`（golangci-lint v2）+ 并发面 `-race`（broker）全绿。新增/改动测试全过。**A1 触碰 cutover 部署面**：release 前建议 `sim drill 10-grow-to-3`/`11-grow-gaps` 复核（happy-path 行为不变，属分内 deploy-tier 门）。

## 4. 交外审

本轮（阶段 A 定稿 + 阶段 B 实现 + 阶段 C 复核处置）就绪，交用户做最终人工外审。外审报告写 `docs/reviews/g1-g7-audit-external-review.md`，主进程在其内逐条回复并修改；**外审不过不算 done**，故此处停下、不 commit、不 git add（外审阶段暂存是外审者的工作）。
