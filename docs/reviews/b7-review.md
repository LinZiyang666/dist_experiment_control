# B7 Stage-C review + adjudication — DOC#1–#7 + AUTO#9（最后一批）

> Stage C：6×Opus 对抗审查（6 维度：doc3-roster-security / byte-equiv-wire / autonine-quorum-safety / doc7-guided-safety / doc2-scope-adjudication / tests-adversary）→ 1 synth。38 findings → synth 去重为 **0 BLOCKER + 5 MAJOR + 11 minor**。synth 经验性探测（probe 脚本）验证了 AUTO#9 的 M2/M3/M4。verdict：**CONDITIONAL PASS → 修 M1–M5 + 廉价 minor → PASS**。
>
> **3 个主进程范围 deviation 全裁 JUSTIFIED**（synth 专门 ruling）：
> - **DOC#2 派生视图（非持久 ledger）**：JUSTIFIED——`opFromPhase` 无第二写态、构造上不可能 divergence，**强于** plan 的 ledger（plan §4 自己 flag 的跨-leader interleave 风险）。代价（M3/m3）：completed retire DELETE 行 → 无查询记录（B6 incident export 同失退役节点）；v1 可接受、已显式化+测。
> - **DOC#5 仅事件**：JUSTIFIED——同步 `cluster drain` ErrNoMigrationTarget + `broker_draining` 告警已显 stuck drain；新告警 kind 需 0009 CHECK rebuild（plan 要避的 migration）。一个 free fix（m1：事件补 sid）。
> - **DOC#3 agent 侧 DEFER**：JUSTIFIED——agent 无 account_pub pin、加它+碰 register-reply apply = plan 已 defer 的 fragile leaf；verifier core 正确（self-vouch 测过、canonical NUL-分隔无碰撞、selfID 构造惰性）。**但 seam 须先修 M1**（broker 已 bake 了会回退的 generation）才算干净。

## 裁定：5 MAJOR — 全采纳已修

| # | finding | 裁定 | 修复 |
|---|---|---|---|
| **M1** | roster generation（max wall-clock 时间）在 recover/restore（删 max-stamp peer）+ retire max-stamp 节点时**回退**；`rosterForRegister` 门 `req.RosterGen>=gen` 会让 recover 后陈旧 agent 被钉死在死 broker 集、broker 拒发更正 roster（休眠但已 bake 进 broker） | ✅ | fix(a)：`rosterForRegister` **去掉 generation 门、始终发**（gen 降为 advisory tie-breaker、绝非 broker 抑制决策）；改正"单调/survives recover"注释为"NOT 严格单调、advisory"。proto RosterGen 注释同改。测 `TestRosterGenerationNeverSuppressesAfterRecover`（huge cached gen 仍发 roster） |
| **M2** | `clusterspec.Diff` 误把退非-voter（learner / `Role==""`）当"last voter"REFUSE + 为其 decrement floor | ✅ | refusal + decrement 都 key 在 `isVoter(id)`；非-voter 退役渲 `cluster remove`（roster cleanup、零 quorum 影响）。测 `TestDiffRetireLearnerNotRefused` |
| **M3** | `Diff` 在 REFUSE last-voter-leader 退役**前**就发矛盾 `transfer-leader` 步；且 `<another-voter>` 占位符从不解析为真 id | ✅ | 先判 refusal、只在退役真进行时发 transfer 步；`resolveSurvivingVoter()` 解析为真存活 voter id。测 `TestDiffLeaderSoleVoterRetireNoTransferStep` + `TestDiffTransferLeaderNamesRealTarget` |
| **M4** | `Diff` 多退役 floor 不计 pending add（add 需带外 PoP、非自动步）→ 印出 quorum-UNSAFE 计划（3 voter 全退 + 加 d：drain a,b 到 N=1 后 d 才成 voter） | ✅ | 不计 unverified add；pending add 时退 voter 越过 <2 voters 即 REFUSE（gate"先验证新 voter"）。测 `TestDiffMultiRetireDoesNotDropBelowFloorBeforeAddVerified` |
| **M5** | `cluster doctor` 自检在 daemon DOWN（socket 在但调用失败）时**静默**回落 offline preflight（只查 secrets/cert/db、全 PASS）→ 控制面挂了却 green-lie | ✅ | socket FILE 在但调用失败 → stderr 响亮警告"daemon may be DOWN, showing OFFLINE preflight, NOT a live health check"；socket 不存在（真 pre-init）才安静 |

## minor 裁定

| # | 裁定 | 处理 |
|---|---|---|
| m1 expose_rehomed 漏 sid/nid（违 plan §2:42） | ✅ | migrateExposes 读 sid、onRehome 签名加 sid、事件补 sid（完成 DOC#5 seam） |
| m2 doctor catch_up `>0` vs computeHealth `>64` 矛盾 | ✅ | `doctorLagThreshold=64`（镜像 observeLagThreshold） |
| m4 `cardTopReason` 含 JOIN_VERIFIED_PENDING_VOTER（computeHealth 不降级） | ✅ | 删该 phase，精确镜像 computeHealth |
| m5 `tsUnixNano` 不解析单调时钟 `m=+…` token | ✅ | 剥 ` m=` 后缀再 parse |
| m8 roster 测用 user seed 非 account seed | ✅ | `TestRosterSignsWithAccountSeed`（`nkeys.CreateAccount`） |
| m7 ExpiresAt 签了不验 | ✅（pin） | `TestRosterExpiryNotYetEnforced`（钉文档化 gap） |
| m10 `Parse` 收重复 node_id | ✅ | 拒重复 node_id。测 `TestParseRejectsDuplicateNodeID` |
| m11 ops 非-add StartedAt 报 join 时间 | ✅ | DRAINING/RETIRING StartedAt=phase_changed_at。测覆盖 |
| m3 retire DELETE → 无 ops 记录（incident 同失） | ◻（显式化+测） | `TestDeriveClusterOpsRoundTrip` 钉 m3 边界（retired 节点无 entry）；runbook 后续 |
| m6 plan 列的 `cluster.roster.req` pull responder 未 ship | ◻（记 DEFER） | **主进程 DEFER**：register-push 已是投递路径、`agent doctor`（亦 deferred）读缓存 roster.json 验证；pull responder 待 agent-discovery 叶子一并落，与 agent 消费同批。本 review 记此 deferral。 |
| m9 Diff 多-voter 退役不标注 F==0 crossing | ◻ | advisory；live `cluster drain` F==0 typed-confirm 是真安全网（plan 仅 advisory）。后续 |

## DROPPED（synth 滤的假阳）
- "DEFER 裁定 ship 坏安全锚"独立 MAJOR → 折进 M1 + DOC#3 ruling（verifier core 本身正确）。
- "CanonicalRosterBytes 缺长度框架" → DROPPED（`LitText` 拒 NUL/非-UTF-8，今天无碰撞；length-framing 是 deferred agent verifier 的 nice-to-have，且若加是 wire-breaking 须配 golden 测一起落）。
- DOC#4 proxy seam / 单模式 byte-equiv guard → 无 live 缺陷（seam 正确），转为新增测试项。

## 结论
0 BLOCKER；5 MAJOR（M1–M5）+ 全部廉价 minor **全修 + 全测**；2 个 minor（m6 pull responder、m9 annotation）带理由 DEFER。`make test` + `make lint`（0 issues）+ gofmt 全绿。3 个范围 deviation 全 JUSTIFIED。达 **PASS**。
