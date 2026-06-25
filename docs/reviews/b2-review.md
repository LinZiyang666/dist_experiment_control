# B2 review（Stage C 内审 + 主进程采纳）

> Stage C：6×Opus 对抗审查（5 视角只读 + 1 综合）。**Verdict：B2 sound 但未完成——2 个 MAJOR 必修**（无 BLOCKER；6 条硬约束全 held：默认文本字节等价、0=成功、无 proto wire-break、0..3 保留、broker/connect 串字节相同、Code additive）。主进程逐条采纳如下；改完 lint 0 / make test 全套绿 / d9 gated 过；**d7 gated 失败经 clean-HEAD worktree 验证同样失败 = 预存 WSL2 -race flake、非回归**。

## 驳回（false alarm，综合已判定）
- "node ls/ps 默认路新增 stderr banner 副作用"：驳回。`withBanner(…, false)` B2 前已存在（D8b），非 B2 引入。
- "`--remote --json` 无判别符"：部分错。它带 `view:"ctl-remote"`（B1 F6 设计），非无判别符。真缺口是 socket 报告无 `schema` 键（见 B2-3）。
- "未提交工作树"：正确观察非缺陷——B1+B2 全在工作树，HEAD `8eb7f68`/`69089d3` 是 D-epic 审计、非 B2。

## 采纳（已修）— MAJOR
- **B2-1 [MAJOR]**：adminsock cluster `Code` 枚举半死（10 个里 5 个从不上线）→ `cluster add`/`drain`/`transfer` 失败误判 70。**核验**：D7 **pin 了**错误串（"catch_up_stalled"/"not in the raft configuration"/"cannot retire the last voter"），故不能改串做 sentinel-`%w`。**修**：broker 端加 `clusterCodeFor(err)` 识别**自己的 test-pinned 串**赋 Code（CLI 侧仍只 code→class、不 sniff prose），接到 Remove/Transfer/Rotate/Drain/Add 错误点；StatusReport 错误（仅 DB 读失败）直赋 `CodeStoreError`。**残留**（文档化 → B2.1）：未 pin 的自由文本 cluster 错误仍 `Code=""`→70（`CodeAlreadyVoter`/`CodeNodeUnknown` 暂未接，待 sentinel 拓宽）。加 `TestB2ClusterCodeFor`。
- **B2-2 [MAJOR]**：arg-validation 返 70 非 64（仅 expose 用了 usageErr）。**修**：`node upgrade`(--url/--sha256/--all 互斥/either)、`session create`(--pin)、`cluster add`(token-call required)、`cluster init`(--from-existing/requires) 全改 `usageErr`(64)。

## 采纳（已修）— MINOR / NIT
- **B2-3 [MINOR]**：socket `cluster status --json` 无 `schema` 键、jsonout 头 + usage §9.14 "每个载体带 schema" over-claim。**修**：jsonout.go 头 + usage §9.14 改为"8 个 list/result DTO 按 schema；cluster status 家族按 view"。
- **B2-4 [MINOR]**：`cluster status --json` 用裸 `json.MarshalIndent`（丢 err）绕过 emitJSON。**修**：走 `emitJSON`。
- **B2-5 [MINOR]**：`nats.ErrTimeout`→70（应 75）。**修**：`classifyExit` 加 `errors.Is(nats.ErrTimeout)→exitTransient`。
- **B2-6 [MINOR]**：session connect 未经 connectError（70 非 69）。**修**：`session create`/`session ls` connect→`connectError`、request→`unavailErr`。（`session create` resp.Error 当 code 传的误分类 = `SessionCreateResp` 无 Code 字段 = wire 改 → **B2.1 延迟**，记残留。）
- **B2-8 [NIT]**：幻影 `session run`。**修**：exitcode.go 注释 + usage §9.13 去掉 `session run`。
- **B2-9 [NIT]**：`proxy status --json`（P13 原始 dump、无 schema）。**修**：usage §9.14 加一句说明。
- **B2-10 [NIT]**：§9.14 branch-field 漏 `is_leader_view`。**修**：补入。
- **B2-11 [NIT]**：`cluster status --json` PARTIAL 仍退 0..3。**修**：usage §9.13 加说明（报告产出即退 0..3、错误经 errors[]/partial、仅无报告退 69）。anti-swallow fold 本身正确、是改进。

## 残留（记 B2.1，非本批）
- `node upgrade --all --json`（per-node result map + dispatchUpgrade 签名改）+ `transfer --json`（含 tier-B async 漏斗）—— 审查确认**延迟干净、payload 内无 B2 blocker**。
- cluster `Code` 对自由文本错误的 sentinel-promotion 拓宽（`CodeAlreadyVoter`/`CodeNodeUnknown`）。
- `session create` 的 `SessionCreateResp.Code` wire 字段（proto 改）。
- **golden 默认文本字节锁**（审查 #1 价值最高但需 §0 renderer 抽取——本批确认字节等价靠 git-diff/inspection，golden test 基础设施留 B2.1）。

## 采纳的新测试
`TestBrokerCodeExitClassesNeverReserved`（每 class ∈{64,69,70,75,77}）、`TestClusterAdminErrorExitClass`（code→class + 消息格式）、`TestEmitJSONEncodeFailure`（→70）、`TestClassifyExit` 加 nats.ErrTimeout、`TestClassifyExitNeverHealthCodes` 强化（非 nil ≠0）、`TestB2ClusterCodeFor`（broker pinned 串）、adminsock `TestResponseCodeRoundTrips` + `TestClusterStatusReportBranchFieldsAlwaysPresent`、jsonout `TestExposeJSONShape`。

## 出口
内审通过、硬闸全绿（d7 gated 预存 flake 非回归）。**外审统一留到全部 B 实现完之后**（按本轮 goal）。
