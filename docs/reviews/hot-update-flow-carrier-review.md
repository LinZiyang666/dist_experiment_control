Pass（处置后；一项间歇性 e2e 失败转入 S6 跟踪）

# S5 · 逻辑流 carrier + SS 状态热更新 —— 内审报告与主进程处置

日期：2026-10-03 起；2026-10-11 按用户裁决收尾。范围：S5 相对 S4 最终候选树 `12740212ac79bb66f27011b0c0cbf9a24343f760` 的全部改动（plan：[hot-update-flow-carrier-plan.md](hot-update-flow-carrier-plan.md)；证据：[hot-update-evidence.md](hot-update-evidence.md) 的 S5 节）。

**形式**：与 S4 相同，开发与审查由 Codex（`gpt-6.1-sol`）承担。本次内审共 4 路审查，审查树为集成第二轮之后的 `a9a473d7`：

| lane | 关注面 |
|---|---|
| rv-flow | 逻辑流生命周期、permit、replay、half-close、carrier 重建 |
| rv-ss | SS actor 冻结/恢复、nonce/salt 连续性、material 配额、私有状态 codec |
| rv-image | image 组合（exec/pty/flow/ss 四域）、上限、final reader gate |
| rv-authority | broker 授权快照、REGISTER/RESUME、grant 排序、就绪信号 |

审查与核验的分工同 S4：

- 每路审查配一个独立的对抗性核验者，逐条尝试推翻 finding，并在同一 lane 内补报最多 5 条。
- 审查者与核验者只能新增测试，不改实现。
- 报告原文（证据、触发条件、建议回归）归档在仓库外的 `~/tether-hot-update-handoff-20260920T2356Z/s5-review/`，本文件只列定位与处置。

## 1. 结论

审查者共报 **20 条**（rv-flow 5、rv-ss 5、rv-image 3、rv-authority 7），核验者另补报 **2 条**（rv-flow-V1、rv-authority-V1）。核验结果：22 条全部 **CONFIRMED**，严重度均未调整。

- rv-image-3 与 rv-authority-5 是两路独立发现的同一缺陷（final reader gate 不检查保留的 flow/SS 域），合并为一条。
- rv-flow-1 与 rv-image-2 同源（已结束的连接仍占活 flow 表示），分别从 resume 与 image 导出两侧暴露，按两侧分别处置、合并验收。

去重后：BLOCKER 6、MAJOR 14、MINOR 1。

## 2. Finding 与处置

| id | 严重度 | 摘要 | 处置包 | 处置 |
|---|---|---|---|---|
| rv-image-1 | BLOCKER | 组合 Flow/SS 的 image 导入一律被拒（Flow 域无 Detach） | FIX-IMAGE | 已修：Flow Manager/Client pristine Detach 保留 sealed/business closure，释放 reservation；TestFrozenDomainsImportAndDetachCompleteDescriptors / TestFlowPristineDetachPreservesOriginalClosure。 [处置/变异回执](/home/weiland/s5wt/prompts/result-fix-image.md) |
| rv-authority-5 = rv-image-3 | BLOCKER | final reader gate 忽略保留的 flow/SS 域（含空 owner 检查之后才接纳 flow 的竞态） | FIX-IMAGE | 已修：CheckBundleCapabilities 检查全部 retained 域，prepareImageCut 在最终 Prepare 重检；TestFrozenBundleRequiresEveryRetainedReaderDomain / TestFinalPrepareRejectsFlowAdmittedAfterReaderPreflight。 [处置/变异回执](/home/weiland/s5wt/prompts/result-fix-image.md) |
| rv-flow-1 | BLOCKER | 33 条已结束连接耗尽 resume/导出所用的活 flow 表示 | FIX-FLOW | 已修：States 排除 settled history，失败 connector 退休；TestTerminalFlowReceiptsDoNotBreakCarrierResume。 [处置/变异回执](/home/weiland/s5wt/prompts/result-fix-flow.md) |
| rv-flow-3 | BLOCKER | 消费过的 permit 永久阻塞后继 owner 的下一次交接 | FIX-FLOW | 已修：当前 owner 可替换 consumed predecessor permit，client 同步 successor generation；TestEndpointPermitSupportsSuccessiveOwnerGenerations。 [处置/变异回执](/home/weiland/s5wt/prompts/result-fix-flow.md) |
| rv-ss-1 | BLOCKER | 经路由的 SS 上游无法恢复：保存的源地址是通配地址 | FIX-SS | 已修：保存 connect 后实际 bound tuple；TestSSOriginalRawUpstreamTupleRestoresOnEveryRoute。 [处置/变异回执](/home/weiland/s5wt/prompts/result-fix-ss.md) |
| rv-authority-4 | BLOCKER | 迟到的旧 proxy push 撤销了 RESUME 的新撤权 | FIX-SS | 已修：fresh RESUME 恢复 watermark/digest，push 经 Authorize；TestRestoredFreshAuthorityDominatesDelayedKeysetPush。 [处置/变异回执](/home/weiland/s5wt/prompts/result-fix-ss.md) |
| rv-image-2 | MAJOR | image 上限把终态回执当作活 flow 计数 | FIX-IMAGE | 已修：export/restore/codec 分别限制 32 active 与 4096 terminal；TestFlowImageRetainsTerminalReceiptsBeyondActiveLimit / TestBundleFlowCapacitySeparatesActiveAndTerminalRecords。 [处置/变异回执](/home/weiland/s5wt/prompts/result-fix-image.md) |
| rv-flow-2 | MAJOR | abort CLOSE 在保留尾流发送/结算前半关闭数据流 | FIX-FLOW | 已修：CLOSE 等待 terminal settlement 再释放 DATA stream；TestAbortCloseKeepsDataStreamUntilTailReceipt。 [处置/变异回执](/home/weiland/s5wt/prompts/result-fix-flow.md) |
| rv-flow-4 | MAJOR | 去掉已应用前缀裁剪后 overlap 判官仍为绿 | FIX-FLOW | 已修测试：distinct suffix 核对真实 sink，原弱 oracle 作为 rc0 反例保留；TestAppliedOverlapWritesOnlyUnappliedSuffix。 [处置/变异回执](/home/weiland/s5wt/prompts/result-fix-flow.md) |
| rv-flow-5 | MAJOR | 被拒 OPEN 不关闭已接受的 yamux 流 | FIX-FLOW | 已修：bindClientFlow 拒绝时关闭实际 accepted stream 并释放 admission；TestRejectedOpenReleasesCarrierStream。 [处置/变异回执](/home/weiland/s5wt/prompts/result-fix-flow.md) |
| rv-flow-V1 | MAJOR | carrier 重建后无法对账丢失的最终 FIN 回执 | FIX-FLOW | 已修：RebindReceipt / carrier recovery 重放 terminal controls 并对账 generation；TestReconnectReconcilesLostFinalFINReceipt。 [处置/变异回执](/home/weiland/s5wt/prompts/result-fix-flow.md) |
| rv-ss-2 | MAJOR | material 历史配额满时 Thaw 后挂起仍存活的 actor | FIX-SS | 已修：quota admission 失败也恢复 survivor authority，保留 64 histories 与原 watermark；TestSSMaterialQuotaFreshGrantKeepsSurvivorForwarding。 [处置/变异回执](/home/weiland/s5wt/prompts/result-fix-ss.md) |
| rv-ss-3 | MAJOR | 真实路径写出的 failed-connect 回执在导入时被拒 | FIX-SS | 已修：sealed Failed receipt 允许 ENOTCONN，保留 consumed errno；TestSSConsumedRefusalSocketImageKeepsItsReceipt。 [处置/变异回执](/home/weiland/s5wt/prompts/result-fix-ss.md) |
| rv-ss-4 | MAJOR（测试缺口） | T26 的保留 image 臂导出 0 个 actor，看不到活 actor 的密钥材料 | FIX-SS | 已修测试：T26 导出真实 authenticated duplex actor，扫描实际 salts/subkeys/nonces/pending ciphertext；TestSSSecretsNeverEnterPublicCarriers。原盐泄漏 mutation rc0，强化后 rc1；未观察到原生泄密。 [处置/变异回执](/home/weiland/s5wt/prompts/result-fix-ss.md) |
| rv-ss-5 | MAJOR | 拒绝旧 grant 时挂起了新 grant 授权的流量 | FIX-SS | 已修：freshness 在 destructive disabled/capacity 处理之前检查；TestSSOlderAuthorityCannotSuspendCurrentSurvivor。current-pair digest conflict 另由 SS-REVOKE 保持 hold。 [处置/变异回执](/home/weiland/s5wt/prompts/result-fix-ss.md) |
| rv-authority-6 | MAJOR | 计划内 freeze 清掉 proxy 就绪，可能触发分配轮换 | FIX-SS | 已修：planned cut 保留已真实 ready latch 至原 absolute grace，失败 PREPARE 撤销 reservation；TestPlannedProxyCutKeepsLastReadinessWithinDetachedGrace。 [处置/变异回执](/home/weiland/s5wt/prompts/result-fix-ss.md) |
| rv-authority-V1 | MAJOR | SS 新授权拒绝 broker generation 的合法恢复 | FIX-SS | 已修：CompareAuthority 按 generation→epoch→revision 排序；TestFreshGenerationAllowsRestoredLowerEpoch。 [处置/变异回执](/home/weiland/s5wt/prompts/result-fix-ss.md) |
| rv-authority-1 | MAJOR | 新 tunnel 授权绕过 proxy 参与判定 | FIX-AUTH-BROKER | 已修：transaction 内读 proxy_capable，共享 proxyParticipationEnabled；TestTunnelAuthorityRequiresProxyParticipation。 [处置/变异回执](/home/weiland/s5wt/prompts/result-fix-broker.md) |
| rv-authority-2 | MAJOR | proxy 授权快照超出 1 秒 SQL 预算（实测 5.008 s） | FIX-AUTH-BROKER | 已修：beginBoundedAuthorityRead 暂停 SQLite busy handler 并恢复池设置，pool/SQL 都受原预算约束；TestProxyAuthoritySQLWaitUsesLookupBudget。 [处置/变异回执](/home/weiland/s5wt/prompts/result-fix-broker.md) |
| rv-authority-3 | MAJOR | v1 REGISTER 把瞬时授权失败变成终态撤权 | FIX-AUTH-BROKER | 已修：REGISTER 保留 try_again / home_catching_up，expired positive 仍暂拒；TestFlowRegisterPreservesTransientAuthorityReasons。 [处置/变异回执](/home/weiland/s5wt/prompts/result-fix-broker.md) |
| rv-authority-7 | MINOR | 公开的 deferred-reason 契约仍是 S4 的十码集合 | FIX-IMAGE | 已修：公开 DeferredReasons 与 consumer 共用 S5 精确九码；TestDeferredReasonSetIsTheDocumentedOne。 [处置/变异回执](/home/weiland/s5wt/prompts/result-fix-image.md) |

## 3. 内审之外的发现

以下处置补齐 merge seam 与实跑暴露的缺陷，不改变 §1 的原始 22 条计数（§2 合并重复后为 21 行）。各报告中的早期 RUN_RED/NOT_RUN 属于当时基线，后续集成与最终门禁分别留据。

| 发现 | 处置与回归 | 回执 |
|---|---|---|
| SS revoke merge seam | DATA 提前关闭使 broker 未处理 revoke、公网 replay 未释放；延后 stream retirement，保留 endpoint lookup，raw workers join 后发布终态。freshness 早退另绕过 current-pair digest conflict hold，Authorize 恢复该 hold。T21/T43、TestHardRevokeHoldsFinalControlUntilRawWorkersJoin、TestTerminalReceiptPublishesOnlyReleasedReplay、TestSSConflictingCurrentKeysetHoldsSurvivorUntilFreshGrant；十条新 reversions 均 rc1。 | [result-ss-revoke.md](/home/weiland/s5wt/prompts/result-ss-revoke.md) |
| private-state descriptors | tunnel cut 补全 20 receipt fields 与 journal/permit 验证；SS cut 保存 actual authority/timer/material membership，agent 在 callbacks 前比较 sealed metadata，旧 TPRX 仍可读。TestClientCutKeepsTerminalReceiptDecisions、TestSSCutOwnsAuthorityTimerAndMaterialMembership、TestProxyCutMatchesSealedSSAuthorityBeforeCallbacks；64 runtime overlays + 一条 on-disk scanner mutation 均 assertion rc1。 | [result-private-state.md](/home/weiland/s5wt/prompts/result-private-state.md) |
| real-exec / rollback 接缝 | restored Client 缺 logger 导致 expose replay panic；NewClient 补 logger。heartbeat repair 与 gateSS 共用 admission，避免销毁 frozen owner；recovery 共用 attachment，保留 refusal，ACK/CLOSE/DATA retirement 先结算再交终态。T39–T41/T44/T45 三轮 race 通过，T45 为 15 次真实 watchdog round；21 个缺陷共 24 次 assertion-red。 | [result-hotupdate-e2e.md](/home/weiland/s5wt/prompts/result-hotupdate-e2e.md) |
| 集成 startup / 结构预算 | SS startup publication、CLI adapter-start fixture 与结构预算接缝修复；metadata 提取至 internal/proxyimage，Agent 7294 code lines / 125 methods / 19 production files。11 个指定包、determinism/architecture/build/vet rc0；148 intended defect receipts 均 assertion-red，弱 oracle 与两条原 HOTUPDATE green probes 单独保留。 | [result-integ-verify.md](/home/weiland/s5wt/prompts/result-integ-verify.md) |
| P13 ordinary restart | idle adapter 在 Start 后、尚无责任时可接收 flow config；普通 supervisor 经 fresh authority 恢复 restarted broker 的 missing owner；cancel 在锁外关闭底层 TLS。既有 P13/CLI assertions 未改，whole-package races rc0，79 mutation executions assertion-red。此兼容例外变更 frozen before-Start 限制；premature-adapter-start 重分类为正控。 | [result-p13-regress.md](/home/weiland/s5wt/prompts/result-p13-regress.md) |
| GATES-FINAL-3 data race | fixture 异步 Manager.Close 与下一 fixture 替换 drillobserve.Hook 竞争；同步关闭两个 manager，REGISTER lookup 前持有 public-port reservation。另补 closed OpenStream retry 分类。paired 20-run 复现 rc1；清理回退 mutation 400ms schedule 报同一 race，恢复 rc0；endpoint + lifecycle GOMAXPROCS=2 race count50 rc0。这不关闭 gates-5 的另一项 invalid flow owner state，见 §4。 | [result-gates-final3.md](/home/weiland/s5wt/prompts/result-gates-final3.md) |
| 测试子进程泄漏 | 2026-10-03 census 曾累积 424 orphan；缺 stream 正常通过也遗留三个 shell。PROC-LEAK 添加 private-path / ancestry / pidfd / start-ticks GuardProcessDirectory 与独立 group cleanup。12 次最终 package census 零 survivor；缺 stream 回归 rc0；七条 on-disk mutation rc1。SIGKILL 可绕过 cleanup 的限制仍保留。 | [result-proc-leak.md](/home/weiland/s5wt/prompts/result-proc-leak.md) |
| activation log 文本钉 | prepareImageCut 委托导致 cmd/tether TestAgentLogFormatMatchesActivationRecords/pty-records 红；修正委托 + 最终 PrepareChecked 文本钉，三处 mutation 均红。后续 gates-3/5 的 test rc0。 | 本报告原有集成期记录；[硬闸](hot-update-evidence.md#172-硬闸与候选树) |
| drill-found 产品与观测 roots | 逐项修复、真实回归与 mutation 索引见 evidence §17.4；包括 CONTROL/DATA credit、PREPARE/current attachment、readiness、pre-SYN legacy custody、terminal receipt、home lag/deadline、raw socket SO_ERROR、heartbeat/revoke、committed height、observer EAGAIN 与 S3 calibration compatibility。fixture/judge 与产品分别登记，旧失败不改判。 | [产品明细](hot-update-evidence.md#174-真实-drill-暴露的产品缺陷与回归)、[result-drill-run.md](/home/weiland/s5wt/prompts/result-drill-run.md) |

## 4. 最终验证与结论边界

2026-10-11 用户裁决关闭 S5，将唯一剩余间歇 e2e 失败转入 **S6 / ENDPOINT-RESUME**。本报告的 Pass 指处置后按该裁决收尾，不表示最新 e2e 全绿，也不代替独立外审。

- gates-3 tree `ee2f1c28`：lint/test/e2e-parallel/gates 全部 native rc0。DRILL-RUN merge 后 gates-4 tree `4c55cb83`：test0，lint/e2e/gates2，转 GATES-FINAL-3 处置。gates-5 tree `f4309233`：lint0/test0/gates0，e2e-parallel **native rc2**，一个失败单元 `HotUpdateFlow:internal/tunnel`：`TestEndpointSurvivesSessionLossAndResumes/wire` 报 `invalid flow owner state`。count50 isolated/pair race 通过不能撤销矩阵红灯；S6 须在矩阵负载下复现、归因和关闭。[原始硬闸及身份](hot-update-evidence.md#172-硬闸与候选树)
- 最终 drill image `9d8a232c`：75 A/C/R/B/W/L GREEN0；39 X0/Y3/P3/F3 精确符合 manifest，后三者仍 PRODUCT-RED3；36/34/35/94 与完整 70/72 GREEN0；33 **INCOMPLETE4** 为预登记 #73 边界；两个 deploy negatives 均 ASSERT-FAIL1、outer control0，恢复原 A GREEN0。[同镜像原生收据](hot-update-evidence.md#173-drill-原生判词与完整回归)
- run279 的十个 whole-package race 检查（agentimage count5）、structural 与 Darwin cross-build 均 rc0；census 清空。freeze 最大 1.063s 超过 500ms 校准值，六臂 hard max stall 均 <5s；历史 FD transient 根因仍未证明。native Darwin 未运行，按 S4 L6 非阻断；联合两目标 live fuzz / P29 逐行 final validation 无足够回执，tasklist 保留未勾。[限制与资源](hot-update-evidence.md#175-资源校准变异与未覆盖边界)

S6 broker exec、S8 authority incarnation/legacy migration、S9 NATS/Caddy switching、S10 mixed rollout 不由本报告签收。
