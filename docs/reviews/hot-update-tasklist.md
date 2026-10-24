# 全栈热更新 tasklist

日期：2026-09-20。基线及授权见 [plan](hot-update-plan.md)。**本表按阶段收据签收，未勾选项必须保留；S5 于 2026-10-11 按用户裁决收尾，ENDPOINT-RESUME 转 S6，不表示全栈增量完成。** 所有实现及收据完成内审后停独立外审门。

**当前停止点：** S6 于 2026-10-12 按用户裁决收尾；gates-3 tree `3a9ec85a` lint/gates rc0，test rc2（1 项）/e2e rc2（3 units），四项归 S6-TESTFIX-2 转 S7 收口流程。37.A/A1/B/T/R、39.BR 与 75/36/39/34/35/33/94、70/72 deploy-tier 实跑 **NOT_RUN**，排入 combined S7-close quiet window；[证据 §18](hot-update-evidence.md#18-s6-broker-image-interim-receipts-2026-10-12)、[S6 review](hot-update-broker-image-review.md) 保留原红灯与未签收范围。S5 历史 carry 边界见[§17](hot-update-evidence.md#17-s5-flow-carrier-ss-最终收尾收据2026-10-11)，当前用户交接无开放 S5 carry。S7/S8/S9/S10 与全栈总项继续开放；S7 在其 frozen lead verdicts 下推进，所有 S7 产品结论仍须实际 receipts。


**S10 candidate coverage boundary (2026-10-21):** The A–H checks below are unsigned for the
current candidate. Their original wording and historical receipts remain intact; the manifest's
`historical_checked` column preserves the former check state. Administrative S0–S6 closure and
historical GREEN results do not sign the S10 candidate. Stable `s10-claim` keys and original-text
SHA256 digests bind all 59 top-level items plus the four C7 clauses to the
[coverage manifest](hot-update-fullstack-coverage.tsv). A green documentation gate checks honesty,
not E3 acceptance. All current required claims are NOT_RUN; P35/lead seals and certifies the final candidate.

## A. 规划与基线

- [ ] 阅读CLAUDE、需求/当前架构、升级/进程/隧道/传输实现与既有drill覆盖；区分旧修复收据和新功能。 <!-- s10-claim:A1 -->
- [ ] 固定三路独立草拟并交叉质疑；root把裁决写入plan §10。 <!-- s10-claim:A2 -->
- [ ] H1 接线三路复核：同进程职责分离+同PID exec；root先更新plan §3.1/§4.2，完整范围/预算/测试门保持。 <!-- s10-claim:A3 -->
- [ ] 记录用户“最彻底”的四层范围，运行层自身升级、原业务与新请求、全部旧版本退出判据。 <!-- s10-claim:A4 -->
- [ ] 把每能力test + 真实simcluster drill + 文档列为不可缺省放行门；计划/故障/首次迁入边界明确。 <!-- s10-claim:A5 -->
- [ ] 完成首轮验收profile校准、真实环境和artifact清单，冻结预算；不得把目标值当实测值。 <!-- s10-claim:A6 -->
- [ ] 归档三路设计原稿/交叉记录，建立[证据索引](hot-update-evidence.md)并引用起点46文件SHA快照；实现后逐候选追加差分/运行身份收据。 <!-- s10-claim:A7 -->

## B. H0 传感器与现状反例

- [ ] 原业务PID/start ticks/nonce与stdout/stderr/退出码oracle：34真实运行返回预期PRODUCT-RED，独立原输出/ledger/管理和未交付真实退出码均确认；见[基线报告](hot-update-process-baseline.md)。这是旧路径反例，不是热更新通过。 <!-- s10-claim:B1 -->
- [ ] 原SSH/TCP/PTY/SS双向序号、无应用重连、half-close/静默预算oracle。 <!-- s10-claim:B2 -->
- [ ] 同transferID、SHA/长度、原子提交/唯一终态oracle。 <!-- s10-claim:B3 -->
- [ ] 每请求新控制完成时延/副作用和全部旧运行映像退出oracle。 <!-- s10-claim:B4 -->
- [ ] 真实旧binary的reexec失败基线；各oracle有确定性负控制，非空性守卫。**S1**：drill 34 以真实 `syscall.Exec` 换代复现 PRODUCT-RED（2026-09-20 旧签名、2026-09-21 S1 签名 `ctl=owner_lost_rc74`）；drill 35 三臂用真实前一发布 `vendor/tether-prev`（v0.6.0 @ a3431a1，源码 worktree 构建）跑 N-1 双向，GREEN；oracle 负控制：probe selftest 65、r9d 205、prev-supply 29（`tests/run-all.sh`），非空性 `_stream_advancing` 双侧 + 每故障窗 ≥20 记录（[证据索引 §12.2](hot-update-evidence.md)）。 <!-- s10-claim:B5 -->
- [ ] 校核simcluster真实Caddy/WSS供给与独立资源，不让harness替产品实施交接。 <!-- s10-claim:B6 -->

## C. H1 运行层与进程

- [ ] 新增Linux显式状态/FD交接基础包，真实exec保原父子/pipe/TCP、失败恢复、CLOEXEC隔离；12个Test+1个Fuzz，真实/race/五轮资源和负控制收据见[证据索引](hot-update-evidence.md)。此项不包含上层安全点/业务接线。 <!-- s10-claim:C1 -->
- [ ] 稳定任务/资源ID、唯一owner/epoch、已接纳请求和退出日志。普通exec/PTY的sole-wait/signal和联合快照已初步接线；**S2（C-2）**：`execsession.Image` v1 + `agentimage.Bundle` v1（每域自带 version、`DisallowUnknownFields`、Version 不等即拒、字段账本 `test/architecture/testdata/image_field_inventory.txt` 只增）；任务/资源 ID 跨映像稳定（TaskImage.pid = registry record.PID = Manager row.ID，导入三段式互相覆盖校验）、导入出来的 runtime **生而 frozen、无 goroutine**、`Imported.Thaw` 是唯一激活点；phase 由 registry record 派生；收据 `TestBundleRoundTrip`/`TestRuntimeImageImportRejectsInvalidBundle`（40 行）/`TestRuntimeImagePreservesLiveChildAcrossExec`、变异 M1–M28 + G1–G3 全红（evidence §13）。**接线（bootstrap/G.1 联合快照含导入任务/可靠事件）归 S3。** <!-- s10-claim:C2 -->
- [ ] 版本化 manager 接口与完整资源所有权、候选无执行权、管理/续接入口身份与代号检查。**ABI 半 S2 已做**（`Lineage{Generation, BootID, ProtoVersion, InstanceID, RoutingNID}` 与 handoff envelope + `Role` 交叉核对、`Lineage.Matches`；候选无执行权 = 导入 runtime 无 goroutine 直到 `Imported.Thaw`）；**产品路径的入口身份/代号检查与接线归 S3。** **S3 已做（C-3）**：`agentboot.Image` 相位机 held/refused/imported/active/ready/lost 是映像唯一所有者，`New` 最后一步 Restore、`Run` 第一条语句 Activate（之前零 goroutine、绝不空表 register）；prev 侧 lineage 以 sealed bundle 为准（`adoptImageLineage`）；install 入口门按 `ownsPendingUpgrade`（target SID/NID + instance/boot proof + live image）判活 owner，marker 带 `generation/resource_phase/imported_resources`；候选执行权由 `checkCandidateABI` + `image-abi v1` 行 + prev ∈ {运行映像, 候选} 约束。收据 `TestImageHeldToReadyCarriesTheChild`、`TestAgentBootstrapRunsBootCheckBeforeRestore`、`TestImportFailureNeverRegistersEmptyTable`、`TestExpiredPendingAdmissionRequiresLiveOwnership`、`TestWatchdogMarkerGenerationMatchesRedispatchedBundle`、`TestInstallPrevSlotAcceptsOnlyRollbackCapableImages`；变异见内审报告 §5 与 [evidence §14](hot-update-evidence.md)。 <!-- s10-claim:C3 -->
- [ ] ctl稳定requestID/幂等与输出seq/ACK/resume，旧ctl能力不足的激活拒绝路径。**S1 已做 ctl 半**：`process-resume` 五 op、handle=lease+RequestID、断链续接（AttachEpoch 恒 1）、丢回复不重建、退出码 taxonomy（74）、连接级有界重试、`--stdin`；真二进制 e2e 14 条 + `TestReliableExecMatrix` 见[证据索引 §12](hot-update-evidence.md)。**未勾**：旧 ctl「能力不足的激活拒绝路径」——S1 裁决是 legacy fallback 而非拒绝（N-1 互通），由 drill 35 臂 C 守（2026-09-21 实跑 GREEN：C1 v0.6.0 ctl 走 legacy、丢 450/522 记录如实记录；C2 v0.6.0 agent 收 probe 后 ctl 退回 legacy、完整）。**S3 已做 legacy 半**：legacy exec 在场（gate 里的 `gateExec` 准入或表里无 owner 的 legacy child）时 `node upgrade` 以 `upgrade_deferred` + `legacy_session_busy` 同步拒绝、盘面不动（`TestPreflightDefersOnEachReasonCode` 的 `legacy_admission`/`legacy_child` 行）。PTY/run 的 seq/ACK/resume 归 S4，总表归 S8。 <!-- s10-claim:C4 -->
- [ ] 联合starting/courier/register准入屏障，覆盖spawn已成功但started尚未持久化时G.1误杀的既有并发窗口；register回复只能settle其实际携带的事件集，不能清除请求发出后新产生的exit。**S1 C-5**：单一 started 站点（`startExecPipes` 在 `AdoptDraining` 之前入队，带真实 identity 三元组）+ `LocalProcess.StartedUnacked`（additive）+ broker reconcile 两处 orphan 循环 skip + `localProcessSnapshot` 并入 pending started；收据：`TestReconcileDefersOrphanKillForStartedUnackedRow`、`internal/agent/process_admission_test.go`、变异 M2 红、wire-freeze 登记（[证据索引 §12](hot-update-evidence.md)）。register 回包按事件身份 settle 的那半在 S0 前已落（§22.2 末段）。 <!-- s10-claim:C5 -->
- [ ] 去重身份/payload/容量/保留期、跨代及回滚结果保留、过期未知拒绝重执行；晚重试故障窗。**S1 已做**：intent hash 去重、容量拒绝（`process_capacity`）、abandoned-terminal 有界保留（2 min grace / 15 min tombstone，`process_output_discarded`）、过期晚重试 `process_expired` 不重建、异实例 `process_instance_lost`；**未做**：跨代及回滚结果保留（随 image 导出，S2/S3）、running-abandoned 背压（S2）。 **S2（C-6）已做**：running-abandoned 8 MiB 封顶（D11：`AbandonedRunningGrace` 15 min 后截断已满的流、child 继续、`TestSweepDiscardsAbandonedRunningOutput`）；跨代及回滚结果保留 = tombstone/revocations/lostTerminal 随 image 导出并在导入后原样生效（`TestRuntimeImagePreservesTombstonesAndRevocations`）。 <!-- s10-claim:C6 -->
- [ ] 同PID可恢复exec、父子关系/Wait、安全点、白名单FD及状态校验。**S3 已做产品路径接线（C-7）**：`node upgrade` = install → gate 关 → 心跳 → Freeze-before-reply → `Frozen.Prepare`（重 Export）→ 同 PID `prepared.Exec` → 新映像 Restore/Activate/register；exec 失败先 Thaw 再盘面回滚；watchdog 回滚重导出**当前态**再派发 prev。真二进制 `TestNodeUpgradeCarriesLiveExecAcrossExec`、`TestNodeUpgradeGenerationChainKeepsLiveExec`、`TestNodeUpgradeDefersOnBusyResource`、`TestNodeUpgradeRefusesLegacyCandidateWithLiveTasks`；deploy-tier drill 34 七臂 GREEN（原 child PID/boot/startticks/nonce 不变、输出完整、原 ctl rc23、agent 同 PID 换 SHA，含一次 watchdog 回滚，[evidence §14](hot-update-evidence.md)）。PTY 域属 C-8（S4）。**ABI 半 S2 已做（接线 S3）**：真实同 PID exec `TestRuntimeImagePreservesLiveChildAcrossExec`（父进程真 `execclient.Run` 作 oracle：stdout 部分 ACK / stderr 未 ACK / stdin 未写完 / 后代持 pipe / ReapHeld 僵尸 / 冻结期 raw SIGKILL 僵尸，跨 exec 字节序与 rc 精确、PID/start ticks/pipe 引用不变、冻结期在途请求经重发续接）+ `TestBundleExecRestoresEveryDomain`；安全点 = `registry.Freeze` 的 ErrBusy（唯一「无在飞 spawn」判据）+ 泵屏障 + Handle 阻塞（D1）；白名单 FD = 每 task 六名 + `files{dev,ino}` 指纹 + `CheckReplayFile/CheckStreamFile`；running-abandoned 8 MiB 封顶（D11，`AbandonedRunningGrace` 15 min 后截断流、child 继续）。**未做**：产品路径接线（S3）、PTY（S4）。 <!-- s10-claim:C7 -->
  - [ ] 进程域导出仅允许frozen； <!-- s10-claim:C7.frozen -->
  - [ ] 版本化状态绑定owner PID/start ticks/boot ID，导入前验证每个未reap child的原parent/group/start ticks。 <!-- s10-claim:C7.identity -->
  - [ ] 终态仅恢复保留结果，绝不访问可能复用的OS PID。 <!-- s10-claim:C7.retained -->
  - [ ] 导入为冻结态，所有域就绪后才Thaw；任一校验失败不得部分安装、reap或signal。 <!-- s10-claim:C7.activation -->
- [ ] exec pipe、PTY、stdin/resize/KA、ps/kill、子树/退出码完整接线。 <!-- s10-claim:C8 -->
- [ ] runtime/协调器自身可升级；systemd/user unit/no-systemd生命周期和清场正确。**S3（C-9）**：drill 34 A（system unit）/ U（产品 `agentUserUnitBody` 原样渲染为 `User=sim` 的 system unit——按 S3 定稿裁决 Q4 是 **body-equivalent**，不证明 `systemctl --user`，该缺口归 drill 82 U3）/ N（setsid+nohup 无 systemd，nonce pidfile、fd2 inode 不变、pidfd 钉住清场）2026-10-02 实跑 GREEN，同 PID、MainPID 不变、无残留；no-systemd 臂另由 S8 复核（stages plan C-9 行）。 <!-- s10-claim:C9 -->
- [ ] 补齐已发现的session_destroyed清场需求缺口；真实exec/PTY/后代与删除/freeze竞争测试，撤权tombstone不得因导入复活。**S1 已做 exec 半（C-10）**：`session_destroyed` → `Runtime.Revoke`（SIGKILL 进程组 + tombstone + 拒绝撤权前 lease）+ `reapManagedChildren`（legacy exec/PTY），daemon 不退；收据 `TestSessionDestroyedRevokesReliableAndLegacyChildren`、`TestRevokeKillsLiveTasksAndRefusesPreRevocationLeases`、真二进制 `TestExecReliableSessionRmMidStreamKillsChild`、变异 M4 红。**未做**：freeze 与删除竞争的 (b)/(c) 臂与「tombstone 不因导入复活」（S2 schema + S3 `TestSessionDestroyedTombstoneSurvivesImport`）。 **S2 已做 (b) 臂**：冻结期 `Revoke` 只登记、kill 由 `Thaw` 补发且 Manager 未 thaw 即先判后拒（`TestRuntimeFrozenFenceBlocksUntilThaw`，变异 M13/M27 红）；tombstone/revocations 随 image 导出并在导入后原样生效（`TestRuntimeImagePreservesTombstonesAndRevocations`，M4 红）。(c) 臂与「导入不复活」的产品接线归 S3。**S3 已做 (c) 臂**：撤权 tombstone 随 bundle 跨同 PID exec 存活、导入后旧 lease 仍拒（`TestSessionDestroyedTombstoneSurvivesImport`）；冻结 cut 期间到达的撤权经 `Frozen.Prepare` 重 Export 进 bundle（`TestFrozenPrepareCarriesRevocationsMarkedAfterFreeze`）。**未勾**：PTY 变体归 S4。 <!-- s10-claim:C10 -->
- [ ] 单测/真进程集成、race/≥5轮资源门、对应真实drill及文档全部有收据。 <!-- s10-claim:C11 -->

## D. H2 连续流与代理

- [ ] 稳定streamID、双向offset/ACK/重放、FIN、限额/背压。S5 D-1 收据：FIX-FLOW (receipt id: `/home/weiland/s5wt/prompts/result-fix-flow.md`)、INTEG-VERIFY (receipt id: `/home/weiland/s5wt/prompts/result-integ-verify.md`)、run279 flow race (receipt id: `/home/weiland/tether-hot-update-handoff-20260920T2356Z/s5-evidence/drills/run-279/flow-race/receipt.json`)。 <!-- s10-claim:D1 -->
- [ ] 两端raw socket可交接，内部TLS/yamux/WSS可替换，原外部socket不关闭。 <!-- s10-claim:D2 -->
- [ ] OPEN中/空闲/活跃/慢接收/half-close/多流/迁移中撤权。S5 场景收据：T39–T45 (receipt id: `/home/weiland/s5wt/prompts/result-hotupdate-e2e.md`)、SS-REVOKE (receipt id: `/home/weiland/s5wt/prompts/result-ss-revoke.md`)、最终 75 A/C/R/B/W/L (receipt id: `/home/weiland/tether-hot-update-handoff-20260920T2356Z/s5-evidence/drills/FINAL-ACCEPTANCE-ARMS.json`)；Endpoint 矩阵间歇失败另列 S6 ENDPOINT-RESUME。 <!-- s10-claim:D3 -->
- [ ] home/pins/port与旧epoch围栏继续有效。 <!-- s10-claim:D4 -->
- [ ] SS AEAD状态/nonce/盐/replay filter接管、秘密保护和解析fuzz。 <!-- s10-claim:D5 -->
- [ ] 真协议自动测试、race/资源、真实SSH/TCP/SS drill、完整退场证据与文档。 <!-- s10-claim:D6 -->

## E. H3 文件传输与broker状态

- [ ] tier A/B push/pull稳定版本/NUID、prepare checkpoint、可信temp/parent身份。 <!-- s10-claim:E1 -->
- [ ] 持久进度、摘要恢复、同操作续传、取消/预算、唯一rename/终态。 <!-- s10-claim:E2 -->
- [ ] broker tracker/watchdog/commit/GC/reaper责任同事务交接。 <!-- s10-claim:E3 -->
- [ ] Raft/SQLite/JS单writer、已ACK写不丢、不恢复旧快照抹写。 <!-- s10-claim:E4 -->
- [ ] 覆盖prepare/读写/sync/rename/终态前后及重复消息/权限撤销。 <!-- s10-claim:E5 -->
- [ ] ≥66,549,440B与1GiB、双向并行、WSS/慢消费者、真实逐层升级drill与文档。 <!-- s10-claim:E6 -->

## F. H4 升级编排与兼容回滚

- [ ] 可查询事务状态，控制接管/业务迁移/全量完成分开，运行SHA证明。 <!-- s10-claim:F1 -->
- [ ] candidate预检、升级域/共址/共享binary sibling/两代限额。 <!-- s10-claim:F2 -->
- [ ] 逐交接窗口故障注入，明确旧owner仍在/已死的不同恢复语义。 <!-- s10-claim:F3 -->
- [ ] wire/manager状态ABI（含实际存在的IPC）/manifest/数据库/第三方配置N-1矩阵和跨纪元拒绝。 <!-- s10-claim:F4 -->
- [ ] 接管前/部分迁移后/新任务执行后回滚，无双执行/丢任务/假成功。 <!-- s10-claim:F5 -->
- [ ] legacy busy安全拒绝、完整idle清单+冻结接纳的首次迁入；无该能力则defer，覆盖idle后并发新任务反例及限制文档。 <!-- s10-claim:F6 -->
- [ ] 单测/真binary/资源/真实drill全部通过，状态和操作文档同步。 <!-- s10-claim:F7 -->

## G. H5 NATS/Caddy与拓扑

- [ ] 产品提供NATS/JS滚动路径、副本/可达性/鉴权readiness与实际版本/SHA检查。 <!-- s10-claim:G1 -->
- [ ] Caddy配置reload和binary升级分别完成；新入口就绪、内部WSS迁移、旧映像退出。 <!-- s10-claim:G2 -->
- [ ] N≥3/共址agent、N=1、N=2/quorum不足、replica落后各有明确行为。 <!-- s10-claim:G3 -->
- [ ] 单实例可用路径实际实现；defer只能记安全拒绝，不能冲抵成功场景。 <!-- s10-claim:G4 -->
- [ ] 无双开同一store；临时节点不绕过join版本门；新增入口也可升级。 <!-- s10-claim:G5 -->
- [ ] 外置NATS/真实Caddy TLS/WSS逐项升回滚drill、原业务/新请求和资源证据、文档。 <!-- s10-claim:G6 -->

## H. H6 全栈验收、文档和内审

- [ ] 真实CLI入口完整四层升级/回滚，持续混合业务，至少五代换代/资源回收。 <!-- s10-claim:H1 -->
- [ ] 延迟/吞吐/FD/内存/consumer/磁盘限额，原业务撤销/取消并发。 <!-- s10-claim:H2 -->
- [ ] 变异：旧reexec、丢checkpoint、错offset、提前close、双owner、漏探针、假运行SHA、旧worker永留均红。 <!-- s10-claim:H3 -->
- [ ] 每能力Test/真实drill臂/预算/身份/命令/rc/原始收据/活文档映射齐全；所有必要臂GREEN。 <!-- s10-claim:H4 -->
- [ ] requirements、当前architecture、usage、ops/runbook、testing/README/simcluster覆盖表同步真实实现。 <!-- s10-claim:H5 -->
- [ ] test/wire/CLI/inventory按规则更新，ctx/结构/豁免账本不放宽，新增门有正负控制。 <!-- s10-claim:H6 -->
- [ ] 同一最终候选 `make test` + 完整 `make e2e-parallel` + 单份 `make lint`；改门另`make gates`。 <!-- s10-claim:H7 -->
- [ ] 固定三路内审及交叉质疑，root逐条回复修复；必要复审/回归收据闭合。 <!-- s10-claim:H8 -->
- [ ] 最终报告首行Pass/Fail，写清疑惑、问题、建议、未覆盖与原始证据；INDEX更新。 <!-- s10-claim:H9 -->
- [ ] 核验仍未提交/部署，停独立外审门；不自行代签外审通过。 <!-- s10-claim:H10 -->

## 执行记录

2026-09-20：正式计划落盘。三路设计确认当前reexec、任务表、TLS/yamux carrier和broker传输ledger不具备目标连续性。全部热更新实现与测试保持未完成；旧缺陷修复的通过结果不计入本功能验收。

2026-09-20 后续：`internal/handoff`基础包已落盘并通过定向验证；尚未接入agent/broker/ctl。H0真实升级反例及Caddy供给草案已在`/tmp/tether-control-recovery/`完成独立本地oracle检查，仍待root整合/真实部署，不能记为simcluster已运行。总体目标继续，完整功能与外审出口尚未完成。

2026-09-20 S0（Claude 接手）：F1/F2 修复（唯一清场 owner + aborted 合同，见 [S0 plan](hot-update-stable-tree-plan.md)）；H-6 首次登记（test +54 / wire +49 / matrix +3 / anchor +3）；两处 QF1001 清理；三硬闸 + gates + lint 全绿。不勾功能项——S0 不新增任何产品能力。stage 分治见 [stages-plan](hot-update-stages-plan.md)。

2026-09-21 S1（可靠 exec，[plan](hot-update-reliable-exec-plan.md) / [内审](hot-update-reliable-exec-review.md)）：三端产品路径 + carrier 可换 + `vendor/tether-prev` 供给 + drill 35（三臂 GREEN 实跑）+ drill 34 重钉（PRODUCT-RED 新签名实跑）；内审原判 Fail → 61 条全部处置 → 三硬闸/gates 全绿；勾 B.5、C.5；C.4/C.6/C.10 记「S1 已做的半」。全部暂存，未 commit。

2026-09-21 S2（agent 运行层 image ABI，[plan](hot-update-agent-image-abi-plan.md)）：execsession fence（Freeze/Export/Import/Thaw/Discard/Detach，Handle 冻结期阻塞 D1，冻结期 exit 由新映像记账恰一次，running-abandoned D11 截断）+ `internal/agentimage`（Bundle v1 四域、Freeze 顺序、导入三段式 + Salvage、deferred 六码）+ spawnsafe 判词导出/`probesInFlight` fail-closed + courier outbox 往返；真实同 PID exec 两处（execsession：真 execclient oracle；agentimage：四域）；五包 `-race -count=3` 绿、Darwin 绿；变异 M1–M27 + G1–G3 全红；三小门（layering 行、ctx-free、image 字段账本）落地；C-2/C-6 勾、C-1/C-10 (b) 半写实。内审 → 修复 → 全部暂存见 evidence §13。


## S4 · pty-terminal 收据（2026-10-03）

按 [evidence §15](hot-update-evidence.md#15-s4-pty-terminal-证据骨架) 的实际 receipt 更新；当前hermetic tree与run5部署候选身份分列，不把历史mutation叫当前重跑。T01–T48为47完整/0部分/1 NOT_RUN（Darwin），T49/T50另完整；321条独立mutation收据，本次补跑51条全红，source已恢复。

- [x] C-4 PTY：两步capability、pre-createfallback、A/Q/credit、controlreceipt、mergedstdout/空stderr、terminal-ack、N-1四象限；T11/T12/T35–T41与run5 36A/C/L、39P，精确command/rc/raw见§15.2/15.6。
- [x] C-8 PTY：原master/foreground、stdin/resize/signal/KA、ps/kill/courier真exit；同SID多PGID/stopped清场；T08/T15/T16/T26/T28/T42及run5 36A/W/L。
- [ ] C-10 PTY：四事件窗口/register-gone/撤权优先/clockunknown/tombstone已有收据；**整体S4不勾**，L1 finalPrepare后事件丢失且同SID复建归S8。L2主动setsid脱离后代不覆盖。
- [x] C-11：行级baseline/mutation/restore、风险包整包race、≥5轮已有；全门rawreceipt（make test/e2e-parallel/gates）、D7全矩阵（D7-LEADER修后ALL PASS）、drill与门禁同一index tree绑定均由§15.8补齐（tree `0ad56120`→仅一单测文件lint修→`9c22ec37`，最终tree上三闸复跑rc0）。Darwin按L6豁免。
- [x] B-2 PTY子项：T35、run5 36A/W/C、39P原terminal双向bytes、原ctlwaitpid/无重建；B-2总项和SSH/TCP/SS仍归S5，不勾总项。
- [x] H1六缺口：EIO、FIN/Ctrl-D、control/Hangup fence、尾流/有限queue、kernelidentity/绝对期限、真出口各有正receipt与独立规定mutation红；旧survivor如实另列§15.2。
- [x] wire/image/domain：inventory实数18wire/58image新增、零删除；ABI1/exec-v1/pty-v1、kind-aware资源、真S3reader（source0ec45）、SHA能力/三次domain、pending/sibling，见T19/T23/T24/T30–T34/T45/T46。
- [x] stdio/取消：T43与T39；TTY/FIFO独立OFD、regularfile和/dev/null成功、socketpre-createlegacy；外部SIGINT/SIGTERM本地取消、join/termiosrestore，raw0x03与显式signal分列。
- [x] run5 36A/W/C/L及父36：native各rc0/GREEN、123pass；strictmixed新请求/停顿≤5s；freeze500ms只校准，当前race最大1080ms保留，见§15.4/15.5。
- [x] run5 39P及父39：真oldexporter+oldctl供给；oracle_rc1/native_rc3/PRODUCT-RED分存，waiver不改native；父X/Y/P joinPRODUCT-RED。**sweepprocessrc未保存，NOT_RECORDED**，不推断rc0。
- [x] run5回归：34七臂和35三臂各GREEN；33A/C/C2实际PASS，同时全33仍rc4/INCOMPLETE（既存gotcha#73）；保留各artifact/原始log/census，不勾掉#73。
- [x] 机械门/预算/清单的定向验证：T47实际gate名字、125methods、layering/ctx-free/leakanchor/enum/whole-package-race matrix/append-only inventories与Go↔shellpin的独立正负收据。**不冒称当前全量make门禁已取得rawreceipt。**
- [ ] Darwin交付组合：cross-build和testbinarycompile实际rc0；terminalio Darwin `[no test files]`；vet-tags用户给定green但当前完整raw未提供，保持组合未签。
- [ ] NOT_RUN · Darwin真legacyrun/resize/cancel/tail：未在Darwin实跑，无runner；按L6不阻塞S4出口。
- [x] P17文档负控：matrix删行、Darwin假PASS、39P假GREEN/隐藏native3均receipt-audit拒绝，restore通过；独立于Go docs门，详见§15.3。
- [x] 最终同候选身份/成本总签：§15.8 以 `s4-final.sh` 在同一index tree `0ad56120`上依次构建镜像 `50613d39`、跑drill sweep（NO DEVIATIONS，sweep processrc=1仅来自33 INCOMPLETE语义，已记录）、make test/e2e-parallel/gates；lint修后tree `9c22ec37`（与drill tree只差一单测文件）上三闸复跑全rc0。实际成本见drill-costs.tsv。内存瞬时峰值NOT_MEASURED（plan不列为出口要求，如实保留）。

## S5 flow-carrier-ss receipt boundary

The S5 implementation and fix round are reflected in the [live contract](../distributed-broker-architecture.md#228-s5-flow-and-ss-domains-binding-contract).
2026-10-11 按实际回执更新以下 S5 子项；[§16 历史 P28 骨架](hot-update-evidence.md#16-s5-flow-carrier-ss-receipt-index) 不代替[§17 最终回执](hot-update-evidence.md#17-s5-flow-carrier-ss-最终收尾收据2026-10-11)。最新 e2e 的 ENDPOINT-RESUME 按用户裁决转 S6，保留原红灯；未取得 exact final receipts 的项继续未勾。
This section does not alter dated S3/S4 checks above or sign the whole increment.

<!-- s5-tasklist:start -->
- [x] S5 B-2: agent 切换的原 SSH/TCP/SS 与原 S4 PTY 子项；同最终 image 9d8a232c 的 byte/identity/Accept ledgers，75 A/C/R/B/W/L、36 A/W/C/L 与 39.P 原 native pin；FINAL-ACCEPTANCE-ARMS.json (receipt id: `/home/weiland/tether-hot-update-handoff-20260920T2356Z/s5-evidence/drills/FINAL-ACCEPTANCE-ARMS.json`)、271/274 (receipt id: `/home/weiland/tether-hot-update-handoff-20260920T2356Z/s5-evidence/drills/FINAL-DEPLOY-SUMMARY.md`)。不外推 broker/基础设施切换。
- [x] S5 D-1: frame/applied ACK/dedupe/OPEN retirement/FIN/abort/bounded replay；P1 55 个 executable mutants (receipt id: `/home/weiland/s5wt/prompts/result-p1.md`)、P2 codec/golden/fuzz (receipt id: `/home/weiland/s5wt/prompts/result-p2.md`)、FIX-FLOW 回归及 18 intended mutants (receipt id: `/home/weiland/s5wt/prompts/result-fix-flow.md`)、INTEG-VERIFY whole flow race / rebased mutations (receipt id: `/home/weiland/s5wt/prompts/result-integ-verify.md`)、run279 flow-race0 (receipt id: `/home/weiland/tether-hot-update-handoff-20260920T2356Z/s5-evidence/drills/run-279/flow-race/receipt.json`)。OPEN 配额修正及 C1 typed capacity/FIN/recovery 见 HANDOFF (receipt id: `/home/weiland/tether-hot-update-handoff-20260920T2356Z/s5-evidence/drills/HANDOFF.md`)；早期 P0 stub red 不作通过凭据。
- [x] S5 D-2 agent subitem: 原 raw FD/replay/image 跨同 PID exec 与 rollback；T39/T41/T44/T45 三轮 race (receipt id: `/home/weiland/s5wt/prompts/result-hotupdate-e2e.md`)、private-state / current-cut regressions (receipt id: `/home/weiland/s5wt/prompts/result-private-state.md`)、最终 75 A/B/W (receipt id: `/home/weiland/tether-hot-update-handoff-20260920T2356Z/s5-evidence/drills/run-271/75-parent/receipt.json`)；broker Endpoint/listener/fence exec 留 S6，顶层 D-2 仍未勾。
- [x] S5 D-3: ALPN/六字段 REGISTER/exact OK/bufio、N-1 四象限、permit/fresh authority/fences；P3 14 个 mutations (receipt id: `/home/weiland/s5wt/prompts/result-p3.md`)、FIX-BROKER (receipt id: `/home/weiland/s5wt/prompts/result-fix-broker.md`)、FIX-FLOW permit/FIN (receipt id: `/home/weiland/s5wt/prompts/result-fix-flow.md`)、run279 tunnel/broker whole-race (receipt id: `/home/weiland/tether-hot-update-handoff-20260920T2356Z/s5-evidence/drills/HANDOFF.md#tests-and-native-receipts`)、75.L (receipt id: `/home/weiland/tether-hot-update-handoff-20260920T2356Z/s5-evidence/drills/run-271/75-parent/receipt.json`)、39.F native3 (receipt id: `/home/weiland/tether-hot-update-handoff-20260920T2356Z/s5-evidence/drills/run-273/39-parent/receipt.json`)。legacy pin 不转写成 GREEN。
- [x] S5 D-4 same-broker subitem: Endpoint/RESUME/reaper 固定回归、SQL/home/port/epoch proofs；FIX-BROKER (receipt id: `/home/weiland/s5wt/prompts/result-fix-broker.md`)、run279 (receipt id: `/home/weiland/tether-hot-update-handoff-20260920T2356Z/s5-evidence/drills/HANDOFF.md#tests-and-native-receipts`)、75.C (receipt id: `/home/weiland/tether-hot-update-handoff-20260920T2356Z/s5-evidence/drills/FINAL-ACCEPTANCE-ARMS.json`)、gates-final3 count50 (receipt id: `/home/weiland/s5wt/prompts/result-gates-final3.md`)。**例外：ENDPOINT-RESUME** 最新 gates-5 e2e (receipt id: `/home/weiland/tether-hot-update-handoff-20260920T2356Z/s5-evidence/gates-5/e2e-parallel.log`) invalid flow owner state 仍红，用户裁决转 S6；broker image/maintenance/exec 与顶层 D-4 留 S6，未勾。
- [ ] S5 D-5: SS actor/parser/AEAD/material FIFO/rotation/revoke/secrets/resources 已有 FIX-SS (receipt id: `/home/weiland/s5wt/prompts/result-fix-ss.md`)、SS-REVOKE (receipt id: `/home/weiland/s5wt/prompts/result-ss-revoke.md`)、PRIVATE-STATE (receipt id: `/home/weiland/s5wt/prompts/result-private-state.md`)、T42/T43 及最终 75/72 (receipt id: `/home/weiland/tether-hot-update-handoff-20260920T2356Z/s5-evidence/drills/HANDOFF.md`) 收据；P2 (receipt id: `/home/weiland/s5wt/prompts/result-p2.md`)/P9 (receipt id: `/home/weiland/s5wt/prompts/result-p9.md`) 仅证明分别 live fuzz。尚缺 final 两目标联合 live invocation 与精确集合回执，不勾整项；顶层 D-5 同样未勾。
- [ ] S5 D-6 agent subitem:75/70/72、S4/exec regressions、whole-race/count5 收据见 HANDOFF (receipt id: `/home/weiland/tether-hot-update-handoff-20260920T2356Z/s5-evidence/drills/HANDOFF.md`)；gates-5 (receipt id: `/home/weiland/tether-hot-update-handoff-20260920T2356Z/s5-evidence/gates-5/rc.txt`) e2e native2 携 ENDPOINT-RESUME，docs audit / P29 逐行签录仍有缺口，未勾全项。broker continuity/drill 与顶层 D-6 留 S6。
- [ ] S5 six exits: 用户裁决关闭阶段，不等于六出口逐行全绿；P29 final validation 文件与 §16 逐项签录、联合 live fuzz 缺回执，最新 e2e 红见 gates-5 (receipt id: `/home/weiland/tether-hot-update-handoff-20260920T2356Z/s5-evidence/gates-5/rc.txt`)。不批量签收。
- [ ] S5 Darwin native: no runner，NOT_RUN，S4 L6 非阻断；run279 darwin-cross-build0 (receipt id: `/home/weiland/tether-hot-update-handoff-20260920T2356Z/s5-evidence/drills/run-279/darwin-build/receipt.json`) 单列，不能代替 actual lifecycle execution。
<!-- s5-tasklist:end -->

Do not sign C-10's PTY same-SID recreation window (S8), broker same-PID exec (S6), NATS/Caddy switching (S9)
or the final five-generation mixed rollout (S10) from S5 data-plane receipts.
README documentation audit 本次 rc1，首个失败为 frozen plan changed（当前/期望 digest 见[§17.6](hot-update-evidence.md#176-s5-docs-本地检查与-audit-缺口)）；completion 检查尚未执行。其规则仍要求指定 P29 validation 文件及全部 123 个非 native-Darwin rows COMPLETE，无法表达本轮部分签收与用户携带项裁决。保留原 guard，实际运行结果另报；请求 owner 为逐项所需 receipt 集合及显式 carried status 更新 audit，不能伪造 validation / COMPLETE 使之通过。

## S6 broker-image receipt boundary

**S6 closes now under the user ruling, 2026-10-12: Pass（处置后；4 项测试失败与 drill 37/39.BR 实跑转入 S7 收口窗口）.**
[Evidence §18](hot-update-evidence.md#18-s6-broker-image-interim-receipts-2026-10-12) and the
[review](hot-update-broker-image-review.md) retain 45 original dispositions and all three lead gate rounds.
The separate candidate worktree `/home/weiland/s6cand`, tree `3a9ec85a133da6573b099c7a91dd6649009b06c8`,
has its patches mirrored into the integration tree. Gates-3 lint/gates rc0; test rc2 (one failure), e2e rc2
(three failed units). S6-TESTFIX-2 owns the four failures in S7. All S6 deploy drills are NOT_RUN and
scheduled in the combined S7-close quiet window; remaining identity/cost/receipt reconciliation follows there.
Earlier gate reds remain historical. Gate wall times/package calibration do not provide final marginal costs.

P34 owns this aggregate index; P35/lead still owns receipt certification. The designated
`hot-update-broker-evidence.md` P35 index is absent; §18 records actual outcomes and the administrative closure,
not six-exit certification. Each COMPLETE row still requires the reviewed raw artifact and final source/artifact
SHA pair. The unchanged audit checks reference shape/existence, not product behavior. All eight aggregate
rows remain NOT_RUN because final certification has not run; available partial executions are listed separately.
No S6 tick closes B-4/D-6 totals, cross-layer coverage, S7/S8/S9/S10 or the whole increment.

### Available S6 subitem receipts (partial scope)

These checks acknowledge only the named completed package work and reported runs. They do not tick
E-4/D-2/D-4/D-6/B-4 totals, coverage rows or the six AND exits. Gates-3’s four failed tests and absent deploy receipts remain separately unchecked below.

- [x] Internal-review disposition record: four original reports (9 BLOCKER / 20 MAJOR / 16 MINOR), all 45 identities and D1–D3/L1–L7 mapped to fixes, regressions, mutations and explicit validation gaps; [review](hot-update-broker-image-review.md); user-approved closure and [INDEX](INDEX.md) row now recorded, with four failures/native drills carried to S7.
- [x] Single/N1 Run-internal exec and current-state lifecycle subitem: five rounds of actual serve cut, current ACK-before-replay, continuous flock, prepared revoke, exec-error reopen and ACTIVE rollback/normal stop; D2 report (receipt id: `/home/weiland/s6wt/prompts/s6-d2-complete-report.md`), core cut log (receipt id: `/tmp/s6-d2-real-cut-final-proof.log`), lifecycle log (receipt id: `/tmp/s6-d2-p28-p26-final.log`), native rc0; 37 accepted mutants rc1. This does not sign native 37.R/T or full E-4.
- [x] N3 maintenance/home integration subitem: real three-process voters, actual agent capability/ACK, signed driver, original TCP/target socket/home/epoch and fixed original history/cache; five rounds native0 in N3 receipt (receipt id: `/tmp/s6-n3-maint-n3-proof-final.log`), report (receipt id: `/home/weiland/s6wt/prompts/s6-n3-maint-report.md`); 16 intended mutants rc1. The original-TCP test is narrower than deploy 37.B.
- [x] T48 full original-flow cohort package subitem: five single + five sole-voter + five quorum rounds, real SSH foreground child/TCP/SS, pending OPEN, original socket/Accept census, exact bytes/FIN/tail and fresh connects; EXITS-2-3 report (receipt id: `/tmp/s6-exits23-receipts/REPORT.md`), final T48 log (receipt id: `/tmp/s6-exits23-t48-release.log`), native0. Max gaps 1.988/2.725/2.592s; unchanged hard5s and reported >500ms calibration. Gates-1 overlay build failures remain historical; HARDGATE-B rebuilt those overlays and gates-3 has no nested-build failure, while its two hotupdate failures remain below.
- [x] P27 and binding S1 compatibility package subitems: absolute budget, real blackout/one-effect/attempt census, maintenance delivery and old-broker/old-agent/run fallback; P27 log (receipt id: `/tmp/s6-exits23-p27-complete.log`), N-1 log (receipt id: `/tmp/s6-exits23-nminus-stable.log`), native0; EXITS-2-3 (receipt id: `/tmp/s6-exits23-receipts/REPORT.md`) records nine accepted mutations rc1. No real-old deploy roll is inferred.
- [x] rv-ctl-11 busy-admission/fresh-freeze unit subitem: real busy handler returns typed defer within RPC, separate preflight/freeze deadlines; COHORT report (receipt id: `/tmp/s6-cohort-validation/REPORT.md`) records both named store_reload_admission tests green and busy/long-join/deadline mutants rc1. Native 37.T transfer oracle NOT_RUN; carried into the combined S7-close quiet window.
- [x] Scoped gate repair checks: GATES-S6-E determinism/architecture and whole cmd/execsession/tunnel race rc0; report (receipt id: `/home/weiland/s5wt/prompts/result-s6-gates-e.md`), raw gate check (receipt id: `/tmp/s6-gates-e-gates-verified.log`), 34/34 mutants rc1. HARDGATE-A tagged vet/scanner/race/build checks rc0, report (receipt id: `/home/weiland/s5wt/prompts/result-s6-hard-a.md`), 18 assertion-red plus one intended compile-red mutation. Original gates-1 remains red; later gates-3 supplies lint/gates rc0 but does not certify final platform/drill acceptance.
- [x] S6 domain post-edit execution subitem: explicit four-domain S5 and five-domain S6/real-emitter expectations executed in HARDGATE-B’s whole agent exact-matrix race rc0, report (receipt id: `/tmp/s6-hard-b-evidence/REPORT.md`), and gates-3 whole agent runs rc0, test log (receipt id: `/home/weiland/tether-hot-update-handoff-20260920T2356Z/s6-evidence/gates-3/test.log`). Original GATES-E red remains historical; no dedicated domain-expectation mutant receipt located, carried for S7 reconciliation.
- [ ] Final all-six-exit assembly: NOT_RUN final source/artifact/running triple, complete deploy oracles/resources and marginal costs; carried into S7 receipt assembly ([§18.4](hot-update-evidence.md#184-closure-receipt-status-and-s7-carry)).
- [ ] 37.A/A1/B/T/R and 39.BR: **NOT_RUN**, scheduled in combined S7-close quiet window; no native positive/old-unwind negative receipt yet.
- [ ] Final 75/36/39/34/35/33/94 and 70/72 regressions: **NOT_RUN**, scheduled in the same combined S7-close quiet window.
- [ ] ENDPOINT-RESUME receipt reconciliation: current user handoff identifies no open S5 carry and gates-3 has no invalid-flow-owner-state failure; dedicated mechanism/mutation closure receipt absent, aggregate certification NOT_RUN / carried into S7. S5 gates-5 red (receipt id: `/home/weiland/tether-hot-update-handoff-20260920T2356Z/s5-evidence/gates-5/e2e-parallel.log`) remains historical red.

- [x] Gate-repair disposition receipts: HARDGATE-A/B, FIX-BLACKOUT/-2, FIX-ROLL, FIX-ORPHAN, FIX-MATRIX, SUPERVISE-CLI, LINT-A..F, TESTFIX and LEDGERS all recorded with native results/mutations/limits in [review §3](hot-update-broker-image-review.md#3-post-review-integration-findings). Earlier cumulative timeouts, unsuccessful latency rounds and mutation survivors remain visible.
- [x] Final lint subitem: gates-3 native0, 2s, zero issues; raw log (receipt id: `/home/weiland/tether-hot-update-handoff-20260920T2356Z/s6-evidence/gates-3/lint.log`), identity (receipt id: `/home/weiland/tether-hot-update-handoff-20260920T2356Z/s6-evidence/gates-3/identity.txt`).
- [x] Separate final gates subitem: gates-3 native0, 539s, vet-tags/proto/determinism/architecture/parallel/hermetic checks; raw log (receipt id: `/home/weiland/tether-hot-update-handoff-20260920T2356Z/s6-evidence/gates-3/gates.log`). Hermetic checks do not sign deployment.
- [ ] Final make test: gates-3 native2; `internal/execsession: TestFreezeRollbackBoundsRuntimeAccountingLock`, `freeze never closed admission`; S6-TESTFIX-2, carried into S7.
- [ ] Final make e2e-parallel: gates-3 native2, three units; S6-TESTFIX-2, carried into S7. `TestBrokerLifecycleSeamLeakCycles` REGISTER/session_closed; `TestHalfCloseTailAcrossRealExec` **real expose: exit status 64**; `TestProxyDisableDuringTunnelDropStaysDown` connectable in no-resurrect window. [Exact four observations](hot-update-broker-image-review.md#4-carried-items-and-verdict-boundary).

- [x] S6-DOCS-1 local documentation checks: Docs/Layout, Origin/Promised/Version and exact S6 README audit native0; five independent negative controls native1/restored0. [Local evidence §18.5](hot-update-evidence.md#185-documentation-only-verification). No stage exit or new test/ledger row is signed.

- [x] S6-DOCS-2 Origin/Promised/Version and exact S6 README audit: native0; five independent audit mutations native1/restored0; [§18.6](hot-update-evidence.md#186-s6-docs-2-closure-document-verification). Four owned closure docs updated; no product or ledger changes.
- [ ] S6-DOCS-2 Docs/Layout map reconciliation: native1, `test/README.md` lacks fullstackharness/infraharness/transferharness rows; exact outside-owner request in [§18.6](hot-update-evidence.md#186-s6-docs-2-closure-document-verification). This integration-tree later-stage map debt does not rewrite gates-3.

### Final aggregate claims (unsigned; carried into S7 receipt assembly)

<!-- s6-tasklist:start -->
- [ ] S6 E-4: T05–T17/T37/T49/T51;37.A/A1/B/R: current-store ACK prefix before replay, single writer/flock and independent unchanged JS witness; NOT_RUN final P35 certification; carried into S7’s combined close window.
- [ ] S6 D-2-broker: T18–T28/T48;37.A/A1/B/R: original listener/raw OFDs, pending OPEN/FIFO, half-close/slow tail and no application reconnect; NOT_RUN final P35 certification; carried into S7’s combined close window.
- [ ] S6 D-4-broker: T24–T28/T38–T44;37.B/R: full allocation CAS, five fences/ref union, all observer ACKs, fixed expiry and home/dwell history; NOT_RUN final P35 certification; carried into S7’s combined close window.
- [ ] S6 D-6-broker: T01–T58;37/39.BR and S3/S4/S5 regression receipts, whole-race/leak/mutations, native verdicts and marginal costs; NOT_RUN final P35 certification; carried into S7’s combined close window.
- [ ] S6 B-4-broker: T29–T36/T50;37.A/A1/B/R: every10/s slot/attempt, auth blackout, true process start/effects, ≤5s and running-image exit; NOT_RUN final P35 certification; carried into S7’s combined close window.
- [ ] S6 coverage-4-5-8: Broker portions only: original SSH/TCP/SS carrier and current persistence; S7 tracker/S9 infra/S10 cross-layer totals remain open; NOT_RUN final P35 certification; carried into S7’s combined close window.
- [ ] S6 gates-platform: Three hard gates plus separate gates, full matrix/controls, Linux and Darwin build/native receipts; Gates-3 lint/gates0 and four test failures; full acceptance/native Darwin NOT_RUN, carried into S7.
- [ ] S6 ENDPOINT-RESUME: Historical S5 invalid-flow-owner-state receipt; no open S5 carry per user handoff; dedicated closure receipt absent; NOT_RUN final P35 certification; carried into S7’s combined close window.
<!-- s6-tasklist:end -->

<!-- s6-receipts:start -->
| Claim | Status | Source SHA | Artifact SHA | P35-reviewed raw receipt |
|---|---|---|---|---|
|E-4|NOT_RUN|-|-|-|
|D-2-broker|NOT_RUN|-|-|-|
|D-4-broker|NOT_RUN|-|-|-|
|D-6-broker|NOT_RUN|-|-|-|
|B-4-broker|NOT_RUN|-|-|-|
|coverage-4-5-8|NOT_RUN|-|-|-|
|gates-platform|NOT_RUN|-|-|-|
|ENDPOINT-RESUME|NOT_RUN|-|-|-|
<!-- s6-receipts:end -->


## S7 transfer-continuation receipt boundary

The [frozen plan](hot-update-transfer-continuation-plan.md) lead LQ1–LQ3/L15–L18 overrides its body.
P36 documents contracts; it does not certify their implementation. **Final S7 product acceptance is
PENDING / NOT_RUN** until P37 publishes the final reviewed index at
`docs/reviews/hot-update-transfer-continuation-receipts.md` (not present in this snapshot).
P34 T54 is installed in transfer_state_inventory_test.go; its execution is not a product receipt.
P30's final coverage table and P37 receipts remain dependencies, not inferred evidence.
The [bounded audit](../../test/hotupdate/README.md#s7-documentation-audit) checks these rows separately.

Only the following portions may be signed for S7; retain the unqualified B/E parent checks above until
their whole scope is reviewed. E-4 single-writer belongs to S6. No F/G/H total or S8/S9/S10 exit is signed.
Each COMPLETE row must link a P37-reviewed aggregate receipt with all plan §0 fields and actual child/mutation/cut
raw artifacts, not this table or the receipt index itself. It must use the same final
source+dirty candidate; raw contents and complete requirements require human review.

<!-- s7-claims:start -->
- [ ] S7 B-3-transfer: Original invocation/ID/NUID, size/SHA, original OFDs, synced prefixes, one primitive effect and exact physical START/TERMINAL; T01–T25/T33–T40/T47/M01–M14; PENDING P37.
- [ ] S7 B-6-transfer: Actual Caddy/WSS, independent four buckets/two sizes/two directions/both live legs at every required hop;63.A/A 1/B/C1/C2/C3/R1/R2/X/W1/W2; PENDING P30/P37.
- [ ] S7 E-1: Tier A/B stable version, prepare/source/tmp/parent checkpoint and custody; T01–T11/T14/T17/T19/T35/T40/T47; PENDING P37.
- [ ] S7 E-2: Durable progress/hash/original budget, same-operation continuation, cancel/Grant and unique primitive/terminal; T08–T19/T32–T38/T45; PENDING P37.
- [ ] S7 E-3: Tracker/watchdog/Grant/ledger/history/finalizer/GC and both TTL List passes carried in one cut before reaper; T20–T31/T36–T39; PENDING P37.
- [ ] S7 E-5: Every prepare/IO/Sync/primitive/courier boundary, duplicate/revoke/cancel, LQ1 no-flag75/no-delete and explicit failed/outcome-unknown abandonment; T12/T13/T31/T35–T45; PENDING P37.
- [ ] S7 E-6: 66,549,440 B and 1 GiB four-cohort drill plus 61 all 41,37.T single/N3,39.BR native3,64 all 23 controls; whole race/direct leaks/inventories/gates/platform/measured costs and docs; PENDING P30/P34/P35/P37.
<!-- s7-claims:end -->

<!-- s7-receipts:start -->
| Claim | Status | Source SHA | Artifact SHA | Running SHA | Native rc | P37-reviewed raw receipt |
|---|---|---|---|---|---|---|
| B-3-transfer | NOT_RUN | - | - | - | - | - |
| B-6-transfer | NOT_RUN | - | - | - | - | - |
| E-1 | NOT_RUN | - | - | - | - | - |
| E-2 | NOT_RUN | - | - | - | - | - |
| E-3 | NOT_RUN | - | - | - | - | - |
| E-5 | NOT_RUN | - | - | - | - | - |
| E-6 | NOT_RUN | - | - | - | - | - |
<!-- s7-receipts:end -->

The six AND exits map separately: object→E-1/B-3; worker→E-2/E-5/B-3;
tracker→E-3; ctl→E-2/E-5; deployment→B-6/E-6; discipline→E-6.
All six are NOT_RUN. Native negative receipts are children of a reviewed positive aggregate, never
rewritten to 0. The aggregate's evidence must include source/artifact/running identity, raw oracle input
and reader digests, each required mutation's original red, restored green and each applicable cut proof.
Source/config/test-judge changes require affected product receipts to be rerun before signature.

P36 local audit/control results are documentation-only and are reported by the package worker; they do
not mark any product row COMPLETE. No runtime test, inventory, leak anchor or matrix row is introduced by P36.


## S9 infrastructure receipt boundary

The [frozen S9 plan](hot-update-infra-nats-caddy-plan.md) lead verdicts govern these five AND exits.
P38 records claim scope; [P39](hot-update-infra-nats-caddy-plan.md#p39--最终候选收据与关闭--hard) supplies reviewed native receipts for one final source/artifact/running
candidate. Installed source gates and local controls certify only their own assertions. They do not
sign infrastructure continuity, a deployment drill, native Darwin, or the whole increment.
All five product exits remain unchecked and NOT_RUN until their own complete receipts exist.
Current CLI/source availability is documented in usage and architecture §22.11–§22.13; it signs no
S8/S9/S10 exit. 38/39/85/86/99/100 live runs, final-candidate three hard gates and Darwin native are
NOT_RUN/PENDING. S10 P34/P35 own final coverage/receipts; A–H totals stay open.

<!-- s9-claims:start -->
| Exit | Exact unsigned scope | Native receipt |
|---|---|---|
| E1 | [ ] Infrastructure transition and recovery: fail-closed readiness, LDM boundaries, same-store exclusion, exact-release join, lost replies and old-process retirement; P28 T24–T31/T59–T63. | NOT_RUN |
| E2 | [ ] Real pinned NATS publish/request lost ACK, consumer retirement/recovery, ordinary auth subscriptions and checkpoint preservation under replica shortage/JS503; P28/P30 T38–T43/T59–T63. | NOT_RUN |
| E3 | [ ] 85.A/A1/B upgrade, rollback and re-upgrade with original SSH/TCP/SS/PTY/exec/four transfers, current/retired process identities, every 10/s control slot within 5s and durable progress within 10s; independent N2/quorum/lag refusals; P32/P33. | NOT_RUN |
| E4 | [ ] 86.R reload and 86.B binary cuts, first retrofit, healthy N2 and bad config/cert arms; actual WSS migration, canonical TLS, original sockets, old/temporary retirement and current ACME state; P29/P33. | NOT_RUN |
| E5 | [ ] Native 32/73/98 regressions, three hard gates plus separate gates, structural/inventory/enum/error/context/race/leak/tag/pin/ping controls, hermetic install supply, supported artifacts and separate Darwin evidence; P34–P39. | NOT_RUN |
<!-- s9-claims:end -->

## S10 coverage and receipt contract (unsigned)

The [frozen S10 plan](hot-update-fullstack-acceptance-plan.md) lead L15–L17 and §13 govern
this closure. The [TSV manifest](hot-update-fullstack-coverage.tsv) contains 12 capability rows,
16 requirement rows, 59 A–H top-level items and four C7 clauses, S0–S9 intake, P21–P33 deliveries,
seven explicit carries and the final candidate seal. These are required keys, without success claims.
A1–A5/A7 retain planning history; A6 requires current measured calibration. Every H item has its
own required receipt key. C7 requires all four children; a parent check cannot cover a missing child.

The TSV header is the schema. Lists use commas; `-` means absent. `source_heading` plus the
SHA256 of each original UTF-8 bullet text (without checkbox or stable-key comment) detects scope
drift. A text change requires manual mapping review. `historical_checked=yes` preserves an old
checkmark, without using it as current evidence. The gate independently pins the required row set
and the capability-to-R/leaf unions from §13.1. `upstream_owner` supplies the evidence;
`pending_owner` names the exact outstanding package; `final_owner=S10/P35` certifies the candidate.
Test names and drill selectors are obligations from the frozen plan, without an execution claim.
Wildcard drill selectors require every applicable child from the upstream frozen catalog.

To sign a row, P35 first supplies the external final-candidate seal bytes and their SHA256 as
both `candidate_id` and `receipt_sha256` on `FINAL`. Every COMPLETE claim uses that same ID and
every `required_receipt_keys` member must have a separate COMPLETE `receipt` row. Its absolute
external `receipt_path` names JSON whose bytes match `receipt_sha256`. JSON contains
`receipt_key`, `candidate_id`, `evidence_kind`, exact `command` argv, original integer `native_rc`,
`raw_path`, `raw_sha256`, `source_commit`, `artifact_sha256`, `running_sha256`, `artifact_set`,
and `releases`. The gate opens the raw bytes and recomputes hashes; an index or link alone is
insufficient. Raw files and executable provenance stay outside the repository, including symlinks.
Claims without evidence remain NOT_RUN with an explicit pending owner and absent success fields.

Receipt kinds are test/deployment/budget/identity/command/mutation/documentation/compatibility/review/history.
Positive receipts retain native rc0; mutation receipts retain native ASSERT-FAIL rc1 separately.
Historical PRODUCT-RED and other native failures remain in the historical indexes above and cannot
sign a positive deployment claim. `compatibility.release` requires real-adjacent-release evidence:
two distinct release versions/source commits/executable hashes, each with a readable external
binary and verified SHA256. Six same-source mechanism stamps do not sign N-1 compatibility.
P27 supplies the full 40-cell wire, ALPN, image-reader, retained-domain and infra decoder receipts;
the documentation gate validates their index integrity, while P27/P35 review their behavioral scope.

| Outstanding execution / evidence | Status | Pending evidence owner |
|---|---|---|
| 38 positive/fault/rollback arms, 39 historical/safety/wire arms and A-6 full profile | NOT_RUN | S8/P27, S8/P28, S8/P26; S8/P32 final receipts; S10/P24 and S10/P27 intake |
| 85.A/A1/B/D/Q/L/S/J/F and 86.R/B/I/H/C/K/F/U, stock retrofit and effective unit/cert permissions; 32/73/98 | NOT_RUN | S9/P32 and S9/P33 live supply/arms; S9/P39 final receipts; S10/P11 and S10/P27 intake |
| 99 nine positive arms, both default single and N3, original cohorts and five novel successors | NOT_RUN | S10/P19 assembly, P21–P26 integration, P35/lead execution |
| 100 all 50 independent baseline/assertion-red/restore controls | NOT_RUN | S10/P20 bindings, S10/P31 catalog, S10/P35/lead execution |
| Final same-candidate test/e2e/lint/gates, full default and live-grow catalogs | NOT_RUN | S10/P28–P32 gate/ledger/runner delivery; S10/P35/lead serial execution |
| S6 final 37.A/A1/B/T/R and 39.BR, regressions, ENDPOINT-RESUME mechanism/mutation/matrix-load | NOT_RUN | S7/P37 combined-close receipts; S10/P22, S10/P27 and S10/P34 intake |
| S7 63/61/37.T, original state/OFD/physical history/GC, inventories and final six exits | NOT_RUN | S7/P30 catalog, S7/P37 receipts; S10/P23 and S10/P27 intake |

| Carry key | Required distinction / missing current evidence | Pending owner |
|---|---|---|
| ENDPOINT-RESUME | Preserve S5 red and transfer decision; require S6 production, intended mutant and final matrix-load receipts | S7/P37; S10/P22, S10/P34 |
| SS-JOINT-FUZZ | One same-candidate `make fuzz FUZZ='^Fuzz(FlowFrame\|SSFrame)$' FUZZ_MIN=2 FUZZTIME=30s`; exact start/end targets internal/flow:FuzzFlowFrame and internal/agent/ssproxy:FuzzSSFrame, raw rc | S10/P35 |
| S5-DOCS-AUDIT | Historical COMPLETE/carried/current-required rows separately audited; no invented S5 P29 receipt or deleted guard | S10/P33, S10/P34 |
| PTY-INCARNATION | C4/C8/C10 control/terminal/current-rollback plus same-SID deletion/recreation, late G.1 and old-boundary assertion reversal | S8/P32 receipts; S10/P21, S10/P24 |
| USER-UNIT | S3 body-equivalent U is historical; actual user-unit 82.U3 and no-systemd/shared-domain terminal evidence are still required | S8/P32 receipts; S10/P27 |
| STAGE-EXITS | Each S6–S9 original child arm, compatibility/inventory and final receipt independently; local green does not sign aggregate | S7/P37, S8/P32, S9/P39; S10/P27, S10/P34 |
| DARWIN-SCOPE | Native lifecycle, tagged compile and cross-build separately; S4 historical exception does not waive S6 native requirements | S10/P27, S10/P35 |

S0–S5 administrative closure remains historical, with no new current-candidate receipt inferred.
S6–S9 aggregate acceptance remains unsigned. S9 hot-cut scope stays linux/amd64 + systemd +
pidfd/cgroup/nft and stock filesystem ACME/managed routes; unsupported live responsibilities defer.
Joint fuzz, platform checks and final reviews are NOT_RUN pending S10/P35. All five S10 exits remain
unsigned. No live drill, final gate or external review is certified by these documentation checks.
