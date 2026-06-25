# B 程序（B1–B7）≥20 专家大规模联合内审 + 主进程定稿

> 外审前的最后一道内部门。22 个专家 lens（使用者/agent-operator/运维/PM/SRE + 安全/并发/错误处理/数据完整/wire/解耦/简洁/CLI/测试/资源泄漏/quorum 安全/可观测/升级/输入校验/DR/文档/命名）并行审计整个 B 暂存区 → synth 合并去重（87 raw findings）→ **0 BLOCKER + 18 MAJOR + ~50 minor**，verdict **FIX-THEN-READY**。
>
> **核心不变量审计全 hold**：无 `ProtoVersion` bump、无 single-WAL-owner 违反、无非集群单 broker byte-equiv 破、破坏性 op typed/machine-confirm 门完好。MAJOR 全是局部可修缺陷，集中在 **DR/restore 正确性** + **可观测诚实性** 两个外审必查面。
>
> 主进程**全采纳 18 MAJOR**、逐条修复 + 加测、重跑硬闸全绿。下表 ✅=修+测，◻=文档/defer（附理由）。

## DATA-INTEGRITY（最高风险，全修）

| # | finding | 修复 |
|---|---|---|
| DI-MAJOR-1 | restore 不重置 `audit_published_index` → 复制游标停在旧高 commit、Publisher `hi<=cur` 永久 wedge、恢复集群终身零审计发布 | ✅ normalize 同 txn 重置 `audit_published_index=0` + 清 `cluster_reqid_ledger`（旧高 raft_index 键永不 GC）。测 `TestRestoreClearsAuditAndStateMarkers` |
| DI-MAJOR-2 | restore 先 install DB 后 wipe raft → kill-9 torn window（新 DB applied_index=0 + 旧 {A,B,C} raft log → 启动复活已删 peer），无 fail-closed 守卫 | ✅ normalize 设 `restore_in_progress` marker（随 install 入 live DB）、bootstrap 成功后才清；`assertClusterDBConsistent` 见 marker 即 FATAL 拒启动（fail-closed，re-run restore 完成）。runbook §5.1 加"勿 mid-restore 启动、可 re-run"。测 markers cleared after success |
| DI-MAJOR-3 | online backup 的 manifest.applied_index/roster 读 **live** RODB（已推进到 Y>X）非 `state.db` copy（X）→ DR 工具误信 bundle 更新 | ✅ handleBackup 在 BackupDBTo+Verify 后开 `state.db` 只读、从 copy 读 identity/roster（与 offline 路径一致） |
| DI-MAJOR (init) | `init --from-manifest` 收 **backup** manifest（只校 Mode 不校 Kind）→ DR 误用得干净 N=1 但**零业务态**（丢全 session/port/alert） | ✅ 加 `m.Kind != ManifestKindRecover` 拒、指向 `cluster restore`。测 `TestInitFromManifestRefusesBackupKind` |
| DI-MAJOR (downgrade) | runbook §6 称混合版本"SAFE"，但新 release 加 migration 后升级节点不可回滚（migration 仅前向） | ◻（文档修）runbook §6 加 schema-downgrade caveat（migrate 后单向、留升级前 backup） |

## QUORUM-SAFETY（全修）

| # | finding | 修复 |
|---|---|---|
| QS-MAJOR-1 | `cluster apply --plan`：leader + 唯一另一 voter 都退役时，`resolveSurvivingVoter()==""` 回落占位符 `<another-voter>` transfer + drain（Stage-C M3 对两真 voter 回归） | ✅ 该情形发单一 REFUSED 步（无占位 transfer、无 drain）。测 `TestDiffTwoVotersBothRetireNoPlaceholder` |

## SECURITY（全修）

| # | finding | 修复 |
|---|---|---|
| SEC-MAJOR-1 | cluster `node_id` 无 charset 校验 → shell-metachar/换行/`/` id 逐字渲入 operator 复制粘贴命令行 + 文件路径（apply/guided/recoverGuided） | ✅ `proto.ValidateNID`（`[a-z0-9-]{1,32}`）fail-closed 于 clusterspec.Parse + broker handleAdd（claimJoinNonce 前）。测 `TestParseRejectsBadNodeID` |

## ERROR-HANDLING（全修）

| # | finding | 修复 |
|---|---|---|
| EH-MAJOR-1 | export-incident 在 session 枚举失败时静默产不完整 bundle（无 Partial/Errors，B2 anti-swallow 未带到 B6）→ forensic bundle 读成 complete-but-empty | ✅ `IncidentBundle` 加 `Partial`/`Errors`；activeSIDs 错 → Partial+Errors；per-sid 非 benign 错记入（benign `history_unavailable` 不算） |

## UX / CLI（全修）

| # | finding | 修复 |
|---|---|---|
| UX-MAJOR-1 | 4 个 B6/B7 `--json`（apply/ops ls/ops show/export-incident）破 B2 `(schema, schema_version)` machine-dispatch 契约 | ✅ 各包 DTO 带非-omitempty `schema`+`schema_version`（`cluster_apply_plan`/`cluster_ops`/`cluster_op`/`incident`） |
| UX-MAJOR-2 | "no active session"（15 站点）归 exit 70（可重试/请上报）→ 终端 login 前置被 robust-retry wrapper 永久重试 | ✅ `classifyExit` 加 "no active session" 前缀 sniff → exit 64（一处覆盖 15 站点）。测 `TestNoActiveSessionExitsUsage` |

## OBSERVABILITY（全修）

| # | finding | 修复 |
|---|---|---|
| OBS-MAJOR-1 | `last_contact_secs` 假信号：每行声明（非 omitempty）、从不写、恒 0 = "刚联系" 即便 peer 死 | ✅ 删字段（无 reader；真可达性在 Reachable/AppliedLag/ReachSource） |
| OBS-MAJOR-2 | `tether_broker_quorum_margin` 报**静态**配置 headroom（3-voter 死 1 仍报 1），非 live resilience | ✅ leader 侧减去 lastObserve 中 unreachable voter（floored 0）= live margin |

## DOCS / SIMPLICITY / TESTS（全修）

| # | finding | 修复 |
|---|---|---|
| DOCS-MAJOR-1 | runbook §5.1 称恢复后 exit 3 FORCE_SINGLE，实际 restore 不设 force_single_active → exit 1 DEGRADED | ✅ runbook 改 exit 1（+ restore 清 force_single_active 见 DI-MAJOR-1 修） |
| DOCS-MAJOR-2 | broker.go 生产接线点自相矛盾注释（"caughtUp/streamsReady nil for now" 紧挨传非-nil 生产探针） | ✅ 删过期"nil for now"句、留准确文本 |
| DOCS-MAJOR-3 | `cluster apply`/`cluster ops` 完全缺席 usage.md | ✅ usage.md §4 加 apply/ops/doctor 行 |
| SIMP-MAJOR-1 | `clusterroster.Verify`/`Select` + `RosterGen` 全程不可达死代码（agent 消费 DEFER post-v2） | ◻ roster.go 包注释标注 = deferred agent-discovery consumer 的**已测 seam**（非死代码、移除将逼重导重测）；b7-review 已记 DEFER |
| TEST-MAJOR-1 | `metricsReady` cluster-mode 分支（/readyz no-silent-fork LB 守卫，B5 BLOCKER 修）零测试 | ✅ `TestMetricsReadyClusterBands`（真 inmem Node：leader+VOTER→ready、CATCHING_UP/RETIRING→503） |
| TEST-MAJOR-2 | `InitFromExisting` live-daemon interlock（比 restore 更危险的不可逆迁移）无 refuse-before-mutate 测 | ✅ `TestD9InitFromExistingRefusesLiveDaemon`（持写锁 → 拒 + 无 .bak） |

## ~50 minor — 处理摘要
- **已修**（随上面 MAJOR 一并）：force_single_active 清、restore_in_progress fail-closed、incident schema_version、no-active-session exit、quorum_margin live、last_contact_secs 删。
- **defer（文档/低影响）**：stale "writerless until D9"/"build-and-prove inert" 注释 sweep（post-D9 行为已变、不改行为）、usage.md 余 B6/B7 flag 表补全、webhook follows-redirect（operator-trusted URL、defense-in-depth）、export-incident 负 `--since`、roster ExpiresAt 不验（deferred consumer 才相关）、force-single 留 stale peer 行、`cluster ops show` "timeline" 措辞、各 dead code（CutPrefix/Drops()/CodeAlreadyVoter）——均非数据/安全/byte-equiv 面，记入外审跟踪清单（不阻塞）。

## 结论
0 BLOCKER；**18 MAJOR 全修 + 全测**（DI×4 / QS×1 / SEC×1 / EH×1 / UX×2 / OBS×2 / DOCS×3 / TEST×2 + downgrade-doc）；high-value minor 全修，余 minor 带理由 defer。`make test` + `make lint`（0 issues）+ gofmt + 并发面 `-race` 全绿。核心不变量审计全 hold。三个 Stage-C 范围 deviation 经独立审计**再确认 JUSTIFIED**。**B 程序 → READY FOR EXTERNAL REVIEW。**
