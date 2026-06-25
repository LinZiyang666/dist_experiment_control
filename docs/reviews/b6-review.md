# B6 Stage-C review + adjudication — Ops day-2 heavy / net-new

> Stage C：6×Opus 对抗审查（固定 6 维度：backup-restore-correctness / security-no-leak / concurrency-shutdown / byte-equiv-wire / cli-skew-confirm / tests-adversary）→ 1 synth 合并去重 + 裁定矛盾 + 滤假阳。专家只读实现 + 提测试条目，绝不改实现。31 findings（3 BLOCKER + 10 MAJOR + 18 minor，synth 去重为 **2 BLOCKER + 7 MAJOR + 13 minor**）。synth **复现了 BLOCKER-1**（orphan-wal shadowing，throwaway test 实证）。verdict：**FAIL → 主进程修复 → CONDITIONAL PASS→PASS**（核心 byte-equiv/provenance/wiring 健全）。
>
> 主进程逐条裁定 + 修复（只有主进程改实现）。下表 ✅=采纳已修，◻=采纳但 defer（附理由）。

## 裁定：2 BLOCKER + 7 MAJOR — 全采纳已修

| # | finding | 裁定 | 修复 |
|---|---|---|---|
| **BLOCKER-1** | kill-9 中途 normalize 留 orphan `restore-staging.db-wal`；re-run 时 stale wal 叠到 fresh bundle、VerifyIntegrity 仍 ok、腐蚀 DB 装到 live（**synth 已复现**） | ✅ | restore.go step5 复制前清 `stage{-wal,-shm,""}` + defer 同清；`normalizeRestoreStaging` 提交后 `PRAGMA wal_checkpoint(TRUNCATE)` 使 staging 自包含。测 `TestRestoreOrphanStagingWalNotApplied`（预埋毒 wal → 装好的 DB 无 POISON、无残留 sidecar）+ `TestRestoreReRunIsIdempotent` |
| **BLOCKER-2** | restore 的 post-migration / post-normalize verify 零测试覆盖（验序是 plan 的 load-bearing 不变量） | ✅ | 加 test-only seam `restoreAfterMigrateHook`（经 `export_test.go` 暴露给外部测试包）；测 `TestRestorePostMigrationCorruptionNotInstalled`（migration 后毁 staging → restore 报错 + live DB 仍持 LIVE marker，未被覆盖） |
| **MAJOR-1** | incident bundle 泄露完整命令行（`AuditProc.Cmd`/`json:"cmd"`）；denylist 无 `cmd` | ✅ | denylist 加 `cmd,pass,auth,cred,bearer,cookie,private`（命令行是密钥明文最常现处）。测 `TestScrubAuditBody`（`mysql -psecret` 必脱敏） |
| **MAJOR-2** | `scrubAuditBody` 仅 depth-1；嵌套 `AuditCall.Target` map 绕过 denylist | ✅ | `scrubAny` 递归进 `map[string]any` + `[]any`，每层 key 套 denylist。测 `TestScrubAuditBodyNestedTarget`（`target.token`/slice 内 secret 脱敏、`target.port` 留） |
| **MAJOR-3** | restore 不重写 `cluster_meta.self_node_id`；torn bundle 的 self_node_id 漂移 → 守护启动 raft LocalID 与 bootstrap 的 `{m.SelfID}` config 不符 → 永不选举 | ✅ | normalize 同 txn UPSERT `self_node_id=selfID`。测 `TestRestoreReStampsSelfNodeID`（改 bundle self_node_id 为 foreign → 装好的 DB 仍 == m.SelfID） |
| **MAJOR-4** | restore 清 peer 但不 rehome `port_allocations.home_broker`；homed 到已删 peer 的 expose 永不可服务（synth 纠正"D6 会重 home"=**错**，旧 home 已不在 roster、无人 propose reassign） | ✅ | normalize `UPDATE port_allocations SET home_broker=self, epoch=epoch+1 WHERE state='ALLOCATED' AND home_broker!=self`（epoch bump → agent re-pin）。测 `TestRestoreReHomesAllocatedPorts`（peer-homed → self、epoch 7→8） |
| **MAJOR-5** | webhook hung-endpoint 非阻塞 + goroutine-leak 证明完全缺失；`deliver()`/`Run()` HTTP 路径零覆盖 | ✅ | 测 `TestWebhookHungEndpointDoesNotWedge`（-race + NumGoroutine leak gate：挂死 httptest server、>cap Post 每个立返、Drops>0、cancel 后 goroutine 回基线）+ `TestWebhookErrorStatusKeepsDraining`（5xx 不 panic、Run 继续） |
| **MAJOR-6** | never-escapable e2e + version-skew ALLOW 路径 + sign-join embed 未测（wiring 本身**正确**，仅缺证） | ✅ | skew gate 抽成可测 `versionSkewResponse`（ALLOW 路径不撞 AddNode nil-node）；测 `TestVersionSkewAllowPaths`（matching/0/release-skew 不拒）+ `TestVersionSkewRejectsMismatch` + `TestRestoreNeverEscapableEndToEnd`（restore + 正确 flag+env 非 TTY 仍 abort）+ `restore` 进 `TestTier2RejectsUnattendedYes` + `TestSignJoinEmbedsJoinerVersions`（add 行含 `--joiner-proto 2`/`--joiner-release`） |
| **MAJOR-7** | 无 restore live-daemon interlock 测、无 kill-9 幂等测 | ✅ | 测 `TestRestoreRefusesLiveDaemon`（持写锁 → 拒 + 不装）+ `TestRestoreReRunIsIdempotent`（跑两次 → applied_index 仍 0、无残留 staging） |

## minor 裁定

| # | 裁定 | 处理 |
|---|---|---|
| M-a gofmt（protocol.go + b6_webhook_test.go） | ✅ | `gofmt -w` 全量 |
| M-b denylist 漏 pass/auth/cred/bearer/cookie/private | ✅ | 随 MAJOR-1 一并加 |
| M-c restore.go header 过度宣称"cannot adopt a foreign cluster" | ✅ | 软化为"证明 restorer 拥有本节点 tunnel 私钥"（security-pragmatic v1） |
| M-d restore 校验 Mode 不校验 Kind（recover manifest 被静默接受） | ✅ | 加 `m.Kind != ManifestKindBackup` 门 |
| M-e `init --from-existing` + `--from-manifest` 并存静默偏 manifest | ✅ | `MarkFlagsMutuallyExclusive` |
| M-i error_hints 注释把 version_skew 归"our-bug→70"（码正确映 64） | ◻ | 纯注释、defer（码无误） |
| M-j 新 B6 wire 类型 additive-omitempty 未 pin | ✅ | `TestB6WireAdditionsOmitemptyRoundTrip` |
| M-k DEGRADE band 边界 + 单模式 metrics 省略未测 | ✅（单模式） | `TestMetricsSingleModeOmitsAllClusterGauges`；band 边界已有 disk/ports band 测 |
| M-f `init --from-manifest --check` Doctor 用空 addr | ◻ | defer：dry-run 仅 doctor 预检，真跑仍读 manifest；非数据/安全面 |
| M-g `init --from-manifest` 空 self_id 时 typed-confirm 可裸 Enter 满足 | ◻ | defer：下游 `InitFromExisting` F4 守卫已拒空身份、无数据损失 |
| M-h DEGRADE disk band 漏 0%-free self（`DiskFreePct>0` 守卫） | ◻ | defer：窄窗（0% 与 ~80%-used disk_pressure 告警之间）；catastrophic full-disk 已由 replicated `disk_pressure` 告警覆盖 |
| M-l BackupDBTo/BackupDBFile 缺 backup-under-write -race | ✅（等价） | 已有 `TestBackupUnderConcurrentWrite`（活 raft 节点高写下 online backup 一致性，-race） |
| M-m incident `--since`/`--sid`/follower 未测 | ◻ | defer：filter 是 SQL WHERE、follower→NotLeader 由 dispatch 统一守卫（已被 D7 测覆盖） |

## DROPPED（synth 已滤的假阳）
- "restore 接受 recover-Kind manifest 装坏状态" → 降为 minor（recover manifest 无 state.db，`os.Stat(statePath)` 先拒；但仍加 M-d Kind 门更早更响）。
- "D6 自驱 rehome 最终重写 peer-homed 行" → synth 纠正为**错**（旧 home 已删、无人 propose reassign）→ MAJOR-4 成立。
- restore provenance 注释 over-claim un-forgeability → 折进 M-c。

## 结论
2 BLOCKER + 7 MAJOR **全修 + 全测**，high-value minor 全修，剩 5 个 minor 带理由 defer（均非数据/安全/byte-equiv 面）。`make test` + `make lint` + 相关 `-race`/leak gate 全绿后达 **PASS**。
