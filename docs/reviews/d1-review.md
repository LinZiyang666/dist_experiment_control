# D1 — 内审报告（对抗审查 + 主进程整合）

> 来源 = 4-agent 对抗审查 workflow（4 维度：raft/FSM 正确性 / SQLite-资源-并发 / 测试严谨-非平凡 / 范围-规约一致 → 1 综合）。专家**只读实现、可建议测试、不改实现**；主进程逐条处置 + 整合。
>
> **综合裁定：CONDITIONAL PASS** —— D1 共识核心构造正确、`-race` 干净，happy-path §3.7 不变式（同 txn applied_index / durable-cursor 幂等重投 / poison advance / sentinel→Response/Error 映射 / NoSnapshotRestoreOnStart / LogCommand-only / bootstrap off-by-one）**全部经源码核验成立**。但**一条承重正文修正被证伪、其证明测试 vacuous**（must-fix）；其余为测试-vacuity/覆盖洞 + 文档时效。**无 fd/goroutine 泄漏、无 -race 失败、产品代码无 D2/D3 scope creep。**
>
> **处置结论：全部 must-fix + should-fix + nits 已整合，全门绿。**

## must-fix（High，已修）

**§3.7 #1 不变式"snapshot.Index ≤ applied_index 仅靠同步提交时序"是错的，且证明测试 vacuous。**
专家对 raft@v1.7.3 源码**亲验**：`runFSM` 在 `FSM.Apply` 返回后**无条件**前进内部 `lastIndex`（不管返回值），`snapshot.Index` 取自它；重启 `restoreSnapshot()` 即便 `NoSnapshotRestoreOnStart=true` 跳过 `FSM.Restore` 仍 `setLastApplied(snapshot.Index)`，故只重投 `[snapshot.Index+1..commit]`。两条破坏路径：
- (a) **LogBarrier/config 尾**：Barrier 进 runFSM 但非 LogCommand → raft.lastIndex 前进、applied_index 不动 → `snapshot.Index > applied_index`（**良性**，无变更）。
- (b) **瞬时 Apply 错误**：`applyCommand` 把 Begin/Commit 错误**返回**给 raft → raft 仍前进 lastIndex；此后一次快照把 meta.Index 钉到 K 而 SQLite 停 K-1 → 重启该 entry 永不再投 → **真丢数据**（证伪了 plan"瞬时错靠重启+重投自愈"）。

**处置（采纳，实现 + 正文 + 测试三改）**：
1. **实现 fail-stop**（`fsm.go`）：`FSM.Apply` **绝不把未落库命令返回给 raft**——瞬时 Begin/Commit 错**重试 `applyMaxAttempts`，仍失败则 panic 停机**。消除 (b) 的丢数据窗口（停机的节点不快照；重启走无快照的全量重投，entry 被重投）。poison entry 仍 advance（无变更，本就该越过）。新增 `applyFailHook` test seam。
2. **正文改述**（§3.7 #1 + §3.8「D1 实现修正」）：真不变式 = 「**重启不丢任何已提交 LogCommand 变更**——raft 重投 `[lastSnapshot+1..commit]`、FSM 按本地 durable applied_index 自跳；snapshot.Index 只可能被**无变更**的 barrier/config entry 超过 applied_index」+ fail-stop 纪律。
3. **替换 vacuous 断言 + 加非平凡测试**：原 `TestSnapshotRestore_RoundTripAndInvariant` 的 `meta.Index ≤ applied` 改为 all-command 尾的 `==`；新增 `TestSnapshotThenRestart`（apply N→快照→apply M→重启→断 N+M 全在 **且 reapply==M**，证存活纯靠 durable SQLite）、`TestSnapshotAfterBarrier`（构造 (a) 良性 gap 仍收敛）、`TestFSM_FailStopOnPersistentApplyError`（断 persistent 错→panic）。

## should-fix（已修）

| ID | finding | 处置 |
|---|---|---|
| FP3 缺 | kill-9 矩阵只有 fp1/fp2，缺 plan §5.1 的 **fp3（backup Step 中 SIGKILL → 活库不动、孤儿 temp 无害）** | **采纳**：`backupStepGate` seam（首 Step 后触发）+ fp3 child（snapshot 阻在 backup 中被 SIGKILL）；断活库 integrity_check ok、applied_index/内容不变、孤儿 snap temp 不阻 New()、恢复后仍能 apply+snapshot。 |
| 判别力对照 harness-disjoint | 反向对照（broken separate-txn）只在进程内手仿、未过真 kill-9 harness+assertRecovery | **采纳（经内容哈希闭环）**：fp1/fp2/fp3 的 assertRecovery 现断 **pre-kill `clusterMetaHash` == post-recovery hash**（fp2/fp3 内容稳定），令真恢复路径的正确性断言承重——broken FSM 的双 apply 会改内容被抓；进程内 `TestDiscriminatingControl` 仍证检测器有判别力。 |
| size-guard vacuous(WAL) | `dbFileSize` 读 4KB 主文件（数据在 -wal） | **采纳**：改 `logicalDBSize`（`page_count*page_size`）。 |
| synchronous=FULL 未测 | WAL 测断 journal_mode/FK/MaxConns，独缺 synchronous | **采纳**：`TestWALConcurrency` 加 `PRAGMA synchronous==2`；新增 `internal/storage/storage_wal_test.go`（OpenWAL/OpenReadOnly 单测，plan 点名却缺）。 |
| FK-check arm 未测 | `verifyIntegrity` 两臂只测 integrity_check，FK 臂从未触发 | **采纳**：`TestSnapshot_FKViolationRejected`（FK 关插孤儿行→结构有效但 FK 臂拒、活库不变）。 |
| fp2 reapply>0 太弱 | 计数器近乎必 >0、不证内容无双 apply | **采纳**：fp2 加内容哈希断言（见上）。 |
| 文档时效 | CLAUDE.md §7 / determinism README / §19 D1 仍 D0 视角 | **采纳**：§7 改写（加 v0.3.5/6 + D0/D1 + proto-v2 地雷）、README 改 L-2-live + 4 新测试、§19 D1 加状态块。 |

## nits（已修）
- **fp not-ConfigurationStore**：新增 `TestFSM_NotConfigurationStore`（*fsm 不实现 ConfigurationStore/BatchingFSM，守 LogCommand-only 不变式，逼 D3 正视 lastIndex-vs-applied gap）。
- **Snapshot-nothing-new 契约未测**：新增 `TestNode_SnapshotNothingNewContract`。
- **lint 守卫谓词未共享 + 吞 walk 错 + 无文件计数下限**：`TestRaftConfinedToClusterPackage` 改调 `raftConfinementOffender`（与 self-check 同谓词）+ 加 visited≥50 下限；`walkGoFiles` 不再吞 `WalkDir` 错。
- **良性/正向恢复对照只断 err==nil**：positive control + trailing_garbage 加 `clusterMetaHash` 内容相等断言。
- **TestNoStrayKill9Child 间接**：保留（"无条件 fork 会启动即无限自递归"是 sound 的间接证；正常 suite 跑通本身即证），措辞接受。
- **Node.Apply transient vs business 错不可分**：fail-stop 后 transient 错变 panic（不再经 Response 返回），D1 无 op 业务错，nit 自然消解；Response 错路径保留供 D2。

## out-of-scope（D3，已登记非缺陷）
- `restoreInPlace` 对**非空活库**（真 InstallSnapshot follower catch-up）未测——D1 `NoSnapshotRestoreOnStart=true` 令本地重启不走 Restore，属 **D3**。
- 带 uncheckpointed WAL frame 的恢复 + 后续 checkpoint(TRUNCATE)——D3 InstallSnapshot 形状。
- 不可信 peer 快照预置 `schema_migrations` 跳 DDL——D1 唯一快照源是本节点可信 online-backup；D3 加 post-restore schema 校验。
- OpenReadOnly 池无上限 / mid-Step 磁盘满的部分恢复原子性——D1 串行单消费者安全，fault-injection 为可选前向项。
- §D6 catch-up 谓词 `applied_index >= barrier index` 处 barrier-gap **非良性**——本次正文修正已显式 flag 为 D6 hazard。

## 不变式已确认（专家核验成立）
同 txn applied_index（`TestApply_SynchronousCommit` 独立 conn 即见）/ durable-cursor 幂等重投 / poison advance 不卡死 / sentinel→Response/Error 映射 / LogCommand-only + bootstrap off-by-one（config/noop 不调 Apply）/ NoSnapshotRestoreOnStart 真 + N=1 纯幂等重投 / synchronous=FULL 实生效 / 单写 WAL 池 + 独立只读 backup handle / 无 fd/goroutine 泄漏 / NewRestore 原子（撕裂前 reject）/ raft-log 编码与 proto v2 解耦 / 范围守住无 D2/D3 creep。

## 门状态（整合后 — 全绿）
`build` ✓ · `vet` ✓ · `make lint` 0 issues ✓ · full `make test` ALL PASS ✓ · `-race`（cluster/storage/determinism）✓ · `make e2e`（`TestD1Matrix` 等全矩阵）✓ · kill-9 fp1/fp2/fp3 承重门绿。

→ 待外审。
