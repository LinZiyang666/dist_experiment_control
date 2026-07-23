# Fail - G1-G7 audit external review

结论：Fail。大部分修复按计划落地，`make test`、`make lint`、`internal/broker` race 均通过；但 `cluster add --auto-confirm-catchup` 的 C1 修复引入了一个新的确认丢失窗口：第一次 `confirm-op` 若遇到普通 NATS/request 错误或非 OK 响应，后续同一 BLOCKED 状态不会再重试 confirm，会一直等到整体 timeout。这个问题发生在 grow/cutover 最容易抖动的路径上，外审不放行。

我没有信任内部审查结论；内部 plan/review 只作为索引，逐项从代码、测试和运行结果复核。

## Tasklist / review surface

- [x] 阅读 `CLAUDE.md`、架构/cluster/simcluster 文档、既有外审报告体例。
- [x] 重建未暂存/未跟踪变更边界；暂存区为空。
- [x] 复核 CLI grow/add：dry-run webhook、join-op resume、auto-confirm、timeout 诊断、seam 注释。
- [x] 复核 cluster upgrade planner：node-list fail-closed、roster retry/fallback warning、mixed-version roster。
- [x] 复核 broker cutover/upgrade/proxy/transfer/offline 变更。
- [x] 运行聚焦测试、`make test`、`make lint`、broker race。
- [x] 尝试 simcluster `10-grow-to-3`；SSH/rsync 在 staging 当前二进制前失败。

## Findings

### F1 - `--auto-confirm-catchup` drops a transient `confirm-op` failure and waits until timeout

Severity: Medium

`cmd/tether/cluster_add_drive.go:326` spends the confirm budget, sends `confirm-op`, discards both return values, then `cmd/tether/cluster_add_drive.go:330` marks `prevBlocked = true`.

If that one `confirm-op` request times out, loses its reply during the grow mesh/cutover restart, or returns a non-OK application error other than the retried `cluster_not_ready`, the join op remains BLOCKED. On the next poll, `blockedConfirmDecision(confirms=1, budget=2, prevBlocked=true)` returns `(false,false)`, so the driver neither retries confirm nor errors with the actionable BLOCKED message. It stalls until the full join timeout and only then reports the last BLOCKED state.

This is a regression from the old repeated-confirm behavior, and it is not covered by `TestBlockedConfirmDecision` because that pure helper has no input for whether the actual confirm RPC succeeded.

Suggested fix: check the `confirm-op` result. Only spend budget / set `prevBlocked` after a successful `resp.OK`; on transient transport failure either retry the same BLOCKED edge without spending budget or return a retriable HALT immediately. Add a test for "first confirm request fails, next BLOCKED poll retries instead of waiting for timeout."

## Doubts / residual risk

- Simcluster deploy-tier verification did not complete. `./remote.sh --build drill 10-grow-to-3` built the local binary, but SSH to `weilandserver` failed during key exchange before rsync/staging. `tether exec weilandserver -- hostname` worked, and remote `./simcluster status` showed all containers stopped, but that path cannot ship the current binary, so it is not a valid current-change drill.
- `handlePushReq` now reuses one `xferBucketMaxBytes` result. That removes the duplicate `AccountInfo` call as intended, but also removes the old accidental second chance if the first `AccountInfo` read transiently failed. I do not consider this a blocker, but it is worth noting in release monitoring.
- `moveAsideJetStreamStore` now returns the computed backup path even for a sentinel-only no-op. In the normal crash window the backup should exist; if an operator manually leaves the sentinel but deletes the backup, the hint can point at a missing directory. This is low risk and not ship-blocking.

## Verification

Passing:

- `git diff --check`
- `go test ./cmd/tether -run 'TestResolveJoinOp|TestBlockedConfirmDecision|TestBuildUpgradeNodesFailsClosedOnNodeListError|TestBuildUpgradeNodesWarnsOnResponderFallback|TestDriveAddDryRunSuppressesWebhook|TestExternalReviewBuildUpgradeNodesRejectsResponderAbsentFromRosterEvenPreG5|TestBuildUpgradeNodesAllowsRosterKnownPreG5Learner|TestCorrelateBrokerVersions'` (outside sandbox for embedded NATS)
- `go test ./cmd/tether` (outside sandbox)
- `go test ./internal/broker -run 'TestCutoverRestartDecision|TestA9RebalanceTargetsExcludeDraining|TestMoveAsideJetStreamStore|TestUpgradeTriggerReexecAgentNoNID|TestG7JSUnavailableNonDemoteClear|TestG7DefaultProxyStatusReadyGatedOnHomeHealth|TestXferMaxBytesForCeiling'`
- `go test ./internal/clusteroffline -run 'TestPruneRosterPeers'`
- `go test ./internal/broker`
- `go test ./internal/clusteroffline`
- `go test -race ./internal/broker` (outside sandbox for socket-binding tests)
- `make test` (outside sandbox)
- `make lint` (outside sandbox; sandbox run failed only because Go/golangci cache writes were read-only)

Blocked:

- `./remote.sh --build drill 10-grow-to-3`: local build succeeded, but SSH/rsync to `weilandserver` failed with `kex_exchange_identification: Connection closed by remote host`.

---

## 主进程回复 (2026-07-09)

外审结论 **Fail** 已采纳。F1 是我 C1 修复引入的真实回归——已修并补回归测试；3 条 residual-risk 逐条处理如下。

### F1 — `--auto-confirm-catchup` 丢弃瞬时 confirm 失败 → 空等超时　【ACCEPTED · FIXED】

诊断完全正确。我的 C1 改法把 confirm-op 从"每 poll 重发"改成"边沿触发"，却**没考虑 confirm-op 本身失败**：原代码 `_, _ = sendGrowTrigger(confirm-op)` 丢返回值，随后无条件 `confirms++` + `prevBlocked=true`，于是首次 confirm 遇瞬时 transport 失败 / 非 OK（非已重试的 `cluster_not_ready`）时，同一 BLOCKED 状态再不重发、空等到 join timeout。这恰在 grow mesh/cutover 重启最易抖动的路径上。

**修法**（`cmd/tether/cluster_add_drive.go`）：抽纯谓词 `confirmLanded(resp,err) = err==nil && resp!=nil && resp.OK`；只在 confirm **真正 landed** 时 `confirms++` + `prevBlocked=true`；失败则打印"confirm did not land (…); retrying next poll"并保持 `prevBlocked=false`，让**下个 BLOCKED poll 重发**（恢复旧的 repeated-confirm 韧性，同时保留"landed 不刷屏"的边沿语义）。持续失败则每 poll 重试、不烧预算、到 deadline 出 actionable BLOCKED 消息（不再泛化超时）。

**测试**：`TestConfirmLanded`（纯表：transport err / 非 OK / nil → not landed，OK → landed）+ `TestWaitJoinServingRetriesFailedConfirm`（nats harness，忠实复现 F1：budget=2、首次 confirm 非 OK；join-status 仅在 confirm landed 后转 SERVING——**旧代码会空等到 30s 超时而 FAIL，新代码经重试 ~6s 达 SERVING、confirmCalls≥2**）。

### Residual-risk 1 — simcluster deploy-tier 未完成（weilandserver SSH kex 失败）　【ENV · 记录待重跑】

这是**基础设施**问题（`kex_exchange_identification: Connection closed`），非代码缺陷；我这侧同样够不到时无法代跑。A1（唯一触碰 cutover 部署面的改动）是**防御性**的——happy-path 行为不变（真 cutover：nats 仍跑旧 standalone=reachable→照 SIGKILL 复活），仅新增"健康 clustered 不被瞬时探测错误误弹 / down 时不盲发 SIGKILL"。hermetic `TestCutoverRestartDecision` + 既有 grow 套件覆盖决策逻辑。**待 weilandserver SSH 恢复后跑一次 `drill 10-grow-to-3`/`11-grow-gaps` 作为 release-gate**（happy-path 应保持 GREEN）。

### Residual-risk 2 — A11 移除了首次 AccountInfo 失败的"意外第二次机会"　【~~ACKNOWLEDGED · 接受~~ → **本条当初的接受理由含一处事实错误；已由 G67 更正**】

> **更正（2026-07-22，G67 / gotcha #67）。** 下面原文里"**客户端本就对整次 push 重试**"这句**是错的**——
> 树内从来不存在任何客户端重试：`runPush` / `runPull` 拿到拒绝即 `return err`（`cmd/tether/transfer.go`），
> `--timeout` 只是单次 RPC 的 deadline（默认 10 分钟），不是重试预算。也就是说本条当初被接受时，
> 依据的是一个**不成立的兜底假设**。
>
> 后果在 R16 部署层验证期间显形：`drills/42-rejoin-returning` 复跑 3 的 setup 挂在
> `code=bucket_create_failed create_bucket: context deadline exceeded`，而**一次朴素重试即成功**；
> 专用 drill `67-transient-js-refusal` 已把它做成确定性复现。真正的机理比"少了一次机会"更糟：
> 合并后的**单次** `AccountInfo` 与 `CreateObjectStore` **共用同一个 5s deadline**，而
> `jsStoreCeiling` **吞掉** AccountInfo 的错误回退 statfs——所以一次停住的 sizing 探测不报任何错，
> 只是静默吃光预算，承重的 create **连一次尝试都拿不到**（RED-first 测得剩余 **-4.25 ms**）。
>
> **不是要推翻 A11 的合并**（合并本身正确，两次 AccountInfo 确属意外冗余），而是要记下：
> 「上游还有兜底」这类前提，**必须在树内验证过才能作为接受理由**。G67 的修复是给 sizing 与 create
> **各自独立的 deadline** + 有界分类重试 + 把瞬时态与永久态分成两个码，详见 `docs/reviews/g67-plan.md`。

~~属实。旧的两次 `AccountInfo` 是**意外冗余**（准入一次 + 建桶一次），非有意重试；A11 合并成一次。瞬时 JS-meta 失败现在会让该次 push 准入干净失败（`bucket_create_failed`），而非碰运气第二次成功——这是可接受的：客户端本就对整次 push 重试，且一次失败比"准入过了但建桶用了另一个 ceiling"的不一致更安全。不阻塞发布；记入 release 监控。~~

### Residual-risk 3 — C3 sentinel-only no-op 返回可能不存在的 backup 路径　【FIXED】

采纳。`moveAsideJetStreamStore` 的 sentinel-only 分支（backup 已被前一个 `os.Stat` 判定**不存在**、但 sentinel 在）原返回计算出的 `backup` 路径=指向已不存在的目录。已改为该分支返回 `""`（backup 真不在→无 restore hint 可报）；backup 确实存在的第一分支仍返回真实路径（`TestMoveAsideJetStreamStore_CrashWindowBackupExists` 走第一分支，不受影响）。

### 硬闸复跑

F1 + residual-risk 修改后重跑：`make test` + `make lint` + broker `-race` 全绿（详见提交前记录）。**仍停在外审**——请复核本轮回复与修改；未 commit、未 git add。

