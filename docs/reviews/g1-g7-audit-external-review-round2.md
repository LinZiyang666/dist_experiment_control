# Pass - G1-G7 audit external review round 2

结论：Pass。上一轮唯一 blocker F1 已闭合：`confirm-op` 只有在收到 `OK` reply 时才消耗 `--auto-confirm-catchup` 预算并置 BLOCKED 边沿；transport error / nil reply / non-OK reply 都不算 landed，下一次 BLOCKED poll 会重发 confirm。新增的 live NATS 回归测试复现了上一轮失败场景并通过。

我仍未信任内部回复；本轮按 diff、测试和 deploy-tier drill 独立复核。

## Tasklist / review surface

- [x] 复建当前 diff 边界；暂存区为空，所有变更在工作区。
- [x] 阅读 round-1 外审报告内的主进程回复。
- [x] 复核 F1 修复：失败 confirm 不烧预算、不置 `prevBlocked`、下一轮重试。
- [x] 复核新增 `TestConfirmLanded` 与 `TestWaitJoinServingRetriesFailedConfirm`。
- [x] 复核 C3 sentinel-only no-op 不再返回不存在的 backup hint。
- [x] 重跑聚焦测试、`make test`、`make lint`、broker `-race`。
- [x] 跑 deploy-tier simcluster `10-grow-to-3`。

## Findings

No blocking findings.

### F1 - resolved

`cmd/tether/cluster_add_drive.go` now routes the confirm branch through `confirmLanded(resp, err)`. The budget increment and `prevBlocked=true` happen only after an `OK` confirm response. If the confirm fails, the code logs the failure and leaves `prevBlocked=false`, so the next observed BLOCKED state re-enters the spend-confirm branch.

The new NATS harness test `TestWaitJoinServingRetriesFailedConfirm` covers the exact failure mode from round 1: first `confirm-op` returns non-OK, join status remains BLOCKED, second confirm succeeds, and the wait reaches SERVING instead of timing out.

### C3 residual - resolved enough

The `moveAsideJetStreamStore` backup-present path still returns the real backup path. The sentinel-only path now returns `""` when the backup dir is absent, so it no longer emits a restore hint to a non-existent path. This is the right tradeoff for a rare/manual state.

## Doubts / residual risk

- `confirm-op` failures now retry every poll until deadline without consuming budget. That is intentional and matches the old resilience profile, but if a permanent non-OK condition repeats, the operator sees retry messages until the actionable timeout.
- The A11 single-`AccountInfo` transfer path still has the same residual noted in round 1: less accidental retry, cleaner consistency. I do not consider it blocking.

## Verification

Passing:

- `git diff --check`
- `go test ./cmd/tether -run 'TestConfirmLanded|TestWaitJoinServingRetriesFailedConfirm|TestBlockedConfirmDecision|TestResolveJoinOp'` (outside sandbox for embedded NATS)
- `go test ./cmd/tether` (outside sandbox)
- `go test ./internal/broker -run 'TestMoveAsideJetStreamStore|TestCutoverRestartDecision'`
- `go test ./internal/broker`
- `go test ./internal/clusteroffline -run 'TestPruneRosterPeers'`
- `go test ./internal/clusteroffline`
- `make test` (outside sandbox)
- `make lint` (outside sandbox)
- `go test -race ./internal/broker` (outside sandbox)
- `./remote.sh --build drill 10-grow-to-3` from `test/simcluster`: GREEN, 19 assertions, including N=1→2→3 grow, R=3 streams, leader transfer, follower-kill quorum, `node ls`, and quorum write.
