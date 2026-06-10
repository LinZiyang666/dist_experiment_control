PASS

# Compliance Cleanup Review

Date: 2026-06-10
Reviewer role: direct-fix reviewer

## Verdict

本轮暂存区外的合规清理可以放行。

原改动中的注释校正、Darwin 测试兼容、auth parser 去重、二进制 reader
修正和 proxy 查询去重均成立。审查中确认三类问题，已直接修改并增加回归；
修复后全量测试、受影响包 race、lint 和 diff check 全部通过。

## Direct Fixes

### F1 - Medium, fixed: duplicate pull ID 会终止原传输

broker 正确拒绝重复 `transfer_id`，但 pull CLI 对所有 prepare 失败都会发送
同 ID 的 failed-finalize。若重复请求来自原 actor，finalize 会命中并 claim
原 entry，写 failed audit、删除原对象并停止 watchdog。

修复：

- `transfer_id_in_flight` 拒绝不再发送 finalize；
- tracker 明确拒绝重复 ID 且不得替换原 entry；
- wire 注释和 failure-code 清单补齐该错误码。

回归：

- `TestPullDuplicateIDRejectionDoesNotFinalizeOriginalTransfer`
- `TestTransferTrackerRejectsDuplicateWithoutReplacingOriginal`

### F2 - Medium, fixed: 固定临时文件名可跟随预置 symlink

identity 和 agent state 原先都写固定 `<path>.tmp`，`O_TRUNC` 会跟随该路径上
预置的符号链接。即使正常目录为 0700，这也不应成为持久化原语的隐含前提。

修复：

- 改用同目录 `os.CreateTemp` 随机文件；
- 显式保持 0600；
- 写入后执行 file fsync、close、rename、parent-directory fsync；
- 所有失败路径清理临时文件。

回归：

- `TestEnsureIdentityDoesNotFollowPredictableTempSymlink`
- `TestStateStoreDoesNotFollowPredictableTempSymlink`
- state/identity 成功路径检查内容、权限和无残留临时文件。

### F3 - Low, fixed: transfer 注释仍混用旧 bucket 模型

生产注释同时描述了旧的 per-transfer bucket 和当前 per-session bucket，
并把对象清理写成删除整个 bucket；`PullPrepareReq` 还描述了不存在的
“agent Stat 后再创建 bucket”流程。

已统一为当前实现：

- bucket/stream 为 `xfer-<sid>` / `OBJ_xfer-<sid>`；
- `transfer_id` 是对象 key；
- finalize 删除对象，不删除 session bucket；
- transfer ID 是随机 ctl-generated ID，不再误称 ULID。

## Accepted Changes

- Darwin Unix socket 路径缩短，避免 `sun_path` 超限；
- BSD/macOS `base64` 兼容；
- `t.TempDir()` 的 `/var` symlink canonicalization；
- auth role parser 一次返回 sid/nid；
- gzip reader 不再做 `[]byte -> string -> reader` 拷贝；
- proxy stale allocation 查询去重；
- 其余 godoc/注释位置和事实校正。

## Questions And Suggestions

1. `docs/reviews/file-transfer-plan.md` 保留了大量历史方案和后续整改回复，
   仍会同时出现旧 per-transfer bucket 与新 per-session bucket。它适合作为
   review history，但不适合作为当前实现 SSOT；建议以后在文首增加
   “Current Contract” 摘要，或明确指向 architecture/proto。
2. pull 的“broker 已接受还是 pre-accept 拒绝”目前通过错误码推断。现有危险
   分支已封住；若以后扩展 transfer 协议，可考虑在 prepare response 加
   `Accepted`/`Tracked` 字段，降低 CLI 与错误码集合的耦合。

以上均非本轮 blocker。

## Verification

- 新增定向回归：PASS，`-count=10`
- transfer CLI E2E：PASS，`-count=10`
- `go test ./... -count=1`：PASS
- 受影响包及 CLI E2E `-race`：PASS
- `golangci-lint run`：PASS，0 issues
- `git diff --check`：PASS

## Final Result

PASS。当前暂存区外合规改动可交付；本轮 reviewer 修改保持未暂存。
