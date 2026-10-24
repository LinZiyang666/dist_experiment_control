# PTY master 测试与接线报告

状态：局部实现已由 root 应用，focused overlay 验证通过；完整 PTY 热更新尚未完成。

本报告在用户要求停止实现、转交 Claude 时归档。下方是原始提案交付收据，保留“当时未应用”的历史陈述；它们描述提案交付时的状态。**随后 root 已应用** `internal/pty/pty.go`、`master_linux.go`、`master_other.go`。交接时只读计算得到的三个文件 SHA 与提案逐文件 SHA 完全相同，见末尾。这不是一次新的测试运行，也不把 overlay 结果改称整栈或部署后 Pass。

测试文件 `internal/pty/master_poll_test.go` 已在工作树。正式版本控制/清单登记状态由 root 的总交接掌管，本 lane 没有 stage/commit 或修改清单。后续接线任务见 [失败路径与连接层交接](hot-update-handoff-failures.md)。

## 原始提案交付记录

Pass — proposed Linux PTY master preparation passed the focused overlay checks. Not applied to production; not complete PTY handoff.

## Delivery

- Proposed patch: `/tmp/tether-control-recovery/pty-master-poll-proposed/proposed.patch`.
- Base production `internal/pty/pty.go` SHA256: `7ff8287bf963e66c5373fda93896303783d78d7465580bfb501d6179a94aedf1`. Exact per-file delivery hashes: `delivery-sha256.json`.
- `git apply --check` succeeds on the current tree. Production pty.go bytes are unchanged from preparation start. Only `internal/pty/master_poll_test.go` was added to the repository by this lane.
- Patch edits pty.go and adds `master_linux.go` / `master_other.go`. Root retains production application and doc-first decisions.

## Smallest implemented change

`Allocate` prepares the master before constructing Session. Linux preparation duplicates the original master using F_DUPFD_CLOEXEC inside RawConn.Control, sets shared O_NONBLOCK before os.NewFile, validates both deadline directions, then closes the obsolete original wrapper. The same kernel PTY survives, including slave pairing. Every preparation failure releases the duplicate; Allocate closes the original pair. No File.Fd call is used.

Linux `SetSize` uses RawConn.Control + unix.IoctlSetWinsize. This avoids creack/pty v1.1.24's File.Fd compatibility behavior, including for an already-owned legacy wrapper whose O_NONBLOCK has been repaired. Non-Linux setup/resize retain their prior behavior; Darwin fallback compiles.

The helper runs only while Allocate exclusively owns both ends, before Start or any I/O. It is not a generic concurrent transfer helper. Shared O_NONBLOCK changes must not race another owner. Successful Close of the replaced wrapper does not close the duplicated master endpoint.

## Exact test receipts

| Check | Original result | Log under /tmp/tether-control-recovery |
|---|---|---|
| Original production, 5 new Test functions, -race | FAIL rc1, 0.034s: four pollability assertions see status 0x8002 without O_NONBLOCK | pty-master-poll-before.log |
| Proposed overlay, same 5 Test functions, -race ×3 | PASS rc0, 1.051s | pty-master-poll-overlay.log |
| Existing allocation, direct/helper spawn, process-owner and adoption tests, -race | PASS rc0, 1.108s | pty-master-poll-adjacent.log |
| Only proposed resize changed back to File.Fd in an overlay | FAIL rc1, 0.033s: original borrowed-File test sees O_NONBLOCK cleared | pty-master-poll-resize-negative.log |
| GOOS=darwin go build -overlay=... ./internal/pty | PASS rc0 | pty-master-poll-darwin-build.log |

Initial sandbox setup rejection (Go cache read-only) is separately preserved as `pty-master-poll-sandbox-denied.log`; it is not represented as a product failure. Authorized rerun had cache access. No make gates or lint was run.

Tests observe actual descriptor flags, unchanged dev/inode, actual kernel winsize, deadline read termination while the original slave remains open, and bytes over that same pair afterward. A five-round resource test uses repository goroutine/FD gates with FD tolerance 0. These do not establish the whole-image 500ms freeze budget.

## Started PTY EIO policy for the next adapter (recommendation, not included in this patch)

A real Start/Wait test confirms an original child exits 23, its complete tail remains readable after Wait, and then master.Read yields syscall.EIO with the master descriptor still open. Linux N_TTY returns EIO for TTY_OTHER_CLOSED (last slave closed): https://github.com/torvalds/linux/blob/master/drivers/tty/n_tty.c .

Introduce an explicit validated **started PTY master** output mode; serialize that mode in the adapter state. Only that mode maps last-slave-close EIO to output FIN, after retaining every n>0 byte. Preserve arbitrary errors elsewhere. Current generic Output treats EIO as ReadError, and this patch intentionally does not silently change all streams' error meaning.

PTY input must use a separate writer adapter: logical input FIN must not close the duplex master or inject Ctrl-D. Keep one authoritative master resource shared by input/output views, export it once, and handle both direction deadlines under the terminal owner's freeze barrier. Session Start-ready ownership, control/resize sequencing, liveness deadlines and final teardown still need the runtime integration described in `hot-update-pty-stream-wiring.md`.

## New test inventory for root

- `TestMasterAllocateIsPollableAndResizeKeepsIdentity`
- `TestMasterResizeDoesNotReblockBorrowedFile`
- `TestMasterReadDeadlineAfterResizePreservesOriginalPair`
- `TestMasterStartedChildTailPrecedesKernelEIO`
- `TestMasterPollableLifecycleReleasesResources`

Resource anchor to register: `internal/pty/master_poll_test.go:TestMasterPollableLifecycleReleasesResources` → `exerciseMasterReadDeadline`. Its loop uses local `const rounds = 5`; no scanner/golden was modified here.

## 停止实现时的已应用文件 SHA256

```json
{
  "internal/pty/pty.go": "2675dfb6500440be61434c60464d87dc238818f6a61d56fe6c96eabde653add4",
  "internal/pty/master_linux.go": "95dda064db0f0c60c090f1183d042950eeefcc9117d1b53c6c4d48e02dd75bca",
  "internal/pty/master_other.go": "df0248a8cb0bb5f9fafcd1497d5b53bab43df535bca6a144dda2e7243e71a53b",
  "internal/pty/master_poll_test.go": "33c5d569fe1375ab3810b3bf9c3fe1f52a874e09b46208808f6825900ec8a3be"
}
```
