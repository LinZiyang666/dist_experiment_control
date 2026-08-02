Fail

# upgrade-safety 独立外部复审（round 2，开发者修复基线）

日期：2026-08-01

## 结论

开发者对 round-1 F1-F7 的方向性处置基本正确，原 4 个 reviewer 反例、F2 真实 binary 黑盒、
跨 Agent flock 测试和基础 durability 顺序测试均已翻绿。但本轮仍发现 **2 个 Major、4 个 Minor**，
因此此开发者基线暂不放行。最重要的两个问题都位于新引入的共享升级域实现：install 与
watchdog/commit 的锁顺序相反，能在 deadline 边界永久死锁；Darwin 发布目标的运行映像证明仍退化为
可被 rename 替换的路径，重新打开伪提交窗口。

本报告记录“开发者修复后的客观基线”。按用户授权，报告与开发者成果加入 index 后，审查者将直接
修复下列问题；修复和最终报告保持 unstaged，供 staged/unstaged 对比。

## Findings

### F8 — Major：install 与 watchdog/commit 构成 ABBA 永久死锁

位置：`internal/agent/upgrade.go:99-101,581-596`、
`internal/agent/upgrade_state.go:527-547,607-676`。

install 先持有 host flock，再在 marker 写入时获取 `upgradeMu`；watchdog 和 commit 则先持有
`upgradeMu`，再阻塞获取 host flock。旧 pending 到达 deadline 后，重试 install 已被入口门放行；若
此时 watchdog 触发或迟到 register 正在 commit，就会形成：

1. install：持 host flock，等待 `upgradeMu`；
2. watchdog/commit：持 `upgradeMu`，等待 host flock。

双方都没有超时或取消点，agent 的升级状态机永久卡死。独立子进程测试
`TestUpgradeHostLockOrderDoesNotDeadlock` 精确走生产顺序，当前稳定在 4s deadline 超时；子进程隔离
保证红测不向主测试进程泄漏死锁 goroutine。

建议：host flock 已经串行化所有 marker/prev/dst RMW，install 内不要再获取 `upgradeMu`；或统一所有
路径为同一全序，但不能保留当前反序。增加 stale deadline + retry install + watchdog/late-register 的
真实调用链回归。

### F9 — Major：Darwin 的“运行映像 SHA”仍是被替换路径，F1 伪提交未跨平台闭合

位置：`internal/agent/upgrade_state.go:439-458`、`build/goreleaser.yaml:41-47`。

Linux 使用 `/proc/self/exe`，确实读取当前进程 inode；非 Linux fallback 调用 `os.Executable()` 后按路径
重开。项目正式发布 Darwin amd64/arm64，因此这不是非支持平台。flip 后旧目标进程按该路径看到
NewSHA；如果同宿主另一个 boot 已令全局 BootCount 非零，则 target identity、BootCount、path SHA 三项
全部满足，旧目标仍可在 re-exec 前的 register 窗口报告 committed。

独立反例 `TestUpgradeCommitProofCannotUseReplacedPathFallback` 模拟当前非 Linux fallback，稳定得到旧
target 报告 `committed`。完整 Darwin 二进制可以交叉构建通过，但构建成功不能证明运行 inode 语义。

建议：使用 marker 中目前尚未参与证明的 `upgrade_id` 形成 process-local boot proof：只有本进程实际
执行 `BootUpgradeCheck` 的 staged 分支才获得该 id，commit 必须同时匹配 target 和 id。这样不依赖
`/proc`，且旧进程无法从磁盘路径借用证明。运行映像 SHA 可作为 Linux 的额外防御，而不能作为 Darwin
唯一证明。

### F10 — Minor：boot shim 不识别 Cobra 合法的 `--bool=true` 形式

位置：`cmd/tether/main.go:108-127`。

识别器只排除裸 `--install-user-service`、`--uninstall`、`--help`。Cobra 同样接受
`--install-user-service=true`、`--uninstall=true`、`--help=true`；这些非 daemon 操作会错误消费 boot
budget，甚至触发 rollback。独立表驱动测试的三个 true 子项当前均红。

建议：只对这三个 bool flag 做最小的 `name[=value]` 解析；true/裸值排除，显式 false 仍视为 daemon。

### F11 — Minor：marker 目录 fsync 失败留下可见 pending，健康旧进程被挡 120s

位置：`internal/agent/upgrade_state.go:203-230`、`internal/agent/upgrade.go:581-587`。

`writeUpgradeMarker` 在 rename 成功、目录 fsync 失败时返回 error；调用方删除 prev 并返回
install_failed，却没有移除已经可见的 pending marker。dst 仍是健康旧版本，但所有重试会命中
`upgrade_in_progress` 直到 deadline。独立测试在第二次 dir sync 注错，当前读到 live pending。

建议：pre-flip marker 写失败时补偿删除 marker 和 prev，并 best-effort sync 目录；不得把未 flip 的失败
事务留成 live pending。

### F12 — Minor：exec-failure rollback 丢弃 host-lock 错误并让唯一旧进程退出

位置：`internal/agent/upgrade.go:316-334`。

`recoverFromFailedExec` 丢弃 `withUpgradeFileLock` 的错误；lock open/flock 失败时 `handled=false`，调用方按
“无 pending marker”执行 `os.Exit(1)`。这与该函数最核心的不变量相反：syscall.Exec 失败后旧进程映像
是唯一仍活着的 known-good 代码。独立测试把 lock path 变成目录以确定性注错，当前返回 false。

建议：flock 对 EINTR 重试；真正 lock 失败必须响亮记录并把该路径视为 handled，保留旧进程在线，不能
把“无法检查 marker”解释成“没有 marker”。watchdog 的同类 `_ = withUpgradeFileLock` 也应至少记录。

### F13 — Minor：F7 文档只修了一处，coverage owner 仍互相矛盾

位置：`docs/reviews/simcluster-coverage-inventory.md:352,395`、`test/simcluster/README.md:306`、
`test/simcluster/drills/31-node-upgrade-fleet.sh:34,205`。

inventory 主表已改为“#28 已修、success drill 不存在且无 owner”，但同文件后两处仍写“#28 墙”；README
仍把 31 drill 记为 PRODUCT-RED/#28 未修；31 drill 又宣称 success 由 dedicated drill 拥有，而该 drill
不存在。F7 只部分闭合。

建议：统一为：#28 已修；31 覆盖 allowlist flip 和 fleet control surface；真实 re-exec/re-register/
rollback deploy-tier 仍 NOT-COVERED，尚无 owner，不声称被 #28 阻断。

## 验证

通过：

- round-1 四个 reviewer 反例均翻绿
- F2 真实 binary pre-Cobra crash-loop 黑盒通过
- F5 strict epoch、F6 基础 sync 顺序、跨 Agent flock 回归通过
- `GOOS=darwin GOARCH=arm64 CGO_ENABLED=0 go build ./cmd/tether`
- `make test` 中除本轮新增 reviewer 红测外的全部包通过；broker 321.316s、cluster、p1-p13、security、
  architecture、concurrency、determinism 等均绿
- `git diff --check`

按预期失败：

- `TestUpgradeHostLockOrderDoesNotDeadlock`（F8）
- `TestUpgradeCommitProofCannotUseReplacedPathFallback`（F9）
- `TestIsAgentDaemonInvocationBooleanForms` 的 true flag 子项（F10）
- `TestMarkerDirSyncFailureDoesNotLeavePendingTransaction`（F11）
- `TestRecoverFromFailedExecKeepsOldProcessOnLockFailure`（F12）

simcluster 当前无残留；服务器仍使用 `127.0.0.53` stub resolver，相关 drill 的 DNS fidelity 前置尚未
满足。本轮未使用 fake-DNS 绕过，也未把 SETUP 状态算作产品 verdict。
