Pass

# S6–S8 外部重审报告（round 8）

日期：2026-07-17
对象：round-7 唯一阻断项 B3-2 的窄重审

## 结论

**Pass，可以放行。** B3-2 已关闭：锁文件以 `O_NOFOLLOW` 打开，符号链接在 fd 产生前即被拒绝；打开后又在
任何 chown/flock 前验证为普通文件。外审原 RED、开发者的 dangling-symlink/FIFO/happy-path 回归、聚焦 race
及 Linux/Darwin 构建全部通过。没有发现新的重大正确性、安全或生命周期问题。

本裁决只确认 round-7 阻断项及其直接回归；不重新打开已经裁定关闭的 B1/B2/B3-1，也不把既有登记项升级为
本轮阻断。

## B3-2 裁决

`internal/cluster/datadirlock.go` 当前顺序为：

1. `O_RDWR|O_CREATE|O_NOFOLLOW` 打开最终 lock 路径；symlink 以 ELOOP 明确拒绝。
2. 对已打开 fd 执行 `Stat`，非普通文件在所有权变更前拒绝。
3. 仅对通过验证的 fd 镜像 data-dir uid/gid。
4. 取得非阻塞 flock，并由 release closure 解锁、关闭。

该顺序消除了 `Lstat`→`OpenFile` 式 TOCTOU；路径在 open 后被替换也不会改变已验证 fd 指向的 inode。正常新建、
已有普通锁、第二 holder 拒绝及 release 后重取行为均保持。

开发者对同类 root-run offline 写入点的边界说明与源码一致：备份/manifest/dump 使用 `O_EXCL`，journal 使用随机
`CreateTemp` 与 no-follow 读取，目录属主处理使用 `Lchown`。本轮未发现另一处相同的符号链接跟随-chown 链。

## 疑惑与非阻断建议

1. `chownLockToDataDirOwner` 仍把 stat/chown 失败作为 best-effort 忽略。在当前受支持的本地 Linux/Darwin 数据盘
   和正常权限模型下不会影响放行；若未来声明支持 root-squash NFS 或特殊 ACL 文件系统，建议改为返回错误，避免
   recovery 成功后 daemon 才暴露 EACCES。
2. 少量旧注释仍描述“daemon 不持有 tether.lock”或“offline 必然留下 root-owned lock”，与当前实现不一致；属于
   维护性问题，不影响运行时裁决。
3. 本轮未重跑 sim-cluster：问题是本地 `open(2)`/fd 文件类型语义，聚焦回归和两个目标 OS 的构建已直接覆盖；
   重跑远端 drill 不会增加有效判据。

## 独立验证证据

- `TestS6S8Round7DataDirLockRefusesSymlink`：由 RED 翻为 PASS。
- developer round-7 dangling symlink、FIFO、clean acquire/互斥/重取回归：全部 PASS。
- `go test -race ./internal/cluster ./internal/clusteroffline ./internal/broker -count=1`：全部 PASS。
- CGO=0 构建：linux/amd64、darwin/amd64、darwin/arm64 全部 PASS。
- `git diff --check` 与 staged diff check：通过。

## Release disposition

Pass。B3-2 关闭，本轮无重大问题，S6–S8 该次外部复审可放行。
