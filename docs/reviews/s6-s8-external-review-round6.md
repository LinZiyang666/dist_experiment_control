Fail

# S6–S8 外部重审报告（round 6）

日期：2026-07-17
对象：round-5 回复后的暂存区外修改；只复核 B1/B2/B3 与 `out_matches`，不重开既往范围

## 结论

本轮修复已实质关闭 B1 与 B2，也正确建立了 daemon 全生命周期锁；但 B3 的锁文件属主修复没有接到
任何 offline 生产调用点。按现行 runbook 执行 `sudo tether cluster recovery force-single` 时，缺失的
`tether.lock` 仍由旧实现创建为 `root:root 0600`，之后 `User=tether` 的唯一 survivor 无法打开它并拒绝
启动。这是明确的恢复路径可用性回归，故本轮仍为 **Fail**。除此之外未发现新的重大问题；修复这一处
调用链后即可窄复核放行，无需再扩大审查。

## 唯一阻断项

### B3-1 — offline 路径未使用带属主修复的公共锁实现

`internal/cluster/datadirlock.go:57-81` 的 `AcquireDataDirLock` 会在打开锁后把其 uid/gid 镜像为 data-dir
属主，且能区分“锁被占用”和“锁不可访问”。`Broker.Run` 在
`internal/broker/broker.go:636-660` 确实使用它并持有到退出，连续互锁本身成立。

然而所有 offline 入口仍调用 `internal/clusteroffline/offline.go:923-935` 的旧 `acquireFlock`：

- force-single：`internal/clusteroffline/offline.go:79`
- resnapshot：`internal/clusteroffline/offline.go:235`
- recover：`internal/clusteroffline/offline.go:713`
- init：`internal/clusteroffline/init.go:155`
- restore：`internal/clusteroffline/restore.go:75`

旧函数只执行 `OpenFile(..., 0600)` 和 `Flock`，没有 chown。`docs/cluster-runbook.md:355` 又明确要求以
`sudo tether ...` 运行 offline force-single，而安装脚本只创建 tether-owned data dir、没有预创建锁。
因此在锁尚不存在的部署上，root 会创建 `root:root 0600` 的锁；恢复结束或甚至前置 gate 零修改拒绝后，
systemd 的 `User=tether` broker 在 `AcquireDataDirLock` 的 open 阶段即 EACCES，尚未到能够修正属主的代码。

开发者新增的 `TestRound6_LockFileInheritsTheDataDirOwnership` 只直接调用公共函数，没有经过 ForceSingle 或
任一 offline 入口，因此没有覆盖这条断开的接线。

建议删除重复的 `clusteroffline.acquireFlock`，让五个入口统一调用
`cluster.AcquireDataDirLock(opts.DataDir)`；补一条针对真实 offline 调用点的属主回归。作为纵深防御，安装时
也可预创建 tether-owned `tether.lock`，并将 runbook 改为 `sudo -u tether`，但两者不应代替公共调用链统一。

## B1/B2/B3 裁决

| 项目 | 裁决 |
|---|---|
| B1 可恢复 force-single | 通过本轮目标：journal 在首次 mutation 前落盘，随机临时文件与 `O_NOFOLLOW` 修复了 symlink 风险；resume 只继承已从当前 roster 消失的 peer 确认；去集群化失败非零，成功后才清 journal。 |
| B2 原子交换 | 通过：能力检查在 mutation 前、位于相同文件系统；非原子 fallback 与 `.pre-rebuild` 删除路径已移除；非 Linux 明确拒绝且 Darwin 可构建。 |
| B3 连续互锁 | 机制通过、接线未完全通过：daemon 生命周期锁与互斥/释放均成立，但 offline 仍走旧实现，形成上述属主阻断。 |
| `out_matches` | 通过：现在同时要求命令 rc=0 与正则匹配；“先输出命中串再 exit 9”不再假绿。 |

## 非阻断疑惑与建议

1. `InterruptedForceSingle` 当前仍只被测试和 helper 自身引用，没有接入 broker 启动错误或 status。journal
   已保证可重入，故本轮不再阻断，但“启动诊断可指名中断恢复”的回复表述尚未兑现，建议后续接线或收窄注释。
2. `internal/cluster/offline.go:40` 仍写“production daemon does NOT take tether.lock”，与新实现相反；建议修正文档，
   避免未来维护者误删连续互锁。
3. 原子交换能力探针只能证明当时同挂载点支持交换，不能消除随后 ENOSPC/EBUSY 等运行时错误；现有 journal
   与重跑路径已把该残余风险降到可接受，不作为本轮 blocker。

## 独立验证证据

- journal symlink 对抗、round-5/6 callsite 测试：通过。
- 聚焦 Go：`go test ./internal/cluster ./internal/clusteroffline ./internal/broker ./cmd/tether`：全部通过。
- race：`go test -race ./internal/cluster ./internal/clusteroffline ./internal/broker`：全部通过。
- shell：`sh -n`、`dash -n`、verdict contract、S6–S8 drill lint：全部通过。
- `out_matches`：rc=9 + 命中输出返回失败；rc=0 + 命中输出返回成功。
- 构建：CGO=0 的 linux/amd64、darwin/amd64、darwin/arm64：全部通过。
- 本轮未重复远端 sim：开发者提交的 drill 20/91 均为 GREEN，且唯一 blocker 是可由本地生产调用链
  直接判定的文件属主问题，远端复跑不会改变裁决。

## Release disposition

Fail，仅剩 B3-1。把 offline 五个入口统一到 `AcquireDataDirLock` 并以真实调用点回归后，可直接进行一次
窄重审；不要求重新审查 B1/B2 或重跑整套 S6–S8。

---

## 主进程回复（round-6）— 唯一重大项已修，采纳无驳回

**判得对，且这条正戳中要害**：我在 round-6 自审查里"修好"的 R2（root 跑 recovery 留下 root:root 锁 →
`User=tether` 的 survivor 永久拒启），**只加在了 `cluster.AcquireDataDirLock` 里**，而 offline 的**五个入口
仍调用本地的旧 `acquireFlock`** —— 属主修复**根本没接到真实的 force-single 路径上**。按 runbook 的
`sudo tether cluster recovery force-single` 依旧会造出 root:root 0600 的锁。修了一个函数，却没接线，
和我这几轮反复批评的「fix 与调用点脱节」是同一个病。

### 修法：不是补五处调用，而是让不安全的路径**不复存在**

| 项 | 改动 |
|---|---|
| 五个入口 | `internal/clusteroffline/offline.go:79`(ForceSingle) · `:235`(Resnapshot) · `:713` · `init.go:155` · `restore.go:75` 全部改用 **`cluster.AcquireDataDirLock(opts.DataDir)`**（取锁时镜像 data-dir 属主 + 区分权限/争用） |
| 旧 helper | **`func acquireFlock` 整体删除**。只补五处调用会把一个"属主不感知的锁"留在原地当陷阱——下一次编辑就能悄悄把 R2 装回去。现在**不安全的路径在代码里不存在** |
| 既有测试 | `offline_test.go` 的 `TestD7OfflineFlockExclusive` 与 round-5 B3 测试改用共享锁（同一属性、正确 helper） |

### 新增回归（mutation 验证）

- `TestRound6_NoPrivateFlockHelperSurvives` —— 扫描本包非测试源码，**禁止任何 `func acquireFlock(` 复活**。
  **Mutation 证明**：把私有 helper 装回 ForceSingle → **该测试精确变红**；恢复 → 绿。这是本条 bug 的
  **可移植、必然生效**的钉子。
- `TestRound6_ForceSingleLeavesADaemonOpenableLock` —— 在**真入口**断言 ForceSingle 建出的锁属主 == data-dir
  属主。**如实标注其局限**：非 root 跑时两种 helper 产出的属主相同、抓不到，它只在 root 跑非 root 目录
  （即真实 runbook 场景）才真正生效；可移植的钉子是上面那条结构性守卫。**不冒充它是通用回归。**

### 本轮硬闸

`make lint` 0 issues · `make test` **63 包全 ok / 0 FAIL / rc=0** · `go test -race ./internal/clusteroffline
./internal/cluster ./internal/broker` 全过 · **linux + darwin(arm64) 构建通过**。

（部署层 drill 20 GREEN(14) / 91 GREEN(37) 的证据见 round-5 报告尾部；本轮改动只把锁调用点换成同语义的
共享实现 + 删死代码，未改任何 drill 行为。）

### 仍未闭合（沿用登记，不冒充闭合）

`InterruptedForceSingle` 无生产调用者 · `blockAfterAttempts` 需 step-aware + `AbortOp`/`ConfirmOp` 守卫 ·
B2 的 EBUSY/ENOSPC 残留（rename 已崩溃一致、事务尚未）· prune→exchange 窗口需 phase-aware 跳过。
