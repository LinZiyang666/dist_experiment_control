Fail

# S6–S8 外部重审报告（round 7）

日期：2026-07-17
对象：round-6 唯一阻断项 B3-1 的窄重审；不重开 B1/B2 或既往范围

## 结论

开发者已经正确关闭 B3-1：五个 offline 入口全部改用 `cluster.AcquireDataDirLock`，旧的
`clusteroffline.acquireFlock` 已删除，真实 ForceSingle 调用点和结构守卫均能约束接线。本轮没有发现该修复
在互斥、释放或跨平台构建上的回归。

但共享 helper 在 root-run offline 的既定信任边界上会跟随 `tether.lock` 符号链接，并随后对目标执行
`f.Chown`。这使服务账号可借下一次 documented `sudo ... recovery` 改变任意 root 文件属主，属于本地提权
风险，不是边角 hardening。因此本轮仍为 **Fail**，仅保留这一项；修复后即可窄复核放行。

## 已关闭：B3-1

- ForceSingle、Resnapshot、Recover、InitFromExisting、RestoreFromBackup 均直接调用
  `cluster.AcquireDataDirLock(opts.DataDir)`。
- 旧私有 helper 已完全删除；非测试生产源码没有第二条锁获取路径。
- 聚焦测试确认锁的互斥、释放后重取、Broker.Run 拒绝并释放，以及真实 ForceSingle 创建公共锁。

## 唯一阻断项

### B3-2 — root-run recovery 跟随 service-owned lock symlink 并 chown 目标

生产部署的 `/var/lib/tether` 由 `tether` 用户拥有并可写（`scripts/install.sh:491`），runbook 则明确使用
root 执行 offline recovery（`docs/cluster-runbook.md:345-357`）。因此已被攻陷的 `tether` 账号能在 daemon
停止后删除/替换 `${DataDir}/tether.lock`。

`internal/cluster/datadirlock.go:57-73` 使用普通 `os.OpenFile`，没有 `O_NOFOLLOW`，随后调用
`chownLockToDataDirOwner`；该函数在 `:86-102` 对已经打开的 fd 执行 `f.Chown(dataDirUID, dataDirGID)`。
攻击者可令 `tether.lock -> <root-owned system file>`，operator 下一次按文档运行 `sudo tether cluster recovery
force-single ...` 时，root 会打开链接目标并把其属主转交给 `tether`。对敏感可写配置或认证文件，这可升级为
root 权限。

独立回归 `TestS6S8Round7DataDirLockRefusesSymlink` 创建 lock→victim 链接；当前
`AcquireDataDirLock` 返回成功，测试稳定 RED，证明 symlink 被跟随。root 场景下的 chown 后果由紧邻调用链
直接成立，无需修改真实系统文件来复现。

建议用带 `O_NOFOLLOW|O_CLOEXEC|O_RDWR|O_CREAT` 的平台实现打开锁（Linux/Darwin 均需覆盖），并在 flock/chown
前用 `f.Stat` 确认是普通文件；不要用 `Lstat` 后再普通 `OpenFile`，否则存在 TOCTOU。chown 失败也建议返回
错误而非 best-effort 静默继续，避免再次留下 daemon 无法打开的锁。

## 疑惑与非阻断建议

1. `lockFileName` 目前只被测试引用，生产入口已经直接使用公共 helper；可后续删除这个仅测试常量，但不影响
   正确性。
2. `offline.go` 和 `cluster/offline.go` 仍有“offline 会创建 root-owned lock”及“daemon 不持有 lock”的旧注释；
   建议修正文案，非本轮 blocker。
3. 本轮未重复 sim-cluster：B3-1 与 B3-2 都是本地文件打开语义，远端 GREEN 不会覆盖或改变裁决。

## 独立验证证据

- 新增调用点测试及 round-5/6 锁测试：通过。
- 独立 symlink 对抗：**RED**，`AcquireDataDirLock followed a symlink`。
- race（沙箱外，允许 loopback）：clusteroffline 与 broker 通过；cluster 仅上述独立 RED 失败。
- CGO=0 构建：linux/amd64、darwin/amd64、darwin/arm64 均通过。
- 初次沙箱内 race 的 socket failures 属执行环境限制；沙箱外复跑已排除。

## Release disposition

Fail，仅剩 B3-2。增加 no-follow + regular-file 校验并让独立 RED 翻绿后即可放行；无需再次审查 B1/B2、
force-single journal 或远端 drill。

---

## 主进程回复（round-7）— 采纳，无驳回

**判得对，而且这是我第三次在同一类缺陷上栽跟头。** 我在 round-6 为修 R2（root 跑 recovery 留下 root:root
锁 → survivor 永久拒启）加的属主镜像，本身造出了**本地提权**：`AcquireDataDirLock` 用
`os.OpenFile`（**跟随符号链接**）打开 `tether.lock`，随后以 root 对**链接目标**执行 `f.Chown` —— 而 data dir
按设计就是 `tether` 可写、runbook 又教操作员 `sudo` 跑 recovery。于是 tether 账户可把**任意系统文件**
（`/etc/shadow`、unit 文件、别的服务的 DB）的属主改成自己。外审的独立回归稳定 RED，事实清楚。

**讽刺且必须记下**：我在 round-6 刚亲手修过 journal 的同款符号链接洞（`writeJournal` 的固定 `.tmp`），
却在同一轮的另一处**原样重造**。修复引入新缺陷 → 修复该新缺陷时再引入同类 —— 这正是"只补报告点名的那一处、
不扫同类面"的代价。

### 修复

| 项 | 改动 |
|---|---|
| **符号链接跟随（提权本体）** | `os.OpenFile(path, O_RDWR\|O_CREATE\|**syscall.O_NOFOLLOW**, 0600)` —— 符号链接直接 ELOOP 拒绝并给出明确错误；`O_CREATE` 仍能在路径不存在时正常建锁 |
| **非常规文件** | O_NOFOLLOW 挡不住在同一路径预置的 **FIFO/设备/目录**，而我们正要以 root chown 它。取锁后 `f.Stat()` **必须是常规文件**，否则拒绝（且 FIFO 上的 open 还可能阻塞） |
| **chown 的安全前提** | 属主镜像（round-6 必需行为）保留，但现在**只在"我们自己打开的、经证明非符号链接的常规文件"上执行** |

### 我主动扫了同类面（不只修被点名的那一处）

`grep` 全部 root-run offline 写入点，逐条判定：`init.go:356/395`、`manifest.go:210`、`offline.go:814`
均为 **`O_CREATE|O_EXCL`**（对已存在路径含符号链接直接 EEXIST，不跟随）✓ · `cluster/offline.go:408` 用
**`os.Lchown`**（设计上不跟随）✓ · `journal.go` 为 `CreateTemp`(O_EXCL 随机名)+`Rename`/`Remove`(不跟随
最后一段)，`readJournal` 已带 O_NOFOLLOW ✓。**该提权面已收敛到锁这一处，且已修。**

### 回归（比外审的更严，全部 mutation 验证）

`internal/cluster/datadirlock_round7_test.go`：
- **dangling 符号链接**（更毒的变体：`O_CREATE` 会在攻击者选定路径**创建**出 root 文件再 chown）——
  断言拒绝 **且目标未被创建**；
- **FIFO/非常规文件** 拒绝；
- **happy path 未被破坏**（干净 dir 仍可取锁/互斥/释放后重取）。

**Mutation 证明**：去掉 `O_NOFOLLOW` → 外审的回归 + 我的 dangling 回归**双双变红**；去掉常规文件校验 →
FIFO 回归变红；恢复 → 全绿。外审自带的 `TestS6S8Round7DataDirLockRefusesSymlink` 现为 **PASS**。

### 本轮硬闸

`make lint` 0 issues · `make test` **63 包全 ok / 0 FAIL / rc=0** · `go test -race ./internal/cluster
./internal/clusteroffline ./internal/broker` 全过 · **linux + darwin(arm64+amd64) 构建通过**。

### 仍未闭合（沿用登记，不冒充闭合）

`InterruptedForceSingle` 无生产调用者 · `blockAfterAttempts` 需 step-aware + `AbortOp`/`ConfirmOp` 守卫 ·
B2 的 EBUSY/ENOSPC 残留（rename 已崩溃一致、事务尚未）· prune→exchange 窗口需 phase-aware 跳过。
