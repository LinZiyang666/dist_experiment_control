# L03 — CLI 层（`cmd/tether` + `internal/cli`）结构性质量审计

> 横切质量审计 · 2026-07-25 · lane key = `cmd-cli-layer`
> 范围：`cmd/tether/` 全部生产文件 **14,611 行** + `internal/cli` 生产 **1,265 行**（合计 **15,876 行**）
> 这是**结构审计**（冗余 / 重复 / 抽象错位 / 演进阻力），不是缺陷审计。只读，未修改任何实现代码。

---

## 结论

**净判断：这个 CLI 层不是屎山。14,611 行里 cobra 样板只占 6.4%（934 行）——"CLI 层臃肿"这个假设在本 lane 是错的。真正的问题是另一件事：25%（3,673 行）的 `package main` 文件里一个 cobra 命令都没有，那是被放错层的编排 / 协议 / 身份校验逻辑；以及同一个"轮询等待"骨架被手抄了 8 遍，抄出了用户可见的语义分叉（`cluster join approve --wait` / `cluster retire --wait` 完全不响应 Ctrl-C，而 `cluster transfer-leader --wait` 响应）。**

**bloat 打分：4 / 10**（1=精炼，5=正常工程债，10=屎山）。

理由：

- **实测样板率极低**。用 `go/ast` 逐行归类 51 个生产文件后：`Use/Short/Long/Example/Args` 等命令元数据 569 行（3.9%），`Flags().XxxVar(...)` 注册 365 行（2.5%）。合计 934 行 = **6.4%**。一个 92 条命令路径的 CLI，平均每条命令 10 行样板，这是**低于**典型 cobra 项目的。"抽一个通用命令骨架"能省的上限就是这 934 行的一部分，投入产出极差——**我不建议做**（详见 F2 的正确切法）。
- **注释占 16.4%（2,398 行）**，远高于 Go 常规。逐条读过之后：绝大多数是"这个 gate 为什么存在 / 哪一轮 review 或哪个 drill 抓到的"的因果记录（例：`cluster.go:911-917` 记录 seam 必须插在 `broker:` 之后而非 EOF append 的原因）。这是本项目实际的缺陷防复发机制，**记为本质成本，不计入臃肿**。
- 扣分项集中在三处：① 8 个手写轮询循环语义分叉（F1）；② 22 处 ctl 连接前导码复制粘贴（F2）；③ 2,987 行非 CLI 逻辑锁死在 `package main`（F6）。
- 加分项很硬：exit code 分类 SSOT（`exitcode.go`）、broker code → 一句人话 + exit class 的单表（`error_hints.go`）、每个机器输出都带 `(schema, schema_version)` 且有成文 bump 政策（`jsonout.go`）、以及**整棵命令树 + 每个 flag + 每个 Hidden 位对 golden 断言**（`command_tree_inventory_test.go`）。最后这一条在 Go CLI 项目里非常罕见。

**verdict：minor-debt。**

---

## 范围与方法

| 项 | 数 |
|---|---|
| `cmd/tether` 生产行 | 14,611（51 文件） |
| `cmd/tether` 测试行 | 13,075（85 文件） |
| `internal/cli` 生产行 | 1,265（5 文件） |
| cobra 命令构造函数 `newXxxCmd` | 85 |
| `Use:` 条目（含 group 父命令） | 92 |
| golden 钉住的命令路径 | 94（构造树）/ 99（runtime 树） |
| 生产函数 | 353，其中 22 个 > 100 行 |
| `cluster_*.go` 生产文件 | 25 个 / 7,594 行 |

方法：
1. 用 `go/ast` 写了一次性只读脚本（`/home/weiland/.claude/jobs/cda1899e/tmp/an1.go`、`an2.go`），把每一行归类为 命令元数据 / flag 注册 / RunE 闭包体 / 其他代码 / 注释 / 空行 / import，并测量每个函数的行数。
2. 逐字读了 `cluster.go`(1037)、`cluster_add_drive.go` 前 180 行 + 全部函数签名、`cluster_offline.go` 前 150 行、`cluster_ops.go`(169 全)、`cluster_join.go`(202 全)、`cluster_wait.go`(183 全)、`ctl_connect.go`(170 全)、`exitcode.go`(104 全)、`jsonout.go`(159 全)、`error_hints.go`(243 全)、`serve.go` 的 `newServeCmd`(297)、`run.go`/`exec.go`/`ps.go` 的 RunE、`internal/cli` 全部 5 个生产文件。
3. 未运行 `make test` / `make e2e` / simcluster。

### AST 逐行归类结果（全 `cmd/tether` 生产文件）

| 类别 | 行数 | 占比 |
|---|---:|---:|
| RunE / Run 闭包体 | 3,207 | 22.0% |
| 其他代码（helper 函数、类型、包级 var） | 6,843 | 46.8% |
| **注释** | **2,398** | **16.4%** |
| 空行 | 835 | 5.7% |
| 命令元数据（Use/Short/Long/Example/Args/Hidden…） | 569 | 3.9% |
| import 块 | 445 | 3.0% |
| flag 注册（`Flags().XxxVar` + `MarkFlags*`） | 365 | 2.5% |

**回答 Q1（一个 CLI 层凭什么要 14,611 行）**：不是靠样板堆的。样板（元数据 + flag 注册）= 934 行 = 6.4%。真正的 14.6k 由三块构成：
- **22.0% RunE 闭包体**——参数校验 + 一次 RPC + 渲染，属于 CLI 本分，但其中有约 400 行是不该待在闭包里的业务逻辑（见 F8）。
- **46.8% "其他代码"**——这才是重点。其中 **3,673 行分布在 17 个"零 cobra 命令"的文件里**（下表），它们连一个 `&cobra.Command{}` 都没有。
- **16.4% 注释**——设计因果记录，判为本质。

#### 零 cobra 命令的 `package main` 文件（3,673 行 = 25.1%）

| 文件 | 行 | 性质 |
|---|---:|---|
| `cluster_add_drive.go` | 887 | grow 编排状态机（P0–P9，可恢复 / HALT-on-refusal） |
| `cluster_upgrade_drive.go` | 452 | 滚动升级编排状态机 |
| `cluster_status_nats.go` | 293 | over-NATS 集群健康广播客户端 + 折叠 |
| `error_hints.go` | 243 | **CLI 本分**：broker code → 人话 + exit class |
| `cluster_secrets.go` | 217 | account/broker nkey 与 nats.conf 的 skew 交叉校验 |
| `d8_alerts.go` | 197 | 破坏性操作前的 alert gate 客户端 |
| `cluster_wait.go` | 183 | 3 个轮询引擎 |
| `ctl_connect.go` | 170 | 连接 + 签名 manifest 采纳（PIN 门 A） |
| `jsonout.go` | 159 | **CLI 本分**：--json schema |
| `cluster_lock_keeper.go` | 155 | grow/roll 租约续约 keeper（后台 goroutine） |
| `cluster_status_card.go` | 149 | **CLI 本分**：渲染 |
| `cluster_rotation.go` | 122 | 轮转 admin 客户端 |
| `node_versions.go` | 120 | broker/agent 双版本关联 |
| `cluster_doctor_online.go` | 114 | 在线 doctor 探测 |
| `exitcode.go` | 104 | **CLI 本分**：exit code 分类 |
| `cluster_offline_wizard.go` | 77 | 交互向导 |
| `logging.go` | 31 | **CLI 本分** |

其中 **686 行是正当的 CLI 层职责**（渲染 / exit code / hint / json），**2,987 行是编排、协议客户端、身份校验**——见 F6。

---

## Findings

### F1 — 8 个手写轮询循环，Ctrl-C / 瞬态重试 / 超时 exit class 三条轴全部分叉 · **high**

**证据**

| 位置 | 循环形态 | ctx 取消 | 瞬态错误 | 超时 exit |
|---|---|---|---|---|
| `cmd/tether/cluster_wait.go:37` `watchClusterStatus` | `NewTicker` + `select ctx.Done()` | ✅ 自建 `signal.NotifyContext` | 重试 | 无超时 |
| `cmd/tether/cluster_wait.go:82` `waitForConverge` | `NewTicker` + `select ctx.Done()` | ✅ | 重试 | **75**（`ExitError{exitTransient}`） |
| `cmd/tether/cluster_wait.go:146` `settleClusterStatus` | `NewTicker` + `select ctx.Done()` | ✅ | 重试 | 返回 last report |
| `cmd/tether/cluster_join.go:169` `waitForOp` | `time.Sleep(2s)` | ❌ **完全不读 ctx** | **首错即 return** | **75** |
| `cmd/tether/cluster_reconcile.go:120` topo 收敛 | `time.Sleep(2s)` | ❌ **完全不读 ctx** | **首错即 return** | **70**（裸 `fmt.Errorf`） |
| `cmd/tether/cluster_add_drive.go:379` `waitJoinServing` | `select` + `time.After` | ✅ | 静默重试 | **70**（裸 `fmt.Errorf`） |
| `cmd/tether/cluster_add_drive.go:496` `waitOpCatchingUp` | 同上 | ✅ | 静默重试 | 70 |
| `cmd/tether/cluster_add_drive.go:691` `awaitJoinerBrokerUpLocal` | 同上 | ✅ | 静默重试 | 返回 bool |
| `cmd/tether/cluster_lock_keeper.go:90` 租约续约 | `NewTicker` | ✅ | 重试 | n/a |

**为什么是债（具体阻碍什么改动）**

1. **用户可见后果 A：两条最长的命令按不了 Ctrl-C。** `main.go:68` 用 `signal.NotifyContext` 接管了 SIGINT/SIGTERM——**进程的默认终止行为已被替换**，第一个信号只是 cancel ctx（且 `NotifyContext` 内部那个 goroutine 收到第一个信号后就退出，后续 SIGINT 落进无人读的 1 容量 buffered chan 被丢弃）。`waitForOp` 完全不读 ctx，于是 `tether cluster join approve --wait`（默认 5 min，`cluster_join.go:163`）和 `tether cluster retire --wait`（默认 10 min，`cluster_retire.go:144`，两处调用 `cluster_retire.go:86/137`）**按几次 Ctrl-C 都不会退出**，只能 SIGQUIT/SIGKILL。同一个二进制的 `cluster transfer-leader --wait` 和 `cluster status --watch` 却正常响应。
2. **用户可见后果 B：同一语义的超时映射到两个 exit code。** `cluster reconcile nats --wait` 超时 → 裸 `fmt.Errorf` → `classifyExit` 落到 `exitInternal`=**70**（文档语义："tether 侧无法分类的故障"）；`cluster join approve --wait` 超时 → **75**（EX_TEMPFAIL，"可重试"）。运维脚本按 75 重试、按 70 报警，两个语义完全相同的"没在时限内收敛"会被区别对待。
3. **演进阻碍**：`--wait` 是集群 CLI 最高频的形状。每加一个 `--wait` 动词就重新推导一次这个循环，重新押注四个决策（tick 源、ctx、瞬态重试、超时 class），而每次只对一部分。已经 8 次了，8 种组合里没有两个完全一致。

**建议**：抽一个 `pollUntil(ctx context.Context, cfg pollCfg, step func() (done bool, err error)) error`，把「tick 间隔下限 / 自建 signal ctx / 瞬态重试 vs fail-fast（显式参数）/ 超时→75 / 取消→75」固化成默认，让每个调用点只写 `step`。`waitForOp` / topo 收敛 / `waitJoinServing` / `waitOpCatchingUp` 全部改写。

**量化**：净减 ~120 行（9 个循环的公共骨架 ≈ 9×25 行 → 一个 ~50 行的 primitive + 9 个 ~8 行的 step）。
**风险 medium**：修 Ctrl-C 是纯改进；但把 reconcile 超时从 70 改成 75 是**脚本可见的行为变更**，需要在 `docs/usage.md §9.13`（自动化重试指引）同步，且属于 `feedback-contract-change-sweep` 说的"改契约要全局扫调用点"。不触碰 wire，不触碰 architecture.md 不变量。

---

### F2 — ctl 会话前导码复制粘贴 22 遍；`--nats-url` 的默认值在同一二进制里有两套 · **high**

**证据**

固定五步前导码（`ReadCurrentSession` → "no active session" → `ResolveNATSURLFromHome` → `EnsureIdentity` → `connectCtl(... nats.Name(CtlNameForSession(sid)))`）：
`cmd/tether/ps.go:56-68`、`exec.go:47-72`、`run.go:69-89`、`expose.go:52-80`、`expose.go:178-190`、`expose.go:300-317`、`transfer.go:126-155`、`transfer.go:401-426`、`history.go:83-99`、`node.go:50-62`、`node.go:176-189`、`alert.go:134-146`、`alert.go:187-199`、`proxy.go:51-63`、`cluster_add.go:83-96`、`cluster_upgrade.go:60-72`、`cluster_unlock.go:140-152`、`cluster_seeds.go:119-131`、`cluster_status_nats.go:185-197`、`cluster_status_nats.go:277-289`、`session.go:37-47/95-107/155-167`。

计数：`cli.ReadCurrentSession` 22 处、"no active session — run \`tether login -s <sid>\` first" 字面串 **15 处逐字重复 + 3 处带括号变体**、`cli.EnsureIdentity` 24 处、`connectCtl` 25 处（其中 21 处参数形状完全一致）、`--nats-url` 注册 23 处、`--home` 注册 24 处。

**分叉**（`grep 'StringVar(&natsURL, "nats-url"'`）：
- 默认值 `"nats://127.0.0.1:4222"` + 帮助文本 `"NATS server URL"`：`ps.go:170`、`exec.go:149`、`run.go:288`、`expose.go:128/229/350`、`node.go:128/246`、`session.go:27`、`history.go:180`、`proxy.go:35`、`transfer.go:67/381`、`login.go:102`、`serve.go:290`、`agent.go:359`
- 默认值 `""` + 帮助文本 `"broker NATS URL"`：`alert.go:162/209`、`cluster_upgrade.go:159`、`cluster_unlock.go:164`
- 默认值 `""` + `"broker NATS URL (with --remote)"`：`cluster.go:193`、`cluster_seeds.go:109`
- 默认值 `""` + `"a live broker NATS URL (the trigger transport)"`：`cluster_add.go:136`

于是 `tether ps --help` 打 `(default "nats://127.0.0.1:4222")`，`tether cluster unlock --help` 什么默认都不打——**同一个 flag，同一个二进制，帮助页说两件事**。

**为什么是债**

`ctl_connect.go:17-20` 的文件头自己记录了这个模式的上一次代价："Every ctl-over-NATS subcommand connects through connectCtl instead of the bare `ResolveNATSURLFromHome+ConnectNATSWithNkey+connectError` triple, so failover ... live in exactly one place."——v0.4.5 阶段 4 加 broker 自动故障转移时，正因为这段三连被抄了 20 多遍，不得不逐个改。**这次只把最内层的 `connectCtl` 收拢了，外面那四步仍然是 22 份副本**，下一次跨切面变更（比如"每条 ctl 命令在连接前打一次 discovery cache 年龄"或"支持 `--session` 覆盖 current_session"）会原样再付一次同样的代价。

**建议**：加 `func ctlSession(cmd *cobra.Command, verb, home, natsURL string) (*nats.Conn, *cli.Identity, string, error)` 收掉五步；加 `func bindCtlFlags(cmd *cobra.Command, natsURL, home *string)` 一次性注册 `--nats-url`/`--home`，把默认值和帮助文本统一（这也顺手让 `command_tree_inventory_test.go` 的 golden 成为"帮助文本一致性"的守门人）。

**量化**：21 处 × ~11 行 → 21 × ~4 行，净减 **~150 行**；flag 注册 47 行 → 21 行，净减 **~26 行**。合计 **~180 行**。
**风险 low**：纯本地重构，命令树 golden 会在默认值统一时失败一次（这正是它的作用），需要有意识地 `-update-command-tree-golden`。

---

### F3 — grow 编排器 `exec` 自己的二进制并 grep 子命令 stdout · **high**

**证据**：`cmd/tether/cluster_add_drive.go:288-330`

```go
func runSelfInit(cmd *cobra.Command, jp joinerParams) error {
    self, err := os.Executable()
    args := []string{"cluster", "init", "--from-existing",
        "--self-id", jp.Joiner, "--name", jp.Name, "--node-ident-pub", jp.NodeIdentPub,
        "--raft-addr", jp.RaftAddr, "--nats-route", jp.NatsRoute, "--tunnel-addr", jp.TunnelAddr,
        "--public-host", jp.PublicHost, "--data-dir", jp.DataDir, "--db", jp.DBPath, "--secrets-dir", jp.SecretsDir,
        "--config", jp.ConfigPath, "--confirm-node-id", firstNonEmpty(jp.ConfirmNodeID, jp.Joiner)}
    c := exec.CommandContext(cmd.Context(), self, args...)
    ...
    return fmt.Errorf("cluster init: %v: %s", err, strings.TrimSpace(stderr.String()))
}
```

以及 `cluster_add_drive.go:310-329` `runSelfJoinPrepare`：exec `cluster join prepare`，然后**逐行扫 stdout 找 `tether-join:` 前缀**。

**为什么是债**

1. **12 个 flag 名变成了字符串形式的内部契约。** 改 `--node-ident-pub` 的名字，`go build ./...` 全绿、`make lint` 全绿、`make test` 全绿——然后在 joiner 主机上、grow 进行到 P2 时才炸，且此时 **grow lock 已经持有**（`cluster_add_drive.go:108` 的 `startLockKeeper` 在 P1 之后启动），HALT 留下一个需要 `cluster unlock` 才能清的半成品状态。
2. **子命令的 stdout 格式变成了内部契约。** 给 `cluster join prepare` 的 stdout 加一行前言，或改 bundle 前缀，`cluster add` 立刻断——没有任何编译期或测试期链接把两侧钉在一起（对比 `error_hints.go:135-138` 里 `dataplaneNotConvergedCode` 的处理方式：那里明确写了"两个包之间没有编译期链接，只有一个 wire 稳定性测试挡着"，说明项目自己知道这类风险，但这里没做同样的钉子）。
3. **exit class 分类在进程边界上被彻底抹掉。** 子进程精心分好的 64/69/70/75/77（`exitcode.go`）在父进程里变成 `fmt.Errorf("cluster init: %v: %s", err, stderr)` —— 一个未分类错误，最终 exit 70。`cluster add` 的调用者看不出"joiner 配置写错了（64）"和"leader 不可达（69）"的区别。

**建议**：`runSelfInit` 直接调 `clusteroffline.InitFromExisting(...)` + `applyClusterSeam(...)`（两者都已经是可直接调用的函数，见 `cluster.go:819` 和 `cluster.go:918`）；`runSelfJoinPrepare` 把 `cluster_join.go:46-119` 的 bundle 铸造体提成 `buildJoinBundle(params) (string, error)` 后直接调用。子进程唯一提供的额外东西是 TTY confirm 的绕过，而这条路径本来就已经用 `--confirm-node-id` + 环境变量绕过了。

**量化**：净减 ~45 行（去掉两个 exec 包装 + stdout 扫描），新增 ~20 行（`buildJoinBundle` 提取）。**净 ~-25 行，但这条不是为了减行**，是为了把三个运行时才炸的契约变成编译期契约。
**风险 medium**：改的是 grow 主路径，必须跑 `test/simcluster` 的 grow drill；不触碰 wire、不触碰 architecture.md 不变量。

---

### F4 — `--ack-alerts` 破坏性操作 gate 是复制粘贴的，覆盖面无任何结构性保证 · **medium**

**证据**

`gateDestructive`（`cmd/tether/d8_alerts.go:71`）在 6 个位置手工调用：`expose.go:83-85`、`expose.go:319-321`、`run.go:91-93`、`session.go:170-172`、`transfer.go:157-159`、`transfer.go:428-430`。对应 flag 用**字符串名**注册 6 次（`expose.go:140/354`、`run.go:290`、`transfer.go:69/383`、`session.go:210`），读取时用 `cmd.Flags().GetBool("ack-alerts")` 且**丢弃 error**（`run.go:91`、`expose.go:83/319`、`transfer.go:157/428`）。

唯一的覆盖面守卫是 `cmd/tether/d8_external_review_test.go:105-124`：一个**写死 4 个文件名的 allow-list**，对每个文件做 `strings.Contains(body, "gateDestructive(")` 子串检查。它无法发现"新增了一个破坏性动词但没加 gate"。

`cmd/tether/testdata/command_tree_golden.txt:71/73/89/90/91/96` 确认：带 `--ack-alerts` 的只有 `expose` / `expose rm` / `pull` / `push` / `run` / `session rm`。

**没有 gate 的破坏性动词**：`tether node upgrade <nid>|--all`（`node.go:136`）——**整个车队原子替换 agent 二进制并 re-exec**，比 `tether push`（拷一个文件，有 gate）破坏性高一个数量级；`tether exec`（远程执行任意 argv）；`tether proxy on`（把每个 agent 变成开放出口节点，它自己有一套独立的 `--yes` y/N 确认，见 `proxy.go:317`，但不查集群 alert）；`tether proxy sub revoke`。

**为什么是债**：quorum_lost / force_single_active 状态下运行 `node upgrade --all` 正是 `docs/reviews/quality-audit/` 之前记录的那类"在坏集群上做破坏性操作"场景。当前没有任何机制（接口、registry、构造器）能让"这个动词是破坏性的吗"在代码里可见，只有 6 处人工记忆。每加一条改变远端状态的命令，默认是**不设防**的。

**建议**：把 gate 变成结构性的——例如 `newMutatingCtlCmd(...)` 构造器统一注册 `--ack-alerts` 并在 RunE 前置 `gateDestructive`；再把 `d8_external_review_test.go` 的 allow-list 反过来写：遍历 `newRootCmd()` 命令树，对每条**没有** `--ack-alerts` 的叶子命令要求它出现在一份显式的"只读/非破坏"白名单里。这样新增命令默认失败，必须显式归类。

**量化**：净减 ~25 行（6 处三行调用 → 构造器内一处），但真正的收益是把 6 处人工记忆换成 1 处编译/测试期强制。
**风险 low**：给 `node upgrade` / `exec` / `proxy on` 补 gate 会**新增拒绝路径**（quorum 坏时这些命令开始要 `--ack-alerts`），属于行为变更，需要 `docs/usage.md` 同步。

---

### F5 — 5 个文件绕过就在旁边的默认路径常量，直接写字面量 · **medium**

**证据**

已有常量：`cmd/tether/cluster_backup.go:23` `defaultClusterSecretsDir = "/etc/tether/secrets"`、`cluster_offline.go:25-26` `defaultDataDir = "/var/lib/tether"` / `defaultDBPath = "/var/lib/tether/tether.db"`、`cluster_offline.go:36` `defaultNatsConfPath`、`cluster_offline.go:39` `defaultBrokerConfigPath`。

绕过它们的字面量：
- `cmd/tether/cluster.go:890` `"--secrets-dir", "/etc/tether/secrets"`（`cluster init`）
- `cmd/tether/cluster_add.go:126` `"/etc/tether/secrets"`、`:127` `"/var/lib/tether"`、`:128` `"/var/lib/tether/tether.db"`
- `cmd/tether/cluster_natsconf.go:53` 和 `:538` `"/etc/tether/secrets"`
- `cmd/tether/cluster_offline_wizard.go:65` `sec = "/etc/tether/secrets"`

**为什么是债**：项目已经做过一次这种搬迁——G1 #22 把 reconciler 管的 nats.conf 从 `/etc/tether/nats.conf` 挪到 `/etc/tether/nats.d/nats.conf`（`cluster_offline.go:32-36` 有完整记录，且 `serve.go:167-171` 记录了旧主机必须先迁移否则二进制升级会静默重指）。secrets dir 是同一类东西。下一次搬迁时，改常量会漏掉这 6 个字面量，而 `make lint` 的默认 linter 集（无 `.golangci.yml`，只有 errcheck/govet/staticcheck/unused/ineffassign）**一个都抓不到**，`make test` 也不会失败——只有命令树 golden 会因为 `(default ...)` 文本变化而报警，前提是常量和字面量同时改了才不报警，正好把漏改伪装成"一致"。

**建议**：6 处字面量全部换成常量引用。顺手把 `defaultClusterSecretsDir` 从 `cluster_backup.go` 挪到 `cluster_offline.go` 的常量块（那里已经是这组路径的 SSOT）。

**量化**：0 行净变化，纯替换。
**风险 low**。这条正是用户记忆里 `feedback-contract-change-sweep`（"改一个契约必须全局扫所有调用点"）指向的那类复发缺陷。

---

### F6 — 2,987 行编排 / 协议 / 身份校验逻辑锁死在 `package main`，不可被任何非 CLI 调用方复用 · **medium**

**证据**

17 个"零 cobra 命令"文件共 3,673 行（表见上文「范围与方法」），扣掉正当的 CLI 职责（`error_hints.go` 243 + `jsonout.go` 159 + `exitcode.go` 104 + `cluster_status_card.go` 149 + `logging.go` 31 = 686），剩 **2,987 行**分四类：

| 类别 | 行 | 文件 |
|---|---:|---|
| 编排状态机 | 1,339 | `cluster_add_drive.go` 887、`cluster_upgrade_drive.go` 452 |
| over-NATS 协议客户端 + 折叠 | 846 | `cluster_status_nats.go` 293、`d8_alerts.go` 197、`cluster_rotation.go` 122、`node_versions.go` 120、`cluster_doctor_online.go` 114 |
| 轮询 / 租约引擎 | 338 | `cluster_wait.go` 183、`cluster_lock_keeper.go` 155 |
| 身份 / 密钥校验 | 387 | `cluster_secrets.go` 217、`ctl_connect.go` 170 |
| 交互向导 | 77 | `cluster_offline_wizard.go` |

**最尖锐的一例**：`cmd/tether/cluster_secrets.go` 的 issuer/broker-nkey skew 检测（`readClusterPublicIdentities:55`、`clusterAuthIssuerSkewChecks:141`、`clusterAuthIssuerSkewError:180`）是**全仓唯一**一处"磁盘上的 account.nk 派生出的 public key 和 live nats.conf 里渲染的 issuer 是否一致"的校验。与此同时，daemon 自己的 `cluster status` 的 `ACCT.NK` 列是**硬编码 Y** —— `cmd/tether/cluster.go:475-476` 的图例原文："ACCT.NK=Y if this node's account key matches (currently always Y — per-node verification not yet wired)"。也就是说：**校验逻辑写好了、测试了、但放在了只有运维手动敲 `cluster reconcile nats --wait` 或 `cluster doctor` 才会触发的地方，broker 自己的 reconcile 路径够不到它。** 这不是"CLI 里多写了几行"，是一条安全相关的一致性检查被放错了层，导致它无法自动运行。

**为什么是债**（其余部分）：`package main` 不可被 import。这意味着 ① `internal/broker` 侧未来要做的"leader 自动 grow / 自动 rebalance"控制器无法复用 `driveAdd` 的 P0–P9 阶段判定与幂等恢复语义，只能重写一遍；② `test/simcluster` 的 drill 只能通过跑二进制来验证编排，无法写针对状态机的 Go 表驱动测试；③ 这 2,987 行的所有测试都必须在 `package main` 内，这正是 `cmd/tether` 有 85 个测试文件 / 13,075 行的直接原因之一。

**建议**：分两步、按收益排序，**不必一次搬完**。
- 第一优先：`cluster_secrets.go`（217 行）→ `internal/clusteroffline` 或新 `internal/clusterident`，让 broker 的 `StatusReport` 能真正填 `AccountNkMatch`。这条有明确的产品收益，不只是"更干净"。
- 第二优先：`cluster_wait.go` 的轮询 primitive（配合 F1）→ 一个小的 `internal/clipoll`；`node_versions.go`（已经是纯函数，见其文件头 "Pure —"）→ `internal/clusterupgrade`。
- **不建议搬** `cluster_add_drive.go` / `cluster_upgrade_drive.go`：它们大量依赖 `cmd.OutOrStdout()` 做 halt-and-print 编排提示，且 887 + 452 行的搬迁风险远超收益。它们留在 main 是可辩护的——**只要 F3 的 exec-self 桥先拆掉**。

**量化**：不减行（这是**搬迁**不是删除）。第一优先项约 217 行跨包移动。
**风险 medium**（第一优先项 low，`cluster_add_drive.go` 搬迁 high — 因此不建议）。

---

### F7 — 两套独立的 nkey CONNECT 实现，且已经分叉出两个功能差异 · **medium**

**证据**

- `internal/cli/natsconn.go:44-71` `ConnectNATSWithNkey`：`nkeys.FromSeed` + `sigCB` + `proxydial.Options` + `MaxReconnects(-1)`，并且在 `:49` 检查 `DevNoAuthEnv`（`TETHER_DEV_NO_AUTH=1` 时省掉 `nats.Nkey()`，匿名连接）。
- `internal/cli/completion_transport.go:118-152`：**第二份** `nkeys.FromSeed` + `sigCB` + `proxydial.Options`，选项不同（`Timeout(750ms)` / `RetryOnFailedConnect(false)` / `MaxReconnects(0)` / `NoEcho()`），**完全不检查 `DevNoAuthEnv`**。

已经产生的两个可观察分叉：
1. `TETHER_DEV_NO_AUTH=1` 下（`internal/cli/natsconn.go:18-30` 文档化的本地 demo 模式），每条命令都能连上 vanilla nats-server，但 `tether __complete` 仍走 nkey CONNECT → 认证失败 → `<TAB>` 静默返回零候选（按 `completion.go` 的契约，任何失败都返回 `(nil, ShellCompDirectiveNoFileComp)`，所以用户看不到任何错误提示）。
2. broker 自动故障转移只在 `cmd/tether/ctl_connect.go:49` 一处应用（`cli.DialFor(...)`）。`completion_transport.go` 的 `NewCompletionContext`（`:60-71`）只调 `ResolveNATSURLFromHome`，**不调 `DialFor`**。后果：被 pin 的 broker 挂掉时，`tether ps` 正常故障转移到存活 broker，但 `tether exec <TAB>` 补全节点名返回空——用户会以为节点没了。

顺带第三份拷贝：`internal/clusterroster/invite.go:21-23` 把 `"TETHER_DEV_NO_AUTH"` 字面量重新声明为包内私有常量 `devNoAuthEnv`，注释还写着 "reuses the existing dev escape hatch (cli.DevNoAuthEnv)" —— 语义上"复用"，代码上是第三个字面量。

**为什么是债**：连接语义（身份、代理、故障转移、dev 逃生舱）是这个 CLI 的**安全与可用性核心**，现在有 2.5 份实现。任何一次连接层变更（换签名方式、加 TLS pin、改代理探测）必须记得同时改补全路径，而补全路径的失败**在设计上是静默的**——它是最不可能被发现分叉的那条路。

**建议**：把签名/代理那半抽成 `cli.nkeyConnectOptions(id) ([]nats.Option, error)`（内含 `DevNoAuthEnv` 分支），两处各自追加自己的超时/重连策略。补全上下文改用 `DialFor`（补全走的是同一个 home，pin 与缓存都现成）。`invite.go` 改引用 `cli.DevNoAuthEnv`（若有 import cycle，则把常量下沉到一个无依赖的小包）。

**量化**：净减 ~30 行，并同时修掉两个已存在的功能分叉。
**风险 low**。

---

### F8 — 3,207 行 RunE 闭包体里混着无法单独调用的业务逻辑 · **low**

**证据**：`cmd/tether/cluster_join.go:46-119` —— `cluster join prepare` 的 RunE 闭包里内联了 ~70 行安全相关逻辑：读 node-ident seed 并 `TrimSpace`、派生 pub key、校验 node id、自铸 16 字节 nonce、`auth.SignWithSeed(seed, cluster.JoinSignBytes(...))` 签 PoP、从 `<secrets>/broker.nk` 派生 bus nkey（fail-closed）、从 tunnel cert 派生 cert_fp（fail-closed）、`cluster.EncodeJoinBundle`。整段没有任何可提取的命名函数，**只能通过构造 cobra 命令并执行来触达**。

其余长构造函数：`newServeCmd`(297, `serve.go:29`)、`newRunCmd`(271, `run.go:40`)、`newSessionCmd`(198)、`newClusterInitCmd`(177, `cluster.go:729`)、`newExecCmd`(161)。`cmd/tether` 共 353 个函数、22 个 >100 行。

**为什么是债**：单独看"函数 120 行"不是问题（我在结论里已经排除这类风格洁癖）。`newServeCmd` 的 297 行是 23 个 flag 变量 + 20 次 `pickFlagOrYaml` + 一个 26 字段的 `broker.Config` 字面量，逐行读下来层次是齐的、可读的——**不算债**。真正的债只有 `cluster join prepare` 这一处：把可 fail-closed 的密钥派生埋在闭包里，等于放弃了对它做表驱动测试的能力，而它的每个 fail-closed 分支（`:98` bus nkey 空、`:103` cert_fp 空）在注释里都写明了"一旦为空会让 grown leader 永久 crash-loop"。这种代价的分支应该有直接的单元测试。

**顺带一个真实的演进摩擦（非本 lane 主线，记一笔）**：`newServeCmd` 加一个 broker 配置项要协调改 6 处 —— `serveconf` 结构体字段 + yaml tag、`broker.Config` 字段、`serve.go` 的局部变量、`pickFlagOrYaml` 行、`broker.Config` 字面量的赋值行、`Flags().StringVar` 注册。漏掉中间任何一处都是**静默失效**（配置写了不生效），没有编译错误。当前 38 个 yaml key、27 个从 `serve.go` 读到，未发现实际漂移，但机制上无保护。

**建议**：把 `cluster join prepare` 的 RunE 体提成 `buildJoinBundle(seedPath, nodeID, ..., secretsDir string) (string, error)`（这同时是 F3 的前置条件）。`newServeCmd` 不建议动。

**量化**：净 0 行（提取，不删除）。
**风险 low**。

---

### F9 — `callAdmin → leaderRedirect → resp.Error → clusterAdminError` 四连重复 17 次 · **low**

**证据**：完全相同的 8 行形状出现在 `cluster.go:525-549/589-598/619-628/654-663/693-704/707-718`、`cluster_ops.go:38-47/63-72`、`cluster_seeds.go:42-51`、`cluster_join.go:144-153`、`cluster_backup.go:313-322`、`cluster_rebalance.go:46-55`、`alert.go:54-65/99-...`、`cluster_retire.go`、`cluster_reconcile.go`。共 36 处 `callAdmin` 调用、17 处 `leaderRedirect`、23 处 `clusterAdminError`。

**为什么（只）是 low**：我核对过 19 处"缺 `leaderRedirect`"的调用点，全部有语义理由——只读 op（`OpClusterStatus`、`OpClusterOps`、`OpClusterHomes`、`admin.go` 的 6 处）或显式支持 follower（`cluster backup --allow-follower`，`cluster_backup.go:56`）。**这不是遗漏，是有意的**。所以它是纯样板，不是正确性风险。

**建议**：`callAdminChecked(cmd, socket, verb, req) (*adminsock.Response, error)` 封装"检查 + leader 重定向 + 错误包装"，只读路径继续用裸 `callAdmin`。

**量化**：17 × 8 行 → 17 × 4 行，净减 **~68 行**。
**风险 low**。

---

## 反证：做得好的地方

以下几处我认为是**明显高于同类 Go CLI 平均水平**的，改动时不要碰坏：

1. **`cmd/tether/exitcode.go`（104 行）——一份真正的 exit code 分类学 SSOT。** 定义了 sysexits 风格的 64/69/70/75/77，并且**显式记录了保留区间不冲突的理由**（`:19-24`：0–3 是 `cluster status` 的健康码、exec/run 透传远端码，两者都 `os.Exit` 不经主 sink）。更关键的是 `:63-64` 写死了一条纪律："The classifier never string-sniffs prose for a class — that would make a reworded message silently change a script's exit code."——`usageErr`/`unavailErr`/`permErr` 在**错误产生处**打分类，不在 sink 里猜。这是正确的做法，且被写下来了。

2. **`cmd/tether/error_hints.go`（243 行）——40+ 个 broker code 到"一句运维能照做的话"+ 一个 exit class 的单一映射表。** `brokerCodeHints` 和 `brokerCodeExitClasses` 两张表并置，每条 exit class 的归类理由都写在注释里（例 `:94-98`：`jetstream_not_ready` 是瞬态的一半 → 75，`bucket_create_failed` 是永久的一半 → 故意留 70）。`:135-138` 还诚实地记录了 `dataplane_not_converged` 这个字面量与 `internal/broker` 之间"没有编译期链接，只有一个 wire 稳定性测试挡着"。**这是 CLI 该有的样子。**

3. **`cmd/tether/jsonout.go`（159 行）——机器输出契约的成文治理。** 每个 schema 带 `(schema, schema_version)` 双判别符；`:22-25` 写了 bump 政策（只在破坏性变更时 bump，加 omitempty 字段不 bump）以及一条硬规则："Branch-load-bearing fields ... are never omitempty — their stable presence IS the schema"；`normSlice` 保证空数组序列化成 `[]` 而非 `null`，注释说明了 `jq '.nodes | length'` 的具体后果。

4. **`cmd/tether/command_tree_inventory_test.go` + `testdata/command_tree_golden{,_runtime}.txt` —— 整棵 CLI 表面对 golden 断言。** 94 条构造树路径 + 99 条 runtime 树路径（含 cobra 注入的 `completion` 子树），每条路径的 local + persistent + inherited flag 全集，每个 flag 的 Hidden 位，全部 zero-diff 断言。新增一条命令 / 一个 flag / 翻一个 Hidden 位都会红。**在 Go CLI 项目里我很少见到这个级别的表面回归网**，它把"CLI 是产品契约"这件事变成了可执行的。

5. **`cmd/tether/ctl_connect.go` 的 `connectCtl` 收拢是正确的抽象位置。** 故障转移的展开条件（只在持久化 `broker_url` 这个来源上展开，flag / env 覆盖一律 pin 单点）、tier-2 signed manifest 的机会式刷新、以及**绝对的 PIN 门 A**（`:80-81`："a nil/empty pin ⇒ return BEFORE any consume — never TOFU-pin off a network responder's self-claimed AccountPub"）都在同一个函数里，且被 `ctl_failover_test.go` / `g3_ctl_pull_test.go` / `g3_external_review_test.go` 从三个角度钉住。安全不变量收在一处，这是对的。

6. **`confirmTypedNodeID`（`cluster_offline.go:590`）的确认分层设计。** `allowMachineEscape` 是**每个调用点的能力，而不是这个共享漏斗的属性**（`:580-589` 明确写了理由：quorum 破坏性操作即使 flag + env 都对也必须落到 TTY 拒绝，"an env var in a systemd unit or CI is not 'attended' for a quorum-DESTRUCTIVE op"）。逃逸需要 flag **和** 环境变量同时等于 node id，单独一个都不够。这是想清楚了的安全设计，不是 `--yes` 一把梭。

7. **样板率本身就是反证。** 92 条命令、6.4% 样板。`newClusterCmd`（`cluster.go:44-49`）按"在哪跑 + 多危险"把子命令分成 online / client / migrate / escape 四组，`escape` 组标题直接写 "DANGER -- recovery + raw escape hatches (runbook section 3)"。这是深思熟虑的运维 UX，不是行数膨胀。

8. **`cluster status --settle` 的诚实语义**（`cluster.go:126-132` + `cluster_wait.go:130-146`）：明确承认 voter 重启产生的 DEGRADED **是真的**，默认不去掩盖它，`--settle` 是显式 opt-in 的去抖，且 quorum-lost / force-single 永不参与去抖。愿意在文档里写"这个瞬态是真的，不是 bug"，比悄悄加个重试要好得多。

---

## 本质 vs 偶然复杂度拆解

**问题域强加了什么**：`tether` 的 CLI 要同时服务三个角色（ctl / agent / broker 运维）、两条传输（NATS 控制面 + broker 本地 Unix admin socket）、两种模式（daemon 在跑的 online 与 daemon 停机的 offline 磁盘直操）、以及一整套 Raft 集群生命周期动词（init / join prepare+approve / drain / retire / remove / transfer-leader / rebalance / reconcile / force-single / restore / resnapshot / rotate-cert / set-raft-addr / upgrade / unlock / backup / seeds / pin / invite / ops×4 / doctor / incident export）。这天然就是 90+ 条命令路径。再加上必须给运维提供**机器可判定**的 exit code 与 JSON schema（这是生产工具的硬要求，不是镀金），14.6k 行是**站得住的**。

**我的估计：本质 72%（≈ 10,500 行），偶然 28%（≈ 4,100 行）。**

偶然那部分的构成，按"能不能真的消掉"分开算：

| 类别 | 行数 | 能否净减 |
|---|---:|---|
| F2 ctl 前导码 + flag 注册重复 | ~180 | ✅ 可净减 |
| F1 轮询循环重复骨架 | ~120 | ✅ 可净减 |
| F9 admin 四连样板 | ~68 | ✅ 可净减 |
| F3 exec-self 桥 | ~25 | ✅ 可净减 |
| F7 第二份 nkey connect | ~30 | ✅ 可净减 |
| **小计：机械可删** | **~420** | **= 全 lane 的 2.9%** |
| F6 放错层但必要的逻辑 | ~2,987 | ❌ 只能搬迁，不减行 |
| F8 埋在闭包里的逻辑 | ~400 | ❌ 只能提取，不减行 |
| F5 常量绕过 | 0 | ❌ 纯替换 |

**这个拆解本身就是本次审计最重要的结论：`cmd/tether` 真正"多写的"只有约 420 行（2.9%）。剩下 3,400 行的"偶然复杂度"不是多写的，是写在了错的地方。** 用户如果期待"重构一下砍掉几千行"，本 lane 给不出这个答案——能给的是"把 3,400 行挪到正确的层，让 broker 和测试能用上它，并把 8 个分叉的等待循环收成 1 个"。

**关于注释 16.4% 的判定**：我把 2,398 行注释全部计入**本质**。逐条读下来，它们不是"这个函数做什么"的复读，而是"这个 gate 为什么存在 / 哪一轮 review 或哪个 deploy-tier drill 抓到的 / 不这么写会出什么事"。例如 `cluster.go:911-917` 记录 seam 必须插在 `broker:` 行之后而非 EOF append，否则 broker 会以 SINGLE 模式启动而 `cluster add` 会误判为已集群化并卡死；`serve.go:167-171` 记录 G1 #22 把 nats.conf 挪到 `/etc/tether/nats.d/` 后，pre-G1 主机必须先迁移否则二进制升级会静默重指。**在一个 wire 破坏 = 现网必须重装的生产工具里，这些注释是比测试更耐久的知识载体。** 删掉它们才是真正的债。

**一个 lane 边界外但值得主进程知道的观察**：`cmd/tether` 有 85 个测试文件 / 13,075 行，其中 **35 个（3,744 行）按审查轮次命名**（`g4_external_rereview_test.go`、`r16_g67_g69_external_review_test.go`、`codex_allgreen_external_review_test.go`、`b6_skew_exit_test.go`、`p9_review_risk_test.go` …）。最小的 `d9_external_review_test.go` 只有 402 字节。这直接印证了任务描述里的假设——「3 阶段 7 步流程本身是某些结构债的成因」：每一轮外审新增的断言按**发现它的那一轮**归档，而不是按**它保护的那个不变量**归档。后果很具体：想知道"grow lock 的释放语义有哪些断言"，得在 `cluster_growlock_release_test.go`、`r16_*`、`g4_*`、`cluster_seam_test.go` 里翻。这是测试组织问题（属其他 lane），但成因在流程，值得一并处理。
