# S1 · reliable-exec（可靠 exec 产品路径与控制面 carrier 可换）实现 plan（主进程定稿）

日期：2026-09-21。上位文档：[stage 分治计划](hot-update-stages-plan.md) S1 一节。entry：S0 出口，index tree `024e36e153de58931c65a61b8dd19445e177f730`。
由 4 路草拟（ctl / broker / agent / deploy）→ 4 路交叉批评 → 1 路综合的 workflow 产物定稿；原始产出归档于仓库外 `~/tether-hot-update-handoff-20260920T2356Z/s1-plan/`。

**主进程定稿裁决**（综合稿 §10 五问 + 三处对 stages-plan 原文的改读，全部采纳）：

| # | 裁决 |
|---|---|
| Q1 | **新增 `tether exec --stdin`**（opt-in；本地读到 EOF，上限 512 KiB，超限 64 在 connect 前拒绝；legacy 与可靠两路同源 `ExecReq.Stdin`）。CLI golden 变更，理由：今天没有任何 CLI 入口能填 `ExecReq.Stdin`，exit #1「48KiB stdin」在真二进制上不可达；opt-in 避免 `while read … tether exec` 脚本被隐式吃掉 stdin。 |
| Q2 | exit #3「撤 member 中途 → agent task SIGKILL 并有 terminal」按 **(a)+(b)** 改读：(a) member 撤销 = broker 每 op 重查 → ctl 得 `not_a_member`（77，不降级，无第二 child），agent 上的任务跑到自己退出、终态由 abandoned-terminal 有界保留回收；(b) SIGKILL + terminal + tombstone 由 `session_destroyed`（C-10）承担。**不**新造 broker→agent 撤权通道（产品无 member 移除动词；盖章转发反转 C.1 pre-forward 模型且机会主义）。 |
| Q3 | `session_destroyed` 的 agent 处置：`Revoke`（reliable）+ `reapManagedChildren`（legacy exec + PTY），**不 `cancelRun`**、daemon 不退出——退出是车队可见语义变化（systemd cgroup 随 daemon 退出杀光 unit 内全部负载，weilandserver 的实验负载就在里面）。daemon 退出留给既有 register 拒绝路径。 |
| Q4 | 退出码 taxonomy：新增 **`exitUnsettled = 74`**（呈现层，不进 `brokerCodeExitClasses`）；create 送达后除权限类 77 外一律 74（含 `session_not_found_or_deleting`、`node_offline: status=OFFLINE`、Ctrl-C）；`ExitError.Code` 保留 wire 码。**先改 usage §9.13 再改码**：重试规则改为「69/70/75 可重试，64/74/77 终态」。 |
| Q5 | drill 35 臂 C：**GREEN + 保留 C2**（判词 = 走了 legacy + 无重复执行 + 不连续如实记录）；「N-1 不承诺连续」是 requirements 口径不是覆盖缺口。 |
| 改读 1 | C-5「starting 记录进联合快照」改为 additive `LocalProcess.StartedUnacked bool`（omitempty）+ broker reconcile 两处 orphan 循环对该标志跳过。「starting」作为新 State 在新旧 broker 下都更糟（已知行 default 臂记 EXITED(-1) / 未知行 orphan 杀）。 |
| 改读 2 | 「短写」在真二进制形态 = 关闭 stdout 读端（SIGPIPE → rc 141，远端真相不变）；`io.ErrShortWrite` 的 ACK 契约由 execclient 单测守。evidence 如实写。 |
| 改读 3 | broker 对每次被转发的 create 都写 audit.call，回复丢失重发时**天然多条**；「无重复执行」的 oracle 一律取 agent 侧证据（marker 文件 / `ps` 行 / `agent: exec … reliable=true` 日志各恰 1），audit 只记录。 |

**真实 drill 成本预告**（stages-plan §-1：实跑前报成本）：`./local.sh --build build` restage ≈3–5 min（首次含 v0.6.0 worktree 构建）；`./run-drills.sh 35-hot-update-exec-carrier` 三臂并行 ≈7 min；`./local.sh drill 34-hot-update-process` ≈2 min；合计 ≈12–15 min，期间不跑 go test/lint/gates。

---

> 综合自 ctl / broker / agent / deploy 四份草案与四份交叉批评。所有行号按当前 index 树（S0 出口 `024e36e1…`）核对：
> `internal/execclient/client.go` 339 行、`cmd/tether/exec.go` 199 行、`cmd/tether/error_hints.go` 489 行、`cmd/tether/exitcode.go` 136 行、
> `cmd/tether/ctl_connect.go` 186 行、`internal/cli/natsconn.go` 132 行、`internal/broker/exec.go` 637 行、`internal/broker/admit.go` 382 行、
> `internal/broker/reconcile.go`、`internal/agent/exec.go` 657 行、`internal/agent/run.go`、`internal/agent/agent.go` 2582 行、
> `internal/agent/proc_delivery.go`、`internal/execsession/runtime.go` 530 行、`internal/procrequest/registry.go` 412 行、
> `internal/proto/process_resume.go` 92 行、`internal/proto/messages.go:292-306`。
> 综合者另行复核过的两条关键事实（批评者实跑 `/tmp/s1critic/{authgap,nonce}`，本稿重读了源码与 nats.go v1.52.0 / nats-server v2.14.6）：
> ① nats.go `DefaultTimeout = 2s` == nats-server `AUTH_TIMEOUT = 2s`，callout responder 缺席时 ctl 的 CONNECT 失败是 **`i/o timeout`**（`net.Error`），不是 `Authorization Violation`；
> ② 匿名嵌入 nats-server 对带 nkey 的 CONNECT 返回 `nkeys not supported`，而 `TETHER_DEV_NO_AUTH=1` 让 ctl 走 `nats.Connect` 无私有 inbox → `execclient.Run` L108-111 直接 `ErrUnsupported`。
> 两条都是本 plan 的结构性前提，不是可选项。

---

## §0 目标与出口

**goal**（照抄 stages-plan S1）：把 procrequest/execsession/execclient/process-resume 原语接成真实 `tether` 二进制 ctl→broker→agent→child 的产品路径；证明控制面 carrier（agent NATS session 重建、broker 重启、nats-server 重启、ctl 断线）可替换而任务不丢、不重、退出码准确。不含任何跨 exec 状态保留。

**entry**：S0 出口；index tree `024e36e153de58931c65a61b8dd19445e177f730`。

| # | 出口（原文） | 机械判定命令（rc=0 即过；均**不用管道取 rc**） |
|---|---|---|
| 1 | `test/cli_e2e/exec_reliable_test.go`（真实构建二进制 + 嵌入 NATS + broker + 真 agent 进程）：48KiB stdin、双流、rc 23、signal→128+sig、短写、丢 create/terminal 回复、agent NATS 会话中断续接、broker 重启续接、legacy fallback、`--timeout` 本地取消不远端 kill 全绿 | `go test -race -count=1 -timeout 15m -run '^TestExecReliable' ./test/cli_e2e/`；每条子测试自带**非空性断言**（agent 日志 `agent: exec … reliable=true` 恰 1 行，或观察订阅计数 `process-resume` create ≥1 且 `exec.req.forwarded` ==0）。「短写」的真二进制形态 = 关闭 stdout 管道读端（SIGPIPE→rc 141，远端真相不变）；`io.ErrShortWrite` 的 ACK 契约由 `internal/execclient` 单测 `TestRunAcknowledgesOnlyBytesWritten`（内审 D9 订正：plan 原名 `TestRunShortWriteAcksDeliveredBytesAndLeavesTaskRetained`，树中以此名交付） 守（§4 表注明，evidence 如实写） |
| 2 | 嵌入 nats-server 上「callout responder 缺席 1.5s 再回来」确定性测试：ctl 连接级重试在预算内成功；负控制：从未认证过的身份不重试 | `go test -race -count=1 -run '^TestCtlConnect' ./test/cli_e2e/`（真 callout + 真 ctl 二进制）+ `go test -count=1 -run '^TestConnectWithAuthRetry' ./cmd/tether/`（纯策略单测，无 NATS） |
| 3 | broker：CallID 三臂绿；撤 member 中途 → ctl 得权限错误（非 unsupported）、agent task SIGKILL 并有 terminal；未知字段穿透测试绿；若改 reply 规则则 fuzz 复验 | `go test -race -count=3 -run '^(TestProcessResume\|TestPrivateProcessReply\|TestConvertedVerbSpecsMatchTheirHandlers\|TestReconcileDefersOrphanKill)' ./internal/broker/` + `go test -race -count=1 -run '^TestExecReliableRevokedMember' ./test/cli_e2e/`（77、不降级、无第二 child）+ `go test -race -count=3 -run '^TestSessionDestroyedRevokes' ./internal/agent/`（SIGKILL + terminal 的 agent 半，见 §2.6 / §10-Q2 的解读）；`privateProcessReply` 规则不改，仍跑 `make fuzz FUZZ='^FuzzPrivateProcessReply$' FUZZTIME=30s` 留收据 |
| 4 | agent：spawn 成功/started 未持久化窗口下 G.1 快照不误杀（真实 child，race×3）；session_destroyed 后真实 exec 子树被杀且 tombstone 拒绝复活；ps 显示 reliable task RUNNING/EXITED | `go test -race -count=3 -run '^(TestRegisterRequestCarriesStartedUnackedWhileStartedIsPending\|TestSessionDestroyedRevokes\|TestExecStartedEnqueuedBeforeAdopt\|TestReliableTaskSurvivesSessionRebuild)$' ./internal/agent/` + `go test -race -count=1 -run '^TestExecReliablePsShows' ./test/cli_e2e/`（内审 D9/协议核验 V1 订正：原正则点名的 `TestRegisterSnapshotMarksStartedUnacked…` 交付为 `TestRegisterRequestCarriesStartedUnackedWhileStartedIsPending`（agent 半，真 register.req 线上带 flag）+ `TestReconcileNeverAcceptsStartedUnackedPidWithoutARow`（broker 半）；`TestReliableTaskSurvivesSessionRebuild` 内审前缺席、内审后交付；正则加 `$` 锚定，名字缺失即空匹配→红） |
| 5 | drill 35 臂 A / 臂 B GREEN：原 child PID/nonce 不变、ctl 收齐全部字节与真实退出码、无重复执行；新请求时延只作校准记录；臂 C：v0.6.0 ctl 对新 agent 走 legacy exec，判「不承诺连续」 | `cd test/simcluster && ./run-drills.sh 35-hot-update-exec-carrier`（三臂并行）；verdict 行 `35….A GREEN`、`35….B GREEN`、`35….C GREEN`，evidence 记 `carrier.json`/`legacy-outcome.json` |
| 6 | drill 34 重跑 PRODUCT-RED（新签名），expected-verdicts 注记更新、旧收据保留 | `cd test/simcluster && ./local.sh drill 34-hot-update-process` → verdict `PRODUCT-RED`，签名 `H0_LEGACY_EXEC_INTERRUPTED output=epipe management=exited_minus_one ctl=owner_lost_rc74 …`；`sh tests/run-all.sh` 绿（probe selftest 含旧形状负例） |
| 7 | 三硬闸 + `make gates` 绿；Darwin 绿；inventory/wire/matrix 只增；CLI golden 若变更写理由；Agent 方法 ≤126、broker 文件 =70 | `make test; echo $?` / `make e2e-parallel; echo $?`（`ALL PASS`）/ `make lint; echo $?` / `make gates; echo $?` / `GOOS=darwin GOARCH=arm64 go build ./... && make vet-tags`；`git diff --numstat -- internal/proto/testdata/wire_field_inventory.txt test/determinism/testdata/test_function_inventory.txt test/e2e/parallel/testdata/matrix_units_golden.txt` 每行删除数 =0；`go test ./test/architecture -run TestStructuralBudget` |

---

## §1 ctl 端

### 1.1 退出码 taxonomy —— **先改 `docs/usage.md §9.13`（L1771-1843），再改代码**

**新增类 `exitUnsettled = 74`（EX_IOERR）**：「远端命令**已被接纳（create 已送达）**，但结果不在手上——输出被截断、本地放弃等待、拥有它的 agent 实例已换代、或 agent 离线；命令可能已带副作用跑完，**绝不盲目重试**」。

- 名称取 `exitUnsettled`（四份草案各自命名 `exitIOErr`/`exitExecuted`/`exitUnsettled`，取语义最贴切的一个；数值一致为 74）。
- 74 全仓未用，与 `0..3`/`64`/`69`/`70`/`75`/`77`/`91`/`128+sig` 无冲突；与 exec 透传的远端 74 的消歧 = §9.13 既有规则（分类码一定带 `exec: …` stderr 行）。
- **74 只在呈现层**：不进 `brokerCodeExitClasses` 值域（`TestBrokerCodeExitClassesNeverReserved` 的 `allowed` 集不改），由新测试 `TestExitUnsettledIsPresentationOnly` 钉住 `74 ∉ values(brokerCodeExitClasses)` 且 `74 ∉ {69,70,75}`。
- usage §9.13 改法：表加一行 `74`；「健壮重试规则」改为 **「把 `69`/`70`/`75` 当可重试（退避），仅 `64`/`74`/`77` 当终态」**；新增 §9.13.2「exec 可靠路径的退出码」：create 送达后除权限类（77）外一律 74、`ctl 中断（--timeout/Ctrl-C）= 放弃`（任务在 agent 上继续，**没有 ctl 侧 kill 动词**，它跑到自己退出或输出累积 8 MiB 后卡住等待 ACK，见 §9 风险）、legacy fallback 的 2s probe 税、被撤权的 ctl 在命令中途重连到 `--timeout` 才退（`IgnoreAuthErrorAbort` 的代价）。

**分类规则（`cmd/tether/exec.go` 可靠分支，表驱动，见 §4 `TestReliableExecFailureClassTable`）**：

| 条件 | 退出类 | 依据 |
|---|---|---|
| `errors.Is(err, ErrUnsupported)` | 走 legacy 分支（不变） | 只在 probe 前产生（client.go L108-111 / L129） |
| `err == nil` | 透传：`Signal!=0` → `os.Exit(128+Signal)`；`ExitCode<0` → 128；否则 `ExitCode` | 不变（L86-96） |
| `*SettledError` 且 `Result.Executed` | **74**，stderr `exec: <se.Error()>; remote exit=%d signal=%d (task %s)` | 命令跑了、输出/终态不干净 |
| `*SettledError` 且 `!Result.Executed`（failStart：child 从未启动） | `execFailureMessage(Result.TerminalError)`（`remote_fs_spawn_timeout`→75、`too_many_wedged_spawns`→64、未知→70） | 从未执行，既有分类正确且可重试 |
| `*ProtocolError`（Kind=error 回复）且 `!CreateSent` | `execFailureMessage(pe.Message)`，class 按 `pe.Code`（空则按冒号前缀） | 既有行为 + Code |
| `*ProtocolError` 且 `CreateSent` 且 class==77 | **77**（`not_a_member` 等） | 撤权不降级、不伪装 74 |
| `*ProtocolError` 且 `CreateSent` 且 class!=77 | **74**，`ExitError.Code = pe.Code`，stderr 附 handle | `process_instance_lost`/`process_expired`/`node_offline: status=OFFLINE`/`session_not_found_or_deleting`/`forward_failed`… 结果未知 |
| ctx `DeadlineExceeded`/`Canceled` 且 `CreateSent` | **74**，文案 `exec: gave up waiting after --timeout %s; the remote task keeps running on %s (task %s) and was NOT killed — a rerun re-executes it` | OQ4 边界 |
| ctx 错误且 `!CreateSent` | **75**：`&ExitError{Class: exitTransient, Err: fmt.Errorf("exec: %w", err)}` | 今天实际退 70（`execFailureMessage` 把任何串包成显式 Class；批评者三方核实），改为显式 75 |
| 其它传输错误（`nats.ErrConnectionClosed` 等）且 `CreateSent` | **74** | 结果未知 |
| 其它错误且 `!CreateSent` | `execFailureMessage(err.Error())`（不变） | — |

**「create 已送达」判据 = `Result.CreateSent`**（ctl 批评 BLOCKER 1）：`result.Handle` 在发 create 前就赋值（L140），`result.PID` 只在首个 status 回复置位（L268）；create 回复丢失后超时时 PID 为空而 child 已在跑。`CreateSent` 在 `call` 内 `op=="create"` 的第一次 `PublishRequest` 返回 nil 后置 true（`PublishRequest` 返回 `ErrMaxPayload`/`ErrConnectionClosed` 时不置位，避免把从未送出的请求说成「远端可能在跑」）。

### 1.2 `internal/execclient/client.go` 逐项改法

| 项 | 改法 | 行 |
|---|---|---|
| `Result` 新字段 | `CreateSent bool`（见上）；`Executed bool`：观察到 `reply.Phase=="running"`、任一流接受过 ≥1 字节、或终态 `Signal!=0 || ExitCode>=0` 即 true；failStart 形状（-1/0/两条空 FIN/never running）保持 false | L25-39 |
| 新类型 `ProtocolError` | `type ProtocolError struct{ Code, Message string; AfterCreate bool }`；`Error()` 返回 `Message`（保持 `execFailureMessage` 的冒号切分兼容）；`receive` L252-253 改为 `return reply, &ProtocolError{Code: reply.Code, Message: reply.Error, AfterCreate: t.created}`；`transport` 加 `created bool`（与 `CreateSent` 同源） | L85-91, L252-253 |
| probe 降级判据 | L129 改为 `errors.Is(err, context.DeadlineExceeded) || errors.Is(err, nats.ErrNoResponders) || isUnsupported(err)`，其中 `isUnsupported` = `pe.Code == proto.CodeProcessUnsupported || (pe.Code == "" && pe.Message == "unsupported process protocol")`；权限/会话错误原样返回（不变） | L123-132 |
| `ErrExpired` 收敛 | L175 改为 `!(pe.Code == proto.CodeProcessExpired || (pe.Code == "" && pe.Message == procrequest.ErrExpired.Error()))`；语义不变（仅持双 end + terminal 时收敛） | L171-177 |
| reconnect 期间不重发 | `call` 循环：`PublishRequest` 前若 `t.nc.IsReconnecting()` → 等 500ms（ctx 可取消）后 `continue`，**不**再 publish（否则 25s 断网会在 reconnect buffer 里堆 ~50 份同 CallID 请求，broker 转发 50 次、audit 50 条）；reconnect 完成后按既有「attempt 500ms 超时 → 重发」路径补发 | L209-233 |
| `AttachEpoch` | **一次 `Run` 内恒为 1**、同 conn 同 inbox（nats.go 重连自动重订阅；`registry.attach` L370 对同 epoch 同 inbox 幂等）。不做 `WithRedial`/epoch++（`IgnoreAuthErrorAbort`+`MaxReconnects(-1)` 下 conn 不会自行 CLOSED；若仍 CLOSED → 74 结果未知）。epoch 递增留给未来 `exec --resume`（OQ4 非目标） | L141-142 |
| 其余 | `accept`/`validateReply`/`acknowledgePartial`/`SettledError` 零改动 | — |

### 1.3 `cmd/tether/exec.go` 改法（cobra 文件，不计 non-CLI 预算）

L69 连接：`connectCtlOpts(cmd, "exec", home, natsURL, id, nats.Name(cli.CtlNameForSession(sid)))`（连接级重试与 `IgnoreAuthErrorAbort` 在 §1.5 落到公共路径，exec.go 不单独加 option）。

L80-97 重写为：

```go
result, reliableErr := execclient.Run(ctx, nc, subject, spec, cmd.OutOrStdout(), cmd.ErrOrStderr())
if !errors.Is(reliableErr, execclient.ErrUnsupported) {
    if reliableErr == nil {
        _ = nc.Drain()
        exitWithRemoteStatus(result) // 128+sig / 128 / ExitCode（原 L86-96 抽出）
    }
    if _, settled := reliableErr.(*execclient.SettledError); settled { _ = nc.Drain() }
    return classifyReliableFailure(nid, timeout, result, reliableErr) // 纯函数，§1.1 表
}
// legacy 分支 L99-168 不变
```

`classifyReliableFailure(nid string, timeout time.Duration, r execclient.Result, err error) error` 放 exec.go（同文件，表驱动可测）；`ExitError.Code` 带 wire code 供脚本分支。

### 1.4 `--stdin`（**待 §10-Q1 裁定**；推荐加）

`cmd.Flags().BoolVar(&readStdin, "stdin", false, "read local stdin to EOF (max 512 KiB) and hand it to the remote command as its one-shot stdin")`；`io.ReadAll(io.LimitReader(cmd.InOrStdin(), 512<<10+1))` 超限 → `usageErr`（64）**在 connect 之前**本地拒绝；legacy 与可靠两路共用同一 `proto.ExecReq{Stdin}`。上限推导：create JSON 必须 < 1 MiB（broker `exec.go:577`、`registry.intentHash` L403、NATS max_payload）；base64(512 KiB)=699 KiB + handle/argv 信封 <1 MiB；加单测「上限 + 最大信封 marshal 后 < 1<<20」。CLI golden：`go test ./cmd/tether -run TestCommandTreeInventory -update-command-tree-golden`，commit message 写「exec 新增 --stdin：可靠 exec 的 stdin 在真二进制 e2e 里没有别的入口；隐式读 stdin 是 `while read … tether exec` 的脚本地雷」。若不采纳：48KiB stdin 子测试改由 harness 直发 `ExecReq.Stdin`，evidence 注明「非真 CLI」。

### 1.5 连接级有界重试（exit #2）

**受益前提「本身份此前认证成功」的载体 = 收据文件**，新文件 `internal/cli/auth_receipt.go`（`internal/cli` 5→6 生产文件，无预算条目）：

```go
func AuthReceiptPath(home, sid string) string        // <home>/auth_receipts/<sid>
func HasAuthReceipt(home, sid, fingerprint string) bool // 文件存在且首行 == fingerprint（换 key 不继承）
func WriteAuthReceipt(home, sid, fingerprint string) error // 0700 目录 / 0600 文件；幂等；失败不致命
func SessionFromCtlName(name string) (sid string, ok bool) // "tether-cli:<sid>" 的对偶；"tether-cli" → ok=false
```

写收据的时机：**任何一次以已激活名（`CtlNameForSession`）成功的 CONNECT**（`connectCtlOpts` 成功后）+ `login.go:84` 写 `current_session` 时——现网所有既存 home 在升级后第一条成功命令即自愈，不必重新 login。

**重试谓词（生产超时形状下必须能触发）**：`transientConnectErr(err)` = `errors.Is(err, nats.ErrNoServers) || errors.Is(err, nats.ErrTimeout) || (net.Error 且 Timeout()) || errors.Is(err, nats.ErrAuthorization) || strings.Contains(err.Error(), "Authorization Violation")`。其它（TLS、`ErrNkeysNotSupported`、其它 `-ERR`）不重试。理由：默认 2s/2s 形状下 responder 缺席看到的是 `i/o timeout`；只有 `ctl_connect.go:72` 的扩展拨号路径（`nats.Timeout(3s)`）才会看到 `-ERR`；`ErrNoServers` 覆盖 nats-server 本身正在重启（drill 35 臂 A 的新请求）。

`cmd/tether/ctl_connect.go`（cobra 文件）L74-77 改为：

```go
sid, activated := cli.SessionFromCtlName(ctlNameOf(opts)) // 把 opts 施加到 nats.GetDefaultOptions() 读 .Name（natsinbox.Connect L78-86 同法）
eligible := verb != "login" && activated && cli.HasAuthReceipt(home, sid, id.Fingerprint)
nc, err := connectWithAuthRetry(cmd.Context(), eligible, func() (*nats.Conn, error) { return cli.ConnectNATSWithNkey(dial, id, opts...) },
    func(attempt int, err error, next time.Duration) {
        fmt.Fprintf(os.Stderr, "tether: %s: broker not ready (%v); retrying in %s (identity previously authenticated for session %s)\n", verb, err, next, sid)
    })
if err != nil { return nil, connectError(verb, base, err) }
if activated { _ = cli.WriteAuthReceipt(home, sid, id.Fingerprint) }
```

`connectWithAuthRetry(ctx, eligible, attempt, report)`：常量 `ctlAuthRetryBudget = 5*time.Second`（新尝试只在预算内**开始**；最坏墙钟 = 预算 + 单次 Timeout 2s，plan 与 usage 明写）、退避 250ms→500ms→1s 封顶；`!eligible || !transientConnectErr(err) || now+backoff ≥ deadline` → 立即返回原错误；`ctx.Done()`（Ctrl-C）打断等待。**负控制机械保证**：`!eligible` 短路，fresh home 单次尝试。

**`nats.IgnoreAuthErrorAbort()`** 加进 `internal/cli/natsconn.go:49` 的 `all`（与 `agent.go:2255-2262` 同一理由：nats-server 重启后 ctl 重连撞 callout 黑窗，两次同错即永久 CLOSED；exec 续接、`history --follow` 都靠它）。副作用写进 usage §5：被撤权的 ctl 在 `exec`/`run` 中途会重连到 `--timeout`/Ctrl-C，而不是两次 auth 错误后断开；撤权由 broker 每 op `admitACL` 以 77 拒绝收口。把 option 列表抽成 `ctlConnOptions(id)` 以便 `TestCtlConnOptionsIgnoreAuthErrorAbort` 施加到 `nats.GetDefaultOptions()` 断言。

### 1.6 结构预算落点（ctl 侧）

| 文件 | 增量 | 预算 |
|---|---|---|
| `cmd/tether/exitcode.go` | `exitUnsettled = 74` 1 行 + 头注释一段（注释不计） | non-CLI +1 |
| `cmd/tether/error_hints.go` | `brokerCodeExitClasses` +2 行（`"process_capacity": exitTransient`, `"process_runtime_closed": exitTransient`）+ `brokerCodeHints` +2 行 | non-CLI +4 → 实测 1267 → ≈1272 < 1300 |
| `cmd/tether/exec.go`、`ctl_connect.go` | `classifyReliableFailure`/`exitWithRemoteStatus`/`connectWithAuthRetry`/`ctlNameOf` ≈ +110 行 | import cobra，不计 non-CLI；`pkg-code-lines cmd/tether` 12000 边界 14000 |
| `internal/cli/auth_receipt.go` | 新文件 ≈60 行 | 无条目 |
| `internal/execclient/client.go` | ≈ +45 行 | 无条目 |

每次动 `error_hints.go`/`exitcode.go` 后立跑 `go test ./test/architecture -run TestStructuralBudget`。

---

## §2 broker 端

### 2.1 wire（全部 additive/omitempty、零值合法；wire inventory **+2**，ProtoVersion 不动）

`internal/proto/process_resume.go`：

```go
// ProcessResumeReply.Code（放在 Error 之前）：Kind=="error" 时的稳定机器码；空 = 尚未携带此字段的对端
// （预发布 dev 构建），客户端按 Error 文本前缀兜底。分类器只看 Code，绝不嗅 prose。
Code string `json:"code,omitempty"`

const (
    CodeProcessBadRequest    = "bad_request"                    // broker：JSON 非对象 / exec 非对象 / call_id 非无符号整数
    CodeProcessTooLarge      = "process_request_too_large"      // broker：>1 MiB
    CodeProcessUnsupported   = "unsupported_process_protocol"   // broker version 不符 / agent 无 runtime（client 降级信号）
    CodeProcessUnsupportedOp = "unsupported_process_operation"
    CodeProcessIdentity      = "process_identity"               // procrequest.ErrIdentity / ErrUnknown
    CodeProcessInstanceLost  = "process_instance_lost"          // procrequest.ErrInstance（§3.7，drill 34 的 ctl 半）
    CodeProcessExpired       = "process_expired"
    CodeProcessConflict      = "process_conflict"               // ErrConflict / workload.ErrReplayOffset
    CodeProcessCapacity      = "process_capacity"               // → 75
    CodeProcessStale         = "process_stale"
    CodeProcessBusy          = "process_busy"                   // ErrBusy / ErrFrozen（S2 才可达）
    CodeProcessRevoked       = "process_revoked"                // execsession.ErrRevoked（§3.5）
    CodeProcessOutputDiscarded = "process_output_discarded"     // execsession.ErrOutputDiscarded（§3.6）
    CodeProcessRuntimeClosed = "process_runtime_closed"         // execsession.ErrClosed → 75
)
```

`internal/proto/messages.go:292-306` `LocalProcess` 追加：
```go
// StartedUnacked (S1 C-5, additive/omitempty): this agent has spawned the pid but the broker has not yet
// acknowledged its proc.started, so the row may not exist yet. A new broker must not orphan-kill it
// (reconcile.go); an old broker ignores the field, which is today's window — never worse.
StartedUnacked bool `json:"started_unacked,omitempty"`
```
`forward_failed` 沿用既有码（表内已归 75）。`TestProcessOutputCodeAndStartedUnackedAreOmitEmpty`（`internal/proto/process_resume_test.go`）钉零值不出现键；`go test ./internal/proto -run TestWireFieldInventoryAppendOnly -update-wire-inventory` 后 `git diff --numstat` 期望 `2 0`。

### 2.2 新包 `internal/procforward`（raw overlay 出 broker，broker 文件 =70、方法 =285、代码行余 ≈600 不动）

```go
package procforward
const MaxRequestBytes = 1 << 20
type Envelope struct { CallID uint64; Version int; Op string; raw, exec map[string]json.RawMessage; hasExec bool }
// Parse reads only version/call_id/op (+ whether exec is a non-null object); every other field is opaque.
// Non-object body, non-object exec, or a call_id that is not an unsigned integer → error.
func Parse(body []byte) (*Envelope, error)
// Stamp deletes every key strings.EqualFold(k,"actor_fp") at the top level and inside exec, writes the
// canonical "actor_fp" (quoted fp) in both, and re-encodes with json.Marshal(map): unknown fields survive
// byte-for-byte AS VALUES (keys re-sorted, RawMessage compacted).
func (e *Envelope) Stamp(actorFP string) ([]byte, error)
```
为什么要剥大小写变体：`encoding/json` 结构体解码键名不区分大小写、重复键取输入序最后一个；map 重编按字典序恰好让 `actor_fp` 排最后是巧合不是契约，`Stamp` 把它变成契约（handoff-protocol §3.5「agent 只信 broker 盖章」的 raw 形式）。文件：`forward.go`、`forward_test.go`、`forward_fuzz_test.go`（`testdata/fuzz/FuzzParseStamp/` ≤64 文件，首行 `go test fuzz v1`）。

### 2.3 `internal/broker/exec.go:568-616` `handleProcessResumeReq` 重写（顺序 = handoff-protocol §3.7 活契约）

```go
func handleProcessResumeReq(b *Broker, nc *nats.Conn, msg *nats.Msg) {
    ing, _, ok := b.admitSubject(msg.Subject, processResumeSpec)
    if !ok || !privateProcessReply(ing.actor, msg.Reply) || b.isClusterFollower() { return }   // ① reply guard 先于任何错误回复
    if len(msg.Data) > procforward.MaxRequestBytes { processResumeReject(b, msg, 0, proto.CodeProcessTooLarge, "process request too large"); return }
    env, err := procforward.Parse(msg.Data)
    if err != nil { processResumeReject(b, msg, 0, proto.CodeProcessBadRequest, "invalid process request"); return }
    spec := processResumeSpec                                   // probe / 未知 op：节点必须 ONLINE
    switch env.Op { case "create", "resume", "ack", "terminal-ack": spec = processResumeAttachSpec }  // 只用 op 选 spec，不在此拒绝
    if den, ok := b.admitACL(&ing, spec); !ok { processResumeReject(b, msg, env.CallID, den.code, den.reason); return }   // ② ACL 先于协议/op 错误
    if env.Version != proto.ProcessResumeVersion { processResumeReject(b, msg, env.CallID, proto.CodeProcessUnsupported, "unsupported process protocol"); return }
    switch env.Op { case "probe", "create", "resume", "ack", "terminal-ack":
    default: processResumeReject(b, msg, env.CallID, proto.CodeProcessUnsupportedOp, "unsupported process operation"); return }
    body, err := env.Stamp(ing.fp)
    if err != nil { processResumeReject(b, msg, env.CallID, proto.CodeProcessBadRequest, "invalid process request"); return }
    if err := nc.PublishMsg(&nats.Msg{Subject: proto.SubjCmdForwarded(ing.sid, ing.nid, ing.verb), Reply: msg.Reply, Data: body}); err != nil {
        processResumeReject(b, msg, env.CallID, "forward_failed", "process forward failed"); return }
    if env.Op == "create" { b.pubAuditCall(ing.sid, ing.fp, ing.actor, ing.verb, ing.nid, true, "", msg.Reply, nil) }
}
func processResumeReject(b *Broker, msg *nats.Msg, callID uint64, code, reason string) {  // 包级；code-carrying helper（参数 3）
    b.replyJSON(msg, proto.ProcessResumeReply{Version: proto.ProcessResumeVersion, CallID: callID, Kind: "error", Code: code, Error: reason})
}
```
- 保留原头注释（reply 注入论证 L565-567），追加「为什么 raw overlay」「为什么按 op 选 spec 但 ACL 仍先于 version/op 拒绝」两段。
- reason 文本一字不改（`not_a_member`、`node_offline: status=STALE`…），旧 client 的文本分类不受影响。
- oversize/malformed 的 `CallID=0` 是明文边界（§3.7），测试显式断言 0。
- `privateProcessReply` L618-637 **不改** → fuzz 只留收据。
- 转发 create 的 audit **天然可多条**（回复丢失 → client 重发同 CallID → 每次转发都 `pubAuditCall`）；「无重复执行」的 oracle 一律取 agent 侧证据（marker 文件恰 1、`ps` 恰 1 行、`agent: exec` 日志恰 1 行），audit 只记录——写进 handoff 活契约。

### 2.4 `admit.go`：attach 类 op 放行 STALE（gotcha #90）

```go
// verbSpec 追加（L56-75）：
// allowStaleNode lets a STALE node (heartbeat gap > StaleAfter, agent most likely alive) through the node
// check; OFFLINE is still refused. Zero value = strict, matching this file's polarity rule. Only the
// process-resume ATTACH ops set it: the task already lives on the agent, and a 5 s heartbeat gap during a
// NATS/broker restart must not turn a running exec into `node_offline` (drill 35 arms A/B). gotcha #90.
allowStaleNode bool
// admitACL L315：
case status != node.StateOnline && !(spec.allowStaleNode && status == node.StateStale):
// L341 旁：
processResumeAttachSpec = verbSpec{verb: proto.ProcessResumeVerb, role: admitMember, allowStaleNode: true}
```
`admit_test.go` 极性表加入该 spec（不 `skipNodeCheck`、member、不 echo），新增 `TestVerbSpecPolarityAllowStaleNode`（只有它为 true）；`admit_subscription_reconcile_test.go` 的 spec 列表加入。**放宽的代价**（写进 gotcha #90 与 usage）：真死 agent 在 STALE 窗（5–60s）内的 attach 请求会被转发到无人订阅的 subject——ctl **不会**收到 503（no-responders 只发给自己持有 reply 订阅的发布者，broker 不持 ctl 的 `_TINBOX` 订阅），而是按 500ms attempt 超时重发，直到 OFFLINE（≤60s）拿到 `node_offline: status=OFFLINE` → 74（create 后）。

### 2.5 `reconcile.go`：C-5 的 broker 半（**两处**循环）

纯分类器 L119-124 与 live L334-350 的 orphan 循环各插入（`if lp.State != "running" { continue }` 之后）：
```go
if lp.StartedUnacked {
    // C-5: the agent adopted this child but the courier still owes us its proc.started — the row simply
    // may not exist yet. Killing it here is the G.1 false-orphan window drainStarted only narrows.
    // Neither accepted (the courier keeps the started pending) nor dropped. origin: hot-update reliable-exec plan §2.5.
    continue   // live 分支另打一条 Info 日志
}
```
`reconcile_marks_test.go` 的分类器/live 等价性表加 `StartedUnacked` 输入行 + `TestReconcileDefersOrphanKillForStartedUnackedRow`。已知行 + running + 标志 → 原 `accepted` 臂（L245-274）→ courier `onRegisterSuccess` 以 `accepted && row.State=="running"` settle started（proc_delivery.go L429-432）。N-1：旧 broker 忽略字段（今天的窗口）；旧 agent 不发字段（今天）。零 Raft op、零 schema。

### 2.6 撤权（exit #3 的解读，**待 §10-Q2 裁定**）

事实：产品无 member 移除动词（`DELETE FROM members` 只有 `audit.go:144` 的 `session rm` 级联）；broker 对 member 撤销只在 `admitACL` 拒绝 ctl；`sys.events` 只有 broker 可发（`session_destroyed`、`agent_evicted`）。推荐解读：
- (a) member 撤销中途 = broker 每 op 重查 → ctl 得 `not_a_member`（77，不降级，无第二 child）；agent 上的任务照跑到自己退出，其 terminal 由 §3.6 有界保留回收；
- (b) 「agent task SIGKILL 并有 terminal + tombstone 拒绝复活」由 **`session_destroyed`**（C-10）承担（§3.5）。
不新造 broker→agent 撤权通道（agent 草案的 `revoked` 盖章无 Reply 转发反转了 C.1「pre-forward 失败只回复」模型、是机会主义的、且漏改 `agent/exec.go:617` 的 `msg.Reply==""` 早返回）。若主进程选盖章方案：wire +1（`ProcessResumeReq.Revoked`）、broker 入口 `Stamp` 同时剥离 `EqualFold(k,"revoked")`、改 617 守卫、加伪造 `Revoked` 负控制。

### 2.7 闸门账本

- `cmd/tether/error_code_coverage_test.go`：`codeCarryingHelpers["processResumeReject"] = 3`（`TestCodeCarryingHelperListIsComplete` 会机械再推导）；`unresolvedCodeSites["internal/broker/exec.go:handleProcessResumeReq#1"] = "den.code from admitACL; the eight admit denial codes are classified by TestAdmitRefusalCodesAreClassified"`（ordinal 以实际扫描为准，写码后跑门取真实键）；agent 侧见 §3.4。
- `proto.CodeProcess*` 常量落在 `scannedTrees` 内（`internal/proto`），字面量自动进 reverse-check universe，表里新加的 `process_capacity`/`process_runtime_closed` 不会被判 stale。
- ACL↔订阅表账本：ctl grant `s.<sid>.cmd.by.<actor>.node.*.*.req` 已覆盖 `process-resume`，agent `_TINBOX.>` Pub 已在模板 → **零改动**；收据 `go test ./internal/auth -run TestACL -count=1`。

### 2.8 N-1 四象限（hermetic，`go test` 内不构建 v0.6.0）

| 象限 | 模拟 | 期望 |
|---|---|---|
| Q1 新 ctl × 旧 broker | `legacyBrokerSim`：只订阅 `s.*.cmd.by.*.node.*.exec.req`、按 L67-90 legacy 语义 decode→re-marshal→forward；不订 `process-resume.req` → ctl 自己拿到 503 | probe 2s 内降级 legacy；观察订阅断言零 process-resume create |
| Q2 旧 ctl × 新 broker | 现有 `runExec`（`exec_io_test.go:28`）走 legacy `exec.req` | 不变；drill 35 臂 C1 用真 v0.6.0 ctl |
| Q3 新 ctl × 旧 agent | `legacyAgentSim`：订 `cmd.node.<nid>.*.req.forwarded` 只答 `exec`，对 `process-resume` 静默（v0.6.0 `dispatchForwarded` default 臂 = Warn+丢弃） | probe 2s 超时 → `ErrUnsupported` → legacy；**记 2s 税 + 旧 agent 每次 exec ~4 条 `unknown forwarded verb` warn**（probe 每 500ms 重发都被转发）进 usage/gotcha；drill 35 臂 C2 用真 v0.6.0 agent |
| Q4 旧 broker × 新 agent | Q1 sim + 真 agent | legacy `handleExecForwarded` 不变 |

**broker 重启窗内的新请求**（deploy 批评 MAJOR）：旧 broker 与缺席的 broker 都表现为 503，probe 2s 后新 `exec` 会静默降级 legacy。S1 裁决：**保持 2s probe 降级**（不引入 version announce 依赖），但 (i) usage 写明「broker 重启窗内的新请求不承诺可靠路径」；(ii) e2e 与 drill 35 的每个新请求探针都断言/记录它走了哪条路径（`reliable=true` 计数），降级不得被计作续接成功。

---

## §3 agent 端

### 3.1 单一 started 站点 + 真实 Identity（任务表合一的第一半）

事实：任务表**今天已是一张** `a.workloads`（reliable `initProcessRuntime.Start` → `startExecPipes` → `registerExecChild` → `AdoptDraining`，legacy `runChild` 同入口），`ps`/`kill`/`DropProcesses`/`reapManagedChildren`/G.1 快照对 reliable 任务已生效。S1 要修的是三处不合一的**时序/载荷**：reliable `Started` 在真实 `Start` 之前（runtime.go:197）且 `""/0`；legacy `enqueueStarted` 在 spawn 之前（exec.go:144）且 `""/0`；PTY `run` 在 adopt 之后带完整三元组（范本）。

改法（`internal/agent/exec.go` / `run.go`）：
- `registerExecChild(pid, p)` 拆成 `execChildIdentity(pid, p) workload.Identity`（`OSPID`、`readBootID()`、`readStartTimeTicks`、`StartedAt`）+ `a.workloads.AdoptDraining(id, p)`。
- `startExecPipes` L229-241：`cmd.Start` 成功 → `id := execChildIdentity(pid, cmd.Process)` → **先** `a.courier.enqueueStarted(pid, req.Argv, req.ActorFP, id.BootID, id.StartTimeTicks, id.StartedAt)` → **再** `AdoptDraining(id, p)`。顺序是 C-5 的关键：started 必须先于 workloads 行可见（agent 批评 MAJOR：adopt→enqueue 会在微秒尺度重开窗口）。返回值加 `id`：`(stdio *workload.Stdio, id workload.Identity, warning string, err error)`。
- 删除 exec.go:144（legacy 先于 spawn 的入队）与 exec.go:604-606（runtime hook 里的入队）。`runChild` 用新签名；其 defer（L251-254）`enqueueExit(pid, -1)` 对从未 started 的 pid → broker `unknown_pid` OK 丢弃（既有 L212-214 路径）。
- **行为变化（写进 usage §5.12 与 architecture）**：spawn 失败的 exec 不再有 `proc.started`，`ps -a` 不再出现 EXITED(-1) 行（与 `run` 的既定约定一致，run.go:283-291）；ctl 仍收 `error` chunk、`audit.call` 仍记录。**实现前先 `grep -rn "EXITED\|exit_code.*-1" test/p4 test/p7 internal/agent/*_test.go`** 找断言失败 exec 出现在 ps 的测试并改测试。
- `initProcessRuntime` 的 `Started` 改为日志 hook：`a.cfg.Logger.Info("agent: exec", "pid", pid, "argv", req.Argv, "reliable", true, "os_pid", id.OSPID)`（与 legacy L138 同前缀，drill 35/34 用 `sim_agent_slog_count agt1 "agent: exec.*<nonce>"` 计数 =1、`reliable=true` 计数区分路径）。

### 3.2 `localProcessSnapshot`（run.go:618-645）：并入 pending started + `StartedUnacked`

`procCourier` 新方法（courier 方法不占 Agent 126 预算）`pendingStartedSnapshot() []proto.LocalProcess`：返回 `kind==started && !parked` 的行 `{PID, State:"running", StartedAt, StartTimeTicks, StartedUnacked:true}`（`pendingProcEvent` 在 `enqueueStarted` 时额外记 `startedAt`/`startTimeTicks` 字段）。合并顺序：workloads 行 → pending started（已存在则置 `StartedUnacked=true`，不存在则新增行——覆盖「已入队未 adopt」窗）→ pending exits（覆盖）。读序不变（先 owners 后 courier）。窗口分析（写在函数注释里）：pid 在读 workloads 与读 courier 之间被 adopt → courier 有 started → 行 running+unacked ✓；started 在两次读之间被 ACK → workloads 有行、courier 无 started → running 无标志，broker 已有行 → accepted ✓；反向（adopt 先于入队）不可能，因入队先于 adopt。标志多报一次 true 的方向是「不杀」，安全。

### 3.3 `internal/execsession/runtime.go`

- `Config.Started func(pid string, spec proto.ExecReq, id workload.Identity)`；删除 L197；在 L202 之后：
  ```go
  id, adopted := r.cfg.Manager.Lookup(t.pid)     // workload.Manager 新方法 Lookup(id string) (Record, bool)（workload 无预算）
  if !adopted { stdio.Close(); r.failStart(t, errUnadopted); return }   // adapter 违约：Start 成功 == 已 adopt
  r.cfg.Started(t.pid, spec, id.Identity)
  ```
- `observe` L331-335：`if err := AllowReap(...); err != nil { if errors.Is(err, workload.ErrUnknown) { r.finish(t, workload.Exit{Code:-1, WaitError: err.Error()}, err); unlock; return }; unlock; <-timer.C; continue }`——ErrUnknown 不再 10ms 自旋（S0 §5 带入项关闭）；ErrFrozen 保留重试（S2）。
- `finish` L352-364：`Complete` 返回 `ErrFrozen` → `return false`（observe 下一 tick 重试）；其它错误 → 仍 `Exited` + `Forget` + `Logger`（broker 行与进程表是运维真相，不依赖客户端结算）+ `t.lostTerminal = true`，`Handle` 对此 task 返回 `ErrTerminalUnrecorded`（Code `process_identity`，我方 bug 类）。签名改 `finish(...) (recorded bool)`。
- `Handle` L84-86 的 `"exec session: runtime closed"` 改为导出哨兵 `ErrClosed`。
- `task` 增 `terminalAt, lastTouch int64`（boot ns），`Handle` 命中 pid 时更新 `lastTouch`。

### 3.4 `handleProcessResumeForwarded`（exec.go:616-646）错误 → Code

新增包级 `processRefusalCode(err error) string`（exec.go 尾部）：`errors.Is` 链 `procrequest.ErrCapacity→CodeProcessCapacity`、`ErrExpired→Expired`、`ErrConflict|workload.ErrReplayOffset→Conflict`、`ErrStale→Stale`、`ErrInstance→InstanceLost`、`ErrIdentity|ErrUnknown→Identity`、`ErrBusy|ErrFrozen→Busy`、`execsession.ErrRevoked→Revoked`、`ErrOutputDiscarded→OutputDiscarded`、`ErrClosed→RuntimeClosed`、`ErrTerminalUnrecorded→Identity`、`a.processRuntime==nil→Unsupported`；default `""`。回复改写成**复合字面量**（让 `Code:` 成为可登记站点）：
```go
reply = proto.ProcessResumeReply{Version: proto.ProcessResumeVersion, CallID: reply.CallID, Kind: "error", Code: processRefusalCode(err), Error: err.Error()}
```
`unresolvedCodeSites["internal/agent/exec.go:handleProcessResumeForwarded#1"] = "Code is processRefusalCode(err): its outcomes are the proto.CodeProcess* constants (scanned at their declarations). process_capacity / process_runtime_closed are classified 75; the rest are post-create-only and take exec.go's phase rule (74), or are our-bug 70 before create."`。

### 3.5 C-10 exec 半：revoke + tombstone（新文件 `internal/agent/process_revoke.go` 16→17 文件；`internal/execsession/revoke.go`）

execsession：
```go
type revocation struct{ AtBootNS int64; Reason string }
var ErrRevoked = errors.New("exec session: task revoked by session authority")
// Revoke SIGKILLs every task's owned process group (exec children are Setpgid), marks each task revoked,
// and records the revocation so a CREATE whose lease predates it is refused (a lease issued after a later
// re-creation of the session is not). Terminal tasks stay settleable; nothing can be revived.
func (r *Runtime) Revoke(sid, reason string, now int64) (killed int)
```
- `Handle`：任一 op 命中 `t.revoked != ""` 或（create 且 `r.revocations[scope.SID]` 存在且 `lease.IssuedBootNS < rev.AtBootNS`）→ `ErrRevoked`；同 handle 的 create 走 `Reserve` 回原记录后同样命中 → 不复活。`finish`：`cause==nil && t.revoked != ""` → `terminal.Error = "revoked: "+reason`。revocation 条目由 `Sweep` 在 `AtBootNS + procrequest.MaxTTL` 后删除。`Close` 丢弃（S2 起随 image 导出）。
- agent（包级）：`revokeSessionWorkloads(a *Agent, reason string)`：`a.processRuntime.Revoke(a.cfg.SID, reason, now)` → `a.reapManagedChildren()`（legacy exec + PTY，run.go:579）。`agent.go:1141-1171` sys.events 回调在 `agent_evicted` 判断前加：`ev.Type=="session_destroyed" && ev.SID==a.cfg.SID` → 日志 + `revokeSessionWorkloads(a, "session_destroyed")`（**不 `cancelRun()`**，见 §10-Q3）。`session_destroyed` 的 NID 为空，不比对 NID。
- **freeze 与删除竞争的定义（写进 plan/architecture，S3 实现导入半）**：撤权是权威事件永远赢。(a) 未冻结 → 立即 kill + tombstone；(b) 已冻结（S2）→ `Revoke` 只登记 `pendingRevocations`（随 image 导出），kill 推迟到 `Thaw`，哪个映像 thaw 就由它执行；(c) 事件落在 exec 窗内丢失 → 持久的另一半是 register 拒绝：新映像首次 register 得 `session_not_found_or_deleting` 时按 `session_destroyed` 处理（agent.go 默认臂前插入，S3）。tombstone 随 image 导出（S2 schema 纳入 `revoked`），导入后 `Handle` 继续拒绝（S3 `TestSessionDestroyedTombstoneSurvivesImport`）。

### 3.6 abandoned-terminal 有界保留（S0 protocol 核验者 missed 项）

execsession：
```go
const AbandonedTerminalGrace = 2 * time.Minute  // 终态后无任何客户端触达达此时长（且 lease 已过期）→ 释放 replay
const DiscardTombstoneTTL   = 15 * time.Minute  // 之后 registry.Settle + Collect；晚重试得 ErrExpired
var ErrOutputDiscarded = errors.New("exec session: output discarded after the client abandoned the terminal")
type tombstone struct{ TerminalID string; ExitCode, Signal int; Error string; FinalEnd [2]uint64; FinalFIN [2]bool; AtBootNS int64 }
func (r *Runtime) Sweep(now int64) (discarded, expired int)
```
- `Sweep`：对每个 `done` 已关、registry phase==terminal、`now >= max(lease.ExpiresBootNS, t.lastTouch+Grace)` 的 task → 取 `t.logs` 快照记 tombstone → `retireLocked(t)`（释放三份 replay 预算）→ `r.discarded[pid]`；对 `discarded` 中 `now >= AtBootNS+TTL` 的 → `registry.Settle(scopeOf(lease), handle, TerminalID)` + `delete`；随后 `registry.Collect()`；再清过期 revocations。
- `Handle`：registry 记录 phase==terminal 且 `r.tasks[pid]==nil` 且 `r.discarded[pid]` 存在：`terminal-ack` 且 `req.TerminalID==ts.TerminalID && acks==ts.FinalEnd && fins==ts.FinalFIN` → `Settle` + 删 tombstone → `settled`（已持有全部字节的客户端仍可结清）；其它 op → 回复带 `TerminalID/ExitCode/Signal/Error` + `ErrOutputDiscarded`。
- 触发点：create 分支 `r.registry.Collect()` 之后调 `r.sweepLocked(now)`；**且** agent `heartbeatLoop` tick 调包级 `sweepProcessRuntime(a)`（`procrequest.BootNow()` + `a.processRuntime.Sweep(now)`），空闲节点也受 lease+grace 封顶（不新增 goroutine，无 ctx 站点）。
- 时钟注入：`Config.Clock func() (int64, error)`（nil → `procrequest.BootNow`）；`procrequest.NewWithClock(instance, limit, ttl, clock)` 让 registry 与 runtime 共用同一注入钟（registry 的 `now` 字段是私有的，跨包测试需要它）。
- **running-abandoned 不在 S1**（C-6 的 S2 半）：无人 ACK 的 running 任务在输出累积 8 MiB 后对原 pipe 施加背压、child 卡住直到自己退出；usage 与 `--timeout` 文案如实写。
- `Config.ReplayCapacity` 已可缩小（New L63-68），负控制用小预算。

### 3.7 procrequest（`registry.go`）

- `ErrInstance = errors.New("process request: lease was issued by another runtime instance; task continuity lost")`；`validate` L302-312 在整体校验**之前**先判 `l.TargetInstance != r.instance || l.BootID != r.bootID` → `ErrInstance`（异实例 lease 无法验 MAC；不泄露任何东西，instance 本就在 capability 回复里）。`Reserve`/`Attach`/`Lookup` 自动继承 → drill 34 的 ctl 半得 `process_instance_lost`。
- `func BootNow() (int64, error) { return bootNow() }` 导出；`NewWithClock`。
- 容量：`Reserve` L162 已在副作用前 `ErrCapacity` ✓；`Collect` 只回收 settled ✓——补测试不改码。

### 3.8 落点与预算

| 文件 | 类型 | 预算 |
|---|---|---|
| `internal/proto/process_resume.go`、`messages.go` | 改 | wire +2 |
| `internal/procrequest/registry.go` | 改（ErrInstance、BootNow、NewWithClock） | — |
| `internal/execsession/runtime.go`、`revoke.go`（新） | 改/新 | — |
| `internal/workload/process.go` | 改（`Lookup`） | — |
| `internal/agent/exec.go`、`run.go`、`agent.go`、`proc_delivery.go`、`process_revoke.go`（新） | 改/新 | Agent 方法 =126（新逻辑全为包级函数或 courier 方法）；文件 17 <20；代码行 ≈6.8k+260 <8000 |
| `internal/broker/exec.go`、`admit.go`、`reconcile.go` | 改 | 文件 =70、方法 =285；+≈60 行（余 ≈600） |
| `internal/procforward/`（新包） | 新 | — |

---

## §4 测试清单

### 4.1 真实二进制 e2e harness 设计（`test/cli_e2e/`，Linux-only，`//go:build linux`）

- **`realstack_harness_test.go`**：
  - `buildTether(t)`：`sync.Once` + `go build -trimpath -o <MkdirTemp>/tether ./cmd/tether`（p4 `buildCtl` L388-411 形状；`repoRoot` 用 `git rev-parse --show-toplevel`）。
  - `newRealStack(t, opts...)`：`reserveTCPPort` + 同端口可重启嵌入 nats-server（`test/chaos/chaos_harness_test.go:77 startNATSOnPort` 形状）。**匿名模式必须 `opts.AlwaysEnableNonce = true`**（否则带 nkey 的真 ctl 得 `nkeys not supported`；而 `TETHER_DEV_NO_AUTH` 会让可靠路径静默退化为 legacy——这是本 harness 最重要的一行，写进 testing-standards）；`withCallout()` 模式照抄 `test/p3/setup_test.go:37-161`（Nkeys+AuthCallout、`broker.AuthCalloutConfig`、`waitAuthCalloutReady`）——把这三段搬进 `test/stackharness/authstack.go`（layering 允许 `_test.go`/`test/` 树 import）。broker：进程内 `broker.New/Run`，**`StaleAfter/OfflineAfter` 留默认 5s/60s**（真 agent 子进程 5s 心跳；harness_test.go:70-78 的 300ms/900ms 会让任何 >0.9s 的 bounce 变 OFFLINE）；`stopBroker()/startBroker()` 复用同一 `*sql.DB`。
  - 身份：匿名模式 `writeIdentity(home, sid)` + `stackharness.SeedSession` + `session.AddMember`；callout 模式走真实 `tether login -s lab --pin <pin> --home <home>`（顺带产生 auth receipt）。
  - `startAgentProcess(nid)`：`exec.Command(bin, "agent", "--session", sid, "--nid", nid, "--nats-url", url, "--home", agentHome, "--tunnel-addr", "")`（callout 模式加 `--pin`），`Setpgid`，stdout/stderr 落 `t.TempDir()/agent.out`，`testharness.WaitNodeOnline(db, sid, nid, 15s)`；`stop()` = SIGTERM + 10s + `kill(-pgid, SIGKILL)` 兜底，`t.Cleanup` 幂等。
  - `ctl(ctx, args...) *ctlRun`：`exec.CommandContext(bin, append([]string{"exec","--home",home,"--nats-url",url}, args...)...)`，Env **不含** `TETHER_DEV_NO_AUTH`；返回 stdout/stderr、rc（被信号杀记 `128+sig`）。
  - `kickAgentConn()`：`ns.Connz(&ConnzOptions{State: ConnOpen})` 找 `Name == "tether-agent:<sid>:<nid>"` → `ns.DisconnectClientByID(cid)`；恰 1 条否则 Fatal（非空性）。`restartNATS(gap)`、`restartBroker(gap)`。
  - `producerScript(t, n, period, rc)`：期望字节在测试内生成、逐字节比较；写 `$marker.<pid>` 文件（无重复执行的 oracle）。
  - `agentLogCount(pattern)`：对 `agent.out` grep 计数（每条可靠用例断言 `agent: exec .* reliable=true` ==1）。
- **`natsrelay_test.go`**：透明 TCP relay `127.0.0.1:0 → nats-server`，按 NATS 文本帧解析 client→server 流（`PUB <subj> <reply> <n>\r\n<payload>\r\n`），谓词命中（subject 含 `process-resume.req` 且 payload 含 `"op":"create"` / `"op":"terminal-ack"`）时**整帧转发完再同时关两端**，只切一次。ctl 经 `--nats-url nats://127.0.0.1:<relayPort>`；nats.go 首次重连即睡 `ReconnectWait` 2s ≫ agent 回复毫秒级 → 回复确定性丢失（agent 用 Publish 无 reply，不产生 503）。不伪造任何字节。
- **`exec_reliable_test.go`** 包头三行：起了什么真的 / 打桩了什么 / 明确不覆盖（PTY、tier-B、Darwin）。
- 矩阵登记：`test/e2e/all_phases_test.go` 加 `TestReliableExecMatrix`（`go test -race -count=1 -timeout 600s -run '^(TestExecReliable|TestCtlConnect)' ./test/cli_e2e/...`）——现有 `TestTransferDefaultsMatrix` 对 cli_e2e 带 `-run TestTransfer_*` 过滤（L181-183），新测试否则永不进 `make e2e-parallel`；`-update-matrix-inventory` 只增；`test/README.md` cli_e2e 行同步。`make test` 总时长预计 +60–90s。

### 4.2 测试表

| 名称 | 文件 | 断言 | 变异（期望红） |
|---|---|---|---|
| `TestRunReportsCreateSentOnlyAfterPublish` | `internal/execclient/client_test.go` | 用只在 create 时 `PublishRequest` 报错的 conn 包装（或 `nc.Close()` 后调用）：`Result.CreateSent==false`；正常 create 发出但回复永不到 + ctx 超时 → `CreateSent==true`、`PID==""` | 把 `created` 置位挪到 Handle 赋值处 → 负臂红 |
| `TestRunMarksExecutedOnlyAfterEvidence` | 同上 | failStart 形状（-1/0/空 FIN）→ `Executed=false`；收到 running 或字节或 signal → true | 恒 true → failStart 臂红 |
| `TestRunSurfacesTypedRefusalWithPhase` | 同上 | probe 阶段 `Code=not_a_member` → `*ProtocolError{AfterCreate:false}` 且非 ErrUnsupported；create 后 `Code=process_identity` → `AfterCreate:true`；`Code=""`+`unsupported process protocol` → ErrUnsupported；`Code=process_expired` 持双 end+terminal → 完成 | 不置 created → AfterCreate 臂红；只认文本不认 Code → expired 臂红 |
| `TestRunKeepsAttachEpochAcrossReconnect` | 同上 | 真嵌入 NATS + `DisconnectClientByID(ctl)` 中途：每个 create/resume/ack/terminal-ack 的 `AttachEpoch==1`、Reply inbox 不变；registry `Attach(epoch=1, 同 inbox)` 幂等 | 断线后 epoch++ 或换 inbox → 请求侧断言红（注意 registry 接受更高 epoch，变异只能靠请求侧断言与 `validateReply` 对账变红） |
| `TestRunDoesNotRepublishWhileReconnecting` | 同上 | 服务端 `DisconnectClientByID` 并延迟接受重连 3s：观察 forwarded 订阅计数，断连期间同 CallID 转发 ≤2 条（一条断前、一条重连后） | 去掉 `IsReconnecting` 分支 → 计数 ≥5 红 |
| `TestRunConvergesOnExpiredSettlementOnlyWhenHoldingBothEnds` | 同上 | 正：双 end+terminal 后 terminal-ack 回 `process_expired` → 完成、真实 exit/signal 保留；负：resume 阶段回 expired → 返回错误 | 收敛条件放宽到任意阶段 → 负臂红 |
| `TestRunShortWriteAcksDeliveredBytesAndLeavesTaskRetained` | 同上 | `partialDestination` 短写 → 最后 ack 的 `StdoutACK`==实际写入、不发 terminal-ack、Run 返回 `io.ErrShortWrite` | accept 推进 offset 到 len → 红 |
| `TestRunNeverDowngradesAfterCreate` | 同上 | create 得 status 后 resume 回 `not_a_member` → 保留权限错、非 ErrUnsupported | fallback 放宽到任意错误 → 红 |
| `TestReliableExecFailureClassTable` | `cmd/tether/exec_exit_class_test.go` | §1.1 表逐行：`classifyExit(classifyReliableFailure(...))`；含 SettledError Executed/非 Executed、ProtocolError 各码 × CreateSent、ctx 错误 × CreateSent、ErrConnectionClosed；断言 74 ∉ {69,70,75}、文案含 `NOT killed`/`task <id>` | 去掉 CreateSent 分流 → 多行红；74 改 75 → 红 |
| `TestExitUnsettledIsPresentationOnly` | `cmd/tether/exitcode_test.go` | `exitUnsettled==74`；不在 `brokerCodeExitClasses` 值域；`classifyExit(&ExitError{Class: exitUnsettled})==74` | 常量改 75 → 红 |
| `TestConnectWithAuthRetryPolicy` | `cmd/tether/ctl_connect_test.go`（新） | fake attempt：`eligible=false` 单次即返、report 0 次；`eligible=true` + 前两次返回 `&net.OpError{Err: timeoutErr}` 第三次成功 → 成功且 report 2 次、总耗时 ∈ [750ms, budget+1s]；非 transient 错误（`ErrNkeysNotSupported`）不重试；ctx 取消打断等待；`transientConnectErr` 对 ErrNoServers/ErrTimeout/net timeout/"Authorization Violation" 真、对 `errors.New("x")` 假 | 谓词恒真 → 非 transient 臂红；删 `!eligible` 短路 → 负臂红 |
| `TestAuthReceiptLifecycle` / `TestCtlConnOptionsIgnoreAuthErrorAbort` | `internal/cli/auth_receipt_test.go` / `natsconn_test.go` | 写→有；指纹不等→无；不同 sid 互不影响；home 不存在→false 不 panic；mode 0600 / `ctlConnOptions` 施加后 `IgnoreAuthErrorAbort==true`、`MaxReconnect==-1` | 写错路径 / 删 option → 红 |
| `TestExecStdinFlagBoundedAndForwarded`（若 Q1 采纳） | `cmd/tether/exec_stdin_test.go` | `cmd.SetIn` 喂 48KiB → 抓到的 ExecReq.Stdin 逐字节相等（fake responder，p4 形状）；512KiB+1 → 64 且零 publish；上限+最大信封 marshal < 1<<20 | 去 LimitReader → 超限臂红 |
| `TestParseStampPreservesUnknownFieldsAndStripsActorVariants` / `FuzzParseStamp` | `internal/procforward/forward_test.go` / `forward_fuzz_test.go` | 顶层/exec/handle.lease 内未知键语义保留（比较解码后的 map，不比字节）、`Actor_FP`/`ACTOR_FP` 变体被删、规范 `actor_fp` 两层 == fp；非对象/exec 非对象/call_id 负数或字符串 → Parse 错；fuzz：Parse 出错或 Stamp 为合法 JSON 且解码为 `ProcessResumeReq` 除 ActorFP 外与输入 struct 解码逐字段相等 | 去掉 EqualFold → 变体残留红；退回 struct 重编 → 未知键丢失红 |
| `TestProcessResumeForwardPreservesUnknownFields` | `internal/broker/process_resume_test.go` | 经真实 handler 转发后的 map 逐键等于输入（含三层未知键）、actor_fp 两处为 `ing.fp`、无大小写变体 | 恢复 typed re-marshal → 红 |
| `TestProcessResumeErrorPreservesCallID`（**既有，扩臂**） | 同上 | 三臂加 `Code` 断言（`not_a_member`/`unsupported_process_protocol`/`unsupported_process_operation`）；新增 oversize(1MiB+1)/malformed 两臂 `Code` 为 `process_request_too_large`/`bad_request` 且 `CallID==0` | reject 传 0 → 三臂红 |
| `TestProcessResumeACLPrecedesProtocolErrors` | 同上 | 非成员发 version=99 / op=bogus → 得 `not_a_member`（不是 unsupported） | 把 version/op 拒绝挪到 ACL 前 → 红 |
| `TestProcessResumeAttachOpsPassStaleNodeButNotOffline` | 同上 | 节点 STALE：probe → `node_offline: status=STALE`；create/resume/ack/terminal-ack → 转发 1 条；OFFLINE：五种 op 全拒零转发；DELETING 会话：全拒 | 未接入 admitACL → STALE 臂红；误用 skipNodeCheck → OFFLINE 臂红 |
| `TestVerbSpecPolarityAllowStaleNode` | `internal/broker/admit_test.go` | 只有 `processResumeAttachSpec.allowStaleNode`；它不 skipNodeCheck、member、不 echo | 给 execSpec 打开 → 红 |
| `TestReconcileDefersOrphanKillForStartedUnackedRow` | `internal/broker/reconcile_marks_test.go` | agent-only running 行 `StartedUnacked=true`：分类器无 `killed_orphan`、live 的 DropProcesses 不含；false 同输入 → 有；等价性表加该行 | 删任一处 `continue` → 红 |
| `TestProcessOutputCodeAndStartedUnackedAreOmitEmpty` | `internal/proto/process_resume_test.go` | 零值不出现 `code`/`started_unacked` 键；缺键解码零值合法 | 去 omitempty → 红 |
| `TestRegistryRejectsForeignInstanceBeforeMAC` | `internal/procrequest/registry_test.go` | 两实例互查对方 lease → `ErrInstance`；同实例篡改 MAC → `ErrIdentity`；`limit=2` 第 3 个 Reserve `ErrCapacity` 无副作用；settled+expired 才 Collect | 删实例先判 → 异实例得 ErrIdentity 红 |
| `TestRuntimeStartedFiresAfterRealStartWithIdentity` | `internal/execsession/runtime_test.go` | Started 收到 `OSPID>0`、`StartTimeTicks>0` 且在 Start 返回之后（通道序号）；Start 失败 → Started 不触发 | 回退到 L197 顺序 → 序号臂红 |
| `TestRuntimeUnadoptedChildFinishesWithoutSpinning` | 同上 | 违约 adapter（Start 成功不 Adopt）→ 终态 Error 含 `unadopted`、observe 1s 内返回、Exited 被调用 | 去掉 ErrUnknown 分支 → 超时红 |
| `TestRuntimeFinishDeliversExitWhenCompleteFails` | 同上 | 同包 `registry.Freeze()`：child 退出后 Exited 未调用、无 terminal；Thaw 后 ≤100ms 记录；非 frozen 失败（注入 lookup 失败的 handle）→ Exited 仍调用、Manager 空、Handle 得 `ErrTerminalUnrecorded` | frozen 也走 Exited → frozen 臂红 |
| `TestRuntimeSweepDiscardsAbandonedTerminalAndKeepsTombstone` | 同上 | `ReplayCapacity` 使 3 个 task 占满：3 个跑到 terminal 不 ack → 第 4 个 create `ErrBackpressure`（**负控制**）；`Sweep(now+lease+grace)` → discarded==3、tasks==0、第 4 个成功；旧 handle resume → `ErrOutputDiscarded` 且回复含原 TerminalID/ExitCode；terminal-ack 带正确 FinalEnd/FIN → settled；`Sweep(+TTL)` 后 → `ErrExpired`；running 任务不受影响；五轮 goroutine/fd/memfd 容差 0 | 不 retire → 第 4 个仍 backpressure 红；Discard 不看 grace → 立即弃红 |
| `TestRuntimeRevokeKillsScopeAndTombstoneRefusesRevival` | 同上 | 两 SID 各一 sleep child（后代同 pgid）：`Revoke(sidA)`：A 子树 ESRCH、终态 Signal 9、Error 前缀 `revoked:`；B 不受影响；A 旧 lease create → `ErrRevoked`；同 handle create → `ErrRevoked`；resume 仍可拿终态并 settle；Sweep 过 MaxTTL 后新 lease 可用 | 不比较 IssuedBootNS → 新 lease 也拒红；不记 reason → Error 臂红 |
| `TestExecStartedEnqueuedBeforeAdoptWithIdentity` | `internal/agent/process_registry_test.go` | 真 child 经 `runChild`：courier pending 的 started payload 含非空 BootID、ticks>0、与 `workloads.Snapshot` 行相等；在 `AdoptDraining` 前注入观察（测试钩子或顺序断言）started 已入队；spawn 失败（argv[0] 不存在）→ 无 started、只有 exit | 入队挪回 adopt 后 / spawn 前 → 红 |
| `TestRegisterSnapshotMarksStartedUnackedAndSurvivesReconcile` | `internal/agent/proc_register_receipt_test.go` | receiptFixture（缩短 `courierRequestTimeout`）：真 child adopt 后扣住 started ACK；触发 register；请求 LocalProcesses 该 pid `running` 且 `StartedUnacked=true`；真 Broker（openDB、无该 pid 行、节点有历史行）`reconcileOnRegister` → DropProcesses 不含、child alive；放行 ACK 再 register → false 且在 Accepted。`-race -count=3` | 标志恒 false / broker 不跳过 → 红 |
| `TestReliableTaskSurvivesSessionRebuild` | `internal/agent/process_runtime_test.go`（新，按单元命名） | in-process agent + 嵌入 NATS + fake register responder：直接向 forwarded subject 发 create → status；`a.fireRedial()`（roster_runtime_test.go:213 形状）触发真实 session 重建；重建后 resume → 同 PID、字节续接、child 存活、exit 精确 | 把 `stopProcessRuntime` 挂到 session 退出 → 红 |
| `TestSessionDestroyedRevokesReliableSubtreeAndTombstones` | `internal/agent/process_revoke_test.go` | 真 agent + 真 runtime：create `sh -c 'sleep 30 & exec sleep 30'`；发布 sys.events `session_destroyed{sid}`：≤3s pgid 内两进程 ESRCH；终态 Signal 9；courier 有 exit；同 handle create/resume → `process_revoked`；不同 SID 事件不触发；**agent 未退出**（runCtx 未取消，除非 Q3 改判） | 不分派 session_destroyed → 子树存活红 |
| `TestExecReliableRoundTripBytesAndExitCode` | `test/cli_e2e/exec_reliable_test.go` | 真 ctl `exec [--stdin] agt -- sh -c 'cat; echo ERR >&2; exit 23'` 喂 48KiB：stdout 逐字节等、stderr 恰 ERR、rc 23；`reliable=true` 日志恰 1 | overlay `outputBatch` 截 1 字节 → 红 |
| `TestExecReliableSignalExitIs128PlusSignal` | 同上 | `sh -c 'kill -TERM $$'` → rc 143 | 去 `128+Signal` → 红 |
| `TestExecReliableLostCreateReplyStartsOneChild` / `…LostTerminalAckReplySettlesOnce` | 同上 | relay 在首个 create / terminal-ack 帧后切断：marker 文件恰 1、字节完整、rc 精确、`ps --json` 恰 1 行、ctl stderr 有断线行（非空性）、<15s 返回 | `Reserve` 对已存在 key 返回 fresh → 2 marker 红；relay 谓词永不命中 → 断线行断言红 |
| `TestExecReliableAgentConnKickMidStream` | 同上 | 6s producer 期间 `kickAgentConn()` 两次：字节完整、rc 精确、agent 子进程 PID 与 child pid 不变、`agent: registered` ≥2、`kill orphan` 0 | `localProcessSnapshot` 不含 workloads 行 → 红 |
| `TestExecReliableNATSRestartMidStream` | 同上 | callout 模式：`restartNATS(1s)` 同端口，broker 延后 1.5s 重连（造 ≥2 次 auth 拒绝）：字节完整、rc 精确、PID 不变 | overlay 删 `IgnoreAuthErrorAbort` → 红（客户端 Timeout 需 > 服务端 auth_timeout 让 -ERR 真到达，harness 用 `AuthTimeout=0.5` 的服务端配置为此臂专用） |
| `TestExecReliableBrokerRestartMidStream` | 同上 | `restartBroker(6s)`（> StaleAfter 5s，确定性跨 STALE）：字节完整、rc 精确、broker 日志/`nodes` 表曾 STALE（非空性） | 关 allowStaleNode → 64 红 |
| `TestExecReliableNodeOfflineAfterCreateIsUnsettled` | 同上 | broker `OfflineAfter=2s`（此臂专用）+ SIGSTOP agent 4s：ctl 得 `node_offline: status=OFFLINE` → rc 74 且 stderr 含 handle | 把 OFFLINE 也放行 → ctl 卡到 --timeout 红 |
| `TestExecReliableLegacyFallbackOnOldBroker` / `OnOldAgent` | 同上 | §2.8 Q1/Q3 sim：rc 精确、observer probe ≥1 create ==0、耗时 ≥2s | probe 超时不映射 ErrUnsupported → 红 |
| `TestExecReliableTimeoutAbandonsWithoutRemoteKill` | 同上 | `--timeout 2s agt -- sh -c 'sleep 4; touch $done; exit 0'`：rc 74、3s 内返回、stderr 含 `NOT killed`；6s 内 `$done` 出现；`ps --json` 最终 EXITED(0) | ctx 错误走 75 → 红；agent 断连 SIGKILL → `$done` 不出现红 |
| `TestExecReliableStdoutClosedEarlyKeepsRemoteTruthful` | 同上 | 读 10 字节后关 stdout 读端：ctl rc 141（SIGPIPE，文档化）；远端 `ps` EXITED(23) 恰 1、marker 恰 1 | attach 断流时重新 Start → 2 marker 红 |
| `TestExecReliablePsShowsTaskRunningThenExited` | 同上 | 运行中 `ps --json` 有 RUNNING 行且 argv 含 nonce；结束后 EXITED exit_code 23 | Started 不入队 → 红 |
| `TestExecReliableRevokedMemberMidStreamExitsNoPerm` | 同上 | 中途 `DELETE FROM members`（fixture 代表撤权）：rc 77、stderr `not_a_member`、`exec.req.forwarded` 0 条 | 规则不豁免 77 → 74 红 |
| `TestExecReliableSessionRmMidStreamKillsChild` | 同上 | 真 CLI `session rm lab`：rc 74（`session_not_found_or_deleting` 归结果未知）；`$done` 不出现；agent.out 含 revoke 行。（tombstone 复活拒绝在 `internal/agent` 测试，经 broker 的 resume 一律被 session 检查拒绝，不能作为 tombstone 证据） | 不处理 session_destroyed → `$done` 出现红 |
| `TestCtlConnectRetriesThroughCalloutBlackWindow` / `TestCtlConnectDoesNotRetryUnreceiptedIdentity` | 同上（callout 模式） | 真 `login` 后 `stopBroker()`，起 `exec agt -- true`，1.5s 后 `startBroker()`：rc 0、总耗时 ≤ 8s、stderr ≥1 行 `retrying in`；负控制：fresh home（有 keys、无收据）同黑窗：恰一次尝试（≈2s）、0 行 `retrying`、class ∈ {69,77} 按机制 | Budget=0 → 正臂红；`HasAuthReceipt` 恒真 → 负臂红 |
| hermetic `tests/prev-supply-test.sh` | `test/simcluster/tests/` | mktemp git 仓：tags v0.5.0<v0.6.0<HEAD → `sim_prev_tag`==v0.6.0；HEAD 在 v0.6.0 上 → v0.5.0；伪造 `tether-prev-source.json` → `sim_prev_source_ok` 非 0；进 `run-all.sh` 循环 | 单分支 `git describe` → HEAD 在 tag 上臂红 |
| probe selftest 扩例 | `drills/lib/hot_update_process_probe.py` + `tests/hot-update-process-probe-test.py` | `carrier` 正例 + 负例（agent MainPID 变／fault 窗零推进／`reliable_lines=0`）；`legacy_carrier` 正例（干净前缀 / 断裂）+ 负例（完整 transcript 不匹配）；34 新签名正例 + 旧 `{finished:false}` 形状负例 + rc 74 且输出完整负例 | 去 `reliable_lines==1` → 负例红 |

**变异纪律**：每条新守卫注入它声称能抓的缺陷并确认变红（`go test -overlay` 只改编译输入；扫源码的门真改磁盘再还原）；变异日志进 evidence。`-race -count=3` 只对表中标注的用例；e2e 用 `-race -count=1`。

---

## §5 部署层

### 5.1 `test/simcluster/lib/stage.sh`：`vendor/tether-prev` 供给

- `sim_prev_tag()`：HEAD 恰在 `v*` tag 上 → 取 `git tag --list 'v*' --sort=-v:refname` 中严格更老的下一个；否则 `git describe --tags --abbrev=0 --match 'v*' HEAD`。今天 = `v0.6.0`（`a3431a1`）。写死 tag 会在打 v0.7.0 当天让 N-1 退化成同版本。
- `sim_stage_prev()`（`sim_stage_binaries` 在 tether-next 之后调用）：缓存判据 = `vendor/tether-prev` + `vendor/tether-prev-source.json` 的 `{tag, commit, binary_sha256}` 三者一致；否则 `wt=$(mktemp -d); git worktree add --detach "$wt" "$sha"` → `( cd "$wt" && CGO_ENABLED=0 GOOS=linux GOARCH=amd64 make build VERSION="$tag" )`（用该 tag 自己的 Makefile/go.mod，`toolchain` 行由其 go.mod 钉住，`GOTOOLCHAIN=auto`）→ `cp bin/tether "$SIM_VENDOR/tether-prev"` → 记 `go version` → `git worktree remove --force`；写 `tether-prev-source.json`：`{schema_version:1, component:"tether-prev", tag, commit, binary_sha256, goos, goarch, toolchain, built_at}`（与 `caddy-source.json` 同风格）；`sim_prev_source_ok tag sha` 供 drill die-gate；**构建失败 fail-closed**（绝不用 tether-next 冒充 prev）。
- `sim_stage_binaries` 末尾写 `vendor/tether-source.json`：`{head, dirty_files, tether_sha256, tether_next_sha256, tether_prev_tag, tether_prev_sha256, go_version}`——evidence 的单源。
- tether-prev **不进 Dockerfile**（COPY 清单被镜像身份绑定），drill 用 `d cp` 送进容器。两张旧镜像被 `check_image_or_die` 拒绝，**首次 `./local.sh --build build` 是硬前置**。
- README §File map 加 `vendor/tether-prev`、`tether-prev-source.json`、`tests/prev-supply-test.sh`；`tests/run-all.sh` 的 `for t in …` 加 `prev-supply-test`（`simcluster_gate_set_test` 双向对账要求）。

### 5.2 drill `35-hot-update-exec-carrier.sh`（新）

manifest：`# arms: A B C` / `# fixture: A=N1 B=N1 C=N1` / `# grows: A=0 B=0 C=0` / `# worst: A=360 B=300 C=420` / `# forgoes: A=- B=- C=-`。共用 34 的供给（`up --brokers 1 --agents 1 --ctl 1; init brk1; session lab; agent-join agt1; _install_probe`），不做 upgrade、不起 artifact server。四条日志流只经 `drills/lib/logs.sh`。

公共骨架：evidence dir + nonce；`_launch_producer`（34 L90-108 形状，`tether exec --timeout 360s agt1 -- python3 -u probe producer --period 0.05 --lifetime 420`）；`assert_setup` 基线 ≥20 记录 + `before/ps.json` RUNNING；**非空性守卫** `_stream_advancing`（两次采样 live.stdout 记录数严格递增且 producer alive）才允许注入；注入后写 `carrier.json`（fault 种类、`t_fault_{start,end}_ns`、agent MainPID/ExecMainStartTimestamp 前后、ledger seq 前后、`exec_start_lines=$(sim_agent_slog_count agt1 "agent: exec.*$NONCE")`、`reliable_lines=…reliable=true…`）；恢复后 `_capture_post` 两次采样 source 推进 ≥20；`stop` → final；judge。

| 臂 | 注入 | oracle（`probe.py carrier`） | 校准（只记录） |
|---|---|---|---|
| **A** nats-server 重启 | A1 `sctl brk1 restart nats-server`（秒级）→ poll `is-active`/`_broker_up`/`_online agt1` → post1；A2 `sctl brk1 stop nats-server; sleep 25; sctl brk1 start nats-server`（**> redialAfter 20s，确定性触发 agent session 重建**，证据 = agent slog `NATS stuck-disconnected; rebuilding session`；broker 若 self-exit 由 `Restart=always` 拉起） | 原 child pid/starttime/nonce/ppid 不变；agent MainPID 与 ExecMainStartTimestamp 不变；两流 transcript == source；无 pipe_error；ctl rc 23；`before` RUNNING / `final` EXITED(23) 且 nonce 行恰 1；`exec_start_lines==1 && reliable_lines==1`；A1/A2 各 fault 窗内 ledger 推进 ≥20 | 恢复后 10 次 `tether exec agt1 -- true` 的 wall p50/max + 每次走的路径（reliable/legacy）→ `carrier.json.calibration` |
| **B** tether-broker 重启 | `sctl brk1 restart tether-broker`（数秒 + callout 黑窗）；`sim_broker_slog brk1 4000` 存证 | 同 A | B-new：restart 发出后立刻起的新 `exec agt1 -- true` 的 rc、路径、wall 时延**只记录**（exit #5 明文：新请求时延不是判据；连接级重试的证明在 hermetic exit #2） |
| **C** N-1（C1→C2 顺序） | DIE-GATE：`sim_prev_source_ok` + 容器内 `sha256sum == source.json` + `tether-prev version` 首行 == tag + `≠ sha256(vendor/tether)`。**C1**：`d cp vendor/tether-prev ctl1:/usr/local/bin/tether-prev`，v0.6.0 ctl 起 producer，基线 ≥20（互通），注入 A2 形状；**C2**：`systemctl stop tether-agent` → 用 tether-prev 覆盖 agt1 的 `$DST` → `start` → `_online agt1` 且 `_release_of agt1 == tag`；新 ctl `exec agt1 -- producer --lifetime 20`（无故障） | C1 judge `legacy_carrier`：`reliable_lines==0 && exec_start_lines==1`（走了 legacy）+ **不连续** = `observed != source` **或** exit 未送达（`finished:false`/rc≠23）——干净前缀（旧 conn 关闭后再无字节）与内部断裂都算；C2：rc 23 且 transcript 完整、`sim_agent_slog_grep agt1 'unknown forwarded verb.*process-resume'` ≥1（probe 到达旧 agent）、`agent: exec` 1 行 | C1 结果写 `legacy-outcome.json`（README/log 写明「N-1 不承诺连续」）；C2 记 2s probe 税 |

verdict：三臂 GREEN（臂 C 的 GREEN = 「走了 legacy + 无重复执行 + 不连续被如实记录」；INCOMPLETE 会永久阻塞 sweep，见 §10-Q5）。**负控制**：C1 本身即臂 A/B 正 oracle 的真实旧 binary 负控制（同一 fault 下 v0.6.0 ctl 的 transcript 必不满足 `carrier`）；probe selftest 的合成负例在 `run-all.sh` 里 hermetic 跑；非空性 `minimum=20`。

### 5.3 drill 34 重钉

- 机制（deploy/agent/broker 三方批评一致；ctl 草案的「Runtime.Close SIGKILL child」是错的）：`node upgrade` 走 `syscall.Exec`（upgrade.go:314），defer 不执行、`Runtime.Close` 从不运行；child 因读端 CLOEXEC 关闭而 EPIPE（`output=epipe`），新映像空表 → G.1 missed-exit(-1)（`management=exited_minus_one`）——**两段不动**。变的只有 ctl 半：ctl 走可靠路径，新 agent 的 runtime 是新 incarnation → `registry.validate` → `ErrInstance` → `process_instance_lost` → create 后 → **rc 74，几秒内退出**（不再等 360s）。
- `probe.py legacy()`：追加 `require(outcome == {"finished": True, "exit_code": 74}, "not the S1 shape: ctl must exit 74 (owner instance lost) promptly")`；签名 `H0_LEGACY_EXEC_INTERRUPTED output=epipe management=exited_minus_one ctl=owner_lost_rc74 pid=<pid> nonce=<nonce>`；`mutate_legacy` 同步；selftest 加旧 `{finished:false}` 与「rc 74 但输出完整」两个负例。
- `34-hot-update-process.sh:241-244` `assert_bug` regex 同步；`_continuity` 正 oracle 原样保留（S3 promote 为 `assert_ok`）。
- `expected-verdicts.tsv:33` 注记改「S1 re-pin：ctl 走可靠路径后 ctl=owner_lost_rc74；旧签名收据见 `docs/reviews/hot-update-process-baseline.md`（log §34 的 2026-09-20 段写的是 NOT_RUN，内审 D7 订正）；deploy 实跑待用户同意」；`expected-verdicts-log.md §34` **追加**一段（旧段不动）；`drill-costs.tsv` **补 34 行**（今天没有）。

### 5.4 五份基线与成本

- `expected-verdicts.tsv`：`35-hot-update-exec-carrier GREEN 0 - - 35-…` 父行 + `35….A/B/C` 子行；`expected-verdicts-log.md` 新 §35；`tests/kept-sites.baseline.tsv`、`tests/assert-identity.baseline.tsv` 由 `sh tests/kept-sites.sh` / `sh tests/assert-identity.sh` 重生成（只增，diff 后核对）；`drill-costs.tsv` 实跑后加 35.A/B/C；README §Drills 表加 35；coverage inventory 按 `sh tests/run-all.sh` 输出重生。
- **成本预告（实跑前向用户征同意）**：`./local.sh --build build` restage ≈3–5 min（首次含 v0.6.0 worktree 构建 ≈1 min）；`./run-drills.sh 35-hot-update-exec-carrier` 三臂并行 ≈7 min 墙钟（串行 ≈18）；`./local.sh drill 34-hot-update-process` ≈2 min。合计 ≈12–15 min；期间**不跑** go test/lint/gates、不留残留容器；evidence 记 `docker image inspect -f '{{.Id}}' tether-sim:dev`、`vendor/tether-source.json`、`vendor/tether-prev-source.json`、每臂 verdict 行与 rc。
- 已知部署层风险：A2 的 25s 停机可能让 tether-broker 撞 systemd StartLimit 进入 failed——那是部署层真缺陷（记 gotcha，SETUP-RED 呈现），**不得改 25s 凑绿**。

---

## §6 文档清单

| 文档 | 改动 |
|---|---|
| `docs/usage.md §9.13`（**先于代码**） | 74 行；重试规则 69/70/75 可重试 · 64/74/77 终态；§9.13.2 exec 可靠路径退出码（create 后除 77 一律 74、`ctl 中断=放弃`、无 ctl 侧 kill、8 MiB 背压、legacy fallback 2s 税与旧 agent 日志噪声、broker 重启窗内新请求不承诺可靠路径、被撤权 ctl 重连到 --timeout） |
| `docs/usage.md §5.12 exec` | 可靠语义（一次执行、断链续接、丢回复不重建、AttachEpoch 恒 1）、`--stdin`（若采纳）、信号 → 128+sig（原「变 128」句改写）、spawn 失败不再出现在 `ps -a`、`session rm` 后各 agent 杀该会话托管进程 |
| `docs/distributed-broker-architecture.md §22.4`（新） | 「可靠进程请求协议与准入」：handoff-protocol 9 条裁决搬成活契约（含转发顺序 §3.7、create audit 可多条）；raw overlay + actor_fp 变体剥离；probe 严格 ONLINE / attach 放行 STALE；Code 词表；StartedUnacked 与 G.1（N-1 四象限论证）；Revocation tombstone 与 freeze-删除竞争三条；abandoned-terminal 保留期（max(lease, 2 min) / 15 min）；连接级重试的收据前提与谓词；退出码 taxonomy |
| `docs/architecture.md §22.3` 末尾 | 「S0 仍退 70」订正为 S1 的 74/已知 setup 子码-class；补 OQ4 边界 |
| `docs/requirements.md §6.8` | OQ4 边界一句：ctl 进程退出/重启后按同一 handle 恢复不是本需求目标；句柄仅在 ctl 进程生命周期内保留 |
| `docs/deploy-tier-gotchas.md` | 新 #90（STALE 5s 窗把在途 exec 判 `node_offline`，S1 以 allowStaleNode 修；放行后真死 agent 最多多等 55s）；#89 族补注（ctl 无 IgnoreAuthErrorAbort 时黑窗内两次同错即永久 CLOSED） |
| `docs/testing-standards.md` + `test/README.md` | 真实二进制 e2e 层（realstack、`AlwaysEnableNonce` 的理由、**真二进制 ctl 不用 `TETHER_DEV_NO_AUTH`**、relay 只切 TCP 不伪造字节、Linux-only）；tether-prev 供给法；cli_e2e 行加 `TestReliableExecMatrix` |
| `test/simcluster/README.md` | Drills 表加 35、34 新签名；File map 加 tether-prev 三件；「tether-prev = 前一发布 tag 源码构建，tether-next 只证机制」 |
| `test/simcluster/expected-verdicts.tsv` + `-log.md` + `drill-costs.tsv` | §5.3/§5.4 |
| `docs/reviews/hot-update-evidence.md` | S1 段：命令/rc/时长/HEAD/`write-tree`/镜像 ID/vendor 三 SHA/v0.6.0 SHA/变异表/三本 golden numstat（wire +2 / test +N / matrix +1）/drill 原始 verdict 行/fuzz 收据 |
| `docs/reviews/hot-update-tasklist.md`、`INDEX.md` | 勾 C-5、B-5（只勾有收据项）；INDEX 一行（日期 / 结论 / `docs/reviews/hot-update-reliable-exec-plan.md`） |
| `cmd/tether/error_code_coverage_test.go` 两张账本注释 | §2.7 / §3.4 |

---

## §7 实施顺序（每步可独立过 `go test`）

1. **文档先行**：usage §9.13/§5.12、requirements §6.8 一句（不动代码）。
2. **wire + 常量**：`proto.ProcessResumeReply.Code`、`CodeProcess*`、`LocalProcess.StartedUnacked`；`-update-wire-inventory`（numstat `2 0`）；`go test ./internal/proto/`。
3. **procrequest / execsession**：`ErrInstance`、`BootNow`、`NewWithClock`；`workload.Manager.Lookup`；Started 后置 + errUnadopted + observe ErrUnknown + finish；`ErrClosed`；`revoke.go`（Revoke/tombstone/Sweep/ErrOutputDiscarded）；`go test -race -count=3 ./internal/procrequest ./internal/execsession ./internal/workload`。
4. **execclient**：`ProtocolError`、`CreateSent`/`Executed`、reconnect-aware `call`、Code-based probe/expired 判据；`go test -race -count=3 ./internal/execclient`。
5. **procforward + broker**：新包 + fuzz 种子；`handleProcessResumeReq` 重写 + `processResumeReject`；`allowStaleNode`；reconcile 两处 skip；账本条目；`go test -race -count=3 ./internal/procforward ./internal/broker -run '…'`；`make fuzz FUZZ='^FuzzParseStamp$' FUZZTIME=30s`；`FuzzPrivateProcessReply` 30s 收据。
6. **agent**：单一 started 站点（先 grep 断言失败 exec 出现在 ps 的测试）、`pendingStartedSnapshot` + 快照合并、`processRefusalCode` 复合字面量、`process_revoke.go` + sys.events 分支、`sweepProcessRuntime` 挂 heartbeat tick、Started 日志 hook；`go test -race -count=3 ./internal/agent -run '…'`。
7. **cmd/tether + cli**：`exitUnsettled`、`classifyReliableFailure`、error_hints +4 行、`auth_receipt.go`、`connectWithAuthRetry`、`IgnoreAuthErrorAbort`、（`--stdin`）；`go test -race ./cmd/tether ./internal/cli`；`TestStructuralBudget`；CLI golden（若 `--stdin`）。
8. **cli_e2e**：stackharness `authstack.go`/`binary.go` → realstack harness → relay → 15 条 e2e；`TestReliableExecMatrix` + `-update-matrix-inventory`；`-update-test-inventory`；逐条变异。
9. **闸门自检**：`go test ./test/architecture/ ./test/determinism/ ./test/e2e/parallel/`；`make gates`；Darwin + `make vet-tags`。
10. **simcluster**：stage.sh prev 供给 + `tests/prev-supply-test.sh` + probe.py 判官/selftest + drill 35 + 34 重钉 + 五份基线；`sh test/simcluster/tests/run-all.sh`。
11. **三硬闸**：`make test` / `make e2e-parallel` / `make lint`（各自取 rc）。
12. **实跑**（征得同意后，严格串行）：`./local.sh --build build` → `./run-drills.sh 35-…` → `./local.sh drill 34-…`；evidence；`git add -A`（不 commit/push）。

---

## §8 批评处置表

| 批评（lens · 严重度） | 处置 | 落点 |
|---|---|---|
| ctl · BLOCKER `result.PID` 判 create 方向错 | **采纳**：`Result.CreateSent` 在首次成功 PublishRequest 后置位 | §1.1/§1.2 |
| ctl/deploy · BLOCKER 连接重试谓词在生产 2s/2s 形状下永不触发；负控制断 77 错 | **采纳**：谓词含 net timeout/ErrTimeout/ErrNoServers/auth；负控制按机制断 {69,77} + 恰一次尝试；预算语义明写 | §1.5、§4 |
| ctl/broker/agent/deploy · BLOCKER STALE 窗让臂 A/B 与续接 e2e 结构上不可达 | **采纳** broker 草案 `processResumeAttachSpec{allowStaleNode}`；OFFLINE 仍拒；harness 用默认 5s/60s；e2e 加跨 STALE 非空性与 OFFLINE 负控制 | §2.4、§4 |
| ctl/broker/agent/deploy · BLOCKER `TETHER_DEV_NO_AUTH` 让可靠 e2e 静默退化 | **采纳** `AlwaysEnableNonce`/真 callout + 每条用例非空性断言 + 写进 testing-standards | §4.1 |
| ctl/broker/agent/deploy · MAJOR pre-create ctx 错误今天退 70 不是 75 | **采纳**：显式 `exitTransient`；测试期望改为目标值 | §1.1 |
| ctl · MAJOR after-create 协议拒绝留在 70 | **采纳** `Code` 字段 + 相位规则「create 后除 77 一律 74」 | §1.1、§2.1 |
| ctl/broker/agent/deploy · MAJOR exit #3 撤 member 的 agent 半无机制 | **采纳解读 (a)+(b)**，交 §10-Q2 裁定；不做盖章转发 | §2.6 |
| ctl/agent/deploy · MAJOR「短写」在真二进制不可达；lost-reply 不能移出 e2e | **采纳**：SIGPIPE→141 诚实记录 + 单测守 ErrShortWrite；relay 注入 lost-reply | §0 #1、§4 |
| ctl/broker · MAJOR「broker 审计恰一条 create」是错 oracle | **采纳**：oracle 改 agent 侧证据，audit 只记录；写进活契约 | §2.3、§5.2 |
| ctl/agent/deploy · MAJOR ctl 无 IgnoreAuthErrorAbort；broker 草案臂 B 理由错位 | **采纳**：加进 `ConnectNATSWithNkey`；归因臂 A/黑窗；hermetic 变异需客户端 Timeout > auth_timeout | §1.5、§4 |
| ctl · MAJOR `--timeout` 文案不实（running 不回收、8 MiB 背压） | **采纳**：文案与 usage 写实；running-abandoned 明列 S2 | §1.1、§3.6 |
| ctl/deploy · MAJOR agent 草案的客户端瞬时集含 node_offline 全量 | **采纳**：不做客户端码级重试；STALE 放行在 broker，OFFLINE 立即 74 | §2.4 |
| ctl · MAJOR cli_e2e 矩阵单元被 `-run` 过滤 | **采纳** `TestReliableExecMatrix` | §4.1 |
| ctl/broker/deploy · MAJOR `--stdin` 上限超 1 MiB 信封 | **采纳** 512 KiB + connect 前拒绝 + marshal 单测；交 Q1 | §1.4 |
| broker/agent/deploy · MAJOR C-5「starting 记录进联合快照」两种单边形式都更糟 | **采纳** additive `StartedUnacked` + 两处 orphan 循环跳过；plan 明写这是对 stages-plan 该句的改读 | §2.5、§3.2 |
| broker · MAJOR enqueueStarted 放 adopt 之后重开窗口 | **采纳**：Start → Identity → enqueueStarted → AdoptDraining；快照并入 pending started | §3.1、§3.2 |
| agent · MAJOR reconcile 只改 live 路径 | **采纳**：分类器与 live 两处 + 等价性表 | §2.5 |
| agent/deploy · MAJOR agent 草案 74 进表撞 `NeverReserved`；`unresolvedCodeSites` 键无效 | **采纳**：74 只呈现层；agent 回复改复合字面量以成为可登记站点 | §1.1、§3.4 |
| ctl/agent/deploy · MAJOR drill 34「Close SIGKILL child」机制错 | **采纳** deploy 描述（syscall.Exec、EPIPE、-1 不变），只改 ctl 半 | §5.3 |
| deploy · MAJOR B-new 作硬判据违反 exit #5 | **采纳**：只记录 | §5.2 |
| deploy · MAJOR broker 重启窗新请求静默降级 | **部分采纳**：保持 2s probe 降级（不引入 announce 依赖），但每个探针断言/记录路径、usage 明写 | §2.8 |
| deploy · MAJOR session_destroyed → cancelRun 是车队可见语义变化 | **采纳**：S1 不 cancelRun，交 §10-Q3 | §3.5 |
| broker · MINOR op switch 先于 ACL 重开 §3.7 顺序 | **采纳**：只用 op 选 spec，ACL 先于 version/op 拒绝，加顺序守卫测试 | §2.3 |
| ctl/agent · MINOR error_hints.go 不 import cobra，settledExecError 放错文件 | **采纳**：全部分类逻辑进 exec.go；error_hints 只 +4 行 | §1.6 |
| ctl · MINOR `TestProcessResumeErrorPreservesCallID` 重名 | **采纳**：作既有测试扩臂 | §4 |
| ctl · MINOR AttachEpoch 变异描述错（registry 接受更高 epoch） | **采纳**：变异说明改为请求侧断言 | §4 |
| ctl · MINOR settledExecError 把 failStart 说成「已跑」 | **采纳** `Executed` 分流 | §1.1 |
| ctl · MINOR raw overlay 不剥大小写变体、「逐字节」断言不成立 | **采纳** procforward `Stamp` EqualFold + map 语义比较 | §2.2 |
| ctl · MINOR reconnecting 期间重发堆积 | **采纳** `IsReconnecting` 不重发 | §1.2 |
| broker · MINOR 转发到无人订阅 subject 不产生 503 | **采纳**：文档与测试按 attempt 超时重试描述 | §2.4 |
| broker · MINOR DisconnectClientByID 注入不可观测；receipt 键 current_session 在 login 时为空 | **采纳** relay；receipt 由 nats.Name 解析 sid，login 成功后写 | §1.5、§4.1 |
| broker · MINOR C1 oracle「必有内部断裂」在干净前缀下误红 | **采纳**：不连续 = observed≠source 或 exit 未送达 | §5.2 |
| broker · MINOR hermetic 手段只触发 reconnect 不是 session 重建 | **采纳**：拆三层（cli_e2e reconnect / internal/agent fireRedial / drill A2） | §4、§5.2 |
| agent · MINOR drainStarted 5s 拖慢 race×3 | **采纳**：测试缩短 `courierRequestTimeout` | §4 |
| agent · MINOR 旧 agent 日志噪声 | **采纳**：usage/gotcha 记；C2 计数 ≥1 | §2.8 |
| agent · MINOR 经 broker 的 resume 不是 tombstone 证据 | **采纳**：tombstone 复活拒绝在 internal/agent 层测 | §4 |
| deploy · MINOR IgnoreAuthErrorAbort 改变 `run` 行为 | **采纳**：usage §5 写明 | §1.5 |
| deploy · MINOR abandoned 只在 create 时回收 | **采纳**：heartbeat tick 也 sweep | §3.6 |
| deploy · MINOR receipt 只由 login 写导致存量 home 不受益 | **采纳**：任何已激活成功连接都写 | §1.5 |
| 四份草案 keep：74 taxonomy、先文档后码、Code 字段、procforward、allowStaleNode、StartedUnacked、Started 后置、ErrInstance、abandoned tombstone、sim_prev_tag、relay、reliable=true 日志行 | 全部保留 | 各节 |

---

## §9 残余风险与带入后续 stage 的项

1. **running-abandoned 未封顶**（C-6 的 S2 半）：ctl 放弃后无人 ACK 的 running 任务在 8 MiB 后卡住 child，且没有 ctl 侧 exec kill 动词；S1 只在 usage/文案写实。→ S2。
2. **`Executed` 是客户端派生**（不上 wire）：未收到任何字节且 child 在 running 状态回复前就被 kill 的极端形状会被判「未执行」→ 走既有分类（可能可重试）。窗口极小；S2 若导出 task 状态可改为 agent 权威。
3. **allowStaleNode 的 55s 代价**：真死 agent 的 attach 请求在 STALE 窗内按 500ms attempt 重发直到 OFFLINE；gotcha #90。
4. **`IgnoreAuthErrorAbort` 全局化**：被撤权的 `run`/`exec` ctl 重连到 `--timeout`/Ctrl-C；撤权仍由 admitACL 每 op 收口。
5. **连接级重试给 authed 身份的失败命令加最多 ~7s**（预算 5s + 单次 2s）：滚动重启窗外的持久错凭据（用户改了 PIN）也白等。
6. **broker 重启窗内新 `exec` 静默降级 legacy**（2s probe）：只能记录路径；S6 broker 映像交接落地后由 S6 的 ≤5s 接纳预算覆盖。
7. **spawn 失败的 exec 不再出现在 `ps -a`**：用户可见变化；存量测试若断言需改（先 grep）。
8. **StartedUnacked 只关「无行」分支**；ACK 丢失且 broker 已有行的窗口靠 accepted 臂——对新 broker 完全安全，对 v0.6.0 broker 等于今天。
9. **e2e 新 flake 面**：真 agent 子进程 + callout 计时（2s auth、2s ReconnectWait、5s 心跳）在满 worker 下；每条用例只用 `WaitFor` 轮询与 T1 环境倍率；定位用 `make e2e-one T=TestReliableExecMatrix`。
10. **A2 的 25s 停机可能撞 systemd StartLimit**：部署层真缺陷，SETUP-RED + gotcha，不改 fixture 凑绿。
11. **freeze 与撤权竞争**只定义未实现（S2/S3）；tombstone/revocation 随 image 导出属 S2 schema。
12. **`--stdin` 若不采纳**，exit #1 的 48KiB 一项只能在 harness 直发层覆盖，evidence 注明。
13. **broker 代码行余量 ≈600**：本 stage 后 broker 不得再放别的逻辑；S6 需先搬职责。
14. **drill 35 三臂 GREEN 依赖本机 weilandserver 的 systemd/容器时序**；首跑记 drill-costs 后再评估 worst。

---

## §10 需主进程裁定的问题（≤5，附推荐）

| # | 问题 | 推荐 |
|---|---|---|
| Q1 | `tether exec` 是否新增 `--stdin`（CLI golden 变更；上限 512 KiB、超限 64、legacy/可靠两路同源）？否则 exit #1「48KiB stdin」只能由 harness 直发 `ExecReq.Stdin` 覆盖。 | **加**。今天没有任何 CLI 入口填 `ExecReq.Stdin`，e2e 的字面要求不可达；opt-in 避免 `while read … tether exec` 脚本地雷；golden 更新在 commit message 写理由。 |
| Q2 | exit #3「撤 member 中途 → agent task SIGKILL 并有 terminal」的解读：(a) member 撤销 = ctl 77 不降级、任务跑完由 abandoned 保留期回收 + (b) SIGKILL/tombstone 由 `session_destroyed` 承担；还是采纳 agent 草案的 broker 盖章 `revoked` 无 Reply 转发（wire +1、反转 C.1 pre-forward 模型、改 617 守卫）？ | **(a)+(b)**，并在本 plan §0 与 evidence 里明写对 exit #3 的改读。产品无 member 移除动词、无 broker→agent 撤权通道，新造通道超出「新 verb 家族只 process-resume」的精神且是机会主义的（被撤成员不再发请求就永远不杀）。 |
| Q3 | `session_destroyed` 的 agent 处置：只 `Revoke`（reliable）+ `reapManagedChildren`（legacy exec + PTY），**不** `cancelRun`；还是像 evict 一样退出 daemon？ | **不退出**。退出是车队可见语义变化（systemd cgroup 随 daemon 退出杀光 unit 内全部负载——weilandserver 的实验负载就在里面；今天同一结果要到下一次 register 拒绝才发生）；S1 只做 exit #4 要求的清场 + tombstone，daemon 退出留给既有 register 拒绝路径。若主进程选退出，需改 p7 存量测试并记 gotcha。 |
| Q4 | 74 的覆盖范围：create 送达后除 77 外**一律** 74——含 `session_not_found_or_deleting`（今天 64）、`node_offline: status=OFFLINE`（今天 64）、Ctrl-C（`context.Canceled`）。接受？ | **接受**。这些都是「任务已被接纳、结果未知」，64「人要改参数」与 130 都会让自动化误判；`ExitError.Code` 保留 wire 码供脚本分支；usage §9.13.2 逐码列出。 |
| Q5 | drill 35 臂 C 的判词：GREEN（只断言走了 legacy + 无重复执行，不连续如实记录）还是 INCOMPLETE/not_covered 表达「N-1 不承诺连续」？以及是否保留 C2（新 ctl × v0.6.0 agent）子段（多 ≈2 min）。 | **GREEN + 保留 C2**。「N-1 不承诺连续」是 requirements 口径不是覆盖缺口，INCOMPLETE 会永久阻塞 sweep；C2 是现网 8 台 v0.6.0 agent 升级前每条 exec 都要走的真实象限，且是 `unknown forwarded verb` 日志噪声与 2s 税的唯一真实收据。 |