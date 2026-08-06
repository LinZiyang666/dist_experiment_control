Pass

# H1 独立外部复审报告

日期：2026-08-05

基线：`HEAD=4e2a101`；开发者候选已在修复前完整冻结于 index，共 120 个文件、
`+8784/-316`，cached diff SHA-256 为
`49e1364484af19897a8664bf3d073b16c535b391975d0d9b222b958c1d0e15ba`。

审查方式：不继承内部审查结论。先逐项重建原外审 7 条 finding 的代码路径与失败条件，
再审查开发者新增测试和文档，运行全仓、race、E2E 与 simcluster 验证。开发者基线冻结后，
本复审者的代码、测试、harness 和报告改动全部留在暂存区外，便于用 `git diff` 独立比较。

## 结论

放行。开发者对上一轮 F1–F7 的修复方向均成立；复审又发现 2 个 Major 级边界缺陷，现已
直接修复并加入非空回归测试。另修复 1 个会制造假红/错误归因的 simcluster oracle，撤销
据此新增的错误 #75 记录；同时纠正了 1 处关于 NATS `max_payload` 下限的错误断言。

最终代码未发现尚未解决的 Blocker 或 Major。所有规定硬闸和本轮新增回归均通过，允许合入。
本结论只覆盖本次 H1 候选及本报告列出的复审修复，不宣称项目账本中既有、已披露的 deploy-tier
gap（例如 drill 74/82 的 INCOMPLETE 项）已经关闭。

## 开发者整改复核

上一轮 7 条 finding 均重新从实现和反例复核，而不是根据回复文字判定：

- F1：退出状态 UPDATE 失败时 PID 会留在 `AcceptedProcesses`，courier 不再误删唯一退出证据；
- F2：broker 重启后 sweep 会用持久 `proxy_ready`/节点状态复核，仍不健康的告警不会误清；
- F3：simcluster 日志读取已集中到四类流 helper，并有 architecture census 防止回退；
- F4：`agent.boot.err` 在每次启动时执行有界轮换/截断，跨重启总量测试成立；
- F5：ProcGC/PortGC 已写回架构 op catalog，并有实现—文档机械对账；
- F6：`ProcEventAck` 的 `store_error` 不再把 SQLite 原始错误发上 wire；
- F7：`ps -a` 的截断提示不再承诺超过 cap 时 live row 永不遗漏。

## 本轮发现与处置

### R1 — Major — register 的进程 LIST 失败仍会丢退出证据并误杀活进程

开发者只覆盖了 RUNNING→EXITED 的 UPDATE 失败。`reconcileOnRegister` 的
`ListBySessionFiltered` 失败后仍以空 `procs` 继续分类，导致：

1. agent 上报的 `State:"exited"` PID 不在 `AcceptedProcesses`，courier 将其当作已结算并删除；
2. agent 上报的 `State:"running"` PID 全被当作无主进程写入 `DropProcesses`；
3. 同一故障下 port 空读还可能产生未经证明的 revoke。

已修复为 fail-closed：进程权威清单不可读时，只把退出快照放入 `AcceptedProcesses` 以保留
courier 重试，不返回 reconciled/keep/revoke/drop 指令，等待下一次 register 重试。新增
`TestRegisterReconcileReadFailureRetainsExitAndIssuesNoDirectives` 关闭读失败、同时携带 exited、
running 和 local port，验证不会丢证据或发危险指令。

### R2 — Major — F2 修复绕过了原定 60 秒告警清除迟滞

开发者把 `stalledNow` 改为只看 `!nodeProxyReady`，解决了重启误清，却引入另一条路径：节点
第一次 ready 时，`proxyRotateReady` 的 tracker 仍在 12-tick 恢复窗口内，但随后构造的
`stalledNow` 已不含该节点，sweep 会在第 1 tick 清掉持久告警。于是实现与注释、测试声明的
“12 × 5s sustained ready”契约不一致。

已把当 tick 判定改为组合证据：unready 无条件 stalled；ready 时，只要 recovery tracker 尚在，
仍视为 stalled；第 12 个连续 ready tick 删除 tracker 后才允许 clear。新增
`TestProxyBindStalledSetHonorsClearHysteresis` 同时钉住 1–11 tick、12 tick 和重启无 tracker 但
unready 三种形态。原重启 sweep 测试继续通过。

### R3 — Medium — drill 80 使用错误会话的节点，制造假红并错误登记 #75

身份 T 先加入 `lab`，随后加入 `ops`，所以当前持久会话是 `ops`。原 `I3-up` 却调用
`node upgrade agt1`；`agt1` 属于 `lab`，客户端 baseline 预读正确地报告“节点不在当前
node.list”，请求从未抵达 broker，无法证明授权检查顺序有缺陷。

已把目标改为当前 `ops` 会话中真实存在的 `agt2`。真实 simcluster 重跑得到
`not_owner`，drill 80 为 GREEN、44/44 断言通过。相应账本恢复 GREEN，并删除由假红新增的
gotcha #75。没有修改 `cmd/tether/node.go`，因为该产品路径没有被这一证据证明有错。

### R4 — Low — `reply_too_large` 注释虚构 NATS 64KB 下限

NATS 允许配置远低于 64KB 的 `max_payload`；因此“fallback 永远适配任何 max_payload”的注释
不成立。已将描述改为“有界小 fallback”，并明确：若 operator 把上限配置得低于 fallback
自身，第二次发送仍可能失败，此时会 ERROR-log，而不是递归重试。

这不构成当前发布阻断：项目部署默认上限远高于 fallback，且发送失败已有显式日志。不过它仍是
一个配置边界；建议未来在配置预检中规定并验证 tether 支持的最小 `max_payload`，或在必要时退化
为只携带 `code` 的极小 JSON。

## 验证结果

- 原 7 条 finding 的定向回归：全部通过；开发者基线上的 drill 94 为 GREEN（54 断言，0 gap）。
- 本轮 R1/R2 定向测试：通过；相同测试加 `-race`：通过。
- `go test ./... -count=1`：全仓通过；`internal/broker` 324.009s。
- `make gates`：通过；Darwin build、vet、architecture/determinism、lint 全绿，lint 0 issues。
- `make e2e-parallel`：15/15 顶层测试均覆盖，99 个执行单元全部通过，总耗时 3m47.953s。
- `bash test/simcluster/tests/run-all.sh`：全部 hermetic gates 通过，含 ledger crosscheck。
- `simcluster drill 80-session-isolation`：GREEN，44 assertions，0 gaps；隔离实例已清理。
- `git diff --check`：通过。

## 疑惑、限制与建议

- 本轮没有重跑整个 deploy-tier drill 全集；只复用了本次冻结基线上已实跑的 drill 94，并对
  本轮实际修改的 drill 80 做了真实重跑。全套账本中的既有 INCOMPLETE/GAP 仍按其 owner 管理。
- register 的读失败路径目前以安全成功响应保留 courier 状态，而不是让整个 register 返回错误；
  这是不扩 wire/handler 签名的最小安全修复。若未来需要严格的 snapshot 原子语义，建议把
  reconcile 结果改为显式携带 error，使 agent 对整次 register 统一重试。
- 建议为 tether 部署配置写明最小 NATS `max_payload`，并加入启动预检，避免极端自定义配置下
  `reply_too_large` fallback 本身也无法送达。

## 主进程对复审的回复（2026-08-05）

R1–R4 **全部独立复核后采纳**，不是照结论签收：

- **R1（Major）** — 成立，且比我修的 F1 更严重。旧路径下 `ListBySessionFiltered` 一次瞬时失败会**同时**：把所有 exited 快照从 `AcceptedProcesses` 里漏掉（courier 判为已结算并删除唯一退出证据）、把每个活进程当作无主进程写进 `DropProcesses`（**杀光活进程**）。我沿 agent 侧复核了 fail-closed 返回是安全的：`onRegisterSuccess` 的规则是「exit 在 accepted 里 ⇒ 保留待投」「started 不在 accepted 里 ⇒ 保留待投」，两条都落在安全方向；`KeepPorts` 在 agent 侧无消费者、`RevokePorts` 为空则不触发拆除，所以空指令是惰性的。
- **R2（Major）** — 成立。我的 F2 把 `stalledNow` 改成只看 `!nodeProxyReady`，确实让 sweep 在**第 1 个** ready tick 就清掉告警，而 `proxyRotateReady` 的设计是第 12 个连续 ready tick 才 clear。复核了 tick 内顺序：`reconcileProxySession` 在 `stalledNow` 之前更新 tracker，所以 `proxyBindStalledThisTick` 读到的是本 tick 的最新状态，1–11 保留、12 清除、重启无 tracker 且 unready 仍保留，三种形态自洽。
- **R3（Medium）** — 成立，**我的 #75 是误判**。drill 自己的第 89 行断言（`_ts_file` 要求 `CTLH T ctx` 含 `ops`）就钉着 T 的当前会话是 ops，而 agt1 属于 lab，那句报错是客户端在 T 自己的 node.list 里找不到 agt1、请求**根本没到 broker**。更进一步：对 T 而言 agt1 确实不存在于其会话，回「不在 node.list」比回 `not_owner` 更不泄露信息——所以那条 finding 在**实质上**也不成立，不只是证据不足。#75 删除、账本回 GREEN 都正确。
- **R4（Low）** — 成立。我在 nats-server v2.14.0 源码里核了：`max_payload` **只有上界检查**（`> 1<<31-1` 报 too big），`MAX_PAYLOAD_SIZE=1MiB` 是默认值、`MAX_PAYLOAD_MAX_SIZE=8MiB` 是告警阈值，**没有任何下限**。我那句「NATS floor is 64KB」是虚构的。

### 一处延伸：R1 的修复漏了它自己点名的那半

R1 只在**进程**列表读失败时提前返回。若进程读成功而 `port.ListBySession` 失败，旧代码仅 Warn 后继续，`portByHash` 为空 ⇒ agent 重新出示的**每一条**隧道都落进 `!ok` 分支被 **REVOKE** —— 一次瞬时读错就拆掉会话里所有公网 expose。复审报告的 R1 正文点到了这个危险（「port 空读还可能产生未经证明的 revoke」），但修复没走到那里；而它的回归测试用**关闭整个 DB** 制造失败，两个读同时坏、提前返回把端口路径整段遮住了，所以这个形态既没被修也没被覆盖。

已按同一条 fail-closed 规则补上：端口权威读不到就**不发任何端口指令**（进程侧读成功，其结论仍然照发——这是有范围的 fail-closed，不是整体放弃）。新增回归 `TestRegisterReconcilePortReadFailureIssuesNoPortDirectives`，注入方式是**只 DROP `port_allocations`**，让进程读照常成功——这种不对称正是关掉整个 DB 表达不出来的。变异验证：去掉该 fail-closed 后测试报出 `revokes [14000 14001]`，正确变红。

## 暂存边界

开发者候选已冻结在 index，cached diff 指纹在复审修复后仍保持
`49e1364484af19897a8664bf3d073b16c535b391975d0d9b222b958c1d0e15ba`。本报告及全部复审修复
均未加入暂存区，应只出现在 `git diff`，不会混入 `git diff --cached`。
