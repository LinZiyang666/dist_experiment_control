Fail

# R16 + G67 + G69 独立外部审查

日期：2026-07-22

基线：`HEAD=b602fc7`

审查边界：开始审查时，候选改动已经全部位于 index（71 个文件，约
`+6515/-325`），工作区没有用户所述的“暂存区外”实现改动。因此本报告审查的是
`git diff --cached` 相对 `HEAD` 的完整候选；内部 tasklist、内部 review、计划中的
“SHIPPED/FIXED”和历史绿测均只作为线索，不作为结论依据。

## 结论

不建议上线。候选有 1 个 Blocker、4 个 Major、2 个 Medium 和若干 Low/证据问题。
最严重的问题是 R16 #57 只在“重启后补写 synthetic terminal”路径上保证审计提交后
才删除账本；正常 complete/failed/watchdog/forward-failure 路径仍先启动异步审计，
随即删除唯一的本地恢复证据。进程在两者之间退出时，R16 要修复的 dangling-start
会原样复现。

此外，#58 的所谓 drill-only 压缩开关实际是普通生产 YAML，允许把 15 分钟安全年龄
降至 1 秒；G69 把已经退役且 Offline 的旧副本计为“当前可放置”证明；JetStream
store 探测及 move-aside 幂等证据都存在跳过新数据的路径。这些问题均有独立红测，
不是对内部审查文字的推测。

## Findings

### F1 — Blocker — 正常终态在 Raft 审计提交前删除 in-flight ledger

`emitTransferAudit` 在 cluster 模式调用异步 sink；sink 启动 goroutine 后立即返回
（`internal/broker/transfer.go:475-487`、
`internal/broker/transfer_audit_forward.go:42-69`）。但四类正常终态紧接着删除对象、
tracker 和 ledger：

- watchdog：`internal/broker/transfer.go:429-440`
- agent `ev.transfer`：`internal/broker/transfer.go:870-876`
- prepare/forward 失败 cleanup：`internal/broker/transfer.go:890-903`
- pull finalize：`internal/broker/transfer.go:1137-1143`

R16 新增的 recovery finalizer 本身采用了正确顺序：先同步
`commitSyntheticTransferTerminal`，确认提交后才删 ledger
（`internal/broker/xfer_inflight.go:185-196,203-225`）。同一不变量却没有应用到更常见
的终态路径。若 broker 在 ledger 删除后、异步 forward 提交前退出，重启扫描无证据
可读，审计中会永久留下 start 而无 terminal。

独立测试
`internal/broker/r16_g67_g69_external_review_test.go::
TestExternalReviewTerminalLedgerSurvivesUntilAuditCommit` 阻塞 audit forward，并证明
`cleanupEntry` 已提前删除 ledger。现有
`TestXferInflightTerminalDropsLedger` 用 no-op sink，反而把错误顺序固定成了绿测。

建议：所有终态统一走“确定性 terminal commit/ack → 删除对象与 ledger”的状态机；
若不能阻塞 NATS handler，应以 completion callback/持久 outbox 完成删除，而不是把
“goroutine 已创建”等同于“已提交”。该项修复并覆盖全部四条路径前不得发布。

### F2 — Major — `xfer_cross_home_reap_age` 可把生产 GC 调到小于活跃 transfer 窗口

代码默认值确实是 `3 × transferTimeoutTierB = 15m`
（`internal/broker/transfer.go:45-62`），但普通 serve YAML 接受任意 `1s..24h`
（`internal/serveconf/serveconf.go:221-241`），`cmd/tether/serve.go` 将其直接接入
broker，GC 也直接采用 override（`internal/broker/transfer_reconcile.go:179-185`）。
这与 `broker.Config` 注释中的“仅供 deploy-tier drill、production 无调参故事”
（`internal/broker/broker.go:227-231`）矛盾，且 `docs/broker-ops.md` 又把它公开给运维。

在 split-home 会话中，没有任一 home 拥有整个 bucket；leader 只排除自己的本地
tracker（`internal/broker/transfer_reconcile.go:90-101`），看不到另一 home 上仍活跃
的 tracker。因此配置为 drill 已使用的 5 秒后，leader 能在 tier-B 的 5 分钟 watchdog
之前删除另一 home 正在使用的对象。

独立测试
`internal/serveconf/r16_g67_g69_external_review_test.go::
TestExternalReviewRejectsUnsafeCrossHomeReapAge` 证明 5 秒配置被接受。

建议：不要把 drill 时间压缩 seam 暴露在生产配置；或者至少强制
`>= 3 × tier-B timeout`。压缩测试可直接构造 broker `Config`，不需要污染生产 YAML。

### F3 — Major — G69 用退役 Offline 副本错误证明“现在可创建新资产”

`AssignedReplicas` 无条件返回 `1 + len(Cluster.Replicas)`，nil、Offline、旧 assignment
全部计数（`internal/jsstream/replicas.go:73-97`）。代码注释甚至准确描述了失败序列：
3→2 retire 后不执行 JS peer-remove，旧 `{A,B,C-dead}` 仍在；随后 2→3 grow 第一 tick
就由 corpse 满足 target（同文件 `84-89`）。

这与上层契约“can the JS meta host a NEW asset at the CURRENT target replica factor”
（`internal/broker/clusterwrite.go:530-545`）及 `jsPlaceableFn` 对首个
`CreateObjectStore(Replicas:N)` 成功性的说明
（`internal/broker/clusteradmin.go:78-83`）直接矛盾。此时 gate 会返回 true，没有等待、
没有 degraded timeline，G67 所处理的首次 provisioning 窗口仍会静默暴露。

独立测试
`internal/jsstream/r16_g67_g69_external_review_test.go::
TestExternalReviewOfflineAssignmentDoesNotProveCurrentPlacement` 复现了 Offline peer 被计为
2/2 的 false proof。drill 67 的正向 oracle 只检查是否出现 “WITHOUT proving” timeline，
检测不到这种“错误地自称已证明”。

建议：用目标 replica 数创建一个空的、确定命名且可安全回收的 canary
stream/object-store，直接测量当前 meta placement；或至少排除 Offline/nil/stale peer，
并校验当前 meta membership。这里不需要等待历史数据 catch-up，空 canary 不会引入
1 GiB 拷贝门。

### F4 — Major — `JSStoreHasData` 在根路径错误和 symlink store 上 fail-open

函数注释承诺 walk error “fails CLOSED”，但根 `os.Stat` 的所有错误都返回 false
（`internal/natsconf/js_store.go:17-25`）。此外，`Stat` 会跟随正常目录 symlink，而
`WalkDir` 不会跟随根 symlink；因此一个指向真实 data-bearing store 的标准 symlink
也返回 false。returning joiner 会把这解释为无需 reset，继续携带旧 clustered meta
启动，重新引入 grow wedge。

独立测试：

- `TestExternalReviewJSStoreRootStatErrorFailsClosed`：self-loop symlink 触发 `ELOOP`；
- `TestExternalReviewJSStoreSymlinkDoesNotHideData`：symlink 目标含非空数据文件。

建议：接口改为 `(hasData bool, err error)`；只把 `IsNotExist` 当作无数据，其他根错误
必须 fail closed；明确选择 resolve symlink 或拒绝 symlink，并对 entry `Info` 错误同样
fail closed。

### F5 — Major — move-aside 的旧 backup/sentinel 能跳过后来重新产生的数据

`MoveAsideJSStore` 在读取当前 store 前，看到 backup 就直接成功；backup 不在而 sentinel
在也直接 no-op（`internal/natsconf/js_store.go:65-80`）。这些证据只能说明“过去某次
move 发生过”，不能说明当前 store 仍是那次操作重建的空目录。

两个实际失败窗口：

1. `reconcile nats --to-standalone` 的 backup 名只精确到秒
   （`cmd/tether/cluster_natsconf.go:237-245,267-268`）。首次 move 后 Apply 失败，运行中
   的 nats-server 可向重建目录写入；同一秒重试会命中旧 backup 并跳过新的 live data。
2. force-single/grow 的 backup 被运维移走后，旧 sentinel 仍可让后来重新产生的
   data-bearing store 永久免检。

独立测试
`TestExternalReviewStandaloneResetBackupNamesDoNotCollide` 和
`TestExternalReviewStaleSentinelCannotDisarmADataBearingReset` 分别钉住两条路径。

建议：幂等键绑定稳定 operation/epoch，同时在采信旧证据前验证当前 store 为空且属于
同一 epoch；backup 名使用 op ID 或纳秒/随机防碰撞。sentinel 绝不能覆盖“当前目录含
数据”的事实。

### F6 — Medium — 被称为 durable 的 ledger 没有文件或目录 fsync

`writeXferInflight` 是 `CreateTemp → Write → Close → Rename`，没有 `tmp.Sync()`，也没有
rename 后父目录 `Sync()`；删除同样没有目录 sync
（`internal/broker/xfer_inflight.go:70-120`）。进程退出之外的掉电/内核崩溃窗口不能
保证文件内容或目录项持久。写入错误又是 best-effort，JSON 解码后没有校验必需字段；
已有 `.corrupt` 时，新的坏文件不会被移走但日志仍声称 moved aside
（同文件 `160-165`）。

建议：若契约继续称为 durable，必须 sync 临时文件和包含目录，并对 record 的
transfer/session/verb/tier/time 做严格校验；失败应产生可观测的 degraded/alert，而不只是
日志。删除或 `.corrupt` rename 也应同步目录并处理目标冲突。

### F7 — Medium — drill 41 的关键前置 oracle 永远可“成功”

`test/simcluster/drills/41-shrink-to-standalone.sh:203-204` 执行
`sh -c "! _no_cluster_block"`。shell function 没有 export 到子 shell，实际是
command-not-found；前置 `!` 又把它变为 rc=0。因此“conf 仍 clustered”的防空过条件
不能证明任何事情。后续 guard 还能发现部分错误形态，但 drill 声称的因果隔离不成立。

独立 shell 测试 `test/simcluster/tests/r16-g67-g69-external-review.sh` 会精确检出该行并
失败。建议直接把 `_no_cluster_block` 作为 `assert_ok` 的函数参数调用。

### F8 — Low — G67 计划中的稳定错误码 10023 没有结构化分类

`g67-plan.md` 明确列出 10023（insufficient resources）为 transient；实现的结构化集合
包含 10040，却漏掉 10023（`internal/jsstream/transient.go:21-30,86-91`）。当前
nats-server 的固定英文描述会被后续 `"insufficient"` 文本规则碰巧捕获，所以不是当前
默认版本的立即故障，但它违背了“稳定 API code、描述中立也成立”的设计姿态。

建议加入 10023 常量和 neutral-description 测试，避免未来文案、本地化或 wrapper 改动
破坏分类。

### F9 — Low — 候选本身未通过 `git diff --check`

`test/simcluster/drills/67-transient-js-refusal.sh:200,203,206` 有行尾空白。功能影响低，
但发布前静态差异门当前为红，应清理。

## 疑惑与需要主进程明确的契约

1. `proto/messages.go` 将 `bucket_create_failed` 定义为永久、重试无效；CLI/usage 对部分
   未分类 provisioning failure 又给出 exit 70 和重试/上报建议。需要明确所有 permanent
   子类的稳定退出码，而不是只修 “store too small” 一支。
2. G69 到底承诺“比没有 gate 更好”，还是承诺 `cluster add rc=0` 后新建
   `Replicas:N` 资产可用？源码与计划在两种说法间切换。上线契约必须只有一个；按当前
   CLI 和 gotcha 目标，应以后者验收。
3. 如果 `xfer_cross_home_reap_age` 真是仅供 drill，为何它进入生产 YAML schema 和
   broker-ops？测试接缝和运维接口应分离。
4. restore 的 grow-ready snapshot、applied-index 归零、marker 随 snapshot 传递及
   marker clear 顺序，经源码追踪和既有/独立回归未发现新的确定性缺陷；但尚缺真实
   kill-9 发生在 snapshot install 与 marker clear 之间的 deploy-tier 证据。

## 独立验证

审查者添加的 7 个 Go 反例和 1 个 shell oracle 检查均在当前候选上稳定红，分别对应
F1–F5/F7。它们只添加测试，没有修改产品实现。

已完成：

- 候选原测试（排除审查者预期红测）：`go test ./...` 全包通过；
- `go vet ./...`：通过；
- `make lint`：通过（0 issues；默认 home cache 在沙箱只读产生非功能性 cache warning）；
- 受影响包 focused `go test -race`：broker、natsconf、clusteroffline、jsstream、
  serveconf、cmd/tether 全部通过；
- `test/simcluster/tests/run-all.sh`：原有 hermetic/ledger/non-vacuity 检查全部通过；
- `git diff --check`：失败，见 F9；
- `make test`：失败；失败项恰为本次 7 个审查红测，其他包通过；
- tagged E2E：`TestAllPhases`、D1、D2、D3 通过；D4/D5 子矩阵重新执行审查者红测而失败，
  失败内容分别是 F1/F3；PhaseFluidity、D6–D9、RemoteFS、ProxyTunnelReconnect 均通过，
  没有未定位的环境故障。

simcluster（重新构建当前源码镜像后、隔离实例）：

- `67-transient-js-refusal`：`INCOMPLETE rc=4`，18 pass、0 product/setup/assert red、
  1 个既有 face-B deploy coverage gap。实测 JS quorum 丢失时 tier-B push 在 2 次/5.2s
  后返回 rc=75、`jetstream_not_ready`，peer 恢复后同类 push 成功。该结果支持 G67
  retry/classification，但其 oracle 无法发现 F3 的 false-positive placement proof。
- `42-rejoin-returning`：`GREEN rc=0`，49 assertions、0 gap；覆盖 force-single
  `--reset-js`、resnapshot grow-ready、returning joiner move-aside、恢复后
  `cluster add` 到 SERVING 及真实 workload。
- `51-full-dr`：`INCOMPLETE rc=4`，72 pass、0 product/setup/assert red、2 个已披露
  coverage/design gap。N=3 全损毁、fresh-box restore、grow-ready N=1 启动、原 agent/expose
  自动恢复、再 grow 到 N=2 及真实数据面均通过；未覆盖项是亚秒 offline window 与
  state.db-only bundle 不恢复 JetStream 的既定 #53 scope。drill 的 step ledger 还明确
  记录 runbook 漏写了 restore 后必需的 `chown`（6 个文档步骤、实际 8 步、1 个未文档
  步骤），因此不能将 full-DR 宣称为 GREEN。
- 三个隔离实例均由 trap 清理；最终 `simcluster status` 无运行节点。

## 建议的修复顺序与验收门

1. 先修 F1，建立所有 terminal 共用的 commit-before-delete 原语；对 watchdog、
   `ev.transfer`、cleanup、pull finalize 各加 crash-window 测试。
2. 收回或硬限制 F2 的生产配置；用直接注入 Config 保留 drill 加速。
3. 用空 canary 重做 F3 的当前 placement 证明，并增加
   `3→2(retire without peer-remove)→3` 实机 differential。
4. 将 JS store 探测改为带 error 的 fail-closed API，重新设计 epoch-bound move-aside
   幂等与 fsync，覆盖 symlink、ELOOP、旧 sentinel、backup collision、Apply 失败重试。
5. 修复 drill 41 的 oracle、10023 分类和 whitespace 后，再运行全部 release gates。
6. 保留本次审查红测；产品修复应使其转绿，不应删除或放宽断言。

Blocker 和所有 Major 转绿之前，本审查维持 **Fail**。
