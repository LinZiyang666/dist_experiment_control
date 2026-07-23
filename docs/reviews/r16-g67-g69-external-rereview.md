Fail

# R16 + G67 + G69 独立外部复审

日期：2026-07-23

基线：`HEAD=b602fc7`

前次结论：`docs/reviews/r16-g67-g69-external-review.md` — Fail

复审边界：开发者回复是在原 79 个已暂存候选/审查文件之上新增的 9 个未暂存修改，
约 `+149/-33`。改动集中在 F1、F2 及 drill 96，没有修改 F3–F9 对应的产品代码。
本复审以有效工作区（HEAD + index + 未暂存回复）为准；新增注释中“已修复”的陈述和
开发者绿测均不直接作为证据。

## 结论

仍不建议上线。

开发者正确关闭了 F1 的“terminal 尚未提交就删除 ledger”窗口，也正确关闭了 F2 的
生产 YAML 低年龄逃逸：原 F1/F2 独立反例均已转绿。但 F1 的 callback 方案只把不可
消除的崩溃窗口移到了提交之后：真实 terminal 已经提交、进程在 ledger unlink callback
前退出时，重启 finalizer 会再提交一条内容不同的 `home_broker_restart` terminal。
当前去重键哈希完整记录，两条 terminal 不会折叠。新的独立反例稳定复现该矛盾审计。

F3–F5 的 5 个 Major 反例没有产品响应，仍全部红；F6–F9 也未修。F2 对 drill 96 的
配套修改不完整：脚本开头预记“15 分钟内不可观察”的 gap，后半段仍按已加载 5 秒
年龄下限判 product red/claim FIXED，expected-verdict 和 gotcha 文档也继续陈述旧事实。

## 前次 finding 处置

### F1 — Blocker — 部分修复，但 post-commit 崩溃会制造互相矛盾的 terminal

已确认修复的部分：

- watchdog、`ev.transfer`、prepare cleanup、pull finalize 均改走
  `emitTerminalTransferAudit`；
- cluster sink 仅在 `forward(payload)` 返回 nil 后调用 `onCommitted`
  （`internal/broker/transfer_audit_forward.go:52-61`）；
- 原独立测试
  `TestExternalReviewTerminalLedgerSurvivesUntilAuditCommit` 已转绿；
- 开发者 focused tests 与 race 均通过。

剩余的对称崩溃窗口：

1. 真实 `complete`/`failed` 已由 leader durable commit；
2. `forward` 返回 nil；
3. 进程在 `onCommitted()` unlink ledger 前 SIGKILL；
4. 重启后 tracker 不存在，start-only ledger 仍存在；
5. `finalizeStrandedXfers` 无法知道真实 terminal 已提交，合成并提交
   `failed/home_broker_restart`（`internal/broker/xfer_inflight.go:176-195`）。

真实 terminal 与 synthetic terminal 的 Kind/Code/Error/Ts/Duration 不同，而
`TransferRecordReqID` 哈希完整 normalized record
（`internal/xferaudit/plan.go:35-45`），因此所谓 deterministic dedup 只能折叠重复
synthetic，不能折叠“真实 complete/failed + synthetic failed”。审计最终会同时声称
一次 transfer 正常结束/原因为 forward failure，又因 broker restart 失败。

新增独立测试
`internal/broker/r16_g67_g69_external_rereview_test.go::
TestExternalRereviewCommittedTerminalCrashDoesNotCreateContradiction` 模拟 commit 成功但
callback 未执行，重启后稳定得到两条不同 terminal。

建议：在 forward 前把**完整、确定的 terminal record/reqID**持久写入 ledger/outbox；
重启只重放同一 record，使 post-commit 重放命中同一 ReqID。仅保存 start 字段后再猜一个
synthetic terminal 无法同时关闭 commit 前后两个窗口。该持久状态还必须按 F6 做 fsync。

### F2 — 原 Major 产品安全问题已解决；配套证据仍有 Medium 缺陷

产品配置修复有效：

- `MinXferCrossHomeReapAge = 15m`；
- YAML 只允许 unset 或 `15m..24h`，5s/8s/14m59s 均拒绝；
- broker 测试钉住 serveconf 常量等于派生的 `3 × tier-B timeout`；
- 原独立测试 `TestExternalReviewRejectsUnsafeCrossHomeReapAge` 已转绿；
- focused config/GC tests 与 race 通过。

因此“生产配置可在另一 home transfer 活跃时提前删除对象”的原 F2 已关闭。

但证据与文档没有同步闭合：

- `test/simcluster/drills/96-mid-flight-chaos.sh:343-344` 无条件预记该臂不可在 15 分钟
  窗口内观察；
- 同脚本 `477-482` 后续仍可能 claim `#58 FIXED`，或在未观察到 reap 时记 product red；
  product-red 文本仍明确声称 setup 加载了 `xfer_cross_home_reap_age=5s`；
- `test/simcluster/expected-verdicts.tsv` 仍声称两个 #58 knob 均被压缩并成功加载；
- `docs/deploy-tier-gotchas.md` 与 `docs/broker-ops.md` 尾部仍把该生产字段描述成供 drill
  压缩年龄排程的 seam，和“只能调高”自相矛盾。

独立 shell 检查
`test/simcluster/tests/r16-g67-g69-external-rereview.sh` 对上述两项 stale oracle 稳定红。
drill 应在宣布该 cross-home arm structurally uncovered 后跳过其旧 judge，或把运行窗口
延长到安全下限之后；不能同时预记 gap 又用不可能发生的 5 秒 reap 判 product regression。

### 新 E1 — Medium — drill 96 在启动 recovery finalizer 前就宣判 #57 product red

本次 deploy-tier 运行成功构造了此前一直欠缺的中断：start 可见后 brk2 被 kill，390 秒内
没有 terminal。脚本随即在 `test/simcluster/drills/96-mid-flight-chaos.sh:406-408` 宣布
“dangling start forever” product red；但 R16 的产品方案名称和实现都是
**finalize-on-recovery**，而脚本直到 `432-435` 才首次重启 brk2。也就是说该 product-red
判定发生时 recovery finalizer 根本没有机会运行。

这次运行因而证明的是“死掉且尚未恢复的 home 不会凭空执行本地 finalizer”，不是
“R16 finalizer 在 recovery 后失败”。脚本随后把 brk2 重启用于 #58，却没有在重启后
重新等待/检查该 transfer 是否获得 synthetic terminal。

独立 shell 检查现同时验证 #57 product-red 行位于 home restart 之前。建议把 #57 judge
移到 brk2 恢复、broker active、timeout+slack/finalizer pass 之后；恢复前只能记录
precondition，不得宣称产品回归。

### F3 — Major — 未修：Offline/corpse assignment 仍被当作当前 placement proof

`AssignedReplicas` 仍为 `1 + len(Cluster.Replicas)`，明确计入 Offline、nil、退役旧 peer
（`internal/jsstream/replicas.go:73-97`）。代码注释仍承认
`3→2→3` 可在第一 tick 由 corpse 满足 target，却继续称其 sufficient。

`TestExternalReviewOfflineAssignmentDoesNotProveCurrentPlacement` 仍红。G69 仍不能兑现
“cluster add rc=0 时当前 JS meta 能创建 Replicas:N 新资产”的契约。

### F4 — Major — 未修：JS store 根错误和 symlink 仍 fail-open

`JSStoreHasData` 对根 `os.Stat` 的所有错误返回 false；`Stat` 接受目录 symlink，
`WalkDir` 又不遍历 symlink target（`internal/natsconf/js_store.go:18-42`）。

以下反例仍红：

- `TestExternalReviewJSStoreRootStatErrorFailsClosed`
- `TestExternalReviewJSStoreSymlinkDoesNotHideData`

### F5 — Major — 未修：旧 backup/sentinel 仍能跳过后来产生的数据

`MoveAsideJSStore` 仍在检查当前 store 前采信 backup/sentinel
（`internal/natsconf/js_store.go:65-80`）；standalone backup 名仍只有秒级精度
（`cmd/tether/cluster_natsconf.go:237-268`）。

以下反例仍红：

- `TestExternalReviewStaleSentinelCannotDisarmADataBearingReset`
- `TestExternalReviewStandaloneResetBackupNamesDoNotCollide`

### F6 — Medium — 未修：ledger 仍不具备所声称的 crash durability

ledger 写仍为 Write/Close/Rename，没有 file sync 或 parent-dir sync；unlink 也不 sync
（`internal/broker/xfer_inflight.go:70-120`）。读取仍只做 JSON decode、不校验必需字段
（同文件 `232-241`）；`.corrupt` 目标已存在时仍不解决原坏文件却声称 moved aside。

### F7 — Medium — 未修：drill 41 precondition 仍永久空过

`test/simcluster/drills/41-shrink-to-standalone.sh:203-204` 仍在 child shell 执行未 export 的
`_no_cluster_block`，前置 `!` 把 command-not-found 变成成功。原独立 shell 检查仍红。

### F8 — Low — 未修：10023 仍未结构化分类

G67 计划中的 transient API code 10023 仍未进入
`internal/jsstream/transient.go:21-30,86-91` 的结构化集合，继续依赖英文
`"insufficient"` 文本兜底。

### F9 — Low — 未修：候选仍未通过 diff whitespace gate

`git diff --cached --check` 仍报告
`test/simcluster/drills/67-transient-js-refusal.sh:200,203,206` 三处行尾空白。
开发者本轮 9 文件的 unstaged diff 自身通过 diff check，但有效候选整体仍红。

## 独立验证

已完成：

- 原 F1 pre-commit 反例：通过；
- 原 F2 unsafe-5s 反例：通过；
- 新 F1 post-commit 反例：失败，稳定生成真实 terminal + synthetic terminal；
- F3–F5 五个 Go 反例：全部仍失败；
- F7 与新版 drill-96 consistency 两个 shell 检查：失败；
- 排除 6 个明确审查红测后的 `go test ./...`：全包通过；
- F1/F2 focused product tests：通过；
- F1/F2 affected-package `-race`：通过；
- `go vet ./...`：通过；
- `make lint`：通过，0 issues；
- simcluster hermetic gates：全部通过；
- `git diff --check`：有效候选失败，见 F9。

tagged E2E 使用 `GOFLAGS=-skip=...` 将 6 个明确审查红测同时从外层和子进程排除：

- P1–P10、transfer defaults、proxy dial、D1–D9、PhaseFluidity、RemoteFS、
  ProxyTunnelReconnect 全部通过；
- P13 首次失败于 tunnel drop 后 `proxy_ready` 15 秒内未 clear；
- 该单项随后 3/3 通过，完整 p13 package 单次通过，但 package `-count=3` 又在另一条
  `DisableDuringTunnelDropStaysDown` no-resurrect 断言失败。开发者本轮没有修改 p13 调用图，
  因此这更像既有 timing flake，但 release gate 客观上不是全绿，不能记录为 Pass。

drill 96 与最终 unfiltered `make test` 结果见收尾记录。

## 疑惑与建议

1. F1 的目标是“没有 dangling start”，还是“每个 transfer 有唯一、真实的 terminal”？
   callback 方案满足前者的一个窗口，却可破坏后者。审计系统应明确唯一 terminal 不变量。
2. 如果 #58 deploy-tier arm 已结构性无法观察，为什么仍在同次运行后半段对它给
   FIXED/product-red 判定？应选择“延长到 15m 后测”或“诚实 skip”，不能两套 oracle 并存。
3. F3 的源码已经把反例写进注释却仍声明 sufficient。建议先统一契约，再选择 empty
   canary/current-membership 方案；“比什么都不测好”不是 rc=0 的充分条件。
4. 修复顺序仍应为：F1 durable terminal outbox → F3 current placement proof →
   F4/F5 fail-closed reset state machine → F6 durability → drill/docs/低优先级清理。

在 F1 Blocker 和 F3–F5 Major 全部转绿之前，本复审维持 **Fail**。

---

# 主进程逐条回复（2026-07-23）

工作树已改，**index 未动一个字节**（暂存是审查者的工作）。所有回复只陈述可复核的事实；
审查者的反例测试**一条都没有删除或放宽断言**，只在 F4 处按你们自己建议的
`(hasData bool, err error)` 新签名适配了**调用形式**。

## F1 — Blocker — **采纳，已按你们的 outbox 方案重做**

你们指出我的 callback 只把窗口移到提交之后，是对的。原方案下：真实 terminal 已提交 → 进程在 unlink 前被杀
→ 重启看到 start-only ledger → finalizer **猜**一条内容不同的 `home_broker_restart`，而 reqID 哈希完整记录，
两条不折叠 ⇒ 审计自相矛盾。

**改法（你们建议的持久 outbox）**：`xferInflightRecord` 新增 `Terminal *schema.AuditTransfer`；
`emitTerminalTransferAudit` 在 **forward 之前**把**完整、已决定**的 terminal 落盘（fsync 文件 + 父目录），
再 forward，提交确认后才 unlink。恢复时**重放同一条记录**而非猜测。

并且回答你们的**疑惑 #1**——不变量我明确定为：**每次传输恰好一条真实终态**。理由：矛盾的终态对比悬空 start
更坏（悬空可被发现和补救，两条互斥终态污染审计本身、无消费者能判断哪条为真）。据此，恢复期在
**无法确证**提交状态时**丢弃而非重放**（`b.cl == nil` 或查询出错），并大声记录——生产上不会走到这一支。

**"检测已提交"已实现**：新增 `cluster.ReqIDCommitted` 查复制的 `cluster_reqid_ledger`
（保留 ~1M raft index ≈ 数月，远超 JS 那 2 分钟 Duplicates 窗口）。恢复前先查，已提交则**零发出**直接丢账。

**证据**：你们两个反例（pre-commit、post-commit）**均转绿，断言未改**。另补一条**用真 raft 节点**的正向证明
`TestStagedTerminalReplayDetectsPriorCommitOnARealLedger`——真实终态经 raft 提交后，恢复期确实检测到并零发出。
补这条是因为你们的桩绕开了 Apply 层去重，我不愿只靠"测试过了"结案。

## F2 — 产品已修（上轮）；**本轮补齐你们指出的证据/文档不一致**

你们说得对：我预记了 gap，后半段却仍按"已加载 5s"判定。已全部改齐：
drill 96 的 `#58` 臂**整段移除旧 judge**（不再可能 claim FIXED 或按不可能发生的 5s reap 记 product red），
只保留 CADENCE 压缩 + 一条无条件 `not_covered`；`docs/broker-ops.md` 标注**只能调高**。
你们的 `r16-g67-g69-external-rereview.sh` 一致性检查现在应为绿（请复核）。

## F3 — **采纳，已修**

`AssignedReplicas` 现在跳过 `nil` 与 **`Offline`** peer。注意我**没有**排除 `!Current`：已指派、仍在拷贝字节的
peer 是真实放置证据，要求追平会让每次 grow 等一次全量字节拷贝（events 上限 1 GiB）——那是设计上刻意避免的。
排除的恰好是你们指出的 corpse。`TestExternalReviewOfflineAssignmentDoesNotProveCurrentPlacement` 转绿。

**疑惑 #3 的契约**：按你们的建议取**强契约**（rc=0 ⇒ 当前 meta 能创建 `Replicas:N` 新资产）。
排除 Offline 消除了 corpse 假证；**空 canary 直接测量仍未做**，如实登记为剩余差距。

## F4 — **采纳，已修**

签名改为你们建议的 `(hasData bool, err error)`。只有 `IsNotExist` 算"无数据"，其余根错误 **fail closed**；
新增 symlink 解析（`Lstat` 判定 + `EvalSymlinks`）；entry `Info` 错误同样 fail closed。
调用方 `resetJoinerJSStore` 在无法判定时**报错而非跳过**。两个反例均转绿。

## F5 — **采纳，已修（并修正了我自己一条编码不安全行为的测试）**

分辨点是 **epoch 绑定**，正如你们所说：
- **backup 目录**名字里带 epoch ⇒ 是本次操作的物证 ⇒ 同 epoch 续跑仍 no-op（保留）；
- **sentinel 在、backup 已没** ⇒ **没有任何存活物证** ⇒ 只能豁免**当前为空**的 store；一旦重新变成
  data-bearing 就必须落回 ack 门。
- backup 名改为**纳秒 + 随机后缀**，消除秒级碰撞。

我原有的 `TestMoveAsideJSStore_SentinelPresentBackupGoneIsNoOp` **断言的正是不安全行为**（seed 数据后仍要求
no-op），已改为断言安全契约的**两半**。你们的两个反例转绿。

## F6 — **采纳，已修**

`writeLedgerRecord` 统一做 **fsync(临时文件) → rename → fsync(父目录)**；unlink 与 `.corrupt` rename 后同样
sync 目录。读取增加**必填字段校验**（`{}` 之类能解码但无效的记录改走 `.corrupt`）。`.corrupt` 目标已存在时
改用**带纳秒的唯一名**，且移动失败时**如实记录"未移走、下轮还会重读"**，不再谎称 moved aside。

## F7 — **采纳，已修（这条我认得最惭愧）**

`sh -c "! _no_cluster_block"` 里函数不在子 shell ⇒ command-not-found(127) ⇒ `!` 变 rc=0 ⇒ **永久空过**。
而这条守卫恰恰是我为回应内审"空过"问题而加的，我用另一种方式让它空过了。已改为直接把函数交给 `assert_ok`
（新增 `_still_clustered`）。

## F8 / F9 — **采纳，已修**

10023 已进入结构化码集合（不再依赖英文 `"insufficient"` 兜底）；drill 67 的三处行尾空白已清除。

## 仍未做（如实登记，供你复核判定）

1. **F3 的空 canary**：当前实现排除了 corpse，但**没有**用空 canary 直接测量"此刻能否创建 R=N 资产"，
   也未做 `3→2(不 peer-remove)→3` 的实机 differential。强契约的**完全**兑现仍欠这一步。
2. **#58 cross-home reap 的部署层观测**：F2 修复后结构上不可在 drill 窗口内观察（15m 下限），
   已记为 `not_covered` 而非开测试后门。
3. **restore 的 kill-9 实机证据**（你们疑惑 #4）仍欠。
