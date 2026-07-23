Pass

# R16 + G67 + G69 独立外部复审（第 6 轮）

日期：2026-07-23

基线：`HEAD=b602fc7`

前轮结论：`Fail`（`r16-g67-g69-external-rereview-round5.md`）

本轮开发者边界：叠加在已暂存 90 文件 round-5 候选之上的 6 个 tracked unstaged 文件，
约 `+481/-108`；初始 diff SHA-256：
`bbbcdfdaa860ca85e34349e7fb72ae81340497cc4e90da1c6083ee252cad94b3`。开发者修改
`internal/broker/{clusterwrite,xfer_inflight,xfer_inflight_test}.go`、
`internal/jsstream/{jsstream,placement_canary_ownership_test}.go`，并在 round-5 报告追加回复。
本轮审查阶段只新增独立测试、tasklist 和本报告，没有修改产品实现。

## 审查阶段结论

不能放行。开发者修复了 round-5 的直接反例：primary 仍是普通文件时会保留 outbox，
primary 持续不可读时会先重放 outbox，markerless/带 consumer 的 canary 流也会 fail closed。
但新实现仍有：

- 2 个 Blocker：双目录状态机在 outbox 不可读、以及 primary 父目录暂时不存在时，仍能为
  同一 transfer 提交真实 `complete` 与 synthetic `failed/home_broker_restart`；
- 1 个 Major：canary 把首次 lookup 的未知错误当作“流不存在”，随后借助幂等
  `CreateStream` 删除已有消息的 marked stream，并误报 placement 成功；
- 1 个 Medium：session test helper 只等待 NATS server ready，没有等待 broker
  subscription ready，完整日志 E2E 实际出现 `no responders` 非确定性失败。

三个产品问题均由独立、确定性测试稳定复现。完整 E2E 的 D4/D5 产品红灯可归因于这些
reviewer tests；另外捕获到的 session test 红灯也已取得具体测试名与同步缺口，不再是开发者
此前丢失身份的未知失败。

## Findings

### R6-F1 — Blocker — outbox 扫描失败被当成空 census，primary start 获得错误合成授权

`finalizeStrandedXfers` 先扫 outbox 并用 `outboxOwned` 阻止 primary start 合成
（`internal/broker/xfer_inflight.go:427-447`）。但是 outbox `ReadDir` 失败时 map 只是空，
随后 primary pass 仍在 `:449-497` 合成 `home_broker_restart`；只要 primary 可读，函数最终
还会在 `:498-514` 把这次 outbox 错误降为 warn 并返回 nil。

“无法读取 outbox”不是“outbox 中没有 exact terminal”。独立测试
`TestExternalRereviewUnreadableOutboxCannotAuthorizeSyntheticTerminal`
建立合法的 `primary=start / outbox=complete`，让 outbox 暂时不可读。第一轮稳定提交
synthetic failed；outbox 返回后第二轮再提交真实 complete：

> failed/home_broker_restart + complete

这直接违反 exactly-one-terminal。两个 source 独立推进是对的，但独立推进是非对称的：
primary 不可读时 exact outbox 可以安全重放；outbox census 未知时，primary terminal 可以
重放，primary start 却绝不能合成，因为高优先级 exact terminal 是否存在仍未知。

建议引入明确的 `outboxCensusKnown` 状态：outbox 扫描失败时仍允许处理 primary staged
terminal，但跳过所有 start-only synthesis，并返回/记录 degraded recovery；只有 census
成功才能授予 synthetic terminal 的生成权。

### R6-F2 — Blocker — child ENOENT 不是 primary row 已 durable 消失，cleanup 仍可删掉唯一 exact terminal

`consumeXferLedgerRow` 把
`os.Remove(<primary>/<row>)` 的任何 `IsNotExist` 当作“primary 已确认不存在”，继而删除
outbox（`internal/broker/xfer_inflight.go:250-275`）。但 ENOENT 也可能来自 primary 父目录/
挂载点暂时消失；该目录原有 start row 仍可在挂载恢复后重新出现。

独立测试
`TestExternalRereviewMissingPrimaryPathIsNotConfirmedLedgerAbsence`
在 fallback terminal 已写成、commit callback 执行前让 primary 路径消失。当前 callback
把 child ENOENT 当作确认，删除 exact outbox；primary 目录恢复后 finalizer 从幸存 start
合成第二条 terminal，稳定得到：

> complete + failed/home_broker_restart

这不是测试夹具特例：代码注释自己把 unmount/replacement 列为 fallback 要承受的故障。
“primary row 已不存在”的确认至少要求 parent directory 本身可打开/检查；parent 缺失、
`ENOTDIR`、权限/I/O 错误都必须保留 outbox。

同一函数还有一处没有兑现既有 F6 durable 契约：primary unlink 成功后
`syncDir(dir)` 的错误在 `:262` 被忽略，随后仍删除 outbox；outbox unlink 的 sync 错误也在
`:274` 被忽略。若 primary unlink 未持久而 outbox unlink 持久，崩溃后 start 可以复现而
exact terminal 已消失，结果仍是矛盾终态。这里不能把 `Remove` 返回 nil 等同于 durable
retirement；primary directory fsync 成功必须是删除 outbox 的前置条件。

建议把“父目录存在且可打开 → remove/ENOENT → directory fsync 成功”做成一个返回强确认的
原语。任何一步未知都保留 outbox。对于 parent ENOENT，没有外部 mount identity 时无法证明
历史 row 不会回来，安全行为只能是保留 exact terminal。

### R6-F3 — Major — canary 忽略 lookup error，幂等 create 绕过 state 保护并删除数据

`ProbeMetaCanPlace` 只在 `js.Stream(...)` 返回 nil error 时检查 ownership/messages/consumers；
任何其他错误都直接落入 create（`internal/jsstream/jsstream.go:340-363`）。这把 timeout、
permission、transport 等 unknown 与 `ErrStreamNotFound` 混为一类。

若已有一个 config/marker 完全相同但含消息的流，首次 lookup 短暂失败后，
`CreateStream` 会按开发者已经确认的同配置幂等语义返回旧流。post-create verification
只检查 marker 和 `Config.Replicas`，不再检查 `State.Msgs/Consumers`（`:375-399`），随后
删除流并返回 nil。

独立测试
`TestExternalRereviewPlacementCanaryLookupErrorCannotBypassStateProtection`
只注入第一次 lookup error，真实 JetStream 上预存 marked stream 与一条
`must survive lookup uncertainty` 消息。当前实现稳定返回 placement 成功并将流删除：

> stream not found

最低修复：

1. 初始 lookup 只有明确 `jetstream.ErrStreamNotFound` 才能进入 create，其他错误 fail closed；
2. post-create `Info` 也必须再次要求 `Msgs==0 && Consumers==0`，否则绝不删除；
3. 若 post-create 校验失败，应在返回错误前只清理能够证明属于本次新建且仍未被外部使用的对象，
   不可无条件 destructive cleanup。

### R6-F4 — Medium — session helper 未等待 broker subscriptions，发布硬闸可随机 `no responders`

本轮 `make e2e` 完整保留输出后，D5 除 reviewer 红测外还出现：

> TestHandleSessionListReturnsOnlyMine: nats: no responders available for request

`runBrokerForSessions` 启动 `b.Run` goroutine 后只调用 `waitNATSReady(t, url)`，该函数证明的是
NATS server 可连，不是 broker 已完成 subscription 注册
（`internal/broker/sessions_test.go:17-44`）。测试随即发送 request，因此存在明确的
subscribe-ready race。单测 `-count=50` 没有复现，只说明窗口很窄；完整矩阵负载下已经真实
命中一次。开发者此前一次丢失身份的 E2E 红灯无法追溯，不能断言就是同一测试，但本轮这次
已经归因。

建议 helper 等待 broker 自身 readiness 或轮询一个有应答的轻量 subject，而不是等待底层
NATS；同时不要忽略 `SessionList` request 的 error（当前 `:167` 丢弃错误后继续 unmarshal）。

## Round-5 finding 处置

- R5-F1：原三条直接反例及 developer state matrix 转绿；但 missing-parent 与 durability
  confirmation 仍产生双终态，转为 R6-F2，不能关闭。
- R5-F2：primary 为普通文件且持续不可用时 outbox-first replay 转绿；反向 source failure
  不安全，转为 R6-F1，不能关闭。
- R5-F3：marker 现在强制，markerless/带 consumer 流均保留，原反例转绿。
- R5-F4：明确看到 existing stream 时，pre-delete error 已 fatal；但 initial lookup
  unknown 可以绕过整段保护，转为 R6-F3。
- Drill 96 #58：标题仍恰好出现一次，reviewer oracle 通过。
- Memory canary 文案：仍明确只证明 meta assignment，不宣称 File/ObjectStore disk
  budget，保持正确。

## 验证结果（审查快照）

通过：

- 开发者本轮 state matrix、primary-unavailable、markerless/authored/consumer/marked-residue
  测试；
- 所有旧 round-3/round-4/round-5 独立反例；
- `git diff --check`；
- `go vet ./...`；
- `make lint`：`0 issues`；
- simcluster hermetic `tests/run-all.sh`：ALL PASS；
- 两个 R16/G67/G69 reviewer shell oracle；
- sim server 起跑前/Drill 67 结束后均无运行 node/container；
- 独立重跑 Drill 67：健康 CONTROL 首次成功，注入后按预期
  `jetstream_not_ready`（2 attempts/5.3s），恢复后首次成功；最终
  `INCOMPLETE assert_fail=0 setup_red=0 product_red=0 nc_gap=1 pass=18`，唯一 gap 仍为
  已登记 face B。

失败且已归因：

- focused tests：只失败于 R6-F1、R6-F2、R6-F3 三条独立反例；
- `make test`：只失败于同三条，其他包通过；
- affected-package `-race` + concurrency leak gate：broker/jsstream 只失败于同三条，无 race
  报告；cluster/natsconf/serveconf/cmd/tether/concurrency 通过；
- `make e2e`：`662.914s`，P1–P13、transfer defaults、proxy dial、D1–D3、
  PhaseFluidity、D6–D9、RemoteFS、ProxyTunnelReconnect 通过；D4/D5 传播 reviewer 红测，
  D5 另命中 R6-F4 的 session helper race。

开发者记录的一次 Drill 67 healthy-control failure本轮未复现。源码算术说明：当前 adopted
契约是“每个已启动 attempt 获得完整 2.5s”，不是保证总有 3 次；两个 full-timeout attempt
加 backoff 后，8s 总预算会按设计拒绝第三次。开发者那次仍是一次真实 transient refusal，
但返回 `jetstream_not_ready` 并指导重试符合当前产品契约，不能据此单独判产品回归。

## 疑惑与建议

1. “两个 source 独立”不能被实现成“任一 source unknown 时另一边所有动作都合法”。请在状态表
   中增加 scan-known/unknown 维度；exact replay 与 start synthesis 的安全条件不同。
2. “confirmed gone”的持久化定义是什么？当前实现既把 missing parent 当 row absence，又忽略
   directory fsync error，与注释中的 durable precedence 不一致。
3. canary 为什么没有显式区分 `ErrStreamNotFound`？所有 destructive read-before-delete
   流程都应把 unknown fail closed；只写 `if err == nil` 是常见且危险的错误形态。
4. 发布硬闸的非确定性失败必须保留完整身份。本轮已找到一条 readiness race；建议让 E2E
   runner 永久保存每个子命令完整日志，避免再次出现 `tail` 之后无法归因。

R6-F1、R6-F2、R6-F3 关闭前，审查快照结论为 **Fail**。

> 用户随后明确授权：本审查快照完整加入暂存后，由当前进程直接修复；修复内容与最终结论
> 必须留在暂存区外。后续结果会追加在本报告，但不会覆盖已暂存的 Fail 证据。

---

## 用户授权修复后的最终结论

**Pass，可以放行当前暂存外修复供下一轮主进程审阅/合入。**

外审 Fail 快照已先完整加入暂存，之后才开始修改实现。三项产品 finding 与一项测试基础设施
finding 均已修复，所有独立红测、全量门禁和相关 deploy-tier 验证通过。本节及修复代码按用户
要求保留在暂存区外；没有再次执行 `git add`。

### 修复内容

#### R6-F1：outbox census unknown 不再授权 synthetic terminal

`finalizeStrandedXfers` 仍允许 outbox 不可读时重放 primary 中的 exact terminal，但
start-only synthesis 增加 `obErr == nil` 硬前置条件。这样两个 source 仍可做安全的独立
进展，同时不会把 unknown census 当空 census。

独立测试
`TestExternalRereviewUnreadableOutboxCannotAuthorizeSyntheticTerminal`
已转绿：outbox 不可读的第一轮不再提交 false failed；outbox 返回后只提交 exact complete。

#### R6-F2：primary retirement 绑定已打开的目录并要求 fsync

新增 `removeLedgerRowDurably`：

1. 先 `os.OpenRoot(parent)`，父目录缺失/不可读即为 unknown；
2. 相对该已打开 root 删除 row，避免 path rename 后操作另一个目录；
3. 对同一目录 handle 执行 `Sync()`，成功后才算 durable retirement；
4. primary 任一步失败都不删除 outbox；
5. primary 已 durable 消失之后，missing outbox directory 才可安全视为无需清理。

独立测试
`TestExternalRereviewMissingPrimaryPathIsNotConfirmedLedgerAbsence`
已转绿；所有既有 callback/recovery/repeated-pass/state-matrix 测试也保持绿色。

#### R6-F3：canary unknown lookup fail closed，并在第二删除点重验 state

初始 `Stream` lookup 现在只有明确 `jetstream.ErrStreamNotFound` 才进入 create；timeout、
permission、transport 等错误直接返回。`CreateStream` 后的 `Info` 除 marker/replicas 外，
再次要求 `Msgs==0 && Consumers==0`，任何外部使用都拒绝 cleanup 并将 placement 判为
unproven。

独立测试
`TestExternalRereviewPlacementCanaryLookupErrorCannotBypassStateProtection`
已转绿；预存消息与 stream 保留，probe 返回 unknown lookup 错误。markerless、authored、
consumer、marked-residue 及 R=1/R=3 基本路径保持绿色。

#### R6-F4：session helper 等待实际 subscription ready

`runBrokerForSessions` 在返回 client 前，用无副作用的 invalid-JSON create 请求轮询 session
handler subscription；`TestHandleSessionListReturnsOnlyMine` 也不再忽略 request error。
该测试连续运行 100 次通过，完整 E2E 的 D5 重跑通过。

### 修复后验证

全部通过：

- 三条 round-6 独立红测，以及全部 round-3 至 round-5 reviewer tests；
- developer fallback state matrix 与 canary ownership tests；
- `TestHandleSessionListReturnsOnlyMine -count=100`；
- `make test`：全包通过；
- affected-package `-race`：
  `internal/broker`、`internal/cluster`、`internal/jsstream`、`internal/natsconf`、
  `internal/serveconf`、`cmd/tether` 全部通过，无 race 报告；
- `test/concurrency` leak gate；
- `git diff --check` 与 `git diff --cached --check`；
- `go vet ./...`；
- `make lint`：`0 issues`；
- simcluster hermetic `tests/run-all.sh`：ALL PASS；
- `make e2e`：`671.537s`，P1–P13、transfer defaults、proxy dial、D1–D9、
  PhaseFluidity、RemoteFS、ProxyTunnelReconnect 全部通过；
- 修复后二进制重新 build sim image 后运行 Drill 96：
  `INCOMPLETE assert_fail=0 setup_red=0 product_red=0 nc_gap=7 pass=37`。7 项均为脚本内已
  登记、逐条说明的既有 coverage gap；无新增/未归因产品失败；
- sim server 最终状态：无运行 node/container。

审查阶段独立 Drill 67 也已通过所有产品断言：
`INCOMPLETE assert_fail=0 setup_red=0 product_red=0 nc_gap=1 pass=18`；修复后 Drill 96 的
grow-to-3 又实际通过 placement canary，因此本次 canary fail-closed 修改没有破坏正常部署路径。

### 最终疑惑与保留项

1. Drill 96 仍不能在高性能 sim host 上可靠 kill 到真正 mid-flight 的 1GiB transfer，
   #57/#58 的关键 crash 形态仍主要由 hermetic tests 证明；这是报告中如实登记的既有证据
   缺口，不是本轮修复伪装成已覆盖的范围。
2. Drill 67 face B 仍缺少“不污染 broker 自身 I/O、只让 caps RPC 失败”的 deploy-tier
   注入器；既有 hermetic owners 仍保留。
3. canary 仍是 MemoryStorage，只证明 meta assignment，不证明 File/ObjectStore disk
   budget；用户可见文案已经正确收窄。

上述保留项均已在既有计划/Drill 账目中显式存在；本轮发现的 Blocker/Major 已有确定性回归
测试并全部转绿。最终结论改为 **Pass**。

### 暂存边界

- 已暂存外审快照：92 个文件，cached diff SHA-256
  `c50ad36345962d60e5847cfa00acb7dd65604c6afc94f519a5d97e901d5ed0d3`；
- 修复及本最终结论：保持 unstaged；
- 未执行 commit/push。
