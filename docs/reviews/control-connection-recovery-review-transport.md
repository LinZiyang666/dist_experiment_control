# Pass — transfer lane internal review after fixes

审查者：固定传输专家；阶段 C，独立于主进程实现。本报告是内审，不能替代外审。生产代码只读；独立新增测试仅在 `internal/jsstream/object_reader_test.go`。未暂存、提交、部署或修改线上服务。

当前结论：本 lane 曾判 **Fail** 的 T-R1/T-R2 已修复并通过原反例复验，连同并发、完整性和预算三项发现均闭合。新增失败 barrier 恢复测试又独立验证了正常 WSS 场景未触发的清理超时分支。当前传输范围未发现未处理的阻断问题。主进程还须完成最终全项目 gates，再交外审；此 Pass 不是整体上线/提交许可。以下保留缺陷发现和修复过程，旧缺陷位置标注为修前行号。

## 审查范围与依据

- `internal/jsstream/object_reader.go` 全部；`internal/agent/transfer.go:227` 与 `:323`；`cmd/tether/transfer.go:966`。
- 既有 `internal/auth/permissions.go` 的 STREAM/CONSUMER/MSG.NEXT/DELETE 和 private inbox 模板；broker admission/重复 commit 状态机；agent Run/session ctx。
- nats.go v1.52.0 `jetstream/object.go`、`jetstream/pull.go`、`jetstream/jetstream_options.go`、`js.go`、`ws.go`；nats-server v2.14.6 `consumer.go`、`websocket.go`、`jetstream_api.go`。
- CLAUDE.md、需求和架构 §9/传输预算、测试规范、`docs/reviews/control-connection-recovery-plan.md`，以及只读现场归档。

## 原阻断项与闭合证据

### T-R1 · Major · 取消/Fetch 错误释放 gate 时，SDK channel 已关闭不等于旧 wire 数据已排空（已闭合）

修前位置：`object_reader.go:193–201`（batch Error 前释放）；`:247–260`（Close drain channel 后释放，再 DeleteConsumer）。修后见 `nextChunk:212–245`、`Close:268–295`。

触发：同一 nc 上预先创建多个 reader；弱下行让第一批数据停留在 server/kernel/WSS pending；取消当前 reader，另一 reader 开始 Fetch。nats.go `pull.go:939–1016` 的 ctxDone/heartbeat 分支会直接结束 fetch goroutine，并关闭 Messages、Unsubscribe，并不等待那批数据从底层连接读完。因此此处 drain 只 join SDK goroutine，不能作为网络 barrier。多个独立取消能把已交出的四分块窗口累积起来。

反例前提有现实入口：broker `transferTracker.markCommitted` 对重复 commit 返回 true，agent 对仍在 cache 的相同 transfer 异步再次读取；仅有每 bucket admission 不能证明同 nc 永远只有一个 reader。GetInfo/CreateConsumer roundtrip 会限制新建 reader 的速度，但不能限制已创建等待 gate 的 reader。

状态：**validation lane 独立真 WSS 已确认**。64 个预创建 reader，依次 Read，独立 TCP 观察 consumer Delivered≥4 后 cancel/并发 Close；4.85s 内 server pending20,868,415B，客户端报 `websocket frame too large: 20737322`，3次 control RPC中1次超时。反例 overlay `/tmp/tether-control-recovery/cancel-window-overlay.json`，日志 `cancel-window-scratch.log`。本 lane 给出源码时序与疑点，验证 lane 独立执行并回传数据。正常 20 reader 测试在每批读完后放 permit，覆盖不到此取消时序。

修复：成功完整批次消费完才交还 permit；错误/取消保持 permit，先 UNSUB 再通过同 nc Flush/PONG 确认排空。如果有界 Flush 失败，window.needsBarrier 保留；下一个预创建 reader 在成功 PONG 前不得 Fetch。不是依赖 cluster DeleteConsumer 回复排序。

独立源码证明：nats-server `client.go:3500` 的 processUnsub 同步 unsubscribe；`:3780` 附近 deliverMsg 在目标 client.mu 内检查 sub.isClosed，`:2740` processPing 在同一 client.mu 内排 PONG。因此本地订阅关闭后，cluster route/outq 的迟到数据不能再排给旧 sub；已排字节位于 PONG 之前。ref 从 Get 入口 retain；构造与旧 Close 并发时不会丢 needsBarrier。最后 ref 真正归零后的新 Get 必须经过 GetInfo→GetLastMsgForSubject 的真实网络请求（无 metadata cache），响应晚于此前 UNSUB，可建立新的安全窗口；该推导不依赖“构造函数名字看起来会联网”。

复验：亲读 `wss-formal-final.log`，64 个取消 reader 16.053s，129 control RPC 零失败，最大270.612ms，零断线，sampled pending最大393,794B；同一正式测试修前红、修后绿。该日志由 validation lane 的测试进程产生，主进程收集收据后交本 lane 交叉核对；validation 最后报告仍是 initial Fail，没有伪称其另行签署了最终 Pass。

额外独立测试 `TestTransferObjectFailedBarrierBlocksNextReaderUntilRecovery`：真 NATS TCP，故意扣留入站 PONG 和删除响应；Close 有界退出后，预创建第二 reader 发出真实 PING，但没有新的 MSG.NEXT；恢复入站后完整对象、consumer归零。`-race` PASS5.287s。仅通过临时 overlay 禁用继承 barrier，同一测试 FAIL7.075s，明确未尝试恢复 barrier；生产文件未被 mutant 修改。日志 `object-reader-failed-barrier.log` / `object-reader-failed-barrier-mutant.log`。

### T-R2 · Major boundary · max_payload 历史保护只检查 metadata.Opts.ChunkSize（已闭合）

修前位置：`object_reader.go:44–48`、`:223`；修后请求双限见 `:238–245`。

正常 Go ObjectStore.Put 写下 ChunkSize，所加 guard 能拒绝正常历史对象在 server max_payload 降低后的危险读取。但 metadata.Opts 允许 nil，ChunkSize 允许零或失真；GetInfo 并不把它与实际 chunk 比对。若流中已有大 chunk，server 之后降低 max_payload，metadata 无 Opts/记录更小值，四消息 Fetch 的实际 bytes 仍可能超过客户端 8×MaxPayload 帧上限。该 guard 目前不能支撑“对任意有效元信息都控制 wire bytes”的宣称。

独立真实 server 反例：MaxPayload1MiB 下写入256KiB chunks，ReloadOptions 降至32KiB，新 nc 确认 INFO 更新；normal Opts 能拒绝，nil Opts 和伪小 ChunkSize 两臂却完整读取危险旧 chunks，`-race` FAIL0.457s。单 chunk 已占 8×新 MaxPayload 的全部空间，附加协议足以越帧限。此测试通过 TCP 证明危险实际 bytes 被读取，没有把它伪称另一场 WSS 断线。当前 Tether 正常 Put 记录默认 ChunkSize，不能把该低频配置/元信息组合等同本次现场直接原因。

修复：SDK legacy public `PullSubscribe(..., nats.Bind(stream, consumer))` + `Fetch(4, nats.Context(ctx), nats.PullMaxBytes(limit))` 同时设消息与字节限额，GetInfo/Create/Delete仍用新公开 API，无手写私有 wire 或升级依赖。limit = min(5×MaxPayload,32MiB)，覆盖客户端 min(8×MaxPayload,64MiB) 的绝对上限。仅设 ConsumerConfig.MaxRequestMaxBytes 不足，server `consumer.go:4622` 对请求 maxBytes=0 不检查，因此采用真实 request 限额是必要差异。SDK legacy Fetch 可以返回 partial 且吞其后的 error，helper 明确 len(batch)!=wanted→UnexpectedEOF，不会误报完成或释放未排空窗口。

同一 `TestTransferObjectRejectsHistoricalOversizedChunks` 三臂修后 `-race` PASS1.418s，保持控制 flush成功、无 consumer残留；日志 `object-reader-historical-bytes.log` / `object-reader-historical-bytes-after.log`。

## 本轮已发现并闭合

### T-C1 · Major · 逐 reader 窗口仍能跨 reader 累积

初实现每 reader Fetch(4)。独立真实 NATS 20-reader 测试暂停每个 reader 于首字节：server 合计 delivered=80，约 10 MiB 默认 chunk，超过默认 8 MiB 帧限。不是推测的“未来并发”，重复 commit 现有路径已能进入。

修复：主进程按 `*nats.Conn` 共享一个批次 gate，reader 引用计数，最后 Close 删除 map key；正常完整批次读尽后释放。相同测试从 FAIL0.243s 转为 `-race` PASS3.623s。日志 `object-reader-concurrent-before.log` / `object-reader-concurrent-after.log`。

额外审查：Close cancel 在 mutex 之前，能解除 gate 等待；等待者 Close 不偷走 holder 的 permit；不同 nc 不共享 gate；Close 幂等防重复释放。后三行为新增真实 NATS 回归 PASS。注意该闭合不包括 T-R1 的网络取消积压。

### T-C2 · Major · 额外尾 chunk 被忽略，错误地报完整成功

初实现仅依 metadata.Chunks 读取并按已读前缀 digest 完成；在真实 `$O.<bucket>.C.<NUID>` 追加尾 chunk 后仍成功。独立 `extra_tail` 子例确认 FAIL0.062s。

修复：每条 message metadata.NumPending 与声明剩余 chunk 数一致，结合 consumer 连续序号、stream 序号递增、长度、digest 校验；尾部、缺块和重排均失败。15 metadata +3 chunk 子例 `-race` PASS1.900s。

### T-C3 · Medium · 固定 5min inactive /额外 30s fetch 会缩短既有 transfer budget

消费者可能在大传输仍有合法总预算时因 sink/共享 gate 等待而过期；额外 fetch 30s 也是新超时源。主进程将 inactive 提升到 max(5min, remaining ctx deadline+1min)，取消固定 fetch 30s，继承调用者 context。新增真实 ConsumerInfo 检查：11min parent 下 inactive≥11min，PASS。

最终改用 legacy Fetch 后没有新 API 对长 Fetch 默认5s heartbeat 的额外约10s失败源；仍继承调用方完整 deadline。架构 §9 不保证断线/重建时的在途传输，保留该边界，不宣称透明续传或任意小于总预算的网络中断都能恢复。

## 通过的审查面

1. **根因区分**：官方 Get 已有 FlowControl；旧 push window 可达 32 MiB，不能称“没有任何回压”。客户端帧限 8×MaxPayload，而非默认 server64MiB pending 阈值。现场 slow_consumers=0，pending8–12.9MiB，未达64MiB。validation 真 WSS 命中 frame-too-large；独立 data nc 的数据仍失败，不能仅靠隔离作为完整修复。
2. **内容与原子发布**：metadata bucket/name/deleted/link/NUID 校验、连续 consumer seq、递增 stream seq、NumPending、最终 size/SHA 均审查。agent/ctl 仍以 prepare 给出的独立 expected size/SHA 做第二层检查，并在 rename 前检查取消；失败不发布局部文件。空对象不创建 consumer。
3. **ACL/N-1**：仍使用原有对象 wire、当前连接和 private inbox；consumer name 无新增账户身份。所需 CREATE/MSG.NEXT/DELETE/STREAM.INFO/STREAM.MSG.GET 都在 sid 范围原模板内；不扩 STREAM.CREATE/PURGE 或跨 sid 权限。新 receiver 可读取旧 writer 标准对象；旧 receiver 没升级仍存在旧 Get 风险，不能声称混部时自动全部修好。实际 auth_callout 权限矩阵需看集成 lane 收据。
4. **session 生命周期**：agent Get 改由 loadRunCtx 派生，使用收到 commit 的 nc 构造 js；不再在传输中读 successor a.js。session 重建取消在途符合架构既定不保续传。测试未证明任意 reconnect 自动续读，报告不宣称此功能。
5. **资源**：6 次部分读取后 Close/cancel，消费者零、subscriptions 回基线，仓库 goroutine/fd 门通过；真阻塞 Fetch 在 Close/cancel 下及时结束。消费者 Delete 失败由 inactive 回收，Close 返回错误但两个上层 defer 忽略它，属于现有 best-effort 清理形状。T-R1修后的失败 barrier 测试证明 gate 不误放新批次，恢复后可继续；引用在所有 Get失败/Close分支释放，Close幂等，不再存在独立数据 nc/callback/session tracker 复杂度。

## 独立测试收据

详见 `/tmp/tether-control-recovery/transport-tests.md`。本 lane 当前 13 个 Test 函数，均真实 JetStream，无 object-store mock。Stage C 新增六个：

- `TestTransferObjectReadersShareOneConnectionWindow`：20 reader，正常路径共享总窗口≤4；`-race` PASS3.623s。
- `TestTransferObjectQueuedClosePreservesActiveWindow`：取消等候者不放开 holder 窗口，holder 关闭后第三 reader 完成。
- `TestTransferObjectConnectionsHaveIndependentWindows`：另一 nc 不被 holder 阻塞，字节完全一致。
- `TestTransferObjectConsumerLivesThroughTransferBudget`：11min context 不被 5min inactive 截短。
- `TestTransferObjectRejectsHistoricalOversizedChunks`：server reload后真实旧 chunks 和三类 Opts，原两臂红、修后全绿。
- `TestTransferObjectFailedBarrierBlocksNextReaderUntilRecovery`：扣留真实网络响应强制Close barrier失败、第二reader安全等候、恢复后完整；移除继承barrier负控红。

后三项一起 `go test -race ./internal/jsstream -run '^TestTransferObject(QueuedClosePreservesActiveWindow|ConnectionsHaveIndependentWindows|ConsumerLivesThroughTransferBudget)$' -count=1` PASS1.635s；日志 `object-reader-window-lifecycle.log`。

此前整组 PASS47.297s 发生在 6tests 版本，不能冒充当前13tests整组收据。各新增路径已单独 `-race` 验证；本 lane 未跑 lint/全量，主进程负责最终候选统一门。

## 交叉核验

已读 recovery 最终报告和 validation initial Fail 报告，并与两专家交换具体反证。支持 recovery 对真实TLS peer、每次redial验证来源、在途insecure握手安装时重验的三层闭合；单纯期望pins或历史pinned标志都不能充当当前transport的认证证明。#4 现场只有秒级重新REGISTER，缺失 opened 日志不能推断4分钟没重拨；真实broker/SQLite/NATS双顺序restart新payload测试是当前修复证据，不反向补造历史payload。

agent 的 loadRunCtx在handler执行时获取，再启动数据goroutine；收到commit的nc已经固定，但ctx/nid并非订阅接收代不可变快照。通常旧nc完成teardown才进入新session，旧请求后续Get失败；本轮未证实跨代成功落盘，保留该说明，不把当前实现宣称为严格所有身份字段同代快照。rename前ctx检查也不意味着能追溯撤销恰好同时完成的原子rename。

亲读最终收据：

- `wss-formal-final.log`：66,549,440B读取38.897s，194 RPC零失败、max283.930ms、零断/重连；取消64reader上述129RPC零失败。测试包race PASS59.298s。
- `transfer-auth-final.log`：真实auth_callout push/pull、PullThenPush，含helper和consumer显式Close，race PASS7.112s。
- `transfer-cli-final.log`：CLI tier-B pull happy path race PASS3.582s。

收据由验证测试进程产生、主进程亲收；validation专家的最后报告停在修前Fail，当前闭合判断由本lane复读最终日志及源码后作出。没有篡改其报告或伪造其最终签署。

## 剩余项目级门

当前候选统一全项目test/e2e/lint/清单/泄漏gates由主进程完成，保留来源与真实结果，然后停独立外审；不能因本lane Pass越过用户要求的外审门。未声称simcluster/plain NATS替代真WSS，未改线上服务、未提交或暂存。
