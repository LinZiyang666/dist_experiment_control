# Fail — 验证与故障模型内审（等待 C1 修复复验）

2026-09-20；审查基线 `cab90d1dcafe6badc4d68a9492c2a2b2ad6cc025`，对象为其后的未暂存开发增量。此报告是固定三专家中的验证专家内审意见，不替代另外两路内审、最终完整门或独立外审。生产代码只读；本专家仅添加/修正测试，没有暂存、提交或线上操作。

## 结论与当前阻断项

**C1 / 高：取消后的四消息窗口过早放行，仍可压断同一控制 WSS。**

`internal/jsstream/object_reader.go` 的 `Close` 在取消 SDK Fetch、其 Messages channel 本地关闭后即释放 connection gate，随后才等待 DELETE consumer 回复；未到客户端的旧 batch 此时仍可位于服务端/套接字队列。预创建的多个 reader 可各发送四条再取消，导致窗口累加。`nextChunk` 在 batch channel 关闭后先 release 再检查 batch error 也属于同类路径。

独立真实 WSS 反例：默认 MaxPayload 1 MiB、默认 max_pending 64 MiB、2 MiB/s socket 下行；先创建 64 reader，逐个 Read，经另一个 TCP 连接确认服务端 Consumer.Delivered >= 4 后取消并异步 Close，再激活下一 reader。没有缩小服务端阈值；没有模拟 WSS/Fetch。4.83 秒内出现 `websocket frame too large: 21130949`，采样 pending 21,130,833 B，控制 RPC 1/2 超时，CID 9→138，多笔 Close DELETE 请求超时。日志 `cancel-window-formal-before.log`；正式测试 `TestTransferObjectCancellationPreservesControlOverSlowWSS` 已在工作树。

验收应证明取消/异常批次的残留数据被排空，或旧连接上的后续窗口被安全阻止；不能仅移动正常成功路径的 gate，也不能在清理超时后无条件放行。已报主进程和 transport 专家，等待修复后同一测试复验。

## 审查面与已完成证据

### 1. 大文件与控制面

- 阅读真实归档索引、varz/connz 和可用日志；归档 exact payload = 66,549,440 B，max_payload 1 MiB、max_pending 64 MiB、slow_consumers = 0。因此“降低 max_pending 造成 slow consumer”不能冒充现场根因。
- 结合固定版本 `nats.go v1.52.0` / `nats-server v2.14.6`：旧 ObjectStore.Get 的 ordered push **已有 flow control**，其窗口可增至 32 MiB；非浏览器未压缩 WSS 可聚合大帧，而客户端限帧为 8*MaxPayload。修复不应以“原来完全没有回压”解释。
- 真 TLS WSS、信任测试 CA、系统默认 socket receive buffer、精确现场随机 payload：旧 shared Get 最终文件完整但多次断线/控制超时；独立 data Get 让控制不中断，但 data 仍因超大帧断线。因此独立连接不能单独称根治。
- 正式 66,549,440 B 测试旧代码 overlay 红：3 次 frame-too-large、14/60 次控制 RPC 失败、CID 改变；新有界正常路径绿：189 次控制 RPC 全成、最大 281 ms、完整 length/SHA、零断线/重连、同 CID、consumer 清理。详见 `wss-formal-before.log` / `wss-formal-after.log`。
- 该正常传输收据不证明所有取消并发路径安全，C1 是进一步故障注入得出的新增阻断项。
- 实验 closed reason 为 Client Closed，历史归档为 Read/WriteError，历史 agent 未记录原始 DisconnectErr。已明确机制与依赖匹配的可复现缺陷，**未宣称唯一确定历史物理断线原因**。
- simcluster README 明确其 plain NATS 路径不覆盖真实 WSS/Caddy；采用以上本机 TLS WSS 作为补充，不把 simcluster 绿当 WSS 证据。

### 2. 同节点 expose 重放

- 检查同一个 adapter、同一个 Client 在控制 session 重建后重新 AddProxy→OpenHome 的调用链；旧实现先拨号/注册才发现旧 listener，因此相同节点占着端口仍会 WARN。
- 新实现对 same NID generation/local port/token/epoch/effective home 检查已有或正在 redial 的 session；无需再占端口；pin 更新仍须按独立 recovery 内审的 generation/fence 检查。
- 纠正早期猜测：`runAcceptLoop` 真实使用传入 localPort 的值，当前 `localPortLookup` 未用于新流；失败重放删除 local map 不能直接证明现有 payload 立即损坏。恢复旧 map 是状态一致性修补，不能夸大已观测影响。
- 本 lane 核查生产调用链；精确 pin/fence/race 反例与双 home 测试由 recovery 独立 lane 负责，最终放行必须合并其意见。

### 3. 单 broker 抖动与真正 survivor 逃逸

- 身份需来自回复对应 ingress 的 NATS ServerID，不能把 broker leader 的 ID 当当前连接入口；Optional ConnectedBrokerID 在 wire 中为 additive/omitempty，签名 roster 的 canonical bytes 不变。
- 检查 response 记录与当前 nc/serverID/URL 的绑定；别名不能凭 URL 字符串算出“另一个 broker”。同时不能以“roster 只有一个 voter”一概拒绝 escape：当前入口可能已被 retired，唯一剩下的恰是不同 survivor。
- 新 `TestBrokerSilenceEscapesToTheOnlyRemainingVoter` 使用两个真实本地 NATS、真实 Agent.Run、带真实签名 roster 的 register stub，通过本机 CONNECT 映射区分保留域名；从仍连接但不回复 refresh 的 retired 真正切换到 survivor，并由 survivor 完成 forwarded exec 的校验响应。不是只观察 rebuild hook。
- `-race` 通过：`silence-sole-survivor.log`。将目标排除逻辑 mutant 改为重拨 cfg.NATSURL，同一测试明确失败：8.09 s，retired full registration 22 次/refresh 66 次，survivor registration 0 次，`silence-destination-mutant.log`。
- 此测试的 registration/membership 决策是桩；证明 agent 实际接入 survivor 与订阅恢复，不冒充真实 membership 变更测试。真实单 voter identity/alias 和 broker lookup 由现有新增单测及主进程全门覆盖。

### 4. broker / NATS 重启恢复

- 历史 #4 已有与“4 分钟没重拨”相反证据：旧版本 broker 在约 2.7/5.6 秒收到两隧道 REGISTER；缺的是旧 supervisor 成功重连日志。历史没有 payload probe，因此也不能反向断言历史 payload 当时已可用。
- 新 `TestExposeRecoversAcrossBrokerAndNATSRestarts` 覆盖真实 broker.New/Run、真实单次 Agent.Run、生产 adapter、文件 SQLite 真关闭再打开、同 stable cert/端口/agent home。每次以禁用 keepalive 的新 HTTP 连接验证原公共端口和 token 下的独立 sentinel。
- 两窗口：① NATS 保持，只重建完整 broker；② NATS 同端口重启且 ServerID 改变，先确认同一个 agent 恢复 NATS 连接、tunnel 仍未启动且 payload 不通，然后启动 broker。无需人为重新 expose/重启 agent/调用 Open。
- `-race` PASS，`broker-restart-full.log`，包 7.066 s；实际 payload 恢复分别约 294 ms / 538 ms。初版 fixture 把正常 context.Canceled 当错误已纠正，不是生产缺陷。
- 此项是 in-process 的真实 broker/DB/NATS/agent 生命周期；未覆盖 systemd、Caddy、WAN 或 released binary 混部，报告保留这个边界。

### 5. history bytes 与非法尾随 JSON

- 新 `TestAuditTransferRendersExactByteCounts` 覆盖 0、66,549,440、2^53+1、MaxInt64；后两项用于精度判别，未宣称产品接受超大文件。
- 基线科学计数/精度丢失明确失败，UseNumber 后绿。新 `TestAuditEntryRejectsTrailingJSON` 保持原先 whole-document 拒绝约束，另允许合法尾随空白。
- 移除新 EOF guard 的 mutant 使四类尾随坏输入全部失败，`history-eof-mutant.log`；不是只验证正常第一条 JSON。

### 6. 权限、协议、错误路径与闸门真实性

- 内审发现原 auth_callout 与 CLI pull 测试的 ctl stand-in 仍调用旧 store.Get，完整 e2e 即使通过也不会证明新 pull consumer ACL。已获主进程授权将这两处改为真实 GetTransferObject，保留跨 SID 拒绝等原断言，并显式检查 Close 返回以证明 DELETE consumer 权限。
- 真 auth_callout push/pull `-race` PASS（7.075 s）；补显式 cleanup assertion 后 pull 再 PASS（3.904 s）；CLI pull happy path PASS（3.230 s）。日志 `transfer-auth-helper.log`、`transfer-auth-cleanup.log`、`transfer-cli-helper.log`。
- 检查 metadata bucket/name/link/deleted/NUID、size/chunks、SHA-256 digest、consumer sequence/stream sequence/NumPending、当前 max_payload 与历史 chunk size、精确 subject 与 AckNone 策略；生产额外的恶意 metadata / slow sink /并发/取消测试由 transport lane 审核。C1 说明仍需组合故障，单项单测不能推出连接层安全。
- Optional wire identity 字段未改变签名输入；按源码与 inventory 检查 N-1 后向兼容。没有运行 released N-1 binary 矩阵，不能把源码兼容性等同已测混部。

## 本专家变更与收据清单

新增文件：

- `internal/jsstream/object_wss_test.go`：2 个 Test（正常真实 WSS、取消真实 WSS）。
- `cmd/tether/history_render_test.go`：2 个 Test。
- `test/p13/broker_restart_test.go`：1 个 Test、两个 restart 窗口。
- `internal/agent/broker_silence_escape_test.go`：1 个 Test。

修改既有测试：`test/security/transfer_authcallout_test.go`、`test/cli_e2e/transfer_test.go`。合计 **6 个新增 Test**，inventory 由主进程统一维护。完整命令/量化数据另见 `validation-tests.md`。生产与清单由主进程编辑，全部仍未暂存。

## 待闭环

1. 修复 C1 后对同一正式取消 WSS 测试跑绿，确认错误/超时路径也不会释放未排空的窗口；按 transport 的实现审查意见交叉核验。
2. 最终实现落定后由主进程执行完整 make test、e2e-parallel、lint/清单/泄漏门，不能沿用修复前的绿收据。
3. 合并 recovery/transport 全范围内审结论与交叉质疑后，停在用户要求的独立外审门；此报告不授予上线或提交许可。
