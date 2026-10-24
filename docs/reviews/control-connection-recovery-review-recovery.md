# Pass — 内审恢复链路；整体候选仍待其他两路闭环及完整门

审查者：独立恢复专家；阶段 C 第二路。生产只读，新增独立测试，不修改 production、inventory，不运行 lint。基线 cab90d1；本路四项发现已闭合，16 个新增测试在最终恢复实现上 -race 连续 3 轮通过。本结论仅覆盖本报告的恢复范围，不取代传输/验证内审、最终完整门或用户要求的外审。

## 审查面与完成情况

- [x] expose replay 的完整身份比较：public/local/token/effective home/epoch/NID、rename generation。
- [x] 活 transport、正在 redial 的唯一 supervisor、现存 stream 与后续 stream。
- [x] caller ctx 与 Client Start ctx；失败配置变更保留旧态。
- [x] persisted 空 pins、同 epoch cert rotation、从 N=1 无验证 transport 升级到 named home。
- [x] adapter local cache 的失败 rollback、stale epoch nil 结果。
- [x] broker 接入身份字段来源、定制 NATS server_name、leader 代答、N-1 omitempty。
- [x] roster 签名采纳边界、迟到响应、current 从 roster 移除、过期/nil、dial alias 排除。
- [x] 真 TLS tunnel.Server 销毁重建、单 Client payload 自主恢复，5 轮 goroutine/fd 门。
- [x] 现场 #2/#3/#4 原始归档核对；历史结论与当前回归分离。

## Findings

### R1 — Major：幂等路径可将未验证 transport 冒充成已验证 named home（已复现并闭合）

最初实现只比较地址/配置并更新 desired pins；已有 N=1 insecure transport 与 named home 使用同地址时，不执行 TLS peer 验证。携带任意其他证书 pin 仍返回成功。

独立 `TestOpenHomeReplayCannotPromoteAnUnverifiedTransport` 最初 5/5 红，日志 `unverified-replay-tests.log`。后续扩展为 explicit named address 与 empty default address 两子例，因为后者只要带非空 pins，原拨号也必须验证。加入正确实际 peer 的正例，要求验证后仍保留现存 REGISTER/stream。

主进程新增真实 `tls.Conn.ConnectionState` 校验和 `clientSession.pinned` 后，发现第二个同源反例：初始默认地址经过 pin 验证；ApplyHome 同 epoch 清除 default desired pins；真实 redial 已经 insecure，旧 pinned=true 却跨 transport 保留。随后带 foreign pin 的 named replay 又错误成功。独立 `TestOpenHomeReplayRechecksTransportAfterUnpinnedRedial` 5/5 红，日志 `replay-pin-provenance-tests.log`。

修法要求：校验实际 TLS peer；每次 redial 返回此次握手是否使用 pin 的不可变结果，swap 连同 transport 更新，不能从可并发改变的 desired pins 推断。named home 即使复用旧已验证连接，也必须有新/保留 desired pins 可供其下一次 redial 使用。

随后发现并复现第三个同源并发边界：无 pin redial 已完成 foreign cert 的 TLS 握手但 REGISTER 被阻塞；replay 验证旧 transport 的真实正确 peer 并接受 named pins；放行旧 handshake 后，swap 仍安装未通过新 pin 的 foreign transport，实际 payload 通。`TestOpenHomeReplayPinsFenceAnInflightUnpinnedRedial` 3/3 红，日志 `replay-inflight-pin-tests.log`。安全升级必须同时覆盖已经在途的 insecure 结果：安装前按最新 desired pins 验证，失败关闭结果并继续 supervisor 的重拨；仅更新 pinned 标记不足。

最终实现：以上三类反例均已修；swap 在 c.mu 内将 insecure 新 peer 与当前 desired pins 核验，拒绝后返回 retry，supervisor 保持同 slot 继续 redial，不重复 down edge。named replay 保存显式 brokerAddr，防以后 ApplyHome 误按 default insecure 路径清掉 pins；其 supervisor 仍持原 brokerAddr 值，但 effective address 相同，最新 pins 从 slot 读取。三个安全测试及正常 replay/轮换/恢复测试连续 3 轮 race 绿。

### R2 — Minor：adapter 缓存需跟随实际安装 slot（主进程已修）

AddProxy 在 OpenHome 前写入 local cache；OpenHome lower epoch / rename fence 可以 nil no-op，单纯错误 rollback 不足以恢复实际 localPort。新实现通过 Client.LocalPort 读回实际已安装配置；无 slot 则删 cache。

当前 tunnel stream 实际使用 supervisor 捕获的 localPort，LocalPortLookup 未走数据路径，因此这里是状态一致性问题，不将其夸大成已证实 traffic corruption。普通失败 rollback 和真实旧 endpoint 存活由独立 replay 负例覆盖，adapter 完整测试交由主进程/验证专家。

### R3 — Minor：旧 identity writer 可覆盖新 session route（静态交错，已修）

rememberConnectedBroker 的 ncBox==nc 检查最初在 rosterMu 外：旧 reply 通过检查后等待 rosterMu；新 session 发布 ncBox 并写入 successor route；旧 writer 后拿锁覆盖。读取端还检查 nc/route，不会把旧 brokerID 当新身份，但新连接已经证明的 ID/aliases 被丢，可能抑制其后必要的 island escape。

建议在 rosterMu 锁内重新检查当前 ncBox；不要在该锁内新增 nats observer 调用。独立迟到测试已覆盖调用前就旧的 serverName/URL/nc，但未强制复现这一特定锁交错，不将测试绿充当并发证明。

最终源码已在 rosterMu 内复查 ncBox，旧 writer 即使在外侧通过检查，也不能在 successor 已发布后覆盖其 route。锁内没有新增 nats observer。其他 route 快照检查和 nil/N-1 兼容仍保留。

追加复核：同一个 *nats.Conn 自动重连不会改变 ncBox，因此新增 `rosterRouteOrder.Add(1)` 在 remember 入口分配顺序，锁内拒绝覆盖已经写入的更高 order。它补足“旧调用已通过 route 检查、等待 rosterMu，新 route 回应先写入”的窗口；无效回复不会写入 route，单纯推进计数不会阻断此前合法回复。order 保存在 Agent 上并与已采纳 route 一起读写，没有引入锁内 NATS observer。未扩大测试面；既有 6 个身份测试 -race 连续 3 轮绿（agent3.050s、broker3.363s），日志 `recovery-route-order-race-tests.log`。这不是专门强制同 nc 锁交错的测试证据，结论依据上述锁序/顺序号静态审查及既有回归。

### R4 — Minor：expiry 判定应使用注入时钟（已修并测过）

最初 predicate 内 time.Now 绕过 Agent.cfg.Now，`expired-roster-disables-escape` 反例红。主进程改为 predicate 显式接收 a.agentNow，最终 3 轮 identity 定向 tests 已绿。

## 其余审查判断

- replay 不再额外 REGISTER；真正 token/local/home/epoch 不同仍进入原验证/拒绝路径，失败不替换旧 slot。正在 redial 的 supervisor 可以被幂等保留；这表示接受既有 desired state，不表示此刻数据面必然 up。
- caller 预取消和锁内取消会返回错误；session 仍归属于 Client Start ctx。SetNID 退休旧 slot，generation 阻止旧握手跨身份安装；新 fast path 还比对 old.nid。
- 同 epoch 轮换允许当前已验证 transport 存活、下一次拨号使用新 pins；不能要求每次 rotation 的新 pin 立即匹配旧 transport，否则会损坏合法滚动轮换。
- ConnectedBrokerID 来自 authenticated register 回复，按请求的 NATS server_name 查询 cluster node；用途是排除当前连接候选，不能被描述为带签名的成员资格证明。字段不改变 roster 签名形状。
- fallback aliases 只从 accepted cachedRoster 推导，不从未验签 response.Roster 推导。独立测试补了 unsigned response 的实际地址伪造反例。
- 无身份单 alias 保守不重建；已识别 current 被删除后，旧 ID 保留使唯一 survivor 仍可作为 escape 目标；nil/expired roster 禁用 heuristic。已知当前 public/route/旧别名均排除。

## #4 现场事实与覆盖边界

归档 broker 在 9/17 21:25:30.191Z 注册 14024，在 21:25:33.078Z 注册 14005，对应 agent EOF 后约 2.7s/5.6s。因此“4 分钟没有重拨”与原始证据矛盾；`opened` 只在初次 OpenHome 打印，不能用其缺失推断 supervisor 没重拨。

历史归档没有恢复后的 public payload，不能由 REGISTER 进一步声称应用当时一定健康。当前独立真实 tunnel server restart 测到了 payload，但不是完整 broker+SQLite+NATS 进程顺序测试；完整层级由验证专家另验。本路未改终端 DENY/revoke 重试权限。

## 交叉内审与跨模块判断

已读 `internal-transport-review.md`、`internal-validation-review.md`、`validation-tests.md`，并对照当前 `object_reader.go` 与 agent transfer 的调用/取消/rename 路径。

- 同意 transport/validation 的取消窗口阻断：SDK Fetch 本地结束不代表旧 wire 数据排空。当前实现已改 UNSUB/PONG barrier 与真实 PullMaxBytes，但本路未独立复现慢 WSS 取消反例或确认最终修复收据，不用恢复测试绿替代 C1/T-R1/T-R2 的闭环。
- GetInfo/consumer create 仍使用收到 commit 的 nc；恢复修改没有改 ACL、terminal DENY、revoke 或 transfer 分配权限。named tunnel pin 的升级仅影响已有同授权 allocation，不授予新端口所有权。
- rename 前已检查 commitCtx.Err，能拒绝在此检查前已经发生的取消；检查之后与原子 rename 同时到来的取消不可能追溯撤销已完成写入，不能宣称绝对取消优先。
- 残余说明：push-commit goroutine 内部读取 `loadRunCtx()`，terminal event 仍读取 `nidOf(a)`，不是 subscription 接收代不可变快照。通常旧 nc 的 finalizer 关闭会让延迟旧请求失败，watchdog处理无法送达终态；本路未证明旧 handler 在 successor 起后仍能写文件，因此未将其列已证实阻断，但文档不可把当前代码称为严格接收代 ctx/identity 绑定。已向主进程建议后续捕获 subscription closure 的 runCtx/nid，而不是依赖运行时读取。
- validation 的真实 broker/SQLite/NATS 双顺序 restart 在 `broker-restart-full.log` race 绿，294ms/538ms payload 恢复；它补足本路 tunnel-only 层级。另 `silence-sole-survivor.log` 的真实落点与 forwarded 回复、旧目标逻辑 mutant 的明确失败，补足本路“触发 rebuild”测试。收据为另一专家独立执行，本报告未冒称自行运行。

**当前整体候选不能仅凭本路结论放行**：另外两路报告读取时仍将 C1/T-R1/T-R2 列待闭合；主进程需整合其最终复验，运行最终完整门，再停外审。

后续交叉收据更新：已读取 `wss-formal-final.log`，正常精确 66,549,440B、194 RPC零失败、零断重连；64 reader取消测试129 RPC零失败、零断线；两项 -race PASS59.298s。`transfer-auth-final.log` 真 auth_callout push/pull两向 PASS7.112s。transport专家另回传严格bytes历史chunk反例、失败PONG barrier与第二reader等待测试已绿。因此前段“等待真实WSS复验”的测试缺口已补；各 lane 正式报告的最终改签与主进程完整门仍由其负责，本路未重复这些长测试。

## 最终复验

命令：

```sh
go test -race ./internal/tunnel ./internal/agent ./internal/broker -run 'Test(OpenHome(Replay|DifferentIdentity|CancelledReplay)|TunnelClientRecoversAcrossServerRestarts|BrokerSilenceUsesConnectedNodeIdentity|ConnectedBrokerIdentityRejectsStaleRouteReplies|BrokerSilenceExcludesEveryKnownCurrentAlias|SingleBrokerAliasSurvivesRepeatedRosterSilence|RegisterReplyIdentifiesTheConnectedBroker|ConnectedBrokerIdentityAbsentOnLegacyReply)' -count=3
```

exit 0；tunnel 3.458s、agent 3.102s、broker 3.084s。日志 `/tmp/tether-control-recovery/recovery-final-race-tests.log`。16 个新增函数共 3 轮，server restart 内置每轮 5 次真实服务重建及 goroutine/fd gate。此前并发编辑引起的 broker QueryRow 参数类型编译问题已修并由该批次证明可编译。
