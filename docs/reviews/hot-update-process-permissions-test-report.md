Pass — broker process-resume 权限与路由限定范围；不代表可靠 Exec 端到端或热更新已完成。

> 交接附记（2026-09-20，用户已要求停止本轮实施）：下文是此前已完成的 **8 Test + 1 Fuzz** 权限测试快照，不是当前整个工作树的放行。之后主进程为 broker 错误回复补 CallID，测试新增 `TestProcessResumeErrorPreservesCallID` 三臂，且原请求 fixture 的 CallID 改为非零 179；这两项最新变化 **未运行**。当前共有 9 Test + 1 Fuzz。最新状态与未验证事项见 `docs/reviews/hot-update-handoff-protocol.md`。原始结果、SHA 和限制保留如下。

## 范围与结论

本路仅新增 `internal/broker/process_resume_test.go`，未改生产、既有测试、清单或门禁，未跑 Docker、全矩阵、lint，未 stage/commit。主进程将 handler 合并至 `internal/broker/exec.go:568`，私有 Reply 校验位于 `:618`。当前权限与转发实现未发现阻断性缺陷。

- L1 parser 测试覆盖 actor 的真实 USER nkey CRC、严格私有前缀与 literal suffix；拒绝他人 `_TINBOX`、旧 `_INBOX`、通配符/控制字节/空 token/ActorFP 冒充公钥，并有合法 reply 正例。
- L2 真 NATS 与 SQLite 覆盖全部五种 op，只有当前 SID member/owner 经授权后转发；顶层与嵌套 Exec.ActorFP 均取认证 subject，完整 opaque handle/lease/TargetInstance/请求参数/ACK 不得变写。
- Reply 在 JSON/ACL 错误之前校验；真实 Msg.Respond 与同连接 marker 验证任意 canary subject 没有收到 broker 注入。marker 随 handler 返回后发布，零消息断言不靠 sleep。
- malformed subject 包括合法外形但 CRC 错误的 actor。旧 ExecReq、未知版本/操作发送到新 verb 必须 typed reject，不转到 legacy exec；合法 probe 正例确保并非一律拒绝。
- 实际单投票 Raft leader 与缺少对端的双投票 nonleader 分别验证转发/静默；broadcast 分类存在。follower 将 DB 关闭后仍不得读 ACL 或回复错误。
- 实际生产 Broker.Run 注册的订阅、auth_callout 签发 JWT、ctl/agent nkey 连接覆盖 member 请求→agent observer→私有 reply；实际 NATS 拒绝伪造 actor 和直接 forwarded subject。保持原 ctl 连接/JWT 删除 membership 后，create/resume/ACK/terminalACK 都必须被当前 DB ACL 拒绝。

## 测试身份（由主进程登记清单）

- `internal/broker/process_resume_test.go: TestPrivateProcessReplyBindsAuthenticatedActor`
- `internal/broker/process_resume_test.go: TestProcessResumeForwardStampsActorAndPreservesHandle`
- `internal/broker/process_resume_test.go: TestProcessResumeRejectsUnsafeReplyBeforeEveryError`
- `internal/broker/process_resume_test.go: TestProcessResumeMalformedSubjectCannotPublish`
- `internal/broker/process_resume_test.go: TestProcessResumeUnsupportedProtocolNeverUsesLegacyExec`
- `internal/broker/process_resume_test.go: TestProcessResumeRechecksCurrentSessionMembership`
- `internal/broker/process_resume_test.go: TestProcessResumeClusterRoutingUsesLeader`
- `internal/broker/process_resume_test.go: TestProcessResumeAuthCalloutPinsActorAndRechecksMembership`
- `internal/broker/process_resume_test.go: FuzzPrivateProcessReply`

## 实际执行与原始收据

1. 最终 8 Test：`go test -race ./internal/broker -run '^(TestPrivateProcessReply|TestProcessResume)' -count=3`，exit 0，PASS **20.808s**。日志 `process-resume-broker-final.log`。最终文件已含 ACL-denial arbitrary Reply canary 与 unsupported-version/legacy-body test。
2. `go test ./internal/broker -run '^$' -fuzz '^FuzzPrivateProcessReply$' -fuzztime=30s -parallel=2`，exit 0，PASS **30.247s**，**75,177** 次执行、32 个新增 interesting input（总42）；无失败语料。日志 `process-resume-private-reply-fuzz.log`。独立 regex oracle 对完整可接受集合判等，覆盖一律拒绝的假绿。fuzz 后只新增行为测试，fuzz 函数和 parser 未改。
3. 三项隔离 /tmp overlay 负控制均为 **预期 FAIL exit 1**，不是编译/vet 失败：
   - `process-resume-mutants/reply-guard.json` 移除入口 Reply 校验；`TestProcessResumeRejectsUnsafeReplyBeforeEveryError` 报不可信空 Reply 仍转发，0.662s。日志 `reply-guard.log`。
   - `process-resume-mutants/actor-stamp.json` 移除顶层 ActorFP 盖章；`TestProcessResumeForwardStampsActorAndPreservesHandle` 所有 op 拒绝伪造身份继续传递，0.604s。日志 `actor-stamp.log`。
   - `process-resume-mutants/lease-target.json` 将 lease TargetInstance 改为 changed-instance；同测试捕获完整请求与原 target 不一致，0.671s。日志 `lease-target.log`。
   每项命令为 `go test -race -overlay=<对应绝对 JSON> ./internal/broker -run '^<对应 Test>$' -count=1`。overlay 映射仅临时副本，仓库生产未变。
4. 迭代记录不冒充产品红：`process-resume-broker-initial.log` 是主进程移动生产文件时 vet 查旧路径失败；`process-resume-broker-initial-stable.log` 所有业务断言已过、仅新 fixture 误把正常 context.Canceled teardown 当失败，测试 cleanup 已修。随后 7 Test ×3 PASS 18.588s（`process-resume-broker-race.log`），再有上述最终 8 Test ×3 完整收据。

全部日志位于 `/tmp/tether-control-recovery/`；负控制日志位于其 `process-resume-mutants/`。

## 覆盖边界与接续约束

- agent 是经真实 ACL 的订阅观察者；没有启动实际业务 child。未验证 agent lease MAC/幂等 registry、stdin/输出 replay、终态 ACK、ctl 重连、跨 exec 状态导入或五秒控制预算。
- malformed subject 测试将 subject/reply 注入真实 bound NATS Msg 以调用 handler，其本身不证明 auth_callout 路由；独立 AuthCallout 测试才覆盖生产订阅/签发权限。
- leader/follower 测试不是双 broker failover drill；未知版本拒绝不是 N-1 binary/client fallback 全链路验证。
- 新增 CallID 后 broker 当前错误响应仍为零，已告知主进程：后续 ctl 如依赖非零 CallID 匹配，须在已校验私有 Reply 后有界提取关联字段（ACL 仍为副作用前硬闸），或采用每 call 独立 inbox。当前尚未启用端到端 API，不能宣称响应关联已闭合。
- proto 在并行开发中新增 CallID/output batch，权限测试用整个 struct marshal 比较，仅已有字段设非零；不把本文件当 CallID/replay schema 完整测试。下列 proto SHA 是报告时快照，可能晚于编译快照；broker 生产和最终 test SHA 在最终运行前后相同。
- 资源 cleanup 全部由 fixture Cleanup/Unsubscribe/Close/取消等待完成；本次不另宣称五轮 g/fd 泄漏门已跑。主进程接总门和 inventory。

## 报告时源码摘要

```
5b26bdf29f7963d59fa5a1a027730c1b1f727f08f364a35be9d0626368216658  internal/broker/process_resume_test.go
2124babfb7d5bc1a438637ff644ee6bbf23b32cd37872a646e508f2c5ad36c51  internal/broker/exec.go
dc9f907b6a1d9793edff8ece735a1b10d4a276461c25f7496f3caef4ab70aee1  internal/broker/admit.go
50d7da5ddf929e079230e874c352116b47102e667c0d847c9ffdd00e32809105  internal/broker/broker.go
9b100673c393a83e9fba450f9a1d79a3d4f80fc26e110687365b91e498a9a30f  internal/broker/clusterwrite.go
8fd745b68b24b779167d1ee3ab613dbc80cdd76ace81e153c235f13e3bebc958  internal/proto/process_resume.go
```
