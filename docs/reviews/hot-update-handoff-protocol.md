# 热更新协议与可靠 Exec 交接

日期：2026-09-20。状态：**用户要求停止实施并转交 Claude；没有完成最终验证，也没有外审放行。**

本文件由内部权限/接收端测试 lane 编写，只记录已见实现、已运行证据和未完成事项。停止指令到达后没有新增/修改源码或测试，没有再启动 Go、Docker、lint、门禁；只回收此前命令结果并整理本交接。没有 stage、commit 或部署。

## 1. 当前代码位置与职责

| 路径 | 已落盘内容 | 本路验证边界 |
|---|---|---|
| `internal/proto/process_resume.go` | 独立 `process-resume` verb，协议版本 1；lease/handle/请求/回复、CallID 和双输出 batch | 权限转发测试覆盖原有字段保留；完整 schema/兼容矩阵未验 |
| `internal/broker/exec.go` 尾部 | `handleProcessResumeReq`、`privateProcessReply` | 旧快照 8 Test 与 fuzz 已过；最新 CallID 错误响应接线未复验 |
| `internal/broker/admit.go`、`broker.go`、`clusterwrite.go` | member 准入、生产订阅、leader broadcast 分类 | 旧快照用真 Broker.Run/auth_callout 与 Raft leader/follower 检查 |
| `internal/procrequest/registry.go`、`state.go`、clock 实现 | 范围绑定的请求表、签名创建 lease、intent hash、freeze/restore、回收与过期拒绝 | 由主进程及其他 lane 实施/测试；本文件不代其出收据 |
| `internal/execsession/runtime.go` | registry 与 workload 进程/输入输出 replay 组合；probe/create/resume/ack/terminal-ack | 本路真 child 集成草稿已写，但未执行 |
| `internal/agent/exec.go`、`agent.go` | 创建 process runtime、处理 forwarded verb、接 lifecycle courier、业务退出时 cleanup | 仅只读核对接线，未验证真实 agent 路径 |
| `internal/execclient/client.go` | 公共 `Run`、私有 inbox、probe、重试、接收/ACK、terminal settlement | 第一批八项 fake responder + 真 NATS 单轮 race 通过；后续生产/测试变化未复验 |
| `cmd/tether/exec.go` | 先调用 Run，仅 ErrUnsupported 走旧 exec；可靠分支 Drain 后传播 exit/signal | 主进程刚接线，本路未跑 CLI |
| `internal/broker/process_resume_test.go` | 权限、路由、CRC、Reply、CallID 独立测试 | 当前 9 Test + 1 Fuzz；最新 CallID 一项未跑 |
| `internal/execclient/client_test.go` | 接收端独立测试与真实 child 草稿 | 当前 11 Test（含 helper），最新完整文件未编译/运行 |

Broker 新生产代码为了保持 70 个生产文件预算，已合并进 `exec.go`，不存在独立生产 `process_resume.go`。测试文件仍按协议职责独立命名。

## 2. Wire 与不重复执行边界

新 subject 为 `proto.SubjCmdBy(sid, actorPublicKey, nid, proto.ProcessResumeVerb)`，经 broker 转发到节点对应 `.req.forwarded`。五种操作：`probe`、`create`、`resume`、`ack`、`terminal-ack`。

- `probe` 无业务创建副作用，返回绑定原 runtime 的 capability/lease。当前 client 先验证 lease version、SID、NID、ActorFP、ClientLineage，以及响应/lease 的 TargetInstance 一致。
- handle = 原始 lease + 稳定 RequestID。lease 含 LeaseID、TargetInstance、BootID、SID、NID、ActorFP、ClientLineage、boot clock 起止、MAC。创建重试必须保留完整 handle，不能因丢失 create 回复而生成第二身份。
- registry key 绑定 scope（SID/NID/ActorFP/ClientLineage）+ LeaseID + RequestID；intent hash 覆盖 exec 请求，防相同身份换 argv/env/cwd 等意图。broker 不得把 handle 内 TargetInstance 改成当前节点实例；同名节点重建不等于原任务。
- lease 限制**未见过请求的首次创建窗口**。已保留记录按原 handle 查找；settled 且原创建 lease 已过期才可回收。回收后晚重试必须 ErrExpired，不能重建任务。默认容量 4096、上限 16384；默认 lease 30 秒、上限 5 分钟，均见 registry 常量。
- registry freeze/restore 能力不自动等于 agent runtime 联合迁移已接好。当前 `initProcessRuntime` 明确创建独立新 incarnation，未来必须恢复原请求表与原资源。旧 syscall.Exec 路径不能据此称为业务无中断。

## 3. 身份、权限、Reply 与 CallID

1. actor 来自 nkey 认证 subject。`auth.FingerprintFromActor` 会验证真实 USER key 和 CRC；`ActorFP` 是原始 key bytes 的指纹，**不能用它代替公钥计算私有 inbox**。
2. 私有 reply 必须以 `auth.InboxPrefixFor(actorPublicKey)+"."` 开头，总长不超过 512；每个 suffix token 非空且只允许 ASCII 字母/数字/下划线/连字符。拒绝通配符、控制字节、他人 `_TINBOX` 和旧 `_INBOX`。旧 exec 路径未加此新限制。
3. broker 在任何错误回复前先验证 subject 和 reply；否则合法 publisher 可借错误响应让 broker 向任意 subject 注入数据。
4. leader 才处理；每次 op 都重新读取**当前 SID membership**，不能把 CONNECT/JWT 时的许可当永久授权。实际 NATS permission 禁止 ctl 伪造其他 actor subject 或直接发布 forwarded subject。
5. broker 覆盖请求顶层及嵌套 Exec.ActorFP；保留 lease、TargetInstance、意图及 ACK。agent 用被 broker 盖章的 actor 和本地 SID/NID 构造 scope，不能信 payload 自称的身份。
6. CallID 是一次逻辑请求/响应关联号。重试同一次 call 保持 CallID 与 JSON bytes；后续 ACK 使用新 CallID。client 丢弃不匹配 CallID 的合法 JSON 迟回复后才检查其 version/status。
7. 原 broker 错误回复 CallID=0 会使新 client 丢弃真实 ACL 拒绝。主进程已改为：安全 reply guard → 最大 1 MiB 检查 → 有界 JSON decode 提取 CallID → 当前 ACL → 协议/op 校验 → 转发。合法 JSON 的拒绝须带原 CallID。新增三臂测试（membership/version/operation）**尚未执行**；malformed/oversize 无可靠可解析关联不在此保证内。

## 4. client 字节接收、重试与兼容

- `Run(ctx,nc,subject,ExecReq,stdout,stderr) (Result,error)` 使用 actor 私有 inbox；probe 最多 2 秒。create 之后只重试原 handle，不自动 fallback 或创建新任务。
- 每个 stream 独立绝对 offset；重复前缀不再次写，重叠只写未收后缀，gap/溢出/FIN 后新增字节/冲突 FIN 拒绝。stdout 与 stderr 没有虚构的全局顺序。
- destination `Write` 的实际 n 才推进 ACK。`n>0,error`、短写 nil 都只 ACK 已交付 bytes，不把本批 FIN 当成功接收。退出前部分 ACK 仅有界尝试，不能保证断链时必达。
- 两路 FIN 与 terminal receipt 必须齐全，才发 terminal-ack。其回复丢失重试原 TerminalID/handle；仅已有双方 FIN 和 terminal 时才可把已过期 settlement 解释为完成。其他请求失败不得吞掉。
- 每次 receive 超时 500ms 重发；idle output 下 20ms 间隔。主进程已为即时 ErrNoResponders 增加可取消 500ms timer，避免 503 忙循环。本路测试在该修法存在时通过；原无 backoff 版本的 overlay 反例**未运行**，不能写“修前实测红”。
- client 校验 RequestID、TargetInstance、AttachEpoch、非空/不变 PID、Phase；主进程后续又加入 exit/signal 数值约束，本路没有针对该最新增量收据。
- N-1：只有创建前的明确 unsupported、无 responder/probe deadline、或未获得私有 inbox 才返回 ErrUnsupported；权限错误须保留原错误。CLI 只对此哨兵走 legacy exec。旧 exec broker 可能 decode/remarshal 丢新幂等字段，因此可靠创建使用独立 verb。没有跑真实 N-1 binary 四象限，也没有验证实际 CLI fallback。

## 5. 已执行的证据

### Broker 已完成快照

详情已复制至 `docs/reviews/hot-update-process-permissions-test-report.md`，保留旧 SHA 与边界。

- 最终旧快照 8 Test：`go test -race ./internal/broker -run '^(TestPrivateProcessReply|TestProcessResume)' -count=3`，exit 0，PASS 20.808s，日志 `/tmp/tether-control-recovery/process-resume-broker-final.log`。
- `FuzzPrivateProcessReply` 30s：exit 0，PASS 30.247s，75,177 次执行、32 个新 interesting（总42），没有失败语料。日志 `process-resume-private-reply-fuzz.log`。
- 三个仅 /tmp 的生产 overlay：去掉 reply guard、去掉 ActorFP 盖章、改写 TargetInstance，均在行为断言处预期 FAIL exit 1。日志位于 `process-resume-mutants/`。
- 原生产文件移动撞编译的 vet 失败，以及新 fixture 将正常 context.Canceled cleanup 当失败，都已分开记录，没有伪称产品红。

### execclient 已完成的第一批测试

唯一实际命令：`go test -race ./internal/execclient -run '^TestRun' -count=1`，session **15763**，最终 **exit 0 / PASS 3.600s**。日志 `/tmp/tether-control-recovery/execclient-initial.log`。该命令运行时仅以下八项：

1. `TestRunAcknowledgesOnlyBytesWritten`：n=2+error、n=2+nil 短写、n=5+error；ACK 必须精确，stderr 尚未写不能 ACK，FIN 不得提前。
2. `TestRunDeduplicatesOverlapAndPreservesFIN`：重复 abc、重叠 bcde 最终只写 abcde；独立 stderr/FIN/terminal。
3. `TestRunRejectsOutputGapsAndConflictingFIN`：gap、过期 final offset、FIN 后新增、offset overflow。
4. `TestRunRejectsChangedTaskIdentityBeforeWriting`：request/instance/attach/PID/kind/phase 改变时不写 bytes。
5. `TestRunIgnoresLateCallReplies`：旧 probe 错误与旧 create 错 PID 经同一 responder 有序先于当前响应到达，均不得污染当前流。
6. `TestRunRetriesLostCreateAndTerminalWithOriginalHandle`：真实消息丢回复，JSON bytes/CallID/inbox/handle/TerminalID 均不变；观察所有 verb 防偷偷走旧 exec。
7. `TestRunProbeAuthorizationErrorCannotDowngrade`：not_a_member/session_deleting/permission denied 不能变 unsupported；明确 unsupported 正例。
8. `TestRunNoRespondersHasBoundedRequestRate`：隔离真 NATS 无订阅，用 server InMsgs 观测 350ms caller budget 内真实请求；最多5、必须非零。没有用观测订阅掩盖服务器真实503。

这是 fake agent responder + 真 NATS 收发，**不是 broker→真实 agent→child 集成**，也不是最新整个文件的验证。

## 6. 停止时已写但未运行的内容

停止消息到达前最后一次工具已完成写入/gofmt，以下新增没有编译、没有运行，不可记 Pass：

- `internal/broker/process_resume_test.go`：新增 `TestProcessResumeErrorPreservesCallID`（3臂）；公共请求 fixture CallID 改为179，也影响其他已有测试的当前版本。
- `internal/execclient/client_test.go`：新增 `TestRunRejectsCapabilityIdentityBeforeCreate`（actor/SID/NID/lineage/instance/version/nil lease）；以 Run 返回后同连接 marker 检查没有 create/legacy 副作用。
- 同文件 `TestRunRealChildSurvivesLostCreateAndSettlementReplies` 和 `TestExecClientChildHelper`：Linux execsession/workload 实际 child，4KiB replay，stdin、80KiB stdout/25KiB stderr、exit17；实际处理但扣留首个 create 与首个 terminal-ack 回复，拟验证唯一 child、完整输出及原 handle。这里只是草稿，不保证编译/cleanup/oracle 正确，下一位须独立检查。
- 同文件 helper 改为 `startSpec` 并允许 `next` 任意 op，属于上述未执行文件状态。
- 原计划的 no-responder 旧实现 overlay、短写/CallID 其他负控制没有启动，也没有生成“已证明”收据。
- 上述新函数 inventory/矩阵/门禁登记由主进程接手，本路没有修改清单；当前 gate 是否因新增身份未登记而红未检查。

当前 client 文件共11个 Test（10业务测试 + 1 helper）；broker 共9 Test +1 Fuzz。

## 7. 后续接手的最小顺序（仅交接建议，未执行）

1. 先检查最新未运行测试草稿，确认真实 child 的资源 cleanup、AdoptDraining/AllowReap/Manager 唯一 waiter 边界与 parent 当前 runtime 实现一致；不要直接把之前8项绿灯延伸到该文件。
2. 后续 Claude 接手时按用户既有开发授权继续，优先定向执行 client 全部 Test 与 broker CallID/权限 tests；按所发现差异补反例。本会话已停止实施，不继续测试。
3. 检查完整 broker/agent/client 接线、真实 CLI 的 stdout/stderr 与 exit/signal、当前 ACL 撤销、断链恢复和 N-1 fallback。CallID 关联必须包含安全拒绝路径。
4. 再接联合 freeze/export/import：registry secret/lease/记录 + workload 原进程/pipe/replay/input/output + lifecycle courier + attach state；并验证旧 runtime 不可空表替代原任务。CLI 能流式重试并不等于 agent syscall.Exec 已无中断。
5. 完整任务仍以正式 hot-update plan/tasklist 的 test/simcluster/docs/内审/外审硬闸为准；本次协议工作没有解除后续约束。没有外审结论。

## 8. 停止时摘要与运行会话

本 lane 没有仍运行的命令。唯一待回收 execclient session15763 已收到 exit0；此前 broker race/fuzz/三项 overlay 都已结束。没有本 lane Docker 或部署。

下列为交接读取时文件 SHA，**不是统一已验证构建的 SHA**。尤其 client 最新变化与 broker CallID 变化未运行。

```
8fd745b68b24b779167d1ee3ab613dbc80cdd76ace81e153c235f13e3bebc958  internal/proto/process_resume.go
92a449857d91ccef065551900465384181dec6b293d080ee14cbe1abcb8c02a7  internal/broker/exec.go
1b54601286a7e9a0490a37863135ba6e289bec8b268ea0ad1edcfcd7e614a5cc  internal/broker/process_resume_test.go
13690b48a04954ea7b520f30f01ef4e0c104912b34bfb2d071223c437ac65d23  internal/execclient/client.go
c7a54ccb8ae3a14b43d8ba11a11cc10bcbc0d0ff80b9c1f902facdf28b0af197  internal/execclient/client_test.go
```

## S5 current contract addendum (2026-10-03)

The dated stop-point observations above remain historical. S0–S4 follow their own plans/receipts;
this addendum does not overwrite earlier hashes or turn historical NOT_RUN into a current result.
The [binding S5 plan](hot-update-flow-carrier-plan.md) and
[live architecture §22.8](../distributed-broker-architecture.md#228-s5-flow-and-ss-domains-binding-contract)
define flow/SS ownership. [Final S5 receipts](hot-update-evidence.md#16-s5-flow-carrier-ss-receipt-index)
are NOT_RUN pending P29.

Endpoint owns public listener/socket/journal/fence, rawsocket owns original kernel sockets,
flow owns replay/offset/FIN, SS actor owns parser/nonce/cipher suffix/material/authority;
carrier TLS/yamux is replaceable and not image state. Each image resource has one owner;
Prepared duplicates are separate cut-owned descriptors, and all private state uses sealed memfd.
PREPARE occurs before global freeze. Final Prepare re-exports every domain, including late revoke/fired/cleanup.
Restore takes all resources frozen with no workers, then Manager/revoke → raw pumps (SS AuthorityPending)
→ TLS/yamux REGISTER/exact OK → fresh bounded local-applied RESUME → SS.Authorize/data bind → NATS/register.
Any partial activation creates current-state responsibility; rollback cuts current offsets/nonces/retirement
at generation+1, never Detaches the pristine bundle. Exec failure Thaws originals and reacquires SS authority.

Flow ACK is original raw Write's n, with dedupe before Write; received≠applied, short suffix retained.
Source EOF fixes FIN End and CloseWrite once at Applied=End, preserving reverse tail. Abort preserves actual
prefix/final receipt without fake EOF. Retired OPEN never redials after receipt GC; SO_ERROR has one consumer.
Revoke irreversibly shuts originals/dup and bound public flow, including silent victims; legitimate survivors
are checked separately. SS uses actual-material salt history and an independent subkey+nonce ledger.

S4 failures §4's tail/partial/control/deadline/identity/EOF responsibilities require same-S5-candidate
36.A/W/C/L and39.P regression plus their units/mutations. PTY finalPrepare lost-event + same-SID recreation
remains S8. Broker same-PID exec/Endpoint export remains S6; NATS/Caddy switching S9; final mixed rollout S10.
500 ms full required-domain freeze is calibration with max/excesses, normal business max5 s is hard.
Darwin native lifecycle is NOT_RUN without a runner, distinct from cross-build and nonblocking under S4 L6.
Completion still requires original test/drill/docs/review steps and actual receipts; this addendum signs none.
