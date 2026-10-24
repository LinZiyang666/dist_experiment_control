# 全栈热更新交接：Claude 从这里继续

**状态：未完成，不可放行。用户因时间限制于 2026-09-20 要求停止实现、移交另一 Claude 会话；本文件是工作交接，不是通过报告。**

交接时间：2026-09-20 18:56 CDT 起（23:56 UTC）。仓库 `/home/weiland/dist_experiment_control`，主机 weilandserver。
HEAD：`cab90d1dcafe6badc4d68a9492c2a2b2ad6cc025`。**index 为空，所有本轮代码、测试、文档仍在暂存区外。没有 commit、push 或现网部署。**

## 0. 接手后的第一步

1. 阅读 `CLAUDE.md`，然后读本文件、[原计划](hot-update-plan.md)、[完整 tasklist](hot-update-tasklist.md)。需求权威链仍是 `docs/requirements.md` → `docs/distributed-broker-architecture.md` / `docs/deploy-tier-gotchas.md`；旧 `docs/architecture.md` 只作历史依据与流程 checklist。
2. 阅读三份专项交接：[进程/请求/运行层](hot-update-handoff-process.md)、[协议/权限/ctl](hot-update-handoff-protocol.md)、[已确认失败/PTY/启动](hot-update-handoff-failures.md)。先修本文 §5 的 **F1/F2**，不要把现有局部绿灯当完整候选绿灯。
3. 核对 `git status --short`、`git diff --cached`、本文 §9 的最终源码清单。**不要 reset、清理未跟踪目录、重新套旧 patch 或执行 `git add -A`。** 当前未跟踪目录包含大量真实实现和独立测试。
4. 最新 `execclient` 文件在停止前新增了未运行的测试草稿；先做有针对性的编译/测试确认，具体名字见协议交接。当前完整 `make test` 预期至少会因两条真实缺陷和未登记清单失败。
5. 按 §8 顺序继续开发、内审和修改。原用户要求是**实现全部目标、内审修改完毕后停在独立外审门**，不是只交原语或缩为 agent 升级。

## 1. 用户到底要什么、哪些授权仍有效

### 1.1 当前产品目标

原要求先修五项现场缺陷，随后追加“最彻底的热更新”。用户明确拒绝只保 agent/broker、把 NATS/Caddy 留到下一版的缩减范围。

- **agent、broker、实际外置 nats-server/JetStream、Caddy、新增运行层/协调器自身都须可更新。**
- 原业务进程、普通 exec 双输出/输入、PTY、SSH/TCP/SS/proxy、文件传输不因更新中断；进程存在但丢输出、失去 ps/kill 或退出码也不合格。
- 外部原 socket/session、原业务 PID/boot ID/start ticks/nonce 保持；内部 WSS/TLS/yamux/NATS carrier 可以重建，但必须真实续接原业务，不能重新 dial 业务端冒充续接。
- 同 transfer ID、同用户调用、完整 SHA/长度、唯一提交/终态；不能 failed 后另起传输再算成功。
- 更新期间的新请求继续完成，有界等待/重试，无重复副作用。
- 所有旧运行映像最终退出；不能无限留旧 worker 承载长任务并宣布升级完成。
- **每项功能必须有自动 test、真实 simcluster drill、失败/回滚验证、原始证据以及文档**，这些已经写入 plan 的验收硬门。

### 1.2 角色和收尾

当前角色是**开发者**。更早会话的“外审结束全部暂存”“先暂存对方再直改”流程已被用户明确结束，不能把那些旧指令套到本次交接。按现有 plan，本增量保持未暂存，内审后等用户指定独立外审。

用户要求加快速度、充分并行使用 agent，token 不是限制。我们用 root + 固定三路专家代替 Codex 没有的 Claude Workflow。`CLAUDE.md` 的边界仍遵守：**主进程定稿 plan、修改生产；专家读实现、审查、可新增测试，或在 `/tmp` 准备 proposal，root 审核后才应用。** 后续 Claude 可恢复自己的 Workflow。继承主模型，不指定较弱模型。

停止前没有授权缩范围、取消必要测试、部署现网、代签外审或 commit/push。本次仅按用户要求整理交接；不因交接把未完成项勾成完成。

## 2. 两个工作增量不能混为一谈

### 2.1 已完成开发/两轮内审的现场缺陷修复

参见 [控制恢复 plan](control-connection-recovery-plan.md)、[两轮内审与回复](control-connection-recovery-review-round2.md)、[证据](control-connection-recovery-evidence.md)。这批约 46 文件的候选曾独立冻结，新增 43 个测试，当时完整 test/e2e/gates/lint 通过，**仍未经过独立外审，也没有提交**。

冻结收据 `/tmp/tether-control-recovery/final-candidate-sha256.txt` 是这批旧增量的身份，**不可用当前热更新树覆盖它**。当时完整并行 e2e 是 67 units、约 4m2.509s；只证明当时的候选。

| 原问题 | 已做内容及需要记住的边界 |
| --- | --- |
| #1 tier-B 66,549,440B 大文件压断控制 WSS | 新 `internal/jsstream/object_reader.go`：同连接共享的有界 JS pull，最多四块、字节上限 `min(5*MaxPayload,32MiB)`；元信息/序号/摘要校验，取消先 UNSUB+PONG，失败屏障不能丢。agent push / ctl pull 接入。真 WSS 复现过 frame-too-large 并验证修法；**不能据此把历史每一条 Read/Write Error 都认定为同一已唯一证明的原因**。 |
| #2 同节点活 expose 重放误报 bind failed | 同 desired 配置在 REGISTER/拨号前幂等复用原 slot/supervisor/流；身份/token/home/epoch 和 peer proof 不能绕过。未验证旧连接升级 pin 的竞态也有回归。 |
| #3 单 broker 的 alias 被当其他 voter | register additive `connected_broker_id` 标识实际接入 broker，而非回复 leader；cache 绑定 connection/server/URL，拒绝迟到覆盖/过期 roster；真实另一 voter 才逃逸，N-1 未知地址保守。 |
| #4 “broker 重启后四分钟没有重拨” | 原因叙述已订正：归档 broker 在 EOF 后约 2.7/5.6s 接受了两个 REGISTER，agent 的成功重拨缺日志。补日志、实际 tunnel/broker 重启后的同 agent payload 测试；**历史业务 payload 状态仍未知，不能倒推它一定连续**。 |
| #5 history bytes 科学计数法 | JSON `UseNumber` 保精确整数，并严格检查 EOF，不吞尾随内容。 |

第二轮还关闭：连接身份 cache/共享主机地址歧义、同 home pin 旁路、可取消 consumer INFO、迁移 fallback 地址回归。保留这些已有修复，继续热更新时要防回归。

### 2.2 正在实现的全栈热更新

[hot-update-plan.md](hot-update-plan.md) 是已定稿方向；[tasklist](hot-update-tasklist.md) 是完整未完成项；[evidence](hot-update-evidence.md) 是按时间追加的收据，某段“待做”可能被后续段落推进。**本文和三份专项交接给出停止瞬间的状态，旧收据不追溯改写。**

## 3. 原计划和不能丢的架构裁决

### 3.1 H0–H6 全部仍在总范围

| 阶段 | 当前进展 | 仍要完成 |
| --- | --- | --- |
| H0 传感器/基线 | 真实 drill 34 复现旧 reexec 的业务破坏；真 Caddy/WSS 供给可用 | 原 SSH/TCP/PTY/SS、同 transfer、新请求与旧映像退出 oracle；正式环境/预算校准 |
| H1 运行层/进程 | FD、进程、replay、输入输出原语；普通 exec/PTY process owner；新请求表、可靠 Exec 三端的第一版接线 | F1/F2、完整准入/导入/回滚、PTY byte actor、bootstrap/upgrade 真接线、H0 正向转绿 |
| H2 原流与 SS | 设计与风险核查 | 原 raw socket 保留、双向 offset/ACK/FIN/resume、OPEN/撤权、SS AEAD 全状态迁移与测试 |
| H3 文件传输/broker | 设计；旧控制连接修复可复用 | 固定版本/NUID、checkpoint、hash/commit/GC/reaper、SQLite/Raft/JS 单写者和活任务责任迁移 |
| H4 编排/回滚/兼容 | 设计 | 产品事务状态、候选预检、两代限制、各失败窗回滚、真正 N-1、legacy busy/idle 首次迁入 |
| H5 NATS/Caddy/拓扑 | 真供给而非升级实现 | N≥3 与 N=1/N=2 路径、外置 NATS/JS 滚动、Caddy binary 切换、备用入口自身升级、旧进程退出 |
| H6 完整验收/内审 | 尚未进入最终验收 | 四层整轮升降级、混载、至少五代、全门禁、文档齐全、三路内审修复后停独立外审 |

### 3.2 选定的实现方式

首选**同一进程内有类型的 workload/flow/transfer manager，agent/broker 整体同 PID 的可恢复 exec**，没有强制引入常驻 controller 子进程或 IPC daemon。这样保原父子关系和唯一 Wait 权，但必须显式导出全部堆状态、用户态缓存、FD 和任务责任；`syscall.Exec` 本身不具备热更新语义。

- 一项资源始终一个 owner。候选不得提前订阅可执行命令、打开同一 store writer、迁移 DB 或抢 wait/commit。
- pre-Cobra/早期 bootstrap 先导入，禁止新 image 以空进程快照 register，让 G.1 清掉原业务。
- 原 raw 泵先恢复、随后重建可替换 carrier；“资源已恢复”和“控制 ready”分开。任何新进度发生后的回滚必须携带**当前**状态，不能倒回旧快照抹去已 ACK 的写入。
- freeze 包含接纳、spawn、wait/signal、输入/resize/KA、cancel/reaper、accept/dial/提交等，不能仅锁进程 map。
- 预检阶段不能到安全点就延期，维持原业务；不能关闭原 socket/PTY 以逼出一个“安全点”。
- Linux 是首个交接支持平台；Darwin 要保持可编译/现有行为并明确 unsupported，不伪造同等恢复能力。
- 真实进程/原 FD 的 exec 测试只证明该域，不能外推 Go TLS/yamux/SS 或完整 agent 已能恢复。

### 3.3 可靠创建/重放裁决

详见专项交接。关键点必须保持：

1. 新 `process-resume` verb 承载 **probe/create/resume/ack/terminal-ack，包括 create**。旧 broker 会解码再重编码旧 `ExecReq`，丢去重字段；旧 verb 上加 requestID 后直接重试是不安全的。
2. 请求身份绑定 SID/NID、当前认证 ActorFP、ClientLineage、原 TargetInstance、**leaseID + RequestID**，不绑定 runtime generation。同键异 intent 拒绝。
3. agent HMAC 创建租约以同 boot `CLOCK_BOOTTIME` 计时。它只限制新接纳，已知 live/terminal 过期后仍可查；settled tombstone 至少留到原 lease 过期。旧结果 GC 后旧 handle 不得再次 spawn。没有采用永久 lineage 水位表。
4. 热恢复保实例/签名 key/任务/tombstone；冷启动空表必须换目标实例/key。现 agent 的 routing instance 可以继承 env，因此新可靠 Runtime 特意用独立 `process-<ULID>` incarnation，待完整 restore 时必须恢复原值。
5. probe 无副作用，尚未 create 时可以判断 legacy 不支持；**一旦 create 发出，超时绝不能换 handle/lease 或回退旧 exec**。
6. 每次操作都走 broker 当前 ACTIVE/member/node 授权。稳定 ID 不是权限；同 SID 另一 member 能 kill 不等于能读/ACK 原 actor 的输出。
7. `CallID` 只关联一次 request/reply；`AttachEpoch` 防旧附件覆盖新 inbox；`RequestID` 是业务身份，三者不同。
8. stdout/stderr 分开连续 offset，ACK 只按用户 Writer 实际返回的 n；接收 terminal 不等于全部输出完成。异常流不能为了 terminal-ack 强行伪造正常 EOF。
9. broker 在所有 error reply **之前**验证 subject 中完整 User NKey/CRC 和该 actor 的私有 `_TINBOX` literal subject。私有前缀 hash 的是编码公钥，不能拿 ActorFP 算。旧 `_INBOX` 只留旧协议兼容。

当前 runtime/ctl 采用每次受权请求返回最多每流 32KiB 的 replay batch，空闲约 20ms 轮询。它没有证明性能/5s 预算；当前 client 的完整 handle 只在内存，ctl 重启/重新登录恢复入口仍须裁定和实现，不能宣称已有持久 CLI 会话恢复。

### 3.4 已定预算是目标，不是测量成绩

完整表见 plan §7：本地业务泵暂停至新泵可用 **≤500ms（含 exec/Go init/import）**；每个新控制请求含排队/重试 **≤5s**；非慢接收原流最大停顿 ≤5s；每秒至少 10 次新控制探针；artifact 就绪后每组件交接 ≤120s，全栈基准 ≤15min；文件正常臂每 10s 有持久进度。

每逻辑 replay 默认 ≤8MiB、节点总预留 ≤256MiB、最多两代。空 log 也按整容量预留。64MiB JSON manifest 不能塞 256MiB backlog，数据在匿名 FD，metadata 只含范围/状态。修改预算必须先改 plan/说明影响/复核，不能为已有失败临时抬门。

## 4. 停止瞬间已经落在工作区的实现

| 位置 | 实际内容 | 不可外推的部分 |
| --- | --- | --- |
| `internal/handoff/` | Linux 白名单 FD、sealed memfd 元数据、nonce/PID/EUID/指纹校验、失败复原 CLOEXEC、Restore 单次 TakeFile | 没有 agent/broker 整体 bootstrap/候选回滚接线 |
| `internal/workload/process*.go` | sole wait/signal manager、原 PID 身份、真实 exit/rusage、冻结/恢复；AdoptDraining 保未 reap leader 到输出结束 | 未覆盖整个 runtime 接纳与所有控制来源 |
| `internal/workload/replay*.go` | 有界 memfd ring、Base/End/FIN、实际容量预留、ACK/Freeze/Restore、随机 wrap 验证 | 不代表网络 carrier 已续接 |
| `internal/workload/input.go` | pipe sole writer，按实际 Write n 推进 Base，全部输入后才关 pipe EOF，冻结不关原 pipe | **仅 FIFO pipe**；TCP 半关闭和 PTY 输入不能用它直接关双工 FD |
| `internal/workload/output.go` | sole reader，输出保留后才暂停；背压；pending suffix 保留；source EOF 与异常分开 | 最新 `NewOutputWithPrefix` 是后加的启动 warning 接线；旧 Output 测试收据早于此改动 |
| agent ordinary exec/PTY process owner | `startExecPipes` 抽取 safe/普通 spawn，显式 stdio + AdoptDraining；普通 kill、DropProcesses TERM→5s KILL、G.1 联合快照 | legacy run/exec 仍有自己的流逻辑；完整 starting/courier census 未完成 |
| `internal/agent/proc_delivery.go` | 每 register attempt 捕获实际事件身份/快照；late ACK 只 settle 自己携带的同对象；防 ABA | 没有消除 G.1 与新创建的全部并发窗口，也未解决 courier 全部持久化/容量/长期保留 |
| `internal/pty/master_linux.go` / `pty.go` | master 新非阻塞 Go wrapper；resize 走 RawConn ioctl，不再让 `File.Fd()` 清 O_NONBLOCK | 未接 PTY byte actor/FIN/error、resize/KA 导出；旧 200ms tail 截断仍待换掉 |
| `internal/procrequest/` | 有界创建 registry、HMAC lease、intent hash、Reserve 唯一 winner、状态/附件/终态、Freeze/Restore | 测试调用方遵守 bool winner 才 Start；当前授权由 broker负责；state 必须装在受认证私有 handoff bundle |
| `internal/proto/process_resume.go` | 新版本化 lease/handle/request/reply、CallID、replay batch 类型 | 最新字段尚未追加 wire inventory；仍需整个错误流结束协议闭合 |
| `internal/broker/exec.go` 尾部 | 新 handler、私有 reply 校验、当前授权、重盖外/内 ActorFP、forward；订阅/admit/broadcast 已接 | **没有单独的 `broker/process_resume.go`**，已合并避免增加 broker 70 文件预算；最新 CallID 拒绝接线尚未最终复验 |
| `internal/execsession/runtime.go` | typed task/resource owner，异步一次创建、三个 replay、Input/Output、原 manager、ACK/terminal settlement、Close | **两条已证失败未修；没有 Freeze/Export/Import**；不是完整热更新 runtime |
| `internal/agent/exec.go` / `agent.go` | 新 verb dispatch、runtime eager init、safe spawn/courier adapters、Run 最终退出才 Close（不挂单次 NATS session） | legacy upgrade 仍会丢这个新 Runtime；未实现恢复前禁止空 register 的最终屏障 |
| `internal/execclient/client.go` / `cmd/tether/exec.go` | probe→稳定 create→ACK/batch→terminal ACK；重复/短写/CallID；CLI 首选新路径，只有尚未 create 的 unsupported 才 legacy；ACK 后 drain 再非零 exit | 最新 CLI 没有真实 binary/四端验收；新 client 后追加测试未执行 |

## 5. 接手时必须优先处理的真实红灯

原报告：[Fail：Runtime 失败路径](hot-update-execsession-failure-review.md)。`runtime.go` 确认候选 SHA：`b25f9b7481f4cea5f9aadc72b436c646376f1e69e6459fb93e7d25183fcf3943`。

### F1：Close 超时后迟到启动失败没有最终资源清场

创建先分配三份真实 replay，Start 阻塞；取消的 Close 标 closed 后提前返回；随后 Start 失败，done 关闭，但任务和 stdout/stderr 两份 memfd/容量仍残留。不能要求调用方再 Close 一次才释放。修复方向：唯一 startup/observer 在 closed runtime 下负责最终 close/delete，且和正常保留 terminal replay 的路径分开。**未修。**

### F2：已启动 child 的 pump setup 失败使终态无法通过协议结清

合法测试容量 8B、启动 warning 16B；原 child 真启动并被 AdoptDraining，stdout actor 已建立，stderr prefix 初始化失败。当前 closeTask 关闭 replay；kill/reap 和 terminal 记录虽然完成，resume 却在已关闭 replay 上 Ack，返回 `file already closed`，不能得到可结算的输出终态。修复要分开物理 pipe/pump 清场与未交付结果责任，明确异常流结束/确认语义。**不能把 error 当正常 FIN，以削弱断言让测试绿。未修。**

根进程被打断时只是考虑过“不可逆停原 source，但保留 replay/error 责任，给异常流明确 ACK”方向，**没有新增 Abort/Detach API，也没有实施/定稿这项设计**。接手者应从现有状态机重新验证修法，不寻找不存在的已完成 patch。

定向复现（这不是要求再次全量跑）：

```sh
go test -race ./internal/execsession -run '^TestRuntime(CloseCanceledStartupReclaimsLateFailure|PumpSetupFailureRetainsObservableTerminal|PartialAllocationFailureSettlesAndReleasesBudget|CloseReapsChildWithUnacknowledgedFullOutput)$' -count=1 -v
```

原结果：两红两绿、rc1 / 0.095s，`execsession-failure-confirmed.log`。测试清理会回收自己资源，但那不能算产品路径已清理。

### 其他确定未完成的风险面

- 新建成功/started 尚未可靠接管与 register snapshot/reconcile 的并发窗口；统一准入还没有完成。
- `session_destroyed` 的既有 agent 业务清场缺口：真实 exec/PTY/后代、撤销/freeze 竞争与 tombstone 导入仍要做。
- PTY 200ms 输出尾部截止、双工 master 的输入结束语义、last-slave EIO、termios/foreground pgrp/resize/KA 绝对期限。
- 新 Runtime 目前没有 image handoff。不能把原语的真实 same-PID exec 成绩当成完整 `node upgrade` 正向成绩。
- 当前 Output 错误/Pending、启动后 pump 失败、错误流 terminal-ack 尚未统一；客户端 Writer 失败后的任务保留/后续恢复入口也须完整处理。
- 全新功能的最终静态检查/全量测试/真实 drill 都未完成。遗留新增枚举/golden/矩阵登记见 §7。

## 6. 验证到哪一步了

### 6.1 原语与接线测试

| 已执行验证 | 原始结论/收据 |
| --- | --- |
| handoff 基础 | 12 Test + 1 Fuzz；真实同 PID child/pipe/TCP、exec 失败 flags、五轮资源与两个负控制；fuzz 30s 364,469 执行；详见 handoff 两份报告 |
| process owner/state | 真实进程/后代和 PTY ownership；state 6 Test race×3 4.861s、17 非法状态臂、真实 live/held zombie/terminal；详见 state 与 workload 报告 |
| Replay | 12 Test 入口（含 helper）、随机环绕/限额/非法 FD/真实 exec；race×3 24.577s，强化 FD 的 exec 子集 23.644s |
| Input / Output | Input 10 Test race×3 4.439s；Output 8 Test race×3 4.546s；原 pipe、真实部分 Write、EOF、冻结与 exec；新 prefix 接线不借用旧收据 |
| register receipt | 5 Test race×3 2.152s；真实 NATS late register + 原 child exit23；全队列清理负控制如期红 |
| PTY master / spawn 抽取 | 5 PTY 测试 overlay race×3 1.051s，旧实现/resize 变异红；原相邻 PTY与Darwin编译通过。startExecPipes 14 个既有测试 overlay race 14.741s；root 已审核应用 |
| procrequest | 11 Test race×3 4.558s；真实并发只启动1个 child、same-PID exec/原 pipe/状态、expiry/GC、5轮资源；两个负控制红 |
| execsession 主线 | 4 Test race×3 10.743s；唯一真实 Start、输入和双流、1KiB replay 下 256KiB 背压/原 inode、授权/ACK、五轮资源；ACK 部分释放负控制红；**整体仍 Fail** |
| 新 broker 权限 | 8 Test race×3 20.808s；真 NKey/auth_callout/撤 member/注入拒绝，fuzz 30s 75,177 次，三个负控制红；后来新增 CallID 测试未运行 |
| execclient | 第一批 8 Test 真 NATS race 单轮 3.600s；后追加 capability/child helper/真 child 整合草稿未运行，详见协议交接 |
| agent adapter compile | `execsession-adapter-compile.log`，已有 `TestExecExplicitStdioPreservesInputAndEOF` PASS0.131s；不是新协议全链路验收 |
| 最近 `test/cli_e2e -run '^TestExec'` | PASS10.162s，`reliable-exec-cli-first.log`；**该套件的 runExec 是旧 `exec` wire 的测试替身，没有调用新 execclient 或构建真实 CLI，所以只证明旧路径兼容** |

### 6.2 最后一次集中 gates 仍是红

[原始门禁报告](hot-update-stream-gates-report.md)：2026-09-20 23:34:21–23:36:01 UTC，rc2 / 99.914s。冻结 1753 文件前后一致，唯一门错误是 Input leak 测试循环用了外包常量，静态形状门无法解析。之后 root 已改成本地 `const rounds = 5`，**没改 gate/没降低轮次**。

但该 `make gates` 在这里终止，simcluster hermetic 和最终 lint **未到达**；随后又新增协议/Registry/Runtime/ctl/测试，不能把修一行当整轮门已绿。后来的定向 compile/guard 调用还遇到当时专家写到一半的 unused imports，以及未登记的新 PTY resource anchor；原日志保留，不能把并发编辑引起的构建中间态当产品 race。

### 6.3 真正跑过的 simcluster

**H0 drill 34：PRODUCT-RED，不是 Pass。**

- 命令（在 `test/simcluster`）：`IMAGE=tether-sim:hot-update-h0 SIM_EVIDENCE_DIR=/tmp/tether-control-recovery/hot-update-process-real ./local.sh drill 34-hot-update-process`。
- 61.900s，17 setup pass、assert_fail=0、原 rc3。agent 同 PID1223 换真实运行 SHA并 commit；原 child1279/boot/startticks仍活且推进，但 stdout/stderr 两 pipe EPIPE，分别缺92/93记录，管理误为 EXITED(-1)，原 ctl 没收到真实业务退出码23。清理时 ctl rc75 不是业务结果。
- [完整基线报告](hot-update-process-baseline.md)，包括 source ledger、原输出、进程/运行 SHA、日志和 teardown。

**Caddy 供给：真服务/TLS/WSS 正向通过，binary 热切换没有实现。**

- 官方固定 Caddy2.7.6 artifact 每次下载并核验 checksum、tar member、ELF/arch/版本，receipt 与 installer/binary/SHA 绑定，Docker 内复核。提供公开 installer `--no-enable`、显式 profile `enable --now`。
- 分开 agent WSS 地址和 tunnel 地址，JSON quoted YAML scalar 防裸 IPv6/非法地址；生产 YAML parser 回归通过。
- 实际 Caddy unit、authenticated WSS、运行 SHA、冷重启后自启/WSS 通过；plain profile Caddy 始终未启动。
- 额外 plain profile 原 Python ingress sidecar 冷 return 失败：broker 换 netns，sidecar 仍在旧 netns。原 rc1 两份保留，这是已证 fixture 边界，不是 Caddy 冲突，**不要在热升级窗口重挂 sidecar 掩盖产品中断**。
- [Caddy 报告](hot-update-caddy-profile-report.md)。上述实例都已 teardown；停止前没有要求保留服务。

**镜像身份非常重要：**

| 产物 | 已冻结身份 |
| --- | --- |
| H0 image | `tether-sim:hot-update-h0` / `sha256:8285559d7fa2bf2cbce942c0f24323a90c9d19e5e57e1b91bb3eadc0644abcd7` |
| Caddy image | `tether-sim:hot-update-caddy` / `sha256:a5f3d4680edd205eee665de885e3623379d479611e766d405c8d9d5c3d1438ce` |
| vendor tether | `4676bb8dba25eddd0809234def9784f490ccdeba88827849ba5f22c7886b2189` |
| vendor tether-next | `b66591bf5e587f76daf3f1a69d5e1e4ec8c4e08890c8fc226165b3d4e68a9f17` |
| Caddy binary | `db3bfc85bb93160f60fa6df9c3ebf2340dc11740e9a52c717d88a14c0430f229` |

两张镜像里的 tether 都早于本轮 ProcessState/Replay/Registry/Runtime/协议新实现。**下次真实验证必须 restage/build 当前冻结源码。** `tether-next` 当时只是同源码改 release 字符串，不是来自另一实际 release 的 N-1 artifact，不能用作真正 N-1 验收。

## 7. 接手后需要补的工程登记和测试纪律

1. 最新 test inventory 尚停在之前一批（对 HEAD +114/-0）：**之后新增 procrequest、PTY master、broker 新协议、execsession、execclient 的测试尚未统一登记**，不能删测试让门过。
2. `internal/proto/process_resume.go` 的 lease/handle/CallID/batch 字段尚未追加 wire inventory。使用现有只增不减 updater。
3. 需要追加 leak anchors：`TestMasterPollableLifecycleReleasesResources → exerciseMasterReadDeadline`；`TestRegistryRepeatedLifecycleReleasesResources → exerciseRegistryLifecycle`；`TestRuntimeRepeatedExecutionReleasesResources → exerciseRuntimeExecution`。各专家文档有路径/行号。循环保持本地 literal/const ≥5，断言在循环外。
4. 当前 whole race 矩阵已加入 handoff/workload，**未登记 procrequest/execsession/execclient**。按最终实际测试职责追加矩阵并更新 append-only matrix inventory。
5. 结构预算不得放宽：broker file=70、Agent methods=126、Broker methods=285 等是冻结债务门。新 broker handler 因此放 `exec.go` 尾部，agent helpers 是包级函数。当前新接线尚未跑最后结构/lint，真实超限应调整职责或按项目规则处理，不能悄悄涨 golden。
6. Go directive1.26.0 / toolchain1.26.8，CGO=0 产品；Darwin编译保持。代码/注释/错误英文，团队文档中文。
7. 测试按被测职责命名，不新增 round/external-review 命名测试文件；finding 用 `// origin:` 链接真实文档。内建 goroutine/FD 门、≥5轮，不用 goleak。
8. 迭代跑具体影响测试；完整候选才收敛 `make test`、**完整 `make e2e-parallel`**、一份 `make lint`。变更 gates/inventory 另跑 `make gates`。禁止全量串行“复核一遍”；lint 有全局锁，不并行两份。
9. 自写 parser 按需 fuzz；NATS private reply parser 的 30s fuzz 已有收据，若再改规则要复验。负控制用 `/tmp` overlay，失败样本/原 rc 都留，不改 expected/阈值抹红。
10. 不在真实 drill 同机并发大规模 Go 测试制造负载假因果；别把“并发红、solo绿”直接贴 LOAD-SENSITIVE。真实 artifact/源码/配置/时区/拓扑都要绑定。
11. 当前另有未跟踪的 `test/simcluster/lib/__pycache__/`，是 Python 运行产物，不是手写实现；本次交接未清理工作树。未来准备提交/外审暂存时应识别并排除它，不能把“全部新文件”理解为自动发布运行缓存。

可用既有 updater（先查它的实际名字/范围，不盲运行多项全量）：

```sh
go test ./internal/proto -run TestWireFieldInventoryAppendOnly -update-wire-inventory
go test ./test/determinism -run '^TestTestFunctionInventoryOnlyGrows$' -update-test-inventory
```

以上测试名已经按现有源码核对；若后续修改路径/名字，仍应核实实际运行匹配，**不要把未匹配任何测试的 exit0 当登记成功**。

## 8. 建议继续执行顺序

### 最近可做的一个闭环

1. 把当前源码/测试当半成品保留，先编译停止前追加的测试草稿，修 F1/F2，整合异常流结束/ACK 设计并写入 plan；原失败 oracle 保留。
2. 复核可靠 client/broker/agent 全路径，重点当前授权、CallID 错误关联、no responder 有界退避、真实短写、lost create/terminal reply、AttachEpoch、期满 GC、cold incarnation、kill/evict/session deletion。**必须有真正新 verb + 实际 ctl binary 的端到端测试。**
3. 接 `execsession` Freeze/Export/Import，统一 registry、process table、stdin/stdout/stderr replay/pumps、courier、pending startup/终态与当前权限责任。不可只导出 Registry 或 Manager 之一。
4. 将 early bootstrap 和真实 upgrade/rollback 接到这个整体状态；导入失败不能以空 Runtime上线。保持 original child/pipe 后，重建 NATS 并接原 actor/handle。先把 H0 的**同一个正向业务 oracle**转绿。
5. PTY 接独立的双工资源 actor：输入 FIN 不等于关 master；resize ioctl 不碰 File.Fd；最后 slave 的 EIO 只在已验证的 started PTY domain解释为结束；尾部输出不能靠200ms定时关闭；KA/resize/信号/前台组/撤销随 fence 导入。

### 后续整个目标

继续 H2 原流/SS、H3 transfer/broker、H4 编排/回滚/N-1、H5 NATS/Caddy各拓扑，最后 H6混载/多代/资源与全部旧映像退出。所有任务和具体矩阵已在原 plan/tasklist，无需重新让用户选择简化范围。

NATS/Caddy 若需要新增备用进程/入口，必须是**产品**生命周期的一部分，也能被升级/最终退出；harness只能供给环境和独立观测，不能替产品迁移业务。单节点不能只用 `deferred` 替代所要求的成功路径。真实新旧 artifact、Raft/SQLite/JS 单 writer、当前状态回滚都要分别证明。

## 9. 证据保存、现场资料和工作区交接

### 9.1 本机持久副本

为了避免 Claude 只拿到 `/tmp` 路径，完整本轮原始证据已复制到仓库忽略目录：

`/home/weiland/dist_experiment_control/tmp/hot-update-handoff-20260920T2356Z/`

- `evidence/`：原 `/tmp/tether-control-recovery/` 的完整副本，约116MB，含 logs、JSON收据、旧冻结清单、proposal、变异、H0/Caddy真实证据；最终以 `archive-receipt.json` 为准。
- `external-notes/`：原放 `/tmp` 顶层的 wire/inbox/权限设计、H0/Caddy/courier 报告。**其中旧 wire 建议含水位表候选，已被正式 plan 的 leaseID 方案替代。**
- `tracked.diff`：交接时对 index 的 tracked diff；`untracked-files.tar.gz`：未忽略的新文件快照；`source-manifest.json`：HEAD、index、tracked/untracked源码逐文件 SHA；`status.txt`：最终工作区状态。
- 不 force-add 此目录；需要提交的是 repo 的实际源/测试/正式文档。原 `/tmp` 路径仍可直接读，复制后的相对目录不变。

交接文档不是系统快照恢复指令：不要把 tar/patch盲套到当前树，当前树已经包含这些修改。清单用于辨别漂移和意外丢失。

### 9.2 用户保全的现网历史证据

`weilandserver:~/tether-patrol/evidence/2026-09-20/`，约38MB，README 为索引。含 connz_closed_agents.tsv（35条）、connz_closed_full.json（环6928条）、varz、四agent tierB日志、broker切片、history、9/17重启/升级、9/18 WSL rebuild、journal/patrol。

- connz live 是 nats-server 内存环，会被 ctl 每日约2300次连接刷掉，优先读归档。
- broker、h100、connz 是 UTC；weilandserver/wsl/timan agent和history是 CDT（-5）。history只印时分秒，PROC 的ULID前10位可分日期；CALL/XFER无pid可能混入前几天同刻，已有日期切片优先。
- #1 pending 8–12.9MB 时 Read/Write Error，未触64MiB max_pending且 slow_consumers=0，不等于 NATS 服务重启。
- #4 是0.5.0旧样本，0.6.0当时没有重启样本；后来取证订正见 §2。
- 设备权限/资源看 `docs/devices.md`、本地忽略的 `docs/devices-ops.local.md`、simcluster README；不要把凭据抄进可提交报告。

### 9.3 停止时运行状态

用户停止指令后，root和三路专家均只做收据/文档收尾，不新增实现、不运行下一批验证。此前真实 H0/Caddy实例已清理。各专家交接末尾记录自己的测试 session 是否已结束；主交接最后的 `archive-receipt.json` 记录收尾时刻。

**下一会话必须以实际工作区为准，继续开发；本次没有任何“整个热更新完成/通过”的结论。**
