# H1 系统接线裁决：同进程分层与同 PID exec

结论：**可采用同进程 workloadManager/control 逻辑分离，替代常驻 controller/runtime IPC；有明确接线前提。**
这是架构可行性判断，非实现或连续性验收通过。只读源码/现有计划；本轮未跑测试、未启动部署。
同 PID exec 保 MainPID、父子关系及白名单内核资源，没有因 systemd/lease/单 writer 而必须双进程的理由。
计划内可恢复交接和可处理候选失败可以等价；任意新映像在 exec 后进程死亡不能被表述为原业务无损回滚。
后者沿用 plan §3.2 已明示的 owner 崩溃边界；若“任意候选失败”包含此项且要求原 socket 存活，纯单持有者方案不满足。
额外 controller 仅隔离 controller 的失败；最终资源 owner 自己升级时仍须解决同一个问题，不能凭进程数宣称已经解决。

## 1. 原目标保持，改变的只是边界实现

- agent、broker、NATS、Caddy、新增运行层及可靠 ctl 全在范围；无限任务继续时全部目标旧映像必须退场。
- 新控制请求端到端 ≤5s、本地 freeze ≤500ms、原进程/原业务 TCP/同一 transfer、单次副作用与提交均保持。
- 最多两代；不兼容/安全点不可达时 defer，不能把延期当支持集合的正常交付结果。
- 本机逻辑接口替代私有 IPC 后，可删除该 IPC 的 wire/peer 认证负担；本地管理 socket/远端续接仍需原有认证。
- 原始事实见 requirements.md:413–427、hot-update-plan.md:60–76、:98–114、:183–190。

## 2. 必须先改计划、再接线的状态机

建议职责：Manager 独占资源、接纳账本和生命周期；Control 仅做经认证的请求路由；Upgrade 协调屏障和镜像选择。
所有操作须经 Manager 的同一准入/fence，不能仍有 NATS handler、转发 handler、PTY callback 或 reaper 绕过。
只需职责接口，不建议在本轮另造通用 IPC 框架。

| 阶段 | 单 owner 与失败语义 |
| --- | --- |
| prepare/preflight | 原 manager 照常服务；候选仅探测协议、导入能力和配置，不接执行订阅、不占活 store 写锁、不跑 migration |
| admission barrier | 关全部新副作用入口；已接纳请求纳入表；新请求由可靠 ctl/健康入口有界保存，沿用同一 requestID |
| freeze/export | 所有读写、accept、wait、rename、计时器、取消到可表示屏障；超时恢复本映像，不能 Kill/Close 强行凑预算 |
| syscall.Exec 返回错误 | 旧映像仍在：回收 Prepare 副本、恢复原 pumps/准入；不得改变原 FD flags、丢已接纳请求 |
| exec 成功、尚未恢复资源 | 旧 Go 映像已消失；新 bootstrap 保全部原资源，校验后导入；可处理失败以同 PID exec prev 恢复 |
| 资源已恢复（含控制未 ready） | 回滚是从当前活状态重新 freeze/export；保新增任务、已 ACK 写、结果去重/退出记录，不能回旧快照 |
| 新映像 fatal/进程终止 | 唯一持有者退出会关闭原 FD；按明示崩溃合同报告，不能声称 preflight 或 systemd restart 保原 socket |

“恢复旧方”只能用于 exec 尚未成功的路径；成功后回滚实际是另一次完整接管。
即使未激活，只要已恢复 pumps 并产生新字节/退出事件，旧导出包也已过时；回滚必须采集当前状态。
返回型失败都须有明确出口；普通 error 到 os.Exit 不能作为热更新失败收敛路径。

## 3. 早期恢复与 rollback bootstrap 是 H1 接线的硬前置

1. 入口在 Cobra、YAML/logger、正常 BootUpgradeCheck、goroutine/子进程和任何资源清理之前识别 handoff。
2. 消耗私有环境变量，验证 PID/EUID/role、manifest/schema、FD 指纹与实例/会话绑定；先设置 CLOEXEC。
3. 保有受信任 rollback envelope/FD 所有权，未验证全部资源前禁止普通启动；未知格式不能猜测资源然后执行。
4. 验证导入布局、旧镜像 SHA/代际/能力；导入完整 manager 状态，然后恢复本地 raw I/O 和 sole wait-owner。
5. 按需要初始化配置/控制依赖；注册只能使用已导入的联合 G.1 快照；就绪后开放唯一接纳入口。
6. 所有可处理错误、watchdog/boot-budget rollback 经同一 handoff dispatcher；不得绕回旧 syscall.Exec 或 os.Exit。
7. 错误恢复前不得运行普通 Close/cleanup 把 live FD/PTY/临时文件删掉；主动 stop/kick/evict 仍能正常清场。

当前缺口有精确来源：
- cmd/tether/main.go:78 先 BootUpgradeCheck，:107 普通错误 os.Exit；agent/upgrade_state.go:382–388 rollback 直接 exec。
- cmd/tether/agent.go:348 配置解析、:360 identity、:378 adapter.Start 均早于 :382 Agent.New；必须为 restore 路径重排。
- agent/agent.go:1781–1807 当前 G.1 只有 PTY 与 courier；不能以新建空表登记并将原任务误收为 EXITED(-1)。
- handoff/image_linux.go:266–278 消耗私有 env，:318–320 验证 owner 后关闭 manifest，:324–336 仍可能拒绝导入。
现有 handoff 原语不是完整 rollback shim；运行层须明确保留/重新生成可信恢复包的方式与失败后的 FD 责任。
既不能在失败后按普通启动继续，也不能绕过校验把未知输入送给任意旧镜像。
Go runtime/package init 发生在 main 之前；候选 smoke 不足以证明该区间永不失败，不可把这个不可捕获窗口藏起来。
当前 architecture §21.3（:927–934）已明示 nohup 起来即崩/boot shim 前初始化缺口；保留此区别，不能将原来的“服务重启回退”升格为“原业务无损”。

## 4. 单 owner、lease 与真实业务状态

- 保 instance lineage、实际 routing NID/lease、sid、node identity、boot/PID/start-ticks、任务 nonce；generation 单调且不是凭据。
- 同 PID 让新旧 Go 映像不会同时执行，但不能阻止候选预检或另起 daemon 重复订阅；候选模式必须禁止活权限入口。
- instance.go:48–67 已明确同 lineage 的双进程会获同名并执行两份请求；环境身份不替代 fencing。
- 接纳账本、pending reply/output ACK、取消/deadline、退出 courier、准备未提交 transfer 都是 manager 状态，不能依赖 goroutine 栈。
- sole wait-owner 必须可暂停；不能以再造 exec.Cmd.Wait 冒领已被旧协程 reap 的子进程。
- raw socket/pipe/PTY 保 FD，读入未消费、部分写、缓冲、half-close 与 deadline 显式导入；TLS/yamux/NATS carrier 重建。
- 恢复 raw I/O 前先导入撤销 tombstone、原绝对 fail-closed/授权期限与唯一 owner；控制尚未 ready 不得延长这些期限。
- 对新的认证/撤销状态重新判定；恢复旧数据不能复活已撤销 allocation/session 或绕过 epoch/pin。

## 5. broker/存储与四层依赖仍是独立工作包

- broker transfer tracker、commit/watchdog/reaper/GC 与内存去重也须归 manager；导入前不执行 boot orphan cleanup。
- SQLite/Raft/JetStream 对象不能从原 FD 推导恢复；交接事务与权属锁，重开受支持 store，保全部已 ACK 的新写。
- 持有本地 advisory lock 的导出权属要明确；不得让 candidate 打开同 data-dir 做 migration，也不能靠丢锁后竞争接管。
- cmd/tether/serve.go:312–330 现路径先让 b.Run 完整关闭再 exec；该路径不能直接用于保业务交接。
- 本地 raw pumps 恢复不应等待网络选举/JS readiness/SQLite migration；控制部分就绪前请求走健康入口/可靠重试。
- N≥3 可逐节点保持 quorum；N=1/N=2、单 Caddy 仍须产品化临时冗余/本机接续和独立成功 drill。
- NATS LDM、Caddy binary 替换由各自产品机制协调；tether 的同 PID 不能替它们保 Go 私有连接状态。
- 临时 broker 精确 release join 门、不兼容 schema/cross-epoch 拒绝门、真正 N-1 ctl 能力门全部保留。
- 保留 §21 的宿主共享二进制升级域锁与 SHA 证明；磁盘路径已换不代表所有共享该路径的运行实例都已升级。

## 6. systemd 与预算定义

systemd Type=simple 的 MainPID 不因原进程 exec 改变；现 broker/reexec.go:21–26 已依赖此行为。
无需 systemctl stop/restart 来升级该进程；若这么做，control-group 清场会破坏业务父子资源。
正常 stop/kick/evict 仍须清场，不能为热更新全局改 KillMode；user unit、无 systemd/setsid-nohup 均保留。
unit 示例 test/simcluster/image/units/tether-agent.service:12–19；systemd 自动重启只证明故障恢复，不证明连续性。

**freeze 从旧业务 pumps 停止到新 pumps 真正恢复计时，包含 exec、Go init 和 import。**
64MiB manifest 上限不等于可在 500ms 内生成/导入；准备工作须前置且有一致性末屏障，按真实最大支持 profile 测量。
候选 init 依赖磁盘/DNS/远端 readiness 无法直接给出 500ms 证据；需要控制启动路径，并对准入容量给出支持边界。
5s 请求预算从原 ctl 发起算起，包括断链探测、重连、排队和重试；正常 NATS 重连退避不是该预算的证明。
requestID/seqACK/resume 能保持原调用，截止时间不能续期；新请求不得被丢给已 freeze 的唯一入口后提前终态失败。
本地 500ms、原流 5s、组件120s、全栈15min 是不同区间；不能用较大交接预算掩盖业务全程冻结。

## 7. 本裁决要求的验收（全部尚待接线实现，原语通过不替代）

| 能力 | 自动测试/真实 reexec | deploy-tier simcluster 与收据 |
| --- | --- | --- |
| 同 PID manager 导入 | 普通 exec+PTY、pending exit/reply/cancel、原 TCP、active/prepare transfer；真实新旧 binary+race | 原 PID/父关系/原流/同 transfer 保持，同时全部旧映像退出；至少5轮资源门 |
| 单 owner/identity | 同 lineage 候选负控制、迟到旧代 callback、重复/异 payload requestID、满额背压 | 持续≥10请求/s，重复副作用0、失败0、每次≤5s；lease NID 不变 |
| 早期失败/回滚 | Cobra/YAML/import/依赖启动失败、exec 返回错误、激活后新工作再回滚；逐阶段故障 | systemd 与 nohup；保持原业务并真回 prev，记录不可保任意进程终止的独立边界 |
| broker/store | import 前禁止 orphan reaper、丢 commit ACK、late finalize、反向迁移拒绝、单写锁 | N=1/N=3/N=2过渡独立；ACK写不丢、唯一终态、原对象/目标inode |
| 四层与预算 | carrier断开后的原ctl调用、窗口背压、freeze超时defer、500ms计时跨exec | 真NATS/Caddy binary替换，永久SSH继续而旧worker退出；每层120s/全栈15min |

每能力继续执行 unit/integration/race/N-1/real-process → 真 drill → 原始收据 → 活文档/审查归档硬闸。
本轮不分配新 drill 编号；既有原语测试/30s fuzz 是已提供的基础证据，不能填写为上述验收已完成。

## 8. 固定三路交叉

- transport lane 已同意：accept/read/write 必须先归表再 freeze；exec 后不能称 unfreeze old owner；任意 fatal 属崩溃边界。
- agent lane 已同意：同进程可行，联合 procs/exits/courier/request 快照、sole wait-owner、全入口屏障不可省。
- 已向两 lane 提出：Restore 消耗包的 rollback 接线、包含 Go init 的 500ms、首次 register 禁空表和 5s 可靠 ctl 探测预算。
- 已亲读 transport 111 行稿：认可 partial read/write/accept 归表、授权期限先导入、256MiB 重放与64MiB manifest须独立payload FD、候选预检不等于新堆预热。
- transport 已亲读本稿并认可；建议代码状态显式区分 resource-active 与 control-ready，杜绝按后者判断可复用初始快照。
- 已亲读 agent 正式稿并认可：sole wait4(WNOHANG)+屏障内 reap→exit 状态事务满足计划内保证，WNOWAIT可选；可靠ctl未完成前不开活跃升级。两lane均已亲读本稿ACK，无新增阻断。
- 建议 root doc-first 将计划“必须 IPC”改为“职责分离、同进程优先”，并保留上述全部验收；三路均只作架构裁决，未宣称H1已交付。
