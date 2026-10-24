# Agent 全业务热更新可行性草案

状态：只读可行性分析，尚未实现、未执行热更新验证，不是 Pass 收据。2026-09-20；基线 HEAD `cab90d1dcafe6badc4d68a9492c2a2b2ad6cc025` 加当前未暂存 control-connection-recovery 候选。此功能与当前五缺陷修复分开。本文由 transfer 内审 lane 草拟，root 定稿 plan。

## 1. 结论与目标

**可以把“计划升级不终止已接纳业务，且新控制请求持续被服务”做成产品契约，但现有 self-exec 不具备该能力。** 最彻底目标需要拆分 agent 控制逻辑与业务运行层，并让后者也具有明确的状态/FD 接管协议；还要与 broker、NATS、Caddy 的连续服务方案共同交付。只保留旧 worker 到业务结束，能实现一部分无中断升级，不能宣称全部组件已升到目标版本。

建议正式计划区分三个可观察完成点：

1. **新版本接管控制**：新请求已由目标控制版本处理，旧请求的归属不变，唯一写者已切换。
2. **业务运行层迁移完成**：活跃业务的 PID、原有外部连接、传输 ID 和可观测性连续，已转到目标 runtime；允许有明确上限的排队/背压延迟，不允许业务重启、流结束、字节丢失或命令重复。
3. **全版本收敛**：所有本次升级范围内的旧 control/runtime/transport 进程与旧映像都退出，FD、consumer、goroutine、旧凭据和旧升级锁回收。未迁移的永久任务不能偷偷从统计中剔除。

“零业务中断”不等于零延迟、所有底层 TCP 不发生重连、宿主故障也无影响。若还要求所有旧运行层在固定期限内退出，而运行层不支持活跃状态迁移，面对无限长 SSH/PTY/任务，这组要求无法同时成立。最彻底路线必须建设迁移机制，不能用无限期排空作最终答案。

## 2. 当前行为：保留什么、断开什么

以下为源码静态核验；本轮没有实际执行 self-exec 并测量业务中断，因此不宣称故障注入已经跑过。

| 对象 | 当前升级时的真实行为与证据 |
| --- | --- |
| 升级状态机 | `docs/distributed-broker-architecture.md:873` 的 §21.3 是下载/校验/冒烟/prev/marker/rename/exec 和失败回滚契约；`internal/agent/upgrade.go:230` 先回执，`:301` 在 100ms 后 `syscall.Exec`。没有活跃业务登记、准入切换、排空或 FD 移交。 |
| PID、节点身份 | self-exec 保持 agent 的 OS PID；`upgrade.go:236` 明述 NATS 连接断开。`instance.go:48`、`:181` 通过环境保留 lineage 与 routing NID，防止升级后被当成克隆改名。这不保留 Go 堆、goroutine、订阅或业务状态。 |
| 非 PTY 进程 | `exec.go:181`/`:185` 建 stdout/stderr 管道，`:227` 只登记内存 child 表，`:232` 泵输出，`:236` 等待退出；`:399` 仅 Setpgid。self-exec 本身不等于显式 kill 全部子进程，但管道读端关闭后，写端可能遇到 EPIPE/SIGPIPE，结果取决于业务程序。stdio 已自行重定向的进程可能继续运行；这不意味着其管理、输出或退出码连续。 |
| PTY 进程 | `internal/pty/pty.go:60` 将 master 保存在 agent；`:126` 将 slave 给子进程；`:139` 设置 Setsid+Setctty。agent exec 丢掉 master 和会话对象，交互链必然丢失，并存在终端 hangup 导致前台任务退出的风险；不能把 setsid 当作 PTY 保活证明。`:315` 对终端 hangup 语义明确说明：tmux attach 可退出而 tmux server 留在后台，忽略 HUP 的业务可能继续。 |
| 输出、输入、resize、KA | `run.go:270` 登记内存 procRec，`:300` 建 master→bus 泵，`:307` 等待子进程；`:283` 的会话级 PTY 订阅只解决同一 agent 进程内的 NATS 重建。`exec.go:288` 已明确输出在连接切换时没有缓冲，可出现缺口。self-exec 会同时丢掉这些对象，现有机制不能恢复。 |
| ps、kill、退出码、审计 | `agent.go:780` 每次启动都新建空 procs/execChildren；`:1781` 的 G.1 snapshot 来自内存 PTY 表。`proc_delivery.go:13` 明确只用内存 courier，不是落盘 journal；`:25` 明确进程崩溃会丢真实退出码。broker `reconcile.go:294` 把未上报的旧 RUNNING/LOST 行收为 EXITED(-1)。即使业务 OS 进程活着，也可能失去正确管理和状态。 |
| 文件传输 | tier-B prepare 只进入内存 `pushCommitCache`，`transfer.go:142`；`:235` 明述 prepare/commit 之间重启返回 transfer_unknown。正在写入的 temp FD、哈希、reader、consumer、context 在当前进程内，见 `:256`、`:329`、`:336`、`:341`。exec 不执行 Go defer，不能指望 Abort/Close 完成清理；新进程也没有恢复记录。原子 rename 保护目的文件完整性，不保证本次传输不中断。 |
| SSH/expose/proxy | state.json 仅保存端口、proxy、roster 等配置，`state.go:87`；它能帮助重建端口，不保存既有流。`tunnel.go:1321` 的 net.Conn、TLS/yamux 会话均在内存；`:1813` 创建 TLS 连接；`:2058` 的 bridge 任一方向结束就关闭两边。重连 tunnel 不等于原 SSH TCP 会话续接。 |
| spawnexec helper | `spawnexec/helper.go:224` 明确 helper 直接 Exec 成业务目标，不保留第二层 worker。它隔离 execve 卡顿，不是可独立升级的常驻任务 supervisor。 |

FD 关闭论据也核对了当前依赖实现：creack/pty v1.1.24 `pty_linux.go:14` 使用 os.OpenFile；本机 Go `os/file_unix.go:260` 打开时带 O_CLOEXEC；`os/pipe2_unix.go:16` 的 Pipe2 同样带 O_CLOEXEC。并没有发现本项目为热更新显式继承这些业务 FD 的路径。不同托管业务收到管道错误/HUP 后的具体反应应由真实 drill 验证，不宜从源码泛化为“所有进程都退出”。

当前 `requirements.md:308` 的控制组件故障继续运行要求与 `:335` 的 N-1 互通要求，不能用来证明已经实现 agent 活跃业务热迁移。需求文档中一些旧 run/exec 名称与现实现反向；本文按真实实现区分：`tether run` 是 PTY，`tether exec` 是非 PTY。

## 3. 推荐分层及单写者约束

### 3.1 分出三个责任

- **唯一入口与所有权协调器**：拥有逻辑 NID 的命令接纳权、请求去重、task→owner 目录、升级代号、切换日志和完整 register snapshot。新旧控制逻辑通过有版本的本机 Unix socket IPC 调用它，不各自订阅同 NID。控制逻辑可新旧重叠启动，先健康检查再原子切换新请求路由。
- **task runtime**：持有业务子进程父子关系、PTY master、stdin/stdout/stderr、wait/reap、KA、signal、退出 journal、传输 context/temp FD 与 commit 归属。任务 ID 的归属跨控制版本保持稳定，ps/kill/stdin/resize 总是路由至真实 owner。无论任务跨多少版本，都继续遵守权限与撤销决定，不能把排空旧 worker 留成无人控制的孤岛。
- **连续流/连接层**：维护公网端与业务端的 TCP/PTY 字节流身份、收发序号、ACK、重放与背压；TLS/yamux/NATS carrier 是可替换承载，不能再直接等同于业务流生命周期。它与 broker 侧共同保证 SSH 外部 socket 与 agent 本地目标 socket 不因 carrier 升级被关闭。

这些可以先是少量进程而不是每个任务起一个大 Go daemon；但责任边界、IPC 兼容、资源限额必须真实可执行。常驻层不意味着永不更新，见下一节。

部署生命周期也必须分层：`agent.go:654–656` 明确现有 systemd 默认 KillMode=control-group 会在 agent 退出时清理其 cgroup；simcluster 的真实 agent unit 同样没有覆盖。把 runtime 作为 controller 子进程放在同一可被整体 stop 的 unit 不形成隔离。要让部署/停止单位与所有权匹配，既覆盖 systemd/user unit，也覆盖受支持的 setsid-nohup 环境；不能只改 KillMode 放任无管理孤儿，evict/kick 仍须完整清场。

### 3.2 不能简单双开同 NID

`instance.go:60–67` 已明确：两个进程复用同一 instance ID，会被 broker 当成同一个实例重连，获得同名并同时订阅，导致命令 fan-out。另铸 instance ID 则会被分配 suffix，改变节点身份与任务归属。两者都不是接管协议。

必须存在唯一接纳点和 generation fence。每个 task 的准备、创建、续写、rename、退出事件都有同一 owner/epoch；切换前冻结旧版本的新接纳，处理在途分派，并把所有已接纳请求登记完毕；切换后旧版本只处理其已归属工作，不能重新执行新请求。发送不等于对方接纳，超时也不等于未执行：需要请求 ID 和持久结果/去重语义覆盖重试窗口。

最小局部实现可让稳定协调器独占现有外部 NATS 订阅；若协调器/NATS 本身也要替换，则必须升级为可恢复的所有权日志与 broker 认可的 epoch 或等效 fence。仅 Unsubscribe+Subscribe 不能证明在途副作用不重复。generation 不是权限凭据，IPC 要核验 Unix peer/进程身份，且不能复用共享 NFS state.json 作为宿主唯一锁或实例身份来源。

G.1 register 必须报告所有仍活跃 runtime 的联合 snapshot 和尚未确认退出事件；新 controller 的“空内存表”不得触发 broker 把旧任务误收为 EXITED(-1)。NID、task ID、OS PID、boot_id/start_ticks 是不同身份，需分别保持与核验。

## 4. runtime 自身也升级：两条机制共同交付

### 4.1 排空是过渡机制，有真实状态

新任务进入目标 runtime，旧任务继续在原 owner 完成，控制与新请求不中断。有限 transfer 最适合先按此实现；prepare 之后尚未 commit 的传输也是活跃任务，不能只数正在 io.Copy 的 goroutine。

排空要可查询：旧版本、剩余 task/连接、最老时间、原因、资源占用、预计/不可预计完成时间。不得在期限到达时偷偷 kill 旧任务并声称无中断。资源达到上限应推迟本次升级或停止追加升级代，而不是驱逐活跃业务。无限任务意味着该版不能仅靠排空达到“全版本收敛”。

### 4.2 永久进程/PTY/SSH 必须支持显式移交

不能序列化任意 Go 堆或直接复制 `tls.Conn`/`yamux.Session`/`exec.Cmd`。应为可迁移状态定义专门 ABI：任务身份、可信 PID/start_ticks、FD 清单、读写游标与待发 buffer、退出事件序号、KA 状态、权限/取消状态、transfer 校验/commit 状态；用两阶段移交和 durable 接管决议避免两边读同一 FD 或两边 rename。

- **子进程父关系与 wait**：单纯 SCM_RIGHTS 传管道并不能让新进程 wait 一个非自己的子进程。可选最小 supervisor 保持父 PID，通过可恢复 self-exec 保留显式 FD 与任务记录，重建 wait/reap；或设计 subreaper/收养协议。旧 waiter 已收割但退出事件未交付的窗口必须由 journal 闭合。pidfd/PID 三元组帮助防 PID 复用，不能替代父关系与退出码记录。
- **PTY/普通 pipe/raw TCP**：在安全点停旧读写，移交 FD 与用户态未消费 buffer；新方确认完整状态后成为唯一 owner，再撤掉旧方。允许短暂背压，不关闭业务端。既可用受认证 Unix socket+SCM_RIGHTS，也可用受控 exec FD 白名单；必须防非业务秘密 FD 意外继承。
- **TLS/yamux/NATS 等复杂连接**：只移交底层 TCP FD 不保留密码序号、解密 buffer、mux stream/flow-control、NATS subscription/RPC 状态。优先新建 carrier，再将有 ID/seq/ACK 的逻辑流迁移过去，保留两端业务 socket；迁移完成后旧 TLS/yamux 进程可退出。若选择导出协议内部状态，必须独立设计并固定库兼容 ABI，工作量与风险更高。
- **文件传输**：第一阶段让旧 runtime 完成完整 transfer；最终需要跨 runtime 移交时，保留固定 parent-dir FD、temp inode、累计字节、摘要可恢复状态或重算方案、对象 sequence、consumer/window 归属、取消/预算和最终结果日志。rename 前后丢 ACK 的窗口也必须有唯一提交与可重查结果，不允许“重试就再写一次”。
- **runtime 的 runtime**：若仍用永不升级的 guardian 才保住业务，就没有完成“所有旧组件退出”。最小协调层也要有同 PID 可恢复 exec 或自己的 FD/日志接管协议；用清晰可枚举状态把最后一层做小，并把它的升级也列入 drill。现有 `reExecInPlace` 不能直接承担此角色。

以上可实现“旧代码退出、业务 OS 进程和用户连接继续”，代价是可测量的移交延迟与明确受支持的状态 ABI；不能承诺对任意未设计的业务内部状态做活体升级。业务程序自己的版本变更是另一层应用能力，不由 tether 替它迁移内部堆。

## 5. 最小交付顺序与初次迁移

用户已选择全链路最彻底路线；以下是工程里程碑，不是删减最终范围。

1. **建立契约与可观察性**：列清所有任务/流，升级事务、owner epoch、结果日志、版本状态、排队/迁移期限与资源上限；补黑盒 red drill 证明现有 exec 会损失业务连续性。正式 plan 固定延迟预算，不先凭空给数字。
2. **引入 runtime + 单入口**：让新创建进程/PTY/transfer/tunnel 进入分层；控制版本交接时已有业务原 owner 继续，新请求由新逻辑处理，G.1/ps/kill/退出码完整。此阶段是可独立验收的最小热换交付，但不是全组件完成。
3. **逻辑流可迁移与 runtime 交接**：补 seq/ACK/replay/背压、FD/父关系接管、transfer commit 去重与状态恢复，使无限长进程/SSH/PTY也能脱离旧 runtime。协调器自身的更新也验收。
4. **与全链路滚动升级联测**：broker/NATS/Caddy 新旧共存、路由/订阅切换、carrier 重建、共址 agent，确保升级过程控制请求持续完成。N-1 IPC/协议、回滚、连续多代、旧版本最终清零全部进入出口门。

首次从现有 monolith 引入此结构时，旧进程已经把 FD/Go 状态封装在没有导出接口的内存中。不能保证只发一次现有 `node upgrade` 就迁走所有正在跑的任务。可安全做的 bootstrap 是在明确维护/自然任务边界安装支持版本，并对当时活跃的无限任务报告无法迁移；也可另行研发受支持的旧版本适配迁移器，但没有该机制和独立证据前，不能宣称首次无损。不能用测试环境提前装好新 runtime 来掩盖这道生产入口。

## 6. 升级安全与版本兼容必须延续

保留 SHA、冒烟、原子落盘、prev、宿主升级域锁、回滚等 §21.3 保障，但现有“新 agent register 成功即 committed”不足以证明业务接管完成。新事务应分别记录 control 已接纳、runtime 移交、所有旧版本退出；回滚先恢复唯一新请求路由，不撤销已对业务执行的命令或已成功 rename 的文件。

同宿主共享二进制的进程要使用版本化不可变 executable slot 或等效机制，不能让新旧 worker 意外以同一路径的最新字节重启。marker/boot budget 的作用域也必须从单目标 self-exec 明确扩展到目标组件与代号，不把正常多 worker 拉起误算成三次失败。

当前 N-1 只保证相邻 release，`requirements.md:335`。若旧 runtime 可跨很多版本驻留，要么承诺并测试更长的 IPC/runtime ABI 兼容窗口，要么在下一代升级前先迁走旧任务；不能用“wire additive”推导任意多代二进制可共存。安全修复要求旧代码限时消失时，只有实际迁移能同时满足无中断；不能留下旧弱点进程然后显示升级完成。

## 7. 必须落在 test/simcluster 的验收矩阵

所有行是待实现、待运行的要求。遵守 simcluster Mandate：真实 systemd/sshd/独立 NATS/安装权限与磁盘；测试可以制造负载和故障，不能替产品手动搬 FD、重启遗漏服务或更改权限来完成升级。源码单测/race 配合，但不替代部署级 drill；每个断言要有正负控制与实测身份，最终报告按真实结果写 GREEN/PRODUCT-RED/INCOMPLETE 等。

| Drill 责任 | 不可省的独立 oracle |
| --- | --- |
| 长寿命普通进程 | 升级前启动真实业务，连续记录 PID+boot_id+start_ticks 与业务自报 nonce/单调计数；覆盖 stdout/stderr、stdin、stdio 自重定向、fork 子树、正在退出等形态。升级全程不重启、不重复执行；ps 仍 RUNNING，kill 精确作用原进程，结束后退出码/审计只一份。 |
| PTY | 真实终端双向有编号交互、resize、Ctrl-C、KA，覆盖 shell 和不忽略 HUP 的前台任务；用户流不 EOF，无缺号/重号/乱序，PID/start_ticks 不变；移交期间不误触发 liveness reap，之后还能操作并正确退出。不能只测 tmux 后台进程存在。 |
| 输出与日志 | 分别证明业务自己落盘日志连续、tether 实时输出完整、生命周期事件可靠。既有业务日志不属于 tether 持久化承诺，不能拿用户文件完整代替 stdout/PTy 流完整。用序号/摘要与发送端记录对账，不用“有最后一行”当全量证明。 |
| SSH/tunnel | 真实 sshd 上建立长会话及大双向数据流，保留外部连接/会话 nonce 和 server 侧原 PID；升级中不断发送探针，拒绝自动重连伪造的“恢复”。原连接不 EOF/reset，内容连续，最大阻塞时间受预算约束；同时新 SSH/新控制请求成功。 |
| transfer | 覆盖 tier-A/B 双向、66,549,440B 案例、慢速与并行；对 prepare 已确认但 commit 未到、下载/上传中、hash/sync 中、rename 前后、终态回执前后逐窗升级。TransferID 不变、目标仅一次原子生效、完整 SHA/长度、无成功前目的半文件、无丢终态与双审计；观测窗内控制 RPC 一直完成，不能只看最终 complete。 |
| 单所有者与 G.1 | 连续发送有唯一副作用记录的命令，同时递送重复消息/重放旧 epoch；真实副作用计数每个请求恰一次，新旧接管时只有一个能发新任务/commit。各 runtime 的联合 snapshot 始终保留在活业务，禁止 RUNNING→EXITED(-1)→RUNNING 抖动；旧实例不能凭旧 token/epoch 恢复写权。 |
| 每个 handoff 窗口 | 在 prepare/snapshot、FD 发出、接收未 ACK、ACK 已发未持久化、owner 决议、旧 FD 回收、exit 已 reap 未 journal、journal 未 ACK 等可观察屏障故障注入。计划内可确认 freeze 的交接必须无业务损失；新方失败时只有仍真实持有完整状态的旧方才可继续。旧方/协调器被 kill-9 的每个窗口必须独立给出可恢复性证据或明确能力边界：普通 TCP/pipe write 已成功但 offset 未记账、wait4 已 reap 但退出码未 journal，都不能仅靠 FD+owner 日志保证恰一次/不丢失；不得把旧方已死写成 fallback 继续。IPC 断开/截断、可恢复重启仍需证明无双主、无假成功。 |
| 回滚 | 新版本校验/启动失败、接管前失败、部分任务接管后失败、新请求已执行后失败分别验证；不得因 rollback 重跑已执行命令。原任务、控制请求与跨版本 ps/kill 持续；检查运行映像 SHA，不只看磁盘或 register 版本。 |
| 延迟与限额 | 预先固定新控制 RPC 成功率/最大延迟、PTY/SSH 最大静默、transfer 最小进度窗口、切换预算；实测升级前/中/后完整时间序列。压测 FD、进程、goroutine、内存、磁盘 journal、重放 buffer、consumer 数量与无 ACK 接收方，验证背压/延期升级策略不杀业务、不无限堆积。 |
| 多代与旧代码退出 | N-1 正向/回滚与持续 N→N+1→N+2 升级、跨两代永久任务、旧版本卡在排空、另一次升级并发、同宿主多个 sid/NID/共用二进制。逐进程核验 `/proc/PID/exe` SHA 和状态记录，升级完成后旧 runtime/transport/协调器映像与关联 FD/consumer 全清；只因 CLI 报 target version 不算完成。 |
| 首次 bootstrap | 从真实发布的当前 monolith 启动活跃业务，再执行产品提供的迁移入口，证明其能力边界。若策略要求自然结束/维护窗，则 drill 必须验证明确延期/拒绝和任务不中止，不把后续已分层版本的绿收据套给首次迁移。 |
| 全链路组合 | 把以上真实负载贯穿 standalone/HA、普通 agent/共址 agent、TCP/WSS、broker/NATS/Caddy 的升级与回滚。单组件绿之后仍需复合升级同一业务连续性 oracle；旧端兼容、单 broker 容量边界与连续新请求预算必须写入正式矩阵。 |

建议负控制：恢复现有无移交 self-exec、允许同 NID 双订阅、删一条任务 snapshot、跳过一次 stdout chunk、handoff 双写同 FD、提前关旧 TLS carrier、跳过一份旧映像清理。各 oracle 必须确实报红，不能只用资源最终归零或新连接能建立来通过。

## 8. 明确边界与待定设计

- 当前代码的证明止于静态生命周期；新分层/移交协议均是设计建议，不是已存在机制。没有热更新 drill 的完成证据。
- 保持业务 PID/连接/字节与有限升级额外延迟可制定契约；零额外延迟不可作为诚实承诺。具体 SLO 要按现有 tier、最低资源与目标并发确定并实测。
- 宿主掉电/内核重启、真实业务程序崩溃、网络完全不可用等不由一次软件接管解决；故障矩阵必须区分受支持计划升级故障与这些边界，不模糊成无限可用性承诺。
- 任意跨纪元旧任务若没有可用 adapter，不能无损强制迁移；要保留兼容承载并显示未收敛，或延后该次升级。用户/安全政策若要求强制退出，应是明确的另一种有中断操作，不能藏在热更新超时处理里。
- 单台服务把唯一入口进程停掉再启动，无法由 agent 单侧保证所有新控制请求连续完成；必须依赖其他 lane 的重叠接纳/队列/HA 或受控短延迟协议。本文不替 broker/NATS/Caddy lane 假定已经具备。

## 9. 固定三路交叉质疑记录

- transport lane 指出任意 kill-9 不能套用计划内 freeze 的连续性证明，已采纳并收窄 §7 对应行。尤其 write/记账和 wait4/journal 跨 syscall 窗口，单纯持久 owner 决议不提供事务性；每个故障窗必须分别标可恢复或边界。
- 已独立阅读 transport 草案：认可 raw SSH socket 同机移交与内部 TLS/yamux carrier 重建相分离；NATS/Caddy 连接更换只有配合逻辑 stream/transfer 续接才对业务透明；保留旧 worker 排空不作为 fully-upgraded。
- transfer 交叉补强：锁定 SDK `jetstream/object.go:651` 每次 Put 分配新 NUID，`:802–805` 成功覆盖后删除旧 NUID chunks。整对象重试需固定版本并协调旧 reader，不能同名重 Put 后让旧 reader 继续读已被清除的对象。broker `transfer.go:1524` 按 sourcehome 内存 tracker 决定 continuation 应答，`:1556` 在转发前改变 terminal 归属；`xfer_inflight.go:879–897` 的启动恢复可合成 home_broker_restart 并删除对象。因此 handoff 必须同时移交 tracker、commit/terminal 归属、期限与 reaper 资格；drill 应交错旧/新 home 的迟到 terminal、creator finalize 和清理，证明不会删活对象或写两个终态。延长 TTL 或只保证对象仍在不够。
- 已读 system 草案并认可其 authority/DB/兼容矩阵。采纳候选 readiness 禁止对在用权威库运行 migration/夺写锁，以及 freeze 遇阻塞 write/D-state 须有限等待并延期，不可为达升级 deadline 关闭业务 FD/kill 任务。反向质疑：永久 SSH 的最终门必须在原连接仍活跃时完成 runtime 迁移与旧映像退出；“结束 SSH 后才 completed”只证明过渡排空路径，不能作为最彻底目标的最终验收。

## 10. 给 root 的有限架构裁决及首批接线职责

**建议选择 Linux 同 PID 可恢复 exec 的小型 runtime 作为业务资源 owner，controller 为可替换子进程/控制模块；不选择复制整份 Agent 内存或任意 Go TLS 状态。** runtime 保留父 PID、子进程关系、原业务 raw FD；控制/tunnel carrier 在重建后按逻辑 ID/seq 续接。最终 runtime 自身从原 PID exec 到目标映像，在明确 safe point 暂停泵、导出有限状态、白名单保留 FD、导入后恢复；这才使无限业务无需自然结束也能让旧映像消失。新请求的短暂等待由仍在线 controller 有限排队吸收，必须满足冻结的绝对请求延迟预算；需要权威 runtime 执行的请求不应提前虚假 ACK 为已执行。

最先实现的生产职责和现接线位置：

| 最小职责 | 当前接线与必要改动边界 |
| --- | --- |
| 启动/角色/生命周期 | `cmd/tether/agent.go:368–395` 当前把 signal ctx、tunnel adapter、Agent.New/Run 全放一进程。先定义 parent runtime 与 controller 的角色、受认证 IPC、版本 slot、服务停止语义。runtime 作为真实受监督 MainPID；平常 controller 更新不 stop 整个 unit。两角色共享一个安装产物可以，运行角色与 SHA 必须可观察。 |
| 接纳与执行分离 | `internal/agent/exec.go:35–105` 的 NATS callback 目前直接 go 每个 handler。改成唯一入口先记录 reqID/任务 owner，再通过 IPC 分派；kill/resize/KA/取消按 task 路由，不随 controller generation 丢弃已接纳任务。ClaimProbe/lineage 仍只有权威入口响应。 |
| 子进程与 I/O owner | `exec.go:159–236`、`run.go:75–342`、`pty/pty.go:43` 的 cmd/pipe/master/wait 移到 runtime，controller 不持不可恢复业务资源。尽量显式建 OS pipe/PTY FD 与有限 buffer，避免依赖 `exec.Cmd` 隐藏的 copier goroutine 状态。 |
| 可恢复 wait 与事件 | `run.go:307`、`exec.go:236` 的 wait 和 `proc_delivery.go:13` 内存 courier 需统一 owner、退出 journal 和 ACK。同 PID exec 保父关系，但不能恢复旧 exec.Cmd 对象；重启后按可信 task/PID/boot/start_ticks 恢复 wait。可进一步评估 waitid(WNOWAIT) 先观察、journal 后 reap 来缩小退出码窗口，不能未经原型验证就承诺任意崩溃不丢退出码。 |
| freeze/export/import | runtime 管理每个 pump/commit 的安全点；等待在途 syscall 返回并冻结 buffer/offset、signal/exit事件、task表后，生成带 schema/代号/校验的状态 manifest 和 FD whitelist。显式清除选定 FD 的 CLOEXEC，exec 后验证 FD 类型/身份再接管并恢复 CLOEXEC。导入必须早于正常 Agent.New/register/孤儿清理；错误时不得跑旧启动路径将任务表清空。超时/不可导出状态时延期，业务继续。 |
| G.1 与清场 | `agent.go:1781` snapshot 改从 runtime 权威表读取，`run.go:554` reapManagedChildren 通过同一运行层清场；`run.go:836` 的 PTY intake/KA 只更新运行层。升级、连接重建、用户主动 shutdown、evict 四种意图必须分开，升级不触发清场也不丢撤销命令。 |
| transfer owner | `transfer.go:142` prepare cache、`:256` session-bound ctx、`:329–364` reader/temp/hash/rename 移为 task 状态。业务任务 ctx 独立于 controller/NATS session ctx；carrier failure触发受预算限制的resume，而非直接取消完整业务。先实现完整事务同owner继续，再实现显式checkpoint/幂等终态；必须同时改broker reaper/owner协议。 |
| tunnel carrier 与 raw stream | `tunnel.go:1321`、`:2033–2075` 将 raw业务socket与TLS/yamux lifetime解耦。保业务socket，序列化自己的streamID/两向offset/ACK/buffer/half-close，重建carrier后继续；不能直接复用现有任一方向结束即Close双方的bridge。 |
| 升级事务 | `upgrade.go:301` 与 `upgrade_state.go:382` 两个 exec入口都接同一个runtime移交流程，包括rollback；artifact安装保护可复用，健康commit改成分阶段证据。不能只替主升级入口却让watchdog rollback走旧的无FD继承exec。 |

必须先用真实进程原型证明：同 PID runtime exec 后原 PTY/pipe FD 与业务 PID/父关系均存在，退出码只回收一次、全部 buffer有归属、错误导入不误清场；然后接真实 NATS/ctl/tunnel。原型可支撑工程方向，但正式完成仍需 §7 全 simcluster 矩阵。

### 10.1 终端消费者也是能力前置

`proto/messages.go:555` 的 ExecReq、`:802` 的 RunReq、`:894` 的 KillReq 没有稳定 request ID；当前 ExecChunk/PTY 流不具备此目标所需的端到端 seq/ACK/resume。若 NATS/Caddy 升级时 ctl 端连接也换代，仅在 agent/controller 内部保存数据仍不能让旧 ctl 补收缺口或去重请求重试。

因此 root 正式 plan 应纳入 ctl 协议/消费端改动或等效可靠边缘代理，并加能力握手与激活门：桥接代 ctl/runtime/broker 全具备支持时才承诺严格连续性。相邻 N-1 普通互通继续保证，但旧 ctl 没有实现新增 ACK/恢复语义时，不能自动被算入热更新连续性的支持集合。正式矩阵须分别证明正常兼容与严格热更新能力，缺能力时产品在激活前报告/延期，不能先切断连接后让用户承担缺口。
