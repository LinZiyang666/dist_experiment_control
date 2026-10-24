# H0 Caddy 补丁建议与 handoff 连接层核查

2026-09-20。仅本 lane 的临时建议、源码审查和隔离探针；不是功能/最终内审 Pass。未改生产、已有测试、Dockerfile/lib 或清单；未起容器、systemd/现网服务。root 正在独立实现 `internal/handoff`，最终代码/测试归 root 统一裁决。

## 1. 交付件与已执行范围

| 文件 | 内容/实际状态 |
| --- | --- |
| `/tmp/tether-control-recovery/h0-caddy-proposed.patch` | 5 文件/183 行的供给原型建议；`git apply --check` 通过，未应用。SHA `a52f7c40c51a46cb4a93a7313ec79edd6b39d8e5126446766b5f69bf54e9e5c4` |
| `h0-caddy-stage-proposal.sh` / `h0-caddy-fixture-proposal.sh` | 上述新 helper 的独立副本；分别 `sh -n` 通过 |
| `h0-caddy-stage-probe.py` / `.log` | 全本地假下载源，真实 tar/SHA/ELF检查：有效包、错checksum、匹配checksum但错ELF、错CPU目标、非法version 5 臂通过；未下载真实Caddy |
| `h0-caddy-address-probe.py` / `.log` | 仅在临时目录应用 patch，再用假的 machine 函数验证 legacy NATS/WSS显式addr/WSS缺addr拒绝/IPv6显式addr 4 臂通过；未调用真实服务 |
| `handoff-transport-probe/socket_test.go` | 独立真实 TCP + syscall.Exec 探针，`go test -race -count=3 -timeout=35s -v .` PASS 5.801s；并非 root 包的测试通过收据 |
| `handoff-transport-socket-probe.log` | 上述原始日志，SHA `ee5a818c859ded62c0f7bc8def03ef70e0e982625ba8cc7a741248d00bef7d08` |
| `handoff-transport-probe/memfd_exec_probe.py` / `handoff-transport-memfd-probe.log` | 隔离实际 memfd ELF exec 正例与 script 负例；无服务。log SHA `c97d6917b5c56273f48e8eb1f4d7433c294e2d4db64c43c095140e8213fb32e0` |

以上相对文件名均在 `/tmp/tether-control-recovery/`。网络/真实 exec 探针经授权在沙箱外只运行本地临时程序；基础服务供给本次未运行。

## 2. Caddy 原型接线及应用前要补的内容

原型新增共享 `lib/stage-caddy.sh`：读取 installer pin/显式版本覆盖、从同一官方 release 取包与checksum、验精确archive SHA和ELF64 little-endian machine=amd64，记录binary/archive SHA与来源。当前sim其余binary固定Linux/amd64，故明确拒绝其他target；不根据remote driver的CPU猜部署CPU。新增 Dockerfile COPY 与真实 unit fixture；CA复用per-instance设施；只在原流READY前供给证书、配置和启动。供给后以真实WSS鉴权请求及`/proc/<PID>/exe`证明路径/映像。

`agent_provision_yaml` 提议增加第6参数显式`tunnel_addr`，保留旧NATS调用；WSS缺显式地址在任何供给前拒绝。四臂已本地验证，包含`wss://[::1]:443/nats`与`[::1]:7000`分开传递。不是把URL字符串删协议再冒充host。真WSS session/agent配置与发现入口必须在测量前完成，详见前份`hot-update-plan-check-transport.md`。

### 2.1 已告知 root：原型仍缺缓存和原子发布，不应原样宣称完整供给

1. **缓存**：原型每次下载，避免先引入信任陈旧vendor的问题。可加按`version/GOOS/GOARCH/archiveSHA`隔离的不可变缓存；命中也验证receipt、binary SHA和ELF。不能只跑`caddy version`或看到文件存在就跳过。坏cache明确重抓/拒绝；改pin/目标不能复用旧cache。
2. **原子发布**：当前最后的`install ... vendor/caddy`会截断旧文件。应先在同一vendor文件系统创建临时文件，写完/校验/chmod并按要求同步，再rename替换。下载失败、SIGTERM、校验失败不得破坏上一个可用artifact。
3. **两文件一致性**：binary与receipt是两个路径，分别rename并非一个事务。可用不可变版本目录+单一指针发布；如果保持现有COPY形状，应receipt最后提交，并在镜像构建前及image RUN重验binary SHA/receipt/target，任何中途混合状态都fail closed。root已有串行冻结vendor/image约束仍须保留；没有构建前一致性检查，不宜只依赖stage曾成功。
4. **部署CPU**：构建配置选amd64之外，真实容器`uname -m`/ELF执行/运行SHA也要与receipt闭合。改为arm64需一并改tether/NATS/CLI/image，不只添加Caddy分支。
5. **真实 readiness**：`caddy validate`/systemd active/可信TLS握手各自只是前置；最终还要真实ctl/agent WSS鉴权、持续控制与必要写入。Caddy WebSocket路径、NATSroute/JS副本及auth_callout互不代替。不能把8222当监控HTTP端口；installer:1244–1247是WS，cluster监控设施用独立8223。

建议stage正式hermetic门增补：温cache命中、cache内容被改、版本/arch切换、下载中断、写temp失败、binary已rename但receipt未commit时build拒绝、同vendor竞争被锁拒绝。上述缓存/发布故障尚未在本原型实现或运行，不编造通过。

### 2.2 SSH 探针的剩余误判风险

- 现有expose sentinel/curl每次新连接，不能复用为原流oracle。必须单次OpenSSH进程经真实expose公开端口，禁ControlMaster/autossh/应用重连；不能走`simcluster ssh`直达agent调试路径。
- 双向独立发送序号帧；只做echo有可能把一方向错误隐藏。记录接收高水位、总字节/hash、最大静默；发送端无最终目标数会导致丢尾被当成功，须最终序号/digest握手。
- client PID/同四元组单独不充分，须原TCP socket身份、远端helper PID/start_ticks/nonce与SSH认证会话无重建一同证明；EOF/错序粘性失败，后续重连不得抹去。
- 原流READY前必须有两方向非空基线；升级后继续足够长进度再收尾。空探针、早退、重连后同payload、升级只在流关闭后发生均应被负控制抓到。
- idle→升级→原socket继续、慢接收与TCP half-close单独测。SSH stdio EOF不等于TCP half-close；raw TCP probe验证一方向FIN后反向还可写。
- 新请求和file/PTY走NATS；SSH既有carrier独立。因此停Caddy/NATS不必让原SSH红，不能为负控制捏造预期，应由对应控制/文件/PTY oracle抓到现状风险。

## 3. handoff 的已证实文件标志风险与 root 裁决

### T-H1：File().Fd() 修改共享 O_NONBLOCK，破坏旧连接deadline — 已证实，root已规避

本机工具链Go1.26.8：`os/file_unix.go:69–81` 的`File.Fd`对网络File调用SetBlocking；`net.conn.File`返回dup且共享open-file description。Linux dup共享status flags/offset，而descriptor级FD_CLOEXEC独立。这既有官方文档说明，也由本次真实TCP反例确认。[Go File.Fd](https://pkg.go.dev/os#File.Fd)、[Linux dup(2)](https://man7.org/linux/man-pages/man2/dup.2.html)

- `TestFileFdMutatesOriginalSocketStatus`：原TCP `F_GETFL` 从`0x802`变`0x2`，NONBLOCK被清。
- `TestDuplicateMethodReadDeadline/unsafe_file_fd_true`：原连接设40ms deadline，却直到约250ms对端写入才返回`n=1, err=nil`。
- safe臂通过`SyscallConn.Control + F_DUPFD_CLOEXEC`复制，deadline仍按40ms返回timeout；变更dup的FD_CLOEXEC不改变原FD的该标志。

root当前`internal/handoff/image_linux.go:138–166`已从RawConn.Control复制，没有调用File.Fd。资源adapter也必须遵循该边界；不能包外先File.Fd再把已经受损的fd交进来。

### T-H2：caller原FD若本来可继承，会在exec后残留未登记引用 — 静态可达，root已加拒绝

复制fd并清duplicate的CLOEXEC，不能关闭原本也无CLOEXEC的caller original。它会同时穿过exec，manifest只列dup，隐藏原引用可能拖延socket EOF或pipe关闭。原代码无需恶意输入即可经raw syscall/OpenFile包装构造。

root采纳“不改变调用者策略，而拒绝非法输入”：当前`image_linux.go:145–152`在Control内先F_GETFD，非CLOEXEC返回error；配套`TestPrepareRefusesInheritableOriginalWithoutChangingIt`由root实现。本人已只读核实guard，不冒充已跑root反例。整体进程中其他未提交给Prepare的故意inheritable FD仍是runtime完整资源清单的责任；foundation不扫描/擅改全部FD。

## 4. 真实 raw TCP 同 PID exec 证明与限制

独立`TestSocketSamePIDExecAndHalfClose`用父探针对端持有同一个真实TCP连接，helper在原socket上报PID/socket inode；父发送gate+payload然后CloseWrite；helper仅先读取一个gate字节，剩余payload仍在内核，然后实际`syscall.Exec`自己。

- 正例：新映像立即恢复duplicate CLOEXEC，经`net.FileConn`接回原FD；同PID、同socket inode、未读payload完整、读到原FIN后反向写仍成功。
- 负例：不清duplicate CLOEXEC，真实exec后该FD不存在、对端原流关闭，被`EXPECTED_FD_MISSING`独立断言抓到。
- 不会新dial原业务目标；不会用“新流成功”代替。

本探针**不证明**TLS/yamux、bufio、应用泵、原netpoll deadline或业务request状态可自动恢复。原Go `net.Conn`的deadline和读写goroutine在用户态；resource adapter须保存剩余预算/绝对截止含时钟策略，并在新netpoll对象上重新设置。已读未写字节、FIN待处理、部分Write记账、TLS/AEAD缓冲需要显式逻辑状态；仅kernel未读数据会自然随socket保存。

首版探针有自身env重复键错误：raw syscall.Exec不会替我清旧`HANDOFF_PROBE_PHASE`，helper再次跑before，导致25s测试timeout。已改为替换旧键并加每个child/conn期限；原断言保留后×3通过，检查无遗留probe进程。该红是夹具bug，不计产品red证据。

## 5. sealed memfd 与可执行 memfd 是两种职责

### 5.1 状态memfd

状态memfd用MFD_CLOEXEC|MFD_ALLOW_SEALING，写完添加WRITE/GROW/SHRINK/SEAL再exec；它仅在受控继承窗口清CLOEXEC。Restore应在任何应用subprocess/业务goroutine前读取并恢复所有继承fd的CLOEXEC，验证PID/UID/nonce/schema/长度与实际fd身份后才交付owner。seal保证不可修改，不等于来源授权或抗任意崩溃的持久日志。[Linux memfd_create(2)](https://man7.org/linux/man-pages/man2/memfd_create.2.html)

当前root实现使用Pwrite/Pread，不依赖共享file offset；identity/status核验先于os.NewFile。应用恢复失败不得退化为空资源正常启动。init阶段若依赖会启动子进程，main里的Restore已经太晚；现有依赖/未来新增init都要维持这一前置。

### 5.2 可执行memfd：ELF目标可以始终保留CLOEXEC

独立Python程序把本机`/bin/sh`的ELF字节装入sealed memfd，保持FD_CLOEXEC=1，真实执行`execve('/proc/self/fd/128',...)`。结果：PID从1311568到1311568不变，新映像观察fd128已关闭，exec成功；说明**ELF执行文件fd自身不用加入长期继承清单**。这是Linux execve语义的实际验证，不是spawn替代同PID exec。

相同路径换成`#!/bin/sh`脚本，exec已经进入解释器，然后解释器因fd关闭找不到路径，退出2；原映像已消失，不能把这种错误当作exec syscall返回后可unfreeze。正式目标应限制经验证的真实目标架构ELF。对应`fexecve/execveat`也明确有脚本+CLOEXEC的解释器限制。[Linux execveat(2)](https://man7.org/linux/man-pages/man2/execveat.2.html)

执行memfd只是减少目标文件路径/远端文件系统读取的风险：

- 复制、SHA、ELF检查、seal、smoke在业务freeze前完成，失败继续旧owner。
- `/proc/self/fd`依赖proc可访问及LSM/seccomp/memfd执行策略；本次沙箱EACCES已保留`handoff-transport-memfd-probe-sandbox-denied.log`，隔离授权执行才通过。应feature-probe，不能现场首次freeze后发现被禁止。
- 动态ELF的PT_INTERP/共享库仍依赖外部路径；想减少该风险要验证无PT_INTERP的静态目标。即便静态memfd，shmem可分页/内核调度仍非严格硬实时保证，不宣称消灭所有D-state/停顿。
- memfd中运行的`os.Executable`/`/proc/.../exe`名称可能是已删除memfd名；安装路径、prev artifact、候选SHA与运行映像SHA必须独立记录，不能靠这个字符串重新打开作为下一次升级/rollback入口。
- 状态memfd里的秘密不入日志/argv；memfd不是同UID恶意子进程的通用秘密隔离，权限/ptrace与资源owner安全边界必须在runtime方案中明确。

## 6. foundation 可接受范围与仍待完整系统证明

同PID exec、已quiesce fd白名单、sealed显式状态的基础路线可行；root底层当前代码已吸收本lane两项flags风险。ForkLock保护Go os/exec创建子进程窗口，**不负责**freeze业务I/O、停止Wait/reap、校验actor授权、处理任意raw/C fork或恢复TLS/yamux堆；当前CGO禁用前提应继续明确。参考[Go ForkLock约定](https://pkg.go.dev/syscall#ForkLock)。

正式通过仍须root包的真exec/失败回滚/继承泄漏/≥5轮资源测试，后续真实SSH/PTY/传输/NATS/Caddy/fullstack drill、文档和旧版本退场收据。本文临时探针只证明具体内核/Go行为，并为正式测试提供独立反例；不代表H1–H5已完成。
