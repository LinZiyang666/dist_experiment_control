Pass — 仅定稿的连接/基础服务静态审查；热更新实现、H0 供给和运行验收仍为 NOT_RUN。

# 热更新定稿核查：连接、Caddy、NATS 与 H0 探针

日期：2026-09-20。职责：只读生产实现与 simcluster，未修改仓库、未启动服务、未提交/部署。本文不签署功能通过，也不替代独立外审。核查对象为当前 `docs/reviews/hot-update-plan.md`、`hot-update-tasklist.md`、`hot-update-evidence.md` 及已归档三路设计。

## 1. 定稿结论

未发现需要阻断计划进入 H0 的连接层遗漏。此前三路质疑的必要条件都已进入正式计划：

| 必要条件 | 定稿位置 | 核查结论 |
| --- | --- | --- |
| 原业务 socket 与可重建 TLS/yamux/WSS 分开；RESUME 不重新拨 sshd | plan:78–86，tasklist:34–41 | 明确；不能用升级后新连通替代原流 |
| ALPN 能力协商、旧 REGISTER/OK 原样、票据绑定原 allocation/授权/交接决议 | plan:84 | 明确；旧 wire 互通不等于旧端已有严格续接能力 |
| ctl 请求去重、输出续接与旧 ctl 能力前置 | plan:66–68 | 明确；新请求/输出不再只靠 agent 单边恢复 |
| SS AEAD 用户态状态、nonce/盐与 replay filter | plan:86，tasklist:40 | 明确；SSH 通过不能替代 SS 通过 |
| 稳定对象版本、tracker/终态/GC 原子责任与单 writer | plan:88–94 | 明确；不允许同名 Put 偷换 NUID 或旧 reaper 删活任务 |
| 真 Caddy 配置 reload、binary 换代分臂；旧 Caddy/NATS 全退场 | plan:96–104、144–147 | 明确；不能用 stream_close_delay 或旧进程无限 drain 代替完成 |
| N=1 产品冗余/本机交接、N=2 降级、HA 副本 readiness、精确 release join | plan:98–104 | 明确；安全 defer 不冲抵单节点成功臂 |
| 可恢复 runtime 自身换代、旧版本 finite exit、bootstrap legacy 边界 | plan:62–76、108–114、176–179 | 明确；不承诺任意 SIGKILL 的 syscall 记账歧义可泛解 |
| 真 systemd/binary drill、原连接 oracle、预算和变异红证据 | plan:134–181，tasklist:H0/H5/H6 | 明确，且当前未勾选；没有拿旧缺陷修复日志充热更新证据 |

以下是 H0 的具体接线前提，属于计划已要求但尚未实现的供给工作。建议将本报告挂入证据索引，实施时逐项确认；无需把静态核查记成运行收据。

## 2. H0 已确认的夹具限制

### H0-T1：现有入口不是 Caddy，也不能代理 NATS WebSocket

- `test/simcluster/Dockerfile:57–62` 只 COPY tether/nats-server/nats/nk，没有 caddy。
- `test/simcluster/drills/lib/ingress.sh:73–80` 启动的是 Python 同 netns sidecar。
- `test/simcluster/image/ingress-proxy.py:18–21` 明确不供 streaming；`:57–73` 用 HTTPConnection 读完整 response、过滤 Connection 后回传，不是 WebSocket 双向升级实现。
- `test/simcluster/image/provision-node.sh:27–36` 调真实 installer 写 Caddyfile，但说明 Caddy 不运行；`simcluster:192–197` 只启动 NATS/broker。

影响：复用 ingress_up、HTTPS 200 或错误 SAN 探针都无法证明真实 Caddy/WSS 生命周期。正式 plan:159 已正确列为待补前置，不是遗漏。

### H0-T2：只把 broker_url 改为 wss 会生成错误 tunnel_addr

- `test/simcluster/drills/lib/agentyaml.sh:38–39` 仅剥离 `nats://` 再去末尾端口。
- 输入 `wss://brk1:443` 得到 `tunnel_addr: wss://brk1:7000`；输入带 `/nats` 的地址更不能被该正则正确拆解。
- `simcluster:479` 和 `:509` 仍强制 `nats://<broker>:4222`；`agentyaml.sh:100–118` 已知道 CLI flag 会覆盖 YAML，并通过 flagless unit 避免。

建议：H0 专用供给明确两个独立值：`broker_url=wss://brk1:443/nats` 和 `tunnel_addr=brk1:7000`。可扩现有 helper 增加显式 tunnel_addr，或使用完整 URL parser 按产品规则提取 host；不要以第二个 sed 猜 scheme/path/IPv6。所有供给、重登、初始 restart 必须在原流 READY 屏障前完成。原流开始后不允许 helper 重启 agent 来“修好”路径。

还须验证 ctl 持久 session URL、agent flag/YAML 和 roster/bootstrap 发现的所有可选入口均满足该臂的 WSS 路径要求；不能 curl Caddy 成功，而测量期间 client 又从 seed/发现列表切到明文 4222。broker 内部本机 NATS 与 route 使用 TCP 是正常独立路径，不需强迫所有内部连接走 Caddy。

### H0-T3：现有 next artifact 不是独立历史 release

`test/simcluster/lib/stage.sh:37–44` 明确 same source，仅 VERSION 字符串不同。它可用于证明真实 syscall.Exec、/proc/exe SHA 更换和当前实现中断负基线；不能充当真实 N-1 wire/IPC/manifest 兼容、依赖版本迁移或 legacy bootstrap 完成证据。H4/H5 必须另冻结实际支持的两份源码/构建与真实 NATS/Caddy 版本，保留 artifact SHA；plan:104、108、146、161 已提供该门，实施不能误用默认 stage。

## 3. H0 真 Caddy 最小供给路线

建议新增独立的 `drills/lib/caddy.sh` 职责模块，保留现有 `ingress.sh` 给既有 manifest/订阅负例。以下是建议接线，不是已经创建的文件或执行过的步骤。

1. **artifact 供给**：扩共享 `lib/stage.sh` 从真实 installer 读取 Caddy pin，类似现有 sim_nats_version，供本地/远程两个 driver 共用。当前 pin 来源 `scripts/install.sh:1115–1116` 是 NATS v2.14.6/Caddy 2.7.6；installer 支持环境覆盖，记录最终解析值。下载按 installer:1188–1202 使用官方版本包、对应 checksum 文件并校验；记录二进制 SHA、版本和架构，不只信缓存文件名。Dockerfile COPY vendor/caddy 并 chmod。H0 先供一个 pin；H5 再供经支持矩阵批准的 old/new 两版本。
2. **复用实际 unit**：`install.sh:1324–1338` 已写真实 `caddy run --config /etc/tether/Caddyfile --adapter caddyfile` systemd unit；installer --skip-download 在 `:1178–1182` 不提供二进制，所以应由镜像供给。H0 在 broker 容器内起该 unit，复用真实 PID1/cgroup、服务用户/权限；不要让 Python sidecar 冒充 unit。不得与同端口旧 ingress sidecar 并起。
3. **本地 TLS 供给**：复用 `ingress.sh:24–31` 的 per-instance CA ingress leaf 和 `:110–116` 的系统信任注入，在 agent/ctl 进程启动前完成。CA 单一所有者不重建（`lib/secrets.sh:19–30`），SAN 为 Docker hostname（`:33–46`）；Caddyfile 的该域配置显式证书/key，保持 root 配置不可由业务用户替换。只有初始供给改 TLS 来源；不以 ACME 获取成功作为本地网必要条件。记录本臂覆盖真实 TLS/WSS，**不声明覆盖公网 DNS/ACME 签发/续期**。
4. **保持生产路由顺序**：installer:1024–1030 的 `/sub/* → 8090` 在 catch-all `→8222` 前；manifest 如需启用保留显式 loopback 路由。NATS WebSocket 仍在 broker loopback 上，外部经 Caddy `wss://brk1:443/nats`。H0 不添加隐藏重试/长期关闭延迟来替产品保流。
5. **供给前检查**：caddy validate/adapt 成功；实际进程 `/proc/<pid>/exe` SHA=冻结 artifact；证书 SAN/链用真实信任检查；受支持的真实 ctl/agent经 WSS 完成认证、注册、至少一个读请求和一个有副作用的请求。错误 SAN/错误 CA 臂须确实拒绝。现有 `_ingress_front_up` 的 `curl -k` 是刻意让坏证书夹具启动的 readiness，不能复用为 H0 正例链验证。
6. **测量路径证明**：记录 agent/ctl 实际连接目标与 NATS WebSocket connection 身份，结合 Caddy listener/accepted FD 或 access/event 证据证明请求确经入口。专用负控制短停 Caddy，必须看到内部 WSS 断连；无需断开业务原 SSH，因为此时 tunnel 数据 carrier 独立。该负控制不能只以“请求仍成功”就判失败：HA 产品可以合法换另一个真实 WSS 入口；应记录旧入口断、新入口身份及该臂规定的拓扑。
7. **供给与升级分开**：H0 建机、安装 CA、初始服务启动可由 harness 完成。原流 READY 后，Caddy/NATS 升回滚必须从产品升级入口发起，由产品准备新路径/迁移/关闭旧映像；harness 仅观测或执行明确故障注入。H0 故意 stop 是测 oracle 的负控制，不冒充升级实现。

NATS 现有外置真实 server 已由 Dockerfile:58 与 systemd 提供；无需用 Go 内嵌组件替代。H5 readiness 必须同时查 route/quorum、JetStream 实际 stream/consumer 副本与 auth_callout，从目标 client 路径发请求；broker ONLINE 或 meta leader 单独不充分。官方 pin 行为核查与来源已在 `docs/reviews/hot-update-design-transport.md` §2–§3，定稿实现若换依赖须重查。

## 4. 持久原 SSH/TCP 流探针的最小可复用模块

### 4.1 可复用现有设施，但不能照搬现有 oracle

| 已有模块 | 可复用部分 | 不能用来证明的东西 |
| --- | --- | --- |
| Dockerfile:14–20、42–47；simcluster:87–88 | 真 sshd、Python、sim 用户、容器内真实服务 | `simcluster ssh` 的直连调试路径绕过 tether，不是待测原流 |
| `drills/70-expose-journey.sh:29、45–49` | 真 CLI 创建 expose，explain JSON 取实际 public_port | `dataplane.sh:45–49` 每次 curl 重新连接；`:91–103` 重启后同端口新流成功不等于老流保住 |
| `lib/docker.sh:129–136`、`lib/tether.sh:8–17` | 按 instance 定位容器、身份及实际 CLI/systemctl | 不在 host 起业务探针；不绕过产品建立转发 |
| `drills/33-node-upgrade-success.sh:82–101、264–267` | MainPID/starttime、`/proc/<pid>/exe` SHA、真实 node upgrade 和 marker 联合身份判据 | 进程映像变了不等于字节流连续 |
| `drills/60-user-journey.sh:23` 的 pty-run helper | PTY 操作与输出采集的模式 | SSH raw socket 与 PTY 必须各自验证；PTY echo 本身可能伪造收到的输入 |

建议职责文件为 `image/flow-probe.py`（独立帧生成/验证/记录）与 `drills/lib/flow.sh`（供给/start/wait_ready/check/stop/collect）。这里只指定职责，正式新增文件名/测试/drill编号由root按现有inventory登记。raw TCP 和 SSH stdio 共用帧验证器，协议端点互不复用已有产品 ACK 实现，保持 oracle 独立。

### 4.2 SSH 路径与身份

- 在专用 agent 上用真实 CLI `expose ... --local 22`，从 explain JSON 得到 public broker/port。ctl 容器用一次普通 OpenSSH 连接这个公开端点，远端执行 probe peer。这条链为 `ctl ssh → 原public TCP → broker tunnel → agent 原local TCP → sshd`；不调用 `simcluster ssh`，不从 ctl 直连 agent:22。
- 供给阶段生成 per-instance 临时 SSH key、sim 用户 authorized_keys 与严格 known_hosts（取被测容器真实 sshd host public key）。使用 BatchMode、IdentitiesOnly、ControlMaster=no、ConnectionAttempts=1；不 autossh、不重启 ssh subprocess、不启应用重连。不会把该 key 写入日志或提交。
- 一次启动的 SSH client PID/start_ticks、原 socket inode/tuple 与 remote helper PID/start_ticks/随机会话 nonce 入 READY 记录；remote 可带 SSH_CONNECTION 与 sshd 子进程身份。四元组或 nonce 单项都不作为完整证明。记录原 client 与 server socket 对应关系，升级后不重新认证/执行 remote helper。
- 远端 probe/sshd 是负载本身，由正常 ssh 会话持有，不放进待升级 tether unit 的 cgroup，也不让 harness 保管/搬移产品内部 socket。没有读到真实 READY 及两方向足量字节前，不开始升级。

### 4.3 独立双向序号 oracle

- 两端**独立**持续发帧，帧含 run nonce、direction、严格递增 sequence、payload length/hash；接收端验证连续序号与预定 payload。采用不同长度和二进制内容，避免行文本/终端 echo 造成假绿。
- 各方向分别记录发送与接收完整高水位、总字节、rolling digest、首次/最近进度时间、最大无进展和退出原因。ACK 是探针观测，不替产品确认字节。对端发到 N 后消失不能被“截至 N 的 SHA 正确”蒙混；收尾必须交换预定最终序号/长度/digest 并校验两端一致。
- 升级窗口必须完全位于持续流量窗口；开始前、每阶段、完成后都要非空证据。EOF、SSH子进程退出、非法重复/缺序、超预算静默立即记录粘性失败；后续恢复不得清零 verdict。日志不能只保存最后一行成功。
- 用单调时钟计算本机进度/往返时延；跨机器 wall clock 不直接相减。基线校准与测试余量分开；CPU饥饿/探针崩溃是 SETUP-RED/INCOMPLETE 而非自动产品绿。
- 持续双向 busy arm 外另做 idle→升级→同socket再发、慢接收/backpressure、边界分片和半关闭。SSH stdio EOF 与 raw TCP shutdown 的语义不同，half-close 使用 raw probe 原 socket 单独验证：一方 shutdown(SHUT_WR) 后另一方向继续并最终FIN，无提前EOF。不要用 SSH ctrl-c/退出替代 TCP half-close。
- H0 负控制至少为：初始可通却中途关闭原 public socket；当前真实 agent reexec；当前真实 broker reexec；空探针/探针早退；重连后同 payload 的伪探针。保持原断言不变，证明它们各自被抓红。NATS/Caddy restart 对已独立建立的 SSH carrier 不必天然造成红，应看对应连续控制/文件/PTY 数据 oracle，不强造错误预期。

### 4.4 新控制与完整退场必须并列，不由 SSH 代替

`drills/30-rolling-upgrade.sh:191` 现有 session-create循环走明文4222且只有 WROTE/WRITEFAIL，没有逐请求时延；可借鉴其“全程实际Raft写”，不能照搬为10请求/s WSS严格验收。新增探针应保同requestID的产品内重试/查询、payload digest、副作用唯一收据、开始/结束单调时刻和p50/p95/p99/max，不能在shell层换新ID重跑后算成功。

同时持续收集所有 component/runtime/coordinator 的 PID/start_ticks/运行SHA与所持资源；完成只在原业务仍进行、所有旧映像已退场、队列/consumer/FD归属一致时通过。H0仅先建立传感器与现状红基线，H1–H5才逐项建立绿证据。

## 5. 建议 H0 执行顺序与边界

1. root 冻结专用 instance/image/artifact/权限/配置，补真 Caddy 与 WSS 供给；静态自检和错误证书负例先过。
2. 不升级，运行 SSH/TCP + 逐请求探针至少一轮预算校准；证明两方向非空、路径真实、身份记录和清理完整。
3. 使用当前真实 binary/CLI 各打一条 agent reexec、broker reexec现状反例；保留原始探针失败及实际 exec 的联合收据。当前失败是功能基线，不改expected让旧实现成为热更新GREEN。
4. 在独立臂验证停 Caddy/停 NATS 的路径传感器与控制/文件 oracle，并把实际结果按当前行为登记；不把组件重启当未来全栈升级实现。
5. H0 退出条件是供给、传感器、校准和负控制完备。N-1、N=1基础服务交接、全栈15min收敛都仍待H4/H5/H6真实实现证明。

本文无运行日志：本次没有执行测试、启动 Docker/systemd/service 或修改生产/现有测试。夹具局限是源码确认；新模块方案与预算达成仍待实际实现和测量。
