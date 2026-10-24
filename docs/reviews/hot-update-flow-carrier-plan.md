> Documentation pointer: P28's §12.2 file names are historical; use [this single frozen plan](hot-update-flow-carrier-plan.md), [deployment guards](../deploy-tier-gotchas.md), and [S5 contracts §22.8](../distributed-broker-architecture.md#228-s5-flow-and-ss-domains-binding-contract).
# 热更新 S5 · flow-carrier-ss 实现 plan（定稿）

> 定稿：2026-10-03，lead（主进程）。起草：4 路 lens 草案（tunnel 一路 GPT-6-Astra，其余三路 gpt-6.1-sol）→ 4 路 gpt-6.1-sol 对抗批评 →
> gpt-6.1-sol 综合；原始稿与批评在仓库外 `~/tether-hot-update-handoff-20260920T2356Z/s5-plan/`。下面「lead 定稿裁决」覆盖正文中与之冲突的文字。
> **实施入口**：S4 出口（stages-plan §S5 entry）；S4 集成未关闭前不开工。

## lead 定稿裁决（覆盖正文）

| # | 裁决 | 理由 |
|---|---|---|
| Q1 | 采纳推荐：SS 授权 = 有界、单 SQLite read transaction 的本地 applied 快照，已知 lag 一律拒绝；**不**宣称 leader 提交即全局线性撤权。 | 更强一致性需要新 Raft/wire 面，属 S8 事务面范围；Barrier(timeout) 不能证明，假装满足是假收据。安全取舍按「够用、不引入大量复杂度」。 |
| Q2 | 采纳推荐：S5 对只持 idle listener / retained slot 的 legacy 一律 `legacy_session_busy`；首次迁入的宽化留 S8 受控事务。 | 与 S3/S4 的 legacy 口径一致；把未知责任放行等于拿连续性赌运气。 |
| Q3 | 采纳推荐：每节点 64 份材料历史配额；满额先撤权、拒新认证并给出失败/重试反馈，不静默 GC 历史，survivor 不受影响。 | 明确、可测的资源上界；扩大配额是另一次预算重算。 |
| M | **模型分工**：§12 的 30 个工作包只有 P1 raw-flow-endpoint 与 P8 ss-actor 两个 HARD 包可用 gpt-6-astra，其余全部 gpt-6.1-sol；集成修复轮默认 sol。 | 用户 2026-10-02：「推进 tether 热更新多用 sol 少用 astra」。 |
| C1 | **drill 75.C 分两段（2026-10-11 裁定，覆盖 §9.1 C 行与之冲突的读法）**：①**饱和段**——32 条原始 active flow 合计预留满 256 MiB 共享 replay 预算；此段内新 flow／新 exec 必须得到明确的原生 capacity 拒绝（计数、带类型），已存在的 flow 不被杀掉让位；饱和 cohort 按各自声明的有限长度自然发完 FIN、最终账本完整后释放预算，随后新请求恢复成功（恢复本身要有收据）。②**正常段**——按 §3.4 的正常 profile（replay 预留 ≤224 MiB，至少留 32 MiB 给并发短流）跨同 PID 真实 cut：原始 socket 与应用身份不变、target Accept=1、各向 ≥20 个 cut 后精确样本、正常流停顿 ≤5 s，10/s 新请求终态失败 = 0。两段都在同一 N3-live 集群、同一镜像、同一次 arm 运行内；不放宽任何判定、期限或产品预算。 | §3.4 本来就规定饱和 profile 与正常 profile 分开测；§9.1 C 行把「32 active 边界」与「新流」写进同一阶段，与 §3.4 的预算算术互斥（32×8 MiB=256 MiB，新流无处预留），drill 实跑证实只能得到正确的容量拒绝。方案原文见 `~/tether-hot-update-handoff-20260920T2356Z/s5-evidence/drills/C-PHASE-PROPOSAL.md`。 |

> 状态（原综合稿）：供 lead 逐行审阅后冻结；本文尚不是已实现、已验证的结论。
> 基线：只读快照 /home/weiland/s5wt/synth，HEAD f6d32fd913c799f86fc3301f0d8fb6c12c92218c；S0–S3 已实现，S4 在并行集成。本轮仅写本文件，未运行构建、测试、gate、drill，所有新增收据均为 NOT_RUN。
> 输入：四稿 plan-proto.md（P）、plan-tunnel.md（T）、plan-ss.md（S）、plan-restore.md（R）；四评 crit-bytes.md（B）、crit-deadlock.md（D）、crit-crypto.md（C）、crit-drill.md（V）。附录 A 的编号对应各评末尾「必须修复」列表，不混用正文反例编号。
> 格式与共享合同：hot-update-agent-bootstrap-plan.md、冻结的 hot-update-pty-terminal-plan.md，尤其顶部 L1–L8、§0、§5.2。上位范围为 stages-plan S5 及 S4/S6/S9 边界；architecture §21、§22.4–§22.6；handoff-failures §4；tasklist B-2、D-1..D-6；plan §3.3；CLAUDE、testing-standards、simcluster README。

## 定稿裁决（建议冻结值；产品范围三问见 §13）

| # | 裁决 | 理由与依据 |
|---|---|---|
| L1 | ImageABIVersion 保持 1；中间行改为 image-domains exec-v1 pty-v1 flow-v1 ss-v1，最后仍是 image-abi v1 | 采用域能力检查，拒绝「ABI 相同即能读」。Bundle 的新域 omitempty，纯 exec/PTY 无保留 flow/SS 责任时仍可回迁旧 reader。 |
| L2 | candidate 与实际 prev 均按真实 SHA 校验当前保留域；prev ∈ {running image, candidate} | 沿用 S4 §5.2 与 upgrade.go:714–727；不恢复 running==disk，不让共享 binary sibling 的合法升级变成拒绝。 |
| L3 | 唯一 tether-flow/1：40-byte header、九种 frame、单调 OpenSeq、永久退役水位；ACK 只表示业务 socket 已写入 | 采用 P/T 的逻辑流 owner，补齐 S/R 的 bind、未到达 OPEN 与退休漏洞。协议具体布局由 §2 冻结。 |
| L4 | 每端每 flow 一个 8 MiB send replay；接收 pending 有界；exec/PTY/flow 共用同一个 256 MiB budget pointer | 拒绝 SS 4+4 MiB 隐含 credit 模型。ACK 释放环内空间，不释放已预留 capacity；workload/replay.go:196–212、272–304。 |
| L5 | ALPN 无交集走 legacy raw；REGISTER 六字段与服务端精确 OK\n 不变；选中 v1 后绝不偷偷降级 | TLS、pin 验证仍先于 token；旧客户端接受集合与服务端发射集合不同，见 §1。 |
| L6 | Endpoint 独立于 serverSession；attach-before-bind；首次 detached grace=8s，不续期 | listener/pubConn/flow/fence 由 Endpoint 持有；grace 严格小于 proxyRehomeDwell 的最短坏相位跨度 10s；不改 reaper 常量。 |
| L7 | planned handoff 在业务仍运行时 PREPARE；candidate RESUME 消费一次 permit；exec 失败旧端重新取得当前授权 | 拒绝全域冻结后等待网络 commit。缓存回执仍重查 tokenLookup、权限与 fences。 |
| L8 | SS 是 freezable actor；nonce、半帧、已 seal 密文 suffix、target parser、盐历史完整迁移；秘密只在 sealed memfd | 不保存 TLS/yamux；不重建业务 TCP；不重 seal 未写完的密文。 |
| L9 | SS 权威采用有界、单 SQLite read transaction 的本地 applied 快照，已知 lag 一律拒绝；不声称 leader 提交即全局撤权 | VerifyLeaderRead/Future.Error 与 Barrier(timeout) 不能证明 1s 完成。更强一致性是 §13 Q1 的产品范围判断，本文推荐合同不新增 Raft/wire。 |
| L10 | revoke 与 freeze 分离；稳定 ConnID→FlowID，撤权不可逆 shutdown 原 socket 及其 dup，并显式终结公网 flow | 静默 victim 也由产品回收 FD/replay；survivor 与被撤销连接分开验收。 |
| L11 | 删除 flow_busy/ss_busy 之前闭合 owner census 与 admission；新增 ss_state_unrepresentable，S4 的 10 项变 9 项 | 不能只删谓词。legacy retained slot、未知 adapter、未完成接纳仍拒绝；不可表示与容量超限分码。 |
| L12 | Agent 恰 125 methods、production files≤19；只在现有 agent/cmd 文件接线；两个 HARD 包承载新增状态机 | 不放宽 structural golden，不新增 agent 生产文件。包级函数与子包负责新行为。 |
| L13 | P0 先落精确接口与失败桩，再并行实现；最终每个文件只有一个 owner | 沿 S4 L7 的时间互斥转移：P0 临时写下游桩文件，落地后立即交给其最终 owner，绝不同时编辑。 |
| L14 | 75.A 同一 cohort 贯穿升级、窗内 SS revoke、forced carrier rebuild；B=可通信回迁，W=真实 120s watchdog | 不拼接不同流的收据，不以普通升级或暂停 SS 权限代签 watchdog。 |
| L15 | 75 六臂全 GREEN；39.F 真实旧 exporter 原生 PRODUCT-RED；36.A/W/C/L GREEN 与 39.P PRODUCT-RED 保留 | 沿 S4 判词先例，不改 validator/runner，不把安全 busy 拒绝伪造成断流负控。 |
| L16 | 正常 flow/新请求最大停顿≤5s 硬判；500ms 全映像冻结窗仍为校准，完整报告超标 | 沿冻结 S4 L4，不用 p95 代 max，不删 trailing silence。Darwin 按 S4 L6，cross-build 与实际 run 分列。 |

## §0 目标与出口（6 条 + 机械判定）

| # | stages-plan S5 出口 | 同一最终候选的机械判定与收据 |
|---|---|---|
| 1 | 真 TLS+yamux，多流双向 seq/hash 跨 carrier 重建；OPEN/idle/slow/half-close/revoke/旧 epoch；ALPN 四象限 | §8 T01–T16、T38；原 socket、target Accept=1、独立双向 ledger、真实 n>0 partial write、未 ACK 数据均非空；每个规定变异实际击红。 |
| 2 | agent 同 PID exec；sshd 与 broker 原 socket 不关闭；dedupe-before-write；背压有界 | T39–T41、T44；原 FD fingerprint/application nonce 不变，运行 SHA 确实变化；接收者仍活时核 FD/replayUsed，不能借进程退出清场。 |
| 3 | 三个固定 broker 测试 | TestEndpointSurvivesSessionLossAndResumes、TestResumeReRunsTokenLookupAndFence、TestReaperDoesNotRehomeDuringPlannedCarrierRebuild，逐字保留；实际 SQL port/token/home/epoch 和 event 计数不变。 |
| 4 | SS nonce/半帧/盐历史/撤销/轮换；两 fuzzer；秘密不进 manifest | T17–T29、T42–T43；独立 AEAD oracle、实际 subkey+nonce Seal ledger；FUZZ_MIN=2 且精确发现 FlowFrame/SSFrame 两个目标；公开载体泄密 mutation→红。 |
| 5 | drill 75 GREEN；原 SSH+4 TCP+OPEN+half-close+SS+新流；禁止应用重连；负控、legacy | §9 A/C/R/B/W/L 六臂、父 75 全 GREEN，nc_gap=0；early-close/wrong-offset 原生 ASSERT-FAIL；39.F 原生 PRODUCT-RED；L 真 v0.6.0 broker 从首业务前运行。 |
| 6 | 70/72 不劣化；三硬闸+gates；Darwin；整包 race/leak；additive、ctx | §10 完整 gate 与 §11 receipts；36 四臂及 39.P 联合回归；明确保留 S6 broker exec、S8 同 SID recreation、S9 infra 边界。 |

范围：S5 完成 agent image 中的 flows/SS、同一 broker 进程内 carrier 重建和原业务连接恢复。S6 才导出 broker Server/Endpoint/listener/fence、交接 SQLite/Raft 与 broker exec；S9 才升级 NATS/Caddy。跨 home/epoch 的业务 socket 迁移不在 S5，旧 allocation 被撤销时必须关闭，不能声称续接到新 home。

执行纪律：本文只授权未来实施合同；本轮仅写计划。每个出口保存 source/artifact/running SHA、命令/native rc、mutation diff、原始 oracle 和资源收据。最终验证只接受同一候选，不能借历史绿灯代签。

## §1 代码裁判与分歧裁决

行号指本快照，不拿草稿旧行号作证据；S4 尚未集成的修复以其冻结合同为前提，不称当前已经修好。

| 争议 | 当前代码裁定 | 最终设计后果 |
|---|---|---|
| session 失联只关闭 carrier | B/D 正确：tunnel/tunnel.go:343–352 的 serverSession 持 listener；:947–954 cancel 并关闭 listener；:962–993 defer 关闭全部桥 | Endpoint 拆离；carrier 的退出不关闭原业务 FD。 |
| REGISTER 已 attach-before-bind | T 的意图正确、现状不成立：tunnel.go:858–869 每次先 bind，:871–888 才检查 fence，:933–941 才替换 session | 先查/保留 Endpoint attachment 槽，只有无现存 owner 的 creator 能 bind。 |
| REGISTER/OK 可增加协商尾字段 | 不采纳：tunnel.go:1841 六字段；:2087–2107 parse 严格六字段；:890 服务端精确 OK\n | ALPN 是唯一版本选择；不发 OK flow-v1。 |
| 旧 client 只接受精确 LF | B 正确：:1854 TrimRight CR/LF，:1855 比较 OK，实际上接受多种末尾 CR/LF；:1847–1861 bufio 预读后把 rawConn 给 yamux，可能丢尾部 | 服务端继续精确 OK\n；新 client 保留旧接受集合；两端将 reader 的剩余字节交给 yamux。 |
| bridge 已保 half-close | B 正确：tunnel.go:2070–2084 第一侧结束就 full Close 两侧 | v1 明确 FIN→CloseWrite，反向仍运行；legacy 不加任何 flow header。 |
| fence 的 in-flight 已保护 detached owner | B/D/C 正确：:815–838 只计 register handler；:1192–1197、:1212–1217 prune 不计 Endpoint/permit；CloseProxy/CloseSession/ForgetSession 只巡现 session | 活跃、detached、prepared、cached-receipt owner 与 timer 都计 fence refs，终态才释放。 |
| 任意 TokenLookup 都可保证 1s | D/C 正确：tunnel.go:838 同步 callback 无截止；cluster/read.go:66–70 Future.Error 不可取消；node.go:497 Barrier(timeout).Error 不能证明 apply 完成≤timeout | v1 只接可取消并可 join 的 bounded lookup；不用 goroutine+timeout 包住无界函数。 |
| proxy authority 已是原子快照 | C 正确：broker/proxy.go:486–501、:516–520 分别读取 Enabled/allocation/Keys/Epoch，Epoch 错误被忽略；expose.go:69–161 同样多读且 lag 只挡部分缺失路径 | 单 read transaction 生成完整 grant，任何读错整体拒绝；positive row 已知 lag 也拒绝。 |
| same Generation/Epoch 即无需重设 keys | C 正确：agent/proxy.go:354–358 同 pair 直接 ACK；:499–505 SetKeys 失败只日志返回 | pair 加完整 keyset digest；同 pair 不同 digest 冲突拒绝，失败不记 applied、不发成功 ACK。 |
| Stop 在锁内没有死锁 | D 正确：agent/proxy.go:788–801 持 p.mu 调 Stop；ssproxy/server.go:454 Stop 等 wg；agent.go:766–778 已声明 hook 必须 lock-free | 标记/摘表/取 owner 在锁内，Shutdown/Close/join 在所有表锁外；回调不得 self-join。 |
| timer=nil 表示无需导出 cleanup | C 正确：proxy.go:1051 先清 timer 指针，:1069–1078 之后清场 | 导出 deadline、Fired、CleanupPending；指针存在与否不构成事实。 |
| ACK 释放 8 MiB reservation | B/D 正确：workload/replay.go:28–52 连空 log 都预留；:196–212 ACK 只改 Base；:272–304 Close/Detach 才释放 | DomainUsed=sum(owned capacity)，不以 retained bytes 或 global Used 冒充本域值。 |
| exec budget 已能直接共享 | B/D 正确：execsession/runtime.go:158 是 value；image.go:147、:323 与 fence.go:403 使用全局 Used | 改为唯一 pointer；image/fence 校验本域 own reservation，import 不覆盖共享 Limit。 |
| SS salt 域是 KeyID/direction | C 正确：crypto.go:62–66 HKDF(master,salt,"ss-subkey") 不含 ID/direction；server.go:424–450 删除 ID 时删历史 | actual master/subkey 域归并 aliases，跨删除/重加/代际保留；response salt 也必须 reserve。 |
| 旧 saltRing 已有 head/count | B 正确：server.go:109–122 是 set+order slice/FIFO，head/count 是新增实现 | image 必须保存新 head/count/FIFO 顺序，并验证旧窗口行为，不伪称现有字段。 |
| 半 AEAD 帧只需 nonce | B/C 正确：crypto.go:96–120 的 read 半帧在栈；:147–169 seal 后 nonce 已推进、raw Write 忽略 n；server.go:788 target parser 也在栈 | actor 持全部 cursor/phase/plain/cipher/nonce/target/DNS/connect 决定，禁止重 Seal。 |
| KeyID 仍有效足以授权新握手 | C 正确：server.go:548 bindKeyConn 只查 ID；:616–638 trial 用的是当时 master | trial 完成和绑定原子重验 active material；旧合法连接与新认证分门。 |
| sealed state FD 无需显式回收 | D 正确：agentimage/freeze.go:185–211 重 Export 替换资源；handoff.Prepare 只 dup borrowed FD | 每次新 sealed FD 有 cut owner，替换/失败/Thaw/Discard 显式 Close，不等进程退出。 |
| resource ID 可用冒号 | B/D/V 正确：handoff/handoff.go:202–213 validName 只允 ASCII 字母数字及 -_. | 用点分名，真实 Prepare/Restore 验证，不只测 JSON。 |
| partial activation 失败可回 pristine image | C 正确：agentboot/image.go:310–335 Activate/Detach 与 agentimage/restore.go:285–315 存在 active 边界风险；S4 L6 已冻结修复 | 任一 resource 激活就承担 active owner，失败只能 current cut；S5 接在同一边界。 |
| 用户要求 ssproxy 零 context import | 拒绝该归因：用户没有此要求；CLAUDE 表旧文字与 dataplane_lifetime_test.go:33–55 现实现不一致，server.go:74–92 有自身 stopCtx | 保持 caller-ctx-free，不禁自身 stop root；P28 修文，P26 加解析 type alias/导出签名的行为守卫。 |
| 独立 control stream 消除 yamux 死锁 | D 正确：yamux v0.1.2 session.go:109 sendCh=64，:391–441 waitForSendErr 可复制已排队 body 后超时；stream.go:128–132 的 Read 同步 sendWindowUpdate，:267–301 可等 writer；mux.go:67 默认10s | §3 给实际 wait-for 图及锁外关闭底层 TLS 的取消路径，不能只设 stream deadline。 |
| leader reaper 不看 proxy_ready | V 正确：proxy_reconcile.go:204–210 home-unhealthy、:226–240 unready 两条分支都可 rehome/rotate | 计划窗保真实 ready latch，测试两个分支、历史 dwell 与 tick 相位；不暂停 reaper。 |
| MarkReady 等于 durable commit | C/V 正确：upgrade_state.go:840–848 写失败 marker 仍 pending/watchdog armed；agent.go:1754–1759 仍 MarkReady | W 用这条真实路径；ready、pending、committed 分开记录。 |

选择理由：采用 send-only replay，避免 SS 的两半 credit 与现有 workload budget 不相容；采用有界 retirement watermark，拒绝 TTL 忘记旧 OPEN；采用 binary sealed codec，拒绝靠 DisallowUnknownFields 假装能拒 duplicate/case alias；采用旧 carrier 先 PREPARE，拒绝冻结后网络 commit。flow 不 import yamux/tunnel，yamux adapter 放 tunnel；stages 的风险描述不是必须形成反向依赖的指令，显式 race/leak 补足自动扫描。

## §2 P0 冻结契约：类型、签名、wire、失败桩

P0 以 S4 集成后的树为基线，先落下列 contract 文件和接口消费点；需要增 Server/Client/Agent/Runtime 字段时，由 P0 在最终 owner 文件中一次性落 private state pointer 和失败桩，随后交还。最终 owner 文件仅列于 §12；P0 的临时编辑清单按每个签名对应 owner 文件生成 receipt。桩统一返回 ErrNotImplemented，error string="<pkg>: not implemented (S5 P<n>)"，不 bind、不 spawn、不 ACK、不假成功。P0 contract 文件永久归 P0；所有实现只改各自的未来 owner 文件。

### 2.1 ctx-free 导出签名与公共数据

下列为唯一接口，实施者不得自行换签名；未列出现有接口保持兼容。类型里的私有 state 在相应 runtime owner 文件定义；Image schema 字段全部在 contract 文件一次冻结。deadline 是调用操作截止，不是对象 lifetime；Stop 是该操作取消源。

~~~go
// internal/rawsocket/contracts.go
var ErrUnsupported, ErrBusy, ErrClosed, ErrIdentity error
type FileSource interface { TakeFile(id, name string) (*os.File, error) }
type Fingerprint struct { Dev uint64; Ino uint64; Type uint32 }
type ConnectState uint8 // 1 Connecting, 2 Connected, 3 Failed
// Tuple 属秘密/private state，不得进入 manifest。
type Tuple struct { Local netip.AddrPort; Remote netip.AddrPort }
type Socket struct { state *socketState }
func Adopt(f *os.File, want Fingerprint) (*Socket, error)
func Connect(addr netip.AddrPort, deadline time.Time, stop <-chan struct{},
    beforeConnect func(Tuple) error) (*Socket, error)
func (s *Socket) CompleteConnect(deadline time.Time, stop <-chan struct{}) error
func (s *Socket) Read(p []byte) (int, error)
func (s *Socket) Write(p []byte) (int, error)
func (s *Socket) SetReadDeadline(t time.Time) error
func (s *Socket) SetWriteDeadline(t time.Time) error
func (s *Socket) CloseWrite() error
func (s *Socket) Shutdown() error // SHUT_RDWR；所有 OFD dup 同时失效
func (s *Socket) Close() error
func (s *Socket) Fingerprint() Fingerprint
func (s *Socket) ExportFile() (*os.File, error) // frozen-only，borrowed
func ValidateFile(f *os.File, want Fingerprint) error // 不消费 SO_ERROR

// internal/sealedstate/contracts.go
const Version = 1
const MaxStateBytes = 32 << 20
var ErrNotImplemented, ErrInvalid, ErrCapacity, ErrUnsupported error
type Snapshot struct { Resources []handoff.Resource; Owned []*os.File }
func New(id string, data []byte) (*Snapshot, error)
func Read(src rawsocket.FileSource, id, name string, limit int64) ([]byte, *os.File, error)
func (s *Snapshot) Close() error // 仅 Owned；不关 borrowed 业务 FD

// internal/flow/contracts.go
const Version = 1
const MaxDataBytes = 32 << 10
const MaxControlBytes = 32 << 10
type ID [16]byte
type EndpointID [16]byte
type Kind uint8 // §2.2 的 1..9
type Phase uint8 // 1 Opening,2 Connecting,3 Active,4 Draining,5 Terminal
type Reason uint16 // §2.2 的 1..16；0 只表示成功
type Frame struct {
    Version uint8; Kind Kind; Flags uint16; BodyLen uint32
    FlowID ID; CarrierGen uint64; Offset uint64; Body []byte
}
func EncodeFrame(w io.Writer, f Frame) error
func DecodeFrame(r io.Reader) (Frame, error)
type Config struct {
    Budget *workload.ReplayBudget; MaxFlows int; MaxPendingFDs int
    Dial func(ID, uint16, time.Time, <-chan struct{}) (*rawsocket.Socket, error)
    OnTerminal func(ID, Reason)
}
type Manager struct { state *managerState }
type Frozen struct { state *frozenState }
type RecordImage struct {
    ID ID; OpenSeq uint64; Intent [32]byte; Phase Phase
    TxBase uint64; TxEnd uint64; RxApplied uint64; RxReceived uint64
    TxFIN uint64; RxFIN uint64; FINFlags uint8; RawRef string; ReplayRef string
}
type RetirementImage struct { EndpointID EndpointID; NextOpenSeq uint64;
    RetiredThrough uint64; Holes []uint64 }
type Image struct { Version int; Records []RecordImage; Retirement []RetirementImage;
    StateRef string; ReservedBytes int64 }
func New(cfg Config) (*Manager, error)
func (m *Manager) Freeze(deadline time.Time) (*Frozen, error)
func (f *Frozen) Export() (Image, *sealedstate.Snapshot, error)
func (f *Frozen) Thaw() error
func (f *Frozen) Discard() error
func Restore(cfg Config, im Image, src rawsocket.FileSource) (*Manager, []handoff.Resource, error)
func (m *Manager) ActivateRaw() error
func (m *Manager) RevokeFlow(id ID, reason Reason) error
func (m *Manager) Close() error

// internal/tunnel/flow_contracts.go
const FlowALPN = "tether-flow/1"
const DetachedGrace = 8 * time.Second
const ResumeAttempt = 2 * time.Second
const LookupBudget = time.Second
const PermitTTL = 120 * time.Second
const CarrierWriteBudget = time.Second
type Allocation struct { SID string; NID string; Port int; TokenHash string; Epoch int64 }
type AuthRequest struct { Allocation Allocation; Deadline time.Time; Stop <-chan struct{} }
type AuthorizedKey struct { ID string; Secret []byte }
type Authority struct { Enabled bool; Allocation Allocation; Generation int64;
    ProxyEpoch int64; AppliedRevision uint64; Keys []AuthorizedKey }
type TokenLookupOperation func(AuthRequest) error
type ResumeAuthorizer func(AuthRequest) (Authority, error)
type FlowConfig struct { Budget *workload.ReplayBudget; InstanceID string;
    OwnerGen uint64; BootID string; MaxEndpoints int;
    MaxPendingFDs int; TokenLookup TokenLookupOperation; Authorize ResumeAuthorizer;
    BeforeLocalConnect func(flow.ID, rawsocket.Tuple) error }
type EndpointImage struct { ID flow.EndpointID; OwnerGen uint64; CarrierGen uint64;
    FlowIDs []flow.ID; StateRef string }
type ClientImage struct { Version int; Endpoints []EndpointImage;
    Flows flow.Image; StateRef string }
type ClientFrozen struct { state *clientFrozenState }
type Census struct { Owned int; Legacy int; Admission int; Rehome int; Retained int; Unsupported bool }
func (s *Server) SetFlowConfig(cfg FlowConfig) error // 必须在 Serve 前
func (c *Client) SetFlowConfig(cfg FlowConfig) error // 必须在 Start 前
func (c *Client) PrepareHandoff(nextOwner uint64, cutID [16]byte, deadline time.Time) error
func (c *Client) FreezeFlows(deadline time.Time) (*ClientFrozen, error)
func (f *ClientFrozen) Export() (ClientImage, *sealedstate.Snapshot, error)
func (f *ClientFrozen) Thaw() error
func (f *ClientFrozen) Discard() error
func RestoreClient(cfg FlowConfig, im ClientImage, src rawsocket.FileSource) (*Client, []handoff.Resource, error)
func (c *Client) ActivateRaw() error
func (c *Client) ResumeCarriers(deadline time.Time) error
func (c *Client) RevokeFlow(id flow.ID, reason flow.Reason) error
func (c *Client) FlowCensus() Census
// OwnerGen来自当前agent image lineage；initial非零，handoff严格+1，溢出拒绝。

// internal/agent/ssproxy/image_contracts.go
var ErrStateUnrepresentable, ErrCapacity, ErrAuthorityPending error
type Image struct { Version int; StateRef string; ListenerRef string;
    ConnectionCount int; SecretBytes int64 }
type Grant struct { Enabled bool; Generation int64; Epoch int64;
    AppliedRevision uint64; Keys []Key }
type Frozen struct { state *frozenState }
func (s *Server) ExpectFlow(id [16]byte, tuple rawsocket.Tuple) error
func (s *Server) SetFlowRevoke(fn func([16]byte, string)) error
func (s *Server) Freeze(deadline time.Time) (*Frozen, error)
func (f *Frozen) Export() (Image, *sealedstate.Snapshot, error)
func (f *Frozen) Thaw() error
func (f *Frozen) Discard() error
func Restore(im Image, src rawsocket.FileSource) (*Server, []handoff.Resource, error)
func (s *Server) ActivateRaw() error // 保持 AuthorityPending
func (s *Server) Authorize(g Grant) error
func (s *Server) RevokeAll(reason string) error
~~~

Config.MaxFlows=32、MaxPendingFDs=32、MaxEndpoints=128；生产只使用这些冻结值。已有 NewServer(TokenLookup)、Client 的 NATS 控制入口与 SS SetKeys API 保持可用；legacy callback 不获 v1 的隐含可取消承诺。生产 bounded TokenLookupOperation 与 legacy tokenLookup 调同一个验证 helper；v1 每次 REGISTER/RESUME 确实重新执行其全部校验，固定测试计数此调用。Authorize 重做同事务验证并返回完整 keys；缺 bounded callbacks 时不得广告/选择 v1。SS 不 import flow，适配器把 [16]byte 与 flow.ID 转换；flow 不 import tunnel/yamux/agent/NATS。

### 2.2 唯一 frame 布局与 control/data bind

header 均 big-endian，恰 40 bytes：Version:u8、Kind:u8、Flags:u16、BodyLen:u32、FlowID:16 bytes、CarrierGen:u64、Offset:u64。Version=1；BodyLen≤32768；不接受 trailing bytes、未知 kind/flags、整数溢出。Flags bit0=direction（0 broker→agent，1 agent→broker）；bit1=FIN receipt（只在 ACK）；bit2=CLOSE receipt（只在 CLOSE）；其它 bit=0。Endpoint control 的 FlowID=0，Offset=0；DATA/ACK/FIN 才可用 Offset。

| Kind | 值 | body 与约束 |
|---|---|---|
| OPEN | 1 | EndpointID:16, OpenSeq:u64, Intent:32, BindNonce:16, TargetPort:u16；共74 bytes；新 data stream 首帧 |
| OPEN_ACK | 2 | EndpointID:16, OpenSeq:u64, Intent:32, BindNonce:16, Status:u8, Reason:u16；共75 bytes；status=1 pending/2 connected/3 rejected |
| RESUME | 3 | §下文固定 prefix、ticket、entries；endpoint control 首帧或恢复 data stream 首帧 |
| RESUME_OK | 4 | 与 request 绑定的 verdict、owner/carrier、ticket、retirement、authority 分页；完整收齐前不 attach |
| REJECT | 5 | RequestID:16, Reason:u16；不产生 Dial/ownership 副作用 |
| DATA | 6 | 1..32768 raw bytes；Offset 为首字节位置；不得超 FIN |
| ACK | 7 | 空 body；Offset=RxApplied；FIN receipt 表示原 CloseWrite 已执行 |
| FIN | 8 | 空 body；Offset=最终 source End；重复相同 FIN 合法，冲突拒绝 |
| CLOSE | 9 | Mode:u8, Reason:u16, FinalEnd[2]:u64, Applied[2]:u64；共35 bytes；Mode=0 normal/1 read_abort/2 write_abort/3 revoke |

Reason=1 malformed、2 offset_gap、3 offset_expired、4 intent_conflict、5 unknown_flow、6 retired、7 revoked、8 stale_epoch、9 owner_conflict、10 capacity、11 unauthorized、12 deadline、13 unsupported、14 io_failure、15 nonce_exhausted、16 state_invalid。0 不得用于 REJECT/错误 CLOSE。Intent=SHA256("tether-flow-open-v1\n" || EndpointID || FlowID || OpenSeq || TargetPort)，整数按上述编码。

RESUME prefix：Scope:u8（0 endpoint/1 flow）、Mode:u8（1 initial/2 reconnect/3 prepare/4 activate/5 abort/6 bind）、PageIndex:u16、PageCount:u16、RequestID:16、EndpointID:16、OwnerGen:u64、NextOwnerGen:u64、CutID:16、ImageDigest:32、TargetPort:u16、TicketLen:u16、Ticket bytes≤512、EntryCount:u16。各 entry：FlowID:16、OpenSeq:u64、Intent:32、TxBase:u64、TxEnd:u64、RxApplied:u64、RxReceived:u64、TxFIN:u64、RxFIN:u64、FINFlags:u8、OpenStatus:u8。计数必须恰匹配 body；PageCount≤64、aggregate≤2 MiB/node；重页只能同 digest，不允许部分页生效。

RESUME_OK prefix：RequestID:16、EndpointID:16、OwnerGen:u64、CarrierGen:u64、ImageDigest:32、GrantDigest:32、Verdict:u8（1 accepted/2 pending/3 rejected）、Reason:u16、TicketLen:u16、Ticket≤512、PageIndex:u16、PageCount:u16、NextOpenSeq:u64、RetiredThrough:u64、HoleCount:u16、holes:u64[]、EntryCount:u16、entries（同RESUME entry布局，取peer本端状态）；authority 后缀：Enabled:u8、Generation:i64、ProxyEpoch:i64、AppliedRevision:u64、KeyCount:u16，逐 key 为 IDLen:u16+UTF-8 ID≤128 bytes、SecretLen:u16+Secret≤512 bytes。所有页带同request/ImageDigest/GrantDigest，GrantDigest覆盖完整authority、entries与retirement快照；分页KeyCount/EntryCount为本页数，完整集合在收齐/验digest后一次生效。秘密只在 TLS 与 sealed private state。Generation/Epoch 非负，dispatch/applied 另按 §7 校验。集合 ID 不重复，材料别名允许。

agent 在 exact REGISTER/OK 后打开第一个 yamux stream 作为 endpoint control；broker 接受并强制首帧 RESUME initial/reconnect/activate。初始 header CarrierGen=0；成功返回的 CarrierGen 是唯一此 attachment 的代号，后续帧必须匹配。broker 为新 flow 打开 data stream，以 OPEN 绑定；恢复 data stream 由 broker 打开，以 Scope=flow/Mode=bind 的 RESUME 绑定已获 endpoint grant 的 FlowID/intent/owner/carrier，agent 回 RESUME_OK。bind 不能消费 handoff permit、不能制造 unknown flow，DATA 不得抢在 ACK/bind 成功前。initial RESUME的TargetPort是agent已批准LocalPortLookup结果，broker保存后用于OPEN，agent重验它与allocation映射一致；后续模式不得改变，禁止任意localhost端口。每 flow、每 attachment 至多一个 data stream；重复绑定采用同一 receipt，不并行写。

ACK/FIN/CLOSE 在 control stream 传送；DATA 在各 data stream。control 与 DATA 无全局先后保证，receiver 以 offset/final-end 等待而不是把 FIN 提前写成 EOF。frame goldens 独立于 encoder 生成，wire inventory 记录 byte offset/type/width/flags/modes，不借 proto JSON inventory 代签。

## §3 internal/flow：字节、OPEN、尾流与背压

### 3.1 offset 不变量与 dedupe-before-write

每端只保存本端 source 的 Tx replay：[B,E)；B 为已确认释放的 Base，E 为已接纳 source End。接收端 A=实际成功写入原业务 socket 的连续字节终点，R=已完整接纳到 pending 的连续终点，A≤R，R−A≤32768。image 同时携带 A/R 与 [A,R) 内容，不能拿接纳或 carrier delivered 代 applied。n>0 与 error 同时出现时先按 n 记账，剩余 suffix 与错误一起保存；source Read 后尚未 Append 的 TxPending 也必须保存。

收到 DATA(offset,payload)：先验 carrier/owner/FlowID/intent，再安全检查 offset+len；offset>R 拒 gap；已在 [0,A) 的前缀在任何 raw Write 前剔除；与 retained [A,R) 重叠部分逐 byte 对比，不同即拒；仅新增后缀入有界 pending。一个 writer 写 pending，按实际 n 原子推进 A、删除相应 pending、提交 ACK。重发 offset=A−1 且内容一致是合法正控，不是错误-offset负控；已经 applied 且不保留的历史不再试图比较，但绝不重写。

收到 ACK a：要求 B≤a≤E，推进 Base；不得要求 a≤本代 Sent。旧代已写入但 ACK 丢失的数据，RESUME 返回 a 可以高于新代尚未发送的进度。低于 B 的恢复要求 offset_expired，拒绝而不重拨目标；高于 E 是 malformed。carrier Write 返回值不是交付事实，yamux 超时后的 body 可能已经入队，全部从 peer applied 恢复。

接收信用由本地有界 pending 和持续读 frame 的调度实现，不另引 SS 4+4 credit。DATA frame 整包接纳，空间不足不部分接纳后谎报完整 ACK；raw writer 慢时可暂停该流 data reader，但 control reader 和其它流仍运行。任何信用值若内部保存，必须为绝对 A+capacity、单调且不低于已授值，不以 free bytes 当绝对 offset。

### 3.2 OPEN journal 与不可再 Dial 的退休证据

FlowID 是 crypto/rand 生成的128-bit随机值，不复用；Endpoint 的 OpenSeq 从1单调增长，溢出拒绝。接受公网 FD 后先登记 FlowID/OpenSeq/intent，再发 OPEN；agent 在目标 Connect 之前登记同样的 opening owner。重复 OPEN 同 ID/seq/intent 只返回当前 pending/connected/failed receipt，不重复 Dial；同 seq 不同 ID/intent 拒绝。

RESUME 对照两侧 journal：OPEN 从未到达时，broker 可以重发原 ID/seq/intent 的 OPEN，不能生新 FlowID；agent 已 connecting/connected 但 OPEN_ACK 丢失时返回原 receipt 与原 FD；只有已知 journal 的 flow 可 bind。目标初始 connect deadline 由 agent 第一次接纳 OPEN 时写入本机 BOOTTIME，跨代不延长，不在跨主机 wire 上传本机 boot deadline。

唯一 connector 消费 SO_ERROR，把成功/失败值持久化后才完成 receipt。ValidateFile 只验类型、O_NONBLOCK/CLOEXEC、fstat/私有 tuple/原连接状态，不能消费 SO_ERROR，也不能把失败读取成零再等或重 Dial。EINPROGRESS 可导出原 socket 与连接阶段，导入继续原 connect；DNS 未有可恢复决定、未追踪 FD 等不可表示状态必须在 cut 前拒绝。

终态以 NextOpenSeq、RetiredThrough 和 bounded holes 保留退休证明；terminal receipt 可 GC，但 seq≤RetiredThrough 或 holes 中退休 seq 的 OPEN 永远拒绝。holes≤1024/allocation、terminal metadata≤4096/node；满额暂停新 OPEN，不驱逐会允许旧 OPEN 再创建的证明。allocation 关闭后由 fences 拒所有旧 seq，owner refs 清零才可清表。CLOSE_ACK/TTL 不是遗忘旧 OPEN 的许可。测试必须在 receipt 删除后重发真实旧 OPEN，并独立计 target Accept=1。

### 3.3 FIN、abort 与 hard revoke

source 真 Read EOF 才记录 TxFIN=E；n+EOF 先保存 n。FIN 可早于 DATA 抵达，接收端记录 F，继续接纳≤F 的缺失数据；仅 A=F 且 pending 空时执行原 TCP CloseWrite 一次，再发 FIN receipt。FIN、CloseWrite 已执行位、两向终点跨 image 保留。read EOF 不等于目标 RST，不关闭反向；half-close 之后反向尾流必须能超过8 MiB并跨 cut，直到独立 End/FIN/receipt。

CLOSE normal 要求双方 FIN 与 applied 到各自最终 End。read_abort 固定 End、join source read、保存已读 suffix 后允许对端 drain 或明确 discard receipt；write_abort 保存 A<E 与错误，不能伪称 tail 已交付。任何 terminal receipt 包含两向最终 End/Applied/FIN/错误；迟到 ACK 不改变终点。hard revoke 是授权事件，可立即 shutdown 两向并明确丢弃未授权尾流，不能把它统计成 survivor 的零丢包成功。

### 3.4 replay 与非 replay 的总容量

同一 agent 节点的 exec stdout/stderr、PTY replay 与 flow send replay 共用同一个 *workload.ReplayBudget，Limit 固定256 MiB；每 log capacity=8 MiB，包括空 log。每 host 每 flow 只一个 send log，双端总量不能错算成本机两个。ACK 释放 retained space，Close/Detach 释放整块 reservation。各域 image.ReservedBytes 与 fence.DomainUsed 都为本域仍拥有的 capacity 之和；所有域之和才对比共享 Used。import 不读旧域 Limit 覆盖新共享 Limit，不重复收费/释放。

一个空 listener 不预留8 MiB。公网 accept 后 FD 立即进入有界 pending owner（node≤32）；公平 round-robin reserve 成功才创建 replay、发送 OPEN 或 Dial 目标；队列满停止下一次 accept，以可唤醒 deadline/poller 暂停，不关 listener。不无限积累等待 reserve 的 goroutine。idle Endpoint≤128；active flows≤32/node。预算不足是明确 capacity/backpressure，已存在 flow 不被杀掉让位。

辅助容量单列：每 flow TxPending≤32 KiB、RxPending≤32 KiB、单 writer staging≤32 KiB；control queued≤64 KiB/Endpoint、≤4 MiB/node；RESUME pages aggregate≤2 MiB/node；一个 pending permit/allocation；SS actors≤128/node、材料历史≤64/node、每材料8192个 salt；所有非 replay 持续数据合计≤64 MiB/process，sealed state≤32 MiB/cut，编码与 Prepared dup 的瞬时峰值另记。yamux window 固定256 KiB/stream，stream数限于 admitted flows/control/有界握手，核算其 buffers，不能用 replay budget 冒充全部 heap 上限。

预检按真实 RLIMIT_NOFILE 和 handoff 16384 上限计活 FD、cut-owned sealed FD、Prepared dup、导入 salvage；逐类计数，不能仅总 FD 相互抵消。超字节/FD预算→image_too_large；未知语义状态→ss_state_unrepresentable。正常75 profile预声明 replay reservation≤224 MiB，留下至少32 MiB给并发短流；固定 cohort约64 MiB，加exec/PTY24/16 MiB与在途短流仍须实算。饱和 profile另测容量拒绝与 ACK/Close 后恢复，不拿故意 slow/saturation 洗正常5s。

### 3.5 yamux window 与 flow ACK 的实际死锁分析

wait-for 图：慢 raw sink→A不进→send replay满→source停读；若同一 owner 同步写 control→sendCh/共享 TLS writer满→control Read 的 sendWindowUpdate 阻塞→ACK/FIN/RESUME读不到→所有 flow 无法释空间。独立 control stream 消除部分应用互等，但仍共享 yamux 的 sendCh=64 与 writer，不能单凭分 stream 证明无死锁。

每个 flow actor 不在任何锁内调用 yamux Write；control reader 只更新有界状态并非阻塞提交 ACK（每flow合并最高A），FIN/CLOSE优先但公平；一个 DATA dispatch至多32 KiB后让出，独立 carrier writer worker 执行。raw writer 与 carrier worker 不互相等待 callback。v1 ConnectionWriteTimeout=1s，legacy保持原默认；control健康 watchdog 按绝对1s预算监督共享 writer，超时先 fence 此 attachment、锁外 Close 底层 TLS conn，随后 yamux.Close，再 join所有 reader/writer。不能先 Stream.Close，也不能仅 SetWriteDeadline/ctx cancel。

冻结同样先停止新 submit、fence attachment，再锁外关闭底层 TLS；单次操作总 deadline从入口开始，排锁/排worker都消耗它。Freeze失败只 Thaw原 owner，不启动第二泵；不得 Close原业务 socket作为成功 safe point。完整冻结目标500ms是校准，正常flow恢复≤5s硬判。测试以最小 yamux window、塞满64 send slots、真实 TLS Write不前进、一个慢sink加一个快flow证明：Close唤醒共享writer/window-update，所有worker可join，快flow不被默认10s等待拖死。

## §4 tunnel：协商、Endpoint、权限、票据与 reaper

### 4.1 ALPN 与 N-1 纪律

Linux v1 两端配置 TLS NextProtos=tether-flow/1；只在完整 flow owner、bounded lookup、恢复能力可用时广告。无交集必须进入legacy raw。新server在GetConfigForClient检查ClientHello：存在共同tether-flow/1才保留该NextProtos，否则clone为NextProtos=nil，保留原certificate/VerifyConnection/pin配置；不能依赖Go默认协商自动成功，默认非空无交集会发no_application_protocol（本机/usr/local/go/src/crypto/tls/handshake_server.go:334–360、handshake_server_tls13.go:301）。new client遇严格旧server的这一精确remote ALPN alert，仅在尚未REGISTER、未有v1 retained flow时允许一次NextProtos=nil重握手，所有原TLS/pin配置不变；按net.OpError的remote-error层与精确alert错误匹配，不用substring或任意TLS-error fallback。旧server空ALPN无需重试。最终SelectedProtocol为空才进入legacy raw；已持v1 flow的redial不可降级。cert/pin及其它TLS错误是失败，不能捕获后关闭pin或重新连成legacy。选中 v1 后协议错误/容量/权限拒绝都不 fallback。

REGISTER 仍精确 REGISTER sid nid port token epoch\n，六字段、epoch非负；服务端只发 OK\n。新 agent 保持旧 reader 对末尾 CR/LF 的接受集合，不扩大到 OK flow-v1。服务端 REGISTER reader 和客户端 OK reader 后面的已预读 bytes 通过 reader-backed net.Conn 继续交给 yamux；同 packet 包含 OK+yamux header 的真实 TLS 测试不可省。legacy stream 首业务 byte 不带 frame header。

四象限单独记收据：new/new=v1；new broker/old agent=legacy；old broker/new agent=legacy；old/old=legacy。额外测 unsupported ALPN无交集、wrong pin/坏证书、selected-v1 malformed不得降级。固定 v0.6.0/a3431a1 历史供给与动态 N-1 分开；动态相邻 release必须记录 tag peeled commit、build flags、artifact SHA与实际 running SHA，不把旧CLI当旧broker。

### 4.2 Endpoint 的独立 owner

Server持 Endpoint map；Endpoint持 public listener、已 accept公网 sockets、flow Manager、retirement、owner/carrier generation、首次 detach deadline、permit及 fence refs。serverSession仅持 TLS/yamux attachment与取消/join责任，不拥有 Endpoint listener/pubConn。Endpoint阶段=1 Binding、2 Attached、3 Detached、4 Prepared、5 Closing、6 Closed；closed不能再attach，Detached/Prepared仍是端口占用和SessionInfos中的live责任。

REGISTER：锁内取完整 port/SID/allocation fence snap、拒 forgotten/closedAllocation、登记inflight；找到同 allocation Endpoint先保留attachment槽。仅无Endpoint时，创建唯一 Binding reservation，锁外 bind，随后锁内重查fences发布listener。已有Endpoint不再bind。OK前再查权限/fences；失败关闭本次carrier/新建且未发布的listener，不伤旧Endpoint。成功建立yamux后必须RESUME通过才CAS attach；新carrier不能通过“完成TLS/REGISTER”先踢旧attachment。attachment替换在锁内fence旧gen，锁外Close旧TLS及join。

所有 CloseProxy、CloseProxyIf、CloseSession、ForgetSession 与 Server.Close 都覆盖 Attached/Detached/Prepared/Binding/accepted pending，而非只巡sessions。killGen*、forgotten、closedAllocation在lookup前与attach/CAS前双重检查。inflight、Endpoint owner、permit、cached receipt、grace timer都持ref；prune必须全部ref=0。close返回值按实际关闭的allocation/端口责任，不因sessions空误报false。timer与callback先摘表/标closing，锁外shutdown/join；cleanup actor不self-join。错误注册不能重置grace或使其他owner失去listener。

### 4.3 有界权威快照与 RESUME 再授权

production TokenLookupOperation、ResumeAuthorizer调用同一个 bounded read helper：通过deadline/Stop创建本操作的ctx-none root，在单一SQLite read transaction内读取session/node活性、allocation(scope/port/token hash/home/epoch)、Enabled、完整Keys、proxy Generation/Epoch、AppliedRevision与已知replication-lag判断。所有字段来自同一snapshot；任何读错、known lag、负数/overflow、disabled、missing、scope mismatch都拒绝。legacy验证也用此helper；v1每次REGISTER/RESUME/bind/缓存receipt重取权限，不能把上次结果当fresh。

lookup预算从调用入口起≤1s，包括DB连接池与锁等待；SQL QueryContext/BeginTx使用同一截止，busy等待不能超过余量；Stop取消后必须join操作并release inflight/ref。测试真实堵SQL/stop证明，不允许把无界旧callback/Future.Error装进goroutine再timeout丢弃。v1缺这项保证时配置失败、不给v1能力。Authorize grant返回完整当时keys而非仅AllowedKeyIDs，rotation/newauth才能使用当前材料。

该合同是可证明的本地applied权威，保留既有bounded-stale一致性范围；已知落后不放行，不能从一次本地positive row推出leader尚未应用/已提交的撤权。AppliedRevision来自SQLite命令域，不混用Raft log index。测试三节点已知lagpositive拒绝、读期间撤权没有mixed grant、旧leader不获“线性一致”标签。Q1若要求leader提交即全局撤权，lead必须先修订范围和真实可取消read barrier接口；实施者不得暗加Raft/NATS字段。

### 4.4 ticket、prepare/activate 与丢回复

ticket由Server当前boot随机HMAC key签发，绑定allocation五元组、EndpointID/incarnation、issuer boot、agent instance/OwnerGen、用途、ResumeID/CutID/NextOwnerGen。ticket与完整authority只进TLS/agent sealed state；不进marker/日志。current-owner ticket不在长期idle期间因一个短TTL失效；第一次detach时启动绝对8s截止，失败reconnect与重复REGISTER不续期。超时关闭Endpoint/业务FD并释放budget；闭allocation拒旧票，旧generation不得创造新owner。

initial仅无现有owner时允许无ticket；reconnect只恢复当前OwnerGen。planned handoff在原业务仍运行时由旧carrier PREPARE，指定NextOwnerGen=OwnerGen+1与CutID，保存唯一一次permit；TTL=120s从首次prepare起算，不能续期；不在此时转移业务owner。final cut记录ImageDigest；candidate完成本地image/FD/lineage验证后RESUME activate携相同CutID/digest，由Server重查权限/fences并一次CAS消费permit、安装新OwnerGen/CarrierGen。raw-active未RESUME不代表已获grant。

缓存activate/reconnect receipt仅允许同RequestID、intent、cut、owner的重试，仍需fresh tokenLookup/Authority/fences；cached success不能绕已撤权。permit已消费但reply丢失，用同ResumeID查询原CAS结果，不再递增generation；未消费时旧owner exec失败本地恢复，网络ABORT可异步有界重试，不能等坏网络才Thaw。消费后若candidate需要回迁，以当前active state生成下一代permit/cut；不能重发最初image或旧票夺回owner。pre-freeze PREPARE失败保持业务运行并明确延期；不可用网络下不得伪称已成功迁移。

### 4.5 detached grace 与 planned proxy_ready

默认reaper interval=5s、proxyRehomeDwell=3；三次坏观测最短跨度=(3−1)×5s=10s，8s严格小于它，而非误算为15s。prepare只允许home健康、Endpoint真实attached、最近节点ready真实成立。planned freeze保留最后真实ready latch，最多8s；新boot在local cut期间不向broker发假false，RESUME完成当前授权与attachment后才发true。revoke/disabled/真实失败立即false；grace到期不得继续latch。非计划loss沿正常诊断与恢复，不能全局暂停reaper/延长OfflineAfter。

TestReaperDoesNotRehomeDuringPlannedCarrierRebuild用真实reconcile与SQL snapshot：健康home；历史dwell=2但已恢复ready；最坏tick相位；planned cut和carrier重建；至少覆盖10s观测窗；port/token/home/epoch及rotation/rehome事件均无变化。负臂关闭latch或把grace放到≥10s必须rotate；unhealthyhome仍正常rehome。不要只断言agent bool；默认reaper常量不改，S6维护态另行实现。

## §5 image 域、FD 所有权、finalPrepare 与失败表

### 5.1 Bundle 与资源闭包

Bundle 新增 Flow *tunnel.ClientImage，json:"flow,omitempty"；SS *ssproxy.Image，json:"ss,omitempty"。Version仍1；两个nil表示没有该域任何保留责任，而非只表示active count=0。Flow涵盖Endpoints、opening/connecting/active/draining、pending accepts、退休水位、permit与cached receipt；SS涵盖listener、actors、key table、材料/盐历史、revoked/cleanupPending、authority待确认与绝对timer。已无actor但仍有salt/key/allocation责任时SS非nil，不能清空以迁入旧reader。

Manifest只含public元数据、opaque resource refs及非秘密计数；业务plaintext/ciphertext pending、SS keys/master/subkey/salt/nonce/target、票据与完整grant都进独立sealed memfd。资源命名固定：flow.<FlowID hex>、flow-endpoint.<EndpointID hex>、flow-state.<CutID hex>、ss-conn.<ConnID hex>、ss-state.<CutID hex>、ss-listener.<instance hex>；file name只用raw、replay、state、listener、upstream、loopback。无冒号，ID唯一且只由typed ID产生。

流的raw public/target FD、SS listener/client-loopback/upstream FD、connecting FD、send replay均原OFD迁移；TLS/yamux/carrier不迁移、wake/poller新建。raw descriptor验证fstat dev/ino/type、socket状态、flags、私有tuple和connect receipt，拒互换/非socket/错tuple/缺CLOEXEC/blocking；不使用File.Fd改变O_NONBLOCK，不导入修flags去凑通过。private连接状态不能被普通Resource.Data泄露；Resource.Data只放版本及resource kind/fingerprint允许项。

sealedstate.New用Linux memfd_create、CLOEXEC与F_SEAL_WRITE|F_SEAL_GROW|F_SEAL_SHRINK|F_SEAL_SEAL；导入验seals、owner/type/size/version，再pure decode再Take业务FD。不用普通tempfile、env、marker JSON、磁盘secret副本。secret binary有declared字段ID、width、版本、边界；reject重复字段、unknown enum、trailing、溢出、缺必填。每个实际encoder/decoder字段都对账，不以opaque Ref覆盖真实schema库存。

### 5.2 域检查：严格沿 S4 §5.2 三道门

version首行兼容既有smoke；中间domains行恰 image-domains exec-v1 pty-v1 flow-v1 ss-v1；最后 image-abi v1。拒重复domains/ABI行、畸形token、未知ABI、无界输出/timeout；能力与probe binary SHA绑定。Darwin版本可列reader能力，但不得以此广告Linux v1可运行能力。

第一道：候选下载/smoke后，用当前live census、registry/retirement/keys/salts/cleanup判所需域，检查candidate及将实际成为prev的reader。先hash实际dst，继续允许dst SHA属于running或candidate；第三SHA/不能hash拒绝。存在live flow或SS而candidate/prev缺flow-v1/ss-v1→image_abi_unsupported(64)，此时零marker/prev/dst变更。不存在这些责任时新域nil，纯exec/PTY可读旧reader。

第二道：关闭admission并Freeze后，对真实Bundle及所有retained责任再检查candidate/prev；不能仅扫描Tasks/active sockets。第三道：PrepareChecked最后重Export Runtime+Flow+SS，连同late revoke/cleanup/fired状态重新Validate/domain/size；采用封闭cut所需域做检查，而非缓存preflight。后两道若处在S3已flip阶段，按其失败表Thaw、恢复dst、写rolled_back/真实phase；不能称“零磁盘变更”。

pending transaction若prev不读新域，必须在public accept、flow journal、SS listener/key table/盐历史、proxy apply订阅副作用前拒绝新domain admission，返回真实agent_upgrading(75)。不能把capability降为0诱导legacy，不可在MarkReady但commit写失败后开门。只有durable commit或prev确有domain才能开放；sibling marker不误关别的instance。既有盐历史可能持续到allocation删除，不能承诺“等流空闲即可旧版回滚”。

### 5.3 freeze、reExport 与 snapshot owner

先关闭admission，旧业务仍运行时为有责任的Endpoint并行PREPARE；失败不进入cut。随后停止新的proxy apply/dispatch admission并等待在飞操作记账；记录timer fired/cleanup决定；同时请求SS/flow到safe point，独立设置两向deadline，避免先等SS Write而其peer已停造成互等。fence并锁外关闭carrier→join；暂停raw/SS actors且原业务FD仍开；沿S4既有Runtime→Manager/spawn/courier顺序完成cut。所有等待共享入口绝对deadline，锁内不join。

finalPrepare重Export全部可变dataplane域，保持courier的既有late-delivery合同；多次Export不累积sealed FD。final-cut confirmation是sealed本地记录：CutID、permit目的/NextOwnerGen、ImageDigest、FD资源清单digest、fresh revoke/fired责任均匹配；activate只消费这个cut，Server第一次CAS把digest钉入receipt，迟到不同digest拒绝。不引入全域冻结后的旧端网络commit。

Frozen owns每次新生成sealed state；业务raw/replay originals是borrowed。重Export先生成并验证新cut，成功替换后关闭旧Owned sealed FD；失败关新sealed且保旧cut。Prepared只own自己的dup；Prepared.Close不关original。Thaw/Discard关闭cut-owned sealed FD；Thaw不得关raw/replay。Restore.Take一次，失败salvage包含所有已Take业务FD与仍在Restored中的FD；SS第二actor失败不能丢第一个upstream/listener/replay。新wrapper成功接管后旧wrapper失去Close责任，不造两个活动netpoll owner。

≥5轮Export/reExport/Prepare失败/Thaw/Discard测试在同一活进程、业务继续通信时核每类FD与budget；不得等receiver退出后数FD。

### 5.4 held/imported/raw-active/resumed/ready 失败矩阵

| 窗口 | owner与授权状态 | 唯一恢复动作 |
|---|---|---|
| PREPARE失败、未freeze | 旧owner/旧carrier仍运行，permit未生效 | 不flip、不关业务；明确延期或前置错误，网络abort有界且非阻塞业务。 |
| frozen、未exec或syscall.Exec失败 | originals仍旧owner，Prepared dup可关闭；SS必须AuthorityPending | Thaw本地raw/runtime，恢复current-owner carrier并fresh Authority，再开放SS；ABORT不能阻塞Thaw。 |
| held /纯校验失败 | 无FD已Take、无pump激活 | handoff.Restored.Repack原bundle回prev；不能Close原业务。 |
| imported、全部仍冻结 | Take一次、无业务副作用，permit可能未消费 | Detach/salvage全部FD回派；未消费permit可abort，current-owner重授权。 |
| raw-active，RESUME未消费permit | 已有local pump/offset推进，属于active责任 | 禁pristine Detach；当前态Freeze下一cut，owner信息保留；未知SS授权不写target。 |
| activate已消费、reply丢失 | Server已新owner，candidate未见receipt | 重试相同ResumeID并fresh auth，取原receipt；不第二次消费/递增。 |
| resumed，首NATS register未成功 | 当前新owner，SS凭fresh grant可运行 | watchdog已预先armed；NATS不决定原raw存活；逾期当前态回迁。 |
| ready但commit落盘失败 | marker pending、watchdog仍armed；不能开旧prev不读新域的admission | 保当前业务，deadline真watchdog第二cut；不能MarkReady当committed。 |
| 任一partial activation失败 | 已激活部分由current owner负责 | 再Freeze当前offsets/nonces/revoke，generation+1回迁；原image仅作谱系，不能作数据恢复。 |
| fresh auth拒绝/撤权 | 不可逆Revoked、CleanupPending仍保留 | shutdown对应raw+dup并显式flow revoke，清责任；任何rollback/Thaw不复活。 |
| 无carrier/坏网络且cut不安全 | originals活、未知授权hold、有界buffers | 保活、真实rollback_blocked/错误phase并重试；不能为了报告回滚而Close业务。 |

Safe point超时是失败cut，恢复原owner并走既有installer失败回收，不冒充某个不适用的deferred reason。DNS/未登记spawn确属spawn_in_flight；未知SSactor为ss_state_unrepresentable；容量超限为image_too_large；legacy责任为legacy_session_busy，四者不混用。

## §6 ssproxy：freezable actor、AEAD、盐、轮换与撤权

### 6.1 actor 状态与 safe point

每条SS连接只有一个actor负责parser/加解密决定；raw IO可各向独立worker，但n、cursor、nonce、receipt由同一guard提交。actor阶段显式closed enum：salt、length、payload、target、connecting、relay、draining、revoked、terminal；unknown不得default relay。ordinary legacy SS仍可用，Linux v1 actor进入image，Darwin不走Linux replay/memfd路径。

必须导出private字段：ConnID/FlowID；active material fingerprint/key version；rx salt32及已读cursor、encrypted length/payload buffers与cursor、decoded length、rx nonce12/exhausted、已Open结果及clear pending；target ATYP、host length、host/address/port bytes及cursor、header跨多个chunk的parser phase、first-payload suffix；原handshake BOOTTIME deadline；policy结果、完整已批准resolved IP候选、candidate index、原connecting FD与已消费SO_ERROR receipt；upstream RawReadPending；tx salt与wire cursor、tx nonce12/exhausted、已Seal length/payload标记、完整密文与真实write cursor；两向EOF/CloseWrite/applied决定；revoked/fired/cleanupPending/authority阶段。没有这些字段的cut不能说可表示。

域名解析用SS自身可取消操作root，不绑caller ctx；inflight DNS未能在deadline内join并形成可导出决定时ss_state_unrepresentable。已批准地址及policy跨代保留，不能重新解析绕private-target策略；connecting原socket恢复，不重Dial。private策略检查失败的target不得写日志中的明文host/IP/keyID。ErrStateUnrepresentable与ErrCapacity分别走ss_state_unrepresentable与image_too_large，不能用同一reason覆盖。

Freeze只暂停，不Stop、不Close原listener/业务socket，不等peer ACK。先把raw syscall已发生的n写入actor状态，停在AEAD primitive及其结果保存不可分割的边界。真实partial Write时0<n<attemptLen，image保存剩余cipher suffix；仅“buffer非空”不是已发生short syscall的证据。

### 6.2 nonce continuity 与独立密码oracle

保持既有chacha20-ietf-poly1305、salt32、tag16、maxChunk=0x3fff、HKDF(master,salt,"ss-subkey")，不偷偷变SS wire。nonce为12-byte little-endian计数；length与payload各消费一次。Open/Seal和结果/nonce advance在同guard原子提交；最后nonce可用后设置exhausted，下一primitive明确nonce_exhausted，不wrap回零。

已Seal密文在raw Write失败/partial/跨exec后原样写suffix，不能再次Seal；已Open plaintext未写target也原样恢复，不能再Open并额外advance。Rx与Tx的nonce值均导出，方向是状态标识，不是密码域隔离。测试ledger以(actual subkey bytes digest, nonce bytes)为唯一键跨所有actor/方向/KeyID；同一primitive对相同数据重Seal也算重复。独立reference AEAD parser与固定ciphertext golden生成预期，不让被测reader/writer彼此自证。

### 6.3 actual material 盐历史与 response reserve

Salt ring归属actual master材料fingerprint（SHA256带固定domain前缀），同PSK不同KeyID合并，HKDF实际subkey相同就不得用ID/direction区分重复。每材料FIFO8192，head/count是本阶段新增；image保存salt内容、顺序、head/count及材料版本。另有所有live actor读/写salt集合，不能因ring滚出就允许碰撞live nonce域。

inbound首次trial成功后，在active-material重验与salt reserve的同guard内绑定；duplicate拒绝且零target Dial/Write。response salt从crypto/rand产生，必须原子reserve，对inbound/current历史/live集合冲突则重新生成，受控RNG测试真实碰撞。两方向可能同材料同salt，不能靠direction“允许”。已退出actor、ID删除、同ID新材料、旧材料重加、alias添加与代际切换均保留既有材料ring，直到allocation责任终结。

材料历史总上限64/node，keys≤256/node、actors≤128/node；资源按§3核算。8192是有界重放窗口，不能称永远记住所有旧盐；FIFO淘汰但live保护永在。历史满时不能悄悄GC revoked材料去开放旧重放，也不能无限增长；拒新认证/新材料并给控制面真实失败。Q3推荐冻结此配额及拒绝策略，不能由实施者选择静默遗忘。

### 6.4 rotation、新认证与旧合法连接

SetKeys取得完整current keyset后先不可逆撤销所有已不存在的ID，即使新材料因容量失败也不能保留被删A。旧合法连接固定原材料/subkey，只要其授权ID仍在当前grant可继续（同ID换新PSK不重置旧连接nonce）；新握手必须trial当前材料，并在bind时再次核对版本/fingerprint，不能ID-only批准旧PSK。别名删除按连接授权ID撤销，材料ring按actual材料保持。

same Generation/Epoch只在完整keyset digest相同且已成功应用时幂等ACK；不同digest视authority conflict fail closed。SetKeys容量/其它失败不记lastApplied、不发成功pair ACK，记录有界结构化错误及count，后续同pair完整snapshot可重试。已绑定survivor的ready按真实listener/carrier状态，不用假unbind诱导reaper；新认证fail closed。broker register与push共用同一snapshot builder；agent两条入口均proxyApplyMu序列化，dispatch/applied成对保存。

### 6.5 revoke 不等于 freeze

BeforeLocalConnect先bind本地ephemeral socket并登记ExpectFlow(FlowID,Tuple)，再connect SS loopback；ss accept按此受控reservation消费一次，保存ConnID→FlowID。tuple只用于一次绑定，不用猜远端地址长期找flow。未知/冲突绑定不得开启v1 SS业务；on-revoke callback只提交typed flow revoke，不在SS锁内调用会join的方法。

revoke先原子Revoked与CleanupPending，禁止新trial、Open/Seal、target Write；锁外SHUT_RDWR原loopback/upstream OFD，让sealed/Prepared中的dup也失效；显式Client.RevokeFlow同FlowID，broker终结对应pubConn与send replay。静默victim无Read/fixture Close也必须清场。其他ID survivor不中断。revoke发生于pre-freeze/frozen/finalPrepare/import/raw-active/active均不可被Thaw/rollback清掉。

fresh grant前SS为AuthorityPending：raw owners可恢复记账/有界接收，但不解密转发或追加target plaintext。exec失败旧Server也进入此状态，不能直接恢复旧keyset写权限；RESUME拒绝即硬撤销，RESUME不可用暂hold有界buffer并诊断。原fail-close deadline仍是BOOTTIME绝对值，Fired/CleanupPending持久化；callback指针nil不是尚未触发证明。

### 6.6 秘密分类与公开载体禁令

PSK/master/subkey、KeyID、盐、nonce、目标host/IP/port、认证grant/ticket、target与cipher/plain pending都视secret；manifest JSON、Resource.Data、marker、argv/env、普通文件、fmt各verb、import errors、blocked-target/revoke生产日志均不出现原值/hex/base64。opaque refs、计数、typed reason允许。ssproxy已有keyID/target日志必须改为不可逆诊断digest或typed reason。SS secret二进制只在sealed memfd，Summary/String/Error redacted；FD密封并不许可把秘密直接放manifest JSON。

secret测试实际走Prepare/Restore/拒绝/blocked-target/revoke/Exec失败路径，读取真实外层manifest、所有Resource.Data/marker/日志/errors，多种高熵secret逐项扫raw/hex/base64。每种泄漏载体有mutation，不把“struct只有Ref”当测试。codec descriptors与actual encode/decode双向库存，删nonce/cursor/salt FIFO字段必须红。

## §7 agent restore、proxy 接线、忙码删除与 Darwin

### 7.1 恢复顺序与 deadline

Agent.New最后一个可失败步骤仍是restore；New中不启动goroutine、不redial、不register、不重新Start SS listener。SS Restore接管原listener、flow Restore接管原raw/replay，manager/actor全部冻结。Run先安装Run-exit cleanup与pending watchdog，再进入本地activation；任一资源激活即标active，不能在网络失败后Detach原始image。

固定顺序：验证/Take完所有资源 → 按S4激活Manager与先处理revoke → 恢复raw pumps（SS仍AuthorityPending） → redial TLS/yamux carrier → REGISTER六字段/exact OK → RESUME重新验证token/fences并取得完整fresh keyset → SS.Authorize与data bind → NATS connect/register/订阅恢复。原flow/SS业务不等NATS首次register才获得RESUME授权；NATS同session重建不杀dataplane owner。不能预置旧keys冒fresh，不能关闭权限门120s来伪签W连续。

active Endpoint优先、并行度32，上限active flows=32，因此不存在8-worker逐批把尾Endpoint拖出5s。每attempt含TLS/REGISTER/RESUME总2s、lookup≤1s；失败retry从250ms增长到1s，首次detach绝对8s不续期。idle Endpoint在active cohort后恢复，慢auth只占本Endpoint的有界worker，不持全局锁。测试满32active并带一个堵auth/大量idle，健康flow全部≤5s；总体未知权限仍fail closed，不声称被拒flow成功。

完整freeze_window_ms从关闭cut admission到最后一个required domain raw pump与carrier RESUME/data bind/SS grant可用，不能在Runtime.Thaw或Manager.Thaw即结束。另记raw_activation_ms、carrier_resume_ms、ss_authority_ms、NATS_register_ms；500ms校准显式max和超标，业务5s独立硬判。NATS ready/commit不混进已完成dataplane的时间归因。

### 7.2 dispatch、apply、timer与锁序

proxy dispatch sequence与lastApplied均int64非负、≤MaxInt64，dispatch≥applied；image成对导出、导入时在任何callback subscription前恢复。register reply的resp.Proxy与push都走同一package-level serialized apply helper，不另加Agent method。pair/keyset digest、原allocation/token/home/epoch、proxy ready latch与FLC absolute deadline/Fired/CleanupPending在SS sealed state，公开manifest不放key材料。

锁序不靠“先p.mu后c.mu”解决join：proxyApplyMu只序列apply决定；p.mu/c.mu/Endpoint/SS key lock内只查表、标状态、取得待清owner。之后全部解锁，执行Stop/RemoveProxy/TLS Close/raw Shutdown/join；callback投递到独立cleanup owner，不调用自身wg.Wait。hook读immutable/atomic snapshot，不回拿p.mu；gate等待从入口deadline算。测试真实Start/SetKeys/Close/Freeze/revoke并发，不能用空callback冒lock-free。

### 7.3 删除 flow_busy/ss_busy 的封闭条件

以S4已删pty_busy的10项为输入，活跃DeferredReasons恰9项且顺序固定：spawn_in_flight、remote_fs_probe_in_flight、foreign_process_rows、image_too_large、runtime_closed、platform_unsupported、transfers_in_flight、legacy_session_busy、ss_state_unrepresentable。移除ReasonFlowBusy/ReasonSSBusy的发射与CLI hint活跃成员，历史文档/收据可保留旧字面量；不保留永远不可达的deferred分支。

升级关闭gate后，Census取拥有FD/metadata的真实owner与inflight并集：opening、pending accept、connecting、carrier attach/resume、SS parser/relay、盐/材料、retirement/timer均有image责任；每个admission在第一项副作用前登记，直到owner接管才减inflight。flow/SS已可表示则允许cut；无owner的legacy raw、unknown custom ExposeAdapter、legacy listener retained slot、foreign socket、无法确认的正在重home责任均不能穿过，legacy_session_busy守未知/legacy，真实transfer守transfers_in_flight。可表示的v1 rehome-control intent须被journal完整保存；无法完整保存的adapter按unsupported legacy责任拒绝，不假报owned。

gateExpose不再无条件叠加到transfer busy：只有完成typed owner登记的v1 expose可从transfer谓词排除；真实transfer仍gateTransfer。删除忙码与新census/gate改写同一个集成commit，任一包未齐就保持旧守卫，不能先删再补。ss_state_unrepresentable逐phase造真实不支持状态；全程可表示的idle SS listener/盐历史允许升级，不再“Server Serving即busy”。

pending旧prev保护从§5进入所有expose/proxy/admission入口，不能只保护主动CLI。所有失败cut恢复原owner；不调用RemoveProxy/re-expose来换新socket。全量节点shutdown保持Run的defer stopProxyOnRunExit；普通NATS session rebuild不触发它。

### 7.4 Darwin与现有S4责任

Linux才广告/运行flow-v1 image dataplane；Darwin不select v1，ordinary expose与SS继续legacy，不调用Linux replay/sealedstate/rawsocket handoff。socket_other/connect_other/state_other给明确ErrUnsupported，不返回成功空image；平台拒绝在新副作用前发生，selected v1后不能fallback。

Darwin交付口径沿S4 L6：GOOS=darwin GOARCH=arm64 go build ./...、make vet-tags与portable stub/legacy测试；真实Darwin TestLegacyExposeDarwinLifecycle/TestLegacySSDarwinLifecycle有runner才实跑，否则NOT_RUN且不阻塞S5出口，报告必须写未实跑。cross-build不能冒充run。

承接S4 L1–L8：same SID硬删重建缺incarnation边界留S8；同SID多PGID/stopped清场但setsid脱离不覆盖；根SIGINT/SIGTERM本地取消；regular file及/dev/null按L5可用、不能沿正文旧方案拒64；500ms仍校准；partial-active只current cut；Darwin按L6；P0失败桩与文件时间互斥按L7/L8。不得因新增dataplane弱化stdin/resize/KA/ps/kill与terminal restore。

## §8 测试、mutation 与非空前提

每个mutation在独立临时checkout/overlay一次一处，保存baseline green、diff/SHA、精确命令、native rc、失败断言，恢复后再绿。不是“写了负例”就有变异收据。测试不fork go test；same-PID harness用当前test binary/spawnexec统一init或真实product build，build继承overlay/race。三固定名保留，旧Test/Fuzz不删除改名；下列新增身份逐项入test inventory。

表中路径相对目录由§12的唯一owner表展开；表列精确文件basename，其所在包不含歧义。每行多个mutation分别跑，不合并遮蔽。

| ID / 测试名 | owner / 文件 | 必须非空断言；mutation |
|---|---|---|
| T01 TestFlowFrameGoldenAndMalformed | P2 frame_test.go | 独立40-byte/九kind golden；flags、length、mode、overflow；改kind编号/接受未知flag各红。 |
| T02 TestFlowACKUsesAppliedNotCarrierSent | P1 manager_test.go | 本代Sent=0但peer A>B且≤E；改ACK为Delivered/要求A≤Sent各红。 |
| T03 TestFlowDedupeBeforePartialWrite | P1 manager_test.go | 真实0<n<len、A<R、overlap/gap/conflict；按len推进A/先Write后去重/漏pending suffix各红；A−1为正控。 |
| T04 TestFlowOpenReconciliationKeepsOriginalDial | P1 open_test.go | OPEN未到、connecting、丢OPEN_ACK三窗，target Accept=1；未知RESUME造flow/重复Dial各红。 |
| T05 TestRetiredOpenCannotDialAfterReceiptGC | P1 retirement_test.go | 删除receipt再发旧seq、holes满；TTL忘退役/满额evict证明各红。 |
| T06 TestFlowFINCloseWritePreservesReverseTail | P1 manager_test.go | FIN先到、真实EOF、反向>8MiB；FIN→Close/Read EOF→target_closed/提前FIN receipt各红。 |
| T07 TestFlowAbortKeepsFinalTailReceipt | P1 manager_test.go | n+error、A<E、固定FinalEnd；丢n/End继续增长/错误报全交付各红。 |
| T08 TestRawSocketPartialWriteAndDescriptorIdentity | P1 socket_linux_test.go | 真堵sink短Write、错原socket/非socket/flags；按len记账/Validate修flags/File.Fd副作用各红。 |
| T09 TestConnectSOErrorHasOneConsumerAcrossCut | P1 connect_test.go | 真EINPROGRESS与失败SO_ERROR，cut后原FD；校验消费SO_ERROR/失败后重Dial各红。 |
| T10 TestFlowFreezeJoinsWithoutClosingBusiness | P1 fence_test.go | 锁等待计deadline、两向泵活动；Close原raw唤醒/漏join/超时启第二泵各红。 |
| T11 TestYamuxSharedWriterCancellationBounds | P1 flow_carrier_test.go | 最小window、64 slots满、TLS Write堵、慢快流；只关stream/默认10s/持锁join各红。 |
| T12 TestEndpointSurvivesSessionLossAndResumes | P1 flow_endpoint_test.go | listener/pubConn原inode，attached→detached→resume；session退出关listener/重新bind各红。 |
| T13 TestResumeReRunsTokenLookupAndFence | P1 flow_resume_test.go | callback确调用，权限/killGen/forgotten/closedAllocation含cached success；漏再查/早prune各红。 |
| T14 TestReaperDoesNotRehomeDuringPlannedCarrierRebuild | P13 flow_rehome_test.go | SQL及事件，dwell=2、tick相位、8s<10s；ready假false/grace≥10s各红。 |
| T15 TestFlowALPNNMinusOneQuadrants | P3 flow_alpn_test.go | 真TLS四象限、无交集、pin失败；强制ALPN/坏pinfallback/selected-v1降级各红。 |
| T16 TestRegisterReaderRetainsYamuxPrefix | P3 register_buffer_test.go | 同TLS packet OK+yamux，服务端精确回复与旧client集合；丢bufio尾部/附加OK字段各红。 |
| T17 TestSSActorEveryByteCutAndTargetParser | P8 actor_test.go | salt/len/payload/ATYP跨chunk、target suffix、原期限；漏cursor/重parse/重stamp各红。 |
| T18 TestSSNonceContinuityAndNoReseal | P8 crypto_nonce_test.go | 独立AEAD，全actor actual-subkey+nonce ledger，真实partial cipher Write；resetnonce/重Seal/wrap各红。 |
| T19 TestSaltHistoryAliasesRotationAndResponseCollision | P8 salt_history_test.go | aliases、删重加、FIFO跨代、受控response RNG撞salt；按ID分ring/丢历史/不reserve response各红。 |
| T20 TestSSNewAuthRechecksActiveMaterial | P8 actor_test.go | trial后rotate，同ID旧conn继续、新旧PSK不同结果；ID-only bind/rotation杀旧合法conn各红。 |
| T21 TestSSSilentRevokeClosesBoundFlow | P8 revoke_flow_test.go | alice先双工后静默、bob继续、dup shutdown与pub/replay清零；无FlowID callback/只Close一个dup各红。 |
| T22 TestSSFreezeAndCleanupJoinOutsideLocks | P8 fence_test.go | SetKeys/Stop/revoke/Freeze与回调真实交错；p/SS锁内join/callback self-join各红。 |
| T23 TestSSPrivateCodecGoldenAndRequiredFields | P9 state_codec_test.go | 独立字段向量、duplicate/trailing/枚举；漏nonce/FIFO/SO_ERROR/pending字段各红。 |
| T24 FuzzSSFrame | P9 state_fuzz_test.go | 真实phase/trial/截断/overflow parser与pure private decode；漏入口/只fuzz空roundtrip被发现门抓红。 |
| T25 FuzzFlowFrame | P2 frame_fuzz_test.go | header/body全部mode、截断/溢出；接受未知kind/越界allocation种子红。 |
| T26 TestSSSecretsNeverEnterPublicCarriers | P10 image_test.go | 真实manifest/Data/marker/log/fmt/error raw/hex/base64；逐载体泄密mutation红。 |
| T27 TestSSFiveRoundImageOwnershipFailures | P10 image_exec_test.go | ≥5轮reExport/失败Take/Thaw，活receiver逐类FD回基线；漏sealed/upstream dup/退出后才查各红。 |
| T28 TestProxyAuthoritySnapshotNeverMixesKeysAndEpoch | P11 proxy_authority_test.go | Enabled/allocation/Keys/Epoch间撤权、SQL错误、samepair冲突；拆transaction/忽略Epoch err各红。 |
| T29 TestResumeAuthorityLookupStopsAndRejectsKnownLag | P12 tunnel_authority_test.go | 堵SQL、Stop后join、positive knownlag拒绝；Future包装timeout/仅missing查lag各红。 |
| T30 TestSharedReplayBudgetCountsOwnedDomains | P4 shared_budget_test.go | exec/PTY/flow同pointer、空log、import全域Used；复制budget/ACK减capacity/image用global各红。 |
| T31 TestFlowImagePendingAndRetirementSurvive | P6 image_test.go（flow） | RawReadPending/partial Append、退休holes；丢TxPending/RxPending/retirement各红。 |
| T32 TestClientImagePreparePermitAndFDClosure | P7 flow_image_test.go | 真Prepare/Restore点分ID、permit丢reply、全部FD；冒号/漏一flow原FD/重consume各红。 |
| T33 TestBundleFinalReexportIncludesFlowSSRevoke | P17 flow_ss_image_test.go | Freeze后late revoke/fired，全域重Export；仅Runtime重导/资源替换丢域各红。 |
| T34 TestFlowSSPartialActivationUsesCurrentCut | P18 flow_ss_activation_test.go | 第二actor失败时第一已推进A/nonce；Detach pristine/Active标在全成功后各红。 |
| T35 TestFlowSSCandidateAndActualPrevDomains | P16 upgrade_domain_flow_test.go | live/idle/salt/retired、dst=candidate、第三SHA；只扫active/只看ABI/恢复running==disk各红。 |
| T36 TestPendingOldPrevRejectsNewFlowSSAdmission | P16 upgrade_domain_flow_test.go | pending与commit写失败、零accept/listen/keyset副作用；MarkReady开门/晚guard各红。 |
| T37 TestPreflightDefersOnEachReasonCode（扩展原名） | P16 upgrade_image_test.go | 九码逐造、unknown adapter、owned-v1不busy；只删flow/ss谓词/gateExpose双分类各红。 |
| T38 TestAgentRestoresRawThenResumeThenRegister | P14 flow_restore_test.go | 32active+idle+堵auth，SS grant前零target Write；8worker串批/NATS先行/旧keys先放行各红。 |
| T39 TestAgentSamePIDExecKeepsFourTCPFlows | P20 flow_exec_test.go | 四双向原socket、运行SHA变、最终seq/hash；只换盘/应用redial/早Close各红。 |
| T40 TestFlowCarrierRebuildLifecycleMatrix | P20 carrier_test.go | OPEN/idle/slow/active/revoke/targetfail/旧epoch逐臂；漏bind/目标EOF全关/过期ticket各红。 |
| T41 TestHalfCloseTailAcrossRealExec | P20 flow_lifecycle_test.go | 原half-close，cut后反向大尾流，最终EOF精确；提前全关/掉尾块各红。 |
| T42 TestSSRealExecKeepsIndependentCipherTrace | P21 ss_exec_test.go | 外部reference客户端、target Accept=1、Seal ledger；重nonce/重Dial/重Seal各红。 |
| T43 TestSSRevokedDuringCutNeverWritesAfterGrant | P21 ss_revoke_test.go | 四cut窗、静默victim、fresh grant revision、bob；导出丢Revoked/旧Server直接Thaw授权各红。 |
| T44 TestActiveRollbackExportsCurrentFlowSSState | P22 rollback_test.go | 真A→B→A，B内A/Base/nonce/receipt确推进；缓存初始bundle/generation不增各红。 |
| T45 TestReadyPendingWatchdogTakesSecondCurrentCut | P22 partial_activation_test.go | commit marker写失败、120s自动回迁、SS持续授权；MarkReady停watchdog/初始image回迁各红。 |
| T46 TestLegacyExposeDarwinLifecycle / TestLegacySSDarwinLifecycle | P23 legacy_darwin_test.go（各包） | 实际legacy listener/SS可用、unsupported image；Darwin进LinuxReplay/空成功stub各红。 |
| T47 test_complete_duplex_trace_passes及负例组 | P24 hot-update-flow-probe-test.py | 独立双向seq/hash/首尾窗/EOF/RST/nonce；删drop/dup/trailing/identity判据使对应负例绿。 |
| T48 hot-update-flow-drill-test.sh | P25 同名文件 | 五headers、六臂、oldbroker供给/39.F提前分派、native rc；丢fault先验/吞subshell rc各红。 |
| T49 各gate的同文件Controls | P26 §10所列文件 | 删除真实字段/enum、新增ctx/Agent126/漏risk/race/错budget等逐项红，不生成空总测试。 |
| T50 TestHotUpdateFlowMatrix | P27 flow_matrix_test.go | parsed whole-package race tuples含全部owner；漏flow/SS/rawsocket包或改-run窄筛红。 |
| T51 TestRawSocketFiveRoundLifecycle / TestFlowFiveRoundLifecycle / TestTunnelFlowFiveRoundLifecycle / TestSealedStateFiveRoundLifecycle | P1 socket_linux_test.go/manager_test.go/flow_endpoint_test.go；P5 state_linux_test.go | 各≥5轮真实exercise，test body直接shared leak helper；漏join/dup、删anchor/exercise各红。 |

P19 harness的raw_probe_test.go与ss_reference_test.go分别拥有TestRawProbeDetectsDroppedAndDuplicatedBytes、TestSSReferenceRejectsNonceReuse：变异删除独立ledger/以被测codec替reference，必须击红。P15 proxy_image_test.go/proxy_restore_order_test.go/proxy_timer_image_test.go拥有TestProxyPairDigestIsNotSuppressed、TestProxyApplyEntrypointsAreSerialized、TestProxyFiredCleanupSurvivesCut：分别变异samepair跳keys、register漏proxyApplyMu、timer=nil丢Fired，必须击红。P6 rawsocket/image_test.go拥有TestRawImageSalvageKeepsAllTakenFiles，漏第二个connecting FD变异击红；P17 flow_ss_activation_test.go与P18 flow_ss_redispatch_test.go分别测domain-complete计时与current-state redispatch，不引用初始snapshot。

P1 flow_resume_test.go另外拥有TestCurrentOwnerTicketCoversLongIdleAndFirstDetach、TestPrepareActivateLostReplyHasOneOwner：变异idle短TTL、失败续grace、cached reply跳auth、permit二次consume分别红。P8 actor_test.go拥有TestSSCapacityFailureStillRevokesRemovedKeys：材料cap已满、删除alice同时新增材料必须先shutdown alice、bob存活且无成功apply ACK；变异容量检查先return保留alice、失败仍推进pair分别红。P16 upgrade_flow_failure_test.go拥有TestFlowSSExecFailureThawsBothDirections：真实syscall.Exec失败、原FD不Close、SS重新grant前零target Write；变异漏abandon/直接Thaw旧权限分别红。

FuzzSSFrame进入实际SS手写phase/trial/length/target解析和pure secret decode，随机FD不得调用Take/Close。live fuzz统一命令 make fuzz FUZZ='^Fuzz(FlowFrame|SSFrame)$' FUZZ_MIN=2 FUZZTIME=30s；核启动/完成的精确(package,name)集合={internal/flow:FuzzFlowFrame,internal/agent/ssproxy:FuzzSSFrame}，不得删一个仍绿；旧fuzzer保留。seed replay与live fuzz分列，crasher入testdata/fuzz，interesting corpus不误写成仓库产物；遵守≤64文件/目录、全仓≤256KiB。

Leak/race不依赖自动import发现：flow、rawsocket、sealedstate、tunnel、ssproxy、agentimage、agentboot、test/hotupdate显式risk/matrix登记。≥5轮测试在receiver仍活且product已退owner时数goroutine与各类FD，fixture teardown前断言；隐藏helper调用不算anchor。真实product两artifact升级收据与same-test-binary kernel收据分列，不能互代。

## §9 drill 75、负控制、legacy与成本

### 9.1 六臂及共同oracle

新文件75-hot-update-flows.sh，只有一个顶层case "${ARM:?}" in；公共setup不能提前建立错误版本/exec-only业务。五个manifest header冻结为：

~~~sh
# arms: A C R B W L
# fixture: A=N1 C=N3-live R=N1 B=N1 W=N1 L=N1
# grows: A=0 C=2 R=0 B=0 W=0 L=0
# worst: A=420 C=600 R=420 B=540 W=660 L=420
# forgoes: A=- C=- R=- B=- W=- L=-
~~~

| 臂 | 场景与不可替代的非空收据 |
|---|---|
| A | 同一批原SSH、4条双向TCP、1条真正EINPROGRESS OPEN、1条half-close、原SS bob及alice，加10/s新短流。先启动并取≥20个双向样本；产品agent升级，freeze窗内撤alice订阅；survivors继续；然后对当前carrier强制重建，同一cohort继续。每阶段SHA/marker/cut/carrier身份与字节ledger，不能分臂拼接。 |
| C | N3-live，健康home不变，32active边界、一个预声明slow flow及新流；controller正常，reaper真运行；SQL/home/epoch不重铸。mix/N-1与lag授权拒绝另有T15/T29，不绕join release门造混部。 |
| R | revoke命中PREPARE/final cut/RESUME race的真实窗口；原alice双工后静默，产品关闭pub/SS两端与dup/replay；bob原连接继续。fresh grant AppliedRevision/epoch与撤权applied receipt相对应，未知授权不写target。 |
| B | 两个可通信、都读flow/SS的真实artifact A→B→A，第二跳B内offset/Base/nonce/retirement已推进，两次commit；不是same test binary，不是watchdog。 |
| W | §9.2真实120s pending-ready watchdog自动第二cut。原cohort持续通信>120s，SS先RESUME获fresh authority，默认reaper一直运行；最终旧running SHA、rolled_back marker、同PID/socket与当前nonce。 |
| L | 固定v0.6.0 broker在首session/expose/proxy业务之前启动，新agent与其legacy raw互通；运行/proc exe SHA、空ALPN、raw首byte无header。活legacy升级安全拒绝75/legacy_session_busy，原业务继续；判“不承诺热更新连续”，不把拒绝当断流。 |

normal cohort开跑前标记slow/idle/half-close，不能失败后改类别。最小20个pre/post样本，各向独立sender/receiver原始seq/len/hash账本，预声明最终seq/length与有限finish deadline；drop/dup/reorder、EOF/RST、换socket/nonce、应用redial是粘性失败。延迟用各端自己的monotonic gap或RTT，不跨主机相减clock。统计p50/p95/max且包括故障起点→首进展、末进展→观测终点的trailing silence；正常flow与每个10/s请求max≤5s。SS用独立AEAD客户端、target Accept计数、实际Seal ledger，不能仅明文hash。

identity包括PID/boot/startticks、dev/ino/type/私有tuple（原始artifact保密）、application session nonce、target Accept=1、原ctl仍同pid；carrier TLS/session nonce必须变化。OPEN先验是真原FD已创建且SO_ERROR阶段EINPROGRESS，不是“调用过Open”；small local目标用只针对该目标SYN的短时故障，先观察被阻侧、恢复后同FD完成，不重Dial。half-close反向尾流跨cut且>8MiB。SS partial write先验记录attemptLen、0<n<len、cipher suffix非空与Seal调用，不用sleep推测。

四日志流统一source drills/lib/logs.sh，fresh cursor，读取失败/缺字段报错。fault取实际cut/prepare/grant事件barrier，超过setup deadline报SETUP-RED，不能空窗GREEN。SQL applied receipt/keys digest、FD/replay per-domain计数在fixture cleanup前保存。

### 9.2 W：真实可执行marker故障

用isolated、由非root agent用户拥有的BIN目录，不碰宿主~/.tether或其它agent。升级完成flip、candidate boot-count已持久化后，短暂阻断该agent的NATS register通路（≤2s），tunnel/RESUME/SS权威通路保持通。外部fixture把此BIN目录从原mode暂改0555，文件与已建lock仍可读/锁；放行NATS，首真实register OK使commitUpgradeAfterRegister在CreateTemp/rename失败，实际产生upgrade_state.go:843–848日志，marker仍pending、MarkReady=true、watchdog armed。

观察该真实失败后立即恢复原mode；权限fault只阻首次commit，不保持120s。保持NATS连接与拓扑稳定，不触发第二次full register；roster refresh仅refreshRosterOnce（roster.go:393–448）不调用commit。持续观察marker pending、candidate running、无COMMITTED日志、默认reaper/allocation不变；到原Deadline自动watchdogRollback，产品自己PREPARE/current-cut/exec prev并报告rolled_back。fixture不能替产品发resume/commit/rollback，不能用fake ready或停reaper。

若进程root/目录权限fault未生效、错过boot-count窗口、发生额外full register导致提前commit，先验不成立→SETUP-RED并重建fixture，不算产品W通过。此合同可由T45 hermetic精确barrier验证，再deploy观察实际filesystem failure。不得单纯断NATS120s，否则无法维持Mandate的SS/reaper权限与5s正常业务约束。

### 9.3 原生负控制与39.F

early-close在真升级cut前后只提前关一个must-survive原业务socket，公共judge必须rc1/ASSERT-FAIL；wrong-offset用实际mutant product把applied ACK改A+1或去掉dedupe-before-write，生成丢/重字节同judge rc1。A−1正确overlap另作positive。两负控独立运行，不塞进75 GREEN父的子臂；每次mutant restage记source/binary/running SHA，完成后恢复final candidate复验。

39-hot-update-legacy-n1.sh新增F，在共同exec-only setup前分派flow fixture；S4既有P与X/Y均保留。F先启动真正v0.6.0 old exporter、原legacy SSH/TCP/SS业务且双向非空，执行历史bare agent exec故障，使用75同continuity judge。必须valid_fault=1、continuity_rc=1与历史early-EOF/RST身份签名吻合，之后assert_bug产生原生rc3 PRODUCT-RED。供给变成安全busy拒绝时，只能另记ability gap，不接受为F的断流证据；setup rc2也不能归PRODUCT-RED。

L证明new agent↔old broker协商，F证明old exporter不能续业务，二者不是一回事。固定历史v0.6.0/a3431a1的peeled commit与制品checksum独立记录；动态N-1选最新相邻release并记录完整源身份。legacy broker的unit ExecStart明确/usr/local/bin/tether-prev及旧版兼容config；从第一项业务起检查/proc exe SHA，禁止仅换ctl、临时绕join门或禁TLS验证。

### 9.4 verdict与receipt形状

expected-verdicts.tsv严格六列：drill、expected、expected_nc_gap、bands、owner、note-ref。75及75.A/C/R/B/W/L七行均GREEN、0、bands=-、owner=-，note-ref各用完整unit名。39.F为PRODUCT-RED、0、bands=-、owner="hot-update-plan H2 legacy flow exporter; S8 legacy-to-capable first migration"、note-ref=39-hot-update-legacy-n1.F；39父保持PRODUCT-RED且owner在保留现有H0、S4 H1全文后追加包含F owner全文的片段。36父/四臂GREEN与39.P PRODUCT-RED按S4原样保留。

期望与expected-verdicts-log对应锚在第一次扫前写齐。原生drill_end必须带实际counter、nc_guard/pass和非空reason，不能手print仿receipt。优先级ASSERT-FAIL>SETUP-RED>PRODUCT-RED>INCOMPLETE>GREEN，rc=1/2/3/75/0；parent=child join、gap=sum、child owner为parent子串与bands suffix规则不改。INCOMPLETE/native75不等于busy业务命令的exit75，收据分列。捕获subshell前保存native rc，不能让管道末尾/cleanup覆盖；token/job PID在MAIN shell取值，不靠assert_setup全局变量回传。

### 9.5 回归、成本与身份

70-expose-journey：保持原GREEN；显式rm后立即同port复用、Stop后≤15s refused再同port重启/sentinel分别签。72-proxy-subscription：wrongPSK在allow-private threaded sink上零SINKBYTES，不用无blocked日志；slow/growing alice撤销、bob存活、off后OS listener与allocation均归零且同port复用。测试不能用fixture关闭victim代产品清场。

同final SHA再跑36.A/W/C/L、39.P；34七臂、35三臂；33 A/C/C2与其既有INCOMPLETE边界不劣化；70/72完整原判词，不缩窄历史断言。S4脚本若尚未集成，先满足entry再跑，不能把“尚无文件”当豁免。

估算（非实测）：75 A/C/R/B/W/L分别180/240/180/240/300/150s，总1290s≈21.5min；declared worst总3060s，runner每臂ceiling=min(2×worst,global)，分别840/1200/840/1080/1320/840s。39.F约180–300s（worst420）；两mutant build/restage/probe约5–10min；S4/exec回归约25–40min；70/72约6min；完整serial deploy验证预留60–90min，失败重建另计，不沿用原8min签名。

drill-costs.tsv只写COMPLETE实际wall/source/date/concurrency/regime，不填estimate/worst；父/arm测量不重复累计。每receipt固定{source_head,dirty_patch_sha,build_flags,artifact_sha,running_sha,role,command,native_rc,verdict,wall_s,oracle_inputs_sha,evidence_path,status}；dirty摘要包括实际内容/未跟踪输入，不只文件数；供应tag记录peeled commit。保存样本总数、缺值、全窗/max、raw ledgers、SQL/fences、逐类FD/replay与四日志，摘要PASS不能代六出口证据。

## §10 gates、库存、race/leak与边际成本

三硬闸沿CLAUDE原命令：make test、make build、make lint；另make gates（含vet-tags、hermetic run-all、lint收尾）。最终由lead串行执行；golangci-lint全局锁，不能两个worker同时lint。本文所有命令仅列未来合同，本轮均NOT_RUN。

### 10.1 新增/扩展gate与控制

| gate / owner | 实际检查与同文件Controls | 预计边际秒数（未实测） |
|---|---|---|
| flow_frame_inventory_test.go / P26 | header offsets/kinds/flags/body/RESUME modes真实codec双向；TestFlowFrameInventoryControls删flags或改编号红 | 0.4 |
| ss_state_inventory_test.go / P26 | private runtime roots→actual binary descriptor→encode/decode逐字段双向；TestSSStateInventoryControls漏nonce/TxPending/FIFO/fired红 | 0.6 |
| image_field_inventory_test.go / P26，账本P27 | Bundle→Flow/SS public具名struct，append-only；TestFlowSSImageInventoryControls删/tag/type漂移红 | 0.2 |
| dataplane_lifetime_test.go / P26 | flow/rawsocket/ssproxy/tunnel image/agentimage/agentboot导出func/method/constructor/alias不得接受caller ctx；own stop root允许；TestFlowSSDataplaneLifetimeControls别名藏ctx红 | 0.3 |
| layering_test.go / P26 | 叶子rawsocket/sealedstate/workload与flow→叶子，tunnel→flow，SS→叶子；禁flow→tunnel/yamux/agent/NATS、SS→flow/agent；TestFlowSSLayeringControls逆向import红 | 0.2 |
| flow_ss_contract_test.go / P26 | Agent receiver恰125、deferred九码、二fuzzer精确发现、sealed公开载体规则、bounded config单源；TestFlowSSContractControls Agent124/126、漏fuzzer、裸default红 | 0.5 |
| upgrade_budget_pin_test.go / P26 | 原Go/shell五预算保持，追加flow grace<最短dwell、node/stream cap引用；TestFlowSSBudgetControls grace=10s/复制字面量红 | 0.2 |
| gate_registry_test.go / P26 | CLAUDE新条目必须在make gates实际包中跑，control名字真存在；TestFlowSSGateRegistryControls漏入口红 | 0.1 |
| leak/enum/matrix登记 / P27 | 精确anchor及parsed whole tuples；新closed family全成员default检查；同既有文件新增control，删成员/race/exercise红 | 0.5 |
| simcluster hermetic登记 / P24/P25/P27 | oracle自检、供给/manifest/native rc、run-all双向登记；测试fixture失败不洗绿 | 3–8 |

新增Go gate预估合计约3s，hermetic约3–8s；make gates旧基线约3m32s，新边际预计6–11s。race功能/真kernel/120s watchdog不放进静态门冒低成本，另列§8矩阵wall。交付必须填实际边际秒数（同机同cache前后差）与失败控制收据；估算不能写成measured。

private字段库存不能只是手写一个永不被codec使用的表：每个encoder/decoder调用字段ID都从descriptor取width/type/required，gate扫描实际读写集合，actor snapshot roots也对账。公开反射只看导出字段的盲点由这条闭合。frame与SS独立binary库存只追加，旧字段改变是纪元决策，不能自动重生golden。SS网络wire保持既有AEAD，private codec版本与frame版本分列。

### 10.2 CLAUDE整表逐项保留

| 不变量 | S5动作/receipt |
|---|---|
| lint三层、nolint递减、structural四维 | 不改.golangci.yml、不放宽golden。Agent==125、≤19 production files、剥注释code<8000；Broker≤285 methods、≤70 files、code<18000；cmd main-noncli<1300。新子包目标每production file≤700行、receiver methods<100，超出先拆子包，不删事故注释换预算。 |
| layering union、TLS sites、tags | 仅追加有理由的new leaf边；既有TLS verify四site不加副本，不关pin；使用已有linux/darwin/!linux标签，不新添integration tag绕矩阵；vet-tags独立receipt。 |
| leader premise、inbox绑定、NATS ping/server pin、seedSession | 新broker测试必须ObserveLeader，不Sleep选举；private reply/routing绑定不变；pin/pingSSOT、八seedSession转发器与stackharness test-only不变。 |
| naming freeze、测试append-only、determinism | Test/Fuzz身份逐行追加；不重命名旧测试；version与origin/promised-guard、cfgdb/raft timing/产品sleep门保持，barrier用可观测receipt，不加纯sleep fixture。 |
| enum closed family | 明确登记FlowKind/FlowPhase/FlowReason、EndpointPhase、ResumeScope/Mode/Verdict、CloseMode、SSPhase/AuthorityPhase/TicketPurpose、RawConnectState、PrivateRecordKind/PrivateFieldType；显式值枚举也登记，不能仅靠iota发现；unknown一律拒绝，无裸default放行。 |
| wire append-only、CLI/error-code、ACL | proto NATS消息字段Δ0，REGISTER六字段Δ0，旧tags/types不动；新binary另账本。CLI command/flag goldenΔ0。frame Reason属TLS协议拒绝，不自动当CLI code；image_abi_unsupported→64、agent_upgrading/deferred→75、实际运行故障沿既有分类，ss_state_unrepresentable是deferred Reason/hint。四处exit-class双向覆盖、hint与发射点对账，不默退70；无新NATS subject/ACL。 |
| ctx-root/ctx-none、dataplane lifetime | 新SQL operation、SS own resolver/stoproot逐站标origin与ctx-none并精确账本；禁止caller ctx锚SS/flow；Run退出仍stopProxy，NATS session取消不Close它。 |
| image inventory、budget pin | public与private二库存分列；Runtime/flow ownUsed、共享pointer、Go/shell单源；Limit不被import覆盖；append-only不删旧行。 |
| leaks、whole-race、唯一go test入口 | 每risk package test body直接helper≥5轮；anchor精确exercise；flow无yamux import也显式risk；whole matrix tuple追加，不能-run筛掉fault tests。test/e2e之外不fork go test。 |
| spawn evidence/isolation、simcluster logs | boundedHomeRead、四哨兵site/wired控制流与lock支配保持；spawnexec统一init/原地Exec/64租约保持；四日志只经logs.sh，不增kept-site豁免。 |
| hermetic、CI、fuzz、gate registry、docs布局 | 新*.sh进run-all按shebang执行；CI release needs三闸、定时fuzz而非PR保持；语料预算保留；CLAUDE新gate实际跑；过程文件只在docs/reviews，不能顶层散落。 |

P27追加whole-race unit覆盖rawsocket、sealedstate、flow、tunnel、ssproxy、agentimage、agentboot、test/hotupdate及受影响agent/broker/execsession；watchdog单元timeout≥240s，不用默认90s误杀。matrix包字面量/run/tags/race元组进golden，runtime go-list closure dedupe机制不改；删一个新tuple必须红。ctx账本、leak anchors、enum家族与matrix库存交付精确diff，不以“风险scanner应该发现”作为证明。

三硬闸/gates、live fuzz、整包-race、五轮leak、各mutant、Darwin cross-build分别保存source/command/native rc/wall；通过后不无理由反复拓宽测试。最终candidate restage后跑集成/回归；lint仅lead一处运行。

## §11 docs、coverage与tasklist收据

P28更新architecture §7/§21/§22（新增§22.7）：逻辑flow层、ALPN与N-1、Endpoint lifetime/fences、resume ticket/current-owner/detach、SS ABI/secret/sealed FD责任、正常与撤权边界。替换「tunnel reconnect只恢复新流」，纠正旧running==disk段落为代码已有的prev规则。§22.6有历史NOT_RUN和已实跑段落时保留日期/SHA归属，不将历史状态覆盖成本候选绿灯。

usage/broker-ops说明公网端口在detached grace继续占用、原socket不重建、legacy safe defer、SS轮换/撤销、capacity与ss_state_unrepresentable、首次old-reader pending admission、8s grace与5s正常性能。说明current local-applied一致性，不能文档声称leader commit全局即时撤权；说明盐8192窗口/材料64配额与长期idle domain责任。gotchas新增deadlock、bufio尾部、SO_ERROR单消费、salt别名、nonce partial write、marker-write W等事故机制及真实守卫。

plan §3.3、stages S5、handoff-protocol/process与failures文档记录资源图和restore顺序，不改S6/S9范围；测试主题地图test/README登记test/hotupdate，主题README说明real artifact/kernel/reference层次、matrix、leak与mutation入口。simcluster README记录75/39.F臂、供给、native判词、五headers、预算/实际wall、70/72边界。reviews/INDEX挂binding plan与validation；hot-update-evidence引用本候选的原始receipt，正文不能只贴GREEN摘要。

### 11.1 S4六项联合回归及边界

| 责任 | 同一S5 candidate的证据 |
|---|---|
| C-4 stable control identity | S4重复signal/resize/KA receipt与旧CallID隔离；新增carrier切换不得让ctl重create，36.C与相关unit。 |
| C-8 stdin/resize/KA/ps/kill | stdin EOF/实际raw 0x03、root cancel/termios、regular files按L5、winsize真实变化、KA绝对deadline、ps/kill统一Manager，36.A/C。 |
| C-10 PTY revoke/cleanup | S4四cut窗口、register-gone、同SID多PGID/stopped清场；finalPrepare后丢事件且同SID硬删重建无incarnation仍留S8，不能从SS fresh grant外推PTY已闭合。 |
| C-11 current-cut/资源/gates | 36.W、partial activation、current bundle回迁、真FD/五轮leak；完整C-10边界未闭合不勾C-11全项。 |
| B-2 PTY原连接 | 原ctl/master/前台/输入输出/resize across upgrade，36.A/W/C/L全GREEN；与S5 SSH/TCP/SS receipts并列。 |
| failures §4尾流/partial/控制/期限/身份/EOF六类 | 对应S4 T01(EIO n+err)、T02/T11/T42(尾流/queue)、T08/T14(control原子与幂等)、T10/T22(kernel身份/absolute deadline)、T04/T35/T36(真短Write)、T05(FIN不造终端EOF)；原S4判词不缩水。 |

S4 entry要求包括完整36.A/W/C/L GREEN、39.P PRODUCT-RED及34/35/33边界，不能只引用36.A。上位handoff交接文档要求的最终test/simcluster/docs/审阅步骤仍履行，不把本计划写完当完成阶段。S6才覆盖broker same-PID exec，S8才扩事务/authority incarnation/legacy migration，S9才覆盖NATS/Caddy的真实组件切换，S10才五代混载完整退出。

### 11.2 tasklist只凭receipt打勾

| item | 可勾的S5责任 | 必须保留的未闭合责任 |
|---|---|---|
| B-2 | §0出口2/5、75原SSH/TCP/SS与S4原PTY全部same-final-SHA receipts齐才勾本子项 | 任一缺原socket/字节ledger/PTY收据不勾；不外推broker/infra切换。 |
| D-1 | internal/flow frame/ACK/dedupe/FIN/backpressure、T01–T11与mutants | 无receipt不勾。 |
| D-2 | agent流FD/replay/image/同PIDexec子项，T31–T41 | broker Server/Endpoint FD跨broker exec留S6，顶层D-2不总勾。 |
| D-3 | ALPN、六字段REGISTER/exact OK、N-1、票据再授权与fences | full unit/75.L/39.F receipts齐才勾。 |
| D-4 | 同broker Endpoint survives carrier、planned reaper回归子项，三固定测试 | broker image/export、维护态/exec臂留S6，顶层D-4不总勾。 |
| D-5 | SS actor/nonce/salts/rotation/revoke/secrets/fuzz/资源 | T17–T29、T42/T43、75与fuzz精确集合/mutations齐才勾。 |
| D-6 | agent层75、70/72、docs、gates、race/leak子项 | broker层业务续接及其drill留S6，顶层D-6不总勾。 |

P29产出validation receipt索引：每条出口、每个命名test/mutation、每臂drill、每gate、每domain FD/budget、N-1四象限、Darwin两栏都有status与原始artifact链接。P28最后据此更新tasklist，不先打勾等证据。没有runner的Darwin实跑明确NOT_RUN，按冻结L6不阻塞但不冒充完成。本文生成时全部新增行NOT_RUN，无任何tasklist勾选动作。

## §12 并行工作包、文件唯一owner与集成DAG

恰两个HARD：P1新增raw/flow/Endpoint并发与kernel责任，P8新增SS/crypto actor状态机。其余NORMAL按冻结算法机械接线、codec/image、测试、文档/账本，不让NORMAL重新设计并发合同。模型分配沿S4 L7：HARD给gpt-6-astra，NORMAL给gpt-6.1-sol；本文不启动worker。

文件列用「目录/: basename列表」表示该目录下逐个明确文件，不是glob；每个basename都展开为独占路径。计数为独占路径数，Δ0库存仅交检查receipt，不为凑数改文件。文件未存在则创建；已有则修改。所有agent生产接线只在现有文件。需要表外修改时写Requests for other packages由lead转派；不得私自编辑别的owner文件。

P0先与S4 lead串行锁定共同文件，S4落地后一次landing：五个contract文件，以及§2每个签名在下游最终owner文件中的可编译失败桩/字段消费点。P0临时触碰下游文件的完整清单就是下表实现文件中实际承担§2签名/消费点的路径，交付逐路径transfer receipt；落地后P0不再编辑这些文件。这是时间互斥，不是两个并行owner。P1–P25据桩同时写；其测试依赖在最后一列，未齐时NOT_RUN，不能改桩假绿。P26/P27/P28可先准备账本/文案，实际收敛在核心稳定后；P29最后串行总验证。

### 12.1 image/codec与runtime跨owner的额外P0 hook

禁止image/codec owner直接依赖另owner未冻结的private runtime struct。P0在对应contracts.go冻结如下hook及通用private记录类型；字段descriptor的名称/类型/required集合按§3–§6全部状态逐项冻结，名称按ASCII排序作为wire field ID，binary encoding使用name长度+name+type+value，不接受unknown/duplicate/case alias/trailing。这里只是跨owner传值，不把记录接入Bundle公开JSON。

~~~go
// sealedstate/contracts.go；Record/Field只传private状态。
type Field struct { Name string; Type uint8; U64 uint64; Bytes []byte }
// Type:1 u64,2 nonnegative i64,3 bool,4 bytes,5 UTF8,6 ID16,7 nonce12,8 salt32。
type Record struct { Kind string; Fields []Field }
// Kind闭集: flow,endpoint,retirement,ss-server,ss-actor,material,salt-ring,
// authority,timer,binding,permit,receipt；唯一ID字段区分同kind记录。
type CutState struct { Records []Record; Resources []handoff.Resource }
// 各同名函数位于其package，无跨packageprivate字段访问。
// flow/contracts.go
func (f *Frozen) SnapshotState() (sealedstate.CutState, error)
func restoreCutState(cfg Config, state sealedstate.CutState) (*Manager, []handoff.Resource, error)
// tunnel/flow_contracts.go
func (f *ClientFrozen) SnapshotState() (sealedstate.CutState, error)
func restoreCutState(cfg FlowConfig, state sealedstate.CutState) (*Client, []handoff.Resource, error)
// ssproxy/image_contracts.go
func (f *Frozen) SnapshotState() (sealedstate.CutState, error)
func restoreCutState(state sealedstate.CutState) (*Server, []handoff.Resource, error)
// ssproxy state_codec.go提供pure codec，不触FD/网络。
func encodeState(records []sealedstate.Record) ([]byte, error)
func decodeState(data []byte) ([]sealedstate.Record, error)
~~~

P0 descriptor列表必须列出§6.1每个cursor/nonce/target/DNS/connect/timer与§3 TxPending/RxPending/retirement；P9只实现codec，P8只产生/消费typed records，P10只封装memfd/image。P6/P7同理调用SnapshotState/restoreCutState，不改P1 structs。Descriptor与实际read/write/build记录三向对账由P26守；不能在P9另立手写假表。正式P0 receipt附展开后的精确descriptor表与桩路径，lead检查全部编译且sentinel触发后放行并行实现。

### 12.2 唯一文件表（测试落地依赖，不限制并行写作）

| 包 / 难度 / 数 | 目标与独占文件 | 拥有测试、必跑mutation；测试需先落地 |
|---|---|---|
| P0 contracts / NORMAL / 5 | 冻结§2/12.1、失败桩。internal/rawsocket/: contracts.go；internal/flow/: contracts.go；internal/tunnel/: flow_contracts.go；internal/agent/ssproxy/: image_contracts.go；internal/sealedstate/: contracts.go | 编译接口/真实ErrNotImplemented自检，不造成功测试；mutation删除签名使消费点编译失败。依赖S4 landing。 |
| P1 raw-flow-endpoint / HARD / 24 | raw连接/pump/journal/Endpoint/carrier。internal/rawsocket/: socket.go,socket_linux.go,socket_other.go,connect_linux.go,connect_other.go,socket_linux_test.go,connect_test.go；internal/flow/: manager.go,open.go,pump.go,fence.go,retirement.go,manager_test.go,open_test.go,fence_test.go,retirement_test.go；internal/tunnel/: tunnel.go,flow_endpoint.go,flow_carrier.go,flow_client.go,flow_fence.go,flow_endpoint_test.go,flow_carrier_test.go,flow_resume_test.go | T02–T13、T51前三名及规定每个mutation。依赖P0/P2/P3/P4/P6/P7。 |
| P2 flow-codec / NORMAL / 4 | 唯一frame codec/golden。internal/flow/: frame.go,frame_test.go,frame_fuzz_test.go,testdata/frame-v1-golden.txt | T01/T25；编号/flags/overflow变异。依赖P0。 |
| P3 alpn-register / NORMAL / 4 | TLS协商及buffer接线。internal/tunnel/: tls.go,flow_alpn.go,flow_alpn_test.go,register_buffer_test.go | T15/T16；强ALPN/丢bufio/pinfallback变异。依赖P0/P1/P2。 |
| P4 shared-budget / NORMAL / 7 | 全调用点pointer与ownUsed。internal/workload/: replay.go,replay_budget_test.go；internal/execsession/: runtime.go,image.go,fence.go,pty.go,shared_budget_test.go | T30及TestReplayACKKeepsCapacityReservation；ACK减reservation/复制pointer/全局Used变异。依赖S4/P0/P1/P6。 |
| P5 sealed-state / NORMAL / 5 | memfd封存与owned Close。internal/sealedstate/: state.go,state_linux.go,state_other.go,state_test.go,state_linux_test.go | T51 sealed名、TestSealedStateRejectsUnsealedDescriptor；漏seal/Close borrowed变异。依赖P0。 |
| P6 flow-raw-image / NORMAL / 6 | frozen状态纯校验/codec与FD复原。internal/flow/: image.go,image_test.go；internal/rawsocket/: image.go,image_linux.go,image_other.go,image_test.go | T31、raw salvage测试；漏pending/错fingerprint/漏FD变异。依赖P0/P1/P4/P5。 |
| P7 client-image / NORMAL / 2 | Client permit/Endpoint资源封装。internal/tunnel/: flow_image.go,flow_image_test.go | T32；非法ID/漏资源/重消费变异。依赖P0/P1/P5/P6。 |
| P8 ss-actor / HARD / 11 | actor/nonce/salt/key/revoke kernel决定。internal/agent/ssproxy/: server.go,crypto.go,actor.go,fence.go,keys.go,salt.go,actor_test.go,crypto_nonce_test.go,salt_history_test.go,revoke_flow_test.go,fence_test.go | T17–T22全部变异。依赖P0/P1/P9。 |
| P9 ss-codec / NORMAL / 4 | private records codec与SS parser fuzz。internal/agent/ssproxy/: state_codec.go,state_codec_test.go,state_fuzz_test.go,testdata/ss-state-v1-golden.txt | T23/T24；字段缺失/duplicate/truncation变异。pure codec依赖P0；live parser依赖P8。 |
| P10 ss-image / NORMAL / 3 | SS sealed image/FD closure。internal/agent/ssproxy/: image.go,image_test.go,image_exec_test.go | T26/T27；每公开载体泄密/漏sealed/upstream变异。依赖P0/P1/P5/P8/P9。 |
| P11 proxy-authority / NORMAL / 3 | broker register/push一致snapshot。internal/broker/: proxy.go,proxy_authority.go,proxy_authority_test.go | T28；拆tx/忽略Epoch err/samepair混keys变异。依赖P0。 |
| P12 bounded-auth / NORMAL / 4 | token与RESUME bounded adapter生产接线。internal/broker/: expose.go,broker.go,tunnel_authority.go,tunnel_authority_test.go | T29；knownlag漏挡/timeout遗worker变异。依赖P0/P1/P3/P11。 |
| P13 reaper-regression / NORMAL / 2 | 不改prod reaper，实SQL验证。internal/broker/: proxy_reconcile_test.go,flow_rehome_test.go | T14；移除ready latch/改grace变异。依赖P1/P11/P12/P14/P15。 |
| P14 agent-restore / NORMAL / 7 | raw→carrier→RESUME→register及binding。internal/agent/: agent.go,bootstrap.go,tunnel_adapter.go,expose.go,flow_restore_test.go,bootstrap_flow_test.go；cmd/tether/: agent.go | T38、TestBootstrapFlowRestoreStartsNoWorkers；顺序/8worker/提前goroutine变异。依赖P0/P1/P4/P7/P10/P17/P18。 |
| P15 agent-proxy / NORMAL / 5 | proxy apply/timer/revoke与锁外join。internal/agent/: proxy.go,exec.go,proxy_image_test.go,proxy_restore_order_test.go,proxy_timer_image_test.go | §8三个proxy固定名；samepair/漏序列/Fired变异。依赖P0/P1/P8/P10/P11/P14。 |
| P16 upgrade-domains / NORMAL / 9 | version/domain三门、pending准入、失败回收。internal/agent/: upgrade.go,upgrade_state.go,upgrade_image.go,upgrade_image_test.go,upgrade_domain_flow_test.go,upgrade_flow_failure_test.go；internal/proto/: version.go,version_test.go；cmd/tether/: main.go | T35–T37；只看ABI/actualprev错/MarkReady开门/只删busy变异。依赖P0/P7/P10/P14/P15/P17/P18。 |
| P17 image-compose / NORMAL / 6 | Bundle域/全域reExport/责任合并。internal/agentimage/: bundle.go,freeze.go,restore.go,bundle_test.go,flow_ss_image_test.go,flow_ss_activation_test.go | T33、TestDomainCompleteActivationWindow；Runtime-only重导/早计时/资源丢域变异。依赖P0/P4/P7/P10。 |
| P18 boot-active / NORMAL / 3 | 沿S4active边界current redispatch。internal/agentboot/: image.go,flow_ss_activation_test.go,flow_ss_redispatch_test.go | T34、TestFlowSSRedispatchUsesCurrentState；pristine Detach/旧image变异。依赖P0/P7/P10/P17。 |
| P19 real-harness / NORMAL / 3 | 真实artifact/kernel/reference通用fixture。test/hotupdate/: harness_test.go,raw_probe_test.go,ss_reference_test.go | §8两reference名；删独立ledger/用被测codec自证变异。依赖P0/P1/P8。 |
| P20 real-flow / NORMAL / 3 | TCP原连接exec/carrier/half-close。test/hotupdate/: flow_exec_test.go,carrier_test.go,flow_lifecycle_test.go | T39–T41全部变异。依赖P1/P14/P16/P18/P19。 |
| P21 real-ss / NORMAL / 2 | 独立SS trace与silent revoke。test/hotupdate/: ss_exec_test.go,ss_revoke_test.go | T42/T43；重Seal/重Dial/旧授权变异。依赖P8/P10/P14/P15/P16/P18/P19。 |
| P22 real-rollback / NORMAL / 2 | 第二cut与真实watchdog。test/hotupdate/: rollback_test.go,partial_activation_test.go | T44/T45；旧bundle/MarkReady停watchdog变异。依赖P14/P15/P16/P18/P19/P20/P21。 |
| P23 darwin / NORMAL / 3 | legacy实际测试及unsupported。internal/rawsocket/: socket_darwin_test.go；internal/tunnel/: legacy_darwin_test.go；internal/agent/ssproxy/: legacy_darwin_test.go | T46与TestRawSocketDarwinImageUnsupported；LinuxReplay/假成功变异。依赖P1/P3/P8，实际run需Darwin runner。 |
| P24 continuity-probe / NORMAL / 4 | 原字节/crypto/身份oracle。test/simcluster/drills/lib/: hot_update_flow_probe.py,flow.sh；test/simcluster/tests/: hot-update-flow-probe-test.py,hot-update-flow-probe-test.sh | T47；逐判据删支使负fixture反绿。依赖P0，仅hermetic；deploy等P25。 |
| P25 drill-supply / NORMAL / 9 | 六臂/39.F/固定旧broker与dirty源收据。test/simcluster/drills/: 75-hot-update-flows.sh,39-hot-update-legacy-n1.sh；test/simcluster/drills/lib/: hot_update_flow_faults.sh,legacy_flow_supply.sh,artifact.sh；test/simcluster/lib/: stage.sh；test/simcluster/tests/: hot-update-flow-drill-test.sh,r9d-nonvacuity.sh,prev-supply-test.sh | T48、供给/非空自检；旧ctl冒旧broker/吞native/缺故障先验变异。依赖P14/P16/P24/P27及S4 scripts。 |
| P26 gates / NORMAL / 8 | 真codec/ctx/layering/结构/预算控制。test/architecture/: flow_frame_inventory_test.go,ss_state_inventory_test.go,image_field_inventory_test.go,dataplane_lifetime_test.go,layering_test.go,flow_ss_contract_test.go,upgrade_budget_pin_test.go,gate_registry_test.go | §10全部同文件Controls、T49；每新增不变量独立mutation。依赖P0/P2/P4/P8/P9/P17，最终等P27。 |
| P27 ledgers-matrix / NORMAL / 17 | 库存/risk/race/enum/ctx/verdict登记。test/architecture/testdata/: image_field_inventory.txt,flow_frame_inventory.txt,ss_state_inventory.txt；test/architecture/: legacy_ctx_background_sites.go；test/determinism/testdata/: test_function_inventory.txt；test/determinism/: leak_assert_shape_test.go,enum_switch_default_test.go；test/e2e/: all_phases_test.go,flow_matrix_test.go；test/e2e/parallel/: inventory_test.go,testdata/matrix_units_golden.txt；test/simcluster/tests/: run-all.sh,assert-identity.baseline.tsv,kept-sites.baseline.tsv；test/simcluster/: drill-costs.tsv,expected-verdicts.tsv,expected-verdicts-log.md | T50、TestFlowSSLeakRiskControls/TestFlowSSEnumControls/TestFlowSSMatrixControls；删行/race/anchor/enum/manifest变异。依赖全部实现/测试/P24/P25/P26；drillcost只COMPLETE更新。 |
| P28 docs / NORMAL / 17 | §11文字/地图与最终receipt勾选。CLAUDE.md；docs/: distributed-broker-architecture.md,usage.md,broker-ops.md,gotchas.md；docs/reviews/: hot-update-plan.md,hot-update-stages-plan.md,hot-update-handoff-failures.md,hot-update-tasklist.md,hot-update-flow-carrier-ss-plan.md,hot-update-evidence.md,INDEX.md,hot-update-handoff-protocol.md,hot-update-handoff-process.md；test/: README.md；test/hotupdate/: README.md；test/simcluster/: README.md | docs-layout/promised-guard/gate-registry；假全勾/错误路径/死测试承诺变异。文字可并行，勾选依赖P29 actual receipts。 |
| P29 final-receipts / NORMAL / 1 | lead串行验证/整合证据。docs/reviews/: hot-update-flow-carrier-ss-validation.md | 执行全§0/8/9/10，收每mutant及资源receipt；缺receipt状态必须NOT_RUN。依赖P0–P28实现/测试/文案，P28最后仅据证据更新tasklist。 |

### 12.3 landing顺序与联合测试

P0后所有实现写作并行；leaf先落P2/P4/P5/P9/P11/P19/P24；runtime落P1/P8；serialization落P6/P7/P10；adapter落P3/P12；composition落P17/P18；agent接线落P14/P15/P16；回归/real tests落P13/P20/P21/P22/P23；drill/gates/ledger/docs落P25/P26/P27/P28；P29总验证。存在双向测试依赖的P1/P3/P6/P7、P8/P9作为小集成组顺序合入后一起复跑，不串行等待对方先完成所有测试。

每owner交实现、精确owned tests、baseline/mutation/恢复绿、文件diff清单及Requests；未满足测试依赖可以NOT_RUN，不可宣称green。lead检查无残留ErrNotImplemented、无跨owner文件修改、Agent==125/≤19与完整descriptor/field库存后进入统一验证。所有static gate、lint、deploy验证串行，避免全局锁和多候选收据混杂；parallel worker不得执行simcluster正式验收或争写shared ledger。

## §13 开放问题（仅产品/范围判断；推荐已写入正文）

| # | lead需要裁定的产品边界 | 推荐与影响 |
|---|---|---|
| Q1 | S5是否要求「leader提交撤权后，全局立即禁止所有SS追加Write」，而不只是已应用的本地权威？ | 推荐沿现有bounded local-applied授权，单事务、known-lag拒绝、grant revision可证明；明确不能称global linearizable。若产品要求更强，先单独修订可取消read-authority/Raft范围和接口，不能靠Barrier(timeout)假满足。 |
| Q2 | legacy只持idle listener/retained slot、尚无active业务时，是否允许第一次裸升级迁入？ | 推荐S5严格legacy_session_busy，未知/retained责任不放行；迁入宽化留S8受控事务。代价是空闲但仍占口的legacy expose/proxy也须先清责任；不能将其冒充连续性成功。 |
| Q3 | allocation长期存续期间，64材料历史/node满额是否允许拒绝新材料/新认证，而保留旧历史和survivors？ | 推荐接受该明确配额：保留8192窗口及live盐保护，满额先执行撤权、拒新认证并给失败/重试反馈，不静默GC历史。扩大quota/重放窗口是产品资源取舍，须相应重算SS/state/aux预算。 |

其它事项均已裁定：send-only8MiB、ACK/applied、所有wire编号、ALPN fallback、8s grace、32并行恢复、秘密sealed、watchdog真实故障、Darwin与500ms沿S4、文件唯一owner不是开放问题。lead冻结时将Q1–Q3的实际决定填入顶部裁决；如改变推荐，必须同步修正文内合同、接口、测试、预算/文案后再给worker，不保留互斥方案供实施者自行选择。

## 附录 A：critique逐项处置（BLOCKER/MAJOR全部覆盖）

编号严格对应各critic末尾must-fix列表：B=crit-bytes，D=crit-deadlock，C=crit-crypto，V=crit-drill。共81项BLOCKER/MAJOR；全部采纳其问题与下表具体修正。对错误前提/实现方法的拒绝在对应行单列，不能把「采纳问题」误读成采纳全部草稿方法。代码争议的文件:行集中在§1，下面交叉引用其证据。

### A.1 crit-bytes（22项）

| item | disposition与具体修正 | 本文位置 |
|---|---|---|
| B-1 首帧/stream bind | 采纳：唯一header/body/mode与control发起方；new data OPEN、恢复data RESUME bind，成功前禁DATA | §2.2、T01/T04 |
| B-2 peer applied≤Sent错误 | 采纳：只验B≤A≤E，本代Sent=0仍可接受旧代applied；不把carrier Write当ACK | §3.1、T02 |
| B-3 A/R与partial suffix | 采纳：区分received/applied，保存[A,R)及真实n，去重先于raw Write | §3.1、T03 |
| B-4 OPEN从未到达 | 采纳：两端journal对照、重发同ID/seq/intent；unknown RESUME不Dial | §3.2、T04 |
| B-5 terminal后旧OPEN | 采纳：RetiredThrough/holes永久拒旧seq，满额拒新OPEN不evict证明 | §3.2、T05 |
| B-6 CLOSE尾流/abort | 采纳：两向FinalEnd/Applied、source join与drain/discard receipt，write error不假全交付 | §3.3、T07 |
| B-7 SS撤销映射flow | 采纳：ConnID→FlowID、一致shutdown原OFD及dup、显式终结pub/replay | §6.5、T21/T43 |
| B-8 非法冒号ID | 采纳：点分typed ID；真实Prepare/Restore验证validName（handoff.go:202–213） | §5.1、T32 |
| B-9 target parser/TxPending漏导 | 采纳：跨chunk ATYP/host/port/DNS/期限、RawReadPending与partial Append全部保存 | §3.1/3.2、§6.1、T17/T31 |
| B-10 fresh SS authority可用性 | 采纳：RESUME单事务完整keyset grant；未知权限hold，正常≤5s，W不等NATS120s | §4.3、§7.1、§9.2 |
| B-11 permit准备/丢reply/回滚owner | 采纳：freeze前PREPARE，candidate一次CAS，丢reply同ResumeID查询，execfail本地Thaw与异步abort | §4.4、§5.4 |
| B-12 actual material盐域 | 采纳：aliases共ring、删重加/跨代保留、response原子reserve | §6.3、T19 |
| B-13 yamux共享writer死锁 | 采纳：实际window-update/sendCh图、fence→锁外TLS Close→yamux Close→join | §3.5、T11 |
| B-14 EOF≠target_closed | 采纳：FIN→CloseWrite，反向>8MiB尾流跨cut | §3.3、T06/T41 |
| B-15 全域budget/fence与空listener | 采纳：同pointer、DomainUsed own capacity、image/fence校验；accept有界队列公平reserve | §3.4、P4、T30 |
| B-16 A−1负控/首尾窗/独立SS | 采纳：A−1正确overlap是positive，mutant A+1/漏dedupe是真负控，trailing silence硬判 | §8、§9.1/9.3 |
| B-17 固定测试名与非空清单 | 采纳：三名逐字保留，每个test path/owner/mutation固定、OPEN/shortwrite真实先验 | §8、§12 |
| B-18 B/W及36完整回归 | 采纳：可通信回迁=B、真实watchdog=W；36.A/W/C/L及39.P同候选复验 | §9.2/9.5、§11.1 |
| B-19 S4 coverage与S6/S8/S9边界 | 采纳：S4 L1–L8及六项责任明确；同SID recreation/组件切换不总勾 | §7.4、§11 |
| B-20 private codec/库存/race/leak | 采纳：actual descriptor读写/build三向清单、显式whole-race/risk/anchor、删除字段mutation | §10、§12.1、P26/P27 |
| B-21 ctx伪归因与现门 | 采纳critic纠正，拒绝草稿“用户要求零import”：现门允许自有stopCtx；禁caller ctx，修CLAUDE旧文 | §1（server.go:74–92、gate:33–55）、§10 |
| B-22 bufio/exact OK事实 | 采纳：reader剩余字节给yamux；服务端exact与旧client TrimRight接受集合分开 | §1（tunnel.go:1847–1861）、§4.1、T16 |

### A.2 crit-deadlock（21项）

| item | disposition与具体修正 | 本文位置 |
|---|---|---|
| D-1 共享send/window等待取消 | 采纳：底层TLS锁外Close是真取消边界，禁止先stream Close或只ctx cancel | §3.5、T11 |
| D-2 bounded lookup可join | 采纳：同deadline SQL operation、Stop→cancel→join→ref释放，不丢超时worker | §4.3、T29 |
| D-3 accept前8MiB耗尽 | 采纳：idle listener零reservation，node32 accepted FD、公平reserve；预算不足停accept | §3.4 |
| D-4 SS credit错误 | 采纳：统一send-only8MiB与A/R pending模型，舍弃4+4；如内部credit必须absolute/单调 | §3.1/3.4 |
| D-5 freeze后network commit | 采纳：old carrier pre-PREPARE，新端consume；execfail abort不阻Thaw | §4.4、§5.3/5.4 |
| D-6 锁内Stop/join/self-join | 采纳：锁内标记摘表、锁外Shutdown/Close/join，callback异步交cleanup | §7.2、T22 |
| D-7 sealed FD cut owner | 采纳：每Export Owned、替换成功关旧、失败关新、Thaw/Discard回收 | §5.3、T27/T51 |
| D-8 冒号ID | 采纳：当前validName合法点分名、Prepare/Restore负控 | §5.1、T32 |
| D-9 盐/control/pages/FD总cap | 采纳：replay与aux/state/actor/material/FD瞬时峰值分账，满额前admission收费 | §3.4、§6.3 |
| D-10 local grant/barrier/W可用性 | 采纳：bounded完整local grant，拒绝用Barrier冒1s；更强consistency仅Q1范围修订 | §4.3、§9.2、§13 Q1 |
| D-11 silent victim | 采纳：稳定FlowID hard revoke，不靠fixture关闭，bob继续、FD/replay归零 | §6.5、T21/T43 |
| D-12 Agent文件/method | 采纳：恰125/≤19，无新增agent生产文件，子包承载状态机，golden不放宽 | §10.2、§12 |
| D-13 unique frame/initiator | 采纳：唯一40-byte、9kind、flags/modes、control发起方与独立golden | §2.2、T01/T15 |
| D-14 retirement | 采纳：单调seq、watermark/holes有界，receipt GC不再Dial | §3.2、T05 |
| D-15 恢复并行与5s | 采纳：32active/32并行，active优先、idle后置，堵auth不占全局锁 | §7.1、T38 |
| D-16 SO_ERROR消费 | 采纳：唯一connector消费并持久化，ValidateFile只读身份，失败原receipt复用 | §3.2、T09 |
| D-17 risk/race/anchor | 采纳：flow无yamux仍显式risk/whole tuple，直接test body≥5exercise与删项控制 | §8/10、P27 |
| D-18 private roots与漏owner | 采纳：actual descriptor三向库存；layering/ctx/gate registry/预算读写文件明确owner | §10、§12 |
| D-19 cohort/B/W | 采纳：75.A同批原流跨升级+revoke+rebuild，B与W独立receipt | §9.1/9.2 |
| D-20 S4联合回归 | 采纳：六PTY责任、C-4/C-8/C-10/C-11/B-2与36四臂，未闭合不总勾 | §11.1/11.2 |
| D-21 500ms/max/trailing | 采纳：沿S4校准，计到最后required domain；normal5s逐流硬判，不只p95 | §7.1、§9.1 |

### A.3 crit-crypto（20项）

| item | disposition与具体修正 | 本文位置 |
|---|---|---|
| C-1 原子directive/samepair | 采纳：Enabled/allocation/Keys/Generation/Epoch同tx；错误整体拒；同pair digest冲突 | §4.3、§6.4、T28 |
| C-2 actual subkey/ring/response | 采纳：材料聚合、aliases/FIFO/live集合、response原子reserve | §6.2/6.3、T18/T19 |
| C-3 旧conn与新trial分门 | 采纳：旧actor固定材料、新trial bind重验active版本，ID-only不授权旧PSK | §6.4、T20 |
| C-4 cached RESUME/permit/fence | 采纳：cached receipt每次fresh auth/fence，prepare/final cut/一次CAS/abort唯一owner | §4.4、§5.3/5.4 |
| C-5 execfail旧Server授权 | 采纳：旧端也AuthorityPending，fresh grant前零target Write；Thaw不恢复旧权限 | §5.4、§6.5、T43 |
| C-6 silent revoke/dup | 采纳：不可逆OFD Shutdown+对应pub flow revoke，prepared dup失效 | §6.5、T21 |
| C-7 wire/秘密/FD闭包 | 采纳：唯一header/body/bind/CLOSE receipt，typed sealed codec及完整salvage | §2、§5、§12.1 |
| C-8 dispatch/applied/fired | 采纳：成对int64校验、callback前restore，Fired/CleanupPending不靠timer指针 | §6.5、§7.2、P15测试 |
| C-9 Barrier不证1s | 采纳；拒绝草稿方法：Future.Error/Barrier不作为bounded证据，真实SQL deadline/Stop join | §1（read.go:66–70、node.go:497）、§4.3 |
| C-10 secrets全载体/旧日志 | 采纳：raw/hex/base64 scan真实manifest/Data/marker/fmt/error/log，scrubtarget/keyID | §6.6、T26 |
| C-11 ticket idle/首次detach | 采纳：current owner无短idle TTL，首次8s绝对截止不续期、permit120s目的绑定 | §4.4 |
| C-12 terminal GC旧OPEN | 采纳：水位/holes永久拒旧OPEN，满额拒新，不靠TTL | §3.2、T05 |
| C-13 失败四窗口 | 采纳：未consume/丢reply/raw-active未RESUME/active回迁均有owner/image/attachment规则 | §5.4 |
| C-14 独立AEAD/Seal ledger/partial | 采纳：actual subkey+nonce全actor ledger、独立golden/reference、真实0<n<len | §6.2、T18/T42 |
| C-15 两fuzzer discovery | 采纳：FUZZ_MIN=2与精确package/name启动/完成集合，真实phase/pure decode | §8、§10 |
| C-16 B/W真watchdog | 采纳：B正常逆向升级，W首次commit写失败后真实120s自动回迁，无reaper补偿 | §9.1/9.2 |
| C-17 39.F旧exporter | 采纳：同oracle有效fault先验rc1→assert_bug rc3；safe busy另ability gap | §9.3 |
| C-18 budget全点/历史满先revoke | 采纳：ownUsed/fence全站；SetKeys先删授权再拒新材料，失败不ACK且可重试 | §3.4、§6.4、P4/P15 |
| C-19 Agent/cmd/ctx漏owner | 采纳：125/19棘轮不放宽，cmd/agent.go与ctx账本均独占owner | §10、P14/P27 |
| C-20 S4/handoff完整步骤 | 采纳：S4六项责任及全候选test/drill/docs审阅，scope未闭合仍留S6/S8/S9 | §11 |

### A.4 crit-drill（18项）

| item | disposition与具体修正 | 本文位置 |
|---|---|---|
| V-1 A同cohort | 采纳：原SSH/4TCP/OPEN/half-close/bob贯穿upgrade/revoke/rebuild，非空每阶段 | §9.1 |
| V-2 L真正oldbroker | 采纳：首业务前v0.6 broker、unit/proc SHA/空ALPN/raw证据；fixed与dynamic N-1分开 | §4.1、§9.3 |
| V-3 F提前dispatch/真旧exporter | 采纳：共同exec setup前分派，valid fault与同judge rc1，busy不冒continuity | §9.3 |
| V-4 unique wire/bind | 采纳：P0 header/kind/flag/body/phase/data bind全部冻结 | §2、T01 |
| V-5 真W与B区分 | 采纳：可执行首commit filesystem failure合同，产品自动watchdog、SS持续fresh grant | §9.2、T45 |
| V-6 首尾窗/最终seq | 采纳：独立双向ledger、预声明final seq/finite deadline、trailing silence、粘性EOF/RST | §9.1、T47 |
| V-7 authority revision/混部/5s | 采纳：单local-applied tx/known-lag拒绝、grant revision证据、Q1范围明确、32并行 | §4.3、§7.1、T15/T29/T38 |
| V-8 silent victim | 采纳：SS-key→FlowID revoke/shutdown，产品FD/worker/replay归零 | §6.5、T21/T43 |
| V-9 six columns/fiveheaders/native | 采纳：六列TSV、五header、rc0/1/2/3/75、parent join/owner子串原契约 | §9.1/9.4、P25/P27 |
| V-10 六出口/完整S4/边界 | 采纳：同source/artifact receipts逐出口，36四臂/39.P、C-10仍留S8 | §0、§11 |
| V-11 names合法 | 采纳：dot-separated typed resources，真实handoff验证 | §5.1、T32 |
| V-12 永久退休 | 采纳：receipt删除后退役水位仍拒旧OPEN，不能重Dial | §3.2、T05 |
| V-13 actual subkey nonce/aliases | 采纳：所有方向材料统一ledger、受控response collision、跨代no-reSeal | §6.2/6.3、T18/T19 |
| V-14 8MiB/256MiB/profile | 采纳：唯一pointer、send-only、pending/aux分账、normal224MiB预留与saturation另测 | §3.4、T30 |
| V-15 完整freeze窗500ms | 采纳：沿S4校准，终点含carrier/SS所有required domain；5s硬判 | §7.1、§9.1 |
| V-16 private库存/enum/race/fuzz | 采纳：actual codec/closed family/explicit risk/whole matrix、两fuzzer发现精确2 | §8/10、P26/P27 |
| V-17 owner/P0排他/S4冲突 | 采纳：30包唯一文件，P0失败桩时间互斥，S4先落shared文件再发包 | §12 |
| V-18 exact125/结构四维 | 采纳：Agent==125，files/code/Broker/cmd所有棘轮不放宽 | §10.2、§12 |

### A.5 MINOR一并处置

| item | disposition | 位置 |
|---|---|---|
| B-23、D-22 事实措辞/六列/head/count | 采纳：ACK不释capacity、ring新增head/count、自有ctx允许、server/client OK集合分清、TSV六列 | §1/2/3/6/9 |
| D-23 最终name/path/owner/mutation/39.P | 采纳：固定§8身份与§12归属，mutation receipts，39.P不改 | §8/9/12 |
| C-21 ctx、C-22命名/Darwin、C-23校准 | 采纳：无用户伪归因、actual runner分列、500ms完整校准 | §1/7/8/10 |
| V-19 70/72能力边界 | 采纳：rm立即reuse与Stop15s分别签；wrongPSK sink数据、silent revoke/FD分别签 | §9.5 |
| V-20 estimate/worst/actual/dirty | 采纳：臂集重算，2×worst ceiling，actual只COMPLETE，dirty内容digest | §9.5、P25/P27/P29 |

任何后续实现发现本计划与代码不一致，提交精确文件:行及最小反例给lead；禁止实现者默默换协议/范围、弱化oracle或放宽ledger。冻结前lead必须裁定三开放问题、检查P0展开契约及文件转移清单；冻结后本文件副本进入P28的binding plan，所有出口仍以P29真实receipt为准。
