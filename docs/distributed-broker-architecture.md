# 分布式 broker 架构文档（proto v2）

> 本文是"把单点 broker 改造成分布式 HA 系统"这一 epic 的**架构基线（实现尺）**，承接 `docs/reviews/distributed-broker-requirements.md`（需求基线）。
>
> **流程状态**：Stage B 定稿 → **已过 Stage C 全部三轮对抗性审查并三次改稿**（报告见 `docs/reviews/dist-broker-arch-review.md`、`-round2.md`、`-round3.md`）→ **外审 round-1（FAIL）已处置**：6 条 finding（F1–F3 High：apply.* 鉴权双契约 / force-single runbook / cluster status 直连 Raft 矛盾；F4–F6 Medium：per-expose 地址粒度 / alert ack 集群级 / §18.4 架构契约升正文）已逐条回写正文、§18 降为审计轨迹、§16 补偏离登记（报告与回复见 `docs/reviews/dist-broker-arch-external-review.md`）。**外审 round-2（FAIL）已处置**：3 条契约欠定义（RF1 apply.* 发布者身份=broker nkey AuthUsers/`PermissionsForBroker`，§6.2；RF2 membership 门控=入群密码学 join PoP + remove/upsert 的 leader-local admission，FSM 不假称校验不可复制的 origin proof，§8.1；RF3 稳定 tunnel 证书的存储/Raft 写/轮换契约，§15/§7.7）+ 2 非阻塞已修。**外审 round-3（FAIL）已处置**：R3F1（membership 拆**两阶段**——roster `ClusterNodeUpsert`+join PoP 由 follower 在 `Apply` 复算，`raft.AddVoter` 仅在其 committed 后发起、非 FSM 校验点；半成功 phase + 顺序 + 幂等 + §13.11 门，§8.1/§5）、R3F2（tunnel cert 轮换定 **wire-list `cert_pins{current,previous,valid_until}`** 无状态 agent 契约，§15/§7.7/§2/§13.7）、R3F3（§8.3 create/add 分发 `broker.nk`+稳定 tunnel cert、§6.2 generated nats config 同含 auth_users 与 static nkey permissions）。架构级结论收敛自洽；**纯实现精度长尾见 §18.4(B)，留 plan 阶段必解**。→ **外审 round-4 PASS / 放行（2026-06）**：4 轮外审（F1–F6 / RF1–RF3 / R3F1–R3F3 全部回写正文）后，本文档**作为下一大版本实现基线进入 plan/implementation**（报告见 `docs/reviews/dist-broker-arch-external-review.md`）。**开发 phase 分解（依赖分明的 D0–D9，体例同主项目 P0–P11）见 §19。**
>
> 语言：中文叙述；代码 / 标识符 / subject / 表名 / 配置键英文。所有结论**已逐条对齐真实代码树**（migrations 到 0007；`SetMaxOpenConns(1)`+rollback-journal；auth_callout 用普通 `Subscribe`、CONNECT 时一次、`JWTTTL=24h`；`go.mod` **只有 `hashicorp/yamux`，无 raft**；8 处 `CURRENT_TIMESTAMP`；`tunnel.denyIsTransient` 默认 **terminal**、agent 端 `InsecureSkipVerify`；**≥71** 处内联 `pubAudit*/pubSysEvent`（实现期 lint 点清，非"约 15/63"）；存储助手均 `*sql.DB` 自开事务；`disk_pressure` 仅 `disk.go` 的 fire-and-forget `pubSysEvent`（无存储/ack/banner）；`tunnel` server 每次 Start 自签**临时**证书、agent `InsecureSkipVerify`；`port_allocations` row_id 是 AUTOINCREMENT；`PermissionsForBroker` 给每台 broker `s.*.audit.>`/`$JS.API.>` 通配 pub；`adminsock` 仅 0600 文件权限、无签名密钥；P13 仅 **CONDITIONAL PASS**）。

---

## 0. 范围、北极星与显式不做

控制面状态用**内嵌 Raft + 本地 SQLite**（rqlite 模式，单 shard）HA；消息+审计用**外置 NATS 集群 + JetStream**（方案 C：tetherd 编排）HA。

- **N=1 与今天功能等价**（功能等价，非字节级，见 §3.6）；现网平滑变 N=1 集群。
- **N=2 = 过渡态**（非一等档，无完整性/无 witness，掉一个即只读），`cluster status` 主动警告。
- **N≥3 奇数 = quorum HA（唯一推荐生产形态，0 丢失承诺的唯一范围）**。
- 失多数派 → 默认只读 + 运维 `force-single` 逃生（放弃完整性）。

**HA 北极星只覆盖需求点名的数据**：session / member / node / process / port + 审计。**显式不在 v1 HA 范围**：
- **P13 proxy（"自建机场"）**：P13 本身仍 CONDITIONAL PASS、是 post-1.0 叶子、不在北极星。**proxy 在 v1 维持 single-home / best-effort**——`proxy_meta.generation`、`escalateProxyGen`、`proxy_epoch`、`proxy_ready` 等 P13 fencing **不进 Raft**，故障切换后 proxy 正确性**不是 v1 HA 保证**（§17 明列）。等 P13 转无条件 PASS 再单独做"proxy HA"叶子增量。**这一刀砍掉本 epic 最大一块新增共识面。**
- 文件传输的 multipath / 最快链路（需求 §1.3 已推后）。

**两个 "REGISTER" 必须分清**（第 1 轮核心纠错）：① **控制面 `register.req`**（NATS）写权威态 → 转发 leader Apply（§4.1）；② **数据面 tunnel REGISTER**（agent 拨 home:7000）→ home broker 本地副本读（§7）。

---

## 1. 信任域与进程模型

### 1.1 NATS 拓扑 = C（外置 + tetherd 编排）
每台 broker 同机一个独立 `nats-server`（保 A.1/A.2 隔离），N 个 routes 组集群、一个 JS domain。`tether cluster *` 生成每台 `nats-server.conf`（routes + 路由 mTLS + JetStream + **auth_callout：共享 account 为 Issuer、所有 broker nkey 为每台 AuthUsers**——跨节点 queue-group 应答的硬前提，见 §6.1）、托管启停。N=1 退化外置单 NATS（今天不变）。

### 1.2 两层复制
状态层 = Raft + 本地 SQLite（已提交 0 丢失，仅 N≥3）；消息层 = NATS 集群 + JetStream（`ev.*`/PTY/transfer 字节 best-effort；`audit.*`/`events` JetStream R≤3）。**铁律**：任何权威消息只由 leader 在其 Raft entry 提交后发布（§4.4 的可重导发布契约）；follower 跑同样 Apply 但跳副作用。**HA 不统一——见 §17 保证矩阵。**

### 1.3 监听面（agent 面隧道认证升为硬要求）
client WSS（Caddy:443/127.0.0.1）；NATS client(127.0.0.1:4222)；**NATS routes(:6222, mTLS cluster CA)**；**Raft transport(:7400, mTLS cluster CA；D3 仅 CA-only X.509，nkey 叶子钉式=D7 join-PoP)**；**tunnel(:7000)——agent 端 v2 必须按 `HomeDirective.cert_pins`（`current`/未过期 `previous`）钉 home broker 证书、都不匹配拒拨**（§7.7/§15 RF3，不再 `InsecureSkipVerify`；只在 N=1 退路保留旧行为）；admin Unix socket(0600)。Raft/route 口私网+防火墙，`cluster doctor` 断言不可从公网到达。

---

## 2. 包布局
```
internal/
  cluster/      NEW. raft node + FSM apply + 成员变更 + 生命周期 + force-single(offline 子命令) + catch-up + status/doctor(+--json/exit code)
  alert/        NEW. 复制 alerts + **单一集群级 ack（§18.3，非 per-identity；记 acked_by 仅供展示）** + 客户端合成 gating + banner；严重告警每新会话必重现
  natscluster/  NEW. 外置 nats-server 配置模板化(含 server_id 确定性命名) + 进程托管 + 路由 mTLS + per-session 流副本重配
  authcallout/  EXTEND. queue-group + 读路径(本地副本+node fail-closed)/写路径(PIN 经 broker-only mTLS 路由转 leader)
  jsstream/     EXTEND. replicasFor(nVoters) + 对 history-<sid> 与 OBJ_xfer-<sid> 的 UpdateStream(Replicas) 重配
  port/         EXTEND. home_broker/rebuild_on_failure/epoch 列；*sql.Tx 变体；ReassignHome；tunnelTokenLookup 加 home==self+epoch 比较
  tunnel/       EXTEND. **per-expose (per-publicPort) brokerAddr**（§18.2.2，非 per-session）：addr 放每个 clientSession（已按 publicPort 键）、Open/AddProxy 收 addr、dialAndRegister/redialWithBackoff/swapTransport 读 `sess.brokerAddr`；一个 tunnel.Client 并发扇出 N 个 home；v2 常量 home_catching_up(transient)+有界重试；cert_fp 按 expose 线程化钉证
  adminsock/    EXTEND. cluster.* + alert raise/clear；危险操作 TTY+typed node_id；**仅本地、非 leader 上 fail-fast 指名 leader host**
  auth/         EXTEND. account 签名私钥 + 专用 node-identity 密钥(≠bus nkey)——**仅用于入群 PoP 挑战-应答 + Raft/route mTLS 叶子钉证（§18.1），不再做 apply.* 转发签名**；短 TTL JWT；**`PermissionsForBroker()` 加 `cluster.apply.>`+`cluster.>` pub/sub，仅授给 broker nkey AuthUsers（apply.* 发布者身份，§6.2 RF1）**
  proto/        EXTEND. v2 subject grammar SSOT(tether.v2)；HomeDirective{node_id,tunnel_addr,**cert_pins{current,previous,valid_until}**(§15 RF3 轮换双 pin),epoch}/RehomeDirective(带 ReassignHome 的 raft_index)；register 自报 nats server_id + 第 6 字段 epoch(§7.2)
cmd/tether/
  cluster.go alert.go   NEW（force-single/recover 为 daemon 停机下的 offline 子命令）
```
所有 Raft-owned 表写助手须提供 `*sql.Tx` 变体（Apply 组单 txn）；无变体者禁入 Apply 路径（lint）。

---

## 3. 状态层（Raft + 本地 SQLite）

### 3.1 选库（Stage-B/合并前置门）
`hashicorp/raft` + `raft-boltdb/v2` + `FileSnapshotStore`。**raft 是全新依赖（go.mod 现只有 yamux）**：pin 版本、`CGO_ENABLED=0`+Go1.25 编译、**确认 `Config.PreVoteDisabled` 字段在**（hashicorp/raft v1.7.x **无 `Config.PreVote`**；真实字段是 `PreVoteDisabled bool`，`DefaultConfig()` 不设它 → 零值 false → pre-vote **缺省启用**。另：raft 在 transport 未实现 `WithPreVote` 接口时**静默关闭** pre-vote，故"字段在"不足证"行为生效"，见 §18.2.14）（§8.4/§9.4 正确性前提）——升为合并前置门。
> **D1 注（快照/恢复 API 按 driver 实际）**：SQLite 侧用 **modernc.org/sqlite v1.50.0**（非 mattn/go-sqlite3）——online-backup/restore 走 modernc 的 `NewBackup`/`NewRestore`（非 mattn `Backup()` 或 `VACUUM INTO`），机制与 raft v1.7.3 的快照 meta.Index 真相见 **§3.8 D1 实现修正**。

### 3.2 写入模型 + 读一致性
**写**：leader 在 `Plan` 把命令渲染成**键/fence 值无不确定性**的 SQL，`cn.Apply→raft.Apply`，各副本 Apply 同 txn 写 `applied_index`。**任何复制写路径 ZERO 直连 `db.Exec`**；废黜 leader 的 `raft.Apply` 返回 `ErrLeadershipLost`。
> **裁定（R2-blocker：stale-leader 读窗）**：写 fencing 只管写。**读一致性契约**：
> - 正确性敏感读（force-single 前的 peer 健康、撤销判定）走 **`raft.VerifyLeader()`/ReadIndex barrier**；
> - `ps`/`history`/`status` 显式降为**有界陈旧**（可能反映尚未下台的旧 leader），§17 列窗口；
> - auth_callout 本地副本读（已 provision）加 **node 级 fail-closed**：本节点**自上次成功 leader 接触 > `T_fence`（§8.4 pin=10s，统一单调时钟谓词）** 即停止 authorize；若**能**听到更高 term 则立即 fence（更快路径），但不依赖之——无接触分区的旧 leader 靠 `T_fence` 超时而非听到更高 term 也会停（§8.4(a)），故被分区的旧 leader 不再放行连接。
> **D3 实现修正（先父后子 · 2026-06，详 `docs/reviews/d3-plan.md` R2）**：fail-closed 谓词按 raft v1.7.3 落为 `cluster.Node.LeaderContactStale(now)` 单调时钟一函数（§3.2/§6.2/§8.4 共用），**leader/follower 非对称**：**leader 分支无状态**——`raft.State()==Leader` 即新鲜（raft 的 `checkLeaderLease` 在失 quorum 当刻 `setState(Follower)`，`raft.go:1036/937`，故 leader 态本身=租约内 quorum 接触证明；**不**用 `LeaderCh()` 自维护时间戳，那只在领导权切换时触发、长任期 leader 会被误 fence）；**follower/candidate** = `now − raft.LastContact()`，零值（从未听到 leader）即 fence。**真实"被隔离仍放行"窗口 = `LeaderLeaseTimeout(500ms) + T_fence(10s)`**（旧 leader 下台时 `setLastContact()` 重置 follower 钟，`raft.go:510`），有界 fail-open、与 §8.4(b)"非反脑裂结构性保证"一致——非 bug。读路**绝不调 `VerifyLeader`**（失 quorum 不锁死，§6.2）；`VerifyLeaderRead` 仅留给正确性敏感读（force-single peer 健康/撤销，D7）。谓词 fence 的是 leader 接触丢失、非 apply 活性（wedged-FSM leader 仍读新鲜，那是 D1 fail-stop 的职责）。

### 3.3 Plan/Apply 确定性（"键/fence 无不确定性"，非"处处无"）
- **`Plan*`（仅 leader）**：读 leader DB、生成并烤入一切喂 PK/唯一索引/fence 的值（port、token_hash、sub_id、ULID pid、时间戳），渲染 SQL，可返回业务错误。
- **`Apply*`（每副本）**：exec leader 给的 SQL + 同 txn 写 `applied_index`/`applied_term`。
- **规则缩为"键/fence 无不确定性"**（非"Apply 处处无计算"）；装饰列（时间戳、`applied_at`）只需功能收敛。
- **每个自生成 mutator 显式 Plan/Apply 拆分清单**：`proxysub.Create`、`port.Allocate/AllocateProxy`、`ProvisionWithPIN`（verify=Plan、INSERT=Apply）等——**编译期断言 Apply-可达函数不 import `crypto/rand`/`math/rand`/`oklog/ulid`**。
- **删 `port.go cfgWithDefaults` 的 `time.Now()` fallback**。lint 扫 `internal/{port,proc,node,session,agentprov}`（proxysub 随 P13 移出 v1，§0）+ 禁 FSM 外对 Apply-owned 表 INSERT + 禁 Apply 调 `*sql.DB`-bound mutator。

### 3.4 确定性雷区（收敛后）
| 雷区 | 处置 |
|---|---|
| `time.Now()`/`CURRENT_TIMESTAMP`（8 处，含 `storage.go schema_migrations`） | leader 烤时间字面量；`schema_migrations` 是操作元数据、**排除出等价断言**（各副本自盖）|
| `genToken`/`rand.Read`/`ulid.Make` pid / entry `ReqID`+`IssuedTS` | leader Plan 一次铸成、字节不可变；去重 key 用 `raft_index` |
| `port_allocations` PK | **保留 AUTOINCREMENT，不做 0008 重建表**（见 §3.6）|
| `reconcile.go` G.1 批 map 迭代 | leader 算好、**按 (pid/port ASC) 排序**成一条 `ReconcileBatch`，DB 变更与审计顺序 replay-stable |
| ~~proxy_meta.generation / proxy_epoch~~ | **随 P13 移出 v1 HA（§0），不进 Raft** |

> **D2 实现修正（先改正文 · 2026-06）**：① 表中「`ulid.Make` pid → leader Plan 铸」**字面有误**——proc 的 pid ULID 由 **agent 侧铸**（`agent/exec.go`/`run.go` 的 `proc.NewPID`，broker 收到成形 pid），**leader 对 proc pid 不烤任何东西**；故 §13.1 banned-import lint 是 **Apply-reachability-scoped**，`oklog/ulid` 合法留在 `internal/proc`（`NewPID` 非 Apply 可达）。port token（`genToken`/crypto/rand）确由 leader 在 `PlanAllocate` 铸。② 「leader 烤时间字面量」的**格式 driver-coupled**：modernc.org/sqlite v1.50.0 把绑定 `time.Time` 存为**精确的 Go `time.Time.String()`**——**忠实于时区甚至 monotonic 读数**（实测：UTC→`… +0000 UTC`、`+0800`→`… +0800 CST`（不转 UTC）、`time.Now()`→带 `m=+…` 后缀），**非 RFC3339**。故 `LitTime(t)` = `t.String()`（**不强制 `.UTC()`**），且**调用方必须传入与对应 live mutator 完全相同的 `time.Time`**——port/proc/node/agentprov 绑 `now.UTC()`、**session.\* 绑 raw `now`（含 monotonic，是 live 既有怪癖，D2 严格等价须忠实复现）**。传错时区会静默破坏 §13.2/DIFF-1 等价 + 线上 `port.ListAllocatedForOfflineNodes` 的字典序 `last_heartbeat_at < cutoff` 比较。已实测字节相等（`TestLitTimeMatchesBoundParam` blocking 门）；driver 升级必重验。详见 `docs/reviews/d2-plan.md` 裁定 R1。

### 3.5 不进 Raft（leader-local 活性 + 列级 lint 替代物理拆表）
活性态（`status/last_heartbeat_at/proxy_ready`）leader-local、永不进快照、G.2 一拍重建；身份态（`boot_id/release/proto/proxy_capable/registered_at`）仅 Apply 写。
> **裁定（R2-nit 简化 + R3 §18.4 修正快照形状）**：**不做 0009 物理拆 `nodes_identity/nodes_liveness`**——改**列级 lint**：`Apply 永不写 status/last_heartbeat_at/proxy_ready`、本地永不写身份列。**不变式改述（架构契约，非快照排除）**：快照采 online-backup 整文件 page-copy，**做不到"投影掉活性列"**；故正确不变式是 **「Apply 永不写活性列 + 恢复/换 leader 无条件 G.2 一拍重建活性列」**——活性列即便随快照带过去也会被重建覆盖，不依赖快照排除。心跳/`ReconcileStates`/`SetProxyReady` 只走活性列。
> **D1 实现修正（恢复后活性基线）**：§3.5 说恢复无条件重建活性列，但未命名"刚恢复、尚无心跳"时的基线。定 **`status=OFFLINE`、`proxy_ready=0`、`last_heartbeat_at=NULL`**（match `node.go`：ONLINE 仅由活的 Register/Heartbeat 置）。`RebuildLiveness(db)` 在 Restore 末**无条件**跑此重置；完整 G.2 reconcile（按真心跳重算）在后续 phase。
> **D2 实现修正（先改正文 · 2026-06）**：`node.Register` 的活性半（`status='ONLINE'`、`last_heartbeat_at`、**`proxy_ready=0`**）拆出为 leader-local 后，**`proxy_ready=0` 复位必须在每次 re-register 无条件触发**（含身份是 content no-op 的重连）——这是 round-6 F8 安全复位（重启/降级 agent 必须重新 ACK 才进 `/sub`）。`OpNodeRegister` 的 Apply 只写身份列、`ON CONFLICT` 不碰活性列；活性复位由 leader-local 写承担。**D2 ops-only 下线上 `node.Register` 仍是单条原子 UPSERT 不变**（拆分是 op 定义属性，供 lint+DIFF-1 exercise，非线上写）。

### 3.6 代理键
> **裁定（R2-minor）**：**保留 AUTOINCREMENT**——单写 leader 串行 Apply 下 SQLite 自赋 rowid 已确定性；leader **省略 row_id 让 SQLite 赋**（match 今天 `port.go` INSERT）。撤"字节级一致"。等价测试（§13.2）比**逻辑内容**（`.dump` 去 `schema_migrations` 或 per-table 排序 SELECT 哈希），**不比文件字节**（避 free-page/vacuum 假发散）。

### 3.7 双存储崩溃一致性（applied_index 权威源 = SQLite）
> **裁定（R3-blocker：纠 R2 的不存在 API）**：hashicorp/raft **没有**"FSM 覆盖 raft 内存 lastApplied"的钩子——raft 只从自己的 snapshot meta + log 推 lastApplied，启动时**无条件**把 `log[lastSnapshot.Index+1 .. commit]` 重投给 FSM。故正确机制是两条不变式：
> 1. **快照 index ≤ SQLite 的 `applied_index`，永不超前**——快照与 SQLite 提交原子地/其后取；这样 raft 截断 log 时不会丢掉 SQLite 尚未落的 entry。
> 2. **Apply 对 `index ≤ applied_index` 的 entry 必须是已验证的 no-op（幂等重放）**——因为 raft **会**重投已应用 entry，FSM 靠"读本地 applied_index 跳过"自我幂等，**不靠 raft 跳过**。
>
> `applied_index` 与 Apply 同 txn。kill-9 矩阵断言"**FSM 容忍 raft 重投已应用 entry**"（真实窗口），而非虚构的覆盖调用。相对增量随 P13 移出后已无。
> **D1 审查纠正（不变式 #1 字面表述有误）**：上面 #1"快照 index ≤ applied_index 永不超前"**字面错**——见 §3.8 的「不变式纠正」：raft 的 `snapshot.Index` 可被**无变更的 barrier/config entry** 合法地超过 `applied_index`（良性）；真正保证是"**重启不丢任何已提交 LogCommand 变更**"。承重纪律 = `FSM.Apply` 对未落库命令**fail-stop（重试→panic）而非返回错误**（否则 raft 前进 lastIndex + 一次快照 = 真丢数据）。

### 3.8 快照 / 恢复
采 **WAL** + SQLite online-backup，**专用独立只读 sqlite handle**（**非放松写池**——`SetMaxOpenConns(1)` 仍管写、保 `port.Allocate`/`session.Create` 复合读改写串行化）。WAL 下重验 FK/单写者；并发测试：持续 Apply + 流式 backup，无超 `busy_timeout`、无 `SQLITE_BUSY`、backup 反映一致提交点（含最后已提交 WAL frame）。恢复 = 临时文件 + `integrity_check`（撕裂拒绝）+ migrations 前向 + 原子换入。
> **D1 实现修正（先改正文 · 2026-06 · driver = modernc.org/sqlite v1.50.0，非 mattn）**：online-backup 走 modernc `NewBackup(dstUri)`（`conn.go:1051`，经只读 handle 的 `Raw` conn + 接口断言取）——它**自开目标 conn、把页拷进一个目标文件**（不 stream 到 `io.Writer`/`SnapshotSink`），故 `FSMSnapshot.Persist` **两段**：① `NewBackup(临时文件)` + `for { more,err:=b.Step(64); if err…; if !more break }`（`Step`：**`true`=还有页 / `false`=完**——反了会发**截断快照**）+ `defer b.Finish()`（Finish 关那个无人管的目标 conn，漏 Finish 即 **fd 泄漏**，被 fd 门抓）；② `io.Copy(sink, 临时文件)`。快照文件是**非 WAL 独立库**（`integrity_check` 对它跑）。**raft v1.7.3 真相 + 不变式纠正（D1 审查 must-fix）**：`FSMSnapshot.Persist` 只写内容、**无 API 设/盖快照 meta.Index**；raft 在 `runFSM` **无条件**取 `snapshot.Index = 其内部 lastIndex`（= 最后一个**已返回**的 `FSM.Apply` 的 log index，**不管返回值**），快照不嵌独立 index。故原"快照 index ≤ applied_index 永不超前"**作为字面不变式是错的**——两种合法情形令 `snapshot.Index > applied_index`：(a) **LogBarrier/config 尾**（Barrier 进 runFSM 但非 LogCommand，FSM 不动 applied_index，raft.lastIndex 却前进——**良性**，这些 entry 不携带变更）；(b) **Apply 把未提交命令返回给 raft**（若 `applyCommand` 将瞬时 Begin/Commit 错误**返回**，raft 仍前进 lastIndex，此后一次快照把 meta.Index 钉到 K 而 SQLite 停 K-1，重启后该 entry 永不再投 FSM.Apply → **真丢数据**）。**正确不变式**：「**重启不丢任何已提交 LogCommand 的变更**——raft 重投 `[lastSnapshot+1..commit]`、FSM 按本地 durable `applied_index` 自跳；`snapshot.Index` 只可能被**无变更的** entry（barrier/config）超过 applied_index」。**实现纪律（fail-stop）**：`FSM.Apply` **绝不把未持久落库的命令返回给 raft**——瞬时错误**重试，仍失败则 panic 停机**（`applyMaxAttempts`），消除 (b) 的丢数据窗口；§13.3 用 `TestSnapshotThenRestart`（快照后再写→重启→断 N+M 全在且 reapply==M）+ `TestSnapshotAfterBarrier`（(a) 的良性 gap 仍收敛）+ `TestFSM_FailStopOnPersistentApplyError`（断 panic）替代原 vacuous 断言。**恢复用 `NewRestore(srcUri)`（`conn.go:1065`）就地拷入活 conn**——不 `os.Rename` 盖开着的 inode（`FSM.Restore` 在 `NewRaft` 内重入、写池已开，rename 会留 stale `-wal/-shm`）：恢复 = 流 `rc` → 同盘临时源文件 → `integrity_check`+`foreign_key_check`（撕裂拒、不动活库）→ `storage.ApplyMigrations` 前向 → `NewRestore` 就地拷 → §3.5 活性列重置。
> **WAL 作用域（D1 限定）**：WAL（+ `synchronous=FULL`，保 `applied_index` 在 §3.7 下持久；WAL 缺省 `NORMAL` 掉电可丢最后已提交 frame）**只开在 cluster FSM 的独立库文件**（D1 可测构造、对 P0–P13 冻结的 broker 库**零爆炸半径**，经新 `storage.OpenWAL`/`WithWAL`，**不翻**共享 `storage.Open` 的 DSN）；真实 mutator + FSM 合到单 WAL 库是 **D9 一次性迁移**（`journal_mode=WAL` 落库头，故非 D1 副作用）。**崩溃模型边界**：kill-9 SIGKILL 是**进程死、非掉电**——矩阵证的是 OS 可见已提交前缀的崩溃一致性，**非** fsync/掉电持久性（勿让文档/测试过度声称 FULL-vs-NORMAL 的区分被 SIGKILL 证到）。

---

## 4. 写转发模型 + Schema

### 4.1 session-scoped 写落在非 leader 的控制流
任意 broker 收到 session-scoped 控制请求（`register.req`/`exec`/`run`/`expose`/`kill`/`push`/`pull`）：入口校验后**经 broker-only subject `tether.v2.cluster.apply.<verb>` 转 leader**。该 subject **走 mTLS cluster routes（broker 间已加密）+ broker-only pub ACL 鉴权**（发布者身份 = broker nkey AuthUsers，可执行定义见 §6.2 RF1）——发起转发的非 leader 本身已是受信 broker。**转发不带 origin-proof 签名 / term / 专用 role：在"所有 broker 同等可信"下 per-broker 转发签名对沦陷 broker 无遏制力（它已在信任边界内），故转发是一致性/路由机制、不是安全边界**（§18.1 / §18.2.3 裁定；遏制由入群 PoP + Raft/route mTLS 叶子钉证提供，见 §6.1/§8.3/§18.2.4）。leader `Plan`→`cn.Apply`→各副本 Apply→**leader post-commit 发唯一审计**（§4.4）。reply 经原 inbox 回 ctl。
> **fail-closed + 跨重试幂等（R3 §18.4 升为正文契约）**：转发落到刚下台 leader（propose 返回 `ErrLeadershipLost`）→ 应答 broker 转 **typed `not_leader` → fail-closed deny + 客户端重连**，不重试旧 leader、不歧义超时。**幂等键由发起 broker（或客户端）在转发前铸、且跨重试稳定**——**不能由 leader 铸**：`ErrLeadershipLost` 含"entry 已提交但 ack 丢失"的歧义，重试会打到新 leader，若键随 leader 重铸则新 leader 无法去重那条已提交 entry → 重复执行。故 FSM 按此稳定键在 Apply 去重已提交 entry（`r:ReqID` 取该键，§5）。
> **G.1 reconcile 全程 leader 权威**（需求 §4.1）：home follower 转 agent 清单 → leader 把**整个 reconcileOnRegister 结果算成一条 `ReconcileBatch` entry**。
> **裁定（R3-blocker：审计可重导）**：`reconcile.go` 现把 DB 变更（`proc.MarkExited`）与内联 `pubAuditProc/pubAuditPort` 交织、迭代 Go map（`agentByPID`，序不定）、且消费 **agent 上报但不在任何持久行里的字段**（`name/local_port/rc` 来自实时 `NodeRegisterReq`）。故 `ReconcileBatch` entry **必须把完整解析后的元组集（pid/port, kind, nid, name, local_port, rc）按 leader 定的全序（port ASC / pid ULID ASC，覆盖两个 map 循环 + killed_orphan 列表）烤进 entry**——使任何新 leader **只读 entry** 就能字节一致地重导审计，绝不再读实时请求或 leader-local map。**§2.2 RTO 含此 leader 往返**（往返预算见 §4.4 末与 §18）。

> **D4 实现定稿（先改正文 · 2026-06 · 详 `docs/reviews/d4-plan.md`）**：D4 = **build + prove 写转发 `apply.*`（follower→leader）+ 跨重试幂等 + ReconcileBatch leader 权威自足**，**不切线上 broker**（同 D2/D3 R1：`cmd/tether/serve.go` 字节不动、不构造 `cluster.Node`、PIN seam 缺省 nil = 今天直连 `ProvisionWithPIN`/`AddMember`、forward responder 永不 subscribe；cutover=D9）。以真 ≥2 节点 `test/d4`（首个**同时**跑路由 NATS + mTLS raft 写的 harness）证之。范围限三动词 `provision`/`join`/`reconcile`（§13.7 命名者；`exec/run/expose/...` 转发是 D9 前死代码、不 wire）。
> - **幂等键 = 发起（转发）broker 铸、跨重试稳定、内容寻址**（绝非 leader 铸、绝非随机 nonce）：`ErrLeadershipLost` 含"已提交但 ack 丢失"歧义，重试打到新 leader、随机 nonce 跨 reconnect/crash 不可重得（`authcallout.Handle()` 每次新建、D3-R3 客户端 deny 即重连），故键须**只依赖重试时还能看见的请求身份**经域分隔 SHA-256 推导（128-bit hex）。**键作用域（外审 F1 修正）**：**只有 `reconcile` 带键** = **`reconcile\0sid\0nid\0bootID\0digest(排序后的 LocalProcesses+LocalPorts)`**（按原始转发请求推导、非 resolved decisions——§4.1 是"follower 转 agent 清单 → leader 算结果"，转发方铸键时尚未 resolve；`bootID` 天然当 epoch、重启→新键、ack-lost 重试→同键；且保护 D5 审计发布不重发同一 register）。**`provision`/`join` 不带键**：其写是 `INSERT OR IGNORE`（结构幂等）且绑定**可被运维 node-evict/kick 删除**——纯 `(sid,nid,fp)` 内容键太粗，evict 后合法重 provision（新逻辑写）会被旧 ledger 行误判重放而跳过 INSERT、返回 ok 但权威行缺失（D9 cutover 后假放行无绑定）；agent 侧无可铸 per-attempt epoch（D3-R3），故这两动词**不经 ledger 去重**，其 ack-lost 重试由结构幂等 + handler 的 already-provisioned 快路兜底。`Command.ReqID`（`{t,v,r,b}` 的 `r`，§5）承载 reconcile 键；空键=不去重（provision/join + D1/D2 op）。`ProposeWithReqID` 在 propose 前校验非空键（外审 F2：非法键否则会成 poison entry 被跳过却回 ok 的假成功）；**dedup ledger 存储是自由工程决定**（"`r:ReqID` 取该键"指信封 `r` 字段、非 KV 前缀）：选**专表** `cluster_reqid_ledger(req_id PK, raft_index)`（migration 0011，在 cluster WAL FSM 库内、D9 前对冻结 broker 库零爆炸半径）——PK 即去重唯一性硬约束、`raft_index` 索引让确定性 GC 范围删有界、不碰 D1 `applied_index` KV 语义。
> - **FSM 去重语义**：`applyCommand` 在既有 `l.Index<=applied` index-skip（rollback `appliedNoOp`）**之后**、applier **之前**加分支——`cmd.ReqID!="" && l.Index>applied` 时查 ledger：命中 = 已提交 entry 的重试落到**新 index**，须**新哨兵 `appliedDedup`（绝不复用回滚的 `appliedNoOp`）**：跳 op SQL、不重插 ledger、**推进 `applied_index`/`applied_term` 并 commit**（raft 无论返回值都推进 lastApplied，不 commit 会 wedge）、`Node.Apply` 映射为 nil 成功；新 `dedupCount` 计数器供非 vacuity 断言。未命中 = 跑 applier + `INSERT ledger` + **同 txn 确定性 GC `DELETE WHERE raft_index < N - reqIDRetentionWindow`**（绝不 leader-local 周期删——非复制 DELETE 打复制表会致 follower ledger 分叉）+ 既有 cursor 写，全在一个 txn。`reqIDRetentionWindow = 1<<20` 索引（≫ 转发重试地平线=agent NATS 重连退避+not_leader 弹跳，秒级；控制面写率低故约数月、有界）；D5 的 `raft_index:kind:seq` 审计去重窗口是另一回事、自管。`decodeCommand` 守 `ReqID` 空或 hex/有界（拒 NUL/非 UTF-8，防 raft-log JSON 跨节点异译的 split-brain ledger）。
> - **转发路由 = broadcast `Subscribe` + 仅 leader 应答（非 queue-group）**（D4 实现裁定，详 d4-review.md M8）：apply.* responder 用普通 `Subscribe`（每台 broker 都收），**仅自认 leader 者应答**，follower **静默**（绝不回 not_leader——否则 follower 的快速 not_leader 可能抢在 leader 提交往返的 ok 之前、致伪重试）。queue-group 无 leader 亲和（leader 地址广告是 D7），故不用；选举中无 leader → 无人应答 → 超时 → 可重试。刚下台 leader（`IsLeader()` 真但 Propose/Apply 失败）回 not_leader，新 leader 可能同时回 ok——请求方取**首个应答** + 内容寻址 ReqID 让任何重试在 Apply 对复制 ledger 去重，无双提交/丢写/假放行。`ErrForwardNotLeader` 哨兵（cluster raft-free）由 broker adapter 映射。
> - **typed 应答 + fail-closed**：reply 信封 `{Status∈{ok,not_leader,error}, ErrKind, ErrMsg}`。`not_leader`（`cluster.IsNotLeader`）/ **NATS 超时 / malformed / 未知 status**= 可重试**同 ReqID**（绝不重试旧 leader、绝不超时当成功、绝不未复制本地直写）；`error` = 永久 typed 业务错（`ErrInvalidPIN`/`ErrSessionMissing` 等），`ErrKind` = 稳定 kind 码（**非 `%T`**——sentinel 的 `%T` 不可区分），`ForwardBusinessError.Is` 据此让 `errors.Is(fwdErr, agentprov.ErrInvalidPIN)` 成立、handler 发同样的 `pin_failed` + canonical deny（与本地路径一致）。失 quorum：leader `Apply` 阻塞到 `applyTimeout` → 有界 transient deny。**agent `isAuthFailure` 不改**（拓宽会让真坏 PIN 永久 flap）——改进是 broker 内透明转发让健康 follower 把 PIN 写转 leader、agent happy path 不再见 not_leader；残留 not_leader（选举/失 quorum）仍 transient、内容寻址 ReqID 让重连重试正确去重。
> - **forwarder/responder 归 broker**（`internal/broker/cluster_forward.go`，import nats+cluster）：`internal/cluster` 保持 **nats-free**（今天 import 零 nats，放订阅进去会倒置 broker→cluster 依赖向；L-2：raft 仅在 cluster）。cluster 只暴露 raft-free 原语 `ProposeWithReqID`（`Propose` 委派空键、D3 F1 leader-gate 字节不变）+ `ErrForwardNotLeader`。
> - **ReconcileBatch leader 权威自足**（§4.1 R3-blocker 落地）：`OpReconcileBatch` Command 留可执行 `MarkExited` UPDATE 于 `Body`（唯一复制 DB 变更、不变）**外加新 apply-inert 结构化字段**烤入有序审计元组 `{Kind,NID,PID,Port,Name,LocalPort,RC *int,Ts}`（`name/local_port`/`rc` 这些不在任何持久行的字段由 leader resolve 时从 `req` 捕获烤入）。**编码 = 结构化字段非 audit 表**（无持久 audit 表、审计只在 JS `history-<sid>`，造表即漂入 D5/D8）；`genericExecApplier` 忽略该字段；**`commandVersion` 1→2**（无持久 v1 log、harness 库新建、无 golden v1 fixture，安全；poison 测试断 v1 形状 ReconcileBatch 推进 applied_index 并 loud log 不 wedge）。**`killed_orphan` 无 `rc` 键**（`exec.go:433` 仅 `exit`/`reconciled_closed` 填 RC）：元组存 `RC *int`、replay 复现 kind-gating（orphan→nil）。**`Ts` 剥单调读**（`now.Round(0)`，raft-log JSON 本就 RFC3339 剥单调）使 replay 审计 JSON 与 live 字节匹配。**全序** = proc 元组（`reconciled_closed`+`killed_orphan` 合并）按 PID-ULID ASC、port 元组按 port ASC（覆盖两 map 循环+orphan list）。纯函数 `ReplayReconcileAudit(body) ([]AuditProc,[]AuditPort)` 只读 decoded op（无 live 请求、无 leader-local map、无 DB）、**不串 `raftIndex`/不算去重键**（那是 D5）。**D4↔D5 边界**：D4 = entry 自足（烤有序元组）+ 纯 replay + 证字节一致重导，**D4 发布零**；D5 = post-commit 单写发布 + `raft_index:kind:seq` 去重窗口 + 选后 sweep。**live `reconcileOnRegister` 字节不变**（inline `MarkExited`+inline `pubAudit*`+directive 数组照旧、共享纯分类器 `resolveReconcile` 但不切 op 路=D9）；live-vs-op 等价**按集合/multiset 比**（不动 live 发射序），跨节点 replay-vs-replay **按字节比**（同一烤好全序 entry）。

### 4.2 migrations（精简：保留 AUTOINCREMENT、不物理拆 nodes）
0007 之后：**0008** `cluster_nodes` 名册（含 `nats_server_id`）；**0009** `cluster_meta`(KV) + `alerts` + `alert_acks`（**删 `cluster_revoked_identities`——§18.3：无 writer/reader**）；**0010** `port_allocations` 加 `home_broker`/`rebuild_on_failure`/`epoch` + 索引。cluster/alert 系表无 `CURRENT_TIMESTAMP` 默认。**不删历史 migration 默认值、不重建表**（migration 引擎按名只跑一次、从不回改已应用库；活性列用 §3.5 列级 lint，不物理拆）。术语：roster 用 `node`/`cluster_nodes`，"member" 留给 session 成员；op 命名 `ClusterNode*`。

`cluster_nodes` 关键列：`node_id`(PK,==Raft ServerID) | `name`(UNIQUE) | `node_ident_pub`(≠bus nkey) | `nats_server_id`(natscluster 模板化时确定性赋、agent 自报用它桥接) | `raft_addr/nats_route/tunnel_addr/public_host` | **`cert_fp`/`cert_fp_prev`/`cert_fp_valid_until`**(稳定 tunnel 证书轮换双 pin，§15 RF3/R3F2) | `phase`(承载 §8.1 `JOIN_VERIFIED_PENDING_VOTER`/`CATCHING_UP`/`VOTER`/`VOTER_ADD_FAILED`/`DRAINING`/`RETIRING`——**全大写为 canonical 枚举值**，单条 CHECK 用此 6 值) | `added_at`。**Raft 投票集权威，`phase` 为派生展示态。**

`alerts`/`alert_acks` DDL（无默认时间戳）：`alerts(id,kind,severity,dedup_key,state,message,raised_at,cleared_at)` + `idx_alerts_dedup_active`；`alert_acks(dedup_key,acked_by,acked_at, PRIMARY KEY(dedup_key))`——**单一集群级 ack（§18.3：一条 alert 一个 ack，`acked_by` 仅记录"谁 ack 的"供展示，非 per-identity 簿记），删 snooze_until/session_nonce/identity_fp**。需求 §8 原要求 per-identity 生效，本架构降为集群级——见 §16 偏离登记。

---

## 5. Raft op 集（自描述、不要 catch-all）
`{t:OpType, v, r:ReqID, b:渲染 SQL+元}`。op：`SessionCreate/Tombstone/HardDelete`、`MemberJoin/Kick`、`NodeRegister/Remove`、`ProcCreate/MarkExited/ReconcileBatch`、`PortAllocate/Free/Revoke/ReassignHome`、`RotatePin`、`AlertRaise/AlertAck/AlertClear`、`ClusterNodeUpsert/Phase/Remove`、`ClusterMetaSet`。
> **裁定（R2-major）**：**删 `GenericRowMutate` catch-all**（绕过 per-mutator lint、毁 raft-log 可读性）——低频 op 本无运行期成本，保持窄类型化。`ProcGC` = **leader-local**（选后 G.2 重跑，不进 Raft，从 §3.4 baked-time 表与 lint 移除）。proxy/`AllocateProxy/ProxyGenAdvance` 随 P13 移出 v1（§0）。`ReconcileBatch` 承载 §4.1 整个 reconcile 结果。
> **层次澄清（R3F1）**：上列均为**业务 FSM op**（走 `FSM.Apply`，followers 可校验）；**`ClusterNodeUpsert` 携 `{node_ident_pub, join_nonce, join_sig, cert_fp}`，followers 在 Apply 复算 join PoP**（入群密码学门）。**`raft.AddVoter`/`raft.RemoveServer` 不在本 op 集**——它们是 hashicorp/raft 的**配置变更路径、非 `FSM.Apply` 拦截点**；membership 两阶段顺序（先 `ClusterNodeUpsert` committed 再 `raft.AddVoter`）见 §8.1。
> **D1 实现注（raft-log 编码 ≠ proto v2 SSOT）**：raft-log `Data` 字段编码（D1 用 `encoding/json`，D1 量级无性能顾虑、可读）**不属 proto v2 subject SSOT**——proto v2 只管 NATS subject grammar，不管 raft log wire。**`Args []any` over `encoding/json` 非 round-trip 类型稳定**（int→float64、`[]byte`→base64）：故 D1 `ClusterMetaSet` 把值烤成 **SQL 字面量**（不走 typed Args 回环），**D2 携参 op 必须用 leader 烤的 SQL 字面量或 typed/positional 编码**，不得把 JSON `[]any` 信封冻结为 D2 传参机制。

> **D2 实现定稿（先改正文 · 2026-06 · 详 `docs/reviews/d2-plan.md`）**：
> - **D2 in-scope op 集**（由 `internal/` 全量权威写 grep 推出，非照搬上列目录）：`SessionCreate/Tombstone/HardDelete`、**`MemberJoin`**（authcallout 的 `JoinWithPIN→AddMember` INSERT 是 live 写，原目录把它与 `Kick` 混淆而漏）、`NodeRegister`(identity-only)、**`NodeEvict`**（`adminsock handleEvict` 的 `DELETE nodes`+`DELETE agent_provisioning`，原 §5「NodeRegister/Remove」的 Remove 写者，survey 漏）、`ProcCreate/MarkExited/ReconcileBatch`、`PortAllocate/Free/Revoke`、`AgentProvision`、`ClusterMetaSet`(D1 接缝)。
> - **推迟（今天无 live writer，加了即死代码）**：`MemberKick`（members 唯一 DELETE 折进 HardDelete）、`PortReassignHome`(D6)、`RotatePin`（只有 NATS 权限串、无 `pin_hash` UPDATE 写者）、`Alert*`(D8)、`ClusterNode*`(D3)。
> - **arg 编码定为「全 leader 烤 SQL 字面量、D2 op 一律禁 `Statement.Args []any`」**（不选 typed Args，因 `int64` 过 JSON→float64 丢精度，`processes.start_time_ticks` 按精确相等比、损坏即破坏 PID-reuse 防御）。单一审计路 `internal/cluster/sqlbake.go`：`LitText`（`''`-doubling + 拒 NUL + **拒非 UTF-8**，唯一 text→SQL 路；非 UTF-8 会被 raft-log JSON 换 U+FFFD 静默腐蚀，故 fail-closed）/`LitInt`/**`LitTime`(=`t.String()`，不强制 UTC——调用方传与对应 live mutator 一致的 `time.Time`：port/proc/node/agentprov 传 `now.UTC()`、session.\* 传 raw `now`，见 §3.4)**/`LitNull`；`Args` 只留给惰性的 D1 `ClusterMetaSet`。`Command`/`Statement`/`commandVersion` 不变。

> **D4 实现注（`commandVersion` 1→2 + `ReqID` 启用）**：D4 给 `OpReconcileBatch` 加 apply-inert 有序审计元组字段（§4.1 自足重导），故 `commandVersion` 升 2（与 proto v2 解耦、只管 raft-log 信封）；`Command.ReqID`（信封 `r`）从 D1 的 RESERVED+INERT 转为**活跃跨重试幂等键**，`decodeCommand` 加 charset 守（空或 hex/有界、拒 NUL/非 UTF-8）。空 ReqID = 今天行为（D1/D2 op 字节不变、不碰 ledger）。详 §4.1 D4 实现定稿。

> **D5 实现注（新 op `OpAuditCheckpointSet`）**：D5 注册 `OpAuditCheckpointSet`（推进复制游标 `cluster_meta.audit_published_index`，FSM 烤单调守卫 UPSERT、reuse `sqlbake.LitInt`、禁 `Args`），经 `genericExecApplier`。**不是放松 `t:` 前缀守卫**（该守卫为 §2.10 防碰撞，`ApplyMetaSet` 仍只准 `t:` 键）——这是独立 op。**不需 migration**（沿用 0009 `cluster_meta`，KV 缺省读 0 同 `applied_index`）。该 op **派生零审计**、在 publisher skip-set。详 §6.3 D5 实现裁定 + `docs/reviews/d5-plan.md`。

---

## 6. NATS 集群层

### 6.1 拓扑与跨节点签名（升为硬要求）
扁平全网格 routes、一个 JS domain、cluster CA 签路由 mTLS。**生成的 conf 必须：共享 account = auth_callout Issuer，所有 broker nkey = 每台 AuthUsers**（broker B 应答 server A 上发起的请求，A 的 nats-server 才接受 B 的本地签名）——**§13.8 跨真 ≥2 节点 nats 集群测**（非内嵌单 server harness）：client 连 A、B 应答、连接被授权。

### 6.2 auth_callout：读本地、PIN 写转 leader（去 XKey）
queue-group `tether-authcallout`。已 provision（无 PIN）= **应答 broker 本地副本读 + §3.2 node fail-closed**，失 quorum 不锁死、不经 leader。
> **裁定（R2-major：XKey 不存在）**：**不引入 NATS XKey**（树里无任何 curve/XKey 设施）。**PIN-join 把请求经 broker-only subject 转 leader，该 subject 走 mTLS cluster routes（broker 间已加密）+ broker-only ACL**——PIN 只在受信 broker 间、加密链路上流动，符合"所有 broker 同等可信"。leader 验 PIN + 提议 provision entry，**allow 门控在 entry 提交后**；失 quorum PIN-join 正确失败。运维恢复永不经 callout（§10.6）。

> **"broker-only ACL" 可执行定义（RF1：发布者身份写实）**：route mTLS 只证明 nats-server 之间的 route，**不**证明本机某个 NATS client 是 broker——故 `cluster.apply.>`/`cluster.>` 的发布权限必须绑到一个**具体 NATS client 身份**：
> - **tetherd 用其 per-broker bus nkey（代码里 `AuthCallout.BrokerNkeySeed`，已是该 server `Options.AuthCallout.AuthUsers` 成员）连本地 NATS**；该 nkey 的权限由 `auth.PermissionsForBroker()` 定义。
> - **只有 broker nkey AuthUsers 获 `cluster.apply.>` + `cluster.>` 的 pub/sub**——即把 `cluster.apply.>`/`cluster.>` 加进 `PermissionsForBroker()`，且**仅** AuthUsers 列里的 broker nkey 持有；其余一律无。
> - **生成的 `nats-server.conf` 必须同时写两处（R3F3）**：`authorization.auth_callout.auth_users = [<每台 brokerPub>]`（回显应答用，§6.1）**且** 给该 broker nkey 配 **static `Nkeys`/`users` 条目，permissions = `PermissionsForBroker()`**（含 `cluster.apply.>`/`cluster.>`）——只配 auth_users 漏 static permissions 则 broker 自身连接拿不到该 subject 权限（现有代码注释即要求二者并存）。
> - **auth_callout 签发给 member/agent/ctl 的 user JWT（用 account 签名私钥铸）绝不含 `cluster.*` pub**；node-identity 不是 NATS client 身份（仅 Raft/route mTLS PoP）；route peer 是 nats-server 非 client。
> - 故威胁边界精确为：**能以 broker nkey 连本地 NATS 者方可 pub `cluster.apply.*`**——这与"所有 broker 同等可信"一致（broker nkey 本就是受信 broker 凭据），而普通 bus 连接（哪怕能连进本地 NATS）拿不到该 subject。
> - 验收（§13.8）：**普通 bus credential pub `cluster.apply.*` 被 NATS ACL 拒；broker nkey credential pub 被允许**；member/agent JWT pub/sub `cluster.*` 被拒。

> **D3 实现修正（PIN 写相边界 · 2026-06，详 d3-plan R3）**：D3 只交付**安全半边**——leader 本地 `Node.Propose`（提交后 allow）；follower/刚下台 leader 的 `raft.ErrNotLeader`/`ErrLeadershipLost` 由 **seam（broker/D9 构建，可 import raft）经 `cluster.IsNotLeader` 映射为 raft-free 的 `authcallout.ErrNotLeader` 哨兵**，handler 据此**分类为 transient 的 deny**（`not_leader`）——**handler 本身保持 raft-free**（架构 L-2：raft 仅在 `internal/cluster`）。**绝不假放行、绝不未复制直写**，全在 callout 边界由测试断言。**agent 客户端现今把任何 auth-deny 当终止性**（`internal/agent/agent.go:617` `isAuthFailure`，不重试），故 D3 **不**声称客户端透明恢复、**不**改 `isAuthFailure`。§13.8"返回可重试 deny"= deny **理由被分类为可重试**，非 D3 客户端行为。透明转发（follower→leader，客户端永不见 not_leader）+ 客户端/转发方重试 = **D4**（§4.1）。`nil` Propose seam ⇒ 生产保留今天直连 `ProvisionWithPIN(h.DB,…)`（零回归）。**conf 模板化 = tetherd 自带 `internal/natscluster` 渲染器**（`$SYS.REQ.USER.AUTH` 订阅改 `QueueSubscribe` 到 `tether-authcallout`）；install.sh/serve.go 在位 conf 接管 = D9 §11。

### 6.3 单写者权威消息 + 审计可重导
只有 leader post-commit 发 `ev.*`/`audit.*`/`sys.events`。
> **裁定（R2-major：跨 leader 死的审计恢复）**：**审计派生 = committed Raft entry + `raft_index` 的纯确定性函数**（任何新 leader 可从复制 entry 重导，**不依赖 leader-local 态**）；同 `raft_index:kind:seq` 去重 key 重发，JetStream 丢重复；**dedup 窗口配置值 > (选举+扫尾) 最大时延**并断言；选后 sweep 幂等可重跑（sweep 中再死可重跑）。源是 leader-local-only 的审计归 best-effort（§17）。**需求 §2.3 偏差登记：审计 0 丢失为"近似"（leader 提交后、JS 发布前崩有窗口，由可重导 sweep 兜底但非严格 exactly-once）。**

> **D5 实现裁定（先改正文 · 2026-06 · 详 `docs/reviews/d5-plan.md`）**：D5 = **build + prove，不切线上**（同 D2/D3/D4，cutover=D9；`serve.go` 字节不动、生产 `Broker` 不构造 `cluster.Node`、publisher loop 永不启动；live `publishAudit`/`reconcileOnRegister` 字节不变，guard `TestD5ProductionWiresNoClusterNode` 锁死）。
> - **可重导面 = `OpReconcileBatch` + `OpTransferAudit`**（D4 烤 reconcile 自足 entry + 纯 `proc.ReplayReconcileAudit`；**D8a 加 transfer audit**：`OpTransferAudit` 纯-Aux 空-Body、每 start/complete/failed 一 committed entry、纯 `xferaudit.ReplayTransferAudit` 从 Aux 重导、`q<reqID>:xfer` 去重、`reqID=hex(sha256(transfer_id:kind))` 经 0011 ledger 压跨选举重复）；`audit.call`(exec)、inline `audit.proc{start,exit}`、live `audit.port`、`sys.events` 仍 best-effort leader-local、**不重导**（§2.3/§17 诚实边界机械化）。
> - **发布游标 = 复制态、非 leader-local**：`cluster_meta` 键 `audit_published_index`，经**新 op `OpAuditCheckpointSet`**（`ApplyMetaSet` 的 `t:` 前缀守卫挡掉它）推进，**FSM 烤入单调守卫 SQL** `ON CONFLICT … WHERE CAST(value AS INTEGER) < CAST(excluded.value AS INTEGER)`（陈旧 ex-leader 的低值在每副本成 no-op），**批量推进**（一批一次 raft 写、上限 256）、**空闲零 raft 写**。游标 op 自身**不派生审计**（防 checkpoint 自激）。
> - **发布天花板 = `raft.CommitIndex()`**（v1.7.3 **确导出** `api.go:1234`——纠正任何"CommitIndex 不可用"措辞；不用 `AppliedIndex`：新选 leader 追平期 AppliedIndex 滞后 CommitIndex，徒增 sweep 延迟），**地板 = `max(checkpoint, LogFirstIndex())` 每轮重夹**；快照截断未发布审计 = **有界 loud accepted-loss**（typed `ErrLogTruncated`、计数+结构化日志、推进游标过 gap 不 wedge），把 §16/§17 的近似 0 丢失收紧为"以快照节律为界"，并断言 `CommitIndex - checkpoint < TrailingLogs`(10240)。
> - **去重 id = `r<raftIndex>:<kind>:<seq>`**（`kind∈{proc,port}`、D5 内封闭；`seq`=`ReplayReconcileAudit` 全序内 0 基位次，两 leader 解同字节得同 id）；**reqID-bearing reconcile 改 `q<reqID>:<kind>:<seq>`**——D4 ack-lost 重试经 `appliedDedup` 落新 raft index 却仍携 Aux，raft-index-keyed 会双发，改用跨重试稳定的 `ReconcileReqID` 键使原提交与去重重试发同 id、JS 收拢（**5 份草稿均漏、综合稿 R-10 抓出**）。
> - **单写者 = 跨选举同 id 收拢、非"只一个进程发"**；follower 不发（publisher loop 结构性只在 `internal/broker`、FSM 够不到=L-2）。**发布门 = `IsLeader() && !LeaderContactStale(now)`**（CommitIndex 天花板是承重安全、fence 是加固）；**不在审计体加 leader-epoch header**（破 build-and-prove 字节锁；消费端拒陈旧 leader 是 D7/D8）。
> - **loop 形态**：**一根长生命 broker-owned goroutine**（每 tick poll `IsLeader()`、非 leader no-op；**无 `OnLeaderChange`/无 per-flap spawn**，同 read.go 拒 `LeaderCh` goroutine 的家风）；per-publish ctx 是 loop ctx 的**子**（leadership 丢→cancel→即放 reply-inbox fd）。机制 A 与 B 合一根 goroutine（tick：门→`publishOnce` drain→`reconcileOnce` pass）。
> - **live `publishAudit` 字节不变**：D5 **不加** `publishAuditWithID` sibling 方法（MP-6 实现修正——独立 `AuditPublisher`（仅 test/d5 构造）**直接经自己的 JS client** 加 `jetstream.WithMsgID` 发布，"live 字节不变"反而成立更彻底）；流建时 `Duplicates=AuditDedupWindow`(2m) 无条件设但**生产惰性**（live 无 msg-id→去重不触发；预置 D9）。**`OpAuditCheckpointSet` 是 publisher 游标推进的硬跳过**（外审 F1）：游标绝不推过自己的 checkpoint entry（一次 advance 本身提交一条 checkpoint entry，若推过它会再生下一条→空闲 leader 每 tick 写 raft log；改由后续真源 entry 隐式带过，至多留 1 条尾随 checkpoint）。**D5↔D7/D8 边界**：D5 出 publisher+sweep+预测语原语，不出 `cluster status`/retire CLI（D7）、不写 `replication_degraded` alert 行（D8b）、不发数据面 rehome（D6）。

### 6.4 JetStream：退回 per-session 流（隔离白送）
> **裁定（R2-major：撤共享流）**：**退回今天的 per-session `history-<sid>` 流模型**——subject+stream 双隔离白送，成员 JWT 现成的 `CONSUMER.CREATE.history-<sid>` 作用域即读隔离，**无需 broker 中介 consumer、无需 per-subject 限额、无需改 FilterSubject 防偷读**。代价是每 session 一条流，在几十~上百量级可接受。`session rm` 仍 DeleteStream。
> **副本重配**：`history-<sid>` 与 `OBJ_xfer-<sid>` 均 `Replicas=replicasFor(nVoters)`；今天 `ensureStream` 拒 UpdateStream，故扩容/retire 时 leader 显式 `UpdateStream(Replicas)`（post-commit、重试、对每条流）。重配窗口内审计写**排队于 leader 不丢**（与 §2.3 对齐）；`cluster status` 显示**每流 actual/target 副本**、任一 actual<target 升 `replication_degraded`、**重配未完不放行 retire 原节点**。kill-during-expand 测试断言无审计丢失 + 有界写停顿。

> **D5 实现裁定（先改正文 · 2026-06 · 详 `docs/reviews/d5-plan.md`）**：
> - **`replicasFor(nVoters)` = `1→1, 2→2, 3→3, 封顶 3`**（架构 R≤3；N=2→R2 是唯一单调 1→2→3 且满足"副本数 ≤ 服务器数"、且使 kill-during-expand 在 2-voter 中途点 no-loss 可达——数据已在第 2 节点）。住 `internal/jsstream`（L-2：纯 `int→int`，**不 import `internal/cluster`**）。§17 登记诚实："RF2 = 读可存活、写零容错"。
> - **三族全重配**：`events`、`history-<sid>`、`OBJ_xfer-<sid>`（三处字面 `Replicas:1`：jsstream.go:74/103、transfer.go:202）；对象库经 `UpdateObjectStore`。`ensureStream` already-in-use 分支 = **只升不降**（`current<target→UpdateStream`；缩容是 D7 retire、带门）。
> - **target = `replicasFor(NumVoters())`（raft 投票集=权威意图）**，**非** `replicasFor(nJSPeers)`（混淆意图与就绪→JS-meta 落后投票入群时 target 算低、永不收敛）。**升配门 = 把 `UpdateStream`/`UpdateObjectStore` 拒绝分类为 `ErrMetaGroupNotReady`（`IsMetaGroupNotReady`）后重试——重试即门（MP-5 实现修正）**，**不是** `metaGroupCanHost` 前置门：R1 流的 `StreamInfo.Cluster` 恒列 0 peer（只反映本流 1 副本放置、不反映 meta 大小），前置门会永久挡住 R1→R3 升配。`StreamInfo.Cluster`/`ActualReplicas` **仅用于 `AllAtTarget` 的 actual 计数**（broker 是 JS client 无 `*server.Server`、不能 `JetStreamClusterPeers()`）。bootstrap：先扩 `events`。`actual = 1(leader) + Σ(Cluster.Replicas 中 Current && !Offline)`，滞后 peer 计为 not-actual（对 retire 门保守）。
> - **D5 只出 `AllAtTarget`/`Degraded` 预测语**（全集 `{events}∪{history-<sid>}∪{OBJ_xfer-<sid>}` 的**唯一权威 canonical 扫描**、list/info 出错 fail-closed、空集→false——§6.4 禁抽样）；**不写 `replication_degraded` alert 行（写者=D8b、store-backed 非 client-synthesized）**、**不出 `cluster status` CLI（D7 消费 `AllAtTarget` 作 retire 门）**。重配并发有界（`maxParallel=8`、pass 返回前 join）。**"排队不丢" = 发布游标不越过未 ACK 的发布**（§6.3 的游标不前进**即**本节的排队、不另设第二队列）。

### 6.5 agent/ctl 发现与归属（server_id 桥接）
NATS 种子 + `connect_urls` 发现 + 无限退避；`ClientAdvertise`=公网:443。
> **裁定（R2-blocker：server_id≠node_id）**：agent 在 `register.req`/heartbeat **自报其当前连接的 nats server_id（N… nuid）**；natscluster 模板化时给每台 nats-server **确定性命名**并写进 `cluster_nodes.nats_server_id`；leader 据 `server_id→cluster_nodes 行→node_id/tunnel_addr/cert_fp` 做**权威 home 指派**（agent 永不自选），经 `NodeRegisterResp.Home HomeDirective` 下发。初版规则：home=agent 当前所连 broker；首猜错由 §7.4 收敛。
>
> **D6 实现修正（先改正文 · 2026-06 · 详 `docs/reviews/d6-plan.md` DA-1/R-1/R-26）**：桥接键 = **确定性 `server_name`（agent 报 `nc.ConnectedServerName()` == `info.Name`）**，**非**易变 per-boot NUID（`nc.ConnectedServerId()` == `info.ID`）——上文"（N… nuid）"措辞**作废**：NUID 每次 nats-server 重启重生成，会在 D6 处理的"home 故障切换"事件本身上**断掉映射**。`internal/natscluster/config.go` 渲染的 `server_name`（如 `tether-1`）即 `cluster_nodes.nats_server_id` 的值（D7 首写）。`NodeRegisterReq.ServerID` 字段承载该 server_name（名沿用 §6.5）。**home 资格 = `phase=='VOTER'`**（其余 phase 不出 directive，§7.4 下次重连收敛）。**agent→server_name 绑定为复制态**：migration 0012 加可空列 `nodes.nats_server`，**live `node.Register` 与 FSM `OpNodeRegister` 双写**（保 D2 DIFF-1 一致），任一 broker / leader 可在 expose 时读以解析 home（`internal/clusternodes.LookupByNatsServer`，纯 SQL、无 nats/raft、守 L-2）。**生产 inert**（仅 `b.node != nil` 时 `homeDirectiveForExpose` 读它）。**build-and-prove**：生产 `serve.go` 不构造 `cluster.Node`、不发 `HomeDirective`、tunnel server 不给稳定 cert；全分布式数据面经多 broker + agent failover harness 直驱 `cluster.Node` 证之，cutover=D9。

---

## 7. 数据面（方案 B）

### 7.1–7.2 唯一映射 + 本地读 + epoch 防双绑
`port_allocations` 唯一权威映射（加 `home_broker/rebuild_on_failure/epoch`），partial-unique-index 保单写。`tunnelTokenLookup` = home broker 本地副本读 + `home_broker==self`。
> **裁定（R2）**：(a) **新瞬态 DENY `home_catching_up`**——leader 在拨号指令前已提交 `port.allocate`，落后副本对未应用行返回它（**不塌成 terminal**）；它是 **v2 wire 常量、broker 发 + agent `denyIsTransient` 同改**（agent 默认 terminal，不改则永久 brick），并加**有界重试**（永远应用不上则升告警，不无限重试）。(b) **防 ex-home 双绑**：`tunnelTokenLookup` 比对**本地副本的 allocation epoch vs agent 出示的 epoch**，本地 epoch 更高即 DENY——ReassignHome 在途时旧 home 未应用也不会双绑同一公网口。**wire 契约（R3 §18.4 升正文）**：REGISTER 加**第 6 个字段 `<epoch>`**（v2 wire break，parser **收正好 6 字段**否则拒）；`HomeDirective` 带 `epoch`；`tunnelTokenLookup` 新变体过滤 `home_broker==self` + port + 返回 epoch 比对；`home_catching_up` 双端同版本上线。(c) **catch-up 谓词 = 服务节点 follower-local `applied_index >= barrier index`**（R3 §18.4 升正文，非"ReadIndex barrier"含糊措辞）：leader 的 `VerifyLeader`/ReadIndex **只用来取一个新鲜 barrier 值**（AddVoter 提交后），新 home 自己比对本地 `applied_index >= 该 barrier` 才算就绪；未过前对 token 查找一律 `home_catching_up`。

> **D6 实现修正（先改正文 · 2026-06 · 详 `docs/reviews/d6-plan.md` DA-3/DA-6/DA-7/DA-12 + R-5..R-12）**：
> - **epoch SSOT（DA-3）**：`port_allocations.epoch` 是**每 port 单调计数器**——allocate 时 `0`（migration-0010 基线）、每次 `OpPortReassignHome` `+1`。**非 raft index、非 1-based**。
> - **`OpPortReassignHome`（R-5/R-7/R-8，command.go 已命名-deferred → 提为 live op）**：`PlanReassignHome` 在 `applyMu` 下读当前 epoch，烤**全字面** `UPDATE … SET home_broker=<lit>, epoch=<LitInt(cur+1)> WHERE port=<lit> AND state='ALLOCATED' AND epoch < <LitInt(cur+1)>`——leader 烤字面（非 `epoch=epoch+1` 列算术）+ 单调 CAS 守卫 `epoch < new` 令陈旧 ex-leader 的低 epoch 重指在每副本上确定性 `RowsAffected==0` no-op（ex-home 双绑的 FSM 层栅栏）。leader-driven backup 路**不带 reqID**（D4 ledger 要求 reqID 由发起 broker 铸、非 leader 铸；同 D4 provision/join "无键"，CAS 即幂等锚）。`PlanAllocate` 加 home 参数、**仅 home 非空时**烤 `home_broker/epoch`，现有调用方传 `("",0)` → 字节等价 INSERT；live `port.Allocate` 直连 mutator 字节不动。
> - **catch-up barrier 校正（R-12/DA-7，覆盖上文 (c) 的"applied_index >= barrier"措辞）**：实测 `fsm.go:78-80` `if l.Type != raft.LogCommand { return nil }`——config/noop entry **不推进 `applied_index`**，故 `applied_index`（command 域）与 raft `CommitIndex`（全 entry 域）**域不兼容**，"applied_index >= raft-commit-barrier" 谓词**错**（首个 post-genesis term 后永久 brick home）。**改 epoch-as-local-barrier**：新 home 比对 **agent 出示 epoch vs 自己本地已应用的 `port_allocations` 行 epoch**；`presented > local` → `home_catching_up`。**不**经 wire 传 raft index、读路**不**调 `VerifyLeader`。充分性（DA-7a）：`OpPortReassignHome(epoch=N)` 与 `applied_index` 同 FSM txn 推进，故本地行 `epoch>=N` 即等价"本副本已应用该重指"。leader 的 `VerifyLeaderRead` 仅用于**发 directive 时取一个新鲜 epoch**（"取新鲜 barrier 值"那半），本地行-epoch 比对是"比对自己本地 applied_index"那半。
> - **`tunnelTokenLookup` 二维 ladder（R-9）**：加 `epoch int64` 参数；**`home_broker==''` → 整条 ladder 跳过（inert、字节等价今天）**。否则按 `(home vs self) × (presented vs row.epoch)`：`presented < row` → **terminal** `token_unknown_or_revoked`（agent 持过期 directive、由更高 epoch 重指收敛）；`presented > row` → **transient** `home_catching_up`（本副本未应用最新重指，**无论 home==self 与否**）；`presented == row && home==self` → **allow**；`presented == row && home!=self` → **terminal**（真 ex-home/never-home）。**否决"home!=self 一律 terminal"**——会在 catch-up 期 brick 新 home（新 home 持旧行 `{home=A,epoch=N-1}`、`self=B`，见 home!=self 而 terminal 拒掉 agent 被指向的正是它）。"self" = `b.node.SelfID()`（`b.node!=nil` 时，否则 `""`，被 `home==''` 短路）。
> - **REGISTER 第 6 字段（DA-6/R-11，§7.2(b) 升实现）**：`REGISTER <sid> <nid> <port> <token> <epoch>`；`parseRegisterLine` 收**正好 6**、`strconv.ParseInt(_,10,64)`（非 `Atoi`），负/溢出/非整 → `malformed_register`；wire **不带 barrier**。N=1 epoch 恒 `0`。`home_catching_up` 经**共享 `const proto.ReasonHomeCatchingUp`**（broker 发 + agent `denyIsTransient` 双引同一符号，杜绝字面漂移 brick 车队）。
> - **初始 home 交付 = C1 致命缺口修（DA-12）**：实测初始 expose 路径 `handleExposeReq → port.Allocate → ExposeForwardedReq{Name,Port,LocalPort,Token,ActorFP}`（5 字段、**无 home**）→ agent，**根本不经 `NodeRegisterResp`**。故 `ExposeForwardedReq` 加 `Home *HomeDirective`（omitempty → N=1 字节等价）；`handleExposeReq` 据 agent 持久 server_name 绑定解析 home（DA-1/R-26）；`handleExposeForwarded` 持久进 `PortToken` 并据之 Open。rehome 经 §7.4。

### 7.3 选址与 happy path
`--on-broker <host>`（校验 ONLINE）/默认=§6.5 所连 broker/`--remote-port <P>`（P12）；`tether expose --rebuild/--no-rebuild`（默认 ON，Plan 写列）。happy path：Plan 选 port+token+home → Raft 提交 → leader 发 `ExposeForwardedReq{tunnel_addr,home_broker,cert_pins,epoch}` → agent 按 `cert_pins`(current/未过期 previous，§15 RF3) 钉证拨 home:7000 → home 绑口并 **ACK 绑定** → leader 才回 `ExposeResp`（无死 URL）。raw token：leader Plan 铸、只复制 hash、经 NATS authed 通道一次性给 agent、agent 在 home 出示、home 按本地行 hash 比对。

### 7.4 home 死的检测与重建（agent 自驱为主）
> **裁定（R2-blocker：rehome 触发解耦）**：home broker 死时其**同机 nats-server 一并死**，故 **agent 的 NATS 连接弹到别节点、`onNATSReconnect` 触发**——**让 agent 自己的 NATS 重连（及任何带更高 epoch HomeDirective 的 register 回包）直接驱动隧道 rehome**：比对 directive epoch vs clientSession 当前 home epoch，调 `Open(newAddr)` 原子替换（旧 supervisor 经 Open-replace cancel 退出——不在 `redialWithBackoff` 里死盯旧 addr）。**leader 侧 broker-死检测（Raft peer + route 健康）+ 推 RehomeDirective 作 BACKUP**（home 死但 agent 的 nats server 还活的少见情形）。
> **限流与就绪对齐**：RehomeDirective **携带其 `ReassignHome` entry 的 `raft_index`**，新 home `applied_index>=该 index` 前回 `home_catching_up`（确定性就绪，非猜）；§7.4 的 K/秒与 agent 退避**绑定**，慢 drip 不被反超。无活 home 时保持 ALLOCATED+info 告警+重试。rebuild OFF=`port.free`+expose-rm+info。`ps`/`status` 显示每 expose 的 home + 末次 rehome；`broker_down` 告警带 ok/failed 计数。

### 7.5–7.7 撤销 / per-expose addr / cert 钉证
撤销权威 fence=`home_broker` 列 + agent 关 yamux；evict 推送 best-effort。**撤销对已连接生效要主动断连**（§10.1）。**per-expose (per-publicPort) brokerAddr** 重构（§18.2.2，非 per-session——同一 agent 的多个 expose 可落在不同 home broker）须改 `Open/AddProxy`(收 addr)、`clientSession`(按 publicPort 键存 addr)、**`dialAndRegister`/`redialWithBackoff`/`swapTransport`(从 session 读 `sess.brokerAddr`、非 `Client.brokerAddr`)**、adapter 支持一个 Client 并发扇出 N 个 home；不变式：每个 expose 的 home addr 一个 generation 内不可变、只经新 Open 换。并发测试（一 agent N session M home failover + mass-rehome 风暴，`-race`+`goleak`，断言有界 re-REGISTER、rehome 后瞬断重拨**新** addr、提交于一个选举窗口内的行不出 terminal DENY）。**cert 钉证为 v2 硬要求**：agent 验 home TLS cert ∈ `HomeDirective.cert_pins`（接受 `current` 或未过 `valid_until` 的 `previous`；源自 Raft 复制的 `cluster_nodes.cert_fp[_prev]`，经 authed NATS 下发），都不匹配则拒拨、raw token 不出示；§16 删"仍 InsecureSkipVerify"矛盾措辞、只限 N=1 退路。**pin 必须来自 §15 的稳定持久 `tunnel-cert.pem`（非每次 Start 自签），写入/轮换经 Raft entry + wire-list 双 pin 窗口（agent 无状态）——完整生命周期契约见 §15 RF3、§18.2.11**；否则重启换 fp → 老 agent 拒连合法重启 home（自伤）。

> **D6 实现修正（先改正文 · 2026-06 · 详 `docs/reviews/d6-plan.md` DA-8/DA-9/DA-10 + R-13..R-24）**：
> - **per-expose brokerAddr（DA-9/R-13）**：`brokerAddr/epoch/certPins` 移到 `clientSession`（按 publicPort 键）；**保留 `Client.brokerAddr` 作 N=1 fallback**（session addr 空时用）。`Open(publicPort,localPort int, token,brokerAddr string, epoch int64, certPins CertPins)`。**supervisor 必须把这些作 `go c.supervise(...)` spawn 时的值参快照接收，绝不在 loop 内从 `c.sessions[port]` 回读**——token 的无竞态正源自"spawn 值参、单 goroutine 读"；loop 内读共享 map 是对 `Open`-replace 写的无同步读 → 硬 `-race` 失败（必带 `-race` 单测锁死）。
> - **自驱 rehome（DA-8/R-14/R-15/R-17）**：rehome `Open` **并发**跑（有界 worker、每 expose 一 goroutine；串行 = N×拨号超时炸 RTO），`Open`-replace **先 `old.cancel()`** 旧 supervisor 即停盯死 addr。rehome 首拨返 `home_catching_up`（首 `Open` 在任何 supervisor 存在前返错、supervisor 重试 loop 看不到它）→ `applyReconciliation` 分类并以同 full-jitter 退避**重排**（非改 `Open` 契约），max-wait 后**记 `catch_up_stalled` 日志**（alert 行属 D8b）、永不塌 terminal。rehome 同骑 `onNATSReconnect` 再注册回包 + expose-forward 路（DA-12），leader 推 `RehomeDirective`（**复用 agent-only forwarded 通道、不新增 subject、绝不上 sys.events**）作 backup；全路 epoch-ordered（`directive.Epoch > clientSession.epoch` 才 apply）幂等。`home_catching_up` transient 本身即天然背压（落后新 home 把惊群摊到退避 loop）。**每口仅一条重试循环**（review A4 B1：重连风暴下按口去重、goroutine 由口数界定，非重连数）。
> **有界 cutover 窗口（诚实 · review L-1）**：ladder 只 gate **新 REGISTER**，不重检**已绑定**的 OS listener。故"恰一绑/catch-up 期零放行"是 **ladder 层**性质，非 listener 层不变式：若 agent↔ex-home(A) 的 yamux 因无关 blip 在重指到达**前**断开，旧 supervisor 以**旧 epoch** 重拨 A，A 若尚未应用重指（行仍 `{home=A,epoch=N-1}`）则 ALLOW 并**重绑公网口**——直到 B 追平 + `OpenHome(B)` cancel 掉该口 supervisor（关 A 会话）。两 host 短暂双 listen，**在途已断**（rehome 是硬 cutover），窗口由指令投递 + ex-home 应用延迟界定、**自愈**；CAS(R-7) 栅栏的是 DB 写、非 A 的活 listener。**权威 ex-home listener 杀**（leader 重指时推 `CloseProxy`）属 **D7 leader-push**（需 broker-死检测）。D6 文档化 + 测试该有界自愈窗口。
> - **cert 钉证 + 轮换窗口（DA-10/R-19..R-24）**：fp SSOT = `tunnel.CertFingerprint(*x509.Certificate)` = `"sha256:"+hex(SHA-256(cert.Raw))`（leaf DER，**非 SPKI**），harness 播种 `cluster_nodes.cert_fp` 与 agent 验证**共用同一函数**。agent 验证用 **`VerifyConnection`**（TLS1.3 resumption 安全，**非 `VerifyPeerCertificate`**——后者 resumption 不跑）：空 pins → `InsecureSkipVerify`（**唯一** N=1 退路、字节等价今天）；非空 pins → `InsecureSkipVerify:true` + `VerifyConnection` 回调（空 `PeerCertificates`=拒；`fp==current` 或 `previous!="" && valid_until!=nil && now<*valid_until && fp==previous` 才收，否则拒）——握手在写 REGISTER token **之前**失败，MITM 拿不到 bearer token。**clustered home 无 InsecureSkipVerify 路**（R-22）：有 `HomeBrokerAddr` 但暂无 pins（如 state.json 回放，pins 不持久）→ **延迟拨号**直到 register/expose 回包带来 pins。`CertPins{Current,Previous,ValidUntil *time.Time}`（指针 time 区分 NULL 与零时）。稳定 cert tunnel server 经 **harness-only** `tunnel.NewServerWithCert`（生产仍 `NewServer` nil-cert 临时签）；轮换窗口经 harness **重启**换 cert 证（无 live hot-swap，操作命令属 D7）。**build-and-prove guard** 禁 `NewServerWithCert(`/`LoadServerCert(`/`HomeDirective{`/`PlanReassignHome(` 等出现在生产 `serve.go`+`internal/broker/*.go`（除 `home.go`）+`internal/agent/*.go`。

---

## 8. 生命周期与 `tether cluster`

### 8.1 命令面 + 传输（admin 严格本地）
> **裁定（R2-major：解矛盾）**：**两条路分清**——
> - **session 控制写**（follower→leader）：走 §4.1 `apply.*` 总线，鉴权 = **route mTLS 客户端身份 + broker-only pub ACL**（无转发签名 / role / term——§18.1：转发是一致性而非安全边界）。
> - **admin 变更**（cluster add/remove/drain/retire/force-single）：**严格本地 admin socket，无任何网络旁路**（adminsock 本就只 unix）。在非 leader 上发起的变更 **fail-fast 指名应去哪台 leader host 重跑**（运维本就有 shell，§10.6）。

| 命令 | 何处 | 危险/确认 |
|---|---|---|
| `cluster init [--from-existing]` / `add <host> <node-pub>` / `remove <node_id>` | 本地 | add/remove 走 quorum 投影；remove TTY+typed node_id |
| `cluster status`/`doctor`（**+`--json`+退出码契约**） | 本地或 NATS | — |
| `cluster transfer-leader <node_id>`（**并入 promote/step-down**，§16） | 本地 | — |
| `cluster drain <node_id> [--retire\|--now\|--abort]` | 本地 | quorum 投影；retire 跌破 HA / plain-drain-at-N=2 typed 确认（F==0 拒 `--yes`）|
| `cluster rotate-tunnel-cert <node_id>`（§15 RF3、走 `OpClusterNodeUpsert` 写 cert_fp 三列、非新 admission） | 本地 | — |
| `cluster node-pub`/`keygen` | 本地 | 打印本机 node-identity 公钥（带 nkey 前缀，防 fat-finger）|
| `cluster sign-join <node-id> <nonce>`（入群机本地、用 node-ident 私钥签 leader 发的 nonce，打印 paste-token） | **入群机本地** | 无 NATS 旁路；token 贴回 `cluster add … --join-token`（签名绑 node_id+pub+nonce，故签时须给 node_id）|
| `force-single` / `recover [--dump-divergent]` | **本地 offline 子命令**（见 §8.4） | TTY+typed node_id，永不认 -y |
| `alert ls/ack` | **NATS（所有人）** | 集群级 ack（§18.3）；`ls` 显示 acked_by |
| `alert raise/clear` | 本地 | — |

**membership flow = 两层、两阶段（R3F1：roster admission FSM op 与 Raft config 变更分离）**：`hashicorp/raft` 的 `AddVoter`/`RemoveServer` 是 **Raft 配置变更路径，不是业务 `FSM.Apply` 能拦截/否决的 `LogCommand`**——故"follower 在 FSM Apply 对 AddVoter 本身密码学 veto"不可实现。join PoP 只能挂在**业务 roster op** 上。定两阶段：
> - **阶段 1 — roster admission（业务 FSM op，可复制校验）**：`OpClusterNodeUpsert{node_id, node_ident_pub, join_nonce, join_sig, cert_fp, raft_addr, nats_route, ...}`。leader 发 nonce、入群 daemon 用 node-identity 私钥签、leader 对运维**带外键入**的 pubkey 验签后才提议；**followers 在 `Apply(OpClusterNodeUpsert)` 复算 join PoP**，验不过则 roster 不落库（这是真·跨节点可验证 proof）。
>   - **签名消息绑定（D7 实现定稿）**：canonical 域分隔元组 `"tether-cluster-join-v2\0" || node_id || "\0" || node_ident_pub || "\0" || join_nonce`（**不绑 cluster-id**——fresh leader-issued nonce 已防跨集群重放）。验签输入 `{node_id, node_ident_pub, join_nonce, join_sig}` 同时走 `cmd.Aux`（apply-inert JSON、applier 不能从烤好的 SQL 字面反推字节）**且**落 `cluster_nodes` 列（过快照截断、retry 重建）；一道 **Aux-vs-Body 交叉校验**（Aux 值 == 烤入列字面）防 leader 拼接错配。
>   - **它是唯一不走 `genericExecApplier` 的自定义 applier**（验签 = `internal/auth.VerifySignature`，纯 ed25519、确定性、cycle-free 无 nats.go，`internal/cluster` 直接 import、可达 `FSM.Apply`；§13.1 白名单此例 + 负向对照：错验签必在每副本同样 poison-跳过）。
>   - **验签失败裁决 = POISON 跳过**（推进 `applied_index`、不执行 roster UPSERT、loud log），**绝不返回 applier 错误**（§3.7 重试→panic 会把沦陷/有 bug 的 leader 提交的一条伪签条目变成全集群每次启动 log 重放即 panic 的远程 brick——§2.8 never-wedge 不变式禁止）。验签是确定性纯函数（固定 nkey 库 + canonical 消息），全副本同判，无"部分接受/部分拒绝"的分叉风险。落地经新增 `errAppliedRejected` 哨兵：`applyCommand` 在重试循环**前** `errors.Is` 它、走 `appliedPoison` 同款路径（提交 applied_index 推进、零 op、不 panic）。
> - **阶段 2 — Raft config 变更（非 FSM proof 点）**：**仅当阶段 1 的 `ClusterNodeUpsert` committed 后**，leader 才调 `raft.AddVoter(node_id, raft_addr)`。**明示此步不是 FSM 校验点**——它是底层 Raft config entry，靠"阶段 1 已把该 node_id 的 PoP 验证并复制进 roster"间接受信。
> - **半成功状态机（`phase` 列承载）**：`JOIN_VERIFIED_PENDING_VOTER`（阶段 1 过、阶段 2 未发/失败）→ `CATCHING_UP`（AddVoter 成功、未过 §8.2 catch-up）→ `VOTER`（健康）；失败分支 `VOTER_ADD_FAILED`。`status/doctor` 显式渲染，绝不出现"DB 认为 voter、Raft config 不是 voter"的静默分叉。
> - **leader-startup membership reconciliation pass（D7 实现定稿——no-silent-fork 的真正保证；status 渲染只是展示、非安全属性）**：新 leader 上任后、服务任何成员命令前，对每个 `cluster_nodes` 行**幂等调和** roster `phase` 与活 `raft.GetConfiguration()`（据 committed phase 列 + 活 config，**绝不**据 in-memory leader 态，故选举切换零进度丢失）：`{PENDING ∧ raft-voter}`（AddVoter 提交后旧 leader 崩在 phase 推进前）→ 前向补 `CATCHING_UP`、**绝不** RemoveServer；`{VOTER_ADD_FAILED ∧ raft-voter}`（AddVoter 超时≠失败、config entry 实已提交——D4 committed-but-ack-lost 同类歧义）→ 重查 config，实为 voter 则推 `CATCHING_UP`；`{roster=∅ ∧ raft-voter}` → loud `INCONSISTENT`、拒自动动作、指 `cluster doctor`。提交序（阶段 1 commit 严格先于阶段 2 AddVoter）令 `{phase=VOTER ∧ 非 voter}` 不可达；危险的崩后态由本 pass 闭合。
> - **重试幂等**：阶段 2 失败重试**复用同一 committed `ClusterNodeUpsert`（PoP 已验、nonce 已消费、不重签）**；`raft.AddVoter` 按 node_id 幂等（已是 voter 即 no-op）。永不追平节点用 §8.3 的"加个永不追平节点再干净移除"清理路径。
> - **Remove/retire 顺序**：**先写 roster `ClusterNodePhase`(`DRAINING`/`RETIRING`，§4.2 canonical 大写枚举) 再 `raft.RemoveServer`，最后 `ClusterNodeRemove`**；任一步失败 `status/doctor` 显示停在哪个 phase + 下一步命令。
> - **诚实威胁边界**：沦陷 *leader* 仍可提任意成员变更（§18.2.4 已接受）；沦陷 *follower* 自合成 `ClusterNodeUpsert` 在 §6.2 RF1 ACL 层（pub 不到 `cluster.*`）+ join PoP 验签层被双重拒。admin socket 仅 leader-local admission（决定 leader 是否**提议**阶段 1），**FSM 不声称校验任何不可复制的 origin proof**。
> - **测试门（§13.11）**：阶段 1 committed 后 AddVoter 失败 / AddVoter 成功后 catch-up stalled / 重复 add 同 node_id——断言状态机幂等、无静默分叉。

### 8.2 catch-up 闸
pin `TrailingLogs`/snapshot 阈值；谓词 = **follower-local `applied_index >= barrier index`**（§7.2c；leader ReadIndex 仅取新鲜 barrier 值，服务节点本地比对）+ 作为非滞后 voter 持续固定墙钟时长，带 max-wait fallback 升 `catch_up_stalled`（不饿死）。只护数据面 token 查找(§7.2)与 ClientAdvertise（auth 读已本地，§6.2）。
> **D7 实现定稿（catch-up barrier 域 = command-domain；待外审签字）**：barrier = leader 在 `AddVoter` 时经 `VerifyLeaderRead` 取的 **`AppliedIndex()`**（= `cluster_meta.applied_index` 命令游标），新 voter 追上 = 其**本地** `AppliedIndex() >= barrier`，持续非滞后固定墙钟、max-wait→`catch_up_stalled`。**绝非 raft `CommitIndex`**——config/noop entry 不经 `FSM.Apply`，命令游标永远到不了 all-entry barrier、每次 join 都会 hang。两侧同处命令域（HA "0 已提交业务丢失"宣称即关于业务态），与 D6 `epoch-as-local-row-epoch`（§7.2c）同一域选择先例。`catch_up_stalled` 是**派生展示态、非第 7 个 phase**（0008 `phase` CHECK 恰 6 值、写第 7 值会 CHECK 失败→applier 错→panic）：保持 `phase=CATCHING_UP`，停滞由 `phase_changed_at` + max-wait 派生、细节入 `voter_add_error` 列。

### 8.3 create/online/drain/retire（drain 对齐需求 §6.1）
create=装二进制 + **生成/分发本机全套 secrets：`account.nk`(共享，可信信道拷) + `broker.nk`(本机 NATS bus 身份，§6.2 RF1) + `node-ident.nk`(本机) + `tunnel-cert.pem`/`tunnel-key.pem`(本机稳定，§15 RF3) + cluster CA**（R3F3：与 §15 文件布局/preflight 对齐，缺任一 preflight 红）。`cluster add`（运维键入新节点 node-identity 公钥，`cluster node-pub` 打印之）= **§8.1 两阶段**：阶段 1 `ClusterNodeUpsert`(带 join PoP) committed → 阶段 2 `raft.AddVoter`。online=`phase` 经 `JOIN_VERIFIED_PENDING_VOTER`→`CATCHING_UP`→`VOTER` 且 JS 副本达 target 才 HEALTHY-HA。**drain（重写）**：① 升 `broker_draining` 带 `drain_deadline`；② **停服前主动迁 expose**（rebuild-ON 走 §7.4、OFF teardown+notify）；③ leader 先 `transfer-leader`；④ `--retire` 则 §8.1 顺序（先 roster `ClusterNodePhase` 再 `raft.RemoveServer` 再 `ClusterNodeRemove`）。**drain/retire 共用 quorum 投影守卫**：显示"操作后 N voters quorum=K 容 F 故障"；**F 为 0（含已处于 N=2 时再 drain）即 TTY+typed 确认 + 升持久 severe**。`cluster add` 半失败（阶段 1 committed 但阶段 2 `VOTER_ADD_FAILED`/`catch_up_stalled`）有文档化清理命令 + "加个永不追平的节点再干净移除"测试。
> **retire 身份吊销如实**（R2-major）：roster/Raft 移除即时；**account-key 信任不撤销**（共享、不轮换）——retired 但留有 `account.nk` 的节点在 TTL 窗内仍可签 JWT。retire-after-疑似沦陷**须配 account.nk 轮换 runbook**；§16/§17 登记此 gap 待外审签字。

### 8.4 force-single（offline 子命令 + 自我 fence + runbook）
> **裁定（R2-blocker×2 + R3 §18.2.5/.6/.7 已回写）**：(a) **"自我观测"fence**——节点 `自上次成功 leader 接触 > T_fence`（一处、一个单调时钟，§3.2/§6.2 复用同一谓词）即**凭自身观测停止接受写**（不依赖听到更高 term），堵无接触分区（native term fencing 只在能互通节点间生效）。**不是反脑裂结构性保证**：若 B 仅被分区但活着，force-single **可能**双写。(b) **force-single/recover = daemon 停机下的 offline 子命令**，直接操作磁盘 `raft/`+`tether.db`，**不经 admin socket**（停机后 socket 已没），**与 daemon 共享同一把磁盘 advisory lock** 防两实例并发动磁盘。
> **D7 实现定稿（disk 互锁 = 两机制；"与 daemon 共享同一把锁"在 D7 不成立）**：D7 生产 daemon 不构造 `cluster.Node`、不持任何 `tether.lock`（daemon 端取锁是 D9 cutover）。故 offline 工具 (a) 取 `flock(2)` `${DataDir}/tether.lock`（防两 offline 实例并发，进程退出自动释放、对 kill-9 的 daemon 正确）**且** (b) **探测 `raft/raft.db` 的 BoltDB 排他锁**（活 daemon 开 BoltStore 即持有它）——不可排他打开 → HARD-REFUSE "daemon 仍在运行；先 `systemctl mask` 并停它"。`SetMaxOpenConns(1)` 仅进程内串行化、零跨进程保护。`internal/clusteroffline` 只做编排（flock + (b)(c)(d) 前置 + peer TCP 探测 + dump-divergent），调 `internal/cluster.RecoverSingleNode`（raft + FSM 接线住 `internal/cluster`、**raft 不外泄**）。

**`T_fence` 数值（pin）**：`electionTimeout = 1000ms`（hashicorp/raft 默认，随 §3.1 pin 的版本核对）；`T_fence = k_fence × electionTimeout`，**`k_fence = 10` → `T_fence = 10s`**。两条约束（§18.2.6/.13）：① `T_fence > 最坏 PreVote 选举`（~2–3s，故 10s 不误触发）；② **`T_fence ≪ runbook step1 的最小墙钟时长`**（人类发现告警→SSH→status→mask→确认 peer 死，分钟级 ≫ 10s），保证被分区旧侧在任何运维能 force-single 另一侧**之前**早已只读。auth_callout fail-closed（§6.2）与正确性敏感读（§3.2）用同一 `T_fence`。
> **D3 实现修正（raftConfig 多节点 · 2026-06，详 d3-plan R6）**：D3 把 `raftConfig` 从 D1 的亚秒值调到多节点 `Heartbeat=Election=1000ms, LeaderLeaseTimeout=500ms`（raft 校验 `Lease≤Heartbeat≤Election`，`config.go:369/372`；**Lease 取 500ms 非 1000ms**——更长租约会**加宽** §3.2/§8.4 的 fail-open 窗口）。三超时经 `cluster.Config` knob 参数化，使 D1/D2 的 **N=1** 测试仍用快值；`TFence=10s` 解耦为独立常量 + 不变式测试钉 `TFence ≥ k_fence(10)×ElectionTimeout(1000ms)`（生产常量），防 test-only 缩短漂移。**静态多节点 bootstrap 属 D3**（test-only `Config.BootstrapPeers`），**动态 AddVoter/join-PoP 属 D7**。改 raftConfig 后**重跑 D1 kill-9 + D2 DIFF-1/TestD2Matrix 全绿是 D3 实现的阻塞前置门**。

**RUNBOOK（可复制，offline 模型；§18.2.5/.7）**：
```
0. systemctl mask tether            # 防 systemd 在你动磁盘期间自动重启 daemon、与 offline 工具争 advisory lock
1. tether cluster status            # 纯本地：离线读磁盘 raft 配置 + 对每 peer 直接 Raft-transport ping（broker 在私网内）
   #  打印每 peer "我测得已 UNREACHABLE 47s（展示阈值=120s，仅供参考）" + 将抛弃的 node_id
   #  *安全闸不是这个计时* —— 见 step2 的 hard-refuse
   #  死 vs 分区核对清单：确认其余节点已断电 / 对 agent 不可达，不只是"对本机不可达"
2. tether cluster force-single --confirm-peers-dead <abandoned_node_id...>
   #  daemon 仍停机；TTY 键入本机 node_id 确认（无 -y）。执行前硬前置：
   #   (b) raft/ 或 tether.db 缺失/空（raft.HasExistingState==false 或无 cluster_meta.applied_index）→ 拒绝
   #   (c) 调和两存储 + 改写 config = raft.RecoverCluster({self}) 自身驱动：它把本地 BoltDB log
   #       [snapshotIndex+1 .. LastIndex()] 经 fsm.Apply（幂等、同 txn 推进 applied_index）前向重放进
   #       SQLite，再写单成员快照 + config。恢复点 = 本地 LastIndex()（commitIndex 离线不可得、从不落
   #       BoltStore；未提交尾部按既成事实提交、loud log）。绝不手搓 ExecCommand 前置重放（会双应用非幂等 op）
   #   (d) --confirm-peers-dead 须列 disk roster 每个非 self 节点（漏一个那节点会脑裂即拒）；对每个 raft_addr
   #       net.DialTimeout 完成 TCP 连接即 HARD-REFUSE（peer 接受 TCP=活着，即便随后 TLS 因证书轮换失败）
3. systemctl unmask tether && systemctl start tether   # 以单投票成员起；升 force_single_active 持久告警
4. 恢复多节点：每个回归 peer 先 wipe：
   tether cluster recover --dump-divergent <file>   # 0600；键入被 wipe 的 node_id 确认；
   #  打印发散摘要（"N 行 sessions/ports/audit 仅存于此节点、将永久丢弃、已存 <file> 仅供取证不可自动合并"）；
   #  dump 写失败则拒 wipe。再 wipe-and-rejoin（cluster add）
```
`status` 实测的 peer-unreachable 秒数仅供展示（默认展示阈值 120s，**非** force-single 依据）；真正的安全闸是 step2 的 (b)(c)(d) 硬前置 + `--confirm-peers-dead`。`status`/`force-single`/`recover` 属 **no-leader-safe 子集**（纯本地磁盘、不重定向 leader），每条 severe banner 的下一步只引用该子集（模拟 quorum 丢失下测试）。runbook 须在 3 节点实跑演练过外审。

> **post-D9 增量 — ONLINE in-process force-single（首选路径，offline 仍是 floor）**：上面的 offline 模型要求**先停 daemon** 才能 `raft.RecoverCluster`——即"用一次新停机换恢复"，且无法在健康集群上演练。新增 `force-single --online` 在 RUNNING broker 内**只热交换 raft 实例**（`cluster.Node.RecoverToSelfOnline`：`atomic.Pointer[raft.Raft]` 下 `old.Shutdown()`→对**活的**两存储 `RecoverCluster({self})`→重建 mTLS transport→`NewRaft` fresh-FSM→**全成功才 atomic Store**；任一步失败保留旧 Shutdown 实例=只读不 brick，offline 即 floor），进程不停、读不断。经 **LOCAL root-only admin socket** 的两步 **arm→commit**，闸：① 持续 quorum-loss **dwell**（`forceSingleDwell≈15s`，防选举抖动误触）② 与 offline **同一** `CheckPeersDead`（raft/nats/tunnel 三端口 TCP 探活 HARD-REFUSE，commit 再探一次抓 arm 窗口内复活的 peer）③ 单发 broker-minted **arm token**（fail-closed：CSPRNG 不可用即拒，不回落可预测 token）④ CLI 不变的 **TTY 输 node_id** 确认（`--self-id` **发给 broker 校验** == socket 所属节点，不符即拒，防误指错节点；`--yes` 永拒）。`--dry-run` 在健康集群上零改动演练。commit 成功边界**含** `force_single_active` marker + recovery `epoch` 的 raft 持久化（任一失败返回 loud 错误并指明重跑，非静默成功——marker 缺失=可写却无可见 emergency）。recovery `epoch` 已持久化作未来跨节点 split-brain DETECTOR 的 durable input；**DETECTOR 本体（跨节点比对+SEVERE 告警）暂缩范围**为后续增量（在线版安全性 ≥ offline floor——offline 本就无 detector，且 total-partition 残留风险已被 commit 前 re-probe 的 peer-liveness HARD-REFUSE 大幅收窄）。

---

## 9. 文件传输（分布式，补全为真 §9）

> **裁定（R2-blocker：transfer 不是 stub）**：
> - **终结于 agent 的 home broker**（与方案 B 一致）。**数据面 push/pull 经 broadcast-SUB + home-keyed gate**（D8 实现修正）：`push.req`/`pull.req`/`push-commit.req`/`ev.transfer.*`/`finalize.req` 是 plain `nc.Subscribe`（`broker.go:532` 订阅表），集群下每 broker 都收每份。**START 路径**（`push.req`/`pull.req`）经 `transferHomeGate(sid,nid)` 门住：`home==self` 才处理、否则**静默不应答**（home 处理）；home 未解析/不合格亦静默（ctl 超时 backoff 重试，绑定收敛后命中真 home）。**continuation/terminal 路径**（`push-commit.req`/`ev.transfer.*`/`finalize.req`）按 **tracker 持有**路由——只有持该 transfer_id tracker entry 的源 broker（START 时的 home）应答（`transfers.get`/`claimFinalize`，余静默）。**无需 home-epoch 围栏**：每次 push/pull 调用铸新 transfer_id、对象按 transfer_id 键、tracker entry 只在单个源 broker，故并发 rehome 不产生跨 broker 双绑（旧 home 只 finalize/删自己 tid 的对象=正确；rehome 后是新 tid 走新 home）——与 §16(f) 一致。**不经 §4.1 转发数据面**（broadcast SUB 已送达 home、多一跳浪费）；**仅 transfer audit 行经 §4.1 leader Apply**。
> - **transfer audit 可重导**（D8 实现修正，与 §4.4/§6.3 一致）：start/complete/failed 经新 op `OpTransferAudit`（纯-Aux 空-Body、每事件一 committed entry、publisher 从 Aux 重导同 `OpReconcileBatch`）；**跨重试幂等 = 派生 `reqID=hex(sha256("xferaudit:"||transfer_id||":"||kind))` 经 0011 ledger**（JS `Duplicates` 窗口有限、不可作跨选举主锚）、去重 id `q<reqID>:xfer`。**start/complete/failed 全经 leader Apply、但 start 异步不阻塞 agent-forward**（保 audit-pair 完整、不给数据面加 raft 延迟）。**矛盾终态由 `claimFinalize` 单-broker 终态原子认领 + home-gate 单写 + per-invocation transfer_id 化解**（无需 terminal-state guard/新表）。
> - **tier-B ObjectStore：`ensureXferBucket` 的 `Replicas` 从字面 1 改 `replicasFor(nVoters)`**；`OBJ_xfer-<sid>` 与 audit 流同样在扩容/retire 时重配、计入 `cluster status` actual/target、受 `replication_degraded` 与"未重配完不 retire"约束。完成对象 N≥3 下 home 死可存活。**retire 门 xfer 副本枚举读 JetStream 实际 `OBJ_xfer-*` 流列表（`js.ListStreams`），非 DB `ListSIDs`**（D8 修正：bucket 可活过 session 行 → purged-session 孤儿不可见 → false-green 丢数据）；retire readiness 用**只读 `ObserveReplicas()`**、非升配副作用的 `ReconcileOnce()`。
> - **D5 实现裁定**：`ensureXferBucket(…, targetReplicas)` 经 `UpdateObjectStore` 只升不降、计入 §6.4 的 `AllAtTarget` 全集（否则 retire 门对未冗余 tier-B false-green）。**no-audit-loss-under-kill 证明锁定 `history-<sid>`**（审计承载流）；OBJ_xfer 在 D5 只证"扩到 target"（E-B8）+ 预测语纳入（E-B4），tier-B no-loss-under-kill 出 D5（OQ-6）。
> - **in-flight tracker + watchdog 在 home broker、best-effort**：home 死则在途丢失、客户端重启；**rehome 不保在途传输**（重启）。**boot orphan reaper（`reconcileXferObjectsOnBoot` 调 `store.Delete`）在 clustered 模式按 home-ownership 过滤**（D8 修正：共享复制 bucket 里别 broker 的 live 在途对象不是孤儿；仅收 home==self 且 committed `OpTransferAudit` 终态的 session 对象，永死 home 孤儿由新 home 接管后收）。生产 `selfID==""` 字节等价。
> - 无 multipath / 最快链路（需求 §1.3 推后）。
> - 门：tier-B 对象在 N≥3 杀 home 后存活；home 死时在途重启。

---

## 10. 告警系统

### 10.1 存储 / ack（简化）
leader 权威 Raft 复制 `alerts`/`alert_acks`，**单一集群级 ack（§18.3，非 per-identity；删 snooze_until/session_nonce/identity_fp）**：一条 alert 一个 ack，对全集群生效，`acked_by` 记录"谁 ack"仅供展示。**ack 只压抑内联 ack 提示、不压 banner**；**严重告警每新会话必重现**（ack 时打印"将于新会话重现"让运维不困惑）。`alert ls` 显示**每条 acked_by + 何时**（集群级，不是 per-identity 各自 ack）。
> **两条**严重 gating 告警由客户端合成（§10.4，quorum 丢时无法 Raft 写）：`quorum_lost`/`force_single_active`。复制存储承载 `manual/below_quorum/broker_draining/broker_down/replication_degraded`——**`replication_degraded` 虽 severe 是 store-backed（0009 CHECK、写者=D8b）、非客户端合成、不硬闸 destructive**（D5 登记纠正、§10.4）。**§16 登记需求 §8 偏差**：并非所有告警都 Raft 存储。

### 10.2 目录 + 文案→动作（保留 disk_pressure/raft_lag）
| kind | severity | banner（面向所有人）| 下一步 |
|---|---|---|---|
| quorum_lost | severe | 集群失多数派已只读；确认其余已死后在某 broker 上 status→force-single | force-single |
| force_single_active | severe | 运行在 force-single（单机、无完整性）；尽快 recover | recover |
| replication_degraded | severe | 审计/对象流副本 actual<target，HA 未达 | status |
| below_quorum | info | 当前仅容 F 故障，再掉一个即只读 | cluster add |
| broker_draining/broker_down | info | brokerX 下线/失联，expose 迁移中 ok/failed | status |
| **disk_pressure** | info | 盘占用超阈（**桥接现有 `disk.go`**）| — |
| raft_lag | info | 某 follower 复制延迟高（**lag 已在 status 算**）| — |
| manual | info | 运维自定义 | — |
> **裁定（R2-major）**：`disk_pressure`/`raft_lag` 是需求 §8 点名源、且 `disk_pressure` 已实现——**留在 v1 目录**，不静默推后。文案风格对齐 `error_hints.go`（每条 severe 点名下一步命令）。
> **D8b 实现修正（桥接非全 trivial）**：(a) **`disk_pressure` 桥接 = follower level-triggered re-assert + `disk.go` 新增 clear-edge**——已核验 `disk.go` 无下降沿（recover 仅 `emitted=false`，`disk.go:85`），故 D8b 加 `alertSink`-gated（生产 nil）clear 信号 + follower 每 tick 向 leader 经 `VerbAlertSignal` 重断言当前 disk 态（leader 幂等 raise/clear、dedup_key=`disk_pressure:<node_id>`、自愈丢失的 clear）。`pubSysEvent` raise 字节不变。(b) **`raft_lag` per-follower writer 推 D9、保留目录 writerless**（§16 显式登记，非静默）——已核验 leader 读不到 follower 命令域游标（`clusterstatus.go:60-61`：status 只算 self lag `CommitIndex-AppliedIndex`，per-follower applied-lag 是 D9 follower-cursor transport）；D8 桥接它会是类别错误（测 leader-local 量却命名 follower 滞后、误导运维）。同 D5 留 `replication_degraded` writerless 至 D8b 之先例。

### 10.3 投递（所有人）
每条 ctl 回包带活跃告警渲染 banner，含普通用户 `ps`/`ls`；§10.4 leader-不可达合成 banner 也对读/列命令触发。
> **裁定（R3 §18.3 覆盖原 R2-nit 披露分层）**：**取消 member/operator 脱敏分层**——单团队全员可信，降级 banner（含拓扑细节：node_id、peer UNREACHABLE 列表、applied-lag）**对所有 NATS 可达者一视同仁**。原"成员看脱敏版"的双层 banner 不做（§16.9 登记）。
> **D8b 实现修正（banner 客户端组装、不挂回包）**：拒"每 Resp 加 `Banner` 字段 + 每命令 leader 读"（~12 结构 wire churn + 延迟税）。改：banner alert 集经 **queue-group** `ctrl.by.<actor>.alert.ls.req`（任一 broker bounded-stale 读应答、一次往返、超时则跳 banner 照常渲染）；ctl **客户端组装、渲染到 stderr**（stdout 保脚本可解析、测试钉 `ps` stdout 字节稳）；**`--json` 命令抑制 stderr banner**；**常驻 banner 仅渲 SEVERE**（INFO kind 如 `below_quorum` 只在 `alert ls` 按需拉，避免每 ps/ls 刷屏致 alert 疲劳）。**ACL carve-out**：这些 member-reachable 读 RPC 置于 actor-scoped `ctrl.by.<actor>.*`（非 broker-only `cluster.*`），narrow `Pub` 加进 `PermissionsForActivatedMember`（§6.2/§13.8）。

### 10.4 destructive 闸 + 选举 vs quorum-loss（server 佐证）
destructive 硬闸**只 `quorum_lost`+`force_single_active`**；`below_quorum` 降信息 + retire 确认触发（N=2 正常不要 `--ack-alerts`）。
> **裁定（R2-major：severe 一致性）**：`replication_degraded` 虽 severe **不**硬闸 destructive——§16 显式枚举"哪些 severe 闸/不闸 + 理由"，调和需求 §8"severe 即闸"。客户端区分"连不上 NATS"vs"NATS 在但无 leader"：(re)connect 时向任一可达 broker 查最近健康（follower 可如实报"已 T 秒无 leader"而无需 Raft 写），据 **server 佐证** 合成阻断；纯客户端网络抖动不阻断；合成 banner 陈述证据与不确定性。**判定用 §8.4 pin 的同一 `T_fence=10s`（`k_fence=10 × electionTimeout 1000ms`）**——"已 T 秒无 leader"的 T 即此值。force-single 永不由客户端合成单独驱动。
> **D8b 实现修正（VerifyLeader-confirmed + 零应答边界 + advisory 框定）**：(a) **client-synth = 两条** `quorum_lost`/`force_single_active`（§10.1）；`replication_degraded` 是 store-backed severe **不**硬闸（§10.4 第一条裁定）。(b) **leadership 信号必须 VerifyLeader-confirmed、非裸 `State()==Leader`**——分区 ex-leader 在 `LeaderLeaseTimeout` 内仍报 `State()==Leader`（恰在丢数据窗口），故 broadcast `cluster-health` 响应器仅在 `VerifyLeaderRead`（`read.go:66`）barrier 通过后答 `writable_leader_confirmed=true`；follower 答 false + 已知 `leader_id`（仅文案）。(c) **gating 规则**：`quorum_lost` 闸触发 iff **≥1 应答** AND **无应答 confirmed-writable-leader** AND follower 报 `LeaderContactStale>T_fence`（brief 选举 <T_fence 不 gate）；`force_single_active` 闸 iff ≥1 应答报其真（读 D7 本地事实）。**零应答 → 不 gate**（= ctl 网络抖动/非集群、合"纯抖动不阻断"；生产 N=1 无响应器永零应答→静默无回归）。(d) **权威保护是 broker 拒绝无法 quorum-serve 的写（loud fail），client-synth 是 advisory 预检**——不过卖。force_single_active 硬闸在 D8 仅 build-and-prove（经 Layer-A health 响应器、D9 live）。

---

## 11. 一次性迁移 + 在位 nats.conf 接管
`cluster init --from-existing`：前向 **0008–0013**（`ApplyMigrations` 按名幂等跑全部已嵌入迁移，非仅 0008–0010）、`cluster_meta(applied_index=0)`、`cluster_nodes` 回填本机 + `home_broker=self`、`BootstrapCluster({self})`、迁移后 DB 作 index-0 快照。**删 `broker.New` 启动期本地写**（proxy-gen 随 P13 移出；启动只读）。DB 永不重写（`.bak`）。
> **裁定（R2-major：接管手调 nats.conf）**：**tether 接管 `nats-server.conf`**（旧的备份 `.bak`），给 **before/after 文件归属表**、与现有 systemd 单元关系、此后不得手改的部分；**同 PR 改写 `broker-ops.md` §2.3/§3.4`**（不只标 stale），免肌肉记忆冲突。
> **D9 实现裁定（2026-06，详 `docs/reviews/d9-plan.md`）**：(a) **迁移范围 = 0008–0013**（上修；`ApplyMigrations` filename-idempotent 跑全部）。(b) **DB = 单一合并 WAL 库**（§3.8/§590 + `storage.go:81`）：cluster 模式 `cluster.Node` own 唯一 WAL 写 handle on `tether.db`，`serve.go` 不再 `storage.Open`，broker 读经 node 只读 handle、写全经 `node.Propose`；每个 grandfathered 直写 mutator（`home.go:109` `UPDATE home_broker` 首当其冲、register/heartbeat/`port.Allocate`…）须 Apply-op 化或证 leader-local-serialized/只读——**非"抉择"、是已定契约**。(c) **`cluster_meta.bootstrapped` 是死键**（FSM 仅读写 `applied_index`/`applied_term`；index-0 = absent-reads-0，`fsm.go:275`）——**不 seed/断言**；index-0 marker 改 seed `applied_index=0` ON CONFLICT DO NOTHING（offline 写、非 daemon 启动写）。(d) **cluster 模式 proxy path 关 + `advanceProxyGeneration` 不跑 + `broker.New` 零 DB 写**（§16.4/§483，P13 出 v1 HA）；single 模式字节不变（call 点 New→Run 重定位）。(e) **检测信号 = on-disk `raft/` 探测**（`cluster.RaftStateExists`）+ cross-check（`raft/` 存在但无 seeded `applied_index`+self-row ⇒ FATAL；`raft/` 存在但 intent single ⇒ FATAL；intent cluster 但无 `raft/` ⇒ FATAL，堵 `node.go:165` 在 live DB 上空 bootstrap）；`--from-existing` 把 `raft/` 建成**最后一步**（seed committed 之后）。(f) **bootstrap 原语 = `BootstrapSingleNode`**（空 store `raft.BootstrapCluster` 非 `RecoverCluster`——never-clustered DB 无 log 可重放）。(g) **disk 互锁 = flock(`tether.lock`) + bolt 探测 + 新 SQLite-busy 探测**（bolt 探测对 never-clustered 的活 daemon 返 false——它从不开 `raft.db` 却在写 `tether.db`）；cluster 模式 daemon D9 起取 `tether.lock`。(h) **nats.conf 接管 = 新 `internal/natsconf` leaf**：`nats-server/v2/conf` 解析、五桶分类（INSTALL_SAFE host/port/jetstream/websocket、TETHER_GEN server_name/cluster/authorization、OPERATOR_AUTH §3.4、**TETHER_PASSTHROUGH max_payload/write_deadline/max_pending 原样保留**、UNKNOWN/`include` fail-closed 拒）、`host`+`port`→`ClientListen`（install.sh 无 `listen` 键）、**identity SSOT 从现有 conf 的 `auth_callout.issuer`+broker nkey 派生**、`server_name` SSOT（= `nats_server_id` = agent `ConnectedServerName`）、websocket 经 `WebsocketBlock` re-emit（非 raw splice）、`nats-server -t` dry-run 门 + 原子换 + `.bak` + 归属表、**halt-and-print 重启序**（tether 不编排 systemctl）。(i) **FDE-absent = advisory `psk_at_rest_unprotected`**（single-home 威胁模型、security-pragmatic）、**secrets-unreadable = 硬 FATAL**（拆开）；**secret 仅 preflight 门、不 codegen**（运维 own CA）。(j) **guard 拆除**：删六 `TestDxProductionWiresNoCluster`+`GuardSelfCheck`/`ExclusionsJustified`，**留全部 L-2 import guard 逐字**，换两模式不变式（cluster-off 字节等价含 planted-regression 自检 + cluster-on N≥3 wires-everything），同 commit。(k) **K/sec mass-reconnect 限流器**（leader-forward 路新机制）+ §18.2.18 e2e 门。(l) **完整 §17 矩阵全 IN（用户裁定：最终 phase 做完所有事，无 D9.1 split）**——观测行 `raft_lag`(新 per-follower cursor RPC)/`broker_down`(leader `raft.Stats` LastContact)/多 broker NATS status 聚合/offline `:7400` raft-ping 一并实现，GA 宣称完整 §17。(m) **README 加最小门面**（GA 对外）。

> **C3 修订（2026-06，详 `docs/reviews/c3-plan.md`）—— 授权 per-broker reload 信号，反转 (h) 的"halt-and-print"约束**：D9(h) 写"**halt-and-print 重启序（tether 不编排 systemctl）**"。C3 的 topology reconciler 让**每台 broker 给它自己同机的 nats-server 发 `nats-server --signal reload`（SIGHUP，经官方 `pgrep` 解析、无 pidfile）**以热重载新 route + auth_callout/auth_users delta。**这不是 systemctl/ssh/root 编排**：broker 与 nats-server 同为 `User=tether`（install.sh 同 uid）、信号本地、零提权——明确区别于"远程编排 systemctl"。**安全论证**：同 uid + loopback-only + 无 root；reconciler 唯一对外 side-effect 是给本机同 uid 进程发 SIGHUP，且渲染前过 `nats-server -t` dry-run + `.bak`，坏/伪造 generation 不能 brick(只 degrade-loud)。仅"无 cluster{} → 加 cluster{}"(首次 single→cluster cutover)需真 restart——走运维 `reconcile nats --manual`，非 reconciler。**reload-only 设计经 v2.10.22 真机 spike 验证**(server 存活、`/varz config_load_time` 推进、auth_callout+routes 热重载生效)。**D9 边界反转**：d9-plan 把 `internal/natsconf`/`natscluster` 划为 admin-tooling-only(serve.go/broker.go 不在运行路用)；C3 **故意让 broker 运行时 import 它们**给 reconciler——无 active guard test 锁旧边界(仅一句 doc),此处记录变更 + 新的 bounded-subprocess 纪律(reload exec 必须 ctx-bounded + single-flight；probe 的 loopback `/varz` 必须绕过 HTTP_PROXY)。

---

## 12. 失效与恢复矩阵
follower 死(N≥3)：leader 保 quorum；info。leader 死：选举~1-2s(PreVote)、已提交存活、**新 leader 从复制 entry 可重导审计补发（幂等 sweep）**、重派活性态、跑 leader-local GC。失 quorum：只读 + server-佐证合成 banner + force-single（节点级自我 fence 已停写）。N=2 死一：只读。home 死：agent 自身 NATS 重连**自驱** rehome（leader 推为 backup）。盘满 follower：逐出 queue group。扩容中崩：流副本未达 target 不报 HEALTHY-HA、不放行 retire。

---

## 13. 测试与合并门（硬）
> **泄漏门约定（D1 实现修正）**：本仓库**刻意不用 `go.uber.org/goleak`**（`test/concurrency/helpers_test.go:5`、`internal/spawnsafe/spawnsafe_test.go:127`、`cmd/tether/history_race_test.go:237` 明确拒之；go.mod/go.sum 无此依赖）。下文及 §19 checklist 凡写"goleak"处，**一律指仓库内建的 `runtime.NumGoroutine` poll-with-tolerance 计数门 + fd 基线门**（fd 门抓 `NewBackup` 漏 `Finish` 留下的目标 conn 泄漏——NumGoroutine 抓不到）。`-race` 仍为硬要求。
1. 确定性 lint：`internal/{port,proc,node,session,agentprov}` Apply-可达 mutator + 编译期断言不 import rand/ulid；禁 FSM 外对 Apply-owned 表 INSERT；禁 Apply 调 `*sql.DB` mutator；列级断言 Apply 不写活性列。
   > **D2 实现修正**：Apply-reachability 调用图用 `golang.org/x/tools/go/callgraph/cha`（或 `rta`）seed 于 `fsm.Apply`，**非手卷 `TypesInfo.Uses` BFS**——Applier 是 `defaultAppliers() map[OpType]Applier` 里的**接口值**，静态 BFS 遍历不了 map/接口 dispatch → 整个 Apply 子树不可达 → lint **vacuously 绿**。CHA/RTA sound 地过近似动态 dispatch。引入 `golang.org/x/tools` 作 **test 依赖**（go.mod 增量，升级前验 go directive）。共享 `genericExecApplier` 切断调用图，故 reachability lint 是**绊线、非 Apply 确定性证明**；真正的确定性保证是下条 §13.2 多 FSM 等价。每条 lint 规则配**经同一 map dispatch 的负向对照**（poison Applier 必被抓、Plan-only helper 不被抓）。
2. 多 FSM **逻辑内容**等价（`.dump` 去 schema_migrations / per-table 排序哈希）；含 port.Allocate/proc(nil clock)、分配→硬删 session→再分配。
3. 快照-恢复-重放确定性（撕裂 integrity_check 拒）。
4. **kill-9 崩溃一致性矩阵**（§3.7 两窗口，含 BoltDB 超前 SQLite 重导 lastApplied）——承重、合并前必存在。
5. WAL 并发：持续 Apply + 流式 backup，无超 busy_timeout/无 SQLITE_BUSY、一致提交点；WAL 下重验 FK/单写者。
6. **per-expose (per-publicPort) brokerAddr**（§18.2.2）+ 一 agent N expose 分散到不同 home + mass-rehome 风暴有界 re-REGISTER + rehome 后瞬断重拨新 addr，`-race`+`goleak`；home_catching_up 不塌 terminal。
7. **RF3 cert 重启不变性 + 轮换（R3F2）**：broker 重启后同一 publicPort 的 advertised fp 不变（稳定持久 `tunnel-cert.pem`，§15）；轮换 wire-list `{current,previous,valid_until}` 下老 agent 在窗口内接受新旧任一、**窗口内 agent 进程重启后仍按 directive 重连成功**、`valid_until` 后旧 cert 被拒；拒真正不在 pin 集的证书。
8. **跨真 ≥2 节点 nats 集群**：client 连 A、B 应答 callout 授权；PIN-join 撞选举/转到刚下台 leader 返回可重试 deny 不假放行；无 PIN/JWT 字节出现在非 broker 可订 subject。**RF1 发布者 ACL**：普通 bus credential pub `cluster.apply.*` 被 NATS ACL 拒、broker nkey credential pub 被允许；member/agent JWT pub/sub `cluster.*` 被拒。**D8b carve-out（正/负向）**：member JWT **CAN** pub `ctrl.by.<actor>.cluster-health.req`/`.alert.ls.req`/`.alert.ack.req`（banner/gating 读 RPC、actor-scoped、非 broker-only）；member **CANNOT** pub `cluster.apply.*`（负向保绿）。
9. 扩容/审计：1→3 后每条 history-<sid> 与 OBJ_xfer 副本到 target 才 online；kill-during-expand 无审计丢失；leader 提交后审计发布前崩 → 新 leader 精确补发不重复。
10. transfer：tier-B 对象 N≥3 杀 home 存活；ex-home ReassignHome 在途不双绑。
11. **membership 两阶段（R3F1）**：`ClusterNodeUpsert` committed 后 `raft.AddVoter` 失败 / AddVoter 成功后 catch-up stalled / 重复 add 同 node_id——断言 `phase` 状态机幂等、`status/doctor` 如实显示停在哪个 phase、绝无"DB voter 但 Raft config 非 voter"静默分叉；follower `Apply(ClusterNodeUpsert)` 复算 join PoP，伪签被拒。
12. force-single→recover 3 节点实跑演练；offline 子命令磁盘锁防并发。
13. `cluster status --json` + 退出码契约；FDE preflight；新 phase 进 e2e。

---

## 14. open issues 裁定（承前 13 + 本轮新增）
NATS=C；快照=WAL+online-backup(独立只读 handle)；applied_index 权威=SQLite；kill-9=门；force-single=**自我观测 fence + offline 子命令 + runbook**（撤"结构性"）；PreVote=前置门；危险操作=TTY+typed 无旁路；JWT=短 TTL + **撤销靠主动断连**（非 TTL）；psk=删 DEK + **FDE 可验证 preflight 门**；expose=先确认后回包 + cert_fp 钉证硬要求；JS=**退 per-session 流** + 显式副本重配；home=agent 自报 server_id 桥接 + 自身 NATS 重连自驱 rehome；audit=可重导 + dedup 窗口 > 选举+扫尾（0 丢失"近似"登记）；**P13/proxy 移出 v1 HA**；**leader-only 发布实为 ~63 处**（机械契约 + lint 门）；op 集删 GenericRowMutate、ProcGC 转 leader-local。

---

## 15. provisioning 与文件布局
```
/etc/tether/secrets/{account.nk(共享,0600), node-ident.nk(每台,0600),
                     broker.nk(每台,0600;tetherd→本地 NATS 的 bus 身份=AuthUsers,§6.2 RF1),
                     tunnel-cert.pem + tunnel-key.pem(每台,0600;稳定持久,§7.7 RF3),
                     cluster-ca.pem/.key}                                              # dir 0700
/var/lib/tether/{tether.db(WAL), raft/{logs,snapshots}}                               # 0600
/var/run/tether/admin.sock                                                            # 0600
```
**稳定 tunnel 证书契约（RF3）**：tunnel server **不再每次 Start 自签临时证书**，改用上列 `tunnel-cert.pem/.key`（绑 node-identity、provision 期生成、跨重启不变）。`cluster_nodes.cert_fp` = 该证书指纹；REGISTER/`HomeDirective` 上报的 `cert_fp` **必须来自该稳定证书**。**写入/轮换经 Raft entry，wire 契约（R3F2：选 wire-list 方案、agent 无状态）**：
- **wire**：`HomeDirective` 不再单 `cert_fp`，改 **`cert_pins{current, previous, valid_until}`**（`previous` 可空、`valid_until` 仅轮换期非零）；`cluster_nodes` 加 `cert_fp_prev`/`cert_fp_valid_until` 两列承载之。agent **按 directive 当前列出的 pin 集验证**（接受 `current` 或未过期的 `previous`），**不在本地持久化任何 pin**——故 agent 在窗口内重启后，重连/register 拿到的 directive 仍带 `{current,previous,valid_until}`，照常验证，无陈旧本地态。
- **初始**：create/add 由 `ClusterNodeUpsert` 写 `current=fp`、`previous=空`。
- **轮换 = `cluster rotate-tunnel-cert <node_id>`**：① `ClusterNodeUpsert` 写 `current=新fp, previous=旧fp, valid_until=now+window`；② broker 即起用**新证书**对外服务（agent 已接受 `current`）；③ `valid_until` 到点后 `ClusterNodeUpsert` 写 `current=新fp, previous=空`——**旧证书此后被拒**。窗口时长 pin（默认值随 plan 定，须 > 最坏 agent 重连周期）。
- 门（§13.7）：**broker 重启后同一 publicPort advertised fp 不变**（重启不变性）；**轮换窗口内 agent 进程重启后仍能按 `{current,previous}` 重连成功，窗口后旧 cert 被拒**；轮换中老 agent 不被合法重启的 home 拒（§18.2.11）。
- 备份/恢复：`tunnel-key.pem` 与其它 secrets 同 0600 + 同 FDE 卷、随 §8.3 secret 分发 runbook 走可信信道。
> **D6 实现修正（fp 格式 SSOT · 2026-06 · `docs/reviews/d6-plan.md` DA-10/R-20/R-21）**：`cert_fp`/`cert_fp_prev` 格式 = `"sha256:"+hex(SHA-256(cert.Raw))`（leaf DER、非 SPKI），唯一函数 `tunnel.CertFingerprint`，harness 播种与 agent 验证共用。agent 验证用 **`VerifyConnection`**（TLS1.3 resumption 安全）：空 pins → `InsecureSkipVerify`（唯一 N=1 退路）；非空 → `current` 或未过 `valid_until`(`*time.Time` 区分 NULL/零时) 的 `previous` 才收，否则握手在写 token 前失败。**D6 build-and-prove**：稳定 cert tunnel server 经 harness-only `NewServerWithCert`、轮换窗口经 harness 重启换 cert 证（live hot-swap + `cluster rotate-tunnel-cert` 操作命令属 D7；生产仍临时签，cutover=D9）。

**`cluster doctor`/`add` 入群前 preflight（绿/红）**：秘密文件（含 `broker.nk`/`tunnel-cert.pem/.key`）存在+0600、`account.nk` 指纹==集群、时钟偏移在界、Raft(:7400)/route(:6222) 可达且不对公网开放、**FDE（LUKS/dm-crypt/FileVault）已开**——缺 FDE **拒绝入群或升持久 `psk_at_rest_unprotected` 告警**（psk 明文复制到 N 盘、靠 FDE 保护，非建议而是可验证门）。`recover --dump-divergent` 文件 0600 + 同 FDE 警示。

---

## 16. 与基线偏离登记（需外审确认）
1. 反转 A.1/A.2：NATS 独立进程但 tether 编排（C）；在位接管 nats.conf、同 PR 改写 `broker-ops.md` §2.3/§3.4。
2. proto v1→v2 硬升级、不兼容老 agent、协调全机群重装；`home_catching_up`/cert_fp 钉证须 broker+agent 同改。
3. 存储：WAL；快照独立只读 handle；保留 AUTOINCREMENT、撤"字节级一致"、等价比逻辑内容。
4. **P13/proxy 移出 v1 HA**：proxy 故障切换正确性非 v1 保证（§0/§17），待 P13 无条件 PASS 再做 proxy-HA 叶子。
5. NTP 降为推荐（leader 单调时间戳地板保排序正确性；NTP 只为审计时间戳可读）。
6. 删 cluster DEK；psk 静态靠**可验证的 FDE preflight**。
7. agent 面隧道 v2 **必须钉 cert_pins**（current/未过期 previous，§7.7/§15 RF3；删"仍 InsecureSkipVerify"矛盾，只限 N=1 退路）。
8. 审计 0 丢失为"近似"（§6.3 窗口）；retire 不撤 account-key 信任（§8.3，需轮换 runbook）；命令面 promote/step-down 并入 transfer-leader；severe 告警非全部硬闸 destructive（§10.4 枚举）；`cluster-ca.key` 单独泄露即可 route-join（§10.2 注，route-cert 接受须再过 roster）。
9. **告警 ack 从需求 §8 的 per-identity 生效降为集群级单 ack**（§18.3/§10.1/§4.2：全员可信单团队，去 per-identity 簿记；`acked_by` 仅展示）。补偿：严重告警每新会话必重现、ack 不压 banner，故"降级"不致永久隐藏严重态。**`alert/` 去 member/operator banner 脱敏分层**（同 §18.3：单团队，原 §10.3 的成员脱敏取消，拓扑细节对所有 NATS 可达者可见）。
10. **node-identity 不再做 apply.* 转发签名**（§18.1）：apply.* 鉴权 = route mTLS + broker-only ACL；node-identity 仅入群 PoP + mTLS 叶子钉证。转发是一致性而非安全边界（全员可信下 per-broker 转发签名无遏制力）。

> **D5 登记（2026-06，详 §6.3/§6.4 D5 实现裁定）**：(a) **`replication_degraded` 是 store-backed（0009 CHECK）、写者 = D8b、非 client-synthesized**（client-synthesized 仅 `quorum_lost`/`force_single_active`）——D5 只出预测语、不写 alert 行；纠正任何"degraded 由客户端合成"的措辞。(b) **审计近似 0 丢失收紧为"以快照节律 + dedup 窗口为界"**（仅 `OpReconcileBatch` 可重导；leader-local-only 审计如 `audit.call` 仍 best-effort）；快照截断未发布的 reconcile 审计 = 有界 loud accepted-loss。(c) **RF2（N=2）= 读可存活、写零容错**（2 副本 raft 组写 quorum=2，掉一即写停；与"N=2 不承诺完整性"一致；外审可一行翻成 R1）。

> **D8 登记（2026-06，详 §9/§10 D8 实现修正 + `docs/reviews/d8-plan.md`）**：(a) **client-synth severe = 两条** `quorum_lost`/`force_single_active`；`replication_degraded` store-backed（写者=D8b）虽 severe **不**硬闸——纠正 §10.1 旧"三条"措辞。(b) **`raft_lag` 与 `broker_down`（contact-loss）writer 一并推 D9、目录保 writerless**——两者同属"leader 读不到 follower/peer 命令域游标与活性"的能力缺口（`clusterstatus.go:60`；hashicorp/raft v1.7.x 不洁净暴露 per-peer liveness），D9 transport 才补；非静默推后=此显式登记。D8b reconcile loop 实做 `replication_degraded`（从 ReplicaReport）+ `below_quorum`（NumVoters 数学：F==0 即 nv==2）+ `broker_draining`（cluster_meta draining:* 键）三条 leader 可廉价判定的 kind。(c) **banner 客户端组装、stderr、仅 SEVERE**（非挂回包 `Banner` 字段；INFO kind 只在 `alert ls`）。(d) **alert raise/clear = 独立 leader-gated `alertReconcile` loop**（非折进 audit-publisher tick——否则 liveness 反转 / audit drain 饥饿 / 破 D5 idle-zero-writes），仅真状态跃迁时 propose；clear 谓词 = **`Observed && !Degraded()`**（在 `else if rep.Observed` 门内，故覆盖 `AllAtTarget` **与**空流集 len==0 两种"已不 degraded"态、又绝不在瞬态 `!Observed`（JS meta-not-ready）下 false-clear——后者由外层 Observed 门排除；Stage C m5 把原 `AllAtTarget` 收紧为此，闭合 Observed&&len==0 的 latent wedge）。(e) **alert ack 永久 display-only、severe 重现纯客户端侧**（无 session-nonce、ack 仅压 inline-prompt 不压 banner）。(f) **transfer audit 可重导（committed 后）**；矛盾终态由 `claimFinalize` 单-broker 终态原子认领 + home-gate 单写 + per-invocation transfer_id 化解（无 terminal-state guard/新表）。(g) **alert-SQL 确定性 = 有序 Apply + committed-state 谓词**（`WHERE NOT EXISTS(ACTIVE)` 同序同态 → 同 `RowsAffected`；非"任何副本都不报错"）。(h) **ACL carve-out**：member-reachable `ctrl.by.<actor>.cluster-health.req`/`.alert.ls.req`/`.alert.ack.req`（非 broker-only `cluster.*`）；§13.8 加正向（member 达）+ 保负向（member 拒 `cluster.apply.*`）。(i) **transfer 数据面经 broadcast-SUB + home-keyed gate**（非 §4.1 转发）；retire 门 xfer 枚举读 JS 实际流列表（非 DB `ListSIDs`）；boot orphan reaper clustered 模式 home-ownership 过滤。(j) **transfer continuation/terminal（push-commit/finalize）tracker-miss 在 clustered（`selfID!=""`）模式静默、生产保留 `transfer_unknown` 回复**（外审 F1：broadcast-SUB 下非 owner 不得抢答）。(k) **client-synth destructive gate 最终 NATS-侧 gated 集 = `session rm`/`push`/`pull`/`expose`/`expose rm`/`run`（+`kill` 随 run 流）**（外审 F2）；各带 `--ack-alerts` 覆盖、N=1 no-responder inert。**不 gate**：`kick`/`rotate-pin`（接缝同形可增量）、proxy/node 子命令、`cluster *`（走 adminsock 非 NATS、有 D7 typed-confirm 门）。(l) **显式 `alert ls`/`ack` fail-closed**（外审 F3/F4：strict fetch 失败返 error、ack 仅 `"ok"` 成功；与 best-effort banner 的 lenient fetch 分离）。**全部 build-and-prove、不切线上**（cutover=D9）。

> **D6 登记（§16.7 偏离 · 2026-06，详 §6.5/§7.x D6 实现裁定 + `docs/reviews/d6-plan.md`）**：(a) **build-and-prove 偏离**：生产 tunnel server 保留每次 Start 临时自签（agent 空 pins → `InsecureSkipVerify` 唯一 N=1 退路）；稳定 cert + cert_pins 钉证仅在收到 `HomeDirective`（harness 才发）的 clustered 路生效；生产 `serve.go` 不构造 `cluster.Node`/不发 directive，cutover=D9（同 D2–D5）。(b) **桥接键 = 确定性 `server_name`（`ConnectedServerName`）非 NUID**（§6.5 DA-1 校正）；agent→server_name 绑定为复制态 `nodes.nats_server`（migration 0012，双写、生产 inert）。(c) **D6 唯一 migration = 0012**（加可空列 `nodes.nats_server`，无索引、无生产读）。(d) **catch-up barrier = epoch-as-local-row-epoch**（非 `applied_index>=raft-commit`——域不兼容，§7.2 DA-7 校正）。

---

## 17. HA 保证矩阵（面向运维，一屏）
| 数据类 | N=1 | N=2 | N≥3 健康 | N≥3 扩容未重配完 |
|---|---|---|---|---|
| 控制状态(session/port/proc/member) | 无冗余 | 双副本掉一即只读 | **已提交 0 丢失**容 1 故障 | 同 |
| 审计 history-<sid> / OBJ_xfer | R1 | R2 | R3 | **仍 R1/部分——未达 HA**（status actual<target + `replication_degraded`，禁 retire 原节点）|
| 审计 0 丢失 | — | — | **近似**（leader 提交后/JS 发布前崩有窗口，可重导 sweep 兜底；D5：仅 `OpReconcileBatch` 可重导、以快照节律+dedup 窗口为界；leader-local-only 审计 best-effort） | 同 |
| ev/PTY/transfer 在途 | best-effort | best-effort | best-effort（重连/重试） | 同 |
| **proxy(/sub) 故障切换** | — | — | **非 v1 HA 保证**（P13 移出，§0）| 同 |
| 撤销延迟 | 即时 | — | **max(副本应用滞后, 残留 JWT TTL, 强制断连耗时)**——靠主动断连非 TTL | 同 |

**结论行**：HA 仅 N≥3 且 JS 副本达 target 后成立；N=1/N=2 无完整性；扩容后审计要等 leader 驱动副本重配完才真 R3；proxy 切换非保证。

**健康判定分两个明确模式（§18.2.8 裁定，互不混用）**：
| 模式 | 何处跑 | 数据源 | peer 活性 | 退出码 | banner 措辞 |
|---|---|---|---|---|---|
| **ctl / NATS 视图**（默认，笔记本） | 任意 NATS 可达处 | **仅 NATS 可达 broker 的自报视图**（follower 如实报"已 T 秒无 leader"+自身 applied-lag） | **不直连 Raft :7400**（私网+防火墙，笔记本够不到） | 0/1/2/3 按自报推断 | 显示 peer UNREACHABLE 时**明说"本机 NATS 视图、非直测，不构成 force-single 依据"** |
| **broker 本机 offline 视图** | 故障 broker 本机（§8.4 runbook） | 磁盘 raft 配置 + admin socket，零 NATS 依赖（§18.2.8） | **对每 peer 直连 Raft-transport ping**（broker 在私网内，够得到） | 同 4 档 | 实测 peer 状态；但 force-single 安全闸是 §8.4 step2 硬前置、非此计时 |

ctl 侧绝不因"笔记本 ping 不到 :7400"误报 `quorum_lost` 而诱导误 force-single；只有 broker 本机 offline 视图能直测 peer，且即便直测，force-single 仍受 §8.4 的 hard-refuse（任一 peer 可达即拒）+ `--confirm-peers-dead` 约束。

### `cluster status` 规格
列：`node_id|name|phase|role(leader/voter/learner)|applied-lag|last-contact|account.nk-fp(Y/N)|stream-replicas(actual/target)`；一行粗体健康判定 + 下一步命令指针（每降级态都有）；**`--json` 稳定 schema + 退出码契约（0=HEALTHY-HA、1=DEGRADED-可写、2=只读/quorum-lost、3=force-single）**供 cron/监控；每状态给一份具体渲染样例（Stage-C 验收 fixture）。
> **D7 实现定稿（`--json` versioned schema + `reach_source` 判别 + 5 fixture）**：每行带 `reach_source`——ctl/NATS 视图 `reachable:false` 必带 `reach_source:"self-report"`（没经 NATS 听到）vs offline mode-B `"raft-ping"`（直拨 :7400、实测死）；**NATS 不可达贡献 UNKNOWN、绝非 DEAD**；exit 2（`quorum_lost`）在 NATS 视图**仅**来自肯定的只读/无 quorum self-report、**绝不**来自"无人上报"（堵 false-quorum-lost→误 force-single→数据丢失链）。`role` 取自 `RaftConfiguration()`、`phase` 取自 roster、交叉渲染失配即粗体 `INCONSISTENT`；`learner` 可渲染但 D7 永不产出（AddVoter 即设 Voter suffrage、不断言 learner fixture）。schema 形如 `{"schema_version":1,"view":"ctl-nats"|"offline","health":...,"exit_code":0..3,"leader_node_id":...,"banner":...,"next_step":...,"nodes":[{...,"reachable":bool,"reach_source":...}]}`。5 个 byte-stable golden fixture（`test/d7/fixtures/`）：HEALTHY-HA(0)/DEGRADED(1, 一节点 CATCHING_UP)/quorum-lost(2, banner 点名 go-to-broker-host)/force-single(3)/joining(1, VOTER_ADD_FAILED 清理指针 + phase/role fork 渲染)。
> **D7 build-and-prove 边界（外审 F4 对齐）**：D7 落地 **per-broker self-report**（每 broker 经 adminsock 自报本机视图、versioned `schema_version:1`）+ **offline 磁盘 roster 快照**（`cluster status --offline` 同 schema、disk-only、**无** health/exit 语义、`reach_source:"disk-snapshot"`）。**多 broker NATS 聚合 ctl 视图**（跨 broker 汇总、exit 2 仅来自多数自报）**与 offline mode-B 真 :7400 raft-ping 推迟 D9**。D7 单 broker 见无 leader 时 banner 明示「单 broker 视图、非集群共识」、不把 force-single 当即时下一步——force-single 自身的 `--confirm-peers-dead` TCP-liveness HARD-REFUSE 才是权威活节点闸。
> **裁定（R3-blocker §17 矛盾）**：**ctl/笔记本侧的健康判定不得依赖直连 Raft :7400**（私网+防火墙，笔记本够不到→会把每个 peer 读成 UNREACHABLE→误报 quorum_lost→吓得运维误 force-single）。**笔记本侧健康只从 NATS 可达 broker 的自报视图推**（follower 如实报"已 T 秒无 leader"）；**对 peer 直连 Raft-transport ping 仅限 broker 本机上的 offline `cluster status`**（它在私网内）。公网侧 status 显示 peer UNREACHABLE **不构成 force-single 依据**，banner 须明说。

---

## 18. 第 3 轮裁定与 Stage-C 残留（审计轨迹）

> 第 3 轮（52 条：8B/20M/21m/3n）已进入实现精度层。
>
> **外审 round-1（FAIL）后处置**：本节的约束性裁定原写作"覆盖正文"，导致正文与 §18 出现**互斥双源**。现已**逐条回写正文对应章节、删除被否定的旧契约**（见各 §18.1/.2/.3 项末的"→ 已回写 §X"与 §16 偏离登记 9/10）。**本节自此降为审计轨迹（记录第 3 轮改了什么、为什么），正文是唯一实现尺**；如正文与本节再有出入，以正文为准。§18.4 进一步把其中**会改 wire/状态机/安全边界的架构契约**升级进正文（F6），仅留纯实现精度在 plan 阶段。

### 18.1 设计岔路裁定：node-identity 密钥
> **裁定**：**node-identity 密钥保留，但只用于两处：(a) 入群挑战-应答 PoP（运维同意的唯一可密码学强制的属性）；(b) 绑进 Raft/route mTLS 叶子证书，使"只有 cluster-CA 签名"不足以入群/入路由**。**删掉**它作为 `apply.*` 转发的"签名 origin proof + tether-broker role + term 校验"——在"所有 broker 同等可信"下，per-broker 属性对转发**无遏制力**（沦陷 broker 已在信任边界内）；`apply.*` 这一跳改由**route mTLS 客户端身份 + broker-only pub ACL** 鉴权（与 §6.2 PIN 走 routes 不用 XKey 同理）。net：少一套"转发签名"机制，保住"入群 PoP + mTLS 叶子绑定"两个真有用的性质。
> **D3 实现修正（2026-06，详 d3-plan R5）**：(b) 的 Raft/route mTLS **叶子 nkey 钉式**属 **D7 join-PoP**——D3 raft transport（`:7400`）只发 **cluster-CA 签名 X.509**（`tlsStreamLayer`→`raft.NewNetworkTransportWithConfig`；raft v1.7.3 **无 `NewTLSTransport`**，已核验），node-identity 叶子钉证待 D7。§1.3 line 37 同改。

### 18.2 第 3 轮 blocker/major 约束性修正（已回写正文，落点见各项；正文为准）
1. **§6.4/§9 流副本重配（blocker）**：三处 `Replicas:1` 字面量**建流时即改 `replicasFor(nVoters)`**；`ensureStream` 的 already-in-use 分支**比对 actual vs target、不足则 `UpdateStream`**；重配须等**新 nats-server 已加入 JS meta-group**（非仅 Raft）；`OBJ_xfer-<sid>` 与 `history-<sid>` 同等处理；**所有流达 target 前禁 retire 原节点**；sweep 限并发/封顶 + 单条卡死的逃生（force-delete 死 session 的流 vs 阻塞）+ status 用"全部 at target"canonical flag（非抽样）。
2. **§7.5/§7.6 粒度（blocker）**：是 **per-expose（per-publicPort）brokerAddr**，不是 per-session——addr 放每个 `clientSession`（已按 publicPort 键），`dialAndRegister/redialWithBackoff/swapTransport` 读 `sess.brokerAddr`；一个 `tune.Client` 并发扇出 N 个 home；cert_fp 按 expose 线程化。
3. **§6.1/§6.2 queue-group 签名（blocker，诚实定性）**：每台 server `AuthCallout.Issuer=共享 account pub、AuthUsers=每台 broker nkey、resp.Audience=发起请求的 `req.Server.ID``（应答 broker 回显，非自身 ID）。**leader-forward 降级为"一致性/PIN 校验"机制、非安全边界**——全员可信下任一 broker 已能签 allow，转发不提供遏制；如实写。
4. **§8.3/§10.3 入群 PoP（blocker）→ 已回写 §8.1 两阶段（R3F1 纠层混淆）**：`cluster add` 增**挑战-应答**——leader 发 nonce、入群 daemon 用 node-identity 私钥签、leader 对运维键入的 pubkey 验签。**join PoP 由 follower 在 `Apply(ClusterNodeUpsert)`（业务 op）复算**，**不是**在 `raft.AddVoter`（Raft config 路径、非 FSM 拦截点）——`raft.AddVoter` 仅在 `ClusterNodeUpsert` committed 后由 leader 发起（§8.1）。明示**不防**：全沦陷 leader 仍可提任意成员变更（已接受）。
5. **§8.4 force-single 离线脚枪（blocker）**：(a) offline 工具与 daemon **共享同一把磁盘 advisory lock**，runbook step1 = `systemctl mask`（防 systemd 自动重启与工具并发动 `raft/`+`tether.db`）；(b) `raft/`+`tether.db` 缺失/空则**拒绝**（"无既有 raft 态，会建空集群丢全部数据"）；(c) 改写 Raft 配置为 {self} 前先**调和两存储**（把 BoltDB 已提交到 commitIndex 的 entry 应用进 SQLite，或文档化恢复点 = SQLite.applied_index、超出者经 `--dump-divergent` 取证后丢弃）；(d) **任一 peer 经 Raft-transport 可达即 HARD-REFUSE force-single**（peer 活着→会脑裂），要求键入被抛弃 peer 的 node_id + `--confirm-peers-dead`。
6. **§8.4 self-fence 谓词（major）→ 已回写 §8.4/§3.2/§6.2**：一处、一个单调时钟——`自上次成功 leader 接触 > T_fence` 即拒绝 (1) leader Apply-forward、(2) auth_callout allow、(3) 正确性敏感读。**已 pin：`T_fence = k_fence(10) × electionTimeout(1000ms) = 10s`，且 `T_fence ≪ runbook step1 最小墙钟`**（§8.4）。
7. **§8.4(b)/recover（major）**：键入被 wipe 的 node_id 确认；打印发散摘要（"N 行 sessions/ports/audit 仅存于此节点、将永久丢弃、已存 `<file>` **仅供取证不可自动合并**"）；dump 写失败则拒 wipe。
8. **§8.4/§11/§17 nats 坏掉的本地诊断（blocker）**：`cluster doctor`/`status` **纯本地可跑**（admin socket + 磁盘，零 NATS 依赖），报"本机 nats-server 未起/conf 非法"+ 精确 systemd 单元名 + `.bak` 回滚一行命令；takeover **preflight 遇 tether 不认识的指令则拒绝接管**（不静默覆盖手调 conf）。§11 须承认**今天根本没有 nats.conf 模板化**（`install.sh` 写的），natscluster 是首次生成、需与 install.sh 产物对账、列出哪些指令转归 tether。
9. **§5/§11/§3.2 proxy 启动 db.Exec（major）**：cluster 模式下 **P13 proxy subscribe 路径编译关/禁用**，`broker.New` 在 cluster 模式**零 DB 写**（`advanceProxyGeneration` 不跑）；门：cluster 模式 `broker.New` 无 DB 写。
10. **§6.3/§16 审计发布无 ACL 强制（major，诚实）**：`PermissionsForBroker` 通配 pub `s.*.audit.>` → 任一 follower（含沦陷/选举中错算）可注入伪造/重复审计且 JetStream 无法辨源。如实写"leader-唯一发布是代码契约+全员可信、非 quorum/源认证强制"；审计盖 `node_id`+`raft_index`+leader-epoch header（消费端可查 raft_index 已提交、丢 stale-leader 发布）；禁非 leader 发布的 lint 设硬门。
11. **§7.7/§1.3 cert_fp 移动靶（major）**：tunnel server 改用**稳定持久证书**（`/etc/tether/secrets`、绑 node-identity、**非每次 Start 重生成**），否则重启即换 fp→`cluster_nodes.cert_fp` 变陈→所有 agent 拒连合法重启的 home（自伤）。定义谁在 provision/rotation 时经 Raft entry 写 cert_fp；门：重启 home→老 agent 仍 re-pin 成功、拒真不同证书。
12. **§3.2/§6.2/§17 撤销（major）**：(1) 每次 callout 决策对**栅栏到 ≥撤销 index 的读**或复制的撤销墓碑 + §3.2 fail-closed 谓词裁决（堵"踢掉后立刻在落后 follower 重新 24h JWT"）；量化"在落后 peer 重认证窗口 = 副本应用滞后"，cluster 场景默认 JWTTTL 调短；(2) **per-port 主动撤销**：`PortRevoke/PortFree` Apply 时 **home broker（即便作为 follower 重放）关该口的公网监听 + yamux**；leader 如何通知远端 home 须定义。
13. **§6.2 auth fail-closed 谓词（major）→ 已回写 §3.2/§6.2（统一为 §8.4 的 `T_fence`）**：节点自身观测到与 leader 失联 > `T_fence`（已 pin=10s，`> 最坏 PreVote 选举`~2–3s，§8.4）才 fail-closed（非"正在选举但仍见 quorum"就黑洞全集群新连接）；provisioned-read 授权是**有界陈旧**（被撤成员在落后应答者上可放行至其应用撤销，界 = 应用滞后），加测试断言最坏接受窗口 = 应用滞后而非 0；谓词是 O(本地读)/连接。
14. **§3.1 PreVote（major）**：§3.1 **pin 确切 raft 版本**并把"该版本 PreVote 实测生效"列为合并门项（配置字段存在 ≠ 行为生效）。**字段名修正（D0 实测对齐 v1.7.x 源码）**：真实字段是 `Config.PreVoteDisabled`（缺省 false=启用），**无 `Config.PreVote`**；且 raft 在 transport 不实现 `WithPreVote` 时**静默关闭** pre-vote（`api.go`：`preVoteDisabled = conf.PreVoteDisabled || !transportSupportPreVote`）——故实测门须三件齐验：① 编译期断言 transport 实现 `WithPreVote`（`var _ raft.WithPreVote = tr`，InmemTransport 已实现）；② 分区一个 follower（保留 leader 侧 quorum），断言**被隔离 follower 自身的 `CurrentTerm()` 不抬高**（pre-vote 拦住 term 自增——这是 pre-vote 的直接效果），且健康 leader 不下台、term 不涨、重连后干净归队；③ 反向对照子测试（`PreVoteDisabled=true`）断言**同一被隔离 follower 的 term 反而抬高**，证明门有判别力、非恒真。**判别信号取被隔离节点自身的 term**（D0 inmem 实测：启用恒为 termBefore、禁用抬到 20+），**非 leader 的 term**——inmem harness 下 leader 侧 term 在两种配置下都不被扰动（rejoin 扰动不在此 harness 内确定性复现），故 leader-term 不变是真不变式但**无判别力**。
15. **§8.3 retire 不撤信任（major，诚实）**：roster/Raft 移除即时，但 `account.nk` + cluster CA **不轮换** → retired 节点留有它们即在 TTL/轮换前仍可签 JWT、出示 route 证书。**把 account.nk + CA 轮换 runbook 内联为本 epic 硬依赖**（含全机群重装、status 显示"retired 节点凭据在轮换前仍密码学有效"）；明示"retire 不轮换 = 仅拓扑变更非安全边界"；**runbook 没写不算 retire done**。
16. **§16.8/§10.2 双信任面（major）**：明列——`cluster-ca.key` 泄露 ⇒ NATS route+callout 参与（总线级沦陷，**即便 Raft 拒它当 voter**）；`account.nk` 泄露 ⇒ JWT 签发。**route mTLS 叶子也须 nkey-钉到 node-identity**（同 Raft 传输），令"只有 CA 签名"不足以 route-join（**D3 实现修正**：D3 仅发 CA-only X.509 routes/raft 传输，**nkey 叶子钉式 = D7 join-PoP**；与 §1.3 line 37 / §18.1 一致）；`cluster-ca.key` 按 `account.nk` 同等重视、定轮换。
17. **§10.4 destructive 命令表 + drain（major）**：§10.4 加**destructive 命令明表**（≥ expose/run/session rm，及 push/pull/kill/expose-rm），逐条标在 quorum_lost/force_single 下是否 gate，与 §4.1 转发动词集对账（别混）；drain 给**默认提前通知时长 + 截止前持续服务 + 到点（或 `--now`）迁移 + `--abort` + status 进度（"draining: 3/5 已迁，1 rebuild-OFF 待"）+ 销毁 rebuild-OFF 须键入确认**。
18. **§4.1/§7.4/§2.2 RTO 预算（major）**：register 现无 home/epoch 字段、tunnel client 固定单 addr 构造一次——**"directive epoch → `Open(newAddr)` 重指"是全新接线**，须具体化（tunnel adapter 加 `Open(newAddr)` 入口）；§7.4/§2.2 列**失效切换最坏串行时延和**（NATS 重连退避 + rehome epoch swap + home `applied_index>=raft_index` 栅栏 + leader reconcile 往返 + JS 副本等待）证明落在数秒~数十秒；§7.4 的 K/秒限流**也作用于 leader-forwarded register→ReconcileBatch 路径**（防上百 agent 同时弹连的惊群撞上选举中 leader）；加 mass-reconnect e2e 门。
19. **§10.2 disk_pressure/raft_lag 诚实定大小（major）**：今天**无 alert store/banner**，disk_pressure 仅 sys.event——晋升进存储/ack/banner 是净新增（小但非零）；raft_lag 全新（raft 都还没有）。保留在 v1 但如实标，别悄悄砍。

### 18.3 采纳的减法（第 3 轮 YAGNI）
删 `cluster_revoked_identities` 表（0009，无 writer/reader）；确定性回到**一条结构性不变式 + 一个 Apply-可达 lint**（非四套 AST 扫描器，因 leader 复制字面量 SQL 后发散已结构性不可能，lint 只是 tripwire）；告警**去 per-identity ack 簿记 + 去 member/operator 脱敏分层**（全员可信单团队）——改**单一集群级 ack + 严重告警每新会话必重现**（ack 只压抑内联 ack 提示、不压 banner）；审计"纯函数"规则**只施于恢复尾**（leader 变更时重发那段），非全 71 站点的全局属性；home 指派加**资格约束**（只选 ONLINE/HEALTHY、非 draining/retiring/未过 catch-up 的 broker）。

### 18.4 残留分类（外审 F6 后）

> 外审 F6 指出原"实现精度残留"里混入了**会改 wire / 状态机 / 安全边界的架构契约**，不能由 plan 自由发挥。现拆两类：
>
> **(A) 已升级为正文契约（F6，正文为准；下列标 `[→正文]`）**——`[→正文]` 的 4 项已写进对应章节，本节仅留指针。
> **(B) 留 plan 阶段的纯实现精度**——其余项，确为长尾，外审就"此边界"签字。
- `sqlite_sequence` 排除出逻辑等价比对 + 规范快照路径为 page-level online-backup（非 `.dump+reload`）+ 禁 Apply 可达 `LastInsertId()`/SELECT row_id（§3.6/§13.2）。
- `ReconcileBatch` **输入**集也须按全序重排再烤（输入是 `started_at DESC`、非唯一时间戳靠 rowid 破并，脆弱）；时间戳走"Raft op 内显式值、各副本 Apply 绑该值"而非字符串字面量（§3.4）。
- **[→正文 §3.5]** 快照形状：online-backup 是整文件 page-copy、投影不掉活性列——§3.5 不变式已改述为"**Apply 永不写活性列 + 恢复/换 leader 无条件重建活性列**"（非"快照排除活性列"，后者 online-backup 做不到）。
- 每个带 `CURRENT_TIMESTAMP` 默认的 Apply-owned 表的运行期测试：断言 Apply-path INSERT 总显式赋值、无副本持 `CURRENT_TIMESTAMP`（§13）。
- **[→正文 §7.2]** REGISTER 加第 6 个 `<epoch>` 字段（v2 wire break，parser 收正好 6）；`tunnelTokenLookup` 新变体过滤 `home_broker==self` + port + 返回 epoch 比对；`home_catching_up` 双端同版本上线。已写进 §7.2(b) 的 wire 契约。
- **[→正文 §7.2c/§8.2]** catch-up 谓词是**服务节点本地 `applied_index >= barrier index`**（follower-local），leader ReadIndex 只用来取一个新鲜 barrier 值。已写进 §7.2(c)/§8.2。
- **epoch vs barrier 张力**（已 flag）：保留 epoch——它堵 ReassignHome 应用间隙里 agent 在未应用的旧 home 重 REGISTER（旧 epoch 过 token-hash）→ 同口双绑；要求 agent 拨新 home **前**先 cancel 旧 supervisor（Open-replace），不复现旧 epoch；加"ReassignHome 在途 + agent 抢在未应用旧 home 重 REGISTER，断言同口不双绑"测试（§7.2b/§7.3）。
- **[→正文 §4.1]** forwarded-write 幂等的"提交后 `ErrLeadershipLost`"歧义：发起 broker/客户端须带**跨重试稳定的幂等键**（非 leader 铸）让新 leader 去重已提交 entry。已写进 §4.1 fail-closed 段。
- per-stream 重配规模：上百 session ×2(OBJ_xfer) 顺序触碰数百流——封顶/并行 + status 如何枚举每流副本态（缓存+重配时失效，或一次 JS meta 查 under-replicated）+ 卡死单流的运维行为（§6.4/§9）。
- 对抗性安全测试构造（非仅断言结果）：member/agent JWT 在 NATS ACL 层被拒 pub/sub `apply.*/cluster.*`；**普通 bus credential 被拒 pub `cluster.apply.*`、broker nkey credential 被允许**（RF1 broker-only ACL，非"若保留签名"——已无 apply 签名）；并发 ex-home+new-home 同口 REGISTER 恰一绑 + loser 得 `home_catching_up`(非 terminal)；撤销后立刻在落后 peer 重认证在界内被拒（§13.8）。
- `cluster init --from-existing` 半跑/失败的幂等/回滚契约（哪些 `.bak` 恢复、半写 `raft/` 怎么办）+ 已半初始化的 preflight 拒绝（§11）。
- `node_ident_pub` 的带外确认（运维比对入群机 console 打印的指纹）+ status 暴露各节点指纹防替换（§4.2/§8.1）。
- "grow to 3" 的具体可复制 secret 分发 runbook（文件→路径+权限、"经可信信道 scp / 勿提交"、新机 `cluster doctor` 全绿且 account.nk 指纹匹配后才 `cluster add`，否则 fail-fast）（§8.3/§15）。
- psk-FDE 连贯性：proxy 既移出 v1 不复制，则"明文复制到 N 盘"的 FDE 理由在 v1 不成立——要么按单 home 威胁模型重述 `psk_at_rest_unprotected` 硬门、要么降为 advisory；并明示 online-backup 输出与 `.bak` 是否含明文 psk/pin_hash、要求同 FDE 卷（§16.6/§0/§11）。
- 中途 home 死的客户端可见契约：客户端收可重试错误 / 观察到 abort 须重发（非静默 resume）（§9）。
- promote/step-down 作为 `transfer-leader` 的文档化别名（CLI 提示"did you mean"）；标注需求 §12 的 alert/cluster 命令分组**已由 §8.1 表解决**（§8.1）。

> **收敛说明**：三轮内审共提 ~140 条（去重后 50/38/52），跨轮修复了包括我两处过度简化在内的全部架构级问题；第 3 轮余下多为实现精度，已归本节路由 plan 阶段。架构层视为 settled，**待外审对"此边界"签字**。

---

## 19. 开发 phase 分解（D0–D9，依赖分明）

> 体例同主项目 `docs/architecture.md` 的 P0–P11：**"树不能先结果再发芽"——每个 phase 的"出口"是下一个 phase 的前提，未过不进下一阶段**。前缀用 **`D`（distributed）**，避开主线 `P0–P13`。每个 D-phase 至少一个 PR、分支 `phase/d<N>-<slug>`。所有"做/测试"以 §0–§18 正文为唯一实现尺，本节只给**切分与顺序**，不重述契约。
>
> 起点 = 现网单点 broker（v0.3.5，proto v1，`go.mod` 仅 yamux 无 raft）。终点 = N≥3 quorum HA（§0 北极星）。**两个里程碑硬节点**：D2 出口 = **N=1 与今天功能等价**（§0）；D9 出口 = **N≥3 HA GA**。

### 依赖图（根→叶）

```
D0  前置门 + proto v2 SSOT + migrations 0008–0010        ← 地基（无运行期行为）
 │
 ▼
D1  状态层：Raft FSM + SQLite Apply + 快照/恢复 + kill-9   ← 共识心脏（单节点 N=1 raft）
 │
 ▼
D2  op 集 + 全 mutator Plan/Apply 移植                     ← 里程碑：N=1 功能等价
 │
 ▼
D3  NATS 集群层：≥2 节点 routes + auth_callout 本地读 +     ← 多节点信任面
    fail-closed(T_fence) + broker-only ACL(apply 发布者)
 │
 ▼
D4  写转发 apply.*（follower→leader）+ 跨重试幂等 +         ← 控制写分布式
    ReconcileBatch leader 权威
 │
 ▼
D5  审计可重导（leader 单写 + dedup + sweep）+              ← 观测/审计 HA
    JS per-session 流副本重配（replicasFor + UpdateStream）
 │
 ▼
D6  数据面分布式：per-expose home + server_id 桥接指派 +     ← 公网面 HA
    REGISTER 6-field epoch + cert_pins 钉证/轮换 +
    home_catching_up + catch-up barrier + 自驱 rehome
 │
 ▼
D7  集群生命周期：init/add(membership 两阶段+join PoP)/     ← 运维面 + 逃生
    online/drain/retire + status/doctor(双视图,--json) +
    force-single(offline+self-fence+runbook)
 │
 ▼
D8  文件传输分布式（终结于 home + tier-B 副本）            ← 叶子（D8a/D8b 可并行）
    ‖ 告警系统（Raft 复制 alerts + 客户端合成 gating + banner）
 │
 ▼
D9  一次性迁移 + 在位 nats.conf 接管 + 发布硬化            ← HA GA / 现网切换
```

### D0 — 前置门 + proto v2 + migrations
**目标**：依赖与 wire/schema 地基就位，无运行期行为变化。
**做**：§3.1 pin `hashicorp/raft`+`raft-boltdb/v2`+`FileSnapshotStore` 确切版本、`CGO_ENABLED=0`+Go1.25 编译、`Config.PreVoteDisabled` 字段在（v1.7.x **无 `Config.PreVote`**；`DefaultConfig` 缺省启用 pre-vote）；§proto 建 `tether.v2` subject grammar SSOT + `ProtoVersion=2` 常量（§16.2 硬升级）；§4.2 migrations 0008（`cluster_nodes` 含 `nats_server_id`+`cert_fp/_prev/_valid_until`+`phase`）、0009（`cluster_meta`+`alerts`+`alert_acks` 集群级 PK、**无 `cluster_revoked_identities`**）、0010（`port_allocations` 加 `home_broker/rebuild_on_failure/epoch`+索引）。
**测试**：§13.1 确定性 lint 骨架可跑；migrations 前向应用幂等（按名只跑一次）；proto v2 golden JSON 回环；`go build ./...` 绿。
**出口**：raft 依赖编译通过且 **PreVote 实测生效**（§3.1/§18.2.14 合并门：分区一个 follower，断言**被隔离节点 term 不抬高** + 健康 leader 不下台；反向对照 `PreVoteDisabled=true` 时该 term 抬高，证明判别力）；0008–0010 在内存库前向跑通。

### D1 — 状态层：Raft FSM + SQLite Apply + 快照/恢复（N=1）
**目标**：单节点 Raft FSM 把写经 Apply 落 SQLite，崩溃一致。
**做**：§3.2 写入模型（leader Plan→`raft.Apply`→各副本 Apply 同 txn 写 `applied_index`，零直连 `db.Exec`）+ 读一致性契约；§3.3 Plan/Apply 确定性框架；§3.7 双存储崩溃一致性两不变式（快照 index≤`applied_index`、Apply 对已应用 entry 幂等 no-op）；§3.8 快照=WAL+online-backup（独立只读 handle）+ 恢复（integrity_check+前向 migrations+原子换入）；§3.5 活性列不变式（Apply 永不写 + 恢复/换 leader 无条件重建）。
**测试**：§13.3 快照-恢复-重放确定性；**§13.4 kill-9 崩溃一致性矩阵**（含 BoltDB 超前 SQLite 重导 lastApplied）；§13.5 WAL 并发（持续 Apply + 流式 backup 无 SQLITE_BUSY/一致提交点）。
**出口**：单节点 FSM 容忍 raft 重投已应用 entry；kill-9 矩阵存在且绿（承重门）。
> **状态（2026-06）**：**实现完成 + 内审过**（多专家对抗审查 CONDITIONAL PASS → must-fix「§3.7 #1 不变式有误 + fail-stop」已修，报告 `docs/reviews/d1-review.md`），**待外审**。⚠ 上「做」里的"快照 index≤`applied_index`"按 §3.7/§3.8 的「D1 实现修正」**改述**为"重启不丢任何已提交 LogCommand 变更"+ `FSM.Apply` 对未落库命令 **fail-stop**（原字面不变式被 LogBarrier 尾 / 瞬时 Apply 错破坏）。kill-9 矩阵 fp1/fp2/fp3 + `TestSnapshotThenRestart`/`AfterBarrier`/`FailStop` 落盘。

### D2 — op 集 + 全 mutator Plan/Apply 移植（里程碑：N=1 功能等价）
**目标**：现有所有权威写改走 FSM；N=1 与今天功能等价。
**做**：§5 窄类型化 op 集（无 `GenericRowMutate`）；§3.3 每个自生成 mutator 显式 Plan/Apply 拆分（port/proc/node/session/agentprov）；§3.4 确定性雷区处置（leader 烤时间/token/ULID/seq；`ReconcileBatch` 全序；保留 AUTOINCREMENT）；`ProcGC` 转 leader-local。
**测试**：§13.1 确定性 lint 全开（编译期断言 Apply 不 import rand/ulid、禁 FSM 外 INSERT、禁 Apply 调 `*sql.DB` mutator、列级断言）；**§13.2 多 FSM 逻辑内容等价**（`.dump` 去 schema_migrations / per-table 排序哈希）。
**出口**：**N=1 集群与今天单点 broker 功能等价**（§0）——现网可平滑变 N=1 集群、行为不变。
> **D2 范围定稿（先改正文 · 2026-06 · 详 `docs/reviews/d2-plan.md`）**：D2 = **build + prove FSM 写路径（cutover-ready），不切线上 broker**。上「目标」的"现有所有权威写改走 FSM"指**建成 FSM-routed 写能力并证其与今天逻辑等价**，**不**指把生产 broker 切到 FSM——后者（`broker.New` 内嵌 `cluster.Node`、删启动期写、真实 mutator + FSM 合到单 WAL 库）是 **D9 `cluster init --from-existing` 一次性迁移**（§3.8 line 109 明划 D9，切线上会拉 D9 前移、违反先父后子）。故：① 出口门「N=1 等价」由 **DIFF-1 差分测试**证明（今天直连 mutator vs `Plan*→Node.Apply` 同输入 → 逻辑内容哈希相等，配负向对照），**等价靠测试直驱 `cluster.Node`**、非切生产；② 线上 broker 保留直连 mutator（同时作 DIFF-1 的 golden 臂，保差分非 vacuous）；③ §13.1「禁 FSM 外 INSERT」lint 是 **Apply-reachability-scoped**（证 Apply 面干净）+ **分级**（线上直连 mutator grandfathered 到 D9 cutover）；④ ReconcileBatch 抽**共享纯分类器**（线上路行为不变、op 路烤 batch）。
> **状态（2026-06）**：**实现完成 + 内审过**（Stage A plan `docs/reviews/d2-plan.md`；Stage B 全 13 op + Plan/Apply；Stage C 5 Opus 4.8 对抗内审 `docs/reviews/d2-review.md` → 抓 2 真 bug（PlanAllocate desired-port fail-stop panic、LitText 非 UTF-8 静默腐蚀）+ 等价 vacuity，全采纳修），**待外审/外审中**。承重事实亲手验：**`LitTime=t.String()`（不强制 UTC，调用方传一致时间）**、int64-精度-禁-`Args`、modernc 存绑定 time.Time 为精确 `time.Time.String()`（时区+monotonic 忠实）。门：build/`go test ./...`/`make lint` 0/`TestD2Matrix` -race/p8 e2e 全绿。

### D3 — NATS 集群层（≥2 节点）
**目标**：多节点 NATS 集群 + 本地读授权 + apply.* 发布者边界。
**做**：§1.1/§6.1 natscluster 配置模板化（routes mTLS + 共享 account Issuer + 每台 broker nkey AuthUsers + 确定性 `nats_server_id`）；§6.2 auth_callout 读本地副本 + node fail-closed（统一 `T_fence=10s`，§3.2/§8.4）+ PIN 写转 leader（去 XKey）；**§6.2 RF1 broker-only ACL**（`cluster.apply.>`/`cluster.>` 仅授 broker nkey AuthUsers，generated conf 同含 auth_users 与 static nkey permissions=`PermissionsForBroker()`）。
**测试**：**§13.8 跨真 ≥2 节点 nats 集群**（client 连 A、B 应答 callout 授权；RF1 ACL 正/负向：普通 bus 拒 pub `cluster.apply.*`、broker nkey 允许、member/agent JWT 拒 `cluster.*`）；fail-closed 谓词测试（失联 >`T_fence` 才 fail-closed、界=应用滞后）。
**出口**：双节点跨服务器 callout 授权通；失 quorum 已 provision 读不锁死；apply.* 发布者身份强制。
> **D3 范围定稿（先改正文 · 2026-06 · 详 `docs/reviews/d3-plan.md`）**：D3 = **build + prove ≥2 节点信任面（cutover-ready），不切线上 broker**（同 D2 R1）。即建成「真 mTLS raft `NetworkTransport` + 静态多节点 bootstrap + `internal/natscluster` conf 渲染器（routes mTLS + 共享 account Issuer + 每台 broker pub 进每台 `auth_users` + static nkey permissions=`PermissionsForBroker()` 含 RF1 + 确定性 `server_name` + 单 JS domain）+ 单一 fail-closed `T_fence` 谓词（`cluster.Node.LeaderContactStale`）+ auth_callout 已 provision 读=本地副本读经谓词门 + PIN 写经 `Node.Propose`」并以**真 ≥2 节点测试**证之；生产 `cmd/tether/serve.go` 不构造 `cluster.Node`、handler seam 缺省 no-op（零回归，guard 测试锁）。**不在 D3**：`apply.*` 写转发（D4）、动态 membership/AddVoter/join-PoP（D7）、`cluster_nodes` 写（D7 首写）、rehome/per-expose home/server_id 桥接（D6；D3 只渲染确定性 `server_name` 不写库）、生产 cutover/nats.conf 接管（D9）。跨服务器 `$SYS.REQ.USER.AUTH` callout 经真 routes **已实证可行**（R0 spike，§6.1 假设成立、无需 co-located fallback）。RF1 subject 是 **version-prefixed** `tether.v2.cluster.apply.>`/`tether.v2.cluster.>`（SSOT `internal/proto/subjects.go`，与 §4.1 转发 subject 一致），仅授 `PermissionsForBroker()`。
> **状态（2026-06）**：**实现完成 + 内审过 + 外审 round-1（F1）已修**。Stage A plan `docs/reviews/d3-plan.md`（5-drafter+5-critic+1-synth 对抗 workflow + 主进程亲手核验全部 raft v1.7.3/nats v2.14.0 API）；Stage B 含 R0 去风险 spike（跨服务器 callout 经真 routes 实证）；Stage C 5+1 对抗内审 `docs/reviews/d3-review.md`（3 BLOCKER + 6 MAJOR 全采纳修）；**外审 round-1 F1**（`Node.Propose` 先在 follower 陈旧副本跑 leader-only Plan → 漏永久 `ErrSessionMissing` 而非 transient `not_leader`）**已修**：`Propose` 加 leader 门（非 leader 先返 `raft.ErrNotLeader`，绝不在 follower 跑 Plan），报告与逐条回复见 `docs/reviews/d3-external-review.md`。门：`go test ./...`/`make lint` 0/`TestD3Matrix` -race/全 phase e2e 全绿。

### D4 — 写转发 apply.*（follower→leader）
**目标**：任意 broker 收 session 控制写 → 转 leader Apply。
**做**：§4.1 入口校验 → broker-only `cluster.apply.<verb>` 转 leader（mTLS routes + broker-only ACL，无转发签名）；not_leader typed fail-closed deny + 客户端重连；**发起 broker 铸跨重试稳定幂等键**（非 leader 铸，§4.1）；G.1 reconcile 全程 leader 权威（整个 `reconcileOnRegister` 算成一条 `ReconcileBatch` entry、全序烤入、审计可重导）。
**测试**：§13.7 PIN-join 撞选举/转到刚下台 leader 返回可重试 deny 不假放行；转发幂等（提交后 `ErrLeadershipLost` 重试不重复执行）；`ReconcileBatch` 任新 leader 只读 entry 字节一致重导。
**出口**：follower 收写经 leader 落库；旧 leader 拒写（`ErrLeadershipLost`→fail-closed）。

> **D4 范围定稿（先改正文 · 2026-06 · 详 `docs/reviews/d4-plan.md`）**：D4 = **build + prove，不切线上**（同 D2/D3 R1，cutover=D9；`serve.go` 字节不动、不构造 `cluster.Node`、PIN seam nil、forward responder 不 subscribe，guard 测试 `TestD4ProductionWiresNoClusterNode` 锁死）。两正交机制 = 内容寻址 ReqID 写转发 + 同-Apply-txn 去重 ledger（migration 0011 `cluster_reqid_ledger`），外加 ReconcileBatch leader 权威自足 entry（`commandVersion`→2、烤有序审计元组、纯 `ReplayReconcileAudit`、D4 发布零）。范围限 `provision`/`join`/`reconcile` 三动词。新建首个**同跑路由 NATS + mTLS raft 写**的 `test/d4` harness（D3 无此组合）。详见 §4.1「D4 实现定稿」全部裁定（幂等键推导、`appliedDedup` 哨兵、确定性 GC、typed not_leader/超时 fail-closed、D4↔D5 审计边界）。

### D5 — 审计可重导 + JS 流副本重配
**目标**：审计/对象流跨 leader 死可重导、跨节点冗余。
**做**：§6.3 leader post-commit 单写 `ev.*`/`audit.*`/`sys.events` + 审计=committed entry+`raft_index` 纯函数 + dedup 窗口 > 选举+扫尾 + 选后幂等 sweep；§6.4/§9 `history-<sid>`/`OBJ_xfer-<sid>` `Replicas=replicasFor(nVoters)` + already-in-use 分支 actual<target 即 `UpdateStream`（等新 nats 入 JS meta-group）+ 全流达 target 前禁 retire + `replication_degraded`。
**测试**：**§13.9（原 8）扩容/审计**：1→3 后每流副本到 target 才 online；kill-during-expand 无审计丢失；leader 提交后审计发布前崩 → 新 leader 精确补发不重复。
**出口**：N≥3 杀 leader 审计可重导补发；扩容副本达 target 才报 HEALTHY-HA。

> **D5 范围定稿（先改正文 · 2026-06 · 详 `docs/reviews/d5-plan.md`）**：D5 = **build + prove，不切线上**（同 D2/D3/D4 R1，cutover=D9；`serve.go` 字节不动、生产 `Broker` 不构造 `cluster.Node`/不持 node 字段、publisher loop 永不启动、live `publishAudit`/`reconcileOnRegister` 字节不变；guard `TestD5ProductionWiresNoClusterNode` 锁死）。两正交机制：(A) **leader-only post-commit 可重导审计发布** = 复制游标 `audit_published_index`（新 op `OpAuditCheckpointSet`、FSM 烤单调守卫、批量推进/空闲零写）+ 天花板 `CommitIndex()`/地板 `max(checkpoint,LogFirstIndex())` 每轮重夹 + 去重 id `r<idx>:<kind>:<seq>`（reqID-bearing reconcile 改 `q<reqID>:…` 收 `appliedDedup` 重试双发，R-10）+ 纯 `ReplayReconcileAudit`（仅 `OpReconcileBatch` 可重导）+ 选后 sweep 即同 loop 大 gap；(B) **JS 流副本重配** = `replicasFor(nVoters)`(1→1,2→2,3→3,封顶 3，住 jsstream、L-2) + 三族 `UpdateStream`/`UpdateObjectStore` 只升不降 + target=`NumVoters`、升配门=`UpdateStream` 拒分类 `ErrMetaGroupNotReady` 重试（`StreamInfo.Cluster` 仅算 actual，非前置门）+ `AllAtTarget`/`Degraded` canonical 预测语（D7 消费、D8b 写 alert）。新增 `cluster.Node` raft-free 原语（`CommitIndex`/`LogFirstIndex`/`CommittedCommandAt`/`AuditPublishedIndex`/`AdvanceAuditPublished`/`NumVoters`）+ 新 `internal/broker/audit_publisher.go` 合一 leader-only goroutine。新建首个**路由 NATS + mTLS raft + clustered JetStream** 三合一 `test/d5` harness。三道 guard：build-and-prove token-scan、`internal/cluster` no-NATS import 门、`internal/jsstream` no-`cluster` import 门。**硬非目标**：`cluster status`/create/online/drain/retire CLI（D7）、数据面 rehome（D6）、serve.go `cluster.Node`（D9）、审计体 epoch header（D8）、`replication_degraded` alert 行写（D8b）。N=2→R2 与 §17 矩阵既有取值一致。

### D6 — 数据面分布式
**目标**：expose/tunnel 跨 home broker 故障切换。
**做**：§6.5 server_id 桥接权威 home 指派（agent 自报 server_id、leader 据 `cluster_nodes` 指派、`NodeRegisterResp.Home`）；§7.1–7.2 per-expose `home_broker/epoch` 唯一映射 + `tunnelTokenLookup` 本地读 + epoch 防双绑 + `home_catching_up` 瞬态 DENY（v2 wire + agent `denyIsTransient` 同改）；**§7.2(b) REGISTER 第 6 字段 `<epoch>`（parser 收正好 6）**；§7.2(c) catch-up = follower-local `applied_index>=barrier`；§7.4 agent 自身 NATS 重连自驱 rehome（leader 推 backup）；§7.5 **per-expose brokerAddr**（一 Client 扇出 N home）；§7.7/§15 **稳定 tunnel cert + `cert_pins{current,previous,valid_until}` 钉证 + wire-list 双 pin 轮换**。
**测试**：**§13.6 per-expose brokerAddr + 一 agent N expose 分散 + mass-rehome 风暴**（`-race`+`goleak`，有界 re-REGISTER、瞬断重拨新 addr、home_catching_up 不塌 terminal）；**§13.7 cert 重启不变性 + 轮换窗口内 agent 重启**；并发 ex-home+new-home 同口 REGISTER 恰一绑。
**出口**：杀 home → agent 自驱 rehome 到新 home、瞬断重连、不双绑；broker 重启 cert_fp 不变。

> **D6 范围定稿（先改正文 · 2026-06 · 详 `docs/reviews/d6-plan.md`）**：D6 = **build + prove 公网数据面故障切换（cutover-ready），不切线上**（同 D2–D5 R1，cutover=D9；`serve.go` 字节不动、生产 `Broker` 不构造 `cluster.Node`/不发 `HomeDirective`/tunnel server 不给稳定 cert、`port.Allocate` 直连 mutator 字节不动；guard `TestD6ProductionWiresNoClusterNode` 扫 `serve.go`+`internal/broker/*.go`(除 `home.go`)+**`internal/agent/*.go`** 锁死）。七机制：(1) server_name 桥接 home 指派（agent 报 `ConnectedServerName`、leader 据 `nodes.nats_server`→`clusternodes.LookupByNatsServer`→VOTER 行指派、`NodeRegisterResp.Home`+`ExposeForwardedReq.Home`(C1 修)) (2) per-expose `home_broker/epoch` + `OpPortReassignHome`(leader 烤字面 epoch+1 + CAS 守卫) (3) `tunnelTokenLookup` 二维 ladder(home×epoch、`home==''` inert) (4) REGISTER 第 6 字段 epoch(收正好 6、`ParseInt`) (5) catch-up = **epoch-as-local-row-epoch**(非 applied_index>=raft-commit、域不兼容) (6) agent 自驱 rehome(并发 `Open`-replace、`home_catching_up` transient 重排、`denyIsTransient` 共享 const、state.json 持久 home addr/epoch) (7) 稳定 cert + `cert_pins{current,previous,valid_until}` 钉证(`VerifyConnection`、fp=`sha256:hex(cert.Raw)`、轮换窗口)。新建首个**多 broker(routed NATS+mTLS raft) + 真 tunnel.Server(稳定 cert) + agent failover** 的 `test/d6` harness（gated `//go:build d6_integration`、跑在 `TestD6Matrix -race`；廉价 guard/ladder/cert-verify/wire 单测在 `make test`）。新 `internal/clusternodes` 纯 SQL 包(守 L-2)。**硬非目标**：`cluster_nodes` 生产写者/`cluster rotate-tunnel-cert` CLI/`cluster add`/`drain`(D7)、serve.go cutover + `nodes.nats_server` 生产 backfill(D9)、alert 行(D8b)、rehome 在途传输续传(不保)、单 expose active-active(禁止不变式)。
> **状态（2026-06）**：**DONE（内审 + 外审 round-2 re-review PASS，commit `d403f38` + pushed main）**。Stage A plan 定稿（`docs/reviews/d6-plan.md`；11×Opus-4.8 对抗 workflow、主进程核验全 API + 裁决 5 OQ，含否决 synth 的 in-memory 绑定改复制态 R-26）；doc-first 回写完成（本节 + §6.5/§7.2/§7.7/§15/§16）；Stage B 实现（7 机制 build-and-prove）；Stage C 6×Opus-4.8 对抗内审 `docs/reviews/d6-review.md`（全 CONDITIONAL PASS → 主进程逐条采纳修复：1 并发 BLOCKER `applyOneHome` 重连风暴 goroutine 无界 → 按口去重 + leak 门；ladder L-1 "恰一绑"过强宣称 → 纠正为有界自愈 cutover 窗口 + 文档化；L-2 共享 DB harness 掩盖初始 home 复制链 → 补 `TestD6InitialHomeReplicatedLadder`；L-4 `applyOneHome` no-op 持久低 epoch → `UpdatePortHome` 单调；空 cert_fp/负 epoch 硬化；guard 扩字段写/结构字面量 token + 剥注释；nats_server DIFF-1 非空双写证 + homed PlanAllocate apply 等价 + TLS resumption VerifyConnection 锁定 + agent rehome 单测全补）。门：`make test`/`make lint` 0/`TestD6Matrix -race`/全 phase e2e 全绿。过程抓修一**真死锁**（嵌套查询 under `MaxOpenConns(1)`）。**外审 round-1 Fail（4 BLOCKER/High）已逐条修**（`docs/reviews/d6-external-review.md`）：F1 `handleRegister` 漏接 `req.ServerID`→`nodes.nats_server`（home bridge 整条死，已接）；F2 deferred clustered replay 拿 pins 后不重开（agent `deferredReplay` 集合 + `openHomeFromState` 从 state.json 开，非 ApplyHome no-op）；F3 stale terminal rehome 删同口已排队的更新 epoch（terminal/deadline 退出前 `hasNewerWant` 重检 continue）；F4 same-epoch pure-pin 轮换被 ApplyHome 当 no-op（ApplyHome 三分支：< no-op / == 原地更 pins 不撕 / > rehome；redial gen-fenced 读会话当前 pins）。4 个 reviewer regression 转绿、硬闸复绿。**外审 round-2 re-review 又抓 2 个（我 round-1 引入的）已修**：RF1 deferred replay 在 state.json 不可读时被当成功清 marker（`openHomeFromState` 改 tri-state：unreadable→保留 deferred 退出、port-absent→清、ok→分类）；RF2 同 epoch 新 pins 排在运行 loop 后被 `epoch>` 丢（引入每口 `rehomeSeq`，退出判定改 seq 变化、统一覆盖 F3+RF2）。6 个 regression 全过、硬闸复绿。**待外审 re-review；外审不过不算 done、未 commit。**

### D7 — 集群生命周期 + 逃生
**目标**：`tether cluster *` 全命令面 + 安全的成员变更与 force-single。
**做**：§8.1 命令面（admin 严格本地、非 leader fail-fast 指名 leader）+ **membership 两阶段**（阶段 1 `ClusterNodeUpsert{join PoP}` follower Apply 复算 → 阶段 2 `raft.AddVoter`、半成功 phase 状态机、Remove/retire 顺序、幂等）；§8.2 catch-up 闸；§8.3 create/online/drain/retire（drain 迁 expose + quorum 投影守卫 + secret 全套分发）；§8.4 **force-single offline 子命令 + self-fence(`T_fence`) + runbook（`systemctl mask`/空缺拒/调和两存储/peer 可达 hard-refuse）**；§17 `status/doctor` 双视图（ctl/NATS vs broker offline）+ `--json` + 退出码。
**测试**：**§13.11 membership 两阶段**（roster committed 后 AddVoter 失败 / catch-up stalled / 重复 add → 状态机幂等无"DB voter/Raft 非 voter"分叉、伪签拒）；**§13.12 force-single→recover 3 节点实跑演练** + offline 磁盘锁防并发；§13.13 `cluster status --json`+退出码契约；`cluster add` 半失败清理。
**出口**：3 节点 add/drain/retire/leader 切换走通；quorum 丢失下 force-single→recover 演练过；status 双视图不误导。

> **D7 范围定稿（先改正文 · 2026-06 · 详 `docs/reviews/d7-plan.md`）**：D7 = 三层不同可证性。**(A) ONLINE**（`cluster add/remove/drain/retire/transfer-leader/rotate-tunnel-cert`、ctl/NATS `status`/`doctor`、online adminsock ops、新 `ClusterNode*` ops、`Node.AddVoter/RemoveServer/LeadershipTransferToServer`、两阶段 orchestrator + leader-startup reconciliation）= **build + prove，不切线上**（同 D2–D6 R1，cutover=D9；`serve.go` 字节不动、生产不构造 `cluster.Node`、online cluster 子命令在 adminsock cluster backend 为 nil 时 fail-fast "cluster mode not enabled"；guard `TestD7ProductionWiresNoCluster` 锁死，orchestrator/responder 机制文件 `internal/broker/clusteradmin.go` 排除扫描）。**(B) OFFLINE**（`force-single`/`recover` + offline `status`，停机操作磁盘 `raft/`+`tether.db`）= **真实生产可发**（逃生闸无 cutover；D9 只加 `--from-existing`），由 harness 造真磁盘态证。**(C) inert-until-D9**（`serve.go`/`broker.New` 构造 `cluster.Node`、注册 adminsock cluster backend、daemon 端取 disk lock）。**核心机制**：(1) **membership 两阶段** = 阶段 1 业务 op `OpClusterNodeUpsert`（**唯一不走 `genericExecApplier` 的自定义 applier**：follower 在 Apply 复算 join-PoP ed25519 验签，**失败=POISON 跳过推 applied_index、绝不返 applier 错误**——返错会触发 §3.7 重试→panic 把沦陷 leader 的伪条目变成全集群 brick；新 `errAppliedRejected` 哨兵小改 `fsm.go`）→ 阶段 2 leader-local `raft.AddVoter`（非 FSM 校验点、仅阶段 1 committed 后）；半成功 `phase` 状态机（`JOIN_VERIFIED_PENDING_VOTER`→`CATCHING_UP`→`VOTER`、失败 `VOTER_ADD_FAILED`）；**no-silent-fork = 提交序 + leader-startup reconciliation pass（据 committed phase 列 + 活 `RaftConfiguration()` 幂等调和，非 in-memory leader 态）+ status 渲染**（render 是展示非安全属性）。验签 = `internal/auth.VerifySignature`（cycle-free、无 nats.go，`cluster` 直接 import；`clusternodes` 只放纯 SQL Plan helper）。canonical 签名消息绑 `(domain-sep, node_id, node_ident_pub, join_nonce)`、**不绑 cluster-id**（fresh leader-issued nonce 已防跨集群重放）；nonce 单用=leader-local 预提交（一致性非安全边界，§18.2.4 接受沦陷 leader）。(2) **catch-up = command-domain** barrier（leader 在 AddVoter 时经 `VerifyLeaderRead` 取 `AppliedIndex()`；voter 本地 `AppliedIndex()≥barrier` 持续墙钟、max-wait→`catch_up_stalled` 派生态非第 7 phase）——**非 raft `CommitIndex`**（command 游标永达不到 all-entry barrier、每次 join 会 hang）。(3) **drain/retire** = quorum 投影守卫（serving-set 容错，F==0 含 plain-drain-at-N=2 触 TTY+typed confirm、拒 `--yes`）+ AllAtTarget 流守卫（retire 前）+ D6 rehome 迁 expose（rebuild-OFF 列举 + typed confirm）+ leader 先 transfer + §8.1 移除序。(4) **force-single offline** = 两机制 disk 互锁（`flock(2)` `${DataDir}/tether.lock` 防两 offline + 探 `raft/raft.db` BoltDB 排他锁防活 daemon——daemon 端取锁是 D9）+ (b) 空态拒 + (d) peer **TCP-completes** hard-refuse（漏列任一非 self 节点即拒）+ (c) **`raft.RecoverCluster({self})` 自身驱动**两存储调和（恢复点=本地 `LastIndex()`、不手搓 `ExecCommand` 前置重放=会双应用）。(5) **status 双视图**：ctl/NATS（不拨 :7400、NATS 不可达=UNKNOWN 非 DEAD、exit 2 仅来自肯定 self-report；`reach_source` 判别）vs broker offline（直拨 :7400）；`--json` versioned schema + exit 码 `0/1/2/3`。migration **0013**（`cluster_nodes` 加 `join_nonce/join_sig/voter_add_error/phase_changed_at`，全 nullable）。新建 `test/d7` harness（gated `//go:build d7_integration`、`TestD7Matrix -race`，多节点 membership + force-single + status 双视图实跑；廉价 op/applier/offline-precondition/status-fixture 单测在 `make test`）。新 `internal/clusteroffline`（编排，调 `internal/cluster.RecoverSingleNode`，**不直接 import raft**——raft 仍限 `internal/cluster`）。**硬非目标**：replicated alert store（D8b）、daemon 端 disk lock（D9）、`cluster init --from-existing`（D9）、account.nk/CA 轮换工具（D9，D7 只出 runbook + retire 诚实警告）。
> **状态（2026-06）**：**Stage A 定稿 + Stage B 实现 + Stage C 内审过，待外审**。Stage A `docs/reviews/d7-plan.md`（4×Opus-4.8 对抗 workflow=4 drafter/4 critic/1 synth，synth 轨迹 `d7-plan-synth.md`；主进程裁决 6 OQ + 亲手核验 `auth.VerifySignature` 无环/`fsm.go` panic 路径/`RecoverCluster` 重放语义/`ExecCommand` 不推 applied_index/`commitIndex` 不落盘）；doc-first 回写本节 + §8.1/§8.2/§8.4/§17。Stage B 按 §K riskiest-first 全实现（migration 0013 + 5 op + 自定义 applier + errAppliedRejected plumbing + membership wrappers + 两阶段 orchestrator + reconciliation + clusteroffline force-single/recover + guard + drain/retire + adminsock + status 双视图 + cobra CLI）。Stage C 6×Opus-4.8 对抗内审 `docs/reviews/d7-review.md`：5 BLOCKER（B1 bare-remove 静默 fork / B2 约束 panic-loop / B3 recover 确认串 / B4 dump 漏表数据丢失 / B5 drain-leader 半完成）+ 10 MAJOR + 12 MINOR **全采纳修复**，核心机制（poison-skip/RecoverCluster replay/L-2/MaxOpenConns/build-and-prove guard/catch-up 命令域）审查确认 sound。门：`make test` exit 0 · `make lint` 0 · gated `TestD7Matrix -race`（7 drill：dynamic-join 复制/forged-sig-on-follower/no-silent-fork/force-single→recover-strengthened/reconcile-pending-voter/drain-retire/drain-leader）· `make e2e-parallel`（TestAllPhases + TestD7Matrix）全绿。**外审 round-1 Fail（5 finding）已逐条修**（`docs/reviews/d7-external-review.md`）：F1 transfer-leader 忽略目标→targeted `LeadershipTransferToServer`；F2 rotate-tunnel-cert stub→实装 `OpClusterCertRotate`；F3 drain 静默迁 rebuild-OFF→按 `rebuild_on_failure` 拆分+拒绝；F4 status 缺 `schema_version`/offline 不符→加版本字段+offline 同 schema disk 快照（多 broker 聚合+:7400 ping=D9）；F5 runbook recover 改 `--self-id`+typed node_id。5 reviewer regression `TestD7Review*` 转绿。另修 2 pre-existing flake（D6 freePort TOCTOU retry、D5 post-election sweep retry），`make e2e-parallel` 满负载复绿。**外审 re-review PASS**（`docs/reviews/d7-external-review.md`）；committed + pushed main。**DONE。**

### D8 — 文件传输分布式 ‖ 告警系统（两叶子可并行）
**目标**：transfer 与 alerts 补成分布式/HA。
**做（D8a transfer）**：§9 终结于 home（**数据面 broadcast-SUB + home-keyed gate，非 §4.1 转发**）+ transfer audit start/complete/failed 经 leader Apply（新 `OpTransferAudit` 纯-Aux 空-Body、`xferaudit.ReplayTransferAudit` 可重导、`reqID=hex(sha256(tid:kind))` 经 0011 ledger 幂等）；tier-B `ensureXferBucket` `Replicas=replicasFor(nVoters)` + 重配（**retire 门读 JS 实际 `OBJ_xfer-*` 流列表、只读 `ObserveReplicas`**）；in-flight tracker/watchdog 在 home best-effort（rehome 不保在途、客户端重启；boot orphan reaper home-ownership 过滤）。
**做（D8b alerts）**：§10 leader 复制 `alerts`/`alert_acks`（集群级单 ack + 严重每新会话重现）+ **两条** severe 客户端合成 gating（`quorum_lost`/`force_single_active`、server 佐证、VerifyLeader-confirmed、零应答不 gate）+ §10.2 目录（桥接 `disk_pressure`=follower level-triggered + `disk.go` clear-edge；**`raft_lag` writer 推 D9**）+ §10.3 banner（所有人、客户端组装、stderr、仅 SEVERE）+ §10.4 destructive 闸枚举（硬闸仅两条；`replication_degraded` store-backed severe 不闸）。alert raise/clear=独立 leader-gated loop（非折进 publisher tick）。ACL carve-out=member-reachable `ctrl.by.<actor>.*` 健康/alert RPC。
**测试**：**§13.10（原 9）transfer**：tier-B 对象 N≥3 杀 home 存活、ex-home 在途不双绑、home 死在途重启；alerts：失 quorum 客户端合成阻断 destructive、纯抖动不阻断、集群级 ack 行为。
**出口**：tier-B 对象杀 home 存活；severe 告警正确 gate destructive 且不误伤。
> **D8 范围定稿（先改正文 · 2026-06 · 详 `docs/reviews/d8-plan.md`）**：build-and-prove 不切线上（cutover=D9；`serve.go` 字节不动、生产 `Broker` 不构造 `cluster.Node`、`transferAuditSink`/`alertSink` nil、`xferTargetReplicas→ReplicasSingle`；guard `TestD8ProductionWiresNoCluster` 锁死）。**无新 migration**（0009 alerts + 0011 ledger 复用）。Stage A 多专家对抗 workflow（4 drafter/4 critic/1 synth、全 Opus 4.8）→ 主进程定稿，核验承重事实（broadcast-SUB fan-out、publisher silent-skip、claimFinalize 单终态、Degraded/AllAtTarget、disk 无下降沿、leader 不读 follower 游标）+ 裁决 5 OQ。doc-first 回写本节 + §9/§10.1/§10.2/§10.3/§10.4/§6.3/§6.2/§13.8/§16。
> **状态（2026-06）**：**DONE（内审 6×Opus + 外审 round-1 Fail→re-review PASS，committed + pushed main）**。Stage A 定稿（`docs/reviews/d8-plan.md` + `d8-plan-synth.md`，4 drafter/4 critic/1 synth 对抗 workflow + 主进程亲验承重事实 + 裁决 10 OQ）；doc-first 回写（本节 + §9/§10.1-10.4/§6.3/§16/§13.8）；Stage B 实现（9 机制 build-and-prove）；Stage C 6×Opus 对抗内审 `docs/reviews/d8-review.md`（核心机制 SOUND 无状态机 BLOCKER；1 doc-BLOCKER + 4 MAJOR + 5 MINOR 全采纳修、1 MINOR 驳回 + 另修 3 满负载 flake）。**外审 round-1 Fail（F1–F4）已逐条修**（`docs/reviews/d8-external-review.md`）：F1 push-commit/finalize tracker-miss clustered 静默（用 `selfNodeID()` 避 D6 guard 误匹配）、生产保 `transfer_unknown`；F2 `gateDestructive`+`--ack-alerts` 接全 NATS-侧破坏性命令（session rm/push/pull/expose/expose-rm/run）；F3 `ackAlert` fail-closed（仅 `"ok"` 成功）；F4 拆 strict `fetchAlertsStrict`（显式 `alert ls` 失败返错）vs lenient banner。**外审 re-review PASS**（4 finding 闭合、reviewer 加 F4 strict-ls 回归、无新 blocker）。门：`make test`/`make lint` 0 · gated `TestD8Matrix -race`（5 drill 含 EXIT-A tier-B 杀 home 存活）· `make e2e-parallel` 全绿（13 矩阵 0 FAIL，另硬化 D5 setup/D7 AddNode/proxy 三满负载 flake）。**DONE。**

### D9 — 一次性迁移 + nats.conf 接管 + 发布硬化（HA GA）
**目标**：现网单点平滑切 N=1 集群、可长成 N≥3、文档对齐、发布。
**做**：§11 `cluster init --from-existing`（前向 **0008–0013** + `BootstrapCluster({self})` 末步 + 迁移后 DB 作 index-0 快照 + 删 `broker.New` 启动期写）；**§11 tether 接管 `nats-server.conf`**（`.bak` + before/after 归属表 + preflight 遇不认识指令拒接管 + 与 install.sh 产物对账）；**同 PR 改写 `broker-ops.md` §2.3/§3.4**；§15 provisioning 全套 secret 分发 runbook + FDE preflight；§8.3 account.nk/CA 轮换 runbook（retire 安全闭环）；**真 cutover**：cluster 模式 `serve.go`/`broker.New` 构造 `cluster.Node` + 接 D2–D8 全接缝（adminsock cluster backend、audit publisher/alert reconciler loop、transfer/alert sink、disk lock）；**完整 §17 矩阵全闭**（含观测行 `raft_lag`/`broker_down`/多 broker status 聚合/offline :7400 ping，无 D9.1 split）；K/sec mass-reconnect 限流器 + guard 拆除换两模式不变式 + README 门面。
**测试**：**§13.12** 3 节点实跑全演练 + **mass-reconnect e2e 门**（§18.2.18：上百 agent 弹连撞选举中 leader）；§13.13 FDE preflight；`cluster init --from-existing` 半跑/回滚幂等；**新 phase 全进 e2e 矩阵**（回填主项目 e2e 洞）。
**出口**：现网单点一次性迁成 N=1 → `cluster add` 长到 N≥3 → **N≥3 quorum HA 全部 §17 保证矩阵达成**（安全 + 观测行全闭，无 split）；usage.md 对齐；GitHub release（proto v2 tag，非 v1 线）。
> **状态（2026-06）**：**Stage A 定稿 + doc-first 回写完成，进 Stage B 实现**。Stage A `docs/reviews/d9-plan.md`（5 drafter/5 critic/1 synth 对抗 workflow + 主进程亲验全部 cutover 接缝/迁移/nats.conf 现状 + 裁决 6 OQ + AFFIRM 4 个被误当开放的已定契约）；synth 轨迹 `d9-plan-synth.md`。**用户裁定（2026-06-23「最终 phase 做完所有事」）：完整 §17 矩阵全 IN、无 D9.1 split。** riskiest-first：步骤 1–7 在测试里证全接缝（不碰 live box）→ 步骤 8–10b 碰 disk/`/etc`/docs/observability。本节上「做」+ §11 D9 实现裁定为实现尺。

### 里程碑映射

| 里程碑 | 完成时机 | 外部可见度 |
|---|---|---|
| **N=1 功能等价**（共识心脏就位、行为不变） | D2 结束 | 内部（现网可无感切 N=1） |
| 多节点信任面 + 控制写分布式 | D4 结束 | 内部 dogfood（≥2 节点） |
| 观测/审计 + 公网数据面 HA | D6 结束 | 可故障切换演示 |
| 运维面 + 逃生闭环 | D7 结束 | 运维可操作集群 |
| **N≥3 quorum HA GA** | D9 结束 | 现网切换 + release |

### 关键依赖警告（违反即返工）

**不能跳序的**（"先发芽"硬约束）：
- ❌ D1 之前碰任何 `apply.*`/转发 —— FSM/Apply 还不存在，写无处落。
- ❌ D3 之前做写转发（D4）—— 转发需 ≥2 节点 routes + broker-only ACL；单节点无"转发"。
- ❌ D3 之前依赖"auth_callout 读本地副本" —— 本地副本由 D1/D2 的 FSM 填，且 fail-closed 谓词（`T_fence`）此处才定。
- ❌ D6 之前做 rehome/per-expose home —— `home_broker/epoch` 在 D1 复制表里、server_id 桥接在 D3、catch-up barrier 在 D6 自身/§8.2。
- ❌ D5/D6 未完做 D7 `drain`（迁 expose 依赖 D6 rehome、JS 重配依赖 D5）。
- ❌ D2（N=1 等价）未过就上多节点 —— 等价性是一切分布式行为的回归基线。
- ❌ 任何 phase 跳过其 §13 测试门进下一阶段（kill-9/等价/跨真 nats/membership 两阶段是承重门）。

**可并行的**（树分叉）：
- D8a（transfer）与 D8b（alerts）互不依赖，可并行。
- D9 的 `usage.md` 改写在各 phase 完成时增量写，不留到最后。
- 确定性 lint（§13.1）随 D1/D2 建立后，对后续每 phase 持续生效（非一次性）。

### 每进入新 D-phase 的 checklist

- [ ] 前一 D-phase 的"出口"断言全部通过（尤其 D2 的 N=1 等价、D1 的 kill-9）。
- [ ] 本节当前 phase 状态翻 ✔。
- [ ] 新开分支 `phase/d<N>-<slug>`；每个 phase 至少一个 PR。
- [ ] 实现中发现设计问题 **先改 §0–§18 正文再改代码**（§18 为审计轨迹、正文为唯一实现尺）。
- [ ] 单测 + e2e 同 PR 落盘；触碰并发面（隧道/Raft/Apply/重配）带 `-race` + 仓库内建 `NumGoroutine`/fd 泄漏门（**非 goleak**，见 §13「泄漏门约定」）。
- [ ] 提交前硬闸 `make test`+`make e2e-parallel`+`make lint` 全绿；新 phase 进 e2e 矩阵。

> **范围边界**：本分解只覆盖 §0 北极星点名的数据（session/member/node/process/port+审计）的 HA。**P13 proxy / 文件传输 multipath 不在 D0–D9**（§0 显式不做）；待 P13 转无条件 PASS 再单做 "proxy-HA" 叶子。

---

## 20. 批次 C 的绑定契约增补（v0.4.8）

> 本节是**第 2 层实现契约**的一部分（CLAUDE.md §1）。批次 C 的三项改动都是加法式的：
> `internal/proto.ProtoVersion` **未变**，现网 agent **不需要重装、不需要同步升级**。

### 20.1 topology reconcile 的 `Action` 上 wire（C3）

- **新字段**（全部 additive `omitempty`，沿用 `TopoReported` 的"老 broker 不带此字段"惯例）：
  `proto.ClusterHealthResp.TopoAction`、`adminsock.ClusterNodeStatus.TopoAction`。
  `proto.ClusterHealthSchemaVersion` 6→7（该常量自陈是 **documentation ledger, not a compat switch**，
  无消费者 gate 在它上面）。
- **唯一分类器**：`internal/natsconf.ClassifyTopo(action, reason, observed, desired, reported) TopoState`。
  `TopoState ∈ {Unreported, Converged, Behind, Held, Stuck, UnknownAction}`。
  banner / next-step / TOPO 列 token 全部由它派生，broker 与 ctl **共用同一份**。
- **不变量**：`Stuck` 与 `Held` 的判定**不得**被 `observed < desired` 门住。
  `ActionUnknownDirective` / `ActionRejected` 返回的 applied/observed 是**不变的**，
  所以一个"收敛之后才卡死"的 reconciler 满足 `observed == desired`，
  加门会让它报 HEALTHY_HA（这是 C3 修掉的真实缺陷之一）。
- **`Held` 的 next-step 必须带否定子句**：`awaiting_clustered_cutover` 是**故意扣住**的首次
  standalone→clustered 换装，conf 完全正常。改动前它被子串 `render`（Reason 里的 "rende**red**"）
  判成 STUCK，而 STUCK 的 next-step 推荐 `reconcile nats --manual` ——
  那正是 `natsconf/reconcile.go` 写明会造成 G4 #10 / #4 的动作。
- **故意不统一**：`cluster_operation_controller.go` 的 `topoConvergedForOp` 保持 fail-closed，
  **不得**改用 `ClassifyTopo`。它回答的是"能否推进一次不可逆的 membership 变更"，
  对不可达 voter 必须判未收敛；渲染器回答的是"该告诉运维什么"，对不可达 voter 排除。
  两种极性各自正确，统一会让其中一个变错。
- **结构门**：`test/determinism/topo_classification_test.go` 用 AST 断言除分类器外
  没有任何文件把 topology reason 标记传给 `strings.Contains` 一族。

### 20.2 tier-B 传输预算（C2）

- **单一来源**：`internal/proto` 的 `XferTierAMaxBytes`(8 MiB) / `XferMaxBytes`(2 GiB) /
  `XferMinThroughput`(2 MiB/s) / `XferPushLegs`(2) / `XferTimeoutTierA` / `XferTimeoutTierBFloor` /
  `XferTierBMaxBudget`(2108s=35m08s，含 60s setup/finalize margin) /
  `XferLegBudget()` / `XferBudget()`。
  broker、agent、ctl 三端全部引用它，**不再各自声明**（AST 门 `TestXferTierCeilingsHaveOneSource`）。
- **预算式**：tier a 固定 30s；tier b = `max(5min, legs × ⌈size / 2 MiB/s⌉)`。
  broker 覆盖 **2 腿**（发送方 Put + 接收方 Get，看门狗在转发 prepare 之前就武装），
  每一**端**只覆盖自己那 1 腿。`size == 0`（pull 无申报 size / 批 C 之前写下的 ledger 记录）
  ⇒ 回落固定下限 = 改动前的行为。
- **有界**：admission 在武装看门狗之前就拒掉 `size > XferMaxBytes`，
  所以 `XferTierBMaxBudget` 是编译期常量。**无上界的看门狗不是更长的超时，是没有超时。**
- **崩溃恢复必须同步**：`transferTimeoutFor(tier, size)` 与活看门狗同式，
  否则恢复方会给一个仍在传的传输写 `failed`。
- **但 synthetic terminal 的 `Ts`/`DurationMs` 故意留在 tier 下限**：`Ts` 是 dedup reqID 的载体，
  若它依赖 `XferMinThroughput`，回滚后新旧二进制会从同一条 ledger 记录算出不同的 `Ts` ⇒
  同一个 transfer 出现两条互相矛盾的终态。
- **跨 home GC 的下限改为逐对象增量**：`floor(obj) = base + max(0, budget(obj.size) − tierBFloor)`。
  **不抬全局常量**——`serveconf.MinXferCrossHomeReapAge` 是生产 YAML 的**硬拒下限**，
  抬高会让显式设过该 knob 的 broker 升级后拒绝启动；而且增量式保住了 drill 压缩该下限的能力。
- **归因订正**：看门狗超时的 push 路径写 `transfer_budget_exceeded`（新 code，EX_TEMPFAIL），
  不再写 `agent_no_responders`（agent 可能一直在传，只是不够快）。
  `expose` / `upgrade` / `cluster upgrade` 三处的 `agent_no_responders` **保持不变**——那里是真的无响应。
- **agent 侧 prep 缓存按 size 逐条到期**，且容量满时**拒新 prepare 而不是淘汰最旧的**：
  最旧的那条按构造就是跑得最久、最大的那次传输。

### 20.3 force-single 的 prune 重试 op（C1）

- **新 op kind** `force_single_finalize`，ladder：`FS_PRUNE_PENDING → FS_FINALIZED | FS_GHOST_LEFT`。
  **无 migration**（`cluster_operations.kind` 无 CHECK），**不动 ProtoVersion**。
- **同步路径逐字不变**：`RecoverToSelfOnline` → `WaitForLeader` → marker → epoch → prune → seeds。
  三条独立理由要求它保持同步：
  1. destructive gate 是 `QuorumLost || ForceSingleActive`，恢复一返回本节点即可写 leader ⇒
     `QuorumLost=false`；marker 若异步写，中间存在一个**门全开**的窗口。
  2. ghost roster 行没有 raft role，`reconcile nats --to-standalone` 对未知 role **直接拒绝**，
     而 runbook 明写"已 pruned 所以 N=1 通过"——异步化会打断运维的下一条命令。
  3. seeds 每个 leader tick 都自愈，本来就不需要 ladder。
- **op 只在同步 prune 失败时创建**。成功路径**不建 op 行**、CLI 文案不变。
- **不可逆改写之前先落 durable intent**（外审 B1）：`<ClusterDataDir>/.force-single-online.intent`
  在 `RecoverToSelfOnline` **之前** fsync（temp→fsync→rename→fsync(dir)，与 offline journal 同一原语），
  内含 `self_id` / `abandoned` / **epoch**（在此处 mint，每次 resume 复用同一个值）/ `marked_at`。
  它是**本地文件、不需要 quorum**——而这条路径恰恰跑在 quorum-lost 节点上。写不成功就**拒绝开始**改写。
  改写之后的每一步（marker、epoch、prune）都可从 intent 幂等重放；
  intent 只在**全部事实经读回确认**后才清除。
  - **intent 存在 ≠ 改写已发生**（复审 R1）：presence 只证明"恢复被授权"。
    forward-complete 之前必须另证 committed raft config **恰为 `{self}` 单 voter**；
    读到旧配置 ⇒ 这是 pre-rewrite crash，durably 丢弃 intent、不写 raft；读不出来 ⇒ 保留 intent、拒绝猜。
  - **`RecoverToSelfOnline` 返回 error 时保留 intent**（复审 R2）：它可能在 `RecoverCluster` 已落盘之后、
    重建 transport 时才失败，调用方从 error 分不出相位——删掉就重新打开 B1 那个窗口。
  - **两个触发点**：leadership-acquired **边沿**（`ReconcileMembershipOnLeadership`，覆盖重启形态）
    与**每个 leader tick**（`driveLeaderMaintenance` → `driveInterruptedForceSingle`，
    覆盖**没有下一个边沿**的形态——刚恢复的单 voter raft 永不再选举，
    而 commit handler 的 marker/epoch 失败分支正是跑在它上面）。
    per-tick 那条以"两条复制事实是否都已可见"为门，所以 intent 因 ghost 未清而长期存在时它不会每 5s 重刷日志。
  - 进程内 `forceSingleIntentMu`：commit handler 持有新 intent 直到同步 prune 出结果，
    两个 resume 触发点都用 `TryLock`（在 observe loop 上，**不得阻塞**）——
    这保住了"干净恢复不建 op"这条契约。
- **崩溃窗口**（intent 之前的老部署 / intent 文件丢失）由 `ReconcileMembershipOnLeadership` 的
  **substrate 推断**兜底：四条全中才建 op ——
  ① `force_single_active` 已置 ② `NumVoters()==1` ③ roster 行 `phase == VOTER` 且不在 raft config
  ④ 无以 self 为 target 的非终态 op。③ 用 `VOTER` 而非任意 live phase：
  正在 join 的行是 `PENDING`/`CATCHING_UP`，会短暂满足"不在 config 中"。
- **`target_node = self`，绝不是 ghost**：`assertNoActiveOp` 是 per-target 的，
  以 ghost 为 target 会让这个 op **fence 掉 `cluster recovery node remove <ghost>`** ——
  为了修 ghost 而废掉清 ghost 的工具。
- **永不 BLOCKED**：预算耗尽进终态 `FS_GHOST_LEFT`，`last_error` 点名残留 id、
  `cluster recovery node remove`、以及"在清干净之前 `--to-standalone` 会拒绝、JetStream 保持 503"。
- **预算是复制式 deadline**（`catchup_deadline` 列），不是进程内 attempt 计数器：
  后者 leader 翻转即归零，且只在 step 返回 error 时递增——
  而"propose 返回 nil 但观测恒假"（`RowsAffected==0` / poison-skip / bounded-stale 落后）
  这一主要失败类根本不计数。
- **upgrade lock 对 finalize 豁免**：rolling upgrade 滚死 quorum 正是 force-single 的典型触发场景，
  而 upgrade marker 靠 leader Propose 续期/删除——那种场景下它被持有且清不掉。
  join/retire 的冻结**保持不变**（`TestUpgradeLockStillFreezesJoinAndRetire` 钉住）。
- **`AbortOp` 不再依赖 `FromState` 枚举**（改用 `PlanClusterOpAbort`，只 guard `terminal = 0`）。
  原因：`PlanClusterOpTransition` **连 FromState 一起校验**，
  所以旧二进制既驱动不了新 state 的 op、也 abort 不了它 ⇒ 该节点 membership plane 被永久 fence。
  `driveOne` 的 `default:` 分支**fail closed：只报告，绝不改动那个 op**（外审 M4 推翻了批 C 初稿的
  "强制转终态"——abort 是**由刚承认自己看不懂这个 op 的二进制**执行的一次 mutation，
  回滚/混版窗口下它会销毁新版本 workflow 唯一的持久记录。运维出口是 enum 无关的
  `cluster ops abort <id>`，见上一条）。
- **伴生的 drain-marker 愈合 pass**：只清**可证明的孤儿**（marker 存在但该节点**没有 roster 行**）。
  半完成的裸 drain（marker 在、phase 仍 VOTER、无 op）**故意不碰**——
  `DrainNode` 的失败出口明文让运维重跑，期间 marker 及其告警是那次 drain **唯一**的可见证据。
  「phase=DRAINING ∧ marker 缺失 ∧ 无非终态 op」在**渲染期**记 `Inconsistent`（不写库）。

## 21. 版本兼容窗口与升级安全（upgrade-safety 增量）

> WHAT 层条文在 `docs/requirements.md §6.7`（N-1 窗口）。本节是 HOW 层绑定契约：
> 版本检查站点的语义、additive 纪律的闸门、agent 升级状态机、marker 格式、对未来 bump 者的义务。

### 21.1 版本检查站点盘点（今日全量）

| # | 站点 | 行为 | N-1 规则下的语义 |
|---|---|---|---|
| 1 | `internal/broker/broker.go` handleRegister | `req.ProtoVersion != proto.ProtoVersion` → 拒 | **保持精确相等**。窗口的兼容单位是 release，不是 proto——proto 在窗口内冻结（§21.2），所以同窗口内两端 proto 恒等，相等检查永真；跨纪元时它正是 `proto_bump_requires_reinstall` 的执行点 |
| 2 | `internal/broker/upgrade.go` HandleUpgrade | 同上 | 同上 |
| 3 | `internal/broker/clusterstatus.go` join（`ErrJoinVersionSkew`） | joiner proto ≠ 本集群 proto → 拒 | 同上（broker 车队滚动升级期间同 proto，不受影响） |
| 3b | 同上 join 门的 **release 相等**检查（`versionSkewRefusal`，缺声明 fail-closed） | joiner release ≠ 本集群 release → 拒 | **保持硬拒——§6.7 的显式豁免**：joiner 是正在添置的新机器（重装即匹配），拒绝不卡任何已部署节点的回滚；放行会让 FSM schema 迁移在混 release 集群 fail-stop（外审 B3） |
| 4 | agent/ctl 侧 | **无反向检查**（broker 的 OK=false 当永久错误上抛） | 保持无——任何 release 层拒绝都会卡死回滚路径 |

三处相等检查**一行不改**，站点注释 `// origin: upgrade-safety N-1 window` 钉住"为何是相等而非区间"：
subject 前缀内嵌 `v<ProtoVersion>`（`internal/proto/version.go` `SubjectVersionToken`），
N-1 纪元的端发布在 `tether.v(N-1).*` 主题树上——在双订阅（§21.4）落地之前，
任何"接受集"分支都是不可达的死代码。

### 21.2 additive 纪律与闸门

- 相邻 release 间一切 wire 变更必须 additive：新增字段 `omitempty` 且**缺省零值是合法语义**；
  新增错误码必须能走旧端 default 分支优雅呈现（`xfer_provision.go` 的 Code-string 增量是既有示范；
  enum-switch-default 闸门保证本仓自己的 switch 不会瞎掉）。
- **机械闸门**：`internal/proto` 的 `TestWireFieldInventoryAppendOnly`——testdata 账本记录每个导出
  消息结构的 {字段名, json tag, 类型}，**只增不删不改**（删 / 改名 / 改 tag / 改类型即红）；
  updater 拒绝删除条目，收窄唯一的出口是手改账本并在 commit message 里写明理由
  （与结构预算棘轮同一摩擦哲学）。**刻意不查 omitempty**：旧端本就忽略未知 JSON 字段，
  真约束是"零值必须是合法语义"——那由本节条文 + 审查纪律扛，不假装机械。
  **闸门边界（内审 S17）**：账本只覆盖 `internal/proto` 包内的导出结构。包外的 wire 面
  （如 `internal/cluster/join_bundle.go` 的 join bundle JSON、adminsock 协议）由本节条文 +
  审查纪律覆盖——写明这条边界，不假称机械闸门管到了它没扫的地方。
- "零值合法"的判据：旧端发来的报文**没有**这个字段时，新端解出零值后的行为必须与增量前一致。

### 21.3 agent 升级状态机（`node upgrade` 安全化）

安装序：下载 → sha256 → **冒烟门** → **prev 槽** → marker=pending → 单次 `rename(tmp, dst)` → 回执 → exec。

- **冒烟门**：rename 前对 tmp 二进制跑 `<tmp> version`（5s 超时，stdout 有 64KiB 上限 +
  `WaitDelay` 防子进程占管道）；exec 失败或首行不符冻结格式 → `smoke_failed`；首行报告的
  **proto 纪元 ≠ 本 agent 纪元** → `proto_bump_requires_reinstall`（外审 F5：请求里的
  ProtoVersion 只证明 ctl/broker/本 agent 同纪元，不证明**下载的字节**属于该纪元——跨纪元
  产物必须走重装，不许被 upgrade 偷运）。两者磁盘零变更。首行严格解析冻结格式
  `tether <release> (proto v<N>)`，提取的 release 与 register 上报的 `ReleaseVersion`
  **同一格式**（ctl `--wait` 的判据依赖该等式）。
- **prev 槽**：`os.Remove(prev)` → `os.Link(dst, prev)`（硬链接，同 fs 零拷贝；link 失败降级 copy）→
  单次 rename 进位。**dst 在任意断点都在位**——这是对"双 rename 断电后 ExecStart ENOENT 永久失联"
  的结构性排除。prev 路径 = `dst + ".prev"`。
- **marker**：`<dst 同目录>/.tether-upgrade.json`，tmp+rename 原子写。字段：
  `state`（`pending|committed|rolled_back|rollback_failed`）、`prev_sha`、`new_sha`、`prev_version`、
  `new_version`、`deadline`（绝对墙钟）、`boot_count`、`boot_budget`（=3）。JSON 损坏视为 idle 并告警。
- **入口门**：marker=pending 时再来 upgrade → `upgrade_in_progress` 拒绝（防二次升级把唯一好二进制
  clobber 成 prev）。
- **exec 失败就地恢复**：`syscall.Exec` 失败时旧进程映像是**唯一存活的旧代码**——
  `rename(prev, dst)` 恢复、写 `rolled_back`、旧进程继续跑**不退出**（同时闭合 setsid-nohup 路径）。
- **启动检查**：`decideBoot(marker, selfSHA, now)` 纯函数，挂在 **`main()` 里 Cobra 解析之前**
  的 boot shim 上（按 argv 形状识别 `tether agent` 守护调用；`version`/help/install 子命令
  绝不碰 marker）。**必须早于一切可失败的启动步骤**（外审 F2）：新二进制若在 flag 解析、
  strict YAML、logger 初始化上回归，进程在到达 RunE 前就退出——boot 检查若在这些步骤之后，
  预算永不消耗、回退永不触发。黑盒钉子：`cmd/tether/agent_boot_shim_test.go` 用真实构建的
  二进制 + 必被 Cobra 拒绝的 flag 连续启动，断言预算逐次消耗并在耗尽时回退。分支：
  pending ∧ self==new_sha ∧ 预算内 ∧ 未过期 → boot_count++、继续、武装 watchdog；
  pending ∧（boot_count≥budget ∨ 过 deadline）→ 校验 prev_sha、恢复 prev、写 `rolled_back`、exec dst；
  pending ∧ self==prev_sha → 收敛写 `rolled_back`；prev 缺失/sha 不符 → `rollback_failed` 终态、
  响亮日志、以现状继续（不制造新循环）。
- **健康签到**：register 成功 = 提交点 → marker 写 `committed`。提交证明是**三件套**（外审 F1）：
  ① marker 的 `target_sid/target_nid` == 本实例（`node upgrade <nid>` 点名谁就只有谁能提交/上报）；
  ② `boot_count > 0`（staged 映像至少经过一次 decideBoot——install 时恒写 0，堵 flip→exec 窗口）；
  ③ **运行映像 sha == new_sha**——取自 `/proc/self/exe`（读的是本进程正在执行的字节，即使路径
  已被 rename 顶替），而非磁盘路径：flip 之后磁盘路径对宿主上**所有**进程都是 NewSHA，
  只有真正 re-exec 过的那个进程运行着它，故该证明不可被共享二进制的兄弟进程借用。
  pending 期武装 register-deadline watchdog（120s 常量；**只有 target 实例武装**——兄弟进程
  重启不代管别人的升级，target 死亡场景由其 supervisor 下次拉起时的 boot 检查收敛）：
  到期未 commit → 恢复 prev、`rolled_back`、exec dst。commit 与回退**互斥**：进程内靠
  upgradeMu，跨进程靠二进制目录下 `.tether-upgrade.lock` 的 **flock**（install 全段非阻塞持有，
  boot/commit/watchdog/exec-failure 四个短临界区阻塞持有）——**同宿主共享二进制 = 一个升级域**。
- **有序持久化协议**（外审 F6；rename 只给崩溃原子性，不给断电持久性）：候选文件在**同一 fd 上**
  chmod 0755 → fsync → close；prev 槽建立后 fsync 目录；marker tmp fsync → rename → fsync 目录；
  dst flip rename → fsync 目录；rollback 的恢复 rename 与终态 marker 同样补目录 fsync。
  install 路径上任何 sync 失败 → `install_failed` 收场（marker 尚未存在，盘面回到起点）。
  顺序由 `upgradeSyncObserver` 注入点钉住（`upgrade_durability_test.go`：顺序断言 + 注入
  sync 失败断言 fail-closed）。
- **boot 预算 = 宿主上 staged 映像的启动次数**：boot shim 早于配置解析、无从得知 sid/nid，
  而 flip 之后宿主上任何进程启动跑的都是 staged 字节——任何一次即崩都是对该映像的证据。
  代价是共享二进制的兄弟实例在 120s 窗口内的正常重启也计入预算（罕见、方向安全，如实记录）。
- **boot_budget 论证**：本仓两个 unit 是 `Restart=always RestartSec=2`（`scripts/install.sh`）与
  `Restart=on-failure RestartSec=5`（user 单元）；按 systemd 默认
  `StartLimitIntervalSec=10s / StartLimitBurst=5`，2s/5s 间隔**不会烧穿 start-limit**——
  起来即崩的二进制会无限循环。boot_budget=3 是"起来即崩"的真正闭合者（~15s 收敛），deadline 是后备。
- **已知 GAP（如实标注，不弥补）**：setsid-nohup 无监督路径下"起来即崩"无人拉起（与增量前同险）；
  "崩得早于启动检查"的窗口 = boot shim 之前的 Go 运行时初始化（外审 F2 订正：在 shim 前置到
  Cobra 解析之前**以前**，这个窗口其实覆盖 flag/YAML/logger 整个启动面，旧论断"极窄"不成立；
  前置后才真正只剩运行时初始化，静态二进制已过真实 exec 冒烟，接受）；
  register 成功但行为异常不在承诺边界内——运维手段是金丝雀 + 手动指旧版本 URL 再升一次。
- co-located agent 的 `ReExecOnly` 路径**不进状态机**（bin 目录 root-owned，`cluster upgrade`
  已有 staging + sha + quorum 保护）。

### 21.4 对未来 ProtoVersion bump 者的义务

bump = 纪元更替，必须整体交付：新 broker 同时订阅 `tether.v(N)` 与 `tether.v(N-1)` 两棵 subject 树、
在请求方的前缀上应答、并保持双订阅至少一个 release 窗口；agent/ctl 端重装（不得 `node upgrade` 跨纪元）。
`internal/proto` 的钉子测试失败信息会把 bump 者引到本节。
