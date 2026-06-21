# 分布式 broker 架构文档 — 对抗性审查报告（第 1 轮）

> 对象：`docs/distributed-broker-architecture.md`（Stage B 定稿）。
> 方式：Workflow 多专家对抗性审查 7 视角（consensus-correctness / storage-determinism / nats-dataplane / security / requirement-fidelity / simplicity-yagni / novice-ops）+ 1 归并。专家只读不改稿。
> 产出：85 条原始 finding 归并去重为 **50 条排序清单**（9 blocker / 24 major / 15 minor / 2 nit）。本报告记主进程逐条裁定。
> 结论：审查质量极高、全部 factual 引用经核对成立；**无整条驳回**（归并阶段已剔除非问题），主进程**采纳全部 50 条**，其中 8 条以"简化/缩范围"方式采纳。改稿已落 `distributed-broker-architecture.md`。

## Blocker（9）— 全部采纳

| # | finding | 裁定与落点 |
|---|---|---|
| B1 | REGISTER 热路径被当"本地只读"，实为写密集（混淆控制面 register.req 与数据面 tunnel REGISTER）；follower 上的权威写/审计无控制流 | 采纳。新增 **§0 两个 REGISTER 辨析 + §4.1 写转发模型**：控制面写一律转发 leader→Plan→Apply→leader-only post-commit 单条审计盖 raft_index；~15 处内联 pubAudit/pubSysEvent 改 leader-only。§2.2 RTO 含此往返。G.1 reconcile leader 权威（§4.1）。|
| B2 | `nodes` 表双写者冲突（Register=Raft vs Heartbeat/Reconcile/SetProxyReady=local 写同列） | 采纳。**§3.5 拆 `nodes_identity`(Raft)/`nodes_liveness`(leader-local，不进快照)**；migration 0009；proxy_ready 归 local，/sub failover 后一拍重建（best-effort，§17）。|
| B3 | `hashicorp/raft` 不在 go.mod（我误称"已 vendored 同家族"）；PreVote 是正确性前提却被推后 | 采纳。**§3.1 升为 Stage-B/合并前置门**：pin raft+raft-boltdb、CGO_ENABLED=0+Go1.25 编译、确认 `Config.PreVote`。|
| B4 | 文件传输在分布式下基本被丢（R3、home 路由、in-flight 全缺）| 采纳。**新增分布式 transfer**：`ensureXferBucket` 由字面 1 改 `replicasFor`、经 home broker 路由、in-flight best-effort 重启、无 multipath、kill-home e2e 门。（并入 §6.4/§12/§13）|
| B5 | auth_callout PIN-join 把 PIN 放总线 + 每次首连阻塞 Raft 往返 + quorum 丢锁死 | 采纳并细化。**§6.2 读/写路径分流**：已 provision = 本地副本读（不锁死、接受微滞后）；PIN-join = XKey 加密转 leader、commit-gated；运维恢复永不经 callout。|
| B6 | home 数据面无可用传输（agent 单地址、RehomeDirective 无 subject、broker 死不可检、落后副本 terminal DENY brick expose）| 采纳。**§7.2 新瞬态 DENY `home_catching_up`；§7.4 broker-liveness 源(Raft/route 健康)+RehomeDirective 经 SubjCmdForwarded+epoch fence+Open-replace；§7.6 per-session brokerAddr 全量重构 + 并发门。**|
| B7 | drain 与需求 §6.1 几乎相反（无预告/迁移/交权/干净退出）| 采纳。**§8.3 drain 重写**对齐需求 + drain/retire 共用 quorum 投影守卫。|
| B8 | `cluster status`（最高频运维工具）输出从未定义 | 采纳。**§17 `cluster status` 输出规格 + 客户端算的一行健康判定**（leader 不可达也出），列为 Stage-C 验收门。|
| B9 | force-single→recover runbook 缺失；自 fence 机制矛盾/欠定 | 采纳并简化。**§8.4 内嵌可复制 runbook + 预检 + fail-closed**；fence 改 **raft 原生 term/config + 节点 fail-closed，删 bespoke incarnation 三件套**（化解与 simplicity minor 的张力）。|

## Major（24）— 全部采纳（要点）

- 两存储幂等：相对增量(proxy_epoch)→leader 烤绝对值；AllocateProxy FREE-then-INSERT→确定性幂等 op（§3.7）。
- 确定性范围：删 `cfgWithDefaults` `time.Now()` fallback；lint 扩到 `internal/{port,proc,node,session,proxysub,agentprov}`；proxysub psk/sub_id、GC cutoff 入 Plan（§3.3/§3.4）。
- AUTOINCREMENT→MAX+1：撤"字节级一致"，改功能等价 + 只经 FSM 写不变式（§3.6）。
- 存储助手 `*sql.DB`→须提供 `*sql.Tx` 变体供 Apply 组单 txn（§2）。
- 启动期 proxy-gen 本地墙钟写删除（§11/§3.4）。
- 快照 vs `SetMaxOpenConns(1)`：升为阻塞决策，采 **WAL** + online-backup（§3.8）。
- 共享 audit 流：per-subject 限额 + broker 中介 consumer 读隔离 + Purge-by-subject + 显式 UpdateStream 重配 + status 副本可见（§6.4）。
- leader-forward origin-proof 简化为"重定向到 leader admin socket"（§8.1）。
- broker.nk 三重过载→**专用 node-identity 密钥与 bus nkey 分离**；JWT TTL 调短 + 撤销残留如实写（§10.1/§10.2）。
- 选举 vs quorum-loss：**server-佐证**而非纯客户端 timer；pin k（§9.4）。
- catch-up 谓词：pin TrailingLogs/snapshot 阈值 + max-wait fallback 告警；只护数据面（§8.2）。
- changeset vs 逻辑复制：CGO-free 下采**leader 渲染字面量 SQL**中间路线，§17 记理由（§3.2）。
- 命令面：§8.1 权威总表 + promote/step-down 并入 transfer-leader（§16 登记）。
- **`below_quorum` 移出 destructive 硬闸**（修复 N=2 可用档）（§9.4）。
- rebuild flag CLI 面 `--rebuild/--no-rebuild` + ps 显示新端点（§7.3/§7.4）。
- banner 面向所有人（每条 ctl 回包带）（§9.3）。
- alert 文案→动作映射表 + alerts/alert_acks DDL（§9.2/§4.2）。
- home 指派：agent register 自报所连 server id，leader 权威（§6.5）。
- cluster-add bootstrap mTLS：nonce 绑键入 pubkey + TOFU + retire 不静默复活（§10.3）。
- agent 面隧道 InsecureSkipVerify + rehome 放大 token-MITM：HomeDirective 钉 cert_fp / §16 登记（§7.7）。
- **删 cluster DEK**（security theater，同盘同失）→ psk 静态靠全盘加密（§10.4）。
- 备份/恢复 4 件套 + "restore=从健康 peer 重 bootstrap"（§3.8/§15，§16 标记 usage 旧文 stale）。
- 在位迁移接管手调 nats.conf（.bak、systemd、文件归属表）（§11）。
- CURRENT_TIMESTAMP 实为 8（含 schema_migrations）；排除出快照字节等价（§3.4/§13）。

## Minor（15）/ Nit（2）— 采纳（要点）

audit 去重 key 加 `:<seq>`（§6.3）；rehome 限流参数 + 无活 home 行为（§7.4）；home-death 运维可观测（ps/status/告警，§7.4/§9.2）；force-single 反脑裂过度工程→raft 原生 fence（§8.4）；op 数收敛（GC 移 leader-local、低频 mutate 折叠）（§5）；alert ack/snooze 缩范围（§9.1）；**N=2 降为过渡态 + status 主动警告**（§0/§17）；术语 member→node、phase vs 投票集单一权威（§4.2）；queue-group 签名 wiring 进 conf（§6.1）；入群 preflight `cluster doctor`（§15）；HA 保证矩阵（§17）；G.1 leader 权威显式（§4.1）；raft_lag/disk_pressure 推后登记（§16）；account.nk 指纹=provisioning 检查非反篡改（§10.5）；沦陷 broker 可自扩名册=已接受爆炸半径（§10.5）；**NTP 降为推荐**（§16）；双信任根成本 + cert_expiring 入 backlog（§10.2）。

## 主进程主动简化（采纳中减法 8 处）
删 DEK、force-single 用 raft 原生 fence、leader-forward 重定向 leader socket、N=2 降过渡、NTP 降推荐、op 数收敛、alert 复制存储缩范围、命令面 promote/step-down 合并——均在采纳审查意见时一并落地，降低小团队维护税。

## 待第 2 轮
本轮改动面大（结构性补 §4.1 写转发、§3.5 表拆分、§7 数据面重写、§9 告警补全），第 2 轮重点复查改动是否自洽、有无引入新矛盾。
