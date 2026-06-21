# 分布式 broker 架构文档 — 对抗性审查报告（第 2 轮）

> 对象：`docs/distributed-broker-architecture.md`（第 1 轮改稿后）。同一 7 视角 + 归并 workflow。
> 产出：50 条原始归并为 **38 条**（8 blocker / 22 major / 6 minor / 2 nit）。全部 factual 引用经核对成立。主进程**采纳全部**，其中显著比例以"简化/减法"落地。
> 本轮价值：既补第 1 轮遗留的正确性洞，又**逮住我第 1 轮两处简化过头并纠回**。

## Blocker（8）— 全部采纳

| # | finding | 裁定与落点 |
|---|---|---|
| B1 | 两存储 applied_index：FSM 须从 **SQLite** 报 lastApplied 给 raft，非 BoltDB，否则 BoltDB 超前→静默丢写 | 采纳。**§3.7 权威游标=SQLite applied_index**，kill-9 矩阵补"BoltDB 超前 SQLite 重导 lastApplied"窗口与测试。|
| B2 | stale-leader **读/auth** 窗未 fence（写 fencing 不覆盖读；分区旧 leader 仍服务读+放行连接）| 采纳。**§3.2 读一致性契约**：敏感读走 VerifyLeader/ReadIndex；ps/history/status 有界陈旧；auth 本地读加 **node 级 fail-closed**（见更高 term/失联>k×electionTimeout）。|
| B3 | **force-single 跨真分区脑裂未结构性防住**（native term fencing 只在能互通节点间生效；我第 1 轮删了 incarnation fence）| 采纳（**纠第 1 轮过简**）。**§8.4 恢复"自我观测"fence**（失 quorum/lease>T 凭自身停写）+ 明示"B 仅分区但活着 force-single 可能双写"+ 删"结构性"。|
| B4 | force-single runbook **不可执行**（step2 `systemctl stop` 删 socket，step3 拨已没的 socket）| 采纳。**§8.4 force-single/recover 改 daemon 停机下 offline 子命令**（直接操作磁盘 raft+db、带锁），重写 runbook。|
| B5 | leader-only post-commit 发布 **实为 ~63 处非 15、多与 DB 变更交织** | 采纳。**§4.4 纠正计数 + 机械契约**（Plan 产出有序审计列表作 entry 内 DATA、Apply 在 leader post-commit 重放盖 raft_index:kind:seq）+ **lint 合并门**；PTY/ev/transfer-progress 归 best-effort 不需 raft_index（缩面）。|
| B6 | transfer ObjectStore 仍 Replicas:1，违需求 §9 R3 | 采纳。**§9 真章**：`ensureXferBucket` 改 `replicasFor`，同 audit 流重配/status/不 retire 约束，杀-home 存活门。|
| B7 | home 指派用 nats server_id(N… nuid) ≠ node_id，无桥 | 采纳。**§6.5/§4.2 加 `cluster_nodes.nats_server_id`**（模板化确定性命名），leader server_id→node_id 映射。|
| B8 | rehome 触发与"真正移动 agent 的事件"解耦（home 死同机 nats 死→agent NATS 弹走，但隧道 supervisor 死盯旧 home）| 采纳。**§7.4 让 agent 自身 NATS 重连(onNATSReconnect)+更高 epoch HomeDirective 直接驱动隧道 rehome（Open-replace）**，leader 推为 backup。|

## Major（22）— 全部采纳（要点）

- **P13/proxy 整个移出 v1 HA**（最大减法）：proxy_meta.generation/escalateProxyGen/proxy_epoch/proxy_ready 不进 Raft，proxy 切换非 v1 保证（§0/§17）——砍掉本 epic 最大新增共识面，且 P13 本就 CONDITIONAL PASS。
- 确定性规则缩为"键/fence 无不确定性"（非处处无）+ 自生成 mutator Plan/Apply 拆分清单 + 编译期断言不 import rand/ulid + 等价比逻辑内容非文件字节（§3.2/§3.3/§13.2）。
- leader-forward 自相矛盾 + broker-role JWT 不存在 + admin-over-network 不存在 → **admin 严格本地 fail-fast 指名 leader**；session 写定义真 `tether-broker:<node_id>` role（§8.1/§4.1）。
- transfer 全 §9 补全（终结 home、audit 经 leader、tracker best-effort、rehome 不保在途）。
- 共享 audit 流读隔离/重配复杂 → **退回 per-session `history-<sid>` 流**（隔离白送）+ 显式 UpdateStream 重配（§6.4）。
- JWT 撤销对已连接无界 → **撤销靠主动断连非 TTL**，§17 合成"撤销延迟=max(滞后,残留TTL,断连耗时)"（§10.1）。
- home_catching_up agent 端 terminal → v2 双端常量 + 有界重试（§7.2）。
- ex-home 双绑 + 就绪竞态 → tunnelTokenLookup 比 epoch + ReadIndex barrier（§7.2）。
- rehome 限流 vs catch-up 就绪冲突 → RehomeDirective 带 ReassignHome 的 raft_index、新 home applied>=该 index 才放行（§7.4）。
- agent 隧道 cert 钉证矛盾 → **cert_fp 钉证为 v2 硬要求**，删 §16.7 矛盾（§7.7）。
- PIN 的 XKey 设施不存在 → **删 XKey，PIN 走 broker-only mTLS 路由 subject**（§6.2）。
- 沦陷 broker 自扩名册（静默 quorum 捕获）→ membership-change op 须携 admin-socket origin proof、FSM 校验（§8.1/§10.5）。
- `disk_pressure` 已实现却被推后 → **留 disk_pressure/raft_lag 在 v1 目录**（§10.2）。
- retire 身份吊销夸大（共享 key 不轮换）→ 如实写 + account 轮换 runbook（§8.3）。
- alert 存储/闸矛盾（client-synth carve-out、replication_degraded severe 却不闸）→ §16 枚举 + 调和（§10.1/§10.4）。
- cluster status 失 quorum 时数据源矛盾 → per-field 来源 + 无 leader 时对 peer 直接 Raft-transport ping（§17）。
- 审计跨 leader 死恢复 → **审计派生为 committed entry+raft_index 的纯函数（可重导）** + dedup 窗口 > 选举+扫尾 + 幂等 sweep + §2.3"近似"登记（§6.3）。
- forwarded-apply fail-closed（not_leader→deny）+ 跨节点 callout Issuer/AuthUsers 升硬要求（§4.1/§6.1）。
- op 集：reconcile 整结果一条 ReconcileBatch；**删 GenericRowMutate** 保窄类型（§5）。
- FDE 未强制 → **cluster doctor 可验证 FDE preflight 门**（§15）。
- 无机读输出 → `cluster status --json` + 退出码契约（§17）。

## Minor（6）/ Nit（2）— 采纳（要点）
保留 AUTOINCREMENT、leader 省略 row_id 让 SQLite 赋、不重建表（§3.6）；快照读用独立只读 handle 非放松写池（§3.8）；**v2 subject grammar SSOT 表 + 发布 subject 须为 FilterSubject 严格前缀**（§2/§6.4）；alert 单布尔 ack + who-acked + 用 stream 级 MaxBytes 不 per-subject discard（避静默审计丢失）（§10.1）；drain 守卫在 F==0（含已 N=2 再 drain）即触发 + `cluster node-pub` 打印公钥 + add 半失败清理（§8.1/§8.3）；force-single 的 X 给具体值 + no-leader-safe 命令子集（§8.4）；§7.6 点名 `dialAndRegister`/`redialWithBackoff` 须读 per-session addr；**不物理拆 nodes 表、用列级 lint**（§3.5）；成员 banner 拓扑脱敏 + `cluster-ca.key` 单独泄露即可 route-join 需再过 roster（§10.2/§10.3）。

## 纠回第 1 轮过度简化（2 处）
- force-single：第 1 轮我删 incarnation fence 信"raft 原生 fencing 结构性"——第 2 轮证伪（跨无接触分区无效）→ **恢复自我观测 fence**。
- leader-forward：第 1 轮我改"重定向到 leader admin socket over Raft mTLS"——adminsock 根本无网络传输 → **admin 严格本地 + fail-fast 指名 leader**，session 写另走定义清楚的 broker-role 总线路径。

## 本轮净减法（采纳中的减法）
P13/proxy 移出 v1、退 per-session 流、删 XKey、删 GenericRowMutate、不物理拆 nodes、保留 AUTOINCREMENT 不重建表、删 DEK 留 FDE——显著降低共识面与小团队维护税。

## 待第 3 轮
本轮结构性减法大（P13 移出、流模型回退、force-single 模型改）。第 3 轮重点：减法后有无悬挂引用/自相矛盾，以及收敛是否到位（期望 finding 数与严重度下降）。
