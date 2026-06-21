# 分布式 broker 架构文档 — 对抗性审查报告（第 3 轮 · 末轮内审）

> 对象：`docs/distributed-broker-architecture.md`（第 2 轮改稿后）。同一 7 视角 + 归并 workflow。
> 产出：50 条原始归并为 **52 条**（8 blocker / 20 major / 21 minor / 3 nit）。全部 factual 引用经核对成立。
> 本轮性质：进入**实现精度层**——审查把每条声明对着真实代码核（go.mod 无 raft、`advanceProxyGeneration` 是 FATAL 启动 db.Exec、`ensureStream` 故意永不 UpdateStream、`tunnel.Client` 单 brokerAddr、REGISTER 硬编码 5 字段、`PermissionsForBroker` 通配 pub 审计、`adminsock` 仅 0600 无签名、tunnel server 每 Start 自签临时证书、`proxy_subscribers.psk` 可恢复、71 处内联审计）。

## 裁定边界（与前两轮不同）
前两轮把架构级问题改干净；第 3 轮余下多为**实现精度 + YAGNI**。主进程据此分三档落地（全部写入正文 §18）：
- **架构级 blocker/major（19 条）→ §18.2 约束性修正**（覆盖正文被纠声明）。
- **设计岔路 1 条 → §18.1 裁定**（node-identity 密钥）。
- **YAGNI 减法 → §18.3**；**纯实现精度长尾 → §18.4 路由 plan 阶段**（外审就此边界签字）。
正文另定点改了 §3.7（raft `lastApplied` 机制纠错）、§4.1（ReconcileBatch 烤 agent payload）、§0（计数 ≥71 + 代码事实校准）、§17（健康判定不走 :7400）。

## Blocker（8）— 全部采纳

| # | finding | 落点 |
|---|---|---|
| B1 | 两存储恢复用了**不存在的 API**（"FSM 覆盖 raft lastApplied"）| §3.7 改：快照 index≤SQLite applied_index + Apply 对已应用 entry 幂等 no-op（raft 必重投，FSM 自跳）。|
| B2 | 审计跨 leader 死**不可重导**（reconcile 交织内联审计 + map 序 + 消费 agent 实时字段）| §4.1 改：ReconcileBatch 把完整解析元组（含 name/local_port/rc）按全序烤进 entry，新 leader 只读 entry 重导。|
| B3 | 流副本重配**静默 no-op**（`ensureStream` 永不 UpdateStream、Replicas:1 字面量）→ HA 假成立 | §18.2(1)：建流即 replicasFor、already-in-use 分支比对并 UpdateStream、等新 server 入 JS group、达 target 前禁 retire。|
| B4 | 写成 per-session brokerAddr，实须 **per-expose**（`tunnel.Client` 单 addr、--on-broker 可分散）| §18.2(2)：addr 移到每 clientSession，N home 并发扇出。|
| B5 | queue-group 签名模型 + leader-forward **非安全边界**（共享 account 下任一 broker 已能签 allow）| §18.2(3)：Issuer/AuthUsers/Audience 具体化；leader-forward 降级为一致性/PIN 校验机制、如实写。|
| B6 | cluster-add **无持有证明**（只键入公钥），不满足需求 §10.3 | §18.2(4)：加挑战-应答（node-identity 私钥签 leader nonce）才 AddVoter。|
| B7 | force-single offline 脚枪（systemd 重启竞态、空 StoreDir 建空集群、两存储发散）| §18.2(5)：共享磁盘锁+`systemctl mask`、空态拒绝、改配置前调和两存储、peer 可达即 HARD-REFUSE。|
| B8 | recover 依赖 banner，但 NATS 可能正被运维弄坏（§11 接管 nats.conf）| §18.2(8)：doctor/status 纯本地可跑 + .bak 一行回滚 + 接管 preflight 遇不识别指令则拒。|

## Major（20）— 全部采纳（§18.2/§18.3 要点）
proxy 启动 db.Exec 与"启动只读"矛盾 → **cluster 模式禁 P13 subscribe 路径、broker.New 零 DB 写**；审计发布无 ACL 强制 → 诚实 + 盖 node_id/raft_index/leader-epoch + lint 硬门；cert_fp 移动靶 → **稳定持久证书**；撤销 re-mint + 无 per-port 断连 → 栅栏到撤销 index + per-port 主动关监听；self-fence 落到一处一时钟 + `T_fence<runbook step1`；auth fail-closed 仅"自身失联 quorum"+ pin k；PreVote pin 版本 + 实测生效门；retire 不撤信任 → account.nk/CA 轮换 runbook 设硬依赖；双信任面分清 + route 叶子也 nkey 钉；destructive 命令明表 + drain 提前通知/续服/--abort/进度；RTO 预算 + register 重指机制 + 惊群限流；disk_pressure/raft_lag 诚实定大小。

## 设计岔路裁定（§18.1）
node-identity 密钥**保留**用于(a)入群 PoP、(b)mTLS 叶子绑定（CA 签名单独不足以入群/路由）；**删**其作 `apply.*` 转发的签名 origin proof（全员可信下无遏制）——转发改 route mTLS + broker-only ACL 鉴权。化解 security（要真签名）vs simplicity（要删）的对峙：删冗余、留两个真有用性质。

## Minor（21）/ Nit（3）
约半数为实现精度（sqlite_sequence 排除、ReconcileBatch 输入全序、快照活性列调和 §3.5/§3.8、CURRENT_TIMESTAMP 列测、REGISTER 第 6 字段、catch-up follower-local、epoch-vs-barrier 双绑竞态、ReqID 提交后丢主歧义、per-stream 重配规模、对抗性安全测试构造、init 半跑回滚、node_ident_pub 带外确认、grow-to-3 secret runbook、psk-FDE 连贯）→ **§18.4 路由 plan 阶段**。YAGNI 减法（删 cluster_revoked_identities、确定性回一条结构不变式+一 lint、告警去 per-identity 簿记/脱敏分层、审计纯函数仅施恢复尾、home 资格约束）→ **§18.3 采纳**。命令别名/分组确认等 → 已纳。

## 收敛与诚实结论
三轮共提 ~140 条（去重 50/38/52），跨轮修复了**全部架构级问题**，含纠回我第 1 轮两处过度简化（force-single fence、leader-forward）。第 3 轮**未收敛到零**——但余下系实现精度，已显式路由 plan 阶段（§18.4）。**架构层视为 settled**；这份"架构已定、精度留 plan"的边界正是请外审签字处。
