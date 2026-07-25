# 分布式 broker 架构文档 — 外审报告

**LATEST RESULT: PASS（round 4 放行；历史 FAIL 轮次保留下文）**

Reviewer role: external reviewer. Scope:
`docs/distributed-broker-architecture.md`（暂存区外、未跟踪文件），并对照
`docs/reviews/distributed-broker-requirements.md` 与既有三轮内审报告
`docs/reviews/dist-broker-arch-review*.md`。

本轮不重新展开三轮内审已收口的问题；重点审查这份文档是否已经可以作为
下一大版本的实现基线。结论：还不能。主要问题是第 3 轮的最终裁定集中写在
§18，但多处正文仍保留被 §18 否定的旧契约。实现者按正文开工会得到互斥设计。

## Findings

### F1 - High: `apply.*` 转发鉴权模型仍有两套互斥契约

正文 §4.1 仍要求 session 控制写经 `apply.*` 转发时携带
`node-identity` 签名 origin proof、当前 term，并由 `tether-broker:<node_id>`
role JWT 授权：

- `docs/distributed-broker-architecture.md:111`
- `docs/distributed-broker-architecture.md:176`
- `docs/distributed-broker-architecture.md:52`

但第 3 轮最终裁定已明确删除这套机制：`node-identity` 只用于入群 PoP 与
mTLS 叶子绑定，`apply.*` 改由 route mTLS 客户端身份 + broker-only pub ACL
鉴权：

- `docs/distributed-broker-architecture.md:331`

这不是文字小错，而是实现边界冲突：到底要不要实现 per-broker 转发签名、term
校验、`tether-broker` role、JWT signer node_id，影响 auth、proto、NATS ACL、
测试和威胁模型。必须把 §18.1 的裁定回写到 §2/§4.1/§8.1/auth 包布局，
删除旧 origin-proof 方案，或反向恢复该方案并解释为什么推翻 §18.1。

### F2 - High: force-single 的安全 runbook 没有吸收第 3 轮 blocker 修正

正文 runbook 仍是：

- `tether cluster status`
- `tether cluster force-single`
- `systemctl start tether`

见 `docs/distributed-broker-architecture.md:202-211`。

第 3 轮 blocker 要求的关键安全步骤没有回写：`systemctl mask` 防止 daemon
自动重启、空/缺失 `raft/` 与 `tether.db` 时拒绝、改写 Raft 配置前调和
BoltDB/SQLite、任一 peer 经 Raft transport 可达即 hard-refuse、以及
`T_fence` 的具体数值约束：

- `docs/distributed-broker-architecture.md:338`
- `docs/distributed-broker-architecture.md:339`

当前正文还保留 `X` / `k` 这类占位描述（`docs/distributed-broker-architecture.md:211`、
`docs/distributed-broker-architecture.md:251`）。force-single 是最危险的运维逃生路径；
如果 runbook 本身不是最终契约，不能声称架构已 settled。

### F3 - High: `cluster status` 健康判定仍同时要求和禁止客户端直连 Raft

§17 结论行仍说健康判定“客户端算”，并“对每 peer 直接 Raft-transport ping”：

- `docs/distributed-broker-architecture.md:318`

紧接着第 3 轮裁定又指出笔记本/ctl 侧不得依赖直连 Raft `:7400`，只能从
NATS 可达 broker 的自报视图推断；peer 直连 ping 只限 broker 本机 offline
`cluster status`：

- `docs/distributed-broker-architecture.md:322`

这会直接影响 `quorum_lost` 判定和 force-single 诱导风险。必须拆成两个明确模式：
ctl/NATS 视图 vs broker 本机 offline 视图，并分别定义数据源、退出码和 banner
措辞。

### F4 - Medium: 数据面地址粒度仍在 per-session 与 per-expose 之间摇摆

包布局与正文 §7.5 仍写 `per-session brokerAddr`：

- `docs/distributed-broker-architecture.md:50`
- `docs/distributed-broker-architecture.md:168`

第 3 轮 blocker 已裁定必须是 per-expose / per-publicPort：

- `docs/distributed-broker-architecture.md:335`

这决定 `tunnel.Client`、`clientSession`、`dialAndRegister`、
`redialWithBackoff`、`swapTransport` 的状态模型。若仍按 per-session 实现，
`--on-broker` 与同一 agent 多个 expose 分散到不同 home broker 的场景会被架构性
卡死。需要把所有 “per-session brokerAddr” 改成 per-expose，并同步测试门文字。

### F5 - Medium: schema / alert ack 模型和第 3 轮减法互相矛盾

正文 migration 仍包含 `cluster_revoked_identities`：

- `docs/distributed-broker-architecture.md:117`

但第 3 轮减法明确删除该表：

- `docs/distributed-broker-architecture.md:355`

同样，正文仍定义 `alert_acks` 为 per-identity ack，`alert ls` 显示 who-acked：

- `docs/distributed-broker-architecture.md:121`
- `docs/distributed-broker-architecture.md:229`

第 3 轮减法却说告警改为单一集群级 ack，去 per-identity 簿记：

- `docs/distributed-broker-architecture.md:355`

这会直接生成错误 migration 和错误 CLI/API。并且需求文档原本要求 ack 按身份生效
（`docs/reviews/distributed-broker-requirements.md:180`）；如果架构决定降级为集群级 ack，
必须在 §16 偏离登记里显式列出。

### F6 - Medium: “实现精度残留”里仍包含必须先定的架构契约

§18.4 把一批内容路由到 plan 阶段，但其中不全是实现细节，而是会改变 wire /
状态机 / 安全边界的架构契约，例如：

- REGISTER 第 6 个 `<epoch>` 字段与 parser 规则（`docs/distributed-broker-architecture.md:362`）
- catch-up 谓词到底是 follower-local `applied_index >= barrier index`（`docs/distributed-broker-architecture.md:363`）
- forwarded-write 提交后 `ErrLeadershipLost` 的跨重试稳定幂等键（`docs/distributed-broker-architecture.md:365`）
- 快照不能排除活性列，§3.5 不变式需要改述（`docs/distributed-broker-architecture.md:360`）

这些不是 plan 可以自由发挥的“精度长尾”；它们决定 proto v2 wire、Raft FSM 幂等性、
catch-up 安全与快照语义。若外审要签“架构已 settled”，这些项至少应升级为
正文约束或合并前置门，而不是留在“知会”列表里。

## Required Before Re-review

1. 将 §18.1/§18.2/§18.3 中的最终裁定回写到正文对应章节，删除被裁定否定的旧契约。
2. 明确哪些 §18.4 项是架构契约、哪些才是 plan 阶段实现细节；前者必须进入正文或硬门。
3. 更新 §16 偏离登记，至少补 alert ack 从 per-identity 改为 cluster-level 的需求偏离。
4. 重新跑一次全文一致性 sweep：搜索 `per-session brokerAddr`、`origin proof`、
   `cluster_revoked_identities`、`per-identity ack`、`直连 Raft ping`、`X`/`k`
   这类已被最终裁定覆盖的残留。

## Verification

只做文档审查，未运行代码测试。读取范围：

```text
docs/distributed-broker-architecture.md
docs/reviews/distributed-broker-requirements.md
docs/reviews/dist-broker-arch-review.md
docs/reviews/dist-broker-arch-review-round2.md
docs/reviews/dist-broker-arch-review-round3.md
```

本轮未修改被审文档，只新增本审查报告。

---

## 主进程回复（round 1）——6 条 finding 全部采纳并回写

**根因认同**：§18 的第 3 轮裁定写成"覆盖正文"，使同一份实现尺出现互斥双源。处置原则：**把 §18.1/.2/.3 裁定逐条回写正文、删除被否定的旧契约；§18 降为审计轨迹（正文为唯一实现尺）；会改 wire/状态机/安全边界的 §18.4 项升级进正文**。

### F1（High，apply.* 鉴权双契约）— 已修
删除正文里的 origin-proof 方案，统一为 §18.1 裁定：
- §2 `auth/`：删 `tether-broker role + JWT 盖 signer node_id`；node-identity **仅**入群 PoP + Raft/route mTLS 叶子钉证。
- §4.1：转发**不带** origin-proof 签名 / term / 专用 role；`apply.*` 走 mTLS cluster routes + **broker-only pub ACL**；明写"转发是一致性/路由机制、非安全边界（全员可信下 per-broker 转发签名无遏制力）"。
- §8.1：session 写鉴权 = route mTLS 客户端身份 + broker-only ACL。
- §16 新增第 10 条登记此决策。残留的 `tether-broker`/`term 校验` 字样仅存在于 §18.1 审计轨迹（明写"删掉"）。

### F2（High，force-single runbook）— 已修
§8.4 runbook 重写，吸收 §18.2.5/.6/.7 全部 blocker：step0 `systemctl mask`；step2 硬前置 (b) 空/缺失 `raft/`+`tether.db` 拒绝、(c) 改写 Raft 配置前调和 BoltDB↔SQLite、(d) **任一 peer 经 Raft-transport 可达即 HARD-REFUSE** + `--confirm-peers-dead <node_id>`；recover 的发散摘要/dump-失败拒 wipe。**pin `T_fence = k_fence(10)×electionTimeout(1000ms)=10s`** + 两条约束（`> 最坏 PreVote 选举`、`≪ runbook step1 最小墙钟`）。删 `X`/`k` 占位（`--on-broker <host>` 改为 arg 记法；§10.4 的 `k` 改引 `T_fence=10s`；§3.2 统一到 `T_fence`）。

### F3（High，cluster status 直连 Raft 矛盾）— 已修
§17 结论行重写为**两个明确模式表**：① ctl/NATS 视图（默认，笔记本）——仅 NATS 自报视图、**不直连 :7400**、peer UNREACHABLE 不构成 force-single 依据；② broker 本机 offline 视图——零 NATS 依赖、直连 Raft-transport ping（私网内）。各自定义数据源、退出码、banner 措辞。与 §17 spec 的 §18.2.8 裁定一致。

### F4（Medium，地址粒度）— 已修
§2 `tunnel/`、§7.5、§13 测试门的 `per-session brokerAddr` 全改 **per-expose (per-publicPort)**：addr 按 publicPort 键存于 `clientSession`，一个 `tunnel.Client` 并发扇出 N 个 home，cert_fp 按 expose 线程化。测试门加"一 agent N expose 分散到不同 home"。

### F5（Medium，schema/alert ack）— 已修
§4.2 删 `cluster_revoked_identities`（0009）；`alert_acks` DDL 改 **集群级**（`PRIMARY KEY(dedup_key)` + `acked_by` 仅展示，删 `identity_fp/snooze_until/session_nonce`）。§2/§10.1/§8.1 同步为集群级 ack + 严重告警每新会话必重现。**§16 新增第 9 条登记需求 §8 per-identity→集群级的偏离**（并登记 §18.3 取消 member/operator 脱敏分层，§10.3 同步）。

### F6（Medium，§18.4 架构契约）— 已修
§18.4 拆 (A) 已升正文契约 / (B) 留 plan。4 项 `[→正文]`：① 快照不能排活性列→§3.5 不变式改述；② REGISTER 第 6 字段 `<epoch>` + parser 收正好 6→§7.2(b) wire 契约；③ catch-up 谓词 = follower-local `applied_index>=barrier`→§7.2(c)/§8.2；④ forwarded-write 跨重试稳定幂等键（非 leader 铸）→§4.1。其余确为实现精度，留 (B)。

### "Required Before Re-review" 对账
1. §18.1/.2/.3 回写正文、删旧契约 ✅（F1–F5 + §18 降审计轨迹）。
2. §18.4 架构契约 vs 实现细节分类 ✅（F6：(A)/(B)）。
3. §16 偏离登记补 alert ack 集群级 ✅（第 9 条）+ apply.* 鉴权（第 10 条）。
4. 全文一致性 sweep ✅：`per-session brokerAddr` / apply.* `origin proof`/`term`/`tether-broker` / `cluster_revoked_identities` / per-identity `identity_fp` ack / "客户端直连 Raft ping" 结论行 / `X`·`k` 占位——正文均已清除，仅 §18 审计轨迹保留"已删除"字样。

**待外审 round-2。**

---

## 外部复审 round 2（2026-06-20）

审查范围：`docs/distributed-broker-architecture.md` 当前全文；重点覆盖正确性和可行性。

结论：**FAIL**。上一轮 F1-F6 的主体问题已经被吸收进正文，但新版本仍有 3 个会影响实现边界或安全正确性的缺口。它们不是文字润色问题，而是会让实现者在权限、Raft Apply 校验和 cert pinning 落地时无法写出一致代码。

### RF1: `apply.*` 的 broker-only ACL 缺少可执行的发布者身份定义

位置：`docs/distributed-broker-architecture.md:52`, `docs/distributed-broker-architecture.md:111`, `docs/distributed-broker-architecture.md:176`

问题：正文现在把 `apply.*` 的安全边界改为“route mTLS + broker-only pub ACL”，并明确不再引入 per-message origin proof / term / 专用签名角色。这可以成立，但文档没有定义“broker-only pub ACL”到底绑定到哪个 NATS client 身份、哪组 subject permission、以及 tetherd 使用哪个 credential 连接本地 NATS 后才允许发布 `cluster.apply.*`。

可行性影响：NATS route mTLS 只能证明 NATS server 之间的 route，不等价于证明本机某个 tetherd client 是 broker。如果不把 ACL 明确绑定到 broker client nkey/JWT/permission，最坏情况下实现会变成“能连进本地 NATS 的用户就可能 publish apply subject”；反过来，如果实现者严格执行 broker-only，也没有文档依据说明应该给哪个身份授权，导致实现不可复现。

建议：在 auth/NATS section 明确一条规范：

- broker process 使用 `broker.nk` 或等价 broker JWT 身份连接本地 NATS；
- 只有该身份拥有 `cluster.apply.*` publish 权限；
- 普通 CLI / proxy / node identity / route peer 均不能 publish `cluster.apply.*`；
- 增加最小验收测试：普通 bus credential publish `cluster.apply.*` 被拒，broker credential publish 被允许。

### RF2: membership-changing op 的 `admin-socket origin proof` 仍未定义且无法由 follower 校验

位置：`docs/distributed-broker-architecture.md:177`, `docs/distributed-broker-architecture.md:190`

问题：正文一方面把 admin socket 定义成本地 `0600 Unix socket + sudo` 的本机管理入口，另一方面要求 membership-changing Raft op 必须携带 `admin-socket origin proof`，并由 FSM 在 Apply 时校验。这个 proof 没有格式、签名密钥、生命周期或 replay 规则；更关键的是，follower 在 Apply 时无法验证“leader 本地 Unix socket 曾收到过管理员命令”这一事实，除非文档额外定义一个可跨节点验证的 cryptographic proof。

正确性影响：Raft FSM Apply 必须是确定性的、各节点可独立验证的状态转换。未定义的本地 origin proof 会把一个 leader-local 事实塞进 replicated Apply 路径，导致实现者只能二选一：要么做不到 follower 校验，要么临时发明一个新安全机制，破坏当前“不引入 apply 签名/专用角色”的决策。

建议二选一并写入正文：

- 简化方案：把 admin socket 校验限定为 leader-local admission control；Raft log entry 只记录已通过本地管理入口的命令，FSM 不再声称校验 origin proof。
- 强安全方案：定义一个可复制校验的 admin capability，例如由本机 admin key 签发的一次性 token，包含 op type、nonce、expiry、target node set，并把验签、replay cache、过期语义写入 Apply contract。

### RF3: tunnel `cert_fp` pinning 缺少稳定证书的配置、存储和轮换契约

位置：`docs/distributed-broker-architecture.md:37`, `docs/distributed-broker-architecture.md:158`, `docs/distributed-broker-architecture.md:168`, `docs/distributed-broker-architecture.md:300`

问题：正文要求 home directive 携带 `cert_fp`，REGISTER 也携带 cert fingerprint，撤销/钉证 section 依赖该指纹判断 tunnel 身份。但 §15 的文件布局只列出了 account / node-ident / cluster-ca 等 broker 级密钥，没有列出 tunnel server 使用的稳定 cert/key，也没有定义 cert_fp 由谁生成、何时写入 Raft、如何轮换、轮换期间旧 agent 如何处理。

可行性影响：如果实现沿用临时自签或进程启动生成 cert，broker 重启后 `cert_fp` 会变化，现有 agent pin 会失效；如果 cert 被长期保存但路径/权限/备份/轮换没有规范，各节点实现会分叉，恢复和故障转移行为不可预测。

建议：把稳定 tunnel cert 纳入正文的 provisioning contract：

- 明确文件路径，例如 `/etc/tether/secrets/tunnel-cert.pem` 和 `/etc/tether/secrets/tunnel-key.pem`，权限与备份策略；
- 明确 REGISTER 上报的 `cert_fp` 来自该稳定证书；
- 明确 cert 轮换流程：先写入新指纹、双 pin 兼容窗口或 agent reconnect 策略，再撤旧；
- 增加重启不变性测试：broker restart 后同一 publicPort 的 advertised cert fingerprint 不变。

### 非阻塞备注

- `docs/distributed-broker-architecture.md:167` 的标题仍写着 `per-session addr`，正文已经改成 per-expose/per-publicPort；建议同步标题，避免读者误会。
- `docs/distributed-broker-architecture.md:396` 的安全测试仍提到“若保留任何签名”，但正文已经删除 apply 签名方案；建议改成 broker-only ACL 测试，和 RF1 对齐。

总评：上一轮的结构性修复方向是对的，尤其是 force-single、cluster status 双视角、alert ack cluster-level、per-expose brokerAddr 这些部分已经能支撑后续实现。但在进入大版本实现前，必须把 `apply.*` publisher identity、admin membership gate、tunnel cert lifecycle 三个契约写实，否则代码实现会在权限边界和恢复路径上出现互不兼容的解释。

---

## 主进程回复（round 2）——RF1–RF3 全部采纳写实，2 非阻塞已修

三条都认同是**契约欠定义**（非润色），均把可执行规范写进正文，并对齐真实代码身份（已读 `internal/auth/permissions.go`、`internal/broker/authcallout.go` 核对）。

### RF1（apply.* 发布者身份）— 已写实
正文原把 `apply.*` 安全边界定为"route mTLS + broker-only ACL"却没说 ACL 绑谁。**§6.2 新增"broker-only ACL 可执行定义"**：
- tetherd 用其 **per-broker bus nkey（代码 `AuthCallout.BrokerNkeySeed`，已是该 server `Options.AuthCallout.AuthUsers` 成员）**连本地 NATS；权限由 `auth.PermissionsForBroker()` 定义。
- **`cluster.apply.>`+`cluster.>` pub/sub 加进 `PermissionsForBroker()`，仅授 broker nkey AuthUsers**；auth_callout 签发的 member/agent/ctl user JWT 绝不含 `cluster.*` pub；node-identity 非 NATS client、route peer 非 client。
- 威胁边界精确化："能以 broker nkey 连本地 NATS 者方可 pub apply"——与"broker 同等可信"一致；普通 bus 连接拿不到。
- §2 `auth/` + §4.1 加指针；**§13.8 验收**：普通 bus credential pub `cluster.apply.*` 被拒、broker nkey 被允许、member/agent JWT pub/sub `cluster.*` 被拒。

### RF2（membership 门控不可复制校验）— 已二选一定（简化方案）
认同"leader 本地 socket 收到命令"是 leader-local 事实、follower 无法在 Apply 校验，塞进 replicated Apply 破坏确定性。**§8.1 重写**（与 §18.1/§18.2.4 一致）：
- **AddVoter（入群）**：门控 = §18.2.4 的**密码学 join PoP**（node-identity 私钥签 leader nonce，leader 对运维带外键入 pubkey 验签才提；AddVoter entry 烤入已验证 `node_ident_pub`+nonce 签名，**follower 在 Apply 复算验签**）——这是可跨节点复制校验的真 proof。
- **RemoveServer/ClusterNodeUpsert**：admin socket **仅 leader-local admission control**（0600+TTY+typed），决定 leader 是否提议；entry 只记已过本地准入的命令，**FSM 不声称校验 origin proof**。
- 诚实威胁边界：沦陷 *leader* 可提任意成员变更（§18.2.4 已接受）；防的是沦陷 *follower* 自合成——它本就 pub 不到 `cluster.*`（RF1 ACL）且 AddVoter 还须过 join PoP，**ACL + PoP 双层拒**，无需不可验证的 origin proof。删原"必须携 admin-socket origin proof、FSM 在 Apply 校验"措辞。

### RF3（tunnel cert 生命周期）— 已纳入 provisioning 契约
**§15 文件布局新增 `tunnel-cert.pem`+`tunnel-key.pem`（每台 0600，稳定持久，绑 node-identity）**（及 `broker.nk`，呼应 RF1）；**§15 新增"稳定 tunnel 证书契约"** + **§7.7 指针**：
- tunnel server 不再每次 Start 自签临时证书；`cluster_nodes.cert_fp` = 该稳定证书指纹；REGISTER/`HomeDirective` 的 `cert_fp` 必来自它。
- **写入/轮换经 Raft entry**：create/add 时 `ClusterNodeUpsert` 写初始 fp；轮换 = `cluster rotate-tunnel-cert` → 先写新 fp（**双 pin 兼容窗口**：agent 窗口内接受新旧任一 / 收 rehome 后 reconnect 重 pin）→ 后撤旧。
- 备份/恢复同 0600+FDE 卷、走 §8.3 secret 分发 runbook；**§13.7 门：broker 重启后同一 publicPort 的 advertised cert fp 不变**、轮换中老 agent 在窗口内仍 re-pin 成功。

### 非阻塞
- §7.5 标题 `per-session addr` → `per-expose addr`（正文早已 per-expose）。
- §18.4 对抗性安全测试"若保留任何签名"→ 改为 RF1 的 broker-only ACL 测试（已无 apply 签名）。

一致性 sweep：`admin-socket origin proof`（作为 replicated proof）/`per-session addr` 标题 /"若保留任何签名" 均已消除；RF1 发布者身份、RF3 证书路径+轮换+重启不变性门均落正文。§13 测试门重新编号（cert 重启=§13.7、跨真 nats 集群+RF1 ACL=§13.8），交叉引用同步。

**待外审 round-3。**

---

## 外部复审 round 3（2026-06-20）

审查范围：`docs/distributed-broker-architecture.md` 当前全文，重点复核 round 2 的 RF1-RF3 回写是否正确且可实现；同时抽查现有代码身份边界（`internal/auth/permissions.go`、`internal/broker/authcallout.go`、`cmd/tether/serve.go`）。

结论：**FAIL**。RF1 的发布者身份和 RF3 的稳定证书主路径已经写实，上一轮两个非阻塞文字残留也已处理；但 RF2 的新写法仍有一个成员变更 / FSM 边界 blocker，RF3 的轮换细节也还有一个会影响实现一致性的中等问题。现在不能签“架构已 settled”。

### R3F1 - High: AddVoter proof 被写成 FSM Apply 校验，但 hashicorp/raft membership change 不是普通业务 FSM op

位置：`docs/distributed-broker-architecture.md:64`, `docs/distributed-broker-architecture.md:126`, `docs/distributed-broker-architecture.md:198`, `docs/distributed-broker-architecture.md:377`

问题：正文选定 `hashicorp/raft`，并把业务 op 集列为 `ClusterNodeUpsert/Phase/Remove` 等；但 RF2 回写又说 “AddVoter entry 烤入已验证的 `node_ident_pub` + join nonce 签名，follower 在 Apply 复算验签”。这句话仍然把两个层混在一起：

- hashicorp/raft 的 `AddVoter` / `RemoveServer` 是 Raft 配置变更路径，不是由业务 FSM 自己定义的 `LogCommand`；
- 业务 FSM 可以校验 `ClusterNodeUpsert` 这类 roster entry，但不能自然地在 `FSM.Apply` 里拦截并拒绝底层 Raft config entry；
- 文档当前没有定义 “先提交可验证 roster op，再执行 raft AddVoter” 的两阶段顺序、失败回滚、重试幂等或状态展示。

正确性影响：实现者如果照字面写，会以为 follower FSM 能对 AddVoter 本身做 cryptographic veto；这在选定库的常规边界下不可执行。实现者如果改成先 `ClusterNodeUpsert` 再 `raft.AddVoter`，又缺少半成功状态契约：roster 已写但 AddVoter 失败、AddVoter 成功但 online/catch-up 失败、重试时 nonce/proof 是否复用、RemoveServer 与 roster 删除谁先提交，都没有明确。

建议：把 membership flow 拆成可实现的两层，并明确测试门：

- **Roster admission op**：`ClusterNodeUpsert{node_ident_pub, join_nonce, join_sig, cert_fp, ...}` 是普通 FSM op，followers 在 Apply 复算 join PoP；失败则 roster 不落库。
- **Raft config op**：leader 只有在 roster op committed 后才调用 `raft.AddVoter(node_id, raft_addr, ...)`；明确该步骤不是 FSM proof 校验点。
- **失败状态**：定义 `JOIN_VERIFIED_PENDING_VOTER` / `VOTER_ADD_FAILED` / `CATCHING_UP` 等中间 phase，或等价状态机；说明重试是否复用同一 proof、如何清理永不追平节点。
- **Remove/retire 顺序**：明确先 `raft.RemoveServer` 再 roster phase/remove，还是先写 roster draining/retiring 再 config change；两者任一步失败时 status/doctor 如何显示。
- **测试门**：模拟 roster op committed 后 AddVoter 失败、AddVoter 成功后 catch-up stalled、重复 add 同 node_id，断言状态机幂等且不会出现 “DB 认为是 voter，Raft config 不是 voter” 的静默分叉。

### R3F2 - Medium: tunnel cert 双 pin 轮换仍缺少 wire/state 契约

位置：`docs/distributed-broker-architecture.md:53`, `docs/distributed-broker-architecture.md:175`, `docs/distributed-broker-architecture.md:295`, `docs/distributed-broker-architecture.md:318`

问题：RF3 已经把稳定 `tunnel-cert.pem/.key`、`cluster_nodes.cert_fp` 和重启不变性写进正文，这是正确方向；但轮换段同时保留单一 `HomeDirective.cert_fp`，又要求 “双 pin 兼容窗口：agent 在窗口内接受新旧任一”。这仍缺一个可执行状态契约：

- 如果 directive 只有一个 `cert_fp`，agent 如何知道旧+新两个 pin？
- 如果 agent 需要用本地旧 pin + 新 directive 组成双 pin，旧 pin 是否持久化？agent 重启后窗口内如何处理？
- 窗口时长、过期条件、server 切换实际证书的顺序、旧证书撤销后的拒绝语义都没定义。

可行性影响：实现者可能写出三种互不兼容方案：把 `HomeDirective.cert_fp` 改成列表；保持 wire 不变但 agent 本地缓存旧 pin；或者只切新 pin 不做真正双 pin。它们在滚动轮换和 agent 重启时行为不同，测试门也无法统一。

建议二选一写入正文：

- **wire-list 方案**：`HomeDirective.cert_fps[]` 或 `{current, previous, valid_until}`，agent 按列表验 pin，窗口过期后 leader 下发单 pin。
- **stateful-agent 方案**：wire 仍单 pin，但 agent 必须持久化 `last_good_cert_fp`，收到新 fp 后在 `valid_until` 前接受 `{old,new}`，server 切换证书顺序和过期清理写死。

同时把 §13.7 的测试门补成包含 agent 重启：轮换窗口内 agent 进程重启后仍能按既定契约重连，窗口后旧 cert 被拒。

### R3F3 - Low: provisioning 主流程仍漏提新增 secrets 与 NATS static nkey permission

位置：`docs/distributed-broker-architecture.md:140`, `docs/distributed-broker-architecture.md:141`, `docs/distributed-broker-architecture.md:206`, `docs/distributed-broker-architecture.md:309`

问题：§15 已新增 `broker.nk` 与 `tunnel-cert.pem/.key`，RF1 也把 broker 身份接到 `PermissionsForBroker()`；但 §8.3 的 create/add 主流程仍只写 “手拷 account.nk + node-ident.nk + cluster CA”，没有把 `broker.nk`、稳定 tunnel cert/key 纳入 create/add runbook。另一个细节是，现有代码注释要求 broker nkey 同时在 `AuthCallout.AuthUsers` 和 static `Nkeys` permission entry 里；正文强调 AuthUsers，但最好明确 “generated nats-server.conf must include static NkeyUser with `PermissionsForBroker()`”。

影响：这不是新的安全设计 blocker，但会让实施 runbook 和 §15 preflight 对不上：preflight 要求文件存在，create/add 却没有生成/分发步骤；NATS 配置生成也可能只配 AuthUsers 而漏 static permissions。

建议：把 §8.3 create/add 一句话同步为：生成或分发 `account.nk`、`broker.nk`、`node-ident.nk`、`tunnel-cert.pem/.key`、cluster CA；并在 §6.2 写明 generated nats config 同时包含 `auth_callout.auth_users=[brokerPub...]` 与 static nkey user permissions = `PermissionsForBroker()`。

### 已确认修复项

- RF1 主体已解决：正文把 `cluster.apply.*` 发布者收束到 broker nkey 身份，并对齐现有 `BrokerNkeySeed` / `PermissionsForBroker()` 可改点。
- RF2 原始 “admin-socket origin proof 由 FSM 校验” 已删除；RemoveServer / ClusterNodeUpsert 的 leader-local admission 方向是对的。
- RF3 主体已解决：稳定 tunnel cert/key 已进入 §15 provisioning，重启不变性进入 §13.7。
- round 2 两条非阻塞残留已修：`per-session addr` 标题已改；“若保留任何签名”已替换为 broker-only ACL 测试。

## Verification

只做文档和本地代码边界审查，未运行 Go 测试。读取/核对范围：

```text
docs/distributed-broker-architecture.md
docs/reviews/dist-broker-arch-external-review.md
internal/auth/permissions.go
internal/broker/authcallout.go
internal/broker/broker.go
cmd/tether/serve.go
```

---

## 主进程回复（round 3）——R3F1–R3F3 全部采纳写实

三条都认同（R3F1 是真 blocker：我 round-2 把 join PoP 写成"follower 对 `raft.AddVoter` 在 Apply 复算"，混了 hashicorp/raft config 变更路径与业务 FSM 两层）。

### R3F1（membership FSM 边界）— 已拆两阶段写实
**§8.1 重写**为可实现的两层：
- **阶段 1 roster admission（业务 FSM op）**：`ClusterNodeUpsert{node_id, node_ident_pub, join_nonce, join_sig, cert_fp, raft_addr, ...}`——**followers 在 `Apply(ClusterNodeUpsert)` 复算 join PoP**，验不过 roster 不落库（真·可复制校验）。
- **阶段 2 Raft config 变更（非 FSM proof 点）**：**仅 §阶段 1 committed 后** leader 调 `raft.AddVoter(node_id, raft_addr)`；明示这不是 FSM 校验点（hashicorp/raft 配置路径）。
- **半成功 phase**：`JOIN_VERIFIED_PENDING_VOTER`→`CATCHING_UP`→`VOTER`，失败 `VOTER_ADD_FAILED`，`status/doctor` 显式渲染，杜绝"DB voter / Raft config 非 voter"静默分叉。
- **幂等**：重试复用同一 committed `ClusterNodeUpsert`（nonce 已消费不重签），`raft.AddVoter` 按 node_id 幂等。
- **Remove/retire 顺序**：先 roster `ClusterNodePhase`(draining/retiring) → `raft.RemoveServer` → `ClusterNodeRemove`；任一步失败 status 显示停点。
- 同步：§5 op 集层次澄清（`ClusterNodeUpsert` 携 join PoP；`AddVoter/RemoveServer` **不在业务 op 集**）、§8.3 create/online 走 phase、§18.2.4 纠层混淆、**§13.11 新增测试门**（roster committed 后 AddVoter 失败 / catch-up stalled / 重复 add 同 node_id → 状态机幂等无分叉）。

### R3F2（cert 双 pin 轮换 wire/state）— 已选 wire-list 方案写实
正文原保留单 `HomeDirective.cert_fp` 又说"双 pin"，欠状态契约。**改 wire-list、agent 无状态**：
- **wire**：`HomeDirective.cert_pins{current, previous, valid_until}`；`cluster_nodes` 加 `cert_fp_prev`/`cert_fp_valid_until`（§2/§4.2/§7.7/§15）。agent 按 directive 列出的 pin 集验（`current` 或未过期 `previous`），**不本地持久化 pin**——窗口内 agent 重启后重连即重验，无陈旧态。
- **轮换 `cluster rotate-tunnel-cert`**：写 `current=新,previous=旧,valid_until=now+window` → broker 起用新证书 → `valid_until` 到点写 `previous=空`、旧证书此后被拒。窗口时长 pin（> 最坏 agent 重连周期，plan 定值）。
- **§13.7 门补**：轮换窗口内 **agent 进程重启**后仍能按 `{current,previous}` 重连、窗口后旧 cert 被拒。

### R3F3（provisioning 主流程 + static nkey permission）— 已补
- **§8.3 create**：明列分发 `account.nk`+`broker.nk`+`node-ident.nk`+`tunnel-cert.pem/.key`+cluster CA（与 §15 文件布局/preflight 对齐）。
- **§6.2 RF1 块**：明写 generated `nats-server.conf` **同时**含 `auth_callout.auth_users=[brokerPub...]` **与** broker nkey 的 static nkey user permissions = `PermissionsForBroker()`（含 `cluster.apply.>`/`cluster.>`）——只配 auth_users 漏 static permissions 则 broker 自身拿不到该 subject 权限。

一致性 sweep：无"AddVoter 由 FSM Apply 复算/校验"残留；单 `HomeDirective.cert_fp` 全改 `cert_pins`（含 §1.3）；§13 门重编号 1–13（membership=§13.11、cert 轮换=§13.7、RF1 ACL=§13.8）。

**待外审 round-4。**

---

## 外部复审 round 4（2026-06-20）

审查范围：`docs/distributed-broker-architecture.md` 当前全文；重点复核 round 3 的 R3F1-R3F3 回写是否消除重大正确性/可行性问题。

结论：**PASS / 放行**。本轮没有发现新的重大问题。R3F1 的 membership 边界已经改成可实现的两阶段模型；R3F2 的 cert rotation 已选定 wire-list `cert_pins{current,previous,valid_until}`，不再依赖 agent 本地持久 pin；R3F3 的 provisioning 与 NATS static nkey permission 也已补齐。当前文档可以作为下一大版本实现基线进入 plan/implementation。

### 复核结果

- **R3F1 已解决**：正文明确 `ClusterNodeUpsert` 是业务 FSM op，followers 在 `Apply(ClusterNodeUpsert)` 复算 join PoP；`raft.AddVoter`/`raft.RemoveServer` 被定义为 hashicorp/raft config path，非 FSM proof 校验点。新增 `JOIN_VERIFIED_PENDING_VOTER` / `CATCHING_UP` / `VOTER` / `VOTER_ADD_FAILED` phase、重试幂等、remove/retire 顺序和 §13.11 测试门，足以指导实现。
- **R3F2 已解决**：`HomeDirective.cert_pins{current,previous,valid_until}`、`cluster_nodes.cert_fp_prev/cert_fp_valid_until`、agent 无状态验 pin、轮换时序和 agent 重启测试门都已写入正文。这个契约可实现且行为可测试。
- **R3F3 已解决**：§8.3 create/add 已纳入 `broker.nk`、`tunnel-cert.pem/.key`；§6.2 明确 generated `nats-server.conf` 同时包含 `auth_callout.auth_users` 与 broker nkey static permissions = `PermissionsForBroker()`。

### 非阻塞备注

- `docs/distributed-broker-architecture.md:170` 的 happy path 仍写 `ExposeForwardedReq{..., cert_fp, epoch}` / “agent 钉 cert_fp”；建议后续文字清理成 `cert_pins`。
- `docs/distributed-broker-architecture.md:343` 的偏离登记仍写 “必须钉 cert_fp”；建议改成 “必须钉 cert_pins/current fp”。这不影响正文 §1.3/§2/§7.7/§15 的权威 wire-list 契约。

### Verification

只做文档审查与本地代码边界对照，未运行 Go 测试。核对范围：

```text
docs/distributed-broker-architecture.md
docs/reviews/dist-broker-arch-external-review.md
internal/auth/permissions.go
internal/broker/authcallout.go
cmd/tether/serve.go
```
