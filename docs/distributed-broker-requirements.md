# 分布式 broker 需求文档（Distributed Broker Requirements）

> 本文是"把 broker 从单点变成分布式系统"这一 epic 的**需求基线**，承接 `docs/requirements.md` 的北极星与威胁模型，是后续 plan 阶段（CLAUDE.md §3）的输入。
>
> 本文只定**要什么 / 不要什么 / 边界在哪**，不定具体实现方案（那是 plan 的事）。所有结论经一次结构化议事逐条钉死。
>
> 语言：讨论与本文用中文；代码 / 标识符 / subject / 配置键一律英文（CLAUDE.md §5）。

---

## 0. 术语

| 词 | 含义 |
|---|---|
| **broker / 节点** | 一台 `tether serve` 实例（= 一个 NATS 节点 + tetherd + 隧道 server 同机） |
| **master / leader** | Raft 选出的唯一写权威节点；本文 master ≡ Raft leader |
| **follower** | 非 leader 的 broker 投票成员，持有状态副本 |
| **N** | 集群中 broker 投票成员数 |
| **健康多数派** | 存活且互通的投票成员数 > N/2（quorum 成立） |
| **归属 broker** | master 为某 agent 指派的、承载其数据面隧道的 broker |
| **force-single** | 失去多数派时，运维显式强制单机续跑的逃生开关（放弃完整性承诺） |

---

## 1. 驱动与非目标

### 1.1 首要驱动
- **可用性（HA）/ 故障切换**：broker 节点故障时另一节点接管，**控制面状态与审计历史不丢**。
- **broker 机群运维**：节点有完整生命周期（创建 → 上线 → 下线 → 退役），每阶段都要有对应行为与命令。

### 1.2 规模假设（非功能性约束）
- 面向 **几十 ~ 上百 agent**；单台 broker 本就能胜任此量级。
- 因此 **Scale（吞吐/容量）不是驱动**：**不做为容量的水平分片**；目标是"小型固定集群把 HA 做扎实"。
- **Geo（就近/区域分布）不是驱动**。

### 1.3 非目标 / 明确不做（本 epic 范围外）
- 文件传输的**最快链路选择 / 多路并行（multipath）**——太复杂、当前量级收益边际，推迟（见 §9）。
- expose 的测量式选址、跨区调度。
- **witness / 仲裁节点**——经讨论放弃（见 §3.3）。
- 为容量做的分片 / 弹性伸缩。

### 1.4 交付方式
- 沿用 phase 分期：**基础（HA）先行，派生增强增量叠加**，各增量独立走 plan→实现→内审→外审，不阻塞主线。

---

## 2. 失效模型与可用性目标（SLA）

### 2.1 集群规模与容错（按 N 分层）
| N | 定位 | 保证 |
|---|---|---|
| **1** | 单 broker，等同今天的系统 | 无 HA；必须继续可用 |
| **2** | 便利 / 开发 / 过渡档 | 数据冗余 + 热备；**不做数据完整性承诺**；**非安全 HA 档** |
| **≥3（奇数）** | **推荐生产形态** | 多数派 quorum：安全自动选举 + fencing + 同步提交 |

### 2.2 切换窗口（RTO）
- 越短越好但**不极致**；**可用性与简易性优先**。
- 黑窗在**数秒 ~ 数十秒**可接受。
- 复用现有 agent 的 NATS 自动重连（指数退避）+ G.2 重启对账。

### 2.3 数据丢失（RPO）
- **健康多数派（N≥3）下**：已提交的**关键控制写**（session / 成员 / 端口分配 / 审计）**力争 0 丢失**（靠 quorum 同步提交）。**这是 0 丢失承诺的唯一适用范围。**
- 瞬时的 `ev.*` 事件与 PTY 字节：维持 **best-effort**（断连即丢、agent 重连只见新数据，与现有 B.3 一致）。
- **降级模式（N=1 / N=2 / 任意失去多数派）：不做数据完整性承诺。**

### 2.4 降级行为（失去多数派）
- **默认安全转只读**：不可写；读仍由存活节点提供（可能旧值）。
- 运维可显式 **`force-single`** 强制单机续跑：由运维担保其他节点确已死透；**主动放弃完整性承诺**（若担保错误 → 脑裂风险）。

---

## 3. 架构基线（权威与一致性）

### 3.1 载体：内嵌 Raft + 本地 SQLite
- 采用 **rqlite/dqlite 模式**：**Raft 复制写操作**；每个节点把操作应用到**自己本地的 SQLite**；**读走本地、写过 leader**。
- 收益：**现有 SQLite 事务语义几乎原封不动保留**（端口分配 partial-unique-index、`session.Create`、三阶段 rm 等不变量保住）；选举 / 复制 / fencing / 成员变更交给成熟 Raft 库；纯 Go、不破单二进制。
- 代价（已接受）：**两层复制**——broker 状态走 Raft，消息 + 审计 history 走 NATS 集群（见 §3.4）。

### 3.2 共识语义
- 你描述的"master 权威 + follower 复制 + master 死了选举 + 全系统共识 + 防脑裂"在标准术语里就是 **Raft**：单一 leader、**不用 gossip**、quorum 提交保证已确认写 0 丢失、自动选举、term 做 fencing。
- 单 leader 串行所有写 → **无需分布式事务（2PC）**。唯一的跨节点协调在数据面（leader 记意图 + 承载 broker 执行 + 失败时对账），属编排（saga），其失败路径由 §5.2 的重建机制覆盖。

### 3.3 不引入 witness
- N=2 自动降级"对方不回应就转单机"在网络分区下**必然脑裂**（节点无法区分"对方死了"和"被分区"）。
- 经评估**放弃 witness**：N=2 直接走 §2.4 的降级规则（默认只读，运维 `force-single`）。要真 HA 就上 N≥3。

### 3.4 NATS 集群化（消息面）
- 为保 HA，NATS 必须**集群化**：**每台 broker 同机一个 NATS 节点**（延续 `architecture.md` A.2 同机原则），N 个节点组成一个逻辑总线。
- auth_callout 走 **queue group**（任一 broker 都能认证连接）；JetStream 流 **R3** 复制。

### 3.5 读路由
- **读一律走 leader**（永远最新、最简单）；follower 纯做热备 + 数据冗余。此量级 leader 扛下所有读无压力。

### 3.6 一个二进制、N=1..n
- 同一个 `tether` 二进制，cluster-aware；**N=1 时行为与今天完全一致**（不分裂成两个产品）。

---

## 4. 控制面与 agent 状态对齐

### 4.1 leader 是唯一权威
- **leader 是"哪些 expose / 进程存在"的唯一真相**，agent 永远向 leader 收敛。
- 复用现有 **G.1 对账**（`reconcileOnRegister`）：agent register 时上报本地清单 → leader 裁决 → 回 `keep / revoke / drop` → agent 本地被覆盖。分布式版唯一改动：**权威从"某个 broker"变成"leader"**；承载 broker 执行 leader 的数据面决定（撤端口=拆隧道）。
- 反向（leader 有、agent 没有）沿用现有规则（ALLOCATED 端口宽限、惰性回收）。

### 4.2 签名私钥（多机）
- 分布式后每台 broker 都要能做 auth_callout 应答 → 每台都需持有同一把 **account 签名私钥**。
- **分发方式：运维手动装**（provision 时人工拷到新机）——**私钥永不过网、不入 Raft log**。延续"安全实用主义 + 能登 broker 的才是运维"。

---

## 5. 数据面（expose / 隧道）

### 5.1 隧道分布 = 摊到各 broker（方案 B）
- master 只管**状态 / 端口分配的权威**；**公网口监听 + 隧道字节落在 agent 实际连接的那台 broker 上**，维护"哪个口在哪台"的映射。

### 5.2 节点失效与 expose 重建
- **默认简单**：承载 broker 死，expose 在别处重建后**公网 `IP:port` 可能变**。
- 新增 **per-expose 重建开关**：
  - **默认 ON**：承载 broker 死 → 自动在存活 broker 重建（逻辑暴露 `name→local` 不变，端口/host 可能换；`tether ps` 显示新端点）。
  - **OFF**：放弃该 expose，master 释放端口（必要时发告警/通知）。
- 只有**承载机**死才触发重建；**master 死不影响已建 expose**（仅控制权威换人）。

### 5.3 选址与端口
- 默认自动：master 选承载 broker（见 §7.3）+ 分配**全局唯一端口**。
- 三者可用可组合：`--on-broker <X>`（新增，指定承载 broker）、`--remote-port <P>`（已有 P12，指定端口号）、全默认。
- 端口段为**全局一池、master 单写分配**，唯一性天然无冲突（复用 partial-unique-index 语义）。

---

## 6. broker 生命周期 + `tether cluster` 命令面

### 6.1 四阶段
| 阶段 | 行为 |
|---|---|
| **创建（provision）** | 运维手动装 account 签名私钥（不过网）+ 分配 nkey 身份 + `cluster add` 入 Raft |
| **上线（join）** | 同时加入 **Raft 集群（状态）+ NATS 集群（消息）**；**追平 Raft log 前不对外服务**（不应答 auth_callout、不绑数据面口）；追平后才服务 |
| **下线（drain，计划内）** | **宽限期 + 提前通知**（告警广播"brokerX 将在 N 分钟后下线"）→ 主动迁 expose（rebuild-ON 重建、rebuild-OFF 拆+通知）→ 若是 leader 先**交权** → 干净退出 Raft 成员 → 关机 |
| **退役（retire）** | drain + 永久移出 Raft 成员 + 吊销身份 + 移出发现列表；状态早已复制，无丢失 |

### 6.2 自动保护
- 移除投票成员会改变 quorum（3→2 会从安全 HA 掉进降级档）。**退役若使集群跌破安全 HA → 先发严重告警 / 要求显式确认**才放行。

### 6.3 命令面
- `tether cluster init / add / remove / status / promote / step-down / transfer-leader / drain / retire`
- `tether alert`（见 §8）
- **危险操作本地化**：`cluster *` 变更与 `force-single` **仅运维本地**（走现有 admin Unix socket，"能登 broker 的才是运维"）。

---

## 7. agent 对多 broker 的应对 + 发现机制

### 7.1 控制面连接（基本免费）
- agent / ctl 拿一份**种子 URL 列表**连任一 NATS 节点 → NATS 客户端**自动发现集群其余节点 + 节点死时自动 failover**；auth_callout queue group 连哪台都能认证。
- **ctl 完全靠这个搞定**（无数据面，只走 NATS 发命令 / 读 PTY）。

### 7.2 数据面归属
- **master 指派归属 broker，agent 服从**（不自己选）——理由：master 本就是端口分配权威，由它定最一致、agent 无需自写选址逻辑、无抢占竞态。
- `--on-broker <X>` 覆盖：某条 expose 想去别处，agent 额外对 X 开一条隧道（默认单归属、需要时才多连）。
- 归属 broker 死 → master 重派 → agent 改连新归属（复用 §5.2 重建）。

### 7.3 指派策略
- 先用简单策略（最少负载 / 轮询 / 就用 agent 当前连的那台），**以后可调，非需求级决策**。

### 7.4 发现
- agent 配置给**种子列表**（静态配置或 DNS）；控制面活成员靠 NATS 动态发现；**数据面有哪些 broker、归属是谁，由 master 经 register 回包下发**（复用 P13 `ProxyDirective` 那种"回包带指令"模式）。
- broker 增删 / 退役**不用挨个重配 agent**——种子够连上一个，其余动态学。

---

## 8. 告警系统（alert）

> 分布式系统跑在降级态（如 `force-single` 无完整性承诺）时，**每条命令都当面提醒**，不让人不知不觉在危险态操作。

| 维度 | 规则 |
|---|---|
| **存储 / 权威** | 集群级权威状态，存 **leader 的 Raft 复制态**（切换后在、全集群一致、重启不丢） |
| **投递** | 每条 `tether` 命令启动先取活跃告警，未消除者作 **banner 顶在输出最前**再执行；面向**所有人**（含普通用户） |
| **来源** | ① **系统自动**（失去多数派、`force-single` 降级、broker 下线/退役中、复制延迟、磁盘压力……）；② **运维手动**广播 |
| **消除** | 自动告警**条件驱动、全局**自动消除；**强制关闭 / ack 按身份（fingerprint）各自生效** |
| **重现分级** | 信息级 ack 后永久不再现；**严重级 ack 后仍在新 ctl 会话重现**（snooze，不可永久隐藏） |
| **拦截** | 默认只显示不拦；**严重级告警下，destructive 命令（`expose`/`run`/`session rm` 等）要求显式 ack 后才执行** |
| **CLI** | 自动 banner + `tether alert ls / ack`（运维另有 `raise / clear`）；命令归属待定 |

---

## 9. 文件传输（push / pull）在分布式下

- **始终经 agent 连接的（归属）broker 中转**——最简单，**无选路、无 multipath**。
- tier-B 的 JetStream ObjectStore 桶 **R3**，已完成对象在 broker 死后仍在。
- **在途传输 = best-effort**：故障切换时在途追踪态（当前为内存态）丢失 → **传输重启、客户端重试**（传输非"已提交控制写"，本就该 best-effort，与 `ev`/PTY 一致）；**不为在途做 Raft 复制**。
- **最快链路 / multipath：明确不做**（太麻烦），将来真有需要再单独评估。

---

## 10. 兼容与安全

### 10.1 wire 兼容：强硬升级
- **破坏兼容、不长期支持老 agent**；可直接走 proto **`v2`**（干净重设计的自由）。
- rollout = **协调式全机群强制升级 / 重装**（复用现有远程 `node upgrade` + 机群升级工具）。

### 10.2 迁移
- 现有单 broker 的 SQLite **一次性就地迁成 Raft 状态机的初始状态**（既有部署平滑变 N=1 集群，再 `cluster add` 扩容）。

### 10.3 安全
- **信任域**：所有 broker **同等可信**（同一运维、同信任域）；签名私钥在每台都有 → **任一 broker 沦陷 = 整个集群 auth 沦陷**——将现有 E.6"broker 沦陷=全系统沦陷"扩到机群，**接受**。
- **broker↔broker 链路**：Raft 复制 + NATS 集群路由**一律 TLS + 双向认证**，防野节点接入。
- **入群认证**：新 broker `cluster add` 须**运维主动发起 + 新节点用自己的 nkey 证明身份**，非任意主机可自封入群。
- **危险操作本地化**：见 §6.3。

---

## 11. 建议的交付分期（粗，非约束；细化留 plan 阶段）

> 仅作 plan 阶段的起点参考，不是承诺的 phase 列表。

1. **状态层换底座**：内嵌 Raft + 本地 SQLite，N=1 行为不变；现有单 broker SQLite 迁移（§3.1 / §10.2）。
2. **多节点共识与选举**：NATS 集群化、auth_callout queue group、JetStream R3、N≥3 HA、降级与 `force-single`（§2 / §3.4）。
3. **数据面分布**：方案 B、归属指派、expose 重建开关、`--on-broker`（§5 / §7.2）。
4. **生命周期 + `tether cluster` 命令面**：create/online/drain/retire（§6）。
5. **告警系统**（§8）。
6. **传输适配**：经归属 broker、ObjectStore R3、在途 best-effort（§9）。
7. **强硬升级收尾**：proto `v2`、机群升级（§10.1）。
8. **（推迟）传输增强**：fastest-path / multipath（§1.3 / §9）——独立叶子增量，不阻塞主线。

---

## 12. 待定项（plan 阶段澄清）

- Raft 库选型（hashicorp/raft 等）与"命令编码进 log + 状态机执行 SQL"那层的具体形态。
- `tether cluster` 与 `tether alert` 的命令分组归属与 help 布局。
- master 归属指派策略的初版具体规则。
- 严重级告警的精确清单与文案。
- NATS 集群拓扑细节（路由 TLS、种子、JetStream 域）。
