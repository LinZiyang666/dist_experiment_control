# tether 集群（HA）手册

> tether 的操作文档按角色拆成三册。**本册面向要做多 broker 高可用（HA）的运维**，讲
> `tether cluster` / `tether alert` 的**命令参考**、集群概念与告警。单机 broker 用户**无需本册**
> ——一台 `tether serve` + 任意多 agent/ctl 即可跑全部功能，`cluster` / `alert` / quorum
> 对单机用户完全不可见。
>
> 注：集群部署下，普通 member/ctl 也会用到只读的 §5.7 `alert ls`/`ack` 与
> §5.6 `cluster status --remote`（笔记本即可跑，命令速查见 [`usage.md`](usage.md) §4）。
>
> **本册是"命令与概念"，不是操作流程。** 具体演练（加 voter 扩容、drain/retire 缩容、
> quorum 丢失的 force-single 逃生、单机迁进集群、备份 / 灾难恢复、滚动升级）在**运维剧本**
> [`docs/cluster-runbook.md`](cluster-runbook.md)——遇到具体场景照剧本走。
>
> 使用者侧命令见 [`docs/usage.md`](usage.md)；单机 broker 部署维护见
> [`docs/broker-ops.md`](broker-ops.md)；架构见
> [`docs/distributed-broker-architecture.md`](distributed-broker-architecture.md)。
>
> **章节号沿用完整手册的原始编号**（三册共用一套编号，便于跨册引用）。

## 目录

- [什么是 cluster / 什么是 quorum（先读）](#什么是-cluster--什么是-quorum先读)
- [§5.6 `tether cluster`（分布式 broker 管理）](#56-tether-cluster分布式-broker-管理)
- [§5.7 `tether alert`（cluster alerts）](#57-tether-alertcluster-alerts)
- [§9.7.1 集群瞬时状态（transient — 等待重试即可）](#971-集群瞬时状态transient--等待重试即可)

> 使用者侧命令 / 场景 / FAQ 见 [`usage.md`](usage.md)；单机 broker 部署维护见 [`broker-ops.md`](broker-ops.md)。

---

## 什么是 cluster / 什么是 quorum（先读）

- **cluster** = 多台 broker 组成一个 raft 组，共享复制状态（session / port / 审计游标 …），
  任一 broker 宕机后其余节点仍可**写**（HA）。单机 broker 是 N=1 的退化情形，功能完整但无 HA。
- **quorum** = raft 多数派（N 台里 ⌈(N+1)/2⌉ 台在线）。有 quorum 才能选出 leader、才能写。
  丢失 quorum（如 N=3 挂 2 台）时集群**只读**，靠 §3 的 **force-single 逃生**（见
  [`cluster-runbook.md`](cluster-runbook.md) §3）临时降回单节点恢复写。
- **leader / follower**：写命令最终都在 leader 上经 raft 提交；follower 收到写会转发给 leader。
- **告警（alert）**：集群健康事件（quorum 丢失、raft 落后、broker down、disk pressure …）经
  复制的告警系统上报，用 `tether alert ls/ack` 查看确认；破坏性命令在有 severe 告警时会被 gate。

---

### 5.6 `tether cluster`（分布式 broker 管理）

```
tether cluster <command> [flag...]
```

**这是什么**：proto v2 的分布式 broker 运维入口。它不替代 `tether serve`；
`serve` 仍是每台 broker 的守护进程，`cluster` 负责把单点 broker 迁移到 Raft
集群、接纳/退役 voter、重渲染 nats.conf、检查 cluster secrets，以及
quorum-loss 的离线逃生。

> **C8 命令迁移。** cluster CLI 在 C8 做了整合，旧拼写 → 新拼写：
>
> | old (≤ C7) | new (C8) |
> |---|---|
> | `tether cluster add <id> <host:7400> <pub> [--join-token …]` | joiner: `tether cluster join prepare --node-id <id> --raft-addr <host:7400> --nats-route nats://<host:6222> --tunnel-addr <host:7000> [--public-host <host>]`（出 bundle）；leader: `tether cluster join approve <bundle> --wait` |
> | `tether cluster sign-join <id> <nonce>` | 折进 `tether cluster join prepare`（prepare 自签，无独立 sign-join） |
> | `tether cluster node-pub` | 隐藏 debug 命令；bootstrap 前置改为 `tether cluster keygen --out /etc/tether/node-ident.nk`（`join prepare` 自动派生公钥） |
> | `tether cluster wait <id> --phase VOTER` | 每操作 `--wait`（如 `join approve … --wait`、`retire … --wait`、`transfer-leader … --wait`），或 `tether cluster ops show <op-id>` / `tether cluster status --watch` |
> | `tether cluster drain <n> --retire` | `tether cluster retire <n>`（纯 `cluster drain <n>` 不变） |
> | `tether cluster remove <n>` | `tether cluster recovery node remove <n> --manual`（routine path 用 `cluster retire`） |
> | `tether cluster force-single …` | `tether cluster recovery force-single …` |
> | `tether cluster recover --self-id …` | `tether cluster recovery rejoin prepare --self-id …` |
> | `tether cluster restore <bundle> …` | `tether cluster recovery restore <bundle> …` |
> | `tether cluster export-incident …` | `tether cluster recovery incident export …` |
> | `tether cluster takeover-natsconf …` | `tether cluster reconcile nats --all --wait` |
>
> 旧顶层拼写（force-single/recover/restore/export-incident/remove/takeover-natsconf）作为 HIDDEN deprecated 别名保留一个 release，之后删除；`add`/`sign-join`/`wait` 现在已删除。

> **什么是 cluster / quorum（一句话版）。** 一个 *cluster* 是多台 broker 用 Raft 把同一份
> 控制面状态复制成多副本，任一台宕机其余继续服务——这就是高可用（HA）。每次写入要被**多数派
> （quorum = ⌊voter 数 / 2⌋ + 1）**确认才算数：3 台 quorum=2、可坏 1 台；5 台 quorum=3、可坏
> 2 台。存活 voter 跌破 quorum 时集群转**只读**（拒写）以防脑裂，直到多数恢复。所以生产 HA 至少
> 要 **3 台 voter**（2 台没有容错，坏 1 台就只读）。单 broker 没有 quorum 可丢，是完整支持的默认形态。

核心术语：

| 术语 | 含义 |
|---|---|
| `node-id` / `self-id` | broker 节点的稳定 id；同时作为 Raft ServerID、NATS `server_name`、`cluster_nodes.node_id` |
| `node-ident.nk` | 节点身份 nkey seed，私钥文件 0600；`cluster join prepare` 用它自签 join bundle，证明“加入者确实持有这个 node id 的身份” |
| `node-ident-pub` | `node-ident.nk` 对应公钥；`cluster join prepare` 自动从 seed 派生（旧的 `cluster add` 第三位置参数已删；`node-pub` 现为隐藏 debug 命令） |
| `voter` | Raft 投票成员；N=3 时允许坏 1 台，N=5 时允许坏 2 台 |
| `leader` | 当前 Raft 写入 leader；在线 membership / drain / status 经 leader 协调 |
| `quorum` | 可写多数；失去 quorum 后集群只读，破坏性 ctl 命令会被 severe alert 拦住 |
| `force-single` | quorum 丢失逃生：在确认其它 peer 都死后，把幸存节点离线改写成单 voter；无 HA / 无完整性保证，必须尽快 recover |
| `returning node` | 曾经属于旧时间线、后来要回来的节点；必须先 `cluster recovery rejoin prepare` 清理旧 DB/raft，再按新节点 rejoin |
| `raft_addr` | broker 私网 Raft transport 地址，通常 `<private-ip>:7400` |
| `nats_route` | broker 私网 NATS route 地址，通常 `nats://<private-ip>:6222` |
| `tunnel_addr` | agent 反向 tunnel 连接的 broker 地址，通常 `<public-host>:7000` |
| `public_host` | 输出给用户 / agent 的公网 DNS host，不是私网 raft 地址 |
| `cert-fp` | 稳定 tunnel server 证书指纹，格式 `sha256:<hex>`；agent 用它 pin home broker |

命令类型：

| 类型 | 子命令 | 在哪里跑 |
|---|---|---|
| 在线 admin socket | `join approve` / `drain` / `retire` / `status` / `transfer-leader` / `rotate-tunnel-cert` / `set-raft-addr` / `reconcile nats` / `ops` | broker 主机，daemon 正在运行；默认走 `--socket /var/run/tether/admin.sock` |
| 离线/本机磁盘操作 | `init --from-existing` / `join prepare` / `recovery force-single` / `recovery rejoin prepare` / `recovery restore` / `recovery node remove --manual` / `recovery incident export` / `doctor` / `keygen` | 对应 broker 主机；`recovery force-single`、`recovery rejoin prepare` 要先停 daemon |

全局 flag：

| flag | 默认 | 说明 |
|---|---|---|
| `--socket` | `/var/run/tether/admin.sock` | 在线 admin socket 子命令连接的本机 Unix socket；离线命令通常不使用它 |

**v0.4.2 — N=1↔2↔N grow / shrink 顺畅流动（不靠 force-single）。** 这批命令把单机→多机的
升级、多机→单机的降级，从代码层面变成日常运维（force-single 退回为「只在真 quorum 丢失时」的逃生）：

| 命令 | 作用 | 有效规模 / 语义 |
|---|---|---|
| `cluster set-raft-addr <host:port> [--route nats://h:p] [--allow-loopback]` | **在线**重绑本 broker 的 raft（+可选 NATS route）advertise 地址（AddVoter 原地改、非 force-single 的 wipe）；N=1 loopback→公网的 grow 前置 | 任意 N，**仅自身**（改 follower 须先 `transfer-leader` 过去）；leader-only；保护模式下拒绝 |
| `cluster reconcile nats --to-standalone --confirm-single --server-name <self>` | **降级末步**：把 lone 幸存者的 nats.conf 重渲染成无 cluster{} 的 standalone（之后 JS reset + 全量重启 nats-server） | **仅 N=1**（先 `retire` 到单 voter）；拒已 standalone |

**完整 grow / shrink 演练 + 保护模式语义见 `docs/cluster-runbook.md` §1.0(grow 前置 rebind)、§2.2(de-cluster 降级)、§2.3(命令语义表 + 边界情形)。** 要点:
- **有多台却要降级**:先 `cluster retire` 逐台降到 N=1,再 `reconcile nats --to-standalone`(有 peer 时硬拒)。
- **只有一台却要升级**:正常 N=1→2 grow(loopback 先 `set-raft-addr`,再 `join`)。
- **保护模式(quorum-lost/只读)**:所有 routine 集群命令都拒(写不进 raft),唯一能动的是 `recovery force-single`(离线)。

最小迁移骨架：

```bash
# 现网单点 broker -> 第一个 cluster voter
sudo systemctl stop tether-broker

sudo tether cluster init --from-existing \
  --self-id <node-1> --name <name> --node-ident-pub <Uxxxx...> \
  --raft-addr <private-host:7400> --nats-route <private-host:6222> \
  --tunnel-addr <public-host:7000> --public-host <public-dns> \
  --secrets-dir /etc/tether/secrets

# 让 tether 接管 nats.conf，写入 routes mTLS + auth_callout（自动路径，按 roster 渲染全 mesh）
sudo tether cluster reconcile nats --all --wait

sudo systemctl restart nats-server
sudo tether cluster status --offline --db /var/lib/tether/tether.db
sudo systemctl start tether-broker
tether cluster status
```

接纳新 voter 是两阶段流程：joining 节点先 `cluster join prepare` 出一个自签 join
bundle，leader 再 `cluster join approve <bundle> --wait` 接纳。完整命令、顺序、
回滚和故障演练以 `docs/cluster-runbook.md` 为准；本册（cluster.md）这里只放入口和常用边界，
避免把危险的 quorum 操作写成随手复制的命令块。

注意：`cluster join approve` 只改变 Raft membership，不会自动形成 NATS route/auth
mesh。新增或退役 broker 后，要跑 `cluster reconcile nats --all --wait`（自动按 roster
渲染全 mesh），滚动重启 `nats-server`（leader 最后），再用 `cluster status` 验证。
`cluster retire` / `recovery node remove` 也只移除 roster + Raft 成员；
如果退役节点可能泄漏了 `account.nk` 或 CA，还要按 runbook 轮换 secrets。

子命令与参数：

| 命令 | 参数 / flag | 默认 | 说明 |
|---|---|---|---|
| `cluster status` | `--json` | false | 输出稳定 JSON schema（`schema_version`）；脚本用这个，不解析人类表格 |
| `cluster status` | `--offline` | false | 不走 admin socket，直接读本机 DB roster；daemon 停止或排障时用 |
| `cluster status` | `--db` | `/var/lib/tether/tether.db` | `--offline` 时读取的 DB 路径 |
| `cluster status` | `--remote` | false | **ctl 用**：经 NATS 查集群健康，无需 broker 主机 / admin socket；返回**用户摘要**（见下）而非操作员表格 |
| `cluster status` | `--nats-url` / `--home` | 自动 | 配合 `--remote`：broker NATS URL 与 tether home dir（同 `tether alert`） |
| `cluster init --from-existing` | `--from-existing` | false | 必填；只支持把现网单 broker 迁移为第一个 voter，不自动创建空集群 |
| `cluster init --from-existing` | `--data-dir` | `/var/lib/tether` | broker data dir；`raft/` 会创建在这里 |
| `cluster init --from-existing` | `--db` | `/var/lib/tether/tether.db` | 要迁移的现有 SQLite DB |
| `cluster init --from-existing` | `--secrets-dir` | `/etc/tether/secrets` | cluster secrets 目录，含 cluster CA、route leaf、tunnel cert/key、node/account/broker seeds |
| `cluster init --from-existing` | `--self-id` | (必填) | 当前 broker 的 cluster node id / Raft ServerID / NATS server_name |
| `cluster init --from-existing` | `--name` | (必填) | 人类可读 broker 名；cluster 内唯一 |
| `cluster init --from-existing` | `--node-ident-pub` | (必填) | 当前节点 `node-ident.nk` 的公钥 |
| `cluster init --from-existing` | `--raft-addr` | (必填) | 当前节点私网 Raft 地址，形如 `<host>:7400` |
| `cluster init --from-existing` | `--nats-route` | (必填) | 当前节点私网 NATS route 地址，形如 `<host>:6222` 或 `nats://<host>:6222` |
| `cluster init --from-existing` | `--tunnel-addr` | (必填) | agent 应拨入的 tunnel control 地址，形如 `<public-host>:7000` |
| `cluster init --from-existing` | `--public-host` | (必填) | 用户可见公网 DNS host，用于 expose URL / home directive |
| `cluster doctor` | `--secrets-dir` | `/etc/tether/secrets` | 检查 cluster secrets；缺失/不可读/私钥权限过松为 fatal，FDE 仅 advisory |
| `cluster reconcile nats` | `--all` / `--wait` | — | **C8 主用（自动路径）**：从 live roster + secrets 派生每台 broker 的 server-name/route-url/bus nkey，按 roster 渲染全 mesh nats.conf；`--wait` 阻塞到收敛。亦支持 `--plan`（dry-run）+ `--json`。`takeover-natsconf` 是其隐藏 deprecated 别名（下列手动 flag 保留一个 release） |
| `cluster takeover-natsconf`（隐藏 deprecated 别名） | `--conf` | `/etc/tether/nats.conf` | 要接管并重写的 nats-server.conf；会保留 pristine `.bak` |
| `cluster takeover-natsconf`（隐藏 deprecated 别名） | `--secrets-dir` | `/etc/tether/secrets` | cluster CA + route cert/key 所在目录 |
| `cluster takeover-natsconf`（隐藏 deprecated 别名） | `--server-name` | (必填) | 本 broker 的 deterministic NATS `server_name`，等于 cluster node id |
| `cluster takeover-natsconf`（隐藏 deprecated 别名） | `--account-issuer` | 从现有 conf 读取 | shared account public nkey；空时只从现有 auth_callout issuer 读取 |
| `cluster takeover-natsconf`（隐藏 deprecated 别名） | `--broker-nkey` | 从现有 conf 读取 | 本 broker bus nkey 公钥；只有现有 auth block 恰好一个 nkey user 时才自动读取，多 user 必须显式传 |
| `cluster takeover-natsconf`（隐藏 deprecated 别名） | `--route-url` | (必填) | 本 broker 对其它 broker 暴露的 route URL，如 `nats://10.0.0.1:6222` |
| `cluster takeover-natsconf`（隐藏 deprecated 别名） | `--cluster-listen` | `0.0.0.0:6222` | NATS route listen 地址，只应在 broker 私网开放 |
| `cluster takeover-natsconf`（隐藏 deprecated 别名） | `--peer` | 可重复 | 其它 broker，格式 `server_name,route_url,bus_nkey`；多 peer 重复传，构成 full mesh |
| `cluster takeover-natsconf`（隐藏 deprecated 别名） | `--nats-server` | `nats-server` | 用于 `nats-server -t` dry-run 校验的二进制 |
| `cluster takeover-natsconf`（隐藏 deprecated 别名） | `--skip-dry-run` | false | 跳过 `nats-server -t` 校验；不推荐，只在目标机没有 nats-server 时临时使用 |
| `cluster join prepare` | `--node-id` | (必填) | joining 节点自己的 node id |
| `cluster join prepare` | `--raft-addr` | (必填) | joining 节点私网 Raft 地址，形如 `<host>:7400` |
| `cluster join prepare` | `--nats-route` | (必填) | joining 节点私网 NATS route，形如 `nats://<host>:6222` |
| `cluster join prepare` | `--tunnel-addr` | (必填) | joining 节点公网 tunnel control 地址，形如 `<host>:7000`；缺失会导致该 voter 不能成为 expose home |
| `cluster join prepare` | `--public-host` | `<host>` | joining 节点公网 host；公网部署应显式传，别默认成私网 raft host |
| `cluster join prepare` | `--seed` | `/etc/tether/node-ident.nk` | 本节点 node identity seed；prepare 用它自签 bundle 并派生公钥（折自旧 `sign-join --seed`） |
| `cluster join approve` | `<bundle>` | (必填) | joining 节点 `cluster join prepare` 出的自签 join bundle（含完整 expose-home + NATS 身份 + cert 指纹） |
| `cluster join approve` | `--wait` | false | 阻塞到新 voter 被接纳（取代旧的 `cluster wait`） |
| `cluster keygen` | `--out` | (空) | 写 node identity seed 到指定路径，0600；`join prepare` 的前置（每台新 broker 一次）；空值只打印 pubkey 不落 seed |
| `cluster drain` | `<node-id>` | (必填) | 要迁移 expose / 准备退休的 voter |
| `cluster drain` | `--now` | false | 跳过 drain notice period，立即迁移 |
| `cluster drain` | `--abort` | false | 取消正在进行的 drain，把节点 phase 退回 `VOTER` |
| `cluster retire` | `<node-id>` | (必填) | drain 完后从 Raft config + roster 移除该节点（原 `drain --retire`）；可 resume；迁移 expose + 等 stream 冗余达标；F==0 时手输 node-id 确认 |
| `cluster recovery node remove` | `<node-id>` | (必填) | 直接从 Raft config + roster 移除（最后手段）；**必须**带 `--manual`；routine path 用 `cluster retire` |
| `cluster recovery node remove` | `--manual` | (必填) | 确认这是 last-resort raw remove（缺它直接拒绝） |
| `cluster transfer-leader` | `<node-id>` | (必填) | 把 Raft leadership 交给一个 caught-up voter |
| `cluster rotate-tunnel-cert` | `<node-id>` | (必填) | 目标 broker node id；必须在目标 broker 上执行并让其成为 leader |
| `cluster rotate-tunnel-cert` | `--cert-fp` | (必填) | 新稳定 tunnel cert 指纹；DB pin 更新后当前/previous pin 进入旋转窗口 |
| `cluster recovery force-single` | `--online` | false | **首选**：经 RUNNING broker 的 admin socket 在线恢复，**不停 daemon、不制造第二次停机**；socket 不可达（broker 真死）才回落 OFFLINE 磁盘路径 |
| `cluster recovery force-single` | `--dry-run` | false | 配合 `--online`：零改动演练（评估 gate + 打印 peer 探测），可在**健康集群**上跑，用来排练命令与核对 peer 列表 |
| `cluster recovery force-single` | `--data-dir` | `/var/lib/tether` | broker data dir；**OFFLINE 路径用**，daemon 必须停止 |
| `cluster recovery force-single` | `--db` | `/var/lib/tether/tether.db` | 本机 DB 路径（OFFLINE 路径用） |
| `cluster recovery force-single` | `--self-id` | (必填) | 当前幸存节点 id；命令会要求人工输入它确认。`--online` 下该值会发给 broker 校验（与 socket 所属节点不符即拒，防误指错节点） |
| `cluster recovery force-single` | `--self-addr` | (`--online` 时不需要) | 当前幸存节点 Raft 地址，形如 `<host>:7400`；仅 OFFLINE 路径需要（在线路径由 broker 从 roster 自取） |
| `cluster recovery force-single` | `--confirm-peers-dead` | (必填，可逗号分隔) | roster 中其它所有节点 id；命令会探测它们 raft/nats/tunnel 端口仍可达则 HARD-REFUSE（活着=会脑裂） |
| `cluster recovery rejoin prepare` | `--data-dir` | `/var/lib/tether` | returning node 的 broker data dir；daemon 必须停止 |
| `cluster recovery rejoin prepare` | `--db` | `/var/lib/tether/tether.db` | returning node 的 DB 路径 |
| `cluster recovery rejoin prepare` | `--dump-divergent` | (必填) | forensic dump 输出路径，0600，必须不存在 |
| `cluster recovery rejoin prepare` | `--self-id` | (必填) | 被清理节点 id；命令会要求人工输入它确认 |

`cluster status` 退出码可用于监控：`0` = HEALTHY-HA，`1` = DEGRADED，
`2` = read-only / quorum-lost，`3` = force-single。`cluster retire` 在退役后
故障容忍度 F=0 时要求手工输入 node id；`cluster recovery node remove` 每次都要求手工输入
node id；`recovery force-single` 和 `recovery rejoin prepare` 都不会接受 `--yes`，必须停 daemon 并手工确认。
这些交互确认是为了避免脚本误删 voter 或把旧时间线塞回新 cluster。

**怎么读 `cluster status` 的输出**（broker 主机 / socket 视图）：

- **列图例**：`LAG`=本节点落后 leader 的 raft 条目数（0=已追平）；`ACCT.NK`=Y 表示账户密钥匹配
  （当前恒 Y——per-node 校验尚未接线）；`STREAMS`=JetStream 副本 actual/target（actual<target=降级）；
  `REACH`=NATS/raft 可达性。
- **白话判定行**（按 voter 数，权威）：1 台=`NO redundancy`（坏了即停）；2 台=`NO fault-tolerant
  writes`（坏一台即只读）；≥3 台且 streams 达标且 HEALTHY=`HA active — survives <F> failure(s)`；
  否则=`HA configured but DEGRADED right now`。
- **view 行**：`view_host` 是出这份自报告的 broker，`is_leader_view` 表示是否权威 leader 视图；
  非 leader 上会提示"re-run on the leader 拿权威判定"。
- **`--json` schema**：B1 新增 `verdict` / `view_host` / `is_leader_view`（additive，**`schema_version`
  仍为 1**，不破坏只读未知 key 的监控）。

**ctl 远程视图**（`tether cluster status --remote`，无需 broker 主机）：登录的 ctl 经 NATS 复用现有
`cluster-health` responder（**无需额外 broker 配置 / ACL**），返回**用户摘要**——"几台 broker 应答 +
reachability 判定 + 指向 leader"，**不是** 8 列表，**也不含** 操作员逃生命令（`recovery force-single` /
`recovery rejoin prepare` / `join approve` / `drain` 仍只在 broker 主机经 socket 执行）。其退出码更薄、永不出 `1/DEGRADED`：零应答→`0`
（单 broker 受支持）；可写 leader→`0`；force-single→`3`；只读（无 leader 且全 stale）→`2`；选举中→`0`。

**`--offline` 退出码语义不同**（磁盘 roster + `:7400` ping，不走 NATS）：`0`=探测正常或 N=1；`2`=
roster >1 台且无人应答 `:7400`（`health` 串为 `ROSTER_UNREACHABLE`，刻意区别于在线 `DEGRADED`=exit1，
使 `(health→exit)` 视图无关）；`3`=force_single_active。脚本按 `view` 字段（`ctl-nats` / `offline`）
区分，不要只看退出码 `2`（在线=quorum-lost、离线=全员不可达，含义不同）。

**确认机制如何工作（how confirmations work）。** cluster 命令有两档确认 + 两个正交开关，别混（B3）：

- **Tier-1 可逆操作**（`drain` 在 F>0、`transfer-leader`、`rotate-tunnel-cert`）：**无需确认**，也**没有** `--yes`（加一个 no-op flag 只会误导）。
- **Tier-2 不可逆 / 影响 quorum 操作**：必须**在 TTY 手输 node-id 确认**，**永不接受 `--yes`**。其中 `recovery node remove` / `recovery force-single` / `recovery rejoin prepare` / `init` 对 `--yes` 给明确报错"this is an irreversible / quorum-affecting op…there is NO --yes override by design"；`cluster retire` 在 F==0 同样要手输 node-id（它本身没有 `--yes` flag，传了就是 cobra 的 `unknown flag: --yes`）。这是设计上的无人值守禁区，防脚本误删 voter / 误把旧时间线塞回新集群。
- **正交开关 A：`--ack-alerts`**（`session rm`/`expose`/`run` 等破坏性 ctl 命令）——在 severe cluster alert 下**强推这一条命令**通过，**不是**确认、不修、不清除告警。
- **正交开关 B：`tether alert ack <dedup_key>`**——store-backed 团队协调 ack，**不能**清除 `quorum_lost`/`force_single_active`（这两个是实时合成的健康条件、非 store-backed alert；对它们 `alert ack` 会解释拒绝）。

`recovery force-single` 在输 node-id 前还会内联打印劈脑裂后果；`cluster retire` 成功后提醒"retire 是拓扑改、非凭据撤销"（见 `cluster-runbook.md` §2.1）。

### 5.7 `tether alert`（cluster alerts）

```
tether alert ls
tether alert ack <dedup_key>
tether alert raise --kind manual --severity {info|severe} --message "<text>" [--label <tag>]   # operator-only
tether alert clear <dedup_key>                                                                    # operator-only
```

**这是什么**：查看和确认当前 active session 可见的 store-backed cluster alert。
alert 是 broker/cluster 级别的健康信号，不是某个 agent 的进程事件；典型 kind 包括
`manual`、`below_quorum`、`broker_draining`、`broker_down`、
`replication_degraded`、`disk_pressure`、`raft_lag`。

子命令与参数：

| 命令 | 参数 / flag | 默认 | 说明 |
|---|---|---|---|
| `alert ls` | `--nats-url` | 同 usage.md §5.1 全局解析链 | broker NATS 入口；分布式 HA 可写逗号分隔 seed list |
| `alert ls` | `--home` | `~/.tether` | 读取 nkey、`current_session`、默认 broker URL 的目录 |
| `alert ack` | `<dedup_key>` | (必填) | 要确认的 alert key，来自 `alert ls` 输出 |
| `alert ack` | `--nats-url` | 同 usage.md §5.1 全局解析链 | broker NATS 入口；分布式 HA 可写逗号分隔 seed list |
| `alert ack` | `--home` | `~/.tether` | 读取 nkey、`current_session`、默认 broker URL 的目录 |
| `alert raise` | `--kind` | `manual` | 只能 `manual`（系统 kind 由集群自动产生，不可手动 raise） |
| `alert raise` | `--severity` | (必填) | `info` 或 `severe`。**severe = 进 ps/node 常驻 banner；但不阻塞写**——只有 `quorum_lost`/`force_single_active` 才硬门 destructive 命令 |
| `alert raise` | `--message` | (必填) | 人类可读告警文本 |
| `alert raise` | `--label` | (空) | 可选去重后缀 → dedup key `manual:<label>`（默认 key `manual`） |
| `alert raise` / `clear` | `--socket` | `/var/run/tether/admin.sock` | broker 本机 admin socket（**operator-only**，不走 session/NATS） |
| `alert clear` | `<dedup_key>` | (必填) | 要清除的 key（`manual` 或 `manual:<label>`）；幂等 |

**`alert raise` / `alert clear` 是 operator-only 命令**：和 `tether cluster *` 一样走 broker 本机 admin socket（不需要 NATS 身份），由 leader 经 raft 复制写入；在 follower 上运行会提示去 leader 重跑，单 broker 上返回 `cluster_not_enabled`（告警是 raft 复制的，单点无 raft）。与 `alert ls`/`alert ack`（member 经 NATS 的只读/团队 ack）不同。`manual` 告警一旦 raise 会一直存在，直到 operator `alert clear`——reconciler 只管它自己的系统 kind，从不动 `manual`。`severe` 只决定是否进常驻 banner，**不**等于 destructive 硬门。

`dedup_key` 是 broker 存储 alert 的稳定去重键：同一个问题 active 期间只保留一条
alert，不会刷屏生成重复条目。`alert ack` 是 store-backed alert 的 cluster-level
ack：它记录“团队已经看过/有人处理”，不会删除 active alert，也不会修复底层问题。
真正恢复仍要跑 `tether cluster status` 并按 runbook 修复。

与 `--ack-alerts` 的关系：`--ack-alerts` 是某次 destructive 命令的临时确认；
`alert ack` 是把某个 store-backed `dedup_key` 标记为已确认，方便团队协作时区分
“没人看过”和“已有人在处理”。`quorum_lost` / `force_single_active` 是 ctl 根据
cluster health 临时合成的 destructive gate，不会出现在 `alert ls` 的 dedup_key 里，
也不能用 `alert ack` 解除；每次确需继续操作时只能在对应 destructive 命令上显式加
`--ack-alerts`。两者都不绕过 broker 端权限和 quorum 规则。

### 9.7.1 集群瞬时状态（transient — 等待重试即可）

分布式 HA 下 broker 故障切换/选举会产生几个**瞬时**码，看到就等几秒重试，**不是**永久错误：

| code / reason | 含义 | 谁消费 / 处置 |
|---|---|---|
| `leader_unavailable` | raft leader 切换 / 选举中，写暂时无人接 | agent register loop 退避自愈 |
| `home_catching_up` | expose 的 home broker 故障切换后正在追平 | agent 反向隧道 REGISTER 自动重试 |
| `try_again` | broker DB 瞬时故障（非 token 失效） | agent 反向隧道 REGISTER 自动重试 |

> 这三个码**目前都由 agent 内部消费、不直接打到 ctl**（`leader_unavailable` 在 register loop；
> `home_catching_up` / `try_again` 在反向隧道 REGISTER 的 DENY 路径，见 `internal/broker/expose.go`
> 的 `tunnelTokenLookup`）。列在此是给读 broker log 的人，并为将来可能的 ctl 暴露路径预留友好渲染
> （`brokerCodeHints`）。区别于 `proto_bump_requires_reinstall` / `tunnel_token_unknown_or_revoked`
> 这类**永久**错误（要重装 / 重新 expose），上面三个看到就等、就重试。

