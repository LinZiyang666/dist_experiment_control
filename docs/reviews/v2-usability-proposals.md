# v2 易用化产品建议

本文从产品经理和外部用户视角整理 v2 cluster 的下一轮功能建议。目标不是削弱一致性安全，而是把用户现在必须手动理解和串联的内部步骤，收敛成可解释、可恢复、可审计的产品流程。

## 核心判断

v2 当前的主要问题不是能力缺失，而是用户需要知道太多内部细节：broker 加入后要人工更新多台 broker 的 NATS 配置，agent/ctl 要人工维护 broker URL，proxy 在 cluster 下不可用，quorum 失败时普通用户很难判断什么还能做。

外部用户期望的 v2 应该是：

1. 新 broker 只在加入时输入一次必要信息，后续拓扑、NATS route、seed 列表自动收敛。
2. 新 agent 只拿一个 invite 或 bootstrap URL，在线后自动学习完整 broker roster。
3. proxy 在 cluster 模式下可用，默认由 leader 管理控制面，数据面可随 home broker 自动迁移。
4. 没有多数派时系统进入只读保护，并清楚告诉用户原因、影响和下一步。
5. 危险恢复可以被向导化，但不能被静默自动化。

## 产品原则

1. 用户表达意图，系统处理机制。比如用户说“加入 broker b4”，不应该再手动重跑每台 broker 的 `takeover-natsconf`。
2. 自动化默认只做安全收敛。涉及分裂脑、丢弃旧时间线、credential rotation 的动作必须保留 typed confirmation。
3. 所有自动分发都必须可验证。agent/ctl 不能接受随机 broker 列表，只接受签名 roster 和单调递增 generation。
4. 状态输出要回答“现在能不能写、为什么、下一步做什么”，而不是只暴露 raft/NATS 原始状态。
5. proxy/expose 的可用性要和 cluster 拓扑一致，不能因为进入 cluster 就整体失效。

## 建议 1：Cluster Operation Controller

把 broker 生命周期从“人在每台机器执行命令”改成“Raft 记录期望拓扑，每台 broker 本机 reconciler 执行本机副作用”。

建议命令：

```bash
tether cluster plan add b4 \
  --public-host b4.example.com \
  --raft-addr 10.0.0.4:7400 \
  --nats-route nats://10.0.0.4:6222 \
  --tunnel-addr b4.example.com:7000

tether cluster apply <plan-id> --wait
tether cluster reconcile nats --all --wait
tether cluster ops ls
tether cluster ops show <op-id> --json
```

机制：

- Raft 保存 `cluster_generation`、peer set、每个 broker 的期望 NATS route 配置、public endpoint、服务状态。
- leader 只提交 intent，不直接远程 ssh/root。
- 每个 broker 上的本机 reconciler 监听 generation，执行本机 `nats.conf` 渲染、`nats-server -t` 校验、原子替换、滚动重启。
- `cluster status` 显示 `desired_generation`、`applied_generation`、`observed_generation`，卡住时给明确下一步。

验收标准：

- 新 broker 加入后，不需要人工登录所有 broker 重跑 NATS 配置。
- 任一 broker 未完成 topology apply 时，集群不能显示为 `HEALTHY-HA`。
- unknown `nats.conf` 指令继续 fail-closed，不能被自动覆盖。

## 建议 2：Broker Join/Retire 工作流

`cluster add/remove/drain` 应该升级为可恢复的 operation，而不是一次性命令块。

加入流程：

```bash
# 新 broker 本机
sudo tether cluster join prepare \
  --self-id b4 \
  --leader-bootstrap https://cluster.example.com/.well-known/tether/cluster.json \
  --public-host b4.example.com

# 现有 leader 或 admin 节点
tether cluster join approve b4 --wait
```

推荐状态机：

```text
PREPARED -> PREFLIGHT_OK -> JOIN_PROOF_VERIFIED -> ROSTER_COMMITTED
-> RAFT_ADDING -> CATCHING_UP -> NATS_ROLLED_OUT -> SERVING
```

退役流程：

```bash
tether cluster retire b2 --wait
tether cluster retire b2 --compromised --require-credential-rotation
```

推荐状态机：

```text
DRAIN_REQUESTED -> NO_NEW_HOME -> REHOME_EXPOSES -> STREAMS_AT_TARGET
-> SEED_WITHDRAWN -> LEADER_TRANSFERRED -> RAFT_REMOVED
-> NATS_ROLLED_OUT -> RETIRED
```

安全边界：

- 操作后如果 N=2 或容错数 F=0，必须 typed confirmation，拒绝无提示 `--yes`。
- JS/OBJ 副本未达 target 时拒绝 retire。
- rebuild-OFF 的 expose 要列出并要求确认，因为它们不能自动无损重建。
- `--compromised` 不能把 retire 当成安全边界，必须进入 account.nk/CA/key rotation 流程。

## 建议 3：Signed Broker Roster 与 Agent 自动发现

把 `broker_url` 从“完整静态列表”降级为“bootstrap URL”。agent/ctl 只需要一个入口，连接成功后自动学习完整 broker roster。

建议命令：

```bash
tether cluster seeds publish \
  --bootstrap https://cluster.example.com/.well-known/tether/cluster.json

tether agent join <invite-url> --start
tether agent config refresh --once
tether agent doctor
```

机制：

- Raft 保存 `seed_generation` 和 public WSS/NATS endpoints。
- 每个 broker 暴露同一份 signed roster manifest。
- agent/ctl 保存 bootstrap URL 和最近一次有效 seed cache。
- agent 启动顺序：cached seeds -> bootstrap URL -> install-time fallback。
- register response 也下发 `ClusterRoster{generation, urls, expires_at}`，在线 agent 自动刷新。
- retire/drain 时，roster 先把节点标记为 `draining`，停止新连接偏好，TTL 到期后再删除。

验收标准：

- 新 agent 只拿 invite 即可入群，不需要手写逗号分隔的 broker list。
- 加一台 broker 后，在线 agent 在 5 分钟内自动刷新 roster。
- 离线 agent 不无限期阻塞 retire，受明确 TTL/阈值策略约束。
- manifest 签名失败、generation 倒退、server identity 不匹配时拒绝更新。

## 建议 4：Proxy Cluster 化

当前 cluster 下 proxy 整体不可用，这会让 v2 对外部用户显得像功能倒退。建议把 proxy 当成系统管理的一组特殊 expose。

控制面：

- `proxy_enabled`、subscriber set、keyset generation、per-agent proxy allocation 纳入 Raft Apply。
- 写操作仍要求多数派；无多数派时不能新增 subscriber、开关 proxy、轮换 keyset。
- leader 作为权威控制面中心，但不要求所有数据流量经过 leader。

数据面：

- 每个 agent 的 `__proxy__` allocation 带 `home_broker`、`epoch`、cert pins。
- `/sub/<token>` 可由任意 broker 服务，但返回当前权威 home：`server=<home public_host>, port=<proxy public_port>`。
- home broker down 后，系统自动 rehome `__proxy__`；订阅客户端下次刷新拿到新 home。
- 既有 TCP 连接可中断，不承诺无损迁移。

建议命令：

```bash
tether proxy on --ha-policy freeze-on-quorum-loss
tether proxy status --cluster
tether proxy sub <name>
tether cluster rebalance proxy --wait
```

降级语义：

- `ACTIVE`：有多数派，proxy 控制面和数据面正常。
- `FROZEN_READONLY`：无多数派，已有订阅和 keyset 冻结，只读服务尽力保留。
- `DISABLED_NO_QUORUM`：无多数派且策略要求 fail-closed，proxy 不提供订阅。
- `FORCE_SINGLE`：事故模式，只允许明确选定的新单节点时间线继续服务。

不建议系统在无多数派时自动“临时选一个中心”继续写 proxy 状态。可以提供事故向导让运维显式进入 `force-single` 或 `proxy force-home`，但这必须带 incident 记录和 typed confirmation。

验收标准：

- cluster 下 `proxy on/sub/status` 可用。
- 杀掉 proxy home broker 后，订阅短暂消失或自动切到新 home，不需要 `proxy off/on`。
- `proxy status --cluster` 能解释每个 agent 为什么进/不进订阅：`ready`、`tunnel_down`、`keyset_stale`、`no_home`、`catching_up`。

## 建议 5：Expose/Rehome 可观测性

expose 已经具备 `home_broker + epoch` 的 cluster 基座，下一步应该把它产品化，让用户能知道“现在这个端口到底在哪台 broker 上，为什么不可达”。

建议命令：

```bash
tether expose ls --json
tether expose explain <name>
tether cluster status --homes
```

建议字段：

```text
name, nid, local, public_url, home_broker, epoch, state,
last_rehome_at, last_error, reconnects, ready_reason
```

建议事件和告警：

- `home_reassign_started/succeeded/failed`
- `proxy_node_ready/unready`
- `sub_render_empty`
- `broker_down_rehome_summary`
- `proxy_partial`
- `proxy_no_ready_nodes`
- `rehome_stalled`
- `agent_roster_stale`

所有事件必须脱敏，不带 token、PSK、private key、session secret。

## 建议 6：Cluster Status 产品化

把 raft/NATS 原始健康信息合成用户级状态卡。

建议状态：

```text
HEALTHY-HA          有多数派，可写，有 HA
DEGRADED-WRITABLE   有多数派，可写，但副本/节点不完整
READ-ONLY           无多数派，写操作被保护
FORCE-SINGLE        事故逃生模式，单节点新时间线
NOT-HA              N=1 或 N=2，能工作但不提供生产 HA
```

建议输出：

```bash
tether cluster status --explain
tether cluster doctor
tether cluster incident export
```

输出需要包含：

- 当前能做什么：读命令、写命令、proxy、expose 是否可用。
- 为什么被保护：quorum lost、catching up、force-single active、stream replicas below target。
- 下一步命令：等待、reconcile、retire、offline diagnose、force-single wizard。
- 不要做什么：比如不要把旧 peer DB 直接塞回新时间线。

`--json` schema 要稳定，给监控系统使用；人类解释放在普通输出和 stderr banner。

## 建议 7：事故恢复向导

恢复可以自动检查和引导，但不能在分区场景下自动替用户做不可逆决定。

建议命令：

```bash
sudo tether cluster recovery diagnose --offline
sudo tether cluster recovery force-single \
  --self-id b1 \
  --confirm-peers-dead b2,b3

sudo tether cluster recovery rejoin prepare \
  --self-id b2 \
  --dump-divergent /root/divergent-b2.json
```

向导应该自动做：

- 检查 broker daemon 是否停下，避免 raft DB 锁。
- 本机探测 peer `:7400`，任一 peer 可达即拒绝 force-single。
- 列出将被丢弃的 peers 和后续 rejoin 步骤。
- force-single 后保留 severe alert，直到 N>=3 且 streams at target。
- returning node 必须 dump divergent 后 wipe，再作为 clean node rejoin。

必须人工确认：

- 输入本机 `node_id`。
- 确认其它 peer 是物理/网络上对服务不可达，而不是当前笔记本视角不可达。
- 接受旧时间线被抛弃和返回节点需 wipe/rejoin 的后果。

## 推荐优先级

P0：

1. `cluster status --explain` 和 `cluster doctor`，先把当前状态讲清楚。
2. signed broker roster，让 agent/ctl 不再手动维护 broker URL。
3. topology reconciler，把 NATS route 渲染和滚动重启从人工流程变成 operation。
4. `agent join <invite>`，降低新机器接入成本。

P1：

1. broker join/retire operation controller。
2. expose/proxy home 可观测性和 `explain` 命令。

P2：

1. proxy cluster HA。
2. recovery/force-single guided incident mode。
3. compromised-node credential rotation assistant。
4. staging 演练工具，定期验证 quorum loss 和 recover 流程。

## 成功指标

1. 加入一台 broker 从多机手动步骤变为一条 prepare + 一条 approve。
2. 新 broker 加入后，无需用户修改任何 agent 配置。
3. 新 agent 只需要一个 invite 或 bootstrap URL。
4. cluster 下 proxy 可用，并有清楚的无多数派降级行为。
5. 只读保护触发时，普通用户能在一分钟内看懂影响和下一步。
6. 退役 broker 不会误删仍承载 expose/proxy home 或未达副本 target 的节点。
7. 所有自动分发都有 generation、签名校验和审计事件。

## 需要明确拒绝的自动化

1. 无多数派时自动选择一个 broker 继续写控制面状态。
2. 自动复制 CA/account private key 到其它机器。
3. manifest 签名失败或 generation 倒退时继续刷新 agent seed。
4. force-single 支持 `--yes` 或仅基于 ctl 客户端视角执行。
5. retire compromised node 后默认认为安全已恢复。

这些拒绝点会增加少量操作步骤，但能避免把网络分区、凭据泄漏和旧时间线回流变成更严重的数据一致性事故。
