# 分布式节点控制系统 — 需求文档

> **文档权威链**（与 `CLAUDE.md §1` 同一表）：本文是 **WHAT** 层——产品要做什么、不做什么，是**需求**的唯一真相。
> 它**不描述当前实现**：v2 / 集群面当前如何实现见 `docs/distributed-broker-architecture.md`
> 与 `docs/deploy-tier-gotchas.md`（HOW·当前）；当初为什么这么选见 `docs/architecture.md` §A–§K（HOW·历史，
> 取舍论证有效、标识符已过时）。历史层从不覆盖当前层。


> **v1 范围裁剪记录**（外审 R6 订正：此处原写作"本文是 v1 之前的历史草稿、当前以
> `architecture.md` 为权威"，与上方权威链直接矛盾——一个读者按上表读到"需求唯一真相"，
> 按下表读到"以 architecture 为准"。**以上表为准**：本文是需求层的真相，下面列的是
> v1 当初没做的部分，是**范围记录**，不是权威转让。）
>
> 不在 v1 范围（已挪到 v2 或显式 deferred）：
> - **`push` / `pull` / 文件传输**（本文 §5、§7、§9 多次出现）—— v1 不实现；
>   架构定位见 `docs/architecture.md` "v1 不做：file transfer (M5)"。
> - **`tether tag`、`tether plugin`** 等模块化扩展 —— v1 不实现。
> - 其它在本文出现但 architecture 未承接的项默认 v2 候选。
>
> 当前 v1 的实际命令面以 `tether --help` + architecture I.1/I.2 章为准。

> **文档定位**：系统的**需求规约（what）**，供实现参考。决策过程请看 `logs/log.md`（历史决策日志，冻结）。
> 架构细节：当前实现见 `docs/distributed-broker-architecture.md`，历史取舍见 `docs/architecture.md`
> （原文写作"待产出"，那是本文起草时的状态，早已不成立）。

---

## 目录

1. [背景与目标](#1-背景与目标)
2. [系统边界（北极星）](#2-系统边界北极星)
3. [术语表](#3-术语表)
4. [角色模型](#4-角色模型)
5. [功能需求](#5-功能需求)
6. [非功能需求](#6-非功能需求)
7. [接口约定（CLI / 消息 / 配置）](#7-接口约定cli--消息--配置)
8. [持久化与数据模型](#8-持久化与数据模型)
9. [鉴权与权限](#9-鉴权与权限)
10. [生命周期与韧性](#10-生命周期与韧性)
11. [一期范围（v1 scope）](#11-一期范围v1-scope)
12. [附录：需求追溯](#12-附录需求追溯)

---

## 1. 背景与目标

### 1.1 使用场景

用户拥有多台分布各处的机器：
- 大多数无 root 权限，位于宿主 NAT 后（家用 / 校园 / 企业网络）。
- **仅一台有公网 IP**，同时承担内网穿透与控制中枢职责。
- 机器上会跑异构工作负载（训练、推理、数据采集、仿真等）。

### 1.2 产品目标

构建一个"**四通八达的 SSH + 文件搬运工**"：
- 从一台前端用户机器上，像 SSH 一样控制任意节点：执行命令、跑交互式 shell、传文件、暴露端口。
- 所有节点纳入一个统一的命名空间（session），无论其物理位置。
- 控制通道对网络恶劣环境鲁棒（NAT、防火墙、短暂断网）。
- 工程上解耦，便于未来按需扩展上层能力（编排、UI、多租户）。

### 1.3 显式非目标

系统**不做**：
- 实验语义层（job / pipeline / dag / 超参搜索 / metric 追踪）。
- 托管进程的业务日志持久化。
- 节点间数据流协议（用户自己选 socket / gRPC / 自研）。
- 容器编排（无 k8s / swarm 职能）。
- 多租户 billing / 配额 / 审计外挂。

---

## 2. 系统边界（北极星）

**我们做的是"四通八达的 SSH + 文件搬运工"，不是实验平台。**

### 2.1 系统管

- 在节点 X 上启动进程 `<argv>`（带 env / cwd / stdin / 可选 PTY）。
- 把文件推到节点 X、从节点 X 拉文件。
- 把节点 X 上某进程监听的端口暴露到公网机。
- 查询节点健康 / 托管进程状态 / 取系统自己的调用历史 / 发信号。
- 维护命名空间（session）与身份（pubkey）。

### 2.2 系统不管

- 托管进程用什么框架 / 生成什么文件 / 采用什么协议互通。
- 托管进程的业务日志（超出截断窗口即丢弃；用户自己 `> file.log 2>&1` + `pull`）。
- 超参数搜索 / metric 追踪 / 结果可视化（用户自选 wandb / mlflow 等）。
- 节点间的应用层数据协议。
- 托管进程的自动重调度（仅告警，重启由人决定）。

### 2.3 边界纪律

任何进一步的抽象（job group、pipeline、dag）引入前必须自问："**这是不是把实验语义偷偷塞进系统了？**" —— 若是，拒绝引入；作为独立的上层工具或插件处理。

---

## 3. 术语表

| 术语 | 定义 |
|---|---|
| **前端用户** | 从 `ctl` CLI 发起指令的人；可能是 owner 或 member。 |
| **owner** | 创建某 session 的 pubkey 持有者；有 kick / rm / rotate-pin 权限。 |
| **member** | 被加入某 session 的 pubkey 持有者；可操作该 session 下所有节点与进程。 |
| **公网机** | 拥有公网 IP、部署 broker + relay + controllerd 的服务器。 |
| **broker** | 公网机上的 `nats-server`，承担控制面消息总线。 |
| **relay** | 公网机上的 `frps`，承担数据面 TCP 端口反向代理。 |
| **controllerd** | 公网机上的系统状态权威服务（自研）。 |
| **agent** | 部署在受控节点上的自研守护进程，主动拨号连接 broker 与 frps。 |
| **ctl** | 前端用户使用的 CLI 工具（自研）。 |
| **node** | = 一个 agent 实例（同一物理机可跑多个 agent = 多个 node）。 |
| **session** | 系统的命名空间基底；所有状态按 session 隔离。 |
| **托管进程（managed process）** | 由 `ctl run` / `ctl exec` 经 agent 拉起、登记在 `state.json` 与 controllerd 中的进程。 |
| **用户进程** | 节点上由用户自己启动、系统不感知的进程（本系统不涉及）。 |
| **identity（身份）** | 一对 ed25519 公私钥；pubkey fingerprint 是系统中 actor 的唯一标识。 |
| **pubkey fingerprint** | OpenSSH 风格 `SHA256:<base64-no-pad>`。 |
| **PIN** | session 的加入口令；owner 人工设置，人工分发。 |
| **history** | 系统**自身的**调用记录（谁在何时对哪个节点做了什么、结果如何）。**不是**托管进程的业务日志。 |

**术语纪律**（T1 / T2 / T3 定稿）：
- 不用 `master`（指服务用 `controllerd`，指机器用"公网机"，指人用 `owner` / "前端用户"）。
- 不用 `controller`（软件实体统一 `controllerd`）。
- 鉴权 / schema 字段语境用 `identity` / `actor_pubkey_fp` / `owner_pubkey` / `allowed_pubkeys`；不用 `user_id`。
- 面向用户 / 系统内部一律 `托管进程` / `process` / `process_id`；不对用户说 `experiment`。

---

## 4. 角色模型

五种系统角色，严格解耦：

### 4.1 Broker（公网机）

- 软件：`nats-server`，监听 `443 (WSS + TLS)`。
- 证书：Let's Encrypt。
- 职责：**无业务逻辑**，纯消息总线。
- 状态：JetStream 持久化数据（见 §8.2），运行时状态可重建。

### 4.2 Relay（公网机）

- 软件：`frps`。
- 职责：TCP 端口反向代理，给 agent 的 `frpc` 子进程提供反向隧道。
- 状态：frp 运行时绑定，无业务语义。

### 4.3 Controllerd（公网机，长驻服务）

- **系统的唯一状态权威**。
- 职责：
  - `sessions` 表（ACL / PIN 哈希 / allowed_pubkeys）。
  - 节点注册表（在线状态、心跳、标签）。
  - 托管进程记录（`processes` 表）。
  - 端口分配表（`frps` remote_port 全局分配，跨 session 共享）。
  - History 记录（SQLite 条目 + JetStream 流联动）。
  - 挑战-响应鉴权。
- **不做**：实验级编排 / 调度、任务状态机、托管进程 stdout/stderr 历史聚合。
- 本身是 NATS 客户端；不暴露任何 REST / HTTP **管理**接口。
  - **P13 例外**：proxy 订阅功能引入一个**只读、loopback 绑定、GET-only** 的订阅端点（`GET /sub/<token>`，经 Caddy 反代），仅吐出 Clash 配置，不接受写、不持有 NATS 句柄、不是管理面。这是当前唯一的对外 HTTP 表面（`broker.sub.listen` 为空时整体禁用）。

### 4.4 Agent（每个受控节点）

- 主动拨号 `wss://broker-domain:443`，维持长连 + 心跳。
- 拉起自己的 `frpc` 子进程（**每 agent 独立一个 frpc**，不复用）。
- RPC 能力：`run` / `exec` / `push` / `pull` / `expose` / `ps` / `kill` / `tag` / `metrics` / `hello`。
- 以 `setsid` 分离方式托管子进程，自身崩溃不影响托管进程。
- 本地持久化 `state.json`，重启后作为对账真相源。

### 4.5 Frontend：`ctl` CLI（前端用户机）

- 纯 NATS 客户端（与 agent 对称）。
- 不保存权威状态；仅缓存当前 session / 身份私钥。
- 交互式 PTY 使用 `github.com/creack/pty` + `golang.org/x/term`。
- **一期不提供 Web dashboard**。

---

## 5. 功能需求

### 5.1 节点原语（agent 提供）

| 原语 | 语义 | 记 history |
|---|---|---|
| `run` | 启动非交互进程（argv + env + cwd + stdin），实时流 stdout/stderr/exit | ✅ 全部 |
| `exec` | 交互式 PTY 会话（支持 vim / tmux / Ctrl-C / 窗口同步） | ✅ 只 meta，不记字节流 |
| `push` | 本地文件/目录 → 节点路径（分块 + 校验 + 断点续传） | ✅ 全部 |
| `pull` | 节点路径 → 本地文件/目录 | ✅ 全部 |
| `expose` | 节点端口 → frps 的 remote_port，返回公网可达地址 | ✅ 全部 |
| `unexpose` | 撤销 expose | ✅ 全部 |
| `ps` | 列出该 agent 当前托管的进程 | ✅ 全部 |
| `kill` | 向托管进程发信号 | ✅ 全部 |
| `tag` | 读写节点元数据标签（k=v） | ✅ 全部 |

### 5.2 Session 管理

| 命令 | 语义 | 权限 |
|---|---|---|
| `ctl session create <name>` | 创建，自动设 owner = 自己 pubkey，要求交互输入 PIN | 任意身份 |
| `ctl session list` | 列出可见 session | 任意身份 |
| `ctl session login <name>` | 首次用 PIN 挑战通过后加 pubkey 到 allowed_pubkeys；之后免 PIN | 任意身份 |
| `ctl session switch <name>` | 切换当前活跃 session | member+ |
| `ctl session current` | 显示当前 session | member+ |
| `ctl session rm <name>` | 删除 session（**交互确认**） | **owner-only** |
| ~~`ctl session kick <pubkey-or-fp>`~~ | 将某身份从 allowed_pubkeys 移除 | **未实现**（见下方 banner） |
| ~~`ctl session rotate-pin`~~ | 生成新 PIN（已加入的 pubkey 不受影响） | **未实现**（见下方 banner） |

> **⚠ 未实现（batch-A A7 订正）**：`session kick` 与 `session rotate-pin` **从未实现**。
> 全仓 `members` 表只有一处 DELETE（session 硬删的级联），`pin_hash` 有 **零处** UPDATE。
> architecture H.1 已由 DOC-12 订正，`internal/broker/audit.go:36-39` 也记录了同一事实，
> 但本文件此前仍将两者列为已交付能力——**规格与 ACL 同时撒谎，比诚实的缺口危险得多**：
> 它让读者以为吊销能力已存在。A7 已同步移除对应的 NATS 授权
> （`session.<sid>.kick.req` / `rotate-pin.req`），并加了双向对账测试防止再次漂移。
> 撤销/PIN 轮换若要落地，需作为新的叶子增量重新设计。

**不做**：默认 / 自动创建的 session；未 login 状态下除 `session list/create/login` 外所有命令均拒绝。

### 5.3 Agent / 节点发现

| 命令 | 语义 |
|---|---|
| `ctl nodes [--filter k=v]` | 列当前 session 下节点（按标签筛选） |
| `ctl nodes show <node>` | 显示节点详情（基础资源 / 历史连接事件） |
| `ctl agent list` | 列当前 session 下 agent（ONLINE / STALE / OFFLINE / 上次心跳 / 累计连接时长） |
| `ctl agent show <id>` | agent 详情（基础资源 / 连接事件 / 托管进程列表） |
| `ctl agent kick <id>` | **主动清场**（agent 自杀 + kill 托管进程 + stop frpc）（**owner-only**） |

**不提供** `reconnect` 命令；agent 自行无限重连。

### 5.4 History

| 命令 | 语义 |
|---|---|
| `ctl history list [--by <fp>] [--kind admin\|call]` | 列本 session 下 history 条目（可按 actor / 类别筛选） |
| `ctl history show <id> [--lines N\|--full]` | 详情；**默认 tail 40 行 stdout + 40 行 stderr**，`--full` 取全量至 1 MB 上限 |
| `ctl history tail <id>` | 对未结束条目做实时流式追加（JetStream 订阅） |

**记录范围**：
- **调用类（`kind=call`）**：`run` / `push` / `pull` / `expose` / `unexpose` / `ps` / `kill` / `tag`。
- **管理类（`kind=admin`）**：`session create` / `rm` / `kick` / `rotate-pin` / `login`。
- **拒绝类（`kind=admin_denied`）**：member 尝试 owner-only 操作。
- **`exec` 类（`kind=exec`）**：仅 meta（actor / 节点 / session / argv / 起止时间 / 退出码 / 窗口尺寸初值），无字节流。

### 5.5 配置与引导

- `ctl config init` — 生成带注释的示范 YAML 到 `~/.config/ctl/config.yaml`。
- `agent config init` — 同理，生成 agent 侧示范配置。
- 配置文件**允许用户手动编辑**；未知字段 warn 不 error，字段缺失用默认值。

### 5.6 扩展性（一期埋点）

- CLI 使用 cobra 风格子命令注册表；每个命令一个独立源文件。
- controllerd 侧分模块 handler；NATS 主题表统一收口。
- agent 侧 RPC 方法注册表；每个能力一个文件。
- 共享消息 schema 放在独立 `api/` 包（protobuf 或 JSON Schema）。
- 保留外部 plugin 发现扩展点（未来支持 `ctl foo` → 查 `$PATH` 的 `ctl-foo`）。
- 节点标签命名空间前缀预留（`system:` / `user:` / `session:`）。

---

## 6. 非功能需求

### 6.1 规模与拓扑假设

- 当前规模 ≤ 10 台受控节点，设计上允许扩到数十至上百而不需要架构重构。
- 节点动态加入 / 退出 / 断线 / 崩溃**随时发生**，按最悲观假设设计。

### 6.2 网络假设

- 所有节点可主动出公网（任意端口）。
- 公网机 **不能** 主动连回节点。
- 控制面**一律走 `wss://broker-domain:443`**（为未来可能的代理 / TLS 审计预留）。
- 数据面**统一走 FRP**（frps 中转），不做 LAN 直连 / NAT 打洞 / 选路切换。
- 公网机带宽假设不为瓶颈。

### 6.3 鉴权与安全

- 传输层：TLS（Let's Encrypt 签发）。
- 应用层：**公私钥挑战-响应**（借鉴 SSH userauth_publickey）。
  - 身份 = ed25519 pubkey；fingerprint = `SHA256:<base64-no-pad>`。
  - **无全局用户表**；只有每个 session 自己的 allowed_pubkeys。
  - 首次 login 用 PIN 挑战，成功后加 pubkey 到列表；之后免 PIN。
- PIN 哈希：**Argon2id**，参数 `memory=64 MiB, iterations=3, parallelism=2, salt=16 B, key=32 B`。存储 PHC 格式。
- mTLS：一期暂不做（未来 TLS 审计环境考量）。
- PIN 字符集限制：ASCII 可打印（0x20–0x7E），拒绝中文 / emoji / 控制字符；不做长度 / 复杂度校验。

### 6.4 韧性要求

- 任一控制组件（broker / controllerd / ctl）崩溃时，**托管进程继续运行**。
- 任一控制组件恢复后，**自动对账**恢复可观测性（不需要人工干预）。
- 网络短暂断连（≤15 分钟级别）后 agent 自动重连，状态无损。
- 节点整机宕机时：controllerd 租约过期标 DOWN，**仅告警，一期不做重调度**。

### 6.5 性能预期（非严格 SLA）

- 心跳间隔 5s；租约超时 15s（→ STALE）；断连 60s 后 → OFFLINE。
- `ctl exec` 端到端延迟目标：< 100 ms（本地→公网机→节点的 NATS 单跳）。
- `push` / `pull` 分块大小：1 MiB，断点续传颗粒度 1 MiB。
- 一期支持 PTY 输出速率 ≥ `htop` 级（每秒数十刷新）。

### 6.6 无权限约束（受控节点）

- 能跑用户态二进制（静态编译首选）。
- 能监听 > 1024 端口（但系统不需要；agent 只主动拨号）。
- **无** TUN 权限 → 不采用真 VPN overlay。
- 不假设 systemd / crontab / 容器；提供 systemd --user 示范但非必需。
- 家目录 `$HOME` 有写权限。
- 启动方式由用户决定（手动 / crontab @reboot / systemd --user / 登录脚本）；agent 自带重连循环，具体启动时机对系统无差别。

### 6.7 版本兼容

- **三端（ctl / controllerd / agent）强制同版本**（major.minor.patch 全一致）。
- 握手时携带版本号；不一致 → 拒绝连接并返回明确错误提示。
- 同仓库同 commit 产物；升级时全栈滚动重启。

---

## 7. 接口约定（CLI / 消息 / 配置）

### 7.1 CLI 总语法

```
ctl <subcommand> <node> [args...]
```

- 严格 1:1 节点语义；**不做 fan-out**（多节点用户自己 shell 循环）。
- `ctl run` 可省略：`ctl <node> "cmd"` 等价于 `ctl run <node> "cmd"`。
- 默认使用 current session；`--session <name>` 单次覆盖。
- stdin / stdout / stderr 像 `ssh` 一样完整 pass-through。
- 标签筛选：`ctl nodes --filter gpu=a100` 返回名字列表，不做扇出。

### 7.2 配置文件（`~/.config/ctl/config.yaml` / `~/.config/agent/config.yaml`）

- 允许手写；`ctl config init` / `agent config init` 生成带注释模板。
- 字段优先级：**命令行 flag > 环境变量 > 配置文件 > 默认值**。
- 核心字段示例（ctl 侧）：
  ```yaml
  broker: wss://example.com:443
  key_path: ~/.config/ctl/id_ed25519
  session: my-session   # 可选
  ```
- 环境变量：`CTL_BROKER` / `CTL_KEY` / `CTL_SESSION` / `CTL_CONFIG`（agent 同理 `AGENT_*`）。
- 未知字段：**warn 不 error**（宽容兼容）。

### 7.3 身份 / 密钥文件

- `~/.config/ctl/id_ed25519`（私钥，不存在时首次运行生成）。
- `~/.config/ctl/id_ed25519.pub`（公钥）。
- `~/.config/ctl/current_session`（当前 session 名，纯文本一行；可被 `CTL_SESSION` 覆盖）。
- 支持 ssh-agent 集成（可选，一期可不做）。

### 7.4 Session 命名规则

- 字符集：`[a-z0-9-]`，全小写；长度 1–32。
- 必须以字母开头，不能以 `-` 结尾。
- **全局唯一**（controllerd 范围内）。
- 保留字（禁止创建）：`default`, `system`, `admin`, `global`, `all`, `*`；前缀 `system-` / `ctl-` 保留。

### 7.5 Node 命名规则

- agent 启动时自报（CLI flag 或 YAML 配置）。
- 在 **本 session 内唯一**；冲突时 controllerd 拒绝注册。
- 一期不支持改名（如需改，先 `kick` 再重新注册）。

### 7.6 NATS 主题规划（待架构文档细化）

- `ctl.request.<session-id>.<verb>` — ctl 发起的请求。
- `agent.event.<session-id>.<node>.<kind>` — agent 事件上报。
- `heartbeat.<session-id>.<node>` — 心跳。
- `history.<session-id>.calls.<call-id>.{meta,stdout,stderr}` — history 流（JetStream）。
- `events.nodes.>` / `events.agents.>` — 全局事件流（JetStream）。

---

## 8. 持久化与数据模型

### 8.1 Controllerd SQLite

- **`sessions`**：`id, name, owner_pubkey, pin_hash (argon2id PHC), created_at, allowed_pubkeys (JSON)`。
- **`nodes`**：`id, session_id, name, last_hello_at, status, tags (JSON), ...`。
- **`processes`**：`process_id, session_id, node_id, argv, cwd, env, started_at, status, exit_code, ...`。
- **`port_allocations`**：`remote_port (PK), session_id, node_id, local_port, expose_id, allocated_at`。**全局单调分配**（跨 session 共享）。
- **`history`**：`id, session_id, actor_pubkey_fp, actor_display_name, kind (call/exec/admin/admin_denied), node_id, argv, invocation, started_at, ended_at, exit_code, stdout_ref, stderr_ref`。

### 8.2 JetStream 流

| 流 | Subject | Retention | max_age | max_bytes | storage | discard | 说明 |
|---|---|---|---|---|---|---|---|
| `events` | `events.nodes.>` / `events.agents.>` | limits | 30d | 1 GB | file | old | 节点连接事件、agent 告警、frpc 崩溃 |
| `history-<session-id>` | `history.<session-id>.>` | limits | ∞ | ∞ | file | new | 每 session 一条；session rm 时 **DELETE stream**（原子） |

- 公共参数：`replicas=1`、`duplicate_window=2m`、`ack_wait=30s`、`max_deliver=5`。
- 一期单公网机，不做多副本。

### 8.3 Agent 本地持久化

- `$HOME/.ctl/processes/<process_id>/`
  - `meta.json`（argv、env、cwd、started_at、PID、截断日志路径）
  - `stdout.log`（截断后 1 MB，覆盖写入）
  - `stderr.log`（同上）
- `$HOME/.ctl/state.json`（活跃托管进程列表）
- `$HOME/.ctl/agents/<agent-id>/frpc/`（frpc 配置与日志目录）

### 8.4 截断策略

- **存储**：`run` 的 stdout/stderr 各保留最后 1 MB；超出部分覆盖 / 丢弃；`--max-log 10M` 覆盖；`--no-capture` 关闭。
- **展示**：`ctl history show <id>` 默认展示 tail 40 行；`--lines N` / `--full` 覆盖。
- `exec` 不存字节流（见 §5.4）。

---

## 9. 鉴权与权限

### 9.1 挑战-响应流程

1. 客户端（ctl 或 agent）建立 WSS 连接。
2. broker / controllerd 发送 challenge nonce。
3. 客户端用本地 ed25519 私钥签名 `nonce + session_id + timestamp`，随 pubkey 一同提交。
4. controllerd 核对：
   - 签名有效性。
   - pubkey 是否在目标 session 的 `allowed_pubkeys` 中。
5. 通过 → 建立授权通道；失败 → 关闭连接并记录失败事件。

### 9.2 首次加入（login）

1. ctl `session login <name>` → 交互输入 PIN。
2. controllerd 用 Argon2id 验 PIN 哈希。
3. 通过 → 把 ctl 的 pubkey 加到 session 的 `allowed_pubkeys`。
4. 之后该 pubkey 可直接挑战-响应免 PIN。

Agent 注册流程同理（使用 agent 自己的 pubkey + 同一 session PIN）。

### 9.3 权限模型

- **二元权限**（可访问 / 不可访问），无角色层级。
- **Owner**（session 创建者）可：`kick` / `rm` / `rotate-pin`。
- **Member**（允许列表中所有 pubkey）可：session 内**所有节点**的全部操作（`run` / `exec` / `push` / `pull` / `expose` / `ps` / `kill` / `history` / `tag`）。
- **同 session 内 member 之间无权限隔离**（bob 可 kill alice 起的托管进程）。
- member 尝试 owner-only 操作：返回权限错误 + 写 `kind=admin_denied` 审计 history。

### 9.4 Owner key 丢失

- **系统不提供恢复机制**。
- 丢 key 后该 session 不可管理；若严重阻塞，走 controllerd 本地 SQL 手工修复（运维动作，非产品能力）。

### 9.5 数据面安全边界

- frps 共享端口池，**不按 session 隔离**。
- 跨 session / 外部连接到 `broker-domain:<port>` 在 TCP 层**可达**。
- 应用层安全由用户自己在托管进程内解决（jupyter token、TLS 终端、应用 auth）。
- 这是明确的权衡：简化架构 > 数据面多租户隔离。

---

## 10. 生命周期与韧性

### 10.1 主动 vs 被动事件

| 事件 | 触发方 | 是否清场 | 是否 kill 托管进程 | frpc 处理 |
|---|---|---|---|---|
| 网络短暂断连 | 网络 | ❌ 被动，保留 | ❌ setsid 保护 | ❌ frpc 自重连 |
| agent 进程崩溃 | 意外 | ❌ 被动 | ❌ setsid 保护 | agent 重启后重拉 |
| 本地 kill agent（任何信号） | 节点用户 | ❌ 被动 | ❌ | ❌ |
| frpc 子进程崩溃 | 意外 | - | - | agent 监督，指数退避自动重启；复用原 remote_port |
| `ctl agent kick <id>` | owner | ✅ 主动 | ✅ agent 退出前 kill 所有托管进程 | 自己的 frpc 子进程一并 kill |
| `ctl session rm <name>` | owner | ✅ 主动 | ✅ 各 agent kill 所有托管进程 | 各自的 frpc kill |
| 节点整机宕机 | 硬件 / OS | 全死 | 全死 | 全死 |

**`rm` / `kick` 必须交互确认**（`Are you sure? [y/N]`）。

### 10.2 端口回收对账

- agent OFFLINE 后 controllerd 可将其 `remote_port` 回收分配给他人。
- agent 重连时 controllerd 做对账：
  - 端口仍属于该 agent → 保留。
  - 端口已分配给他人 → controllerd 返回"该 expose 已失效"，agent 本地 invalidate，用户需重新 `expose`。
- 事件进 `events.agents.>` 流，用户可通过 `ctl agent show` 感知。

### 10.3 状态机

- Node：`ONLINE → STALE（心跳超时 15s）→ OFFLINE（断连 60s）`。
- 托管进程：`RUNNING → EXITED(exit_code) / ORPHAN（agent 不再汇报但仍有记录）`。

### 10.4 重建协议

- agent 重连发 `hello` 消息，附带托管进程列表 `[{process_id, status, pid, started_at}]`。
- controllerd 与本地 SQLite 记录比对：
  - 未知条目 → 重新纳管。
  - 已记录但 agent 不汇报 → 标 ORPHAN。
- controllerd 重启后：从 SQLite + JetStream 重建视图，与在线 agent 走一遍对账。

### 10.5 一期不做

- 多 broker / 多 controllerd HA 集群。
- 跨公网机的 DNS failover。
- 托管进程的自动重调度（仅告警，由人决定重启）。

---

## 11. 一期范围（v1 scope）

### 11.1 一期必做

- 五种角色（broker / relay / controllerd / agent / ctl）全部实现。
- §5 全部功能需求。
- §9 鉴权（挑战-响应 + PIN login）。
- §10 主动/被动生命周期 + 对账协议。
- 完整 PTY 体验（`github.com/creack/pty` + `golang.org/x/term`）。
- 配置 `config init` 引导。
- JetStream `events` + `history-<session-id>` 两条流。
- 三端版本绑定（强制同 commit）。

### 11.2 一期不做

- Web dashboard（未来作为 ctl-plugin 独立发布）。
- MinIO / 对象存储旁路。
- 节点间 peer-to-peer 传输（全部 FRP 中转）。
- 多 broker / 多 controllerd 集群 / HA。
- 真 VPN overlay（Tailscale / WireGuard）。
- 外部二进制插件发现（`ctl-foo` fallback）。
- fan-out / 实验编排 / 上层 job 抽象。
- 托管进程自动重调度。
- Owner key 丢失恢复机制。
- ssh-agent 集成（可后续加）。
- PIN 自动轮换 / 一次性 PIN。

### 11.3 里程碑建议（供 `implementation-plan.md` 参考）

1. **M1 — 最小连通**：controllerd + agent + ctl 三端骨架 + NATS 握手 + `hello` + `ctl nodes`。
2. **M2 — session / 鉴权**：挑战-响应 + login + allowed_pubkeys + `session create/login/switch/list`。
3. **M3 — `run` 非交互**：agent 的 `run` RPC + history 条目 + SQLite 写入。
4. **M4 — `exec` PTY**：完整 PTY + SIGWINCH + 信号透传。
5. **M5 — 文件传输**：`push` / `pull` 分块 + 断点续传。
6. **M6 — 数据面 FRP**：frps 部署 + agent 内置 frpc 子进程 + `expose` 端口分配。
7. **M7 — 韧性与对账**：心跳租约 + 重连对账 + 端口回收。
8. **M8 — 审计 / history**：JetStream `events` 流 + `history-<session>` 流 + `kind=admin` 记录 + tail/show 展示层。
9. **M9 — 打磨**：错误信息、`config init`、测试覆盖、发布工作流。

---

## 12. 附录：需求追溯

本文档基于 `logs/log.md` 中的决策日志。主要追溯：

- **系统边界** → log.md §系统边界
- **Q1 规模 / 消息总线** → §5.1 / §6.1
- **Q2 网络 / 连通** → §6.2
- **Q3 公网机带宽 / FRP** → §4.2 / §6.2
- **Q4 原语集 / history** → §5.1 / §5.4
- **Q5 数据面** → §6.2
- **Q6 生命周期** → §10
- **Q7 PTY** → §4.5 / §5.1
- **Q8 查看形式（无 Web）** → §4.5 / §11.2
- **Q9 文件同步** → §5.1
- **Q10 无权限约束** → §6.6
- **Q11 鉴权** → §9
- **A1–A12 模糊点** → §3 / §7.4 / §7.5 / §9 / §10
- **T1/T2/T3 术语** → §3
- **O1–O6 遗漏** → §5.4 / §6.3 / §6.7 / §8.2 / §9

如对任何条款有疑问，应先回查 `logs/log.md` 对应编号的决策记录；该文档已冻结为历史，任何推翻需在此需求文档中显式声明变更并同步到 `architecture.md`。
