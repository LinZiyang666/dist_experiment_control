# tether 使用手册 · 使用者篇

> tether 的操作文档按角色拆成三册。**本册面向普通使用者 / 实验机管理员**（`ctl`、`agent`），
> 只讲"怎么连、怎么跑命令、怎么传文件、怎么排错"，**不涉及公网 broker 的部署与维护**。
>
> | 你的角色 | 看哪册 |
> |---|---|
> | 使用者（笔记本上跑 `ctl`）、实验机管理员（装 `agent`） | **本册** `docs/usage.md` |
> | broker 运维（部署 / 维护公网 broker） | [`docs/broker-ops.md`](broker-ops.md) |
> | 集群 HA（多 broker 高可用）：命令与概念 | [`docs/cluster.md`](cluster.md) |
> | 集群 HA：运维剧本（扩容 / 缩容 / quorum 逃生 / 迁移 / 备份 / 滚动升级） | [`docs/cluster-runbook.md`](cluster-runbook.md) |
>
> 架构设计请看 [`docs/architecture.md`](architecture.md)。
>
> 适用版本：v0.1.0+（公开 GitHub Release 已发布）。子命令名与 flag 兼容性遵循 SemVer；
> wire 协议变更走 `tether.v1.*` → `tether.v2.*` 的整次跨版本路径，详见 §8.3。
>
> **章节号沿用完整手册的原始编号**（例如本册 §7 只含 §7.7，§7.1–7.6 在 broker-ops.md）——这是有意为之：
> 三册共用一套编号，跨册引用（如"见 broker-ops §7.4"）一目了然。

---

## 目录

1. [总体心智模型](#1-总体心智模型)（§1.1 关键词汇表）
2. [安装与部署](#2-安装与部署)——本册含 2.1 ctl、2.2 agent、2.5 卸载、2.6 从源码构建；**broker 安装（2.3/2.4）见 [`broker-ops.md`](broker-ops.md)**
3. [配置文件](#3-配置文件)——本册含 3.1 ctl、3.2 agent；**broker.yaml / auth_callout（3.3/3.4）见 [`broker-ops.md`](broker-ops.md)**
4. [命令速查表](#4-命令速查表)
5. [完整命令参考](#5-完整命令参考)——本册含 5.1–5.4（全局 / version / completion / proxy-dial）与 5.8–5.19（agent / login / session / ps / exec / run / expose / proxy / push / pull / history / node）；**serve/admin（5.5/5.20）见 [`broker-ops.md`](broker-ops.md)；cluster/alert（5.6/5.7）见 [`cluster.md`](cluster.md)**
6. [日常使用场景](#6-日常使用场景)
7. [运维 / 维护](#7-运维--维护)——本册只含 7.7（网络盘挂死自救）；**7.1–7.6（broker 运维）见 [`broker-ops.md`](broker-ops.md)**
8. [升级](#8-升级)——本册含 8.1 升级路径、8.2 远程升级 agent、8.3 proto 升级；**broker 升级（8.4/8.5）见 [`broker-ops.md`](broker-ops.md)**
9. [错误码与故障排查](#9-错误码与故障排查)——本册含使用者侧条目；**NATS 连接 / admin（9.7/9.10）见 [`broker-ops.md`](broker-ops.md)，集群瞬态（9.7.1）见 [`cluster.md`](cluster.md)**
10. [常见问题 FAQ](#10-常见问题-faq)
11. [安全注意事项](#11-安全注意事项)
12. [开发与本地调试](#12-开发与本地调试)
- [附录 A：目录结构总结](#附录-a目录结构总结)（agent 主机；broker 主机见 broker-ops.md）
- [附录 C：版本与发布](#附录-c版本与发布)

> 附录 B（网络端口）在 [`broker-ops.md`](broker-ops.md)。

---

## 1. 总体心智模型

tether 是 "**SSH + 端口暴露**" 的 NAT 穿透控制面：在 NAT/防火墙后的 *agent* 节点
通过 NATS 反向连接到一台公网 *broker*，使用者（*ctl*）通过同一 NATS 把命令路由到
agent。三种角色全部由同一个二进制 `tether` 提供，子命令切换：

| 角色 | 子命令 | 部署位置 | 数量 |
|---|---|---|---|
| broker | `tether serve` | 公网服务器（需 sudo / 域名 / TLS） | 1；分布式 HA 时每个 broker 节点 1 个 |
| agent  | `tether agent` | 实验机（NAT 后） | N |
| ctl    | `tether login` / `exec` / `run` / `expose` / `ps` / `history` / ... | 使用者笔记本 | M |

> **单机 broker 是默认且完整支持的部署。** 一台 `tether serve` + 任意多 agent/ctl 即可跑全部
> 功能（exec / run / expose / push / pull / history / proxy），**不需要任何 `tether cluster`
> 命令**。只有要做多机高可用（HA，任一 broker 宕机仍可写）时才用到 `cluster` / `alert` /
> quorum 这些概念——它们对单机用户完全不可见（responder 不挂载、destructive gate 不触发、
> 告警 banner 不渲染）。没有集群需求就整段跳过 cluster.md §5.6 `cluster` 与 `docs/cluster-runbook.md`。

数据面由两条独立通道组成：

```
                  +-----------+
ctl  ──── NATS WSS 443 ────►   |           ◄── NATS 4222 (本机) ─── tether-broker
                  |  broker   |
   ◄── 暴露端口 14000-14999 ─ |           ◄── 反向 TCP 7000 ──── agent
                  +-----------+
```

- **控制面**：NATS（命令、注册、心跳、审计）。CLI 走 wss://，agent 走 wss://，
  broker 与本机 nats-server 走 nats://。
  分布式 HA 时 ctl/agent 的 `--nats-url` / `broker_url` 可写逗号分隔 seed list，
  例如 `wss://b1.example:443,wss://b2.example:443,wss://b3.example:443`；NATS
  客户端会自动发现/切换其它 server。broker 自己仍连本机 NATS
  `nats://127.0.0.1:4222`。
- **数据面**：内置 yamux-over-TCP 反向隧道（`internal/tunnel`）。`tether expose`
  把 agent 上的本地端口反向打到 broker 的 14000-14999 公网带，URL 形如
  `http://<broker>:<port>`。

身份模型：
- 每个用户、每个 agent 在 `~/.tether/keys/` 下持有一对 **Ed25519 nkey**，公钥指纹
  形如 `SHA256:abcd…` 即用户/节点的稳定身份。
- 加入 session 一次性出示 PIN，broker 把 nkey 公钥绑定到 (sid, role)，之后
  所有 NATS CONNECT 都用 nkey 签名鉴权（`auth_callout`）。

### 1.1 关键词汇表

| 术语 | 含义 |
|---|---|
| **broker**   | 公网中心节点，跑 `tether serve` + nats-server + Caddy。单机模式唯一；分布式 HA 时每个 broker 节点各跑一套。 |
| **agent**    | NAT 后被远程操作的实验机，跑 `tether agent`。一台一个进程，可多 session。 |
| **ctl**      | 使用者本地的 CLI（不长驻），跑 `tether login` / `exec` / `run` 等。 |
| **session (sid)** | 隔离单位 = 一组 agent + 一组使用者 + 一段 history。形如 `01HABC...`。 |
| **node (nid)**    | session 内 agent 的标识，operator 起的人类可读名（如 `gpu-01`）。 |
| **owner**    | 创建 session 的使用者，拥有 `session rm` / `node upgrade` 等高权限。 |
| **member**   | 持有 session PIN 加入的使用者，拥有 ps/exec/run/expose/history 权限。 |
| **PIN**      | 一次性入场券，仅首次 join 时使用，之后绑定到 nkey 公钥永久失效。 |
| **nkey**     | NATS 标准的 Ed25519 身份对，每个 (人) 或 (机器, sid) 一对。 |
| **fingerprint** | nkey 公钥的 SHA256 摘要，形如 `SHA256:base64nopad...`。 |
| **proto version** | wire 协议版本（`internal/proto.ProtoVersion`），不一致必须重装。 |
| **expose**   | "把 agent 上的本地端口反向暴露到 broker 公网端口" 的动作或对象。 |
| **JetStream**| NATS 的持久化流系统，tether 用它存 `history-<sid>` audit 流。 |
| **auth_callout** | NATS 的鉴权扩展机制，broker 通过它实现 nkey-based ACL。 |
| **G.1 reconcile** | agent 重连后 broker 端的状态同步算法（保留 RUNNING / EXITED）。 |
| **G.2 reconcile** | broker 重启后从 SQLite + agent 心跳重建集群视图的算法。 |

---

## 2. 安装与部署

`scripts/install.sh` 是三种角色的统一入口。**核心不变量**：脚本只落文件、生成
配置和 systemd unit，**永不启动任何进程**。脚本结束后 `pgrep tether` 必须为空。

默认从最新公开的 GitHub Release 拉二进制 + SHA256SUMS（无需 `--version`）；
脚本内部嗅探 `releases/latest` 的 301 重定向解析出当前 tag，再从
`releases/download/<tag>/` 取对应平台的 tarball。

```
基址：https://github.com/LinZiyang666/dist_experiment_control/releases/latest/download
```

### 2.1 ctl（使用者笔记本）

```bash
curl -fsSL https://github.com/LinZiyang666/dist_experiment_control/releases/latest/download/install.sh | sh
# 默认 --role ctl
# 优先写到 /usr/local/bin/tether（若可写），否则 ~/.local/bin/tether
```

下一步：

```bash
tether login --broker wss://<broker>:443 --session lab --pin <pin>
```

### 2.2 agent（实验机）

```bash
curl -fsSL https://github.com/LinZiyang666/dist_experiment_control/releases/latest/download/install.sh | sh -s -- \
  --role agent \
  --broker wss://<broker>:443 \
  --session lab \
  --pin <pin> \
  --nid lab-1
```

写入：
- `~/.local/bin/tether`
- `~/.tether/agent/lab/agent.yaml`（包含 broker_url + session + nid + tunnel_addr）
- `~/.tether/agent/lab/keys/`（首次 `tether agent` 启动时生成 nkey）

启动方式（**install.sh 不会自动启动**）：

```bash
# 方式 A：手动后台
setsid nohup ~/.local/bin/tether agent --session lab --pin <pin> \
  >> ~/.tether/agent/lab/agent.log 2>&1 &

# 方式 B：systemd --user
~/.local/bin/tether agent --install-user-service --session lab --nid lab-1
systemctl --user daemon-reload
systemctl --user enable --now tether-agent@lab.service
loginctl enable-linger $USER     # 用户登出后仍保活
```

### 2.5 卸载

```bash
# ctl
~/.local/bin/tether --help >/dev/null 2>&1 && rm -f ~/.local/bin/tether
# 或 install.sh --uninstall --role ctl

# agent
systemctl --user disable --now tether-agent@lab.service 2>/dev/null
~/.local/bin/tether agent --uninstall --session lab
rm -rf ~/.tether
rm -f ~/.local/bin/tether

# broker
sudo systemctl disable --now tether-broker nats-server caddy
sudo install.sh --role broker --uninstall
sudo userdel tether    # install.sh 不会自动删除系统用户
```

### 2.6 从源码构建（开发者用）

仅在你要 hack tether 本身、跑测试矩阵、或需要未发布的特性时才走这条路径；
日常运行 / 部署用 §2.1–2.2 即可（broker 安装见 [`broker-ops.md`](broker-ops.md) §2.3–2.4）。

```bash
git clone https://github.com/LinZiyang666/dist_experiment_control.git
cd dist_experiment_control
make build                 # 输出 ./bin/tether
make test                  # 单元 + 包内测试，~30s
make e2e-parallel          # 完整端到端矩阵；44 核开发机约 3 分钟，小型 CI 会自动降并行度
```

要求 `Go 1.25+`（由 `nats-io/jwt/v2 ≥ v2.8.1` 锁定），`CGO_ENABLED=0` 静态
二进制，跨平台构建见 §12.5。

---

## 3. 配置文件

### 3.1 `~/.tether/config.toml`（ctl，可选）

ctl 几乎不需要常驻配置；活动会话用 `~/.tether/current_session` 一行文件持久化
（`tether login -s` 会写、`tether logout` 会清空）。

环境变量：
- `TETHER_SESSION` — 临时覆盖活动会话（优先级高于 `current_session` 文件）
- `TETHER_HOME`    — 覆盖 `~/.tether`（用于多账号隔离 / 测试）
- `TETHER_NATS_URL` — 覆盖 broker URL（优先级：显式 `--nats-url` / `--broker` flag > 此 env > `~/.tether/broker_url` > cobra 默认）
- `TETHER_RUN_READY_TIMEOUT` — `tether run` 等待 agent PTY ready 的超时，默认 `20s`（需 ≥ agent attach deadline 15s + 网络 RTT）
- `TETHER_RUN_LIVENESS_TIMEOUT` — `tether run` 心跳看门狗超时，默认 `15s`（= 3 × 心跳间隔 5s）；设为 `0` 关闭看门狗
- `TETHER_DEV_NO_AUTH=1` — **仅本地开发**：跳过 nkey 生成与签名，匿名连接

### 3.2 `~/.tether/agent/<sid>/agent.yaml`（agent）

由 `install.sh --role agent` 写出，使 `tether agent --session <sid>` 不再需要任何
flag 即可启动：

```yaml
broker_url: wss://broker.example.com:443
session: lab
nid: lab-1
tunnel_addr: broker.example.com:7000

# 可选：file_transfer 是 `tether push` / `tether pull` 的路径策略。
# 不配（如上）= 开放：push/pull 可达 agent 用户能访问的任意绝对路径
# （与 run/exec 同等）。要收紧或关闭再写下面这块：
# file_transfer:
#   allow_roots:             # 仅允许这些绝对前缀（不配则全盘开放）
#     - /srv/local/<user>
#     - /tmp
#   # allow_roots: []        # 显式空列表 = 彻底禁用 push/pull
```

字段语义：

| 字段 | 含义 |
|---|---|
| `broker_url`  | NATS WSS 入口（=ctl 的 `--nats-url`） |
| `session`     | 这个 agent 服务的 session id |
| `nid`         | 节点 id；session 内必须唯一 |
| `tunnel_addr` | broker 反向隧道控制端口（默认 `host:7000`） |
| `file_transfer.allow_roots` | `tether push`/`pull` 路径策略：**键缺省 → 全盘开放**（= run/exec 触达）；**非空列表 → 收紧**到这些绝对前缀；**显式 `[]` → 禁用**（`transfer_disabled`） |
| `remote_fs.mode` | 网络盘挂死时 `exec`/`run` 的自救策略：`auto`（默认；无网络挂载时完全惰性）/ `off`（关闭，回今天的行为）。见 §7.7。 |
| `remote_fs.safe_dir` | 检测到挂载死掉且用户未指定 `--cwd` 时，子进程的本地替代 cwd；缺省优先选择经验证的 `os.TempDir()`，再尝试 `/tmp`、`/var/tmp`。 |
| `remote_fs.probe_timeout` / `.spawn_timeout` / `.wedge_ceiling` | 挂载存活探测超时（默认 `2s`）/ execve 启动窗口超时（默认 `30s`）/ 并发放弃 spawn 上限（默认 `64`）。 |

`allow_roots` **是可选收紧而非安全边界**：member 本就有不受限的 `run`/`exec`
（requirements §9.3），随时能绕过白名单。配了 `allow_roots` 时的比对方式：先
`EvalSymlinks` 解析目标路径的父目录，再做"前缀 + `/`" 严格匹配（中间目录软链允许、
解析后落在 root 内即可）。叶子层符号链接、TOCTOU、必须普通文件等**机制级加固在
所有模式下都生效**（`O_NOFOLLOW` + `lstat` 双道防线），防的是 agent 机器上的敌对
非 member 进程，与白名单是否配置无关。

可手动编辑；优先级：**显式 CLI flag > yaml > cobra 默认值**。

## 4. 命令速查表

> 下表含 broker / 集群命令以便速查，但其详解不在本册：`tether serve` / `admin *` 见
> [`broker-ops.md`](broker-ops.md) §5.5 / §5.20；`tether cluster` / `alert` 见
> [`cluster.md`](cluster.md) §5.6 / §5.7。使用者命令详解在本册 §5。

**怎么选命令**（按"我现在想干什么"分类）：

| 我想... | 用 |
|---|---|
| 装 / 卸载 tether | `install.sh` (§2) |
| 启停 broker / agent | `tether serve` / `tether agent` + systemd |
| 加入一个 session / 切换 session | `tether login` / `tether logout` / `tether ctx` |
| 创建 / 删除 session | `tether session create` / `tether session rm` |
| 看看远端跑了什么 / 占了哪些端口 | `tether ps` |
| 在远端跑一条命令 | `tether exec`（脚本类）/ `tether run`（交互类） |
| 杀掉远端进程 | `tether run` 里按 Ctrl-C；当前没有独立 `tether kill` 命令 |
| 把远端服务暴露到公网 | `tether expose` / `tether expose rm` |
| 把整组 agent 变成 Clash 订阅出口 | `tether proxy on/off` + `tether proxy sub create/ls/revoke` |
| 把文件上传/下载到远端 | `tether push` / `tether pull` |
| 查"过去发生了什么" | `tether history`（含 `--follow` 实时模式） |
| 给远端 agent 升级 | `tether node upgrade` / `... --all` |
| 在 broker 主机上做运维 | `tether admin sessions/nodes/audit/evict` |
| 管理分布式 broker 集群 | `tether cluster init/add/status/drain/remove/...`（cluster.md §5.6，完整流程见 `docs/cluster-runbook.md`） |
| 查看 / 确认 store-backed cluster 告警 | `tether alert ls` / `tether alert ack <dedup_key>` |
| 生成 shell 补全脚本 | `tether completion bash|zsh|fish|powershell` |

完整速查：

| 命令 | 角色 | 用途 |
|---|---|---|
| `tether version`                       | 任意 | 打印版本 + 平台 |
| `tether serve`                         | broker | 启动 broker 守护进程 |
| `tether agent`                         | agent  | 启动 agent 守护进程 |
| `tether agent --install-user-service`  | agent  | 写 systemd --user unit（不启动） |
| `tether agent --uninstall`             | agent  | 删 systemd --user unit |
| `tether agent join <invite> [--start]` | agent  | C2：用 OOB invite 入群（写 agent.yaml + 钉 account_pub；`--start` 在进程内拉起 daemon） |
| `tether agent config refresh --once`   | agent  | C2：拉一次签名 roster/seed bundle 刷新发现缓存（需已钉 account） |
| `tether agent doctor`                  | agent  | C2：自检 agent 配置 / 发现缓存 / 连通性 |
| `tether completion <shell>`            | 任意 | 生成 shell 补全脚本 |
| `tether login`                         | ctl    | 加载 nkey 身份；可选激活 session |
| `tether logout`                        | ctl    | 清空 `current_session` |
| `tether ctx`                           | ctl    | 打印当前激活 session |
| `tether session create <name> --pin X` | ctl    | 创建 session（自己成 owner） |
| `tether session ls`                    | ctl    | 列出可见 session |
| `tether session rm <sid>`              | ctl    | tombstone（owner 限定） |
| `tether ps [-a]`                       | ctl    | 当前 session 的进程 + 端口 |
| `tether exec [flags] <nid> -- argv ...` | ctl   | 非交互远程命令 |
| `tether run [flags] <nid> -- argv ...` | ctl    | 交互式 PTY |
| `tether expose <nid> --local P --name N [--remote-port R] [--no-rebuild] [--on-broker B]` | ctl  | 暴露端口（可选指定公网端口 R / 锁定 home broker） |
| `tether expose rm <nid> --name N`      | ctl    | 撤销暴露 |
| `tether expose explain <name>`         | ctl    | 看一个 expose 的 home / epoch / rebuild 策略（member 可读，复用 ps RPC） |
| `tether proxy on [--ha-policy …] / off` | ctl(owner) | 代理订阅总开关（自建机场，§5.15）。**cluster HA 模式可用**（C5：杀 home 自动 rehome + quorum-loss 降级）；`--ha-policy freeze-on-quorum-loss`（默认，/sub 继续服务）或 `disable-on-quorum-loss`（/sub 404） |
| `tether proxy status`                  | ctl    | 看开关/在线节点/订阅（member 可读，无密钥）；cluster 模式多一行 `CLUSTER: <state> ha-policy=… writable=…`（ACTIVE/FROZEN_READONLY/DISABLED_NO_QUORUM/FORCE_SINGLE） |
| `tether proxy sub create --name N`     | ctl(owner) | 签发订阅 URL（只打印一次） |
| `tether proxy sub ls / revoke <name>`  | ctl(owner) | 列 / 撤销订阅 |
| `tether push <local> <nid>:<remote>`   | ctl    | 上传本地文件到远端（≤2 GiB） |
| `tether pull <nid>:<remote> <local>`   | ctl    | 下载远端文件到本地（≤2 GiB） |
| `tether history [-n N] [--kind K] [-f]` | ctl   | 审计历史回放（含 `--kind transfer`） |
| `tether node ls [-a]`                  | ctl    | 列 session 内全部 agent（含 OFFLINE / STALE） |
| `tether node upgrade <nid>`            | ctl    | 升级单台 agent（owner） |
| `tether node upgrade --all`            | ctl    | 升级 session 内所有 ONLINE agent |
| `tether cluster status [--json|--offline|--watch <dur>]` | broker 本机 | 查看 cluster health / roster（`--watch` 每隔 ≥2s 重绘，Ctrl-C 退出；含 cert/容量字段 + cert 临期 advisory） |
| `… --wait`（每操作 flag）/ `tether cluster status --watch` | broker 本机 | 阻塞到 membership 操作完成 / 节点到达某 phase（取代旧的独立 `cluster wait`）；亦可 `cluster ops show <op-id>` |
| `tether cluster doctor`                | broker 本机 | 预检 cluster secrets |
| `tether cluster init --from-existing`  | broker 本机/离线 | 把单 broker 迁移为第一个 cluster voter |
| `tether cluster reconcile nats --all [--plan]` | broker 本机 | 接管 / 重渲染 NATS route + auth_callout 配置（自动路径，按 roster 渲染全 mesh；`--plan` = dry-run，打印将改动 + `--json`，**不写任何文件**） |
| `tether cluster transfer-leader <node> [--wait]` | broker 本机 | 转移 Raft leadership（`--wait` 阻塞到 leadership 落到目标） |
| `tether cluster join prepare` / `join approve <bundle>` | joiner / leader | 两阶段接纳新 voter（joiner 出自签 bundle，leader approve） |
| `tether cluster seeds publish --bootstrap <https-url> --endpoint <nats-url>…` | broker 本机(leader) | C2：发布签名 SeedBundle（client-dialable endpoints + bootstrap URL），供 `agent join`/`doctor` 消费 |
| `tether cluster seeds show [--json]`    | broker 本机 | C2：读回已发布的 seeds（leader-agnostic） |
| `tether cluster keygen --out <path>`   | broker 本机 | 生成 node identity seed（`node-pub` 现为隐藏 debug 命令） |
| `tether cluster drain` / `retire`      | broker 本机 | 迁移（drain）或退役（retire，原 `drain --retire`）broker voter |
| `tether cluster rebalance proxy [--dry-run]` | broker 本机(leader) | 主动把 `__proxy__` homes 均摊到各 eligible voter（reaper 只在 home down 时迁；加完 broker 后跑此填新容量；`--dry-run` 预览） |
| `tether cluster rotate-tunnel-cert`    | broker 本机 | 轮换 broker 稳定 tunnel 证书 pin |
| `tether cluster recovery force-single` / `recovery rejoin prepare` | broker 离线 | quorum-loss 逃生 / returning-node 清理 |
| `tether cluster backup [--offline] --out <dir>` | broker 本机/离线 | 写 `{state.db, manifest.json}` 备份 bundle（online 任意节点；`--offline` daemon 停止时） |
| `tether cluster recovery restore <bundle> --confirm-node-id <id>` | broker 离线 | 从 bundle 恢复为单 voter cluster（不可逆、typed-confirm、再用 cluster-runbook.md §1 join 流程长回去） |
| `tether cluster recovery incident export [--since <dur>] [--out f.json]` | broker 本机 | 导出只读取证 bundle（告警 + membership + 审计）；对 secret-shaped key 做**尽力**脱敏（非保证——分享前请人工复核；写文件用 O_EXCL+O_NOFOLLOW 防符号链接覆盖） |
| `tether cluster ops ls` / `ops show <node>` | broker 本机 | 查看 membership 操作（add/drain/retire）状态 + resume 提示（派生自 roster） |
| `tether cluster apply -f roster.yaml [--json]` | broker 本机 | 对期望 roster 差分、印 quorum-safe 收敛计划（**仅 plan、不执行**） |
| `tether cluster doctor [--offline]` | broker 本机 | 诊断集群——daemon 在则 online 健康检查，否则 init 前 preflight |
| `tether alert ls`                      | ctl    | 列出当前 active store-backed cluster alerts |
| `tether alert ack <dedup_key>`         | ctl    | 确认一个 store-backed cluster alert |
| `tether admin sessions`                | broker 本机 | 列所有 session |
| `tether admin nodes`                   | broker 本机 | 列所有节点 |
| `tether admin audit <sid> [-n N]`      | broker 本机 | 审计尾部 N 条 |
| `tether admin evict <sid> <nid>`       | broker 本机 | 强制踢节点 |

---

## 5. 完整命令参考

### 5.1 全局 flag

常用 ctl 子命令接受：

| flag | 默认 | 说明 |
|---|---|---|
| `--nats-url`  | 显式 flag > `TETHER_NATS_URL` > `~/.tether/broker_url` > `nats://127.0.0.1:4222` | 连 broker 的 NATS 入口（ctl 通常 `wss://broker:443`；cluster 可写逗号分隔 seed list） |
| `--home`      | `~/.tether`             | tether 的家目录；ctl 从这里读 nkey、`current_session`、默认 broker URL |

`tether agent` / `tether serve` 各自还有专属 flag，详见下文。

`tether exec` 与 `tether run` 均开启 `SetInterspersed(false)`：第一个位置参数后
所有 `-flag` 都视为远程 argv 的一部分，避免 cobra 把 `ls -la` 当成自己的参数解析
失败。

### 5.2 `tether version`

**这是什么**：打印 tether 二进制的 release version、proto wire 版本、
平台 (`linux/amd64`)、Go 编译器版本。**不需要任何配置或网络**。

**何时用**：
- 排查"我装的是哪一版"；
- 提 issue 时随手贴；
- 确认 `tether node upgrade` 是否生效（升级前后跑一次对比）；
- 确认 broker / agent 的 proto 版本是否一致（不一致需要重装而非 upgrade）。

```
tether version
```

输出示例：
```
tether v0.0.0-dev (proto v2)
linux/amd64
go1.25.0
```

### 5.3 `tether completion`

**这是什么**：生成 shell 补全脚本。它是 Cobra 自动提供的本地命令，不连接 broker，
不读取 session，也不受 cluster 状态影响。

```
tether completion bash
tether completion zsh
tether completion fish
tether completion powershell
```

参数：

| 参数 / flag | 默认 | 说明 |
|---|---|---|
| `bash` / `zsh` / `fish` / `powershell` | (必填) | 目标 shell 类型；输出写到 stdout，由你按 shell 规则安装 |
| `--no-descriptions` | false | 生成补全脚本时不包含命令描述；bash/zsh/fish/powershell 都支持 |

### 5.4 控制面代理（proxy-aware dial）

**这是什么**：让 tether 的**控制面 NATS 连接**经一个本地代理（如 Clash `127.0.0.1:7897`）出网。**默认不开**——只有设了代理 env（`ALL_PROXY`/`HTTPS_PROXY`/`HTTP_PROXY`）时才生效；没设时行为与以前逐字节一致。

**何时用**：NAT 后的笔记本（尤其 **WSL 镜像网络**）连墙外 broker 时 `tether ps`/`exec` 永久 hang。根因：broker 域名被本地代理（Clash 等）解析成 **fake-ip**（`198.18.x.x`），而 WSL 的透明 TUN 承载不动 fake-ip 流量 → TLS 握手卡死。设了代理 env 后，tether 把**域名**交给代理做**远程 DNS**，绕开本地 fake-ip。Mac 原生 TUN 能承载 fake-ip 故无此问题。

**支持的 env**（每个 key 大写优先于小写；控制面恒为 TLS 目标）：

| env | 用途 |
|---|---|
| `HTTPS_PROXY` | 控制面（TLS）首选 |
| `ALL_PROXY`   | 次选（catch-all） |
| `HTTP_PROXY`  | 末选 |
| `NO_PROXY`    | 直连旁路列表（见下） |

优先级是 **per-key**：`HTTPS_PROXY` > `ALL_PROXY` > `HTTP_PROXY`，每个 key 自身**大写优先于小写**、首个非空胜（故小写 `https_proxy` 仍胜过大写 `ALL_PROXY`）。

**支持的 scheme**：`http://`/`https://`（HTTP CONNECT）、`socks5://`/`socks5h://`。**`socks5://` 与 `socks5h://` 在 tether 里等价——都让代理做远程 DNS**（这正是绕开 WSL fake-ip 的关键）。设了代理 env 但 URL 不可解析/不支持 → **直接报错**（fail-closed，不会静默裸连再 hang）。

**NO_PROXY 规则**（只对**目标 broker host** 判定，不影响到代理本身的连接）：
- 内建旁路：`localhost` / `127.0.0.0/8` / `::1` **即使 NO_PROXY 没写也直连**——故本地 dev broker `nats://127.0.0.1:4222` 永不走代理。
- `*` = 全部直连；精确 host（大小写不敏感）；`example.com` 匹配该域**及其子域**；`.example.com` 只匹配子域；CIDR（如 `10.0.0.0/8`）**仅对 IP 字面量 broker URL 生效**（域名 broker 走远程解析、CIDR 不命中）。

**典型 WSL + Clash 7897 例子**：
```bash
export HTTPS_PROXY=http://127.0.0.1:7897      # 或 socks5h://127.0.0.1:7897
tether ps                                      # 经代理连 weiland.top:443，不再 hang
```
带认证：`export HTTPS_PROXY=socks5://user:pass@127.0.0.1:7897`（HTTP 代理用 `http://user:pass@...`，发 `Proxy-Authorization`）。

**TLS 不破**：代理只承载**裸 TCP**，TLS 握手 / SNI / 证书校验仍由 tether **端到端**做（SNI 取自 broker URL 的主机名）——代理看不到明文、无法 MITM。

**角色覆盖**：**ctl 与 agent 都认**这些 env；**broker 不认**（它是 server，不外拨）。

**范围边界（本版）**：**只代理控制面**（命令/注册/心跳/审计/历史/文件传输——都走同一条 NATS 连接）。**数据面反向隧道**（`tether expose` 的 agent→broker:7000、端口暴露字节、proxy 订阅流量）**本版不经代理**，登记为可选 follow-up。

**排查**：先验代理本身通：`curl -x $HTTPS_PROXY https://<broker>:443 -k`（能连上代理即说明 7897 可用）。代理不可达/认证失败/`NO_PROXY` 误把 broker 列进去导致仍直连，错误信息会点名（凭据不回显）。

### 5.8 `tether agent`

```
tether agent --session <sid> [--nid <nid>] [--pin <pin>] [flag...]
```

**这是什么**：agent 守护进程。每台 NAT 后的实验机（"被远程操作"的目标
机）跑一个，反向连接到 broker、注册自己、心跳、接收 exec/run/expose 命令、
启动反向隧道、上报进程状态。

**核心职责**：
1. **注册与心跳**：以 (sid, nid) 标识自己 → broker；
2. **接收远程命令**：处理 ctl 的 exec / run（PTY）/ expose / kill / upgrade
   请求；
3. **管理本地子进程**：fork+exec、PID 跟踪、退出回报、孤儿清理（G.1
   reconcile）；
4. **反向隧道客户端**：在 broker `:7000` 上建立长连接，agent.yaml 的
   `tunnel_addr` 指定地址；
5. **自更新**：收到 `upgrade.req` 后下载新二进制 → 校验 SHA256 → 原子替换
   → `syscall.Exec` 原地重启（PID 不变，systemd 不感知）。

**何时用**：实验机管理员部署后通过 `setsid nohup` 或 systemd --user 拉起，
此后长驻。普通使用者不需要直接运行。

`--session` 必填；`--nid` 可由 agent.yaml 提供。`--pin` 仅首次连接（绑定 nkey
↔ (sid, nid)）需要，之后省略。

| flag | 默认 | agent.yaml 等价 |
|---|---|---|
| `--nats-url`            | `nats://127.0.0.1:4222` | `broker_url` |
| `--session`             | (必填)                 | `session` |
| `--nid`                 | (必填或 yaml)           | `nid` |
| `--pin`                 | (空)                   | — (不持久化) |
| `--tunnel-addr`         | `127.0.0.1:7000`       | `tunnel_addr` |
| `--install-user-service`| false                  | — |
| `--uninstall`           | false                  | — |

`--install-user-service` 会把 CLI 上**显式**给的 `--nats-url` / `--tunnel-addr`
写进 `ExecStart`，cobra 默认值不写（默认值由 agent.yaml 提供）。`TETHER_HOME`
环境变量也会通过 `Environment=` 写进 unit。

分布式 HA 影响：`broker_url` / `--nats-url` 可以写逗号分隔 seed list，例如
`wss://b1.example:443,wss://b2.example:443,wss://b3.example:443`，NATS client 会自动
重连和发现其它 broker。`--tunnel-addr` / `agent.yaml tunnel_addr` 是单 broker
和未 homed expose 的 fallback；cluster 模式下每个 expose 会从 broker 收到 home
directive。这里的 home directive 是 broker 下发给 agent 的隧道目标；home broker 是
当前负责某个 expose 公网 listener 的 broker；epoch 是每次 rehome 递增的版本号，防止
旧指令覆盖新指令。agent tunnel 会按该 expose 的 home broker 地址和 cert pin 建连，
broker drain / rehome 时按 epoch 更新，不需要重新跑 `tether expose`。

`TETHER_DEV_NO_AUTH=1` 时 agent 以匿名身份连接，**仅在 broker 同样未启用
auth_callout 时**可用。

### 5.9 `tether login` / `logout` / `ctx`

```
tether login                                # 仅生成/加载 nkey
tether login -s <sid>                       # 激活已加入的 session
tether login -s <sid> --pin <pin>           # 首次加入：PIN 验证 + 激活
tether logout                               # 清空 current_session（不解绑成员）
tether ctx                                  # 打印当前激活 session
```

**`tether login` 是什么**：使用者身份初始化命令。完成两件事：
(a) 在 `~/.tether/keys/default.nk` 加载或生成你的 Ed25519 nkey 身份对
（首次跑会自动生成，0600 权限）；
(b) 可选：用 PIN 加入指定 session 并把它设为本机的"活动 session"
（写 `~/.tether/current_session`）。

PIN 仅在**首次加入**时使用 —— broker 会把 PIN ↔ nkey 公钥的绑定持久化到
`session_members`，后续 `login -s <sid>` 不带 `--pin` 也能直接激活。
所以 PIN 是一次性入场券，不要长期保存。

**`tether logout` 是什么**：清空 `current_session` 文件，仅影响**本机**
当前 session 选择，**不会**从 broker 端解除会员关系（要彻底踢人需要 owner
跑 `tether admin evict`）。等价于 "切换到无 active session 状态"，幂等
（重复调用安全）。

**`tether ctx` 是什么**：打印当前激活 session 的 sid，没有则静默退出 0。
用于 shell prompt / 脚本里的 `[ -n "$(tether ctx)" ]` 之类条件判断。
读取顺序：`$TETHER_SESSION` 环境变量 > `~/.tether/current_session` 文件。

`login -s` 会真实地走一次 NATS CONNECT（auth_callout 验证），只有连接成功才会
写 `~/.tether/current_session`；失败时不留痕。

flag：

| 命令 | flag | 默认 | 说明 |
|---|---|---|---|
| `login` | `--nats-url` | 同 §5.1 全局解析链 | NATS broker URL；分布式 HA 可写逗号分隔 seed list |
| `login` | `--broker` | 同 §5.1 全局解析链 | `--nats-url` 的别名（与 `install.sh --broker` 拼写一致；两者共享同一变量，写任一都生效） |
| `login` | `--session, -s` | (空) | 要激活的 session id |
| `login` | `--pin` | (空) | 首次加入时的一次性 PIN；只在同时给 `--session` 时有意义 |
| `login` / `logout` / `ctx` | `--home` | `~/.tether` | 读取/写入 nkey、`current_session`、默认 broker URL 的目录 |

`login` 显式给 `--nats-url` 或 `--broker` 之一时，broker URL 会被持久化到
`~/.tether/broker_url`，之后所有 ctl 子命令默认读它（见 §3.1
`TETHER_NATS_URL` 的优先级链）。

### 5.10 `tether session`

**这一组命令是什么**：管理"会话"这个核心隔离单位。一个 session 是
"一组 agent + 一组使用者 + 一段历史" 的容器：
- 同 session 内的 member 都能看到彼此的 ps / history / expose；
- 跨 session 完全隔离（NATS subject ACL + auth_callout 强制）；
- session 是计费、配额、生命周期回收的最小颗粒。

典型用法：每个项目 / 每个实验组开一个 session，owner 把 PIN 发给协作者。

```
tether session create <name> --pin <pin>    # 创建并自动激活；调用者成 owner
tether session ls                           # 列出可见 session
tether session rm <sid> [--ack-alerts]      # tombstone：ACTIVE → DELETING（owner）
```

**`session create` 是什么**：申请一个新的 sid（broker 自动分配，形如
`01HABCXYZ...`），把调用者注册为 owner，同时把 sid 写进本机
`current_session` 自动激活。`--pin` 必填且**只能由 owner 在创建时
设定**，之后无法修改 PIN（要换 PIN 只能 `session rm` 重建）。

**`session ls` 是什么**：列出当前 nkey 身份**有权访问**的所有 session
（owner 或 member 都算）。输出含状态（ACTIVE / DELETING）、角色
（owner / member）、活动标记（`*` 表示当前激活）、创建时间。

**`session rm` 是什么**：owner-only，把 session 标 `DELETING` 并触发三
阶段回收（见下）。**不可逆**：tombstone 后无法 undo，全部 history、port
分配、agent 注册都会被清理。

`session rm` 是三阶段删除（P7 architecture H.5）：
1. broker SQLite 把 state 设为 DELETING；
2. broadcast `sys.events{type:session_deleting}` ⇒ 所有 agent 拒绝该 sid 的新调用，
   主动断开 sid 范围内的 NATS subject；
3. 等待静默期后回收 JetStream stream（`history-<sid>`）+ port_allocations + 全部
   agent_provisioning 行。

`tether session ls` 输出含 `STATE`（ACTIVE / DELETING）+ `ROLE`（owner / member）+
当前 active 标记 `*`。

flag：

| 命令 | flag | 默认 | 说明 |
|---|---|---|---|
| `session create` | `--pin` | (必填) | 新 session 的一次性加入 PIN；owner 创建后无法修改 |
| `session rm` | `--ack-alerts` | false | 分布式集群处于 `quorum_lost` 或 `force_single_active` severe alert 时仍继续删除；只确认本次风险，不会修复或清除告警 |
| 全部 `session` 子命令 | `--nats-url` | 同 §5.1 全局解析链 | NATS 入口；分布式 HA 可写逗号分隔 seed list |
| 全部 `session` 子命令 | `--home` | `~/.tether` | 读取 nkey、`current_session`、默认 broker URL 的目录 |

分布式 HA 影响：`session rm` 是破坏性写操作，ctl 会先查询 cluster health；
如果可达 broker 报告 `quorum_lost`（失去可写多数）或 `force_single_active`
（正在单节点逃生模式），默认拒绝执行，避免在不完整时间线里删除 session。
确认已经按 runbook 判断过风险后再加 `--ack-alerts`。`session create` 和
`session ls` 不需要这个覆盖；真正的写入仍由 broker 端权限和 quorum 规则最终裁定。

### 5.11 `tether ps`

**这是什么**：当前活动 session 的"实时态"快照 —— 所有 agent 上**正在跑
的进程** + **正在占用的公开端口**。类比 `ps -ef` + `ss -tlnp` 的合并视
图，但范围限定在当前 session 而不是单台机器。

**何时用**：
- 想知道远端跑了什么 → 看 PROCESSES 节；
- 想知道 expose 占了哪些公网端口 → 看 PORTS 节；
- 排查"我的 agent 还活着吗" → 看进程是否仍 RUNNING；
- 想看历史/已退出 → 加 `-a` 把 EXITED 也列出来。

数据来源：broker SQLite 的 `processes` + `port_allocations` 表，G.1
reconcile 保证 agent 重连后状态自愈。`ps` 是 read-only 的，不会触发
agent 上的任何动作。

```
tether ps        # 默认：活跃进程（RUNNING + LOST）+ ALLOCATED 端口
tether ps -a     # 含 EXITED 进程 + RELEASED 端口
```

flag：

| flag | 默认 | 说明 |
|---|---|---|
| `--all, -a` | false | 同时显示 EXITED 进程、释放/历史端口以及 OFFLINE / STALE 相关状态 |
| `--nats-url` | 同 §5.1 全局解析链 | NATS 入口；分布式 HA 可写逗号分隔 seed list |
| `--home` | `~/.tether` | 读取 nkey、`current_session`、默认 broker URL 的目录 |

LOST 是 read-derived 状态——broker 的 SQLite 行还停在 RUNNING，
但所属 node 已被 reconcile 转成 OFFLINE。`tether ps` 在响应阶段
合成 LOST 标签，让运维一眼能看出"这个进程还没确认结束、但承载它
的 agent 跟丢了"。当 agent 重新 register 时，G.1 reconcile 会把
对应行收敛到 EXITED(rc=-1, missed-exit) 或重新接受 RUNNING。

分布式 HA 影响：`ps` 是 read-only，不需要 `--ack-alerts`，也不会改动任何
cluster 状态。若当前有 severe cluster alert，`ps` 会把 alert banner 打到 stderr，
stdout 的表格保持可脚本解析。PORTS 节里看到的 expose 可能已经 rehome 到其它
broker，但对用户仍表现为同一个 `NAME` / `PUBLIC` 端口。

输出两节：

```
PROCESSES
  PID                                  NODE   STATE    EXIT  STARTED  CMD
  proc-01HXYZ...                       lab-1  RUNNING  -     2m       sleep 60

PORTS
  NAME     NODE   LOCAL   PUBLIC  STATE      CREATED
  jupyter  lab-1  :8888   :14001  ALLOCATED  5m
```

无 active session 时直接报错并提示先 `tether login -s <sid>`。

### 5.12 `tether exec`

**这是什么**：在指定远端节点上跑一条**非交互**命令。等价于 SSH 的
`ssh user@host '<argv>'` 但走 NATS 控制面 + 不需要 SSH 登录。

**核心特性**：
- **不分配 PTY**：进程的标准输入是 /dev/null，stdout/stderr 是普通管道，
  适合脚本化批处理 / CI 流水线 / 一次性命令；
- **流式回传**：stdout / stderr 实时分流回本地（不是命令结束后一次性
  返回），可以 `tether exec gpu -- tail -f log.txt` 看实时日志；
- **退出码透传**：远端进程的 exit code 即本地 exit code，方便 `&&` /
  `||` 串联；
- **信号杀变 128**：远端进程被信号杀 ⇒ stderr 打印 "remote process
  terminated by signal" 然后本地 `exit 128`（当前不传具体信号号）。

**何时用**：`ls` / `grep` / `python script.py` / `make test` / 任何
"输入定死、输出可流" 的命令。

**何时别用**：vim、htop、less、有进度条 / cursor 控制的命令 → 用
`tether run`（PTY 模式）。

```
tether exec [--cwd DIR] [--timeout 10m] [--safe] <nid> -- <argv...>
```

flag：

| flag | 默认 | 说明 |
|---|---|---|
| `--nats-url` | 同 §5.1 全局解析链 | NATS 入口；分布式 HA 可写逗号分隔 seed list |
| `--home` | `~/.tether` | 读取 nkey、`current_session`、默认 broker URL 的目录 |
| `--cwd`     | (空 = agent 端默认目录) | 远端进程的工作目录 |
| `--timeout` | `10m`              | 整次命令最长时间（含流式输出），超时返回 `timed out after ...` |
| `--safe` | false | 在 agent 端用去掉疑似挂死网络挂载的 PATH 解析 argv[0]，避免 NFS/CIFS 卡死；必须写在 `<nid>` 之前 |

注：`--` 是可选分隔符（cobra 自动剥离）；`tether exec <nid> ls -la` 与
`tether exec <nid> -- ls -la` 等价，因为 `SetInterspersed(false)` 已让 `-la`
不被父命令解析。

分布式 HA 影响：`exec` 没有 ctl 侧 `--ack-alerts` 覆盖开关；它仍走当前
NATS seed 连接到任意可达 broker，由 broker 路由到目标 agent。若集群已经失去
写入能力，broker 端会按实际状态返回错误或超时；脚本不应把 `exec` 当成修复
`quorum_lost` 的运维入口，集群修复走 `tether cluster status/force-single/recover`。

### 5.13 `tether run`

**这是什么**：在指定远端节点上跑**交互式**命令，agent 端分配一个真正的
PTY，本地终端切 raw 模式后所有键盘字节、箭头序列、Ctrl-C、终端大小变
化（SIGWINCH）原样透传给远端进程组。等价于 `ssh -t user@host '<argv>'`。

**核心特性**：
- **PTY 分配**：远端进程的 stdin/stdout/stderr 都连到一个伪终端 → 程序
  会进入"我在终端里"的模式（颜色、cursor 控制、单字符读取都正常）；
- **raw mode 本地终端**：键盘字符不被本地 kernel 加工（不会触发本地
  Ctrl-C 中断 ctl 进程），原样发到远端；
- **SIGWINCH 同步**：本地终端 resize ⇒ remote PTY 同步 resize；vim / less
  会自动刷新；
- **两阶段 attach 握手**：架构 C.5.1，确保 PTY 的第一个字节不丢；
- **3 秒 attach deadline**：ctl 没在 3s 内完成订阅 ⇒ agent 主动放弃 PTY，
  防止 ctl 崩溃导致孤儿 PTY 占用 agent 资源。

**何时用**：vim / nano / htop / 进入 bash 交互式 shell / 跑带颜色或进度
条的程序 / 任何需要终端能力的场景。

**何时别用**：批处理 / 脚本 / 不需要终端的 grep / ls → `tether exec` 更
轻量（不消耗 agent 端 PTY 配额）。

```
tether run [--cwd DIR] [--safe] [--ack-alerts] <nid> -- <argv...>
```

flag：

| flag | 默认 | 说明 |
|---|---|---|
| `--nats-url` | 同 §5.1 全局解析链 | NATS 入口；分布式 HA 可写逗号分隔 seed list |
| `--home` | `~/.tether` | 读取 nkey、`current_session`、默认 broker URL 的目录 |
| `--cwd` | (空 = agent 端默认目录) | 远端 PTY 进程的工作目录 |
| `--safe` | false | 与 `exec --safe` 相同，先避开疑似挂死网络挂载再解析 argv[0]；必须写在 `<nid>` 之前 |
| `--ack-alerts` | false | 分布式集群处于 `quorum_lost` 或 `force_single_active` severe alert 时仍继续启动交互进程；只确认本次风险 |

非 tty 调用（如 `tether run nid -- bash <<<EOF ... EOF`）时 SIGINT 转成
`kill.req{SIGINT}` 发往 agent 的进程组。

分布式 HA 影响：`run` 会打开新的交互式远程进程，属于可能扩大损害面的写操作；
ctl 会先检查 cluster health。看到 `quorum_lost` 或 `force_single_active` 时默认拒绝，
除非显式加 `--ack-alerts`。这只绕过 ctl 侧提示，不会绕过 broker 端权限、session
状态或 quorum 能力检查。

调优 env vars（详见 §3.1）：
- `TETHER_RUN_READY_TIMEOUT` — ctl 等待 agent PTY ready 的总超时，默认 `20s`；
  慢链路 / 跨大陆链路上 PTY 首字节迟到时调大。
- `TETHER_RUN_LIVENESS_TIMEOUT` — 心跳看门狗超时，默认 `15s`（agent 心跳间隔
  5s × 3）；agent 长时间停滞时 ctl 本地会先打印 "agent unreachable" 再退出，
  避免无限挂起。设为 `0` 关闭看门狗。

### 5.14 `tether expose` / `expose rm`

**这是什么**：把 NAT 后 agent 上的某个本地端口反向打到 broker 的公网端
口，让公网客户端可以直接访问 agent 上的 HTTP/TCP 服务。等价于 `ngrok`
/ `frp` / `cloudflared tunnel` 但绑定到 tether 的会话和身份模型。

**典型场景**：
- 在 GPU 机上跑 Jupyter Lab → expose 8888 → 本地浏览器打开公网 URL；
- 远程机上的 Web demo / Grafana → expose；
- 临时 socat / netcat 互联调试。

**工作原理**：
1. ctl 调 broker 申请端口 → broker 从 `[14000-14999]` 分一个公网端口 +
   一次性 token；
2. broker 把 token 推给目标 agent；
3. agent 的 tunnel 客户端用 token 在 broker `:7000` 上建一条反向连接；
4. broker 把公网端口的所有 TCP 流量通过这条反向连接 yamux 多路复用到
   agent 的 `--local` 端口；
5. token 持久化到 `~/.tether/agent/<sid>/state.json` ⇒ agent 重启自动
   重连，URL 不变。

**安全注意**：暴露的公网端口**没有再次鉴权**（设计如此）。把内部服务
expose 出去等于把它放公网，敏感服务必须自带认证（Jupyter token、HTTPS
证书等）。

```
tether expose <nid> --local <port> --name <logical-name> [--remote-port <port>] [--no-rebuild] [--on-broker <node>] [--ack-alerts]
tether expose rm <nid> --name <logical-name> [--ack-alerts]
tether expose explain <name>
```

参数与 flag：

| 参数 / flag | 默认 | 说明 |
|---|---|---|
| `<nid>` | (必填) | 目标 agent 节点 id；必须在当前 session 内 ONLINE |
| `--local` | (必填) | agent 本机要暴露的 TCP 端口，必须为 `1..65535` |
| `--name` | (必填) | 当前 session 内的逻辑名；同一 session 内唯一；`expose rm` 用它定位条目 |
| `--remote-port` | `0` = 自动 | 请求一个具体公网端口；必须落在 broker 的 `frp.port_range` 内，否则 broker 拒绝 |
| `--no-rebuild` | false（=rebuild ON） | 把 expose 钉死在它的 home broker：home 挂了**不**自动迁到存活 voter（默认会自动 rehome）。`cluster drain` 遇到 rebuild-OFF expose 会**拒绝**静默迁移。**cluster only**；单 broker / 旧 broker 静默忽略 |
| `--on-broker` | (空) | 把 expose 的 home 钉到指定 cluster node（cluster_nodes.node_id）。**cluster only**：单 broker 报 `on_broker_single_mode`；目标必须是 eligible（VOTER、有 cert pin）node，否则 `on_broker_unknown`（不写任何行） |
| `--ack-alerts` | false | 分布式集群处于 `quorum_lost` 或 `force_single_active` severe alert 时仍继续 expose / expose rm；只确认本次风险 |
| `--nats-url` | 同 §5.1 全局解析链 | NATS 入口；分布式 HA 可写逗号分隔 seed list |
| `--home` | `~/.tether` | 读取 nkey、`current_session`、默认 broker URL 的目录 |

- `<nid>` 节点上的 `--local` 端口被反向打到 broker 的 `[14000-14999]` 公网带，
  分配的端口 + URL 在输出里：`exposed: http://broker.example:14001 → lab-1:8888`。
- `--local` 必须为 1-65535（ctl 端强制校验，否则 `--local must be 1..65535`）。
- `--remote-port`（可选）指定要分配的**具体公网端口**，省略=自动取最低空闲端口（默认行为，字节级不变）。语义：
  - 该端口必须落在 broker 的公网带 `[14000-14999]` 内，否则报 `port_out_of_band`；
  - 该端口当前已被占用（有 ALLOCATED 记录）则**直接失败** `port_taken`，**不回退**自动分配——你要别的端口请显式改值重试；
  - 已释放（`expose rm`）或被回收（node 长期 OFFLINE）的端口号不算占用，可被重新指定；
  - ctl 端只做 `0`(自动) 或 `1..65535` 的粗校验，**带范围由 broker 裁定**（band 是 broker 配置、各 broker 可不同）；
  - **跨版本注意**：若你的 ctl 比 broker 新而 broker 还没这个特性，旧 broker 会忽略 `--remote-port` 静默改走自动分配（无 proto 区分）；成功输出里打印的就是实际分配到的端口，以它为准。
- `--name` 是会话内的逻辑标识，`expose rm` 用它定位条目；同 session 内必须
  唯一。
- `expose rm` 立即把公网端口归还池子，agent 上对应的 tunnel client 也会
  drop。

分布式 HA 影响：
- `expose` / `expose rm` 都是 destructive 操作。ctl 会先检查 cluster health；
  可达 broker 报 `quorum_lost` 或 `force_single_active` 时默认拒绝，加
  `--ack-alerts` 才继续。
- 分布式模式下，端口分配行会带 `home_broker` 和 `epoch`。agent 不再总是拨自己
  配置里的单一 `--tunnel-addr`，而是按 broker 下发的 home directive 连接该 expose
  的 home broker tunnel 地址，并校验稳定 tunnel cert pin。`home_broker` 是当前承载
  公网 listener 的 broker node id；`epoch` 是该分配的版本号，每次 rehome 递增。
- `cluster retire` 或 broker 故障恢复可能把 expose rehome 到其它 voter；agent
  通过 register reply / rehome directive 按 epoch 迁移 tunnel。rehome 指把同一个
  port allocation 的 home broker 改到另一个 voter，公网端口和
  `--name` 不变。
- stale close / rehome 事件按 `port + sid + nid + token_hash` 栅栏，旧分配的关闭不会
  误关同一公网端口上新分配的 listener；`token_hash` 是 expose token 的哈希，不泄露
  token 本体。如果 NATS close broadcast 丢失，broker 的 tunnel reconcile tick
  （周期性对账）会按 DB 里的 home/epoch 收敛。

- **`--no-rebuild` / `--on-broker` 跨版本注意**：这两个 flag 对旧 broker 静默 no-op
  （旧 broker 忽略未知字段，rebuild 留默认 ON、home 走默认解析；无 proto 区分能告诉
  ctl flag 被丢了）——失败方向都是安全的。`--no-rebuild` 在单 broker 上无意义（没有
  别的 broker 可迁），ctl 会在 stderr 提示 `note: --no-rebuild has no effect on a
  single broker`。

子命令：
- `expose <nid>` / `expose rm <nid>`，**没有** `expose ls` —— 想看当前已分配的暴露
  条目，用 `tether ps` 的 PORTS 节（数据源同为 broker SQLite 的 `port_allocations`）。
  分布式模式下 PORTS 表会多一列 `HOME`（单 broker 不显示，输出字节级不变）；已 rehome
  过的行标 `(moved)`。
- `expose explain <name>` —— 解释**单个** expose：node / state / public port / home
  broker / epoch（"moved" = 已故障切换过）/ rebuild 策略。member 可读（复用 `ps` RPC，
  不加任何新 subject / ACL）；能解释任意状态的条目（含已 REVOKED/FREED）。`last_error`
  / `reconnects` / `ready_reason` / `last_rehome_at` 等事件级可观测字段尚未记录（footer
  注明 planned，B5/DOC#5），不打空占位。`--json` 输出 `schema=expose_explain`。

### 5.15 `tether proxy`（v0.3.0+，代理订阅 / 自建机场）

**这是什么**：把当前 session 的**全部在线 agent** 变成一个 Clash 订阅 —— 打开总开关后,
每个在线 agent(含后加入者)在本机启动内嵌 shadowsocks 服务端并自动 expose, broker 托管
一个自动更新的订阅 URL; 普通用户(无须 tether 身份)把 URL 导入 Clash for Windows 等,
即可像用商用机场一样,**经你的 agent 的网络出口**上网。每个 agent = 订阅里一个可选节点。

```
tether proxy on            # 开总开关(owner-only;需 --yes 或交互确认)
tether proxy off           # 关:所有 agent 停 SS,订阅 URL 立即无节点
tether proxy status        # 看开关/在线节点/订阅列表(member 可读;不含任何密钥)
tether proxy sub create --name alice   # 给某人签发订阅 URL(只打印一次,务必保存)
tether proxy sub ls                    # 列订阅(NAME/STATE/CREATED;绝不显示 token)
tether proxy sub revoke alice          # 撤销一个订阅(其他人不受影响,在飞连接被切断)
```

子命令与参数：

| 命令 | 参数 / flag | 默认 | 说明 |
|---|---|---|---|
| `proxy on` | `--yes` | false | 跳过开放出口节点确认；owner-only，脚本化开启时必须显式承担风险 |
| `proxy off` | — | — | 关闭 session proxy 总开关，停止所有 agent 的 SS server 并切断 tunnel |
| `proxy status` | `--json` | false | 输出原始 JSON；默认输出人类可读摘要 |
| `proxy sub create` | `--name` | (必填) | 订阅持有人标签，1..64 printable ASCII，不允许 `/`；URL 只打印一次 |
| `proxy sub ls` / `proxy sub list` | — | — | 列订阅，不显示 token / PSK |
| `proxy sub revoke` | `<name>` 或 `--name` | (必填) | 撤销一个订阅；其它订阅不受影响，当前连接会被切断 |
| 全部 `proxy` 子命令 | `--nats-url` | 同 §5.1 全局解析链 | NATS 入口；分布式 HA 可写逗号分隔 seed list |
| 全部 `proxy` 子命令 | `--home` | `~/.tether` | 读取 nkey、`current_session`、默认 broker URL 的目录 |

**底层与安全**：
- 协议 = 经典 `chacha20-ietf-poly1305` shadowsocks(AEAD 加密+认证;经典 Clash for Windows 即可),
  每个订阅一把独立 PSK,所有 agent 用单端口**试解密**承载多订阅,撤销互不影响(撤销后该 PSK 的重放 salt 也立即失效)。
- 数据面是明文公网 TCP 口(与 `expose` 的公网带同性质,见 §11 安全注意第 6 条),SS 的 AEAD 是唯一的门 —— **没有订阅密钥连不进来**。
- **责任警告**:开启后每个在线 agent 是对任意 link 持有者(含非成员)开放的互联网出口,
  流量从你 agent 的 IP 出去,你负责。一键关闭=`tether proxy off`。`tether proxy on` 因此强制确认。
- **出口范围默认仅公网**:agent 默认拒绝订阅流量去 loopback / 内网(RFC1918+RFC6598 共享段)/ link-local / multicast / 云 metadata(`169.254.169.254`、`100.100.100.200` 等)等**非公网地址**,按**解析后 IP** 判定(挡 DNS-rebinding,连 NAT64/6to4 编码的内网 IPv4 也挡)。即订阅持有者**只能经 agent 出公网,碰不到 agent 本机/内网**。确需私网访问,在该 agent 的 `agent.yaml` 设 `proxy.allow_private_destinations: true` 显式开启(**高危**:等于把该 agent 所在内网借给订阅持有者)。
- 这是应用层代理,不是整机 VPN;想整机路由,在你自己设备上用 Clash 的 TUN 模式(与 agent 无关)。
- broker 侧需在 `broker.yaml` 设 `broker.sub.listen` 启用订阅 HTTP 端点；`tether serve --sub-http-listen`
  的 CLI 默认是空值（禁用），install.sh 生成的新 broker 配置通常写成 `127.0.0.1:8090`。
  install.sh 生成的 Caddyfile 已把 `/sub/*` 反代到它(排在 NATS-WSS catch-all 之前)。**v0.3.0 之前装的 broker 升级后,这两处配置不会自动出现**——见 broker-ops.md §8.5 手动迁移。

分布式 HA 影响：当前 cluster HA 版本不支持 proxy 数据面和订阅管理。cluster 模式下
`proxy on/off`、`proxy sub create/ls/revoke` 会由 broker 返回 `proxy_unsupported`，
agent register 也不会收到 proxy directive；`proxy status` 最多只能读现有 DB 状态，
不表示 cluster 下 proxy 可用。需要 proxy 时使用单 broker 模式；迁移到 cluster 前应先
关闭 proxy 并通知订阅使用者。

### 5.16 `tether push` / `tether pull`（v0.2.0+）

**这是什么**：在 ctl 与远端 agent 之间搬一个文件。无须 `expose` + `scp` /
`rsync`，复用 NATS 控制平面 + JetStream Object Store 做传输；单次最大 2 GiB
（再大请走 `tether expose` + rsync）。

**两档机制**（`file-transfer-plan` v0.2.0）：

| 大小 | 档位 | 通道 | 是否需 JetStream |
|---|---|---|---|
| ≤ 8 MiB（且 ≤ broker `max_payload`/2） | A | 直接塞进 NATS 消息体 | 否 |
| 8 MiB – 2 GiB | B | broker 创建 ObjectStore bucket，sender Put → receiver Get | **必须** |
| > 2 GiB | — | 拒绝（`too_large`）；改走 `tether expose` + rsync | — |

ctl 在请求前会先打一次 `caps.req` 探测 broker 能力 + `nc.MaxPayload()`，
据此选档；agent 也会复算一次（防止操作员把 broker 的 `max_payload` 调
小了 ctl 不知道）。

**tier A 实际上限**为 `min(8 MiB, ctl_conn.max_payload/2 - 1 KiB, broker_caps.max_payload/2 - 1 KiB)`
（源码 `cliTierAMaxBytes = 8*1024*1024`，再与**每一个已知**测量取小）。G67 / #67：
**探测失败绝不会放宽这个上限**——早先的实现只在 `caps.MaxPayload > 0` 时才收紧，于是一次失败的
caps 探测（零值 `CapsResp`）会把上限悄悄抬回 8 MiB 默认值、无声挪动 tier-A/B 分界；现在缺失的测量
一律忽略，`nc.MaxPayload()` 这一项在任何已连接的连接上都可得。
NATS 默认 `max_payload`（1 MiB）下 tier A 实际只能塞 ~500 KiB；要让 tier A 跑满
8 MiB，broker 的 NATS `max_payload` 必须 ≥ 16 MiB（broker.yaml 之外的 nats.conf
配置）。超过此阈值的请求由 ctl 自动升 tier B。

**先决条件**：

1. broker 必须已开 JetStream（用 tarball install 写出的 nats.conf 缺省即开）；
   否则 tier-B 会以 `jetstream_unavailable` 拒掉（**永久态**）。注意与 `jetstream_not_ready`
   区分：后者是**瞬时**的（broker 重启 / 选主 / grow 进行中），稍后重试即可；
   caps 探测本身失败时 ctl **不再**自行断言 broker 没有 JetStream，而是照常发起请求、
   由 broker 裁决（探测失败会在 stderr 打一条 warning）；
2. push/pull **默认开放**（无需配置）：不配 `file_transfer.allow_roots` 时，
   远端路径可达 agent 用户能访问的**任意绝对路径**（与 `run`/`exec` 同等触达）。
   `allow_roots` 是**可选收紧**；显式 `allow_roots: []`（空列表）则**禁用**
   push/pull（`transfer_disabled`）。`agent.yaml` 使用严格字段校验，拼错
   `file_transfer` / `allow_roots` 会直接拒绝启动，不会静默回落到开放模式。
   详见下方「安全模型」与「升级说明」。

**命令**：

```
tether push <local-path> <nid>:<remote-path> [--force] [--timeout 10m] [--ack-alerts]
tether pull <nid>:<remote-path> <local-path> [--force] [--timeout 10m] [--ack-alerts]
```

参数与语义：

| 参数 / flag | 默认 | 说明 |
|---|---|---|
| `<local-path>` | (必填) | ctl 所在机器上的本地文件路径；push 源必须是普通文件，pull 目标可相对/绝对 |
| `<nid>:<remote-path>` | (必填) | 目标节点 + 该节点上的**绝对**路径；缺省可达任意路径，配了 `allow_roots` 才需落在其前缀内 |
| `--force` | false | 目标已存在时覆盖；缺省直接拒（`dst_exists` 或本地 path exists） |
| `--timeout` | `10m` | 整次传输上限；tier A 一般 < 30s，tier B 取决于带宽 |
| `--ack-alerts` | false | 分布式集群处于 `quorum_lost` 或 `force_single_active` severe alert 时仍继续传输；只确认本次风险 |
| `--nats-url` | 同 §5.1 全局解析链 | NATS 入口；分布式 HA 可写逗号分隔 seed list |
| `--home` | `~/.tether` | 读取 nkey、`current_session`、默认 broker URL 的目录 |

分布式 HA 影响：push/pull 会创建 transfer tracker、ObjectStore bucket 和审计事件，
因此在 severe alert 下默认被 `--ack-alerts` 门控。cluster 模式下所有 broker 都可能
收到 start request，但只有目标 `<nid>` 的 home broker 会真正创建 tracker / bucket /
转发给 agent；非 home broker 静默让路，避免同一文件传输被 N 个 broker 重复处理。
tier-B ObjectStore bucket 按 cluster replica 策略创建；传输开始后，即使目标 node
后续 rehome，commit/finalize 仍由最初持有 tracker 的 broker 完成。

**安全模型**（plan §"Refusing dangerous paths"）：

- 远端路径**必须绝对**。缺省（未配 `allow_roots`）可达任意路径；配置后须落在
  某前缀下（否则 `path_outside_roots`）。`allow_roots` 是**便利性收紧而非安全
  边界**——member 本就有不受限的 `run`/`exec`（requirements §9.3），随时能绕过；
  但叶子软链 / TOCTOU / 普通文件等**机制级加固在所有模式下都生效**（中间目录
  软链先解析到真实目录；随后父目录逐级 `openat(O_NOFOLLOW)` 固定，落地叶子软链
  一律拒绝），防的是 agent 机器上的敌对**非 member** 进程；
- 父目录不存在 → `path_parent_missing`（push）或 `path_not_found`（pull）；
  v0.2.0 不自动 `mkdir -p`，需手动 `tether exec <nid> -- mkdir -p ...`；
- pull 还会做 dev+inode TOCTOU 校验（`lstat` 与 `open` 之间的换底攻击）；
- broker 是 ObjectStore bucket 的**唯一所有者**：member/agent JWT 都没有
  `STREAM.CREATE / DELETE / PURGE` 权限，bucket 由 broker 在 prepare 阶段
  创建、在 `ev.transfer` / `finalize.req` 收到时删除；超时（tier A 30s /
  tier B 5min）broker watchdog 也会兜底删除并写 audit failed。

**典型用例**：

```bash
# 把本地脚本送到远端 GPU
tether push ./train.py a100:/srv/local/alice/jobs/train.py

# 把训练日志拉回本地
tether pull a100:/srv/local/alice/jobs/run-2026-05-12.log ./run-2026-05-12.log

# 大文件（tier B）：12 MiB random
tether push ./checkpoint.bin a100:/srv/local/alice/ckpt.bin --force
# tether push: ... -> a100:... (tier=b, 12582912 bytes, transfer_id=...)
# tether push: OK (tier B, 12582912 bytes, 62ms)
```

**audit**：每次传输都会有 `start` 和 `complete|failed` 两条
`audit.transfer` 行；用 `tether history --kind transfer` 查看。"接收方-终结
不变量"（plan §Audit）：push 由 agent 触发终结，pull 由 ctl 触发终结，
broker 永远是 single audit writer。

**常见错误码**：

| code | 含义 |
|---|---|
| `transfer_disabled` | 目标 agent 配了显式 `allow_roots: []`（关闭 push/pull） |
| `path_outside_roots` | 配了 `allow_roots` 收紧，且远端路径不在任一前缀下 |
| `path_parent_missing` / `path_not_found` | 父目录不存在 / 源文件不存在 |
| `not_a_regular_file` | 叶子是软链 / 设备 / 目录 |
| `dst_exists` | 目标已存在；加 `--force` 覆盖 |
| `sha_mismatch` / `size_mismatch` | 接收端在提交目标文件前发现 SHA-256 / 字节数不符 |
| `path_race` | 校验后父目录或文件 inode 被并发替换，操作已拒绝 |
| `too_large` | 文件 > 2 GiB |
| `jetstream_unavailable` | **永久配置态**：该 broker 根本没有 JetStream 客户端（nats.conf 里禁用了，或 broker 启动时那次一次性 JetStream 探测失败——broker 不会重探，重启即重探）。重试无用 |
| `jetstream_not_ready` | **瞬时态**（G67 / #67）：JetStream 在，但此刻不受理该请求——broker 重启、选主、或 grow 正在进行。broker **已经**有界重试过若干次才回这个码；**稍后重跑同一条命令**即可，退出码 75。持续不消失则 `tether cluster status` |
| `bucket_create_failed` | **永久态**：tier-B 对象存储无法建立且重试无用（JS store 小于 tier-B 下限、非集群节点上 replicas>1 等）。文案刻意不含任何"重试"字样 |
| `node_offline` / `not_a_member` | 目标节点离线 / 调用者不是 session 成员 |
| `not_owner_or_creator` | finalize 阶段 actor 跟 transfer 创建者不一致（防伪造） |

> **升级说明（v0.4.0 行为变更）**：v0.3.x 及更早版本里"未配 `allow_roots`"
> 等价于 push/pull **禁用**（含 `install.sh` 写出的默认 `agent.yaml`——它根本
> 不含 `file_transfer` 块）。自本版起，**未配 = 全盘开放**（与 `run`/`exec` 同等
> 触达）。
>
> | `agent.yaml` 配置 | 旧行为 | 新行为 |
> |---|---|---|
> | 无 `file_transfer` / `allow_roots` 键 | 禁用 | **开放（全盘）** |
> | `allow_roots: [/a, /b]`（非空） | 收紧到这些前缀 | 不变 |
> | `allow_roots: []`（显式空列表） | 禁用 | 不变（仍禁用） |
>
> 想在升级后保持关闭：在 `agent.yaml` 的 `file_transfer:` 下加一行 `allow_roots: []`
> 并重启 agent。同一 `ProtoVersion` 内换二进制重启即可；跨 `ProtoVersion`
> 升级必须走 §8.3 全量重装。开放模式下 agent 启动会打一条 WARN 提示当前为全盘触达。

### 5.17 `tether history`

**这是什么**：当前活动 session 的"审计回放器"。每条**命令调用**（exec /
run / expose / kill / upgrade...）、每个**进程生命周期**（started /
exited）、每次**端口分配**（expose / release）都会被 broker 写进 JetStream
`history-<sid>` stream，`tether history` 把它们打印出来供事后审计 / 排
查 / 合规导出。

**与 `tether ps` 的区别**：
- `ps` 是**实时态**（"现在有什么在跑"），数据从 SQLite 当前行；
- `history` 是**事件流**（"过去发生了什么"），数据从 JetStream append-only
  流，包含已退出的进程 / 已释放的端口 / 失败的调用。

**典型用法**：
- `tether history -n 100` 看最近 100 条；
- `tether history --follow` 实时监控（替代 `journalctl -f` 在远端的需要）；
- `tether history --kind call | jq 'select(.ok==false)'` 找失败的调用；
- `tether history --kind proc | jq 'select(.rc != 0)'` 找异常退出。

```
tether history                  # 从最早回放至空闲 250ms 后退出
tether history -n 50            # 最近 50 条
tether history --kind call      # 仅 call / proc / port / transfer
tether history --follow         # 持续 tail（Ctrl-C 退出）
```

flag：

| flag | 默认 | 说明 |
|---|---|---|
| `--lines, -n` | `0` | 只显示最近 N 条；`0` 表示从最早开始；与 `--follow` 同用时被忽略 |
| `--follow, -f` | false | 打完快照后继续 tail 新 audit message，Ctrl-C 退出 |
| `--kind` | (空 = all) | 过滤 audit kind：`call` / `proc` / `port` / `transfer` |
| `--nats-url` | 同 §5.1 全局解析链 | NATS 入口；分布式 HA 可写逗号分隔 seed list |
| `--home` | `~/.tether` | 读取 nkey、`current_session`、默认 broker URL 的目录 |

底层是 JetStream `history-<sid>` stream 的 ephemeral consumer（架构 H.2）。`-n N`
带 `--kind` 时走 ring-buffer 全量过滤（filter 后 seq 稀疏，不能用 `LastSeq-N+1`
快捷路径）。

输出格式：

```
15:04:05  CALL  lab/lab-1  fp=SHA256:abcdefg…  verb=exec  ok=true
15:04:06  PROC  lab/lab-1  pid=proc-01H…       kind=started  rc=<nil>  cmd=sleep 60
15:04:07  PORT  lab/lab-1  port=14001          name=jupyter  kind=expose
```

需要原始 JSON 走 `--kind call | jq .` 之类管线。

分布式 HA 影响：`history` 是 read-only，不需要 `--ack-alerts`。ctl 可连接任意
可达 broker seed；JetStream stream 名仍是 `history-<sid>`，由 broker 侧保证事件写入
和复制策略。`--follow` 期间如果当前连接 broker 掉线，NATS client 会按 seed list
重连，短窗口内可能看到 reconnect 延迟。

### 5.18 `tether node ls`

**这是什么**：列出当前活动 session 内**所有注册的 agent**（不仅是有进程在跑的），
显示在线状态、心跳年龄、proto 版本、release 版本。

**与 `tether ps` 的区别**：
- `ps` 是**进程视图**，节点只在跑过命令后才出现；
- `node ls` 是**节点视图**，注册了就出现，从未 exec 过也能看到。

**与 `tether admin nodes` 的区别**：
- `admin nodes` 跨 session、走 broker 本机 Unix socket、无 NATS 鉴权；
- `node ls` 仅当前 session、走 NATS auth_callout，任何 member 可用。

**何时用**：
- 刚装好 agent，想确认它连上 broker（还没跑过任何命令）；
- 找 OFFLINE / STALE 节点，决定是否 `tether admin evict`；
- 升级前后核对 `RELEASE` 列。

```
tether node ls          # 仅 ONLINE 节点
tether node ls -a       # 含 OFFLINE / STALE（同 --all）
```

flag：

| flag | 默认 | 说明 |
|---|---|---|
| `--all, -a` | false | 包含 OFFLINE / STALE 节点；默认只看 ONLINE |
| `--nats-url` | 同 §5.1 全局解析链 | NATS 入口；分布式 HA 可写逗号分隔 seed list |
| `--home` | `~/.tether` | 读取 nkey、`current_session`、默认 broker URL 的目录 |

输出列：

| 列 | 含义 |
|---|---|
| `NODE`      | nid（节点 id） |
| `STATUS`    | `ONLINE` / `OFFLINE` / `STALE`（依 broker 端心跳分类） |
| `HEARTBEAT` | 最后心跳年龄（如 `<1s` / `2s` / `5m`），OFFLINE 时为最后一次见的年龄 |
| `PROTO`     | wire 协议版本号；与 broker 不一致 ⇒ 必须重装而非 upgrade |
| `RELEASE`   | release 版本字符串（如 `0.2.7`） |

底层 RPC 是 `node.list.req`，与 `node upgrade --all` 共享同一枚举机制。

分布式 HA 影响：`node ls` 是 read-only，不需要 `--ack-alerts`；有 severe
cluster alert 时会把 banner 打到 stderr。cluster 模式下 broker 会记录 agent
当前连上的 NATS `server_name`，用于后续 expose home 选择和 transfer home gate。

### 5.19 `tether node upgrade`

**这是什么**：owner-only 的"远程升级 agent 二进制"命令。无需登录目标
机，无需停服务，PID 不变，systemd 不感知 —— agent 内部用 `syscall.Exec`
原地替换自己的镜像。

**为什么单独存在**：tether 的常见部署是 N 台 NAT 后的实验机，逐台
SSH 上去 `wget && systemctl restart` 既慢又容易漏。这个命令把"分发新二
进制 + 校验 + 替换 + 重启 + 状态恢复"打包成一次远程调用。

**安全模型**：
1. **broker 端白名单前缀强校验**：URL 必须前缀匹配
   `broker.upgrade.url_allow`（默认仅放本仓库的 GitHub release 前缀，自托管
   镜像需 yaml 覆盖）；任何前缀外的 URL 都拒；
2. **owner-only**：必须是 session owner，member 没权限；
3. **白名单 + SHA256**：URL 必须前缀匹配 broker 配置的白名单，tarball
   的 SHA256 必须由调用者提供并校验通过；
4. **文件解压白名单**：tarball 里只允许 `tether` 这一个 entry，路径不
   允许包含 `..`、symlink；
5. **size cap**：下载封顶 64 MiB（`io.LimitReader`），防 DoS；
6. **proto 必须一致**：跨 proto 升级会被拒（必须重装），因为 wire 协议
   不兼容时升级中途会出现"新 agent 跟旧 broker 谈不拢"的窗口。

**何时用**：发布新 release 后批量推到全部实验机；不能用于跨大版本
（proto bump）升级。

**何时别用**：跨 proto / 全新部署 / agent 不在线 → 用 `install.sh
--role agent` 现场重装。

```
tether node upgrade <nid> --url https://... --sha256 <hex64>      # 单台
tether node upgrade --all  --url https://... --sha256 <hex64>     # session 内全量 ONLINE
```

flag：

| flag | 默认 | 说明 |
|---|---|---|
| `--url`     | (必填) | 绝对 `https://` 的 release tarball URL；必须前缀匹配 `broker.upgrade.url_allow` |
| `--sha256`  | (必填) | tarball 的 SHA256 hex（64 位小写） |
| `--all`     | false  | 升级全部 ONLINE 节点（与 positional `<nid>` 互斥） |
| `--timeout` | `60`（秒） | 单台 upgrade 的总超时；超大 tarball / 慢链路要显式调大 |
| `--nats-url`| 同 §5.1 全局解析链 | NATS 入口；分布式 HA 可写逗号分隔 seed list |
| `--home`    | `~/.tether` | 读取 nkey、`current_session`、默认 broker URL 的目录 |

- broker 校验：owner、URL 在 `broker.upgrade.url_allow`、proto 一致、SHA256 64 位
  小写 hex；任一不过 ⇒ 拒绝（`url_not_allowed` / `not_owner` / `proto_bump_*` /
  `sha256_invalid`）。
- agent 收到后：本地再校验 URL 白名单 → 流式下载（封顶 64 MiB，`io.LimitReader`）
  → SHA256 校验 → 解 tarball（白名单只允许 `tether` 这一个文件，
  `O_EXCL|O_NOFOLLOW`）→ 原子替换自身 → `syscall.Exec` 原地重启（PID 不变）。
- 新二进制重连后跑 G.1 reconcile（保留进程清单 / 端口分配，OFFLINE → ONLINE）。

`--all` 的失败分类策略（区分瞬时 vs 配置错）：

| 类型 | code | --all 行为 |
|---|---|---|
| 瞬时 | `node_offline` / `node_not_found` / `agent_no_responders` / `agent_malformed_resp` / `deadline exceeded` / `context canceled` | log + skip，结尾汇总 |
| 配置 | `not_owner` / `url_not_allowed` / `sha256_invalid` / `proto_bump_requires_reinstall` / `actor_invalid` / `session_not_found_or_deleting` | 立即终止（fail-fast） |

单台模式（不带 `--all`）任何错都直接终止。

`--timeout` 默认 60s，必须大于 agent 下载 + 解压时间，超大 tarball / 慢链路记得
显式调大。

分布式 HA 影响：`node upgrade` 是 owner-only 写操作，但当前 CLI 没有
`--ack-alerts` 覆盖；它连接任意可达 broker seed 后由 broker 做 owner、URL allowlist、
proto 一致性和目标 ONLINE 校验。批量 `--all` 的节点枚举来自 cluster 复制后的
session 视图；某个 broker 正在 drain / quorum 不足时，实际结果以 broker 返回的
错误码和超时分类为准。

---

## 6. 日常使用场景

### 6.1 第一次接入实验

**实验机管理员**：
```bash
# 1. 安装 agent（broker 已经在跑，已经创建了 lab session 并给了 PIN）
curl -fsSL https://github.com/LinZiyang666/dist_experiment_control/releases/latest/download/install.sh \
  | sh -s -- \
    --role agent --broker wss://broker.example.com:443 \
    --session lab --pin 384712 --nid gpu-01

# 2. systemd --user 自动启动
~/.local/bin/tether agent --install-user-service --session lab --nid gpu-01
systemctl --user daemon-reload
systemctl --user enable --now tether-agent@lab.service
loginctl enable-linger $USER

# 3. 验证
journalctl --user -u tether-agent@lab.service --since '1 minute ago' -f
```

**使用者**：
```bash
# 1. 安装 ctl
curl -fsSL https://github.com/LinZiyang666/dist_experiment_control/releases/latest/download/install.sh | sh

# 2. 登录并激活 session（同样的 PIN）
tether login --broker wss://broker.example.com:443 --session lab --pin 384712
export TETHER_SESSION=lab

# 3. 列节点
tether ps
```

### 6.2 跑一条远程命令

```bash
tether exec gpu-01 -- nvidia-smi
tether exec gpu-01 -- ls -la /data       # SetInterspersed(false) 让 -la 透传
tether exec --cwd /work gpu-01 -- python train.py --epochs 10
```

### 6.3 SSH 风格交互式 shell

```bash
tether run gpu-01 -- bash -l
# Ctrl-C / 上下键 / vim / htop 都正常
exit                                     # 远端进程退 ⇒ 本地退
```

### 6.4 暴露 Jupyter

```bash
# agent 内启动 jupyter（监听 :8888）
tether exec gpu-01 -- bash -c 'cd /work && nohup jupyter lab --no-browser --port 8888 >jupyter.log 2>&1 &'

# 暴露
tether expose gpu-01 --local 8888 --name jupyter
# 输出：exposed: http://broker.example.com:14001 → gpu-01:8888

# 本地浏览器打开 http://broker.example.com:14001
# 实验完
tether expose rm gpu-01 --name jupyter
```

agent 重启 / 网络抖动后会自动用 `state.json` 里的 token 重连，URL 不变。

### 6.5 多人共享 session

```bash
# owner 把 PIN 告诉成员（不要走 git / 公开渠道）
# 成员
tether login --broker wss://broker.example.com:443 --session lab --pin 384712

# 之后 ls / exec / expose / history 全员可见
# 但只有 owner 能 session rm / node upgrade
```

### 6.6 查看历史

```bash
tether history -n 100                    # 最近 100 条全类型
tether history -n 50 --kind call         # 仅命令调用
tether history --follow                  # 直播
tether history --kind proc | jq 'select(.rc != 0)'  # 失败进程
tether history --kind transfer -n 20     # 仅 push/pull
```

### 6.7 上传 / 下载文件（push / pull）

```bash
# 把本地脚本送到 a100 的某绝对路径（缺省可达任意路径，配了 allow_roots 才收紧）
tether push ./train.py a100:/srv/local/alice/jobs/train.py

# 拉日志回来
tether pull a100:/srv/local/alice/jobs/run.log ./run.log

# 大文件用 tier B（broker 自动选档；> 8 MiB 走 ObjectStore）
tether push ./checkpoint.bin a100:/srv/local/alice/ckpt.bin --force

# 文件 > 2 GiB：先 expose、再 rsync
tether expose a100 --local 22 --name ssh-tunnel
rsync -av --progress big-dataset/ rsync@broker:14001/...
```

排查：
- `tether push` 报 `transfer_disabled` → agent.yaml 里 `file_transfer.allow_roots: []`（显式关闭）；删掉该键即恢复开放；
- `path_outside_roots` → 配了 `allow_roots` 收紧，远端绝对路径不在任一前缀下；
- `dst_exists` → 远端已有同名文件，加 `--force` 覆盖；
- `jetstream_unavailable` → 该 broker 没有 JetStream 客户端（禁用了，或启动时的一次性探测失败；重启 broker 会重探）——**永久态**，重试无用；
- `jetstream_not_ready` → **瞬时态**：JetStream 在但此刻不受理（broker 重启 / 选主 / grow 进行中），broker 已有界重试过；**稍后重跑同一条命令**，持续则 `tether cluster status`；
- 详细 audit：`tether history --kind transfer -n 20`。

---

## 7. 运维 / 维护

> broker 侧运维（§7.1 健康检查 / §7.2 disk pressure / §7.3 admin socket / §7.4 备份 /
> §7.5 日志轮转 / §7.6 监控 tether 自身）见 [`broker-ops.md`](broker-ops.md) §7；本册只保留
> 使用者会直接遇到的 §7.7（网络盘挂死自救）。

### 7.7 网络文件系统（NFS/CIFS/…）挂死时 run/exec 卡住（v0.3.3+）

**症状**：某台 agent 的 home / `$PATH` / 数据盘在 NFS（或 CIFS/sshfs）上，NFS 服务端
挂掉后，`tether exec <node> ...` / `tether run <node>` **永久卡死出不来**，但 `node ls`
/ `ps` / `expose` 等其余命令照常——`ssh <node>` 通常也一起卡。

**为什么**：`hard` 挂载的 NFS 一旦服务端无响应，任何访问该路径的系统调用会进入
**不可中断 D 状态**（连 `kill -9` 都打不动）。agent 启动子进程时，`os/exec` 会先按
`$PATH` 逐个 `stat` 找可执行文件；只要 `$PATH` 里排在前面的目录在死掉的 NFS 上（常见：
conda 在 `/shared`、`~/.local/bin` 在 NFS home），**在 fork 之前就 D 住**，连 `whoami`
都跑不出来。agent 守护进程本身不受影响（它热路径不碰盘），所以只有 run/exec 卡。

**v0.3.3 起的自救（默认 `remote_fs.mode: auto`，无网络挂载的机器完全惰性、行为同旧）**：
agent 检测到某网络挂载无响应时，会**把该挂载的目录从 `$PATH` 剔除**、自己把 argv[0]
解析成绝对路径再启动（绕过会卡的 `LookPath`），并对 fork/exec 设启动窗口超时。于是：

- **不依赖那块盘的命令照常可用**——停机排障时你仍能 `nvidia-smi`、`kill`、看本地盘、
  跑本地 conda（若 conda 在本地盘）。
- **依赖死盘的命令快速失败带提示**而非永久卡：
  - `remote_fs_unhealthy` —— argv[0] 在死挂载上（换本地盘的二进制，或 `--cwd` 本地目录）;
  - `remote_fs_unsafe_cwd` —— `--cwd` 指向死挂载;
  - `remote_fs_spawn_timeout` —— fork/exec 卡死被放弃（二进制/数据可能真在死 NFS 上）。

**手动强制**（即便 agent 设了 `mode: off`）：`tether exec --safe <node> -- ...`、
`tether run --safe <node> -- ...`。注意 `--safe` 必须放在 node 前面（`exec`/`run`
把 node 之后的 flag 当远程命令的参数）。

**做不到的事（诚实说明）**：如果命令的**二进制或数据本身就在挂死的 NFS 上**，没有任何
办法能跑它（内核 D 状态、不可杀）——safe 模式只能保住"不碰那块盘"的命令，并让其余
快速失败。根治仍是：**把 agent 装在本地盘**（`HOME=/srv/local/<user>/...`，见 §2 安装），
这样网络盘挂了 agent 与本地命令都不受影响。

**降级注意**：`agent.yaml` 里写了 `remote_fs:` 块后，若要把 agent **降级回 v0.3.3 之前**的
二进制，必须先删掉该块——老二进制用 `KnownFields` 严格解析，遇到未知顶层键会拒绝启动
（与 `file_transfer`/`proxy` 等历史 additive 块同理）。

**已知边界（明确契约，非 bug）**：
- **启动后新挂载**：`auto` 模式只在**启动时**判定本机有无网络挂载;启动时全本地的 agent,
  生命周期内**不会**检测启动后新挂的网络盘(为保证本地机零开销)。需要让这种 agent 也防护,
  用显式 `tether exec/run --safe`(每次都重新探测挂载),或重启 agent。
- **autofs**:未触发的 autofs 挂载**不**当作死挂载、也不主动探测(否则会破坏健康机的首次
  自动挂载),但会启用有界 argv0 解析和启动窗口看门狗。若 automount 目标真死了,返回
  `remote_fs_spawn_timeout`,不是永久卡。
- **网络盘上的 `~/.tether`(state.json)**:Component I 只把**重连时的读**做了有界降级,保住
  agent 在线 + run/exec 防护;但 `expose`/`proxy` 的**写**(改 state.json)在网络 Home 挂死时
  仍会阻塞该次操作。**网络 Home 是 best-effort**,强烈建议把 agent 装在本地盘(`HOME=/srv/local/...`)。

---

## 8. 升级

### 8.1 三种角色的升级路径

| 角色 | 路径 | PID 变化 | 停机 |
|---|---|---|---|
| ctl    | `install.sh` 重跑 / 手动 wget+chmod | — | — |
| agent  | `tether node upgrade <nid>`（owner 触发，远程） | **不变**（syscall.Exec 原地） | 几百 ms |
| broker | 手动：`systemctl stop tether-broker && cp tether /usr/local/bin && systemctl start tether-broker` | 变 | 1-2 s |

### 8.2 owner 远程升级 agent

```bash
# 1. 从 GitHub Release 拿 tarball URL + SHA256SUMS
VER=v0.1.1
BASE=https://github.com/LinZiyang666/dist_experiment_control/releases/download/$VER
URL=$BASE/tether_${VER}_linux_amd64.tar.gz
SHA=$(curl -fsSL $BASE/SHA256SUMS | awk "/tether_${VER}_linux_amd64.tar.gz\$/ {print \$1}")

# 2. 单台试点
tether node upgrade gpu-01 --url $URL --sha256 $SHA

# 3. 灰度 OK 后全量
tether node upgrade --all --url $URL --sha256 $SHA
```

agent 内部步骤：
1. broker 校验 owner / URL allowlist / proto / sha256 格式；
2. agent 收到 → 本地再校 URL allowlist → 流式下载（≤64 MiB）→ sha256 校验 →
   解 tar.gz（仅允许 `tether` 文件） → `O_EXCL|O_NOFOLLOW` 写到 tmp →
   原子 rename 到自身路径 → `syscall.Exec(self)`；
3. 新进程重连 broker → G.1 reconcile（进程 / 端口 状态保留）。

如果失败：
- broker 拒绝 ⇒ CLI 输出带 hint 的错误码（见 §9）；
- agent 拒绝 ⇒ broker 转传 `agent_rejected:<sub-code>`，CLI 同样翻译；
- 升级中 agent 崩溃 ⇒ 旧二进制还在原地（atomic rename 保证），systemd 拉起即恢复。

### 8.3 proto 升级

`internal/proto/version.go` 里的 `ProtoVersion` 是 wire 协议版本。bump 后
新 broker 不接受旧 agent（`proto_bump_requires_reinstall`）：必须先在每台 agent
上重跑 `install.sh --role agent` 或人工 `cp` 同版本 tether 二进制，再启 broker。
**`tether node upgrade` 不能跨 proto** —— upgrade 设计成只在同 proto 内的 patch
升级里安全。

## 9. 错误码与故障排查

CLI 输出错误统一格式：`<verb> failed: <人话提示> (<架构稳定的 code>)`。本节列
出**使用者侧**已知 code 及处置；broker 侧的 NATS 连接 / admin 码（§9.7 / §9.10）见 [`broker-ops.md`](broker-ops.md)，集群瞬态码（§9.7.1）见 [`cluster.md`](cluster.md)。

### 9.1 成员 / 所有权 / 生命周期

| code | 含义 | 处置 |
|---|---|---|
| `not_owner`                     | 仅 owner 可执行 | 让 owner 来做；当前不支持所有权转交，需 owner 重建 session |
| `not_owner_or_creator`          | 仅 owner 或资源创建者可执行 | 同上 |
| `not_a_member`                  | 你没加入此 session | 让 owner 给 PIN，`tether login -s <sid> --pin <pin>` |
| `session_not_found_or_deleting` | session 不存在或正在删除 | `tether session ls` 确认；DELETING 中无救（等回收） |
| `session_not_found`             | session 不存在 | 重建：`tether session create <name> --pin <pin>` |
| `actor_invalid`                 | 你的身份不合法 | 极少见；`rm -rf ~/.tether/keys/`（会丢全部成员资格）后重新 join |

### 9.2 节点生命周期

| code | 含义 | 处置 |
|---|---|---|
| `node_not_found`        | 该 session 下无此 nid | `tether ps` 确认；可能 nid 拼错 |
| `node_offline`          | agent 心跳过期（超过 OfflineAfter） | 在 agent 机器：`systemctl --user start tether-agent@lab` 或检查 `agent.log` |
| `agent_no_responders`   | agent 在 NATS 上无响应 | agent 进程死或 NATS 断；同上排查 |
| `agent_malformed_resp`  | agent 回包格式错（可能是 proto 不一致或 agent bug） | 先查 `tether version` / broker-agent proto；proto 不一致走 §8.3 全量重装，同 proto patch 才用 `tether node upgrade <nid>` |
| `proto_mismatch`        | broker / agent proto 版本不一致 | 全量重装 agent（不能用 upgrade） |

### 9.3 升级类

| code | 含义 | 处置 |
|---|---|---|
| `url_not_allowed`               | broker 没把 URL 前缀加白名单 | 让 broker 操作员在 `broker.yaml` `broker.upgrade.url_allow` 加上 |
| `url_not_allowed_local`         | agent 本地 allowlist 拒绝 | 检查 agent 的 `--upgrade-url-allow`（默认与 broker 同步，覆盖时小心） |
| `sha256_invalid`                | sha256 不是 64 位小写 hex | 用 `sha256sum` 重新算 |
| `sha256_mismatch`               | 下载的 tarball 与 sha256 不符 | 重传 release / 重算 sha256 |
| `proto_bump_requires_reinstall` | 跨 proto 升级 | 必须 `install.sh --role agent` 重装，不能 upgrade |

### 9.4 expose 类

| code | 含义 | 处置 |
|---|---|---|
| `name_taken`        | session 内已有同 name | 换 name 或 `tether expose rm <nid> --name <X>` 先释放 |
| `port_exhausted`    | broker 14000-14999 全占满 | `tether admin sessions` 找闲置 expose；`tether expose rm` 释放；或扩 `frp.port_range` |
| `local_port_invalid`| `--local` 不在 1..65535 | 检查 flag 值 |
| `port_taken`        | `--remote-port` 指定的公网端口已被占用（有 ALLOCATED 记录） | 换端口、省略 `--remote-port` 自动选、或 `tether expose rm` 先释放 |
| `port_out_of_band`  | `--remote-port` 不在 broker 公网带 `[14000-14999]` 内 | 选带内端口或省略该 flag 自动选 |
| `frpc_failed`       | agent 起 tunnel 客户端失败 | 看 `~/.tether/agent/<sid>/agent.log` |
| `tunnel_token_unknown_or_revoked` | 反向隧道 token 失效 | 重新 `tether expose`（agent 重启后 state.json 损坏可能触发） |

> 注：`home_catching_up` / `try_again` 是 **agent 反向隧道 REGISTER 的 DENY reason**（broker 故障切换/瞬时
> 存储抖动时），由 **agent 自动重试**，**不会**作为 `tether expose` 的 ctl 回复码出现；它们与
> `leader_unavailable` 一起归在 cluster.md §9.7.1。

### 9.5 PTY (`tether run`) 失败原因

| reason | 含义 | 处置 |
|---|---|---|
| `attach_timeout`   | agent 分配了 PTY 但 ctl 3s 内没订阅 | 重试；持续出现查 NATS 时延 / clock skew |
| `pty_alloc_failed` | agent 端 `/dev/ptmx` 不可用 | 容器内常见，加 `--device=/dev/ptmx` 或开 `--privileged` |
| `exec_failed`      | PTY 分了但 argv 起不来 | 检查 argv（typo / not in PATH / no exec bit） |
| `argv_required`    | 没传 argv | `tether run gpu-01 -- bash` 形式调用 |

### 9.6 存储 / 通用

| code | 含义 | 处置 |
|---|---|---|
| `store_error` | broker SQLite 写失败 | 看 broker 日志；磁盘满 / 权限错 / WAL 损坏 |
| `json_parse`  | broker 解析请求失败 | tether bug，请汇报 |

> §9.7（NATS 连接失败）见 [`broker-ops.md`](broker-ops.md) §9.7；§9.7.1（集群瞬时状态）见 [`cluster.md`](cluster.md) §9.7.1。

### 9.8 历史回放卡住

`tether history -n N` 没有 `--follow` 时，靠 250ms 静默判定快照结束。如果 broker
推送速率持续高于 250ms 间隔，命令永远收不到 EOF。临时方案：用 `--follow | head
-n N`。

### 9.9 agent 显示 OFFLINE 但实际在跑

90% 是 NATS 连接断了。
1. agent 机器：`journalctl --user -u tether-agent@lab.service --since '5 min ago'`
   看是否在重连；
2. 检查机器到 broker 的 NAT 是否 idle timeout（NATS 默认有 ping/pong，但企业
   防火墙偶尔会切）；
3. agent 重连后 broker 自动跑 G.1 reconcile，`tether ps` 应该自愈。

> §9.10（admin 命令 connection refused）见 [`broker-ops.md`](broker-ops.md) §9.10。

### 9.11 `make lint` 抱怨 Go 版本

```
language version 1.23 is lower than the targeted Go version 1.25
```

⇒ 用了 v1.x 的 golangci-lint。`make tools` 装好 v2.5.0 即可。

### 9.12 `make e2e-parallel` 挂起或 OOM

- 矩阵串行跑全 P1-P10，单跑 70-90s。CI 资源紧张时 `test/p5
  TestRunExitCodePropagates` 可能因 attach_timeout 偶发抖动 —— 单跑
  `go test -run TestRunExitCodePropagates ./test/p5/` 应通过；
- 内存峰值 ~500 MiB（嵌入式 nats-server + JetStream），低内存机加 swap。

### 9.13 退出码 taxonomy（脚本 / 监控用）

`tether` 的进程退出码遵循 sysexits 风格，便于脚本区分失败种类（B2）。**`0` 永远是成功，旧的
`$? != 0` 脚本不受影响**；只是具体的非零值现在更有信息量：

| 退出码 | 含义 | 典型场景 |
|---|---|---|
| `0` | 成功 | — |
| `64` | 用法/参数错（EX_USAGE） | 缺必填 flag、互斥 flag、`--local` 越界、`name_taken`/`port_exhausted` 等需人工处置的 broker 码 |
| `69` | 服务不可达（EX_UNAVAILABLE） | broker/NATS 连不上、no-responder、admin socket 不在 |
| `70` | 内部/未分类（EX_SOFTWARE） | malformed reply、解码失败、tether 没能分类的错误（=该上报的 tether 侧缺口） |
| `75` | 瞬时可重试（EX_TEMPFAIL） | `leader_unavailable`/`home_catching_up`/`catch_up_stalled`、deadline、no-responder 的 agent |
| `77` | 权限（EX_NOPERM） | `not_owner`/`not_a_member`、非 leader 上跑 cluster mutate（`not_leader`） |

**保留区间（不来自上表）**：

- `0..3` 仅 `tether cluster status` 的健康码（`0`=HEALTHY-HA、`1`=DEGRADED、`2`=read-only/quorum-lost、`3`=force-single），**只有该命令发**，且 `--remote` 更薄（永不出 1，见 cluster.md §5.6）。注：`cluster status` 只要**产出报告**就退 `0..3`（按健康），broker 侧错误经 `errors[]`/`partial`/stderr 暴露而非改退出码；**只有拿不到报告才退 69**。
- `exec` / `run` **透传远端进程退出码**（任意 `0..255`，含 64/69/70/77/128+），不入分类器；判别靠"你跑的是哪个命令"，不是值。所以"77=权限"仅限 broker-RPC 命令。

**健壮重试规则**：把 `69`/`70`/`75` 当可重试（退避），仅 `64`/`77` 当终态。

> **batch-A A1 订正**：此前有 **62 个** broker/agent 错误码**没有** exit class 归类，因而全部落 `70`——
> 也就是被上面这条规则指示"退避重试"。其中包括物理上不可能靠重试成功的（`too_large` 超 2 GiB 硬上限、
> `self_path`、`verb_mismatch`、`pid_unknown`），以及 **force-single 的全部拒绝码**
> （`peer_alive` = 一个"已确认死亡"的 broker 竟回应了探测、`quorum_not_lost`、`arm_expired`）——
> 后者意味着自动化会反复重试一个**因为对端还活着而被正确拒绝**的 force-single。
>
> 62 个码现已逐个归类（77×2 / 75×10 / 64×42 / 70×8）——其中 8 个经判断确属我方 bug、
> **仍归 70 但现在是显式声明**（"判过"与"没人看过"因此可区分），所以**退出码真正发生变化的是 54 个**。
> 由 `cmd/tether/error_code_coverage_test.go`
> 的守门测试防止再次漏归。**wire 上的码字符串一个字节未改**，只改了进程退出码，
> 现网混版本 broker/agent/ctl 无感。
>
> 仍留在 `70` 的是**经判断确实无法分类**的兜底分支（如 `io_error`：磁盘满与瞬时 EIO 落进同一分支，
> 无从区分）。它们在守门测试里带**书面理由**登记，而不是继续隐式落到 default。

### 9.14 机器 JSON 接口（`--json`）

list/status/结果类命令支持 `--json`（**opt-in，默认人读文本字节不变**）：`node ls` / `ps` /
`session ls` / `alert ls` / `expose` / `expose rm` / `admin sessions·nodes·audit` / `cluster status`。
（`node upgrade --all` 与 `transfer` 的 `--json` 在 B2.1 补。）

- 上述 8 个 list/result 载体各带 **`schema`（串判别符）+ `schema_version`（int）**；监控按
  `(schema, schema_version)` 派发。**例外**：`cluster status` 家族——socket/offline 报告带
  `schema_version` 且按 `view`（`ctl-nats`/`offline`）判别、`--remote` 轻量摘要按 `view`(`ctl-remote`)
  判别且**无 schema_version**（见 cluster.md §5.6）。`proxy status --json`（P13）是早于本契约的**原始 proto dump**，
  无 `schema`/`schema_version`。**bump 政策**：仅破坏性改动（删/改键、改类型、改语义）才 bump；加
  omitempty 字段不 bump。branch-load-bearing 字段（`schema`/`schema_version`/`view`/`health`/`exit_code`/
  `errors`/`partial`/`is_leader_view`）**永不 omitempty**——其稳定在场就是 schema 的一部分。
- list 的空结果是 `[]` 而非 `null`（`jq '.nodes|length'` 恒有定义）。
- `alert ls --json` 带 `dedup_key`，可往返：
  `tether alert ls --json | jq -r '.alerts[].dedup_key' | xargs -n1 tether alert ack`。
- `cluster status --json` **不再吞 broker 错误**：报告在场但 broker 同时报错时，`errors[]` 收录、
  `partial:true`（绝不静默丢）；无报告则退出 `69`。`cluster status --remote --json` 是轻量 ctl 摘要
  （带 `view:"ctl-remote"`、**无 schema_version**），监控契约请用 socket/offline 的 `--json`（见 cluster.md §5.6）。

adminsock cluster admin 回复带稳定 `code`（`not_leader`/`already_voter`/`not_a_voter`/`catch_up_stalled`/
`quorum_confirm_required`/`nonce_used`/`cluster_not_enabled`/`node_unknown`/`store_error`/`bad_request`），
CLI 据此映射退出类 + 提示。

---

## 10. 常见问题 FAQ

**Q: 我能在一台机器上同时跑多个 agent 吗？**
A: 能。每个 agent 用不同 `--session` + `--nid`，nkey 在 `~/.tether/agent/<sid>/keys/`
分目录隔离。同 sid 同 nid 同时跑两次会被 broker 拒（`actor_invalid` /
`provisioning conflict`）。

**Q: 把 PIN 公开了怎么办？**
A: `tether session rm <sid>` 把会话标 DELETING（旧 PIN 全部失效），然后
`tether session create <name> --pin <newpin>` 重建。所有 agent 必须用新 PIN
重新加入。

**Q: 同一个使用者在多台笔记本登录？**
A: nkey 在 `~/.tether/keys/`，复制到另一台同样路径即可。或者两台都首次登录
（PIN 把两个 nkey 都注册成 member，broker 视为不同身份）。

**Q: `tether run` 卡 raw mode 怎么办？**
A: 远端进程崩溃没正常退出时，本地终端可能停在 raw 模式。`reset` 或 `stty sane`
恢复。

**Q: agent 想换机器 / 改 nid？**
A:
1. owner 在原机：`tether admin evict <sid> <old-nid>`（当前没有 ctl 端 evict
   入口，必须在 broker 本机）；
2. 新机重跑 `install.sh --role agent --nid <new-nid> --pin <pin>`；
3. owner 改成 `evict` 后旧 agent 进程会在 1s 内自杀。

**Q: 想批量 expose？**
A: 当前没有批量入口，写个 shell loop：
```bash
for nid in gpu-01 gpu-02 gpu-03; do
  tether expose $nid --local 8888 --name jupyter-$nid
done
```

**Q: 如何只授权别人看 ps 不能 exec？**
A: 当前不支持细粒度 RBAC。member 拿到 PIN 后即可 exec / run / expose。如需要
划分角色，开多个 session（每个 session 单独 PIN + member 名单）。owner-only
操作只有 `session rm` 与 `node upgrade`。

**Q: broker 能横向扩展吗？**
A: proto v2 支持用 `tether cluster` 把 broker 迁移为 Raft 管理的多 broker 集群；
入口见 cluster.md §5.6，生产步骤以 `docs/cluster-runbook.md` 为准。注意 `tether proxy`
当前仍不支持 cluster HA 模式。

**Q: 数据加密？**
A: 控制面 wss://（Caddy 终端 TLS）；nkey 签名鉴权。数据面（`expose` 反向 TCP）
明文 —— 暴露的服务自己负责 TLS（如 Jupyter 用 `--certfile`）。

**Q: 退出会话但保留 agent？**
A: ctl 端 `tether logout` 只清当前活动 session；agent 自己继续跑。要彻底
清成员关系：让 owner `tether admin evict`（当前无 ctl 入口）。

---

## 11. 安全注意事项

1. **PIN 是一次性入场券**：PIN 仅在首次 join 时绑定 nkey，绑定后失效。但绑定前
   PIN 在带内传输 ⇒ 永远走带外渠道（口头 / 加密短信）。
2. **`TETHER_DEV_NO_AUTH=1` 仅本地**：跳过身份验证；启了它的 broker 接受任何
   匿名连接，等于把整个集群裸奔到公网。
3. **upgrade URL 白名单**：默认只放本仓库的 GitHub release 前缀。换成自托管
   镜像时只允许你完全控制的 HTTPS 源 + 由你签名/审核过的 sha256，避免任何
   "受 broker 信任的"任意下载源被劫持。
4. **admin socket 不出本机**：见 broker-ops.md §7.3。
5. **sudo 与隔离**：broker 必须 sudo 装（systemd unit + 系统用户）；
   agent / ctl 一律普通用户（`~/.local/bin`）。`tether agent` 不需要 root。
6. **ports 14000-14999 是公开的**：上面的 expose 没有再次鉴权（设计如此 —— 公开
   端口供任意 HTTP 客户端访问）。敏感服务自带认证，或在 expose 前先用
   `tunnel-only` 模式（当前无）。
7. **session rm 不可逆**：tombstone 后 history-<sid> 与 ports 全部回收，无 undo。
8. **agent 二进制可写**：upgrade 路径 `O_EXCL|O_NOFOLLOW` + 仅允许 `tether`
   tar entry，但本地用户能直接覆写。共享机器上 agent 应跑独立用户 + 锁定
   `~/.local/bin/tether` 的 ACL。

---

## 12. 开发与本地调试

### 12.1 本地无鉴权 demo

```bash
make nats-server-install                    # 一次性
make nats-dev                               # term A: nats-server -js -DV

TETHER_DEV_NO_AUTH=1 ./bin/tether serve \
  --db ./tether.db \
  --admin-socket /tmp/tether-admin.sock     # term B

TETHER_DEV_NO_AUTH=1 ./bin/tether agent \
  --session lab --nid lab-1                 # term C

./bin/tether admin --socket /tmp/tether-admin.sock nodes  # term D
```

### 12.2 测试

```bash
make test            # 单元 + 包内测试，~30s
make e2e-parallel    # P1-P10 完整 e2e；44 核开发机约 3 分钟，小型 CI 自动降并行度
make lint            # golangci-lint v2.5.0
```

跑单 phase：
```bash
go test -count=1 ./test/p7/...
go test -count=1 -v -run TestRunExitCodePropagates ./test/p5/
```

### 12.3 排查 broker 行为

```bash
# 详细日志
./bin/tether serve --config /etc/tether/broker.yaml 2>&1 | tee broker.log

# 看 NATS 实际订阅
nats sub 'tether.v2.>'

# 看 JetStream stream
nats stream ls
nats stream info history-lab
nats consumer ls history-lab
```

### 12.4 排查 agent

```bash
# 直接前台跑
TETHER_DEV_NO_AUTH=1 ./bin/tether agent --session lab --nid lab-1 \
  --nats-url nats://127.0.0.1:4222

# state.json
cat ~/.tether/agent/lab/state.json

# nkey
cat ~/.tether/agent/lab/keys/agent.nk           # seed（保密！0600）
cat ~/.tether/agent/lab/keys/agent.pub          # 公钥（可公开）
```

### 12.5 跨平台 build

```bash
GOOS=linux  GOARCH=amd64 CGO_ENABLED=0 go build -trimpath -o tether_linux_amd64  ./cmd/tether
GOOS=linux  GOARCH=arm64 CGO_ENABLED=0 go build -trimpath -o tether_linux_arm64  ./cmd/tether
GOOS=darwin GOARCH=arm64 CGO_ENABLED=0 go build -trimpath -o tether_darwin_arm64 ./cmd/tether
```

或一次性 `goreleaser build --snapshot --clean` 走 `build/goreleaser.yaml`。

---

## 附录 A：目录结构总结

> broker 主机目录结构见 [`broker-ops.md`](broker-ops.md) 附录 A。

### agent 主机
```
~/.tether/
├── keys/                       # 用户 ctl nkey（也作为 agent 的"actor 身份"）
│   ├── ctl.nk                  # 0600
│   └── ctl.pub                 # 公钥
├── agent/<sid>/                # 0700
│   ├── agent.yaml              # 0600  install.sh 写的配置
│   ├── agent.log               # systemd append
│   ├── state.json              # 0600  expose token 持久化
│   └── keys/
│       ├── agent.nk            # 0600  agent 自身 nkey 种子
│       └── agent.pub
└── current_session             # 0644  ctl 活动 session 文件
```

## 附录 C：版本与发布

- 公开 GitHub Release 见
  `https://github.com/LinZiyang666/dist_experiment_control/releases`。
- 发布流水线：`git tag vX.Y.Z && git push --tags` 触发
  `.github/workflows/release.yml`，由 goreleaser 出 4 个平台的 tarball +
  `install.sh` + `SHA256SUMS` 上传 Release。
- `install.sh` 在 `--version` 缺省时嗅探 `releases/latest` 的 301 重定向自动
  锁到最新 tag；`--source-base` 指向私有镜像时跳过嗅探。
- ProtoVersion bump（wire 协议不兼容）走 §8.3 全量重装，不能用
  `tether node upgrade`；同一 ProtoVersion 内 patch 升级是 `tether node
  upgrade` 的支持路径。

排查 / 反馈：把 `tether version` + 完整错误输出 + `journalctl -u tether-broker
--since '10 min ago'` 一起贴到 issue。
