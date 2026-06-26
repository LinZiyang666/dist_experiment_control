# tether 使用手册

> 面向运维 / 实验机管理员 / 普通使用者的全量操作文档。架构设计请看
> `docs/architecture.md`，本文只讲"怎么用、怎么部、怎么排错"。

适用版本：v0.1.0+（公开 GitHub Release 已发布）。子命令名与 flag 兼容性遵循
SemVer；wire 协议变更走 `tether.v1.*` → `tether.v2.*` 的整次跨版本路径，详见
§8.3。

---

## 目录

1. [总体心智模型](#1-总体心智模型)
2. [安装与部署](#2-安装与部署)
3. [配置文件](#3-配置文件)
4. [命令速查表](#4-命令速查表)
5. [完整命令参考](#5-完整命令参考)
6. [日常使用场景](#6-日常使用场景)
7. [运维 / 维护](#7-运维--维护)
8. [升级](#8-升级)
9. [错误码与故障排查](#9-错误码与故障排查)
10. [常见问题 FAQ](#10-常见问题-faq)
11. [安全注意事项](#11-安全注意事项)
12. [开发与本地调试](#12-开发与本地调试)

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
> 告警 banner 不渲染）。没有集群需求就整段跳过 §5.6 `cluster` 与 `docs/cluster-runbook.md`。

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

### 2.3 broker（运维侧，需 sudo + 域名 + ACME 邮箱）

> 下面这套**单机 broker 就是生产推荐的默认部署**，不需要任何 cluster 步骤。要做多机 HA 才继续看
> 本节末尾的"分布式 HA 安装边界"以及 §5.6 / `docs/cluster-runbook.md`。

```bash
curl -fsSL https://github.com/LinZiyang666/dist_experiment_control/releases/latest/download/install.sh | sudo sh -s -- \
  --role broker \
  --domain tether.example.com \
  --acme-email admin@example.com
```

写入：
- `/usr/local/bin/{tether, nats-server, caddy}`（nats-server v2.10.22, caddy v2.7.6，
  逐个 sha256 校验后落地）
- `/etc/tether/{broker.yaml, Caddyfile}`
- `/etc/systemd/system/{nats-server, tether-broker, caddy}.service`
- `/var/{lib,log,run}/tether`（属主 `tether` 系统用户，`install.sh` 自动 useradd）

> **分布式 HA（proto v2）安装边界**：升级成集群时，tether 接管 `nats.conf`（`tether cluster
> reconcile nats --all --wait`，见 §3.4）并留 `nats.conf.bak.<ts>`；cluster secrets（`/etc/tether/secrets/`：
> `cluster-ca.pem`、`route-cert.pem`/`route-key.pem`、稳定 `tunnel-cert.pem`/`tunnel-key.pem`、
> `broker.nk`、`node-ident.nk`、`account.nk`，私钥 0600）须先 provision（`tether cluster doctor`
> 预检：缺/不可读/私钥权限松 = 拒；FDE 缺 = advisory）。现网单点→N≥3 的一次性迁移全流程 +
> 回滚见 **`docs/cluster-runbook.md` 第 4 节**。proto v2 不与现网 v1 车队 wire 兼容（需协调全车队重装）。

启动：

```bash
sudo systemctl daemon-reload
sudo systemctl enable --now nats-server tether-broker caddy
sudo systemctl status nats-server tether-broker caddy
```

`broker.upgrade.url_allow` 的内置默认值已经包含本仓库的 GitHub release 前缀，
开箱即可 `tether node upgrade`；要自托管镜像才需要改 `broker.yaml` 或
`--upgrade-url-allow` 覆盖。auth_callout 启用见 §3.4。

### 2.4 install.sh flag 全集

| flag | 说明 |
|---|---|
| `--role {agent,ctl,broker}` | 角色（默认 ctl） |
| `--broker URL`              | agent 用，必填 |
| `--session SID`             | agent 用，必填 |
| `--pin PIN`                 | agent 用，仅首次连接需要 |
| `--nid NID`                 | agent 用，必填 |
| `--domain DOMAIN`           | broker 用，必填 |
| `--acme-email EMAIL`        | broker 用，必填 |
| `--version VER`             | 默认 `v0.0.0-dev` ⇒ 触发 latest-tag 嗅探；显式如 `v0.1.0` 则锁版本 |
| `--source-base URL`         | 覆盖 release tarball 基址（自托管镜像走这条；同时跳过 latest 嗅探） |
| `--prefix DIR`              | 覆盖安装根目录 |
| `--dry-run`                 | 演练，不下载/不写文件（也跳过 latest 嗅探） |
| `--skip-download`           | 跳过下载，使用已就位的二进制 |
| `--uninstall`               | 卸载本角色 |

环境变量 `TETHER_VERSION` 等价于 `--version`。

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
日常运行 / 部署用 §2.1–2.3 即可。

```bash
git clone https://github.com/LinZiyang666/dist_experiment_control.git
cd dist_experiment_control
make build                 # 输出 ./bin/tether
make test                  # 单元 + 包内测试，~30s
make e2e                   # 端到端矩阵，~80s（嵌入式 nats-server）
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

### 3.3 `/etc/tether/broker.yaml`（broker）

```yaml
broker:
  domain: tether.example.com           # ACME + Caddyfile 主键
  public_host: tether.example.com      # `tether expose` 输出的 URL host
                                       # 缺省 fallback 到 broker.domain
  nats:
    url: nats://127.0.0.1:4222         # tetherd → 本机 nats-server
    wss_listen: ":443"                 # Caddy 监听
    ws_internal: "127.0.0.1:8222"      # Caddy 反代到 nats-server WS
  frp:
    bind_addr: "0.0.0.0"               # 公开端口绑定地址
    control_listen: ":7000"            # 反向隧道控制监听
    port_range: "14000-14999"          # 公开端口分配带（含两端）
  admin:
    socket: /var/run/tether/admin.sock # `tether admin *` 走的本机 Unix socket
  storage:
    db: /var/lib/tether/tether.db      # SQLite 文件
    js_store: /var/lib/tether/jetstream # JetStream store dir
  upgrade:
    url_allow:                         # `tether node upgrade` 白名单（可选）
      - https://github.com/LinZiyang666/dist_experiment_control/releases/
      - https://releases.example.com/tether/   # 追加自托管镜像（可选）
  cluster:                             # 分布式 HA 时填写；空/无 raft state = 单机模式
    data_dir: /var/lib/tether          # holds raft/ + tether.db
    raft_addr: 10.0.0.1:7400           # 私网 Raft transport
    secrets_dir: /etc/tether/secrets   # cluster-ca/route/tunnel/node-ident/broker/account seeds
```

修改后 `sudo systemctl restart tether-broker` 生效（不支持热重载）。

`broker.upgrade.url_allow` 缺省值是 `https://github.com/LinZiyang666/dist_experiment_control/releases/`
（与本仓库 release 一致），开箱即可 `tether node upgrade`。yaml 或
`--upgrade-url-allow` 给出非空切片即整段覆盖默认；要禁用 upgrade 显式给
单个不存在的前缀即可。

`frp.port_range` 校验严格：`low-high` 形式，二者均 `1..65535` 且 `low ≤ high`，
非法输入直接 fatal 退出（避免操作员把 `1400-1499` 写成 `14000-14999` 后默默落
回默认）。

`broker.cluster.*` 只在完成 `tether cluster init --from-existing` 后开启。cluster
模式下 `tether serve` 不再直接打开 `broker.storage.db` 写库，而由 Raft 节点持有唯一
WAL writer；同时默认从 `broker.cluster.secrets_dir` 读取 `broker.nk` + `account.nk`
启用 auth_callout。只有把 seeds 放到非 secrets_dir 路径时才需要额外传
`--auth-callout-seeds-dir`。

### 3.4 nats-server.conf 启用 auth_callout

`install.sh --role broker` 默认写出的 nats.conf 已经预留 auth_callout 占位但
**没有启用**（开机即可登录、便于第一遍部署排错）。**正式开放公网必须启用
auth_callout**，否则 ctl 会得 `nats: nkeys not supported by the server`，
任何匿名连接都能进集群。

> **单 broker（非集群）保持本节的手动流程不变**——`install.sh` 写 nats.conf + 你按下面手动加
> `authorization{}`，是单 broker 的受支持路径。**分布式 HA（proto v2）下不要手改 nats.conf**：改用
> `tether cluster reconcile nats --all --wait` 让 tether 接管——它在保留 install.sh 的 websocket/jetstream +
> 已记录的调优指令（如 `max_payload`）的前提下重写出集群指令（routes mTLS + auth_callout + per-broker
> ACL），遇到不认识的指令会 fail-closed 拒绝（不静默覆盖手调 conf），并留一份 pristine `.bak`。整套
> 现网单点→集群的一次性迁移见 §2.3 指向的 `docs/cluster-runbook.md` 第 4 节。
> 这条**自动路径**从 live roster + secrets 派生每台 broker 的 server-name / route-url / bus nkey，
> 因此**不再手传 per-broker `--broker-nkey`**——多 broker 也由 `--all` 一次性按 roster 渲染全 mesh。

#### 第 1 步：生成 broker.nk + account.nk

二选一：

**A. 在能跑 Go 1.20+ 的 dev 机上**（broker 主机 Go 太老就走这条）：

```bash
mkdir -p /tmp/seeds && cd /tmp/seeds
cat > go.mod <<'GOMOD'
module genseeds
go 1.25
require github.com/nats-io/nkeys v0.4.7
GOMOD
cat > gen.go <<'GO'
package main
import ("fmt"; "os"; "github.com/nats-io/nkeys")
func dump(name string, kp nkeys.KeyPair) {
    seed, _ := kp.Seed()
    pub, _ := kp.PublicKey()
    _ = os.WriteFile(name+".nk", seed, 0o600)
    fmt.Printf("%s_PUB=%s\n", name, pub)
}
func main() {
    uk, _ := nkeys.CreateUser();    dump("broker", uk)
    ak, _ := nkeys.CreateAccount(); dump("account", ak)
}
GO
go mod tidy && go run gen.go
# 输出形如：
#   broker_PUB=UBVHCNRDN34LOXMAW5PAUYAKAT4DC5CPKRO4GQHF3XD4K2P3PITVY6FV
#   account_PUB=AC4AQPOXCEMJDK6O3RQV4R2TG44VKH4DSFSPUI42BMH66DL3CRAXCFT3
# 落地两个 .nk 文件 + 把两个 PUB 记下来
scp broker.nk account.nk root@your-broker:/tmp/
```

**B. 在 broker 主机直接装 nats-io 的 `nk` 工具**（要 Go 1.20+）：

```bash
go install github.com/nats-io/nkeys/nk@latest
~/go/bin/nk -gen user    > /tmp/broker.nk
~/go/bin/nk -inkey /tmp/broker.nk -pubout
~/go/bin/nk -gen account > /tmp/account.nk
~/go/bin/nk -inkey /tmp/account.nk -pubout
```

#### 第 2 步：在 broker 上落地 seeds

```bash
sudo install -d -o tether -g tether -m 0700 /etc/tether/seeds
sudo install -m 0600 -o tether -g tether /tmp/broker.nk  /etc/tether/seeds/broker.nk
sudo install -m 0600 -o tether -g tether /tmp/account.nk /etc/tether/seeds/account.nk
sudo rm /tmp/broker.nk /tmp/account.nk    # 清临时副本
```

#### 第 3 步：改 nats.conf 加 authorization 块

⚠️ **nkey 用户不能带 `user:` 或 `password:` 字段**（写错会得
`Nkey users do not take usernames or passwords`，nats-server 拒绝启动）。
`auth_callout.auth_users` 必须用 nkey 公钥本身做身份引用，不是 alias 字符串。
把下面两处 `$BROKER_PUB` 与一处 `$ACCOUNT_PUB` 替换成第 1 步打印的实际值：

```bash
sudo tee /etc/tether/nats.conf > /dev/null <<EOF
host: "127.0.0.1"
port: 4222

jetstream {
  store_dir: "/var/lib/tether/jetstream"
}

websocket {
  host: "127.0.0.1"
  port: 8222
  no_tls: true
}

authorization {
  users: [
    { nkey: "$BROKER_PUB" }
  ]
  auth_callout {
    issuer: "$ACCOUNT_PUB"
    auth_users: [ "$BROKER_PUB" ]
  }
}
EOF
```

字段含义：
- `users:` 列出 nats-server **直接认可**的 nkey 身份。这里只放 broker 自己，
  且只能写 `nkey:` —— 加 `user:` 会被 nats-server 拒绝。
- `auth_callout.issuer:` 信任由这个 account 公钥签发的 JWT。broker 用对应的
  account.nk 私钥给来访 ctl/agent 签发临时 JWT。
- `auth_callout.auth_users:` 哪些身份会触发 callout —— 这里就是 broker 自己
  的 nkey 公钥；除它之外的所有连接都走 callout 流程，由 broker 决定放不放行。

写完先 `nats-server -c /etc/tether/nats.conf -t` dry-run 一下，输出
`configuration file ... is valid` 再继续。

#### 第 4 步：改 tether-broker.service 传 seeds dir + 重启

```bash
sudo sed -i 's|tether serve --config /etc/tether/broker.yaml|& --auth-callout-seeds-dir /etc/tether/seeds|' \
  /etc/systemd/system/tether-broker.service

sudo systemctl daemon-reload
sudo systemctl reset-failed nats-server tether-broker
sudo systemctl restart nats-server tether-broker
sleep 2
sudo systemctl status nats-server tether-broker --no-pager | head -20
sudo tail -10 /var/log/tether/broker.err   # 应见 "auth_callout=on (seeds=...)"
```

#### 验证

```bash
# 笔记本：
tether session create lab --pin 040415 --nats-url wss://your-broker.example:443
# 不应再报 "nkeys not supported"
```

单 broker 未配 `--auth-callout-seeds-dir` 时以 P2 模式运行（无 NATS 层身份强制，
仅适合 dev / 内网 demo）。cluster 模式例外：`tether serve` 会默认从
`broker.cluster.secrets_dir` 读取 `broker.nk` / `account.nk`。

---

## 4. 命令速查表

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
| 管理分布式 broker 集群 | `tether cluster init/add/status/drain/remove/...`（§5.6，完整流程见 `docs/cluster-runbook.md`） |
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
| `tether proxy on / off`                | ctl(owner) | 代理订阅总开关（自建机场，§5.15；当前 cluster HA 模式不支持） |
| `tether proxy status`                  | ctl    | 看开关/在线节点/订阅（member 可读，无密钥） |
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
| `tether cluster keygen --out <path>`   | broker 本机 | 生成 node identity seed（`node-pub` 现为隐藏 debug 命令） |
| `tether cluster drain` / `retire`      | broker 本机 | 迁移（drain）或退役（retire，原 `drain --retire`）broker voter |
| `tether cluster rebalance proxy [--dry-run]` | broker 本机(leader) | 主动把 `__proxy__` homes 均摊到各 eligible voter（reaper 只在 home down 时迁；加完 broker 后跑此填新容量；`--dry-run` 预览） |
| `tether cluster rotate-tunnel-cert`    | broker 本机 | 轮换 broker 稳定 tunnel 证书 pin |
| `tether cluster recovery force-single` / `recovery rejoin prepare` | broker 离线 | quorum-loss 逃生 / returning-node 清理 |
| `tether cluster backup [--offline] --out <dir>` | broker 本机/离线 | 写 `{state.db, manifest.json}` 备份 bundle（online 任意节点；`--offline` daemon 停止时） |
| `tether cluster recovery restore <bundle> --confirm-node-id <id>` | broker 离线 | 从 bundle 恢复为单 voter cluster（不可逆、typed-confirm、再用 §1 join 流程长回去） |
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

### 5.5 `tether serve`（broker）

```
tether serve [--config /etc/tether/broker.yaml] [flag...]
```

**这是什么**：broker 守护进程的入口。在公网服务器上长驻，扮演 NATS 订阅者
+ 节点状态机 + 反向隧道控制端 + 审计 sink + 端口分配器。单机模式只有一个
`tether serve`；分布式 HA 模式每个 broker 节点各运行一个 `tether serve`。

**核心职责**：
1. **NATS 鉴权与路由**：通过 `auth_callout` 校验 ctl/agent 的 nkey，把命令
   subject 路由到目标 agent；
2. **会话与节点状态机**：维护 SQLite 里的 sessions / members / nodes /
   port_allocations / agent_provisioning 表；
3. **反向隧道控制端**：在 `:7000` 接受 agent 的反向连接，再把
   `[14000-14999]` 公开端口透传给上游；
4. **审计 sink**：把每条 call/proc/port 事件写入 JetStream `history-<sid>`
   stream；
5. **admin 接口**：监听本机 `/var/run/tether/admin.sock` 供运维脚本访问。

**何时用**：仅 broker 主机部署时由 systemd 拉起，操作员极少手动跑（除
debug）。普通使用者不会接触此命令。

精度规则：**显式 flag > broker.yaml > cobra 默认**。

| flag | 默认 | yaml 等价 |
|---|---|---|
| `--config`               | (空)                           | — |
| `--nats-url`             | `nats://127.0.0.1:4222`        | `broker.nats.url` |
| `--db`                   | `./tether.db`                  | `broker.storage.db` |
| `--auth-callout-seeds-dir` | (空 = 单机 P2 dev；cluster 默认用 `broker.cluster.secrets_dir`) | — |
| `--public-host`          | `localhost`                    | `broker.public_host`，缺则 `broker.domain` |
| `--tunnel-addr`          | `0.0.0.0:7000`                 | `broker.frp.control_listen` |
| `--tunnel-public-host`   | `0.0.0.0`                      | `broker.frp.bind_addr` |
| `--store-dir`            | (空 = 不监控)                   | `broker.storage.js_store` |
| `--sub-http-listen`      | (空 = 禁用)                     | `broker.sub.listen` |
| `--admin-socket`         | `/var/run/tether/admin.sock`   | `broker.admin.socket` |
| `--upgrade-url-allow` (slice) | (空 ⇒ 退回内置默认 = 本仓库 GitHub release 前缀)  | `broker.upgrade.url_allow` |
| `--cluster-data-dir`     | (空)                           | `broker.cluster.data_dir` |
| `--cluster-raft-addr`    | (空)                           | `broker.cluster.raft_addr` |
| `--cluster-secrets-dir`  | (空)                           | `broker.cluster.secrets_dir` |
| `--log-level`            | `info`（debug/info/warn/error） | (flag-only) |
| `--log-json`             | `false`（结构化 JSON 日志）      | (flag-only) |
| `--metrics-listen`       | (空 = 禁用)                     | (flag-only) |

`--log-level`/`--log-json`（B5 OPS#8）同样作用于 `tether agent`；非法 level 直接报错退出（不静默默认）。

**`--metrics-listen`（B5 OPS#1，Prometheus 观测端点）**：设为 `host:port`（如 `127.0.0.1:9090` 或私网口）启用 HTTP `/metrics` + `/healthz` + `/readyz`；**空 = 不起监听**（零开销、字节等价）。
- `/metrics`：纯文本 exposition——`tether_broker_{cluster_mode,is_leader,voters,quorum_margin,applied_index,commit_index,force_single,alerts_active}` + per-peer `peer_applied_lag{node}`/`peer_reachable{node}`（leader 才有、最多 ~5s 旧）。单 broker 只出 `cluster_mode 0`+`alerts_active`，**绝不假造 HA**。
- `/healthz`：监听起来即 200（进程存活）。`/readyz`：单 broker→200；集群→200 当且仅当**有 leader 且本节点是健康 serving VOTER**（自身 CATCHING_UP/RETIRING/VOTER_ADD_FAILED/roster-vs-raft 不一致→503，便于负载均衡摘除本节点；2-voter 等 DEGRADED-but-serving 仍 200）。
- **安全**：端点**无鉴权**、只暴露公开拓扑（leader 标志、voter 数、raft index、per-peer lag、告警**计数**、cert **指纹**），**绝不含**任何 nkey/seed/token/cert 私钥。与 `--sub-http-listen` 不同，它**不强制 loopback**（运维按需绑私网/监控网）——**务必绑在私网或监控网，勿暴露公网**。
- **磁盘/端口 DEGRADE band（B6 OPS#4）**：本节点自身 disk_free<10% 或 ports≥90% used 会让 `cluster status` 进 **DEGRADED（exit 1）**——但**绝不**覆盖 FORCE_SINGLE(3)/QUORUM_LOST(2)（容量是次要降级）。容量值仍在 `cluster status --json` 与 `/metrics` 可见，磁盘吃紧也仍发 replicated `disk_pressure` 告警（`tether alert ls` / `alerts_active`）。`/metrics` 另有 `tether_broker_stream_replicas_{actual,target}`（仅集群 + 已观测时）。
- **告警 webhook（B6 OPS#2）**：`broker.observability.alert_webhook_url`（或 `--alert-webhook-url`，仅集群模式）令 leader 在每次 committed 告警 raise/clear 时 POST 到该 http/https 端点（body 仅公开拓扑、绝无密钥；URL 不得含 userinfo）。非阻塞投递、队列满即丢（端点挂死不卡 reconcile）。空 = 关闭、零额外 wiring。

cluster 模式由 `--cluster-data-dir` 下的 raft state 触发，不是单靠 yaml 字段触发。
启动前必须先跑 `tether cluster init --from-existing`；完整步骤见
`docs/cluster-runbook.md` 第 4 节。集群内部还需要私网 `:7400` Raft 和 `:6222`
NATS route；不要暴露到公网。

启动后处理 `SIGINT/SIGTERM` 优雅退出（tunnel 等所有子组件按 ctx 拆解）。

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
| 在线 admin socket | `join approve` / `drain` / `retire` / `status` / `transfer-leader` / `rotate-tunnel-cert` / `reconcile nats` / `ops` | broker 主机，daemon 正在运行；默认走 `--socket /var/run/tether/admin.sock` |
| 离线/本机磁盘操作 | `init --from-existing` / `join prepare` / `recovery force-single` / `recovery rejoin prepare` / `recovery restore` / `recovery node remove --manual` / `recovery incident export` / `doctor` / `keygen` | 对应 broker 主机；`recovery force-single`、`recovery rejoin prepare` 要先停 daemon |

全局 flag：

| flag | 默认 | 说明 |
|---|---|---|
| `--socket` | `/var/run/tether/admin.sock` | 在线 admin socket 子命令连接的本机 Unix socket；离线命令通常不使用它 |

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
回滚和故障演练以 `docs/cluster-runbook.md` 为准；`usage.md` 这里只放入口和常用边界，
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
| `cluster recovery force-single` | `--data-dir` | `/var/lib/tether` | broker data dir；daemon 必须停止 |
| `cluster recovery force-single` | `--db` | `/var/lib/tether/tether.db` | 本机 DB 路径 |
| `cluster recovery force-single` | `--self-id` | (必填) | 当前幸存节点 id；命令会要求人工输入它确认 |
| `cluster recovery force-single` | `--self-addr` | (必填) | 当前幸存节点 Raft 地址，形如 `<host>:7400` |
| `cluster recovery force-single` | `--confirm-peers-dead` | (必填，可逗号分隔) | roster 中其它所有节点 id；命令会探测它们 `:7400` 仍可达则拒绝 |
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
| `alert ls` | `--nats-url` | 同 §5.1 全局解析链 | broker NATS 入口；分布式 HA 可写逗号分隔 seed list |
| `alert ls` | `--home` | `~/.tether` | 读取 nkey、`current_session`、默认 broker URL 的目录 |
| `alert ack` | `<dedup_key>` | (必填) | 要确认的 alert key，来自 `alert ls` 输出 |
| `alert ack` | `--nats-url` | 同 §5.1 全局解析链 | broker NATS 入口；分布式 HA 可写逗号分隔 seed list |
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
  install.sh 生成的 Caddyfile 已把 `/sub/*` 反代到它(排在 NATS-WSS catch-all 之前)。**v0.3.0 之前装的 broker 升级后,这两处配置不会自动出现**——见 §8.5 手动迁移。

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

**tier A 实际上限**为 `min(8 MiB, broker.max_payload/2 - 1 KiB)`（源码
`cliTierAMaxBytes = 8*1024*1024`，再与 broker `caps.MaxPayload/2 - 1024` 取小）。
NATS 默认 `max_payload`（1 MiB）下 tier A 实际只能塞 ~500 KiB；要让 tier A 跑满
8 MiB，broker 的 NATS `max_payload` 必须 ≥ 16 MiB（broker.yaml 之外的 nats.conf
配置）。超过此阈值的请求由 ctl 自动升 tier B。

**先决条件**：

1. broker 必须已开 JetStream（用 tarball install 写出的 nats.conf 缺省即开）；
   否则 tier-B 会以 `jetstream_unavailable` 拒掉；
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
| `jetstream_unavailable` | tier-B 但 broker 没开 JetStream |
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

### 5.20 `tether admin *`（broker 本机）

**这一组命令是什么**：运维用的"broker 内部状态"工具，绕过 NATS
auth_callout，直接通过 broker 进程的本机 Unix socket 读写 SQLite 状态。
设计目标是：万一 NATS 鉴权坏了 / owner 跑路了 / 需要紧急踢节点时还能
干活。

**与普通 ctl 命令的区别**：

| 维度 | `tether session/ps/exec/...` | `tether admin *` |
|---|---|---|
| 鉴权 | NATS auth_callout（nkey + 成员关系） | 无鉴权（仅靠 socket 文件 mode 0600） |
| 跨 session | 否（受 ACL 限制） | 是（看到全部 session） |
| 网络 | 远程，走 NATS | 仅本机 Unix socket |
| 谁能用 | 任何 session member | 仅 broker 主机上有 socket 读权限的用户 |

**强约束**：admin socket **绝对不要**通过 ssh / socat / nginx 转发出本
机 —— 它没有再次身份验证，能做 evict / 看全量 audit 等高权限操作。

走 broker 的 `/var/run/tether/admin.sock`（mode 0600，owned by `tether`）。运行者
必须有读权限：

```bash
sudo -u tether tether admin sessions
# 或 root：
sudo tether admin --socket /var/run/tether/admin.sock nodes
```

持久 flag（所有 admin 子命令均接受）：

| flag | 默认 | 说明 |
|---|---|---|
| `--socket` | `/var/run/tether/admin.sock` | broker admin Unix socket 路径（需读权限；仅限本机） |

子命令：

```
tether admin sessions                       # 全部 session
tether admin nodes                          # 全部节点（含心跳年龄、proto、release）
tether admin audit <sid> [-n 50]            # 审计尾部 N 条
tether admin evict <sid> <nid>              # 强制踢
```

各子命令含义：

| 子命令 | 是什么 | 何时用 |
|---|---|---|
| `admin sessions` | 列出 broker SQLite 里**全部** session（不区分 owner / member），含 ACTIVE / DELETING 状态、owner 指纹、创建时间。 | 排查"用户说看不见 session"、确认 tombstone 是否完成、容量盘点。 |
| `admin nodes` | 列出**全部**已注册 agent，含 sid、nid、当前状态（ONLINE/OFFLINE/STALE）、心跳年龄、proto 版本、release 版本。 | 排查 OFFLINE 节点、版本审计、找到要 evict 的目标。 |
| `admin audit <sid>` | 直接从 JetStream `history-<sid>` stream 读最近 N 条审计 entry（不走 ctl 的 NATS path）。 | NATS 鉴权坏了 / owner 失联但需要查历史 / 合规导出。 |
| `admin evict <sid> <nid>` | 强制把指定 (sid, nid) 从 broker 移除：删 `agent_provisioning` 行 + `nodes` 行 + 广播 `sys.events{type:agent_evicted}`。 | agent 失控 / 机器换主 / nkey 泄漏要立即吊销。 |

子命令参数：

| 命令 | 参数 / flag | 默认 | 说明 |
|---|---|---|---|
| `admin audit` | `<sid>` | (必填) | 要读取审计流的 session id |
| `admin audit` | `--n, -n` | `50` | 读取最近 N 条 |
| `admin evict` | `<sid>` | (必填) | 目标 session id |
| `admin evict` | `<nid>` | (必填) | 要强制踢出的 agent node id |

`evict` 行为（架构 P9 / I.2b）：
1. 删 `agent_provisioning` 行 + `nodes` 行；
2. broadcast `sys.events{type:agent_evicted, sid, nid}`；
3. 在线 agent 收到后约 1s 内自杀；离线 agent 下次 CONNECT 直接被拒（不再
   provisioned）。

分布式 HA 影响：`admin *` 是 broker 本机 socket 工具，不走 ctl 的 NATS
membership 路径，也没有 `--ack-alerts`。cluster 模式下应优先用 `tether cluster`
处理 roster/raft 层面的 broker 节点；`admin evict` 只处理 session 内 agent
provisioning，不会移除 broker voter，也不会修复 quorum。

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
- `jetstream_unavailable` → broker 没开 JetStream，tier B 走不了；
- 详细 audit：`tether history --kind transfer -n 20`。

---

## 7. 运维 / 维护

### 7.1 broker 健康检查

```bash
sudo systemctl status tether-broker nats-server caddy
sudo journalctl -u tether-broker --since '1 hour ago' -p warning
sudo ss -tlnp | grep -E '(443|4222|6222|7000|7400|14[0-9]{3})'
```

关键指标：
- `tether-broker` 启动日志：`tether serve: NATS=... DB=... auth_callout=on(...)/off(...)`
- 连接的 agent 数 = `tether admin nodes` 里 `STATE=ONLINE` 的行数
- 占用端口 = `tether admin sessions` ↔ `port_allocations`（每条 expose 一行）
- 分布式模式额外检查 `tether cluster status`：Raft 私网 `:7400`、NATS route `:6222`
  只在 broker 内网开放；公网仍只需要 443、7000、`frp.port_range`。

### 7.2 disk pressure 告警

JetStream store 占用超过磁盘 80% 时 broker 会发出 `sys.events{type:disk_pressure}`
并在日志里 `WARN`。处理方法：
1. `df -h /var/lib/tether/jetstream` 确认；
2. 列出 stream：`/usr/local/bin/nats stream ls`；
3. 删除已废弃 session 的 history（自动随 `session rm` 第三阶段回收，但若 owner 没
   rm，需要手动）：
   ```bash
   /usr/local/bin/nats stream rm history-<sid>
   ```
4. 或扩盘 / 调整 `jsstream.MaxBytesPerSession`（重新部署）。

### 7.3 admin socket 权限

`/var/run/tether/admin.sock` 设计为 `0600` + owner=tether，**只允许 broker 主机
本地访问**。安全保护：
- 父目录 `/var/run/tether` mode 0700，install.sh 创建时校验 uid（防符号链接攻击）。
- 永远不要把 admin.sock 通过 ssh tunnel / socat 转发出去 —— 它没有再次身份验证，
  能做 `evict` 等破坏性操作。

### 7.4 备份

只有两类持久数据：
1. `/var/lib/tether/tether.db` — SQLite，含 sessions / members / port_allocations /
   nodes / agent_provisioning。SQLite `.backup` 命令或单纯 `cp`（broker 可在线，
   busy_timeout=5s）：
   ```bash
   sudo -u tether sqlite3 /var/lib/tether/tether.db ".backup /backup/tether-$(date +%F).db"
   ```
2. `/var/lib/tether/jetstream/` — JetStream 文件存储。停机 + `tar` 是最稳的方法；
   在线快照需要 nats CLI：`nats stream backup history-<sid> ./snap`。

单机恢复：复制回原目录、保持属主 `tether:tether` mode 0750、`systemctl start` 即可。
分布式模式不要把某个旧 peer 的 `tether.db`/`raft/` 直接塞回新时间线；按
`docs/cluster-runbook.md` 的 force-single / recover 流程处理，并重新跑 `cluster status`。

### 7.5 日志轮转

systemd unit 用 `StandardOutput=append:/var/log/tether/broker.log`。建议 logrotate
配置：

```
/var/log/tether/*.log {
    daily
    rotate 14
    compress
    notifempty
    missingok
    copytruncate
    su tether tether
}
```

注意 **不要用 `create`**（会破坏 systemd append 的 fd）；`copytruncate` 是配合
append 的标准写法。

### 7.6 监控 tether 自身

最低限度的监控：
- `tether admin nodes` 里 `STATE=OFFLINE` 数量 > 0 报警；
- broker 进程存活：`systemctl is-active tether-broker`；
- NATS 连接数：`/usr/local/bin/nats server check connections`；
- JetStream 磁盘：`df` 阈值 80%；
- `journalctl -u tether-broker | grep -E '(disk_pressure|store_error|panic)'`。

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

### 8.4 broker 升级

```bash
sudo systemctl stop tether-broker
sudo install -m 0755 ./bin/tether /usr/local/bin/tether
sudo -u tether sqlite3 /var/lib/tether/tether.db 'PRAGMA integrity_check;'
sudo systemctl start tether-broker
sudo journalctl -u tether-broker --since '1 minute ago' -f
```

启动后 broker 跑 G.2 reconcile：
- 按 `nodes` 表里 `last_heartbeat_at` 计算每个 agent 应处于的状态；
- 等待 agent 重新 register（agent 端在 NATS 重连后会自动 re-register）；
- 期间 ctl 调用排队等待 broker 响应。

### 8.5 升级到 v0.3.0 启用代理订阅的 broker 配置迁移

`tether node upgrade` 只换 **agent** 二进制；§8.4 的 broker 升级只换 broker 二进制。
两者都**不会**改 broker 的 `broker.yaml` / `Caddyfile`。所以一台 **v0.3.0 之前装的
broker** 升级到 v0.3.0 后，`tether proxy` 的控制面能用，但 `/sub/<token>` 端点
不会自动出现——必须手动补两处配置；如果同时从 proto v1 升到 proto v2，仍按 §8.3
做全车队重装，不能只靠 `node upgrade`：

```bash
# 1) broker.yaml 增加 sub 块（缺省监听 127.0.0.1:8090）
sudo sed -i '/^  admin:/i\  sub:\n    listen: "127.0.0.1:8090"' /etc/tether/broker.yaml
# 或手动在 broker: 下加：
#   sub:
#     listen: "127.0.0.1:8090"

# 2) Caddyfile 在 $DOMAIN:443 { ... } 块里、catch-all `handle {` 之前插入 /sub 反代：
#    handle /sub/* {
#        reverse_proxy 127.0.0.1:8090
#    }
#    （顺序很重要：必须排在 NATS-WSS 的 catch-all 之前，否则 Clash 请求会被打到
#     nats-server，且 wss://$DOMAIN/nats 的 WSS upgrade 会坏。）

# 3) 重启并自检
sudo systemctl restart tether-broker caddy
curl -fsS "https://$DOMAIN/sub/<任一已签发 token>"   # 应回 Clash YAML
# 同时确认 wss://$DOMAIN/nats 仍能 upgrade（ctl 还能正常连）
```

省事也可以直接重跑 `install.sh --role broker --domain ... --acme-email ...`
重写 `broker.yaml` + `Caddyfile`（它幂等、不动 SQLite/JetStream 数据），但会覆盖
你对这两个文件做过的任何手改，谨慎。

---

## 9. 错误码与故障排查

CLI 输出错误统一格式：`<verb> failed: <人话提示> (<架构稳定的 code>)`。本节列
出全部已知 code 及处置。

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
> `leader_unavailable` 一起归在 §9.7.1。

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

### 9.7 NATS 连接失败

```
exec: cannot reach broker at nats://127.0.0.1:4222: <err> (verify the broker is running and --nats-url is correct)
```

排查清单：
1. `--nats-url` 是否正确（ctl 用 `wss://broker:443`，agent 同上，broker 自己用
   `nats://127.0.0.1:4222`）；
2. broker 主机 `systemctl status tether-broker nats-server caddy`；
3. `curl -v https://broker:443/` 能否拿到 Caddy 的握手；
4. 防火墙 / 安全组：公网放行 443 + 7000 + 14000-14999；分布式 broker 内网还要放行
   `6222`（NATS route）和 `7400`（Raft），不要把这两个私网端口暴露到公网。

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

### 9.10 admin 命令 connection refused

```
admin: dial /var/run/tether/admin.sock: connect: permission denied
```

- 你不在 broker 主机上 ⇒ `tether admin` 设计上只在 broker 本机用；
- 用户没读 socket 权限 ⇒ `sudo -u tether tether admin ...` 或 `sudo`；
- broker 没起 / `--admin-socket=` 被设空 ⇒ 检查 broker.yaml `broker.admin.socket`。

### 9.11 `make lint` 抱怨 Go 版本

```
language version 1.23 is lower than the targeted Go version 1.25
```

⇒ 用了 v1.x 的 golangci-lint。`make tools` 装好 v2.5.0 即可。

### 9.12 `make e2e` 挂起或 OOM

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

- `0..3` 仅 `tether cluster status` 的健康码（`0`=HEALTHY-HA、`1`=DEGRADED、`2`=read-only/quorum-lost、`3`=force-single），**只有该命令发**，且 `--remote` 更薄（永不出 1，见 §5.6）。注：`cluster status` 只要**产出报告**就退 `0..3`（按健康），broker 侧错误经 `errors[]`/`partial`/stderr 暴露而非改退出码；**只有拿不到报告才退 69**。
- `exec` / `run` **透传远端进程退出码**（任意 `0..255`，含 64/69/70/77/128+），不入分类器；判别靠"你跑的是哪个命令"，不是值。所以"77=权限"仅限 broker-RPC 命令。

**健壮重试规则**：把 `69`/`70`/`75` 当可重试（退避），仅 `64`/`77` 当终态。

### 9.14 机器 JSON 接口（`--json`）

list/status/结果类命令支持 `--json`（**opt-in，默认人读文本字节不变**）：`node ls` / `ps` /
`session ls` / `alert ls` / `expose` / `expose rm` / `admin sessions·nodes·audit` / `cluster status`。
（`node upgrade --all` 与 `transfer` 的 `--json` 在 B2.1 补。）

- 上述 8 个 list/result 载体各带 **`schema`（串判别符）+ `schema_version`（int）**；监控按
  `(schema, schema_version)` 派发。**例外**：`cluster status` 家族——socket/offline 报告带
  `schema_version` 且按 `view`（`ctl-nats`/`offline`）判别、`--remote` 轻量摘要按 `view`(`ctl-remote`)
  判别且**无 schema_version**（见 §5.6）。`proxy status --json`（P13）是早于本契约的**原始 proto dump**，
  无 `schema`/`schema_version`。**bump 政策**：仅破坏性改动（删/改键、改类型、改语义）才 bump；加
  omitempty 字段不 bump。branch-load-bearing 字段（`schema`/`schema_version`/`view`/`health`/`exit_code`/
  `errors`/`partial`/`is_leader_view`）**永不 omitempty**——其稳定在场就是 schema 的一部分。
- list 的空结果是 `[]` 而非 `null`（`jq '.nodes|length'` 恒有定义）。
- `alert ls --json` 带 `dedup_key`，可往返：
  `tether alert ls --json | jq -r '.alerts[].dedup_key' | xargs -n1 tether alert ack`。
- `cluster status --json` **不再吞 broker 错误**：报告在场但 broker 同时报错时，`errors[]` 收录、
  `partial:true`（绝不静默丢）；无报告则退出 `69`。`cluster status --remote --json` 是轻量 ctl 摘要
  （带 `view:"ctl-remote"`、**无 schema_version**），监控契约请用 socket/offline 的 `--json`（见 §5.6）。

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
入口见 §5.6，生产步骤以 `docs/cluster-runbook.md` 为准。注意 `tether proxy`
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
4. **admin socket 不出本机**：见 §7.3。
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
make e2e             # P1-P10 完整 e2e，~80s（embedded nats-server，无外部依赖）
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

### broker 主机
```
/etc/tether/
├── broker.yaml
├── Caddyfile
└── seeds/                      # 可选 auth_callout 种子
    ├── broker.nk
    └── account.nk

/etc/tether/secrets/            # cluster 模式必需；私钥 0600
├── cluster-ca.pem
├── route-cert.pem
├── route-key.pem
├── tunnel-cert.pem
├── tunnel-key.pem
├── node-ident.nk
├── broker.nk
└── account.nk

/var/lib/tether/                # 0750 owned by tether
├── tether.db                   # SQLite
├── tether.db-wal
├── tether.db-shm
└── jetstream/                  # JetStream store dir
    └── jetstream/$G/...

/var/log/tether/                # 0750 owned by tether
├── broker.log
└── broker.err

/var/run/tether/                # 0700 owned by tether
└── admin.sock                  # 0600

/etc/systemd/system/
├── nats-server.service
├── tether-broker.service
└── caddy.service

/usr/local/bin/
├── tether
├── nats-server
└── caddy
```

---

## 附录 B：网络端口

| 端口 | 协议 | 方向 | 用途 |
|---|---|---|---|
| 443        | TCP / TLS / WSS | 入站 broker | ctl + agent 的 NATS WSS（Caddy 终结 TLS） |
| 4222       | TCP             | 仅本机 | broker → 本机 nats-server |
| 6222       | TCP / mTLS      | broker 私网互通 | NATS route 集群连接 |
| 7400       | TCP / mTLS      | broker 私网互通 | Raft transport |
| 8222       | TCP             | 仅本机 | nats-server WebSocket（Caddy 反代） |
| 7000       | TCP             | 入站 broker | 反向隧道控制连接（agent 主动连） |
| 14000-14999| TCP             | 入站 broker | `expose` 公开端口 |

单机公网防火墙最少放行：`443, 7000, 14000-14999/tcp`（入方向）。
分布式 HA 额外只在 broker 私网/安全组内放行 `6222/tcp` 和 `7400/tcp`，不对公网开放。

---

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
