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
| broker | `tether serve` | 公网服务器（需 sudo / 域名 / TLS） | 1 |
| agent  | `tether agent` | 实验机（NAT 后） | N |
| ctl    | `tether login` / `exec` / `run` / `expose` / `ps` / `history` / ... | 使用者笔记本 | M |

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
| **broker**   | 公网中心节点，跑 `tether serve` + nats-server + Caddy。集群唯一。 |
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

# 可选：v0.2.0 起的 file_transfer / `tether push` / `tether pull` 白名单。
# 必须配置否则 push/pull 会以 `transfer_disabled` 拒掉。
file_transfer:
  allow_roots:
    - /home/<user>           # 用户家目录
    - /tmp                   # 临时区
    - /srv/local/<user>      # UIUC 风格 local-disk 根；其他场景可省
```

字段语义：

| 字段 | 含义 |
|---|---|
| `broker_url`  | NATS WSS 入口（=ctl 的 `--nats-url`） |
| `session`     | 这个 agent 服务的 session id |
| `nid`         | 节点 id；session 内必须唯一 |
| `tunnel_addr` | broker 反向隧道控制端口（默认 `host:7000`） |
| `file_transfer.allow_roots` | `tether push` 写、`tether pull` 读允许触达的绝对路径前缀列表；**为空 → push/pull 直接回 `transfer_disabled`** |

`allow_roots` 比对方式：先 `EvalSymlinks` 解析目标路径的父目录，再做"前缀 + `/`" 严格匹配，
所以中间的目录软链允许（解析后落在 root 内即可），叶子层符号链接一律拒绝
（`O_NOFOLLOW` + `lstat` 双道防线）。

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
```

修改后 `sudo systemctl restart tether-broker` 生效（不支持热重载）。

`broker.upgrade.url_allow` 缺省值是 `https://github.com/LinZiyang666/dist_experiment_control/releases/`
（与本仓库 release 一致），开箱即可 `tether node upgrade`。yaml 或
`--upgrade-url-allow` 给出非空切片即整段覆盖默认；要禁用 upgrade 显式给
单个不存在的前缀即可。

`frp.port_range` 校验严格：`low-high` 形式，二者均 `1..65535` 且 `low ≤ high`，
非法输入直接 fatal 退出（避免操作员把 `1400-1499` 写成 `14000-14999` 后默默落
回默认）。

### 3.4 nats-server.conf 启用 auth_callout

`install.sh --role broker` 默认写出的 nats.conf 已经预留 auth_callout 占位但
**没有启用**（开机即可登录、便于第一遍部署排错）。**正式开放公网必须启用
auth_callout**，否则 ctl 会得 `nats: nkeys not supported by the server`，
任何匿名连接都能进集群。

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

未配 `--auth-callout-seeds-dir` 时 broker 以 P2 模式运行（无 NATS 层身份强制，
仅适合 dev / 内网 demo）。

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
| 杀掉远端进程 | `tether run` 里按 Ctrl-C；或下次 v2 加 `tether kill` |
| 把远端服务暴露到公网 | `tether expose` / `tether expose rm` |
| 把文件上传/下载到远端 | `tether push` / `tether pull` |
| 查"过去发生了什么" | `tether history`（含 `--follow` 实时模式） |
| 给远端 agent 升级 | `tether node upgrade` / `... --all` |
| 在 broker 主机上做运维 | `tether admin sessions/nodes/audit/evict` |

完整速查：

| 命令 | 角色 | 用途 |
|---|---|---|
| `tether version`                       | 任意 | 打印版本 + 平台 |
| `tether serve`                         | broker | 启动 broker 守护进程 |
| `tether agent`                         | agent  | 启动 agent 守护进程 |
| `tether agent --install-user-service`  | agent  | 写 systemd --user unit（不启动） |
| `tether agent --uninstall`             | agent  | 删 systemd --user unit |
| `tether login`                         | ctl    | 加载 nkey 身份；可选激活 session |
| `tether logout`                        | ctl    | 清空 `current_session` |
| `tether ctx`                           | ctl    | 打印当前激活 session |
| `tether session create <name> --pin X` | ctl    | 创建 session（自己成 owner） |
| `tether session ls`                    | ctl    | 列出可见 session |
| `tether session rm <sid>`              | ctl    | tombstone（owner 限定） |
| `tether ps [-a]`                       | ctl    | 当前 session 的进程 + 端口 |
| `tether exec <nid> -- argv ...`        | ctl    | 非交互远程命令 |
| `tether run <nid> -- argv ...`         | ctl    | 交互式 PTY |
| `tether expose <nid> --local P --name N` | ctl  | 暴露端口 |
| `tether expose rm <nid> --name N`      | ctl    | 撤销暴露 |
| `tether push <local> <nid>:<remote>`   | ctl    | 上传本地文件到远端（≤2 GiB） |
| `tether pull <nid>:<remote> <local>`   | ctl    | 下载远端文件到本地（≤2 GiB） |
| `tether history [-n N] [--kind K] [-f]` | ctl   | 审计历史回放（含 `--kind transfer`） |
| `tether node ls [-a]`                  | ctl    | 列 session 内全部 agent（含 OFFLINE / STALE） |
| `tether node upgrade <nid>`            | ctl    | 升级单台 agent（owner） |
| `tether node upgrade --all`            | ctl    | 升级 session 内所有 ONLINE agent |
| `tether admin sessions`                | broker 本机 | 列所有 session |
| `tether admin nodes`                   | broker 本机 | 列所有节点 |
| `tether admin audit <sid> [-n N]`      | broker 本机 | 审计尾部 N 条 |
| `tether admin evict <sid> <nid>`       | broker 本机 | 强制踢节点 |

---

## 5. 完整命令参考

### 5.1 全局 flag

所有需要连 NATS 的子命令都接受：

| flag | 默认 | 说明 |
|---|---|---|
| `--nats-url`  | `nats://127.0.0.1:4222` | 连 broker 的 NATS 入口（ctl 通常 `wss://broker:443`） |
| `--home`      | `~/.tether`             | tether 的家目录 |

`tether agent` / `tether serve` 各自还有专属 flag，详见下文。

`tether exec` 与 `tether run` 均开启 `SetInterspersed(false)`：第一个位置参数后
所有 `-flag` 都视为远程 argv 的一部分，避免 cobra 把 `ls -la` 当成自己的参数解析
失败。

### 5.1.5 `tether version`

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
tether v0.0.0-dev (proto v1)
linux/amd64
go1.25.0
```

### 5.2 `tether serve`（broker）

```
tether serve [--config /etc/tether/broker.yaml] [flag...]
```

**这是什么**：broker 守护进程的入口。在公网服务器上长驻，扮演 NATS 订阅者
+ 节点状态机 + 反向隧道控制端 + 审计 sink + 端口分配器。整个 tether 集群
有且只有一个 `tether serve`。

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
| `--auth-callout-seeds-dir` | (空 = P2 dev 模式)            | — |
| `--public-host`          | `localhost`                    | `broker.public_host`，缺则 `broker.domain` |
| `--tunnel-addr`          | `0.0.0.0:7000`                 | `broker.frp.control_listen` |
| `--tunnel-public-host`   | `0.0.0.0`                      | `broker.frp.bind_addr` |
| `--store-dir`            | (空 = 不监控)                   | `broker.storage.js_store` |
| `--admin-socket`         | `/var/run/tether/admin.sock`   | `broker.admin.socket` |
| `--upgrade-url-allow` (slice) | (空 ⇒ 退回内置默认 = 本仓库 GitHub release 前缀)  | `broker.upgrade.url_allow` |

启动后处理 `SIGINT/SIGTERM` 优雅退出（tunnel 等所有子组件按 ctx 拆解）。

### 5.3 `tether agent`

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

`TETHER_DEV_NO_AUTH=1` 时 agent 以匿名身份连接，**仅在 broker 同样未启用
auth_callout 时**可用。

### 5.4 `tether login` / `logout` / `ctx`

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

flag（`login` 专属）：

| flag | 默认 | 说明 |
|---|---|---|
| `--nats-url`    | `nats://127.0.0.1:4222` | NATS broker URL |
| `--broker`      | `nats://127.0.0.1:4222` | `--nats-url` 的别名（与 `install.sh --broker` 拼写一致；两者共享同一变量，写任一都生效） |
| `--session, -s` | (空)                  | 要激活的 session id |
| `--pin`         | (空)                  | 首次加入时的一次性 PIN |
| `--home`        | `~/.tether`           | tether 家目录 |

`login` 显式给 `--nats-url` 或 `--broker` 之一时，broker URL 会被持久化到
`~/.tether/broker_url`，之后所有 ctl 子命令默认读它（见 §3.1
`TETHER_NATS_URL` 的优先级链）。

### 5.5 `tether session`

**这一组命令是什么**：管理"会话"这个核心隔离单位。一个 session 是
"一组 agent + 一组使用者 + 一段历史" 的容器：
- 同 session 内的 member 都能看到彼此的 ps / history / expose；
- 跨 session 完全隔离（NATS subject ACL + auth_callout 强制）；
- session 是计费、配额、生命周期回收的最小颗粒。

典型用法：每个项目 / 每个实验组开一个 session，owner 把 PIN 发给协作者。

```
tether session create <name> --pin <pin>    # 创建并自动激活；调用者成 owner
tether session ls                           # 列出可见 session
tether session rm <sid>                     # tombstone：ACTIVE → DELETING（owner）
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

### 5.6 `tether ps`

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

LOST 是 read-derived 状态——broker 的 SQLite 行还停在 RUNNING，
但所属 node 已被 reconcile 转成 OFFLINE。`tether ps` 在响应阶段
合成 LOST 标签，让运维一眼能看出"这个进程还没确认结束、但承载它
的 agent 跟丢了"。当 agent 重新 register 时，G.1 reconcile 会把
对应行收敛到 EXITED(rc=-1, missed-exit) 或重新接受 RUNNING。

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

### 5.7 `tether exec`

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
  terminated by signal" 然后本地 `exit 128`（v1 不传具体信号号）。

**何时用**：`ls` / `grep` / `python script.py` / `make test` / 任何
"输入定死、输出可流" 的命令。

**何时别用**：vim、htop、less、有进度条 / cursor 控制的命令 → 用
`tether run`（PTY 模式）。

```
tether exec <nid> -- <argv...>  [--cwd DIR] [--timeout 10m]
```

flag：

| flag | 默认 | 说明 |
|---|---|---|
| `--cwd`     | (空 = agent 端默认目录) | 远端进程的工作目录 |
| `--timeout` | `10m`              | 整次命令最长时间（含流式输出），超时返回 `timed out after ...` |

注：`--` 是可选分隔符（cobra 自动剥离）；`tether exec <nid> ls -la` 与
`tether exec <nid> -- ls -la` 等价，因为 `SetInterspersed(false)` 已让 `-la`
不被父命令解析。

### 5.8 `tether run`

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
tether run <nid> -- <argv...>  [--cwd DIR]
```

非 tty 调用（如 `tether run nid -- bash <<<EOF ... EOF`）时 SIGINT 转成
`kill.req{SIGINT}` 发往 agent 的进程组。

调优 env vars（详见 §3.1）：
- `TETHER_RUN_READY_TIMEOUT` — ctl 等待 agent PTY ready 的总超时，默认 `20s`；
  慢链路 / 跨大陆链路上 PTY 首字节迟到时调大。
- `TETHER_RUN_LIVENESS_TIMEOUT` — 心跳看门狗超时，默认 `15s`（agent 心跳间隔
  5s × 3）；agent 长时间停滞时 ctl 本地会先打印 "agent unreachable" 再退出，
  避免无限挂起。设为 `0` 关闭看门狗。

### 5.9 `tether expose` / `expose rm`

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
tether expose <nid> --local <port> --name <logical-name>
tether expose rm <nid> --name <logical-name>
```

- `<nid>` 节点上的 `--local` 端口被反向打到 broker 的 `[14000-14999]` 公网带，
  分配的端口 + URL 在输出里：`exposed: http://broker.example:14001 → lab-1:8888`。
- `--local` 必须为 1-65535（ctl 端强制校验，否则 `--local must be 1..65535`）。
- `--name` 是会话内的逻辑标识，`expose rm` 用它定位条目；同 session 内必须
  唯一。
- `expose rm` 立即把公网端口归还池子，agent 上对应的 tunnel client 也会
  drop。

子命令：v1 只有 `expose <nid>` / `expose rm <nid>`，**没有** `expose ls` —— 想看
当前已分配的暴露条目，用 `tether ps` 的 PORTS 节（数据源同为 broker SQLite 的
`port_allocations`）。

### 5.10 `tether push` / `tether pull`（v0.2.0+）

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
2. 目标 agent 的 `agent.yaml` 里必须配 `file_transfer.allow_roots`；
   为空 → push/pull 直接以 `transfer_disabled` 拒。

**命令**：

```
tether push <local-path> <nid>:<remote-path> [--force] [--timeout 10m]
tether pull <nid>:<remote-path> <local-path> [--force] [--timeout 10m]
```

参数与语义：

| 参数 | 说明 |
|---|---|
| `<local-path>` | ctl 所在机器上的本地文件路径，可为相对/绝对 |
| `<nid>:<remote-path>` | 目标节点 + 该节点上的**绝对**路径；`<remote-path>` 必须落在 `allow_roots` 内 |
| `--force` | 目标已存在时覆盖；缺省直接拒（`dst_exists`） |
| `--timeout` | 整次传输上限，缺省 10min（tier A 一般 < 30s，tier B 取决于带宽） |

**安全模型**（plan §"Refusing dangerous paths"）：

- 远端路径**必须绝对**且落在 `allow_roots` 任一前缀下；中间目录的软链
  允许，落地叶子的软链一律拒（`O_NOFOLLOW`）；
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
| `transfer_disabled` | 目标 agent 的 `allow_roots` 为空 |
| `path_outside_roots` | 远端路径不在 `allow_roots` 任一前缀下 |
| `path_parent_missing` / `path_not_found` | 父目录不存在 / 源文件不存在 |
| `not_a_regular_file` | 叶子是软链 / 设备 / 目录 |
| `dst_exists` | 目标已存在；加 `--force` 覆盖 |
| `sha_mismatch` | 接收端校验 SHA-256 失败（线上字节翻转 / 攻击） |
| `too_large` | 文件 > 2 GiB |
| `jetstream_unavailable` | tier-B 但 broker 没开 JetStream |
| `node_offline` / `not_a_member` | 目标节点离线 / 调用者不是 session 成员 |
| `not_owner_or_creator` | finalize 阶段 actor 跟 transfer 创建者不一致（防伪造） |

### 5.11 `tether history`

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

### 5.11.5 `tether node ls`

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

输出列：

| 列 | 含义 |
|---|---|
| `NODE`      | nid（节点 id） |
| `STATUS`    | `ONLINE` / `OFFLINE` / `STALE`（依 broker 端心跳分类） |
| `HEARTBEAT` | 最后心跳年龄（如 `<1s` / `2s` / `5m`），OFFLINE 时为最后一次见的年龄 |
| `PROTO`     | wire 协议版本号；与 broker 不一致 ⇒ 必须重装而非 upgrade |
| `RELEASE`   | release 版本字符串（如 `0.2.7`） |

底层 RPC 是 `node.list.req`，与 `node upgrade --all` 共享同一枚举机制。

### 5.12 `tether node upgrade`

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
| `--nats-url`| `nats://127.0.0.1:4222` | NATS broker URL |
| `--home`    | `~/.tether` | tether 家目录 |

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

### 5.13 `tether admin *`（broker 本机）

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

`evict` 行为（架构 P9 / I.2b）：
1. 删 `agent_provisioning` 行 + `nodes` 行；
2. broadcast `sys.events{type:agent_evicted, sid, nid}`；
3. 在线 agent 收到后约 1s 内自杀；离线 agent 下次 CONNECT 直接被拒（不再
   provisioned）。

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
tether exec gpu-01 --cwd /work -- python train.py --epochs 10
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
# 把本地脚本送到 a100，落到 agent allow_roots 内的绝对路径
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
- `tether push` 报 `transfer_disabled` → agent.yaml `file_transfer.allow_roots` 没配；
- `path_outside_roots` → 远端绝对路径不在 `allow_roots` 任一前缀下；
- `dst_exists` → 远端已有同名文件，加 `--force` 覆盖；
- `jetstream_unavailable` → broker 没开 JetStream，tier B 走不了；
- 详细 audit：`tether history --kind transfer -n 20`。

---

## 7. 运维 / 维护

### 7.1 broker 健康检查

```bash
sudo systemctl status tether-broker nats-server caddy
sudo journalctl -u tether-broker --since '1 hour ago' -p warning
sudo ss -tlnp | grep -E '(4222|7000|14[0-9]{3}|443)'
```

关键指标：
- `tether-broker` 启动日志：`tether serve: NATS=... DB=... auth_callout=on(...)/off(...)`
- 连接的 agent 数 = `tether admin nodes` 里 `STATE=ONLINE` 的行数
- 占用端口 = `tether admin sessions` ↔ `port_allocations`（每条 expose 一行）

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

恢复：复制回原目录、保持属主 `tether:tether` mode 0750、`systemctl start` 即可。

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

`internal/proto/proto_version.go` 里的 `ProtoVersion` 是 wire 协议版本。bump 后
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

---

## 9. 错误码与故障排查

CLI 输出错误统一格式：`<verb> failed: <人话提示> (<架构稳定的 code>)`。本节列
出全部已知 code 及处置。

### 9.1 成员 / 所有权 / 生命周期

| code | 含义 | 处置 |
|---|---|---|
| `not_owner`                     | 仅 owner 可执行 | 让 owner 来做，或 owner 把所有权转交（v1 不支持，需 owner 重建 session） |
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
| `agent_malformed_resp`  | agent 回包格式错（多半 proto 不一致） | `tether node upgrade <nid>` 升级 |
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
| `frpc_failed`       | agent 起 tunnel 客户端失败 | 看 `~/.tether/agent/<sid>/agent.log` |
| `tunnel_token_unknown_or_revoked` | 反向隧道 token 失效 | 重新 `tether expose`（agent 重启后 state.json 损坏可能触发） |

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
4. 防火墙 / 安全组放行 443 + 7000 + 14000-14999。

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
1. owner 在原机：`tether admin evict <sid> <old-nid>`（或 ctl 端 v1 没有 evict
   入口，必须在 broker 本机）；
2. 新机重跑 `install.sh --role agent --nid <new-nid> --pin <pin>`；
3. owner 改成 `evict` 后旧 agent 进程会在 1s 内自杀。

**Q: 想批量 expose？**
A: v1 没有批量入口，写个 shell loop：
```bash
for nid in gpu-01 gpu-02 gpu-03; do
  tether expose $nid --local 8888 --name jupyter-$nid
done
```

**Q: 如何只授权别人看 ps 不能 exec？**
A: v1 不支持细粒度 RBAC。member 拿到 PIN 后即可 exec / run / expose。如需要
划分角色，开多个 session（每个 session 单独 PIN + member 名单）。owner-only
操作只有 `session rm` 与 `node upgrade`。

**Q: broker 能横向扩展吗？**
A: v1 单实例。NATS 本身能 cluster，但 tether 的 SQLite + JetStream 单写副本设计
没做多 broker 协调。需要 HA 走主备 + 共享 disk + 手动 failover。

**Q: 数据加密？**
A: 控制面 wss://（Caddy 终端 TLS）；nkey 签名鉴权。数据面（`expose` 反向 TCP）
明文 —— 暴露的服务自己负责 TLS（如 Jupyter 用 `--certfile`）。

**Q: 退出会话但保留 agent？**
A: ctl 端 `tether logout` 只清当前活动 session；agent 自己继续跑。要彻底
清成员关系：让 owner `tether admin evict`（v1 无 ctl 入口）。

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
   `tunnel-only` 模式（v1 无）。
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
nats sub 'tether.v1.>'

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
| 8222       | TCP             | 仅本机 | nats-server WebSocket（Caddy 反代） |
| 7000       | TCP             | 入站 broker | 反向隧道控制连接（agent 主动连） |
| 14000-14999| TCP             | 入站 broker | `expose` 公开端口 |

防火墙最少放行：`443, 7000, 14000-14999/tcp`（入方向）。

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
