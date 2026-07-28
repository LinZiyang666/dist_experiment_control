# tether broker 运维手册

> tether 的操作文档按角色拆成三册。**本册面向 broker 运维**——在公网服务器上部署、配置、
> 维护、升级 broker（`tether serve`），需要 sudo / 域名 / TLS。使用者侧（`ctl`/`agent`）的
> 安装与命令见 [`docs/usage.md`](usage.md)；多 broker 高可用见 [`docs/cluster.md`](cluster.md)
> 与运维剧本 [`docs/cluster-runbook.md`](cluster-runbook.md)。架构设计见
> [`docs/architecture.md`](architecture.md)。
>
> **章节号沿用完整手册的原始编号**（三册共用一套编号，便于跨册引用）。

## 30 秒心智模型（完整版见 [`usage.md`](usage.md) §1）

- **三角色一个二进制**：`broker`（`tether serve`，公网 1 台，分布式 HA 时每节点 1 个）、
  `agent`（实验机，NAT 后，N 台）、`ctl`（使用者笔记本，M 台）。
- **控制面** = NATS（命令 / 注册 / 心跳 / 审计）：ctl / agent 走 `wss://<broker>:443`，
  broker 走本机 `nats://127.0.0.1:4222`。**数据面** = 内置 yamux-over-TCP 反向隧道；
  `tether expose` 把 agent 的本地端口反向打到 broker 的 `14000-14999` 公网带。
- **身份** = 每个用户 / agent 一对 Ed25519 nkey，加入 session 出示一次性 PIN，broker 把
  公钥绑定到 `(sid, role)`，之后所有 CONNECT 用 nkey 签名鉴权（`auth_callout`）。
- **单机 broker 是默认且完整支持的部署**；`cluster` / `alert` / quorum 只在多机 HA 时才用到，
  见 [`cluster.md`](cluster.md)。

---

## 目录

2. [安装与部署（broker）](#2-安装与部署broker)——2.3 broker、2.4 install.sh flag 全集
3. [配置文件（broker）](#3-配置文件broker)——3.3 `/etc/tether/broker.yaml`、3.4 nats-server.conf 启用 auth_callout
5. [命令参考（broker）](#5-命令参考broker)——5.5 `tether serve`、5.20 `tether admin *`
7. [运维 / 维护](#7-运维--维护)——7.1 健康检查、7.2 disk pressure 告警、7.3 admin socket 权限、7.4 备份、7.5 日志轮转、7.6 监控 tether 自身（**§7.7 网络盘挂死自救属使用者侧，见 [`usage.md`](usage.md) §7.7**）
8. [升级（broker）](#8-升级broker)——8.4 broker 升级、8.5 升级到 v0.3.0 的代理订阅配置迁移
9. [错误码与故障排查（broker 侧）](#9-错误码与故障排查broker-侧)——9.7 NATS 连接失败、9.10 admin connection refused
- [附录 A：broker 主机目录结构](#附录-abroker-主机目录结构)、[附录 B：网络端口](#附录-b网络端口)

> 使用者侧命令 / 场景 / FAQ 见 [`usage.md`](usage.md)；集群命令与 quorum 见 [`cluster.md`](cluster.md)。

---

## 2. 安装与部署（broker）

### 2.3 broker（运维侧，需 sudo + 域名 + ACME 邮箱）

> 下面这套**单机 broker 就是生产推荐的默认部署**，不需要任何 cluster 步骤。要做多机 HA 才继续看
> 本节末尾的"分布式 HA 安装边界"以及 cluster.md §5.6 / `docs/cluster-runbook.md`。

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

---

## 3. 配置文件（broker）

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
  observability:                       # 可选；缺省 = info/text/无 metrics/无 webhook
    # metrics_listen: 127.0.0.1:9090   # Prometheus /metrics + /healthz + /readyz（空 = 关）
    # alert_webhook_url: https://...   # 每次 committed 告警 raise/clear POST（仅集群，空 = 关）
    # disk_check_interval: 5m          # 磁盘监控采样间隔（#39；默认 5m；flag --disk-check-interval 优先）
    # --- broker.cluster.* 的 xfer 回收节奏（仅 cluster 模式；生产通常全部留空用内建默认）---
    # xfer_reap_interval: 5m           # #58/P10：home 权威的 orphan tier-B 对象回收周期（默认 5m；<1s 或 >24h 在 Load 期拒绝）
    # xfer_cross_home_reap_age: 15m    # R16 #58：LEADER 跨-home GC 的年龄下限（**只能调高，不能调低**；
    #                                   # 外审 F2：低于 tier-B 看门狗的下限会让 leader 删掉另一 home 上
    #                                   # 仍在用的对象——leader 看不见别人的 tracker）。回收「没有任何 home 能回收」的
    #                                  # split-home / 零节点会话 bucket。默认派生自 3×tier-B 超时(=15m)——比 per-home
    #                                  # grace 长，护住另一 home 上仍在飞的传输（跨节点时钟偏斜留余量）。**没有生产调参
    #                                  # 场景**，暴露它只为 deploy-tier drill 压缩排程；<1s 或 >24h 在 Load 期拒绝。
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

> **注意（#22）**：nats-server.service 加载的是 `/etc/tether/nats.d/nats.conf`（G1 起 install.sh 写这份、
> unit `ExecStart -c` 指这份），**不是** `/etc/tether/nats.conf`。authorization 块必须写进 nats.d/ 那份 ——
> 写错到 `/etc/tether/nats.conf` 会得到一个 nats-server 永不加载的孤儿文件（`nats-server -t` 仍会校验通过、
> 看似完成），重启后 auth_callout 静默未生效。`sudo tee` 以 root 跑，能写进 tether-owned 的 `nats.d/`。

⚠️ **nkey 用户不能带 `user:` 或 `password:` 字段**（写错会得
`Nkey users do not take usernames or passwords`，nats-server 拒绝启动）。
`auth_callout.auth_users` 必须用 nkey 公钥本身做身份引用，不是 alias 字符串。
把下面两处 `$BROKER_PUB` 与一处 `$ACCOUNT_PUB` 替换成第 1 步打印的实际值：

```bash
sudo tee /etc/tether/nats.d/nats.conf > /dev/null <<EOF
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

写完先 `nats-server -c /etc/tether/nats.d/nats.conf -t` dry-run 一下，输出
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

## 5. 命令参考（broker）

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

**环境变量**：
- `TETHER_VERSION` — 等价 `--version`（见 §5 其它命令注）。
- `TETHER_AUTO_REBALANCE`（G7 #18，默认关）——设为 `on` 时,leader 在某 broker **回归**后主动把 `__proxy__` homes 迁回、均摊到 eligible voter（受 cooldown + 静默期门控）。**默认关**是有意的:rehome 是 break-before-make,已缓存旧 `/sub` 的客户端会短暂 black-hole 到 refetch,而客户端刷新周期未验证——故除非你确认车队客户端能及时刷订阅,否则保持关。手动预览/触发用 `tether cluster rebalance proxy --dry-run`（见 [`cluster.md`](cluster.md) §5.6.11）。写进 `tether-broker.service` 的 `Environment=` 即持久生效。

cluster 模式由 `--cluster-data-dir` 下的 raft state 触发，不是单靠 yaml 字段触发。
启动前必须先跑 `tether cluster init --from-existing`；完整步骤见
`docs/cluster-runbook.md` 第 4 节。集群内部还需要私网 `:7400` Raft 和 `:6222`
NATS route；不要暴露到公网。

启动后处理 `SIGINT/SIGTERM` 优雅退出（tunnel 等所有子组件按 ctx 拆解）。

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
tether admin events [-n 50] [--since 1h] [--kind proxy_keyset_changed] [--json]  # sys.events 运维事件流
tether admin evict <sid> <nid>              # 强制踢
tether admin runtime [--json]               # 本进程 runtime 自省（goroutines/threads/fds/rss/uptime + reconciler last-tick）
```

各子命令含义：

| 子命令 | 是什么 | 何时用 |
|---|---|---|
| `admin sessions` | 列出 broker SQLite 里**全部** session（不区分 owner / member），含 ACTIVE / DELETING 状态、owner 指纹、创建时间。 | 排查"用户说看不见 session"、确认 tombstone 是否完成、容量盘点。 |
| `admin nodes` | 列出**全部**已注册 agent，含 sid、nid、当前状态（ONLINE/OFFLINE/STALE）、心跳年龄、proto 版本、release 版本。 | 排查 OFFLINE 节点、版本审计、找到要 evict 的目标。 |
| `admin audit <sid>` | 直接从 JetStream `history-<sid>` stream 读最近 N 条审计 entry（不走 ctl 的 NATS path）。 | NATS 鉴权坏了 / owner 失联但需要查历史 / 合规导出。 |
| `admin events` | 读 broker 的 H.1 `events` 流（**运维契约事件流** sys.events）：`session_created`/`session_destroyed`、`member_joined`、`agent_registered`/`agent_evicted`、`agent_roster_stale`、`disk_pressure`、`tetherd_restarted`，以及拓扑/迁移类 `proxy_*`（含 `proxy_keyset_changed`）/`nats_topology_*`/`home_reassign_*`/`grow_cutover_*`。**member 能 subscribe sys.events，但 operator（broker 主机 root、无 NATS 成员凭据）此前没有任何读命令**——这个动词就是那个 reader，走同一 root-only 0600 admin socket。读的是**持久化流**（保留 30d/1GiB），故 `--since` 能翻历史、`--kind` 能只看某类。载荷**永不含 secret**（生产者手搭 allow-list 标量 map）。 | "集群刚发了哪些拓扑/告警/迁移事件"、排查 rehome/proxy 变更、disk_pressure 复盘、离线取证。 |
| `admin evict <sid> <nid>` | 强制把指定 (sid, nid) 从 broker 移除：删 `agent_provisioning` 行 + `nodes` 行 + 广播 `sys.events{type:agent_evicted}`。 | agent 失控 / 机器换主 / nkey 泄漏要立即吊销。 |
| `admin runtime` | 读**活着的 broker 进程自身**的 runtime：`goroutines`（`runtime.NumGoroutine()` 进程内真值）、`threads`（threadcreate profile：**累计**创建过的 OS 线程/M 数，单调不回落——是高水位而非当前存活数）、`open_fds`、`rss`、`uptime`，以及每个周期 reconciler 的 `last_tick`。**只读本进程 + R7 注册表**，不碰 DB / raft / leadership，单机与集群、leader 与 follower 都能答。 | 诊断**活 broker 上的 goroutine / fd 泄漏**（隔一段时间连采两次、看计数是否单调爬升）；诊断**卡死的 reconciler**（某 pass 的 `last_tick` 不再前进）。现网车队出过泄漏/崩溃事故、而此前生产二进制**零自省面**——这个动词就是补它。 |

> **为什么用 `goroutines` 不用 `/proc/<pid>/status` 的 `Threads`**：`Threads` 是 OS 线程数（M），10k 个泄漏的 goroutine 若都 park 住，**线程数纹丝不动**——拿 `Threads` 当 goroutine 代理会在进程漏 goroutine 时报一个平坦的假计数。两者从各自来源分别上报，正是为了让运维看到它们背离。
>
> **为什么不引入 pprof**：`admin runtime` 返回计数即足以定位泄漏（连采看爬升），且已被 root-only 的 0600 admin socket 网住。常驻 `net/http/pprof` 端点会额外引入攻击面（heap / goroutine-stack dump + CPU-profile DoS，暴露远超计数）与体积；深度栈级取证是**罕见、刻意、离线**的动作，不该做成常驻 HTTP 面。故：走 admin socket 返回计数，不上 pprof。
>
> **`--json`**：稳定 schema（`schema:"admin_runtime"` + `schema_version:2`）供监控采集。
> **v1→v2 是破坏性变更**（batch-B 余项 / B7）：`reconcilers[]` **删除了 `leader_only` 布尔**，
> 由 `authority`（`"any"` / `"leader"`）取代——bool 只能回答"是不是 leader"，而 registry 现在同时承载
> per-broker、leader-gated 与将来的其它门，第二种门只能以第二个 bool 混进来。
> **按 usage.md 的 bump 政策，删键/改语义必须 bump**，所以采集端按 `leader_only` 写的规则会看到一个
> 它不认识的 `schema_version`，而不是静默匹配不到任何东西。
> 同批新增字段单独不构成 bump 理由（**外审 B2-7 订正**：原文写"都是 omitempty"，不准确——
> `cluster_loops[].last_iter` 与 `.iterations` **刻意不是 omitempty**，理由见
> `internal/adminsock/protocol.go` 的 `ClusterLoopInfo`：**零就是"死"这个含义本身**，
> 一个 periodic 循环报 `iterations: 0` 表示它连一次循环体都没跑完，
> 把键在零时丢掉恰恰会在坏消息发生时删掉信号。不 bump 的真实理由是**只加键、不改已有键的含义**）：
> `reconcilers[]` 加 `last_dur_ms`/`max_dur_ms`/`overruns`（omitempty；pass 耗时与超支——"op 驱动器卡住"此前完全不可观测），
> `cluster_loops[]` 加 `cadence_ms`（omitempty）与 `last_iter`/`iterations`（**非** omitempty，同上）：
> 每次迭代的活性；`cadence_ms == 0` 表示事件驱动，此时 `iterations == 0` 不代表死。
> **`iterations` 是活性、不是投递成功**（外审 B2-4）——被端点拒掉的 POST 仍是一次完成的迭代；
> 投递结果在单独的 `alert_webhook.{accepted,rejected,drops}` 里，
> 它是 omitempty 指针（不在场 = 没配 webhook），块内三个计数器则不是 omitempty（零有意义）。成功时**只有** JSON 落 stdout，失败只落 stderr + 非零退出——`admin runtime --json 2>&1 | jq` 不会被散文污染。

> **`admin events` 为什么走 admin socket 而不是 NATS sub**：sys.events 的读者有两类——**member** 用 NATS 订阅（数据面成员信任层）、**operator** 用 admin socket（root-only 0600，与 `admin runtime`/`alert raise` 同层）。operator 在 broker 主机上通常没有 NATS 成员凭据，admin socket 才是它的信任面；且 `events` 是**持久化流**，admin socket 读它能给出**历史**（`--since` 翻过去、`--kind` 过滤），裸 NATS sub 只能拿到**订阅之后**的 live 事件。
>
> **为什么没有 `--follow`**：admin socket 刻意是"一次请求一次响应"的非流式协议（见 `internal/adminsock`），而 operator 的需求是**时点读**"集群刚发了哪些事件"。要 live tail 就 poll：
> ```bash
> while :; do sudo -u tether tether admin events --since 5s --json | jq -c '.events[]'; sleep 5; done
> ```
> 真要连续订阅可用一个 member 身份 `nats sub tether.v2.sys.events`（但那是 member 面、非 operator 面）。
>
> **`--json`**：稳定 schema（`schema:"admin_events"` + `schema_version:1`）；成功只落 JSON、失败只落 stderr——`admin events --json 2>&1 | jq` 干净。**载荷无 secret**：所有 sys.events 由生产者手搭 allow-list 标量 map（`sid`/`nid`/`type`/`ready`/`capable` 等），从不夹带 PSK/token；reader 原样转发、不新增字段。

子命令参数：

| 命令 | 参数 / flag | 默认 | 说明 |
|---|---|---|---|
| `admin audit` | `<sid>` | (必填) | 要读取审计流的 session id |
| `admin audit` | `--n, -n` | `50` | 读取最近 N 条 |
| `admin events` | `--n, -n` | `50` | 返回最近 N 条（过滤后）事件 |
| `admin events` | `--since` | `0`（无时间界） | 只看 now 往前该时长内的事件（如 `1h`/`30m`） |
| `admin events` | `--kind` | `""`（全部） | 只看该 type 的事件（如 `proxy_keyset_changed`/`disk_pressure`/`nats_topology_reload`） |
| `admin events` | `--json` | `false` | 输出稳定机器 JSON（默认人类表格） |
| `admin evict` | `<sid>` | (必填) | 目标 session id |
| `admin evict` | `<nid>` | (必填) | 要强制踢出的 agent node id |
| `admin runtime` | `--json` | `false` | 输出稳定机器 JSON（默认人类表格） |

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

**采样间隔（可配，#39）**：磁盘监控**默认每 5 min** 采样一次 `--store-dir` 占用。
运维可用 `broker.observability.disk_check_interval`（Go duration，如 `5m`/`30s`/`1h`）
或等价 flag `--disk-check-interval` 覆盖，优先级 **flag > broker.yaml > 内建默认（5m）**。
空/`0` = 保持 5m 默认。低于 `1s` 的值在启动时被拒（子秒采样纯属 statfs 空转）。
调短用于填盘演练或需要更快 `disk_pressure` 反应的场景；调长用于极大盘 / 极慢存储降开销。

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

**进程 runtime 自省（goroutine / fd 泄漏 · 卡死 reconciler）**：`tether admin runtime`
（详见 §5.20）读活 broker 进程自身的 `goroutines`（`runtime.NumGoroutine()` 真值）、
`threads`、`open_fds`、`rss`、`uptime` + 每个 reconciler 的 `last_tick`。诊断法：
- **goroutine / fd 泄漏**：隔几分钟连采两次 `admin runtime --json`，`goroutines` 或
  `open_fds` **单调爬升**即泄漏迹象（现网车队出过此类事故；用 `NumGoroutine` 真值而非
  `Threads` 代理——park 住的泄漏 goroutine 不涨线程数）。
  ```bash
  sudo -u tether tether admin runtime --json | jq '{goroutines, open_fds, rss_bytes}'
  ```
- **卡死 reconciler**：某个 pass 的 `last_tick` 停在过去不再前进 = 该周期对账停摆。
  ```bash
  sudo -u tether tether admin runtime --json | jq '.reconcilers[] | {name, last_tick, runs, last_err}'
  ```

> 网络文件系统（NFS/CIFS/…）挂死时 run/exec 卡住的**使用者侧**自救（`agent.yaml remote_fs`、
> `tether exec/run --safe`、`remote_fs_*` 错误码）见 [`usage.md`](usage.md) §7.7。

---

## 8. 升级（broker）

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
不会自动出现——必须手动补两处配置；如果同时从 proto v1 升到 proto v2，仍按 usage.md §8.3
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

### 8.6 G1 部署面加固迁移（#22 nats.conf 迁址 / #23 Restart drop-in / #6 offline-op 属主 / #24 证书 SAN）

G1（本批部署面加固）起，几处部署面缺陷已在 `install.sh` / 二进制层修复。**新装机自动生效、无需本节**；
本节只给**已装的现网 broker** 的一次性迁移。四条互相独立，可分别做。

**#22 — reconciler 的 nats.conf 迁到 tether-owned 子目录**。in-broker 拓扑 reconciler 以 `User=tether`
跑、要原子重写 nats.conf，但旧版把它放在 root-owned 的 `/etc/tether/`（`os.CreateTemp`+rename 需目录可写）
→ 成员变更后拓扑永不自动收敛（`cluster status` 卡 DEGRADED，即使 raft/JS/routes 全 HA）。G1 把 conf 迁到
tether-owned 的 `/etc/tether/nats.d/`；`/etc/tether` 本身**保持 root-owned**——root 跑的 caddy 读
`/etc/tether/Caddyfile`，若 `/etc/tether` 归 tether 会成 tether→root 本地提权面。**现网迁移（clustered
成员，逐台，务必按序）**：
```
sudo install -d -o tether -g tether -m 0750 /etc/tether/nats.d
sudo mv /etc/tether/nats.conf /etc/tether/nats.d/nats.conf
sudo sed -i 's#-c /etc/tether/nats.conf#-c /etc/tether/nats.d/nats.conf#' /etc/systemd/system/nats-server.service
# broker.yaml 的 broker.cluster 块必须显式含 nats_conf_path 指向新路径。真实 `cluster init` 历史上不写
# 这行（只写 data_dir/raft_addr/secrets_dir；G1 起 init 的 step 3 会提示补它），故现网多数 clustered
# broker 没有它、靠代码默认——而升级二进制会把默认翻到 nats.d/，所以升级前务必补上（与 data_dir 同级、
# 缩进 4 空格）：
#     nats_conf_path: /etc/tether/nats.d/nats.conf
# 已有该行的机器改用 sed：s#nats_conf_path:.*#nats_conf_path: /etc/tether/nats.d/nats.conf#
sudo systemctl daemon-reload
# nats-server 重启会断经 broker 的 ctl 通道 → 用 detached：
sudo systemd-run --collect systemctl restart nats-server
```
**然后再升级 tether 二进制**（默认路径同步迁到 `nats.d/`）。**关键顺序**：conf 迁址 + broker.yaml 补
`nats_conf_path` 必须在升级二进制**之前**完成——二进制升级把代码默认从旧路径翻到 `nats.d/`，若届时 conf
还在旧址且 broker.yaml 无显式行，reconciler 会指向不存在的路径。
**⛔ 切勿在 clustered 成员上裸重跑 `install.sh`**：它**无条件覆盖** `broker.yaml`（抹掉整个
`broker.cluster.{data_dir,raft_addr,secrets_dir,nats_conf_path}` 块 → 该成员重启退回**单机模式**、脱离集群，
比 nats.conf 被覆盖更严重的 R3 静默去集群化）、覆盖 `Caddyfile`、并用 standalone 模板覆盖 clustered
nats.conf，且不 daemon-reload。

**#23 — broker 丢 nats 连接后不再永久停摆（unit drop-in）**。旧版 `tether-broker.service` 是
`Restart=on-failure`，而 broker 在某些 nats-loss 路径上 clean-exit(0)（`serve.go` 把 `context.Canceled` 当
exit 0）→ 不被拉起、停死（inactive 而非 failed）。G1 改 `Restart=always`。**现网用 drop-in（不重跑
install.sh、不重启在跑的 broker）**：
```
sudo systemctl edit tether-broker    # 加三行：[Service] / Restart=always / RestartSec=2
sudo systemctl daemon-reload         # 下次退出即生效；无需重启 broker
```
drop-in 跨 `install.sh` 重跑存活，且不碰 nats.conf。`Restart=always` **不会**拉起 `systemctl stop` 主动停
的服务（systemd 知道是自己停的），故不影响运维 stop/restart；默认 StartLimit 仍拦真崩溃循环。

**#6 — offline op 必须以 data_dir 属主（tether）跑**。`cluster init` / `force-single` / `recover` /
`resnapshot` / `restore` 以 **root** 跑，会把 `tether.lock` / `raft/` / `tether.db` 建成 root-owned，之后
`sudo -u tether` 的 offline op 开不了（flock EACCES，或 raft.db bolt 探测硬报错）。**一律
`sudo -u tether tether cluster …`**（G1 的 offline CLI 会在 root 跑对 tether-owned dir 时 WARN 提示）。已被
root-init 污染的机器：停 daemon → `sudo chown -R tether:tether /var/lib/tether`（整树，因整树都被污染）→ 再跑。

**#24 — route/tunnel 证书须带 SAN**：见 [`cluster-runbook.md`](cluster-runbook.md) 的 route-cert 铸证段
（nats route mesh 走标准 x509、需 `subjectAltName` 匹配 route-URL host；tether 自己的 raft transport 不需要，
勿被 `internal/cluster/transport.go` 的注释误导）。

---

## 9. 错误码与故障排查（broker 侧）

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

### 9.10 admin 命令 connection refused

```
admin: dial /var/run/tether/admin.sock: connect: permission denied
```

- 你不在 broker 主机上 ⇒ `tether admin` 设计上只在 broker 本机用；
- 用户没读 socket 权限 ⇒ `sudo -u tether tether admin ...` 或 `sudo`；
- broker 没起 / `--admin-socket=` 被设空 ⇒ 检查 broker.yaml `broker.admin.socket`。

---

## 附录 A：broker 主机目录结构
```
/etc/tether/                    # root-owned（root 跑的 caddy 读这里的 Caddyfile）
├── broker.yaml
├── Caddyfile
├── nats.d/                     # tether-owned 0750（#22）：reconciler 管理的 nats.conf 放这里
│   └── nats.conf               #   （cluster 模式；nats-server -c 指向此）
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

