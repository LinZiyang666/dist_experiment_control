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
- `/usr/local/bin/{tether, nats-server, caddy}`（nats-server v2.14.6, caddy v2.7.6，
  逐个 sha256 校验后落地）
  - **存量主机会被 retrofit**：install.sh 重跑时会比对 `nats-server --version`，
    版本不符就**替换二进制并要求你重启** `nats-server`（它此前是「已存在即跳过」，
    于是任何服务端侧修复都到不了现网——现网因此在一个版本上停了整整一个 release）。
    替换用 rename 而非原地截断，所以对正在运行的进程是安全的；新二进制在下次重启才生效。
  - **为什么 pin 必须保持在 v2.14.6**：公网 WebSocket 面不能发布已知受影响且已退出上游支持窗口的
    服务端。此前 drill `50-backup-restore` / `95-broker-selfheal` 在升级后暴露的根因不是新版 NATS，
    而是 broker 启动期只探测 JetStream 一次、瞬态未就绪就进入 systemd crash loop。该路径现已改成
    有界等待，并在 v2.14.6 上复跑两条 drill 为 GREEN；不得用降级服务端替代 boot wait 修复。
- `/etc/tether/{broker.yaml, Caddyfile}`
- `/etc/systemd/system/{nats-server, tether-broker, caddy}.service`
  （#76:默认已 `systemctl enable`——仅建开机 symlink,**不启动**;`--no-enable` 退出）
- `/etc/systemd/journald.conf.d/60-tether.conf`
  (#77:journald `SystemMaxUse` 按盘三档 <10G→200M / <40G→500M / ≥40G→1024M;
  任何位置已有显式 `SystemMaxUse=` 则不写;`systemctl restart systemd-journald` 或重启后生效)
- `/var/{lib,log,run}/tether`（属主 `tether` 系统用户，`install.sh` 自动 useradd）

> **分布式 HA（proto v2）安装边界**：升级成集群时，tether 接管 `nats.conf`（`tether cluster
> reconcile nats --all --wait`，见 §3.4）并留 `nats.conf.bak.<ts>`；cluster secrets（`/etc/tether/secrets/`：
> `cluster-ca.pem`、`route-cert.pem`/`route-key.pem`、稳定 `tunnel-cert.pem`/`tunnel-key.pem`、
> `broker.nk`、`node-ident.nk`、`account.nk`，私钥 0600）须先 provision（`tether cluster doctor`
> 预检：缺/不可读/私钥权限松 = 拒；FDE 缺 = advisory）。现网单点→N≥3 的一次性迁移全流程 +
> 回滚见 **`docs/cluster-runbook.md` 第 4 节**。proto v2 不与现网 v1 车队 wire 兼容（需协调全车队重装）。

启动（units 已由 install.sh enable,只差 start）：

```bash
sudo systemctl start nats-server tether-broker caddy
sudo systemctl status nats-server tether-broker caddy
```

> **存量装机 retrofit(#76/#77)**:老版 install.sh 装的 broker(如 2026-08 前的现网)没有上面两项默认。
> 重跑 install.sh 本身是安全的(自 2026-09-02 起它保留既有 broker.yaml/Caddyfile,新内容写成 `<file>.new`),
> 但**不要**带 `--force-config`——那会覆写这两个文件。手动补齐更直接:
>
> ```bash
> sudo systemctl enable nats-server tether-broker caddy
> # review F8: create the drop-in DIR first — on an old/minimal install it may not exist,
> # and the `tee` would fail. The marker's first line matches install.sh's ownership check
> # so a later `--uninstall` will clean this file up (F3).
> sudo install -d -m 0755 /etc/systemd/journald.conf.d
> printf '# managed-by: tether-install.sh (#77 journald cap)\n[Journal]\nSystemMaxUse=500M\n' \
>   | sudo tee /etc/systemd/journald.conf.d/60-tether.conf
> sudo systemctl restart systemd-journald
> # verify the cap actually took (read-only). NOTE (review F8): do NOT use
> # `systemctl show systemd-journald -p SystemMaxUse` — journald does not export
> # that property, so it prints an empty value and exits 0, proving nothing.
> # Read the MERGED config instead and confirm your drop-in is the LAST effective
> # SystemMaxUse (drop-ins override /etc/systemd/journald.conf; last one wins):
> systemd-analyze cat-config systemd/journald.conf | grep -nE '^\s*SystemMaxUse=|^# /'
> # the final uncommented `SystemMaxUse=` printed must be the drop-in's `=500M`,
> # and the `# /…/60-tether.conf` source header must appear after any base value.
> ```

`broker.upgrade.url_allow` 的内置默认值已经包含本仓库的 GitHub release 前缀，
开箱即可 `tether node upgrade`；要自托管镜像才需要改 `broker.yaml` 或
`--upgrade-url-allow` 覆盖。auth_callout 启用见 §3.4。

> **⚠ 全新 broker 还差最后一步：准入第一个使用者。**
>
> `session create` 需要调用者的指纹在准入表里，而**全新 broker 的准入表是
> 空的**——不做这一步，第一个用户会被拒（退出码 77，`not_allowed`），而
> broker 本身一切正常。
>
> ```bash
> # 使用者在自己的机器上（离线，不需要连 broker）：
> tether whoami
> #   fingerprint: SHA256:…
> #   public key:  U…
>
> # 你在 broker 主机上：
> sudo tether admin session-allow SHA256:…
> sudo tether admin session-allow --list        # 确认
> ```
>
> 使用者不跑 `whoami` 也行：他直接 `tether session create` 被拒时，那条
> 拒绝文案里就带着他自己的指纹和这条命令。详见 §5.20。
>
> **从旧版升级的存量 broker 不需要做这一步**：broker 启动时会一次性把每个
> 已经拥有 session 的 owner 指纹自动纳入准入表（经 raft，只做一次，所以后
> 续升级不会复活你撤销过的指纹）。

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
| `--no-enable`               | broker 用:不 `systemctl enable`(默认 enable,仅 symlink 不启动;#76) |
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
    # ⚠ xfer_reap_interval / xfer_cross_home_reap_age 属于 broker.cluster.*，不在此段——见下方 cluster: 块
  upgrade:
    url_allow:                         # `tether node upgrade` 白名单（可选）
      - https://github.com/LinZiyang666/dist_experiment_control/releases/
      - https://releases.example.com/tether/   # 追加自托管镜像（可选）
  cluster:                             # 分布式 HA 时填写；空/无 raft state = 单机模式
    data_dir: /var/lib/tether          # holds raft/ + tether.db
    raft_addr: 10.0.0.1:7400           # 私网 Raft transport
    secrets_dir: /etc/tether/secrets   # cluster-ca/route/tunnel/node-ident/broker/account seeds
    # xfer 回收节奏（仅 cluster 模式；生产通常全部留空用内建默认；#75 起就地取消注释即生效，键位正确）
    # xfer_reap_interval: 5m           # #58/P10：home 权威的 orphan tier-B 对象回收周期（默认 5m；<1s 或 >24h 在 Load 期拒绝）
    # xfer_cross_home_reap_age: 15m    # R16 #58：LEADER 跨-home GC 的年龄下限（**只能调高，不能调低**；外审 F2：
    #                                  # 低于 tier-B 看门狗下限会让 leader 删掉另一 home 仍在用的对象——leader 看不见别人的
    #                                  # tracker）。默认派生自 3×tier-B 超时(=15m)。**没有生产调参场景**，只为 drill 压缩排程。
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
tether admin session-allow <fp>             # 准入一个指纹，使其可以 session create
tether admin session-allow --list           # 看当前准入表
tether admin session-allow --remove <fp>    # 撤销（不删该指纹已有的 session）
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
| `admin session-allow` | **谁可以 `session create`** 的准入表。`<fp>` 准入、`--remove` 撤销、`--list` 查看。指纹形如 `SHA256:…`，由使用者的 `tether whoami` 给出——他们跑 `session create` 被拒时那条文案里也有。**撤销不会删除该指纹已有的 session**（撤销「能建」不等于删除别人的工作）。重复准入是幂等的，且**不会**改写原有的 `--note`（会明说）。集群模式下这条写入走 raft，所以在任一 broker 上执行都对全体生效；混版集群里若有 voter 的二进制还不认识这条 op，命令会**拒绝**而不是写出一份只在部分副本生效的策略。 | **全新 broker 的第一步**：装完之后准入表是空的，第一个使用者必须在这里放行一次，否则他 `session create` 会被拒（退出码 77）。升级到本版的**存量部署不需要任何动作**：broker 启动时会一次性把每个已拥有 session 的 owner 指纹自动纳入。此外用于吊销：某人离职 / 某台机器的 nkey 泄漏时，撤销它继续开新 session 的能力。 |

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

### 7.5 日志轮转（h1 F 起：进程内封顶，不再依赖 logrotate）

**现状（h1 及以后）**：broker 的 slog 输出走**自己的**大小封顶轮转文件，由
`broker.yaml` 的 `observability:` 块配置，默认 50MB × 2 个备份（最坏 150MB）：

```yaml
broker:
  observability:
    log_file: /var/log/tether/broker.log
    log_max_size_mb: 50
    log_max_backups: 2
```

systemd unit 改成 `StandardOutput=journal` / `StandardError=journal`——**stdout/stderr
只承载 panic、stacktrace 和 logger 建起来之前的启动输出**，而 journald 自身是有配额的。
agent 侧同理：默认写 `<home>/agent/<sid>/agent.log`（同样 50MB × 2），并把自己的 fd 2
指到 `agent.boot.err`。

**为什么改**：`append:` 这个 sink 没有任何上限，而宿主不一定装了 logrotate——
2026-08-04 事故里 `/var/log/tether/broker.err` 在 19GB 的盘上长到 **5.3GB**
（d5 循环每 100ms 一条同样的 WARN，刷了五周）。上限必须在**进程里**，因为那是唯一
无论怎么启动都跟着走的东西。

**不再需要 logrotate 配置**。若你已经装了针对 `/var/log/tether/*.log` 的 logrotate
规则，删掉它：两套轮转互相打架（外部 rename 会让进程内的大小计数与磁盘脱节）。

**sink 生效自证(#75)**:file sink 打开后 broker 往 stderr(→journal)打一行
`tether: log sink /var/log/tether/broker.log (cap 50MB x 2 backups)`——`journalctl -u
tether-broker` 里看到它即证明 observability 段真的生效;若看到的是 slog 业务日志本体,
说明配置没接上(#75 修复后错嵌套/typo 会直接**拒启**并报键名行号,不会再静默忽略)。

**隧道 REGISTER 读失败 WARN 的降频语义(#78,内审 M1/M2/Mi5 后的最终形态)**:
`tunnel server: read REGISTER` 这条 WARN(唯一未鉴权、互联网可触发的日志站点)现在**按错误类
(eof/timeout/other)各自独立降频**——两个异类故障源并存(比如一个半开 agent + 一个探测器)
不会互相把对方"顶"成每事件一条。每类:风暴只打**第一条**(带 `class`、`remote` 主机与
`suppressed_since_last` 计数),之后静默,**每约 5 分钟重申一条**("仍在发生"的确认——安静
broker 上跨周的同类独立事件也因此不会被永久静音);直到**下一次已鉴权的 REGISTER**(过 token
校验的真 agent——垃圾连接/TLS 探测器触发不了)打 `REGISTER reads recovered class=… suppressed=N`
把账补上。需要逐条取证时把 log level 开到 debug(被抑制的重复以 Debug 级保留)。
**⚠ `remote` 字段语义(外审疑惑2)**:每个 class 是**全进程一个** damper(不是 per-remote,内存有界),
所以 WARN 里的 `remote` 只代表**恰好触发这条(首条或重申)的那个来源**;同 class 的其他来源被全局吞掉、
不各自留 `remote`。**不要**把 `remote` 当 per-source 归因——它只说"这一类里至少有它在拨",要精确定位所有
来源需 debug 级或 netfilter/`ss` 侧观测。

**从 h1 之前的部署迁移（必须按顺序）**，见 §8.8。

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

> **车队滚动顺序**（requirements §6.7 N-1 窗口）：**broker 先升，agent/ctl 后升**；回滚顺序相反。
> 混合 release 车队在一个窗口（相邻两个 release）内是受支持状态。

### 8.4 broker 升级

> **#75 严格化升级注意**:自 #75 修复版起,broker 对 `broker.yaml` **严格解析**——未知/错嵌套的键
> 会**拒启**并在错误里报键名与行号(此前是静默忽略)。手工编辑过 broker.yaml 的存量机(比如手抄过
> `observability:` 段),flip 前用新二进制跑一次 **`tether serve --config /etc/tether/broker.yaml --config-check`**
> ——它只做严格解析 + 全部校验器就退出(exit 0=配置 OK),**不开库、不跑 migration、不起监听**,
> 因此可以安全地用新二进制对活库所在主机验证而不污染回退路径。**切勿用不带 `--config-check` 的
> `tether serve` 做验证**:那会 `storage.Open` 并对活的 `tether.db` 应用新二进制的全部 pending
> migrations,即便随后 Ctrl-C,"决定不 flip"的回退也已被污染。拒启的修法:按报错把键放回正确层级
> (对照 install.sh 模板)。

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

⛔ **不要为了省事直接重跑 `install.sh` 来改配置。** 自 2026-09-02 发布前审计（DRD-F1）起，
重跑**不再覆盖**已存在的 `broker.yaml` / `Caddyfile` / `nats.d/nats.conf` / 各 `*.service`：
它会保留原文件、把新内容写成 `<file>.new`，并在结尾横幅里逐条列出，供你 `diff` 后自行决定。
需要按新模板整体覆盖时，显式加 `--force-config`。

这条改动的由来值得记住：此前裸重跑会**无条件重写** `broker.yaml`，把 `broker.cluster` 整块抹掉，
该 broker 重启后退回单机模式、静默脱离集群——机器看起来完全健康。

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
**⛔ 切勿在任何"已按 §3.4 启用 auth_callout 或已入集群"的 broker 上裸重跑 `install.sh --force-config`**：
那会用 standalone 模板覆盖 `broker.yaml`（抹掉整个
`broker.cluster.{data_dir,raft_addr,secrets_dir,nats_conf_path}` 块 → 该成员重启退回**单机模式**、脱离集群，
比 nats.conf 被覆盖更严重的 R3 静默去集群化）、覆盖 `Caddyfile`、并用 standalone 模板覆盖 clustered
nats.conf，且不 daemon-reload。

不带 `--force-config` 的重跑现在是安全的（DRD-F1）：既有文件被保留，新模板落到 `<file>.new`。
把适用范围从"clustered 成员"放宽到上面这句，是因为 auth_callout 的手工配置同样只存在于被覆盖的那份 conf 里。

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

### 8.7 agent 升级安全（upgrade-safety 增量）— 运维须知

机制全图（冒烟门 / prev 槽 / marker / 自动回退）见 `distributed-broker-architecture.md §21.3`，
使用者视角见 `usage.md §5.19`。运维需要知道的硬事实：

- **磁盘足迹**：升级期间 agent 二进制目录会多出 `<二进制>.prev`（旧版硬链接，≈10 MiB）与
  `.tether-upgrade.json`（marker，<1 KiB），升级提交后 prev 保留（下次升级时被替换）。
  小盘 VPS（如 racknerd）`df` 时记得算上这份常驻 ≈10 MiB；下载/解压期间另有一份新二进制的
  **瞬时** tmp 副本（同目录 `.tether-upgrade-*`，安装结束即删，峰值再 +≈10 MiB）。
- **首跳无保护**：把带本设施的第一个 release 推上现网的那一次 `node upgrade`，执行者仍是
  **旧 agent 代码**（无冒烟/无 prev/无回退），风险与历史版本等同——那一跳请手工金丝雀：
  先升 1 台非关键 agent，`tether node ls` 确认 RELEASE 列变了再扇出。
- **setsid-nohup 部署 GAP**：无监督路径下"新二进制起来即崩"无人拉起（与旧版同险）；
  systemd 部署（system 层或 user 层单元）不受此限。
- **NAT 弱网下的假回退**：120s 内 register 不成会触发自动回退——弱网节点可能明明二进制没问题
  也被退回旧版。无害（节点仍在线、旧版本），代价是浪费一轮：网络恢复后重跑一次 upgrade 即可。
- **回退的观测**（外审 F7 订正措辞）：ctl 的 `--wait` **不消费任何持久升级状态**——
  `COMMITTED` 是对 `node.list` release 轮询的**推断**（新 release + ONLINE），超时打印的是
  `likely ROLLED BACK`（推断，非断言）。**权威状态**看两处日志：agent log 的状态机行、
  broker log 的 `agent-reported upgrade outcome` 行（agent 重新 register 时携带
  `upgrade_state`：committed / rolled_back / rollback_failed）。
- **同宿主多 agent = 一个升级域**（外审 F1 裁决）：共享同一 `tether` 二进制的多个 agent
  共享 prev/marker，同一时刻只允许一个在途升级（其余节点该跳返回 `upgrade_in_progress`，
  `--all` 会 transient-skip、稍后重试）；升级的提交/回退只由 `node upgrade` 点名的那个
  nid 驱动。跨进程互斥由二进制目录下的 `.tether-upgrade.lock`（flock）保证。
- **同 tag 重装（same-tag re-push）**：`--wait` 无法用 release 变化区分"旧行"与"新 register"，
  一律 fail-closed 报 **UNCONFIRMED**（exit 非零）；`--all` 的金丝雀遇到它会中止扇出。
  同 tag 修复损坏二进制请**逐台**执行并以 agent/broker 日志人工确认。

### 8.8 h1 增量的 broker 迁移（日志封顶 + 存储 GC + 新告警种类）

h1 改了三样运维可见的东西：日志 sink、`port_allocations`/`processes` 的保留期 GC、
以及一个新的 `proxy_bind_stalled` 告警种类（migration 0018）。**升级顺序：先 broker，
后 agent**（新 agent 的 proc-event ACK 需要新 broker 才有人应答；旧 broker 只会让 agent
降级回老行为，但没必要制造这段噪声）。

**在已有部署上（例如 racknerd）逐步做**。重跑 install.sh 不再覆盖你手工维护的 `cluster:` 段
（它会保留原文件、把新模板写成 `broker.yaml.new`），但带 `--force-config` 会。手工执行：

1. **确认 journald 持久化**（新 unit 把 panic 交给它）：
   `mkdir -p /var/log/journal && systemctl restart systemd-journald`，
   并在 `/etc/systemd/journald.conf` 设一个与磁盘匹配的 `SystemMaxUse=`（19GB 盘建议 ≤500M）。
2. **改 unit**：`/etc/systemd/system/tether-broker.service` 里
   `StandardOutput=append:…` / `StandardError=append:…` 两行换成 `journal`。
3. **加 yaml 块**：broker.yaml 的 `broker:` 下补 §7.5 的 `observability:` 三行。
4. **删掉旧的 root-owned 日志文件**：
   `rm -f /var/log/tether/broker.log /var/log/tether/broker.err`。
   ⚠ **这一步不能省，`truncate` 也不行**：systemd 的 `append:` 是在**降权到 tether 之前**
   打开/创建这两个文件的，所以它们属 root；换成进程内 sink 之后，tether 用户去 open 会
   EACCES，Writer 就永久停在 degraded 模式（内容只 spill 到 stderr）。truncate 不改属主。
   （先留档就 `tail -c 50M … | gzip > /root/broker.err.tail.gz` 再删。）
5. `systemctl daemon-reload && systemctl restart tether-broker`。
6. **冒烟检查**（三条都要看）：
   - `ls -l /var/log/tether/broker.log` → 属主是 **tether**，且在增长；
   - `tether ps` 毫秒级返回（h1 A 的有界回包）；
   - `tether alert ls` → 若现网有 agent 的 proxy 绑不上，会看到
     `proxy_bind_stalled:<sid>/<nid>`（severity **info**，不会挡破坏性命令）。

**GC 的一次性追赶**：启动后 GC 每 5 分钟一轮、每轮每张表最多 5000 行。
2026-08-04 的积压（24k FREED 端口行 + 8.5k EXITED 进程行）约 **25 分钟**排干；
期间 `tether ps -a` 的 total 计数会一路下降。保留期：processes 1 小时（`proc_retention`），
port_allocations 24 小时（常量，无配置项）。

**回滚约束（N-1 窗口）**：一旦 `proxy_bind_stalled` 告警被写进过 raft 日志，
**从零重放日志**到 h1 之前的二进制会撞 0009 的 CHECK（fail-stop）。回滚只支持
**snapshot-restore**，不支持 log-replay。同理，多 broker 集群必须**锁步**升级——
新的 FSM op（`ProcGC`/`PortGC`）与新告警种类在旧 broker 上都是未知的。

---

### 8.9 升到本版后轮换一次 tunnel token / 订阅 PSK（存量部署，一次性）

**只对 v0.5.0 及更早升上来的部署适用；全新部署不需要。**

v0.5.0 及更早的 broker 把 agent register reply 发进共享 `_INBOX`，而那个空间对任何
**自称旧版**的连接可读——不需要凭据，一条当场生成、从未被准入、不是任何 session 成员的
nkey 就够了（自报走 CONNECT 的 `Username`，攻击者只要**不发**那个标记）。reply 里带的是
`ProxyDirective{Token, Keys[].Secret}`：原始 tunnel token 加**全部**订阅者的 Shadowsocks PSK。

本版起这些秘密不再进入共享空间（回复主题不在 `_TINBOX.` 根就整段省略，见 requirements §6.7），
**但升级不会让已经泄漏出去的旧值失效**——它们是长寿凭据，全仓没有一处会自动轮换。所以升完
broker 后，对每个开了 proxy 的 session 做一次：

**次序是有意的，不能重排**（外审 R2-B1 订正——本节此前写的是 `off` → `on` → 再逐个 revoke，
那个次序有一段窗口会把**旧 PSK 重新启用**）：

```bash
# ① 关掉数据面。注意：off/on 并不重铸 subscriber keyset——PSK 存在 subscriber 行里，
#    只有 revoke 才作废它；此前本节声称 off 会「作废当前 keyset」，那是错的。
tether proxy off -s <sid>

# ② 仍在 OFF 状态下，逐个作废全部旧 subscriber。这一步才是真正让旧 PSK 与
#    /sub bearer token 失效的动作。
tether proxy sub ls -s <sid>
tether proxy sub revoke -s <sid> --name <each>

# ③ 现在才开。此时 keyset 是空的，tunnel token 重铸，没有任何旧值可被重新启用。
tether proxy on -s <sid>

# ④ 只从**已升级**的 ctl 重建订阅者——旧 ctl 收不到 /sub URL（本版起被 withheld），
#    这正是设计：一个已经在共享总线上出现过的 token 不该再发一次。
tether proxy sub create -s <sid> --name <each>
```

**为什么不能先 `on` 再 revoke**：`enableProxy` 与 cluster reaper 读的是 `activeProxyKeys`，
也就是**当前仍然存在的 subscriber 行**。在它们被 revoke 之前 `on` 一次，等于把那批旧 PSK
当作新 keyset 重新下发一遍——泄漏过的凭据于是又活了一段，直到最后一条 revoke 完成。

⚠ `proxy off` 会**中断该 session 的数据面**直到 ④ 完成，所以挑窗口做。
⚠ 未升级的 agent 在这之后拿不到新 token（这正是本版的设计：秘密不进共享空间），
表现为 expose/proxy 挂起而**控制面照常**——先 `tether node upgrade <nid>`，再让它重新注册。

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
