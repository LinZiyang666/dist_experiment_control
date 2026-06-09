# tether 架构文档

> 本文面向实现者，承接 `docs/requirements.md`。按议题（A–K）增量落盘，每议题定稿即写入对应小节。
>
> 术语与北极星请参见 `docs/requirements.md` §2、§3。本文只谈"如何实现"。

---

## A. 部署拓扑

### A.1 公网侧进程合并度

公网侧 v1 默认三进程部署：

| 进程 | 职责 | 二进制来源 |
|---|---|---|
| `caddy` | 443 入口 TLS 终结 + ACME 自动证书 + 反代到 NATS WebSocket | Caddy 官方二进制 |
| `nats-server` | 控制面消息总线 + JetStream 持久化，仅 127.0.0.1 WebSocket 内部监听 | NATS 官方二进制 |
| `tetherd` | 业务逻辑：鉴权（`auth_callout`）、会话/节点/进程状态、端口分配、**内嵌 frps 库**（监听 `:7000` control + `:14000-14999` remote_port） | 本项目发布 |

**选择理由**：
- NATS 外置保留社区生态（`prometheus` exporter、`nats-top`、托管 NATS 集群替换等运维能力）。
- `frps` 内嵌入 `tetherd`：端口分配 → 生成 token → 通知 agent 是一次进程内内存操作，避免跨进程 RPC 协调。
- 不嵌入 NATS：避免失败域耦合、便于独立调优 JetStream 磁盘/监控/备份。
- Caddy 单独进程：专司 443 TLS + ACME，tetherd 不做证书管理；frps 自管 TLS 不经 Caddy（见 A.3）。

**不选**：
- 单进程巨石（`tetherd` 内嵌 NATS + frps + Caddy）：部署虽简但放弃 NATS 与 Caddy 社区工具链。
- `tetherd` 直监听 443：需要 tetherd 内置 ACME、证书热加载等 Caddy 已经解决的问题。

### A.2 `tetherd` 与 `nats-server` 同机性

v1 **强制同机部署**：
- `tetherd` 默认连 `nats://127.0.0.1:4222`。
- 磁盘上 `tetherd` 的 SQLite 与 JetStream `$JS_STORE` 位于同一盘，便于整体备份。
- NATS 不对外暴露 4222，仅经 broker 入口转发 WSS。

配置可覆盖为外置 NATS URL（逃生舱），不作为一期主路径，也不出现在 `tether broker init` 向导里。

### A.3 公网入口与端口策略

v1 采用 **split ports** 单一部署模型。取消此前草案的 M1 单端口多路复用 / M2 双端口 / M3 自定义三模式 —— 理由：frp TCP remote_port 模式的数据面流量没有 SNI/ALPN，Caddy 按 ALPN 分不了 frpc 自协议；要做 443 单端口多路复用必须先验证一条 L4 mux 路径（frp vhost / 自研 mux），而这件事扩大了 v1 实现面，还要造测试矩阵。split ports 换来设计、安装、测试的闭合；"只出站 443 的严防火墙环境"降级为 v2 目标。

**端口清单**：

| 方向 | 端口 | 用途 | 协议 |
|---|---|---|---|
| broker 入站 | `:443` | NATS WSS（Caddy 终结 TLS → 反代到 `127.0.0.1:<nats_ws>`） | TLS + WebSocket |
| broker 入站 | `:7000` | frpc control connection（frps 直接监听，frp 原生 TLS） | TCP + TLS |
| broker 入站 | `:14000-14999` | frp TCP remote_port，业务 expose 对外端口 | TCP（业务层自带协议） |
| agent 出站 | broker:443 | NATS WSS | —— |
| agent 出站 | broker:7000 | frpc control | —— |

**硬约束**：agent 主机必须能出站到 broker 的 `:443` 和 `:7000`，无法出站 = 不支持。使用者访问 expose 端口（`broker:14022` 等）走使用者自己的网络路径，broker 侧只负责开放。v1 不做 STUN/TURN、UDP、协议伪装、单 443 mux。

**诚实说明**：
- 若 agent 所在网络严格只允许 443 出站 → v1 不支持。这类场景 v2 再评估（需要专门的 L4 mux 设计 + 验证）。
- expose 业务流量默认 **明文 HTTP/TCP**：`python -m http.server` 暴露后访问是 `http://broker:14022`，不是 `https://`。业务要 HTTPS 由业务自己终结 TLS，frp 不插手。

**默认向导** `tether broker init` 问两项：
1. 域名（用于 ACME 证书与客户端连接串）
2. 证书来源（Let's Encrypt ACME / 手动证书路径）

生成 `broker.yaml` 与 systemd 单元文件。用户也可跳过向导手写 yaml。

#### broker.yaml 骨架

```yaml
broker:
  domain: tether.example.com
  tls:
    acme:
      email: owner@example.com
    # 或手动证书：
    # cert_file: /etc/tether/cert.pem
    # key_file:  /etc/tether/key.pem
  nats:
    wss_listen: ":443"           # Caddy 终结 TLS，反代到 127.0.0.1:<nats_ws_internal>
    ws_internal: "127.0.0.1:8222"
  frp:
    bind_addr: "0.0.0.0"         # frps 监听网卡；split ports 要求可被外部 agent 访问（见 F.1）
    control_listen: ":7000"      # frps 直接监听，自带 TLS
    port_range: "14000-14999"    # expose 对外端口池
```

#### 客户端配置（自动生成）

```yaml
broker:
  nats_url:       wss://tether.example.com:443/nats
  frps_control:   tether.example.com:7000
```

#### 反代/监听分工

- **Caddy**：仅反代 `:443` → `127.0.0.1:<nats_ws_internal>`（NATS 的 WebSocket 内部口）。负责 ACME 证书与 TLS 终结。NATS 本身不开外网口。
- **nats-server**：WebSocket 内部监听（如 `127.0.0.1:8222`），应用层走 WSS。
- **frps**（嵌入 `tetherd`，见 F.1）：`:7000` 监听 frpc control connection（frp 原生 TLS，自管证书，可复用 domain 证书或自签）；`:14000-14999` 监听业务 remote_port。

Caddy 不触碰 frp 流量。v1 不做 `:443` 上的 ALPN/SNI 分流。

---

## B. NATS subject 命名空间

### B.0 设计原则

1. **版本前缀**：所有 subject 以 `tether.v1.` 开头。协议升级走 `tether.v2.`，可与 v1 并存。
2. **分层即权限**：subject 前缀直接决定订阅/发布权限，由 NATS `auth_callout` 按模板授权。
3. **per-session 子树 + 固定 token 数**：同 session 的消息塞进 `tether.v1.s.<sid>.*` 做隔离；但 **JWT permission 一律不用 `s.<sid>.>` 大通配**，而是按 B.2 / C.4 的精确模板按固定 token 数下发（pub / sub 各自只开明确需要的叶子 subject）。大通配会让 ctl 能跨 verb / 跨发起人 pub `.forwarded`，破坏 C.4 的 ctl→tetherd→agent 强转发。
4. **动词在末段**：`…req` / `…reply` / `…heartbeat` 等后缀便于批量订阅。
5. **解耦正交性**：鉴权不占用业务 subject；节点控制面集中一处不分散。

### B.1 顶层四段分层

每条业务命令的 subject 里**嵌入发起人 actor**（= NATS user nkey public key，见 B.5），JWT permissions 精确限定 ctl 只能 pub 自己 actor 段的 subject —— 这样 tetherd 从 subject 即可提取不可伪造 actor，不信任消息头自报字段。

```
tether.v1.
├── ctrl.                                                       ← 全局控制（会话 + 节点 + 版本广播）
│   ├── version.announce                                        broker pub
│   │
│   ├── by.<actor>.session.create.req                           ctl pub
│   ├── by.<actor>.session.list.req                             ctl pub
│   ├── by.<actor>.session.<sid>.rm.req                         ctl pub（owner-only，应用层检查）
│   ├── by.<actor>.session.<sid>.kick.req                       ctl pub（owner-only，应用层检查）
│   ├── by.<actor>.session.<sid>.rotate-pin.req                 ctl pub（owner-only，应用层检查）
│   ├── by.<actor>.s.<sid>.ps.req                               ctl pub（不针对单 node，tetherd 查库直接 reply）
│   ├── by.<actor>.s.<sid>.node.list.req                        ctl pub
│   ├── by.<actor>.s.<sid>.node.<nid>.tag.req                   ctl pub
│   │
│   ├── s.<sid>.node.<nid>.register.req                         agent pub（有 reply；G.1 对账）
│   ├── s.<sid>.node.<nid>.unregister.req                       agent pub（有 reply；优雅离开 ack）
│   └── s.<sid>.node.<nid>.heartbeat                            agent pub，5s 一次（事件，无 reply）
│
│   (注：session login/join 走 NATS 原生 $SYS.REQ.USER.AUTH，不占业务 subject；见 §E.0 / §E.2)
│
├── s.<sid>.                                                    ← per-session 子树（核心隔离边界）
│   ├── cmd.by.<actor>.node.<nid>.<verb>.req                    ctl pub；tetherd 订；agent **不订、不 pub**
│   │                                                           verb ∈ {run, exec, expose, expose-rm, kill, upgrade}
│   │                                                           （ps / tag 不走 cmd：ps 走 ctrl.by.<A>.s.<S>.ps.req 查库 reply；
│   │                                                            tag 走 ctrl.by.<A>.s.<S>.node.<N>.tag.req 查库写库 reply，不经 agent 转发）
│   ├── cmd.node.<nid>.<verb>.req.forwarded                     tetherd pub；agent 订；ctl **deny pub**
│   ├── ev.node.<nid>.state                                     tetherd pub（ONLINE / STALE / OFFLINE 转移）
│   ├── ev.node.<nid>.proc.<pid>.{started,exit}                 agent pub（运行时唯有 agent 知晓）
│   ├── ev.port.<port>.{allocated,revoked,freed}                tetherd pub
│   ├── ev.session.destroyed                                    tetherd pub
│   ├── audit.call                                              tetherd pub → JetStream（kind=call/admin/admin_denied）
│   ├── audit.proc                                              tetherd pub → JetStream（订 ev.proc.* 后转写）
│   ├── audit.port                                              tetherd pub → JetStream
│   ├── pty.<pid>.out                                           agent pub（PTY master → 前端）
│   ├── pty.<pid>.ready                                         agent pub（attach 两阶段握手；见 C.5）
│   ├── pty.<pid>.failed                                        agent pub（attach_timeout 等启动失败；tetherd 订后写 audit.proc + 回 run.req reply）
│   ├── pty.<pid>.in                                            ctl pub（键盘 → 进程）
│   ├── pty.<pid>.resize                                        ctl pub（SIGWINCH）
│   └── pty.<pid>.attach                                        ctl pub（attach-ready 握手回执）
│
└── sys.events                                                  ← 全局运维事件（JetStream events 流订阅点）
```

**解耦检查**：
- 无独立 `auth.*` 子树 —— 鉴权走 NATS 原生 `auth_callout`（见 B.2），在 CONNECT 阶段完成，不占业务 subject。
- 所有业务命令 subject 嵌 `by.<actor>` 段，actor 身份由 NATS 强制（JWT permission 限定）而非消息头，彻底消除 header 伪造路径。
- agent 侧控制面 subject 带 `s.<sid>` 前缀（`ctrl.s.<sid>.node.<nid>.*`）——nid 仅 session 内唯一（B.5），必须带 sid 隔离。
- 心跳仍属 global control-plane 数据（不归属 session 子树下），但通过 `ctrl.s.<sid>.node.<nid>.heartbeat` 做 session 隔离；tetherd 转化出 `s.<sid>.ev.node.<nid>.state` 让 session 成员感知节点在线状态。
- **固定 token 数**：命令类 subject 一律不用 `>` 大通配，模板按固定 token 数下发。`tether.v1.s.S.cmd.by.A.node.*.*.req` 不会误匹配 `…req.forwarded`（token 数多一段）。

### B.2 鉴权：NATS nkeys + `auth_callout`

**身份**：用户/agent 的 ed25519 私钥**同时**用作：
1. 应用层身份（SHA256 fingerprint 即 identity，对应 requirements §9 `actor_pubkey_fp`）
2. NATS nkey（`go-nkeys` 支持从裸 ed25519 seed 构造 NKey）

**握手**：
1. 客户端以 nkey 向 NATS 发起连接（`tls wss://…`）。NATS 服务端发 nonce，客户端用 nkey 签名返回，证明私钥持有。
2. NATS 把签名后的身份（public nkey）传给 `auth_callout` 服务（= tetherd）。
3. tetherd 查 SQLite：这条 nkey 属于哪个 identity？有哪些 session 成员资格？是否 owner？
4. tetherd 返回一个**一次性 user JWT**，内含按本连接定制的 permissions。
5. NATS 应用该 JWT，连接正式可用。

**每条 ctl 连接对应一个"已激活"的 session**（见 §E.0 session 激活模型）。JWT 只含该 session 的权限；切换 session = 重新 CONNECT 拿新 JWT。

**模板里的占位**：`<A>` = actor nkey public key（见 B.5）；`<S>` = 已激活 session 的 sid；`<N>` = 本 agent 的 nid。tetherd 在 auth_callout 里按连接角色实例化。

**已认证但未激活 session 的 ctl 连接**（刚 `tether login` 未指定 session）：
```jsonc
{
  "publish": {
    "allow": [
      "tether.v1.ctrl.by.<A>.session.create.req",
      "tether.v1.ctrl.by.<A>.session.list.req",
      "_INBOX.>"
    ]
  },
  "subscribe": {
    "allow": [
      "tether.v1.ctrl.version.announce",
      "tether.v1.sys.events",
      "_INBOX.>"
    ]
  }
}
```

**已激活 session S 的 ctl 连接（所有 member 共用同一模板；owner-only 动作的拦截在 tetherd 应用层）**：
```jsonc
{
  "publish": {
    "allow": [
      "tether.v1.ctrl.by.<A>.session.create.req",
      "tether.v1.ctrl.by.<A>.session.list.req",
      "tether.v1.ctrl.by.<A>.session.<S>.rm.req",
      "tether.v1.ctrl.by.<A>.session.<S>.kick.req",
      "tether.v1.ctrl.by.<A>.session.<S>.rotate-pin.req",
      "tether.v1.ctrl.by.<A>.s.<S>.ps.req",
      "tether.v1.ctrl.by.<A>.s.<S>.node.list.req",
      "tether.v1.ctrl.by.<A>.s.<S>.node.*.tag.req",
      "tether.v1.s.<S>.cmd.by.<A>.node.*.*.req",
      "tether.v1.s.<S>.pty.*.in",
      "tether.v1.s.<S>.pty.*.resize",
      "tether.v1.s.<S>.pty.*.attach",
      "_INBOX.>"
    ]
  },
  "subscribe": {
    "allow": [
      "tether.v1.ctrl.version.announce",
      "tether.v1.s.<S>.ev.>",
      "tether.v1.s.<S>.audit.>",
      "tether.v1.s.<S>.pty.*.out",
      "tether.v1.s.<S>.pty.*.ready",
      "tether.v1.sys.events",
      "_INBOX.>"
    ]
  }
}
```

**owner-only 动作的拦截**：`session.<S>.{rm,kick,rotate-pin}` 与 `cmd.by.<A>.node.*.upgrade.req` 虽对所有 member 放行 pub 到 NATS，但 tetherd 在审核时查 SQLite `members.role` —— 非 owner 立即 reply `admin_denied` 并写 `audit.call{kind:admin_denied}`，不 pub `.forwarded`。NATS 仅做"连接身份 + actor 防伪"两层，owner 校验单点落在 tetherd。理由：v1 少一套权限模板；owner 逻辑集中易审查；NATS JWT 不承载业务策略。

**agent 连接的 permissions（node nid=N，属 session S）**：
```jsonc
{
  "publish": {
    "allow": [
      "tether.v1.ctrl.s.<S>.node.<N>.register.req",
      "tether.v1.ctrl.s.<S>.node.<N>.unregister.req",
      "tether.v1.ctrl.s.<S>.node.<N>.heartbeat",
      "tether.v1.s.<S>.ev.node.<N>.>",
      "tether.v1.s.<S>.pty.*.out",
      "tether.v1.s.<S>.pty.*.ready",
      "tether.v1.s.<S>.pty.*.failed",
      "_INBOX.>"
    ]
  },
  "subscribe": {
    "allow": [
      "tether.v1.s.<S>.cmd.node.<N>.*.req.forwarded",
      "tether.v1.s.<S>.pty.*.in",
      "tether.v1.s.<S>.pty.*.resize",
      "tether.v1.s.<S>.pty.*.attach",
      "_INBOX.>"
    ]
  }
}
```

**agent 明确不获得** `s.<S>.audit.*` 的 pub 权限 —— 审计一律由 tetherd 订 ev 后写入（C.1 第 4 条）。agent 只发运行时事件（`ev.node.N.proc.*`），tetherd 转写 `audit.proc`。

**tetherd 自身连接的 permissions**（tetherd 以独立 nkey 连 NATS，拥有全局 broker 特权）：
```jsonc
{
  "publish": {
    "allow": [
      "tether.v1.s.*.cmd.node.*.*.req.forwarded",
      "tether.v1.s.*.ev.>",
      "tether.v1.s.*.audit.>",
      "tether.v1.ctrl.version.announce",
      "tether.v1.sys.events",
      "_INBOX.>"
    ]
  },
  "subscribe": {
    "allow": [
      "tether.v1.ctrl.by.*.>",
      "tether.v1.ctrl.s.*.node.*.register.req",
      "tether.v1.ctrl.s.*.node.*.unregister.req",
      "tether.v1.ctrl.s.*.node.*.heartbeat",
      "tether.v1.s.*.cmd.by.*.node.*.*.req",
      "tether.v1.s.*.ev.>",
      "tether.v1.s.*.pty.*.failed",
      "$SYS.REQ.USER.AUTH",
      "_INBOX.>"
    ]
  }
}
```

**关键安全不变式**（NATS 服务端强制，非应用层约定）：
- ctl 无法 pub `cmd.node.*.*.req.forwarded` —— 不在任何 ctl 模板的 allow list 中。
- ctl 无法 pub 其他 actor 段的 subject —— `by.<A>` 的 `<A>` 被 tetherd 写死在下发的 JWT 里。
- agent 无法 pub audit —— tetherd 是审计唯一写入方。
- agent 无法订原始 `.req` —— 只订 `.req.forwarded`。

### B.3 四类传输

| 类型 | Subject | 传输语义 | 可靠性 |
|---|---|---|---|
| **命令（req/reply）** | `tether.v1.s.<sid>.cmd.*` 及 `tether.v1.ctrl.*` | NATS `Request()`，默认超时 10s，reply 走自动 inbox | 同步等待；超时即失败，由发起方决定重试 |
| **事件（pub/sub）** | `tether.v1.s.<sid>.ev.*`、`tether.v1.sys.events`（订阅端） | core pub，订阅者 push 接收 | best-effort；离线订阅者丢消息 |
| **审计（stream）** | `tether.v1.s.<sid>.audit.*` → JetStream `history-<sid>` | `Publish()` 带 ACK | 至少一次，持久化 |
| **PTY 字节流** | `tether.v1.s.<sid>.pty.<pid>.{in,out}` | core pub，**不入流** | 顺序即会话顺序；断联即丢 |
| **心跳** | `tether.v1.ctrl.s.<sid>.node.<nid>.heartbeat` | core pub，5s 一次 | 丢即超时 → tetherd 判 STALE/OFFLINE |

**PTY 不入流**：前端 tether 断线期间的字节流永久丢失（与 Q4 / O6 "不记录 PTY 字节流"一致）。一期不做 replay。

### B.4 reply subject：用 NATS 自动 inbox

NATS `req/reply` 基于 pub/sub 约定实现：请求方在消息头带 `reply-to`，响应方 pub 到该 subject。

| 做法 | reply subject | 可否被第三方订阅 | 复杂度 |
|---|---|---|---|
| **inbox（选定）** | `_INBOX.<rand>.<rand>`，SDK 自动生成 | 否（随机 token） | 零配置 |
| 显式 `*.reply.<req-id>` | 固定规则，可预测 | 是 | 需手动管理 req-id 与权限子树 |

**选 inbox**。审计由 **tetherd 单边** 主动写 `audit.*` 流（权威、结构化，agent 不 pub `audit.*`；见 C.1 规则 4），不需要第三方旁听 reply；同 session 队友看彼此动作靠订 `ev.*`。reply 的唯一消费者 = 请求方自己。

### B.5 标识符字符集

| 标识符 | 规则 | 来源 |
|---|---|---|
| `sid` | `[a-z0-9-]{1,32}`，全局唯一，有保留字 | requirements.md §7.3 |
| `nid` | `[a-z0-9-]{1,32}`，session 内唯一（A2：重复即报错） | agent 自报 |
| `pid` | ULID 小写（`01hzxk…`） | tetherd 生成 |
| `port` | 十进制整数字符串 | tetherd 分配 |
| `actor` | NATS user nkey public key，base32（`U…`，56 字符，字符集 `A-Z2-7`） | auth_callout 上下文，与客户端 nkey 一一对应 |

**actor_token 说明**：
- 形如 `UABCDEFGH…`（首字符 `U` = NATS 身份前缀 user，后跟 base32 编码的 ed25519 公钥 + checksum）。
- 完整公钥，无碰撞歧义；字符集 subject-safe，直接嵌入 subject 无需转义。
- tetherd 维护 `actor_token → pubkey_fp → display_name` 的 SQLite 映射；审计里同时写 `actor_nkey` 与 `actor_fp`（见 §H.5）。
- **不可伪造性**：ctl 在 auth_callout 阶段由 NATS 验证 nkey 持有，之后下发的 JWT permissions 里 `by.<actor>` 段被 tetherd 写死为本连接的 actor_token；ctl 试图 pub 到别人的 `by.<other>` subject 会被 NATS 直接拒绝。

所有标识符直接嵌入 subject 无需转义。

---

## C. 控制面协议分层

### C.1 六条链路规则

1. **ctl 只与 tetherd 对话**。ctl 不直连 agent；agent 接收的所有命令必定过 tetherd 审核。
2. **命令两段式转发**。ctl pub `.req` → tetherd 订并审核 → tetherd pub `.req.forwarded` → agent 订并执行。详见 C.4。
3. **阻塞语义两层（RPC 受理 vs CLI 前台）**，分开理解：
   - **RPC 受理（req/reply 契约）**：每条 `.req` 都有 reply，tetherd 转发 agent 或查库后返回 "已受理 / 无此资源 / 权限拒" 之类的结果。这一层所有 verb 都是"完成即返"（完成 = 请求被受理并给出确定结果，不代表业务动作跑完）。
   - **CLI 前台语义（用户看到什么）**：用户视角 CLI 是否阻塞到业务结束，取决于 verb：
     - **CLI 立即返回**：`expose`、`kill`、`ps`、`tag`、`session.*`（拿到结果即退出）
     - **CLI 前台驻留读 PTY 字节流**：`run`（RPC 受理拿到 pid 后，CLI 订 `.pty.<pid>.out` 一直读到 `proc.exit`；退出 = 进程退出）
     - **CLI 前台驻留读 stdout/stderr**：`exec`（非交互；RPC 受理后转驻留读直到 `proc.exit`）
   - 结论：阻塞不阻塞是 CLI 体验层的事；协议层面所有 `.req` 一视同仁走受理 reply。
4. **审计由 tetherd 写**。actor 身份由 `by.<actor>` subject 段权威提供（见 B.1 / B.5），tetherd 从 subject 提取；agent 不写 `audit.*`。agent 只 pub `ev.node.N.proc.*` 等运行时事件，tetherd 订 ev 后转写 `audit.proc` / `audit.port`。
5. **运行时事件由 agent 写**。`proc.started`、`proc.exit` 等发生时刻只有 agent 知道，走 `s.<sid>.ev.node.<nid>.*`。tetherd 触发的事件（`port.allocated/revoked/freed`、`session.destroyed`、`node.state`）由 tetherd 自己 pub。
6. **session 状态前置校验**。tetherd 在审核转发任何 req 之前，先查 SQLite `sessions.state`：状态为 `DELETING` 或记录不存在 → 立即 reply `{code:"session_not_found_or_deleting"}`，不写 `audit.call`（或写 `admin_denied`），不 pub `.forwarded`。这条规则独立于 JWT permissions，保障"旧 JWT 不强制吊销"策略（§G.2）下 session rm 的窗口安全。

### C.2 命令 → 事件 → 审计 映射

（两列：**RPC reply** = tetherd 对 `.req` 的受理结果；**CLI 前台** = 用户终端何时退出）

| 命令 | RPC reply 回到 ctl 的时机 | CLI 前台退出时机 | 事件 | 审计（JetStream） |
|---|---|---|---|---|
| `run` | 受理（两阶段握手完成 + 拿到 pid） | 远端进程退出 / PTY EOF / 用户 Ctrl-C | `s.S.ev.node.N.proc.P.exit` | `s.S.audit.call` + `s.S.audit.proc` |
| `exec` | 受理（拿到 pid） | 进程结束（含 rc 打印） | `s.S.ev.node.N.proc.P.exit` | `s.S.audit.call`（只 meta：verb/target/ok；stdin/stdout 不记） |
| `expose` | 受理（拿到 port） | 立即（打印 URL） | `s.S.ev.port.<port>.allocated` | `s.S.audit.call`（port 记录，token 不记） |
| `ps` | 受理（查库直接 reply） | 立即（打印表） | — | `s.S.audit.call`（admin 只读） |
| `kill` | 受理（信号已发） | 立即 | `s.S.ev.node.N.proc.P.exit` | `s.S.audit.call`（admin） |
| `tag` | 受理（已写 DB） | 立即 | — | `s.S.audit.call`（admin） |
| `session.{create,list}` | 受理 | 立即 | — | `sys.events` |
| `login`（含 join-via-pin） | 走 NATS 原生 `$SYS.REQ.USER.AUTH`，不经 `ctrl.*` | 立即 | — | `sys.events`（`member_joined` / `pin_failed`） |
| `session.{rm,kick,rotate-pin}` | 受理（非 owner 返回明确 error reply，不静默丢弃） | 立即 | — | `sys.events`（含 `admin_denied`） |

### C.3 命令级重试：不自动重试

- req/reply 默认超时 10s。超时即 ctl 报错退出，**不自动重发**。
- `run` 不幂等（重发 = 两次启动进程）；其他命令一期不做 idempotency key。
- 用户判断是否需要重发。

> **与连接级重连的区分**：连接级（NATS / frpc 长连接）**全部自动重连**，NATS Go 客户端内置指数退避，无上限重试。agent/ctl 掉网后恢复时自动恢复会话态（依 requirements A11 被动生命周期语义）。

### C.4 `.forwarded` 中转：防止 ctl 绕过 tetherd

**目的**：哪怕 ctl 密钥泄漏或代码有 bug，agent 收到的命令也必定经过 tetherd 的权限校验；且 tetherd 收到 `.req` 时能权威识别发起人，不依赖消息头自报。

**subject 分工**：
- `s.<sid>.cmd.by.<actor>.node.<nid>.<verb>.req` — ctl 可 pub（仅自己的 `<actor>` 段）；tetherd 订；agent **不订、不 pub**。
- `s.<sid>.cmd.node.<nid>.<verb>.req.forwarded` — tetherd 可 pub；agent 订；ctl **不在权限表，NATS 直接拒**。

**权限边界**（由 auth_callout 下发的 JWT；与 B.2 同一套模板的两种视图）：

| 角色 | 对 `cmd.by.<actor>.node.*.*.req` | 对 `cmd.node.*.*.req.forwarded` |
|---|---|---|
| ctl | allow pub（`<actor>` 被 tetherd 写死为本连接身份） | 不在 allow list，NATS 拒绝 |
| tetherd | allow sub（`cmd.by.*.node.*.*.req` 全订） + pub | allow pub |
| agent | — | allow sub（`cmd.node.<N>.*.req.forwarded` 仅本 node） |

**actor 权威来源**：tetherd 订到 `.req` 时，**从 subject 的 `by.<actor>` 段**解析发起人的 nkey public key；查 SQLite 拿到 `pubkey_fp` 和 `display_name`。**不信任任何消息头自报**的 actor 字段。

**tetherd 转发时的消息头**（非权威，仅为 agent 侧日志 / 展示方便）：
- `X-Actor-Name`：display_name（冗余，权威来自 subject 推出的 actor_token）
- `X-Req-Id`：ULID，审计关联用
- `X-Session`：sid（冗余，便于 agent 侧日志）
- （不再有 `X-Actor-Fp` / `X-Actor-Nkey` header —— actor 由 subject 承载）

**v1 安全边界**：NATS 服务端强制 JWT permissions，ctl 无法写 `.forwarded` subject，也无法冒充别人的 `by.<actor>` 段。这解决了"ctl 绕过 tetherd 直接控 agent"和"ctl 伪造 actor header 污染审计 / 绕过 owner-only"两个基础威胁。未处理的威胁（供应链、内部人员、tetherd 自身沦陷）留给后续版本。

### C.5 `run` 的实时字节流（进度条支持）

**机制**：agent 启动用户进程时分配 PTY（creack/pty）；用户进程的 stdin/stdout/stderr 绑到 PTY slave；agent 读 PTY master 原始字节，块级 pub 到 `s.S.pty.<pid>.out`；ctl 订阅后按字节原样写入本地 tty。

#### C.5.1 两阶段 attach 握手（避免首字节丢失）

**问题**：若 agent 立即 fork+exec 开始 pub `pty.<pid>.out`，而 ctl 还没来得及 subscribe，progress bar 的前几行字节就永久丢失（PTY 不入 JetStream，§B.3）。

**解决**：`run` 走 pre-subscribe 握手。

```
ctl                                                tetherd                    agent
 │  1. pub s.S.cmd.by.<A>.node.<N>.run.req ───────> │
 │                                                   │  2. ACL 转发
 │                                                   │ ───> pub s.S.cmd.node.<N>.run.req.forwarded
 │                                                   │                         │
 │                                                   │  3. agent 分配 pty、**不 exec**，
 │                                                   │     先占位 pid 资源，pub s.S.pty.<pid>.ready
 │                                                   │     ready 载荷：{pid, cols, rows}
 │  4. 收到 pty.<pid>.ready →                        │                         │
 │     sub s.S.pty.<pid>.out / .in / .resize         │                         │
 │  5. pub s.S.pty.<pid>.attach  ───────────────────────────────────────────>  │
 │                                                   │                         │  6. 收到 attach → fork+exec，
 │                                                   │                         │     PTY slave 绑 stdio，
 │                                                   │                         │     首字节直接进 .out
 │  7. 读 .out 实时输出 ─────────────────────────────────────────────────────<─┤
```

**超时语义（v1 简化：拒绝启动）**：agent pub ready 后等 ctl 的 `attach`，3 秒内没到 → **放弃启动**，pub `s.S.pty.<pid>.failed{reason:"attach_timeout"}` 并释放 pid 资源；tetherd 收 failed 后写 `audit.proc{kind:attach_timeout}` 并回 ctl 原 run.req 的 reply。用户不会看到半截启动 + 前几行丢字节的尴尬状态。

**为什么不做"先启动再丢字节通知用户"**：v1 阶段，攻击面小、用户少；"启动了 + 丢了字节"的调试难度比"直接失败、重试就好"更高。延续"安全实用主义"。

#### C.5.2 字节流与交互细节

**正确工作的场景**：`\r` 覆盖式进度条、curl/wget 百分比、htop/top 刷新、vim 光标、颜色、`isatty(1)` 检测、密码输入回显控制。

**实现要点**：

| 要素 | 处理 |
|---|---|
| 不按行缓冲 | 按字节块聚合：4KB 或 50ms（满任一即 pub），确保进度条流畅 |
| 终端尺寸 | ctl 在 `attach` 载荷中带初始 cols/rows；之后监听 `SIGWINCH` pub 到 `.resize` |
| Ctrl-C | ctl 捕获本地 Ctrl-C，发一条 `kill` 命令（发 SIGINT 到进程组），不依赖字节透传 |
| 颜色 / TTY 检测 | PTY 即真终端，`isatty` 返回 true，程序自然输出 ANSI 序列 |
| 断线 | PTY 不入流（B.3），重连后只看到新字节；进程本身仍在跑（agent 未死） |

**不支持 / 会观感差的场景**：
- 极高速输出（如 `cat /dev/urandom | base64`）：NATS 限速导致丢字节，和 SSH 同样丑，不会崩。
- 二进制输出到 stdout（`cat image.png`）：终端乱码，和 SSH 一样。
- 交互式 TUI 在重连瞬间：PTY 状态丢失，需要手动刷新（`Ctrl-L` 或退出重进）。

---

## D. 状态机

本章为 session / node / process / port 四类实体建模状态与转移事件。对账协议（§G）等价于"某状态下某事件到达、应跳向哪里"。

### D.1 session

```
     [session.create]
           │
           ▼
       ┌────────┐   session.rm 第一步      ┌──────────┐   第二/三步完成
       │ ACTIVE │ ────────────────────────>│ DELETING │ ──────────────────> [DESTROYED]
       └────────┘   （SQLite 提交墓碑；      └──────────┘   （流 DELETE + SQLite
                     C.1 第 6 条前置校验                     真删 + sys.events 广播）
                     从此拒所有 req）
```

- **ACTIVE**：正常使用态。
- **DELETING**：墓碑态；tetherd 已提交 `sessions.state='DELETING'`，但 JetStream 流 / SQLite 从表尚未清理完。此态下所有 ctrl/cmd req 被 C.1 第 6 条立即拒（`session_not_found_or_deleting`）；tetherd 重启也会扫到 DELETING 继续推进清理（§G.2）。
- **DESTROYED**：DB 行和流都已删干净；等同于"不存在"。

三阶段清理见 §H.3。v1 不设暂停/归档——要停则 rm、要留则不 rm。

### D.2 node（核心）

```
                  [agent register 成功]
                        │
                        ▼
  (不存在) ──────>   ┌────────┐ ──(5s 无 heartbeat)──> ┌───────┐
                    │ ONLINE │                         │ STALE │
                    └────────┘ <─(heartbeat 恢复)───── └───────┘
                     ▲    │                                │
                     │    │(agent unregister)              │(持续 60s 无 heartbeat)
                     │    │                                ▼
        (agent 重连) │    │                           ┌─────────┐
                     │    └──────────────────────────>│ OFFLINE │
                     └───────────────────────────────┤         │
                                                      └─────────┘
                                                           │
                                                           │(owner kick / session rm)
                                                           ▼
                                                      [REMOVED]（SQLite 删行；该 node
                                                       的 port 全部 FREED；process 凝固）
```

| 状态 | 定义 | 命令行为 |
|---|---|---|
| **ONLINE** | 过去 5s 内收到 heartbeat | 命令直达 |
| **STALE** | 5 – 60s 未收 heartbeat | tetherd 暂存命令，TTL 30s；超时 ctl 收 timeout |
| **OFFLINE** | ≥ 60s 未收 heartbeat | tetherd 立即拒绝命令（ctl 报错）；数据与端口保留（A11 被动语义） |
| **REMOVED** | owner 主动清理 | SQLite 删除，端口 FREED，进程凝固 |

**阈值**：heartbeat 周期 5s；STALE = 1×；OFFLINE = 12×（60s）。

**关键不变式**：STALE 和 OFFLINE **不做任何清理**，只改变命令路由行为。只有 REMOVED 动数据。

### D.3 process

```
   [run/exec 受理]
        │
        ▼
    ┌─────────┐  ← kill ────────────┐
    │ RUNNING │                     │
    └─────────┘                     │
     │       │                      │
     │       │(agent 上报 exit 事件)│
     │       ▼                      │
     │   ┌────────┐                 │
     │   │ EXITED │                 │
     │   │ (rc=N) │                 │
     │   └────────┘                 │
     │                              │
     │(所在 node 进 OFFLINE)         │
     ▼                              │
    ┌──────┐                        │
    │ LOST │                        │
    └──────┘                        │
     │                              │
     │(agent 重连上报本地进程清单)    │
     ├──仍在跑──> RUNNING           │
     └──已死────> EXITED            │
                                    │
   (node REMOVED：所有 process       │
    凝固，不再更新状态) ─────────────┘
```

| 状态 | 定义 |
|---|---|
| **RUNNING** | agent 报告进程在跑 |
| **EXITED** | 进程已结束，记录 rc（退出码，含信号退出如 SIGKILL→137） / 结束时间 / 运行时长 |
| **LOST** | 进程所在 node 暂时 OFFLINE；重连对账后必跳 RUNNING 或 EXITED |

**保留策略**：EXITED 不自动回收（存储成本忽略不计；历史可查对调试重要）。owner 可显式 `tether ps --prune` 清理。

**`tether ps` 默认行为**：只列 RUNNING；加 `-a` / `--all` 才含 EXITED（参考 `docker ps` 语义）。

### D.4 port

```
   [expose 受理]
        │
        ▼
   ┌──────────┐   (所属 node OFFLINE ≥ 15 min)      ┌─────────┐
   │ALLOCATED │ ──────────────────────────────────> │ REVOKED │──端口号立即回池（可被新 expose 复用）
   └──────────┘                                     └─────────┘
      │                                                 │
      │(owner revoke / session rm / expose-rm)          │(agent 重连对账：token 已失效 → 清本地)
      ▼                                                 └─ 期间 agent 若仍在线，即 pub revoke 指令
   [FREED]                                                 强制 frpc 丢弃该 proxy
      └─端口号同样立即回池
```

| 状态 | 定义 | 端口号是否占用 |
|---|---|---|
| **ALLOCATED** | tetherd 已分配、frps 已接受、agent 已知 | 是 |
| **REVOKED** | tetherd 单方面作废（常因 node 长时 OFFLINE 让位资源）；token 作废、记录保留供 audit 查询 | **否**（立即回池） |
| **FREED** | owner 主动 `expose-rm` 或 session rm 释放 | **否**（立即回池） |

**回收阈值 T = 15 分钟**：比 OFFLINE 60s 宽松，避免短暂出差丢端口；又避免 agent 永久失联永占端口。一旦进 REVOKED，端口号立即可复用——若原 agent 后来回连，其 token_hash 对不上 SQLite 新主，被 frps 插件钩子拒绝，agent 上报 `expose-failed` 后清本地（见 F.6）。

### D.5 实体依赖与级联

```
  session ──包含──>  node  ──包含──>  process
                      │
                      └──持有──>  port
```

| 级联事件 | 连锁效应 |
|---|---|
| `session rm` | 所有 node → REMOVED；所有 process 凝固；所有 port → FREED |
| `agent kick N` | node N → REMOVED；N 上 process 凝固；N 的 port → FREED |
| node OFFLINE ≥ 15 min | N 的 port 逐个 → REVOKED（node 本身仍 OFFLINE，不变 REMOVED） |

**OFFLINE vs REMOVED**：OFFLINE 保留一切等重连；REMOVED 清理一切不再期待重连。二者语义严格不同。

---

## E. 鉴权与权限

### E.0 Session 激活模型（conda-like）

**核心原则**：一个 ctl 进程（shell 终端）同时只激活一个 session，类比 `conda activate`。不存在"一个面板管理多 session"。

**命令合并规则**：`login` 和 `join` 统一为 `tether login` —— 都是"进入一个 session"的动作，是否首次靠 `--pin` 识别。

#### `tether login` 的三种调用形态

| 调用 | 语义 | 对成员关系的影响 |
|---|---|---|
| `tether login` | 纯认证，不激活任何 session | 无 |
| `tether login -s S` | 认证 + 激活 S（要求已是成员） | 无；非成员时报错 `not a member of session 'S'` |
| `tether login -s S --pin X` | 认证 + 校验 PIN + 首次加入 S + 激活 S | 成功则把当前 FP 加入 S 成员表（via=pin） |

**为什么合并**：用户侧的心智模型就是"我要进 S"；"第一次进" vs "后续进"的分别是实现细节，不该暴露为两个命令。合并后 99% 场景只用 `tether login -s S`（带不带 `--pin` 根据是否首次）。

#### 状态流转

```
  未登录 NATS
     │  tether login            （纯认证，不激活）
     │  tether login -s S       （已是成员 → 认证 + 激活）
     │  tether login -s S --pin X   （首次 → 认证 + PIN 校验 + 加入 + 激活）
     ▼
  已认证 / 未激活
     │  可用命令：session create / list / logout / agent install
     │  NATS JWT 仅含 ctrl.session.* 与 sys.events
     │
     │  tether session create -s NEW    →  创建并自动激活 NEW
     │  tether login -s S [--pin X]     →  激活 S
     ▼
  已激活 session = S
     │  可用命令：run / exec / expose / ps / kill / tag
     │            + 若 owner(S)：session rm / kick / rotate-pin
     │  提示符：(S) user@host$
     │
     │  tether logout            →  清除激活
     │  tether login -s T        →  直接切换（无需先 logout）
```

#### 激活状态的存储

| 载体 | 优先级 | 性质 |
|---|---|---|
| `TETHER_SESSION` 环境变量 | 高 | 每个 shell 独立；多终端各自登录不同 session 时必需 |
| `~/.tether/current_session` 文件 | 低 | env 未设时的默认；适合单 shell 用户 |

`tether login -s S [--pin X]` 的副作用：
1. 写文件 `~/.tether/current_session = S`
2. 从 TTY 直接调用：打印 `export TETHER_SESSION=S` 供用户 `eval`
3. 用户用 `eval "$(tether shellinit bash)"` 做集成：自动完成 export
4. 首次 join（带 `--pin`）时额外在 SQLite 写成员表

多终端独立：terminal A `tether login -s lab`、terminal B `tether login -s prod` —— 靠 `TETHER_SESSION` 互不干扰。

#### 命令总表（登录/激活相关）

| 命令 | 含义 |
|---|---|
| `tether login` | 认证，不激活 session |
| `tether login -s S` | 认证 + 激活 S（已是成员） |
| `tether login -s S --pin X` | 认证 + PIN 校验 + 加入 S + 激活 S（首次） |
| `tether login -s T`（已激活 S 时） | 直接切换至 T（= logout + login -s T） |
| `tether logout` | 清空 TETHER_SESSION + 删状态文件 + 断开 NATS |
| `tether ctx` | 打印当前激活 session（未激活则空） |
| `tether shellinit {bash,zsh,fish}` | 输出 shell 集成脚本（PS1 钩子） |

注：**不再有 `tether session join` 和 `tether session switch`** —— 功能全部并入 `tether login`。

#### 未激活时能做什么

| 命令 | 原因 |
|---|---|
| `tether session create -s NEW` | 创建完自动激活 NEW |
| `tether session list` | 看自己能进哪些 session |
| `tether login [-s S [--pin X]]` / `tether logout` / `tether ctx` | 激活状态管理 |
| `tether agent --session <sid>` / `tether agent status` / `tether agent stop` / `--install-user-service` | agent per-machine per-session 的 daemon（K.1），和 ctl 登录态无关 |

其余业务命令（run/exec/expose/ps/kill/tag/session rm/kick/rotate-pin）**必须已激活 session**，否则 ctl 本地检查失败并提示 `error: no active session. run: tether login -s <name>`。

#### agent 侧无"激活"概念

agent 是 per-machine per-session 的进程：一台机器可以同时跑多个 agent 进程（各自绑定不同 session），但**一个 agent 只对应一个 session**（requirements A2），由启动时 `~/.tether/agent/<sid>/agent.yaml` 里写死的 `session_id` 决定。

同机器接入多 session 就开多 agent：
```bash
tether agent --session lab    # 绑 lab，写 ~/.tether/agent/lab/
tether agent --session prod   # 绑 prod，写 ~/.tether/agent/prod/
```

每个 agent 独立的 PID 文件、日志、frpc 实例、端口列表，互不干扰。

### E.1 身份模型

**身份 = 公钥指纹（fingerprint）**。无用户名/密码/账号概念。

`identity` 记录：
| 字段 | 说明 |
|---|---|
| `pubkey_fp` | `SHA256:<base64>=`，主键 |
| `pubkey_blob` | 32 字节 ed25519 公钥原始字节 |
| `display_name` | 昵称（审计里人类可读；可重复、可空） |
| `created_at` | |
| `notes` | 可选备注 |

**性质**：
- 身份**全局唯一**但**不绑定 session** —— 同一指纹可同时拥有多 session 成员资格。
- **激活边界**：ctl shell 一次仅激活一个 session（§E.0）；要同时在两个 session 操作，需开两个 shell 分别 login。
- 凭据 = **私钥文件**（默认 `~/.ssh/id_ed25519` 或 `~/.tether/key`）。
- 丢私钥 = 丢身份。v1 无自助恢复；走 owner kick + 重新 PIN join（见 E.4）。

### E.2 `tether login -s S` 的统一流程

`tether login` 是进入 session 的**唯一动作**（见 §E.0 合并规则）。是否首次加入由 `--pin` 标志和 tetherd 侧的成员表决定，用户心智无需区分。

```
用户侧
  tether login -s S [--pin X]
     │
     ▼
ctl 本地
  读私钥 → 准备 nkey → CONNECT NATS，header 带 tether-session: S, tether-pin: X?
     │
     ▼
NATS ─── $SYS.REQ.USER.AUTH ──>  tetherd (auth_callout)
                                    │
                                    │── 查成员表：FP 是否在 S？
                                    │
                                    ├── 是：
                                    │     ├── 忽略 --pin（可选，不校验）
                                    │     └── 下发含 S 权限的 JWT → 激活成功
                                    │
                                    └── 否：
                                          ├── 无 --pin → 拒绝 connection，回错 "not a member"
                                          │
                                          └── 有 --pin：
                                                ├── Argon2id 校验 PIN
                                                │     ├── 通过：
                                                │     │     ├── FP 写入 S 成员表（via=pin）
                                                │     │     ├── 写 sys.events (member_joined)
                                                │     │     └── 下发含 S 权限的 JWT → 激活成功
                                                │     └── 失败：
                                                │           ├── 拒绝 connection
                                                │           ├── 写 sys.events (pin_failed)
                                                │           └── 按 E.6 速率限制计数
```

**subject**：`auth_callout` 走 NATS 原生的 `$SYS.REQ.USER.AUTH`，没有应用层 `ctrl.session.join.req` —— B.1 / B.2 里不保留 `join` 相关 subject。

**owner 初始化特例**：`tether session create -s S` 时 tetherd 把创建者 FP 登记为 S 的 owner，并自动激活 S（ctl 侧在命令完成后写 `TETHER_SESSION=S`），无需再 login。

### E.3 权限三档（激活态维度）

| 角色 | 资格获取 | 可做 |
|---|---|---|
| **未登录** | 无有效私钥 / 未认证 | 仅 NATS 握手；失败则断连 |
| **已认证 / 未激活** | `tether login` 无 `-s` | `session create / list / login -s X` + `agent install/*` + `ctx/logout` |
| **成员（member）已激活 S** | FP 在 S 成员表 + `tether login -s S` | S 内：run / exec / expose / ps / kill / tag；订 S 的 ev / audit / pty |
| **所有者（owner）已激活 S** | 创建 S 者（v1 不提供转让） | member 全部 + `session rm / kick / rotate-pin`（均针对 S） |

**关键规则**：
- **激活**是 per-ctl-shell 状态，不是身份属性 —— 同一私钥在 A shell 激活 `lab`、B shell 激活 `prod` 完全独立。
- **owner 是 per-session**，无全局超管。broker 运维通过 SSH 直连 VPS 管理，不在 tether 协议内。
- 成员资格跨 session 可叠加（Alice 可同时是 `lab` 的 owner、`prod` 的 member），但激活态一次一个。

### E.4 PIN 与密钥运维

**PIN 参数**（与 requirements §6.3 对齐）：
- 字符集：ASCII 可打印（`0x20–0x7E`），拒绝中文 / emoji / 控制字符。
- **v1 不做长度 / 复杂度校验**——owner 自定，可任意 ASCII 可打印串；系统随机生成时默认给一段易输入的字母数字。
- 存储：Argon2id + 随机 salt，PHC 格式（`$argon2id$v=19$m=65536,t=3,p=2$<salt>$<hash>`），参数固定为 `m=64MiB, t=3, p=2`（OWASP 推荐基线）。
- 可用次数：不限。泄漏即由 owner `rotate-pin` 刷新（老 PIN 立即失效，已 join 成员不受影响）。
- owner 自担扩散责任（requirements A3）。

**成员密钥丢失**：
```
owner 查成员表找到 B 的 FP
  → tether session kick -s S --fp SHA256:xxx=
  → B 生成新密钥
  → owner 发新 PIN
  → B 重新 join
```

**owner 密钥丢失（v1）**：
- 无 CLI 恢复机制。
- broker 运维**直接改 SQLite**将新 FP 标为 owner（紧急通道，文档化为 FAQ）。
- v2 候选：recovery code / 双 owner；一期不做。

### E.5 auth_callout 数据流

```
客户端 B                     NATS 服务端                     tetherd (auth-callout svc)
   │──CONNECT (nkey=B)──────>│                                  │
   │                         │──$SYS.REQ.USER.AUTH {nkey: B}──>│
   │                         │                                  │─SQLite 查询：
   │                         │                                  │   B 所属 session 集合
   │                         │                                  │   owner 身份标志
   │                         │                                  │   display_name
   │                         │<──user JWT { permissions } ─────│
   │<─CONNECT 完成 + JWT 生效┤                                  │
```

**性质**：
- 握手一次；权限贯穿整个连接生命周期。重连重走握手，允许成员身份变化即时生效。
- tetherd 即 `$SYS.REQ.USER.AUTH` 的订阅者（NATS 2.10+ 原生机制）。
- tetherd 挂掉 → 新连接拒绝；已建立连接不影响直至其自然断开。恢复流程见 §G。

### E.6 v1 未处理的威胁（明写不管）

| 威胁 | 一期处理 |
|---|---|
| tetherd 进程被攻陷 | 不隔离；= broker 沦陷 |
| broker VPS 内核沦陷 | 同上 |
| 本地私钥文件权限弱（`0644`） | 启动时 warning，不强制 |
| FP 枚举 / 时序攻击 | 不做恒定时间查询；FP = SHA256 输出，搜索空间 2²⁵⁶ 无意义 |
| PIN 暴力 | 速率限制：每 IP 每分钟 ≤ 10 次尝试；不做账户锁定 |

v2+ 候选：recovery code、双 owner、tetherd 进程沙箱、mTLS 双向认证、审计签名。

---

## F. 数据面 frp 集成

### F.1 frps 嵌入 tetherd

tetherd 内嵌 `github.com/fatedier/frp/server` 包，作为 goroutine 启动 `Service`；不另起 frps 进程。

```go
frpsCfg := v1.ServerConfig{
    BindAddr: cfg.Frp.BindAddr,        // 来自 broker.yaml.frp.bind_addr，默认 "0.0.0.0"（§A.3 split ports）
    BindPort: cfg.Frp.ControlPort,     // 默认 7000，来自 broker.yaml.frp.control_listen
    // 业务 remote_port 监听范围：cfg.Frp.PortRange（默认 14000-14999）
    Transport: v1.ServerTransportConfig{
        TLS: v1.TLSServerConfig{ Force: true /* cert/key 来自 broker.yaml.frp.tls */ },
    },
    Auth: v1.AuthServerConfig{Method: "token", Token: ""},  // frp 原生静态 token 不用，走插件钩子
}
srv, _ := server.NewService(frpsCfg)
go srv.Run()
```

**监听面**：frps 直接对外监听 `:7000`（frpc control）与 `:14000-14999`（业务 remote_port），不经 Caddy。A.3 / P9 的 split ports 模型要求 `BindAddr` 必须可被外部 agent 访问；默认 `0.0.0.0`，若 broker 有多网卡/只想暴露单网卡可在 `broker.yaml.frp.bind_addr` 指定。绑 `127.0.0.1` 会让所有 agent 连不到 frpc control → 数据面不可用。

通过 frp 的插件钩子（`HTTPPluginOptions` + 自定义 handler）劫持"proxy 注册"，由 tetherd 决定每条 proxy 放行/拒绝。

**收益**：端口分配 / token 生成 / proxy 放行 / audit 写入在同一个 SQLite 事务内完成，无跨进程 RPC。

**代价**：tetherd 升级连带 frp 版本升级；frp 有严重漏洞必须发 tetherd 补丁。

### F.2 agent 单 frpc 实例 + proxy 热加载

每个 agent 持有**一个** frpc（内嵌或子进程），所有 expose 复用同一条到 broker 的长连接。新增 / 删除 expose 通过 frp reload 接口热加载 proxy 配置。

- 资源占用小；一条连接承载多个 proxy。
- 失败域单一：连接断则所有 proxy 同时断、同时恢复 —— 语义清晰。
- 一个 agent = 一个 session（§E.0 / requirements A2），frpc 与 agent 同生命周期。

### F.3 端口按需分配

broker 侧公网端口从 `broker.yaml` §A.3 的 `frp.port_range`（默认 `14000-14999`，1000 个）内分配。分配模式由 wire 字段 `ExposeReq.remote_port`（P12，additive，`omitempty`）决定：

- **自动分配（默认，`remote_port` 省略/0）**：`expose` 到达 → tetherd 从段内找**第一个空闲端口** → 写 SQLite 事务（`port_allocations`） → 返回 agent。
- **指定分配（P12 `tether expose --remote-port <P>`，`remote_port` 非 0）**：在**同一** SQLite 事务内，按 name 唯一性检查**之后**：
  - 端口必须落在 `frp.port_range` 带内，否则拒 `port_out_of_band`；
  - 带内但已有 `state=ALLOCATED` 行 → **硬拒** `port_taken`，**绝不回退**自动分配（要别的端口须显式改值重试）；
  - 占用判定只看 `state=ALLOCATED`，故 REVOKED/FREED 的旧端口号**可被重新指定**；
  - 原子性由 `port_allocations(port) WHERE state='ALLOCATED'` 的**部分唯一索引**保证：并发抢同口恰一个 INSERT 成功，其余 `port_taken`（不依赖应用层 check-then-act）。
  - **不 bump proto**（仅 additive 字段）。同 proto 跨版本下若 ctl 新而 broker 旧，旧 broker 忽略 `remote_port`、**静默退回自动分配**且无报错——无法检测，记为已知限制，**不做能力协商**（部署侧以同版本灰度保证）。
- 回收：一旦进 REVOKED，端口号立即回池可复用（见 §D.4）；FREED 亦回池。agent 后续若拿着失效 token 回连，frps 插件钩子按 `port_allocations.state≠ALLOCATED` 直接拒绝（见 F.4）。

### F.4 token：tetherd 单边签发，一对一绑定

**expose 命令端到端流程**：

```
ctl  tether expose --local 8888 --name jupyter [--remote-port 14022]
  │
  └──pub s.S.cmd.by.<A>.node.N.expose.req{…,remote_port}─────>  tetherd
                                             │
                                             │──SQLite 事务：
                                             │    · 选 port P：remote_port 非 0 则用该端口（校验带内 + 未被 ALLOCATED 占用，否则 port_out_of_band / port_taken）；否则找第一个空闲 → P=14022
                                             │    · 生成 token T（32 字节 URL-safe base64）
                                             │    · 写 port_allocations{sid,nid,port,token_hash,local_port,name,state=ALLOCATED,created_by_fp}
                                             │    · 写 audit.call
                                             │
                                             │──pub …expose.req.forwarded{P,T,local_port,name}──>  agent
                                                                                                     │
                                                                                                     │─frpc 热加载新 proxy：
                                                                                                     │   ServerAddr=<broker>:7000  （= broker.yaml.frp.control_listen）
                                                                                                     │   Auth.Token=T
                                                                                                     │   LocalPort=8888
                                                                                                     │   RemotePort=P
                                                                                                     │   ProxyName=jupyter-S-N
                                                                                                     │
                                                                                                     │─frpc 连 frps，出示 T
                                                                                                     │
             frps 插件钩子 ──查 SQLite：T 对应的 port 与声明的 P 是否一致；state=ALLOCATED？──> allow/deny
                                             │
                                             │─允许 → 绑定 :14022 监听外部流量
                                             │
                                             │──pub s.S.ev.port.14022.allocated──>  ctl / 其他成员
```

**token 性质**：
- 32 字节随机 URL-safe base64。
- **一对一**：绑定 (agent, port) 组合；agent 不能用此 token 注册其他端口。
- **存储分界**：
  - broker 侧 SQLite 只存 `token_hash`（SHA256），**不存明文 token**、不入 audit。
  - agent 侧需要明文 token 向 frps 出示，落盘到 per-session 路径 `~/.tether/agent/<sid>/state.json`（0600；见 I.2 / K.1），供 agent 重启后 frpc 自动重连。
- 生命周期 = 端口生命周期：REVOKED / FREED 时 token 作废，agent 对账（§G.1）时被指令清除本地记录。

### F.5 数据面 TLS

frpc ↔ frps 连接启用 frp 原生 TLS（`tls_enable=true`），**由 frps 自管证书**，不经 Caddy：

- frps 监听 `:7000`（frpc control）与 `:14000-14999`（业务 remote_port）；TLS 仅作用于 `:7000` 的 frp 控制信令，业务 remote_port 上的流量本身由业务协议决定是否加密。
- frps 证书来源：`broker.yaml.frp.tls`（未写）可与 443 共用 domain 证书文件，或单独指定；否则 frps 回落到自签（仅 v1 够用，业务通信安全由业务自己负责）。
- Caddy **不参与** frp 流量。v1 不在 443 上复用 frpc control，理由见 §A.3。

**数据面不做应用层加密**。暴露的业务（Jupyter、Postgres 等）若需要 HTTPS，用户在业务层自管；frp 仅保障 frpc↔frps 控制信令的 TLS。

### F.6 断联与恢复语义

| 场景 | 行为 |
|---|---|
| frpc ↔ frps 短暂抖动（< 30s） | frpc 内置自动重连；proxy 不变；外部访问短暂不可达 |
| frpc 永久失联（agent 死机） | node 进 OFFLINE；≥ 15 分钟后端口进 REVOKED、端口号立即回池（§D.4） |
| agent 重启，端口仍 ALLOCATED | agent 读 `state.json` 取回明文 token + 端口表；frpc 凭 token 自动重建 proxy；tetherd 侧对账通过（§G.1） |
| agent 重连但端口已 REVOKED（或已被其他 agent 抢占复用） | frps 插件钩子查 SQLite 拒绝 → agent 收 `expose-failed` → tetherd pub `ev.port.X.revoked` → agent 清理本地 state.json 条目 → 用户必须重 `expose`（requirements A8） |
| tetherd 重启 | SQLite 是权威；重启后重建 frps 插件上下文；ALLOCATED 端口的 token_hash 仍有效；agent 侧 frpc 只需重连 `:7000` |

### F.7 v1 不做

- 带宽限制 / 配额。
- frp HTTP 域名模式（`jupyter.broker.example.com` 子域名多路复用）—— 一期仅 TCP 端口模式。
- 隧道内容审计 / 内容检查。
- UDP 代理。
- **文件搬运原语（push/pull）**：requirements 的北极星是"控制 + 观测"而非"做文件分发通道"。一期用户如需传文件，直接 `expose` 本地 HTTP/SCP/rsync 端口即可；v2 视需要再评估是否内建 push/pull（若做，应作为 "file transfer v2" 独立设计，重走 subject / audit / 背压方案，不是简单加两个 verb）。

### F.8 查看与管理：统一并入 `tether ps`

**取消独立的 `tether expose ls / show`**。端口和进程都是"节点此刻的状态"，同一命令查：

```bash
tether ps                    # 当前激活 session，全部 node 的 processes + ports
tether ps -n lab-1           # 单节点
tether ps -s OTHER           # 临时查别的 session（要求是该 session 成员；不推荐，标准做法重新 login）
tether ps -a                 # 含 EXITED 进程（-a 唯一语义；无"跨 session"含义）
tether ps --procs-only       # 仅进程
tether ps --ports-only       # 仅端口
```

**输出示例**（激活 session = lab）：

```
SESSION: lab   NODE: lab-1   STATE: ONLINE   HB: 2s ago   VERSION: 0.1.0

PROCESSES
  PID          CMD                        STATE         STARTED
  01hzxk...    python train.py            RUNNING       2026-04-18 09:32
  01hzxm...    sleep 30                   RUNNING       2026-04-18 10:10
  (加 -a 时追加)
  01hzxj...    broken.sh                  EXITED(1)     2026-04-18 08:40

PORTS
  NAME         LOCAL    PUBLIC                         STATE       CREATED_BY
  jupyter      :8888    broker.example.com:14022       ALLOCATED   alice
  ssh          :22      broker.example.com:14023       ALLOCATED   alice

SESSION: lab   NODE: lab-2   STATE: OFFLINE   HB: 45min ago

PROCESSES
  01hzx...     my-service                 LOST          2026-04-17 22:00

PORTS
  postgres     :5432    —                              REVOKED     alice
```

**命令集对称**：

| 动作 | 命令 |
|---|---|
| 创建暴露 | `tether expose --local 8888 --name jupyter [--remote-port 14022]`（省略 = 自动取最低空闲；指定 = 该带内端口，占用则 `port_taken` 硬拒，见 F.3） |
| **撤销暴露** | `tether expose rm --name jupyter` 或 `--port 14022` |
| 杀进程 | `tether kill --pid 01hzxk...` |
| 查进程 + 端口 | `tether ps` |

**撤销权限**：发起人可撤销自己发起的 expose；owner 可撤销 session 内任何 expose。

**ps 不经 agent**：tetherd 是权威状态持有者（SQLite 有 `processes` 和 `port_allocations` 表），`tether ps` 直接查库返回；OFFLINE node 的进程显示 LOST、端口显示 ALLOCATED/REVOKED，一致可见。

**subject**：
- `ctrl.by.<actor>.s.<sid>.ps.req` — 查 processes + ports 聚合结果（tetherd 直接查库返回，不经 agent forwarded）
- `s.<sid>.cmd.by.<actor>.node.<nid>.expose.req` — 创建（已在 C.2；verb=`expose`）
- `s.<sid>.cmd.by.<actor>.node.<nid>.expose-rm.req` — 撤销（verb=`expose-rm`，单 token 保持固定 token 数）

### F.9 port_allocations 表（schema 预告，最终落到 §H/requirements）

```
port_allocations
  port            INTEGER PRIMARY KEY         -- broker 侧公网端口号
  sid             TEXT    NOT NULL            -- 所属 session
  nid             TEXT    NOT NULL            -- 所属 node
  name            TEXT    NOT NULL            -- 用户起的名字；session-scope unique per node
  local_port      INTEGER NOT NULL            -- agent 本地端口
  token_hash      TEXT    NOT NULL            -- SHA256(token)；token 本身不存
  state           TEXT    NOT NULL            -- ALLOCATED | REVOKED | FREED
  created_by_fp   TEXT    NOT NULL            -- 发起人 pubkey fp（用于撤销权限判定）
  created_at      TIMESTAMP
  revoked_at      TIMESTAMP
```

---

## G. 对账协议

**对账（reconciliation）** = 两方状态分歧时核对出权威版本。系统内三类场景：

| 场景 | 谁对谁 | 触发 |
|---|---|---|
| **G.1 agent 重连** | agent ↔ tetherd | agent 从 OFFLINE/STALE 恢复 ONLINE，或首次启动注册 |
| **G.2 tetherd 重启** | tetherd ↔ 持久化状态 + 在线连接 | tetherd 进程冷启或重启 |
| **G.3 ctl 重连** | ctl ↔ tetherd | ctl 的 NATS 连接自动重连后 |

SQLite 是所有对账的最终权威。

### G.1 agent 重连对账

**首包**：agent 重连 CONNECT 成功后发 `ctrl.s.<sid>.node.<nid>.register.req`（有 reply），携带：
- `agent_version`、`proto_version`、`os/arch`
- `boot_id`（读 `/proc/sys/kernel/random/boot_id`；Linux 唯一；容器内亦有效），用于识别"机器有没有在这期间重启过"
- `local_processes[]`：`{pid, state: running|exited, rc?, started_at, start_time_ticks}`——数据源：`/proc/<pid>/stat` 第 22 字段（clock ticks），配合 `boot_id` 做 PID 复用防误判（同一 PID 数值但 start_time_ticks 不同 = 新进程）
- `local_ports[]`：`{port, name, local_port, token_hash}`——数据源：`state.json.port_tokens`（§I.2），agent 只报 `token_hash`（SHA256），明文 token 不上线

**tetherd 核对规则**（SQLite 权威，agent 本地可被覆盖；PID 验活用 `(boot_id, pid, start_time_ticks)` 三元组）：

进程维度：
| tetherd 侧状态 | agent 清单状态 | 结果 |
|---|---|---|
| RUNNING | 在清单 running 且 `(boot_id,start_time_ticks)` 匹配 SQLite | 保持 RUNNING |
| RUNNING | 在清单 running 但 `(boot_id,start_time_ticks)` 不匹配 | **判定为 PID 复用**：原进程 → EXITED(rc=-1, reason=missed-exit)；新进程视为 SQLite 无记录（孤儿处理） |
| RUNNING | 在清单 exited | EXITED（记 rc） |
| RUNNING | 不在清单 | **EXITED(rc=-1, reason=missed-exit)** + `audit.proc{kind:reconciled_closed}` |
| LOST | 在清单 running 且三元组匹配 | 升级 RUNNING |
| LOST | 在清单 running 但三元组不匹配 | 原进程 → EXITED(rc=-1, missed-exit)；新进程视为孤儿 |
| LOST | 在清单 exited | EXITED（记 rc） |
| LOST | 不在清单 | EXITED(rc=-1, missed-exit) |
| SQLite 无记录 | agent 声称持有 | **孤儿进程 → v1 直接 kill（SIGTERM+5s+SIGKILL）** |

端口维度：
| tetherd 侧状态 | agent 持 token | 结果 |
|---|---|---|
| ALLOCATED | 有且 port 号未被别人抢 | 保持；放行 frpc 重连 |
| REVOKED | — | 指令 agent 删 proxy（reply 列 `revoke_ports`） |
| SQLite 无记录 | agent 声称持有 | 指令 agent 删（视为 orphan proxy） |

**reply 载荷**：
```jsonc
{
  "accepted_processes": ["..."],
  "reconciled_processes": [{"pid":"01hzxk","new_state":"EXITED","rc":0}],
  "keep_ports": [14022],
  "revoke_ports": [14023],
  "drop_processes": ["01hzxm..."]
}
```

**收尾**：tetherd 写 `node.state = ONLINE`，pub `ev.node.<nid>.state`；agent 按 reply 应用变更后开始常规 heartbeat。

### G.2 tetherd 重启对账

```
启动
  ├─① 读 SQLite：加载 sessions / members / nodes / processes / port_allocations
  │   node 状态按 last_heartbeat_at 推断：
  │     Δ < 5s  → ONLINE      Δ < 60s → STALE      Δ ≥ 60s → OFFLINE
  │
  ├─①b **扫描 DELETING session 续跑清理**（见 H.3）：
  │     SELECT sid FROM sessions WHERE state='DELETING'
  │     对每一行继续执行 §H.3 ②③（DELETE stream + DELETE 关联表行）
  │     期间 C.1 第 6 条仍生效，拒绝所有走到这些 sid 的新 req
  │     兼带孤儿扫描：
  │       · SQLite 有 session(state=ACTIVE) 无 stream  → 重建空 stream
  │       · 有 stream 无 SQLite session              → 删 stream
  │
  ├─② 启动内嵌 frps；加载 port_allocations；等待 agent 重连走 G.1 对账
  │
  ├─③ 连 NATS，注册 $SYS.REQ.USER.AUTH 订阅
  │   继续消费 s.*.audit.* / ctrl.s.*.node.*.heartbeat / s.*.ev.*
  │
  ├─④ **旧 JWT 不强制吊销**（选项 β）
  │   老连接用老 JWT 继续直到自然断开；简单无感
  │   代价：短时（分钟级）内老连接权限可能与新状态不匹配；
  │        DELETING session 由 C.1 第 6 条兜底（应用层 precheck 独立于 JWT 时间线）
  │
  ├─⑤ pub sys.events {type: tetherd_restarted, version, started_at}
  │   订阅者仅作信息性感知，不触发动作
  │
  └─⑥ 进入稳态；首次 agent 心跳到达时更新 last_heartbeat_at
```

**不对账的事项**：
- 重启期间丢心跳 → 按正常 STALE/OFFLINE 延后确认。
- 重启期间丢命令 → ctl 已收 timeout，用户决定重发；无命令重放机制。
- 重启期间丢事件（ev.* best-effort）→ 可查 JetStream `history-<sid>` 回溯。

**容错评估**：tetherd 重启 ≤ 10s → 最坏 STALE 判定窗口 ≈ 15s << OFFLINE 阈值 60s，无误杀。

### G.3 ctl 重连对账

ctl 无持久状态，重连流程：
1. NATS Go 客户端自动重连（指数退避无上限）。
2. 重连成功走完整 auth_callout；`TETHER_SESSION` env 未变则自动保持激活态。
3. 重订阅 `s.S.ev.*` / `s.S.pty.*` 等关心的主题。
4. **PTY 字节流断线期间永久丢失**（§B.3），用户只见新输出。
5. **事件（ev.*）best-effort 丢失**，需回溯走 `s.S.audit.*`（JetStream 持久化）。

无协议层追赶机制；关键动作的可追溯性由 audit stream 提供。

### G.4 触发矩阵

| 触发 | 类型 | 主动方 |
|---|---|---|
| agent OFFLINE/STALE → ONLINE | G.1 | agent |
| agent 首次启动注册 | G.1（无前置状态） | agent |
| tetherd 冷启 / 重启 | G.2 | tetherd |
| ctl NATS 重连 | G.3 | ctl |

### G.5 对账审计

G.1 每次产生 `audit.proc` / `audit.port` 记录：

```jsonc
{
  "kind": "reconciled",
  "actor": "system",
  "ts": "2026-04-18T09:32:00Z",
  "sid": "lab",
  "nid": "lab-1",
  "changes": [
    {"pid": "01hzxk", "from": "LOST", "to": "EXITED", "rc": 0},
    {"port": 14022, "from": "ALLOCATED", "to": "REVOKED"},
    {"pid": "01hzxm", "from": "unknown", "to": "killed_orphan"}
  ]
}
```

用途：事后追溯"为什么进程突然是 EXITED(-1)" —— 查审计 `kind=reconciled` 即可。

---

## H. JetStream 细化

### H.0 术语回顾

- **Stream**：磁盘上的消息日志，按 subject 过滤器持久化匹配消息。
- **Consumer**：stream 的读取游标。
  - **Durable**：游标位置存服务端磁盘，重连续读；须显式命名与清理。
  - **Ephemeral**：游标仅存内存，客户端断开即消失；重连须指定起点。

O4 已定：两条流 —— 全局 `events` + per-session `history-<sid>`。

### H.1 两条流定义

#### Stream A：`events`（全局运维）

```jsonc
{
  "name": "events",
  "subjects": ["tether.v1.sys.events"],
  "retention": "limits",
  "max_age": "30d",
  "max_bytes": 1073741824,    // 1 GiB
  "discard": "old",
  "storage": "file",
  "replicas": 1
}
```

**消息类型**：
- `session_created` / `session_destroyed` / `member_joined` / `pin_failed` / `rotated_pin` / `kicked`
- `tetherd_restarted`
- `agent_registered` / `agent_unregistered`
- `disk_pressure`（告警）
- 未来的运维指标

**订阅者**：owner ctl（查询）、运维工具（ephemeral 或自建 durable）。

#### Stream B：`history-<sid>`（per-session 审计，与 session 同生命周期）

```jsonc
{
  "name": "history-<sid>",
  "subjects": ["tether.v1.s.<sid>.audit.>"],
  "retention": "limits",
  "max_age": -1,              // 不过期
  "max_bytes": -1,            // 不限大小（requirements A10）
  "discard": "new",           // 磁盘真满时拒新不丢旧
  "storage": "file",
  "replicas": 1
}
```

**消息类型**：`audit.call` / `audit.proc` / `audit.port`（格式见 §H.5）。

**订阅者**：session 成员（ephemeral replay）、tetherd 自身不订（原因见 §H.2）。

### H.2 Consumer 策略

**全部用 ephemeral；tetherd 不自建 durable**。

| 场景 | 用法 | 说明 |
|---|---|---|
| `tether history` | ephemeral，默认从 stream 最早起 | 一次性查询，Ctrl-C 即丢 |
| `tether history --since X` | ephemeral + `OptStartTime` | |
| `tether history --follow` | ephemeral，`DeliverNew` 起点 | 实时跟新；Ctrl-C 丢 |
| 运维工具需长期追审计 | 用户自建 durable | v1 CLI 不提供 `--durable`；NATS 客户端直连 |

**为什么 tetherd 不建 durable 落 SQLite**：SQLite 已存权威实时状态（`processes/port_allocations`），与 JetStream（历史过程档案）**语义不重叠**；不双写同一份信息。

### H.3 `session rm` 原子性

**三阶段（墓碑 + 删流 + 真删）**。动机：要保证"rm 一旦开始，期间收到的任何命令立刻失败"，又不能依赖 NATS 的 JWT 实时吊销（v1 不做，JWT 有 TTL）。引入 SQLite `sessions.state = DELETING` 做墓碑；`C.1 第 6 条`在 tetherd 审核入口前置校验，处于 DELETING 的 session 任何 req 直接拒。

```
session rm S
  ├─① SQLite 事务提交墓碑：
  │     UPDATE sessions SET state='DELETING', deleting_at=now() WHERE sid=S
  │     COMMIT → 此刻起 C.1 第 6 条生效，新 req 被拒
  │     失败 → 什么都没动，用户重试
  │
  ├─② JetStream DELETE stream history-<sid>         (高失败率：磁盘 I/O)
  │     失败 → session 停留在 DELETING；tetherd 重启/手动 retry 会继续走 ②③（见 G.2 扫描）
  │     成功 → 继续
  │
  ├─③ SQLite 事务真删：
  │     DELETE FROM processes        WHERE sid=S
  │     DELETE FROM port_allocations WHERE sid=S
  │     DELETE FROM members          WHERE sid=S
  │     DELETE FROM nodes            WHERE sid=S
  │     DELETE FROM sessions         WHERE sid=S
  │     COMMIT
  │
  ├─④ pub sys.events: session_destroyed {sid, by_fp, at}
  └─⑤ pub s.<sid>.ev.session.destroyed（短暂；订阅者此时连接可能已被 kick）
```

**为什么 DELETING 要分独立阶段**：v1 不做 JWT 实时吊销，旧 JWT 在 TTL 内仍可连 NATS、pub 命令 subject —— 若直接"删流 + 删库"，期间到达的 req 可能写到已不存在的 session 上，出现孤儿 audit / 孤儿 port 分配。墓碑把"拒入新命令"的判定点上移到 SQLite + 应用层 precheck，独立于 NATS permission 时间线。

**启动时孤儿清理 / DELETING 续跑**（tetherd 重启扩展 §G.2）：
- SQLite `sessions.state=DELETING` → 继续执行 ②③，直到 session 真删或把失败日志打出来。
- SQLite 有 session 行（state=ACTIVE）+ 无 `history-<sid>` stream → **重建空 stream**，审计从此刻起。
- 有 stream + SQLite 无对应 session → **删 stream**（孤儿清理）。

### H.4 磁盘容量与告警

**events 流**：30d / 1GiB `discard=old` 足够，永不阻塞写入。

**history-<sid>**：requirements A10 "不考虑磁盘"，但需监控：
- tetherd 周期性（如每 5 min）抽样每个 stream 的 `bytes` / `msgs`，暴露 metrics。
- `tether admin disk`（owner / 运维）查看：

```
SID    SIZE       MSGS        CREATED       LAST_EVENT
lab    42MiB      18k         2026-01-10    2 min ago
prod   1.2GiB     540k        2025-06-01    5 s ago
```

- 硬盘 > 80% 占用 → tetherd pub `sys.events{type:disk_pressure}`；**不自动裁剪**。owner 自决 `session rm` 老 session 或扩容。

### H.5 Audit 消息格式

所有 audit 为 JSON，版本化字段 `v`：

```jsonc
// audit.call — 命令调用
{
  "v": 1,
  "kind": "call",              // call | admin | admin_denied | exec | reconciled
  "ts": "2026-04-18T09:32:00.123Z",
  "actor_nkey": "UABCD...",    // 不可伪造：tetherd 从 subject 的 by.<actor> 段提取
  "actor_fp": "SHA256:xxx=",   // SQLite 映射得到，便于人类识别
  "actor_name": "alice",       // display_name，仅展示用
  "session": "lab",
  "node": "lab-1",
  "req_id": "01hzxk...",
  "verb": "run",
  "target": { /* verb 相关 */ },
  "ok": true,
  "error": null
}

// audit.proc — 进程启停/对账
{
  "v": 1,
  "kind": "start | exit | reconciled_closed | killed_orphan",
  "ts": "...",
  "session": "lab",
  "node": "lab-1",
  "pid": "01hzxk...",
  "rc": 0,
  "duration_ms": 130432,
  "cmd": "python train.py"     // 仅 start 时
}

// audit.port — 端口生命周期
{
  "v": 1,
  "kind": "allocated | revoked | freed | reconciled",
  "ts": "...",
  "session": "lab",
  "node": "lab-1",
  "port": 14022,
  "name": "jupyter",
  "local_port": 8888,
  "actor_nkey": "UABCD...",    // allocated 时；kind=freed 且由用户触发时同填
  "actor_fp": "SHA256:..."     // allocated 时
}
```

**不记入审计**：PTY 字节流、`exec` 的 stdin/stdout、file 内容、token 明文、PIN 明文、私钥。

**追加日志不可编辑**：发现错误写补偿条目（如 `kind=corrected`），不原地改。

### H.6 v1 不做

- Stream 复制 / 集群（`replicas > 1`）；broker 本身单点。
- 跨 session 的全局审计聚合（每 session 独立 stream）。
- JetStream KV / Object Store（全部走 stream + SQLite）。
- 压缩（v2 打开 zstd）。

---

## I. monorepo 代码布局

### I.0 为什么 monorepo

三种角色（broker daemon = `tether serve` / agent daemon = `tether agent` / CLI）共享协议、schema、鉴权逻辑，**任何一次协议改动都要三边同步**。放同一个 git 仓库、同一个 `go.mod`，重构自由、版本天然对齐；运行时仍是**单一 `tether` 二进制**按子命令分派（见 I.1 / I.2）。v1 不对外发布 SDK，没有 polyrepo 的收益。

### I.1 顶层目录

```
tether/
├── go.mod                      # module: github.com/<org>/tether
├── go.sum
├── Makefile                    # build / test / release 入口
├── README.md
├── LICENSE
├── docs/
│   └── architecture.md
│
├── cmd/
│   └── tether/                 # 单一二进制入口（cobra 子命令树）
│       ├── main.go             #   main() + root cmd + 三层 help 分组
│       ├── serve.go            #   `tether serve [reload]` —— broker daemon
│       ├── agent.go            #   `tether agent [status|stop]` —— agent daemon
│       ├── ctl_*.go            #   login/logout/session/node/ps/run/exec/expose/history
│       └── admin_*.go          #   admin sessions/nodes/audit/evict（走本地 Unix socket）
│
├── internal/                   # 业务逻辑（Go 编译器强制私有）
│   ├── proto/                  # subject 常量 + 请求/响应 struct
│   ├── schema/                 # 审计 / 事件 JSON schema（v:1）
│   ├── auth/                   # nkeys、auth_callout、PIN(argon2id)、JWT
│   ├── storage/                # SQLite 访问层（sessions/members/nodes/processes/port_allocations；audit 不落 SQLite）
│   ├── session/                # session 生命周期、三阶段 rm（DELETING 墓碑→删流→真删）、激活态
│   ├── node/                   # node 注册 / 心跳 / 状态机
│   ├── proc/                   # process 状态机 + reconcile
│   ├── port/                   # 端口分配 / 状态机（ALLOCATED→REVOKED，15min OFFLINE 触发；REVOKED/FREED 端口号立即回池）
│   ├── pty/                    # PTY 封装（creack/pty + SIGWINCH）
│   ├── tunnel/                 # 反向 TCP 隧道（in-process yamux；spec 的 frpmgr 已替换，见 README "Architecture deep-dive"）
│   ├── jsstream/               # JetStream stream / consumer 管理
│   ├── reconcile/              # G.1 / G.2 / G.3 对账逻辑
│   ├── adminsock/              # admin Unix socket server（仅 serve 模式启用）
│   └── cli/                    # ctl / admin 子命令实现
│
├── pkg/                        # 可被外部引用的公共 API（v1 留空）
│
├── build/
│   └── goreleaser.yaml
│
├── scripts/
│   └── install.sh
│
└── test/
    └── e2e/                    # 端到端：真实 nats-server + tetherd + agent
```

**单二进制多角色**：所有角色共用一个 `tether` 二进制，用子命令区分运行模式。`tether serve` = broker daemon；`tether agent` = agent daemon；其余为 ctl / admin 命令。`consul` / `nomad` / `nats-server` 都是这个模式。**体积代价**：agent 机器上二进制约 40MB（含 frps server lib 等 broker 侧代码）而不是 15MB；在"安全实用主义"威胁模型下可接受。

**`cmd/` vs `internal/` 约定**：`cmd/tether/` 只做子命令路由 + 依赖装配；业务全在 `internal/*`。`internal/` 是 Go 语言层面的私有目录，外部 module `import` 编译失败——内部重构自由。

**单一 `go.mod`**：同一 module 保证本机所有角色 ProtoVersion 天然一致；跨机仍由 J.2 的 handshake 校验。

**`pkg/` v1 留空**：不对外暴露 SDK。若将来允许第三方实现 agent，再把 `proto/` + `schema/` 挪进 `pkg/`。

### I.2 角色 ↔ 运行时激活的包

二进制只有一个，下表列出**按子命令进入不同模式时**实际会 wire 起来的 `internal/` 包（其他包虽然编译进去但不启动）：

| 运行模式 | 启动子命令 | 运行时激活的 `internal/` 包 | 运行时不激活 |
|---|---|---|---|
| broker | `tether serve` | 全部（broker 是大脑） | — |
| agent | `tether agent` | `proto/`, `auth/`（客户端侧）, `pty/`, `proc/`（上报侧）, `tunnel/`（yamux 隧道客户端） | `storage/`, `jsstream/`, `session/`, `reconcile/`（broker 侧）, `adminsock/` |
| ctl | `tether login/ps/run/…` | `cli/`, `auth/`（客户端侧）, `proto/` | `storage/`, `tunnel/`, `jsstream/`, `adminsock/` |
| admin | `tether admin …` | `cli/admin`, 连本地 Unix socket | 其他均走本地 socket 协议 |

- **agent / ctl 无 `storage/`（不用 SQLite）**，但 agent **有最小持久化状态**落到 **per-session 目录** `~/.tether/agent/<sid>/state.json`（0600）——对应 §E.0 "一机多 session 开多 agent" 的语义，路径按 sid 隔离：
  - `actor_nkey`、`sid`、`nid`、`agent_version`、`proto_version`、`boot_id`、`last_seen_at`
  - `port_tokens[]`：`{port, token, local_port, name}` —— 明文 token，用于 agent 重启后 frpc 无须重新 `expose` 即可恢复 proxy
  - 原子写：tmp + fsync + rename；权限 0700（目录）/ 0600（文件）
  - 重启流程：读 state.json → 连 broker → 发 register.req 带 local_ports token_hash → tetherd 按 SQLite 对账回 `keep_ports / revoke_ports`（§G.1）→ agent 清理或保留本地条目
  - 真相仍以 broker SQLite 为准；state.json 只是"加速恢复 + 承载 agent 独占明文（token）"的缓存
- **ctl 无持久状态**：全走 NATS，本地仅 `~/.tether/current_session` 等小文件用标准 `os` 包读写。
- **代码里 wire 用 if/switch 按子命令分支**：未被激活的包符号虽然存在但没有运行代码路径。

**Command surface by host**：

| 主机 | 实际有意义的子命令 |
|---|---|
| broker 主机 | `tether serve`, `tether serve reload`, `tether admin *`, `tether version` |
| 实验机器（agent 所在） | `tether agent`, `tether agent status`, `tether agent stop`, `tether version` |
| 使用者笔记本 | `tether login/logout/session/node/ps/run/exec/expose/history/version` |

ctl 命令在 broker / agent 主机上技术上也能跑（只要能连 broker），但日常无需要。

**`tether --help` 按三层分组显示**，避免 25 条命令一屏糊脸：

```
$ tether --help
Tether distributed experiment control.

Daemon:
  serve     Run broker (on broker host)
  agent    Run agent (on experiment machine)

Control:
  login, logout, session, node, ps, run, exec, expose, history

Admin (broker local):
  admin sessions / nodes / audit / evict

Other:
  version
```

### I.2b admin 命令走本地 Unix socket

`tether admin *` **不走 NATS**，直接连 broker 本地 `/var/run/tether/admin.sock`（`serve` 模式启动时创建，仅 `tether` 系统用户可读写）。

**为什么不走 NATS**：
- 远程 admin 需要额外鉴权层（who can admin？跨 session？）——v1 简化为"能物理登录 broker 的就是 admin"。
- 避免 admin 通道与 ctrl 通道混用 subject，降低 JWT permissions 复杂度。

**代价**：admin 必须在 broker 机器上本地 shell 跑。v2 若需远程 admin，再在 NATS 上加独立 subject + 强鉴权。

### I.3 共享包边界

**`internal/proto/`**—— RPC 层。subject 模板 + 请求 / 响应 struct + JSON tag。业务 subject 里嵌 `by.<actor>` 段（见 B.1），所以 proto 层提供 sprintf 风格的 builder 函数，不是 const 字符串。

```go
package proto

const (
    ProtoVersion = 1
    SubjectPrefix = "tether.v1"
)

// 无 actor 的 subject（agent 自发、broker 广播类）
const (
    SubjVersionAnnounce = "tether.v1.ctrl.version.announce"
    SubjSysEvents       = "tether.v1.sys.events"
)

// actor-scoped subject builder（ctl 侧）
func SubjCtrlBy(actor, leaf string) string {
    return fmt.Sprintf("tether.v1.ctrl.by.%s.%s", actor, leaf)
}
// Args ordered as the subject reads ("s.<sid>.cmd.by.<actor>.node.<nid>.<verb>"),
// matching every other per-session builder which takes sid first.
func SubjCmdBy(sid, actor, nid, verb string) string {
    return fmt.Sprintf("tether.v1.s.%s.cmd.by.%s.node.%s.%s.req", sid, actor, nid, verb)
}

// session / node 自发 subject（agent 侧）
func SubjNodeRegister(sid, nid string) string {
    return fmt.Sprintf("tether.v1.ctrl.s.%s.node.%s.register.req", sid, nid)
}
func SubjNodeHeartbeat(sid, nid string) string {
    return fmt.Sprintf("tether.v1.ctrl.s.%s.node.%s.heartbeat", sid, nid)
}

// forwarded subject（tetherd pub、agent sub）
func SubjCmdForwarded(sid, nid, verb string) string {
    return fmt.Sprintf("tether.v1.s.%s.cmd.node.%s.%s.req.forwarded", sid, nid, verb)
}

type SessionCreateReq struct {
    Name string `json:"name"`
    PIN  string `json:"pin,omitempty"`
}
type SessionCreateResp struct { SID string `json:"sid"` /* ... */ }
```

**actor 值来自哪里**：actor = ctl 自己的 nkey public key（见 B.5），ctl 本地 `~/.tether/keys/<fp>.nk` 加载后即可得出；无须额外的 hello 往返。tetherd 下发的 JWT permissions 把 `by.<A>` 段里的 `<A>` 锁死为本连接的 nkey，ctl 用错值会被 NATS 层 deny。agent 的 subject 不含 actor（它们不是业务动作发起人）。

**`internal/schema/`**—— 入流的审计 / 事件记录（v:1）。

```go
package schema

const AuditSchemaVersion = 1

type AuditCall struct {
    V     int    `json:"v"`
    Kind  string `json:"kind"`   // "call"
    Actor string `json:"actor"`
    // ...
}
```

**为什么 `proto` 与 `schema` 分开**：`proto` 是 RPC 消息，会随功能迭代频繁变；`schema` 一旦入流必须长期向后兼容（历史记录要能解析）。解耦两者的演进节奏。

**`internal/auth/`**—— nkeys / JWT / PIN(argon2id) / auth_callout 统一入口。client 与 server 侧不拆子包；v1 代码量小，拆了反而绕。

### I.4 测试布局

- **单元测试**：`internal/xxx/*_test.go`（Go 约定，紧挨实现）。
- **集成 / e2e**：`test/e2e/` 下拉起真实 `nats-server -js` + `tether serve` + `tether agent`，跑端到端流程（login → run → expose → ps → history）。
- **不 mock NATS、不 mock SQLite**。延续"集成测打真实依赖"原则。

### I.5 构建与发布

- **Makefile**：`make build` → `./bin/tether`（单二进制）；`make test`；`make e2e`；`make release` 触发 goreleaser。
- **单条** `CGO_ENABLED=0 go build -ldflags "-X main.version=$V -X github.com/<org>/tether/internal/proto.ReleaseVersion=$V" ./cmd/tether` 出全部功能。
- 静态链接（`CGO_ENABLED=0`）保证无权限机器可直接放置运行（见 K）。
- **goreleaser** 负责多平台交叉编译（`linux/amd64`、`linux/arm64` 首发；`darwin/*` 可选），输出 `tether_<ver>_<os>_<arch>.tar.gz`（内含单一 `tether` 二进制）+ `install.sh` + `SHA256SUMS`。

### I.6 v1 不做

- 不拆多二进制，不做 polyrepo；不发布公共 SDK（`pkg/` 留空）。
- 不做 plugin 架构；所有功能编译进主二进制。
- **不做 `-tags slim` 精简 agent 构建**（多出构建矩阵，v1 不值）。
- `auth/` 不拆 client/server 子包；`internal/cli/` 不按子命令再拆 module。
- admin 通道 v1 只走本地 Unix socket，不做远程 admin。

---

## J. 版本与升级

### J.0 目标

虽然所有角色共享同一个 `tether` 二进制（见 I.1），但三种部署实例（broker 的 `tether serve` / 实验机器的 `tether agent` / 使用者笔记本的 `tether` CLI）生命周期不同，跨机升级时版本漂移概率高。协议改了一边没改另一边，**最坏情况是"看起来能连、某字段被静默丢弃"**。J 节让这种状态在握手阶段就失败。

### J.1 版本编号：双轨

| 字段 | 示例 | 谁用 |
|---|---|---|
| `ReleaseVersion` | `v0.3.2` | 人看、release note、下载文件名 |
| `ProtoVersion` | `1` | 程序用、握手比对 |

- `ProtoVersion` 单调递增整数，**硬编码**在 `internal/proto/version.go`：
  ```go
  package proto
  const ProtoVersion = 1
  ```
- `ReleaseVersion` 由 goreleaser 经 `-ldflags` 注入 `main.version`。
- **规则**：协议 breaking change 才 bump `ProtoVersion`；bug fix / 向后兼容字段追加只 bump `ReleaseVersion`。
- **本机同二进制**：单二进制多角色设计使得同一台机器上 `tether serve` / `tether agent` / `tether` ctl 的 `ProtoVersion` 天然一致；跨机 handshake 仍按 J.2 校验。

### J.2 握手：strict same-version

v1 采用**严格同 ProtoVersion 校验**，不同则拒。**不做 N-1 兼容层**。

**agent → tetherd**（`register.req` 首字段）：
```json
{ "proto_version": 1, "release_version": "v0.3.2", "nid": "lab-1", ... }
```

tetherd 比对自己编译进的 `proto.ProtoVersion`：
- 不等 → 拒绝，返回 `{ "code": "proto_mismatch", "server_proto": 1, "client_proto": 2 }`
- 等 → 继续 G.1 reconcile。

**CLI → tetherd**：不单独开 `ctrl.hello.req` 业务 subject（会绕开 auth_callout 的 permissions 精确模板）。CLI 在 **NATS CONNECT 阶段**附带 `proto_version`（通过 `client.Opts.Name` 或 `ConnectionInfo.Jwt.Claims.Aud` 等字段），由 `auth_callout` 在签 JWT 之前比对：
- 不等 → 直接拒绝 CONNECT，NATS 层返回 authorization error，CLI 解码出相同的 `proto_mismatch` 文案。
- 等 → 正常下发 JWT permissions。

此路径把 proto check 和鉴权合在一次 CONNECT 内，零业务 subject 开销；也避免"未激活 ctl 又能 pub 一条未在 subject 树里的 req"这种模板漏洞。

**不兼容的理由**：双版本共存需要消息转换层 + 测试矩阵翻倍。v1 用户量小，"三边一起升"成本低、收益大。延续"安全实用主义"。

**错误文案面向人**（CLI 侧）：
```
✖ proto mismatch: your tether CLI is v0.3.2 (proto=2), server is v0.2.0 (proto=1)
  → upgrade server or downgrade CLI: https://…/releases
```

### J.3 升级顺序

**场景一：Minor release / bug fix（同 ProtoVersion）**

推荐顺序：**tetherd → agent → CLI**。三者 `ProtoVersion` 不变，新旧混合期仅 `ReleaseVersion` 不同，互相连通。

1. 停 / 升 / 起 tetherd（中断 ~30s；agent G.1 reconnect 恢复）。
2. 用 `tether node upgrade`（§J.4）逐台升 agent。
3. 各使用者按提示升自己的 CLI。

**CLI 最后**的理由：CLI 是瞬时连接，临时 ReleaseVersion 漂移可由用户自行升级；tetherd / agent 常驻，先升可缩短过渡窗口。

**场景二：Breaking proto release（`ProtoVersion` bump，例如 1→2）**

proto bump = 协议不兼容，新 tetherd 会主动拒绝老 agent / 老 CLI（§J.2 `proto_mismatch`）。**不能走 `tether node upgrade`**——该命令依赖 CLI↔tetherd↔agent 三边同 proto 才能通信；一旦 tetherd 升了新 proto，老 agent 连不上 tetherd，命令根本下发不到 agent 执行升级。

唯一正确路径：
1. 宣告时间窗，让所有使用者下线。
2. 升级 tetherd（新 proto）。此时所有老 agent 自动进入 `proto_mismatch` 被拒状态、node 全变 OFFLINE。
3. 运维/用户**物理登录每台实验机**，执行：
   ```
   curl -fsSL https://<broker>/install.sh | sh -s -- --role agent --upgrade
   ```
   `install.sh --upgrade` 下新二进制 + 保留 `~/.tether/` 配置（包括 nkey / state.json），替换 `~/.local/bin/tether` 后重启 agent 服务（systemd --user 或用户手动）。
4. 使用者分别 `curl install.sh | sh` 升级 CLI。

**为什么不设"旁路升级通道"**：代价是"协议版本不兼容时仍要允许某种最低连接来传一个升级指令"——那条通道本身就是新协议面，要测要维护，违背"v1 实用主义"。breaking proto 本就是大版本事件，要求运维动手是合理的代价。

### J.4 agent 升级通道（仅 Minor / Bug Fix）

**前提**：CLI / tetherd / agent 的 `ProtoVersion` 一致（否则走 §J.3 场景二）。

**(a) 被动通知（仅提醒，不自救）**：tetherd 在 `tether.v1.sys.events` 广播：
```json
{ "v": 1, "kind": "agent_upgrade_available",
  "proto_version": 1, "release": "v0.4.0",
  "url": "https://…/install.sh",
  "sha256": "..." }
```
agent 订阅到后仅写日志 info（"新版本可用，建议 owner 下发 `tether node upgrade`"），**不自动触发**、**不在 register 回包加 upgrade_hint**、**不做任何本地自救**。所有升级必须由 owner 主动发起；agent 不信任广播信息实施任何操作。

**(b) 主动触发**（owner 权限，v1 = session 创建者）：
```
tether node upgrade lab-1      # 单台
tether node upgrade --all      # 当前 session 全部 agent
```
路径：CLI → `tether.v1.s.<sid>.cmd.by.<actor>.node.<nid>.upgrade.req` → tetherd（owner 校验 + sha256/url 白名单 + `proto_version` 必须等于当前 tetherd） → `tether.v1.s.<sid>.cmd.node.<nid>.upgrade.req.forwarded` → agent。

tetherd 侧硬拒条件：
- 请求 `proto_version` ≠ 当前 tetherd `ProtoVersion` → reply `{code:"proto_bump_requires_reinstall"}`，引导到 §J.3 场景二。
- url 不在白名单 / sha256 缺失 → 拒。

agent 执行步骤：
1. `curl -L <url> -o /tmp/tether.new`
2. 校验 `sha256`（不对则拒升）
3. `chmod +x` → 原子 rename 到 `~/.local/bin/tether`
4. `systemd-run` / `nohup` 拉起新 `tether agent --session <sid>`（保留 `~/.tether/` 下全部配置与 state.json）
5. 旧 agent 主动断 NATS → 新 agent 启动后 G.1 reconnect

**安全约束**：
- **sha256 强校验**，哈希不对拒绝（防 HTTP 中间人）。
- **url 白名单**：tetherd 配置文件写死允许前缀（默认 `https://github.com/<org>/tether/releases/`），agent 收到 url 后本地再验一次白名单。
- **`node.upgrade` 需 owner 权限**：v1 简化为"session 创建者 = owner"（§E.4）。
- **proto 相同才能跑**：proto bump 走 §J.3 场景二，不经此路径。

**v1 不做**：代码签名（Sigstore/cosign）、灰度 / canary、自动回滚、自救模式（upgrade_hint）。

### J.5 tetherd / CLI 升级

- **tetherd**：`systemctl stop tetherd → 替换二进制 → start`。**不做在线热升级**。
- **CLI**：用户 `curl install.sh | sh`（见 K）。可选 `tether version --check` 手动查最新 release。

### J.6 v1 不做

- 跨 ProtoVersion 兼容层。
- agent 灰度 / 签名 / 自动回滚。
- tetherd 零停机热升级。
- 自动每日升级检查（仅 `version --check` 手动触发）。

---

## K. 安装与分发

### K.0 核心原则

1. **单二进制 `tether`**：所有角色共用；安装即"把 `tether` 放到路径 + 准备配置 + 写启动单元（可选）"。
2. **install ≠ start**：**安装脚本只铺文件、不启动任何服务**。用户必须手动执行 `tether serve` / `tether agent` / `systemctl start ...` 才会真正跑起来。这样可控、可审查、可在多次试错中安全重入。
3. **无权限优先**：agent 必须能在无 root 的实验机器上装。
4. **分发渠道**：GitHub Releases + 反代域名 `<broker>/install.sh`。

### K.1 agent 无权限安装（实验机器）

**安装路径**（全在用户 HOME 下，无需 root；**per-session 目录布局**，匹配 §E.0 "一机多 session 开多 agent"）：

```
~/.local/bin/tether                         # 单二进制（静态链接），全 session 共用
~/.tether/                                  # 0700
└── agent/
    └── <sid>/                              # 每个 session 独立子目录，0700
        ├── agent.yaml                      # 配置：broker URL / sid / nid（非密），0600
        ├── agent.log                       # 日志，0600
        ├── agent.pid                       # PID 文件，0600
        ├── state.json                      # 见 I.2：actor_nkey/sid/nid/boot_id/port_tokens 等，0600
        └── keys/
            └── agent.nk                    # nkey seed（ed25519 私钥），0600
```

- 单 session 场景用户只看到 `~/.tether/agent/lab/…`；同机接入多 session 时开多 agent 进程，各自绑自己的 `<sid>` 目录，PID / 日志 / frpc 实例 / token 互不覆盖。
- 权限约束：`~/.tether/` 与子目录 `0700`，所有文件 `0600`。写入采用 tmp+fsync+rename 原子替换。

**安装（不启动）**：
```bash
curl -fsSL https://<broker>/install.sh | sh -s -- \
    --role agent \
    --broker wss://<broker>:443 \
    --session lab \
    --pin 123456 \
    --nid lab-1
```

`install.sh --role agent` 做的事（**一次安装对应一个 session**，多 session 再跑一次 install，路径按 sid 隔离）：
1. 探测 OS / arch（`uname -m -s`），下载对应 `tether_<ver>_<os>_<arch>.tar.gz` + `.sha256`，校验。
2. 解到 `~/.local/bin/tether`，`chmod +x`（多 session 共用同一个二进制）。
3. 用 `--pin` 向 broker 注册（走 E.2 join 流程），落 `~/.tether/agent/<sid>/agent.yaml`（broker URL / sid / nid）+ `~/.tether/agent/<sid>/keys/agent.nk`（nkey seed）。
4. **打印下一步命令**（示例 sid=`lab`）：
   ```
   ✔ tether installed to ~/.local/bin/tether
   ✔ agent config written to ~/.tether/agent/lab/agent.yaml

   To start the agent now:
       setsid nohup ~/.local/bin/tether agent --session lab \
         >> ~/.tether/agent/lab/agent.log 2>&1 &

   For auto-start across logins (optional):
       ~/.local/bin/tether agent --install-user-service --session lab
       # 生成 ~/.config/systemd/user/tether-agent@lab.service
     or append to ~/.bashrc manually (see docs).
   ```

**不自动启动**，用户自己决定什么时候 `tether agent --session <sid>` 跑起来。

**自启动（可选，按权限降级）**：

| 可用设施 | 做法 |
|---|---|
| 有 `systemd --user` | `tether agent --install-user-service --session <sid>` 生成 `~/.config/systemd/user/tether-agent@<sid>.service`（template unit）；用户再 `systemctl --user enable --now tether-agent@<sid>` + `loginctl enable-linger <user>` |
| 无 systemd 但有 bash | 用户手动往 `~/.bashrc` 加每 session 一条幂等片段：`pgrep -f "tether agent --session lab" \|\| setsid nohup ~/.local/bin/tether agent --session lab &>> ~/.tether/agent/lab/agent.log &` |
| 以上都没有 | 每次手动 `setsid nohup tether agent --session lab &` |

`setsid` 让进程脱离终端（关 ssh 不带走）。`systemctl --user` 是"用户级 systemd"，免 root。同机多 session 就起多个 `tether-agent@<sid>` 实例，PID / 日志 / state 各自在 `~/.tether/agent/<sid>/`。

**卸载**：`tether agent --uninstall`（或 `install.sh --uninstall --role agent`）→ 杀进程 + 删 `~/.local/bin/tether` + 删 `~/.tether/`。

### K.2 CLI 安装（使用者笔记本）

```bash
curl -fsSL https://<broker>/install.sh | sh
```

默认 `--role ctl`（缺省值）。装到：
- 有 `/usr/local/bin` 写权限 → `/usr/local/bin/tether`。
- 否则 → `~/.local/bin/tether`，提示 `export PATH=$HOME/.local/bin:$PATH`。

配置：
- `~/.tether/config.toml`（broker URL）
- `~/.tether/current_session`（激活态，见 E.0）
- `~/.tether/keys/<fp>.nk`（nkey seed，per broker）

**不启动任何东西**——CLI 本来就是按需调用，不涉及 daemon。

**替代分发**（v1 不做、v2 可选）：Homebrew tap / APT / RPM / `go install`。

### K.3 broker 安装（运维侧）

```bash
curl -fsSL https://<broker>/install.sh | sudo sh -s -- \
    --role broker \
    --domain tether.example.com \
    --acme-email admin@example.com
```

`install.sh --role broker` 做的事（**全部为"铺文件、写 unit、生成配置"，不启动**）：

1. 创建系统用户 `tether`。
2. 下载并安装 `tether` 到 `/usr/local/bin/tether`。
3. **附带下载** `nats-server` 与 `caddy` 静态二进制到 `/usr/local/bin/`（A 议题决定：broker 主机需要这三个进程）。
4. 生成 `/etc/tether/broker.yaml`（骨架见 §A.3：`domain`、`tls.acme` 或手动证书路径、`nats.wss_listen=:443` + `nats.ws_internal=127.0.0.1:8222`、`frp.control_listen=:7000` + `frp.port_range=14000-14999`；admin socket 路径 `/var/run/tether/admin.sock`）。
5. 生成 Caddyfile：仅一条反代规则 `example.com:443 → 127.0.0.1:8222 (NATS WebSocket)` + ACME 邮箱。Caddy **不接触** frp；frps 由 tetherd 内嵌直监听 7000/14000-14999。
6. 生成三个 systemd unit（但**不 enable / 不 start**）：
   - `/etc/systemd/system/nats-server.service`
   - `/etc/systemd/system/tether-broker.service`（`ExecStart=/usr/local/bin/tether serve`，`After=nats-server.service`）
   - `/etc/systemd/system/caddy.service`
7. 创建目录：`/var/lib/tether/`（SQLite + JetStream store）、`/var/log/tether/`、`/var/run/tether/`（admin.sock 父目录）。
8. **打印下一步**：
   ```
   ✔ broker files installed.
   ✔ systemd units created (nats-server, tether-broker, caddy).

   To start the broker stack:
       sudo systemctl daemon-reload
       sudo systemctl enable --now nats-server tether-broker caddy

   First-time admin setup (after start):
       sudo -u tether tether admin init
   ```

**运维必须自己 `systemctl enable --now`**——这是 K.0 原则 2 的体现。

### K.4 release 产物

每次 tag 触发 goreleaser，输出：

```
tether_<version>_linux_amd64.tar.gz      # 单二进制 tether
tether_<version>_linux_arm64.tar.gz
tether_<version>_darwin_amd64.tar.gz     # v1 可选
tether_<version>_darwin_arm64.tar.gz     # v1 可选

install.sh                               # 统一入口：--role {agent,ctl,broker}
SHA256SUMS
SHA256SUMS.asc                           # v2 加 GPG 签名
```

**只有一个 tarball / 平台**、**只有一个 install.sh**：相比三个二进制时代的 6 个包 + 3 个脚本，分发面积减半。

**发布渠道**：GitHub Releases 主仓；`<broker>/install.sh` 反代 GitHub raw。域名层解耦 broker 换主机不影响下载。

### K.5 三类角色的一条龙体验

| 角色 | 一行安装命令 | 启动动作（不自动执行） |
|---|---|---|
| 使用者 | `curl install.sh \| sh` | `tether login -s <S> --pin <P>` |
| 实验机器 | `curl install.sh \| sh -s -- --role agent --broker <U> --session <S> --pin <P> --nid <N>` | `setsid nohup tether agent --session <S> >> ~/.tether/agent/<S>/agent.log &`（或用户级 systemd `tether-agent@<S>`） |
| 运维 | `curl install.sh \| sudo sh -s -- --role broker --domain <D>` | `sudo systemctl enable --now nats-server tether-broker caddy` |

**强制语义**：`install.sh` 返回后没有任何 tether 进程在跑；用户必须显式执行启动命令才生效。

### K.6 v1 不做

- **不自动启动服务**（core principle）。
- 不签名 release 二进制（Sigstore / cosign v2 再说）。
- 不发 deb / rpm / brew / winget / Chocolatey；只走 `install.sh` + tarball。
- 不做 auto-update（见 J）。
- 不做 Windows 支持（PTY、setsid 差异太大；v2 评估）。
- 不做 Docker image / Kubernetes manifest。
- 不做离线 / air-gapped 安装包。
- 不做安装脚本的 rollback / atomic install（失败让用户重跑；目录都是幂等重入的）。

---

# Part II — 实现路线图

> 设计原则："树不能先结果再发芽"——每个 phase 的完成是下一个 phase 的前提。每个 phase 必须有**可执行的交付**与**可验证的测试**，未通过不进入下一阶段。

## 依赖关系（根→叶）

```
P0  scaffold
 │
 ▼
P1  foundation packages (proto / schema / auth / storage)
 │
 ▼
P2  broker + agent 最小闭环（无 auth、无 CLI）     ← 系统能活起来
 │
 ▼
P3  auth + session + login                          ← 多租户骨架
 │
 ▼
P4  control plane — exec（非交互）                   ← 控制面 MVP
 │
 ▼
P5  run（PTY 交互）                                  ← 用户可用 v1 雏形
 │
 ▼
P6  expose（frp 数据面）
 │
 ▼
P7  audit + history（JetStream）                     ← 观测与审计
 │
 ▼
P8  reconciliation                                   ← 崩溃恢复
 │
 ▼
P9  caddy :443 + admin socket                        ← 生产表面
 │
 ▼
P10 distribution（install.sh / goreleaser）          ← 可下发
 │
 ▼
P11 release hardening + docs                        ← v0.1.0
```

---

## P0 — Scaffold

**目标**：`tether version` 能跑。

**做**
- 初始化 `go.mod`（`module github.com/<org>/tether`）。
- `cmd/tether/main.go` + cobra root cmd + `tether version` 子命令。
- `Makefile`：`build / test / lint`（golangci-lint）。
- GitHub Actions：push 触发 build + test + lint。
- `.gitignore`、`LICENSE`、README 骨架。

**测试**
- `make build` 产出 `./bin/tether`。
- `./bin/tether version` 打印 `v0.0.0-dev`。
- 本 PR CI 绿。

**出口**：`git clone && make build && ./bin/tether version` 走通。

---

## P1 — Foundation packages

**目标**：协议 / schema / 鉴权原语 / 存储层可独立单元测试。

**做**
- `internal/proto`：subject 常量、请求/响应 struct、`ProtoVersion = 1`、JSON tag。
- `internal/schema`：`AuditCall / AuditProc / AuditPort`，`AuditSchemaVersion = 1`。
- `internal/auth`：nkey 生成/加载、JWT 签/验、PIN argon2id 哈希/校验。
- `internal/storage`：SQLite 表（`sessions / members / nodes / processes / port_allocations`）+ 迁移（`embed` + 顺序 SQL）。**不建 `audit_log` 表**——audit 只落 JetStream `history-<sid>`（见 H.2：SQLite 存权威实时状态，JetStream 存历史过程档案，不双写）。

**测试**
- `proto`：golden JSON 回环测试（Marshal→Unmarshal→Marshal 字节相等）。
- `auth`：nkey roundtrip；JWT 签后本地验通过；argon2id 验证已知向量。
- `storage`：内存 SQLite 跑迁移、事务提交/回滚、级联删除。
- 覆盖率 ≥ 70%。

**出口**：`go test ./internal/...` 全绿；各包可被后续 phase import。

---

## P2 — Broker + Agent 最小闭环（无 auth、无 CLI）

**目标**：broker 能识别 agent 在线/离线。

**做**
- `make nats-dev` 本机起 `nats-server -js`（dev 模式，无鉴权）。
- `tether serve`：连 dev NATS，订阅 `tether.v1.ctrl.s.*.node.*.register.req` 入库。
- `tether agent`：连 NATS，发 `register.req`，5s 心跳发 `node.heartbeat`。
- Node 状态机（D.2）：ONLINE / STALE / OFFLINE（5s / 60s）。
- **临时** `tether admin nodes`（本 phase 简化成直接读 SQLite；正式 admin socket 留 P9）。

**测试**
- 起 broker + 1 agent → 1s 内 `tether admin nodes` 见 ONLINE。
- kill agent → 5s 后 STALE → 60s 后 OFFLINE。
- 重启 agent → 回到 ONLINE。
- 10 分钟稳定性：心跳不中断，状态翻转正确。

**出口**：心跳闭环稳定跑 10 分钟。

---

## P3 — 鉴权 + session + login

**目标**：多 session 隔离，CLI 能登录。

**做**
- 启用 NATS `auth_callout`（E.2，nkey native），broker 实现回调：查 PIN → 签 session JWT。
- `internal/session`：CRUD（三阶段 rm 的 stream 清理与墓碑后 ②③ 步骤留 P7；本 phase 先实现 ACTIVE→DELETING 墓碑与 C.1 第 6 条拒入口）。
- `tether session {create, ls, rm, info}` 子命令。
- `tether login [-s S] [--pin X]` 统一入口（E.0）：PIN 验证 → 落 `~/.tether/keys/<fp>.nk` + `~/.tether/current_session`。
- JWT permissions：**严格实现 B.2 的固定 token 数模板**。未激活仅 `ctrl.by.<A>.session.{create,list}.req` + `_INBOX.>`；激活后按 B.2 激活态模板精确开启各条 `ctrl.by.<A>.s.<S>.*.req` / `s.<S>.cmd.by.<A>.node.*.*.req` / `s.<S>.ev.*` / `s.<S>.audit.*` / `s.<S>.pty.<pid>.{in,resize,attach}` 等叶子。**禁止任何 `s.<S>.>` / `tether.v1.>` 的大通配 allow**。
- 实现核对：CI 加一条静态测试——加载每个角色模板，**只禁危险大通配**，不是"全禁 `.>`"（B.2 里 tetherd 合法订 `ctrl.by.*.>`、member 合法订 `s.<S>.ev.>` / `s.<S>.audit.>` 等叶子级通配）。具体黑名单：
  - 任一角色 allow 出现 `tether.v1.>` 或 `tether.v1.s.<S>.>`（跨子树大通配）→ 失败。
  - `ctl` 任一 allow 能匹配到 `.req.forwarded` 后缀（例如 `s.<S>.cmd.node.*.*.req.forwarded` 或 `s.<S>.cmd.>`）→ 失败。
  - `ctl` 的 pub allow 不包含锁死的 `by.<A>` 段（出现 `by.*.` 形式能伪造他人）→ 失败。
  - `agent` 的 pub allow 出现 `s.<S>.audit.*` 任意前缀 → 失败（audit 必须 tetherd 单边）。

**测试**
- `tether session create lab --pin 123` → `tether login -s lab --pin 123` 成功。
- 错误 PIN 被拒（argon2 失败）。
- session A 的 token 订阅 `tether.v1.s.<sidB>.ev.>` → NATS permission denied（精确模板限制）。
- ctl 尝试 pub `s.<S>.cmd.node.<N>.run.req.forwarded`（绕 tetherd）→ permission denied（`.forwarded` 不在 ctl allow）。
- ctl 伪造他人 `by.<actor>` 段 pub → permission denied（actor 段被 tetherd 写死为本连接 nkey）。
- 两个 shell 分别激活不同 session，`$TETHER_SESSION` 互不干扰。

**出口**：多 session 多 shell 互不串台；未登录 ctl 命令明确拒绝。

---

## P4 — Control plane：`exec`（非交互）

**目标**：从 CLI 发命令到 agent 执行并回收结果。

**做**
- `.forwarded` 中转（C.4）：ctl → broker → agent 走 `req.forwarded`；agent 只订阅 `.forwarded`；permissions 禁止 ctl 直连 agent subject。
- agent 侧 `os/exec` 非交互执行；stdout / stderr 分块 pub 回 `_INBOX`。
- `internal/proc`：process 状态机（RUNNING / EXITED），生命周期事件入 SQLite。
- `tether exec <cmd>`：简化 run，无 PTY；等待退出码并打印。
- `tether ps`（本 phase 仅进程列，端口列留 P6）。
- **audit 写入权威**：tetherd（不是 agent）收到 `.req` 审核通过后 pub `s.<sid>.audit.call`；agent 只 pub `s.<sid>.ev.node.<N>.proc.*`，tetherd 订 ev 后转写 `s.<sid>.audit.proc`。本 phase 先以 NATS core pub 形式写，P7 接 JetStream `history-<sid>` 后自动变持久化；**不写入 `sys.events`**（`sys.events` 仅放全局运维事件，不放 per-session audit）。

**测试**
- `tether exec echo hello` → stdout "hello"；exit 0。
- `tether exec false` → exit 1。
- `tether exec sleep 30 &` → `tether ps` 见 RUNNING；等结束后见 EXITED。
- ctl 尝试直接发 `agent.proc.start.req.forwarded`（bypass broker）→ permission denied。

**出口**：从笔记本一条命令远程跑，结果可回收、可查状态。

---

## P5 — `run`：PTY 交互

**目标**：`tether run vim` / `htop` / 带进度条的脚本可用。

**做**
- `internal/pty`：`creack/pty.Start` 包装，双向 `io.Copy` + SIGWINCH（C.5）。
- **两阶段 attach 握手**（C.5.1）：agent 先 pub `pty.<pid>.ready`，ctl 订好 `.out/.in/.resize` 后 pub `pty.<pid>.attach`，agent 收到 attach 才 fork+exec。
- 3s attach 超时：agent pub `pty.<pid>.ready` 后若 3s 没收到 ctl 的 `attach` → pub `pty.<pid>.failed{reason:"attach_timeout"}` + 放弃启动；tetherd 写 `audit.proc{kind:attach_timeout}`，回 ctl run.req reply 失败。
- NATS core pub 传字节流（不入 JetStream）；agent ↔ broker ↔ CLI 原样转发。
- 窗口大小：初始 cols/rows 随 `pty.<pid>.attach` 载荷带入；后续 SIGWINCH 发 `pty.<pid>.resize`。
- Ctrl-C：CLI 捕获 → 发 `kill` 命令（SIGINT 到进程组）→ agent `kill -2`。
- `tether run` 子命令。

**测试**
- `tether run bash` → 两阶段握手完成，进入交互 shell，`exit` 正常返回。
- `tether run vim /tmp/x` → `:wq` 写成功，文件存在。
- `tether run -- pip install numpy` → 进度条首字节不丢（`#####` 全见），`\r` 正确刷新。
- **attach 超时**：mock ctl 收到 `pty.<pid>.ready` 后故意不 pub `attach` → 3s 后收到 run.req 的 failure reply（`attach_timeout`）、agent 侧无孤儿 PTY。
- `tether run sleep 10` + 本地 Ctrl-C → agent 进程 1s 内退出。
- 终端 resize → `tether run stty size` 实时正确。

**出口**：主观体验"和 ssh 差不多"；进度条首字节不丢；attach 超时场景明确拒绝且无资源泄漏。

---

## P6 — 数据面：expose + frp

**目标**：把 agent 本地端口暴露到 broker 公网侧。

**做**
- `internal/port`：分配表（14000-14999）、token 生成、ALLOCATED→REVOKED（OFFLINE ≥ 15min 自动触发），REVOKED/FREED 立即回池。
- `internal/tunnel`（v1 实现选择 in-process yamux-over-TCP，
  替换 spec 原本的 `internal/frpmgr` + frpc 子进程方案，详见
  README "Architecture deep-dive"）：broker 侧 `tunnel.Server`
  监听 `:7000`（agent control + TLS）+ 按需绑公网端口
  `:14000-14999`；agent 侧 `tunnel.Client` 维护 yamux 会话，
  expose 重启自动重连。
- `tether expose --local 8888 --name jupyter [--remote-port P]` → 分端口（省略=最低空闲；指定=该带内端口，占用则 `port_taken`，见 F.3）→ 返回 `http://<broker>:<port>`（默认明文，业务自管 HTTPS）。
- `tether expose rm --name jupyter`。
- **`tether ps` 升级为统一视图**（进程 + 端口同一张表，F.8）。
- Port 状态机（D.4）；agent 侧 `state.json` 落 `port_tokens[]` 支持重启恢复。

**测试**
- agent 机器 `python -m http.server 8888`；`tether expose --local 8888 --name jupyter` → broker 侧 `curl http://<broker>:14022` 回目录。
- `tether expose rm --name jupyter` → 立即断流；端口号立刻回池（随后 `expose --local 9999 --name other` 立即可复用 14022）。
- node 进 STALE：端口仍保持 ALLOCATED（SQLite 未改），业务流量按 frpc 已有连接继续；15 分钟后 tetherd 触发 ALLOCATED→REVOKED，pub `ev.port.<port>.revoked`，端口号回池。
- agent 重启 → 读 `state.json` → frpc 凭本地明文 token 自动重连 → ALLOCATED 端口无需人工 re-expose。

**出口**：典型 jupyter / tensorboard 暴露用例端到端可用。

---

## P7 — 审计 + history（JetStream）

**目标**：所有操作可回放、可审计。

**做**
- `internal/jsstream`：session create 时创建 `history-<sid>`（unlimited / discard=new）；broker 启动时创建 `events`（30d / 1GiB / discard=old，仅放全局运维事件如 `session_destroyed` / `disk_pressure` / `tetherd_restarted`）。
- P4-P6 的 `s.<sid>.audit.{call,proc,port}` 从 core pub 升级为 JetStream publish，**只进 `history-<sid>`**（单处落盘，不双写 `events`；`events` 不记 per-session audit）。
- `tether history [-n N] [--follow] [--kind call|proc|port]`：ephemeral consumer 从 `history-<sid>` 回放。
- **Session rm 三阶段**（H.3）：① SQLite 提交 `state=DELETING` 墓碑 → ② JetStream DELETE stream → ③ SQLite 真删关联表。C.1 第 6 条前置校验在 ① 之后立即生效。tetherd 重启扫 `state=DELETING` 续跑 ②③（见 G.2 ①b）；兼带孤儿 stream 清理。
- Disk 监控：store 目录 > 80% → 发 `sys.events{type:disk_pressure}`。

**测试**
- 连跑 50 条 `exec`，`tether history -n 100` 全部可见，时间顺序正确。
- `session rm lab` 全流程：观测到 SQLite `sessions.state` 瞬时经过 `DELETING` → 最终 `nats stream ls` 无 `history-lab` + SQLite 无 lab 记录；rm 期间任何 `tether run -s lab …` 立即收 `session_not_found_or_deleting` 错误 reply。
- 模拟 `state=DELETING` 但未完成 → 重启 tetherd → 启动后自动续跑清理（G.2 ①b）。
- 模拟孤儿 stream（手动创一个 DB 没有的 `history-xxx`）→ broker 启动自动清理。
- 磁盘人为灌到 80% → 收到 `sys.events{type:disk_pressure}`。

**出口**：每个动词都在流里找得到；`session rm` 不留垃圾。

---

## P8 — 对账（reconciliation）

**目标**：agent 或 broker 崩后重启自动恢复一致状态。

**做**
- **G.1 agent 重连**：`register.req` → broker 响应带"本 agent 名下 RUNNING 进程快照" → agent 双向收敛（broker 认为 RUNNING 但 agent 无 → 标 EXITED(-1)；agent 有但 broker 无 → kill orphan）。
- **G.2 broker 重启**（option β）：从 SQLite 加载快照；**v1 不 revoke JWT**；扫 `state=DELETING` 续跑 session rm（H.3）；重新 embed frps；重订 auth_callout。
- **G.3 ctl 重连**：不 replay，直接取最新快照。
- ALLOCATED→REVOKED 的 15min OFFLINE 计时在 broker 重启后保留剩余时间（持久化 `nodes.last_heartbeat_at`，重启时推算剩余阈值）。
- process LOST 检测（超时无心跳 + 状态不可确认）。

**测试**
- chaos 脚本：`tether exec sleep 9999` × 10 → kill agent → 重启 agent → 10 个进程全翻 EXITED(-1)。
- 同上 kill broker → 重启 broker → agent 自动重连 → 状态一致。
- 人为 orphan：`kill -STOP <broker>` 期间 agent 上起进程 → broker 恢复后该 orphan 被 kill。
- 长稳定性：跑 24h，每小时注入一次 chaos，结束时状态一致。

**出口**：通过 24h + 24 次 chaos 稳定性测试。

---

## P9 — 生产表面：caddy + admin socket

**目标**：split ports 生产部署 + 本地 admin 通道。

**做**
- Caddy 仅反代 `:443` → `127.0.0.1:8222`（NATS WebSocket 内部口）；不触碰 frp 流量。
- frps（tetherd 内嵌）直接监听 `:7000`（frpc control, frp 原生 TLS）+ `:14000-14999`（业务 remote_port）。
- ACME 自动签 443 的域名证书；`broker.yaml` 配置段（见 A.3 骨架）。
- `internal/adminsock`：`/var/run/tether/admin.sock`（仅 `tether` 用户 600）。
- `tether admin {sessions, nodes, audit, evict}` 客户端。

**测试**
- 外网 `curl https://<broker>/install.sh`、`wscat wss://<broker>:443/nats` 均通；`telnet <broker> 7000` 能握手 frpc control；`curl http://<broker>:14022` 访问业务 expose。
- 非 `tether` 用户访问 admin.sock → permission denied。
- `tether admin evict lab lab-1` → agent 1s 内下线 + JWT revoke（或 v1 简化：等 TTL）。

**出口**：broker 防火墙放行 `:443` + `:7000` + `:14000-14999`；agent 出站需能到 `:443` + `:7000`。不支持"agent 仅出站 443"（v2 目标）。

---

## P10 — 分发（install.sh / goreleaser）

**目标**：三条 `install.sh` 路径可把系统部署到 fresh 机器（人工启动）。

**做**
- goreleaser 配置：linux/amd64 + arm64（darwin 可选）。
- `install.sh --role {agent, ctl, broker}`（K 章）——**只铺文件、不启动服务**。
- systemd unit 模板：system 级 for broker；user 级 for agent。
- `tether agent --install-user-service` 生成用户级 unit。
- `tether node upgrade` 主动触发 + sha256 + url 白名单（J.4）。

**测试**
- 3 台 fresh VM（或 docker）：1 broker + 2 agent，走 install.sh → **人工启动** → `tether login` + `tether exec` 端到端通。
- 断言：`install.sh` 执行完**没有任何 tether 进程在跑**（`pgrep tether` 空）。
- `tether node upgrade lab-1` 升级成功；伪造 sha256 被拒；非白名单 url 被拒。

**出口**：外人按 README 30 分钟内在 3 台机器完成部署。

---

## P11 — Release hardening

**目标**：v0.1.0 正式发布。

**做**
- E2E 测试矩阵：P2-P10 各 phase 测试合并到 `test/e2e/`，CI 每夜跑。
- 错误提示 UX 走查：每个 error 有面向人的说明 + 建议动作。
- README / quickstart / troubleshooting。
- `docs/architecture.md` 最终校对。
- GitHub Release v0.1.0：goreleaser 触发，挂 tarball + SHA256SUMS。

**测试**
- CI 夜跑稳定 ≥ 7 天无 flaky fail。
- README 从零走一遍无死链、无过时命令、无未定义术语。
- 全仓 `grep -iE 'push|pull|文件搬运|文件传输'`（排除 "pub/sub push 接收" 与 "git push"）无 verb 级残留；`tether --help` 无 `push` / `pull` 子命令。

**出口**：v0.1.0 打 tag；外部用户按文档独立部署成功。

---

## 里程碑映射

| 里程碑 | 完成时机 | 外部可见度 |
|---|---|---|
| "能看到 node" | P2 结束 | 仅你自己 |
| "能远程跑命令" | P5 结束 | 你 + 合作者（无公网） |
| "能暴露端口" | P6 结束 | jupyter / tensorboard 演示可用 |
| "全功能 dev" | P8 结束 | 内部 dogfood |
| "公网 alpha" | P9 结束 | 可邀请外部人 |
| "可下载" | P10 结束 | install.sh 发布 |
| "v0.1.0" | P11 结束 | GitHub release |

---

## 关键依赖警告（违反即返工）

**不能跳序的**（"先发芽" 原则硬约束）：
- ❌ P5（PTY）依赖 P4 的 proc 状态机 —— 不能先做 run 再做 exec。
- ❌ P3 之前做 JWT permissions 没意义 —— 需要 session 概念。
- ❌ P7 之前做 `history` 命令 —— stream 还不存在。
- ❌ P8 之前做公网部署（P9）—— 崩溃恢复未验证过，线上炸。
- ❌ P10 之前做 `node upgrade` —— 还没 release artifact 给它下。

**可并行的**（树分叉）：
- P9（caddy）与 P10（install.sh）后期可并行。
- P11 文档在各 phase 完成时增量写，不留到最后。

---

## 每进入新 phase 的 checklist

- [ ] 前一 phase 的"出口"断言全部通过。
- [ ] 本文档当前 phase 状态翻成 ✔。
- [ ] 新开分支 `phase/<N>-<slug>`；每个 phase 至少一个 PR。
- [ ] `architecture.md` 若在实现中发现设计问题，**先改文档再改代码**。
- [ ] 单元测试 + e2e 测试在同一 PR 内落盘，不拖延。

---

## phase 0 之前需要决定的外部变量

- [ ] GitHub 组织 / 用户名（决定 `module github.com/<?>/tether`）。
- [ ] 第一个 broker 部署的域名（决定 install.sh 反代目标）。
- [ ] 首发是否包含 arm64（决定 goreleaser 矩阵）。

这三项不阻塞 P0-P1，但应在 P9 之前敲定。
