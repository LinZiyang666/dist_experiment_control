# P13 — Session-scoped proxy subscription（"自建机场"）· Plan（定稿）

> 本文是 P13 的**唯一定稿 plan**（CLAUDE.md §3 阶段 A step 2，主进程定稿）。
> 草拟来自 5 专家对抗 workflow（`p13-plan-draft`，5 起草 → 5 对抗审查 → 1 综合），
> 主进程逐条评估、解决全部 `unresolvedForMainProcess`、并修正综合稿里的一处技术误判后定稿。
> 实现以本文为尺；实现中若发现设计问题，**先改本文再改代码**（§3 checklist）。

---

## 0. 主进程决议摘要（关闭 workflow 留下的所有 fork）

| # | 未决项 | 主进程决议 |
|---|---|---|
| **DEP fork（核心）** | SS2022-EIH 多密钥需 blake3（x/crypto 没有）vs 经典单密钥一订阅一端口 | **都不选。** 走第三条路：经典 `chacha20-ietf-poly1305` + **试解密（trial-decryption）多密钥同端口**（Outline pre-2022 机制）。服务端用每个 PSK 对连接首部明文 salt 派生 subkey、试 AEAD 解密第一个 length chunk，认证通过的 PSK 即该订阅。仅依赖 `golang.org/x/crypto`（chacha20poly1305 + HKDF-SHA1，已在树内），**无 blake3、无新重依赖、CGO-free、兼容经典 Clash for Windows**（用户明确指定的客户端），且保留**一 agent 一个 `__proxy__` 端口** + 每订阅可撤销。O(N) 试解密对每条新连接、N=活跃订阅数（数十）可忽略。 |
| cipher 字符串 / Clash 客户端 | 需锁定客户端能认的 cipher 串 | `chacha20-ietf-poly1305`。经典 Clash for Windows **与** Clash-Meta 都认，不强制 Clash-Meta。golden fixture 锁定渲染串。 |
| proxy status 可见性 | owner-only vs member-readable | **member-readable**（status 不含任何 secret），`on/off`、`sub create/revoke` 仍 **owner-only**。 |
| 订阅 token TTL | 是否 v1 加 `--expires` | **延后**到后续增量；v1 手动 `revoke` 为主。 |
| fail-closed grace N | 在线 agent 与 broker 失联多久后自毁 SS | **统一锚到既有 15min OFFLINE→REVOKE 阈值**：(a) broker 侧权威——agent OFFLINE≥15min 其 `__proxy__` 端口被 REVOKE、隧道断、公网口不可达；(b) agent 侧兜底——NATS **持续断连 ≥15min** 主动 `Stop()` SS。单阈值=15min，撤销陈旧窗口 ≤15min，记为残余威胁 R-1'。 |
| 两 PR vs 单 PR | phase/13 拆几个 PR | 本阶段**停在外审、不 commit**。全部工作落在单分支 `phase/13-proxy-subscription`；是否拆 2 PR 留到 step 7 push 时定。 |
| allPhases 矩阵回填 | 矩阵止于 p10 | **只 append `"p13"`**。p11/post-1.0（file-transfer、ps-retention）不在矩阵是既有缺口，**不在 P13 内回填**（避免 scope 蔓延），仅作为一条 note 提请外审知悉。 |

**对综合稿的修正**：综合稿（§4）把 cipher 当作"SS2022-blake3 多密钥 vs 经典单密钥多端口"的二选一，遗漏了经典 cipher 的试解密多密钥路径。本文采用试解密路径，因此：**不引入 blake3**、`port_allocations` 保持"一 agent 一 `__proxy__` 行"（不退化成一订阅一行）、不要求 Clash-Meta。其余综合稿结论全部采纳。

---

## 1. Scope & non-goals

**In scope（锁定需求）：** per-session、owner-only 总开关 `tether proxy on/off/status`；总开关 ON 后每个 ONLINE agent（含后加入者）自动起内嵌纯 Go shadowsocks AEAD server 并自动 expose 其端口；每订阅可撤销 token（`proxy sub create/ls/revoke`）；broker 托管、自动更新的 HTTPS 订阅 URL（`https://<broker>/sub/<token>`），Clash 可导入；**转发全部流量、不做规则**，每个 ONLINE agent = Clash 里一个可选节点；订阅消费者是任意 link 持有者（含非 tether 成员）。

**Non-goals（v1 砍掉；守北极星 §2）：** 无可配订阅数上限/`proxy_sub_limit`（若需 keyset 上限，用一个**写死的宽松常量**，不入 serveconf、不作 error code）；无流量计量/计费/告警；无 UDP relay（仅 TCP）；无自研 HTTP rate limiter（Caddy 在 :443 前置）；无 SIP008（v1 仅 Clash YAML）；无 egress 过滤/按订阅限速/目的地规则（转发全部已锁定）。

**北极星守卫：** proxy 默认全局 OFF；OFF 时整个 P13 表面惰性、与 v0.2.8 **字节等价**（无 HTTP listener 除非显式配置、register-resp 无 proxy 块、无新 sys.events）。由回归测试断言。

---

## 2. Wire / proto —— 全 additive，**不 bump proto**（ProtoVersion 保持 1）

理由（沿用 P12 RemotePort 打法）：新 subject 不与严格 register 握手碰撞（旧 broker 对 `proxy.*` 直接 `nats: no responders`）；所有新字段 `omitempty`；register-resp 的 proxy 块用**指针**，proxy-off 时与 v0.2.8 字节等价。为一个**纯 opt-in** 功能做 v1→v2 会逼全栈重装——拒绝。由 `proto_invariants_test` 断言 `ProtoVersion==1` + 一条 proxy-off `NodeRegisterResp` 的 golden 字节等价测试钉死。

**新 owner 命令 subject（`ctrl.by` 树、session-scoped、非 node-scoped、非 `.forwarded`）：**
```
SubjCtrlProxySet(actor,sid)       -> tether.v1.ctrl.by.<A>.s.<sid>.proxy.set.req      (leaf 5 token)
SubjCtrlProxyStatus(actor,sid)    -> ...proxy.status.req                               (leaf 5 token)
SubjCtrlProxySubCreate(actor,sid) -> ...proxy.sub.create.req                           (leaf 6 token)
SubjCtrlProxySubList(actor,sid)   -> ...proxy.sub.list.req                             (leaf 6 token)
SubjCtrlProxySubRevoke(actor,sid) -> ...proxy.sub.revoke.req                           (leaf 6 token)
```
单 `proxy.set.req{Enabled bool}` 同时承载 on/off（一条 JWT literal、一条 registry 项；CLI on→{true} / off→{false}）。**同一个 builder 被 permissions 模板与 `cmd/tether/proxy.go` 共用**，加一条交叉测试防字符串漂移。

**新 per-(sid,nid) Agent-only keyset 下推 subject（机密 live-delta 通道）：**
```
tether.v1.s.<sid>.cmd.node.<nid>.proxy-keys.req.forwarded
```
它**复用既有** broker-Pub `s.*.cmd.node.*.*.req.forwarded` 与 agent-Sub `s.<sid>.cmd.node.<nid>.*.req.forwarded` 通配 → **零 JWT 改动**，且 JWT 作用域保证只有目标 agent 能订、ctl 证明性地不能 pub `.forwarded`（C.4 不变式）。在 agent `dispatchForwarded`（exec.go）switch 加 verb `proxy-keys`。body = `ProxyDirective`。无 reply（best-effort live delta；register-resp + 重连再注册是权威兜底）。

**新消息结构（`internal/proto/messages.go`，全 omitempty）：**
```go
type ProxyKey struct {            // 一个 ACTIVE 订阅的 SS 凭据
    SubID  string `json:"sub_id"`
    Secret string `json:"secret"` // base64 PSK
}
type ProxyDirective struct {      // 权威的 per-node proxy 状态
    Enabled    bool       `json:"enabled"`
    PublicPort int        `json:"public_port,omitempty"`
    Token      string     `json:"token,omitempty"`   // tunnel token（明文，仅一次）
    Cipher     string     `json:"cipher,omitempty"`  // 锁定 cipher 串 "chacha20-ietf-poly1305"
    Keys       []ProxyKey `json:"keys,omitempty"`
    Epoch      int64      `json:"epoch,omitempty"`   // broker-DB-单调 keyset 版本
}
type ProxySetReq struct{ Enabled bool `json:"enabled"` }
type ProxySetResp struct{ OK, Enabled bool; AffectedNodes int `json:"affected_nodes"`; Code, Error string }
type ProxyStatusReq struct{}
type ProxyStatusResp struct{ Enabled bool; Nodes []ProxyNodeEntry; Subscribers []ProxySubEntry; SubURLPrefix string; Code, Error string }
type ProxyNodeEntry struct{ NID, Status, PublicHost string; PublicPort int; Ready bool `json:"ready"` }
type ProxySubEntry struct{ Name, State string; CreatedAt time.Time; RevokedAt *time.Time `json:"revoked_at,omitempty"` } // 永不含 token/psk
type ProxySubCreateReq struct{ Name string }
type ProxySubCreateResp struct{ OK bool; Name, SubURL string; Code, Error string } // SubURL 只打印一次
type ProxySubListReq struct{}
type ProxySubListResp struct{ Subs []ProxySubEntry; Code, Error string }
type ProxySubRevokeReq struct{ Name string }
type ProxySubRevokeResp struct{ OK bool; Name, Code, Error string }
```
**NodeRegisterResp 扩展（唯一锁定形态——指针+omitempty）：**
```go
Proxy *ProxyDirective `json:"proxy,omitempty"` // nil == proxy off == 与 pre-P13 字节等价
```
**sys.events 新 kind —— 仅 secret-free 提醒：** `proxy_enabled{sid}`、`proxy_disabled{sid}`、`proxy_keyset_changed{sid}`。**PSK / keyset / port / token 永不上 sys.events**（它对 member 与 unactivated 可订）。`permissions_test` 断言任何承载 PSK 的 subject 都不被 Unactivated/ActivatedMember 模板 Sub 放行。

---

## 3. Storage —— 单一 migration `0006_proxy.sql`

```sql
-- 总开关：sessions 上的 additive 列（无独立表，随 session 行消亡）
ALTER TABLE sessions ADD COLUMN proxy_enabled INTEGER NOT NULL DEFAULT 0 CHECK (proxy_enabled IN (0,1));

-- agent SS 已绑定 ACK 标志（render gate；pre-P13 agent 永不置 1 → 永不被渲染）
ALTER TABLE nodes ADD COLUMN proxy_ready INTEGER NOT NULL DEFAULT 0 CHECK (proxy_ready IN (0,1));

CREATE TABLE proxy_subscribers (
    sid          TEXT      NOT NULL REFERENCES sessions(sid) ON DELETE CASCADE,
    sub_id       TEXT      NOT NULL,            -- ULID，稳定内部 id（== SS 试解密候选标识）
    name         TEXT      NOT NULL,            -- owner 起的标签 "alice"
    token_hash   TEXT      NOT NULL,            -- SHA256(raw 32B url-safe sub token)；raw 永不存
    psk          TEXT      NOT NULL,            -- base64 SS PSK；可恢复 secret（见 R-2）
    cipher       TEXT      NOT NULL,            -- 锁定 cipher 串
    state        TEXT      NOT NULL DEFAULT 'ACTIVE' CHECK (state IN ('ACTIVE','REVOKED')),
    created_by_fp TEXT     NOT NULL,
    created_at   TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    revoked_at   TIMESTAMP,
    PRIMARY KEY (sid, sub_id)
);
CREATE UNIQUE INDEX idx_proxy_sub_token_active ON proxy_subscribers(token_hash) WHERE state='ACTIVE';
CREATE UNIQUE INDEX idx_proxy_sub_name_active  ON proxy_subscribers(sid, name)   WHERE state='ACTIVE';
CREATE INDEX        idx_proxy_sub_sid_state    ON proxy_subscribers(sid, state);

-- 防止一个 agent 重复分配 proxy 端口：__proxy__ 是 port_allocations 的保留 name 行，
-- (sid,nid) 上 ALLOCATED 且 name='__proxy__' 唯一。
CREATE UNIQUE INDEX idx_port_alloc_proxy_unique
    ON port_allocations(sid, nid) WHERE state='ALLOCATED' AND name='__proxy__';
```
PSK 存明文（不 hash）：agent 必须用它解密、HTTP renderer 必须吐出它；**sub token 才 hash**。DB 本就是 0600 crown-jewel（pin_hash、token_hash）。在 DDL 注释 + storage README + 残余威胁 R-2 处**响亮记录**。

**session-rm 级联（综合稿 Critique 2/4 BLOCKER，已核对 `internal/broker/audit.go:89` `dropSessionRows` 是逐表枚举而非纯靠 FK cascade）：** 在 `dropSessionRows` 的语句数组里、`DELETE FROM sessions` **之前**加 `DELETE FROM proxy_subscribers WHERE sid = ?`。`proxy_enabled` 随 sessions 行消亡。

**新包 `internal/proxysub`：** `Create(db,sid,name,fp,now)→(Subscriber{Token raw 仅一次, PSK})`、`ListBySession`、`ListActive`、`LookupByTokenHash`（仅 ACTIVE）、`Revoke(db,sid,name,now)`、`HashToken`。沿用 `internal/port` 习惯（crypto/rand 生成 token+PSK、partial-unique-index 单赢家、`ErrNameTaken`/`ErrNotFound`）。session 辅助 `GetProxyEnabled`/`SetProxyEnabled(...)→changed`（`UPDATE ... WHERE state='ACTIVE'`，对 C.1 §6 做纵深防御）。`SetProxyReady(db,sid,nid,bool)`。

---

## 4. Agent 侧

**内嵌 SS —— 自研 vendored 纯 Go 子集（plan of record，非整 module）：** 新包 `internal/agent/ssproxy`，约 250–350 LOC，仅实现：
- AEAD 经典 shadowsocks stream 协议（salt + HKDF-SHA1 派生 subkey + chacha20poly1305 分块），全用 `golang.org/x/crypto`。
- **多密钥试解密 CipherList**：`SetKeys([]Key)` 原子换 snapshot；新连接读 32B salt 后，对每个 ACTIVE PSK 派生 subkey 试解密首个 length chunk，认证成功者绑定该 conn 到对应 `KeyID`。
- 与 `internal/tunnel` 替换 frp 同样的"自研子集"先例；**不引入 outline-ss-server 整 module**（避开其 prometheus 等重 indirect 依赖，保证 `go list -deps | grep -i cgo` 为空）。

```go
type Key struct{ KeyID, Secret string } // KeyID==sub_id；Secret=base64 PSK；cipher 全局锁定
type Server struct{ /* ln, ciphers snapshot, perKeyIDConns, wg, cancel, localPort */ }
func (s *Server) Start(ctx context.Context, wantLocalPort int, keys []Key) (localPort int, err error) // 绑 127.0.0.1:wantLocalPort(0=OS 选)
func (s *Server) SetKeys(keys []Key) error // 原子换 snapshot；并 force-close KeyID 消失的在飞连接（硬撤销，Critique 2/5）
func (s *Server) Stop()                     // 幂等；关 listener 后 wg.Wait
func (s *Server) LocalPort() int
```
- 绑 **127.0.0.1**（非 0.0.0.0）：SS server 只经 yamux 隧道可达（broker 公网 :14xxx → tunnel stream → agent dial 127.0.0.1:localPort）。公网暴露面是 broker:14xxx（世界可达明文 TCP，载荷是 SS-AEAD 密文），SS PSK 是唯一门。
- **空 keyset ⇒ 拒绝所有握手**（fail-closed）。
- Leak-safe：单 accept goroutine + wg 跟踪的 per-conn goroutine；ctx 为 `runCtx` 子；`Stop()` 也接到隧道会话丢失。per-KeyID 在飞连接表与 snapshot 同锁，供 force-close。

**state.json（`internal/agent/state.go`）：** `StateFile` 加 `Proxy *ProxyState{PublicPort,LocalPort,Token,Epoch}`（指针、omitempty、旧文件照常加载）——**仅持久化隧道 footprint + 最后应用 epoch；PSK 永不落盘**（每次 (re)register/push 由 broker 重发）。新增 `SetProxy/GetProxy/ClearProxy`（同 `mu` + tmp+rename 原子写）。`PortToken` 与 `Proxy` **隔离**，保证 `__proxy__` 端口绝不与用户 expose 名/端口冲突，并各自按 on/off 生命周期 GC。

**编排 `internal/agent/proxy.go`：** `proxyMgr{mu, srv, cancel, epoch}` 串行化 apply。`applyProxyDirective(ctx, d *ProxyDirective)` 幂等且 **epoch-guarded（!= 即替换，非 >，以扛 broker DB 还原后 epoch 回退）**：
- `enabled` ⇒ 在新 127.0.0.1 端口起 SS（`Start`）+ 经 `TunnelExposeAdapter` 为 (PublicPort,Token) 开隧道 + `SetKeys` + 持久化 `Proxy` + **回 ACK 让 broker 置 `proxy_ready=1`**（见 §5）。
- `disabled` ⇒ `Stop` SS + 关隧道 + `ClearProxy`。
- 保留 name `__proxy__` 在 agent 侧也拒绝用户 expose（broker 为主守卫、agent 纵深防御）。

**proxy_ready ACK：** agent 成功 `Start`+开隧道后，pub 一条 `tether.v1.s.<sid>.ev.node.<nid>.proxy.ready`（复用 agent 既有 `s.<sid>.ev.node.<nid>.>` pub 权限，零 JWT 改动）；broker 订之置 `nodes.proxy_ready=1`。disabled/Stop 时 pub `...proxy.unready` 置 0。

**forwarded verb：** `dispatchForwarded` 加 `case "proxy-keys": go a.handleProxyKeysForwarded(...)` → 解 `ProxyDirective` → `applyProxyDirective`。

**ReconnectHandler（综合稿 Critique 4 BLOCKER；已核对 agent 仅在 `Run` 内 register 一次、heartbeat 无 reply）：** 在 agent 连接时装 `nats.ReconnectHandler`，重连后触发**一次**重新 register（复用既有 register 路径；broker `handleRegister` 对 boot_id 幂等，G.1）。register-resp 即重连/broker 重启后的权威收敛路径，**不发明 heartbeat 请求/应答协议**。

---

## 5. Broker 侧

**Handlers `internal/broker/proxy.go`**（5 个 handler，严格照 `handleSessionRm`/`handleUpgradeReq` 模板）：`ParseCtrlBy` → `parseProxyLeaf` **精确 token 数**校验（set/status=5、sub.*=6，否则 `subject_malformed`，**在任何 DB 之前**）+ `ValidateSID` → `auth.FingerprintFromActor` → `session.IsActive`（C.1 §6 precheck **最先**）→ `session.IsOwner`（**status 例外：member 可读**）→ 否则 `not_owner` + `audit{kind:admin_denied}`。在 `broker.go` inline registry 注册 5 条 subject。

- **proxy.set on：** `SetProxyEnabled(true)`；对每个 **ONLINE 且 P13-capable** node（capability gate：`release_version >= P13 release`，用最小 semver 比较；pre-P13 不分配不下推）reuse-or-allocate `__proxy__` 端口（`port.Allocate(db,sid,nid,"__proxy__",0,0,ownerFP,cfg)` + `idx_port_alloc_proxy_unique` 保单赢家）；经各 node 的 `proxy-keys.req.forwarded` 下推 `ProxyDirective{enabled,port,token,cipher,ListActive keys,epoch}`；emit secret-free `sys.events{proxy_enabled,sid}`；reply `AffectedNodes`=已下推的 ONLINE 数（实际就绪以 `proxy status` 的 `Ready` 为准）。audit `proxy.on`。
- **proxy.set off：** `SetProxyEnabled(false)`；对每 node 下推 `ProxyDirective{enabled:false}`（agent 停 SS 并驱动自身 `__proxy__` expose-rm，走既有审计 `port.Free` 路径——broker 不跨 node 记端口账）；emit `proxy_disabled`。audit。
- **sub create：** `proxysub.Create`；若 ON：bump epoch + 向每 node 下推更新 keyset + emit `proxy_keyset_changed`；reply `SubURL` 一次；audit `{name,sub_id}`，**无 token/psk**。
- **sub ls：** `ListBySession` → 脱敏 entry。
- **sub revoke：** `proxysub.Revoke`；若 ON：bump epoch + 下推缩减 keyset（agent `SetKeys` 硬撤销在飞连接）+ emit `proxy_keyset_changed`；audit。

**Register-time（`handleRegister`，在 reconcile 之后）：** 若 `proxy_enabled` 且 node P13-capable，reuse-or-allocate `__proxy__` 端口，置 `resp.Proxy = &ProxyDirective{enabled,port,token,cipher,ListActive,epoch}`。这是**加入时**的权威路径。pre-P13 agent 收到未知 `proxy` 字段 JSON 忽略、不绑 SS、不 ACK ready → 永不被渲染（自然排除，无需 hard error）。

**收敛矩阵：** join=register-resp 权威；连接态 live-delta=push（epoch-guard apply）；flap/重连=ReconnectHandler→单次再 register→新 register-resp；broker 重启=弹连的 agent 再 register、未断的 agent 续服（可用性）后对账；**fail-closed=15min**（§9）。

**JWT（`internal/auth/permissions.go`）—— 唯一一处改动：** `PermissionsForActivatedMember`（permissions.go:53/55）的 Pub Allow **+5 条固定 token literal**，各自钉死 `by.<actor>` 与 `s.<sid>`（无通配）。Broker Sub `ctrl.by.*.>` 已路由它们；broker Pub `s.*.cmd.node.*.*.req.forwarded`、agent Sub `s.<sid>.cmd.node.<nid>.*.req.forwarded` 已覆盖 `proxy-keys`；agent `ev.node.<nid>.>` 已覆盖 `proxy.ready`。测试断言 **Broker+Agent 模板字节未变**。

---

## 6. HTTP 订阅面 —— tether 首个对外 HTTP 字节（最小化）

**先改文档（项目规则 §3）：** 修订 requirements §4.3 切出"只读订阅端点"例外；在 architecture 子系统图 + install.sh Caddyfile 契约补这条 listener。

新包 `internal/subhttp`：独立 `net/http.Server`，绑 **loopback 127.0.0.1:8090（写死）**，**仅 GET、只读（只 SELECT，无 NATS handle、无写）**。锚到 `Run` 的 ctx，`context.AfterFunc(ctx, srv.Shutdown)`。`SubHTTPAddr==""` 时**整体不启用**（所有 pre-P13 部署不变）。经 `broker.Config.SubHTTPAddr/SubPublicHost`、`serveconf.BrokerSection.Sub`、`serve.go` flag（`--sub-http-listen` 默认 `""`）、`install.sh` broker.yaml 串起。

**Caddy（控制面关键改动）：** 在既有 `$DOMAIN:443` 块内、catch-all `handle { reverse_proxy 127.0.0.1:8222 }` **之前**加路径作用域 `handle /sub/* { reverse_proxy 127.0.0.1:8090 }`。bump install.sh Caddyfile pin-policy 注释；要求 `log.md` 人工验证 NATS WSS 改写后仍 upgrade + 一条 e2e/CI 断言经 Caddy 的 WSS 仍可连。

**路由 `GET /sub/{token}`：** 末段 hash → `proxysub.LookupByTokenHash`（仅 ACTIVE 索引）。**unknown / revoked / DELETING-or-missing-session 一律单一 404**（消除 410/存在性 oracle，单查询无 oracle）。非 GET→405。限制输入长度。`session.IsActive` precheck。仅渲染 **Clash YAML**（v1）：
```yaml
proxies:
  - {name: <nid>, type: ss, server: <publicHost>, port: <14xxx>, cipher: chacha20-ietf-poly1305, password: <psk>}
proxy-groups: [{name: "tether-<sid-short>", type: select, proxies: [<nid>...]}]
rules: [MATCH,tether-<sid-short>]
```
Header：`Cache-Control: no-store`、`Referrer-Policy: no-referrer`、`Content-Type: application/yaml`、`Profile-Update-Interval: 1`。空节点列表 ⇒ 合法空 doc。

**Live 节点装配（自动更新、每请求查）：** `LiveProxyNodes(sid)` = `port_allocations.name='__proxy__' AND state='ALLOCATED'` JOIN `nodes.status='ONLINE' AND proxy_ready=1`，按 nid 排序。死 agent 下次刷新自动掉（ONLINE 门早于 15min 端口计时）。pre-P13 / 未就绪 agent 因 `proxy_ready=0` 被排除（消除老 agent 黑洞，Critique 2/3）。

---

## 7. 安全与威胁模型

- **两类 secret：** 隧道 port token=仅 hash（agent↔broker）；SS PSK=at-rest 可恢复（agent 要解密、broker 要吐出）；sub token=仅 hash 的 bearer（与 PSK 独立，1:1 token↔订阅，泄漏一个只暴露该订阅）。
- **PSK 永不上 sys.events**（member/unactivated 可读）。只经 register-resp（`_INBOX`）与 per-(sid,nid) Agent-only `.forwarded` push 下发。
- **硬撤销：** `SetKeys` force-close 掉 KeyID 的在飞连接。SLA：被撤 key 一个 push/重连内拒新连 + 断在飞；离线 agent 重连即应用（绝不复活）。CLI + usage.md 写明。
- **`proxy on` 强制确认：** `tether proxy on` 需 `--yes`（或交互默认 N）+ 责任文案：每个 ONLINE agent 成为对任意 link 持有者（含非成员）开放的互联网出口；其 IP 出任意流量；你负责；一键关闭=`proxy off`。无 `--yes` 且非交互 ⇒ 非零退出、DB flag 不变。
- **TestProxyAuditNoSecrets（keystone）：** 跑 on/create/revoke/register/fetch，grep audit + history-<sid> + sys.events + 日志 + 落盘 state.json，断言 raw token + PSK 字节 + 完整 /sub URL **处处缺席**。
- **数据跳证明：** broker:14xxx 只见 SS-AEAD 密文 + 元数据；空 keyset 拒一切。
- **残余威胁（architecture E.6 风格，文档化、接受）：** R-1' 撤销陈旧窗口 ≤15min（partition 期间被撤订阅可经被隔离 agent 续用至多 15min，到点端口 REVOKE/agent 自毁）；R-2 broker DB 存可恢复 PSK（文件权限 + revoke-rotate，不做假加密）；R-4 开放中继/运营者责任（**主导风险**，仅靠警告 + revoke + off 控制）；R-5 token-in-URL 泄漏（1:1 可撤 + no-referrer + no-store；TTL 延后）；R-7 broker 处流量分析；R-8 非成员 bearer 除 token 外无身份。

---

## 8. CLI 表面

`cmd/tether/proxy.go` `newProxyCmd()`（main.go 注册）。每个 leaf 照 expose.go 模板（ReadCurrentSession 守卫、ResolveNATSURL、EnsureIdentity、ConnectNATSWithNkey、RequestWithContext 15s、resp.Code→brokerErrorMessage）。
```
tether proxy on            -> ProxySetReq{Enabled:true}   （需 --yes / 交互默认 N + 责任文案）
tether proxy off           -> ProxySetReq{Enabled:false}
tether proxy status        -> ProxyStatusReq              （--json；人读表 NID/STATUS/READY/EXIT + 订阅列表）
tether proxy sub create --name alice -> ProxySubCreateReq （打印 SubURL 一次 + "立即保存；撤销用 ..."）
tether proxy sub ls                  -> ProxySubListReq    （alias list；NAME/STATE/CREATED/REVOKED）
tether proxy sub revoke <name>       -> ProxySubRevokeReq  （位置参数或 --name）
```
client 侧 `--name` 校验：1..64 可打印 ASCII，无 `/`。completion `CompleteProxySubNames`（仿 expose）。error_hints 补：`proxy_disabled`、`sub_name_invalid`、`sub_name_taken`、`sub_not_found`、`already_revoked`、`subject_malformed`、`proxy_unsupported_broker`（`nats.ErrNoResponders` 映射，新 ctl/旧 broker 的 skew）。

---

## 9. 对账与生命周期

- **Join：** register-resp `Proxy` directive = per-node 权威真相。
- **Live delta（连接态 agent）：** broker 经 `proxy-keys.req.forwarded` push；epoch-guard apply。
- **Flap/重连：** NATS ReconnectHandler ⇒ 单次 re-register ⇒ 新 register-resp（补上"不会自发重注册"的洞）。
- **Broker 重启：** 弹连 agent 再 register；未断 agent 续服（可用性）后对账；fail-closed 15min。
- **OFFLINE≥15min：** `__proxy__` 端口被 `reconcilePorts` REVOKE；走标准 G.1 token_hash 对账（agent 在 `LocalPorts[]` 上报它）；重连若 token 不符 broker 拒陈旧隧道 REGISTER；`ssproxy.Server.Stop()` 接到隧道会话丢失防孤儿 accept goroutine。
- **state.json 丢失致端口双分配：** 由 `idx_port_alloc_proxy_unique` + reuse-or-replace `LookupActiveByName` 防住。
- **Epoch：** broker-DB 单调、持久化进 state.json、apply-on-differ（扛 DB 还原）。
- **session rm：** 显式 `DELETE FROM proxy_subscribers`（在 sessions 删之前）；`/sub` 404；重建同名 sid 不暴露旧 token。
- **fail-closed 15min：** agent NATS 持续断连 ≥15min ⇒ 主动 `Stop()` SS（与 broker 侧端口 REVOKE 同阈值，撤销陈旧窗口 ≤15min）。

---

## 10. 测试矩阵

leak 门用**项目惯用 `runtime.NumGoroutine()` snapshot/poll-with-tolerance**（`test/concurrency/`）——**goleak 是禁用依赖**（已核对 helpers_test.go:5）。e2e：`allPhases` append `"p13"`（已核对当前止于 `p10`；p11/post-1.0 缺口仅作 note，不在 P13 回填）。把 SS 字节往返做成**in-process UNIT 测**（先例 `tunnel_test.go`），forked e2e 仅断言 control-plane + HTTP-render + revoke-removes-node，守住 90s/phase 预算。

**UNIT：**
1. proto：新结构往返；proxy-OFF `NodeRegisterResp` 无 `proxy` key、与 v0.2.8 golden 字节等价（依赖 `*ProxyDirective` 指针）；`SubjCtrlProxy*` builder 精确串；`ProtoVersion==1` 不变。
2. permissions：ActivatedMember Pub 恰含 5 条 `proxy.*` literal、各 `by.<actor>`+`s.<sid>` 钉死；既有 `TestCtlPubLocksActorSegment`/`NoTopLevelWildcard`/`NoCrossSubtreeWildcard`/`TestCtlCannotPublishForwarded` 仍绿；断言无任何 ctl 模板 Sub/Pub 承载 PSK subject；断言 Broker+Agent 模板字节未变。
3. broker leaf parser：set/status(len5) + sub.*(len6) + 畸形（多 token/缺 req/坏 sid/错 actor）⇒ `subject_malformed`，在 DB/owner 检查之前。
4. broker handlers：非 owner `proxy.set` ⇒ `not_owner` + `audit{admin_denied}`；DELETING/缺失 session ⇒ `session_not_found_or_deleting`（在 owner 检查之前）；disabled 下 `sub.create` 允许（enable 时激活）/ 重名 ⇒ `sub_name_taken`；revoke 未知 ⇒ `sub_not_found`；register 带 `proxy_enabled` ⇒ resp.Proxy 带 port+token+keyset+epoch 且恰分配一行 `__proxy__` ALLOCATED；proxy off ⇒ flag off + nudge。
5. proxysub storage：Create 返回 raw token 一次、DB 仅存 token_hash；active-name 唯一（revoke 后可重建同名）；LookupByTokenHash 仅 ACTIVE；revoke 隔离（3 订阅撤 1，另 2 PSK 不变）；SetProxyEnabled 幂等 + 拒 DELETING；migration 0006 在 pre-P13 DB 上可应用（既有行 proxy_enabled=0、proxy_ready=0）。
6. session-rm 级联**走真实 `finalizeSessionRm`/`dropSessionRows`**：建订阅→跑→proxy_subscribers 清零 且 `/sub`→404 且重建 sid 不暴露旧 token。
7. ssproxy（in-process，无网络）：2-key CipherList 试解密——key A 流由 A 解、key B 流由 B 解、未知 key 流被拒；`SetKeys` 去掉 B 后 B 在飞连接被 force-close、A 不受扰；空 keyset 拒一切；`Stop` 幂等。
8. subhttp handler：有效 token→200 + 合法 Clash YAML（golden fixture 锁 cipher 串 + 结构）；unknown/revoked/DELETING→同一 404；非 GET→405；空节点→合法空 doc；只读（无 DB 写）。
9. ssproxy + tunnel 往返（in-process UNIT，marquee）：起 ssproxy + tunnel server/client，经公网口用 SS 客户端原语跑一条字节回环到本地 echo，验证 PSK 正确解密、错误 PSK 失败。

**并发/leak（`test/concurrency/`，-race）：**
10. ssproxy Start→大量并发连接→Stop，`NumGoroutine` 回基线（tolerance）。
11. subhttp Server start→ctx cancel→goroutine 归零。
12. proxyMgr 并发 applyProxyDirective（on/off/keyset 交替、epoch 乱序）无 data race、最终态一致。

**e2e（`test/p13/`，进 matrix，<90s）：**
13. proxy on（owner，--yes）→agent 绑 SS + 隧道 + proxy_ready=1→`proxy sub create`→`GET /sub/<token>` 返回含该 node 的 Clash YAML→`proxy sub revoke`→刷新 `/sub` 该订阅 token 404 / keyset 不含该 key→`proxy off`→节点从渲染消失、SS 停。
14. 新 agent 在 on 之后加入→register-resp 带 directive→自动出现在 `/sub`。
15. 非 owner `proxy on`→`not_owner`。
16. 经 Caddy 的 WSS 在加了 `/sub` 路由后仍 upgrade（control-plane 不回归）。

**构建门：** `CGO_ENABLED=0 go build ./...` 绿 且 `go list -deps ./... | grep -i cgo` 空（确认无 blake3/cgo 引入）；golangci-lint v2 绿。

---

## 11. Exit criteria（出口）

`make test` + `make e2e`（p13 在 matrix）+ `make lint`（golangci-lint v2）全绿；并发门过 `-race`；`CGO_ENABLED=0 go build ./...` 绿且 deps 无 cgo；`ProtoVersion==1` 不变量 + proxy-off 字节等价绿；`TestProxyAuditNoSecrets` + revoke-isolation keystone 绿；`log.md` 人工验证 Caddy-WSS-仍-upgrade + 真 Clash 客户端导入；内审（step 4-5）通过、外审（step 6）通过；**无 AI co-author trailer**。

---

## 12. 实现顺序（spike-first，单分支 phase/13-proxy-subscription）

1. **Spike commit：** `internal/agent/ssproxy` 试解密多密钥 + in-process 往返 UNIT 测（验证 cipher 决议落地、`CGO_ENABLED=0` 干净）。**这是最高风险未知，先钉死。**
2. proto（messages + subjects + builders）+ proto_invariants/golden。
3. storage migration 0006 + `internal/proxysub` + session helpers + dropSessionRows 改动 + 单测。
4. broker handlers + 注册 + JWT 一处改 + register-time + push + proxy_ready 订阅 + audit/events + 单测。
5. agent proxy.go 编排 + state.json + dispatch verb + ReconnectHandler + fail-closed 15min。
6. `internal/subhttp` + serveconf/serve.go 串接 + Caddy/install.sh + golden + 文档（requirements §4.3 / architecture / usage.md）。
7. CLI `cmd/tether/proxy.go` + error_hints + completion。
8. 并发/leak 测 + e2e `test/p13/` + append allPhases。
9. 全量硬闸（test/e2e/lint/-race/CGO_ENABLED=0 build），停在外审。

---

## 附录 — round-3 收敛/能力契约更新（supersedes §0/§5 相关条目）

外审 round-3 收敛到统一定序模型,以下契约**取代**正文里的标量 epoch 与 release-only capability 描述:

1. **`(generation, epoch)` 统一定序**:`ProxyDirective` 增 `Generation`(broker 进程启动 unix-nanos,跨重启含 DB 还原单调增)。register-reply / live push / 心跳修复全部携带同一对;agent 按字典序 `(gen,epoch)>` 应用,且仅在**成功应用**后推进已应用对。更高 generation 即便 epoch 更低也应用(DB 还原收敛);同 generation 内更低 epoch 陈旧(杜绝 OFF 后被陈旧 register-reply 复活)。**移除**原 §0/§9 的「标量 epoch 低值守卫」。
2. **心跳每条都驱动收敛**:`repairProxyEpoch` 在每条心跳被调用;ON 时 `!ready || agentEpoch != sessionEpoch` 补推 keyset(覆盖瞬时启动失败 unready 重试 + DB 还原后 agent 高 epoch 收敛);OFF 时 agent 仍服务则补推 disable。**取代**原「agent epoch < session epoch 才修复」。
3. **OFF 授权边界**:`tunnelTokenLookup` 对 `__proxy__` 行要求 session 开关 ON,否则拒授权;`proxy off` 先提交 `proxy_enabled=0` 再 CloseProxy+Free,故即便 ALLOCATED 行短暂可见也不会被 re-REGISTER 复活(配合 `killGen` 覆盖在飞 REGISTER)。
4. **显式 capability gate**:`NodeRegisterReq.Capabilities` 含 `proxy-v1` 才 P13-capable(`nodeHasProxyCap`);`isP13Capable` 纯 semver 无 dev 例外;`nodes.proxy_capable` 在 register 落库。**取代**原 §0「release_version ≥ 0.2.9 + dev 恒 capable」。

> 真实部署出口标准(真 Caddy/WSS、真 Clash 导入)仍为锁定项;in-process 已补真 CLI→NATS wire + 合并数据面覆盖,真硬件联调需项目 owner 裁决(排期到 lab 或显式修订出口标准)。

---

## 附录 — round-4 fencing/收敛/OFF 健壮性更新（supersedes round-3 附录相关条目）

1. **持久单调 generation(F1)**:`Generation` 不再是裸 `now.UnixNano()`,而是持久化 `proxy_meta.generation` 取 `max(persisted+1, now)`，跨重启即便时钟回拨也严格单调。**运维契约**:DB 还原 runbook 须把该行推进到超过任何 agent 已应用值(DB 内计数器单靠自身无法 fence DB 还原)。
2. **心跳带完整对(F2)**:`HeartbeatPayload` 增 `ProxyGeneration`;agent 上报已应用 `(gen, epoch)`;`repairProxy` 仅当 `ready && agentGen==brokerGen && agentEpoch==sessionEpoch` 才算收敛,否则补推 —— 含同 epoch 不同 generation(两份 DB 快照可同 epoch 而 keyset 不同)。
3. **OFF 不依赖 DB 杀数据面(F3)**:`tunnel.Server.CloseSession(sid)` 在 `proxy off` 提交开关后立即同步关该 session 全部公网监听 + bump killGen,**不查 DB**。`BumpProxyEpoch`/`ListBySession` 失败 ⇒ OFF 回 `store_error` 不谎报成功;`port.Free` 失败 ⇒ 不发 `freed`，stale 行由下次 ON 轮换(`proxy_ready=0` 时重铸 port+token)。

---

## 附录 — round-5 fencing 硬化（supersedes round-4 附录 F1 的「手动 runbook」)

1. **持久 generation 取不到即拒启(F2)**:`broker.New` 调 `advanceProxyGeneration`,失败**返回 error 不退化到裸 wall clock**(裸 wall clock 正是被修的 fencing 洞)。`proxy_meta` 改由**独立 forward migration `0007`** 交付(不改已应用的 0006);`advanceProxyGeneration` 加 `stored+1` 溢出守卫(`maxProxyGeneration = 1<<62`)。
2. **DB 还原自动收敛,取消手动 runbook(F3)**:round-4 附录把还原正确性委托给「未来 runbook」——**作废**。改为:broker 在心跳见 `agentGen >= brokerGen` 时 `escalateProxyGen` 原子地把持久 generation 抬到 `agentGen+1` 之上(`advanceProxyGeneration` 的 floor + 事务内 max),随后推送压过 agent。多 agent 经事务 max 收敛。`b.proxyGen` 运行时由 `proxyGenMu` 保护。
3. **session 级 kill 覆盖在飞 REGISTER(F1)**:tunnel 加 `killGenSession[sid]`;`handleAgent` 授权前同时快照 `killGen[port]`+`killGenSession[sid]`,装入前都校验;`CloseSession(sid)` **无条件** bump `killGenSession[sid]`(即便此刻无已装入监听),故 OFF 按 sid 杀也能 fence 已授权未装入的 REGISTER。

---

## 附录 — round-6 信任边界/事务/数据面硬化（12 项)

1. **F1 convergence-first**:`repairProxy` 先判 ON+ready+`(gen,epoch)` 精确相等即返回,不升代不重推（杜绝正常心跳的 `proxy_meta` 写风暴）。
2. **F2 capability-gated repair**:`repairProxy` 先查 `nodes.proxy_capable`，非 capable 节点不得影响 generation/repair/render。
3. **F3 generation 输入有界**:`escalateProxyGen` 拒绝 `>= now+10y` 或触顶（`maxProxyGeneration=1<<62`）的 agent 值；`advanceProxyGeneration` 加 `next>=ceiling` 守卫 —— 单个 capable agent 无法耗尽 generation 而 brick 全体 session。残余：完全可验证 witness（签名/外部单调存储）更强，记 R-11。
4. **F4 ForgetSession 不丢 fence**:改为 BUMP `killGenSession[sid]` + `inflightBySID` 引用计数，仅在在飞 REGISTER 清零后剪枝（在飞授权 REGISTER 不会在删 session 后装入）。
5. **F5/F6 事务化**:`SetProxyEnabledAndBumpEpoch`、`createSubAndBump`、`revokeSubAndBump` 把状态/凭据变更与 epoch bump 放一个事务；bump 失败回滚、不谎报成功。
6. **F7 fail-closed 可恢复**:`needsReestablish` 标志允许重连接受 exact-equal full directive 重建（不清零对、不重开复活洞）。
7. **F8 register 清 proxy_ready**:每次注册 `proxy_ready=0`，新进程重建数据面前不被渲染。
8. **F9 render 认主开关**:`LiveProxyNodes` 加 `s.state=ACTIVE AND s.proxy_enabled=1 AND n.proxy_capable=1`。
9. **F10 subhttp 同步绑定**:`subhttp.Bind` 同步失败由 `broker.Run` 传播。
10. **F11 SS 防重放**:每 key 有界 salt 过滤，revoke 清。
11. **F12 目的地策略**:agent 默认 deny-private（仅公网出口），`proxy.allow_private_destinations` 显式开启 —— **owner 决定**记 R-10 + CLI 警告 + plan。

---

## 附录 — round-7 目的地策略闭合 + OFF 惰性 + 配置接线（5 项)

1. **F1 DNS-rebinding TOCTOU 闭合**:废弃「校验 hostname 后再拨 hostname」；改用 `net.Dialer.Control` 在连接时校验**实际候选 IP**（校验=连接同一 IP），precheck 仅 fast-fail。
2. **F2 完整 non-public 前缀表**:`blockedIP` 加 IANA special-purpose（RFC6598 100.64/10、RFC2544 198.18/15、TEST-NET、240/4、IPv6 ULA 等）+ 云 metadata，归一化 IPv4-mapped IPv6。
3. **F3 幂等 re-ACK**:exact-equal enabled 指令且仍服务 → 只重发 ready，不重建/不推进 pair。
4. **F4 OFF 惰性**:`pushCurrentKeyset` 仅 `proxy_enabled=1` 才推；OFF 下 sub create/revoke 只提交 DB+epoch。
5. **F5 YAML 接线**:`agentYAML.Proxy.AllowPrivateDestinations`（`proxy.allow_private_destinations`）接到 `agent.Config`。

---

## 附录 — round-8 reviewer 直接修复（4 项）

1. **IPv6 目的地策略补全**:按 IANA 当前 special-purpose registry 补 NAT64
   well-known、dummy、Teredo、benchmark、6to4、第二 documentation、SRv6
   等前缀；阻断把 metadata/private IPv4 编码进 NAT64/6to4 literal 的绕过。
2. **proxy mutation 串行化**:`proxy on/off` 与 `sub create/revoke` 在同一个
   `proxyOpMu` 临界区完成 mutation + publish，关闭不同 NATS subscription
   callback 的 check-then-publish 交错窗口。
3. **无 tunnel 不 ready**:`ExposeAdapter == nil` 时 P13 start fail-closed，
   不启动 runtime、不 ACK ready，避免订阅渲染不可达节点。
4. **TunnelExposeAdapter 并发安全**:Add/Remove 串行化，`localFor` 用 RWMutex
   保护，普通 expose 与 P13 proxy 并发不再产生 map race/crash。
