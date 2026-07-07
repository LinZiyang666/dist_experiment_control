# g3-plan.md — G3（成员变 → 客户端视图自动收敛）定稿 plan

> 定稿人：主进程（唯一定稿人）。基于 Stage-A 多专家对抗性 workflow（12 个 Opus 4.8 专家：6 drafter →
> 5 critic → 1 synth，run `wf_6b6073f5-d0b`，1.45M tok，0 error）综合的候选骨架 + 主进程现场代码核验裁决。
> 本 epic = HA grow/force-single/deploy 整治 roadmap 的 **G3**（含 gotcha #1/#17/#11；#9 已修 v0.4.6）。
> 依赖 **G2 已 done**（force-single 已 DELETE-prune abandoned 出 `cluster_nodes`）。走 3 阶段 7 步，本轮到外审门。

---

## 0. 主进程定稿裁决（逐 OQ + 推翻/加固 synth 的点）

Stage-A synth 骨架质量高、已把 5 份 critique 逐条消化。主进程现场核验了全部 load-bearing 代码事实，
**采纳 synth 骨架的绝大部分，但在两处推翻/加固**（下标 ★）：

### 已核实的代码事实（定稿地基）
- **[已核实] `AdoptDecision`（`internal/agent/roster.go:136-178`）在 `pin==""` 时 TOFU roster**（`:142-145`
  `tofu := pin==""; vp = r.AccountPub`）；**seeds 仅在 `pin!=""` 时才验**（`:169`）。→ 改法二 ctl 消费前**必须结构性
  重申门A**（`PinAccountPub!=""`），否则从 NATS 应答者 TOFU-pin = 恶意 broker 注入信任锚。**seed 路径安全、roster
  路径不安全**（security kill-shot）。
- **[已核实] `DialURLs`（`internal/clusterroster/roster.go:177-222`）用客户端 templateURL 的 scheme+port+path
  （`EscapedPath()` `:194`）套到 roster 各 broker 的裸 `PublicHost`**（`:198,:206`）——**"全集群同构端口/scheme"
  是既有客户端拨号模型的内建不变量**，不是 G3 新引入。这决定 OQ-A。
- **[已核实] `seedGenBumpStmt`（`internal/cluster/seeds.go:68`）非 change-gated**（无 `WHERE changes()>0`，对比
  `rostergen.go:65` 有）；**`PlanClusterSeedsPublish`（seeds.go:79）拒空端点集 + UPSERT 无条件抹 `seed_bootstrap`**
  （seeds.go:104-105，空串照抹）；`seed_generation` MAX-floored `MAX(existing+1, now.UnixNano())`（`:71`，结构上无回滚）。
- **[已核实] 三个 membership commit 点全都只 bump roster+topology、从不碰 seed / 从不重发 seeds**：AddNode→VOTER
  （`clusteradmin.go:248`）、DrainNode retire（`clusterdrain.go:161`）、force-single prune（`force_single_online.go:294-301`，
  **best-effort**）。offline force-single（`clusteroffline/offline.go:396` pruneRosterPeers + `:423` gen bump）同理不碰 seed。
- **[已核实] `manifestBytes()`（`internal/broker/cluster_manifest.go:38`）是 atomic-load、≥30s re-sign 的缓存**，返回
  完整 `ClusterManifest`（roster+seeds）；而 `buildSignedRoster`/`buildSeedBundle` **每次调用都 ed25519 签**（DoS 放大器）。
  → 改法二 responder **必须回 `manifestBytes()` 缓存字节**。
- **[已核实] §13.8 负测（`internal/auth/permissions_test.go:119`）keys on 字面 `.cluster.` token** → 新 subject
  `...cluster-roster.req`（含 `.cluster-roster.` 而非 `.cluster.`）**保持绿**；三个 Permissions 模板位置：
  `PermissionsForUnactivated`（permissions.go:20）/`PermissionsForActivatedMember`（:55）/`PermissionsForBroker`（:204）。
- **[已核实] `refreshCtlEndpoints(ctx, home, base)`（`cmd/tether/ctl_connect.go:70`）未收 live `nc`**；唯一触发在
  `connectCtlOpts:59` 连上后、仅 `expandable`（persisted broker_url 默认路径）；四重门在 `:72`（PinAccountPub/FloorURL/
  BootstrapURL）+ `:75`（TTL 10min）。`base` 是 resolved persisted broker_url，**透明 failover 时 `FloorURL==base` 仍成立**
  → **门B 只在 operator 手动 re-point 时触发、不在透明 failover 时触发**（correctness 诊断纠偏）。

### 逐 OQ 裁决

| OQ | 裁决 | 理由 |
|---|---|---|
| **OQ-A** #1 keystone：client 端口/scheme | **选项①（模板继承 `seed_endpoints[0]` 的 scheme+port+path），否决②（加 `client_endpoint` 列）** | 与 shipped `DialURLs` 同一同构假设、同一不变量；②被四条 critic 独立攻破：**wire-compat 致命**（新列 baked 进复制 SQL，未迁移 follower apply `no such column` → panic/poison-skip 入册 → 混版分叉）、**security 致命**（join PoP 只签 `(node_id,ident_pub,nonce)`、**不覆盖 client_endpoint** → 合法但被攻陷 member 自报 `nats://attacker` → honest leader 签进 account-signed bundle = 把端点投毒门槛从"偷 account seed"降到"join 报个字符串"）、scope-dep（那是 G4 的活）、ops-reality（install.sh 强制同构 `wss://$DOMAIN:443`）。**关键辨析：①用既有已签的 `public_host`（非新增 self-report 列）→ 不引入新 poison 面。** ②留 backlog/G4。 |
| **OQ-B** #1 触发/gate/bootstrap | 共享 helper（3 online 尾 + leadership backstop）；**Go 侧确定性 sort 后 set-equal gate**；bootstrap 读回；空守卫；force-single helper 门控在 prune 成功后 | 见下 §1 #1。 |
| **OQ-B clobber-safety** ★**推翻 synth** | ★ **去 provenance KV（否决 synth B-1）→ 用 host-match 启发式** | synth 选 B-1（`seed_source` KV），但 **correctness 攻破：B-1 让 #1 对存量 racknerd 零效**（`seed_source` 缺失+非空→判 manual→永不接管，需 operator 显式 opt-in）——与"现网止血"目标冲突；且 provenance 要跨 `handleSeedsPublish` 双写、跨两 plan 锁步（synth 自承）。**改用 Draft 1 的 host-match**：stored 有 host 匹配某 broker `public_host` → 接管重派生（存量 racknerd 手动集含自己 public_host → **自动收敛、零 opt-in**）；stored 无一匹配任何 broker → operator VIP/LB 集 → **不接管（保护）**。无新 KV、无新 CLI、无锁步。security 侧亦满足（不静默覆盖 operator 意图、端点源仍是既有已签 public_host）。 |
| **OQ-B auto-派生 scheme** ★**加固 synth** | ★ **auto-派生拒 `nats://`**（保留/warn，不传播明文 PIN 载体） | security cross-cutting：`validateSeedEndpoint`（seeds.go:117）放行 `nats://` 明文；operator 手输 nats:// 是 operator 选择，**machine-derived 不是** → auto 接管后 wholesale 派生会把明文端点传播到全 broker。定稿：`DeriveSeedEndpoints` 若模板 scheme==`nats://` → 不派生（warn 保留 stored）。 |
| **OQ-C** #17 深度 | **改法二（主，mandatory）+ 改法一（cheap adjunct）**，否决"仅改法一" | 改法一（去门B）只解 operator 手动 re-point 场景（`FloorURL==base` 门 correctness 诊断）；现网痛点（floor broker 死、DNS 不变、透明 failover）下门B 放行但仍走单一 `FetchManifest(ce.BootstrapURL)`（ctl_connect.go:82）→ 超时刷不到。改法二在**刚连上的 survivor NATS conn** 上 request `manifestBytes()`，摆脱 HTTP-bootstrap 单点。 |
| **OQ-D** #11 IP | **否决 auto-IP 进 signed seeds；#11 = 既有 ctl `InviteSeeds` 手动 floor + 诚实 doc** | auto-IP 被 ops-reality 彻底攻破：install.sh broker 唯一公网面 = Caddy `wss://$DOMAIN:443`（ACME SNI 绑 domain、无 IP SAN、无公网 raw-NATS listener）→ `tls://<ip>` 对现网 broker 拨不通；Clash fake-ip 靠 `proxydial` 交 **hostname**（IP-literal 无 hostname 可交 → 直连 NAT 后不可达 → 每端点吃满 `proxyDialTimeout=10s`、把 failover 变分钟级 hang）。**security 澄清：`tls://<ip>` 无 IP-SAN 是 fail-CLOSED（可用性浪费）、非 MITM**；真 MITM 面只有 nats:// 明文 + Draft 2 self-report。`broker.public_ip` 探测/配置 **OUT**（非 v1）。 |
| **OQ-E** scope/边界 | **#1 + #17 同批；#11 = InviteSeeds+doc**；DEFER ②→G4、auto-IP/`broker.public_ip`→backlog；**OUT** 任何需 install.sh 渲染的新 `broker.yaml` key（G3 非 deploy-tier 批） | 模板来源改用 `seed_endpoints[0]` 继承（不引入新配置 key，不把 G3 拖进部署面）。G4 边界：`cluster add` 复用同一 helper；G7 边界：rebalance 订阅 staleness 是数据面、正交。 |
| **OQ-F** 现网痛点 | 见 §4 上线序列 | ★ host-match 启发式下 racknerd **无需 opt-in**：下次 leadership backstop（重启/re-acquire）自动接管收敛（手动集含 racknerd public_host）。 |
| **OQ-G** offline seed 收敛 ★ | ★ **IN（offline drop-only：移除 departed host + MAX-floor bump）** | offline force-single 是 racknerd 类事故的实际恢复路径；DEFER 会留"offline 后 seeds 陈旧"缺口、与 G3 目标矛盾。drop-only（只删 host 匹配被 pruned peer 的项，不构造新端点）是最安全离线形态（security：不引入端点、无 nats:// 降级风险）；只加 seed-meta 收敛、**不改 prune/membership 状态机**（未越 R3 实质）。需 export `MetaKeySeedGeneration`（seeds.go:22）。 |

**驳回**（含被 critic 攻破的选项，勿在 Stage-B 复活）：OQ-A ②`client_endpoint` 列（wire+PoP-coverage 双致命）；
OQ-B 字符串级 SQL gate（`WHERE value != excluded.value`，序敏感、被 shuffle/异构击穿 + backstop 放大 churn）；
synth 的 `seed_source` provenance KV（对存量零效、需锁步）；每请求签名的 responder（DoS 放大器）；`cluster.*` 命名空间
给 client 授权（破 broker 隔离、非 actor-scoped）；auto-IP 进 signed seeds / IP 探测；drop-only 覆盖 online 路径
（放弃 grow 收敛 + prune-fail 引入 roster/seed skew——online 用 rebuild-from-roster）。

---

## 1. 逐缺口最终 approach + 落点 file:line

### #1 — 成员变 → seed 自动收敛（online rebuild-from-roster + offline drop-only）

**A. 纯函数**（`internal/cluster/seeds.go`，紧邻 `PlanClusterSeedsPublish:79`；复用 `net/url`、`validateSeedEndpoint:117`、
`isUndialableHost`（roster.go:227 的等价））
- `DeriveSeedEndpoints(prev []string, brokers []RosterBroker) []string`：
  1. `len(prev)==0` → `nil`（**从未 publish → 不 bootstrap 首发布**，首发布仍手动，signature-guarded GAP——首发布须
     operator 确立 scheme/port/path 模板约定）。
  2. 解析 `prev[0]` 的 scheme+port+**path**（`EscapedPath()`，**勿硬编码 443/无 path**）。★ **scheme==`nats://` →
     return nil + warn**（OQ-B 加固：不 auto-传播明文 PIN 载体）。
  3. 对每个 **dialable** broker（跳 `isUndialableHost` loopback/空 host）按 phase 分层（VOTER→transient→draining，
     **镜像 DialURLs 分层但不 `rand.Shuffle`**——stored 须确定序）套模板；dedupe；cap `maxSeedEndpoints=8`（seeds.go:23）
     保 VOTER 优先；**确定性排序输出**（INV-1：喂 change-gate、防 shuffle churn）。
- `seedSetEqual(a, b []string) bool`：各自 sort 后逐元素比对（change-gate 用）。
- `seedHostsMatchAnyBroker(prev []string, brokers []RosterBroker) bool`（★ host-match 启发式）：`prev` 中任一 URL 的
  host 等于某 broker 的 `public_host` → true（= broker 端点集，接管）；全不匹配 → false（= operator VIP 集，不接管）。

**B. leader helper**（`internal/broker/clusteradmin.go`，紧邻 `PublishSeeds:352`）
```
func (a *ClusterAdmin) deriveAndConvergeSeedsFromRoster() error   // best-effort
  1. eps, bootstrap, _, _ := cluster.Seeds(a.node.RODB())          // 读回 bootstrap（INV-3）
  2. len(eps)>0 && !seedHostsMatchAnyBroker(eps, brokers) → return // ★ host-match：纯 VIP 集，hands-off
  3. brokers := readRosterBrokers(a.node.RODB())                   // read-after-Propose（leader 本进程可靠，SetMaxOpenConns(1)）
  4. next := DeriveSeedEndpoints(eps, brokers)
  5. len(next)==0 → warn + return                                  // INV-2 空守卫，绝不 Propose 空集
  6. seedSetEqual(next, eps) → return                              // change-gate：不变则不 Propose
  7. a.node.Propose(PlanClusterSeedsPublish(next, bootstrap, a.now()))
```
- **best-effort**：任何一步出错只 `logger.Warn`，**绝不失败 membership op**（对齐 `force_single_online.go:298`）。
- **复用既有 `PlanClusterSeedsPublish`**（all-literal、rides `genericExecApplier`、**无新 op → 混版不 decode-poison**）；
  **不新增 SQL-gate plan**（避字符串 gate 序敏感）。

**C. 三个 online 落点**（均 leader-only、mutation Propose 之后、`return` 前）
- join→VOTER：`clusteradmin.go` `AddNode` `setPhase(VOTER)` + clear-force-single 之后（≈:248-261）。
- retire：`clusterdrain.go` `PlanClusterNodeRemove`(:161) + clear drain marker(:167) 之后（≈:172 前）。
- online force-single：`force_single_online.go` **prune Propose 成功之后**（`:295 err==nil` 分支）、`return Response`(:302) 前
  —— **必在 prune 之后**（INV-4：prune 是 best-effort，无条件放 prune 后会在 prune 失败时把死端点重派生回 seeds；
  门控在 `err==nil` → 接受"prune 失败 → seeds 与 roster 一致陈旧、由 `cluster recovery node remove` 一起修"）。
- **leadership backstop**：`ReconcileMembershipOnLeadership` 尾也调 helper（INV-6：关闭"per-op best-effort 失败 + 无后续
  membership op → 永久陈旧"洞）——**前提是 change-gate 确定性**（B 步 sort + set-equal 保证同集不 bump gen、不 churn）。
  ★ 现网价值：racknerd 下次重启（leadership re-acquire）自动接管收敛，零 operator opt-in。

**D. offline drop-only**（`internal/clusteroffline`，`pruneRosterPeers`（offline.go:396）之后，同一 flock 窗口）
- 移除 stored `seed_endpoints` 中 **host 匹配被 pruned peer** 的项（drop-of-departed，**无需模板源** → 无 scheme 歧义/
  无 security nats:// 降级风险）；保留其余（VIP host 非 pruned peer → 天然保留）。
- direct-SQL bump `seed_generation` 复刻 `MAX(existing+1, now.UnixNano())` 单调地板（防重启回退 → 混版 fail-closed）。
  需 export `MetaKeySeedGeneration`（seeds.go:22），或在 `internal/cluster` 出一个 offline 可调的收敛函数（Stage-B 择更小）。
- change-gated：无 departed host 命中 stored → 不写、不 bump（幂等）。

**落点**：`internal/cluster/seeds.go`（`DeriveSeedEndpoints`/`seedSetEqual`/`seedHostsMatchAnyBroker`/export
`MetaKeySeedGeneration`）· `internal/broker/clusteradmin.go`（helper + AddNode 尾 + `ReconcileMembershipOnLeadership` 尾）·
`internal/broker/clusterdrain.go:161-172`（retire 尾）· `internal/broker/force_single_online.go:295-302`（prune 后）·
`internal/clusteroffline/offline.go:396-433`（offline drop-only）

---

### #17 — ctl 自动收敛（改法二 mandatory + 改法一 adjunct）

**改法二（新 NATS roster-pull，摆脱 floor/bootstrap 单点）**
- **subject**（`internal/proto/subjects.go`，镜像 `SubjCtrlClusterHealth`）：`tether.v2.ctrl.by.<actor>.cluster-roster.req`。
  - **actor-scoped**（`by.<actor>` 锁连接真实 nkey、不可伪造）；**hyphen token `.cluster-roster.` 不含 `.cluster.`** →
    §13.8 负测（permissions_test.go:119）保持绿。**否决 `tether.v2.cluster.roster.pull`**（broker-only 命名空间、
    非 actor-scoped、触红 §13.8）。
- **broker responder**（`internal/broker`，cluster 模式挂载，镜像 `SubscribeClusterHealth`）：reply body =
  **`b.manifestBytes()`（cluster_manifest.go:38）缓存字节**（完整 `ClusterManifest`，已 marshaled + ≥30s re-sign）。
  **绝不 `buildSignedRoster`/`buildSeedBundle` 每请求签**（ed25519 DoS 放大器）。single 模式 `selfID==""` →
  `manifestBytes` 返回 `(nil,false)` → `ErrNoResponders`-等价 → ctl no-op（byte-equivalent）。
- **授权**（`internal/auth/permissions.go`）：pub allow 加进 **`PermissionsForUnactivated`(:20) 与
  `PermissionsForActivatedMember`(:55) 两模板**（`refreshCtlEndpoints` 在每个 expandable connect 触发，含未激活
  `session list`/`login`——**否决 activated-only**：未激活刷新会 NATS permission violation 静默失败）。broker sub
  `ctrl.by.*.>` 已被 `PermissionsForBroker`(:204) 覆盖。payload 是 discovery-only 公开拓扑（零 secrets）→ 授权级正当。
- **ctl 消费**（`cmd/tether/ctl_connect.go`）：
  - **plumb 活的 `nc`** 进 refresh（改 `refreshCtlEndpoints` 签名传 `nc`，现 `:59` 未传）。
  - ★ **门A pin 前置门结构性重申**：`ce.PinAccountPub==""` → 不消费（**绝不从 NATS 应答者 TOFU**——`AdoptDecision:142`
    在 `pin==""` 时 TOFU roster；kill-shot）。
  - `nc.RequestWithContext(SubjCtrlClusterRoster(actor), …)`（复用 `ctlManifestFetchTimeout`）→ `agent.AdoptDecision(prev,
    m.Roster, m.Seeds, templateURL, now)`（验签+单调 gen+reject 保原状 no-poison 全不变）→ accepted 才写
    `cluster_endpoints.json`。
  - HTTP `FetchManifest`(ctl_connect.go:82) 降为**次级 fallback**（NATS 拉失败/旧 broker 无 responder 时），保 no-poison。

**改法一（顺手，`cmd/tether/ctl_connect.go:72`）**
- 去掉门B `ce.FloorURL != base`；保门A（PinAccountPub）+ 门C（HTTP fallback）+ 门D（TTL）。
- accept 后（且**仅 accepted 后**——no-poison）迁移 `ce.FloorURL = base`（写 cache 前），self-heal `DialFor` 的
  `FloorURL==base` 展开门（cluster_endpoints.go:173）。
- **templateURL 用缓存 `FloorURL`**（稳定，非 transient failover `base`——security minor：不同 broker URL 形态漂移）。

**落点**：`internal/proto/subjects.go`（新 subject 常量 + helper）· `internal/broker`（responder，镜像
`SubscribeClusterHealth`）· `internal/auth/permissions.go:20,55`（两模板）· `cmd/tether/ctl_connect.go:59,70-99`
（plumb nc + 门A 重申 + AdoptDecision 消费 + HTTP 降 fallback + 拆门B + FloorURL 迁移）

---

### #11 — IP 直连 fallback（仅 doc + test，0 代码新增）

- **本批 0 代码新增**（除 doc+test）。既有 `InviteSeeds`（cluster_endpoints.go:186，OOB/paste-trusted 永久 floor、
  never-clobber、已收 `tls://<ip>:443`）已是完整手动 IP-floor 路径。
- doc（`docs/cluster.md`/`docs/broker-ops.md`）诚实写明：signed seed 层是 broker-托管、随成员自动收敛；**durable 自定义/
  IP 端点放 ctl `InviteSeeds`**（权责分离）；`tls://<ip>` 对 Caddy-fronted broker **不可用除非**部署层改造（IP-SAN 证书 +
  Caddy default site + 公网 raw-NATS listener）→ DNS-独立 IP fallback 是**拓扑改造、非本批**（backlog）。#11 由 (a) #1 的
  DNS 收敛（name-stable failover）+ (b) InviteSeeds（DNS 死但 IP 可达的窄场景）联合满足。

---

## 2. Epic scope（IN / DEFER / OUT）+ wire 判定

| 项 | 归属 | wire 判定 |
|---|---|---|
| #1 online rebuild-from-roster 派生+发布（复用 `PlanClusterSeedsPublish`、扁平 `Endpoints`、host-match gate、auto 拒 nats://） | **IN** | **零 wire**：ProtoVersion=2 不动；`CanonicalSeedBytes`/`CanonicalRosterBytes` 公式**零字节改**（只变 `Endpoints` 内容）；无新 op、无 migration；旧 agent 用旧公式重算含新端点 → 验签通过 → 直接拨。 |
| #1 offline drop-only + seed_gen MAX-floor bump | **IN** | 零 wire；direct-SQL bump 复刻 MAX-floor 单调（防混版 fail-closed）；export `MetaKeySeedGeneration`（仅此需要）。 |
| #17 改法二 subject（`ctrl.by.<actor>.cluster-roster.req`，reply=cached `ClusterManifest`）+ 授权两模板 | **IN** | **additive-safe**：无新 proto 类型（复用 `ClusterManifest`）、仅新 subject 字符串；旧 broker 无 responder → `ErrNoResponders` → fallback（never fail-closed/poison）；旧 ctl 不发 → 无影响。 |
| #17 改法一（拆门B + FloorURL 迁移） | **IN** | 零 wire（ctl 本地逻辑）。 |
| #11（InviteSeeds doc+test） | **IN** | 零 wire（扁平字符串 OOB floor）。 |
| ② `cluster_nodes.client_endpoint` 列 + join 自报 | **DEFER→G4** | **NOT additive-safe**（未升级 follower baked-SQL panic/poison）+ join PoP 不覆盖（self-report 投毒）。 |
| auto-IP-in-seeds / `broker.public_ip` | **DEFER→backlog** | 功能面否决（OQ-D）。 |
| 新 `broker.yaml` deploy-rendered key（模板/IP） | **OUT**（G3 非 deploy-tier 批） | — |
| offline/online **nats.conf / membership 状态机**改动 | **OUT**（R3；那是 G2/G4） | — |

**wire-compat 判定**：`ProtoVersion` 保持 2、`SeedBundleSchemaVersion`/`ClusterRosterSchemaVersion` 不变。#1 只改
`SeedBundle.Endpoints` 的**内容**（canonical 字节公式零改）；#17 只加 subject 字符串 + 复用 `ClusterManifest`。**wire 零破坏**
→ 混版车队（v2 v0.4.x + 半修 racknerd + 旧 ctl）安全，无需 v1→v2、无需重装。

---

## 3. TestItems

**hermetic Go（`make test`，嵌入式 `nats-server/v2/test`；触碰 reconcile/传输/ctl-refresh 面带 `-race` + 内建
NumGoroutine/fd 泄漏门）**

*#1 纯函数 `DeriveSeedEndpoints`（表驱动）*
1. `prev` 空 → nil（首发布不自动，钉残留 GAP）。
2. `prev=[wss://b1:443/nats]`, brokers=`[b1,b2 VOTER]` → `[wss://b1:443/nats, wss://b2:443/nats]`（grow 收敛、**path 保留**）。
3. b2 retire → `[wss://b1:443/nats]`（死端点掉出）。
4. **域名迁移**：`prev=[wss://pc732:443]`, brokers=`[racknerd VOTER]` → `[wss://racknerd:443]`（re-template 到当前 host、pc732 掉出）。
5. loopback/`0.0.0.0`/空 host 跳过（`isUndialableHost`）。
6. ★ **模板 scheme==`nats://` → nil + warn**（auto 拒明文 PIN 载体，OQ-B 加固）。
7. `>8` broker → cap 保 VOTER 优先。
8. **确定性排序**：同 broker 集多次派生字节一致（喂 change-gate、防 shuffle churn）。
9. 非法/不可解析 `prev[0]` → nil（不 panic、不发毒端点）。

*#1 host-match 启发式（表驱动）*
10. `prev=[wss://racknerd:443]`（含 broker public_host）→ `seedHostsMatchAnyBroker`=true → 接管。
11. `prev=[wss://vip.lb.example:443]`（无一匹配 broker）→ false → hands-off（保护 VIP，钉存量收敛 vs VIP 保护分界）。

*#1 broker 级（in-proc，嵌入 nats）*
12. change-gate：派生==stored → **不 Propose**（spy 计数 0、`seed_generation` 不变）；变更 → 恰 Propose 一次 + gen 单调 +1。
13. 空守卫：self loopback → 派生空 → **不 Propose 空集**（不 error、seeds 不变、warn）。
14. bootstrap 保真：publish endpoints+bootstrap → 触发 auto → `seed_bootstrap` **存活**（钉 seeds.go:104 抹除 hazard）。
15. 3 commit 点：AddNode→VOTER 后 `Seeds()` 含新成员；retire 后消失；**force-single prune 之后**收敛 `{self}`
    （钉 INV-4：**注入 prune 失败 → 不重派生死端点**、seeds 与 roster 一致陈旧）。
16. offline force-single：direct-SQL drop-only 后 `seed_endpoints` 剔除 pruned host + `seed_generation` **单调不回退**
    （重启重放后仍单调）；VIP host（非 pruned）保留。
17. backstop 幂等：模拟丢主（commit 点未调到）→ 新 leader `ReconcileMembershipOnLeadership` 补收敛；**同集重收敛不 bump gen**
    （钉 backstop × 确定 gate 不 churn）。
18. 混版 no-fail-closed：旧公式对"自动填充多端点 bundle" `VerifySeedsAt` 仍通过；forged/foreign/rollback 仍 reject 保原状。

*#17 改法二*
19. responder 回 `manifestBytes()` 缓存字节；cluster 模式签名 manifest；single(`selfID==""`) → nil/`ErrNoResponders`-等价。
20. **DoS**：并发 N 请求 → re-sign 次数 ≤ now/30s（断言不每请求签）。
21. ctl NATS refresh：`AdoptDecision` 经 NATS 路径 == HTTP 路径同 no-poison/单调-gen 语义；forged/foreign/lower-gen 全
    reject 保 `cluster_endpoints.json` 字节。
22. ★ **门A kill-shot**：`PinAccountPub==""` 的 ctl 做 NATS pull **绝不** TOFU-pin（钉 security 横切）。
23. 旧 broker 无 responder → `ErrNoResponders` → 退回 HTTP/缓存（no-poison），不崩。
24. **授权矩阵**：unactivated **能** pub `ctrl.by.<self>.cluster-roster.req`；跨 actor `by.<other>` **拒**；member **不能**
    pub `cluster.apply.*`/`cluster.*`（§13.8 保持绿）；两模板均含新 subject。

*#17 改法一*
25. 门B 拆除：`base != FloorURL`（re-point）→ 现在刷新 + accept 后 `FloorURL:=base`；DialFor 下次展开（two-call self-heal）。
26. 门A/C/D 保留：无 pin 早退、无 bootstrap 无 HTTP fetch、TTL 内节流。

*#11*
27. `DialFor` 带 `InviteSeeds=[tls://1.2.3.4:443]` → 出现在拨号串、**floor-last**、不被 dedupe 丢；与 DNS roster 并存时
    VOTER 在前、IP floor 在后。〔诚实注：此测只证字符串在拨号串、不证可达〕

**新 G3 sim drill（`test/simcluster/drills/NN-g3-client-converge.sh`，现无 RED drill，需新建；遵守 §5 铁律；按需单跑、不 loop）**
- **A. grow 自动收敛**：真 3-broker，先 operator 手动 `cluster seeds publish`（确立模板）→ `grow` → 断言
  `cluster seeds show` **自动含新成员**（无第二次手动 publish）、`seed_generation` 前进。
- **B. force-single 收敛**：杀 brk2/3 → `force-single brk1` → 断言 `seeds show` 收敛**仅 brk1**、死端点消失。
- **C. offline force-single**：N=2 掉一进 offline force-single → prune 后 `seeds show` 剔除 departed（drop-only）。
- **D. 杀 floor broker，ctl 从 survivor 刷（改法二）**：ctl 连 brk1 建 `cluster_endpoints.json` → 杀 brk1 → ctl 跑命令经
  `DialFor` failover 连 survivor → 断言 `cluster_endpoints.json` roster_gen **从 survivor 经 NATS 前进**。
- **残留 `[GAP]`**：首次 seed 发布仍手动（`prev==空→nil`）——钉"grow 前从未 publish 时 `seeds show` 为空"，首发布
  自动化落地后翻 GREEN。

**e2e 矩阵**：hermetic Go 进各 package；**不**把 clustered raft-swap + ctl-NATS-refresh 加进 `make e2e` 的重 gated 面
（routed-JS flake 类，e2e 刻意串行避开）；真跨机 seed 收敛 + ctl 缓存文件收敛归 sim drill。

---

## 4. 现网上线序列（上线时执行，不阻塞 Stage-B）

**Step 0 — pre-flight**：① racknerd 现 `cluster seeds show` 内容（应含 racknerd 自己 public_host = 手动 publish 残留）；
② `seed_source` 无关（本批无 provenance）；③ ctl 本机二进制版本（旧 ctl 无改法二 responder 消费，靠车队一起升）。

**Step 1 — 上线二进制**：#1 host-match 启发式使 racknerd 手动集（含自己 public_host）被判"接管" → **下次 leadership
backstop（重启/re-acquire）自动 rebuild-from-roster 收敛**（N=1 → `[wss://racknerd:443]`），**零 operator opt-in**。
若想立即收敛：`systemctl restart tether-broker`（触发 leadership re-acquire → backstop）或一条手动
`cluster seeds publish <racknerd-endpoint>`。

**Step 2 — 验证**：`cluster seeds show` 仅含 racknerd；`seed_generation` 前进；新 register 的 agent/ctl 缓存收敛（无死端点）。

**Step 3 — #17 改法二生效前提**：ctl 与 broker 都升到含改法二的版本；旧 ctl 仍走 HTTP fallback（no-poison，不崩）。

**铁律**：racknerd 手动集若**被 operator 混入过 VIP/自定义端点**（现网核对：无）→ host-match 仍判"接管"（含 racknerd
public_host）→ wholesale rebuild 会抹 VIP；故 durable 自定义端点**必须**放 ctl `InviteSeeds`（never-clobber），doc 明写。
任何 rebuild 对 already-converged 集是确定性 no-op（change-gate skip、Go 测试 TestItem 12 覆盖）。

---

## 5. Stage-B 实现顺序（先父后子）

1. **纯函数**（`internal/cluster/seeds.go`）：`DeriveSeedEndpoints`（path-preserving、确定序、nats:// 拒）、`seedSetEqual`、
   `seedHostsMatchAnyBroker`、export `MetaKeySeedGeneration`。可单测（TestItems 1-11）。
2. **change-gate + bootstrap-preserve + 空守卫 + host-match 的 leader helper**（`internal/broker/clusteradmin.go`，
   复用 `PlanClusterSeedsPublish`，**不新增 op/plan/SQL-gate**）。
3. **接 3 online commit 尾 + leadership backstop**（AddNode→VOTER、clusterdrain retire、force_single_online **prune 成功后**）。
4. **offline drop-only 收敛 + MAX-floor seed_gen bump**（`internal/clusteroffline` after `pruneRosterPeers`）。
5. **#17 改法二**：proto subject 常量 → broker responder（`manifestBytes()`）→ 授权两模板 → ctl plumb `nc` + 门A 重申 +
   `AdoptDecision` 消费 + HTTP 降 fallback。
6. **#17 改法一**：拆门B + accept 后 `FloorURL:=base`。
7. **#11 doc**（InviteSeeds 权责分离 + IP-SAN 拓扑前置）。
8. **hermetic 测试**（§3；并发面带 `-race`+leak 门）。
9. **新 sim drill**（RED-first）。
10. 提交前硬闸：`make test` + `make e2e` + `make lint` 全绿；改法二触碰 ctl-refresh/传输面另过 `-race` + 泄漏门。

**贯穿不变量**：change-gate 确定性（INV-1）、空派生 preserve（INV-2）、bootstrap 读回（INV-3）、force-single helper
门控在 prune 成功后（INV-4）、seed_gen 跨 online+offline 单调（INV-5）、leadership backstop 补覆盖（INV-6）、门A
在每个新 NATS 消费点结构性重申（security kill-shot）、responder 走 cached `manifestBytes()`（防 DoS）。

---

## 6. Stage-C 内审更正（2026-07-07）

Stage-C 多专家内审的逐条评估与修复见 `docs/reviews/g3-review.md`「主进程评估与修复」（无 BLOCKER；2 CONFIRMED MAJOR + 测试缺口簇全修）。**plan↔代码一处更正（review m13）**：本 plan §1.A / §落点 / §5-step1 原写纯函数 `DeriveSeedEndpoints`/`seedSetEqual`/`seedHostsMatchAnyBroker` 落 `internal/cluster/seeds.go`；**实际落 `internal/broker/seed_converge.go`**——唯一调用者是 `broker.ClusterAdmin` 方法、依赖 `proto.RosterBroker` + `clusterroster.IsUndialableHost`（broker 已 import、无 import cycle），offline 复用的 `SeedEndpointsDropHosts` 仍留 `internal/cluster/seeds.go`。其余 Stage-C 落地增量：M1 `RemoveNode`/`removeGhost` 收敛 seeds（online/offline 平价）、M2 `refreshCtlEndpoints` 失败刷新限流、m11 派生排除 `VOTER_ADD_FAILED`、m18 robust 模板（第一个可解析 tls/wss）、m10 单快照读、m8 混合集 doc、n21 IPv6 bracket、n23 `MaxSeedEndpoints` SSOT、n24 `MetaKeySeedBootstrap` unexport。
