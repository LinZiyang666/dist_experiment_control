# L10 — 认证 / 授权 / 会话隔离的**结构**审计

> lane key: `auth-security-structure`
> 日期：2026-07-25 · 只读审计（未修改任何实现代码）
> 与 `docs/reviews/quality-audit/02-security.md`（漏洞审计）**不重叠**：本文找的是冗余、重复、职责错位、
> 抽象缺失、演进阻力，不是找可利用漏洞。

---

## 结论

**这个 lane 不是屎山，恰恰相反：它是全仓最"瘦"的部分之一。真正的债不在行数，而在于凭据生命周期只做了前半段
（铸造 + 分发），后半段（轮换 + 吊销）在 ACL 里开了口子、在 requirements 里写了规格、在代码里一行都没有；
并且在生产实际跑的单 broker 拓扑下，整套 auth_callout 授权栈是一个 `install.sh` 不会设置、`broker.yaml`
没有对应字段的可选开关——只能靠 `docs/broker-ops.md §3.4` 那套手抄 nats.conf + `sed -i` systemd unit 的
4 步人工流程打开。**

bloat 打分：**3 / 10**（1=精炼，5=正常工程债，10=屎山）。

打 3 分而不是 1 分的理由，只有三条实打实的：
- `internal/auth/jwt.go` 92 行生产代码在生产路径上**完全没有调用者**（`AccountSigner` / `IssueUserJWT` /
  `DecodeUserJWT` / `LoadAccountSigner` 全仓只被测试引用），真正签 JWT 的是
  `authcallout.Handler.allow()` 里另写的一份；
- "session ACTIVE + 是 member/owner" 这个**唯一的运行时吊销机制**在 `internal/broker` 里被手抄了 24 遍
  （29 处 `session.Is*` 调用、9 个文件），仓库里已经证明可行的两个提取（`transferGate` /
  `proxyActiveOwnerGate`）只覆盖了其中 8 处，连同文件里的 `handleCapsReq` 都没用上；
- 同一套 subject 文法被**独立拼写了 4 遍**（proto 构造器 / auth ACL 字面量 / broker 订阅表 /
  `isBroadcastClusterSubject` 后缀表），且三条 ACL 授权（`kick` / `rotate-pin` / `node.*.tag`）
  已经指向不存在的订阅者。

打 3 分而不是 6 分的理由（反证见 §反证）：**没有任何过度抽象**，`internal/auth` 是零依赖纯叶子包
（只 import `nkeys` + `jwt/v2`），4 个权限模板是全仓**唯二**的 `jwt.Permissions{}` 构造点（另两个在测试里），
版本前缀有 AST 级 SSOT tripwire 且带自检（非空洞），`natscluster.RenderConf` 直接从
`auth.PermissionsForBroker()` 渲染 nats.conf 而不是手抄，PIN 限流器的 N×预算问题被显式论证并接受而不是装看不见。
**这是一份被认真想过的授权代码，只是它的生命周期缺了一半。**

verdict: **minor-debt**（结构本身健康；缺的是能力，不是被债压垮）。

---

## 范围与方法

### 范围（生产行，约 3,140 行）

| 位置 | 生产行 | 角色 |
|---|---:|---|
| `internal/auth` | 577 | 纯策略/密码学原语 + 4 个 NATS 权限模板 |
| `internal/authcallout` | 598 | auth_callout 决策引擎 + PIN 限流 |
| `internal/session` | 593 | session/member CRUD + FSM Plan 渲染 |
| `internal/node` | 392 | node 生命周期（授权相关面很小） |
| `internal/agentprov` | 219 | `(sid,nid) → agent fp` 绑定 |
| `internal/proto/subjects.go` | 434 | subject 文法 SSOT（ACL 的另一半） |
| `internal/broker/{exec,run,expose,proxy,transfer,sessions,upgrade}.go` 的 gate 段 | ~220 | 应用层 authz 决策点 |
| `internal/natscluster/config.go` authorization 渲染 | ~45 | 静态 broker ACL 落盘 |
| `internal/subhttp/subhttp.go` token gate | ~60 | /sub 承载 token 授权 |

测试行（2,498）不计入 scope，但作为证据引用。

### 方法

- 通读上表所有文件（`internal/auth/*`、`internal/authcallout/*`、`internal/session/*`、`internal/node/*`、
  `internal/agentprov/*`、`internal/proto/subjects.go` 全文；broker 的 gate 段与订阅表逐段读）。
- `go list -deps` 确认包依赖方向；`go build ./...` 确认基线可编译。
- 一次性只读 AST 脚本统计 `internal/broker` 内 `session.Is{Active,Member,Owner}` 调用点分布。
- grep 交叉核对：subject 字面量散落度、`jwt.Permissions{}` 构造点、`Plan*` 渲染器与调用点、
  ACL 授权 ↔ 实际订阅者的对账、`pin_hash` / `members` 的写路径。
- 对照 `docs/architecture.md` B.0–B.5 / C.4 / E.2 / E.6、`docs/requirements.md` §6.3/§9、
  `docs/broker-ops.md §3.4`、`docs/usage.md §logout`、`scripts/install.sh`。
- **未运行**任何重测试（`make test` / `make e2e` / simcluster）。

---

## Findings

### F1 — [HIGH] 凭据生命周期只有"铸造 + 分发"，**没有轮换、没有吊销**；ACL 却为这两个动词开了口子

**证据**
- `internal/auth/permissions.go:89` — 授权 pub `…ctrl.by.<actor>.session.<sid>.kick.req`
- `internal/auth/permissions.go:90` — 授权 pub `…ctrl.by.<actor>.session.<sid>.rotate-pin.req`
- `internal/auth/permissions.go:93` — 授权 pub `…ctrl.by.<actor>.s.<sid>.node.*.tag.req`
- 三者在全仓**没有任何订阅者、没有 proto 消息类型、没有 CLI 子命令**。
  `node.*.tag.req` 全仓唯一引用就是 permissions.go 那一行本身。
- `internal/broker/audit.go:36-40` 已经书面承认：`session kick`/`rotate-pin` are not implemented。
- `docs/requirements.md:193,194,416` 把 `kick` / `rotate-pin` 列为 **owner-only 必备能力**；
  `docs/architecture.md:126,127,213,214,239,379` 全套铺开了它们的 subject、权限、审计语义。
- 写路径核对：`pin_hash` 只在 `session.Create`（`internal/session/session.go:82`）和
  `PlanCreate`（`internal/session/plan.go:41`）被 INSERT，**全仓无一处 UPDATE**。
- `members` 表：INSERT 三处（`session.go:93`、`session.go:361`、`plan.go:43/137`），
  DELETE **仅一处**——`internal/broker/audit.go:135`，即整个 session 硬删时的级联清理。
- `tether admin evict` 只删 `agent_provisioning` + `nodes`（`internal/adminsock/server.go:407-430`，
  `internal/node/plan.go:67-77`），**碰不到 `members`**。
- `docs/usage.md:523-524` 告诉用户"要彻底踢人需要 owner 跑 `tether admin evict`"——
  这条指引**双重错误**：① evict 不删 member 行；② evict 走 broker 本机 admin socket，
  一个没有 broker shell 的 owner 根本跑不了。

**为什么是债 / 它让什么变难**
一旦某个 nkey 通过 PIN 加入了 session，它对这个 session 的成员资格**在 session 生命周期内不可撤销**。
唯一的"吊销"手段是删掉整个 session（连带所有 node、port、process、history、xfer bucket）。
同理 PIN 一旦泄露，就永久是这个 session 的有效入场券——`docs/usage.md:518` 说"PIN 是一次性入场券，
不要长期保存"，但**服务端没有任何机制让这句话成真**。

具体挡住的未来改动：
1. **任何"人员离职/设备丢失"的响应流程无法实现**。现在的唯一 runbook 是"重建 session + 让所有其他成员重新 join
   + 所有 agent 重新 provision"，在 6-agent 车队上是一次全量重装。
2. **`internal/auth`/`internal/authcallout` 的 24h JWT TTL 设计前提被架空**。
   `handler.go:119 defaultJWTTTL = 24h` + `ensureMember` 每次 CONNECT 重查 `IsMember`，
   这套设计的全部意义就是"成员资格可以在下次连接时收回"——但没有任何代码能改变 `members` 的内容。
3. **要补 `kick` 时会踩满地雷**：它是一个跨 raft FSM 的写（要新增 `OpMemberKick` + `PlanKick` +
   forward verb + 单模式直写），同时要处理"被 kick 的成员手上还有 23h 有效 JWT、
   已经建立的连接不会断"这个 NATS 层无解的问题——现在没人在设计里承认这个约束。

**建议**
分两步、不必一次做完：
1. **立刻（零风险）**：把 `permissions.go:89/90/93` 三条授权删掉，并在
   `docs/requirements.md`/`docs/architecture.md` 里把 kick/rotate-pin 从"能力"降级为"未实现，见 §gap"。
   现在的状态是**规格与 ACL 都在撒谎**，比诚实的缺口更危险——下一个读 architecture.md 的人（或 agent）
   会以为吊销存在。
2. **规划**：`session kick` 是唯一真正需要的那个（rotate-pin 可以用 kick + 新 session 替代）。
   实现时必须同时给出"已签发 JWT 无法撤回、最长 24h 窗口"的显式文档，并考虑把 `defaultJWTTTL`
   从 24h 降到 1h（`handler.go:119` 单点改动，代价是 24× 的 callout 频率——单 broker 上无所谓）。

**量化**：立刻可删 3 行 ACL；补 kick 约需新增 150–200 行（Plan + verb + 单模式直写 + CLI + 测试）。
**风险**：删 ACL = low（无订阅者，删掉不影响任何在跑的流量）。补 kick = medium（碰 FSM verb 空间，
但**不碰 wire 版本**——新增 subject 是加法，`ProtoVersion` 不动）。
**触碰 wire/不变量**：否（新增 subject 与新增 FSM op 都是加法）。

---

### F2 — [HIGH] 单 broker 拓扑下，整套 auth_callout 授权栈是"装完不生效、只能手工开"的可选开关

**证据**
- `cmd/tether/serve.go:426-434` `effectiveAuthSeedsDir`：非 cluster 模式且未显式传 flag ⇒ 返回 `""` ⇒
  `cfg.AuthCallout = nil` ⇒ 广播 `auth_callout=off (dev / P2-style)`（`serve.go:270-276`）。
- `scripts/install.sh:771` `ExecStart=$bin/tether serve --config $etc/broker.yaml` ——
  **不带** `--auth-callout-seeds-dir`。
- `internal/serveconf/serveconf.go:21-34` — `broker.yaml` 的 section 列表里
  **没有任何 auth seeds 字段**；`cluster.secrets_dir`（`:70`）只在 `cluster.data_dir` 存在时生效。
  也就是说单 broker 连"改配置文件打开它"这条路都没有，**必须改 systemd unit**。
- `docs/broker-ops.md:162-305` §3.4 是一套 4 步人工流程：`nk -gen` 造 seed → `scp` → 手写整个
  `authorization { users / auth_callout { issuer / auth_users } }` 块 `sudo tee` 进 nats.conf →
  `sed -i` 改 systemd ExecStart。文档自己列了三个已知踩坑（写错路径得到 nats-server 永不加载的孤儿文件、
  nkey user 不能带 `user:`/`password:`、`auth_users` 必须是 nkey pub 不是 alias）。
- `docs/broker-ops.md:304` 明说：单 broker 未配 seeds 时以 P2 模式运行，**无 NATS 层身份强制**。
- `test/simcluster/drills/43-migrate-live-data.sh:43` 为了让 drill 跑通，得自己
  `sed -i "/^ExecStart=/ s|$| --auth-callout-seeds-dir /etc/tether/secrets|"` —— 模拟集群
  在如实暴露这个缺口（符合它的 mandate）。
- 对照：cluster 模式**有**完整自动路径 —— `natscluster.RenderConf`（`internal/natscluster/config.go:162-189`）
  直接从 `auth.PermissionsForBroker()` 渲染同一个 `authorization{}` 块，
  `natsconf.Preflight`/`takeover` 能安全接管 install.sh 写的 conf，`cluster reconcile nats` 是产品命令。
  **机器早就造好了，只是没接到单 broker 这条线上。**

**为什么是债 / 它让什么变难**
这是本 lane 里唯一一条"结构决定了事故必然反复发生"的发现。项目记忆里那条
`test-locally-first-no-hotfix-cycles`（"JWT/auth_callout perm 必须本地真跑通才推"）教训的**根因就在这里**：
生产上最高风险的一段配置（谁能连、连上能干什么）是**人手抄的**，而且抄的位置
（`/etc/tether/nats.d/nats.conf` vs `/etc/tether/nats.conf`）抄错了会得到一个 `nats-server -t` 依然报
`is valid` 的孤儿文件——**dry-run 校验为绿、实际未生效**。

具体挡住的未来改动：
1. **`PermissionsForBroker()` 的任何修改，在单 broker 上都不会自动落到 nats.conf。**
   加一条 broker 侧订阅（这在 D8b/G3/G4/G5 已经发生过 4 次）→ cluster 模式 reconcile 一下就好，
   单 broker 必须人工重抄整个 authorization 块，否则 broker 起来后订阅被 NATS 拒绝。
   这是一类**只在生产单点拓扑上出现、hermetic 测试与 simcluster 都不必然覆盖**的故障。
2. **无法把 auth 变成默认**。想让 `install.sh` 默认安全，现在得同时改
   install.sh（造 seed + 渲染 conf + 加 flag）、serveconf（加字段）、serve.go（默认值）——
   三个互不相干的地方，且没有任何测试会因为漏改而变红。
3. **现网就是这个拓扑**。按项目记忆，生产是 racknerd 单 broker + 6 agent，
   即"最不自动化的那条路径"承载了 100% 的生产流量。

**建议**
按代价从低到高：
1. **`serveconf` 加一个 `broker.auth.seeds_dir` 字段**（约 6 行 + serve.go 里 3 行接线）。
   把"改 systemd unit"降级为"改 yaml"，`install.sh` 的 broker.yaml 骨架里注释掉一行示例。
   这一步单独就能把 §3.4 第 4 步（`sed -i` systemd）消掉。
2. **让 `tether serve` 在单模式下，若 seeds_dir 存在但 nats.conf 缺 `auth_callout` 块，
   FAIL-CLOSED 拒绝启动并打印精确的差异**（复用已有的 `natsconf.Preflight` + `AuthIdentity`，
   `internal/natsconf/preflight.go:216-235` 已经能读出 issuer/broker nkey）。
   现在的失败模式是"静默以 P2 模式跑"，这是最坏的一种。约 30 行。
3. **（可选，更大）** 把 `natscluster.RenderConf` 的 authorization 渲染抽成单/多 broker 共用，
   让 `cluster reconcile nats` 在 N=1 也能用。代码基本都在了。

**量化**：净增约 40 行代码，换掉一份 140 行、含 3 个已知踩坑的人工 runbook。
**风险**：第 1、2 步 low（新增字段 + 启动期检查，不改任何 wire、不改任何已有默认行为——
默认 seeds_dir 为空时行为字节不变）。第 3 步 medium（碰 nats.conf 渲染，属 deploy-tier，需 simcluster drill）。
**触碰 wire/不变量**：否。

---

### F3 — [MEDIUM] `internal/auth/jwt.go` 整个文件（92 行）在生产路径上是死代码，且它试图守的不变量被生产路径重新实现了一遍

**证据**
- `internal/auth/jwt.go:14,22,48,73,86` — `AccountSigner` / `LoadAccountSigner` / `IssueUserJWT` /
  `AccountPublicKey` / `DecodeUserJWT`。
- 全仓引用（已 grep 确认）：`internal/auth/jwt_test.go`（176 行）+ `test/p1/foundation_risk_test.go:29-55`。
  **`internal/` 与 `cmd/` 下的生产代码零引用。**
- 真正的签发路径：`internal/authcallout/handler.go:427-460` `allow()` 直接持有
  `nkeys.KeyPair`（`handler.go:44 AccountKp`）并 `uc.Encode(h.AccountKp)`；
  keypair 由 `internal/broker/authcallout.go:70-78` 从 seed 造出后**故意不 Wipe**、
  在 broker 整个生命周期内常驻。
- `jwt.go:49` 的 `nkeys.IsValidPublicUserKey(userPub)` 守卫（防 `jwt.NewUserClaims("")` 返回 nil 后
  写字段 panic，见 `jwt.go:45-47` 注释与 `test/p1/foundation_risk_test.go:42-54`）
  在生产路径上由 `handler.go:169-171` 另写了一份。
- `jwt.go:22-40` 的 seed 卫生（校验必须是 account seed、复制 seed、Wipe 临时 kp）
  在生产路径上**没有对应物**——`broker/authcallout.go:70-74` 只做 `nkeys.FromSeed`，
  不校验它是不是 account seed。

**为什么是债 / 它让什么变难**
这不是"多了 92 行"的问题，是**一个包里有两份签发实现，且被测试覆盖的那份不是生产用的那份**。
`internal/auth/jwt_test.go` 176 行 + `test/p1` 一整个风险测试文件，全都在验证一条生产上不走的路径——
`TestLoadAccountSignerRejectsUserSeed`（"接受 user seed 会静默产出 wrong-kind issuer 的 JWT"）
守的正是生产路径 `broker/authcallout.go:70` **没有守**的那个洞。

具体挡住的未来改动：改 JWT 签发逻辑（比如把 TTL 从 24h 降下来配合 F1、加 `uc.BearerToken`、
换 audience 语义）时，会看到两个候选实现，改错一个而测试全绿。

**建议**
二选一，不要维持现状：
- **(A) 删** `internal/auth/jwt.go` + `jwt_test.go`（-268 行），把
  `test/p1/foundation_risk_test.go` 的两个断言重定向到 `authcallout.Handler.allow()`
  （它已经是导出可测的——`handler_seams_test.go` 就在这么做）；同时把
  `LoadAccountSigner` 的 account-seed 校验**搬进** `broker/authcallout.go:70`（+4 行），
  这是这个文件唯一有生产价值的东西。
- **(B) 用**：让 `Handler` 持 `*auth.AccountSigner` 而非裸 `nkeys.KeyPair`，
  `allow()` 改调 `IssueUserJWT`。代价是 `allow()` 还要签 `AuthorizationResponseClaims`
  （`handler.go:452-458`），得再给 `AccountSigner` 加一个方法。

我倾向 **(A)**：`allow()` 那 30 行已经是最小实现，多一层 `AccountSigner` 换不来任何东西。

**量化**：(A) 净删约 268 行（92 生产 + 176 测试），净加约 4 行守卫。
**风险**：low（删的是零引用代码；加的 4 行是启动期校验，seed 不合法时 broker 本来就跑不通）。
**触碰 wire/不变量**：否。

---

### F4 — [MEDIUM] 会话隔离的**运行时**那一半（IsActive + IsMember/IsOwner）在 broker 里被手抄了 24 遍，仓库里已证明可行的提取只用在 8 处

**证据**（AST 统计：`internal/broker` 生产代码共 29 处 `session.Is*` 调用，分布在 9 个文件）

```
exec.go 6   expose.go 5   proxy.go 4   run.go 4   transfer.go 4
sessions.go 2   upgrade.go 2   broker.go 1   clusterwrite.go 1
```

- 逐行同构的内联 gate（IsActive → IsMember，各带 store_error / session_not_found_or_deleting /
  not_a_member 三分支，约 20 行）：
  `exec.go:48-67`（exec）、`exec.go:210-227`（node list）、`exec.go:276-295`（ps）、
  `run.go:41-59`（run）、`run.go:134-152`（kill）、`expose.go:178-195`（expose）、
  `expose.go:386-403`（expose-rm）、`proxy.go:327-345`（proxy status）、
  `transfer.go:1048-1064`（finalize 前置）、`transfer.go:1029-1064`（caps）。
- 已提取的两个 helper：`internal/broker/transfer.go:968-997 transferGate`（返回 code，调用方自己映射 reply）
  与 `internal/broker/proxy.go:552-573 proxyActiveOwnerGate`。**`transferGate` 的 doc 注释
  （`transfer.go:965-967`）明确写着"used for finalize.req / caps.req"，但
  `handleCapsReq`（`transfer.go:1029-1064`）却把同一段逻辑内联抄了一遍**——
  同一个文件里、helper 上方 60 行处。
- 各 handler 的 reply 形状不同（`replyExecErr` / `replyRunFailed` / `replyExposeErr` / `proxyErr` /
  `replyJSON(NodeListResp{})` / `replyJSON(CapsResp{})`），这是它们没被合并的**唯一**原因。
  而 `transferGate` 的"返回 code 字符串，调用方映射"模式已经证明这个原因可以被绕过。

**为什么是债 / 它让什么变难**
`IsActive`+`IsMember` 不是冗余检查——它是**这套系统里唯一的运行时吊销点**（NATS JWT 一旦签发就带 24h TTL，
无撤销列表；见 F1）。也就是说：**最重要的那条授权判断，没有单一实现。**

具体挡住的未来改动：
1. **补 F1 的 `kick` 时，"被 kick 者的在途连接怎么办"这个问题的答案就是这 24 处**——
   必须逐个确认它们都在正确的位置调用了 `IsMember`。现在没有任何结构保证新加的 handler 会有 gate
   （靠的是纪律；见反证 §C4，纪律目前守住了）。
2. **加任何一条新的授权维度**（例如 requirements §6 曾设想的 read-only member、
   或 C5 已经存在的 `Vendable(sid)` quorum 门）都要改 24 个地方。
   `subhttp` 已经因为这个原因把 quorum 门做成了注入式 `Vendable func(sid) bool`
   （`internal/subhttp/subhttp.go:64`）——**同一个问题在 subhttp 里被正确解决了一次，
   在 broker 里没有。**
3. **审计的时候没法一眼看全**。`02-security.md` 要覆盖这个面就必须逐 handler 走一遍。

**建议**
把 `transferGate` 泛化成 broker 级的单一 gate，签名带"需要什么"：

```go
// 形如（示意，不是要落的代码）
func (b *Broker) authGate(sid, fp, nid string, need gateNeed) (code string)
// need ∈ {needMember, needOwner} × {withNodeOnline}
```

各 handler 保留自己的 `reply(code)` 一行映射。`transferGate`（含 nid=="" 跳过 node 检查的分支）
与 `proxyActiveOwnerGate`（owner 变体 + admin_denied 审计）合起来已经覆盖了所需的全部形状。
**最小起步**：先把 `handleCapsReq` 改成调 `transferGate(sid, fp, "")`（-14 行，零语义变化），
它是这条建议的零风险样例。

**量化**：24 个内联 gate ≈ 216 行 → 单一 gate + 24 行映射 ≈ 70 行，**净减约 140 行**；
授权决策点从 24 个收敛到 1 个。
**风险**：medium（碰的是每条 ctl 命令的准入路径；每个 handler 的错误码字符串必须逐字保持
——`cmd/tether/error_hints.go:21-22,80` 与 `exitcode.go:34` 按 code 字符串做 exit-code 映射，
改错一个字符就改了 CLI 退出码契约）。建议**分 handler 家族小步做**，不要一次性大改。
**触碰 wire/不变量**：否（错误码是 reply body 里的字符串，不是 subject/proto 版本；但**必须逐字不变**）。

---

### F5 — [MEDIUM] 同一套 subject 文法被独立拼写了 4 遍；已有 3 条 ACL 授权指向不存在的订阅者

**证据**
一条 ctl RPC 的 subject 文法，在生产代码里有 4 个互不引用的表达：

| # | 位置 | 形式 |
|---|---|---|
| 1 | `internal/proto/subjects.go`（~40 个 `Subj*` 构造器 + 7 个 `Parse*`） | `fmt.Sprintf("%s.ctrl.by.%s.s.%s.proxy.set.req", …)` |
| 2 | `internal/auth/permissions.go:62-154`（激活态模板 33 条 pub / 12 条 sub） | `subjectPrefix + ".ctrl.by." + actor + ".s." + sid + ".proxy.set.req"` |
| 3 | `internal/broker/broker.go:958-1012`（订阅表） | `proto.SubjectPrefix + ".ctrl.by.*.s.*.proxy.set.req"` |
| 4 | `internal/broker/clusterwrite.go:59-79 isBroadcastClusterSubject` | `".proxy.set.req"` 后缀表 |

- 第 4 张表漏一条的后果有先例，代码注释自己记着：`broker.go:1001-1005`
  "Mega-audit MAJ-2: … The wildcard never matched isBroadcastClusterSubject's per-leaf HasSuffix check,
  so it was queue-grouped and the leader-only create/revoke landed on a silent follower ~(N-1)/N of the
  time → ctl timeout."
- 第 2 张表漏一条的后果：客户端在 CONNECT 后第一次 pub 就吃 NATS permissions violation，
  **hermetic 单测大量用不带 auth_callout 的 broker，抓不到**（`test/p3`/`p4`/`security` 才带）。
- **对账结果：第 2 张表有 3 条授权在第 3 张表里没有对应订阅者** ——
  `permissions.go:89`（kick）、`:90`（rotate-pin）、`:93`（node.*.tag）。见 F1。
- 反向没有漏：broker 订阅的每条 subject 都能被某个模板 pub 到（逐条核对过）。
- 实测一次新增 verb 的横向代价（grep 落地文件数，不含测试）：
  `cluster-grow` 9 个生产文件、`cluster-upgrade` 7 个、`proxy.sub.create` 12 个。
  其中 4 个纯粹是上面这 4 张表。
- **测试侧**：没有任何测试把这 4 张表对起来。`internal/auth/permissions_test.go` 的 10 个测试
  全是**形状不变量**（无顶层通配 `:34`、无跨子树通配 `:47`、ctl 不能 pub `.forwarded` `:66`、
  actor 段锁定 `:89`、agent 不能 pub audit `:158`、agent sub 限本节点 `:168`、
  cluster.* 仅 broker `:206`）——这些是**对的、且是最高价值的那类测试**，
  但它们证明不了"模板里的这条 subject 真的有人在听"。
- `parseRole`（`internal/authcallout/handler.go:223-242`）是第 5 处 subject 相关的字符串文法
  （连接名 `tether-cli:<sid>` / `tether-agent:<sid>:<nid>`），与 subject 文法各自独立演进。
- 另有 4 个 handler 在 `ParseCtrlBy` 之后**手工按位切 leaf**，绕过了 proto 的类型化 parser：
  `exec.go:196-203`（node.list）、`exec.go:260-266`（ps）、`sessions.go:136-143`（session.rm）、
  `transfer.go:1035-1041`（caps）。这 4 处**都没有调 `proto.ValidateSID`**，
  而 `ParseCmdBy`（`subjects.go:330`）/`ParseCtrlProxy`（`:423`）/`ParseTransferFinalize`（`:165`）
  都调了——同一条"audit shard 03 F5 纵深防御"不变量，7 条入口做了 3 条、漏了 4 条。
  （**已核实不可利用**：sid 段在 ACL 模板里是字面量、在 `ensureMember`/`ensureAgentProvisioned`
  铸造 JWT 时已经过 `proto.ValidateSID`，所以到达 handler 的 sid 必然合法。
  这是**一致性缺陷，不是漏洞**——但它在 auth_callout 关闭的 P2 模式下就没有这层保护了，见 F2。）

**为什么是债 / 它让什么变难**
版本前缀升级（v2→v3）**很便宜**——`proto.SubjectVersionToken` 一个常量 + `auth/permissions.go:11` 那份
被 whitelist 的副本，且有 AST tripwire 兜底（见反证 §C2）。真正贵的是**文法变更**：
往任何一段插入/移动一个 token，要同时改 4 张表 + 7 个 `Parse*` 的硬编码下标
（`subjects.go:157,177,300,323,347,365,417` 全是 `parts[N] != "…"` + 精确 `len(parts)`），
且任何一处漏改都**不是编译错误**——是运行时 `subject_malformed` 或静默的权限拒绝。

具体挡住的未来改动：**这就是"加一个新权限天然危险"的结构成因。**
每个新 verb 有 4 个必须同时正确的独立位置，其中 2 个（ACL、broadcast 表）的错误只在
带 auth_callout 的 e2e 或多 broker 集群下才显形。项目记忆里那条"JWT/auth_callout perm 必须
本地真跑通才推"，与 broker.go:1001 记着的 MAJ-2 事故，是同一个结构问题的两次发作。

**建议**
不建议做"统一 subject DSL"这种大重构——收益比不上风险。做三件小事：
1. **加一条对账测试**（约 60 行，纯静态）：从 `auth.PermissionsFor*` 的 pub allow 列表里
   把每条 `ctrl.by.<actor>.*` / `s.<sid>.cmd.by.*` 授权转成 broker 订阅表的通配形式，
   断言每条都能被订阅表里的某条 pattern 匹配（反向亦然，broker 订阅的 ctl-facing subject
   必须被某个模板允许）。这条测试**今天就会红**（抓出 F1 那 3 条），且从此把第 2、3 张表钉死。
2. **把 `isBroadcastClusterSubject`（第 4 张表）改成从订阅表派生**：
   给 `broker.go:958` 那个匿名 struct 加一个 `broadcast bool` 字段，删掉 `clusterwrite.go:59-79`。
   4 张表变 3 张，且 MAJ-2 那类"忘了往后缀表里加一条"从结构上不可能再发生。
3. **把 4 个手工 leaf parser 收进 `internal/proto`**（各约 12 行），
   顺带补上 `ValidateSID`，让 7 条入口的不变量一致。

**量化**：删 21 行（`isBroadcastClusterSubject`）+ 收编 4 个 parser（净 ±0，但一致性 3/7 → 7/7）；
加 60 行对账测试；新增 verb 的必改点从 4 处降到 3 处。
**风险**：建议 1 low（纯新增测试）；建议 2 medium（碰的是 cluster 模式下 queue-vs-broadcast 的路由决策，
必须逐条核对现有 broadcast 名单一字不差搬过去，且要跑 `go test ./test/d9/...`）；建议 3 low。
**触碰 wire/不变量**：否（不改任何 subject 字面量，只改这些字面量住在哪个文件）。

---

### F6 — [LOW] 每个 mutator 都有"直写 + Plan 渲染"两份实现，两份的语义分歧靠注释与差分 harness 维持

**证据**（本 lane 内）
| 直写（单模式） | Plan 渲染（cluster FSM） | 分歧 |
|---|---|---|
| `session.Create` `session.go:70-106` | `session.PlanCreate` `plan.go:23-46` | 无 |
| `session.Tombstone` `session.go:181-203` | `session.PlanTombstone` `plan.go:51-70` | 无 |
| `session.JoinWithPIN` `session.go:378-398` | `session.PlanJoinWithPIN` `plan.go:114-141` | 无 |
| `agentprov.ProvisionWithPIN` `agentprov.go:104-162` | `agentprov.PlanProvisionWithPIN` `plan.go:17-57` | 无 |
| `node.Register` `node.go:84-142` | `node.PlanRegister` `plan.go:29-61` | **有**：直写 `status='ONLINE'` + 写 heartbeat + 清 `proxy_ready`；Plan 只写 identity 列、INSERT 时强制 `status='OFFLINE'`。`plan.go:24-28` 注释承认"差分 harness 把 liveness 列排除在比对之外" |
| `adminsock.handleEvict` 直接 tx（`server.go:431+`） | `node.PlanEvict` `plan.go:67-77` | 无（但 handleEvict 自己有两条分支：`server.go:415-430` seam 路径 vs `:431+` 直写路径） |

- 再往上还有两层同构包装：`NewProvisionSeam`（`cluster_forward.go:711-740`）与
  `NewJoinSeam`（`:745-770`）是 25 行逐字同构的两份（只有 payload 类型不同）；
  每个 Plan 又被两处调用——leader-local（`clusterwrite.go`）与 follower-forward
  （`cluster_forward.go dispatchForward`）。
- 本 lane 内 Plan 渲染代码：`session/plan.go` 141 + `session/proxy_plan.go` 45 +
  `agentprov/plan.go` 57 + `node/plan.go` 77 = **320 行**。全仓 46 个 `Plan*`、9 个 plan 文件、1,220 行。

**为什么是债 / 它让什么变难**
**先说清楚：双路径本身是本质复杂度，不是债。** raft FSM 的 Apply 必须确定性，
所以 leader 在 propose 前把决策做完、只复制渲染好的 SQL 文本——这是正确的设计
（`internal/cluster/command.go:78` 的注释解释得很清楚）。
债在于**两条路径同时是 live 的**：单 broker 走直写、cluster 走 Plan，
于是任何一条授权相关的判断（"session 必须 ACTIVE 才能 join"、"fp 冲突要拒"）
都必须在两个地方写对，而**生产实际跑的是直写那条**（单 broker），
**测试重点覆盖的是 Plan 那条**（d2/d3/d4/d9 差分 harness）。

具体挡住的未来改动：F1 的 `kick`、F2 的任何 auth 状态机改动，都要写两遍并证明等价。
`node.Register` 已经证明"两遍写不一样"会发生，而且发生了之后是靠**在比对里排除那些列**来收场的。

**建议**
不建议现在动——收益不明确、风险高。但建议**记一条约束**到 `docs/architecture.md`：
凡是新增 **auth 相关**的 mutator（写 `members` / `agent_provisioning` / `sessions.pin_hash` /
`sessions.state`），**只允许走 Plan 路径**，单模式用 N=1 的本地 propose，不再写第二份直写。
这样新增的授权状态机不会再产生第二份实现，存量 5 对慢慢自然收敛。

**量化**：0 行可删（本次）；约束一条，防止未来再翻倍。
**风险**：high（若真去合并——碰 FSM op 空间、碰单模式全部写路径，
且单模式引 raft 依赖会破 `internal/cluster` 的 L-2 分层约束）。因此本条建议**只写约束、不改代码**。
**触碰 wire/不变量**：是（合并会碰 `architecture.md` §3.3/§3.5 的 D2 不变量与 L-2 分层）——
所以建议才是"别动，加约束"。

---

## 反证：做得好的地方

### C1 — `internal/auth` 是一个真正的纯叶子包，`auth` / `authcallout` 的边界干净、零重叠

lane brief 的前置假设是"两个包 2,883 行，职责重叠吗？"——**这个前提不成立**。
2,883 是含测试的数字；生产代码是 577 + 598 = **1,175 行**，且边界是全仓最清爽的之一：

- `go list -deps ./internal/auth` 的全部外部依赖：`nkeys` + `jwt/v2`（加 argon2/crypto 标准库）。
  **零 DB、零 NATS、零 proto**——它是纯函数策略层，被 8 个包引用。
- `internal/authcallout` 是决策引擎：DB 查询 + role 解析 + PIN 限流 + 响应 JWT，
  它 import `auth` 而不是重新实现 `auth`。
- 两者之间只有一处真重叠（F3 的 `jwt.go`），且重叠的那半是死的。
- `internal/session` 为了不 import `auth`（避免 jwt/ed25519 链），
  把 `verifyPIN` 做成注入参数（`session.go:377`、`agentprov.go:101-103`）——
  这是**恰当**的依赖倒置，不是过度抽象：注入的是一个 2 参数函数，不是一个接口。

### C2 — 版本前缀 SSOT 是我在这个仓库里见过最扎实的一处

- 全仓生产代码里 `"tether.v<N>"` 字面量**只有 9 个**：`proto/subjects.go` 5、
  `jsstream/jsstream.go` 3、`auth/permissions.go` 1（唯一合法的破环副本，
  `permissions.go:5-11` 解释了为什么、并指名了守它的测试）。
- `test/determinism/lint_skeleton_test.go:262-295 TestNoStrayVersionLiteral` 用 **AST 扫 STRING 字面量**
  （不是 grep，所以注释里的 `tether.v2` 不误报）+ 一个 whitelist 兜底。
- 更关键的是 `:301-320 TestNoStrayVersionLiteralSelfCheck` —— **它证明这个扫描非空洞**
  （合成一个含 stray literal 的源文件 + 一条含相同 token 的注释，断言恰好抓到 1 个）。
  绝大多数项目的"禁止 XXX"lint 都缺这一步，绿了也不知道是"真没有"还是"扫描坏了"。
- `internal/auth/permissions_test.go:190 TestSubjectPrefixInSyncWithProto` 把那份副本钉在 proto 上。

**结论：wire 版本升级（v2→v3）的代价是低的。** F5 说的贵是"文法变更"，不是"版本变更"——这两件事要分开评估。

### C3 — 4 个权限模板的"啰嗦"是安全模型本身，不是 bloat

`internal/auth/permissions.go` 264 行里绝大部分是逐条列出的 subject 字面量。
这**看起来**像可以被"抽象掉"的重复，但不能：

- `architecture.md:110` B.0 §3 明确禁止 `s.<sid>.>` 大通配，理由是它会让 ctl 跨 verb / 跨发起人
  pub `.forwarded`，破坏 C.4 的强转发。NATS 的 ACL 是平坦 allow-list，
  "member of session S" 这个概念**只能**用穷举叶子表达。**啰嗦即是不变量。**
- 每一条非显然的授权都带了"为什么在这里"的注释，且注释指向具体的 review 编号
  （G3 #17、D8b §10、G5 #13、G4 §B、round-3 F1、audit-fix P11 F4）。
  `permissions.go:26-31` 那段解释"为什么未激活 ctl 也要能 pub cluster-roster"
  是我读到的最好的一条 ACL 注释：说清了触发路径、为什么放宽是安全的（O(1) 预签缓存、零 secret）、
  以及哪条负向测试仍然绿。
- 全仓 `jwt.Permissions{}` 的构造点：**4 个，全在这个文件里**（另 3 个在
  `auth/jwt_test.go` 和 `test/p1`，都是空 literal）。**没有任何测试脚手架复制过一份权限模板。**
- `internal/natscluster/config.go:164` 直接 `perms := auth.PermissionsForBroker()` 再渲染成 nats.conf 文本
  ——静态 broker ACL 不是手抄的。（这也正是 F2 想说的：这条自动化在 cluster 模式做对了。）
- `permissions_test.go` 的 10 个测试都是**形状不变量**而不是快照：
  "不许有顶层通配"、"actor 段必须锁死"、"agent 只能订自己的 node"。
  这类测试在加新 subject 时不会误红，但真加错时一定红。**这是对的测试设计。**

### C4 — 应用层 gate 虽然是手抄的，但**一处都没漏**

我逐个核对了 15 个 ctl-facing handler（exec / run / kill / ps / node.list / expose / expose-rm /
upgrade / push / pull / push-commit / finalize / caps / proxy.set / proxy.status / proxy.sub.*）
+ session.create / session.list / session.rm：**每一个都有 IsActive，且每一个都有 IsMember 或更强的 IsOwner**。
`handleUpgradeReq`（`upgrade.go:47,58`）用 IsOwner 替代 IsMember——更严，正确。
`alert.ls` / `alert.ack` / `cluster-health` / `cluster-roster` 无 session gate，
但这是**文档化的设计决定**（`docs/cluster.md:393` 明说 `alert ls`/`alert ack` 是
"member 经 NATS 的只读/团队 ack"），不是遗漏。

在没有任何结构性强制的情况下 24 处全对——这说明**纪律确实在起作用**。
F4 说的是"纪律不该是唯一的保障"，不是"纪律失效了"。

### C5 — PIN 限流器：把一个尴尬的取舍摆到台面上论证，而不是藏起来

`internal/authcallout/ratelimit.go:38-52` 显式承认：auth_callout 是 queue 分发的，
所以 E.6 的"每 IP 每分钟 ≤10 次"在 N broker 集群下实际是 ≈N×10/min。
它没有装看不见，而是给了三条理由（argon2id 64MiB 才是主防线、PIN 无长度上限、
分布式计数器会把一次分布式写放到**未认证**的 connect 路径上、DoS 面更糟），
并指明了未来若要收紧应走的低 DoS 方案（best-effort gossip）。
`:14-29` 的 trust boundary 分析同样到位（IP 来自 nats-server 填的 TCP peer，不可伪造；
共享出口 NAT 的误伤是**主动接受**的取舍，且已确认不影响已 provision 的成员）。
`blocked()` 是纯读、不消耗 token（`:113-121`），所以"正确 PIN 在封禁期到达"既被拒也不延长封禁——
这个细节想清楚了。

131 行，其中 52 行是论证。**这个比例是对的。**

### C6 — `internal/node` 不是 auth 包，而它也确实没假装是

392 行生产代码，是干净的 node 生命周期状态机（`stateForAge` 三态 + reconcile + 快照）。
它对授权的贡献只有两处：`Register` 的 session-ACTIVE 前置（`node.go:97-108`）
与 `LookupStatus` 被当作转发前置（`node.go:256-268`，doc 里说清了为什么这个 O(1) 预检值得）。
体量正当、职责单一、没有被塞进 auth 关注点。

---

## 本质 vs 偶然复杂度拆解

lane 范围内生产代码约 **3,140 行**。

### 本质（问题域强加的，约 85% / ~2,660 行）

| 项 | 行 | 为什么是本质 |
|---|---:|---|
| 4 个 NATS 权限模板 | 264 | NATS ACL 是平坦 allow-list，"session 成员"只能穷举；禁大通配是 C.4 不变量（见 C3） |
| `subjects.go` 构造器 + parser | 434 | subject 是 wire 契约，必须有类型化构造 + 严格解析 |
| authcallout 决策逻辑 | ~420 | 两阶段 agent provision、fenced/not-leader 瞬态拒绝、PIN 引导、role 解析——每一条都对应真实需求 |
| PIN 限流器 | 131 | E.6 明确要求；实现里 52 行是取舍论证（见 C5） |
| argon2id + PHC 编解码 | 119 | 密码原语；PHC 是正确选择（可迁移参数） |
| nkey / fingerprint 原语 | 102 | 身份基础 |
| session / agentprov / node CRUD | ~900 | 存储层，形状与 schema 一一对应 |
| Plan 渲染的 SQL 生成部分 | ~130 | raft FSM 的确定性 Apply 强制要求（F6） |
| broker gate 的**判断**部分 | ~80 | 24h JWT 无撤销 ⇒ 每次请求重查成员资格，是**必需**的，不是冗余 |
| natscluster / subhttp 授权投影 | ~80 | 两个不同介质（nats.conf 文本、HTTP token）各需一次投影 |

### 偶然（实现方式造成的，约 15% / ~480 行）

| 项 | 行 | 可消除性 |
|---|---:|---|
| `auth/jwt.go` 死代码 | 92 | **完全可删**（F3），另带 176 行测试 |
| Plan 渲染中重复 live mutator 决策的部分 | ~190 | 结构性（F6），现在不该动，但应加约束止损 |
| broker gate 的**样板**部分（24 份 × ~6 行 reply 分支） | ~140 | 可折叠（F4） |
| `isBroadcastClusterSubject` 后缀表 | 21 | 可由订阅表派生（F5 建议 2） |
| 4 个手工 leaf parser 与 proto parser 的重复 | ~40 | 可收编（F5 建议 3） |
| 3 条死 ACL 授权 | 3 | 立即可删（F1） |

**判断：85% 本质。** 一个同时要做 NAT 穿透 + auth_callout nkey 身份 + PIN 引导 + raft 复制的授权层，
3,140 行里只有 ~480 行是可消除的实现噪音——这是**健康的**。
把 lane 的行数当成"臃肿"证据是误判：本 lane 的问题不是写多了，是**有一半的功能没写**（F1）
且**写好的那部分在生产拓扑上默认没开**（F2）。

### 一个必须点破的观察

本 lane 的**测试/生产比是 0.79**（2,498 测试 : 3,140 生产），远低于全仓的 1.36（93,084 : 68,328）。
考虑到这是全系统的授权层，这个比例是**偏低**的——而且低的地方还偏了：
`internal/auth/jwt_test.go`（176 行）+ `test/p1/foundation_risk_test.go` 覆盖的是**生产不走的路径**（F3），
`internal/session/session_test.go` 323 行覆盖 593 行含 FSM Plan 的存储层。
真正缺的那类测试在 F5 建议 1：**把 ACL 模板与 broker 订阅表对起来的静态对账**——
一条测试就能同时钉死 F1 的 3 条死授权与 F5 的整类漂移，60 行，零运行时风险。
如果这份审计只能落地一条建议，我选这条。
