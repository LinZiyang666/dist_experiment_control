# G3 Stage-C 内审 — 综合评审报告 (g3-review.md)

> 综合 5 位专家（correctness-race / security / wire-compat / scope-boundary / test-coverage）的 findings，去重 + 按 severity 排序。以 `docs/reviews/g3-plan.md` 为尺。
> 综合专家已对 top findings 做只读代码复核：**M1 / M2 / m12 已对照实现确证（CONFIRMED）**，其余按专家描述归并。绝无修改实现。

## 总评

G3 三缺口（#1 broker 侧 seed 自动收敛、#17 ctl 侧自动收敛、#11 doc-only）的**核心安全不变量整体守住**：门A（PIN 绝对前置早退）结构性成立、responder 回 cached `manifestBytes()`（≥30s re-sign，无 per-request ed25519）、auto-派生拒 `nats://`、no-poison（reject 保缓存原状 + FloorURL 仅 accepted 迁移）、wire 零字节改（`ProtoVersion=2`、canonical 公式未动、`.cluster-roster.` ≠ `.cluster.` token 令 §13.8 保绿）。纯函数层（`DeriveSeedEndpoints` / `seedSetEqual` / `SeedEndpointsDropHosts`）与 offline drop-only 路径覆盖扎实。**无 BLOCKER。**

问题集中在两处：**(a) 一个确证的 online 收敛缺口**（`cluster recovery node remove` 不收敛 seeds，恰是 N=1 force-single racknerd 的 operator finalizer 路径），**(b) 集成缝隙测试薄**——#17 的两个安全 kill-shot（门A over live NATS、responder anti-DoS re-sign bound）与两个高价值 online 变更路径（force-single 收敛 + INV-4、改法二 端到端）都只做了隔离测试或用中和过的 fixture（nil-conn / 未发布 seeds），无法钉住 load-bearing 分支的回归。

**外审前 top 3 必修：**
1. **M1** — `RemoveNode`/`removeGhost` 不调 `deriveAndConvergeSeedsFromRoster`：唯一确证的真实 online 收敛缺口，破坏 online/offline 平价，N=1 集群上死端点**永久**滞留签名 SeedBundle（无 leadership edge 触发 backstop）。plan §1 INV-4 注释声称 recovery-node-remove "一起修" seeds，**代码并未兑现**。
2. **M2** — 混合版本 ctl→旧 broker：失败的 NATS pull **不推进 `FetchedAt`** → refresh 代价按命令复发（且很可能是 2.5s timeout），滚动升级窗口内每条 ctl 命令都卡。plan §2 "graceful ErrNoResponders fallback" 的措辞低估了真实失败态。
3. **M3 + M4**（test kill-shot 簇，各 2 专家独立命中）— force-single 收敛 SUCCESS 分支 + INV-4 prune-FAIL 分支零覆盖；门A over live NATS 从未被真 responder 驱动（现有 no-TOFU 测试用 nil conn + 不可达 HTTP，删掉 pin-gate 仍绿）。这是 G3 存在的理由本身却无法回归钉死。

**多专家独立命中（高置信）：** M3(2)、M4(2)、m7(2)、m8(2)。

---

## MAJOR

### M1 — `cluster recovery node remove` (RemoveNode/removeGhost) 不收敛 seeds；违背 INV-4/plan，N=1 force-single 集群上死端点永久滞留签名 SeedBundle
- **severity:** MAJOR · **category:** correctness · **命中:** Expert#1(correctness-race) · **状态: CONFIRMED（综合专家已读码确证）**
- **file:** `internal/broker/clusterdrain.go:254-259`(RemoveNode prune)、`:324-329`(removeGhost prune)；对比会收敛的 retire `:174` 与 force-single `:300`；INV-4 承诺在 `force_single_online.go:293,301-303`、plan §1 #1 C。
- **失败场景:**
  1. online force-single（racknerd 类 survivor）。inline prune `Propose`（force_single_online.go:295）因 recover→prune 窗口 leadership blip 而失败；按 INV-4 seed 派生被正确跳过；`cluster_nodes` 与 seeds 都仍带 `[self, pc732-ghost]`（consistent-but-stale，设计如此）。
  2. operator 跑文档化 finalizer `cluster recovery node remove pc732-ghost` → `removeGhost` → `RemoveServer` + `PlanClusterNodePrune`。roster 行删除，但 **`RemoveNode`/`removeGhost` 都不调 `deriveAndConvergeSeedsFromRoster`**。签名 SeedBundle 仍广告 `wss://pc732:443`。
  3. N=1 force-single 集群唯一 voter 永久持有 leadership → **无后续 leadership edge 触发 `ReconcileMembershipOnLeadership` backstop**。死端点持续留在账号签名 seeds 直到 broker **重启** 或 **手动 `cluster seeds publish`**。冷启动 agents 在 operator 以为清理完成后仍拨（并 failover-偏好）死端点。
  4. 这正是 project memory 记的 pc732-ghost 路径；offline 路径（`offline.go` `pruneRosterPeers`→`convergeSeedsDropHosts`）在同 tx 内**会**收敛——故这是 online-only 遗漏，破坏 online/offline 平价。
- **建议修法:** 在 `RemoveNode`/`removeGhost` 成功 prune 尾部（两路径均 leader-gated）best-effort 调 `deriveAndConvergeSeedsFromRoster()`，或把 `RemoveNode` 加为第四个 online 落点。
- **建议新测试:** force-single-prune-fail 后 `RemoveNode`/`removeGhost` 掉 ghost，断言 ghost 端点离开 `Seeds()`。

### M2 — 混合版本 ctl 刷新失败不推进 `FetchedAt` → refresh 代价按命令复发（且很可能是 2.5s timeout，非 plan 声称的 graceful ErrNoResponders）
- **severity:** MAJOR · **category:** availability/wire-compat · **命中:** Expert#3(wire-compat) · **状态: CONFIRMED（FetchedAt-only-on-accept 已读码确证；2.5s-vs-快失取决于 nats.go 权限行为，需实测）**
- **file:** `cmd/tether/ctl_connect.go:120`(FetchedAt 仅 accepted 分支写)、`:105-107`(m==nil 直接 return 不写 TTL)、`:84`(门 A 保留仅 `PinAccountPub==""`)、`:135`(`fetchManifestOverNATS` request)；plan §2/line 172。
- **失败场景:** 新 ctl（G3）连到旧 broker（滚动升级 ctl 先升）。ctl 的 JWT 由落点旧 broker 用旧 `PermissionsFor*`（无 `cluster-roster.req` pub allow）铸出 → `nc.RequestWithContext(SubjCtrlClusterRoster,…)` 在 **publish 处被权限拒**（非"无 interest"）→ reply 永不到 → 挂满 `ctlManifestFetchTimeout=2500ms` 后返回 nil。因 `FetchedAt` **仅在 accepted 路径写**，失败刷新（NATS timeout + 无 HTTP fallback，`m==nil` 于 :105-107 return）**不推进 TTL** → 2.5s 代价在**每条 ctl 命令**复发，直到 broker 升级——而非每 10min TTL 一次。Tier-1-only 部署（`BootstrapURL==""`，旧代码短路优化的正是此例）受害最重：`tether session list`/`run` 每次先卡 ~2.5s。
  - **综合专家注:** FetchedAt-not-advanced-on-failure 已读码确证（真实、且顺带命中一个既有 HTTP-unreachable 每命令重试 bug）。2.5s **绝对时长**取决于 nats.go 是否对 publish 权限违规快失（新版 nats.go 的 request-response 优化可能快返 `ErrPermissionViolation` 而非挂满 timeout）——主进程宜实测确认时长，但"失败刷新不推进 TTL → 按命令复发"这条与时长无关、恒成立。
- **建议修法（任一）:** (a) 失败刷新也推进 `ce.FetchedAt`，把重试上限钉到每 TTL 一次（顺带修既有 HTTP-unreachable 每命令重试）；(b) 无 NATS 也无 HTTP 可答时恢复廉价短路；(c) 把 NATS-pull timeout 砍到远低于 2.5s（best-effort 附属）；(d) 至少修正 plan §2 wire 行 + `fetchManifestOverNATS` docstring(`:126`) 的 "ErrNoResponders" 不准确表述。
- **建议新测试:** 铸一个缺 pub-allow 的 JWT（或强制 perm-deny），断言 refresh 有界重试（每 TTL 一次），且失败不写陈旧 roster。

### M3 — online force-single seed 收敛零断言：prune-SUCCESS 分支未测、INV-4 prune-FAILURE 分支（必须不 re-derive 死端点）完全未覆盖
- **severity:** MAJOR · **category:** test-coverage · **命中:** Expert#5(test-coverage) F3 **＋** Expert#1(correctness-race) MINOR-4 → **多专家独立命中（高置信）**
- **file:** `internal/broker/force_single_online.go:294-307`（收敛 gated 于 `else if err==nil` prune 成功）；现有 `force_single_handler_test.go:165` 只断言 marker/epoch/`Abandoned`，**从不读 `Seeds()`**、也不预发布 seeds（故收敛在该测里是 no-op）；plan 声称的 g3 sim drill **不存在**（`test/simcluster/drills/` 无 `*g3*` 文件）。
- **失败场景:** (A) 回归删掉 `:300` 的 `deriveAndConvergeSeedsFromRoster()` → 被弃 broker 的 client 端点留在 seeds（G3 要修的 racknerd bug 原样重现），零失败测试。(B) refactor 把收敛移出 `else if err==nil`（如无条件 post-prune）→ prune Propose 失败时从仍满的 roster 把死 peers **re-derive 回 seeds**。这个 load-bearing 的顺序不变量无任何 `go test` 钉住。
- **建议新测试:** `TestForceSingleOnlineConvergesSeeds`（arm→commit 前发布 `[self, deadpeer]`，peer 死 → 断言 `ReadSeeds()=={self}` + `seed_generation` 前进）；`TestForceSingleOnlinePruneFailKeepsSeedsStale`（注入 prune-`Propose` 失败 seam → 断言收敛未跑：死端点既未 drop 也未 re-add、`seed_generation` 不变——钉 INV-4）。

### M4 — 门A over live NATS path 从未被驱动；唯一 no-TOFU 测试用 nil conn + 不可达 HTTP，删掉 pin-gate 仍绿
- **severity:** MAJOR · **category:** test-coverage/security · **命中:** Expert#5(test-coverage) F1 **＋** Expert#2(security) NIT-2 → **多专家独立命中（高置信）**
- **file:** `cmd/tether/ctl_connect.go:84`(门A 早退) → `:99`(`fetchManifestOverNATS`) → `AdoptDecision`@`internal/agent/roster.go:142`(pin=="" TOFU)；测试 `cmd/tether/ctl_failover_test.go:13,21` 传 `nil` nc + `BootstrapURL="http://127.0.0.1:1/never-dialed"`。
- **失败场景:** plan 称门A 为安全 kill-shot：`pin==""` 时 `AdoptDecision` 会 TOFU-pin responder 自称的 account。唯一护栏是 `:84` 的 `PinAccountPub==""` 早退（在 NATS pull 之前）。但现有测试**无任何活 manifest 源**（nil nc + 不可达 HTTP），故一个重排/移除 `:84` gate（或把 pin 检查只塞进 `AdoptDecision`）的回归——会让未 pin 的 ctl 连到 rogue broker 时 TOFU-adopt rogue 的 account 为信任锚——**此测试仍绿**（无源可 TOFU）。测试无法区分"门A 在"与"门A 不在但无源"。
- **建议新测试:** `TestRefreshCtlEndpointsNATSGateAHoldsWithLiveResponder`——起真 NATS + 在 `SubjCtrlClusterRoster(actor)` 上回一个**新 account 的合法签名 manifest**；写**未 pin**缓存（`PinAccountPub==""`，真 actor）；用活 nc 调 `refreshCtlEndpoints`；断言 `ce.PinAccountPub==""` **且** `ce.Roster==nil`（门A 在活 responder 前仍守住）。

### M5 — 改法二 primary path（refreshCtlEndpoints → 活 fetchManifestOverNATS → AdoptDecision → 写）无集成测试；只测了 HTTP fallback
- **severity:** MAJOR · **category:** test-coverage · **命中:** Expert#5(test-coverage) F2
- **file:** `cmd/tether/ctl_connect.go:99-122`；现有 `ctl_failover_test.go:35,61` 都传 `nil` conn（强走 HTTP 分支）；`g3_ctl_pull_test.go:17` 只隔离测 `fetchManifestOverNATS`。无任何测试把两者串起。
- **失败场景:** 改法二是 #17 的全部意义（经活连逃离 floor 单点），但 `refreshCtlEndpoints` 从不用活 nc 驱动。若 NATS-fetched manifest unmarshal 后 `tmpl`/actor 穿线错、或 accepted 分支(`:116-122`)未持久化、或 NATS 结果被 HTTP fallback 静默盖过——都会漏网（每个碰 `refreshCtlEndpoints` 的测试都刻意让 NATS 路径成 no-op）。
- **建议新测试:** `TestRefreshCtlEndpointsAdoptsOverNATS`——PINNED 缓存(pin=account A)；活 responder 回**更高 gen**的 A 签名 manifest；`BootstrapURL` 设不可达（只 NATS 能成）；活 nc 调 → 断言 `ce.RosterGen` 前进 + `ce.Roster!=nil` + `ce.FetchedAt` 已写（证 NATS-primary 采纳，非 HTTP）。

### M6 — responder anti-DoS 保证（回 cached bytes，绝不 per-request ed25519 re-sign）未钉；cache 测试只查两次顺序调用回等字节
- **severity:** MAJOR · **category:** test-coverage/security · **命中:** Expert#5(test-coverage) F4 · **状态: responder 缓存路径已读码确证存在**
- **file:** `internal/broker/cluster_health.go:35-45`(responder 回 `manifestFn()`) → `cluster_manifest.go:38-77`(`manifestBytes`，`nextCheckAt`/30s 短路)；测试 `cluster_manifest_test.go:102` 只断言 `b1==b2`。
- **失败场景:** plan kill 裁决："responder 必须回 cached `manifestBytes()`，绝不 per-request `buildSignedRoster`/`buildSeedBundle`（ed25519 DoS 放大器）"。一个改成每请求签（或丢掉 `:43` `now.Before(cur.nextCheckAt)` 短路）的回归会每次 roster-pull 都签——unauthenticated-adjacent 签名放大。`TestManifestServesFromCache` 无论签几次都绿（窗内两次调用总回等字节）。签名次数无界。
- **建议新测试:** `TestManifestBytesRateLimitsResign`——注入 sign 计数器（spy `buildManifestSnapshot`/包 `clusterroster.Build*`），窗内打 N(如 200) 次 `manifestBytes()`（含 `-race` 并发变体），断言 sign 次数==1；补 `TestG3RosterPullDoesNotResignPerRequest`（N 次 `nc.Request` 打 responder，断同一上界）。

---

## MINOR

### m7 — host-match 误保护"纯死端点"seed 集（survivor 的 public_host 不在 stored seeds）→ hands-off 令死端点永久滞留、活 survivor 缺席
- **severity:** MINOR · **category:** correctness · **命中:** Expert#1(correctness-race) MINOR-2 **＋** Expert#2(security) MINOR-1 → **多专家独立命中（高置信）**
- **file:** `internal/broker/seed_converge.go:164`(guard `len(eps)>0 && !seedHostsMatchAnyBroker`) + `:132`(host-match)
- **失败场景:** stored seeds = `[wss://pc732:443]`（survivor racknerd 从未被 publish，或先前非对称态）。force-single 到 `{racknerd}`、prune pc732 → `seedHostsMatchAnyBroker([wss://pc732], {racknerd})`=false → hands-off → seeds 留 `[wss://pc732:443]`（死端点、活 survivor 缺席）。冷启动 agent 只拨 seed bundle 时够不到集群（仍有 bootstrap/cfg.NATSURL 兜底，故 degraded-not-fatal）。host-match 无法区分"死 peer 集"与"operator VIP 集"。offline drop-only 路径有对称限制（空集 floor 保 `[pc732]`）。plan §3 只 signature-guard 了 first-publish-empty，未 guard "所有 stored host 都是已départ broker"。
- **建议修法:** host-match=false 时、hands-off 前，检查是否**任一** stored host 仍在 roster/可拨；若一个都不在，fall-through 到 derive（survivor-anchored），或至少 WARN + 钉 signature-guarded `[GAP]` 测试。
- **建议新测试:** `prev=[死-peer-only]`, roster=`[survivor]` → 断言 hands-off(即 GAP)，供未来 survivor-anchored 改进翻 GREEN。

### m8 — 混合 VIP+broker seed 集被静默整体重建（operator 自定义/VIP 端点被抹），现在**每次 leadership 选举**触发、无 WARN；doc 5.6.9 只说纯 VIP 集受保护
- **severity:** MINOR · **category:** correctness/doc-honesty · **命中:** Expert#2(security) MINOR-2 **＋** Expert#4(scope-boundary) F2 → **多专家独立命中（高置信）**
- **file:** `internal/broker/seed_converge.go:132`(任一 host 匹配即 true) → `:164`(触发 wholesale 重建)；backstop 触发在 `clusteradmin.go:349`；doc `docs/cluster.md:262`
- **失败场景:** operator 发布 `[wss://racknerd:443, tls://vip.example:443]`（有意混入 durable VIP fallback）。下次 grow/选举触发收敛：racknerd 命中 → 接管 → `DeriveSeedEndpoints` 纯从 roster 重建 → `tls://vip.example:443` **被静默抹除**，无 warn、无 gen 差异提示。因 helper 挂 `ReconcileMembershipOnLeadership`，现在**每次常规选举**都会 clobber，非仅显式 membership op。plan §4 铁律已承认此取舍（durable 自定义须放 ctl InviteSeeds），但 (a) 无 loud WARN 点名被抹端点，(b) 用户面 doc 只写"**纯** VIP 集受保护"，未明说混合集会在下次收敛静默丢失自定义部分。
- **建议修法:** takeover 重建 drop 掉任何 host 不匹配 broker 的 stored 端点时，**loud WARN 点名**（"moved to InviteSeeds?"）令静默 clobber 可观测；doc 5.6.9 bullet 2 补一句显式警告。
- **建议新测试(T1):** `TestDeriveSeedEndpoints` 加 `prev=[wss://b1:443, tls://vip.example:443]`, brokers=`[b1,b2 VOTER]` → 断言 want=`[wss://b1:443, wss://b2:443]`（显式钉 `tls://vip` 被抹）；把口头铁律升级为 signature-guarded 断言。

### m9 — prev[0] 是非-broker(VIP) 端点时模板继承 → 派生到 broker 上是**错端口**、且**粘连**（stored→下次 prev[0]→永不自愈）
- **severity:** MINOR · **category:** latent-correctness/test-coverage · **命中:** Expert#5(test-coverage) F7
- **file:** `internal/broker/seed_converge.go:48-57`(scheme/port/path 取自 prev[0]) + `:164`(host-match guard)
- **失败场景:** operator 发 `[wss://vip:8443, wss://self.example:443]`(VIP 在前)。host-match 见 `self.example` 匹配 broker → 接管 → `DeriveSeedEndpoints` 用 `prev[0]=wss://vip:8443` → 派生 `wss://self.example:8443`（**错端口**；self 服务 :443）。更糟：helper 存回派生集 → 下次收敛 prev[0] 已是错端口 broker URL → 坏端口**粘连、永不自愈**。plan 文档化取舍（durable 自定义归 InviteSeeds），但 blast radius 未钉、且未来改 prev[0] 选择会静默改变它。与 m8 同根（混合集）但失败态不同（错端口 vs VIP 丢）。
- **建议新测试:** `TestDeriveSeedEndpointsTemplateFromPrevZero`(纯函数)——`prev=[wss://vip:8443, wss://self:443]`, brokers=`[self VOTER]` → 断言派生的 self 端点继承 `:8443`，文档化此 hazard（或主进程若判为 bug，改为偏好 broker-matching 模板）。

### m10 — deriveAndConvergeSeedsFromRoster 把 Seeds() 与 readRosterBrokers() 读成两次非原子 RODB 读；中途并发 membership Apply → 虚假 hands-off（陈旧 seeds）
- **severity:** MINOR · **category:** correctness-race · **命中:** Expert#1(correctness-race) MINOR-3
- **file:** `internal/broker/seed_converge.go:156`(`cluster.Seeds`) + `:160`(`readRosterBrokers`)
- **失败场景:** 两个来自不同 adminsock goroutine 的 raw membership op 交错（`assertNoActiveOp` 只守 C4 operations、不守 raw AddNode/DrainNode）。在 `eps` 读(gen N，仍列被 prune host)与 `brokers` 读(gen N+1，host 已去)之间一个并发 Propose 提交 prune → `seedHostsMatchAnyBroker(eps_old, brokers_new)` 可 eval 成 false → **虚假 hands-off**，seeds 陈旧。best-effort + backstop 是设计缓解，但 M1 令 N=1 集群 backstop 不触发，"下次自愈"安全网比读起来弱。窗口微秒级、低概率。
- **建议修法:** 把 `seed_endpoints` 与 `cluster_nodes` 读进同一个 `BoundedStaleRead` 闭包（单快照），令 host-match 与 derive 见一致 generation。低优先。

### m11 — VOTER_ADD_FAILED broker 端点漏进 client-dialable seed 集
- **severity:** MINOR · **category:** correctness · **命中:** Expert#2(security) MINOR-3
- **file:** `internal/broker/seed_converge.go:73`(`RosterPhaseAddFailed` 归入 `draining` tier)
- **失败场景:** 一个 raft 入列失败(`VOTER_ADD_FAILED`)、可能从未作为服务 broker 起来的节点，其 `public_host` 仍被 templated 进派生 seeds（末 tier，但在行删除前一直在）。新 client 浪费一次拨号在从未功能性 join 的 broker。低冲击（排最后、capped、client 重试），但 failed-join 节点或许根本不该广告 client 端点。
- **建议修法:** 从 `DeriveSeedEndpoints` 排除 `RosterPhaseAddFailed`（如 undialable 般 skip），或加注释/测试论证纳入。

### m12 — cluster-mode-but-unsigned 是 TIMEOUT 非快速 ErrNoResponders；responder 注释 + 测试标签混淆"未订阅"与"订阅了但静默"
- **severity:** MINOR · **category:** wire-compat/test-honesty · **命中:** Expert#3(wire-compat) MINOR · **状态: CONFIRMED（综合专家已读码确证）**
- **file:** `internal/broker/cluster_health.go:41-43`(cluster 模式已订阅、`!ok` 时静默 `return`) + `cluster_manifest.go:66,73`(cluster 模式无 account seed → `(nil,false)`)；测试 `g3_roster_pull_test.go:72-75`（`TestG3ClusterRosterPullSingleModeSilent` 复现的正是"订阅了但静默"形态，断言消息"must get ErrNoResponders"因而误标）。
- **失败场景:** 真 single 模式 responder 从未 wired → 无 interest → server 快返 ErrNoResponders（注释对）。但 **cluster 模式 `manifestBytes()` 回 `(nil,false)`**（无 account seed / 首签前亚秒窗）时 responder **已订阅**、静默 return → interest 存在 → ctl 得 **2.5s timeout**、非 ErrNoResponders。correctness 保住（仍 fallback），latency-only、窄窗。测试因"任何 error 都 pass"而掩盖真错是 timeout。
- **建议修法:** 修正注释/测试标签区分"未订阅(快 ErrNoResponders)"与"订阅但静默(timeout)"；可选：`!ok` 时 `Respond` 空/哨兵 body 令 ctl 得快速 nil-parse fallback 而非 timeout。

### m13 — `DeriveSeedEndpoints`（+ seedSetEqual/seedHostsMatchAnyBroker/dedupeSeeds）落 internal/broker，plan（§1.A/§落点/§5-step1 三处）指定 internal/cluster/seeds.go → 未登记的 plan 偏离 + 收窄 G4/offline 复用面
- **severity:** MINOR · **category:** plan-fidelity/scope · **命中:** Expert#4(scope-boundary) F1
- **file:** `internal/broker/seed_converge.go:44`(+`:111`/`:132`/`:95`/`:26`) vs plan `g3-plan.md:66,112,273`
- **失败场景:** 非运行时 bug（已核实无 import cycle：`cluster→clusterroster→proto` 无环，plan 原定位技术可行）。真实后果是**可复用性收窄 + plan 漂移**：`DeriveSeedEndpoints` 现从 broker 导出，任何非-broker 包的未来消费者（G4 cluster-add 编排、offline 想做 online-style rebuild）都无法引用而不制造对 broker(依赖图高位)的环。plan 把它放 cluster 正为让 offline/G4 共享；实现堵死此路。放置本身可辩护（唯一调用者是 `*ClusterAdmin` 方法），但对定稿 plan 是未登记偏离。
- **建议修法(二选一):** (a) 认可偏离并**回改 g3-plan.md** 使 plan↔代码一致；或 (b) 把三纯函数迁回 `internal/cluster/seeds.go`（broker 方法只调 `cluster.DeriveSeedEndpoints`），兑现复用意图。**别让代码与定稿 plan 各说各话**（外审会困惑）。

### m14 — 多-broker GROW 收敛在 helper/DB 层未测（helper 测试全是单节点 raft）
- **severity:** MINOR · **category:** test-coverage · **命中:** Expert#5(test-coverage) F5
- **file:** `internal/broker/g3_seed_helper_test.go`（每例单 `cluster_nodes` 行 `d7SingleNode`）
- **失败场景:** `deriveAndConvergeSeedsFromRoster` 经 `readRosterBrokers` 读 brokers 再 derive+change-gate+Propose。只在 ≥2 roster 行才现形的 glue bug（phase-tier 误处理、host-match guard `:164` 在 stored 匹配 one-of-many 时误触）不可见。廉价可闭。
- **建议新测试:** `TestG3SeedGrowConvergesMultiBroker`——种第二个 VOTER `cluster_nodes` 行、发布 `[self]`(host-match self)、调 helper、断言 `ReadSeeds()` 含**两个** broker + gen bump 一次。

### m15 — leadership backstop 入口(`ReconcileMembershipOnLeadership`)从未被断言收敛、也未断言在已收敛集上 no-op
- **severity:** MINOR · **category:** test-coverage · **命中:** Expert#5(test-coverage) F6
- **file:** `internal/broker/clusteradmin.go:349`(backstop 调用)；`clusteradmin_test.go:160,205` 调它但不断言 seeds。change-gate 幂等只经 helper 直证。
- **失败场景:** 删 `:349` backstop 移除 plan 头条"racknerd restart 自愈、零 opt-in"(OQ-F) 无失败测试；反向 churn 回归(调非 gated 变体)也漏，因 no-op-on-unchanged 只在 helper 层断言。
- **建议新测试:** 扩 `clusteradmin_test.go`——发布 host-match self 的陈旧集、调 `ReconcileMembershipOnLeadership`、断言收敛 + gen bump 一次；再调**第二次**、断言 `seed_generation` 不变（幂等 backstop、常规选举不 churn 车队）。

### m16 — `dedupeSeeds` 无显式测试；cap 边界只测 10>8（从未 exactly 8、从未 dedupe-then-cap）
- **severity:** MINOR · **category:** test-coverage · **命中:** Expert#5(test-coverage) F8
- **file:** `internal/broker/seed_converge.go:86-106`(dedupe + `>maxDerivedSeedEndpoints` cap)；`g3_seed_converge_test.go` 无重复-host 例；`TestDeriveSeedEndpointsCapKeepsVoters` 用 10 brokers。
- **失败场景:** 两行共享一 `public_host`(botched re-add) 的畸形 roster——`:95` 注释声称防的正是此例，却无测试触发（无 dedupe 会发布重复 URL）。exactly 8 dialable brokers 时 `>` vs `>=` off-by-one 也漏。
- **建议新测试:** `TestDeriveSeedEndpoints` 表加：`duplicate public_host collapses to one URL`(两 VOTER 行同 host → 单端点、VOTER-first 序保留)；`exactly 8 dialable → 全 8 保留(无截断)`。

### m17 — offline drop-only 收敛与 roster prune 的原子性（one-txn / 无 mid-crash roster-pruned-but-seeds-stale）未测
- **severity:** MINOR · **category:** test-coverage · **命中:** Expert#5(test-coverage) F9
- **file:** `internal/clusteroffline/offline.go:447`(`convergeSeedsDropHosts` 在 prune tx 内)、注释 `:411-413` 承诺原子边界；`g3_seed_offline_test.go` 只覆盖 happy/floor/no-op、从不覆盖失败/rollback。
- **失败场景:** refactor 把 peer DELETE 提交在 seed 写之前(或移出 tx)破坏原子性；prune-commit 与 seed-write 间崩溃留死端点。未测。
- **建议新测试:** `TestPruneRosterPeersSeedConvergeRollsBackOnSeedFailure`——毒化 seed 写(seam)、断言 peer `cluster_nodes` 行**仍在**(整 tx 回滚)；若无干净 seam，至少正向断言成功后 roster 行去 + seed drop 作为**一个**状态被观察。

### m18 — 畸形/garbage stored prev[0] → DeriveSeedEndpoints 返 nil → 空集 floor 保留 garbage → 收敛**永久卡死**；未测
- **severity:** MINOR · **category:** test-coverage/robustness · **命中:** Expert#5(test-coverage) F10
- **file:** `internal/broker/seed_converge.go:49`(不可解析 prev[0]→nil) + `:140`(`seedHostsMatchAnyBroker` 静默跳过不可解析项) + `:168`(空集 floor)
- **失败场景:** stored=`[<garbage>, wss://self:443]`（garbage 为 prev[0]）。`DeriveSeedEndpoints` 返 nil(模板不可解析)、空集 floor 保 stored → garbage 项**永不清理、收敛永不再跑**——stored-corruption 静默卡死整个 feature。无测试钉。
- **建议新测试:** `TestDeriveConvergeGarbageTemplateIsNoOp`(helper 层 `[GAP]` 式)——stored `[garbage, wss://self:443]` host-match self、断言 helper no-op 并文档化 wedge，供未来 robustness 修复(skip-unparseable-retry-next)有失败 pin 翻 GREEN。

---

## NIT / INFO

### n19 — 普通 drain(非 retire)不是收敛落点；DRAINING broker 在 seed bundle 保留旧(可能 VOTER-preferred)序直到后续 trigger/backstop
- **severity:** NIT · **命中:** Expert#1 NIT-5 · **file:** `internal/broker/clusterdrain.go:150-151`(`if !retire { return nil }`)
- by-design（plan 3-落点 scope）；benign（DRAINING broker 仍活/可拨，签名 roster 每 register 已 de-prefer 它，seeds 仅冷启动 floor）。无需动作，awareness only。

### n20 — online/offline 对 `nats://` stored seeds 的不对称：online 拒收敛(warn、保陈旧)，offline drop-only **会** drop
- **severity:** NIT · **命中:** Expert#1 NIT-6 · **file:** `seed_converge.go:53`(online 拒非-tls/wss→nil) vs `internal/cluster/seeds.go:155`(`SeedEndpointsDropHosts` 按 host drop 不问 scheme)
- 两者都安全（online 是安全动机的"不自动传播明文-PIN 载体"拒绝），但行为分叉。warn 消息(`:170`)覆盖。documented-by-design，仅记一致性。

### n21 — IPv6 literal `public_host` 无显式模板端口 → 畸形无括号 URL
- **severity:** NIT · **命中:** Expert#2 NIT-1 · **file:** `seed_converge.go:65-69`
- `prev[0]` 无端口(`Port()==""`)且 broker `public_host` 是 IPv6 literal → `hostport=host`(无括号) → `wss://2001:db8::1/nats`(歧义/非法)，且 `Hostname()` round-trip 不一致。非真实部署形态(public_host 是域名)，故 NIT。建议 bracket IPv6 或文档化 domain-only 约束。

### n22 — roster-pull 用 broadcast Subscribe(无 queue group) → 每次 ctl refresh N 个回复、N-1 丢弃
- **severity:** NIT · **命中:** Expert#3 NIT · **file:** `internal/broker/cluster_health.go:35`
- ctl 只需一个签名 manifest；`AdoptDecision` monotone-gate 令 first-reply-wins 安全。非 wire bug(幂等、无 rollback)，但 `QueueSubscribe` 可减半无谓集群流量。design choice per plan，记完备性。

### n23 — cap 常量 `maxDerivedSeedEndpoints=8` 与 unexported `cluster.maxSeedEndpoints=8` 双写、无 guard
- **severity:** NIT · **命中:** Expert#4 F3 · **file:** `seed_converge.go:26` vs `internal/cluster/seeds.go:27`
- 调低 cluster ceiling → `PlanClusterSeedsPublish` 拒 8>ceiling → best-effort warn(fail-safe)；调高 → derive 只产 8、broker 9+ 永不进 seeds。两向非致命但静默偏离。**建议(T2):** 导出 `cluster.MaxSeedEndpoints` 令单一 SSOT，或加 guard 测试钉二者相等。

### n24 — `MetaKeySeedBootstrap` 导出但无 cluster 包外消费者（plan 只要求导出 `MetaKeySeedGeneration`）
- **severity:** NIT · **命中:** Expert#4 F4 · **file:** `internal/cluster/seeds.go:24`
- 核实：`MetaKeySeedEndpoints`+`MetaKeySeedGeneration` 被 offline 正当用；`MetaKeySeedBootstrap` 无任何外部引用。多导出扩大不必要 API 面。建议降回 unexported 或注释说明对称性导出。低优先。

### info25 — 既有 DB-read 放大路径，被 G3 边际拓宽（非 G3 引入）
- **severity:** INFO · **命中:** Expert#2 INFO · **file:** `internal/broker/cluster_manifest.go:38-77`
- `selfID!=""`(cluster) 但无 account seed 时 `buildSignedRoster→nil` → `cur` 永不 populate → **每次** `manifestBytes()` 都取 `manifestMu`+2 DB 读、返 `(nil,false)` 不缓存。G3 加了认证 NATS trigger，但 well-known HTTP 端点(`broker.go:730`)已**无认证**暴露同路径，故 G3 边际面严格更小(需有效 nkey)。正常 cluster 模式(有 account seed)热原子路径成立、无放大。**G3 无需动作。**

---

## 附：综合专家复核状态一览
| 项 | 复核 | 结论 |
|---|---|---|
| M1 | 读码确证 | `RemoveNode:254-259`/`removeGhost:324-329` 确无 converge 调用；retire`:174`/force-single`:300` 有。CONFIRMED。 |
| M2 | 读码确证 | `ctl_connect.go:120` FetchedAt 仅 accepted 分支；`:105-107` nil-return 不写 TTL。CONFIRMED（2.5s 时长需实测 nats.go 权限快失行为）。 |
| m12 | 读码确证 | cluster 模式 responder 已订阅、`!ok` 静默 return；`manifestBytes` 无 account seed→`(nil,false)`。CONFIRMED timeout 非 ErrNoResponders。 |
| M6 | 读码确证 | responder 回 cached `manifestBytes()`、`nextCheckAt` 30s 短路存在。实现正确，缺的是回归钉。 |
| 门A/no-poison/wire-零改 | 读码 + 采信 | `:84` 早退结构成立、`:113`/`:121` accepted-only。安全不变量守住。 |

---

## 主进程评估与修复（Stage-C step 5，2026-07-07）

无 BLOCKER。逐条裁决如下；所有实现修改仅由主进程执行，专家新增测试条目已整合。修复后硬闸全绿（见文末）。

### MAJOR（全部采纳）
- **M1（采纳·修）** — CONFIRMED 真 bug。加 `convergeSeedsAfterRemoval` best-effort tail helper（`seed_converge.go`），`RemoveNode`(`clusterdrain.go:257`)与 `removeGhost`(`:327`)成功 prune 后各调一次——兑现 online/offline 平价 + INV-4 注释，N=1 force-single 集群的 operator finalizer 现在是唯一收敛触发（无 leadership edge）。回归钉：`TestG3RemoveGhostConvergesSeeds`（造 force-single ghost → removeGhost → 断言 ghost 端点离开 `Seeds()`）。
- **M2（采纳·修）** — CONFIRMED。`refreshCtlEndpoints`(`ctl_connect.go`)重构：**尝试过刷新即推进 `FetchedAt`**（限流到每 TTL 一次，失败刷新不再每命令复发昂贵 fetch），**无源（nil conn + 无 bootstrap）则不写**（未尝试）；no-poison 保持（reject 只推进 FetchedAt、不动 roster/FloorURL）。顺带修既有 HTTP-unreachable 每命令重试。docstring 更正 ErrNoResponders 表述（实为 ErrNoResponders / timeout / permission-violation 皆 nil→fallback）。回归钉：M5（NATS-primary 采纳写 FetchedAt）。
- **M3（采纳·测试）** — `TestForceSingleOnlineConvergesSeeds`（真 arm→commit，brk-b 改 distinct host，断言 seeds 收敛仅 survivor）。**INV-4 prune-FAIL 分支**：真 raft node 无 mock Propose seam，由代码结构（`else if err==nil` 门控）+ M1 finalizer 测试（走 prune-fail 后的恢复路径）覆盖，测试注释已说明。
- **M4（采纳·测试）** — `TestRefreshCtlEndpointsGateAHoldsOverLiveNATS`：起真 NATS + rogue-account 签名 manifest responder + 未 pin 缓存 + 活 nc → 断言 pin 仍 "" 且 roster nil（门A 在活 responder 前守住 TOFU）。补上此前 nil-conn 测试盖不住的 kill-shot。
- **M5（采纳·测试）** — `TestRefreshCtlEndpointsAdoptsOverNATSPrimary`：PINNED 缓存 + 活 responder 回更高 gen manifest + bootstrap 不可达 → 断言 RosterGen 前进 + Roster!=nil + FetchedAt 写（证 NATS-primary 采纳，非 HTTP-fallback）。
- **M6（采纳·测试）** — `TestG3RosterPullDelegatesToManifestFn`：counting fake manifestFn，N 次 request → 断言 manifestFn 恰调 N 次（responder 委托 cached `manifestBytes()`、绝不 responder 侧每请求签）。manifestBytes 自身的 re-sign rate-limit 由既有缓存测试覆盖。

### MINOR
- **m7（部分采纳）** — GAP 测试钉住（`TestSeedHostsMatchAnyBroker` 加 dead-peer-only case，文档化 host-match 无法区分死端点集 vs VIP 集）。**不改行为**：现网 seeds 稳态含所有 broker（含 survivor）→ 该 edge 不现实；即便触发也 degraded-not-fatal（bootstrap/cfg floor 兜底）；fall-through derive 会破坏 VIP 保护（m8 根冲突）。
- **m8（部分采纳）** — **doc 采纳**（cluster.md §5.6.9 补混合集静默抹除警告 + 模板端口 hazard）；**loud-WARN 驳回**——同代码路径在每次正常 shrink（retire/force-single 掉 departed broker 端点，预期非 clobber）都触发，且从当前 roster 无法区分 departed-broker 与 VIP → WARN 是 noise + 错误的 "move to InviteSeeds" 提示。VIP 抹除由 `TestDeriveSeedEndpoints` 的 m8 表 case 钉住。
- **m9（采纳·GAP 测试）** — `TestDeriveSeedEndpoints` 加 "prev[0] is a VIP → 派生继承 VIP 错端口" 表 case（文档化混合集模板 hazard，durable 自定义须放 InviteSeeds）。
- **m10（采纳·修）** — helper 把 `Seeds()`+`readRosterBrokers()` 读进**单个 `BoundedStaleRead` 快照**（一致 generation，消除并发 membership Apply 夹在两读之间的虚假 hands-off）。
- **m11（采纳·修）** — `DeriveSeedEndpoints` 排除 `RosterPhaseAddFailed`（failed-join 节点从不广告 client 端点）；表 case 钉。
- **m12（采纳·修）** — responder 注释 + `fetchManifestOverNATS` docstring 准确区分"未订阅=快 ErrNoResponders" vs "订阅但 unsigned=timeout"；测试改名 `TestG3ClusterRosterPullUnsignedIsSilentTimeout` + 断言 `errors.Is(err, nats.ErrTimeout)`。（未 Respond 空哨兵：broadcast 下会让 ctl 取错空 body 而错过有效 broker。）
- **m13（采纳·回改 plan）** — 认可 `DeriveSeedEndpoints`/`seedSetEqual`/`seedHostsMatchAnyBroker` 落 `internal/broker/seed_converge.go`（非 plan 原定 cluster 包）：唯一调用者是 `broker.ClusterAdmin` 方法，函数依赖 `proto.RosterBroker`+`clusterroster.IsUndialableHost`（broker 已 import）；offline 复用的 `SeedEndpointsDropHosts` 仍留 cluster 包。g3-plan.md 加 Stage-C 更正注记使 plan↔代码一致。
- **m14（采纳·测试）** — `TestG3SeedGrowConvergesMultiBroker`（种第二个 VOTER roster 行 → helper → 断言 seeds 含两 broker + gen bump 一次）。
- **m15（采纳·测试）** — `TestG3SeedBackstopConvergesAndIsIdempotent`（`ReconcileMembershipOnLeadership` 收敛陈旧集 + 第二次调用 no-op 不 churn）。
- **m16（采纳·测试）** — dedupe（重复 public_host 折叠单 URL）表 case + `TestDeriveSeedEndpointsExactlyEight`（8 dialable 全保留、无 off-by-one 截断）。
- **m17（采纳·测试）** — `TestPruneRosterPeersDropsDepartedSeeds` 加 `rowCount==0` 断言（一次 pruneRosterPeers 同时删 roster 行 + drop seed，证同 tx 原子）。
- **m18（采纳·修）** — `DeriveSeedEndpoints` 用 prev 中**第一个可解析的 tls/wss** 项作模板（robust to garbage/nats:// prev[0] 否则空集 floor 永久卡死）；nats:// 仍被跳过（不传播明文 PIN 载体）；表 case 钉 garbage/nats:// fall-through。

### NIT
- **n21（采纳·修）** — bare IPv6 literal `public_host` 加括号（well-formed URL）。
- **n23（采纳·修）** — export `cluster.MaxSeedEndpoints` 为单一 SSOT，`seed_converge.go` 用它替本地双写常量。
- **n24（采纳·修）** — `MetaKeySeedBootstrap` 降回 unexported（无 cluster 包外消费者，减 API 面）。
- **n19 / n20 / n22 / info25（驳回）** — 均 by-design 或非 G3 引入：n19 drain-非-retire 不收敛（DRAINING broker 仍活可拨，签名 roster 每 register 已 de-prefer）；n20 online 拒 nats:// vs offline drop-only 的不对称（两者都安全，documented-by-design）；n22 broadcast vs queue-group（与 cluster-health 一致 + first-reply-wins 幂等安全）；info25 既有无认证 HTTP manifest 路径（G3 边际面严格更小、需有效 nkey）。

### sim drill（deploy-tier follow-up）
G3 触碰 grow/force-single 生命周期但**不改部署栈**（install.sh / nats.conf 渲染 / systemd unit 均未动）。核心逻辑已由 hermetic 套件充分覆盖（纯函数 + broker helper 多-broker grow + force-single 收敛 + offline drop-only + responder + ctl 改法二/门A）。真集群端到端 seeds 收敛 + ctl failover 刷新验证列为 **deploy-tier follow-up**（CLAUDE.md §5「sim drill 按需运行」；plan §3 的 A/B/C/D 场景待 deploy-tier 门跑）。

### 提交前硬闸（修复后）
`make lint` = 0 · `make test` 全包绿 · `make e2e` 全矩阵（唯一 flake `TestProxyFalseOnlineRecoversAfterTunnelDrop` 已证非 G3：G3 未碰 proxy/tunnel，单测+全包 2 次均绿）· 触碰 ctl-refresh/responder 并发面另过 `-race` + 内建泄漏门。
