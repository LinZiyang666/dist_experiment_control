# L02 — cluster / nats 系包边界与重叠（结构性质量审计）

> Lane key: `cluster-package-boundaries`
> 范围：`internal/{cluster, clusteroffline, clusterroster, clusterspec, clusterupgrade, clusternodes, clustermanifest, natscluster, natsconf, natsreconcile, serveconf}` — 11 包 / 11,166 行生产代码
> 只读审计。本文不找 bug（那是 `01`–`06` 的活），找的是冗余、重复、抽象错位、演进阻力。

---

## 结论

**这 11 个包不臃肿，也没有一个是"为切而切"。真正的债不在包的数量上，而在两处「同一不变量被实现两遍」和一处「本该有的抽象没建」——其中 force-single 的 online/offline 双实现已经造成过一次真实产线事故（racknerd JetStream 503 烂了 5 天）。**

净判断，逐条可反驳：

1. **反对"过度切分"的指控**。4 个 <250 行的小包（`clustermanifest` 78 / `clusternodes` 134 / `clusterupgrade` 171 / `natscluster` 202）**全部**是有理由的纯叶子：前三个 internal import 数为 **0**，`natscluster` 只 import `auth`。它们存在的原因（L-2 raft 隔离、纯 net/http 叶子、把决策核心从胖 orchestrator 里抠出来做表驱动测试）在代码里都能被验证，不是包名幻觉。
2. **11 个包之间只有 5 条内部依赖边，无环，无"为绕环而生"的畸形依赖**。这在一个 6.8 万行的分布式系统里是不错的成绩。
3. **真正的结构债是"抽象不足"，不是"抽象过度"**：`nats.conf` 的换装管线（Preflight→BuildMergedConf→DryRun→Apply→re-Preflight）被手抄了 **5 遍**，每遍自己拼一个 `natscluster.Config{}`；`cluster_nodes` 这张全域最承重的表有 **21 个非测试文件**在写裸 SQL，而项目里明明有个叫 `clusternodes`、职责就是"cluster_nodes 的读侧"的包，里面只有 3 条查询。
4. **online/offline 双轨是最深的一处债**。force-single 的同一不变式（raft={self} / roster={self} / gen 单调 / seeds 收敛 / force_single_active / nats.conf 脱簇）在 `clusteroffline` 里是**一个事务 + 自动脱簇**，在 `broker/force_single_online.go` 里是**四个互不原子的 raft 提案 + 打印一段让运维手抄的 remedy 文案**。两条路径的"完成度"不同，而且编译器看不见这个差异。
5. **量化**：可净删约 **560 行**（占本 lane 5%），包数可从 **11 → 8**。这不是一个"删掉一半"的故事——**75% 的行是问题域强加的本质复杂度**。

**Bloat score（本 lane）：4/10**。理由：体量正当（一个要同时干 raft HA + 接管别人写的 nats.conf + 停机磁盘手术 + 签名发现材料的控制面，1.1 万行不算多），注释密度高但绝大多数注释在记载"为什么"（某个 external review finding、某次事故），不是废话；真正的减重空间只有 5%。扣分全部来自双轨实现和缺失抽象带来的**演进阻力**，不是行数。

---

## 范围与方法

| 手段 | 说明 |
|---|---|
| `go list -f '{{.Imports}}'` | 得出 11 包的真实 internal 依赖图（不靠猜包名） |
| `grep -rl "tether/internal/<pkg>\""` | 反向依赖（谁 import 谁、几个文件） |
| `go/ast` 一次性脚本 | 精确统计每包导出面（type/func/method/const/var），比 `go doc` 行数计数可靠 |
| 逐文件通读 | `natscluster/config.go`、`natsconf/{preflight,takeover,remedy,js_store}.go`、`natsreconcile/reconcile.go`、`clusteroffline/{offline,backup,doctor,manifest}.go`、`cluster/{offline,membership_ops,node,seeds}.go`、`clusterroster/*`、`clusterspec/spec.go`、`clusterupgrade/plan.go`、`serveconf/serveconf.go` 全部真读进去 |
| 交叉比对 | 把 online 路径（`internal/broker/{force_single_online,clusterbackup,cluster_grow_cutover,topology_reconcile}.go`）和 offline 路径逐段对齐 |
| `go build ./...` | 仅确认树是干净的（PASS）。**未跑任何测试**（12 agent 并发，按 lane 纪律） |

统计口径：生产代码 = `*.go` 且非 `*_test.go`。

### 每包一句话真实职责 + 导出面

真实职责是**读代码得出**的，不是读包 doc 抄的（有两处 doc 已与现实脱节，见下表备注）。

| 包 | 生产行 | 导出面（type/func/method/const/var） | 一句话真实职责 |
|---|---:|---|---|
| `cluster` | 5300 | 16 / 84 / 49 / 84 / 10 = **243** | Raft 引擎 + FSM + 快照/恢复 + mTLS transport + `Command`（全仓 SQL-bake 通货）+ **cluster 自己那批领域 op 的 Plan/read** + 离线 raft-store 手术原语。**5 件事挤一个包** |
| `clusteroffline` | 2959 | 19 / 29 / 0 / 10 / 3 = **61** | 名为"离线"，实为 **cluster 数据面的杂物间**：force-single/recover 编排 + backup/restore + `init --from-existing` + manifest 读写 + doctor + secrets preflight + journal + peer 探活。**其中 6 个导出被 online broker 直接 import**（包名撒谎） |
| `clusterroster` | 708 | 1 / 18 / 0 / 0 / 6 = **25** | 账号签名的**发现材料**族：roster / SeedBundle 的规范字节+签+验、invite token 铸/解、well-known manifest 抓取、客户端 dial 串拼装。broker(签)/agent(验)/ctl(拼) 三方共享的纯叶子 |
| `natsconf` | 820 | 3 / 7 / 8 / 8 / 0 = **26** | 接管 install.sh 写的 `nats.conf`：真 nats lexer 解析 → 五桶归属分类（fail-closed）→ 合并渲染 → `-t` dry-run → 原子换 + `.bak`；外加 JS store 移开与"脱簇 remedy 文案 SSOT" |
| `serveconf` | 328 | 10 / 1 / 5 / 1 / 0 = **17** | `broker.yaml` 的 Go 投影 + 5 个 duration 校验器（每个带上下限和事故理由）。**与 nats.conf 渲染链无关**（只携带路径） |
| `natsreconcile` | 245 | 2 / 2 / 0 / 7 / 0 = **11** | 单 broker 把本机 nats.conf 收敛到复制态 desired topology 的**纯步进机**，reload/probe 是注入 seam |
| `clusterspec` | 221 | 4 / 2 / 0 / 0 / 0 = **6** | `roster.yaml`（运维意图）解析 + 与 live roster 求差，渲染成 quorum-safe 的**打印版**收敛计划（只印不执行） |
| `natscluster` | 202 | 2 / 1 / 0 / 0 / 0 = **3** | 一个纯函数 `Render(Config) (string, error)`：把集群身份渲染成 nats-server.conf 正文 |
| `clusterupgrade` | 171 | 5 / 1 / 2 / 5 / 0 = **13** | 滚动升级的**纯排序/拒绝逻辑**（followers-first / leader-last / 三态 agent 存在性），零 IO |
| `clusternodes` | 134 | 2 / 3 / 1 / 1 / 1 = **8** | `cluster_nodes` 的**纯 SQL 读投影**（home 解析 + 拓扑 peer 列表），不 import raft/nats。**doc 里承诺的"D7 的写侧稍后搬来"从未发生** |
| `clustermanifest` | 78 | 0 / 3 / 0 / 1 / 0 = **4** | loopback-only HTTP 服务一个已签名的发现 manifest，纯 `net/http` 叶子 |

备注两处 doc 脱节：
- `clusteroffline` 的包注释说"CLI 包装这些"，但实际上 `internal/broker` 的三个 online 文件也在 import 它（`clusterbackup.go`、`force_single_online.go`、`cutover.go`）。
- `clusternodes/read.go:9-11` 写着"D7's ClusterNodeUpsert writer will co-locate here later"——D7 早已完成，写侧落在了 `internal/cluster/membership_ops.go`，这句承诺成了化石。

### 导入关系图（本 lane 11 包，实测）

```
                     ┌──────────┐
   auth,proto,storage│ cluster  │◄──────────────┐
        ─────────────►│  5300   │               │
                     └──────────┘               │
                                                │
              ┌────────────────┐                │
  auth,proto  │ clusterroster  │   (无 lane 内边)│
        ──────►│     708      │                 │
              └────────────────┘                │
                                                │
   proto  ┌─────────────┐                       │
   ───────►│ clusterspec │  (无 lane 内边)       │
          │    221      │                       │
          └─────────────┘                       │
                                                │
  ┌──────────────┐                              │
  │clusterupgrade│ 171   ⟵ internal import 数 = 0 │
  ├──────────────┤                              │
  │ clusternodes │ 134   ⟵ internal import 数 = 0 │
  ├──────────────┤                              │
  │clustermanifest│ 78   ⟵ internal import 数 = 0 │
  ├──────────────┤                              │
  │  serveconf   │ 328   ⟵ internal import 数 = 0 │
  └──────────────┘                              │
                                                │
  auth   ┌─────────────┐                        │
  ───────►│ natscluster │                       │
         │    202      │◄────┐                  │
         └─────────────┘     │                  │
                ▲            │                  │
                │            │                  │
         ┌─────────────┐     │                  │
         │  natsconf   │─────┘                  │
         │    820      │◄────────────────────────┤
         └─────────────┘     ▲                  │
                ▲            │                  │
                │      ┌───────────────┐        │
                └──────│ natsreconcile │        │
                       │     245       │        │
                       └───────────────┘        │
                                                │
         ┌────────────────┐                     │
         │ clusteroffline │─────────────────────┘
         │     2959       │──► natsconf, proto, storage, tunnel
         └────────────────┘
```

**lane 内共 5 条边**：`clusteroffline→cluster`、`clusteroffline→natsconf`、`natsconf→natscluster`、`natsreconcile→natscluster`、`natsreconcile→natsconf`。**无环，无循环规避性畸形依赖**（没有那种"为了不成环而把一个类型搬到第三个包"的痕迹）。

只被一处 import 的包：`clusterspec`（仅 `cmd/tether` 1 文件）、`clusterupgrade`（仅 `cmd/tether` 5 文件）、`clustermanifest`（`cmd/tether` + `broker` 各 1）、`natsreconcile`（`broker` 2 文件）。见 §F7 的判定——**单一消费者不等于该合并**。

---

## Findings

### F1 — [high] `nats.conf` 换装管线被手抄 5 遍，每遍自己拼一个 `natscluster.Config{}`

**证据**（5 个独立的 `natscluster.Config{}` 装配点 + 5 条 Preflight→Build→DryRun→Apply 序列）：

| # | 装配点 | 管线 | 该处独有的决策 |
|---|---|---|---|
| 1 | `internal/natsreconcile/reconcile.go:117` | `:96 Preflight` → `:144 Build` → `:174 DryRun` → `:190 Apply` | `MonitorListen` 留空（靠 `BuildMergedConf` harvest）；secrets 三件套 `:137-139` |
| 2 | `internal/broker/cluster_grow_cutover.go:256` | `:56 Preflight` → `:273 Build` → `:127 DryRun` → `:146 Apply` | `MonitorListen: topoMonitorListen`；secrets 三件套 `:268-270` |
| 3 | `cmd/tether/cluster_natsconf.go:359`（manual takeover） | `:295 Preflight` → `:386 Build` → `:401/412 DryRun` → `:415 Apply` | `MonitorListen: "127.0.0.1:8223"` 硬编码 `:373`；secrets 三件套 `:374-376` |
| 4 | `cmd/tether/cluster_natsconf.go:199`（`--to-standalone`） | `:101 Preflight` → `:208 Build` → `:216/231 DryRun` → `:247 Apply` → `:252 re-Preflight` | `MonitorListen: own.MonitorHTTP()` `:206` |
| 5 | `cmd/tether/cluster_offline.go:96`（offline 脱簇） | `:72 Preflight` → `:105 Build` → `:125 DryRun` → `:128 Apply` → `:131 re-Preflight` | `MonitorListen: own.MonitorHTTP()` `:103` |

同一个 monitor 地址 `127.0.0.1:8223` 有 **3 个来源**：`internal/broker/topology_reconcile.go:30` 的常量、`cmd/tether/cluster_natsconf.go:373` 的字面量、以及"harvest 现有值"。代码里的注释自己承认了这个耦合：

- `cmd/tether/cluster_natsconf.go:372` — "Keep this addr in sync with the broker's topoMonitorListen."
- `internal/broker/cluster_grow_cutover.go:266` — "Every other restart-bearing takeover forces 8223 too (cluster_natsconf.go)."
- `internal/broker/cluster_grow_cutover.go:250-252` — "so the applied conf is **byte-identical** to the one the reconciler DryRun-validated then withheld."

同理，"jetstream 开着但没有显式 store_dir 就拒绝渲染"这条 fail-closed 规则被独立写了 **3 遍**（`cmd/tether/cluster_natsconf.go:141`、`:384`、`cmd/tether/cluster_offline.go:81`），错误串各不相同。

**为什么是债（它让什么未来改动变难/变危险）**：
`natscluster.Config` 每加一个字段（比如未来要在 conf 里加 leafnode、加 `no_advertise`、改 JS domain 策略），都必须**同时改 5 个装配点**，而其中 3 个在 `cmd/tether`、1 个在 `internal/broker`、1 个在 `internal/natsreconcile`——跨 3 个包，没有任何编译期机制强制它们保持一致。漏改任何一个的后果不是编译错误，而是**两条路径渲染出不同的 conf**：reconciler 校验通过并 withhold 的那份，和 grow cutover 真正 apply 的那份不再字节相同，SIGHUP 与重启会看到不同配置。`natsreconcile.SynthesizeClusterListen` 被导出（`reconcile.go:222`）的唯一理由就是这个——注释直说"Exported so the grow cutover (which APPLIES the swap the reconciler WITHHELD) renders the identical listen"。**一个函数因为跨包同步需求而被迫导出，就是包边界画错了的直接证据。**

**建议**：在 `natsconf` 里建一个 `SwapIntent` + `natsconf.Swap(intent) (Outcome, error)`，把 5 处共有的骨架（Preflight → 装配 Config → Build → DryRun → Apply → re-Preflight 断言）收成一份。5 个调用点各自只提供真正不同的那部分意图（`Mode: Clustered|Standalone`、`Peers`、`SecretsDir`、`Restartability: SIGHUP|FullRestart`、`Plan bool`）。secrets 三件套文件名和 monitor listen 成为 `natsconf` 的包级常量。

**量化**：净减约 **180 行**；`natscluster.Config` 的装配点从 5 → 1；secrets 路径 join 从 3 → 1；store_dir fail-closed 从 3 → 1。
**改动风险**：medium。不碰 wire；碰的是"渲染出的 conf 字节"，必须靠现有 golden test + deploy-tier drill（`test/simcluster` 的 grow/shrink/upgrade 那几个）把关。

---

### F2 — [critical] force-single 的同一不变量被实现两遍，且 online 那遍不完整——这是 racknerd 事故的结构成因

**证据**：

offline 路径（`cmd/tether/cluster_offline.go` + `internal/clusteroffline`）：

```
cluster.RecoverSingleNode                                  # raft config → {self}
internal/clusteroffline/offline.go:560 raiseForceSingleMarker   # 直写 SQL
internal/clusteroffline/offline.go:581 pruneRosterPeers         # 一个事务：DELETE peers
                                                                #  + roster/topology gen 单调 bump
internal/clusteroffline/offline.go:643 convergeSeedsDropHosts   #  + seeds drop-only（同一事务，含空集地板）
cmd/tether/cluster_offline.go:214 deClusterStandaloneConf       # ✅ 自动重渲 nats.conf 为 standalone
cmd/tether/cluster_offline.go:244 resetForceSingleJSStore       # ✅ 自动移开 clustered JS store
cmd/tether/cluster_offline.go:249 ClearForceSingleJournal
```

online 路径（`internal/broker/force_single_online.go:227-306`）：

```
node.RecoverToSelfOnline                                   # raft config → {self}
:263 Propose(cluster.PlanSetForceSingle)                   # 提案 1
:279 Propose(cluster.PlanForceSingleEpoch)                 # 提案 2
:295 Propose(cluster.PlanClusterNodePrune)                 # 提案 3（best-effort，失败只 Warn）
:300 admin.deriveAndConvergeSeedsFromRoster()              # 提案 4（语义与 offline 的 drop-only 不同：这是 re-derive）
                                     ❌ 没有任何 nats.conf 步骤
cmd/tether/cluster_offline.go:366 打印 natsconf.DeClusterRemedyCmd  # 让运维手抄一条命令
```

差异清单：

| 维度 | offline | online |
|---|---|---|
| roster prune + gen bump + seeds | **1 个 SQL 事务**（`pruneRosterPeers`） | **2 个独立 raft 提案**，中间崩溃留半态 |
| prune 失败 | 硬失败 | `:301` 仅 `logger.Warn`，留 ghost 节点 |
| seeds 语义 | drop-only + 空集地板（`convergeSeedsDropHosts`） | 从 roster **re-derive**（`seed_converge.go:160`） |
| nats.conf 脱簇 | **产品自动完成** | **打印文案让人手抄** |
| JS store 重置 | **产品自动完成** | 无 |

`internal/clusteroffline/offline.go:578` 和 `:613` 的注释自己承认这是双写：*"the offline (daemon-down) equivalent of `cluster.PlanClusterNodePrune`, so the online and offline force-single paths leave the SAME cluster_nodes state"*、*"parity with the online rosterGenBumpStmt/topologyGenBumpStmt"*。**"parity"是靠注释和人的记性维持的，编译器一无所知。**

**为什么是债**：这不是理论风险。`docs/reviews/quality-audit` 之外有一次真实事故：online force-single 把 raft 收成 N=1，但没动 `nats.conf`——那份 conf 仍然是 clustered 的，孤零零一个 voter 永远凑不齐 clustered JS meta 的 quorum-of-2，**JetStream 503 烂了 5 天**，最后靠人工脱簇修复。`natsconf/remedy.go` 整个文件（47 行）就是这次事故的产物——它把"该说的话"抽成 SSOT，供三处（boot FATAL / 状态横幅 / `recovery restore` 收尾）共享。但 `remedy.go:6-12` 的文件注释精准地描述了问题却开错了药：*"the product knew exactly what to say and only said it too late"*——真正的问题不是"说得晚"，是**产品在 offline 路径上能自动做完的事，在 online 路径上只肯说不肯做**。按 `test/simcluster` 的定位铁律④，这正是"一次操作若要靠人工绕过才能成功，那是产品的失败"。

未来任何一次对 force-single 语义的修改（加一个 phase、改 seeds 收敛规则、加一个 marker）都必须记得改两个地方，且两处的原子性边界不同（1 事务 vs 4 提案），很容易改出"online 半态"。

**建议**：
1. 把 force-single 的**收尾契约**（roster prune → gen bump → seeds converge → nats.conf 脱簇 → JS store 重置）抽成一份声明式的 step list，online/offline 各自只提供"怎么落地一步"（raft Propose vs 直写 SQL）。
2. 立即把 online 分支补上 nats.conf 脱簇 + JS reset（哪怕仍以 `--confirm` gate 二次确认），让"产品自动完成"成为两条路径的共同承诺。
3. online 的 prune + seeds 收敛应合成**一条**命令（一次 Apply 里 bake 两组语句），消掉半态窗口。

**量化**：净减约 **100 行**（合并第 3/4 提案 + 共享收尾契约）；更重要的是把"两套完成度"收敛成一套。
**改动风险**：high。触碰灾难恢复路径与 raft 提案原子性，必须过 `test/simcluster` 的 force-single / de-cluster drill。

---

### F3 — [high] `clusteroffline` 名不副实：一个被 online 路径依赖的 7 职责杂物间

**证据**：包名和 doc 都写着"runs with the daemon STOPPED"（`internal/clusteroffline/offline.go:1-8`），但 `internal/broker`（在线守护进程）import 了它的 **6 个导出**：

- `internal/broker/force_single_online.go:196,201,240,249` — `ReadRoster` / `ProbePeers` / `CheckPeersDead` / `Peer` / `PeerLiveness`
- `internal/broker/clusterbackup.go:80,90,96` — `ReadSelfIdentity` / `ProjectRoster` / `WriteManifest` / `ManifestKindBackup` / `ManifestModeCluster`
- `internal/broker/cutover.go` — `TunnelCertFingerprint` / `SecretsPreflightOptions`
- 另有 `DoctorCheck` / `DoctorOptions` / `DoctorSummary` 的在线 doctor 复用

包内 10 个生产文件覆盖 **7 个不同关注点**：`offline.go`(965, force-single/recover/probe/dump) · `restore.go`(455) · `init.go`(421) · `manifest.go`(254) · `doctor.go`(227) · `journal.go`(208) · `backup.go`(189) · `preflight.go`(117) · `diagnose.go`(68) · `bundle_scope.go`(55)。共 61 个导出符号。

包内还有一层无谓的可见性摆动：`checkPeersDead`(`:447`) 和 `CheckPeersDead`(`:556`) 是**同一函数的两个名字**（`:557` 就是 `return checkPeersDead(...)`），`readRoster`(`:515`) / `ReadRoster`(`:528`) 同理——都是"本来私有，后来 online 要用，于是加个导出壳"留下的年轮。

**为什么是债**：一个 `internal/broker` 的作者要复用 roster 读取，必须 import 一个名字宣告"仅限守护进程停机时使用"的包。这有两个具体后果：(a) 新代码不会去那里找，于是又写一遍裸 SQL（见 F5，21 个文件）；(b) 任何人想给 `clusteroffline` 加一条"因为是离线所以可以直写 DB"的假设，都会静默地把这个假设强加给三个在线调用点。

**建议**：三分。
- `clusterbundle`（新）：`manifest.go` + `backup.go` + `restore.go` + `bundle_scope.go`（≈953 行）——bundle 的**唯一**读写者，online/offline 共用（顺带解决 F4）。
- `clusterdiag`（新）：`doctor.go` + `preflight.go` + `diagnose.go`（≈412 行）——纯只读体检，online/offline 共用。
- `clusteroffline`（留）：`offline.go` + `init.go` + `journal.go`（≈1594 行）——真·停机磁盘手术，包名重新变成真话。

**量化**：包内导出面从 61 降到 ≈30/包；消掉 2 对导出壳（≈15 行）；三个新包的 doc 契约不再自相矛盾。
**改动风险**：low（纯搬家 + 改 import，无行为变化）。

---

### F4 — [medium] online backup handler 重抄了 `OfflineBackup` 的 bundle 管线，还得靠一个复制的时间格式常量维持"字节一致"

**证据**：`internal/broker/clusterbackup.go:47-100` vs `internal/clusteroffline/backup.go:60-110`。两段代码同序做同一串事：

| 步骤 | offline (`backup.go`) | online (`clusterbackup.go`) |
|---|---|---|
| 创建 bundle 目录 O_EXCL | `:71-76` | `:48-53` |
| 失败清理 defer | `:78-82` | `:54-59` |
| 拷 state.db | `:87` `cluster.BackupDBFile` | `:63` `node.BackupDBTo` |
| 校验完整性 | `:91` `cluster.VerifyIntegrity` | `:66` `cluster.VerifyIntegrity` |
| **从拷贝**（非 live handle）建 manifest | `:96 buildBackupManifest` | `:73-91` 手写同样的逻辑 |
| 写 manifest | `:99 WriteManifest` | `:94 WriteManifest` |

最能说明问题的是 `internal/broker/clusterbackup.go:116-118`：

```go
// timeRFC3339Nano mirrors time.RFC3339Nano without importing time at the file top (the layout is
// a const so the manifest timestamps match the offline path byte-for-byte).
const timeRFC3339Nano = "2006-01-02T15:04:05.999999999Z07:00"
```

**为了让两条路径产出字节一致的 manifest，标准库常量被手抄了一份。** 而且两边已经悄悄分叉了：offline 的 `buildBackupManifest` 会**探测** `ManifestModeSingleBroker`（`backup.go:120-125`），online 直接 `m.Mode = ManifestModeCluster` 硬写（`clusterbackup.go:84`）。

**为什么是债**：bundle 格式是**灾难恢复的输入**——`RestoreFromBackup` 的 provenance gate 会读 manifest 的 `self_cert_fp` / `applied_index` / `schema_version`。给 manifest 加一个字段（比如未来要记 JetStream 备份指针，`JetStreamBackupScopeWarning` 已经预告了这个缺口）就必须同时改两个 writer，而 restore 只有一个 reader——写者漏改一个，产出的 bundle 在 restore 时行为不一致，且只有在真出事的那天才会被发现。

**建议**：把 `OfflineBackup` 的骨架参数化成 `clusterbundle.Write(src BackupSource, opts)`，`BackupSource` 是个两方法接口（`CopyTo(path) error` / `Provenance() (role, leaderID string)`）。offline 传一个文件源，broker 传 `node`。

**量化**：净减约 **55 行**；manifest writer 从 2 → 1；消掉 `timeRFC3339Nano` 复制常量。
**改动风险**：low-medium。产出字节必须不变——现有 `restore_test.go` 的 round-trip 测试是天然回归网。

---

### F5 — [medium] `cluster_nodes` 这张最承重的表，裸 SQL 散在 21 个非测试文件 / 6 个包里，而专门的读侧包只装了 3 条查询

**证据**：`grep -rlE '(SELECT|UPDATE|DELETE FROM|INSERT INTO)[^;]*cluster_nodes'` 命中 22 个非测试文件（其中 `internal/adminsock/protocol.go` 是注释误命中，实为 **21**）：

```
internal/cluster/membership_ops.go        (17 处 — 写侧权威)
internal/clusternodes/read.go             (11 处 — 名义上的读侧，只有 3 条查询)
internal/clusteroffline/{offline,manifest,init,restore}.go
internal/broker/{clusterdrain,cutover,cluster_operation_controller,clusterstatus,
                 force_single_online,clusteradmin,cluster_grow_trigger,cluster_health,
                 metrics_wire,observability,proxy_rebalance,proxy_reconcile,
                 topology_reconcile}.go
cmd/tether/{cluster,cluster_offline}.go
```

同一条谓词在多处手写：`WHERE phase='VOTER'` 出现在 `broker/proxy_reconcile.go:318`、`broker/proxy_rebalance.go:203`、`broker/observability.go:285`、`broker/clusterdrain.go:761`、`subhttp/subhttp.go:204`——而 `clusternodes.PhaseVoter`（`read.go:27`）和 `proto.RosterPhaseVoter`（`cluster_roster.go:31`）是这个字符串的**另外两个 SSOT**。三份真相。

`clusternodes` 包的 doc（`read.go:9-11`）本来给出了正解——"D7's ClusterNodeUpsert writer will co-locate here later"——**但那次搬家从未发生**，写侧最终落在 `internal/cluster/membership_ops.go`（因为写侧要造 `*cluster.Command`，而 `clusternodes` 不能 import `cluster`，L-2）。于是这张表的四个家：写侧在 `cluster`、离线写侧在 `clusteroffline`、读投影在 `clusternodes`、临时读散在 `broker`。

**为什么是债**：`cluster_nodes` 加一列（migration 0008 之后已经加过 `bus_nkey_pub`、`nats_route`、`cert_fp_prev`…）时，"谁需要跟着改"没有任何机械答案——要靠人 grep 21 个文件。`clusterstatus.go:137` 那条 `SELECT node_id, name, phase, cert_fp, cert_fp_prev, cert_fp_valid_until` 和 `clusternodes.selectHomeNode` 的列集重叠但不相同，两边各自演化。**这就是 F2 里"两条路径的 roster 状态是否一致"没法被证明的根因**。

**建议**：把 `clusternodes` 兑现成它 doc 里承诺的东西——`cluster_nodes` 的**唯一投影层**（读），提供 `HomeNode` / `TopoPeer` / `RosterRow` / `VoterIDs()` 等具名投影，把 21 个文件里的裸查询收编。写侧留在 `cluster`（L-2 强制），但读侧应当只有一个入口。`PhaseVoter` 与 `proto.RosterPhaseVoter` 二选一。

**量化**：约 **120 行**重复投影可收编；`cluster_nodes` 裸 SQL 文件数 21 → ≤6；"VOTER" SSOT 3 → 1。
**改动风险**：low（纯读，无行为改变），但涉及面广，建议分批。

---

### F6 — [medium] `internal/cluster`：5300 行 / 243 个导出符号 / 5 个职责挤在一个包里

**证据**（按文件归类，行数为生产行）：

| 职责 | 文件 | 行 |
|---|---|---:|
| A. Raft 引擎 + FSM + 快照 + transport | `node.go`(569) `fsm.go`(409) `snapshot.go`(282) `read.go`(231) `transport.go`(137) `membership.go`(119) `production.go`(99) `liveness.go`(26) | ≈1872 |
| B. Command / SQL-bake 通货 | `command.go`(377) `sqlbake.go`(104) `planapply.go`(27) `util.go`(10) | ≈518 |
| C. cluster 自己的领域 op（Plan + read，**不需要 raft**） | `membership_ops.go`(634) `lock_lease.go`(288) `seeds.go`(177) `operation_ops.go`(170) `operation_read.go`(147) `clustermeta.go`(138) `alert_ops.go`(127) `alert_read.go`(114) `rostergen.go`(69) `auditcursor.go`(52) `topologygen.go`(45) | ≈1961 |
| D. 离线磁盘手术 | `offline.go`(526) `datadirlock.go`(122) `exchangedir_*.go`(57) | ≈705 |
| E. 杂项 | `join_bundle.go`(109) `route_san.go`(91) `sqlbake` 之外的 `doc.go`/`testhooks.go` | ≈244 |

C 组共 1961 行 / 25 个 `Plan*` 函数，**一行 raft 都不需要**——它们只是把参数烤成 SQL 字面量塞进 `*Command`。证据：仓库里已经有 7 个别的包（`node/plan.go`、`port/plan.go`、`session/plan.go`、`proc/plan.go`、`proxysub/plan.go`、`xferaudit/plan.go`、`agentprov/plan.go`）就是这么干的——它们 import `cluster` 拿 `Command`，Plan 逻辑住在自己包里。**cluster 自己的 op 只是因为"它是 cluster 的 op"就留在了引擎包里，没有技术理由。**

**为什么是债**：`internal/broker` 有 76 个文件 import `internal/cluster`。它们中的绝大多数只想要 C 组（`PlanClusterNodePhase` / `ActiveAlerts` / `NonTerminalOperations` / `Seeds`）或 B 组（`Command`），却不得不把整个 raft 引擎、bolt store、FileSnapshotStore 拖进编译单元和心智模型里。反过来，**`internal/cluster` 的 API 表面是 243 个符号**——想理解"raft 引擎给外界的契约是什么"的人，要先从 243 个里筛掉 130 个属于领域 op 的。`test/determinism` 的 raft-confinement lint（`lint_skeleton_test.go:119-146`）只约束 raft **不能出去**，从不约束"非 raft 的东西不能进来"，所以这个包只会继续长。

**建议**：抽出 `clusterstate`（C 组 + B 组的 read 侧 + `clusternodes` 折入），保留 `cluster` = A+B(Command 类型)+D+E ≈ 3340 行。`clusterstate` import `cluster` 拿 `Command`（单向，无环，和现有 7 个 `*/plan.go` 完全同构）。

**量化**：净行数变化 ≈ 0（这是拆不是删）；`internal/cluster` 导出面 243 → ≈100；76 个 broker 文件中约一半可以改成只依赖 `clusterstate`；包数 11 → 11（但 F7 的合并会把总数拉到 8）。
**改动风险**：medium。纯搬家，但 76 个 import 点要动，且 `test/determinism` 的白名单断言（`lint_skeleton_test.go:167-175` 要求 `internal/cluster` 下必须存在 raft import）需要复核。**优先级低于 F1–F5**——这条是"结构更正确"，不是"当前正在流血"。

---

### F7 — [low] 4 个小包**不是**过度切分：逐个判定

lane brief 点名要求逐个判定，结论是**全部正当**，证据如下。

| 包 | 判定 | 证据 |
|---|---|---|
| `clustermanifest` (78) | **正当** | internal import 数 = **0**（只用 `net/http`）。它服务的是一个**未认证**的 loopback endpoint（`manifest.go:18-33` 硬拒非 loopback 地址）；把它塞进 `broker` 会让"这段代码绝对不能碰 DB/NATS/seed 材料"这个安全属性从**包边界保证**降级为**注释保证**。`Handler()` 被单独抽出正是为了 `httptest` |
| `clusternodes` (134) | **正当（但未兑现，见 F5）** | internal import 数 = **0**。存在理由写在 `read.go:6-12`：home 解析路径必须能在**不拖 raft/nats** 的前提下工作（L-2）。8 个 broker 文件 import 它。**问题不是它多余，是它太小**——它该收编的 21 个文件的裸 SQL 没收编 |
| `clusterupgrade` (171) | **正当** | internal import 数 = **0**，纯 `sort`。它是从 `cmd/tether/cluster_upgrade_drive.go`(451) 这个胖 orchestrator 里抠出来的**决策核心**，因此能在没有 raft/systemd harness 的情况下做对抗性表驱动测试（`external_review_test.go` + 317 行测试 / 171 行生产）。`AgentPresence` 三态（`plan.go:22-44`）的设计说明写得很清楚：两态会让 agentless 主机永远 `AtTarget=false`。**这是"从胖 orchestrator 抠纯核心"的教科书用法，应该表扬而不是合并** |
| `natscluster` (202) | **建议合并（但理由不是"太小"）** | 唯一非测试消费者是 `natsconf`(4 文件) / `natsreconcile`(4 文件) / `broker`(3) / `cmd`(3)。它的整个 API 是 `Render(Config)` 一个函数 + 2 个类型。合并进 `natsconf` 的理由是 **F1**：`Config` 的装配需要和 Preflight harvest 的结果协同（`BuildMergedConf` 已经在替调用者补 `CAFile`/`ClusterListen`/`MonitorListen`——`takeover.go:63-84`），把渲染器和归属分类器分在两个包，正是 5 处手抄 `Config{}` 的结构诱因 |

另外，`clusterspec`(221) 与 `clusterupgrade` 同理正当（纯 differ，只印不执行，`spec.go:1-13` 明确写了它与 `clusterroster` 的概念区分——那次命名碰撞是被 Stage-A 评审抓住的，说明团队对包边界是有意识的）。

**建议**：只合 `natscluster` + `natsreconcile` → `natsconf`（配合 F1），其余四个保持独立。
**量化**：包数 −2；F1 的修复因此不需要跨包（否则 `SwapIntent` 会成为第四个跨包同步点）。
**改动风险**：low。

---

### F8 — [low] `clusterroster`：roster 与 seeds 两条平行继承线，≈90 行结构性重复

**证据**：`roster.go` 和 `seeds.go` 是逐函数镜像的两族：

| roster 族 | seeds 族 | 差异 |
|---|---|---|
| `CanonicalRosterBytes` `roster.go:43` | `CanonicalSeedBytes` `seeds.go:23` | 域前缀字符串 + 字段顺序 |
| `Build` `roster.go:65` | `BuildSeeds` `seeds.go:45` | 排序 vs 保序 |
| `Verify` `roster.go:96` | `VerifySeeds` `seeds.go:71` | **逻辑逐行相同**（pin 比对 → hex decode → `auth.VerifySignature`） |
| `VerifyAt` `roster.go:126` | `VerifySeedsAt` `seeds.go:89` | **逻辑逐行相同**（schema 上界 + `expires_at` 过期） |
| `DialURLs` `roster.go:180` | `SeedDialURLs` `seeds.go:110` | 前者做 host 重模板化，后者直通 |

`Verify`/`VerifyAt` 与 `VerifySeeds`/`VerifySeedsAt` 加起来 ≈70 行，除了类型名和常量名之外**没有一处实质差异**。加上两族的 sign 路径重复，共 ≈90 行。

同时 `Select`（`roster.go:145`）在生产与测试中**均无调用者**（0/0）；`roster.go:8-12` 的包注释解释了 `Verify`/`Select` 是给"deferred 的 agent-discovery 消费者"预留的 seam，但实际上 `VerifyAt` 已被 7 处生产代码使用，只有 `Select` 真的悬空。

**为什么是债**：加第三种签名发现材料（很可能会有——`clustermanifest` 服务的 `ClusterManifest` 目前只有 `FetchManifest` 解析、没有对应的 canonical/verify 族）会带来第三份拷贝。而 verify 的安全语义（pin 比对必须在签名校验之前、schema 上界必须拒未来版本、过期必须硬拒）是**同一套安全论证被复述 N 遍**——任何一次加固漏改一族就是一个真实的验证绕过。

**建议**：抽 `signedArtifact[T]` 的三件套 helper（`verifyPinned(accountPub, canonical []byte, sigHex string)`、`checkSchema(got, max int)`、`checkExpiry(expiresAt string, now)`），两族各自只保留 `canonicalBytes` 与字段装配。删 `Select`（`git` 是历史，不需要在树里留悬空 API）。

**量化**：净减约 **90 行**（含 `Select` 的 17 行）。
**改动风险**：low。签名字节不能变——`CanonicalRosterBytes`/`CanonicalSeedBytes` 保持逐字不动即可，重构只碰 verify 侧。

---

### F9 — [low] `RecoverSingleNode` 与 `GrowReadySnapshot`：两个导出名，同一个函数体

**证据**：`internal/cluster/offline.go:253-255` 与 `:274-276`

```go
func RecoverSingleNode(dataDir, dbPath, selfID, selfRaftAddr string, logger *slog.Logger) error {
	return recoverClusterToSelf(dataDir, dbPath, selfID, selfRaftAddr, logger)
}
...
func GrowReadySnapshot(dataDir, dbPath, selfID, selfRaftAddr string, logger *slog.Logger) error {
	return recoverClusterToSelf(dataDir, dbPath, selfID, selfRaftAddr, logger)
}
```

`:268-269` 的注释诚实地说明了：*"It is mechanically identical to RecoverSingleNode … but semantically distinct"*。

**为什么是债**：两个名字承载不同的**调用方义务**（`RecoverSingleNode` 要求调用方已 HARD-REFUSE 过存活 peer；`GrowReadySnapshot` 要求调用方处理 D5 audit-window guard），但这些义务对编译器不可见。若将来两者需要真正分叉（比如 `GrowReadySnapshot` 要跳过 log replay），改 `recoverClusterToSelf` 会同时影响另一个，且没有任何测试会因"我只想改其中一个"而变红。

**建议**：要么合并成一个 `recoverToSelf(dataDir, ..., purpose Purpose)`，要么把各自的**前置条件断言**真正写进各自的函数体（`GrowReadySnapshot` 断言 `NumVoters()==1`、`RecoverSingleNode` 断言 journal 已写），让两个名字名副其实。后者更好——它把注释里的义务变成代码。

**量化**：净行数 +10（加断言）/ −15（合并）。这条不是为了减行，是为了让语义差异可被机械检查。
**改动风险**：low。

---

## 反证：做得好的地方

按 lane brief 的要求，明确写出本 lane 内设计合理、体量正当的部分。

1. **依赖图干净得不像一个 6.8 万行的项目**。11 个包只有 5 条内部边，无环。7 个包的 internal import 数 ≤1，其中 4 个是 **0**。**没有一处"为规避循环导入而把类型搬到第三个包"的畸形**——这在同规模的 Go 项目里相当罕见。

2. **纯叶子的隔离是靠包边界强制的，不是靠注释**。`clusternodes` 不 import raft 是**编译期事实**；`natsreconcile` 不 import nats/raft/broker 是编译期事实；`test/determinism/lint_skeleton_test.go:119-146` 还额外把 raft 钉死在 `internal/cluster`，并且 `:167-175` 有个反向自检防止白名单变成空断言（*"L-2 whitelist is vacuous"*）。**这是我在本 lane 里见到的最好的一处工程判断。**

3. **`natsconf` 是本 lane 边界最正确的包**。820 行做一件事：安全地接管一份别人（install.sh / 运维手改）写的配置文件。五桶归属分类（`preflight.go:42-56`）+ 子键白名单（`:66-76`）+ fail-closed 拒未知指令 + 用真 nats-server lexer 而不是行匹配（包注释第 9-10 行专门解释了为什么行匹配会挂在带引号的 nkey subject 上）。这 820 行**没有一行是可以省的**——它保护的是"绝不静默丢弃运维手调的配置"这个承诺。

4. **`natsconf.MoveAsideJSStore` 是团队自己成功去重的证据**。`internal/broker/cluster_grow_cutover.go:279-282` 记载：*"the delicate move-aside logic (M3 backup-dir-first, m4 fail-closed ReadDir, non-empty→refuse, EACCES→loud chown hint) now lives ONCE in natsconf.MoveAsideJSStore, shared with the grow joiner reset (A1), offline force-single (A3), and reconcile-nats --to-standalone (A4)."` **5 个调用点，1 份实现。** 这说明 F1/F2 指出的重复不是能力问题，是没轮到——而且 R16 那轮已经证明这种合并是可做的。

5. **纯决策核心的抽取（`clusterupgrade` / `clusterspec` / `natsreconcile`）是正确的架构直觉**。三者都是"把一个胖 orchestrator 里最容易出错、最难在真环境里测的判断逻辑，抠成零 IO 的纯函数"。`clusterupgrade.AgentPresence` 的三态设计（`plan.go:22-44`）和 `natsreconcile` 的 6 种 `Action` 分类（`reconcile.go:22-38`）都把"失败模式"做成了一等公民（`ActionAwaitingClusteredCutover` 是"故意不动"而非"错误"，`ActionSwappedReloadPending` 是"降级但没砖"）。**这不是过度设计，这是分布式系统该有的样子。**

6. **`serveconf` 的每个校验器都带事故理由，且上下界都有**。`maxReapInterval = 24h`（`serveconf.go:186-194`）的注释直接点名 racknerd 小盘填满事故：*"a value like 10000h passes the sub-second floor check yet SILENTLY DISABLES the reaper"*。`MinXferCrossHomeReapAge`（`:216`）宁可复制一份常量也不 import `broker`，并用测试把两者钉在一起。**"知道自己在跟什么权衡"的代码。**

7. **注释是资产不是噪音**。本 lane 的高注释密度不是废话堆砌——抽查的每一处长注释都在记载"为什么是这个数"（`LockLeaseTTL` 的三项推导，`node.go` 之外整整 40 行）、"哪个 external review finding 改的"、"漏掉会怎样"。`cluster/rostergen.go:8-33` 解释为什么不能复用派生的 `max(added_at, phase_changed_at)`（retire-of-newest 会回退，严格单调的 agent 会拒绝修复后的 roster——一个楔子）——**这种注释省下的是下一个人重新踩一遍坑的时间。**

8. **本 lane 生产代码里 TODO/FIXME 数量为 0**，且悬空导出只有 1 个（`clusterroster.Select`）。这不是一个到处是"回头再说"的代码库。

---

## 本质 vs 偶然复杂度拆解

**估计：本 lane 11,166 行中约 75%（≈8,400 行）是问题域强加的本质复杂度，25%（≈2,760 行）是实现方式造成的偶然复杂度，其中约 560 行（5%）是可净删的直接重复。**

### 本质复杂度（≈8,400 行）——问题域就是这么贵

| 子域 | 行数 | 为什么删不掉 |
|---|---:|---|
| Raft 引擎 + FSM + 快照/恢复 + mTLS transport | ≈1,870 | 用 hashicorp/raft 而不是自己写已经是最省的选择；FSM 必须在**同一个 SQLite 事务**里推进 `applied_index`（§3.7 崩溃一致性），这个约束本身就要求自定义 applier 与快照/恢复走 modernc 的在线 backup。没有捷径 |
| 领域 op 的 SQL-bake（25 个 `Plan*`） | ≈1,960 | 复制的是**已烤好的 SQL 字面量**而不是"重放函数调用"——这是为了 replica 之间的确定性。代价就是每个 op 要显式烤字面量 + 相位守卫谓词（`PlanClusterNodeUpsert` 的 `WHERE phase IN (...)` 是成员变更的 CAS）。这是分布式一致性的直接税 |
| 停机磁盘手术（offline force-single/recover/init/restore/journal） | ≈2,300 | "最后一个 broker 挂了怎么救"没有廉价版本：flock + bolt 锁探测 + peer TCP 存活硬拒 + 原子 `RENAME_EXCHANGE` + 所有权镜像 + 前向可完成的 journal。`mirrorTreeOwnership`（`cluster/offline.go:390`）之所以存在，是因为 `sudo` 跑的工具会把 root:root 转移给 `User=tether` 的守护进程，导致永久 EACCES 崩溃循环——真实踩过的坑 |
| 接管别人写的 nats.conf | ≈820 | 见反证 3 |
| 签名发现材料（roster/seeds/invite/manifest/dial） | ≈700 | NAT 后的 agent 需要在没有稳定 broker 地址的前提下找到集群；签名 + pin + 反回滚 generation + 过期 是最小可用集 |
| 纯规划器（upgrade/spec）+ 收敛步进机 | ≈640 | 每一条 `Refused` 分支都对应一个"这么干会毁集群"的场景 |

**一个反直觉的观察**：本 lane 里最"啰嗦"的代码（`clusteroffline` 的前置条件、`natsconf` 的 fail-closed 拒绝、`clusterspec.Diff` 的 5 种 REFUSED）**恰恰是本质复杂度密度最高的部分**。它们不是防御性编程的洁癖，是"这个操作做错了要人肉救三天"的直接反映。用行数当臃肿指标会把它们误判。

### 偶然复杂度（≈2,760 行）——实现方式造成的

| 来源 | 估计行 | 归属 finding |
|---|---:|---|
| online/offline 双轨（force-single、backup、seeds 收敛） | ≈900 | F2, F4 |
| nats.conf 换装管线 5 处手抄 + 5 处 `Config{}` 装配 | ≈450 | F1 |
| `cluster_nodes` 裸 SQL 在 21 个文件里的重复投影 | ≈400 | F5 |
| `clusteroffline` 杂物间导致的导出壳 / 可见性摆动 / 名实不符 | ≈250 | F3 |
| `clusterroster` 平行继承线 | ≈90 | F8 |
| 分散的 SSOT（secrets 文件名 ×6、"VOTER" ×3、monitor listen ×3、RFC3339Nano ×2） | ≈60 | F1, F4, F5 |
| `internal/cluster` 5 职责同居带来的心智/API 表面成本 | ≈0 行但 143 个多余导出符号 | F6 |
| 其余：为跨包同步而被迫导出的函数、按评审轮次命名的测试文件（本 lane 15 个） | ≈600 | — |

**其中真正可"净删"的只有约 560 行**（F1 180 + F2 100 + F5 120 + F8 90 + F4 55 + F9 15）。剩下的偶然复杂度是**搬家能治、删不掉**的（F3/F6 是纯搬家，净行数≈0）。

### 为什么偶然复杂度长成这样——流程的锅

lane brief 提到"这套流程本身可能是某些结构债的成因"。本 lane 的证据支持这个假设，但方向和直觉不同：

- **多轮外审确实在代码里留下了年轮**：15 个按评审轮次命名的测试文件（`s6_s8_round7_external_review_test.go`、`r16_g67_g69_external_review_test.go`…）、以及大量 `// EXTERNAL REVIEW F4a:` / `// Stage-C M3:` / `// audit CC-2:` 形式的行内标记。这些标记本身**是资产**（它们保存了"这行为什么长这样"），但文件名把它们变成了考古地层。
- **真正的成因是"叶子增量"的推进方式**：每个增量（G2/G3/G4/R10/R16…）都在**已有路径旁边**加一条新路径（online force-single 加在 offline 旁边、grow cutover 加在 reconciler 旁边），而不是把两条路径的共同契约先抽出来。这是**增量交付的理性选择**（不动已上线路径 = 低风险），代价是每一次都欠一笔"稍后合并"的债。`natsconf.MoveAsideJSStore` 证明这笔债是可以还的——R16 那轮真的还了一次。
- **缺少机械约束**：仓库**没有 `.golangci.yml`**，`make lint` 只跑默认集，没有 `dupl` / `gocyclo` / `goconst`。F1 的 5 处手抄、F5 的 21 处裸 SQL、F8 的 90 行镜像，**任何一个重复度 linter 都会当场报出来**。`test/determinism` 那套自制 lint 证明这个团队完全有能力写机械约束——只是没往"重复"这个方向指过。

**最小可行的流程改进（成本极低、收益直接）**：加一份 `.golangci.yml`，只开 `dupl`（阈值放宽到 150 token）+ `goconst`（min-occurrences 3），把结果当**信息**而不是门（`--new-from-rev` 只看增量）。这会在下一次"在已有路径旁边加一条新路径"时，第一时间把 F1/F2 类的复制暴露在 PR 里，而不是等下一次审计或下一次产线事故。

---

## 合并方案汇总：11 → 8

| 新包 | 由谁构成 | 行（估） | 边界一句话 |
|---|---|---:|---|
| `cluster` | 现 `cluster` 的 A+B+D+E 组 | ≈3,340 | Raft 引擎 + `Command` 通货 + 离线 raft-store 手术（L-2 lint 钉死的那个包） |
| `clusterstate` **新** | 现 `cluster` 的 C 组 + `clusternodes` | ≈2,100 | `cluster_*` 表的**唯一** Plan（写）与投影（读）层，零 raft |
| `clusteroffline` | 现 `offline.go` + `init.go` + `journal.go` | ≈1,594 | 真·停机磁盘手术（包名重新变成真话） |
| `clusterbundle` **新** | 现 `clusteroffline` 的 backup/restore/manifest/bundle_scope + `broker/clusterbackup.go` 的管线 | ≈1,000 | bundle 的唯一 writer/reader，online + offline 共用 |
| `clusterdiag` **新** | 现 `clusteroffline` 的 doctor/preflight/diagnose | ≈412 | 只读体检，online + offline 共用 |
| `natsconf` | 现 `natsconf` + `natscluster` + `natsreconcile` | ≈1,267 | `/etc/tether/nats.d/nats.conf` 的完整生命周期：解析 → 渲染 → 校验 → 换装 → 收敛，对外只暴露 `Swap(intent)` 与 `ReconcileOnce` |
| `clusterdiscovery` | 现 `clusterroster` + `clustermanifest` | ≈786 | 账号签名的发现材料：铸/签/验/取/拼 dial 串 + 服务 well-known endpoint |
| `clusterplan` | 现 `clusterspec` + `clusterupgrade` | ≈392 | 运维意图 → quorum-safe 计划的纯规划器（零 IO，表驱动可测） |
| `serveconf` | 不动 | 328 | `broker.yaml` 解析 + duration 校验（**与 nats.conf 链无关**） |

**消除的胶水**：
- `natsreconcile.SynthesizeClusterListen` 不再需要导出（跨包同步消失）
- 5 处 `natscluster.Config{}` 装配 → 1 处
- secrets 三件套路径 join 6 处 → 1 处（`natsconf` 包常量 + `clusterdiag` 引用）
- `clusteroffline` 的 2 对导出壳（`checkPeersDead`/`CheckPeersDead`、`readRoster`/`ReadRoster`）消失
- `timeRFC3339Nano` 复制常量消失
- online backup handler 从 118 行降到 ≈40 行（只剩 leader 门 + adminsock 响应装配）

**建议实施顺序**（按"当前正在流血"排序，不按结构优雅度）：
**F2（产线正确性）→ F1（下一次改 conf 就会踩）→ F4/F5（DR 与投影一致性）→ F3（纯搬家，低风险）→ F8/F9（收尾）→ F6（最后，且可以不做）**。

**不建议现在做的**：F6 的 `cluster` 拆分。它是本 lane 里最"结构正确"的建议，也是收益最虚的——76 个 import 点要动、`test/determinism` 白名单要复核，换来的是"API 表面更清爽"。对一个跑着 1 broker + 6 agent 现网的工具，这个交换现在不划算。**等到下一次真的需要在 `broker` 之外复用领域 op 时再做。**
