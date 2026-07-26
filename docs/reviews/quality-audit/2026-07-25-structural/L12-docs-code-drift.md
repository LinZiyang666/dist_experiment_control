# L12 — 文档体量、漂移与流程沉积物

> 横切结构性质量审计 · lane key = `docs-code-drift` · 2026-07-25
> 范围：`docs/`（8,801 行）+ `docs/reviews/`（66,844 行 / 335 文件）+ `CLAUDE.md` + `README.md` +
> `test/simcluster/README.md` + `scripts/install.sh`（838 行）。合计约 **76,987 行**。
> **只读审计**：未修改任何实现代码 / 脚本 / 配置。

---

## 结论

**文档不臃肿，但「实现尺」歪了。**

面向人的三册运维手册（`usage.md` / `broker-ops.md` / `cluster.md` + `cluster-runbook.md`）质量**明显高于
行业均值**，与代码对得上：`tether serve` 的 23 个 flag 里 20 个被文档覆盖；`broker-ops.md` 附录 B 的端口表
`443/4222/6222/7000/7400/14000-14999` 与代码实际监听面完全一致；`usage.md` 甚至主动记录了「当前没有独立
`tether kill` 命令」这种否定事实；`install.sh` 的 13 个 flag 全部有文档。这一层**不是债**。

真正的问题有三处，且都不是"字太多"：

1. **`docs/architecture.md`（2315 行）作为 CLAUDE.md 指定的「实现尺」，约 40%（≈950 行）已与代码相反或失效。**
   NATS subject 前缀在文档里 69 处写 `tether.v1.`，代码是 `tether.v2`；§I.3 的代码样例白纸黑字写
   `ProtoVersion = 1`，代码是 `2`；整个 §F「数据面 frp 集成」（193 行，含可编译的 `frp/server` 调用样例）
   描述的依赖**已不在 `go.mod` 里**；§I.1 的包布局列 14 个包（含一个不存在的 `internal/reconcile`、一个不存在的
   `pkg/`），实际 35 个；Part II 的 P0–P11 实现路线图（311 行）是一份已完成到 v0.4.7 的待办清单。
   这不是"文档略旧"，是**尺本身给出相反读数**。

2. **运维手册与产品首推路径分叉了一次，且分叉在最危险的动词上。** `tether cluster add`（G4 的一命令 grow
   编排）在 `tether cluster --help` 里被列在第一位、是产品推荐的扩容路径；而 `cluster.md` 与
   `cluster-runbook.md` 的 C8 迁移表把 `cluster add` 归在「old (≤C7)」一列并明写
   "`add`/`sign-join`/`wait` are deleted now"，runbook §1 的 grow 流程仍然是更手工的 `join prepare` /
   `join approve`。运维照 runbook 走的是老路，deploy-tier drill 跑的是新路。

3. **66,844 行 review 沉积物里，140 份（17,656 行 / 26%）被任何文件引用为零**，且 `docs/reviews/` 没有索引、
   335 个文件平铺一层。上一次归档（commit `03ff578`）只处理了 `docs/` **顶层**的 5 份，`reviews/` 内部
   一次都没分层过。

**bloat 打分：6 / 10。**
理由：8,801 行 baseline 文档对应 68,328 行生产代码（12.9%）是**健康比例**，不是臃肿；`docs/reviews/`
与生产代码几乎 1:1（66,844 : 68,328）看着骇人，但其中 21,800 行 plan 是真正的决策记录（只有 6 份 plan
成为孤儿，说明 plan 被持续引用、是载荷）。扣分主要来自：① 唯一被 CLAUDE.md 指定为"尺"的文件有 40% 是错的；
② 沉积物零分层零索引；③ CLAUDE.md 自身 52% 的篇幅在 §5，其中单两行占全文 32%。
不到 8 分，是因为**没有发现任何"为了写文档而写文档"的产物**——每份文档都对应一次真实工作，问题是**寿命管理**
而非**产量控制**。

---

## 范围与方法

**做了什么**

- 通读 `docs/architecture.md` 全部 2315 行、`docs/distributed-broker-architecture.md` 头部与 §19、
  `CLAUDE.md`、`README.md`、`cluster.md`、`cluster-runbook.md` 头部、`usage.md` 目录与关键节、
  `test/simcluster/README.md` Mandate。
- **构建了真实 CLI 树作为 ground truth**：`CGO_ENABLED=0 go build ./cmd/tether` → 递归 `--help` 抓出
  **87 个命令 + 每命令的 flag 集**，另手工探测 8 个 hidden deprecated 别名与 `cluster keygen` /
  `cluster node-pub` 两个隐藏命令。
- 写了一个只读脚本，把 6 份文档里出现的 `tether <cmd> --flag` 全部抽出来与真实 flag 集做差集。
- 逐条到代码里核实 **28 条架构断言**（下表），不靠 grep 计数下结论。
- 对 335 份 review 文档做引用图分析（含 `xxx-{plan,review}.md` 花括号写法的展开匹配），算出孤儿集。
- 跨文件 4 行滑窗哈希查重（结论：**逐字重复为 0**，重复是语义层的，靠人工比对定位）。

**没做什么**：未跑 `make test` / `make e2e` / 未碰 `test/simcluster/`（按审计约束）。

### 架构断言逐条核实表（28 条）

| # | architecture.md 断言 | 代码事实 | 判定 |
|---|---|---|---|
| 1 | `SubjectPrefix = "tether.v1"`（§I.3:1486，全文 69 处） | `internal/proto/version.go:31` = `tether.v2` | **假** |
| 2 | `const ProtoVersion = 1`（§I.3:1485 / §J.1:1585） | `internal/proto/version.go:20` = `2` | **假** |
| 3 | §F.1 tetherd 内嵌 `github.com/fatedier/frp/server` | `go.mod` 无 frp；`internal/tunnel`（yamux） | **假** |
| 4 | §F.2 agent 持一个 frpc 实例，reload 热加载 proxy | `internal/tunnel/tunnel.go` yamux client | **假** |
| 5 | §I.1 存在 `internal/reconcile/` | 不存在（对账逻辑在 `internal/broker`） | **假** |
| 6 | §I.1 存在 `pkg/`（v1 留空） | 目录不存在 | **假** |
| 7 | §I.1 internal 包 14 个 | 实际 35 个 | **假** |
| 8 | §I.2 `tether serve reload` 子命令 | `serve` 无子命令 | **假** |
| 9 | §I.2 `tether --help` 三层分组、约 25 条命令 | 87 条命令 / 6 个自定义分组 | **假** |
| 10 | §E.0 `tether shellinit {bash,zsh,fish}` | 全仓无 `shellinit` | **假** |
| 11 | §F.8 `tether ps --procs-only` / `--ports-only` / `-n` / `-s` | `ps` 只有 `--all --json --home --nats-url` | **假** |
| 12 | §D.3 `tether ps --prune` | 不存在 | **假** |
| 13 | §H.2 `tether history --since X` | `history` 只有 `--follow --kind --lines` | **假** |
| 14 | §H.4 `tether admin disk` | `admin` 只有 audit/events/evict/nodes/runtime/sessions | **假** |
| 15 | §J.5 `tether version --check` | `version` 只有 `--help` | **假** |
| 16 | §K.3 `tether admin init` | 不存在 | **假** |
| 17 | §B.1/§C.2/§F.8 `tag` 动词可用 | 无 handler / 无 CLI；仅 `internal/auth/permissions.go:93` 残留一条 JWT 授权 | **假** |
| 18 | §H.1 `history-<sid>` `max_bytes: -1`（不限） | `internal/jsstream/jsstream.go:121,129` = 1 GiB + `DiscardNew` | **假（行为级）** |
| 19 | §H.1 两条流 `replicas: 1`；§H.6「broker 本身单点」 | `jsstream.ReplicasFor(nVoters)`，raft HA 已 GA | **假** |
| 20 | §A.3 broker 入站端口 = 443 / 7000 / 14000-14999 | 另有 6222(route) / 7400(raft) / 7480(manifest) / 8090(sub) | **不完整** |
| 21 | §B.1 subject 树 | 缺 transfer / proxy / cluster / alert / caps 共 ~14 个 builder 家族 | **不完整** |
| 22 | §B.2 已激活 member JWT 模板 13 条 allow | `permissions.go:64-133` 40 条 | **不完整** |
| 23 | §L.2「proxy 全 additive，proto 保持 v1」 | proto = 2 | **假** |
| 24 | §L.5 migration 到 `0007_proxy_generation.sql` | 到 `0017_port_alloc_last_rehome.sql`（含 `0016_proxy_cluster`） | **不完整** |
| 25 | §D.2 heartbeat 5s / STALE / OFFLINE 60s | `internal/node/node.go:33-34` 一致 | **真** ✅ |
| 26 | §D.4/§F.3 端口带 14000-14999 | `internal/port/port.go:50-51` 一致 | **真** ✅ |
| 27 | §E.6 PIN 限速 每 broker/每 IP/每分钟 ≤10，集群下 ≈N×10 | `internal/authcallout/ratelimit.go:57` 及其注释完全一致 | **真** ✅ |
| 28 | §E.4 argon2id `m=64MiB, t=3, p=2` PHC | `internal/auth/pin.go:34,45` 一致 | **真** ✅ |

（另：§C 控制面分层、§D 四类状态机、§G 对账语义、§K 安装路径抽查均**成立**，见「反证」。）

---

## Findings

### F1 — [critical] `architecture.md` 作为「实现尺」有 ≈40% 给出相反读数

**证据**

- `docs/architecture.md:108` / `:1486` / `:1502` / `:1507`（共 69 处）：subject 前缀 `tether.v1.`
  ↔ `internal/proto/version.go:20-31`（`ProtoVersion = 2`，`SubjectPrefix = "tether." + SubjectVersionToken` = `tether.v2`）
- `docs/architecture.md:1485`：`const ProtoVersion = 1`（代码样例，可被直接抄）
- `docs/architecture.md:831-1023`（§F 全章 193 行）：`frpsCfg := v1.ServerConfig{...}` / `srv, _ := server.NewService(frpsCfg)` /
  「frps 插件钩子查 SQLite」↔ `go.mod` 无任何 `fatedier/frp`；实际是 `internal/tunnel/tunnel.go`（`hashicorp/yamux`），
  代码里只剩历史注释（`internal/tunnel/tunnel.go:5` "We deviate: tunnel is a…"）
- `docs/architecture.md:1392`：`internal/reconcile/` — 不存在；`:1397` `pkg/` — 目录不存在
- `docs/architecture.md:1936-2246`（Part II，311 行）：P0–P11 的「做 / 测试 / 出口」清单，全部完成于 v0.1.0，
  当前已发布 v0.4.7

**为什么这是债（它让什么未来改动变难/变危险）**

CLAUDE.md §1 把本文件定义为「架构基线（**实现尺**）」，§2 要求"每进入新 phase 先过 architecture.md checklist"，
§5 要求"不变量以 `architecture.md` 为准，实现与审查都以它为尺"。于是：

- 任何按 §B.1 实现新 subject 的人会写出 `tether.v1.…` 并在 `internal/proto` 的静态守卫上炸掉（好情况），
  或在只读的文档/脚本里留下错前缀（坏情况——已经发生：`docs/architecture.md` 自己就是 69 处）。
- 下一次真的要做 wire v2→v3 时，**没有一份文件能告诉你 v2 的完整 subject 面**：§B.1 缺 transfer / proxy /
  cluster / alert / caps 共 ~14 个 builder 家族（`internal/proto/subjects.go` 有 40 个 `Subj*` 构造器，
  §B.1 覆盖约 26 个）。
- §F 让新人以为数据面要处理 frp reload / frps 插件钩子；真实数据面是 in-process yamux + `tunnel.Server`
  的 `killGen` / `CloseSession` 语义，与文档描述的失败域完全不同。**这是最贵的一类误导：它不会立刻编译失败，
  会先让人做完错误的设计再发现。**

**建议**

分三刀，全部是文档改动、零代码风险：

1. 文件顶部加一条 status banner：「§A–§K 是 **proto v1 单 broker** 的历史基线；proto v2 / 集群面的绑定契约在
   `docs/distributed-broker-architecture.md`」——**这一条最便宜、收益最大**，把"错的尺"降级为"历史档案"。
2. 机械订正 6 处可自动化的事实：`tether.v1.`→`tether.v2.`（69 处，`sed` 可做但需人工核 §J 讲版本迁移的段落）、
   `ProtoVersion = 1`→`2`、§I.1 包表重建、§A.3 端口表补 6222/7400/7480/8090、§H.1 两条 stream 配置、
   §L 的 proto/migration 版本号。
3. §F 整章重写为 `internal/tunnel`，或整体移入 `docs/reviews/archive/`（frp 方案的取舍论证有历史价值，
   但不该占据"尺"的位置）；Part II（311 行）随 P0–P11 完成一并归档——**这与 `03ff578` 归档
   `allgreen-remediation-roadmap.md` 的判据完全一致，只是当时没扫到 architecture.md 内部**。

**量化**：可移出 ≈**500 行**（Part II 311 + §F 大部约 170 + `phase 0 之前需要决定的外部变量` 8）；
可机械订正 ≈**450 行**（69 处前缀 + 包表 + 端口表 + 流配置 + 命令面 8 处）。

**改动风险**：low（纯文档）。**不触碰 wire 协议或不变量**——恰恰相反，是让文档重新描述现有不变量。

---

### F2 — [critical] 运维手册教的 grow 路径不是产品首推的 grow 路径

**证据**

- 产品：`tether cluster --help` 的 "Online (leader, daemon running)" 组**第一行**是
  `add  Grow the cluster: orchestrate a new broker to VOTER in one idempotent, resumable command (run on the joiner)`，
  22 个 flag，引入于 commit `d93d842`（"G4 one-command grow orchestration"）
- `docs/cluster-runbook.md:15`：`| `tether cluster add <id> <host:7400> <pub> …` | joiner: `tether cluster join prepare …` → …; leader: `tether cluster join approve <bundle> --wait` |`
  —— 把 `cluster add` 放在 "old (≤ C7)" 列
- `docs/cluster-runbook.md:27`："… `add`/`sign-join`/`wait` are deleted now." —— **`add` 没有被删除**
- `docs/cluster.md:72`（中文镜像）同样写「`add`/`sign-join`/`wait` 现在已删除」
- `docs/cluster-runbook.md:41`：`## 1. Grow the cluster (add a voter) — two-phase, prepare/approve`
  —— 全篇 grow 剧本走 prepare/approve
- 反向证据：`test/simcluster/README.md:160-161` 与 `:189` 已经在跑 `tether cluster add`
  （drill trailer `GREW-VIA-TETHER-CLUSTER-ADD`），`docs/deploy-tier-gotchas.md:89`（#31）整条 gotcha
  讨论的是 `cluster add` 的 grow lock —— **deploy-tier 文档比运维手册新了整整一代**
- 另：`cluster sign-join` / `cluster wait` 被文档宣告"已删除"，实测**仍作为 hidden 命令存在**
  （`tether cluster sign-join --help` 返回 0）

**为什么这是债**

- 运维在真实扩容时（最不容错的场景之一）照着 runbook 走 **prepare/approve 手工双阶段**，而 deploy-tier
  drill 覆盖的是 `cluster add` 端到端路径。**实操路径与被测路径分岔**——`cluster add` 里被 G4 修掉的
  half-failure 清理、nonvoter 暂存、JS 重置等编排，手工路径不一定等价。
- 这正是项目记忆里那条教训（「改动一个命令/动词的契约后，必须全局扫所有调用点」）**在文档侧的复发**：
  代码侧扫得很干净（`grep` 产品打印的命令提示，`cluster recovery force-single` 等新拼写全对），
  运维手册侧漏了。
- 一句自相矛盾的断言（"`add` … are deleted now"）会让运维**不敢用**唯一的一命令扩容路径。

**建议**

- `cluster-runbook.md` §1 改为：`tether cluster add`（在 joiner 上跑）为**主路径**，
  `join prepare` / `join approve` 降级为「需要人工审批边界时的分步路径」；
- 两份 C8 迁移表里 `add` 那行改写为「旧的 **位置参数** 形式 `cluster add <id> <host> <pub>` → 新的 flag 形式
  `cluster add --self-id … --raft-addr …`」，并删掉"`add` … 已删除"这半句；
- `sign-join` / `wait` 那半句改成「保留为 hidden 别名」（与其余六个别名一致），或真的删掉命令。

**量化**：改动约 **40 行文档**；影响的是每一次 grow 操作。

**改动风险**：low（文档）；但若选择"真删 hidden 命令"则是 medium（有脚本可能在用）。

---

### F3 — [high] 文档与代码存在**行为级**不一致：`history-<sid>` 的容量语义

**证据**

- `docs/architecture.md:1218-1219`：
  `"max_age": -1, // 不过期` / `"max_bytes": -1, // 不限大小（requirements A10）`
- `internal/jsstream/jsstream.go:121`：`const historyMaxBytesPerSession = 1 << 30 // 1 GiB`
- `internal/jsstream/jsstream.go:129`：`MaxBytes: historyMaxBytesPerSession` + `Discard: jetstream.DiscardNew`
- `internal/jsstream/jsstream.go:113-120` 的注释自陈改动来源：
  "Audit shard 03 F3: previously MaxBytes=-1 made DiscardNew unreachable code"
- **最刺眼的一点**：`internal/jsstream/jsstream.go:86-90` 的注释写着
  "Architecture H.1 spec values are inlined rather than imported from anywhere else; **if H.1 changes, this is
  the one place to update**" —— 契约被声明为单向（H.1 → 代码），实际改动却是**反向**发生的，H.1 没被更新。
- 同章同表还有 `"replicas": 1`（§H.1 两条流）与 §H.6「v1 不做 Stream 复制 / 集群（`replicas > 1`）；
  broker 本身单点」↔ `internal/jsstream/replicas.go:44 ReplicasFor(nVoters)`

**为什么这是债**

`max_bytes` + `discard` 是**运维会据此做容量规划的数字**。文档承诺"不限大小、磁盘真满时才拒新"，
代码实际是"单 session 审计到 1 GiB 就 `DiscardNew` 拒写"。一个长跑 session（`prod`，按 §H.4 示例
1.2 GiB / 540k msgs）在文档语义下应该继续写，在实际语义下**已经在拒审计**。审计拒写是静默的
（`DiscardNew` = publish 报错，但审计是 broker 单边 fire-and-forget 路径）。

它同时污染了一条元规则：既然 `jsstream.go` 自称"H.1 变了改这里"，那 H.1 就必须是可信的；
现在这条注释是**反向的谎**，下一个改这里的人会以为自己不用回改文档。

**建议**

- 订正 §H.1 两条 stream 的 `max_bytes` / `replicas`，并把 §H.6 的"broker 本身单点"改成
  「单 broker 是 N=1 退化形态，HA 见 distributed-broker-architecture.md §6.4」；
- 把 `jsstream.go:89` 的注释方向反过来：「本文件的常量是 SSOT，§H.1 引用之」——SSOT 归代码，
  文档负责解释；这比要求人两边同步现实得多。

**量化**：文档 6 行 + 代码注释 2 行。**改动风险**：low。

---

### F4 — [high] `CLAUDE.md` 漏掉两份真正的实现尺，同时 32% 的篇幅压在两行 simcluster 长文上，且含 3 条已过时指令

**证据**

- `CLAUDE.md` §1「项目与文档地图」列出 requirements / architecture / usage / broker-ops / cluster /
  devices / reviews / quality-audit / simcluster，**未列**：
  - `docs/distributed-broker-architecture.md`（691 行）—— `README.md:20` 称其为「Distributed-broker HA (proto v2)」，
    `docs/cluster-runbook.md:3` 称其为 "**the binding contract**"，`grep -c "distributed-broker-architecture" CLAUDE.md` = **0**
  - `docs/deploy-tier-gotchas.md`（618 行活账本，12 条 OPEN）—— `grep -c` = **0**
- 体量：`CLAUDE.md` 7,680 字符（13,029 字节）；按 `##` 切分，**§5「编码与测试约定」= 4,002 字符 = 52%**；
  其中 **第 65 行单行 1,627 字符 + 第 66 行 845 字符 = 2,472 字符 = 全文 32%**，内容是 simcluster 的
  「什么时候跑 / 在哪台机器跑 / 三档判断 / 定位铁律」——`test/simcluster/README.md`（403 行）已完整覆盖
- 过时指令：
  - `CLAUDE.md` §6「在默认分支（`main`）上**先开分支**（`phase/<N>-<slug>`，每个 phase 至少一个 PR）」
    ↔ `git log` 显示近期全部直接落 `main`（`84bf030` / `b063ade` / `03ff578` …），无 `phase/*` 分支；
    `docs/architecture.md:2303` 的 checklist 同样要求开分支
  - `CLAUDE.md` §3「Workflow 模型硬约束：一律不得低于 **Opus 4.8** …（= Opus 4.8 `claude-opus-4-8[1m]`，最稳）」
    ↔ 当前会话主模型是 Opus 5（`claude-opus-5[1m]`）；型号常量写死在指令里必然过期
  - `CLAUDE.md` §1 描述 `docs/reviews/` 的命名约定为 `p<N>-plan.md` / `p<N>-review.md`，
    实际 335 个文件用了 `b/c/d/g/r/s` 六套前缀 + 一堆自由命名（见 F5）

**为什么这是债**

`CLAUDE.md` 是**每个会话唯一强制加载的入口**，它的错误会被放大到之后的每一步：

- 缺 `distributed-broker-architecture.md` ⇒ 新会话被告知「architecture.md 是实现尺」，于是拿 **proto v1
  单 broker 的尺** 去量 proto v2 集群代码（`internal/cluster` 5,300 行 + `internal/clusteroffline` 2,959 行
  + broker 里的集群面全部无尺可对）。**F1 的伤害被这条放大。**
- 缺 `deploy-tier-gotchas.md` ⇒ 触碰部署面时不知道有 12 条 OPEN 缺陷账本，会重复踩。
- "先开分支"的指令与实践矛盾 ⇒ agent 要么照做（开一个没人 review 的分支）、要么违反（那这条指令就是噪声）。
- 32% 的固定 token 预算花在一段"仅当触碰部署面时才需要"的内容上，**挤压的是每会话都要读的 §1–§4**。

**建议**

- §1 补两行地图（distributed-broker-architecture.md / deploy-tier-gotchas.md），并把 architecture.md 那行
  改成「**proto v1 单 broker 历史基线**；v2 / 集群面见 distributed-broker-architecture.md」；
- §5 的两段 simcluster 长文压成 **3 行**：定位一句 + "跑法与铁律见 `test/simcluster/README.md` 顶部 Mandate"
  + 宿主判断三档保留（这条是真的每次都要用的）；
- 删掉 `phase/<N>-<slug>` 分支条款（与 `architecture.md:2303` 一并）；
- 把模型硬约束改成"继承会话主模型，不显式设 `model`"，**去掉具体版本号**。

**量化**：`CLAUDE.md` 可从 7,680 字符降到 ≈**5,200 字符（-32%）**，同时信息量**增加**（补了两份尺）。

**改动风险**：low。

---

### F5 — [high] 66,844 行 review 沉积物零分层零索引；140 份（17,656 行 / 26%）被任何文件引用为零

**证据**

- `docs/reviews/`：335 个 `.md` 平铺一层，**没有 README / INDEX / 00-*.md**
- 引用图分析（匹配含 `xxx-{plan,review,external-review}.md` 花括号展开）：
  - 被 `docs/reviews/` **以外**的任何文件引用：**76 / 335（23%）**
  - 被**任何文件**（含其它 review）引用：195 / 335
  - **孤儿 140 份 / 17,656 行**，构成：external-review* **89**、review* **43**、plan **6**、other 2
- 分类体量：plan 65 份 / 21,800 行；external-review 119 份 / 20,058 行；review 66 份 / 11,426 行；
  review-roundN 30 份 / 4,622 行；external-review-tasklist 16 份 / 1,410 行
- 工作单元：**109 个不同 stem**，中位数 2 份/单元，均值 613 行/单元；最重的几个：
  `s3-s5` 25 份 / 3,936 行、`s6-s8` 21 份 / 3,570 行、`p13` 10 份 / 2,890 行、
  `simcluster-coverage-roadmap` 14 份 / 2,556 行
- 上一次归档 `03ff578` 只移动了 `docs/` **顶层**的 3 个文件 + 拆分了 gotcha 账本；`docs/reviews/` 内部
  **一次都没分层过**

**为什么这是债**

- **plan 是载荷，review 是残渣，但它们平铺同级。** 数据本身证明了这点：65 份 plan 里只有 6 份成孤儿
  （92% 被引用），而 119 份 external-review 里 89 份是孤儿（75% 无引用）。plan 会被
  `architecture.md`（§L 引 `p13-plan.md`）、`distributed-broker-architecture.md`（§19 引 d0–d9-plan）、
  代码注释持续引用；review 报告在 finding 被采纳后**语义即终止**。
- 直接后果：想回答「上次这个决定是怎么定的」，只能在 335 个命名不规则的文件里 grep。
  `docs/reviews/` 已经出现了 `p<N>-`、`b<N>-`、`c<N>-`、`d<N>-`、`g<N>-`、`r<N>-`、`s<N>-s<M>-`
  七套编号体系，`CLAUDE.md` §1 只描述了其中一套（`p<N>-`）。
- **`docs/reviews/` 已经是事实上的第二基线目录**：`distributed-broker-requirements.md`、
  `allgreen-remediation-roadmap.md`、`deploy-tier-gotchas-closed.md`、`v0.4.5-ha-grow-ops-gotchas.md`
  都住在这里，与一次性的 `-review-round4.md` 混在一起。这让归档这个动作变得危险
  ——不敢批量移，因为不知道哪些是活的。

**建议（不是废流程，是给沉积物加寿命管理）**

1. 目录分三层，**一次 `git mv` 完成**：
   - `docs/reviews/` 根：**只留 plan + 活账本 + 基线**（65 plan + gotchas + roadmap ≈ 24,000 行）
   - `docs/reviews/archive/<stem>/`：该工作单元的所有 review / round / tasklist（≈43,000 行）
   - `docs/reviews/quality-audit/`：保持现状（已有 `00-punch-list.md` 索引，是全仓最好的组织）
2. 写一份 **`docs/reviews/INDEX.md`**：109 行，每个 stem 一行 —— `stem | 版本 | 日期 | 一句结论 | plan 路径`。
   这是**最高杠杆的一处新增**：120 行换掉 335 个文件的检索成本。
3. 流程侧加一条收尾动作：phase 结束时，把 review 报告移进 `archive/<stem>/`，只把"最终裁定"回写进 plan
   的一个 `## 落地结论` 小节。这样**下一次沉积自动分层**，不需要再来一次批量归档。

**量化**：移出主目录 ≈**17,656 行孤儿 + ~25,000 行已终结 review**；新增 ≈**120 行索引**。
主目录从 335 文件降到 ≈**70 文件**。

**改动风险**：low（`git mv` 保历史；需同步修 76 处跨目录引用——`03ff578` 已证明这个动作可控）。

---

### F6 — [medium] 154 个测试文件（20,710 行 / 占测试总量 22%）以「审查轮次」而非「被测主题」命名

**证据**

- 严格按 review 命名：65 文件 / 6,375 行；放宽到含 `b/c/d/g/r/s<N>_` 流程前缀：**154 文件 / 20,710 行**
  （测试总量 93,084 行的 **22%**）
- 同包同主题被 round 号切成多份：`internal/broker/p13_external_review_round{2,4,5,6,8}_test.go` —— 5 个文件
- 以审查者命名：`cmd/tether/codex_allgreen_external_review_test.go`、
  `internal/authcallout/codex_allgreen_external_review_test.go`、`claude-allgreen-*`
- 以多轮 finding 编号拼名：`cmd/tether/r16_g67_g69_external_review_test.go`、
  `internal/broker/g5_g7_external_rereview_test.go`
- 内容**是有价值的**：例如 `internal/broker/p13_external_review_round8_test.go:14`
  `TestExternalReviewProxyMutationsShareSerializationLock` 钉的是 `proxyOpMu` 串行化——一条真不变量

**为什么这是债**

这不是"测试太多"（93k 测试 : 68k 生产 对一个分布式控制面是**正常甚至偏低**的），而是**检索键错了**。
要加一个 proxy 功能时，"已有哪些 proxy 不变量被钉住"这个问题，答案分散在 5 个 `p13_external_review_roundN`
文件里，而 round 号在修复落地那天就失去了全部语义——它只剩考古坐标。后果是：

- 新增测试时倾向于**再开一个新文件**（因为不知道往哪个 roundN 里加），沉积继续；
- 删除/合并时不敢动（不知道 round4 和 round6 是不是同一组不变量的两半）；
- `grep TestExternalReview` 会命中几十个语义无关的测试。

**建议（不删测试）**

- 按主题重命名 + 合并：`proxy_serialization_test.go` / `proxy_generation_fencing_test.go` /
  `force_single_gates_test.go` …；
- 在每个测试函数上方保留一行溯源注释：`// origin: p13 external review round 6 F2`——审计轨迹不丢，
  但不再占用文件名；
- 流程侧：step 4 的专家新增测试**要求落到主题文件**，不允许新建 `*_external_review_*_test.go`。

**量化**：净减行数 ≈**0**（纯重命名 + 合并 import/helper 后小幅减少）；文件数 **154 → ≈40**。

**改动风险**：low（不改断言逻辑）。注意：跑一次 `make test` 验证即可，**不触碰 wire 或不变量**。

---

### F7 — [medium] 「里程碑映射」只登记了 7 个 post-1.0 增量，实际 109 个；整个 D0–D9 分布式 epic 不在里面

**证据**

- `docs/architecture.md:2259-2260`：post-1.0 增量共 **7 项**（file-transfer / ps-retention / P12 / P13 /
  transfer-unrestrict / remote-fs-resilience / cluster-phase-fluidity）
- `docs/reviews/` 的 stem 分析：**109 个不同工作单元**
- D0–D9 分布式 broker epic 的登记在**另一份文档**：`docs/distributed-broker-architecture.md:530`（§19），
  architecture.md 全文对 raft / 集群的提及只有 10 处，且全部集中在 §E.6 与 2268-2280 的
  cluster-phase-fluidity 引用块——**epic 本体没有出现在里程碑表里**
- `CLAUDE.md` §2 明确指定：「按 `architecture.md`「里程碑映射」的 **P<N> 序列**推进」

**为什么这是债**

CLAUDE.md 把这张表指定为 phase 排序与"先父后子"判定的权威。它覆盖了 ~7/109 ≈ **6%** 的历史。
于是"下一个该做什么 / 这个改动的前序做完没有"这类问题，实际答案在三个地方：
architecture.md 里程碑映射（7 项）、distributed-broker-architecture.md §19（D0–D9）、
以及 109 份散落的 plan 文件名。**没有单一处能回答"这个仓库到今天为止做过什么"。**

**建议**

- 把「里程碑映射」明确降级为「P0–P11 历史里程碑」，并在其下加一行指向 F5 建议新增的
  `docs/reviews/INDEX.md`——由索引承担登记职责；
- 或者反过来：把 INDEX.md 的内容并进里程碑映射。**二选一，不要两份都半死不活。**

**量化**：改 architecture.md 约 10 行；与 F5 的 INDEX.md 是同一件事的两面。

**改动风险**：low。

---

### F8 — [medium] 三册运维文档之间的语义重复：quorum 讲解 3 处、C8 迁移表 2 份、且 `cluster.md` 内部自我重复

**证据**

- quorum 解释出现 **3 次**：
  - `docs/cluster.md:32`「什么是 cluster / 什么是 quorum（先读）」（32-44 行）
  - `docs/cluster.md:74`「什么是 cluster / quorum（一句话版）」（74-83 行）——**同一文件内，相隔 40 行**
  - `docs/cluster-runbook.md:29-40`「0. What is a cluster, and what is quorum?」
- C8 命令迁移表（11 行）**两份**：`docs/cluster.md:59-74`（中文，blockquote）与
  `docs/cluster-runbook.md:13-27`（英文）——逐行一一对应
- 跨文件 4 行滑窗查重结果为 **0**（说明重复不是复制粘贴，而是**两次独立表述同一事实**——这更糟，
  因为改一处时另一处不会被 grep 命中）

**为什么这是债**

- 每次命令改名要同步 **2 份**迁移表；F2 里 "`add` … 已删除" 的错误就是**同时错在两处**——
  说明同步已经失效过一次。
- quorum 定义若将来变化（比如支持 witness / non-voter 计入），要改 3 处。
- `cluster.md` 内部两处 quorum 解释还给出**不同的公式写法**（`⌈(N+1)/2⌉` vs `⌊voter 数 / 2⌋ + 1`），
  虽然数值等价，但读者会怀疑自己看错了。

**建议**

- quorum 概念单点落在 `cluster.md`「先读」一节；`cluster.md` §5.6 的重复块删掉，
  `cluster-runbook.md` §0 压成 3 行 + 一个链接；
- C8 表只保留 `cluster-runbook.md`（英文，运维实操侧），`cluster.md` 改为一行链接。

**量化**：可删 ≈**60 行**，把 2 处同步点降到 1 处。

**改动风险**：low。

---

### F9 — [low] 产品打印给运维的手抄命令仍有一处陈旧：`tether session list`

**证据**

- `cmd/tether/error_hints.go:24`：`"session_not_found_or_deleting": "the session doesn't exist or is being deleted; check \`tether session list\`."`
- `cmd/tether/error_hints.go:25`：`"session_not_found": "the session doesn't exist; check \`tether session list\`."`
- 实际动词是 `tether session ls`；实测 `tether session list` 只打印 `session` 分组的 help（exit 0），
  **不会列出 session**
- `docs/architecture.md:684` 同样写 `tether session list`
- 反证：其余产品打印的命令提示（`tether cluster recovery force-single`、`tether cluster reconcile nats`、
  `tether cluster unlock`、`tether alert clear` …，共 60+ 处）**全部与真实 CLI 一致**——
  说明契约扫描做得很好，只漏了这一处

**为什么这是债**

这正是项目记忆里那条教训（「改动一个命令/动词的契约后，必须全局扫所有调用点——**含产品打印给运维的手抄文案**」）
的**唯一残留复发点**。错误提示是用户在最困惑时读的文案；给一条跑了不报错但也不做事的命令，比不给更糟。

**建议**

- 改成 `tether session ls`（2 行）；
- 加一条静态测试，断言 `error_hints.go` 里每个反引号命令都能被 cobra 解析到叶子命令——
  仓库已有 `cmd/tether/command_tree_inventory_test.go`，扩它即可，**让这类漂移以后自动被抓**。

**量化**：2 行修复 + ~30 行测试。**改动风险**：low。

---

### F10 — [low] `requirements.md`（被 CLAUDE.md 称为「唯一需求真相」）用的是另一个产品的名词

**证据**

- `docs/requirements.md:130`「### 4.2 Relay（公网机）」、`:136`「### 4.3 Controllerd（公网机，长驻服务）」
  —— 实际是**单二进制 `tether` 三角色**（broker / agent / ctl），没有 Relay、没有 Controllerd 进程
- `:132-134`「软件：`frps`。职责：TCP 端口反向代理，给 agent 的 `frpc` 子进程提供反向隧道」
  —— `go.mod` 无 frp；数据面是 `internal/tunnel`（yamux）
- `:153`「拉起自己的 `frpc` 子进程（**每 agent 独立一个 frpc**，不复用）」—— 无子进程模型
- `:312`「`~/.config/ctl/config.yaml` / `~/.config/agent/config.yaml`」—— 实际 `~/.tether/config.toml` /
  `~/.tether/agent/<sid>/agent.yaml`
- `:206`「`ctl agent kick <id>`」、`:446`「`ctl session rm <name>`」—— 二进制叫 `tether`，且 `kick` 未实现
- 统计：`ctl ` 35 处、`frp` 19 处、Relay/Controllerd 28 处、`.ctl/` 路径 9 处

**为什么这是债**

需求基线是唯一能判定「这个改动是否越界 / 是否偏离北极星」的文件。名词全错时它只能被当散文读、
不能被当尺用——于是"这个功能在不在范围内"的判断**没有可引用的依据**，只能靠人记忆。
`§1.3 显式非目标` 和 `§2 系统边界（北极星）` 这两节的**意图**仍然有效且宝贵，
但被包在一层过期术语里，可信度被整体拉低。

**建议**

- 顶部加 status banner：「术语与角色划分已被 `architecture.md` / `distributed-broker-architecture.md` 取代；
  本文的**持续有效部分**是 §1.3 显式非目标、§2 北极星、§6 非功能需求」；
- 或做一次机械改名（`ctl`→`tether`、删 Relay/Controllerd 两节、frp→tunnel），约 90 行改动。

**量化**：banner 方案 5 行；改名方案 ≈90 行。**改动风险**：low。

---

## 反证：做得好的地方

审计范围里**明确做得好、体量正当**的部分，逐条点名：

1. **三册运维手册的拆分是教科书级的。** `usage.md` 的 TOC（`docs/usage.md:23-44`）不仅列出本册内容，
   还**逐节说明哪几节被移到了哪一册**（「5.5/5.20 见 broker-ops.md；5.6/5.7 见 cluster.md」），
   并保留了跨册统一的章节编号。§5.4→§5.8 的编号"跳号"不是遗漏，是**故意保留的锚点**。
   拆册后没有产生一处逐字重复（4 行滑窗查重 = 0）。

2. **`broker-ops.md` 附录 B 的端口表完全正确且完整**：`docs/broker-ops.md:766-779` 覆盖
   443 / 4222 / 6222 / 7000 / 7400 / 14000-14999，并注明 6222/7400 只在私网放行——
   与代码实际监听面（`grep` 得 4222×21、6222×8、7400×4、7480、8090、7000×3）一致。
   这是 architecture.md §A.3 端口表**应该长成的样子**。

3. **`usage.md` 记录否定事实。** `docs/usage.md:279`：「杀掉远端进程 | `tether run` 里按 Ctrl-C；
   当前没有独立 `tether kill` 命令」。主动写下"这个东西不存在"是罕见的文档纪律
   ——architecture.md 至今仍在 §C.2 / §F.8 里把 `kill`、`tag` 当作已交付命令列表。

4. **architecture.md 的 DOC-12 更正块是"文档如何认错"的范本。** `docs/architecture.md:1198-1206`
   逐条说明 `rotated_pin` / `kicked` / `agent_unregistered` 三个事件类型「v1 没有任何 writer 发它们，
   属于文档超前承诺，已删除」，并进一步论证「把代码改名成 `kicked` 会破坏 agent 对 `agent_evicted`
   的订阅与既有测试，故**订正文档、保留代码名**」。这正是 F1/F3 该走的路子——
   **说明它已经被证明可行，只是没被系统化应用。**

5. **产品打印给运维的命令提示扫得很干净。** 抽取生产代码里所有 `tether …` 形式的打印文案（60+ 处唯一命令），
   除 F9 一处外全部与真实 CLI 一致——`cluster recovery force-single`、`cluster reconcile nats`、
   `cluster unlock`、`cluster ops show`、`alert clear`、`agent join` 等新拼写全对。
   记忆里那条「契约改动要全局扫」的教训**在代码侧确实被贯彻了**。

6. **`cluster.md` / `cluster-runbook.md` 提供了 11 行的 old→new 命令迁移表**，并明确说明
   「旧顶层拼写作为 HIDDEN deprecated 别名保留一个 release」。实测 8 个别名确实全部存在——
   **文档承诺与运行时行为一致**（除 F2 指出的 `add`/`sign-join`/`wait` 三行）。
   给命令改名配一张迁移表是很多项目都不做的事。

7. **`docs/reviews/quality-audit/` 是全仓组织最好的一块。** 有 `00-punch-list.md` 索引 + 编号命名
   `01-concurrency` … `06-deadcode-drift`。F5 建议的 INDEX.md 就是把这个已被证明有效的模式
   推广到 `docs/reviews/` 根目录。

8. **`test/simcluster/README.md` 的 Mandate 段（403 行文档的头 30 行）值得单独表扬。**
   「Never accommodate tether's broken design」「Expose the gap; do not fill it」
   「一次操作若靠模拟集群写的复杂脚本才成功，那是 tether 的失败被掩盖」——
   这是**把测试工具的反激励写进文档**，防止工具漂移成"让产品看起来更能干"的糖衣。
   17,583 行 shell 的 drill 代码有这样一份 mandate 压着，是体量正当的。

9. **`.gitignore` 正确隔离了本地 scratch 文档。** `docs/cluster-ha-realmachine-test-plan.md`（325 行，
   v0.4.2 的一次性执行计划）与 `docs/devices-ops.local.md`（201 行，含凭据）都在 `.gitignore:67,70`——
   它们**不占仓库体量**。审计初期以为这是两份未归档的过程产物，实测不是。

10. **`internal/auth/permissions.go:5-11` 的 off-SSOT 副本处理得当**：为避免 import 环而复制
    `subjectPrefix = "tether.v2"`，注释写明理由 + 指出有静态守卫测试
    `TestSubjectPrefixInSyncWithProto` + 说明 D0 tripwire 白名单了它。
    **技术债被显式标注 + 用测试封住 = 不是债。**

11. **`03ff578` 那次归档判据是对的。** commit message 逐条说明为什么三份移出、为什么两份留下
    （「deploy-tier-gotchas.md 是活账本（#25+ 仍在长）；distributed-broker-architecture.md 是 v2 实现尺，
    被 README 和 Go 注释引用」），还核实了 gotcha 编号全局连续性以保证 `assert_bug` token 仍可解析。
    F5 不是说这次归档做错了，而是说**它的判据没有被推广到 `reviews/` 内部**。

---

## 本质 vs 偶然复杂度拆解

本 lane 的"范围行数"是文档而非生产代码，故按文档语料 76,987 行拆解。

### 本质（问题域强加，删不掉）— 约 40%

| 分类 | 行数 | 为什么是本质 |
|---|---:|---|
| 三册运维手册 + runbook | 3,798 | 一个要人 sudo 部署 broker、管 raft quorum、做灾难恢复的系统，**运维手册就是产品的一部分**。`usage.md` 1,755 / `broker-ops.md` 782 / `cluster.md` 422 / `cluster-runbook.md` 839——对 87 个 CLI 命令来说是**偏薄**的。 |
| 两份架构基线（去掉已死内容） | ~2,700 | NAT 穿透 + auth_callout + raft HA + JetStream + 文件传输 + PTY 六个子系统，各自的取舍论证不可省。 |
| `deploy-tier-gotchas.md` 活账本 | 618 | 12 条 OPEN 部署缺陷，每条带复现签名。这是**唯一记录"真实部署下 tether 会怎么坏"的地方**，hermetic 测试结构上到不了。 |
| `docs/reviews/*-plan.md` | 21,800 | 决策记录。数据证明它是载荷：65 份 plan 只有 6 份成孤儿（92% 被持续引用），被 architecture.md §L、distributed-broker-architecture.md §19、代码注释直接引用。 |
| `install.sh` + simcluster README | 1,241 | 部署脚本与 deploy-tier 工具的定位约束。 |
| `CLAUDE.md` 精简后 | ~350 字符行 | 工作流约束本身是必需的。 |
| **小计** | **≈30,700** | **≈40%** |

### 偶然（实现/流程方式造成，可消除）— 约 60%

| 分类 | 行数 | 可消除性 |
|---|---:|---|
| 已终结的 review / round / tasklist 报告 | ~45,000 | **不是"该删"，是"该分层"**。finding 被采纳后语义即终止；其中 17,656 行（140 份）已经零引用。归档到 `archive/<stem>/` + 一份 120 行 INDEX 即可把检索成本降一个数量级。这是**流程的自然沉积，不是任何人的错**——但沉积需要寿命管理。 |
| architecture.md 的 Part II（P0–P11 路线图） | 311 | 100% 已完成的待办清单，与 `03ff578` 归档掉的 `allgreen-remediation-roadmap.md` 同类。 |
| architecture.md §F（frp 集成） | 193 | 描述一个 `go.mod` 里不存在的依赖。取舍论证有历史价值 → 归档；当前尺应描述 `internal/tunnel`。 |
| 三册文档间的语义重复（quorum ×3、C8 表 ×2） | ~60 | 纯重复，删一处不损信息。 |
| requirements.md 的过期术语层 | ~90 | Relay / Controllerd / frpc / `~/.config/ctl/` — 换名或加 banner 即可。 |
| **小计** | **≈45,700** | **≈60%** |

### 判断

**这 76,987 行文档里，只有 ~500 行是"本可以不存在"的**（Part II + §F + 语义重复 + 过期术语）。
剩下的偶然复杂度全部是**寿命管理问题**：45,000 行 review 报告在它们被写出来的那一刻是有价值的
（它们确实驱动了修复——`internal/broker/p13_external_review_round8_test.go` 里的
`proxyOpMu` 串行化测试就是证据），只是**没人定义它们什么时候该退休**。

对比一下比例会更清楚：baseline 文档 8,801 行 : 生产代码 68,328 行 = **12.9%**。
对一个有 87 个 CLI 命令、需要 sudo 部署、要管 raft quorum 和灾难恢复的生产工具，
这个比例**偏低而非偏高**。文档量不是问题。

**真正花钱的地方是 F1**：一份 2315 行、被每个会话指定为"实现尺"、却有 40% 给出相反读数的文件。
它的成本不体现在行数上，体现在**每一次有人照它做设计的时间**。

---

## 附：如果只做三件事

1. **给 `docs/architecture.md` 加一条顶部 status banner**（≈5 行，10 分钟）——
   把"错的尺"降级为"历史档案"，同时在 `CLAUDE.md` §1 补上 `distributed-broker-architecture.md`。
   这一步单独就消除了 F1 的大部分危害。
2. **修 `cluster-runbook.md` §1 的 grow 路径**（≈40 行）——这是唯一一条会**直接导致运维操作走错路**的漂移。
3. **写 `docs/reviews/INDEX.md`**（≈120 行）——109 个工作单元一行一条，
   把 335 个文件的检索成本降到可接受。归档动作可以之后再做，索引先有。
