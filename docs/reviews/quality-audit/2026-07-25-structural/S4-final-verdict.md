# S4 — 最终裁决：tether 到底是不是屎山？

> 结构性质量审计 · 2026-07-25 · 综合 4（final-verdict）
> 输入：L01–L12 十二条限定范围 lane 的完整报告 + 本文自行复核的独立度量
> 只读。未修改任何实现代码。本文是唯一直接回答用户原问题的文件。

---

## 0. 结论（先说答案）

**不是屎山。而且差得很远。**

按可执行代码行算，tether 的生产代码是 **45,832 行**（不是 16 万，也不是 68,328——后者含 18,232 行注释和 4,264 行空行）。对这 45,832 行，我给出的三档拆分是：

| 档位 | 行数 | 占生产代码 | 含义 |
|---|---:|---:|---|
| **本质复杂度** | ~41,600 | **90.8%** | 问题域真的需要。删不掉，也不该删。 |
| **形状债（可收口的重复/错位）** | ~2,700 | **5.9%** | 不是多余的行，是抄在了 N 个地方。收口后净减但主要收益是"改一处而不是改 N 处"。 |
| **真冗余（可净删）** | ~1,500 | **3.3%** | 死代码 + 平行实现 + 死 wire 面。删了没人会发现。 |

**一句话定量判断：可安全净删的生产代码不到 3.5%，可收口的重复不到 6%，合计不到 10%。屎山的通俗定义（大量死代码、巨型函数、改一处炸全身、没人敢动）在这个仓库四条全部不成立。**

我愿意为这个判断被反驳设定的硬指标（任何人都可以复跑验证）：

- **函数尺度**：1,884 个生产函数里 **72.8% ≤ 25 行**，**只有 6 个（0.3%）超过 200 行**，最长的 `Broker.Run` 529 行 span 里只有 338 行代码、其余是逐段解释"为什么这一步必须在这个位置"的注释。屎山的第一诊断指标——巨型函数——这里不存在。
- **死代码**：RTA 全程序可达性分析（L08 复核，我独立确认了其结论方向）显示全仓 **51 个不可达函数 / 392 行 = 0.57%**；生产树 **TODO/FIXME 总共 1 处**（`cmd/tether/cluster_natsconf.go:500`，且带 issue 标签）；**0 处注释掉的代码块**。同龄同体量的 Go 代码库通常有 5–15% 死代码。
- **构建健康**：`go build ./...` 干净通过；`internal/proto` 的依赖闭包只有 `[fmt regexp strings time]`，零 module-internal 依赖——最底层的 wire 包是真叶子，不是名义叶子。

那这份审计为什么还是找出了 100 多条 finding？因为**这个仓库的债不是体积债，是索引债和收口债**。下面逐层说清楚。

---

## 1. 拆穿"16 万行"这个数字

用户问的 16 万行是 Go 树的物理行数。逐层剥开：

```
161,412  Go 物理行（用户问的"16 万"）
├── 68,328  生产（internal/ + cmd/）
│   ├── 45,832  可执行代码        ← 真正需要维护的东西
│   ├── 18,232  注释（26.7%）
│   └──  4,264  空行
└── 93,084  测试
    ├── 73,682  可执行代码
    ├── 12,079  注释
    └──  7,323  空行
```

**第一件事：16 万里有 3 万行是注释和空行。**第二件事：**58% 是测试。**真正意义上"我改了会怕"的生产代码只有 **45,832 行**——不到用户印象里那个数字的 **29%**。

（仓库还有 83,447 行 markdown 和 17,583 行 simcluster shell，但那不在"16 万"里，见 §5 的坏消息 #5。）

### 这 45,832 行买到了什么

| 子系统 | 规模（生产代码行，含注释按包算） | 是不是"重活" |
|---|---:|---|
| NAT 穿透控制面（NATS 反连 + subject 路由 + 转发语义） | broker 21.6k / agent 7.0k 的一部分 | 是 |
| NATS `auth_callout` 身份（nkey/JWT 签发 + argon2id PIN + per-session ACL） | auth 0.6k + authcallout 0.6k + session 0.6k | 是 |
| **Raft HA 集群**（自建 FSM + SQLite 状态机 + membership ops + join/retire/force-single 状态机 + 跨机 route mTLS + 离线灾难恢复） | cluster 5.3k + clusteroffline 3.0k + clusterroster 0.7k + natsconf 0.8k + broker 集群面 | **是，而且是最难的一块** |
| 文件传输（双 tier + JS ObjectStore + 路径加固 + 崩溃可恢复台账） | broker/agent/cmd 三侧 3.8k | 是 |
| 端口暴露 + 隧道数据面（yamux + 三维 fence + 公网 bind） | tunnel 1.4k + port 1.0k | 是 |
| PTY / exec / run / proxy 订阅 / SOCKS | pty 0.3k + proxydial 0.5k + proxysub 0.3k + spawnsafe 1.0k | 是 |
| 审计 + 告警 + 事件（JetStream 流 + 幂等重放 + webhook） | jsstream 0.7k + xferaudit 0.1k + broker 审计面 | 是 |
| 部署运维（install.sh / nats.conf 安全接管 / 备份恢复 / 升级编排） | natsconf 0.8k + natsreconcile 0.2k + clusterupgrade 0.2k + cmd 编排 | 是 |
| **92 条命令路径的 CLI** | cmd/tether 14.6k（cobra 样板只占 6.4%） | 是 |

### 横向对照（凭工程经验给参照系，非精确测量）

| 项目 | 大致 Go 生产量级 | 功能面对照 |
|---|---:|---|
| `bore` / `rathole` 类 | ~5k | 只做隧道。tether 的隧道面（tunnel+port ≈ 2.4k）就是这个量级。 |
| `fatedier/frp` | ~50–60k（含测试） | **只做**反向代理隧道 + 插件。没有共识层、没有身份签发、没有审计、没有 HA、没有部署编排。tether 用差不多的总量多做了 6 个子系统。 |
| `hashicorp/boundary` | ~200k+ | 目标访问代理 + 身份 + 会话录制，后端依赖外部 Postgres/KMS，不自建共识。 |
| `gravitational/teleport` | ~1M+ | 功能面与 tether 高度可比（访问代理 + 审计 + PTY + 集群），**规模是 tether 的 20 倍以上**。 |
| `nats-io/nats-server` | ~150k | tether 的依赖之一。 |
| `hashicorp/raft` | ~8k | tether 的 `internal/cluster` 在它之上又写了 5.3k 的领域状态机。 |

**结论：45,832 行对这个功能面是偏小的，不是偏大。** 最直接的对照是 frp——它只做 tether 八个子系统里的一个，代码量却相当。这不是"膨胀了"，这是"密度很高"。

诚实的反向注记：tether 的规模目标很小（现网 1 broker + 6 agent），单 raft group + SQLite，不承担 Teleport 那种多租户/多后端/合规负担。所以**不能说"tether 用 1/20 的代码做了 Teleport"**——能说的是：**它涉及的"困难子系统数量"与那些项目可比，而单位子系统的代码量显著更低。**

---

## 2. 12 条 lane 的分数怎么合成（以及为什么不能直接平均）

先摆原始分：

| lane | verdict | bloat/10 | essential% | scope 行 |
|---|---|---:|---:|---:|
| L01 broker-god-object | significant-debt | 4 | 75 | 21,618 |
| L02 cluster-package-boundaries | minor-debt | 4 | 75 | 11,166 |
| L03 cmd-cli-layer | minor-debt | 4 | 72 | 15,876 |
| L04 concurrency-structure | minor-debt | 3 | 80 | 16,862 |
| L05 error-observability | significant-debt | 4 | 65 | 5,100 |
| L06 proto-wire-debt | minor-debt | 3 | 80 | 3,384 |
| L07 test-suite-organization | minor-debt | 4 | 80 | 93,084 |
| L08 deadcode-unused | minor-debt | **2** | **97** | 68,328 |
| L09 transfer-subsystem | minor-debt | 4 | **57** | 5,611 |
| L10 auth-security-structure | minor-debt | 3 | 85 | 3,140 |
| L11 reconcile-engine | minor-debt | 4 | 85 | 6,203 |
| L12 docs-code-drift | significant-debt | **6** | **40** | 76,987 |

**为什么不能加权平均**：这些 scope 严重重叠（L08 的 68,328 = 全部生产；L07 的 93,084 = 全部测试；L12 的 76,987 主要是 markdown；L04/L05/L09/L10/L11 都横切 broker）。把 L12 的 essential=40% 拿去当代码指标，等于用"文档过时率"污染"代码质量"。

**我的合成方法**：只对**代码面**做判断，把 L12 单独归为文档面；对重叠 lane 取被覆盖代码的"最悲观读数"。得到 §0 那张三档表。

### 点名：真的重的区域

1. **`internal/broker`（21,618 行 span / 14,051 行代码）** — 唯一一个 significant-debt 的代码 lane。`Broker` 263 方法 / 45 字段（我 AST 独立复核，与 L01 完全一致）。但注意 L01 自己的结论：这是**命名空间型**而非共享状态型 God Object，263 个方法只有 6 个碰 ≥6 个字段。它重在"分解做了一半就停了"，不重在"纠缠"。
2. **`internal/agent`（7,003 行 / `Agent` 55 字段 / 106 方法）** — **这是本次审计的覆盖盲区**。`Agent` 的字段数（55）比 `Broker`（45）还多，方法 106 个，却没有任何一条 lane 以它为主体。L04 从并发角度扫过（13 把细粒度锁，判定健康），但没人做过 L01 那样的字段足迹分析。**下次审计必须补这条 lane。**
3. **`cmd/tether`（14,611 行）** — L03 证伪了"CLI 是屎山"这个假设（cobra 样板只占 6.4%），但坐实了另一件事：**25%（3,673 行）的 `package main` 文件里一个 cobra 命令都没有**，是被锁死在不可 import 层的编排状态机与协议客户端。
4. **`docs/`（83,447 行，其中 74,747 是 reviews）** — 唯一 bloat=6 的 lane，而且它是对的（见坏消息 #3、#5）。

### 点名：被冤枉的区域

1. **`internal/storage`（269 行生产 / 1,048 行测试）** — L09 逐行读完后的判词是"抽象不足但诚实"：没有 interface、没有 factory、没有 registry，三个 `Open*` 变体各对应一个真实需求。它常因为 4:1 的测试比被误认为过度设计。
2. **`internal/proto`（2,042 行 span / 956 行代码 / 86 个 wire 类型）** — 5.4 行/类型，接近 Go struct 定义的物理下限。48% 注释率在这里是资产（记录的是"NATS `*` 不能匹配部分 token 所以 bucket 必须 per-session"这类推不出来的约束）。
3. **整个测试树（73,682 行代码 / 2,112 个测试函数）** — L07 逐个打开 29 个"零断言"嫌疑测试，**全部是假阳性**；断言密度在所有热点区都是每 100 行 8–13 个。1.36:1 的 test:prod 比对这个问题域（共识 + auth_callout + 跨机 mTLS + clustered JS + 隧道 + PTY）**偏低而非偏高**。测试树的问题是命名，不是体量。
4. **注释密度 26.7%** — 抽样复核后我同意所有 lane 的判断：这些不是"复述代码"，是"记录哪次事故导致了这道门"。典型如 `internal/natsconf/remedy.go`——40 行注释解释一个字符串常量为什么必须是 SSOT，因为"三份手抄会导致晚的那份被修好、早的那份烂掉"。**删注释是这个仓库最坏的一种"优化"。**

---

## 3. 我驳回或修正的 lane 主张

lane 是限定范围的，会高估自己的重要性。以下 7 条是我核实后改判的：

**① L02 说 force-single 的 nats.conf 缺口是 `critical`、是 racknerd 事故的结构成因 → 我判定 `high`，且因果陈述描述的是已被修掉的旧代码。**
依据：`internal/broker/force_single_online.go` 确实 **0 处** `natsconf` 引用（L02 的事实对）。但 `cmd/tether/cluster_offline.go:366-367` 在 online force-single 成功后**显式打印**了 `natsconf.DeClusterRemedyCmd`（SSOT 常量）+ `--reset-js` 注记 + "cluster status 会持续显示 DATA-PLANE-DEGRADED banner 直到你做完"。`git log --diff-filter=A -- internal/natsconf/remedy.go` 显示该 SSOT 引入于 `55b1451`，即**事故后的修复**。按 simcluster 的"暴露而非弥补"铁律，"产品做不了就如实呈现缺口"是**符合设计意图的**，不是缺陷。真正未闭合的是 **L11 的读数**：prune 失败后 re-run 被 dwell 门结构性拒绝，留下永久 ghost roster 行——这与现网"删不掉的 pc732 VOTER"完全对应。**采纳 L11 的 medium 定性 + L02 的"两份实现"结构批评，驳回 critical 与因果归因。**

**② L05 说有 92 个 wire 错误码；L06 说 65；L09 说 43 → 我实测：51 个不同的字面量 code + 22 个具名常量。**
方法：`grep -rhoP '(Code|code):\s*"\K[a-z0-9_]+' | sort -u` = 51；`brokerCodeHints` 34 条、`brokerCodeExitClasses` 22 条。三条 lane 的绝对数都不准（各自统计的子集不同），**但缺口方向和量级完全一致且严重**：约 **29 个码没有 exit class**，全部落到 exit 70，而 `docs/usage.md:1542` 明确指示自动化"把 70 当可重试"。**结论不变，数字以 51/34/22 为准。**

**③ L01（13 处，high）、L05（10 个 handler prologue，medium）、L10（24 处，medium）在描述同一个缺陷，三个计数三个severity → 我判定 `high`，采用 L10 的论证。**
实测：`internal/broker` 非测试代码中 `session.IsActive` **14 处**、`IsMember` 11 处、`IsOwner` 5 处（另 `authcallout` 2 处、`subhttp` 1 处）。severity 的裁决理由是**跨 lane 组合出来的、任何单条 lane 都没说全**：L10 F1 证明了全仓 `DELETE FROM members` **只有 1 处**（且是 session 硬删的级联）、`pin_hash` **零处 UPDATE**——也就是说**没有任何吊销机制**；而 NATS JWT 一经签发带 24h TTL 且无 revocation list。**因此这 14 处手抄的应用层 gate 是全系统唯一的运行时吊销点。**L01 和 L05 都把它当"样板重复"（medium 级的形状债），只有把 L10 的事实叠上去才能看出它是 high。

**④ L12 的 bloat=6 / essential=40% 不能当作仓库代码分。**
它的 scope（76,987）主要是 markdown。作为文档面判断我完全采纳（我复核了核心证据：`docs/architecture.md` 有 **70 处** `tether.v1` 而 `internal/proto/version.go:20` 是 `ProtoVersion = 2`），但把它并进代码 bloat 会得到错误结论。**分开记账。**

**⑤ L09 的 essential=57% 是全场最低，我认为过苛，应在 ~75%。**
它自己的 counter-evidence 写着 `internal/agent/transfer.go` 的 560 行路径加固"是真正的本质复杂度，不该被简化"，也写着 `xfer_inflight.go` 的双目录 outbox "不建议直接删"（并自评 changeRisk=high）。这两块合计约 1,200 行 / 5,611 行 = 21%。**把自己判定为本质的部分算进 accidental，是内部不自洽。**

**⑥ L07（134 文件 / 18,228 行）与 L12（154 / 20,710）对"按审查轮次命名的测试文件"计数不一致 → 我实测 142 文件 / 19,627 行 / 占 499 个测试文件的 28%。**
不是实质分歧（正则口径差异），但基线数字应以 142/19,627 为准。其中 `*external_review*_test.go` **49 个**。

**⑦ L06 说 `internal/auth` 里"为避免 import cycle 而复制 subjectPrefix"的理由不成立 → 确认，理由确实是假的。**
`go list -f '{{.Imports}}' ./internal/proto` = `[fmt regexp strings time]`。零 module-internal 依赖，不存在任何环。这条我完全采纳，且它比 L06 自己给的 severity（low）更有信号价值：**仓库里唯一被 determinism lint 白名单豁免的 SSOT 偏离，理由是错的**，而白名单一开，`TestNoStrayVersionLiteral` 对该文件就完全失效了。

---

## 4. 诚实的坏消息（按严重度，5 条）

### 坏消息 1 — wire 动词与错误码没有 SSOT；三条 lane 独立命中同一个洞

**证据**：51 个不同的字面量 code 散落在 broker/agent/adminsock，`cmd/tether/error_hints.go` 的两张手工表分别覆盖 34 和 22 条。10 个命令动词（`internal/proto/subjects.go` 构造器、`internal/broker/broker.go:962` 订阅表、`internal/agent/exec.go:57` switch）三处裸字面量，无编译期关联。仓库无 `.golangci.yml`，`make lint` 跑默认集，抓不到任何一类。

**L05 判 critical、L06 判 high、L09 判 high——这是本次审计唯一被三条独立 lane 收敛命中的缺陷。**

**六个月后会怎样**：每加一个动词/错误码，缺口扩大一格。`docs/usage.md` 已经指示运维"把 70 当可重试"，于是 `dst_exists`、`too_large`、`path_outside_roots`、`argv_required` 这类**人为错误**会被监控脚本无限退避重试——tier-B 传输每次重试都要重算全文件 SHA-256，永远不会成功。而 `usage.md:1548` 已登记待做的 `transfer --json` 一旦落地，机器可读 code 与退出码会给出**互相矛盾**的可重试性判断。同时动词侧打错一个字符（`expose_rm` vs `expose-rm`）编译过、单测过，只在真跑起来时表现为 agent 的 `unknown forwarded verb` 警告 + ctl 超时。

### 坏消息 2 — 凭据生命周期只做了前半段：铸造与分发有，吊销与轮换一行代码都没有

**证据（我独立复核）**：全仓 `DELETE FROM members` **1 处**（`internal/broker/audit.go:135`，session 硬删的级联）；`pin_hash` 的 **UPDATE 零处**。而 `internal/auth/permissions.go:89/90/93` 给每一张签发的 JWT 授予了 `kick` / `rotate-pin` / `node.*.tag` 三个**没有任何订阅者**的动词权限；`docs/requirements.md` 与 `docs/architecture.md` 把它们写成已有能力；`docs/usage.md:523` 给出的替代指引（跑 `admin evict`）双重错误——它不删 member 行，且走 broker 本机 socket。

**这架空了整套设计的意义**：24h JWT TTL + 每次 CONNECT 重查 `IsMember` 存在的**唯一理由**就是"成员资格可以在下次连接时收回"。现在收不回。

**六个月后会怎样**：第一次真实的人员离职 / 设备丢失，唯一的响应是**重建整个 session + 全部 agent 重新 provision**——在 6-agent 车队上等于一次全量重装。更隐蔽的伤害是：规格与 ACL 都在说这个能力存在，所以下一个读文档的人（或下一个 AI 会话）会基于"吊销存在"做设计。**规格撒谎比缺口本身更贵。**

### 坏消息 3 — CLAUDE.md 指定的"实现尺"约 40% 给出与代码相反的读数

**证据（我独立复核）**：`docs/architecture.md` 2,315 行，其中 **70 处**写 `tether.v1`（代码是 `ProtoVersion = 2`）；整章 §F（193 行）讲一个 `go.mod` 里根本不存在的 frp（真实数据面是 in-process yamux + `tunnel.Server` 的 killGen/CloseSession，失败域完全不同）；§I.1 指向不存在的 `internal/reconcile` 和不存在的 `pkg/`；Part II 是 311 行已完成的待办清单；`docs/requirements.md`（被 CLAUDE.md 称为"唯一需求真相"）里 `ctl ` 35 处、frp 19 处、Relay/Controllerd 28 处——**全是另一个产品的名词**。

**这一条在 AI 辅助工作流下危害被放大**：CLAUDE.md 是每会话唯一强制加载的入口，它把 architecture.md 指定为实现与审查的尺（§1/§2/§5 三处），且**没有列出** `distributed-broker-architecture.md`（691 行，README 与 runbook 都称其为绑定契约）与 `deploy-tier-gotchas.md`（618 行活账本、12 条 OPEN）。于是每个新会话都被发一把 proto v1 单 broker 的尺，去量 `internal/cluster`(5.3k) + `internal/clusteroffline`(3.0k) + broker 集群面。

**六个月后会怎样**：下一次做 wire v2→v3 时，没有任何文件能给出 v2 的完整 subject 面（§B.1 只覆盖 26/40 个 `Subj*` 构造器，缺 transfer/proxy/cluster/alert/caps 五族）。而错误的设计输入不会编译失败——它会先让人把错的东西做完。

### 坏消息 4 — 两条关键不变量靠复制粘贴维持，漏改既不编译失败也不测试变红

两处形状相同、后果不同：

- **ingress 准入门**（见 §3 ③）：14 处 `IsActive` + 11 处 `IsMember` 手抄。抽象开了头就停了——`transferGate` 返回 code 不回消息、`proxyActiveOwnerGate` 自己回消息并检 `IsOwner`，两者签名不兼容，其余 8 个 handler 仍逐行手写。`broker.go:1320` 的注释自陈 register 曾经**漏过一次**。漏掉的后果是授权洞，且只在 `session rm` 的 DELETING 窗口写脏数据。
- **raft 写动词**：加一个动词要同时改 5 处（Verb 常量 / Payload 类型 / 领域 Plan 函数 / `clusterwrite.go` 路由方法 / `dispatchForward` 的 case），且**同一个 Plan 闭包被写两遍**（leader 本地一份、follower 转发后 leader 侧一份）。L01 指出今天有一处**只是巧合正确**：`freePortAllocation` 传完整 `port.Allocation` 而 `dispatchForward` 只从 5 个字段重建，恰好因为 `planAllocationStateChange` 只读那 5 个字段。给 `PlanFreeAllocation` 加一条 epoch fencing 条件（本项目已在 `PlanAllocate` 做过同类事）就会产生只在"ctl 打到 follower + 该字段非零"才触发的静默正确性回归。

**六个月后会怎样**：post-1.0 的叶子增量模式意味着**几乎每个增量都要加一个写路径或一个 session-scoped 动词**。这两处的漏改概率随增量数线性累积，而 hermetic Go 套件结构上抓不到（leader 直连的测试全绿；单机模式下 126 处 `&Broker{}` 测试字面量大多不走集群路径）。

### 坏消息 5 — 过程沉积正在破坏索引：测试按轮次命名、review 文档无索引、文件按 phase 批量创建

**证据（我独立复核）**：142 个测试文件（19,627 行 / 占 499 个测试文件的 **28%**）按审查轮次或 gap 编号命名而非按被测单元，其中 49 个叫 `*external_review*_test.go`。`docs/reviews/` 355 个 md / 74,747 行**平铺一层无索引**，L12 的引用图分析显示 140 份（26%）零引用。`git log --diff-filter=A` 对 `internal/broker` 65 个生产文件的首次引入日期做直方图：2026-06-23 一天建 12 个文件、06-26 建 10 个、06-25 建 7 个、07-21 建 7 个——**文件名编码来历而非内容**。

**最锋利的一条证据**（L07 F2，我认为是全审计信息量最高的单条 finding）：`internal/tunnel` 的同一条 fence 不变量，被 round2 / round5 / round6 三轮外审各发现一次、各建一个文件，三份代码的 diff 只有 3 行实质差异。**如果 round2 当时写成 `{verb, killFn}` 表，round5 和 round6 这两轮返工在结构上不会发生。**

**六个月后会怎样**：这是一个**自我强化循环**。"改 X 会挂哪些测试"已经答不出来（`grep -rli drain` 返回 62 个文件，真正的守卫一个都不在里面），所以新增测试无处安放，所以继续按当前轮次开新文件。git 显示这个循环已经跑了至少 4 轮。同时 74,747 行 review 是那 288 处代码注释里 `R7a` / `C4-M8` / `G69` 这类标签的**唯一解码字典**——字典没有索引，注释这份资产就在贬值。

---

## 5. 诚实的好消息（5 条，均经我独立复核）

### 好消息 1 — 函数尺度健康到可以直接反驳"屎山"

AST 实测：1,884 个生产函数，**72.8% ≤ 25 行**，16.0% 在 26–50 行，**只有 6 个（0.3%）> 200 行**。逐个看这 6 个：`Broker.Run`（529 span / 338 代码，20 段真正承重的启动 DAG，三处 `PLACEMENT IS LOAD-BEARING` 注释各钉住一次真实故障）、`newServeCmd`（297 行 = 23 个 flag 变量 + 20 次 `pickFlagOrYaml` + 一个 26 字段 Config 字面量，层次是齐的）、`ClusterAdmin.StatusReport`（272 行，**这个是真的该拆**，L01 F7 已给出方案）。**6 个里只有 1 个我同意是债。**这不是表面功夫——把 45,832 行代码维持在这个分布上，需要持续的克制。

### 好消息 2 — 死代码近乎为零，且是被工具证明的不是被声称的

RTA 全程序可达性（`golang.org/x/tools/cmd/deadcode`，从唯一 main 出发，正确处理接口动态派发）：**51 个不可达函数 / 392 行 = 0.57%**。35 个 internal 包里只有 1 个（`internal/testharness`）不在主二进制 import closure 内。生产树 **TODO/FIXME 共 1 处**、**0 处注释掉的代码**、2 处空方法体（都合法）。

**但这里有一个必须说破的虚高**：L08 指出，本仓的待办改用 `t.Skip("tracked follow-up")` 和**永不构建的 build tag** 记录（`test/c7/drill_test.go` 的 `c7_integration` 标签全仓无人引用），这些不会出现在任何 TODO 扫描里。所以"1 个 TODO"这个指标是**真的干净 + 一点点记账转移**。我仍判定这条是真强项，因为 392 行的不可达量是工具算出来的、无法被记账手法掩盖。

### 好消息 3 — wire 版本 SSOT 带自检的 lint tripwire（这个是少数派做法）

`SubjectVersionToken` 单点定义 → 40 个 builder + 7 个 parser 全派生 → `TestProtoVersionStillPositive` 断言 `token == "v"+itoa(ProtoVersion)` → `TestNoStrayVersionLiteral` 用 **AST**（扫 `*ast.BasicLit`，注释里的 `tether.v1` 不误报）禁止游离字面量 → **`TestNoStrayVersionLiteralSelfCheck` 合成一个含 stray literal 的源文件、断言扫描恰好抓到 1 个**。

最后那一步是关键。绝大多数项目的"禁止 XXX" lint 缺这一步，于是有一天正则写错、断言变空洞、没人发现。**这里做对了。**我独立确认了 `internal/proto` 的依赖闭包只有 `[fmt regexp strings time]`——最底层的 wire 包是真叶子。直接推论：**wire v2→v3 的升级代价是低的**（贵的是 subject 文法变更，两件事必须分开评估）。

### 好消息 4 — 测试是真的，不是覆盖率表演

2,112 个测试函数；L07 逐个打开 29 个"零断言"嫌疑，**全部假阳性**；断言密度在所有热点区 8–13/100 行，**没有低洼区**——9.3 万行规模上很难做到。126 处 `&Broker{}` 真实构造（不是 mock 堆）。

更有说服力的是**对抗性纪律的具体证据**：`internal/broker/r8_home_delivery_test.go:103-148` 的 `peerSilenceMonitor` 断言的是**因果**而非结果，注释明说"没有它，测试可能因为 fake agent 碰巧重新注册而通过，bug 看起来修好了但其实没有"；`g67_review_fixes_test.go:28-33` 显式拒绝了更弱的 `countingJS` 并把理由写进注释。自建的 goroutine/fd 泄漏门（`runtime.NumGoroutine` poll + fd 基线）**刻意不用 goleak** 是经过论证的自主决策而非无知——L04 复核后同意不该换，只该修容差与练习次数。

### 好消息 5 — 注释是机构记忆，且团队证明过自己会收口

26.7% 注释率（18,232 行）。抽样复核后我确认：绝大多数不是"这个函数做什么"，是"**不这么写会出什么事、哪次事故导致了这道门**"。样板级例子 `internal/natsconf/remedy.go`——40 行注释解释一个字符串常量为什么必须是 SSOT：*"Three divergent copies is how the late one gets updated and the early one rots."*

而且**团队证明过自己会去重**：`natsconf.MoveAsideJSStore` 被提取一次给 4 个调用方共享；`remedy.go` 把三份手抄的运维处方收成一个常量；`reconcile_registry.go` 是教科书级抽取（带 one-vote-veto 不变量论证 + 假时钟等价性证明 + 零 goroutine 零 timer 以避开自家泄漏门）；`internal/clusterupgrade` 是从 451 行胖 orchestrator 里抠出的纯决策核心。**这不是能力问题——机器造好了，只是没接到每一条线上。**

`test/simcluster/README.md` 顶部的四条 Mandate（"绝不迎合 tether 的错误设计"/"有问题就暴露、绝不替 tether 弥补"/"界限分明"/"判定反转：靠复杂脚本才成功说明 tether 失败了"）配 `[GAP #N]` 标注机制，是我在同类项目里很少见到的自我约束——17,583 行 shell drill 有这样一份 mandate 压着，体量是正当的。

---

## 6. 归因：这些债是怎么来的

先摆时间线，因为它解释了大部分现象：

```
2026-04-18  Initial commit
2026-07-25  当前（v0.4.7，现网 1 broker + 6 agent）
─────────────────────────────────────────────
   ~14 周 · 186 个 commit · 161,412 行 Go + 83,447 行 md + 17,583 行 shell
   ≈ 每月 48,000 行 Go
```

**这个速率对单人团队只有一种解释：AI 辅助开发。**而本次审计发现的债务画像与这个成因**逐条吻合**：

**(a) AI 擅长的地方 → 全部是本审计的强项。** 短函数（72.8% ≤25 行）、无 TODO（1 处）、无死代码（0.57%）、注释详尽（26.7%）、测试厚实（2,112 个函数、零空绿）。这些指标在人类高速交付的代码库里通常是相反的。

**(b) AI 不擅长的地方 → 全部是本审计的债。**
- **复制优先于抽象**：LLM 会在上下文里重新推导一遍惯用法，而不是去仓库里找现成的 helper。结果就是 `hashToken` 有 3 份（`tunnel` / `proxysub` / `port`——上一轮 audit 的 F10 决定"用注释解决而非提取"，此后**又长出第三份**）、drain barrier 协议写了 2 遍、`Bind+ServeListener` 抄了 3 份且 shutdown 语义已经漂移（两个优雅排空、一个硬关）、终态处置序列手抄 4 遍。
- **新建文件比合并进既有文件"更安全"**：对 LLM 而言开新文件是零冲突操作，合并要读懂既有文件。git 直方图（一天建 12 个文件）与 142 个按轮次命名的测试文件都是这个偏好的化石。

**(c) 7 步流程的结构性副作用——这一条我认为是最主要的成因。**
CLAUDE.md §3 的流程是 `plan → 实现 → 多专家审查 → 主进程修复 → 外审 → commit`。**这个流程每一步都产出新工件，没有任何一步是"收口"**：step 4 明确授权专家"可自行新增测试条目"，step 6 产出一份新的 external-review 文档，但没有任何步骤要求"把本轮新增的测试按被测单元归位"或"把本轮发现的同类缺陷收成一张表"。于是流程是**单调递增**的。

最干净的证明就是 L07 F2 那三个 tunnel fence 文件：**同一个流程连续三轮抓到同一类 bug，每一轮都正确修好了实例、每一轮都没有修类。**这不是执行不力——这是流程里没有那一步。

**(d) 问题域的必然。** 45,832 行里我判定 90.8% 是本质复杂度，这个 floor 是真实的：NAT 穿透 + 共识 + 身份签发 + 部署接管，每一样都是分布式系统里的重活。`internal/agent/transfer.go` 那 560 行路径加固（逐段 `O_NOFOLLOW` → `Fstatat` 预检 → `Openat` → dev/ino 复核 → `dirFDStillNamesPath` → `Linkat`）不能被"简化"——每一步对应一个具体攻击。

**克制的总结**：这不是能力问题（§5 好消息 5 的六处成功抽取是反证），主要是**流程只增不减**加上**AI 的复制偏好**在 14 周高速交付下的沉积。修流程比修代码重要。

---

## 7. 如果只做三件事

> **① 把 wire 动词与错误码提进 `internal/proto` 常量，并加一条 AST guard test（约 40 行）。**
> 一次性关掉本次审计中**唯一被三条独立 lane 收敛命中**的缺陷类：guard test 扫描生产代码里赋给 `Code:` 字段的字符串字面量集合，断言 ⊆ `brokerCodeHints ∪ brokerCodeExitClasses` 的键集。**字符串值一个都不改 ⇒ 不动 `ProtoVersion` ⇒ 现网无需重装。**这条测试今天就会红（约 29 个码缺 exit class），这正是它的作用。

> **② 修那把尺：给 `docs/architecture.md` 顶部加 10 行 status banner（"§A–§K 是 proto v1 单 broker 历史基线，v2/集群面的绑定契约在 distributed-broker-architecture.md"），并在 CLAUDE.md §1 补上 `distributed-broker-architecture.md` 与 `deploy-tier-gotchas.md` 两行地图。**
> 全仓性价比最高的一处修改——因为它修的不是代码，是**每个未来会话的输入**。在 AI 辅助工作流里，一份 40% 错读的"实现尺"每天都在制造新的偏差；机械订正那 70 处 `tether.v1` 可以稍后，但 banner 今天就该加。

> **③ 给 §3 的 7 步流程加第 8 步"收口"：本轮新增的测试按被测单元改名归位、本轮发现的同类缺陷收成一张表（而不只是修实例）、review 报告移入 `docs/reviews/archive/` 并把最终裁定回写进 plan。**
> 这是唯一能阻止债务再生的一条。前两条修的是存量，这条修的是**产生存量的机制**——L07 F2 那三轮 tunnel 返工，是这一步缺失的直接代价。

**另外一件不是重构、但需要你（而非 agent）拍板的事**：坏消息 2 的凭据吊销缺口。它不是结构债，是**产品缺口 + 规格撒谎**。最低成本的止血是 3 行——删掉 `internal/auth/permissions.go:89/90/93` 那三条指向不存在订阅者的授权，并把 `requirements.md` / `architecture.md` 里的 `kick` / `rotate-pin` 从"能力"降级为"未实现"。**先让规格说真话，再排期实现 `session kick`。**

---

## 8. 指标基线表（供下次复查对照）

> 所有数字由本文在 2026-07-25 于 `HEAD = 84bf030` 独立测得，方法记录在每行括号内，可机械复跑。

### 8.1 总量

| 指标 | 值 | 方法 |
|---|---:|---|
| Go 物理行（用户口中的"16 万"） | **161,412** | `find -name '*.go' \| xargs wc -l` |
| — 生产物理行 | 68,328 | 排除 `*_test.go` |
| — 生产**可执行代码**行 | **45,832** | 逐行分类 code/comment/blank（含块注释状态机） |
| — 生产注释行 | 18,232 (26.7%) | 同上 |
| — 生产空行 | 4,264 | 同上 |
| — 测试物理行 | 93,084 | |
| — 测试可执行代码行 | 73,682 | |
| test : prod（代码行） | **1.61 : 1** | 73,682 / 45,832 |
| markdown 行 | 83,447 | `docs/**/*.md`（其中 `docs/reviews/` 74,747） |
| simcluster shell 行 | 17,583 | `test/simcluster/**/*.sh` |
| go.mod 直接依赖 | 19（+25 indirect） | |
| commit 数 / 项目年龄 | 186 / ~14 周 | `git rev-list --count HEAD` |

### 8.2 生产包分布（前 12）

| 包 | 生产行 | 测试行 | test:prod |
|---|---:|---:|---:|
| `internal/broker` | 21,618 | 19,895 | 0.92 |
| `cmd/tether` | 14,611 | 13,075 | 0.89 |
| `internal/agent` | 7,003 | 5,205 | 0.74 |
| `internal/cluster` | 5,300 | 5,239 | 0.99 |
| `internal/clusteroffline` | 2,959 | 3,553 | 1.20 |
| `internal/proto` | 2,042 | 2,370 | 1.16 |
| `internal/tunnel` | 1,413 | 1,551 | 1.10 |
| `internal/cli` | 1,265 | 687 | 0.54 |
| `internal/adminsock` | 1,245 | 394 | 0.32 |
| `internal/spawnsafe` | 1,029 | 863 | 0.84 |
| `internal/port` | 978 | 1,295 | 1.32 |
| `internal/natsconf` | 820 | 955 | 1.16 |

### 8.3 复杂度分布（AST，1,884 个生产函数）

| 函数长度 | 数量 | 占比 |
|---|---:|---:|
| ≤ 25 行 | 1,371 | **72.8%** |
| 26–50 行 | 301 | 16.0% |
| 51–100 行 | 150 | 8.0% |
| 101–200 行 | 56 | 3.0% |
| **> 200 行** | **6** | **0.3%** |

**最长 6 个**：`Broker.Run` 529 / `newServeCmd` 297 / `ClusterAdmin.StatusReport` 272 / `newRunCmd` 271 / `Agent.handleRunForwarded` 225 / `driveAdd` 208。

### 8.4 类型规模（前 6）

| 类型 | 方法数 | 字段数 |
|---|---:|---:|
| `internal/broker.Broker` | **263** | 45 |
| `internal/agent.Agent` | 106 | **55** |
| `internal/broker.ClusterAdmin` | 67 | 24 |
| `internal/cluster.Node` | 36 | — |
| `internal/broker.Config` | — | 43 |
| `internal/adminsock.Request` | — | 40 |

### 8.5 健康度 / 债务指标

| 指标 | 值 | 判读 |
|---|---:|---|
| 生产 TODO/FIXME | **1** | 优（见 §5 好消息 2 的记账转移注记） |
| 不可达函数 / 行 | 51 / 392 (0.57%) | 优 |
| 生产注释掉的代码块 | 0 | 优 |
| `go` 语句（生产） | 76 | 低密度，优 |
| mutex：`Broker` / `Agent` | 8 / 13 | 全部细粒度、无全局大锁 |
| `context.TODO()` | 0 | 优 |
| 测试函数数 | 2,112 | |
| 零断言测试（复核后真阳性） | 0 | 优 |
| **不同的 wire 错误码字面量** | **51** | 其中 34 有 hint、**22 有 exit class** ⇒ 约 29 个静默退 70 |
| `session.IsActive` 手抄点（broker） | 14 | +`IsMember` 11、`IsOwner` 5 |
| `DELETE FROM members` | **1**（级联） | **无吊销机制** |
| `pin_hash` UPDATE | **0** | **无 PIN 轮换** |
| `&Broker{}` 测试字面量 | 126 | 重构 blast radius 的确切上界 |
| `hashToken` 重复实现 | **3** | tunnel / proxysub / port |
| 按审查轮次命名的测试文件 | **142 / 19,627 行**（占测试文件 28%） | 其中 `*external_review*` 49 个 |
| `docs/reviews/` 文件 / 行 | 355 / 74,747 | 无索引，L12 测得 26% 零引用 |
| `architecture.md` 里 `tether.v1` | **70**（代码 `ProtoVersion = 2`） | 尺歪了 |
| `.golangci.yml` | 不存在 | 跑默认集 |
| `go build ./...` | 通过 | |

### 8.6 下次复查该看什么变了

按优先级，这 6 个数字如果朝好的方向动，说明结构在改善（而不只是功能在增加）：

1. **22 → 51**（有 exit class 的错误码 / 总错误码）——坏消息 1 是否闭合。
2. **142 → ?**（按轮次命名的测试文件数）——坏消息 5 / 流程第 8 步是否落地。
3. **70 → 0**（`architecture.md` 里的 `tether.v1`）——坏消息 3 是否闭合。
4. **14 → 1**（broker 里 `IsActive` 手抄点）——坏消息 4 上半是否收口。
5. **0 → ≥1**（`pin_hash` UPDATE 或 member 吊销路径）——坏消息 2 是否有产品动作。
6. **45,832 / 0.3%**（生产代码行 / >200 行函数占比）——这两个数字**不该显著变差**；若生产代码行增长快于功能增长、或 >200 行函数占比翻倍，说明本报告的好消息 1 正在失守。

---

## 附：一句话交付给用户

**16 万行里只有 45,832 行是需要维护的生产代码，其中 90.8% 是这个问题域真的需要的；可安全净删的不到 3.5%，可收口的重复不到 6%。函数尺度（72.8% ≤25 行、仅 6 个 >200 行）、死代码率（0.57%）、TODO 数（1）三项硬指标都排在同类 Go 项目的前列。它不是屎山——它是一个在 14 周里以 AI 辅助速率长到 4.6 万行、分解做了一半、索引开始跟不上的高密度系统。真正的债是三样东西没有 SSOT（wire 错误码 / 准入门 / 那把叫 architecture.md 的尺），以及一个只增不减的 7 步流程。**
