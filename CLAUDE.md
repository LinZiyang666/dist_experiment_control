# tether — 项目工作说明（CLAUDE.md）

> 本文件每个会话加载进上下文，定义**怎么推进本项目**。需求/架构/用法细节在 `docs/`，本文件不重复。
> 工作流移植自 `auto_daemon`，按 tether（Go-only、已发布、phase 序列推进）的实际情况改写。

## 1. 项目与文档地图

- 一句话：**"SSH + 端口暴露" 的 NAT 穿透控制面**——NAT 后的 agent 经 NATS 反连公网 broker，使用者（ctl）经同一 NATS 把命令路由到 agent。三角色同一二进制 `tether`，子命令切换。
### 文档权威链 —— **冲突时上位覆盖下位**

| 层 | 文档 | 权威范围 |
|---|---|---|
| 1 · WHAT | `docs/requirements.md` | **需求**的唯一真相；不描述当前实现 |
| 2 · HOW（当前） | `docs/distributed-broker-architecture.md` + `docs/deploy-tier-gotchas.md` | **实现**的绑定契约 |
| 3 · HOW（历史） | `docs/architecture.md` §A–§K | 只提供**当初为何这样取舍**；标识符与拓扑细节已过时，**不作实现依据** |

**第 3 层从不覆盖第 2 层。** 但 `architecture.md` 的**「里程碑映射」「关键依赖警告」「每进入新 phase 的 checklist」**仍然有效，§2/§3 依赖它们。

其余文档：`usage.md`（使用者手册）、`broker-ops.md`（broker 运维）、`cluster.md` + `cluster-runbook.md`（HA）、
**`testing-standards.md`（怎么写测试——§5 讲跑哪些，它讲怎么写）**、`devices.md`（设备清单）、
`devices-ops.local.md`（凭据/资源）、`reviews/`（各 phase 的 plan 与审查报告）、
`test/simcluster/README.md`（deploy-tier drill 的 Mandate 与用法）。

## 2. 工作单元：一次一个 phase

- 按 `architecture.md`「里程碑映射」的 **P<N> 序列**推进，**一次只做一个 phase**。主线 P0–P11 已带到 **v0.1.0**；其后改为 **post-1.0 叶子增量**模式（各自独立 plan→实现→内审→外审，不在线性 P 序内、不阻塞主线），P12（expose `--remote-port`）/P13（proxy 订阅）等均按此走。当前版本以 `git tag --sort=-v:refname | head -1` 为准——本文不写死版本号，写死的必然腐化。
- **新工作**：除非用户明确要延续线性里程碑（则取下一个未做的 P 号），否则一律当作**新的叶子 feature 增量**——范围先与用户敲定，再按 §3 的 3 阶段 7 步开工。
- 依赖"**先父后子**"：任何 phase 只用已完成的前序产物，绝不超前——严格遵守 `architecture.md`「关键依赖警告」里的不可跳序约束。
- 每进入新 phase 先过 `architecture.md`「每进入新 phase 的 checklist」（前一 phase 出口断言全过、翻状态、**实现中发现设计问题先改文档再改代码**、单测+e2e 同一次提交落盘）。

## 3. 每个 phase 的工作流（3 阶段 · 7 步）

### 阶段 A — 规划
1. **多专家对抗性草拟 plan（用 Workflow 工具）**：主进程**为当前 phase 现场草拟一个 workflow 脚本**（按该 phase 的范围/风险定制 fan-out），并行多个不同视角的专家起草 → 对抗性互评 → 综合出候选 plan。
2. **主进程审核并修改 plan**：主进程是 plan 的**唯一定稿人**；定稿写入 `docs/reviews/p<N>-plan.md`。

### 阶段 B — 实现
3. **主进程按 plan 编写代码 + 测试**：连续块；遵守 §5 约定与 `architecture.md` 的不变量。

### 阶段 C — 审查与收尾
4. **多专家对抗性审查代码（用 Workflow 工具）**：主进程为当前 phase 现场草拟一个审查 workflow 脚本，并行多视角专家对抗性审查。**专家只读实现、可自行新增测试条目，但绝不修改实现代码。** 报告写入 `docs/reviews/p<N>-review.md`（多轮则 `-round2`/`-round3`）。
5. **主进程评估审查正确性并修改**：逐条采纳/驳回 finding；整合专家新增的测试；**只有主进程能改实现**。
5b. **测试归位**：本轮新增/整合的测试文件**按被测单元命名**（`tunnel_fence_test.go`），
   **不允许**新建 `*_external_review_*_test.go` / `*_round<N>_*_test.go` / `p<N>_*` / `b<N>_*` / `g<N>_*`
   这类**按开发过程事件**命名的文件。审查者的 finding 写成测试函数上方的一行
   `// origin: p13 external review round 6 F2`——它扛得住改名，而文件名扛不住：
   下一个改 `CloseProxy` 的人 grep 不到 `p13_external_review_round2_test.go`，
   于是同一个不变量每轮审查被重新发现一次（`internal/tunnel` 曾有**四个**文件测同一条 fence，
   round2 抓到 `CloseProxy` 漏 fence、round5 抓到 `CloseSession` 同样的洞、round6 抓到 `ForgetSession`——
   **若 round2 就写成 `{verb, killFn}` 表，round5/round6 这两轮返工在结构上不会发生**）。
   由 `test/determinism/test_naming_test.go` 的冻结门在 `make test` 里拦下。该门管**三样**：
   测试文件名（存量 158 个已全部改名，`legacy_process_named_list.go` 的账本**已排空**、由 `published=0` 钉住）、
   **测试函数名**（存量 **439** 个站点在 `legacy_process_named_funcs.go` 的递减账本里，
   key 是 `路径: 函数名` 而非裸名——裸名会让每条变成可复用通行证，且三个名字各存在于两个包、
   被折叠成一条，所以旧数字 436 既放行未来站点又少报当下站点）、
   **生产文件名**（零账本——落门时全仓只有 1 个违规，已改名）。
   两本账本都只减不增：**账本里已不存在的条目会让门变红**，所以它们不会腐化成永久豁免。
6. **外部审查（用户本人）**：提交给用户做最终人工外审；用户出报告 `docs/reviews/p<N>-external-review.md`，主进程评估后**在报告文件内逐条回复**并修改。**外审不过不算 done。**
7. **phase 结束**：先**归档**再提交，两步都做完才算结束。
   - **归档判据（一半机械、一半是判断，两半都说清）**：`docs/` **顶层**只放**活文档**——即"下一次改代码时还会被读"的基线。
     - **机械的那半有闸门**：`test/architecture/docs_layout_test.go` 拦下任何**已跟踪**的
       `docs/*-plan.md` / `*-review*.md` / `*-tasklist.md` / `*-roadmap.md`。文件名带这四个后缀
       就是过程产物，不需要判断。`docs/cluster-ha-realmachine-test-plan.md` 在 `.gitignore` 里、
       未被跟踪，故不在此列——这是**明确裁定**，不用动它。
     - **另一半是判断，不假称机械**（原文写「机械，不靠临场判断」而给出的可操作形式是
       「问下一次改代码的人需不需要读它」——那就是判断）：文件名不带后缀但内容已经完结的文档，
       靠人读。这一条整个曾经**没有任何闸门**，于是 `batch-a-roadmap.md` 在 `03ff578` 正确归档过一次之后又躺回了顶层。
     一份文档一旦它描述的工作已经完成，它就不再是基线而是过程产物，`git mv` 进 `docs/reviews/`
     （本轮的 `*-plan.md` / `*-review*.md` / `*-tasklist.md` / 已收尾的 `*-roadmap.md`）。
     **只重指活文档的引用**（`docs/` 顶层 + `CLAUDE.md` + `test/simcluster/README.md`）；
     `docs/reviews/` 下的冻结报告**不追溯改**，与上面「存量的轮次标签不追溯改」是同一条原则。
     （原文写「把**所有**引用一并重指」，实测既不可执行也不该执行：`d8_alerts.go` 那次改名在
     `docs/reviews/` 留下 15 处旧名，追着改等于把历史记录改成不再是当时看到的样子。
     一条要么逼人改几百份冻结报告、要么被违反的条款，是一条没有约束力的条款。）
     可操作形式：**问「下一次改代码的人需要读它吗」——不需要就归档。**
   - 在 `docs/reviews/INDEX.md` 追加一行（日期 / 一句结论 / plan 路径）。
   - `git commit` + `git push`（见 §6）。
   **不归档不算 phase 结束**——沉积的成因是"报告与基线平铺同级、无人敢批量归档"。
   这条**必须是条款而不是一次清扫**：`03ff578` 做过一次正确的归档，但没成条款，于是批 A 完成后
   `batch-a-roadmap.md` 又躺回 docs/ 顶层，同一沉积在同一位置重新长了出来。

> **Workflow 不预置固定文件**：步骤 1、4 的多专家编排**每个 phase 自己即时草拟脚本**（用 Workflow 工具的 inline `script` 跑），不维护复用的 `.claude/workflows/` 文件。fan-out 的专家维度按当前 phase 现定。每个 phase 完成后**停下等用户外审/确认**再进下一个。
>
> **Workflow 模型硬约束**：所有 `agent()` 调用（drafter / critic / synth 等任意 subagent）**一律省略 `model`**，
> 从而继承会话主模型；`meta.phases[].model` 同样不设。**禁止 Haiku、禁止 Sonnet。**
> 本条**不写具体版本号**——写死的型号每次模型升级都会过时，这条指令本身就曾写死当时的最新型号并在升级后
> 误导了每个新会话。规则是「继承 + 具名排除弱模型」，不是「版本下限」。
> 若误用了被排除的模型跑出结果，**弃用并重跑**（resume 时改 `model` opt 会让缓存失效、自动重跑）。
> fan-out 的 agent 数为**静态常量**，不由上一阶段的产出内容决定；**条件跳过同样算动态数量**——
> 固定宽度阶段的每条 lane 必须无条件 spawn，输入为空时让该 agent 返回空结果即可。

## 4. 角色边界（不可越界）

| 谁 | 能做 | 不能做 |
|---|---|---|
| **主进程** | 定稿 plan、编写/修改实现、采纳审查、整合测试、commit/push | — |
| **专家**（workflow 内 agent） | 草拟 plan 建议（step1）；审查 + **新增测试**（step4） | **改实现代码**、定稿 plan、commit |
| **外部审查**（用户本人） | 独立人工外审、出报告 | 改代码 |

## 5. 编码与测试约定

- **语言**：团队讨论用中文；**代码 / 标识符 / 注释 / commit / 日志 / 错误串一律英文**。
- **Go-only**：`CGO_ENABLED=0` 静态二进制；工具链锁 **Go 1.25**（由 `nats-io/jwt/v2 ≥ v2.8.1` 拉高，**升级依赖前必验 go directive**）。
- **不变量**：控制面/数据面分离、auth_callout nkey 身份、G.1/G.2 reconcile、proto wire 版本一致性、session ACL 隔离、port 分配带 …。
  **按 §1 权威链取尺**（外审 R6 订正：这里原写"以 `architecture.md` 为准…都以它为尺"，与 §1"第 3 层不作为实现依据"直接冲突）——
  集群面/v2 的不变量以 `distributed-broker-architecture.md` + `deploy-tier-gotchas.md`（第 2 层）为准；
  `architecture.md` §A–§K 只提供**当初为何这样取舍**的论证，其标识符与拓扑细节已过时，不作实现依据。
- **wire 协议**：`internal/proto.ProtoVersion` 是 SSOT。**N-1 兼容窗口**（requirements §6.7 +
  architecture §21）：相邻 release 间一切 wire 变更必须 additive/omitempty 且零值合法（append-only
  账本闸门在守）；**ProtoVersion bump = 纪元更替**，是唯一被许可的兼容断裂——必须重装而非 upgrade，
  且 bump 者背双 subject 树订阅义务（`tether.v(N)` + `v(N-1)`，见 §21.4）。
- **`context.Background()` 只在两处合法**（S3 §2 裁决 5 采纳的那一半；`contextcheck` 被判 REJECT-FOREVER，
  所以这条靠人读，站点注释就是它的载体）：
  1. **进程/循环的根**——`main`、一个后台 loop 的起点、一个 offline 工具的顶层。
  2. **调用方结构上没有 ctx**——nats.go 的 `MsgHandler(func(*nats.Msg))`、cobra 命令外的 helper、
     raft FSM 的 `Apply`（那里 ctx 取消会破坏确定性，见 `.golangci.yml` 设计约束 3）。
  其余每一处都该接上游的 ctx。**新增站点必须在那一行写明落在哪一类**——
  `// ctx-root: 后台 loop 起点` 或 `// ctx-none: nats.go MsgHandler 无 ctx`。
  存量站点在 `test/architecture/legacy_ctx_background_sites.go` 的递减账本里（2026-09-01 落门时 40 处），
  改到那一行时补标注并删账本行——"顺手补"曾经只是一句话，六周里站点从 39 涨到 44、补了 1 处。
- **文件与注释纪律**：
  - **文件按职责命名，不按开发过程事件命名**——生产文件与测试文件同此规矩
    （`alert_gate.go` 不是 `d8_alerts.go`；`tunnel_fence_test.go` 不是 `p13_external_review_round6_test.go`）。
    新代码优先并入职责匹配的既有文件，而不是新开一个以本轮批次命名的文件。**测试函数名同样受此约束。**
  - **溯源用稳定锚点**：写 `// origin: p13 external review round 6 F2` 放在被溯源的那个测试/守卫上方——
    它扛得住改名，而文件名和函数名都扛不住。**存量的轮次标签不追溯改。**
  - **注释是资产，任何重构必须整段搬运。** 本仓**生产 Go 源码**（`cmd/` + `internal/`，不含测试与文档）
    有 29–33% 的行是注释，记录的是「为什么」（事故、外审轮次、
    不变量论证）。`.golangci.yml` 故意不启用 `funlen` / `lll` / `gocyclo` / `gocognit` / `cyclop`，
    就是为了不让行数闸门反向激励删注释——**改那份配置前先读它头部的四条设计约束**，
    其中第 1 条记着一个实测反例：`maintidx` 并**不**像它曾被宣称的那样对注释免疫
    （给 `handleGrowTrigger` 只加 20 行注释、CC 与 Halstead 不变，MI 就掉 1 分），
    所以它的豁免按**函数名**而不是按 MI 数值匹配。
- **闸门清单**（每条都是「机械可判 + 有变异验证」的守卫，不是风格建议）：

  | 闸门 | 位置 | 管什么 |
  |---|---|---|
  | lint 三层 | `.golangci.yml` | T1 零基线正确性／T2 结构棘轮（带理由 `//nolint` 落基线）／T3 `nolintlint` |
  | 结构预算棘轮 | `test/architecture/structural_budget_test.go` + `testdata/` | **四维**：类型方法数／包文件数／包**代码行**（剥注释、量化 2000）／`cmd/tether` 非 CLI 代码行（剥注释、量化 100）。第三维是内审补的——前三维对「在已有文件里长大」全盲，而那是阻力最小的路 |
  | 分层规则表 | `test/architecture/layering_test.go` | 谁不许 import 谁（单表，加规则=加一行）；带 `originalUnion` 收据，四份被删的 regression 文件的每条子句都可机械反查 |
  | TLS 配对门 | `test/architecture/tls_verify_pairing_test.go` | `InsecureSkipVerify:true` 必须配 `VerifyPeerCertificate`／`VerifyConnection`，或进带理由的账本；**站点数精确钉死**（今天 4），新增一处即使本身合规也变红——就是要逼人去读 |
  | nolint 指令自检 | `test/architecture/nolint_directive_test.go` | 每条 `//nolint` 必须点名一个**本仓真启用**的 linter（点名未启用的 linter 等于什么都没抑制，却读作「这是已知情的例外」）|
  | build-tag 编译闸 | `make vet-tags` + `test/architecture/build_tags_test.go` | 隐身套件必须还能编译；**tag 列表从源码提取并与 Makefile 双向对账**；**tag 局部性**：`_integration` tag 门控的文件只许落在自己的 `test/<dir>`（例外账本 1 条：`internal/broker/phasefluidity_lifecycle_test.go`），门控文件总数**精确钉死**（今天 23）——这是矩阵去重「共享包在各 tag 下闭包全等」前提的守卫，往 `internal/proc` 放一个 `//go:build d5_integration` 文件会让 D5 的二进制与 D4 不同而无人察觉 |
  | T3 前提 | `test/determinism/leader_premise_test.go` + `legacy_leader_premise_sites.go` | 测试里裸读 `.IsLeader()` / `.State() == raft.Leader`——不在**身份已证明**的轮询 helper 的谓词闭包里（外部原语按 import 路径信任：`clusterharness.WaitForCond`/`WithLeader`、`testharness.WaitFor`；本包 helper 须由 `verifiedPollingHelpers` 从其实现证明"deadline 循环里反复求值谓词或转发给已证明原语"，同名一次性 shadow 不算——外审复审 R1）、也不是**body 只等待**的循环条件（body 里任何赋值/发送/业务调用/非 continue 分支都算对刚读到的 premise 行动——外审 F2 + R1）——即红，除非在 `path: func` 递减账本；helper 是 `test/clusterharness.WithLeader`（观测→行动→再观测，移动即整段重来；`test/d3` 的 follower PIN 写是第一个调用方）。「观测领导权然后假设它不变」是 parallel-flake-rootcause 根因 2，第二次被修错两回 |
  | 命名冻结 | `test/determinism/test_naming_test.go` | 测试文件名／测试函数名／生产文件名（见 §3 step 5b）；**文件首注释里的文件名必须等于 basename**（158 文件改名后 59 个头还写着旧名，2026-09-01 逐个改正为 `x_test.go (formerly old_test.go)`，此后账本为 0） |
  | test tree 地图 | `test/architecture/test_layout_map_test.go` | `test/README.md` 的目录表与 `test/` 顶层目录**双向对账**；表里点名的矩阵函数与 build tag 必须真实存在；`p*/d*` 目录冻结为**精确集合 18**（不迁不增，新目录按主题命名；裁决见 plan §0 A2） |
  | ctx 站点冻结 | `test/architecture/ctx_background_ledger_test.go` + `legacy_ctx_background_sites.go` | 生产源每处 `context.Background()` 要么同行/上一行带 `ctx-root:`/`ctx-none:`，要么在 `path: func` 递减账本（初值 40）。上面那条"存量 39 处改到那一行时顺手补"是没有机制的承诺：从 39 涨到 44、只补了 1 处 |
  | 闸门标准 | `test/architecture/gate_standards_test.go` + `legacy_gates_without_controls.go` | 本表点名的每个门文件必须有 `// gate-control: TestXxx` 锚点指向**同文件**的正负控制测试（G1 的薄机械形式），否则在递减账本（初值 13）；每本 `legacy*` 账本必须登记键单位 ∈ {file, site, promise}（G3） |
  | 就绪 sleep 冻结 | `test/determinism/sleep_barrier_test.go` + `legacy_sleep_barriers.go` | `startBroker*/startAgent*/seedSession*` 函数体内的裸 `time.Sleep` 屏障（不含轮询循环、func literal）冻结在 `path: func` 账本（初值 19）；`// sleep-fixture: <理由>` 豁免；替换目标是测试真正依赖的可观测量（`WaitNodeOnline` / 对 broker subject 的短超时 `nc.Request`），**不是** `WaitConnect` |
  | 测试身份清单 | `test/determinism/test_inventory_test.go` + `testdata/` | 每个 Test/Fuzz/Benchmark 函数以 `path: Name` 记入**只增不减**的 golden；`-update-test-inventory` 只追加、拒删——删/改名测试必须手改 golden 并在 commit message 写理由。**测试代码重构没有测试保护**：layering 四合一曾丢一整行 + 一条子句仍全绿、promised-guard 一装上抓到 34 条死承诺、runner 曾静默少跑 11 个套件——这份清单是所有后续测试重构（harness 吸收、矩阵去重、fuzz 目标）的前置收据（`docs/testing-standards.md §六`） |
  | 确定性 lint | `test/determinism/`（时序守卫在 `test/determinism/raft_timing_guard_test.go`） | raft Apply 可达性、版本字面量 SSOT、docs wire 版本（扫描面含 `test/simcluster/README.md`）、时序守卫（raft 计时门识别**复合字面量与赋值两种形式**——第一版只看 `KeyValueExpr`，`internal/cluster/prevote_test.go` 与 `transport_test.go` 共六处亚秒赋值从未被看见，现已豁免登记；**产品计时 sleep 门**：同一测试函数里既引用 `leaseGrantWindow/probeTTL/DefaultLeaseGrace…` 又 `time.Sleep(纯字面量)` 即红——`leaseGrantWindow` 从 1s 改 5s 后两处按旧值标定的注释六周没人发现）、`// origin:` 指向真实文档、promised-guard（注释里点名的测试必须存在，含 `[deleted]`/`[example]` 标记的上限与诚实性守卫）|
  | 枚举 switch 的 `default:` | `test/determinism/enum_switch_default_test.go` | 15 个自有枚举家族：`default:` 只在**每个成员都被列举**时允许——`exhaustive` 的 `default-signifies-exhaustive:true` 让一个裸 `default:` 永久瞎掉该 switch（实测：注入 batch-C 那个 doctor 缺陷，加 default 后零报告）。`TestEnumFamiliesCoverEveryIotaEnum` 反向对账，防止家族表退回 3/15 |
  | wire 错误码 ↔ exit class | `cmd/tether/` 4 处 | 每个码都有归类，不静默退 70；**双向**（含表键反查发射点） |
  | ACL ↔ 订阅表 | `internal/auth/acl_reconcile_test.go` | 双向：无订阅者的授权、无授权的订阅都报 |
  | wire 字段清单 append-only | `internal/proto/wire_inventory_test.go` + `testdata/` | N-1 窗口的机械半（requirements §6.7 / architecture §21.2）：每个导出消息结构的 {字段名, tag, 类型} 只增不删不改；updater 拒绝收缩——收缩是纪元决策，必须手改账本并在 commit message 写明理由 |
  | CLI 表面 golden | `cmd/tether/command_tree_inventory_test.go` | 命令/flag/Hidden 位漂移即红 |
  | 泄漏门 | `test/concurrency/helpers_test.go` + `test/determinism/leak_assert_shape_test.go` | NumGoroutine + fd 基线，**刻意不用 goleak**；断言形状（≥5 轮 exercise，`leakExerciseAnchors` 表钉住每条断言在循环里的那个调用）；**风险包覆盖闸**（`TestLeakGateCoversRiskyPackages`：非测试文件 import raft/yamux/tunnel/pty 的包必须有 `Test*` 函数体内直接调用共享泄漏 helper——死 helper 里的调用不算；例外表带 cap）；`-race` 归属由「矩阵单元清单」行守 |
  | seedSession 收据 | `test/stackharness/seed_test.go` | 8 个单机套件的 `seedSession` 必须仍是转发器（`absorbedSeedSession` 表 + 反向断言）；`test/stackharness` 只许被 `_test.go` / `test/` 树 import（layering 反向扫描）。它在 `make gates` 里真跑——2026-09-01 前只在 `make test` 里 |
  | docs 布局 | `test/architecture/docs_layout_test.go` | 已跟踪的过程产物（`*-plan/review/tasklist/roadmap.md`）不许留在 `docs/` 顶层——§3 step 7 那条曾整条无闸门 |
  | 数据面生命周期 | `test/architecture/dataplane_lifetime_test.go` | ①`internal/agent/ssproxy` 非测试文件不得 import `context`；②`applyProxyDirective`/`proxyStartLocked` 不得再收 ctx 参数；③`Run` 必须接线 `defer stopProxyOnRunExit`。**控制面 ctx 绑数据面对象生命周期**曾让一次普通 NATS session 重建杀死活着的 SS 出口 7h40m（gotcha #80）；三条都是那条不变量的机械形式，第③条补的是"去掉 ctx 锚点后 agent 退出不再自动停它" |
  | simcluster 日志 oracle | `test/architecture/simcluster_log_oracle_test.go` | drill 只许经 `drills/lib/logs.sh` **一份**映射读四条流（broker slog／broker panic／agent slog／agent panic）；调用 helper 必须 source（漏 source 是 runtime not-found，`bash -n` 看不见）；例外进**精确计数**的递减账本。h1 挪了两条流，十来个各自内联的 drill 同时开始把**健康的产品报成失败**——这是本 harness 最坏的输出 |
  | spawn 停滞证据 | `test/architecture/spawn_stall_evidence_test.go` | `remote_fs_spawn_timeout` / `too_many_wedged_spawns` 的**铸造点**（`&FSError{Code:…}`、`return Err…`、`fmt.Errorf("%w",Err…)`；不含声明／`errors.Is`／分类返回）必须逐字等于精确账本（今天 4 条），且账本**双向** enforce（给 ceiling 站点接线＝静默反转裁决，同样变红）；标 wired 的两条，作废必须**在到达该 return 的每条路径上都已同步执行**（路径敏感；`go`/`defer`/func literal 均不算，**遇 `goto`/label 直接 fail-closed 不再分析**——臂内提前 return、裸 `go` 语句、`goto` 绕过都曾被记成 wired，外审 F2 + 复审 RR-F1）；**锁支配检查**（限 `internal/spawnsafe`）——能带着 `p.mu` 到达作废调用即红，分支不一致按**持有**处理，复合语句 initializer 按 Go 顺序处理；函数同时有 Policy lock+invalidation 时，`goto`/label 或 deferred invalidation 直接 fail-closed（条件 Unlock、if-init Lock、goto 绕 Unlock、LIFO defer 作废均曾漏掉确定性死锁）。同族正负控制在 `TestSpawnStallEvidenceGateControlFlowFamilies` 及外审反例。**healthy 判定曾是终态**，agent 活 18 天后整套 remote-fs 保护静默退化成只剩两条死线（gotcha #81）。⚠ 它**够不到**第四个证据站点 `boundedHomeRead`（那里不铸造任何哨兵），那条由 `internal/agent/remotefs_test.go` 的行为守卫钉住——闸门文件头已如实登记 |
  | spawn runtime 隔离 | `test/architecture/spawn_exec_isolation_test.go` + `internal/spawnexec`（由 `make gates` 直接运行） | safe exec 与 PTY run 的风险目标不得在 agent runtime 内直接 `cmd.Start`；必须经 `/proc/self/exe` helper + 可取消握手。dispatch 必须由 `internal/spawnexec.init` **对所有链接者统一执行**，不得要求各测试包手写 `TestMain`（漏一个就递归执行整个 `*.test`）。helper **只能原地 `syscall.Exec`，禁止再 `cmd.Start`/fork 目标**；私有 mode env 必须从继承与显式 child env 双向剥离；CLOEXEC 内核租约把跨 agent 重启遗留的 pre-exec helper 全局限制为 64。直接 goroutine+timeout 会冻结 agent runtime；漏 TestMain + helper 内二次 fork 曾让 5,789 个 `exe` 累积到约 307 GiB RSS、触发主机 global OOM（#81 / 2026-09-01 外审事故） |
  | hermetic 闸集 | `test/simcluster/tests/run-all.sh` + `test/architecture/simcluster_gate_set_test.go` | simcluster 的 hermetic 自检集（verdict 契约、expected-verdicts、lint-drills / lint-install、ledger-crosscheck、r9d / teardown 非空性、kept-sites…）作为 `make gates` 的一行与 CI build-test 的一步**真跑**（约 2 分钟，5 个 poll 类脚本在等真计时器）；`tests/*.sh` 与 run-all.sh 的循环**双向对账**（漏网脚本、点名不存在的脚本都红），脚本按 shebang 调用。2026-09-01 接线时它已在干净树上红了（gotcha #80 标题写"已修"、门的闭合词表只认"已修复"）、漏了 3 个脚本、其中 1 个是 bash-only 而旧循环一律用 `sh` 调用——即使列进去也跑不起来——"a gate nobody runs is a gate that does not exist"是它自己头注释里的话 |
  | CI 发布链 | `test/architecture/ci_workflow_test.go` | release job 必须 `needs` build-test / lint / e2e 三闸、只在 `refs/tags/v` 上跑、带 job 级 `contents: write`；**没有任何 workflow 在 ci.yml 之外发布**（`release.yml` 曾在 tag 上直接 goreleaser、与 e2e 互不可见，"发布时本机过了不够"是一条无人检查的声明）；fuzz job 必须无人值守可达（schedule + cron）且**绝不**挂在 push / PR / tag 上（非确定性红灯会训练人重跑到绿） |
  | fuzz 语料预算 | `test/architecture/fuzz_corpus_test.go` | 提交的 `testdata/fuzz/<Fuzz*>/` 每目录 ≤64 文件、全仓 ≤256KiB、首行必须是 `go test fuzz v1`（没有头的文件被工具链静默忽略）。`-fuzz` 只把 **crasher** 写进 testdata，interesting 语料在 `$GOCACHE/fuzz`——三份独立草案都把机制写错，见 `docs/testing-standards.md G7` |
  | 矩阵单元清单 | `test/e2e/parallel/inventory_test.go` + `testdata/` | `all_phases_test.go` 声明的每个单元 `(matrix, pkg, tags, race, run)` 记入**只增不减**的 golden（删 glob 在 all_phases 里是静默的——它自己的 B5 note 承认；在这里是红）；**唯一 `go test` 入口**（test/e2e 之外任何 `_test.go` fork `go test` 即红）；**-race 归属**（import raft/yamux/tunnel/pty 的包必须被某个 -race 矩阵跑到；2026-09-01 前 `internal/clusteroffline` 起 raft 七次却不在任何矩阵，`test/chaos` 22 个测试从未跑过 -race）。runner 的 `-dedupe`（默认开）按**运行时 `go list -race` 闭包 hash 全等**折叠重复单元（cluster ×5、proc ×3、broker ×2、test/cluster ×2 → 各 1），不等即不折、`go list` 出错即不折，**两种「不折」都打印理由**（hasher 全坏与「没有重复」在收据上必须可区分）；whole 矩阵（5 个 helper 形状）的每个包字面量／`-run` 值／phase 也进 golden（第一版对它们完全不透明，内审 L4-F1）；矩阵源**只为 -race 归属加包**（D1 +clusteroffline 并 240s→300s、新增 `TestChaosMatrix`／`TestLeakRiskMatrix`），**不为去重改** |
  | 闸门清单自对账 | `test/architecture/gate_registry_test.go` | ① 本表点到的位置必须真在 `make gates` 里跑（`go test` 包按目录前缀、`sh <script>` 行按精确路径）；② `.golangci.yml` 的函数名账本里每个名字必须还存在（死豁免会被报出）|

  - **改闸门本身走同一流程**：动 `.golangci.yml`、任何 golden、任何递减账本**等同于改不变量**，
    必须在 commit message 里写明「为什么这个预算该放宽」。golden 只许往收紧方向自动更新——
    `-update-structural-budget` **拒绝写入比现值更宽的数**，放宽必须手改文件，**这个摩擦点就是闸门的全部价值**。
  - **豁免必须自带过期压力**：`//nolint` 必须指名 linter 并带一句理由；问题修好后悬空的指令会报错。
    递减账本里已不存在的条目同样使门变红——**只许减不许增的账本才不会腐化成永久豁免**。
  - **改了闸门自身时跑 `make gates`** 做一次集中自检。它以 `vet-tags` 开头（否则整套 tag 门控套件
    连「能不能编译」都没人验）、中间一行 `sh test/simcluster/tests/run-all.sh`（hermetic 闸集，约 2 分钟，
    2026-09-01 前无人调用且在干净树上是红的）、以 `make lint` 收尾（配置本身就是闸门）。基线 **1m31s**（run-all.sh 接线前）
    → **3m32s**（2026-09-01 接线后实测：其中 run-all.sh ≈2 min 是 hermetic 闸集本身、`test/e2e/parallel` ≈5s、本增量的新 Go 门合计 <10s），
    每加一道 Go 门都该在 plan 里写它的边际秒数。
    ⚠ golangci-lint 有**全局锁**，第二个同参数调用**不是排队等待、而是直接 rc=3 失败**，
    所以 `make gates` 不得与另一个 lint 并行（审查 workflow 里 lint 要收敛到单个 agent）。
- **测试纪律**：
  - **按需测试，非必要不跑全量**：迭代时只跑碰过的那块——`go test ./test/pX/...`、
    `go test -tags dN_integration -race ./test/dN/`、`go test ./internal/broker/ ./internal/cluster/`。
    改了**手写解析器**（wire 行、AEAD 分帧、subject、invite、连接名）就顺手
    `make fuzz FUZZ='^FuzzXxx$' FUZZTIME=30s`。**fuzz 三态**：种子与已提交语料在 `make test` 里确定性回放；
    变异在 CI 每周一跑（`fuzz` job）与按需 `make fuzz`；**绝不**进 push / PR 闸——
    fuzz 证明的是"不崩"不是"算对了"，且它的红不可复现。判断该不该 fuzz 一段代码只问一句：这段解析逻辑是自己写的，
    还是标准库写的（`internal/proto` 的 22 个 JSON fuzzer 测的是 `encoding/json`，2026-09-01 前唯一有价值的 fuzz 是 SOCKS5 那一个）。
  - **全矩阵闸门只有一个：`make e2e-parallel`**（约 3–4 min）。**并行全绿即通过，不得再串行"复核一遍"。**
    **全量串行的 target 已从 Makefile 删除**——不是弃用，是拿掉了，因为存在的 target 就会有人跑。
    串行唯一合法用途是**定位并行报出的那一个**：`make e2e-one T=TestD5Matrix`（`T` 强制、无 "all" 模式）。
    理由与四类根因见 `docs/reviews/parallel-flake-rootcause.md`；写测试的规范见 `docs/testing-standards.md`。
    - **约 3–4 min 是本机 44 核的数**，不是套件的属性：worker 数 = `min(单元数, 物理核/2, 20)`，
      **2 核机只坐得下 1 个 worker**，同样 99 个单元从 5 层队列变成 99 层。实测（`taskset` 限到 2 物理核）
      **42m22s、ALL PASS**——套件在小机器上不脆，只是慢。整轮 deadline 已改为**按队列深度推导**并打进 plan，
      不再是写死的 25m（那是开发机的属性）；打满预算会印 `DEADLINE EXCEEDED` 并明说这不是测试失败。
    - **CI 不再每次 push 跑全矩阵**：`.github/workflows/ci.yml` 的 e2e job 触发改为
      **每周一 02:30 UTC + 版本 tag + 手动 `workflow_dispatch`**。理由是本条的硬闸已让**每个 commit**
      在本机跑过全矩阵，CI 那份加的是**另一套环境**（2 核 runner、冷缓存、新 runner 镜像、
      setup-go 当天解析到的 Go 补丁版），而环境按周变不按小时变。成本也真实：42m × 每月 38 次 push
      ≈ 1300 min，而免费额度 2000 min/月还要喂 build-test 与 lint。
      `test/architecture/ci_workflow_test.go` 钉住**这个 job 仍然可达**——`if:` 点名的每个事件必须在
      `on:` 里真声明，且至少留一个**无人值守**触发（只剩手动不算闸）。
  - 表驱动；`make test` 用嵌入式 `nats-server/v2/test`，**不需要本机 nats-server**。
  - 并发安全：`-race` + **仓库内建泄漏门**（NumGoroutine + fd 基线，见 `test/concurrency/helpers_test.go`；
    **刻意不用 goleak**）；触碰隧道/PTY/reconcile/传输/Raft 必须带 race + leak 门。
  - lint：`make lint`（golangci-lint **v2**；v1 在 Go 1.25 模块上会拒跑）。
  - **simcluster deploy-tier drill（`test/simcluster/`）**：第三层测试，不进 `go test`/CI。
    **⚠ 按需运行、非必要绝不运行**——只在改动真实部署栈（`install.sh` / `nats.conf` / systemd unit /
    集群生命周期 / 跨机 route mTLS）时跑，且只跑相关的那一个。跑在 `weilandserver`；
    **本机就是它**（`hostname -I` 含 `192.168.0.200`；2026-08-10 前是 `192.168.1.150`）时直接
    `cd test/simcluster && ./local.sh drill <name>`，
    不要 ssh、不要 `remote.sh`。全套用 `./run-drills.sh`（可并发）。
    **定位铁律：忠实复现真实部署环境、如实暴露缺陷，绝不替 tether 弥补**——
    tether 干不了的只呈现（标 `[GAP #N]`）、不代劳；靠复杂脚本才"成功"的操作是缺陷不是成就。
    完整 Mandate 与用法见 `test/simcluster/README.md`，凭据/资源见 `docs/devices-ops.local.md §6`。
- **提交前硬闸**：`make test` + **`make e2e-parallel`**（唯一的全矩阵闸门；全量串行 target 已删除，**严禁**并行过了再手工串行"复核一遍"）+ `make lint` 全绿，并发改动另过 `-race` + 内建 NumGoroutine/fd 泄漏门（非 goleak）；**任一不过不算 done**。

## 6. Git

- 只在 phase 收尾（step 7）、内审+外审通过后才 `commit`/`push`；**直接落 `main`，不开 phase/side 分支**——本仓是单开发者仓库，分支与 PR 只会制造无人评审的仪式。
- commit message：conventional commits `<type>(scope): <imperative summary>`（如 `feat(ps): retention-bounded ps RPC`、`fix(auth): grant $JS.API.DIRECT.GET`）。
- **绝不添加 `Co-Authored-By: Claude` 或任何 AI 署名**（全局规则；已推送的若混入，用户会 rebase 移除）。作者/协作者只保留用户本人。
