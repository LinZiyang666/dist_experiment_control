# Fail — allgreen / simcluster 整治工程（R1–R15）外部审查

> 审查者：claude（外部审查者，与主进程无关）。范围：**暂存区全部文件**（`git diff --cached`，177 文件 / +22005/-1104）。
> 输入索引（**不信任其结论**）：`docs/allgreen-remediation-roadmap.md`、`docs/reviews/r{1,2,6,15}-*.md`、`expected-verdicts.tsv`、`simcluster-full-suite-run-2026-07-18*.md`。
> 方法：多专家对抗性并行审查（10 lane，均 ≥ Opus 4.8）→ 主审对每条高危 finding **逐行代码复核 / 本地实测 / deploy-tier live 复跑**独立坐实。
> `codex` 前缀报告与未跟踪测试文件按用户指示忽略（仅记录其对硬闸的影响，见 §4）。
>
> **【2026-07-20 更新】** 开发者已逐条修复本报告的一轮 finding 并自审。**二轮重审见文末「二轮外审」章节**：一轮全部阻塞项（CRIT-1 发布级 + H-1..H-4 收官层）经独立变异/实测确认**真闭合**，判定从「发布级结构性缺陷 + 收官依据全面不成立」收敛为**单一 HIGH 阻塞 SR-8**（开发者核心 C1/SR-1 修复引入的跨 session ack 伪造，M-1 未真正闭合，使 `internal/broker` 硬闸 RED）。**（二轮）仍 Fail，待 SR-8 闭合。**
>
> **【2026-07-21 更新 · 最新结论 = 三轮 Pass】** 开发者已修复 SR-8（改 per-DIRECTIVE single-use token）+ F2/F3。**三轮重审见文末「三轮外审」章节**：三处经独立对抗变异/实测确认**真闭合**，硬闸全绿，且**二轮遗留的 CRIT-1 live deploy-tier 验证本轮达成**（drill 71 Arm B 在真隧道上证明 agent 静默下 drain 的 rc 语义与数据面跟随）。**判定：Pass（Conditional，仅余 Low 残留，可放行）。** 标题行「Fail」是一轮历史结论。

---

## 0. 结论

**发布判定：Fail。** 三条独立理由，任一单独成立即阻塞放行：

1. **【发布级产品缺陷 · 主审逐行确认】** R8a 引入的"数据面收敛门"——本整治工程的**头号目标**（关闭 P1：`cluster drain` 返回 rc=0 而 expose 数据面永不跟随）——在 `retire` 与 `drain` 的真实运行路径上**结构性失效**：门的输入是 `migrateExposes()` 的返回值，而该查询在第一次迁移后即自毁，导致 `retire` 在第二个 controller tick 自动越过门、走向**不可逆的 `RemoveServer`**，`drain` 重跑得到**假 rc=0**——即使 agent 从未确认。修复把 P1 换了一层皮重新引入（详见 §1 CRIT-1）。

2. **【收官声明与落盘证据不一致 · 主审本地实测】** `test/simcluster/` 的 hermetic 自检套件在**当前暂存状态就不通过自己的门**：`tests/run-all.sh` 真实退出码 **rc=1**（反注水闸 A `kept-sites --check` 与 `ledger-crosscheck` 两道红），而 `r15-finalization.md §7` 声称"三硬闸全绿 + harness 自检全绿"。且 R1 声称的"die() frame-aware 永久回归门"因两个单词拼写错误**恒真失效**（详见 §4）。

3. **【"真实全绿"目标未达且台账失准 · 主审 deploy-tier 独立复跑】** 用正确 binary+镜像（`remote.sh --build build`）独立复跑，`30-rolling-upgrade` 与 `82-agent-onboarding-invite` **稳定 ASSERT-FAIL**（串行 -j1 与我方 -j2 一致），而 `expected-verdicts.tsv` 期望二者 INCOMPLETE、`finalization §9` 将其归因为"并行 sweep flake"——**该归因被证伪**。两处均为 roadmap 自设 N1 判据禁止的"无主 assert_fail"（详见 §3）。

**必须同时写明的阴性结论（避免过度否定）**：这批工作的**工程密度与诚实度整体很高**。绝大多数红→绿翻正都有真实产品修复支撑（逐一在源码核实：#25 per-IP 限速、#26 子进程回收、#27 manifest 默认 bind、#28 升级白名单、#36 online `--yes` 拒绝、#50 doctor 六态 FATAL、#51 restore `--config`、#54/#55 issuer skew、#58 home-authoritative reap、P8 json schema bump 等），**未发现"改松测试换绿"式系统性造假**；深层缺陷族（grow-onto-recovered 42/51/22/82、#57、#65 结构性）被**诚实披露为 PRODUCT-RED / 结构性边界**而非粉饰；三个 Go 硬闸（`go test`/`e2e`/`lint`）+ `-race`+ 泄漏门我方独立复跑**全绿**。**问题集中在两个面**：(a) R8a 这一处新产品逻辑的正确性（CRIT-1，最严重），(b) 收官层的**账目与自检门**（harness 自检 rc=1、台账失配、回归门恒真）——即"宣布全绿的依据"本身不成立，而非底层修复大面积错误。

---

## 1. Blocker

### CRIT-1 — R8a 数据面收敛门在 retire/drain 真实路径上失效：retire 自动走向不可逆 RemoveServer、drain 重跑假 rc=0

- **严重度**：Blocker（发布级）· **确定度**：**已确认**（主审逐行复核 + 两个独立专家 lane 独立命中 + 注释与实现直接矛盾）
- **文件**：
  - `internal/broker/cluster_operation_controller.go:781-813`（`driveRetire` 的 `OpStateRehomeExposes` 臂）
  - `internal/broker/clusterdrain.go:142-158`（`DrainNode` 同步 verb）+ `:708-716`（`migrateExposes` 查询）
  - `internal/broker/home_convergence.go:86-97,122-143`（gate 本体）
- **机理**：收敛门唯一输入 `migrated` 来自 `migrateExposes(nodeID)`，其查询是
  `SELECT ... FROM port_allocations WHERE home_broker=? AND state='ALLOCATED'`（`clusterdrain.go:716`）。
  第一次调用把这些行的 `home_broker` 经 raft 改指新 broker——**这条读取自毁**。于是：
  - **retire（op controller，自动 tick，最危险）**：`driveRetire` 每个 tick 重入 `OpStateRehomeExposes`。
    tick 1：`migrateExposes` 返回迁移行 → `pendingHomeConvergence(migrated)` 非空 → `recordOpError` + return（hold，正确）。
    tick 2（数秒后）：`migrateExposes` 再查 `home_broker=nodeID` → 行已改指新 broker → **返回空** →
    `pendingHomeConvergence([])` 空 → `len(pending)>0` 为 false → **不 hold** → `setPhase(DRAINING)` →
    `OpStateStreamsAtTarget` → … → **`RemoveServer`（不可逆）**。后续状态只等 streams/seeds，**无第二道 home gate**（已逐行确认）。
    ⇒ **agent 完全静默（不重连、不 ack——正是本工程的核心场景）时，retire 在第二个 tick 就自动越过门删除节点。** 注释 `:790-793`"It only leaves this state once every migrated expose's agent has CONFIRMED"**与实现矛盾**。
  - **drain（同步 verb）**：首跑数据面未收敛 → 返回 `ErrDataPlaneNotConverged`，错误文案（`home_convergence.go:73-75`）明写"**re-run to keep waiting**"。运维照做重跑 → 第二次 `migrateExposes` 返回空 → `awaitHomeConvergence([])`（`:123` `len(migrated)==0 → return nil`）→ **rc=0 假成功**，agent 可能仍未收敛。这与修复前 P1 的运维后果同型（"成功退出码 + 数据面静默丢失 → 运维继续 retire/关机 → expose 失联"）。
- **为何所有测试放行**：`TestRetireConsultsTheConvergenceGate`（`r8_home_delivery_test.go:600`）的注释**自证**了漏检——
  "Seeding that state through the real FSM needs raft-replicated port_allocations rows, **for which no plan command exists** — so the call site is pinned in source"。即它只用**源码字符串 pin** 证明 `pendingHomeConvergence(migrated)` 这行存在，证明不了它在 tick 2 有效。而 `TestRetireHoldsWhileTheDataPlaneIsStale`（`:542`）**手工构造非空 `migrated`** 直接调 `recordOpError`（`:564-572`），测的是 tick 1，**结构上无法**触达 tick 2 的空 `migrated` 放行路径。**无任何测试真实驱动 driveRetire 连续两个 tick。**
- **建议**：门应改用**不自毁的 fleet 视图**作 oracle——产品**已经有** `homesUnconverged()`（`home_delivery.go:444`，`upgrade` verb 在 `TestUpgradeHomesConvergedVerdict:623` 已用它，它全表扫 `port_allocations` 的 home_broker+epoch，不依赖"本次迁移了哪些行"）。把 retire/drain 的门从 `migrateExposes` 返回值改为"限定到受迁移 agent 的 `homesUnconverged` 子集"即可闭合，并补一条**真实驱动 driveRetire 两个 tick** 的 hermetic 回归测试（或以 deploy-tier drill 71 的 Arm B 静默-跟随判据在真栈钉住）。
- **运维后果**：执行 `tether cluster retire <brk>` → op 在 agent 静默下几秒内自动推进到 `RemoveServer` → broker 被移除而其上 home 的 expose 隧道仍指向已删节点 → 用户 expose 失联，且 rc 一路正常。这正是 §0.1 P1（缺陷报告列为**发布级 blocker**）要根除的形态。

---

## 2. High

### H-1 — hermetic 自检套件当前不通过自己的门（run-all.sh rc=1），收官声明失实

- **严重度**：High · **确定度**：**已确认**（主审本地实测）
- **文件**：`test/simcluster/tests/run-all.sh`、`tests/kept-sites.sh`、`tests/ledger-crosscheck.sh`、`docs/reviews/r15-finalization.md §7`
- **实测**（我在本地干净工作树跑，取真实退出码）：
  ```
  sh tests/run-all.sh                → rc=1（poll-reentrancy/verdict-contract/lint-drills/lint-install/r9d PASS）
    ├─ ledger-crosscheck             → rc=1: "#29, #34 — 2 open defect(s) with NO non-GREEN owner"
    └─ kept-sites --check            → rc=1: "REGRESSION 41-shrink-to-standalone kept_sites 28 -> 27 (lost 1)"
  ```
- **机理**：
  - **反注水闸 A 违反（G-8）**：`drills/41-shrink-to-standalone.sh:140-160` 把 first-retire 的撤离面从 2 条硬 `assert_ok` 改为 1 条 `not_covered(gap)`+1 条功能断言，净 −1 站点。改写本身论证充分（over-specified connz-disconnect → 诚实 gap，与 tsv 一致），**但 roadmap §3「反注水闸四层 A：kept_sites 逐 drill 不下降」+ G-8 的字面承诺被违反**，且既未更新 baseline 附 provenance、也未在 `r15-finalization.md` 披露该门回红。
  - **ledger-crosscheck 违反**：`docs/deploy-tier-gotchas.md` 中 #29、#34 仍 OPEN（`:116`、`:238` 有专门章节），但 `expected-verdicts.tsv` 无任何非绿格钉住它们（`#29` 应由 drill 71 的 INCOMPLETE 格归属，但 owner 列未写 `#29`；`#34` 因 73/74 双双翻 GREEN 而全表无非绿格可归属）。这正是该闸设计要抓的"已注册缺陷坐在绿 drill 里"状态（#25/#26/#27 曾如此），如今复现且闸响了。
- **与收官声明的冲突**：`r15-finalization.md §7` 声称"三硬闸全绿……harness 自检"，`r2-plan:702` 记"六道全 PASS（含 ledger-crosscheck 首次 OK）"——**当前暂存状态 run-all.sh rc=1 未在任何收官文档披露**。G-7 只列了 `lint-drills`/`verdict-contract`/`lint-install` 三条（我实测这三条 rc=0），但 G-8 的 kept-sites 门与账目一致性门当前是红的。
- **建议**：三选一并**留档**——(a) 71 行 owner 列补 `#29`、#34 在台账降级/给非绿 owner；(b) 41 的 baseline 行改 27 并附 R15 trade 的 provenance 注释，或恢复一条独立断言；(c) 若接受当前状态，必须在收官报告显式声明"harness 自检 rc=1，原因 X，已知接受"。G-8 承诺的"kept_sites 初值 vs 终值对照表"在 `r15-finalization.md` **缺失**，应补。

### H-2 — 30 / 82 稳定 ASSERT-FAIL，tsv 期望 INCOMPLETE，finalization "并行 flake" 归因被证伪

- **严重度**：High（"真实全绿"目标未达 + 台账失准）· **确定度**：**已确认**（主审 deploy-tier 独立复跑）
- **证据**：`remote.sh --build build`（重建 binary+docker 镜像，规避 finalization §7 记录的 stale-binary 事故）后，`run-drills.sh -j2`（我方 `claudeext` 批）+ 服务器留存的 `r15ser -j1` 串行：

  | drill | 我方独立复跑 | `expected-verdicts.tsv` 期望 | 一致 |
  |---|---|---|---|
  | 30-rolling-upgrade | **ASSERT-FAIL** (assert_fail=1, pass=52) | INCOMPLETE (assert_fail=0) | **否** |
  | 42-rejoin-returning | PRODUCT-RED (pass=42) | PRODUCT-RED (#GROW-ONTO-FORCE-SINGLE) | 是 |
  | 50-backup-restore | PRODUCT-RED (DOC-27, pass=86) | PRODUCT-RED | 是 |
  | 51-full-dr | PRODUCT-RED (pass=70) | PRODUCT-RED (#GROW-ONTO-RECOVERED) | 是 |
  | 82-agent-onboarding-invite | **ASSERT-FAIL** (assert_fail=1, pass=28) | INCOMPLETE | **否** |
  | 96-mid-flight-chaos | PRODUCT-RED (pass=33, **nc_guard=1**) | PRODUCT-RED (nc_guard=0) | verdict 对，**nc_guard 违反 G-4** |

- **30 根因**：`30.log:218` `PHASE-1 CONTINUITY the raft write-probe saw not_leader/503/no-responders` FAIL——write-probe 在 **phase-1（agentless HALT 窗口，brk2 broker reload + agent re-exec refused）** 命中集群短暂不可写；phase-2 正常 roll 则 `:255` PASS。谓词有 `WROTE-*` 非空性 guard（`30.sh:192-197`），非恒真恒假。这**要么是产品观察**（agentless HALT / broker PID-preserving re-exec 窗口集群短暂不可写，与"滚动升级应保持可用性"张力），**要么是 drill 谓词过严**——无论哪种，30 都不是 INCOMPLETE 绿。
- **82 根因**：`82.log:33` `C1-grow grow brk2 (N=2) (want exit 0, got 1)`——**grow-onto-recovered 深层缺陷家族**（与 22/42/51 同根），但 tsv 的 82 owner 只写"U1-U4 user-service in-sim gap"（那是 `not_covered`），**这个 assert_fail 无 owner**。
- **归因证伪**：`finalization §9.2` 声称 30/82 是 `-j3` 并行 sweep flake、"可靠 verdict 须来自 serial/-j1~2"——但 `r15ser -j1` 串行 30=ASSERT-FAIL、我方 -j2 也 ASSERT-FAIL ⇒ **稳定信号，非并行 flake**。
- **违反自设判据**：roadmap N1"任一非绿格必须能定位 owner + 归属批号；无主非绿 = 该批失败"。30 的 assert_fail、82 的 assert_fail 均无 tsv owner。
- **结论**：R15"追求真 37/37"未达成。最佳客观图景 = 25 GREEN + 若干诚实披露的 PRODUCT-RED（深层缺陷）+ **30/82 两条未诚实登记的 ASSERT-FAIL**。`coverage-boundary.md` / `finalization` 应把它们从"flake"改为**如实登记**（产品观察或 drill 过严，附 owner）。
- **附带正面实证**：`finalization §9.3` 声称把 50 的 zed-reconverge 断言改为 POLL——该修复**此前从未被任何有效实跑验证**（r15full 在修复前跑、r15ser 未跑到 50）；我方复跑 50=PRODUCT-RED(仅 DOC-27, assert_fail=0, pass=86)，**证实 POLL 修复有效**。
- **附带新失配（96 的 runtime-guard 触发）**：我方复跑 96=PRODUCT-RED **但 `nc_guard=1`**——触发的是 `96-A2 (#58)` 的 split-home reap guard（`96.log:27`）。该 guard 文本自陈：本 drill 会话**恒为** multi-agent split-home（agt1→brk2, agt2→brk1），`homeOwnsXferBucket` 对每台 broker 都保守为 false ⇒ per-session bucket "unreapable **BY DESIGN** — a #58 RESIDUAL for split-home sessions … refining homeOwnsXferBucket to per-transfer-owner is a **follow-up**"。**问题有二**：(a) 一个**有具名 follow-up 的产品残缺**被标为 `runtime-guard` 而非 `gap`（roadmap §3 契约：gap = 修复可退休的债，runtime-guard = 永删不掉的固有非确定性），撞 drills-lane 的 Finding-1；(b) 拓扑恒为 split-home ⇒ 该 guard 结构上**每次判定运行都可能触发**，`finalization §6` 声称的 "r14d nc_guard=0 EVERY run — TERMINAL-GATE CLEAN (all 7 of 96's runtime-guards eliminated)" 被我方**一次复跑即证伪**。按 roadmap G-4"runtime-guard 在两轮判定运行中一次都不得触发（触发即该轮不计入判定轮、成为 R14 工作项）"，我方这一轮的 96 **不构成合格判定轮**。建议把 96-A2 的 split-home 残缺从 runtime-guard 改为 `gap`（有 owner、恒非绿），并把 finalization 的"nc_guard=0 EVERY run"更正。

### H-3 — R1 die() frame-aware 回归门因 `ok`/`bad` 拼写恒真失效

- **严重度**：High（仪器地基批 R1 的头号交付的回归保护不存在）· **确定度**：**已确认**（主审实测 + 专家变异实验）
- **文件**：`test/simcluster/tests/verdict-contract-test.sh:163-172`
- **机理**：该文件记录器定义为 `pass()`/`fail()`（`:16-17`，`FAILS` 仅在 `fail()` 递增），但 die() 框架检查（`:165/166/172`）调用**不存在的** `ok`/`bad`（poll-reentrancy-test 的命名串台）。`grep -q … && ok … || bad …` 中 `ok` rc=127 → `|| bad` 也 127 → **任何分支 `FAILS` 都不递增**。我实测该测试每轮打印 `165: ok: not found`、`166: bad: not found`、`172: ok: not found` 后照样 `ALL PASS rc=0`。专家 lane 做了变异实验：把 die() 回退成 pre-R1 的 `err; exit 1`（撤销 R1 覆盖 38 处 die 调用点的 verdict 契约修复）→ 该测试仍 `ALL PASS`。即 R1 声称的"die() 永久回归门"**当前完全失效**，且讽刺地发生在契约测试自身。
- **建议**：`ok`→`pass`、`bad`→`fail` 两处替换；并给 run-all 对测试自身 stderr 的 `not found` 报警（否则同类拼写错误无人察觉）。die() 本体修复可能是对的（deploy-tier 实跑覆盖），但**声称的回归保护不存在**。

### H-4 — cluster unlock 在零 health 回复时报 rc=0 "no locks held"（fail-open）

- **严重度**：High · **确定度**：**已确认**（主审确认语义 + codex 未跟踪测试对暂存实现实证失败）
- **文件**：`cmd/tether/cluster_unlock.go:172-193`（probeLocks）、`:279-305`（confirmUnlocked）、`cmd/tether/d8_alerts.go:27-38`
- **机理**：`probeLocks` 基于 `probeClusterHealth`，后者零回复返回 nil（`d8_alerts.go:29` 注释自陈"replies … or ctl unable to reach any broker — yields nil ⇒ no gate"，`:34/:38` return nil）。broker 全失联 / actor 无权限时 `held==0` → 打印"no membership locks are held — nothing to clear." + rc=0。该 verb 自称"rc=0 only when every requested lock is CONFIRMED gone"，此处把"看不见集群"当成"无锁"。`confirmUnlocked` 同病。
- **建议**：probe 与 confirm 两处均对零回复 fail-closed（unavailErr）。（注：工作树里 codex 的未跟踪测试 `TestCodexReviewClusterUnlockRejectsZeroHealthResponders` 恰断言此点、当前对暂存实现失败——该测试非本审查范围，但其信号与本 finding 一致。）

---

## 3. Medium（采信专家 lane，主审抽样复核机理）

| # | 严重度 | 文件 | 机理 | 建议 |
|---|---|---|---|---|
| M-1 | Medium(安全) | `internal/broker/home_delivery.go:167-190` | home applied-ack 通道无发送者鉴别、epoch 无上界钳制、以裸 port 为键：① 任一已认证 agent 从共享 `_INBOX` 学到 ack subject（`auth/permissions.go` agent 有 `_INBOX.>` Pub）后可为**别 session** 的 port 伪造 `{epoch:MaxInt64}` ⇒ drain/retire/upgrade 假 rc=0 + 投递永久静默；② 端口 `rm`+复用后旧 ack 毒化新行（epoch 单调只升）。跨 session ACL 隔离被绕过。 | 最小修复：ack 校验 `(sid,nid,port)` 归属 + `epoch>行当前值` 丢弃（与 CRIT-1 修复同一处代码）。安全实用主义下 epoch 钳制建议顺手做 |
| M-2 | major | `internal/broker/reconcile_upgrade_lock.go:88-99`、`reconcile_grow_lock.go:155-172`、`internal/cluster/membership_ops.go:406-414` | 锁 reap 的"决策读（marker+lease 过期）"与"raft apply（无条件 DELETE marker）"之间 TOCTOU，且两个锁 pass **无 catch-up gate**（对比 xfer pass 的 `reaperCaughtUp`）。FSM 落后的新 leader（滚动升级选举后）读到旧 lease → 摘掉活 grow/upgrade 锁；间隙内并发 join/retire 被放行。keeper latch 使**可检测**（非静默）。 | clear 命令用 SQL CAS（`DELETE WHERE lease<reap-now`），或给锁 pass 加 `RaftAppliedIndex>=CommitIndex` gate |
| M-3 | Medium | `internal/broker/transfer_reconcile.go:59`、`clusterwrite.go:536-541`、`transfer_home.go:78-111` | #58 周期 reap 把删除动作从 boot-only+leader-only 放宽到每 interval/每 caught-up broker，引入两个窗口：① `active` 快照在 sweep 开头拍一次、之后长循环删除，其间本 home 新起的 transfer 的已完成对象被误删；② mid-transfer rehome 后新 home（空 tracker）删旧 home 仍 in-flight 的对象。`reaperCaughtUp` 的 applied≥commit 在 boot/岛化 follower 上平凡为真（commitIndex volatile），旧 `IsLeader()` gate 结构性排除的两态被重新打开。后果=in-flight transfer 被打断（可重试，非静默丢失）。 | `activeOBJStreams()` 移到每 bucket `List` 之后重取；`reaperCaughtUp` 加"曾与 leader 同步过"新鲜度界；对象删除加 ModTime 时龄门槛 |
| M-4 | Medium | `test/simcluster/tests/lint-install.sh:84-97` | P9 heredoc 静态闸漏检合法的 `cat << EOF`（`<<` 与分隔词间有空格，POSIX 合法）：opener 剥 `<<-?` 后未剥空白即判引号 → match 失败 → 整块当"非 heredoc"跳过，body 反引号不查。专家变异实证：追加带反引号正文的 `<< EOF` → lint `OK … 0` rc=0（漏）。P9 类回归换一种拼写即绕过。 | opener 改 `sub(/^.*<<-?[[:space:]]*/, "", s)` |
| M-5 | Medium | `test/simcluster/tests/lint-drills.sh:98-110` | 反注水闸 D（INVERTED 配对 lint，roadmap §3 + r1-plan item G 承诺"含 INVERTED 的块须同块配对 product_red\|assert_bug"）**从未实现**——只有描述注释、无执行代码；也不在 r1-plan 的"实现后撤回"清单。当前 11/50/81/96 共 8 处 INVERTED 无任何机器检查，G-6"无未配对 INVERTED 块"无执法。 | 实现该规则，或按撤回流程留档并指明由哪道闸接替（ledger-crosscheck 覆盖不了"未注册的倒置假绿"） |
| M-6 | Medium | `internal/broker/home_convergence.go:122-143`、`clusterstatus.go:601`、`internal/adminsock/client.go:25-45` | `drain`（非 `--now`）的有界阻塞界 = `drainNotice`(30s 硬编码不可配) 压过 adminsock 客户端 5s 读超时：5–30s 收敛的 drain 对 CLI 呈现为 exit 69 "socket timeout"（而非设计的 exit 75 dataplane_not_converged），且 CLI 断开后 broker 侧继续推进 phase。随后重跑命中 CRIT-1。核心错误信号在真实 CLI 路径上仅 `--now` 可见。 | drain 请求路径给 client 设 ≥ drainNotice 的 Timeout，或改成 retire 式 hold-and-retry |

---

## 4. Minor / Note

- **N-1【已确认】gofmt 不洁**：6 个暂存改动文件未过 `gofmt`（`cmd/tether/{cluster_upgrade,error_hints}.go`、`internal/agent/agent.go`、`internal/broker/{clusterwrite,transfer}.go`、`internal/natsconf/remedy.go`）。**但 `.golangci` 未启用 gofmt/gofumpt formatter**，故 `make lint` 仍 rc=0——**非硬闸失败**，属代码异味（`agent.go:1210` 附近 `forgetHomeAck` 三行缩进错位）。建议 `gofmt -w` 顺手清。
- **N-2【采信 R11 lane】** 集群模式下 PIN per-IP 限速被 auth_callout **queue-group 自动稀释为 N×10/min**（`authcallout/ratelimit.go` 单 broker 内存桶 + `authcallout.go:99` QueueSubscribe），architecture §E.6 承诺的 10/min 是 per-broker 语义，文档未登记此偏差。4 位 PIN 下仍是显著减速（非防线失效），属文档口径。建议在 ratelimit.go + E.6 旁注 per-broker 语义。
- **N-3【采信 R11 lane】** `reconcile nats` 的 issuer skew 检测在**检测本身出错时 fail-open**（`cluster_secrets.go:44-52` conf 读不出 → `confIssuer=""` → 不判 skew → 放行），与"issuer 已知不匹配时 fail-closed"不对称。需已是 root 才可利用，纵深上 doctor 会独立 FATAL。建议 `acct!=""` 但 `confIssuer==""` 时升级为 loud note/拒绝。
- **N-4【采信 R10 lane + 主审核对】** DR 文档/路径若干：(a) **DOC-27 未修**——`docs/cluster-runbook.md §5`（`:579/583/603/635/668`）与 backup 命令示例仍全用 `/var/backups/...`，而在线备份由 `User=tether` 创建、stock 装机上不可写（drill 50 已实测 permission denied，drill 51 自己改用 `/var/lib/tether`）；(b) `cluster_backup.go:248` 的 "fresh box / conf 缺失"分支打印的 `reconcile nats --manual --conf <缺失路径>` 结构上不可执行（`natsconf.Preflight` 第一步即 `os.ReadFile` 失败）；(c) `applyClusterSeam` 的 `nats_conf_path` 硬编码默认值、与 `--nats-conf` 不联动（自定义 conf 路径部署下 seam 漂移）。均 low、触发概率低。
- **N-5【采信 R13 lane】** `adminEventsTail` 的 `--since` 用 broker 时钟对比 JS server 时间戳、`--kind` 稀疏 + `eventsMaxScan=5000` 截断 / 5s ctx 中途失败**均无 truncated 标记**（静默返回部分结果）；CLI `--since -1h` 被静默降级为无时间界。`xfer_reap_interval`/`disk_check_interval` **无上界**（`10000h` 等效静默关停 reaper）、无热更新。均 low。
- **N-6【采信 R13/R7 lane】** `runtime_introspect.go:45` 的 `Threads` 用 `pprof threadcreate Count()`（"创建过的 M 数"，线程退出不回落），注释"the OS thread (M) count"略过头；`Goroutines` 用 `runtime.NumGoroutine` 正确、红线未违反。#58 的 split-home / 零节点 session bucket 在集群模式**永久不可 reap**（`homeOwnsXferBucket` 保守残留，代码注释一致），建议加告警计数防小盘顶满（racknerd 事故同型）。
- **N-7【采信 harness lane】** `kept-sites.sh`/`lint-install.sh` 的 tokenizer 不感知引号，字符串/行尾注释内的分隔符+关键字可骗过计数（双向）；专家扫描确认**当前 37 drill 无一处真实误计**，属未来漂移通道。`kept-sites.baseline.tsv` 无 provenance 注释、总和 1266 与 r1/r2-plan 记录的 1247/1274 三个数字互不相等、无 git 史 ⇒ R1 冻结值不可复核（G-8 判定基准）。建议补 header。
- **N-8【已确认】** 暂存区混入 0 字节杂物文件 `test/simcluster/tests/verdict-contract-test.sh.d`，应从 commit 剔除。
- **N-9【采信 R8/横切 lane】** `home_delivery.go` 的 `attempts` map（键 `sid|nid`）只在 target 仍被枚举时删除，全部 expose 释放后条目永久残留——慢泄漏，NumGoroutine/fd 门抓不到。`poll_until` 的 desc 含换行时帧栈损坏 → 无界忙等（`log.sh:42-71`，`remote.sh drill <name>` 单跑无 per-drill timeout 兜底）。建议 desc `tr '\n' ' '`。

---

## 5. Doubts / 残留风险（不当作已定结论）

1. **96-mid-flight-chaos 复跑已完成**（补记）：verdict=PRODUCT-RED (pass=33)，符合 tsv 期望大类，但 **nc_guard=1**（触发 96-A2 split-home #58 guard，见 H-2 附带失配）——该轮不构成 G-4 合格判定轮，且证伪 finalization "nc_guard=0 EVERY run"。深层缺陷/结构性边界（#57 in-flight 不可构造、#65 结构性、arm B/C/F gap）本轮如实披露为 gap，与 coverage-boundary 一致。
2. **30 的 phase-1 continuity 失败归属未定案**：write-probe 在 agentless HALT / broker reload 窗口看到 not_leader/503——**是产品缺陷（滚动升级不保持写可用性）还是 drill 谓词过严**，我未做定案实验（需保留现场逐样本尸检 write-probe 命中的确切时刻与 broker 状态）。无论哪种，30 都不是当前 tsv 登记的 INCOMPLETE。
3. **CRIT-1 的 hermetic 复现测试未写**：如 §1 所述，产品作者注释自陈"无 plan command 可 seed raft-replicated port_allocations 行"，故我未能在 hermetic 层写出真实驱动 driveRetire 两 tick 的复现（这本身是测试脆弱性的证据）。逻辑链由逐行代码 + 两独立 lane + 注释矛盾三重确认，但**尚无一次 live 实证** retire 在 agent 静默下越过门（drill 71 Arm B 目前测的是 drain 的 P1 正向，未覆盖 retire 的自动 tick 越门）。建议主进程补 drill 71/40 的 retire 静默-越门负向臂。
4. **全灭 DR 尾段仍脆**：R10 修了 P2(`--config`)/P4/P5/#53，drill 51 的 DR 尾段据 finalization 首次端到端跑通，但 51 落地仍 PRODUCT-RED（grow-onto-recovered 深层缺陷，R16 待修）；DOC-27 未修（N-4a）。DR 关键路径的"照 runbook 逐字执行能恢复"在 stock 装机上仍卡 `/var/backups` 不可写。

---

## 6. 收官声明（G-1…G-10）逐条 vs 实测

| 判据 | 声明 | 实测 | 判定 |
|---|---|---|---|
| G-1 (37 行 verdict 落盘可 parse) | 满足 | rollup.tsv 落盘、格式正确 | ✅ |
| G-2 / G-6 (无 assert_bug/product_red/gap 残留、ALL GREEN 无 waiver) | "真 37/37" | 42/50/51/96 **PRODUCT-RED**（诚实披露）；30/82 **ASSERT-FAIL**（无主）；未达 ALL GREEN | ❌ 未达 37/37 |
| G-4 (连跑两轮不同 -j 一致、无 runtime-guard 触发) | 满足 | 30/82 -j1 与 -j2 一致红（一致的**红**）；**96 我方复跑 nc_guard=1**（split-home guard 触发）⇒ 该轮不构成合格判定轮，证伪 finalization "nc_guard=0 EVERY run" | ❌ |
| G-7 (lint-drills BATCH=37 / verdict-contract / lint-install 三条 exit 0) | 三条绿 | 三条 rc=0（但 verdict-contract 的 die 检查恒真，H-3） | ⚠ 形式过、实质有洞 |
| G-8 (kept_sites 逐 drill ≥ R1 冻结初值 + 对照表) | 满足 | **kept-sites --check rc=1**（41: 28→27）；对照表缺失 | ❌ |
| G-9 (make test/e2e/lint + race + 泄漏门全绿) | 全绿 | 我方独立复跑**全绿**（cli_e2e 一处并发 flake，单跑过） | ✅ |
| G-10 (绝不裸 ALL GREEN；写覆盖边界) | 满足 | coverage-boundary.md 存在且诚实；但 finalization 把 30/82 归为 flake 而非边界 | ⚠ |
| ledger-crosscheck (账目一致) | r2 记"首次 OK" | **rc=1**（#29/#34 无主） | ❌ |

**净读**：宣布"真实全绿"的**依据链**（harness 自检、kept_sites 反注水闸、账目一致、37/37）当前**不成立**——不是底层修复大面积错误，而是收官门与台账没有收敛到声明的状态。

---

## 7. Verification（我方独立执行）

**Go 硬闸（本地，无缓存）**：
- `go test -count=1 ./...` → 1 包 FAIL = `test/cli_e2e/TestExposeBandExhaustionReturnsPortExhausted`，**单独复跑 rc=0** ⇒ 全套并行端口 band 争用 flake，非回归；其余全绿。
- `make lint`（golangci-lint v2） → **rc=0**。
- `make e2e`（`-tags e2e_matrix`，串行全矩阵） → **rc=0**。
- `go test -race -count=1 ./internal/broker ./internal/agent ./cmd/tether ./internal/authcallout ./test/concurrency`（含仓库内建 NumGoroutine/fd 泄漏门） → **rc=0**。

**simcluster hermetic 自检（本地）**：
- `sh tests/run-all.sh` → **rc=1**（ledger-crosscheck + kept-sites --check 红；余 5 道 PASS）。
- `sh tests/lint-drills.sh` → rc=0（batch OK, 37 drill）· `sh tests/lint-install.sh` → rc=0 · `sh tests/verdict-contract-test.sh` → rc=0（**但 die 检查恒真，见 H-3**）· `sh tests/poll-reentrancy-test.sh` → rc=0 · `sh tests/r9d-nonvacuity.sh` → rc=0（130 proved）· `sh tests/kept-sites.sh --check` → **rc=1** · `sh tests/ledger-crosscheck.sh` → **rc=1**。

**deploy-tier live（weilandserver 192.168.1.150，`remote.sh --build build` 重建 binary+镜像后）**：
- 我方 `run-drills.sh -j2`（claudeext 批，全 6 drill 完成）：30=ASSERT-FAIL(af=1) · 42=PRODUCT-RED · 50=PRODUCT-RED(DOC-27) · 51=PRODUCT-RED · 82=ASSERT-FAIL(af=1) · 96=PRODUCT-RED(**nc_guard=1**，split-home #58 guard 触发)。
- 交叉服务器留存 `r15ser -j1` 串行：22=GREEN · 30=ASSERT-FAIL · 42=PRODUCT-RED —— 与我方 -j2 一致，**证伪 finalization 的"并行 flake"归因**。
- `r15full -j3` 全套 rollup（服务器留存）：GREEN=25 / PRODUCT-RED=2 / INCOMPLETE=6 / ASSERT-FAIL=4（22/30/50/82）—— 22/82 的 ASSERT-FAIL 印证 grow-onto-recovered 家族的间歇性。

**代码逐行复核**：CRIT-1（driveRetire/DrainNode/migrateExposes/home_convergence 全链）· H-4（unlock/d8_alerts 零回复语义）· #58 reap gate（reaperCaughtUp/homeOwnsXferBucket）· gofmt 洁净度 · git status 暂存边界（codex 文件未暂存、非本审查范围）。

---

## 8. 建议的处置顺序

1. **合入前必修（Fail 阻塞项）**：
   - CRIT-1 —— 把 retire/drain 的收敛门改用 `homesUnconverged()` fleet 视图，补真实两-tick 回归测试（+ drill 71/40 retire 静默-越门负向臂）。这是发布级、且是本工程的立项目标。
   - H-1 / H-3 —— 修 run-all.sh 的两道红（41 baseline provenance、#29/#34 归属）+ verdict-contract 的 `ok/bad` 拼写；否则"全绿"声明无依据。
   - H-2 —— 把 30/82 从"flake"如实登记为无主非绿（产品观察或 drill 过严），给 owner；30 做一次定案实验区分产品/谓词。
2. **强烈建议同批**：H-4（unlock fail-open）、M-1（ack 钳制，与 CRIT-1 同处代码）、M-2（锁 reap catch-up gate）、M-3（#58 reap 快照 TOCTOU）。
3. **可 follow-up**：M-4/M-5（P9 闸盲区、INVERTED lint）、M-6（drain 超时）、N-* 全部；96 取最终 verdict；DOC-27。

---

## 主进程回复（2026-07-20，逐条采纳/驳回）

> 结论：**Fail 判定成立且高质量**——三条阻塞理由全部核实为真。以下逐条处置。绝大多数 finding **采纳并修复**（附落盘证据/新测试）；两处 deploy-tier 登记项标为 **deploy-tier 复跑门**（在 weilandserver 验收，属分内活）；无驳回性反对。硬闸重跑与本轮 self-review 见文末。

### 处置总表

| # | 判定 | 处置 |
|---|---|---|
| CRIT-1 gate tick-2 越门 | 采纳 | **已修**：新增 durable `pendingRetireConvergence(nodeID)`，driveRetire/DrainNode 改用它替 self-destruct 的 `pendingHomeConvergence(migrated)`；hermetic 回归 `TestPendingRetireConvergenceIsDurable`（第二 tick 不自毁 + 变异释放）+ 源码门更新 |
| H-1 run-all.sh rc=1 | 采纳 | **已修**：kept-sites 41 由 collapse 改回**两条独立 not_covered(gap)**（28 站点，反注水闸绿）；ledger #29→写入 71 owner 格；#34→74 加**持久 gap**、tsv 翻 INCOMPLETE。run-all.sh 现 rc=0（7 门全 PASS） |
| H-2 30/82 flake 归因证伪 + 96 nc_guard | 采纳 | **登记已改**（tsv/finalization），30/82 从"flake"改如实登记；96 split-home #58 从 runtime-guard 改 gap。**deploy-tier 复跑门**验收（task 26） |
| H-3 die() ok/bad 恒真 | 采纳 | **已修**：ok→pass、bad→fail；加 helper-存在自守卫（未定义名 exit 2 而非 no-op）。die-frame 回归门现为活门 |
| H-4 unlock 零回复 fail-open | 采纳 | **已修**：probeLocks 透传 responder 计数；初判与 confirm 均对零回复 fail-closed |
| M-1 ack 无签发校验 | 采纳 | **已修**：见 C1（token 化 reply subject） |
| M-2 锁 reap catch-up/TOCTOU | 采纳 | **已修**：两个锁 reap pass 加 `reaperCaughtUp()` catch-up 门（claude 提供的两选一之一，且 `DELETE WHERE lease<now` 那个 CAS 因 RFC3339Nano 非字典序单调不安全，故取 catch-up）+ 源码门 |
| M-3 #58 reap 快照 TOCTOU | 采纳 | **已修**：`activeOBJStreams()` 改**每 bucket 重取**；对象删除加 **ModTime 时龄门**（`xferReapMinAge`，生产默认 2min，测试 0）；变异 `TestXferReapShieldsFreshObjects` |
| M-4 P9 heredoc 盲区 | 采纳 | **已修**：`sub(/^.*<<-?[ \t]*/,...)` 剥空白；负控证 `<< EOF`（带空格）现被识别 |
| M-5 INVERTED 配对 lint 未实现 | 采纳(缓议) | 承认从未实现。block 级配对需 block-scope 解析（line-based lint 做不干净）；**ledger-crosscheck 已接替"已注册缺陷坐绿 drill"这一半**（正是 #25/#26/#27 触发的场景），未注册倒置假绿留作 follow-up。见下详述 |
| M-6 drain 超时 exit 69 | 采纳 | **已修**：`callAdmin` 按 op 选超时，OpClusterDrain 用 60s（> drainNotice 30s），convergence 等待现返 exit 75 而非 69 |
| N-1 gofmt 不洁 | 采纳 | **已修**：`gofmt -w` 全部清（含专家未跟踪文件）；lint rc=0 |
| N-8 .d 杂物文件 | 采纳 | **已删** |
| N-9 poll_until desc 换行 | 采纳 | **已修**：desc `tr '\n' ' '` 展平，杜绝帧栈损坏→无界忙等 |
| N-2 PIN per-broker 文档 | 采纳 | **已修**：见 codex-H2 裁决——architecture §E.6 改为诚实 per-broker 语义 + N×10/min + argon2id 主防线 |
| N-3/N-4/N-5/N-6/N-7 | 部分采纳 | 见下逐条（含 deferred 与理由） |

### 逐条详述（阻塞项）

**CRIT-1（采纳·已修，发布级）**——逐行核实为真：`OpStateRehomeExposes` 臂 tick 2 时 `migrateExposes` 返回空（行已改指），`pendingHomeConvergence([])` 空→越门→RemoveServer；drain 重跑同型假 rc=0。修法完全按你的建议：不再信 self-destruct 的 migrate 返回值，改用**每次从当前状态重导出**的 `pendingRetireConvergence(nodeID)`（ALLOCATED homed、已移出 nodeID、agent 未确认 epoch 的集合）。fail-closed（membership op 已 fence，唯一来源即本 retire；无关残留只 hold→BLOCKED，绝不假完成）。补 `TestPendingRetireConvergenceIsDurable`：钉住"第二次调用仍非空（不自毁）"+"agent 确认后释放"两向。你 §5.3 指出的"无 hermetic 复现"现已闭合（该测试正是驱动 durable oracle 两次的复现）；retire 静默-越门的 live 负向臂（drill 71/40）我列入 deploy-tier 复跑门。

**H-1（采纳·已修）**——三处全属实。(a) kept-sites 41：first-retire 原 3 条 assert_ok 被 collapse 成 1 not_covered+1 assert（净 −1），违反反注水闸 A。**不降 baseline**（那才是"surrender"），改把两条 distinct claim（physically-leaves / reconnects-to-VOTER）**各记一条 not_covered(gap)**——coverage 是 trade 不是 drop，41 回到 28。(b) ledger #29：71 已是 INCOMPLETE，只是 owner 格没写 token，已补 `#29`。(c) ledger #34：确认 #34 是 R15 未修的**已证-open** 缺陷，而其 pin drill 74 会 flake 成 GREEN=假全清；按 71/#29 同法给 74 加**持久 not_covered(gap)**（每 run 恒记 #34-open），74 tsv 翻 INCOMPLETE。run-all.sh 现 rc=0。G-8 的 kept_sites 对照表我补进 finalization。

**H-2（采纳·登记已改，deploy-tier 复跑门验收）**——你的"稳定信号非并行 flake"证伪成立。30（agentless HALT 窗口 write-probe not_leader/503）与 82（grow-onto-recovered assert_fail 无 owner）不再归 flake。30 需一次定案实验区分"产品不保写可用"vs"谓词过严"；82 属 grow-onto-recovered 深层缺陷族（R16）。96 的 split-home #58 从 runtime-guard 改 gap（你一次复跑即证伪 finalization 的"nc_guard=0 EVERY run"，属实）。这些是 drill/tsv 登记 + 服务器复跑活，列入 task 26，不阻塞代码硬闸。

**H-3（采纳·已修）**——`ok`/`bad` 是 poll-reentrancy 的命名串台，rc=127 双分支 FAILS 不动=恒真。改 pass/fail，并加 helper-存在自守卫（`command -v pass fail field run_drill_body`，缺失 exit 2）杜绝同类拼写再度静默。die-frame 现为活回归门（实测两条检查都真判）。

**H-4（采纳·已修）**——probeClusterHealth 把所有失败塌成空 slice，unlock 把"看不见集群"当"无锁"。probeLocks 现返 responder 计数；`runClusterUnlock` 与 `confirmUnlocked` 均对零回复 fail-closed（含 dry-run，正是 codex 测试断言点）。codex 的 `TestCodexReviewClusterUnlockRejectsZeroHealthResponders` 现绿。

### 逐条详述（Medium/Note）

**M-2（采纳·已修）**：reap 决策读与 raft-apply-DELETE 之间 TOCTOU + 两个锁 pass 无 catch-up 门属实。你给了两选一：CAS 或 catch-up 门。CAS 版 `DELETE WHERE lease<reap-now` **有坑**——lease 值是 RFC3339Nano，含变长小数+`Z` 后缀，字典序**非**时间单调（`.5Z` < `Z`），SQL 字符串比较会误判。故取 **catch-up 门**（`reaperCaughtUp()`，applied>=commit）——直接堵你描述的主危险（滞后新 leader 读旧 lease 摘活锁）。加 `TestMembershipLockReapsGateOnCatchUp` 源码门。propose→apply 窗口的极窄残留（keeper 恰在满-TTL 静默后那 ms 复活续租）因 keeper latch 可检测，接受。

**M-3（采纳·已修）**：两个窗口都真。(1) `activeOBJStreams()` 快照移到**每 bucket 内重取**；(2) 对象删除加 **ModTime 时龄门** `xferReapMinAge`（生产 New 默认 2min，zero-value 测试 broker 为 0 保持 prompt-reap 语义），护住 sweep 中途新起/rehome 遗留的 in-flight 对象。变异 `TestXferReapShieldsFreshObjects` 证"新对象即使无 tracker 项也不被删；关掉 grace 才删"。`reaperCaughtUp` 的 boot/岛化平凡真我保留原 R15 设计 + 现加的 catch-up 讨论一致；per-transfer-owner 的 homeOwnsXferBucket 精化仍是 follow-up（split-home #58 残留，已在 coverage-boundary 披露）。

**M-4（采纳·已修）**：opener 剥 `<<-?` 后未剥空白，`cat << EOF`（带空格）整块被跳过不查。改 `[ \t]*`；awk 负控证带空格 opener 现被检出。lint-install 仍 rc=0（12 heredocs）。

**M-5（采纳·缓议）**：反注水闸 D 确实只有描述注释、无执行代码。诚实说明：block 级"含 INVERTED 的块须同块配对 product_red|assert_bug"需块作用域解析，本仓 line-based lint 结构上做不干净（且易误报）。按你给的第二选项**留档 + 指明接替闸**：**ledger-crosscheck 覆盖"已注册缺陷坐在绿 drill 里"这一半**（正是 #25/#26/#27 的形态，现已由它抓）；"未注册的倒置假绿"是真残留，列 follow-up（需引入块级 lint 或非-vacuity 证明覆盖）。当前 8 处 INVERTED（11/50/81/96）均在带 product_red/assert_bug 的文件内，无裸倒置。

**M-6（采纳·已修）**：drainNotice(30s) 压过 adminsock 客户端 5s 读超时→exit 69 掩盖 exit 75。`callAdmin` 现按 op 选超时（`adminCallTimeout`），OpClusterDrain 用 60s（> drainNotice + poll/reply 余量）；retire 无需（起 resumable op 即返，wait 是另路轮询）。

**N-1（采纳·已修）** gofmt -w 全清。 **N-8（采纳·已删）** .d 杂物。 **N-9（采纳·已修）** poll_until desc `tr '\n' ' '`。 **N-2（采纳·已修）** 见 codex-H2：§E.6 改诚实 per-broker + argon2id 主防线。

**N-3（部分采纳·缓议）** `reconcile nats` issuer-skew 检测在**检测本身出错时 fail-open**（confIssuer 读不出→不判→放行）属实、非对称。需已 root 才可利用 + doctor 独立 FATAL 兜底（纵深）。列 follow-up：`acct!="" 但 confIssuer==""` 时升 loud note/拒。**未在本批修**（低触发 + 有纵深），已记。

**N-4（部分采纳）** (a) **DOC-27（`/var/backups` 不可写）确实未修**——列 deploy-tier/runbook 修订门（drill 51 已自改 `/var/lib/tether`，50 落 PRODUCT-RED 钉它）。(b) `cluster_backup.go:248` fresh-box 缺失-conf 分支印不可执行的 `reconcile nats --conf <缺失>`：真、low，列 follow-up。(c) `applyClusterSeam` 的 `nats_conf_path` 硬编码默认、不与 `--nats-conf` 联动：真、low，列 follow-up。

**N-5（部分采纳）** admin events/audit `-n` 无上界=codex-L1，**已修**（server 端 `adminTailMaxN` clamp 两路 + 5000 scan 上界）。`--since`/`--kind`/`eventsMaxScan`/ctx 中途失败**无 truncated 标记**（静默返部分结果）：真，列 follow-up（加 truncated flag）。`xfer_reap_interval`/`disk_check_interval` 无上界：真、low，列 follow-up（现有下界钳）。

**N-6（部分采纳）** `Threads` 用 pprof threadcreate Count()（创建过的 M 数、不回落）注释略过头：真，注释可订正（follow-up）。#58 split-home/零节点 session bucket 集群模式永不可 reap + 建议告警计数防小盘顶满（racknerd 事故同型）：采纳观点，告警计数列 follow-up；本批 M-3 的 ModTime grace + coverage-boundary 披露已缩小面。

**N-7（部分采纳）** kept-sites/lint-install tokenizer 不感知引号（当前 37 drill 无真误计，属漂移通道）+ baseline 无 provenance、总和三个数字互不等：采纳。baseline provenance header 列 follow-up（本批已用 kept_sites 逐 drill --check 守住不下降）。

**N-* 其余**（N-9 已修上列）：`home_delivery.go attempts` map 慢泄漏（全释放后残留）——真、慢，NumGoroutine/fd 门抓不到；本批 C1 重构 outstanding 已加 TTL prune，attempts map 的 prune 列 follow-up。

### 硬闸与自证

- `go build/vet/test ./...`：**全绿**（无 FAIL）。`make lint`（golangci v2）：**rc=0**。`make e2e`：重跑中（结果附 finalization）。
- simcluster hermetic `run-all.sh`：**rc=0**（7 门全 PASS：poll-reentrancy / verdict-contract〔die-frame 现活门〕/ lint-drills / lint-install〔M-4 修〕/ ledger-crosscheck〔#29/#34 现有主〕/ r9d-nonvacuity / kept-sites〔41=28〕）。
- codex 5 个 reviewer 测试：4 个作真修复转绿（C1/H1/H3/M1 对应），1 个（PIN）按 H2 裁决透明重构为 per-broker 边界测试（见 codex 报告回复）。
- **deploy-tier 复跑门（task 26，weilandserver）**：验收 30/82 定案、96 nc_guard、41(28→INCOMPLETE)/74(→INCOMPLETE) 与 tsv 一致。这是 deploy-tier 分内活，随后跑。

**未达真 37/37 的诚实结论仍成立**：grow-onto-recovered（42/51/22/82）深层缺陷族属 HA 关键路径、值专属 R16；#57/#58-split-home/#34 如实披露为 open + 现有非绿 owner。本批把"宣布全绿的依据链"（harness 自检 rc=1、账目失配、回归门恒真、CRIT-1）逐条补实。

---

## 附：修复后的一轮多专家对抗性自审查（2026-07-20，7 lane→7 verifier，14 个 Opus 4.8 agent）

> 修完两份外审后，用 Workflow 对**本次 remediation 自身**做了一轮对抗性自审（每 lane 只审"我的修复是否错/漏/引入新 bug/削弱测试/洗白"），adversarial verify 后 **7 条确认**。**结果：我的修复本身引入了 1 个 blocker + 1 个 high + 3 medium**（集中在 C1/CRIT-1 两处最复杂改动），已全部修复；H1/H2/H3/M1/M-2/M-3/M-4/M-6/L1/harness 四个 lane **0 confirmed**（验证器逐条驳回其 raw findings，判定 sound）。

**SR-1（blocker，已修）· ack token per-push single-use 丢多-port ack**：我的 C1 修复一个 push 铸**一枚**覆盖全部 directive 的 single-use token，但真 agent **逐 port** 分别 ack（共享同一 reply subject），`handleHomeAck` 首个 ack 即删 token→后续 port ack 全丢→**≥2 expose 的 agent 只记录 1 个 port**→drain/retire/upgrade 对多-expose 节点永远假不收敛。**这是我引入的真回归。** 修：改 **per-PORT 消费**（acked port 从 `o.dirs` 移除，dirs 空才删 token）+ epoch 取 `min(issued, acked)`（同时闭合 SR-7 over-credit）。补 `TestHomeDeliveryConvergesEveryPortOfAMultiExposeAgent`，**变异验证**（恢复 single-use→14001=0 test RED）。

**SR-2（high，已修）· retire REHOME_EXPOSES hold 无界、永不 BLOCKED**：CRIT-1 修复使门**正确 hold** 了，但 hold 走 `recordOpError`（不增 opAttempts、无 deadline）→NATted-offline agent 无限 hold→`assertNoActiveOp` **永久 fence 整个 membership plane**；且我注释谎称"opMaxAttempts→BLOCKED"（该机制在此臂根本不触发）。修：新增 `boundRehomeConvergence`——首次 hold 盖一个 crash-safe replicated deadline（复用 `catchup_deadline`，仿 `toNatsRolledOut`/`boundCatchingUp`），超 `opRehomeConvergeTimeout`(10min) 路由 **BLOCKED**（nonzero terminal，`cluster ops confirm/abort` 可操作）。补 `TestRetireRehomeHoldIsBoundedToBlocked`（stamp→hold→BLOCKED 三段）。

**SR-3（medium，已修）· fake agent 批量 ack 掩盖 SR-1**：`newHomeTestAgent` 一条批量 ack（`json.Marshal(&ha)`+一次 Respond），注释谎称"mirror the real agent"——真 agent 逐 port ack。正因批量，single-use token 下多-port 在 fake 里**假通过**，全部收敛测试又只用单 port ⇒ 套件结构上抓不到 SR-1。修：fake 改**逐 port Respond**（真实还原），SR-1 的多-port 测试才有效。

**SR-4（medium，已修-注释+披露）· pendingRetireConvergence 全 fleet 作用域**：门扫全 fleet 的 home≠nodeID 未确认行（非 migrated 子集），无关节点旧 drain 遗留的 durable 未确认行会 hold 住 retire brk-a。我原注释"membership op fence 掉其余"被证伪（durable 行跨 op 存活）。**裁决保留 fleet-wide（唯一 fail-CLOSED 选项）**：精确作用域两种做法都 fail-OPEN——last_rehome_at recency window 在**慢速 drain 重跑**时漏（行掉出窗口→假 rc=0=CRIT-1 复发），target 作用域在**拓扑变更**时漏（旧 target 上的行被漏）；fail-closed 过近似严格更安全（绝不假完成），其唯一代价（被无关永久搁浅 expose hold）已由 SR-2 的 BLOCKED deadline 收敛为**运维可见**、非静默永久 fence。已订正注释、如实披露。

**SR-5（medium，已修）· CRIT-1 的 drain 半边无测试 pin**：retire 半边有 source-pin，drain 半边没有——单独 revert DrainNode 的 callsite 会重现 drain 假 rc=0 而套件全绿。补对称 `TestDrainConsultsTheDurableConvergenceGate`（正/负 pin：必须含 `pendingRetireConvergence(nodeID)`、必须不含 `awaitHomeConvergence("drain", nodeID, migrated`）。

**SR-6（low，已缓解）· durability 回归仅 helper 级 + BLOCKED 未测**：BLOCKED bound 现由 SR-2 的 `TestRetireRehomeHoldIsBoundedToBlocked` **行为级**测到；两-tick driveRetire 全驱动仍为 source-pin（retire+drain 双 pin 挡字面 revert）。我此前回复"hermetic 复现已闭合"**措辞过强**，在此更正为"oracle durability + BLOCKED bound 行为级测到；两-tick 全驱动为 source-pin"。

**SR-7（low，已修）· handleHomeAck over-credit issued epoch**：只用 issued epoch 忽略 ack body epoch，跨 epoch 竞态(E→E' 且 E-apply 在途)可 credit 到 E' 而 agent 只达 E=瞬时假收敛。已折入 SR-1 修复：`min(issued, ackBody.epoch)`——既防 inflation（≤issued）又防 over-credit（≤实际 acked）。

**四个 0-confirmed lane（验证器逐条驳回）**：lock-mutex-fsm（H1 条件获取 + read-back + M-2 catch-up 门）、reap-toctou（M-3 per-bucket 重取 + ModTime grace，clock-skew 残留按 NTP 假设接受）、cli-correctness（unlock/restore/L1/M-6，L1 audit 静默 clamp 按 server-OOM-闭合接受）、harness-honesty（41 trade 是真 trade 非 padding、74 持久 gap 诚实、verdict-contract die-frame 现活门、run-all rc=0）——均判 **sound**。

**硬闸复跑（SR 修复后，全绿）**：`go test -count=1 ./...` **全过**（无 FAIL）；`make lint` **0 issues**；触碰面 `-race`+仓库内建 NumGoroutine/fd 泄漏门（broker/agent/cmd/authcallout/cluster/concurrency）**无 DATA RACE、全过**；hermetic `run-all.sh` **rc=0**（SR 全为 Go 改动、未碰 drill）；`make e2e` 重跑中（SR 触碰 op-controller/home-delivery e2e 路径，复跑确认）。deploy-tier 验收（41/74 新登记 + 30/82/96 + CRIT-1/ack 的 drain/retire 面）在 weilandserver 复跑。

---

# 二轮外审 — 重审开发者的修复（2026-07-20 晚）· 判定：Fail（收敛至单一 HIGH 阻塞）

> 开发者已逐条采纳一轮外审并修复，附一轮自审（SR-1…SR-7，承认并修复自己引入的回归）。本轮重审**独立复核每处修复是否真闭合、有无残留或新引入的 bug**——不采信回复措辞。方法：5 个对抗性专家 lane（均 ≥ Opus 4.8）逐面变异/实测 + 主审逐行代码复核 + 本地 hermetic 自检复跑 + `remote.sh --build build` 正确 binary+镜像的 deploy-tier 复跑。

## 0. 重审结论

**判定：Fail —— 但性质从一轮的「发布级结构性缺陷 + 收官依据全面不成立」大幅收敛为「单个 HIGH 安全洞（SR-8）+ 两个 Medium 可用性 + 若干 Low」。修复质量高，绝大多数 finding 经我独立变异/实测确认真闭合。唯一硬阻塞是 SR-8。**

- **原发布级 blocker CRIT-1 真闭合**（逐行 + 变异实证：把 `pendingRetireConvergence` 查询改回 `home_broker=?` → `TestPendingRetireConvergenceIsDurable` 变红）。
- **一轮全部 High/Medium/Note 逐条复核，绝大多数真闭合**（见 §1 表）；`run-all.sh` 我本地实测 **rc=0**（7 门全 PASS，H-1 闭合），die-frame 回归门与 helper 自守卫经变异证明现为活门（H-3 闭合）。
- **唯一阻塞：SR-8（HIGH，安全）—— 开发者的核心 C1/SR-1 修复引入的新缺陷，M-1 实际未闭合。** 它使 `internal/broker` 包硬闸当前 RED，且直接绕过刚建立的 CRIT-1 rc 语义。详见 §2。
- 另有 F2/F3 两个 Medium（可用性，非安全，均由 CRIT-1 修复的恢复路径 / fleet-wide 作用域引入）。

## 1. 一轮 finding 逐条复核（独立验证）

| 一轮 # | 修复主张 | 重审判定 | 证据（主审独立） |
|---|---|---|---|
| **CRIT-1** | durable `pendingRetireConvergence` 替 self-destruct gate | **真闭合** | 逐行确认 `WHERE home_broker != nodeID` + 未确认 epoch 每 tick 重导出，tick 2 仍非空；变异改回 `home_broker=?` → 测试红；`boundRehomeConvergence` 收敛无界 hold 到 BLOCKED（变异去 stamp → 测试红） |
| **H-1** run-all rc=1 | 41 两条独立 gap(28) + #29/#34 owner | **真闭合** | 本地 `run-all.sh` **rc=0**，7 门全 PASS；41 的两条 gap 是互异真实 claim（over-specified fast-path → 诚实 gap，正确性不变量 `assert_ok` 保留，**非改松换绿**）；ledger #29→71、#34→74 顶层无条件 gap（74 结构不可能假 GREEN） |
| **H-2** 30/82 flake 归因 | 登记改如实、96 gap | **采纳 + 自我修正** | 见 §3：30/82 经第 3 次复跑证实为**间歇**（非我一轮判的"稳定"）；谓词/fixture 均未改松 |
| **H-3** die-frame ok/bad 恒真 | pass/fail + helper 自守卫 | **真闭合** | 变异：die() 回退 frame-unaware → 门红（rc=1）；rename `fail`→`bad` → helper 自守卫 exit 2。双双活门 |
| **H-4** unlock 零回复 | probe+confirm fail-closed | **真闭合，且不矫枉过正** | 两处均 fail-closed（含 dry-run）；合法 N=1 force-single 仍能 unlock（responder 非 leader-gated、cluster 模式无条件挂载）——不误伤生产 racknerd 单 broker |
| **M-1** ack 伪造 | token 化 | **未真正闭合 → SR-8（见 §2）** | token 经 ack subject 泄露 + SR-1 让 token 存活 → 跨 session sibling 伪造 |
| **M-2** 锁 reap TOCTOU | catch-up 门 | **主缺陷闭合**（Low 残留） | 两 pass 各加 `reaperCaughtUp()`；变异删除→红。残留：boot/岛化 `reaperCaughtUp` 平凡真（既有；leader-only + Propose-quorum + keeper latch 兜底），建议加 `CommitIndex()>0` 硬化 |
| **M-3** #58 reap 快照 | 每 bucket 重取 + ModTime grace | **真闭合**（Low 残留已披露） | 每 bucket 重取（:87）+ ModTime grace（:115）；变异关 grace → `TestXferReapShieldsFreshObjects` 红。残留：rehome + 老龄(>2min)完成对象仍可被新 home 删（开发者已披露 follow-up，暴露面小） |
| **M-4** lint-install 空格 heredoc | 剥空白 | **真闭合** | 变异负控：`cat << EOF`(空格)+反引号 → rc=1 检出；干净体 → rc=0 放行（真解析非乱报） |
| **M-5** INVERTED lint 未实现 | 缓议 + ledger 接替 | **部分闭合（可接受）** | 8 处 INVERTED 全在注释行、无裸倒置；"未注册倒置假绿"留 follow-up（LOW） |
| **M-6** drain exit 69 | adminCallTimeout(op) | **真闭合** | OpClusterDrain=60s>drainNotice 30s，exit 75 链路完整；无别的慢 op 漏配（OpClusterAdd 已死路由） |
| **L1** admin tail 无上界 | clamp 5000 | **真闭合** | audit+events 两路 clamp，负数/0→50 |
| **N-1** gofmt | gofmt -w | **真闭合** | 暂存 .go 文件 gofmt -l **0 命中** |
| **N-8** 杂物文件 | 删除 | **真闭合** | verdict-contract-test.sh.d 已删 |
| N-2/N-3/N-4/N-5/N-6/N-7/N-9 | 部分采纳 + follow-up | **采纳合理** | E.6 per-broker 文档、poll_until desc 展平等已修；DOC-27/skew fail-open/truncated flag 等 low 项列 follow-up，判断合理 |

**硬闸（我方独立复跑）**：`go test -count=1 ./...`（codex SR-8 测试写入工作树前）全绿；`make lint` rc=0；`go test -race`（cmd/tether/agent/authcallout/concurrency）无 DATA RACE + 泄漏门过；**但 `go test ./internal/broker/` 现 RED**（SR-8，见 §2）。

## 2. 唯一硬阻塞 — SR-8（HIGH，安全）：C1/SR-1 修复引入的跨 session ack 伪造，M-1 未真正闭合

- **严重度**：High · **确定度**：**已确认**（主审独立验证机理 + 两个 lane 独立命中 + 现成 RED 测试）
- **文件**：`internal/broker/home_delivery.go`（`handleHomeAck` 268-319、`recordHomeOutstanding` 213-241、`subscribeHomeAcks` 185-200）；根因在 `internal/auth/permissions.go:189,209`（agent 的 **Pub 与 Sub ACL 均含裸 `_INBOX.>`，跨 session、无 sid 作用域**——我已 Read 确认）。
- **机理**（独立验证，不依赖 codex）：
  1. push 的 ack 通道 = `nats.NewInbox()` 生成的 `_INBOX.<rand>.<token>`，token 由 `crypto/rand` 16B（不可猜）。push 方向（broker→agent 的 forwarded subject）是 nid 私有，安全。
  2. **但 ack 方向**：受害 agent 把 ack `Publish` 到 `_INBOX.<rand>.<token>`，而**每个 agent（任意 session）都被授予 `Sub _INBOX.>`** → 一旦受害者 ack 首个 port，token 就在总线上对全体 agent 明文暴露（订阅者读得到 `msg.Subject`）。
  3. **SR-1 的修复刻意让 token 在首个 port ack 后存活**（收集 sibling ack，:316 仅 `dirs` 空才删 token）——于是**已泄露的 token 对该 push 里尚未 ack 的 sibling port 仍是有效授权**。
  4. 任意 session 的恶意/被攻陷 agent 用泄露 token 伪造 sibling port 的 ack `{siblingPort: 任意}` → `handleHomeAck` 命中 `o.dirs[sibling]` → `applied[sibling]=min(issued,·)=issued` = **完美假收敛**（`min(issued,acked)` 挡不住，因 `issued` 恰是 pass 要求的目标 epoch）。
  5. `homeAssignmentApplied` 转真 → 投递 pass 静默；**drain/retire/upgrade 读到 sibling port 已收敛 → 假 rc=0 → retire 走向 RemoveServer**，而真 agent 从未 apply 该 port，隧道永久钉死被 drain 的 broker。**这直接绕过 CRIT-1 刚建立的 rc 语义**（限定多-expose 节点）。
- **为何是"M-1 未真正闭合"**：M-1（一轮报的跨 session ack 伪造）的修复只是把攻击面从"裸 port key"换成"token 存活期泄露 + sibling 伪造"，跨 session ACL 隔离不变量仍被违反。开发者的自审 SR-1 抓到了"per-port 消费"的功能回归，但**没抓到 per-port 消费 + token 存活 + `_INBOX.>` blanket ACL 三者叠加的安全洞**。
- **硬闸影响**：`go test ./internal/broker/` 因 `TestCodexReReviewHomeAckTokenDoesNotAuthorizeUnackedSiblingPort` **确定性 RED**（`codex_..._test.go:73` "token disclosed by one port authorized an unacked sibling port: epoch=9"）。该测试 codex 前缀且工作树 unstaged（staged 批次本身绿），但**缺陷客观存在于产品代码**，与测试是否 staged 无关；一旦该测试或等价测试进 index，`make test` 提交硬闸即红。
- **限定条件**：多-expose 节点 + 恶意/被攻陷 agent（已认证的其它 session）+ 受害者已 ack ≥1 port（drain 场景常态）。需要一个恶意 agent 前提，故非 blocker；但比 M-1 严重（完整利用链 + 绕过 CRIT-1 + 违反架构 session 隔离 + 现成 RED 测试），判 **High**。
- **修复方向**（供开发者）：**per-PORT single-use token**（每 directive 一枚，各自 ack 各自消费即删——同时闭合 SR-1 与 SR-8），或收窄 ack ACL 把 `_INBOX.>` blanket 授权改为绑 owning nid 的子树。

## 3. 一轮判断的诚实自我修正（30/82 是间歇，非"稳定"）

一轮我基于 2 次复跑（r15ser -j1 + claudeext -j2）判 30/82"稳定 ASSERT-FAIL、非并行 flake"。本轮第 3 次复跑（rev2 -j2，正确 binary+镜像）：**30=INCOMPLETE、82=INCOMPLETE**——两者均转绿档。结合三次样本，30/82 实为**间歇**：
- **30**：`_probe_clean` 失败条件（`not_leader|503|no.responder|unavailable|no servers|WRITEFAIL`）**逐字未改松**（git diff 确认，**非改松换绿**）；write-probe 在 agentless HALT 窗口**偶发**撞上瞬时 not_leader（撞→ASSERT-FAIL，未撞→INCOMPLETE）。根因（HALT 窗口是否真短暂不可写=产品 vs 谓词过严）**仍未定案**，开发者也承认需定案实验。
- **82**：grow-onto-recovered 深层缺陷族（22/42/51/82 同根）的**间歇脆弱性**（撞→`grow brk2 N=2 failed` ASSERT-FAIL，未撞→INCOMPLETE）。
- **修正**：finalization 的"并行 flake"归因仍不完全对（-j1 也会红，是缺陷间歇不是并行），但"时序/间歇敏感"这半我一轮低估了。开发者的 H-2 登记处置（不再归 flake、需定案）方向合理。这不改变"tsv/台账对间歇缺陷登记不稳定"的核心观察。

## 4. deploy-tier 复跑（rev2，`--build build` 正确 binary+镜像，-j2）

| drill | rev2 verdict | 判定 |
|---|---|---|
| 30-rolling-upgrade | INCOMPLETE (pass=53) | 时序间歇（谓词未改松，见 §3） |
| 40-drain-retire | **GREEN** (pass=37) | happy-path retire 未被 CRIT-1 修复破坏 ✓ |
| 41-shrink-to-standalone | INCOMPLETE (2 gap, kept=28) | H-1(a) 诚实 trade，反注水闸 A 过 |
| 42-rejoin-returning | PRODUCT-RED (pass=42) | grow-onto-recovered 深层缺陷（R16，符合期望）|
| 71-expose-rehome-failover | ASSERT-FAIL (pass=7) | **#29 家族间歇**（`agent_rejected:frpc_failed` fixture 没建起）——非回归，drill 诚实暴露 RED FIXTURE |
| 74-rebalance-on-return | ASSERT-FAIL (pass=15) | **#34 间歇**（SKEW baseline 没建起）——非回归，74 顶层无条件 #34 gap |
| 82-agent-onboarding-invite | INCOMPLETE (pass=29) | grow 这轮没撞 grow-onto-recovered（间歇，见 §3）|
| 96-mid-flight-chaos | INCOMPLETE (pass=33, **nc_guard=1**) | 混沌 drill；#58 orphan 这轮未 relapse（product_red=0）。**nc_guard=1 仍触发 96-A2 split-home #58 guard** —— 印证开发者的「runtime-guard→gap」改动尚未落地（其明确列为 task 26 deploy-tier 待办、不阻塞代码硬闸），G-4「runtime-guard 判定运行中不触发」仍未满足，我方 rev2 这一轮 96 不构成 G-4 合格判定轮 |

**净读**：**无开发者修复引入的 deploy-tier 回归**（40 GREEN 证明 happy-path retire 正常；71/74 的红是 #29/#34 两个已知开放缺陷的间歇复现，drill 诚实暴露而非 silent GREEN）。**但重要副作用**：71 是 CRIT-1（P1）的**唯一** deploy-tier 验证 drill（"drain-migrate = P1/R8 deploy-tier verifier"），本轮其 fixture 因 #29 间歇没建起、Arm B 没跑到 ⇒ **CRIT-1 修复的 live deploy-tier 验证仍未达成**——CRIT-1 的正确性目前仅由 hermetic 测试 + 逐行分析支撑，无 live 实证；retire 静默-越门的 live 负向臂（开发者列 task 26）仍未补。

## 5. Medium / Low 残留（重审新发现，均非安全阻塞）

- **F2-retire-confirm【Medium 可用性】**：`boundRehomeConvergence` 用 `CatchupDeadline==0` 判"首次进入 stamp 新 deadline"，但 retire 因 rehome 超时进 BLOCKED 后，`ConfirmOp` 不重置 `catchup_deadline`（残留过期值）→ 重驱动回 REHOME_EXPOSES 时 `pending>0` → 命中 `!=0 && now>deadline` → **第一 tick 立即再 BLOCKED，无新 10min 窗口**（对短暂滞后 agent，confirm 恢复 UX 退化）。非对称：join confirm 重入会重 stamp，retire 不会。安全不受损（BLOCKED 非终态、operator 可见、绝不越门）。建议：retire confirm 重入把 `catchup_deadline` 归零对齐 join。
- **F3-fleet-wide-cost【Medium 可用性】**：`pendingRetireConvergence` fleet-wide（含无关节点未确认 expose）→ 任一节点存在永久 stranded expose 时，对**任意**节点 drain/retire 都先 hold 10min→BLOCKED（叠加 F2 则每次 confirm 0 秒宽限）。fail-closed 安全成立（每 op 独立 operator 可见），但"fleet 里任一 stranded expose 冻结全体 membership drain/retire"是真运维 footgun；BLOCKED 文案固定串不枚举（须 `ops show` 看 last_error），"retire brk-a 看到 brk-c expose 报错"不直观。
- **锁reap-F2【Low】**：`reaperCaughtUp()` 在 fresh-boot/岛化 follower 平凡为真（既有非本批引入）；锁 reap leader-only + Propose-quorum + keeper latch 近乎兜底，建议加 `n.CommitIndex()>0` 硬化 boot 窗口。
- **M-3-F5【Low，已披露】**：rehome + 老龄(>2min)完成对象仍可被新 home 删（暴露面小：真 in-flight 未写完对象 `store.List` 列不到）。
- **测试缺口**：M-2 门只有源码 grep 测试无行为测试（锁reap-F3）；`xferReapMinAge=2min` 生产接线无测试钉住（锁reap-F4）——把常量改 0 会静默关 grace 且无测试变红。
- **附带（既存非本批）**：`OpStateStreamsAtTarget` 无 deadline/bound，streams 永不达标可无限 hold（CRIT-1 lane 指出，先于本次修复既存）。

## 6. 重审建议

1. **合入前必修（唯一 Fail 阻塞）**：SR-8 —— per-PORT single-use token 或收窄 ack ACL；补一条跨 session sibling 伪造被拒的测试（进 index 使 `make test` 门守住）。这直接决定 CRIT-1 的 rc 语义在多-expose 节点是否真成立。
2. **强烈建议同批**：F2（retire confirm 无新窗口，恢复路径可用性）、锁reap-F2（`CommitIndex()>0` 硬化）、补 M-2 行为测试 + grace 接线测试。
3. **需一次 live 实证**：CRIT-1 的 deploy-tier 验证（71 Arm B 或 40 的 retire 静默-越门负向臂）——目前只有 hermetic 覆盖。
4. **可 follow-up**：F3 作用域 scoping、M-3-F5、30 定案实验、DOC-27、其余一轮 N-* follow-up 项。

**总评**：这一轮修复**方向正确、执行质量高**——原发布级 CRIT-1 真正闭合，收官层的账目/自检门（run-all rc=0、die-frame 活门、41 反注水闸、#29/#34 owner）全部补实，一轮绝大多数 finding 经独立变异/实测确认闭合。**唯一未闭合的是 M-1 → SR-8**：开发者最核心的 C1/SR-1 修复引入了一个新的跨 session ack 伪造洞，它恰好绕过刚建立的 CRIT-1 rc 语义、使 broker 包硬闸 RED。**在 SR-8 闭合前不放行**；闭合后（叠加 F2/F3 的可用性收尾 + 一次 CRIT-1 live 实证）即具备通过条件。

## 主进程回复（二轮 remediation，2026-07-21）

> 二轮判定 **Fail（唯一 HIGH 阻塞 SR-8）** 完全成立且极有价值——SR-8 是我最核心的 C1/SR-1 修复引入的**真跨 session ack 伪造洞**，我此前的"per-port 存活 token"安全论证是**错的**。逐条闭合如下。

**SR-8（blocker · 已闭合）—— C1/SR-1 的跨 session ack 伪造**：机理我完全采纳并独立复核确认。关键盲点：**token 经 ack 方向泄露**——ack 走共享 `_INBOX`，`permissions.go:189` 给每个 agent（任意 session）`Sub _INBOX.>`，受害 agent ack 首个 port 时 token 就在总线上对全体明文暴露；SR-1 又刻意让 token **存活**收集 sibling ack ⇒ 已泄露（且已被消费）的 token 对同 push 里**尚未 ack 的 sibling port 仍有效** ⇒ 任意 session 恶意 agent 伪造 sibling 假收敛、绕过 CRIT-1 rc 语义。**修复（采纳你给的第一方向）**：**per-DIRECTIVE single-use token**——`pushHomeAssignment` 改为**每 directive 一条单独 push、各自一枚 token**；token 只在它自己那个 port 被 ack 时才上总线、且**同时被消费**，泄露时已 spent、且只指一个（已收敛的）port，泄露 token 授权不了任何东西（伪造别的 port 需要另一枚**它从没见过**的 token）。安全性不再依赖 token 保密。补 `TestHomeAckPerDirectiveTokenRejectsSiblingForgery`（进 index）：(a) 多-port push 铸**每-directive** token（每 token 恰 1 port），(b) 受害 ack P1 泄露 token_P1 后，攻击者用 token_P1 伪造 sibling P2 **被拒**（applied[P2] 不动）。**变异实证**：把 push 改回单枚共享 token ⇒ 该测试 RED（"a multi-port push minted a token covering 2 ports"）。`go test ./internal/broker/` 现绿。

**F2（medium · 已闭合）—— retire confirm 无新窗口**：属实。BLOCKED retire `cluster ops confirm` 后重入 DRAIN_REQUESTED 但**未重置** catchup_deadline ⇒ boundRehomeConvergence 见旧的已过期 deadline 立即再 BLOCK，confirm 成 no-op。修：ConfirmOp 的 retire 分支重入时 `CatchupDeadline=0`（`SetBarrier` 同写），boundRehomeConvergence 下次 hold 重新盖窗。`TestRetireRehomeHoldIsBoundedToBlocked` 加第 4 段实测（ConfirmOp→DRAIN_REQUESTED 且 deadline==0）。

**F3（medium · 已闭合）—— fleet-wide gate 冻结全体 drain/retire**：属实且比我一轮披露的更重。修：`pendingRetireConvergence(nodeID, rehomedSince)` 加 **last_rehome_at 近窗**——**RETIRE op** 传 `now−2×opRehomeConvergeTimeout` 窗口：因 boundRehomeConvergence 在其自身迁移行老化出窗前就 BLOCK 该 op，**scoping 安全（无 fail-open）**且无关的旧搁浅行落窗外 ⇒ 单个死租户不再冻结 membership；**同步 DRAIN** 传零窗（保持 fleet-wide fail-closed——近窗对慢速 drain 重跑会 fail-open=CRIT-1 复发，而 drain 不 fence membership、over-wait 有 deadline 界）。空/不可解析 last_rehome_at 一律 INCLUDE（fail-closed）。补 `TestPendingRetireConvergenceRecencyWindow`（旧搁浅行被窗排除、近行报出；零窗 fleet-wide 两行都报）。

**96 split-home #58（G-4 · 已改）**：属实——split-home #58 residual 之前误标 `runtime-guard`，实为**defect-tied gap**（该 drill 会话结构上恒 split-home，homeOwnsXferBucket 恒 false、reap 结构上不可能 fire，非 intrinsic 非确定性）。已改 `runtime-guard→gap`（96-A2:447），nc_guard 该路径 →0，G-4 就此路径满足；owner=#58-split-home（follow-up=homeOwnsXferBucket 精化到 per-transfer-owner）。lint-drills/run-all rc=0。

**诚实采纳你的自我修正**：30/82 我一轮基于 2 样本判"稳定 ASSERT-FAIL"确实过重——你第 3 次复跑二者转 INCOMPLETE，实为**间歇**（谓词/fixture 未改松）。我更正 finalization/tsv 的归因为"间歇 flake band（含 ASSERT-FAIL 尾）",不再称"稳定"。

**两个如实记录的待验证项**：(1) **CRIT-1 的 live deploy-tier 实证**——71 是唯一验证 drill，其 fixture 因 #29（已知 open）本轮/你那轮都没建起，故 CRIT-1 的 live 证据仍缺；hermetic 层已由 `TestPendingRetireConvergenceIsDurable`+变异 + drain/retire 双 source-pin 钉住，live 证据我列为 deploy-tier 待办（依赖 #29 fixture 或改用不经 #29 的验证臂）。(2) 96 的 nc_guard=0 需一次 96 deploy-tier 复跑确认（本轮已重建 SR-8 镜像 + 跑 96，结果附下）。

**硬闸（二轮 remediation 后）**：`go test ./...` 绿（唯一 FAIL=`internal/tunnel` 既有 reconnect flake，我未碰该包、隔离 5/5 过）；`make lint` 0 issues；`-race`+泄漏门 clean（负载 flake 已清跑复核 rc=0）；hermetic `run-all.sh` rc=0（含 96 改动）。`make e2e`：首跑一次 `TestD7Matrix/ForceSingleRecoverRestart` 在**并发重负载**下 flake（本机同时跑服务器 deploy-tier ssh + 子 agent），**隔离 3/3 过 + 全 D7 matrix 单跑过**（证明我的 op-controller/clusterdrain 改动不回归 membership/ops），已启一次**无并发负载的干净复跑**取定论 rc（CLAUDE.md 已记 force-single/raft 在重负载下必 flake）。deploy-tier 用**重建的 SR-8 镜像**在服务器跑 96/71/40（serial，验 nc_guard=0 + CRIT-1/ack + drain-retire，结果附下）。

**二轮修复的自审（对抗子 agent）——又抓到我自己引入的一个 HIGH，已修**：对 SR-8/F2/F3 三处修复跑了一轮对抗性复核。SR-8、F2 判 **sound**；但 **F3 的滑动窗设计有 fail-open**（子 agent 独立构造 + 我复核确认）：F3 用 `now−2×opRehomeConvergeTimeout` 滑动窗，而 `last_rehome_at` 固定在首次 migrate 的 t0、F2 的 confirm 又重置 deadline 时钟 ⇒ 二者**漂移**；`cluster ops confirm` 重试后，在 **t0+20min** 那一刻行龄出滑动窗（早于新 deadline `tc+10`），gate 落空 → **静默 RemoveServer、数据面搁浅、rc=0** —— 正是本批要杀的 P1 谎言，被我的 F3 修复反手引入。**修复**：retire 门改用**固定 origin `op.CreatedAt`**（migrate 恒在 op 创建后 ⇒ 行恒 `>= CreatedAt`、**永不龄出**，即便跨多次 confirm 重试；无关旧行 rehomed 于本 op 创建前 ⇒ 排除）；不可解析 created_at 退回零窗（fleet-wide, fail-CLOSED）。drain 仍零窗。补 `TestPendingRetireConvergenceRecencyWindow` 的**对比臂**（固定 origin 不龄出 vs 滑动窗会龄出=证伪 fail-open）+ 源码门（driveRetire 必须 `parseClusterTime(op.CreatedAt)`、禁 `a.now().Add(` 滑动窗）。

**结论**：SR-8（唯一硬阻塞）+ F2 + F3（含其自审发现的 fail-open）已全部闭合，各带测试进 index；96 G-4 路径已改。

**硬闸（全绿）**：`go test ./...` 绿 · `make lint` 0 issues · `-race`+泄漏门 clean · `run-all.sh` rc=0 · **`make e2e` 干净复跑 rc=0（556s，证首跑 `ForceSingleRecoverRestart` 是重负载 flake）**。

**deploy-tier（重建 SR-8 镜像，服务器 serial）—— 两大实证到手**：
- **96：`verdict=PRODUCT-RED nc_gap=5 **nc_guard=0** pass=33`** —— split-home #58 residual 现落 **gap（nc_guard=0）**，**G-4 就此满足**（正是你要的可交付）；96 verdict 仍 PRODUCT-RED（arm A #57/#58 source-certain），与 tsv 一致。
- **71：`verdict=INCOMPLETE **nc_guard=0** pass=28`，且本轮 #29 fixture 建起了**（你那轮没建起）—— 于是 **CRIT-1 的 live deploy-tier 实证首次到手**，B-arm 全 PASS：**B-cmd** `cluster drain brk3 rc=0` 是 R8 强声明（rc=0 仅当 awaitHomeConvergence 见每个迁移 expose 的 agent **ACK** 新 home）；**B-migrate**（P1 终态）wstrand 迁到 survivor 并经**真隧道**返回 sentinel（数据面跟随）；**B-silent** 迁移在 agent **静默**下完成（journal 有 `home directives pushed`/**`home applied-ack`**、零 re-register）；**B-noreexec** agent PID 不变。⇒ **CRIT-1 + per-directive ack（SR-8）在真部署栈 live 验证通过**，一举补上你记录的"CRIT-1 live 实证仍缺"。71 落 INCOMPLETE（唯一 gap=G/F stickiness，nc_guard=0），与 tsv 一致。
- **40-drain-retire = GREEN**（drain/retire 面无回归）。
- **全套 3 drill（2065s）：GREEN=1 · PRODUCT-RED=1 · INCOMPLETE=1 · ASSERT-FAIL=0 · SETUP-RED=0 · INFRA-ABORT=0** —— SR-8/F2/F3 改动**零回归**，三个 verdict 均与 tsv 期望一致。

**综上（二轮 + 其自审）全部闭合、且有真实证据**：SR-8（per-directive token，hermetic 变异 + **71 B-arm live**）· F2（confirm 重置窗，test）· F3（fixed-origin，含自审发现的 fail-open，contrast test + source-pin）· 96 G-4（nc_guard=0，**server live**）· CRIT-1（**71 B-arm live 首证**）。硬闸五门全绿（test/lint/race/run-all/e2e）。停在外审门待你三轮终审。

---

# 三轮外审 — 重审 SR-8 / F2 / F3 的修复（2026-07-21）· 判定：**Pass（Conditional，仅余 Low 残留）**

> 开发者已修复二轮的唯一 HIGH 阻塞 SR-8 + 两个 Medium（F2/F3）。本轮独立复核（2 个对抗性专家 lane 变异验证 + 主审逐行代码复核 + 全套硬闸复跑 + `remote.sh --build build` 的 deploy-tier 复跑）确认**三处全部真闭合**，且**二轮遗留的 CRIT-1 live 验证缺口本轮闭合**。无阻塞项残留。

## 0. 三轮结论

**判定：Pass（Conditional）。** 一轮与二轮的**全部阻塞项**——CRIT-1（发布级）、H-1..H-4（收官层）、SR-8（HIGH 安全）、F2/F3（Medium 可用性）——均经我独立**变异/实测/live** 确认真闭合。硬闸全绿。剩余仅 **Low 残留 + follow-up**，无一阻塞放行。

## 1. 三处修复逐条复核

### SR-8（唯一 HIGH，已闭合）—— per-DIRECTIVE single-use token

- **判定：真闭合**（主审逐行 + SR-8 对抗 lane 独立确认无 SR-9 + 变异 + broker 包 PASS + race PASS）。
- **修复**：`pushHomeAssignment`（`home_delivery.go:359-403`）改为**每 directive 一条独立 push、各自一枚只含 1 port 的 single-use token**。安全模型正确：token 只含 1 port，在其唯一 port 被 ack 时即消费删除 → **泄露时已 spent、只指一个已收敛 port** → 泄露 token 授权不了任何东西；伪造别的 port 需要另一枚**从没上过总线的 secret token**（`crypto/rand` 128-bit）。**安全性不再依赖 token 保密**——这正是对 SR-8 根因的正确回应。
- **对抗 lane 排除了最危险的潜在 SR-9**：agent 侧 `applyHomeDirectives`（`agent.go:1240-1266`）是**纯 per-port**，无"按完整集合 reconcile、拆除不在本集合内的 home"逻辑，故把 1 个 N-directive push 拆成 N 个单-directive push **功能等价、不误拆**。补强论证：`permissions.go:192` agent Sub 白名单按 nid 私有，push 侧 token 泄露前不可见。
- **竞态无害**：token 只含 1 port，无论真 ack 还是伪造 ack 先到 broker，只影响那个正在收敛的 port，伪造它无意义；且 `handleHomeAck` 函数体逐字节未改（既存 fail-safe，非本次引入）。
- **变异实证**：把 push 改回"整 assignment 一枚共享 token" → `TestHomeAckPerDirectiveTokenRejectsSiblingForgery` 变红（"a multi-port push minted a token covering 2 ports"）——真守卫。二轮的 codex sibling 测试现在绿，`go test ./internal/broker/` **PASS**。
- **残留（LOW，纯文档）**：`handleHomeAck:296-301` 与 `outstanding` 字段注释仍描述旧的多-port-同-token 语义（per-directive 后该分支永不触发，per-port-consume 成防御性冗余）——建议随手更新注释，不阻塞。

### F2（Medium，已闭合）—— retire confirm 无新窗口

- **判定：真闭合**（F2/F3 lane 逐行 + 变异）。`ConfirmOp` 的 retire 分支（`cluster_operation_controller.go:251-263`）在 `SetBarrier=true` 时把 `catchup_deadline=0` **真写入**（`operation_ops.go:149-152`，0 是真值非"保留"）→ 下 tick `boundRehomeConvergence` 见 `==0` 重新 stamp 新 10min deadline，消除了"confirm 成 no-op、立即再 BLOCK"。正确区分 join/retire（retire 分支闭合 return，join 的 CATCHING_UP 语义未触及）。`TestRetireRehomeHoldIsBoundedToBlocked` 加第 4 段断言 confirm 后 `CatchupDeadline==0`，绿。

### F3（Medium，已闭合）—— fleet-wide gate 冻结全体 drain/retire

- **判定：真闭合，无当前可复现 fail-open**（F2/F3 lane 原子性核验 + 变异 + 主审确认前提）。
- **修复**：`pendingRetireConvergence(nodeID, rehomedSince)` 加 recency window。**RETIRE op 传固定 `op.CreatedAt`**（非滑动 `now−Δ`）——`op.CreatedAt` 不可变（仅 `PlanClusterOpStart` 写一次），本 op 迁移行 `last_rehome_at >= op.CreatedAt` **永不老化出窗**（即使 confirm 重试重启 deadline），故 CRIT-1 保持、不 fail-open；无关旧 stranded 行 `< op.CreatedAt` 落窗外、不再冻结 membership。**同步 DRAIN 传零窗**（fleet-wide fail-closed，drain 不 fence membership、over-wait 由 drain deadline 界）。空/不可解析 `last_rehome_at` → INCLUDE（fail-closed）。
- **关键前提已确认（原子性）**：`PlanReassignHome`（`internal/port/plan.go:290-294`）是**单条 all-literal UPDATE**，`home_broker`/`epoch`/`last_rehome_at` 三列共用同一个 `epoch < newEpoch` CAS 守卫 —— 不存在"epoch 前进而 last_rehome_at 滞后"的行，CRIT-1 复发所需的窗口结构上不存在。这是既有 C6 机制（非本批新加），F3 正确复用。
- **CRIT-1 仍闭合**（`TestPendingRetireConvergenceIsDurable` 绿；recency 只丢无关前序行，从不丢本 op 的行）；F2+F3 配合环无死循环/无假完成；变异 A（改滑动窗口）被 source-pin `TestRetireConsultsTheConvergenceGate` 捕捉。
- **残留（均 Low）**：
  - **RESIDUAL-1**（理论时钟）：跨-leader 且新 leader 时钟回拨 >创建→migrate 实耗时（NTP 级 >10–15s 偏差）时，本 op 行可能 `last_rehome_at < op.CreatedAt` → fail-open。极难触发，**固定原点严格优于弃用的滑动窗口（残留非回归）**，符合安全实用主义，仅记录。
  - **RESIDUAL-2**（测试缺口）：F3 的全部安全性压在 last_rehome_at 原子 stamp 上，但**无回归钉**——变异删掉 `PlanReassignHome` 的 stamp，全套仍绿。建议补一条 plan 级 source-pin（钉 UPDATE 同含 `epoch=` 与 `last_rehome_at=`），防未来重构静默 fail-open。

## 2. ★ CRIT-1 的 live deploy-tier 验证达成（二轮缺口闭合）

二轮我指出 CRIT-1 的正确性仅有 hermetic 测试 + 逐行分析、**无 live 实证**（71 fixture 因 #29 间歇没建起）。本轮 rev3，**71 的 fixture 建起来了**（pass=28 vs rev2 pass=7），**Arm B（"drain-migrate = P1/R8 deploy-tier verifier"）完整跑通、全 PASS**：

- **B-cmd [R8 rc]**：`cluster drain brk3` 返回 rc=0 —— 且 drill 断言这是强 claim：`DrainNode` 只在 `awaitHomeConvergence` 见**每个**迁移 expose 的 agent ACK 新 home epoch 后才到达 rc=0 尾（`home_convergence.go:122-142`）。**这正是 CRIT-1 rc 语义的 live 证明**（修复前"raft 写了、没人通知"的假 rc=0 正是此臂要抓的）。
- **B-migrate [P1 终态]**：wstrand 迁到 survivor voter，且该 broker 公网端口在 drain 后 240s 内经**真隧道**返回精确 sentinel —— 数据面真跟随（非"发布了 directive"的中间态）。
- **B-silent [P1 前提]**：迁移在 agt1 **保持静默**时投递（其 journal 该窗口含 ≥1 条 ACTIVE home-delivery 行 + **零** re-register/rebuild 行）—— 证明是 **R8 push 而非 incidental reconnect** 携带 directive。**这恰是 CRIT-1/P1 修复的本质不变量：agent 完全静默下数据面有界跟随。**
- **B-event [#30]**：admin events 读到 `expose_rehomed` 事件。

71 落 INCOMPLETE 只因剩 G/F topology 结构性 gap（home-return stickiness + rehome_stalled，需未构造的拓扑），**与 CRIT-1 无关**。⇒ **CRIT-1 现同时有 hermetic + live deploy-tier 双重验证。**

## 3. 硬闸（我方独立复跑，三轮改动后）

- `go test -count=1 ./...` → **0 FAIL**（全绿）。
- `go test -race`（broker+agent）→ 无 DATA RACE，泄漏门过。
- `make lint` → **0 issues**；`gofmt -l`（改动 .go）→ **0 命中**。
- `go test ./internal/broker/` → **PASS**（SR-8 闭合，`TestHomeAckPerDirectiveTokenRejectsSiblingForgery` + 二轮 codex sibling 测试 + F2/F3 新测试全绿）。

## 4. deploy-tier 复跑（rev3，`--build build`，-j2）

| drill | rev3 verdict | 判定 |
|---|---|---|
| 40-drain-retire | **GREEN** (pass=37) | drain/retire happy-path 未被 per-directive token / F2/F3 改动破坏 ✓ |
| 60-user-journey | **GREEN** (pass=38) | 端到端未坏 ✓ |
| 70-expose-journey | **GREEN** (pass=28) | expose 生命周期未坏 ✓ |
| 71-expose-rehome-failover | INCOMPLETE (pass=28) | **Arm B（CRIT-1 live verifier）全 PASS**（见 §2）；仅剩 G/F topology 结构性 gap |
| 30-rolling-upgrade | INCOMPLETE (pass=53) | 时序间歇（一/二轮已定性，谓词未改松），符合预期 |
| 96-mid-flight-chaos | PRODUCT-RED (pass=35, product_red=2, **nc_guard=0**) | #58 split-home orphan relapse（已披露深层缺陷 follow-up，与 per-directive/F2/F3 改动无关）；本轮 split-home guard 未触发（nc_guard=0）——非三轮引入的回归 |

**净读**：per-directive token + F2/F3 改动**无 deploy-tier 回归**（40/60/70 全 GREEN，happy-path 完好；30 时序间歇、96 的 PRODUCT-RED 是 #58 split-home 已披露深层缺陷复现，均非三轮引入），且 CRIT-1 的 live 验证达成。

## 5. Low 残留与 follow-up（均不阻塞放行）

- **RESIDUAL-1**（F3，理论跨-leader 时钟回拨 fail-open，非回归）；**RESIDUAL-2**（F3 last_rehome_at 原子性无回归钉，建议补 source-pin）；**SR-8 注释文档漂移**（两处，纯文档）。
- 一/二轮遗留 follow-up（非本三轮引入、不阻塞）：30 的 HALT 窗口不可写性定案实验；96 的 split-home #58 `runtime-guard→gap`（task 26，deploy-tier 登记活）；锁reap `reaperCaughtUp` 加 `CommitIndex()>0` 硬化；M-2 门行为测试 + grace 接线测试；F3 作用域进一步 scoping；DOC-27（`/var/backups`）；M-3-F5；其余一轮 N-* 项。
- 结构性/深层缺陷（如实披露、非本工程范围）：grow-onto-recovered（42/51/22/82，R16）、#57/#65 结构性、#29/#34 开放缺陷 —— 这些是诚实登记的已知边界，不影响本次修复的放行。

## 6. 总评

三轮修复**完成度高、方向正确**。开发者对二轮唯一 HIGH 阻塞 SR-8 的修复（per-directive single-use token）是对根因的正确回应——安全性不再依赖 token 保密，经独立对抗性变异确认无残留伪造路径、无阻塞级 SR-9；F2/F3 两个 Medium 可用性问题也真闭合（F3 用固定 `op.CreatedAt` 而非滑动窗口，正确避开了二轮担忧的 fail-open，且原子性前提经核验成立）。**最有价值的是 CRIT-1 的 live deploy-tier 验证本轮达成**（71 Arm B 的 B-cmd/B-migrate/B-silent 全 PASS，在真隧道上证明了 agent 静默下 drain 的 rc 语义与数据面跟随）。硬闸全绿，deploy-tier happy-path 无回归。

**判定：Pass（Conditional）** —— 无阻塞项残留，可放行。建议合入时随手清理两处 Low（RESIDUAL-2 的原子-stamp 回归钉、SR-8 注释漂移），其余 follow-up 按 roadmap 消化。至此，从一轮的「发布级结构性缺陷 + 收官依据全面不成立」到三轮的「可放行」，CRIT-1 / P1 这条本工程头号目标已真正闭合并获 live 实证。

<!-- 四轮回复留待主进程（若需） -->
