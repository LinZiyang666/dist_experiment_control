# D2 内审报告（Stage C · 多专家对抗 + 主进程处置）

> **来源**：Stage C 审查 workflow（5 个 **Opus 4.8** 对抗审查员，~730k tok，run `wf_3b8d8140-fa5`）。审查员只读实现 + 提测试条目，**不改实现**。本文件是**主进程逐条处置**（采纳/驳回 + 修复）。
> **总评**：实现 renderer 本身经审查**逐列忠实**（无 UTC/raw 选错、无 NULL/'' 失配、无 ON CONFLICT/列偏差、scope 守得住、注入面闭合）。但审查抓到 **2 个真 bug** + 一批**等价 harness 漏洞**——里程碑"非 vacuous 等价"被高估。**全部采纳并修。**

---

## 真实现 bug（必修）

### B1 — `PlanAllocate` desired-port(P12) 不查占用 → Apply 撞约束 → fail-stop PANIC【BLOCKER，2 审查员独立证实】
- **位点**：`internal/port/plan.go` desiredPort!=0 分支 vs `internal/port/port.go:163-187`。
- **缺陷**：live `Allocate(desiredPort)` 靠 INSERT 撞 `idx_port_alloc_unique_active` → `translateInsertErr` → `ErrPortTaken`。op 路烤裸 INSERT（无 OR IGNORE），desired-port 已被占用时 entry 提交后在 Apply 撞 UNIQUE → applier 返错 → **D1 fail-stop panic**。重复 `--remote-port` 崩 FSM 而非返 typed error。
- **处置（采纳·已修）**：PlanAllocate 在 desiredPort 分支加 leader-DB 占用预检（`SELECT COUNT(*) WHERE port=? AND state='ALLOCATED'`），>0 即返 `ErrPortTaken`，**在 propose 前**。因 `Node.Propose` 持 `applyMu` 横跨 Plan-读+Apply，预检与 Apply 原子，并发 Propose 也不会双选。配并发 Propose -race 测试（见 T5）。

### B2 — JSON Command 信封对非 UTF-8 字节静默损坏【BLOCKER】
- **位点**：`internal/cluster/command.go` encode(=`json.Marshal`) + `sqlbake.go LitText`。
- **缺陷**：真实复制写路（`Node.Apply→cmd.encode→raft log→fsm.Apply→decode`）每 op 都跑。`json.Marshal` 把非法 UTF-8 字节换成 U+FFFD → 直接烤的文本列（argv/name）含非 UTF-8 时**线上播静默损坏**、副本发散；live mutator 绑参存原始字节。
- **处置（采纳·已修）**：`LitText` 加 `utf8.ValidString` 拒非 UTF-8（**fail-closed**，与拒 NUL 同姿态）——宁拒不腐。malformed（NUL/非 UTF-8）输入下 op 拒、live 接受的不对称是**有意硬化**（这些列本应 UTF-8/NUL-free；live 接受是潜在隐患）；登记于此。

---

## 等价 harness 漏洞（测试加固，主进程整合专家测试条目）

### T1 — 差分全固定 UTC → UTC/raw 选错不可见【BLOCKER vacuity】
- 审查员实证：往 `session.PlanCreate` 注入 `.UTC()`（必须绑 raw）后 `TestDifferential_MultiOp` 仍**绿**——LitTime 在 UTC `now` 下 `now==now.UTC()` 字节同，差分对时间轴 vacuous，恰好瞎在 plan 自标的 Critical 风险上。
- **处置（采纳·加测）**：加**非 UTC 时钟差分**（CST +0800）。session.\* 绑 raw(存 +0800)、port/proc/node/agentprov 绑 `now.UTC()`(存 UTC)——正确实现仍收敛，任一 Plan 错用/漏用 `.UTC()` 即发散。这是抓 UTC/raw 交换的唯一非 vacuous 测试。

### T2 — `PlanRevoke`/`PlanEvict` 零测试【MAJOR】
- **处置（采纳·加测）**：加 direct-vs-FSM 等价 —— PlanRevoke（ALLOCATED→REVOKED，含 not-found ErrNotFound 奇偶）、PlanEvict（两 DELETE 顺序 + FK 级联，seed session+node+proc+port+agent_provisioning 后比四表皆空 + 不存在 (sid,nid) 干净 0 行 no-op）。进 TestD2Matrix。

### T3 — Plan 侧 typed 业务错奇偶性仅测 ErrNameTaken【MAJOR】
- **处置（采纳·加测）**：表驱动「Plan 返回与 live mutator 同 typed error」——同 error 诱导输入过两路、断 `errors.Is`。覆盖 ErrAlreadyExists(session)、ErrNotFound/ErrDeleting(tombstone)、ErrInvalidPIN/ErrSessionMissing/ErrSessionDeleting/ErrAlreadyProvisioned(agentprov)、ErrNodeMissing(proc)、ErrPortTaken/ErrPortOutOfBand/ErrPortExhausted(port)。

### T4 — CHA lint：`StaticCallee` 漏函数值调用 + 缺负向对照 + 只载 2 包【MAJOR×2 + minor】
- 审查员实证：`var randVia = rand.Read; randVia(buf)` 的 `StaticCallee()==nil` → 报 0 命中（函数值/方法值/接口调用同漏）。且 plan §6 要求的负向对照（poison applier 经同一 map dispatch 必被抓）**缺失**。且 `packages.Load` 只载 cluster+port，proc/node/session/agentprov 不在 SSA 程序里。
- **处置（采纳·已修测试）**：① `bannedStaticCalls` 在 `StaticCallee()==nil` 时回退查 call-graph 边（`n.Out`）的 callee 包；② 加**负向对照**（test-only poison applier 经 reachableFuncs/bannedStaticCalls 同机制必被抓）；③ `packages.Load` 载全 5 mutator 包，使 reachability 图完整。

### T5 — 并发 Propose 测试缺失（applyMu/死锁的承重缓解）【MAJOR】
- **处置（采纳·加测）**：`-race` + 内建 NumGoroutine/fd 泄漏门的并发 Propose 测试——K goroutine 同 (sid,name)/同窄 band 并发 PlanAllocate→Apply，断恰一胜、败者得 ErrNameTaken/ErrPortExhausted/ErrPortTaken（**绝不 panic**）、wall-clock 超时即判死锁。证 applyMu + no-open-handle 前置 + B1 防 fail-stop。

### T6 — `resolveReconcileMarks` 仅对硬编码比、未对 live【MAJOR】
- 审查员实证：往分类器注入漂移（missed-exit -1→-99）只挂 `TestResolveReconcileMarks_G1Cases`，p8 e2e 与全门仍绿——硬编码 `want` 不防 hand-copy 漂移。
- **处置（采纳·加测）**：加差分——同 seeded DB + 同 NodeRegisterReq 跑 **live `(*Broker).reconcileOnRegister`** 与 `resolveReconcileMarks`，断 live 路实际施加到 processes 的 (pid→EXITED, exit_code) 集 == 分类器产的 marks。（用 broker 测试 harness；若 pubAudit 需 NATS 则用其测试 NATS。）

### T7 — 列级活性 lint 未实现（plan §6(c) 列的 D2 deliverable）【MAJOR】
- **处置（采纳·加测）**：对**渲染后的 op SQL**（各 Plan\* 烤出的 Statement.SQL）做列级断言——identity op（OpNodeRegister）的 SQL **绝不出现** status/last_heartbeat_at/proxy_ready；配非 vacuity 控制。**「禁 FSM 外 INSERT」保持 D9-staged**（ops-only 下 live 直连 mutator 仍在、grandfathered；plan §0 #5 既定，非 D2 欠账）——本报告确认此边界。

---

## minor / nit（采纳轻修或登记）

- **secret-leak 守卫用 `%+v` 非真 wire 字节**（minor）→ 改用 `json.Marshal(cmd)` 真编码断言。
- **负向对照注释说"wrong port"实改的是 local_port**（nit）→ 改注释/改成腐蚀真 public port。
- **PlanRegister ON CONFLICT 重注册路无测试**（minor）→ 加同身份 re-register 测试（更身份列、保 registered_at、不碰活性列）。
- **D2 op 测试未触内建 NumGoroutine/fd 泄漏门**（minor）→ T5 的并发测试带泄漏门即覆盖。
- **D0 baseline `math/rand/v2` 未纳入**（nit）→ 对齐 `bannedImports`。
- **D2-R5 旧 baseline 应非对称**（nit）→ 把 `TestDeterminismBannedImportBaseline` 改成只对**新增** banned import 报红（移除命中不报，因 Plan 侧搬迁合法）。

---

## 复验门（修完）

修完后复跑：`go build ./...` / `go test ./...` / `make lint` 0 / `TestD2Matrix` -race / `make e2e`（含 p8 reconcile + D2 matrix）全绿 → 交付外审。

## 修复实现状态（全部已落 + 验证）

| 项 | 实现 | 验证 |
|---|---|---|
| **B1** desired-port 占用预检→ErrPortTaken | `internal/port/plan.go` PlanAllocate | `TestPlanAllocate_DesiredPortCollision`（返 ErrPortTaken 非 panic）+ T5 并发版 |
| **B2** LitText 拒非 UTF-8 | `internal/cluster/sqlbake.go` `utf8.ValidString` | build/lint 绿；fail-closed |
| **T1** 非 UTC 时钟差分 | `TestDifferential_MultiOp_NonUTC` | **实证咬住**：注入 `.UTC()` 到 session.PlanCreate → FAIL；还原 → PASS |
| **T2** PlanRevoke/PlanEvict 等价 | `TestPortRevoke_Equivalence`/`TestNodeEvict_Equivalence` | PASS（含 not-found 奇偶 + 空 no-op）|
| **T3** Plan 错误奇偶 | `TestPlanError_Parity`（4 子例）| PASS |
| **T4** CHA 边检测动态调用 + 载全 5 包 + dispatch 非vacuity | `apply_reachability_test.go` reachableNodes/bannedCallsFromNode + 证 Apply 到 genericExecApplier.ApplyTx | PASS（3.8s）|
| **T5** 并发 Propose -race | `TestConcurrentPropose`（K=12，same-name + same-port）| PASS（-race，exactly-1 winner，无死锁/panic/泄漏）|
| **T6** live-vs-分类器差分 | `TestResolveReconcileMarks_VsLiveDifferential`（真 `reconcileOnRegister` vs 分类器+op）| PASS（processes 表逐行相等）|
| **T7** 列级活性断言 + ON CONFLICT 路 | `TestNodeRegister_IdentityOnly_NoLivenessColumns` | PASS（identity SQL 无活性列 + re-register 更身份列）|
| minors | secret-leak 改 `json.Marshal` 真 wire；baseline 非对称（D2-R5）；`math/rand/v2` 对齐；PlanRegister re-register（并入 T7）| PASS |

**复验门（全绿）**：`go build ./...` ✓ / `go test ./...` ✓ / `make lint` **0 issues** ✓ / **`TestD2Matrix` -race**（21.9s）✓ / **p8 reconcile e2e**（5.4s，live G.1 路未破）✓。

## 主进程结论

审查**高质量**：抓到 2 个会 panic/腐蚀的真 bug + 证明等价 gate 在时间轴/未覆盖 op/错误奇偶/CHA 动态调用/并发上 vacuous。**全部采纳**（无驳回），修复 = 2 处实现（B1/B2）+ 一批测试加固（T1–T7 + minors），均主进程亲手做（专家不改实现），**逐项验证 + 全门复绿**。→ **交付外审**。

> **D9-staged（非 D2 欠账，本报告确认边界）**：「禁 FSM 外 INSERT 到 Apply-owned 表」lint（ops-only 下 live 直连 mutator 仍在、grandfathered）+ 「leader-local 写不碰身份列」反向断言 + reconcileOnRegister↔分类器的真正共用重构 + broker 切到 FSM——均 D9 cutover 工作（plan §0 #5 / §5 / §19-D2 既定）。
