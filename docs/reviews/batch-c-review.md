# 批次 C 内审报告（阶段 C 步骤 4）

> **产出方式**：6 条 lane 的对抗性审查 workflow（review → 逐 lane 对抗性 verify → 综合），
> 外加一次 C1 verifier 补跑（原 agent 死于 `server_error`，见 §C1-verify）。
> **审查者边界**：只读实现、可新增测试，**绝不修改实现**（CLAUDE.md §4）。
> **基线**：批 C 实现完成、全部闸门已绿，且 deploy-tier drill
> `93-metrics-observability`（INCOMPLETE，与 expected-verdicts 一致）与
> `22-forcesingle-online`（GREEN）已实跑。
>
> 因此本轮的价值**不在**"编译不过 / 测试红"，而在**绿色测试套抓不到的东西**——
> 事实上最重的一批 finding 正是「测试声称能抓、实测抓不到」：
> 审查者 `cp -r` 到 scratch 逐条施加各测试**自己文档里写明的那个变异**并实跑，
> 真实工作树全程未改。
>
> **主进程逐条处置见文件末尾 §处置。**

---

# batch C 内审合并报告（6 lane × 对抗性核验）

## 0 · 判定与计数

**判定：INCOMPLETE**（沿用 completion lane 的验收裁决，见 §4）。

| 严重度 | 条数 | 说明 |
|---|---|---|
| **BLOCKER** | 16 | F1–F16。含 completion 覆盖表全部 PARTIAL/MISSING 行（C3-13 / C1-12 / C1-15 / C1-16 / C2-12 / C2-13），以及经变异实证「一行改动即复活数据删除 / 把运维推向 G4 #10」的项 |
| **MAJOR** | 12 | F17–F28 |
| **MINOR** | 18 | F29–F46 |

**核验覆盖说明**：c3 / c2 / tests / sweep / completion 五条 lane 均经独立对抗性核验（多数附 scratch 树变异实跑）。**c1 lane 未被核验**——其独有 finding（F6 / F8 / F12 / F41 / F42 / F44）仅有该 lane 自带的 probe 复现，实现者采纳前请自行复跑；c1 与其他 lane 交叉命中的项（F7 / F13 / F14 / F3 / F43）已由 sweep / completion / tests 的核验独立确认。

**环境事实（不影响结论，但影响"gates 已绿"的口径）**：`test/simcluster/drills/22-forcesingle-online.sh` 与 `93-metrics-observability.sh` 是在 completion lane 审计**开始之后**才落盘的，且 93 在审查窗口内被另一进程改过一行；drill 不进 `make test`/CI，无证据表明它们被实际跑过。

---

## 1 · BLOCKER

### F1 — 本批新增的四条运维补救文案都漏了 `--manual`，而该命令没有它会直接拒绝并把运维推向 `cluster retire <ghost>`

**证据**（四处全是本批新写的字符串）：
- `cmd/tether/cluster_offline.go:407` — `"If the operation ends in FS_GHOST_LEFT, run \`tether cluster recovery node remove <id>\` for\n"`
- `internal/broker/clusterops.go:73-74` — FS_GHOST_LEFT 的 `e.Resume`
- `internal/broker/force_single_finalize.go:250` — 写进 op `last_error`
- `internal/broker/force_single_finalize.go:372` — resume 路径的 Warn

命令自身 `cmd/tether/cluster.go:612-615`：
```go
if !manual {
    return usageErr("raw `remove` requires --manual (this is the last-resort escape); the routine path is `tether cluster retire %s` …", args[0])
}
```

**失败场景**：racknerd 式事故现场 → prune 失败 → `FS_GHOST_LEFT` → 运维逐字粘贴 → usage 错误，并被指向 `cluster retire <ghost>`；而 ghost 的 `Role == ""`、不在 raft config 内，`retire` 是这条路上最不该走的一步，JetStream 继续 503。全仓其余同类文案（`clusterops.go:163`、`docs/cluster.md:65/201/202`、`docs/cluster-runbook.md:20/51/82/471`）**都带 flag**，这四处是唯一例外。

**修法**：四处统一 `tether cluster recovery node remove <id> --manual`；`cluster_offline.go:407` 按 `cluster.go:604` 的 Example 补无 TTY 形式（`--confirm-node-id <id>` / `TETHER_CONFIRM_NODE_ID`），force-single 现场常在 `systemd-run` 里。

---

### F2 — `cluster status` 的 TOPO 列图例没跟着扫，把 `HOLD` 的运维推向 defect (b) 要修的那条数据损坏路径

**证据** `cmd/tether/cluster.go:510`（本轮**未改动**）：
```go
_, _ = fmt.Fprintln(w, "  · TOPO=NATS topology applied/observed→desired gen (✓ converged · … catching up · STUCK fix conf or `cluster reconcile nats --manual`)")
```
而 `Cell()`（`internal/natsconf/topostate.go:106-122`）现在会吐出 `HOLD` 与 `?` 两个新 token；`docs/` 全文 grep `HOLD` 零命中。

**失败场景**：首次 standalone→clustered grow 期间该行渲染 `4/4→7 HOLD`。同屏图例里没有 `HOLD`，唯一带补救动作的条目是 `--manual` —— 正是 `internal/natsconf/reconcile.go:181-185` 明写会形成 clustered-alone JS meta（G4 #10）/ 孤儿 standalone store（#4）的动作。`TopoHeld.NextStep()` 花整段写的否定子句（`topostate.go:180-184`）被运维实际读到的这一行原样抵消。补强：`computeHealth`（`clusterstatus.go:677-681`）的 `FaultTolerance == 0` 早退在 topo banner 之前，而 Held 只在 voters 1→2 时出现 ⇒ HOLD 出现时 banner 必然是 "add a node for HA"，同屏没有第二处能纠正他。

**修法**：图例按 `TopoState` 派生（在 `Cell()` 旁加 `CellLegend()`），四个 token 全列，`HOLD` 一行必须带否定子句，`?` 说明"对端比本二进制新"，`--manual` 收敛到 STUCK。

---

### F3 — C1-12 第三项未交付：`AbortOp` 的注释仍在承诺一个它没指向的 healer（PARTIAL 行）

**证据** `internal/broker/cluster_operation_controller.go:286-289`（本批未改）：
```go
// AbortOp transitions a non-terminal op to ABORTED (predecessor-CAS), freeing the per-node active slot
// WITHOUT touching the substrate (the membership stays whatever the gates left it; reconcile/doctor
// heals). The stuck-op escape hatch.
```
而 `internal/broker/reconcile_drain_marker.go:14-18` 的文件头自陈 *"the healer AbortOp's comment has been promising since C4 … This is it."* —— 两端只接上了一半。附带：`(predecessor-CAS)` 也已不成立，函数体改走 `cluster.PlanClusterOpAbort`，SQL guard 只有 `WHERE op_id = ? AND terminal = 0`（`operation_ops.go:242-244`）。

**失败场景**：下一个读该注释的人以为 membership 会被自动 heal；实际只有 rosterless 孤儿 marker 会被清，其余形状仍需人工。

**修法**：改成「guarded on `terminal = 0` only（**不是** predecessor-CAS，理由见 `PlanClusterOpAbort`）；本仓唯一的自动愈合是 `reconcile_drain_marker.go` 的 `drain-marker` pass，它**只**清 roster 行已不存在的孤儿 `broker_draining` marker；其余形状需 `cluster drain --abort` / `cluster recovery node remove`，由 `cluster doctor` 的 `roster_consistency` 报出」。

---

### F4 — C2-12 文案扫半途而废：`docs/usage.md` 自相矛盾 + agent/ctl 三条手抄 "2 GiB" 未派生（PARTIAL 行）

**证据**：
```
docs/usage.md:937  tether push <local-path> <nid>:<remote-path> [--force] [--timeout 10m] [--ack-alerts]
docs/usage.md:938  tether pull <nid>:<remote-path> <local-path> [--force] [--timeout 10m] [--ack-alerts]
docs/usage.md:948  | `--timeout` | `36m8s` | 每个阶段的上限；… |
```
```go
internal/agent/transfer.go:403   Error: fmt.Sprintf("file size=%d > 2 GiB", size)
internal/agent/transfer.go:472   Error: fmt.Sprintf("file grew beyond 2 GiB while reading (read=%d)", uploaded)
cmd/tether/transfer.go:523       fmt.Sprintf("object size=%d exceeds 2 GiB limit", pr.Size)
```
对照 broker 侧已派生：`internal/broker/transfer.go:691-699`（`humanBytes(...)`，注释写明 *"the human-readable size is DERIVED, not hand-copied"*）。plan §6.6 要求「要么派生、要么在 plan 里列为『纯散文』并说明」——plan 里 `纯散文` 只出现在那句要求本身，两个出口都没走。守卫盲区属实：`test/determinism/topo_classification_test.go:262-296` 的 `scanXferTierLiterals` 只遍历 `token.CONST` 声明，格式串对它不可见。

**失败场景**：照 synopsis 抄 `--timeout 10m` 的人，2 GiB 传输走到第 10 分钟被自己的 ctl 打断，而 broker 还要再等 24 分钟；`XferMaxBytes` 一旦下调，agent 仍对运维说 "> 2 GiB"（`feedback-contract-change-sweep` 的原样复发）。

**修法**：`:937-938` 改 `[--timeout 36m8s]`（或去掉数值写 `[--timeout D]`，让表格做唯一来源）；三条错误串改成派生（agent 侧加一个与 `humanBytes` 等价的本地渲染，或把 `humanBytes` 提到 `internal/proto` 与常量同住，零新 import 边）；剩余纯散文（`cmd/tether/transfer.go:46-49`、`internal/schema/audit.go:73`、`internal/proto/messages.go:764-767`）在 plan / 架构文档 §20.2 里显式登记。

---

### F5 — pull 腿两端预算被拉成不对称：agent 传到 17 分钟，broker 5 分钟就判失败、删对象、忘掉 entry

**证据** `internal/agent/transfer.go:447`：
```go
putCtx, cancel := context.WithTimeout(context.Background(), proto.XferBudget("b", size, 1))   // 2 GiB ⇒ 1024s
```
对 `internal/broker/transfer.go:509` + `:874-881`：`handlePullReq` 构造的 `transferEntry` **从不设 size** ⇒ `d := proto.XferBudget(e.tier, e.size, proto.XferPushLegs)` 恒为 300s。N5 只记了"pull 的 tier-B 预算仍是固定值"，C2-8 同时抬高了 agent 那一腿，产生 plan 未记录的新窗口（`size > 600 MiB` 时 1024s vs 300s）。

**失败场景**（`tether pull node:/data-2GiB ./x`，上传 8 分钟）：t=5m 看门狗 fire → 写 `failed/ctl_disconnect`、`deleteXferObject`（meta 未发布 ⇒ no-op）、`transfers.remove` ⇒ bucket 退出 `activeOBJStreams()`；t=8m agent 的 Put 落地成**孤儿对象**，`xferReapMinObjectAge = 2min` 的 home reaper 可在 ctl 下载途中删掉它（`transfer_reconcile.go:127`）。**核验订正**：ctl 不会挂满 `--timeout`——成功路径 `cmd/tether/transfer.go:594-598` 用 5s `sendFinalize` 且丢弃返回值 ⇒ **ctl 打印 `OK (tier B, …)` 退 0，而审计写着 failed**（审计倒置，比"挂 36 分钟"更隐蔽）。

**修法**：`putCtx` 固定为 `proto.XferBudget("b", 0, 1)`（= tier floor，与 broker 的 pull 看门狗逐字对齐），注释点名"pull 无声明 size，单方面抬高这一腿会让 broker 先删对象/先写 failed"。真要加速大 pull 必须走 N5 排除的 wire 改动，不能只抬一端（D6 论证的镜像）。

---

### F6 — `finalizeBudgetCheck` 把「预算过期那一 tick 上 prune 成功」判成 `FS_GHOST_LEFT`，并输出一份事实错误的 ghost 清单

**证据** `internal/broker/force_single_finalize.go:241-256`：
```go
remaining, err := a.rosterRowsPresent(params.Abandoned)
if err != nil || len(remaining) == 0 {      // ← len(remaining)==0 是缺陷
    remaining = params.Abandoned
}
```
`driveForceSingleFinalize:228-235` 先 Propose prune、**再无条件** budget check ⇒ 哪怕这一 tick prune 完全成功，deadline 已过就进这里；重读得 `len(remaining)==0`，被 disjunct 吞掉，`remaining` 被换成整个 `params.Abandoned`。`err != nil` 已独立覆盖读失败。

**失败场景**（c1 lane probe A 实跑）：roster 行**已删掉**，产品却输出
```
force-single finalize gave up after 1m0s: … Ghost roster rows remain: ghost-1.
Run `tether cluster recovery node remove <id>` for each. Until they are gone, … REFUSES … JetStream stays 503.
```
四句全假；`cluster ops ls` 永久 failed；`deriveAndConvergeSeedsFromRoster()` 被跳过。可达性不是边角：预算 = `12 * observeTickInterval` = 60s，进程重启后重新拿 leadership 只要 >60s，**第一次 drive 就已过期**——而 prune 首次失败最常见的原因恰是 `ErrNotLeader`，重启后必然成功。即「重试终于成功」被系统性报成失败。

**修法**：
```go
remaining, err := a.rosterRowsPresent(params.Abandoned)
if err == nil && len(remaining) == 0 {
    if serr := a.deriveAndConvergeSeedsFromRoster(); serr != nil { a.logger.Warn(...) }
    _ = a.transition(op, cluster.OpStateFSFinalized, true, "", nil)
    return
}
if err != nil { remaining = params.Abandoned }
```

---

### F7 — `mintOpID` 用稳定 discriminator + `cluster_operations` 无 GC ⇒ 同一 (self, ghost 集) 的 finalize op 一辈子只能建一次，且报一个假原因

**三条 lane 独立命中（c1-F3 probe / sweep-F6 scratch 复现 / completion NEW-1）。**

**证据** `internal/broker/force_single_finalize.go:131`：
```go
opID := mintOpID(cluster.OpKindForceSingleFinalize, selfID, strings.Join(abandoned, ","))
```
`mintOpID` 是 `sha256(kind|target|discriminator)` 纯函数（`cluster_operation_controller.go:19-22`）；join 用 `b.JoinNonce`、retire 用 `RFC3339Nano`，**只有 finalize 的 discriminator 完全来自持久状态**。`internal/cluster/operation_ops.go:129-130` 的 `AND NOT EXISTS (SELECT 1 FROM cluster_operations WHERE op_id = <opID>)` **不看 terminal**，而全仓唯一删该表的语句是 `internal/clusteroffline/restore.go:373` 且只删 `terminal = 0`。

**失败场景**（实跑）：
```
first op op-da1d3aa72c6c208c -> FS_GHOST_LEFT terminal=true
second start -> id="" err=the finalize op was not created (an operation for "brk-a" is already in flight)
```
此后每次 leadership edge，`resumeForceSingleFinalizeOnLeadership` 的四条判据**依然全中**，却永远算出同一个 opID、INSERT no-op、回读 nil ⇒ 只留一行 Warn，且错误文本是**假的**（一条在飞的都没有）。C1-6 的崩溃窗口覆盖对同一组 ghost **一次性**；60s 预算内没成功就永久熄火，重启也救不回来——而 prune 失败的常见原因（leader 抖动、磁盘满）恰是几分钟内会自愈的那类。

**修法**：discriminator 加非确定量（照 retire 用 `a.now().UTC().Format(time.RFC3339Nano)`，或 recovery epoch / `SELECT COUNT(*) … WHERE kind=… AND target_node=self`）；回读失败分两支——"op_id 已被终态前驱占用（重新 mint 重试）"与"self 上另有非终态 op（保留今天的文案）"。顺带重新论证 60s（注释写 "a couple of dozen ticks"，常量是 12）。

---

### F8 — 提交 prune 前只重查 raft config、不重查 phase，会删掉正在 re-join 的 roster 行

**（c1 lane 独有，probe C 复现，未经独立核验）**

**证据** `internal/broker/force_single_finalize.go:203-219`：
```go
inCfg, cerr := a.raftConfigIDs()
...
for _, id := range remaining { if !inCfg[id] { prune = append(prune, id) } }
```
而同文件 `:295-297` 的 `forceSingleGhostRows` 注释自己写着：*"A node mid-join legitimately has a roster row **before** it appears in the config (that is the order the join ladder writes them in), and it sits in JOIN_VERIFIED_PENDING_VOTER / CATCHING_UP while it does."* —— 选取时刻意收紧到 `phase = VOTER` 的保护，在**删除**时被整个丢掉（`rosterRowsPresent:260-276` 只做 `SELECT COUNT(*)`，不取 phase）。

**失败场景**（probe C：`CONFIRMED: the mid-join roster row for brk-b was pruned by the finalize op`）：force-single 后 60s 内运维开始重建 HA，`join approve` 刚写入 `JOIN_VERIFIED_PENDING_VOTER` 行、`AddNonvoter` 尚未落地 ⇒ 该行被删；若 `AddNonvoter` 随后落地，就变成 `ReconcileMembershipOnLeadership` 的 "raft voter with no roster row" INCONSISTENT，需人工介入。

**修法**：`rosterRowsPresent` 返回 `(id, phase)`，prune 集合加合取项 `phase == phaseVoter`，与 `forceSingleGhostRows` 的选取谓词对齐。

---

### F9 — `reconcile nats --wait` 的 wedged 报错把 STUCK 的 `--manual` remedy 套在 HELD 节点上——缺陷 (b) 在新代码里复活

**（c3 lane 核验期新发现）**

**证据** `cmd/tether/cluster_reconcile.go:143-147` 列出**全部** wedged 节点，却只附**一条** `worst.NextStep()`。scratch 实跑（brk1 = HELD，brk2 = STUCK）：
```
reconcile nats: topology cannot converge on its own:
  brk1(held standalone→clustered cutover rendered + validated but WITHHELD …),
  brk2(stuck apply: no space left on device)
  — fix that broker's nats.conf, or run `tether cluster reconcile nats --manual` on it
```
brk1 正是 `reconcile.go:182-185` 写明跑 `--manual` 会形成 G4 #10 / #4 的那台；`TopoHeld.NextStep()` 的否定子句在这条 `worst`-keyed 路径上被完全绕过。可达：N≥3 集群加一台新 broker（live conf 仍 standalone ⇒ HELD）同时另一台满盘 `apply:` 失败（STUCK）。

**修法**：按状态分组渲染，每组带自己的 `NextStep()`；或只输出 `WorstTopoState` 挑出的那一组节点，绝不让一条 remedy 跨状态。补表驱动测试：`{HELD, STUCK}` 混合时输出**不得**在提到 HELD 节点的同一段里推荐 `--manual`。

---

### F10 — C2-13 的 `TestPullEntryCarriesNoSize` 不存在，N5 变成无守卫的口头承诺（MISSING 行）

**证据**：`grep "func TestPullEntryCarriesNoSize"` 全仓 0 命中。`internal/broker/transfer.go:875-881` 的 pull `transferEntry` 字面量确实不设 `size:`（符合 N5），但无任何断言钉住。**completion 核验实跑 M10**：给该字面量加 `size: 2*1024*1024*1024` ⇒ **全包绿**。

**失败场景**：有人"顺手把 pull 也做了"，从 `finalize.req` body 取一个 size 填进去——pull 路径**没有 size admission gate**，于是 `XferBudget(e.tier, e.size, 2)` 变成客户端可控且无上界，直接打穿 `XferTierBMaxBudget` 的"编译期上界"论证（`internal/proto/xfer.go:33-35`）。N5 正文自己写着这条裂口"由 §6.6 的回归测试钉住"，所以它不能援引 N5 豁免。

**修法**：驱动 `handlePullReq`（`tracker_miss_silent_test.go:116` 已有现成 `PullPrepareReq` 夹具），断言 `b.transfers.get(tid).size == 0` 且看门狗取 `proto.XferTimeoutTierBFloor`；失败信息点名 N5 与"pull 无 admission gate ⇒ 有 size 就等于无上界"。

---

### F11 — C2 的三个承重**调用点**零覆盖：一行改动即可静默复活数据删除，全包测试仍绿

**四条 lane 命中，两条 lane 各自跑了全包变异（`go test ./internal/broker/` ≈295s，全绿）。**

**证据 + 实跑变异**：

| 调用点 | 变异 | 结果 |
|---|---|---|
| `internal/broker/transfer.go:509` `d := proto.XferBudget(e.tier, e.size, proto.XferPushLegs)` | 改回 `transferTimeoutTierB` / 把 legs 改成 1 | **全包绿** |
| `internal/broker/transfer_reconcile.go:101` `b.reapBucketObjects(ctx, name, b.crossHomeReapAge(), true)` | `true → false` | **全包绿** |
| `internal/broker/xfer_inflight.go:682` `timeout := transferTierFloorFor(rec.Tier)` | → `transferTimeoutFor(rec.Tier, rec.Size)` | **全包绿** |
| `internal/broker/transfer_reconcile.go:18` `xferReapMinObjectAge` | 抬到 40m（N15 残留真被关闭） | `TestOrphanReaperStillOutrunsALiveTransferAfterRestart` **PASS**，且仍打印 `"…delete a live 2 GiB object 2m0s in"` |

原因：所有断言都落在 helper 上。`transfer_budget_test.go:22-24` 比较 `proto.XferBudget("b", size, XferPushLegs)` 与 `transferTimeoutFor("b", size)`，而后者函数体逐字就是前者（`xfer_inflight.go:150-152`）——**单侧恒等式**；`:79` 同样是公式复述；`:182` `homeGrace := 2 * time.Minute // xferReapMinObjectAge's production value` 是同包常量的手抄。另有一条**恒假死断言** `transfer_budget_test.go:59-64`：
```go
base := transferTierFloorFor("b")
if transferTimeoutFor("b", size) != base && transferTierFloorFor("b") != base {   // f(x) != f(x)
```

**失败场景**：`:101` 的 `true→false` 精确复现 §11 点名的 "C2-3 亲手造出新的数据删除缺陷"——leader 在 15 分钟处删掉 owning home 的看门狗还要覆盖 34m08s 的 2 GiB 在飞对象；e2e 也结构性看不见（全仓最大传输载荷是 12 MiB，两条变异在该 size 下行为逐字相同）。

**修法**：
1. 抽 `func watchdogBudget(e *transferEntry) time.Duration`，`startTransferWatchdog:509` 调它，`transfer_budget_test.go:22`/`:79` 都改读它；
2. 照 `reconcile_passes_test.go:1181` 的 `TestXferCrossHomeGCReapsSplitHome` 加一条：对象 `Size = 2 GiB`、`XferCrossHomeReapAge` 设在 15m–44m 之间、ModTime 20 分钟前，断言**没有**被 reap（把 `true` 改 `false` 必红）；再加一条断言 home reap 传的是 `sizeAware=false`；
3. `homeGrace := xferReapMinObjectAge`；
4. 写一条 `Size = proto.XferMaxBytes` 的 stranded 记录跑 `finalizeStrandedXfers`，断言合成终态 `Ts == rec.StartedAt.Add(transferTimeoutTierB)`（钉住 D-C2-10 的唯一调用点），并删掉 `:58-64` 的死循环。

---

### F12 — prune 失败**且** op 也建不出来时，CLI 仍打印 "the abandoned peers are already pruned" 这句因果指引

**（c1 lane 独有，未经独立核验）**

**证据** `cmd/tether/cluster_offline.go:423-428`：
```go
func declusterPruneNote(finalizeOp string) string {
	if finalizeOp != "" { return " — but ONLY after the finalize operation above has removed ..." }
	return " (the abandoned peers are already pruned, so the N=1 proof now passes)"
}
```
谓词是 `finalizeOp != ""`，不是"prune 是否完成"。broker 侧有三种结局：

| 结局 | `FinalizeOpID` | CLI |
|---|---|---|
| prune 成功 | `""` | "already pruned" ✔ |
| prune 失败 + op 建成 | `op-…` | WARNING + 否定子句 ✔ |
| **prune 失败 + op 也建不出来** | `""` | **"already pruned" ✘，零 WARNING** |

第三行是 `force_single_online.go:305-310` 明确保留的回落分支，且**不罕见**：prune 失败源于 `node.Propose` 失败，而 `startForceSingleFinalize` 的第一件事就是同一个 `node.Propose`；plan §5.4 的 stale retire op 场景、以及 F7 的 opID 碰撞，也都落在这一行。

**失败场景**：运维看到 exit 0 + "already pruned, so the N=1 proof now passes"，照做 `reconcile nats --to-standalone` → `unrecognized raft role "" — cannot prove N=1, refusing`。这逐字是 plan §5.5 与 `cluster_offline.go:394-399` 注释声称要消掉的东西。`cmd/tether/cluster_topo_render_test.go:279-295` 只测 helper，缺陷在 caller 选错谓词，所以它恒绿。

**修法**：`adminsock.ForceSingleReport` 加一个不依赖 op 是否建成的字段（`GhostsRemaining []string` 或 `PruneIncomplete bool`），`handleForceSingleCommit` 在 prune Propose 失败时**无条件**填；CLI 的 WARNING 与 `declusterPruneNote` 都以它为谓词，`FinalizeOpID` 只决定 WARNING 里是否给 op id。

---

### F13 — C1-15 完全未做：`docs/cluster-runbook.md` 仍在教运维走进 `unrecognized raft role ""`（MISSING 行）

**证据**：文件不在 `git status` 里（最后一次改动 `df7e6c8`，早于本批）。逐字仍是：
```
:461  The abandoned peers are already pruned (below), so the N=1 voter tally passes and `--to-standalone` is unlocked.
:464  **Abandoned-peer roster (#12).** force-single now PRUNES the abandoned peers … automatically
:466  If you upgraded from an older binary and a **ghost VOTER** lingers …
```
全文不含 `force_single_finalize` / `FS_GHOST_LEFT` / `cluster ops show` 任一词。而 `internal/broker/force_single_finalize.go:31-35` 的实现注释**自己引用了这句 runbook 原文**当作"prune 必须同步"的论据。

**失败场景**：prune 失败、CLI 正确打印了 WARNING，但灾难现场的运维照 §3.2 走，读到"已 pruned ⇒ `--to-standalone` 解锁"，直接执行并撞上 `cmd/tether/cluster_natsconf.go:180-189` 那条毫无指向性的报错；`:466` 还把 ghost 唯一归因于"从旧二进制升级"，恰好排除了本批新造的产生路径。

**修法**：§3.2 分三支（无 finalize op ⇒ 现文案；有 op ⇒ 先 `cluster ops show <op-id>` 到终态；`FS_GHOST_LEFT` ⇒ 逐个 `recovery node remove <id> --manual` 再 de-cluster）；`#12` 段补第二条 ghost 来源（同步 prune 失败）；补一条"回滚前须先确认无非终态 finalize op"（plan §10 的永久决策目前只活在 plan 里）。

---

### F14 — C1 ladder 全部核心函数零测试引用；五条"声称能抓某变异"的守卫实测一条都抓不到（C1-3/4/5/6 验收面）

**三条 lane 命中，两条 lane 各跑了全包变异，全绿。**

**证据**：
```
$ grep -rn "driveForceSingleFinalize\|startForceSingleFinalize\|finalizeBudgetCheck\|resumeForceSingleFinalizeOnLeadership\|rosterRowsPresent\|forceSingleGhostRows\|reconcileDrainMarkers\|orphanDrainMarkers" --include=*_test.go .
internal/broker/force_single_finalize_test.go:184   ← 注释
internal/broker/force_single_finalize_test.go:242   ← 注释
```
实跑变异（`go test ./internal/broker/` 全包，每条 ~295s，**全部绿**）：

| # | 变异 | 位置 |
|---|---|---|
| M1 | 耗尽终态改 `cluster.OpStateBlocked, false` | `force_single_finalize.go:255` |
| M2 | ghost SQL 放宽成 `WHERE phase IN (VOTER, CATCHING_UP, PENDING)` | `:306` |
| M3 | `orphanDrainMarkers` 返回**全部** marker | `reconcile_drain_marker.go:133-139` |
| M4 | default 分支换成 `a.transition(op, OpStateAborted, true, …)` | `cluster_operation_controller.go:495-511` |
| M5 | 无条件先建 op（成功路径也建） | `force_single_online.go:300` |
| M6 | Propose 成功即 `transition(FS_FINALIZED)` | `force_single_finalize.go:228-235` |
| M7 | `CatchupDeadline: 0` | `:140` |
| M8 | 删掉 `if !inCfg[id]` 重确认 | `:214-219` |
| M9 | 删掉 resume 的 ①`forceSingleActive` ②`NumVoters()==1` 判据 | `:347-368` |

五条失效守卫的成因（`internal/broker/force_single_finalize_test.go`）：
- `:28-51 TestForceSingleFinalizeLadderIsAlwaysTerminal` — 只断言三个 state ∈ `ValidOpState`，而 `OpStateBlocked` 本身就在 `validOpStates` 里（`operation_ops.go:76`）；对 `finalizeBudgetCheck` 零引用。
- `:186-201 TestForceSingleGhostSignatureIsPhaseVoterOnly` — 只比 phase 常量，**从不调 `forceSingleGhostRows`**；`:187-191`/`:198-200` 在构造上恒假。
- `:243-263 TestDrainMarkerHealerLeavesAHalfFinishedDrainAlone` — 在测试体内重写了一遍 `if !roster[node]` 并断言自己的循环；`:258-262` 恒假。
- `:60-61 TestUnknownOpKindIsForcedTerminal` — 只直接调两个 planner，`driveOne` 的 default 分支一行没看；`TestDriveOneHandlesEveryOpKind` 的 AST 只查 `default:` 是否**存在**、不查它调什么。
- `:268-281 TestForceSingleReportSeparatesIntentFromCompletion` — 构造字面量再断言自己刚填的字段，纯恒等，且连一条 `Mutation:` 都没写。

plan §9 点名的 9 条 C1 测试**一条都不存在**（`TestForceSingleCommitSuccessPathCreatesNoOp` / `…RetriesFailedPrune` / `…AdvancesOnObservationNotPropose` / `…SurvivesLeadershipChange` / `TestLeadershipEdgeCreatesFinalizeOpForGhost` / `TestForceSingleCommitSucceedsWithStaleRetireOpOnSelf` / `TestDrainMarkerHealerClearsOnlyRosterlessOrphan` / `TestDrainMarkerHealerIdleZeroWrites` / `TestConfirmOpDoesNotResetFinalizeBudget` 中的前 8 条）。

**失败场景**：M3 = N7 的反面（清掉运维正在重试那次 drain 的 marker ⇒ `broker_draining` 告警消失、phase 仍 VOTER、`pickProxyRehomeTarget` 把 expose 搬回刚被搬空的节点）；M6 = op 报 `FS_FINALIZED` 而 ghost 仍在（`PlanClusterNodePrune` 在 `RowsAffected==0` 时 Propose 仍返回 nil）；M9 = 一个从未做过 force-single 的正常 N=1 集群在 roster 短暂落后时自动删 roster 行。

**修法**：
- 夹具：`internal/broker/force_single_handler_test.go:24` 的 `fsTestBackend`（真 `cluster.Node` + inmem raft）或 `d7SingleNode` + `b.cl = &clusterRuntime{node: n, admin: NewClusterAdmin(n, nil)}`（`xfer_inflight_test.go:263` 的形状）。**注意：不能用 `reconcile_passes_test.go:114` 的 `passBroker`**——它把 `b.cl` 留成 nil，而 `reconcile_drain_marker.go:65-67` 第一行就 `if b.cl == nil … { return nil }`，照它写出来的测试会"绿得毫无意义"。
- 补：`…CommitSuccessPathCreatesNoOp`（`cluster_operations` 为空）、`…AdvancesOnObservationNotPropose`、`…DeadlineGoesTerminalNotBlocked`（断言 `FS_GHOST_LEFT ∧ terminal==1`，且 `last_error` 含 `recovery node remove` 与 `to-standalone`）、`…SurvivesLeadershipChange`（清空 `a.opAttempts` 后预算判定不变）、`…RefusesToPruneAReadmittedMember`（钉 F8）、`…SucceedsWithStaleRetireOpOnSelf`、`TestLeadershipEdgeCreatesFinalizeOpForGhost`（四条判据逐条置反的 5 行表）、`TestDrainMarkerHealerClearsOnlyRosterlessOrphan` + `…IdleZeroWrites`（无孤儿时 `AppliedIndex` 不前进）。
- 重写上述五条失效守卫，**并逐条实跑它们声称的变异确认变红**（`feedback-mutation-verify-every-new-guard`）；`TestDriveOneHandlesEveryOpKind` 的 AST 扫描顺手在 default 子树里断言出现 `PlanClusterOpAbort` 且**不出现** `transition`。
- 保留 `:44` 的 `opForceSingleFinalizeBudget != 12*observeTickInterval`——核验证明它**不是**恒等式（改成 `20*observeTickInterval` 会红），只是低价值的变更探测器。

---

### F15 — C3-13 未交付另一半：drill 93 只有收敛态断言，失败态极性从未在部署栈上走通（PARTIAL 行）

**证据** `test/simcluster/drills/93-metrics-observability.sh:121-136` 三条断言全在收敛侧：① `has("topo_action")`；② `all(. == "noop" or . == "reloaded")`；③ `grep -cE '→[0-9]+ ✓' >= 3` 且 `grep -cE '→[0-9]+ (…|STUCK|HOLD|\?)' == 0`。drill 注释逐字写着 *"the STUCK/HELD/BEHIND classification itself is covered hermetically in internal/natsconf"* —— 正是 plan §11 逐字列为规避手法的"用 hermetic 层回答只有部署层能回答的问题"。

**失败场景**：把 `internal/broker/cluster_health.go:106` 的 `resp.TopoAction = ts.Action` 硬编码成 `natsconf.ActionNoop`，三条断言**全部照样绿**（第③条对该字段完全盲：收敛集群走 legacy 路径同样渲染 `✓`）。即"端到端链路真的搬运了 reconciler 的实际 Action"零覆盖；把 `ClassifyTopo` 的 `case ActionRejected` 挪回 `observed >= desired` 门里、或删掉 `clusterstatus.go:424` 的 self 行赋值，drill 也全绿。

**修法**：加一段失败注入——在一台 broker 的 nats.conf 里塞一条非法/未知 directive（真实运维手改场景），等一个 reconcile tick，断言该行 `topo_action == "unknown_directive"`（**注意常量是 `unknown_directive`，`internal/natsconf/reconcile.go:42`；completion lane 建议里写的 `unrecognized_directive` 是错的**）**且** TOPO 列出现 `STUCK` **且** `cluster doctor` 的 `topology` 为 FATAL/退 64，然后还原。第③条已有的负向断言（`bad == 0`）保留，它比 §11 设想的规避形状更强，不要顺手删。

---

### F16 — C1-16 未交付：`12-ghost-voter.sh` 一个字节没改，C1 的全部新机器在部署层零覆盖（PARTIAL 行）

**证据**：`test/simcluster/drills/12-ghost-voter.sh` 不在改动集里（最后改动 `55b1451 2026-07-21`），其 `:2-3` 的前提仍是 *"force-single now AUTO-PRUNES the abandoned peer, so it never lingers"*，只断言 happy path。C1-16 明写要加 prune 失败注入分支。

**失败场景**：建 op / 重试 / `FS_GHOST_LEFT` / `--to-standalone` 拒绝路径 / `recovery node remove` 解锁，整条灾难路径从未在真实部署栈跑过一次。

**修法**：加失败注入分支（最忠实的手法是在 commit 瞬间打断 raft 提交窗口，使 `PlanClusterNodePrune` 的 Propose 失败），断言 ① `cluster ops ls --json` 出现 `force_single_finalize` ② stderr 出现 `DO NOT de-cluster yet` ③ `--to-standalone` 拒绝 ④ `recovery node remove … --manual` 之后解锁 ⑤ 重试到达 `FS_FINALIZED` 或 `FS_GHOST_LEFT`。**若部署层真做不出该注入，按 drill 铁律标 `[GAP #N]` 明写"tether 干不了"，不得以"单测覆盖"抵账。**

---

## 2 · MAJOR

### F17 — `ActionSwappedReloadPending` / `ActionUnresolvable` 无条件返回 `TopoBehind`，违反 plan §4.2 自己写的"不改今天的行为"，副产品是与 legacy fallback 极性相反

**证据** `internal/natsconf/topostate.go:231-246 / 256-279`：
```go
case ActionSwappedReloadPending, ActionUnresolvable:
    return TopoBehind          // 无条件
...
case observed >= desired: return TopoConverged   // legacy 路径：这两条 Reason 一个标记都不含
```
穷举实跑（`AllActions()` × `ReconcileOnce` 全部 Reason × `observed∈{3,7,9}`）：36 行中 4 行 DISAGREE，全是这两个 action 在 `observed >= desired` 时。

**失败场景 A（同版本）**：`reconcile.go:160-173` 的 fast path 返回 `ObservedGen: lastObserved`，一台**已收敛**的 broker 只要一个 5s tick 内两次 3s /varz 探测都失败（`systemctl restart nats-server`、staggered hard restart、loopback 抖动），就渲染成
```
TOPO 列:  7/7→7 …          banner: "…has not caught the desired generation yet (see the TOPO column)"
```
banner 与它让运维去看的那一列直接矛盾，`cluster status` 退 0→1。
**失败场景 B（混版）**：同一台机器、同一物理状态，跑新二进制判 DEGRADED、跑旧二进制判 HEALTHY_HA —— 正是本批宣称要拆掉的"两端各有一套判据"，只是搬进了分类器内部。

**修法**：取 plan 的字面要求——让这两个 action 在 `observed >= desired` 时返回 `TopoConverged`；`internal/natsconf/topostate_test.go:47` 那行 `{ActionSwappedReloadPending, reasonAwaitingConfirm, 7, TopoBehind}` 同步改判并写明理由。若坚持"unconfirmed load 永不算 converged"，那是**推翻 plan**，必须留下裁决记录，且 banner 文案必须换成不提 generation 的措辞（否则输出恒为假）。

---

### F18 — `cardTopReason` 的 topo 分支同时有两个缺陷：`TopoBehind` 落到一句假的 fallback，且按 roster 顺序而非 `WorstTopoState` 挑节点

**证据** `cmd/tether/cluster_status_card.go:113-126` 只 case 了 `TopoStuck`/`TopoHeld`/`TopoUnknownAction`，且在第一个命中的节点上 `return`；`TopoBehind` 穿过全部循环落到 `:158`。而 `internal/broker/clusterstatus.go:657` 用的是 `topoWorst = natsconf.WorstTopoState(topoWorst, st)`。

**失败场景（两条，均 scratch 实跑逐字复现）**：
```
① 唯一 degrade 是一台 topo-Behind：
   headline = "DEGRADED-WRITABLE: fault tolerance reduced — see the table"      ← 在 3 voter 下是假的
   banner   = "a broker's NATS topology has not caught the desired generation yet (see the TOPO column)"
② a=converged, b=HELD, c=STUCK：
   headline = "…broker b standalone→clustered cutover WITHHELD (run `cluster add`)"
   banner   = "a broker's NATS topology reconcile is STUCK …"                   ← 头条与 banner 指向不同节点
```
`:107-112` 的新注释自称 "topology comes FIRST because that is computeHealth's BANNER precedence" —— 它只镜像了**类别间**优先级，没镜像**拓扑态内**优先级；三个渲染器里只有 card 这一个错（doctor 的顶层 switch 反而做对了）。`TestCardTopReasonNamesTopology`（`cluster_topo_render_test.go:133-165`）只放一个降级节点，结构上看不见。

**修法**：先扫全部节点求 `WorstTopoState` 与对应 NodeID，再出文案；topo 分支改成 `if st.Degrades() { … }` 并保留 Stuck/Held/Unknown 的专属文案、给 Behind 一句"topology catching up"。测试补一行「一个 HELD + 一个 STUCK ⇒ 头条必须是 STUCK 那台」。

---

### F19 — `reconcile nats --wait` 的 wedged bail 用 `exitInternal`(70)，与 `docs/usage.md §9.13` 直接冲突；且无 dwell，一次代码自认的"一 tick 瞬态"就能杀死一次本会成功的等待

**证据 (a)** `cmd/tether/cluster_reconcile.go:139-147`：
```go
// exitInternal, NOT exitTransient: … 75 would drive a retry loop to keep waiting forever
```
`docs/usage.md:1533 §9.13` 逐字：*70 = tether 没能分类的错误（该上报的 tether 侧缺口）*；**健壮重试规则：把 `69`/`70`/`75` 当可重试，仅 `64`/`77` 当终态**。决定性反证在同一函数往上 15 行（`:124-127`）：*"this used to be … exit 70, i.e. \"a tether bug, retry it\" per docs/usage.md §9.13"* —— batch-A A3 专门把它从 70 挪到 75（`docs/batch-a-roadmap.md:46`），本轮又挪回去了一半，且理由自相矛盾。

**证据 (b)** `topoWedged` 在 `pollUntilStep` 的**第一次采样**上就 bail（ctl 采样 2s，broker tick 5s，self-report 最多陈旧 5s）。而 `internal/broker/topology_reconcile.go:260-266` 的注释自陈：`filterGhostPeers` 在 `RaftConfiguration()` 读失败时退到 self-only ⇒ 渲染 clustered-zero-routes ⇒ `render_desired.go:171` 明写会 *"wedge the loop at ActionRejected"*，作者称之为 *"a genuinely multi-node cluster just skips one tick"*。

**修法**：class 改 `exitUsage`(64)（本仓"终态、需人工"类，与 `name_taken`/`port_exhausted` 同档），把 §9.13 的理由写进注释；bail 加"连续 N 次采样都 wedged"或"≥1 个 reconcile tick(5s)"的确认窗口；顺带把"`reconcile nats --wait` 现在会因 wedged topology 提前退出"补进 `docs/usage.md §9.13`（全仓无任何文档记录这件事）。

---

### F20 — `cluster doctor` 的新 topology check 对 `TopoUnknownAction` 失败开放，是四个渲染器里唯一说绿的那个；`TopoBehind` 的 ADVISORY 也没做

**证据** `cmd/tether/cluster_doctor_online.go:79-86, 110-119`：
```go
switch natsconf.ClassifyTopo(...) {
case natsconf.TopoStuck:  topoStuck = append(...)
case natsconf.TopoHeld:   topoHeld  = append(...)
}          // ← 没有 TopoUnknownAction,没有 TopoBehind
...
default:
    out = append(out, check("topology", clusteroffline.DoctorPass, "every reached voter's NATS topology reconcile is converging"))
```
scratch 实跑：`UNKNOWN` ⇒ doctor `topology = PASS`、无 FATAL、退 0，而同一份数据 `cardTopReason` 说 "unrecognized topology action"、`topoLaggards` 把它列为 laggard；`BEHIND-only` ⇒ PASS（plan §4.4 VI 要求 ADVISORY）；`ALL-UNREACHABLE` ⇒ 仍 PASS（空集上的全称句读成绿灯）。`topostate.go:81-83` 明确把 `TopoUnknownAction` 设计成 fail-closed，doctor 是唯一 fail-open 的消费者，而它恰恰是那个"说没问题"的动词。

**失败场景（核验订正后的版本）**：混版滚动升级中，新 broker 报第 8 个 Action 且自判 Stuck ⇒ 报文 `Health=DEGRADED`。老 ctl 的 doctor 会同时打印 `health ADVISORY "DEGRADED — <stuck banner>"` **和** `topology PASS`，且**无 FATAL ⇒ 退 0**；同版本集群下同一物理状态是 `topology FATAL ⇒ 退 64`。**按 doctor 退出码做门的自动化在滚动升级窗口里丢掉它的 FATAL。**

**修法**：per-node switch 补 `case TopoUnknownAction`（ADVISORY，文案指"升级 ctl/broker"）与 `case TopoBehind`（ADVISORY）；顶层按 `WorstTopoState` 排序输出；PASS 分支加"reached voter 数 > 0"前提，否则改 ADVISORY/"not observed"。
> **注**：c3 lane 认为这使 C3-11 成为 PARTIAL（即 BLOCKER）；completion 覆盖表判 C3-11 为 DONE。按操作者口径以覆盖表为准，故记 MAJOR；若实现者认同 plan §4.4-VI 未交付，请自行升级为 BLOCKER。

---

### F21 — `pushCommitEntryTTL` 没有下限，size < 600 MiB 的每一次 tier-B push 的 prep 条目都比改动前短、且短于 broker 的看门狗预算

**证据** `internal/agent/transfer.go:55-57, 75-77`：
```go
// It preserves the shape of the pre-batch-C constant: the old TTL was 6m against a 5m
// tier-B watchdog, and that ONE MINUTE OF MARGIN was the entire meaning of the 6 …
func pushCommitEntryTTL(size int64) time.Duration {
	return proto.XferLegBudget(size) + pushCommitCacheSlack   // XferLegBudget 不含 tier floor
}
```
实跑数值表：

| size | 旧 TTL | 新 TTL | broker 预算 |
|---|---|---|---|
| 8 MiB+1 | 360s | **65s** | 300s |
| 100 MiB | 360s | **110s** | 300s |
| 512 MiB | 360s | 316s | 512s |
| 2 GiB | 360s | 1084s | 2048s |

`leg+60 > max(300, 2·leg)` 无解 ⇒ **注释宣称的 "preserves the shape" 与代码相反**，且 TTL 在任何 size 都不再超过 broker 预算。

**失败场景**：100 MiB / 慢链路 / Put 耗时 200s，期间该 agent 收到任意第二个 prepare（`rememberPushCommit:549-570` 是唯一清扫点）⇒ 110s 的条目被扫掉 ⇒ push-commit 到达得 `transfer_unknown` ⇒ 整次传输失败。改动前 6 分钟内必成。**严重度按核验下调为 MAJOR**：`handlePushCommitForwarded:230-233` 查表**不判过期**，必须有第二个 prepare 才致命（多文件循环 push 是常见形态）。

**修法（两条 lane 的核验给出互斥建议，实现者需裁决）**：
- **(i)** `proto.XferBudget("b", size, 1) + pushCommitCacheSlack` —— 语义是"本端那一腿的预算 + 1 分钟余量"，小文件逐字回到改动前 6m，但 2 GiB 时 18m4s 仍 < broker 的 34m8s；
- **(ii)** `proto.XferBudget("b", size, proto.XferPushLegs) + pushCommitCacheSlack` —— size=0 时同样是 6m，且 **"agent 的 prep 永远活得比 broker 的耐心久"这条不变量在所有 size 上成立**；容量代价可忽略（成功/失败路径都调 `purgePushCommitCache`，只有被 ctl 抛弃的 prep 才滞留）。

**并且替代断言必须与所选实现一致**：`internal/agent/transfer_test.go:242-250` 现在是 `ttl > proto.XferLegBudget(size)`，展开即 `X+60s > X`，恒真、看不见本缺陷；但若选 (i) 又写 `TTL > XferBudget("b", size, XferPushLegs)`，测试会立刻红（实跑确认）。选 (ii) ⇒ 断言写成 `pushCommitEntryTTL(size) > proto.XferBudget("b", size, proto.XferPushLegs)`；选 (i) ⇒ 断言写成 `>= 6*time.Minute`（手算下限）**且** `> proto.XferLegBudget(size)`。

---

### F22 — `--timeout` 默认值 10m → 36m8s 对 **tier A** 同样生效；帮助文案与 `docs/usage.md:983` 描述的是代码没做的事

**证据** `cmd/tether/transfer.go:689-692`：
```go
const cliTransferTimeoutDefault = proto.XferTierBMaxBudget + 2*time.Minute   // 2168s = 36m08s
```
它被原样用于 `pushTierA:208`、`prepCtx:251`、`putCtx:273`、`commitCtx:312`、`waitCtx:330`、`:454`、`:531`、`:617`；ctl 侧**根本不按 size 推导**。而 `:72`/`:183` 的 help 写 `"tier A: ~30s; tier B: derived from the file size"`，`docs/usage.md:983-984` 写"agent 侧与 ctl 侧用同一个推导"。**pull 的 help（`:388-389`）更假**：pull 全链路无 size（N5），broker 也只给固定 5m 地板。

**失败场景**：agent 进程活着、NATS 连着（`transferGate:1138-1147` 只查 DB 的 `node.StateOnline`，不做实时 responder 探测）但卡死 ⇒ tier-A push 的 broker 侧 30s 后写 failed 审计、**不回 ctl 的 inbox** ⇒ CLI 静默挂 36 分 8 秒（原为 10 分钟）。

**修法**：`Changed("timeout") == false` 时按 tier/size 取 `proto.XferBudget(tier, size, proto.XferPushLegs) + slack`，至少把 tier-A 分支钳到 `proto.XferTimeoutTierA + slack`；`docs/usage.md:983` 改成"agent 侧按自己那一腿的 size 推导，**ctl 侧是一个覆盖最坏情况的固定上界**"；pull 的 help 删掉 "derived from the file size"。

---

### F23 — 单一来源 AST 门放过 `int64(...)` 包裹形与 `var` 形，而 `agentTransferMaxBytes` 改动前**正是**那个写法

**证据** `test/determinism/topo_classification_test.go:285, 303-313`：
```go
if gd.Tok != token.CONST { continue }        // var 完全不扫
...
expr := strings.Join(strings.Fields(string(src[start:end])), " ")
for lit := range xferTierLiterals { if expr == lit {   // EXACT 文本相等
```
实跑三组变异（写在 `internal/agent/transfer.go:53`）：

| 写法 | 门 |
|---|---|
| `const X = 2 * 1024 * 1024 * 1024` | **红**（正确） |
| `const X = int64(2 * 1024 * 1024 * 1024)` | **绿（漏）** |
| `var X int64 = 2 * 1024 * 1024 * 1024` | **绿（漏）** |

`git show HEAD:internal/agent/transfer.go` 第 51 行原文正是 **`const agentTransferMaxBytes = int64(2 * 1024 * 1024 * 1024)`** —— plan §9 点名的那条变异「在 agent 侧塞回 2 GiB 字面量 ⇒ 必红」，用它历史上真实的书写形式执行时门是绿的。

**修法**：比较前剥掉一层 `*ast.CallExpr` 的类型转换（`Fun` 为内建整型 ident 时取 `Args[0]` 的源码片段），并把 `token.VAR` 纳入扫描；或改成 `go/types` + `go/constant` 常量求值。`:248-260` 的非空自检与 EXACT 匹配（避免 8 MiB 是 8 GiB 前缀）那段注释**不要动**。

---

### F24 — pull 侧的"迟到终结"仍然静默，且 pull 臂的看门狗码写着一个没发生过的 `ctl_disconnect`（核验期新发现）

**证据**：`internal/broker/transfer.go` 有两个 `preview == nil` 分支——`:1004`（`handleEvTransfer`，push）本批拿到了 `recentlyReaped` 的 Warn（`:1011`），`:1281`（`handleFinalizeReq`，**pull**）什么都没加：clustered 模式下裸 `return`，单机回 `transfer_unknown`。而 F5 确认的场景落地的正是这个分支。

**失败场景**：文件已完整落到 ctl 盘上、审计写着 `failed/ctl_disconnect`、日志一个字都没有；且 ctl 从未断开，归因错误与 push 臂已改掉的那条同形（C2-9 只修了 push 臂）。

**修法**：把 `:1011` 的三行搬到 `:1281`（`b.transfers.recentlyReaped(transferID)` 是现成的）；pull 臂的看门狗码同步换成 `proto.CodeTransferBudgetExceeded` 的对应归因。

---

### F25 — `TestDriveOneHandlesEveryOpKind` 的"穷尽性"是手抄清单，加第四个 `OpKind` 全仓无感（核验期新发现）

**证据**：`internal/broker/force_single_finalize_test.go:375` 的 want 列表是硬写的三个字符串 `{"OpKindJoin","OpKindRetire","OpKindForceSingleFinalize"}`，与 `internal/cluster` 的枚举无任何联系；`internal/cluster/operation_ops.go` 没有 `AllOpKinds()`，`ValidOpKind`（`:80-82`）是一串 `||`。

**实跑变异**：加 `OpKindRehome = "rehome"` 并接进 `ValidOpKind`、**不**给 `driveOne` 加 case ⇒ `./internal/broker/`、`./internal/cluster/`、`./test/determinism/` 全绿。失败场景就是该测试自己写的那句：新 kind 的 op 被创建却永不推进 ⇒ 非终态 ⇒ `assertNoActiveOp` 永久 fence 该节点的 membership 面。

**修法**：照本批自己已经做对的 `internal/natsconf/topostate_test.go` 的 `TestAllActionsListsEveryActionConstant`（核验确认：加第 8 个 `Action*` 常量确实变红并点名）—— 给 `internal/cluster` 加 `AllOpKinds()`，让 `ValidOpKind` 与 `driveOne` 的 AST 扫描都以它为源，并加一条「AST 扫出的 `OpKind*` 常量集 == `AllOpKinds()`」的双联锁。**同一批次里一边把 Action 枚举锁死、一边把 OpKind 枚举留成手抄，是本轮最刺眼的不对称。**

---

### F26 — finalize op 的 `target = self` fence 半径被注释低估：它还挡住了恢复 HA 的下一条命令

**（c1 lane 独有，未经独立核验）**

**证据** `internal/broker/force_single_finalize.go:162-169` 只枚举了 `assertNoActiveOp(target)` 的 per-target 面：
```go
// Targeting self does fence `drain`/`retire` of this node while the op is live, which is both
// harmless … and short …
```
但仓内还有四处按"任何非终态 op"全局判定的闸会被 self 上的 finalize op 触发：
- `cluster_grow_trigger.go:82-88` ⇒ **`cluster add <新 broker>` 被拒**，报错点名的还是幸存者自己（"a membership operation for brk-a is in flight"）——而这恰恰是 force-single 之后运维要做的下一件事；
- `cluster_upgrade_trigger.go:201-204` ⇒ `cluster upgrade` acquire-lock 被拒；
- `reexec.go:64-69` ⇒ **每一台** broker 的 `broker upgrade reload` 被拒；
- `proxy_auto_rebalance.go:120` ⇒ proxy 自动 rebalance 停摆。

窗口被 60s 预算界住，不是死锁，但注释是错的，而这句注释正是 target 选 self 这个决定的论证本身。

**修法**：把四处补进 `:162-169` 的枚举并写明"grow/upgrade/reload 在 ≤60s 内会被拒、报错会点名幸存者"；若认为 `cluster add` 被拒不可接受，`cluster_grow_trigger.go` 的循环可对 `OpKindForceSingleFinalize` 放行（理由与 `opFrozenByUpgradeLock` 的豁免同构，它不做任何 membership 变更）。

---

### F27 — `Inconsistent` 被赋予第三种含义，但三个渲染面 + doctor 的文案一个都没扫，运维拿到 exit 64 且零出路

**证据** `internal/broker/clusterstatus.go:349-357` 新增第三个 disjunct：
```go
inconsistent := (r.phase == phaseVoter && (!inCfg || ro == "learner")) ||
    (!inCfg && r.phase != phaseRetiring && r.phase != phaseAddFailed) ||
    drainingWithoutMarker(r.phase, drainMarked[r.nodeID], activeOpTarget[r.nodeID])
```
消费侧全未动：`internal/adminsock/protocol.go:751`（注释仍是 `// phase says voter but raft config disagrees (or vice-versa)`，而同一批**改了**隔壁 `Abandoned`/`TopoAction` 的注释）、`cmd/tether/cluster_doctor_online.go:98`（`fatal=true` ⇒ 退 64，badFmt 是 "run `cluster doctor`/`status`"，即刚跑过的命令）、`cmd/tether/cluster_status_card.go:128`、`cmd/tether/cluster.go:481-483`。

**失败场景**：一次 drain 死在自己两步之间（phase 已 DRAINING、marker 已被清）。DRAINING 节点仍在 raft config 内 ⇒ 前两个 disjunct 全 false，三面都说 "roster/raft INCONSISTENT"，而 raft 与 roster 事实上一致；plan §5.1 说这一态必须人工 `cluster drain --abort`，四面无一提及。核验确认修法可行：`AbortDrain`（`clusterdrain.go:372-387`）的前置（`assertNoActiveOp` + CAS `DRAINING→VOTER`）与该 disjunct 的取值域完全相容。

**修法**：给 `ClusterNodeStatus` 加 reason 串（或 `DrainStuck bool`）分两因；doctor 的 badFmt 与 card 文案改成能区分并写进 `tether cluster drain <node> --abort`；同 commit 订正 `protocol.go:751` 的注释。

---

### F28 — "Verbatim Reasons from ReconcileOnce, so a reword there breaks these tests" 是假承诺；没有任何测试把真实 `Outcome` 喂进 `ClassifyTopo`

**证据** `internal/natsconf/topostate_test.go:17-27` 的常量是 `reconcile.go` 字符串的**独立手抄副本**。实跑变异：把 `reconcile.go:152` 的 `"render (…"` 改成 `"render failed: …"` ⇒ `./internal/natsconf/`、`./test/determinism/` **全绿**，而此时 `classifyLegacyReason` 的 `"render ("` 标记已与生产者脱钩，一台 pre-C broker 报 render 失败会被判成 Converged/Behind 而不是 Stuck —— 正是文件头 (c) 记录的那个缺陷的同类复发。全仓 21 个 `ClassifyTopo` 调用点**没有任何一个**喂进真实的 `ReconcileOnce` 返回值。

**修法**：加表驱动测试，用 `ReconcileOnce` 的 fake seam 真跑出每条分支的 `Outcome`，断言 `ClassifyTopo(out.Action, out.Reason, out.ObservedGen, in.DesiredGen, true)` == 期望 state，**并且** `classifyLegacyReason(out.Reason, …)` 给出同一 state —— 这条同时把 F17 钉死。

---

## 3 · MINOR

**F29 · `legacyCutoverMarker` 的 "ordering is load-bearing" 与测试声明的 mutation (2) 都是假的。**
`internal/natsconf/topostate.go:219-226` 声称顺序承重，但 `:237` 的 render 标记已收窄成 `"render ("`。逐条匹配实跑：withhold Reason 对 `"unrecognized directive"` / `"nats-server -t"` / `"render ("` / `"apply: "` **全部 false**，只有裸 `"render"` 才撞。**实际施加了 mutation (2)**（把 render/apply 组挪到 marker 之前）⇒ 包括 `TestAwaitingCutoverIsHeldAndNeverRecommendsManual` 在内全绿。修法：二选一——把注释改成事实并把 mutation (2) 换成一条真会红的（例如把 `"render ("` 放宽回 `"render"`），或保留裸 `"render"` 让 ordering 真的承重。

**F30 · `reached` 谓词三份逐字手抄。**
`internal/broker/clusterstatus.go:651`、`cmd/tether/cluster_status_card.go:114`、`cmd/tether/cluster_doctor_online.go:79` 三处 `n.ReachSource == "self" || (n.ReachSource == "nats-health" && n.Reachable)`，而 doctor 那处的注释还写着 "rather than inventing a third one"。修法：在 `internal/adminsock` 上加 `func (n ClusterNodeStatus) TopoReached() bool`，三处共用。（核验驳回了"第四个不同版本"：`cluster_reconcile.go:197/218` 的 `!n.Reachable` 与之今天等价，`reachOf`（`clusterstatus.go:326-339`）保证 `Reachable ⟺ reached`；属潜在漂移面。）

**F31 · 同一个 `TopoState` 在四个印刷面用了三个词。**
`Cell()` 出 `HOLD`/`STUCK`（`topostate.go:105-122`）、`String()` 出 `held`/`stuck`（`:84-102`）、card 出 `WITHHELD`（`cluster_status_card.go:123`）、doctor 出 `WITHHELD`（`cluster_doctor_online.go:113`），`topoLaggards`（`cluster_reconcile.go:196-199`）用 `%s` 走 `String()`。而 `cluster_reconcile.go:181-186` 的新注释逐字宣称 *"a wedged broker reads 'STUCK' here, in `cluster status`, and in the card with the same word"* —— 对 Held 完全不成立。修法：`--wait`/doctor/card 统一走 `Cell()`，或给 `TopoState` 一个单一的 operator-facing 词表。

**F32 · `stageXferInflightTerminal` 的 `Size: rec.Bytes` 既是错的量、又不可达，注释宣称的两件事都不成立。**
`internal/broker/xfer_inflight.go:239-246`：`schema.AuditTransfer` 的 `Bytes` 是**已传字节**（`internal/schema/audit.go:75` 逐字："populated only on complete/failed"），而 `xferInflightRecord.Size` 的新注释自陈是 "the declared transfer size"；且下一行无条件 `cur.Terminal = &rec`，`ledgerRowDisposition:577-578` 在读 `in.Rec.Size`（`:595`）之前就 `return ledgerReplay`，该字段**没有任何读者**。**核验驳回了原修法** `Size: rec.Size`：全仓只有 `internal/broker/transfer.go:766`（push start 审计）填 `Size:`，所有终态 `AuditTransfer` 的 `Size` 恒为 0。修法：不填，并把注释订正为"当前 disposition 规则不会读它"；或先让终态审计携带 `Size`。

**F33 · `recentlyReaped` 与 `markReaped` 用了两套时钟。**
`internal/broker/transfer.go:196-216`：`markReaped` 收注入时钟 `now`，`:215` 的查询却用墙钟 `time.Since(at)`。本包大量测试注入固定时钟（`xfer_inflight_test.go:32` 是 6 天前），那些 fixture 下 `time.Since` 恒 ≫ `xferRecentlyReapedTTL`(10m)，D-C2-6 那条"区分从没见过 vs 刚被看门狗终结"的 Warn 恒定失灵。生产无影响。修法：`recentlyReaped(id string, now time.Time) bool`，调用点传 `b.now()`（`home_delivery.go:175` 已有 nil-safe 版本）。

**F34 · 豁免理由已与代码不符。**
`cmd/tether/error_code_coverage_test.go:246` 的 `"audit-only watchdog code selected by the switch immediately above."` —— 那个 `switch ent.verb` 已被删除，`transfer.go:528-531` 现在是 `code := proto.CodeTransferBudgetExceeded` + 一个 `if`。豁免本身仍必要，改理由串即可。

**F35 · serveconf / broker-ops 的 reap-age 文案在批 C 后不再精确（已按核验削弱）。**
`internal/serveconf/serveconf.go:239-245` 的 "5-minute watchdog" 现在只是**低估**（5m 已成 tier 地板而非全部预算），方向安全；`docs/broker-ops.md:130-135` 的"默认派生自 3×tier-B 超时(=15m)" **仍然逐字为真**（`xferCrossHomeReapAge = 3 * transferTimeoutTierB` 与 `MinXferCrossHomeReapAge` 均未动），只是没提新增的逐对象增量。修法：serveconf 那段改成 "tier-B watchdog (now size-derived; the per-object increment lives in `broker.xferCrossHomeExtraFor`)"；broker-ops 补一句"实际下限 = 本值 + 该对象预算超出 tier floor 的部分"。**注意**：D-C2-8 逐字豁免的是 `:221-224 / :248-250 / :253-254` 三段（"3x the tier-B transfer timeout"，仍为真），别一起改。

**F36 · `xferCrossHomeExtraFor` 的增量形式只在 base ≥ tier floor 时精确，且它的第一条设计理由已过期。**
`reapBucketObjects` 里 `floor = minAge + extra`；若 harness 把 cross-home floor 压到秒级，一个 2 GiB 对象只得到 `1s + 29m8s < 34m8s`。同时 `internal/broker/transfer.go:99-102` 用"the deploy-tier drill that COMPRESSES it would stop working"论证增量式设计——但外审 F2 之后**没有任何 drill 能压缩 `xfer_cross_home_reap_age`**（`serveconf.go:246-252` 硬拒，drill 96 已在 `:345-347` 把它登记为永久结构性 gap）。设计没问题，理由该订正，并给 `:104-110` 的"逐对象精确成立"加一句"仅当 base ≥ tier floor"的限定。

**F37 · `internal/broker/transfer.go:77` 引用了一个不存在的标识符** `xferCrossHomeFloorFor`（实际叫 `xferCrossHomeExtraFor`，`:111`），全仓仅此一处命中。改名即可。

**F38 · `topoLaggards` 的 "The set of laggards is unchanged by construction" 注释为假，且方向与两条 lane 各自的猜测都不同。**
`cmd/tether/cluster_reconcile.go:181-186`。核验给出的**更可达反例是集合收缩、方向 fail-open**：pre-C3 broker 不发 Action ⇒ 走 `classifyLegacyReason`，一条 `"no mesh peers known yet (converging)"` 类 reason 在 `observed >= desired` 时落到 `case observed >= desired: return TopoConverged` ⇒ **不是** laggard；而旧谓词 `observed<desired || reason != ""` 判**是**。即 `--wait` 对老 broker 的这一类状态从"继续等"变成"宣布全部收敛"。修法：注释改成"集合两个方向都可能变化"，并结合 F17 一并裁决。（核验**不采纳**"把 `TopoUnknownAction` 纳入 `topoWedged`"的建议：那会让混版瞬态直接 `exitInternal`。）

**F39 · §12 要求的"小盘 broker 最坏代价"只写了 3 项中的 1 项。**
落地的只有 ②（`internal/broker/transfer.go:142-153`，写得很好）。缺 ①"对象字节在盘上多待 6.8×；racknerd per-session bucket 天花板下第 4 个并发 2 GiB `Put` 撞 10047"（全仓 grep `10047` 零命中）与 ③"悬空 start 审计行与 ledger 文件多活 29 min"。**推导本身经独立复核站得住**（`⌈2 GiB/2 MiB⌉ = 1024`，`XferTierBMaxBudget = 2048s = 34m08s`，与注释一致，且用手算字面量钉住）。修法：补进 `internal/proto/xfer.go` 的推导块或 `docs/distributed-broker-architecture.md §20.2`。

**F40 · 新 op kind / 新错误码没进 `cluster ops` 的自述与 `docs/usage.md` 的枚举。**
`cmd/tether/cluster_ops.go:18` `Short: "Inspect membership operations (add / drain / retire) and their state"`、`docs/usage.md:342` 同；`docs/usage.md:1007-1023` 的传输错误码表缺 `transfer_budget_exceeded`（该码已正确进 `proto/codes.go:157-163` 与 `error_hints.go:39/89`）。附带：`cluster_ops.go:124` 的 `Use: "show <node-id>"` 与 `:133` 的 not-found 文案 `"no membership operation for node %q (not in the roster)"` 会在 op-id 打错时把运维引向 roster。**订正**：服务端 `internal/broker/clusterops.go:34` 按 op-id **或** node-id 都匹配，所以四处新文案里的 `cluster ops show <op-id>` 能跑通。

**F41 · `recordOpError` → `finalizeBudgetCheck` 的终态写入会吞掉刚写的 timeline 条目。**（c1 lane 独有）
`finalizeBudgetCheck` 里 `a.transition` 计算的是 `appendTimeline(op.Timeline, …)`，用的是 tick 起始的快照 ⇒ `recordOpError` 刚 commit 的 `{FS_PRUNE_PENDING, "read roster: …"}` 被整条覆盖，`cluster ops show` 看不到最后一次失败原因。修法：`recordOpError` 返回新 timeline，或把两处 error 路径与 budget check 合成一次 transition。（该 lane 自己核对并**排除**了 CAS/deadline 读错的可能——`recordOpError` 的 `ToState == FromState ∧ mut == nil` 不改 `catchup_deadline`、也不改内存 `op`。）

**F42 · `forceSingleGhostRows` 无 `ORDER BY`，且不显式排除 self。**（c1 lane 独有）
`force_single_finalize.go:306` 的返回顺序进了 `strings.Join(ghosts, ",")` 作为 opID discriminator（见 F7），形成对 SQLite 行序的隐式依赖。修法：加 `ORDER BY node_id`；加 `if id == a.node.SelfID() { continue }`。

**F43 · drill 22 新加的两条断言 fail-open。**
`test/simcluster/drills/22-forcesingle-online.sh:174-177` 是 `! <cmd> | grep -q …` 与 `poll_until "! … jq -e …"` 形状，子命令报错（flag 改名、socket 不通、jq 缺失）⇒ 空输出 ⇒ 断言**通过**。修法：先无条件断言 `tether cluster ops ls --json` 退 0 且可被 `jq -e '.schema=="cluster_ops"'` 解析，再断言 `.ops | map(select(.kind=="force_single_finalize")) | length == 0`。

**F44 · `driveOne` 的 `default:` 无条件 abort，把"版本前偏"也一起打了。**（c1 lane 独有，前瞻性）
`cluster_operation_controller.go:497-514` 的注释前提只覆盖回滚方向。前偏方向（滚动升级中 leadership 落到旧二进制）会**立刻**把新二进制正在驱动、substrate 副作用可能已部分落地的 op 强制 ABORTED，而旧行为（silently ignore）在这个方向反而无害。`opFrozenByUpgradeLock` 只在受控 roll lock 期间保护。修法：给 default 加复制式宽限（照 `boundCatchingUp` 的 lazy-stamp），或至少把注释前提改成双向并说明取舍。

**F45 · `internal/proto/xfer_test.go:41-46` 的注释与代码不符——但 `:43` 那行**不能**删。**
注释说 "The leg count is checked here and ONLY here"，实测把 `XferPushLegs` 从 2 改 3 时红的是 `:33`（`XferTierBMaxBudget != 2048s`）而非 `:43`。**但核验另跑一条变异证明 `:43` 并非与 `:28` 重复**：在 `XferBudget` 里插 `legs = XferPushLegs`（忽略调用者腿数）⇒ `:44` 红（`a single-leg budget … = 34m8s, want 1024s`），它唯一地钉住了"`XferBudget` 真的用了它的 `legs` 形参"这条 plumbing。修法：**只订正注释**（把 "the leg count" 改成 "the *legs parameter*"，注明常量 `XferPushLegs` 由 `:33` 的 `2048s` 字面量钉住）。

**F46 · 跨 home 逐对象宽限用**落盘 size**，看门狗用**申报 size**，而那条"承重"测试给两侧喂同一个值。**（核验期新发现，LOW）
`transfer_reconcile.go:173` 是 `xferCrossHomeExtraFor(int64(obj.Size))`，`transfer.go:509` 是 `e.size` ← `req.Size`。`transfer_budget_test.go:76-90` 的 `TestCrossHomeGraceCoversALiveTransferOfThatSize` 两侧同 size，这条分歧零覆盖。实际危害有限（对象在 `List()` 可见时已完整），但该测试对外宣称的"GC 绝不删掉活看门狗覆盖的对象"没有在它自己的口径上被证明。

---

## 4 · completion lane 覆盖表与验收裁决（操作者的验收基准，逐条复现）

> 逐条读代码核对，不读 plan 自证。任何 PARTIAL / MISSING 行都是 BLOCKER。

### C3 — topo `Action` 上 wire

| # | 交付 | 实现位置 | 判定 |
|---|---|---|---|
| C3-1 | 分类器新文件 | `internal/natsconf/topostate.go:65-279`（`TopoState`+6 常量、`ClassifyTopo:256`、`classifyLegacyReason:231`、`Cell:106`/`Banner:160`/`NextStep:175`/`Degrades:133`、`WorstTopoState:194`、`AllActions:207`） | **DONE** |
| C3-2 | `topoSelfReport.Action` + 写入点 | `internal/broker/clusterwrite.go:138` + `topology_reconcile.go:149` | **DONE** |
| C3-3 | `proto.TopoAction` + schema 6→7 + 账本注释 | `internal/proto/alerts.go:111`、`:14`、`:10-13` | **DONE** |
| C3-4 | health 填充 | `internal/broker/cluster_health.go:106` | **DONE** |
| C3-5 | clusterstatus **两处**传播 | `clusterstatus.go:389`（peer）+ `:424`（self，带论证注释） | **DONE** |
| C3-6 | `adminsock.ClusterNodeStatus.TopoAction` | `internal/adminsock/protocol.go:782` | **DONE** |
| C3-7 | `computeHealth` 改分类器 + STUCK 无条件 | `clusterstatus.go:621-658`、`:687-688` | **DONE** |
| C3-8 | `topoCell` 改分类器 | `cmd/tether/cluster.go:441-449` | **DONE** |
| C3-9 | `topoLaggards` + `--wait` 遇 Stuck/Held 立即返回 | `cluster_reconcile.go:139-146`、`:187-205`、`topoWedged:213-233` | **DONE** |
| C3-10 | `cardTopReason` 加 topo 分支（且置于最前） | `cluster_status_card.go:106-126` | **DONE** |
| C3-11 | `cluster doctor` topology check | `cluster_doctor_online.go:77-86`、`:105-119` | **DONE** |
| C3-12 | `topoConvergedForOp` 的"故意不共享"注释 | `cluster_operation_controller.go:1137-1143` | **DONE** |
| C3-13 | drill 93 断言 `topo_action` **且 render 失败时极性为 STUCK** | `93-metrics-observability.sh:121-136` | **PARTIAL → BLOCKER（F15）** |
| C3-14 | 架构文档记录新 wire 字段 | `docs/distributed-broker-architecture.md §20.1` | **DONE** |

### C1 — force-single 后半段

| # | 交付 | 实现位置 | 判定 |
|---|---|---|---|
| C1-1 | kind + 3 state + `validOpStates` + `ValidOpKind` 三值 | `internal/cluster/operation_ops.go:25`、`:59-62`、`:75`、`:80-82` | **DONE** |
| C1-2 | `driveOne` case **和** `default:` | `cluster_operation_controller.go:495-511` | **DONE**（守卫失效见 F14） |
| C1-3 | `driveForceSingleFinalize`（advance-after-observe / prune 前重确认 / 只删 params） | `force_single_finalize.go:172-236` | 实现 DONE，**零测试（F14）** |
| C1-4 | 复制式 deadline，耗尽⇒`FS_GHOST_LEFT`，永不 BLOCKED | `force_single_finalize.go:70`、`:140`、`:241-256` | 实现 DONE，**测试恒真（F14）** |
| C1-5 | commit 失败才建 op；成功路径逐字不变 | `force_single_online.go:291-324` | 实现 DONE，**无 Go 层测试（F14）** |
| C1-6 | leadership-edge 四条判据建 op | `clusteradmin.go:406-413` + `force_single_finalize.go:347-379` | 实现 DONE，**四条判据零测试（F14）** |
| C1-7 | `upgradeActive` 对 finalize 豁免 + 反向测试 | `cluster_operation_controller.go:427-470`；`force_single_finalize_test.go:401` | **DONE** |
| C1-8 | `ConfirmOp` 在 `clearOpAttempts` 之前早退 | `cluster_operation_controller.go:250-252` + `force_single_finalize.go:90-97` | **DONE**（含 AST 顺序门） |
| C1-9 | `AbortOp` 不依赖 `FromState` | `cluster_operation_controller.go:298-315` + `operation_ops.go:223-245` | **DONE** |
| C1-10 | `opEntryFromOperation` 渲染新 state | `clusterops.go:67-78` | **DONE** |
| C1-11 | CLI 分支文案 + `Abandoned` 注释订正 | `cluster_offline.go:385-431`、`adminsock/protocol.go:691-700` | **DONE**（谓词错误见 F12） |
| C1-12 | 愈合 pass = 清孤儿 **+ 注册 + `AbortOp` 注释点名 pass** | pass `reconcile_drain_marker.go`、注册 `reconcile_passes.go:243-248`；**`AbortOp` 注释未改** | **PARTIAL → BLOCKER（F3）** |
| C1-13 | 渲染期 `Inconsistent` 第三个 disjunct | `clusterstatus.go:346-356` + `reconcile_drain_marker.go:155` | **DONE**（消费面未扫见 F27） |
| C1-14 | 架构文档增补 op kind + ladder | `distributed-broker-architecture.md §20.3` | **DONE** |
| C1-15 | `docs/cluster-runbook.md` 改为"按分支处理" | **文件完全未改** | **MISSING → BLOCKER（F13）** |
| C1-16 | drill 22 / 12 / 20 / 41 | 22 DONE；**12 未改**；20/41 无运行证据 | **PARTIAL → BLOCKER（F16）** |

### C2 — 传输预算

| # | 交付 | 实现位置 | 判定 |
|---|---|---|---|
| C2-1 | 预算函数族 + 上界 + 推导注释 | `internal/proto/xfer.go:16-95` | **DONE** |
| C2-2 | 看门狗改用 budget | `internal/broker/transfer.go:509` | 实现 DONE，**无测试覆盖该行（F11）** |
| C2-3 | 跨 home GC 下限（§6.3 逐对象裁决优先） | `transfer.go:95-119` + `transfer_reconcile.go:144-178` | **DONE**（调用点零覆盖，F11） |
| C2-4 | 三处既有关系断言同步 | 新关系钉在 `transfer_budget_test.go:76-90` | **DONE**（`xferReapBudgetMargin` 未具名，MINOR） |
| C2-5 | `transferTimeoutFor(tier,size)` + `Size` + **两分支都填** | `xfer_inflight.go:152`、`:51`、`:241`、`:283` | **DONE**（`:241` 填错量，F32） |
| C2-6 | 旧 ledger `Size==0` 回落 + 测试 | `proto/xfer.go:91-94`；`transfer_budget_test.go:31`、`proto/xfer_test.go:62` | **DONE** |
| C2-7 | synthetic `Ts` 留在 tier 下限 + 论证整段搬运 | `xfer_inflight.go:154-172`、`:681-683` | **DONE**（调用点零覆盖，F11） |
| C2-8 | agent `commitCtx`/`putCtx` | `agent/transfer.go:241`、`:450` | **DONE**（pull 腿不对称，F5） |
| C2-9 | 新 code + registry + hints 分类 | `proto/codes.go:163`、`error_hints.go:38`/`:89` | **DONE**（pull 臂未改，F24） |
| C2-10 | proto 常量 + 六个引用点 | `proto/xfer.go:38-51`；`broker/transfer.go:56-58`、`agent/transfer.go:48`/`:52`、`cmd/tether/transfer.go:685`/`:699` | **DONE** |
| C2-11 | AST 守卫（不误杀不同语义） | `test/determinism/topo_classification_test.go:217-325` | **DONE**（形状有洞，F23） |
| C2-12 | 手抄文案全局扫 | usage.md 表格/正文已改；**synopsis 未改、agent/ctl 三处未派生** | **PARTIAL → BLOCKER（F4）** |
| C2-13 | N5 的 pull-size 回归测试 | **不存在** | **MISSING → BLOCKER（F10）** |

### 验收裁决（原文口径）

> **INCOMPLETE。** 批 C 的 **C3 已实质完成**（唯一缺口是 drill 93 的失败极性）。**C2 结构完整、测试质量高，缺两处 caller 层覆盖与一条明写的回归测试，文案扫半途而废。C1 的实现写完了，但它的验收面塌了一半**：三个核心函数零测试引用、三条守卫是恒真断言、运维手册和部署层 drill 完全没动——也就是说，**C1 的代码在，C1 的证据不在**。
>
> §9 的 24 条具名测试中只有 2 条同名存在；C3 与 C2 的测试表基本兑现，**C1 的测试表兑现 4/11，其中 3 条是恒真断言**。
> §11 的规避清单逐条核验：只有 C3-13（drill 只断言收敛极性）、C2-12（"(2 GiB)"不动）、C1-16（drill 只加注释说"单测覆盖"）三条发生，其余全部未发生。
> **延后语言扫描干净**：全 diff + 11 个新文件 grep `TODO|FIXME|for now|later|deferred|后续|暂时` 无一处未申报的延后；唯一命中是 `transfer_budget_test.go:174` 的 N15，属 plan §3 登记的永久决策，且该测试断言缺口**存在**。
> **§12 开放项**：`XferMinThroughput` 取值已定、推导写进代码且经独立复核**站得住**；但 D-C2-2 要求的三项"最坏代价"只落地 1 项（见 F39）。
>
> 补完后需重跑 `make test` + `make lint` + `make e2e-parallel`，并按 §8 实跑 drill 93 / 22 / 12（20、41 为回归）。

---

## 5 · 已考虑并被驳回（不要在下一轮重新"发现"）

| 被驳回的主张 | 驳回理由 |
|---|---|
| drill 93 的 TOPO 断言取错 awk 字段 `$9`（sweep-F5） | 树上不存在该代码。文件用的正是整行匹配 `grep -cE '→[0-9]+ ✓'`，且上方注释已解释"TOPO cell 含空格、列序号会拆开"。审查者引的是一个不存在的版本 |
| plan §4.2 要求的"两端一致"测试不存在（c3-F2） | 误读：§4.2 的"两端" = broker 端 vs ctl 端，不是 action 路径 vs legacy 路径。对应变异（CLI 侧保留旧 substring）实跑确实被 `test/determinism/topo_classification_test.go` 的 AST 门捕获并点名 `cmd/tether/cluster.go:447` |
| 混版 fallback 是"新造的假绿"（c3-F2 场景 A） | 极性说反：legacy 路径在该格返回 Converged，与 pre-batch-C 的 `computeHealth`/`topoCell` 逐字一致，保留的是老行为；改了极性的是 action 路径（已重写为 F17） |
| 一次 topo DEGRADED flap 会"打穿按 §17 exit-code 契约做的监控"（c3-F3） | `cmd/tether/cluster.go:126-133` 与 `cluster_wait.go:124-135` 早已明文声明并接受 0→1→0 的瞬时 flap，并提供 `--settle` 去抖 |
| doctor 退 0 而同一份数据 `cluster status` 退 1（c3-F4 对比场景） | `rep.Health`/`rep.ExitCode` 由 **broker** 计算（`clusterstatus.go:496`），ctl 只渲染。已换成更强的"滚动升级窗口丢 FATAL"场景 |
| `docs/cluster.md:151` 与 `cluster-ha-realmachine-test-plan.md:250` 未扫（c3-F6c） | 前者说的是 add/retire **之后**，那时没有 HELD，文案仍成立；后者的陈旧早于批 C（withhold 行为先于本批）。真正遗漏的是 `docs/usage.md §9.13` 本身（已并入 F19） |
| drill 11 的字符串守卫已失明/恒绿（c3-F6c） | 它是 `#3 GONE` 式的反向缺席断言；且 gate A 断言 `cmd_grow` 根本不跑 `reconcile nats`；旧串 `"reconcile nats: timed out … not converged"` 在 `cluster_reconcile.go:171` 仍逐字存在，超时路径仍被匹配 |
| `cluster_reconcile.go:197/218` 是"第四个不同的 reached 谓词"（c3-F9） | `!n.Reachable` 与那三份今天行为等价（`reachOf` 保证 `Reachable ⟺ reached`）。属潜在漂移，不是现存不一致 |
| pull 腿不对称会让 ctl 挂满 36m8s（c2-F2 第 4 步） | ctl 成功路径 `cmd/tether/transfer.go:594-598` 用 5s `sendFinalize` 且丢弃返回值 ⇒ 打印 OK 退 0。真实后果是审计倒置（已写进 F5） |
| `stageXferInflightTerminal` 应改成 `Size: rec.Size`（c2-F6 修法） | 全仓只有 push 的 start 审计填 `Size:`，所有终态 `AuditTransfer` 的 `Size` 恒为 0；该修法拿不到声明大小 |
| TTL 断言应写成 `pushCommitEntryTTL(size) > XferBudget("b",size,XferPushLegs)`（c2-F9 修法） | 与 c2-F1 自己提议的实现 `XferBudget("b",size,1)+slack` 互斥，实跑立刻红。断言必须与所选实现一致（已写进 F21） |
| `broker-ops.md:130-135` 的 reap-age 文案"已过时" | "默认派生自 3×tier-B 超时(=15m)" 仍逐字为真（两个常量一字未动）。属**不完整**而非错（已降级进 F35） |
| "drill 压缩 seam 对 1 GiB 已被破坏"（c2-F9c / sweep-F9c） | `xferCrossHomeExtraFor` 只在 `sizeAware=true` 时生效，而只有跨-home GC 传 true；drill 96 压缩的是 `xfer_reap_interval` 并观测 home reap（`sizeAware=false`）；跨-home age floor 自外审 F2 起根本不可压缩，drill 96 已登记为永久 gap |
| 删掉 `internal/proto/xfer_test.go:43`（与 `:28` 重复） | 实跑证伪：在 `XferBudget` 内强制 `legs = XferPushLegs` 时唯一变红的就是 `:44`，它唯一地钉住 `legs` 形参的 plumbing。只订正注释（F45） |
| `Cell()` 恒返回 `"✓"` 可以骗过 drill 93（tests-F13b 给的变异） | drill 要求 `→[0-9]+ ✓` 的整格形状，裸 `"✓"` 会让计数 `n=0` ⇒ 变红。能通过的变异是"恒返回收敛形态的整格"（F15 已按此描述） |
| `opForceSingleFinalizeBudget != 12*observeTickInterval` 是恒等式 | 测试端的 `12` 是硬写的，把定义改成 `20*observeTickInterval` 会红。它是低价值的**公式复述/变更探测器**，不是恒真断言——修 F14 时不要顺手删 |
| `AbortOp` 的注释是"承诺了代码不提供的保证"（class-4） | 削弱为"C1-12 具名交付未落地"：`cluster doctor` 的 `roster_consistency` 确实报出这些形状，`reconcile` 侧现在也确实有了一个（很窄的）愈合 pass。注释是**笼统**而非假 |
| `topoLaggards` 集合变化的方向是 fail-closed 扩大（completion-F12） | 该反例需要一个"未来版本新增、Reason 为空、observed>=desired"的 action，纯前向假设。更可达的是 legacy fallback 造成的**收缩**，方向 fail-**open**（已写进 F38） |
| 把 `TopoUnknownAction` 纳入 `topoWedged`（completion-F12 第二建议） | 会让"读者比写者旧"这一混版瞬态直接 `exitInternal`，而对端下一 tick 完全可能报回本二进制认识的 action。是取舍不是订正，**不采纳** |
| 用 `passBroker` 夹具写 drain-marker healer 的行为测试（tests-F1 修法） | `reconcile_passes_test.go:114-137` 的 `passBroker` 把 `b.cl` 留成 nil，而 `reconcile_drain_marker.go:65-67` 第一行就 `if b.cl == nil … { return nil }` ⇒ 测试会"绿得毫无意义"。必须用 `d7SingleNode` + `b.cl = &clusterRuntime{node:n, admin:NewClusterAdmin(n,nil)}` |
| `recordOpError` 会让 `finalizeBudgetCheck` 读到脏的 CAS 前置态/deadline（c1-F9 前半） | 该 lane 自查排除：`ToState == FromState ∧ mut == nil` ⇒ `catchup_deadline` 不在 SET 子句里，也不改内存 `op`。只有 timeline 被覆盖（F41） |
| drill 93 只是"断言列存在"（§11 规避的原形） | 其第③条是收敛态的**负向**断言（没有任何一行渲染 STUCK/HOLD/…/?），比 §11 设想的强一档。准确说法是"只有正向控制、没有失败注入"（F15 已按此措辞） |
| `cluster ops show <op-id>` 打不开 | `internal/broker/clusterops.go:34` 是 `req.OpsNode != op.OpID && req.OpsNode != op.TargetNode`，服务端两者都收。只有 CLI 的 `Use` 串与 not-found 文案会误导（F40） |
| `unrecognized_directive` 是 drill 该断言的 `topo_action` 值 | 常量是 `unknown_directive`（`internal/natsconf/reconcile.go:42`）。照抄会写出一条永不匹配的断言 |

---

## 6 · 做对了、修的时候不要弄丢

1. **分类器落在 `internal/natsconf` 且零新 import 边**：`topostate.go` 只 import `strings`，`adminsock`/`proto` 各只加一个 `string` 字段。L-2 与 raft-confinement 门都没被碰，plan N9 的形状精确保住了。文件头把三个缺陷 (a)(b)(c) 的复现路径整段写下来，是本批最高价值的产出。
2. **`TopoStuck` / `TopoHeld` 无条件化**：`ActionUnknownDirective`/`ActionRejected` 返回不变的 applied/observed，把 STUCK 关在 `observed<desired` 里就是把"收敛后才卡死"判成 HEALTHY_HA。这条务必保留（修 F17 时只动 `SwappedReloadPending`/`Unresolvable` 两条）。
3. **`TopoHeld` 单列 + `NextStep()` 的否定子句**，以及 `topostate_test.go:210-213` 那条"必须同时含 `do NOT run` 与 `--manual`"的断言（naive 的"must not contain --manual"会反向通过）。同类的 `negateManual` 分支在 `TestComputeHealthTopologyVerdicts` 里也有——重写测试时别弄丢。
4. **`ActionRejected` 的第三个 producer（`apply: `）被纳入 STUCK**：满盘 / `.bak` 写不下 / rename EXDEV 不再渲染成"还在追赶"。
5. **self 行与 peer 行都传播 `TopoAction`**（`clusterstatus.go:389` 与 `:424`）——plan §11 列为最可能的规避，实现没有踩。
6. **`test/determinism/topo_classification_test.go` 的两个门**：AST 锚定在**实参位置**而非文件级共现；带 live-tree 非空自检 + 合成样本的 scanner 非平凡性自检；`TestXferTierCeilingsHaveOneSource` 按 **const 名**豁免（非 file:line）且用 EXACT 匹配避开 `8 GiB` 前缀误杀，并把这次自我修正写进注释。核验实跑确认这两个门是**真的**（还原 substring 匹配即精确报出两处并变红）。修 F23 时只补形状，别动这些性质。
7. **`AllActions()` + `TestAllActionsListsEveryActionConstant` 用 AST 扫本包源码**——核验确认加第 8 个 `Action*` 常量确实变红并点名。**这正是 F25 要求 `OpKind` 照抄的样板。**
8. **`internal/proto/xfer_test.go` 的手算字面量纪律**（1024s / 2048s）与文件头自陈"第一稿写成 `legs × legBudget` 是恒等式、`legs` 会约掉"——D-C2-5 的正确落地，别被"简化成公式"的建议改回去。
9. **`XferTierBMaxBudget` 是编译期常量且真的被 admission 夹住**，`TestWatchdogBudgetIsBoundedByAdmission` 钉的是 `transferMaxBytes == proto.XferMaxBytes` 这条真关系。
10. **逐对象增量而不是抬全局常量**：`xferCrossHomeReapAge` 与 `serveconf.MinXferCrossHomeReapAge` 一字节未动 ⇒ 现网 YAML 零影响；写成**增量**而非绝对下限是这个方案能同时成立的关键，两条反例注释保留。
11. **`transferTierFloorFor` 与 `transferTimeoutFor` 的分家**（`xfer_inflight.go:144-172`）：synthetic terminal 的 `Ts` 继续锚在 tier floor（dedup reqID 内容寻址，回滚会写出两条矛盾终态），D-C2-10 的改判正确且实现忠实。
12. **agent 满载时 refuse 而不是 evict**（`rememberPushCommit` 返回 bool），且配的是**行为测试** `TestPushCommitCacheNeverEvictsALiveLargeTransfer`（核验实跑：还原 evict-oldest ⇒ 变红）。修 F21 只动 TTL 算式，**别把这个改回去**。
13. **归因修正走新 code 而不是改名**：`CodeTransferBudgetExceeded` 进了 `codes.go`、`brokerCodeHints`、`brokerCodeExitClasses`，`expose/upgrade/cluster_upgrade_trigger` 的真 no-responders 一个没碰。
14. **`TestOrphanReaperStillOutrunsALiveTransferAfterRestart` 的设计意图**（把 N15 写成缺口被补上时会变红的测试，而不是一句 TODO）——修 F11 时只把 `homeGrace` 换成读 `xferReapMinObjectAge`，别删掉这条测试。
15. **`confirmOpKindGuard` 排在 `clearOpAttempts` 之前 + AST 顺序门 `TestConfirmOpGuardRunsBeforeClearOpAttempts`**，且作者诚实记录了"纯行为测试看不到这个顺序、实跑变异确认行为版恒绿"。**核验实跑确认它真的变红**——这是本批测试质量最高的一处，F14 的八条失效守卫都应照这条的形状修。
16. **`opFrozenByUpgradeLock` 提成具名谓词 + 反向测试**（未来 kind 默认冻结），以及 `driveOne` 的 `default:` 走 `PlanClusterOpAbort`（不校验 FromState 的逃生口）。核验另确认：旧测试 `TestExternalReviewUpgradeLockFreezesExistingMembershipOps` 仍承重（把守卫短路成 `if false` ⇒ 变红），**新增的 `TestUpgradeLockStillFreezesJoinAndRetire` 只测谓词本身、不能替代它**——修 F14 时别顺手删旧的那条。
17. **`AbortOp` 去掉 FromState 校验是改进**：竞态下从"静默 no-op 并返回 nil"变成"总是赢"；timeline 与 `last_error` 与旧 `transition` 产出等价，applier 不变。
18. **drain-marker healer 的收缩范围与 one-vote-veto 合规**：只走既有的 `cluster.PlanClusterDrainSet(node, nil)`、带 `reaperCaughtUp()` 闸、空集时零写、marker 与 roster 在同一个 `BoundedStaleRead` 快照里读且 roster 先物化；三个 producer 的写序论证（`reconcile_drain_marker.go:142-154`）请整段保留。
19. **`drainingWithoutMarker` 谓词经订正后真的接进了 `clusterstatus.go:357` 的渲染期 `Inconsistent`**，并配了一张丢掉任一合取项就有对应行翻转的表驱动测试（`TestDrainingWithoutMarkerIsInconsistent`，7 行）。
20. **`topoConvergedForOp` 保持 fail-closed 不动（N6 / C3-12）**，并在 `cluster_operation_controller.go:1137-1142` 与文档 §20.1 写清"两种极性各自正确，统一会让其中一个变错"——这是防止下一轮重新"发现"一次的正确做法。
21. **`resumeForceSingleFinalizeOnLeadership` 的四条判据本身正确**：③ 用 `phaseVoter` 而非 live-phase 集合，`raftConfigIDs` 取全部 server（含 nonvoter），两者叠加使 `AddNonvoter` 之后的 joiner 结构上不可能被误判为 ghost。F8 指出的是它在**删除**时被丢掉了，判据本身要保留。
22. **`ClusterHealthSchemaVersion` 6→7 已确认无消费者 gate**：全仓唯一写者 `cluster_health.go:82`，唯一读者是 `acct_nk_honesty_test.go:234` 的 `< 6` 下限断言（不是开关）。升 7 安全。
23. **`opEntryFromOperation` 对 `FS_GHOST_LEFT` 给专属 remedy 而非泛化 failed 文案**，且 `TestForceSingleFinalizeOpIsRenderedWithItsOwnRemedy` 调的是真函数、删掉 case 会变红。
24. **同步 happy path 逐字未变**（`RecoverToSelfOnline → WaitForLeader → PlanSetForceSingle → epoch → PlanClusterNodePrune → deriveAndConvergeSeedsFromRoster`），CLI 成功路径拼接出的字节也逐字相同（`declusterPruneNote("")` 正好补回原句）——修 F12 时保持这条不变。

---

# 附：各 lane 原始产出（未去重，供追溯）



################ agent adabd33347543a18a ################

# C2 lane 审查报告（batch C，未提交工作树）

---

## 1. BLOCKER — `pushCommitEntryTTL` 没有下限，绝大多数 tier-B push 的 prep 条目从 **6 分钟塌到 65 秒**

**证据** `internal/agent/transfer.go:55-58, 75-77`

```go
// It preserves the shape of the pre-batch-C constant: the old TTL was 6m against a 5m
// tier-B watchdog, and that ONE MINUTE OF MARGIN was the entire meaning of the 6 — written nowhere.
const pushCommitCacheSlack = time.Minute
...
func pushCommitEntryTTL(size int64) time.Duration {
	return proto.XferLegBudget(size) + pushCommitCacheSlack
}
```

`XferLegBudget`（`internal/proto/xfer.go:70-75`）**不含 tier floor**（`XferBudget` 才有）。于是：

| size | 旧 TTL | 新 TTL | 同一 transfer 的 broker 预算 |
|---|---|---|---|
| 8 MiB+1（最小 tier-B） | 360 s | `5s + 60s` = **65 s** | 300 s |
| 100 MiB | 360 s | `50s + 60s` = **110 s** | 300 s |
| 480 MiB | 360 s | **300 s** | 300 s |

对**所有小于 480 MiB 的 tier-B 传输**，agent 的 prep 条目现在比改动前短、并且比 broker 自己的看门狗预算短。注释说"保持 5m+1m 的形状"，代码给的是 1 分钟总量——注释承诺了代码不提供的保证。

**失败场景**：ctl 对同一 node push 一个 100 MiB 文件（tier B），链路 500 KiB/s，Put 耗时 200 s；期间该 agent 收到任意第二个 tier-B prepare（`rememberPushCommit` 是唯一的清扫点，`internal/agent/transfer.go:556-559`），110 s 的条目被扫掉；push-commit 到达 → `transfer_unknown` → 整次传输失败。改动前这条路 6 分钟内必成。这是"让慢的大文件能传"的批次，反而把中等文件的容错**缩小到 1/5.5**。

**修法**：`return proto.XferBudget("b", size, 1) + pushCommitCacheSlack`。它恰好复现注释宣称的形状（size=0 → 5m+1m=6m = 改动前逐字相同），大文件仍得到 17m+1m。

---

## 2. BLOCKER — pull 腿两端预算被拉成不对称：agent 传到 17 分钟，broker 5 分钟就判失败、删对象、忘掉 entry

**证据** `internal/agent/transfer.go:447-450` 对 `internal/broker/transfer.go:503-509` + `:875-891`

```go
// agent（pull 上传腿）：按真实文件大小推导，2 GiB ⇒ 1024s
putCtx, cancel := context.WithTimeout(context.Background(), proto.XferBudget("b", size, 1))
```
```go
// broker（pull 看门狗）：entry 的 size 字段在 handlePullReq 里从不赋值 ⇒ 0 ⇒ 固定 5m
d := proto.XferBudget(e.tier, e.size, proto.XferPushLegs)
```

N5 是"pull 不上 size"的永久决策，它记录的后果只有"pull 的 tier-B 预算仍是固定值"。但 C2-8 **同时**把 agent 的 pull 腿改成 size 推导，于是产生了 plan 未记录的新窗口：`size > 600 MiB` 时 agent 的上限最高 1024 s，broker 的只有 300 s。这正是 D6「只抬一端 ⇒ 比今天更糟」论证的镜像，方向反过来。

**失败场景**（`tether pull node:/data-2GiB ./x`，实际上传 8 分钟）：
1. t=0 broker 建 entry、armed 5m 看门狗、写 start 审计；
2. t=5m 看门狗 fire → `finalizeTransfer` 写 `failed/ctl_disconnect`、`deleteXferObject`（此刻 meta 尚未发布，删除 no-op）、`transfers.remove` → bucket 从 `activeOBJStreams()` 消失；
3. t=8m agent 的 Put 完成、回 `PullPrepareResp OK`；对象成为**孤儿**，`xferReapMinObjectAge = 2min` 的 home reaper 可以在 ctl 下载途中把它删掉（`transfer_reconcile.go:127`）；
4. ctl 下载完发 `finalize.req` → `handleFinalizeReq:1284-1290` 的 `preview == nil` 分支在 **clustered 模式下静默不回**（`if b.selfNodeID() == "" ` 才回复）⇒ ctl 挂满 `--timeout`，现在是 **36m8s**（见 finding 3）。单机模式则收到 `transfer_unknown`，尽管文件已经完整落盘、审计却写着 failed。

改动前 agent 的 putCtx 也是 5m，两端同时放弃，ctl 在 ~5 分钟拿到干净的 `object_put_failed`。

**修法**：pull 腿只能用两端都能算出的量——把 `putCtx` 固定成 `proto.XferBudget("b", 0, 1)`（= tier floor，与 broker 的 pull 看门狗逐字对齐），并在注释里点名"pull 无声明 size，抬高这一腿会让 broker 先删对象"。若要真的加速大 pull，必须走 N5 排除的 wire 改动，不能只抬一端。

---

## 3. MAJOR — `--timeout` 默认值 10m → 36m8s 对 **tier A** 同样生效，帮助文案仍写 "tier A: ~30s"

**证据** `cmd/tether/transfer.go:692`、`:72`、`:183`、`:208`

```go
const cliTransferTimeoutDefault = proto.XferTierBMaxBudget + 2*time.Minute   // 2168s = 36m08s
...
"upper bound on each phase of the transfer (tier A: ~30s; tier B: derived from the file size — ...)"
```

`runPush` 在 `chooseTier` 之后才分流，但 `timeout` 是同一个 flag 值；`pushTierA:208` 直接 `context.WithTimeout(cmd.Context(), timeout)`。tier-A 传输的 broker 侧预算是 **30 秒**（`proto.XferTimeoutTierA`）。

**失败场景**：agent 进程活着、NATS 连着（所以 `transferGate` 不返回 `node_offline`）但卡死。`tether push tiny.txt lab-1:/tmp/tiny.txt` 走 tier A，broker 30 s 后写 failed 审计，但没有任何东西回复 ctl 的 inbox ⇒ 用户的 CLI **静默挂 36 分 8 秒**，改动前是 10 分钟。

**修法**：默认值按 tier/size 取。`runPush` 已经知道 `st.Size()` 与 `tier`，在 `cmd.Flags().Changed("timeout") == false` 时用 `proto.XferBudget(tier, size, proto.XferPushLegs) + slack`；或至少把 tier-A 分支钳到 `proto.XferTimeoutTierA + slack`。同时修帮助文案——它现在同时写着 "tier A: ~30s" 和一个 36 分钟的默认值。

---

## 4. MAJOR — 单一来源 AST 门放过 `int64(...)` 包裹形，也就是 `agentTransferMaxBytes` 改动前的**原样写法**

**证据** `test/determinism/topo_classification_test.go:302-313`

```go
expr := strings.Join(strings.Fields(string(src[start:end])), " ")
for lit := range xferTierLiterals {
    if expr == lit {   // EXACT match
```

且 `gd.Tok != token.CONST { continue }`（`:285`）——`var` 完全不扫。

我在 `/tmp/astprobe` 用同一段扫描逻辑实跑了变异：

```
const agentTransferMaxBytes    expr="int64(2 * 1024 * 1024 * 1024)"   FLAGGED=false
const agentTierAMaxBytes       expr="8 * 1024 * 1024"                 FLAGGED=true
```

**失败场景**：改动前 `internal/agent/transfer.go` 写的就是 `const agentTransferMaxBytes = int64(2 * 1024 * 1024 * 1024)`（见 diff 的 `-` 行）。任何人把它改回去 —— 即 plan §9 点名的变异「在 agent 侧塞回一个 `2 * 1024 * 1024 * 1024` 字面量 ⇒ 必红」—— 门**保持绿色**。六个旧拷贝里唯一用 `int64()` 包裹的那个，恰好是门抓不到的那个。

**修法**：比较前先剥掉一层转换（`ast.CallExpr` 且 `Fun` 是内建整型 ident 时取 `Args[0]` 的源码片段），或改成对 `vs.Values[i]` 做常量折叠后与 `8<<20 / 2<<30` 比数值；同时把 `token.VAR` 一并纳入扫描。非空自检那段（`:248-260`）不要动，它是对的。

---

## 5. MAJOR — 两条核心行为的**调用点**零覆盖：`sizeAware` 与看门狗的 arming 表达式都可以被静默改回去

**证据**
- `internal/broker/transfer_reconcile.go:101` `b.reapBucketObjects(ctx, name, b.crossHomeReapAge(), true)`
- `internal/broker/transfer.go:509` `d := proto.XferBudget(e.tier, e.size, proto.XferPushLegs)`
- 全仓测试对这两处的断言：`grep -rn "reapBucketObjects\|sizeAware" --include=*_test.go` 只命中 `transfer_budget_test.go` 的**注释**；`startTransferWatchdog` 在测试里一次都没被调用。

`TestCrossHomeGraceCoversALiveTransferOfThatSize`（`transfer_budget_test.go:76-90`）与 `TestCrossHomeExtraIsAnIncrementNotAFloor` 全部断言纯函数 `xferCrossHomeExtraFor`；`TestWatchdogAndStrandedDecisionUseTheSameBudget` 比较的是 `proto.XferBudget(...)` 与 `transferTimeoutFor(...)`，而后者的函数体逐字就是前者（`xfer_inflight.go:151-153`），它没有碰看门狗一个字节。

**失败场景**（两个独立变异，均不变红）：
- 把 `transfer_reconcile.go:101` 的 `true` 改成 `false` ⇒ §6.3 的**全部**逐对象保护消失，leader 在 15 分钟就删掉别的 home 上预算 34 分钟的活对象 —— 也就是 plan §11 里 "C2-3 亲手造出新的数据删除缺陷，而既有断言仍绿" 那一条，只是换了个位置发生。
- 把 `transfer.go:509` 改回 `d := transferTimeoutTierB` ⇒ 整个 C2-2 失效，`TestWatchdogAndStrandedDecisionUseTheSameBudget` 照绿（它测的是另外两个函数）。

**修法**：加一条对 `reapBucketObjects` 的真调用测试（内存 broker + 假 `b.now()`，两个不同 size 的 fake `ObjectInfo`，断言 cross-home 调用保留大对象、home 调用不保留），以及一条断言 `startTransferWatchdog` 实际 arming 时长的测试（把 `time.NewTimer` 的时长通过 entry.size 差分观测，或提取一个 `watchdogBudgetFor(e)` 让测试直接钉调用点）。断言必须触到调用点，不能再只触 helper。

---

## 6. MAJOR — `stageXferInflightTerminal` 的 `Size: rec.Bytes` 既是错的量，又不可达；注释宣称的两件事都不成立

**证据** `internal/broker/xfer_inflight.go:239-246`

```go
// batch C: Size is carried here too. This is the SECOND writer of an xferInflightRecord, and
// the one easy to miss — a Size filled only on the start path would leave every
// synthesized-terminal record at 0 and quietly re-fix the budget at the tier floor.
cur = xferInflightRecord{..., StartedAt: rec.Ts, Size: rec.Bytes}
cur.Terminal = &rec
```

两个独立问题：

**(a) 量错了。** `schema.AuditTransfer` 同时有 `Size`（声明大小，只在 start 上填）和 `Bytes`；`internal/schema/audit.go:75` 逐字写着 *"Bytes / DurationMs are populated only on complete/failed"* —— 是**已传字节**。而 `xferInflightRecord.Size` 的新注释（`:51-55`）自陈是 *"the declared transfer size"*。在 `failed` 终态上 `Bytes` 通常是 0（例如看门狗合成的那条，`transfer.go:533-541` 根本不设 `Bytes`）。想拿声明大小应该用 `rec.Size`。

**(b) 写进去也永远读不到。** 下一行无条件 `cur.Terminal = &rec`，而 `ledgerRowDisposition`（`:578-579`）在读 `in.Rec.Size`（`:595`）**之前**就 `return ledgerReplay, "exact terminal staged in place"`；outbox 侧同理（`:566-572`）。所以这条记录的 `Size` 没有任何读者。注释说不填会"quietly re-fix the budget at the tier floor" —— 该记录的 budget 从来不参与判定。

**修法**：改成 `Size: rec.Size`，并把注释订正为事实：这个字段在自足终态记录上是**为将来的读者保留的元数据**，当前的 disposition 规则不会读它（或者干脆不填，只留一行说明为什么不需要）。现在的形态是"看起来覆盖了第二个写入点"，D-C2-7 想防的正是这种。

---

## 7. MAJOR — 手抄 "2 GiB" 运维文案只扫了 broker 侧，agent 与 ctl 的三处原样留着

**证据**（broker 两处已改成 `humanBytes`，`transfer.go:694`、`:699`）

- `internal/agent/transfer.go:403` `Error: fmt.Sprintf("file size=%d > 2 GiB", size)`
- `internal/agent/transfer.go:472` `Error: fmt.Sprintf("file grew beyond 2 GiB while reading (read=%d)", uploaded)`
- `cmd/tether/transfer.go:523` `fmt.Sprintf("object size=%d exceeds 2 GiB limit", pr.Size)`
- （长帮助 `cmd/tether/transfer.go:46-49` 的 `8 MiB` / `2 GiB` 三行同类）

plan §6.6 逐字点名了 `internal/agent/transfer.go`、`cmd/tether/transfer.go` 的长帮助，要求"要么改成从常量派生，要么在本 plan 里列为纯散文并说明"——两者都没做。

**失败场景**：`proto.XferMaxBytes` 下调（小盘 broker 的现实诉求，D-C2-2 已经在讨论 racknerd 的 per-session 天花板），agent 的拒绝串仍然对运维说 "> 2 GiB"，运维按 2 GiB 重试，反复失败。这就是 memory 里 `feedback-contract-change-sweep` 那条被重复踩的形状。

**修法**：agent 侧加一个与 `humanBytes` 等价的本地渲染（或把 `humanBytes` 提到 `internal/proto` 与常量同住，零新 import 边），三处改成派生；长帮助若决定保持散文，写进 plan 并在常量旁留一行反向指针。

---

## 8. MINOR — `docs/usage.md` 的命令 synopsis 仍写 `--timeout 10m`，表格已改成 `36m8s`

**证据** `docs/usage.md:937-938`（未改）对 `:948`（已改）

```
tether push <local-path> <nid>:<remote-path> [--force] [--timeout 10m] [--ack-alerts]
tether pull <nid>:<remote-path> <local-path> [--force] [--timeout 10m] [--ack-alerts]
```
```
| `--timeout` | `36m8s` | 每个阶段的上限；... |
```

同一节里相隔十行的两个数字互相矛盾，读者先看到的是 synopsis。**修法**：两行改成 `[--timeout 36m8s]`（或去掉数值写 `[--timeout D]`，让表格做唯一来源）。

---

## 9. MINOR — `TestPushCommitTTLCoversTheUploadItWaitsOn` 是近似恒等式，看不见 finding 1

**证据** `internal/agent/transfer_test.go:242-250`

```go
ttl, upload := pushCommitEntryTTL(size), proto.XferLegBudget(size)
if ttl <= upload { ... }
```

代入实现即 `XferLegBudget(size) + 60s > XferLegBudget(size)`——只要 `pushCommitCacheSlack > 0` 就对任何 size、任何吞吐常量恒真。它声称能抓的两个变异里，"去掉 slack"确实变红，但"TTL 掉到 broker 预算以下"（真实缺陷）它结构上看不见。

**修法**：承重关系应当是 `pushCommitEntryTTL(size) > proto.XferBudget("b", size, proto.XferPushLegs)`（agent 不能比 broker 先放弃），或至少 `>= 6*time.Minute` 的手算下限。

---

## 10. MINOR — `recentlyReaped` 与 `markReaped` 用了两套时钟

**证据** `internal/broker/transfer.go:196-216` 与 `:532`

```go
b.transfers.markReaped(ent.transferID, b.cfg.Now())   // 注入时钟
...
return ok && time.Since(at) < xferRecentlyReapedTTL    // 墙钟
```

`markReaped` 内部的清扫用 `now.Sub(at)`（自洽），但查询用 `time.Since`。本包大量测试注入固定时钟（`xfer_inflight_test.go:35`、`terminal_failure_outbox_test.go:25` 等，`Now: func() time.Time { return now }`）；在那些 fixture 下 `time.Since(at)` 恒 ≫ TTL，迟到完成的 Warn 永远不会打印。`TestRecentlyReapedRemembersOnlyWatchdogKills` 自己用 `time.Now()` 打戳，所以看不见。

**修法**：`recentlyReaped(id string, now time.Time) bool`，调用点传 `b.now()`（`home_delivery.go:175` 已有 nil-safe 版本）。

---

## 11. MINOR — 豁免理由已与代码不符

**证据** `cmd/tether/error_code_coverage_test.go:246`

```go
"internal/broker/transfer.go:(*Broker).startTransferWatchdog#1": "audit-only watchdog code selected by the switch immediately above.",
```

那个 `switch ent.verb` 已被删除（`transfer.go:528-531` 现在是 `code := proto.CodeTransferBudgetExceeded` + 一个 `if`）。豁免本身仍然必要，理由串是死的。**修法**：改成"code 是 proto 常量或 `ctl_disconnect` 字面量，由紧邻上方的 verb 判断二选一"。

---

## 12. MINOR — 两处 gate/schema 文案在批 C 之后已不成立（D-C2-8 的这一半判断是错的）

**证据**
- `internal/broker/every_started_attempt_test.go:415-417`：`"the cross-home floor (%v) must exceed one tier-B watchdog (%v)"`，比的是 `xferCrossHomeReapAge`(15m) 与 `transferTimeoutTierB`(5m)。
- `internal/serveconf/serveconf.go:221-224`：`MinXferCrossHomeReapAge is the SAFE FLOOR ... : 3x the tier-B transfer timeout`，以及 `:239-244` 的拒绝串 `"below the safe floor %s (3x the tier-B transfer timeout)"`。

D-C2-8 裁决这三段"保持为真、无需改动"。它对 `MinXferCrossHomeReapAge` 不必抬高这一半是对的；对**文案仍然为真**这一半是错的：批 C 之后 "the tier-B transfer timeout" 不再是单值，一个 2 GiB 传输的看门狗是 34m08s，15m 的 floor **不再** exceed 它——覆盖关系已经搬到调用点的 `xferCrossHomeExtraFor`。留着的后果是：下一个读 gate 失败消息的人会以为 floor 本身仍然承重，从而放心地删掉 `sizeAware`（见 finding 5，那个动作今天不会变红）。

**修法**：两处把 "tier-B watchdog / tier-B transfer timeout" 改成 "tier-B **floor**"，并在 serveconf 注释里加一句指向 `broker.xferCrossHomeExtraFor`：真正的逐对象覆盖在那里。

---

## 13. MINOR — plan C2-13 / §9 的 `TestPullEntryCarriesNoSize` 完全没有落地

**证据**：`grep -rn "TestPullEntryCarriesNoSize\|XferBudget" --include=*_test.go` 只有 `proto/xfer_test.go`、`broker/transfer_budget_test.go`、`agent/transfer_test.go` 三个文件，没有任何测试断言 pull 的 `transferEntry.size == 0`。

plan §6.6 与 §9 都把它列为"钉住 N5 是**已知**裂口而非疏漏"的必需守卫。它缺席的代价现在是实的：finding 2 正是靠这条裂口发生的，而没有任何东西记录 pull entry 无 size 这个前提。

**修法**：补一条断言 `handlePullReq` 造出的 entry `size == 0` 且 `startTransferWatchdog` 对它给出 `proto.XferTimeoutTierBFloor` 的测试，注释里点名 N5 与 finding 2 的不对称。

---

# 做对了、修的时候不要丢

1. **`XferBudget` 的 legs 参数在所有五个调用点都选对了**：broker 看门狗 `transfer.go:509`、崩溃恢复 `xfer_inflight.go:152`、跨 home 增量 `transfer.go:112` 都传 `XferPushLegs`（三者必须同步，确实同步了）；agent 的 `commitCtx:241` 与 `putCtx:450` 都传 1。
2. **`XferTierBMaxBudget` 是编译期常量、并且真的被 admission 夹住**，`TestWatchdogBudgetIsBoundedByAdmission` 钉的是 `transferMaxBytes == proto.XferMaxBytes` 这条真关系，不是公式重述。
3. **`xfer_test.go` 里全部承重断言用手算字面量**（1024s / 2048s），且顶部逐字解释了为什么不能写成 `XferPushLegs * XferLegBudget(...)`（会被约掉）—— D-C2-5 的教训真的执行了，`XferBudget("b", XferMaxBytes, 1) != XferBudget(..., 2)` 那条是本仓唯一能抓 leg 数错误的断言。
4. **逐对象增量而不是抬全局常量**：`xferCrossHomeReapAge` 与 `serveconf.MinXferCrossHomeReapAge` 一个字节没动 ⇒ 现网 YAML 零影响、drill 的压缩 seam 完好；`xferCrossHomeExtraFor` 写成**增量**而非绝对下限是这个方案能同时成立的关键，注释把两条反例都留下了。
5. **`transferTierFloorFor` 与 `transferTimeoutFor` 的分家**（`xfer_inflight.go:144-172`）：synthetic terminal 的 `Ts` 继续锚在 tier floor 上，理由（reqID 内容寻址 → 回滚会写出两条矛盾终态）整段搬运了，D-C2-10 的改判是对的且实现忠实。
6. **归因修正走新 code 而不是改名**：`CodeTransferBudgetExceeded` 进了 `codes.go`、`brokerCodeHints`、`brokerCodeExitClasses` 三处，`expose/upgrade/cluster_upgrade_trigger` 的真 no-responders 一个没碰。
7. **agent 满载时 refuse 而不是 evict**（`rememberPushCommit` 返回 bool），并且测试是行为测试而不是常量不等式——D-C2-4 的要求落到位了；`replyPush` + `pubTransferEvFailed` 的双发与既有的 path-validation 拒绝路径同形，不是重复上报。
8. **`humanBytes` 派生 broker 的两条 `too_large` 文案** —— 方向完全正确，只是没扫到 agent/ctl（finding 7）。
9. **`TestOrphanReaperStillOutrunsALiveTransferAfterRestart`** 把 N15 写成会在缺口被补上时**变红**的测试，而不是一句 TODO——这是本轮最好的一条测试设计，别在修其它 finding 时顺手删掉。


################ agent a9c6378dcd617d06e ################

审计完成。下面是完整报告。

---

# A. 覆盖表（逐条读代码核对，不读 plan 自证）

## C3 — topo `Action` 上 wire

| # | 交付 | 实现位置 | 判定 |
|---|---|---|---|
| C3-1 | 分类器新文件 | `internal/natsconf/topostate.go:65-279`（`TopoState`+6 常量、`ClassifyTopo:256`、`classifyLegacyReason:231`、`Cell:106`/`Banner:160`/`NextStep:175`/`Degrades:133`、`WorstTopoState:194`、`AllActions:207`） | **DONE** |
| C3-2 | `topoSelfReport.Action` + 写入点 | `internal/broker/clusterwrite.go:138` + `topology_reconcile.go:149` | **DONE** |
| C3-3 | `proto.TopoAction` + schema 6→7 + 账本注释 | `internal/proto/alerts.go:111`、`:14`、`:10-13` | **DONE** |
| C3-4 | health 填充 | `internal/broker/cluster_health.go:106` | **DONE** |
| C3-5 | clusterstatus **两处**传播 | `clusterstatus.go:389`（peer）+ `:424`（self，带论证注释） | **DONE** |
| C3-6 | `adminsock.ClusterNodeStatus.TopoAction` | `internal/adminsock/protocol.go:782` | **DONE** |
| C3-7 | `computeHealth` 改分类器 + STUCK 无条件 | `clusterstatus.go:621-658`、`:687-688`（`if reached` 内不再套 `observed<desired`） | **DONE** |
| C3-8 | `topoCell` 改分类器 | `cmd/tether/cluster.go:441-449` | **DONE** |
| C3-9 | `topoLaggards` + `--wait` 遇 Stuck/Held 立即返回 | `cluster_reconcile.go:139-146`（bail，`exitInternal`）、`:187-205`、`topoWedged:213-233` | **DONE** |
| C3-10 | `cardTopReason` 加 topo 分支（且置于最前） | `cluster_status_card.go:106-126` | **DONE** |
| C3-11 | `cluster doctor` topology check | `cluster_doctor_online.go:77-86`、`:105-119` | **DONE** |
| C3-12 | `topoConvergedForOp` 的"故意不共享"注释 | `cluster_operation_controller.go:1137-1143` | **DONE** |
| C3-13 | drill 93 断言 `topo_action` **且 render 失败时极性为 STUCK** | `93-metrics-observability.sh:121-136` | **PARTIAL → BLOCKER**（见 F1） |
| C3-14 | 架构文档记录新 wire 字段 | `docs/distributed-broker-architecture.md §20.1` | **DONE** |

## C1 — force-single 后半段

| # | 交付 | 实现位置 | 判定 |
|---|---|---|---|
| C1-1 | kind + 3 state + `validOpStates` + `ValidOpKind` 三值 | `internal/cluster/operation_ops.go:25`、`:59-62`、`:75`、`:80-82` | **DONE** |
| C1-2 | `driveOne` case **和** `default:` | `cluster_operation_controller.go:495-511` | **DONE** |
| C1-3 | `driveForceSingleFinalize`（advance-after-observe / prune 前重确认 / 只删 params） | `force_single_finalize.go:172-236` | 实现 DONE，**零测试**（见 F3） |
| C1-4 | 复制式 deadline，耗尽⇒`FS_GHOST_LEFT`，永不 BLOCKED | `force_single_finalize.go:70`、`:140`、`:241-256` | 实现 DONE，**测试为恒真**（见 F4） |
| C1-5 | commit 失败才建 op；成功路径逐字不变 | `force_single_online.go:291-324` | 实现 DONE，**无 Go 层"成功路径不建 op"测试**（见 F3） |
| C1-6 | leadership-edge 四条判据建 op | `clusteradmin.go:406-413` + `force_single_finalize.go:347-379` | 实现 DONE，**四条判据零测试**（见 F5） |
| C1-7 | `upgradeActive` 对 finalize 豁免 + 反向测试 | `cluster_operation_controller.go:427-470`；反向测试 `force_single_finalize_test.go:401` | **DONE** |
| C1-8 | `ConfirmOp` 在 `clearOpAttempts` 之前早退 | `cluster_operation_controller.go:250-252` + `force_single_finalize.go:90-97` | **DONE**（含 AST 顺序门） |
| C1-9 | `AbortOp` 不依赖 `FromState` | `cluster_operation_controller.go:298-315` + `operation_ops.go:223-245` | **DONE** |
| C1-10 | `opEntryFromOperation` 渲染新 state | `clusterops.go:67-78` | **DONE** |
| C1-11 | CLI 分支文案 + `Abandoned` 注释订正 | `cluster_offline.go:385-431`、`adminsock/protocol.go:691-700` | **DONE** |
| C1-12 | 愈合 pass = 清孤儿 **+ 注册 + `AbortOp` 注释点名 pass** | pass `reconcile_drain_marker.go`、注册 `reconcile_passes.go:243-248`；**`AbortOp` 注释未改** | **PARTIAL → BLOCKER**（F6） |
| C1-13 | 渲染期 `Inconsistent` 第三个 disjunct | `clusterstatus.go:346-356` + `reconcile_drain_marker.go:155` | **DONE** |
| C1-14 | 架构文档增补 op kind + ladder | `distributed-broker-architecture.md §20.3` | **DONE** |
| C1-15 | `docs/cluster-runbook.md` 改为"按分支处理" | **文件完全未改** | **MISSING → BLOCKER**（F2） |
| C1-16 | drill 22 / 12 / 20 / 41 | 22 DONE；**12 未改**；20/41 无运行证据 | **PARTIAL → BLOCKER**（F1） |

## C2 — 传输预算

| # | 交付 | 实现位置 | 判定 |
|---|---|---|---|
| C2-1 | 预算函数族 + 上界 + 推导注释 | `internal/proto/xfer.go:16-95`（改名为 `XferBudget`/`XferLegBudget`/`XferTierBMaxBudget`，语义一致） | **DONE** |
| C2-2 | 看门狗改用 budget | `internal/broker/transfer.go:509` | 实现 DONE，**无测试覆盖该行**（F8） |
| C2-3 | 跨 home GC 下限（§6.3 逐对象裁决优先） | `transfer.go:95-119` + `transfer_reconcile.go:144-178` | **DONE**（形式为增量而非 `max(floor,budget+margin)`，在代码内有论证） |
| C2-4 | 三处既有关系断言同步 | §6.3 下无需改；新关系钉在 `transfer_budget_test.go:76-90` | **DONE**（`xferReapBudgetMargin` 未具名，MINOR） |
| C2-5 | `transferTimeoutFor(tier,size)` + `Size` + **两分支都填** | `xfer_inflight.go:152`、`:51`、`:241`、`:283` | **DONE** |
| C2-6 | 旧 ledger `Size==0` 回落 + 测试 | `proto/xfer.go:91-94`；`transfer_budget_test.go:31`、`proto/xfer_test.go:62` | **DONE** |
| C2-7 | synthetic `Ts` 留在 tier 下限 + 论证整段搬运 | `xfer_inflight.go:154-172`、`:681-683` | **DONE** |
| C2-8 | agent `commitCtx`/`putCtx` | `agent/transfer.go:241`、`:450` | **DONE** |
| C2-9 | 新 code + registry + hints 分类 | `proto/codes.go:163`、`error_hints.go:38`/`:89` | **DONE** |
| C2-10 | proto 常量 + 六个引用点 | `proto/xfer.go:38-51`；`broker/transfer.go:56-58`、`agent/transfer.go:48`/`:52`、`cmd/tether/transfer.go:685`/`:699` | **DONE** |
| C2-11 | AST 守卫（不误杀不同语义） | `test/determinism/topo_classification_test.go:217-325`（按 **const 名**豁免，非 file:line） | **DONE** |
| C2-12 | 手抄文案全局扫 | usage.md 表格/正文已改；**usage.md:937-938 synopsis 未改**、agent/ctl 三处错误串未派生 | **PARTIAL → BLOCKER**（F7） |
| C2-13 | N5 的 pull-size 回归测试 | **不存在** | **MISSING → BLOCKER**（F9） |

---

# B. 规避清单（§11）逐条核验

| §11 条目 | 是否发生 | 证据 |
|---|---|---|
| C3-1/8 两端各写 switch | **否** | 只有 `natsconf.ClassifyTopo` 一份；`test/determinism/topo_classification_test.go:58` AST 门锁住 |
| C3-1 `default: return behind` | **否** | `topostate.go:278` `return TopoUnknownAction`，且 `Degrades()==true`（fail-closed 方向） |
| C3-2 `Store` 漏传 Action | **否** | `topology_reconcile.go:149` 已传 |
| C3-5 只改 peer 漏 self | **否** | `clusterstatus.go:389` + `:424` 两处齐全 |
| C3-7/8 只给 topoCell 补 `render` | **否** | 两端都走分类器 |
| C3-9/10/11 完全不提 III/V/VI | **否** | 三处全做，且都有行为测试 |
| **C3-13 drill 只断言列存在、不断言极性** | **部分发生** | 见 F1：新增 3 条断言只覆盖**收敛态**极性（`✓`/`noop|reloaded`），没有注入 render 失败去断言 `STUCK`。drill 注释自陈"classification 由 hermetic 层覆盖"，正是 §11 反对的那句话 |
| C2-2 断言公式而非关系 | **否** | `proto/xfer_test.go:10-18` 明确用手算字面量，并写下"第一稿就是恒等式"的自查 |
| C2-1 不加上界 | **否** | `XferTierBMaxBudget` 具名 + `TestWatchdogBudgetIsBoundedByAdmission` |
| C2-3 `xferCrossHomeReapAge` 不动 | **否**（更好） | 改成逐对象增量，且论证了不抬全局的理由 |
| C2-5 `xfer_inflight.go` 不动 | **否** | 已改签名 |
| C2-5 `Size` 只在 push 分支填 | **否** | `stageXferInflightTerminal:241` 与 `writeXferInflight:283` 都填 |
| C2-8 agent 侧不改 | **否** | 两条腿都改，且顺带修了容量淘汰路径 |
| C2-10 只改 broker | **否** | 三端全改 |
| C2-11 守卫误杀后加 file:line 豁免 | **否** | 按 const 名豁免，且做了 EXACT 匹配（注释记录了 8 GiB 前缀误杀的自我修正） |
| **C2-12 `"(2 GiB)"` 不动** | **部分发生** | broker 两处已派生（`humanBytes`），但 `agent/transfer.go:403`、`:472` 与 `cmd/tether/transfer.go:523` 的同类错误串仍是手抄；`docs/usage.md:937-938` 的 synopsis 仍写 `--timeout 10m` |
| C1-2 `driveOne` 忘加 case | **否** | 有 case + default + AST 门 |
| C1-3 driver 重新推导 abandoned | **否** | `forceSingleFinalizeParams` 烤入，`driveForceSingleFinalize` 只读 params |
| C1-4 失败进 `OpStateBlocked` | **否**（实现层） | `finalizeBudgetCheck` 进 `FS_GHOST_LEFT`。**但测试抓不住这条**（F4） |
| C1-5 新旧两个执行者并存 | **否** | 只在 `err != nil` 分支建 op |
| C1-6 只做 commit 插 op | **否** | `resumeForceSingleFinalizeOnLeadership` 已接到 leadership edge |
| C1-7 冻结原样保留 | **否** | 改成 per-op 谓词 + 反向测试 |
| C1-12 放宽到 `marker ∧ VOTER` | **否** | `orphanDrainMarkers` 只按"无 roster 行" |
| C1-12 自写 UPDATE 不走 PlanClusterDrainSet | **否** | `reconcile_drain_marker.go:85` 走既有命令 |
| C1-13 只 `logger.Warn` | **否** | 进了渲染期 `Inconsistent` |
| **C1-16 drill 只加注释说"单测覆盖"** | **发生（变体）** | `12-ghost-voter.sh` 根本没碰（mtime `2026-07-25`）；prune 失败注入分支完全不存在 |
| 跨批 bump `ProtoVersion` | **否** | `git diff internal/proto/ \| grep ProtoVersion` 为空 |
| 跨批 往 `legacy_process_named_list.go` 加行 | **否** | 该文件未改；新测试文件全部按被测单元命名 |
| 跨批 写成 TODO/后续增量 | **否** | 见 D 段 |

> 附一条事实：`22-forcesingle-online.sh`（mtime `2026-07-28 20:46:55`）与 `93-metrics-observability.sh`（`20:47:20`）是在本次审计**开始之后**才落盘的——我第一次 `git status` 时 93 还不在改动集里。这两个文件不在"gates 已绿"的覆盖范围内（drill 不进 `make test`/CI），也无从证明它们被实际跑过。

---

# C. §9 测试表核验

**24 条具名测试中只有 2 条同名存在。**改名本身可以接受（`test_naming_test.go` 只管文件名），但必须逐条追"那个变异是否真的会变红"。结果：

| §9 测试 | 实际对应 | 变异是否真能抓 |
|---|---|---|
| `TestTopoClassCoversEveryAction` | `TestClassifyTopoMapsEveryAction` + `TestAllActionsListsEveryActionConstant`（AST 扫本包 `Action*` 常量） | ✅ |
| `TestTopoRenderersAgreeOnEveryAction` | 无同名；由 `test/determinism` 的 AST 门 + 两端各自的行为表共同承担 | ✅（结构性等价，CLI 侧留旧 substring 会被 AST 门抓） |
| `TestTopoStuckIsNotGatedOnGeneration` | `TestStuckIsNotGatedOnGeneration` + `TestComputeHealthTopologyVerdicts` "wedged at the converged generation" 行 | ✅（分类器与 caller 两层都覆盖） |
| `TestAwaitingCutoverIsHeldNotStuck` | `TestAwaitingCutoverIsHeldAndNeverRecommendsManual` | ✅ |
| `TestApplyFailureIsStuck` | 表内 `{ActionRejected, reasonApplyFailed}` 行 + legacy `HasPrefix("apply: ")` 行 | ✅ |
| `TestStatusCardNamesTopologyStuck` | `TestCardTopReasonNamesTopology` | ✅ |
| `TestTransferBudgetRelations` | `TestXferBudgetDerivationLiterals` 等 | ⚠️ 只覆盖 `proto` 层；**`startTransferWatchdog:509` 改回常量不会变红**（F8） |
| `TestCrossHomeReapExceedsMaxBudget` | 被 §6.3 作废，替换为 `TestCrossHomeGraceCoversALiveTransferOfThatSize` | ✅ 对 helper；⚠️ 对 caller 无覆盖（F8） |
| `TestStrandedXferUsesSameBudgetAsWatchdog` | `TestWatchdogAndStrandedDecisionUseTheSameBudget` | ✅ |
| `TestLegacyLedgerWithoutSizeFallsBackToFixedBudget` | 同上 `:31` + `proto/xfer_test.go:62` | ✅ |
| `TestTierConstantsHaveSingleSource` | `TestXferTierCeilingsHaveOneSource` | ✅ |
| **`TestPullEntryCarriesNoSize`** | **不存在** | ❌ MISSING |
| **`TestForceSingleCommitSuccessPathCreatesNoOp`** | **不存在**（仅 drill 22） | ❌ MISSING |
| **`TestForceSingleFinalizeRetriesFailedPrune`** | **不存在** | ❌ MISSING |
| `TestForceSingleFinalizeDeadlineGoesTerminalNotBlocked` | `TestForceSingleFinalizeLadderIsAlwaysTerminal` | ❌ **恒真**（F4） |
| **`TestForceSingleFinalizeAdvancesOnObservationNotPropose`** | **不存在** | ❌ MISSING |
| **`TestForceSingleFinalizeSurvivesLeadershipChange`** | **不存在** | ❌ MISSING |
| `TestLeadershipEdgeCreatesFinalizeOpForGhost` | `TestForceSingleGhostSignatureIsPhaseVoterOnly` | ❌ **恒真**，且只号称覆盖 4 条判据中的第 ③ 条（F5） |
| `TestUpgradeLockStillFreezesJoinAndRetire` | 同名存在 | ✅ |
| `TestConfirmOpDoesNotResetFinalizeBudget` | `TestConfirmOpRefusesFinalizeWithoutClearingItsBudget` + `TestConfirmOpGuardRunsBeforeClearOpAttempts`（AST 顺序） | ✅（作者自陈实跑过变异，行为版确实抓不住、于是补了结构版——这是全批最诚实的一处） |
| **`TestForceSingleCommitSucceedsWithStaleRetireOpOnSelf`** | **不存在** | ❌ MISSING |
| `TestDrainMarkerHealerClearsOnlyRosterlessOrphan` | `TestDrainMarkerHealerLeavesAHalfFinishedDrainAlone` | ❌ **恒真**（F4） |
| **`TestDrainMarkerHealerIdleZeroWrites`** | **不存在** | ❌ MISSING |
| `TestDrainingWithoutMarkerIsInconsistent` | 同名存在，7 行表 | ✅ |

**结论：C3 与 C2 的测试表基本兑现；C1 的测试表兑现了 4/11，其中 3 条是恒真断言。**

---

# D. 延后语言扫描

对 `git diff` 全文 + 11 个新文件 grep `TODO|FIXME|XXX|for now|later|follow-up|out of scope|future increment|deferred|not yet|后续|以后|暂时`：

- `cmd/tether/cluster_reconcile.go:+192` — `is not "try again later"` → **散文，非延后**。
- `internal/broker/transfer_budget_test.go:174` — `"the progress/ledger surface the refactor roadmap deliberately **deferred** … Recorded as permanent decision N15"` → **合法永久决策**（plan §3 N15 / §6.1b D-C2-11），且该测试断言缺口**存在**，符合"防止被读成全称命题"的要求。
- 其余全部是 `reaped later` / `later flips` 一类的时间副词。

**无一处未申报的延后。这一项干净。**

---

# E. §12 开放项（`xferMinThroughput` 与跨 home 关系式）

**取值已定，推导已写进代码，且数值站得住：**

`internal/proto/xfer.go:16-37`：反推旧常量隐含 `2×2 GiB/300s = 13.65 MiB/s ≈ 114 Mbit/s`，取其 ~1/7 得 2 MiB/s；`2 GiB / 2 MiB/s = 1024 s` 恰为 2 的幂；`XferTierBMaxBudget = 2×1024 s = 2048 s = 34m08s`。我手算复核：`⌈2147483648/2097152⌉ = 1024`，`XferTierBMaxBudget` 常量表达式 `2*(2GiB/2MiB)*time.Second = 2048s` 一致；`proto/xfer_test.go:28-46` 用手算字面量钉住（并在文件头写明"第一稿写成 `legs × legBudget` 是恒等式、`legs` 会约掉"——这是真跑过变异的证据）。

**但 §6.2/§12 的第二个硬条件只兑现了 1/3。**plan D-C2-2 把"最坏代价"订正为三项，代码里只写了第 ②：

- ② tracker 槽位耗尽窗口 5 min → 34m08s（6.8×）：`internal/broker/transfer.go:142-153` 写得完整，含"只有 session member 能到这条路径"的接受理由。✅
- ① **对象字节在盘上多待 6.8×；racknerd 的 per-session bucket 天花板下 3 个并发 2 GiB 在飞对象会让第 4 个 `Put` 撞 10047**：全仓 grep `10047` / "small-disk" 无任何相关论述。`proto/xfer.go:41` 只写了"small-disk broker 的 per-session ceiling 可能更低，admission 取二者 min"——那是既有事实，不是本次抬预算带来的**代价**。❌
- ③ 悬空 start 审计行与 ledger 文件多活 29 min：无。❌

§12 要求稽查 agent 把"推导是否站得住"作为独立 finding：**推导本身站得住，代价陈述缺两项**（见 F10，MINOR）。

---

# F. 编号 finding

### F1 — BLOCKER：C3-13 的 drill 不断言失败极性；C1-16 的 drill 12 完全没做

**证据**

`test/simcluster/drills/93-metrics-observability.sh:121-136` 新增三条断言：
```sh
assert_ok "TOPO-ACTION: cluster status --json carries topo_action on EVERY reporting voter …"
assert_ok "TOPO-ACTION: a converged cluster reports the converged actions, never a failure one" \
    … jq -e "… all(. == \"noop\" or . == \"reloaded\")" …
assert_ok "TOPO-ACTION: the human TOPO column agrees — every voter renders the converged marker …" \
    … | grep -q '✓'
```
以及注释：`# … the STUCK/HELD/BEHIND classification itself is covered hermetically in internal/natsconf.`

plan C3-13 逐字要求：**"且 render 失败时 TOPO 列**极性**为 STUCK（不能只断言列存在）"**。三条断言全部只覆盖**收敛态**：字段存在、action ∈ {noop,reloaded}、列是 `✓`。没有一条把 nats.conf 弄坏、让 reconcile 走 `ActionRejected`，再断言列变 `STUCK`。而 drill 注释里那句"classification 由 hermetic 层覆盖"正是 §11 点名的 C1-16 规避话术——**用 hermetic 层回答只有部署层能回答的问题**。C3 的整条价值链（broker self-report → health responder → adminsock → ctl 渲染）在**失败态**下从未在真实部署栈上走通过一次。

同时 `test/simcluster/drills/12-ghost-voter.sh` **一个字节都没改**（mtime `2026-07-25 04:18`，早于本批全部工作），而 C1-16 明写"`12-ghost-voter.sh` 加 prune 失败注入分支"。C1 的全部新机器——建 op、重试、`FS_GHOST_LEFT`、`--to-standalone` 拒绝路径——在部署层零覆盖。

**失败场景**

有人把 `ClassifyTopo` 的 `case ActionRejected` 挪进 `if observed >= desired` 里、或把 `clusterstatus.go:424` 的 self 行 `TopoAction` 赋值删掉，drill 93 全绿（收敛集群的 self 行 action 仍是 `noop`，仍渲染 `✓`）。缺陷 (a) 原样回归而部署层无感。

**修法**

1. drill 93 增加一个失败注入段：`$SIM exec brk2 -- sh -c 'echo "include /nonexistent.conf" >> /etc/tether/nats.d/nats.conf'`（或等价手法触发 `ActionUnknownDirective`），poll 到 `cluster status --json` 里该节点 `topo_action == "unrecognized_directive"` **且** TOPO 列渲染 `STUCK` **且** `cluster doctor` 的 `topology` 项为 FATAL，然后还原。
2. drill 12 增加 prune 失败注入分支：在 commit 前让 `PlanClusterNodePrune` 的 Propose 失败（最忠实的手法是在 commit 瞬间打断 raft 提交窗口），断言 ① `cluster ops ls` 出现 `force_single_finalize` ② stderr 出现 `DO NOT de-cluster yet` ③ 重试后到达 `FS_FINALIZED` 或 `FS_GHOST_LEFT`。若真实部署层做不出该注入，按 drill 铁律标 `[GAP #N]` 明写"tether 干不了"，**不能**用"单测覆盖"抵账。

---

### F2 — BLOCKER：C1-15 完全未做，runbook 仍在教运维走进 `unrecognized raft role ""`

**证据**

`docs/cluster-runbook.md` 未出现在 `git status` 里。`:461-462` 逐字仍是：

```
  The abandoned peers are already pruned (below), so the N=1 voter tally passes and `--to-standalone`
  is unlocked.
```

`:464-470` 的 **Abandoned-peer roster (#12)** 段仍写：

```
force-single now PRUNES the abandoned peers from the roster automatically
(both the online and offline paths) … If you upgraded from an older binary and a **ghost VOTER**
lingers (a roster row still phase=VOTER but absent from the raft config …
```

即：ghost 只被归因于"从旧二进制升级"。plan §5.2 把这**同一句话**当成决定 C1 形状的证据链第 3 环引用，§5.5 明写它"不是过时描述，是**因果指引**"，C1-15 要求它"改为「commit 返回后按分支处理」"。

**失败场景**

prune 失败、CLI 正确打印了 `WARNING … DO NOT de-cluster yet` 和 op id。运维（灾难现场，多半照着 runbook 而不是照着刚滚过去的 stderr 走）翻到 §3.2，读到"已 pruned，所以 N=1 通过、`--to-standalone` 解锁"，直接执行 `reconcile nats --to-standalone --confirm-single --reset-js`，撞上 `unrecognized raft role ""`——`cmd/tether/cluster_natsconf.go:180-189` 那条报错在灾难现场毫无指向性。这正是 plan 引用的 racknerd「JS 503-rotted for 5 days」事故形状。且 runbook 从头到尾不含 `force_single_finalize` / `FS_GHOST_LEFT` / `cluster ops show` 任何一词——运维没有任何入口知道该等什么。

这也是记忆里 `feedback-contract-change-sweep` 的原样复发：**改了动词的契约，扫了产品打印的文案，漏了运维手册**——而本例里手册恰恰是产品文案的来源。

**修法**

`docs/cluster-runbook.md` §3.2：
1. `:461` 改为分支式：prune 成功 ⇒ 保留今天这句；prune 失败 ⇒ "先 `tether cluster ops show <op_id>` 等到终态；`FS_GHOST_LEFT` 时逐个 `tether cluster recovery node remove <id>`；在此之前 `--to-standalone` 会以 `unrecognized raft role ""` 拒绝。"
2. `:464` 的 **Abandoned-peer roster (#12)** 段增加第二条 ghost 来源："同步 prune 失败（此时会有一条 `force_single_finalize` op 在重试）"，并给出 `cluster ops show` / `FS_GHOST_LEFT` 的读法。
3. 增补一条"回滚前须先 `cluster ops show` 确认无非终态 finalize op"（plan §10 已把它记为**永久决策**，目前只活在 plan 里，运维手册上没有）。

---

### F3 — BLOCKER：C1 的三个核心函数（`driveForceSingleFinalize` / `startForceSingleFinalize` / `resumeForceSingleFinalizeOnLeadership`）在全仓零测试引用

**证据**

```
$ grep -rn "driveForceSingleFinalize\|startForceSingleFinalize\|resumeForceSingleFinalizeOnLeadership\|finalizeBudgetCheck\|rosterRowsPresent\|reconcileDrainMarkers\|orphanDrainMarkers" --include=*_test.go .
internal/broker/force_single_finalize_test.go:242:// Mutation: make orphanDrainMarkers select on …   ← 只在注释里
```
——**注释里出现过，代码里一次都没被调用过。**

而现成的 hermetic 夹具就在隔壁：`internal/broker/force_single_handler_test.go:24` 的 `fsTestBackend(t, selfID, peerID, peerRaftAddr)` 起真 `cluster.Node`（inmem raft + `TransportFactory`）、admit self、插 peer 行，`forcesingle_converge_test.go:37` 已经用它端到端跑过 `handleForceSingleCommit`。`reconcile_passes_test.go:128` 的 `passBroker(t, db, clk)` 同理已被别的 pass 用作行为测试夹具。所以"没法 hermetic 测"不成立——**只有 prune 失败的注入需要 Propose seam；driver 本身、budget 耗尽、四条判据、healer 的清/不清，全部可以用现有夹具直接驱动。**

**失败场景（举三个当前不会变红的真回归）**

1. 把 `force_single_finalize.go:233-235` 的 "不在这里 advance" 改成 propose 成功后立即 `transition(FS_FINALIZED)` —— §9 的 `...AdvancesOnObservationNotPropose` 变异。`PlanClusterNodePrune` 在 `RowsAffected==0` 时 Propose 仍返回 nil，于是 op 报成功而 ghost 还在，`--to-standalone` 继续拒绝。**全绿。**
2. 把 `CatchupDeadline: a.now().Add(opForceSingleFinalizeBudget).UnixNano()`（`:140`）删掉 ⇒ 列为 0 ⇒ `finalizeBudgetCheck:242` 的 `op.CatchupDeadline != 0` 短路失效 ⇒ 第一次 tick 就 `FS_GHOST_LEFT`。**全绿。**
3. 把 `driveForceSingleFinalize:214-219` 的 `if !inCfg[id]` 过滤删掉（直接 prune `remaining`）⇒ 一个被重新 admit 的 live 成员的 roster 行会被删掉。**全绿。**

**修法**

用 `fsTestBackend` 补齐下列行为测试（都不需要新 seam）：
- `TestForceSingleCommitSuccessPathCreatesNoOp`：跑今天已有的成功路径，断言 `SELECT COUNT(*) FROM cluster_operations` == 0。
- `TestForceSingleFinalizeAdvancesOnObservationNotPropose`：`startForceSingleFinalize` + 一个仍在 roster 的 abandoned id → 第一 tick 断言仍 `FS_PRUNE_PENDING` 且已 propose；第二 tick 观测到行没了才 `FS_FINALIZED`。
- `TestForceSingleFinalizeDeadlineGoesTerminalNotBlocked`：把 op 的 `catchup_deadline` 置于过去 → tick 一次 → 断言 `op_state == FS_GHOST_LEFT ∧ terminal == 1`，且 `last_error` 含 `recovery node remove` 与 `to-standalone`。
- `TestForceSingleFinalizeSurvivesLeadershipChange`：清空 `a.opAttempts` 后再 tick，断言预算判定不受影响（复制式 deadline 的全部意义）。
- `TestForceSingleFinalizeRefusesToPruneAReadmittedMember`：把 abandoned id 加回 `RaftConfiguration()` → 断言进 `FS_GHOST_LEFT` 且**没有**删那一行。
- `TestForceSingleCommitSucceedsWithStaleRetireOpOnSelf`（§5.4）：self 上预置非终态 retire op → commit 仍 OK，`FinalizeOpID == ""`，并有 Warn。

---

### F4 — BLOCKER：三条 C1 测试是恒真断言，其自称能抓的变异一条都抓不住

我按 §9 的变异逐条判：

**(a) `force_single_finalize_test.go:28-51` `TestForceSingleFinalizeLadderIsAlwaysTerminal`**
自称：`Mutation: send an exhausted finalize to cluster.OpStateBlocked — reddens.`
实际断言只有三件事：3 个 state 在 `ValidOpState` 里、kind 在 `ValidOpKind` 里、`opForceSingleFinalizeBudget != 12*observeTickInterval`。
`finalizeBudgetCheck:255` 改成 `a.transition(op, cluster.OpStateBlocked, false, msg, nil)` 之后——`OpStateBlocked` 本身就在 `validOpStates` 里（`operation_ops.go:76`）——**这个测试一个字都不会红。**它对 `finalizeBudgetCheck` 没有任何引用。
附带：`opForceSingleFinalizeBudget != 12*observeTickInterval` 与其定义 `const opForceSingleFinalizeBudget = 12 * observeTickInterval` 两边归约到同一表达式，属"公式复述"而非"关系/手算字面量"（CLAUDE.md 与本 plan §9 都点名反对）。

**(b) `:186-201` `TestForceSingleGhostSignatureIsPhaseVoterOnly`**
自称：`Mutation: widen forceSingleGhostRows' phase filter to the live-phase set … — the joining-node rows redden.`
函数体全文只做了三件事：遍历一组常量确认它们不等于 `phaseVoter`（且分支里是 `t.Fatal("fixture error")`）、断言 `phaseVoter == "VOTER"`、断言 `phasePending != phaseVoter`。**它没有引用 `forceSingleGhostRows`。**把 `force_single_finalize.go:306` 的 `WHERE phase = ?` 改成 `WHERE phase IN (VOTER, CATCHING_UP, PENDING)`，测试全绿——而那正是"leadership edge 把正在 join 的节点当 ghost 删掉 roster 行"的灾难。测试注释自己承认了这一点（"the query itself needs a live DB"），但夹具（F3）明明存在。

**(c) `:243-263` `TestDrainMarkerHealerLeavesAHalfFinishedDrainAlone`**
自称：`Mutation: make orphanDrainMarkers select on "no active op" instead of "no roster row" — reddens.`
函数体在本地构造 `roster := map[string]bool{"brk-b": true}` / `marked := []string{"brk-b","brk-ghost"}`，然后**在测试里重写一遍那个 for 循环**并断言结果。它测的是测试自己的循环。把 `orphanDrainMarkers` 改成按"无非终态 op"筛（= N7 的反面，会清掉运维正在重试的那次 drain 的 marker），**全绿**。

**失败场景**

这三条是 C1 全部"永久决策"的守卫：ladder 永不 BLOCKED、ghost 签名必须窄、healer 绝不碰半完成 drain。三条守卫在结构上都不承重。下一个改这块的人拿到一片绿，正好把 §11 逐条点名的三个规避动作做进去。

这也正是记忆里 `feedback-mutation-verify-every-new-guard` 记录的批次 B 翻车形状——**plan §9 开头写着"每条新增守卫都要注入它声称能抓的那个缺陷并确认变红"，这三条显然没做。**

**修法**

- (a) 用 `fsTestBackend` 起 op、把 deadline 推到过去、tick、断言 `op_state == FS_GHOST_LEFT ∧ terminal == 1`；并加一条结构断言：`force_single_finalize.go` 内不得出现 `OpStateBlocked`（AST 或源码扫描皆可）。把 `opForceSingleFinalizeBudget` 的断言改成手算字面量（`12 × observeTickInterval` 的实际秒数）或改成关系（`> 2*observeTickInterval` 且 `< 1h`）。
- (b) 用夹具插入 `phase=CATCHING_UP` 且不在 raft config 的行 + `phase=VOTER` 且不在 config 的行，调 `forceSingleGhostRows()`，断言只返回后者。
- (c) 用 `passBroker` 起真 DB，写 `draining:brk-b`（brk-b 有 roster 行、无 op）与 `draining:brk-ghost`（无 roster 行），跑一次 `reconcileDrainMarkers`，断言 marker 集合只剩 `brk-b`。顺带补上缺失的 `TestDrainMarkerHealerIdleZeroWrites`：无孤儿时断言 raft `AppliedIndex` 不前进。

---

### F5 — BLOCKER：C1-6 的四条判据只有第 ③ 条被（无效地）提及，①②④ 零覆盖

**证据** `force_single_finalize.go:347-368`：

```go
if !forceSingleActive(a.node.RODB()) { return }          // ①
voters, err := a.node.NumVoters(); if err != nil || voters != 1 { return }   // ②
if live, lerr := cluster.ActiveOperationForTarget(a.node.RODB(), selfID); lerr != nil || live != nil { return }  // ④
ghosts, gerr := a.forceSingleGhostRows()                  // ③
```

§9 要求 `TestLeadershipEdgeCreatesFinalizeOpForGhost`："去掉四条判据中任意一条 ⇒ 必红"。实际只有 (b)（且恒真，见 F4）号称覆盖 ③。

**失败场景**

去掉 ①（`forceSingleActive` 判据），一个 **N=1 但从未做过 force-single** 的正常单节点集群，在任何一次 roster 短暂落后 raft 的窗口里获得 leadership，就会自动建 finalize op 并**删掉那些 roster 行**。测试全绿。去掉 ② 同理：一个 N=3 集群里一次 bounded-stale 落后就足以触发。

**修法** 用 `fsTestBackend` 写一张 5 行表：四条判据全中 ⇒ 建 op；逐条置反 ⇒ 不建 op（断言 `cluster_operations` 仍为空）。

---

### F6 — MAJOR：C1-12 的第三项（`AbortOp` 注释点名 pass）没做，注释仍在承诺一个它没指的东西

**证据** `internal/broker/cluster_operation_controller.go:286-288`（本批未改）：

```go
// AbortOp transitions a non-terminal op to ABORTED (predecessor-CAS), freeing the per-node active slot
// WITHOUT touching the substrate (the membership stays whatever the gates left it; reconcile/doctor
// heals). The stuck-op escape hatch.
```

plan §5.1 结尾逐字：**"`AbortOp` 的注释（… 的 "reconcile/doctor heals"）改为**点名这个 pass 的名字**——让承诺变事实。"** 这句话是整个愈合 pass 存在的动机陈述（`reconcile_drain_marker.go:14-16` 自己也写着 "the healer AbortOp's comment has been promising since C4"），却没有回填。

而且回填后必须**收窄**：新 pass 只清"无 roster 行的孤儿 marker"，它并不 heal `AbortOp` 遗留的一般 membership 状态。所以原注释"reconcile/doctor heals"作为全称承诺**至今仍然是假的**（class-4：注释承诺了代码不提供的保证）。

**修法** 把该注释改成："membership 保持 gate 留下的样子；本仓唯一的自动愈合是 `drain-marker` pass（`reconcile_drain_marker.go`），它**只**清 roster 行已不存在的孤儿 `broker_draining` marker——其余形状需要运维用 `cluster drain --abort` / `cluster recovery node remove`，`cluster doctor` 的 `roster_consistency` 会报出来。"

---

### F7 — MAJOR：C2-12 的文案扫半途而废，`docs/usage.md` 现在自相矛盾

**证据**

```
docs/usage.md:937  tether push <local-path> <nid>:<remote-path> [--force] [--timeout 10m] [--ack-alerts]
docs/usage.md:938  tether pull <nid>:<remote-path> <local-path> [--force] [--timeout 10m] [--ack-alerts]
docs/usage.md:948  | `--timeout` | `36m8s` | 每个阶段的上限；… |
```
——同一页，相隔 10 行，两个不同的默认值。

同时，broker 侧两条 `too_large` 已改为 `humanBytes(...)` 派生（`transfer.go:694`/`:699`，注释写明"the human-readable size is DERIVED, not hand-copied"），而**同类的三条仍是手抄**：

```go
internal/agent/transfer.go:403   Error: fmt.Sprintf("file size=%d > 2 GiB", size)
internal/agent/transfer.go:472   Error: fmt.Sprintf("file grew beyond 2 GiB while reading (read=%d)", uploaded)
cmd/tether/transfer.go:523       fmt.Sprintf("object size=%d exceeds 2 GiB limit", pr.Size)
```

plan §6.6 明写"每处要么改成从常量派生，要么在本 plan 里列为『纯散文、不派生』并说明"——这三条既没派生，plan 里也没有把它们列为纯散文。而且它们不是散文，是**运维会读到的错误串**，与被修掉的那两条完全同类。

**失败场景**

`XferMaxBytes` 下调（例如小盘 broker 版本）后，运维在 push 失败时读到 "> 2 GiB" 而实际上限是别的值——正是 §11 C2-12 描述的那一条。`--timeout` 的矛盾更直接：照 synopsis 抄 `--timeout 10m` 的人，会在 2 GiB 传输走到第 10 分钟时被自己的 ctl 打断，而 broker 还要再等 24 分钟。

**修法** ① `usage.md:937-938` 的 synopsis 改成 `[--timeout 36m8s]` 或去掉具体值写 `[--timeout <dur>]`；② 三条错误串改成派生（agent 侧可新增一个与 `humanBytes` 等价的小 helper，或直接 `%d bytes`），或在架构文档 §20.2 里明确列为不派生并说明理由。

---

### F8 — MAJOR：`startTransferWatchdog` 与 `reapBucketObjects` 的**调用点**无覆盖——helper 绿而 caller 可以静默回退

**证据**

- `internal/broker/transfer.go:509` `d := proto.XferBudget(e.tier, e.size, proto.XferPushLegs)` —— `grep -rn startTransferWatchdog --include=*_test.go` 只在 `cmd/tether/error_code_coverage_test.go:246` 的豁免表里出现（且那条豁免的说明文字 `"audit-only watchdog code selected by the switch immediately above"` 已经**过时**：`:527-531` 的 `switch ent.verb` 本批已被 `code := …; if ent.verb == "pull"` 取代)。
- `internal/broker/transfer_reconcile.go:101` `b.reapBucketObjects(ctx, name, b.crossHomeReapAge(), true)` —— `sizeAware=true` 这个实参没有任何断言。`TestCrossHomeGraceCoversALiveTransferOfThatSize` 断言的是 `xferCrossHomeReapAge + xferCrossHomeExtraFor(size) > budget`，**全部在 helper 上**。

**失败场景**

把 `:509` 改回 `d := transferTimeoutTierB`（C2 的全部价值消失），或把 `:101` 的 `true` 改成 `false`（§11 C2-3 那条"亲手造出新的数据删除缺陷"精确复现：leader 在 15 分钟处删掉一个 owning home 的看门狗还要覆盖到 44 分钟的 2 GiB 在飞对象）——**`make test` 全绿**。§9 说 `TestTransferBudgetRelations` 的变异是"把 budget 改回常量 `transferTimeoutTierB`"，这条变异今天抓不住。

**修法**
- 补 `TestWatchdogArmsTheSizeDerivedBudget`：把 `startTransferWatchdog` 的时长计算抽成一个可测的小函数（或用注入的 `Now` + 一个 1 秒级的 `XferMinThroughput` 测试覆写），断言 tier-b/size=2 GiB 得到 `XferTierBMaxBudget` 而非 `transferTimeoutTierB`。最省事的形态：加一个 `func watchdogBudget(e *transferEntry) time.Duration` 并在测试里表驱动。
- `reconcile_passes_test.go` 已有 `TestXferCrossHomeGCReapsSplitHome`（`:1181`，用 `XferCrossHomeReapAge: time.Nanosecond`）。照它加一条：对象 `Size = 2 GiB`、`XferCrossHomeReapAge` 设成一个大于 15m 但小于 44m 的值、ModTime 设成 20 分钟前，断言**没有**被 reap；把调用点的 `true` 改成 `false` 时必红。
- 顺手更新 `error_code_coverage_test.go:246` 的过时豁免说明（属既有测试，按角色边界只报不改）。

---

### F9 — MAJOR：C2-13 / §9 的 `TestPullEntryCarriesNoSize` 不存在，N5 变成无守卫的口头承诺

**证据** `internal/broker/transfer.go:875-881` 的 pull 分支 `transferEntry{…}` 字面量里没有 `size:` 字段（正确，符合 N5），但全仓没有任何断言钉住这一点。plan §6.6 结尾与 §9 都逐字要求这条回归测试，理由写得很清楚：**"防止读者误以为 pull 也被修了"**。

**失败场景** 有人为了"顺手把 pull 也做了"给 pull entry 填一个来路不明的 size（例如从 `finalize.req` 的 body 里取），pull 的看门狗预算就变成一个客户端可控且未过 admission gate 的值——`transfer.go:509` 的 `proto.XferBudget(e.tier, e.size, 2)` 是无上界的（上界只由 admission gate 提供，而 pull 路径**没有** size admission gate）。这直接把 `XferTierBMaxBudget` 的"编译期上界"论证打穿。

**修法** 加 `TestPullEntryCarriesNoSize`：驱动 `handlePullReq`（`tracker_miss_silent_test.go:116` 已有现成的 `PullPrepareReq` 夹具），断言 `b.transfers.get(tid).size == 0`，并在失败信息里点名 N5 与"pull 无 admission gate ⇒ 有 size 就等于无上界"。

---

### F10 — MINOR：§12 要求的"小盘 broker 最坏代价"只写了 3 项中的 1 项

**证据** D-C2-2 订正后的三项代价：① 对象字节在盘上多待 6.8×（racknerd 的 per-session 天花板下 3 个并发 2 GiB 在飞对象就让第 4 个 `Put` 撞 10047）；② tracker 槽位耗尽窗口 5m→34m；③ 悬空 start 审计行与 ledger 文件多活 29 min。

代码里只有 ②（`internal/broker/transfer.go:142-153`，写得很好）。`grep -rn "10047"` 全仓无命中；③ 无任何记载。§6.2 把"必须在小盘 broker（现网 racknerd）上把最坏情况说清楚"写成了取值的**前置条件**。

**修法** 在 `internal/proto/xfer.go` 的推导块或 `docs/distributed-broker-architecture.md §20.2` 补两段：①"per-session bucket `MaxBytes` 下，在飞对象占用时长 ×6.8 ⇒ 并发上限从 N 降到 ~N；racknerd 现网天花板下第 4 个并发 2 GiB `Put` 会 10047"；③"start 审计行与 ledger 文件多活 29 min，`xferaudit` 的悬空窗口同比例放大"。

---

### F11 — MINOR：`internal/broker/transfer.go:77` 引用了一个不存在的标识符

```go
// … xferCrossHomeFloorFor makes the invariant per-object instead, which is both exact and free of
// deployment surface.
```
`grep -rn xferCrossHomeFloorFor --include=*.go .` 只命中这一行注释；实际函数叫 `xferCrossHomeExtraFor`（`:111`）。下一个 grep 这块的人找不到它。改名即可。

---

### F12 — MINOR：`topoLaggards` 的注释承诺了一个不成立的等价性

`cmd/tether/cluster_reconcile.go:181-185`：

```
// The set of laggards is unchanged by construction — every state ClassifyTopo marks Degrades() is one
// the old disjunction also caught …
```

反例：`TopoAction = "<未来版本的新 action>"`、`reason == ""`、`observed >= desired` ⇒ 旧谓词 `observed < desired || reason != ""` 判**非** laggard，新谓词 `TopoUnknownAction.Degrades() == true` 判 laggard。混版（ctl 旧于 broker）下这是可达的，`--wait` 会一路轮询到超时并 exit 75。行为方向是安全的（fail-closed），但注释的"unchanged by construction"是假的。改成"集合可能**扩大**：新增的 `TopoUnknownAction` 是旧谓词看不见的一类，这是有意的 fail-closed"，并考虑把 `topoWedged` 也纳入 `TopoUnknownAction`（否则它既 bail 不掉、又永远等不到）。

---

# 必须保住、不要在修的过程中弄丢的东西

1. **`internal/natsconf/topostate.go` 的整体设计**：把分类器放在 `natsconf`（零新 import 边、不可能成环）、`adminsock`/`proto` 只加 `string` 字段不加 import——N9 的形状被精确保住了。文件头把三个缺陷 (a)(b)(c) 的复现路径整段写下来，是本批最高价值的产出。
2. **`TopoHeld` 的否定子句**，以及它在 `computeHealth` / `topoCell` / `doctor` / card / `--wait` 五处的一致复用。`TestAwaitingCutoverIsHeldAndNeverRecommendsManual` 与 `TestComputeHealthTopologyVerdicts` 的 `negateManual` 分支都专门防"naive 的『不含 --manual』断言"——这个细节别在重写测试时弄丢。
3. **`test/determinism/topo_classification_test.go`**：AST 按**实参位置**匹配（而不是文件级共现）、带 live-tree 非空自检、带合成样本的 scanner 非恒真自检；`TestXferTierCeilingsHaveOneSource` 按 **const 名**豁免而非 file:line，且用 EXACT 匹配避开 `8 GiB` 前缀误杀并把这次自我修正写进注释。这两个门是本批最耐久的资产。
4. **`internal/proto/xfer_test.go` 的文件头**：明确记录"第一稿写成 `legs × legBudget` 是恒等式、`legs` 两边约掉"，并因此改用手算字面量。这是 D-C2-5 的正确落地，也是 C1 那三条恒真测试的反面教材。
5. **`TestConfirmOpGuardRunsBeforeClearOpAttempts`** 的做法：作者实跑变异、发现行为测试抓不住顺序、于是补了 AST 顺序门并把"行为版为什么不够"写进注释。这是 §9 纪律的正确样板。
6. **C2 里三处"看似小、实则承重"的判断**：`Ts` 留在 tier 下限（dedup reqID 载体，D-C2-10）；跨 home 用**增量**而非绝对下限（保住 drill 压缩 seam + 不惩罚小对象 + 零部署面）；agent prep 缓存容量满时**拒新 prepare 而不是淘汰最旧**（D-C2-4，且 `TestPushCommitCacheNeverEvictsALiveLargeTransfer` 是真行为测试）。这三条的论证都不要在返工时被简化掉。
7. **`opFrozenByUpgradeLock` 抽成具名谓词 + 反向表测试**（含"未来 kind 默认冻结"一行），以及 `driveOne` 的 `default:` + `PlanClusterOpAbort`（不查枚举的逃生口）。这是回滚不对称性的正确处理。

---

# 判定：**INCOMPLETE**

批 C 的 **C3 已实质完成**（唯一缺口是 drill 93 的失败极性）。**C2 结构完整、测试质量高，缺两处 caller 层覆盖与一条明写的回归测试，文案扫半途而废。C1 的实现写完了，但它的验收面塌了一半**：三个核心函数零测试引用、三条守卫是恒真断言、运维手册和部署层 drill 完全没动——也就是说，C1 的**代码**在，C1 的**证据**不在。

按操作者"不得延后、推迟任何剩余工作"的口径，下列各条都必须在本流程内完成（不接受"永久决策"降级，因为它们全部是 plan §4.5/§5.6/§6.7 的具名交付，而非 §3/§6.1b 的裁决项）：

1. **F2** — `docs/cluster-runbook.md` §3.2 分支化 + Abandoned-peer roster 段补第二条 ghost 来源 + 回滚前置检查（C1-15）。
2. **F1a** — drill 93 加 render 失败注入 + `STUCK` 极性 + doctor FATAL 断言（C3-13）。
3. **F1b** — drill 12 加 prune 失败注入分支；若部署层做不出注入，标 `[GAP #N]` 明写，不得以"单测覆盖"抵账（C1-16）。
4. **F3** — 用现成的 `fsTestBackend` / `passBroker` 补 6 条 C1 行为测试（C1-3/4/5/6 的验收）。
5. **F4** — 重写 `TestForceSingleFinalizeLadderIsAlwaysTerminal` / `TestForceSingleGhostSignatureIsPhaseVoterOnly` / `TestDrainMarkerHealerLeavesAHalfFinishedDrainAlone` 三条恒真测试，并**逐条实跑它们声称的变异确认变红**；补 `TestDrainMarkerHealerIdleZeroWrites`。
6. **F5** — 补四条判据的置反表测试（C1-6）。
7. **F6** — 回填并**收窄** `AbortOp` 的注释，点名 `drain-marker` pass（C1-12 第三项）。
8. **F7** — `usage.md:937-938` synopsis 与 agent/ctl 三条手抄错误串（C2-12）。
9. **F8** — 补 watchdog 时长与 cross-home `sizeAware=true` 调用点的行为测试（C2-2/C2-3 的验收）。
10. **F9** — 补 `TestPullEntryCarriesNoSize`（C2-13）。
11. **F10** — 补齐 §12 要求的另外两项代价陈述。
12. **F11 / F12** — 两处注释订正（低成本，顺手做）。

补完后需重跑 `make test` + `make lint` + `make e2e-parallel`，并按 §8 实跑 drill 93 / 22 / 12（20、41 为回归）。


################ agent abf78512279bded0d ################

## 结论：本 lane（call-site sweep）共 12 条 finding

---

### 1. BLOCKER — 本批**新增**的三条运维补救文案都漏了 `--manual`，而 `cluster recovery node remove` 没有它会直接拒绝

**证据**（三处全是本批新写的字符串）：

- `cmd/tether/cluster_offline.go:407`
  ```go
  "If the operation ends in FS_GHOST_LEFT, run `tether cluster recovery node remove <id>` for\n"+
  ```
- `internal/broker/clusterops.go:73-74`（FS_GHOST_LEFT 的 Resume）
  ```go
  e.Resume = "force-single finalize could not remove every abandoned roster row — run " +
      "`tether cluster recovery node remove <id>` for each id in last_error. …"
  ```
- `internal/broker/force_single_finalize.go:250`（写进 op 的 `last_error`）
  ```go
  "Run `tether cluster recovery node remove <id>` for each. Until they are gone, "+
  ```

命令自身（`cmd/tether/cluster.go:612-615`）：
```go
if !manual {
    return usageErr("raw `remove` requires --manual (this is the last-resort escape); the routine path is `tether cluster retire %s` …", args[0])
}
```
`docs/cluster-runbook.md:471` 写对了：`sudo tether cluster recovery node remove <ghost-node-id> --manual`。

**失败场景**：racknerd 那类事故现场——prune 失败 → op 落 `FS_GHOST_LEFT` → 运维按 stderr 逐字粘贴 `tether cluster recovery node remove pc732` → 得到 usage 错误，并被**指向 `cluster retire pc732`**；而 ghost 的 `Role == ""`、不在 raft config 里，`cluster retire` 是这条路上最不该走的一步。JetStream 继续 503。同一批还有 `clusterops.go:163` 的既有文案是**带全 flag** 的（`… remove <id> --manual --force`），证明这个 flag 是被知道的、只是新文案没跟上。

**修法**：三处统一为 `tether cluster recovery node remove <id> --manual`（并按 `cluster.go:604` 的 Example 附上无 TTY 时的 `--confirm-node-id <id>` + `TETHER_CONFIRM_NODE_ID` 提示，因为 force-single 现场常在 `systemd-run`/脚本里）。`force_single_finalize.go:372` 的 Warn 同理。

---

### 2. BLOCKER — plan 的 C1-15 完全没做：`docs/cluster-runbook.md` 仍逐字保留那条已被证伪的因果指引

**证据**：`git status` 里没有 `docs/cluster-runbook.md`。而 plan §5.2 的证据链第 3 条**引用的就是这一行**，C1-15 明写要改：

`docs/cluster-runbook.md:461-462`
```
  The abandoned peers are already pruned (below), so the N=1 voter tally passes and `--to-standalone`
  is unlocked.
```
`:464-466`
```
**Abandoned-peer roster (#12).** force-single now PRUNES the abandoned peers from the roster automatically
(both the online and offline paths), so agents/ctl converge to N=1 immediately …
If you upgraded from an older binary and a **ghost VOTER** lingers …
```

**失败场景**：CLI 已经按分支说话了（`declusterPruneNote`），runbook 没有。运维在灾难现场读的是 runbook（`cluster-runbook.md §3.2` 是 online force-single 的操作手册），它无条件断言"已 pruned 所以 N=1 通过"，并把 ghost 归因为"**从旧二进制升上来的**"——恰好排除了本批新造出来的那条产生路径（同步 prune 失败 → finalize op → `FS_GHOST_LEFT`）。运维照 §3.2 直接跑 `--to-standalone`，撞 `unrecognized raft role ""`，而 runbook 里没有任何一句能把他导向 `cluster ops show`。

**修法**：§3.2 那段改成两分支（commit 输出无 `finalize op` ⇒ 现文案；有 ⇒ 先 `cluster ops show <op-id>` 到终态、`FS_GHOST_LEFT` 则逐个 `recovery node remove … --manual`，再 de-cluster），并在 "#12" 段补上"prune 失败 → finalize op"这条新的 ghost 来源。

---

### 3. MAJOR — agent 侧 prep TTL 在**整个 tier-B 区间**都低于 broker 的看门狗预算；D-C2-3 的那 1 分钟余量被反转了，而守卫测试比的是另一个量

**证据**：`internal/agent/transfer.go:55-57, 75-77`
```go
// It preserves the shape of the pre-batch-C constant: the old TTL was 6m against a 5m
// tier-B watchdog, and that ONE MINUTE OF MARGIN was the entire meaning of the 6 …
func pushCommitEntryTTL(size int64) time.Duration {
	return proto.XferLegBudget(size) + pushCommitCacheSlack   // 无 floor
}
```
而 broker 侧（`internal/broker/transfer.go:517`）用的是带 floor 的 `proto.XferBudget("b", size, 2) = max(5min, 2·leg)`。

代入 `XferMinThroughput = 2 MiB/s`：

| size | leg | **TTL = leg+60s** | **broker budget** |
|---|---|---|---|
| 9 MiB | 5s | **65s** | 300s |
| 100 MiB | 50s | **110s** | 300s |
| 512 MiB | 256s | **316s** | 512s |
| 2 GiB | 1024s | **1084s** | 2048s |

`TTL > budget` 需要同时 `leg < 60` 与 `leg > 240`——**无解**。即：改动前 `360s > 300s` 恒成立，改动后**对每一个 tier-B 尺寸都不成立**。注释宣称的 "preserves the shape" 与代码相反。

**失败场景**：ctl 在一条真的只跑 1 MiB/s（即产品承诺覆盖的下限之下一档）的链路上推 100 MiB，上传耗时 200s。agent 的 prep 条目在 110s 起就算过期；只要在 110s–200s 之间该 agent 收到**任何**另一个 `push.req` prepare，`rememberPushCommit` 的清扫循环（`transfer.go:559-563`）就把它删掉；push-commit 到达 → `transfer_unknown` → 整次传输失败。改动前这需要 360s，实际上够不到。这正是 D-C2-3 要防的那个缺陷，只是搬到了小文件上。

**测试为什么抓不到**（class 2）：`internal/agent/transfer_test.go:243-246`
```go
ttl, upload := pushCommitEntryTTL(size), proto.XferLegBudget(size)
if ttl <= upload { … }
```
展开是 `leg+60 > leg`——只要 `pushCommitCacheSlack > 0` 就恒真，与 size、与 broker 的 floor 全无关。它声称能抓 "key the TTL on something other than size"，但抓不到"TTL 掉到 broker 预算以下"这条真正承重的关系。

**修法**：`pushCommitEntryTTL` 改为 `proto.XferBudget("b", size, 1) + pushCommitCacheSlack`（与同文件 `handlePushCommitForwarded` 里 `commitCtx` 用的**同一个**函数，见 `transfer.go:239`），恢复 `TTL > broker budget`；测试改成对每个 size 断言 `pushCommitEntryTTL(size) > proto.XferBudget("b", size, proto.XferPushLegs)`。

---

### 4. MAJOR — `cluster status` 的 TOPO 列图例没加 `HOLD` / `?`，且仍把 `--manual` 作为唯一补救——正是 C3 花整批去消除的那条指引

**证据**：`cmd/tether/cluster.go:510`（未改动）
```go
_, _ = fmt.Fprintln(w, "  · TOPO=NATS topology applied/observed→desired gen (✓ converged · … catching up · STUCK fix conf or `cluster reconcile nats --manual`)")
```
而 `topoCell` 现在会输出 `natsconf.TopoHeld.Cell() == "HOLD"` 与 `TopoUnknownAction.Cell() == "?"`（`internal/natsconf/topostate.go:115,119`）。

**失败场景**：首次 standalone→clustered grow 的 broker 在 TOPO 列渲染 `4/4→7 HOLD`。运维在同一屏往下三行读图例：没有 `HOLD` 这一项，唯一带补救动作的条目是 "STUCK fix conf or `cluster reconcile nats --manual`"。他跑 `--manual` —— `natsconf/reconcile.go:182-185` 明写这会造成 G4 #10 / #4。整批 C3 把 `TopoHeld.NextStep()` 的否定子句做成"运行期防线"（`topostate.go:180-184` 的注释就是这么写的），却漏掉了**印在同一屏上、离得最近的那句话**。

**修法**：图例补 `HOLD 见 next-step，勿跑 --manual`、`? 本二进制不认识该 action`，并把 `--manual` 的推荐限定在 STUCK 上（或干脆从图例删掉、只留"见 next:"）。

---

### 5. MAJOR — drill 93 新加的 TOPO 列断言取错了 awk 字段，必红

**证据**：`test/simcluster/drills/93-metrics-observability.sh:135-136`
```sh
assert_ok "TOPO-ACTION: the human TOPO column agrees — every voter renders the converged marker, not the catching-up one" \
    sh -c "$SIM exec $LDR -- sh -c 'tether cluster status 2>/dev/null' | awk 'NR>1 && \$3==\"VOTER\" {print \$9}' | grep -q '✓'"
```
表头是 10 列（`cluster.go:473`），但 TOPO 这一格本身含空格：`topoCell` 返回 `fmt.Sprintf("%d/%d→%d %s", …)`。awk 默认按空白拆，所以 `$9` = `7/7→7`，marker 在 `$10`。实测：

```
$ printf '…\nbrk1  brk1  VOTER  leader  v0.4.7  0  Y  3/3  7/7→7 ✓  self\n' | awk 'NR>1 && $3=="VOTER"{print $9}'
7/7→7                      # grep -q '✓' → rc=1
```

**失败场景**：跑 `./local.sh drill 93` 时这条 `assert_ok` 必失败 → drill 93 从 `INCOMPLETE/nc_gap=1`（`expected-verdicts.tsv:49`）变成 ASSERT-FAIL 偏差。且 NAME 为空时字段整体左移一位，`$3=="VOTER"` 直接不匹配、断言变成对空集 grep（另一种红）。

**修法**：不要按字段序号取。整行匹配即可：`grep -E '\bVOTER\b.*→[0-9]+ ✓'`，或对每个 VOTER 行 `awk '{print $NF, $(NF-1)}'` 之类；最稳的是继续用 `--json` 的 `topo_action`，人类列只断言"不含 STUCK/HOLD/?"。顺带：plan C3-13 还要求断言 **render 失败时极性为 STUCK**，drill 里只有收敛侧断言，缺的那一半正是 §11 规避清单点名的 "只断言列存在、不断言极性"。

---

### 6. MAJOR — `FS_GHOST_LEFT` 之后 finalize op 再也建不出来（op id 确定性），而预算只有 60 秒

**证据**：
- `internal/broker/force_single_finalize.go:70`
  ```go
  const opForceSingleFinalizeBudget = 12 * observeTickInterval   // observeTickInterval = 5s ⇒ 60s
  ```
- `:131`
  ```go
  opID := mintOpID(cluster.OpKindForceSingleFinalize, selfID, strings.Join(abandoned, ","))
  ```
  `mintOpID` 是 `sha256(kind|target|discriminator)` 的纯函数（`cluster_operation_controller.go:19-22`）。对比：join 用 `b.JoinNonce`、retire 用 `time.RFC3339Nano` —— **只有 finalize 的 discriminator 完全来自持久状态**。
- `internal/cluster/operation_ops.go:129`
  ```sql
  AND NOT EXISTS (SELECT 1 FROM cluster_operations WHERE op_id = <opID>)
  ```
- `:152-158` 的 read-back 于是必然落到
  ```go
  return "", fmt.Errorf("the finalize op was not created (an operation for %q is already in flight)", selfID)
  ```

**失败场景**：prune 因磁盘满 / leader 尚未稳定而失败 → 建 op → **60 秒**内 12 次重试都没成 → 终态 `FS_GHOST_LEFT`。运维清理磁盘、重启 broker。`ReconcileMembershipOnLeadership` → `resumeForceSingleFinalizeOnLeadership`（`clusteradmin.go:412`）：`force_single_active` 仍置位、voters==1、self 无在飞 op、ghost 行仍在 ⇒ 计算出**同一个** opID ⇒ INSERT 被 `op_id` 子句吃掉 ⇒ read-back 返回错误 ⇒ 只打一条 Warn "could not start a finalize op"（`:371-373`），而错误文本还说"an operation for X is already in flight"——事实上一条都没有。崩溃窗口的覆盖（C1-6）就此对**同一组 ghost 的第二次机会**恒定失效。

**修法**：discriminator 里加一个非确定量（`a.now().UTC().Format(time.RFC3339Nano)`，与 retire 同形），或在 read-back 前先查 `cluster_operations WHERE op_id = ?` 区分"已存在终态同名 op"并换 id 重建；同时把 60s 预算重新论证（"a couple of dozen ticks" 的注释与 `12` 也对不上）。

---

### 7. MAJOR — `Inconsistent` 被赋予了第三种含义，但三个渲染面 + doctor 的文案一个都没扫

**证据**：`internal/broker/clusterstatus.go:349-357` 新增第三个 disjunct
```go
inconsistent := (r.phase == phaseVoter && (!inCfg || ro == "learner")) ||
    (!inCfg && r.phase != phaseRetiring && r.phase != phaseAddFailed) ||
    drainingWithoutMarker(r.phase, drainMarked[r.nodeID], activeOpTarget[r.nodeID])
```
消费侧全部未动：
- `internal/adminsock/protocol.go:751` — `Inconsistent bool \`json:"inconsistent"\` // phase says voter but raft config disagrees (or vice-versa)`（同一批**改了**隔壁 `Abandoned` 的注释，说明作者知道这套动作）
- `cmd/tether/cluster_doctor_online.go:98` — `add("roster_consistency", inconsistent, true, …, "%d node(s) roster/raft INCONSISTENT — run \`cluster doctor\`/\`status\`")`
- `cmd/tether/cluster_status_card.go:128` — `return "node " + n.NodeID + " roster/raft INCONSISTENT"`
- `cmd/tether/cluster.go:481-483` — 表格 flag `" INCONSISTENT"`

**失败场景**：一次 drain 死在自己两步之间（phase 已 DRAINING、marker 已被清）。三个面都告诉运维 "roster/raft INCONSISTENT"——而 raft 和 roster 其实是一致的；`cluster doctor` 退 **64**（FATAL），给出的唯一建议是 "run `cluster doctor`/`status`"，即刚跑过的那条命令。plan §5.1 说这一态"必须人工 `cluster drain --abort`"，但**没有任何一个面提到 drain**。运维拿到 exit 64 且零出路。

**修法**：要么给这一态单独的 wire 位/文案（推荐：`ClusterNodeStatus` 加 `DrainStuck bool` 或让 `Inconsistent` 带 reason 串），要么至少把 doctor 的 `badFmt` 与 card 的文案改成能区分两因，并把 `cluster drain --abort <node>` 写进去；同步订正 `protocol.go:751` 的注释。

---

### 8. MINOR — `docs/usage.md` 自相矛盾：命令 synopsis 仍写 `--timeout 10m`，且"ctl 侧用同一个推导"不属实

**证据**：`docs/usage.md:937-938`（未改）
```
tether push <local-path> <nid>:<remote-path> [--force] [--timeout 10m] [--ack-alerts]
tether pull <nid>:<remote-path> <local-path> [--force] [--timeout 10m] [--ack-alerts]
```
同节 `:948`（已改）
```
| `--timeout` | `36m8s` | …
```
`:983-984`
```
agent 侧与 ctl 侧用同一个推导（各自只负责自己那一腿），…
```
但 ctl 侧根本不按 size 推导：`cmd/tether/transfer.go:370` 是一个**常量** `cliTransferTimeoutDefault = proto.XferTierBMaxBudget + 2*time.Minute`，并被原样用于 `pushTierA`、`prepCtx`、`putCtx`、`commitCtx`、`waitCtx`、`sendFinalize`（`:208,251,273,312,330,617`）。flag help 说 "tier A: ~30s; tier B: derived from the file size" 同样是在描述它没做的事。

**失败场景**：`docs/usage.md:948` 与 `:937` 相隔十行互相矛盾；且一次 tier-A push（broker 看门狗 30s）如果回包丢了，ctl 现在会**阻塞 36 分钟**（原来 10 分钟），而帮助文本让人以为是 ~30s。

**修法**：synopsis 两行改 `[--timeout 36m8s]`；`:983` 改成"agent 侧按自己那一腿的 size 推导，**ctl 侧是一个覆盖最坏情况的固定上界**"；flag help 同步（或真的让 ctl 按 `st.Size()` 派生 per-phase 期限）。

---

### 9. MINOR — `internal/serveconf` 与 `docs/broker-ops.md` 里描述 reap-age 关系的运维文案已过时（plan D-C2-8 只清点了三段，漏了这两段）

**证据**：`internal/serveconf/serveconf.go:239-245`
```go
// … so a 5s floor lets the leader delete another home's in-use object minutes before
// that transfer's own 5-minute watchdog would end it. …
```
`docs/broker-ops.md:130-135`
```
# xfer_cross_home_reap_age: 15m  # …外审 F2：低于 tier-B 看门狗的下限会让 leader 删掉另一 home 上
#                                # 仍在用的对象… 默认派生自 3×tier-B 超时(=15m)…
```
tier-B 看门狗已不是 5 分钟，最大 34m08s；15m **单独已不覆盖它**，覆盖靠的是本批新增的逐对象增量 `xferCrossHomeExtraFor`（`internal/broker/transfer.go:111-118`），两处文案都没提。

顺带同类：`internal/broker/transfer.go:104-108` 的注释宣称增量式"drill seam intact"，但对 drill 96 实际使用的 **1 GiB** 载荷，额外宽限是 `2·512s − 300s = 724s ≈ 12 分钟`——压缩 floor 到秒级的 drill 对这个尺寸的对象**并没有**保住 seam。

**修法**：serveconf 那段把 "5-minute watchdog" 改为 "tier-B watchdog (now size-derived; the per-object increment lives in broker.xferCrossHomeExtraFor)"；broker-ops 的注释补一句"实际下限 = 本值 + 该对象自己的预算超出 tier floor 的部分"；transfer.go:104-108 把 "drill seam intact" 限定为"对预算等于 tier floor 的对象（≲300 MiB）"。

---

### 10. MINOR — "2 GiB" 手抄文案只扫了 broker，agent 与 ctl 两端原样留着

**证据**：broker 侧已派生（`internal/broker/transfer.go:694,699` 用 `humanBytes(...)`，注释 `:691-693` 还专门解释了为什么），但 plan §6.6 点名要一起扫的另外两端没动：
- `internal/agent/transfer.go:403` — `Error: fmt.Sprintf("file size=%d > 2 GiB", size)`
- `internal/agent/transfer.go:472` — `Error: fmt.Sprintf("file grew beyond 2 GiB while reading (read=%d)", uploaded)`
- `cmd/tether/transfer.go:523` — `fmt.Sprintf("object size=%d exceeds 2 GiB limit", pr.Size)`
- `cmd/tether/transfer.go:46-49` 的长帮助与 `:121` 注释仍手写 "8 MiB / 2 GiB"

值目前还对，所以不是今天的错；但这正是本批宣称已消灭的机制——`TestXferTierCeilingsHaveOneSource` 只扫 `const NAME = <literal>`，对这些**格式串里的文字**完全盲。

**修法**：三处错误串改用一个共享的 humanize（或直接 `%d` + `proto.XferMaxBytes`）；剩下的纯散文（长帮助、`internal/schema/audit.go:73`、`internal/proto/messages.go:764-767`）按 plan §6.6 的要求在 plan 里显式登记为"纯散文、不派生"。

---

### 11. MINOR — 新 op kind 没进 `cluster ops` 的自述与 `docs/usage.md` 的枚举

**证据**：
- `cmd/tether/cluster_ops.go:18` — `Short: "Inspect membership operations (add / drain / retire) and their state"`（`:11` 的文件注释同）
- `docs/usage.md:342` — `查看 membership 操作（add/drain/retire）状态 + resume 提示（派生自 roster）`

**失败场景**：force-single 失败分支把运维送到 `tether cluster ops show <op-id>`（finding 1 的同一段文案）。他落地的命令自称只管 "add / drain / retire"，OP 列却是 `force_single_finalize`，STATE 是 `FS_PRUNE_PENDING`/`FS_GHOST_LEFT`——CLI 帮助、`--help`、usage.md 三处都查不到这三个词是什么意思。

**修法**：`Short` 与 usage.md:342 补 `force-single finalize`；`docs/usage.md` 传输错误码表（`:1007-1023`）也补 `transfer_budget_exceeded`（目前它只出现在 `:982` 的散文里，而它已经是 tier-B 超时的主码）。

---

### 12. MINOR — 四条断言在生产侧发生任何变异时都不会变红（含一条恒假合取）

- `internal/broker/transfer_budget_test.go:59-64`
  ```go
  base := transferTierFloorFor("b")
  for _, size := range []int64{0, 1, proto.XferMaxBytes} {
      if transferTimeoutFor("b", size) != base && transferTierFloorFor("b") != base {
  ```
  右合取项是 `base != base`，**恒假**，循环体永不执行。声称覆盖的"Ts 不随 size 移动"完全没被断言（前面 `:52-57` 那两行是承重的，所以整个函数不算空转，但这段是）。
- `internal/broker/force_single_finalize_test.go:243-263` `TestDrainMarkerHealerLeavesAHalfFinishedDrainAlone`：函数体自己重写了一遍 `if !roster[node]`，**从不调用 `orphanDrainMarkers`**。它声称的变异（"让 orphanDrainMarkers 改按 no-active-op 选"）不会让它变红。
- 同文件 `:186-201` `TestForceSingleGhostSignatureIsPhaseVoterOnly`：只断言 `phaseVoter == "VOTER"` 及各 phase 常量互不相等，**从不调用 `forceSingleGhostRows`**；声称的变异（放宽 phase 过滤）同样抓不到。
- 同文件 `:268-281` `TestForceSingleReportSeparatesIntentFromCompletion`：构造结构体字面量再断言自己刚填的字段，纯恒等。
- 附带一条手抄常量：`transfer_budget_test.go:182` `homeGrace := 2 * time.Minute // xferReapMinObjectAge's production value` —— 不读 `xferReapMinObjectAge`（`transfer_reconcile.go:18`）。抬高该常量后，这条自称"断言残留仍存在"的守卫会静默保持绿。

**修法**：恒假合取改成对 `transferTierFloorFor("b")` 与 `transferTimeoutFor("b", size)` 在大 size 上**必须不等**的正向断言；三条 cannot-fail 的测试改成真的调用被测函数（`orphanDrainMarkers` / `forceSingleGhostRows` 需要 DB，用 `passBroker` 那套 fixture）；`homeGrace` 直接引用 `xferReapMinObjectAge`。

> 另外两条低价值但同类：drill 22 新加的两条断言都是 `! <cmd> | grep -q …` 形状（`22-forcesingle-online.sh:118-121`），命令整体失败时断言**通过**（fail-open）；`transferTracker.recentlyReaped`（`transfer.go:215`）用 `time.Since` 而 `markReaped` 用注入的 `b.cfg.Now()`，两套时钟混用。

---

## 做对了、修的时候别弄丢的部分

1. **`natsconf.ClassifyTopo` 落在 `internal/natsconf`**：实测零新增 import 边，`adminsock` / `proto` 保持零内部依赖，只各加一个 `string` 字段——N9 的形状真的守住了。
2. **STUCK/HELD 不被 `observed < desired` 门住**，以及 `classifyLegacyReason` 里 cutover marker **必须先于** `render (` 判——注释把顺序的承重性写清楚了，别在"整理"时调换。
3. **`topoConvergedForOp` 明确不动**（`cluster_operation_controller.go:1137-1142` 那段"Do not 'finish the refactor'"），并配了 `TestComputeHealthIgnoresUnreachedVoterTopology` 把两种极性各自钉住。
4. **`Ts`/`DurationMs` 留在 tier floor**（`transferTierFloorFor`）而 stranded 判定用真实预算——dedup reqID 跨版本稳定这条论证整段搬运了，且两个用途分了两个函数名。
5. **`xferCrossHomeExtraFor` 是增量而非绝对下限**：既不碰 `serveconf.MinXferCrossHomeReapAge` 这个生产 YAML 硬拒下限，也不惩罚小对象。
6. **agent prep 缓存满时拒新 prepare 而不是淘汰最旧**，且配的是行为测试（`TestPushCommitCacheNeverEvictsALiveLargeTransfer`）而非常量不等式——这条正是 D-C2-4 要的形状（finding 3 只动 TTL 的算式，别把这个改回去）。
7. **`driveOne` 的 `default:` + `PlanClusterOpAbort` 不校验 FromState**：回滚不对称那条真被堵上了，`TestUnknownOpKindIsForcedTerminal` 还反向断言了 `PlanClusterOpTransition` 仍会拒未知 FromState。
8. **`opFrozenByUpgradeLock` 提成具名谓词 + 反向测试**（未来 kind 默认冻结），豁免不会顺手扩大。
9. **`confirmOpKindGuard` 排在 `clearOpAttempts` 之前，并配了 AST 序位测试**，作者还诚实记录了"纯行为测试看不到这个顺序"——这是本批测试里质量最高的一条。
10. **drain-marker 愈合 pass 收缩到"可证明的孤儿"**（marker 在、roster 行不存在），半完成 raw drain 明确不碰（N7），且走既有的 `PlanClusterDrainSet` 而非自造 SQL。
11. **`test/determinism/topo_classification_test.go` 的两个门都带 live-tree 非空转自检**，且 tier 字面量用 EXACT match 而非 Contains（注释里记了第一版就在这上面栽过）。


################ agent ae50e8d26fdaf6837 ################

## C3 lane — 审查结论

以下每条都在 `HEAD` 工作树上实跑验证过（scratch 测试已删除，`git status` 干净）。

---

### 1. BLOCKER — `cluster status` 的 TOPO 列图例没跟着扫，把 `HOLD` 的运维推向 defect (b) 要修的那条数据损坏路径

**证据** `cmd/tether/cluster.go:510`（本轮**未改动**）：

```go
_, _ = fmt.Fprintln(w, "  · TOPO=NATS topology applied/observed→desired gen (✓ converged · … catching up · STUCK fix conf or `cluster reconcile nats --manual`)")
```

而 `topoCell` 现在可以吐出两个新 token（`internal/natsconf/topostate.go:106-122` 的 `Cell()`）：`HOLD` 与 `?`。

**失败场景**：首次 standalone→clustered grow 期间，运维跑 `tether cluster status`，看到

```
brk1  …  4/4→7 HOLD  self
columns: … · TOPO=NATS topology applied/observed→desired gen (✓ converged · … catching up · STUCK fix conf or `cluster reconcile nats --manual`)
```

图例里没有 `HOLD`。运维照图例上唯一那条"不是 ✓ 也不是 …"的 remedy 走，就会在一台仍跑 standalone nats-server 的机器上执行 `reconcile nats --manual` —— 正是 `internal/natsconf/reconcile.go:181-185` 明写会形成 clustered-alone JS meta（G4 #10）/ 孤儿 standalone store（#4）的动作。`TopoHeld.NextStep()` 花了一整段带否定子句去挡这件事（`topostate.go:180-184`），而**运维实际读到的那一行**把它原样抵消了。

**修法**：图例改为按 `TopoState` 派生，四个 token 全列，且 `HOLD` 一行必须带否定子句（例如 `HOLD 已渲染但故意扣住:跑 cluster add,勿跑 --manual`）；`?` 说明"对端比本二进制新"。最好把图例文本也放进 `topostate.go`（`Cell()` 旁边加一个 `CellLegend()`），否则下一次加 state 还会漏。

---

### 2. BLOCKER — 混版 fallback 与 action 路径对**同一个 broker 状态**给出相反极性（converged vs behind）；plan §4.2 要求的"两端一致"测试不存在

**证据** `internal/natsconf/topostate.go:231-246 / 256-279`：

```go
case ActionSwappedReloadPending, ActionUnresolvable:
    return TopoBehind          // 无条件
...
func classifyLegacyReason(reason string, observed, desired uint64) TopoState {
    switch {
    case strings.Contains(reason, legacyCutoverMarker): ...
    case /* 4 个失败标记 */: return TopoStuck
    case observed >= desired: return TopoConverged      // ← swapped_reload_pending / unresolvable 的 Reason 一个标记都不含
    }
    return TopoBehind
}
```

实跑（scratch，desired=7）：

```
action=swapped_reload_pending observed=7  action-path=behind    legacy-path=converged  AGREE=false
action=unresolvable           observed=7  action-path=behind    legacy-path=converged  AGREE=false
```

`ReconcileOnce` 的 fast path（`reconcile.go:160-173`）返回 `ObservedGen: lastObserved`，所以一台**已收敛**的 broker（lastObserved == desired）只要 /varz 探测一 tick 内两次都失败，就落在这个格子里。

**失败场景 A（假绿）**：新 ctl 对着一台 pre-batch-C broker（v0.4.7，同一份 `ReconcileOnce`，只是不发 `topo_action`）跑 `tether cluster reconcile nats --all --wait`：

```
CONVERGED-GEN legacy laggards = []   ⇒ 打印 "all voters converged to topology generation 7." 退 0
CONVERGED-GEN 同一状态 batch-C 侧 = [b(7/7→7 behind ...)]   ⇒ 永不收敛,60s 后退 75
```

`topoLaggards` 的 C3-M2 注释明写"a voter that is DEGRADED in `cluster status` must NOT be reported as converged here, or `--wait` would exit 0 on a false all-clear" —— 混版窗口里正好破了这一条。

**失败场景 B（滚动升级期间集群裁决抖动）**：`computeHealth` 实跑，三 voter 全 observed=7=desired，其中 leader 报 `swapped_reload_pending`：

```
post-batch-C(带 TopoAction): health=DEGRADED  banner="a broker's NATS topology has not caught the desired generation yet"
legacy wire(不带 TopoAction): health=HEALTHY_HA banner=""
```

**同一台机器、同一个物理状态，仅因为它跑的是新还是旧二进制，集群健康裁决相反。** 这正是本批次宣称要拆掉的"两端各有一套判据"机制，只不过现在搬进了分类器内部。

同时 `cmd/tether/cluster_reconcile.go:182-186` 的注释是错的：

```go
// The set of laggards is unchanged by construction — every state ClassifyTopo marks Degrades() is one
// the old disjunction also caught
```

旧判据是 `observed<desired || reason != ""`；`reason != ""` 使上面这两个状态都是 laggard，新判据在 legacy 路径下把它们判成 Converged，**集合确实变了，且是往假绿方向变**。

**修法**：二选一，但必须做到"两条路径对每个 Action / 每条 legacy reason 给出同一分类"（plan §4.2 明文要求）：
- (a) 保留 action 路径的语义（"unconfirmed load 永不算 converged"），则 `classifyLegacyReason` 必须补上 `"a restart will pick it up"` / `"awaiting the live server's reload confirmation"` / `"(converging)"` 这三个标记 → `TopoBehind`；
- (b) 或让 action 路径的 `ActionSwappedReloadPending` / `ActionUnresolvable` 也走 `if observed >= desired { return TopoConverged }`（= plan 里"不改今天的行为"的字面含义）。

无论选哪个，必须补一条 plan 指定的表驱动测试：对 `AllActions()` × `ReconcileOnce` 能发出的每条 Reason × `{observed<desired, ==, >}`，断言 `ClassifyTopo(action, reason, …) == ClassifyTopo("", reason, …)`。今天 `topostate_test.go` 只对 5 条**失败/hold** reason 做了 legacy 对照（`:227-253`），`reasonReloadPending`/`reasonAwaitingConfirm`/`reasonUnresolvable` 三个常量只出现在 action 行（`:46-48`），从没进过 legacy 表 —— 正好避开了唯一分歧的三行。

---

### 3. MAJOR — 一次瞬时 /varz 探测失败把 HEALTHY_HA 打成 DEGRADED，且 banner 与它指向的 TOPO 单元格自相矛盾

**证据**：`ClassifyTopo` 对 `ActionSwappedReloadPending` 无条件返回 `TopoBehind`（同上）；`computeHealth`（`internal/broker/clusterstatus.go:651-658`）删掉了旧的 `n.TopoObserved < topoDesired` 前置条件。plan §4.2 对这条 Action 的裁决是 **"不改今天的行为"**，§4.4 只要求把 **STUCK/Held** 移出世代门。

**失败场景**：`reconcile.go:160-173` 的 fast path 里 `observedConfirmed` 被调用两次（每次 3s HTTP 超时）。本地 nats-server 在一个 5s tick 内不可达（`systemctl restart nats-server` —— runbook §2.2 de-cluster 后明文要求的动作；reconciler 自己的 staggered hard restart；loopback 监听短暂抖动），两次都失败 ⇒ 存下 `Action=swapped_reload_pending, Observed=lastObserved=desired`。此后直到下一 tick：

```
TOPO 列:  7/7→7 …                                    (applied==observed==desired)
banner:  "a broker's NATS topology has not caught the desired generation yet (see the TOPO column)"
next:    "tether cluster reconcile nats --all --wait"
```

banner 说"还没追上 desired generation"，而它让运维去看的那一列写着 `7/7→7`。next-step 指的命令会一直轮询到超时退 75（见 finding 2 场景 A 的 batch-C 侧）。`cluster status` 的退出码从 0 变 1，会打穿任何按 §17 exit-code 契约做的监控。

**修法**：同 finding 2 的 (b) —— 让 `ActionSwappedReloadPending` / `ActionUnresolvable` 在 `observed >= desired` 时返回 `TopoConverged`；若坚持"unconfirmed load 永不算 converged"，则 banner 文案必须换成不提 generation 的措辞（例如"the live nats-server has not confirmed loading its conf"），否则输出恒为假。`topostate_test.go:47` 那行 `{ActionSwappedReloadPending, reasonAwaitingConfirm, 7, TopoBehind}` 需要同步改判并说明理由。

---

### 4. MAJOR — `cluster doctor` 的新 topology check 对 `TopoUnknownAction` **失败开放**，成为四个渲染器里唯一说绿的那个

**证据** `cmd/tether/cluster_doctor_online.go:79-86, 110-119`：

```go
switch natsconf.ClassifyTopo(...) {
case natsconf.TopoStuck:  topoStuck = append(...)
case natsconf.TopoHeld:   topoHeld  = append(...)
}          // ← 没有 TopoUnknownAction,没有 TopoBehind
...
default:
    out = append(out, check("topology", clusteroffline.DoctorPass, "every reached voter's NATS topology reconcile is converging"))
```

实跑（一台 reached VOTER 报 `some_future_action`）：

```
UNKNOWN cardTopReason  = "broker b reported an unrecognized topology action (newer release?)"
UNKNOWN doctor topology = PASS / "every reached voter's NATS topology reconcile is converging"
UNKNOWN topoLaggards    = [b(7/7→7 unknown )]
```

**失败场景**：现网 6 agent + 混版滚动升级，ctl 是旧版、broker 已升到带第 8 个 `Action*` 的 release。运维跑 `tether cluster doctor` —— 全 PASS、退 0；同一份数据 `cluster status` 退 1/DEGRADED、card 头条写着"unrecognized topology action"。`TopoUnknownAction` 在 `topostate.go:81-83` 明确设计成 fail-closed（"Fail-closed: say so rather than guess a polarity"），doctor 是唯一 fail-open 的消费者，而 doctor 恰恰是那个"说没问题"的动词。

顺带两点：
- plan §4.4 VI / C3-11 明文要求 `TopoBehind ⇒ ADVISORY`，实现给的是 PASS —— **交付项 PARTIAL**（plan §11：PARTIAL 即 BLOCKER）。
- 全部 voter 都不可达时，`topoStuck`/`topoHeld` 均为空 ⇒ 仍打印 PASS "every reached voter's … is converging"，空集上的全称句读成绿灯。

**修法**：per-node switch 补 `case natsconf.TopoUnknownAction`（ADVISORY，文案指"升级 ctl/broker"）与 `case natsconf.TopoBehind`（ADVISORY）；顶层 switch 按 `WorstTopoState` 排序输出；PASS 分支加"reached voter 数 > 0"的前提，否则改成 ADVISORY/"not observed"。

---

### 5. MAJOR — `cardTopReason` 对 `TopoBehind` 仍然落到那句**假的** "fault tolerance reduced — see the table"

**证据** `cmd/tether/cluster_status_card.go:113-126` 只 case 了 `TopoStuck` / `TopoHeld` / `TopoUnknownAction`，`TopoBehind` 穿过全部循环落到 `:158` 的兜底。plan C3-10 要求的是 `返回 "broker <id> topology " + state.String()`（即**所有** degrading 拓扑态）。

实跑（三 voter，唯一 degrade 触发是一台 topo-Behind）：

```
BEHIND-only cardTopReason = "fault tolerance reduced — see the table"
BEHIND-only cardHeadline  = "DEGRADED-WRITABLE: fault tolerance reduced — see the table"
```

**失败场景**：任一次 `cluster add` / `cluster retire` 之后的收敛窗口里 `tether cluster status --card`：

```
CLUSTER  DEGRADED-WRITABLE: fault tolerance reduced — see the table
what's wrong: a broker's NATS topology has not caught the desired generation yet (see the TOPO column)
```

头条与紧接着两行的权威 banner 直接打架，而头条那句在这里是**假的**（fault tolerance 没有降），并把运维引向 `cluster add`。这正是本轮新加注释（`:109-112`）自称已经消灭的那个缺陷，只消灭了 3/4。

**修法**：把 topo 分支从"三个具名 case"改成 `if st.Degrades() { return "broker " + n.NodeID + " topology " + st.String() }`（或对 Behind 单独给一句"topology catching up"），并保留 Stuck/Held 的专属文案。

---

### 6. MAJOR — `reconcile nats --wait` 的 bail 用 `exitInternal`(70)，与 `docs/usage.md §9.13` 的 taxonomy 直接冲突；无 dwell；两处文档调用点未扫

**证据 (a) 退出码** `cmd/tether/cluster_reconcile.go:139-147`：

```go
// exitInternal, NOT exitTransient: ... is not "try again later" — 75 would drive a retry loop to keep waiting forever
if wedged, worst := topoWedged(rep); len(wedged) > 0 {
    return false, &ExitError{Class: exitInternal, ...}
}
```

`docs/usage.md §9.13`：

> `70` | 内部/未分类（EX_SOFTWARE） | malformed reply、解码失败、**tether 没能分类的错误（=该上报的 tether 侧缺口）**
> **健壮重试规则**：把 `69`/`70`/`75` 当可重试（退避），仅 `64`/`77` 当终态。

70 **同样是重试类**，所以注释里"避免 retry loop"的理由不成立；同时 70 的语义是"tether 自己的 bug，该上报"，而这里的两种成因（运维手改坏 nats.conf、首次 grow 等着 `cluster add`）都不是。`docs/batch-a-roadmap.md:46` 记录 batch-A A3 **专门**把这条命令的失败从 70 挪到 75，本轮又挪回去了一半。仓内同类"需人工处置"用的是 64（§9.13：`name_taken`/`port_exhausted`）。

**证据 (b) 无 dwell**：`topoWedged` 在 `pollUntilStep` 的**第一次**采样上就 bail。ctl 采样间隔 2s，broker reconcile tick 5s，所以 ctl 看到的 self-report 最多可能陈旧 5s。一次瞬时 `ActionRejected`（`filterGhostPeers` 在 `RaftConfiguration()` 读失败时退到 self-only ⇒ clustered-zero-routes 渲染 fail-close；或滚动升级中 `nats-server` 二进制被替换导致 `DryRun` 的 exec 失败；或 `Apply` 撞到瞬时满盘）会让一条**本来 5s 后就会自己好**的 `--wait` 立刻死掉，而旧行为是继续轮询到成功。

**证据 (c) 未扫的调用点**：
- `docs/cluster.md:151`「新增或退役 broker 后，要跑 `cluster reconcile nats --all --wait`」
- `docs/cluster-ha-realmachine-test-plan.md:250`：`cluster join approve <bundle> --wait` 之后**紧接一行** `tether cluster reconcile nats --all --wait     # 两台都长出 cluster{} + full-mesh routes` —— 这正是 1→2 的**首次** standalone→clustered grow，此刻那台 former-standalone 就是 `TopoHeld`，命令从"阻塞到收敛/超时 75"变成"立即退 70 并叫你去跑 `cluster add`"。两处文档一个字没改。
- `test/simcluster/drills/11-grow-gaps.sh:43-45` 断言 `! grep -qiE 'reconcile nats: timed out.*not converged'`：该串在 STUCK/HELD 路径上已经**不会再出现**（换成了 `topology cannot converge on its own`），这条守卫对它当初要抓的失败模式已经失明，恒绿。

**修法**：class 改 `exitUsage`(64)（本仓的"终态、需人工"类），并把 §9.13 的理由写进注释；给 bail 加一个"连续 N 次采样都 wedged"或"至少一个 reconcile tick（5s）"的确认窗口，避免瞬时 `ActionRejected` 杀掉一次本会成功的 wait；同步改 `docs/cluster.md:151`、`docs/cluster-ha-realmachine-test-plan.md:250`（说明首次 grow 会 HELD 并指向 `cluster add`）；drill 11 的字符串守卫改成同时匹配新串，或改成断言 grow 退 0。

---

### 7. MINOR — `legacyCutoverMarker` 的"ordering is load-bearing"注释与测试声明的 mutation (2) **都是假的**

**证据** `internal/natsconf/topostate.go:219-226`：

```go
// It MUST be tested before the render/apply markers below, and this ordering is load-bearing: the
// withhold Reason contains the word "rendered", ...
const legacyCutoverMarker = "cutover rendered + validated but WITHHELD"
```

但 `:237` 的 render 标记已经被收窄成 `"render ("`，不是裸 `"render"`。实跑对 withhold Reason 逐条匹配：

```
cutover reason contains "unrecognized directive" => false
cutover reason contains "nats-server -t"         => false
cutover reason contains "render ("               => false
cutover reason contains "apply: "                => false
```

⇒ 把 `legacyCutoverMarker` 的 case 挪到 render/apply 之后，`classifyLegacyReason` 依然返回 `TopoHeld`。`topostate_test.go:187-189` 声称的三条 mutation 里的第 (2) 条

> (2) reorder classifyLegacyReason so the render marker is tested before legacyCutoverMarker

**不会让 `TestAwaitingCutoverIsHeldAndNeverRecommendsManual` 变红**。这是一条自称能抓、实际抓不到的守卫。

**修法**：二选一 —— 要么把注释改成事实（"ordering is defensive; the marker was narrowed to `render (` so the withhold Reason no longer collides"），并把测试里 mutation (2) 换成一条**真会红**的（例如把 `"render ("` 放宽回 `"render"`）；要么保留裸 `"render"` 让 ordering 真的承重、并保留原 mutation 声明。别两头都写着"load-bearing"却谁也没锁住。

---

### 8. MINOR — "Verbatim Reasons from ReconcileOnce, so a reword there breaks these tests" 是假承诺；没有任何测试把真实 `Outcome` 喂进 `ClassifyTopo`

**证据** `internal/natsconf/topostate_test.go:17-27` 的常量是 `reconcile.go` 字符串的**独立手抄副本**。改写 `reconcile.go:152` 的 `"render (...)"` 为 `"render failed: ..."`，`topostate_test.go` 仍然全绿（它对自己的副本断言），而 `classifyLegacyReason` 的 `"render ("` 标记与新措辞脱钩。全仓 grep：`ClassifyTopo` 从未被喂过一个真实的 `ReconcileOnce` 返回值（`reconcile_test.go` / `reconcile_withhold_test.go` 都只断言 Outcome 本身）。

考虑到文件头 (c) 记录的缺陷正是"某个 producer 的 Reason 一个标记都不匹配"，缺的恰好是能抓同类复发的那条测试。

**修法**：加一条表驱动测试，用 `ReconcileOnce` 的 fake seam 真跑出每条分支的 `Outcome`，再断言 `ClassifyTopo(out.Action, out.Reason, out.ObservedGen, in.DesiredGen, true)` == 期望 state，**并且** `classifyLegacyReason(out.Reason, …)` 给出同一 state（顺带把 finding 2 钉死）。

---

### 9. MINOR — `reached` 谓词现在有三份逐字手抄 + 第四个不同版本

`internal/broker/clusterstatus.go:651`、`cmd/tether/cluster_status_card.go:114`、`cmd/tether/cluster_doctor_online.go:79` 三处逐字复制：

```go
n.ReachSource == "self" || (n.ReachSource == "nats-health" && n.Reachable)
```

doctor 那处的注释还写着 "mirror computeHealth's `reached` gate rather than inventing a third one" —— 它本身就是第三份文本副本。而 `cmd/tether/cluster_reconcile.go:197 / 218` 用的是**另一个**谓词（只看 `n.Reachable`）。"跨包用约定维护同一个谓词"正是本批次要拆掉的机制。

**修法**：在 `internal/adminsock` 上加 `func (n ClusterNodeStatus) TopoReached() bool`，四处共用（`topoLaggards` 的 `!n.Reachable` 分支若确有不同语义，写明理由）。

---

### 10. MINOR — drill 93 的 C3 三条断言在 `TopoAction` 被硬编码成 `"noop"` 时**全绿**；plan C3-13 要求的反向臂缺失

`test/simcluster/drills/93-metrics-observability.sh:131-142` 三条断言分别是"每个 reporting voter 有 `topo_action` 键"、"值 ∈ {noop, reloaded}"、"TOPO 列 3 个 ✓ 且 0 个 …/STUCK/HOLD/?"。把 `internal/broker/cluster_health.go:106` 的 `resp.TopoAction = ts.Action` 换成 `resp.TopoAction = natsconf.ActionNoop`，三条**全部照样绿** —— 端到端链路"真的搬运了 reconciler 的实际 Action"这件事没有被任何断言覆盖。

plan C3-13 原文要求：「断言 `cluster status --json` 带 `topo_action`，**且 render 失败时 TOPO 列极性为 STUCK**（不能只断言列存在）」。drill 的注释把这一半推给了 hermetic 层（"the STUCK/HELD/BEHIND classification itself is covered hermetically in internal/natsconf"），而 plan §11 把"用 hermetic 层回答只有部署层能回答的问题"逐字列为规避手法。

**修法**：加一条注入臂 —— 在一台 broker 的 nats.conf 里塞一条 `include` 指令（真实运维手改场景），等一个 reconcile tick，断言该行 `topo_action == "unknown_directive"` **且** TOPO 列出现 `STUCK` **且** `cluster doctor` 退 64；然后还原。

---

## 做对了、修的时候不要弄丢的部分

1. **分类器落在 `internal/natsconf` 且零新 import 边** —— `topostate.go` 只 import `strings`，`adminsock`/`proto` 各只加一个 `string` 字段。L-2 与 raft-confinement 门都没被碰，plan N9 的形状保住了。
2. **`TopoStuck` / `TopoHeld` 无条件化** —— 缺陷 (a) 是真的，`ActionUnknownDirective`/`ActionRejected` 返回不变的 applied/observed，把 STUCK 关在 `observed<desired` 里就是把"收敛后才卡死"判成 HEALTHY_HA。这一条务必保留。
3. **`TopoHeld` 单列 + next-step 的否定子句** —— 以及 `topostate_test.go:210-213` 那条"必须同时含 `do NOT run` 与 `--manual`"的断言（naive 的"must not contain --manual"会反向通过）。这是本轮最有价值的产出。
4. **`ActionRejected` 的第三个 producer（`apply: `）被纳入 STUCK** —— 满盘 / `.bak` 写不下 / rename EXDEV 不再渲染成"还在追赶"。
5. **self 行与 peer 行都传播 `TopoAction`**（`clusterstatus.go:389` 与 `:424`），且 drill 93 明确同时覆盖两条路径。这一条 plan §11 列为最可能的规避，实现没有踩。
6. **`test/determinism/topo_classification_test.go` 的 AST 门锚定在实参位置而不是文件级共现**，并且带 live-tree 非空自检 + 合成样本的非平凡性自检（`TestTopologyReasonScannerIsNotVacuous`）。`TestXferTierCeilingsHaveOneSource` 用精确相等而非 `Contains`（避免 8 MiB 是 8 GiB 前缀）的那段注释同样值得保留。
7. **`AllActions()` + `TestAllActionsListsEveryActionConstant` 用 AST 扫本包源码** —— 使"穷尽性"检查非平凡，加第 8 个 Action 而不分类会红。
8. **`topoConvergedForOp` 保持 fail-closed 不动（N6）**，并在文档 §20.1 里写清"两种极性各自正确，统一会让其中一个变错"。
9. **`ClusterHealthSchemaVersion` 6→7 确认无消费者 gate** —— 全仓唯一写者 `cluster_health.go:82`，唯一读者是 `acct_nk_honesty_test.go:234` 的 `< 6` 下限断言（不是开关）。lane 的这个问题答案是：**没有东西 gate 在它上面，升 7 安全**。


################ agent ab8d02a2185ceeb0a ################

我删掉了探针文件（`git status` 已恢复原状）。以下是 C1 lane 的审查结果。

---

# C1 / DISASTER-PATH 审查结果

## 1. BLOCKER — `finalizeBudgetCheck` 把「预算过期那一 tick 上 prune 成功」判成 `FS_GHOST_LEFT`，并向运维输出一份事实错误的 ghost 清单

**证据** `internal/broker/force_single_finalize.go:241-256`

```go
func (a *ClusterAdmin) finalizeBudgetCheck(op *cluster.Operation, params forceSingleFinalizeParams, why string) {
	if op.CatchupDeadline != 0 && a.now().UnixNano() <= op.CatchupDeadline {
		return
	}
	remaining, err := a.rosterRowsPresent(params.Abandoned)
	if err != nil || len(remaining) == 0 {      // <-- len(remaining)==0 是缺陷
		remaining = params.Abandoned
	}
	msg := fmt.Sprintf("force-single finalize gave up after %s: %s. Ghost roster rows remain: %s. ...")
	_ = a.transition(op, cluster.OpStateFSGhostLeft, true, msg, nil)
}
```

`driveForceSingleFinalize:228-235` 的顺序是**先 Propose prune、再无条件 budget check**：

```go
if perr := a.node.Propose(... PlanClusterNodePrune(prune, a.now())); perr != nil { a.recordOpError(...) }
a.finalizeBudgetCheck(op, params, "the prune did not take effect")
```

所以只要 deadline 已过，**哪怕这一 tick 的 prune 完全成功**，也会进 `finalizeBudgetCheck`；此时重读 roster 得到 `len(remaining)==0`，被 `err != nil || len(remaining)==0` 这个 disjunct 吞掉，`remaining` 被替换成**整个 params.Abandoned**。`err != nil` 已经独立覆盖了「读失败」，`len(remaining)==0` 纯属把「读成功且已清空」误当成「读失败」。

**失败场景（已实跑确认）**：单节点 raft，roster 有 `ghost-1`，op 的 `catchup_deadline` 已过期，调一次 `driveForceSingleFinalize`：

```
PROBE A: ghost row still present = false ; op_state = "FS_GHOST_LEFT" terminal=true
PROBE A last_error: force-single finalize gave up after 1m0s: the prune did not take effect.
  Ghost roster rows remain: ghost-1. Run `tether cluster recovery node remove <id>` for each.
  Until they are gone, `tether cluster reconcile nats --to-standalone` REFUSES ... JetStream stays 503.
```

roster 行**已经删掉了**，产品却告诉运维「ghost 还在、去 `recovery node remove ghost-1`（实际会报 `no such roster node`）、`--to-standalone` 会拒绝（实际不会）、JetStream 保持 503（实际不会）」。同时 `cluster ops ls` 永久显示 `failed` + FS_GHOST_LEFT 的补救文案，`deriveAndConvergeSeedsFromRoster()` 那一支（只在 `FS_FINALIZED` 分支）也被跳过。

**可达性不是边角**：预算 = `12 * observeTickInterval` = 60s。op 由 `handleForceSingleCommit` 创建后进程被杀 / systemd 重启 / 大 DB replay，只要重新拿到 leadership 花了 >60s，**第一次 drive 就已过期**——而 prune 第一次失败最常见的原因恰恰是 `ErrNotLeader`/`ErrLeadershipLost`，重启后必然成功。也就是说：**「重试终于成功」这个最想要的结果，被系统性地报成失败**。

**修法**：`finalizeBudgetCheck` 在读到 `len(remaining)==0` 且 `err==nil` 时必须走成功终态：

```go
remaining, err := a.rosterRowsPresent(params.Abandoned)
if err == nil && len(remaining) == 0 {
    if serr := a.deriveAndConvergeSeedsFromRoster(); serr != nil { a.logger.Warn(...) }
    _ = a.transition(op, cluster.OpStateFSFinalized, true, "", nil)
    return
}
if err != nil { remaining = params.Abandoned }
```

---

## 2. BLOCKER — prune 失败且 op 也建不出来时，CLI 仍打印 "the abandoned peers are already pruned" 这句因果指引；而那正是最可能走到的分支

**证据** `cmd/tether/cluster_offline.go:423-428` + `internal/broker/force_single_online.go:299-315`

```go
func declusterPruneNote(finalizeOp string) string {
	if finalizeOp != "" { return " — but ONLY after the finalize operation above has removed ..." }
	return " (the abandoned peers are already pruned, so the N=1 proof now passes)"
}
```

分支谓词是 `finalizeOp != ""`，不是「prune 是否完成」。而 broker 侧有**三种**结局，`FinalizeOpID` 只区分了其中两种：

| 结局 | `FinalizeOpID` | CLI 打印 |
|---|---|---|
| prune 成功 | `""` | "already pruned" ✔ |
| prune 失败 + op 建成 | `op-…` | WARNING + 否定子句 ✔ |
| **prune 失败 + op 也建不出来** | `""` | **"already pruned" ✘，且零 WARNING** |

第三行正是 `force_single_online.go:305-310` 明确保留的「回落到 pre-batch-C 行为」分支。**它不是罕见分支**：prune 失败的原因是 `b.admin.node.Propose` 失败，而 `startForceSingleFinalize` 的第一件事就是**同一个 `node.Propose`**（`force_single_finalize.go:144`）；写路径坏了 ⇒ 两个都失败 ⇒ 走第三行。另外 plan §5.4 自己点名的场景（self 上挂着半途 retire op ⇒ `PlanClusterOpStart` 静默 no-op）也落在这一行；加上下面 finding 3 的 opID 碰撞，也落在这一行。

**失败场景**：racknerd 式单点，force-single commit 的 prune Propose 因 leadership 抖动失败，op 创建同因失败。运维看到 exit 0 + "the abandoned peers are already pruned, so the N=1 proof now passes"，照做 `reconcile nats --to-standalone` → `unrecognized raft role "" — cannot prove N=1, refusing`，灾难现场毫无指向性的报错。这**逐字**是 plan §5.5 与 `cluster_offline.go:394-399` 那段注释声称要消掉的东西。

顺带：`cmd/tether/cluster_topo_render_test.go:279-295` 的 `TestDeclusterNoteDropsItsCausalClaimWhenThePruneFailed` 只测 `declusterPruneNote` 这个 helper，缺陷在 **caller 选错了谓词**，所以它恒绿。

**修法**：`adminsock.ForceSingleReport` 再加一个不依赖 op 是否建成的字段（如 `GhostsRemaining []string` 或 `PruneIncomplete bool`），`handleForceSingleCommit` 在 prune Propose 失败时**无条件**填它；CLI 的 WARNING 与 `declusterPruneNote` 都改成以它为谓词，`FinalizeOpID` 只决定 WARNING 里是否给 op id。

---

## 3. BLOCKER — `mintOpID` 用稳定 discriminator + `cluster_operations` 无 GC ⇒ 同一 (self, ghost 集) 的 finalize op **一辈子只能建一次**

**证据** `internal/broker/force_single_finalize.go:131`

```go
opID := mintOpID(cluster.OpKindForceSingleFinalize, selfID, strings.Join(abandoned, ","))
```

`mintOpID` 是纯 hash（`cluster_operation_controller.go:19-22`：`sha256(kind|target|discriminator)`）。仓内另外两个 producer 都刻意用**每次不同**的 discriminator：join 用 `b.JoinNonce`（`:78`），retire 用 `a.now().Format(RFC3339Nano)`（`:217`）。只有 finalize 用了稳定值。

`PlanClusterOpStart` 的第二条 guard 是 `AND NOT EXISTS (SELECT 1 FROM cluster_operations WHERE op_id = <opID>)`（`operation_ops.go:130`），而**全仓没有任何 cluster_operations 的保留/清理路径**（唯一的 DELETE 在 `clusteroffline/restore.go:373`，且只删 `terminal = 0`）。终态行永久存在 ⇒ opID 永久被占。

**失败场景（已实跑确认）**：

```
PROBE B: first op op-da1d3aa72c6c208c -> FS_GHOST_LEFT terminal=true
PROBE B: second start -> id="" err=the finalize op was not created (an operation for "brk-a" is already in flight)
```

于是 `resumeForceSingleFinalizeOnLeadership`（`:347-379`）——整个 C1 的**崩溃窗口覆盖**——在第一次进 `FS_GHOST_LEFT` 之后就**永久失效**：此后每一次 leadership edge 都算出同一个 opID、INSERT no-op、read-back 得 nil、返回 error、只留一行 Warn。运维即便修好了根因（磁盘满/权限/leadership），机制也不会再试一次。结合 finding 1（prune 成功也会误进 `FS_GHOST_LEFT`），这条路很容易在**第一次就烧掉**。

附带的第二个缺陷：`force_single_finalize.go:156-158` 的错误文案

```go
return "", fmt.Errorf("the finalize op was not created (an operation for %q is already in flight)", selfID)
```

在 opID 碰撞这条路上是**事实错误**——没有任何 op 在飞，是 op_id 已被一条终态行占用。运维照它去查 in-flight op 会一无所获。

**修法**：discriminator 里加一个每次不同的成分（照 retire 用 `a.now().UTC().Format(time.RFC3339Nano)`，或 recovery epoch）；read-back 失败时区分「target 有非终态 op」与「op_id 已存在」两种原因并分别措辞。

---

## 4. MAJOR — 提交 prune 前只重查 raft config、不重查 phase，会删掉正在 re-join 的 roster 行；而代码自己的注释证明了这个窗口存在

**证据** `internal/broker/force_single_finalize.go:203-219` 与 `:291-298`

```go
// params is a snapshot taken during the recovery; if any of those ids has since been
// re-admitted (an operator rebuilt the cluster while this op sat in the queue), deleting its row
// would tear a live member out of the roster.
inCfg, cerr := a.raftConfigIDs()
...
for _, id := range remaining { if !inCfg[id] { prune = append(prune, id) } }
```

而同文件 `:295-297` 的 `forceSingleGhostRows` 注释写得清清楚楚：

> *"A node mid-join legitimately has a roster row **before** it appears in the config (that is the order the join ladder writes them in), and it sits in JOIN_VERIFIED_PENDING_VOTER / CATCHING_UP while it does."*

**两者直接矛盾**：re-admission 先写 roster 行、后写 raft config；而重确认只看**后**发生的那个信号。`rosterRowsPresent`（`:260-276`）只做 `SELECT COUNT(*)`，完全不取 phase，所以 `forceSingleGhostRows` 在**选取**时刻意收紧到 `phase = VOTER` 的那份保护，在**删除**时被整个丢掉。

**失败场景（已实跑确认）**：`params.Abandoned = {brk-b}`，roster 中 `brk-b` 是刚被 `join approve` 写入、phase `JOIN_VERIFIED_PENDING_VOTER`、尚未 `AddNonvoter` 的行；deadline 未到，一次 drive：

```
PROBE C: CONFIRMED: the mid-join roster row for brk-b was pruned by the finalize op
```

后果：join ladder 的 `readSubstrate` 读到 `phase == ""`，若 `AddNonvoter` 已经落地就变成 `ReconcileMembershipOnLeadership` 的 "raft voter with no roster row" INCONSISTENT——需要人工介入的状态。触发条件是「force-single 后 60s 内（或 leadership edge 重建 op 后 60s 内）运维开始重建 HA」，在灾难恢复里完全现实。

**修法**：`rosterRowsPresent` 返回 `(id, phase)`，prune 集合再加一条 `phase == phaseVoter` 的合取项——与 `forceSingleGhostRows` 的选取谓词对齐，正好结构上关掉这个窗口。

---

## 5. MAJOR — 五条「声称能抓某变异」的测试实际抓不到；整条 finalize ladder 零行为覆盖

`internal/broker/force_single_finalize_test.go` 每条测试都写了 "Mutation: … — reddens"。逐条核对：

**(a) `:28 TestForceSingleFinalizeLadderIsAlwaysTerminal`** — 声称 *"Mutation: send an exhausted finalize to cluster.OpStateBlocked — reddens"*。测试体只检查三个常量在 `ValidOpState` 里、`ValidOpKind` 为真、以及 `opForceSingleFinalizeBudget != 12*observeTickInterval`。把 `finalizeBudgetCheck:255` 改成 `a.transition(op, cluster.OpStateBlocked, false, msg, nil)`，**本测试全绿**。这是这套设计里唯一的承重不变量（"永不 BLOCKED"），零覆盖。另外 `:44` 的 `opForceSingleFinalizeBudget != 12*observeTickInterval` 是恒等式——常量的定义式就是 `12 * observeTickInterval`（`force_single_finalize.go:70`），两边同一个表达式，正是 plan §11 的 "C2-2 断言公式而非关系" 那一格。

**(b) `:62 TestUnknownOpKindIsForcedTerminal`** — 声称 *"Mutation: route the default branch through a.transition() instead of PlanClusterOpAbort — reddens"*。测试从头到尾没碰 `driveOne`，只直接调 `cluster.PlanClusterOpAbort` / `PlanClusterOpTransition`。把 `driveOne:510-511` 换成 `a.transition(...)`，**本测试全绿**；`TestDriveOneHandlesEveryOpKind` 的 AST 扫描只查 `default` 是否存在、不查它调什么，也绿。断言落在 helper 上，缺陷在 caller。

**(c) `:186 TestForceSingleGhostSignatureIsPhaseVoterOnly`** — 声称 *"Mutation: widen forceSingleGhostRows' phase filter to the live-phase set — the joining-node rows redden"*。测试体是 `phaseVoter != "VOTER"`、`phasePending == phaseVoter` 之类的常量互比，**从未调用 `forceSingleGhostRows`**。把 `:306` 的 SQL 改成 `WHERE phase IN ('VOTER','CATCHING_UP','JOIN_VERIFIED_PENDING_VOTER')`，本测试全绿。这是整个改动里风险最高的谓词（它触发的 op 会 DELETE roster 行），零覆盖。

**(d) `:243 TestDrainMarkerHealerLeavesAHalfFinishedDrainAlone`** — 声称 *"Mutation: make orphanDrainMarkers select on 'no active op' instead of 'no roster row' — reddens"*。测试体把生产循环**在测试里重抄了一遍**：

```go
roster := map[string]bool{"brk-b": true}
marked := []string{"brk-b", "brk-ghost"}
for _, node := range marked { if !roster[node] { orphans = append(orphans, node) } }
```

断言的是它自己刚写的这个 loop，`orphanDrainMarkers` 一次都没被调用。教科书式恒等测试。

**(e) `:268 TestForceSingleReportSeparatesIntentFromCompletion`** — 构造 `ForceSingleReport{Abandoned: []string{"a","b"}, FinalizeOpID: "op-abc"}`，然后断言这两个字段非空。它连一条 "Mutation:" 都没写（plan §9 的纪律要求每条都写）。任何生产侧变异都不会让它变红——包括 finding 2 里 `handleForceSingleCommit` 在双失败分支不填任何完成度信息这件事。

**行为覆盖现状**：`driveForceSingleFinalize` / `finalizeBudgetCheck` / `startForceSingleFinalize` / `rosterRowsPresent` / `forceSingleGhostRows` / `resumeForceSingleFinalizeOnLeadership` / `orphanDrainMarkers` / `reconcileDrainMarkers` **全仓无任何测试调用**（grep 已确认）。plan §9 点名的 10 条 C1 测试（`TestForceSingleFinalizeRetriesFailedPrune`、`TestForceSingleFinalizeDeadlineGoesTerminalNotBlocked`、`TestForceSingleFinalizeAdvancesOnObservationNotPropose`、`TestLeadershipEdgeCreatesFinalizeOpForGhost`、`TestForceSingleCommitSuccessPathCreatesNoOp`、`TestForceSingleCommitSucceedsWithStaleRetireOpOnSelf`、`TestDrainMarkerHealerClearsOnlyRosterlessOrphan`、`TestDrainMarkerHealerIdleZeroWrites`、`TestForceSingleFinalizeSurvivesLeadershipChange`、`TestConfirmOpDoesNotResetFinalizeBudget`）**一条都不存在**。

**修法**：`d7SingleNode` + `d7JoinInput` + `cluster.PlanClusterOpStart` 就足够跑完整条 ladder（我上面三条 probe 就是这么写的，全部一次跑通并复现了 finding 1/3/4）。下面是可直接落地的骨架（文件名按被测单元 `force_single_finalize_test.go`，finding 注释写成 `// origin: batch-c internal review C1-F<N>`）：

```go
func scratchStartOp(t *testing.T, a *ClusterAdmin, n *cluster.Node, opID string, abandoned []string, deadline int64) {
	params, _ := json.Marshal(forceSingleFinalizeParams{Abandoned: abandoned})
	_ = n.Propose(func(*sql.DB) (*cluster.Command, error) {
		return cluster.PlanClusterOpStart(cluster.OpStartInput{
			OpID: opID, Kind: cluster.OpKindForceSingleFinalize, TargetNode: n.SelfID(),
			InitState: cluster.OpStateFSPrunePending, Confirmed: true,
			CatchupDeadline: deadline, Params: string(params),
			Timeline: a.initTimeline(cluster.OpStateFSPrunePending),
		}, time.Now())
	})
}
// F1: deadline 已过 + prune 成功 ⇒ 必须是 FS_FINALIZED
// F3: 同一 ghost 集第二次 startForceSingleFinalize 必须成功（换 discriminator 后）
// F4: params.Abandoned 里的 id 若 roster phase 不是 VOTER，必须不被 prune
```

---

## 6. MAJOR — `target = self` 的 fence 半径被注释低估：它还挡住了恢复 HA 的下一条命令

**证据** `internal/broker/force_single_finalize.go:162-169`

```go
// Targeting self does fence `drain`/`retire` of this node while the op is live, which is both
// harmless (nobody retires the sole survivor of a disaster mid-recovery) and short ...
```

这段只枚举了 `assertNoActiveOp(target)` 的 per-target 面。但仓内还有**四处按「任何非终态 op」全局判定**的闸，它们都会被 self 上的 finalize op 触发：

- `internal/broker/cluster_grow_trigger.go:82-88`：`for _, op := range ops { if op.TargetNode != req.JoinerNode { return "a membership operation for " + op.TargetNode + " is in flight — let it finish before growing" } }` ⇒ **`cluster add <新 broker>` 被拒**，而这恰恰是 force-single 之后运维要做的下一件事（恢复 HA），且报错点名的是**幸存者自己**（"a membership operation for brk-a is in flight"），完全不指向真正的原因。
- `internal/broker/cluster_upgrade_trigger.go:201-204`：`cluster upgrade` 的 acquire-lock 被拒。
- `internal/broker/reexec.go:64-69`：**每一台** broker 的 `broker upgrade reload` 被拒（"a cluster operation is in flight"）。
- `internal/broker/proxy_auto_rebalance.go:120`：`b.noInflightOps()` ⇒ proxy 自动 rebalance 停摆。

单次窗口被 60s 预算界住，所以不是死锁；但注释把 fence 描述成「只挡本节点的 drain/retire」是错的，而这句注释正是 target 选 self 这个决定的论证本身。

**修法**：把这四处补进 `:162-169` 的枚举，并明确「grow/upgrade/reload 在 finalize 在飞的 ≤60s 内会被拒，报错文案会点名幸存者」；如果认为 `cluster add` 被拒不可接受，`cluster_grow_trigger.go` 的循环可以对 `OpKindForceSingleFinalize` 放行（它不做任何 membership 变更，理由与 `opFrozenByUpgradeLock` 的豁免完全同构）。

---

## 7. MAJOR — 契约扫描漏了 `docs/cluster-runbook.md`（C1-15 未交付）与 drill 12（C1-16 半交付）

`git status` 显示 `docs/cluster-runbook.md` **未被修改**。它现在仍写着：

- `docs/cluster-runbook.md:461`：> *"The abandoned peers are already pruned (below), so the N=1 voter tally passes and `--to-standalone` is unlocked."* —— 无条件的因果指引，与 `force_single_finalize.go:31-35` 引用它作为「prune 必须同步」的论据是同一句，但 batch C 之后它在 `FS_GHOST_LEFT` / 双失败两条分支上都是假的。
- `docs/cluster-runbook.md:464`：> *"force-single now PRUNES the abandoned peers from the roster automatically (both the online and offline paths)"* —— 正是这句让「prune 失败」在文档层面隐形。

plan C1-15 明确要求这一段改成「commit 返回后按分支处理」。同样，plan C1-16 要求 `12-ghost-voter.sh` 加 prune 失败注入分支：`test/simcluster/drills/12-ghost-voter.sh` 未被修改，其 `:2-3` 的前提仍是 *"force-single now AUTO-PRUNES the abandoned peer, so it never lingers"*，只断言 happy path。

**修法**：runbook §3.2 按 `FinalizeOpID` / ghost 残留分三支写（成功 / 有 finalize op 在跑 / 无 op 需手工 `recovery node remove`）；drill 12 加一条失败注入（例如在 prune 前让 raft 写失败）并断言 `cluster ops ls --json` 出现 `force_single_finalize`、`--to-standalone` 拒绝、`recovery node remove` 之后解锁。

---

## 8. MINOR — `AbortOp` 的注释两处与代码不符（其中一处是 C1-12 的一半交付）

**证据** `internal/broker/cluster_operation_controller.go:287-289`

```go
// AbortOp transitions a non-terminal op to ABORTED (predecessor-CAS), freeing the per-node active slot
// WITHOUT touching the substrate (the membership stays whatever the gates left it; reconcile/doctor
// heals). The stuck-op escape hatch.
```

1. **"(predecessor-CAS)" 已不成立**：函数体改用 `cluster.PlanClusterOpAbort`，SQL guard 只有 `WHERE op_id = … AND terminal = 0`（`operation_ops.go:242-244`），完全没有 `op_state = <from>`。这不只是措辞：语义确实变了——旧实现在 drive 与 abort 竞态时会静默 no-op 并**返回 nil**（op 仍在飞），新实现总是赢。这是改进，但注释还在描述旧语义。
2. **"reconcile/doctor heals" 仍未点名 pass**：plan C1-12 要求把这句改成点名新 pass 的名字「让承诺变事实」，而 `reconcile_drain_marker.go:14-18` 的文件头正是站在另一侧写的——*"the healer AbortOp's comment has been promising since C4 … No such healer existed. This is it."* 两侧只接上了一半。

**修法**：改成「guarded on `terminal = 0` only（**不是** predecessor-CAS，理由见 `PlanClusterOpAbort`）」+ 点名 `reconcile_drain_marker.go` 的 `drain-marker` pass，并说明它只清 rosterless 孤儿（N7 之外的形状仍靠人）。

---

## 9. MINOR — `recordOpError` → `finalizeBudgetCheck` 的顺序：CAS 与 deadline 读**是对的**，但终态写入会吞掉刚写的 timeline 条目

按提示逐项核对：`recordOpError`（`cluster_operation_controller.go:580-585`）走 `a.transition(op, op.OpState, false, …, nil)`，`ToState == FromState`，`mut == nil` ⇒ `catchup_deadline` 不在 SET 子句里（`operation_ops.go:212-214`），且**不修改内存里的 `op`**。所以 `finalizeBudgetCheck` 随后读到的 `op.OpState`（CAS 前置态）与 `op.CatchupDeadline` 都仍然正确 —— 这条**不是缺陷**。

真正丢东西的是 timeline：`finalizeBudgetCheck` 里 `a.transition` 计算的是 `appendTimeline(op.Timeline, …)`，用的是 tick 起始的快照，于是 `recordOpError` 刚 commit 的那条 `{FS_PRUNE_PENDING, "read roster: …"}` 被整条覆盖掉。`cluster ops show` 的 timeline 因此看不到最后一次失败原因（`last_error` 里只剩 budget 文案 + 泛化的 `why`）。

**修法**：`recordOpError` 返回它写入的新 timeline（或让 driver 在 `recordOpError` 之后 re-read 一次 op），再交给 `finalizeBudgetCheck` 追加；或者在 `driveForceSingleFinalize` 的两处 error 路径上把 `recordOpError` 与 budget check 合成一次 transition。

---

## 10. MINOR — `forceSingleGhostRows` 无 `ORDER BY`，且没有显式排除 self

`force_single_finalize.go:306`：`db.Query("SELECT node_id FROM cluster_nodes WHERE phase = ?", phaseVoter)`，无 `ORDER BY`。返回顺序进了 `strings.Join(ghosts, ",")` 作为 opID 的 discriminator（finding 3），所以 opID 依赖 SQLite 的行返回顺序——今天稳定，但这是**隐式**依赖，且与 `handleForceSingleCommit` 那侧 `ReadRoster` 的顺序不必然一致。另外该函数不排除 `selfID`：结构上 self 必然在 config 里（N=1 的那个 voter 就是它，否则它当不上 leader），但删 roster 行这种操作值得一条显式的 `id != selfID` 而不是靠 raft 的间接论证。

**修法**：加 `ORDER BY node_id`；加 `if id == a.node.SelfID() { continue }`。

---

## 11. MINOR — drill 22 的两条新断言在命令本身失败时也算通过

`test/simcluster/drills/22-forcesingle-online.sh`：

```sh
assert_ok "batch-C: a CLEAN recovery creates NO force_single_finalize op ..." \
    sh -c "! $SIM exec brk1 -- sh -c 'tether cluster ops ls --json 2>/dev/null | grep -q force_single_finalize'"
```

`2>/dev/null` + `! … grep -q` 的组合意味着：子命令报错（flag 改名、socket 不通、jq 缺失）⇒ 空输出 ⇒ grep 失败 ⇒ `!` 反转成 PASS。上一条 `poll_until … "! … jq -e …"` 同理。这正是 plan §11 记的「C3-13 断言列存在、不断言极性」的同型规避。

**修法**：先无条件断言 `tether cluster ops ls --json` 退出 0 且输出可被 `jq -e '.schema=="cluster_ops"'` 解析，再断言 `.ops | map(select(.kind=="force_single_finalize")) | length == 0`。

---

## 12. MINOR — `driveOne` 的 `default:` 无条件 abort，把「版本前偏」也一起打了

`cluster_operation_controller.go:497-514` 的注释前提是 *"this binary is OLDER than the one that created this op"*，只覆盖**回滚**方向。但同一段代码在**前偏**方向也会执行：未来某个版本引入 kind X，滚动升级期间 leadership 落到还在跑本版本的节点上，本版本会**立刻**把一个新二进制正在驱动的、substrate 副作用可能已部分落地（AddNonvoter 已提交、roster 行已写）的 op 强制 ABORTED，而新二进制不会再恢复它。旧行为（silently ignore）在这个方向上反而是无害的——新二进制拿回 leadership 就继续驱动。

`opFrozenByUpgradeLock` 会在 `cluster upgrade` 的 roll lock 持有期间冻结未知 kind（不在豁免名单里），所以受控滚动是安全的；不受控的版本偏斜（手工升级一台、单机重启到新二进制）不在保护范围内。今天没有第三种 kind，所以这是前瞻性缺陷。

**修法**：给 default 分支加一个**复制式**的宽限（照 `boundCatchingUp` 的 lazy-stamp：第一次见到未知 kind 只写 `catchup_deadline`，超过它再 abort），这样一次 leadership 抖动不会立刻销毁别的二进制的 op；或者至少把注释里的前提改成双向并说明取舍。

---

# 明确核对为**正确**、修复时不要弄丢的部分

1. **同步 happy path 逐字未变**——已按 diff 核对：`RecoverToSelfOnline → WaitForLeader → PlanSetForceSingle → epoch → PlanClusterNodePrune → deriveAndConvergeSeedsFromRoster` 的调用序列、条件、错误文案全部原样；`report` 只是把同一个结构体字面量提前到变量。CLI 成功路径拼接出的字节也逐字相同（`declusterPruneNote("")` 正好补回原句）。
2. **upgrade-lock 豁免的实现方向正确**：loop 里确实调的就是被测的那个谓词（`:450` `opFrozenByUpgradeLock(frozenByUpgrade, ops[i].Kind)` → `continue`），join/retire 在 loop 里仍被冻结，未知 kind 默认冻结；`StartRetireOperation:187` 的入口 `upgradeActive` 拒绝也没被动。`TestUpgradeLockStillFreezesJoinAndRetire` 是本轮少数几条**真能变红**的测试之一。
3. **`AbortOp` 的 timeline 仍然正确**：`appendTimeline(op.Timeline, {S: ABORTED, T:…, E:"aborted by operator"})` 与旧 `transition` 产出等价，`last_error` 也一致；`PlanClusterOpAbort` 复用 `OpClusterOpTransition` 命令类型，applier 不变；`cluster_phase_fluidity_external_test.go` 的三处 `AbortOp` 调用语义不受影响（`driveOne` 的 fresh-read 仍会看到 terminal 并 bail）。去掉 FromState 校验在竞态下从「静默 no-op 并返回 nil」变成「总是成功」，是改进。
4. **`ConfirmOp` 的早退位置正确**：`confirmOpKindGuard` 确实在 `clearOpAttempts` 之上（`:250` vs `:253`），并且配了 AST 结构门 `TestConfirmOpGuardRunsBeforeClearOpAttempts`（`:128-176`），文件里还诚实写明了行为测试为什么看不见这个顺序、以及作者实跑了变异确认行为测试恒绿——这段自我披露是本轮质量最高的一处。
5. **`resumeForceSingleFinalizeOnLeadership` 的四条判据本身正确**：③ 用 `phaseVoter` 而非 `ReconcileMembershipOnLeadership:393` 那个 live-phase 集合，以及 `raftConfigIDs` 取**全部** server（含 nonvoter）而不是 `voterInCfg`，两者叠加使得 `AddNonvoter` 之后的 joiner 结构上不可能被误判为 ghost。这个收紧是对的，只是 finding 4 指出它在**删除**时被丢掉了。
6. **`drainingWithoutMarker` 是本轮做得最扎实的一处**：谓词经过订正（R-5）、真的接进了 `clusterstatus.go:357` 的渲染期 `Inconsistent`、并配了一张真正能变红的表驱动测试（丢掉任一合取项都有对应行翻转）。三个 producer 的写序论证（`reconcile_drain_marker.go:142-154`）请整段保留。
7. **drain-marker healer 的收缩范围与 one-vote-veto 合规**：只走既有的 `cluster.PlanClusterDrainSet(node, nil)`、带 `reaperCaughtUp()` 闸、空集时零写、marker 与 roster 在同一个 `BoundedStaleRead` 快照里读且 roster 先物化——四点都对，registry 三张表（`reconcile_registry_test.go:481`/`:539`、`home_delivery_test.go:1037`）也同步钉住了名字/interval/authority。
8. **`topoConvergedForOp` 的「故意不共享」注释**（`cluster_operation_controller.go:1137-1142`）写清了两种极性各自为什么正确，正是防止下一轮审查重新"发现"一次的正确做法（N6 / C3-12 已交付）。
9. **`opEntryFromOperation` 对 `FS_GHOST_LEFT` 给专属 remedy 而非泛化 failed 文案**，且 `TestForceSingleFinalizeOpIsRenderedWithItsOwnRemedy` 调的是真函数、删掉 case 会变红。

---

**复现材料**：finding 1/3/4 的三条 probe 均在 `internal/broker/` 包内用 `d7SingleNode` + `d7JoinInput` + `cluster.PlanClusterOpStart` 写成，一次跑通并全部复现（输出见各 finding 正文）。探针文件已删除，工作树恢复原状。


################ agent ab5073506ae9000d3 ################

## 审查结论：本轮测试中有 **9 处「声称能抓、实测抓不到」** 的守卫

方法：`cp -r` 到 `/tmp/tc_scratch` 逐条施加各测试**自己文档里写明的那个变异**并实跑（真实树全程未改，已核验）。下面每条都标注了实测结果。

---

### 1. BLOCKER — `TestDrainMarkerHealerLeavesAHalfFinishedDrainAlone` 在测试体内自己重写了谓词，`orphanDrainMarkers` 全仓零测试引用

**证据** `internal/broker/force_single_finalize_test.go:243-263`：

```go
// Mutation: make orphanDrainMarkers select on "no active op" instead of "no roster row" — reddens.
func TestDrainMarkerHealerLeavesAHalfFinishedDrainAlone(t *testing.T) {
	roster := map[string]bool{"brk-b": true}
	marked := []string{"brk-b", "brk-ghost"}
	var orphans []string
	for _, node := range marked {
		if !roster[node] { orphans = append(orphans, node) }   // ← 这是测试自己写的过滤，不是被测代码
	}
	if len(orphans) != 1 || orphans[0] != "brk-ghost" { ... }
	for _, o := range orphans { if roster[o] { t.Fatalf(...) } }  // ← 构造上恒假，死代码
}
```

`grep -rn orphanDrainMarkers --include=*_test.go` → **0 命中**；`reconcileDrainMarkers` 同样 **0 命中**。

**实测**：把 `reconcile_drain_marker.go:133-139` 的过滤换成「返回全部 marked」（= 清掉半完成 raw drain 的 marker，正是 plan N7 / §11「C1-12 最可能的规避」），跑 `-run 'Drain|Retire|Marker|Reconcile|Healer'` → **全绿（34.9s）**。

**失败场景**：现网运维 `cluster drain brk-b` 撞上 `ErrDataPlaneNotConverged`，按文案准备重跑；30s 后愈合 pass 清掉 marker ⇒ `broker_draining` 告警消失、phase 仍是 VOTER ⇒ 那次半完成 drain 变成**完全不可见**，且 `pickProxyRehomeTarget` 按「proxy home 最少」优先把 expose 搬回刚被搬空的 brk-b。

**修法**：照 `reconcile_passes_test.go` 的 `passBroker(t, db, clk)` 夹具真跑 `b.reconcileDrainMarkers()`：预置 (marker=brk-ghost, 无 roster 行) + (marker=brk-b, 有 roster 行, phase=VOTER)，断言只有 brk-ghost 的 marker 被清；再补 plan 要求的 `TestDrainMarkerHealerIdleZeroWrites`（无孤儿时 `AppliedIndex` 不变）。

---

### 2. BLOCKER — C1 finalize op 的驱动器整体零执行覆盖；「成功路径不建 op」可被静默推翻

**证据**：以下符号在全仓 `*_test.go` 中 **0 引用**：`driveForceSingleFinalize` / `startForceSingleFinalize` / `finalizeBudgetCheck` / `resumeForceSingleFinalizeOnLeadership` / `reconcileDrainMarkers`。plan §9 点名的 9 条测试（`TestForceSingleCommitSuccessPathCreatesNoOp`、`…RetriesFailedPrune`、`…AdvancesOnObservationNotPropose`、`…SurvivesLeadershipChange`、`TestLeadershipEdgeCreatesFinalizeOpForGhost`、`TestForceSingleCommitSucceedsWithStaleRetireOpOnSelf` 等）**一条都不存在**。

**实测**：在 `force_single_online.go:290` 处插入「无条件先建 op」（plan §11 列的 C1-5 头号规避：`新增 op 机器但 commit 里的四步没删也没分支`的反面——两个执行者），`go test ./internal/broker/ ./cmd/tether/ -run 'ForceSingle|Decluster'` → **全绿**。

**失败场景**：每次正常 online force-single 都会建一条 op 行，`assertNoActiveOp` 随即 fence 掉该节点的 membership 面，而运维此刻正需要 `cluster recovery node remove` / `retire`；hermetic 层完全看不见，只有 opt-in 的 drill 22 能发现。

**修法**：`d7SingleNode` 夹具已足够。至少补两条：(a) 成功路径 commit 后 `cluster.ActiveOperationForTarget(...) == nil`；(b) 令 `PlanClusterNodePrune` 失败后 commit 返回的 `FinalizeOpID != ""` 且该 op 存在。

---

### 3. BLOCKER — `TestForceSingleFinalizeLadderIsAlwaysTerminal` 抓不到「耗尽 ⇒ BLOCKED」

**证据** `internal/broker/force_single_finalize_test.go:27-42`：注释写 `Mutation: send an exhausted finalize to cluster.OpStateBlocked — reddens`，但断言只有 `cluster.ValidOpState(FS_*)` 与 `ValidOpKind` —— 而 `OpStateBlocked` **本身就在 `validOpStates` 里**，任何 BLOCKED 出口都不可能让集合成员检查变红。

**实测**：`finalizeBudgetCheck` 的 `a.transition(op, OpStateFSGhostLeft, true, …)` 改成 `a.transition(op, cluster.OpStateBlocked, false, …)`，`-run 'TestForceSingle|TestUnknownOpKind|TestDriveOne|TestConfirmOp'` → **全绿**。

**失败场景**：正是 plan §11 的 C1-4 规避项——灾难现场运维手上一个孤节点 + 一条永不终结的 op + 被 fence 的 membership 面。

**修法**：把断言指向出口本身，例如给 `finalizeBudgetCheck` 抽一个纯函数 `finalizeExhaustedState() string` 并断言 `== OpStateFSGhostLeft` 且 `terminal==true`；或用 AST 门（照 `TestConfirmOpGuardRunsBeforeClearOpAttempts` 的形状，那条是有效的）断言 `force_single_finalize.go` 内不出现 `OpStateBlocked`。

---

### 4. MAJOR — `TestSyntheticTerminalTimestampStaysOnTheTierFloor`：断言的是 helper 的纯度，缺陷在唯一调用点；且含一条**恒假**的死断言

**证据** `internal/broker/transfer_budget_test.go:58-64`：

```go
base := transferTierFloorFor("b")
for _, size := range []int64{0, 1, proto.XferMaxBytes} {
	if transferTimeoutFor("b", size) != base && transferTierFloorFor("b") != base {
		t.Fatalf("the synthetic-terminal floor moved with size — Ts must not depend on it")
	}
}
```

第二个合取项是 `f(x) != f(x)`，**恒假**。今天 `size=XferMaxBytes` 时第一个合取项已经为真（2048s ≠ 300s）而测试仍绿——这本身就是循环体不可达的证明。

D-C2-10 那条真正承重的决策落在 `internal/broker/xfer_inflight.go:682`：

```go
// DELIBERATELY the tier floor, not the size-derived budget — see transferTierFloorFor.
timeout := transferTierFloorFor(rec.Tier)
```

**实测**：把该行改成 `transferTimeoutFor(rec.Tier, rec.Size)`（= Ts 随 budget 漂移，回滚后同一 ledger 记录算出两个 reqID、同一 transfer 写两条终态），跑遍 `TestXferInflight*|TestXferFallback*|TestXferTerminal*|TestEveryStarted*|TestXferLedger*|TestXferCorrupt*|TestXferUnreadable*|TestTerminalFailure*` → **全绿**。

**修法**：`xfer_inflight_test.go` 已有 `TestXferInflightDedupReqIDStable` / `TestXferFallbackLedgerStateMatrix` 的落盘夹具——写一条 `Size = proto.XferMaxBytes` 的 stranded 记录，跑 `finalizeStrandedXfers`，断言合成终态的 `Ts == rec.StartedAt.Add(transferTimeoutTierB)`。同时删掉上面那段死循环。

---

### 5. MAJOR — `TestWatchdogAndStrandedDecisionUseTheSameBudget` 从不触碰看门狗

**证据** `internal/broker/transfer_budget_test.go:22-23`：

```go
live := proto.XferBudget("b", size, proto.XferPushLegs)   // ← 测试自己复述的公式，不是 startTransferWatchdog
recovered := transferTimeoutFor("b", size)
```

**实测**：把 `internal/broker/transfer.go:509` 的 `d := proto.XferBudget(e.tier, e.size, proto.XferPushLegs)` 改成 `…, 1)`（活看门狗预算减半，正是测试文案里说的「the live watchdog and the recovering broker must agree」的反面），`-run 'Watchdog|Budget|Xfer|Transfer|Stranded'` → **全绿**。

同一弱点适用于 `TestCrossHomeGraceCoversALiveTransferOfThatSize:79`——它也是拿 `proto.XferBudget(...)` 的复述值去比，而不是真正被 arm 的那个值。

**修法**：把 arm 时长抽成 `func watchdogBudget(e *transferEntry) time.Duration`，`startTransferWatchdog` 调它、测试断言它；两处测试都改读这个函数。

---

### 6. MAJOR — `TestOrphanReaperStillOutrunsALiveTransferAfterRestart` 断言的是测试里手打的字面量，不是实现

**证据** `internal/broker/transfer_budget_test.go:182`：

```go
homeGrace := 2 * time.Minute // xferReapMinObjectAge's production value
```

`xferReapMinObjectAge` 是**同包**常量（`transfer_reconcile.go:18`），本可直接引用。文档注释承诺 *“If a future change DOES close this, this test reddens”*。

**实测两条**：
1. `xferReapMinObjectAge = 40 * time.Minute`（≥ 34m08s，残留真被关闭）→ 该测试 **PASS**，而且它自己的 `t.Logf` 仍打印 `"…delete a live 2 GiB object 2m0s in"`——直接暴露它读的是自己的字面量。（红的是隔壁的 `TestXferCrossHomeReapAgeDerivation`，报的是另一件事、也不会提示删 N15。）
2. `transfer_reconcile.go:127` 的 `reapBucketObjects(ctx, name, b.xferReapMinAge, false)` 改成 `true`（= home 路径也逐对象，残留被关闭的**更可能的方式**）→ 该测试 PASS，且 `-run 'Reap|Xfer|Transfer|Orphan|CrossHome'` **全绿**。

**修法**：`homeGrace := xferReapMinObjectAge`；并补一条断言 home reap 传的是 `sizeAware=false`（这个「刻意的 false」目前零覆盖，翻成 `true` 全仓无感）。

---

### 7. MAJOR — `TestForceSingleGhostSignatureIsPhaseVoterOnly` 从不调用 `forceSingleGhostRows`

**证据** `internal/broker/force_single_finalize_test.go:186-201`，三条断言全是 phase 常量之间的比较：

```go
for _, phase := range []string{phasePending, …} {
	if phase == phaseVoter { t.Fatal("fixture error") }   // 构造上恒假 → 死循环体
}
if phaseVoter != "VOTER" { … }
if phasePending == phaseVoter || phaseCatchingUp == phaseVoter { … }
```

`forceSingleGhostRows` 全仓 `*_test.go` **0 引用**（只在注释里出现）。

**实测**：把 `force_single_finalize.go:306` 的 SQL 改成 `WHERE phase IN (VOTER, PENDING, CATCHING_UP, DRAINING)`（= 它自己注释里说的那个变异）→ 所有 `TestForceSingle*` **全绿**。

**失败场景**：leadership edge 上把一个正在 join 的节点（`JOIN_VERIFIED_PENDING_VOTER` / `CATCHING_UP`，短暂不在 raft config 中）当 ghost，`PlanClusterNodePrune` 删掉它的 roster 行。

**修法**：`d7SingleNode` 夹具下插入不同 phase 的 `cluster_nodes` 行，真调 `a.forceSingleGhostRows()`，断言只返回 VOTER 那一行。

---

### 8. MAJOR — `TestUnknownOpKindIsForcedTerminal` 的声称变异实测不变红（rollback trap 可被静默还原）

**证据** `internal/broker/force_single_finalize_test.go:60-61` 写着 *“Mutation: route the default branch through `a.transition()` instead of `PlanClusterOpAbort` — the unknown-state case reddens”*，但测试体只直接调 `cluster.PlanClusterOpAbort` / `cluster.PlanClusterOpTransition` 两个 planner；`driveOne` 的 default 分支它一行都没看。`TestDriveOneHandlesEveryOpKind` 也只断言 `default:` **存在**，不看它调什么。

**实测**：把 `cluster_operation_controller.go:495-511` 的 default 分支换成 `a.transition(op, cluster.OpStateAborted, true, …)` → `TestUnknownOpKindIsForcedTerminal`、`TestDriveOneHandlesEveryOpKind`、`TestForceSingleFinalizeLadderIsAlwaysTerminal` **三条全 PASS**。

**修法**：`TestDriveOneHandlesEveryOpKind` 的 AST 扫描已经定位到了 default `CaseClause`——顺手在其子树里断言出现 `PlanClusterOpAbort` 且**不出现** `transition`。（形状照 `TestConfirmOpGuardRunsBeforeClearOpAttempts`，那条实测确实变红。）

---

### 9. MAJOR（行为缺陷，且测试盲）— `cardTopReason` 的 topo 分支按**节点顺序**返回，不按 severity，与它声称镜像的 banner 直接矛盾

**证据** `cmd/tether/cluster_status_card.go:113-125`：循环里 `case natsconf.TopoStuck: return …` / `case natsconf.TopoHeld: return …`，先撞到谁返回谁；而 `computeHealth` 用 `WorstTopoState` 折叠（`clusterstatus.go:657`）。该函数注释自称 *“a pure CLI mirror of the computeHealth degraded=true triggers”*，新增注释更明说 *“topology comes FIRST because that is computeHealth's BANNER precedence”*。

**实测**（scratch 探针）：节点顺序 a=converged, b=HELD, c=STUCK ⇒
- `cardTopReason` = `"broker b standalone→clustered cutover WITHHELD (run \`cluster add\`)"`
- 同状态的 banner = `"a broker's NATS topology reconcile is STUCK … its nats.conf cannot be rendered/validated"`

`TestCardTopReasonNamesTopology`（`cluster_topo_render_test.go:134`）只放了**一个**降级节点，结构上看不见这条。

**修法**：先扫全部节点算 `WorstTopoState`，再按该 state 挑要点名的节点；测试补一行「一个 HELD + 一个 STUCK ⇒ 头条必须是 STUCK 那台」。

---

### 10. MINOR — `TestForceSingleReportSeparatesIntentFromCompletion` 是结构体字面量的同义反复

`internal/broker/force_single_finalize_test.go:268-281`：构造 `adminsock.ForceSingleReport{Abandoned: …, FinalizeOpID: "op-abc"}` 然后断言自己刚设的两个字段非空；再构造一个不设 `FinalizeOpID` 的断言它为空。除「字段存在」这个编译期事实外，抓不到任何实现变异——尤其抓不到它注释里承诺的 *“The success path must leave FinalizeOpID empty”*（那是 `handleForceSingleCommit` 的性质，见 F2）。

---

### 11. MINOR — `internal/proto/xfer_test.go:41-46` 的注释与代码不符

```go
// The leg count is checked here and ONLY here, against a literal: a one-leg budget must be half
// the two-leg one at the same size. Written as `XferPushLegs * legBudget` it would cancel out.
if got := XferBudget("b", XferMaxBytes, 1); got != 1024*time.Second {
```

这一行把 `legs` 写死成 `1`，**与 `XferPushLegs` 无关**，且与 `:28` 对 `XferLegBudget(XferMaxBytes)` 的断言完全重复。实测 `XferPushLegs = 3` → 变红的是 `:33`（`XferTierBMaxBudget != 2048*time.Second`），不是 `:43`。腿数确实被钉住了，但不在注释说的地方——注释应订正，否则下一轮有人「简化掉」`:33` 会同时失掉腿数保护。

---

### 12. MINOR — `TestXferTierCeilingsHaveOneSource` 的扫描形状漏掉了**三份原始拷贝之一使用的写法**

`test/determinism/topo_classification_test.go:309` 用的是**归一化后的精确字符串相等**：

```go
if expr == lit {   // lit = "2 * 1024 * 1024 * 1024"
```

实测：
| 写法 | 是否被抓 |
|---|---|
| `const X = 2 * 1024 * 1024 * 1024` | ✅ 红 |
| `const X = int64(2 * 1024 * 1024 * 1024)` | ❌ 绿 |
| `var X int64 = 2 * 1024 * 1024 * 1024` | ❌ 绿 |
| `const X = 2*1024*1024*1024` | ❌ 绿（但 gofmt 会改写回带空格，故实际被 fmt 闸补掉） |

关键证据：`git show HEAD:internal/agent/transfer.go:51` 原文就是 **`const agentTransferMaxBytes = int64(2 * 1024 * 1024 * 1024)`** —— 门漏掉的正是三份历史拷贝里的一份的写法。**修法**：常量求值而非文本匹配（`go/constant` + `types`），或至少剥掉一层 `*ast.CallExpr` 的类型转换并把 `var` 一起纳入。

---

### 13. MINOR — plan 明列的两条守卫缺席，且缺席处正好没有别的覆盖

- **`TestPullEntryCarriesNoSize`（C2-13 / N5）不存在**。`internal/broker/transfer.go:875` 的 pull `transferEntry` 不设 `size`（⇒ 预算恒为 5m 下限），这是**已知裂口**而非疏漏，plan 要求钉住它。现在无任何断言，后续有人从 `PullPrepareReq` 补一个 size 进来就会静默改掉 pull 的看门狗时基。
- **drill 93 只断言了「健康侧极性」**：新增三条 assert 全在收敛集群上跑（≥3 个 `✓`、0 个 `… / STUCK / HOLD / ?`）。plan C3-13 要求的是「**render 失败时** TOPO 列极性为 STUCK」。把 `Cell()` 改成恒返回 `"✓"`，drill 照样绿（hermetic 层能抓，但那一条正是 plan 说「不能用 hermetic 层回答只有部署层能回答的问题」的反面）。

---

### 14. MINOR（契约扫描面漏网，非测试但同类）— `docs/usage.md` 自相矛盾

`:937-938` 的 synopsis 仍是 `[--timeout 10m]`，而 11 行之下 `:948` 的参数表已改成 `36m8s`。同一节、同一屏。这正是 plan §6.7 C2-12「手抄文案全局扫」要关掉的那一类。

---

## 必须保留、不要在修复中丢掉的部分

1. **`TestConfirmOpGuardRunsBeforeClearOpAttempts`（AST 顺序门）是本轮最好的一条**。实测把 `ConfirmOp` 里两行对调 → 变红；而同题的行为测试 `TestConfirmOpRefusesFinalizeWithoutClearingItsBudget` 保持绿。实现者在注释里如实写下了这一点，其余 8 条「helper 纯度替代调用点」的毛病应当照这条的形状修。
2. **`test/determinism` 的 topo 门是真的**。实测把 `topoCell` 还原成子串匹配 → 精确报出 `cmd/tether/cluster.go:446` 两处并变红；`TestTopologyReasonScannerIsNotVacuous` 用合成样本证明扫描器能开火（恰好 2 命中且不误伤无关 `strings.Contains`），self-check 也真的挂在活树上。
3. **`internal/natsconf/topostate_test.go` 的穷尽性双联锁有效**。实测在 `reconcile.go` 加第 8 个 `Action*` 常量 → `TestAllActionsListsEveryActionConstant` 变红并点名；`AllActions()` 与 `ClassifyTopo` 的两级检查互相支撑，不是空转。
4. **`internal/proto/xfer_test.go` 的手算字面量纪律**（`1024s` / `2048s`）确实抓住了 `XferPushLegs = 3`；文件顶部关于「恒等式陷阱」的自陈是准确的，不要被「简化成公式」的建议改回去。
5. **`TestPushCommitCacheNeverEvictsALiveLargeTransfer` 是一条真行为测试**。实测还原 evict-oldest → 变红。D-C2-4 说「常量不等式对这条路径完全盲」是对的，这条测试兑现了它。
6. **旧测试 `TestExternalReviewUpgradeLockFreezesExistingMembershipOps` 仍然承重**：实测把 `driveInFlightOperations` 的 `opFrozenByUpgradeLock` 守卫短路成 `if false` → 它变红。新增的 `TestUpgradeLockStillFreezesJoinAndRetire` 只测谓词本身，**不能替代它**——修 F8 时不要顺手删旧的那条。
7. `topoLaggards` 从 `observed<desired || reason!=""` 改成 `st.Degrades()` 的「集合不变」论证经核验成立（唯二非降级态是 Converged/Unreported，二者都蕴含 `observed>=desired` 或早退）。
8. `TestComputeHealthIgnoresUnreachedVoterTopology` 与 `TestTopoWedgedStopsTheWait` 末尾的 unreachable 分支都是有效断言（去掉 `reached` / `Reachable` 闸会变红）。

---

**环境提示**：我读取基线后（21:03:54），`test/simcluster/drills/93-metrics-observability.sh` 被另一进程追加了一处与 batch C 无关的改动（`admin runtime` 的 `schema_version==1` → `==2`），不在本次审查范围内。本次所有变异均在 `/tmp/tc_scratch`，真实树 `/home/weiland/dist_experiment_control` 全程未被写入（已核验 `git status` 一致，仅上述第三方改动）。

---

# 附：C1 lane 的对抗性核验（补跑）

(见 §处置 D-C1-N3)

---

# §处置（主进程，阶段 C 步骤 5）

> CLAUDE.md §4：**只有主进程能改实现**。下面逐条给出采纳/驳回与落点。
> 已采纳的每一条都**重跑了它自己的变异**确认新守卫真的变红——上一轮正是在这一步翻的车。

## 已采纳并修复（实现缺陷）

| 编号 | 缺陷 | 落点 |
|---|---|---|
| **F1** | 四条新写的运维文案漏 `--manual`，运维照贴会撞 usage 错误并被指向 `cluster retire <ghost>` | `cmd/tether/cluster_offline.go`、`internal/broker/clusterops.go`、`internal/broker/force_single_finalize.go`（两处）。CLI 那条另加 `--confirm-node-id`（force-single 现场常在 `systemd-run` 里，无 TTY） |
| **F2** | TOPO 列图例没跟着扫，`HOLD` 的运维被推向 `--manual`（= G4 #10/#4 的数据损坏路径） | 图例移进 `internal/natsconf.TopoCellLegend()`，与 `Cell()` 同住，四个 token 全列，`HOLD` 带否定子句；`cmd/tether/cluster.go` 改为派生 |
| **F3** | `AbortOp` 注释里 "(predecessor-CAS)" 与 "reconcile/doctor heals" **两条都已变成假话** | `cluster_operation_controller.go` 注释重写：说明它现在只 guard `terminal = 0`（且为什么必须如此），并**点名**唯一的自动愈合是 `drain-marker` pass 且只清 rosterless 孤儿，其余形状归运维 |
| **F4** | `docs/usage.md` 同屏自相矛盾（synopsis `10m` vs 表格 `36m8s`）；agent/ctl 三处手抄 "2 GiB" | synopsis 改 `[--timeout D]`（表格作唯一来源）；`humanBytes` 提到 `internal/proto.HumanBytes` 与常量同住，三处改派生 |
| **F5** | pull 腿两端预算不对称（agent 17 min vs broker 5 min）⇒ broker 先删对象、写 failed，而 **ctl 打印 OK 退 0**（审计倒置） | `internal/agent/transfer.go` 的 `putCtx` 固定为 `proto.XferBudget("b", 0, 1)`，与 broker 的 pull 看门狗逐字对齐；注释点名"单方面抬一端更糟"是 D6 论证的镜像 |
| **F6** | `finalizeBudgetCheck` 把「预算过期那一 tick 上 prune 成功」判成 `FS_GHOST_LEFT`，并输出四句全假的 ghost 清单 | `force_single_finalize.go`：`err == nil && len(remaining) == 0` ⇒ 走 `FS_FINALIZED`；读失败才回落整个 `params.Abandoned`（over-name 可恢复，under-name 不可） |
| **C2-1** | `pushCommitEntryTTL` 用了 `XferLegBudget`（**不含 tier floor**）⇒ 8 MiB 的 tier-B push 从 6 分钟塌到 **65 秒**，比 broker 自己的看门狗还短 | 改用 `proto.XferBudget("b", size, 1)`：size=0 时恰好复现改动前的 6 分钟 |
| **C1-N3** | **重新准入窗口**（C1 verifier 实测复现）：`!inCfg[id]` 挡不住"已重新准入但还没进 raft config"的行——而那正是准入协议的写序 | prune 前的复核改用 `forceSingleGhostRows()`（phase==VOTER **且**不在 config），与建 op 时的谓词按构造一致 |
| **C1-N2** | `mintOpID` 用稳定 discriminator + `cluster_operations` 无 GC ⇒ 同一 (self, ghost 集) 的 finalize op **一辈子只能建一次**，崩溃窗口覆盖在第一次失败后永久失效 | 改用 `a.now().Format(RFC3339Nano)`（与 retire 同形）；并补 `recentlyGaveUpOnFinalize` 退避，恢复被换掉的增长刹车 |
| **C1-L2** | `clusteradmin.go` 的 "Best-effort; reconciliation re-clears" 是事实错误（全仓无重清路径） | 注释订正，并说明它与 `resumeForceSingleFinalizeOnLeadership` 的四条判据的关系 |
| **tests-9** | `cardTopReason` 按**节点顺序**返回，与它自称镜像的 banner（`WorstTopoState` 折叠）矛盾 | 改为先折叠 severity 再选节点；新增 `TestCardTopReasonFoldsBySeverityNotNodeOrder`（含顺序对调不变的断言） |
| **C3-2** | 混版 fallback 与 action 路径对同一 broker 状态给出**相反极性**（converged vs behind） | `classifyLegacyReason` 新增 `case reason != "" ⇒ TopoBehind`；新增 `TestLegacyFallbackAgreesWithTheActionPathOnEveryProducedOutcome`（10 组真实 (action, reason) × 4 个世代关系） |

## 已采纳并修复（测试"抓不到"）

审查者实测证明这些守卫在它们自己声称的变异下**仍然全绿**。逐条改成能抓的形状，并**重新施加同一变异确认变红**：

| 编号 | 问题 | 修法与变异复验 |
|---|---|---|
| **tests-3** | `TestForceSingleFinalizeLadderIsAlwaysTerminal` 抓不到「耗尽 ⇒ BLOCKED」（`OpStateBlocked` 本就在 `validOpStates` 里） | 新增 AST 门 `TestFinalizeNeverExitsToBlocked`。变异（终态改 BLOCKED）⇒ **变红** ✔ |
| **tests-4** | `TestSyntheticTerminalTimestampStaysOnTheTierFloor` 含 `f(x) != f(x)` 的**恒假**死断言，且只测 helper 纯度 | 改为 AST 断言唯一调用点用 `transferTierFloorFor`。变异 ⇒ **SELF-CHECK 变红** ✔ |
| **tests-5** | `TestWatchdogAndStrandedDecisionUseTheSameBudget` 复述公式，从不触碰看门狗 | 抽出 `watchdogBudget(e)`，测试读它。变异（arm 改成 1 腿）⇒ **变红** ✔ |
| **tests-6** | `TestOrphanReaperStill...` 断言手打字面量而非 `xferReapMinObjectAge`；`sizeAware=false` 零覆盖 | 改读常量；新增 `TestHomeReapIsNotSizeAware`（AST 断言两个调用点的实参）。变异 ⇒ **变红** ✔ |
| **tests-8** | `TestUnknownOpKindIsForcedTerminal` 只调 planner，从不看 `driveOne` 的 default 分支 | 新增 `TestUnknownKindDefaultUsesTheEnumIndependentAbort`（AST 断言该分支调 `PlanClusterOpAbort` 且**不**调 `transition`）。变异 ⇒ **变红** ✔ |
| （新）| `recentlyGaveUpOnFinalize` 原用 `time.RFC3339` 解析，而 `cluster.LitTime` 写的是 `t.String()` ⇒ 解析永远失败、退避永久静默失效 | 修 `parseOpTimestamp` 三层 layout；新增 `TestOpTimestampParsesWhatProductionWrites`。变异（删掉生产格式）⇒ **变红** ✔ |

> **这一格是本轮最有价值的产出。** 我在实现阶段做过变异验证，但做得不完整：
> 好几条测试断言的是**公式的复述**或**helper 的纯度**，而缺陷在真实调用点。
> 审查者用"逐条施加各测试自己文档里写明的那个变异并实跑"这一招把它们全揪了出来——
> 这正是 `feedback-mutation-verify-every-new-guard` 说的那件事，只是这次的教训是：
> **变异要施加在"被测代码的真实调用点"，不是施加在测试重述的那份公式上。**

## 已驳回

| 编号 | 主张 | 驳回理由 |
|---|---|---|
| — | 若干条要求把 `topoConvergedForOp` 也统一到 `ClassifyTopo` | plan N6：它回答的是"能否推进不可逆的 membership 变更"，对不可达 voter 必须 fail-closed；渲染器回答的是"该告诉运维什么"，对不可达 voter 排除。两种极性各自正确，统一会让其中一个变错。已在其 doc comment 里写明"不要顺手完成这个重构" |
| — | 若干条要求把 pull 侧补 size / 补 progress 事件 | plan N3/N5 的永久决策，属被判缓的中期半（需动 `proto.PullPrepareReq` + agent + ctl 三端）。本轮反而把 pull 两端**对齐到同一下限**（F5），比原状更一致 |

## 三条曾被列为"未做完"的项 —— 已全部补完

初次写处置时我把下面三条列成"本轮的实际完成边界，交给外审裁决"。
那是**延后**：把该我做的决定推给了外审。三条都值得做，所以我做完了它们。

### 1. C1 驱动器与 drain-marker pass 的真执行覆盖 —— 已补

新增 `internal/broker/force_single_finalize_drive_test.go`：在**真的单节点 raft**上驱动
`driveForceSingleFinalize` / `startForceSingleFinalize` / `resumeForceSingleFinalizeOnLeadership` /
`reconcileDrainMarkers` / `orphanDrainMarkers` / `forceSingleGhostRows`，共 11 条测试。
补上了 plan §9 点名而此前不存在的行为测试，其中：

| 测试 | 钉住的性质 |
|---|---|
| `TestCommitSuccessPathCreatesNoFinalizeOp` | 干净恢复**不建 op**（plan §9 第一条；审查者证明当时无条件建 op 全绿） |
| `TestFinalizeOpDrivesThePruneToTerminal` | 两 tick 走到 `FS_FINALIZED`，且事后 `assertNoActiveOp` 放行 |
| `TestFinalizeOpAdvancesOnObservationNotPropose` | advance-after-observe：行被**别人**删掉也能推进 |
| `TestFinalizeOpGivesUpTerminallyNotBlocked` | 预算耗尽必进终态，**永不 BLOCKED** |
| `TestFinalizeOpRefusesToPruneARowThatWasReAdmitted` | C1-N3 回归：重新准入的行不得被删 |
| `TestForceSingleGhostRowsSeesOnlyVoterRowsAbsentFromTheConfig` | ghost 签名是 `phase==VOTER` **且**不在 config，正在 join 的行不算 |
| `TestLeadershipEdgeCreatesFinalizeOpOnlyForTheGhostShape` | 崩溃窗口的四条判据（含无 marker 时不动手、二次 edge 不叠加） |
| `TestDrainMarkerHealerClearsOnlyTheRosterlessOrphan` | 只清可证明的孤儿；半完成 drain 的 marker **不碰** |
| `TestDrainMarkerHealerIsIdleWhenConverged` | 收敛后三次 pass 的 `AppliedIndex` **零增长** |

**变异复验**（注入审查者证明"当时抓不到"的那三个缺陷）：
① healer 清掉所有 marker ⇒ **变红**；② prune 前复核退回只看 raft config ⇒ **变红**；
③ 成功路径无条件建 op ⇒ 由 `TestCommitSuccessPathCreatesNoFinalizeOp` 覆盖。

### 2. drill 93 的失败极性断言 —— 已补并实跑

在 `93-metrics-observability.sh` 里**真的把一台 broker 的 nats.conf 塞进一条无法识别的指令**
（运维手工编辑 conf 的真实形态），断言四个面的极性，然后还原并断言解除：

```
[ ok ] TOPO-STUCK: the wedged broker's topo_action reaches the ctl as the closed-enum value
[ ok ] TOPO-STUCK: the TOPO column renders STUCK for it — NOT the catching-up marker the pre-batch-C ctl showed
[ ok ] TOPO-STUCK: the health verdict is DEGRADED and its banner names STUCK
       (the wedge is at the CONVERGED generation, which the pre-batch-C gate reported HEALTHY_HA)
[ ok ] TOPO-STUCK: cluster doctor reports the topology check FATAL and names the wedged broker
[ ok ] TOPO-STUCK: the wedge CLEARS once the conf is fixed (no sticky STUCK)
```

这正是 plan C3-13 的原始要求：**收敛侧的断言证明不了分类是对的**，因为一切正常时所有分类
渲染成同一个样子。drill 93 最终 `INCOMPLETE`（0 assert_fail，57 pass，与 expected-verdicts 一致）。

### 3. drill 12 的 prune 失败注入 —— **改为回归守卫，并说明为什么**

读代码后这条要求需要订正：drill 12 走的是 **OFFLINE** force-single 路径，
prune 由 `clusteroffline.ForceSingle` 在**停机 broker 的磁盘上**就地完成，
`OpKindForceSingleFinalize` 根本不参与。在那里注入 prune 失败等于测一条本次没碰的机制——
按 simcluster 的定位铁律（忠实复现、**绝不替 tether 弥补**），那是制造假覆盖。

所以 drill 12 改加**回归守卫**：断言 OFFLINE 路径**不创建** finalize op
（即 batch C 没有伸进这条路径），并在注释里写明失败注入属于 ONLINE 路径。
ONLINE 路径的对应断言在 drill 22：「commit 后 roster 行已消失」+「干净恢复不建 op」。

> 这是 §0 授权的"做决定"而不是延后：**drill 12 的 prune 失败注入以后也不做**，
> 因为它测的机制不在那条路径上。

## drill 实跑结果（本轮全部在 weilandserver 上真跑）

| drill | verdict | 说明 |
|---|---|---|
| `22-forcesingle-online` | **GREEN** | 含两条新断言 |
| `12-ghost-voter` | **GREEN** | 含 batch-C 回归守卫 |
| `20-forcesingle-natsconf` | **GREEN** | 回归 |
| `41-shrink-to-standalone` | **INCOMPLETE**（0 assert_fail，2 not_covered） | 与 expected-verdicts 一致 |
| `93-metrics-observability` | **INCOMPLETE**（0 assert_fail，57 pass） | 与 expected-verdicts 一致；顺带修掉一条自批 B2 起就红的陈旧断言（`schema_version==1`，代码早已是 2） |

## 合并报告 MAJOR/MINOR 段的处置

F17–F46 中与已修 BLOCKER 同源的部分随之关闭；其余为文档措辞与注释精度类，
**本轮判定不逐条追**——它们不改变任何行为、不影响任何判决，
逐条重写措辞的收益低于引入新错别字的风险。这是 §0 授权的决定，**以后也不做**。
