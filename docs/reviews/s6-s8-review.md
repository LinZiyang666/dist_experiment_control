# `docs/reviews/s6-s8-review.md` — G-B (S6+S8) Stage-C 内审报告

> CLAUDE.md §3 step 4 交付物。对抗内审 workflow（6 reviewer + 6 verifier + 1 synth，全 Opus 4.8，run `wf_cac80c24-ad5`）产出；mandate-fidelity（"擦屁股"）lane 为核心。**主进程逐条裁决见下 §0，专家报告全文见 §1+（synth）。**

## §0 主进程裁决（step 5：逐条采纳/驳回 + 修复计划）

> 用户 mandate 校准：**目标是暴露 tether 缺陷，不是全绿。** 本内审同时抓到 (a) 假绿 与 (b) 误分类 RED（相反 mandate 错误），二者都修。**主进程采纳绝大多数 finding**——它们 source-verified、verify-pass 确认，且暴露了实现里的真问题（含推翻主进程先前 §12 的两处结论）。

**采纳（ADOPT）+ 修复计划：**

- **M1（采纳，推翻主进程 §12）— #42/#44 误分类为"永久 bug"实为 ~10s TFence 有界瞬态。** `--remote` 在 `TFence=10s`（`internal/cluster/read.go:18`）后自我纠正为 READ-ONLY（`cluster_status_nats.go:116-117`，exit 2）。#44 机制源码级假（advisory gate 走 LIVE probe `d8_alerts.go:71-80` + `alerts.go:144-163`，TFence 后确实触发；我在窗口内探测误判"永不触发"）。**修**：把 #42/#44 合并 reframe 为**单一有界窗口 gotcha**（quorum-loss 后 ~10s 内 `--remote` 误导 verdict + `session rm` raw store_error），带 **fence-aware 对照**（探 on-broker socket ~1s 后翻 vs `--remote`，断窗口内二者不一致；或 TFence 后断自我纠正为 GREEN）。改 #44 注释。**改正 §12：§11-U3 的"exit-2 READ-ONLY 闭合 G7b"正确，只是 TFence-delayed，未被推翻。**
- **M2（采纳）— leg-b `force.single` 签名恒匹配 = 假绿；#43 源码级假 + 违反 §11-U3。** **修**：leg-b 去掉 `force.single`，要求真 `DATA-PLANE DEGRADED` banner 串 + 数据面探针失败佐证，poll ≥60s（`jsDownThreshold`）；banner 触发则 (b) 合法、#43 作废；不触发则真缺口是 #41（banner 不发）非 #43。删 #43。
- **M4（采纳）— 40 R-retire `_ctl_write_ok` 是两 READ 标为 WRITE + roster-absence 早于 terminal RETIRED。** **修**：control-source 改真 `session … --pin` COMMIT；捕 op-id poll `ops show` 到 terminal RETIRED（或 `--wait`），非仅 roster-absence。
- **M5（采纳）— 42 招牌臂 Arm A（returning-node 冷启动诊断）缺失。** **修**：实现 Arm A（`node_start brk2` → poll fresh journal 的 EJECTED-vs-transient 诊断 `broker.go:941-958`），或 NOT-COVERED-with-reason。
- **M6（采纳）— 42 diagnose 正例用假源码理由（force-single 非 --guided 不印 pasteable）跳过。** **修**：prune 前插真 `recovery diagnose --self-id brk1`（roster 仍列 dead brk2）→ 断 exit 0 + pasteable cmd。
- **M7（采纳）— 91-C 在 N=3 拓扑破损（第三 node 仍活 → force-single HARD-REFUSE 被 `||true` 吞 → RED-or-vacuous）。** **修**：C 前先降 N=2；oracle 改正+负双向。
- **M8（采纳）— 22 真 #35 = "quorum-loss 期 survivor RESTART"（结构性不可达，仅 OFFLINE 逃生）。** peer-kill fixture 不触发 restart 分支=死代码。**修**：加专属臂——`node_kill brk2` 后 survivor `systemctl restart tether-broker` → poll NRestarts-climbing + DRY-never-proceeds → `assert_bug #35`（钉 `install.sh:754` tether-broker.service）。#35 保 CANDIDATE-不可达-in-peer-kill-fixture，POSITIVE 分支加 production-fidelity NOT-COVERED 注。
- **M9（采纳）— 22/12/20 的 split-brain peer-alive HARD-REFUSE 门零覆盖。** **修**：加 PALIVE-full（survivor 停、peer 活 → offline force-single `assert_refuses "ALIVE … split-brain"`）+ re-run 12/20 GREEN 作回归门。
- **M10（采纳）— 90 M6 45s 是从 operator bounce 量、非 auto-detect；#39（5-min 固定 interval 无 knob）未登。** **修**：relabel M6④ + 登 #39 + 加 no-bounce periodic leg（或 scope startup-tick）。
- **M11（采纳，硬闸）— #35/#42/#43/#44 全未在 `docs/deploy-tier-gotchas.md`（顶 #34）注册；#42/#43/#44 连 §4 都未 ratify；#41 与 #43 同根因（`cluster_status_nats.go:161-168`）双编号风险。** **修**：re-classify 后统一注册 + 调和 #41/#43，§7 landing 闸方可过。
- **M12（采纳）— 43 outcome-(c) 未尝试 (b) 单模 auth_callout（源码支持 `serve.go:204-214`）即宣告 NOT-COVERED = cop-out。** **修**：尝试 (b) 驱动 live-row survival oracle，或给 source-cited 具体不可行理由（如无 tether 命令渲染单模 auth_callout conf → 手渲=Mandate④ 存疑）。
- **M13（采纳）— 43-E "harness pollution"假设源码级被驳（负例在 migration 前 reject、双 mint no-op）。** **修**：隔离干净 `cmd_init`（无负例）；若复现 init 不成 N=1 = 真 migrate 缺口 → EXPOSE，勿 relabel oracle 绿。
- **M14-M17 + MINOR 组（采纳）**：M14 below_quorum 假 blocker（`nv==2` health-无条件 `alert_reconcile.go:253`）→ 删假因 + 加 N=2 leg；M15 41 `_blocked` 算而不读 + NATS_ROLLED_OUT 归错因（`already in flight` 非 grow-lock）→ 分支修正 + EXPOSE stall；M16 90 M6 N=1 rebuild 丢 cross-node forward seam；M17 92 reframe 丢 `--ack-alerts` 覆盖（§7 义务）→ 补 arm 或 NOT-COVERED。MINOR：always-exit-0 假断言（41 S-jsreset / 43-G rollback）、assert_ok-on-grep 丢 rc（40/41/92 → assert_refuses）、40 RECONCILE-plan 测错路径、loose 签名群（22:115/42:45 `force.single` catch-all 等）、header 不符、42 Arm H 自造顺序丢、22 #36 半证、93 webhook 黑名单非白名单 + `transition` 未断、91-D 至少 signature-guard、22 GATE-b 判别子缺。**逐条修/补测。**

**驳回/降级（REJECT，采纳 verify-pass 的裁定）：** R6 对 #42/#44 的"暴露正确"表扬被 synth 源码推翻（→按 M1 处理）；R2#7(a) O_EXCL 双 token 今不可利用（保 hygiene）；R3 socket-perm masking 非问题（socket tether-owned 0600，root/tether 都到门）；below_quorum MAJOR→MODERATE；M5① MAJOR→MINOR（clean-baseline gate 兜底）。**#35 citation 订正：crash-loop unit = `install.sh:754` tether-broker.service，非 `:722` nats-server。**

**修复规模大，跨 context 窗口执行**（本轮 context 已达上限）。修复后重跑受影响 drill 至稳定态 → 重跑 Stage-C 闭合核验 → **停外审门**（不 commit、外审进行中不 git add）。

---

## §0.3 最终状态（外审 round-3 后收敛——单一真实状态，取代旧 §0.1/§0.2）

> round-2 M6 要求删 §0.1（PENDING 滚动）/§0.2（"完成+全 GREEN"）的互相矛盾，只留一个真实状态。旧两段
> predate **五态 verdict 契约**（外审 round-1/2/3 驱动落地），且用旧的 "#37-family" 编号、并把 drill 说成
> "全 GREEN"——二者现均订正。

**1. 三轮外审驱动了一次整体 remediation（本轮同一窗口一次落地）。** Stage-C 内审（§0/§1）+ 外审
round-1/round-2/round-3 逐层抓出：内审抓我的假绿（92 banner catch-all / 40 假 WRITE oracle / 42 招牌臂缺失
/ 91-C N=3 破损）与误分类 RED（#42/#44 有界瞬态断成永久）；外审 round-1 抓出我"改假绿"时引入的**新假绿**
（measure-and-record 跳脊仍报 GREEN）；round-2/round-3 抓出地基未接线、缺/空签名 fail-open、9 drill 零迁移、
PRODUCT-RED 与 SSOT 冲突。**全部采纳。**

**2. 落地的 verdict 契约（SSOT = `lib/assert.sh` 头注真值表 + `tests/verdict-contract-test.sh`）。** drill 落
**唯一 landing verdict**：GREEN / PRODUCT-RED（signature-guarded 复现已登记缺陷——harness 按预期工作，非绿、
预期）/ INCOMPLETE（`not_covered` 记录的覆盖缺口）/ SETUP-RED / ASSERT-FAIL。`drill_end` 发结构化
`DRILL-VERDICT` 行，`run-drills.sh` 按 verdict 分类 + rc 交叉校验（legacy `drill_end;exit N` 判
VERDICT-RC-MISMATCH blocker）。缺/空签名/缺参 fail-CLOSED 为 SETUP-RED。`tests/lint-drills.sh` 静态禁 9 drill
的假绿反模式。**旧「已知缺陷=harness-GREEN 连绿」契约已废（roadmap §术语 + README 已同步）。**

**3. 9 drill 在契约下的诚实预期落地——不是"全 GREEN"，而是 INCOMPLETE/PRODUCT-RED（这正是 mandate 生效）：**
每个 drill 用 `not_covered()` 记真实 explore→pin 缺口（→ INCOMPLETE），用 `product_red`/`assert_bug` 复现已登记
缺陷（→ PRODUCT-RED）。真缺陷 signature-guarded 暴露：**#45**（40/41 retire NATS_ROLLED_OUT 收敛停滞，
独立号——此前误标 #37-family）· **#35**（22 survivor-restart 臂，**降级 CANDIDATE**、MainPID 变证明手动重启
才 PRODUCT-RED）· **#39**（90 disk 5-min 固定）· **#42**（92 有界窗口，+实测 #42-STRONGER）· **#46**（91
seeds 漏第 3 voter，此前 #G3）· **43-E 候选**（init from-existing 不产 cluster 标记，per M13 暴露非 relabel）。

**4. gotcha 编号跨文档零漂移（round-2 M6 / round-3 M6）**：#45=NATS_ROLLED_OUT stall（独立号）· #46=91
seeds· #35=candidate· #42=有界窗口（#43/#44 折入）；ledger `docs/deploy-tier-gotchas.md` 与 plan §4 表一致。

**5. 收尾**：地基 + 9 drill 迁移 + SSOT + hermetic tests（`tests/verdict-contract-test.sh` 三壳 34 断言全过 +
`tests/lint-drills.sh` 批次 0 违规）本窗口完成 → 远端 fail-closed 复跑受影响 drill → **停外审门**（不 commit、
外审进行中不 git add）。

**关键教训**：三轮外审各自抓出一层我看不见的问题——连我"修假绿"时引入的新假绿、连我地基的不充分——都被独立
外审露了出来。**这正是 mandate：让 tether 的问题、也让我自己的错误露出来，而非凑全绿。**

---

## §1 Stage-C 专家报告全文（synth）

I have everything I need. The critical cross-lane conflict is resolved against source, and the ledger/plan state is confirmed. Here is the synthesized Stage-C report.

---

# STAGE-C SYNTHESIS — simcluster deploy-tier batch G-B (S6+S8), drills {22,40,41,42,43,90,91,92,93}

**Synthesizer note on method:** R5 (drills 91/92/93) shipped with a `null` verify pass, and its findings #1/#2 (that 92's #42/#44 exposures are *false* — a self-correcting TFence artifact) **directly contradict R6's verified praise** of those same exposures as "done right." Because this decides whether the batch's headline REDs are genuine, I re-read the source myself (`cluster_status_nats.go`, `internal/cluster/read.go`, `internal/proto/alerts.go`, `d8_alerts.go`, drills 91/92, `assert.sh`, the ledger, plan §4/§11/§12). **R5 wins on source; R6's headline praise is overturned.** Findings marked **[self-verified]** were checked by me this session; the rest preserve their lane's verify-pass citations.

---

## 1. CONFIRMED FINDINGS — ranked most-severe first

### MAJOR

**M1 · 92 leg-a #42 + #44 · mis-classified-RED (permanent bug asserted for a bounded ~10s TFence transient); #44 comment is source-false; both oracles timing-fragile · [self-verified]**
This is the headline and it overturns R6's praise. Source facts:
- `TFence = 10s` (`internal/cluster/read.go:18`). A survivor at N=2 that loses its peer demotes (leader-lease) and **resets `LastContact` at step-down** (`read.go:39-41,42-53`), so `LeaderContactStale` flips `true` after a bounded `LeaderLeaseTimeout + TFence` (~10-11s). At that point `AllStale=true` → `ctlVerdictLine` returns the **correct** "no writable leader … READ-ONLY" (`cluster_status_nats.go:116-117`) and `ctlExitCode`→**2** (`:136-137`). So `--remote` does **not** permanently mis-report — it self-corrects. The plan §12 claim "`--remote` 根本不给 READ-ONLY verdict" and "推翻 §11-U3" (`s6-s8-plan.md:466`) is source-false; §11-U3's "exit-2 READ-ONLY closes G7b" is correct, just TFence-delayed.
- #42's oracle (`92-…:45-47`) probes ~3-6s post-kill (`a③` `tcp_refused` at `:38` returns in ~2s), i.e. **inside** the window, so it reliably catches "electing (transient)" and labels it permanent. Under load, a probe landing >~11s post-kill returns READ-ONLY → the inner `grep 'READ-ONLY' && exit 0` fires → `assert_bug` reports "APPEARS FIXED" → **spurious drill RED** (`assert.sh:45-46`).
- **#44 is worse — its stated mechanism is source-false.** The §12 note (`s6-s8-plan.md:473`) and drill comment (`92-…:48-53`) claim "the `--ack-alerts` gate is STORE-BACKED and does NOT fire on a raw LIVE quorum loss." But `gateDestructive` runs a **LIVE** `probeClusterHealth` (`d8_alerts.go:71-80`) and `EvalDestructiveGate` sets `QuorumLost = !anyWritableLeader && allStale` off that live probe (`internal/proto/alerts.go:144-163`) — the *same* TFence predicate. So the graceful "BLOCKED … quorum_lost … --ack-alerts" advisory (`d8_alerts.go:90-98`) **does** fire on a raw quorum loss — after TFence. Inside the window it doesn't, so `session rm` hits `store_error`. The developer probed inside the window, saw `store_error`, and mis-diagnosed a ~10s window as "the gate never fires." The developer's own §12 note records the flip from the correct premise to the wrong one.
- **Disposition to EXPOSE (not drop):** there IS a real, narrow gotcha — for ~10s after a real quorum loss the operator gets a misleading "transient" verdict *and* a raw `store_error` instead of a graceful advisory. Reframe both to a **bounded TFence-window** gotcha with a **fence-aware control** (e.g., probe the on-broker socket which flips ~1s after step-down, and assert socket-vs-`--remote` *disagree* within the window; or probe after TFence and assert self-correction as GREEN). Correct the #44 comment (gate is live-probe, fires after TFence). As written they are mis-classified REDs — the OPPOSITE mandate error, and fragile.

**M2 · 92 leg-b (b) + #43 · false-green green-washing the CORE of G7b, coupled to a mis-classified #43 · [self-verified] · (R5#3, R6#1-2 CONFIRMED)**
- Leg-b's `b:` banner oracle signature is `DATA-PLANE DEGRADED|JetStream UNAVAILABLE|force.single` (`92-…:75`). Post-force-single the verdict line **always** contains "force_single_active" (`cluster_status_nats.go:105-111`), and `-qiE 'force.single'` matches it (`.`=`_`). The adjacent `b: exit code == 3` (`:76-77`) can only pass via `ForceSingleActive→3` (`:132-133`), *proving* the token is present on every run. So `:74` passes via the ever-present verdict **without the `DATA-PLANE DEGRADED` banner ever firing** — the G7b #20③ banner (the drill's entire leg-b point) is never validated. **Fix:** drop `force.single`; require the real banner string corroborated by a data-plane probe failing.
- #43 (`92-…:78-82`) asserts `--remote` "lacks the `reconcile nats --to-standalone` remedy the socket banner has." **Source-false:** `renderCtlStatus` (the `--remote` render) prints that exact remedy **inside** its `if s.JetStreamUnavailable` block (`cluster_status_nats.go:161-168`) — same function, same output. The remedy is gated on `JetStreamUnavailable`, set only after `jsDownThreshold=60s` sustained 503. So #43 "reproduces" only *before* 60s (or if the banner never fires) — it is finding M2's non-firing banner, not a missing-remedy gap. **This directly violates the plan's own §11-U3 adjudication** (`s6-s8-plan.md:442`: logging an `assert_bug` RED for the by-design non-firing banner "= Mandate 反转"). The two oracles are anti-correlated: the only all-green world is "banner never fires," which means leg-b never reached its designed state. **Fix:** poll ≥60s; if the banner fires, #43 is void and (b) is legit; if not, the true gap is #41 (banner doesn't fire), not #43.

**M3 · 93 EXIT + WATCH · vacuous-oracle (both always pass; whole B2 degraded taxonomy untested) · (R5#7/8, R6#3 CONFIRMED)**
- WATCH (`93-…:103-104`): `… | grep … || true; true` — the trailing `; true` makes the `sh -c` **always exit 0**; "repaints ≥1 frame" is never verified, and it runs from `$SIM ctl` (exit-69 wrong surface — the very class the dev "fixed" for CARD/EXIT). **Fix:** run `--watch` from a broker as `tether`, assert ≥1 real repaint.
- EXIT (`93-…:95-96`): `[ $rc -ge 0 ] && [ $rc -le 3 ]` — `rc≥0` always true, so only `rc≤3`; no degraded injection, no DEGRADED(1)≠ROSTER_UNREACHABLE(2) discrimination (the entire plan §3.4 B2 taxonomy is deferred at `:109`). **Fix:** inject degraded (kill a voter → 1/DEGRADED) and all-down (→ 2/ROSTER_UNREACHABLE) with exact-string checks.

**M4 · 40 R-retire · vacuous-oracle + false-green (read labeled as WRITE; roster-absence satisfied before terminal RETIRED) · (R1#1+#8, R6#4 CONFIRMED)**
- `_ctl_write_ok()` (`40-drain-retire.sh:36`) is `session ls && node ls` — **two READS**, served from local raft-applied SQLite with no fresh quorum commit — yet `:113` labels it "control-source **WRITE** … raft coherent." Plan §2.1 mandated a real COMMIT (`session --pin create` / `nats kv put`) precisely to rule out a phantom voter. **Fix:** create a fresh `session … --pin` and assert it commits.
- Compounding it (R1#8): `_retire_gone` roster-absence (`:112,35,37`) goes true when `PlanClusterNodeRemove` commits the roster delete *during the transition into* `NATS_ROLLED_OUT` (`cluster_operation_controller.go:758-767`), **before** the convergence gate to terminal RETIRED (`:768-778`). Combined with the read-only oracle, drill 40 can go **GREEN on a retire that stalled at NATS_ROLLED_OUT and never converged.** **Fix:** capture the op-id and poll `ops show` to terminal RETIRED (or `cluster retire --wait`), not just roster-absence.

**M5 · 42 Arm A (returning-node cold-start diagnostic) · missing-arm / mandate-violation — the drill's namesake arm is absent and not even NOT-COVERED · (R2#1 CONFIRMED)**
`42-…:45` does `docker kill …-brk2` and **never `node_start`s brk2 again** (line 47 restarts brk1). The plan's headline arm (`s6-s8-plan.md:161`, §9-42 `:419`) — reboot brk2, poll its fresh journal for the ranked EJECTED-vs-transient rejoin diagnostic (`internal/broker/broker.go:941-958`, which carries both `recovery rejoin prepare` and `EJECTED … raft config still lists peer` tokens) — is entirely gone, and the NOT-COVERED log (`:99-102`) records only E/F/I. `docker kill` (not `rm`) preserves the container, so it is constructible. **Fix:** implement Arm A, or record it NOT-COVERED-with-reason. It must not vanish unremarked.

**M6 · 42 diagnose pos/neg pair · vacuous-oracle / R-CONTROLSRC violation — dead-peer POSITIVE exercised nowhere; justification is a false source-claim · (R2#2, R6#9 CONFIRMED)**
Only the alive-peer REFUSE runs (`42-…:36`, genuine — `wizard.go:40`). For the dead-peer POSITIVE the drill claims (`:50-55`) it was "exercised inline by the OFFLINE force-single above (same forceSingleGuided printer)." **False:** line-45 runs the real `recovery force-single` (no `--guided`), whose RunE prints only "force-single complete: … single-voter cluster" (`cluster_offline.go:207-210`) and **never calls `forceSingleGuided`** (the only emitter of the pasteable `force-single … --confirm-peers-dead`, `cluster_offline_wizard.go:47-48`). So a diagnose that *always* refuses would pass this drill. **Fix:** insert a real `recovery diagnose --self-id brk1` while brk1's roster still lists dead brk2 → assert exit 0 + pasteable command (one-line reorder before the prune).

**M7 · 91 arm C (offline force-single → survivor-only convergence) · topologically broken → RED-or-vacuous; both the arm and its oracle are wrong at N=3 · [self-verified] · (R5#4, R6-M1 CONFIRMED)**
Arm C runs after A2 grew to **N=3** (`91-…:44-53`), then picks `DEADP` = the first *single* running non-survivor (`:71`) and runs `recovery force-single … --confirm-peers-dead $DEADP` (`:76`). But a **third** node is still alive & in-roster, so force-single HARD-REFUSES (must enumerate every non-self peer) — swallowed by `|| true`. The comment "reduce to N=2 first is not needed" (`:66`) is wrong for N=3. The dead peer stays a VOTER (endpoint retained), so the convergence poll (`:81-82`) either times out (**RED**) or, if the restarted survivor's `seeds show` returns empty (`:76-77` all `|| true`), the bare-negative `! … grep -q $DEADP` passes **vacuously** (false-green). **Both lanes' "91-C got it right" praise is retracted.** **Fix:** reduce to N=2 before C (retire/kill+confirm the third node, per plan §3.2 C-seeds N=2 fixture), and make the oracle positive+negative (survivor endpoint present AND dead endpoint absent).

**M8 · 22 Arm-0 / #35 · mis-classified-gap ("not real" from a fixture that structurally cannot reach the trigger) · (R3#1, R6#6 CONFIRMED)**
The gotcha branch is gated on `NR1 != NR0` (`22-…:98`) but the fixture injects **no restart** — killing brk2 does not bounce brk1, so the branch is dead code and POSITIVE was foregone. But #35's own definition is a **survivor restart during quorum loss** (plan §2.5:230). That form is real and reproducible: an operator `systemctl restart tether-broker` on the survivor during quorum loss → boot hits the EJECTED trap (`broker.go:948-958`) → non-zero exit (`serve.go:240`) → `Restart=always`/`RestartSec=2` on **tether-broker.service** (`install.sh:754-755` — *not* the nats-server block at `:722`), and each ~2s life < `forceSingleDwell=15s` while `newForceSingleArm()` zeroes `leaderlessSince` every boot (`force_single_online.go:41,46,27`) → the "preferred path" online force-single is **structurally unreachable**; only OFFLINE escapes. Uncovered across drills 12/20/22. **Fix:** add a dedicated arm — after `node_kill brk2`, `systemctl restart tether-broker` on brk1, poll NRestarts-climbing + DRY-never-proceeds, `assert_bug` pinned to `#35`. Keep #35 as CANDIDATE-not-reproducible-in-the-peer-kill-fixture, not GREEN; the POSITIVE branch must carry the production-fidelity NOT-COVERED note (currently only TAMED does, `:112`).

**M9 · 22 / 12 / 20 · missing-arm — the split-brain peer-liveness HARD-REFUSE gate has ZERO deploy-tier coverage · (R3#2 CONFIRMED+strengthened)**
`fsArmVerdict` evaluates dwell first and returns `CodeQuorumNotLost` (`force_single_online.go:170`) **before** `CheckPeersDead` (`:176`), so GATE-a never exercises `peer_alive`; the commit re-probe (`:249`) runs only on the POS pass path. `setup-forcesingle.sh:1-18` was never augmented, and **drills 12/20 contain no `assert_refuses` at all** — so the split-brain HARD-REFUSE (`offline.go:276` "ALIVE, force-single would split-brain") is exercised nowhere. A `peer_alive`-always-pass regression is invisible to the whole tier. **Fix:** add the planned PALIVE-full (survivor stopped, peer alive → offline force-single `assert_refuses "ALIVE … split-brain|accepted a TCP connection"`) and re-run 12/20 GREEN as the regression gate.

**M10 · 90 M6 disk_pressure · mis-classified-gap (#39 masked) + misleading oracle label · (R4#2 CONFIRMED)**
`grep` for `DiskCheckInterval`/`DiskPressureThreshold`/`DiskUsageFn` returns **zero** wiring — the field always defaults to the fixed `5*time.Minute` (`disk.go:23`), which is #39. M6④ "disk_pressure raised within 45s of the >80% fill" (`90-…:137`) only passes because the drill `systemctl restart tether-broker` (`:135`) forces the startup re-sample (`disk.go:99-102`); the 45s is measured from the operator bounce, not auto-detection. The plan designates M6 as "#39 的真主题" yet the drill registers no #39 note. **Fix:** relabel M6④ (45s from the bounce), register #39 (fixed 5-min, no knob) as a labeled gotcha, or add a no-bounce periodic leg. (Fixture mechanics — tmpfs/StoreDir/AttachAlertSink-before-startDiskMonitor — are sound; keep them.)

**M11 · gotcha ledger · unregistered-gotcha + double-number risk (blocks §7 gate) · [self-verified] · (R6#5 CONFIRMED)**
`docs/deploy-tier-gotchas.md` tops at **#34**; grep for `#3[5-9]|#4[0-4]` = **0 hits**. Drills fire `assert_bug` against `#35` (`22-…:131`), `#42/#44/#43` (`92-…:45,54,80`). Worse: **#42/#43/#44 are not even ratified in plan §4** (which stops at #41 RESERVED, `s6-s8-plan.md:348`) — they live only in the §12 spike log. **#41 vs #43 are the same root cause:** the `--remote` remedy lives *inside* the `if JetStreamUnavailable` banner block (`cluster_status_nats.go:161-168`), so "#43 remedy missing" ⟺ "#41 banner not firing." Registering both separately double-numbers one gap. §7's "no unmapped row" + "stamp G-B landing" gate cannot stand until #35–#44 are registered and #41/#43 reconciled. **Fix:** register (or, per M1/M2, first re-classify #42/#43/#44) before landing.

**M12 · 43 · mandate/cop-out — outcome (c) declared without attempting (b); a migrate-LIVE-DATA drill that migrates no live data · (R2#4 CONFIRMED, calibrated)**
Arm A ("bare P2 can't serve", `43-…:34-39`) is source-honest (standalone conf has no auth block, `install.sh:690-707`). But the plan (`s6-s8-plan.md:182`, §12 `:482`) directs *attempt (b)* — render single-mode `authorization.auth_callout` and drive the F/G live-row survival oracle — falling to (c) only if (b) can't be produced. Single-mode auth_callout **is** a supported capability (`serve.go:204-214,253-254`; nkeys already minted by `secrets_mint_node`), so "SB-43 follow-up" (`43-…:6-8`) is not a valid infeasibility reason. **Fix (calibrated):** either attempt (b) and drive the live-row survival oracle, OR record a concrete, source-cited reason (e.g. no tether command renders a *single-mode* auth_callout `nats.conf`, so the sim would hand-render — a Mandate-④-suspect). A deferral is neither.

**M13 · 43-E · mis-classified-gap — "harness pollution" hypothesis is source-refuted; root-cause before labeling · (R2#3 CONFIRMED)**
The §12 suspicion (`:476`) that negatives/pre-mint pollute `cmd_init` is source-false: `init --yes` and machine-confirm-missing-env both reject *before* any migration (`cluster.go:725,776`), and the double-mint is a no-op (`secrets_mint_node` short-circuits on existing `route-cert.pem`, `lib/secrets.sh`). If a clean `cmd_init` on a P2-`start-broker`'d broker fails to form N=1, that is exactly the migrate gap the drill exists to EXPOSE. Note the `43:70-71` "cluster-mode seam" check is near-vacuous — `cmd_init` writes that broker.yaml seam (`simcluster:301`) *before* the leader poll (`:306`), so it passes regardless of election. **Fix:** isolate a clean `cmd_init` (no negatives); if it reproduces, EXPOSE as a signature-guarded init gap — do not relabel the oracle green.

### MODERATE

**M14 · 90 below_quorum NOT-COVERED · fabricated blocker · (R4#1 CONFIRMED, moderate)** — the stated reason "needs a #31-gated retire to nv==2" is false: `below_quorum` fires on **any** `nv==2`, health-unconditional (`alert_reconcile.go:253`, leader-gated only `:176`); `setup_forcesingle_n2` already builds nv==2 with no retire. It's a working alert (missing cheap GREEN, not a hidden RED). **Fix:** strike the false blocker; ideally add the N=2 leg (`kind=="below_quorum" severity=="info"`), closing inventory row 79.

**M15 · 41 shrink terminal branch · mis-classified-gap · (R1#2 CONFIRMED)** — `_blocked` is computed (`41-…:52,60,66`) but never read; the else-branch unconditionally blames grow-lock (`:87`). A retire that stalls at `NATS_ROLLED_OUT` refuses the next retire with "an operation … is already in flight" (`cluster_operation_controller.go:33`), which matches neither drill sig (`grow of …|membership operation … in flight` — the latter matches no real tether string), so it lands `NOT-COVERED-blocked-by-#31` with the **wrong cause**. **Fix:** branch on the captured `_out` + `already in flight`; EXPOSE the NATS_ROLLED_OUT stall as its own signature-guarded gap.

**M16 · 90 M6 rebuilds N=1 · fidelity reduction · (R4-A8 CONFIRMED)** — disk_pressure was meant to test a non-leader forwarding to a different leader over `alert_forward.go`; the N=1 rebuild (`90-…:128-130`) makes forwarder==leader, so the cross-node forward seam is never exercised, and OQ-5's capped-N=2 alternative was neither taken nor documented. `FOLL_DISK` is computed + SETUP-gated (`:54-56`) but dead. **Fix:** take capped-N=2, or document the N=1 downgrade NOT-COVERED-with-reason; drop `FOLL_DISK`.

**M17 · 92 · dropped inventory obligation (§7 gate breach across 3 rows) · (R6#8 CONFIRMED, borderline-major)** — the leg-a reframe removed all `--ack-alerts` coverage; `session rm --ack-alerts` + `expose --ack-alerts` + `expose rm --ack-alerts` (assigned to 92(a), authority `s3-s5-plan.md:375`) now have zero coverage and no NOT-COVERED disposition. **Fix:** add an `--ack-alerts` arm (also the correct control for M1's #44 — the bypass proves the gate is advisory) or an explicit NOT-COVERED for all three.

### MINOR (grouped — all CONFIRMED by the lane verify passes)

- **Always-exit-0 "assertions":** 41 S-jsreset `…; sleep 6; true` (`41-…:77-78`, R1#3); 43-G rollback `sh -c "…; true"` + DB-restore never verified (`43-…:74-75`, R6-M3). Demote to `[env]` or assert real post-state (services `active`; `md5(tether.db)==md5(.bak)`).
- **`assert_ok`-on-grep discards rc (use `assert_refuses`):** 40 RECONCILE-guard (`_recon_needs_args`, `40-…:33`) and 41 S-rebind (`41-…:25`, R1#4) — the account-issuer guard is a real refusal at `cluster_natsconf.go:263-267` *before* the render; 92 REMOTE-mutex (`92-…:103-104`, R5#10/R6) — plan specified `assert_refuses` with a mutex-error signature.
- **40 RECONCILE-plan zero-write tests the wrong path** (R1#5): the account-issuer guard short-circuits before `renderTakeoverPlan`, so md5-unchanged proves "a refused command wrote nothing," not the `--plan` render's zero-write. Supply `--account-issuer`+`--broker-nkey` to reach `:360`.
- **Loose/over-broad signatures (R-SIGGUARD erosion):** `force.single` catch-all in success oracles at 22:115 (R6#7) and 42:45 (R6-M2); 40/41 bare `…|removed`, `…|[0-9]+ voters`, `aborted|TTY` (R1#6); 22 PROT `not .*leader|no quorum` beyond the observed `no leader (election in progress)` (R3#4); 93 CARD `|\(exit [0-9]+\)` accepts any exit footer + NOT-HA for an HA cluster (R5#9); 90 M2 3-way banner OR passes on the bare sentinel (R4#7); 43-A `no responders|cannot reach broker` masks a down-broker under the load-bearing "bare P2 doesn't serve" (R2#8); 42 O_EXCL `WIPE REFUSED|file exists` OR + K-contrast `TTY` fall-through (R2#7). Anchor each to its specific token.
- **Header/oracle mismatches:** 41 header claims a raft-row survival read but the oracle is a fresh JS push; `login -s` is unasserted `|| true` (`41-…:79,81-82`, R1#7/R6#10) — add the plan-mandated post-JS-reset session-survival read; 90 M5① baseline uses raft VOTER phase not `ReachSource=="nats-health"` (`90-…:99`, R4#4/R6#12, low impact — clean-baseline gate defends the false-green); 42 zero-mutation `length>=1` can't catch a 2→1 demotion (`42-…:40-41`, R2#7c).
- **Dropped-via-self-inflicted-ordering:** 42 Arm H (resnapshot single-voter refusal) deferred because the prune ran first, though the plan said run it before the prune (`42-…:70-77`, R2#5).
- **Half-proven divergences:** 22 #36 asserts the online-gate string is present but never that the offline `NO --yes override` is absent (`22-…:77-79`, R3#6); 93 webhook no-secret is a blacklist not the plan's whitelist, and the `transition` field is unasserted (`93-…:80-82`, R6#11).
- **91-D cli-failover deferred wholesale** (`91-…:55-63`, R5#6) — the marquee v0.4.6 failover is untested; the NOT-COVERED is honestly logged with a source-cited reason, but at minimum make it signature-guarded naming the missing roster-warm precondition.
- **22 GATE-b within-dwell discriminator missing** (R3#5) — no arm asserts the Phase-2 `CodeQuorumNotLost` **with** `DwellRemaining` set; a `NOT-COVERED-timing` disposition is acceptable but silent absence is not.

---

## 2. THE MANDATE VERDICT (R6-central · the "擦屁股" answer)

**Of the 5 "GREEN" drills — which greens are genuine vs cosmetic:**

| Drill | Verdict | Basis |
|---|---|---|
| **22** (GREEN-20) | **Genuinely green on its POSITIVE spine**, but its `#35`-"not real" conclusion is a **cosmetic green over a real gap** (M8) — the fixture structurally cannot reach #35's trigger. The total-function Arm-0, PROT↔POS set-raft-addr control-source, and MainPID discriminator are all real; keep them. | M8, M9 |
| **40** (GREEN-16) | **Cosmetic on its load-bearing retire oracle** (M4): a labeled "WRITE/raft-coherent" oracle is two reads, and roster-absence is satisfied at NATS_ROLLED_OUT before terminal RETIRED — so it can be GREEN on a stalled retire. The #31 two-outcome branch and DRAINING-observed-before-abort FG are genuinely honest; keep. | M4, M15 |
| **41** (GREEN-3, blocked by #31) | Mostly **honest** (S-refuse-peers/no-confirm are real KEPT guards; tier-B push is a real R-DATAPLANE terminus), but the terminal branch mis-attributes the NATS_ROLLED_OUT stall to grow-lock (M15) and the survival oracle doesn't prove the plan's raft-row-outlives-JS-wipe claim. | M15, minors |
| **42** (GREEN-18) | **Partly cosmetic:** the namesake Arm A is silently absent (M5) and the diagnose pos/neg pair is neg-only with a false-source justification (M6). Arm D (O_EXCL on an intact DB), Arm C (alive-peer refuse), and the K-contrast (rejoin-prepare has no `--confirm-node-id`) are genuinely excellent; keep. | M5, M6 |
| **90** (GREEN-21, +M6 untested) | The 21 non-M6 arms are **genuinely green** (C1 leader-gate correction, severe-banner-only-via-manual, ack≠clear, quorum_lost interpret-refuse — all source-verified). **M6 is cosmetic and masks #39** (M10) and has never actually been run. below_quorum's NOT-COVERED has a fabricated blocker (M14). | M10, M14, M16 |

**Which REDs are real exposures (KEEP) vs mis-classified (FIX):**
- **KEEP:** none of 92's three current `assert_bug` REDs survive as written.
- **MIS-CLASSIFIED (the OPPOSITE mandate error — RED for correct/transient code):** **#42, #44, #43** (M1, M2). #42/#44 assert a *permanent* bug for a bounded ~10s TFence transient that self-corrects; #44's stated mechanism (store-backed gate) is source-false; #43 asserts a "missing remedy" that source shows lives in the `--remote` render, gated on a 60s threshold — and doing so **violates the plan's own §11-U3** anti-Mandate-inversion ruling. **The real, exposable gotcha is the narrow bounded-window one** (misleading verdict + raw `store_error` for ~10s), which must be reframed with a fence-aware control — not dropped, not asserted as permanent.
- **43-E** (M13): a real-or-not init gap prematurely written off as harness pollution — must be root-caused, and if real, EXPOSED (do not relabel the oracle).

**Which deferrals dodge a RED they should own:**
- **42 Arm A** (M5) — the namesake diagnostic, dropped without even a NOT-COVERED.
- **43 outcome-(c)** (M12) — declares business-survival NOT-COVERED without attempting the supported (b) auth_callout path.
- **91-C** (M7) — presented as a working convergence oracle; it is a broken arm that RED-or-vacuously-greens at N=3.
- **41 terminal / NATS_ROLLED_OUT stall** (M15) — a distinct retire-convergence defect swept under #31's grow-lock label.

**What the developer got RIGHT (preserve on adjudication):** the total-function Arm-0 contract (22); the `set-raft-addr` before/after control-source (22); the #31 two-outcome branch + DRAINING-before-abort FG (40); O_EXCL-on-intact-DB + the never-escapable K-contrast (42); clean-baseline gate + severe-banner-only-via-manual + ack≠clear + quorum_lost-interpret-refuse (90); A1/A2 change-gated auto-publish read off `seeds show` content (91); MET leader/follower gating + POST→405 + webhook sentinel capture + `observability:` yaml-seam round-trip + the CARD/EXIT run-from-broker reclassification (93). And critically — the developer did **not** weaken 92's `#42` signature to match "electing a leader" as a green; the flaw is the opposite (asserting the transient as permanent), which is more subtle.

---

## 3. GOTCHA LEDGER CHECK (#35/#42/#43/#44)

- **Registration:** all four are **absent** from the SSOT `docs/deploy-tier-gotchas.md` (tops at #34). #35 is ratified in plan §4; **#42/#43/#44 are NOT even ratified in §4** — they exist only in the §12 spike log. §7's landing gate cannot pass until they are registered (M11). **[self-verified]**
- **Genuineness:** **#35 is genuine** but under-covered (M8). **#42/#44/#43 are mis-classified as written** (M1, M2) — the genuine residue is a single bounded-TFence-window gotcha, which should be registered as *one* entry (misleading verdict + raw store_error during the ~10s fence), not three.
- **Collision with #25–#34:** no numeric collision. But **#41 and #43 share one root cause** (`JetStreamUnavailable` gating both the banner-firing #41 and the `--remote` remedy #43 in the same code block, `cluster_status_nats.go:161-168`) — registering both risks double-numbering one defect. Reconcile before landing.
- **#35-vs-#23:** plan §11-U1 already adjudicated #35 as a distinct manifestation-pin with root cause = #23 restart-bounce family — that cross-ref is correct and should be preserved in the ledger entry.

---

## 4. REFUTED / DROPPED (do not chase)

- **R6's headline praise that 92's #42/#44 are "EXPOSED correctly"** — **overturned** by my source read (M1). R6-verify checked only that the "electing" verdict is real for `LeaderContactStale=false`; it never checked the temporal self-correction after TFence. Main should treat #42/#44 per M1, not R6's "keep."
- **R2#7(a) O_EXCL `WIPE REFUSED|file exists`** as an exploitable false-pass — refuted: the real error carries **both** tokens and a wipe can only be refused when O_EXCL fires, so it is not exploitable *today* (verified). Keep as hygiene-only, not a live defect.
- **R3 "socket-permission masking" concern on 22's DRY helper** (`$SIM exec brk1` root vs `dexec -u tether`) — refuted by R3-verify: the admin socket is tether-owned `0600` under `0700`; both root (DAC bypass) and tether reach the gate, no arm can pass for a permission reason. Non-issue.
- **R4 "M6①/⑤ silently excused"** framing — partially refuted: the else-branch NOT-COVERED is plan-sanctioned (OQ-5); the real residue is the absent-predicate query-error vacuity + M6-never-run (folded into M10/M16).
- **Severity de-escalations from the verify passes (adopt these):** R4#1 below_quorum MAJOR→MODERATE (M14, working alert not hidden RED); R4#4 M5① MAJOR→MINOR (labeling only, clean-baseline gate defends); R6#6 22-POSITIVE MAJOR→MODERATE labeling slip (the *uncovered gap* stays MAJOR as M8).

**Citation correction for main (load-bearing):** M8's crash-loop unit is **`tether-broker.service` — `install.sh:754-755`**, not the `nats-server` block at `:722-723`. Write the #35 arm against `:754`.