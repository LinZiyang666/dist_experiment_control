Fail

# S3–S5 (G-A) external re-review round 7

## Review conclusion

Do not release G-A. The developer correctly reversed the round-6 false-GREEN policy: failed moved-exit data
plane, failed auto effect, failed 71 fixture, and failed drain migration now affect the drill verdict. The
unauthenticated D1 owner rescope was also removed. Those are material closures.

However, the current drills still let failed causal prerequisites flow into dependent destructive arms, omit two
locked and directly executable controls, and in one independently reproduced 73 run issue a diagnosis that is the
opposite of its failed endpoint cross-check. These are not theoretical malformed-input cases: every Major below
occurred in the fresh concurrent server run or is an unconditional missing arm in the current script.

The product itself is also demonstrably not releasable: the independent run reproduced the lingering-operation
drain block, moved-exit failures, auto-rebalance deferral, proxy-home drift, and quorum endpoint mismatch. Correct
RED is welcome, but a RED suite still needs trustworthy causal boundaries before it can serve as the regression
gate that later flips green.

## Reviewed boundary and R6 closure

The staged round-6 Fail tree was the baseline. The developer response changed eight tracked files: 295 insertions
and 158 deletions, with no developer deletion or untracked payload. The response appended its claims to the
round-6 report; those claims and its historical server results were not treated as independent verification.

| R6 item | Round-7 disposition |
|---|---|
| R6-M1: 74 failed locked behavior stayed GREEN | Partly closed. B-dp and C-auto are hard assertions; failed command/duplicate/empty-control probes now fail. Dependent arms still run after failed foundations, and Arm C still lacks its locked data-plane and ordinary-expose controls. |
| R6-M2: developer-authored owner rescope | Partly closed. The unauthenticated quote and D1 rescope are removed; fixture and B now RED. Arm E remains wrongly omitted even though the existing fixture can execute it directly. |
| R6 advice: non-zero grow rc | Closed as observability. Late convergence is now explicitly labelled; attempt count and individual rc remain visible. |

## R7-M1 — drill 74 continues destructive claims after its locked baseline failed, and Arm C still lacks data-plane closure (Major)

The first independent run reached a valid 1/1/1 snapshot, then `SETUP-ss-brk2` failed after 120 seconds. The
subsequent `SKEW-reconstruct` also failed after 120 seconds; the actual distribution was `brk1=1 brk2=0 brk3=2`,
not the required 1/1/1. Nevertheless the script selected brk3, killed it, and recorded SKEW, RETURN, A, and B-real
PASS lines. This is exactly the current `assert_ok`-then-continue control flow at lines 190–221: reconstruction is
not a hard branch prerequisite even though the following comments claim the destructive arms run over a fresh
balanced baseline.

The later reconstruction can also change which agent is on each broker, but no per-exit byte baseline is repeated
after it. `DP_A` is captured only after B-real. Consequently a B-dp failure does expose a real broken data plane,
but the drill cannot always attribute it specifically to this manual move: it did not capture that exact agent's
old home and prove the same agent flowed immediately before the move.

Arm C remains incomplete even in the clean retry where all three setup legs, reconstruction, B-dp, and the manual
negative control passed:

- It checks only env, kill/return, and distribution. It has no SS byte baseline immediately before the C kill and
  no SS closure through the auto-moved exits after the return.
- The ordinary expose `reg` is removed before Arm C. Thus the plan's `__proxy__`-only negative control is tested
  only by an extra manual rebalance after B, not across the automatic return transaction it was specified to
  control.
- `_construct_111` still discards the setup rebalance command rc, so the setup label can attribute an already
  balanced snapshot to a failed command. This is a normal failure mode in this project because lingering
  membership operations already refuse other cluster commands.

Required correction: branch or hard-gate each destructive arm on its own valid distribution **and** exact
nid→home→flow baseline. If the objective is to collect several REDs in one run, rebuild an independent valid
fixture for the next arm rather than accumulating a failed foundation. Capture the exact moved nids before/after
B. Keep a serving ordinary expose across Arm C and require pre-return plus post-auto SS bytes. Include setup
rebalance rc in `_construct_111`.

## R7-M2 — drill 71 still omits the directly executable rebuild-OFF drain refusal arm (Major)

Arm E does not require Arm B to complete successfully. The locked plan explicitly requires a live rebuild-OFF
expose followed by `cluster drain brkH` and an exact `rebuild-OFF ... will NOT be auto-migrated` refusal. The current
fixture already provides precisely that object: `wnr` is proven `rebuild:false`, homed on live brk3, serving before
the crash, and serving again after return. The script then deletes `wnr` before invoking drain and claims E needs a
successful B drain as a prerequisite. That claim is false.

The correct sequence is available without a new topology: while `wnr` and `wstrand` are both live after return,
invoke drain and require the rebuild-OFF refusal signature; then remove `wnr` and execute the rebuild-ON B journey.
If the lingering `NATS_ROLLED_OUT` operation intercepts E first, the signature assertion should RED and expose that
the required rebuild-OFF behavior remains unreachable. That is more informative than a warning that silently
groups E with G/F.

The repository also contains no `_test.go` reference to `ErrRebuildOffExposes` or its refusal text, despite the
new README/ledger claim that this behavior is hermetic-tested. The independent run and its retry both established
the full `wnr` fixture and both reproduced B's rc=70 `NATS_ROLLED_OUT` refusal, so this omission is on the ordinary
path, not a rare constructibility corner.

For Arm B, check the drain command outcome directly. In both external runs the exact #31 signature was visible,
so the product diagnosis is credible; the executable assertion currently ignores `_dmrc`/`_dm_out` and merely
waits another 180 seconds for migration. A wrong refusal or transport failure would receive the same generic RED
description. Split command success/signature from the eventual migrated-home/bytes oracle.

## R7-M3 — drill 73 crosses a failed endpoint cross-check and then prints the opposite diagnosis (Major)

The first independent concurrent run constructed both live Q legs, but `_sub_server_of(agt1)` returned `brk1`
while control-plane home was `brk2`. `Q-xcheck` correctly failed. The script nevertheless killed brk2, the leg kept
flowing, and the black-hole plus separation assertions failed. Its final warning then said the vended server
“matched” the killed home and ruled out stale rendering/status attribution—even though the immediately preceding
failed assertion and log showed `vended server=brk1, home brk2`.

This invalidates the prior R5-M6 closure. A cross-check described as the causal prerequisite must gate the kill,
not merely increment the final failure counter. If it fails, record the endpoint mismatch as the exposed defect
and skip that destructive branch. The separate retry behaved correctly at an earlier boundary: after a flowing
REHOME baseline, the agent's home drifted before injection, `REHOME-precond` failed, and the drill refused the
vacuous kill. Q should use the same control-flow discipline.

## Documentation accuracy and non-blocking corrections

- The #34 entry should distinguish observations from root cause. The runs prove distribution instability and a
  non-terminal op closing C-auto's fire gate. A generic `_ss_via_home` timeout alone does not distinguish missing
  home, `/sub` rendering, ss-local readiness, and sink failure, so it cannot by itself prove “eligibility loss.”
- README, inventory, ledger, and the appended response freeze different 74 counts (`3/33`, `5/31`, `2/34`), while
  the independent run produced `7/29`. Counts are intentionally branch-dependent; document them as per-run facts
  or link a specific retained log rather than presenting one as the stable current verdict.
- The final 74 warning still calls the raw-event limitation “owner-approved” although the revised owner file
  explicitly claims no owner scope decision. “Technically unobservable / review-accepted limitation” is accurate.

## Verified fixes and tests

- PASS: the former duplicate-nid snapshot, failed dry-run command, and empty-explain negative-control adversarial
  probes now return non-zero.
- PASS: `sh -n` for changed drills 71/74 and the shared cluster helper; cached and uncached `git diff --check`.
- PASS: focused Go tests for `TestAutoRebalance*` and the existing drain/rebalance tests. The first broad test
  selector hit an unrelated `httptest` socket sandbox denial; it was retried with exact test names and a writable
  `/tmp` Go cache, then passed.
- `shellcheck` is not installed locally; no result is claimed.
- Local and server hashes matched before execution: 71 `2167ef89...3504`, 74 `f1363dea...9f5e`, cluster helper
  `a6b97bcd...ae81`, simcluster `7d6390de...930`, Dockerfile `31694276...1ca`, current binary
  `497121d3...d7474`, next binary `2ea83152...a29eb5`.

## Independent concurrent simcluster evidence

All access used tether CLI. The current hashes were already staged on `weilandserver`; inotify was 8192. The
first run used three isolated instances concurrently with `-j 3 --no-retry`:

| Drill | Result | Relevant evidence |
|---|---:|---|
| 71 | RED 1 failed / 20 passed | Full fixture and crash core passed; B refused rc=70 by `NATS_ROLLED_OUT`. |
| 73 | RED 3 failed / 39 passed | Embedded grow attempt 1 failed and was visibly retried once; Q endpoint mismatch, dead leg still flowed, separation failed. |
| 74 | RED 7 failed / 29 passed | brk2 setup leg and re-1/1/1 failed but dependent arms continued; B-dp/negative control/C-auto also failed. |

Logs: `/tmp/s3s5-r7/logs/`; runner summary: `/tmp/s3s5-r7/summary.log`; wall time 1122 seconds.

Per the instruction to retry failures, 73 was rerun while 74 was still running. Its grow succeeded on attempt 1,
but the selected exit's home drifted after the verified flowing baseline; `REHOME-precond` failed and the script
correctly aborted before the kill (rc=1, no drill-end verdict). Then 71 and 74 were launched together as a second
concurrent group. 71 again established the full fixture and reproduced rc=70 / RED 1-of-20. The final 74 retry
removed the noisy setup failures: grow, all three SS baselines, reconstructed 1/1/1, manual rebalance, moved-exit
data plane, and the ordinary-expose control all passed. Its sole failure was C-auto: the returned voter still had
zero homes after the locked 180-second window while a `brk3 join in_progress` operation closed the fire gate, so
the result was RED 1 failed / 35 passed. This clean retry confirms a specific automatic-return product gap rather
than a general fixture outage. Retry logs: `/tmp/s3s5-r7-retry73/logs/` and `/tmp/s3s5-r7-retry/logs/`; the latter
runner summary is `/tmp/s3s5-r7-retry/summary.log` (two drills, two RED, 607 seconds).

## Doubts and questions

1. Why is Arm E described as requiring a successful B drain when `wnr` is already live and the plan explicitly
   defines E as the refusal control that should run before removing it?
2. Where is Arm C's required SS data-plane closure and its ordinary-expose negative control? Distribution alone
   is not the locked acceptance oracle.
3. When Q-xcheck says vended=`brk1` and home=`brk2`, why does the script still kill brk2 and later state that the
   endpoints matched?

## Release disposition

Fail. Do not release or mark G-A complete. The new hard REDs are an important improvement and the underlying
product defects are real, but the dependent-arm causality and the missing E/C controls must be corrected before
this suite can safely certify their eventual fixes.

---

## 开发者回复（round-7）

采纳全部 3 项 Major + 3 问 + 3 条非阻塞文档订正。**核心认知（延续 round-6）**：round-6 我把"失败必须 RED"
落实了，但没走到位——**失败的因果前置条件仍旧只 +1 计数、然后让依赖它的破坏步骤照跑**，等于用一个失败的地基
去撑后面的 PASS 行。你 round-7 点破的这一层正是 mandate 的第二个推论：**失败的因果前置必须 GATE（分支/跳过）
依赖它的破坏步骤，不是仅仅进失败计数器**。三个 drill 已按此改。**只改控制流（门控/拆分/无条件负控），不动任何
oracle 的判定强度**——不是为了变绿，恰恰相反：门控之后暴露得更干净。

### R7-M1（drill 74：失败的锁定地基仍流入破坏步骤；Arm C 缺数据面闭环 + 普通-expose 负控）— 已改

- **SKEW-reconstruct 现为硬门控（跳过，非累积）**（`74:211-217`）：`if poll_until … _construct_111; then RECON=1;
  else RECON=0; fi` → `assert_ok "SKEW-reconstruct …" sh -c "[ '$RECON' = 1 ]"`（失败即 RED）→ **`if [ "$RECON" != 1 ]`
  则 warn NOT-COVERED-THIS-RUN + `drill_end; exit`**，破坏臂（SKEW/RETURN/A/B/C）**一条不跑**。你 run 里"re-1/1/1
  失败后仍 selected brk3、kill、记 SKEW/RETURN/A/B-real PASS"的 `assert_ok`-then-continue 控制流已消除。
- **`_construct_111` 现要求 rebalance 命令 rc=0**（`74:41`）：`_rebal >/dev/null 2>&1 || return 1; _spread_zero`——
  一个已经均衡的快照不再能被归因给一个**失败的**命令（挂起 op 会拒 cluster 命令，这是本项目常见失败态）。
- **B-dp 归因：捕获精确 moved-nid + per-nid flow 基线**。① `BEFORE_HOMES` 在 B 前捕获每个 agent 的 home
  （`74:260`），`DP_BEFORE` 取出 moved exit 的旧 home（`74:285`），日志显式判定它是否真从旧 home MOVED 到 KTGT
  （`74:286`）。② **新增 `B-flow-pre` per-nid 数据面基线**（`74:266-273` + helper `_ss_via_agent` `74:56`）：
  B-real 前对**每一个当前 exit（按 nid）**证其 flow 字节——所以 B 移到 KTGT 的那个 exit（DP_A）在移动前**被证 live**；
  它若在移到 KTGT 后 strand，**归因就锁死为"这次移动 stranded 它"**，而非一个既有坏数据面。你说的"未捕获该 exit
  移动前的旧 home、未证同一 agent 移动前 flow"两点都补上了。
- **Arm C 数据面闭环 + 普通-expose 负控**（回答问题 2）：① `C-ss-pre`（`74:318`）—— C-kill **前**的 SS 字节基线
  （pre-auto data-plane baseline）。② `C-dp`（`74:349-351`）—— auto-moved exit 上的 post-auto SS 闭环；**条件于
  auto 真发火**（`C_AUTO=1`），因为 auto 没发火时**没有 auto-move 可穿**（这是诚实的条件，不是擦屁股）。③ **`C-negctrl`
  现无条件**（`74:360`，移出了 `C_AUTO` 门控）：普通 expose `reg`（homed 在 tunnel broker brk1、永不是 KTGT）在
  **整个 Arm C 返回事务**（kill+return+auto 窗口）中必须 home 不变 + 仍 serving，**无论 auto 是否发火**——一个只在
  "被控对象发火时才跑"的负控根本不算负控。④ **`reg` 全程 keep alive 跨 Arm C**（B 后不再删；`74:361` 后才删），
  所以负控真的横跨 automatic return transaction，而不是仅由 B 后一次额外 manual rebalance 测。

### R7-M2（drill 71：可直接执行的 rebuild-OFF drain 拒绝仍被遗漏）— 已改（回答问题 1）

- **Arm E 现为直接执行、在移除 wnr 之前**（`71:144-159`）：return 后 `wnr`（rebuild:false）+ `wstrand` **都 live**
  时直接 `cluster drain brk3 --now`，捕获 rc + 输出，**3 路分支**：命中 rebuild-OFF 拒绝签名（clusterdrain.go:665）
  → **E PASS**；命中 `#31 in flight / NATS_ROLLED_OUT` → **E RED（该签名被 #31 拦截、不可达——比"静默把 E 与 G/F
  归并成 NOT-COVERED"更有信息量）**；两者皆非 → **E RED（undocumented/transport）**。E 完成后 abort drain、删 wnr、
  keep wstrand 走 rebuild-ON 的 B 旅程——完全按你规定的序列。**E 不再要求"成功的 B drain 作前提"**，那个说法本就是错的。
- **Arm B 现拆命令结果 / 迁移 oracle**（`71:161-176`）：直接检 drain 命令 `_dmrc`/`_dm_out`——`rc=0` → **B-cmd PASS**
  + 跑 `B-migrate` 迁移 oracle（迁到 survivor + serve，180s）；命中 #31 签名 → **B-cmd PASS（documented block）** +
  **B-migrate RED（release-blocking，迁移不可达）**；其他 → **B-cmd RED（undocumented）** + **B-migrate RED**。
  一个 wrong-refusal / transport failure 不再拿到与 #31 相同的泛化 RED。
- **撤回 "hermetic-tested" 假称**（回答你的 documentation 点 + 诚实起见连 rebuild-ON 一并订正）：仓库确无任何 `_test.go`
  引用 `ErrRebuildOffExposes` 或其拒绝文本（我核实过：clusterdrain.go:655 有该错误类型，但 `_test.go` 零引用）。
  进一步核实：hermetic 覆盖的是 drain 的 **marker + phase-advance + rehome-target-exclusion**（clusteradmin_test、
  g1g7 A9），而**端到端 drain-migrate 数据面**（rebuild-ON expose 真迁到 survivor 并 serve）与 **rebuild-OFF 拒绝**
  **都无 `_test.go` 覆盖**。README（`71 行`）、gotcha ledger（#29 条）、71 drill 内注（`71:184`）三处的"rebuild-ON
  migrate + rebuild-off 拒是 hermetic-tested"已撤回，改成准确陈述：**drill 71 Arm B/E 是它们唯一的覆盖**。

### R7-M3（drill 73：端点交叉校验失败后仍继续 kill 并输出与证据相反的诊断）— 已改（回答问题 3）

- **Q-xcheck 现门控 kill**（`73:321-345`）：`Q-xcheck` 断言 `_dsrv = DEAD_HB`（失败即 RED），随后
  **`if [ "$_dsrv" = "$DEAD_HB" ]; then <kill + black-hole + separation> else <记录 endpoint mismatch 为暴露缺陷 +
  跳过 kill> fi`**。端点不匹配时**拒绝破坏性 kill**（`73:344`），并显式声明"the leg vends via `$_dsrv`, not `$DEAD_HB`
  —— a vacuous kill；QUORUM separation NOT-COVERED THIS RUN；**no contradictory 'endpoints matched' claim is made**"。
- **Q-DIAGNOSIS 不再输出与证据相反的诊断**（`73:342`）：该 warn 现**只在 xcheck-PASSED 分支内**，其文本明说
  "vended server=`$_dsrv` == home `$DEAD_HB`, Q-xcheck **PASSED**"——只有真匹配时才声称匹配。你 run 里"vended=brk1、
  home=brk2 却仍说 endpoints matched 并排除 stale 渲染"的矛盾已根除。这与 REHOME-precond 失败后拒绝 vacuous kill 用
  的是同一套控制流纪律（你也肯定了 retry run 里 REHOME-precond 的正确行为）。

### 非阻塞文档订正 — 3 条已改

1. **#34 区分"观测"与"根因"**（gotchas #34 条）：新增**证据分级**——【已证 A】分布不稳定（1/1/1→brk1=3，`home_broker`
   计数**控制面直读、无歧义**）；【已证 B】C-auto fire-gate（运行时 `cluster ops ls` 见非终态 op）；【观测但未独立归因】
   SS 腿超时——**明确标注单条 `_ss_via_home` 超时无法区分 missing-home / `/sub` 渲染 / ss-local readiness / sink 失败，
   故不单独证 "eligibility loss"**；真正钉住 eligibility 不稳定的是已证 A 的分布漂移。"eligibility 不稳定"机制标为
   **从 home 计数推断、未经源码/事件独立确认**。
2. **74 计数标为 per-run 事实**（README `74 行` + inventory 对应行）："**RED, per-run（failed 数 branch-dependent，勿
   把单个数当稳定判定）**：跨 run 观测 1–7 failed（干净重试 `RED 1/35` 仅 C-auto；噪声 run `3/33`/`5/31`/`7/29`）；
   **稳定的 release-blocking RED = B-dp(#33) + C-auto(#31 fire-gate)**，总数随该 run 有多少瞬态 setup/eligibility 腿
   一起失败而变（每条这种 RED 本身即 #34 的暴露）"。你 run 的 `7/29` 已纳入该区间。
3. **owner-approved → technically-unobservable / review-accepted**（`74:363` 最终 warn）：raw-event 无 reader 的限制现表述
   为 "**a TECHNICALLY-UNOBSERVABLE / review-accepted limitation — NOT an owner scope decision**"，与已订正的
   owner-decisions.md（声明无 owner scope decision）一致。

### 验证与硬闸

- 三个改动 drill `dash -n` 通过 + 无未转义反引号（`grep -nE '^[^#]*` ` `'` 排除 `\` ` `` 干净）。
- `_ss_via_agent`、`B-flow-pre` 循环、无条件 `C-negctrl` 均只依赖既有 lib（ss_sub_fetch/ss_up/ss_curl_ok/dexec），未新增 lib 依赖。
- 三个 drill 已 sha256-verified 同步到 weilandserver，并**并发**（`run-drills.sh 74 71 73`，非串行）发起验证跑
  确认新分支正确执行（结果附于本节末，见下方"验证跑结果"）。deploy-tier drill 是本流程分内活。

### 验证跑结果（并发，两组，weilandserver）

**均经 `run-drills.sh <names>` 并发（非串行）**，本地→服务器 sha256-verified 同步。两组：v1 = `74 71 73` 并发
（`/tmp/s3s5-r7v`）；v2 = 修正版 `71 73` 并发（`/tmp/s3s5-r7v2`）。**三个 R7-M3/M2/M1 修复的每条控制流分支都被真跑覆盖**：

- **74 — RED 3/38（R7-M1 全链路真跑）**：
  - `SKEW-reconstruct` **PASS** → RECON 门通过、破坏臂跑在**有效** 1/1/1 基线上（门在这个方向工作；反方向=RECON 失败则
    跳过全部破坏臂——见 drill 内 `if [ "$RECON" != 1 ]` 分支）。
  - `B-flow-pre exit agt1` / `agt2` **PASS**（per-nid 移动前基线）→ **`B-dp` FAIL 带精确归因**：日志
    "*an SS leg MUST flow through the exit MOVED onto brk3 (**agent agt2, was on brk1 before B**) … RED if STRANDS =
    #33-family*"——agt2 **移动前被证 flow**、移到 brk3 后 **strand**，正是你要的"该 exit 移动前 flow + 精确旧 home"归因。
  - `C-ss-pre` **PASS**（pre-kill 数据面基线）；`C-auto` **FAIL** + FIRE-GATE 诊断实测 leader `cluster ops ls` 有
    `brk3 join in_progress`（#31 关闭 `no in-flight op` gate 的运行时证据）；`C-dp` **NOT-COVERED**（auto 没发→无 auto-move
    可穿，条件化正确）；**`C-negctrl` PASS 且无条件跑**（C_AUTO=0 仍跑，reg 仍 homed brk1 + serving）。
  - 3 failed = B-dp(#33) + C-auto(#31) + 1 条瞬态 setup-SS 腿（#34 不稳定）——正是文档订正 2 说的 per-run 波动。
- **71 — RED 2/21（两组一致，R7-M2）**：`E [rebuild-OFF DRAIN REFUSAL]` **FAIL**（被 #31 rc=70 NATS_ROLLED_OUT 在到达
  rebuild-off 检查前拦截 → RED 暴露"不可达"，非静默 NOT-COVERED）；`B-cmd` **PASS**（documented #31 block）/ `B-migrate`
  **FAIL**（迁移不可达）——命令结果与迁移 oracle 的拆分生效。
- **73 — 三分支全覆盖**：
  - v1 **RED 1/33**：dead-homed 腿（agt2 on brk2）pre-kill 未渲染 → 走**更早的 finding-8 门**（拒绝对 dead baseline 做
    vacuous kill），Q-xcheck 门未触及。
  - v2 **GREEN 42**：dead-homed 腿渲染出来 → **`Q-xcheck` PASS**（agt1 的 /sub-vended server == 待杀的 brk2 == 控制面
    home，端点一致）→ **门放行 kill** → 完整 quorum separation 证（black-hole + survivor-flows + SEPARATION + write-fenced）
    **全 PASS**。
  - 你 round-7 那次的 **mismatch→skip** 分支（vended≠home → 记录 endpoint mismatch 缺陷 + 拒绝 kill + 无矛盾诊断）是第三条
    路径。三条合起来，R7-M3 的门控在 pass / earlier-gate / mismatch 三种情形都真跑验证。

**backtick 噪声**：验证中发现我在 71 warn 里引入的 `` `_test.go` `` 未转义（v1-71 日志有一条 `_test.go: not found`），已去反引号
+ 重同步；v2 修正版 71 日志该条**已消失**。剩余的 `/opt/sim/install.sh: 1: cluster: not found`（每 drill 3×）是**预存的
install.sh provisioning 产物**（所有轮次/所有 drill 都有、不影响任何判定、与 R7 无关），非本轮引入，未在本轮范围内动它。

日志：v1 `/tmp/s3s5-r7v/`、v2 `/tmp/s3s5-r7v2/`（服务器留存）。

> 本轮仅改控制流 + 文档，未弱化任何 oracle。外审不过不算 done——改完停在此，等你 round-8 重审。
