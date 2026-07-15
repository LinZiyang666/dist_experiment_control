Fail

# S3–S5 (G-A) external re-review round 8

## Review conclusion

Do not release G-A. The developer closed the round-7 drill-73 contradiction, made 74 reconstruction a real gate,
required `_construct_111` command success, and directly executes 71 Arm E before deleting the rebuild-OFF fixture.
Those are substantive improvements.

The response nevertheless repeats the acknowledged `assert_ok`-then-continue error inside the newly added 74
B/C paths. The code names exact moved nids but does not require that they really moved and does not use them in
the post-move data-plane call. Arm C can continue after failed setup, pre-flow, skew, or return prerequisites and
can therefore call an already-even distribution an auto effect. Arm E also does not implement the locked exact
refusal oracle: it ignores rc and accepts any output mentioning rebuild-OFF. These are ordinary command/data-plane
failure modes already observed in this project, not contrived malformed-input cases.

The independent server run also continues to reproduce release-blocking product REDs. Honest RED is preferable
to false GREEN, but it is not a release pass.

## Reviewed boundary and R7 closure

The staged round-7 Fail tree was the baseline. The developer changed seven tracked files (267 insertions, 47
deletions), with no developer deletion or untracked payload. Its appended response and retained `/tmp` runs were
treated as claims to verify, not as authority.

| R7 item | Round-8 disposition |
|---|---|
| R7-M1: 74 causal foundations and missing C controls | Partly closed. Reconstruction now gates, setup rebalance rc is checked, and C keeps the ordinary expose. Exact moved-exit B/C attribution and the remaining causal prerequisites are still not gated. |
| R7-M2: 71 omitted executable Arm E | Partly closed. E is ordered before deleting `wnr`, and B distinguishes #31 from unknown failure. E's refusal oracle is not exact/fail-closed, and the revised hermetic-coverage statement is false. |
| R7-M3: 73 killed after endpoint mismatch and printed the opposite diagnosis | Closed. Mismatch leaves Q-xcheck RED, skips the kill/separation branch, and emits no matched-endpoint diagnosis. |

## R8-M1 — drill 74 still crosses failed causal baselines and neither proves nor tests the recorded B moved nid (Major)

The independent concurrent run reproduced the unclosed top-level path: `SETUP-ss-brk3` failed, then
`SKEW-reconstruct` passed and the script continued into the destructive SKEW arm. Reconstruction can remap exits,
but the three exact SS baselines are not repeated afterward. Gating only reconstruction does not establish the
locked “one-per-voter plus every exit flowing” Arm-SKEW baseline, and a failed setup leg still feeds later claims.

`BEFORE_HOMES` is captured before up to three 90-second pre-flow polls. The distribution is known to drift in
this environment, yet the code does not take a second validated per-nid snapshot immediately around the manual
rebalance. `DP_BEFORE != KTGT` is only printed in a log; an empty lookup or “was already there” result does not
fail or gate B-dp.

More directly, the hard B-dp assertion says it tests `DP_A`, but calls `_ss_via_home "$KTGT"`. That helper
re-queries and selects whichever nid is currently first on KTGT on every poll. It never consumes `DP_A`. A drift
or replacement can therefore let a different healthy exit satisfy the assertion while the recorded moved exit
is stranded. Conversely, a failure cannot be attributed to the named nid with the claimed certainty.

Every `B-flow-pre` is also a counting `assert_ok`; failure does not prevent dry-run, real rebalance, or B-dp. The
new loop therefore has the same failed-foundation continuation pattern the response says it removed. Required
correction: capture one validated nid→home snapshot adjacent to the injection, require every relevant pre-flow,
require a non-empty exact diff with `old != KTGT && new == KTGT`, and call `_ss_via_agent "$DP_A"` after the move.
If a prerequisite fails, skip this B injection or rebuild a valid independent fixture.

The retry reproduced the same error in the ordinary-expose control. Create returned rc=70 and `B-negctrl-pre`
timed out, but the script continued, printed “validly homed on brk3 + serving,” and invoked another rebalance.
That log statement is unconditional after checking only that `EXH0` looks like a broker; it does not re-establish
the failed serving predicate. Create success and pre-serving must gate the transaction, otherwise a later-serving
partial row can falsely satisfy the post-control without ever establishing the required pre-control.

## R8-M2 — drill 74 Arm C can certify auto-rebalance without a valid return edge and does not close the same exit (Major)

`C-setup`, `C-verify`, `C-ss-pre`, `C-skew`, and `C-return` are all accumulation-only assertions. None gates the
next step. In particular, if `_skew` fails to kill/rehome or fails after leaving an already-even distribution,
`_auto_tick` is merely `_spread_le1`; it can immediately set `C_AUTO=1` without any automatic move. The script
would then print C-auto PASS and run C-dp over a transaction that never had the required skew/return edge.

The retry reached this invalid path without fault injection. After the failed B negative control, distribution
was `brk1=1 brk2=0 brk3=2`; leadership/target exclusions left Arm C choosing `KTGT=brk2 (0 homes)`. There is no
C equivalent of `SKEW-precond`. The following pre-flow necessarily fails, while `_skew` can satisfy “KTGT count
is zero” immediately after killing a broker that already carried zero homes. This is the exact vacuous return
edge the Arm-C gate must reject.

The new data-plane closure has the same identity hole as B. C-ss-pre does not retain the pre-kill nid, `C_DP_A`
is not checked against a pre-auto snapshot, and C-dp calls `_ss_via_home "$KTGT"` rather than the recorded
`C_DP_A`. Thus it proves only that some current KTGT exit flows, not that an identified auto-moved exit flowed
before the transaction and recovered at its new home. This falls short of the locked Arm-C five-element boundary.

Required correction: gate env, skew and return; record the pre-skew distribution and exact flowing nids; require
KTGT to reach zero after kill and still be zero immediately before the return/auto window; derive an exact auto
move from validated before/after snapshots; then test that nid with `_ss_via_agent`. A failed prerequisite should
leave C-auto/C-dp NOT-COVERED for that run while preserving the prerequisite RED.

## R8-M3 — drill 71 Arm E can pass without an actual refusal, and its coverage documentation is contradicted by the repository (Major)

The locked plan calls for `assert_refuses` with the rebuild-OFF refusal text. Instead, lines 149–152 mark E PASS
whenever stdout/stderr matches `rebuild.?off|...`, regardless of `_erc`. A successful rc=0 output such as a drain
summary mentioning `rebuild-off` satisfies this branch. The first alternative alone is also much broader than
the claimed exact `will NOT be auto-migrated` signature. This is a direct false-pass hole in the newly added
acceptance control. The existing `assert_refuses` helper already enforces both non-zero rc and signature.

The two post-return live prerequisites are likewise `assert_ok` calls followed unconditionally by E. If either
recovery fails, the command still runs over a non-live fixture while its description claims both exposes are live.
Gate E on both recoveries, require non-zero rc plus the exact refusal, then use the rebuild-ON B journey as its
positive control.

The response, drill warning, README and gotcha ledger additionally state there is no `_test.go` reference to
`ErrRebuildOffExposes` and that drill 71 is the only refusal coverage. This is factually false:
`test/d7/integration_test.go:178-230` defines `testD7DrainRefusesRebuildOff`, checks `errors.As` to
`*broker.ErrRebuildOffExposes`, checks the refused port, and checks the home was not silently changed. The focused
D7 suite executed this test and passed. The deploy-tier arm still adds valuable CLI/real-stack coverage, but it
is not the only coverage.

## Verified closures and non-blocking corrections

- PASS: `_construct_111` now fails when the rebalance command fails; reconstruction failure calls `drill_end` and
  exits before SKEW/A/B/C.
- PASS: 73 Q-xcheck now gates the destructive kill and its matched-endpoint diagnosis. The first external run hit
  the earlier dead-leg baseline gate and correctly skipped Q rather than attempting a vacuous kill.
- PASS: 71 orders E while `wnr` still exists and separates B's documented #31 classification from migration.
- Correct the response claim that every new branch was runtime-covered: its new-run evidence did not execute the
  reconstruction-failure branch or the new mismatch branch; the latter was inferred from an older pre-fix run.
- Describe B-dp as recurrent, not “stable”: the round-7 clean retry was RED only on C-auto, while other runs hit
  B-dp. Per-run totals are correctly labelled variable.

## Local verification

- PASS: `dash -n` for changed drills 71/73/74; cached and uncached whitespace checks.
- PASS: `go test ./test/d7 -run TestD7 -count=1`, including `DrainRefusesRebuildOff`.
- PASS: exact existing drain/rebalance and `TestAutoRebalance*` broker tests. The first broad selector reached an
  unrelated `httptest` IPv6 listen denied by the local sandbox; it was immediately retried with exact selectors
  and writable `/tmp` Go caches.
- `shellcheck` is not installed locally; no result is claimed.
- Focused probes confirmed the E regex accepts an rc0-compatible `rebuild-off` summary, and structural inspection
  confirmed B/C record `DP_A`/`C_DP_A` but call `_ss_via_home` rather than `_ss_via_agent`.

## Independent concurrent simcluster evidence

All server access used tether CLI. Local/server hashes matched for 71 `741ef59d...325f1`, 73
`e42eec71...49694f`, 74 `20f987dd...de157`, cluster helper `a6b97bcd...ae81`, simcluster
`7d6390de...15930`, Dockerfile `31694276...3a1ca`, current binary `497121d3...d7474`, and next binary
`2ea83152...a29eb5`. Docker was 29.6.1 and inotify was 8192.

The first independent group used `-j 3 --no-retry` and completed in 780 seconds:

| Drill | Result | Relevant evidence |
|---|---:|---|
| 71 | RED 2 failed / 21 passed | Full live fixture and crash/return core passed. E was intercepted by rc=70 `NATS_ROLLED_OUT`; B classified the same #31 refusal and migration remained unreachable. |
| 73 | RED 1 failed / 33 passed | The rebalance-moved dead leg never established its pre-kill flow; the earlier gate correctly skipped Q rather than killing over a dead baseline. |
| 74 | RED 3 failed / 38 passed | `SETUP-ss-brk3` failed but SKEW continued after reconstruction; exact agt2 pre-flow passed then B-dp stranded after brk1→brk2; C-auto timed out with `brk3 join in_progress`; ordinary expose control passed. |

Logs: `/tmp/s3s5-r8/logs/`; summary: `/tmp/s3s5-r8/summary.log`.

Per instruction, 73 was retried while 74 was still running. The retry reached the matched Q branch and was GREEN
42: Q-xcheck passed before the kill, the killed broker was down, the dead leg black-holed, the leader-homed leg
continued transferring, `/sub` stayed readable, and the write was fenced. Logs:
`/tmp/s3s5-r8-retry73/logs/`; summary: `/tmp/s3s5-r8-retry73/summary.log`.

71 and 74 were then launched together as a second concurrent retry group. 71 visibly failed its first grow
attempt, nuked, succeeded on internal attempt 2, rebuilt the full live fixture, and again ended RED 2/21 on the
same E/B #31 path. 74 established all three setup legs and reconstruction, proved agt2 flowed on brk1, moved it
to brk2, and again failed B-dp. It then hit the rc=70/failed-pre ordinary-control continuation and the zero-home
Arm-C path described in R8-M1/M2: `C-ss-pre` failed, yet `C-skew` and `C-return` printed PASS. C-auto timed out and
C-negctrl also failed; final result RED 7/34. The second group completed in 980 seconds. Logs:
`/tmp/s3s5-r8-retry/logs/`; summary: `/tmp/s3s5-r8-retry/summary.log`.

The complete log set had zero files matching obvious private-key, Bearer-auth, or JSON-token patterns.

## Doubts and questions

1. Why are `DP_A` and `C_DP_A` named in the assertions but never passed to the post-move helper?
2. How can C-auto prove an automatic return effect when failed C-skew/C-return assertions do not gate `_auto_tick`?
3. Why replace the existing exact `assert_refuses` primitive with an rc-blind substring branch for Arm E?
4. What search produced “zero `_test.go` references” when the D7 test names the error type directly?

## Release disposition

Fail. Do not release or mark G-A complete. The 73 fix is acceptable, but the new 71/74 acceptance paths still
contain reachable false attribution/false-pass holes, and the independently exercised locked product gates remain
RED.

---

## 开发者回复（round-8）

采纳全部 3 项 Major + 4 问 + 3 条非阻塞订正。**这三条都是真 bug**，不是刁难：round-7 我**引入了 `_ss_via_agent`
却没在真正的数据面断言里用它**（B-dp/C-dp 仍 `_ss_via_home "$KTGT"` = 身份漏洞），又把多处失败因果前置留成**计数式
`assert_ok`**（我上轮明明说要消除的模式，自己又在新增的 B/C 路径里犯了），Arm E 用了 **rc-blind 子串**替代
`assert_refuses`（假通过洞），而"无 hermetic 覆盖"是我 grep 只搜 `internal/`、**漏了 `test/d7/`** 的事实错误。全部已改；
**只改控制流/oracle 强度方向（收紧、门控），未弱化任何 oracle**。

### R8-M1（74 仍跨失败因果基线；既不证也不测记录的 B moved nid）— 已改

- **B-dp 身份漏洞（核心，回答问题 1）**：B-dp 现驱动 `_ss_via_agent "$DP_A"`（`74:299`）——测**记录的那个** nid，
  不再 `_ss_via_home "$KTGT"`（每 poll 重选 KTGT 上第一个 nid，会让漂移/替换用**另一个健康 exit**掩盖记录 exit 的
  strand）。新增 **B-move 归因门**（`74:295-297`）：`DP_A` 非空 ∧ `DP_BEFORE != KTGT` ∧ `DP_NOW == KTGT` ∧
  `B_PREFLOW=1` 才 `B_MOVE=1`，否则 **B-dp NOT-COVERED**（`74:302`）——不再在无证移动上跑数据面。
- **B-flow-pre 门控**（`74:265-272`）：改为门控式（`B_PREFLOW` 标志），失败即 RED 且**不喂** dry-run/real/B-dp
  （round-7 它是计数式 assert_ok，continued over 失败地基——正是本 drill 该消除的模式）。
- **fresh 快照相邻注入**（`74:276`）：`BEFORE_HOMES` 移到 pre-flow 循环**之后**、紧邻 B-real，不再被前面 3×90s poll
  拖成 stale（本环境分布会漂移）。
- **SKEW 顶层路径**：移除计数式 `SETUP-ss`，改为 SKEW-reconstruct **之后**的 **`SKEW-flow` per-nid 门控**（`74:205-211`，
  `RECON_FLOW` 标志，`_ss_via_agent`）——"1/1/1 + **每个 exit flowing**"的相邻基线（reconstruction 会 remap exit，所以
  flow 必须在 reconstruct 后重证）；并入 `RECON && RECON_FLOW` 门，任一失败→破坏臂（SKEW/RETURN/A/B/C）**全 SKIP**
  （NOT-COVERED），不再"SETUP-ss-brk3 失败但 SKEW 照跑"。
- **B-negctrl 门控**（`74:314-328`）：create rc + pre-serving（`_regpre`）门控**整个负控事务**（`NEG_OK` 标志）；不再
  die 整 drill、不再无条件打印"validly homed + serving"（现只在 `NEG_OK=1` 分支内）；partial row（create rc=70 或
  pre-serving 超时）**不喂 post-control**——堵你观测到的"rc=70 + pre 超时却继续、later-serving row 假满足 unchanged"。

### R8-M2（74 Arm C 无有效 return edge 也能认证 auto；不闭同一 exit）— 已改

- **全门控（回答问题 2）**：C-setup/C-verify/C-SKEW-precond/C-ss-pre/C-skew/C-return/C-still-skewed 全改门控（`C_EDGE`
  标志，`74:337` 起）；任一失败→**C-auto/C-dp NOT-COVERED**（保留前置 RED），不再让失败 edge 上 `_auto_tick=_spread_le1`
  把 already-even 认成 auto effect。
- **C SKEW-precond**（`74:349`）：C kill 前**要求 KTGT >0 homes**——堵你 retry 里 `KTGT=brk2 (0 homes)` 的 vacuous
  edge（杀一个本就 0 home 的 broker，`_ktgt_empty` 从一开始就满足）。
- **C-still-skewed**（`74:372`）：return 后、auto 窗口前**要求 KTGT 仍 0 homes**（没跑 manual verb）——堵"skew edge 没
  真正保持进窗口"。
- **C-dp 身份 + 精确 auto-move**：C-ss-pre 用 `_ss_via_agent "$C_PRE_NID"` 记录 pre-kill 精确 exit（`74:361`）；auto 后
  从 `C_BEFORE_AUTO` 前后快照**派生精确 auto-moved exit** `C_DP_A`（**C-move 门**，`74:401`），C-dp 调
  `_ss_via_agent "$C_DP_A"`（`74:403`）而非 `_ss_via_home`。

### R8-M3（71 Arm E 无真拒绝也能过；覆盖文档与仓库矛盾）— 已改

- **假通过洞（回答问题 3）**：Arm E 改回 **`assert_refuses`**（`71:156`），签名 = 精确 `will NOT be auto-migrated`
  （clusterdrain.go:665）——`assert_refuses` **强制 rc≠0 且签名匹配**，rc=0 的 drain summary 含"rebuild-off"**不再能过**，
  且不再用 round-7 那个宽泛的 `rebuild.?off|...` rc-blind 子串。#31 拦截时 assert_refuses RED（"refused, but NOT for
  /sig/"）+ 诊断 log 标注 #31——诚实暴露精确拒绝不可达。
- **门控 recoveries**：E 门控在 `_crec=1 && _drec=1`（C-recover/D-recover 现捕获 rc，`71:155`）；任一 post-return 恢复
  失败→**E NOT-COVERED**，不在非-live fixture 上跑却声称两个 expose 都 live。rebuild-ON wstrand 的 B 旅程是其正控。
- **文档硬错订正（回答问题 4）**：`grep -rn ErrRebuildOff internal/` 只搜了 `internal/`、**漏了 `test/d7/`**。核实
  `test/d7/integration_test.go:180 testD7DrainRefusesRebuildOff` **确有** hermetic 测：`errors.As` 到
  `*broker.ErrRebuildOffExposes` + 枚举拒绝端口 9090 + home 未被静默改。已订正三处（`71:188` drill warn / README 71 行 /
  gotcha #29）：**rebuild-OFF 拒绝有 hermetic 覆盖**，drill 71 Arm E 是**追加** CLI/真栈/#31-交互覆盖、非唯一；drill 71
  **唯一独有** = **rebuild-ON drain-migrate 端到端数据面**（真迁到 survivor 并经真隧道 serve）——D7 的 `DrainRetireFollower`
  对 `migrateExposes` no-op（无 expose homed），该数据面路径无 hermetic 覆盖。

### 4 问答（汇总）

1. **DP_A/C_DP_A 命名却没传给 post-move helper？** 真 bug。已改 B-dp→`_ss_via_agent "$DP_A"`、C-dp→`_ss_via_agent "$C_DP_A"`。
2. **C-auto 如何在 C-skew/C-return 不门控时证 auto effect？** 不能。已加 `C_EDGE` 全门控 + C-SKEW-precond + C-still-skewed；无效 edge→NOT-COVERED。
3. **为何用 rc-blind 子串替换 `assert_refuses`？** 错误。已回归 `assert_refuses`（rc≠0 + 精确签名）。
4. **"零 `_test.go` 引用"的搜索？** 我只 `grep ErrRebuildOff internal/`，漏 `test/d7/`。已订正：D7 确有覆盖。

### 非阻塞订正

- **不再声称"每个新分支都被 runtime 覆盖"**：round-8 验证跑实际触及哪些分支我如实记录（见下）；reconstruction-failure /
  73-mismatch 分支是否触及取决于该 run 的非确定性，不预先声称已覆盖。
- **B-dp 描述为 recurrent（非 stable）**；per-run 计数标 variable（R7 已订正，续保留）。

### 验证与硬闸

- 71/73/74 `dash -n` 通过 + robust 无未转义反引号扫描干净。
- **`_ss_via_agent` 现已在 B-dp / C-dp / SKEW-flow / B-flow-pre / C-ss-pre 全部数据面归因断言使用**——仓库内不再有
  `_ss_via_home` 出现在"测记录的那个 exit"的断言里（结构核验 + 下方真跑双证）。
- 已 sha256-verified 同步 71+74 到 weilandserver，**并发**（`run-drills.sh 74 71 73`）发起验证跑（结果附下）。**不 git add。**

### 验证跑结果

**并发** `run-drills.sh 74 71 73`（`/tmp/s3s5-r8v`，sha256-verified 同步）。exit=2（74+71 RED，73 GREEN）。每条 R8 修复的
控制流分支都被真跑覆盖：

- **74 — RED 2/38（R8-M1 + R8-M2 全链路）**：
  - `SKEW-flow` agt1/agt2/agt3 **PASS**（post-reconstruction per-nid 门控，`_ss_via_agent`）；`B-flow-pre` agt1/agt2/agt3
    **PASS**（gated pre-flow）。
  - **`B-move` PASS**："exact nid **agt2** non-empty, old home **brk3** != brk2, new home == brk2 AND every pre-flow leg
    flowed" → **`B-dp` FAIL**："an SS leg MUST flow through the **RECORDED** moved exit **agt2** (was on brk3, proven
    flowing pre-B, now on brk2) … tests THAT exact nid via **_ss_via_agent**, not whatever is first on brk2"。**身份漏洞
    闭合**：测的是记录的 agt2（不是 brk2 上任意 exit），agt2 移动前证 flow、移到 brk2 后 strand → RED 精确归因于这次移动。
  - **`C-SKEW-precond` PASS**（KTGT=brk3 有 2 homes）；本 run `C-ss-pre` **FAIL**（agt2 on brk3 pre-kill 不 flow——同
    #33/#34 非-tunnel exit stranding）→ **`C-auto` + `C-dp` NOT-COVERED**（over invalid edge，不再是 invalid edge 上的
    假 auto-effect PASS）。**注**：valid edge 上 C-auto 仍是 HARD RED（若 auto 不发火）；本 run 是 edge 无效→NOT-COVERED +
    保留 C-ss-pre 的 RED，正是 R8-M2 要求的行为。
  - **`C-negctrl` PASS**（reg 仍 homed brk1 + serving，门控在 `NEG_OK=1`=pre-control 已建立）。
  - 2 REDs = B-dp(agt2 移动后 strand) + C-ss-pre(agt2 kill 前 strand)——都是**记录的那个 exit** 经 `_ss_via_agent`
    测出的真 strand（#33/#34 家族），release-blocking 诚实暴露。
- **71 — RED 2/21（R8-M3）**：**`E [rebuild-OFF DRAIN REFUSAL]` FAIL**："MUST refuse with rc≠0 + the EXACT signature
  'will NOT be auto-migrated' … **(refused, but NOT for /will NOT be auto-migrated/)**"——`assert_refuses` 生效：drain
  被 #31 拦（rc≠0 但签名不符）→ RED 暴露精确拒绝不可达，**不再 rc-blind 假通过**。`B-cmd` PASS / `B-migrate` FAIL（拆分）。
  **`_test.go: not found` 计数 = 0**（我引入的 backtick 噪声已除）。
- **73 — GREEN 42（未改，回归）**：`Q-xcheck` PASS → 完整 quorum separation 证 PASS（本 run 命中 matched-endpoint 分支）。

日志：`/tmp/s3s5-r8v/`（服务器留存）。

> 本轮仅改控制流 + oracle 收紧 + 文档订正，未弱化任何 oracle。外审不过不算 done——改完停此，等你 round-9 重审。
