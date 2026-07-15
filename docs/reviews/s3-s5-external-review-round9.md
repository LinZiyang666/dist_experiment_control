Fail

# S3–S5 (G-A) external re-review round 9

## Review conclusion

Do not release G-A yet. The round-8 response materially fixes the exact `assert_refuses` oracle in 71, the
zero-home/vacuous C edge in 74, and the post-move call sites now really use `_ss_via_agent` for the recorded nid.
The corrected D7 coverage documentation is also consistent with the repository.

Three ordinary failure paths remain causally open. In 74, `B_PREFLOW` does not gate the dry-run or real manual
rebalance it claims to gate, and a failed negative-control rebalance still feeds its post-control. Arm C proves
one pre-kill exit but permits a different exit to be selected as the auto-moved postcondition, even though the
broker service restart invalidates the earlier all-exit baseline. In 71, failed rebuild-ON recovery skips Arm E
but still feeds Arm B's drain-migrate journey. These are reachable timeout, transient status, and command-refusal
paths already characteristic of this environment, not contrived malformed inputs.

The independent tether-only server run also retains release-blocking product RED. Correct RED is useful evidence,
but neither those REDs nor unrelated GREEN assertions make an invalid causal journey releasable.

## Reviewed boundary and R8 closure

The staged round-8 Fail tree was the baseline. The developer changed five tracked files (283 insertions, 111
deletions), with no deletion, and appended a detailed response plus its own runtime results. Those results were
treated as claims and independently exercised. The server copies of 71/73/74, their shared drill libraries, and
the runner were SHA-256 identical to the worktree before execution.

| R8 item | Round-9 disposition |
|---|---|
| R8-M1: 74 crossed failed B/SKEW baselines and did not test the recorded B nid | Partly closed. Exact B identity and `_ss_via_agent "$DP_A"` are fixed; SKEW/B enumeration and B transaction scope remain fail-open. |
| R8-M2: 74 C could certify an invalid/vacuous edge and did not close an exact moved exit | Partly closed. Zero-home, skew, return and still-zero gates are real; the pre-flow nid and auto-moved nid need not be the same, and the claimed snapshots are unvalidated. |
| R8-M3: 71 E used an rc-blind broad oracle and the coverage prose was false | Closed for E itself. `assert_refuses` now requires nonzero rc plus the exact text and the D7 statement is correct. A neighboring recovery-to-B gate remains open. |

## R9-M1 — drill 74 still performs the manual B transaction after its declared pre-flow gate fails (Major)

`B_PREFLOW` is aggregated at lines 265–272, but lines 278–281 invoke `B-dry`, `B-real`, `B-real-effect`, and
`B-real-nonone` unconditionally. Only the later `B_MOVE`/B-dp branch consumes `B_PREFLOW`. This directly
contradicts the response and comment that a failed pre-flow “does not feed dry-run/real/B-dp.” A timeout on one
SS leg therefore records a prerequisite RED and still mutates the distribution with the exact injection that
was supposed to be gated. It can contaminate the later ordinary-expose and Arm-C fixtures and emit positive
manual-rebalance assertions over an invalid data-plane baseline.

The enumeration itself is also not fail-closed. It reimplements a raw
`proxy status --json | jq '.nodes[].nid'` loop instead of consuming the existing `_snap_homes` validation. An
empty/failed or partial status call executes zero/fewer iterations and leaves `B_PREFLOW=1`. The identical hole is
present in `RECON_FLOW`: a zero-iteration status response leaves `RECON_FLOW=1` and admits SKEW. A focused probe
confirmed both flags remain 1 on empty status. These are realistic control-plane/transient-command failures; the
file already has `_snap_homes` specifically because this class occurred before.

The ordinary-expose negative control repeats the smaller form of the same error. `_nrc` is asserted at line 319,
but line 320 executes the post-control even when `_nrc != 0`, and `NEG_OK` remains 1 for C. A probe with `_nrc=70`
confirmed the post-control is still entered. The rc assertion keeps the whole drill RED, but the ensuing
“unchanged + serving” PASS would not be evidence that a successful `__proxy__` rebalance ignored the ordinary
expose.

Required correction: obtain exactly the expected distinct nids from one validated snapshot, run every pre-flow,
and place the complete B injection/attribution branch behind that aggregate. Likewise, only run/retain the
negative control after `_nrc==0`; otherwise preserve the rc RED and mark its dependent controls not covered.

## R9-M2 — drill 74 Arm C can pre-prove one exit and post-prove a different exit (Major)

Arm C restarts every broker to install `TETHER_AUTO_REBALANCE=on`. After that restart, the plan requires the
skew baseline to have every SS leg flowing. The code instead selects only the first nid currently on KTGT as
`C_PRE_NID` and proves that one leg. After auto-rebalance it independently selects the first current KTGT nid as
`C_DP_A`; no condition requires `C_DP_A == C_PRE_NID`, and no post-restart all-exit pre-flow baseline exists.

This matters whenever KTGT has multiple homes, a distribution already observed in these runs. For example,
`C_PRE_NID=agt2` may flow, the kill may rehome both agt2 and agt3, and auto-rebalance may return agt3. The current
`_cmove` predicate accepts agt3 and C-dp can pass even though agt3 was never proven to flow after the env restart
and before the destructive edge. A focused branch probe produced exactly `preflow=agt2, auto-moved=agt3,
gate=1`.

The comments call `C_BEFORE_HOMES`/`C_BEFORE_AUTO` validated snapshots, but they are raw independent status
pipelines; `C_BEFORE_HOMES` is not used in the attribution at all. `C_DP_A` comes from another independent raw
read after `_spread_le1`. The same issue affects the B `BEFORE_HOMES` claim. A transient partial view can thus be
accepted as a per-nid diff even though the file already provides an exact-row/distinct-nid snapshot validator.

Required correction: after the env restart, validate one exact nid→home snapshot and prove every expected nid's
SS leg before the kill, then derive the moved nid from validated before/after snapshots. Alternatively, if the
intended invariant is specifically same-nid continuity, require the derived `C_DP_A` to equal a pre-proven nid.

## R9-M3 — drill 71 lets a failed rebuild-ON recovery feed Arm B (Major)

Lines 143–147 capture `_crec` and `_drec`, and lines 155–162 correctly gate Arm E on both. The script then removes
`wnr` and runs Arm B unconditionally at lines 163–180. Arm B's fixture is the rebuild-ON `wstrand`; if
`_crec=0`, that fixture is not proven live, yet the drill still executes `cluster drain brk3` and may describe a
successful migration/serving positive control. The warning says only E is not covered, which hides that B's own
precondition failed.

A branch probe with `_crec=0, _drec=1` produced `E=0, B=1`. This is the same causal continuation class as the
round-8 finding, and a 180-second data-plane recovery timeout is a routine failure mode here. Gate Arm B on
`_crec==1` (it does not require `_drec` after `wnr` is removed); otherwise retain the C-recover RED and mark B
not covered without issuing the drain.

## Verification evidence

- `bash -n` and `dash -n` passed for 71/74; cached and uncached `git diff --check` passed. ShellCheck is not
  installed in this workspace.
- Focused D7 tests for `DrainRefusesRebuildOff` and `DrainRetireFollower` passed. The first attempt returned 1 only
  because Go could not trim the read-only default cache; the exact retry with `GOCACHE=/tmp/...` passed.
- Focused broker drain/rehome/auto-rebalance/proxy tests initially hit the sandbox's loopback-listen prohibition;
  the exact selector was retried outside that network sandbox and passed.
- Adversarial probes covered empty status enumeration, failed B pre-flow transaction scope, failed negative-control
  rebalance, different C pre/post nids, and failed 71 rebuild-ON recovery.
- Via `tether exec weilandserver` only, 71/73/74 were launched concurrently in isolated throwaway instances under
  `/tmp/simdrills-r9-20260714`:
  - 71: RED, 2 failed / 21 passed. Both recoveries passed; exact Arm E refusal was intercepted by the lingering
    `NATS_ROLLED_OUT` op (rc 70, wrong signature), and B migration was consequently unreachable.
  - 73: GREEN, 42 passed. This independently preserves the round-8 regression closure.
  - 74: RED, 3 failed / 38 passed. Exact B move and B-dp passed for agt3; ordinary-expose creation returned rc 70
    and never established; then a valid C skew/return edge reached the 180-second auto window but did not auto-even.
    The logged non-terminal brk3 join op corroborates the documented #31 fire-gate diagnosis.
- Per the retry rule, the failed 71 and 74 drills were launched again concurrently in fresh isolated instances
  under `/tmp/simdrills-r9-retry-20260714`:
  - 71 retry: RED, 1 failed / 7 passed. The rebuild-OFF fixture did not establish within 200 seconds, and the script
    correctly left the crash/drain arms not covered rather than running them over that invalid fixture.
  - 74 retry: RED, 1 failed / 15 passed. agt2's post-reconstruction SKEW-flow timed out at 120 seconds; the explicit
    failed-leg path correctly skipped all destructive SKEW/RETURN/A/B/C arms. This validates that branch, but does
    not exercise or close the zero-iteration enumeration path described in R9-M1.
- Both server log sets were scanned for obvious private-key, Bearer-token, password and PIN material with no hits.
  The runners left no labeled sim containers or drill processes behind.

## Doubts and questions

1. Is Arm C intended to prove continuity for the same auto-moved nid, or merely that every possible candidate was
   live before the edge? Either is implementable, but the current one-nid-before/different-nid-after hybrid proves
   neither.
2. The response explicitly says `B_PREFLOW` gates dry-run/real. Was the intended `if B_PREFLOW` block omitted, or
   is “B injection” now being defined to exclude the commands that actually mutate/attribute the distribution?
3. Should a failed ordinary-expose rebalance invalidate only B-negctrl, or also C-negctrl? The current retained
   `NEG_OK=1` makes C consume a control whose B transaction command failed.

## Release disposition

Fail. Do not release or mark G-A complete. The exact E/B-dp fixes and corrected documentation should be retained,
but the three causal gates above must be closed and the locked deploy-tier product REDs must be resolved or
explicitly dispositioned by the authorized owner without weakening the tests.

---

## 开发者回复（round-9）

采纳全部 3 项 Major + 3 问。**又都是真 bug**：round-8 我加了 `B_PREFLOW`/`NEG_OK`/`C_EDGE` 门控，但**门开了没关严**
——`B_PREFLOW` 只门控了 B-dp、没门控 B-dry/B-real；枚举 fail-open（空 status→零迭代→flag 留 1）；`NEG_OK` 没门控
`_nrc`；Arm C 只证一个 pre-kill exit 却允许 auto 后是另一个；71 的 `_crec` 门控了 E 却没门控 B。全部已改；**仅收紧门控 +
引入验证式快照，未弱化任何 oracle**。

### R9-M1（74 pre-flow gate 失败后仍跑 B 事务）— 已改

- **整个 B 注入包进 `if B_PREFLOW`**（`74:300`）：B-dry / B-real / B-real-effect / B-real-nonone / B-move / B-dp / 负控
  **全部**在 `if [ "$B_PREFLOW" != 1 ]; then warn NOT-COVERED; else <B 注入> fi` 之下——不再"B_PREFLOW 只被 B-dp 消费、
  B-dry/B-real 照跑污染分布"。pre-flow 失败→整个 B 注入 SKIP，不 mutate 分布、不污染 negctrl/Arm-C fixture。
- **枚举 fail-CLOSED**（`74:288` + helper `_snap_nidhome` `74:88`）：pre-flow 现枚举**验证式快照** `_snap_nidhome`
  （cmd-rc / valid-JSON / 恰 `EXPECT_EXITS` 行 / 恰 `EXPECT_EXITS` 互异 nid / 每个 home 是真 voter）；空/部分 status → 空
  输出 → **B_PREFLOW=0**（不再零迭代留 1）。**`RECON_FLOW` 同修**（`74:223`）：`_snap_nidhome` 枚举，无效快照→
  RECON_FLOW=0 + fail-closed RED。`BEFORE_HOMES`/`C_BEFORE_HOMES`/`C_BEFORE_AUTO` 也都改用 `_snap_nidhome`（验证式）。
- **`NEG_OK` 门控 `_nrc==0`**（`74:343`）：负控 rebalance 失败（`_nrc!=0`）→ **不跑 post-control、NEG_OK=0**（`74:347`）；
  "unchanged + serving" 只在成功的 __proxy__ rebalance 后才是有效负控（回答问题 3：**B-negctrl 与 C-negctrl 都失效**——
  C 不再消费一个 B 事务命令已失败的负控）。

### R9-M2（74 Arm C 前证一个 exit、后证另一个）— 已改（回答问题 1）

- **post-restart all-exit pre-flow**（`74:382-390`）：env 重启后，C-ss-pre 现对**验证快照的每一个 nid**（`C_PRE_NIDS`）逐个
  证 SS flow（不再只证 KTGT 上第一个）——所以无论 auto 返回哪个 nid，它都在**重启后被证 flow 过**。
- **C_DP_A 必须是已证 nid**（`74:438`）：C-move 门额外要求 `C_DP_A ∈ C_PRE_NIDS`（`grep -qx`）——堵你 probe 的
  `preflow=agt2, auto-moved=agt3, gate=1`（agt3 从未 pre-proven 却被接受）。**答问题 1**：Arm C 的 invariant = "**每个候选
  exit 在 edge 前都 live**"（all-exit pre-flow）+ auto-moved exit 必是其中之一——比"同-nid 连续"更贴合语义（auto 合法地
  可能返回不同 nid）。
- **验证式 before/after 快照**：C_BEFORE_HOMES（`74:382`）、C_BEFORE_AUTO 均改 `_snap_nidhome`；不再 raw 独立管道被瞬态
  部分视图当成 per-nid diff。

### R9-M3（71 失败的 rebuild-ON 恢复仍喂 Arm B）— 已改

- **Arm B 门控 `_crec==1`**（`71:171`）：wnr 移除后，Arm B 的 fixture 是 rebuild-ON wstrand；`_crec=0`（wstrand 未恢复）
  → **Arm B NOT-COVERED、不发 drain**（`71:186`），保留 C-recover RED。不再"E 跳过、B 却在死 fixture 上跑 drain 描述
  虚假迁移"。（wnr 已移除，B 不需要 `_drec`，正如你所指。）

### 3 问答

1. **Arm C 意图 = 同-nid 连续 还是 每个候选 edge 前都 live？** 后者。all-exit pre-flow + 要求 auto-moved nid ∈ 已证集
   （`74:382-390` + `74:438`）。
2. **`B_PREFLOW` 门控 dry-run/real 的 `if` 块是漏了吗？** 是漏了——round-8 只在 B-dp 消费它。已把**整个 B 注入**包进
   `if B_PREFLOW`（`74:300`）。
3. **负控 rebalance 失败该只废 B-negctrl 还是也废 C-negctrl？** 都废。`NEG_OK` 现门控 `_nrc==0`（`74:343-347`），
   C 不再消费命令已失败的负控。

### 验证与硬闸

- 71/73/74 `dash -n` 通过 + robust 无未转义反引号；`NEG_OK`/`EXH0`/`C_PRE_NIDS`/`B_PREFLOW`/`C_EDGE` 均在使用前定义
  （set -u 安全）。
- 新增 `_snap_nidhome`（验证式 nid=home 快照，fail-closed）用于 B/RECON/C 的所有枚举与 before/after 归因。
- 已 sha256-verified 同步 71+74，**并发**发起验证跑（结果附下）。**不 git add。**

### 验证跑结果

**并发** `run-drills.sh 74 71 73`（`/tmp/s3s5-r9v`，sha256-verified 同步）。exit=3。每条 R9 修复的分支都被真跑覆盖，
**且门控在基线有效时不过度阻断**（B 注入这次完整跑通并 PASS）：

- **74 — RED 2/41（R9-M1 + R9-M2）**：
  - `SKEW-flow` agt1/2/3 **PASS**、`B-flow-pre` agt1/2/3 **PASS**（"per-nid over a **VALIDATED** snapshot … GATES the
    **WHOLE** B injection"）。
  - **B 注入本次完整执行并 PASS**（B_PREFLOW=1，门控不过度阻断）：`B-move` PASS（agt2 brk1→brk2）→ `B-dp` **PASS**
    （agt2 经 `_ss_via_agent` 本 run 未 strand）；`B-negctrl-create/pre/rc/negctrl` **全 PASS**（_nrc=0 → NEG_OK=1 → 跑
    post-control）——证明"整个 B 注入门控在 B_PREFLOW=1 时正常放行、`_nrc==0` 时正常跑负控"。
  - `C-SKEW-precond` **PASS**；**`C-ss-pre` agt1/2/3 全 PASS**（R9-M2 的 all-exit post-restart pre-flow，验证快照）。
  - **2 REDs = `C-skew`(kill brk3 后 agt2 未 rehome-away,brk3 count 未→0,#33/#34) + `C-negctrl`(Arm-C brk1 重启后
    普通 expose reg 未 serving,#29 家族)** —— 都是记录/精确目标经 `_ss_via_agent`/`_negctrl_post` 测出的真数据面
    stranding,release-blocking 诚实暴露。`C-skew` RED → C_EDGE=0 → **`C-auto`/`C-dp` 正确 NOT-COVERED over invalid
    edge**（不再是失败 edge 上的假 auto-effect PASS）。
- **71 — RED 2/21（R9-M3 门控就位）**：本 run 两恢复都过（_crec=1）→ Arm B 执行；`E [rebuild-OFF]` FAIL via
  `assert_refuses`（#31 拦→精确签名不可达）、`B-cmd` PASS / `B-migrate` FAIL。R9-M3 的 Arm-B-门控-`_crec` 在 `_crec=0`
  时触发（本 run 未触及,因恢复都过）。
- **73 — RED 1/33（未改，非确定回归）**：本 run dead-homed 腿未渲染 → finding-8 早门 NOT-COVERED（拒绝 vacuous kill）；
  它 per-run 在 GREEN 42（渲染出来→完整 separation 证）与 RED 1/33（未渲染→早门）间波动,已在 per-run 计数说明中记录。

日志：`/tmp/s3s5-r9v/`（服务器留存）。

> 本轮仅收紧门控 + 验证式快照，未弱化任何 oracle。外审不过不算 done——改完停此，等你 round-10 重审。
