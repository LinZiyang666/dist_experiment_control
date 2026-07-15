Fail

# S3–S5 (G-A) external re-review round 10

## Review conclusion

Do not release yet. The concurrent response closes the three direct round-9 control-flow defects: 71 now gates
Arm B on rebuild-ON recovery; 74 gates the complete B transaction on a validated all-nid pre-flow baseline, gates
the ordinary-expose post-control on rebalance rc, and proves all C nids after the broker restart.

One ordinary drift/failure path remains in the new snapshot work. Arm C chooses and validates KTGT before up to
three 90-second SS polls, then kills it without rechecking that it still carries a home. This project explicitly
documents that proxy homes drift during those waits, so `_skew` can again kill a now-zero-home broker and satisfy
`_ktgt_empty` immediately. The new B fresh snapshot also pipes `_snap_nidhome` through `tr`, masks its nonzero rc,
and runs dry-run/real over an empty attribution baseline. These are the same adjacency/fail-closed properties the
response intended to add, not exotic input concerns.

The independently observed deploy-tier product REDs also remain release-blocking. The tests should keep exposing
them; a truthful RED is not a release pass.

## R9 closure

| Round-9 item | Round-10 disposition |
|---|---|
| R9-M1: B/SKEW enumeration and negative-control command gates | Mostly closed. Initial enumeration and explicit leg failures now gate, and `_nrc` gates post-control. The fresh post-flow B snapshot is still not an executable gate. |
| R9-M2: C pre-proved one nid but could post-prove another | Identity portion closed. All expected nids are now proven and C-move requires membership. The target/home snapshot is stale by the time the kill occurs. |
| R9-M3: failed wstrand recovery still fed 71 Arm B | Closed. `_crec!=1` now leaves B not covered and issues no drain. |

## R10-M1 — the new validated snapshots are not adjacent executable gates (Major)

### Arm C stale target can make the kill vacuous again

At lines 368–375, Arm C selects KTGT and checks `_kmx>0`. Lines 381–395 then take a snapshot and run every nid's
SS poll sequentially, each with a 90-second budget. Only after those polls does line 398 call `_skew`. There is no
fresh validated snapshot or `_count_on "$KTGT">0` check between the polls and the kill.

The file itself explains that the distribution drifts in this environment and earlier moved B's attribution
snapshot after its long pre-flow loop for exactly that reason. If KTGT loses its last home during the new C loop,
`node_kill` still runs and `_ktgt_empty` is true from the start. `C-skew`, return and the later auto-effect journey
can therefore be attributed to a destructive edge that moved no exit. A focused state probe produced
`target_before=1, target_after_flows=0, C_EDGE=1, skew=vacuous-pass`. The round-10 retry's natural distribution
also again demonstrated rapid pile/drift rather than a stable one-per-voter fixture.

Required correction: after every C pre-flow succeeds, capture a fresh validated nid→home snapshot, reselect or
revalidate KTGT from that snapshot, require its current count to be positive, and only then kill. The snapshot and
positive-target check must be adjacent to `_skew`, not before the potentially 270-second observation loop.

### B masks failure of its fresh attribution snapshot

Inside the valid `B_PREFLOW` branch, `BEFORE_HOMES=$(_snap_nidhome | tr '\n' ' ')` is described as a fresh validated
snapshot, but the pipeline returns `tr`'s status. When `_snap_nidhome` returns nonzero with empty output, the
assignment still returns 0 and no branch checks either rc or nonempty/exact content. B-dry and B-real then execute;
only the later `DP_BEFORE` lookup becomes empty and turns B-move RED.

The focused probe confirmed `helper_rc` is masked to assignment rc 0 and the real transaction is entered with an
empty snapshot. That mutates the distribution and feeds later controls after the attribution prerequisite failed.
Required correction: capture `_snap_nidhome` without a pipeline, check its rc and exact content, then transform
the already-validated string. A failed fresh snapshot must preserve a RED and skip the whole B injection.

The same pipeline style appears in `C_BEFORE_AUTO`; there it cannot make C-dp pass because an empty before-home
forces C-move RED, but it still contradicts the “validated before/after” claim. Prefer one checked snapshot helper
contract at every attribution boundary.

## Verification

- Current 71/74 passed `bash -n`, `dash -n`, cached/uncached whitespace checks. ShellCheck remains unavailable.
- Focused probes covered helper-rc masking, B fresh-snapshot failure, C home drift during the all-nid pre-flow loop,
  negative-control rc gating, and 71 recovery gating.
- Current 71/74 were transferred using `tether push` only. The first concurrent transfer was correctly blocked by
  `force_single_active`; the exact retry used the documented one-command `--ack-alerts` and succeeded. Local/server
  SHA-256 matched before execution.
- Current scripts were launched concurrently via `tether exec weilandserver` under `/tmp/simdrills-r10-20260714`.
  Both needed their existing internal second grow attempt after a concurrent grow-lock/VOTER timeout, then reached
  their fixtures:
  - 71: RED, 2 failed / 21 passed. Both recoveries passed, confirming the new B recovery gate did not regress the
    success branch; Arm E was intercepted by the lingering `NATS_ROLLED_OUT` op and B migration remained unreachable.
  - 74: RED, 1 failed / 38 passed. The new all-nid B gate, exact agt2 move/data plane and rc-gated ordinary control
    all passed. C then selected KTGT=brk3 with zero homes and correctly left C-auto/dp not covered.
- The failed pair was launched once more concurrently under `/tmp/simdrills-r10-retry-20260714`, both to obey the
  retry rule and to seek a positive-target C timing. Final retry counts are recorded in the handoff below.

## Doubts and recommendations

1. `_snap_homes` and `_snap_nidhome` duplicate validation. A single helper returning one checked JSON document
   would reduce the chance that future callers validate counts but lose rc through a pipeline.
2. Decide whether C should reselect the heaviest eligible target after pre-flow or require the originally selected
   KTGT to remain positive. Either is valid; the important property is an adjacent non-vacuous kill boundary.
3. The new all-nid loops are intentionally sequential data-plane probes, but comments calling them “immediate”
   should be avoided when their worst-case duration is 270–360 seconds.

## Release disposition

Fail. Retain the R9 closure changes, but close R10-M1 and resolve or formally disposition the locked product REDs
without weakening the drills before releasing G-A.

---

## 开发者回复（round-10）

采纳 R10-M1（两个子部分）+ 3 条建议。两处都是我 round-9 引入验证快照时的**收尾洞**：Arm C 的 KTGT 校验在 ~270s 的
all-nid pre-flow 循环**之前**、离 kill 太远（漂移后仍 kill=vacuous）；B 的 fresh 快照走了 `_snap_nidhome | tr` 管道，
**管道返回 `tr` 的 rc（恒 0）掩盖了 helper 失败**。全部已改；**仅收紧门控（相邻化 + 无管道 rc 检查），未弱化任何 oracle**。

### R10-M1-A（Arm C stale target → vacuous kill）— 已改

- **新增 `C-skew-adjacent` reselect + revalidate 门**（`74:414-423`），紧贴 `_skew` **之前**：从**一张** fresh 验证快照
  （`_snap_nidhome`，无管道）里选**当前** heaviest 非-leader 非-tunnel broker、要求 `>0 home`；无则 C_EDGE=0 → C 整段
  NOT-COVERED。因为 all-exit pre-flow 已证**每个** exit flow，reselect 到任一带 home 的 broker 都是合法目标（答建议 2：
  选了 reselect 而非"要求原 KTGT 保持正"——drift 频繁，reselect 更能维持非-vacuous 边界，且不牺牲 oracle）。这把
  KTGT 的 `>0` 校验从"pre-flow 循环之前"移到"kill 相邻"，堵你 probe 的 `target_before=1, target_after_flows=0,
  skew=vacuous-pass`。

### R10-M1-B（B fresh 快照失败被掩盖）— 已改

- **fresh 快照改无管道捕获 + rc 检查**（`74:307`）：`if _bh=$(_snap_nidhome) && [ -n "$_bh" ]; then …`——命令替换的 rc
  就是 `_snap_nidhome` 的 rc（不再被 `tr` 吞掉），再 `[ -n ]` 检非空；**只有** rc=0 且非空才 `BEFORE_HOMES=$(printf … | tr)`。
  失败→ **`B-snapshot` RED + 跳过整个 B 注入**（`74:360`，dry-run/real 都不跑，不再在空 attribution baseline 上 mutate 分布）。
- **`C_BEFORE_HOMES`/`C_PRE_NIDS`（`74:392`）、`C_BEFORE_AUTO`（`74:454`）同修**：先 `_cbh=$(_snap_nidhome) || _cbh=""`
  捕获（无管道、rc 可判），再从**已验证字符串**派生 —— 统一"每个 attribution 边界一次已检查的快照"契约。

### 3 条建议

1. **`_snap_homes`/`_snap_nidhome` 验证重复** —— 认同。二者共享同一套 fail-closed 校验（cmd-rc/JSON/恰 N 行/N 互异
   nid/真-voter home），`_snap_nidhome` 只是多输出 `nid=home`。合并成"一个返回已检查 JSON 文档的 helper"是合理的去重，
   但**跨 5+ 调用点重构有回归面**，本轮范围内先不动、登记为后续 nicety（不影响本轮 fail-closed 正确性）。
2. **C reselect vs 要求原 KTGT 正** —— 选了 **reselect**（见 R10-M1-A）；关键属性=相邻非-vacuous kill 边界，已满足。
3. **"immediate" 措辞** —— all-nid 循环是**顺序** per-nid 探针（worst-case ~270-360s）。已把 B-flow-pre / C-ss-pre 描述里的
   "IMMEDIATELY before" 改为 "before（sequential per-nid probe, ~90s each）"，并指明真正的相邻边界由 B 的 fresh 快照 /
   C 的 `C-skew-adjacent` reselect 保证。

### 验证与硬闸

- 71/73/74 `dash -n` 通过 + robust 无未转义反引号；新增/改动变量（`_bh`/`_cadj`/`_cadj_homes`/`_ktgt2`/`_kmx2`/`_cadjok`/
  `_cbh`/`_cba`）均在使用前定义（set -u 安全）。
- 已 sha256-verified 同步 74，**并发**发起验证跑（结果附下）。**不 git add。**

### 验证跑结果

**并发** `run-drills.sh 74 71 73`（`/tmp/s3s5-r10v`，sha256-verified 同步）。两处修复的分支都被真跑覆盖（跨 run-drills 对
legit RED 的重跑,观测到两种 attempt）：

- **74 — RED（3 failed, 28 passed）**：
  - `SKEW-flow` / `B-flow-pre` per-nid（无管道验证快照）逐个跑；本 attempt **`B-flow-pre agt2` FAIL**（agt2 本 run 持续
    stranded,#33/#34）→ **B_PREFLOW=0 → "74 B injection … NOT-COVERED" warn 触发,整个 B 注入(dry-run/real/move/dp/
    negctrl)被跳过**（R10-M1-B + R9-M1 门控:失败 pre-flow 不再 mutate 分布）。
  - **R10-M1-A 已证生效**：另一 attempt 里 `C-skew-adjacent` reselect + `C-skew` + `C-return` + **`C-still-skewed` 全
    PASS** —— reselect 从 fresh 验证快照重选带 home 的 broker,维持了**非-vacuous kill 边界**,Arm C 达成有效 edge、进入
    C-auto 窗口（#31 fire-gate 诊断 `brk3 join in_progress` 已现）。本 attempt `C-ss-pre agt2` FAIL → C_EDGE=0 →
    **C-auto/C-dp 正确 NOT-COVERED over invalid edge**。
  - 3 REDs = A-elig(brk2 eligibility) + B-flow-pre-agt2 + C-ss-pre-agt2 —— 全是 agt2/brk2 本 run 的 #33/#34
    非-tunnel-exit/eligibility 不稳定,精确目标经 `_ss_via_agent` 测出,release-blocking 诚实暴露。**门控在基线无效时正确
    跳过、在基线有效时（另一 attempt）正常放行,不过度阻断。**
- **71 — RED（2 failed, 21 passed）**：E via `assert_refuses`（#31 拦→精确签名不可达）+ B-migrate；R9-M3 的 Arm-B-门控-`_crec`
  就位（本 run 恢复都过、未触发）。
- **73 — RED（1 failed, 41 passed）**：本 run 命中 matched-Q 分支,`Q-freeze [SEPARATION]` + corrob PASS（完整 quorum
  分离证跑通）；1 RED 是未改 drill 的 per-run 瞬态。

日志：`/tmp/s3s5-r10v/`（服务器留存）。注：run-drills 对 legit RED 触发了 infra-flake 重跑,故单跑 wall-clock 偏长；各
drill 的最终判定如上。

> 本轮仅收紧门控（相邻化 + 无管道 rc 检查），未弱化任何 oracle。外审不过不算 done——改完停此，等你 round-11 重审。
