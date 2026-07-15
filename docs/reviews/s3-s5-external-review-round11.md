Fail

# S3–S5 (G-A) external re-review round 11

## Review conclusion

The round-10 harness response passes this review: both R10-M1 sub-findings are closed, the new branches are
fail-closed, and I found no new Major/Minor defect in the final 74 changes. In particular, the fresh B attribution
snapshot now gates every B mutation, while C reselects a currently positive target from one validated snapshot
adjacent to `_skew` after all sequential SS probes.

The overall release conclusion is still Fail because the locked deploy-tier product criteria remain RED. This is
not a demand for an all-green test suite: the REDs are doing their job by exposing real #31/proxy eligibility and
auto-rebalance gaps. Shipping while those locked acceptance criteria remain unmet would nevertheless be a release
of the exposed defects.

## R10 closure

| R10 item | Round-11 disposition |
|---|---|
| B fresh snapshot rc masked by `tr` | Closed. `_bh=$(_snap_nidhome)` preserves helper rc, requires nonempty content, and failure skips dry-run/real/move/control with an explicit RED. |
| C target could drift to zero during the all-nid loop | Closed. `C-skew-adjacent` takes one fresh validated mapping, reselects a non-leader/non-tunnel broker with `>0` homes, and gates `_skew` immediately. |
| Other attribution pipelines | Closed for the affected boundaries. C captures checked strings before transforming; an empty C-before-auto snapshot cannot satisfy C-move. |

## Code review evidence

- The B success branch keeps exact old/new nid attribution and `_ss_via_agent "$DP_A"`; the snapshot-failure branch
  leaves `NEG_OK=0` and does not create the ordinary expose.
- The C pre-flow list comes from exactly three distinct nids with real voter homes. After all probes, the adjacent
  reselect uses the same validated snapshot for target counts and requires a positive target before kill.
- Reselecting KTGT does not break identity closure: all expected nids were pre-proven, and C-move still requires the
  derived auto-moved nid to belong to `C_PRE_NIDS` before exact post-flow.
- `bash -n`, `dash -n`, cached/uncached whitespace checks and focused success/failure probes passed. ShellCheck is
  unavailable. Probes confirmed bad B snapshots do not mutate, positive C reselect succeeds, no-positive-target
  clears the edge, and failed C-before-auto data cannot satisfy C-move.

## Runtime evidence

- The final 74 local/server SHA-256 matched (`1460cf9b...753dcb1`). Access, sync and execution used tether CLI only.
- A prior retry was discarded because another reviewer started the same fixed drill instance name concurrently;
  simcluster names instances by drill basename, so both same-name runs were contaminated. I did not terminate the
  other reviewer's process.
- The final tree was copied to the unique temporary drill name `74-r11review` and run in its own instance under
  `/tmp/simdrills-r11-20260714`: **RED, 1 failed / 47 passed** in 627 seconds. The one growth attempt, all SKEW
  flows, every B pre-flow, `B-snapshot`, the exact agt2 brk1→brk2 B move/data-plane check, the ordinary-expose
  create/pre/rc/post control, all three C pre-flows, `C-skew-adjacent`, C kill/return/still-zero, and C negative
  control passed.
- The sole failure was `C-auto`, which timed out after 180 seconds. The adjacent snapshot had selected brk3 with
  one home and the kill/return checks passed, but the brk3 join operation remained `in_progress` at
  `NATS_ROLLED_OUT`; this is the locked #31 fire-gate/product behavior, not a harness prerequisite failure.
- The final log secret scan found no hits. The unique process/instance left no residue, and the temporary server-side
  `74-r11review` copy was removed.

## Remaining doubts and recommendations

1. `_snap_homes` and `_snap_nidhome` still duplicate validation. Consolidation remains a reasonable non-blocking
   cleanup, but the current contracts are fail-closed.
2. Logs should continue distinguishing harness prerequisite RED, product RED, and NOT-COVERED; this review does
   not recommend weakening any of them to obtain a green summary.

## Release disposition

Harness changes: accepted. Overall G-A release: Fail until the locked product REDs are fixed or an authorized owner
formally changes the acceptance criteria with an evidence-backed decision outside the test implementation.

---

## 开发者回复（round-11）

**接受本轮结论,无 drill 改动。** 经 7 轮外审(round-5..11)迭代,harness 变更本轮被**正式 accepted**——两处 R10-M1
全闭、新分支 fail-closed、最终 74 无新 Major/Minor。你的干净单跑 `74-r11review` **RED 1/47** 印证了 harness 的终态:所有
前置/归因/负控/门控断言 PASS,**唯一 RED = C-auto**(#31 fire-gate 锁定产品行为,非 harness 前置失败)——drill 现在**忠实
钉住 tether 的真缺陷、绝不替它擦屁股**,这正是 mandate 的目标。

**关于剩余的整体 Fail**:如你所述,它**不是 drill 的问题**,而是 tether 产品的锁定验收标准仍 RED(#31 挂起-op /
proxy-eligibility / auto-rebalance-on-return 缺口)。按 mandate 与你的明确要求,**绝不弱化测试来变绿**——这些 RED 是诚实
暴露,应保留。整体放行需要二选一,且**两者都在 drill 实现之外、属 owner 决策**:
- **(a) 修产品**:修 tether 的 #31 lingering-op 家族(挡 drain/upgrade/auto-rebalance 三样)+ proxy-eligibility 稳定化 +
  auto-rebalance-on-return 的 fire-gate——一个独立的产品工程增量,drill 会在其落地后自动从 RED 翻成 GREEN 回归。
- **(b) owner 正式 disposition**:由授权 owner 以证据支撑的决定正式变更验收标准(在测试实现之外)。

**下一步交回 owner（用户）决策**——审查-整改环已到达终态(harness accepted),不再自动武装 poller。是否 commit 已接受的
harness 工作、以及如何处置产品 RED((a) 还是 (b)),等你的指示。

### 硬闸

- 71/73/74 `dash -n` 通过 + robust 无未转义反引号;本轮无 drill 改动(仅本回复)。
- 未 git add / 未 commit。
