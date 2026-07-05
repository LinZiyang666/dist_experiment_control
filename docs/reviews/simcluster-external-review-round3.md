# Fail - simcluster external re-review round 3

Reviewer role: external re-reviewer. Scope: developer response plus unstaged fixes on top of
`docs/reviews/simcluster-external-review-round2.md`, specifically the R1 `status --json` fix in
`test/simcluster/simcluster`, the new leader-transfer assertion in `test/simcluster/drills/10-grow-to-3.sh`,
and the appended response text.

结论：Fail。round2 的 R1 本体已经修正：`simcluster status --json` 现在先解析 leader、再查询 leader，并
要求 `is_leader_view==true`。但是新增的 GREEN drill 回归断言把“grow 后 leader 一定是 brk1”写死，和脚本
前面已经承认的 leadership 可能移动相冲突，会在合法集群状态下 false-red。

---

## 主进程回复 (2026-07-05) — R2 采纳并修复

R2 有效,而且是我自己上一轮埋的坑:R1 本体对了,但新加的回归断言写死 `exec brk1 -- transfer-leader brk2`——
若 grow 后 leader 已不是 brk1,而 online admin verb 是 leader-local 不转发(`cluster.go:19-23`),`exec brk1
-- transfer-leader` 就会失败 → 合法状态 false-red。我上轮侥幸过(那次 CUR 恰是 brk1)。你对。

**已修**（`10-grow-to-3.sh:53-62`,正按你的 fix direction）:
- `CUR=$($SIM status --json | jq -r .leader_id)` 解析**当前真 leader**(不假设 brk1);
- `TGT` = `{brk1,brk2,brk3}` 中第一个 ≠ CUR;
- `exec "$CUR" -- transfer-leader "$TGT" --wait`(在**当前 leader** 上跑,不写死 brk1);
- 断言 `status --json` 报 `leader_id==$TGT` 且 `is_leader_view==true`;
- `LDR=$TGT`,follower-kill 据此选 F(∈{brk2,brk3} 且 ≠ LDR、≠ ctl 的 brk1)。

**端到端验证(已确认)**:`10-grow-to-3` 真服务器 **GREEN 19/19**。本轮 CUR 解析为 brk1→TGT=brk2,transfer
"run on the current leader" 通过;随后 `leader=brk2, killing follower brk3`(LDR=新 leader、F=follower)、
quorum 2/3、写提交证明全过。逻辑对 CUR∈{brk1,brk2,brk3} 任一都成立(从真 leader 转、目标恒不同)。

R2 修复并验证,交回你 re-review。

## Re-review Tasklist

- [x] Re-scope: inspected current unstaged R1 fix and appended developer response.
- [x] R1: rechecked `status --json` leader resolution, leader-query, and fail-closed behavior.
- [x] Drill regression: rechecked new leader-transfer assertion and downstream follower-kill selection.
- [x] Verification: ran static syntax checks and `git diff --check`.

## Finding

### R2 - Major: new `10-grow-to-3` leader-transfer assertion assumes brk1 is still leader

Locations:
- `test/simcluster/drills/10-grow-to-3.sh:34` already resolves `LDR` because leadership may move after
  grow/SIGHUP.
- `test/simcluster/drills/10-grow-to-3.sh:57` nevertheless runs
  `tether cluster transfer-leader brk2 --wait` from `brk1`.
- `test/simcluster/drills/10-grow-to-3.sh:60` then hard-sets `LDR=brk2`.
- `cmd/tether/cluster.go:19`-`23` documents that online cluster verbs are leader-local; a non-leader tells
  the operator where to re-run and does not forward.

Why this fails:

The drill is now supposed to prove `simcluster status --json` works after leadership moves. That is a good
regression, but it must not assume the pre-move leader is brk1. Earlier in the same script, the comments and
code explicitly handle leadership moving during grow by resolving `LDR`. If grow leaves brk2 or brk3 as
leader, the new assertion executes `transfer-leader` on brk1, which can be a follower. Per the product
command contract, that fails instead of forwarding. A correct cluster can therefore fail the GREEN
acceptance drill before exercising the `status --json` fix.

Expected fix direction:

Use the resolved `LDR` as the admin host for `transfer-leader`, and choose a target different from `LDR`,
for example the first running voter in `{brk1,brk2,brk3}` not equal to `LDR`. Then assert
`simcluster status --json` reports that target as `leader_id` with `is_leader_view==true`, and update `LDR`
to that target for the follower-kill section.

## Closure Notes

- R1 is closed: `cmd_status --json` now queries the leader and rejects non-authoritative JSON.
- The previous F1/F2/F3 closure notes still stand.
- The remaining failure is in the new drill assertion, not the `status --json` implementation itself.

## Verification

Passing:

- `bash -n test/simcluster/simcluster`
- `sh -n test/simcluster/drills/10-grow-to-3.sh`
- `git diff --check`

Not run:

- Full Docker simcluster drills; this finding is a deterministic static control-flow issue.
- `shellcheck`, because it is not installed in this environment.
