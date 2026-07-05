# Pass - simcluster grow-honest external re-review round 2

Reviewer role: external reviewer. Scope: latest working-tree changes after the main process responded to
`simcluster-grow-honest-external-review.md`; the index was known stale from the prior `git add -A`, so this
round reviews the on-disk worktree first and expects a fresh re-stage afterward.

结论：Pass。前轮两个问题在工作区版本中均已闭环：F1 从“按 grow 序号写 trailer token”改为先执行真实
`tether cluster reconcile nats --all --wait --timeout 25s` 并基于观测失败生成 #3/#22 token；F2 的
`CLAUDE.md` whitespace 问题在工作区已消失，且被开发者追溯修复为恢复误删的 §7。未发现新的阻断级问题。

## Review Tasklist

- [x] Re-read the external Fail report and the main-process reply appended to it.
- [x] Re-scope staged-vs-unstaged drift and review the latest worktree, not the stale index.
- [x] Re-check `cmd_grow` ordering: real `reconcile nats --all` attempt before root manual render.
- [x] Re-check #3/#22 trailer generation and whether it is observation-gated.
- [x] Re-check `11-grow-gaps` for a real #3 signature, not just sim trailer labels.
- [x] Re-check `13-inbroker-reconcile-perm` for a real auto-path #22 assertion plus isolated write probe.
- [x] Re-run independent static/syntax checks.
- [x] Record doubts, residual risk, and recommendations.

## Closure Review

### F1 - Closed

`test/simcluster/simcluster:249-250` now runs the real one-command reconciliation path:
`tether cluster reconcile nats --all --wait --timeout 25s`, before `_reconcile_clustered` performs the
root/manual workaround. The result is captured and `_allfailed` is set only when the command output includes
non-convergence, harvest failure, or permission-denied evidence (`test/simcluster/simcluster:251-254`).

The trailer no longer blindly names #3/#22 by ordinal alone: #22 is appended only on later grows when the
observed `--all` attempt failed (`test/simcluster/simcluster:280`), and #3 is appended on first grow only
when that observed attempt failed (`test/simcluster/simcluster:304`). This fixes the specific false-green I
flagged: if the product auto path starts converging, the token drops and the drill flips.

The drills now pin the real product strings:

- `test/simcluster/drills/11-grow-gaps.sh:42` asserts both `reconcile nats: timed out.*not converged` and
  `no cluster.{0,4}block to harvest` for #3.
- `test/simcluster/drills/13-inbroker-reconcile-perm.sh:51-52` asserts the second-grow auto path records
  `reconcile nats: timed out.*not converged` plus `apply: natsconf: temp:.*permission denied` for #22.
- The same drill keeps the isolated User=tether write probe with `--skip-dry-run --allow-partial-mesh`
  (`test/simcluster/drills/13-inbroker-reconcile-perm.sh:67-69`), which is now secondary evidence rather
  than the only #22 proof.

Residual note: `_allfailed` is intentionally broad in `cmd_grow`; an unrelated later-grow non-convergence
could still label #22 in the transcript. The blocking concern is nevertheless resolved because drill 13
pins the actual #22 reason, so a false reason should fail the drill instead of passing silently.

### F2 - Closed

The working tree passes `git diff --check`; the prior `git diff --cached --check` failure is stale-index
state from the earlier staging of the bad `CLAUDE.md`. The developer restored the missing §7 in the
worktree, and this round will re-stage everything after the report.

## Doubts / Limits

- I did not independently rerun the Docker deploy-tier drills (`10-grow-to-3`, `11-grow-gaps`, `13-*`) in
  this environment. I reviewed the code paths and ran syntax/whitespace gates; the server-run claims remain
  internal evidence, not external evidence.
- The rework still intentionally leaves several grow gaps as labeled/trailer-guarded but not fully
  signature-pinned (#4/#5/#10/#23/#24). That is documented as deferred coverage, so I do not block this
  round on it.

## Suggestions

- Promote the remaining trailer-only gaps into dedicated signature-pinned drills when time permits,
  especially the cheap ones (#5, #10, #24).
- If future reviewers see a #22 trailer without drill 13's exact `apply: natsconf: temp: ... permission
  denied` evidence, treat it as a regression in evidence quality.

## Verification

Passing on the reviewed worktree:

- `bash -n test/simcluster/simcluster`
- `sh -n test/simcluster/drills/11-grow-gaps.sh`
- `sh -n test/simcluster/drills/13-inbroker-reconcile-perm.sh`
- `sh -n test/simcluster/image/provision-node.sh`
- `git diff --check`

The stale-index issue was then cleared by re-staging all files:

- `git add -A`
- `git diff --cached --check`
