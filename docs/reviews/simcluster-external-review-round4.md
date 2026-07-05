# Pass - simcluster external re-review round 4

Reviewer role: external re-reviewer. Scope: developer response plus unstaged fixes on top of
`docs/reviews/simcluster-external-review-round3.md`, specifically the dynamic leader-transfer fix in
`test/simcluster/drills/10-grow-to-3.sh` and the appended response text.

结论：Pass。上一轮 R2 已修复：`10-grow-to-3` 不再假设 brk1 是当前 leader，而是通过
`simcluster status --json` 解析当前 leader，在当前 leader 上执行 `transfer-leader` 到另一个 broker，
再用新的 leader 更新后续 follower-kill 选择。没有发现新的 Major/Blocker。

## Re-review Tasklist

- [x] Re-scope: inspected current unstaged R2 fix and appended developer response.
- [x] R2: rechecked dynamic current-leader resolution, transfer target selection, and post-transfer `LDR`.
- [x] Follower-kill continuity: rechecked that the selected killed broker is not the new leader.
- [x] Verification: ran static syntax checks and `git diff --check`.

## Closure Notes

- R2 is closed: the leader-transfer regression now runs on `CUR`, targets `TGT != CUR`, asserts
  `status --json` returns the authoritative `TGT` leader view, and updates `LDR=$TGT`.
- R1 remains closed: `simcluster status --json` queries the leader and rejects non-authoritative JSON.
- The earlier F1/F2/F3 closure notes still stand.

## Residual Non-blocking Risks

- Full Docker drills were not rerun by me; developer reports `10-grow-to-3` GREEN 19/19 on the dedicated
  server.
- Known deferred coverage remains out of scope for this increment: image freshness stamping,
  `reconcile nats --all`, and in-broker C3 auto-reconciler coverage after #22 is fixed.
- `shellcheck` is still unavailable in this environment.

## Verification

Passing:

- `bash -n test/simcluster/simcluster`
- `bash -n test/simcluster/remote.sh`
- `sh -n test/simcluster/drills/10-grow-to-3.sh`
- `git diff --check`
