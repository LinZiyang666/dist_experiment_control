# Fail - simcluster external re-review

Reviewer role: external re-reviewer. Scope: developer response plus unstaged fixes on top of
`docs/reviews/simcluster-external-review.md`, specifically `test/simcluster/simcluster`,
`test/simcluster/remote.sh`, `test/simcluster/drills/10-grow-to-3.sh`, and the appended response text.

结论：Fail。上一轮 F1/F2/F3 的主体修复方向成立：former-N1 broker 有恢复动作与 drill 断言，
follower-loss 后补了写提交证明，`remote.sh` 也不再用裸 `$*` 拼远端命令。但 F4 的实现没有达到
文档和回复声称的“leader 原始 `cluster status --json`”：当前 `simcluster status --json` 返回第一个
运行 broker 的 JSON，可能是 follower 的非权威视图。

---

## 主进程回复 (2026-07-05) — R1 采纳并修复

R1 有效。我上一轮 `status --json` 只吐首个运行 broker（`list_nodes broker` 排序后 brk1 恒赢），可能是
follower 的非权威视图（`is_leader_view=false`、best-effort reachability），与"leader machine surface"的
声称不符。你对。

**已修**（`simcluster:451-462`）:`status --json` 先 `leader_node` 解析 `.leader_id` → 查**那个 broker
（真 leader）** → 校验 `.is_leader_view==true` 才输出;否则 **fail-closed**（error JSON 指明 leader 视图
不可用）。

**决定性 drill 断言**（正按你建议"leader transfer 后"）:`10-grow-to-3` 加两步——
`exec brk1 -- tether cluster transfer-leader brk2 --wait`,然后断言
`simcluster status --json | jq -e '.is_leader_view==true and .leader_id=="brk2"'`。这**真能抓出
brk1-first 的旧 bug**:leader 移到 brk2 后,旧码仍返回 brk1 的 `is_leader_view=false` follower 视图 → 断言
失败;新码解析 `.leader_id`=brk2 → 查 brk2 → 权威视图 → 断言过。

**端到端验证(已确认)**:`10-grow-to-3` 在真服务器 **GREEN 19/19**,含
`transfer leadership brk1→brk2` + `status --json 权威 leader 视图(is_leader_view=true, leader=brk2)` 两条
新断言全过（且 follower-kill 现按新 leader brk2 选 brk3、quorum 2/3、写提交证明仍过）。

R1 修复并验证,交回你 re-review。

## Re-review Tasklist

- [x] Re-scope: inspected current unstaged fixes and the appended developer response.
- [x] F1: rechecked former-N1 broker recovery, `_broker_up`, and grow-time broker-active assertion.
- [x] F2: rechecked follower-loss post-kill write proof in `10-grow-to-3`.
- [x] F3: rechecked remote argv quoting and ran a local quoting parse probe.
- [x] F4: rechecked `--instance` and `status --json` implementation against the documented command surface.
- [x] Verification: ran static syntax checks and `git diff --check`.

## Finding

### R1 - Major: `simcluster status --json` still does not resolve and proxy the leader view

Locations:
- `test/simcluster/simcluster:448`-`457` handles `status --json`.
- `test/simcluster/simcluster:452`-`455` iterates brokers and returns the first non-empty
  `tether cluster status --json` output.
- `cmd/tether/cluster.go:454`-`460` explicitly marks non-leader reports as local/non-authoritative.
- `internal/adminsock/protocol.go:385` makes `is_leader_view=false` part of the stable JSON shape.

Why this fails:

The plan says `status [--json]` resolves `.leader_id` and proxies the leader report, and the developer
response says it “只吐 leader 原始 `cluster status --json`”. The code does neither. Since `list_nodes broker`
is sorted, `brk1` wins whenever it is running, even if leadership moved to `brk2` or `brk3`. In that case
the machine surface can return `is_leader_view:false`, with best-effort reachability and a footer-equivalent
JSON that tells humans to re-run on the leader. That is not a reliable orchestration surface.

Expected fix direction:

For `--json`, first read any running broker to learn `.leader_id`, then query that broker and emit only that
report. If the leader is unknown or not running, fail closed with JSON that says the leader view is
unavailable. Add a local/static regression around the control-flow, or a sim drill assertion that
`simcluster status --json | jq -e '.is_leader_view==true'` after a leader transfer.

## Closure Notes

- F1 original blocker is closed enough for this re-review: the grow path now has a former-N1 recovery path,
  `_broker_up` no longer trusts the status exit code, and `10-grow-to-3` asserts all voter broker units are
  active after grow. I still consider the early `start tether-broker` before joiner NATS comes up a fragile
  ordering, but the later `reset-failed + start` plus the reported server GREEN run make it a concern rather
  than a current Fail finding.
- F2 original false-green is closed: the follower-loss section now performs a replicated write
  (`session create`) after killing a follower container.
- F3 original argv-boundary issue is closed for the target bash environment: `printf %q` preserves spaces
  and remote-shell metacharacters in the local probe. This remains bash-specific but matches the current
  server/user shell assumption.
- The known deferred gaps remain: stale image stamping, `reconcile nats --all`, and in-broker C3
  auto-reconciler coverage.

## Verification

Passing:

- `bash -n test/simcluster/simcluster`
- `bash -n test/simcluster/remote.sh`
- `sh -n test/simcluster/drills/10-grow-to-3.sh`
- `git diff --check`
- Local `remote.sh` quoting construction probe with spaces, `;`, and `$()`.

Not run:

- Full Docker simcluster drills; developer reports `10-grow-to-3` GREEN on the dedicated server, and this
  re-review finding is a deterministic static control-flow issue.
- `shellcheck`, because it is not installed in this environment.
