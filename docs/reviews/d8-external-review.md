# Fail — D8 external review

Reviewer role: external reviewer. Scope: all unstaged / untracked D8 changes, including
`docs/distributed-broker-architecture.md`, `docs/reviews/d8-{plan,review}.md`,
`internal/broker` transfer/alert additions, `internal/cluster` alert ops, `cmd/tether`
alert/gate/banner wiring, ACL/proto changes, and `test/d8`.

结论：Fail。D8 的核心方向正确：`OpTransferAudit` 可重导、alert store 经 Raft
复制、health probe 使用 VerifyLeader-confirmed 信号、ACL carve-out 和 production guard
都有落点。但还有三个上线级问题没有闭合：transfer continuation/terminal 路径在
tracker miss 时仍会抢答错误，严重告警 destructive gate 只接到了 `session rm`，以及
`alert ack` 会把 broker 的错误回复当成功退出。另有一个 explicit `alert ls` 误报空告警的
可用性问题。当前实现不能外审通过。

## Tasklist

- [x] Scope census: enumerated tracked and untracked D8 changes outside staging.
- [x] Process/docs alignment: read `CLAUDE.md`, architecture §9/§10/§19-D8, D8 plan/internal review, and prior external-review style.
- [x] Transfer routing review: checked broadcast-SUB home gate, continuation tracker routing, finalize, tier-B commit, audit forwarding, and orphan reaper boundaries.
- [x] Alert path review: checked alert ops/read model, broker responders, ack forwarding, health gate, banner, and ACL carve-out.
- [x] CLI destructive gate review: checked `gateDestructive` call sites and `--ack-alerts` coverage for destructive commands.
- [x] Test rigor audit: added independent reviewer regressions for tracker-miss silence, ack error semantics, and gate coverage.
- [x] Verification: ran the focused reviewer regressions and captured deterministic failures.
- [x] Report: this report written as `docs/reviews/d8-external-review.md`.

## Findings

### F1 — High: transfer continuation/finalize paths reply `transfer_unknown` on tracker miss instead of staying silent

Locations:
- `docs/distributed-broker-architecture.md:318` — continuation/terminal transfer subjects are broadcast-SUB and must route by tracker ownership; non-owners stay silent.
- `docs/reviews/d8-plan.md:31` and `:43` — every broker receives these subjects; `entry==nil` for `finalize.req` is explicitly silent.
- `internal/broker/transfer.go:648`-`651` — `push-commit.req` tracker miss replies `transfer_unknown`.
- `internal/broker/transfer.go:934`-`939` — `finalize.req` tracker miss replies `transfer_unknown`.
- Reviewer repros: `internal/broker/d8_external_review_test.go`.

Why this fails:

D8 relies on plain NATS subscriptions. In a routed broker cluster, every broker sees
`push-commit.req` and `finalize.req`; only the broker that created the transfer tracker
entry is allowed to answer. A non-home / non-origin broker with an empty tracker currently
replies first with `transfer_unknown`, so ctl can consume that error before the true home
broker's OK. That can falsely fail tier-B push commit and pull finalize exactly in the
fan-out case D8 is supposed to fix.

Expected fix direction:

For continuation/terminal paths, treat tracker absence as "not mine": return without
responding. Keep real validation errors for the broker that owns the tracker entry. Add the
reviewer tests as permanent regressions.

### F2 — High: severe destructive gate is only wired to `session rm`; push/pull, expose, expose-rm, and run bypass it

Locations:
- `docs/reviews/d8-plan.md:13` — D8b gate must wrap D8a push/pull.
- `docs/reviews/d8-plan.md:185`-`186` — planned `cmd/tether/{expose,run,session,node,transfer,proxy,cluster}.go` gate wiring.
- `docs/distributed-broker-architecture.md:491` — destructive command list includes at least expose/run/session rm plus push/pull/kill/expose-rm.
- `cmd/tether/session.go:165`-`168` — the only real `gateDestructive` call site.
- `cmd/tether/transfer.go:123`-`170` and `:382`-`433` — push/pull publish mutating requests without gate.
- `cmd/tether/expose.go:45`-`87` and `:138`-`166` — expose/expose-rm publish mutating requests without gate.
- `cmd/tether/run.go:67`-`119` — run publishes process creation without gate; kill is reached from the run flow.
- Reviewer repro: `cmd/tether/d8_external_review_test.go`.

Why this fails:

The exit criterion says severe alerts correctly gate destructive operations without
mis-gating pure client jitter. The implementation only protects `session rm`. During
`quorum_lost` or `force_single_active`, operators can still start remote processes, open
public tunnels, remove tunnels, and push/pull files without the `--ack-alerts` override
that D8 documents. This is not merely incomplete UX; it is the D8b/D8a cross-dependency
called out in the plan.

Expected fix direction:

Add `--ack-alerts` and a `gateDestructive(nc, actor, ackAlerts)` preflight to every
destructive ctl path after authenticated NATS connection and before the mutating publish.
Keep the no-responder N=1 inert behavior.

### F3 — Major: `tether alert ack` exits success when the broker replied `error: ...`

Locations:
- `internal/broker/cluster_health.go:78`-`102` — ack responder returns plain `"ok"` or `"error: ..."` strings.
- `cmd/tether/d8_alerts.go:148`-`157` — `ackAlert` returns any response body with nil error.
- `cmd/tether/alert.go:70`-`76` — CLI prints the response and exits nil.
- Reviewer repro: `cmd/tether/d8_external_review_test.go`.

Why this fails:

If the broker cannot forward the ack to the leader, the command can print
`error: no leader`, print the follow-up note, and exit 0. Operators and scripts will treat
the alert as acknowledged even though no Raft write happened.

Expected fix direction:

Make `ackAlert` accept only exact `"ok"` as success. Return an error for `"error: ..."` and
malformed/empty replies. A small structured JSON response would be cleaner, but the current
plain-text protocol can still be made fail-closed.

### F4 — Medium: explicit `alert ls` silently reports "no active alerts" on query failure

Locations:
- `cmd/tether/d8_alerts.go:89`-`100` — `fetchAlerts` returns nil on timeout, no responder, or malformed response.
- `cmd/tether/alert.go:43` — explicit `alert ls` passes that nil slice to the printer.
- `cmd/tether/d8_alerts.go:133`-`135` — nil/empty renders as `no active alerts`.

Why this fails:

Best-effort nil is correct for the always-on banner, where alert lookup must not break a
read command. It is not correct for the explicit operator command. During no leader,
responder skew, or a malformed response, `tether alert ls` currently gives a false green
instead of saying the alert store could not be queried.

Expected fix direction:

Split banner fetch from strict CLI fetch. `withBanner` can keep swallowing lookup errors;
`alert ls` should return an error on no responder, timeout, and parse failure.

## Questions / concerns

- Are `tether alert ls/ack` intended to be user-visible before D9 responder wiring? If yes,
  explicit commands need strict errors. If no, hide or fail-fast them until the responders are live.
- The plan mentions gate wiring for `node`, `proxy`, and `cluster` command files as well as
  transfer/expose/run/session. Please reconcile the final destructive command table so tests do
  not guess which verbs are gate-required.
- The client-synth gate remains advisory by design. Which server-side mutating paths already
  refuse writes when quorum cannot be served, and which are still D9-only?

## Confirmed clean areas

- `OpTransferAudit` keeps a content-derived reqID and the node proposal path preserves it instead
  of overwriting with an empty reqID.
- Alert raise/clear/ack ops use deterministic SQL against committed state, with active alert reads
  separated from the write path.
- `AlertReconciler` is leader-gated and avoids idle writes for the inspected replicated alerts.
- The activated-member ACL carve-out grants actor-scoped health/alert subjects while keeping
  broker-only `cluster.apply.*` denied.
- The D8 production guard continues to enforce the build-and-prove boundary for cluster responders.

## Verification

Passing:

- `git diff --check`

Failing reviewer regressions:

- `go test ./internal/broker -run 'TestD8ReviewPushCommitTrackerMissIsSilent|TestD8ReviewFinalizeTrackerMissIsSilent' -count=1 -v`
  - `push-commit` tracker miss replied `{"ok":false,"code":"transfer_unknown",...}`.
  - `finalize` tracker miss replied `{"ok":false,"code":"transfer_unknown"}`.
- `go test ./cmd/tether -run 'TestD8Review' -count=1 -v`
  - `TestD8ReviewAckAlertErrorReplyIsCommandError` saw nil error for `"error: no leader"`.
  - `TestD8ReviewDestructiveGateCoverage` found no `gateDestructive` call in `transfer.go`, `expose.go`, or `run.go`.

Commands that start embedded NATS were run outside the restricted sandbox because the sandboxed
environment blocks those process/socket paths.

Not run:

- Full `make test`, `make e2e`, and `make lint`, because the deterministic reviewer regressions
  already make this external review a Fail.

---

## 主进程回复（外审 round-1 处置 · 2026-06）

结论：**4 条 finding 全采纳并修复，4 个 reviewer 回归全部转绿**；逐条如下。

### F1（High，transfer continuation/finalize tracker-miss 抢答）— 已修
确属真 bug：我 §9 prose 说 continuation/terminal 按 tracker-presence 路由、miss 静默，但**从未改 `handlePushCommitReq`/`handleFinalizeReq` 的既有 miss 回复**（它们为单 broker 写、miss 回 `transfer_unknown` 是对的）。集群 broadcast-SUB 下非 owner 会抢答错误码。**修**：两处 tracker-miss 改为——`selfID != ""`（clustered）→ **静默返回不应答**（非 owner，让真 home 答）；`selfID == ""`（生产单 broker=owner）→ 保留 `transfer_unknown` 回复（字节等价）。`internal/broker/transfer.go` push-commit miss（`entry==nil`）+ finalize miss（`preview==nil`）。reviewer 回归 `TestD8ReviewPushCommitTrackerMissIsSilent`/`TestD8ReviewFinalizeTrackerMissIsSilent` 转绿。

### F2（High，severe gate 只接 session rm）— 已修
确属真缺口（D8b/D8a 跨依赖 + EXIT-B）。**修**：`gateDestructive(nc, actor, ackAlerts)` 预检 + `--ack-alerts` flag 接入全部 NATS-侧破坏性命令——`push`/`pull`（transfer.go）、`expose`/`expose rm`（expose.go）、`run`（run.go，kill 是 run 流的 Ctrl-C 清理、随 run 门控）、`session rm`（已有）。插在 authed NATS 连接后、mutating publish 前；保 N=1 no-responder inert（probe 503 快path）。reviewer 回归 `TestD8ReviewDestructiveGateCoverage`（4 子测试）转绿。**最终 gated 集**见下 Q2 表。

### F3（Major，alert ack 错误回复仍 exit 0）— 已修
**修**：`ackAlert` 改 **fail-closed**——仅 reply 恰为 `"ok"` 算成功；`"error: …"`/空/malformed/transport 失败均返非 nil error。`alert ack` 命令既有 `if err != nil { return }` 即在错误时退非零、且不打印"将重现"误导 note。reviewer 回归 `TestD8ReviewAckAlertErrorReplyIsCommandError` 转绿。

### F4（Medium，alert ls 误报空告警）— 已修
**修**：拆 `fetchAlertsStrict`（no-responder/timeout/malformed → 真 error，供**显式** `alert ls`）与 `fetchAlerts`（best-effort nil，供 always-on banner）。`alert ls` 命令改用 strict 版、失败返 error（不再误报 `no active alerts`）。

### Questions / concerns 回应
- **Q1（alert ls/ack 是否 D9 前对用户可见）**：是，member-reachable RPC，现已 strict（F3/F4）。N=1 生产**无** alert responder（build-and-prove，responder 是 harness-wired、cutover=D9），故 `alert ls`/`ack` 在 N=1 **诚实 fail-fast**（"no broker answered the alert store" / ack 非 ok 返错）——非 false-green，符合 F4 修正方向。alert store 在 D9 cutover 后才有 live 数据。
- **Q2（destructive 命令表对账）**：最终 **NATS-侧 gated 集** = `session rm`、`push`、`pull`、`expose`、`expose rm`、`run`（+ `kill` 随 run 流）。**不 gate**：`kick`/`rotate-pin`（session 控制写，gate 接缝同形、可增量接，未在本轮 reviewer 命名集）、proxy/node 子命令、`cluster *` 管理命令（走本地 adminsock 非 NATS、有自己的 D7 typed-confirm 门）。已在 §16 D8(h) 与 plan §6 登记此表，测试按此判定（不再猜）。
- **Q3（server 侧写拒绝边界）**：client-synth gate 是 **advisory 预检**；权威保护是 **D9 cutover 后** FSM-routed 写经 `cluster.Node.Propose`——quorum 丢失时 `Propose` 返 `raft.ErrLeadershipLost`/not-leader、broker loud-fail 该写（§4.1 fail-closed）。**N=1 生产无 quorum 概念**（单节点），故当前无 server 侧 quorum 拒绝路径——这正是 build-and-prove 的边界（cutover=D9）；client-synth gate 在 N=1 亦 inert（no-responder）。

### 门（修复后全绿）
`go build ./...` 0 · `make test` exit 0 · `make lint` 0 · gated `TestD8Matrix -race` · `make e2e` exit 0 · 4 个 reviewer 回归（`TestD8Review*`）全绿。**待外审 re-review。**

---

# Pass — D8 external re-review

Reviewer role: external re-reviewer. Scope: the post-Fail fixes for F1-F4 plus the
developer response above. I re-read the patched transfer, alert, gate, docs, and reviewer
tests instead of trusting the internal/developer summary.

结论：Pass。上轮 4 个 finding 均已按合同修复，且未在复审面内发现新的 blocker。
我额外补了一个 reviewer regression，锁住 F4 的 strict `alert ls` 行为。

## Tasklist

- [x] Re-scope: inspected unstaged post-response diffs and the appended developer response.
- [x] F1: rechecked `push-commit` and `finalize` tracker-miss behavior in clustered vs production mode.
- [x] F2: rechecked `gateDestructive` + `--ack-alerts` wiring for `session rm`, `push`, `pull`, `expose`, `expose rm`, and `run`.
- [x] F3: rechecked `alert ack` fail-closed semantics.
- [x] F4: rechecked strict `alert ls` vs best-effort banner split and added reviewer coverage.
- [x] Verification: ran focused reviewer tests, D8 package tests, D8 guard tests, gated D8 matrix under `-race`, `go test ./...`, and `git diff --check`.

## Finding Closure

- F1 closed: in clustered mode (`selfID!=""`) tracker miss now returns without replying for both
  `push-commit` and `finalize`; production single-broker mode preserves `transfer_unknown`.
- F2 closed: the named NATS-side destructive commands all have `--ack-alerts` and call
  `gateDestructive` after authenticated NATS connect and before mutating publish.
- F3 closed: `ackAlert` treats only exact `"ok"` as success and returns errors for broker
  `"error: ..."` replies, empty replies, malformed replies, and transport failures.
- F4 closed: explicit `alert ls` now uses `fetchAlertsStrict`; the banner keeps the lenient
  best-effort fetch. Added `TestD8ReviewAlertLsStrictFetchErrorsOnQueryFailure`.

## Residual Notes

- The final destructive-gate table intentionally excludes `kick` and `rotate-pin`; the docs now
  say these are incrementally gateable but out of this D8 gated set. I accept this for D8 because
  the external finding named the documented high-risk transfer/expose/run/session set and the
  final contract is now explicit.
- `alert ack` error messages are slightly double-prefixed through `alert.go` wrapping
  `ackAlert` errors, but this is cosmetic and does not affect fail-closed behavior.
- I did not re-run `make e2e` or `make lint`; I did run the D8 `-race` matrix and the full
  non-integration `go test ./...` suite.

## Verification

Passing:

- `go test ./internal/broker -run 'TestD8ReviewPushCommitTrackerMissIsSilent|TestD8ReviewFinalizeTrackerMissIsSilent' -count=1 -v`
- `go test ./cmd/tether -run 'TestD8Review|TestRenderBannerSevereOnly' -count=1 -v`
- `go test ./internal/cluster ./internal/proto ./internal/auth -run 'TestAlert|TestEvalDestructiveGate|TestD8bMemberAlertACLCarveOut' -count=1 -v`
- `go test ./internal/broker -run 'TestD8(PlanAlertSignal|AlertReconcile|Publisher|TransferAudit|TransferHome|HomeOwns|XferTarget)' -count=1 -v`
- `go test ./cmd/tether -run 'TestD8|TestRenderBannerSevereOnly' -count=1`
- `go test ./internal/broker -run 'TestD8' -count=1`
- `go test ./test/d8 -run 'TestD8' -count=1 -v`
- `go test ./internal/xferaudit ./internal/proto ./internal/cluster ./internal/auth -run 'TestD8|TestAlert|TestEvalDestructiveGate|TestActiveAlerts|TestIsAlertActive' -count=1`
- `go test -race -count=1 -tags d8_integration -timeout 300s ./test/d8 -run '^TestD8Matrix$' -v`
- `go test ./...`
- `git diff --check`
