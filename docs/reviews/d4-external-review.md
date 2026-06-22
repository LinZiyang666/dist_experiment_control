# Pass — D4 external review re-review round 2

Reviewer role: external reviewer. Scope: all unstaged / untracked D4 changes, including
`docs/distributed-broker-architecture.md`, `docs/reviews/d4-{plan,review}.md`,
`internal/{broker,cluster,proc,storage}`, `test/d4`, `test/cluster`, and the D4 e2e
matrix wiring.

结论：Pass。主进程 round-2 已把上轮 RF1 从 seam 软约定提升为 `cluster.apply`
wire-boundary 硬不变量：`dispatchForward` 对 `VerbProvision` / `VerbJoin` 的非空
`env.ReqID` 返回永久错误 `ErrReqIDNotAllowed`，并对这两个动词改走 `node.Propose`
（无 key）；只有 `reconcile` 继续走 `ProposeWithReqID`。上轮失败的
`TestD4ProvisionNonEmptyReqIDMustNotFalseSuccessAfterEvict_Review` 已转绿。RF2 的
D4 plan stale text 也已修正：provision/join key 推导被标记为 SUPERSEDED，§2.6 改为
空 key seam + wire-boundary reject。复审未发现新的阻塞问题。

## Round-2 Re-review Tasklist

- [x] Read the main-process round-2 reply and identify the new unstaged changes.
- [x] Review `cluster_forward.go` RF1 boundary fix and ensure provision/join no longer stamp a ReqID.
- [x] Review `forward_test.go` changes after provision/join non-empty keys became prohibited.
- [x] Re-scan D4 plan and architecture text for live stale provision/join key contract.
- [x] Run the three external-review regressions plus the updated dedup/ack-drop tests.
- [x] Run focused D4/non-D4 verification and `git diff --check`.
- [x] Update this report with a current Pass/Fail conclusion.

## Round-2 Findings

No blocking findings.

Notes:
- `ErrReqIDNotAllowed` is correctly permanent: it maps through `status=error` /
  `ForwardBusinessError`, not `cluster.ErrForwardNotLeader`, so a caller does not retry a
  prohibited provision/join key as a transient leadership failure.
- The dedup primitive is no longer tested by forwarding a prohibited provision key through
  the real responder; the cross-leader ledger test moved to direct `Node.ProposeWithReqID`,
  and the ack-drop test still exercises `Forwarder` timeout classification with a custom
  responder. That is a reasonable split after RF1 because production provision/join cannot
  be reqID-bearing forwarded ops.
- Historical review text in `docs/reviews/d4-review.md` still records earlier proposed
  `ProvisionReqID` / `JoinReqID` tests as part of the original internal-review record; the
  current plan and this external report supersede that history.

## Round-2 Verification

Passing:

- `GOCACHE=/tmp/tether-gocache go test ./test/d4 -run 'TestD4(ProvisionReqIDMustNotDedupAcrossEvict_Review|ForwardInvalidReqIDMustNotReturnOK_Review|ProvisionNonEmptyReqIDMustNotFalseSuccessAfterEvict_Review|DedupReplicatedLedgerAcrossLeadershipTransfer|ForwardAckDropCommittedRetryDedups)' -count=1 -v`
- `GOCACHE=/tmp/tether-gocache go test ./test/d4 -count=1`
- `GOCACHE=/tmp/tether-gocache go test ./internal/cluster -run 'TestD4|TestFSM_IdempotentReapply|TestFSM_PoisonEntry' -count=1`
- `GOCACHE=/tmp/tether-gocache go test ./internal/broker -run 'TestD4Reconcile|TestResolveReconcile' -count=1`
- `GOCACHE=/tmp/tether-gocache go test ./internal/proc -run TestD4 -count=1`
- `git diff --check`

Full `make test` / `make e2e -race` / `make lint` were not rerun by the reviewer in this
turn; main process reports them green.

## Tasklist

- [x] Scope census: confirmed no staged baseline and reviewed all tracked/untracked D4 files.
- [x] Requirements / process alignment: checked CLAUDE, distributed-broker requirements/architecture, D4 plan, prior D4 internal review/adjudication, and prior external-review report style.
- [x] FSM / ReqID audit: reviewed `commandVersion` / ReqID validation, `appliedDedup`, same-txn ledger insert/dedup/GC, migration 0011, and cursor advancement semantics.
- [x] Forwarding audit: reviewed `cluster.apply.*` broadcast routing, typed `ok/not_leader/error` taxonomy, provision/join seams, bad-PIN error identity, timeout direction, ACL surface, and production non-wiring guard.
- [x] Reconcile audit: reviewed live-path zero-regression, pure resolver, Aux audit tuples, replay ordering, monotonic time handling, and live-vs-op audit differential.
- [x] Test rigor audit: checked named §13.7 gates, vacuity controls, leak/fd gate, D4 e2e matrix wiring, and deferred infra items.
- [x] Per-file second pass: scanned every staged file individually after the initial report.
- [x] Independent reviewer tests: added regressions for stale provision ReqID after node evict and invalid-ReqID poison-ok.
- [x] Verification: ran focused non-socket suites and the reviewer regression outside the sandbox because embedded NATS requires local listen sockets.
- [x] Report: this report written as `docs/reviews/d4-external-review.md`.

## Round-1 Re-review Tasklist

- [x] Read the main-process reply appended to this report and identify every changed file.
- [x] Re-audit the F1/F2 production code paths in `cluster_forward.go` and `node.go`.
- [x] Scan the D4 tests after the main-process edits and add a boundary regression for non-empty provision ReqID.
- [x] Re-run the two original reviewer regressions plus the new boundary regression in the real socket-capable environment.
- [x] Re-scan D4 docs for stale provision/join ReqID contract text.
- [x] Update this report with the current Pass/Fail conclusion.

## Round-1 Re-review Findings (resolved in round 2)

### RF1 — High: provision/join "no ReqID" is only honored by the seam, not enforced at the `cluster.apply` boundary — resolved

Locations:
- `internal/broker/cluster_forward.go:315` — `VerbProvision` still calls `node.ProposeWithReqID(env.ReqID, ...)`.
- `internal/broker/cluster_forward.go:323` — `VerbJoin` has the same shape.
- `internal/broker/cluster_forward.go:396` / `:426` — the production authcallout seams now send the empty ReqID, but this is only one caller.
- `test/d4/review_fixes_test.go:171` — reviewer boundary regression added.

Why this still fails:

The accepted F1 fix says provision/join must not carry a forwarding ReqID because their
effects are operator-deletable. The code now satisfies that for `NewProvisionSeam` and
`NewJoinSeam`, but the wire responder still accepts a legal non-empty ReqID on those verbs
and stamps it into the raft command. That keeps the old bad state reachable from any
broker-nkey publisher on `cluster.apply.provision`, any future use of the exported generic
`Forwarder.Forward`, or a future seam regression. Once such a ledger row exists, node-evict
can delete the binding and the same non-empty ReqID retry returns `ok` while
`agent_provisioning` stays absent.

Reviewer repro added:

- `test/d4/review_fixes_test.go:183`
- `GOCACHE=/tmp/tether-gocache go test ./test/d4 -run 'TestD4(ProvisionReqIDMustNotDedupAcrossEvict_Review|ForwardInvalidReqIDMustNotReturnOK_Review|ProvisionNonEmptyReqIDMustNotFalseSuccessAfterEvict_Review)' -count=1 -v`

Observed:

```text
--- PASS: TestD4ForwardInvalidReqIDMustNotReturnOK_Review
--- PASS: TestD4ProvisionReqIDMustNotDedupAcrossEvict_Review
--- FAIL: TestD4ProvisionNonEmptyReqIDMustNotFalseSuccessAfterEvict_Review
    review_fixes_test.go:234: non-empty provision ReqID returned success after evict but did not recreate the row on node 0; got 0
```

Expected fix direction:

Close the contract at the boundary, not only at the current seam. Acceptable fixes are:
reject non-empty ReqID for `provision` / `join` with a permanent typed error before raft
proposal, or ignore `env.ReqID` for those verbs and always call `node.Propose`. If the
generic dedup primitive still needs forward-wire coverage, exercise it with a verb whose
effect is not deletable under the same content key, or with a test-only responder that does
not bless the prohibited provision/join shape.

### RF2 — Low: D4 plan still documents the old provision/join ReqID design — resolved

Locations:
- `docs/reviews/d4-plan.md:19`-`:23` — R-3 still defines PIN-provision and PIN-join content-addressed keys.
- `docs/reviews/d4-plan.md:55` / `:126` — still says residual not_leader / scope is protected by content-addressed ReqID broadly, without excluding provision/join.
- `docs/reviews/d4-plan.md:201` — §2.6 still says the broker-injected provision/join seam mints `reqID` and calls `ProposeWithReqID` / `Forward(..., reqID, ...)`.

This is not the primary product failure, but it is dangerous doc drift in the plan-of-record:
the architecture doc has the corrected F1 scope, while the D4 plan still instructs a future
reader to reintroduce the rejected provision/join key.

## Round-1 Confirmed Fixed

- Original F2 is fixed for the tested path: invalid non-empty ReqID is rejected before raft proposal and no row is written.
- Original F1 is fixed for the current production seam path: provision/join are sent with empty ReqID and the evict→re-provision lifecycle regression now passes.
- Queue-group text in the main D4 routing sections and `ErrForwardNotLeader` comment was cleaned up.

## Round-1 Verification

Passing in the focused re-review run:

- `TestD4ForwardInvalidReqIDMustNotReturnOK_Review`
- `TestD4ProvisionReqIDMustNotDedupAcrossEvict_Review`

Failing for RF1:

- `TestD4ProvisionNonEmptyReqIDMustNotFalseSuccessAfterEvict_Review`

Command run outside the default socket-restricted sandbox:

```text
GOCACHE=/tmp/tether-gocache go test ./test/d4 -run 'TestD4(ProvisionReqIDMustNotDedupAcrossEvict_Review|ForwardInvalidReqIDMustNotReturnOK_Review|ProvisionNonEmptyReqIDMustNotFalseSuccessAfterEvict_Review)' -count=1 -v
```

## Initial Findings (round 1, before main-process response)

The section below is preserved as the first external-review record. Main has fixed original
F2 and the production-seam part of original F1; RF1 above is the remaining blocking
success-semantics issue.

### F1 — High: provision ReqID dedups across node-evict lifecycle and can return success without recreating `agent_provisioning`

Locations:
- `internal/broker/cluster_forward.go:150` — `ProvisionReqID` is derived only from `(sid,nid,fp)`.
- `internal/cluster/fsm.go:148` — any later command with the same ReqID takes the ledger dedup path, skips op SQL, advances `applied_index`, and returns success.
- `internal/node/plan.go:55` — `PlanEvict` deletes `agent_provisioning` for `(sid,nid)`.
- `internal/authcallout/handler.go:284` — a nil error from `ProvisionAgentWrite` emits `member_joined` and allows the connection; it does not re-check that the binding row now exists.

Why this fails:

D4 treats the same `(sid,nid,fp)` as the same logical provision forever within the
retention window. That is only true for an immediate retry after a lost ack. It is false
after an operator evicts the node. Evict removes the authoritative `(sid,nid)` binding,
and the same agent identity may legitimately reconnect with the PIN to recreate it.
The current ledger row from the first provision remains, so the re-provision is
dedup-skipped. The forwarder returns `ok`, but the row stays absent.

Impact:

This is not just a test-vacuity issue. At D9 cutover, authcallout would mint an agent JWT
for a node that is not present in `agent_provisioning`. The immediate connection is
false-allowed, and the next reconnect without PIN will likely fail because the durable
state was never restored. The same class can affect any content-addressed PIN write whose
effect can later be deleted while the ReqID remains in the ledger.

Reviewer repro added:

- `test/d4/review_fixes_test.go:125`
- `go test ./test/d4 -run TestD4ProvisionReqIDMustNotDedupAcrossEvict_Review -count=1 -v`

Observed failure outside the socket-restricted sandbox:

```text
review_fixes_test.go:163: re-provision after evict must recreate the row on node 0; got 0
```

Expected fix direction:

The dedup key or dedup decision must be scoped to a lifecycle that changes when the
effect is invalidated. Acceptable fixes include a durable per-request/client attempt
epoch, op-specific validation on dedup hit that verifies the intended effect still
holds before skipping, or ledger invalidation tied to replicated delete ops. A pure
`sid/nid/fp` key is too coarse for provision because the binding is explicitly deletable.

### F2 — High: invalid forwarded ReqID is committed as poison but still returned as `ok`

Locations:
- `internal/broker/cluster_forward.go:306` / `:313` / `:321` / `:329` — `dispatchForward` forwards `env.ReqID` into `ProposeWithReqID` without validating it.
- `internal/cluster/command.go:149` — `decodeCommand` rejects malformed ReqID.
- `internal/cluster/fsm.go:83` and `:220` — decode failure becomes `appliedPoison`, advances `applied_index`, and runs no op SQL.
- `internal/cluster/node.go:274` — `Node.Apply` treats non-error FSM responses, including `appliedPoison`, as nil success.
- `internal/broker/cluster_forward.go:349` — nil dispatch error becomes reply status `ok`.

Why this fails:

The D4 contract says malformed ReqID must fail closed, because a malformed key cannot be
trusted to round-trip identically across raft JSON and the ledger. The current forward
path does reject it inside the FSM, but too late for the caller: the poison entry is
durably skipped and then reported as success. A generic `Forwarder.Forward` caller, or
any future seam bug that passes a bad key, can observe `ok` for a write that did not
execute.

Reviewer repro added:

- `test/d4/review_fixes_test.go:99`
- `go test ./test/d4 -run TestD4ForwardInvalidReqIDMustNotReturnOK_Review -count=1 -v`

Observed failure outside the socket-restricted sandbox:

```text
ERROR cluster: poison entry, advancing applied_index past it as a no-op ... err="cluster: invalid req_id ..."
review_fixes_test.go:110: invalid ReqID must not return success; poison-skipping the op and replying ok is a false success
```

Expected fix direction:

Validate forwarded ReqIDs before proposing them, preferably at the broker responder
boundary and/or in `ProposeWithReqID`, and return a non-ok typed error for invalid keys.
Do not let an invalid forwarded key enter raft as a poison command that the requester sees
as committed.

## Per-file Review Notes (round 1 baseline)

- `docs/distributed-broker-architecture.md` — D4 contract now correctly states broadcast
  `Subscribe` + leader-only reply and build-and-prove/no production cutover. No blocker.
- `docs/reviews/d4-plan.md` — reviewed against implementation; it still contains stale
  queue-group / `QueueSubscribe` wording in older plan sections (`R-8`, `R-11`, §1.2,
  §2.4) even though §0bis-H corrects it. Non-blocking doc drift, but should be cleaned
  before this document becomes an implementation reference.
- `docs/reviews/d4-review.md` — prior adjudication is reflected in code/tests; no new
  contradiction beyond the stale plan text above.
- `docs/reviews/d4-external-review.md` — this report, updated after the second pass.
- `internal/broker/broker.go` — only the test audit tap was added; production path remains
  nil-checked and inert.
- `internal/broker/cluster_forward.go` — broadcast routing and typed business errors are
  sound; F1 and F2 are both rooted here. Minor text drift: `ProvisionReqID` comment says
  `"prov"` while the implementation hashes `VerbProvision == "provision"`.
- `internal/broker/reconcile.go` — pure classifier mirrors live reconcile without moving
  live side effects; no new issue found.
- `internal/broker/reconcile_marks_test.go` — live-vs-op DB and audit multiset checks are
  non-vacuous for PID reuse, orphan kill, and port reconcile.
- `internal/broker/testhooks.go` — unexported global test seam; safe under current
  non-parallel tests.
- `internal/cluster/command.go` — ReqID charset guard is correct; participates in F2
  because poison is surfaced as success to `Forward`.
- `internal/cluster/crash_invariant_test.go` — D4 FSM dedup semantics and poison coverage
  are meaningful; no new issue.
- `internal/cluster/fsm.go` — transaction shape is correct; F1/F2 both depend on the fact
  that dedup/poison skip op SQL while still advancing the cursor.
- `internal/cluster/node.go` — `ProposeWithReqID` preserves the leader gate; participates
  in F2 because `appliedPoison` is not converted to a proposer-visible error. Comment on
  `ErrForwardNotLeader` still says "re-request the queue group".
- `internal/cluster/reqid_dedup_test.go` — atomicity, GC, dedup-hit rollback, and decode
  poison tests are non-vacuous.
- `internal/proc/plan.go` — ReconcileBatch Aux replay is apply-inert and deterministic.
- `internal/proc/reconcile_replay_test.go` — round-trip, ordering, no-rc orphan, and
  monotonic-time checks are adequate.
- `internal/storage/cluster_migrations_test.go` — migration ordering includes 0011; runtime
  FSM tests exercise the actual table.
- `internal/storage/migrations/0011_cluster_reqid_ledger.sql` — simple deterministic table
  + index, no timestamp/default concern.
- `test/cluster/d2_command_shape_review_test.go` — updated ReconcileBatch call shape only.
- `test/cluster/equiv_test.go` — existing equivalence tests still cover D2 command shape
  after the ReconcileBatch input refactor.
- `test/d4/setup_test.go` — combined routed-NATS + mTLS raft harness and fd/goroutine leak
  gates are in place; socket-restricted sandbox cannot run this suite reliably.
- `test/d4/forward_test.go` — happy path, no-leader fail-closed, idempotent retry,
  leadership transfer, reconcile forward, and leak checks are useful but missed F1/F2.
- `test/d4/regression_test.go` — production no-cutover guard is correctly scoped to
  production wiring files, not `cluster_forward.go`.
- `test/d4/review_fixes_test.go` — prior review fixes plus the two external reviewer
  regressions; both external reviewer regressions fail on current code.
- `test/e2e/all_phases_test.go` — D4 matrix is wired under `-race` with the relevant
  internal packages and `test/d4`.

## Reviewer Additions (round 1)

Added two external-review regressions in `test/d4/review_fixes_test.go`.

`TestD4ProvisionReqIDMustNotDedupAcrossEvict_Review` uses the existing real routed-NATS
+ mTLS raft D4 harness:

1. Forward provision commits and replicates.
2. `NodeEvict` commits through raft and deletes the binding.
3. The same agent identity re-provisions with the same PIN.
4. Current code returns success but leaves `agent_provisioning` empty, proving the stale ledger false-dedup.

`TestD4ForwardInvalidReqIDMustNotReturnOK_Review` forwards a provision payload with an
invalid ReqID and asserts the caller must not receive success. Current code returns `ok`
after committing a poison no-op.

## Initial Verification (round 1)

Passing:

- `GOCACHE=/tmp/tether-gocache go test ./internal/cluster -run 'TestD4|TestFSM_IdempotentReapply|TestFSM_PoisonEntry' -count=1`
- `GOCACHE=/tmp/tether-gocache go test ./internal/proc -run TestD4 -count=1`
- `GOCACHE=/tmp/tether-gocache go test ./internal/broker -run 'TestD4Reconcile|TestResolveReconcile' -count=1`
- `git diff --check`

Failing for the product issue:

- `GOCACHE=/tmp/tether-gocache go test ./test/d4 -run TestD4ProvisionReqIDMustNotDedupAcrossEvict_Review -count=1 -v`
- `GOCACHE=/tmp/tether-gocache go test ./test/d4 -run TestD4ForwardInvalidReqIDMustNotReturnOK_Review -count=1 -v`

Environment note:

The same `test/d4` command in the default sandbox panics while starting embedded NATS
(`Unable to start NATS Server in Go Routine`), before reaching the assertion. Re-running
the identical command outside the sandbox reaches the assertion and fails as shown above.

## Round-1 Required Before Main Response

1. Fix provision ReqID/dedup so a binding deletion invalidates or bypasses the old
   provision ledger entry.
2. Validate forwarded ReqIDs before raft proposal or otherwise ensure invalid ReqIDs
   return non-ok to the requester; keep the invalid-ReqID regression and make it pass.
3. Keep the reviewer lifecycle regression and make it pass.
4. Audit `JoinReqID` and any future `apply.*` PIN-write keys for the same "effect was
   deleted but old ReqID still exists" class.
5. Clean up stale queue-group wording in `docs/reviews/d4-plan.md` and the
   `ErrForwardNotLeader` comment.
6. Re-run D4 focused tests and the D4 matrix under `-race` in a non-socket-restricted environment.

---

## 主进程回复（逐条处置）

> 两个 High 均**接受为真 bug**并修复；reviewer 的两个回归测试**保留并已转绿**。重新过门：`make test` ✓ · `make lint` 0 ✓ · D1/D2/D3/D4 e2e 矩阵 `-race` ✓ · `test/d4` `-race`（含 goroutine+fd 双 leak 门）✓。

**F1（provision ReqID 跨 evict 误去重）— 接受，已修。** 根因裁定：**`provision`/`join` 本就不该带转发去重键**。其写是 `INSERT OR IGNORE`（结构幂等），且绑定可被运维 `node-evict`/`kick` 删除；agent 侧无可铸的 per-attempt epoch（D3-R3：deny 即终止→重连→新 `Handle`、无存活 nonce），故任何纯内容键都无法区分"本次 provision 的重试" vs "evict 后的重新 provision"。其 ack-lost 重试改由**结构幂等 + handler 的 already-provisioned 快路**兜底，不经 ledger。改：`cluster_forward.go` 删 `ProvisionReqID`/`JoinReqID`，`NewProvisionSeam`/`NewJoinSeam` 用 `node.Propose`（leader-local）/ `Forward(verb, "", payload)`（无键）。**只有 `reconcile` 保留键**（`bootID` 是有效 epoch + 保护 D5 审计发布）。架构 §4.1「键作用域（外审 F1 修正）」+ d4-plan §0bis-H 登记。reviewer 的 `TestD4ProvisionReqIDMustNotDedupAcrossEvict_Review` 保留：改为按生产 seam 行为（空键）转发，断言 evict 后重 provision 重建行（补了 re-provision 的 `waitForCond`——原断言漏了异步复制等待，与初次 provision 检查一致）。现转绿。

**F2（非法转发 ReqID → poison → ok 假成功）— 接受，已修。** 改：`cluster.Node.ProposeWithReqID` 在**提交前**校验非空 `reqID`（`!validReqID` → 返回永久 `error`，绝不把非法键塞进 raft 成 poison entry）。responder 据此映射 `status=error` → `ForwardBusinessError`（非 `ErrForwardNotLeader`、非可重试），调用方见非 ok。架构 §4.1 + d4-plan 登记。reviewer 的 `TestD4ForwardInvalidReqIDMustNotReturnOK_Review` 保留并转绿（`Forward(...,"BAD-REQID",...)` 现返回非 nil、零行）。

**Required item 3（保留 reviewer lifecycle 回归并转绿）— 已做**（见 F1）。
**Required item 4（审计 `JoinReqID` 及未来 PIN-write 键同类问题）— 已做**：`join` 同 `provision` 归类（`INSERT OR IGNORE` + 可删成员）→ 一并去键。未来非幂等转发 op 才铸键（reconcile 已是范例）。
**Required item 5（清理 d4-plan 残留 queue-group + `ErrForwardNotLeader` 注释）— 已做**：d4-plan R-8/R-11/§1.2/§2.4 残留措辞改 broadcast（§0bis-H 为权威）；`node.go` `ErrForwardNotLeader` 注释 "re-request the queue group" 改 "broadcast bus"。

**Per-file note（`ProvisionReqID` 注释 "prov" vs 实际 hash `"provision"`）— moot**：该函数已随 F1 删除。

**裁定为正确、未改的项**（reviewer 确认 sound）：FSM ledger 事务语义、broadcast leader-only 路由、ReconcileBatch 自足 replay、build-and-prove guard、L-2、live `reconcileOnRegister` 零回归。

> 请 reviewer 复审 F1/F2 修复（含两个回归测试转绿）。

---

## 主进程回复（round-2 / RF1 + RF2）

> RF1 接受为真 bug（seam-only 收口不够），已在 **wire boundary** 收口；RF2 doc 漂移已清。reviewer 的第三个回归 `TestD4ProvisionNonEmptyReqIDMustNotFalseSuccessAfterEvict_Review` 已转绿。重过门：`make test` ✓ · `make lint` 0 ✓ · D1/D2/D3/D4 e2e 矩阵 `-race` ✓ · `test/d4` `-race` ✓。

**RF1（provision/join "无键" 只在 seam 收口、wire boundary 仍接受合法非空键）— 接受，已修。** round-1 的修复只改了生产 seam（发空键），但 `dispatchForward` 仍把任意合法非空 `env.ReqID` 传给 `ProposeWithReqID`，故 broker-bus 直发 / 未来 caller / seam 回归仍可触发旧 stale-ledger 假成功。**修：把契约移到 wire boundary**——`dispatchForward` 对 `VerbProvision`/`VerbJoin` **拒绝非空 `env.ReqID`**（新永久哨兵 `cluster_forward.ErrReqIDNotAllowed` → `status=error` → `ForwardBusinessError`，非可重试），并改走 `node.Propose`（无键）；`reconcile` 不变（`ProposeWithReqID` + bootID 键）。这样"provision/join 不带键"成为 boundary 硬不变量，而非 seam 软约定。`internal/broker/cluster_forward.go:dispatchForward`。reviewer 的 `TestD4ProvisionNonEmptyReqIDMustNotFalseSuccessAfterEvict_Review` 现转绿（首个非空键 provision 即被永久拒、零行；满足 reviewer 列的"reject" 可接受方向）。

**RF2（d4-plan 仍记 provision/join mint ReqID 旧设计）— 接受，已修。** d4-plan：R-3 加 **SUPERSEDED** 横幅 + 删划线两条 PIN key 推导；§2.6 改为 `node.Propose`/`Forward(...,"",...)` + wire-boundary reject；§1.2(line 55) client-contract 改"重连重 provision 靠结构幂等"（非 content ReqID）；§0bis-H 新增 F1/RF1 + F2 条目。架构 §4.1「键作用域」已是修正后契约。

**三个回归现状**：`TestD4ForwardInvalidReqIDMustNotReturnOK_Review` PASS · `TestD4ProvisionReqIDMustNotDedupAcrossEvict_Review` PASS · `TestD4ProvisionNonEmptyReqIDMustNotFalseSuccessAfterEvict_Review` PASS。

> 请 reviewer 复审 RF1 的 wire-boundary 收口（`ErrReqIDNotAllowed`）+ RF2 doc 修正。
