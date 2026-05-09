# P8 Review

Date: 2026-05-09
Reviewer role: test engineer

## Scope

Reviewed commit:

- `0bbf183 P8: reconciliation (G.1 agent reconnect + G.2 broker boot)`

Focus:

- G.1 agent reconnect reconciliation for processes and ports
- G.2 broker boot reconciliation
- LOST read-side derivation
- P8 chaos/e2e coverage and regression impact across P1-P7

## Verdict

P8 is not approved yet.

The implemented happy path is strong: existing P8 tests pass, and the chaos
case of agent crash + restart converges RUNNING process rows to EXITED(-1).
However, I found two contract-level gaps that should be fixed before promoting
P8.

## Findings

### F1 - High: `register.req` accepts a DELETING session and mutates state

`internal/broker/broker.go:422-481` parses and validates the register payload,
then calls `node.Register` directly. `node.Register` only checks that the
session row exists; it does not require `sessions.state='ACTIVE'`.

This violates architecture C.1 §6: any request targeting a DELETING session
must fail immediately with `session_not_found_or_deleting`, independently of
JWT permissions. The auth_callout path checks active state on new CONNECT, but
that is not a complete substitute for the application-layer ingress gate:
v1 explicitly keeps old JWTs alive, and the dev/no-auth topology can still
publish the register subject.

Impact:

- a tombstoned session can get a fresh `nodes` row or have an existing node
  forced back to ONLINE;
- `agent_registered` can be emitted for a session that is being destroyed;
- `reconcileOnRegister` can write process/port reconciliation side effects
  while H.3 cleanup is supposed to reject ingress.

Reviewer test added:

```text
go test ./test/p8 -run TestReviewRegisterRejectsDeletingSession -count=1 -v

register into DELETING session should be rejected; got OK=true code="" err=""
```

Recommendation:

Add the same `session.IsActive` precheck used by exec/run/ps/expose before
`node.Register`. Return `Code:"session_not_found_or_deleting"` and do not write
nodes, sys.events, audits, or reconciliation side effects.

### F2 - High: PID reuse protection required by G.1 is not implementable

The architecture requires `local_processes[]` to carry
`started_at` and `start_time_ticks`, and says broker validation must use
`(boot_id, pid, start_time_ticks)` to distinguish a live original process from
a different OS process that reused the same PID.

Current code explicitly omits that:

- `internal/proto/messages.go:43-47` has only `PID`, `State`, and `RC`.
- `internal/broker/reconcile.go:14-21` documents that PID reuse detection is
  not implemented.
- `internal/broker/reconcile.go:65-71` accepts any agent-reported
  `State:"running"` with the same PID, without checking `boot_id` or
  `start_time_ticks`.

This means a stale SQLite RUNNING row can be incorrectly preserved when the
agent reports a newer, unrelated process with the same PID. In that case the
broker neither marks the original row EXITED(-1) nor treats the new process as
an orphan to drop.

Reviewer test added:

```text
go test ./test/p8 -run TestReviewLocalProcessCarriesPIDReuseFields -count=1 -v

proto.LocalProcess missing StartedAt; G.1 cannot verify (boot_id,pid,start_time_ticks)
```

Recommendation:

Do not leave this as an undocumented v1 exception in a P8 implementation. Add
the protocol fields, persist/read `processes.start_time_ticks`, collect
`/proc/<pid>/stat` field 22 in the agent snapshot, and make mismatch follow the
G.1 table: old row EXITED(-1, `reconciled_closed`), new pid handled as orphan.
If product scope intentionally excludes this, update the architecture and test
plan explicitly before calling P8 complete.

### F3 - Low: `make lint` fails on an unused P8 helper

After installing the pinned toolchain with `make tools`, lint still fails:

```text
PATH=$PATH:/home/weiland/go/bin make lint

test/p8/reconcile_e2e_test.go:200:6: func runExec is unused (unused)
```

`test/p8/reconcile_e2e_test.go:200` defines `runExec`, but the current P8 tests
seed process rows directly instead of calling it. Remove the helper or add the
intended coverage so the lint gate returns to 0 issues.

## Verification

Commands requiring embedded NATS/JetStream or Go cache writes were run outside
the default sandbox.

Existing P8 suite before adding reviewer risk tests:

```text
go test ./test/p8 -count=1 -v
# PASS, 15 tests
```

Related packages:

```text
go test ./internal/broker ./internal/agent ./internal/proc ./internal/port -count=1
# PASS
```

Reviewer risk tests:

```text
go test ./test/p8 -run 'TestReview' -count=1 -v
# FAIL: TestReviewRegisterRejectsDeletingSession
# FAIL: TestReviewLocalProcessCarriesPIDReuseFields
```

Full suite with reviewer risk tests present:

```text
go test ./... -count=1
# PASS: cmd/tether, internal/*, test/p1, test/p2, test/p3, test/p4, test/p5, test/p6, test/p7
# FAIL: test/p8 only, due to the two reviewer risk tests above
```

Static checks:

```text
go vet ./...
# PASS

make tools
# installed golangci-lint v2.5.0 to /home/weiland/go/bin

PATH=$PATH:/home/weiland/go/bin golangci-lint version
# golangci-lint has version 2.5.0 built with go1.25.1

PATH=$PATH:/home/weiland/go/bin make lint
# FAIL: unused runExec helper in test/p8/reconcile_e2e_test.go
```

## Reviewer Tests Added

Added `test/p8/p8_review_risk_test.go` with:

- `TestReviewRegisterRejectsDeletingSession`
- `TestReviewLocalProcessCarriesPIDReuseFields`

Keep these tests as the fix acceptance tests for the next round.

---

## Maintainer Response (round 1)

Date: 2026-05-09

### F1 — accepted, fixed

The C.1 §6 ingress gate was missing on `register.req`. `internal/broker/broker.go`
now runs `session.IsActive` before `node.Register`, returning
`code:"session_not_found_or_deleting"` when the session is missing or in
DELETING. No nodes/sys.events/audit/reconcile side effects fire on the
rejected path.

Reviewer's `TestReviewRegisterRejectsDeletingSession` now passes.

### F2 — accepted with reasoning, fixed

I want to flag the in-code reality before describing the fix, then state why
I implemented the verification anyway:

In v1 as it stood, the agent's `a.procs` map was strictly in-process: no disk
persistence, no rebuild path on restart. After agent restart `a.procs` is
empty, so the snapshot lists no PIDs as `running`, and broker's missed-exit
path takes over. There was no executable code path through which the agent
could ever claim a stale ULID was running with a reused OS PID — so the
"stale RUNNING row preserved" scenario was not observable today.

That said, the architecture spec (line 1044) literally calls for the
`(boot_id, pid, start_time_ticks)` triple, the protocol field absence was
real, and adding the verification is cheap and forward-defensive. So I
implemented the full path:

- `proto.LocalProcess` + `StartedAt` and `StartTimeTicks`.
- `proto.ProcStartedEvent` + `BootID` and `StartTimeTicks` so the data lands
  in `processes` at insert time, not just on register.
- `proc.Process.StartTimeTicks` + scanning + `nullableInt64` so legacy/exec
  rows stay NULL (avoids spurious "tick 0 == tick 0" matches).
- `internal/agent/pty.Session.OSPID()` + `internal/agent/agent.go`
  `readStartTimeTicks(osPID)` reads `/proc/<pid>/stat` field 22 honoring the
  last-`)` rule for comm-with-spaces.
- `agent.procRec` replaces the bare `*pty.Session` map value so each ULID
  carries `osPID + startTimeTicks + startedAt` captured at fork time.
- `pubProcStartedWithTriple` ships the triple data to the broker; exec-style
  children call the legacy `pubProcStarted` (no triple — they're sync, no
  agent-side persistence path that would need it).
- `broker.reconcileOnRegister.pidReused()` does the comparison: triple
  mismatch → original row EXITED(rc=-1, `reconciled_closed`) AND new pid
  pushed into `drop_processes` so the agent kills the squatter. Missing
  data on either side → falls back to the pre-triple accept path so legacy
  rows / non-Linux agents don't get false-positive kills.

Coverage I added (all green in `test/p8`):

- `TestG1PIDReuseTriggersReconcileAndOrphan` — triple mismatch → EXITED-1
  + `drop_processes` contains the pid + accepted is empty.
- `TestG1TripleMatchKeepsRowRunning` — matching triple → accept; row stays
  RUNNING.
- `TestG1MissingTripleFallsBackToAccept` — NULL `boot_id`/`start_time_ticks`
  in SQLite → accept (no false-positive kill of legacy rows).
- `TestG1StartTimeTicksPersistedFromProcStarted` — ProcStartedEvent triple
  data round-trips into `processes.boot_id` / `.start_time_ticks` so the
  next reconcile has something to compare.

Reviewer's `TestReviewLocalProcessCarriesPIDReuseFields` now passes.

### F3 — accepted, fixed

Removed unused `runExec` helper from
`test/p8/reconcile_e2e_test.go`. All P8 tests seed process rows via direct
DB inserts so `runExec` was genuinely dead code.

`PATH=$PATH:/home/weiland/go/bin golangci-lint run ./...` → `0 issues.`

### Verification

```text
go build ./...                → ok
go vet ./...                  → ok
golangci-lint run ./...       → 0 issues
go test ./...                 → all 26 packages PASS, 21 cases in test/p8
```

Authoritative test from the architecture's P8 chaos invariant
(`TestChaosKillAgentRestartConverges`) still passes: real broker + real
agent + 10 RUNNING rows seeded mid-flight + agent kill + agent restart →
all 10 → EXITED(rc=-1) + `reconciled_closed` audit lands in
`history-<sid>`.
