# P5 Review

Date: 2026-05-08
Reviewer role: test engineer

## Scope

Reviewed commit:

- `4a3b42d P5: PTY interactive run + two-phase attach + Ctrl-C kill`

Focus:

- P5 PTY `run` lifecycle
- two-phase attach handshake
- Ctrl-C / `kill` forwarding
- interaction with the P4 process state machine and `tether ps`

## Verdict

P5 is not ready to promote.

The basic PTY happy path works: `run echo`, non-zero exit propagation, attach
timeout, kill/SIGINT, DELETING-session rejection, and missing-node rejection all
pass in the existing P5 e2e suite.

However, P5 currently bypasses the P4 process lifecycle. A `run` process is not
recorded in SQLite as RUNNING, is not later marked EXITED, and therefore does
not show up in `tether ps` or normal `audit.proc{start,exit}` handling.

I added `test/p5/run_risk_test.go` to cover this.

## Findings

### F1 - High: `run` processes are invisible to `ps` and normal proc audit

Evidence:

- `internal/agent/run.go:104-117` starts the PTY child and replies
  `RunChunk{Kind:"started"}`, but does not publish a
  `ProcStartedEvent`.
- `internal/agent/run.go:147-168` waits for the child and replies
  `RunChunk{Kind:"exit"}`, but does not publish a `ProcExitEvent`.
- `internal/agent/exec.go:171-201` already has the needed
  `pubProcStarted` / `pubProcExit` helpers used by non-PTY `exec`.
- `internal/broker/exec.go:128-164` is already subscribed to
  `ev.node.<nid>.proc.<pid>.{started,exit}` and knows how to insert /
  mark process rows plus write `audit.proc{start,exit}`.
- `internal/broker/run.go:192-213` only handles `pty.<pid>.failed` audit
  events; it does not compensate for missing start/exit proc events.

Why this matters:

P4 introduced `internal/proc` as the authoritative process state machine.
P5 builds an interactive process mode on top of that same control plane. If
`run` does not emit proc lifecycle events:

- `tether ps` cannot show `tether run sleep 30` as RUNNING.
- `tether ps -a` cannot show the same PTY process as EXITED after it exits.
- `started_by_fp` attribution is missing for PTY processes.
- Normal `audit.proc{kind:start}` and `audit.proc{kind:exit}` are missing for
  successful PTY runs.

Added test:

- `test/p5/run_risk_test.go::TestRunRecordsProcessLifecycleForPs`

Failure:

```text
--- FAIL: TestRunRecordsProcessLifecycleForPs (2.40s)
    run_risk_test.go:88: run process to be recorded RUNNING: condition not met within 2s
```

Recommendation:

After `sess.Start(...)` succeeds and before or around the `started` lifecycle
chunk, publish `proc.started` with:

- the same P5 pid,
- `req.Argv`,
- `req.ActorFP`,
- current timestamp.

After `sess.Wait()` returns, publish `proc.exit` with the same pid and exit
code before or around the `exit` lifecycle chunk. Reuse the existing
agent-side helpers from `exec` if possible, or move them to a shared file in
the `agent` package. Keep attach-timeout and exec-start failures separate:
attach timeout should not create a process row because no child was started.

The risk test should then verify RUNNING after `started`, EXITED after kill /
exit, and the correct `StartedByFP`.

## Verification

Commands requiring embedded NATS were run outside the default sandbox because
the sandbox denies loopback listeners.

Baseline existing P5 suite before adding the risk test:

```text
env GOCACHE=/tmp/tether-go-build-cache go test ./test/p5 -count=1 -v
# PASS
# ok   github.com/LinZiyang666/tether/test/p5  5.179s
```

P5-related internal packages:

```text
env GOCACHE=/tmp/tether-go-build-cache go test ./internal/pty ./internal/agent ./internal/broker ./internal/proc -count=1
# PASS
```

New risk test:

```text
env GOCACHE=/tmp/tether-go-build-cache go test ./test/p5 -run TestRunRecordsProcessLifecycleForPs -count=1 -v
# FAIL: run process to be recorded RUNNING: condition not met within 2s
```

Full suite with the risk test present:

```text
env GOCACHE=/tmp/tether-go-build-cache go test ./... -count=1

ok  	github.com/LinZiyang666/tether/cmd/tether
ok  	github.com/LinZiyang666/tether/internal/agent
ok  	github.com/LinZiyang666/tether/internal/agentprov
ok  	github.com/LinZiyang666/tether/internal/auth
ok  	github.com/LinZiyang666/tether/internal/authcallout
ok  	github.com/LinZiyang666/tether/internal/broker
ok  	github.com/LinZiyang666/tether/internal/cli
ok  	github.com/LinZiyang666/tether/internal/node
ok  	github.com/LinZiyang666/tether/internal/proc
ok  	github.com/LinZiyang666/tether/internal/proto
ok  	github.com/LinZiyang666/tether/internal/pty
ok  	github.com/LinZiyang666/tether/internal/schema
ok  	github.com/LinZiyang666/tether/internal/session
ok  	github.com/LinZiyang666/tether/internal/storage
ok  	github.com/LinZiyang666/tether/test/p1
ok  	github.com/LinZiyang666/tether/test/p2
ok  	github.com/LinZiyang666/tether/test/p3
ok  	github.com/LinZiyang666/tether/test/p4
FAIL	github.com/LinZiyang666/tether/test/p5
```

Build and lint:

```text
env GOCACHE=/tmp/tether-go-build-cache make build
# PASS; produced bin/tether

./bin/tether version
# tether v0.0.0-dev (proto v1)
# linux/amd64
# go1.25.0

PATH=/home/weiland/go/bin:$PATH make lint
# PASS: 0 issues.
```

`make build` emitted the same Go stat-cache warning seen in prior reviews
because the module cache is read-only in this environment, but the command
exited 0 and produced a working binary.

## Notes

- I did not find a new NATS permission-boundary issue in this pass. P5 reuses
  the P3/P4 `cmd.by.<actor>.node.*.*.req` and PTY subject templates.
- The current P5 suite still does not directly exercise resize behavior or a
  real interactive shell/vim workflow. Those are useful follow-up tests, but
  F1 is the blocker because it breaks the process-state contract shared by P4
  and P5.

## Recommendation

Do not start P6 yet. Close F1 first, keep the added risk test, then rerun the
full suite.

---

## Maintainer Response (2026-05-08)

F1 accepted; reviewer is right. The miss is exactly what the review describes:
`run` builds on the same control plane as `exec` but I forgot to wire it into
the same proc lifecycle, so PTY processes were invisible to `ps` / audit.

### Fix

`internal/agent/run.go`:

- After `sess.Start(...)` succeeds and the session is registered into
  `a.procs`, call the existing `a.pubProcStarted(nc, pid, req.Argv,
  req.ActorFP)` (the same helper exec already uses). Done BEFORE the
  `RunChunk{Kind:"started"}` lifecycle reply.
- Before the final `RunChunk{Kind:"exit"}` lifecycle reply, call
  `a.pubProcExit(nc, pid, exitCode)`.
- Failures BEFORE `sess.Start()` returns success (attach_timeout /
  pty_alloc_failed / exec_failed) deliberately do NOT publish
  proc.started — no child was actually started, so no SQLite row should
  exist. Those continue to surface via `PtyFailedEvent` →
  `audit.proc{kind:reason}` (broker.handlePtyFailed).

Two-line code change, no new helpers needed. broker.handleProcEvent
(already wired in P4) inserts the row at proc.started and marks it
EXITED at proc.exit; the same `started_by_fp` broker-stamping flow as
exec applies (broker re-marshals RunReq with ActorFP at forward time,
agent echoes it into ProcStartedEvent).

### Verification

```text
env GOCACHE=/tmp/tether-go-build-cache go test ./test/p5 -count=1 -v

=== RUN   TestRunHelloWorld                       PASS  (0.54s)
=== RUN   TestRunExitCodePropagates               PASS  (0.54s)
=== RUN   TestAttachTimeout                       PASS  (3.38s)
=== RUN   TestKillSendsSIGINT                     PASS  (0.54s)
=== RUN   TestRunRejectedForDeletingSession       PASS  (0.09s)
=== RUN   TestRunRejectedForMissingNode           PASS  (0.08s)
=== RUN   TestRunRecordsProcessLifecycleForPs     PASS  (3.45s)   ← reviewer risk test

env GOCACHE=/tmp/tether-go-build-cache go test ./... -count=1
# 19 packages green (cmd/tether + internal/{agent,agentprov,auth,authcallout,
# broker,cli,node,proc,proto,pty,schema,session,storage} + test/{p1..p5}).

PATH=/home/weiland/go/bin:$PATH golangci-lint run ./...
# 0 issues.
```

### Notes

- Reviewer flagged the missing resize / interactive-shell tests as
  follow-up. I agree they're worth doing but they need a real terminal
  on both ends to be meaningful (the bytes through PTY look the same
  whether it's a real shell or `sh -c "echo $LINES $COLUMNS"`); not a
  blocker for P5 promotion. Keeping out of this round per the
  reviewer's own categorization.
- attach_timeout / exec_failed / pty_alloc_failed continue to be
  audit-only (no proc row). The risk test only checks the success
  path, which matches the failure-mode contract documented in the
  agent run handler comment block.
