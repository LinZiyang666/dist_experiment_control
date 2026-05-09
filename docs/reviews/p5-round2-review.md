# P5 Round 2 Review

Date: 2026-05-08
Reviewer role: test engineer

## Scope

Reviewed commit:

- `0bd8c21 Address P5 review F1: wire run into proc lifecycle (visible to ps + audit)`

Focus:

- P5 review F1 from `docs/reviews/p5-review.md`
- PTY `run` integration with the P4 process state machine
- P5 e2e stability and P1-P4 regression impact

## Verdict

P5 is approved.

I found no new P5-blocking issue in this round. The prior blocker is fixed:
PTY `run` now publishes the same proc lifecycle events as `exec`, so `tether ps`
can observe RUNNING/EXITED state and normal `audit.proc{start,exit}` handling
is restored through the existing broker path.

## Verified Fixes

### F1 - `run` processes are recorded in the proc lifecycle

The fix is in `internal/agent/run.go`:

- after `sess.Start(...)` succeeds and the PTY session is registered in
  `a.procs`, the agent calls `a.pubProcStarted(nc, pid, req.Argv,
  req.ActorFP)` before sending the `started` lifecycle chunk;
- before the final `RunChunk{Kind:"exit"}`, the agent calls
  `a.pubProcExit(nc, pid, exitCode)`;
- attach timeout, PTY allocation failure, and exec-start failure still do not
  create process rows because no child successfully started.

This matches the intended split: successful PTY children enter
`internal/proc`; pre-start failures remain failure/audit events only.

The reviewer risk test now passes:

```text
=== RUN   TestRunRecordsProcessLifecycleForPs
--- PASS: TestRunRecordsProcessLifecycleForPs (3.46s)
PASS
ok  	github.com/LinZiyang666/tether/test/p5	3.468s
```

## Verification

Commands requiring embedded NATS were run outside the default sandbox because
the sandbox denies loopback listeners.

```text
env GOCACHE=/tmp/tether-go-build-cache go test ./test/p5 -run TestRunRecordsProcessLifecycleForPs -count=1 -v
# PASS

env GOCACHE=/tmp/tether-go-build-cache go test ./test/p5 -count=1 -v
# PASS, 7 tests

env GOCACHE=/tmp/tether-go-build-cache go test ./internal/pty ./internal/agent ./internal/broker ./internal/proc -count=1
# PASS

env GOCACHE=/tmp/tether-go-build-cache go test ./... -count=1
# PASS: cmd/tether, internal/*, test/p1, test/p2, test/p3, test/p4, test/p5

env GOCACHE=/tmp/tether-go-build-cache go test ./test/p5 -count=3
# PASS, 25.795s

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

## Residual Notes

- Resize behavior and a real interactive shell/vim workflow still deserve
  dedicated tests, but I did not find evidence that they block P5 promotion.
- P5 continues to rely on the P3/P4 permission templates for `cmd.by.*` and
  `pty.*` subjects; I did not find a new subject-permission regression.

## Recommendation

P5 can be promoted. Keep `TestRunRecordsProcessLifecycleForPs` as a regression
test before moving into P6.
