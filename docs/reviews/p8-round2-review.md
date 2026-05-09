# P8 Round 2 Review

Date: 2026-05-09
Reviewer role: test engineer

## Scope

Reviewed commit:

- `16a8530 Address P8 review: register DELETING gate (F1) + PID-reuse triple (F2) + lint (F3)`

Focus:

- Round-1 findings in `docs/reviews/p8-review.md`
- `register.req` DELETING-session ingress gate
- G.1 PID-reuse triple data path and mismatch handling
- P8 regression impact across broker, agent, proc, proto, and prior phase tests

## Verdict

P8 is approved.

All three round-1 findings are fixed. I did not find a new P8-blocking issue in
this round.

## Verified Fixes

### F1 - `register.req` rejects DELETING sessions before side effects

`internal/broker/broker.go` now calls `session.IsActive` before `node.Register`.
The rejected path returns `Code:"session_not_found_or_deleting"` and does not
write node state, publish `agent_registered`, or run `reconcileOnRegister`.

Reviewer acceptance test:

```text
go test ./test/p8 -run TestReviewRegisterRejectsDeletingSession -count=10 -v
# PASS
```

### F2 - PID-reuse triple is now wired through the reconciliation path

The implementation now carries the G.1 data through the production path:

- `proto.LocalProcess` includes `StartedAt` and `StartTimeTicks`.
- `proto.ProcStartedEvent` includes `BootID` and `StartTimeTicks`.
- PTY `run` captures `/proc/<os_pid>/stat` field 22 after fork.
- `proc.Insert` persists `boot_id` and nullable `start_time_ticks`.
- `reconcileOnRegister` compares broker row vs register snapshot and treats a
  mismatch as original row EXITED(-1) plus `drop_processes` for the new orphan.

The fallback for missing triple data preserves legacy/non-Linux/exec behavior,
which is the right compatibility choice for this codebase.

Acceptance tests:

```text
go test ./test/p8 -run 'TestReview|TestG1PIDReuse|TestG1TripleMatch|TestG1MissingTriple|TestG1StartTimeTicksPersisted' -count=10 -v
# PASS
```

### F3 - lint gate is clean

The unused `runExec` helper was removed. `golangci-lint` now reports 0 issues.

## Verification

Commands requiring embedded NATS/JetStream or Go cache writes were run outside
the default sandbox.

```text
go test ./test/p8 -count=1 -v
# PASS, 21 tests

go test ./internal/broker ./internal/agent ./internal/proc ./internal/proto -count=1
# PASS

go test ./... -count=1
# PASS

go vet ./...
# PASS

PATH=$PATH:/home/weiland/go/bin make lint
# PASS, 0 issues

go build ./...
# PASS
```

## Residual Notes

- `heartbeat` and agent runtime `ev.*` messages still do not use the
  `session.IsActive` gate. That is not a new P8 register/reconcile issue, and
  command ingress remains protected, but it is worth keeping in mind if H.3
  cleanup semantics are tightened later.
