# P2 Broker + Agent Heartbeat Review

Date: 2026-05-08
Reviewer role: test engineer

## Scope

Reviewed P2 against `docs/architecture.md` P2:

- `tether serve`
- `tether agent`
- temporary `tether admin nodes`
- `internal/broker`
- `internal/agent`
- `internal/node`
- `test/p2`

P2 target: broker recognizes agent online/offline, with register + heartbeat over dev NATS and the node state machine `ONLINE -> STALE -> OFFLINE`.

## Verdict

P2 is not ready to promote.

The basic broker and agent unit tests pass when embedded NATS is allowed to listen on loopback, but the P2 end-to-end path is not stable. More importantly, the agent exits permanently when NATS is up but the broker register responder is not ready yet. That is a real startup resilience bug and explains the flaky P2 heartbeat lifecycle test.

## Findings

### 1. High: agent exits if the broker register responder is temporarily missing

Evidence:

- Added `test/p2/agent_startup_resilience_test.go`.
- `TestAgentSurvivesMissingRegisterResponder` fails reliably:

```text
agent exited before broker responder appeared: agent: register request: nats: no responders available for request
```

Root cause:

- `Agent.Run` calls `a.register(ctx, nc)` once.
- `register` returns immediately on `nc.RequestWithContext` error.
- `Run` returns that error and the daemon exits.

Relevant code:

- `internal/agent/agent.go:81`
- `internal/agent/agent.go:110`
- `test/p2/agent_startup_resilience_test.go:15`

Why this matters:

In the deployed topology, `nats-server` and `tether serve` are separate processes. If NATS is reachable before `tether serve` has subscribed to `register.req`, the agent sees `no responders` and dies instead of waiting/retrying. That is also what makes the P2 e2e test flaky: it starts broker and agent goroutines back-to-back without waiting for the broker subscription to become active.

Suggested fix:

- Make agent registration retry until context cancellation, with bounded per-attempt timeout and short backoff.
- Treat `nats: no responders`, request timeout, and transient NATS reconnect errors as retryable.
- Keep hard rejections from broker (`proto_mismatch`, malformed config) as fatal.
- Keep the new risk test as a regression test.

### 2. Medium: existing P2 heartbeat e2e is flaky

Evidence:

Repeated run:

```text
go test ./test/p2 -run TestHeartbeatLifecycle -count=20
```

failed 2 times with:

```text
waitForState(lab/lab-1, "ONLINE") timed out after 2s; last=""
```

The package-level repeated run also failed:

```text
go test ./internal/agent ./internal/broker ./test/p2 -count=5
```

Why this matters:

The P2 exit criterion is a stable heartbeat loop. A flaky P2 e2e means the test does not reliably prove the phase exit condition. The underlying cause appears to be the same register startup race from finding 1.

Suggested fix:

- Fix agent registration retry first.
- Optionally add a broker-readiness probe in the test, but do not rely only on test-side waiting; the daemon should survive startup ordering in real deployments.

## Verification

Commands requiring embedded NATS were run outside the default sandbox because the sandbox denies loopback listeners (`nc -l 127.0.0.1 ...` returns `Operation not permitted`).

Commands run:

```bash
env GOCACHE=/tmp/tether-go-build-cache go test ./...
env GOCACHE=/tmp/tether-go-build-cache go test ./internal/... ./test/... -cover
env GOCACHE=/tmp/tether-go-build-cache go test ./test/p2 -run TestHeartbeatLifecycle -count=20
env GOCACHE=/tmp/tether-go-build-cache go test ./internal/agent ./internal/broker ./test/p2 -count=5
env GOCACHE=/tmp/tether-go-build-cache make build
./bin/tether version
make lint
```

Results:

- `go test ./...`: fails in `test/p2`.
- `go test ./test/p2 -run TestAgentSurvivesMissingRegisterResponder`: fails reliably.
- `go test ./test/p2 -run TestHeartbeatLifecycle -count=20`: flaky; failed 2/20.
- `go test ./internal/agent`: passes.
- `go test ./internal/broker`: passes.
- `go test ./internal/node ./internal/auth ./internal/proto ./internal/schema ./internal/storage ./test/p1`: passes.
- `make build`: passes.
- `./bin/tether version`: prints `tether v0.0.0-dev (proto v1)`.
- `make lint`: not run locally because `golangci-lint` is not installed; Makefile reports `Run: make tools`.

## Recommendation

Do not start P3 yet. Fix agent registration retry and then rerun:

```bash
env GOCACHE=/tmp/tether-go-build-cache go test ./...
env GOCACHE=/tmp/tether-go-build-cache go test ./test/p2 -run TestHeartbeatLifecycle -count=20
```

P2 can be reconsidered once both are green.

---

## Maintainer Response (2026-05-08)

### F1 — Agent exits on missing register responder (HIGH)

**Status: ACCEPTED.**

Real bug, and the right framing: in deployment, NATS / tetherd / the agent
are three independent processes started in arbitrary order. NATS becoming
reachable before tetherd has installed its register subscription is the
common case, not a corner case. Treating `nats: no responders` as fatal
made the agent flap on every broker restart.

Fix: `Agent.register` is now a retry loop with exponential backoff (default
100ms → 2s, doubled per failure) and a per-attempt `RegisterTimeout`
governing one round-trip rather than the whole boot. The classification
matches the reviewer's recommendation:

- **Retry**: any error from `nc.RequestWithContext` (no responders,
  per-attempt timeout, NATS reconnect mid-flight) and garbled JSON replies
  (broker bug / partial deploy).
- **Fatal**: an explicit `OK=false` reply from the broker
  (`proto_mismatch`, `nid_mismatch`, etc.) — these are configuration bugs
  no amount of retry will paper over.
- **Cancel-aware**: every backoff sleep races against `ctx.Done()`, so
  Ctrl-C / SIGTERM exits cleanly at any retry stage.

Two new `Config` knobs (`RegisterRetryInitial`, `RegisterRetryMax`) with
sensible defaults so tests and unusual deployments can override without
touching the production code path.

`internal/agent/agent.go` — `register()` rewritten.

### F2 — Heartbeat e2e flaky (MEDIUM)

**Status: ACCEPTED — root-caused to F1, fixed by the same change.**

The flake mode (`waitForState(...) timed out after 2s; last=""`) is the
agent dying on first attempt because the broker hadn't subscribed yet.
With retry, the agent waits the broker out, and the e2e converges
deterministically. No test-side ordering trick added — the daemon is now
robust to startup ordering on its own (which is also the reviewer's
preferred phrasing).

I did NOT add a broker-side readiness probe to the test, on purpose: that
would mask exactly the production failure mode F1 was about. The test now
exercises the same race the deployed system would face.

### Tests added / kept

- `test/p2/agent_startup_resilience_test.go`
  (`TestAgentSurvivesMissingRegisterResponder`) — kept verbatim as the
  reviewer authored it. Now passes.
- `internal/agent/agent_test.go` — added two same-package tests so the
  retry branches are covered by `-cover`:
  - `TestAgentRegisterRetriesUntilResponderAppears` — mirrors the e2e
    intent, ensures agent doesn't exit while no responder, and reaches
    success once one appears.
  - `TestAgentRetriesOnGarbledReply` — first attempt gets `not json`,
    next gets a valid OK reply; agent must retry past the garbled one.

`TestAgentSurfacesRegisterRejection` still passes unchanged: `OK=false`
from the broker is classified as fatal and propagated immediately, no
retry. `TestAgentRunFailsOnBadNATSURL` also unchanged: the failure is at
`nats.Connect` before `register`, so the retry path isn't reached.

### Verification

```bash
go test ./...
  ok  ... cmd/tether internal/{agent,auth,broker,node,proto,schema,storage}
  ok  ... test/p1
  ok  ... test/p2

go test ./test/p2 -run TestAgentSurvivesMissingRegisterResponder
  PASS  (0.34s)         # was: failing reliably

go test ./test/p2 -run TestHeartbeatLifecycle -count=20
  ok    (15.2s total)   # was: 2/20 failures

go test ./internal/agent ./internal/broker ./test/p2 -count=5
  ok                    # reviewer's repro command

go test -cover ./internal/...
  agent     90.0%       # 90.2% → 78.6% (new branches) → 90.0% (new tests)
  auth      85.4%
  broker    78.4%
  node      81.2%
  proto    100.0%
  schema   [no statements]
  storage   81.8%
```

### Notes

- `make lint` still requires `make tools` locally — already documented as
  the v1 contract (P0 review F1).
- No broker-side change. The broker correctly handles startup ordering
  too; the bug was strictly in agent's brittle single-shot register.

