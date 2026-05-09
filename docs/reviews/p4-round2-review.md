# P4 Round 2 Review

Date: 2026-05-08
Reviewer role: test engineer

## Scope

Reviewed commit:

- `3f29fda Address P4 review: agent provisioning + auth_callout closes F1; F2-F4 fixes`

Focus:

- previous P4 review findings F1-F4
- secure `auth_callout` agent provisioning path
- anonymous P4 app-layer regression path
- P1-P3 regression impact

## Verdict

P4 is approved.

I found no new P4-blocking issue in this round. The four findings from
`docs/reviews/p4-review.md` are addressed, and the reviewer-added risk tests now
pass unchanged.

## Verified Fixes

### F1 - Agent role is provisioned through `auth_callout`

The previous blocker is closed. `roleAgent` is no longer granted from the
client-controlled connection name alone:

- `internal/authcallout/handler.go:116-125` routes agent connections through
  `ensureAgentProvisioned` before issuing `PermissionsForAgent`.
- `internal/authcallout/handler.go:195-246` validates `sid`, `nid`, and the
  public user nkey, checks the `(sid,nid)` binding, denies hijacks, requires a
  PIN for first bootstrap, and re-checks session ACTIVE on reconnect.
- `internal/agent/agent.go:309-338` now uses the auth-aware
  `tether-agent:<sid>:<nid>` connection name plus `nats.Nkey`, with optional
  `nats.Token(pin)`.
- `cmd/tether/serve.go` now exposes `--auth-callout-seeds-dir`, so the real
  daemon command can run the secure P3/P4 auth boundary.

Coverage:

- `test/p4/exec_authcallout_test.go` exercises secure agent PIN-bootstrap,
  pinless reconnect, and unprovisioned-agent denial.
- `internal/authcallout/handler_test.go` covers no-PIN denial, PIN bootstrap,
  rebind, hijack denial, and wildcard denial.
- `test/p3/agent_role_risk_test.go` still confirms wildcard and unprovisioned
  agent roles are denied without provisioning/PIN.

### F2 - Missing/offline node no longer hangs exec

`internal/broker/exec.go:64-82` performs the node pre-forward check. Missing
nodes return `node_not_found`; non-ONLINE nodes return `node_offline`. The
broker also writes `audit.call` with `ok=false` for those rejections.

The reviewer test `TestExecMissingNodeReturnsBrokerError` passes.

### F3 - `ps` rejects DELETING sessions

`internal/broker/exec.go:190-199` applies the same `session.IsActive` gate to
`ps` that `exec` already used.

The reviewer test `TestPsRejectsDeletingSession` passes.

### F4 - Process actor attribution is stamped by the broker

`internal/broker/exec.go:84-97` discards any ctl-supplied `actor_fp`, stamps the
fingerprint parsed from the authenticated `by.<actor>` subject, and forwards the
re-marshaled request. The agent only echoes that broker-stamped value into
`ProcStartedEvent.StartedByFP`.

The reviewer test `TestExecRecordsStartedByFingerprint` passes.

## Verification

Commands requiring embedded NATS were run outside the default sandbox because
the sandbox denies loopback listeners.

```text
env GOCACHE=/tmp/tether-go-build-cache go test ./test/p4 -count=1 -v
# PASS, 12 tests, including secure auth_callout e2e and all 3 reviewer risk tests

env GOCACHE=/tmp/tether-go-build-cache go test ./internal/agentprov ./internal/authcallout ./internal/agent ./internal/broker ./internal/node ./internal/proc -count=1 -v
# PASS

env GOCACHE=/tmp/tether-go-build-cache go test ./... -count=1
# PASS: cmd/tether, internal/*, test/p1, test/p2, test/p3, test/p4

env GOCACHE=/tmp/tether-go-build-cache go test ./test/p4 -count=3
# PASS, 26.643s

env GOCACHE=/tmp/tether-go-build-cache make build
# PASS; produced bin/tether

./bin/tether version
# tether v0.0.0-dev (proto v1)
# linux/amd64
# go1.25.0

make tools
# PASS; installed /home/weiland/go/bin/golangci-lint

/home/weiland/go/bin/golangci-lint version
# golangci-lint has version 2.5.0 built with go1.25.1

PATH=/home/weiland/go/bin:$PATH make lint
# PASS: 0 issues.
```

`make build` emitted a Go stat-cache warning because the module cache is
read-only in this environment, but the command exited 0 and produced a working
binary.

Initial lint attempts failed after installing the tool because the sandboxed
process could not clear stale entries in `/home/weiland/.cache`. After running
`golangci-lint cache clean` and `go clean -cache` outside the sandbox, lint
completed with `0 issues`.

## Residual Notes

- I did not add new tests in this round; the maintained tests already exercise
  the previous high-risk paths and passed.
- Agent revoke and persisted `state.json` hints remain explicitly out of scope
  per the maintainer response and do not block P4.
