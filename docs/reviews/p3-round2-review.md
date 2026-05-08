# P3 Round 2 Review

Date: 2026-05-08
Reviewer role: test engineer

## Scope

Reviewed commit:

- `0b7c9c0 Address P3 review: full NATS auth_callout + login membership verify`

Focus:

- F1 forged `by.<actor>` fix
- F2 `login -s` membership verification
- F3 NATS auth_callout integration
- P3 permission-denied e2e
- newly introduced auth_callout role boundary

## Verdict

P3 is not ready to promote.

The previous three findings are materially fixed for CLI/session flows:

- CLI now connects with `nats.Nkey(...)`.
- `login -s` performs a real NATS CONNECT and no longer writes `current_session` when CONNECT/auth fails.
- P3 e2e now exercises NATS-level permission denial for forged actor, cross-session subscribe, and `.req.forwarded` publish.

However, the new auth_callout implementation introduces a critical agent-role bypass. Any client with any user nkey can set its NATS connection name to `tether-agent:<sid>:<nid>` and receive agent permissions for that session and node. Worse, because the agent sid/nid are not validated before permissions are minted, `tether-agent:*:*` receives wildcard permissions and can act across sessions.

This breaks the same P3 multi-tenant boundary that auth_callout was meant to enforce.

## Findings

### 1. Critical: any client can self-declare as an agent and register into arbitrary sessions

Evidence:

- Added independent test: `test/p3/agent_role_risk_test.go`
- Failing command:

```bash
env GOCACHE=/tmp/tether-go-build-cache go test ./test/p3 -run 'TestAuthCalloutRejects.*AgentRole' -count=1 -v
```

Failures:

```text
=== RUN   TestAuthCalloutRejectsWildcardAgentRole
    agent_role_risk_test.go:42: wildcard agent role was accepted: attacker connected as tether-agent:*:* and registered node evil in session lab
--- FAIL: TestAuthCalloutRejectsWildcardAgentRole (2.03s)
=== RUN   TestAuthCalloutRejectsUnprovisionedAgentRole
    agent_role_risk_test.go:76: unprovisioned agent role was accepted: attacker connected as tether-agent:lab:evil and registered in session lab
--- FAIL: TestAuthCalloutRejectsUnprovisionedAgentRole (2.03s)
```

Root cause:

- `internal/authcallout/handler.go:104` parses role/sid/nid from the client-controlled NATS connection name.
- `internal/authcallout/handler.go:115-120` allows `roleAgent` without checking membership, PIN, provisioning state, or even sid/nid syntax.
- `auth.PermissionsForAgent(sid, nid)` is then minted directly from those untrusted strings.

Impact:

- `tether-agent:lab:evil` lets an arbitrary nkey register node `evil` into existing session `lab`.
- `tether-agent:*:*` grants wildcard agent permissions such as `tether.v1.ctrl.s.*.node.*.register.req`.
- The attacker can pollute node state today and will be positioned to receive/send agent subjects when P4 command forwarding lands.
- This also bypasses the P3 isolation model: a connection should not gain rights in a session merely by naming that session in `CONNECT` metadata.

Why this blocks P3:

The maintainer response says agent auth is a P4 scope cut. That is acceptable only if agent role is denied or kept outside auth_callout until P4. What is not acceptable is granting unauthenticated agent authority in the new P3 auth boundary.

Required fix options:

- Preferred P3 fix: reject `roleAgent` in auth_callout until P4 wires proper agent install/provisioning.
- If agent role must stay enabled now, validate `sid` with `proto.ValidateSID`, validate `nid` with `proto.ValidateNID`, and check that the presented nkey is provisioned for that `(sid,nid)` before issuing `PermissionsForAgent`.
- Add regression tests for both valid but unprovisioned `tether-agent:lab:evil` and wildcard `tether-agent:*:*`.

## Resolved Previous Findings

### F1: forged `by.<actor>` impersonation

Status: fixed for CLI/session subjects.

Evidence:

```text
=== RUN   TestForgedActorCannotTombstoneVictimSession
nats: permissions violation: Permissions Violation for Publish to "tether.v1.ctrl.by.<owner>.session.lab.rm.req" on connection [...]
--- PASS: TestForgedActorCannotTombstoneVictimSession (2.06s)
```

The forged owner subject is now denied at the NATS permission layer.

### F2: `login -s` activated without membership verification

Status: fixed.

Evidence:

```text
=== RUN   TestLoginSessionWithoutPINRequiresMembershipVerification
--- PASS: TestLoginSessionWithoutPINRequiresMembershipVerification (0.00s)
```

`login -s` now attempts CONNECT and does not write `current_session` when broker/auth is unavailable.

### F3: auth_callout missing

Status: implemented for CLI/session paths.

Evidence:

- `internal/authcallout/handler.go` handles `$SYS.REQ.USER.AUTH` request decisions.
- `internal/broker/authcallout.go` installs the broker subscription.
- `internal/cli/natsconn.go` uses `nats.Nkey(...)`.
- `test/p3/permissions_e2e_test.go` covers NATS-level denial for cross-session subscribe, `.req.forwarded`, and forged actor publish.

## Verification

Commands requiring embedded NATS were run outside the default sandbox because the sandbox denies loopback listeners.

Commands run:

```bash
env GOCACHE=/tmp/tether-go-build-cache go test ./cmd/tether ./internal/authcallout ./internal/broker ./internal/cli ./internal/session ./test/p3 -count=1 -v
# PASS before adding the new agent-role risk test

env GOCACHE=/tmp/tether-go-build-cache go test ./test/p3 -count=10
# PASS, 180.669s

env GOCACHE=/tmp/tether-go-build-cache go test ./test/p3 -run 'TestAuthCalloutRejects.*AgentRole' -count=1 -v
# FAIL: wildcard and unprovisioned agent roles accepted

env GOCACHE=/tmp/tether-go-build-cache go test ./... -count=1
# FAIL in test/p3 on TestAuthCalloutRejectsWildcardAgentRole

env GOCACHE=/tmp/tether-go-build-cache go test -cover ./internal/...
# PASS

env GOCACHE=/tmp/tether-go-build-cache make build
# PASS

./bin/tether version
# tether v0.0.0-dev (proto v1)
# linux/amd64
# go1.25.0

make lint
# FAIL: golangci-lint not found. Run: make tools
```

Coverage from `go test -cover ./internal/...`:

```text
internal/agent       89.7%
internal/auth        86.3%
internal/authcallout 72.3%
internal/broker      60.2%
internal/cli         64.6%
internal/node        81.2%
internal/proto      100.0%
internal/session     82.5%
internal/storage     81.8%
```

## Recommendation

Do not start P4 yet.

Close the agent-role bypass first. The smallest safe P3 patch is to deny `tether-agent:*` connection names in auth_callout until P4 has real agent provisioning. If P3 must keep agent permissions active, enforce sid/nid validation and a real authorization check before minting `PermissionsForAgent`.

After the fix, rerun:

```bash
env GOCACHE=/tmp/tether-go-build-cache go test ./test/p3 -run 'TestAuthCalloutRejects.*AgentRole' -count=1 -v
env GOCACHE=/tmp/tether-go-build-cache go test ./test/p3 -count=10
env GOCACHE=/tmp/tether-go-build-cache go test ./... -count=1
```

---

## Maintainer Response (2026-05-08)

### Round-2 finding — Agent role bypass (CRITICAL)

**Status: ACCEPTED.**

Real bug, and exactly the right framing: my round-1 commit silently
granted `PermissionsForAgent(sid, nid)` to anyone who set their CONNECT
name to `tether-agent:<sid>:<nid>`. The previous maintainer note said
"agent unchanged P2 behavior" — that's true for `test/p2` (broker
without auth_callout), but in P3 (with auth_callout) it created a new,
worse path: the role exists in the handler but trusts client-supplied
metadata. `tether-agent:*:*` literally minted wildcard agent rights.
Reviewer's two regression tests prove this.

#### Fix (preferred reviewer option)

Hard-deny `roleAgent` in `internal/authcallout/handler.go`. P4 will
re-enable it once it wires real per-machine agent provisioning
(architecture K.1) + per-session `(sid,nid)` registration backed by a
DB-stored agent identity. Keeping the role recognized at parse time
(but denying at decision time) means the deny path returns a clear
error message instead of "unknown role" — easier to diagnose during
the P4 transition.

This intentionally does NOT break test/p2: `test/p2` boots the broker
with `broker.Config.AuthCallout = nil`, so no auth_callout subscription
is installed and agents continue to connect anonymously.

`internal/authcallout/handler.go` — `roleAgent` branch returns deny
with reason `"agent role not provisioned (P4 will wire agent install
+ auth_callout)"`.

#### Tests

- `test/p3/agent_role_risk_test.go` — kept reviewer's two tests
  verbatim. Both now PASS:
    - `TestAuthCalloutRejectsWildcardAgentRole` — `tether-agent:*:*`
      CONNECT is denied; the test's `if err != nil { return }`
      early-return treats this as success.
    - `TestAuthCalloutRejectsUnprovisionedAgentRole` — same shape for
      `tether-agent:lab:evil` against an existing `lab` session.

- `internal/authcallout/handler_test.go` — replaced the previous
  `TestHandleAgentRoleAllowsWithoutMembership` (which encoded the now-
  rejected behavior) with two same-package regression tests:
    - `TestHandleAgentRoleIsDeniedUntilP4` — straight `lab/lab-1` agent
      role, expect denial with the provisioning-pending message.
    - `TestHandleWildcardAgentRoleDenied` — `tether-agent:*:*`, also
      denied. Defensive even if `parseRole` is ever rerouted.

#### Verification

```bash
go test ./test/p3 -run 'TestAuthCalloutRejects.*AgentRole' -count=1 -v
  PASS  TestAuthCalloutRejectsWildcardAgentRole
  PASS  TestAuthCalloutRejectsUnprovisionedAgentRole

go test ./test/p3 -count=10            -> ok (215.687s, 0 flake / 130 runs)
go test ./... -count=1                 -> all green

go test -cover ./internal/...
  agent       89.7%
  auth        86.3%
  authcallout 72.3%   (new tests replaced the agent-allow test; coverage unchanged)
  broker      60.2%
  cli         64.6%
  node        81.2%
  proto      100.0%
  schema     [no statements]
  session     82.5%
  storage     81.8%
```

#### Notes for P4

When P4 re-enables `roleAgent`, the gate to add is:

  1. `proto.ValidateSID(sid)` and `proto.ValidateNID(nid)` (defense-in-
     depth against `*` and other shapes — even though parseRole's
     `len(parts) != 2` already rejects empty segments).
  2. A DB lookup like `agent.IsProvisioned(db, sid, nid, fp)` confirming
     this nkey was installed for that `(sid,nid)` via `tether agent
     --install` (architecture K.1). Anything else stays denied.

Pinning that as a P4 entry condition.
