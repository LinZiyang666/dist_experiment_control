# P4 Review

Date: 2026-05-08
Reviewer role: test engineer

## Scope

Reviewed commit:

- `6c036c7 P4: control plane — exec (non-interactive) + ps + audit`

Focus:

- P4 `exec` / `ps` control plane behavior
- `.req.forwarded` broker-to-agent routing boundary
- process lifecycle persistence and audit attribution
- P3 `auth_callout` security boundary carried into P4

## Verdict

P4 is not ready to promote.

The happy path works in the anonymous P2-style harness: `exec echo`, non-zero
exit codes, stderr capture, and basic `ps` all pass. The blocking issue is that
the delivered P4 path is not wired into the secure P3 `auth_callout` model. To
make P4 exec work today, the system must run with anonymous agent connections
and effectively bypass the NATS identity boundary that P3 established.

I added independent P4 risk tests in `test/p4/exec_risk_test.go`. They expose
three additional high-value failures around missing nodes, tombstoned sessions,
and process actor attribution.

## Findings

### F1 - Critical: P4 exec only works without the secure `auth_callout` agent path

Evidence:

- `README.md:8-30` marks P4 complete but still says the agent role in
  `auth_callout` is hard-denied until P4 ships real agent provisioning.
- `internal/authcallout/handler.go:115-131` still rejects every
  `tether-agent:<sid>:<nid>` connection name with `agent role not provisioned`.
- `internal/agent/agent.go:10-15` documents that the agent connects anonymously
  and only works when `broker.Config.AuthCallout=nil`.
- `internal/agent/agent.go:140-152` actually connects with no nkey credentials
  and with name `tether-agent/<sid>/<nid>`, which does not match the
  `tether-agent:<sid>:<nid>` role format.
- `cmd/tether/serve.go:33-37` constructs `broker.Config` without any
  `AuthCallout` config, so the real daemon command cannot enable the P3
  auth boundary.
- `test/p4/exec_e2e_test.go:6-9` explicitly disables `auth_callout` in the P4
  e2e suite.

Why this matters:

P4's architecture depends on NATS permissions to keep ctl from publishing
directly to `.req.forwarded` and to pin `by.<actor>` to the authenticated nkey.
With `auth_callout` enabled, the current agent cannot connect. With it disabled,
the control plane runs, but the P3 identity and subject permission guarantees
are no longer enforced.

Verification:

```text
env GOCACHE=/tmp/tether-go-build-cache go test ./test/p3 -run 'TestAuthCalloutRejects.*AgentRole' -count=1 -v

=== RUN   TestAuthCalloutRejectsWildcardAgentRole
--- PASS: TestAuthCalloutRejectsWildcardAgentRole (2.03s)
=== RUN   TestAuthCalloutRejectsUnprovisionedAgentRole
--- PASS: TestAuthCalloutRejectsUnprovisionedAgentRole (2.03s)
PASS
```

Recommendation:

Implement the P4 agent provisioning gate before promoting P4:

- give each installed agent a real nkey identity;
- make `agent.connectNATS` use `nats.Nkey(...)` and `cli.AgentName(sid, nid)`;
- add DB-backed proof that the presented nkey is provisioned for `(sid,nid)`;
- validate `sid` and `nid` before minting permissions;
- only then re-enable `roleAgent` in `auth_callout`;
- add a P4 e2e with `auth_callout` enabled where ctl exec works through the
  broker, ctl direct `.req.forwarded` publish is denied, and an unprovisioned
  agent is denied.

### F2 - High: exec to a missing/offline node hangs until ctl timeout

Evidence:

- `internal/broker/exec.go:42-61` checks session active and membership.
- `internal/broker/exec.go:63-79` then forwards to
  `s.<sid>.cmd.node.<nid>.exec.req.forwarded` without checking that the target
  node exists or is online.

Added test:

- `test/p4/exec_risk_test.go::TestExecMissingNodeReturnsBrokerError`

Failure:

```text
exec against a missing node timed out instead of returning a broker error chunk: nats: timeout
```

Why this matters:

The broker already promises in `handleExecReq` comments that pre-forward
failures return `ExecChunk{kind:error}` instead of a NATS timeout. A typoed node
name or offline agent is a normal operator path, not a stream-level hang.

Recommendation:

Before forwarding, check the node table for `(sid,nid)` and require an online
state / fresh heartbeat. Return an error chunk such as `node_not_found` or
`node_offline`, and write `audit.call` with `ok=false`.

### F3 - High: `ps` does not reject a DELETING session

Evidence:

- `internal/broker/exec.go:134-161` verifies actor and membership for `ps`.
- Unlike `handleExecReq`, it never calls `session.IsActive`, so tombstoned
  sessions are still queryable by old members.

Added test:

- `test/p4/exec_risk_test.go::TestPsRejectsDeletingSession`

Failure:

```text
ps on a DELETING session should be rejected, got success with []
```

Why this matters:

P3 explicitly made ACTIVE -> DELETING tombstone a hard ingress rejection point.
P4 adds a new session-scoped ingress subject and must apply the same gate.

Recommendation:

Add the same `session.IsActive` precheck used by exec before membership lookup
in `handlePsReq`, and return `session_not_found_or_deleting`.

### F4 - Medium: process rows lose the ctl actor fingerprint

Evidence:

- `internal/agent/exec.go:161-169` publishes `ProcStartedEvent` with
  `StartedByFP` empty.
- `internal/broker/exec.go:107-112` inserts `StartedByFP: ev.StartedByFP`,
  so the empty value is persisted.

Added test:

- `test/p4/exec_risk_test.go::TestExecRecordsStartedByFingerprint`

Failure:

```text
process started_by_fp mismatch: got "" want "SHA256:..."
```

Why this matters:

`internal/proc` exposes `StartedByFP`, and `PsEntry` returns it. Leaving it blank
breaks auditability: after the process event is stored, the DB row no longer
answers who started it.

Recommendation:

Do not trust the agent to supply the actor. The broker should generate a request
ID when forwarding, remember `(sid,nid,req_id) -> actor_fp`, have the agent echo
that request ID in proc events, and fill `StartedByFP` from the broker-side
pending request map. Another viable shape is broker-assigned PID before
forwarding, but the important part is that actor attribution must come from the
broker-parsed `by.<actor>` subject, not from agent-supplied data.

## Verification

Commands requiring embedded NATS were run outside the default sandbox because
the sandbox denies loopback listeners.

Baseline P4 happy path before adding risk tests:

```text
env GOCACHE=/tmp/tether-go-build-cache go test ./test/p4 -count=1 -v

PASS
ok  	github.com/LinZiyang666/tether/test/p4	2.047s
```

Related packages:

```text
env GOCACHE=/tmp/tether-go-build-cache go test ./internal/proc ./internal/broker ./internal/agent -count=1

ok  	github.com/LinZiyang666/tether/internal/proc	0.020s
ok  	github.com/LinZiyang666/tether/internal/broker	0.982s
ok  	github.com/LinZiyang666/tether/internal/agent	0.780s
```

New risk tests:

```text
env GOCACHE=/tmp/tether-go-build-cache go test ./test/p4 -run 'TestExecMissingNodeReturnsBrokerError|TestPsRejectsDeletingSession|TestExecRecordsStartedByFingerprint' -count=1 -v

FAIL TestExecMissingNodeReturnsBrokerError
FAIL TestPsRejectsDeletingSession
FAIL TestExecRecordsStartedByFingerprint
```

Full suite with risk tests present:

```text
env GOCACHE=/tmp/tether-go-build-cache go test ./... -count=1

ok  	github.com/LinZiyang666/tether/cmd/tether	0.009s
ok  	github.com/LinZiyang666/tether/internal/agent	0.774s
ok  	github.com/LinZiyang666/tether/internal/auth	1.093s
ok  	github.com/LinZiyang666/tether/internal/authcallout	0.033s
ok  	github.com/LinZiyang666/tether/internal/broker	1.063s
ok  	github.com/LinZiyang666/tether/internal/cli	0.013s
ok  	github.com/LinZiyang666/tether/internal/node	0.023s
ok  	github.com/LinZiyang666/tether/internal/proc	0.028s
ok  	github.com/LinZiyang666/tether/internal/proto	0.003s
ok  	github.com/LinZiyang666/tether/internal/schema	0.003s
ok  	github.com/LinZiyang666/tether/internal/session	0.030s
ok  	github.com/LinZiyang666/tether/internal/storage	0.024s
ok  	github.com/LinZiyang666/tether/test/p1	0.049s
ok  	github.com/LinZiyang666/tether/test/p2	1.443s
ok  	github.com/LinZiyang666/tether/test/p3	20.728s
FAIL	github.com/LinZiyang666/tether/test/p4	3.161s
```

## Recommendation

Do not start P5 yet.

Fix F1 first because it decides whether P4 runs inside the P3 security model or
only in anonymous demo mode. Then keep the three added risk tests as regression
tests and close F2-F4 before promotion.

---

## Maintainer Response (2026-05-08)

All four findings accepted. The three reviewer-added risk tests now pass
unmodified; the architecture-mandated "ctl bypass `.req.forwarded` → permission
denied" was already covered in `test/p3/permissions_e2e_test.go:64-101` (P3
exit criterion line 1976) so I did not duplicate it in `test/p4`.

### F1 — Critical: agent role re-enabled in `auth_callout`

Implemented K.1-shape agent provisioning end-to-end:

- New migration `0002_agent_provisioning.sql` adds `agent_provisioning(sid,
  nid, agent_fp, joined_at)` (PK `(sid, nid)`, FK `sid → sessions ON DELETE
  CASCADE`).
- New package `internal/agentprov` with `Lookup` / `Provision` /
  `ProvisionWithPIN` (verify-callback pattern, mirrors `session.JoinWithPIN`).
- `internal/authcallout/handler.go::Handle` `roleAgent` branch no longer
  hard-denies. Decision tree (`ensureAgentProvisioned`):
    - validate sid/nid + nkey strictly (P3-round-2 lesson),
    - existing binding with matching fp → allow (re-validates session ACTIVE),
    - existing binding with different fp → deny `nid ... bound to a different
      agent identity`,
    - no binding + no PIN → deny `not provisioned; first connect must supply
      --pin`,
    - no binding + valid PIN → `ProvisionWithPIN` writes the row, allow.
- `internal/agent/agent.go` connects with `nats.Nkey` + Name
  `cli.AgentName(sid, nid) = "tether-agent:<sid>:<nid>"` + optional
  `nats.Token(pin)`. The pre-fix `tether-agent/<sid>/<nid>` mismatch the
  reviewer caught is gone. Auth failures are detected (`Authorization
  Violation` substring) and surfaced fail-fast instead of flapping forever.
- `cli.EnsureAgentIdentity(home, sid)` lays the agent nkey at
  `~/.tether/agent/<sid>/keys/agent.nk` per K.1 (0700 dir / 0600 file via
  the existing `generateAndPersist` helper).
- `cmd/tether/agent.go` adds `--pin` flag (only required first time);
  `TETHER_DEV_NO_AUTH=1` skips the nkey path for laptop demos.
- `cmd/tether/serve.go` adds `--auth-callout-seeds-dir` flag (loads
  `broker.nk` + `account.nk`); without it the broker keeps the dev/P2
  default. The reviewer's exact complaint "the real daemon command cannot
  enable the P3 auth boundary" is closed.

New unit + e2e coverage:

- `internal/agentprov/agentprov_test.go` — five tests pinning
  Lookup/Provision/idempotency/conflict/cascade behavior.
- `internal/authcallout/handler_test.go` replaces
  `TestHandleAgentRoleIsDeniedUntilP4` with three specific tests:
  `TestHandleAgentRoleDeniedWithoutProvisioningAndPIN`,
  `TestHandleAgentRolePINBootstrapAndRebind`,
  `TestHandleAgentRoleRejectsHijack`.
- `test/p4/exec_authcallout_test.go` (NEW) — full secure-stack exec/ps
  e2e: `TestExecHappyPathThroughAuthCallout`,
  `TestAgentReconnectsWithoutPINAfterBootstrap`,
  `TestAgentWithoutPINIsRejectedAtConnect`. The existing anonymous-mode
  `exec_e2e_test.go` is kept verbatim — it's now narrowly focused on
  app-layer logic without JWT scaffolding noise. The two suites together
  prove both layers.

### F2 — High: missing/offline node returns broker error chunk

`internal/node.LookupStatus(db, sid, nid)` (new) plus a precheck in
`broker.handleExecReq` between membership and forward: missing row returns
`ExecChunk{kind:error,Error:"node_not_found"}`, non-`ONLINE` row returns
`node_offline:<status>`. Both also write `audit.call{ok=false}` so the
operator sees the rejection in audit. Reviewer's `TestExecMissingNodeReturnsBrokerError` passes.

### F3 — High: `ps` rejects DELETING session

`broker.handlePsReq` now applies the same `session.IsActive` precheck used
by exec, returning `Code:"session_not_found_or_deleting"`. Reviewer's
`TestPsRejectsDeletingSession` passes.

### F4 — Medium: actor attribution originates at the broker

Accepted the issue, used a simpler mechanism than the reviewer's pending-map
suggestion: `proto.ExecReq` gains an `ActorFP` field that is broker-stamped
at forward time (`broker.handleExecReq` re-marshals the body with the
broker-parsed `by.<actor>` fingerprint after auth checks pass; whatever
ctl supplies is discarded). `agent.handleExecForwarded` reads it and
echoes into `ProcStartedEvent.StartedByFP`. The agent never invents the
value — it only replays what the broker stamped — which satisfies "actor
attribution must come from the broker-parsed `by.<actor>` subject, not
from agent-supplied data" without a per-request memory map.

Tradeoff: a malicious agent could still publish fake `ev.proc.started`
on subjects unrelated to a real exec, and the StartedByFP would be
whatever it cooks up. This is an acceptable limit for v1: the agent is
the operator's own machine (architecture K.1) and is in the trust
envelope; if it cannot be trusted, the entire control plane is
compromised regardless of attribution mechanism. The pending-map design
would not raise this floor — only make spoofing slightly louder.

Reviewer's `TestExecRecordsStartedByFingerprint` passes.

### Verification

```text
env GOCACHE=/tmp/tether-go-build-cache go test ./... -count=1

ok  	github.com/LinZiyang666/tether/cmd/tether
ok  	github.com/LinZiyang666/tether/internal/agent
ok  	github.com/LinZiyang666/tether/internal/agentprov          ← new
ok  	github.com/LinZiyang666/tether/internal/auth
ok  	github.com/LinZiyang666/tether/internal/authcallout
ok  	github.com/LinZiyang666/tether/internal/broker
ok  	github.com/LinZiyang666/tether/internal/cli
ok  	github.com/LinZiyang666/tether/internal/node
ok  	github.com/LinZiyang666/tether/internal/proc
ok  	github.com/LinZiyang666/tether/internal/proto
ok  	github.com/LinZiyang666/tether/internal/schema
ok  	github.com/LinZiyang666/tether/internal/session
ok  	github.com/LinZiyang666/tether/internal/storage
ok  	github.com/LinZiyang666/tether/test/p1
ok  	github.com/LinZiyang666/tether/test/p2
ok  	github.com/LinZiyang666/tether/test/p3   (22.6s — auth_callout suite)
ok  	github.com/LinZiyang666/tether/test/p4   ( 9.0s — 12 tests)

env GOCACHE=/tmp/tether-go-build-cache golangci-lint run ./...
0 issues.
```

Manual local-shell check (dev mode, `TETHER_DEV_NO_AUTH=1`) — session
create / agent / `exec echo` / `exec sh -c "exit 9"` (exit propagated) /
`ps -a` (table with both rows) all green. The auth_callout secure path
is now exercised by `test/p4/exec_authcallout_test.go` end-to-end (real
nats-server + real auth_callout + real PIN-bootstrap + real exec).

### Items deliberately out of scope this round

- A `tether agent revoke <sid> <nid>` admin tool (operator-side undo of
  an agent_provisioning row when the agent fp must be replaced). The
  data model supports it (DELETE FROM agent_provisioning WHERE …); just
  no CLI surface yet.
- Persisting the agent's "have I bootstrapped already?" hint to
  `~/.tether/agent/<sid>/state.json` per K.1 / I.2 — reduces the
  PIN-supplied-twice-by-accident error noise but isn't a correctness
  gap. P8 reconciliation will read/write this state.json anyway.
- Production install.sh that auto-runs `tether agent --pin` on first
  install (K.1 line 1723) — distribution territory, P10.
