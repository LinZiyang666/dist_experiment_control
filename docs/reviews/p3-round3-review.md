# P3 Round 3 Review

Date: 2026-05-08
Reviewer role: test engineer

## Scope

Reviewed commit:

- `a81427b Address P3 round-2 review: hard-deny agent role in auth_callout until P4`

Focus:

- Round-2 critical agent-role bypass
- P3 auth_callout and JWT permission enforcement
- P3 session/login e2e
- P2 regression, because P3 intentionally leaves P2 anonymous agent flow intact when `AuthCallout=nil`

## Verdict

P3 is approved.

The round-2 blocker is fixed: auth_callout now hard-denies `tether-agent:*` connection names until P4 wires real agent provisioning. The two reviewer risk tests for wildcard and unprovisioned agent roles both pass.

No new P3-blocking issue was found in this round.

## Verified Fixes

### 1. Agent role is denied in auth_callout until P4

Code path:

- `internal/authcallout/handler.go` keeps parsing `roleAgent`, but the branch now returns an auth denial instead of minting `PermissionsForAgent`.
- `internal/authcallout/handler_test.go` covers both ordinary agent names and wildcard agent names.
- `test/p3/agent_role_risk_test.go` covers the end-to-end NATS auth_callout behavior.

Verification:

```text
=== RUN   TestAuthCalloutRejectsWildcardAgentRole
--- PASS: TestAuthCalloutRejectsWildcardAgentRole (2.03s)
=== RUN   TestAuthCalloutRejectsUnprovisionedAgentRole
--- PASS: TestAuthCalloutRejectsUnprovisionedAgentRole (2.03s)
PASS
ok  	github.com/LinZiyang666/tether/test/p3	4.070s
```

```text
=== RUN   TestHandleAgentRoleIsDeniedUntilP4
--- PASS: TestHandleAgentRoleIsDeniedUntilP4 (0.00s)
=== RUN   TestHandleWildcardAgentRoleDenied
--- PASS: TestHandleWildcardAgentRoleDenied (0.00s)
PASS
ok  	github.com/LinZiyang666/tether/internal/authcallout	0.009s
```

This is the right P3 boundary. Agent auth can land in P4, but P3 must not grant agent privileges based only on a client-controlled connection name.

### 2. Previous P3 findings remain fixed

The earlier CLI/session security findings remain covered:

- forged `by.<actor>` session rm is denied at NATS permission layer
- `login -s` performs real CONNECT before writing `current_session`
- cross-session event subscribe is denied
- `.req.forwarded` publish from ctl is denied
- forged actor publish is denied
- transitional `session.join.req` surface is gone

The P3 architecture test list is now represented in `test/p3` and `cmd/tether`.

## Verification

Commands requiring embedded NATS were run outside the default sandbox because the sandbox denies loopback listeners.

Commands run:

```bash
env GOCACHE=/tmp/tether-go-build-cache go test ./test/p3 -run 'TestAuthCalloutRejects.*AgentRole' -count=1 -v
# PASS

env GOCACHE=/tmp/tether-go-build-cache go test ./internal/authcallout -run 'TestHandle.*AgentRole|TestHandleWildcardAgentRoleDenied' -count=1 -v
# PASS

env GOCACHE=/tmp/tether-go-build-cache go test ./cmd/tether ./internal/authcallout ./internal/broker ./internal/cli ./internal/session ./test/p3 -count=1
# PASS

env GOCACHE=/tmp/tether-go-build-cache go test ./test/p2 -count=5
# PASS

env GOCACHE=/tmp/tether-go-build-cache go test ./test/p3 -count=10
# PASS, 216.759s

env GOCACHE=/tmp/tether-go-build-cache go test ./... -count=1
# PASS

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

## Non-blocking Notes

### P4 agent auth entry condition

When P4 re-enables `roleAgent`, it must not simply restore `PermissionsForAgent(sid,nid)` from the CONNECT name. The gate should include:

- `proto.ValidateSID(sid)`
- `proto.ValidateNID(nid)`
- DB-backed proof that the presented nkey is provisioned for `(sid,nid)`, e.g. `agent.IsProvisioned(db, sid, nid, fp)`

The two tests in `test/p3/agent_role_risk_test.go` should stay as regression tests and should only pass after P4 supplies real provisioning checks.

### Lint environment

`make lint` still cannot run in this environment because `golangci-lint` is not installed. This is an environment/tooling gap, not a P3 code failure.

## Recommendation

P3 can be promoted. Proceed to P4 with the agent provisioning gate above as an explicit entry condition.

---

## Maintainer Response (2026-05-08)

**ACK — P3 放行确认。**

无新 finding 需处理。两条入仓约束按 reviewer 要求保留：

1. `test/p3/agent_role_risk_test.go` 是**永久回归测试**：当 P4 重新启用
   `roleAgent` 时，这两个测试只有在加上 `proto.ValidateSID/NID` + 数据库
   provision 校验之后才能继续通过。它们是 P4 entry gate 的执行式说明书。

2. P4 agent auth entry condition（写进 backlog）：
   - `proto.ValidateSID(sid)` + `proto.ValidateNID(nid)`（防御层）
   - `agent.IsProvisioned(db, sid, nid, fp)`（数据库证明 nkey 经
     `tether agent --install` 配置过）
   两条都过才能放行 agent CONNECT。

下一步是先做一轮全代码质量审查（把多次 review 修补留下的痕迹清掉），之后才进 P4。
