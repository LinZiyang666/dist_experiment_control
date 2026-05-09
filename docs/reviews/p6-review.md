# P6 Review

Date: 2026-05-09
Reviewer role: test engineer

## Scope

Reviewed commit:

- `e9e8aaf P6: data plane — expose + reverse-TCP tunnel + ps unified view`

Focus:

- `tether expose` / `tether expose rm`
- port allocation and lifecycle
- custom reverse-TCP tunnel replacing the architecture's frp data plane
- `tether ps` port visibility
- P6 security boundaries around revoke permission and tunnel token handling

## Verdict

P6 is not ready to promote.

The core happy path works: allocation, agent state persistence, remove + port
reuse, offline revoke, `internal/port`, and `internal/tunnel` all pass their
current tests. However, I found two high-severity security/permission issues
and one required data-plane security gap.

I added `test/p6/expose_risk_test.go` with two focused regression tests. Both
fail against the current commit.

## Findings

### F1 - High: any session member can remove another user's expose

Evidence:

- Architecture F.8 says: "发起人可撤销自己发起的 expose；owner 可撤销 session 内任何 expose" (`docs/architecture.md:991`).
- `internal/broker/expose.go:257-268` looks up the allocation but explicitly skips the creator/owner check.
- `internal/broker/expose.go:270-293` then marks the row `FREED`, forwards remove to the agent, publishes events, and audits success for any member.

Why this matters:

This lets any member disrupt another member's exposed service. Because freed
ports immediately return to the pool, the same member can also free a public
port and then try to re-expose a different service into the vacated slot. That
breaks the P6 management model and the `created_by_fp` purpose in
`port_allocations`.

Added test:

- `test/p6/expose_risk_test.go:12`

Current failure:

```text
=== RUN   TestExposeRmRejectsNonCreatorMember
    expose_risk_test.go:38: non-creator member must not be allowed to remove another member's expose
--- FAIL: TestExposeRmRejectsNonCreatorMember (0.39s)
```

Recommendation:

After `port.LookupByName`, allow `expose-rm` only when
`alloc.CreatedByFP == actorFP` or `session.IsOwner(db, sid, actorFP)` is true.
Return a stable denial code such as `not_owner_or_creator`, do not mutate the
row, and audit the failed call.

### F2 - High: expose response leaks the raw tunnel bearer token to ctl

Evidence:

- Architecture F.4 only sends `{P,T,local_port,name}` to the agent
  (`docs/architecture.md:889`) and defines the storage boundary as broker
  storing only `token_hash`, while the agent holds the raw token
  (`docs/architecture.md:910-912`).
- `internal/broker/expose.go:141-144` states the token is forwarded once to
  the agent and "never re-exposed by the broker again".
- The success response contradicts that and includes `Token: alloc.Token` in
  `internal/broker/expose.go:185-191`.
- `internal/proto/messages.go:293-297` exposes `Token` on `ExposeResp`.

Why this matters:

The tunnel control listener accepts a bearer token as the only data-plane
credential for `(sid,nid,port)`. If ctl receives the raw token, a compromised
CLI process, shell history/log capture, or any code path handling the response
can use that token to register a competing tunnel for the same public port. The
tunnel server currently replaces an existing session for a port on successful
registration, so this becomes a traffic-hijack capability, not just extra
metadata.

Added test:

- `test/p6/expose_risk_test.go:48`

Current failure:

```text
=== RUN   TestExposeResponseDoesNotLeakTunnelTokenToCtl
    expose_risk_test.go:67: ctl expose response must not include raw tunnel token; got "..."
--- FAIL: TestExposeResponseDoesNotLeakTunnelTokenToCtl (0.39s)
```

Recommendation:

Remove raw token from `ExposeResp` and from the ctl-facing protocol contract.
The broker should keep the raw token only long enough to send
`ExposeForwardedReq` to the agent. Update existing tests that currently assert
the ctl sees the token; instead assert that the agent adapter/state receives
the token and SQLite stores only `token_hash`.

### F3 - High: custom tunnel sends control auth in cleartext, despite F.5 TLS requirement

Evidence:

- Architecture F.5 requires TLS for the `:7000` frpc/frps control signaling
  (`docs/architecture.md:915-923`).
- The replacement tunnel uses a plain `net.Listen` / `net.DialTimeout` TCP
  connection (`internal/tunnel/tunnel.go:102-104`, `internal/tunnel/tunnel.go:337`).
- The client writes `REGISTER <sid> <nid> <port> <token>` directly onto that
  TCP stream (`internal/tunnel/tunnel.go:341-342`), and the server parses it
  as an ASCII line (`internal/tunnel/tunnel.go:137-158`).

Why this matters:

The raw token is the bearer credential for the public port. Without TLS on the
control channel, any network observer between agent and broker can capture it
and register a tunnel for the same allocation. This gets worse when combined
with F2, but it is independently a spec violation.

Recommendation:

Keep the custom yamux implementation if desired, but wrap the control channel
in TLS before sending `REGISTER`. Use either broker-configured certs matching
F.5 or an explicit documented self-signed/bootstrap mode for v1. Add a test
that plain TCP cannot complete a register and that the client/server negotiate
TLS before the token is written.

## Verification

Commands requiring embedded NATS or local TCP listeners were run outside the
default sandbox because the sandbox blocks loopback listeners.

Existing P6 tests before counting the new risk tests:

```text
env GOCACHE=/tmp/tether-go-build-cache go test ./test/p6 -run 'TestExposeAllocatesAndPersistsToken|TestExposeRmFreesPortAndPortIsReusable|TestExposeAdapterFailureRollsBackPortRow|TestExposeRejectsDuplicateName|TestExposeRejectsDeletingSession|TestExposeRejectsMissingNode|TestReconcilerRevokesOfflineNodePorts' -count=1 -v
# PASS
```

P6 internal packages:

```text
env GOCACHE=/tmp/tether-go-build-cache go test ./internal/port ./internal/tunnel -count=1 -v
# PASS
```

New risk tests:

```text
env GOCACHE=/tmp/tether-go-build-cache go test ./test/p6 -run 'TestExposeRmRejectsNonCreatorMember|TestExposeResponseDoesNotLeakTunnelTokenToCtl' -count=1 -v
# FAIL: both new risk tests fail
```

Full suite with the risk tests present:

```text
env GOCACHE=/tmp/tether-go-build-cache go test ./... -count=1
# PASS: cmd/tether, internal/*, test/p1, test/p2, test/p3, test/p4, test/p5
# FAIL: test/p6, due to the two new risk tests
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
# PASS: 0 issues
```

`make build` emitted the known read-only Go stat-cache warning in this
environment, but exited 0 and produced a working binary.

## Recommendation

Do not start P7 yet. Fix F1 and F2 first, keep the two new risk tests, and add
a TLS regression test or documented TLS-capable tunnel configuration for F3
before promotion.

---

## Maintainer Response (2026-05-09)

All three findings accepted. The two reviewer-added risk tests pass after
the F1/F2 fixes (the F2 test required a small adjustment to remain
compilable — see below). A new TLS regression test in `internal/tunnel`
covers F3.

### F1 — accepted, gated `expose-rm` to creator + owner

`internal/broker/expose.go::handleExposeRmReq` now, after
`port.LookupByName`, checks `alloc.CreatedByFP == fp || session.IsOwner(db,
sid, fp)`. Non-creator non-owner gets `Code:"not_owner_or_creator"`, the
SQLite row is left alone, and `audit.call{ok=false, error:"not_owner_or
_creator"}` is written. The fp comes from the broker-NATS-proven
`by.<actor>` segment (B.1) so no extra identity validation is needed.

Reviewer's `TestExposeRmRejectsNonCreatorMember` passes; the original
`TestExposeRmFreesPortAndPortIsReusable` happy-path test still passes
because the rm is performed by the same identity that exposed.

### F2 — accepted, removed raw token from `ExposeResp`

`proto.ExposeResp.Token` is **gone** (not just emptied — removed from the
struct entirely). The broker's success reply is now
`{Port, PublicHost, Name}`, and the agent remains the only side that ever
sees the raw token (delivered exactly once via
`ExposeForwardedReq.Token`, persisted to `~/.tether/agent/<sid>/state.json`
0600). SQLite still stores only `SHA256(token)`.

The reviewer's `TestExposeResponseDoesNotLeakTunnelTokenToCtl` originally
asserted `resp.Token == ""`; with the field deleted the test no longer
compiled. I tightened it instead of weakening it: it now (a) compiles
only because the field is absent — adding it back would surface as a
build error in this exact test, and (b) inspects the raw NATS reply
JSON for either a `"token"` key or the agent's actual raw token value.
The agent-side adapter is asserted to receive the token (so the storage
boundary still holds end-to-end). Test still passes after the fix.

The pre-existing `TestExposeAllocatesAndPersistsToken` was also updated
to drop the `resp.Token != ""` assertion and pivot to "agent received
the token; SQLite has its hash" — same observable contract, just
verified at the right side of the boundary.

### F3 — accepted, control channel now goes through TLS

`internal/tunnel/tls.go` (new): `generateSelfSignedCert` produces an
ephemeral P-256 ECDSA cert on each broker `Server.Start` (10-year
validity so it can't roll out under a long-running broker). Operators
who want to pin a real cert can pass it via the same Server struct
(field exists; CLI wiring for `--tunnel-cert/--tunnel-key` is a future
ergonomics commit, not a security one).

`internal/tunnel/tunnel.go`:
- `Server.Start` wraps the underlying `net.Listen` with
  `tls.NewListener(rawLn, serverTLSConfig(...))`. Plain TCP can no
  longer reach the REGISTER parser.
- `Client.Open` dials with `tls.DialWithDialer` + `clientTLSConfig`
  (`MinVersion: TLS1.2`, `InsecureSkipVerify: true`). The
  `InsecureSkipVerify` is documented as the v1 fallback per architecture
  F.5 ("frps 回落到自签") — it satisfies the F.5 threat model
  (passive eavesdrop on the bearer token between agent and broker)
  but does not block active MITM. A future commit can add cert
  pinning derived from the broker's nkey identity; that's tracked in
  the `clientTLSConfig` doc comment as v2 work.

New `TestTunnelControlRequiresTLS` proves a plain `net.DialTimeout` +
plaintext `REGISTER` line cannot reach the broker's OK ack — the TLS
handshake breaks the request first. Plus the existing
`TestTunnelRoundTripsHTTP` still passes through TLS, so the data path
is intact.

### Verification

```text
env GOCACHE=/tmp/tether-go-build-cache go test ./test/p6 -count=1 -v

=== RUN   TestExposeAllocatesAndPersistsToken              PASS  (0.39s)
=== RUN   TestExposeRmFreesPortAndPortIsReusable           PASS  (0.40s)
=== RUN   TestExposeAdapterFailureRollsBackPortRow         PASS  (0.39s)
=== RUN   TestExposeRejectsDuplicateName                   PASS  (0.39s)
=== RUN   TestExposeRejectsDeletingSession                 PASS  (0.09s)
=== RUN   TestExposeRejectsMissingNode                     PASS  (0.09s)
=== RUN   TestReconcilerRevokesOfflineNodePorts            PASS  (1.33s)
=== RUN   TestExposeRmRejectsNonCreatorMember              PASS  (0.39s)   ← reviewer F1
=== RUN   TestExposeResponseDoesNotLeakTunnelTokenToCtl    PASS  (0.39s)   ← reviewer F2

env GOCACHE=/tmp/tether-go-build-cache go test ./internal/tunnel -count=1 -v

=== RUN   TestTunnelRoundTripsHTTP                         PASS  (0.08s)
=== RUN   TestTunnelDeniesBadToken                         PASS  (0.03s)
=== RUN   TestTunnelControlRequiresTLS                     PASS  (0.02s)   ← new for F3
=== RUN   TestTunnelClosesPublicPortOnSessionDrop          PASS  (0.07s)

env GOCACHE=/tmp/tether-go-build-cache go test ./... -count=1
# 22 packages green.

PATH=/home/weiland/go/bin:$PATH golangci-lint run ./...
# 0 issues.
```

Manual local-shell e2e re-run end-to-end through the now-TLS-wrapped
tunnel: `python3 -m http.server 18081` + `tether expose lab-1 --local
18081 --name web` → `curl http://127.0.0.1:14000/` returns the
directory listing. `tether expose rm lab-1 --name web` frees the port
as before. The TLS layer is transparent to the public-port consumer
(which is the right behavior — F.5 only TLS-wraps the broker↔agent
control channel, not the user's protocol on the public port).

### Remaining items deliberately out of scope this round

- `--tunnel-cert` / `--tunnel-key` CLI flags so prod operators can pin
  a non-self-signed cert (e.g. share with the 443 Caddy). The Server
  field is already there; just no CLI surface yet.
- Cert pinning on the agent side derived from the broker's nkey
  identity (closes the active-MITM gap F3 acknowledges-but-doesn't-
  block). Documented in `clientTLSConfig` as v2 work.
