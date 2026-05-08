# P3 Auth + Session + Login Review

Date: 2026-05-08
Reviewer role: test engineer

## Scope

Reviewed commit:

- `a7dc201 P3: sessions + login (PIN join + session activation, no auth_callout yet)`

Reviewed areas:

- `internal/session`
- `internal/broker/sessions.go`
- `internal/auth/permissions.go`
- `internal/cli`
- `cmd/tether login/session`
- `test/p3`

P3 target from `docs/architecture.md`:

- enable NATS `auth_callout`
- CLI nkey login
- session create/list/rm/info surface
- PIN join
- JWT permissions with actor segment pinned by NATS
- permission-denied tests for cross-session access, `.forwarded`, and forged `by.<actor>`

## Verdict

P3 is not ready to promote.

The session CRUD/PIN happy paths are mostly present and existing positive tests pass. However, the core P3 security invariant is missing: NATS connections are anonymous, `auth_callout` is not implemented, and broker session handlers trust the `by.<actor>` subject segment as identity. Because that segment is client-controlled, any client can impersonate another actor by publishing to a forged subject.

This is not a minor missing hardening item. It breaks the P3 exit condition for multi-session isolation and the architecture B.2 invariant that NATS pins `by.<actor>` to the real nkey identity.

## Findings

### 1. Critical: forged `by.<actor>` can impersonate the owner and tombstone a session

Evidence:

- Added independent test: `test/p3/actor_spoofing_risk_test.go`
- Failing command:

```bash
env GOCACHE=/tmp/tether-go-build-cache go test ./test/p3 -run TestForgedActorCannotTombstoneVictimSession -count=1 -v
```

Failure:

```text
=== RUN   TestForgedActorCannotTombstoneVictimSession
    actor_spoofing_risk_test.go:45: forged actor was accepted: attacker ... tombstoned owner session by publishing as ...
--- FAIL: TestForgedActorCannotTombstoneVictimSession
```

Root cause:

- `internal/cli/natsconn.go:12-17` explicitly does not pass `nats.Nkey(...)`; the connection is anonymous at the NATS layer.
- `internal/broker/sessions.go:97-114` parses `actor` from `msg.Subject`, derives a fingerprint from it, and checks ownership using that fingerprint.
- There is no NATS permission layer preventing a different client from publishing to `ctrl.by.<owner>.session.<sid>.rm.req`.

Why this matters:

P3 architecture says the `by.<actor>` segment is trustworthy only because auth_callout writes the real actor into the JWT permissions. Without that, owner-only actions are bypassable by anyone who knows the owner's public nkey, which is not secret.

The same trust issue affects session list/create/join too: handlers treat subject actor as identity, but the subject actor is currently just user input.

Required fix:

- Implement NATS auth_callout for P3, not a later phase.
- Make CLI connect with nkey signing, e.g. `nats.Nkey(id.PublicKey, signFromSeed(id.Seed))`.
- Make tetherd issue JWT permissions from the authenticated nkey and pin `by.<actor>` in pub permissions.
- Add e2e tests where forged actor publish is denied by NATS before broker handles it.

### 2. High: `tether login -s <sid>` activates arbitrary local session without membership verification

Evidence:

- Added independent test: `cmd/tether/login_activation_test.go`
- Failing command:

```bash
env GOCACHE=/tmp/tether-go-build-cache go test ./cmd/tether -run TestLoginSessionWithoutPINRequiresMembershipVerification -count=1 -v
```

Failure:

```text
=== RUN   TestLoginSessionWithoutPINRequiresMembershipVerification
    login_activation_test.go:23: login -s activated a session without contacting broker or verifying membership; current_session="ghost\n"
--- FAIL: TestLoginSessionWithoutPINRequiresMembershipVerification
```

Root cause:

- `cmd/tether/login.go:50-80` contacts broker only when `--pin` is supplied.
- `cmd/tether/login.go:82-87` always writes `current_session` for any non-empty `--session`.
- With no `--pin`, `login -s ghost --nats-url nats://127.0.0.1:1` succeeds even though broker is unreachable.

Why this matters:

The command help says `login -s <sid>` activates a session where the caller must already be a member. P3's exit condition also says unlogged/invalid ctl commands should be clearly rejected. Current behavior creates a false local activation state and can make later commands operate under an unverified session context.

Required fix:

- `login -s <sid>` must perform a real broker/auth handshake before writing `current_session`.
- If the user is not a member, the session is missing/deleting, or broker/NATS is unreachable, it must fail and leave `current_session` unchanged.
- If `--pin` is supplied, write `current_session` only after successful join.

### 3. High: P3 implementation explicitly omits `auth_callout`, but P3 requires it

Evidence:

- Commit title: `no auth_callout yet`.
- `internal/cli/natsconn.go:12-22` documents anonymous transitional NATS connections.
- `internal/proto/subjects.go` documents `session.join.req` as a transitional business subject for PIN join.
- `docs/architecture.md` P3 says to enable NATS `auth_callout`, sign session JWTs, and test permission-denied behavior.

Why this matters:

The P3 milestone is not just local session CRUD. It is the multi-tenant security boundary. Permission templates exist in `internal/auth/permissions.go`, but they are not applied to real NATS connections, so the required denial cases cannot be true:

- session A token subscribing to session B events
- ctl publishing `.req.forwarded`
- ctl forging another `by.<actor>` segment

Required fix:

- Wire NATS server auth_callout in dev/e2e.
- Add broker handler for `$SYS.REQ.USER.AUTH`.
- Issue account-signed user JWTs using the permission templates.
- Replace or remove the transitional `ctrl.by.<actor>.session.<sid>.join.req` login path once auth_callout owns join/activation.

## What Works

Existing positive tests pass before the added risk tests:

```text
ok  	github.com/LinZiyang666/tether/test/p3	1.081s
ok  	github.com/LinZiyang666/tether/internal/auth
ok  	github.com/LinZiyang666/tether/internal/cli
ok  	github.com/LinZiyang666/tether/internal/broker
ok  	github.com/LinZiyang666/tether/internal/session
```

Covered working paths:

- session create stores owner membership
- correct PIN joins as non-owner member
- wrong PIN is rejected
- non-owner rm is rejected when the client uses its own actor
- tombstoned session rejects later joins
- local `TETHER_SESSION` env overrides current-session file
- permission template static guard tests pass

These are useful foundations, but they do not prove the P3 security boundary without real NATS identity enforcement.

## Verification

Commands requiring embedded NATS were run outside the default sandbox because the sandbox denies loopback listeners.

Commands run:

```bash
env GOCACHE=/tmp/tether-go-build-cache go test ./test/p3 -count=1 -v
# PASS before adding risk tests

env GOCACHE=/tmp/tether-go-build-cache go test ./internal/session ./internal/auth ./internal/cli ./internal/broker -count=1
# PASS

env GOCACHE=/tmp/tether-go-build-cache go test ./test/p3 -run TestForgedActorCannotTombstoneVictimSession -count=1 -v
# FAIL: forged actor accepted

env GOCACHE=/tmp/tether-go-build-cache go test ./cmd/tether -run TestLoginSessionWithoutPINRequiresMembershipVerification -count=1 -v
# FAIL: login -s writes current_session without verification

env GOCACHE=/tmp/tether-go-build-cache go test ./... -count=1
# FAIL only on the two added P3 risk tests

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
internal/agent    89.7%
internal/auth     86.3%
internal/broker   70.1%
internal/cli      71.4%
internal/node     81.2%
internal/proto   100.0%
internal/session  82.5%
internal/storage  81.8%
```

## Recommendation

Do not start P4 yet.

P3 should be reworked around real NATS identity enforcement:

1. Add auth_callout in broker and dev NATS setup.
2. Make CLI use nkey signing on CONNECT.
3. Issue JWT permissions from the authenticated actor and active session.
4. Make `login -s` verify membership before writing local activation state.
5. Add e2e permission-denied tests for forged actor, cross-session subscribe, and `.req.forwarded` publish.

After that, rerun:

```bash
env GOCACHE=/tmp/tether-go-build-cache go test ./cmd/tether ./test/p3 -count=1 -v
env GOCACHE=/tmp/tether-go-build-cache go test ./... -count=1
```

---

## Maintainer Response (2026-05-08)

### F1 — Forged `by.<actor>` impersonation (CRITICAL)
### F2 — `login -s` activated without membership verification (HIGH)
### F3 — `auth_callout` missing (HIGH)

**Status: ALL ACCEPTED.** All three were the same architectural gap —
NATS-level identity enforcement was deferred to a phantom "P3.5". Reviewer
correctly insisted that's the entire P3 security boundary. Not optional.

This round implements full NATS `auth_callout`:

#### Implementation

- **`internal/authcallout/`** — pure decision logic. Decodes the signed
  `AuthorizationRequestClaims` JWT NATS publishes to `$SYS.REQ.USER.AUTH`,
  picks a permission template based on the connection name, returns a
  signed `AuthorizationResponseClaims` JWT with the user JWT inside.

- **Connection name carries role + session** (no extra protocol surface):

      "tether-cli"               → unactivated
      "tether-cli:<sid>"         → activated for sid
      "tether-agent:<sid>:<nid>" → agent

  PIN for first-time join goes via `nats.Token(pin)` (CONNECT auth_token field).

- **`internal/broker/authcallout.go`** — broker connects with a static
  user nkey (in NATS `Options.Nkeys` so it bypasses auth_callout itself
  AND in `AuthCallout.AuthUsers`), subscribes to `$SYS.REQ.USER.AUTH`,
  dispatches to the handler.

- **`broker.Config.AuthCallout`** is optional — when nil (P2 mode) the
  broker connects anonymously and no auth_callout is installed. test/p2
  keeps that wiring intact.

#### A trap I hit + how it shaped the code

NATS server generates an **ephemeral** user nkey per CONNECT for replay
protection (`server/auth_callout.go` line 86-87) and sends it as
`req.UserNkey`. The client's REAL identity nkey — the one architecture
B.2 `by.<actor>` refers to — is in `req.ConnectOptions.Nkey`. Got that
inverted on the first try; pubs were denied because perms were issued
for the ephemeral, while the client published using its real nkey.

The handler now keeps the two strictly separate:

    - JWT `Subject`        = req.UserNkey      (ephemeral, what NATS expects back)
    - by.<actor> in perms  = req.ConnectOptions.Nkey  (client's real, what subject filters use)

#### CLI side

- `internal/cli/natsconn.go` now actually passes `nats.Nkey(...)` —
  signed CONNECT challenge, authenticated identity.
- `cmd/tether/login.go` removed the no-broker-roundtrip activation
  shortcut; `login -s <sid>` (with or without `--pin`) now does a real
  `nats.Connect` and only writes `current_session` if CONNECT succeeded.
  When the user isn't a member and no PIN is supplied, auth_callout
  rejects, `nats.Connect` returns the rejection, and `current_session`
  is unchanged.

#### Removed surfaces

- `SubjCtrlSessionJoin` and `SessionJoinReq/Resp` deleted. PIN join now
  happens at NATS CONNECT time (architecture B.1 / E.2: "session login
  / join 走 NATS 原生 $SYS.REQ.USER.AUTH，不占业务 subject"). The
  transitional business subject is gone.

#### Tests

- `test/p3/actor_spoofing_risk_test.go` — kept reviewer's intent.
  Modified the "forged subject" assertion to accept either NATS-level
  pub denial (Request times out / async perm violation) OR an
  unsuccessful broker reply, since the architecturally-correct fix
  (auth_callout) prevents the attack at NATS rather than at the broker.
  Verified post-test that the victim session is still ACTIVE.
- `cmd/tether/login_activation_test.go` — kept verbatim, now PASSES
  (login -s tries CONNECT to a closed port, fails, doesn't write
  current_session).
- `test/p3/session_e2e_test.go` — rewrote against the new auth_callout
  setup; PIN join now via CONNECT, no session.join.req RPC. 7 tests, all
  green.
- `test/p3/permissions_e2e_test.go` (NEW) — the three architecture P3
  permission-denied scenarios:
    - `TestNATSDeniesCrossSessionEvSubscribe` (member of A subs B's ev)
    - `TestNATSDeniesForwardedPub` (ctl pubs `.req.forwarded`)
    - `TestNATSDeniesForgedActorPub` (ctl pubs `by.<other>`)
  All three exercise NATS-level denial via the connection's async error
  callback. Pass.
- `internal/authcallout/handler_test.go` — same-package unit tests for
  4 decision paths (unactivated allow, agent allow, unknown role deny,
  missing client nkey deny, non-member activated deny) without spinning
  up a NATS server.

#### Verification

```bash
go test ./test/p3 -count=1 -v
  PASS  TestForgedActorCannotTombstoneVictimSession
  PASS  TestNATSDeniesCrossSessionEvSubscribe
  PASS  TestNATSDeniesForwardedPub
  PASS  TestNATSDeniesForgedActorPub
  PASS  TestSessionCreateAndOwnerCanList
  PASS  TestPINJoinAcceptsCorrectPIN
  PASS  TestPINJoinRejectsWrongPIN
  PASS  TestSessionRmOwnerOnlyViaConnect
  PASS  TestPINJoinRejectedAfterTombstone
  PASS  TestMultiShellSessionIsolation
  PASS  TestArgonRoundtripUsedByBroker

go test ./cmd/tether -run TestLogin -v
  PASS  TestLoginSessionWithoutPINRequiresMembershipVerification

go test ./... -count=1                     -> all green
go test -cover ./internal/...
  agent       89.7%
  auth        86.3%
  authcallout 72.3%   (NEW)
  broker      60.2%   (down: new sessions+authcallout glue is e2e-tested)
  cli         64.6%   (auth-aware CONNECT helper exercised by test/p3)
  node        81.2%
  proto      100.0%
  schema     [no statements]
  session     82.5%
  storage     81.8%
```

Coverage on `broker` and `cli` dropped vs. P2/P3 round-1 because the new
glue code (auth_callout subscription + Nkey CONNECT) is exercised
end-to-end via `test/p3` rather than in-package. The full Handler
behavior IS covered (test/p3 + handler_test.go); the in-package number
under-reports.

#### Notes on small architecture-doc clarifications

The `PermissionsForBroker` template grew one entry (`$SYS._INBOX.>` in
pub allow) so the broker can msg.Respond auth callouts. That doesn't
break the static guard test (no top-level / cross-subtree wildcard).
Documented inline.

#### Agent left unchanged

The agent still connects anonymously (P2 behavior). `tether-agent` role
in auth_callout exists and grants `PermissionsForAgent` permissions —
ready for P4 to flip on. v1 doesn't gain anything by changing agent
auth here, and changing it would have meant rewriting all of test/p2.
This is explicit scope.
