# P6 Round 2 Review

Date: 2026-05-09
Reviewer role: test engineer

## Scope

Reviewed commit:

- `3383fdc Address P6 review: expose-rm gate (F1) + ctl-token leak (F2) + TLS (F3)`

Focus:

- P6 review findings in `docs/reviews/p6-review.md`
- `expose-rm` creator/owner authorization
- ctl-facing token exposure
- tunnel control-channel TLS
- P1-P6 regression impact

## Verdict

P6 is approved.

All three P6 review findings are fixed and verified. I did not find a new
P6-blocking issue in this round.

## Verified Fixes

### F1 - non-creator member cannot remove another user's expose

`internal/broker/expose.go:269-286` now allows `expose-rm` only when the
request actor is the original `created_by_fp` or `session.IsOwner(...)` returns
true. A non-creator non-owner receives `not_owner_or_creator`, the allocation
row remains `ALLOCATED`, and the failed attempt is audited.

The reviewer risk test now passes:

```text
=== RUN   TestExposeRmRejectsNonCreatorMember
--- PASS: TestExposeRmRejectsNonCreatorMember (0.42s)
```

### F2 - ctl no longer receives the raw tunnel token

`proto.ExposeResp` no longer has a `Token` field
(`internal/proto/messages.go:293-308`). The broker success response only
contains `port`, `public_host`, and `name` (`internal/broker/expose.go:185-193`).
The raw token still flows once through `ExposeForwardedReq` to the agent, and
SQLite keeps only `token_hash`.

The reviewer risk test now checks the wire JSON and the actual agent-side token
value; it passes:

```text
=== RUN   TestExposeResponseDoesNotLeakTunnelTokenToCtl
--- PASS: TestExposeResponseDoesNotLeakTunnelTokenToCtl (0.39s)
```

### F3 - tunnel control channel is no longer plaintext

`internal/tunnel` now wraps the control listener with TLS
(`internal/tunnel/tunnel.go:105-124`) and the client dials with
`tls.DialWithDialer` before sending `REGISTER` (`internal/tunnel/tunnel.go:354-365`).
The fallback server cert is self-signed P-256 ECDSA (`internal/tunnel/tls.go`).

The new tunnel guard test passes:

```text
=== RUN   TestTunnelControlRequiresTLS
--- PASS: TestTunnelControlRequiresTLS (0.03s)
```

## Verification

Commands requiring embedded NATS or local TCP listeners were run outside the
default sandbox because the sandbox blocks loopback listeners.

```text
env GOCACHE=/tmp/tether-go-build-cache go test ./test/p6 -run 'TestExposeRmRejectsNonCreatorMember|TestExposeResponseDoesNotLeakTunnelTokenToCtl' -count=1 -v
# PASS

env GOCACHE=/tmp/tether-go-build-cache go test ./internal/tunnel -count=1 -v
# PASS, 4 tests

env GOCACHE=/tmp/tether-go-build-cache go test ./internal/port ./internal/broker -count=1
# PASS

env GOCACHE=/tmp/tether-go-build-cache go test ./test/p6 -count=1 -v
# PASS, 9 tests

env GOCACHE=/tmp/tether-go-build-cache go test ./... -count=1
# PASS: cmd/tether, internal/*, test/p1, test/p2, test/p3, test/p4, test/p5, test/p6

PATH=/home/weiland/go/bin:$PATH make lint
# PASS: 0 issues

env GOCACHE=/tmp/tether-go-build-cache make build
# PASS; produced bin/tether

./bin/tether version
# tether v0.0.0-dev (proto v1)
# linux/amd64
# go1.25.0
```

`make build` still emits the known read-only Go stat-cache warning in this
environment, but exits 0 and produces a working binary.

## Residual Notes

- The tunnel TLS client uses `InsecureSkipVerify` with the self-signed fallback
  cert. This closes passive token sniffing, which was the P6 F3 blocker, but it
  does not defend against active MITM. The commit documents this as a v2
  broker-cert/nkey pinning item; I do not consider it a P6 blocker under the
  current F.5 fallback scope.
- The code comment mentions `NewServerWithCert`, but the constructor is not
  exposed yet. That matches the deferred `--tunnel-cert/--tunnel-key` work and
  is not a promotion blocker.

## Recommendation

P6 can be promoted. Keep the two P6 reviewer risk tests and the tunnel TLS
guard test in the suite before starting P7.
