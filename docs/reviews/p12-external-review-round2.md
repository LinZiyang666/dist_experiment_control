# P12 External Review Round 2

**RESULT: PASS**

Date: 2026-06-09
Reviewer role: external reviewer

## Scope

Re-reviewed the maintainer response and fixes for:

- F1: architecture SSOT did not define requested remote ports;
- F2: CLI `--remote-port` wire-through was not tested;
- the three follow-up suggestions in `p12-external-review.md`;
- P12-focused build, test, race, vet, and lint gates.

## Verdict

P12 is approved.

Both round-1 findings are fixed and verified. I found no new P12-blocking
issue in this round.

## Verified Fixes

### F1 - architecture now defines both allocation modes

`docs/architecture.md:867-878` now makes `ExposeReq.remote_port` the mode
selector and records the full contract:

- omitted/zero selects the lowest free port;
- nonzero requests the exact in-band port;
- out-of-band requests return `port_out_of_band`;
- an active allocation returns `port_taken` with no automatic fallback;
- REVOKED/FREED history does not block reuse;
- the partial unique index is the concurrency arbiter;
- same-proto new-ctl/old-broker silent downgrade is an accepted limitation.

The F.4 flow, F.8 command table, and P6 milestone text also show
`--remote-port`, so the authoritative architecture and user manual no longer
contradict the implementation.

### F2 - the real CLI request body is now covered

`cmd/tether/expose_remote_port_test.go:78-154` runs `newExposeCmd` against an
embedded NATS responder and captures the actual published body. It verifies:

- `--remote-port 14005` sends `"remote_port":14005`;
- omitting the flag sends no `remote_port` key.

This test exercises the Cobra flag-to-`ExposeReq` path rather than constructing
the protocol struct directly. Removing `RemotePort: remotePort` from
`cmd/tether/expose.go` would now fail the test.

`test/p6/expose_remote_port_e2e_test.go:138-163` also verifies that
out-of-band requests never reach the agent adapter.

## Follow-up Review

- The capability-negotiation suggestion was reasonably deferred. The silent
  same-proto downgrade is now explicit in both architecture and usage docs.
- A live public-listener data-plane test remains useful but is not required
  for this additive control-plane allocation change.
- The central protocol roundtrip catalogue now populates
  `ExposeReq.RemotePort`, covering the nonzero field.

## Verification

```text
go test ./cmd/tether \
  -run 'TestExposeRemotePortWireThrough|TestExposeRemotePortClientSideGuard|TestExposeRemotePortValidValueReachesConnect|TestBrokerErrorMessageRegisteredCodes' \
  -count=20
# PASS

go test ./internal/port ./internal/proto -count=20
# PASS

go test ./test/p6 \
  -run 'TestExpose(HonorsRequestedRemotePort|RequestedPortTakenHardFails|RequestedPortOutOfBand|RequestedPortReusableAfterRm|OmittedRemotePortUnchanged)$' \
  -count=10
# PASS

go test -race ./test/concurrency \
  -run 'TestConcurrentPortAllocations|TestConcurrentDesiredPortExactlyOneWins|TestSqliteFileTwoOpensCanCoexist' \
  -count=20
# PASS

go test ./cmd/tether ./internal/port ./internal/proto ./internal/broker ./test/p6 -count=1
# PASS

go vet ./internal/port ./internal/proto ./cmd/tether ./internal/broker ./test/p6 ./test/concurrency
# PASS

go build ./...
# PASS

golangci-lint v2.5.0 run
# PASS, 0 issues

git diff --check
git diff --cached --check
# PASS
```

The lint command initially could not load packages inside the restricted
sandbox; the same pinned binary passed outside the sandbox.

The full repository and phase matrix were not repeated in round 2. Round 1
already established that P12/P6 passed while unrelated macOS-sensitive P9/P10
and Unix-socket/path tests failed on this host.

## Recommendation

P12 can proceed to phase closeout. Keep the CLI wire-through, desired-port
allocation, and concurrency tests as permanent regression coverage.
