# P1 Round 3 Review

Date: 2026-05-08
Reviewer role: test engineer

## Scope

Third-round review of commit `256f021` after the maintainer response to the P1 round-2 finding.

Focus areas:

- Verify the `IssueUserJWT` panic fix.
- Re-run all independent P1 risk tests.
- Re-run full test, coverage, build, and version checks.

## Verdict

P1 is approved to proceed to P2.

No important blocker remains in the P1 foundation packages. The round-2 auth crash path is fixed: bad `userPub` inputs are rejected before `jwt.NewUserClaims` is called, and the independent panic regression test now passes.

## Results

Independent P1 risk tests all pass:

```text
TestAccountSignerRejectsUserSeed
TestIssueUserJWTRejectsEmptySubjectWithoutPanic
TestStorageForeignKeysHoldAcrossPooledConnections
```

Full test suite passes:

```text
go test ./...
```

Internal coverage remains above the P1 target:

```text
auth     85.4%
proto   100.0%
storage 81.8%
schema  [no statements]
```

Build and version output are valid:

```text
tether v0.0.0-dev (proto v1)
linux/amd64
go1.25.0
```

## Verification Commands

```bash
env GOCACHE=/tmp/tether-go-build-cache go test ./test/p1 -run Test -count=1 -v
env GOCACHE=/tmp/tether-go-build-cache go test ./...
env GOCACHE=/tmp/tether-go-build-cache go test ./internal/... -cover
env GOCACHE=/tmp/tether-go-build-cache make build
./bin/tether version
make lint
```

`make lint` was not executable in this local environment because `golangci-lint` is not installed. The Makefile reports `Run: make tools`, which is the documented local setup path and not a release blocker for P1.

## Notes For P2

- Keep `test/p1/foundation_risk_test.go` in the full `go test ./...` suite; it now guards the auth and SQLite invariants that P2/P3 depend on.
- P2 can build on the current `auth`, `proto`, `schema`, and `storage` packages.

---

## Maintainer Response (2026-05-08)

**ACK — approval received, proceeding to P2.**

No new findings to address. `test/p1/` stays as a permanent regression
suite; nothing else from rounds 1–3 needs revisiting. The reviewer's note
about `make lint` requiring `make tools` locally is the documented v1
contract (P0 review F1), no change.

Phase status updated to P2 in README at the start of the next commit.

