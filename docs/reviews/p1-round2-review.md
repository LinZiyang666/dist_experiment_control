# P1 Round 2 Review

Date: 2026-05-08
Reviewer role: test engineer

## Scope

Second-round review of commit `e184648` after the maintainer response to the first P1 review.

Focus areas:

- Regression testing the first-round high-risk findings.
- Checking whether the fixes introduced new edge-case failures.
- Verifying package coverage and build health.

## Verdict

P1 is improved, but still not ready to promote to P2.

The previous high-risk findings are fixed:

- `LoadAccountSigner` now rejects user seeds.
- SQLite foreign-key enforcement now holds across pooled connections.

However, a new independent auth risk test exposes that `IssueUserJWT` panics on an empty user public key instead of returning an error. This should be fixed before P2/P3 build on the auth primitives.

## Regression Results

These first-round risk tests now pass:

```text
TestAccountSignerRejectsUserSeed
TestStorageForeignKeysHoldAcrossPooledConnections
```

Full package tests before adding the new edge-case test were green:

```text
go test ./...
```

Internal coverage remains above the P1 target:

```text
auth     85.1%
proto   100.0%
storage 81.8%
schema  [no statements]
```

## New Finding

### High: `IssueUserJWT` panics for an empty user public key

Evidence:

- Added `TestIssueUserJWTRejectsEmptySubjectWithoutPanic` in `test/p1/foundation_risk_test.go`.
- The test fails with `runtime error: invalid memory address or nil pointer dereference`.
- Root cause: `jwt.NewUserClaims("")` returns `nil`, then `IssueUserJWT` writes `uc.Permissions`.

Why this matters:

`IssueUserJWT` is an auth primitive intended for auth_callout. Bad or missing user public keys should be rejected as ordinary errors, not converted into a tetherd crash path. P3 will put this function on the connection-auth path, so this should be hardened now.

Suggested fix:

- Validate `userPub` before calling `jwt.NewUserClaims`, preferably with `nkeys.IsValidPublicUserKey(userPub)`.
- Return a clear error for empty or non-user subjects.
- Keep the new independent test as a regression test.

Relevant code:

- `internal/auth/jwt.go:44`
- `test/p1/foundation_risk_test.go:42`

## Verification

Commands run:

```bash
env GOCACHE=/tmp/tether-go-build-cache go test ./test/p1 -run Test -count=1 -v
env GOCACHE=/tmp/tether-go-build-cache go test ./...
env GOCACHE=/tmp/tether-go-build-cache go test ./internal/... -cover
env GOCACHE=/tmp/tether-go-build-cache make build
./bin/tether version
make lint
```

Results:

- `go test ./test/p1`: failed only on `TestIssueUserJWTRejectsEmptySubjectWithoutPanic`.
- `go test ./...`: failed because `test/p1` fails.
- `go test ./internal/... -cover`: passed; coverage above target.
- `make build`: passed. The sandbox still prints a non-fatal Go stat-cache warning under `/home/weiland/go/pkg/mod/cache`.
- `./bin/tether version`: printed `tether v0.0.0-dev (proto v1)`.
- `make lint`: not run locally because `golangci-lint` is not installed; Makefile reports `Run: make tools`.

## Recommendation

Fix `IssueUserJWT` input validation, then rerun:

```bash
env GOCACHE=/tmp/tether-go-build-cache go test ./...
```

After that, P1 looks close to promotable. I do not see remaining blockers in the previous SQLite/account-signer fixes.

---

## Maintainer Response (2026-05-08)

### Round-2 finding — `IssueUserJWT` panics on bad userPub

**Status: ACCEPTED.**

Real bug. `jwt.NewUserClaims("")` returns `nil`; the next line
`uc.Permissions = perms` then nil-derefs. P3 will route this through
auth_callout, where any malformed input (typo in connection metadata, a
client connecting before nkey generation, etc.) would crash tetherd instead
of returning a clean reject.

Fix: validate `userPub` with `nkeys.IsValidPublicUserKey` before calling
`jwt.NewUserClaims`. Returns a clear error for empty / malformed /
wrong-kind inputs. Account public keys (`A…`) are also rejected even though
they parse — only user public keys (`U…`) belong here.

`internal/auth/jwt.go` — `IssueUserJWT` (3-line guard at the top).

Two tests now cover this:

- `test/p1/foundation_risk_test.go` —
  `TestIssueUserJWTRejectsEmptySubjectWithoutPanic` (kept as the reviewer
  added it).
- `internal/auth/jwt_test.go` — `TestIssueUserJWTRejectsBadUserPub`, same-
  package coverage of four bad-input shapes (empty, garbage, malformed
  user-shaped, account-kind).

Both use `defer recover()` to catch any future regression that brings the
panic back.

### Verification

```bash
go test ./...
  ok  github.com/LinZiyang666/tether/cmd/tether
  ok  github.com/LinZiyang666/tether/internal/auth
  ok  github.com/LinZiyang666/tether/internal/proto
  ok  github.com/LinZiyang666/tether/internal/schema
  ok  github.com/LinZiyang666/tether/internal/storage
  ok  github.com/LinZiyang666/tether/test/p1

go test -cover ./internal/...
  internal/auth     85.4%   (up from 85.1%; new test path)
  internal/proto   100.0%
  internal/schema  [no statements]
  internal/storage  81.8%
```

No other findings remain. Ready for P2.

### Note on `make lint` reproducibility

Reviewer notes `golangci-lint` not installed locally; Makefile already
prints `Run: make tools` (P0 review F1 fix). This is the documented v1
contract — no change.

