# P1 Foundation Packages Review

Date: 2026-05-08
Reviewer role: test engineer

## Review Scope

Reviewed P1 against `docs/architecture.md` P1 requirements:

- `internal/proto`
- `internal/schema`
- `internal/auth`
- `internal/storage`
- P0 carry-over changes to version output, CI, Makefile, and README

I also added independent risk tests under `test/p1/` for security and storage invariants that are easy to miss in package-local tests.

## Verdict

P1 is not ready to promote to P2 yet.

The package-local tests are in good shape and meet the P1 coverage target, but the independent tests exposed two high-risk implementation bugs:

- `auth.LoadAccountSigner` accepts a user nkey seed as an account signer.
- SQLite foreign-key enforcement is not reliable across pooled connections.

Both should be fixed before P2 starts depending on storage state and auth primitives.

## Findings

### 1. High: SQLite foreign keys are only enabled on one pooled connection

Evidence:

- `storage.Open` calls `PRAGMA foreign_keys = ON` once after `sql.Open`.
- SQLite PRAGMAs are connection-local.
- `*sql.DB` may open more than one connection later.
- The migration relies on foreign keys and cascading deletes for `members`, `nodes`, `processes`, and `port_allocations`.
- Independent test `TestStorageForeignKeysHoldAcrossPooledConnections` failed: an orphan `members` row referencing a missing session was accepted on another pooled connection.

Why this matters:

This breaks the state authority guarantees. P2/P3 code could create orphan members, processes, or ports, and H.3 session cleanup/cascade behavior becomes connection-dependent.

Suggested fix:

- For v1, set `db.SetMaxOpenConns(1)` immediately after opening SQLite, before migrations.
- Keep the independent pooled-connection test, or adjust it to assert the chosen connection policy.
- If later higher concurrency is needed, use a driver-level connection hook or DSN pragma that applies `foreign_keys=ON` to every new connection.

Relevant code: `internal/storage/storage.go:37`, `internal/storage/migrations/0001_init.sql:7`.

### 2. High: account signer accepts user seeds

Evidence:

- `LoadAccountSigner` only checks that `nkeys.FromSeed(seed)` parses.
- It does not check that the seed is an account seed.
- Independent test `TestAccountSignerRejectsUserSeed` failed: a `SU...` user seed was accepted and produced a `U...` issuer.

Why this matters:

The auth_callout path depends on tetherd issuing NATS user JWTs from an account identity. Accepting user seeds makes signer misconfiguration silent and can produce invalid or misleading JWTs.

Suggested fix:

- Validate the derived public key is an account public key (`A...`) before accepting the signer.
- Add tests for: account seed accepted, user seed rejected, malformed seed rejected.

Relevant code: `internal/auth/jwt.go:16`.

### 3. Medium: `SubjCmdBy` argument order differs from the architecture document

Evidence:

- Architecture I.3 specifies `SubjCmdBy(actor, sid, nid, verb string)`.
- Implementation is `SubjCmdBy(sid, actor, nid, verb string)`.
- All parameters are strings, so a caller following the document will compile and produce a wrong subject.

Why this matters:

This is exactly the sort of quiet proto-layer mistake that later becomes a NATS permission failure or, worse, a misrouted subject. P3 will generate JWT permissions from these strings, so the builder API should be unambiguous.

Suggested fix:

- Prefer matching the architecture signature: `SubjCmdBy(actor, sid, nid, verb string)`.
- Alternatively, update the architecture and add a comment explaining why `sid` comes first.

Relevant code: `internal/proto/subjects.go:21`, `docs/architecture.md:1480`.

### 4. Medium: PIN policy is inconsistent across docs and implementation

Evidence:

- `pin.go` enforces ASCII printable, no length or complexity rules.
- `docs/requirements.md` says ASCII printable and no length/complexity.
- `docs/architecture.md` E.4 says PIN length 6-12 and numeric or alphanumeric.
- Existing tests accept `p@ssw0rd!` and 32-character PINs.

Why this matters:

P3 session login will expose this as user-visible security policy. If the wrong rule ships, changing it later affects existing sessions and documentation.

Suggested fix:

- Decide which document is authoritative.
- If requirements wins, update architecture E.4.
- If architecture wins, tighten `ValidPIN` and tests before P3.

Relevant code: `internal/auth/pin.go:66`.

### 5. Low: node ID validation is stricter than B.5

Evidence:

- Architecture B.5 says `nid` is `[a-z0-9-]{1,32}`.
- `ValidateNID` also requires leading lowercase alpha and no trailing hyphen.

Why this matters:

This may reject valid agent configurations such as `1gpu` that the architecture currently allows. It is not unsafe, but it is a compatibility decision that should be explicit before agent config lands.

Suggested fix:

- Either relax `ValidateNID` to match B.5, or update B.5 to document the stricter node-id grammar.

Relevant code: `internal/proto/identifiers.go:50`, `docs/architecture.md:333`.

### 6. Low: README still reports phase P0

Evidence:

- README status says `phase P0`.
- Current HEAD is `P1 foundation packages`.

Why this matters:

Minor, but phase status is part of handoff hygiene. It can mislead reviewers and future contributors.

Suggested fix:

- Update README status to P1 after the P1 fixes land.

## Independent Tests Added

Added `test/p1/foundation_risk_test.go`:

- `TestAccountSignerRejectsUserSeed`
- `TestStorageForeignKeysHoldAcrossPooledConnections`

Current result:

```text
FAIL github.com/LinZiyang666/tether/test/p1
```

These tests fail against the current P1 implementation and should remain as regression tests after the fixes.

## Verification

Commands run locally:

```bash
env GOCACHE=/tmp/tether-go-build-cache go test ./internal/... -cover
env GOCACHE=/tmp/tether-go-build-cache go test ./test/p1 -run Test -count=1 -v
env GOCACHE=/tmp/tether-go-build-cache go test ./...
env GOCACHE=/tmp/tether-go-build-cache make build
./bin/tether version
```

Results:

- `go test ./internal/... -cover`: passed.
- Coverage: `auth` 86.2%, `proto` 100.0%, `storage` 74.0%; `schema` has no executable statements.
- `go test ./test/p1`: failed on the two independent risk tests above.
- `go test ./...`: failed because `test/p1` fails.
- `make build`: passed. The sandbox printed a non-fatal Go module stat-cache warning under `/home/weiland/go/pkg/mod/cache`.
- `./bin/tether version`: printed `tether v0.0.0-dev (proto v1)`.

## Recommendation

Fix the two high-severity failures before entering P2. After that, rerun:

```bash
env GOCACHE=/tmp/tether-go-build-cache go test ./...
```

Then resolve the API/documentation mismatches while the surface is still small.

---

## Maintainer Response (2026-05-08)

Triage of the six findings. Verification at the end of this section.

### F1 — SQLite foreign keys across pooled connections

**Status: ACCEPTED.**

The bug is real: SQLite PRAGMAs are connection-local, the one-shot `PRAGMA
foreign_keys = ON` after `sql.Open` only covered whichever pooled conn ran
it. Reviewer's `TestStorageForeignKeysHoldAcrossPooledConnections` failed
against this and was reproducible.

The reviewer's suggested fix (`SetMaxOpenConns(1)` only) is necessary but
**not sufficient**: the reviewer's own test deliberately re-sets
`SetMaxOpenConns(2)` after `Open`, which would re-open the bug. So I went
belt-and-suspenders:

1. Inject `_pragma=foreign_keys(1)` into the DSN — modernc.org/sqlite runs
   it at every `sqlite3_open_v2`, so every conn in the pool starts with FK
   on regardless of how the `*sql.DB` pool is later configured.
2. Also `SetMaxOpenConns(1)` — SQLite is single-writer; serializing writes
   sidesteps `SQLITE_BUSY` and any "second conn missed the pragma" class of
   bug.

Plain `:memory:` isn't URI-style and won't accept query parameters; promoted
to `file::memory:` so the parameter is honored. Removed the now-redundant
explicit `db.Exec("PRAGMA foreign_keys = ON")` (it never propagated to other
conns anyway).

`internal/storage/storage.go` — `Open` + new `withForeignKeysPragma`.

Risk test now passes:

```text
ok  github.com/LinZiyang666/tether/test/p1  0.028s
```

Storage coverage rose from 74.0% → 81.8% (new branches in DSN helper and
removal of unreachable code).

### F2 — Account signer accepted user seeds

**Status: ACCEPTED.**

Real bug. `LoadAccountSigner` only validated that the seed parsed at all,
not that it was an *account* seed. A `SU…` user seed produced a `U…`
issuer, which would have silently broken auth_callout.

Fix: derive the public key, then check `nkeys.IsValidPublicAccountKey(pub)`
before storing the seed. User and malformed seeds are rejected with explicit
error messages.

Added two new tests next to the existing malformed-seed test:

- `TestLoadAccountSignerAcceptsAccountSeed` — happy path, asserts derived
  public key starts with `A`.
- `TestLoadAccountSignerRejectsUserSeed` — what the risk test checks, now
  with a same-package assertion.

`internal/auth/jwt.go` — `LoadAccountSigner`. Risk test
`TestAccountSignerRejectsUserSeed` now passes.

### F3 — `SubjCmdBy` argument order

**Status: PARTIALLY DECLINED — fixed the doc instead.**

The doc/impl mismatch is real and worth resolving, but I disagree with the
*direction* of the fix. The implementation uses `(sid, actor, nid, verb)`,
matching every other per-session subject builder in the same file
(`SubjCmdForwarded`, `SubjEvProc`, `SubjEvPort`, `SubjPty*`, `SubjAudit*`)
which all take `sid` first. Changing only `SubjCmdBy` to `(actor, sid, nid,
verb)` would create a one-off ordering inconsistency that's *more* error-
prone, not less.

The architecture has an explicit rule: "若在实现中发现设计问题，先改文档再改
代码" (when implementation reveals a design issue, fix the doc first). I.3
was an illustrative snippet; the normative subject layout in B.1 is
unambiguous. Updated the snippet at `docs/architecture.md` I.3 to
`(sid, actor, nid, verb)` with a one-line comment explaining the convention.

If a stronger guard is wanted later (typed args), that's a separate change
worth doing for *every* builder, not just `SubjCmdBy`.

### F4 — PIN policy inconsistency

**Status: ACCEPTED.**

requirements.md §6.3 and architecture.md E.4 contradicted each other.
requirements is the authoritative "what"; architecture is the "how" and
must serve it. Implementation already matched requirements (ASCII printable,
no length/complexity).

Updated architecture E.4 to align: ASCII printable, no length/complexity in
v1, with an explicit reference to requirements §6.3. Pinned the argon2id
parameters explicitly in the doc (`m=64MiB, t=3, p=2`) so the section is
self-contained.

No code change.

### F5 — `ValidateNID` stricter than B.5

**Status: ACCEPTED.**

B.5 only specifies the character class `[a-z0-9-]{1,32}`. requirements §7.5
adds nothing further. The leading-letter / no-trailing-dash constraints I
added were sid rules from §7.4 that I extended to nid by analogy — that
analogy isn't in any spec.

Relaxed `ValidateNID` to just the regex; updated tests so `1gpu`, `node-`,
and `default` are accepted (sid still gets the strict rules from §7.4 and
keeps its reserved-name list).

`internal/proto/identifiers.go` — `ValidateNID`.

### F6 — README still reported P0

**Status: ACCEPTED.** Updated to phase P1 with a one-line scope summary.

### Independent risk tests — kept as regressions

`test/p1/foundation_risk_test.go` is now committed as-is (no edits to the
reviewer's assertions); both tests pass after F1 + F2.

### Verification

```bash
make build && ./bin/tether version | head -1
  tether v0.0.0-dev (proto v1)

go test ./...
  ok  github.com/LinZiyang666/tether/cmd/tether
  ok  github.com/LinZiyang666/tether/internal/auth
  ok  github.com/LinZiyang666/tether/internal/proto
  ok  github.com/LinZiyang666/tether/internal/schema
  ok  github.com/LinZiyang666/tether/internal/storage
  ok  github.com/LinZiyang666/tether/test/p1

go test -cover ./internal/...
  internal/auth     85.1%
  internal/proto   100.0%
  internal/schema  [no statements]
  internal/storage  81.8%
```

Ready to enter P2.

