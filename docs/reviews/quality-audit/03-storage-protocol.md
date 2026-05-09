# Storage & Protocol Audit

Scope: `internal/storage/`, `internal/storage/migrations/*.sql`,
`internal/session/`, `internal/node/`, `internal/proc/`, `internal/port/`,
`internal/jsstream/`, `internal/schema/`, `internal/proto/`,
`internal/broker/audit.go`, `internal/agentprov/`. Read-only review.

## Verdict

11 findings (0 critical, 2 high, 5 medium, 4 low).

The data layer is on the whole well-organized: every SQL call uses
placeholders, FK + CASCADE chain is consistent, the H.3 three-phase rm is
deliberately idempotent, and the migration framework is correctly
bracketed in transactions. The high-severity findings are
contract-shape issues that already shipped (audit `req_id` permanently
empty; `target` map permanently absent) and one missing index on a hot
authoritative-read path (`port_allocations.token_hash`). The rest are
minor — defensive style + future-proofing.

No SQL injection. No unclosed tx leaks. No panics on malformed
subjects. Migrations are properly transactional and re-entrant.

## Findings

### F1 — high — `port_allocations.token_hash` lookup is full-table-scan

**Where**: `internal/port/port.go:224` (`LookupByTokenHash`),
`internal/storage/migrations/0001_init.sql:81-82`,
`internal/storage/migrations/0003_port_alloc_history.sql:44-48`.

**Issue**: `LookupByTokenHash` is invoked by `tunnelTokenLookup`
(`internal/broker/expose.go:50`) on every frpc REGISTER — i.e. every
tunnel client connection. The query is
`WHERE token_hash=? AND state='ALLOCATED'`. The schema indexes the table
on `(sid, nid)`, `(state)`, and the partial unique
`(port WHERE state='ALLOCATED')`, but never on `token_hash`. SQLite
falls back to a full scan of `port_allocations` for every connection.

**Impact**: With v1 expectations (small port band, ≤1000 ALLOCATED rows)
the absolute cost is small, but a long-running broker accumulates
REVOKED/FREED rows indefinitely (architecture D.4 explicitly keeps them
for audit), so a busy lab can grow to tens of thousands of rows. Each
frpc connect then walks all of them. Worse, the path from frpc REGISTER
to deny/allow is the only thing standing between an attacker with a
guessed/leaked token and a tunnel — a slow path here means the broker's
TLS termination thread is held up waiting on SQLite under attack.

**Fix**: add a non-unique index in a new migration:
```sql
CREATE INDEX IF NOT EXISTS idx_port_alloc_token_hash
    ON port_allocations(token_hash);
```
The query planner will then use it for the equality lookup; the
`AND state='ALLOCATED'` becomes a residue check on the matching rows.
Token hashes are SHA256 hex (64 chars, unique with overwhelming
probability), so an `idx_port_alloc_token_hash_active` partial index on
`token_hash WHERE state='ALLOCATED'` would be even tighter, but the
plain index is already O(log N).

---

### F2 — high — `schema.AuditCall.ReqID` and `.Target` are never populated

**Where**: `internal/schema/audit.go:18-32` (definition),
`internal/broker/exec.go:368-381` (`pubAuditCall`),
`internal/broker/expose.go` (all expose call sites).

**Issue**: `AuditCall` declares
`ReqID string \`json:"req_id"\`` (no omitempty) and
`Target map[string]any \`json:"target,omitempty"\``. The audit
test (`internal/schema/audit_test.go:28-32`) confirms both are part of
the contract. Yet every call to `pubAuditCall` constructs the envelope
with neither field set — `ReqID` is implicitly the zero string and is
written as `"req_id":""` on every audit row, and `Target` is omitted
entirely (no verb-specific context: no `argv`, no `cwd`, no `name` for
expose, no `pid` for kill, etc.).

**Impact**:
- `req_id` is the architecture H.5 hook that lets a single ctl request
  be traced through call → proc → port audit lines. Permanent empty
  string means audit consumers cannot stitch a session's event history
  back to the originating command without ad-hoc heuristics on
  timestamps + actor.
- `target` is the verb-specific payload audit consumers need to render
  meaningful tail output (today `tether history` shows
  `Verb=expose Ok=true` and nothing about which expose). The shipped
  shape forces operators to cross-reference the SQLite tables.

This is the same class of bug P7 round-2 review F2 ("audit schema
alignment") was meant to close out; the schema struct has the
fields but the producer side was never wired.

**Fix**: in `pubAuditCall` accept and write both:
```go
func (b *Broker) pubAuditCall(sid, actorFP, actorNkey, verb, nid, reqID string,
    target map[string]any, ok bool, errMsg string) {
    ...
    schema.AuditCall{ ..., ReqID: reqID, Target: target, ... }
}
```
`reqID` can come from `nats.Msg.Header.Get("Nats-Msg-Id")` if ctl
stamps one, or the broker can generate a fresh ULID on first sight and
pass it through to `pubAuditProc` / `pubAuditPort` so the three audit
streams share an id. `target` maps cleanly per verb (exec/run →
`{argv, cwd}`; expose → `{name, local_port, port}`; expose-rm →
`{name}`; kill → `{pid, signal}`; upgrade → `{url, sha256}`).

---

### F3 — medium — `JetStream` history stream has no eviction at all

**Where**: `internal/jsstream/jsstream.go:83-95`.

**Issue**: `EnsureHistoryStream` sets `MaxAge: 0` (== unlimited),
`MaxBytes: -1` (unlimited), no `MaxMsgs`, no `MaxMsgsPerSubject`, and
`Discard: DiscardNew`. With every limit unset, the discard policy is
unreachable code — JetStream will keep accepting writes until the
underlying filesystem fills up, at which point publishes start failing
with `insufficient_resources` (which currently logs a warn from
`broker.publishAudit` and returns nil so the original op completes).

The disk-pressure monitor (`internal/broker/disk.go`) emits an advisory
sys.event at 80% but takes no action — H.4 says "operator decides". So
in practice a long-lived session's history grows unboundedly until the
disk is full, then ALL audit goes silent (because new publishes fail)
without any per-stream backpressure.

**Impact**: Operationally, there is no graceful degradation. v1 design
explicitly chooses this; the documented intent is that operators
`session rm` cold sessions before they fill the disk. But the
`Discard: DiscardNew` setting suggests someone meant to bound something
and forgot to set the bound. Either remove `Discard` from the config
(it's currently meaningless) or set a real cap (`MaxBytes` or
`MaxMsgsPerSubject`) consistent with H.4.

**Fix**: pick one. Either:
1. Drop `Discard: jetstream.DiscardNew` and add a comment
   "no eviction by design — operator does session rm" (codify intent).
2. Set `MaxBytes` to a large per-session ceiling (e.g. 100 MiB) so an
   accidental loop publishes can't take down a whole broker, and let
   `Discard: DiscardNew` actually do its job (preserve old audit, refuse
   new at the brink). Option 2 is safer.

---

### F4 — medium — `splitDot` in `broker/exec.go` returns wrong shape for empty input

**Where**: `internal/broker/exec.go:347-360`, used by `handleNodeListReq`
(line 184) and another `splitDot` site at line 248.

**Issue**: `splitDot("")` returns `[""]` (length 1) instead of
`[]` — the loop never enters and the final `append(out, s[start:])`
appends one empty string. The two callers gate on
`len(parts) == 5 || ...` so an empty leaf would safely fall into
`subject_malformed`. But `splitDot("a")` returns `["a"]`; `splitDot("a.")`
returns `["a", ""]`; `splitDot(".")` returns `["", ""]`. Combined with
`ParseCtrlBy` that already accepts a `leaf` like `s.lab.node.list.req`,
a malformed subject like `tether.v1.ctrl.by.UABC.s..node.list.req`
parses to `parts = ["s","","node","list","req"]`, len==5, parts[2]=="node",
parts[3]=="list", parts[4]=="req" — **passes** the shape check, and
`sid` is set to the empty string. The broker then asks
`session.IsActive(db, "")` which (correctly) returns false → reply
`session_not_found_or_deleting`.

So no security impact today, but the parser is tolerating injected dots
that should be rejected at the subject layer.

**Impact**: Low under JWT-permissioned subjects (B.2 pins
`by.<actor>` per-connection so the actor can't spoof, and the
malformed sid would simply fail the membership check). However, audit
logs will still record an `actor_invalid:` or `not_a_member` on a
syntactically nonsense subject when the cleaner fail-mode is
`subject_malformed`.

**Fix**: after `splitDot` (or in the parser itself), validate the sid
through `proto.ValidateSID` and the nid through `proto.ValidateNID`. The
identifier package has these; use them.

---

### F5 — medium — `proto.ParseCmdBy` and friends don't validate sid/nid

**Where**: `internal/proto/subjects.go:127-191`.

**Issue**: `ParseCmdBy`, `ParseCtrlBy`, `ParseEvProc`,
`ParseSidNidFromCtrl` all check token count + literal segment names but
treat the variable segments (sid / nid / actor / pid / verb) as opaque
strings. Most callers then pass these straight into SQL placeholders
(safe) and JetStream stream names (mostly safe). But the validation
helpers `ValidateSID` / `ValidateNID` / `ValidateActorToken` exist
exactly to enforce the architecture B.5 character class
([a-z0-9-]{1,32}); the parsers should call them so a malformed token
triggers `ok=false` rather than reaching the next layer.

**Impact**: defense-in-depth gap. With NATS JWT permissions pinning
`by.<A>` (B.2), callers cannot spoof actor segments. Subjects pinned
in subscriptions ensure malformed token counts get NATS-rejected
before they reach handlers. Still, no defense against e.g. a future
broker-side test or admin tool that constructs a subject manually.

**Fix**: in each parser, after the structural check, run
`ValidateSID`/`ValidateNID`/`ValidateActorToken` on the corresponding
field; return `ok=false` if any fails. The cost is one regex match per
incoming message, negligible.

---

### F6 — medium — `proc.MarkExited` swallows non-`ErrNoRows` errors from the existence check

**Where**: `internal/proc/proc.go:145-165`.

**Issue**: When the UPDATE affects zero rows, the function does a second
`SELECT 1 FROM processes WHERE pid=?` to disambiguate "unknown pid"
(returns `ErrNotFound`) from "already non-RUNNING" (idempotent OK).
But it only branches on `errors.Is(err, sql.ErrNoRows)`. Any other
error from the SELECT (e.g. database locked, I/O error, query timeout
once one is added) is silently dropped on the floor and `MarkExited`
returns nil — the caller (`handleProcEvent` at
`internal/broker/exec.go:164`) believes the row was successfully
transitioned when in fact the agent's `proc.exit` event has now been
silently lost.

**Impact**: rows can stay stuck in RUNNING under transient store
errors; surfaced later as a missed-exit during reconcile (rc=-1
synthesized), so the architectural backstop catches it. But the
`pubAuditProc(sid, "exit", ...)` call after MarkExited still runs and
publishes the (genuine) agent rc into the audit stream, so the audit
record claims a clean exit with rc=N while SQLite still says RUNNING.
Inconsistent.

**Fix**:
```go
if err := db.QueryRow(...).Scan(&n); err != nil {
    if errors.Is(err, sql.ErrNoRows) {
        return ErrNotFound
    }
    return fmt.Errorf("proc: existence check: %w", err)
}
```
Same pattern as `port.Free` does correctly.

---

### F7 — medium — `session.JoinWithPIN` does the SELECT and AddMember in two separate connections (no tx)

**Where**: `internal/session/session.go:257-277`.

**Issue**: The function does `db.QueryRow(...)` for `pin_hash, state`,
verifies, then calls `AddMember` which does a separate `db.Exec`. With
`SetMaxOpenConns(1)` enforced by `storage.Open`, the two operations are
serialized on the single connection, but no transaction brackets them.
Between the SELECT and the INSERT, another goroutine could (a)
tombstone the session (UPDATE state) or (b) rotate the pin hash (a
future feature). The current PIN check would still permit the join.

**Impact**: today there is no rotate-pin verb, and tombstoning between
the SELECT and INSERT is a race window of microseconds. Damage is
bounded — the session is in DELETING, but a member row gets added
anyway. The H.3 phase ③ `dropSessionRows` would then sweep it on
finalize. So no leak.

**Fix**: wrap the read+write in a single `db.Begin()` + `tx.Commit()`
the same way `Create`, `Register`, `Insert` already do. The
`AddMember` helper takes a `*sql.DB` today; refactor to take an
`exec` interface so it can be reused inside a tx.

---

### F8 — low — `port_allocations.created_at` reuses old `revoked_at` value on row reuse via partial unique index

**Where**: `internal/storage/migrations/0003_port_alloc_history.sql:44-48`,
`internal/port/port.go:Allocate`.

**Issue**: The partial unique index `idx_port_alloc_unique_active`
correctly allows multiple historical rows for the same `port` as long
as at most one is `state='ALLOCATED'`. This is the right shape for
audit. However, the in-memory `findFreePort` function loads
`SELECT port FROM port_allocations WHERE state='ALLOCATED' AND port
BETWEEN ? AND ?` and picks the lowest unclaimed port. If the inner
loop picks port 14022 and then the INSERT proceeds, the PRIMARY KEY is
the synthetic `row_id` — no constraint will reject the INSERT if a
concurrent INSERT (on a different SQLite connection — not possible
here) raced and got 14022 first. The partial unique index is the
catch-all.

With `SetMaxOpenConns(1)`, this race truly cannot happen in v1 — the
two writes are on the same conn and necessarily serialized. The
partial index is correctly defensive against future pool-size changes
or against a sidecar admin tool writing through a second
connection. Nothing to fix; flagging because the safety story is
"don't ever raise SetMaxOpenConns(1) without re-thinking the whole
allocator". Worth a comment in `port.Allocate` and/or
`storage.Open`.

**Fix**: add to the doc comment on `storage.Open`:
> SetMaxOpenConns(1) is load-bearing: many writers in the codebase
> (port.Allocate, session.Create, ...) execute compound read-then-write
> sequences under default-deferred transactions and rely on the pool
> serializing all access through one connection. Raising the pool
> size requires reviewing every Begin() site for proper IMMEDIATE
> mode + retry-on-busy.

---

### F9 — low — DSN doesn't set `_busy_timeout`; concurrent external writers will get SQLITE_BUSY

**Where**: `internal/storage/storage.go:55-78`.

**Issue**: With `SetMaxOpenConns(1)` Go writers cannot conflict with
each other, but anything outside the broker process (a sqlite3 CLI for
debugging, a backup script with `.dump`, a future admin tool that
opens its own connection) will immediately collide with an in-flight
broker tx and get `SQLITE_BUSY`. The DSN has no `_busy_timeout` /
`_pragma=busy_timeout(N)`, so external readers/writers get an
instant failure rather than backing off.

**Impact**: low — operations work in the steady state; the failure
shows up only when an operator opens the DB in another tool while
broker is running. But an out-of-band backup that gets
`database is locked` is annoying.

**Fix**: bake `&_pragma=busy_timeout(5000)` into `withForeignKeysPragma`.
5s is the conventional friendly value — a broker tx never holds that
long under normal ops.

---

### F10 — low — `broker.adminAuditTail` underflow guard depends on `n > 0`

**Where**: `internal/broker/admin.go:50-60`.

**Issue**:
```go
want := uint64(n)
startSeq := uint64(1)
if last > want-1 {
    startSeq = last - want + 1
}
```
The function caps `n` to 50 if `n <= 0`, so `want >= 1` is currently
guaranteed. If a future caller path skips the cap (or passes a large
negative int that, cast to uint64, becomes huge), `want-1` underflows
or `last - want + 1` wraps to a huge number, then the `if startSeq <
first` guard saves it by clamping back to `first`. So the worst case
today is "loops over the whole stream" rather than a panic. Robust
enough, but the arithmetic is fragile.

**Fix**: either clamp once at the top with a single source of truth,
or use signed math:
```go
if n <= 0 || n > maxTailN { n = defaultTailN }
want := uint64(n)
if last < want { startSeq = first } else { startSeq = last - want + 1 }
```

---

### F11 — low — Migration file naming is sort-stable but lacks any compile-time guard

**Where**: `internal/storage/storage.go:84-122`,
`internal/storage/migrations/`.

**Issue**: Migrations are applied in `sort.Strings`-order over file
names, so the convention `NNNN_name.sql` (zero-padded prefix) is the
only thing keeping `0010_*.sql` from being applied before `0009_*.sql`
in some future. Drop the leading zero on a future contributor's
checkout (file system case-mismatch on macOS) and the order changes
silently. The framework also allows a non-numeric file like
`bootstrap.sql` to slot in alphabetically wherever, with no
indication.

**Impact**: low — the team has 3 migrations, all consistent. Future
risk only.

**Fix**: at apply time, refuse names that don't match a regex like
`^\d{4}_[a-z0-9_]+\.sql$`. Then the convention is enforced by build
rather than habit.

---

## Cross-cutting positives worth keeping

- Every SQL statement uses parameter placeholders. No
  `fmt.Sprintf`-into-SQL anywhere except the (literal) table-name
  interpolation in `applyMigrations` for `schema_migrations`, which
  is a constant. No injection surface.
- All transaction sites follow the canonical
  `tx, err := db.Begin(); if err {...}; defer tx.Rollback(); ... ;
  return tx.Commit()` pattern. No leaked tx anywhere I could find.
- FK + CASCADE chain consistent: a single `DELETE FROM sessions`
  reaches every dependent table. The explicit per-table DELETEs in
  `dropSessionRows` are correctly ordered (children before parents)
  and serve as documentation + future-proofing.
- `JetStream` create/delete are properly idempotent; the
  `ErrStreamNameAlreadyInUse` / `ErrStreamNotFound` sentinels are
  trapped exactly once at the right layer.
- Subject parsers reject malformed token counts before passing
  variable segments downstream — combined with NATS JWT permissions
  this is already a strong gate.
- `port_allocations`'s "row-per-state-transition + partial unique
  active" pattern is the right SQLite encoding of architecture D.4.
- Audit envelope schema (`schema/audit.go`) is properly versioned
  (`AuditSchemaVersion`) with `omitempty` discipline that survives
  round-trip tests.
