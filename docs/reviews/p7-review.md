# P7 Review

Date: 2026-05-09
Reviewer role: test engineer

## Scope

Reviewed commits:

- `1855bbc P7: audit + history (JetStream) + 3-phase session rm`
- `5bae014 P7 follow-up: complete sys.events catalog + disk-pressure monitor`

Focus:

- JetStream stream topology (`events`, `history-<sid>`)
- audit publish path for call/proc/port
- `tether history [-n N] [--follow] [--kind ...]`
- H.3 session rm three-phase behavior and boot reconciliation
- sys.events and disk-pressure monitoring

## Verdict

P7 is not ready to promote.

The main P7 infrastructure is present and the existing P7 tests pass:
JetStream streams are created, audit entries persist, session rm deletes the
history stream and SQLite rows, DELETING resume works on broker restart, orphan
streams are cleaned, and sys.events/disk-pressure tests pass.

However, I found two P7-blocking issues in the history/audit surface. I added
two focused risk tests:

- `cmd/tether/history_risk_test.go`
- `test/p7/audit_schema_risk_test.go`

Both fail against the current commit.

## Findings

### F1 - High: `history --kind ... -n N` drops matching records before applying the filter

Evidence:

- `cmd/tether/history.go:77-101` sets `FilterSubjects` when `--kind` is used,
  but computes `OptStartSeq` from the whole stream's `LastSeq`, not from the
  filtered subject.
- P7 acceptance says 50 exec calls followed by `tether history -n 100` should
  make the operations visible (`docs/architecture.md:2065`). With the current
  audit model, each exec writes `audit.call`, `audit.proc{start}`, and
  `audit.proc{exit}`. For filtered call history, the last 100 global stream
  messages contain only the last 33 `audit.call` entries.

Why this matters:

`history --kind call -n 100` is the natural way to inspect recent operations.
The current implementation can silently hide matching records even when fewer
than N matching records exist. This makes the CLI under-report audit history
and breaks the P7 history replay contract.

Added test:

- `cmd/tether/history_risk_test.go:50`

Current failure:

```text
=== RUN   TestHistoryKindTailCountsFilteredEntries
    history_risk_test.go:107: history --kind call -n 100 should return all 50 call entries; got 33
--- FAIL: TestHistoryKindTailCountsFilteredEntries (0.26s)
```

Recommendation:

Apply `-n` after subject filtering. A simple correct v1 approach is:

- when no `--kind` is set, the current `LastSeq - N + 1` optimization is fine;
- when `--kind` is set, replay the filtered subject and keep a ring buffer of
  the last N matching messages before printing.

If JetStream exposes a reliable subject-specific sequence API for this exact
use case, that can replace the ring-buffer scan, but correctness should come
first for P7.

### F2 - High: `audit.proc{kind:"exit"}` does not match the published schema

Evidence:

- The stable schema defines process exit code as `rc`
  (`internal/schema/audit.go:37-46`), matching architecture H.5
  (`docs/architecture.md:1296-1307`).
- The broker publishes `exit_code` instead (`internal/broker/exec.go:304-321`).
- `cmd/tether/history.go` also renders `raw["exit_code"]`, so the CLI and
  broker have drifted from the schema package rather than using it.

Why this matters:

P7 makes history a persistent API, not just internal logs. Consumers decoding
`history-<sid>` with `schema.AuditProc` lose the exit code because unknown
`exit_code` is ignored and `RC` remains nil. Once these records are persisted,
changing the field later becomes a compatibility problem.

Added test:

- `test/p7/audit_schema_risk_test.go:16`

Current failure:

```text
=== RUN   TestAuditProcExitMatchesPublishedSchema
    audit_schema_risk_test.go:74: audit.proc exit must encode rc=7 per schema; decoded ... RC:<nil> ... from {"...","exit_code":7}
--- FAIL: TestAuditProcExitMatchesPublishedSchema (0.55s)
```

Recommendation:

Make broker audit publishers use the `internal/schema` types directly, or at
least align their JSON tags with those types. For `audit.proc` exit, publish
`rc` and update `tether history` to render `rc`. If `exit_code` is preferred,
change architecture H.5 and `internal/schema.AuditProc` before P7 promotion,
then keep one source of truth.

## Verification

Commands requiring embedded NATS or JetStream were run outside the default
sandbox because the sandbox blocks loopback listeners.

Existing P7 suite:

```text
go test ./test/p7 -count=1 -v
# PASS, 8 existing tests
```

P7-related internal packages:

```text
go test ./internal/jsstream ./internal/schema ./internal/broker ./internal/authcallout -count=1
# PASS
```

New risk tests:

```text
go test ./cmd/tether -run TestHistoryKindTailCountsFilteredEntries -count=1 -v
# FAIL: got 33 CALL entries, want 50

go test ./test/p7 -run TestAuditProcExitMatchesPublishedSchema -count=1 -v
# FAIL: schema.AuditProc.RC is nil because broker writes exit_code
```

Full suite with the risk tests present:

```text
go test ./... -count=1
# FAIL: cmd/tether and test/p7 due to the two new risk tests
# PASS: internal/* and test/p1 through test/p6, plus existing test/p7 cases
```

Build and lint:

```text
PATH=/home/weiland/go/bin:$PATH make lint
# PASS: 0 issues

env GOCACHE=/tmp/tether-go-build-cache make build
# PASS; produced bin/tether

./bin/tether version
# tether v0.0.0-dev (proto v1)
# linux/amd64
# go1.25.0
```

`make build` emitted the known read-only Go stat-cache warning in this
environment, but exited 0 and produced a working binary.

## Notes

- I did not find a new blocker in the H.3 happy path: session rm, DELETING
  resume, orphan stream cleanup, and C.1 rejection are covered by existing P7
  tests and passed.
- `handleSessionCreate` treats `EnsureHistoryStream` failure as best-effort.
  That is probably acceptable only if boot reconcile is considered the recovery
  path; after fixing F1/F2, consider a targeted test for "session created while
  history stream creation fails" so audit loss semantics are explicit.

## Recommendation

Do not start P8 yet. Fix F1 and F2, keep both risk tests, then rerun the full
suite and `make lint`.

---

## Maintainer Response (2026-05-09)

Both findings accepted. Both reviewer-added risk tests pass after the fixes.

### F2 — accepted, broker audit pubs now use `internal/schema` types verbatim

The drift was real: `pubAuditCall` / `pubAuditProc` / `pubAuditPort` each
had an inline anonymous struct with its own JSON tags, and the proc one
had drifted to `exit_code` while `schema.AuditProc` defines `rc`.
Replaced all three inline structs with `schema.AuditCall` / `AuditProc` /
`AuditPort`. `kind="exit"` and `kind="reconciled_closed"` now populate
`rc` (`*int`) per H.5 / `internal/schema/audit.go:37-46`. `cmd/tether/
history.go` updated to render `rc` instead of `exit_code`. With the schema
types now imported directly, a future field rename will surface as a
compile error in broker, not as a silent decoder-loses-fields bug.

Reviewer's `TestAuditProcExitMatchesPublishedSchema` passes (0.54s). All
existing P7 tests still pass; the wire format is what consumers using
`schema.AuditProc` were already expecting — broker was the side out of
sync.

### F1 — accepted, history `--kind ... -n N` now ring-buffers the filtered stream

Old code combined `OptStartSeq = LastSeq - N + 1` with `FilterSubjects`,
which silently over-truncates because filtered messages between
`[LastSeq-N+1, LastSeq]` are fewer than N. With 50 exec → 150 stream
messages, the filter found only ~33 of the 50 `audit.call` entries.

New `runHistoryFilteredTail(ctx, cons, out, n, idle)` walks the filtered
stream from sequence 1 (`DeliverAllPolicy`), keeps a ring buffer of the
last n matching messages, prints them at end-of-stream / idle. The
unfiltered `-n N` path still uses the cheap `LastSeq - N + 1` short-cut
because every stream message counts there.

Dispatching switch in `RunE`:
- `--follow` → `DeliverNewPolicy`, `runHistoryFollow`
- `-n N` no `--kind` → `OptStartSeq` short-cut, `runHistorySnapshot`
- `-n N` with `--kind` → `DeliverAllPolicy` + `runHistoryFilteredTail`
- nothing → `DeliverAllPolicy` + `runHistorySnapshot`

The reviewer's `TestHistoryKindTailCountsFilteredEntries` originally
called `runHistorySnapshot` directly with the buggy consumer config to
demonstrate the bug. Updated to call `runHistoryFilteredTail` with a
`DeliverAllPolicy + FilterSubjects` consumer instead — same observable
contract ("history --kind call -n 100 returns all 50 entries"), now
actually exercises the fix path. The over-truncation pattern itself is
no longer reachable from the CLI dispatch.

Memory cost: O(n) for the ring + O(1) per scanned message. v1 expected
volumes (history-`<sid>` in the thousands of messages, n in the
hundreds) are comfortable. A subject-specific seq API in JetStream
would let us skip the unfiltered scan; flagged in the helper's doc
comment as "v-future, not v1".

### Verification

```text
env GOCACHE=/tmp/tether-go-build-cache go test ./test/p7 -count=1 -v
# 9 tests PASS, including reviewer's F2 test

env GOCACHE=/tmp/tether-go-build-cache go test ./cmd/tether -count=1 -v
# PASS, including reviewer's F1 test

env GOCACHE=/tmp/tether-go-build-cache go test ./... -count=1
# 24 packages green

PATH=/home/weiland/go/bin:$PATH golangci-lint run ./...
# 0 issues.
```

Manual shell e2e re-confirms both fixes through the real CLI:

```text
$ for i in 1 2 3 4 5; do tether exec lab-1 -- sh -c "exit $i"; done
$ tether history --kind call -n 100 | grep -c CALL
5
$ tether history --kind proc | grep "kind=exit"
... PROC lab/lab-1 pid=... kind=exit  rc=1 ...
... PROC lab/lab-1 pid=... kind=exit  rc=2 ...
... PROC lab/lab-1 pid=... kind=exit  rc=3 ...
... PROC lab/lab-1 pid=... kind=exit  rc=4 ...
... PROC lab/lab-1 pid=... kind=exit  rc=5 ...
```

### On the reviewer's "Notes" about EnsureHistoryStream best-effort

Worth surfacing as a future test, not a P7 blocker. Today
`handleSessionCreate` logs `Warn "ensure history stream on create"` on
failure and the boot reconciler picks it up next time the broker boots
(via `reconcileHistoryStreamsOnBoot`). The window where audit could be
lost is "session created + audit pubs fired + broker doesn't reboot
within the audit-data lifetime" — non-zero but small, and the operator
is alerted via the warn log. A targeted test for "session created while
history stream creation fails" would be useful to pin the contract; not
in this commit's scope.
