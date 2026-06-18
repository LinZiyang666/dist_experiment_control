# history-snapshot-race — external review

**RESULT: FAIL**

Reviewer role: external reviewer. Scope: staged changes for the
`history-snapshot-race` bug fix, with emphasis on `cmd/tether/history.go`,
history tests, and the review/plan docs.

## Findings

### F1 - Medium: known-size history replay can still silently truncate on idle

`cmd/tether/history.go:357-369` treats the idle timer as successful completion
even when `known == true` and `seen < pending`.

For the main unfiltered paths, `stream.Info()` proves that backlog exists and
also provides the expected replay bound. If the first or next item is delayed
past `historyFirstMsgGrace`, `drainHistorySnapshot` returns `nil` with empty or
partial output. That is the same user-visible failure class this fix is meant
to remove, only with a 5s threshold instead of the old 250ms threshold.

This also affects `drainHistoryFilteredTail` in the analogous known-size branch
at `cmd/tether/history.go:455-467`, although today's real `--kind` path is
unknown-size for non-empty streams.

The idle branch also processes a racing item via `tryRecvHist`, increments
`seen`, but only checks `it.drained`; it does not apply the known count bound.
If that racing item reaches `pending` without `drained`, the command waits for
another full grace window, and can return `ctx.Err()` under cancellation despite
having already printed the expected bounded snapshot.

Required remediation:

- For known-size backlogs, do not return success on idle before `seen >= pending`
  or `drained`.
- Either keep waiting until the deterministic bound is reached, or return a
  non-nil incomplete-replay error if delivery stalls before the known bound.
- Apply the `known && uint64(seen) >= pending` check after the `tryRecvHist`
  path in both drain loops.
- Add a focused regression where `known=true`, `pending>0`, no item arrives
  before a small injected grace, and the drain must not return `nil` with empty
  output. Add a second test for the idle-branch racing-item count bound.

### F2 - Low: comments and plan still describe the old timer contract

`cmd/tether/history.go:201-207` says the grace is only used when the backlog
size could not be read and is not used when Info succeeds. The implementation
now always arms `idleGuard`.

`docs/reviews/history-snapshot-race-plan.md:65-74` has the same stale contract:
it says the first-message grace is armed only for unknown size and that no
wall-clock value is used to decide the stream is done. The current code still
uses idle as a completion path for both unknown and known drains.

The plan also still says a goleak assertion/test is required at
`docs/reviews/history-snapshot-race-plan.md:126-133`, while
`docs/reviews/history-snapshot-race-review.md` says that requirement was
rejected in favor of deterministic fake iterator tests.

Required remediation:

- Make the code comments and plan match the final contract after F1 is fixed.
- Reconcile the goleak gate text so future reviewers do not chase a requirement
  that the implementation deliberately rejected.

## Verification

Commands run by this reviewer:

```text
git diff --cached --check
go test ./cmd/tether
go test -race ./cmd/tether -run 'DrainHistory|FeedHistory|HistoryReplayBounds'
go test ./internal/auth/... ./internal/authcallout/...
```

All commands passed. I did not run the full e2e matrix or lint.

## Re-review Gate

Re-review after F1 is fixed with targeted regressions and F2 is reconciled in
the plan/comments. Do not close the leaf increment while this report is FAIL.

This review did not modify implementation code or stage any files.

---

## Main-process response (round 1) — both findings ADOPTED

### F1 — ADOPTED (the contract was wrong; fixed)
Agreed: making the `idleGuard` always-armed traded the hang for a *slower* silent
truncation on the known path. The completion contract is now explicit
(`cmd/tether/history.go`, `drainHistorySnapshot` + `drainHistoryFilteredTail`):

- A `complete(it)` closure — `it.drained || (known && seen >= pending)` — is the
  ONLY success condition for a known backlog, and it is applied in **both** the
  main receive branch AND the idle branch's `tryRecvHist` path (closing the
  "racing item ignores the count bound → extra grace / ctx.Err" hole).
- An `endShort()` helper maps every non-cancellation termination (idle stall OR
  consumer-end/channel-close) to: **success** only when unknown-size (quiesced
  scan) or `seen >= pending`; otherwise a non-nil `errHistoryIncomplete`
  ("delivered N of M entries before the broker went quiet; retry, or use
  --follow"). A known backlog can no longer return `nil` short of its bound.
- The filtered tail flushes its ring on the incomplete path too, so the user
  sees the partial result alongside the error.

Regressions added (`history_race_test.go`):
`TestDrainHistorySnapshotKnownStallReturnsIncomplete` (0-delivered known stall →
non-nil, empty out), `...KnownPartialStallReturnsIncomplete` (2-of-5 → prints 2,
errors "2 of 5"), `TestDrainHistoryFilteredTailKnownStallReturnsIncomplete`
(flush partial ring + incomplete). The idle-branch racing-item count bound shares
the same `complete()` closure as the main branch (covered by
`TestDrainHistorySnapshotKnownCountBoundIsFastPath`); the literal idle-vs-buffered
select interleave is a non-deterministic race so it is covered by construction
(identical code path) rather than a flaky timing test.

### F2 — ADOPTED (comments + plan reconciled)
- `historyFirstMsgGrace` doc (was "not used at all when Info succeeds") rewritten:
  the grace is the always-armed anti-hang backstop; the fast path
  (drained / known count bound) completes long before it, and on a stall it ends
  an unknown scan as drained or marks a known replay incomplete.
- `runHistorySnapshot` / `drainHistory*` doc comments rewritten to the
  success / incomplete / ctx.Err contract above.
- `history-snapshot-race-plan.md` §2/§4/§5 rewritten: the always-armed backstop +
  the known-stall-→-incomplete-error contract, and the goleak gate text reconciled
  to the repo-wide no-goleak convention (deterministic fake-iterator leak tests),
  matching `history-snapshot-race-review.md`.

Gates re-run green after the changes: `go test ./...`, `go test -race
./cmd/tether/...`, `golangci-lint run` (0 issues), `make e2e`.
**Re-review requested.**

---

## Reviewer final re-review (2026-06-18)

**RESULT: PASS for the current working tree.**

Re-review scope: the maintainer response appended above plus the current
working-tree changes in `cmd/tether/history.go`,
`cmd/tether/history_race_test.go`, and
`docs/reviews/history-snapshot-race-plan.md`.

F1 is resolved. The known-size drain contract now has a real non-success path:
`drainHistorySnapshot` and `drainHistoryFilteredTail` share a `complete()`
predicate (`drained || known && seen >= pending`) in both the normal receive
branch and the idle `tryRecvHist` branch, and `endShort()` converts idle or
iterator-close short reads into `errHistoryIncomplete`. A known backlog can no
longer return `nil` before reaching its deterministic bound.

F2 is resolved. The `historyFirstMsgGrace` comment, drain comments, and plan now
describe the always-armed idle backstop and the known-stall → incomplete-error
contract. The plan's obsolete goleak requirement is reconciled to the repo's
deterministic fake-iterator leak tests.

Additional note: the remediation is currently in the unstaged working tree, not
fully in the staged snapshot. Before closeout, stage the worktree updates to
`cmd/tether/history.go`, `cmd/tether/history_race_test.go`,
`docs/reviews/history-snapshot-race-plan.md`, and this external review report.

Verification run by this reviewer:

```text
git diff --check
git diff --cached --check
go test ./cmd/tether
go test -race ./cmd/tether -run 'History|DrainHistory|FeedHistory'
go test ./internal/auth/... ./internal/authcallout/...
```

All commands passed. I did not rerun `make e2e` or lint in this re-review.
