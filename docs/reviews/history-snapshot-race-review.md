# history-snapshot-race — internal review (round 1) + dispositions

Focused adversarial internal review (3 lenses: concurrency / behavior / tests +
synthesis), run via Workflow. Experts read the implementation and proposed
findings + tests; **only the main process modified implementation** (CLAUDE.md
§4/§5). Verdict: CHANGES REQUESTED. All blockers actioned; dispositions below.

## Must-fix (blockers) — all ADOPTED

### #1 `drainHistoryFilteredTail` (and the default `--kind` snapshot) could fail to terminate — ADOPTED
The `--kind` paths run with `known=false, pending=0`, so the count bound was dead
and the first-message guard was nil-ed after the first item. On an actively
(matching-)written session the per-filter `NumPending` may never settle to 0, so
no item is tagged `drained` → the loop's only exits were channel-close / ctx →
it could hang (the very "completion signal can fail to fire" failure the fix
targets, relocated to the filtered path). The default `--kind` snapshot
(`drainHistorySnapshot`, `known=false`) shared the gap.

**Fix applied** — and extended past the reviewer's framing: the `idleGuard` is
now an **always-armed, generous (5s), rolling** anti-hang backstop for BOTH
drain loops, reset on every item. Completion stays driven by the fast signals
(per-message `drained`, and `seen>=pending` when known); the idle only fires
when delivery has been quiet for `grace`. This also closes a symmetric gap the
review did not name: a **known-size** read whose delivery STALLS mid-backlog
(broker/tunnel reconnect, or a stream that shrank) — previously it would wait on
ctx forever; the old code's 250ms idle used to bound exactly that. Because grace
is generous it never truncates a still-flowing backlog nor a slow remote first
message, and the empty case is short-circuited (`known && pending==0`) before the
drain so it never waits. The idle case also does a non-blocking `tryRecvHist`
peek so a buffered item that races the timer is never dropped (folds in the
optional select-fairness finding).
Guard tests: `TestDrainHistoryFilteredTailTerminatesWithoutDrained`,
`TestDrainHistorySnapshotUnknownTerminatesWithoutDrained`.

### #2 `-n N` unfiltered start-seq path had ZERO coverage — ADOPTED
The single most-broken user-facing shape (`-n 5/20/50`) was the least-tested.
**Added** `TestHistoryStartSeqTailReplaysExactlyLastN` (real embedded NATS):
derives bounds via the production `historyReplayBounds`, builds the actual
`DeliverByStartSequencePolicy` consumer at `OptStartSeq`, asserts exactly the
last N print and runtime << grace (so NumPending/count, not a timer, drove
completion). Variants N<Msgs and N>=Msgs.

### #3 `newHistoryCmd` `Info()→known/pending` derivation untested — ADOPTED
**Refactored** the derivation (start-seq math + known/pending mapping) into a
pure `historyReplayBounds(kind, lastN, streamMsgs, lastSeq)` and table-tested it
(`TestHistoryReplayBounds`): empty→known/0, unfiltered N>Msgs→known/Msgs,
unfiltered N<Msgs→known/N, kind+nonempty→!known/0, etc.

## Optional — dispositions

- **select-fairness truncation (known=false)** — ADOPTED, folded into #1 via the
  `tryRecvHist` peek in the idle branch.
- **`runHistoryFollow` watcher goroutine leak (pre-existing)** — ADOPTED (cheap,
  same file): derive a `cctx`/`cancel`, `defer cancel()` so the watcher exits on
  a non-ctx `Next()` error too.
- **`--kind transfer` untested + too-loose enum assertion** — ADOPTED: added
  `transfer` to the valid-values loop and tightened the negative assertion to the
  full message `must be one of: call | proc | port | transfer`.
- **uint64→int overflow in `seen >= int(pending)`** — ADOPTED: compare in uint64
  (`uint64(seen) >= pending`), dropping the 32-bit caveat.
- **plan vs no-goleak convention** — ADOPTED (doc): the plan's "goleak mandatory"
  line is reconciled — the repo deliberately avoids goleak
  (`test/concurrency/helpers_test.go`), and feedHistory's exit paths are pinned by
  deterministic fake-based tests instead. Plan amended.

## Rejected lens suggestions — AGREE with synthesis

- **Add goleak** — rejected: violates the documented repo-wide no-goleak
  convention; deterministic fake tests are the sound substitute.
- **A separate "keep writing non-matching" filtered timing test** — rejected as
  redundant: non-matching writes don't change a `--kind` filter's `NumPending`, so
  the scenario reduces to #1's guard test.

## Added tests beyond the review

- `TestFeedHistoryMetadataErrorNotDrained` — pins the `msg.Metadata()` error
  branch (drained=false, item still forwarded).
- `TestDrainHistorySnapshotKnownCountBoundIsFastPath` — count bound terminates a
  known backlog without any drained flag, well before the generous idle.

## Real-hardware validation (broker `wss://weiland.top`, session `lab`)

Ground-truth probe: the `history-lab` stream is stable at **296,159 messages**
(the session's original "1113" reading was the OLD idle race truncating a replay
at a >250ms gap — the stream was always large). Final headline, `-n 5`, 30 runs:
**NEW 30/30 returns exactly the correct 5 entries; OLD 23/30 non-empty (7
blank).** The residual variance is the v0.3.4 reverse-tunnel's intermittent
reachability (benign reconnect noise on stderr), which affects old and new
equally and is orthogonal to this fix.
