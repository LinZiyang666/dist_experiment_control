# history-snapshot-race — plan

> Leaf increment (post-1.0). Branch `phase/history-snapshot-race`. Trimmed
> workflow (no multi-expert plan fan-out): main-process plan → implement +
> adversarial tests → one focused internal review → external review → commit.

## 1. Problem (root cause, reproduced on real hardware)

`tether history` against the remote broker (`wss://weiland.top`, session `lab`,
~1113 audit entries) is **flaky / frequently prints nothing**. Repro on pc732/
weiland.top:

- `tether history` (full) → 1113 lines, but occasionally needs a retry.
- `tether history -n 20` → 3/6 runs printed 0 lines, 3/6 printed data.
- `tether history -n 5 / -n 1 / -n 50` → 0 lines even after 4 retries.
- `-n 5` when it *does* win → exactly the last 5 entries (so `-n` math is fine).

Root cause in `cmd/tether/history.go`: `runHistorySnapshot` and
`runHistoryFilteredTail` decide "snapshot complete" with a **250 ms idle
window that starts counting at function entry, before the first message
arrives**. On a high-latency / WSS broker the first message (consumer create +
seek + RTT) often lands after 250 ms, so the idle timer fires first and the
command returns having printed nothing. The full-replay path survives more
often only because ~1113 messages flood in and keep resetting the timer; the
`-n N` path seeks near the tail, delivers only a handful, and almost always
loses the race.

This is a latency-sensitivity bug, not an empty stream and not a config issue.
The audit history is intact.

## 2. Fix — replace the wall-clock idle "completion" signal with a deterministic one

JetStream already tells us authoritatively when a consumer has drained its
backlog: `NumPending` (both `ConsumerInfo.NumPending` and per-message
`MsgMetadata.NumPending`) is **filter-aware** ("messages matching the
consumer's filter not yet delivered"). The repo already uses
`meta.NumPending == 0` as the drain signal in `test/p7/audit_e2e_test.go`.

New completion logic for the snapshot + filtered-tail paths:

1. **Up-front empty / size check from the STREAM** — `stream.Info().State.Msgs`,
   read once in `newHistoryCmd`. This yields `(known, pending)` passed into the
   run functions:
   - unfiltered (full / `-n` no `--kind`): `known=true`, `pending` = total (or
     `min(N, Msgs)` for `-n`); every stream message is delivered.
   - totally empty stream (`Msgs == 0`): `known=true`, `pending=0` regardless of
     filter → return immediately.
   - non-empty `--kind`: `known=false` — we can't count matching messages
     without scanning, so the drain leans on per-message `NumPending` + the
     first-message grace.

   > **Design correction found during implementation** (architecture rule:
   > document first): the original plan used `cons.Info(ctx).NumPending` on the
   > ordered consumer for this check. That is **unusable** —
   > `orderedConsumer.Info()` returns `ErrOrderedConsumerNotCreated` until the
   > first read (verified in nats.go v1.52 `jetstream/ordered.go`), and calling
   > it before `Messages()` mis-detects the backlog. The size therefore comes
   > from the *stream*, never the consumer. Real-broker repro confirmed the
   > consumer-Info approach was flaky; the stream approach is 30/30 stable.

2. **Drain stop** — read messages; stop the instant a delivered message reports
   `NumPending == 0` (the last backlog entry, filter-aware), or once `seen`
   reaches the up-front `pending` (only when `known`) as a belt-and-braces bound
   against a never-zero `NumPending` on a continuously-written stream.
3. **Always-armed rolling idle backstop** (`idleGuard`, `historyFirstMsgGrace =
   5s`) — armed for BOTH known and unknown drains and reset on every item. It is
   the anti-hang backstop, never the fast completion signal, and being generous
   (>> realistic RTT) it cannot be tripped by a slow first/next message nor
   truncate a still-flowing backlog. When it DOES fire (delivery quiet for the
   grace):
   - **unknown size** (`--kind` on a non-empty stream): treat as
     caught-up/quiesced → success (the matching scan is done).
   - **known size, `seen >= pending`**: success.
   - **known size, `seen < pending`**: the replay STALLED short of its proven
     bound → return a non-nil `errHistoryIncomplete` ("delivered N of M …"),
     never a silent partial/empty success. (External-review F1.) The same
     incomplete-vs-success decision applies when the consumer iterator ends
     (channel close) short of a known bound.
4. **Ctrl-C / ctx** — unchanged escape; filtered-tail flushes its ring on every
   return (success, incomplete, or ctx).

The wall-clock idle is therefore an anti-hang/quiesce backstop only — completion
of a known backlog is decided by the deterministic `drained`/`seen>=pending`
bound, and a known backlog can never silently truncate: it either completes or
errors.

## 3. Shape of the change (`cmd/tether/history.go`)

- Add `type histItem { subject string; data []byte; drained bool }`.
- Add `feedHistory(ctx, it MessagesContext, ch chan<- histItem)` — single reader
  goroutine; computes `drained` from `msg.Metadata().NumPending == 0`; sends on
  `ch` with a `select` on `ctx.Done()` so it can never leak when the consumer is
  blocked on send (paired with a `cancel()`-on-return in the caller).
- Extract the buggy select-loops into **testable cores** that take a plain
  `<-chan histItem` (no JetStream types), so the latency race is unit-testable
  with a hand-fed channel and no fake `jetstream.Consumer`:
  - `drainHistorySnapshot(ctx, ch, out, known, pending, grace)`
  - `drainHistoryFilteredTail(ctx, ch, out, n, known, pending, grace)`
- `runHistorySnapshot(ctx, cons, out, known, pending)` /
  `runHistoryFilteredTail(ctx, cons, out, n, known, pending)` — the `idle
  time.Duration` param is **gone**; `known/pending` are supplied by the caller
  (from `stream.Info()`), and the funcs wire `feedHistory` + the drain core with
  `historyFirstMsgGrace`. They short-circuit `known && pending == 0` without
  touching the consumer.
- `newHistoryCmd` reads `stream.Info()` once for the non-follow paths and derives
  `known/pending` (drops the old per-branch `250ms` arg).
- `runHistoryFollow` unchanged (no idle; already ctx-driven).

## 4. Tests (adversarial; `-race` + count-based goroutine-leak check)

> **Convention note**: the repo deliberately does NOT use the `go.uber.org/goleak`
> library (`test/concurrency/helpers_test.go` — avoid the dep; hand-rolled
> poll-with-tolerance instead). Leak-safety of the one goroutine this change adds
> (`feedHistory`) is pinned by deterministic fake-based exit tests
> (`TestFeedHistoryExitsOnIteratorStop` / `...OnCtxCancelWhileSending`), which are
> stronger here than a goleak baseline polluted by the embedded server's
> per-consumer goroutines. No goleak dependency is added.

Existing 3 (`history_paths_test.go`, `history_risk_test.go`) updated for the new
signatures; their assertions (replays all / empty exits clean / filtered count)
must still pass.

New (drain cores fed by hand-built channels — latency race unit-testable with
no fake `jetstream.Consumer`):
- **slow first message within a generous grace** (`known=true`, first item
  delayed 400ms, grace 2s) → all N printed: a slow remote first message is never
  mistaken for end-of-backlog.
- **known count bound is the fast path** (no `drained` flag) → returns at
  `seen>=pending`, well before the idle grace, ignoring later buffered items.
- **stops exactly on `drained`** even with more items buffered (no over-read).
- **unknown-size terminates without `drained`** (snapshot + filtered): rolling
  idle quiesce → success, printing what arrived.
- **known-size stall → `errHistoryIncomplete`** (snapshot + filtered, F1): no
  item before the grace → non-nil error, empty/partial output but never silent
  success; a partial stall names the shortfall ("2 of 5").
- **`feedHistory` leak-safety**: deterministic fake `MessagesContext`/`Msg` —
  the reader goroutine exits on iterator stop and on ctx-cancel-while-sending;
  metadata-error tags `drained=false` and still forwards.
- **`historyReplayBounds` table test**: the `Info()→known/pending/start-seq`
  derivation (empty / `-n`<Msgs / `-n`≥Msgs / `--kind` ± empty).
- **real embedded NATS** (`testharness.StartJSNATS`): empty snapshot prints
  nothing; default replays ALL; `--kind -n` count correct (existing); the `-n`
  unfiltered **start-seq** path replays exactly the last N (and N≥Msgs → all),
  bounded runtime proves NumPending/count not the grace drove completion.

> Leak-safety uses the repo's count-based / fake-iterator approach, NOT the
> goleak library (see the convention note above) — no new dependency.

## 5. Gates

`make test`, `make e2e` (history lives in the p7 matrix path), `make lint`
(golangci-lint v2), and `go test -race ./cmd/tether/...`. All green; the
concurrency face is covered by `-race` plus the deterministic fake-iterator
leak tests (no goleak dependency, per repo convention).

## 6. Out of scope

- No wire/proto change (proto stays v1).
- No change to broker-side audit emission or stream config.
- `--follow` semantics unchanged.
