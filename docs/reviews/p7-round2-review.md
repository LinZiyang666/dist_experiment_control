# P7 Round 2 Review

Date: 2026-05-09
Reviewer role: test engineer

## Scope

Reviewed commit:

- `0c696b0 Address P7 review: history filter+lastN (F1) + audit schema alignment (F2)`

Focus:

- P7 review findings in `docs/reviews/p7-review.md`
- `tether history --kind ... -n N` filtered tail behavior
- persistent `audit.proc` schema compatibility
- P7 regression impact across history, JetStream audit, session rm, and sys.events

## Verdict

P7 is approved.

Both P7 review findings are fixed and verified. I did not find a new
P7-blocking issue in this round.

## Verified Fixes

### F1 - `history --kind ... -n N` now counts after filtering

`cmd/tether/history.go:84-124` now dispatches the four cases separately:

- `--follow` uses `DeliverNewPolicy`;
- unfiltered `-n N` keeps the efficient `LastSeq - N + 1` shortcut;
- filtered `-n N` uses `DeliverAllPolicy` with `FilterSubjects`, then
  `runHistoryFilteredTail` keeps the last N matching messages;
- unbounded snapshot replays all matching messages.

The reviewer risk test now passes repeatedly:

```text
go test ./cmd/tether -run TestHistoryKindTailCountsFilteredEntries -count=20 -v
# PASS
```

### F2 - `audit.proc{kind:"exit"}` now matches `schema.AuditProc`

The broker now publishes audit envelopes using `internal/schema` types:

- `schema.AuditCall` in `internal/broker/exec.go:281-287`
- `schema.AuditProc` in `internal/broker/exec.go:299-315`
- `schema.AuditPort` in `internal/broker/expose.go:363-368`

`audit.proc` exits now publish `rc`, and `tether history` renders `raw["rc"]`.
The reviewer schema test now passes repeatedly:

```text
go test ./test/p7 -run TestAuditProcExitMatchesPublishedSchema -count=10 -v
# PASS
```

## Verification

Commands requiring embedded NATS or JetStream were run outside the default
sandbox because the sandbox blocks loopback listeners.

```text
go test ./cmd/tether -count=1 -v
# PASS

go test ./test/p7 -count=1 -v
# PASS, 9 tests

go test ./internal/schema ./internal/broker ./internal/jsstream ./internal/authcallout -count=1
# PASS

go test ./... -count=1
# PASS: cmd/tether, internal/*, test/p1, test/p2, test/p3, test/p4, test/p5, test/p6, test/p7

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

- `runHistoryFilteredTail` currently uses a separate `doneCh` instead of
  closing/draining the data channel. I could not reproduce message loss
  (`-count=20` passed), so this is not a blocker, but a small cleanup would make
  the end-of-stream path obviously drain-safe.
- `schema.AuditCall` includes `req_id`, but current publishers leave it empty.
  This is not new in the P7 fix and does not block replay/audit visibility, but
  generating stable request IDs would make history entries easier to correlate.

## Recommendation

P7 can be promoted. Keep the two reviewer risk tests in the suite before
starting P8.
