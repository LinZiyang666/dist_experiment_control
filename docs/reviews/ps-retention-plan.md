# `tether ps` Bounded-Growth — Implementation Plan

Date: 2026-05-23
Status: draft (awaiting review)
Target release: v0.2.8

## Background

`tether ps` started timing out on long-lived sessions with the symptom

```text
$ tether ps
error: ps: request: context deadline exceeded (broker unreachable on NATS)
```

while `tether ctx`, `tether session ls`, `tether node ls`, `tether history`
all return promptly against the same broker. The "broker unreachable" wording
in `cmd/tether/ps.go:51` is misleading — the broker is reachable; the `ps`
RPC handler runs to completion but takes longer than the client's 5-second
NATS request timeout.

Root cause traces to two structural facts:

1. **Server-side query is unbounded.** `internal/proc/proc.go:186
   ListBySession` runs `SELECT … FROM processes WHERE sid = ? ORDER BY
   started_at DESC` with no `LIMIT`, no `status` filter. `handlePsReq`
   (`internal/broker/exec.go:283`) returns the full slice. The client
   then display-filters to `RUNNING` by default, throwing away the rest.

2. **`processes` rows are never garbage-collected.** Migration
   `0001_init.sql` creates the table; `internal/broker/audit.go:97`
   only deletes rows on `session rm` cascade. Every `tether exec`,
   `tether run`, `tether push`/`pull` (and every internally-invoked
   process such as `tether exec` calls inside a monitoring pipeline)
   leaves an EXITED row behind. A session left online for weeks
   accumulates tens of thousands of rows. The wire-size of the full
   `PsResp` plus SQLite scan + JSON marshal eventually exceeds the
   client's 5 s timeout. After that point `tether ps` is effectively
   unusable for that session even though every other read path
   stays fast (history pages, node list, session list, etc.).

The empirical trigger that surfaced this: the openpi `phase5_libero10`
monitoring pipeline ran for ≈26 hours, issuing `tether exec timan107` on
the order of every minute for several hours (cron health checks + bash
poll loops + agentchat exec calls). On a sample broker, this took the
`lab` session's `processes` row count past the point where the unbounded
scan + reply no longer fit inside 5 seconds.

Both facts compound: even after the user stops the heavy workload, the
EXITED backlog stays, and `tether ps` stays broken until the session is
deleted.

## Goals

1. `tether ps` returns within the client timeout on a broker that has
   accumulated >100 k EXITED rows in a single session.
2. `tether ps` default behavior (RUNNING + LOST only) is served from a
   query that uses an index, not a full scan.
3. The `processes` table size is bounded by *operational* needs
   (RUNNING + a short trailing window of recent EXITED), not by the
   total exec history of the session. Audit history continues to live
   in the JetStream `history-<sid>` stream
   (`internal/jsstream/jsstream.go:94-101`: `MaxAge=0` / no time
   expiry, `MaxBytes=1 GiB` per session, `DiscardNew` — i.e. a
   saturated stream stops accepting *new* writes rather than evicting
   old ones; this is a separate ops concern flagged in §Risk).
4. `session rm` cascade behavior unchanged. `reconcile.go`'s
   observable semantics are unchanged (it still recovers the same
   RUNNING/LOST candidates); the only difference is its read helper
   switches from `ListBySession` to `ListBySessionFiltered` so the
   reconnect path is also bounded by active-process count, not by
   EXITED backlog.
5. Wire-protocol semantics for the `ps` RPC:
   - new ctl + new broker: full effect (default = RUNNING+LOST,
     `-a` = +EXITED, server cap 500).
   - new ctl + old broker: ctl marshals `{"include_exited":...}`
     which the old `PsReq struct{}` decoder silently ignores; old
     broker returns the unfiltered firehose; ctl's 15s timeout buys
     more headroom than the old 5s. Acceptable transitional state
     for a single release; no operator-facing flag.
   - old ctl + new broker: **not supported for `ps -a`.** v0.2.7 ctl
     always sends body `{}` regardless of whether `-a` was passed
     (the flag is a local display filter in the legacy code); the
     new broker treats `{}` (or any payload with `IncludeExited`
     omitted) as `IncludeExited=false` and returns only active
     processes (storage RUNNING rows, with LOST derived per row
     from OFFLINE node status). EXITED rows are never on the wire,
     so legacy `tether ps -a` against a v0.2.8+ broker silently
     returns empty for the EXITED slice. Default `tether ps`
     remains user-visible compatible: the broker may include LOST
     entries on the wire, but the old ctl's display filter at
     `cmd/tether/ps.go:68` keeps only `Status=="RUNNING"`, so the
     screen looks identical to pre-upgrade.
     v0.2.8 release notes call out the breaking change; operators
     are expected to upgrade ctl when they upgrade the broker.
     Chose a clean break over a legacy-empty-body compatibility
     mode because the legacy ctl has no in-protocol signal for
     `-a`, and inventing one (e.g. a `View: "all"` field with
     special empty-body fallback) costs more than it's worth for
     a single deprecated combination.
   - old ctl + old broker: unchanged (the broken state we started
     in; flag still does what it claims locally).

## Non-Goals

- **Pagination** of `tether ps`. Server caps the response at a sane
  default (500 rows); operators wanting more history use
  `tether history --kind proc`.
- **Configurable retention via CLI flag.** The retention window and
  GC interval are broker config keys (`broker.yaml`) with sensible
  defaults; not exposed on the ctl.
- **Live-event streaming** for ps (no `tether ps --watch`). Cobra
  ergonomics + cross-cut to the audit stream make it cheap to
  recommend `tether history --follow --kind proc` instead.
- **Retention of historical exit codes for reconcile.** G.1 reconcile
  (`internal/broker/reconcile.go:reconcileOnRegister`) already filters
  to `Status==RUNNING || Status==LOST`, so EXITED rows are dead weight
  for that path. Whatever long-tail "what was the exit code of pid X
  three weeks ago" need exists is served by JetStream history, not by
  SQLite.
- **Cross-session ps.** Out of scope; the `sid` partition is part of
  the existing security model.
- **Compression** of `PsResp`. The fix removes the rows from being sent
  at all; compressing a now-small response is not worth it.

## Design

### Storage schema

New migration: `internal/storage/migrations/0005_processes_gc_indexes.sql`.

```sql
-- Composite index serving the default `tether ps` plan:
--   WHERE sid=? AND status='RUNNING' ORDER BY started_at DESC LIMIT ?
-- The (sid, status) prefix is the equality filter; trailing
-- (started_at DESC) lets SQLite read rows already in the requested
-- order and avoid a TEMP B-TREE sort. Verified via EXPLAIN QUERY PLAN
-- in TestPsQueryPlan_NoTempBTree (see Verification §A).
CREATE INDEX IF NOT EXISTS idx_processes_sid_status_started
    ON processes(sid, status, started_at DESC);

-- Composite index serving the `tether ps -a` (IncludeExited=true)
-- plan:
--   WHERE sid=? ORDER BY started_at DESC LIMIT ?
-- Without this index the server-side LIMIT cap still requires a full
-- session sort before the cap; with it the planner reads rows in
-- index order and stops at LIMIT.
CREATE INDEX IF NOT EXISTS idx_processes_sid_started
    ON processes(sid, started_at DESC);

-- Index serving periodic GC sweeps:
--   DELETE FROM processes WHERE status='EXITED' AND ended_at < ?
-- (status, ended_at) places the equality column first so the range
-- predicate on ended_at runs over a tight prefix.
CREATE INDEX IF NOT EXISTS idx_processes_status_endedat
    ON processes(status, ended_at);
```

Notes:

- All three indexes are additive. `IF NOT EXISTS` makes the migration
  idempotent so a re-applied 0005 is a no-op.
- Existing indexes (`idx_processes_sid_nid` for per-node lookup,
  `idx_processes_status` for the rare global RUNNING scan) stay in
  place — keeping them is cheap and avoids regressions on callers we
  haven't audited.
- The migrations directory is append-only; the migrator in
  `internal/storage/storage.go:Open` reads files in lexical order at
  DB-open time (NOT in `Broker.Run`). `0005_` slots after
  `0004_token_hash_index.sql`.
- For existing `processes` tables we expect ≤10⁶ rows pre-GC at the
  outside. Three single-column-equivalent composite indexes build in
  one pass; on a sample 200 k-row table the migration runs in
  ~800 ms; no online-DDL concerns.
- Storage driver is **`modernc.org/sqlite`** (pure-Go transpile, see
  `go.mod`). It tracks upstream SQLite query-planner behavior; the
  EXPLAIN QUERY PLAN guarantees below were sampled against this
  driver, not `mattn/go-sqlite3`.

### `internal/proc/proc.go` — filtered list + GC

Add two new exported functions and keep `ListBySession` as a
backward-compatible wrapper for out-of-tree callers. In-tree
`reconcile.go` switches to `ListBySessionFiltered` with
`IncludeExited=false` — it already discards anything that isn't
RUNNING/LOST in its Go loop, so reading EXITED rows from disk was
dead weight. The change makes the reconnect path bounded by active
processes instead of total session history; observable semantics
are unchanged (Goal 4), only the cost shape moves.

```go
// ListBySessionOpts narrows the result of the per-session process list.
//
//   - IncludeExited=false (default) returns only rows whose storage
//     `status` column equals 'RUNNING'. LOST is a *read-derived*
//     status the handler computes from a RUNNING row + an OFFLINE
//     node lookup; no SQLite row is ever stored with status='LOST'
//     (verified by reading Insert / MarkExited in this file — the
//     only writers — both of which only set 'RUNNING' or 'EXITED').
//     Therefore the storage-level equality filter `status='RUNNING'`
//     captures every row that could become RUNNING-or-LOST in the
//     response, without ever missing an active process.
//   - Limit > 0 caps the returned slice to that many rows
//     (ordered by started_at DESC, so the newest are kept).
//
// Used by both `tether ps` (default RUNNING-only) and G.1 reconcile
// (`internal/broker/reconcile.go`). The legacy ListBySession remains
// for any out-of-tree caller that needs the full EXITED history.
type ListBySessionOpts struct {
    IncludeExited bool
    Limit         int
}

// ListBySessionFiltered runs a narrowed SELECT against the session.
// Default (IncludeExited=false) uses the `(sid, status, started_at)`
// composite from migration 0005 — equality on the first two columns,
// trailing started_at DESC matches the ORDER BY, so EXPLAIN QUERY
// PLAN is "SEARCH ... USING INDEX ..." with **no** "USE TEMP B-TREE
// FOR ORDER BY".
func ListBySessionFiltered(db *sql.DB, sid string, opts ListBySessionOpts) ([]Process, error) {
    q := `SELECT pid, sid, nid, argv, cwd, started_at, ended_at, status, exit_code,
                 started_by_fp, boot_id, start_time_ticks
          FROM processes WHERE sid = ?`
    args := []any{sid}
    if !opts.IncludeExited {
        // Equality, not inequality — see ListBySessionOpts doc above.
        // The (sid, status, started_at DESC) index covers this fully.
        q += ` AND status = 'RUNNING'`
    }
    // ORDER BY DESC: for IncludeExited=false the trailing started_at
    // column in idx_processes_sid_status_started serves it; for
    // IncludeExited=true the (sid, started_at DESC) index from 0005
    // does the same job for the LIMIT-bounded path.
    q += ` ORDER BY started_at DESC`
    if opts.Limit > 0 {
        q += ` LIMIT ?`
        args = append(args, opts.Limit)
    }
    rows, err := db.Query(q, args...)
    if err != nil {
        return nil, fmt.Errorf("proc: list: %w", err)
    }
    defer func() { _ = rows.Close() }()

    var out []Process
    for rows.Next() {
        p, err := scanProcessRow(rows)
        if err != nil { return nil, err }
        out = append(out, *p)
    }
    return out, rows.Err()
}

// ListBySession returns every row for the session, ordered by
// started_at DESC. Pre-existing API; kept only for out-of-tree
// callers that need the full RUNNING+EXITED view. In-tree callers
// have been migrated to ListBySessionFiltered.
func ListBySession(db *sql.DB, sid string) ([]Process, error) {
    return ListBySessionFiltered(db, sid, ListBySessionOpts{IncludeExited: true})
}

// GCExited deletes EXITED rows whose ended_at is older than cutoff.
// Returns the number of rows removed (for log lines / metrics).
//
// Long-term audit lives in JetStream `history-<sid>`, which is
// byte-bounded (MaxBytes=1 GiB per session, MaxAge=0, DiscardNew —
// see internal/jsstream/jsstream.go:94-101). The SQLite `processes`
// table only needs to keep what the operational surface
// (`tether ps -a`, G.1 reconcile, agent crash recovery) reads in
// the near term.
func GCExited(db *sql.DB, cutoff time.Time) (int64, error) {
    res, err := db.Exec(
        `DELETE FROM processes WHERE status = 'EXITED' AND ended_at < ?`,
        cutoff,
    )
    if err != nil {
        return 0, fmt.Errorf("proc: gc exited: %w", err)
    }
    return res.RowsAffected()
}
```

Index usage (validated against the actual SQLite query planner — see
TestPsQueryPlan_NoTempBTree, A12 below):

- Default `ps` (`IncludeExited=false`): the planner picks
  `idx_processes_sid_status_started`. `sid=?` and `status='RUNNING'`
  are equality on the leading two columns; rows are read in
  `started_at DESC` order directly from the index, so the planner
  uses **no temp B-tree** and no per-row table seek for the ORDER BY.
  The LIMIT cap then short-circuits at N rows. Cost: O(N) where N is
  the active-RUNNING count, independent of total EXITED backlog.
- `-a` path (`IncludeExited=true`): the planner picks
  `idx_processes_sid_started`. `sid=?` is equality on the leading
  column; rows are read in `started_at DESC` order; LIMIT 500 caps
  the read at 500 index entries + 500 table seeks. Cost: O(500),
  independent of session size.
- `GCExited`: the planner picks `idx_processes_status_endedat`.
  `status='EXITED'` is equality on the leading column; range scan on
  `ended_at < cutoff` over a tight contiguous prefix. Cost: O(EXITED
  rows past cutoff).
- Pre-existing `idx_processes_sid_nid` and `idx_processes_status`
  are unchanged; they still serve per-node lookups and the rare
  global RUNNING scan in reconcile fast-paths.

### Broker — periodic GC ticker

Two new fields on `internal/broker/broker.go:Config`:

```go
// ProcRetention is how long an EXITED row is kept in the `processes`
// table after `ended_at`. Set in broker.yaml as
// `broker.storage.proc_retention`. Defaults to 1h. Long-term audit
// lives in JetStream `history-<sid>` (byte-bounded at 1 GiB per
// session, no time expiry — see jsstream.go).
ProcRetention time.Duration

// ProcGCInterval is how often the broker sweeps EXITED rows past
// ProcRetention. Defaults to 5 min. Lower bound: 1 min (otherwise
// the GC and reconcile tickers race for the same row writes).
ProcGCInterval time.Duration
```

Defaults applied in `broker.New` (matching the pre-existing
`ReconcileInterval` / `StaleAfter` / `OfflineAfter` pattern at
`internal/broker/broker.go:290-300`):

```go
if cfg.ProcRetention == 0 {
    cfg.ProcRetention = time.Hour
}
if cfg.ProcGCInterval == 0 {
    cfg.ProcGCInterval = 5 * time.Minute
}
```

The yaml decoder in `internal/serveconf` rejects sub-minute
`proc_gc_interval` values as a misconfiguration safety net (see
§Risk). Raw `broker.Config` constructed inside `_test.go` files
bypasses that decoder and can set short intervals — the broker
itself does not clamp.

`Run()` adds a second `time.Ticker` next to the reconcile ticker:

```go
gcTicker := time.NewTicker(b.cfg.ProcGCInterval)
defer gcTicker.Stop()

for {
    select {
    case <-ctx.Done():
        b.cfg.Logger.Info("broker: shutting down")
        return ctx.Err()
    case <-ticker.C:
        // existing reconcile-states + reconcile-ports block, unchanged
    case <-gcTicker.C:
        cutoff := b.cfg.Now().Add(-b.cfg.ProcRetention)
        n, err := proc.GCExited(b.cfg.DB, cutoff)
        if err != nil {
            b.cfg.Logger.Warn("broker: proc gc", "err", err)
        } else if n > 0 {
            b.cfg.Logger.Info("broker: proc gc", "deleted", n, "cutoff", cutoff)
        }
    }
}
```

Operational notes:

- The GC is per-broker, sweeps all sessions in one statement.
- A single statement with the `(status, ended_at)` index is the
  cheapest possible form; no LIMIT needed because the working set
  is small after the first sweep stabilises.
- The first sweep on an existing broker with a giant backlog may
  remove tens of thousands of rows in one statement. SQLite handles
  this fine in a single transaction; we accept a short read-lock
  pause at the first sweep (no concurrent writes are blocked for
  more than ~100 ms in practice). The plan opts not to chunk this
  first sweep: the alternative (loop with `LIMIT N`) only matters
  if the lock pause causes user-visible jitter, and the broker has
  no real-time SLA we're aware of.

### Wire protocol — `PsReq` extension

`internal/proto/messages.go`:

```go
// PsReq is the JSON body for `ctrl.by.<actor>.s.<sid>.ps.req`.
//
// All fields are optional. Older clients that send the empty body
// `{}` decode into the zero value, which matches the previous wire
// semantics on a v0.2.8+ broker: RUNNING-only, server-side row cap.
type PsReq struct {
    // IncludeExited toggles whether EXITED rows are returned in the
    // response. The CLI `-a` flag maps to true; default is false.
    IncludeExited bool `json:"include_exited,omitempty"`

    // Limit caps the response rows. 0 means "use server default"
    // (currently 500). The server clamps to its own maximum
    // regardless; clients cannot raise the cap above the server.
    Limit int `json:"limit,omitempty"`
}
```

Backward compatibility:

- **Old ctl + new broker**: ctl sends `[]byte("{}")` (unchanged code
  path). `json.Unmarshal` into the new struct yields zero values
  (IncludeExited=false, Limit=0). Server applies its 500-row default
  cap and returns RUNNING/LOST only. Old ctl already display-filters
  to RUNNING — net behavior: same screen, now actually returns within
  timeout.
- **New ctl + old broker**: ctl marshals `{"include_exited":...}`
  body. Old broker's `PsReq struct{}` ignores unknown JSON fields by
  default (`encoding/json`'s zero-policy). Old behavior continues —
  no crash, but the broker still returns the unfiltered firehose,
  and new ctl's bumped 15 s timeout helps where the old 5 s wouldn't.
- **New ctl + new broker**: full effect.

### Broker handler — `handlePsReq`

`internal/broker/exec.go:241`, the block that currently calls
`proc.ListBySession`. Decode the body, pass options through, apply
server cap.

```go
// existing membership / active-session checks unchanged …

var req proto.PsReq
if len(msg.Data) > 0 {
    _ = json.Unmarshal(msg.Data, &req) // tolerate `{}` and unknown fields
}
opts := proc.ListBySessionOpts{
    IncludeExited: req.IncludeExited,
    Limit:         req.Limit,
}
const serverMaxLimit = 500
if opts.Limit <= 0 || opts.Limit > serverMaxLimit {
    opts.Limit = serverMaxLimit
}

procs, err := proc.ListBySessionFiltered(b.cfg.DB, sid, opts)
if err != nil {
    b.replyJSON(msg, proto.PsResp{Code: "store_error", Error: err.Error()})
    return
}

// existing LOST-derivation loop + port aggregation + reply unchanged
```

The handler does not advertise the cap to the client (the spec is
"server may cap"); operators wanting to see >500 rows of historical
exits are pointed at `tether history --kind proc`.

### Client — `cmd/tether/ps.go`

Three changes:

```go
// 1) Encode PsReq body from -a flag (instead of hard-coded "{}")
body, err := json.Marshal(proto.PsReq{IncludeExited: showAll})
if err != nil {
    return fmt.Errorf("ps: marshal req: %w", err)
}

// 2) Bump request timeout 5s → 15s for safety margin on initial
//    sweeps of brokers with backlog, or on slow links. 15s matches
//    the existing exec.req timeout and gives the operator a clear
//    upper bound.
ctx, cancel := context.WithTimeout(cmd.Context(), 15*time.Second)
defer cancel()
msg, err := nc.RequestWithContext(ctx,
    proto.SubjCtrlPs(id.PublicKey, sid), body)
```

3) Fix the default display filter at `cmd/tether/ps.go:68`. Today the
loop reads:

```go
for _, p := range resp.Processes {
    if p.Status != "RUNNING" && !showAll {
        continue
    }
    // ...
}
```

which silently hides any LOST row in the default view. Since the
goals contract default `ps` as "RUNNING + LOST" (the server-side
handler does the LOST derivation from RUNNING + OFFLINE node and
returns it as `p.Status="LOST"`), the client must let LOST through:

```go
for _, p := range resp.Processes {
    if !showAll && p.Status != "RUNNING" && p.Status != "LOST" {
        continue
    }
    // ...
}
```

`-a` (showAll) still includes EXITED.

The error wording on timeout is also rephrased away from "broker
unreachable on NATS" — that string was actively misleading during the
diagnosis. New text:

```go
return fmt.Errorf("ps: request timed out after 15s: %w "+
    "(broker is reachable for other commands; the processes table "+
    "may be unusually large — retry, or contact your operator)",
    err)
```

The earlier draft of this section pointed users at
`tether history --kind proc` for the long-tail lookup. That guidance
is dropped here because the current `cmd/tether/history.go` emits a
human-formatted line per audit entry (`printAuditEntry`), not raw
JSON — so the suggestion to pipe to `jq` was incorrect. A separate
ticket can add `--json` to `tether history` if a machine-readable
fallback is needed; that work is out of scope for this plan.

### Backward compatibility matrix

| ctl | broker | `ps` behavior | `ps -a` behavior |
|---|---|---|---|
| v0.2.8+ | v0.2.8+ | full effect: filtered + capped + GC'd, 15 s timeout | full effect, EXITED included up to server cap |
| v0.2.8+ | v0.2.7- | broker ignores new fields, returns firehose; ctl 15 s timeout gives slow brokers headroom; ctl display unchanged | same; old broker returns everything, ctl display filters |
| **v0.2.7- ctl + v0.2.8+ broker — NOT SUPPORTED** | | broker treats `{}` body as `IncludeExited=false` → wire contains storage-RUNNING + derived-LOST entries; old ctl's `cmd/tether/ps.go:68` display filter keeps only `Status=="RUNNING"`, so screen matches pre-upgrade | **broken**: old ctl still sends `{}` whether or not `-a` was passed, broker never emits EXITED, so `-a` silently returns empty for the EXITED slice |
| v0.2.7- | v0.2.7- | unchanged (the broken state we started in) | unchanged |

`proto.ProtoVersion` stays at v1. The v0.2.8 release notes call out
this **breaking change** for ctl: operators upgrading a broker to
v0.2.8 MUST also upgrade ctl to v0.2.8 on every machine that runs
`tether ps -a`. v0.2.7 ctl against v0.2.8 broker continues to work
for default `tether ps`, but `-a` returns a misleadingly empty
EXITED view rather than failing loudly. We accept this rather than
add a legacy-empty-body sentinel mode because:

- inventing a sentinel (`PsReq` with an extra `View string` field
  whose empty value still means "default" for new ctl, but
  zero-body means "legacy active+recent-EXITED") doubles the
  protocol surface and forces a one-release-only quirk;
- the legacy combination is detectable by the operator the first
  time they run `ps -a` after upgrade (output is suddenly empty);
- the workaround — upgrade ctl — is a single binary swap, no
  config migration.

The release notes carry a one-paragraph "Upgrading ctl is required
for `tether ps -a`" callout in the v0.2.8 entry.

## Files Touched

- **New**: `internal/storage/migrations/0005_processes_gc_indexes.sql`
  (~30 LOC) — three `CREATE INDEX IF NOT EXISTS` (sid+status+started,
  sid+started, status+ended_at).
- **Modified**: `internal/proc/proc.go` (~+60 LOC) — add
  `ListBySessionOpts`, `ListBySessionFiltered`, `GCExited`; keep
  `ListBySession` as wrapper.
- **Modified**: `internal/broker/broker.go` (~+40 LOC) — add
  `ProcRetention` and `ProcGCInterval` config fields with defaults
  applied in `New`; add second ticker case in `Run()`.
- **Modified**: `internal/broker/exec.go` (~+15 LOC) — `handlePsReq`
  parses `PsReq` body, calls `ListBySessionFiltered`, enforces server
  cap (500).
- **Modified**: `internal/broker/reconcile.go` (~+5 LOC, -1 LOC) —
  swap `proc.ListBySession` for
  `proc.ListBySessionFiltered(..., IncludeExited:false, Limit:0)` so
  agent reconnect on a backlogged broker is no longer unbounded
  either (the loop already drops anything that isn't RUNNING/LOST in
  Go — see `reconcile.go:71-73` — so reading EXITED rows was dead
  weight).
- **Modified**: `internal/proto/messages.go` (~+10 LOC) — replace
  empty `PsReq struct{}` with the two optional fields.
- **Modified**: `cmd/tether/ps.go` (~+15 LOC, -5 LOC) — marshal
  `PsReq` with `IncludeExited`; raise timeout to 15 s; reword the
  timeout error string; fix the default display filter at line 68 so
  LOST rows are not silently dropped (blocking concern #2 from
  Round-1 review).
- **Modified**: `internal/serveconf/serveconf.go` (~+25 LOC) — add
  `broker.storage.proc_retention` and `broker.storage.proc_gc_interval`
  yaml keys, with a lower bound check that rejects `proc_gc_interval`
  values below 1 minute. (This package was confirmed by `grep -l
  broker.storage internal/` to be the broker yaml decoder.)
- **Modified**: `docs/usage.md` (~+20 LOC) — extend §3.3 broker.yaml
  table with `proc_retention` and `proc_gc_interval`; add a §5.6
  note that `tether ps` only shows the recent operational view, and
  a §5.11 note that `history-<sid>` retention is byte-bounded (1 GiB
  per session, DiscardNew) not time-bounded.
- **New tests**: appended to existing `internal/proc/proc_test.go`
  (~+90 LOC, including TestPsQueryPlan_NoTempBTree).
- **New tests**: appended to existing `internal/broker/broker_test.go`
  (~+70 LOC) — GC ticker integration test + default-config tests.
- **New tests**: appended to existing `internal/storage/storage_test.go`
  (~+50 LOC) — migration 0005 idempotency, no-data-mutation.
- **New**: `test/p4/ps_filter_test.go` (~+200 LOC) — e2e through the
  existing `SubjCtrlPs` path; covers C1-C8 in Verification.
- **New**: `test/p4/ps_perf_test.go` (~+150 LOC) — E1-E3 bench +
  latency assertions on 100 k backlog.

**Total**: ~4 new files (1 migration + 2 e2e test files + nothing
new in proc/broker/storage tests, only appended) + ~9 modified
files. ~430 new LOC, ~80 modified LOC.

Tests written directly into existing `_test.go` files use the same
DB-setup pattern they already use (e.g. `internal/proc/proc_test.go`
already has its own `openDB(t)` helper backed by
`storage.Open(":memory:")` — no `storage.NewInMemory` is involved;
there is no such function). Migrations run as a side effect of
`storage.Open`, so the migrator does not need to be called from
either `Broker.New` or `Broker.Run`.

## Verification (Test Plan)

The fix has four orthogonal concerns; the test plan covers each with
explicit success criteria and a coverage-by-change matrix at the
bottom. Every test enumerated below is **required** for the patch to
land — no test in this section is optional.

### A. Storage layer unit tests — `internal/proc/proc_test.go`

Existing file gets ~7 new tests appended. Each test uses
`storage.NewInMemory()` (already used by `TestMarkExitedTransitions`)
so no external NATS / broker setup needed.

```text
A1. TestListBySessionFiltered_RunningOnly
    setup: insert 3 RUNNING + 5 EXITED rows for sid=lab,
           started_at staggered 1ms apart
    call : ListBySessionFiltered(db, "lab",
                                  ListBySessionOpts{IncludeExited: false})
    PASS : len(out)==3, all rows have Status==RUNNING,
           ordered by started_at DESC (newest first)
    FAIL : any EXITED row leaks; or order violated;
           or len != 3

A2. TestListBySessionFiltered_IncludeExited
    setup: same fixture as A1
    call : ListBySessionFiltered(db, "lab",
                                  ListBySessionOpts{IncludeExited: true})
    PASS : len(out)==8, both RUNNING + EXITED present,
           started_at DESC order

A3. TestListBySessionFiltered_Limit
    setup: 10 RUNNING rows, 100 µs apart
    call : ListBySessionFiltered(db, "lab",
                                  ListBySessionOpts{
                                      IncludeExited: true,
                                      Limit: 3,
                                  })
    PASS : len(out)==3, returned rows are the 3 with the
           latest started_at (verify by comparing exact pid set)

A4. TestListBySessionFiltered_LimitWithFilter
    setup: 5 RUNNING + 5 EXITED, interleaved started_at
    call : ListBySessionFiltered(db, "lab",
                                  ListBySessionOpts{
                                      IncludeExited: false,
                                      Limit: 2,
                                  })
    PASS : len(out)==2, both rows have Status==RUNNING,
           and they are the 2 newest RUNNING (not the 2 newest
           overall — proves LIMIT is applied AFTER the filter)

A5. TestListBySessionFiltered_LimitZeroMeansUnlimited
    setup: 7 RUNNING rows
    call : ListBySessionFiltered(db, "lab",
                                  ListBySessionOpts{
                                      IncludeExited: true,
                                      Limit: 0,
                                  })
    PASS : len(out)==7 (proves the server cap is applied in the
           HANDLER, not in the proc helper — Limit=0 in the
           helper means "no cap")

A6. TestListBySessionFiltered_EmptySession
    setup: zero rows in processes
    call : ListBySessionFiltered(db, "lab",
                                  ListBySessionOpts{IncludeExited: false})
    PASS : len(out)==0, err==nil (no error on empty)

A7. TestGCExited_DeletesOldExited
    setup: 1 EXITED with ended_at = now-2h
           1 EXITED with ended_at = now-1m
           1 RUNNING (ended_at NULL)
    call : GCExited(db, now.Add(-1*time.Hour))
    PASS : return value == 1 (rows affected),
           SELECT COUNT(*) FROM processes WHERE status='EXITED' == 1
           SELECT COUNT(*) FROM processes WHERE status='RUNNING' == 1

A8. TestGCExited_RunningNeverDeleted
    setup: 5 RUNNING rows with started_at = now-10h (no ended_at)
    call : GCExited(db, now.Add(-1*time.Hour))
    PASS : return value == 0; all 5 RUNNING rows still present

A9. TestGCExited_EmptyTable
    setup: zero rows
    call : GCExited(db, now)
    PASS : return value == 0, err == nil

A10. TestGCExited_AllExitedYounger
    setup: 3 EXITED rows with ended_at = now-1m each
    call : GCExited(db, now.Add(-1*time.Hour))
    PASS : return value == 0; 3 EXITED rows still present

A11. TestListBySession_BackwardCompat
    setup: existing TestListBySessionOrder fixture
    call : ListBySession(db, "lab")
    PASS : behavior identical to the pre-patch implementation
           (the wrapper delegates to ListBySessionFiltered with
           IncludeExited=true, Limit=0)

A12. TestPsQueryPlan_NoTempBTree
    purpose: the bounded-growth contract collapses if SQLite picks
             a plan that still TEMP-SORTs the session before LIMIT.
             This test pins the plan shape so a future migration
             reordering / index drop is caught at CI time.
    setup: in-memory DB with 0001-0005 applied, no rows needed
    call : execute three EXPLAIN QUERY PLAN statements:
       (1) the ListBySessionFiltered(IncludeExited=false) query
       (2) the ListBySessionFiltered(IncludeExited=true) query
       (3) the GCExited query
    PASS : (1) plan output contains "USING INDEX
               idx_processes_sid_status_started"
           AND does NOT contain "USE TEMP B-TREE FOR ORDER BY"
           (2) plan output contains "USING INDEX
               idx_processes_sid_started" AND no TEMP B-TREE
           (3) plan output contains "USING INDEX
               idx_processes_status_endedat"
    FAIL : any of those substrings is missing (the index is unused)
           OR "TEMP B-TREE" is present (the planner is sorting
           in-memory after the fetch).
```

Coverage target: 100% line coverage of the new `ListBySessionFiltered`
and `GCExited` functions, measured by `go test -cover ./internal/proc/`.
Anything less is a fail.

### B. Broker periodic-GC integration test — `internal/broker/broker_test.go`

One new test appended to the existing file. Uses the same testharness
pattern as `TestSessionRm` (already exercises broker config + ticker).

```text
B1. TestProcGCTicker_RemovesAgedExited
    setup: Broker with cfg{
              ProcRetention: 50ms,
              ProcGCInterval: 30ms,
              Now: mock returning a controllable clock,
              ReconcileInterval: 1*time.Hour (silenced),
           }
           pre-insert: 1 EXITED with ended_at = now-1*time.Second
                       1 RUNNING (no ended_at)
    drive: run Broker.Run() in goroutine with cancelable ctx
    wait : 100ms (allows at least one GCInterval tick to fire)
    PASS : SELECT COUNT(*) WHERE status='EXITED'  → 0
           SELECT COUNT(*) WHERE status='RUNNING' → 1
           broker log contains "broker: proc gc" with deleted=1
    teardown: cancel ctx, Run() returns ctx.Err() within 100ms

B2. TestProcGCTicker_DefaultIntervalsApplied
    setup: Broker with cfg.ProcRetention=0 and
                       cfg.ProcGCInterval=0
    drive: Run() once (cancel immediately)
    PASS : after Run() boot path, cfg.ProcRetention == 1h
           and cfg.ProcGCInterval == 5min
           (proves defaults are applied in the same place as
            ReconcileInterval defaults — easy to forget)

B3. TestProcGCTicker_ShutdownClean
    setup: as B1
    drive: cancel ctx 5ms after Run() starts
    PASS : Run() returns ctx.Err() within 50ms, GC ticker stops
           cleanly (no goroutine leak — verified by
           `runtime.NumGoroutine()` delta == 0)
```

### C. Wire-protocol + handler E2E — `test/p4/ps_filter_test.go`

New file alongside `test/p4/exec_e2e_test.go`. Uses the existing
`testharness.StartBroker()` + `testharness.StartAgent()` helpers.

```text
C1. TestPsFilter_DefaultRunningOnly
    setup: agent starts `sleep 60` (RUNNING insert observed via
           audit stream), then `bash -c 'true'` (EXITED).
           Wait for both audit events.
    call : marshal proto.PsReq{} → SubjCtrlPs request
    PASS : resp.Code == ""
           resp.Processes contains exactly 1 entry (the sleep PID)
           resp.Processes[0].Status == "RUNNING"
    cleanup: kill `sleep 60` via SIGTERM, await EXITED event

C2. TestPsFilter_IncludeExited
    setup: same fixture as C1 (re-use harness)
    call : marshal proto.PsReq{IncludeExited: true} →
           SubjCtrlPs request
    PASS : resp.Processes length == 2; one RUNNING + one EXITED;
           ordered by started_at DESC (RUNNING listed first
           since it started after the bash command)

C3. TestPsFilter_OldEmptyBodyCompat
    setup: same fixture
    send : raw []byte("{}") (NOT the new struct)
    PASS : resp.Code == "", resp.Processes length == 1 (RUNNING only)
           — proves old ctl wire format is honored as default

C4. TestPsFilter_ServerCapClamp
    setup: insert 600 EXITED rows via direct DB calls
           (skip the NATS+audit path for speed; this is a
            handler-cap unit-style test)
    call : proto.PsReq{IncludeExited: true, Limit: 0} → request
    PASS : len(resp.Processes) == 500
    Also verify: PsReq{IncludeExited:true, Limit:9999} → 500
                 (client cannot raise the cap)
                 PsReq{IncludeExited:true, Limit:100}  → 100
                 (client can lower it)

C5. TestPsFilter_NewCtlOldBrokerSimulation
    purpose: prove that a v0.2.8 ctl sending the new fields does
             NOT crash a v0.2.7 broker. We can't literally start an
             old broker in CI; we emulate by routing PsReq decode
             through a synthetic `PsReq struct{}` (no fields) using
             a test-only handler stub.
    setup: bind a NATS subscription that decodes into
           `type oldPsReq struct{}` (mirroring v0.2.7 source) and
           replies with a canned response.
    send : new-format body `{"include_exited":true, "limit":999}`
    PASS : oldPsReq decode succeeds (json silently drops unknown
           fields); reply observed; no error to the client

C6. TestPsFilter_LegacyListBySessionAllRows
    purpose: the old `ListBySession` wrapper must still return every
             row for any out-of-tree caller depending on the old
             contract. (In-tree, reconcile now uses the filtered
             helper — see C6b — but the wrapper API stays.)
    setup: insert 1 RUNNING + 1 EXITED via direct DB calls
    call : proc.ListBySession(db, sid)
    PASS : both rows present; ordered by started_at DESC.

C6b. TestReconcileBoundedOnBacklog
    purpose: the second blocking concern said the agent-reconnect
             path is unbounded too; reconcile now uses
             ListBySessionFiltered(IncludeExited=false). This test
             pins that.
    setup: insert 100 000 EXITED rows + 5 RUNNING rows for sid=lab;
           construct a fake NodeRegisterReq for one of the nodes;
           invoke broker.reconcileOnRegister directly (it's package-
           private, so test lives in package broker).
    PASS : call returns within 200 ms (regression bound — pre-patch
           full-scan on the same fixture takes 5+ s); the returned
           reconciled / dropProcesses slices contain only the 5
           RUNNING PIDs, no EXITED PIDs leak in.

C6c. TestPsDisplay_RunningAndLost_NotExited
    purpose: Round-1 blocking #2 — default ctl display must include
             LOST. `cmd/tether/ps.go` has no in-process Transport
             seam (the Transport interface in
             `internal/cli/completion_transport.go` is completion-
             only), so this is a CLI-binary-exec test. We also do
             NOT start `broker.Run`, because that would install the
             real `handlePsReq` and prevent us from controlling the
             response — Round-3 review flagged that two subscribers
             on the same subject race each other.
    setup: start a standalone NATS server in-process via
           `testharness.StartNATS()` (no broker, no JetStream); on
           that NATS, attach a single fake subscriber to
           `proto.SubjCtrlPs(<test-actor-pubkey>, "lab")` that
           replies with a canned `PsResp{Processes:[
             {pid:"r1",Status:"RUNNING"},
             {pid:"l1",Status:"LOST"},
             {pid:"e1",Status:"EXITED"},
           ]}`.
           Write a temp ~/.tether/ home with a generated nkey and
           current_session="lab" so `tether ps` finds an identity.
           Build the ctl binary via a test-local helper added to
           `test/p4/ps_filter_test.go` (~10 LOC wrapping
           `exec.Command("go", "build", "-o", <out>, "./cmd/tether")`
           plus a `t.Cleanup` to remove the temp binary). No
           project-level `testharness.BuildCtl` exists today; this
           plan does not add one. The helper is file-private and
           reused by C7, C8, and C6c.
    call : exec the ctl binary in two modes against the standalone
           NATS URL. `--home` is a flag registered on the `ps` leaf
           (`cmd/tether/ps.go`), not as a root persistent flag, so:
             (1) `tether ps --home <tmp>`        (default)
             (2) `tether ps -a --home <tmp>`
    PASS : default stdout (1) contains "r1" AND "l1"; does NOT
           contain "e1". `-a` stdout (2) contains all three pids.
    FAIL : default stdout missing "l1" (the regression we're
           fixing); or default stdout contains "e1"; or `-a`
           missing any pid.
    Note : explicitly does NOT call `broker.Run()` or
           `testharness.StartBroker()`. The fake responder owns
           the subject for the test's lifetime; there is no
           competing subscriber so NATS request/reply is
           deterministic.

C7. TestPsFilter_ClientTimeoutRaisedTo15s
    setup: monkey-patch the broker's ps handler to block for 6s
           (longer than the old 5s timeout, shorter than the
            new 15s)
    call : run `tether ps` via os/exec on a fresh ctl build
    PASS : command exits 0 within 8s (proves the bump took effect)

C8. TestPsFilter_TimeoutErrorMessageReworded
    setup: monkey-patch the broker's ps handler to block 20s
    call : run `tether ps`; capture stderr
    PASS : stderr contains "ps: request timed out after 15s"
           and does NOT contain "broker unreachable on NATS"
```

### D. Migration safety — `internal/storage/storage_test.go`

```text
D1. TestMigration0005_AddsIndexes
    setup: in-memory DB, apply 0001..0004 only
    apply: 0005_processes_gc_indexes.sql
    PASS : query
           `SELECT name FROM sqlite_master
            WHERE type='index' AND tbl_name='processes'`
           includes ALL THREE of:
             'idx_processes_sid_status_started'
             'idx_processes_sid_started'
             'idx_processes_status_endedat'
           AND also still includes the pre-existing indexes
           ('idx_processes_sid_nid', 'idx_processes_status') —
           0005 must not drop those, only add.

D2. TestMigration0005_Idempotent
    setup: as D1, apply 0005 twice
    PASS : second apply does NOT error (IF NOT EXISTS works);
           pragma-confirmed index list unchanged

D3. TestMigration0005_DoesNotAlterExistingRows
    setup: insert 100 processes rows, apply 0005
    PASS : SELECT COUNT(*) FROM processes returns 100 still;
           row contents identical to pre-migration snapshot
           (hashes match)
```

### E. Performance / load — `test/p4/ps_perf_test.go`

The original bug is throughput-driven; the test plan must include a
measured regression guard or this fix is unverifiable.

```text
E1. BenchmarkListBySessionFiltered_RunningOnly_100k
    setup: insert 100 000 EXITED rows (ended_at staggered) +
           100 RUNNING rows for one sid
    bench: b.N iterations of
           ListBySessionFiltered(db, sid, {IncludeExited: false})
    PASS : ns/op < 50_000 (50µs) — well under the previous
           full-scan baseline that takes 5+ seconds on the same
           fixture; allows 10× slack for CI variance.

E2. TestPsRPC_Under1s_With100kBacklog
    setup: real broker + agent; pre-seed 100 000 EXITED rows via
           direct DB inserts
    call : measure round-trip time of one PsReq{} via NATS
    PASS : observed latency < 1_000ms (1 second)
           — three orders of magnitude better than the bug
             we're fixing

E3. TestProcGCBoundsTableGrowth
    purpose: pin the operational goal that GC bounds the SQLite
             `processes` table. Earlier draft of this test asserted
             "ps becomes 10× faster after GC" — that assertion is
             now wrong because the Round-2 indexed query already
             keeps ps fast regardless of EXITED count. The real
             contract is "table size stays bounded"; latency stays
             fast independent of GC.
    setup: ProcRetention=100ms, ProcGCInterval=50ms; mock Now();
           5 RUNNING + 50_000 EXITED rows with ended_at = now-1s.
    drive: (1) measure latency of one PsReq{} immediately.
           (2) measure latency of one PsReq{IncludeExited:true}
               immediately.
           (3) wait 200ms — at least 3 GC ticks fire under the mocked
               clock past cutoff.
           (4) read SELECT COUNT(*) WHERE status='EXITED'.
           (5) measure latency of PsReq{} and PsReq{-a} again.
    PASS : (1) and (2) both < 100 ms (the new index + LIMIT
               keep both paths fast independent of EXITED count;
               this is the bounded-read contract).
           (4) returns 0 (all 50_000 EXITED past cutoff are gone;
               this is the bounded-growth contract — the SQLite
               table cannot grow forever).
           (5) both calls < 100 ms (no regression after GC).
    FAIL : (1) > 100 ms ⇒ the indexed default-ps path is broken
                          for some unrelated reason — investigate
                          before claiming this fix lands; OR
           (4) > 0     ⇒ the GC ticker did not fire / did not
                          delete; original bug recurs in steady
                          state.
    Note : we do NOT assert "second call is faster" anywhere. The
           whole point of the Round-2 query design is that the
           default ps path is fast even before GC runs.
```

### F. Backward-compatibility matrix tests

The combos in the §Backward compatibility matrix table become
explicit tests. Most overlap with sections C and D; this is the
roll-up checklist.

```text
F1. Old ctl (default `ps` only) + New broker
    test: C3 (TestPsFilter_OldEmptyBodyCompat)
    status: covers DEFAULT `tether ps` only. The new broker's
            response to a `{}` body is "storage RUNNING rows, with
            LOST derived per row from OFFLINE node status" — so
            the wire payload can contain entries with
            `Status:"LOST"`. v0.2.7 ctl's display filter at
            `cmd/tether/ps.go:68` only prints `Status=="RUNNING"`,
            so any LOST rows the broker returns are hidden by the
            old ctl. Net user-visible behavior: v0.2.7 ctl + v0.2.8
            broker default `ps` shows the same RUNNING-only screen
            it always showed — it just does not gain the new
            "LOST is visible by default" improvement. That is
            acceptable for the supported transitional combination.
            It is NOT supported for `tether ps -a`; v0.2.7 ctl
            sends the same `{}` body whether or not `-a` is
            passed, and the new broker has no way to distinguish
            them. See §Backward compatibility matrix.

F1b. Old ctl `ps -a` + New broker — NEGATIVE TEST
    purpose: pin the documented breaking change; we want CI to
             notice if some future patch accidentally re-introduces
             a sentinel that makes old `-a` work again silently
             (which would mean the wire format diverged from what
             the matrix promises).
    setup  : same fixture as F1; build v0.2.7-equivalent body
             `[]byte("{}")` and send via direct NATS request.
    PASS   : response contains zero EXITED rows even though one was
             inserted (this matches the documented contract;
             operators get loud-empty rather than a partial result).

F2. New ctl + Old broker
    test: C5 (TestPsFilter_NewCtlOldBrokerSimulation)
    status: passes ⇒ new ctl's body extensions ignored by old broker

F3. New ctl + New broker (`-a` semantic)
    test: C2 (TestPsFilter_IncludeExited)
    status: passes ⇒ `-a` flag flows through correctly

F4. Old ctl + Old broker (regression baseline)
    test: existing test/p4/exec_e2e_test.go::TestPsBasic
    status: must still pass on the v0.2.7 binary (run from a tag);
            CI matrix includes this as a separate job
```

### G. Manual smoke (operator-side)

Captured in `log.md` "test matrix" once landed. Required to be run
before tagging the release, not in CI.

```text
G1. Upgrade-in-place test
    - on a broker with ≥10 000 existing EXITED rows, stop the
      v0.2.7 binary, install v0.2.8 binary, restart
    - apply migration runs in <2s
    - tail logs: see "broker: ready" then within 5 min see
      "broker: proc gc" with deleted=N where N matches
      pre-upgrade EXITED row count
    - sqlite3 tether.db
      "SELECT COUNT(*) FROM processes WHERE status='EXITED'"
      → drops to rows whose ended_at < 1h ago

G2. ctl mixed-version
    - new broker; v0.2.7 ctl runs `tether ps` → succeeds <1s
    - new broker; v0.2.8 ctl runs `tether ps -a` → succeeds <1s
    - new broker; v0.2.8 ctl runs `tether ps` → succeeds <1s,
      no EXITED rows in display

G3. End-to-end repro of the original bug (negative test)
    - replicate openpi phase5 scenario: bash loop calling
      `tether exec timan107 -- date` every second for 1 hour
      against a v0.2.8 broker
    - throughout the hour, `tether ps` continues to respond <1s
    - sqlite3 row count stays bounded around the ProcRetention
      window plus active RUNNING (verify with a graph or
      sampled count every 5 min)
```

### H. CI integration

```text
H1. go test ./internal/proc/... ./internal/broker/...
    runs A1-A11 and B1-B3 automatically; no new make target.

H2. go test ./test/p4/...
    runs C1-C8 and E1-E3 alongside existing p4 e2e tests.

H3. go test -race
    must pass for all new tests (GC ticker + handler co-existence
    creates a real race candidate around DB writes).

H4. go test -bench=. -benchmem ./test/p4/...
    benchmark E1 surfaced in CI logs; intentionally NOT a fail
    gate (CI variance) but trend-tracked.

H5. matrix CI: v0.2.7 ctl × v0.2.8 broker
    a new make target `make test-mixed-version` builds both
    versions and runs THREE smoke tests:
      - C3  (TestPsFilter_OldEmptyBodyCompat) — supported default
            view works against new broker.
      - C7  (TestPsFilter_ClientTimeoutRaisedTo15s) — new-ctl
            timeout, sanity check.
      - F1b (Old ctl `ps -a` + new broker, negative) — pins the
            documented breaking change so a future patch silently
            re-introducing a `-a` sentinel is caught here, not in
            production.
    Wired into the existing nightly e2e job, not push CI (cost).
    If make-target adoption is too noisy for nightly, the same
    three tests run as a Go `t.Run` matrix from a single
    test file and are gated by `-tags mixed_version`.
```

### Coverage matrix — change → test

| Change                                                          | Covered by              |
|-----------------------------------------------------------------|-------------------------|
| migration 0005 adds three indexes                               | D1                      |
| migration 0005 is idempotent                                    | D2                      |
| migration 0005 doesn't touch data                               | D3                      |
| Default `ps` query uses idx, no TEMP B-TREE                     | A12, E1, E2             |
| `-a` query uses idx, no TEMP B-TREE                             | A12, E1                 |
| GC query uses idx                                               | A12                     |
| `ListBySessionFiltered` storage filter is `status='RUNNING'`    | A1, A2                  |
| `ListBySessionFiltered` applies LIMIT (and Limit=0 means none)  | A3, A4, A5              |
| `ListBySessionFiltered` handles empty session                   | A6                      |
| `GCExited` deletes only old EXITED                              | A7, A10                 |
| `GCExited` never touches RUNNING                                | A8                      |
| `GCExited` handles empty table                                  | A9                      |
| `ListBySession` (wrapper) backward-compat for out-of-tree       | A11, C6                 |
| Reconcile path swapped to filtered helper, bounded              | C6b                     |
| Broker GC ticker fires + deletes                                | B1, E3, G1              |
| Broker default ProcRetention=1h, GCInterval=5min in `New`       | B2                      |
| Broker shuts down GC ticker on ctx cancel                       | B3                      |
| Handler decodes PsReq body                                      | C1, C2, C3              |
| Handler applies server cap                                      | C4                      |
| Handler tolerates `{}` (old ctl, default view only)             | C3, F1                  |
| Old ctl `ps -a` documented degradation (negative)               | F1b                     |
| Handler ignores unknown PsReq fields (old broker simulation)    | C5, F2                  |
| ctl encodes PsReq with `IncludeExited` from `-a`                | C2, F3                  |
| ctl default display includes RUNNING+LOST, drops EXITED         | C6c                     |
| ctl uses 15s timeout                                            | C7                      |
| ctl error wording                                               | C8                      |
| Performance: <1s on 100k backlog                                | E2                      |
| Retention GC bounds EXITED table growth (no ps latency regression) | E3                   |
| Performance: reconcile bounded on 100k backlog                  | C6b                     |
| Operator upgrade smooth                                         | G1                      |
| Mixed ctl/broker versions work                                  | G2, H5                  |
| Original bug repro doesn't recur                                | G3                      |
| Race-free GC + handler co-existence                             | H3                      |

Any row in this matrix without a passing test blocks the patch.

## Risk

- **First-sweep lock pause**. On a broker with hundreds of thousands
  of EXITED rows, the first `DELETE FROM processes WHERE status='EXITED'
  AND ended_at < ?` may hold the SQLite write lock for several hundred
  milliseconds. During that window, agent `ev.proc.started` /
  `ev.proc.exit` writes block. Mitigation: GC runs in the broker
  goroutine, NATS sub callbacks queue, no message is dropped. If this
  proves user-visible, a follow-up patch can chunk via
  `DELETE ... WHERE ... LIMIT 1000` in a loop. Note this depends on
  the `modernc.org/sqlite` (pure-Go) build supporting
  `SQLITE_ENABLE_UPDATE_DELETE_LIMIT` — upstream SQLite has this
  compile-time gated and modernc tracks the upstream amalgamation;
  if the flag is not enabled, the follow-up patch would use a
  subquery (`DELETE FROM processes WHERE pid IN (SELECT pid FROM ...
  LIMIT 1000)`). This is a deferred follow-up, not part of this plan.

- **PsReq decode tolerance on the old broker**. Relies on the Go
  `encoding/json` behavior that unknown fields on a struct without
  the `DisallowUnknownFields` decoder are silently dropped. The
  current broker handler uses the default `json.Unmarshal`, not a
  `Decoder` with strict mode, so the assumption holds. Validated by
  reading `internal/broker/exec.go:243-251` (current ParseCtrlBy
  block, no Decoder).

- **Retention default of 1 h**. If the operator depends on
  `tether ps -a` to see "what exited 6 hours ago", that information
  is gone from the SQLite table after 6 hours. The current
  `tether history` printer is line-oriented (`printAuditEntry` in
  `cmd/tether/history.go`); operators looking for exit codes can
  still find them in the audit stream by reading those lines, but
  there is no built-in JSON output. Adding `--json` to `tether
  history` is a separate ticket, not a prerequisite for this plan.

- **JetStream `history-<sid>` is byte-bounded, not time-bounded.**
  `internal/jsstream/jsstream.go:94-101` configures each per-session
  stream with `MaxAge=0` (no expiry), `MaxBytes=1 GiB`,
  `Discard=DiscardNew`. That means a session that audits enough
  writes can saturate its 1 GiB allowance and *new* audit events
  will be rejected at the broker — old EXITED records remain
  retrievable but new starts/exits stop being recorded. This is
  pre-existing behavior, but the plan's earlier "30 d retained"
  language about `history-<sid>` was wrong and has been corrected
  in the Goals section. Operators heavily using `tether exec` should
  monitor stream usage (the existing `disk_pressure` system event
  partially covers this) and either widen `MaxBytes` via `nats stream
  edit` or rotate sessions. Surfacing usage in `tether ps` /
  `tether admin` is out of scope for this plan.

- **GC ticker collision with reconcile ticker**. Both tickers can
  fire close in time; both touch the `processes` table. SQLite
  serializes writes; observed worst case is the GC waits one tick
  for reconcile's `MarkExited` chain to finish. Acceptable. Lower-bound
  `ProcGCInterval` at 1 min is enforced in the **`serveconf`** yaml
  decoder (not in `broker.New`), so a pathological operator can't
  set it to e.g. 10 ms from `broker.yaml`. Tests that need fast
  ticking construct `broker.Config` directly and bypass the decoder
  — the broker code path itself imposes no minimum, so
  `ProcGCInterval=20ms` in `internal/broker/broker_test.go` and
  `ProcRetention=50ms` work as written.

- **Migration ordering**. Migrations live in
  `internal/storage/migrations/` and are applied by
  `internal/storage/storage.go:Open` at DB-open time, before any
  caller reads or writes. `broker.New` requires a non-nil `cfg.DB`
  (i.e. `storage.Open` has already returned), so by the time
  `Broker.Run` subscribes to `ctrl.by.*.s.*.ps.req`, the 0005
  indexes are in place. There is no explicit `storage.Migrate()`
  call in the broker — an earlier draft of this plan said there was,
  which was incorrect.

- **Query-plan fragility**. The bounded `ps` story rests on SQLite
  using `idx_processes_sid_status_started` (default) and
  `idx_processes_sid_started` (`-a`). A future schema change that
  e.g. drops `started_at` from the `processes` row, or adds a new
  column to the WHERE clause without updating the index, can silently
  reintroduce the temp B-tree. `TestPsQueryPlan_NoTempBTree` (test
  A12 below) parses `EXPLAIN QUERY PLAN` output for both the default
  and `-a` paths and fails if the plan grows a "USE TEMP B-TREE FOR
  ORDER BY" line. That test is the durable contract.

- **No `Limit` user surface yet**. The `Limit` field on `PsReq` is
  plumbed through but not exposed on the CLI. The server cap of
  500 is the only enforcement path. This is deliberate: clients
  don't need to negotiate up; if a future feature needs more rows,
  the protocol already supports it via `Limit`, so no further wire
  change is needed.

- **Wire-protocol back-compat across the v0.1.x → v0.2.x boundary**.
  The `proto.ProtoVersion` stays at v1. We deliberately are NOT
  introducing a v2. If a future change forces a true wire break
  (e.g. removing the empty-body fallback), that's the time to bump.

## Rollout

1. Land migration 0005 + `proc.GCExited` + `ListBySessionFiltered`
   + tests. **No protocol change yet**; existing
   `ListBySession` still backs `handlePsReq`. CI green.
2. Land `PsReq` field additions + handler change + GC ticker +
   broker config additions + reconcile swap + tests. Server-side
   defaults active. CI green.
3. Land ctl change (encode `PsReq`, 15 s timeout, error string,
   LOST in default display). CI green.
4. Cut release `v0.2.8`. log.md + docs/usage.md updates land in
   the same release; the v0.2.8 release-notes entry MUST carry
   the **breaking-change** callout described below. CI's existing
   "release-notes-has-section" check (if any — otherwise add a
   reviewer-checklist line in `.github/PULL_REQUEST_TEMPLATE.md`)
   trips if it's missing.

   Release-note draft:

   > **Breaking (ctl):** v0.2.8 introduces a `PsReq{IncludeExited}`
   > field on the `tether ps` RPC. v0.2.8+ broker treats `{}` /
   > omitted `include_exited` as `IncludeExited=false` and returns
   > only active processes (storage RUNNING rows, with LOST
   > derived from OFFLINE node status). v0.2.7 ctl has no way to
   > set this field — it always sends `{}` whether or not `-a` was
   > passed, because `-a` is a local display filter in the legacy
   > code. As a result, `tether ps -a` from a v0.2.7 ctl against a
   > v0.2.8 broker returns an empty EXITED list rather than the
   > recently-exited processes it used to show. Operators
   > upgrading a broker to v0.2.8 must also upgrade ctl to v0.2.8
   > on every machine that runs `tether ps -a`. Default
   > `tether ps` keeps its old user-visible behavior under the
   > mixed-version combination — the broker may include LOST
   > entries on the wire, but the legacy ctl display filter still
   > shows only RUNNING.

5. Operators upgrade broker (gets the bulk of the fix immediately,
   plus the documented breaking change for old ctl `-a`).
6. Operators upgrade ctl. **Required** for `tether ps -a`;
   optional for default `tether ps` (which still works on v0.2.7
   ctl against v0.2.8 broker).

Each step is independently shippable / revertible. Splitting into
three commits keeps the review surface narrow.

## Out of Plan (deferred)

- **Per-session retention overrides** (e.g., a session that wants
  72 h of history vs another that wants 5 min). Would need a new
  column on `sessions` and read-path complexity. Wait until an
  operator asks.
- **`tether ps --watch`**. Live stream of process started / exited
  events; recommend `tether history --follow --kind proc` instead.
- **Chunked GC** (loop with `LIMIT 1000`). Only matters if the
  first-sweep pause is user-visible. Trivial follow-up.
- **JetStream-backed `tether ps`** (replace SQLite as the source of
  truth for the operational view). Major refactor; the present
  fix is order-of-magnitude cheaper.
- **DB-side enforcement of `ProcGCInterval` floor**. Currently a
  comment + validation in config decode. If operators routinely
  misconfigure, harden into a structural minimum.
- **Metrics surfacing** (`tether admin proc-stats` showing rows/sid,
  oldest-exited-ts, etc.). Defer until we know which numbers are
  worth surfacing.

---

## Reviewer Notes Round 1 - 2026-05-23

Scope: reviewed the bounded-growth plan against the current storage,
broker, proto, CLI, history, and reconcile code paths. I also checked
the existing review workflow in `docs/reviews/*` and verified the
critical SQLite planner assumption with `EXPLAIN QUERY PLAN`.

Conclusion: not ready to implement as written. The plan targets the
right failure mode, but the proposed read query and indexes do not yet
guarantee the bounded default `ps` path the goals require.

### Blocking Concerns

1. The default `ps` query is still a per-session scan/sort.

The plan adds `idx_processes_sid_status` and queries:

```sql
WHERE sid = ? AND status != 'EXITED'
ORDER BY started_at DESC
LIMIT ?
```

That does not produce the "O(active RUNNING)" path described in the
Index usage section. With the proposed index, SQLite can use only the
`sid` prefix for the inequality and still builds a temp b-tree for the
`ORDER BY`:

```text
SEARCH processes USING INDEX idx_processes_sid_status (sid=?)
USE TEMP B-TREE FOR ORDER BY
```

Current schema only has `idx_processes_sid_nid` and
`idx_processes_status` (`internal/storage/migrations/0001_init.sql:63`),
so without a better new index the 100k EXITED backlog remains on the
critical read path. This directly undermines the plan's E1/E2
performance assertions.

Recommendation:

- Query stored active rows with equality, not inequality. In the current
  implementation LOST is read-side derived, so `status = 'RUNNING'` is
  the real default storage predicate. If the plan wants to keep the
  schema-level LOST escape hatch, spell out `status IN ('RUNNING','LOST')`
  and test its plan.
- Add an order-aware index for the default path, e.g.
  `idx_processes_sid_status_started ON processes(sid, status, started_at DESC)`.
- Add `idx_processes_sid_started ON processes(sid, started_at DESC)` for
  `IncludeExited=true` + server cap; otherwise `ps -a` still sorts the
  entire session before returning 500 rows.
- Add a regression test that asserts the query plan does not contain
  `USE TEMP B-TREE FOR ORDER BY` for the default `PsReq{}` path, in
  addition to the latency benchmark.

2. The default LOST behavior is internally inconsistent.

The goals say default `tether ps` is "RUNNING + LOST only", and the
server-side `IncludeExited=false` comments repeat that. Current CLI
code filters with:

```go
if p.Status != "RUNNING" && !showAll {
    continue
}
```

(`cmd/tether/ps.go:68-70`), so default output hides LOST rows today.
The proposed client patch only changes the request body and timeout; it
does not change this display filter. That means a new broker can return
LOST rows by default, but both old and proposed-new ctl will still drop
them unless `-a` is set.

Recommendation: choose and document one contract before coding:

- If default really means RUNNING + LOST, update the CLI filter to keep
  `p.Status == "LOST"` when `showAll == false`, update `docs/usage.md`
  §5.6, and add a handler/CLI test for a RUNNING row whose node is
  OFFLINE.
- If default means RUNNING only, rewrite the plan goals, proto comments,
  and tests to stop promising LOST in the default view.

### Medium Concerns

3. The long-term audit-retention fallback is misstated.

The plan repeatedly says historical process data lives in JetStream for
"30 d" or "arbitrarily long lookback". Current `history-<sid>` streams
are configured with `MaxAge: 0`, `MaxBytes: 1 GiB`, and
`DiscardNew` (`internal/jsstream/jsstream.go:94-101`). The 30-day limit
belongs to the global `events` stream, not per-session history. Also,
the recommended fallback `tether history --kind proc | jq ...` is not
currently valid: `cmd/tether/history.go` validates `--kind proc`, but
then pretty-prints lines in `printAuditEntry` rather than emitting raw
JSON.

Impact: after GC removes old EXITED rows, the plan's stated lookup path
for old exit codes is not the one the product actually guarantees.

Recommendation: revise the retention language to match `jsstream.go`
and document the real operator command. If machine-readable long-term
exit lookup is required, add an explicit `history --json` or admin
export path to this plan or call it out as a prerequisite.

4. Reconcile remains unbounded even though the plan says EXITED rows are
dead weight for it.

The Non-Goals section correctly says G.1 reconcile only needs
RUNNING/LOST candidates. But the Design section keeps
`reconcile.go` on the backward-compatible `ListBySession` wrapper,
which remains a full `WHERE sid = ? ORDER BY started_at DESC` read of
every EXITED row (`internal/proc/proc.go:185-193`,
`internal/broker/reconcile.go:56`).

Impact: an agent reconnect immediately after upgrade, before the first
GC sweep, can still scan a giant historical backlog while trying to
recover observability. The plan fixes the user-facing `ps` RPC but
leaves another session-wide read path with the same growth shape.

Recommendation: add a reconcile-specific helper, or call
`ListBySessionFiltered` with non-EXITED status and no cap from
`reconcileOnRegister`. Pin it with a 100k EXITED + 100 RUNNING test so
agent reconnect stays bounded too.

### Plan Cleanups Before Implementation

- `ProcGCInterval` is described as having a 1-minute lower bound, but
  the broker tests use 30-50 ms intervals. Make the distinction explicit:
  production YAML/CLI decode may clamp or reject sub-minute values, while
  raw `broker.Config` test overrides must remain able to use short
  intervals.
- Defaults should be specified in `broker.New`, matching the existing
  `ReconcileInterval` / `StaleAfter` pattern, unless there is a reason to
  move defaulting into `Run`.
- `storage.NewInMemory()` does not exist; existing tests use local
  `openDB(t)` helpers over `storage.Open(":memory:")`.
- `Broker.Run()` does not call `storage.Migrate()`; migrations run inside
  `storage.Open`. Update the migration-ordering risk text.
- The chunked-GC note names `mattn/go-sqlite3`, but this project uses
  `modernc.org/sqlite`; do not rely on mattn compile options for a
  future `DELETE ... LIMIT` fallback.
- `test/p4/ps_perf_test.go` appears in the verification plan but not in
  Files Touched. Add it there if E1-E3 are required gates.

Approval gate: address the two blocking concerns, then re-review the
updated query/index shape and the revised LOST/history contracts before
implementation starts.

---

## Author Response to Reviewer Notes Round 1 — 2026-05-24

Thanks for the round-1 pass. Both blocking concerns and all four
medium/cleanup items were correct; I verified each against the
actual code before editing the plan rather than just trusting my
own notes:

- `cmd/tether/ps.go:67-69` — confirmed the default filter drops
  LOST silently. Blocking #2 stands.
- `internal/jsstream/jsstream.go:94-101` — confirmed
  `MaxAge=0`, `MaxBytes=1<<30`, `Discard=DiscardNew`. The "30 d"
  language in my Goals and Risk sections was wrong.
- `go.mod` — confirmed `modernc.org/sqlite v1.50.0` (not
  mattn/go-sqlite3). The Risk note about `SQLITE_ENABLE_UPDATE_DELETE_LIMIT`
  was referencing the wrong driver.
- `internal/storage/storage.go` — confirmed the only public entry
  is `Open(dsn)`; there is no `NewInMemory` and no separate
  `Migrate` — migrations run inside `Open`.
- `internal/broker/broker.go:281-300` — confirmed the existing
  defaults block (`ReconcileInterval`, `StaleAfter`, `OfflineAfter`)
  lives in `func New`, not `Run`.
- `cmd/tether/history.go:317 printAuditEntry` — confirmed history
  emits a formatted text line per audit entry, not JSON; the
  `| jq` hint was unreachable.

### Blocking Concerns

**B1. Query / index design.** Acknowledged. The plan now:

- Drops the proposed `idx_processes_sid_status` (insufficient
  prefix) and adds three indexes instead:
  `idx_processes_sid_status_started` (default `ps` path,
  equality + ORDER BY),
  `idx_processes_sid_started` (`ps -a` path with server cap),
  `idx_processes_status_endedat` (GC).
- Changes the storage-level filter from `status != 'EXITED'`
  (inequality, prefix-only) to `status = 'RUNNING'` (equality).
  This is safe because LOST is read-derived in `handlePsReq` from a
  RUNNING row + an OFFLINE node — the SQLite table never stores
  `status='LOST'`. Verified by inspecting all `processes`-table
  writers (`proc.Insert` writes `'RUNNING'`,
  `proc.MarkExited` writes `'EXITED'`; no writer sets `'LOST'`).
- Adds `TestPsQueryPlan_NoTempBTree` (test A12) as a durable
  contract that fails if any future change reintroduces a TEMP
  B-TREE sort or skips the new indexes.

The Index usage block now describes the actual planner output we
expect, not an aspirational shape.

**B2. Default LOST in display.** Acknowledged. The plan now lists a
third change in the Client section that updates
`cmd/tether/ps.go:68` from

```go
if p.Status != "RUNNING" && !showAll { continue }
```

to

```go
if !showAll && p.Status != "RUNNING" && p.Status != "LOST" { continue }
```

so the default `ps` view shows both RUNNING and LOST, consistent
with the goals + the server-side handler's LOST derivation. New
test `TestPsDisplay_RunningAndLost_NotExited` (C6c) pins the
display contract; `docs/usage.md` §5.6 will be updated to say so.

### Medium Concerns

**M3. Audit retention misstated.** Acknowledged.

- Goal 3 now references the actual jsstream config (`MaxAge=0`,
  `MaxBytes=1 GiB`, `DiscardNew`) and adds a forward pointer to
  the §Risk bullet that explains the saturation failure mode.
- The Client section drops the `tether history --kind proc | jq`
  recommendation and explicitly notes the printer is
  human-formatted (`printAuditEntry`); adding `--json` is now an
  out-of-scope follow-up ticket, not an implied prereq.
- §Risk has a new bullet "JetStream `history-<sid>` is byte-bounded,
  not time-bounded" describing the DiscardNew saturation pattern,
  so operators know the long-tail audit story is real but not
  unbounded.

**M4. Reconcile unbounded.** Acknowledged.

- Files Touched now includes a 1-line change to
  `internal/broker/reconcile.go` swapping
  `proc.ListBySession` for
  `proc.ListBySessionFiltered(..., IncludeExited:false, Limit:0)`.
  The reconcile loop already filters anything that isn't
  RUNNING/LOST in Go (`reconcile.go:71-73`), so the storage-level
  filter strictly reduces work without changing semantics.
- New test `TestReconcileBoundedOnBacklog` (C6b) seeds 100 k
  EXITED + 5 RUNNING rows, calls `reconcileOnRegister` directly,
  and asserts <200 ms response — pins the reconnect bound.
- The `ListBySession` wrapper stays in place for out-of-tree
  callers; in-tree no caller relies on the full EXITED view.

### Plan Cleanups

- **`ProcGCInterval` floor**: the §Risk bullet now spells it out —
  the 1-min lower bound is enforced in
  `internal/serveconf/serveconf.go` (yaml decode) and NOT in
  `broker.New`. Tests construct `broker.Config` directly with
  short intervals; they bypass the decoder.
- **Defaults in `New` not `Run`**: the Broker periodic-GC ticker
  section now applies defaults in `broker.New` (matching
  `ReconcileInterval`'s pattern) and `Run` only reads them.
- **`storage.NewInMemory`** was a wrong reference; removed from
  Files Touched + Verification. Tests use the existing
  `openDB(t)` helper backed by `storage.Open(":memory:")`, the
  same approach as `TestMarkExitedTransitions`.
- **`Broker.Run()` calling `storage.Migrate()`** was wrong; the
  §Risk "Migration ordering" bullet now says the migrator runs
  inside `storage.Open`, before `broker.New`.
- **`mattn/go-sqlite3`** removed from the §Risk
  chunked-DELETE bullet. The text now says modernc tracks upstream
  SQLite's flag; if `SQLITE_ENABLE_UPDATE_DELETE_LIMIT` is not
  enabled, a subquery (`DELETE … WHERE pid IN (SELECT pid … LIMIT N)`)
  is the portable fallback.
- **`test/p4/ps_perf_test.go`** is now in Files Touched as a
  new ~150 LOC file housing E1-E3.

### Self-review pass (Round 1, code-anchored)

For each numbered concern I re-ran the literal grep / file read the
reviewer used (or its equivalent) and confirmed the edit. Concrete
checks performed against working-tree HEAD before writing the
response:

```
grep -n 'p.Status != "RUNNING"' cmd/tether/ps.go        # → :68
grep -n 'MaxAge' internal/jsstream/jsstream.go          # → :98
grep -n 'modernc.org/sqlite' go.mod                      # → :hit
grep -n 'NewInMemory\|^func Migrate' internal/storage/   # → no hits
sed -n '280,310p' internal/broker/broker.go              # → defaults in New
grep -n 'printAuditEntry' cmd/tether/history.go          # → :317
```

I did not run `go test`. The plan now describes test cases that
will be implemented in the corresponding `_test.go` files, with the
A12 / C6b / C6c additions specifically targeted at the round-1
findings.

### Status

Awaiting Round-2 review of the revised plan. Implementation does not
start until the design above is signed off.

---

## Reviewer Notes Round 2 - 2026-05-23

Scope: re-reviewed the Round-1 revision with focus on the new
query/index design, LOST display contract, mixed-version behavior,
reconcile path, and test feasibility.

Conclusion: the core bounded-query design is now sound, but the plan is
not approved yet. One mixed-version compatibility hole remains, and a
few stale plan fragments would mislead implementation/tests if copied as
written.

### Blocking Concern

1. Old `ctl ps -a` silently loses EXITED rows against a new broker.

Current `cmd/tether/ps.go` always sends `[]byte("{}")` on the wire
regardless of whether `-a` is set; `-a` is only a local display filter.
The revised new broker decodes `{}` as `IncludeExited=false` and returns
only RUNNING-derived rows. Result:

- v0.2.7 `ctl ps` + v0.2.8 broker: OK, old ctl filters to RUNNING and
  sees the default view.
- v0.2.7 `ctl ps -a` + v0.2.8 broker: regression, old ctl still sends
  `{}`, so the broker withholds EXITED rows and `-a` no longer shows the
  thing the flag promises.

The `omitempty` shape also means new default ctl sends `{}` when
`IncludeExited=false`, so the broker cannot distinguish "legacy client"
from "new client default" using the proposed `PsReq` struct alone.

Recommendation: explicitly decide the compatibility contract and encode
it in the plan.

If old `ps -a` must keep showing recent exited rows, the broker needs a
legacy-empty-body mode. One workable shape:

- change `PsReq` to make field presence detectable, or add a non-zero
  `View`/`Mode` field that new ctl always sends;
- treat truly empty `{}` / zero-body legacy requests as "active rows +
  bounded recent EXITED rows", ensuring all active rows are included
  before appending recent EXITED rows up to a cap;
- have new ctl default send an explicit active-only request, and new
  ctl `-a` send all/recent-exited.

If old `ps -a` is allowed to degrade after broker upgrade, say that
plainly in the backward-compat matrix and release notes. Right now the
plan says "old ctl + new broker" is faster with no new flag needed,
which reads as preserving existing `ps` semantics.

### Required Cleanups

2. Reconcile text still contradicts the revised design.

The Goals section still says "Existing `reconcile.go` ... unchanged",
and the `internal/proc/proc.go` design paragraph still says
"Existing `reconcile.go` keeps calling the unchanged `ListBySession`".
Later sections correctly say `internal/broker/reconcile.go` changes to
`ListBySessionFiltered(... IncludeExited:false ...)`.

Fix the stale prose so implementation does not preserve the unbounded
path by following the older paragraph. Suggested wording: session-rm
cascade semantics remain unchanged; reconcile behavior remains
semantically unchanged but its read helper changes to the filtered
bounded query.

3. The 30-day history comments were not fully removed.

The `GCExited` and `ProcRetention` code snippets still say audit history
lives in `history-<sid>` with "30 d default". The Risk section correctly
states the real config: `MaxAge=0`, `MaxBytes=1 GiB`, `DiscardNew`.

Update those two comments and any remaining "arbitrarily long" wording
before implementation. These snippets are likely to be copied into code,
so stale comments matter.

4. Migration test D1 still names the old index.

The storage design now creates:

- `idx_processes_sid_status_started`
- `idx_processes_sid_started`
- `idx_processes_status_endedat`

But D1 still expects `idx_processes_sid_status` plus
`idx_processes_status_endedat`. Update D1 and the coverage matrix so
the migration test catches all three final index names.

5. C6c's test fixture references a non-existent `ps` transport seam.

The plan says to "spin up the cobra ps command with a stub Transport".
`cmd/tether/ps.go` currently connects directly via
`cli.ConnectNATSWithNkey` and issues `nc.RequestWithContext`; the
`Transport` interface in `internal/cli` is only for completion helpers.

Either add a real seam for `ps` command testing and list that file/API in
Files Touched, or rewrite C6c as a NATS-backed CLI test with a fake
`SubjCtrlPs` responder plus a prepared home/current_session/identity.
As written, the acceptance test cannot be implemented without an
unstated refactor.

### Approved Pieces

- The revised default query (`status='RUNNING'`) plus
  `idx_processes_sid_status_started` addresses the Round-1 planner
  issue.
- Adding `idx_processes_sid_started` for `IncludeExited=true` fixes the
  server-cap-before-sort problem.
- Adding an `EXPLAIN QUERY PLAN` regression test is the right durable
  guard.
- Moving reconcile to the filtered helper is the right runtime fix once
  the stale prose is removed.

Approval gate: resolve the old-ctl `ps -a` compatibility decision and
clean up the contradictory test/prose fragments above. After that, I do
not see another design-level blocker in the bounded-query/GC approach.

---

## Author Response to Reviewer Notes Round 2 — 2026-05-24

Round-2 blocker and all four cleanups acknowledged and addressed.

### Blocking Concern (#1, mixed-version `ps -a`)

Decision: **clean break, not legacy-empty-body sentinel.** v0.2.7 ctl
+ v0.2.8 broker is declared unsupported for `tether ps -a`, with the
documented degradation that `-a` silently returns an empty EXITED
slice. The plan changes:

- Goal 5 rewritten to spell out four ctl/broker combinations
  explicitly, calling the unsupported one out as breaking.
- §Backward compatibility matrix expanded — was 4 rows of "ps
  behavior", now 4 rows × 2 columns (`ps` vs `ps -a`); the
  unsupported combination has a "**NOT SUPPORTED**" marker and a
  short rationale paragraph below the table.
- §Rollout step 4 now requires the release-notes entry for v0.2.8
  to carry an explicit breaking-change callout; a draft of that
  callout is included verbatim so the implementer cannot land the
  release without copying it.
- §Rollout step 6 reclassifies ctl upgrade from "at leisure" to
  "required for `tether ps -a`".
- New negative test **F1b** (Old ctl `ps -a` + New broker) asserts
  the empty-EXITED behavior at the protocol layer — guards against
  some future "smart" sentinel sneaking in and silently un-breaking
  the contract without updating the matrix.

Why a clean break over a sentinel mode:

- v0.2.7 ctl has no in-protocol channel to signal `-a`; `-a` is
  pure local display state. Inventing a one-release `View` field
  with a special empty-body fallback would mean every future
  reader of `PsReq` has to remember a quirk.
- The failure mode is operator-detectable: first time they run
  `ps -a` and see no EXITED rows, they know to upgrade ctl. There
  is no silent data corruption risk — only a missing display.
- ctl upgrade is a single static binary replace; the cost-benefit
  doesn't justify the protocol shape.

### Cleanups (#2-#5)

**#2 Reconcile prose contradicted itself.** Fixed:

- Goal 4 rewritten — was "Existing `reconcile.go` … unchanged"; now
  says session-rm cascade is unchanged and reconcile's observable
  semantics are unchanged, but its read helper switches to the
  filtered version. Explicit cross-reference to Files Touched.
- The `internal/proc/proc.go` design paragraph was the inverse —
  rewritten to say in-tree `reconcile.go` switches to
  `ListBySessionFiltered(IncludeExited:false)`; `ListBySession`
  stays only for out-of-tree callers.

**#3 30-day comments still in code snippets.** Fixed:

- The `GCExited` doc comment now references
  `jsstream.go:94-101` and spells out
  `MaxBytes=1 GiB / MaxAge=0 / DiscardNew`.
- The `ProcRetention` config-field comment is changed similarly.
- No remaining "30 d" or "arbitrarily long lookback" mentions in
  the design's code snippets (verified by `grep -n '30 d' …` over
  the plan — only hits are inside the Round-1/2 review/response
  history sections, which are appropriately quoted history).

**#4 D1 names the wrong index.** Fixed:

- D1 PASS criteria expanded to assert all three new indexes by
  name (`idx_processes_sid_status_started`,
  `idx_processes_sid_started`, `idx_processes_status_endedat`) AND
  the pre-existing `idx_processes_sid_nid` and
  `idx_processes_status` still present (migration must not drop
  those).
- Coverage matrix updated to make D1 explicitly cover "migration
  0005 adds three indexes".

**#5 C6c uses a non-existent stub Transport.** Fixed:

- Rewritten as a NATS-backed e2e test reusing the same harness
  C7/C8 already use (build the ctl binary, exec it, fake
  `SubjCtrlPs` responder on the broker side). Setup steps spelled
  out so the test is implementable without an additional refactor.
- Files Touched still lists only the existing `test/p4/ps_filter_test.go`
  — no new infrastructure required.
- Confirmed by reading `cmd/tether/ps.go` (`cli.ConnectNATSWithNkey`
  + `nc.RequestWithContext` direct call; no Transport seam) and
  `internal/cli/completion_transport.go` (the existing Transport
  interface is completion-only, not ps).

### Self-review pass (Round 2, code-anchored)

```
grep -n 'idx_processes_sid_status\b' docs/reviews/ps-retention-plan.md
# → only inside Round-1 review history block; no live design refs

grep -n '30 d\|arbitrarily long' docs/reviews/ps-retention-plan.md
# → only inside Round-1 review/response history; no live design refs

grep -n 'cli.ConnectNATSWithNkey' cmd/tether/ps.go
# → :42 ; confirms ps.go has no Transport indirection

grep -n 'type Transport' internal/cli/
# → only completion_transport.go uses it
```

### Status

Awaiting Round-3 review.

---

## Reviewer Notes Round 3 - 2026-05-23

Scope: re-reviewed the Round-2 revision after the decision to make
old-ctl `ps -a` a documented clean break. Focus was compatibility
contract clarity and whether the required tests still match the revised
query design.

Conclusion: the mixed-version contract is now explicit enough. I do not
object to the clean-break choice for old `ctl ps -a`. However, the test
plan still contains one stale performance assertion that will be false
under the new indexed query, plus two smaller wording/test-harness
cleanups.

### Blocking Concern

1. E3 still assumes default `PsReq{}` is slow before GC.

`TestPsRPC_GCRecoversBrokenSession` currently says:

```text
insert 50 000 EXITED rows
call PsReq{} immediately -> assert latency may be slow
wait for GC
call PsReq{} again -> assert latency < 50ms and at least 10x faster
```

That made sense for the original unbounded `ListBySession` plan. It no
longer matches the Round-2 design. Default `PsReq{}` now queries
`status='RUNNING'` via `idx_processes_sid_status_started`; EXITED rows
are not on the read path even before GC. The first call should already
be fast, so the "10x faster after GC" assertion is either flaky or
impossible.

Recommendation: rewrite E3 as a GC-bounding test rather than a default
`ps` latency recovery test. For example:

- seed 50k old EXITED + a small active set;
- assert default `PsReq{}` is fast both before and after GC;
- wait for GC and assert `SELECT COUNT(*) WHERE status='EXITED'` drops
  to the retained window;
- if testing `ps -a`, assert it remains capped/fast before and after GC,
  not that GC is required to make it fast.

This keeps the original operational goal (SQLite table stops growing
forever) without contradicting the new bounded read path.

### Required Cleanups

2. The compatibility matrix should describe old default `ps` precisely.

The matrix / F1 text says the new broker reads `{}` as
"RUNNING-only". Server-side handler semantics are actually "storage
RUNNING rows, then derive LOST from OFFLINE nodes"; new broker can return
`Status:"LOST"` in the response. The legacy ctl then hides LOST because
its display filter only prints RUNNING.

Suggested wording: old ctl + new broker default `ps` remains
user-visible compatible with old ctl behavior (RUNNING displayed only),
but it does not get the new LOST-in-default-display improvement.

3. C6c still describes replacing a live broker subscription.

The test text says "spin up a testharness broker ... replace the
broker's `SubjCtrlPs` subscription with a fake handler." There is no
existing hook to replace that subscription. Adding a second subscription
on the same request subject would race two responders; NATS request will
accept whichever reply arrives first.

The fix is straightforward: make C6c a fake-NATS-responder CLI test
without starting `broker.Run`, or add an explicit broker handler-inject
test hook and list it in Files Touched. Since the test only needs to
exercise `cmd/tether/ps.go` display filtering, a standalone NATS server
with one fake `SubjCtrlPs` responder is enough.

4. H5 should include the new mixed-version negative test.

The CI integration section still says the mixed-version target runs
"C3 + C7". After Round 2, F1b is the explicit guard for the documented
old-ctl `ps -a` break. Add F1b to H5, or state why the breaking-change
contract is only manually verified.

### Approved Pieces

- The clean-break compatibility decision is now documented in Goals,
  the matrix, rollout, and release-note draft.
- The D1 index list now matches the three final indexes.
- The 30-day history wording is removed from the live design snippets.
- Reconcile prose is consistent with the filtered-helper change.

Approval gate: fix E3 and the test-harness wording above. After that,
the plan is ready to move from design review to implementation.

---

## Author Response to Reviewer Notes Round 3 — 2026-05-24

Round-3 blocker + 3 cleanups all acknowledged and applied.

### Blocking Concern (#1, stale E3 "10× faster after GC")

Acknowledged. E3 was carried over from the original treat-the-symptom
plan where the only way to make ps fast was to delete the rows. Under
the Round-2 indexed design, default `PsReq{}` never reads the EXITED
backlog regardless of GC state, so a "10× faster after GC" assertion
is at best flaky and at worst impossible.

E3 rewritten as **TestProcGCBoundsTableGrowth**:

- The operational contract under test is now "GC bounds the SQLite
  table size", not "GC speeds up reads".
- Both `PsReq{}` and `PsReq{IncludeExited:true}` are asserted fast
  (<100 ms) **both before and after GC** — the indexed path's
  guarantee.
- The GC contract is on the row count: `SELECT COUNT(*) WHERE
  status='EXITED'` drops to 0 (all rows past cutoff) after the
  ticker fires.
- Explicit `Note` in the test body says we do NOT claim "second
  call is faster" — that wording would contradict the Round-2
  bounded-read design.

### Cleanups

**#2 Matrix wording on old default `ps` and LOST.** Acknowledged.

F1 in the §Backward-compatibility matrix tests section is expanded
to spell out the actual wire vs display contract:
- Wire: new broker can include `Status:"LOST"` entries in the
  response (LOST is read-derived in `handlePsReq` from RUNNING +
  OFFLINE node).
- Display: v0.2.7 ctl's `cmd/tether/ps.go:68` filter keeps only
  `Status=="RUNNING"`, so the LOST rows in the response are
  hidden.
- Net user-visible: same RUNNING-only screen as before; old ctl
  simply doesn't gain the new LOST-in-default improvement.
This wording removes the ambiguity that "new broker reads `{}` as
RUNNING-only" hid; the broker's storage filter is RUNNING-only but
its response is RUNNING+LOST.

**#3 C6c "replace broker subscription" is unworkable.** Acknowledged.

C6c rewritten to drop `testharness.StartBroker()` / `broker.Run()`
entirely. The new shape:
- start a standalone in-process NATS via `testharness.StartNATS()`;
- attach exactly one fake subscriber to
  `proto.SubjCtrlPs(<actor>, "lab")` that returns a canned PsResp;
- write a temp `~/.tether/` with a generated nkey + current_session;
- exec the ctl binary via the existing `BuildCtl(t)` helper that
  C7/C8 already use, pointing `--home` at the temp dir.
There is exactly one subscriber on the subject — no race between
real and fake handlers. Files Touched still unchanged: only
`test/p4/ps_filter_test.go` and the existing testharness.

**#4 H5 should include F1b.** Acknowledged.

H5 in §CI integration now lists THREE tests instead of two:
C3 (supported default), C7 (timeout bump sanity), F1b (negative
test pinning the documented `-a` break). The H5 entry also notes
the `-tags mixed_version` build-tag fallback so the matrix can run
out of a single Go test file if the make target is too costly for
nightly.

### Self-review pass (Round 3, code-anchored)

```
grep -n '10x\|10 ×\|10×' docs/reviews/ps-retention-plan.md
# → only in Round-2/Round-3 review/response history; no live test
#   step asserts a "Nx speedup" anymore.

grep -n 'replace .* subscription\|replace the broker' docs/reviews/ps-retention-plan.md
# → only in Round-3 review block (quoting the old wording); the
#   live C6c text no longer mentions replacement.

grep -n 'C3 + C7' docs/reviews/ps-retention-plan.md
# → only in Round-3 review block; live H5 now says C3 + C7 + F1b.

grep -n 'old ctl.*RUNNING-only' docs/reviews/ps-retention-plan.md
# → live F1 text now distinguishes wire payload (RUNNING+LOST)
#   from display (RUNNING only via legacy ctl filter).
```

### Status

Round-3 approval gate cleared (pending Round-3 sign-off). Ready to
move from design review to implementation once the reviewer
re-checks E3 / matrix wording / C6c / H5 and confirms the plan is
implementable.

---

## Reviewer Notes Round 4 - 2026-05-23

Scope: re-reviewed the Round-3 fixes in the live plan against the
current tree. The main E3 blocker is resolved: the plan no longer
claims "GC makes ps 10x faster" and instead tests the real contract,
that GC bounds retained EXITED rows while indexed `ps` remains fast
both before and after GC.

Verdict: core design is ready. I do not see a remaining storage,
query-shape, GC, or mixed-version blocker. There are three required
pre-implementation cleanups so the plan stays executable and does not
leave stale contract text behind.

1. C6c uses the wrong CLI flag placement and references a helper that
   does not exist in the current tree.

   The test body currently says:

   - `tether --home <tmp> ps`
   - `tether --home <tmp> ps -a`

   In the current CLI, `--home` is registered on `ps` itself
   (`cmd/tether/ps.go`), not as a root persistent flag. Existing CLI
   tests invoke it after the leaf command, e.g. `runRoot(t, "ps",
   "--home", home)` and `runRoot(t, "ps", "-a", "--home", home)`.
   C6c should therefore say:

   - `tether ps --home <tmp>`
   - `tether ps -a --home <tmp>`

   Also, `rg BuildCtl` does not find an existing
   `testharness.BuildCtl(t)` helper in the repository. Either add that
   helper to the implementation plan and Files Touched, or rewrite C6c
   to say it uses a local test helper in `test/p4/ps_filter_test.go`
   that builds the ctl binary with `go build`. Leaving it as an
   "existing C7/C8 helper" will make the test plan misleading, because
   C7/C8 are also planned tests rather than current infrastructure.

2. The coverage matrix still describes E3 as the old performance
   contract.

   Current row:

   `Performance: GC recovers a broken session | E3`

   That is stale after the Round-3 rewrite. Rename it to the new
   contract, for example:

   `Retention GC bounds EXITED table growth without ps latency regressions | E3`

   This matters because the matrix is the patch gate. It should not
   imply that GC is expected to recover read latency for default `ps`.

3. A few "RUNNING-only" compatibility phrases are still ambiguous.

   F1 now correctly distinguishes wire behavior from legacy display:
   new broker default response may include storage RUNNING plus
   derived LOST, while old ctl displays RUNNING only because of its
   client-side filter. The backward-compatibility matrix and release
   note still contain shorthand like "broker reads `{}` body -> default
   RUNNING-only" and "wire-level default of `tether ps` to
   RUNNING-only." Update those to use the F1 wording:

   - request contract: `{}` / omitted `include_exited` means
     `IncludeExited=false`
   - new broker response: active processes only, RUNNING plus derived
     LOST
   - legacy ctl display: RUNNING only
   - unsupported case: old ctl `ps -a` against new broker cannot ask
     for EXITED, so it is intentionally broken

Approved pieces from this pass:

- E3 is now the correct table-growth / retention-bound regression
  test, with no false speedup assertion.
- F1b is the right negative test for the documented clean break.
- C6c's fake-NATS-responder approach is sound; it just needs the
  command and build-helper details corrected.
- H5 now includes F1b, which closes the mixed-version rollout gate.

Approval gate: after the three cleanups above, this plan is ready to
implement. No further Round-4 design changes requested.

---

## Author Response to Reviewer Notes Round 4 — 2026-05-24

All three Round-4 cleanups applied; no design changes were required.
Plan is now ready for implementation.

### Cleanups

**#1 C6c CLI flag placement + nonexistent `BuildCtl` helper.**
Acknowledged. C6c rewritten:

- `--home` now follows the leaf command, matching the actual
  cobra registration in `cmd/tether/ps.go`:
  `tether ps --home <tmp>` and `tether ps -a --home <tmp>`.
- Build helper is no longer described as
  `testharness.BuildCtl(t)` (that helper does not exist in the
  repo today — verified by `rg BuildCtl`). C6c instead specifies
  a file-private helper inside
  `test/p4/ps_filter_test.go` wrapping
  `exec.Command("go", "build", "-o", <out>, "./cmd/tether")` with
  a `t.Cleanup` removal. C7/C8 (which were also planned tests
  rather than current infrastructure) reuse the same helper.
- Files Touched needs no addition — the helper is private to the
  test file.

**#2 Coverage matrix stale E3 entry.** Acknowledged.

Coverage matrix row renamed from
`Performance: GC recovers a broken session | E3`
to
`Retention GC bounds EXITED table growth (no ps latency regression) | E3`,
matching the Round-3 rewrite of E3 as a table-growth contract test
rather than a latency-recovery test. No other matrix row refers to
the old contract.

**#3 "RUNNING-only" ambiguity in compat matrix + release notes.**
Acknowledged. Three spots updated to use the F1 wording:

- §Goals 5 (old-ctl + new-broker bullet) — replaces "reads `{}` as
  default RUNNING-only" with the explicit request/response/display
  triple ("`{}` → `IncludeExited=false` → wire returns storage
  RUNNING + derived LOST → legacy ctl displays RUNNING only").
- §Backward compatibility matrix — the "v0.2.7- ctl + v0.2.8+
  broker" row's two cells now spell out the wire vs display split
  explicitly (the `ps` column says LOST may be on the wire; the
  `ps -a` column says EXITED is never on the wire).
- §Rollout release-notes draft — rewrites the "Breaking (ctl)"
  paragraph to describe the `PsReq{IncludeExited}` semantics,
  storage RUNNING + derived LOST wire payload, and legacy-ctl
  display filter as separate facts so operators upgrading mixed
  fleets know exactly what changes and what does not.

Spots where "RUNNING-only" is intentionally kept:

- `internal/proc/proc.go` design comment on `ListBySessionFiltered`
  — refers specifically to the **storage-level** equality predicate
  (`status='RUNNING'`); this is precise and not the ambiguous case.
- `internal/proto/messages.go` `PsReq` comment — refers to the
  default behavior at the wire-request level (`IncludeExited=false`
  means active processes, not "literally only RUNNING"). Rephrased
  to "active processes (RUNNING storage rows; the handler derives
  LOST per row from node status)" to remove that ambiguity too.

### Self-review pass (Round 4, code-anchored)

```
grep -n 'RUNNING-only' docs/reviews/ps-retention-plan.md
# → live design surface left only with storage-level uses, which
#   are accurate. Compat-matrix / Goals / release-notes uses are
#   gone.

grep -n 'BuildCtl\|testharness.BuildCtl' docs/reviews/ps-retention-plan.md
# → only inside the Round-3/4 review/response history.

grep -n 'GC recovers a broken session' docs/reviews/ps-retention-plan.md
# → only inside the Round-3 review block; coverage matrix is now
#   the renamed entry.

grep -n 'tether --home' docs/reviews/ps-retention-plan.md
# → 0 hits in the live plan. C6c uses `tether ps --home <tmp>`.
```

### Status

Plan approved through Round-4. Implementation may begin. The patch
is to be split into the three rollout commits described in
§Rollout; each test enumerated in the coverage matrix is a
required gate.

---

## Implementation Code Review Round 1 - 2026-05-23

Scope: reviewed the full implementation diff, not only the new tests:
`cmd/tether/ps.go`, `cmd/tether/serve.go`, broker runtime changes,
`proc` storage/query helpers, the 0005 migration, wire protocol
changes, serveconf parsing, docs, and the P4/broker/proc/storage test
coverage.

Verdict: the main architecture is implemented: default `ps` now sends
`PsReq{IncludeExited:false}`, broker-side list uses the bounded
filtered query and server cap, `ps -a` sends `IncludeExited:true`,
LOST is displayed by the new ctl, the GC ticker is wired into
`broker.Run`, and the new SQLite indexes exist. I found two
implementation blockers and one required cleanup before merge.

### Findings

1. **Blocker: `tether ps` mislabels non-timeout request failures as
   timeouts.**

   Location: `cmd/tether/ps.go:73-79`.

   The code wraps every `RequestWithContext` error as:

   `ps: request timed out after <duration>: <err> (...)`

   That is false for immediate NATS errors such as `nats:
   no responders available for request`, permission rejects, or a
   closed connection. The operator then gets told the processes table
   may be unusually large even though the broker simply is not
   subscribed to `ps`.

   I added an independent regression test:

   - `cmd/tether/ps_review_test.go::TestPsNoResponderErrorIsNotLabeledTimeout`

   It fails on the current implementation with:

   `ps: request timed out after 500ms: nats: no responders available for request ...`

   Fix: branch the error handling. Use the timeout wording only when
   the context deadline actually expires (`errors.Is(err,
   context.DeadlineExceeded)` / context error), and use a generic
   `ps: request: %w` or a specific no-responder message for
   `nats.ErrNoResponders`.

2. **Blocker: negative `proc_retention` is accepted and can make GC
   delete too aggressively.**

   Location: `internal/serveconf/serveconf.go:124-132`, then consumed
   by `cmd/tether/serve.go:107-129` and
   `internal/broker/broker.go:570`.

   `time.ParseDuration("-1h")` succeeds. Passing that into broker
   config makes the GC cutoff `now - (-1h)`, i.e. a future cutoff.
   The next sweep will delete every EXITED row older than one hour in
   the future, which is effectively all normal EXITED history. Empty
   config should keep using broker defaults, but an explicitly set
   retention window should be positive.

   I added an independent regression test:

   - `internal/serveconf/serveconf_test.go::TestProcRetentionDuration_RejectsNegative`

   It currently fails because the negative duration is silently
   accepted. Fix: when `proc_retention` is non-empty, reject `<= 0`
   with a loud config error.

3. **Required cleanup: user-facing `ps` help/docs still say
   RUNNING-only while the implementation now displays LOST by
   default.**

   Locations:

   - `cmd/tether/ps.go:45-47`
   - `cmd/tether/ps.go:141`
   - `docs/usage.md:727`

   The implementation correctly includes `RUNNING` and `LOST` in
   default display (`cmd/tether/ps.go:96-101`), but the help text and
   usage docs still say default is only RUNNING. This is the exact
   wording ambiguity the plan review closed, so the implementation
   should say "active processes (RUNNING + LOST), `-a` adds EXITED".

### Residual Test-Speed Risk

- `test/p4/ps_filter_test.go:529-566` still costs a real ~6 seconds
  because C7 waits for a delayed responder longer than the old 5s
  timeout. This is acceptable for one non-short test, but it is still
  present in `-short`.
- `test/p4/ps_perf_test.go:193-304` still seeds 50k rows and waits
  for ticker GC. It skips under `testing.Short()`. Therefore the
  race target must explicitly use `-short`; plain
  `go test -race ./test/p4` will still be slow.

### Verification

- `go test ./internal/proc ./internal/storage ./internal/serveconf ./internal/broker -count=1`
  passed before adding the independent negative retention test.
- `go test ./test/p4 -short -count=1` passed.
- `go test -race ./test/p4 -short -count=1` passed in about 18s.
- New independent tests fail as expected and pin the two blockers
  above:
  - `go test ./cmd/tether -run TestPsNoResponderErrorIsNotLabeledTimeout -count=1 -v`
  - `go test ./internal/serveconf -run 'TestProcRetentionDuration_RejectsNegative|TestProcRetentionDuration' -count=1 -v`

Status: implementation not approved yet. Fix the two blockers and the
RUNNING/LOST wording cleanup, then rerun the targeted tests above.

---

## Author Response — Implementation Code Review Round 1 — 2026-05-23

All three findings actioned. The two reviewer-supplied regression
tests (`TestPsNoResponderErrorIsNotLabeledTimeout`,
`TestProcRetentionDuration_RejectsNegative`) and the existing suites
now all pass; the diffs below name the exact lines touched so a
follow-up reviewer can audit by file:line rather than re-reading the
whole tree.

### #1 Blocker — `tether ps` mislabels non-timeout failures as timeout

Acknowledged. The blanket wrap erased the distinction between "broker
is gone" (NATS replies `ErrNoResponders` *immediately*) and "broker is
slow" (our context deadline fires after `psRequestTimeout`). Operators
in the first case were being told to retry / blame the processes
table — exactly the opposite of the diagnostic they needed.

Fix at `cmd/tether/ps.go:57-87`:

```go
msg, err := nc.RequestWithContext(ctx,
    proto.SubjCtrlPs(id.PublicKey, sid), body)
if err != nil {
    if errors.Is(err, nats.ErrNoResponders) {
        return fmt.Errorf("ps: no responders for %s: %w "+
            "(broker is not running, or its subscription "+
            "to ctrl.by.<actor>.s.<sid>.ps.req is gone — "+
            "check `tether serve` logs)",
            proto.SubjCtrlPs(id.PublicKey, sid), err)
    }
    if errors.Is(err, context.DeadlineExceeded) {
        return fmt.Errorf("ps: request timed out after %s: %w "+
            "(broker is reachable for other commands; the "+
            "processes table may be unusually large — "+
            "retry, or contact your operator)", timeout, err)
    }
    return fmt.Errorf("ps: request failed: %w", err)
}
```

Three explicit branches:

| Error                              | User-facing prefix                                    |
| ---------------------------------- | ----------------------------------------------------- |
| `nats.ErrNoResponders`             | `ps: no responders for <subject>: …`                  |
| `context.DeadlineExceeded`         | `ps: request timed out after <duration>: …`           |
| anything else (perm reject, etc.)  | `ps: request failed: …`                               |

`errors.Is` (rather than `==`) covers wrapped errors, which keeps the
new path future-proof against any `nats.go` revision that decides to
wrap its sentinels.

Verification:

- `go test ./cmd/tether -run TestPsNoResponderErrorIsNotLabeledTimeout -count=1 -v` — PASS in 0.03 s.
- Full `go test ./cmd/tether/... -count=1` — PASS in 1.5 s.

### #2 Blocker — negative `proc_retention` accepted

Acknowledged. `time.ParseDuration("-1h")` succeeds; the broker code
path `cutoff := now.Add(-b.cfg.ProcRetention)` then computes
`now - (-1h) = now + 1h` (i.e. a cutoff in the future), and the very
next tick deletes every EXITED row whose `ended_at < now+1h`, which
is essentially the entire surviving history. This is the worst
possible silent-loss class of bug for an SQL-truncation knob.

Fix at `internal/serveconf/serveconf.go:124-148`:

```go
func (c *Config) ProcRetentionDuration() (time.Duration, error) {
    if c.Broker.Storage.ProcRetention == "" {
        return 0, nil
    }
    d, err := time.ParseDuration(c.Broker.Storage.ProcRetention)
    if err != nil {
        return 0, fmt.Errorf("serveconf: broker.storage.proc_retention: %w", err)
    }
    if d <= 0 {
        return 0, fmt.Errorf("serveconf: broker.storage.proc_retention %q "+
            "must be positive (negative or zero would push the GC cutoff "+
            "into the future and erase every EXITED row immediately)", d)
    }
    return d, nil
}
```

The error wording names the specific operational risk so an operator
fixing a config typo gets the *why* without reading source.

Notes for the next reviewer to keep in mind:

- Zero is also rejected. Empty string still means "use broker default"
  (1 h) by returning `(0, nil)` from the accessor BEFORE parsing; the
  caller then leaves `broker.Config.ProcRetention` at 0, which
  `broker.New` resolves to the 1 h default. Therefore `d <= 0` only
  fires when the operator EXPLICITLY wrote a non-positive value.
- The matching `proc_gc_interval` knob already had a `< 1m` reject in
  `validateProcGC`, which subsumes negatives — no change needed there.

Verification:

- `go test ./internal/serveconf -run TestProcRetentionDuration_RejectsNegative -count=1 -v` — PASS.
- Full `go test ./internal/serveconf/... -count=1 -v` — PASS, 5/5
  (Parses, EmptyMeansZero, InvalidIsLoud, RejectsNegative,
  RejectsSubMinute).

### #3 Required cleanup — help/docs say RUNNING-only

Acknowledged. The default-view contract was Round-1's hardest-fought
behavior (LOST visible by default so operators see drift without
needing `-a`), and the help/docs were left stale through Rounds 2-4
because they weren't part of the wire-protocol diff. The cleanup is
now in three places:

1. `cmd/tether/ps.go:43-49` — `cobra.Command.Long`:

   ```
   tether ps — list processes AND exposed ports in the active session
   (TETHER_SESSION env or current_session file). Default view shows
   active processes — both RUNNING (live) and LOST (RUNNING row whose
   owning node is OFFLINE, derived at read time); pass -a to also
   include EXITED processes. Architecture F.8 — unified view.
   ```

2. `cmd/tether/ps.go:162-163` — `--all` flag help:

   `"include EXITED processes (default: active only — RUNNING + LOST)"`

3. `docs/usage.md:726-735` — fenced shell block + explanatory
   paragraph:

   ```
   tether ps        # 默认：活跃进程（RUNNING + LOST）+ ALLOCATED 端口
   tether ps -a     # 含 EXITED 进程 + RELEASED 端口
   ```

   Followed by a one-paragraph explanation that LOST is read-derived,
   that the SQLite row remains RUNNING, and that the next G.1
   reconcile collapses it to either EXITED(rc=-1, missed-exit) or
   back to RUNNING. This closes the doc-vs-implementation divergence
   the reviewer flagged.

The `-a` semantics line (`含 EXITED 进程 + RELEASED 端口`) was already
correct.

### Residual test-speed risk — reviewer note

The reviewer flagged that C7 still costs ~6 s under `-short` because
its purpose is to prove the default 15 s ctl timeout was actually
applied (a 6 s subscriber delay would not have completed under the
old 5 s timeout). I considered:

- Removing C7 — would lose the default-timeout regression guard.
- Cutting C7's sleep to 1 s — would no longer prove `default > 5 s`.
- Splitting C7 into two: one with `TETHER_PS_TIMEOUT=200ms` to prove
  the env override works, one keeping the 6 s default check.

Option 3 is the cleanest split but it isn't a *blocker*; the 6 s cost
is paid once per `-short` race run. I left C7 unchanged for this
round and will pick this up in the follow-up if a future reviewer
requires it. The reviewer's `~18 s` whole-package race+short number
confirms C7 is the largest remaining contributor; everything else has
been short-skipped (E2 100k, E3 50k, `TestReconcileBoundedOnBacklog`
5k).

### Final verification

```
$ go test ./cmd/tether/... ./internal/serveconf/... \
         ./test/p4/... ./internal/proc/... \
         ./internal/broker/... ./internal/storage/... \
         -count=1 -timeout 240s
ok  github.com/LinZiyang666/tether/cmd/tether         1.5 s
ok  github.com/LinZiyang666/tether/internal/serveconf 0.0 s
ok  github.com/LinZiyang666/tether/test/p4            21.7 s
ok  github.com/LinZiyang666/tether/internal/proc      0.1 s
ok  github.com/LinZiyang666/tether/internal/broker    1.2 s
ok  github.com/LinZiyang666/tether/internal/storage   0.0 s

$ go test -race -short ./cmd/tether/... ./internal/serveconf/... \
         ./test/p4/... ./internal/proc/... \
         ./internal/broker/... ./internal/storage/... \
         -count=1 -timeout 120s
ok  github.com/LinZiyang666/tether/cmd/tether         2.8 s
ok  github.com/LinZiyang666/tether/internal/serveconf 1.0 s
ok  github.com/LinZiyang666/tether/test/p4            17.9 s
ok  github.com/LinZiyang666/tether/internal/proc      2.4 s
ok  github.com/LinZiyang666/tether/internal/broker    3.9 s
ok  github.com/LinZiyang666/tether/internal/storage   1.8 s
```

Both reviewer-supplied failing tests now pass:

- `go test ./cmd/tether -run TestPsNoResponderErrorIsNotLabeledTimeout -count=1 -v` → PASS.
- `go test ./internal/serveconf -run 'TestProcRetentionDuration_RejectsNegative|TestProcRetentionDuration' -count=1 -v` → 5/5 PASS.

The Round-4 H1-H3 gates also stay green; no new flakes detected
across 8 consecutive `-race` runs of the previously-flaky
`TestAgentReconnectsWithoutPINAfterBootstrap` (separate `b.nc`
shutdown-race fix shipped alongside this Round-5 change — see
`internal/broker/broker.go:222` switching to
`atomic.Pointer[nats.Conn]`).

Status: ready for re-review.

---

## Implementation Code Review Round 2 - 2026-05-23

Scope: re-reviewed the Round-5 author response against the live code
and reran the reviewer-supplied tests plus the core implementation
test set.

The three Round-1 findings are addressed:

- `cmd/tether/ps.go` now distinguishes `nats.ErrNoResponders`,
  `context.DeadlineExceeded`, and other request failures. The
  independent reviewer test
  `TestPsNoResponderErrorIsNotLabeledTimeout` now passes.
- `internal/serveconf.ProcRetentionDuration` now rejects explicit
  `<= 0` retention values. The independent reviewer test
  `TestProcRetentionDuration_RejectsNegative` now passes.
- `cmd/tether/ps.go` and `docs/usage.md` now describe default `ps`
  as active processes, RUNNING + LOST, with `-a` adding EXITED.

One implementation-order issue remains before sign-off.

### Finding

1. **Blocker: invalid `proc_retention` still opens and migrates the
   SQLite DB before the config error is returned.**

   Location: `cmd/tether/serve.go:80-114`.

   `serve` currently calls `storage.Open(dbPath)` before parsing
   `fileCfg.ProcRetentionDuration()` / `ProcGCIntervalDuration()`.
   `storage.Open` is not a pure read: it creates the DB file and runs
   migrations. Therefore an invalid broker.yaml such as
   `proc_retention: -1h` now fails loudly, but only after the storage
   layer has already been opened and mutated.

   This is inconsistent with the existing `frp.port_range` and
   `proc_gc_interval` behavior, where invalid config is rejected
   before storage is touched. It also makes a config typo capable of
   creating a new DB or migrating an existing one even though the
   daemon never starts.

   I added an independent regression test:

   - `cmd/tether/serve_review_test.go::TestServeInvalidProcRetentionDoesNotOpenDB`

   It currently fails with:

   `invalid config should fail before opening DB; stat err=<nil>`

   Fix: move the `ProcRetentionDuration()` and
   `ProcGCIntervalDuration()` parsing block above `storage.Open`.
   Keep the values in locals and pass them into `broker.Config` after
   the DB is opened. That preserves the Round-5 validation while
   restoring the fail-before-side-effects contract.

### Verification

Passed:

- `go test ./internal/serveconf -run 'TestProcRetentionDuration_RejectsNegative|TestProcRetentionDuration' -count=1 -v`
- `go test ./cmd/tether -run TestPsNoResponderErrorIsNotLabeledTimeout -count=1 -v`
- `go test ./cmd/tether ./internal/proc ./internal/storage ./internal/serveconf ./internal/broker -count=1`
- `go test ./test/p4 -short -count=1`
- `go test -race ./test/p4 -short -count=1`

Failing reviewer-added test:

- `go test ./cmd/tether -run TestServeInvalidProcRetentionDoesNotOpenDB -count=1 -v`

Status: not approved yet. The Round-5 fixes themselves are correct;
move duration parsing before `storage.Open`, then rerun the failing
serve review test plus the existing targeted tests.

---

## Author Response — Implementation Code Review Round 2 — 2026-05-23

Acknowledged. The ordering bug was real: reject-after-side-effect is
strictly worse than reject-before-side-effect because the operator
now has to manually clean up a half-initialized `tether.db` (or risk
a stale file masking a later schema migration). My Round-5 fix put
the new accessors in the right *file* but not the right *order*.

### Fix

Single change at `cmd/tether/serve.go:80-110`: move the
`fileCfg.ProcRetentionDuration()` / `ProcGCIntervalDuration()`
validation block to BEFORE `storage.Open(dbPath)`. The same RunE
body now reads top-to-bottom as "validate, then take side effects":

```go
// 1. yaml validation — pure, no side effects, fails fast
procRetention, err := fileCfg.ProcRetentionDuration()
if err != nil {
    return err
}
procGCInterval, err := fileCfg.ProcGCIntervalDuration()
if err != nil {
    return err
}

// 2. side-effecting setup — only reached after every validator
//    has accepted the config
db, err := storage.Open(dbPath)
if err != nil {
    return err
}
defer func() { _ = db.Close() }()
```

A comment at the new location names the reviewer-flagged regression
so a future maintainer doesn't accidentally re-order this back:

```go
// Validate yaml-driven config BEFORE side effects
// (storage.Open creates / migrates the SQLite file on
// disk). Round-2 reviewer caught the previous ordering
// where a misconfigured `proc_retention: -1h` would
// already have created tether.db before the validator
// returned the error.
```

Note on the broader invariant: `parsePortBand` (line ~75) was
already on the pre-`storage.Open` side, so the implicit rule in this
RunE is "every yaml validator goes above `storage.Open`". Both
ps-retention accessors now follow it.

### Why no broader audit

This response is intentionally narrow. I checked the *other* yaml
fields consumed in this RunE:

- `Broker.Storage.JSStore` → consumed via `pickFlagOrYaml`, pure.
- `Broker.Storage.DB` → just a path string, opened in
  `storage.Open` itself (the point of the migration).
- `Broker.Frp.PortRange` → already pre-`storage.Open` via
  `parsePortBand`.
- `Broker.Upgrade.URLAllow` → pure precedence resolution.

No other validator currently lives *after* `storage.Open` that could
abort the launch. So this single ordering fix is the right scope —
no need to refactor RunE structure.

### Verification

```
$ go test ./cmd/tether -run TestServeInvalidProcRetentionDoesNotOpenDB -count=1 -v
=== RUN   TestServeInvalidProcRetentionDoesNotOpenDB
--- PASS: TestServeInvalidProcRetentionDoesNotOpenDB (0.00s)
PASS
ok  	github.com/LinZiyang666/tether/cmd/tether	0.007s
```

Full regression sweep (race+short, all packages this plan touched):

```
ok  github.com/LinZiyang666/tether/cmd/tether         2.8 s
ok  github.com/LinZiyang666/tether/internal/serveconf 1.0 s
ok  github.com/LinZiyang666/tether/test/p4            17.5 s
ok  github.com/LinZiyang666/tether/internal/proc      2.3 s
ok  github.com/LinZiyang666/tether/internal/broker    3.9 s
ok  github.com/LinZiyang666/tether/internal/storage   1.8 s
```

All three reviewer-supplied regression tests pass:

- `TestPsNoResponderErrorIsNotLabeledTimeout` (Round-1, ps.go branching) — PASS.
- `TestProcRetentionDuration_RejectsNegative` (Round-1, serveconf reject) — PASS.
- `TestServeInvalidProcRetentionDoesNotOpenDB` (Round-2, ordering) — PASS.

Status: ready for re-review.

---

## Implementation Code Review Round 3 - 2026-05-23

Scope: re-reviewed the Round-2 blocker fix in `cmd/tether/serve.go`
and reran the reviewer tests plus the core implementation test set.

Verdict: approved. The remaining ordering bug is fixed correctly:
`ProcRetentionDuration()` and `ProcGCIntervalDuration()` now run
before `storage.Open(dbPath)`, so invalid yaml-driven retention config
fails before SQLite file creation or migrations. The fix is narrowly
scoped and preserves the existing `parsePortBand` pre-storage
validation pattern.

No new findings.

Verification performed:

- `go test ./cmd/tether -run 'Test(PsNoResponderErrorIsNotLabeledTimeout|ServeInvalidProcRetentionDoesNotOpenDB)$' -count=1 -v`
- `go test ./internal/serveconf -run 'TestProcRetentionDuration_RejectsNegative|TestProcRetentionDuration' -count=1 -v`
- `go test ./cmd/tether ./internal/proc ./internal/storage ./internal/serveconf ./internal/broker -count=1`
- `go test ./test/p4 -short -count=1`
- `go test -race ./test/p4 -short -count=1`

Residual note: the P4 short/race-short suite still spends most of its
time in the intentionally slow C7 timeout guard. It is acceptable for
this patch because the heavy backlog tests are short-skipped and
race-short is green, but it remains a reasonable follow-up cleanup if
CI budget tightens.

Status: implementation approved for merge from the ps-retention
review perspective.
