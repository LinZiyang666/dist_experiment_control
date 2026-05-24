-- Migration 0005 — indexes that make `tether ps` and the broker's
-- periodic EXITED GC scale independently of total session history.
-- See docs/reviews/ps-retention-plan.md for the full design.
--
-- All three are CREATE INDEX IF NOT EXISTS so the migration is
-- idempotent (re-applying a 0005 is a no-op). 0001's existing
-- (sid, nid) and (status) indexes are NOT dropped.

-- Default `tether ps` query plan:
--   WHERE sid=? AND status='RUNNING' ORDER BY started_at DESC LIMIT ?
-- (sid, status) is equality on the leading two columns; trailing
-- (started_at DESC) lets SQLite read rows already in the requested
-- order and skip TEMP B-TREE sort.
CREATE INDEX IF NOT EXISTS idx_processes_sid_status_started
    ON processes(sid, status, started_at DESC);

-- `tether ps -a` (IncludeExited=true) query plan:
--   WHERE sid=? ORDER BY started_at DESC LIMIT ?
-- Without this index the server-side LIMIT cap would still require
-- a full session sort before the cap; with it the planner reads
-- rows in index order and stops at LIMIT.
CREATE INDEX IF NOT EXISTS idx_processes_sid_started
    ON processes(sid, started_at DESC);

-- Periodic GC sweep:
--   DELETE FROM processes WHERE status='EXITED' AND ended_at < ?
-- (status, ended_at) places the equality column first so the
-- range predicate on ended_at runs over a tight contiguous prefix.
CREATE INDEX IF NOT EXISTS idx_processes_status_endedat
    ON processes(status, ended_at);
