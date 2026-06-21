-- Migration 0010 — port_allocations cluster columns (§4.2, §7.1–7.3).
--
-- ALTER ADD COLUMN, NOT a table rebuild: this preserves the row_id
-- INTEGER PRIMARY KEY AUTOINCREMENT from 0003 (§3.6/§4.2) and leaves every
-- historical default and index (idx_port_alloc_unique_active, the FK to
-- nodes(sid,nid) ON DELETE CASCADE) untouched.
--
-- New-column defaults are deterministic CONSTANTS (§3.4):
--   * rebuild_on_failure = 1  -> rebuild ON by default (§7.3)
--   * home_broker        = '' -> NOT 'self'; there is no "self" concept until D7.
--                               D9 `cluster init --from-existing` backfills =self
--                               for live rows. D0 must NOT backfill (先父后子).
--   * epoch              = 0  -> baseline; ReassignHome advances it (§7.2).
-- No writer fills home_broker/epoch in D0; the tunnelTokenLookup home==self/epoch
-- variant lands in D6 and the one-time backfill in D9 (§11).
ALTER TABLE port_allocations ADD COLUMN rebuild_on_failure INTEGER NOT NULL DEFAULT 1
    CHECK (rebuild_on_failure IN (0, 1));
ALTER TABLE port_allocations ADD COLUMN home_broker TEXT NOT NULL DEFAULT '';
ALTER TABLE port_allocations ADD COLUMN epoch INTEGER NOT NULL DEFAULT 0;

-- Serves tunnelTokenLookup's home_broker==self filter (§7.1–7.2). NON-UNIQUE:
-- per-port uniqueness is already enforced by 0003's idx_port_alloc_unique_active
-- (UNIQUE on port WHERE state='ALLOCATED'); this index is purely for lookup.
CREATE INDEX idx_port_alloc_home_active
    ON port_allocations(home_broker, port) WHERE state = 'ALLOCATED';
