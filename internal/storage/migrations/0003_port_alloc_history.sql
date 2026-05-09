-- 0003_port_alloc_history.sql — let `port_allocations` keep historical
-- REVOKED/FREED rows for the same port number, while still enforcing
-- "at most one ALLOCATED row per port".
--
-- Why: architecture D.4 says a port number returns to the pool the
-- moment it transitions to REVOKED/FREED, AND that the row is kept for
-- audit. The original 0001 schema made `port` the primary key, which
-- is incompatible with the second goal — re-allocating port 14022
-- would conflict with the surviving REVOKED 14022 row.
--
-- Fix: synthetic INTEGER PRIMARY KEY (`row_id`) + a partial unique
-- index on `port WHERE state='ALLOCATED'`. Multiple rows for the same
-- `port` are allowed as long as at most one is ALLOCATED at any time.
--
-- SQLite has no DROP CONSTRAINT, so we go via the standard
-- new-table-rename pattern. The 0001-init test harness applies all
-- migrations in order, so any code that reads `port_allocations` after
-- this migration sees the new shape.

CREATE TABLE port_allocations_new (
    row_id         INTEGER   PRIMARY KEY AUTOINCREMENT,
    port           INTEGER   NOT NULL,
    sid            TEXT      NOT NULL,
    nid            TEXT      NOT NULL,
    name           TEXT      NOT NULL,
    local_port     INTEGER   NOT NULL,
    token_hash     TEXT      NOT NULL,
    state          TEXT      NOT NULL
                              CHECK (state IN ('ALLOCATED','REVOKED','FREED')),
    created_by_fp  TEXT      NOT NULL,
    created_at     TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    revoked_at     TIMESTAMP,
    FOREIGN KEY (sid, nid) REFERENCES nodes(sid, nid) ON DELETE CASCADE
);

INSERT INTO port_allocations_new
    (port, sid, nid, name, local_port, token_hash, state, created_by_fp, created_at, revoked_at)
SELECT port, sid, nid, name, local_port, token_hash, state, created_by_fp, created_at, revoked_at
FROM port_allocations;

DROP TABLE port_allocations;
ALTER TABLE port_allocations_new RENAME TO port_allocations;

CREATE UNIQUE INDEX idx_port_alloc_unique_active
    ON port_allocations(port)
    WHERE state='ALLOCATED';
CREATE INDEX idx_port_alloc_sid_nid ON port_allocations(sid, nid);
CREATE INDEX idx_port_alloc_state   ON port_allocations(state);
