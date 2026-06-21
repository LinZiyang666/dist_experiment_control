-- Migration 0009 — cluster_meta KV + replicated alerts (single cluster-level ack).
--
-- cluster_meta = the Raft FSM KV sidecar (applied_index/applied_term/bootstrapped,
-- §11). Per §18.3: there is NO cluster_revoked_identities table (no writer/reader),
-- and the alert ack is a SINGLE cluster-level ack (PK = dedup_key alone; acked_by
-- is display-only, not per-identity bookkeeping).
--
-- NO CURRENT_TIMESTAMP defaults anywhere (leader bakes time literals for
-- determinism, §3.4). D0 = schema only, NO seed rows: cluster_meta's
-- applied_index/bootstrapped rows are written by D1 Apply (upsert-on-first-Apply,
-- §3.2) and/or the D9 `cluster init --from-existing` seed (§11); the alerts writer
-- arrives in D8b.
--
-- alerts.kind CHECK is the REPLICATION-STORE-BACKED set ONLY (§10.4): the
-- client-synthesized severe gating kinds quorum_lost / force_single_active are
-- NEVER written to this replicated table (they fire precisely when quorum is lost
-- and a Raft write is impossible), so they are deliberately excluded from the CHECK.
-- disk_pressure / raft_lag are kept (catalog kinds, §10.2; D8b may persist them).
CREATE TABLE cluster_meta (
    key    TEXT PRIMARY KEY,
    value  TEXT NOT NULL
);

CREATE TABLE alerts (
    id          TEXT      PRIMARY KEY,            -- ULID, leader-baked (§3.4)
    kind        TEXT      NOT NULL
                          CHECK (kind IN (
                              'manual', 'below_quorum', 'broker_draining',
                              'broker_down', 'replication_degraded',
                              'disk_pressure', 'raft_lag'
                          )),
    severity    TEXT      NOT NULL CHECK (severity IN ('severe', 'info')),
    dedup_key   TEXT      NOT NULL,
    state       TEXT      NOT NULL DEFAULT 'ACTIVE'
                          CHECK (state IN ('ACTIVE', 'CLEARED')),
    message     TEXT      NOT NULL,
    raised_at   TIMESTAMP NOT NULL,               -- leader-baked literal (NO default)
    cleared_at  TIMESTAMP
);
-- At most one ACTIVE alert per dedup_key (history rows model, mirrors 0003/0004).
CREATE UNIQUE INDEX idx_alerts_dedup_active ON alerts(dedup_key) WHERE state = 'ACTIVE';

CREATE TABLE alert_acks (
    dedup_key  TEXT      NOT NULL,
    acked_by   TEXT      NOT NULL,                -- display only (§18.3), not per-identity
    acked_at   TIMESTAMP NOT NULL,               -- leader-baked literal (NO default)
    PRIMARY KEY (dedup_key)
);
