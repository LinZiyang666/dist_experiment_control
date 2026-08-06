-- 0018 (h1 E2): admit 'proxy_bind_stalled' into the alerts.kind CHECK.
--
-- SQLite cannot ALTER a CHECK constraint, so this is the standard rebuild:
-- new table with the widened enum (a STRICT SUPERSET of 0009's — every
-- existing row satisfies the new constraint by construction), copy, swap,
-- recreate the partial-unique dedup index.
--
-- proxy_bind_stalled is the leader-raised, info-severity alert for a node
-- whose __proxy__ data-plane tunnel repeatedly fails to bind (the reaper's
-- rotation loop is in backoff — see internal/broker/proxy_reconcile.go).
-- Severity is 'info' BY DESIGN (h1 plan Q7 ruling): 'severe' feeds the ctl
-- banner + the --ack-alerts friction gate on destructive commands, and one
-- node's broken laptop link must not tax every push/expose/run for however
-- long that laptop stays broken. `tether alert ls` + the webhook still see it.
--
-- N-1 note (docs/deploy-tier-gotchas.md, h1): once a proxy_bind_stalled row
-- has ever been raised, a from-scratch raft-LOG REPLAY on a pre-h1 binary
-- fails this row's 0009 CHECK (fail-stop). Rollback below this release is
-- snapshot-restore only. Lockstep-upgrade rule: all brokers install this
-- release before the first alert of the new kind can flow.

CREATE TABLE alerts_new (
    id          TEXT      PRIMARY KEY,            -- ULID, leader-baked (§3.4)
    kind        TEXT      NOT NULL
                          CHECK (kind IN (
                              'manual', 'below_quorum', 'broker_draining',
                              'broker_down', 'replication_degraded',
                              'disk_pressure', 'raft_lag',
                              'proxy_bind_stalled'
                          )),
    severity    TEXT      NOT NULL CHECK (severity IN ('severe', 'info')),
    dedup_key   TEXT      NOT NULL,
    state       TEXT      NOT NULL DEFAULT 'ACTIVE'
                          CHECK (state IN ('ACTIVE', 'CLEARED')),
    message     TEXT      NOT NULL,
    raised_at   TIMESTAMP NOT NULL,               -- leader-baked literal (NO default)
    cleared_at  TIMESTAMP
);

INSERT INTO alerts_new (id, kind, severity, dedup_key, state, message, raised_at, cleared_at)
SELECT id, kind, severity, dedup_key, state, message, raised_at, cleared_at FROM alerts;

DROP TABLE alerts;
ALTER TABLE alerts_new RENAME TO alerts;

-- At most one ACTIVE alert per dedup_key (history rows model, mirrors 0009).
CREATE UNIQUE INDEX idx_alerts_dedup_active ON alerts(dedup_key) WHERE state = 'ACTIVE';
