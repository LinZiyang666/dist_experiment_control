-- 0001_init.sql — initial schema for tetherd's authoritative state.
--
-- Tables match architecture §H.1 (audit_log NOT in SQLite — H.2: audit lives
-- only in JetStream `history-<sid>`). Five tables:
--   sessions, members, nodes, processes, port_allocations.
--
-- session rm cascades through FKs (architecture H.3: explicit DELETEs in code
-- + ON DELETE CASCADE here as a safety net). PRAGMA foreign_keys must be ON
-- (storage.Open enforces).

CREATE TABLE sessions (
    sid              TEXT      PRIMARY KEY,
    name             TEXT      NOT NULL,
    owner_pubkey_fp  TEXT      NOT NULL,
    pin_hash         TEXT      NOT NULL,                       -- argon2id PHC
    state            TEXT      NOT NULL DEFAULT 'ACTIVE'
                                CHECK (state IN ('ACTIVE','DELETING')),
    created_at       TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    deleting_at      TIMESTAMP                                  -- set when state→DELETING
);

CREATE TABLE members (
    sid          TEXT      NOT NULL REFERENCES sessions(sid) ON DELETE CASCADE,
    pubkey_fp    TEXT      NOT NULL,
    role         TEXT      NOT NULL CHECK (role IN ('owner','member')),
    via          TEXT      NOT NULL CHECK (via IN ('create','pin')),
    joined_at    TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (sid, pubkey_fp)
);

CREATE TABLE nodes (
    nid                TEXT      NOT NULL,
    sid                TEXT      NOT NULL REFERENCES sessions(sid) ON DELETE CASCADE,
    last_heartbeat_at  TIMESTAMP,
    status             TEXT      NOT NULL DEFAULT 'ONLINE'
                                  CHECK (status IN ('ONLINE','STALE','OFFLINE')),
    boot_id            TEXT,
    release_version    TEXT,
    proto_version      INTEGER,
    tags               TEXT      NOT NULL DEFAULT '{}',         -- JSON object
    registered_at      TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (sid, nid)
);

CREATE TABLE processes (
    pid                TEXT      PRIMARY KEY,                   -- ULID
    sid                TEXT      NOT NULL,
    nid                TEXT      NOT NULL,
    argv               TEXT      NOT NULL,                      -- JSON array
    cwd                TEXT,
    env                TEXT,                                    -- JSON object, optional
    started_at         TIMESTAMP NOT NULL,
    ended_at           TIMESTAMP,
    status             TEXT      NOT NULL
                                  CHECK (status IN ('RUNNING','EXITED','LOST')),
    exit_code          INTEGER,
    boot_id            TEXT,                                    -- agent's /proc boot_id
    start_time_ticks   INTEGER,                                 -- /proc/<pid>/stat field 22
    started_by_fp      TEXT      NOT NULL,
    FOREIGN KEY (sid, nid) REFERENCES nodes(sid, nid) ON DELETE CASCADE
);

CREATE INDEX idx_processes_sid_nid ON processes(sid, nid);
CREATE INDEX idx_processes_status   ON processes(status);

CREATE TABLE port_allocations (
    port           INTEGER   PRIMARY KEY,                       -- 14000-14999 in v1
    sid            TEXT      NOT NULL,
    nid            TEXT      NOT NULL,
    name           TEXT      NOT NULL,
    local_port     INTEGER   NOT NULL,
    token_hash     TEXT      NOT NULL,                          -- SHA256(token); raw token never stored
    state          TEXT      NOT NULL
                              CHECK (state IN ('ALLOCATED','REVOKED','FREED')),
    created_by_fp  TEXT      NOT NULL,
    created_at     TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    revoked_at     TIMESTAMP,
    FOREIGN KEY (sid, nid) REFERENCES nodes(sid, nid) ON DELETE CASCADE
);

CREATE INDEX idx_port_alloc_sid_nid ON port_allocations(sid, nid);
CREATE INDEX idx_port_alloc_state    ON port_allocations(state);
