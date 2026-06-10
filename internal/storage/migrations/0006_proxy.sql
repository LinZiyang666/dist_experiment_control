-- Migration 0006 — P13 session-scoped proxy subscription.
-- See docs/reviews/p13-plan.md §3 for the full design.
--
-- Adds: a per-session master switch (sessions.proxy_enabled), a per-node
-- "SS server is bound" ACK flag (nodes.proxy_ready), the per-subscriber
-- revocable Shadowsocks credential table (proxy_subscribers), and a partial
-- unique index that keeps each agent to at most one ALLOCATED proxy port.
--
-- All statements apply cleanly on a pre-P13 DB: existing sessions get
-- proxy_enabled=0 and existing nodes get proxy_ready=0 (proxy OFF by default,
-- so the whole feature is inert until an owner runs `tether proxy on`).

-- Master switch: dies with the sessions row (no separate table).
ALTER TABLE sessions ADD COLUMN proxy_enabled INTEGER NOT NULL DEFAULT 0
    CHECK (proxy_enabled IN (0, 1));

-- Monotonic keyset version, bumped on every enable / sub create / sub revoke.
-- Ordering on the agent is by the (generation, epoch) pair (see 0007 and
-- architecture L.3), which keeps a broker DB restore convergent.
ALTER TABLE sessions ADD COLUMN proxy_epoch INTEGER NOT NULL DEFAULT 0;

-- Render gate: set to 1 when the agent ACKs that its embedded SS server is
-- bound and the tunnel is up; a node is rendered into /sub only when ready.
-- A pre-P13 agent never sends the ACK, so it stays 0 and is excluded.
ALTER TABLE nodes ADD COLUMN proxy_ready INTEGER NOT NULL DEFAULT 0
    CHECK (proxy_ready IN (0, 1));

-- Capability gate: set at register from the agent's explicit proto.CapProxyV1
-- capability (release_version is only a secondary signal). The broker only
-- allocates/pushes proxy directives to capable nodes.
ALTER TABLE nodes ADD COLUMN proxy_capable INTEGER NOT NULL DEFAULT 0
    CHECK (proxy_capable IN (0, 1));

-- One ACTIVE Shadowsocks key per subscriber. token_hash is the bearer
-- subscription token (SHA256, raw never stored); psk is the SS password,
-- stored RECOVERABLE because the agent must decrypt with it and the /sub
-- renderer must vend it (the DB is already the 0600 crown-jewel store next to
-- pin_hash). See p13-plan.md §7 residual R-2.
CREATE TABLE proxy_subscribers (
    sid           TEXT      NOT NULL REFERENCES sessions(sid) ON DELETE CASCADE,
    sub_id        TEXT      NOT NULL,            -- ULID; stable internal id (== ssproxy KeyID)
    name          TEXT      NOT NULL,            -- owner label, e.g. "alice"
    token_hash    TEXT      NOT NULL,            -- SHA256(raw subscription token)
    psk           TEXT      NOT NULL,            -- base64 Shadowsocks password (recoverable)
    cipher        TEXT      NOT NULL,            -- locked AEAD method string
    state         TEXT      NOT NULL DEFAULT 'ACTIVE'
                              CHECK (state IN ('ACTIVE', 'REVOKED')),
    created_by_fp TEXT      NOT NULL,
    created_at    TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    revoked_at    TIMESTAMP,
    PRIMARY KEY (sid, sub_id)
);

-- Bearer-token lookup for GET /sub/<token>: at most one ACTIVE row per hash.
CREATE UNIQUE INDEX idx_proxy_sub_token_active
    ON proxy_subscribers(token_hash) WHERE state = 'ACTIVE';

-- Owner-facing name uniqueness: at most one ACTIVE subscriber per (sid, name);
-- a revoked name can be re-created.
CREATE UNIQUE INDEX idx_proxy_sub_name_active
    ON proxy_subscribers(sid, name) WHERE state = 'ACTIVE';

CREATE INDEX idx_proxy_sub_sid_state ON proxy_subscribers(sid, state);

-- At most one ALLOCATED proxy port per agent (sid, nid). The proxy port is a
-- port_allocations row with the reserved name '__proxy__'; this makes a
-- double-allocation impossible even if agent state.json is lost.
CREATE UNIQUE INDEX idx_port_alloc_proxy_unique
    ON port_allocations(sid, nid) WHERE state = 'ALLOCATED' AND name = '__proxy__';
