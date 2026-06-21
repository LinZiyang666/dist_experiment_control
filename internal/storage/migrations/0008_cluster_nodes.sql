-- Migration 0008 — cluster_nodes roster (distributed-broker §4.2).
--
-- Raft voting set is authoritative; `phase` is a DERIVED display state (§8.1).
-- NO column carries a CURRENT_TIMESTAMP default: every value is leader-baked at
-- Apply time so all replicas converge byte-for-byte (§3.4/§4.2). D0 only creates
-- schema; the first writer is D7's ClusterNodeUpsert. Empty until
-- `cluster init --from-existing` (D9). Applies cleanly on a pre-cluster DB.
--
-- `phase` CHECK = the canonical ALL-UPPERCASE 6-value set (§4.2/§8.1, casing
-- normalized in D0: the doc previously mixed lowercase draining/retiring with
-- the uppercase values, which a single CHECK cannot encode).
CREATE TABLE cluster_nodes (
    node_id              TEXT      PRIMARY KEY,   -- == Raft ServerID (stable)
    name                 TEXT      NOT NULL,
    node_ident_pub       TEXT      NOT NULL,      -- node-identity nkey pub (≠ bus nkey)
    nats_server_id       TEXT,                    -- deterministic nats-server id; agent self-reports it to bridge home (§6.5); NULL until D3
    raft_addr            TEXT      NOT NULL,
    nats_route           TEXT      NOT NULL,
    tunnel_addr          TEXT      NOT NULL,
    public_host          TEXT      NOT NULL,
    cert_fp              TEXT      NOT NULL,       -- current tunnel-cert fingerprint (§15 RF3)
    cert_fp_prev         TEXT,                     -- non-null only during a rotation window
    cert_fp_valid_until  TIMESTAMP,                -- leader-baked literal; set only during rotation
    phase                TEXT      NOT NULL
                                   CHECK (phase IN (
                                       'JOIN_VERIFIED_PENDING_VOTER',
                                       'CATCHING_UP', 'VOTER', 'VOTER_ADD_FAILED',
                                       'DRAINING', 'RETIRING'
                                   )),
    added_at             TIMESTAMP NOT NULL,       -- leader-baked literal (NO default)
    UNIQUE (name)
);
CREATE INDEX idx_cluster_nodes_phase ON cluster_nodes(phase);
