-- Migration 0015 — cluster_operations (C4 operation controller).
--
-- The REPLICATED, RESUMABLE operation log for membership join/retire workflows. It is the SSOT for
-- the named workflow CURSOR (op_state), the operator-confirm bit, the captured catch-up barrier +
-- deadline, last_error, and the durable transition timeline — NOT for membership itself. Membership
-- stays authoritative in cluster_nodes.phase + the live raft configuration + topology_generation +
-- the broker_draining marker (the C4 SSOT split): the two never overlap as authorities, and the
-- membership-window display states are projected LIVE from the substrate in `ops show`.
--
-- IMPORTANT (C4 §0): NO sql CHECK on kind/op_state and NO partial-UNIQUE index. These ops ride the
-- shared genericExecApplier, whose constraint failure is a PLAIN error → the FSM retries → panics on
-- EVERY replica = a cluster brick (the deterministic poison-skip path is custom-applier-only, used
-- only by clusterNodeUpsertApplier). Enum validity is validated in Go on the leader bake surface, and
-- single-active-op is enforced by a baked `INSERT ... SELECT ... WHERE NOT EXISTS(active op for
-- target)` guard (a RowsAffected==0 no-op, never a constraint trap).
--
-- All timestamps are leader-baked literals (§3.4 determinism) — NO CURRENT_TIMESTAMP. Additive: a
-- pre-cluster / non-cluster broker never reads or writes this table (byte-equivalent single mode).
CREATE TABLE cluster_operations (
    op_id            TEXT      PRIMARY KEY,            -- leader-baked stable handle (= plan-id)
    kind             TEXT      NOT NULL,               -- 'join' | 'retire' (validated in Go)
    target_node      TEXT      NOT NULL,
    op_state         TEXT      NOT NULL,               -- named workflow cursor (validated in Go)
    confirmed        INTEGER   NOT NULL DEFAULT 0,     -- replicated F==0 typed-confirm bit
    confirmed_ft     INTEGER   NOT NULL DEFAULT 0,     -- FaultTolerance shown at confirm (resume worsen-check)
    barrier          INTEGER   NOT NULL DEFAULT 0,     -- captured catch-up barrier (raft index), persisted
    catchup_deadline INTEGER   NOT NULL DEFAULT 0,     -- leader-baked UnixNano stall deadline (leader-side only)
    topo_target_gen  INTEGER   NOT NULL DEFAULT 0,     -- topology_generation this op's membership change bumped
    join_nonce       TEXT      NOT NULL DEFAULT '',    -- consumed joiner-minted nonce (replay-dedup); NO PoP sig stored
    params           TEXT      NOT NULL DEFAULT '{}',  -- JSON re-drive inputs; NO secrets, NO duplicated roster fields
    last_error       TEXT      NOT NULL DEFAULT '',
    timeline         TEXT      NOT NULL DEFAULT '[]',  -- capped JSON array of recent transitions (durable, bounded)
    terminal         INTEGER   NOT NULL DEFAULT 0,
    created_at       TIMESTAMP NOT NULL,
    updated_at       TIMESTAMP NOT NULL
);
-- Resume/attach scan by node (NON-unique — single-active is enforced by the baked guard, not a
-- partial-UNIQUE that would constraint-trap the genericExecApplier into an FSM panic).
CREATE INDEX idx_cluster_ops_active   ON cluster_operations(target_node, terminal);
CREATE INDEX idx_cluster_ops_terminal ON cluster_operations(terminal);
