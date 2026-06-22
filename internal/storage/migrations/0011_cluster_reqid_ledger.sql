-- Migration 0011 — cluster_reqid_ledger (distributed-broker D4 §4.1 cross-retry idempotency).
--
-- The dedup ledger for forwarded session-control writes (follower->leader). The
-- originating broker mints a content-addressed, cross-retry-STABLE req_id; the FSM
-- records it here in the SAME txn as the op + applied_index, and dedups a
-- committed-but-ack-lost retry (same req_id at a NEW raft index) by skipping the op
-- while still advancing applied_index.
--
-- FSM-owned: written ONLY by FSM.Apply inside the op txn (never a direct mutator).
-- No CURRENT_TIMESTAMP (§4.2/§3.4 determinism — every value the FSM writes is
-- leader-baked / replica-deterministic). The req_id PRIMARY KEY is the
-- dedup-uniqueness backstop (a duplicate INSERT is a bug → fail-stop). The
-- raft_index index makes the deterministic same-txn GC range-delete bounded.
--
-- Cluster WAL FSM DB only; zero blast radius to the frozen broker DB until the D9
-- single-WAL merge.
CREATE TABLE IF NOT EXISTS cluster_reqid_ledger (
    req_id     TEXT    PRIMARY KEY,
    raft_index INTEGER NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_reqid_ledger_index ON cluster_reqid_ledger(raft_index);
