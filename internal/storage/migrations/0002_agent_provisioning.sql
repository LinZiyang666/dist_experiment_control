-- 0002_agent_provisioning.sql — bind one agent nkey to one (sid, nid).
--
-- P4 review F1: re-enable the `tether-agent:<sid>:<nid>` role in
-- auth_callout. The handler needs a DB-backed answer to "is this nkey
-- the one provisioned to this (sid, nid)?". The `nodes` table can't
-- carry that — `nodes` rows are created at register-time (post-CONNECT),
-- but auth_callout fires before register. So agent identity is bound
-- here, at the very first PIN-bootstrap CONNECT, and re-validated on
-- every subsequent CONNECT.
--
-- Lifecycle:
--   - first connect with --pin: row inserted, fp recorded.
--   - subsequent connects     : fp must match the recorded one.
--   - session rm              : ON DELETE CASCADE removes the row.
--
-- Uniqueness is on (sid, nid): a single (sid, nid) maps to exactly
-- one agent identity. Re-provisioning to a different agent requires
-- the operator to first DELETE the existing row (e.g. via a future
-- `tether agent revoke` admin tool — out of scope for this round).

CREATE TABLE agent_provisioning (
    sid       TEXT      NOT NULL REFERENCES sessions(sid) ON DELETE CASCADE,
    nid       TEXT      NOT NULL,
    agent_fp  TEXT      NOT NULL,
    joined_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (sid, nid)
);

CREATE INDEX idx_agent_prov_fp ON agent_provisioning(agent_fp);
