-- 0019_session_creators.sql — admission control for `session create`.
--
-- origin: prerelease audit round 2. handleSessionCreate had NO admission control at
-- all: any connection presenting a syntactically valid user nkey could publish
-- `ctrl.by.<actor>.session.create.req`, become the owner of a session it named, and
-- from there mint both the activated-member and the agent permission template. The
-- control plane is on the public internet by design, so "activated member" and
-- "provisioned agent" meant nothing — which invalidated that phrase everywhere it was
-- used as a trust boundary (reply inboxes, the sys.events feed, unbounded JetStream
-- stream creation, and an argon2 hash per request that nothing charged).
--
-- THIS MIGRATION CREATES THE TABLE EMPTY AND SEEDS NOTHING. The upgrade backfill —
-- admitting every fingerprint that already owns a session, so no deployment breaks — is
-- real and still happens, but it happens ONCE, THROUGH RAFT, from
-- session.SeedCreatorsFromOwners at broker boot. A fresh broker starts empty and the
-- operator admits the first fingerprint with `tether admin session-allow`.
--
-- WHY NOT HERE, where it started life as an INSERT…SELECT — origin: prerelease audit
-- increment 2 internal review, reported by eight lanes (admission-product/L8-F4,
-- n1-quadrants/L3-F5, raft-op/F3, ops-upgrade/IMPACT-F1, adminsock-cli/REFUTE-F1,
-- admission-enforcement/REFUTE-F1, repo-invariants/EXPLOIT-F1, empirical
-- migration-0019/F2, and the resurrection half again by n1-quadrants/EXPLOIT-F1 and
-- raft-op/EXPLOIT-F2+F3):
--
--   1. session_creators is REPLICATED state — the raft FSM is its only writer and
--      snapshots ship it. A migration deriving it from ANOTHER replicated table, locally,
--      at whatever moment each node happens to restart, produces a different table on
--      each node in any rolling upgrade. Nothing detects it, and it never converges.
--   2. It RESURRECTS REVOCATIONS. `session-allow --remove` deliberately does not delete
--      the fingerprint's sessions, so the next time this SELECT ran — a later upgrade, or
--      installing a pre-0019 snapshot, both of which re-run migrations — every revoked
--      operator came back. A security control that a routine upgrade silently undoes is
--      not a control.
--   3. CURRENT_TIMESTAMP is evaluated per node, so even the rows that DID agree carried a
--      different added_at on every replica.
CREATE TABLE session_creators (
    fp         TEXT      PRIMARY KEY,             -- SHA256:… of the ctl's user nkey
    added_at   TIMESTAMP NOT NULL,
    added_by   TEXT      NOT NULL,                -- 'upgrade' | 'admin' | an admitting fp
    note       TEXT
);
