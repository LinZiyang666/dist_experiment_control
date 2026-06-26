-- Migration 0016 — sessions.proxy_ha_policy (C5 proxy cluster-ization).
--
-- The ONLY additive column C5 needs. The proxy HA policy is per-session, replicated control state set
-- by `proxy on --ha-policy`: it governs how `/sub` behaves when the cluster loses quorum —
--   'freeze-on-quorum-loss' (default): proxy CONTROL writes are refused (no add/revoke/toggle/rehome),
--       but `/sub` KEEPS vending the last committed state so existing subscriber sessions survive
--       (the data plane is leader-decoupled). A routine election does NOT 404 /sub.
--   'disable-on-quorum-loss': writes refused AND `/sub` returns 404 (fail-closed for the strict case).
-- The default is freeze (a fail-closed default would kill working subscriptions on any quorum blip).
--
-- A CHECK on this NEW column is safe (it is created WITH the constraint — no ALTER-of-CHECK, which
-- SQLite cannot do; that is exactly why C5's force-home uses cluster_operations, not a new alerts.kind).
--
-- C5 needs NO other schema: the keyset epoch (sessions.proxy_epoch, 0006), the proxy_subscribers
-- table (0006), and the __proxy__ home_broker/epoch (port_allocations, 0006+0010) already exist;
-- keyset_stale is derived live from the broadcast heartbeat (no proxy_applied_epoch column). Additive:
-- a non-cluster broker reads the default 'freeze-on-quorum-loss' and the proxy stays single-mode P13.
ALTER TABLE sessions ADD COLUMN proxy_ha_policy TEXT NOT NULL DEFAULT 'freeze-on-quorum-loss'
    CHECK (proxy_ha_policy IN ('freeze-on-quorum-loss', 'disable-on-quorum-loss'));
