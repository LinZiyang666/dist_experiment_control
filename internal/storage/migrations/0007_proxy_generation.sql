-- Migration 0007 — P13 broker incarnation generation (round-5 F2).
--
-- Delivered as a FORWARD migration (not an amendment to 0006) because the
-- migration engine records applied files by name and skips them on re-run, so a
-- DB that already applied an earlier 0006 would never receive a table appended
-- to it. This file is a new name, so every DB — fresh or already-on-0006 —
-- applies it exactly once.
--
-- proxy_meta.generation is a PERSISTED monotonic fencing counter. Each broker
-- boot computes max(stored+1, now_unixnano) and writes it back, so a later
-- process always observes a strictly higher value even if the wall clock rolled
-- backward (NTP/manual/VM restore). A broker that cannot read/advance this row
-- fails to start (it must not fall open to a bare wall clock).
--
-- DB-restore convergence is automatic: a broker restored from an older snapshot
-- whose proxy_meta rewound observes a connected agent's higher generation in the
-- heartbeat and atomically escalates this row above it (see repairProxy), so no
-- manual runbook value is required.
-- IF NOT EXISTS / INSERT OR IGNORE keep this idempotent even on a DB that
-- already materialized proxy_meta out-of-band (defensive; the engine still runs
-- each migration file at most once).
CREATE TABLE IF NOT EXISTS proxy_meta (
    id         INTEGER PRIMARY KEY CHECK (id = 1),
    generation INTEGER NOT NULL DEFAULT 0
);
INSERT OR IGNORE INTO proxy_meta (id, generation) VALUES (1, 0);
