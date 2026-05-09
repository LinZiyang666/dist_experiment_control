-- 0004_token_hash_index.sql — index port_allocations.token_hash so
-- the frps plugin-hook lookup (LookupByTokenHash) is not a
-- full-table scan.
--
-- Quality audit shard 03 (Storage/Protocol) F1: REVOKED/FREED rows
-- are intentionally retained for audit (see 0003 partial unique
-- index doc), so the table grows monotonically over the broker's
-- lifetime. Without this index, every frpc REGISTER walks the full
-- table; on a long-lived broker that's an unbounded scan per
-- connection.
--
-- Plain index, not partial: LookupByTokenHash filters
-- state='ALLOCATED' itself, but a partial-on-state index would
-- block correctly-rejecting lookups for a token that was just
-- REVOKED (we want to log "token_unknown_or_revoked", not
-- "token_unknown" — small UX difference, easier to diagnose).
CREATE INDEX IF NOT EXISTS idx_port_alloc_token_hash
    ON port_allocations(token_hash);
