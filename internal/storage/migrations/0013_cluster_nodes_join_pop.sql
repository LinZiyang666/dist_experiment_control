-- Migration 0013 — D7 §8.1 join-PoP + half-success bookkeeping on cluster_nodes.
--
-- ALTER ADD COLUMN only, NOT a table rebuild: preserves the node_id PRIMARY KEY,
-- the UNIQUE(name) constraint, and idx_cluster_nodes_phase from 0008. The phase
-- CHECK already carries all 6 values (JOIN_VERIFIED_PENDING_VOTER / CATCHING_UP /
-- VOTER / VOTER_ADD_FAILED / DRAINING / RETIRING) — NO enum change here.
--
-- Columns are NULLABLE (NOT `NOT NULL DEFAULT ''`): an empty-string signature must
-- be impossible-to-pass, not conventionally skipped (nkeys.Verify('','') fails
-- closed), and the D9 `cluster init --from-existing` self-row + any pre-D7
-- grandfathered row are admitted by bootstrap, not a join challenge, so they
-- legitimately carry NULL join_nonce/join_sig. All values are leader-baked SQL
-- literals (§3.4) — NO CURRENT_TIMESTAMP default, like every other Apply-owned
-- timestamp.
ALTER TABLE cluster_nodes ADD COLUMN join_nonce       TEXT;      -- leader-issued single-use nonce, baked; followers re-verify the PoP from row+Aux
ALTER TABLE cluster_nodes ADD COLUMN join_sig         TEXT;      -- hex(ed25519 sig) over canonicalJoinMsg(node_id, node_ident_pub, join_nonce)
ALTER TABLE cluster_nodes ADD COLUMN voter_add_error  TEXT;      -- VOTER_ADD_FAILED detail / 'catch_up_stalled' derivation hint; NULL = none
ALTER TABLE cluster_nodes ADD COLUMN phase_changed_at TIMESTAMP; -- leader-baked literal; "stuck in phase X since T" + the catch_up_stalled max-wait derivation
