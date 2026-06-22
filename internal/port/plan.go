package port

import (
	"database/sql"
	"fmt"
	"time"

	"github.com/LinZiyang666/tether/internal/cluster"
)

// Plan* are the D2 leader-side renderers for the port mutators (architecture
// §3.3 + docs/reviews/d2-plan.md §3). They read the leader DB, make every
// business decision (so a doomed op is never proposed), and bake all fence/
// timestamp values into an all-literal cluster.Command via the audited sqlbake
// helpers. The matching replicated write is the shared genericExecApplier in
// internal/cluster — there is no port-specific Applier (Apply is pure SQL exec).
//
// The live broker keeps calling Free/Revoke/Allocate directly in D2 (ops-only,
// no cutover — §5/§19-D2); these Plan* funcs are exercised by the equivalence /
// differential harnesses and become the live write path at D9.

// PlanFree renders OpPortFree (ALLOCATED -> FREED). Matching Free's semantics it
// returns ErrNotFound when no row carries this public port (a leader-side
// decision made before proposing); an existing-but-already-non-ALLOCATED row
// yields an idempotent op (the WHERE state='ALLOCATED' guard makes Apply a
// no-op), exactly like today's RowsAffected==0 path.
func PlanFree(db *sql.DB, publicPort int, now time.Time) (*cluster.Command, error) {
	return planPortStateChange(db, cluster.OpPortFree, "FREED", publicPort, now)
}

// PlanRevoke renders OpPortRevoke (ALLOCATED -> REVOKED). Same shape as PlanFree.
func PlanRevoke(db *sql.DB, publicPort int, now time.Time) (*cluster.Command, error) {
	return planPortStateChange(db, cluster.OpPortRevoke, "REVOKED", publicPort, now)
}

// PlanAllocate renders OpPortAllocate. It performs Allocate's compound
// read-modify-write entirely on the leader DB (name-uniqueness check +
// findFreePort scan or desired-port band gate), mints the token (crypto/rand) and
// its hash on the leader, and bakes an all-literal INSERT that omits row_id
// (AUTOINCREMENT stays — SQLite assigns it deterministically under the single
// serial-Apply leader). Returns the *Allocation carrying the RAW token in memory
// (the caller forwards it to the agent once) and the *Command carrying ONLY the
// token_hash — the raw token is never replicated.
//
// Business errors (ErrNameTaken / ErrPortOutOfBand / ErrPortExhausted) are
// returned BEFORE any command is built, so a doomed op is never proposed —
// matching today's Allocate semantics. MUST run under Node.Propose (applyMu) so
// two concurrent allocations cannot bake the same port between the leader read
// and the committed Apply.
func PlanAllocate(db *sql.DB, sid, nid, name string, localPort, desiredPort int, createdByFP string, cfg *Config) (*Allocation, *cluster.Command, error) {
	low, high, now := cfgWithDefaults(cfg)

	var existing int
	if err := db.QueryRow(
		`SELECT COUNT(*) FROM port_allocations WHERE sid=? AND name=? AND state='ALLOCATED'`,
		sid, name,
	).Scan(&existing); err != nil {
		return nil, nil, fmt.Errorf("port: plan allocate name check: %w", err)
	}
	if existing > 0 {
		return nil, nil, ErrNameTaken
	}

	var publicPort int
	if desiredPort == 0 {
		p, err := findFreePort(db, low, high)
		if err != nil {
			return nil, nil, err
		}
		publicPort = p
	} else {
		if desiredPort < low || desiredPort > high {
			return nil, nil, ErrPortOutOfBand
		}
		// P12 desired-port collision. Live Allocate relies on the partial-unique
		// index idx_port_alloc_unique_active tripping at INSERT (-> translateInsertErr
		// -> ErrPortTaken). The op path bakes a bare INSERT, so a doomed entry would
		// commit then FAIL the UNIQUE constraint at Apply -> applier error -> D1
		// fail-stop PANIC. The leader must detect the collision in Plan (read under
		// Node.applyMu, so atomic vs a concurrent Propose) and return the SAME typed
		// error BEFORE proposing. (Stage C blocker, 2 reviewers.)
		var taken int
		if err := db.QueryRow(
			`SELECT COUNT(*) FROM port_allocations WHERE port=? AND state='ALLOCATED'`, desiredPort,
		).Scan(&taken); err != nil {
			return nil, nil, fmt.Errorf("port: plan allocate desired-port check: %w", err)
		}
		if taken > 0 {
			return nil, nil, ErrPortTaken
		}
		publicPort = desiredPort
	}

	token, err := genToken()
	if err != nil {
		return nil, nil, fmt.Errorf("port: plan allocate token: %w", err)
	}
	tokenHash := hashToken(token)
	nowUTC := now.UTC()

	lits, err := cluster.LitTextAll(sid, nid, name, createdByFP, tokenHash)
	if err != nil {
		return nil, nil, fmt.Errorf("port: plan allocate literal: %w", err)
	}
	sidLit, nidLit, nameLit, fpLit, thLit := lits[0], lits[1], lits[2], lits[3], lits[4]

	sql := `INSERT INTO port_allocations` +
		` (port, sid, nid, name, local_port, token_hash, state, created_by_fp, created_at)` +
		` VALUES (` + cluster.LitInt(int64(publicPort)) + `, ` + sidLit + `, ` + nidLit + `, ` + nameLit + `, ` +
		cluster.LitInt(int64(localPort)) + `, ` + thLit + `, 'ALLOCATED', ` + fpLit + `, ` + cluster.LitTime(nowUTC) + `)`

	alloc := &Allocation{
		Port: publicPort, SID: sid, NID: nid, Name: name, LocalPort: localPort,
		TokenHash: tokenHash, State: StateAllocated, CreatedByFP: createdByFP,
		CreatedAt: nowUTC, Token: token,
	}
	return alloc, cluster.NewCommand(cluster.OpPortAllocate, cluster.Stmt(sql)), nil
}

func planPortStateChange(db *sql.DB, op cluster.OpType, newState string, publicPort int, now time.Time) (*cluster.Command, error) {
	var exists int
	if err := db.QueryRow(
		`SELECT COUNT(*) FROM port_allocations WHERE port=?`, publicPort,
	).Scan(&exists); err != nil {
		return nil, fmt.Errorf("port: plan %s: %w", op, err)
	}
	if exists == 0 {
		return nil, ErrNotFound
	}
	// newState is a fixed internal enum, never external text -> MustLitText.
	// port.Free/Revoke bind now.UTC(); LitTime must receive the same value.
	sql := `UPDATE port_allocations SET state=` + cluster.MustLitText(newState) +
		`, revoked_at=` + cluster.LitTime(now.UTC()) +
		` WHERE port=` + cluster.LitInt(int64(publicPort)) + ` AND state='ALLOCATED'`
	return cluster.NewCommand(op, cluster.Stmt(sql)), nil
}
