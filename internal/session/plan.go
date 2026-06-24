package session

import (
	"database/sql"
	"fmt"
	"time"

	"github.com/LinZiyang666/tether/internal/cluster"
	"github.com/LinZiyang666/tether/internal/proto"
)

// Plan* are the D2 leader-side renderers for the session mutators (architecture
// §3.3 + docs/reviews/d2-plan.md §3). session binds its timestamps RAW (Create /
// Tombstone bind `now` without .UTC()), so LitTime receives the raw time.Time.
// The PIN is pre-hashed at the broker boundary (auth.HashPIN, crypto/rand salt),
// so internal/session never imports crypto/rand and the hash is a fixed input.

// PlanCreate renders OpSessionCreate. It validates the SID and checks existence
// on the leader DB (returning ErrAlreadyExists before proposing, matching Create's
// unique-violation path), then bakes the two INSERTs (sessions + owner member) as
// ONE Command so Apply runs both in the same FSM txn — preserving today's
// two-INSERT atomicity.
func PlanCreate(db *sql.DB, sid, name, ownerFP, pinHash string, now time.Time) (*cluster.Command, error) {
	if err := proto.ValidateSID(sid); err != nil {
		return nil, fmt.Errorf("session: %w", err)
	}
	var n int
	if err := db.QueryRow(`SELECT COUNT(*) FROM sessions WHERE sid=?`, sid).Scan(&n); err != nil {
		return nil, fmt.Errorf("session: plan create existence: %w", err)
	}
	if n > 0 {
		return nil, ErrAlreadyExists
	}
	lits, err := cluster.LitTextAll(sid, name, ownerFP, pinHash)
	if err != nil {
		return nil, fmt.Errorf("session: plan create literal: %w", err)
	}
	sidL, nameL, ownerL, pinL := lits[0], lits[1], lits[2], lits[3]
	t := cluster.LitTime(now)

	sessRow := `INSERT INTO sessions(sid, name, owner_pubkey_fp, pin_hash, state, created_at) VALUES (` +
		sidL + `, ` + nameL + `, ` + ownerL + `, ` + pinL + `, ` + cluster.MustLitText(string(StateActive)) + `, ` + t + `)`
	memRow := `INSERT INTO members(sid, pubkey_fp, role, via, joined_at) VALUES (` +
		sidL + `, ` + ownerL + `, ` + cluster.MustLitText(string(RoleOwner)) + `, ` + cluster.MustLitText(string(ViaCreate)) + `, ` + t + `)`
	return cluster.NewCommand(cluster.OpSessionCreate, cluster.Stmt(sessRow), cluster.Stmt(memRow)), nil
}

// PlanTombstone renders OpSessionTombstone (ACTIVE -> DELETING). It returns
// ErrNotFound (no such sid) or ErrDeleting (already non-ACTIVE) before proposing,
// matching Tombstone's RowsAffected==0 diagnosis.
func PlanTombstone(db *sql.DB, sid string, now time.Time) (*cluster.Command, error) {
	var state string
	switch err := db.QueryRow(`SELECT state FROM sessions WHERE sid=?`, sid).Scan(&state); err {
	case nil:
		if state != string(StateActive) {
			return nil, ErrDeleting
		}
	case sql.ErrNoRows:
		return nil, ErrNotFound
	default:
		return nil, fmt.Errorf("session: plan tombstone lookup: %w", err)
	}
	sidL, err := cluster.LitText(sid)
	if err != nil {
		return nil, fmt.Errorf("session: plan tombstone literal: %w", err)
	}
	sql := `UPDATE sessions SET state='DELETING', deleting_at=` + cluster.LitTime(now) +
		` WHERE sid=` + sidL + ` AND state='ACTIVE'`
	return cluster.NewCommand(cluster.OpSessionTombstone, cluster.Stmt(sql)), nil
}

// PlanHardDelete renders OpSessionHardDelete — the ordered DELETEs of
// dropSessionRows (internal/broker/audit.go) as ONE Command, so Apply removes the
// whole session subtree in a single FSM txn (matching today's one-tx semantics).
// It is fenced to the DELETING state: a stale finalizer for a previously deleted
// same-sid session must not delete a newly-created ACTIVE replacement.
func PlanHardDelete(db *sql.DB, sid string) (*cluster.Command, error) {
	var state string
	switch err := db.QueryRow(`SELECT state FROM sessions WHERE sid=?`, sid).Scan(&state); err {
	case nil:
		if state != string(StateDeleting) {
			return nil, ErrDeleting
		}
	case sql.ErrNoRows:
		// Idempotent retry after another finalizer already removed the row. Still
		// return a no-op command so the forward/propose path has a concrete op.
	default:
		return nil, fmt.Errorf("session: plan hard delete lookup: %w", err)
	}
	sidL, err := cluster.LitText(sid)
	if err != nil {
		return nil, fmt.Errorf("session: plan hard delete literal: %w", err)
	}
	childTables := []string{
		"port_allocations", "processes", "proxy_subscribers",
		"agent_provisioning", "nodes", "members",
	}
	stmts := make([]cluster.Statement, 0, len(childTables)+1)
	for _, tbl := range childTables {
		stmts = append(stmts, cluster.Stmt(
			`DELETE FROM `+tbl+` WHERE sid IN (`+
				`SELECT sid FROM sessions WHERE sid=`+sidL+` AND state=`+cluster.MustLitText(string(StateDeleting))+`)`))
	}
	stmts = append(stmts, cluster.Stmt(
		`DELETE FROM sessions WHERE sid=`+sidL+` AND state=`+cluster.MustLitText(string(StateDeleting))))
	return cluster.NewCommand(cluster.OpSessionHardDelete, stmts...), nil
}

// PlanJoinWithPIN renders OpMemberJoin — the JoinWithPIN/AddMember INSERT path
// (architecture §5; a live authoritative write the survey missed). The leader
// reads pin_hash/state + runs verifyPIN (ErrNotFound / ErrDeleting / ErrInvalidPIN
// before proposing); the op is INSERT OR IGNORE members (role=member, via=pin).
// AddMember binds the raw now.
func PlanJoinWithPIN(
	db *sql.DB, sid, pubkeyFP, pin string,
	verifyPIN func(pin, hash string) error,
	now time.Time,
) (*cluster.Command, error) {
	var pinHash, state string
	switch err := db.QueryRow(`SELECT pin_hash, state FROM sessions WHERE sid=?`, sid).Scan(&pinHash, &state); err {
	case nil:
		if state != string(StateActive) {
			return nil, ErrDeleting
		}
	case sql.ErrNoRows:
		return nil, ErrNotFound
	default:
		return nil, fmt.Errorf("session: plan join lookup: %w", err)
	}
	if err := verifyPIN(pin, pinHash); err != nil {
		return nil, ErrInvalidPIN
	}
	lits, err := cluster.LitTextAll(sid, pubkeyFP)
	if err != nil {
		return nil, fmt.Errorf("session: plan join literal: %w", err)
	}
	sql := `INSERT OR IGNORE INTO members(sid, pubkey_fp, role, via, joined_at) VALUES (` +
		lits[0] + `, ` + lits[1] + `, ` + cluster.MustLitText(string(RoleMember)) + `, ` +
		cluster.MustLitText(string(ViaPin)) + `, ` + cluster.LitTime(now) + `)`
	return cluster.NewCommand(cluster.OpMemberJoin, cluster.Stmt(sql)), nil
}
