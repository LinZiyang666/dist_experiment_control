package agentprov

import (
	"database/sql"
	"fmt"
	"time"

	"github.com/LinZiyang666/tether/internal/cluster"
)

// PlanProvisionWithPIN renders OpAgentProvision (architecture §3.3 + d2-plan §3).
// The leader does the whole decision on its DB — session pin_hash/state read +
// verifyPIN + the idempotency check — returning ErrSessionMissing /
// ErrSessionDeleting / ErrInvalidPIN / ErrAlreadyProvisioned BEFORE proposing.
// The replicated op is a bare INSERT OR IGNORE (idempotent for the same fp).
// agentprov binds now.UTC().
func PlanProvisionWithPIN(
	db *sql.DB, sid, nid, agentFP, pin string,
	verifyPIN func(pin, hash string) error,
	now time.Time,
) (*cluster.Command, error) {
	var pinHash, state string
	switch err := db.QueryRow(`SELECT pin_hash, state FROM sessions WHERE sid = ?`, sid).Scan(&pinHash, &state); err {
	case nil:
	case sql.ErrNoRows:
		return nil, ErrSessionMissing
	default:
		return nil, fmt.Errorf("agentprov: plan session lookup: %w", err)
	}
	if state != "ACTIVE" {
		return nil, ErrSessionDeleting
	}
	if err := verifyPIN(pin, pinHash); err != nil {
		return nil, ErrInvalidPIN
	}
	// idempotency / conflicting-fp decision on the leader (matches the live
	// INSERT-OR-IGNORE + re-read): a row with a DIFFERENT fp is a hard error.
	var existing string
	switch err := db.QueryRow(
		`SELECT agent_fp FROM agent_provisioning WHERE sid = ? AND nid = ?`, sid, nid,
	).Scan(&existing); err {
	case nil:
		if existing != agentFP {
			return nil, ErrAlreadyProvisioned
		}
	case sql.ErrNoRows:
	default:
		return nil, fmt.Errorf("agentprov: plan existing lookup: %w", err)
	}
	lits, err := cluster.LitTextAll(sid, nid, agentFP)
	if err != nil {
		return nil, fmt.Errorf("agentprov: plan literal: %w", err)
	}
	sql := `INSERT OR IGNORE INTO agent_provisioning(sid, nid, agent_fp, joined_at) VALUES (` +
		lits[0] + `, ` + lits[1] + `, ` + lits[2] + `, ` + cluster.LitTime(now.UTC()) + `)`
	return cluster.NewCommand(cluster.OpAgentProvision, cluster.Stmt(sql)), nil
}
