package proxysub

import (
	"fmt"
	"time"

	"github.com/LinZiyang666/tether/internal/agent/ssproxy"
	"github.com/LinZiyang666/tether/internal/cluster"
)

// plan.go (C5) — leader-baked Plan helpers for the replicated proxy-subscriber control plane. The
// subscriber PSK travels the Raft log as a LitText literal (C5 §D-SECRET: recoverable-by-contract
// within the broker-only mTLS trust boundary); the RAW bearer token NEVER does — only its hash. The
// leader mints the secrets via MintSubscriber, bakes the INSERT (hash + psk), and returns the raw
// token leak-once to ctl (captured from the Propose closure, like allocatePort). Each create/revoke
// change-gates a keyset-epoch bump on the session so agents re-fetch the keyset.

// MintSubscriber generates the per-subscriber secrets on the leader. The raw token is returned ONCE
// (the caller vends it leak-once); only its hash + the PSK are persisted/replicated.
func MintSubscriber() (rawToken, tokenHash, psk, subID string, err error) {
	rawToken, err = genSecret(32)
	if err != nil {
		return "", "", "", "", fmt.Errorf("proxysub: token gen: %w", err)
	}
	psk, err = genSecret(16)
	if err != nil {
		return "", "", "", "", fmt.Errorf("proxysub: psk gen: %w", err)
	}
	return rawToken, HashToken(rawToken), psk, newSubID(), nil
}

// proxyEpochBumpStmt bumps the session keyset epoch, change-gated on the PRECEDING statement (the
// create INSERT / revoke UPDATE) so a no-op (duplicate create, already-revoked) does not bump.
func proxyEpochBumpStmt(sidLit string) cluster.Statement {
	return cluster.Stmt(`UPDATE sessions SET proxy_epoch=proxy_epoch+1 WHERE sid=` + sidLit +
		` AND state='ACTIVE' AND changes() > 0`)
}

// PlanCreate bakes the subscriber INSERT (guarded against an ACTIVE (sid,name) duplicate so it is a
// RowsAffected==0 no-op rather than a partial-unique violation that would fsm-panic genericExecApplier)
// + the change-gated epoch bump.
func PlanCreate(sid, subID, name, tokenHash, psk, createdByFP string, now time.Time) (*cluster.Command, error) {
	lits, err := cluster.LitTextAll(sid, subID, name, tokenHash, psk, ssproxy.Cipher, createdByFP)
	if err != nil {
		return nil, fmt.Errorf("proxysub: plan create: literal: %w", err)
	}
	sidL, subL, nameL, hashL, pskL, cipherL, fpL := lits[0], lits[1], lits[2], lits[3], lits[4], lits[5], lits[6]
	ins := `INSERT INTO proxy_subscribers (sid, sub_id, name, token_hash, psk, cipher, state, created_by_fp, created_at) ` +
		`SELECT ` + sidL + `, ` + subL + `, ` + nameL + `, ` + hashL + `, ` + pskL + `, ` + cipherL + `, 'ACTIVE', ` + fpL + `, ` + cluster.LitTime(now.UTC()) + ` ` +
		`WHERE NOT EXISTS (SELECT 1 FROM proxy_subscribers WHERE sid=` + sidL + ` AND name=` + nameL + ` AND state='ACTIVE')`
	return cluster.NewCommand(cluster.OpProxySubCreate, cluster.Stmt(ins), proxyEpochBumpStmt(sidL)), nil
}

// PlanRevoke bakes the ACTIVE→REVOKED UPDATE (idempotent: an already-revoked name is a no-op) + the
// change-gated epoch bump (so the agent cuts the revoked key on its next keyset fetch).
func PlanRevoke(sid, name string, now time.Time) (*cluster.Command, error) {
	sidL, err := cluster.LitText(sid)
	if err != nil {
		return nil, fmt.Errorf("proxysub: plan revoke: sid literal: %w", err)
	}
	nameL, err := cluster.LitText(name)
	if err != nil {
		return nil, fmt.Errorf("proxysub: plan revoke: name literal: %w", err)
	}
	upd := `UPDATE proxy_subscribers SET state='REVOKED', revoked_at=` + cluster.LitTime(now.UTC()) +
		` WHERE sid=` + sidL + ` AND name=` + nameL + ` AND state='ACTIVE'`
	return cluster.NewCommand(cluster.OpProxySubRevoke, cluster.Stmt(upd), proxyEpochBumpStmt(sidL)), nil
}
