package session

import (
	"database/sql"
	"errors"
	"fmt"
	"github.com/LinZiyang666/tether/internal/cluster"
	"time"
)

// creators.go — who may create a session.
//
// origin: prerelease audit round 2. handleSessionCreate had NO admission control: any
// connection presenting a syntactically valid user nkey could name a session, become its
// owner, and from there mint both the activated-member and the agent permission
// template. The control plane is on the public internet by design, so every place the
// audit reasoned "an activated member is an authorized principal" was reasoning about a
// set that included the entire internet.
//
// This is a POLICY table, not a credential store: it holds fingerprints, and holding one
// only lets you CREATE a session. Joining an existing session is still the PIN's job.

// ErrNotAllowedToCreate is the refusal a fingerprint outside the table gets.
var ErrNotAllowedToCreate = errors.New("session: this identity is not allowed to create sessions")

// MayCreateSession reports whether fp is admitted, and surfaces a read error rather than
// folding it into "no" — a broker that cannot read its own policy table must refuse
// LOUDLY rather than look like a broker with an empty one.
func MayCreateSession(db *sql.DB, fp string) (bool, error) {
	if fp == "" {
		return false, nil
	}
	var one int
	switch err := db.QueryRow(`SELECT 1 FROM session_creators WHERE fp=? LIMIT 1`, fp).Scan(&one); {
	case err == nil:
		return true, nil
	case errors.Is(err, sql.ErrNoRows):
		// NOT ON THE LIST — but the list may not exist yet.
		//
		// origin: prerelease audit external review M-2. The one-shot backfill that
		// grandfathers existing owners is proposed by the LEADER. A standard rolling
		// upgrade is followers-first/leader-last, so every new broker comes up while the
		// old leader is still serving, and the last step hands leadership to an
		// already-running new follower before restarting the old leader — which returns as
		// a follower. Every node therefore skipped the backfill, and each new follower
		// began enforcing an EMPTY table against owners who had been creating sessions for
		// months. "Upgrade needs no operator action" was true only if some node happened to
		// be leader at its own boot.
		//
		// So until the marker is committed, fall back to the question the backfill is
		// about to answer anyway: does this fingerprint already OWN a session here? That
		// admits exactly the set the backfill would admit and nothing else — a stranger
		// gets the same refusal as before, because owning a session is not something an
		// unadmitted identity can arrange. Once the marker lands, the table is the only
		// authority and a revoked owner stays revoked.
		seeded, serr := CreatorsSeeded(db)
		if serr != nil {
			return false, serr
		}
		if seeded {
			return false, nil
		}
		return ownsAnySession(db, fp)
	default:
		return false, fmt.Errorf("session: read the creator allow-list: %w", err)
	}
}

// ownsAnySession reports whether fp is the owner of at least one session — the
// pre-marker grandfather predicate. It asks the same question OwnersNeedingAdmission
// asks in bulk, so the two cannot drift into admitting different sets.
func ownsAnySession(db *sql.DB, fp string) (bool, error) {
	var one int
	switch err := db.QueryRow(
		`SELECT 1 FROM sessions WHERE owner_pubkey_fp=? LIMIT 1`, fp).Scan(&one); {
	case err == nil:
		return true, nil
	case errors.Is(err, sql.ErrNoRows):
		return false, nil
	default:
		return false, fmt.Errorf("session: read session ownership: %w", err)
	}
}

// AllowCreator admits fp and reports whether this call was the one that added it.
// Idempotent, so an operator re-running the command is not an error and re-admitting does
// not rewrite who admitted whom.
//
// THE BOOL IS NOT DECORATION. `INSERT OR IGNORE` also ignores a NEW --note on a
// fingerprint that is already admitted, and the CLI used to print "admitted" either way —
// so the one human-written field on the row silently did not change while the operator
// was told it had. Returning "was it already there" lets the caller say so.
// origin: prerelease audit increment 2 internal review, admission-enforcement/L9-F9 ≡
// admission-product/EXPLOIT-F2 ≡ test-blast-radius/EXPLOIT-F2.
func AllowCreator(db *sql.DB, fp, addedBy, note string, now time.Time) (added bool, err error) {
	if fp == "" {
		return false, fmt.Errorf("session: a fingerprint is required")
	}
	res, err := db.Exec(
		`INSERT OR IGNORE INTO session_creators(fp, added_at, added_by, note) VALUES (?,?,?,?)`,
		fp, now.UTC(), addedBy, note)
	if err != nil {
		return false, err
	}
	n, err := res.RowsAffected()
	return n > 0, err
}

// DenyCreator removes fp. Removing an fp does NOT touch the sessions it already owns —
// revoking the ability to create is not the same as deleting somebody's work, and
// conflating them would make an operator hesitate to use this at all.
func DenyCreator(db *sql.DB, fp string) (bool, error) {
	res, err := db.Exec(`DELETE FROM session_creators WHERE fp=?`, fp)
	if err != nil {
		return false, err
	}
	n, err := res.RowsAffected()
	return n > 0, err
}

// Creator is one admitted fingerprint, for `tether admin session-allow --list`.
type Creator struct {
	FP      string
	AddedAt time.Time
	AddedBy string
	Note    string
}

// ListCreators returns the allow-list, oldest first.
func ListCreators(db *sql.DB) ([]Creator, error) {
	rows, err := db.Query(
		`SELECT fp, added_at, added_by, COALESCE(note,'') FROM session_creators ORDER BY added_at, fp`)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()
	var out []Creator
	for rows.Next() {
		var c Creator
		if err := rows.Scan(&c.FP, &c.AddedAt, &c.AddedBy, &c.Note); err != nil {
			return nil, err
		}
		out = append(out, c)
	}
	return out, rows.Err()
}

// PlanSetCreator renders the replicated form of AllowCreator / DenyCreator.
//
// origin: prerelease audit round 2. In cluster mode the raft FSM is the sole SQLite
// writer, so the admin socket cannot db.Exec this table directly — and beyond that
// invariant there is a reason specific to this table: an fp admitted on one broker must
// be admitted on ALL of them, or which broker a ctl happens to reach decides whether it
// may create a session.
func PlanSetCreator(fp, addedBy, note string, allow bool, now time.Time) (*cluster.Command, error) {
	fpL, err := cluster.LitText(fp)
	if err != nil {
		return nil, fmt.Errorf("session: creator fp literal: %w", err)
	}
	if !allow {
		return cluster.NewCommand(cluster.OpSessionCreatorSet,
			cluster.Stmt(`DELETE FROM session_creators WHERE fp=`+fpL)), nil
	}
	lits, err := cluster.LitTextAll(addedBy, note)
	if err != nil {
		return nil, fmt.Errorf("session: creator literals: %w", err)
	}
	// cluster.LitTime, NOT an RFC3339Nano string — origin: prerelease audit increment 2
	// internal review, raft-op/F4 ≡ repo-invariants/F4 ≡ admission-product/L8-F9.
	//
	// LitTime renders exactly what the modernc driver writes when AllowCreator passes a
	// time.Time to db.Exec. Baking a different encoding here put TWO text formats in one
	// column depending on whether the row arrived through the single-mode direct write or
	// the replicated one — which breaks the §13.2/DIFF-1 single↔cluster equivalence the
	// whole Plan* convention exists for, and makes ListCreators' `ORDER BY added_at`
	// (documented as "oldest first") compare two alphabets.
	return cluster.NewCommand(cluster.OpSessionCreatorSet,
		cluster.Stmt(`INSERT OR IGNORE INTO session_creators(fp, added_at, added_by, note) VALUES (`+
			fpL+`,`+cluster.LitTime(now.UTC())+`,`+lits[0]+`,`+lits[1]+`)`)), nil
}

// CreatorsSeededKey marks that the one-shot upgrade backfill has run. It lives in
// cluster_meta, which is replicated, so in a cluster the decision is made once by the
// leader and every replica learns it through the log rather than each deciding for itself.
const CreatorsSeededKey = "session_creators_seeded"

// creatorsSeedNote is the note the backfill leaves, so `--list` distinguishes a
// fingerprint an operator admitted from one the upgrade grandfathered in.
const creatorsSeedNote = "owned a session before admission control existed"

// CreatorsSeeded reports whether the one-shot upgrade backfill has already been recorded.
func CreatorsSeeded(db *sql.DB) (bool, error) {
	var v string
	switch err := db.QueryRow(`SELECT value FROM cluster_meta WHERE key=?`, CreatorsSeededKey).Scan(&v); {
	case err == nil:
		return true, nil
	case errors.Is(err, sql.ErrNoRows):
		return false, nil
	default:
		return false, fmt.Errorf("session: read the creator-seed marker: %w", err)
	}
}

// OwnersNeedingAdmission lists the distinct owner fingerprints of sessions that exist
// today — the set the upgrade backfill admits. Read-only, and deliberately separate from
// the write so both modes seed from exactly the same query.
func OwnersNeedingAdmission(db *sql.DB) ([]string, error) {
	rows, err := db.Query(
		`SELECT DISTINCT owner_pubkey_fp FROM sessions
		  WHERE owner_pubkey_fp IS NOT NULL AND owner_pubkey_fp != ''
		  ORDER BY owner_pubkey_fp`)
	if err != nil {
		return nil, fmt.Errorf("session: list session owners: %w", err)
	}
	defer func() { _ = rows.Close() }()
	var out []string
	for rows.Next() {
		var fp string
		if err := rows.Scan(&fp); err != nil {
			return nil, err
		}
		out = append(out, fp)
	}
	return out, rows.Err()
}

// PlanSeedCreators renders the upgrade backfill as ONE replicated command: every owner
// fingerprint plus the marker that stops it ever running again.
//
// ONE COMMAND, NOT N — the marker and the rows must land in the same raft entry, or a
// leadership change between them re-runs the backfill on the next boot and re-admits
// anything revoked in between. That is the same resurrection the migration form had, just
// through a narrower window.
//
// An empty owner list still writes the marker: "there was nothing to grandfather" is a
// decision that must also be made once, or every boot re-derives it against a sessions
// table that has since changed.
func PlanSeedCreators(fps []string, now time.Time) (*cluster.Command, error) {
	noteL, err := cluster.LitText(creatorsSeedNote)
	if err != nil {
		return nil, fmt.Errorf("session: seed note literal: %w", err)
	}
	byL, err := cluster.LitText("upgrade")
	if err != nil {
		return nil, fmt.Errorf("session: seed added_by literal: %w", err)
	}
	stmts := make([]cluster.Statement, 0, len(fps)+1)
	for _, fp := range fps {
		fpL, lerr := cluster.LitText(fp)
		if lerr != nil {
			return nil, fmt.Errorf("session: seed fp literal: %w", lerr)
		}
		stmts = append(stmts, cluster.Stmt(
			`INSERT OR IGNORE INTO session_creators(fp, added_at, added_by, note) VALUES (`+
				fpL+`,`+cluster.LitTime(now.UTC())+`,`+byL+`,`+noteL+`)`))
	}
	markL, err := cluster.LitText(CreatorsSeededKey)
	if err != nil {
		return nil, fmt.Errorf("session: seed marker literal: %w", err)
	}
	stmts = append(stmts, cluster.Stmt(
		`INSERT OR REPLACE INTO cluster_meta(key, value) VALUES (`+markL+`,`+cluster.LitTime(now.UTC())+`)`))
	return cluster.NewCommand(cluster.OpSessionCreatorSet, stmts...), nil
}

// SeedCreatorsLocally is the single-broker form of PlanSeedCreators: no raft, so the rows
// and the marker go in one local transaction for the same atomicity.
func SeedCreatorsLocally(db *sql.DB, fps []string, now time.Time) error {
	tx, err := db.Begin()
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback() }()
	for _, fp := range fps {
		if _, err := tx.Exec(
			`INSERT OR IGNORE INTO session_creators(fp, added_at, added_by, note) VALUES (?,?,?,?)`,
			fp, now.UTC(), "upgrade", creatorsSeedNote); err != nil {
			return fmt.Errorf("session: seed creator %s: %w", fp, err)
		}
	}
	if _, err := tx.Exec(`INSERT OR REPLACE INTO cluster_meta(key, value) VALUES (?,?)`,
		CreatorsSeededKey, now.UTC()); err != nil {
		return fmt.Errorf("session: record the creator-seed marker: %w", err)
	}
	return tx.Commit()
}
