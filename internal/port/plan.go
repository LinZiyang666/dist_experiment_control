package port

import (
	"database/sql"
	"errors"
	"fmt"
	"math"
	"strings"
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

// PlanFreeAllocation renders a fenced expose-rm free. Unlike PlanFree, the leader
// re-checks the allocation identity selected by the caller before freeing so a stale
// name->port read or a concurrent port reuse cannot free someone else's live row.
func PlanFreeAllocation(db *sql.DB, a Allocation, now time.Time) (*cluster.Command, error) {
	return planAllocationStateChange(db, cluster.OpPortFree, "FREED", "free", a, now)
}

// PlanRevokeAllocation renders a fenced offline-node revoke. It carries the
// full allocation identity selected by the leader's offline scan so a stale scan
// cannot revoke a newly-reused public port.
func PlanRevokeAllocation(db *sql.DB, a Allocation, now time.Time) (*cluster.Command, error) {
	return planAllocationStateChange(db, cluster.OpPortRevoke, "REVOKED", "revoke", a, now)
}

func planAllocationStateChange(db *sql.DB, op cluster.OpType, newState, label string, a Allocation, now time.Time) (*cluster.Command, error) {
	where, err := allocationIdentitySQL(a, StateAllocated, false)
	if err != nil {
		return nil, err
	}
	authority, err := allocationAuthoritySQL(db, a.Authority, a.SID, a.NID)
	if err != nil {
		return nil, err
	}
	// Repeat creation identity in the actual replicated write, even after the
	// leader's comparison succeeds: replay may follow session recreation.
	// origin: hot-update S8 plan §6.2 / TestPortMutationRejectsRecreatedSession
	where += ` AND ` + authority
	var exists int
	if err := db.QueryRow(`SELECT COUNT(*) FROM port_allocations WHERE ` + where).Scan(&exists); err != nil {
		return nil, fmt.Errorf("port: plan %s allocation lookup: %w", label, err)
	}
	if exists == 0 {
		return nil, ErrNotFound
	}
	sql := `UPDATE port_allocations SET state=` + cluster.MustLitText(newState) +
		`, revoked_at=` + cluster.LitTime(now.UTC()) +
		` WHERE ` + where
	return cluster.NewCommand(op, cluster.Stmt(sql)), nil
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
//
// D6 §7.1-7.2: homeBroker is the cluster_nodes.node_id of the home this expose
// is assigned to. When NON-EMPTY the baked INSERT additionally sets
// home_broker=<lit>, epoch=0 (the per-port counter baseline, DA-3). When EMPTY
// (every current caller — build-and-prove, cutover=D9) the INSERT is BYTE-
// IDENTICAL to pre-D6 (home_broker/epoch take their migration-0010 defaults
// ” / 0). The live port.Allocate direct mutator is unchanged and always EMPTY.
func PlanAllocate(db *sql.DB, sid, nid, name string, localPort, desiredPort int, createdByFP, homeBroker string, rebuildOff bool, cfg *Config) (*Allocation, *cluster.Command, error) {
	return planAllocate(db, sid, nid, name, localPort, desiredPort, createdByFP, homeBroker, rebuildOff, cfg, nil, false, "")
}

// planAllocate shares token/port selection and literal rendering with the cold
// API. Guarded callers supply their already-validated replacement and predicate;
// the cold API retains its original name scope and byte-identical SQL.
func planAllocate(db *sql.DB, sid, nid, name string, localPort, desiredPort int, createdByFP, homeBroker string, rebuildOff bool, cfg *Config, replacing *Allocation, proxyScope bool, insertWhere string) (*Allocation, *cluster.Command, error) {
	low, high, now := cfgWithDefaults(cfg)

	scope, err := allocationNameSQL(sid, nid, name, proxyScope)
	if err != nil {
		return nil, nil, err
	}
	replacementExclusion := ""
	if replacing != nil {
		identity, err := allocationIdentitySQL(*replacing, StateAllocated)
		if err != nil {
			return nil, nil, err
		}
		replacementExclusion = ` AND NOT (` + identity + `)`
	}
	scope += replacementExclusion
	var existing int
	if err := db.QueryRow(`SELECT COUNT(*) FROM port_allocations WHERE ` + scope).Scan(&existing); err != nil {
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
		portScope := `port=` + cluster.LitInt(int64(desiredPort)) + ` AND state='ALLOCATED'` + replacementExclusion
		if err := db.QueryRow(`SELECT COUNT(*) FROM port_allocations WHERE ` + portScope).Scan(&taken); err != nil {
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

	cols := ` (port, sid, nid, name, local_port, token_hash, state, created_by_fp, created_at)`
	vals := ` VALUES (` + cluster.LitInt(int64(publicPort)) + `, ` + sidLit + `, ` + nidLit + `, ` + nameLit + `, ` +
		cluster.LitInt(int64(localPort)) + `, ` + thLit + `, 'ALLOCATED', ` + fpLit + `, ` + cluster.LitTime(nowUTC) + `)`
	if homeBroker != "" {
		// D6: clustered home assignment. Append home_broker + epoch=0. The "" case
		// above stays byte-identical to pre-D6 (no extra columns).
		homeLit, herr := cluster.LitText(homeBroker)
		if herr != nil {
			return nil, nil, fmt.Errorf("port: plan allocate home literal: %w", herr)
		}
		cols = strings.TrimSuffix(cols, `)`) + `, home_broker, epoch)`
		vals = strings.TrimSuffix(vals, `)`) + `, ` + homeLit + `, ` + cluster.LitInt(0) + `)`
	}
	// B4 conditional column (composes with the home conditional above into a 2×2): only
	// --no-rebuild (rebuildOff) appends rebuild_on_failure=0; rebuild-ON (the default)
	// omits it so the column takes its migration-0010 DEFAULT 1 and the baked SQL stays
	// byte-identical to pre-B4 for that home-state. Slice-then-reappend (instead of a
	// 4th literal cols/vals pair) keeps the !rebuildOff path provably untouched.
	if rebuildOff {
		cols = cols[:len(cols)-1] + `, rebuild_on_failure)`
		vals = vals[:len(vals)-1] + `, ` + cluster.LitInt(0) + `)`
	}
	if insertWhere != "" {
		// origin: hot-update S6 plan §7.2 / T42
		// INSERT SELECT turns a stale absence/maintenance decision into zero
		// rows rather than a unique-constraint failure in the replicated FSM.
		vals = ` SELECT ` + vals[len(` VALUES (`):len(vals)-1] + ` WHERE ` + insertWhere +
			` AND NOT EXISTS (SELECT 1 FROM port_allocations WHERE port=` + cluster.LitInt(int64(publicPort)) + ` AND state='ALLOCATED')`
	}
	sql := `INSERT INTO port_allocations` + cols + vals

	alloc := &Allocation{
		Port: publicPort, SID: sid, NID: nid, Name: name, LocalPort: localPort,
		TokenHash: tokenHash, State: StateAllocated, CreatedByFP: createdByFP,
		CreatedAt: nowUTC, Token: token, HomeBroker: homeBroker, Epoch: 0,
		RebuildOff: rebuildOff,
	}
	return alloc, cluster.NewCommand(cluster.OpPortAllocate, cluster.Stmt(sql)), nil
}

// PlanAllocateProxy renders OpProxyAllocate (C5): the leader-baked __proxy__ allocation for one
// (sid, nid), home-stamped. Unlike PlanAllocate it prechecks per-(sid, nid) uniqueness (NOT
// per-(sid, name) — '__proxy__' is the shared name, so a per-name check would reject a 2nd node's
// proxy), and it FREES any existing ALLOCATED __proxy__ row for the node first (reuse-or-replace) in
// the SAME Command. The leader picks the free port + mints the token; the raw token rides
// Allocation.Token leak-once (only its hash is baked). homeBroker is the resolved D6 home (epoch=0).
//
// MUST run under Node.Propose (applyMu) on the live n.db — like PlanAllocate, findFreePort reads the
// committed, serialized view so two concurrent allocations cannot bake the same port (the global
// idx_port_alloc_unique_active would otherwise trip at Apply and fail-stop the FSM, Stage-C B2).
func PlanAllocateProxy(db *sql.DB, sid, nid, homeBroker string, cfg *Config) (*Allocation, *cluster.Command, error) {
	low, high, now := cfgWithDefaults(cfg)
	if homeBroker == "" {
		return nil, nil, fmt.Errorf("port: plan proxy allocate: homeBroker required (cluster mode)")
	}
	// Per-(sid,nid) precheck: an EXISTING ALLOCATED __proxy__ row is freed (not a conflict) — but a
	// concurrent insert is fenced by the baked free-then-insert order on the serial Apply.
	freePort, err := findFreePort(db, low, high)
	if err != nil {
		return nil, nil, err
	}
	token, err := genToken()
	if err != nil {
		return nil, nil, fmt.Errorf("port: plan proxy allocate token: %w", err)
	}
	tokenHash := hashToken(token)
	nowUTC := now.UTC()
	lits, err := cluster.LitTextAll(sid, nid, ProxyPortName, tokenHash, homeBroker)
	if err != nil {
		return nil, nil, fmt.Errorf("port: plan proxy allocate literal: %w", err)
	}
	sidLit, nidLit, nameLit, thLit, homeLit := lits[0], lits[1], lits[2], lits[3], lits[4]
	// 1. FREE any existing ALLOCATED __proxy__ for this (sid,nid) (reuse-or-replace).
	freeExisting := `UPDATE port_allocations SET state='FREED', revoked_at=` + cluster.LitTime(nowUTC) +
		` WHERE sid=` + sidLit + ` AND nid=` + nidLit + ` AND name=` + nameLit + ` AND state='ALLOCATED'`
	// 2. INSERT the fresh home-stamped row (epoch=0, created_by_fp='' = system-owned).
	ins := `INSERT INTO port_allocations (port, sid, nid, name, local_port, token_hash, state, created_by_fp, created_at, home_broker, epoch) ` +
		`VALUES (` + cluster.LitInt(int64(freePort)) + `, ` + sidLit + `, ` + nidLit + `, ` + nameLit + `, 0, ` + thLit +
		`, 'ALLOCATED', '', ` + cluster.LitTime(nowUTC) + `, ` + homeLit + `, 0)`
	alloc := &Allocation{
		Port: freePort, SID: sid, NID: nid, Name: ProxyPortName, LocalPort: 0,
		TokenHash: tokenHash, State: StateAllocated, CreatedByFP: "", CreatedAt: nowUTC,
		Token: token, HomeBroker: homeBroker, Epoch: 0,
	}
	return alloc, cluster.NewCommand(cluster.OpProxyAllocate, cluster.Stmt(freeExisting), cluster.Stmt(ins)), nil
}

// PlanReassignHome renders OpPortReassignHome (D6 §7.1-7.2): re-point one
// expose's home broker and bump its per-port epoch by exactly 1. The leader
// reads the current epoch on its DB (MUST run under Node.Propose's applyMu so a
// concurrent reassign cannot read the same base epoch) and bakes an all-literal
// UPDATE with a MONOTONIC CAS GUARD: `... WHERE port=<lit> AND state='ALLOCATED'
// AND epoch < <LitInt(newEpoch)>`. A stale ex-leader proposing a LOWER-or-equal
// epoch is then a deterministic RowsAffected==0 no-op on every replica — the
// FSM-layer fence against an ex-home double-bind. Returns the new epoch so the
// caller can stamp it into the HomeDirective it sends the agent.
//
// ErrNotFound when no ALLOCATED row carries this port (nothing to rehome). The
// leader-driven backup path carries NO reqID (R-8): the CAS guard is the sole
// idempotency anchor, since a leader-push has no originating-broker-minted key.
//
// C6 (建议5): Reassign now stamps last_rehome_at = LitTime(now.UTC()) INSIDE the CAS-guarded UPDATE
// (the row's created_at stays immutable; last_rehome_at is the rehome observability stamp powering
// `cluster status --homes`). Because it is in the same `WHERE … epoch < newEpoch` UPDATE, a stale-epoch
// RowsAffected==0 no-op never stamps it. All-literal → deterministic on every replica.
//
// NOTE (review A2 m2): the returned newEpoch is computed PRE-Apply; a caller must
// stamp its HomeDirective from the post-Apply row (homeForRegister/homeForExpose
// read the row), NOT from this return, so a lost CAS race (a stale ex-leader
// committing a lower epoch — impossible under single-leader raft, defense only)
// can never point an agent at the losing home. The CAS guard makes Apply a
// deterministic RowsAffected==0 no-op on every replica regardless.
func PlanReassignHome(db *sql.DB, publicPort int, newHome string, now time.Time) (int64, *cluster.Command, error) {
	if newHome == "" {
		return 0, nil, fmt.Errorf("port: plan reassign-home requires a non-empty home")
	}
	// Only a live (ALLOCATED) expose can be rehomed; a freed/revoked port has
	// nothing to migrate. The lookup predicate enforces this before planning.
	var curEpoch int64
	switch err := db.QueryRow(
		`SELECT epoch FROM port_allocations WHERE port=? AND state='ALLOCATED'`, publicPort,
	).Scan(&curEpoch); {
	case err == sql.ErrNoRows:
		return 0, nil, ErrNotFound
	case err != nil:
		return 0, nil, fmt.Errorf("port: plan reassign-home lookup: %w", err)
	}
	newEpoch := curEpoch + 1
	homeLit, err := cluster.LitText(newHome)
	if err != nil {
		return 0, nil, fmt.Errorf("port: plan reassign-home literal: %w", err)
	}
	sql := `UPDATE port_allocations SET home_broker=` + homeLit +
		`, epoch=` + cluster.LitInt(newEpoch) +
		`, last_rehome_at=` + cluster.LitTime(now.UTC()) +
		` WHERE port=` + cluster.LitInt(int64(publicPort)) +
		` AND state='ALLOCATED' AND epoch < ` + cluster.LitInt(newEpoch)
	return newEpoch, cluster.NewCommand(cluster.OpPortReassignHome, cluster.Stmt(sql)), nil
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

// PlanReassignHomeGuarded plans only against the live writer DB passed by
// Node.Propose. Its epoch is an expected projection, never a committed receipt;
// the caller must read back the complete identity before publishing a directive.
// origin: hot-update S6 plan §2.1 / §7.2 / T42
func PlanReassignHomeGuarded(db *sql.DB, guard MutationGuard, newHome string, now time.Time) (int64, *cluster.Command, error) {
	if guard.Expected == nil || guard.ExpectAbsent || newHome == "" || guard.ToHome != newHome ||
		guard.Expected.Epoch == math.MaxInt64 {
		return 0, nil, ErrMutationGuard
	}
	where, err := mutationPredicate(db, guard)
	if err != nil {
		return 0, nil, err
	}
	home, err := cluster.LitText(newHome)
	if err != nil {
		return 0, nil, err
	}
	epoch := guard.Expected.Epoch + 1
	sql := `UPDATE port_allocations SET home_broker=` + home + `, epoch=` + cluster.LitInt(epoch) +
		`, last_rehome_at=` + cluster.LitTime(now.UTC()) + ` WHERE ` + where
	return epoch, cluster.NewCommand(cluster.OpPortReassignHome, cluster.Stmt(sql)), nil
}

// PlanAllocateGuarded requires explicit absence of the logical allocation and
// explicit current maintenance expectations for the target home. Proxy names
// use (SID,NID), preserving the existing proxy uniqueness contract.
// origin: hot-update S6 plan §2.1 / §7.2 / T42
func PlanAllocateGuarded(db *sql.DB, guard MutationGuard, intent AllocationIntent) (*Allocation, *cluster.Command, error) {
	if guard.Expected != nil || !guard.ExpectAbsent || guard.FromHome != "" || guard.ToHome != intent.HomeBroker {
		return nil, nil, ErrMutationGuard
	}
	// Absence alone cannot let an old grant create a port under a reused SID.
	// origin: hot-update S8 plan §6.2 / TestPortMutationRejectsRecreatedSession
	if !allocationAuthorityMatches(guard.Authority, intent.SID, intent.NID) {
		return nil, nil, ErrMutationGuard
	}
	maintenance, err := mutationPredicate(db, guard)
	if err != nil {
		return nil, nil, err
	}
	absence, err := allocationAbsenceSQL(intent)
	if err != nil {
		return nil, nil, err
	}
	return planAllocateIntent(db, intent, nil, maintenance+` AND `+absence)
}

// PlanRotateAllocationGuarded keeps free and insert in ONE replicated
// transaction. Both statements carry the same maintenance expectations and
// full old identity. changes() belongs to the transaction's connection: it
// proves that this command's immediately preceding free actually won its CAS.
// origin: hot-update S6 plan §2.1 / §7.2 / T42
func PlanRotateAllocationGuarded(db *sql.DB, guard MutationGuard, intent AllocationIntent, now time.Time) (*Allocation, *cluster.Command, error) {
	old := guard.Expected
	if old == nil || guard.ExpectAbsent || guard.ToHome != intent.HomeBroker ||
		old.SID != intent.SID || old.NID != intent.NID || old.Name != intent.Name {
		return nil, nil, ErrMutationGuard
	}
	where, err := mutationPredicate(db, guard)
	if err != nil {
		return nil, nil, err
	}
	freedIdentity, err := allocationIdentitySQL(*old, StateFreed)
	if err != nil {
		return nil, nil, err
	}
	maintenance, err := maintenancePredicate(db, guard)
	if err != nil {
		return nil, nil, err
	}
	absence, err := allocationAbsenceSQL(intent)
	if err != nil {
		return nil, nil, err
	}
	// Use one leader timestamp for both halves, including callers with Config.Now.
	cfg := Config{}
	if intent.Config != nil {
		cfg = *intent.Config
	}
	cfg.Now = func() time.Time { return now }
	intent.Config = &cfg
	insertWhere := `changes()=1 AND EXISTS (SELECT 1 FROM port_allocations WHERE ` + freedIdentity +
		` AND revoked_at=` + cluster.LitTime(now.UTC()) + `) AND ` + maintenance + ` AND ` + absence
	allocation, insert, err := planAllocateIntent(db, intent, old, insertWhere)
	if err != nil {
		return nil, nil, err
	}
	// origin: hot-update S6 plan §7.2 / T42
	// A stale target-port decision must suppress BOTH halves. Otherwise a
	// guarded insert could lose to a new allocation after freeing the old one.
	oldIdentity, err := allocationIdentitySQL(*old, StateAllocated)
	if err != nil {
		return nil, nil, err
	}
	where += ` AND NOT EXISTS (SELECT 1 FROM port_allocations WHERE port=` + cluster.LitInt(int64(allocation.Port)) +
		` AND state='ALLOCATED' AND NOT (` + oldIdentity + `))`
	free := `UPDATE port_allocations SET state='FREED', revoked_at=` + cluster.LitTime(now.UTC()) + ` WHERE ` + where
	op := cluster.OpPortAllocate
	if intent.Name == ProxyPortName {
		op = cluster.OpProxyAllocate
	}
	return allocation, cluster.NewCommand(op, cluster.Stmt(free), insert.Body[0]), nil
}

// PlanFreeAllocationGuarded covers maintenance-sensitive rotation/cleanup.
// The legacy permission path remains independent of maintenance; widening its
// five-field forwarding payload requires a separate additive wire change.
// origin: hot-update S6 plan §2.1 / §7.2 / T26 / T42
func PlanFreeAllocationGuarded(db *sql.DB, guard MutationGuard, now time.Time) (*cluster.Command, error) {
	if guard.Expected == nil || guard.ExpectAbsent || guard.ToHome != "" {
		return nil, ErrMutationGuard
	}
	where, err := mutationPredicate(db, guard)
	if err != nil {
		return nil, err
	}
	sql := `UPDATE port_allocations SET state='FREED', revoked_at=` + cluster.LitTime(now.UTC()) + ` WHERE ` + where
	return cluster.NewCommand(cluster.OpPortFree, cluster.Stmt(sql)), nil
}

// PlanGCTerminated renders one OpPortGC chunk (h1 B1): up to `limit`
// FREED/REVOKED port_allocations rows whose end-of-life timestamp is older
// than `cutoff`, deleted by EXPLICIT row_id keyset with the terminal-state
// guard re-asserted. Returns (nil, 0, nil) when nothing is due (Propose
// no-ops on a nil command → idle-zero-writes).
//
// This is the op that drains the 2026-08-04 incident backlog: 24k FREED
// `__proxy__` rows minted by the reaper's 20s rotation loop, which nothing
// ever deleted (port_allocations had NO GC in any mode).
//
// Same three contracts as proc.PlanGCExited, same rationale (see its doc):
// retention comparison stays leader-side against heterogeneous timestamp
// TEXT (cutoff MUST be UTC); COALESCE(revoked_at, created_at) so a NULL
// revoked_at terminal row is not immortal; keyset materialized + Rows closed
// BEFORE the Command is built (shared SetMaxOpenConns(1) pool — an open
// *sql.Rows at Propose time wedges fsm.Apply forever).
func PlanGCTerminated(db *sql.DB, cutoff time.Time, limit int) (*cluster.Command, int, error) {
	rows, err := db.Query(
		`SELECT row_id FROM port_allocations
		  WHERE state IN ('FREED','REVOKED') AND COALESCE(revoked_at, created_at) < ?
		  ORDER BY row_id LIMIT ?`,
		cutoff.UTC(), limit,
	)
	if err != nil {
		return nil, 0, fmt.Errorf("port: plan gc select: %w", err)
	}
	var ids []int64
	for rows.Next() {
		var id int64
		if err := rows.Scan(&id); err != nil {
			_ = rows.Close()
			return nil, 0, fmt.Errorf("port: plan gc scan: %w", err)
		}
		ids = append(ids, id)
	}
	_ = rows.Close()
	if err := rows.Err(); err != nil {
		return nil, 0, fmt.Errorf("port: plan gc rows: %w", err)
	}
	if len(ids) == 0 {
		return nil, 0, nil
	}
	lits := make([]string, 0, len(ids))
	for _, id := range ids {
		lits = append(lits, cluster.LitInt(id))
	}
	sql := `DELETE FROM port_allocations WHERE row_id IN (` + strings.Join(lits, `, `) +
		`) AND state IN ('FREED','REVOKED')`
	return cluster.NewCommand(cluster.OpPortGC, cluster.Stmt(sql)), len(ids), nil
}

// allocationIdentitySQL fences the new guarded mutations. Home and
// exact epoch must survive planning AND replay: epoch inequality alone can hit
// an unrelated replacement with a freshly reset counter on the same port.
// origin: hot-update S6 plan §7.2 / T26 / T42
func allocationIdentitySQL(a Allocation, state State, full ...bool) (string, error) {
	lits, err := cluster.LitTextAll(a.SID, a.NID, a.Name, a.TokenHash)
	if err != nil {
		return "", err
	}
	identity := `port=` + cluster.LitInt(int64(a.Port)) + ` AND sid=` + lits[0] + ` AND nid=` + lits[1] +
		` AND name=` + lits[2] + ` AND token_hash=` + lits[3] + ` AND state=` + cluster.MustLitText(string(state))
	// The ordinary free/revoke payload still carries its original five fields;
	// maintenance writers additionally fence the exact home and epoch.
	// origin: hot-update S8 plan §6.2 / TestPortMutationRejectsRecreatedSession
	if len(full) == 0 || full[0] {
		home, err := cluster.LitText(a.HomeBroker)
		if err != nil {
			return "", err
		}
		identity += ` AND home_broker=` + home + ` AND epoch=` + cluster.LitInt(a.Epoch)
	}
	return identity, nil
}

var ErrMaintenanceUnknown = errors.New("port: maintenance proof unavailable or invalid")
