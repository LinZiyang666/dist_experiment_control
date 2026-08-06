package broker

import (
	"context"
	"database/sql"
	"errors"
)

// dbrole.go (batch B, B3) — the three named DB roles.
//
// THE DEFECT
//
// `b.cfg.DB` is ONE field with TWO meanings. broker.go assigns `b.cfg.DB = cl.node.RODB()` at
// runtime when clusterMode is true, so the same field is a writable pool in single mode and a
// READ-ONLY pool in cluster mode. All three handles are `*sql.DB`, so the compiler cannot tell
// them apart, and every new data access has to answer a question that is only written in a
// comment. clusterwrite.go records the answer being got wrong: "route admin evict through raft
// (else the direct tx hits the RODB handle and fails)". reconcile.go records another: a write
// that "silently failed" because the error was swallowed and the state machine continued.
//
// The failure mode is a RUNTIME readonly-database error that only appears in cluster mode — and
// the ~126 zero-value `&Broker{}` literals in this package's tests mostly exercise the SINGLE
// mode path, so it is structurally uncovered.
//
// WHAT THIS FILE DOES AND DELIBERATELY DOES NOT DO
//
//   - `Config.DB` keeps its name AND its type. It is a CONSTRUCTOR input; changing it would
//     churn 52 test literals and break test/concurrency's direct `broker.Config{DB: db}` (which
//     would take the goroutine-leak gate, the fd gate and -race down with it) for zero safety
//     gain. The semantic switch happens at RUNTIME, not at construction.
//   - The accessors DERIVE from b.cfg.DB on every call. They are NOT cached in fields, because
//     broker.go re-points b.cfg.DB after New() returns and the 126 test literals never call
//     New()/Run() at all — a field would be stale in production and wrong in tests.
//   - singleWriter() returns a handle plus false; it does NOT panic. clusteradmin.go states there
//     is no recover() anywhere in this package, the unit is Restart=always, and the fleet has one
//     broker — a panic would turn a legible per-command error into an unattended crashloop of the
//     whole control plane. evictNode already chose a plain error return for the mirror-image
//     wiring bug, deliberately.
//
// The compile-time half of the guarantee: readDB exposes Query/QueryRow and nothing else, so
// `session.Create(b.read(), …)` does not compile. The other half — "nobody reaches around the
// accessors and touches b.cfg.DB directly" — is not expressible in the type system while
// Config.DB stays a *sql.DB, and is enforced by the AST ratchet in
// test/determinism instead. Neither half substitutes for the other.

// readDB is a read-only view of a database handle. It embeds NOTHING — embedding *sql.DB would
// re-export Exec and Begin and defeat the entire point.
type readDB struct {
	db *sql.DB
}

func (r readDB) Query(query string, args ...any) (*sql.Rows, error) {
	return r.db.Query(query, args...)
}

func (r readDB) QueryRow(query string, args ...any) *sql.Row {
	return r.db.QueryRow(query, args...)
}

func (r readDB) QueryRowContext(ctx context.Context, query string, args ...any) *sql.Row {
	return r.db.QueryRowContext(ctx, query, args...)
}

// SQL is the escape hatch for the domain functions that still take a concrete *sql.DB.
//
// It exists because widening ~35 leaf read signatures to an interface was CUT from this
// increment: those functions live in internal/{session,port,node,proc,agentprov}, five of the six
// packages test/determinism's CHA lint loads, and adding a shared interface there perturbs its
// over-approximation of interface dispatch. Until that widening happens, a read site that must
// pass a concrete handle calls this and the AST ratchet counts it.
//
// It is deliberately NOT named DB() or Handle(): a caller writing `.SQL()` is visibly opting out
// of the read-only guarantee, which is what makes the ratchet's allowlist meaningful.
func (r readDB) SQL() *sql.DB { return r.db }

// errClusterModeSingleWrite is what singleWriter reports instead of handing out a writable
// handle that would fail at the SQLite layer with `attempt to write a readonly database`.
var errClusterModeSingleWrite = errors.New(
	"broker: this write path is single-mode only; in cluster mode it must route through raft " +
		"(proposeOrForward) — see internal/broker/clusterwrite.go")

// read returns the handle for NON-authoritative reads. In cluster mode this is the node's
// read-only pool (Run re-points b.cfg.DB to it); in single mode it is the same pool everything
// else uses. Either way the returned type cannot write.
func (b *Broker) read() readDB { return readDB{db: b.cfg.DB} }

// THERE IS DELIBERATELY NO liveness() ACCESSOR HERE.
//
// One was written — `func (b *Broker) liveness() *sql.DB { return b.livenessDB() }` — as the third
// member of this file's read/liveness/singleWriter trio. It had zero callers, and converting
// livenessDB's six call sites to it would have been a rename to a synonym: livenessDB ALREADY names
// the role, already carries the godoc that reconciles the three-column set against its writers, and
// is what the reconciling doc references. A second name for the same handle adds a lookup, not a
// guarantee.
//
// That matters because this batch spent its budget deleting exactly this shape elsewhere
// (ClusterGrowSchemaVersion, ingress.status, ClusterNodeStatus.last_contact_secs): a declared
// symbol with a documented contract and no consumer. Keeping one here — and worse, manufacturing
// consumers for it so `unused` would go quiet — would have been the same defect with this batch's
// name on it.
//
// read() and singleWriter() are different: neither had a named equivalent, and both make a
// guarantee the raw *sql.DB does not (readDB cannot Exec; singleWriter refuses in cluster mode).

// singleWriter returns a writable handle ONLY in single mode.
//
// In cluster mode it returns (nil, false): the FSM owns the sole writer, and any authoritative
// write must go through proposeOrForward. Callers must check the bool — using the handle without
// checking would dereference nil, which is a louder failure than the readonly-database error this
// replaces, and is caught by the negative test rather than in production.
func (b *Broker) singleWriter() (*sql.DB, bool) {
	if b.clusterMode {
		return nil, false
	}
	return b.cfg.DB, true
}

// singleWriteRefusal is the error a caller returns when singleWriter denies. Naming it keeps the
// remedy wording in one place, so an operator who hits it on a clustered broker is pointed at
// raft rather than at their own command.
func singleWriteRefusal() error { return errClusterModeSingleWrite }
