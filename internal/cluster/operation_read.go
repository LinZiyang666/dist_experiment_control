package cluster

import (
	"database/sql"
	"fmt"
)

// operation_read.go (C4) — the pure-RODB read side of the operation log (architecture L-2: no nats,
// no raft). The controller + the `ops` read surface use these; they are leader-agnostic bounded-stale
// reads of cluster_operations.

// Operation is one row of cluster_operations (the workflow-progress SSOT — NOT membership).
type Operation struct {
	OpID            string
	Kind            string
	TargetNode      string
	OpState         string
	Confirmed       bool
	ConfirmedFT     int64
	Barrier         uint64
	CatchupDeadline int64
	TopoTargetGen   uint64
	JoinNonce       string
	Params          string
	LastError       string
	Timeline        string
	Terminal        bool
	CreatedAt       string
	UpdatedAt       string
}

const opSelectCols = `op_id, kind, target_node, op_state, confirmed, confirmed_ft, barrier, ` +
	`catchup_deadline, topo_target_gen, join_nonce, params, last_error, timeline, terminal, created_at, updated_at`

func scanOperation(s interface{ Scan(...any) error }) (*Operation, error) {
	var o Operation
	var confirmed, terminal int
	var barrier, topoGen int64
	if err := s.Scan(&o.OpID, &o.Kind, &o.TargetNode, &o.OpState, &confirmed, &o.ConfirmedFT, &barrier,
		&o.CatchupDeadline, &topoGen, &o.JoinNonce, &o.Params, &o.LastError, &o.Timeline, &terminal,
		&o.CreatedAt, &o.UpdatedAt); err != nil {
		return nil, err
	}
	o.Confirmed = confirmed != 0
	o.Terminal = terminal != 0
	o.Barrier = uint64(barrier)
	o.TopoTargetGen = uint64(topoGen)
	return &o, nil
}

// scanOperationRows fully materializes the rows BEFORE returning (the D6 deadlock lesson: no nested
// query under MaxOpenConns(1)).
func scanOperationRows(rows *sql.Rows) ([]Operation, error) {
	defer func() { _ = rows.Close() }()
	var out []Operation
	for rows.Next() {
		o, err := scanOperation(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, *o)
	}
	return out, rows.Err()
}

// NonTerminalOperations returns every in-flight operation (the controller's resume work-list),
// oldest-first so a deterministic drive order.
func NonTerminalOperations(ro *sql.DB) ([]Operation, error) {
	rows, err := ro.Query(`SELECT ` + opSelectCols + ` FROM cluster_operations WHERE terminal = 0 ORDER BY created_at, op_id`)
	if err != nil {
		return nil, fmt.Errorf("cluster: list non-terminal operations: %w", err)
	}
	return scanOperationRows(rows)
}

// ActiveOperationForTarget returns the single non-terminal operation for a node, or nil if none (the
// single-active-guard means there is at most one).
func ActiveOperationForTarget(ro *sql.DB, target string) (*Operation, error) {
	row := ro.QueryRow(`SELECT `+opSelectCols+` FROM cluster_operations WHERE target_node = ? AND terminal = 0 LIMIT 1`, target)
	o, err := scanOperation(row)
	if err == sql.ErrNoRows {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("cluster: active operation for %q: %w", target, err)
	}
	return o, nil
}

// LatestOperationForTarget returns the most recently updated operation for a node —
// terminal or not — or nil when the node has never had one. ActiveOperationForTarget
// answers "is something in flight"; this answers "what happened last", which is what
// the R7 grow-lock reaper needs to tell a FINISHED grow (terminal row ⇒ the lock's
// holder is gone) from a grow that has not created its operation yet (no row at all ⇒
// the acquire-lock window, hands off).
func LatestOperationForTarget(ro *sql.DB, target string) (*Operation, error) {
	row := ro.QueryRow(`SELECT `+opSelectCols+` FROM cluster_operations WHERE target_node = ? ORDER BY updated_at DESC, op_id LIMIT 1`, target)
	o, err := scanOperation(row)
	if err == sql.ErrNoRows {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("cluster: latest operation for %q: %w", target, err)
	}
	return o, nil
}

// OperationByID returns one operation by its op_id (= plan-id), or nil if absent.
func OperationByID(ro *sql.DB, opID string) (*Operation, error) {
	row := ro.QueryRow(`SELECT `+opSelectCols+` FROM cluster_operations WHERE op_id = ?`, opID)
	o, err := scanOperation(row)
	if err == sql.ErrNoRows {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("cluster: operation %q: %w", opID, err)
	}
	return o, nil
}

// RecentOperations returns up to limit operations, newest-first (for `cluster ops ls`).
func RecentOperations(ro *sql.DB, limit int) ([]Operation, error) {
	if limit <= 0 {
		limit = 100
	}
	rows, err := ro.Query(`SELECT `+opSelectCols+` FROM cluster_operations ORDER BY updated_at DESC, op_id LIMIT ?`, limit)
	if err != nil {
		return nil, fmt.Errorf("cluster: recent operations: %w", err)
	}
	return scanOperationRows(rows)
}

// NonceConsumed reports whether a (target_node, join_nonce) pair has already been admitted by a
// TERMINAL prior operation — the replay-AFTER-retire dedup (C4 §7/M1). It counts ONLY terminal rows:
// a non-terminal (in-flight) op with the same nonce is handled by the single-active guard + the
// idempotent same-op_id re-approve, NOT treated as a replay (else a plain double-approve of the same
// bundle would be wrongly refused). An empty nonce is never "consumed".
func NonceConsumed(ro *sql.DB, target, nonce string) (bool, error) {
	if nonce == "" {
		return false, nil
	}
	var n int
	if err := ro.QueryRow(`SELECT COUNT(1) FROM cluster_operations WHERE target_node = ? AND join_nonce = ? AND terminal = 1`, target, nonce).Scan(&n); err != nil {
		return false, fmt.Errorf("cluster: nonce-consumed check: %w", err)
	}
	return n > 0, nil
}
