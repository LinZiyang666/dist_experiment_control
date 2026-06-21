package cluster

import (
	"context"
	"database/sql"
)

// Planner renders a request into a deterministic Command on the LEADER only
// (architecture §3.3): it reads the leader DB and bakes every value that feeds a
// PK / unique index / fence (ports, tokens, ULIDs, timestamps) into the SQL, so
// every replica's Apply is value-identical. D1 ships ONE trivial planner
// (clusterMetaPlanner); the full typed op set + real-mutator migration is D2.
type Planner interface {
	Plan(ctx context.Context, db *sql.DB, req any) (*Command, error)
}

// Applier executes ONE op's statements on a replica. It is BOUND TO *sql.Tx by
// construction — the type system therefore makes a replicated write that bypasses
// the transaction UNREPRESENTABLE (architecture §3.2 "zero direct db.Exec" + §3.7
// "applied_index in the same txn"). An Applier never sees a *sql.DB.
//
// The FSM writes applied_index/applied_term itself (FSM machinery, not op
// business), in the SAME txn it hands to ApplyTx. D1 has one Applier
// (clusterMetaApplier); D2 registers the typed §5 set behind this same seam.
type Applier interface {
	ApplyTx(tx *sql.Tx, cmd *Command) error
}
