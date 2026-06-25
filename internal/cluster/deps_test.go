package cluster

import (
	"github.com/hashicorp/raft"
	raftboltdb "github.com/hashicorp/raft-boltdb/v2"
)

// Dependency-pin compile references (§19 D0: "pin hashicorp/raft +
// raft-boltdb/v2 + FileSnapshotStore 确切版本").
//
// These live in a _test.go on purpose: D0 must not wire raft into any product
// (non-test) package (§13.1 L-2 raft-confinement guard). Referencing the
// raft-boltdb/v2 types here keeps the module in the require set through
// `go mod tidy` even though no D0 product code uses BoltStore yet (it backs the
// Raft log/stable store in D1), and proves the §19-named APIs exist and compile
// against the pinned versions (raft v1.7.3, raft-boltdb/v2 v2.3.1).
var (
	_ *raft.FileSnapshotStore     // §19 names FileSnapshotStore among the 3 pins (D1 consumes the struct)
	_ = raft.NewFileSnapshotStore // also pin the §19-NAMED constructor (catches an API rename)
	_ *raftboltdb.BoltStore       // D1 will back the Raft log/stable store with this
	_ raftboltdb.Options
)
