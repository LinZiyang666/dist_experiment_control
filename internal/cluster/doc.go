// Package cluster is the home of the distributed-broker Raft state layer
// (docs/distributed-broker-architecture.md §2/§3).
//
// D0 scope: this package contains ONLY dependency-verification tests — the
// PreVote merge gate (prevote_test.go) and the pinned-API compile references
// (deps_test.go). There is deliberately NO product code here yet: the Raft FSM,
// SQLite Apply path, snapshot/restore and node wiring land in D1 (§3, §19
// dependency warning "❌ D1 之前碰任何 apply.*"). Keeping D0 product (non-test)
// code free of any raft import is enforced by the determinism-lint
// raft-confinement guard (test/determinism, §13.1 L-2) — hence this file
// imports nothing.
package cluster
