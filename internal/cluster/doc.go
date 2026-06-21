// Package cluster is the distributed-broker Raft state layer (docs/
// distributed-broker-architecture.md §2/§3; plan docs/reviews/d1-plan.md).
//
// D1 builds the consensus heart at N=1: a single-node hashicorp/raft node whose
// FSM applies leader-rendered SQL to SQLite in ONE transaction that also advances
// applied_index/applied_term (§3.7 — applied_index authority = SQLite, written in
// the same txn as the op; zero direct db.Exec on the replicated write path), with
// online-backup snapshot (modernc NewBackup) / restore (integrity_check + forward
// migrations + in-place NewRestore) and a real-SIGKILL kill-9 crash-consistency
// matrix proving the §3.7 crash-consistency property: a restart loses NO committed
// LogCommand (raft re-delivers [lastSnapshot+1..commit] and the FSM self-skips
// index <= durable applied_index; snapshot.Index may exceed applied_index only by
// mutation-free barrier/config entries), with Apply fail-stopping rather than
// returning an un-applied command to raft.
//
// SCOPE (held hard — see d1-plan §1): D1 is test-constructible only. It is NOT
// wired into tetherd/broker.New (D2/D4), ships exactly ONE representative op
// (ClusterMetaSet) rather than migrating any real mutator (D2), and does NOT do
// apply.* forwarding, multinode, auth_callout or membership (D3+). The transport
// is injected — tests use raft.InmemTransport; the real mTLS NetworkTransport is
// D3. The determinism-lint raft-confinement guard (test/determinism, §13.1 L-2)
// now ALLOWS raft here (the first product raft import) while still forbidding it
// elsewhere.
package cluster
