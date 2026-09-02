// Package stackharness holds the single-node stack primitives that need PRODUCT packages — the
// ones internal/testharness cannot hold without an import cycle.
//
// WHY A THIRD HARNESS PACKAGE
// ---------------------------
// internal/testharness is imported by fourteen `package broker` tests, seven `package agent` tests and
// internal/session's own test. The moment it imports internal/broker, internal/agent or
// internal/session, every one of those packages imports a package that imports them back, and the
// build dies. That is the whole reason its header says "only the truly identical primitives live
// here" and why sixteen copies of seedSession / startBroker / startAgent grew in the phase suites:
// the shared place could not hold anything that touches the product.
//
// This package can. It lives under test/, it may import anything, and it may only ever be imported by
// `_test.go` files or by packages under test/ (test/architecture/layering_test.go enforces that
// direction). It sits beside test/clusterharness, which holds the CLUSTER scaffolding for the same
// reason and whose header records why the cluster builders themselves are never merged.
//
// WHAT IS HERE TODAY
// ------------------
// SeedSession only. Eight suites carried a byte-for-byte or Fatalf-format-only copy of the same
// session.Create call; those eight now forward here, and seed_test.go's absorbedSeedSession table
// asserts they stay forwarders (docs/testing-standards.md §六 R2). startBroker / startAgent are NOT
// absorbed: their sixteen copies differ in broker.Config / agent.Config fields per suite and the
// review judged the functional-options form worth building only when the next Config field change
// forces sixteen edits (docs/reviews/test-system-overhaul-plan.md §0 A3).
package stackharness

import (
	"database/sql"
	"testing"
	"time"

	"github.com/LinZiyang666/tether/internal/session"
)

// PlaceholderPINHash is the placeholder pin hash every absorbed seed used. Tests that need a REAL PIN go
// through auth.HashPIN themselves (the WithPIN variants were not absorbed).
const PlaceholderPINHash = "test-pin-hash"

// SeedSession creates the ACTIVE session `sid` owned by `ownerFP` through the production writer
// (session.Create), exactly as the eight absorbed helpers did. It deliberately does NOT take a raw
// INSERT path: the two suites that seed sessions with raw SQL (test/d3, test/d4) do so to construct
// column-level fixtures on a raft-replicated DB and were left alone.
func SeedSession(t testing.TB, db *sql.DB, sid, ownerFP string) {
	t.Helper()
	if _, err := session.Create(db, sid, sid, ownerFP, PlaceholderPINHash, time.Now().UTC()); err != nil {
		t.Fatalf("session.Create(%q): %v", sid, err)
	}
}
