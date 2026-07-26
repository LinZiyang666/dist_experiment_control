// Package tokenhash is the single implementation of the bearer-token hashing
// scheme used across tether's token namespaces (public-port tokens, tunnel
// REGISTER tokens, proxy-subscription tokens).
//
// Batch-A A11. Four byte-identical copies of hex(sha256(raw)) existed:
// internal/port, internal/tunnel, internal/proxysub (exported as HashToken) and
// cmd/tether (as hexSHA256, whose doc even called itself "the canonical" pair).
// None of them referenced the others.
//
// A previous audit found three of them, judged it low severity and chose to
// "solve it with a comment". The comment worked — internal/tunnel's copy says
// clearly that it must match the others — and a year later proxysub grew a
// fourth copy anyway, whose own comment says "same scheme as port tokens",
// unaware there were already two.
//
// The reason this matters more than four duplicated lines: the copies must
// agree FOREVER, and disagreement is not a compile error, not a test failure,
// and not even a visible fault. Add a pepper to one of them and every agent's
// tunnel REGISTER is refused at once — a fleet-wide, fail-closed outage whose
// error messages point at token distribution, not at a hash function.
//
// Why a new package rather than reusing proxysub.HashToken (already exported):
// proxysub pulls in database/sql and crypto/rand, and internal/tunnel is
// deliberately a dependency-graph leaf. This package imports only stdlib
// hashing, so every caller can depend on it without acquiring anything else.
//
// The scheme is wire/storage-visible: port_allocations.token_hash and the proxy
// subscription rows hold values produced by it. Changing the algorithm
// invalidates every stored hash — it is a data migration, not a refactor.
package tokenhash

import (
	"crypto/sha256"
	"encoding/hex"
)

// Sum returns the lowercase hex encoding of SHA-256 over raw.
func Sum(raw string) string {
	sum := sha256.Sum256([]byte(raw))
	return hex.EncodeToString(sum[:])
}

// SumBytes is Sum for callers that already hold a byte slice.
func SumBytes(raw []byte) string {
	sum := sha256.Sum256(raw)
	return hex.EncodeToString(sum[:])
}
