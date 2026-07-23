package main

import "testing"

// TestExternalReviewStandaloneResetBackupNamesDoNotCollide exercises the identity
// boundary used by reconcile-nats --to-standalone. MoveAsideJSStore treats an
// existing backup path as proof that this exact move already happened. Therefore
// two command attempts must not receive the same name: after the first move, a
// running nats-server can write into the recreated store before an Apply failure
// is retried, and a collision would silently leave that live store in place.
func TestExternalReviewStandaloneResetBackupNamesDoNotCollide(t *testing.T) {
	seen := make(map[string]struct{}, 10_000)
	for range 10_000 {
		stamp := nowStampUTC()
		if _, exists := seen[stamp]; exists {
			t.Fatalf("two reset attempts received the same idempotency key %q; second-resolution names can mistake a new live store for an already-moved one", stamp)
		}
		seen[stamp] = struct{}{}
	}
}
