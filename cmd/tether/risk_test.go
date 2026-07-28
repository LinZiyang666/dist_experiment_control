package main

import "testing"

// origin: p9_review_risk_test.go (renamed in B6) — docs/reviews/p9-review.md
func TestReviewServeAdminSocketDefaultMatchesAdminClient(t *testing.T) {
	flag := newServeCmd().Flags().Lookup("admin-socket")
	if flag == nil {
		t.Fatal("serve command missing --admin-socket flag")
	}
	if flag.DefValue != defaultAdminSocket {
		t.Fatalf("serve --admin-socket default = %q, want %q to match tether admin default",
			flag.DefValue, defaultAdminSocket)
	}
}
