package main

import "testing"

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
