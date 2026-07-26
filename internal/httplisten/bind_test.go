package httplisten

import (
	"net"
	"testing"
)

// Batch-A review B1. The AST policy check in policy_test.go asserts each package
// passes the right requireLoopback bool — but a bool argument is only as good as
// what Bind does with it. Consolidating three copies of the loopback guard
// inverted the empty-host case, and no behavioural test noticed, because the two
// pre-existing ones only fed "0.0.0.0:0" and "8.8.8.8:80".
//
// ":8080" is the shape an operator actually writes.
func TestBindRejectsNonLoopbackWhenRequired(t *testing.T) {
	refuse := []struct {
		name string
		addr string
	}{
		{"empty host binds every interface", ":0"},
		{"empty host, explicit port", ":8080"},
		{"explicit wildcard v4", "0.0.0.0:0"},
		{"explicit wildcard v6", "[::]:0"},
		{"routable address", "8.8.8.8:80"},
	}
	for _, tc := range refuse {
		t.Run(tc.name, func(t *testing.T) {
			ln, err := Bind("test", tc.addr, true)
			if err == nil {
				addr := ln.Addr().String()
				_ = ln.Close()
				t.Fatalf("Bind(%q, requireLoopback=true) succeeded on %s.\n"+
					"These surfaces are UNAUTHENTICATED (/sub vends subscriber tokens + PSK; the cluster "+
					"manifest vends an account-signed roster) and speak plaintext HTTP. Binding anything "+
					"but loopback publishes them to whatever the interface reaches.", tc.addr, addr)
			}
		})
	}

	accept := []string{"127.0.0.1:0", "localhost:0", "[::1]:0"}
	for _, addr := range accept {
		t.Run("accepts "+addr, func(t *testing.T) {
			ln, err := Bind("test", addr, true)
			if err != nil {
				t.Fatalf("Bind(%q, requireLoopback=true) refused a genuine loopback address: %v", addr, err)
			}
			host, _, _ := net.SplitHostPort(ln.Addr().String())
			if ip := net.ParseIP(host); ip == nil || !ip.IsLoopback() {
				t.Errorf("bound to %s, which is not loopback", ln.Addr())
			}
			_ = ln.Close()
		})
	}
}

// TestBindAllowsAnyInterfaceWhenNotRequired is the non-vacuity half: the guard
// must be scoped to the surfaces that asked for it. /metrics is meant to be
// scraped from a private interface, so a blanket refusal would be a different bug.
func TestBindAllowsAnyInterfaceWhenNotRequired(t *testing.T) {
	for _, addr := range []string{":0", "0.0.0.0:0", "127.0.0.1:0"} {
		ln, err := Bind("test", addr, false)
		if err != nil {
			t.Fatalf("Bind(%q, requireLoopback=false) refused: %v — /metrics must stay bindable "+
				"on a private interface", addr, err)
		}
		_ = ln.Close()
	}
}
