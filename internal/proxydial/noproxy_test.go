package proxydial

import "testing"

func TestBypassProxy(t *testing.T) {
	cases := []struct {
		host, noProxy string
		want          bool
	}{
		// Built-in loopback bypass — applies even with empty NO_PROXY so the dev
		// broker nats://127.0.0.1:4222 never goes through the proxy.
		{"localhost", "", true},
		{"127.0.0.1", "", true},
		{"127.0.0.5", "", true},
		{"::1", "", true},
		{"0:0:0:0:0:0:0:1", "", true},
		{"weiland.top", "", false},
		// Star = everything direct.
		{"weiland.top", "*", true},
		// Bare host (D4): matches the domain AND its subdomains.
		{"example.com", "example.com", true},
		{"foo.example.com", "example.com", true},
		{"example.com", "other.com", false},
		{"notexample.com", "example.com", false}, // suffix must be on a dot boundary
		// Leading dot: subdomains only.
		{"foo.example.com", ".example.com", true},
		{"example.com", ".example.com", false},
		// Case-insensitive.
		{"EXAMPLE.com", "example.com", true},
		// CIDR only bites an IP-literal target (under SkipHostLookup the dialer
		// usually sees a hostname, for which CIDR can't match).
		{"10.1.2.3", "10.0.0.0/8", true},
		{"weiland.top", "10.0.0.0/8", false},
		{"11.0.0.1", "10.0.0.0/8", false},
		// Entry may carry a port; only the host is compared.
		{"weiland.top", "weiland.top:443", true},
		// IP-literal entry.
		{"1.2.3.4", "1.2.3.4", true},
		{"1.2.3.5", "1.2.3.4", false},
		// Multiple comma entries with whitespace.
		{"weiland.top", "a.com, weiland.top , b.com", true},
		{"c.com", "a.com, weiland.top, b.com", false},
		// Trailing FQDN dot tolerated.
		{"example.com.", "example.com", true},
	}
	for _, c := range cases {
		if got := bypassProxy(c.host, c.noProxy); got != c.want {
			t.Errorf("bypassProxy(%q, %q) = %v, want %v", c.host, c.noProxy, got, c.want)
		}
	}
}
