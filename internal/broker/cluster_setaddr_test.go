package broker

import "testing"

// cluster_setaddr_test.go (v0.4.2 review — validator security boundary). The set-raft-addr /
// set-route advertise validators are the only thing standing between an operator typo and a
// loopback/unspecified advertise that silently dooms a cross-network grow; this locks them.

func TestValidateAdvertiseHostPort(t *testing.T) {
	cases := []struct {
		addr          string
		allowLoopback bool
		ok            bool
	}{
		{"203.0.113.5:7400", false, true},        // public IP — the happy path
		{"broker.example.com:7400", false, true}, // DNS name (may resolve routable) — not flagged
		{"127.0.0.1:7400", false, false},         // loopback v4
		{"[::1]:7400", false, false},             // loopback v6
		{"0.0.0.0:7400", false, false},           // unspecified — a valid BIND, never an advertise
		{"[::]:7400", false, false},              // unspecified v6
		{":7400", false, false},                  // empty host
		{"203.0.113.5:", false, false},           // empty port
		{"203.0.113.5:0", false, false},          // port 0 (review suggestion)
		{"203.0.113.5:99999", false, false},      // out-of-range port (review suggestion)
		{"not-a-hostport", false, false},         // no port at all
		{"127.0.0.1:7400", true, true},           // loopback allowed under --allow-loopback
		{"0.0.0.0:7400", true, true},             // even unspecified allowed under the escape
	}
	for _, tc := range cases {
		err := validateAdvertiseHostPort(tc.addr, tc.allowLoopback)
		if (err == nil) != tc.ok {
			t.Errorf("validateAdvertiseHostPort(%q, allowLoopback=%v) err=%v, want ok=%v", tc.addr, tc.allowLoopback, err, tc.ok)
		}
	}
}

func TestValidateNatsRoute(t *testing.T) {
	cases := []struct {
		route         string
		allowLoopback bool
		ok            bool
	}{
		{"nats://203.0.113.5:6222", false, true},
		{"nats://broker.example.com:6222", false, true},
		{"nats://127.0.0.1:6222", false, false},   // loopback
		{"nats://0.0.0.0:6222", false, false},     // unspecified
		{"http://203.0.113.5:6222", false, false}, // wrong scheme
		{"nats://", false, false},                 // no host
		{"nats://h", false, false},                // no port
		{"nats://203.0.113.5:0", false, false},    // port 0 (review suggestion)
		{"nats://alice:s3cret@203.0.113.5:6222", false, false}, // userinfo credential (F6)
		{"nats://203.0.113.5:6222/x", false, false},            // path decoration (F6)
		{"garbage", false, false},
		{"nats://127.0.0.1:6222", true, true}, // loopback allowed under --allow-loopback
	}
	for _, tc := range cases {
		err := validateNatsRoute(tc.route, tc.allowLoopback)
		if (err == nil) != tc.ok {
			t.Errorf("validateNatsRoute(%q, allowLoopback=%v) err=%v, want ok=%v", tc.route, tc.allowLoopback, err, tc.ok)
		}
	}
}

func TestIsLoopbackHelpers(t *testing.T) {
	for _, a := range []string{"127.0.0.1:7400", "[::1]:7400", "0.0.0.0:7400", "[::]:7400"} {
		if !isLoopbackAdvertiseHostPort(a) {
			t.Errorf("isLoopbackAdvertiseHostPort(%q) = false, want true", a)
		}
	}
	for _, a := range []string{"203.0.113.5:7400", "broker.example.com:7400", "garbage"} {
		if isLoopbackAdvertiseHostPort(a) {
			t.Errorf("isLoopbackAdvertiseHostPort(%q) = true, want false", a)
		}
	}
	if !isLoopbackNatsRoute("nats://127.0.0.1:6222") {
		t.Error("isLoopbackNatsRoute(nats://127.0.0.1) = false, want true")
	}
	if isLoopbackNatsRoute("nats://203.0.113.5:6222") {
		t.Error("isLoopbackNatsRoute(nats://203.0.113.5) = true, want false")
	}
}
