package cli

import (
	"strings"
	"testing"
)

// TestConnectNATSWithNkey_ProxyWiredFailClosed proves the proxy-aware dial is
// wired into the ctl connect path: a set-but-unsupported proxy env makes the
// connect fail closed (the proxydial error surfaces) BEFORE any network dial,
// rather than silently connecting directly. This is the cheap proof that
// proxydial.Options is appended in ConnectNATSWithNkey.
func TestConnectNATSWithNkey_ProxyWiredFailClosed(t *testing.T) {
	t.Setenv("HTTPS_PROXY", "ftp://unsupported-proxy:21")
	id := &Identity{PublicKey: "UTESTPUBKEY", Seed: []byte("ignored-no-network")}

	_, err := ConnectNATSWithNkey("nats://127.0.0.1:4222", id)
	if err == nil {
		t.Fatal("expected a fail-closed error from the proxydial injection")
	}
	if !strings.Contains(err.Error(), "proxydial") {
		t.Errorf("expected the proxydial fail-closed error, got: %v", err)
	}
}
