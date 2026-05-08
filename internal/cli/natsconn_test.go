package cli

import (
	"strings"
	"testing"
)

func TestConnectNATSWithNkeyRejectsNilIdentity(t *testing.T) {
	if _, err := ConnectNATSWithNkey("nats://127.0.0.1:4222", nil); err == nil {
		t.Fatal("expected error for nil identity")
	}
}

// Closed port — exercises the underlying nats.Connect() path and confirms
// the helper passes the URL through (rather than e.g. silently succeeding).
func TestConnectNATSWithNkeyFailsOnUnreachable(t *testing.T) {
	id := &Identity{
		PublicKey:   "U" + strings.Repeat("A", 55),
		Fingerprint: "SHA256:" + strings.Repeat("a", 43),
		// Seed left empty: P3 helper doesn't sign anything (no nkey credentials
		// are sent), so a nil seed is fine.
	}
	if _, err := ConnectNATSWithNkey("nats://127.0.0.1:1", id); err == nil {
		t.Fatal("expected connect error to closed port")
	}
}
