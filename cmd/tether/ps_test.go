package main

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/LinZiyang666/tether/internal/auth"
	"github.com/LinZiyang666/tether/internal/testharness"
)

// origin: ps_review_test.go (renamed in B6)
func TestPsNoResponderErrorIsNotLabeledTimeout(t *testing.T) {
	url := testharness.StartNATS(t)
	home := t.TempDir()
	writeReviewPsIdentity(t, home, "lab")

	t.Setenv("TETHER_DEV_NO_AUTH", "1")
	t.Setenv("TETHER_PS_TIMEOUT", "500ms")

	_, _, err := runRoot(t, "ps", "--nats-url", url, "--home", home)
	if err == nil {
		t.Fatal("expected ps to fail without a broker responder")
	}
	msg := err.Error()
	if strings.Contains(msg, "request timed out after") {
		t.Fatalf("no-responder path mislabeled as timeout: %s", msg)
	}
	if !strings.Contains(msg, "no responders") {
		t.Fatalf("expected no-responders diagnostic, got: %s", msg)
	}
}

func writeReviewPsIdentity(t *testing.T, home, sid string) {
	t.Helper()
	if err := os.MkdirAll(filepath.Join(home, "keys"), 0o700); err != nil {
		t.Fatal(err)
	}
	seed, err := auth.GenerateUserSeed()
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(home, "keys", "default.nk"), seed, 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(home, "current_session"), []byte(sid), 0o600); err != nil {
		t.Fatal(err)
	}
}
