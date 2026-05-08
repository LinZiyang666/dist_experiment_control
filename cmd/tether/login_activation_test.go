package main

import (
	"bytes"
	"os"
	"path/filepath"
	"testing"
)

func TestLoginSessionWithoutPINRequiresMembershipVerification(t *testing.T) {
	home := t.TempDir()
	cmd := newLoginCmd()
	var out bytes.Buffer
	cmd.SetOut(&out)
	cmd.SetErr(&out)
	cmd.SetArgs([]string{
		"--home", home,
		"--nats-url", "nats://127.0.0.1:1",
		"--session", "ghost",
	})

	if err := cmd.Execute(); err == nil {
		t.Fatalf("login -s activated a session without contacting broker or verifying membership; current_session=%q",
			readFileIfExists(t, filepath.Join(home, "current_session")))
	}
}

func readFileIfExists(t *testing.T, path string) string {
	t.Helper()
	b, err := os.ReadFile(path)
	if os.IsNotExist(err) {
		return ""
	}
	if err != nil {
		t.Fatal(err)
	}
	return string(b)
}
