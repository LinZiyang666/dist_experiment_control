package main

import (
	"io"
	"strings"
	"testing"
)

// origin: S8 internal review cluster-roll F4
func TestClusterResumeRejectsReplanningBeforeConnecting(t *testing.T) {
	for _, args := range [][]string{{"--resume", "0123456789abcdef0123456789abcdef", "--to-version", "v2"}, {"--resume", "0123456789abcdef0123456789abcdef", "--expect-sha256", "new"}, {"--resume", "0123456789abcdef0123456789abcdef", "--broker-only"}, {"--resume", "0123456789abcdef0123456789abcdef", "--dry-run"}} {
		cmd := newClusterUpgradeCmd()
		cmd.SetOut(io.Discard)
		cmd.SetErr(io.Discard)
		cmd.SetArgs(append(args, "--home", t.TempDir()))
		if err := cmd.Execute(); err == nil || !strings.Contains(err.Error(), "accepts no new target or scope") {
			t.Fatalf("resume accepted a new target or scope: %v", args)
		}
	}
}

// origin: S8 internal review cluster-roll F4
func TestClusterResumeRequiresTheSignedControlCredential(t *testing.T) {
	cmd := newClusterUpgradeCmd()
	cmd.SetOut(io.Discard)
	cmd.SetErr(io.Discard)
	cmd.SetArgs([]string{"--resume", "0123456789abcdef0123456789abcdef", "--home", t.TempDir()})
	if err := cmd.Execute(); err == nil || !strings.Contains(err.Error(), "--account-seed") {
		t.Fatalf("resume still used the unwired nil adapters: %v", err)
	}
}
