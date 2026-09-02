package main

import (
	"fmt"
	"testing"

	"github.com/LinZiyang666/tether/internal/adminsock"
)

// skew_exit_test.go (formerly b6_skew_exit_test.go) — B6 A3: the CLI maps a version_skew admin code to exit 64 (operator
// fixes it by reinstalling the joiner on a matching release), not exit 70 (internal).
func TestVersionSkewMapsToExitUsage(t *testing.T) {
	if got := brokerCodeExitClass(adminsock.CodeVersionSkew); got != exitUsage {
		t.Fatalf("version_skew must map to exit %d (usage), got %d", exitUsage, got)
	}
}

// TestNoActiveSessionExitsUsage — Audit UX-MAJOR-2: a "no active session" precondition is exit 64
// (usage), not 70 (a tether bug a retry-wrapper would retry forever).
func TestNoActiveSessionExitsUsage(t *testing.T) {
	err := fmt.Errorf("no active session — run `tether login -s <sid>` first")
	if got := classifyExit(err); got != exitUsage {
		t.Fatalf("no-active-session must classify as exit %d, got %d", exitUsage, got)
	}
}
