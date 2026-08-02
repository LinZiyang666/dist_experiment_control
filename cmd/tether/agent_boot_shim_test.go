package main

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
	"time"
)

// origin: upgrade-safety external review F2 — the boot-shim contract can only
// be proven BLACK-BOX: the failure class it exists for is "the staged binary
// dies before RunE" (unknown flag, strict-YAML regression, logger failure),
// and any in-process test of a helper necessarily runs after those stages.
// This test builds the real tether binary, stages an upgrade marker next to
// it, launches it with a flag Cobra must reject, and asserts that every such
// doomed launch still consumes boot budget until the shim rolls the binary
// back to the prev slot — with no process ever reaching agent code.

func TestAgentBootShimConsumesBudgetOnPreRunEFailures(t *testing.T) {
	if testing.Short() {
		t.Skip("builds the full binary")
	}
	sandbox := t.TempDir()
	dst := filepath.Join(sandbox, "tether")

	build := exec.Command("go", "build", "-o", dst, ".")
	if out, err := build.CombinedOutput(); err != nil {
		t.Fatalf("go build: %v\n%s", err, out)
	}

	// The prev slot: the same binary with one appended byte — a different
	// sha that still executes (trailing bytes are ignored by the loader).
	dstBytes, err := os.ReadFile(dst)
	if err != nil {
		t.Fatal(err)
	}
	prev := dst + ".prev"
	if err := os.WriteFile(prev, append(append([]byte{}, dstBytes...), '\n'), 0o755); err != nil {
		t.Fatal(err)
	}
	shaOf := func(b []byte) string { s := sha256.Sum256(b); return hex.EncodeToString(s[:]) }
	newSHA := shaOf(dstBytes)
	prevBytes, _ := os.ReadFile(prev)
	prevSHA := shaOf(prevBytes)

	marker := filepath.Join(sandbox, ".tether-upgrade.json")
	pending := fmt.Sprintf(`{"state":"pending","prev_sha":%q,"new_sha":%q,`+
		`"prev_version":"v0.0.1","new_version":"v0.0.2","deadline":%q,"boot_count":0,"boot_budget":3,`+
		`"target_sid":"lab","target_nid":"n1"}`,
		prevSHA, newSHA, time.Now().UTC().Add(time.Hour).Format(time.RFC3339))
	if err := os.WriteFile(marker, []byte(pending), 0o644); err != nil {
		t.Fatal(err)
	}

	readMarker := func() (state string, bootCount int) {
		t.Helper()
		raw, err := os.ReadFile(marker)
		if err != nil {
			t.Fatalf("marker unreadable: %v", err)
		}
		var m struct {
			State     string `json:"state"`
			BootCount int    `json:"boot_count"`
		}
		if err := json.Unmarshal(raw, &m); err != nil {
			t.Fatalf("marker corrupt: %v (%s)", err, raw)
		}
		return m.State, m.BootCount
	}

	// Three doomed launches: Cobra rejects the flag long before RunE, which
	// is exactly the pre-RunE failure class the shim must survive. Budget
	// must tick every time.
	for i := 1; i <= 3; i++ {
		cmd := exec.Command(dst, "agent", "--flag-that-does-not-exist")
		if err := cmd.Run(); err == nil {
			t.Fatalf("launch %d: a bogus flag must fail the process", i)
		}
		state, count := readMarker()
		if state != "pending" || count != i {
			t.Fatalf("after doomed launch %d: marker %s/boot_count=%d, want pending/%d — "+
				"a pre-RunE failure did not consume boot budget", i, state, count, i)
		}
	}

	// Fourth launch: budget exhausted — the shim must restore prev over dst
	// (and then exec it; the restored binary fails on the same bogus flag,
	// which is fine — the DISK must have converged).
	cmd := exec.Command(dst, "agent", "--flag-that-does-not-exist")
	_ = cmd.Run() // non-zero either way
	state, _ := readMarker()
	if state != "rolled_back" {
		t.Fatalf("after budget exhaustion: marker state %q, want rolled_back", state)
	}
	restored, err := os.ReadFile(dst)
	if err != nil {
		t.Fatal(err)
	}
	if shaOf(restored) != prevSHA {
		t.Fatalf("dst was not restored to the prev slot after budget exhaustion")
	}
}

// origin: upgrade-safety external re-review F10 — the pre-Cobra recognizer
// must honor the bool flag forms Cobra accepts. In particular, service
// installation/removal and help requests are NOT daemon boots and must never
// consume a pending upgrade's boot budget merely because the operator used
// `--flag=true` instead of the bare flag.
func TestIsAgentDaemonInvocationBooleanForms(t *testing.T) {
	tests := []struct {
		name string
		argv []string
		want bool
	}{
		{"daemon", []string{"tether", "agent", "--session", "lab"}, true},
		{"unknown_flag_still_daemon", []string{"tether", "agent", "--bad"}, true},
		{"bare_install", []string{"tether", "agent", "--install-user-service"}, false},
		{"true_install", []string{"tether", "agent", "--install-user-service=true"}, false},
		{"false_install", []string{"tether", "agent", "--install-user-service=false"}, true},
		{"true_uninstall", []string{"tether", "agent", "--uninstall=true"}, false},
		{"bare_help", []string{"tether", "agent", "--help"}, false},
		{"true_help", []string{"tether", "agent", "--help=true"}, false},
		{"false_help", []string{"tether", "agent", "--help=false"}, true},
		{"help_subcommand", []string{"tether", "agent", "help"}, false},
		{"version", []string{"tether", "version"}, false},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if got := isAgentDaemonInvocation(tc.argv); got != tc.want {
				t.Fatalf("isAgentDaemonInvocation(%q) = %v, want %v", tc.argv, got, tc.want)
			}
		})
	}
}
