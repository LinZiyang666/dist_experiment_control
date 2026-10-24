//go:build linux

package cli_e2e_test

import (
	"net/http/httptest"
	"os"
	"testing"
)

// These are S3/S4 image-mechanism controls, separate from the genuine adjacent
// release quadrants. S8 §7.2 deliberately forbids their legacy live-upgrade
// route. Start the entire historical control stack before business begins;
// changing a capable broker's policy would reopen the forbidden bypass.
// The snapshot contains the completed PTY implementation and predates S8.
// origin: hot-update S8 plan §6.1 / §7.2 / §9.4; S8 internal review upgrade-maketest-2 F2
const ptyPreTransactionSnapshot = "e0a218b1ee2b1b372933d653ce8be5210af7e472"

var ptyHistoricalImages = map[bool]string{}

func historicalPTYImage(t *testing.T, candidate bool) string {
	t.Helper()
	if path := ptyHistoricalImages[candidate]; path != "" {
		return path
	}
	label, version := "pty-pretransaction", "v0.0.0-pty-historical"
	if candidate {
		label, version = "pty-pretransaction-next", "v0.0.0-pty-historical-next"
	}
	path := buildHistoricalPTYBinary(t, ptyPreTransactionSnapshot, label, version)
	t.Logf("historical PTY mechanism: commit=%s binary_sha256=%s candidate=%v", ptyPreTransactionSnapshot, upgradeFileSHA(t, path), candidate)
	ptyHistoricalImages[candidate] = path
	return path
}

func newHistoricalPTYUpgradeStack(t *testing.T, srv *httptest.Server, configure ...func(*realStack)) *realStack {
	t.Helper()
	return newUpgradeStack(t, srv, func(s *realStack) {
		s.brokerBin, s.diskDB, s.callout = historicalPTYImage(t, false), true, true
		if err := os.WriteFile(s.agentBin, mustReadFile(t, s.brokerBin), 0o755); err != nil {
			t.Fatal(err)
		}
		for _, setup := range configure {
			setup(s)
		}
	})
}
