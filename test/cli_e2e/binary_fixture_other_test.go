//go:build !linux

package cli_e2e_test

import (
	"testing"
	"time"
)

const binaryFixtureTestBudget = 10 * time.Minute

// False keeps the complete original test body on its existing platform path.
// Only Linux fixtures use the subreaper-backed scheduling optimization.
// origin: hot-update S10 plan L17 / §11.1 / POLISH-B
func runIsolatedBinaryTest(t *testing.T, _ time.Duration) bool {
	t.Helper()
	return false
}
