//go:build !linux

package testharness

import "testing"

// GuardProcessDirectory requires Linux procfs and pidfds for identity-safe kills.
// Fixture teardown still runs on other platforms; this measurement is explicit.
func GuardProcessDirectory(t *testing.T, dir string) string {
	t.Helper()
	t.Logf("process leak measurement unavailable without Linux procfs: %s", dir)
	return dir
}
