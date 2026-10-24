//go:build !linux

package main

import "testing"

// These command fixtures require the Linux broker image implementation and
// independently verify /proc executable bytes and kernel process identity.
// origin: hot-update S8 plan §9.4 / L21
func signedRollBuild(t *testing.T, _ ...bool) string {
	t.Helper()
	t.Skip("signed roll process fixtures require Linux image handoff")
	return ""
}
func exerciseSignedAgentlessRoll(t *testing.T, _ string) {
	t.Helper()
	t.Skip("Linux broker image handoff required")
}
func exerciseSignedMixedRoll(t *testing.T, _ string) {
	t.Helper()
	t.Skip("Linux broker image handoff required")
}
func exerciseSignedPlanGovernedRoll(t *testing.T, _ string) {
	t.Helper()
	t.Skip("Linux broker image handoff required")
}
func exerciseSignedBrokerDaemonRoll(t *testing.T, _ string) {
	t.Helper()
	t.Skip("Linux broker image handoff required")
}
func exerciseSignedRollKeeper(t *testing.T, _ string) {
	t.Helper()
	t.Skip("Linux broker image handoff required")
}
