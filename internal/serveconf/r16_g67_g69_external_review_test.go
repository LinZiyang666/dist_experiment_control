package serveconf

import "testing"

// TestExternalReviewRejectsUnsafeCrossHomeReapAge protects the production config
// boundary. The product derives a 15-minute floor specifically to outlive a remote
// home's 5-minute transfer watchdog plus skew/slack. Accepting the drill's 5-second
// compression in ordinary broker.yaml lets the leader delete another home's live
// tier-B object because tracker state is node-local.
func TestExternalReviewRejectsUnsafeCrossHomeReapAge(t *testing.T) {
	_, err := Load(writeConfig(t, "broker:\n  cluster:\n    xfer_cross_home_reap_age: 5s\n"))
	if err == nil {
		t.Fatal("production config accepted a 5s cross-home GC age even though a remote-home transfer may remain live for 5m")
	}
}
