package natscluster

import (
	"strings"
	"testing"
)

// TestRenderNeverEmitsMaxFileStore (G6 #21) pins that Render NEVER emits a jetstream.max_file_store subkey.
// natsconf.Preflight fail-closes on that subkey (it would brick the reconciler + the de-cluster render
// surface + rolling upgrade), so disk-aware sizing lives in the per-session bucket MaxBytes, NEVER in the
// rendered conf. This is a golden guard against a future "just render max_file_store" regression.
func TestRenderNeverEmitsMaxFileStore(t *testing.T) {
	dir := t.TempDir()
	clustered, _, _ := threeBrokerConfig(t, dir)
	standalone := clustered
	standalone.Standalone = true
	standalone.Peers = []Broker{clustered.Local}
	standalone.CAFile, standalone.CertFile, standalone.KeyFile = "", "", ""
	for _, tc := range []struct {
		name string
		cfg  Config
	}{{"clustered", clustered}, {"standalone", standalone}} {
		out, err := Render(tc.cfg)
		if err != nil {
			t.Fatalf("%s: Render: %v", tc.name, err)
		}
		if strings.Contains(out, "max_file_store") {
			t.Errorf("%s: rendered conf contains max_file_store (G6 #21 forbids it — it bricks reconcile):\n%s", tc.name, out)
		}
	}
}
