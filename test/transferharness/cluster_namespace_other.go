//go:build !linux

package transferharness

import "testing"

// Other kernels require explicitly configured loopback aliases. Their cold
// infrastructure tests retain the ordinary loopback address; real-binary
// topology/custody and port-isolation receipts belong to the Linux tagged lane.
func clusterListenerHost(t *testing.T) string {
	t.Helper()
	return "127.0.0.1"
}
