//go:build linux

package transferharness

import (
	"crypto/rand"
	"fmt"
	"testing"
)

// Linux routes the entire 127/8 through loopback. Give each fixture its own
// listener address, distinct from outbound NATS clients on 127.0.0.1. The
// released reservation can then never become that client's ephemeral socket.
// origin: S7 internal review INTEG-O F2 / clustered fixture port census
func clusterListenerHost(t *testing.T) string {
	t.Helper()
	var address [3]byte
	_, err := rand.Read(address[:])
	clusterMust(t, err)
	return fmt.Sprintf("127.%d.%d.%d", 1+int(address[0])%254, address[1], 1+int(address[2])%254)
}
