package architecture

import (
	"strconv"
	"testing"
)

// A responsibility's receipt leaf belongs to that owner on every build graph.
// Its imports still participate in the owner's complete transitive closure.
// origin: S7 internal review GATES-PROD F2 / S7 GATE-CONVERGE
func TestAllOSReceiptLeavesPreserveRuntimeBans(t *testing.T) {
	root := transferFixtureTree(t, map[string]string{
		"internal/drillobserve/owner_other.go":   "package drillobserve;import _ " + strconv.Quote(modPrefix+"internal/drillobserve/receipt"),
		"internal/drillobserve/receipt/owner.go": "package receipt;import _ \"encoding/json\"",
	})
	if err := checkBrokerAllOSLayering(brokerGateSources(t, root)); err != nil {
		t.Fatalf("owned receipt leaf rejected: %v", err)
	}
	for _, target := range []string{"internal/broker", "internal/agent", "internal/cli", "internal/foreign/receipt"} {
		transferWriteFixture(t, root, "internal/drillobserve/receipt/owner.go", "package receipt;import _ "+strconv.Quote(modPrefix+target))
		if checkBrokerAllOSLayering(brokerGateSources(t, root)) == nil {
			t.Fatalf("owned receipt hid forbidden runtime %s", target)
		}
	}
}
