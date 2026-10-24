package upgraderoll

import (
	"testing"

	"github.com/LinZiyang666/tether/internal/upgradetx"
)

// origin: S8 internal review remaining-orchestration F5 (R5)
func TestRollKeysFollowTheFrozenComponentContract(t *testing.T) {
	for _, key := range []upgradetx.Key{
		{DomainID: "domain", Component: upgradetx.ComponentAgent, SID: "lab", ConfiguredNID: "node"},
		{DomainID: "domain", Component: upgradetx.ComponentBroker, ConfigPath: "/etc/broker.yaml"},
	} {
		if err := upgradetx.ValidateKey(key); err != nil {
			t.Fatal(err)
		}
		if !validKey(key) {
			t.Fatalf("roll rejects a P00-valid installation key: %+v", key)
		}
	}
}
