package broker

import (
	"os"
	"strings"
	"testing"
)

// Scan the composition sites themselves: leaf tests cannot detect an omitted
// factory, a SYS source left nil, or a managed defer capturing false by value.
// origin: hot-update S7 plan §8.1 / §8.2 / T36 / T39; S9 SYS principals
func TestTransferActivationKeepsProductionBindings(t *testing.T) {
	for _, site := range []struct{ path, required string }{
		{"../../cmd/tether/serve.go", "TransferFactory: broker.ServeTransferFactory"},
		{"authcallout.go", "SystemPrincipals: brokerSystemPrincipals(b)"},
		{"xfer_inflight.go", "return xferserve.LoadedSystemPrincipals(b.cfg.NatsConfPath, b.cfg.Logger)"},
		{"broker.go", "defer stopBrokerTransferRun(b, &managed, &runErr)"},
	} {
		body, err := os.ReadFile(site.path)
		if err != nil {
			t.Fatal(err)
		}
		if !strings.Contains(strings.Join(strings.Fields(string(body)), " "), site.required) {
			t.Errorf("%s: production binding missing: %s", site.path, site.required)
		}
	}
}
