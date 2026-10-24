//go:build linux && amd64

// Copy into cmd/tether-infra-launch. No process or host effects.
package main

import (
	"strings"
	"testing"

	c "github.com/LinZiyang666/tether/internal/infracontract"
	"github.com/LinZiyang666/tether/internal/infraunit"
)

// origin: S9 privilege review — generated unit must invoke the real parser.
// origin: S9 internal review rv-priv F5
func TestManagedUnitLauncherArgumentsAccepted(t *testing.T) {
	for _, component := range []c.Component{c.NATS, c.Caddy} {
		body, err := infraunit.ManagedUnitDropIn(component)
		if err != nil {
			t.Fatal(err)
		}
		found := false
		for _, line := range strings.Split(string(body), "\n") {
			if !strings.HasPrefix(line, "ExecStart=/") {
				continue
			}
			found = true
			argv := strings.Fields(strings.TrimPrefix(line, "ExecStart="))
			got, err := parseComponent(argv[1:])
			if err != nil || got != component {
				t.Errorf("generated unit argv %q rejected: got=%v err=%v", argv[1:], got, err)
			}
		}
		if !found {
			t.Fatal("missing ExecStart")
		}
	}
}
