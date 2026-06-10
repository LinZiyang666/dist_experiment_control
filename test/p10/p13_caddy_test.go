package p10_test

import (
	"os"
	"strings"
	"testing"
)

// P13 F9/M9: the install.sh Caddyfile must reverse-proxy the read-only
// subscription endpoint on a PATH-SCOPED handle for /sub/* that comes BEFORE
// the NATS WSS catch-all — otherwise Clash subscription fetches would hit
// nats-server and the WSS upgrade would break. This is a static content check
// because install.sh refuses --role broker on macOS.
func TestInstallShCaddySubRouteOrdering(t *testing.T) {
	b, err := os.ReadFile(scriptPath(t))
	if err != nil {
		t.Fatal(err)
	}
	s := string(b)

	subIdx := strings.Index(s, "handle /sub/*")
	if subIdx < 0 {
		t.Fatal("install.sh Caddyfile has no `handle /sub/*` route for the P13 subscription endpoint")
	}
	if !strings.Contains(s, "reverse_proxy 127.0.0.1:8090") {
		t.Fatal("the /sub route must reverse_proxy to the loopback subscription listener 127.0.0.1:8090")
	}
	// The WSS catch-all to nats-server must still exist.
	wssIdx := strings.Index(s, "reverse_proxy 127.0.0.1:8222")
	if wssIdx < 0 {
		t.Fatal("install.sh Caddyfile lost the NATS WSS catch-all (127.0.0.1:8222)")
	}
	// /sub must be declared BEFORE the catch-all so it isn't shadowed.
	if subIdx > wssIdx {
		t.Fatal("the `handle /sub/*` route must precede the WSS catch-all, or Caddy shadows it")
	}
	// broker.yaml must ship the loopback sub.listen so the endpoint is enabled.
	if !strings.Contains(s, "listen: \"127.0.0.1:8090\"") {
		t.Fatal("install.sh broker.yaml must set sub.listen to 127.0.0.1:8090")
	}
}
