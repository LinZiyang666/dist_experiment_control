package agent

import (
	"context"
	"strings"
	"testing"

	"github.com/LinZiyang666/tether/internal/cli"
)

// TestConnectNATS_ProxyWiredBothBranches proves connectNATS injects the proxy
// options for BOTH branches of buildConnOptions (anonymous Identity==nil, and
// nkey). A set-but-unsupported proxy env makes connectNATS fail closed with the
// proxydial error BEFORE any network dial (and before the retry loop) — proving
// the single connectNATS seam covers both of buildConnOptions' returns.
//
// The agent is the unattended role on remote NAT'd hosts: a silently-dropped or
// half-wired proxy option here is a permanent hang with no operator present,
// which is exactly the failure proxy-aware-dial exists to prevent.
func TestConnectNATS_ProxyWiredBothBranches(t *testing.T) {
	t.Setenv("HTTPS_PROXY", "ftp://unsupported-proxy:21") // unsupported scheme -> fail closed
	for _, withID := range []bool{false, true} {
		a := &Agent{cfg: Config{SID: "lab", NID: "lab-1"}}
		if withID {
			a.cfg.Identity = &cli.Identity{PublicKey: "UTESTPUB", Seed: []byte("unused-no-dial")}
		}
		_, err := a.connectNATS(context.Background())
		if err == nil {
			t.Errorf("withID=%v: expected a fail-closed proxydial error", withID)
			continue
		}
		if !strings.Contains(err.Error(), "proxydial") {
			t.Errorf("withID=%v: expected the proxydial error, got: %v", withID, err)
		}
	}
}
