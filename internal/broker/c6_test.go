package broker

import (
	"reflect"
	"strings"
	"testing"

	"github.com/LinZiyang666/tether/internal/adminsock"
)

func structFieldNames(v any) []string {
	t := reflect.TypeOf(v)
	out := make([]string, 0, t.NumField())
	for i := 0; i < t.NumField(); i++ {
		out = append(out, t.Field(i).Name)
	}
	return out
}

func containsFold(s, sub string) bool {
	return strings.Contains(strings.ToLower(s), strings.ToLower(sub))
}

// c6_test.go — C6 status-naming + homes-readiness invariants (the load-bearing pure logic).

func TestHealthLabelMatrix(t *testing.T) {
	cases := []struct {
		health string
		voters int
		want   string
	}{
		{healthHealthyHA, 3, "HEALTHY-HA"},
		{healthDegraded, 5, "DEGRADED-WRITABLE"},
		{healthDegraded, 2, "NOT-HA"}, // N<=2 promotes to the first-class NOT-HA state
		{healthDegraded, 1, "NOT-HA"},
		{healthQuorumLost, 3, "READ-ONLY"},
		{healthForceSingle, 1, "FORCE-SINGLE"},
		{"", 0, ""}, // offline snapshot with no computed health
	}
	for _, c := range cases {
		if got := healthLabel(c.health, c.voters); got != c.want {
			t.Errorf("healthLabel(%q,%d)=%q want %q", c.health, c.voters, got, c.want)
		}
	}
}

// TestNotHAFaultToleranceEquivalence proves the NOT-HA condition (DEGRADED + voters<=2) lines up with
// FaultTolerance==0, so NOT-HA reuses exit 1 (the FaultTolerance==0 branch already returns DEGRADED)
// and never collides with 0/2/3.
func TestNotHAFaultToleranceEquivalence(t *testing.T) {
	for v := 0; v <= 6; v++ {
		ft0 := ProjectQuorum(v, false).FaultTolerance == 0
		notHA := healthLabel(healthDegraded, v) == "NOT-HA"
		if ft0 != notHA {
			t.Errorf("v=%d: FaultTolerance==0 is %v but NOT-HA label is %v (must match)", v, ft0, notHA)
		}
	}
}

func TestExposeReadyReasonGatesPublicHost(t *testing.T) {
	cases := []struct {
		name                      string
		home, phase, cert, public string
		reachable                 bool
		want                      string
	}{
		{"no home", "", "", "", "", true, proxyReasonNoHome},
		{"non-voter home", "b1", "CATCHING_UP", "fp", "h", true, proxyReasonCatchingUp},
		{"no cert", "b1", "VOTER", "", "h", true, proxyReasonCatchingUp},
		{"no public_host", "b1", "VOTER", "fp", "", true, proxyReasonCatchingUp}, // gate public_host
		{"unreachable", "b1", "VOTER", "fp", "h", false, proxyReasonTunnelDown},
		{"ready", "b1", "VOTER", "fp", "h", true, proxyReasonReady},
	}
	for _, c := range cases {
		got := exposeReadyReason(c.home, c.phase, c.cert, c.public, c.reachable)
		if got != c.want {
			t.Errorf("%s: exposeReadyReason=%q want %q", c.name, got, c.want)
		}
		// Render-equivalence: a Ready node MUST have a deliverable home (public_host set).
		if proxyInSubRender(got) && c.public == "" {
			t.Errorf("%s: reason %q is render-included but public_host is empty (Ready⇒public_url violated)", c.name, got)
		}
	}
}

// TestHomeEntryHasNoSecretFields is a structural secret-free guard: HomeEntry must carry NO token/psk/
// key field (only home/epoch/url/reason). A future field add that leaks a secret trips this.
func TestHomeEntryHasNoSecretFields(t *testing.T) {
	// A populated entry the way buildHomesReport fills it — assert the JSON never carries a secret key.
	e := adminsock.HomeEntry{
		Kind: "proxy", SID: "lab", NID: "n1", Name: "__proxy__", Port: 14000,
		HomeBroker: "b1", Epoch: 2, Reconnects: 2, PublicURL: "h:14000", ReadyReason: "ready", Ready: true,
	}
	// Reflectively walk the struct fields; none may be named like a secret.
	forbidden := []string{"token", "psk", "secret", "key", "password", "seed"}
	for _, f := range structFieldNames(e) {
		for _, bad := range forbidden {
			if containsFold(f, bad) {
				t.Errorf("HomeEntry field %q looks like a secret — homes report must be secret-free", f)
			}
		}
	}
}
