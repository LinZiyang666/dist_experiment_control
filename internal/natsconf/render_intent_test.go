package natsconf

import (
	"strings"
	"testing"
)

// render_intent_test.go — the four RenderIntents must be four, and the table below is why (B5, plan T4).
//
// The obvious enumeration is preserve / force-standalone / force-clustered. It is wrong, and the case
// that proves it is the manual takeover's `Standalone: len(peers) == 1`. Collapsing that into either
// neighbour produces a real regression on a real path, so this table renders EVERY intent against the
// inputs that distinguish them and asserts the mode each one picks.
//
// The three rows the plan named specifically:
//
//	(i)   standalone live conf + 1 peer   — the settled N=1 shape
//	(ii)  CLUSTERED live conf + 1 peer    — the discriminating case
//	(iii) clustered live conf + 3 peers   — the ordinary voter
//
// Row (ii) is the whole argument. Under IntentPreserve it must stay CLUSTERED (the autonomous loop may
// not cross the destructive boundary); under IntentStandaloneIfLone it must render STANDALONE (that is
// what the operator asked for by supplying a lone peer set). Two intents, opposite answers, same inputs
// — which is exactly what a single "standalone if lone" rule could not express.
func TestRenderIntentResolvesTheModeItPromises(t *testing.T) {
	self := Broker{ServerName: "brk-a", NkeyPub: "UBUSNKEYA", RouteURL: "nats://10.0.0.1:6222"}
	peer := Broker{ServerName: "brk-b", NkeyPub: "UBUSNKEYB", RouteURL: "nats://10.0.0.2:6222"}

	lone := Inputs{SelfServerName: "brk-a", Peers: []Broker{self}, AccountIssuer: "ABROKERACCOUNTPUB"}
	mesh := Inputs{SelfServerName: "brk-a", Peers: []Broker{self, peer}, AccountIssuer: "ABROKERACCOUNTPUB"}

	standaloneOwn := ownFrom(t, `server_name: "brk-a"
host: "0.0.0.0"
port: 4222
jetstream {
  store_dir: "/var/lib/tether/js"
}
`)
	clusteredOwn := ownFrom(t, `server_name: "brk-a"
host: "0.0.0.0"
port: 4222
jetstream {
  store_dir: "/var/lib/tether/js"
}
cluster {
  name: "tether"
  listen: "0.0.0.0:6222"
  routes: [
    "nats://10.0.0.2:6222"
  ]
  tls {
    ca_file: "/etc/tether/secrets/cluster-ca.pem"
    cert_file: "/etc/tether/secrets/route-cert.pem"
    key_file: "/etc/tether/secrets/route-key.pem"
    verify: true
  }
}
`)

	cases := []struct {
		name           string
		intent         RenderIntent
		in             Inputs
		own            *Ownership
		wantStandalone bool
		why            string
	}{
		// (i) settled N=1.
		{"preserve/standalone-conf/lone", IntentPreserve, lone, standaloneOwn, true,
			"the post---to-standalone shape must stay converged across restarts and gen bumps (review F4)"},

		// (ii) THE DISCRIMINATING ROW — same inputs, opposite answers.
		{"preserve/CLUSTERED-conf/lone", IntentPreserve, lone, clusteredOwn, false,
			"a still-clustered N=1 must NOT be silently de-clustered by the autonomous loop: that bypasses " +
				"--confirm-single, the backup warning, the full restart and the clustered->standalone JS reset (R3)"},
		{"standalone-if-lone/CLUSTERED-conf/lone", IntentStandaloneIfLone, lone, clusteredOwn, true,
			"the operator supplied a lone peer set to a manual takeover — that IS the de-cluster request, " +
				"and refusing it here would break `reconcile nats --manual` on a shrinking cluster"},

		// (iii) ordinary voter.
		{"preserve/clustered-conf/mesh", IntentPreserve, mesh, clusteredOwn, false, "a voter stays clustered"},
		{"standalone-if-lone/clustered-conf/mesh", IntentStandaloneIfLone, mesh, clusteredOwn, false,
			"more than one peer means the takeover is renderng a real mesh, not a de-cluster"},

		// The two unconditional intents must ignore both the roster and the live conf — that is what
		// makes them usable on a transition where inference would read the PRE-transition state.
		{"force-clustered/standalone-conf/mesh", IntentForceClustered, mesh, standaloneOwn, false,
			"the grow cutover runs ON the standalone->clustered transition, so inferring from the live " +
				"conf would render exactly the wrong mode"},
		{"force-standalone/clustered-conf/mesh", IntentForceStandalone, mesh, clusteredOwn, true,
			"--to-standalone means standalone even with peers still in the roster"},
		{"force-standalone/standalone-conf/lone", IntentForceStandalone, lone, standaloneOwn, true,
			"idempotent"},
		{"force-clustered/clustered-conf/mesh", IntentForceClustered, mesh, clusteredOwn, false, "idempotent"},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := tc.intent.standalone(tc.in, tc.own); got != tc.wantStandalone {
				t.Fatalf("intent %s resolved standalone=%v, want %v.\nwhy it matters: %s",
					tc.intent, got, tc.wantStandalone, tc.why)
			}
		})
	}
}

// TestPreserveAndStandaloneIfLoneDisagreeOnAClusteredLoneNode is the collapse test stated as its own
// assertion, so the four-intent enumeration cannot be quietly reduced to three.
//
// If someone folds IntentStandaloneIfLone into IntentPreserve, this fails; if they fold it into
// IntentForceStandalone, TestRenderIntentResolvesTheModeItPromises's mesh row fails. Between them the two
// neighbours are both closed off.
func TestPreserveAndStandaloneIfLoneDisagreeOnAClusteredLoneNode(t *testing.T) {
	self := Broker{ServerName: "brk-a", NkeyPub: "UBUSNKEYA", RouteURL: "nats://10.0.0.1:6222"}
	lone := Inputs{SelfServerName: "brk-a", Peers: []Broker{self}, AccountIssuer: "ABROKERACCOUNTPUB"}
	clustered := ownFrom(t, `server_name: "brk-a"
host: "0.0.0.0"
port: 4222
jetstream {
  store_dir: "/var/lib/tether/js"
}
cluster {
  name: "tether"
  listen: "0.0.0.0:6222"
  routes: [
    "nats://10.0.0.2:6222"
  ]
  tls {
    ca_file: "/etc/tether/secrets/cluster-ca.pem"
    cert_file: "/etc/tether/secrets/route-cert.pem"
    key_file: "/etc/tether/secrets/route-key.pem"
    verify: true
  }
}
`)

	preserve := IntentPreserve.standalone(lone, clustered)
	ifLone := IntentStandaloneIfLone.standalone(lone, clustered)
	if preserve == ifLone {
		t.Fatalf("IntentPreserve and IntentStandaloneIfLone agree (%v) on a CLUSTERED lone node — then one "+
			"of them is redundant and the four-intent enumeration is over-specified. They must not: the "+
			"autonomous loop has to keep it clustered, and an operator's lone-peer takeover has to "+
			"de-cluster it. Same inputs, opposite correct answers.", preserve)
	}
}

// TestRenderDesiredForcesTheMonitorOnlyWhenAsked pins the other half of RenderOverride: a caller that
// says nothing about the monitor gets the LIVE conf's, because nats-server rejects an http-port add on
// SIGHUP and only a restart-bearing path may establish one.
func TestRenderDesiredForcesTheMonitorOnlyWhenAsked(t *testing.T) {
	self := Broker{ServerName: "brk-a", NkeyPub: "UBUSNKEYA", RouteURL: "nats://10.0.0.1:6222"}
	lone := Inputs{SelfServerName: "brk-a", Peers: []Broker{self}, AccountIssuer: "ABROKERACCOUNTPUB"}
	own := ownFrom(t, `server_name: "brk-a"
host: "0.0.0.0"
port: 4222
http: "127.0.0.1:9999"
jetstream {
  store_dir: "/var/lib/tether/js"
}
`)

	harvested, err := RenderDesired(lone, own, RenderOverride{Intent: IntentPreserve})
	if err != nil {
		t.Fatalf("harvested render: %v", err)
	}
	if !strings.Contains(harvested, `http: "127.0.0.1:9999"`) {
		t.Errorf("with no MonitorListen override the live conf's http block must be PRESERVED — the "+
			"reconciler is SIGHUP-only and cannot establish a new one:\n%s", harvested)
	}

	forced, err := RenderDesired(lone, own, RenderOverride{
		Intent: IntentForceStandalone, MonitorListen: "127.0.0.1:8223",
	})
	if err != nil {
		t.Fatalf("forced render: %v", err)
	}
	if !strings.Contains(forced, `http: "127.0.0.1:8223"`) {
		t.Errorf("an explicit MonitorListen must WIN over the harvest — the restart-bearing takeover is "+
			"the only thing that can establish the address the post-restart probe hits:\n%s", forced)
	}
}

func ownFrom(t *testing.T, body string) *Ownership {
	t.Helper()
	own, err := Preflight(writeConf(t, body))
	if err != nil {
		t.Fatalf("fixture must preflight clean: %v", err)
	}
	return own
}
