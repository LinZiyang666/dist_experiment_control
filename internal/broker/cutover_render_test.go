package broker

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/LinZiyang666/tether/internal/auth"
	"github.com/LinZiyang666/tether/internal/natsconf"
)

// cutover_render_test.go — CHARACTERIZATION of renderClusteredCutoverConf (batch-B plan §3 row P2).
//
// WHY IT EXISTS, AND WHY IT IS LATE
// ---------------------------------
// The plan promised this file and it was not written. The mutation-honesty audit found the gap by
// grepping: `renderClusteredCutoverConf` had NO direct test caller anywhere in the repo, and the
// function's own doc comment claimed it was "Pinned by TestCutoverRenderMatchesReconcilerRenderExceptTheMonitor"
// — which is false. That test builds both Configs from a local fixture inside itself and never reaches
// production assembly code at all; injecting the plan's own stated mutation (deleting the forced
// MonitorListen from THIS function) left it green.
//
// So the one restart-bearing render on the standalone->clustered path was covered only transitively,
// through natsconf's own tests, with nothing pinning what THIS function feeds it.
//
// THE INPUT THE PLAN NAMED: a live conf with NO `http:` LINE
// ----------------------------------------------------------
// That is the case that matters, and it is not hypothetical — install.sh wrote confs without a monitor
// for most of this project's life. nats-server cannot hot-add an http port on SIGHUP, so the
// per-broker reconciler can only ever PRESERVE a monitor, never establish one; the cutover is the one
// step that restarts, so it is the one step that can. And restartAndVerifyClustered probes exactly
// topoMonitorListen afterwards — if this render harvested the (absent) http block instead of forcing
// the address, a perfectly healthy revival would be reported as cutover_revival_failed after a 45s
// connection-refused, and the operator would be told the grow failed when it succeeded.
func TestCutoverRenderForcesTheMonitorOntoAConfThatHasNone(t *testing.T) {
	b, own := newCutoverRenderFixture(t, confWithoutMonitor)

	merged, err := b.renderClusteredCutoverConf(own)
	if err != nil {
		t.Fatalf("render: %v", err)
	}

	// (1) THE LOAD-BEARING ONE. The live conf has no http: at all, so this address can only have come
	// from the forced override.
	if strings.Contains(confWithoutMonitor, "http:") {
		t.Fatal("the fixture must NOT contain an http: line, or this test proves nothing about forcing")
	}
	if !strings.Contains(merged, `http: "`+topoMonitorListen+`"`) {
		t.Errorf("the cutover must ESTABLISH the loopback monitor restartAndVerifyClustered probes "+
			"(%s) even when the live conf has none — otherwise a healthy revival is reported as "+
			"cutover_revival_failed after a 45s connection-refused:\n%s", topoMonitorListen, merged)
	}

	// (2) It renders CLUSTERED unconditionally. The live conf is standalone by definition on this path
	// (that is what the cutover is transitioning away from), so anything that INFERS the mode from the
	// live conf renders exactly the wrong thing here.
	if !strings.Contains(merged, "cluster {") {
		t.Errorf("the cutover renders the standalone->clustered transition; the mode may not be inferred "+
			"from the live (still standalone) conf:\n%s", merged)
	}
	// routes[] carries the PEERS and deliberately not self — a broker does not dial itself; its own
	// endpoint is the `listen` below. auth_users, by contrast, must carry EVERY broker's nkey, because
	// that list is who may connect to this server.
	if !strings.Contains(merged, `"nats://10.0.0.2:6222"`) {
		t.Errorf("the peer's route is missing — a clustered conf that omits a voter passes "+
			"`nats-server -t` and then loses that broker's routes/auth/ACLs:\n%s", merged)
	}
	if strings.Contains(merged, `"nats://10.0.0.1:6222"`) {
		t.Errorf("self's own route URL must not appear in routes[] — a self-route is a wasted dial and "+
			"nats-server logs it as a duplicate:\n%s", merged)
	}
	for _, nkey := range []string{"UBUSNKEYA", "UBUSNKEYB"} {
		if !strings.Contains(merged, nkey) {
			t.Errorf("auth_users must carry every broker's bus nkey including self's (%s); routes[] "+
				"excludes self but the auth list may not:\n%s", nkey, merged)
		}
	}

	// (3) The routes-mTLS identity comes from the secrets dir, because a standalone conf has no
	// cluster{} block to harvest it from. This is the first-grow case the shared
	// Config.ApplySecretsDirIdentity exists for.
	for _, want := range []string{"cluster-ca.pem", "route-cert.pem", "route-key.pem"} {
		if !strings.Contains(merged, filepath.Join(b.cfg.ClusterSecretsDir, want)) {
			t.Errorf("routes mTLS %s must be taken from the secrets dir on a first grow (there is no "+
				"cluster{} block to harvest from):\n%s", want, merged)
		}
	}

	// (4) The route listen is SYNTHESIZED from self's own route URL, not defaulted. Self is
	// nats://10.0.0.1:6222, so the listen is 0.0.0.0:6222 — and if self's route port were 7222 the
	// listen would have to follow it, which is the property the next test pins.
	if !strings.Contains(merged, `listen: "0.0.0.0:6222"`) {
		t.Errorf("the route listen must be derived from self's route URL:\n%s", merged)
	}

	// (5) JetStream's store dir is PRESERVED from the live conf. Rendering a different one would point
	// the revived clustered server at an empty directory while the real data sits next to it — the
	// silent-data-loss shape that natsconf's fail-closed store_dir refusal exists for.
	if !strings.Contains(merged, `store_dir: "/var/lib/tether/js"`) {
		t.Errorf("the live conf's JetStream store_dir must be preserved:\n%s", merged)
	}
}

// TestCutoverRenderFollowsSelfsRoutePortIntoTheListen is the discriminating half of assertion (4)
// above: with self on a NON-default route port, a defaulted listen and a derived listen give different
// answers, so this is what proves the derivation is real rather than a coincidence of 6222.
func TestCutoverRenderFollowsSelfsRoutePortIntoTheListen(t *testing.T) {
	b, own := newCutoverRenderFixture(t, confWithoutMonitor)
	if _, err := b.cfg.DB.Exec(
		`UPDATE cluster_nodes SET nats_route='nats://10.0.0.1:7222' WHERE node_id='brk-a'`); err != nil {
		t.Fatal(err)
	}

	merged, err := b.renderClusteredCutoverConf(own)
	if err != nil {
		t.Fatalf("render: %v", err)
	}
	if !strings.Contains(merged, `listen: "0.0.0.0:7222"`) {
		t.Errorf("self advertises nats://10.0.0.1:7222, so the route listen must be 0.0.0.0:7222. A "+
			"defaulted 6222 would leave the broker listening on a port no peer dials — the mesh would "+
			"form in the conf and never form on the wire:\n%s", merged)
	}
}

// TestCutoverRenderRefusesBeforeItRenders pins the three PRE-RENDER guards. They are policy, not
// intent, and the B5 collapse deliberately left them at this call site rather than absorbing them into
// natsconf.RenderDesired — so they need their own pin, or a refactor that moves the assembly can drop
// them without anything noticing.
//
// Each refusal is a real fleet state, not a defensive stub:
//   - fewer than 2 mesh peers: the roster has not converged yet, and rendering now would produce a
//     clustered conf with no peer to route to, which nats-server accepts under `-t` and then FATALs on
//     at boot ("JetStream cluster requires configured routes");
//   - no secrets dir: there is nothing to render routes mTLS from and no cluster{} block to harvest it
//     from either, so the alternative to refusing is emitting a route listener with no TLS identity;
//   - self absent from the mesh: the roster describes a cluster this broker is not in.
func TestCutoverRenderRefusesBeforeItRenders(t *testing.T) {
	cases := []struct {
		name    string
		mutate  func(t *testing.T, b *Broker)
		wantSub string
	}{
		{
			name: "single mesh peer (roster not converged)",
			mutate: func(t *testing.T, b *Broker) {
				if _, err := b.cfg.DB.Exec(`DELETE FROM cluster_nodes WHERE node_id='brk-b'`); err != nil {
					t.Fatal(err)
				}
			},
			wantSub: "fewer than 2 mesh peers",
		},
		{
			name:    "no secrets dir",
			mutate:  func(t *testing.T, b *Broker) { b.cfg.ClusterSecretsDir = "" },
			wantSub: "no secrets dir",
		},
		{
			// A converged mesh that does NOT contain this broker. Note it takes a THIRD peer to reach:
			// simply deleting self's row leaves one peer and trips the peers<2 guard first, so the
			// only way to observe this refusal is a roster that is complete and about someone else.
			// (Renaming self's nats_server_id does NOT reach it either — that column IS the conf's
			// server_name, so renaming it just renames self, and the render is correct to follow.)
			name: "self not in the mesh",
			mutate: func(t *testing.T, b *Broker) {
				seedTopoPeer(t, b, "brk-c", "nats://10.0.0.3:6222", "UBUSNKEYC")
				if _, err := b.cfg.DB.Exec(`DELETE FROM cluster_nodes WHERE node_id='brk-a'`); err != nil {
					t.Fatal(err)
				}
			},
			wantSub: "self not present",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			b, own := newCutoverRenderFixture(t, confWithoutMonitor)
			tc.mutate(t, b)
			merged, err := b.renderClusteredCutoverConf(own)
			if err == nil {
				t.Fatalf("expected a refusal (%s); got a rendered conf, which the cutover would then "+
					"APPLY and restart into:\n%s", tc.wantSub, merged)
			}
			if !strings.Contains(err.Error(), tc.wantSub) {
				t.Errorf("error = %q, want it to mention %q — an operator reading a cutover failure needs "+
					"to know which precondition was missing", err, tc.wantSub)
			}
			if merged != "" {
				t.Errorf("a refusing render must return no conf, got %d bytes", len(merged))
			}
		})
	}
}

// confWithoutMonitor is the fixture the plan named: a standalone-JetStream conf with NO http: line.
// This is what install.sh wrote for most of this project's life, so it is the live shape a first grow
// actually finds on an older broker.
const confWithoutMonitor = `# tether nats-server config (generated by install.sh)
server_name: "brk-a"
host: "0.0.0.0"
port: 4222
jetstream {
  store_dir: "/var/lib/tether/js"
}
authorization {
  auth_callout {
    issuer: "ABROKERACCOUNTPUB"
    account: "$G"
    auth_users: [ "UBUSNKEYA" ]
  }
  users: [
    { nkey: "UBUSNKEYA" }
  ]
}
`

// newCutoverRenderFixture builds a broker whose roster is a converged 2-voter mesh with self at
// brk-a, plus the parsed live conf. b.cl is nil, so filterGhostPeers passes the roster through
// unchanged — the ghost filter has its own tests and is not what this file is about.
func newCutoverRenderFixture(t *testing.T, confText string) (*Broker, *natsconf.Ownership) {
	t.Helper()
	dir := t.TempDir()
	confPath := filepath.Join(dir, "nats.conf")
	if err := os.WriteFile(confPath, []byte(confText), 0o600); err != nil {
		t.Fatal(err)
	}
	own, err := natsconf.Preflight(confPath)
	if err != nil {
		t.Fatalf("fixture must preflight clean: %v", err)
	}

	seed, err := auth.GenerateUserSeed()
	if err != nil {
		t.Fatal(err)
	}
	b := &Broker{selfID: "brk-a"}
	b.cfg.DB = openDB(t)
	b.cfg.Logger = silentLogger()
	b.cfg.NatsConfPath = confPath
	b.cfg.ClusterSecretsDir = filepath.Join(dir, "secrets")
	b.cfg.AuthCallout = &AuthCalloutConfig{AccountSeed: seed}

	seedTopoPeer(t, b, "brk-a", "nats://10.0.0.1:6222", "UBUSNKEYA")
	seedTopoPeer(t, b, "brk-b", "nats://10.0.0.2:6222", "UBUSNKEYB")
	return b, own
}

func seedTopoPeer(t *testing.T, b *Broker, nodeID, route, busNkey string) {
	t.Helper()
	if _, err := b.cfg.DB.Exec(
		`INSERT OR REPLACE INTO cluster_nodes
		   (node_id, name, node_ident_pub, nats_server_id, raft_addr, nats_route, tunnel_addr,
		    public_host, cert_fp, phase, bus_nkey_pub, added_at)
		 VALUES (?,?,?,?,?,?,?,?,?,?,?,?)`,
		nodeID, nodeID, "Nident-"+nodeID, nodeID, "127.0.0.1:7400", route, "127.0.0.1:7443",
		nodeID+".example", "sha256:"+nodeID, "VOTER", busNkey, time.Now().UTC(),
	); err != nil {
		t.Fatalf("seed topo peer %s: %v", nodeID, err)
	}
}
