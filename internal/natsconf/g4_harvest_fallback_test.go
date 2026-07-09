package natsconf

import (
	"strings"
	"testing"

	"github.com/LinZiyang666/tether/internal/natscluster"
)

// g4_harvest_fallback_test.go (G4 #3) — the FIRST standalone→clustered grow renders clustered over a live
// conf that still has NO cluster{} block, so ClusterMTLS() has nothing to harvest and BuildMergedConf
// hard-fails today. When the caller (the reconciler) supplies the routes-mTLS identity explicitly (from the
// secrets dir), BuildMergedConf must SKIP the harvest and render a complete clustered conf.

// a standalone (post-init, pre-grow) broker conf: jetstream + auth_callout, NO cluster{} block.
const g4StandaloneConf = `server_name: "brk-a"
host: "0.0.0.0"
port: 4222
http: "127.0.0.1:8223"
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

func TestBuildMergedConfStandaloneHasNothingToHarvest(t *testing.T) {
	own, err := Preflight(writeConf(t, g4StandaloneConf))
	if err != nil {
		t.Fatalf("standalone conf must pass Preflight: %v", err)
	}
	if !own.IsStandaloneJetStream() {
		t.Fatal("fixture must be standalone JetStream (no cluster{} block)")
	}
	// Rendering CLUSTERED (2 peers) with NO explicit mTLS → BuildMergedConf tries to harvest a cluster{}
	// block that does not exist → the #3 hard-fail. This is the pre-G4 behavior the fallback fixes.
	cfg := natscluster.Config{
		Local: natscluster.Broker{ServerName: "brk-a", NkeyPub: "UBUSNKEYA", RouteURL: "nats://10.0.0.1:6222"},
		Peers: []natscluster.Broker{
			{ServerName: "brk-a", NkeyPub: "UBUSNKEYA", RouteURL: "nats://10.0.0.1:6222"},
			{ServerName: "brk-b", NkeyPub: "UBUSNKEYB", RouteURL: "nats://10.0.0.2:6222"},
		},
		AccountIssuer: "ABROKERACCOUNTPUB",
		JSStoreDir:    own.JSStoreDir(),
		ClientListen:  own.ClientListen(),
	}
	if _, err := BuildMergedConf(own, cfg); err == nil {
		t.Fatal("a standalone conf has no cluster{} block to harvest — BuildMergedConf must fail closed without an explicit mTLS fallback (#3)")
	}
}

func TestBuildMergedConfSecretsDirFallbackRendersFirstGrow(t *testing.T) {
	own, err := Preflight(writeConf(t, g4StandaloneConf))
	if err != nil {
		t.Fatalf("preflight: %v", err)
	}
	// G4 #3: the reconciler supplies CA/cert/key + listen from the secrets dir → BuildMergedConf SKIPS the
	// harvest (BuildMergedConf:67) and renders a complete clustered conf, so the first grow can proceed.
	cfg := natscluster.Config{
		Local: natscluster.Broker{ServerName: "brk-a", NkeyPub: "UBUSNKEYA", RouteURL: "nats://10.0.0.1:6222"},
		Peers: []natscluster.Broker{
			{ServerName: "brk-a", NkeyPub: "UBUSNKEYA", RouteURL: "nats://10.0.0.1:6222"},
			{ServerName: "brk-b", NkeyPub: "UBUSNKEYB", RouteURL: "nats://10.0.0.2:6222"},
		},
		AccountIssuer: "ABROKERACCOUNTPUB",
		JSStoreDir:    own.JSStoreDir(),
		ClientListen:  own.ClientListen(),
		CAFile:        "/etc/tether/secrets/cluster-ca.pem",
		CertFile:      "/etc/tether/secrets/route-cert.pem",
		KeyFile:       "/etc/tether/secrets/route-key.pem",
		ClusterListen: "0.0.0.0:6222",
	}
	merged, err := BuildMergedConf(own, cfg)
	if err != nil {
		t.Fatalf("#3: BuildMergedConf must render the first grow via the secrets-dir mTLS fallback: %v", err)
	}
	for _, want := range []string{"cluster {", "cluster-ca.pem", "route-cert.pem", "route-key.pem", `nats://10.0.0.2:6222`} {
		if !strings.Contains(merged, want) {
			t.Fatalf("fallback-rendered clustered conf must contain %q:\n%s", want, merged)
		}
	}
	// The cluster name defaults to "tether" (matches an already-clustered peer's harvested name → no split-mesh).
	if !strings.Contains(merged, `name: "tether"`) {
		t.Fatalf("fallback render must default cluster name to \"tether\":\n%s", merged)
	}
}
