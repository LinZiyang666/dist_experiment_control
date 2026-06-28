package natsconf

import (
	"strings"
	"testing"

	"github.com/LinZiyang666/tether/internal/natscluster"
)

// c3_harvest_test.go (C3 Stage-C fixes B1/B3/M1) — the SUCCESS path the original C3 never exercised:
// BuildMergedConf must harvest the routes-mTLS identity + the http monitor from the LIVE conf so the
// reconciler (which carries no cert paths) renders a COMPLETE conf, and Preflight must fail-closed on
// an unrecognized cluster/authorization subkey (auth_callout.xkey) now that the block is auto-regenerated.

// a post-cutover cluster broker conf (the shape the reconciler reconciles against).
const c3LiveConf = `server_name: "brk-a"
host: "0.0.0.0"
port: 4222
http: "127.0.0.1:8223"
jetstream {
  store_dir: "/var/lib/tether/js"
}
cluster {
  name: "tether"
  listen: "0.0.0.0:6222"
  routes: [ "nats://10.0.0.2:6222" ]
  tls {
    ca_file: "/etc/tether/secrets/cluster-ca.pem"
    cert_file: "/etc/tether/secrets/route-cert.pem"
    key_file: "/etc/tether/secrets/route-key.pem"
    verify: true
  }
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

func TestBuildMergedConfHarvestsClusterTLSAndMonitor(t *testing.T) {
	own, err := Preflight(writeConf(t, c3LiveConf))
	if err != nil {
		t.Fatalf("a well-formed cluster conf must pass Preflight: %v", err)
	}
	// The reconciler builds a cfg with NO cluster TLS / listen / name / monitor — exactly the case B1
	// said could never render. BuildMergedConf must harvest them from `own` and succeed.
	cfg := natscluster.Config{
		Local: natscluster.Broker{ServerName: "brk-a", NkeyPub: "UBUSNKEYA", RouteURL: "nats://10.0.0.1:6222"},
		// self + a REAL peer so the clustered render carries a route (a lone-self clustered conf is
		// unbootable and now refused by natscluster.Render — audit D).
		Peers: []natscluster.Broker{
			{ServerName: "brk-a", NkeyPub: "UBUSNKEYA", RouteURL: "nats://10.0.0.1:6222"},
			{ServerName: "brk-b", NkeyPub: "UBUSNKEYB", RouteURL: "nats://10.0.0.2:6222"},
		},
		AccountIssuer: "ABROKERACCOUNTPUB",
		JSStoreDir:    own.JSStoreDir(),
		ClientListen:  own.ClientListen(),
	}
	merged, err := BuildMergedConf(own, cfg)
	if err != nil {
		t.Fatalf("BuildMergedConf must harvest cluster mTLS from the live conf and render (B1): %v", err)
	}
	for _, want := range []string{"cluster-ca.pem", "route-cert.pem", "route-key.pem", `http: "127.0.0.1:8223"`} {
		if !strings.Contains(merged, want) {
			t.Fatalf("merged conf must preserve the harvested %q (B1/B3):\n%s", want, merged)
		}
	}
}

func TestBuildMergedConfFailsClosedWithoutClusterTLS(t *testing.T) {
	// A conf with a cluster{} block but NO tls{} — the reconciler must NOT render a conf that drops
	// routes mTLS; ClusterMTLS fails closed.
	noTLS := strings.Replace(c3LiveConf,
		`  tls {
    ca_file: "/etc/tether/secrets/cluster-ca.pem"
    cert_file: "/etc/tether/secrets/route-cert.pem"
    key_file: "/etc/tether/secrets/route-key.pem"
    verify: true
  }
`, "", 1)
	own, err := Preflight(writeConf(t, noTLS))
	if err != nil {
		t.Fatalf("preflight: %v", err)
	}
	cfg := natscluster.Config{
		Local:         natscluster.Broker{ServerName: "brk-a", NkeyPub: "UBUSNKEYA", RouteURL: "nats://10.0.0.1:6222"},
		Peers:         []natscluster.Broker{{ServerName: "brk-a", NkeyPub: "UBUSNKEYA", RouteURL: "nats://10.0.0.1:6222"}},
		AccountIssuer: "ABROKERACCOUNTPUB",
	}
	if _, err := BuildMergedConf(own, cfg); err == nil {
		t.Fatal("BuildMergedConf must FAIL CLOSED when the live conf has no cluster routes mTLS to harvest")
	}
}

func TestPreflightFailsClosedOnAuthCalloutXkey(t *testing.T) {
	// auth_callout.xkey is NOT re-emitted by Render → an auto-reconcile would silently drop it
	// (breaking callout decryption). M1: Preflight must refuse it fail-closed.
	withXkey := strings.Replace(c3LiveConf,
		`    auth_users: [ "UBUSNKEYA" ]`,
		`    auth_users: [ "UBUSNKEYA" ]`+"\n"+`    xkey: "XSECRET"`, 1)
	if _, err := Preflight(writeConf(t, withXkey)); err == nil {
		t.Fatal("Preflight must FAIL CLOSED on authorization.auth_callout.xkey (M1) — it is auto-regenerated and would be dropped")
	}
}

func TestPreflightFailsClosedOnUnknownClusterSubkey(t *testing.T) {
	withNoAdvertise := strings.Replace(c3LiveConf,
		`  name: "tether"`, `  name: "tether"`+"\n"+`  no_advertise: true`, 1)
	if _, err := Preflight(writeConf(t, withNoAdvertise)); err == nil {
		t.Fatal("Preflight must FAIL CLOSED on an unrecognized cluster.* subkey (M1)")
	}
}
