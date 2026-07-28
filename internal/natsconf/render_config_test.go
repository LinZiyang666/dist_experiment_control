package natsconf

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"math/big"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	natsserver "github.com/nats-io/nats-server/v2/server"
	"github.com/nats-io/nkeys"
)

// writeClusterCertPEMs writes an ephemeral CA + one server leaf as PEM files
// (ca.pem, cert.pem, key.pem) so server.ProcessConfigFile can actually load the
// routes-mTLS block. Returns the three paths.
func writeClusterCertPEMs(t *testing.T, dir string) (caPath, certPath, keyPath string) {
	t.Helper()
	caKey, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	caTmpl := &x509.Certificate{
		SerialNumber: big.NewInt(1), Subject: pkix.Name{CommonName: "tether-test-ca"},
		NotBefore: time.Now().Add(-time.Hour), NotAfter: time.Now().Add(24 * time.Hour),
		IsCA: true, KeyUsage: x509.KeyUsageCertSign | x509.KeyUsageDigitalSignature, BasicConstraintsValid: true,
	}
	caDER, err := x509.CreateCertificate(rand.Reader, caTmpl, caTmpl, &caKey.PublicKey, caKey)
	if err != nil {
		t.Fatal(err)
	}
	caCert, _ := x509.ParseCertificate(caDER)

	leafKey, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	leafTmpl := &x509.Certificate{
		SerialNumber: big.NewInt(2), Subject: pkix.Name{CommonName: "tether-test-node"},
		NotBefore: time.Now().Add(-time.Hour), NotAfter: time.Now().Add(24 * time.Hour),
		KeyUsage:    x509.KeyUsageDigitalSignature,
		ExtKeyUsage: []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth, x509.ExtKeyUsageClientAuth},
		DNSNames:    []string{"localhost"},
	}
	leafDER, err := x509.CreateCertificate(rand.Reader, leafTmpl, caCert, &leafKey.PublicKey, caKey)
	if err != nil {
		t.Fatal(err)
	}
	keyDER, err := x509.MarshalPKCS8PrivateKey(leafKey)
	if err != nil {
		t.Fatal(err)
	}

	caPath = filepath.Join(dir, "ca.pem")
	certPath = filepath.Join(dir, "cert.pem")
	keyPath = filepath.Join(dir, "key.pem")
	writePEM(t, caPath, "CERTIFICATE", caDER)
	writePEM(t, certPath, "CERTIFICATE", leafDER)
	writePEM(t, keyPath, "PRIVATE KEY", keyDER)
	return caPath, certPath, keyPath
}

func writePEM(t *testing.T, path, typ string, der []byte) {
	t.Helper()
	if err := os.WriteFile(path, pem.EncodeToMemory(&pem.Block{Type: typ, Bytes: der}), 0o600); err != nil {
		t.Fatal(err)
	}
}

func freshBrokerPub(t *testing.T) string {
	t.Helper()
	kp, err := nkeys.CreateUser()
	if err != nil {
		t.Fatal(err)
	}
	pub, _ := kp.PublicKey()
	kp.Wipe()
	return pub
}

func threeBrokerConfig(t *testing.T, dir string) (Config, []string, string) {
	t.Helper()
	ca, cert, key := writeClusterCertPEMs(t, dir)
	acctKp, _ := nkeys.CreateAccount()
	acctPub, _ := acctKp.PublicKey()
	acctKp.Wipe()

	pubs := []string{freshBrokerPub(t), freshBrokerPub(t), freshBrokerPub(t)}
	peers := []Broker{
		{ServerName: "tether-1", NkeyPub: pubs[0], RouteURL: "nats://127.0.0.1:6201"},
		{ServerName: "tether-2", NkeyPub: pubs[1], RouteURL: "nats://127.0.0.1:6202"},
		{ServerName: "tether-3", NkeyPub: pubs[2], RouteURL: "nats://127.0.0.1:6203"},
	}
	cfg := Config{
		// Config.Account deleted (external review B2-7): it had no writer in production, so every
		// caller — including these three fixtures — passed the one value the renderer now hardcodes.
		Local: peers[0], Peers: peers, AccountIssuer: acctPub,
		JSStoreDir:   filepath.Join(dir, "js"),
		ClientListen: "127.0.0.1:4222", ClusterName: "tether", ClusterListen: "127.0.0.1:6201",
		CAFile: ca, CertFile: cert, KeyFile: key,
	}
	return cfg, pubs, acctPub
}

// origin: internal/natscluster/config_test.go — moved into this package by B5's three-package merge
// (natsreconcile + natscluster + natsconf -> natsconf). See docs/reviews/batch-b2-plan.md §2.1.
//
// TestRenderStandaloneOmitsClusterBlock (v0.4.2 shrink) renders a STANDALONE conf — the post-shrink
// N=1 shape — and loads it with the REAL nats-server parser, asserting: NO cluster routes (a lone
// node can never reach the clustered JS meta quorum-of-2, so it runs JS standalone), but JetStream
// IS still enabled and auth_callout is intact. The reverse of the grow render.
func TestRenderStandaloneOmitsClusterBlock(t *testing.T) {
	dir := t.TempDir()
	acctKp, _ := nkeys.CreateAccount()
	acctPub, _ := acctKp.PublicKey()
	acctKp.Wipe()
	pub := freshBrokerPub(t)
	cfg := Config{
		Local:         Broker{ServerName: "tether-solo", NkeyPub: pub},
		Peers:         []Broker{{ServerName: "tether-solo", NkeyPub: pub}},
		AccountIssuer: acctPub,
		JSStoreDir:    filepath.Join(dir, "js"),
		ClientListen:  "127.0.0.1:4222",
		Standalone:    true, // the shrink mode: no cluster{} block, no routes mTLS required
	}
	confText, err := Render(cfg)
	if err != nil {
		t.Fatalf("Render(standalone): %v", err)
	}
	confPath := filepath.Join(dir, "nats.conf")
	if err := os.WriteFile(confPath, []byte(confText), 0o600); err != nil {
		t.Fatal(err)
	}
	opts, err := natsserver.ProcessConfigFile(confPath)
	if err != nil {
		t.Fatalf("standalone conf does not parse with the real nats-server parser: %v\n--- conf ---\n%s", err, confText)
	}
	// THE point: no clustered routes (a lone node runs JS standalone, not clustered).
	if opts.Cluster.Port != 0 || len(opts.Routes) != 0 {
		t.Errorf("standalone render must have NO cluster routes; got cluster.port=%d routes=%d", opts.Cluster.Port, len(opts.Routes))
	}
	// JetStream still enabled (standalone JS, just without the cluster meta).
	if !opts.JetStream {
		t.Error("standalone render must still enable JetStream")
	}
	// auth_callout intact — agents still authenticate against the lone broker.
	if opts.AuthCallout == nil || opts.AuthCallout.Issuer != acctPub {
		t.Error("standalone render must keep the auth_callout block")
	}
}

// TestRenderClusteredZeroRoutesFailsClosed (v0.4.4 review audit-D keystone): a clustered render
// (Standalone:false) whose only peer is self yields a cluster{} block with ZERO routes — a conf
// nats-server FATAL-refuses at boot ("JetStream cluster requires configured routes") YET `nats-server -t`
// PASSES, so the unbootable conf slips dry-run and bricks the broker. Render must FAIL CLOSED rather than
// emit it; a lone node must render Standalone. Exercised DIRECTLY here (the phase-fluidity test only
// reaches this transitively with NatsServerBin=/bin/true, so a refactor could silently revert the gate).
func TestRenderClusteredZeroRoutesFailsClosed(t *testing.T) {
	dir := t.TempDir()
	ca, cert, key := writeClusterCertPEMs(t, dir)
	acctKp, _ := nkeys.CreateAccount()
	acctPub, _ := acctKp.PublicKey()
	acctKp.Wipe()
	pub := freshBrokerPub(t)
	cfg := Config{
		Local:         Broker{ServerName: "tether-solo", NkeyPub: pub, RouteURL: "nats://127.0.0.1:6222"},
		Peers:         []Broker{{ServerName: "tether-solo", NkeyPub: pub, RouteURL: "nats://127.0.0.1:6222"}},
		AccountIssuer: acctPub,
		JSStoreDir:    filepath.Join(dir, "js"),
		ClientListen:  "127.0.0.1:4222", ClusterName: "tether", ClusterListen: "127.0.0.1:6222",
		CAFile: ca, CertFile: cert, KeyFile: key,
		// Standalone deliberately FALSE — the bug shape: a lone node mis-rendered clustered.
	}
	_, err := Render(cfg)
	if err == nil {
		t.Fatal("clustered render with zero routes (only self) must FAIL CLOSED, got nil — an empty-routes cluster{} bricks nats-server at boot")
	}
	if !strings.Contains(err.Error(), "ZERO routes") {
		t.Fatalf("want a 'ZERO routes' fail-closed error, got: %v", err)
	}
}

// TestD3NatsClusterRendersAllBrokerPubsEveryServer renders a 3-broker conf and
// loads it with the REAL nats-server parser (not a golden string compare),
// asserting: every broker pub is in auth_users, the shared account is the Issuer,
// every broker has a static nkey user carrying the RF1 cluster.* publish+subscribe
// grants, and the cluster block is mTLS.
func TestD3NatsClusterRendersAllBrokerPubsEveryServer(t *testing.T) {
	dir := t.TempDir()
	cfg, pubs, acctPub := threeBrokerConfig(t, dir)

	confText, err := Render(cfg)
	if err != nil {
		t.Fatalf("Render: %v", err)
	}
	confPath := filepath.Join(dir, "nats.conf")
	if err := os.WriteFile(confPath, []byte(confText), 0o600); err != nil {
		t.Fatal(err)
	}

	opts, err := natsserver.ProcessConfigFile(confPath)
	if err != nil {
		t.Fatalf("rendered conf does not parse with the real nats-server parser: %v\n--- conf ---\n%s", err, confText)
	}

	// auth_callout: shared Issuer + EVERY broker pub in auth_users.
	if opts.AuthCallout == nil {
		t.Fatal("rendered conf has no auth_callout block")
	}
	if opts.AuthCallout.Issuer != acctPub {
		t.Errorf("auth_callout issuer=%q, want shared account %q", opts.AuthCallout.Issuer, acctPub)
	}
	for _, p := range pubs {
		if !containsStr(opts.AuthCallout.AuthUsers, p) {
			t.Errorf("broker pub %q missing from auth_users %v (B cannot answer A's callout)", p, opts.AuthCallout.AuthUsers)
		}
	}
	// Cardinality (m10): exactly the roster — no duplicate / extra / leaked nkey.
	if len(opts.AuthCallout.AuthUsers) != len(pubs) {
		t.Errorf("auth_users has %d entries, want exactly %d (no extras/dupes)", len(opts.AuthCallout.AuthUsers), len(pubs))
	}
	if len(opts.Nkeys) != len(pubs) {
		t.Errorf("static nkey users = %d, want exactly %d (no extra/leaked nkey)", len(opts.Nkeys), len(pubs))
	}

	// RF1: every broker has a static nkey user with cluster.apply.>/cluster.> in
	// BOTH publish and subscribe.
	byNkey := map[string]*natsserver.NkeyUser{}
	for _, u := range opts.Nkeys {
		byNkey[u.Nkey] = u
	}
	for _, p := range pubs {
		u := byNkey[p]
		if u == nil {
			t.Errorf("broker pub %q has no static nkey user (auth_users alone => no subject permissions, §6.2 R3F3)", p)
			continue
		}
		if u.Permissions == nil || u.Permissions.Publish == nil || u.Permissions.Subscribe == nil {
			t.Errorf("broker %q static user missing permissions", p)
			continue
		}
		if !containsStr(u.Permissions.Publish.Allow, "tether.v2.cluster.apply.>") ||
			!containsStr(u.Permissions.Publish.Allow, "tether.v2.cluster.>") {
			t.Errorf("broker %q PUBLISH allow missing RF1 cluster.* grants: %v", p, u.Permissions.Publish.Allow)
		}
		if !containsStr(u.Permissions.Subscribe.Allow, "tether.v2.cluster.apply.>") ||
			!containsStr(u.Permissions.Subscribe.Allow, "tether.v2.cluster.>") {
			t.Errorf("broker %q SUBSCRIBE allow missing RF1 cluster.* grants: %v", p, u.Permissions.Subscribe.Allow)
		}
	}

	// Cluster block is mTLS (routes TLS verify on).
	if opts.Cluster.Name == "" {
		t.Error("cluster block missing name")
	}
	if opts.Cluster.TLSConfig == nil {
		t.Error("cluster routes must be mTLS (TLSConfig nil)")
	}
	if len(opts.Routes) == 0 {
		t.Error("cluster routes empty (no full-mesh)")
	}
}

// TestD3NatsClusterOmissionDetected proves the completeness assertion bites: a
// peer dropped from the roster is genuinely absent from the rendered auth_users.
func TestD3NatsClusterOmissionDetected(t *testing.T) {
	dir := t.TempDir()
	cfg, pubs, _ := threeBrokerConfig(t, dir)
	dropped := pubs[2]
	cfg.Peers = cfg.Peers[:2] // drop the third broker

	confText, err := Render(cfg)
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(confText, dropped) {
		t.Fatalf("dropped broker pub %q must NOT appear in the rendered conf", dropped)
	}
	confPath := filepath.Join(dir, "nats.conf")
	if err := os.WriteFile(confPath, []byte(confText), 0o600); err != nil {
		t.Fatal(err)
	}
	opts, err := natsserver.ProcessConfigFile(confPath)
	if err != nil {
		t.Fatal(err)
	}
	if containsStr(opts.AuthCallout.AuthUsers, dropped) {
		t.Fatalf("dropped broker %q must not be in auth_users", dropped)
	}
}

func containsStr(set []string, want string) bool {
	for _, s := range set {
		if s == want {
			return true
		}
	}
	return false
}
