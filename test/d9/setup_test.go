//go:build d9_integration

// Package d9_test is the D9 cutover proof harness (gated, runs in TestD9Matrix -race).
// It exercises the REAL production cutover path: seed+bootstrap a DB via
// clusteroffline.InitFromExisting, then start a broker in CLUSTER MODE (broker.New +
// Run with broker.cluster.* config + on-disk secrets) against an embedded JetStream
// NATS server — the same path serve.go takes after `cluster init --from-existing`.
// This validates the step-4 seam wiring (cluster.Node constructed, all seams attached,
// leader-gated loops + responders running) and the step-3 write routing (a session
// create round-trips through Propose) that the deleted build-and-prove guards used to
// only assert was ABSENT.
package d9_test

import (
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"math/big"
	"net"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/LinZiyang666/tether/internal/auth"
	"github.com/LinZiyang666/tether/internal/broker"
	"github.com/LinZiyang666/tether/internal/clusteroffline"
	"github.com/LinZiyang666/tether/internal/storage"
	natsserver "github.com/nats-io/nats-server/v2/server"
	natstest "github.com/nats-io/nats-server/v2/test"
)

// ---- cluster CA + leaf (mTLS raft route cert), mirrors test/d8 ----

type d9CA struct {
	certDER []byte
	cert    *x509.Certificate
	key     ed25519.PrivateKey
}

func newD9CA(t *testing.T) *d9CA {
	t.Helper()
	pub, priv, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	tmpl := &x509.Certificate{
		SerialNumber: big.NewInt(1), Subject: pkix.Name{CommonName: "tether-d9-ca"},
		NotBefore: time.Now().Add(-time.Hour), NotAfter: time.Now().Add(24 * time.Hour),
		IsCA: true, KeyUsage: x509.KeyUsageCertSign | x509.KeyUsageDigitalSignature, BasicConstraintsValid: true,
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, pub, priv)
	if err != nil {
		t.Fatal(err)
	}
	cert, _ := x509.ParseCertificate(der)
	return &d9CA{certDER: der, cert: cert, key: priv}
}

// signedLeafPEM returns (certPEM, keyPEM) for a leaf chaining to the CA.
func (ca *d9CA) signedLeafPEM(t *testing.T, cn string) (string, string) {
	t.Helper()
	pub, priv, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	tmpl := &x509.Certificate{
		SerialNumber: big.NewInt(7), Subject: pkix.Name{CommonName: cn},
		NotBefore: time.Now().Add(-time.Hour), NotAfter: time.Now().Add(24 * time.Hour),
		KeyUsage:    x509.KeyUsageDigitalSignature,
		ExtKeyUsage: []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth, x509.ExtKeyUsageClientAuth},
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, ca.cert, pub, ca.key)
	if err != nil {
		t.Fatal(err)
	}
	keyDER, err := x509.MarshalPKCS8PrivateKey(priv)
	if err != nil {
		t.Fatal(err)
	}
	certPEM := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der})
	keyPEM := pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: keyDER})
	return string(certPEM), string(keyPEM)
}

func mustWrite(t *testing.T, path, content string) {
	t.Helper()
	if err := os.WriteFile(path, []byte(content), 0o600); err != nil {
		t.Fatalf("write %s: %v", path, err)
	}
}

// writeD9Secrets writes the §15 secrets the cluster daemon + InitFromExisting need:
// cluster-ca.pem (trust anchor), route-cert/key.pem (this node's CA-signed raft leaf),
// tunnel-cert/key.pem (the stable tunnel cert whose fp seeds cluster_nodes.cert_fp).
func writeD9Secrets(t *testing.T, ca *d9CA) string {
	t.Helper()
	dir := t.TempDir()
	caPEM := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: ca.certDER})
	mustWrite(t, filepath.Join(dir, "cluster-ca.pem"), string(caPEM))
	routeCert, routeKey := ca.signedLeafPEM(t, "tether-d9-route")
	mustWrite(t, filepath.Join(dir, "route-cert.pem"), routeCert)
	mustWrite(t, filepath.Join(dir, "route-key.pem"), routeKey)
	tunCert, tunKey := ca.signedLeafPEM(t, "tether-d9-tunnel")
	mustWrite(t, filepath.Join(dir, "tunnel-cert.pem"), tunCert)
	mustWrite(t, filepath.Join(dir, "tunnel-key.pem"), tunKey)
	// node-ident.nk is the cluster join identity; broker.nk/account.nk are the broker-bus
	// and auth_callout account seeds cluster-mode serve defaults to cluster secrets for.
	seed, err := auth.GenerateUserSeed()
	if err != nil {
		t.Fatalf("gen node-ident.nk: %v", err)
	}
	mustWrite(t, filepath.Join(dir, "node-ident.nk"), string(seed))
	brokerSeed, err := auth.GenerateUserSeed()
	if err != nil {
		t.Fatalf("gen broker.nk: %v", err)
	}
	mustWrite(t, filepath.Join(dir, "broker.nk"), string(brokerSeed))
	accountSeed, err := auth.GenerateUserSeed()
	if err != nil {
		t.Fatalf("gen account.nk: %v", err)
	}
	mustWrite(t, filepath.Join(dir, "account.nk"), string(accountSeed))
	return dir
}

func freeTCPAddr(t *testing.T) string {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	addr := ln.Addr().String()
	_ = ln.Close()
	return addr
}

// d9Broker is a single cluster-mode broker + its embedded NATS server.
type d9Broker struct {
	b      *broker.Broker
	nats   *natsserver.Server
	url    string
	cancel func()
	done   chan error
}

// startD9Broker seeds+bootstraps a fresh single-voter cluster DB (InitFromExisting),
// then starts a broker in cluster mode against an embedded JetStream NATS server. It
// blocks until Run signals ready. selfID is the node_id (== raft ServerID).
func startD9Broker(t *testing.T, selfID string) *d9Broker {
	t.Helper()
	natsOpts := natstest.DefaultTestOptions
	natsOpts.Port = -1
	natsOpts.JetStream = true
	natsOpts.StoreDir = t.TempDir()
	ns := natstest.RunServer(&natsOpts)
	t.Cleanup(ns.Shutdown)
	h, _ := startD9BrokerOn(t, selfID, newD9CA(t), ns.ClientURL())
	h.nats = ns
	return h
}

// startD9BrokerOn seeds + bootstraps a single-voter cluster DB (InitFromExisting, shared
// CA) and starts a broker in cluster mode against the given (possibly shared) NATS URL. It
// returns the handle + this broker's raft address (for a join). Used by both startD9Broker
// (own NATS) and startD9Pair (shared NATS + shared CA → a real 2-broker cluster).
func startD9BrokerOn(t *testing.T, selfID string, ca *d9CA, natsURL string) (*d9Broker, string) {
	t.Helper()
	dataDir := t.TempDir()
	dbPath := filepath.Join(dataDir, "tether.db")
	db, err := storage.Open(dbPath)
	if err != nil {
		t.Fatalf("seed DB: %v", err)
	}
	_ = db.Close()

	secrets := writeD9Secrets(t, ca)
	raftAddr := freeTCPAddr(t)
	if err := clusteroffline.InitFromExisting(clusteroffline.InitFromExistingOptions{
		DataDir: dataDir, DBPath: dbPath, SecretsDir: secrets,
		SelfID: selfID, Name: selfID, NodeIdentPub: "pub-" + selfID,
		RaftAddr: raftAddr, NatsRoute: "127.0.0.1:6222",
		TunnelAddr: "127.0.0.1:7000", PublicHost: "localhost",
		Now: time.Now,
	}); err != nil {
		t.Fatalf("InitFromExisting(%s): %v", selfID, err)
	}

	cfg := broker.Config{
		NATSURL:           natsURL,
		ClusterDataDir:    dataDir,
		ClusterRaftAddr:   raftAddr,
		ClusterSecretsDir: secrets,
		DBPath:            dbPath,
		StoreDir:          t.TempDir(),
		Now:               time.Now,
		ReadyCh:           make(chan struct{}),
	}
	b, err := broker.New(cfg)
	if err != nil {
		t.Fatalf("broker.New(%s, cluster mode): %v", selfID, err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- b.Run(ctx) }()
	select {
	case <-cfg.ReadyCh:
	case err := <-done:
		cancel()
		t.Fatalf("broker.Run(%s) exited before ready: %v", selfID, err)
	case <-time.After(30 * time.Second):
		cancel()
		t.Fatalf("broker.Run(%s) not ready within 30s", selfID)
	}
	h := &d9Broker{b: b, url: natsURL, cancel: cancel, done: done}
	t.Cleanup(func() {
		cancel()
		select {
		case <-done:
		case <-time.After(10 * time.Second):
			t.Errorf("broker.Run(%s) did not exit within 10s of cancel", selfID)
		}
	})
	return h, raftAddr
}

// startD9Pair starts a real 2-broker cluster: brokers A + B share ONE NATS bus (the
// cluster.apply forward + cluster-health broadcast ride it) and ONE cluster CA (their raft
// route certs chain to it). Both bootstrap {self} via InitFromExisting; the caller then
// joins B to A via the leader's ClusterAdmin (the §8.1 two-phase add). Returns A, B, B's
// node_id, and B's raft address.
func startD9Pair(t *testing.T) (a, b *d9Broker, bID, bRaftAddr string) {
	t.Helper()
	ca := newD9CA(t)
	natsOpts := natstest.DefaultTestOptions
	natsOpts.Port = -1
	natsOpts.JetStream = true
	natsOpts.StoreDir = t.TempDir()
	ns := natstest.RunServer(&natsOpts)
	t.Cleanup(ns.Shutdown)

	a, _ = startD9BrokerOn(t, "d9-A", ca, ns.ClientURL())
	a.nats = ns
	b, bRaftAddr = startD9BrokerOn(t, "d9-B", ca, ns.ClientURL())
	b.nats = ns
	return a, b, "d9-B", bRaftAddr
}
