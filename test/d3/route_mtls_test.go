package d3_test

import (
	"crypto/ed25519"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"fmt"
	"math/big"
	"net"
	"testing"
	"time"

	natsserver "github.com/nats-io/nats-server/v2/server"
	natstest "github.com/nats-io/nats-server/v2/test"
)

// routeCA is an ephemeral CA that issues route-mTLS tls.Configs for embedded nats
// servers (Certificates + RootCAs + ClientCAs + RequireAndVerifyClientCert), with
// a 127.0.0.1 IP SAN so the dialing side's hostname check passes.
type routeCA struct {
	cert *x509.Certificate
	key  ed25519.PrivateKey
	pool *x509.CertPool
}

func newRouteCA(t *testing.T) *routeCA {
	t.Helper()
	pub, priv, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	tmpl := &x509.Certificate{
		SerialNumber: big.NewInt(1), Subject: pkix.Name{CommonName: "tether-route-ca"},
		NotBefore: time.Now().Add(-time.Hour), NotAfter: time.Now().Add(24 * time.Hour),
		IsCA: true, KeyUsage: x509.KeyUsageCertSign | x509.KeyUsageDigitalSignature, BasicConstraintsValid: true,
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, pub, priv)
	if err != nil {
		t.Fatal(err)
	}
	cert, _ := x509.ParseCertificate(der)
	pool := x509.NewCertPool()
	pool.AddCert(cert)
	return &routeCA{cert: cert, key: priv, pool: pool}
}

// clusterLeaf issues a cluster-CA-signed leaf as a tls.Certificate for
// cluster.NewMTLSTransport (the raft transport verifies chain-to-CA, not hostname).
func (ca *routeCA) clusterLeaf(t *testing.T) tls.Certificate {
	t.Helper()
	pub, priv, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	tmpl := &x509.Certificate{
		SerialNumber: big.NewInt(7), Subject: pkix.Name{CommonName: "tether-raft-node"},
		NotBefore: time.Now().Add(-time.Hour), NotAfter: time.Now().Add(24 * time.Hour),
		KeyUsage:    x509.KeyUsageDigitalSignature,
		ExtKeyUsage: []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth, x509.ExtKeyUsageClientAuth},
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, ca.cert, pub, ca.key)
	if err != nil {
		t.Fatal(err)
	}
	return tls.Certificate{Certificate: [][]byte{der}, PrivateKey: priv}
}

func (ca *routeCA) tlsConfig(t *testing.T) *tls.Config {
	t.Helper()
	pub, priv, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	tmpl := &x509.Certificate{
		SerialNumber: big.NewInt(2), Subject: pkix.Name{CommonName: "tether-route-node"},
		NotBefore: time.Now().Add(-time.Hour), NotAfter: time.Now().Add(24 * time.Hour),
		KeyUsage:    x509.KeyUsageDigitalSignature,
		ExtKeyUsage: []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth, x509.ExtKeyUsageClientAuth},
		DNSNames:    []string{"localhost"},
		IPAddresses: []net.IP{net.ParseIP("127.0.0.1")},
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, ca.cert, pub, ca.key)
	if err != nil {
		t.Fatal(err)
	}
	return &tls.Config{
		Certificates: []tls.Certificate{{Certificate: [][]byte{der}, PrivateKey: priv}},
		RootCAs:      ca.pool,
		ClientCAs:    ca.pool,
		ClientAuth:   tls.RequireAndVerifyClientCert,
		MinVersion:   tls.VersionTLS12,
	}
}

func startRouteMTLSServer(t *testing.T, routeTLS *tls.Config, seedRoute string) *natsserver.Server {
	t.Helper()
	o := baseClusterOpts()
	o.Cluster.TLSConfig = routeTLS
	o.Cluster.TLSTimeout = 5
	if seedRoute != "" {
		o.Routes = natsserver.RoutesFromStr(seedRoute)
	}
	ns := natstest.RunServer(o)
	t.Cleanup(func() { ns.Shutdown(); ns.WaitForShutdown() })
	if !ns.ReadyForConnections(5 * time.Second) {
		t.Fatal("route-mTLS server not ready")
	}
	return ns
}

// TestD3RouteMTLSRejectsBadCert is the LIVE routes-mTLS gate (not a golden-string
// check): two servers sharing the cluster CA form a mesh; a server presenting a
// foreign-CA route leaf never joins the mesh.
func TestD3RouteMTLSRejectsBadCert(t *testing.T) {
	t.Run("same CA forms the mesh", func(t *testing.T) {
		ca := newRouteCA(t)
		a := startRouteMTLSServer(t, ca.tlsConfig(t), "")
		seed := fmt.Sprintf("nats://127.0.0.1:%d", a.ClusterAddr().Port)
		b := startRouteMTLSServer(t, ca.tlsConfig(t), seed)
		waitRoutes(t, a, 1)
		waitRoutes(t, b, 1)
	})

	t.Run("foreign CA never joins the mesh", func(t *testing.T) {
		ca1 := newRouteCA(t)
		ca2 := newRouteCA(t)
		a := startRouteMTLSServer(t, ca1.tlsConfig(t), "")
		seed := fmt.Sprintf("nats://127.0.0.1:%d", a.ClusterAddr().Port)
		b := startRouteMTLSServer(t, ca2.tlsConfig(t), seed) // foreign CA
		// Give it ample time to (fail to) form a route, then assert it never did.
		deadline := time.Now().Add(3 * time.Second)
		for time.Now().Before(deadline) {
			if a.NumRoutes() > 0 || b.NumRoutes() > 0 {
				t.Fatalf("a foreign-CA route leaf must NOT join the mesh (a=%d b=%d routes)", a.NumRoutes(), b.NumRoutes())
			}
			time.Sleep(100 * time.Millisecond)
		}
	})
}
