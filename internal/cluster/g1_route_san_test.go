package cluster

import (
	"crypto/ed25519"
	"crypto/rand"
	"crypto/x509"
	"crypto/x509/pkix"
	"math/big"
	"net"
	"testing"
	"time"
)

// TestRouteCertSANRequiredForMesh pins the G1 #24 invariant that the corrected transport.go comments
// + docs describe: tether's OWN raft transport (transport.go) skips hostname verification and accepts
// a CN-only leaf, but the SAME route-cert.pem is consumed by nats-server's cluster route mesh, which
// renders `cluster{ tls{ verify: true } }` and does STANDARD Go x509 verification. Since Go 1.15 that
// verification REQUIRES a subjectAltName matching the DIALED route host (a DNS SAN for a hostname
// route URL, an IP SAN for an IP one — e.g. natscluster's example `nats://10.0.0.2:6222`). A CN-only
// route leaf is therefore rejected by the mesh even though it works for raft. This reproduces exactly
// that standard verification (chain-to-CA + hostname), independent of a running nats-server.
func TestRouteCertSANRequiredForMesh(t *testing.T) {
	caCert, caKey := g1NewCA(t)
	roots := x509.NewCertPool()
	roots.AddCert(caCert)

	mkLeaf := func(cn string, dns []string, ips []net.IP) *x509.Certificate {
		pub, _, err := ed25519.GenerateKey(rand.Reader)
		if err != nil {
			t.Fatal(err)
		}
		tmpl := &x509.Certificate{
			SerialNumber: big.NewInt(time.Now().UnixNano()),
			Subject:      pkix.Name{CommonName: cn},
			NotBefore:    time.Now().Add(-time.Hour),
			NotAfter:     time.Now().Add(24 * time.Hour),
			KeyUsage:     x509.KeyUsageDigitalSignature,
			ExtKeyUsage:  []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth, x509.ExtKeyUsageClientAuth},
			DNSNames:     dns,
			IPAddresses:  ips,
		}
		der, err := x509.CreateCertificate(rand.Reader, tmpl, caCert, pub, caKey)
		if err != nil {
			t.Fatal(err)
		}
		leaf, err := x509.ParseCertificate(der)
		if err != nil {
			t.Fatal(err)
		}
		return leaf
	}

	cases := []struct {
		name     string
		leaf     *x509.Certificate
		dialHost string
		wantOK   bool
	}{
		{"CN-only leaf, hostname dial → REJECTED (Go 1.15+ drops legacy CN)", mkLeaf("brk1", nil, nil), "brk1", false},
		{"DNS-SAN leaf, hostname dial → accepted", mkLeaf("brk1", []string{"brk1"}, nil), "brk1", true},
		{"IP-SAN leaf, IP dial → accepted", mkLeaf("brk1", nil, []net.IP{net.ParseIP("10.0.0.2")}), "10.0.0.2", true},
		{"DNS-SAN leaf, IP dial → REJECTED (SAN must match the route-URL host)", mkLeaf("brk1", []string{"brk1"}, nil), "10.0.0.2", false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := tc.leaf.Verify(x509.VerifyOptions{Roots: roots, DNSName: tc.dialHost})
			if tc.wantOK && err != nil {
				t.Fatalf("route leaf should PASS standard x509 verify for %q, got: %v", tc.dialHost, err)
			}
			if !tc.wantOK && err == nil {
				t.Fatalf("route leaf should FAIL standard x509 verify for %q (the #24 mesh SAN requirement)", tc.dialHost)
			}
		})
	}
}

// g1NewCA mints a self-signed ed25519 CA for the #24 route-cert SAN test (named distinctly from
// transport_test.go's newTestCA to avoid a collision in package cluster).
func g1NewCA(t *testing.T) (*x509.Certificate, ed25519.PrivateKey) {
	t.Helper()
	pub, priv, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	tmpl := &x509.Certificate{
		SerialNumber:          big.NewInt(1),
		Subject:               pkix.Name{CommonName: "tether-test-cluster-ca-g1"},
		NotBefore:             time.Now().Add(-time.Hour),
		NotAfter:              time.Now().Add(24 * time.Hour),
		IsCA:                  true,
		KeyUsage:              x509.KeyUsageCertSign | x509.KeyUsageDigitalSignature,
		BasicConstraintsValid: true,
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, pub, priv)
	if err != nil {
		t.Fatal(err)
	}
	cert, err := x509.ParseCertificate(der)
	if err != nil {
		t.Fatal(err)
	}
	return cert, priv
}
