package cluster

import (
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
)

// TestRouteCertSANPreflight pins that RouteCertSANMatches performs the SAME standard x509 verification the
// nats route mesh does (chain-to-CA + SAN-vs-dialed-host), rejecting a CN-only route leaf up front (#24)
// while accepting a DNS-SAN leaf for a hostname route and an IP-SAN leaf for an IP route.
func TestRouteCertSANPreflight(t *testing.T) {
	caCert, caKey := g1NewCA(t)

	mkLeafPEM := func(cn string, dns []string, ips []net.IP) []byte {
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
		return pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der})
	}
	caPEM := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: caCert.Raw})

	cases := []struct {
		name     string
		leafPEM  []byte
		dialHost string
		wantErr  bool
	}{
		{"CN-only leaf, hostname dial → rejected (#24)", mkLeafPEM("brk1", nil, nil), "brk1", true},
		{"DNS-SAN leaf, hostname dial → accepted", mkLeafPEM("brk1", []string{"brk1"}, nil), "brk1", false},
		{"DNS-SAN leaf, host:port dial → accepted (port stripped)", mkLeafPEM("brk1", []string{"brk1"}, nil), "brk1:6222", false},
		{"IP-SAN leaf, IP dial → accepted", mkLeafPEM("brk1", nil, []net.IP{net.ParseIP("10.0.0.2")}), "10.0.0.2", false},
		{"DNS-SAN leaf, IP dial → rejected (SAN must match route host)", mkLeafPEM("brk1", []string{"brk1"}, nil), "10.0.0.2", true},
		{"nats:// scheme tolerated", mkLeafPEM("brk1", []string{"brk1"}, nil), "nats://brk1:6222", false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			dir := t.TempDir()
			if err := os.WriteFile(filepath.Join(dir, "cluster-ca.pem"), caPEM, 0o600); err != nil {
				t.Fatal(err)
			}
			if err := os.WriteFile(filepath.Join(dir, "route-cert.pem"), tc.leafPEM, 0o600); err != nil {
				t.Fatal(err)
			}
			err := RouteCertSANMatches(dir, tc.dialHost)
			if tc.wantErr && err == nil {
				t.Fatalf("expected #24 SAN rejection for dial %q, got nil", tc.dialHost)
			}
			if !tc.wantErr && err != nil {
				t.Fatalf("expected accept for dial %q, got: %v", tc.dialHost, err)
			}
		})
	}
}

// TestRouteCertSANPreflight_MissingFiles pins that a missing route-cert / cluster-ca is a clear error, not a panic.
func TestRouteCertSANPreflight_MissingFiles(t *testing.T) {
	if err := RouteCertSANMatches(t.TempDir(), "brk1"); err == nil {
		t.Fatal("expected an error for a secrets dir with no route-cert.pem")
	}
}
