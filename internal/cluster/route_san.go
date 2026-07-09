package cluster

import (
	"crypto/x509"
	"encoding/pem"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"strings"
)

// route_san.go (G4 #24 fail-fast) — the STANDARD Go x509 verification that nats-server's cluster route mesh
// performs (chain-to-CA + SAN match against the DIALED route host). tether's own raft transport
// (transport.go) sets InsecureSkipVerify:true and accepts a CN-only leaf, but the SAME route-cert.pem feeds
// the nats route mesh's `cluster{ tls{ verify: true } }`, which since Go 1.15 REQUIRES a subjectAltName
// matching the route-URL host — so a CN-only leaf mounts raft fine yet silently breaks the mesh
// (`x509: certificate relies on legacy Common Name field`), leaving a fresh voter stuck CATCHING_UP with no
// visible cause. `cluster add` calls this in P0 to fail FAST with an actionable message. It does NOT clear
// #24's trailer token (tether still ships no cert-minting tool) — it only turns a silent handshake failure
// into a loud preflight error. The invariant this reproduces is pinned by g1_route_san_test.go.

// RouteCertSANMatches verifies secretsDir/route-cert.pem chains to secretsDir/cluster-ca.pem AND carries a
// SAN matching routeHost (a DNS name or an IP; a "host:port" or a "nats://host:port" is tolerated). It
// returns nil on success, or an actionable error naming the #24 remedy.
func RouteCertSANMatches(secretsDir, routeHost string) error {
	host := strings.TrimPrefix(strings.TrimSpace(routeHost), "nats://")
	if h, _, err := net.SplitHostPort(host); err == nil {
		host = h
	}
	if host == "" {
		return fmt.Errorf("route SAN preflight: empty route host")
	}

	certPath := filepath.Join(secretsDir, "route-cert.pem")
	caPath := filepath.Join(secretsDir, "cluster-ca.pem")

	certPEM, err := os.ReadFile(certPath)
	if err != nil {
		return fmt.Errorf("route SAN preflight: read %s: %w", certPath, err)
	}
	leaf, intermediates, err := parseCertChain(certPEM)
	if err != nil {
		return fmt.Errorf("route SAN preflight: parse %s: %w", certPath, err)
	}

	caPEM, err := os.ReadFile(caPath)
	if err != nil {
		return fmt.Errorf("route SAN preflight: read %s: %w", caPath, err)
	}
	roots := x509.NewCertPool()
	if !roots.AppendCertsFromPEM(caPEM) {
		return fmt.Errorf("route SAN preflight: %s has no usable CA certificate", caPath)
	}

	if _, verr := leaf.Verify(x509.VerifyOptions{Roots: roots, Intermediates: intermediates, DNSName: host}); verr != nil {
		return fmt.Errorf("route-cert.pem does not verify for route host %q (%v) — the nats cluster route mesh needs a subjectAltName matching the route-URL host "+
			"(a CN-only cert works for raft but breaks the mesh since Go 1.15, #24). Re-mint route-cert.pem with `subjectAltName=DNS:%s` (or IP:%s for an IP route)", host, verr, host, host)
	}
	return nil
}

// parseCertChain splits a PEM (leaf first, then any intermediates) into the leaf + an intermediate pool.
func parseCertChain(pemBytes []byte) (*x509.Certificate, *x509.CertPool, error) {
	var leaf *x509.Certificate
	intermediates := x509.NewCertPool()
	rest := pemBytes
	for {
		var block *pem.Block
		block, rest = pem.Decode(rest)
		if block == nil {
			break
		}
		if block.Type != "CERTIFICATE" {
			continue
		}
		c, err := x509.ParseCertificate(block.Bytes)
		if err != nil {
			return nil, nil, err
		}
		if leaf == nil {
			leaf = c
		} else {
			intermediates.AddCert(c)
		}
	}
	if leaf == nil {
		return nil, nil, fmt.Errorf("no CERTIFICATE block found")
	}
	return leaf, intermediates, nil
}
