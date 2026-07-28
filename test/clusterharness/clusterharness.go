// Package clusterharness holds the scaffolding the multi-node integration suites (test/d3, d4, d5,
// d8, d9) each used to carry their own copy of (B9).
//
// WHY A SEPARATE PACKAGE FROM internal/testharness
// ------------------------------------------------
// internal/testharness is imported by nearly every test in the repo, including single-node unit tests.
// What lives here pulls in crypto/x509, crypto/tls and the embedded nats-server — machinery only the
// clustered suites need. Keeping the two apart means a unit test that wants SilentLog does not compile
// a certificate authority.
//
// WHY NOT _test.go FILES
// ----------------------
// A _test.go file is visible only to its own package, which is precisely why the copies existed: five
// suites, five setup_test.go files, five of everything. Ordinary .go files in an ordinary package are
// what makes them shareable, and `testing.T` in a non-test file is exactly what internal/testharness
// already does.
//
// WHAT BELONGS HERE
// -----------------
// Only helpers that were genuinely IDENTICAL across suites, or identical modulo a name string. The
// per-suite cluster builders (startCluster4 / startCluster5 / startD8Cluster) stay where they are:
// they differ in what they boot (auth vs plain, JS vs not, forwarder vs publisher vs alerts), and
// merging them would produce one function with five modes — which is the shape this batch spent its
// budget deleting elsewhere.
package clusterharness

import (
	"crypto/ed25519"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"math/big"
	"net"
	"testing"
	"time"
)

// RouteCA is a throwaway certificate authority for raft route mTLS.
//
// It replaces three character-for-character copies (test/d4, test/d5, test/d8 setup_test.go), which
// differed only in the CN string baked into the template — and in that the d8 copy silently DROPPED
// two error checks, so a CreateCertificate failure there produced a nil-DER certificate and a
// confusing TLS handshake error a layer later instead of a test failure at the point of the fault.
// Sharing one implementation fixes that copy by construction, which is the argument for sharing:
// three copies means three chances to be the one that skips a check.
type RouteCA struct {
	Cert *x509.Certificate
	Key  ed25519.PrivateKey
	Pool *x509.CertPool
}

// NewRouteCA mints a self-signed CA valid for 24h. name is folded into the CN so a handshake failure
// in a mixed run still says which suite's CA was involved (e.g. "tether-d5-ca").
func NewRouteCA(t *testing.T, name string) *RouteCA {
	t.Helper()
	pub, priv, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	tmpl := &x509.Certificate{
		SerialNumber: big.NewInt(1), Subject: pkix.Name{CommonName: name + "-ca"},
		NotBefore: time.Now().Add(-time.Hour), NotAfter: time.Now().Add(24 * time.Hour),
		IsCA: true, KeyUsage: x509.KeyUsageCertSign | x509.KeyUsageDigitalSignature, BasicConstraintsValid: true,
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, pub, priv)
	if err != nil {
		t.Fatalf("create %s CA: %v", name, err)
	}
	cert, err := x509.ParseCertificate(der)
	if err != nil {
		t.Fatalf("parse %s CA: %v", name, err)
	}
	pool := x509.NewCertPool()
	pool.AddCert(cert)
	return &RouteCA{Cert: cert, Key: priv, Pool: pool}
}

// Leaf issues a server+client leaf signed by this CA. Both EKUs are set because a raft route is
// mutually authenticated: the same certificate is presented as the server end of one connection and
// the client end of another.
func (ca *RouteCA) Leaf(t *testing.T, name string) tls.Certificate {
	t.Helper()
	pub, priv, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	tmpl := &x509.Certificate{
		SerialNumber: big.NewInt(7), Subject: pkix.Name{CommonName: name + "-node"},
		NotBefore: time.Now().Add(-time.Hour), NotAfter: time.Now().Add(24 * time.Hour),
		KeyUsage:    x509.KeyUsageDigitalSignature,
		ExtKeyUsage: []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth, x509.ExtKeyUsageClientAuth},
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, ca.Cert, pub, ca.Key)
	if err != nil {
		t.Fatalf("create %s leaf: %v", name, err)
	}
	return tls.Certificate{Certificate: [][]byte{der}, PrivateKey: priv}
}

// WaitForCond polls pred every 20ms until it returns true or within elapses, then evaluates it ONE
// more time and returns that. The final evaluation is not a stylistic flourish: without it, a
// predicate that becomes true during the last sleep is reported false, and the resulting failure is a
// flake that reproduces only under load.
//
// This replaces four identical copies (test/d3, d4, d5 as waitForCond; test/d8 as waitFor). It is
// kept distinct from testharness.WaitFor, which takes an explicit interval and a *testing.T — the
// clustered suites call this from inside predicates and helper closures where threading t through
// buys nothing.
func WaitForCond(within time.Duration, pred func() bool) bool {
	deadline := time.Now().Add(within)
	for time.Now().Before(deadline) {
		if pred() {
			return true
		}
		time.Sleep(20 * time.Millisecond)
	}
	return pred()
}

// FreePort reserves an ephemeral TCP port and immediately frees it, so a server can bind it.
//
// This is a TOCTOU by construction and that is understood (docs/testing-standards.md T5): the port can
// be taken between the Close here and the Listen there. It is used anyway because clustered JetStream
// leaves no alternative — JS refuses to start without routes configured at startup ("JetStream cluster
// requires configured routes"), so the whole route mesh must be wired BEFORE any server starts, which
// means every cluster port has to be known up front. The seed-then-join pattern that lets plain NATS
// use port 0 does not work for clustered JS. Callers handle the loss by retrying the entire bring-up
// on a fresh port set.
//
// Replaces two copies (test/d5 freePort, test/d8 freeClusterPort).
func FreePort(t *testing.T) int {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	port := l.Addr().(*net.TCPAddr).Port
	_ = l.Close()
	return port
}
