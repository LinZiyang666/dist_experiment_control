//go:build d6_integration

// Package d6_test's integration harness (gated //go:build d6_integration, run in
// the dedicated TestD6Matrix -race subprocess, kept OUT of `make test`). It
// proves the D6 data plane END-TO-END with REAL tunnel servers (stable pinned
// certs) + a REAL agent tunnel.Client + REAL broker tunnelTokenLookups, over a
// SHARED port_allocations DB that stands in for the replicated cluster state
// (seeded directly, build-and-prove — the FSM producer op OpPortReassignHome is
// unit-proven separately). No routed NATS / raft is needed: D6 home assignment
// is a consumer-side mechanism (read the seeded home_broker/epoch + the injected
// selfID), so the harness stays light.
package d6_test

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/sha256"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"database/sql"
	"encoding/hex"
	"encoding/pem"
	"io"
	"log/slog"
	"math/big"
	"net"
	"strconv"
	"testing"
	"time"

	"github.com/LinZiyang666/tether/internal/broker"
	"github.com/LinZiyang666/tether/internal/proto"
	"github.com/LinZiyang666/tether/internal/storage"
	"github.com/LinZiyang666/tether/internal/tunnel"
)

func sha256hex(s string) string {
	sum := sha256.Sum256([]byte(s))
	return hex.EncodeToString(sum[:])
}

func silent() *slog.Logger { return slog.New(slog.NewTextHandler(io.Discard, nil)) }

// homeBrokerInst is one home broker in the harness: a *broker.Broker (selfID +
// stable cert attached via the build-and-prove seam) running a real tunnel
// server over the shared DB.
type homeBrokerInst struct {
	nodeID  string
	addr    string
	certFP  string
	cert    *tls.Certificate
	srv     *tunnel.Server
	cancel  context.CancelFunc
	b       *broker.Broker
}

// newHomeBroker builds a home broker over the shared DB with selfID=nodeID and a
// freshly-generated stable cert, starts its tunnel server, and returns it.
func newHomeBroker(t *testing.T, db *sql.DB, nodeID string) *homeBrokerInst {
	t.Helper()
	certPEM, keyPEM, fp := genStableCert(t)
	cert, err := tunnel.LoadServerCert(certPEM, keyPEM)
	if err != nil {
		t.Fatalf("load cert: %v", err)
	}
	b, err := broker.New(broker.Config{NATSURL: "nats://x", DB: db, Logger: silent()})
	if err != nil {
		t.Fatalf("broker.New: %v", err)
	}
	b.AttachClusterSeam(nodeID, cert)

	addr := "127.0.0.1:" + strconv.Itoa(freePort(t))
	srv := tunnel.NewServerWithCert(addr, "127.0.0.1", b.TunnelTokenLookupForTest(), cert, silent())
	ctx, cancel := context.WithCancel(context.Background())
	if err := srv.Start(ctx); err != nil {
		cancel()
		t.Fatalf("tunnel start: %v", err)
	}
	return &homeBrokerInst{nodeID: nodeID, addr: addr, certFP: fp, cert: cert, srv: srv, cancel: cancel, b: b}
}

func (h *homeBrokerInst) stop() {
	h.cancel()
	h.srv.Close()
}

// seedClusterNode inserts (or replaces) a cluster_nodes VOTER row for a home.
func seedClusterNode(t *testing.T, db *sql.DB, nodeID, natsServer, tunnelAddr, certFP string) {
	t.Helper()
	if _, err := db.Exec(
		`INSERT OR REPLACE INTO cluster_nodes
		   (node_id, name, node_ident_pub, nats_server_id, raft_addr, nats_route, tunnel_addr, public_host, cert_fp, phase, added_at)
		 VALUES (?,?,?,?,?,?,?,?,?, 'VOTER', ?)`,
		nodeID, nodeID, "Nident", natsServer, "127.0.0.1:7400", "nats://127.0.0.1:6222",
		tunnelAddr, "pub", certFP, time.Now().UTC(),
	); err != nil {
		t.Fatalf("seed cluster_node: %v", err)
	}
}

// seedHomedExpose inserts the session/node parents + one ALLOCATED homed expose
// row carrying token_hash + home_broker + epoch.
func seedHomedExpose(t *testing.T, db *sql.DB, sid, nid, name string, port, localPort int, rawToken, homeBroker string, epoch int64) {
	t.Helper()
	_, _ = db.Exec(`INSERT OR IGNORE INTO sessions(sid, name, owner_pubkey_fp, pin_hash) VALUES (?,?,?,?)`, sid, sid, "SHA256:o", "h")
	_, _ = db.Exec(`INSERT OR IGNORE INTO nodes(sid, nid, last_heartbeat_at, status) VALUES (?,?,?,?)`, sid, nid, time.Now().UTC(), "ONLINE")
	if _, err := db.Exec(
		`INSERT INTO port_allocations(port, sid, nid, name, local_port, token_hash, state, created_by_fp, created_at, home_broker, epoch)
		 VALUES (?,?,?,?,?,?, 'ALLOCATED', ?, ?, ?, ?)`,
		port, sid, nid, name, localPort, sha256hex(rawToken), "SHA256:o", time.Now().UTC(), homeBroker, epoch,
	); err != nil {
		t.Fatalf("seed expose: %v", err)
	}
}

func reseedHome(t *testing.T, db *sql.DB, port int, homeBroker string, epoch int64) {
	t.Helper()
	if _, err := db.Exec(`UPDATE port_allocations SET home_broker=?, epoch=? WHERE port=?`, homeBroker, epoch, port); err != nil {
		t.Fatalf("reseed home: %v", err)
	}
}

// openSharedDB returns ONE in-memory DB handed to BOTH home brokers in a test.
//
// SIMPLIFICATION (review L-2): a single shared DB stands in for the replicated
// cluster state, so a write by one broker is instantly visible to the other.
// This is faithful for the REHOME path (the row's home_broker/epoch is SEEDED
// directly via seedHomedExpose/reseedHome, simulating a leader-committed,
// replicated reassign). It is NOT faithful for `homeForExpose`'s direct,
// leader-local `db.Exec(UPDATE home_broker)` (home.go), which in real raft would
// NOT replicate to followers until the D9 cutover wires PlanAllocate(home) through
// the FSM. So TestD6LadderEnforcedEndToEnd proves the CONSUMER (ladder) against a
// homed row, NOT that the INITIAL-expose write replicates — that producer→replica
// chain is proven separately by TestD6InitialHomeReplicatedLadder (unit, two DBs).
func openSharedDB(t *testing.T) *sql.DB {
	t.Helper()
	db, err := storage.Open(":memory:")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	return db
}

// startEcho starts a loopback TCP echo server and returns its port.
func startEcho(t *testing.T) int {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = ln.Close() })
	go func() {
		for {
			c, err := ln.Accept()
			if err != nil {
				return
			}
			go func(c net.Conn) { _, _ = io.Copy(c, c); _ = c.Close() }(c)
		}
	}()
	return ln.Addr().(*net.TCPAddr).Port
}

// dialEcho dials the public port, writes msg, and returns the echoed reply (or
// an error). Used to assert the data plane flows through the current home.
func dialEcho(publicPort int, msg string) (string, error) {
	c, err := net.DialTimeout("tcp", "127.0.0.1:"+strconv.Itoa(publicPort), 2*time.Second)
	if err != nil {
		return "", err
	}
	defer func() { _ = c.Close() }()
	_ = c.SetDeadline(time.Now().Add(2 * time.Second))
	if _, err := c.Write([]byte(msg)); err != nil {
		return "", err
	}
	buf := make([]byte, len(msg))
	if _, err := io.ReadFull(c, buf); err != nil {
		return "", err
	}
	return string(buf), nil
}

func waitEcho(t *testing.T, publicPort int, msg string, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	var lastErr error
	for time.Now().Before(deadline) {
		got, err := dialEcho(publicPort, msg)
		if err == nil && got == msg {
			return
		}
		lastErr = err
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatalf("data plane never flowed on public port %d: last err %v", publicPort, lastErr)
}

func freePort(t *testing.T) int {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	p := ln.Addr().(*net.TCPAddr).Port
	_ = ln.Close()
	return p
}

// genStableCert returns a self-signed ECDSA cert as PEM (cert,key) + its
// CertFingerprint (matching tunnel.CertFingerprint over the leaf DER).
func genStableCert(t *testing.T) (certPEM, keyPEM, fp string) {
	t.Helper()
	priv, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	serial, _ := rand.Int(rand.Reader, new(big.Int).Lsh(big.NewInt(1), 128))
	tmpl := x509.Certificate{
		SerialNumber: serial,
		Subject:      pkix.Name{CommonName: "tether-home"},
		NotBefore:    time.Now().Add(-time.Hour),
		NotAfter:     time.Now().Add(24 * time.Hour),
		KeyUsage:     x509.KeyUsageDigitalSignature | x509.KeyUsageKeyEncipherment,
		ExtKeyUsage:  []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
	}
	der, err := x509.CreateCertificate(rand.Reader, &tmpl, &tmpl, &priv.PublicKey, priv)
	if err != nil {
		t.Fatal(err)
	}
	leaf, _ := x509.ParseCertificate(der)
	keyDER, _ := x509.MarshalECPrivateKey(priv)
	certPEM = string(pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der}))
	keyPEM = string(pem.EncodeToMemory(&pem.Block{Type: "EC PRIVATE KEY", Bytes: keyDER}))
	return certPEM, keyPEM, tunnel.CertFingerprint(leaf)
}

func pinsFor(h *homeBrokerInst) proto.CertPins { return proto.CertPins{Current: h.certFP} }
