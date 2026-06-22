package d3_test

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"fmt"
	"math/big"
	"net"
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	"github.com/LinZiyang666/tether/internal/authcallout"
	"github.com/LinZiyang666/tether/internal/natscluster"
	natsserver "github.com/nats-io/nats-server/v2/server"
	"github.com/nats-io/nats.go"
)

// writeRouteLeafPEMs writes an ECDSA cluster CA + one leaf (IP SAN 127.0.0.1 so the
// route hostname check passes) as ca.pem/cert.pem/key.pem under dir.
func writeRouteLeafPEMs(t *testing.T, dir string) (caPath, certPath, keyPath string) {
	t.Helper()
	caKey, _ := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	caTmpl := &x509.Certificate{
		SerialNumber: big.NewInt(1), Subject: pkix.Name{CommonName: "tether-rc-ca"},
		NotBefore: time.Now().Add(-time.Hour), NotAfter: time.Now().Add(24 * time.Hour),
		IsCA: true, KeyUsage: x509.KeyUsageCertSign | x509.KeyUsageDigitalSignature, BasicConstraintsValid: true,
	}
	caDER, _ := x509.CreateCertificate(rand.Reader, caTmpl, caTmpl, &caKey.PublicKey, caKey)
	caCert, _ := x509.ParseCertificate(caDER)

	leafKey, _ := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	leafTmpl := &x509.Certificate{
		SerialNumber: big.NewInt(2), Subject: pkix.Name{CommonName: "tether-rc-node"},
		NotBefore: time.Now().Add(-time.Hour), NotAfter: time.Now().Add(24 * time.Hour),
		KeyUsage:    x509.KeyUsageDigitalSignature,
		ExtKeyUsage: []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth, x509.ExtKeyUsageClientAuth},
		DNSNames:    []string{"localhost"}, IPAddresses: []net.IP{net.ParseIP("127.0.0.1")},
	}
	leafDER, _ := x509.CreateCertificate(rand.Reader, leafTmpl, caCert, &leafKey.PublicKey, caKey)
	keyDER, _ := x509.MarshalPKCS8PrivateKey(leafKey)

	caPath = filepath.Join(dir, "ca.pem")
	certPath = filepath.Join(dir, "cert.pem")
	keyPath = filepath.Join(dir, "key.pem")
	for _, w := range []struct {
		path, typ string
		der       []byte
	}{{caPath, "CERTIFICATE", caDER}, {certPath, "CERTIFICATE", leafDER}, {keyPath, "PRIVATE KEY", keyDER}} {
		if err := os.WriteFile(w.path, pem.EncodeToMemory(&pem.Block{Type: w.typ, Bytes: w.der}), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	return caPath, certPath, keyPath
}

func freePort(t *testing.T) int {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = l.Close() }()
	return l.Addr().(*net.TCPAddr).Port
}

// bootFromConfTry boots a server from a rendered conf WITHOUT t.Fatal, so the
// caller can retry on a transient bind/startup failure (a pre-allocated port can
// be grabbed before the server binds it under heavy parallel -race load). Returns
// (nil, false) on any failure; the caller owns shutdown of a returned server.
func bootFromConfTry(confPath string) (*natsserver.Server, error) {
	opts, err := natsserver.ProcessConfigFile(confPath)
	if err != nil {
		return nil, fmt.Errorf("ProcessConfigFile: %w", err)
	}
	opts.NoLog = true
	opts.NoSigs = true
	s, err := natsserver.NewServer(opts)
	if err != nil {
		return nil, fmt.Errorf("NewServer: %w", err)
	}
	go s.Start()
	if !s.ReadyForConnections(10 * time.Second) {
		s.Shutdown()
		s.WaitForShutdown()
		return nil, fmt.Errorf("not ready in 10s")
	}
	return s, nil
}

// TestD3RenderedConfCrossServerCalloutDedupe closes M4+M5+M6: two servers booted
// from the REAL rendered conf form an mTLS-routes mesh; a cross-server auth_callout
// is authorized; and the tether-authcallout queue group means EXACTLY ONE of the two
// brokers answers (no double-answer). This exercises the production conf shape end to
// end (renderer → live server → mTLS routes → cross-server callout → dedupe).
func TestD3RenderedConfCrossServerCalloutDedupe(t *testing.T) {
	dir := t.TempDir()
	ca, cert, key := writeRouteLeafPEMs(t, dir)

	brokers := []brokerID{newBrokerID(t), newBrokerID(t)}
	acctKp, acctPub := newAccount(t)

	// Conf needs EXPLICIT cluster ports (conf port 0 = clustering disabled, unlike
	// Options.Cluster.Port=-1). Pre-allocated ports carry a small TOCTOU window
	// under heavy parallel -race load, so the whole 2-server mesh boot is wrapped in
	// a bounded retry: on any transient bind/not-ready, reallocate ports and reboot.
	// Both confs carry BOTH broker pubs in auth_users (cross-server callout).
	// Full-mesh routes (both servers route to each other), explicit cluster + client
	// ports (conf cannot express a random cluster port, and a random client port
	// proved unreliable for the routed second server).
	mkConf := func(name string, local int, routeURLs [2]string, clusterPort, clientPort int) string {
		peers := []natscluster.Broker{
			{ServerName: "tether-1", NkeyPub: brokers[0].pub, RouteURL: routeURLs[0]},
			{ServerName: "tether-2", NkeyPub: brokers[1].pub, RouteURL: routeURLs[1]},
		}
		cfg := natscluster.Config{
			Local: peers[local], Peers: peers, AccountIssuer: acctPub, Account: "$G",
			ClientListen: fmt.Sprintf("127.0.0.1:%d", clientPort), ClusterName: "tether",
			ClusterListen: fmt.Sprintf("127.0.0.1:%d", clusterPort),
			CAFile:        ca, CertFile: cert, KeyFile: key,
			// JSStoreDir intentionally empty: this test exercises routes + callout, not JS.
		}
		conf, err := natscluster.Render(cfg)
		if err != nil {
			t.Fatalf("render %s: %v", name, err)
		}
		confPath := filepath.Join(dir, name+".conf")
		if err := os.WriteFile(confPath, []byte(conf), 0o600); err != nil {
			t.Fatal(err)
		}
		return confPath
	}

	// Pre-allocated ports carry a small TOCTOU window under heavy parallel -race
	// load, so the whole 2-server mesh boot is wrapped in a bounded retry: on any
	// transient bind/not-ready, reallocate ports and reboot from fresh confs.
	var servers []*natsserver.Server
	for attempt := 1; attempt <= 4 && servers == nil; attempt++ {
		cp := [2]int{freePort(t), freePort(t)}
		clp := [2]int{freePort(t), freePort(t)}
		routes := [2]string{
			fmt.Sprintf("nats://127.0.0.1:%d", cp[0]),
			fmt.Sprintf("nats://127.0.0.1:%d", cp[1]),
		}
		sA, errA := bootFromConfTry(mkConf("nats-a", 0, routes, cp[0], clp[0]))
		var sB *natsserver.Server
		var errB error
		if errA == nil {
			sB, errB = bootFromConfTry(mkConf("nats-b", 1, routes, cp[1], clp[1]))
		}
		if errA == nil && errB == nil &&
			waitForCond(5*time.Second, func() bool { return sA.NumRoutes() >= 1 && sB.NumRoutes() >= 1 }) {
			servers = []*natsserver.Server{sA, sB}
			break
		}
		t.Logf("mesh boot attempt %d failed: errA=%v errB=%v", attempt, errA, errB)
		if sA != nil {
			sA.Shutdown()
			sA.WaitForShutdown()
		}
		if sB != nil {
			sB.Shutdown()
			sB.WaitForShutdown()
		}
	}
	if servers == nil {
		t.Fatal("could not boot a 2-node mTLS mesh from rendered confs after retries")
	}
	t.Cleanup(func() {
		for _, s := range servers {
			s.Shutdown()
			s.WaitForShutdown()
		}
	})

	// One responder per server, sharing the tether-authcallout queue group; each
	// counts how many requests IT answered.
	var answered [2]int64
	for i := range servers {
		h := &authcallout.Handler{DB: openMemDB(t), AccountKp: acctKp, Now: time.Now}
		idx := i
		nc, err := nats.Connect(servers[i].ClientURL(), nats.Name("tetherd"),
			nats.Nkey(brokers[i].pub, sigFromSeed(brokers[i].seed)), nats.Timeout(3*time.Second))
		if err != nil {
			t.Fatalf("responder %d connect: %v", i, err)
		}
		t.Cleanup(func() { nc.Close() })
		if _, err := nc.QueueSubscribe("$SYS.REQ.USER.AUTH", "tether-authcallout", func(m *nats.Msg) {
			atomic.AddInt64(&answered[idx], 1)
			resp, herr := h.Handle(string(m.Data))
			if herr == nil && m.Reply != "" {
				_ = m.Respond([]byte(resp))
			}
		}); err != nil {
			t.Fatalf("responder %d subscribe: %v", i, err)
		}
		_ = nc.Flush()
	}

	// Client connects to server A (index 0); authorized via the cluster.
	cli, err := connectCLI(servers[0].ClientURL())
	if err != nil {
		t.Fatalf("cross-server callout over the rendered-conf mTLS mesh failed: %v", err)
	}
	cli.Close()

	// Exactly one responder answered (queue-group dedupe — no double-answer).
	total := atomic.LoadInt64(&answered[0]) + atomic.LoadInt64(&answered[1])
	if total != 1 {
		t.Fatalf("tether-authcallout queue group must yield EXACTLY ONE answer per request; got %d (a=%d b=%d)",
			total, answered[0], answered[1])
	}
}
