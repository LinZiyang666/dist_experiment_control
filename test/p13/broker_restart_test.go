package p13_test

// Real broker + NATS lifecycles, file SQLite, stable tunnel certificate, one
// unchanged agent.Run and production TunnelExposeAdapter, and fresh HTTP streams.
// No systemd, auth_callout, WAN, or recording adapter is involved.

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/json"
	"encoding/pem"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"math/big"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/LinZiyang666/tether/internal/agent"
	"github.com/LinZiyang666/tether/internal/auth"
	"github.com/LinZiyang666/tether/internal/broker"
	"github.com/LinZiyang666/tether/internal/proto"
	"github.com/LinZiyang666/tether/internal/session"
	"github.com/LinZiyang666/tether/internal/storage"
	"github.com/LinZiyang666/tether/internal/testharness"
	natsserver "github.com/nats-io/nats-server/v2/server"
	"github.com/nats-io/nats.go"
)

func writeRestartTunnelCertificate(t *testing.T, dir string) *tls.Config {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	template := &x509.Certificate{
		SerialNumber: big.NewInt(1), Subject: pkix.Name{CommonName: "broker-restart-test"},
		NotBefore: time.Now().Add(-time.Minute), NotAfter: time.Now().Add(time.Hour),
		IPAddresses: []net.IP{net.ParseIP("127.0.0.1")},
		KeyUsage:    x509.KeyUsageDigitalSignature, ExtKeyUsage: []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
	}
	der, err := x509.CreateCertificate(rand.Reader, template, template, &key.PublicKey, key)
	if err != nil {
		t.Fatal(err)
	}
	keyDER, err := x509.MarshalECPrivateKey(key)
	if err != nil {
		t.Fatal(err)
	}
	for name, block := range map[string]*pem.Block{
		"tunnel-cert.pem": {Type: "CERTIFICATE", Bytes: der},
		"tunnel-key.pem":  {Type: "EC PRIVATE KEY", Bytes: keyDER},
	} {
		if err := os.WriteFile(filepath.Join(dir, name), pem.EncodeToMemory(block), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	cert, err := x509.ParseCertificate(der)
	if err != nil {
		t.Fatal(err)
	}
	pool := x509.NewCertPool()
	pool.AddCert(cert)
	return &tls.Config{MinVersion: tls.VersionTLS12, RootCAs: pool}
}

// origin: control-connection-recovery, historical restart evidence needs a
// payload oracle; the absence of an "opened" log is not proof of no redial.
func TestExposeRecoversAcrossBrokerAndNATSRestarts(t *testing.T) {
	for _, restartNATS := range []bool{false, true} {
		t.Run(fmt.Sprintf("restart_nats=%v", restartNATS), func(t *testing.T) {
			dir := t.TempDir()
			db, err := storage.Open(filepath.Join(dir, "broker.db"))
			if err != nil {
				t.Fatal(err)
			}
			var ns *natsserver.Server
			var brokerCancel, agentCancel context.CancelFunc
			var brokerDone, agentDone chan error
			stopBroker := func() {
				if brokerCancel == nil {
					return
				}
				brokerCancel()
				select {
				case err := <-brokerDone:
					if err != nil && !errors.Is(err, context.Canceled) {
						t.Errorf("broker exit: %v", err)
					}
				case <-time.After(5 * time.Second):
					t.Error("broker did not stop")
				}
				brokerCancel = nil
			}
			t.Cleanup(func() {
				if agentCancel != nil {
					agentCancel()
					select {
					case err := <-agentDone:
						if err != nil && !errors.Is(err, context.Canceled) {
							t.Errorf("agent exit: %v", err)
						}
					case <-time.After(5 * time.Second):
						t.Error("agent did not stop")
					}
				}
				stopBroker()
				if ns != nil {
					ns.Shutdown()
					ns.WaitForShutdown()
				}
				_ = db.Close()
				if !testharness.WaitFor(t, 5*time.Second, time.Millisecond, func() bool { return db.Stats().OpenConnections == 0 }) {
					t.Error("SQLite connections outlived the fixture")
				}
			})
			startNATS := func(port int) {
				next, err := natsserver.NewServer(&natsserver.Options{Host: "127.0.0.1", Port: port, NoLog: true, NoSigs: true})
				if err != nil {
					t.Fatal(err)
				}
				ns = next
				go ns.Start()
				if !ns.ReadyForConnections(5 * time.Second) {
					t.Fatal("NATS did not start")
				}
			}
			startNATS(-1)
			natsURL := ns.ClientURL()
			parsedURL, err := url.Parse(natsURL)
			if err != nil {
				t.Fatal(err)
			}
			natsPort, err := strconv.Atoi(parsedURL.Port())
			if err != nil {
				t.Fatal(err)
			}
			owner, fp := testharness.FreshUserPub(t)
			if _, err := session.Create(db, "lab", "lab", fp, "pin-hash", time.Now().UTC()); err != nil {
				t.Fatal(err)
			}
			logger := slog.New(slog.NewTextHandler(io.Discard, nil))
			tunnelAddr := freePort(t)
			probeTLS := writeRestartTunnelCertificate(t, dir)
			_, publicPortText, err := net.SplitHostPort(freePort(t))
			if err != nil {
				t.Fatal(err)
			}
			bandLow, err := strconv.Atoi(publicPortText)
			if err != nil {
				t.Fatal(err)
			}
			bandLow = min(bandLow, 65520)
			cfg := broker.Config{
				NATSURL: natsURL, DB: db, Logger: logger, PublicHost: "127.0.0.1",
				PortBandLow: bandLow, PortBandHigh: bandLow + 15,
				TunnelControlAddr: tunnelAddr, TunnelPublicHost: "127.0.0.1", StableTunnelCertDir: dir,
				ReconcileInterval: 100 * time.Millisecond,
			}
			startBroker := func() {
				instance, err := broker.New(cfg)
				if err != nil {
					t.Fatal(err)
				}
				ctx, cancel := context.WithCancel(context.Background())
				brokerCancel, brokerDone = cancel, make(chan error, 1)
				go func() { brokerDone <- instance.Run(ctx) }()
				if !testharness.WaitFor(t, 5*time.Second, 20*time.Millisecond, func() bool {
					conn, err := tls.DialWithDialer(&net.Dialer{Timeout: 200 * time.Millisecond}, "tcp", tunnelAddr, probeTLS)
					if err != nil {
						return false
					}
					_ = conn.Close()
					return true
				}) {
					t.Fatal("broker did not serve the persisted tunnel certificate")
				}
			}
			startBroker()
			local := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { _, _ = io.WriteString(w, "same-agent-after-restart") }))
			t.Cleanup(local.Close)
			localURL, err := url.Parse(local.URL)
			if err != nil {
				t.Fatal(err)
			}
			localPort, err := strconv.Atoi(localURL.Port())
			if err != nil {
				t.Fatal(err)
			}
			actx, cancel := context.WithCancel(context.Background())
			agentCancel = cancel
			adapter := agent.NewTunnelExposeAdapter(tunnelAddr, "lab", "lab-1", logger)
			adapter.Start(actx)
			ag, err := agent.New(agent.Config{
				NATSURL: natsURL, SID: "lab", NID: "lab-1", Home: filepath.Join(dir, "agent"), Logger: logger,
				ExposeAdapter: adapter, HeartbeatInterval: 100 * time.Millisecond, RegisterTimeout: time.Second,
			})
			if err != nil {
				t.Fatal(err)
			}
			agentDone = make(chan error, 1)
			go func() { agentDone <- ag.Run(actx) }()
			testharness.WaitNodeOnline(t, db, "lab", "lab-1", 5*time.Second)
			ctl, err := nats.Connect(natsURL, nats.CustomInboxPrefix(auth.InboxPrefixFor(owner)), nats.MaxReconnects(-1), nats.ReconnectWait(100*time.Millisecond))
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(ctl.Close)

			// Retrying a different public port handles the reserve/release race
			// in a busy parallel matrix; it does not relax recovery assertions.
			var exposed proto.ExposeResp
			for port := bandLow; port <= bandLow+15; port++ {
				body, _ := json.Marshal(proto.ExposeReq{Name: "echo", LocalPort: localPort, RemotePort: port})
				response, err := ctl.Request(proto.SubjCmdBy("lab", owner, "lab-1", "expose"), body, 8*time.Second)
				if err != nil {
					t.Fatal(err)
				}
				if err := json.Unmarshal(response.Data, &exposed); err != nil {
					t.Fatal(err)
				}
				if exposed.Code == "" {
					break
				}
				if exposed.Code != "frpc_failed" || !strings.Contains(exposed.Error, "public_port_bind_failed") {
					t.Fatalf("expose refused: %+v", exposed)
				}
			}
			if exposed.Port == 0 || exposed.Code != "" {
				t.Fatalf("no available public port: %+v", exposed)
			}
			publicURL := fmt.Sprintf("http://127.0.0.1:%d/", exposed.Port)
			client := &http.Client{Timeout: 750 * time.Millisecond, Transport: &http.Transport{DisableKeepAlives: true}}
			t.Cleanup(client.CloseIdleConnections)
			payloadWorks := func() bool {
				resp, err := client.Get(publicURL)
				if err != nil {
					return false
				}
				defer func() { _ = resp.Body.Close() }()
				body, err := io.ReadAll(resp.Body)
				return err == nil && string(body) == "same-agent-after-restart"
			}
			if !testharness.WaitFor(t, 5*time.Second, 20*time.Millisecond, payloadWorks) {
				t.Fatal("baseline public payload did not reach the real agent")
			}
			var tokenHash string
			if err := db.QueryRow(`SELECT token_hash FROM port_allocations WHERE sid='lab' AND nid='lab-1' AND name='echo'`).Scan(&tokenHash); err != nil {
				t.Fatal(err)
			}
			initialServer := ns.ID()
			stopBroker()
			if !testharness.WaitFor(t, 3*time.Second, 20*time.Millisecond, func() bool { return !payloadWorks() }) {
				t.Fatal("stopped broker still served the public payload")
			}
			if err := db.Close(); err != nil {
				t.Fatal(err)
			}
			if !testharness.WaitFor(t, 5*time.Second, time.Millisecond, func() bool { return db.Stats().OpenConnections == 0 }) {
				t.Fatal("stopped broker still held the SQLite database")
			}
			db, err = storage.Open(filepath.Join(dir, "broker.db"))
			if err != nil {
				t.Fatal(err)
			}
			cfg.DB = db
			if restartNATS {
				ns.Shutdown()
				ns.WaitForShutdown()
				startNATS(natsPort)
				if ns.ID() == initialServer {
					t.Fatal("fixture did not replace the NATS server")
				}
				// Wait for the old agent to reach the replacement control transport
				// while the broker/tunnel service is still absent. No manual register,
				// replay, Open, or second agent.Run repairs it for the product.
				if !testharness.WaitFor(t, 10*time.Second, 20*time.Millisecond, func() bool {
					connections, err := ns.Connz(&natsserver.ConnzOptions{State: natsserver.ConnOpen})
					if err != nil {
						return false
					}
					for _, conn := range connections.Conns {
						if strings.HasPrefix(conn.Name, "tether-agent/") || strings.HasPrefix(conn.Name, "tether-agent:") {
							return true
						}
					}
					return false
				}) {
					t.Fatal("same agent did not reconnect to NATS before tunnel recovery")
				}
				if payloadWorks() {
					t.Fatal("the delayed tunnel was unexpectedly serving")
				}
			} else if ns.ID() != initialServer {
				t.Fatal("broker-only restart changed NATS")
			}
			started := time.Now()
			startBroker()
			// The production tunnel backoff caps at 30 seconds. A minute allows
			// one capped retry, its TLS/REGISTER work, and a loaded race runner;
			// it cannot accept the historical four-minute alleged outage.
			if !testharness.WaitFor(t, time.Minute, 50*time.Millisecond, payloadWorks) {
				t.Fatal("same agent did not autonomously recover the original expose")
			}
			select {
			case err := <-agentDone:
				agentDone <- err
				t.Fatalf("agent.Run exited during broker recovery: %v", err)
			default:
			}
			var recoveredHash string
			if err := db.QueryRow(`SELECT token_hash FROM port_allocations WHERE sid='lab' AND nid='lab-1' AND name='echo' AND port=? AND state='ALLOCATED'`, exposed.Port).Scan(&recoveredHash); err != nil {
				t.Fatal(err)
			}
			if recoveredHash != tokenHash {
				t.Fatal("recovery replaced the allocation instead of reconnecting it")
			}
			t.Logf("original public port %d recovered after %s with the same agent lifecycle, token, database, and TLS certificate", exposed.Port, time.Since(started))
		})
	}
}
