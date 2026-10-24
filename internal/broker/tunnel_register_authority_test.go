package broker

import (
	"bufio"
	"crypto/ed25519"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"math/big"
	"net"
	"runtime"
	"strconv"
	"testing"
	"time"

	"github.com/LinZiyang666/tether/internal/port"
	"github.com/LinZiyang666/tether/internal/testharness"
	"github.com/LinZiyang666/tether/internal/tunnel"
	"github.com/LinZiyang666/tether/internal/workload"
)

// origin: hot-update S5 internal review rv-authority-3
func TestFlowRegisterPreservesTransientAuthorityReasons(t *testing.T) {
	if runtime.GOOS != "linux" {
		t.Skip("selected v1 requires Linux flow ownership")
	}
	g, fd := runtime.NumGoroutine(), testharness.FDCount()
	for round := 0; round < 5; round++ {
		t.Run(fmt.Sprintf("round-%d", round), func(t *testing.T) { exerciseFlowRegisterAuthorityReasons(t) })
	}
	testharness.AssertNoGoroutineLeak(t, "REGISTER authority reasons", g)
	testharness.AssertNoFDLeak(t, "REGISTER authority reasons", fd, 0)
}

func exerciseFlowRegisterAuthorityReasons(t *testing.T) {
	t.Helper()
	cert, pin := registerAuthorityCertificate(t)
	roots := x509.NewCertPool()
	roots.AddCert(cert.Leaf)
	for _, tc := range []struct {
		name, fault, want string
		epoch             int64
	}{
		{"store-fault", "DROP TABLE proxy_subscribers", "try_again", 4},
		{"home-lag", "", "home_catching_up", 5},
		{"revoked", "UPDATE sessions SET proxy_enabled=0", "session_closed", 4},
		{"late-positive", "", "try_again", 4},
	} {
		t.Run(tc.name, func(t *testing.T) {
			db := openDB(t)
			b := &Broker{cfg: Config{DB: db, Logger: silentLogger()}, selfID: "home"}
			rawToken := "register-authority-token"
			seedHomedExpose(t, b, "lab", "lab-1", port.ProxyPortName, flowReaperFreePort(t), port.HashToken(rawToken), "home", 4)
			if _, err := db.Exec(`UPDATE sessions SET proxy_enabled=1; UPDATE nodes SET proxy_capable=1; INSERT INTO proxy_subscribers(sid,sub_id,name,token_hash,psk,cipher,created_by_fp) VALUES('lab','alice','alice','hash','password','aes-256-gcm','owner')`); err != nil {
				t.Fatal(err)
			}
			if tc.fault != "" {
				if _, err := db.Exec(tc.fault); err != nil {
					t.Fatal(err)
				}
			}
			addr := net.JoinHostPort("127.0.0.1", strconv.Itoa(flowReaperFreePort(t)))
			server := tunnel.NewServerWithCert(addr, "127.0.0.1", b.tunnelTokenLookup, &cert, silentLogger())
			defer server.Close()
			if tc.name == "late-positive" {
				// Keep real SQL authority, then hold its positive result past the
				// callback deadline to exercise REGISTER's independent expiry guard.
				if err := server.SetFlowConfig(tunnel.FlowConfig{
					InstanceID: "deadline-broker", BootID: "deadline-boot", OwnerGen: 1, Budget: &workload.ReplayBudget{},
					TokenLookup: func(req tunnel.AuthRequest) error {
						_, err := tunnelAuthorityLookup(b, req)
						if err != nil {
							return err
						}
						timer := time.NewTimer(time.Until(req.Deadline))
						defer timer.Stop()
						select {
						case <-timer.C:
							return nil
						case <-req.Stop:
							return errTunnelAuthorityRetry
						}
					},
					Authorize: func(req tunnel.AuthRequest) (tunnel.Authority, error) { return tunnelAuthorityLookup(b, req) },
				}); err != nil {
					t.Fatal(err)
				}
			} else if err := configureTunnelAuthority(b, server); err != nil {
				t.Fatal(err)
			}
			if err := server.Start(t.Context()); err != nil {
				t.Fatal(err)
			}
			// Verify the fixture chain and the matching production fingerprint.
			conn, err := tls.DialWithDialer(&net.Dialer{Timeout: tunnel.ResumeAttempt}, "tcp", addr, &tls.Config{
				MinVersion: tls.VersionTLS12, NextProtos: []string{tunnel.FlowALPN}, RootCAs: roots,
				VerifyConnection: func(cs tls.ConnectionState) error {
					if len(cs.PeerCertificates) != 1 || tunnel.CertFingerprint(cs.PeerCertificates[0]) != pin {
						return errors.New("certificate pin mismatch")
					}
					return nil
				},
			})
			if err != nil {
				t.Fatal(err)
			}
			defer func() { _ = conn.Close() }()
			if conn.ConnectionState().NegotiatedProtocol != tunnel.FlowALPN {
				t.Fatal("REGISTER did not select v1")
			}
			if err := conn.SetDeadline(time.Now().Add(tunnel.ResumeAttempt)); err != nil {
				t.Fatal(err)
			}
			var publicPort int
			if err := db.QueryRow(`SELECT port FROM port_allocations`).Scan(&publicPort); err != nil {
				t.Fatal(err)
			}
			if _, err := fmt.Fprintf(conn, "REGISTER lab lab-1 %d %s %d\n", publicPort, rawToken, tc.epoch); err != nil {
				t.Fatal(err)
			}
			reply, err := bufio.NewReader(conn).ReadString('\n')
			if err != nil || reply != "DENY "+tc.want+"\n" {
				t.Errorf("REGISTER refusal = %q/%v, want DENY %s", reply, err, tc.want)
			}
		})
	}
}

func registerAuthorityCertificate(t *testing.T) (tls.Certificate, string) {
	t.Helper()
	pub, key, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	template := &x509.Certificate{SerialNumber: big.NewInt(1), NotBefore: time.Now().Add(-time.Minute), NotAfter: time.Now().Add(time.Hour), KeyUsage: x509.KeyUsageDigitalSignature | x509.KeyUsageCertSign, IsCA: true, BasicConstraintsValid: true, IPAddresses: []net.IP{net.ParseIP("127.0.0.1")}}
	der, err := x509.CreateCertificate(rand.Reader, template, template, pub, key)
	if err != nil {
		t.Fatal(err)
	}
	leaf, err := x509.ParseCertificate(der)
	if err != nil {
		t.Fatal(err)
	}
	return tls.Certificate{Certificate: [][]byte{der}, PrivateKey: key, Leaf: leaf}, tunnel.CertFingerprint(leaf)
}
