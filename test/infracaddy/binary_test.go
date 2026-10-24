//go:build infracaddy_integration && linux && amd64

package infracaddy_test

// These tests use catalog-verified official archives supplied by P26, private
// high-port stock processes, real Unix admin and real NATS WSS connections.
// No host backend is substituted; root ingress and transaction composition are
// separate pending evidence, rather than inferred from this low-privilege cell.

import (
	"bufio"
	"bytes"
	"context"
	"crypto/rand"
	"crypto/sha256"
	"crypto/tls"
	"crypto/x509"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"github.com/LinZiyang666/tether/internal/caddyconfig"
	"github.com/LinZiyang666/tether/internal/controlbudget"
	c "github.com/LinZiyang666/tether/internal/infracontract"
	"github.com/LinZiyang666/tether/internal/infrahost"
	"github.com/LinZiyang666/tether/internal/infrapriv"
	"github.com/LinZiyang666/tether/internal/testharness"
	"github.com/LinZiyang666/tether/internal/upgradebudget"
	"github.com/LinZiyang666/tether/test/infraharness"
	"github.com/nats-io/nats-server/v2/server"
	"github.com/nats-io/nats.go"
	"io"
	"net"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"testing"
	"time"
)

const caddyArtifactDirectory = "TETHER_INFRA_CADDY_ARTIFACT_DIR"

// origin: S9 internal review INFRA-SUITE-SUPPLIES F1; hot-update S9 plan §5.4 / T64 / M10
func TestCaddyReloadKeepsOriginalBusiness(t *testing.T) {
	fixture := newCaddyFixture(t)
	defer fixture.close(t)
	main := fixture.start(t, "2.7.6", 8443, "revision-A")
	warm := fixture.start(t, "2.7.6", 0, "revision-A")
	cohort := fixture.cohort(t, main)
	defer cohort.close()
	cohort.check(t, "before-reload")
	before := main.identity
	beforeConfig := main.current(t)
	cohort.move(t, warm)
	main.requireDrained(t)
	cfg := main.prepare(t, "revision-B")
	if cfg.Digest() == beforeConfig {
		t.Fatal("reload fixture has no configuration change")
	}
	admin := main.admin(t)
	digest, err := admin.Load(t.Context(), cfg)
	_ = admin.Close()
	current := main.current(t)
	if err != nil || digest != cfg.Digest() || current != cfg.Digest() {
		t.Fatalf("stock reload did not apply the prepared digest: %v returned=%x current=%x want=%x", err, digest, current, cfg.Digest())
	}
	if observed := main.observe(t); observed != before {
		t.Fatal("reload replaced PID, start ticks, invocation or executable")
	}
	main.request(t, "/sub/current", "subscription-revision-B")
	main.request(t, "/manifest", "manifest-revision-B")
	main.requestHTTP2(t)
	cohort.check(t, "during-reload")
	cohort.move(t, main)
	warm.requireDrained(t)
	warm.retire(t)
	cohort.check(t, "after-reload")
	main.request(t, "/sub/current", "subscription-revision-B")
	fixture.requireRetired(t, warm)
	t.Logf("reload receipt: pid=%d ticks=%d running_sha=%x before_config=%x current_config=%x independent_ncs=%d",
		before.PID, before.StartTicks, before.ExeSHA256, beforeConfig, cfg.Digest(), len(cohort.rows))
}

// origin: hot-update S9 plan §5.3 / T65 / M09 / M12 / M17
func TestCaddyAdjacentBinaryCycle(t *testing.T) {
	fixture := newCaddyFixture(t)
	defer fixture.close(t)
	main := fixture.start(t, "2.7.6", 0, "revision-A")
	cohort := fixture.cohort(t, main)
	defer cohort.close()
	for hop, version := range []string{"2.7.5", "2.7.6", "2.7.5", "2.7.6"} {
		before := main
		warm := fixture.start(t, version, 0, "revision-A")
		cohort.move(t, warm)
		cohort.check(t, fmt.Sprintf("hop-%d-outbound", hop))
		before.requireDrained(t)
		before.retire(t)
		main = fixture.start(t, version, before.port, "revision-A")
		if main.identity.ExeSHA256 == before.identity.ExeSHA256 || main.identity.PID == before.identity.PID ||
			main.identity.InvocationID == before.identity.InvocationID {
			t.Fatal("binary hop did not replace the real server incarnation")
		}
		main.request(t, "/sub/current", "subscription-revision-A")
		cohort.move(t, main)
		cohort.check(t, fmt.Sprintf("hop-%d-return", hop))
		warm.requireDrained(t)
		warm.retire(t)
		fixture.requireRetired(t, before, warm)
		t.Logf("binary hop=%d version=%s archive_sha=%x binary_sha=%x running_sha=%x old_pid=%d main_pid=%d independent_ncs=%d",
			hop, version, main.artifact.ArchiveSHA256, main.artifact.BinarySHA256, main.identity.ExeSHA256,
			before.identity.PID, main.identity.PID, len(cohort.rows))
	}
	if main.artifact.Version != "2.7.6" {
		t.Fatal("cycle failed to restore the default pin")
	}
	t.Run("dead_main_is_not_ready_when_shadow_serves", checkDeadMainWithHealthyShadow)
}

// origin: hot-update S9 plan §12 P29 / §9.5
func TestInfraCaddyLifecycle(t *testing.T) {
	// Verify supply before taking the baseline: extraction and version probes
	// are fixture admission, while every round exercises actual service lifetime.
	fixture := newCaddyFixture(t)
	defer fixture.close(t)
	beforeG, beforeFD := runtime.NumGoroutine(), testharness.FDCount()
	for round := 0; round < 5; round++ {
		exerciseCaddyLifecycle(t, fixture)
	}
	testharness.AssertNoGoroutineLeak(t, "stock Caddy lifecycle", beforeG)
	testharness.AssertNoFDLeak(t, "stock Caddy lifecycle", beforeFD, 0)
}

func exerciseCaddyLifecycle(t *testing.T, fixture *caddyFixture) {
	t.Helper()
	main := fixture.start(t, "2.7.6", 0, "revision-A")
	cohort := fixture.cohort(t, main)
	cohort.check(t, "lifecycle")
	main.request(t, "/sub/current", "subscription-revision-A")
	cohort.close()
	main.requireDrained(t)
	main.retire(t)
	fixture.requireRetired(t, main)
}

type caddyFixture struct {
	root        string
	cert, key   string
	leaf        *x509.Certificate
	server      *server.Server
	responder   *nats.Conn
	upstream    string
	binaries    map[string]string
	artifacts   map[string]c.Artifact
	processes   []*stockCaddy
	serial      uint64
	launchLease *os.File
}

// origin: hot-update S9 plan §9.1 / §12 P29
func newCaddyFixture(t *testing.T) *caddyFixture {
	t.Helper()
	if os.Geteuid() == 0 {
		t.Fatal("NOT_RUN: stock Caddy integration requires a nonroot fixture UID")
	}
	supply := infraharness.StockSource("", os.Getenv(caddyArtifactDirectory))
	if !filepath.IsAbs(supply) || filepath.Clean(supply) != supply || supply == "/" || supply == os.TempDir() {
		t.Fatalf("NOT_RUN pending P26: set %s to the private directory containing both official Caddy archives and checksum files", caddyArtifactDirectory)
	}
	root, err := os.MkdirTemp("", "infra-caddy-")
	if err != nil {
		t.Fatal(err)
	}
	f := &caddyFixture{root: root, binaries: make(map[string]string), artifacts: make(map[string]c.Artifact)}
	// This fallback handles failed setup. Positive retirement assertions execute
	// before it and must never rely on fixture cleanup to make old owners vanish.
	t.Cleanup(func() { f.close(t) })
	f.cert, f.key, f.leaf = issueFixtureCertificate(t, root, 1)
	for _, version := range []string{"2.7.5", "2.7.6"} {
		staged, err := infraharness.PrepareStockArtifact(root, supply, c.CaddyBinary, version, fixtureDeadline(t, 30*time.Second), nil)
		if err != nil {
			t.Fatal(err)
		}
		path, artifact := staged.Binary, staged.Artifact
		probeCtx, cancelProbe := context.WithTimeout(t.Context(), upgradebudget.RequestLatency)
		cmd := exec.CommandContext(probeCtx, path, "version")
		cmd.Env = fixtureEnvironment(root, randomInvocation(t))
		var output bytes.Buffer
		cmd.Stdout = &output
		probe, err := infraharness.StartProcess(cmd)
		if err != nil {
			cancelProbe()
			t.Fatal(err)
		}
		probeErr := probe.Wait()
		cancelProbe()
		if probeErr != nil || !strings.HasPrefix(strings.TrimSpace(output.String()), "v"+version+" ") {
			t.Fatal("verified stock binary version probe failed")
		}
		f.binaries[version], f.artifacts[version] = path, artifact
	}
	opts := &server.Options{Host: "127.0.0.1", Port: -1, NoLog: true, NoSigs: true, AlwaysEnableNonce: true,
		Websocket: server.WebsocketOpts{Host: "127.0.0.1", Port: -1, NoTLS: true}}
	f.server, err = server.NewServer(opts)
	if err != nil {
		t.Fatal(err)
	}
	go f.server.Start()
	if !f.server.ReadyForConnections(upgradebudget.RequestLatency) {
		t.Fatal("embedded NATS did not become ready")
	}
	f.upstream = strings.TrimPrefix(f.server.WebsocketURL(), "ws://")
	f.responder, err = nats.Connect(f.server.ClientURL(), nats.NoReconnect())
	if err != nil {
		t.Fatal(err)
	}
	_, err = f.responder.Subscribe("infra.original", func(m *nats.Msg) { _ = m.Respond(m.Data) })
	if err != nil || f.responder.FlushTimeout(upgradebudget.RequestLatency) != nil {
		t.Fatal("original responder subscription did not flush")
	}
	return f
}

func fixtureDeadline(t *testing.T, duration time.Duration) c.Deadline {
	t.Helper()
	d, err := controlbudget.After(duration)
	if err != nil {
		t.Fatal(err)
	}
	return d
}

func randomInvocation(t *testing.T) string {
	t.Helper()
	var nonce [16]byte
	if _, err := rand.Read(nonce[:]); err != nil {
		t.Fatal(err)
	}
	return hex.EncodeToString(nonce[:])
}

func fixtureEnvironment(root, invocation string) []string {
	return []string{"HOME=" + root, "XDG_CONFIG_HOME=" + root, "XDG_DATA_HOME=" + root,
		"TMPDIR=" + root, "PATH=/usr/bin:/bin", "INVOCATION_ID=" + invocation}
}

type stockCaddy struct {
	fixture   *caddyFixture
	artifact  c.Artifact
	directory string
	socket    string
	port      int
	process   *infraharness.Process
	identity  c.Process
	joined    chan error
	retired   bool
}

func (f *caddyFixture) start(t *testing.T, version string, port int, revision string) *stockCaddy {
	return f.startConfig(t, version, port, revision, nil, nil)
}

func (f *caddyFixture) startConfig(t *testing.T, version string, port int, revision string,
	adjust func(*stockCaddy, map[string]any), ready func(*stockCaddy)) *stockCaddy {
	t.Helper()
	f.serial++
	dir := filepath.Join(f.root, fmt.Sprintf("invocation-%d", f.serial))
	if err := os.Mkdir(dir, 0700); err != nil {
		t.Fatal(err)
	}
	if port == 0 {
		listener, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			t.Fatal(err)
		}
		port = listener.Addr().(*net.TCPAddr).Port
		_ = listener.Close()
	}
	s := &stockCaddy{fixture: f, artifact: f.artifacts[version], directory: dir,
		socket: filepath.Join(dir, "admin.sock"), port: port, joined: make(chan error, 1)}
	cfg := s.prepare(t, revision)
	raw := cfg.Bytes()
	var err error
	if port != 8443 {
		// Only the unprivileged fixture listener is rebased. Keep an independent
		// digest of these actual bytes; never claim this as canonical nft ingress.
		object := configObject(t, raw)
		configServer(object)["listen"] = []string{net.JoinHostPort("127.0.0.1", strconv.Itoa(port))}
		var err error
		raw, err = json.Marshal(object)
		if err != nil {
			t.Fatal(err)
		}
	}
	if adjust != nil {
		object := configObject(t, raw)
		adjust(s, object)
		raw, err = json.Marshal(object)
		if err != nil {
			t.Fatal(err)
		}
	}
	path := filepath.Join(dir, "config.json")
	writeFixtureFile(t, path, raw, 0600)
	cmd := exec.Command(f.binaries[version], "run", "--config", path)
	cmd.Env = fixtureEnvironment(dir, randomInvocation(t))
	if f.launchLease != nil {
		cmd.ExtraFiles = []*os.File{f.launchLease}
	}
	s.process, err = infraharness.StartProcess(cmd)
	if err != nil {
		t.Fatal(err)
	}
	f.processes = append(f.processes, s)
	go func() { s.joined <- s.process.Wait() }()
	s.identity = s.observe(t)
	if s.identity.ExeSHA256 != s.artifact.BinarySHA256 {
		t.Fatal("running SHA differs from verified archive member")
	}
	limit := time.NewTimer(upgradebudget.RequestLatency)
	defer limit.Stop()
	tick := time.NewTicker(10 * time.Millisecond)
	defer tick.Stop()
	for {
		if info, err := os.Stat(s.socket); err == nil && info.Mode()&os.ModeSocket != 0 {
			admin := s.admin(t)
			digest, err := admin.Current(t.Context())
			_ = admin.Close()
			if err != nil || digest != c.Digest(sha256.Sum256(raw)) {
				t.Fatal("stock admin config readback differs from staged bytes")
			}
			if ready != nil {
				ready(s)
			}
			s.request(t, "/manifest", "manifest-"+revision)
			return s
		}
		select {
		case err := <-s.joined:
			s.retired = true
			t.Fatalf("stock Caddy exited before readiness: %v", err)
		case <-limit.C:
			t.Fatal("stock Caddy Unix admin readiness budget expired")
		case <-tick.C:
		}
	}
}

func (s *stockCaddy) prepare(t *testing.T, revision string) *caddyconfig.Config {
	t.Helper()
	raw := stockConfig(s.fixture.cert, s.fixture.key, filepath.Join(s.directory, "live-storage"), s.fixture.upstream, revision)
	cfg, err := caddyconfig.Prepare(raw, caddyconfig.Options{Host: fixtureHost, AdminSocket: s.socket,
		Shadow: true, Certificate: s.fixture.cert, Key: s.fixture.key})
	if err != nil {
		t.Fatal(err)
	}
	return cfg
}

func (s *stockCaddy) observe(t *testing.T) c.Process {
	t.Helper()
	p, err := infrahost.ReadProcess(int32(s.process.Identity().PID), fixtureDeadline(t, upgradebudget.RequestLatency))
	if err != nil {
		t.Fatal(err)
	}
	return p
}

// origin: hot-update S9 plan §5.1 / T64 / M10
func (s *stockCaddy) admin(t *testing.T) *caddyconfig.Admin {
	t.Helper()
	verify := func(conn *net.UnixConn, expected c.Process, uid uint32) error {
		peer, err := infrapriv.PeerCredentials(conn)
		if err != nil || peer.UID != uid || peer.PID != expected.PID || peer.BootID != expected.BootID || peer.StartTicks != expected.StartTicks {
			return caddyconfig.ErrAdmin
		}
		observed, err := infrahost.ReadProcess(peer.PID, fixtureDeadline(t, upgradebudget.RequestLatency))
		if err != nil || observed != expected {
			return caddyconfig.ErrAdmin
		}
		return nil
	}
	admin, err := caddyconfig.OpenAdmin(t.Context(), s.socket, s.identity, uint32(os.Geteuid()), verify)
	if err != nil {
		t.Fatal(err)
	}
	return admin
}

func (s *stockCaddy) current(t *testing.T) c.Digest {
	t.Helper()
	admin := s.admin(t)
	defer func() { _ = admin.Close() }()
	digest, err := admin.Current(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	return digest
}

func (s *stockCaddy) tlsConfig() *tls.Config {
	pool := x509.NewCertPool()
	pool.AddCert(s.fixture.leaf)
	return &tls.Config{RootCAs: pool, ServerName: fixtureHost, MinVersion: tls.VersionTLS12}
}

func (s *stockCaddy) request(t *testing.T, path, expected string) {
	t.Helper()
	conn, err := tls.DialWithDialer(&net.Dialer{Timeout: upgradebudget.RequestLatency}, "tcp",
		net.JoinHostPort("127.0.0.1", strconv.Itoa(s.port)), s.tlsConfig())
	if err != nil {
		t.Fatal("first raw TLS/HTTP attempt failed")
	}
	defer func() { _ = conn.Close() }()
	_ = conn.SetDeadline(time.Now().Add(upgradebudget.RequestLatency))
	if s.accepted(t, conn.LocalAddr().String()) == 0 || s.observe(t) != s.identity {
		t.Fatal("TLS success did not reach the specified accepted socket and process")
	}
	if _, err := fmt.Fprintf(conn, "GET %s HTTP/1.1\r\nHost: %s\r\nConnection: close\r\n\r\n", path, fixtureHost); err != nil {
		t.Fatal(err)
	}
	response, err := http.ReadResponse(bufio.NewReader(conn), &http.Request{Method: http.MethodGet})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = response.Body.Close() }()
	body, err := io.ReadAll(response.Body)
	if err != nil || response.StatusCode != http.StatusOK || string(body) != expected {
		t.Fatal("specific route response is incorrect")
	}
}

func (s *stockCaddy) requestHTTP2(t *testing.T) {
	t.Helper()
	transport := &http.Transport{TLSClientConfig: s.tlsConfig(), DisableKeepAlives: true, ForceAttemptHTTP2: true}
	defer transport.CloseIdleConnections()
	client := &http.Client{Transport: transport, Timeout: upgradebudget.RequestLatency}
	response, err := client.Get(fmt.Sprintf("https://127.0.0.1:%d/manifest", s.port))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = response.Body.Close() }()
	_, err = io.Copy(io.Discard, response.Body)
	if err != nil || response.ProtoMajor != 2 || response.StatusCode != http.StatusOK {
		t.Fatal("stock HTTP/2 response did not finish before retirement")
	}
}

func checkDeadMainWithHealthyShadow(t *testing.T) {
	t.Helper()
	fixture := newCaddyFixture(t)
	defer fixture.close(t)
	main := fixture.start(t, "2.7.6", 0, "revision-A")
	warm := fixture.start(t, "2.7.5", 0, "revision-A")
	cohort := fixture.cohort(t, warm)
	defer cohort.close()
	pin, err := infrahost.PinProcess(main.identity, fixtureDeadline(t, upgradebudget.RequestLatency))
	if err != nil {
		t.Fatal(err)
	}
	err = pin.Signal(c.SignalStop, fixtureDeadline(t, upgradebudget.RequestLatency), nil)
	_ = pin.Close()
	if err != nil {
		t.Fatal(err)
	}
	select {
	case err := <-main.joined:
		main.retired = true
		if err == nil {
			t.Fatal("main failure control did not deliver a real process fault")
		}
	case <-time.After(upgradebudget.RequestLatency):
		t.Fatal("main fault was not joined")
	}
	warm.request(t, "/manifest", "manifest-revision-A")
	cohort.check(t, "healthy-shadow-after-main-fault")
	if conn, err := tls.DialWithDialer(&net.Dialer{Timeout: upgradebudget.RequestLatency}, "tcp",
		net.JoinHostPort("127.0.0.1", strconv.Itoa(main.port)), main.tlsConfig()); err == nil {
		_ = conn.Close()
		t.Fatal("dead main was classified as ready from healthy shadow evidence")
	}
	fixture.requireRetired(t, main)
}

func (s *stockCaddy) retire(t *testing.T) {
	t.Helper()
	s.requireDrained(t)
	pin, err := infrahost.PinProcess(s.identity, fixtureDeadline(t, upgradebudget.RequestLatency))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = pin.Close() }()
	if err := pin.Signal(c.SignalGraceful, fixtureDeadline(t, upgradebudget.RequestLatency), nil); err != nil {
		t.Fatal(err)
	}
	select {
	case err := <-s.joined:
		s.retired = true
		if err != nil {
			t.Fatalf("drained stock Caddy did not exit naturally: %v", err)
		}
	case <-time.After(upgradebudget.RequestLatency):
		t.Fatal("drained stock Caddy exceeded graceful retirement budget")
	}
}

func (f *caddyFixture) requireRetired(t *testing.T, retired ...*stockCaddy) {
	t.Helper()
	for _, s := range retired {
		if !s.retired {
			t.Fatal("retirement was inferred before process Wait")
		}
		exists, err := infraharness.ProcessExists(s.process.Identity())
		if err != nil || exists {
			t.Fatal("old process incarnation survived product retirement")
		}
		if conn, err := net.DialTimeout("unix", s.socket, 100*time.Millisecond); err == nil {
			_ = conn.Close()
			t.Fatal("retired invocation retains a usable admin listener")
		}
	}
}

func (f *caddyFixture) close(t *testing.T) {
	t.Helper()
	for _, s := range f.processes {
		if !s.retired {
			// Fallback failure cleanup only. The supervisor already owns Wait;
			// kill the exact incarnation and collect its original join receipt.
			_ = s.process.Stop()
			select {
			case <-s.joined:
				s.retired = true
			case <-time.After(upgradebudget.RequestLatency):
				t.Error("fixture process supervisor did not join")
			}
		}
	}
	if f.responder != nil {
		f.responder.Close()
		f.responder = nil
	}
	if f.server != nil {
		f.server.Shutdown()
		f.server.WaitForShutdown()
		f.server = nil
	}
	if f.root != "" {
		_ = os.RemoveAll(f.root)
		f.root = ""
	}
}
