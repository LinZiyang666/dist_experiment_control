//go:build linux || darwin

// realstack_harness_test.go — a REAL `tether` binary as ctl and as agent
// subprocess, an embedded nats-server that can be restarted on the same
// port, and an in-process broker that can be stopped and restarted on the
// same DB. This is the layer between the stubbed execclient tests and the
// deploy-tier simcluster drills: the real CLI, the real agent process, the
// real NATS wire, the real auth_callout when asked for.
//
// Two things here are load-bearing and easy to get wrong:
//
//   - The ctl is NEVER started with TETHER_DEV_NO_AUTH. Without an nkey the
//     ctl has no private inbox and execclient.Run returns ErrUnsupported
//     before it sends a byte, so every "reliable path" test would silently
//     run the legacy exec path and stay green. In anonymous mode the server
//     is started with AlwaysEnableNonce so an nkey CONNECT is accepted; the
//     callout mode uses the real auth_callout. Each reliable test also
//     asserts the agent logged `reliable=true` exactly once (non-emptiness).
//   - The broker keeps the product's StaleAfter/OfflineAfter defaults. A
//     real agent heartbeats every 5 s; the 300 ms/900 ms values other suites
//     use would mark the node OFFLINE across any restart under test.
//
// origin: docs/reviews/hot-update-reliable-exec-plan.md §4.1.
package cli_e2e_test

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"github.com/LinZiyang666/tether/internal/auth"
	"github.com/LinZiyang666/tether/internal/broker"
	"github.com/LinZiyang666/tether/internal/cli"
	"github.com/LinZiyang666/tether/internal/proto"
	"github.com/LinZiyang666/tether/internal/session"
	"github.com/LinZiyang666/tether/internal/storage"
	"github.com/LinZiyang666/tether/internal/testharness"
	"github.com/nats-io/jwt/v2"
	natsserver "github.com/nats-io/nats-server/v2/server"
	natstest "github.com/nats-io/nats-server/v2/test"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nkeys"
)

var (
	tetherBinOnce   sync.Once
	tetherBinPath   string
	tetherBinDir    string
	tetherBinErr    error
	sharedTetherBin string
	// raceEnabled is set by realstack_race_test.go (//go:build race): when the
	// harness process runs under -race, the product binary it builds is
	// instrumented too, so the ctl and agent under fault injection are not
	// exempt from the detector the matrix row claims (S1 review E4).
	raceEnabled bool
)

// TestMain removes the once-per-process build directory (28 MB per run; 47
// of them were found in /tmp — S1 review E10). Every test still runs
// exactly as before.
func TestMain(m *testing.M) {
	flag.Parse()
	// Go tools launch a detached telemetry service even for go env. A fresh
	// isolated HOME can trigger it during a nested build; it then correctly
	// fails the supervisor's descendant census. Confine and disable telemetry
	// before every build/probe while retaining strict process leak detection.
	// origin: S10 internal review POLISH-B F2 / plan L15
	telemetry, err := os.MkdirTemp("", "tcli-telemetry-")
	if err == nil {
		err = os.WriteFile(filepath.Join(telemetry, "mode"), []byte("off"), 0600)
	}
	if err == nil {
		err = os.Setenv("TEST_TELEMETRY_DIR", telemetry)
	}
	if err != nil {
		_, _ = fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	parallelExplicit := false
	flag.Visit(func(f *flag.Flag) {
		if f.Name == "test.parallel" {
			parallelExplicit = true
		}
	})
	// These independent stacks mostly wait on real watchdog, lease and carrier
	// deadlines. A two-CPU worker can overlap those waits without changing its
	// GOMAXPROCS; an explicit caller limit still takes precedence.
	// origin: hot-update S4 plan §8.4; CLI-LATENCY two-CPU measurements.
	// The added transfer settlement suites keep several slots occupied for
	// minutes. Twelve bounded slots overlap those real waits without making a
	// late queued lifecycle consume the default package's remaining budget.
	// origin: hot-update S10 plan L17 / §11.1 / POLISH-B
	if !parallelExplicit {
		if err := flag.Set("test.parallel", "12"); err != nil {
			_, _ = fmt.Fprintln(os.Stderr, err)
			os.Exit(1)
		}
	}
	// The parent's race runtime has already read GORACE. Remove only its
	// descendants' one-second exit padding: every owner still joins/reaps, and
	// child instrumentation, diagnostics and failing exit codes remain enabled.
	// origin: hot-update S4 plan §8.4; CLI-LATENCY phase profile.
	if err := os.Setenv("GORACE", strings.TrimSpace(os.Getenv("GORACE")+" atexit_sleep_ms=0")); err != nil {
		_, _ = fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	sharedTetherBin = os.Getenv("_TETHER_CLI_TEST_BINARY")
	if err := os.Unsetenv("_TETHER_CLI_TEST_BINARY"); err != nil {
		_, _ = fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	code := m.Run()
	if tetherBinDir != "" {
		_ = os.RemoveAll(tetherBinDir)
	}
	if err := os.RemoveAll(telemetry); err != nil {
		_, _ = fmt.Fprintln(os.Stderr, err)
		code = 1
	}
	os.Exit(code)
}

// tetherBuildCmd is the exact `go build` the harness runs. The literal
// "build" stays in the exec.Command call so the matrix-only fork gate
// (test/e2e/parallel TestOnlyTheMatrixForksGoTest) can prove this command
// never runs a test; -race is spliced into Args afterwards when the harness
// itself runs under -race. Exposed for the receipt test that pins both.
func tetherBuildCmd(out string) *exec.Cmd {
	cmd := exec.Command("go", "build", "-trimpath", "-o", out, "./cmd/tether")
	if raceEnabled {
		cmd.Args = append(cmd.Args[:2:2], append([]string{"-race"}, cmd.Args[2:]...)...)
	}
	return cmd
}

// buildTether builds the product binary once per test process. A mutation
// of ctl/agent code for these tests must reach this nested `go build`: pass
// it as GOFLAGS=-overlay=<json> (inherited from the environment), not as a
// `go test -overlay` flag, which only patches the harness process (S1
// review e2e-V3; evidence §12).
func buildTether(t *testing.T) string {
	t.Helper()
	tetherBinOnce.Do(func() {
		root, err := testharness.Output(exec.Command("git", "rev-parse", "--show-toplevel"))
		if err != nil {
			tetherBinErr = fmt.Errorf("git rev-parse: %w", err)
			return
		}
		dir, err := os.MkdirTemp("", "tether-realstack-")
		if err != nil {
			tetherBinErr = err
			return
		}
		tetherBinDir = dir
		// Isolated tests borrow the immutable product build from their parent.
		// Their variant/S3 caches remain private, and only the parent removes
		// the shared binary after every subprocess has been joined.
		// origin: hot-update S4 plan §8.4; CLI-LATENCY immutable setup.
		if sharedTetherBin != "" {
			tetherBinPath = sharedTetherBin
			return
		}
		tetherBinPath = filepath.Join(dir, "tether")
		cmd := tetherBuildCmd(tetherBinPath)
		cmd.Dir = strings.TrimSpace(string(root))
		if out, err := testharness.CombinedOutput(cmd); err != nil {
			tetherBinErr = fmt.Errorf("go build ./cmd/tether: %w\n%s", err, out)
		}
	})
	if tetherBinErr != nil {
		t.Fatal(tetherBinErr)
	}
	if testharness.ReleaseBudgetRun(t) {
		return buildReleaseTether(t)
	}
	return tetherBinPath
}

// ctlEnv is the environment every real ctl/agent subprocess gets: the
// developer's shell minus the variables that would redirect the binary away
// from the stack under test. TETHER_DEV_NO_AUTH must be empty (see the file
// header); TETHER_SESSION and TETHER_NATS_URL are the operator's fleet
// defaults and made every ctl in this suite target the wrong session when
// exported (S1 review e2e-V2).
func ctlEnv() []string {
	return append(os.Environ(), "TETHER_DEV_NO_AUTH=", "TETHER_SESSION=", "TETHER_NATS_URL=")
}

// realStack is one test's stack. Fields are owned by the test goroutine;
// the helpers that touch the server/broker from a fault-injection goroutine
// take mu.
type realStack struct {
	t   *testing.T
	bin string
	// Upgrade tests install over a private copy; ctl keeps using the build
	// artifact. Extra agent flags carry the loopback artifact allowlist.
	// origin: hot-update S3 plan §7 T16.
	agentBin    string
	agentArgs   []string
	agentURL    string
	beforeAgent func(*realStack)
	callout     bool
	port        int
	url         string
	db          *sql.DB
	sid, nid    string
	pin         string
	ctlHome     string
	agentHome   string
	ctlID       *cli.Identity

	brokerOpts []func(*broker.Config)
	brokerBin  string
	diskDB     bool
	dbPath     string
	// legacyBrokerSurface models a v0.6.0 broker for the N-1 quadrant "new
	// ctl × old broker" (plan §2.8 Q1): the server DENIES the broker's
	// subscription to `…process-resume.req` (that subscription did not exist
	// before S1), so the ctl's probe gets a real no-responders 503 from the
	// server and falls back to the legacy exec verb, which the broker still
	// serves unchanged. Callout mode only (permissions ride on the broker's
	// nkey user). No byte is forged and no product code is patched.
	legacyBrokerSurface bool
	// authTimeout (seconds, callout mode) is the server's AUTH_TIMEOUT. The
	// default 2 s ties with nats.go's 2 s connect deadline, so a black window
	// with no callout responder surfaces on the client as `i/o timeout` —
	// which never reaches nats.go's auth-error abort logic at all. A test
	// that wants to exercise IgnoreAuthErrorAbort must make the server refuse
	// FIRST: a 0.5 s timeout turns every attempt into `-ERR Authorization
	// Violation` well inside the client's deadline (S1 review E2).
	authTimeout float64

	mu          sync.Mutex
	ns          *natsserver.Server
	brokerStop  func()
	brokerSeed  []byte
	accountSeed []byte
	agent       *exec.Cmd
	agentOut    string
	agentStdout string
	agentDone   chan error
}

const realStackPIN = "realstack-pin"

var (
	realStackPINOnce sync.Once
	realStackPINHash string
	realStackPINErr  error
)

// newRealStack boots NATS (restartable), the in-process broker, a ctl home
// with an admitted identity and a real agent subprocess, and waits for the
// node to be ONLINE. callout selects the real auth_callout wiring.
func newRealStack(t *testing.T, callout bool, brokerOpts ...func(*broker.Config)) *realStack {
	t.Helper()
	return bootRealStack(t, &realStack{callout: callout, brokerOpts: brokerOpts})
}

// newRealStackLegacyBroker boots a callout stack whose broker has the
// v0.6.0 subscription surface for `process-resume` (see legacyBrokerSurface).
func newRealStackLegacyBroker(t *testing.T) *realStack {
	t.Helper()
	return bootRealStack(t, &realStack{callout: true, legacyBrokerSurface: true})
}

// newRealStackFastAuthTimeout boots a callout stack whose restarted server
// refuses an unanswered CONNECT after authTimeout seconds (see realStack.authTimeout).
func newRealStackFastAuthTimeout(t *testing.T, authTimeout float64) *realStack {
	t.Helper()
	// The short timeout creates the reconnect fault, after ordinary initial PIN
	// authentication has completed. Applying it to bootstrap makes race/CPU
	// contention abort that first authentication before there is any live task
	// to test; the fault window's timeout and refusal assertions stay exact.
	// origin: S10 internal review POLISH-B F3 / plan §11.1
	s := bootRealStack(t, &realStack{callout: true})
	s.authTimeout = authTimeout
	return s
}

func bootRealStack(t *testing.T, s *realStack) *realStack {
	t.Helper()
	s.t, s.bin, s.sid, s.nid, s.pin = t, buildTether(t), "lab", "agt1", realStackPIN
	s.ctlHome, s.agentHome = t.TempDir(), t.TempDir()
	if s.diskDB {
		s.dbPath = filepath.Join(t.TempDir(), "stack.db")
		var err error
		s.db, err = storage.Open(s.dbPath)
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = s.db.Close() })
	} else {
		s.db = openDB(t)
	}
	callout := s.callout
	s.port = reserveRealStackPort(t)
	s.url = fmt.Sprintf("nats://127.0.0.1:%d", s.port)
	if callout {
		s.brokerSeed, s.accountSeed = mintRealStackSeeds(t)
	}
	s.startNATS()
	t.Cleanup(s.stopNATS)
	// ctl identity: generated into the home the real binary will read.
	id, err := cli.EnsureIdentity(s.ctlHome)
	if err != nil {
		t.Fatal(err)
	}
	s.ctlID = id
	if err := os.WriteFile(filepath.Join(s.ctlHome, "current_session"), []byte(s.sid+"\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	// The same fixture PIN is used by every private stack. Its expensive
	// salted Argon2 hash is immutable setup, not the authentication behavior
	// under test; each stack still performs real PIN verification at login.
	// origin: hot-update S4 plan §8.4; CLI-LATENCY per-round setup profile.
	realStackPINOnce.Do(func() { realStackPINHash, realStackPINErr = auth.HashPIN(realStackPIN) })
	if realStackPINErr != nil {
		t.Fatal(realStackPINErr)
	}
	if _, err := session.Create(s.db, s.sid, s.sid, id.Fingerprint, realStackPINHash, time.Now().UTC()); err != nil {
		t.Fatal(err)
	}
	s.startBroker()
	t.Cleanup(s.stopBroker)
	if s.beforeAgent != nil {
		s.beforeAgent(s)
	}
	s.startAgent()
	t.Cleanup(s.stopAgent)
	t.Cleanup(func() {
		if t.Failed() {
			body, _ := os.ReadFile(s.agentOut)
			stdout, _ := os.ReadFile(s.agentStdout)
			t.Logf("agent log:\n%s\nagent stdout:\n%s", body, stdout)
		}
	})
	testharness.WaitNodeOnline(t, s.db, s.sid, s.nid, 20*time.Second)
	// Register makes the DB row ONLINE before the agent installs its command
	// subscription. A probe forwarded in that gap is lost, even if Subscribe
	// runs immediately afterwards; concurrent two-CPU fixtures expose the gap.
	// Observe the server's actual intake before starting the test's RPC budget.
	// origin: hot-update S4 plan §8.3 / T36–T39; CLI-LATENCY startup profile.
	if !testharnessWait(10*time.Second, func() bool {
		connz, err := s.server().Connz(&natsserver.ConnzOptions{State: natsserver.ConnOpen, Subscriptions: true})
		if err != nil {
			return false
		}
		for _, conn := range connz.Conns {
			if conn.Name != cli.AgentName(s.sid, s.nid) {
				continue
			}
			for _, subject := range conn.Subs {
				if subject == proto.SubjCmdForwarded(s.sid, s.nid, "*") {
					return true
				}
			}
		}
		return false
	}) {
		t.Fatal("agent did not install its forwarded-command subscription")
	}
	return s
}

func reserveRealStackPort(t *testing.T) int {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	port := l.Addr().(*net.TCPAddr).Port
	_ = l.Close()
	return port
}

func mintRealStackSeeds(t *testing.T) (brokerSeed, accountSeed []byte) {
	t.Helper()
	brokerKp, err := nkeys.CreateUser()
	if err != nil {
		t.Fatal(err)
	}
	rawBrokerSeed, _ := brokerKp.Seed()
	brokerSeed = append([]byte(nil), rawBrokerSeed...)
	brokerKp.Wipe()
	accountKp, err := nkeys.CreateAccount()
	if err != nil {
		t.Fatal(err)
	}
	rawAcct, _ := accountKp.Seed()
	accountSeed = append([]byte(nil), rawAcct...)
	accountKp.Wipe()
	return brokerSeed, accountSeed
}

func realStackServerPerms(p jwt.Permissions) *natsserver.Permissions {
	return &natsserver.Permissions{
		Publish:   &natsserver.SubjectPermission{Allow: p.Pub.Allow, Deny: p.Pub.Deny},
		Subscribe: &natsserver.SubjectPermission{Allow: p.Sub.Allow, Deny: p.Sub.Deny},
	}
}

// startNATS starts (or restarts) the embedded server on the stack's port.
func (s *realStack) startNATS() {
	s.t.Helper()
	opts := natstest.DefaultTestOptions
	opts.Host, opts.Port = "127.0.0.1", s.port
	if s.callout {
		brokerKp, err := nkeys.FromSeed(s.brokerSeed)
		if err != nil {
			s.t.Fatal(err)
		}
		brokerPub, _ := brokerKp.PublicKey()
		brokerKp.Wipe()
		accountKp, err := nkeys.FromSeed(s.accountSeed)
		if err != nil {
			s.t.Fatal(err)
		}
		accountPub, _ := accountKp.PublicKey()
		accountKp.Wipe()
		perms := realStackServerPerms(auth.PermissionsForBroker())
		if s.legacyBrokerSurface {
			// A v0.6.0 broker never subscribed to this subject; deny it so the
			// server answers the ctl's probe with 503 exactly as an old broker
			// would (by absence). The broker's SUB is refused asynchronously
			// (`Permissions Violation for Subscription`) and it keeps running.
			perms.Subscribe.Deny = append(perms.Subscribe.Deny,
				proto.SubjectPrefix+".s.*.cmd.by.*.node.*."+proto.ProcessResumeVerb+".req")
		}
		opts.Nkeys = []*natsserver.NkeyUser{{Nkey: brokerPub, Permissions: perms}}
		opts.AuthCallout = &natsserver.AuthCallout{Issuer: accountPub, Account: "$G", AuthUsers: []string{brokerPub}}
		if s.authTimeout > 0 {
			opts.AuthTimeout = s.authTimeout
		}
	} else {
		// The real ctl and agent CONNECT with nkeys; an anonymous server only
		// accepts that when it always issues a nonce. Without this the
		// binary gets `nkeys not supported`.
		opts.AlwaysEnableNonce = true
	}
	ns := natstest.RunServer(&opts)
	if !ns.ReadyForConnections(5 * time.Second) {
		ns.Shutdown()
		s.t.Fatal("embedded nats-server not ready")
	}
	s.mu.Lock()
	s.ns = ns
	s.mu.Unlock()
}

func (s *realStack) stopNATS() {
	s.mu.Lock()
	ns := s.ns
	s.ns = nil
	s.mu.Unlock()
	if ns != nil {
		ns.Shutdown()
		ns.WaitForShutdown()
	}
}

// restartNATS stops the server, waits gap, and starts it again on the same
// port. Every client (ctl, agent, broker) sees a real disconnect and
// reconnects on its own.
func (s *realStack) restartNATS(gap time.Duration) {
	s.stopNATS()
	time.Sleep(gap)
	s.startNATS()
}

func (s *realStack) server() *natsserver.Server {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.ns
}

// startBroker runs the in-process broker on the stack's DB with the product
// defaults for the node liveness windows.
func (s *realStack) startBroker() {
	s.t.Helper()
	if s.brokerBin != "" {
		s.startHistoricalBroker()
		return
	}
	cfg := broker.Config{
		NATSURL:                  s.url,
		DB:                       s.db,
		Logger:                   silentLog(),
		ReconcileInterval:        200 * time.Millisecond,
		PublicHost:               "test.local",
		PortBandLow:              14000,
		PortBandHigh:             14002,
		PortRevokeAfterDur:       300 * time.Millisecond,
		ExposeForwardTimeoutDur:  1 * time.Second,
		UpgradeForwardTimeoutDur: 5 * time.Second,
	}
	if s.callout {
		cfg.AuthCallout = &broker.AuthCalloutConfig{BrokerNkeySeed: s.brokerSeed, AccountSeed: s.accountSeed}
	}
	for _, o := range s.brokerOpts {
		o(&cfg)
	}
	b, err := broker.New(cfg)
	if err != nil {
		s.t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background()) // ctx-root: stack broker lifetime ends in stopBroker
	done := make(chan error, 1)
	go func() { done <- b.Run(ctx) }()
	if s.callout {
		s.waitCalloutReady()
	} else {
		// Ready = the broker's command subscriptions exist on the server
		// (read from the server's connection table), which is what every
		// ctl request in these tests depends on — not merely "NATS accepts
		// connections".
		if !testharnessWait(5*time.Second, func() bool { return s.brokerHasSubscription(".exec.req") }) {
			s.t.Fatal("broker did not subscribe to its command subjects in time")
		}
	}
	s.mu.Lock()
	s.brokerStop = func() {
		cancel()
		select {
		case <-done:
		case <-time.After(5 * time.Second):
			s.t.Error("broker did not exit")
			return
		}
		// Run returns after starting nc.Drain, which is asynchronous: its
		// drainConnection goroutine flushes until the server confirms. If
		// stopNATS (the next cleanup) shuts the server down first, that
		// goroutine waits out the whole 5 s flush timeout and every
		// leak-gated test using a real stack reports it as a leak. Drain
		// unsubscribes before it flushes, so a vanished subscription proves
		// nothing; wait until every broker connection has left the server's
		// table.
		// origin: hot-update S4 make-test leak gates (TestRunReliableLocalCancellationRestoresTerminal).
		if !testharnessWait(5*time.Second, func() bool { _, found := s.brokerSubscriptionState(""); return !found }) {
			s.t.Error("broker connection did not finish draining")
		}
	}
	s.mu.Unlock()
}

func (s *realStack) stopBroker() {
	s.mu.Lock()
	stop := s.brokerStop
	s.brokerStop = nil
	s.mu.Unlock()
	if stop != nil {
		stop()
	}
}

// waitCalloutReady probes with a throwaway unactivated nkey until the
// broker answers auth_callout.
func (s *realStack) waitCalloutReady() {
	s.t.Helper()
	probeKp, _ := nkeys.CreateUser()
	probePub, _ := probeKp.PublicKey()
	rawSeed, _ := probeKp.Seed()
	seed := append([]byte(nil), rawSeed...)
	probeKp.Wipe()
	sigCB := func(nonce []byte) ([]byte, error) {
		kp, err := nkeys.FromSeed(seed)
		if err != nil {
			return nil, err
		}
		defer kp.Wipe()
		return kp.Sign(nonce)
	}
	deadline := time.Now().Add(10 * time.Second)
	for {
		nc, err := nats.Connect(s.url, nats.Nkey(probePub, sigCB), nats.Name(cli.CtlNameUnactivated), nats.Timeout(300*time.Millisecond))
		if err == nil {
			nc.Close()
			return
		}
		if time.Now().After(deadline) {
			s.t.Fatalf("broker auth_callout not ready: %v", err)
		}
		time.Sleep(50 * time.Millisecond)
	}
}

// startAgent launches the real agent subprocess in its own process group,
// logging to a file the tests grep (agentLogCount).
func (s *realStack) startAgent() {
	s.t.Helper()
	s.agentOut = filepath.Join(s.t.TempDir(), "agent.log")
	// The agent daemon has no --home flag: its home is $TETHER_HOME (the
	// systemd unit passes it the same way).
	agentURL := s.url
	if s.agentURL != "" {
		agentURL = s.agentURL
	}
	args := []string{"agent", "--session", s.sid, "--nid", s.nid, "--nats-url", agentURL,
		"--tunnel-addr", "", "--log-file", s.agentOut, "--log-level", "info"}
	if s.callout {
		args = append(args, "--pin", s.pin)
	}
	args = append(args, s.agentArgs...)
	bin := s.agentBin
	if bin == "" {
		bin = s.bin
	}
	cmd := exec.Command(bin, args...)
	cmd.Dir = s.agentHome
	// The lease survives worker-thread exec; the target keeps its own group
	// and PID for signal and upgrade identity assertions.
	// origin: hot-update S6 plan §9.3 / FIX-ORPHAN CLI fixtures; S1 review E10.
	cmd.SysProcAttr = agentProcessAttributes()
	s.agentStdout = filepath.Join(s.t.TempDir(), "agent.stdout")
	stdout, err := os.Create(s.agentStdout)
	if err != nil {
		s.t.Fatal(err)
	}
	cmd.Stdout, cmd.Stderr = stdout, stdout
	cmd.Env = append(ctlEnv(), "TETHER_HOME="+s.agentHome) // never anonymous
	wait, err := testharness.StartProcess(cmd)
	_ = stdout.Close()
	if err != nil {
		s.t.Fatal(err)
	}
	done := make(chan error, 1)
	go func() { done <- wait() }()
	s.mu.Lock()
	s.agent, s.agentDone = cmd, done
	s.mu.Unlock()
}

func (s *realStack) stopAgent() {
	s.mu.Lock()
	cmd, done := s.agent, s.agentDone
	s.agent = nil
	s.mu.Unlock()
	if cmd == nil || cmd.Process == nil {
		return
	}
	_ = cmd.Process.Signal(syscall.SIGTERM)
	select {
	case <-done:
	case <-time.After(10 * time.Second):
		_ = syscall.Kill(-cmd.Process.Pid, syscall.SIGKILL)
		<-done
	}
	// origin: hot-update S5 PROC-LEAK. Wait can finish before group members.
	_ = syscall.Kill(-cmd.Process.Pid, syscall.SIGKILL)
}

// agentPID is the OS pid of the agent daemon; a test proves the daemon did
// not restart by reading it before and after a fault.
func (s *realStack) agentPID() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.agent == nil || s.agent.Process == nil {
		return 0
	}
	return s.agent.Process.Pid
}

// agentLogCount counts agent log lines matching re.
func (s *realStack) agentLogCount(re string) int {
	body, err := os.ReadFile(s.agentOut)
	if err != nil {
		return 0
	}
	return len(regexp.MustCompile(re).FindAll(body, -1))
}

// agentConnID reports the server-side client id of the agent's connection
// and whether exactly one such connection exists right now.
func (s *realStack) agentConnID() (uint64, bool) {
	ns := s.server()
	if ns == nil {
		return 0, false
	}
	connz, err := ns.Connz(&natsserver.ConnzOptions{State: natsserver.ConnOpen})
	if err != nil {
		return 0, false
	}
	want := cli.AgentName(s.sid, s.nid)
	var ids []uint64
	for _, c := range connz.Conns {
		if c.Name == want {
			ids = append(ids, c.Cid)
		}
	}
	if len(ids) != 1 {
		return 0, false
	}
	return ids[0], true
}

// kickAgentConn disconnects the agent's NATS connection server-side (the
// agent's own reconnect logic then rebuilds it) and returns the client id
// it kicked, so a caller can wait for a DIFFERENT one before the next
// fault. Exactly one connection must match, or the fault was not injected.
func (s *realStack) kickAgentConn() uint64 {
	s.t.Helper()
	cid, ok := s.agentConnID()
	if !ok {
		s.t.Fatalf("expected exactly one agent connection named %q", cli.AgentName(s.sid, s.nid))
	}
	if err := s.server().DisconnectClientByID(cid); err != nil {
		s.t.Fatal(err)
	}
	return cid
}

// brokerSubscribes reports whether the broker's live NATS connection holds a
// subscription whose subject contains needle — read from the server's own
// connection table, so "the old-broker surface really was installed" is a
// server fact, not an assumption (the injection self-proof).
func (s *realStack) brokerSubscribes(needle string) bool {
	s.t.Helper()
	has, found := s.brokerSubscriptionState(needle)
	if !found {
		s.t.Fatal("no broker connection named tetherd on the server")
	}
	return has
}

// brokerHasSubscription is the non-fatal form for readiness polling: false
// while the broker connection or the subscription is not there yet.
func (s *realStack) brokerHasSubscription(needle string) bool {
	has, found := s.brokerSubscriptionState(needle)
	return found && has
}

func (s *realStack) brokerSubscriptionState(needle string) (has, found bool) {
	ns := s.server()
	if ns == nil {
		return false, false
	}
	connz, err := ns.Connz(&natsserver.ConnzOptions{State: natsserver.ConnOpen, Subscriptions: true})
	if err != nil {
		return false, false
	}
	for _, c := range connz.Conns {
		if c.Name != "tetherd" {
			continue
		}
		found = true
		for _, sub := range c.Subs {
			if strings.Contains(sub, needle) {
				return true, true
			}
		}
	}
	return false, found
}

// ctlResult is one real `tether exec` invocation.
type ctlResult struct {
	rc      int
	stdout  string
	stderr  string
	elapsed time.Duration
}

// exec runs the real ctl binary against the stack. extra are flags placed
// before the node argument (e.g. "--timeout", "2s", "--stdin"); argv is the
// remote command. url lets a test route the ctl through a relay.
func (s *realStack) exec(ctx context.Context, url string, stdin []byte, extra []string, argv ...string) ctlResult {
	s.t.Helper()
	args := append([]string{"exec", "--home", s.ctlHome, "--nats-url", url}, extra...)
	args = append(args, s.nid, "--")
	args = append(args, argv...)
	cmd := exec.CommandContext(ctx, s.bin, args...)
	cmd.Env = ctlEnv()
	if stdin != nil {
		cmd.Stdin = bytes.NewReader(stdin)
	}
	var out, errOut bytes.Buffer
	cmd.Stdout, cmd.Stderr = &out, &errOut
	start := time.Now()
	err := testharness.RunProcess(cmd)
	res := ctlResult{stdout: out.String(), stderr: errOut.String(), elapsed: time.Since(start)}
	var exitErr *exec.ExitError
	switch {
	case err == nil:
		res.rc = 0
	case errorsAs(err, &exitErr):
		if st, ok := exitErr.Sys().(syscall.WaitStatus); ok && st.Signaled() {
			res.rc = 128 + int(st.Signal())
		} else {
			res.rc = exitErr.ExitCode()
		}
	default:
		s.t.Fatalf("run ctl: %v", err)
	}
	return res
}

// ctlCommand runs any other ctl verb (ps, session rm, ...) and returns rc + output.
func (s *realStack) ctlCommand(ctx context.Context, args ...string) ctlResult {
	s.t.Helper()
	full := append([]string{args[0], "--home", s.ctlHome, "--nats-url", s.url}, args[1:]...)
	cmd := exec.CommandContext(ctx, s.bin, full...)
	cmd.Env = ctlEnv()
	var out, errOut bytes.Buffer
	cmd.Stdout, cmd.Stderr = &out, &errOut
	start := time.Now()
	err := testharness.RunProcess(cmd)
	res := ctlResult{stdout: out.String(), stderr: errOut.String(), elapsed: time.Since(start)}
	var exitErr *exec.ExitError
	if err != nil {
		if errorsAs(err, &exitErr) {
			res.rc = exitErr.ExitCode()
		} else {
			s.t.Fatalf("run ctl %v: %v", args, err)
		}
	}
	return res
}

// producer returns a remote shell command that prints n numbered lines to
// stdout (and every third to stderr), period apart, writes marker.<pid>
// once at start, then exits rc. The expected stdout/stderr are generated
// here too, so a test compares bytes, not counts.
func producer(n int, period time.Duration, marker string, rc int) (script string, stdout, stderr []byte) {
	var sb, so, se strings.Builder
	fmt.Fprintf(&sb, ": > %s.$$; i=0; while [ $i -lt %d ]; do printf 'line %%05d\\n' $i; if [ $((i %% 3)) -eq 0 ]; then printf 'err %%05d\\n' $i >&2; fi; i=$((i+1)); sleep %s; done; exit %d",
		marker, n, strconv.FormatFloat(period.Seconds(), 'f', 3, 64), rc)
	for i := 0; i < n; i++ {
		fmt.Fprintf(&so, "line %05d\n", i)
		if i%3 == 0 {
			fmt.Fprintf(&se, "err %05d\n", i)
		}
	}
	return sb.String(), []byte(so.String()), []byte(se.String())
}

// markerCount counts marker.<pid> files: the "executed exactly once" oracle.
func markerCount(t *testing.T, marker string) int {
	t.Helper()
	matches, err := filepath.Glob(marker + ".*")
	if err != nil {
		t.Fatal(err)
	}
	return len(matches)
}

// waitMarker waits until at least one marker.<pid> file exists (the remote
// command has started), so a fault is injected while the exec is in flight.
func waitMarker(t *testing.T, marker string) {
	t.Helper()
	if !testharness.WaitFor(t, 15*time.Second, 20*time.Millisecond, func() bool { return markerCount(t, marker) > 0 }) {
		t.Fatal("remote command never started")
	}
}

func errorsAs(err error, target **exec.ExitError) bool {
	e, ok := err.(*exec.ExitError)
	if ok {
		*target = e
	}
	return ok
}

// testharnessWait polls pred until it holds or d elapses.
func testharnessWait(d time.Duration, pred func() bool) bool {
	deadline := time.Now().Add(d)
	for {
		if pred() {
			return true
		}
		if time.Now().After(deadline) {
			return false
		}
		time.Sleep(25 * time.Millisecond)
	}
}

// execOnNode is exec against another node id of the same session.
func (s *realStack) execOnNode(ctx context.Context, nid string, extra []string, argv ...string) ctlResult {
	s.t.Helper()
	saved := s.nid
	s.nid = nid
	defer func() { s.nid = saved }()
	return s.exec(ctx, s.url, nil, extra, argv...)
}

// execWithStdoutClosedAfter runs the ctl with a pipe as stdout, reads n bytes
// and closes the read end: the ctl's next stdout write raises SIGPIPE.
func (s *realStack) execWithStdoutClosedAfter(ctx context.Context, n int, argv ...string) ctlResult {
	s.t.Helper()
	args := append([]string{"exec", "--home", s.ctlHome, "--nats-url", s.url, s.nid, "--"}, argv...)
	cmd := exec.CommandContext(ctx, s.bin, args...)
	cmd.Env = ctlEnv()
	pr, pw, err := os.Pipe()
	if err != nil {
		s.t.Fatal(err)
	}
	var errOut bytes.Buffer
	cmd.Stdout, cmd.Stderr = pw, &errOut
	start := time.Now()
	wait, err := testharness.StartProcess(cmd)
	if err != nil {
		s.t.Fatal(err)
	}
	_ = pw.Close()
	buf := make([]byte, n)
	_, _ = io.ReadFull(pr, buf)
	_ = pr.Close()
	err = wait()
	res := ctlResult{stderr: errOut.String(), elapsed: time.Since(start)}
	var exitErr *exec.ExitError
	if err != nil {
		if errorsAs(err, &exitErr) {
			if st, ok := exitErr.Sys().(syscall.WaitStatus); ok && st.Signaled() {
				res.rc = 128 + int(st.Signal())
			} else {
				res.rc = exitErr.ExitCode()
			}
		} else {
			s.t.Fatalf("run ctl: %v", err)
		}
	}
	return res
}

// legacyAgentSim stands in for a v0.6.0 agent on another node of the same
// session: it answers legacy exec on the forwarded subject and, like the
// old dispatcher's default arm, ignores process-resume entirely (the real
// v0.6.0 agent logs `unknown forwarded verb` and drops it). It counts what
// it saw so a test can prove no reliable create ever reached it.
type legacyAgentSim struct {
	probes, creates, execs atomic.Int64
}

func startLegacyAgentSim(t *testing.T, s *realStack, nid string) *legacyAgentSim {
	t.Helper()
	// origin: hot-update S6 internal review rv-ctl-1 / lead D1
	// The real old agent registers its release; missing metadata is not legacy proof.
	if _, err := s.db.Exec(`INSERT INTO nodes(sid,nid,status,registered_at,release_version,proto_version) VALUES(?,?,'ONLINE',?,?,?)`, s.sid, nid, time.Now().UTC(), "v0.6.0", proto.ProtoVersion); err != nil {
		t.Fatal(err)
	}
	nc, err := nats.Connect(s.url)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(nc.Close)
	sim := &legacyAgentSim{}
	_, err = nc.Subscribe(proto.SubjCmdForwarded(s.sid, nid, "*"), func(msg *nats.Msg) {
		parts := strings.Split(msg.Subject, ".")
		verb := parts[len(parts)-3]
		switch verb {
		case proto.ProcessResumeVerb:
			if strings.Contains(string(msg.Data), `"op":"create"`) {
				sim.creates.Add(1)
			} else {
				sim.probes.Add(1)
			}
			return // a v0.6.0 agent has no handler for this verb
		case "exec":
			sim.execs.Add(1)
			var req proto.ExecReq
			_ = json.Unmarshal(msg.Data, &req)
			reply := func(c proto.ExecChunk) {
				body, _ := json.Marshal(c)
				_ = nc.Publish(msg.Reply, body)
			}
			reply(proto.ExecChunk{Kind: "started", PID: "legacy-pid"})
			reply(proto.ExecChunk{Kind: "stdout", Data: []byte(strings.Join(req.Argv[1:], " ") + "\n")})
			reply(proto.ExecChunk{Kind: "exit", PID: "legacy-pid", ExitCode: 7})
		}
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := nc.FlushTimeout(2 * time.Second); err != nil {
		t.Fatal(err)
	}
	return sim
}

// psEntries runs the real `tether ps --json` (with -a when all) and decodes
// the process rows; a failed ps yields nil so callers can keep polling.
func (s *realStack) psEntries(ctx context.Context, all bool) []proto.PsEntry {
	s.t.Helper()
	args := []string{"ps", "--json"}
	if all {
		args = append(args, "-a")
	}
	res := s.ctlCommand(ctx, args...)
	if res.rc != 0 {
		return nil
	}
	var doc struct {
		Processes []proto.PsEntry `json:"processes"`
	}
	if err := json.Unmarshal([]byte(res.stdout), &doc); err != nil {
		return nil
	}
	return doc.Processes
}

// origin: S1 internal review E4 — the matrix row for this package says
// race=true; that must mean the ctl and agent binaries under fault injection,
// not only the harness process. The sentinel file flips raceEnabled under
// -race; here both states of the build line are pinned.
func TestRealStackBuildPassesRaceThrough(t *testing.T) {
	// origin: hot-update S5 PROC-LEAK / final process census.
	testharness.GuardProcessDirectory(t, filepath.Dir(t.TempDir()))

	saved := raceEnabled
	t.Cleanup(func() { raceEnabled = saved })
	raceEnabled = false
	if args := strings.Join(tetherBuildCmd("/tmp/x").Args, " "); strings.Contains(args, "-race") || !strings.HasPrefix(args, "go build ") {
		t.Fatalf("a non-race harness must not instrument the product binary (and must still run `go build`): %s", args)
	}
	raceEnabled = true
	if args := strings.Join(tetherBuildCmd("/tmp/x").Args, " "); !strings.HasPrefix(args, "go build -race ") || !strings.HasSuffix(args, " ./cmd/tether") {
		t.Fatalf("a -race harness must instrument the product binary: %s", args)
	}
}

// waitFreshHeartbeat blocks until the node's last_heartbeat_at is younger
// than maxAge — a phase anchor for tests that must stop the broker right
// after a heartbeat landed (the next one then falls after the restart).
func (s *realStack) waitFreshHeartbeat(t *testing.T, maxAge time.Duration) {
	t.Helper()
	if !testharnessWait(10*time.Second, func() bool {
		var last string
		if err := s.db.QueryRow(`SELECT COALESCE(last_heartbeat_at, '') FROM nodes WHERE sid=? AND nid=?`, s.sid, s.nid).Scan(&last); err != nil {
			return false
		}
		hb, err := time.Parse("2006-01-02 15:04:05.999999999 -0700 MST", last)
		return err == nil && time.Since(hb) < maxAge
	}) {
		t.Fatal("no fresh heartbeat within 10 s")
	}
}

// origin: hot-update S4 plan §9.5 / T38. A historical broker must execute
// its own op whitelist; synthesizing unsupported replies would only test
// a test double. A disk DB lets its real process share the isolated stack.
func (s *realStack) startHistoricalBroker() {
	s.t.Helper()
	if s.dbPath == "" {
		s.t.Fatal("historical broker requires a private disk DB")
	}
	args := []string{"serve", "--db", s.dbPath, "--nats-url", s.url,
		"--tunnel-addr", "", "--admin-socket", "", "--public-host", "test.local"}
	// Historical upgrade arms need the same explicit loopback artifact
	// authority on both consumers, installed before the first business request.
	// origin: hot-update S8 plan §7.2; S8 internal review upgrade-maketest-2 F2
	for i := 0; i+1 < len(s.agentArgs); i++ {
		if s.agentArgs[i] == "--upgrade-url-allow" {
			args = append(args, s.agentArgs[i:i+2]...)
		}
	}
	if s.callout {
		seeds := s.t.TempDir()
		for name, seed := range map[string][]byte{"broker.nk": s.brokerSeed, "account.nk": s.accountSeed} {
			if err := os.WriteFile(filepath.Join(seeds, name), seed, 0o600); err != nil {
				s.t.Fatal(err)
			}
		}
		args = append(args, "--auth-callout-seeds-dir", seeds)
	}
	cmd := exec.Command(s.brokerBin, args...)
	cmd.Env = append(ctlEnv(), "TETHER_HOME="+s.t.TempDir())
	cmd.SysProcAttr = agentProcessAttributes()
	output := &ptyCapture{}
	cmd.Stdout, cmd.Stderr = output, output
	wait, err := testharness.StartProcess(cmd)
	if err != nil {
		s.t.Fatal(err)
	}
	done := make(chan struct{})
	go func() { _ = wait(); close(done) }()
	stop := func() {
		_ = cmd.Process.Signal(syscall.SIGTERM)
		select {
		case <-done:
		case <-time.After(5 * time.Second):
			_ = syscall.Kill(-cmd.Process.Pid, syscall.SIGKILL)
			<-done
		}
	}
	s.mu.Lock()
	s.brokerStop = stop
	s.mu.Unlock()
	if !testharnessWait(10*time.Second, func() bool { return s.brokerHasSubscription(".run.req") }) {
		stop()
		s.t.Fatalf("historical broker failed to subscribe: %s", output.bytes())
	}
	if s.callout {
		s.waitCalloutReady()
	}
	s.t.Logf("historical broker binary=%s pid=%d", s.brokerBin, cmd.Process.Pid)
}
