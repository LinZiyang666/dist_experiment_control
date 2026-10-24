package main

// The capable continuation path end to end through the real Cobra commands:
// an in-process broker with the single-broker transfer graph installed and an
// in-process agent with the persistent worker, both on one embedded
// JetStream server. Nothing here substitutes a controller, worker, codec,
// tracker, ledger or history writer; the only fixtures are the process
// harnesses and an optional NATS reply-dropping relay.

import (
	"bytes"
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/LinZiyang666/tether/internal/agent"
	"github.com/LinZiyang666/tether/internal/auth"
	"github.com/LinZiyang666/tether/internal/broker"
	"github.com/LinZiyang666/tether/internal/cli"
	"github.com/LinZiyang666/tether/internal/controlbudget"
	"github.com/LinZiyang666/tether/internal/proto"
	"github.com/LinZiyang666/tether/internal/session"
	"github.com/LinZiyang666/tether/internal/testharness"
	x "github.com/LinZiyang666/tether/internal/xfercontract"
	"github.com/LinZiyang666/tether/test/stackharness"
	"github.com/nats-io/nats.go"
	"github.com/spf13/cobra"
)

type capableStack struct {
	url, home string
	db        *sql.DB
	b         *broker.Broker
}

// testTransferCapacity is a plain bounded account for the broker graph; the
// worker and ctl install their own process accounts.
// That historical fixture account has been replaced: all accounts now come
// from the production constructors, keeping negotiated source inspection and
// settlement on the same graph as the command assertions.
// origin: S7 internal review MAKETEST-TRANSFER F4; S7 plan §8.1 / P20 / P22
func capableStackLogger() *slog.Logger {
	if os.Getenv("TETHER_E2E_LOG") != "" {
		return slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelDebug}))
	}
	return testharness.SilentLog()
}

func startCapableStack(t *testing.T) *capableStack {
	t.Helper()
	s := &capableStack{url: startJetStreamNATS(t), home: t.TempDir(), db: testharness.OpenDB(t)}
	writeReviewPsIdentity(t, s.home, "lab")
	id, err := cli.EnsureIdentity(s.home)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := session.Create(s.db, "lab", "lab", id.Fingerprint, stackharness.SessionPINHash(t), time.Now().UTC()); err != nil {
		t.Fatal(err)
	}
	// Use the shipped graph on both roles: the earlier injected worker has
	// no production source-inspection link and cannot model negotiated pulls.
	// origin: S7 internal review MAKETEST-TRANSFER F4; S7 plan §6.1 / §8.1 / P20 / P22
	s.b, err = broker.New(broker.Config{NATSURL: s.url, DB: s.db, Logger: capableStackLogger(), ReconcileInterval: time.Hour,
		StaleAfter: time.Hour, OfflineAfter: 2 * time.Hour,
		TransferFactory: broker.ServeTransferFactory})
	if err != nil {
		t.Fatal(err)
	}
	stopBroker := runCapableOwner(t, "broker", s.b.Run)
	a, err := agent.New(agent.Config{NATSURL: s.url, SID: "lab", NID: "lab-1", Logger: capableStackLogger(), HeartbeatInterval: 100 * time.Millisecond,
		RegisterTimeout: 2 * time.Second})
	if err != nil {
		t.Fatal(err)
	}
	stopAgent := runCapableOwner(t, "agent", a.Run)
	// LIFO: the agent stops before its broker, as in an ordinary shutdown.
	t.Cleanup(func() { stopAgent(); stopBroker() })
	testharness.WaitNodeOnline(t, s.db, "lab", "lab-1", 5*time.Second)
	// ONLINE proves registration, not completion of the broker's transfer graph.
	// Wait for its actual read-only probe before exercising a capable invocation.
	// origin: hot-update S7 plan §6.1 / INTEG-D2 startup readiness regression
	nc, err := nats.Connect(s.url, nats.CustomInboxPrefix(auth.InboxPrefixFor(id.PublicKey)))
	if err != nil {
		t.Fatal(err)
	}
	defer nc.Close()
	cfg, closeClient, err := cli.TransferClientConfig(nc, s.url, id, "lab")
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := closeClient(); err != nil {
			t.Errorf("cleanup: %v", err)
		}
	}()
	if !testharness.WaitFor(t, 5*time.Second, 10*time.Millisecond, func() bool {
		deadline, err := controlbudget.After(200 * time.Millisecond)
		if err != nil {
			return false
		}
		response, _, err := cfg.Transport.Request(x.Request{Op: x.OpProbe, CallID: "fixture-ready", Identity: x.Identity{SID: "lab", NID: "lab-1", ActorFP: id.Fingerprint, TransferID: "fixture-ready"}}, deadline)
		return err == nil && response.Probe != nil && response.Probe.Version == x.Version
	}) {
		t.Fatal("capable broker and agent never completed the transfer probe")
	}
	return s
}

func runCapableOwner(t *testing.T, name string, run func(context.Context) error) func() {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background()) // ctx-root: test-owned process lifetime.
	done := make(chan error, 1)
	go func() { done <- run(ctx) }()
	var once sync.Once
	return func() {
		once.Do(func() {
			cancel()
			select {
			case <-done:
			case <-time.After(10 * time.Second):
				t.Errorf("%s did not exit", name)
			}
		})
	}
}

// runTransferCLI executes one real Cobra command and returns stdout/stderr.
func (s *capableStack) runTransferCLI(t *testing.T, build func() *cobra.Command, args ...string) (string, string, error) {
	t.Helper()
	return s.runTransferCLIAt(t, s.url, build, args...)
}

// runTransferCLIAt is runTransferCLI through another NATS URL (a relay).
func (s *capableStack) runTransferCLIAt(t *testing.T, url string, build func() *cobra.Command, args ...string) (string, string, error) {
	t.Helper()
	cmd := build()
	cmd.SetArgs(append(args, "--home", s.home, "--nats-url", url))
	var stdout, stderr bytes.Buffer
	cmd.SetOut(&stdout)
	cmd.SetErr(&stderr)
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second) // ctx-root: one bounded CLI invocation.
	defer cancel()
	err := cmd.ExecuteContext(ctx)
	return stdout.String(), stderr.String(), err
}

func transferField(out, key string) string {
	for _, field := range strings.Fields(out) {
		if value, ok := strings.CutPrefix(field, key+"="); ok {
			return value
		}
	}
	return ""
}

func sha256Hex(b []byte) string {
	sum := sha256.Sum256(b)
	return hex.EncodeToString(sum[:])
}

// TestTransferCommandsCompleteThroughCapableBrokerAndAgent drives tether push
// (inline Tier A and object Tier B) and tether pull (Tier B) through the real
// broker controller and agent worker, then reads each result back with the
// read-only status command. Every destination must equal its source, every
// receipt must name exact history, and status must report the same terminal.
// The second object pull and push follow decided transfers of the same
// session: per-session object single flight serializes byte work, never a
// decided entry whose remaining duty is cleanup (or a published sender).
// origin: hot-update S7 plan §7 / §10 E4 / §1.2 / INTEG-D end-to-end carriers
// origin: S7 internal review MAKETEST-TRANSFER F4 / F7
// origin: S7 internal review TRANSFER-MAKETEST-2 F1
func TestTransferCommandsCompleteThroughCapableBrokerAndAgent(t *testing.T) {
	// origin: hot-update S10 plan L17 / §11.1 / POLISH-B
	if runIsolatedBinaryTest(t) {
		return
	}
	g, fd := runtime.NumGoroutine(), testharness.FDCount()
	for round := 0; round < 5; round++ {
		t.Run("round", func(t *testing.T) { exerciseTransferCommandsCompleteThroughCapableBrokerAndAgent(t) })
	}
	testharness.AssertNoGoroutineLeak(t, "TestTransferCommandsCompleteThroughCapableBrokerAndAgent", g)
	testharness.AssertNoFDLeak(t, "TestTransferCommandsCompleteThroughCapableBrokerAndAgent", fd, 0)
}

func exerciseTransferCommandsCompleteThroughCapableBrokerAndAgent(t *testing.T) {
	t.Helper()

	s := startCapableStack(t)
	remote := t.TempDir()
	local := t.TempDir()
	for _, tc := range []struct {
		name string
		size int
		verb string
	}{{"push-inline", 4096, "push"}, {"push-object", 3<<20 + 17, "push"}, {"pull-object", 1<<20 + 5, "pull"},
		{"pull-object-again", 1<<20 + 9, "pull"}, {"push-object-again", 2<<20 + 3, "push"}} {
		t.Run(tc.name, func(t *testing.T) {
			payload := bytes.Repeat([]byte(tc.name+"|"), tc.size/len(tc.name+"|")+1)[:tc.size]
			src, dst := filepath.Join(local, tc.name+".src"), filepath.Join(remote, tc.name+".dst")
			var stdout, stderr string
			var err error
			if tc.verb == "push" {
				if err := os.WriteFile(src, payload, 0o600); err != nil {
					t.Fatal(err)
				}
				stdout, stderr, err = s.runTransferCLI(t, newPushCmd, src, "lab-1:"+dst)
			} else {
				src, dst = filepath.Join(remote, tc.name+".src"), filepath.Join(local, tc.name+".dst")
				if err := os.WriteFile(src, payload, 0o600); err != nil {
					t.Fatal(err)
				}
				stdout, stderr, err = s.runTransferCLI(t, newPullCmd, "lab-1:"+src, dst)
			}
			if err != nil {
				t.Fatalf("%s failed: %v\nstdout=%s\nstderr=%s", tc.name, err, stdout, stderr)
			}
			if transferField(stdout, "kind") != "complete" || transferField(stdout, "sha256") != sha256Hex(payload) ||
				transferField(stdout, "outcome_unknown") != "false" || strings.Contains(stdout, "best_effort") {
				t.Fatalf("%s did not print one exact capable receipt: %q", tc.name, stdout)
			}
			got, err := os.ReadFile(dst)
			if err != nil || !bytes.Equal(got, payload) {
				t.Fatalf("%s destination differs from source: %v", tc.name, err)
			}
			id := transferField(stdout, "transfer_id")
			status, stderr, err := s.runTransferCLI(t, func() *cobra.Command { return newTransferCmd(transferCLI) }, "status", id, "--node", "lab-1", "--json")
			if err != nil || !strings.Contains(status, `"state":"terminal"`) || !strings.Contains(status, `"kind":"complete"`) ||
				!strings.Contains(status, `"transfer_id":"`+id+`"`) {
				t.Fatalf("status did not report the exact terminal: %v %s %s", err, status, stderr)
			}
		})
	}
}

// TestTransferRateLimitIsSealedAndPaced drives --rate-limit through both real
// directions: the ctl writer paces a push, the agent's sender paces a pull, and
// the durable budget seal (read back by status) carries the requested rate.
// An unachievable rate is refused before the first side effect: the relay
// sees no permit/prepare publication at all (LQ2: refuse >2108s before bytes).
// origin: hot-update S7 plan §1.2 / lead LQ2 / INTEG-D --rate-limit
// origin: S7 internal review MAKETEST-TRANSFER F4
func TestTransferRateLimitIsSealedAndPaced(t *testing.T) {
	// origin: hot-update S10 plan L17 / §11.1 / POLISH-B
	if runIsolatedBinaryTest(t) {
		return
	}
	g, fd := runtime.NumGoroutine(), testharness.FDCount()
	for round := 0; round < 5; round++ {
		t.Run("round", func(t *testing.T) { exerciseTransferRateLimitIsSealedAndPaced(t) })
	}
	testharness.AssertNoGoroutineLeak(t, "TestTransferRateLimitIsSealedAndPaced", g)
	testharness.AssertNoFDLeak(t, "TestTransferRateLimitIsSealedAndPaced", fd, 0)
}

func exerciseTransferRateLimitIsSealedAndPaced(t *testing.T) {
	t.Helper()

	s := startCapableStack(t)
	const rate, size = 1 << 20, 2<<20 + 3
	remote, local := t.TempDir(), t.TempDir()
	for _, verb := range []string{"push", "pull"} {
		t.Run(verb, func(t *testing.T) {
			payload := bytes.Repeat([]byte(verb+"-paced|"), size/len(verb+"-paced|")+1)[:size]
			src, dst, build, args := filepath.Join(local, verb+".src"), filepath.Join(remote, verb+".dst"), newPushCmd, []string{}
			// The ctl paces each chunk by the bytes it will have sent; the sender
			// paces by the bytes already sent, so its floor omits one chunk.
			floor := time.Duration(size) * time.Second / rate
			if verb == "pull" {
				src, dst, build = filepath.Join(remote, verb+".src"), filepath.Join(local, verb+".dst"), newPullCmd
				floor = time.Duration(size-1<<20) * time.Second / rate
				args = append(args, "lab-1:"+src, dst)
			} else {
				args = append(args, src, "lab-1:"+dst)
			}
			if err := os.WriteFile(src, payload, 0o600); err != nil {
				t.Fatal(err)
			}
			started := time.Now()
			stdout, stderr, err := s.runTransferCLI(t, build, append(args, "--rate-limit", fmt.Sprint(rate))...)
			elapsed := time.Since(started)
			if err != nil || transferField(stdout, "kind") != "complete" {
				t.Fatalf("paced %s failed: %v\nstdout=%s\nstderr=%s", verb, err, stdout, stderr)
			}
			if got, err := os.ReadFile(dst); err != nil || !bytes.Equal(got, payload) {
				t.Fatalf("paced %s destination differs: %v", verb, err)
			}
			if elapsed < floor {
				t.Fatalf("paced %s finished in %v, below the %v the requested rate allows", verb, elapsed, floor)
			}
			status, stderr, err := s.runTransferCLI(t, func() *cobra.Command { return newTransferCmd(transferCLI) }, "status", transferField(stdout, "transfer_id"), "--node", "lab-1", "--json")
			if err != nil || !strings.Contains(status, fmt.Sprintf(`"rate_limit_bps":%d`, rate)) {
				t.Fatalf("the durable budget seal lost the requested rate: %v %s %s", err, status, stderr)
			}
		})
	}
	t.Run("unachievable", func(t *testing.T) {
		fence, url := startReplyFence(t, s.url, fenceSpec{match: func(string) bool { return false }})
		src := filepath.Join(local, "unachievable.src")
		if err := os.WriteFile(src, bytes.Repeat([]byte{7}, 3<<20), 0o600); err != nil {
			t.Fatal(err)
		}
		_, stderr, err := s.runTransferCLIAt(t, url, newPushCmd, src, "lab-1:"+filepath.Join(remote, "unachievable.dst"), "--rate-limit", "1000")
		if err == nil || !strings.Contains(err.Error(), "transfer_budget_exceeded") || classifyExit(err) != 64 {
			t.Fatalf("an unachievable rate was not refused as budget_exceeded (rc64): %v rc=%d %s", err, classifyExit(err), stderr)
		}
		for _, p := range fence.publications() {
			if _, _, _, _, op, ok := proto.ParseCtrlTransferRequest(p.Subject); ok && op != "probe" && op != "status" {
				t.Fatalf("refused rate still published a transfer mutation: %s", p.Subject)
			}
			if strings.HasSuffix(p.Subject, ".push.req") || strings.HasSuffix(p.Subject, ".push-commit.req") {
				t.Fatalf("refused rate still published a prepare/commit carrier: %s", p.Subject)
			}
		}
	})
}
