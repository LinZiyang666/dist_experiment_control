//go:build linux

package main

import (
	"bytes"
	"context"
	"encoding/json"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/LinZiyang666/tether/internal/cli"
	"github.com/LinZiyang666/tether/internal/proto"
	"github.com/LinZiyang666/tether/internal/testharness"
	"github.com/LinZiyang666/tether/internal/upgradecli"
	"github.com/LinZiyang666/tether/internal/upgradetx"
	"github.com/LinZiyang666/tether/internal/upgradewire"
	"github.com/nats-io/nats.go"
)

// These tests run the BUILT tether binary by explicit path against an
// embedded NATS whose upgrade subjects are answered by scripted targets. The
// policy under test is the ctl's: exit classes, fresh nonces, non_hot
// labelling and the exact transaction envelope it sends.
// origin: hot-update S8 plan §7.1 / §11.2 P19 (real CLI tests)

// transactionCLIBinary builds the product binary into the test's own temp
// directory, so every run removes it again; the Go build cache keeps the
// per-test link cheap. A shared out-of-test directory would outlive the run.
func transactionCLIBinary(t *testing.T) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "tether")
	ctx, cancel := context.WithTimeout(t.Context(), 4*time.Minute)
	defer cancel()
	build := exec.CommandContext(ctx, "go", "build", "-o", path, ".")
	var out bytes.Buffer
	build.Stdout, build.Stderr = &out, &out
	wait, err := testharness.StartProcess(build)
	if err == nil {
		err = wait()
	}
	if err != nil {
		t.Fatalf("go build: %v\n%s", err, out.String())
	}
	return path
}

type cliRun struct {
	rc             int
	stdout, stderr string
	elapsed        time.Duration
}

// runTransactionCLI runs one supervised ctl invocation in a private home.
func runTransactionCLI(t *testing.T, bin, home string, args ...string) cliRun {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
	defer cancel()
	cmd := exec.CommandContext(ctx, bin, args...)
	cmd.Env = append(os.Environ(), "TETHER_HOME="+home, "HOME="+home, cli.DevNoAuthEnv+"=1")
	var stdout, stderr bytes.Buffer
	cmd.Stdout, cmd.Stderr = &stdout, &stderr
	start := time.Now()
	wait, err := testharness.StartProcess(cmd)
	if err != nil {
		t.Fatal(err)
	}
	_ = wait()
	if cmd.ProcessState == nil {
		t.Fatalf("ctl %v was not reaped", args)
	}
	return cliRun{rc: cmd.ProcessState.ExitCode(), stdout: stdout.String(), stderr: stderr.String(), elapsed: time.Since(start)}
}

// transactionCtlHome creates a ctl home with an active session and identity.
func transactionCtlHome(t *testing.T) (string, *cli.Identity) {
	t.Helper()
	home := t.TempDir()
	id, err := cli.EnsureIdentity(home)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(home, "current_session"), []byte("lab\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	return home, id
}

func unknownTransactionView(key upgradetx.Key, actorFP string) upgradetx.View {
	h := upgradetx.Handle{Key: key, TxID: strings.Repeat("a", 32), CutID: strings.Repeat("b", 32), Generation: 2, Revision: 7}
	g := upgradetx.Grant{Handle: h, ActorFP: actorFP, Mode: upgradetx.ModeHot, SourceSHA: strings.Repeat("1", 64), TargetSHA: strings.Repeat("2", 64),
		Issuer: upgradetx.Owner{BootID: "boot", PID: 7, StartTicks: 9, InstanceID: strings.Repeat("e", 32)}, ClientNonce: strings.Repeat("c", 32),
		SourceRelease: "v1", TargetRelease: "v2", ScopeDigest: strings.Repeat("3", 64), CapabilityDigest: strings.Repeat("4", 64), ExpiresAt: 1}
	completed := upgradetx.Attempt{Handle: h, Direction: upgradetx.DirectionForward, Phase: upgradetx.PhaseCompleted, Outcome: upgradetx.OutcomeSuccess, CutStage: upgradetx.CutReleased}
	current := completed
	current.Outcome = upgradetx.OutcomeUnknown
	return upgradetx.View{Code: upgradetx.CodeUnsettled, Detail: "current owner is unknown",
		Historical:  upgradetx.Record{Version: upgradetx.RecordVersion, Key: key, Revision: 7, Grant: g, Forward: completed},
		Observation: upgradetx.Observation{OwnerKnown: false, Current: current}}
}

// A successful read of an accepted transaction whose current owner became
// unknown is rc0 with a fresh nonce per query; waiting on it is bounded
// upgrade_unsettled rc74. The node is literally named "status".
// origin: hot-update S8 plan §7.1 / §11.2 P18/P19 (query unknown=0, wait unknown=74)
func TestTransactionCLIQueryUnknownIsZeroWaitIs74(t *testing.T) {
	bin := transactionCLIBinary(t)
	url := testharness.StartNATS(t)
	home, id := transactionCtlHome(t)
	key := upgradetx.Key{DomainID: strings.Repeat("d", 32), Component: upgradetx.ComponentAgent, SID: "lab", ConfiguredNID: "status", UID: 1000}
	view := unknownTransactionView(key, id.Fingerprint)
	if err := upgradecli.RememberGrant(home, view.Historical.Grant); err != nil {
		t.Fatal(err)
	}
	nc, err := nats.Connect(url)
	if err != nil {
		t.Fatal(err)
	}
	defer nc.Close()
	var mu sync.Mutex
	var nonces []string
	sub, err := nc.Subscribe(proto.SubjCmdBy("lab", id.PublicKey, "status", "upgrade-tx"), func(m *nats.Msg) {
		req, err := upgradewire.DecodeRequest(m.Data, upgradetx.ComponentAgent)
		reply := view
		if err != nil || req.Op != upgradewire.OpQuery || req.Key != key || req.TxID != view.Historical.Grant.Handle.TxID {
			body, _ := upgradewire.EncodeResponse(upgradewire.Response{Code: upgradetx.CodeRejected, Error: "unexpected envelope"})
			_ = m.Respond(body)
			return
		}
		mu.Lock()
		nonces = append(nonces, req.QueryNonce)
		mu.Unlock()
		reply.Observation.Nonce, reply.Observation.At = req.QueryNonce, time.Now().UnixNano()
		body, _ := upgradewire.EncodeResponse(upgradewire.Response{View: reply})
		_ = m.Respond(body)
	})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = sub.Unsubscribe() }()
	if err := nc.Flush(); err != nil {
		t.Fatal(err)
	}
	// Five complete CLI child lifetimes: every supervised ctl and its NATS
	// connection must be gone afterwards (CLI child leak anchor).
	g, fd := runtime.NumGoroutine(), testharness.FDCount()
	for round := 0; round < 5; round++ {
		exerciseUnknownTransactionCLI(t, bin, home, url, view.Historical.Grant.Handle.TxID)
	}
	testharness.AssertNoGoroutineLeak(t, "transaction CLI children x5", g)
	testharness.AssertNoFDLeak(t, "transaction CLI children x5", fd, 0)
	mu.Lock()
	defer mu.Unlock()
	if len(nonces) < 15 || len(nonces) != len(uniqueStrings(nonces)) {
		t.Fatalf("the ctl did not send one fresh query challenge per status/wait poll: %v", nonces)
	}
}

func exerciseUnknownTransactionCLI(t *testing.T, bin, home, url, tx string) {
	t.Helper()
	var seen []string
	for range 2 {
		r := runTransactionCLI(t, bin, home, "node", "upgrade-status", "status", "--tx-id", tx, "--json", "--home", home, "--nats-url", url)
		var got upgradetx.View
		if r.rc != 0 || json.Unmarshal([]byte(r.stdout), &got) != nil {
			t.Fatalf("unknown query must be a successful rc0 read: %+v", r)
		}
		if got.Observation.OwnerKnown || got.Code != upgradetx.CodeUnsettled || len(got.Observation.Nonce) != 32 || got.Historical.Forward.Phase != upgradetx.PhaseCompleted {
			t.Fatalf("status rewrote the unknown observation: %s", r.stdout)
		}
		seen = append(seen, got.Observation.Nonce)
	}
	if seen[0] == seen[1] {
		t.Fatal("two queries reused one fresh-observation challenge")
	}
	wait := runTransactionCLI(t, bin, home, "node", "upgrade-wait", "status", "--tx-id", tx, "--timeout", "1s", "--home", home, "--nats-url", url)
	if wait.rc != 74 || !strings.Contains(wait.stdout+wait.stderr, upgradetx.CodeUnsettled) || wait.elapsed > 15*time.Second {
		t.Fatalf("unknown wait must be bounded upgrade_unsettled rc74: %+v", wait)
	}
	other := runTransactionCLI(t, bin, home, "node", "upgrade-status", "another-node", "--tx-id", tx, "--json", "--home", home, "--nats-url", url)
	if other.rc != 64 {
		t.Fatalf("a recorded grant for node status answered for another node: %+v", other)
	}
}

func uniqueStrings(values []string) map[string]bool {
	out := map[string]bool{}
	for _, v := range values {
		out[v] = true
	}
	return out
}

// The explicit first migration is never hot: an accepted legacy dispatch is
// rc74 non_hot, and each refusal keeps its legacy class with the non_hot label.
// origin: hot-update S8 plan §7.1 / §7.2 / §11.2 P19 (interrupting=non_hot)
func TestTransactionCLIInterruptingIsNeverHot(t *testing.T) {
	bin := transactionCLIBinary(t)
	url := testharness.StartNATS(t)
	home, id := transactionCtlHome(t)
	nc, err := nats.Connect(url)
	if err != nil {
		t.Fatal(err)
	}
	defer nc.Close()
	replies := map[string]proto.UpgradeResp{
		"old-agent":  {OK: true, NewVersion: "v0.7.0"},
		"busy-agent": {Code: proto.CodeBadRequest, Error: "legacy_busy"},
		"idle-agent": {Code: "try_again", Error: "legacy_quiescence_unproved"},
	}
	for nid, resp := range replies {
		body, _ := json.Marshal(resp)
		sub, err := nc.Subscribe(proto.SubjCmdBy("lab", id.PublicKey, nid, "upgrade"), func(m *nats.Msg) {
			var req proto.UpgradeReq
			if json.Unmarshal(m.Data, &req) != nil || req.ProtoVersion != proto.ProtoVersion || req.URL == "" {
				_ = m.Respond([]byte(`{"code":"bad_request"}`))
				return
			}
			_ = m.Respond(body)
		})
		if err != nil {
			t.Fatal(err)
		}
		defer func() { _ = sub.Unsubscribe() }()
	}
	if err := nc.Flush(); err != nil {
		t.Fatal(err)
	}
	sha := strings.Repeat("9", 64)
	for nid, want := range map[string]int{"old-agent": 74, "busy-agent": 64, "idle-agent": 75, "absent-agent": 75} {
		r := runTransactionCLI(t, bin, home, "node", "upgrade", nid, "--url", "https://example.invalid/t.tar.gz", "--sha256", sha, "--interrupting", "--home", home, "--nats-url", url)
		if r.rc != want || !strings.Contains(r.stdout+r.stderr, "non_hot") {
			t.Fatalf("%s: interrupting rc=%d want %d, or the outcome was not labelled non_hot: %+v", nid, r.rc, want, r)
		}
	}
	both := runTransactionCLI(t, bin, home, "node", "upgrade", "old-agent", "--url", "https://example.invalid/t.tar.gz", "--sha256", sha, "--interrupting", "--prepare-only", "--grant-out", filepath.Join(home, "g.json"), "--home", home, "--nats-url", url)
	if both.rc != 64 {
		t.Fatalf("interrupting combined with prepare-only must be refused: %+v", both)
	}
}
