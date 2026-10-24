//go:build linux

// transfer_cut_test.go runs the real default broker graph, a real agent with
// its production continuation worker and the real ctl engine, and attempts
// agent image cuts while a capable transfer runs. Nothing here substitutes a
// worker, actor or transport: the cut freezes the handler's bound worker.
package agent

import (
	"bytes"
	"context"
	"crypto/rand"
	"database/sql"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/LinZiyang666/tether/internal/agentimage"
	"github.com/LinZiyang666/tether/internal/broker"
	"github.com/LinZiyang666/tether/internal/cli"
	"github.com/LinZiyang666/tether/internal/controlbudget"
	"github.com/LinZiyang666/tether/internal/natsinbox"
	"github.com/LinZiyang666/tether/internal/testharness"
	"github.com/LinZiyang666/tether/internal/xferclient"
	x "github.com/LinZiyang666/tether/internal/xfercontract"
	"github.com/LinZiyang666/tether/test/stackharness"
	"github.com/nats-io/nats-server/v2/server"
	natsservertest "github.com/nats-io/nats-server/v2/test"
	"github.com/nats-io/nats.go"
)

// The cut exports live senders and receivers, then abandonment reopens their
// writer, atomic sink and admission gates. The transfer keeps its exact bytes
// and receipt; later cuts carry its retained result without stopped actors.
// origin: hot-update S7 plan §8.2 / §8.4 / P21 / T14 / T37 — S7 production activation
func TestTransferImageCutCarriesLiveTransferAndRetainedReceipts(t *testing.T) {
	g, fd := runtime.NumGoroutine(), testharness.FDCount()
	for round := 0; round < 5; round++ {
		t.Run("round-"+strconv.Itoa(round), func(t *testing.T) { exerciseTransferImageCut(t) })
	}
	testharness.AssertNoGoroutineLeak(t, "transfer activation", g)
	testharness.AssertNoFDLeak(t, "transfer activation", fd, 0)
}

func exerciseTransferImageCut(t *testing.T) {
	t.Helper()

	url := cutJetStream(t)
	db := testharness.OpenDB(t)
	id, err := cli.EnsureIdentity(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	stackharness.SeedSession(t, db, "lab", id.Fingerprint)
	startCutBroker(t, url, db)
	remote := t.TempDir()
	a := startCutAgent(t, url, remote)
	testharness.WaitNodeOnline(t, db, "lab", "cut-xfer", 5*time.Second)
	nc, _, err := natsinbox.Connect(url, id.PublicKey, nil)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(nc.Close)
	local := t.TempDir()
	for _, verb := range []string{"pull", "push"} {
		t.Run(verb, func(t *testing.T) {
			payload := make([]byte, 2<<20+4321)
			_, _ = rand.Read(payload)
			src, dst := filepath.Join(local, verb+"-src.bin"), filepath.Join(remote, verb+"-dst.bin")
			if verb == "pull" {
				src, dst = filepath.Join(remote, verb+"-src.bin"), filepath.Join(local, verb+"-dst.bin")
			}
			if err := os.WriteFile(src, payload, 0o600); err != nil {
				t.Fatal(err)
			}
			remotePath, localPath := dst, src
			if verb == "pull" {
				remotePath, localPath = src, dst
			}
			var deferred, carried atomic.Int32
			receipt := runCutTransfer(t, nc, url, id, verb, remotePath, localPath, "cut"+verb, func() {
				// One cut per tick while the transfer runs; each one either
				// carries the entry and is abandoned; no live actor may be lost.
				if cutAttempt(t, a) {
					deferred.Add(1)
				} else {
					carried.Add(1)
				}
			})
			if receipt.Kind != "complete" || receipt.Bytes != int64(len(payload)) || receipt.HistorySeq == 0 {
				t.Fatalf("%s receipt after cuts: %+v", verb, receipt)
			}
			if got, err := os.ReadFile(dst); err != nil || !bytes.Equal(got, payload) {
				t.Fatalf("%s bytes differ after cuts: %v", verb, err)
			}
			// The agent sends a pull at a bounded rate for seconds: at least
			// one cut must have carried its live sender.
			if verb == "pull" && (carried.Load() == 0 || deferred.Load() != 0) {
				t.Fatalf("live pull sender did not cross every cut (carried=%d)", carried.Load())
			}
			t.Logf("%s: deferred=%d carried=%d", verb, deferred.Load(), carried.Load())
			// A settled entry is not a cut failure: its stopped actors and
			// closed files are no longer part of the image.
			a.gate.close()
			cut, err := freezeForUpgrade(a, time.Now().Add(10*time.Second))
			if err != nil {
				a.gate.open()
				t.Fatalf("cut after settlement: %v (cause: %s)", err, cutCause(err))
			}
			ref := cut.frozen.Bundle().Transfers
			if err := cut.abandon(a); err != nil {
				t.Fatal(err)
			}
			if ref == nil || ref.Count == 0 {
				t.Fatalf("settled receipt retention was lost across the cut: %+v", ref)
			}
		})
	}
}

// cutAttempt reports whether the cut deferred for a live transfer. Any other
// refusal fails; a successful cut is abandoned, which thaws every owner.
func cutAttempt(t *testing.T, a *Agent) bool {
	t.Helper()
	a.gate.close()
	cut, err := freezeForUpgrade(a, time.Now().Add(10*time.Second))
	if err == nil {
		if err := cut.abandon(a); err != nil {
			t.Errorf("abandon: %v", err)
		}
		return false
	}
	a.gate.open()
	var deferral *agentimage.DeferredError
	if !errors.As(err, &deferral) || deferral.Reason != agentimage.ReasonTransfersInFlight {
		t.Errorf("cut refused for another reason: %v (cause: %s)", err, cutCause(err))
	}
	return true
}

// runCutTransfer drives the real ctl engine and calls tick repeatedly from
// the moment its prepare returned (the agent then owns the entry) until the
// invocation finishes. Before that no cut may close the creation gate.
func runCutTransfer(t *testing.T, nc *nats.Conn, url string, id *cli.Identity, verb, remotePath, localPath, tid string, tick func()) x.TerminalReceipt {
	t.Helper()
	deadline, err := controlbudget.After(90 * time.Second)
	if err != nil {
		t.Fatal(err)
	}
	cfg, closeData, err := cli.TransferClientConfig(nc, url, id, "lab")
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := closeData(); err != nil {
			t.Error(err)
		}
	}()
	prepared := make(chan struct{})
	var once sync.Once
	cfg.Observe = func(o x.Observation) {
		if o.Attempt.LogicalID == tid+"-"+string(x.OpPrepare) {
			once.Do(func() { close(prepared) })
		}
	}
	cfg.Identity.SID, cfg.Identity.NID, cfg.Identity.TransferID = "lab", "cut-xfer", tid
	// A bounded rate keeps the byte actor live for seconds.
	cfg.Intent = x.Intent{Verb: verb, Path: remotePath, Force: true, RateLimitBPS: 512 << 10}
	if verb == "push" {
		cfg.Source, err = os.Open(localPath)
		if err != nil {
			t.Fatal(err)
		}
		info, _ := cfg.Source.Stat()
		cfg.Intent.Size = info.Size()
	} else if cfg.Destination, err = cli.NewTransferReceiver(localPath, true); err != nil {
		t.Fatal(err)
	}
	for {
		legacy, err := cli.SelectTransferClient(&cfg, deadline)
		if errors.Is(err, nats.ErrNoResponders) {
			time.Sleep(50 * time.Millisecond)
			continue
		}
		if err != nil || legacy {
			t.Fatalf("%s: transfer-v1 was not selected: legacy=%t err=%v", tid, legacy, err)
		}
		break
	}
	invocation, err := xferclient.NewClient(cfg)
	if err != nil {
		t.Fatal(err)
	}
	type result struct {
		receipt x.TerminalReceipt
		err     error
	}
	done := make(chan result, 1)
	go func() {
		receipt, err := invocation.Run(deadline)
		done <- result{receipt, err}
	}()
	select {
	case <-prepared:
	case r := <-done:
		t.Fatalf("%s finished before its prepare was observed: %+v %v", tid, r.receipt, r.err)
	}
	var r result
	for running := true; running; {
		select {
		case r = <-done:
			running = false
		case <-time.After(150 * time.Millisecond):
			tick()
		}
	}
	stop, _ := controlbudget.After(5 * time.Second)
	if err := invocation.Stop(stop); err != nil {
		t.Errorf("%s stop: %v", tid, err)
	}
	if r.err != nil {
		t.Fatalf("%s across agent cuts: %v", tid, r.err)
	}
	return r.receipt
}

// cutJetStream has the CONNECT nonce a production auth_callout deployment
// always has: the dedicated data attachments authenticate with nkeys.
func cutJetStream(t *testing.T) string {
	t.Helper()
	srv := natsservertest.RunServer(&server.Options{Host: "127.0.0.1", Port: -1, JetStream: true, StoreDir: t.TempDir(), AlwaysEnableNonce: true})
	t.Cleanup(func() { srv.Shutdown(); srv.WaitForShutdown() })
	if !srv.ReadyForConnections(2*time.Second) || !testharness.WaitFor(t, 2*time.Second, 20*time.Millisecond, srv.JetStreamIsCurrent) {
		t.Fatal("embedded JetStream server not ready")
	}
	return srv.ClientURL()
}

// startCutBroker runs the default broker, which installs the production
// continuation graph on every JetStream broker (no injected factory).
func startCutBroker(t *testing.T, url string, db *sql.DB) {
	t.Helper()
	b, err := broker.New(broker.Config{NATSURL: url, DB: db, Logger: testharness.SilentLog(),
		ReconcileInterval: 50 * time.Millisecond, StaleAfter: 300 * time.Millisecond, OfflineAfter: 900 * time.Millisecond})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background()) // ctx-root: isolated broker Run for this test.
	done := make(chan error, 1)
	go func() { done <- b.Run(ctx) }()
	t.Cleanup(func() {
		cancel()
		select {
		case <-done:
		case <-time.After(10 * time.Second):
			t.Error("broker did not stop")
		}
	})
	testharness.WaitConnect(t, url, 3*time.Second)
}

func startCutAgent(t *testing.T, url, root string) *Agent {
	t.Helper()
	image := filepath.Join(t.TempDir(), "tether")
	if err := os.WriteFile(image, []byte("cut image"), 0o755); err != nil {
		t.Fatal(err)
	}
	a, err := New(Config{NATSURL: url, SID: "lab", NID: "cut-xfer", Home: t.TempDir(), Logger: testharness.SilentLog(),
		HeartbeatInterval: 100 * time.Millisecond, RegisterTimeout: 2 * time.Second, RemoteFSMode: "off", UpgradeNoExit: true,
		AllowRoots: []string{root}, RootsConfigured: true})
	if err != nil {
		t.Fatal(err)
	}
	a.cfg.UpgradeExecutablePath, a.cfg.UpgradeRunningImagePath = image, image
	ctx, cancel := context.WithCancel(context.Background()) // ctx-root: isolated agent Run for this test.
	done := make(chan error, 1)
	go func() { done <- a.Run(ctx) }()
	t.Cleanup(func() {
		cancel()
		select {
		case <-done:
		case <-time.After(10 * time.Second):
			t.Error("agent did not stop")
		}
	})
	return a
}

func cutCause(err error) string {
	if err == nil {
		return ""
	}
	out := fmt.Sprintf("%T{%s}", err, err.Error())
	if j, ok := err.(interface{ Unwrap() []error }); ok {
		for _, e := range j.Unwrap() {
			out += " [" + cutCause(e) + "]"
		}
	} else if e := errors.Unwrap(err); e != nil {
		out += " <- " + cutCause(e)
	}
	return out
}
