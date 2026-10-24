//go:build linux

// transfer_graph_cut_test.go drives a real broker image cut (store-close,
// export, prepare) whose exec fails, so the same process rolls back onto the
// current store, while a capable transfer runs through the default graph with
// a real agent and ctl. Nothing substitutes the graph, worker or transport.
package broker

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"log/slog"
	"os"
	"path/filepath"
	"runtime"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/LinZiyang666/tether/internal/adminsock"
	"github.com/LinZiyang666/tether/internal/agent"
	"github.com/LinZiyang666/tether/internal/agentprov"
	"github.com/LinZiyang666/tether/internal/auth"
	"github.com/LinZiyang666/tether/internal/brokerimage"
	"github.com/LinZiyang666/tether/internal/cli"
	"github.com/LinZiyang666/tether/internal/controlbudget"
	"github.com/LinZiyang666/tether/internal/natsinbox"
	"github.com/LinZiyang666/tether/internal/storage"
	"github.com/LinZiyang666/tether/internal/testharness"
	"github.com/LinZiyang666/tether/internal/xferclient"
	x "github.com/LinZiyang666/tether/internal/xfercontract"
	"github.com/LinZiyang666/tether/test/stackharness"
	"github.com/nats-io/nats-server/v2/server"
	natsservertest "github.com/nats-io/nats-server/v2/test"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nkeys"
)

// A broker image cut joins the continuation graph with its store users (and
// retires its bucket-config writer); the current-store rollback reinstalls it
// from the durable ledger under the SAME origin and the writer's successor
// generation. A transfer admitted before the cut finishes after it with its
// exact receipt and bytes: the tracker re-adopted its row instead of
// forgetting or reaping it.
// origin: hot-update S7 plan §5.3 / §8.2 / P24 / T36 / T37 — S7 production activation
func TestTransferGraphSurvivesBrokerCutRollback(t *testing.T) {
	// origin: hot-update S10 plan §11.1 / TEST-BUDGET
	if runIsolatedBrokerTest(t) {
		return
	}
	g, fd := runtime.NumGoroutine(), testharness.FDCount()
	for round := 0; round < 5; round++ {
		t.Run("round-"+strconv.Itoa(round), func(t *testing.T) { exerciseTransferGraphRollback(t) })
	}
	testharness.AssertNoGoroutineLeak(t, "transfer activation", g)
	testharness.AssertNoFDLeak(t, "transfer activation", fd, 0)
}

func exerciseTransferGraphRollback(t *testing.T) {
	t.Helper()

	if err := brokerimage.InstallColdSingleStoreSupport(); err != nil {
		t.Fatal(err)
	}
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "state.db")
	store, db, err := brokerimage.OpenSingleStore(brokerimage.SingleStoreConfig{DBPath: dbPath, Registry: storage.DefaultDriverRegistry()})
	if err != nil {
		t.Fatal(err)
	}
	id, err := cli.EnsureIdentity(t.TempDir())
	if err != nil {
		_ = brokerimage.CloseStoreOwner(store)
		t.Fatal(err)
	}
	stackharness.SeedSession(t, db, "lab", id.Fingerprint)
	// The original agent must install and sign the single-mode route hold.
	// Provision before Run; the measured cut never fabricates capability rows.
	// origin: S6 lead decision D-C / plan §7.1
	account, err := nkeys.CreateAccount()
	if err != nil {
		t.Fatal(err)
	}
	defer account.Wipe()
	accountSeed, _ := account.Seed()
	accountPub, _ := account.PublicKey()
	brokerSeed, err := auth.GenerateUserSeed()
	if err != nil {
		t.Fatal(err)
	}
	agentID, err := cli.EnsureAgentIdentity(t.TempDir(), "lab")
	if err != nil {
		t.Fatal(err)
	}
	if err := agentprov.Provision(db, "lab", "graph-cut", agentID.Fingerprint, time.Now()); err != nil {
		t.Fatal(err)
	}
	executable, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	args := []string{executable, "serve", "--db", dbPath}
	image, err := NewImageRun(args, nil)
	if err != nil {
		_ = brokerimage.CloseStoreOwner(store)
		t.Fatal(err)
	}
	// The exec target never exists: the cut runs to its exec latch, then the
	// same process must recover the current store. Preflight checks a staged
	// artifact that this fixture does not have.
	image.CandidatePath = filepath.Join(dir, "missing-candidate")
	image.Preflight = func(b *Broker, d controlbudget.Deadline) error {
		if _, err := image.Capture(b, d); err != nil {
			return err
		}
		config, err := brokerimage.BootConfigBytes(args)
		if err != nil {
			return err
		}
		hash, err := runningBinarySHA256()
		if err != nil {
			return err
		}
		c := b.imageControl
		c.args, c.exporter, c.candidate, c.previous = args, hash, hash, hash
		digest := sha256.Sum256(config)
		c.configDigest = hex.EncodeToString(digest[:])
		c.cutID = c.localMaintenance.Record.CutID
		return nil
	}
	var execReached atomic.Bool
	latch := image.WithExecLatch
	image.WithExecLatch = func(exec func() error) error {
		return latch(func() error { execReached.Store(true); return exec() })
	}
	callout := &AuthCalloutConfig{BrokerNkeySeed: brokerSeed, AccountSeed: accountSeed}
	url := graphCutJetStream(t, callout)
	ready := make(chan struct{})
	b, err := New(Config{NATSURL: url, DB: db, DBPath: dbPath, SingleStore: store, ImageRun: image, ReadyCh: ready, Logger: slog.Default(), AuthCallout: callout,
		ReconcileInterval: 50 * time.Millisecond, StaleAfter: 300 * time.Millisecond, OfflineAfter: 900 * time.Millisecond})
	if err != nil {
		_ = brokerimage.CloseStoreOwner(store)
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background()) // ctx-root: isolated broker Run for this cut.
	done := make(chan error, 1)
	go func() { done <- b.Run(ctx) }()
	defer func() {
		cancel()
		select {
		case <-done:
		case <-time.After(15 * time.Second):
			t.Error("broker did not stop")
		}
	}()
	select {
	case <-ready:
	case err := <-done:
		done <- err
		t.Fatalf("startup: %v", err)
	case <-time.After(10 * time.Second):
		t.Fatal("broker did not become ready")
	}
	remote := t.TempDir()
	stopAgent := startGraphCutAgent(t, url, remote, agentID, accountPub)
	defer stopAgent()
	testharness.WaitNodeOnline(t, db, "lab", "graph-cut", 5*time.Second)
	nc, _, err := natsinbox.Connect(url, id.PublicKey, []nats.Option{
		nats.Name(cli.CtlNameForSession("lab")),
		nats.Nkey(id.PublicKey, func(nonce []byte) ([]byte, error) { return auth.SignWithSeed(id.Seed, nonce) }),
	})
	if err != nil {
		t.Fatal(err)
	}
	defer nc.Close()
	before := b.transferRuntime.Load()
	if before == nil {
		t.Fatal("default continuation graph is not installed")
	}
	payload := make([]byte, 2<<20+777)
	_, _ = rand.Read(payload)
	src := filepath.Join(t.TempDir(), "src.bin")
	if err := os.WriteFile(src, payload, 0o600); err != nil {
		t.Fatal(err)
	}
	dst := filepath.Join(remote, "dst.bin")
	var after *BrokerTransfers
	receipt := runGraphCutTransfer(t, nc, url, id, dst, src, func() {
		// Cut while the ctl streams its object: the broker holds only the
		// tracker's durable row, which the rollback must re-adopt.
		d, err := brokerCutDeadline()
		if err != nil {
			t.Fatal(err)
		}
		if err := captureBrokerReloadAdmission(b, adminsock.Request{}, d); err != nil {
			t.Fatalf("signed single-mode cut admission: %v", err)
		}
		b.RequestReExec()
		testharness.WaitFor(t, 30*time.Second, 20*time.Millisecond, func() bool {
			current := b.transferRuntime.Load()
			if current != nil && current != before {
				after = current
				return true
			}
			return false
		})
	})
	if receipt.Kind != "complete" || receipt.Bytes != int64(len(payload)) || receipt.HistorySeq == 0 {
		t.Fatalf("receipt across the broker cut: %+v", receipt)
	}
	if got, err := os.ReadFile(dst); err != nil || !bytes.Equal(got, payload) {
		t.Fatalf("bytes across the broker cut: %v", err)
	}
	if !execReached.Load() {
		t.Fatal("rollback occurred before the actual exec latch")
	}
	if after == nil || after.Origin != before.Origin {
		t.Fatalf("rollback changed the continuation origin: before=%+v after=%+v", before.Origin, after)
	}
	record, err := os.ReadFile(filepath.Join(dir, "xfer-continuation", before.Origin.InstanceID, "config-writer.json"))
	if err != nil || !bytes.Contains(record, []byte(`"generation":1`)) {
		t.Fatalf("the cut did not record the retired config writer: %s %v", record, err)
	}
}

func graphCutJetStream(t *testing.T, callout *AuthCalloutConfig) string {
	t.Helper()
	brokerPub, err := auth.PublicKeyFromSeed(callout.BrokerNkeySeed)
	if err != nil {
		t.Fatal(err)
	}
	accountPub, err := auth.PublicKeyFromSeed(callout.AccountSeed)
	if err != nil {
		t.Fatal(err)
	}
	srv := natsservertest.RunServer(&server.Options{Host: "127.0.0.1", Port: -1, JetStream: true, StoreDir: t.TempDir(), AlwaysEnableNonce: true,
		Nkeys: []*server.NkeyUser{{Nkey: brokerPub}}, AuthCallout: &server.AuthCallout{Issuer: accountPub, Account: "$G", AuthUsers: []string{brokerPub}}})
	t.Cleanup(func() { srv.Shutdown(); srv.WaitForShutdown() })
	if !srv.ReadyForConnections(2*time.Second) || !testharness.WaitFor(t, 2*time.Second, 20*time.Millisecond, srv.JetStreamIsCurrent) {
		t.Fatal("embedded JetStream server not ready")
	}
	return srv.ClientURL()
}

func startGraphCutAgent(t *testing.T, url, root string, identity *cli.Identity, accountPub string) func() {
	t.Helper()
	a, err := agent.New(agent.Config{NATSURL: url, SID: "lab", NID: "graph-cut", Home: t.TempDir(), Logger: silentLogger(), Identity: identity, AccountPub: accountPub, ProxyOptOut: true,
		HeartbeatInterval: 100 * time.Millisecond, RegisterTimeout: 2 * time.Second, RemoteFSMode: "off",
		AllowRoots: []string{root}, RootsConfigured: true})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background()) // ctx-root: isolated agent Run for this cut.
	done := make(chan error, 1)
	go func() { done <- a.Run(ctx) }()
	var once sync.Once
	return func() {
		once.Do(func() {
			cancel()
			select {
			case <-done:
			case <-time.After(10 * time.Second):
				t.Error("agent did not stop")
			}
		})
	}
}

// runGraphCutTransfer pushes at a bounded rate and calls cut once, after the
// prepare returned, while the ctl is still writing its object.
func runGraphCutTransfer(t *testing.T, nc *nats.Conn, url string, id *cli.Identity, remotePath, localPath string, cut func()) x.TerminalReceipt {
	t.Helper()
	deadline, err := controlbudget.After(120 * time.Second)
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
	const tid = "graphcut"
	prepared := make(chan struct{})
	var once sync.Once
	cfg.Observe = func(o x.Observation) {
		if o.Attempt.LogicalID == tid+"-"+string(x.OpPrepare) {
			once.Do(func() { close(prepared) })
		}
	}
	cfg.Identity.SID, cfg.Identity.NID, cfg.Identity.TransferID = "lab", "graph-cut", tid
	cfg.Intent = x.Intent{Verb: "push", Path: remotePath, Force: true, RateLimitBPS: 512 << 10}
	if cfg.Source, err = os.Open(localPath); err != nil {
		t.Fatal(err)
	}
	info, _ := cfg.Source.Stat()
	cfg.Intent.Size = info.Size()
	for {
		legacy, err := cli.SelectTransferClient(&cfg, deadline)
		if errors.Is(err, nats.ErrNoResponders) {
			time.Sleep(50 * time.Millisecond)
			continue
		}
		if err != nil || legacy {
			t.Fatalf("transfer-v1 was not selected: legacy=%t err=%v", legacy, err)
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
		t.Fatalf("finished before its prepare was observed: %+v %v", r.receipt, r.err)
	}
	time.Sleep(300 * time.Millisecond) // sleep-fixture: lets the ctl's object writes begin before the cut.
	cut()
	r := <-done
	stop, _ := controlbudget.After(5 * time.Second)
	if err := invocation.Stop(stop); err != nil {
		t.Errorf("stop: %v", err)
	}
	if r.err != nil {
		t.Fatalf("transfer across the broker cut: %v", r.err)
	}
	return r.receipt
}
