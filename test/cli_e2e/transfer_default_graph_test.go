package cli_e2e_test

import (
	"bytes"
	"crypto/rand"
	"errors"
	"log/slog"
	"os"
	"path/filepath"
	"runtime"
	"strconv"
	"testing"
	"time"

	"github.com/LinZiyang666/tether/internal/agent"
	"github.com/LinZiyang666/tether/internal/broker"
	"github.com/LinZiyang666/tether/internal/cli"
	"github.com/LinZiyang666/tether/internal/controlbudget"
	"github.com/LinZiyang666/tether/internal/natsinbox"
	"github.com/LinZiyang666/tether/internal/testharness"
	"github.com/LinZiyang666/tether/internal/xferclient"
	x "github.com/LinZiyang666/tether/internal/xfercontract"
	natstest "github.com/nats-io/nats-server/v2/test"
	"github.com/nats-io/nats.go"
)

// A default broker (no injected factory) and a default agent (no injected
// worker) must run a capable tier-B push and pull end to end: probe →
// permit → prepare over the legacy carrier → source seal → fixed-version
// object → durable grant delivered to the original receiver → exact history
// receipt. Each direction runs twice in one session: the second transfer can
// only be admitted if the first one's broker row AND agent entry actually
// settled (both owners keep a single tier-B slot per session until then).
// origin: hot-update S7 plan §0 E2/E4 / §12 P20–P22 — S7 production activation
func TestTransferCapablePushPullThroughDefaultGraph(t *testing.T) {
	// origin: hot-update S10 plan L17 / §11.1 / POLISH-B
	if runIsolatedBinaryTest(t, binaryFixtureTestBudget) {
		return
	}
	g, fd := runtime.NumGoroutine(), testharness.FDCount()
	for round := 0; round < 5; round++ {
		t.Run("round-"+strconv.Itoa(round), func(t *testing.T) { exerciseTransferDefaultGraph(t) })
	}
	testharness.AssertNoGoroutineLeak(t, "transfer activation", g)
	testharness.AssertNoFDLeak(t, "transfer activation", fd, 0)
}

func exerciseTransferDefaultGraph(t *testing.T) {
	t.Helper()

	testharness.GuardProcessDirectory(t, filepath.Dir(t.TempDir()))
	url := startNonceJSNATS(t)
	db := openDB(t)
	id, err := cli.EnsureIdentity(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	seedSession(t, db, "lab", id.Fingerprint)
	// Keep the actual owner refusal when a merged graph fails between phases.
	// One shared handler serializes both daemon logs; cleanup reads after joins.
	// origin: hot-update S7 plan §11 / T35; INTEG-M activation diagnostics
	var logs bytes.Buffer
	logger := slog.New(slog.NewTextHandler(&logs, &slog.HandlerOptions{Level: slog.LevelDebug}))
	t.Cleanup(func() {
		if t.Failed() {
			t.Log(logs.String())
		}
	})
	defer startBroker(t, url, db, func(c *broker.Config) { c.Logger = logger })()
	remote := t.TempDir()
	defer startAgent(t, url, "lab", "a100", withAllowRoots(remote), func(c *agent.Config) { c.Logger = logger })()
	testharness.WaitNodeOnline(t, db, "lab", "a100", 5*time.Second)
	nc, _, err := natsinbox.Connect(url, id.PublicKey, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer nc.Close()
	local := t.TempDir()
	for round := 0; round < 2; round++ {
		payload := make([]byte, 3<<20+12345)
		_, _ = rand.Read(payload)
		src := filepath.Join(local, "src.bin")
		if err := os.WriteFile(src, payload, 0o600); err != nil {
			t.Fatal(err)
		}
		dst := filepath.Join(remote, "pushed.bin")
		receipt := runCapableTransfer(t, nc, url, id, "push", dst, src, "push"+string(rune('a'+round)))
		if receipt.Kind != "complete" || receipt.Bytes != int64(len(payload)) || receipt.HistorySeq == 0 {
			t.Fatalf("round %d push receipt: %+v", round, receipt)
		}
		if got, err := os.ReadFile(dst); err != nil || !bytes.Equal(got, payload) {
			t.Fatalf("round %d pushed bytes differ: %v", round, err)
		}
		pulled := filepath.Join(local, "pulled.bin")
		receipt = runCapableTransfer(t, nc, url, id, "pull", dst, pulled, "pull"+string(rune('a'+round)))
		if receipt.Kind != "complete" || receipt.Bytes != int64(len(payload)) || receipt.HistorySeq == 0 {
			t.Fatalf("round %d pull receipt: %+v", round, receipt)
		}
		if got, err := os.ReadFile(pulled); err != nil || !bytes.Equal(got, payload) {
			t.Fatalf("round %d pulled bytes differ: %v", round, err)
		}
	}
}

// startNonceJSNATS is the JetStream test server with the CONNECT nonce a
// production auth_callout deployment always has: the ctl's dedicated data
// attachment authenticates with its nkey, which a nonce-less server refuses.
func startNonceJSNATS(t *testing.T) string {
	t.Helper()
	opts := natstest.DefaultTestOptions
	opts.Port, opts.JetStream, opts.StoreDir, opts.AlwaysEnableNonce = -1, true, t.TempDir(), true
	ns := natstest.RunServer(&opts)
	t.Cleanup(func() { ns.Shutdown(); ns.WaitForShutdown() })
	if !ns.ReadyForConnections(2*time.Second) || !testharness.WaitFor(t, 2*time.Second, 20*time.Millisecond, ns.JetStreamIsCurrent) {
		t.Fatal("embedded JS nats-server not ready")
	}
	return ns.ClientURL()
}

// runCapableTransfer drives the real ctl engine (the same composition the
// `tether push/pull` command uses) and requires explicit transfer-v1 selection.
// The second same-session transfer retries admission while the first one's
// cleanup finishes, but never accepts a refusal other than the single-slot one.
func runCapableTransfer(t *testing.T, nc *nats.Conn, url string, id *cli.Identity, verb, remotePath, localPath, tid string) x.TerminalReceipt {
	t.Helper()
	deadline, err := controlbudget.After(60 * time.Second)
	if err != nil {
		t.Fatal(err)
	}
	for {
		cfg, closeData, err := cli.TransferClientConfig(nc, url, id, "lab")
		if err != nil {
			t.Fatal(err)
		}
		cfg.Identity.SID, cfg.Identity.NID, cfg.Identity.TransferID = "lab", "a100", tid
		cfg.Intent = x.Intent{Verb: verb, Path: remotePath, Force: true}
		if verb == "push" {
			cfg.Source, err = os.Open(localPath)
			if err != nil {
				t.Fatal(err)
			}
			info, _ := cfg.Source.Stat()
			cfg.Intent.Size = info.Size()
		} else {
			cfg.Destination, err = cli.NewTransferReceiver(localPath, true)
			if err != nil {
				t.Fatal(err)
			}
		}
		legacy, err := cli.SelectTransferClient(&cfg, deadline)
		if err != nil && errors.Is(err, nats.ErrNoResponders) {
			// The broker subscribes its continuation graph after the agent can
			// already register; only "nobody is listening yet" is retried.
			_ = closeData()
			time.Sleep(50 * time.Millisecond)
			continue
		}
		if err != nil || legacy {
			t.Fatalf("%s %s: explicit transfer-v1 was not selected: legacy=%t err=%v", verb, tid, legacy, err)
		}
		invocation, err := xferclient.NewClient(cfg)
		if err != nil {
			t.Fatal(err)
		}
		receipt, runErr := invocation.Run(deadline)
		stop, _ := controlbudget.After(5 * time.Second)
		if err := invocation.Stop(stop); err != nil {
			t.Errorf("%s %s stop: %v", verb, tid, err)
		}
		if err := closeData(); err != nil {
			t.Errorf("%s %s close data: %v", verb, tid, err)
		}
		if runErr == nil {
			return receipt
		}
		var coded *xferclient.Error
		if !errors.As(runErr, &coded) || coded.Code != x.CodeCapacityExceeded {
			t.Fatalf("%s %s: %v", verb, tid, runErr)
		}
		if _, err := controlbudget.Remaining(deadline); err != nil {
			t.Fatalf("%s %s: the previous same-session transfer never settled: %v", verb, tid, runErr)
		}
		// The tracker can already own this identity while the agent's previous
		// courier is joining. Retry that same permit/prepare: minting another
		// ID would strand the admitted row and permanently occupy its slot.
		// origin: hot-update S7 plan §7 / T35; INTEG-M retained preparation
		time.Sleep(200 * time.Millisecond)
	}
}
