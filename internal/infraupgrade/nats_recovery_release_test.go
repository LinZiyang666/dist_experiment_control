package infraupgrade

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"runtime"
	"testing"
	"time"

	"github.com/LinZiyang666/tether/internal/controlbudget"
	c "github.com/LinZiyang666/tether/internal/infracontract"
	"github.com/LinZiyang666/tether/internal/infrarepair"
	"github.com/LinZiyang666/tether/internal/testharness"
	"github.com/LinZiyang666/tether/internal/upgradebudget"
	ns "github.com/nats-io/nats-server/v2/server"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

// This adapter records calls but delegates every recovery decision to the real
// product gate. The driver fixture supplies only the unrelated host/route seams.
type natsRecoveryGate struct {
	*infrarepair.Gate
	f                *natsFixture
	stream           jetstream.Stream
	sealPlans        []c.Plan
	sealDeadlines    []c.Deadline
	verifyDeadlines  []c.Deadline
	releaseDeadlines []c.Deadline
	checkpoints      []c.Checkpoint
	releaseCalls     int
	checks           int
	partial          bool
	unavailable      bool
	verifyStop       c.Stop
}

func (g *natsRecoveryGate) Seal(p c.Plan, deadline c.Deadline) (c.Checkpoint, error) {
	g.sealPlans = append(g.sealPlans, p)
	g.sealDeadlines = append(g.sealDeadlines, deadline)
	cp, err := g.Gate.Seal(p, deadline)
	if err == nil {
		g.f.seal, g.f.checkpoint = cp.Assets, cp
	}
	return cp, err
}

func (g *natsRecoveryGate) VerifyBeforeReplay(cp c.Checkpoint, deadline c.Deadline, stop c.Stop) error {
	g.checkpoints = append(g.checkpoints, cp)
	g.verifyDeadlines = append(g.verifyDeadlines, deadline)
	g.verifyStop = stop
	return g.Gate.VerifyBeforeReplay(cp, deadline, stop)
}

func (g *natsRecoveryGate) Release(cp c.Checkpoint, deadline c.Deadline) error {
	g.releaseCalls++
	g.releaseDeadlines = append(g.releaseDeadlines, deadline)
	return g.Gate.Release(cp, deadline)
}

func newNATSRecoveryGate(t *testing.T) (*natsFixture, *natsRecoveryGate) {
	t.Helper()
	f := newNATSFixture(t, true)
	server, err := ns.NewServer(&ns.Options{Host: "127.0.0.1", Port: -1, NoLog: true, NoSigs: true,
		JetStream: true, StoreDir: t.TempDir()})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { server.Shutdown(); server.WaitForShutdown() })
	server.Start()
	if !server.ReadyForConnections(3 * time.Second) {
		t.Fatal("embedded NATS did not start")
	}
	nc, err := nats.Connect(server.ClientURL(), nats.NoReconnect())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(nc.Close)
	js, err := jetstream.New(nc)
	if err != nil {
		t.Fatal(err)
	}
	stream, err := js.CreateStream(t.Context(), jetstream.StreamConfig{Name: "ACKS", Subjects: []string{"receipts"}, Storage: jetstream.FileStorage})
	if err != nil {
		t.Fatal(err)
	}
	var original infrarepair.Responsibilities
	for _, payload := range []string{"first acknowledged chunk", "second acknowledged chunk"} {
		ack, err := js.Publish(t.Context(), "receipts", []byte(payload))
		if err != nil {
			t.Fatal(err)
		}
		original.ACKs = append(original.ACKs, infrarepair.ACK{Account: "$G", Stream: ack.Stream,
			Subject: "receipts", Sequence: ack.Sequence, Message: infrarepair.MessageDigest(nil, []byte(payload))})
	}
	g := &natsRecoveryGate{f: f, stream: stream}
	g.Gate, err = infrarepair.New(infrarepair.Config{Domain: f.plan.Domain, Required: []string{"retained-writer"},
		Accounts: map[string]jetstream.JetStream{"$G": js},
		Assets: func(deadline c.Deadline, _ c.Stop) (c.AssetSeal, error) {
			remaining, err := controlbudget.Remaining(deadline)
			if err != nil {
				return c.AssetSeal{}, err
			}
			ctx, cancel := context.WithTimeout(t.Context(), remaining)
			defer cancel()
			info, err := stream.Info(ctx)
			if err != nil {
				return c.AssetSeal{}, err
			}
			// Bind the actual stream identity/config, independently of LastSeq.
			data, err := json.Marshal(struct {
				Created time.Time
				Config  jetstream.StreamConfig
			}{info.Created, info.Config})
			if err != nil {
				return c.AssetSeal{}, err
			}
			return c.AssetSeal{Complete: true, Accounts: c.Digest{1}, Assets: sha256.Sum256(data)}, nil
		},
		Exclusive: func(c.Deadline, c.Stop) error { return nil },
		Transfers: func(c.Deadline) (c.TransferSeal, error) {
			if !f.transfersHeld {
				return c.TransferSeal{}, errors.New("original transfer fence was released")
			}
			return f.transfer, nil
		}})
	if err != nil {
		t.Fatal(err)
	}
	if err := g.Register(infrarepair.Producer{Name: "retained-writer",
		Capture: func(c.Deadline) (infrarepair.Responsibilities, error) {
			if g.partial {
				return infrarepair.Responsibilities{}, errors.New("capture temporarily unavailable")
			}
			return original, nil
		},
		Check: func(got infrarepair.Responsibilities, _ c.Deadline, _ c.Stop) error {
			g.checks++
			if len(got.ACKs) != len(original.ACKs) || got.ACKs[0] != original.ACKs[0] {
				return infrarepair.ErrReceipt
			}
			if g.unavailable {
				return errors.New("owner readback temporarily unavailable")
			}
			return nil
		}}); err != nil {
		t.Fatal(err)
	}
	deps := f.dependencies()
	deps.Recovery = g
	f.driver, err = NewNATS(deps)
	if err != nil {
		t.Fatal(err)
	}
	return f, g
}

func assertNATSRecoveryAdmission(t *testing.T, g *natsRecoveryGate, fenced bool) {
	t.Helper()
	permit, err := g.Enter("retained-writer")
	if permit != nil {
		permit.Finish(nil)
	}
	if fenced && !errors.Is(err, infrarepair.ErrFenced) || !fenced && err != nil {
		t.Fatalf("recovery admission fenced=%v: %v", fenced, err)
	}
}

// origin: S9 internal review s9-p18 F1
func TestNATSPreCutRecoveryVerifiesBeforeRelease(t *testing.T) {
	beforeG, beforeFD := runtime.NumGoroutine(), testharness.FDCount()
	for round := 0; round < 5; round++ {
		t.Run("retained-ACKs", func(t *testing.T) { exerciseNATSPreCutRecovery(t) })
	}
	testharness.AssertNoGoroutineLeak(t, "NATS pre-cut recovery", beforeG)
	testharness.AssertNoFDLeak(t, "NATS pre-cut recovery", beforeFD, 0)
}

func exerciseNATSPreCutRecovery(t *testing.T) {
	t.Helper()
	f, g := newNATSRecoveryGate(t)
	f.fail["cohort.seal"] = 1
	stop := make(chan struct{})
	status, err := f.driver.Advance(f.plan, f.deadline, stop)
	if err == nil || status.Phase != c.Deferred {
		t.Fatalf("pre-cut cleanup did not verify and release retained checkpoint: phase=%v err=%v verifies=%d releases=%d", status.Phase, err, len(g.checkpoints), g.releaseCalls)
	}
	if len(g.checkpoints) != 1 || g.checkpoints[0] != f.checkpoint || len(g.sealPlans) != 1 || g.checks != 1 || g.releaseCalls != 1 || g.verifyStop != c.Stop(stop) {
		t.Fatal("cleanup did not use the original checkpoint and cancellation scope")
	}
	if g.releaseDeadlines[0] != g.verifyDeadlines[0] {
		t.Fatal("release renewed the verification budget")
	}
	if f.transfersHeld || f.cohortHeld || f.driver.(*natsDriver).run != nil {
		t.Fatal("verified pre-cut cleanup retained short seals")
	}
	f.noSignals(t)
	assertNATSRecoveryAdmission(t, g, false)
}

// origin: S9 internal review s9-p18 F1
func TestNATSPreCutRecoveryRetainsLostACKFence(t *testing.T) {
	f, g := newNATSRecoveryGate(t)
	f.fail["cohort.seal"] = 1
	f.hook = func(event string) {
		if event == "cohort.seal" {
			if err := g.stream.DeleteMsg(t.Context(), 1); err != nil {
				t.Fatal(err)
			}
		}
	}
	status, err := f.driver.Advance(f.plan, f.deadline, nil)
	if !errors.Is(err, infrarepair.ErrReceipt) || status.Phase != c.Halted || g.releaseCalls != 0 || !f.transfersHeld {
		t.Fatalf("lost acknowledged chunk did not retain recovery fence: phase=%v err=%v releases=%d transferHeld=%v", status.Phase, err, g.releaseCalls, f.transfersHeld)
	}
	retained := f.driver.(*natsDriver).run.recovery
	status, err = f.driver.Recover(f.plan, f.deadline, nil)
	if !errors.Is(err, infrarepair.ErrReceipt) || status.Phase != c.Halted || len(g.sealPlans) != 1 || g.releaseCalls != 0 || retained != f.driver.(*natsDriver).run.recovery {
		t.Fatal("recovery forgot the original lost receipt or recaptured a checkpoint")
	}
	assertNATSRecoveryAdmission(t, g, true)
	f.noSignals(t)
}

// origin: S9 internal review s9-p18 F1
func TestNATSPreCutRecoveryRetriesPartialSeal(t *testing.T) {
	f, g := newNATSRecoveryGate(t)
	g.partial = true
	status, err := f.driver.Advance(f.plan, f.deadline, nil)
	if err == nil || status.Phase != c.Halted || len(g.sealPlans) != 1 || len(g.checkpoints) != 0 || g.releaseCalls != 0 {
		t.Fatalf("partial seal was implicitly retried or released: phase=%v err=%v", status.Phase, err)
	}
	assertNATSRecoveryAdmission(t, g, true)
	// A replacement controller must use the saved plan and retained transfer
	// identity. Completing the gate's partial acquisition is not a fresh attempt.
	g.partial = false
	deps := f.dependencies()
	deps.Recovery = g
	f.driver, err = NewNATS(deps)
	if err != nil {
		t.Fatal(err)
	}
	status, err = f.driver.Recover(f.plan, f.deadline, nil)
	if err == nil || status.Phase != c.Deferred || len(g.sealPlans) != 2 || g.sealPlans[1] != f.plan || len(g.checkpoints) != 1 || g.releaseCalls != 1 {
		t.Fatalf("explicit recovery did not complete the retained partial seal: phase=%v err=%v seals=%d verifies=%d releases=%d", status.Phase, err, len(g.sealPlans), len(g.checkpoints), g.releaseCalls)
	}
	remaining, budgetErr := controlbudget.Remaining(g.sealDeadlines[1])
	if budgetErr != nil || remaining > upgradebudget.RequestLatency || g.sealDeadlines[1] != g.verifyDeadlines[0] || g.releaseDeadlines[0] != g.verifyDeadlines[0] {
		t.Fatal("partial seal, verification and release did not share the business budget")
	}
	assertNATSRecoveryAdmission(t, g, false)
	f.noSignals(t)
}

// origin: S9 internal review s9-p18 F1
func TestNATSPreCutRecoveryUsesBusinessBudget(t *testing.T) {
	for _, short := range []bool{false, true} {
		t.Run(map[bool]string{false: "business-bound", true: "caller-bound"}[short], func(t *testing.T) {
			f, g := newNATSRecoveryGate(t)
			f.fail["cohort.seal"] = 1
			if short {
				var err error
				f.deadline, err = controlbudget.After(upgradebudget.RequestLatency / 2)
				if err != nil {
					t.Fatal(err)
				}
			}
			f.hook = func(event string) {
				if event == "cohort.seal" {
					g.unavailable = true
				}
			}
			_, _ = f.driver.Advance(f.plan, f.deadline, nil)
			if len(g.verifyDeadlines) != 1 {
				t.Fatal("missing bounded pre-cut verification")
			}
			remaining, err := controlbudget.Remaining(g.verifyDeadlines[0])
			if err != nil || remaining > upgradebudget.RequestLatency || g.verifyDeadlines[0] > f.deadline {
				t.Fatalf("verification inherited component budget: remaining=%v err=%v", remaining, err)
			}
			g.unavailable = false
			status, err := f.driver.Recover(f.plan, f.deadline, nil)
			if err == nil || status.Phase != c.Deferred || len(g.sealPlans) != 1 || len(g.checkpoints) != 2 || g.checkpoints[0] != g.checkpoints[1] {
				t.Fatal("transient verification failure lost its original checkpoint")
			}
			assertNATSRecoveryAdmission(t, g, false)
		})
	}
}

// origin: S9 internal review s9-p18 F1
func TestNATSPreCutRecoveryRetainsIncompleteEvidence(t *testing.T) {
	for _, defect := range []string{"checkpoint", "transfer-fence", "transfer-receipt"} {
		t.Run(defect, func(t *testing.T) {
			f, g := newNATSRecoveryGate(t)
			g.partial = defect != "checkpoint"
			f.fail["cohort.seal"] = 1
			f.hook = func(event string) {
				if event == "cohort.seal" {
					f.driver.(*natsDriver).run.recovery.Assets.Complete = false
				}
			}
			status, err := f.driver.Advance(f.plan, f.deadline, nil)
			if err == nil || status.Phase != c.Halted {
				t.Fatal("incomplete acquisition was declared released")
			}
			r := f.driver.(*natsDriver).run
			switch defect {
			case "checkpoint":
			case "transfer-fence":
				r.transfersHeld = false
			case "transfer-receipt":
				r.transfers.SourceSHA256 = c.Digest{}
			}
			retained := r.recovery
			g.partial = false
			status, err = f.driver.Recover(f.plan, f.deadline, nil)
			if err == nil || status.Phase != c.Halted || len(g.sealPlans) != 1 || len(g.checkpoints) != 0 || g.releaseCalls != 0 || r.recovery != retained {
				t.Fatalf("incomplete evidence authorized a new checkpoint or release: phase=%v err=%v seals=%d verifies=%d releases=%d", status.Phase, err, len(g.sealPlans), len(g.checkpoints), g.releaseCalls)
			}
			assertNATSRecoveryAdmission(t, g, true)
			f.noSignals(t)
		})
	}
}
