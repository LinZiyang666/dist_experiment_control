package infraupgrade

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"testing"

	c "github.com/LinZiyang666/tether/internal/infracontract"
	"github.com/LinZiyang666/tether/internal/upgradelock"
	"github.com/LinZiyang666/tether/internal/upgradetx"
)

func newNATSTestJournal(t *testing.T) *FileTransactionJournal {
	t.Helper()
	dir := t.TempDir()
	if err := os.Chmod(dir, 0o700); err != nil {
		t.Fatal(err)
	}
	canonical := filepath.Join(dir, "canonical")
	if err := os.WriteFile(canonical, []byte("artifact"), 0o600); err != nil {
		t.Fatal(err)
	}
	key := upgradetx.Key{Component: upgradetx.ComponentNATS, ConfiguredNID: "node", ConfigPath: filepath.Join(dir, "config")}
	domain, err := upgradelock.RegisterDomain(upgradelock.Registration{CanonicalPath: canonical, ControlDir: dir,
		DeploymentID: "nats-recovery", Participants: []upgradelock.Participant{{Key: key, ServiceID: "nats"}}})
	if err != nil {
		t.Fatal(err)
	}
	key.DomainID = domain
	return &FileTransactionJournal{Directory: dir, Key: key}
}

// The state-table fixture uses the real private file journal for recovery state.
// Root effects remain the existing fixture: these tests do not claim systemd,
// stock-server or privileged kernel receipts.
func (a natsFixtureTx) LoadNATS(p c.Plan, deadline c.Deadline) (NATSTransition, error) {
	if a.f.transitionJournal == nil {
		var x NATSTransition
		if json.Unmarshal(a.f.transitionBytes, &x) != nil || !natsTransitionValid(x, p) {
			return NATSTransition{}, c.ErrIncomplete
		}
		return x, nil
	}
	state, err := a.f.transitionJournal.Load(p.Tx, deadline)
	if err != nil || state.NATS == nil || !natsTransitionValid(*state.NATS, p) {
		return NATSTransition{}, c.ErrIncomplete
	}
	return *state.NATS, nil
}
func (a natsFixtureTx) StoreNATS(p c.Plan, x NATSTransition, deadline c.Deadline) error {
	if !natsTransitionValid(x, p) {
		return c.ErrIncomplete
	}
	if a.f.transitionJournal == nil {
		var err error
		a.f.transitionBytes, err = json.Marshal(x)
		return err
	}
	j := a.f.transitionJournal
	state, err := j.Load(p.Tx, deadline)
	if errors.Is(err, os.ErrNotExist) {
		state = TransactionState{Version: transactionStateVersion, Plan: p, Deadline: x.Deadline}
		state.Grant.Handle.Key = j.Key
	} else if err != nil {
		return err
	}
	expected := state.Revision
	state.Revision++
	state.NATS = &x
	return j.CompareAndSwap(expected, state, deadline)
}

// origin: S9 internal review rv-infra F08
func TestNATSDurableProgressRetiresResponsibilities(t *testing.T) {
	f := newNATSFixture(t, false)
	f.transitionJournal = newNATSTestJournal(t)
	status, err := f.driver.Advance(f.plan, f.deadline, nil)
	if err != nil || status.Phase != c.Completed {
		t.Fatalf("advance phase=%v error=%v cause=%v events=%v", status.Phase, err, errors.Unwrap(err), f.events)
	}
}

type natsLostPhase struct {
	natsFixtureTx
	phase c.Phase
	lost  bool
}

func (a *natsLostPhase) CompareAndSwap(s c.Status, phase c.Phase, proof c.Digest, dl c.Deadline) (c.Status, error) {
	next, err := a.natsFixtureTx.CompareAndSwap(s, phase, proof, dl)
	if err == nil && phase == a.phase && !a.lost {
		a.lost = true
		return c.Status{}, errors.New("lost phase commit reply")
	}
	return next, err
}

// origin: S9 internal review rv-infra F08
func TestNATSRecoverRestoresEveryEffectWindow(t *testing.T) {
	for _, op := range []c.HostOp{c.FenceStart, c.Promote, c.SignalLDM, c.SignalStop, c.ReleaseStart} {
		t.Run(fmt.Sprintf("lost_root_reply_%d", op), func(t *testing.T) {
			f := newNATSFixture(t, false)
			f.loseReply = op
			status, err := f.driver.Advance(f.plan, f.deadline, nil)
			if err == nil || status.Phase != c.Halted || f.effects[op] != 1 {
				t.Fatal("actual root effect/reply-loss window was not reached")
			}
			before, err := (natsFixtureTx{f}).LoadNATS(f.plan, f.deadline)
			if err != nil || before.Pending == nil || before.Pending.Op != op {
				t.Fatal("exact pending root request was not retained")
			}
			original := *before.Pending
			replacement, err := NewNATS(f.dependencies())
			if err != nil {
				t.Fatal(err)
			}
			status, err = replacement.Recover(f.plan, f.deadline, nil)
			if err != nil || status.Phase != c.Completed || f.effects[op] != 1 {
				t.Fatalf("replacement repeated or stranded root effect %d: %v", op, err)
			}
			repeated := 0
			for _, r := range f.requests {
				if r.Sequence == original.Sequence {
					repeated++
					if !reflect.DeepEqual(r, original) {
						t.Fatal("pending request was re-signed or renewed")
					}
				}
			}
			if repeated != 2 {
				t.Fatal("recovery did not query the original request")
			}
		})
	}
	for _, phase := range []c.Phase{c.Cutting, c.Verifying, c.Returning, c.Retiring, c.Completed} {
		t.Run(fmt.Sprintf("lost_phase_reply_%d", phase), func(t *testing.T) {
			f := newNATSFixture(t, false)
			tx := &natsLostPhase{natsFixtureTx: natsFixtureTx{f}, phase: phase}
			deps := f.dependencies()
			deps.Tx = tx
			driver, err := NewNATS(deps)
			if err != nil {
				t.Fatal(err)
			}
			status, err := driver.Advance(f.plan, f.deadline, nil)
			if err == nil || status.Phase != c.Halted || !tx.lost {
				t.Fatal("actual phase commit/reply-loss window was not reached")
			}
			driver, err = NewNATS(deps)
			if err != nil {
				t.Fatal(err)
			}
			status, err = driver.Recover(f.plan, f.deadline, nil)
			if err != nil || status.Phase != c.Completed || f.effects[c.SignalStop] != 1 {
				t.Fatalf("lost phase %d could not reconcile: %v", phase, err)
			}
		})
	}
}

type natsControllerCrash struct{}
type natsCrashAfterEffect struct {
	natsFixtureHost
	op      c.HostOp
	crashed bool
}

func (h *natsCrashAfterEffect) Do(r c.HostRequest, dl c.Deadline, stop c.Stop) (c.HostReceipt, error) {
	receipt, err := h.natsFixtureHost.Do(r, dl, stop)
	if err == nil && r.Op == h.op && !h.crashed {
		h.crashed = true
		panic(natsControllerCrash{})
	}
	return receipt, err
}
func crashNATSController(t *testing.T, driver c.Driver, p c.Plan, dl c.Deadline) {
	t.Helper()
	defer func() {
		if _, ok := recover().(natsControllerCrash); !ok {
			t.Fatal("controller crash did not hit the post-effect/pre-save window")
		}
	}()
	_, _ = driver.Advance(p, dl, nil)
}

// origin: S9 internal review rv-infra F08
func TestNATSRecoverReadsDurableIntentAfterControllerDeath(t *testing.T) {
	f := newNATSFixture(t, false)
	f.transitionJournal = newNATSTestJournal(t)
	host := &natsCrashAfterEffect{natsFixtureHost: natsFixtureHost{f}, op: c.SignalStop}
	deps := f.dependencies()
	deps.Host = host
	driver, err := NewNATS(deps)
	if err != nil {
		t.Fatal(err)
	}
	crashNATSController(t, driver, f.plan, f.deadline)
	if f.oldAlive || f.effects[c.SignalStop] != 1 {
		t.Fatal("crash happened before the root effect")
	}
	// Re-open the actual private file owner, discarding every driver pointer.
	j := *f.transitionJournal
	f.transitionJournal = &j
	driver, err = NewNATS(deps)
	if err != nil {
		t.Fatal(err)
	}
	status, err := driver.Recover(f.plan, f.deadline, nil)
	if err != nil || status.Phase != c.Completed || f.effects[c.SignalStop] != 1 {
		t.Fatalf("durable intent did not recover: %v", err)
	}
	x, err := (natsFixtureTx{f}).LoadNATS(f.plan, f.deadline)
	if err != nil || x.Pending != nil || !x.Returned || x.CohortHeld || x.RecoveryHeld || x.TransfersHeld {
		t.Fatal("durable final state still owns responsibility")
	}
}

// origin: S9 internal review rv-infra F08
func TestNATSRecoveryKeepsOriginalTimeoutAndRouteAuthority(t *testing.T) {
	for _, fault := range []string{"expired_route", "different_plan", "expired_action", "normal_timeout"} {
		t.Run(fault, func(t *testing.T) {
			f := newNATSFixture(t, false)
			f.fail["install.return.2"] = 1
			status, err := f.driver.Advance(f.plan, f.deadline, nil)
			if err == nil || status.Phase != c.Halted {
				t.Fatal("fixture did not halt during partial installation")
			}
			x, err := (natsFixtureTx{f}).LoadNATS(f.plan, f.deadline)
			if err != nil {
				t.Fatal(err)
			}
			plan, deadline := f.plan, f.deadline
			switch fault {
			case "expired_route":
				x.Inbound.Deadline = 1
			case "different_plan":
				plan.Tx[0]++
			case "expired_action":
				deadline = 1
			case "normal_timeout":
				x.Deadline = 1
			}
			f.transitionBytes, err = json.Marshal(x)
			if err != nil {
				t.Fatal(err)
			}
			driver, err := NewNATS(f.dependencies())
			if err != nil {
				t.Fatal(err)
			}
			prepared := countEvent(f.events, "routes.prepare.return")
			status, err = driver.Recover(plan, deadline, nil)
			if fault == "normal_timeout" {
				stored, loadErr := (natsFixtureTx{f}).LoadNATS(f.plan, f.deadline)
				if err != nil || status.Phase != c.Completed || status.Code != "infra_budget_exceeded" || loadErr != nil || !stored.TimedOut || stored.Deadline != 1 {
					t.Fatal("recovery erased the normal timeout")
				}
			} else if err == nil || status.Phase == c.Completed {
				t.Fatal("recovery reused invalid authority")
			}
			if countEvent(f.events, "routes.prepare.return") != prepared {
				t.Fatal("recovery minted a fresh route nonce/hold")
			}
		})
	}
}

type natsCrashAfterPhase struct {
	natsFixtureTx
	phase   c.Phase
	crashed bool
}

func (a *natsCrashAfterPhase) CompareAndSwap(s c.Status, phase c.Phase, proof c.Digest, dl c.Deadline) (c.Status, error) {
	next, err := a.natsFixtureTx.CompareAndSwap(s, phase, proof, dl)
	if err == nil && phase == a.phase && !a.crashed {
		a.crashed = true
		panic(natsControllerCrash{})
	}
	return next, err
}

// origin: S9 internal review rv-infra F08
func TestNATSRecoverReconcilesPhaseAfterControllerDeath(t *testing.T) {
	for _, phase := range []c.Phase{c.Cutting, c.Verifying, c.Returning, c.Retiring, c.Completed} {
		t.Run(fmt.Sprint(phase), func(t *testing.T) {
			f := newNATSFixture(t, false)
			tx := &natsCrashAfterPhase{natsFixtureTx: natsFixtureTx{f}, phase: phase}
			deps := f.dependencies()
			deps.Tx = tx
			driver, err := NewNATS(deps)
			if err != nil {
				t.Fatal(err)
			}
			crashNATSController(t, driver, f.plan, f.deadline)
			driver, err = NewNATS(deps)
			if err != nil {
				t.Fatal(err)
			}
			status, err := driver.Recover(f.plan, f.deadline, nil)
			if err != nil || status.Phase != c.Completed || f.effects[c.SignalStop] != 1 {
				t.Fatalf("phase %d death stranded the transaction: %v", phase, err)
			}
		})
	}
}

// origin: S9 internal review rv-infra F08
func TestNATSRecoverRestoresVerificationProgress(t *testing.T) {
	f := newNATSFixture(t, false)
	f.fail["recovery.release"] = 1
	status, err := f.driver.Advance(f.plan, f.deadline, nil)
	if err == nil || status.Phase != c.Halted || !f.verified || !f.rebound {
		t.Fatal("release failure did not follow actual verification and rebind")
	}
	driver, err := NewNATS(f.dependencies())
	if err != nil {
		t.Fatal(err)
	}
	status, err = driver.Recover(f.plan, f.deadline, nil)
	if err != nil || status.Phase != c.Completed || countEvent(f.events, "recovery.verify") != 1 || countEvent(f.events, "transfer.rebind") != 1 {
		t.Fatalf("completed verification progress was lost across replacement: %v", err)
	}
}
