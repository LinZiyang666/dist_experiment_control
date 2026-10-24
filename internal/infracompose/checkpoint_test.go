package infracompose

import (
	"errors"
	"testing"
	"time"

	"github.com/LinZiyang666/tether/internal/controlbudget"
	c "github.com/LinZiyang666/tether/internal/infracontract"
)

// These tables model the two independent seal owners and verify their exact
// operands. They do not certify root effects, physical census or an infra cut.
type checkpointOwner struct {
	c.Cohort
	raw   c.Checkpoint
	calls int
	err   error
}

func (o *checkpointOwner) Seal(c.Plan, c.Deadline) (c.Checkpoint, error) { return o.raw, nil }
func (o *checkpointOwner) check(cp c.Checkpoint) error {
	if cp != o.raw {
		return errors.New("modified raw participant checkpoint")
	}
	o.calls++
	return o.err
}
func (o *checkpointOwner) Install(cp c.Checkpoint, _ c.RouteProof, _ c.Deadline) error {
	return o.check(cp)
}
func (o *checkpointOwner) Move(cp c.Checkpoint, _ c.Deadline, _ c.Stop) (c.MoveResult, error) {
	return c.MoveResult{}, o.check(cp)
}
func (o *checkpointOwner) Return(cp c.Checkpoint, _ c.Deadline, _ c.Stop) (c.MoveResult, error) {
	return c.MoveResult{}, o.check(cp)
}
func (o *checkpointOwner) Bind(cp c.Checkpoint, _ []c.RouteProof, _ c.Deadline) (c.Census, error) {
	return c.Census{}, o.check(cp)
}
func (o *checkpointOwner) Release(cp c.Checkpoint, _ c.Deadline) error { return o.check(cp) }

type recoveryOwner struct {
	c.Recovery
	cp    c.Checkpoint
	reads int
	err   error
	drift bool
}

func (o *recoveryOwner) Seal(c.Plan, c.Deadline) (c.Checkpoint, error) {
	o.reads++
	return o.cp, o.err
}
func (o *recoveryOwner) ReadSeal(c.Plan, c.Deadline, c.Stop) (c.AssetSeal, error) {
	s := o.cp.Assets
	if o.drift {
		s.Revision++
	}
	return s, o.err
}

func checkpointDeadline(t *testing.T) c.Deadline {
	t.Helper()
	d, err := controlbudget.After(5 * time.Second)
	if err != nil {
		t.Fatal(err)
	}
	return d
}

func checkpointFixture() (*checkpointCohort, *checkpointOwner, *recoveryOwner) {
	raw := &checkpointOwner{raw: c.Checkpoint{Cohort: c.ID{1}, Proof: c.Digest{1},
		Assets: c.AssetSeal{Revision: 11, Participants: c.Digest{11}}}}
	recovery := &recoveryOwner{cp: c.Checkpoint{Cohort: c.ID{2}, Proof: c.Digest{2},
		Assets: c.AssetSeal{Revision: 22, Accounts: c.Digest{22}, Assets: c.Digest{23}, Participants: c.Digest{24}, Complete: true},
		Transfers: c.TransferSeal{Version: 1, SourceSHA256: c.Digest{31}, SchemaSHA256: c.Digest{32},
			InvocationSet: c.Digest{33}, ReceiptSet: c.Digest{34}, ConsumerSet: c.Digest{35}, Complete: true}}}
	return &checkpointCohort{raw: raw, recovery: recovery}, raw, recovery
}

// origin: hot-update S9 plan §2.1 / §4.3 / P22 / NATS-COMPOSITION
func TestCheckpointCompositionPreservesBothRawSeals(t *testing.T) {
	o, raw, recovery := checkpointFixture()
	originalRaw, originalRecovery := raw.raw, recovery.cp
	p := c.Plan{Component: c.NATS, Tx: c.ID{2}, Step: 1}
	d := checkpointDeadline(t)
	cp, err := o.Seal(p, d)
	if err != nil || !completeRecovery(cp) || cp.Cohort != raw.raw.Cohort || cp.Assets != recovery.cp.Assets ||
		cp.Transfers != recovery.cp.Transfers || cp.Proof == raw.raw.Proof || cp.Proof == recovery.cp.Proof {
		t.Fatalf("independent owner seals not composed: %+v, %v", cp, err)
	}
	if retry, err := o.Seal(p, d); err != nil || retry != cp {
		t.Fatal("same-plan retry changed the aggregate identity", err)
	}
	if err := o.Install(cp, c.RouteProof{}, d); err != nil {
		t.Fatal(err)
	}
	if _, err := o.Move(cp, d, nil); err != nil {
		t.Fatal(err)
	}
	if _, err := o.Return(cp, d, nil); err != nil {
		t.Fatal(err)
	}
	if _, err := o.Bind(cp, nil, d); err != nil {
		t.Fatal(err)
	}
	if err := o.Release(cp, d); err != nil {
		t.Fatal(err)
	}
	if err := o.Release(cp, d); err != nil || raw.calls != 5 {
		t.Fatal("release retry repeated the owner effect", err, raw.calls)
	}
	if raw.raw != originalRaw || recovery.cp != originalRecovery {
		t.Fatal("composition edited an owner's checkpoint")
	}
	if err := o.Install(cp, c.RouteProof{}, d); err == nil {
		t.Fatal("released aggregate still authorized an action")
	}
	other, _, _ := checkpointFixture()
	if err := other.Install(cp, c.RouteProof{}, d); err == nil {
		t.Fatal("unbound aggregate authorized a new controller's action")
	}
	// Each independent commitment must affect the aggregate, even when all
	// other fields happen to match a previous successful observation.
	for _, mutate := range []func(){func() { raw.raw.Assets.Participants[0]++ }, func() { recovery.cp.Proof[0]++ }} {
		mutate()
		next, err := o.Seal(p, d)
		if err != nil || next.Proof == cp.Proof {
			t.Fatal("aggregate did not bind both original proofs", err)
		}
	}
}

// origin: hot-update S9 plan §4.3 / P22 / NATS-COMPOSITION
func TestCheckpointCompositionRefusesIncompleteAndKeepsCleanupHandle(t *testing.T) {
	for _, defect := range []string{"recovery", "participants", "transfers", "drift", "read_error"} {
		t.Run(defect, func(t *testing.T) {
			o, raw, recovery := checkpointFixture()
			switch defect {
			case "recovery":
				recovery.cp.Assets.Complete = false
			case "participants":
				raw.raw.Assets.Participants = c.Digest{}
			case "transfers":
				recovery.cp.Transfers.Complete = false
			case "drift":
				recovery.drift = true
			case "read_error":
				recovery.err = c.ErrIncomplete
			}
			d := checkpointDeadline(t)
			cp, err := o.Seal(c.Plan{Component: c.NATS, Tx: c.ID{2}}, d)
			if err == nil {
				t.Fatal("incomplete owner became a complete aggregate")
			}
			if defect == "drift" {
				if err := o.Install(cp, c.RouteProof{}, d); err == nil {
					t.Fatal("unverified cleanup handle authorized an action")
				}
				if cp.Cohort == (c.ID{}) || o.Release(cp, d) != nil || raw.calls != 1 {
					t.Fatal("post-acquisition failure lost the original cleanup handle")
				}
			}
		})
	}
}

// origin: hot-update S9 plan §5 / P22 / NATS-COMPOSITION
func TestCaddyCheckpointBorrowsOnlyShortAssetFence(t *testing.T) {
	o, raw, recovery := checkpointFixture()
	d := checkpointDeadline(t)
	cp, err := o.Seal(c.Plan{Component: c.Caddy}, d)
	if err != nil || !completeAssets(cp.Assets) || cp.Transfers != (c.TransferSeal{}) || recovery.reads != 0 {
		t.Fatal("Caddy borrowed an unreleasable NATS recovery seal", err)
	}
	raw.err = c.ErrIncomplete
	if err := o.Release(cp, d); err == nil {
		t.Fatal("failed owner release was hidden")
	}
	raw.err = nil
	if err := o.Release(cp, d); err != nil || raw.calls != 2 {
		t.Fatal("failed release could not retry the original handle", err)
	}
	for _, mutate := range []func(*c.Checkpoint){
		func(cp *c.Checkpoint) { cp.Proof[0]++ }, func(cp *c.Checkpoint) { cp.Assets.Accounts[0]++ },
		func(cp *c.Checkpoint) { cp.Cohort[0]++ }, func(cp *c.Checkpoint) { cp.Transfers.Version++ },
	} {
		forged := cp
		mutate(&forged)
		if err := o.Release(forged, d); err == nil {
			t.Fatal("modified aggregate released a raw owner")
		}
	}
}
