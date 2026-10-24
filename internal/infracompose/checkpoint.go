package infracompose

import (
	"crypto/sha256"
	"encoding/json"
	"sync"

	"github.com/LinZiyang666/tether/internal/controlbudget"
	c "github.com/LinZiyang666/tether/internal/infracontract"
)

// The registry owns an intentionally partial participant checkpoint. P18 owns
// producer/asset/ACK identity. Neither raw checkpoint may be edited and returned
// to its owner: both owners compare the entire original value on every action.
// The aggregate proof commits to both; dispatch translates only exact aggregates.
// origin: hot-update S9 plan §2.1 / §4.3 / P22
type checkpointCohort struct {
	mu       sync.Mutex
	raw      c.Cohort
	recovery Recovery
	bindings map[c.Checkpoint]checkpointBinding
}

type checkpointBinding struct {
	Plan     c.Plan
	Registry c.Checkpoint
	Recovery c.Checkpoint
	Verified bool
	Released bool
}

func completeAssets(s c.AssetSeal) bool {
	return s.Complete && s.Revision != 0 && s.Accounts != (c.Digest{}) &&
		s.Assets != (c.Digest{}) && s.Participants != (c.Digest{})
}

func completeRecovery(cp c.Checkpoint) bool {
	s := cp.Transfers
	return cp.Cohort != (c.ID{}) && cp.Proof != (c.Digest{}) && completeAssets(cp.Assets) &&
		s.Complete && s.Version == 1 && s.SourceSHA256 != (c.Digest{}) && s.SchemaSHA256 != (c.Digest{}) &&
		s.InvocationSet != (c.Digest{}) && s.ReceiptSet != (c.Digest{}) && s.ConsumerSet != (c.Digest{})
}

func (o *checkpointCohort) Seal(p c.Plan, d c.Deadline) (c.Checkpoint, error) {
	if _, err := controlbudget.Remaining(d); err != nil {
		return c.Checkpoint{}, err
	}
	if !o.mu.TryLock() {
		return c.Checkpoint{}, c.ErrIncomplete
	}
	defer o.mu.Unlock()
	var recovery c.Checkpoint
	var err error
	switch p.Component {
	case c.NATS:
		// This is an idempotent same-plan read of the driver's already-held
		// recovery seal. Its cohort and proof remain P18's original identity.
		recovery, err = o.recovery.Seal(p, d)
		if err == nil && (!completeRecovery(recovery) || recovery.Cohort != p.Tx) {
			err = c.ErrIncomplete
		}
	case c.Caddy:
		// Caddy does not restart JS. Borrow the short asset fence, rather than
		// taking a producer recovery seal that its driver never releases.
		recovery.Assets, err = o.recovery.ReadSeal(p, d, nil)
		if err == nil && !completeAssets(recovery.Assets) {
			err = c.ErrIncomplete
		}
	case c.HostSupport:
		err = c.ErrUnsupported
	default:
		err = c.ErrUnsupported
	}
	if err != nil {
		return c.Checkpoint{}, err
	}
	raw, err := o.raw.Seal(p, d)
	if err != nil {
		return c.Checkpoint{}, err
	}
	if raw.Cohort == (c.ID{}) || raw.Proof == (c.Digest{}) || raw.Assets.Revision == 0 ||
		raw.Assets.Participants == (c.Digest{}) {
		return c.Checkpoint{}, c.ErrIncomplete
	}
	binding := checkpointBinding{Plan: p, Registry: raw, Recovery: recovery}
	data, err := json.Marshal(binding)
	if err != nil {
		return c.Checkpoint{}, c.ErrIncomplete
	}
	cp := c.Checkpoint{Cohort: raw.Cohort, Assets: recovery.Assets, Transfers: recovery.Transfers,
		Proof: sha256.Sum256(append([]byte("tether-infra-checkpoint-v1\n"), data...))}
	if o.bindings == nil {
		o.bindings = make(map[c.Checkpoint]checkpointBinding)
	}
	o.bindings[cp] = binding
	// Keep the acquired registry handle even on this failure. The driver's
	// pre-effect cleanup must still be able to release that exact admission seal.
	after, err := o.recovery.ReadSeal(p, d, nil)
	if err != nil || after != recovery.Assets {
		return cp, c.ErrIncomplete
	}
	binding.Verified = true
	o.bindings[cp] = binding
	return cp, nil
}

func (o *checkpointCohort) original(cp c.Checkpoint, d c.Deadline) (c.Checkpoint, error) {
	if _, err := controlbudget.Remaining(d); err != nil {
		return c.Checkpoint{}, err
	}
	o.mu.Lock()
	defer o.mu.Unlock()
	b, ok := o.bindings[cp]
	if !ok || b.Released || !b.Verified {
		return c.Checkpoint{}, c.ErrIncomplete
	}
	return b.Registry, nil
}

func (o *checkpointCohort) Install(cp c.Checkpoint, proof c.RouteProof, d c.Deadline) error {
	raw, err := o.original(cp, d)
	if err != nil {
		return err
	}
	return o.raw.Install(raw, proof, d)
}

func (o *checkpointCohort) Move(cp c.Checkpoint, d c.Deadline, stop c.Stop) (c.MoveResult, error) {
	raw, err := o.original(cp, d)
	if err != nil {
		return c.MoveResult{}, err
	}
	return o.raw.Move(raw, d, stop)
}

func (o *checkpointCohort) Return(cp c.Checkpoint, d c.Deadline, stop c.Stop) (c.MoveResult, error) {
	raw, err := o.original(cp, d)
	if err != nil {
		return c.MoveResult{}, err
	}
	return o.raw.Return(raw, d, stop)
}

func (o *checkpointCohort) Bind(cp c.Checkpoint, proofs []c.RouteProof, d c.Deadline) (c.Census, error) {
	raw, err := o.original(cp, d)
	if err != nil {
		return c.Census{}, err
	}
	return o.raw.Bind(raw, proofs, d)
}

func (o *checkpointCohort) Release(cp c.Checkpoint, d c.Deadline) error {
	if _, err := controlbudget.Remaining(d); err != nil {
		return err
	}
	o.mu.Lock()
	defer o.mu.Unlock()
	b, ok := o.bindings[cp]
	if !ok {
		return c.ErrIncomplete
	}
	if b.Released {
		return nil
	}
	if err := o.raw.Release(b.Registry, d); err != nil {
		return err
	}
	b.Released = true
	o.bindings[cp] = b
	return nil
}
