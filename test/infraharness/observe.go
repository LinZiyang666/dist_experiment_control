package infraharness

import (
	"errors"
	"sync"

	"github.com/LinZiyang666/tether/internal/infracontract"
)

// Producer is independent reader evidence, registered for this transaction and
// step. It must come from the actual emitter and raw oracle, never from Status.
type Producer struct {
	Tx         infracontract.ID
	Step       uint32
	Kind       string
	BootID     string
	Subject    infracontract.ID
	Generation uint64
	Evidence   infracontract.Digest
}

// Observer validates observations against producer/evidence custody. It is a
// reader adapter, not a substitute for an actual emitter or physical oracle.
type Observer struct {
	mu        sync.Mutex
	producers map[Producer]bool
	rows      []infracontract.Observation
}

var _ infracontract.Observer = (*Observer)(nil)

// Register installs a nonempty independent producer before accepting its row.
func (r *Observer) Register(p Producer) error {
	if err := validateProducer(p); err != nil {
		return err
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.producers == nil {
		r.producers = make(map[Producer]bool)
	}
	r.producers[p] = true
	return nil
}

// origin: hot-update S9 plan §2.3 / T58
func validateProducer(p Producer) error {
	if p.Tx == (infracontract.ID{}) || p.Step == 0 || p.BootID == "" ||
		p.Subject == (infracontract.ID{}) || p.Generation == 0 || p.Evidence == (infracontract.Digest{}) {
		return errors.New("infra harness: incomplete observation producer")
	}
	for _, kind := range infracontract.ObservationKinds() {
		if p.Kind == kind {
			return nil
		}
	}
	return errors.New("infra harness: unknown observation kind")
}

// Record rejects a state projection, empty evidence or invented generation.
// Matching includes Tx/Step so a rollback cannot reuse an earlier hop's proof.
// origin: hot-update S9 plan §2.3 / T58
func (r *Observer) Record(o infracontract.Observation) error {
	p := Producer{Tx: o.Tx, Step: o.Step, Kind: o.Kind, BootID: o.BootID,
		Subject: o.Subject, Generation: o.Generation, Evidence: o.Evidence}
	if o.Version != 1 || o.BootNS <= 0 {
		return errors.New("infra harness: invalid observation clock or version")
	}
	if err := validateProducer(p); err != nil {
		return err
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if !r.producers[p] {
		return errors.New("infra harness: observation has no independent producer")
	}
	r.rows = append(r.rows, o)
	return nil
}

// Observations returns independent storage so readers cannot alter receipts.
func (r *Observer) Observations() []infracontract.Observation {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]infracontract.Observation(nil), r.rows...)
}
