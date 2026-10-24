package infraupgrade

import (
	"errors"
	"os"
	"reflect"

	"github.com/LinZiyang666/tether/internal/controlbudget"
	c "github.com/LinZiyang666/tether/internal/infracontract"
	"github.com/LinZiyang666/tether/internal/upgradelock"
	"github.com/LinZiyang666/tether/internal/upgradetx"
)

var _ c.ServiceFactory = NewService

// ServiceTransactions adds plan recovery to the frozen, deliberately small
// Transaction port. Reading a status does not reserve a domain or mint a grant.
// ResumePlan takes the existing domain reservation before a driver can mutate.
// origin: hot-update S9 plan §7.2 / §8 / T44
type ServiceTransactions interface {
	c.Transaction
	Plan(c.ID, c.Deadline) (c.Plan, error)
	ResumePlan(c.ID, c.Deadline) (c.Plan, error)
	Finish(c.ID, c.Deadline) error
}

type infraService struct {
	tx    ServiceTransactions
	nats  c.Driver
	caddy c.Driver
}

// ComponentTransactions registers two independently journaled components in
// one S8 host domain. The shared lease excludes concurrent component/image
// effects; separate keys preserve each component's own current and rollback.
type ComponentTransactions struct {
	nats  *TransactionAdapter
	caddy *TransactionAdapter
}

var _ ServiceTransactions = (*ComponentTransactions)(nil)
var _ CaddyJournal = (*ComponentTransactions)(nil)

// RegisterInfraDomain extends the actual S8 deployment declaration, including
// its existing agent/broker siblings. No alias key or separate infra flock can
// substitute for this registration. S8 versions that still reserve these keys
// return unsupported before any infrastructure effect.
// origin: hot-update S9 plan §8 / T44 / M22
func RegisterInfraDomain(reg upgradelock.Registration) (upgradetx.Key, upgradetx.Key, error) {
	var nats, caddy upgradetx.Key
	for _, participant := range reg.Participants {
		switch participant.Key.Component {
		case upgradetx.ComponentNATS:
			if nats != (upgradetx.Key{}) {
				return nats, caddy, c.ErrIncomplete
			}
			nats = participant.Key
		case upgradetx.ComponentCaddy:
			if caddy != (upgradetx.Key{}) {
				return nats, caddy, c.ErrIncomplete
			}
			caddy = participant.Key
		case upgradetx.ComponentAgent, upgradetx.ComponentBroker:
			// Existing image owners remain participants of the same host domain.
		default:
			return nats, caddy, c.ErrUnsupported
		}
	}
	if nats == (upgradetx.Key{}) || caddy == (upgradetx.Key{}) {
		return nats, caddy, c.ErrContractStub
	}
	domain, err := upgradelock.RegisterDomain(reg)
	if err != nil {
		return upgradetx.Key{}, upgradetx.Key{}, transactionError(err)
	}
	nats.DomainID, caddy.DomainID = domain, domain
	return nats, caddy, nil
}

// RegisterTransactions does not create an S8 registration or issue grants.
// Startup supplies adapters opened with real registered S8 keys; admission
// still goes through those Store/Acquire ports. No third public component is
// accepted, and mismatched host domains cannot be hidden behind this router.
// origin: hot-update S9 plan §8 / T44 / M22
func RegisterTransactions(nats, caddy *TransactionAdapter) (*ComponentTransactions, error) {
	if nats == nil || caddy == nil || nats.options.Key.Component != upgradetx.ComponentNATS || caddy.options.Key.Component != upgradetx.ComponentCaddy {
		return nil, c.ErrContractStub
	}
	if nats.options.Key.DomainID != caddy.options.Key.DomainID || nats.options.Key.UID != caddy.options.Key.UID ||
		nats.options.Key.ConfiguredNID != caddy.options.Key.ConfiguredNID {
		return nil, c.ErrIncomplete
	}
	return &ComponentTransactions{nats: nats, caddy: caddy}, nil
}

func (r *ComponentTransactions) forID(id c.ID, deadline c.Deadline) (*TransactionAdapter, error) {
	n, nErr := r.nats.Plan(id, deadline)
	k, cErr := r.caddy.Plan(id, deadline)
	if nErr == nil && cErr == nil { // One Tx cannot name two independently executable steps.
		return nil, c.ErrIncomplete
	}
	if nErr == nil && errors.Is(cErr, os.ErrNotExist) && n.Component == c.NATS {
		return r.nats, nil
	}
	if cErr == nil && errors.Is(nErr, os.ErrNotExist) && k.Component == c.Caddy {
		return r.caddy, nil
	}
	return nil, c.ErrIncomplete
}

func (r *ComponentTransactions) Acquire(p c.Plan, deadline c.Deadline) (c.Status, error) {
	switch p.Component {
	case c.NATS:
		return r.nats.Acquire(p, deadline)
	case c.Caddy:
		return r.caddy.Acquire(p, deadline)
	case c.HostSupport:
		return c.Status{}, c.ErrUnsupported
	default:
		return c.Status{}, c.ErrUnsupported
	}
}

func (r *ComponentTransactions) CompareAndSwap(s c.Status, phase c.Phase, proof c.Digest, deadline c.Deadline) (c.Status, error) {
	tx, err := r.forID(s.Tx, deadline)
	if err != nil {
		return c.Status{}, err
	}
	return tx.CompareAndSwap(s, phase, proof, deadline)
}

func (r *ComponentTransactions) Load(id c.ID, deadline c.Deadline) (c.Status, error) {
	tx, err := r.forID(id, deadline)
	if err != nil {
		return c.Status{}, err
	}
	return tx.Load(id, deadline)
}

func (r *ComponentTransactions) Plan(id c.ID, deadline c.Deadline) (c.Plan, error) {
	tx, err := r.forID(id, deadline)
	if err != nil {
		return c.Plan{}, err
	}
	return tx.Plan(id, deadline)
}

func (r *ComponentTransactions) ResumePlan(id c.ID, deadline c.Deadline) (c.Plan, error) {
	tx, err := r.forID(id, deadline)
	if err != nil {
		return c.Plan{}, err
	}
	return tx.ResumePlan(id, deadline)
}

func (r *ComponentTransactions) Finish(id c.ID, deadline c.Deadline) error {
	tx, err := r.forID(id, deadline)
	if err != nil {
		return err
	}
	return tx.Finish(id, deadline)
}

func (r *ComponentTransactions) LoadCaddy(p c.Plan, deadline c.Deadline) (CaddyTransition, error) {
	return r.caddy.LoadCaddy(p, deadline)
}

func (r *ComponentTransactions) StoreCaddy(p c.Plan, x CaddyTransition, deadline c.Deadline) error {
	return r.caddy.StoreCaddy(p, x, deadline)
}

// NewService registers both real component coordinators without doing I/O.
// Accepting an arbitrary compile driver here would advertise an unwired port.
// Physical capability is still checked at admission, by root and readiness;
// this constructor is not an InfraUpgradeVersion announcement.
// origin: hot-update S9 plan §2.1 / §6.4 / T44 / M22
func NewService(deps c.Dependencies, nats, caddy c.Driver) (c.Service, error) {
	for _, dep := range []any{deps.Host, deps.Artifacts, deps.Ready, deps.Cohort,
		deps.Routes, deps.Transfers, deps.Recovery, deps.Tx, deps.Observe, nats, caddy} {
		if natsMissing(dep) {
			return nil, c.ErrContractStub
		}
	}
	tx, ok := deps.Tx.(*ComponentTransactions)
	n, nOK := nats.(*natsDriver)
	k, cOK := caddy.(*caddyDriver)
	if !ok || !nOK || !cOK || !reflect.DeepEqual(n.deps, deps) || !reflect.DeepEqual(k.deps, deps) {
		return nil, c.ErrContractStub
	}
	return &infraService{tx: tx, nats: nats, caddy: caddy}, nil
}

func serviceBudget(deadline c.Deadline, stop c.Stop) error {
	select {
	case <-stop:
		return errors.New("infra_budget_exceeded")
	default:
	}
	if _, err := controlbudget.Remaining(deadline); err != nil {
		return errors.New("infra_budget_exceeded")
	}
	return nil
}

func (s *infraService) driver(p c.Plan) (c.Driver, error) {
	if c.ValidatePlan(p) != nil || p.Action == c.Recover {
		return nil, c.ErrUnsupported
	}
	switch p.Component {
	case c.NATS:
		return s.nats, nil
	case c.Caddy:
		return s.caddy, nil
	case c.HostSupport:
		return nil, c.ErrUnsupported
	default:
		return nil, c.ErrUnsupported
	}
}

func (s *infraService) Start(p c.Plan, deadline c.Deadline, stop c.Stop) (c.Status, error) {
	if err := serviceBudget(deadline, stop); err != nil {
		return c.Status{}, err
	}
	driver, err := s.driver(p)
	if err != nil {
		return c.Status{}, err
	}
	status, runErr := driver.Advance(p, deadline, stop)
	return s.result(p, status, deadline, runErr)
}

func (s *infraService) Resume(id c.ID, deadline c.Deadline, stop c.Stop) (c.Status, error) {
	if err := serviceBudget(deadline, stop); err != nil {
		return c.Status{}, err
	}
	p, err := s.tx.ResumePlan(id, deadline)
	if err != nil {
		return c.Status{}, err
	}
	driver, err := s.driver(p)
	if err != nil {
		return c.Status{}, err
	}
	status, runErr := driver.Recover(p, deadline, stop)
	return s.result(p, status, deadline, runErr)
}

func (s *infraService) Status(id c.ID, deadline c.Deadline) (c.Status, error) {
	if id == (c.ID{}) {
		return c.Status{}, c.ErrIncomplete
	}
	if err := serviceBudget(deadline, nil); err != nil {
		return c.Status{}, err
	}
	return s.tx.Load(id, deadline)
}

// The journal and an independent live observation, rather than the driver's
// return value, decide completion. A failed read cannot turn a lost reply into
// permission to start a second transaction. Preserve an already known error.
// origin: hot-update S9 plan §2.3 / T44
func (s *infraService) result(p c.Plan, local c.Status, deadline c.Deadline, runErr error) (c.Status, error) {
	if local.Tx == (c.ID{}) || local.Revision == 0 {
		return local, runErr
	}
	durable, err := s.tx.Load(p.Tx, deadline)
	if err != nil {
		// A historical/local completion cannot survive an unknown live read.
		// Keep its identity/revision for diagnosis but withdraw runtime success.
		if local.Phase == c.Completed {
			local.Phase, local.Code = c.Halted, "infra_result_unknown"
		}
		if runErr != nil {
			return local, runErr
		}
		return local, errors.New("infra_result_unknown")
	}
	if durable.Tx != p.Tx || durable.Step != p.Step || durable.Before != p.Before ||
		durable.Revision < local.Revision || (runErr == nil && durable.Phase != c.Completed) {
		return durable, errors.New("infra_recovery_required")
	}
	if durable.Phase == c.Completed || durable.Phase == c.Deferred {
		if err := s.tx.Finish(p.Tx, deadline); err != nil {
			return durable, err
		}
	}
	return durable, runErr
}
