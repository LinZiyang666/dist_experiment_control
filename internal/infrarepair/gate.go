// Package infrarepair owns the admission barrier between infrastructure recovery
// and the producers which could otherwise conceal a lost acknowledged write.
package infrarepair

import (
	"context"
	"errors"
	"sort"
	"sync"
	"time"

	"github.com/LinZiyang666/tether/internal/controlbudget"
	c "github.com/LinZiyang666/tether/internal/infracontract"
	"github.com/nats-io/nats.go/jetstream"
)

var (
	ErrFenced  = errors.New("infrarepair: producer admission closed")
	ErrUnknown = errors.New("infrarepair: request retirement unproven")
	ErrReceipt = errors.New("infrarepair: acknowledged receipt changed")
)

// Config binds one upgrade domain. Assets must use P11's complete inventory and
// SealDigests projection. Exclusive must check the supported deployment's actual
// writer policy; matching LIST responses cannot establish writer exclusivity.
// Accounts are management clients used only for raw reads by this package.
type Config struct {
	Domain    c.ID
	Required  []string
	Accounts  map[string]jetstream.JetStream
	Assets    func(c.Deadline, c.Stop) (c.AssetSeal, error)
	Exclusive func(c.Deadline, c.Stop) error
	Transfers func(c.Deadline) (c.TransferSeal, error)
}

// Producer captures retained owner obligations after admission is closed and
// all admitted operations have joined. Check compares local durable state (reader
// Sync prefix, publisher cursor, pending attempts), without repairing it. Retire
// must fence and join the producer's transport before resolving sent-unknown IO.
// These callbacks must obey the supplied absolute deadline; the gate never
// abandons an unjoined callback in a background goroutine.
type Producer struct {
	// AssetWriter distinguishes topology mutation from ordinary ACK/data progress.
	AssetWriter bool
	// Acknowledged stores original publish receipts in the producer-owned ledger.
	Acknowledged func(ACK) error
	Name         string
	Capture      func(c.Deadline) (Responsibilities, error)
	Check        func(Responsibilities, c.Deadline, c.Stop) error
	Retire       func(c.Deadline, c.Stop) error
}

type Gate struct {
	mu         sync.Mutex
	operation  chan struct{}
	cfg        Config
	producers  map[string]Producer
	revision   uint64
	active     int
	unknown    map[string]bool
	changed    chan struct{}
	closed     bool
	plan       c.Plan
	checkpoint c.Checkpoint
	receipts   map[string]Responsibilities
	verified   bool
	lost       bool
	prefixes   []originalPrefix
}

var _ c.Recovery = (*Gate)(nil)

func New(cfg Config) (*Gate, error) {
	if cfg.Domain == (c.ID{}) || len(cfg.Required) == 0 || len(cfg.Accounts) == 0 || cfg.Assets == nil || cfg.Exclusive == nil || cfg.Transfers == nil {
		return nil, c.ErrIncomplete
	}
	cfg.Required = append([]string(nil), cfg.Required...)
	sort.Strings(cfg.Required)
	for i, name := range cfg.Required {
		if name == "" || (i > 0 && name == cfg.Required[i-1]) {
			return nil, c.ErrIncomplete
		}
	}
	accounts := make(map[string]jetstream.JetStream, len(cfg.Accounts))
	for name, js := range cfg.Accounts {
		if name == "" || js == nil {
			return nil, c.ErrIncomplete
		}
		accounts[name] = js
	}
	cfg.Accounts = accounts
	g := &Gate{cfg: cfg, producers: make(map[string]Producer), revision: 1, unknown: make(map[string]bool), changed: make(chan struct{}), operation: make(chan struct{}, 1)}
	g.operation <- struct{}{}
	return g, nil
}

// Register is intentionally strict: a missing producer is not an empty producer,
// and replacing a live registration would lose the old owner's obligations.
// origin: hot-update S9 plan §3.2 / T41
func (g *Gate) Register(p Producer) error {
	g.mu.Lock()
	defer g.mu.Unlock()
	if g.closed {
		return ErrFenced
	}
	if p.Name == "" || p.Capture == nil || p.Check == nil || g.producers[p.Name].Capture != nil {
		return c.ErrIncomplete
	}
	switch p.Name {
	case StreamEnsure, StreamDelete, ObjectDelete, PlacementProbe, ReplicaReconcile:
		p.AssetWriter = true
	}
	g.producers[p.Name] = p
	g.revision++
	return nil
}

// Permit is a single admitted operation. Finish must run after all child work
// joins, even on cancellation. A network timeout retains an unknown obligation;
// it is not proof that the server did not apply the request.
type Permit struct {
	gate     *Gate
	name     string
	finished bool
}

func (g *Gate) Enter(name string) (*Permit, error) {
	g.mu.Lock()
	defer g.mu.Unlock()
	if g.closed {
		return nil, ErrFenced
	}
	if g.producers[name].Capture == nil {
		return nil, c.ErrIncomplete
	}
	g.active++
	if g.producers[name].AssetWriter {
		g.revision++ // Even create/delete/recreate of identical names invalidates a seal.
	}
	return &Permit{gate: g, name: name}, nil
}

func (p *Permit) Finish(err error) {
	g := p.gate
	g.mu.Lock()
	defer g.mu.Unlock()
	if p.finished {
		return
	}
	p.finished = true
	if unknownResult(err) {
		g.unknown[p.name] = true
	}
	g.active--
	close(g.changed)
	g.changed = make(chan struct{})
}

func unknownResult(err error) bool {
	if err == nil || errors.Is(err, ErrMetaGroupNotReady) {
		return false
	}
	var api *jetstream.APIError
	// An actual API rejection has retired the request. A local timeout, lost
	// response, or reconnect cancellation has no such server-side evidence.
	return !errors.As(err, &api)
}

// RetireUnknown does not reopen admission. An owner must actually retire its
// transport before the same sealed generation can be retried.
func (g *Gate) RetireUnknown(name string, deadline c.Deadline, stop c.Stop) error {
	if err := g.beginOperation(deadline, stop); err != nil {
		return err
	}
	defer g.endOperation()
	if err := admission(deadline, stop); err != nil {
		return err
	}
	g.mu.Lock()
	p := g.producers[name]
	ready := g.closed && g.active == 0 && g.unknown[name]
	g.mu.Unlock()
	if !ready || p.Retire == nil {
		return ErrUnknown
	}
	if err := p.Retire(deadline, stop); err != nil {
		return err
	}
	if err := admission(deadline, stop); err != nil {
		return err
	}
	g.mu.Lock()
	delete(g.unknown, name)
	g.mu.Unlock()
	return nil
}

// Seal closes product admission before capturing anything repairable. Failures
// retain that fence, including partial joins; a retry must name the same plan.
// origin: hot-update S9 plan §4.3 / T40 / T41
func (g *Gate) Seal(plan c.Plan, deadline c.Deadline) (c.Checkpoint, error) {
	if err := g.beginOperation(deadline, nil); err != nil {
		return c.Checkpoint{}, err
	}
	defer g.endOperation()
	if err := admission(deadline, nil); err != nil {
		return c.Checkpoint{}, err
	}
	g.mu.Lock()
	if plan.Tx == (c.ID{}) || plan.Domain != g.cfg.Domain || (g.closed && g.plan != plan) {
		g.mu.Unlock()
		return c.Checkpoint{}, c.ErrIncomplete
	}
	g.closed, g.plan = true, plan
	g.mu.Unlock()
	if err := g.join(deadline); err != nil {
		return c.Checkpoint{}, err
	}
	if g.checkpoint != (c.Checkpoint{}) {
		return g.checkpoint, nil
	}
	if err := g.cfg.Exclusive(deadline, nil); err != nil {
		return c.Checkpoint{}, err
	}
	assets, err := g.readAssets(deadline, nil)
	if err != nil {
		return c.Checkpoint{}, err
	}
	receipts := make(map[string]Responsibilities, len(g.producers))
	for name, producer := range g.producers {
		r, err := producer.Capture(deadline)
		if err != nil {
			return c.Checkpoint{}, err
		}
		if err := validateResponsibilities(r, g.cfg.Accounts); err != nil {
			return c.Checkpoint{}, err
		}
		receipts[name] = cloneResponsibilities(r)
	}
	transfers, err := g.cfg.Transfers(deadline)
	if err != nil {
		return c.Checkpoint{}, err
	}
	if !transfers.Complete || transfers.Version != 1 || transfers.SourceSHA256 == (c.Digest{}) || transfers.SchemaSHA256 == (c.Digest{}) || transfers.InvocationSet == (c.Digest{}) || transfers.ReceiptSet == (c.Digest{}) || transfers.ConsumerSet == (c.Digest{}) {
		return c.Checkpoint{}, c.ErrIncomplete
	}
	if err := admission(deadline, nil); err != nil {
		return c.Checkpoint{}, err
	}
	prefixes, err := g.captureOriginalPrefixes(receipts, deadline)
	if err != nil {
		return c.Checkpoint{}, err
	}
	g.prefixes, g.receipts = prefixes, receipts
	g.checkpoint = c.Checkpoint{Cohort: plan.Tx, Assets: assets, Transfers: transfers,
		Proof: digest(struct {
			Plan      c.Plan
			Assets    c.AssetSeal
			Transfers c.TransferSeal
			Receipts  map[string]Responsibilities
		}{plan, assets, transfers, receipts})}
	return g.checkpoint, nil
}

func (g *Gate) join(deadline c.Deadline) error {
	for {
		g.mu.Lock()
		active, changed := g.active, g.changed
		if active == 0 {
			defer g.mu.Unlock()
			if len(g.unknown) != 0 {
				return ErrUnknown
			}
			for _, name := range g.cfg.Required {
				if g.producers[name].Capture == nil {
					return c.ErrIncomplete
				}
			}
			return nil
		}
		g.mu.Unlock()
		remaining, err := controlbudget.Remaining(deadline)
		if err != nil {
			return err
		}
		timer := time.NewTimer(remaining)
		select {
		case <-changed:
			timer.Stop()
		case <-timer.C:
			return controlbudget.ErrExpired
		}
	}
}

// VerifyBeforeReplay reads every original receipt, with no ensure, consumer
// creation, replay or GC reachable. A confirmed loss stays red even if some
// external actor later recreates the bytes. Transient unavailability may retry.
// origin: hot-update S9 plan §4.3 / T40 / M20
func (g *Gate) VerifyBeforeReplay(cp c.Checkpoint, deadline c.Deadline, stop c.Stop) error {
	if err := g.beginOperation(deadline, stop); err != nil {
		return err
	}
	defer g.endOperation()
	if err := admission(deadline, stop); err != nil {
		return err
	}
	if !g.matches(cp) || g.lost {
		return ErrReceipt
	}
	g.verified = false
	if err := g.cfg.Exclusive(deadline, stop); err != nil {
		return err
	}
	assets, err := g.readAssets(deadline, stop)
	if err != nil {
		return err
	}
	if assets != cp.Assets {
		g.lost = true
		return ErrReceipt
	}
	if err := g.verifyOriginalPrefixes(deadline, stop); err != nil {
		if errors.Is(err, ErrReceipt) {
			g.lost = true
		}
		return err
	}
	for name, r := range g.receipts {
		if err := verifyResponsibilities(g.cfg.Accounts, r, deadline, stop); err != nil {
			if errors.Is(err, ErrReceipt) {
				g.lost = true
			}
			return err
		}
		if err := g.producers[name].Check(cloneResponsibilities(r), deadline, stop); err != nil {
			if errors.Is(err, ErrReceipt) {
				g.lost = true
			}
			return err
		}
	}
	// Repeat the identity read after raw messages: an out-of-policy external
	// recreation during verification must not inherit a successful earlier LIST.
	assets, err = g.readAssets(deadline, stop)
	if err != nil {
		return err
	}
	if assets != cp.Assets {
		g.lost = true
		return ErrReceipt
	}
	if err := admission(deadline, stop); err != nil {
		return err
	}
	g.verified = true
	return nil
}

func (g *Gate) matches(cp c.Checkpoint) bool {
	g.mu.Lock()
	defer g.mu.Unlock()
	return g.closed && cp.Proof != (c.Digest{}) && cp == g.checkpoint && cp.Assets.Revision == g.revision
}

// Release never doubles as a pre-cut abort. Even an unchanged old server must
// pass the read-only verification before repair admission can reopen.
// origin: hot-update S9 plan §4.3 / T40 / M20
func (g *Gate) Release(cp c.Checkpoint, deadline c.Deadline) error {
	if err := g.beginOperation(deadline, nil); err != nil {
		return err
	}
	defer g.endOperation()
	if err := admission(deadline, nil); err != nil {
		return err
	}
	if !g.matches(cp) || !g.verified || g.lost {
		return ErrFenced
	}
	if err := g.cfg.Exclusive(deadline, nil); err != nil {
		return err
	}
	assets, err := g.readAssets(deadline, nil)
	if err != nil {
		return err
	}
	if assets != cp.Assets {
		g.lost = true
		return ErrReceipt
	}
	g.mu.Lock()
	defer g.mu.Unlock()
	g.closed, g.verified = false, false
	g.checkpoint, g.plan, g.receipts = c.Checkpoint{}, c.Plan{}, nil
	g.prefixes = nil
	g.revision++
	return nil
}

func admission(deadline c.Deadline, stop c.Stop) error {
	select {
	case <-stop:
		return context.Canceled
	default:
	}
	_, err := controlbudget.Remaining(deadline)
	return err
}

// Contending callers consume their own deadline while queued. A mutex here
// would let a short-budget verify wait behind an unrelated long read forever.
// origin: hot-update S9 plan §3.2 / §4.3 / T41
func (g *Gate) beginOperation(deadline c.Deadline, stop c.Stop) error {
	remaining, err := controlbudget.Remaining(deadline)
	if err != nil {
		return err
	}
	timer := time.NewTimer(remaining)
	defer timer.Stop()
	select {
	case <-g.operation:
		return nil
	case <-stop:
		return context.Canceled
	case <-timer.C:
		return controlbudget.ErrExpired
	}
}
func (g *Gate) endOperation() { g.operation <- struct{}{} }
