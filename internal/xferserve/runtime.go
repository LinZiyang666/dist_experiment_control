package xferserve

import (
	"errors"
	"time"

	"github.com/LinZiyang666/tether/internal/drillobserve"
	"github.com/LinZiyang666/tether/internal/xferauth"
	x "github.com/LinZiyang666/tether/internal/xfercontract"
	"github.com/LinZiyang666/tether/internal/xfercontrol"
	"github.com/LinZiyang666/tether/internal/xferfinal"
	"github.com/LinZiyang666/tether/internal/xfergc"
	"github.com/LinZiyang666/tether/internal/xferhistory"
	"github.com/LinZiyang666/tether/internal/xferledger"
	"github.com/LinZiyang666/tether/internal/xfertrack"
	"github.com/nats-io/nats.go/jetstream"
)

// Preserve the caller's deadline at the outer scheduling seam.
// origin: hot-update S7 plan §1.2 / §5.3 / LQ3
// origin: S7 internal review GATES-PROD F1
func PassDeadline(until time.Time, stop <-chan struct{}) (x.Deadline, error) {
	return xfercontrol.PassDeadline(until, stop)
}

// AssembleRuntime preserves the imported tracker closure while constructing
// fresh backend borrowers. xfercontrol.Assemble is the cold-only constructor;
// importing an empty tracker here would erase Reaped and ConfigAttempts.
// origin: hot-update S7 plan §8.2 / T36
//
// AssembleRuntime returns a sleeping tracker. Its first Bind reconciles the actual
// primary/outbox census before admission, subscriptions or destructive passes.
// On partial construction the returned Runtime still owns every acquired leaf.
func AssembleRuntime(cfg xfercontrol.AssemblyConfig, imported *x.TrackerState) (*xfercontrol.Runtime, error) {
	r := &xfercontrol.Runtime{Buckets: cfg.Buckets}
	// Bind custody before constructors can emit. All current backend borrowers
	// share this image generation while the permit keeps its original origin.
	// origin: S7 internal review INTEG-O F5
	generationForCustody := cfg.Origin.Generation
	if imported != nil {
		generationForCustody = imported.Generation + 1
	}
	if drillobserve.Hook != nil {
		sink := drillobserve.ProcessTransferObserver[x.Observation](generationForCustody, "broker")
		cfg.Observe = func(event x.Observation) { event.Generation = 0; sink(event) }
	}
	if cfg.JS == nil || cfg.Authority == nil || cfg.Permits == nil || cfg.Transport == nil || cfg.Buckets == nil || cfg.Receiver == nil || cfg.Objects == nil || cfg.Owners == nil || cfg.SessionOwners == nil {
		return r, errors.New(x.CodeOwnerRecovering)
	}
	var err error
	r.Ledger, err = xferledger.New(x.LedgerConfig{Base: cfg.Base, Root: cfg.Root, FallbackRoot: cfg.FallbackRoot})
	if err != nil {
		return r, err
	}
	history := sessionHistory{base: cfg.Base, js: cfg.JS}
	final := &finalizerBinding{}
	state := x.TrackerState{Schema: x.StateSchema, Origin: cfg.Origin, Generation: cfg.Origin.Generation,
		Admission: x.AdmissionState{Closed: true}, ProvisioningFence: x.AdmissionState{Closed: true}}
	if imported != nil {
		state = *imported
	}
	r.Tracker, err = xfertrack.Import(x.TrackerConfig{Base: cfg.Base, Origin: cfg.Origin, Ledger: r.Ledger, History: history, ResolveReceiver: cfg.Receiver, Finalizer: final}, state)
	if err != nil {
		return r, err
	}
	generation := cfg.Origin.Generation
	if imported != nil {
		generation = imported.Generation + 1
	}
	decisions, err := xfertrack.DecisionsForBinding(r.Tracker, generation)
	if err != nil {
		return r, err
	}
	r.Finalizer, err = xferfinal.New(x.FinalConfig{Base: cfg.Base, LegacyOrphans: &x.LegacyOrphanConfig{JS: cfg.JS, Owners: cfg.Owners}, Decisions: decisions, Ledger: r.Ledger, History: history, Objects: cfg.Objects, ResolveReceiver: cfg.Receiver})
	if err != nil {
		return r, err
	}
	final.port = r.Finalizer
	r.Collector, err = xfergc.New(x.GCConfig{Base: cfg.Base, JS: cfg.JS, Owners: cfg.Owners, Finalizer: r.Finalizer})
	if err != nil {
		return r, err
	}
	r.Controller, err = xfercontrol.New(xfercontrol.Config{Base: cfg.Base, Tracker: r.Tracker, Authority: cfg.Authority, Permits: cfg.Permits, Buckets: cfg.Buckets, Transport: cfg.Transport})
	if err != nil {
		return r, err
	}
	r.Settlement, err = xferauth.NewSettlement(xferauth.SettlementConfig{Owners: cfg.SessionOwners, Tracker: r.Tracker, Ledger: r.Ledger, Finalizer: r.Finalizer, UTC: cfg.UTC})
	return r, err
}

// This binding closes the tracker/finalizer cycle before the sleeping tracker
// can run. A missing target is an error, never a successful no-op finalizer.
type finalizerBinding struct{ port x.FinalizerPort }

func (f *finalizerBinding) Advance(id x.Identity, d x.Deadline) (x.Status, error) {
	if f.port == nil {
		return x.Status{}, errors.New(x.CodeOwnerRecovering)
	}
	return f.port.Advance(id, d)
}
func (f *finalizerBinding) Stop(d x.Deadline) error {
	if f.port == nil {
		return errors.New("transfer finalizer not installed")
	}
	return f.port.Stop(d)
}

type sessionHistory struct {
	base x.Base
	js   jetstream.JetStream
}

func (h sessionHistory) Confirm(r x.CanonicalRecord, d x.Deadline) (x.HistoryReceipt, error) {
	p, err := xferhistory.New(x.HistoryConfig{Base: h.base, JS: h.js, SID: r.Identity.SID})
	if err != nil {
		return x.HistoryReceipt{}, err
	}
	return p.Confirm(r, d)
}
func (h sessionHistory) Lookup(k x.Key, d x.Deadline) (x.HistorySet, error) {
	p, err := xferhistory.New(x.HistoryConfig{Base: h.base, JS: h.js, SID: k.SID})
	if err != nil {
		return x.HistorySet{}, err
	}
	return p.Lookup(k, d)
}
