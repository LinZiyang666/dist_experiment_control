// Package contractstub supplies inert compilation witnesses for S8 consumers.
// Production must never import it: no stub advertises capability, consumes a
// reader, takes an FD, invokes a callback or starts work. Every operation with
// an error result rejects, including cleanup, so missing wiring stays visible.
// origin: hot-update S8 plan §2.2 / §11.2 P00
package contractstub

import (
	"context"
	"io"

	"github.com/LinZiyang666/tether/internal/upgradeartifact"
	"github.com/LinZiyang666/tether/internal/upgradeauthority"
	"github.com/LinZiyang666/tether/internal/upgradecut"
	"github.com/LinZiyang666/tether/internal/upgradelock"
	"github.com/LinZiyang666/tether/internal/upgradeobserve"
	"github.com/LinZiyang666/tether/internal/upgradeport"
	"github.com/LinZiyang666/tether/internal/upgradetx"
)

// A fresh typed error prevents a consumer from mutating another call's refusal.
// The code is the plan's fail-closed classification; the detail identifies the
// package that must implement the operation, without claiming acceptance.
func notImplemented(owner string) error {
	return &upgradetx.Error{Code: upgradetx.CodeNotWired, Detail: "not implemented (S8 " + owner + ")"}
}

// P01 owns the pure transaction predicates in internal/upgradetx.
func ValidatePlan(upgradetx.Plan) error {
	return notImplemented("P01")
}
func Transition(upgradetx.Record, upgradetx.Event) (upgradetx.Record, error) {
	return upgradetx.Record{}, notImplemented("P01")
}
func Completed(upgradetx.Attempt, upgradetx.Observation) error {
	return notImplemented("P01")
}

// Store witnesses P02's durable owner without creating a journal or grant.
type Store struct{}

var _ upgradetx.Store = Store{}

func (Store) Prepare(upgradetx.Key, upgradetx.PrepareRequest) (upgradetx.Grant, error) {
	return upgradetx.Grant{}, notImplemented("P02")
}
func (Store) Start(upgradetx.Grant, upgradetx.Fence) (upgradetx.Record, error) {
	return upgradetx.Record{}, notImplemented("P02")
}
func (Store) Load(upgradetx.Key, string) (upgradetx.View, error) {
	return upgradetx.View{}, notImplemented("P02")
}
func (Store) Apply(upgradetx.Handle, upgradetx.Event) (upgradetx.Record, error) {
	return upgradetx.Record{}, notImplemented("P02")
}
func (Store) PrepareReverse(upgradetx.Fence, upgradetx.ReverseRequest) (upgradetx.Grant, error) {
	return upgradetx.Grant{}, notImplemented("P02")
}
func (Store) Archive(upgradetx.Handle, upgradetx.DeathProof) (upgradetx.View, error) {
	return upgradetx.View{}, notImplemented("P02")
}
func (Store) CloseWindow(upgradetx.Handle) (upgradetx.Metadata, error) {
	return upgradetx.Metadata{}, notImplemented("P02")
}

// Registry witnesses P03 without signing an empty or unobserved census.
type Registry struct{}

var _ upgradeobserve.Registry = Registry{}

func (Registry) Begin(upgradetx.Resource) (uint64, error) {
	return 0, notImplemented("P03")
}
func (Registry) Finish(uint64, upgradetx.Resource) error {
	return notImplemented("P03")
}
func (Registry) Snapshot(upgradetx.Stamp) (upgradetx.Census, error) {
	return upgradetx.Census{}, notImplemented("P03")
}
func (Registry) Seal(upgradetx.Attempt, string) (upgradetx.Receipt, error) {
	return upgradetx.Receipt{}, notImplemented("P03")
}

// Lease is a method-set witness only. Its getters cannot return errors in the
// frozen API, so they expose no identity and an invalid FD, never stdin's FD 0.
// Acquire and Adopt always return nil rather than fabricating ownership.
type Lease struct{}

var _ upgradelock.Lease = Lease{}

func (Lease) FD() uintptr      { return ^uintptr(0) }
func (Lease) DomainID() string { return "" }
func (Lease) Close() error     { return notImplemented("P04") }

func Acquire(context.Context, upgradetx.Key, string) (upgradelock.Lease, error) {
	return nil, notImplemented("P04")
}
func Adopt(uintptr, upgradetx.Key, string) (upgradelock.Lease, error) {
	return nil, notImplemented("P04")
}
func WithGuard(upgradetx.Key, func() error) error {
	return notImplemented("P04")
}

// P05 owns the real immutable publication and component selector functions.
func Publish(context.Context, upgradetx.Grant, io.Reader, io.Reader) (upgradeartifact.Receipt, error) {
	return upgradeartifact.Receipt{}, notImplemented("P05")
}
func Verify(upgradeartifact.Receipt, upgradetx.Grant) error {
	return notImplemented("P05")
}
func Resolve(upgradetx.Key, upgradetx.View) (string, error) {
	return "", notImplemented("P05")
}

// P07 owns the strict adjunct codec and the Take-before-cleanup boundary.
func Encode(upgradecut.State) ([]byte, error) {
	return nil, notImplemented("P07")
}
func Decode([]byte) (upgradecut.State, error) {
	return upgradecut.State{}, notImplemented("P07")
}
func Take(upgradecut.State, func(uint32) (uintptr, error)) (upgradelock.Lease, error) {
	return nil, notImplemented("P07")
}

// P08 owns derivation and the SQL creation predicate. Stub errors never include
// caller input, particularly the salt or encoded PIN hash.
func Derive(string, string, int64, []byte) (string, error) {
	return "", notImplemented("P08")
}
func Match(string, string) error {
	return notImplemented("P08")
}
func ReadCreation(upgradeauthority.RowReader, upgradeauthority.Guard) (upgradeauthority.Creation, error) {
	return upgradeauthority.Creation{}, notImplemented("P08")
}
func CreationPredicate(upgradeauthority.Creation) (string, []any, error) {
	return "", nil, notImplemented("P08")
}

// The frozen New signature has no error return. Construction therefore returns
// a concrete endpoint whose every operation refuses with its real owner, not
// nil and not a success response. The two owner types also work at zero value.
type AgentEndpoint struct{}
type BrokerEndpoint struct{}

var (
	_ upgradeport.Endpoint = AgentEndpoint{}
	_ upgradeport.Endpoint = BrokerEndpoint{}
)

func NewAgent(upgradetx.Store, upgradeport.RuntimePort, upgradeobserve.Registry) upgradeport.Endpoint {
	return AgentEndpoint{}
}
func NewBroker(upgradetx.Store, upgradeport.RuntimePort, upgradeobserve.Registry) upgradeport.Endpoint {
	return BrokerEndpoint{}
}

func (AgentEndpoint) Prepare(context.Context, upgradetx.Key, upgradetx.PrepareRequest) (upgradetx.Grant, error) {
	return upgradetx.Grant{}, notImplemented("P09")
}
func (AgentEndpoint) Start(context.Context, upgradetx.Grant, upgradeartifact.Receipt) (upgradetx.View, error) {
	return upgradetx.View{}, notImplemented("P09")
}
func (AgentEndpoint) Query(context.Context, upgradetx.Key, string, string) (upgradetx.View, error) {
	return upgradetx.View{}, notImplemented("P09")
}
func (AgentEndpoint) Rollback(context.Context, upgradetx.Fence, upgradetx.ReverseRequest) (upgradetx.View, error) {
	return upgradetx.View{}, notImplemented("P09")
}
func (AgentEndpoint) Cancel(context.Context, upgradetx.Handle) (upgradetx.View, error) {
	return upgradetx.View{}, notImplemented("P09")
}
func (AgentEndpoint) Archive(context.Context, upgradetx.Handle) (upgradetx.View, error) {
	return upgradetx.View{}, notImplemented("P09")
}
func (AgentEndpoint) CloseWindow(context.Context, upgradetx.Handle) (upgradetx.View, error) {
	return upgradetx.View{}, notImplemented("P09")
}

func (BrokerEndpoint) Prepare(context.Context, upgradetx.Key, upgradetx.PrepareRequest) (upgradetx.Grant, error) {
	return upgradetx.Grant{}, notImplemented("P10")
}
func (BrokerEndpoint) Start(context.Context, upgradetx.Grant, upgradeartifact.Receipt) (upgradetx.View, error) {
	return upgradetx.View{}, notImplemented("P10")
}
func (BrokerEndpoint) Query(context.Context, upgradetx.Key, string, string) (upgradetx.View, error) {
	return upgradetx.View{}, notImplemented("P10")
}
func (BrokerEndpoint) Rollback(context.Context, upgradetx.Fence, upgradetx.ReverseRequest) (upgradetx.View, error) {
	return upgradetx.View{}, notImplemented("P10")
}
func (BrokerEndpoint) Cancel(context.Context, upgradetx.Handle) (upgradetx.View, error) {
	return upgradetx.View{}, notImplemented("P10")
}
func (BrokerEndpoint) Archive(context.Context, upgradetx.Handle) (upgradetx.View, error) {
	return upgradetx.View{}, notImplemented("P10")
}
func (BrokerEndpoint) CloseWindow(context.Context, upgradetx.Handle) (upgradetx.View, error) {
	return upgradetx.View{}, notImplemented("P10")
}

func Run(context.Context, upgradetx.Store, upgradeport.Endpoint, upgradetx.Plan) (upgradetx.View, error) {
	return upgradetx.View{}, notImplemented("P11")
}

// Runtime witnesses P17's S8-side integration adapter. It deliberately imports
// no S7 runtime, Bundle or private codec while those owners work in parallel.
type Runtime struct{}

var _ upgradeport.RuntimePort = Runtime{}

func (Runtime) Preflight(context.Context, upgradetx.Grant, upgradeartifact.Receipt) (upgradeport.PreflightProof, error) {
	return upgradeport.PreflightProof{}, notImplemented("P17")
}
func (Runtime) Freeze(upgradetx.Handle) (upgradeport.Cut, error) {
	return upgradeport.Cut{}, notImplemented("P17")
}
func (Runtime) ExportCurrent(upgradetx.Handle) (upgradeport.Cut, error) {
	return upgradeport.Cut{}, notImplemented("P17")
}
func (Runtime) Activate(upgradeport.Cut) (upgradeport.Activation, error) {
	return upgradeport.Activation{}, notImplemented("P17")
}
func (Runtime) Observe(upgradetx.Stamp) (upgradetx.Observation, error) {
	return upgradetx.Observation{}, notImplemented("P17")
}
func (Runtime) Retire(upgradetx.Handle) (upgradetx.Receipt, error) {
	return upgradetx.Receipt{}, notImplemented("P17")
}
func (Runtime) Thaw(upgradetx.Handle) error {
	return notImplemented("P17")
}
