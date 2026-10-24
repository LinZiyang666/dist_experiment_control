// Package txcontracts compiles the S8 contract composition independently of
// unfinished production engines. It proves exact linkage only, never successful
// IO, durable acceptance, image replacement or an advertised capability.
// origin: hot-update S8 plan §2.2 / §11.1 P00
package txcontracts

import (
	"context"
	"database/sql"
	"io"

	"github.com/LinZiyang666/tether/internal/upgradeartifact"
	"github.com/LinZiyang666/tether/internal/upgradeauthority"
	"github.com/LinZiyang666/tether/internal/upgradecut"
	"github.com/LinZiyang666/tether/internal/upgradelock"
	"github.com/LinZiyang666/tether/internal/upgradeobserve"
	"github.com/LinZiyang666/tether/internal/upgradeport"
	"github.com/LinZiyang666/tether/internal/upgradetx"
	s "github.com/LinZiyang666/tether/internal/upgradetx/contractstub"
)

// FixtureSchemaVersion is independent of the v2 product journal. A fixture is
// evidence only when its recorded argv and executable identities were actually
// observed; naming an old release does not prove an old binary was executed.
const FixtureSchemaVersion = 1

// WireFixture is the frozen P22/P24 eight-quadrant provenance row. Kill uses the
// explicit wire probe because the product has no top-level kill command. Empty
// probe fields on other verbs remain present rather than hiding the schema.
// origin: hot-update S8 plan §7.3 / P00
type WireFixture struct {
	Cell           string   `json:"cell"`
	Verb           string   `json:"verb"`
	Argv           []string `json:"argv"`
	CtlPath        string   `json:"ctl_path"`
	CtlSHA         string   `json:"ctl_sha"`
	AgentPath      string   `json:"agent_path"`
	AgentSHA       string   `json:"agent_sha"`
	BrokerPath     string   `json:"broker_path"`
	BrokerSHA      string   `json:"broker_sha"`
	ProbePath      string   `json:"probe_path"`
	ProbeSHA       string   `json:"probe_sha"`
	SourceCommit   string   `json:"source_commit"`
	ExpectedEffect string   `json:"expected_effect"`
}

// Each function is checked against both the owner alias and an independent
// literal signature. Changing the stub and alias together must still be red.
// These blank assignments are compile assertions, not mutable function slots.
var (
	_ upgradetx.ValidatePlanFunc                                                                           = s.ValidatePlan
	_ func(upgradetx.Plan) error                                                                           = s.ValidatePlan
	_ upgradetx.TransitionFunc                                                                             = s.Transition
	_ func(upgradetx.Record, upgradetx.Event) (upgradetx.Record, error)                                    = s.Transition
	_ upgradetx.CompletedFunc                                                                              = s.Completed
	_ func(upgradetx.Attempt, upgradetx.Observation) error                                                 = s.Completed
	_ upgradelock.AcquireFunc                                                                              = s.Acquire
	_ func(context.Context, upgradetx.Key, string) (upgradelock.Lease, error)                              = s.Acquire
	_ upgradelock.AdoptFunc                                                                                = s.Adopt
	_ func(uintptr, upgradetx.Key, string) (upgradelock.Lease, error)                                      = s.Adopt
	_ upgradelock.WithGuardFunc                                                                            = s.WithGuard
	_ func(upgradetx.Key, func() error) error                                                              = s.WithGuard
	_ upgradeartifact.PublishFunc                                                                          = s.Publish
	_ func(context.Context, upgradetx.Grant, io.Reader, io.Reader) (upgradeartifact.Receipt, error)        = s.Publish
	_ upgradeartifact.VerifyFunc                                                                           = s.Verify
	_ func(upgradeartifact.Receipt, upgradetx.Grant) error                                                 = s.Verify
	_ upgradeartifact.ResolveFunc                                                                          = s.Resolve
	_ func(upgradetx.Key, upgradetx.View) (string, error)                                                  = s.Resolve
	_ upgradeauthority.DeriveFunc                                                                          = s.Derive
	_ func(string, string, int64, []byte) (string, error)                                                  = s.Derive
	_ upgradeauthority.MatchFunc                                                                           = s.Match
	_ func(string, string) error                                                                           = s.Match
	_ upgradeauthority.ReadCreationFunc                                                                    = s.ReadCreation
	_ func(upgradeauthority.RowReader, upgradeauthority.Guard) (upgradeauthority.Creation, error)          = s.ReadCreation
	_ upgradeauthority.CreationPredicateFunc                                                               = s.CreationPredicate
	_ func(upgradeauthority.Creation) (string, []any, error)                                               = s.CreationPredicate
	_ upgradecut.EncodeFunc                                                                                = s.Encode
	_ func(upgradecut.State) ([]byte, error)                                                               = s.Encode
	_ upgradecut.DecodeFunc                                                                                = s.Decode
	_ func([]byte) (upgradecut.State, error)                                                               = s.Decode
	_ upgradecut.TakeFunc                                                                                  = s.Take
	_ func(upgradecut.State, func(uint32) (uintptr, error)) (upgradelock.Lease, error)                     = s.Take
	_ upgradeport.NewFunc                                                                                  = s.NewAgent
	_ func(upgradetx.Store, upgradeport.RuntimePort, upgradeobserve.Registry) upgradeport.Endpoint         = s.NewAgent
	_ upgradeport.NewFunc                                                                                  = s.NewBroker
	_ func(upgradetx.Store, upgradeport.RuntimePort, upgradeobserve.Registry) upgradeport.Endpoint         = s.NewBroker
	_ upgradeport.RunFunc                                                                                  = s.Run
	_ func(context.Context, upgradetx.Store, upgradeport.Endpoint, upgradetx.Plan) (upgradetx.View, error) = s.Run
)

// Both directions pin exact method sets. A removed method is as much a contract
// change as an added one, even when the concrete stub would still implement it.
type store interface {
	Prepare(upgradetx.Key, upgradetx.PrepareRequest) (upgradetx.Grant, error)
	Start(upgradetx.Grant, upgradetx.Fence) (upgradetx.Record, error)
	Load(upgradetx.Key, string) (upgradetx.View, error)
	Apply(upgradetx.Handle, upgradetx.Event) (upgradetx.Record, error)
	PrepareReverse(upgradetx.Fence, upgradetx.ReverseRequest) (upgradetx.Grant, error)
	Archive(upgradetx.Handle, upgradetx.DeathProof) (upgradetx.View, error)
	CloseWindow(upgradetx.Handle) (upgradetx.Metadata, error)
}
type registry interface {
	Begin(upgradetx.Resource) (uint64, error)
	Finish(uint64, upgradetx.Resource) error
	Snapshot(upgradetx.Stamp) (upgradetx.Census, error)
	Seal(upgradetx.Attempt, string) (upgradetx.Receipt, error)
}
type lease interface {
	FD() uintptr
	DomainID() string
	Close() error
}
type rowReader interface {
	QueryRow(string, ...any) *sql.Row
}
type endpoint interface {
	Prepare(context.Context, upgradetx.Key, upgradetx.PrepareRequest) (upgradetx.Grant, error)
	Start(context.Context, upgradetx.Grant, upgradeartifact.Receipt) (upgradetx.View, error)
	Query(context.Context, upgradetx.Key, string, string) (upgradetx.View, error)
	Rollback(context.Context, upgradetx.Fence, upgradetx.ReverseRequest) (upgradetx.View, error)
	Cancel(context.Context, upgradetx.Handle) (upgradetx.View, error)
	Archive(context.Context, upgradetx.Handle) (upgradetx.View, error)
	CloseWindow(context.Context, upgradetx.Handle) (upgradetx.View, error)
}
type runtimePort interface {
	Preflight(context.Context, upgradetx.Grant, upgradeartifact.Receipt) (upgradeport.PreflightProof, error)
	Freeze(upgradetx.Handle) (upgradeport.Cut, error)
	ExportCurrent(upgradetx.Handle) (upgradeport.Cut, error)
	Activate(upgradeport.Cut) (upgradeport.Activation, error)
	Observe(upgradetx.Stamp) (upgradetx.Observation, error)
	Retire(upgradetx.Handle) (upgradetx.Receipt, error)
	Thaw(upgradetx.Handle) error
}

var (
	_ store                      = upgradetx.Store(nil)
	_ upgradetx.Store            = store(nil)
	_ registry                   = upgradeobserve.Registry(nil)
	_ upgradeobserve.Registry    = registry(nil)
	_ lease                      = upgradelock.Lease(nil)
	_ upgradelock.Lease          = lease(nil)
	_ rowReader                  = upgradeauthority.RowReader(nil)
	_ upgradeauthority.RowReader = rowReader(nil)
	_ endpoint                   = upgradeport.Endpoint(nil)
	_ upgradeport.Endpoint       = endpoint(nil)
	_ runtimePort                = upgradeport.RuntimePort(nil)
	_ upgradeport.RuntimePort    = runtimePort(nil)
)
