// Package upgradeport is the S8 control-plane adaptation boundary.
// It must not import agent, broker or cmd. Runtime adapters belong to integration
// owners so an unfinished S7 proposal cannot become a dependency by accident.
// origin: hot-update S8 plan §2.2 / §11 / P00
package upgradeport

import (
	"context"
	"os"

	"github.com/LinZiyang666/tether/internal/upgradeartifact"
	"github.com/LinZiyang666/tether/internal/upgradeobserve"
	"github.com/LinZiyang666/tether/internal/upgradetx"
)

// Query's last argument is the caller's fresh observation nonce. CloseWindow
// delegates to the unique Store owner, not an arbitrary completion callback.
type Endpoint interface {
	Prepare(context.Context, upgradetx.Key, upgradetx.PrepareRequest) (upgradetx.Grant, error)
	Start(context.Context, upgradetx.Grant, upgradeartifact.Receipt) (upgradetx.View, error)
	Query(context.Context, upgradetx.Key, string, string) (upgradetx.View, error)
	Rollback(context.Context, upgradetx.Fence, upgradetx.ReverseRequest) (upgradetx.View, error)
	Cancel(context.Context, upgradetx.Handle) (upgradetx.View, error)
	Archive(context.Context, upgradetx.Handle) (upgradetx.View, error)
	CloseWindow(context.Context, upgradetx.Handle) (upgradetx.View, error)
}

type PreflightProof struct {
	Grant         upgradetx.Grant  `json:"grant"`
	ReadersDigest string           `json:"readers_digest"`
	ConfigDigest  string           `json:"config_digest"`
	Census        upgradetx.Census `json:"census"`
}

// File is an owned descriptor passed in process, never serialized as evidence.
// Index binds the validated adjunct to the exact auxiliary custody entry.
type FD struct {
	Index uint32          `json:"index"`
	File  *os.File        `json:"-"`
	Kind  string          `json:"kind"`
	Owner upgradetx.Owner `json:"owner"`
}

type Cut struct {
	Attempt    upgradetx.Handle   `json:"attempt"`
	Owner      upgradetx.Owner    `json:"owner"`
	Stage      upgradetx.CutStage `json:"stage"`
	Descriptor []byte             `json:"descriptor"`
	Auxiliary  []FD               `json:"auxiliary"`
	Scope      upgradetx.Scope    `json:"scope"`
	Sequence   uint64             `json:"sequence"`
}

type Activation struct {
	Attempt    upgradetx.Handle   `json:"attempt"`
	Owner      upgradetx.Owner    `json:"owner"`
	Stage      upgradetx.CutStage `json:"stage"`
	RunningSHA string             `json:"running_sha"`
}

// Only control-plane preflight receives a caller context. ExportCurrent after
// partial activation must preserve current work and ACKs; it cannot replay the
// pristine input. Retirement proves all retained owners, not just a dead PID.
type RuntimePort interface {
	Preflight(context.Context, upgradetx.Grant, upgradeartifact.Receipt) (PreflightProof, error)
	Freeze(upgradetx.Handle) (Cut, error)
	ExportCurrent(upgradetx.Handle) (Cut, error)
	Activate(Cut) (Activation, error)
	Observe(upgradetx.Stamp) (upgradetx.Observation, error)
	Retire(upgradetx.Handle) (upgradetx.Receipt, error)
	Thaw(upgradetx.Handle) error
}

// P09 and P10 each implement New with this exact shape in their own package.
// P11 implements Run; these aliases create no replaceable global function table.
type NewFunc = func(store upgradetx.Store, runtime RuntimePort, registry upgradeobserve.Registry) Endpoint
type RunFunc = func(ctx context.Context, store upgradetx.Store, endpoint Endpoint, plan upgradetx.Plan) (upgradetx.View, error)
