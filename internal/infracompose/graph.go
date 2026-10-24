// Package infracompose assembles the nonroot infrastructure owners. Physical
// observations and normal authority stay with their existing runtime owners;
// construction cannot manufacture their evidence or advertise a capability.
// origin: hot-update S9 plan §2.1 / §8 / P22
package infracompose

import (
	"encoding/hex"
	"path/filepath"
	"reflect"
	"runtime"

	"github.com/LinZiyang666/tether/internal/infraartifact"
	c "github.com/LinZiyang666/tether/internal/infracontract"
	"github.com/LinZiyang666/tether/internal/inframaint"
	"github.com/LinZiyang666/tether/internal/infrapriv"
	"github.com/LinZiyang666/tether/internal/infrareadiness"
	"github.com/LinZiyang666/tether/internal/infratransfer"
	"github.com/LinZiyang666/tether/internal/infraupgrade"
	"github.com/LinZiyang666/tether/internal/ingressmove"
	"github.com/LinZiyang666/tether/internal/upgradelock"
	"github.com/LinZiyang666/tether/internal/upgradestore"
	"github.com/LinZiyang666/tether/internal/upgradetx"
)

// Recovery is the broker's own producer gate, including its short asset fence.
// An unrelated gate cannot certify the producers sharing its business NC.
type Recovery interface {
	c.Recovery
	infrareadiness.SealSource
}

type CaddyReadiness interface {
	c.ReadyProbe
	infraupgrade.CaddyReloader
}

// Transaction selects private journal locations and the existing controller
// identity/scope. New replaces Key, Store and Journal with registered owners.
// Evidence must read the root journal and actual resources, never Status.
type Transaction struct {
	Directory string
	Options   infraupgrade.TransactionOptions
}

// Config contains trusted startup inputs, not an IPC or image representation.
// Bind wires the resulting registry into every dial owner, including
// independent transfer NCs, before a Service can be published. It also binds
// the gate's transfer-seal source to this graph's retained S7 adapter, resolving
// that construction cycle without opening another producer gate. Missing physical
// or normal-authority sources are setup errors, never successful empty owners.
type Config struct {
	Transport    infrapriv.HostTransport
	Authority    infrapriv.HostAuthority
	Artifacts    infraartifact.Config
	Topology     infrareadiness.TopologySource
	System       infrareadiness.Requester
	Accounts     []infrareadiness.AccountAccess
	Auth         *infrareadiness.OrdinaryAuth
	CaddyReady   CaddyReadiness
	Witness      ingressmove.Witness
	VerifyProof  ingressmove.VerifyProof
	Bind         func(*Graph) error
	Routes       inframaint.Config
	Transfers    infratransfer.Config
	Registration upgradelock.Registration
	NATS         Transaction
	Caddy        Transaction
	Observe      c.Observer
}

type Graph struct {
	Dependencies c.Dependencies
	Registry     *ingressmove.Registry
	Transactions *infraupgrade.ComponentTransactions
}

// Snapshot lets the normal authority source read the actual sealed NC set
// through the aggregate handle. Passing that aggregate directly to Registry
// would fail its exact raw-checkpoint comparison, or tempt a caller to weaken it.
func (g *Graph) Snapshot(cp c.Checkpoint, d c.Deadline) ([]c.Attachment, error) {
	if g == nil || g.Registry == nil {
		return nil, c.ErrIncomplete
	}
	cohort, ok := g.Dependencies.Cohort.(*checkpointCohort)
	if !ok {
		return nil, c.ErrIncomplete
	}
	raw, err := cohort.original(cp, d)
	if err != nil {
		return nil, err
	}
	return g.Registry.Snapshot(raw, d)
}

func absent(v any) bool {
	if v == nil {
		return true
	}
	r := reflect.ValueOf(v)
	switch r.Kind() {
	case reflect.Chan, reflect.Func, reflect.Interface, reflect.Map, reflect.Pointer, reflect.Slice:
		return r.IsNil()
	default:
		return false
	}
}

// New builds the final leaf graph without substituting failure adapters. The
// caller owns supplied transports, credentials, NCs, retained S7 owners and the
// producer gate. No worker, unit or business invocation is started here.
// origin: hot-update S9 plan §2.1 / §8 / P22 / P40
func New(cfg Config, recovery Recovery) (*Graph, error) {
	if runtime.GOOS != "linux" || runtime.GOARCH != "amd64" {
		return nil, c.ErrUnsupported
	}
	if absent(recovery) || absent(cfg.Transport) || absent(cfg.Artifacts.Source) ||
		absent(cfg.Artifacts.Supervisor) || absent(cfg.Artifacts.Compatibility) || absent(cfg.Topology) ||
		absent(cfg.System) || absent(cfg.CaddyReady) || absent(cfg.Witness) || cfg.VerifyProof == nil || cfg.Bind == nil ||
		absent(cfg.Routes.Source) || absent(cfg.Observe) || cfg.Auth == nil || cfg.Auth.Dial == nil ||
		cfg.Auth.Receipt == nil || cfg.Auth.RPC == nil || cfg.Auth.WSS == nil ||
		cfg.Routes.Domain != cfg.Authority.Domain || cfg.NATS.Directory == cfg.Caddy.Directory ||
		!filepath.IsAbs(cfg.NATS.Directory) || !filepath.IsAbs(cfg.Caddy.Directory) ||
		cfg.NATS.Options.Context == nil || cfg.Caddy.Options.Context == nil ||
		absent(cfg.NATS.Options.Evidence) || absent(cfg.Caddy.Options.Evidence) {
		return nil, c.ErrIncomplete
	}
	for _, account := range cfg.Accounts {
		if absent(account.Query) {
			return nil, c.ErrIncomplete
		}
	}
	host, err := infrapriv.NewManagementHost(cfg.Transport, cfg.Authority)
	if err != nil {
		return nil, err
	}
	artifacts, err := infraartifact.New(cfg.Artifacts)
	if err != nil {
		return nil, err
	}
	inventory, err := infrareadiness.NewJetStream(cfg.System, cfg.Accounts)
	if err != nil {
		return nil, err
	}
	ready, err := infrareadiness.New(cfg.Topology, inventory, recovery, cfg.Auth)
	if err != nil {
		return nil, err
	}
	registry, err := ingressmove.New(cfg.Witness, cfg.VerifyProof)
	if err != nil {
		return nil, err
	}
	routes, err := inframaint.NewRoutes(cfg.Routes)
	if err != nil {
		return nil, err
	}
	transfers, err := infratransfer.New(cfg.Transfers)
	if err != nil {
		return nil, err
	}
	natsKey, caddyKey, err := infraupgrade.RegisterInfraDomain(cfg.Registration)
	if err != nil {
		return nil, err
	}
	if natsKey.DomainID != hex.EncodeToString(cfg.Authority.Domain[:]) {
		return nil, c.ErrIncomplete
	}
	natsTx, err := transaction(cfg.NATS, natsKey, cfg.Registration.ControlDir)
	if err != nil {
		return nil, err
	}
	caddyTx, err := transaction(cfg.Caddy, caddyKey, cfg.Registration.ControlDir)
	if err != nil {
		return nil, err
	}
	tx, err := infraupgrade.RegisterTransactions(natsTx, caddyTx)
	if err != nil {
		return nil, err
	}
	deps := c.Dependencies{Host: host, Artifacts: artifacts, Ready: componentReadiness{ready, cfg.CaddyReady},
		Cohort: &checkpointCohort{raw: registry, recovery: recovery}, Routes: routes,
		Transfers: transfers, Recovery: recovery, Tx: tx, Observe: cfg.Observe}
	graph := &Graph{Dependencies: deps, Registry: registry, Transactions: tx}
	// Publication is last: live dial/producer sources need the actual owners,
	// including the transfer adapter used by the broker's recovery callback.
	if err := cfg.Bind(graph); err != nil {
		return nil, err
	}
	return graph, nil
}

func transaction(cfg Transaction, key upgradetx.Key, control string) (*infraupgrade.TransactionAdapter, error) {
	options := cfg.Options
	options.Key = key
	store, err := upgradestore.Open(upgradestore.Options{Key: key, Owner: options.Owner,
		Path: filepath.Join(cfg.Directory, "transactions.json"), GuardPath: filepath.Join(control, key.DomainID+".guard"),
		Transition: infraupgrade.TransactionTransition})
	if err != nil {
		return nil, err
	}
	options.Store = store
	options.Journal = infraupgrade.FileTransactionJournal{Key: key, Directory: cfg.Directory}
	return infraupgrade.NewTransaction(options)
}

// Service keeps the exact same dependencies for both component drivers and the
// router. Decorations used by independent oracles must happen before this call.
func Service(deps c.Dependencies) (c.Service, error) {
	nats, err := infraupgrade.NewNATS(deps)
	if err != nil {
		return nil, err
	}
	caddy, err := infraupgrade.NewCaddy(deps)
	if err != nil {
		return nil, err
	}
	return infraupgrade.NewService(deps, nats, caddy)
}
