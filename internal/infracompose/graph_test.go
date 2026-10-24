package infracompose

import (
	"crypto/ed25519"
	"encoding/hex"
	"errors"
	"io"
	"os"
	"path/filepath"
	"testing"

	"github.com/LinZiyang666/tether/internal/infraartifact"
	c "github.com/LinZiyang666/tether/internal/infracontract"
	"github.com/LinZiyang666/tether/internal/inframaint"
	"github.com/LinZiyang666/tether/internal/infrapriv"
	"github.com/LinZiyang666/tether/internal/infrareadiness"
	"github.com/LinZiyang666/tether/internal/infratransfer"
	"github.com/LinZiyang666/tether/internal/infraupgrade"
	"github.com/LinZiyang666/tether/internal/upgradelock"
	"github.com/LinZiyang666/tether/internal/upgradetx"
	"github.com/nats-io/nats.go"
)

// Registration, Store, FileTransactionJournal and all seven leaf factories are
// real. IO/evidence ports below only refuse; a constructed graph is not evidence
// of a stock cut, compatibility, normal authentication or root authority.
type unavailablePorts struct{}

func (*unavailablePorts) Do(c.HostRequest, c.Deadline, c.Stop) (c.HostReceipt, error) {
	return c.HostReceipt{}, c.ErrIncomplete
}
func (*unavailablePorts) Open(infraartifact.Entry, c.Deadline, c.Stop) (io.ReadCloser, io.ReadCloser, error) {
	return nil, nil, c.ErrIncomplete
}
func (*unavailablePorts) Version(*infraartifact.Verified, c.Deadline, c.Stop) (infraartifact.VersionReport, error) {
	return infraartifact.VersionReport{}, c.ErrIncomplete
}
func (*unavailablePorts) Check(infraartifact.CompatibilityRequest, *infraartifact.Verified, *infraartifact.Verified, c.Deadline) (infraartifact.CompatibilityReport, error) {
	return infraartifact.CompatibilityReport{}, c.ErrIncomplete
}
func (*unavailablePorts) ReadTopology(c.Plan, c.Deadline, c.Stop) (infrareadiness.Topology, error) {
	return infrareadiness.Topology{}, c.ErrIncomplete
}
func (*unavailablePorts) Request(string, []byte, c.Deadline, c.Stop) ([]byte, error) {
	return nil, c.ErrIncomplete
}
func (*unavailablePorts) Verify(c.Attachment, c.RouteTarget, c.Deadline) error {
	return c.ErrIncomplete
}
func (*unavailablePorts) Census(c.Plan, []c.Attachment, c.Deadline) (c.Census, error) {
	return c.Census{}, c.ErrIncomplete
}
func (*unavailablePorts) Record(c.Observation) error { return c.ErrIncomplete }
func (*unavailablePorts) ValidateCaddyReload(c.Plan, c.Deadline, c.Stop) error {
	return c.ErrIncomplete
}
func (*unavailablePorts) ReloadCaddy(c.Plan, c.Process, c.Deadline, c.Stop) (c.Digest, error) {
	return c.Digest{}, c.ErrIncomplete
}
func (*unavailablePorts) Read(c.Plan, c.Deadline, c.Stop) (c.Readiness, error) {
	return c.Readiness{}, c.ErrIncomplete
}

type unavailableRouteSource struct{}

func (*unavailableRouteSource) Read(c.Plan, c.Checkpoint, c.RouteDirection, c.Deadline) ([]inframaint.State, error) {
	return nil, c.ErrIncomplete
}

type unavailableEvidence struct{}

func (*unavailableEvidence) Read(c.Plan, upgradetx.Attempt, c.Phase, c.Digest, c.Deadline) (infraupgrade.TransactionProof, error) {
	return infraupgrade.TransactionProof{}, c.ErrIncomplete
}

func graphConfiguration(t *testing.T) Config {
	t.Helper()
	dir := t.TempDir()
	canonical := filepath.Join(dir, "canonical")
	if err := os.WriteFile(canonical, []byte("canonical deployment identity"), 0600); err != nil {
		t.Fatal(err)
	}
	natsKey := upgradetx.Key{Component: upgradetx.ComponentNATS, ConfiguredNID: "node", UID: uint32(os.Getuid()), ConfigPath: filepath.Join(dir, "nats.conf")}
	caddyKey := natsKey
	caddyKey.Component, caddyKey.ConfigPath = upgradetx.ComponentCaddy, filepath.Join(dir, "caddy.json")
	reg := upgradelock.Registration{CanonicalPath: canonical, ControlDir: dir, DeploymentID: "composition",
		Participants: []upgradelock.Participant{{Key: natsKey, ServiceID: "nats"}, {Key: caddyKey, ServiceID: "caddy"}}}
	natsKey, caddyKey, err := infraupgrade.RegisterInfraDomain(reg)
	if err != nil {
		t.Fatal(err)
	}
	var domain c.ID
	b, err := hex.DecodeString(natsKey.DomainID)
	if err != nil {
		t.Fatal(err)
	}
	copy(domain[:], b)
	private := ed25519.NewKeyFromSeed(make([]byte, ed25519.SeedSize))
	ports := &unavailablePorts{}
	sign := func(b []byte) ([]byte, error) { return ed25519.Sign(private, b), nil }
	owner := upgradetx.Owner{BootID: "test-boot", PID: os.Getpid(), StartTicks: 1, InstanceID: "controller"}
	transaction := func(key upgradetx.Key) Transaction {
		dir := filepath.Join(dir, string(key.Component))
		if err := os.Mkdir(dir, 0700); err != nil {
			t.Fatal(err)
		}
		scope := upgradetx.Scope{ID: "11111111111111111111111111111111", Participants: []upgradetx.Key{key}, Domains: []upgradetx.Domain{upgradetx.DomainControl},
			Expected: []upgradetx.Resource{{Domain: upgradetx.DomainControl, ID: "control", Owner: owner, Origin: "original", Generation: 1, State: "live", Custody: "owned"}}}
		return Transaction{Directory: dir, Options: infraupgrade.TransactionOptions{Context: t.Context(), Owner: owner,
			ActorFP: "actor", Incarnation: "controller", Scope: scope, Evidence: &unavailableEvidence{}}}
	}
	return Config{Transport: ports, Authority: infrapriv.HostAuthority{Domain: domain, BootID: "test-boot", Revision: 1, KeyID: "current",
		Public: private.Public().(ed25519.PublicKey), Sign: sign},
		Artifacts: infraartifact.Config{Source: ports, Supervisor: ports, Compatibility: ports}, Topology: ports, System: ports,
		Accounts: []infrareadiness.AccountAccess{{ID: "$G", Query: ports}}, CaddyReady: ports,
		Auth: &infrareadiness.OrdinaryAuth{
			Dial: func(infrareadiness.Server, infrareadiness.Identity, c.Deadline, c.Stop) (*nats.Conn, error) {
				return nil, c.ErrIncomplete
			},
			Receipt: func(*nats.Conn, infrareadiness.Identity, c.Deadline, c.Stop) (infrareadiness.AuthReceipt, error) {
				return infrareadiness.AuthReceipt{}, c.ErrIncomplete
			}, RPC: func(*nats.Conn, infrareadiness.Identity, c.Deadline, c.Stop) (c.Digest, error) {
				return c.Digest{}, c.ErrIncomplete
			},
			WSS: func(infrareadiness.Server, infrareadiness.Identity, c.Deadline, c.Stop) (infrareadiness.WSSProof, error) {
				return infrareadiness.WSSProof{}, c.ErrIncomplete
			}}, Witness: ports, VerifyProof: func(c.Plan, c.RouteProof, c.Deadline) error { return c.ErrIncomplete },
		Bind:   func(*Graph) error { return nil },
		Routes: inframaint.Config{Domain: domain, KeyID: "current", Sign: sign, Source: &unavailableRouteSource{}},
		Transfers: infratransfer.Config{SourceSHA256: c.Digest{1}, SchemaSHA256: infratransfer.ObservationSchema(),
			BeforeReplay: func(c.ID, c.Deadline) error { return c.ErrIncomplete }}, Registration: reg,
		NATS: transaction(natsKey), Caddy: transaction(caddyKey), Observe: ports}
}

// origin: hot-update S9 plan §2.1 / §8 / P22 / NATS-COMPOSITION
func TestCompositionBuildsRegisteredOwnersWithoutSigningEvidence(t *testing.T) {
	cfg := graphConfiguration(t)
	_, _, recovery := checkpointFixture()
	var bound *Graph
	cfg.Bind = func(graph *Graph) error { bound = graph; return nil }
	graph, err := New(cfg, recovery)
	if err != nil || graph == nil || graph.Registry == nil || graph != bound {
		t.Fatal("real factory graph did not assemble", err)
	}
	deps := graph.Dependencies
	if _, ok := deps.Host.(*infrapriv.ManagementHost); !ok {
		t.Fatal("graph bypassed authenticated management host")
	}
	if _, ok := deps.Artifacts.(*infraartifact.Catalog); !ok {
		t.Fatal("graph bypassed embedded catalog")
	}
	if _, ok := deps.Transfers.(*infratransfer.Adapter); !ok {
		t.Fatal("graph bypassed retained S7 adapter")
	}
	cohort, ok := deps.Cohort.(*checkpointCohort)
	if !ok || cohort.raw != graph.Registry || cohort.recovery != recovery || deps.Recovery != recovery || deps.Tx != graph.Transactions {
		t.Fatal("graph replaced an original participant, recovery or transaction owner")
	}
	if _, ok := deps.Tx.(infraupgrade.NATSJournal); !ok {
		t.Fatal("NATS lost its durable recovery operands through component routing")
	}
	if _, ok := deps.Tx.(infraupgrade.CaddyJournal); !ok {
		t.Fatal("Caddy lost its durable recovery operands through component routing")
	}
	service, err := Service(deps)
	if err != nil || service == nil {
		t.Fatal("same dependencies did not construct both drivers and Service", err)
	}
	d := checkpointDeadline(t)
	if _, err := graph.Snapshot(c.Checkpoint{Cohort: c.ID{99}}, d); err == nil {
		t.Fatal("normal authority enumerated an unbound checkpoint")
	}
	if _, err := service.Status(c.ID{9}, d); err == nil {
		t.Fatal("construction invented a durable transaction")
	}
	for _, component := range []c.Component{c.NATS, c.Caddy} {
		if _, err := deps.Ready.Read(c.Plan{Component: component}, d, nil); err == nil {
			t.Fatal("construction signed missing runtime evidence")
		}
	}
}

// origin: hot-update S9 plan §2.1 / §8 / P22 / NATS-COMPOSITION
func TestCompositionRejectsMissingRuntimeSourcesBeforeAdmission(t *testing.T) {
	for _, field := range []string{"source", "supervisor", "compatibility", "topology", "system", "account", "caddy", "witness", "auth", "wss", "route", "observe", "evidence", "domain", "journals"} {
		t.Run(field, func(t *testing.T) {
			cfg := graphConfiguration(t)
			_, _, recovery := checkpointFixture()
			var missing *unavailablePorts
			switch field {
			case "source":
				cfg.Artifacts.Source = missing
			case "supervisor":
				cfg.Artifacts.Supervisor = missing
			case "compatibility":
				cfg.Artifacts.Compatibility = missing
			case "topology":
				cfg.Topology = missing
			case "system":
				cfg.System = missing
			case "account":
				cfg.Accounts[0].Query = missing
			case "caddy":
				cfg.CaddyReady = missing
			case "witness":
				cfg.Witness = missing
			case "auth":
				cfg.Auth = nil
			case "wss":
				cfg.Auth.WSS = nil
			case "route":
				cfg.Routes.Source = (*unavailableRouteSource)(nil)
			case "observe":
				cfg.Observe = missing
			case "evidence":
				cfg.NATS.Options.Evidence = (*unavailableEvidence)(nil)
			case "domain":
				cfg.Routes.Domain = c.ID{99}
			case "journals":
				cfg.Caddy.Directory = cfg.NATS.Directory
			}
			admitted := false
			cfg.Bind = func(*Graph) error { admitted = true; return nil }
			if graph, err := New(cfg, recovery); err == nil || graph != nil || admitted {
				t.Fatal("incomplete runtime graph reached live admission")
			}
		})
	}
}

type markedReadiness struct {
	*unavailablePorts
	err error
}

func (r markedReadiness) Read(c.Plan, c.Deadline, c.Stop) (c.Readiness, error) {
	return c.Readiness{}, r.err
}

// origin: hot-update S9 plan §3.2 / §5.1 / P22 / NATS-COMPOSITION
func TestReadinessRoutesComponentAndReloadToOwner(t *testing.T) {
	natsError, caddyError := errors.New("NATS observation unavailable"), errors.New("Caddy observation unavailable")
	r := componentReadiness{nats: markedReadiness{err: natsError},
		CaddyReadiness: markedReadiness{unavailablePorts: &unavailablePorts{}, err: caddyError}}
	d := checkpointDeadline(t)
	for _, row := range []struct {
		component c.Component
		err       error
	}{{c.NATS, natsError}, {c.Caddy, caddyError}, {c.HostSupport, c.ErrUnsupported}} {
		if _, err := r.Read(c.Plan{Component: row.component}, d, nil); !errors.Is(err, row.err) {
			t.Fatal("readiness reached another component owner", row.component, err)
		}
	}
	if err := r.ValidateCaddyReload(c.Plan{}, d, nil); !errors.Is(err, c.ErrIncomplete) {
		t.Fatal("reload validation bypassed the admin owner", err)
	}
	if _, err := r.ReloadCaddy(c.Plan{}, c.Process{}, d, nil); !errors.Is(err, c.ErrIncomplete) {
		t.Fatal("reload bypassed the admin owner", err)
	}
}
