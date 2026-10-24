package infraattach

import (
	"crypto/ed25519"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"runtime"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/LinZiyang666/tether/internal/adminsock"
	"github.com/LinZiyang666/tether/internal/infraartifact"
	c "github.com/LinZiyang666/tether/internal/infracontract"
	"github.com/LinZiyang666/tether/internal/infrapriv"
	"github.com/LinZiyang666/tether/internal/infraupgrade"
	"github.com/LinZiyang666/tether/internal/proto"
	"github.com/LinZiyang666/tether/internal/testharness"
	"github.com/LinZiyang666/tether/internal/upgradelock"
	"github.com/LinZiyang666/tether/internal/upgradestore"
	"github.com/LinZiyang666/tether/internal/upgradetx"
)

// These ports refuse effects. The test certifies composition, shared recovery
// ownership and public projection, never a physical cut or kernel readiness.
type unavailableServicePorts struct{}

func (unavailableServicePorts) Read(c.Plan, c.Deadline, c.Stop) (c.Readiness, error) {
	return c.Readiness{}, c.ErrIncomplete
}
func (unavailableServicePorts) Seal(c.Plan, c.Deadline) (c.Checkpoint, error) {
	return c.Checkpoint{}, c.ErrIncomplete
}
func (unavailableServicePorts) Install(c.Checkpoint, c.RouteProof, c.Deadline) error {
	return c.ErrIncomplete
}
func (unavailableServicePorts) Move(c.Checkpoint, c.Deadline, c.Stop) (c.MoveResult, error) {
	return c.MoveResult{}, c.ErrIncomplete
}
func (unavailableServicePorts) Return(c.Checkpoint, c.Deadline, c.Stop) (c.MoveResult, error) {
	return c.MoveResult{}, c.ErrIncomplete
}
func (unavailableServicePorts) Bind(c.Checkpoint, []c.RouteProof, c.Deadline) (c.Census, error) {
	return c.Census{}, c.ErrIncomplete
}
func (unavailableServicePorts) Release(c.Checkpoint, c.Deadline) error { return c.ErrIncomplete }
func (unavailableServicePorts) Prepare(c.Plan, c.Checkpoint, c.RouteDirection, c.Deadline) ([]c.RouteProof, error) {
	return nil, c.ErrIncomplete
}

func (unavailableServicePorts) VerifyBeforeReplay(c.Checkpoint, c.Deadline, c.Stop) error {
	return c.ErrIncomplete
}

type unavailableRoutes struct{}

func (unavailableRoutes) Prepare(c.Plan, c.Checkpoint, c.RouteDirection, c.Deadline) ([]c.RouteProof, error) {
	return nil, c.ErrIncomplete
}
func (unavailableRoutes) Bind(c.Plan, c.MoveResult, c.RouteDirection, c.Deadline) ([]c.RouteProof, error) {
	return nil, c.ErrIncomplete
}

type unavailableTransfers struct{}

func (unavailableTransfers) Fence(c.ID, c.Deadline) (c.TransferSeal, error) {
	return c.TransferSeal{}, c.ErrIncomplete
}
func (unavailableTransfers) Rebind(c.TransferSeal, c.Deadline) error  { return c.ErrIncomplete }
func (unavailableTransfers) Release(c.TransferSeal, c.Deadline) error { return c.ErrIncomplete }

type unavailableObserver struct{}

func (unavailableObserver) Record(c.Observation) error { return c.ErrIncomplete }

type unavailableTransactionEvidence struct{}

func (unavailableTransactionEvidence) Read(c.Plan, upgradetx.Attempt, c.Phase, c.Digest, c.Deadline) (infraupgrade.TransactionProof, error) {
	return infraupgrade.TransactionProof{}, c.ErrIncomplete
}

type unavailableHostTransport struct{}

func (unavailableHostTransport) Do(c.HostRequest, c.Deadline, c.Stop) (c.HostReceipt, error) {
	return c.HostReceipt{}, c.ErrUnsupported
}

func serviceConfiguration(t *testing.T) (*ServiceConfig, c.ID) {
	t.Helper()
	dir := t.TempDir()
	canonical := filepath.Join(dir, "canonical")
	if err := os.WriteFile(canonical, []byte("test canonical artifact"), 0600); err != nil {
		t.Fatal(err)
	}
	key := upgradetx.Key{Component: upgradetx.ComponentNATS, ConfiguredNID: "node", ConfigPath: filepath.Join(dir, "nats.conf")}
	caddy := key
	caddy.Component, caddy.ConfigPath = upgradetx.ComponentCaddy, filepath.Join(dir, "caddy.conf")
	natsKey, caddyKey, err := infraupgrade.RegisterInfraDomain(upgradelock.Registration{CanonicalPath: canonical, ControlDir: dir, DeploymentID: "service",
		Participants: []upgradelock.Participant{{Key: key, ServiceID: "nats"}, {Key: caddy, ServiceID: "caddy"}}})
	if err != nil {
		t.Fatal(err)
	}
	owner := upgradetx.Owner{BootID: "boot", PID: os.Getpid(), StartTicks: 1, InstanceID: "service"}
	makeTransaction := func(key upgradetx.Key) *infraupgrade.TransactionAdapter {
		scope := upgradetx.Scope{ID: strings.Repeat("1", 32), Participants: []upgradetx.Key{key}, Domains: []upgradetx.Domain{upgradetx.DomainControl},
			Expected: []upgradetx.Resource{{Domain: upgradetx.DomainControl, ID: "original-control", Origin: "original", Owner: owner, Generation: 1, State: "live", Custody: "owned"}}}
		store, err := upgradestore.Open(upgradestore.Options{Path: filepath.Join(dir, string(key.Component)+".json"), GuardPath: filepath.Join(dir, key.DomainID+".guard"),
			Key: key, Owner: owner, Transition: infraupgrade.TransactionTransition})
		if err != nil {
			t.Fatal(err)
		}
		tx, err := infraupgrade.NewTransaction(infraupgrade.TransactionOptions{Context: t.Context(), Key: key, Owner: owner, ActorFP: "actor", Incarnation: "service", Scope: scope,
			Store: store, Journal: infraupgrade.FileTransactionJournal{Key: key, Directory: dir}, Evidence: unavailableTransactionEvidence{}})
		if err != nil {
			t.Fatal(err)
		}
		return tx
	}
	tx, err := infraupgrade.RegisterTransactions(makeTransaction(natsKey), makeTransaction(caddyKey))
	if err != nil {
		t.Fatal(err)
	}
	var domain c.ID
	data, err := hex.DecodeString(natsKey.DomainID)
	if err != nil {
		t.Fatal(err)
	}
	copy(domain[:], data)
	private := ed25519.NewKeyFromSeed(make([]byte, ed25519.SeedSize))
	host, err := infrapriv.NewManagementHost(unavailableHostTransport{}, infrapriv.HostAuthority{Domain: domain, BootID: "boot", Revision: 1, KeyID: "current",
		Public: private.Public().(ed25519.PublicKey), Sign: func(b []byte) ([]byte, error) { return ed25519.Sign(private, b), nil }})
	if err != nil {
		t.Fatal(err)
	}
	catalog, err := infraartifact.New(infraartifact.Config{})
	if err != nil {
		t.Fatal(err)
	}
	return &ServiceConfig{Dependencies: c.Dependencies{Host: host, Artifacts: catalog, Ready: unavailableServicePorts{}, Cohort: unavailableServicePorts{},
		Recovery: unavailableServicePorts{}, Routes: unavailableRoutes{}, Transfers: unavailableTransfers{}, Tx: tx, Observe: unavailableObserver{}},
		Plan:       func(proto.InfraCommand) (c.Plan, error) { return c.Plan{}, c.ErrIncomplete },
		Capability: func() *proto.InfraCapability { return &proto.InfraCapability{Version: 1, NATS: true, Caddy: true} }}, domain
}

// origin: hot-update S9 plan §2.1 / §7.1 / WIRE-2 R10
func TestBrokerServiceSharesRecoveryAndAdminHealthRuntime(t *testing.T) {
	beforeG, beforeFD := runtime.NumGoroutine(), testharness.FDCount()
	for round := 0; round < 5; round++ {
		t.Run(fmt.Sprint(round), func(t *testing.T) { exerciseBrokerService(t) })
	}
	testharness.AssertNoGoroutineLeak(t, "broker infra Service x5", beforeG)
	testharness.AssertNoFDLeak(t, "broker infra Service x5", beforeFD, 0)
}
func exerciseBrokerService(t *testing.T) {
	t.Helper()
	f := newBrokerAttachFixture(t, true)
	service, domain := serviceConfiguration(t)
	config := f.config(&BrokerRecovery{Domain: domain,
		Assets:    func(c.Deadline, c.Stop) (c.AssetSeal, error) { return c.AssetSeal{}, c.ErrIncomplete },
		Exclusive: func(c.Deadline, c.Stop) error { return c.ErrIncomplete }, Transfers: func(c.Deadline) (c.TransferSeal, error) { return c.TransferSeal{}, c.ErrIncomplete }})
	var durablePending atomic.Bool
	config.Infra.Pending = durablePending.Load
	config.Infra.Service = service
	// A projection cache cannot replace the durable domain's pending reader.
	partial := *config.Infra
	partial.Pending = nil
	unboundConfig := config
	unboundConfig.Infra = &partial
	unbound, err := NewBroker(unboundConfig)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := unbound.Close(brokerAttachDeadline(t, 5*time.Second)); err != nil {
			t.Error(err)
		}
	})
	rejected, err := unbound.Connect(brokerAttachDeadline(t, 5*time.Second))
	if rejected != nil {
		rejected.Close()
	}
	if err == nil {
		t.Fatal("Service activated without the durable domain pending reader")
	}
	owner, err := NewBroker(config)
	if err != nil {
		t.Fatal(err)
	}
	nc, err := owner.Connect(brokerAttachDeadline(t, 5*time.Second))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		nc.Close()
		if err := owner.Close(brokerAttachDeadline(t, 5*time.Second)); err != nil {
			t.Error(err)
		}
	})
	if owner.service == nil {
		t.Fatal("configured Broker.Connect did not compose the real Service")
	}
	// Inspect each REAL driver's stored dependency, rather than trusting an
	// unused config field. Both must name this broker's producer gate and owner.
	realService := reflect.ValueOf(owner.service.Service).Elem()
	for _, component := range []string{"nats", "caddy"} {
		driver := realService.FieldByName(component).Elem().Elem()
		recovery := driver.FieldByName("deps").FieldByName("Recovery").Elem()
		if recovery.Type() != reflect.TypeFor[brokerRecovery]() || recovery.FieldByName("o").Pointer() != reflect.ValueOf(owner).Pointer() ||
			recovery.FieldByName("gate").Pointer() != reflect.ValueOf(owner.gate).Pointer() {
			t.Fatalf("%s driver certified a different recovery owner", component)
		}
	}
	bindings := InfraBindings(owner)
	if bindings == nil || bindings.Service != owner.service || bindings.Capability() != nil {
		t.Fatal("unadmitted service advertised capability")
	}
	f.owner, f.nc = owner, nc
	f.arm(t)
	backend := BindAdmin(owner, adminsock.Backend{})
	if backend.Infra == nil || backend.Infra.Service != bindings.Service {
		t.Fatal("admin uses a different service")
	}
	owner.service.remember(c.Status{Tx: c.ID{1}, Step: 1, Phase: c.Halted, Revision: 3, Detail: "private-canary", Census: c.Census{LiveBusiness: 4}})
	health, runtime := proto.ClusterHealthResp{}, adminsock.RuntimeReport{}
	SetClusterHealth(owner, &health)
	SetRuntimeReport(owner, &runtime)
	encoded, err := json.Marshal([]any{health, runtime})
	if err != nil || strings.Contains(string(encoded), "private-canary") || health.InfraUpgradeVersion != 1 || runtime.InfraUpgradeVersion != 1 ||
		health.Infra == nil || runtime.Infra == nil || health.Infra.Revision != 3 || runtime.Infra.Revision != 3 {
		t.Fatalf("shared service projection absent or leaked private status: %s %v", encoded, err)
	}
	if ImageExclusive(owner) != ErrInfraPending {
		t.Fatal("pending Service did not exclude a broker image")
	}
	owner.service.remember(c.Status{Tx: c.ID{1}, Phase: c.Completed})
	durablePending.Store(true)
	if ImageExclusive(owner) != ErrInfraPending {
		t.Fatal("historical completed projection cleared durable domain exclusion")
	}
	durablePending.Store(false)
	if ImageExclusive(owner) != nil {
		t.Fatal("completed Service retained image exclusion")
	}
}
