package infraupgrade

import (
	"crypto/sha256"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/LinZiyang666/tether/internal/controlbudget"
	c "github.com/LinZiyang666/tether/internal/infracontract"
	"github.com/LinZiyang666/tether/internal/infracontract/contractstub"
	"github.com/LinZiyang666/tether/internal/upgradelock"
	"github.com/LinZiyang666/tether/internal/upgradestore"
	"github.com/LinZiyang666/tether/internal/upgradetx"
)

func infraDeadline(t *testing.T) c.Deadline {
	t.Helper()
	d, err := controlbudget.After(time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	return d
}

func infraPlan(component c.Component) c.Plan {
	kind, version := c.NATSBinary, "2.14.5"
	if component == c.Caddy {
		kind, version = c.CaddyBinary, "2.7.5"
	}
	previous := c.Artifact{Kind: kind, Version: version, GOOS: "linux", GOARCH: "amd64",
		ArchiveSHA256: caddyDigest(1), BinarySHA256: caddyDigest(2), CatalogSHA256: caddyDigest(3)}
	next := previous
	next.Version, next.BinarySHA256, next.ArchiveSHA256 = version+"-next", caddyDigest(4), caddyDigest(5)
	return c.Plan{Tx: caddyID(1), Step: 1, Domain: caddyID(2), Component: component, Action: c.Upgrade,
		Previous: previous, Candidate: next, Before: caddyTestProcess(10, previous.BinarySHA256),
		ConfigSHA256: caddyDigest(6), Store: c.Store{Device: 1, Inode: 2, MountID: 3, LockDevice: 4, LockInode: 5}, PolicyRevision: 1}
}

func infraPhysicalProof(p c.Plan, phase c.Phase, digest c.Digest) TransactionProof {
	current := p.Before
	if phase >= c.Verifying && phase <= c.Completed {
		current = caddyTestProcess(20, p.Candidate.BinarySHA256)
		if p.Action == c.Reload {
			current = p.Before
		}
	}
	return TransactionProof{Plan: p, Journal: caddyDigest(7), Proof: digest, Current: current,
		Census:         c.Census{Complete: true, Generation: 1, Processes: []c.Process{current}, LiveBusiness: 7, Digest: caddyDigest(8)},
		FilesInstalled: true, RuntimeReady: true, ReturnVerified: true, TemporaryRetired: true}
}

// origin: hot-update S9 plan §8 / §11 T44 / M22
func TestInfraComponentsRegisterWithTransaction(t *testing.T) {
	t.Run("neutral", func(t *testing.T) {
		t.Run("stub_service_refuses_admission", func(t *testing.T) {
			deps := contractstub.NewDependencies()
			if service, err := NewService(deps, &natsDriver{deps: deps}, &caddyDriver{deps: deps}); service != nil || !errors.Is(err, c.ErrContractStub) {
				t.Fatalf("unwired transaction registered: service=%v error=%v", service, err)
			}
			var missing *natsDriver
			if service, err := NewService(deps, missing, &caddyDriver{deps: deps}); service != nil || !errors.Is(err, c.ErrContractStub) {
				t.Fatalf("typed nil driver registered: %v, %v", service, err)
			}
			if !errors.Is(transactionError(&upgradetx.Error{Code: upgradetx.CodeNotWired}), c.ErrContractStub) {
				t.Fatal("S8 failure stub became success")
			}
			p := infraPlan(c.NATS)
			s := infraService{tx: refusingServiceTransactions{Transaction: deps.Tx}}
			local := c.Status{Tx: p.Tx, Step: p.Step, Before: p.Before, Phase: c.Completed, Revision: 1}
			status, err := s.result(p, local, infraDeadline(t), nil)
			if err == nil || status.Phase == c.Completed || status.Code != "infra_result_unknown" {
				t.Fatalf("local completion survived a failed live read: %+v %v", status, err)
			}
		})
		t.Run("root_projection_requires_physical_completion", func(t *testing.T) {
			p := infraPlan(c.Caddy)
			proof := caddyDigest(9)
			x := infraPhysicalProof(p, c.Completed, proof)
			if !validTransactionProof(p, c.Completed, proof, x) {
				t.Fatal("positive physical completion with live business refused")
			}
			for name, mutate := range map[string]func(*TransactionProof){
				"wrong_domain":          func(x *TransactionProof) { x.Plan.Domain = caddyID(80) },
				"wrong_tx":              func(x *TransactionProof) { x.Plan.Tx = caddyID(80) },
				"wrong_policy":          func(x *TransactionProof) { x.Plan.PolicyRevision++ },
				"wrong_step":            func(x *TransactionProof) { x.Plan.Step++ },
				"no_root_journal":       func(x *TransactionProof) { x.Journal = c.Digest{} },
				"disk_only":             func(x *TransactionProof) { x.RuntimeReady = false },
				"return_unacknowledged": func(x *TransactionProof) { x.ReturnVerified = false },
				"warm_alive":            func(x *TransactionProof) { x.TemporaryRetired = false },
				"old_process_alive":     func(x *TransactionProof) { x.Census.Processes = append(x.Census.Processes, p.Before) },
				"old_process_execed": func(x *TransactionProof) {
					old := p.Before
					old.ExeSHA256 = p.Candidate.BinarySHA256
					x.Census.Processes = append(x.Census.Processes, old)
				},
				"old_owner_pending": func(x *TransactionProof) { x.Census.OldOwnerPending = 1 },
				"unknown_census":    func(x *TransactionProof) { x.Census.Complete = false },
				"empty_census":      func(x *TransactionProof) { x.Census.Processes = nil },
				"wrong_running_sha": func(x *TransactionProof) {
					x.Current.ExeSHA256 = p.Previous.BinarySHA256
					x.Census.Processes = []c.Process{x.Current}
				},
				"unbound_current": func(x *TransactionProof) { x.Current.PID++ },
				"wrong_boot": func(x *TransactionProof) {
					x.Current.BootID = "another-boot"
					x.Census.Processes = []c.Process{x.Current}
				},
			} {
				t.Run(name, func(t *testing.T) {
					x := infraPhysicalProof(p, c.Completed, proof)
					mutate(&x)
					if validTransactionProof(p, c.Completed, proof, x) {
						t.Fatal("unproven completion accepted")
					}
				})
			}
			p.Action, p.Candidate = c.Reload, p.Previous
			x = infraPhysicalProof(p, c.Completed, proof)
			if !validTransactionProof(p, c.Completed, proof, x) {
				t.Fatal("same-PID reload rejected")
			}
			x.Current = caddyTestProcess(20, p.Previous.BinarySHA256)
			x.Census.Processes = []c.Process{x.Current}
			if validTransactionProof(p, c.Completed, proof, x) {
				t.Fatal("reload changed process identity")
			}
		})
		t.Run("phase_order_and_budget", func(t *testing.T) {
			for phase := c.Prepared; phase < c.Completed; phase++ {
				if !transactionEdge(phase, phase+1) || transactionEdge(phase, c.Completed) && phase != c.Retiring {
					t.Fatalf("phase %v can skip a barrier", phase)
				}
			}
			for _, phase := range []c.Phase{c.Completed, c.Deferred} {
				if transactionEdge(phase, c.Ready) {
					t.Fatal("terminal execution resurrected")
				}
			}
			stop := make(chan struct{})
			close(stop)
			if serviceBudget(infraDeadline(t), stop) == nil || serviceBudget(0, nil) == nil {
				t.Fatal("cancelled or unknown deadline admitted")
			}
			if transactionReason(c.Ready, caddyDigest(1)) == transactionReason(c.Ready, caddyDigest(2)) {
				t.Fatal("CAS intent lost its exact root proof")
			}
			p := infraPlan(c.NATS)
			original := transactionPlanDigest(p)
			for _, mutate := range []func(*c.Plan){
				func(p *c.Plan) { p.PolicyRevision++ }, func(p *c.Plan) { p.Step++ },
				func(p *c.Plan) { p.Domain = caddyID(99) }, func(p *c.Plan) { p.Before.StartTicks++ },
			} {
				next := p
				mutate(&next)
				if transactionPlanDigest(next) == original {
					t.Fatal("grant commitment lost an immutable plan operand")
				}
			}
		})
		t.Run("component_keys_keep_the_registered_domain", func(t *testing.T) {
			p := infraPlan(c.NATS)
			key := upgradetx.Key{DomainID: hexID(p.Domain), Component: upgradetx.ComponentNATS, ConfiguredNID: "node", ConfigPath: "/test/nats.conf"}
			tx := &TransactionAdapter{options: TransactionOptions{Key: key}}
			if !tx.matches(p) {
				t.Fatal("registered component key rejected")
			}
			p.Domain = caddyID(99)
			if tx.matches(p) {
				t.Fatal("plan crossed its registered S8 domain")
			}
			p = infraPlan(c.Caddy)
			if tx.matches(p) {
				t.Fatal("Caddy executed through the NATS component key")
			}
			if _, err := transactionDomain(upgradetx.Key{DomainID: key.DomainID, Component: upgradetx.ComponentBroker}); err == nil {
				t.Fatal("infra component aliased to broker")
			}
			if _, err := NewTransaction(TransactionOptions{}); !errors.Is(err, c.ErrContractStub) {
				t.Fatalf("missing S8 bindings constructed: %v", err)
			}
			parent := TransactionOptions{ParentTxID: hexID(caddyID(50)), ParentSlot: "nats", ParentToken: "held-parent"}
			if !transactionParent(parent) || !transactionParent(TransactionOptions{}) {
				t.Fatal("valid standalone/parent declaration rejected")
			}
			for _, mutate := range []func(*TransactionOptions){
				func(o *TransactionOptions) { o.ParentTxID = "" }, func(o *TransactionOptions) { o.ParentSlot = "" },
				func(o *TransactionOptions) { o.ParentToken = "" }, func(o *TransactionOptions) { o.ParentTxID = "not-an-id" },
			} {
				bad := parent
				mutate(&bad)
				if transactionParent(bad) {
					t.Fatal("child borrowed a reservation without the durable parent slot")
				}
			}
			if router, err := RegisterTransactions(nil, nil); router != nil || !errors.Is(err, c.ErrContractStub) {
				t.Fatal("nil component registrations advertised")
			}
			if _, _, err := RegisterInfraDomain(upgradelock.Registration{}); !errors.Is(err, c.ErrContractStub) {
				t.Fatal("incomplete deployment advertised infrastructure components")
			}
			nats := &TransactionAdapter{options: TransactionOptions{Key: key}}
			caddyKey := key
			caddyKey.Component, caddyKey.DomainID = upgradetx.ComponentCaddy, hexID(caddyID(99))
			if router, err := RegisterTransactions(nats, &TransactionAdapter{options: TransactionOptions{Key: caddyKey}}); router != nil || err == nil {
				t.Fatal("component registrations crossed host domains")
			}
		})
	})
	// These are real S8 registration/store/lock integration tests. The S8
	// snapshot still reserves nats/caddy without enabling their generic keys;
	// until its owners extend those guards these subtests fail at registration.
	// They intentionally do not alias the components or skip that prerequisite.
	for name, component := range map[string]c.Component{"real_s8_nats": c.NATS, "real_s8_caddy": c.Caddy} {
		t.Run(name, func(t *testing.T) { exerciseInfraTransaction(t, component) })
	}
}

// A refusal-only port exercises the service's response to unavailable evidence.
// It never supplies a fake successful S8 or root implementation.
type refusingServiceTransactions struct{ c.Transaction }

func (refusingServiceTransactions) Plan(c.ID, c.Deadline) (c.Plan, error) {
	return c.Plan{}, c.ErrContractStub
}
func (refusingServiceTransactions) ResumePlan(c.ID, c.Deadline) (c.Plan, error) {
	return c.Plan{}, c.ErrContractStub
}
func (refusingServiceTransactions) Finish(c.ID, c.Deadline) error { return c.ErrContractStub }

// This fixture supplies adversarial physical evidence to the neutral adapter;
// it does not claim stock/root integration. Store, transition and lock are real.
type infraEvidenceFixture struct{ scope upgradetx.Scope }

func (f infraEvidenceFixture) Read(p c.Plan, a upgradetx.Attempt, phase c.Phase, proof c.Digest, _ c.Deadline) (TransactionProof, error) {
	x := infraPhysicalProof(p, phase, proof)
	owner := upgradetx.Owner{BootID: "test-boot", PID: 200, StartTicks: 200, InstanceID: "observer"}
	resources := append([]upgradetx.Resource(nil), f.scope.Expected...)
	for i := range resources {
		resources[i].Owner, resources[i].Generation = owner, a.Handle.Generation
		resources[i].Custody = "target"
	}
	nonce := hexID(caddyID(byte(a.Handle.Revision + 30)))
	scopeBytes, _ := json.Marshal(f.scope)
	expectedBytes, _ := json.Marshal(f.scope.Expected)
	stamp := upgradetx.Stamp{Attempt: a.Handle, Producer: owner, Nonce: nonce, Sequence: a.Handle.Revision + 100,
		At: time.Now().UnixNano(), ScopeDigest: digestText(sha256.Sum256(scopeBytes))}
	census := upgradetx.Census{Stamp: stamp, Known: true, Complete: true, Resources: resources, ExpectedDigest: digestText(sha256.Sum256(expectedBytes))}
	kind := upgradetx.ReceiptControlReady
	if phase == c.Retiring {
		kind = upgradetx.ReceiptResourcesMigrated
	}
	if phase == c.Completed {
		kind = upgradetx.ReceiptOldGenerationReleased
	}
	x.Receipt = upgradetx.Receipt{Kind: kind, Stamp: stamp, Known: true, Complete: true, RunningSHA: a.TargetSHA,
		Census: census, EvidenceDigest: digestText(proof)}
	if a.Phase == upgradetx.PhaseCompleted {
		stamp.Nonce = hexID(caddyID(250))
		stamp.Producer, stamp.ScopeDigest = a.Released.Stamp.Producer, a.Released.Stamp.ScopeDigest
		census.Stamp = stamp
		x.Observation = upgradetx.Observation{Nonce: stamp.Nonce, At: stamp.At, Current: a, Census: census,
			OwnerKnown: true, RunningSHA: a.TargetSHA, EffectiveSHA: a.TargetSHA}
	}
	return x, nil
}

func infraScope(key upgradetx.Key, owner upgradetx.Owner) upgradetx.Scope {
	s := upgradetx.Scope{ID: hexID(caddyID(60)), Participants: []upgradetx.Key{key}, Domains: []upgradetx.Domain{
		upgradetx.DomainAuthority, upgradetx.DomainBrokerRuntime, upgradetx.DomainBrokerStore, upgradetx.DomainControl,
		upgradetx.DomainExec, upgradetx.DomainFlow, upgradetx.DomainPTY, upgradetx.DomainSS,
		upgradetx.DomainTransferTracker, upgradetx.DomainTransferWorker, upgradetx.DomainUpgrade}}
	for _, domain := range s.Domains {
		s.Expected = append(s.Expected, upgradetx.Resource{Domain: domain, ID: "original-" + string(domain), Origin: "original", Owner: owner, Generation: 1, State: "live", Custody: "owned"})
	}
	return s
}

func exerciseInfraTransaction(t *testing.T, component c.Component) {
	t.Helper()
	p := infraPlan(component)
	dir := t.TempDir()
	if err := os.Chmod(dir, 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "canonical"), []byte("test artifact"), 0o600); err != nil {
		t.Fatal(err)
	}
	key := upgradetx.Key{Component: upgradetx.ComponentNATS, ConfiguredNID: "node", ConfigPath: filepath.Join(dir, "nats.conf")}
	caddyKey := key
	caddyKey.Component, caddyKey.ConfigPath = upgradetx.ComponentCaddy, filepath.Join(dir, "caddy.conf")
	natsKey, caddyKey, err := RegisterInfraDomain(upgradelock.Registration{CanonicalPath: filepath.Join(dir, "canonical"), ControlDir: dir,
		DeploymentID: "infra-test", Participants: []upgradelock.Participant{{Key: key, ServiceID: "nats"}, {Key: caddyKey, ServiceID: "caddy"}}})
	if err != nil {
		t.Fatalf("real S8 component registration must accept %s: %v", key.Component, err)
	}
	key = natsKey
	if component == c.Caddy {
		key = caddyKey
	}
	domain := key.DomainID
	p.Domain, err = transactionDomain(key)
	if err != nil {
		t.Fatal(err)
	}
	owner := upgradetx.Owner{BootID: "test-boot", PID: 100, StartTicks: 100, InstanceID: "controller"}
	scope := infraScope(key, owner)
	store, err := upgradestore.Open(upgradestore.Options{Path: filepath.Join(dir, "s8.json"), GuardPath: filepath.Join(dir, domain+".guard"),
		Key: key, Owner: owner, Transition: TransactionTransition})
	if err != nil {
		t.Fatal(err)
	}
	options := TransactionOptions{Context: t.Context(), Key: key, Owner: owner, ActorFP: "actor", Incarnation: "incarnation", Scope: scope,
		Store: store, Journal: FileTransactionJournal{Key: key, Directory: dir}, Evidence: infraEvidenceFixture{scope}}
	tx, err := NewTransaction(options)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = tx.release() })
	d := infraDeadline(t)
	status, err := tx.Acquire(p, d)
	if err != nil || status.Phase != c.Prepared {
		t.Fatalf("prepare: %+v, %v", status, err)
	}
	if _, err := upgradelock.Acquire(t.Context(), key, ""); err == nil {
		t.Fatal("infra admission did not hold the shared domain")
	}
	for phase := c.Staged; phase <= c.Completed; phase++ {
		prior := status
		status, err = tx.CompareAndSwap(prior, phase, caddyDigest(byte(phase+40)), d)
		if err != nil || status.Phase != phase || status.Revision <= prior.Revision {
			t.Fatalf("phase %v: %+v, %v", phase, status, err)
		}
		if _, err := tx.CompareAndSwap(prior, phase, caddyDigest(byte(phase+40)), d); err == nil {
			t.Fatal("stale status overwrote a completed CAS")
		}
	}
	reopened, err := NewTransaction(options)
	if err != nil {
		t.Fatal(err)
	}
	loaded, err := reopened.Load(p.Tx, d)
	if err != nil || loaded.Phase != c.Completed || loaded.Census.LiveBusiness != 7 {
		t.Fatalf("reopened completion: %+v, %v", loaded, err)
	}
	if err := tx.Finish(p.Tx, d); err != nil {
		t.Fatal(err)
	}
	lease, err := upgradelock.Acquire(t.Context(), key, "")
	if err != nil {
		t.Fatalf("settled component retained its domain: %v", err)
	}
	if err := lease.Close(); err != nil {
		t.Fatal(err)
	}
}
