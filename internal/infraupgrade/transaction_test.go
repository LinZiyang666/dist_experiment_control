package infraupgrade

import (
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"runtime"
	"testing"

	c "github.com/LinZiyang666/tether/internal/infracontract"
	"github.com/LinZiyang666/tether/internal/testharness"
	"github.com/LinZiyang666/tether/internal/upgradelock"
	"github.com/LinZiyang666/tether/internal/upgradestore"
	"github.com/LinZiyang666/tether/internal/upgradetx"
)

func supportFixture() (SupportState, SupportCensus, c.Artifact) {
	active := c.Artifact{Kind: c.HelperBinary, Version: "current", GOOS: "linux", GOARCH: "amd64",
		ArchiveSHA256: caddyDigest(1), BinarySHA256: caddyDigest(2), CatalogSHA256: caddyDigest(3)}
	previous, candidate := active, active
	previous.Version, previous.BinarySHA256 = "previous", caddyDigest(4)
	candidate.Version, candidate.BinarySHA256 = "next", caddyDigest(5)
	before := caddyTestProcess(10, active.BinarySHA256)
	s := SupportState{Domain: caddyID(1), Revision: 1, Kind: c.HelperBinary, Active: active, Previous: previous,
		Selector: caddyDigest(6), Journal: caddyDigest(7), Before: []c.Process{before}}
	x := SupportCensus{Domain: s.Domain, Generation: 1, Digest: caddyDigest(8), Complete: true, Observer: before, Processes: []c.Process{before}}
	return s, x, candidate
}

// origin: hot-update S9 plan §6.5 / §7.3 / §11 T45 / M22
func TestInfraImageAndSupportDomainsExcludeOverlap(t *testing.T) {
	goroutines, fds := runtime.NumGoroutine(), testharness.FDCount()
	for round := 0; round < 5; round++ {
		exerciseSupportDomain(t)
	}
	testharness.AssertNoGoroutineLeak(t, "infra support domain", goroutines)
	testharness.AssertNoFDLeak(t, "infra support domain", fds, 0)
	t.Run("support_retains_two_generations", func(t *testing.T) {
		s, census, candidate := supportFixture()
		for _, busy := range []bool{false, true} {
			if _, err := PrepareSupport(s, candidate, busy); err == nil {
				t.Fatal("third generation displaced the rollback artifact")
			}
		}
		if next, err := PrepareSupport(s, s.Previous, true); err != nil || next.Pending != s.Previous || next.Previous != s.Previous || next.Selector != s.Selector {
			t.Fatalf("busy stage changed a selector or lost rollback: %+v %v", next, err)
		}
		s.RollbackClosed = true
		s.RetainedOwners = true
		if _, err := PrepareSupport(s, candidate, false); err == nil {
			t.Fatal("retained owner was evicted with its artifact")
		}
		s.RetainedOwners = false
		next, err := PrepareSupport(s, candidate, false)
		if err != nil || next.Previous != (c.Artifact{}) || next.Pending != candidate || next.Active != s.Active {
			t.Fatalf("released closed window cannot stage next generation: %+v %v", next, err)
		}
		for _, mutate := range []func(*c.Artifact){
			func(a *c.Artifact) { a.Kind = c.NATSBinary }, func(a *c.Artifact) { a.GOARCH = "arm64" },
			func(a *c.Artifact) { a.CatalogSHA256 = c.Digest{} }, func(a *c.Artifact) { a.ArchiveSHA256 = c.Digest{} },
		} {
			bad := candidate
			mutate(&bad)
			if _, err := PrepareSupport(s, bad, false); err == nil {
				t.Fatal("unverified supporting artifact staged")
			}
		}
		staged, err := PrepareSupport(s, s.Previous, true)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := SupportSelection(staged, census.Observer, census, caddyDigest(10), caddyDigest(11)); err == nil {
			t.Fatal("busy stage selected a supporting artifact")
		}
		s.Before[0].PID++
		if staged.Before[0].PID == s.Before[0].PID {
			t.Fatal("staged worker roster aliases caller memory")
		}
	})
	t.Run("worker_exit_is_independent", func(t *testing.T) {
		s, census, _ := supportFixture()
		var err error
		s, err = PrepareSupport(s, s.Previous, false)
		if err != nil {
			t.Fatal(err)
		}
		another := caddyTestProcess(11, s.Active.BinarySHA256)
		s.Before = append(s.Before, another)
		census.Processes = append(census.Processes, another)
		if _, err := SupportSelection(s, census.Observer, census, caddyDigest(10), caddyDigest(11)); err == nil {
			t.Fatal("selector changed before other old workers joined")
		}
		census.Processes = census.Processes[:1]
		late := census
		late.Processes = append(append([]c.Process(nil), census.Processes...), caddyTestProcess(80, s.Active.BinarySHA256))
		if _, err := SupportSelection(s, census.Observer, late, caddyDigest(10), caddyDigest(11)); err == nil {
			t.Fatal("late old-selector worker survived selection")
		}
		if _, err := SupportSelection(s, census.Observer, census, s.Selector, caddyDigest(11)); err == nil {
			t.Fatal("selector CAS acknowledged an unchanged selector")
		}
		selected, err := SupportSelection(s, census.Observer, census, caddyDigest(10), caddyDigest(11))
		if err != nil || !selected.Retiring {
			t.Fatalf("selector intent not retained: %+v %v", selected, err)
		}
		if _, err := CompleteSupport(selected, census, caddyDigest(12)); err == nil {
			t.Fatal("old worker signed its own retirement")
		}
		newWorker := caddyTestProcess(20, selected.Pending.BinarySHA256)
		census.Observer, census.Processes = newWorker, []c.Process{newWorker}
		for name, mutate := range map[string]func(*SupportCensus){
			"old_alive":        func(x *SupportCensus) { x.Processes = append(x.Processes, s.Before[0]) },
			"late_old_worker":  func(x *SupportCensus) { x.Processes = append(x.Processes, caddyTestProcess(80, s.Active.BinarySHA256)) },
			"unknown":          func(x *SupportCensus) { x.Complete = false },
			"empty":            func(x *SupportCensus) { x.Processes = nil },
			"wrong_domain":     func(x *SupportCensus) { x.Domain = caddyID(90) },
			"unknown_observer": func(x *SupportCensus) { x.Observer.StartTicks = 0 },
			"reused_pid": func(x *SupportCensus) {
				x.Observer = s.Before[0]
				x.Observer.ExeSHA256 = selected.Pending.BinarySHA256
				x.Processes = []c.Process{x.Observer}
			},
		} {
			t.Run(name, func(t *testing.T) {
				x := census
				x.Processes = append([]c.Process(nil), census.Processes...)
				mutate(&x)
				if _, err := CompleteSupport(selected, x, caddyDigest(12)); err == nil {
					t.Fatal("unproven worker retirement completed")
				}
			})
		}
		completed, err := CompleteSupport(selected, census, caddyDigest(12))
		if err != nil || completed.Active != s.Previous || completed.Previous != s.Active || completed.Pending != (c.Artifact{}) || completed.Retiring {
			t.Fatalf("rollback did not retain both artifacts: %+v %v", completed, err)
		}
	})
	t.Run("launcher_exec_is_a_distinct_retirement", func(t *testing.T) {
		s, census, _ := supportFixture()
		s.Kind, s.Active.Kind, s.Previous.Kind = c.LauncherBinary, c.LauncherBinary, c.LauncherBinary
		s.Pending = s.Previous
		// The selecting helper is not a resident launcher or the stock server.
		caller := caddyTestProcess(30, caddyDigest(70))
		census.Observer, census.Processes = caller, []c.Process{caller}
		selected, err := SupportSelection(s, caller, census, caddyDigest(10), caddyDigest(11))
		if err != nil {
			t.Fatal(err)
		}
		observer := caddyTestProcess(40, caddyDigest(70))
		stock := s.Before[0]
		stock.ExeSHA256, stock.ExeInode = caddyDigest(71), 900
		census.Observer, census.Processes, census.LaunchTarget, census.InheritedLease = observer, []c.Process{observer, stock}, stock.ExeSHA256, true
		if _, err := CompleteSupport(selected, census, caddyDigest(12)); err != nil {
			t.Fatalf("same-PID stock exec with inherited lease rejected: %v", err)
		}
		self := census
		self.Observer, self.Processes = caller, []c.Process{caller, stock}
		if _, err := CompleteSupport(selected, self, caddyDigest(12)); err == nil {
			t.Fatal("selecting worker certified its own retirement")
		}
		for _, mutate := range []func(*SupportCensus){
			func(x *SupportCensus) { x.InheritedLease = false },
			func(x *SupportCensus) { x.LaunchTarget = c.Digest{} },
			func(x *SupportCensus) { x.Processes[1] = s.Before[0] },
			func(x *SupportCensus) { x.Processes[1].InvocationID = "replacement-unit" },
		} {
			x := census
			x.Processes = append([]c.Process(nil), census.Processes...)
			mutate(&x)
			if _, err := CompleteSupport(selected, x, caddyDigest(12)); err == nil {
				t.Fatal("pre-exec or unbound launcher ACK retired the image")
			}
		}
	})
	t.Run("sidecar_and_s8_phase_mapping", func(t *testing.T) { exerciseTransactionPersistence(t) })
}

func exerciseSupportDomain(t *testing.T) {
	t.Helper()
	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, "canonical"), []byte("test artifact"), 0o600); err != nil {
		t.Fatal(err)
	}
	brokerKey := upgradetx.Key{Component: upgradetx.ComponentBroker, ConfigPath: filepath.Join(dir, "broker.conf")}
	agentKey := upgradetx.Key{Component: upgradetx.ComponentAgent, SID: "sid", ConfiguredNID: "nid"}
	domain, err := upgradelock.RegisterDomain(upgradelock.Registration{CanonicalPath: filepath.Join(dir, "canonical"), ControlDir: dir, DeploymentID: "shared",
		Participants: []upgradelock.Participant{{Key: brokerKey, ServiceID: "broker"}, {Key: agentKey, ServiceID: "agent"}}})
	if err != nil {
		t.Fatal(err)
	}
	brokerKey.DomainID, agentKey.DomainID = domain, domain
	s, _, _ := supportFixture()
	decoded, err := hexDomain(domain)
	if err != nil {
		t.Fatal(err)
	}
	s.Domain = decoded
	d := infraDeadline(t)
	image, err := upgradelock.Acquire(t.Context(), agentKey, "")
	if err != nil {
		t.Fatal(err)
	}
	if _, lease, err := AcquireSupport(t.Context(), brokerKey, s, s.Previous, false, d); err == nil || lease != nil {
		t.Fatal("support mutation overlapped a sibling image cut")
	}
	if err := image.Close(); err != nil {
		t.Fatal(err)
	}
	next, support, err := AcquireSupport(t.Context(), brokerKey, s, s.Previous, false, d)
	if err != nil || support == nil || next.Pending != s.Previous {
		t.Fatalf("support reservation: %+v %v", next, err)
	}
	t.Cleanup(func() { _ = support.Close() })
	if contender, err := upgradelock.Acquire(t.Context(), agentKey, ""); err == nil {
		_ = contender.Close()
		t.Fatal("image cut overlapped a support mutation")
	}
	if err := support.Close(); err != nil {
		t.Fatal(err)
	}
	wrong := s
	wrong.Domain = caddyID(91)
	if _, lease, err := AcquireSupport(t.Context(), brokerKey, wrong, s.Previous, false, d); err == nil || lease != nil {
		t.Fatal("support borrowed a different canonical domain")
	}
}

func hexDomain(s string) (c.ID, error) {
	key := upgradetx.Key{DomainID: s, Component: upgradetx.ComponentNATS, ConfiguredNID: "node", ConfigPath: "/test/config"}
	return transactionDomain(key)
}

func exerciseTransactionPersistence(t *testing.T) {
	t.Helper()
	dir := t.TempDir()
	if err := os.Chmod(dir, 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "canonical"), []byte("test artifact"), 0o600); err != nil {
		t.Fatal(err)
	}
	key := upgradetx.Key{Component: upgradetx.ComponentBroker, ConfiguredNID: "node", ConfigPath: filepath.Join(dir, "broker.conf")}
	domain, err := upgradelock.RegisterDomain(upgradelock.Registration{CanonicalPath: filepath.Join(dir, "canonical"), ControlDir: dir, DeploymentID: "durability",
		Participants: []upgradelock.Participant{{Key: key, ServiceID: "broker"}}})
	if err != nil {
		t.Fatal(err)
	}
	key.DomainID = domain
	owner := upgradetx.Owner{BootID: "test-boot", PID: 100, StartTicks: 100, InstanceID: "controller"}
	store, err := upgradestore.Open(upgradestore.Options{Path: filepath.Join(dir, "store.json"), GuardPath: filepath.Join(dir, domain+".guard"), Key: key, Owner: owner, Transition: TransactionTransition})
	if err != nil {
		t.Fatal(err)
	}
	scope := infraScope(key, owner)
	grant, err := store.Prepare(key, upgradetx.PrepareRequest{ClientNonce: hexID(caddyID(61)), ActorFP: "actor", Mode: upgradetx.ModeHot, Scope: scope,
		SourceRelease: "source", TargetRelease: "target", SourceSHA: digestText(caddyDigest(2)), TargetSHA: digestText(caddyDigest(4)), CapabilityDigest: digestText(caddyDigest(30))})
	if err != nil {
		t.Fatal(err)
	}
	record, err := store.Start(grant, upgradetx.Fence{Owner: owner, RunningSHA: grant.SourceSHA})
	if err != nil {
		t.Fatal(err)
	}
	// A real broker-key S8 store tests the generic phase/persistence interface.
	// It is not passed to NewTransaction or advertised as an infra registration.
	p := infraPlan(c.NATS)
	p.Domain, err = hexDomain(domain)
	if err != nil {
		t.Fatal(err)
	}
	for phase := c.Staged; phase <= c.Completed; phase++ {
		for {
			a := transactionAttempt(record)
			proof, err := (infraEvidenceFixture{scope}).Read(p, a, phase, caddyDigest(byte(phase+40)), infraDeadline(t))
			if err != nil {
				t.Fatal(err)
			}
			event, err := transactionEvent(a, phase, proof)
			if err != nil {
				t.Fatal(err)
			}
			record, err = store.Apply(a.Handle, event)
			if err != nil {
				t.Fatalf("generic S8 phase %v: %v", phase, err)
			}
			if phase != c.Cutting || transactionAttempt(record).Phase == upgradetx.PhaseActivating {
				break
			}
		}
	}
	x, _ := (infraEvidenceFixture{scope}).Read(p, transactionAttempt(record), c.Completed, caddyDigest(50), infraDeadline(t))
	if err := upgradetx.Completed(transactionAttempt(record), x.Observation); err != nil || record.EffectiveSHA != grant.TargetSHA {
		t.Fatalf("three S8 completion points or selector mapping failed: %v", err)
	}
	journal := FileTransactionJournal{Key: key, Directory: dir}
	state := TransactionState{Version: transactionStateVersion, Revision: 1, Plan: p, Deadline: infraDeadline(t), Grant: grant}
	if err := journal.CompareAndSwap(0, state, infraDeadline(t)); err != nil {
		t.Fatalf("publish private sidecar: %v", err)
	}
	loaded, err := journal.Load(p.Tx, infraDeadline(t))
	if err != nil || loaded.Plan != p || loaded.Grant != grant {
		t.Fatalf("durable plan binding changed: %+v %v", loaded, err)
	}
	if err := journal.CompareAndSwap(0, state, infraDeadline(t)); err == nil {
		t.Fatal("old sidecar CAS overwrote a durable plan")
	}
	next := state
	next.Revision = 2
	if err := journal.CompareAndSwap(1, next, infraDeadline(t)); err != nil {
		t.Fatal(err)
	}
	if err := journal.CompareAndSwap(1, next, infraDeadline(t)); err == nil {
		t.Fatal("stale sidecar revision rewrote a durable result")
	}
	path, err := journal.path(p.Tx)
	if err != nil {
		t.Fatal(err)
	}
	b, err := json.Marshal(state)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.Chmod(path, 0o644); err != nil {
		t.Fatal(err)
	}
	if _, err := journal.Load(p.Tx, infraDeadline(t)); !errors.Is(err, c.ErrIncomplete) {
		t.Fatalf("public sidecar permissions accepted: %v", err)
	}
	if err := os.Chmod(path, 0o600); err != nil {
		t.Fatal(err)
	}
	target := path + ".target"
	if err := os.Rename(path, target); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(target, path); err != nil {
		t.Fatal(err)
	}
	if _, err := journal.Load(p.Tx, infraDeadline(t)); err == nil {
		t.Fatal("mutable symlink used as a sidecar")
	}
	if err := os.Remove(path); err != nil {
		t.Fatal(err)
	}
	if err := os.Rename(target, path); err != nil {
		t.Fatal(err)
	}
	wrong := state
	wrong.Grant.Handle.Key.DomainID = hexID(caddyID(99))
	wrongBytes, err := json.Marshal(wrong)
	if err != nil {
		t.Fatal(err)
	}
	for _, corrupt := range [][]byte{append([]byte("{\"Version\":99,"), b[1:]...), append(append([]byte(nil), b...), []byte("{}")...), wrongBytes, []byte("{}"), []byte("{\"Unknown\":1}")} {
		if err := os.WriteFile(path, corrupt, 0o600); err != nil {
			t.Fatal(err)
		}
		if _, err := journal.Load(p.Tx, infraDeadline(t)); !errors.Is(err, c.ErrIncomplete) {
			t.Fatalf("corrupt sidecar treated as idle: %v", err)
		}
	}
}
