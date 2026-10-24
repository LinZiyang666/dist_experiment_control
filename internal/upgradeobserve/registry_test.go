package upgradeobserve

import (
	"errors"
	"fmt"
	"math"
	"reflect"
	"runtime"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/LinZiyang666/tether/internal/testharness"
	"github.com/LinZiyang666/tether/internal/upgradetx"
)

// The fixture implements the S8-side observation ports, not any pending S7
// adapter. Resources live independently of the registry's admission records;
// missing Begin, omissions and false zero snapshots are observable defects.
type registryFixture struct {
	mu        sync.Mutex
	attempt   upgradetx.Attempt
	producer  upgradetx.Owner
	resources []upgradetx.Resource
	oldOwners []upgradetx.Owner
	edit      func(upgradetx.Domain, *upgradetx.Census)
}

func testDomains() []upgradetx.Domain {
	return []upgradetx.Domain{
		upgradetx.DomainControl, upgradetx.DomainExec, upgradetx.DomainPTY,
		upgradetx.DomainFlow, upgradetx.DomainSS, upgradetx.DomainTransferWorker,
		upgradetx.DomainTransferTracker, upgradetx.DomainBrokerStore,
		upgradetx.DomainBrokerRuntime, upgradetx.DomainAuthority, upgradetx.DomainUpgrade,
	}
}

func fixture() *registryFixture {
	key := upgradetx.Key{DomainID: "domain", Component: upgradetx.ComponentAgent,
		SID: "session", ConfiguredNID: "node", ConfigPath: "/sandbox/agent.yaml"}
	old := upgradetx.Owner{BootID: "boot", PID: 42, StartTicks: 7, InstanceID: strings.Repeat("1", 32)}
	producer := old
	producer.InstanceID = strings.Repeat("2", 32)
	resource := upgradetx.Resource{Domain: upgradetx.DomainExec, ID: "original-process", Origin: "pid-42-start-7",
		Owner: old, Generation: 1, State: "active", Custody: "runtime"}
	attempt := upgradetx.Attempt{
		Handle:    upgradetx.Handle{Key: key, TxID: strings.Repeat("3", 32), CutID: strings.Repeat("4", 32), Generation: 2, Revision: 1},
		Direction: upgradetx.DirectionForward, SourceSHA: strings.Repeat("a", 64), TargetSHA: strings.Repeat("b", 64),
		SourceGeneration: 1, Phase: upgradetx.PhaseMigrating, CutStage: upgradetx.CutActive, Outcome: upgradetx.OutcomePending,
		Scope: upgradetx.Scope{ID: strings.Repeat("5", 32), Participants: []upgradetx.Key{key},
			Domains: testDomains(), Expected: []upgradetx.Resource{resource}},
	}
	resource.Owner, resource.Generation = producer, 2
	return &registryFixture{attempt: attempt, producer: producer, resources: []upgradetx.Resource{resource}}
}

func (f *registryFixture) config() Config {
	config := Config{Attempt: f.attempt, Producer: f.producer, Probe: f.probe}
	for _, domain := range testDomains() {
		config.Observers = append(config.Observers, DomainObserver{Domain: domain, CapabilityDigest: strings.Repeat("c", 64),
			Observe: func(stamp upgradetx.Stamp) (upgradetx.Census, error) { return f.observe(domain, stamp) }})
	}
	return config
}

func (f *registryFixture) observe(domain upgradetx.Domain, stamp upgradetx.Stamp) (upgradetx.Census, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	expected, err := ExpectedDigest(f.attempt.Scope)
	if err != nil {
		return upgradetx.Census{}, err
	}
	census := upgradetx.Census{Stamp: stamp, Known: true, Complete: true, ExpectedDigest: expected}
	for _, resource := range f.resources {
		if resource.Domain == domain {
			census.Resources = append(census.Resources, resource)
		}
	}
	if domain == upgradetx.DomainUpgrade {
		census.OldOwners = append([]upgradetx.Owner(nil), f.oldOwners...)
	}
	if f.edit != nil {
		f.edit(domain, &census)
	}
	return census, nil
}

func (f *registryFixture) probe(stamp upgradetx.Stamp) (upgradetx.Observation, error) {
	return upgradetx.Observation{Nonce: stamp.Nonce, At: stamp.At, Current: f.attempt,
		Census: upgradetx.Census{Stamp: stamp}, OwnerKnown: true,
		RunningSHA: f.attempt.TargetSHA, EffectiveSHA: f.attempt.TargetSHA}, nil
}

func newRegistry(t *testing.T, f *registryFixture) Registry {
	t.Helper()
	r, err := New(f.config())
	if err != nil {
		t.Fatal(err)
	}
	return r
}

func stampFor(t *testing.T, f *registryFixture, sequence uint64) upgradetx.Stamp {
	t.Helper()
	digest, err := ScopeDigest(f.attempt.Scope)
	if err != nil {
		t.Fatal(err)
	}
	return upgradetx.Stamp{Attempt: f.attempt.Handle, Producer: f.producer, Nonce: strings.Repeat("6", 32),
		Sequence: sequence, At: time.Now().UnixNano(), ScopeDigest: digest}
}

func requireRefusal(t *testing.T, err error) {
	t.Helper()
	var refusal *upgradetx.Error
	if !errors.As(err, &refusal) || refusal.Accepted ||
		(refusal.Code != upgradetx.CodeUnsettled && refusal.Code != upgradetx.CodeRejected) {
		t.Fatalf("expected classified fail-closed refusal, got %v", err)
	}
}

// origin: hot-update S8 plan §2.1 / §4.2; S9 plan §8 / T44
func TestInfrastructureRegistryKeepsComponentIdentity(t *testing.T) {
	g, fd := runtime.NumGoroutine(), testharness.FDCount()
	for round := 0; round < 5; round++ {
		exerciseInfrastructureRegistry(t)
	}
	testharness.AssertNoGoroutineLeak(t, "infrastructure registry", g)
	testharness.AssertNoFDLeak(t, "infrastructure registry", fd, 0)
}

func exerciseInfrastructureRegistry(t *testing.T) {
	t.Helper()
	for _, component := range []upgradetx.Component{upgradetx.ComponentNATS, upgradetx.ComponentCaddy} {
		f := fixture()
		key := f.attempt.Handle.Key
		key.Component, key.SID = component, ""
		f.attempt.Handle.Key, f.attempt.Scope.Participants[0] = key, key
		r := newRegistry(t, f)
		receipt, err := r.Seal(f.attempt, strings.Repeat("6", 32))
		if err != nil || receipt.Kind != upgradetx.ReceiptResourcesMigrated || receipt.Stamp.Attempt.Key != key {
			t.Fatalf("infrastructure seal failed: %v", err)
		}
		observation := freshObservation(t, f)
		for _, proof := range []func(upgradetx.Attempt, upgradetx.Observation, string) (upgradetx.Receipt, error){ControlProof, ResourcesProof, ReleasedProof} {
			if _, err := proof(f.attempt, observation, observation.Nonce); err != nil {
				t.Fatal(err)
			}
			for _, foreign := range []upgradetx.Component{upgradetx.ComponentBroker, upgradetx.ComponentNATS, upgradetx.ComponentCaddy, "unknown"} {
				if foreign == component {
					continue
				}
				wrong := observation
				wrong.Current.Handle.Key.Component = foreign
				_, err := proof(f.attempt, wrong, observation.Nonce)
				requireRefusal(t, err)
			}
		}
		foreign := f.attempt
		foreign.Handle.Key.Component = upgradetx.ComponentBroker
		_, err = r.Seal(foreign, strings.Repeat("7", 32))
		requireRefusal(t, err)
		for _, defect := range []string{"unknown", "sid", "node", "long_node", "relative", "unclean", "long_path", "path_control"} {
			wrong := key
			switch defect {
			case "unknown":
				wrong.Component = "unknown"
			case "sid":
				wrong.SID = "agent-session"
			case "node":
				wrong.ConfiguredNID = ""
			case "long_node":
				wrong.ConfiguredNID = strings.Repeat("n", 257)
			case "relative":
				wrong.ConfigPath = "config"
			case "unclean":
				wrong.ConfigPath = "/managed/../config"
			case "long_path":
				wrong.ConfigPath = "/" + strings.Repeat("p", 4096)
			case "path_control":
				wrong.ConfigPath = "/managed/config\n"
			}
			if validKey(wrong) {
				t.Fatalf("%s: invalid infrastructure key accepted", defect)
			}
			config := f.config()
			config.Attempt.Handle.Key = wrong
			config.Attempt.Scope.Participants = []upgradetx.Key{wrong}
			_, err = New(config)
			requireRefusal(t, err)
		}
	}
}

// origin: hot-update S8 plan §4.2 / P03 admission sequencer
func TestRegistryAdmissionSequencer(t *testing.T) {
	before, fdBefore := runtime.NumGoroutine(), testharness.FDCount()
	for round := 0; round < 5; round++ {
		exerciseAdmissionSequencer(t)
	}
	testharness.AssertNoGoroutineLeak(t, "registry admissions", before)
	testharness.AssertNoFDLeak(t, "registry admissions", fdBefore, 0)
}

func exerciseAdmissionSequencer(t *testing.T) {
	t.Helper()
	f := fixture()
	r := newRegistry(t, f)
	const count = 24
	base := f.resources[0]
	var wg sync.WaitGroup
	sequences := make(chan uint64, count)
	for i := 0; i < count; i++ {
		wg.Go(func() {
			resource := base
			resource.ID, resource.Origin = fmt.Sprintf("new-%d", i), fmt.Sprintf("origin-%d", i)
			sequence, err := r.Begin(resource)
			if err != nil {
				t.Error(err)
				return
			}
			sequences <- sequence
			f.mu.Lock()
			f.resources = append(f.resources, resource)
			f.mu.Unlock()
			wrong := resource
			wrong.Owner.StartTicks++
			requireRefusal(t, r.Finish(sequence, wrong))
			if err := r.Finish(sequence, resource); err != nil {
				t.Error(err)
			}
			if err := r.Finish(sequence, resource); err != nil {
				t.Error(err)
			}
		})
	}
	wg.Wait()
	close(sequences)
	seen := make(map[uint64]bool)
	for sequence := range sequences {
		if sequence == 0 || seen[sequence] {
			t.Fatalf("non-unique admission sequence %d", sequence)
		}
		seen[sequence] = true
	}
	if len(seen) != count {
		t.Fatalf("registered %d admissions, want %d", len(seen), count)
	}
	receipt, err := r.Seal(f.attempt, strings.Repeat("7", 32))
	if err != nil {
		t.Fatal(err)
	}
	if receipt.Stamp.Sequence != count+1 || receipt.Census.Admitting != 0 || len(receipt.Census.Resources) != count+1 {
		t.Fatalf("admission barrier lost work: %+v", receipt.Census)
	}
	if err := Commit(r, receipt.Stamp, func(upgradetx.Census) error { return nil }); err != nil {
		t.Fatal(err)
	}
	requireRefusal(t, r.Finish(count+2, f.resources[0]))
	requireRefusal(t, r.Finish(0, f.resources[0]))
	_, err = r.Begin(f.resources[0])
	if err != nil {
		t.Fatal(err)
	} // An initial original has not been separately admitted.
	_, err = r.Begin(f.resources[0])
	requireRefusal(t, err)
}

// origin: hot-update S8 plan §4.2 / M04 / M06
func TestRegistrySealIncludesLateAdmission(t *testing.T) {
	f := fixture()
	r := newRegistry(t, f)
	late := f.resources[0]
	late.Domain, late.ID, late.Origin = upgradetx.DomainTransferWorker, "late-prepare", "original-grant"
	sequence, err := r.Begin(late)
	if err != nil {
		t.Fatal(err)
	}
	_, err = r.Seal(f.attempt, strings.Repeat("7", 32))
	requireRefusal(t, err)
	if err := r.Finish(sequence, late); err != nil {
		t.Fatal(err)
	}
	_, err = r.Seal(f.attempt, strings.Repeat("7", 32))
	requireRefusal(t, err) // Finish alone cannot substitute for observed custody.
	f.resources = append(f.resources, late)
	receipt, err := r.Seal(f.attempt, strings.Repeat("8", 32))
	if err != nil {
		t.Fatal(err)
	}
	if receipt.Stamp.Sequence != sequence || len(receipt.Census.Resources) != 2 || receipt.Census.Resources[1] != late {
		t.Fatalf("late admission absent from seal: %+v", receipt.Census)
	}
	f.resources = f.resources[:1]
	_, err = r.Snapshot(stampFor(t, f, sequence))
	requireRefusal(t, err) // A later observer cannot forget a finished admission.
}

// origin: hot-update S8 plan §4.2 / P03 admitting=0 barrier
func TestRegistryPendingAdmissionCannotComplete(t *testing.T) {
	f := fixture()
	r := newRegistry(t, f)
	late := f.resources[0]
	late.ID = "effect-awaiting-finish"
	sequence, err := r.Begin(late)
	if err != nil {
		t.Fatal(err)
	}
	f.resources = append(f.resources, late)
	census, err := r.Snapshot(stampFor(t, f, sequence))
	if err != nil || census.Admitting != 1 {
		t.Fatalf("snapshot concealed in-flight admission: %+v %v", census, err)
	}
	_, err = r.Seal(f.attempt, strings.Repeat("7", 32))
	requireRefusal(t, err)
	if err := r.Finish(sequence, late); err != nil {
		t.Fatal(err)
	}
	if _, err := r.Seal(f.attempt, strings.Repeat("8", 32)); err != nil {
		t.Fatal(err)
	}
}

// origin: hot-update S8 plan §4.2 / M04
func TestRegistryRejectsUnregisteredEffects(t *testing.T) {
	f := fixture()
	r := newRegistry(t, f)
	rogue := f.resources[0]
	rogue.ID, rogue.Origin = "effect-without-begin", "rogue-worker"
	f.resources = append(f.resources, rogue)
	_, err := r.Seal(f.attempt, strings.Repeat("7", 32))
	requireRefusal(t, err)
	seq, err := r.Begin(rogue)
	if err != nil {
		t.Fatal(err)
	}
	if err := r.Finish(seq, rogue); err != nil {
		t.Fatal(err)
	}
	if _, err := r.Seal(f.attempt, strings.Repeat("8", 32)); err != nil {
		t.Fatal(err)
	}
}

// origin: hot-update S8 plan §2.1 / §4.2 / M06
func TestRegistryCensusRequiresEveryDomain(t *testing.T) {
	for _, domain := range testDomains() {
		t.Run(string(domain), func(t *testing.T) {
			f := fixture()
			config := f.config()
			for i, binding := range config.Observers {
				if binding.Domain == domain {
					config.Observers = append(config.Observers[:i], config.Observers[i+1:]...)
					break
				}
			}
			_, err := New(config)
			requireRefusal(t, err)
			for _, field := range []string{"known", "complete"} {
				f := fixture()
				f.edit = func(current upgradetx.Domain, census *upgradetx.Census) {
					if current == domain {
						if field == "known" {
							census.Known = false
						} else {
							census.Complete = false
						}
						census.Resources = nil
					}
				}
				_, err := newRegistry(t, f).Seal(f.attempt, strings.Repeat("7", 32))
				requireRefusal(t, err)
			}
		})
	}
	// Genuine idle is positive only after every applicable owner was observed.
	f := fixture()
	f.attempt.Scope.Expected, f.resources = nil, nil
	receipt, err := newRegistry(t, f).Seal(f.attempt, strings.Repeat("7", 32))
	if err != nil || !receipt.Known || !receipt.Complete || len(receipt.Census.Resources) != 0 {
		t.Fatalf("known idle rejected: %+v %v", receipt, err)
	}
}

// origin: hot-update S8 plan §2.1 / §4.2 / P03 explicit not_applicable
func TestRegistryRequiresCapabilityForExcludedDomain(t *testing.T) {
	f := fixture()
	config := f.config()
	excludeIdleDomains(&config, upgradetx.DomainExec)
	for i := range config.Observers {
		if config.Observers[i].Domain != upgradetx.DomainExec {
			config.Observers[i].NotApplicable, config.Observers[i].Observe = true, nil
		}
	}
	if _, err := New(config); err != nil {
		t.Fatal(err)
	}
	for _, defect := range []string{"capability", "callback", "scope", "duplicate", "unknown"} {
		t.Run(defect, func(t *testing.T) {
			bad := config
			bad.Observers = append([]DomainObserver(nil), config.Observers...)
			switch defect {
			case "capability":
				bad.Observers[0].CapabilityDigest = ""
			case "callback":
				bad.Observers[0].Observe = f.config().Observers[0].Observe
			case "scope":
				bad.Attempt.Scope.Domains = append([]upgradetx.Domain{upgradetx.DomainControl}, bad.Attempt.Scope.Domains...)
			case "duplicate":
				bad.Observers = append(bad.Observers, bad.Observers[0])
			case "unknown":
				bad.Observers[0].Domain = "unknown"
			}
			_, err := New(bad)
			requireRefusal(t, err)
		})
	}
}

// origin: hot-update S8 plan §4.2 / M06 / M15
func TestRegistryRejectsInvalidDomainEvidence(t *testing.T) {
	cases := map[string]func(*upgradetx.Census){
		"missing original":       func(c *upgradetx.Census) { c.Resources = nil },
		"substituted ID":         func(c *upgradetx.Census) { c.Resources[0].ID = "different" },
		"substituted origin":     func(c *upgradetx.Census) { c.Resources[0].Origin = "different" },
		"duplicate":              func(c *upgradetx.Census) { c.Resources = append(c.Resources, c.Resources[0]) },
		"wrong domain":           func(c *upgradetx.Census) { c.Resources[0].Domain = upgradetx.DomainPTY },
		"unknown owner":          func(c *upgradetx.Census) { c.Resources[0].Owner.StartTicks = 0 },
		"unknown retained owner": func(c *upgradetx.Census) { c.OldOwners = []upgradetx.Owner{{PID: 42}} },
		"stale nonce":            func(c *upgradetx.Census) { c.Stamp.Nonce = strings.Repeat("f", 32) },
		"stale sequence":         func(c *upgradetx.Census) { c.Stamp.Sequence++ },
		"stale scope":            func(c *upgradetx.Census) { c.ExpectedDigest = strings.Repeat("f", 64) },
		"admitting":              func(c *upgradetx.Census) { c.Admitting = 1 },
	}
	for name, edit := range cases {
		t.Run(name, func(t *testing.T) {
			f := fixture()
			f.edit = func(domain upgradetx.Domain, c *upgradetx.Census) {
				if domain == upgradetx.DomainExec {
					edit(c)
				}
			}
			_, err := newRegistry(t, f).Seal(f.attempt, strings.Repeat("7", 32))
			requireRefusal(t, err)
		})
	}
	// Keep the expected identity present, but let the wrong domain produce it.
	// Otherwise the original-set check masks a missing domain-owner predicate.
	f := fixture()
	f.edit = func(domain upgradetx.Domain, c *upgradetx.Census) {
		if domain == upgradetx.DomainExec {
			c.Resources = nil
		}
		if domain == upgradetx.DomainPTY {
			c.Resources = append([]upgradetx.Resource(nil), f.resources...)
		}
	}
	_, err := newRegistry(t, f).Snapshot(stampFor(t, f, 1))
	requireRefusal(t, err)
	f = fixture()
	config := f.config()
	config.Observers[0].Observe = func(upgradetx.Stamp) (upgradetx.Census, error) {
		return upgradetx.Census{}, errors.New("private-secret")
	}
	_, err = mustNew(t, config).Seal(f.attempt, strings.Repeat("7", 32))
	requireRefusal(t, err)
	if strings.Contains(err.Error(), "private-secret") {
		t.Fatal("domain error disclosed private detail")
	}
}

// origin: hot-update S8 plan §4.2 / M06 / M14 exact original responsibility IDs
func TestRegistryCensusRequiresEveryExpectedIdentity(t *testing.T) {
	// Include the new transfer responsibilities by original IDs, independently
	// of worker counts. Each omitted ID must fail even if the rest migrated.
	ids := []string{"process", "master-and-child", "stream-FIN", "salt-FIFO", "prepare-grant", "pending-terminal", "result", "GC-permit", "store", "listener", "transport", "aux-lock", "timer", "FD-duplicate", "reaper", "courier", "callback"}
	for _, id := range ids {
		t.Run(id, func(t *testing.T) {
			f := fixture()
			original := f.attempt.Scope.Expected[0]
			original.Domain, original.ID, original.Origin = upgradetx.DomainUpgrade, id, "origin-"+id
			if id == "prepare-grant" || id == "pending-terminal" {
				original.Domain = upgradetx.DomainTransferWorker
			}
			if id == "result" || id == "GC-permit" {
				original.Domain = upgradetx.DomainTransferTracker
			}
			f.attempt.Scope.Expected = append(f.attempt.Scope.Expected, original)
			current := original
			current.Owner, current.Generation = f.producer, f.attempt.Handle.Generation
			f.resources = append(f.resources, current)
			r := newRegistry(t, f)
			if _, err := r.Snapshot(stampFor(t, f, 1)); err != nil {
				t.Fatal(err)
			}
			f.resources = f.resources[:1]
			_, err := r.Snapshot(stampFor(t, f, 1))
			requireRefusal(t, err)
		})
	}
}

func mustNew(t *testing.T, config Config) Registry {
	t.Helper()
	r, err := New(config)
	if err != nil {
		t.Fatal(err)
	}
	return r
}

// origin: hot-update S8 plan §4.2 / P03 scope immutability
func TestRegistryPreservesOriginalScope(t *testing.T) {
	f := fixture()
	r := newRegistry(t, f)
	f.attempt.Scope.Expected[0].ID = "rewritten"
	_, err := r.Seal(f.attempt, strings.Repeat("7", 32))
	requireRefusal(t, err)
	_, err = r.Snapshot(stampFor(t, f, 1))
	requireRefusal(t, err)
	// Mutation of an input or returned census cannot mutate the registry.
	f.attempt.Scope.Expected[0].ID = "original-process"
	stamp := stampFor(t, f, 1)
	census, err := r.Snapshot(stamp)
	if err != nil {
		t.Fatal(err)
	}
	census.Resources[0].ID = "mutated-return"
	again, err := r.Snapshot(stamp)
	if err != nil || again.Resources[0].ID != "original-process" {
		t.Fatalf("snapshot aliases mutable evidence: %+v %v", again, err)
	}
	for _, edit := range []func(*upgradetx.Stamp){
		func(s *upgradetx.Stamp) { s.Attempt.Revision = 0 }, func(s *upgradetx.Stamp) { s.Producer.PID++ },
		func(s *upgradetx.Stamp) { s.Sequence++ }, func(s *upgradetx.Stamp) { s.Nonce = "" },
		func(s *upgradetx.Stamp) { s.At = 0 }, func(s *upgradetx.Stamp) { s.ScopeDigest = "" },
	} {
		bad := stamp
		edit(&bad)
		_, err := r.Snapshot(bad)
		requireRefusal(t, err)
	}
	if !reflect.DeepEqual(f.resources[0], again.Resources[0]) {
		t.Fatal("original observation was modified")
	}
}

// origin: hot-update S8 plan §4.2 / M04 completion and Begin share one gate
func TestRegistrySealSerializesAdmission(t *testing.T) {
	before, fdBefore := runtime.NumGoroutine(), testharness.FDCount()
	for round := 0; round < 5; round++ {
		exerciseSealAdmission(t)
	}
	testharness.AssertNoGoroutineLeak(t, "registry seal gate", before)
	testharness.AssertNoFDLeak(t, "registry seal gate", fdBefore, 0)
}

func exerciseSealAdmission(t *testing.T) {
	t.Helper()
	f := fixture()
	config := f.config()
	entered, release := make(chan struct{}), make(chan struct{})
	var releaseOnce, enteredOnce sync.Once
	unblock := func() { releaseOnce.Do(func() { close(release) }) }
	defer unblock()
	original := config.Observers[0].Observe
	config.Observers[0].Observe = func(stamp upgradetx.Stamp) (upgradetx.Census, error) {
		enteredOnce.Do(func() { close(entered) })
		<-release
		return original(stamp)
	}
	r := mustNew(t, config)
	var joined sync.WaitGroup
	defer func() { unblock(); joined.Wait() }()
	sealDone := make(chan error, 1)
	joined.Go(func() { _, err := r.Seal(f.attempt, strings.Repeat("7", 32)); sealDone <- err })
	select {
	case <-entered:
	case <-time.After(2 * time.Second):
		t.Fatal("seal did not reach observer")
	}
	old := f.attempt.Scope.Expected[0]
	old.ID = "racing-old-effect"
	beginDone := make(chan error, 1)
	joined.Go(func() { _, err := r.Begin(old); beginDone <- err })
	select {
	case err := <-beginDone:
		t.Fatalf("Begin escaped the held completion barrier: %v", err)
	case <-time.After(20 * time.Millisecond):
	}
	unblock()
	select {
	case err := <-sealDone:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("seal did not join")
	}
	select {
	case err := <-beginDone:
		requireRefusal(t, err)
	case <-time.After(2 * time.Second):
		t.Fatal("admission did not join")
	}
	if err := Commit(r, stampFor(t, f, 1), func(upgradetx.Census) error { return nil }); err != nil {
		t.Fatal(err)
	}
	newResource := f.resources[0]
	newResource.ID = "post-barrier-new-effect"
	sequence, err := r.Begin(newResource)
	if err != nil || sequence != 2 {
		t.Fatalf("new generation admission failed: %d %v", sequence, err)
	}
	f.resources = append(f.resources, newResource)
	if err := r.Finish(sequence, newResource); err != nil {
		t.Fatal(err)
	}
}

// origin: hot-update S8 plan §2.1 / §4.2 / P03 admission identity guards
func TestRegistryAdmissionRejectsInvalidIdentity(t *testing.T) {
	for _, defect := range []string{"domain", "ID", "origin", "owner", "generation", "state", "custody", "oversized", "NUL", "unobserved", "sequence-overflow"} {
		t.Run(defect, func(t *testing.T) {
			f := fixture()
			config := f.config()
			resource := f.resources[0]
			resource.ID = "invalid-admission"
			switch defect {
			case "domain":
				resource.Domain = "unknown"
			case "ID":
				resource.ID = ""
			case "origin":
				resource.Origin = ""
			case "owner":
				resource.Owner.StartTicks = 0
			case "generation":
				resource.Generation = 3
			case "state":
				resource.State = ""
			case "custody":
				resource.Custody = ""
			case "oversized":
				resource.ID = strings.Repeat("x", 4097)
			case "NUL":
				resource.ID = "resource\x00id"
			case "unobserved":
				excludeIdleDomains(&config, upgradetx.DomainExec)
				resource.Domain = upgradetx.DomainPTY
				for i := range config.Observers {
					if config.Observers[i].Domain != upgradetx.DomainExec {
						config.Observers[i].NotApplicable, config.Observers[i].Observe = true, nil
					}
				}
			}
			r := mustNew(t, config)
			if defect == "sequence-overflow" {
				r.(*sequencer).sequence = math.MaxUint64
			}
			seq, err := r.Begin(resource)
			requireRefusal(t, err)
			if seq != 0 {
				t.Fatalf("refused admission allocated sequence %d", seq)
			}
		})
	}
}

func excludeIdleDomains(config *Config, applicable upgradetx.Domain) {
	for _, d := range config.Attempt.Scope.Domains {
		if d != applicable {
			config.Attempt.Scope.Expected = append(config.Attempt.Scope.Expected, upgradetx.Resource{Domain: d, ID: "inapplicable", Origin: "capability", State: "not_applicable", Custody: "not_applicable"})
		}
	}
}
