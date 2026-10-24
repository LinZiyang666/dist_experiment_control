package upgradeobserve

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"reflect"
	"strings"
	"testing"
)

import "github.com/LinZiyang666/tether/internal/upgradetx"

func freshObservation(t *testing.T, f *registryFixture) upgradetx.Observation {
	t.Helper()
	stamp := stampFor(t, f, 1)
	census, err := newRegistry(t, f).Snapshot(stamp)
	if err != nil {
		t.Fatal(err)
	}
	observation, err := f.probe(stamp)
	if err != nil {
		t.Fatal(err)
	}
	observation.Census = census
	observation.Current.Control = upgradetx.Receipt{Kind: upgradetx.ReceiptControlReady, Stamp: stamp,
		Known: true, Complete: true, RunningSHA: observation.RunningSHA, EvidenceDigest: strings.Repeat("c", 64)}
	return observation
}

// origin: hot-update S8 plan §4.2 / M02 / M07 / M15
func TestCompletedRequiresRunningImageProof(t *testing.T) {
	cases := map[string]func(*upgradetx.Observation){
		"disk replacement without exec": func(o *upgradetx.Observation) { o.RunningSHA = o.Current.SourceSHA },
		"effective selector mismatch":   func(o *upgradetx.Observation) { o.EffectiveSHA = o.Current.SourceSHA },
		"unknown producer":              func(o *upgradetx.Observation) { o.OwnerKnown = false },
		"PID alone":                     func(o *upgradetx.Observation) { o.Census.Stamp.Producer.StartTicks = 0 },
		"missing boot identity":         func(o *upgradetx.Observation) { o.Census.Stamp.Producer.BootID = "" },
		"missing instance":              func(o *upgradetx.Observation) { o.Census.Stamp.Producer.InstanceID = "" },
		"stale response nonce":          func(o *upgradetx.Observation) { o.Nonce = strings.Repeat("f", 32) },
		"stale census nonce":            func(o *upgradetx.Observation) { o.Census.Stamp.Nonce = strings.Repeat("f", 32) },
		"time mismatch":                 func(o *upgradetx.Observation) { o.At-- },
		"missing barrier":               func(o *upgradetx.Observation) { o.Census.Stamp.Sequence = 0 },
		"another Tx":                    func(o *upgradetx.Observation) { o.Current.Handle.TxID = strings.Repeat("f", 32) },
		"another cut":                   func(o *upgradetx.Observation) { o.Current.Handle.CutID = strings.Repeat("f", 32) },
		"another generation":            func(o *upgradetx.Observation) { o.Current.Handle.Generation++ },
		"another revision":              func(o *upgradetx.Observation) { o.Current.Handle.Revision++ },
		"another direction":             func(o *upgradetx.Observation) { o.Current.Direction = upgradetx.DirectionReverse },
		"another source SHA":            func(o *upgradetx.Observation) { o.Current.SourceSHA = strings.Repeat("f", 64) },
		"another target SHA":            func(o *upgradetx.Observation) { o.Current.TargetSHA = strings.Repeat("f", 64) },
		"missing participant":           func(o *upgradetx.Observation) { o.Current.Scope.Participants = nil },
		"missing domain":                func(o *upgradetx.Observation) { o.Current.Scope.Domains = o.Current.Scope.Domains[:1] },
		"missing Expected ID":           func(o *upgradetx.Observation) { o.Census.Resources = nil },
		"substituted original":          func(o *upgradetx.Observation) { o.Census.Resources[0].Origin = "other" },
		"duplicate identity":            func(o *upgradetx.Observation) { o.Census.Resources = append(o.Census.Resources, o.Census.Resources[0]) },
		"wrong domain":                  func(o *upgradetx.Observation) { o.Census.Resources[0].Domain = "unknown" },
		"unknown census":                func(o *upgradetx.Observation) { o.Census.Known = false; o.Census.Resources = nil },
		"incomplete census":             func(o *upgradetx.Observation) { o.Census.Complete = false },
		"admission in progress":         func(o *upgradetx.Observation) { o.Census.Admitting = 1 },
		"wrong scope digest":            func(o *upgradetx.Observation) { o.Census.Stamp.ScopeDigest = strings.Repeat("f", 64) },
		"wrong expected digest":         func(o *upgradetx.Observation) { o.Census.ExpectedDigest = strings.Repeat("f", 64) },
	}
	for name, edit := range cases {
		t.Run(name, func(t *testing.T) {
			f := fixture()
			observation := freshObservation(t, f)
			edit(&observation)
			_, err := ResourcesProof(f.attempt, observation, strings.Repeat("6", 32))
			requireRefusal(t, err)
		})
	}
	f := fixture()
	observation := freshObservation(t, f)
	_, err := ResourcesProof(f.attempt, observation, "")
	requireRefusal(t, err)
	// A real observed empty set is the positive control. Unknown emptiness is
	// rejected by each independent proof; a missing Expected ID cannot mask it.
	f = fixture()
	f.attempt.Scope.Expected, f.resources = nil, nil
	observation = freshObservation(t, f)
	for _, proof := range []struct {
		name  string
		build func(upgradetx.Attempt, upgradetx.Observation, string) (upgradetx.Receipt, error)
	}{{"control", ControlProof}, {"resources", ResourcesProof}, {"released", ReleasedProof}} {
		t.Run("unknown-zero-"+proof.name, func(t *testing.T) {
			if _, err := proof.build(f.attempt, observation, observation.Nonce); err != nil {
				t.Fatal(err)
			}
			unknown := observation
			unknown.Census.Known = false
			_, err := proof.build(f.attempt, unknown, unknown.Nonce)
			requireRefusal(t, err)
		})
	}
}

// origin: hot-update S8 plan §4.2 / M02 / M15 registry probe boundary
func TestRegistrySealRequiresFreshImageObservation(t *testing.T) {
	cases := map[string]func(*upgradetx.Observation){
		"stale running SHA": func(o *upgradetx.Observation) { o.RunningSHA = o.Current.SourceSHA },
		"missing producer":  func(o *upgradetx.Observation) { o.OwnerKnown = false },
		"stale stamp":       func(o *upgradetx.Observation) { o.Census.Stamp.Sequence++ },
		"stale image nonce": func(o *upgradetx.Observation) { o.Nonce = strings.Repeat("f", 32) },
		"stale producer":    func(o *upgradetx.Observation) { o.Census.Stamp.Producer.InstanceID = strings.Repeat("f", 32) },
	}
	for name, edit := range cases {
		t.Run(name, func(t *testing.T) {
			f := fixture()
			config := f.config()
			config.Probe = func(stamp upgradetx.Stamp) (upgradetx.Observation, error) {
				o, err := f.probe(stamp)
				edit(&o)
				return o, err
			}
			r := mustNew(t, config)
			_, err := r.Seal(f.attempt, strings.Repeat("7", 32))
			requireRefusal(t, err)
			old := f.attempt.Scope.Expected[0]
			old.ID = "retry-after-refusal"
			if _, err := r.Begin(old); err != nil {
				t.Fatalf("failed seal installed a completion fence: %v", err)
			}
		})
	}
	f := fixture()
	config := f.config()
	config.Probe = func(upgradetx.Stamp) (upgradetx.Observation, error) {
		return upgradetx.Observation{}, errors.New("private-secret")
	}
	_, err := mustNew(t, config).Seal(f.attempt, strings.Repeat("7", 32))
	requireRefusal(t, err)
	if strings.Contains(err.Error(), "private-secret") {
		t.Fatal("image error disclosed private detail")
	}
}

// origin: hot-update S8 plan §4.2 / M05 / M15
func TestControlProofRequiresIndependentReadiness(t *testing.T) {
	f := fixture()
	// Control is allowed before migration; it proves usable subscriptions and
	// nonce request, not ownership convergence or old generation retirement.
	f.resources[0] = f.attempt.Scope.Expected[0]
	o := freshObservation(t, f)
	control, err := ControlProof(f.attempt, o, o.Nonce)
	if err != nil || control.Kind != upgradetx.ReceiptControlReady {
		t.Fatalf("fresh control proof rejected: %+v %v", control, err)
	}
	_, err = ResourcesProof(f.attempt, o, o.Nonce)
	requireRefusal(t, err)
	cases := map[string]func(*upgradetx.Receipt){
		"registration only":   func(r *upgradetx.Receipt) { *r = upgradetx.Receipt{} },
		"unknown":             func(r *upgradetx.Receipt) { r.Known = false },
		"subscription failed": func(r *upgradetx.Receipt) { r.Complete = false },
		"different receipt":   func(r *upgradetx.Receipt) { r.Kind = upgradetx.ReceiptResourcesMigrated },
		"stale nonce":         func(r *upgradetx.Receipt) { r.Stamp.Nonce = strings.Repeat("f", 32) },
		"stale cut":           func(r *upgradetx.Receipt) { r.Stamp.Attempt.CutID = strings.Repeat("f", 32) },
		"stale producer":      func(r *upgradetx.Receipt) { r.Stamp.Producer.StartTicks++ },
		"stale image":         func(r *upgradetx.Receipt) { r.RunningSHA = strings.Repeat("f", 64) },
		"no request evidence": func(r *upgradetx.Receipt) { r.EvidenceDigest = "" },
	}
	for name, edit := range cases {
		t.Run(name, func(t *testing.T) {
			bad := o
			edit(&bad.Current.Control)
			_, err := ControlProof(f.attempt, bad, bad.Nonce)
			requireRefusal(t, err)
		})
	}
}

// origin: hot-update S8 plan §4.2 / M03 independent old-generation release
func TestReleasedProofRequiresEveryRetainedOwner(t *testing.T) {
	for _, id := range []string{"runtime", "worker", "timer", "reaper", "FD-keeper", "duplicate-FD", "grant-courier", "terminal", "GC-pin", "callback"} {
		t.Run(id, func(t *testing.T) {
			f := fixture()
			owner := f.attempt.Scope.Expected[0].Owner
			f.oldOwners = []upgradetx.Owner{owner}
			o := freshObservation(t, f)
			if _, err := ResourcesProof(f.attempt, o, o.Nonce); err != nil {
				t.Fatal(err)
			}
			_, err := ReleasedProof(f.attempt, o, o.Nonce)
			requireRefusal(t, err)
			f.oldOwners = nil
			o = freshObservation(t, f)
			released, err := ReleasedProof(f.attempt, o, o.Nonce)
			if err != nil || released.Kind != upgradetx.ReceiptOldGenerationReleased {
				t.Fatalf("settled release rejected: %+v %v", released, err)
			}
		})
	}
	// A terminal result can retain its original identity, but it must have
	// independently durable settlement and no execution/courier/GC custody.
	f := fixture()
	f.resources[0] = f.attempt.Scope.Expected[0]
	f.resources[0].State, f.resources[0].Custody = "terminal", "durable"
	o := freshObservation(t, f)
	if _, err := ReleasedProof(f.attempt, o, o.Nonce); err != nil {
		t.Fatal(err)
	}
	for _, custody := range []string{"pending-terminal", "courier", "GC-pin"} {
		o.Census.Resources[0].Custody = custody
		_, err := ResourcesProof(f.attempt, o, o.Nonce)
		requireRefusal(t, err)
	}
}

// origin: hot-update S8 plan §4.2 / M01 target ownership and generation
func TestResourcesProofRequiresTargetCustody(t *testing.T) {
	for _, defect := range []string{"old owner", "old generation", "third generation", "PID reuse", "boot reuse", "terminal with execution"} {
		t.Run(defect, func(t *testing.T) {
			f := fixture()
			o := freshObservation(t, f)
			r := &o.Census.Resources[0]
			switch defect {
			case "old owner":
				r.Owner = f.attempt.Scope.Expected[0].Owner
			case "old generation":
				r.Generation = 1
			case "third generation":
				r.Generation = 3
			case "PID reuse":
				r.Owner.StartTicks++
			case "boot reuse":
				r.Owner.BootID = "another-boot"
			case "terminal with execution":
				r.State, r.Custody, r.Owner = "terminal", "runtime", f.attempt.Scope.Expected[0].Owner
			}
			_, err := ResourcesProof(f.attempt, o, o.Nonce)
			requireRefusal(t, err)
		})
	}
}

// origin: hot-update S8 plan §2.1 / §4.2 / P03 canonical original scope
func TestScopeDigestBindsExactOriginalSet(t *testing.T) {
	f := fixture()
	first, err := ScopeDigest(f.attempt.Scope)
	if err != nil {
		t.Fatal(err)
	}
	scope := f.attempt.Scope
	scope.Domains = append([]upgradetx.Domain(nil), scope.Domains...)
	for i, j := 0, len(scope.Domains)-1; i < j; i, j = i+1, j-1 {
		scope.Domains[i], scope.Domains[j] = scope.Domains[j], scope.Domains[i]
	}
	second, err := ScopeDigest(scope)
	if err != nil || first != second {
		t.Fatalf("ordering changes scope identity: %s %s %v", first, second, err)
	}
	canonical := scope
	canonical.Domains = []upgradetx.Domain{"authority", "broker_runtime", "broker_store", "control", "exec", "flow", "pty", "ss", "transfer_tracker", "transfer_worker", "upgrade"}
	data, err := json.Marshal(canonical)
	if err != nil {
		t.Fatal(err)
	}
	sum := sha256.Sum256(data)
	if first != hex.EncodeToString(sum[:]) {
		t.Fatal("digest does not bind all original fields")
	}
	expected, err := ExpectedDigest(scope)
	if err != nil {
		t.Fatal(err)
	}
	data, err = json.Marshal(scope.Expected)
	if err != nil {
		t.Fatal(err)
	}
	sum = sha256.Sum256(data)
	if expected != hex.EncodeToString(sum[:]) {
		t.Fatal("Expected digest does not bind every original field")
	}
	for _, defect := range []string{"origin", "owner", "generation", "custody", "domain", "participant", "ID"} {
		t.Run(defect, func(t *testing.T) {
			f := fixture()
			scope := f.attempt.Scope
			switch defect {
			case "origin":
				scope.Expected[0].Origin = "another"
			case "owner":
				scope.Expected[0].Owner.StartTicks++
			case "generation":
				scope.Expected[0].Generation++
			case "custody":
				scope.Expected[0].Custody = "retained"
			case "domain":
				scope.Expected[0].Domain = upgradetx.DomainPTY
			case "participant":
				scope.Participants[0].ConfiguredNID = "another"
			case "ID":
				scope.ID = strings.Repeat("f", 32)
			}
			changed, err := ScopeDigest(scope)
			if err != nil || changed == first {
				t.Fatalf("digest ignores %s: %s %v", defect, changed, err)
			}
		})
	}
	for _, defect := range []string{"empty", "duplicate-domain", "duplicate-resource", "duplicate-participant", "unknown-domain", "invalid-resource", "invalid-key"} {
		t.Run(defect, func(t *testing.T) {
			f := fixture()
			scope := f.attempt.Scope
			switch defect {
			case "empty":
				scope.Domains = nil
			case "duplicate-domain":
				scope.Domains = append(scope.Domains, scope.Domains[0])
			case "duplicate-resource":
				scope.Expected = append(scope.Expected, scope.Expected[0])
			case "duplicate-participant":
				scope.Participants = append(scope.Participants, scope.Participants[0])
			case "unknown-domain":
				scope.Domains[0] = "unknown"
			case "invalid-resource":
				scope.Expected[0].Custody = ""
			case "invalid-key":
				scope.Participants[0].ConfiguredNID = ""
			}
			_, err := ScopeDigest(scope)
			requireRefusal(t, err)
		})
	}
}

// origin: hot-update S8 plan §2.1 / §4.2 / M07 independent receipt evidence
func TestProofReceiptsHaveIndependentDigestsAndCopies(t *testing.T) {
	f := fixture()
	o := freshObservation(t, f)
	control, err := ControlProof(f.attempt, o, o.Nonce)
	if err != nil {
		t.Fatal(err)
	}
	resources, err := ResourcesProof(f.attempt, o, o.Nonce)
	if err != nil {
		t.Fatal(err)
	}
	released, err := ReleasedProof(f.attempt, o, o.Nonce)
	if err != nil {
		t.Fatal(err)
	}
	if control.EvidenceDigest == resources.EvidenceDigest || resources.EvidenceDigest == released.EvidenceDigest ||
		control.EvidenceDigest == released.EvidenceDigest {
		t.Fatal("independent receipts share evidence")
	}
	if control.Stamp != resources.Stamp || resources.Stamp != released.Stamp || !reflect.DeepEqual(resources.Census, released.Census) {
		t.Fatal("receipts do not describe the same barrier")
	}
	resources.Census.Resources[0].ID = "mutated"
	if released.Census.Resources[0].ID != "original-process" || o.Census.Resources[0].ID != "original-process" {
		t.Fatal("receipt aliases caller or another receipt")
	}
	reverse := f.attempt
	reverse.Handle.CutID, reverse.Handle.Generation, reverse.SourceGeneration = strings.Repeat("f", 32), 3, 2
	reverse.Direction, reverse.SourceSHA, reverse.TargetSHA = upgradetx.DirectionReverse, f.attempt.TargetSHA, f.attempt.SourceSHA
	_, err = ResourcesProof(reverse, o, o.Nonce)
	requireRefusal(t, err)
	_, err = ControlProof(reverse, o, o.Nonce)
	requireRefusal(t, err)
	_, err = ReleasedProof(reverse, o, o.Nonce)
	requireRefusal(t, err)
}

// origin: hot-update S8 plan §2.1 / P03 invalid producer and cut
func TestRegistryRejectsIncompleteIdentity(t *testing.T) {
	for _, defect := range []string{"producer", "producer-PID", "producer-start", "producer-instance", "probe", "TxID", "CutID", "generation", "source-generation", "revision", "direction", "source-SHA", "target-SHA", "participant", "role", "config"} {
		t.Run(defect, func(t *testing.T) {
			f := fixture()
			config := f.config()
			switch defect {
			case "producer":
				config.Producer.BootID = ""
			case "producer-PID":
				config.Producer.PID = 0
			case "producer-start":
				config.Producer.StartTicks = 0
			case "producer-instance":
				config.Producer.InstanceID = ""
			case "probe":
				config.Probe = nil
			case "TxID":
				config.Attempt.Handle.TxID = ""
			case "CutID":
				config.Attempt.Handle.CutID = strings.Repeat("F", 32)
			case "generation":
				config.Attempt.Handle.Generation = 0
			case "source-generation":
				config.Attempt.SourceGeneration = config.Attempt.Handle.Generation
			case "revision":
				config.Attempt.Handle.Revision = 0
			case "direction":
				config.Attempt.Direction = ""
			case "source-SHA":
				config.Attempt.SourceSHA = "unknown"
			case "target-SHA":
				config.Attempt.TargetSHA = ""
			case "participant":
				config.Attempt.Scope.Participants = nil
			case "role":
				config.Attempt.Handle.Key.Component = upgradetx.ComponentCaddy
			case "config":
				config.Attempt.Handle.Key.ConfigPath = ""
			}
			_, err := New(config)
			requireRefusal(t, err)
		})
	}
}
