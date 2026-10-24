package upgraderoll

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/LinZiyang666/tether/internal/upgradeartifact"
	"github.com/LinZiyang666/tether/internal/upgradetx"
	"github.com/LinZiyang666/tether/internal/upgradetx/contractstub"
)

// These boundary models exercise the real roll driver, not P02 durability or
// P06 capability collectors. Unused frozen operations still refuse through the
// actual contractstub. Production journal/lease adapters remain integration
// obligations; none of these models is a substitute for their receipts.
type rollModel struct {
	contractstub.Store
	mu           sync.Mutex
	plan         upgradetx.Plan
	sealed       *upgradetx.Plan
	record       upgradetx.Record
	children     map[string]upgradetx.Attempt
	log          []string
	starts       map[string]int
	holder       bool
	stopCount    int
	interval     time.Duration
	queryHook    func(upgradetx.Step, *upgradetx.View) error
	fleetHook    func(*FleetProof)
	receiptHook  func(upgradetx.Step, *upgradetx.Receipt)
	parentHook   func(*upgradetx.Observation)
	artifactHook func(*upgradeartifact.Receipt)
	startHook    func(upgradetx.Step) error
	applyHook    func(upgradetx.Handle, upgradetx.Event) error
	renewHook    func(context.Context) error
	releaseError bool
	applyNoWrite bool
	startCalls   int
	reserveCalls int
}

type rollPort struct {
	contractstub.AgentEndpoint
	model *rollModel
}
type rollReservation struct{ model *rollModel }

func testID(index int) string  { return fmt.Sprintf("%032x", index) }
func testSHA(index int) string { return fmt.Sprintf("%064x", index) }
func testOwner(index int) upgradetx.Owner {
	return upgradetx.Owner{BootID: "fixture-boot", PID: 100 + index, StartTicks: uint64(1000 + index), InstanceID: testID(100 + index)}
}

func newRollModel(t *testing.T) (*rollModel, *rollPort) {
	t.Helper()
	keys := []upgradetx.Key{
		{DomainID: "host-a", Component: upgradetx.ComponentBroker, ConfiguredNID: "voter", ConfigPath: "/fixture/broker-a", UID: 101},
		{DomainID: "host-a", Component: upgradetx.ComponentAgent, SID: "session", ConfiguredNID: "agent", ConfigPath: "/fixture/agent", UID: 102},
		{DomainID: "host-b", Component: upgradetx.ComponentBroker, ConfiguredNID: "non-voter", ConfigPath: "/fixture/broker-b", UID: 101},
	}
	parentKey := upgradetx.Key{DomainID: "cluster", Component: upgradetx.ComponentBroker, ConfiguredNID: "parent", ConfigPath: "/fixture/parent"}
	plan := upgradetx.Plan{Parent: upgradetx.Handle{Key: parentKey, TxID: testID(1), CutID: testID(2), Generation: 2, Revision: 1}, SourceManifestDigest: testSHA(11), TargetManifestDigest: testSHA(12), Deadline: time.Now().Add(time.Minute).UnixNano()}
	plan.Scope = upgradetx.Scope{ID: testID(3), Participants: append([]upgradetx.Key(nil), keys...), Domains: []upgradetx.Domain{upgradetx.DomainControl, upgradetx.DomainExec, upgradetx.DomainUpgrade}}
	slices.SortFunc(plan.Scope.Participants, keyOrder)
	scopes := make(map[upgradetx.Key]upgradetx.Scope)
	for i, key := range keys {
		resource := upgradetx.Resource{Domain: upgradetx.DomainExec, ID: "original-" + key.ConfiguredNID, Origin: "fixture", Owner: testOwner(i + 1), Generation: 1, State: "active", Custody: "owned"}
		scope := upgradetx.Scope{ID: testID(20 + i), Participants: []upgradetx.Key{key}, Domains: append([]upgradetx.Domain(nil), plan.Scope.Domains...), Expected: []upgradetx.Resource{resource}}
		if i == 2 {
			scope.Expected = append(scope.Expected, upgradetx.Resource{Domain: upgradetx.DomainUpgrade, ID: "retained-helper", Origin: "fixture", Owner: testOwner(4), Generation: 1, State: "active", Custody: "owned"})
		}
		scopes[key] = scope
		plan.Scope.Expected = append(plan.Scope.Expected, scope.Expected...)
	}
	slices.SortFunc(plan.Scope.Expected, func(a, b upgradetx.Resource) int { return strings.Compare(resourceID(a), resourceID(b)) })
	participants := []upgradetx.Key{keys[0], keys[1], keys[0], keys[0], keys[2], keys[2], keys[2], parentKey}
	kinds := []upgradetx.StepKind{upgradetx.StepBrokerCut, upgradetx.StepAgentCut, upgradetx.StepHomeConverge, upgradetx.StepRelease, upgradetx.StepBrokerCut, upgradetx.StepHomeConverge, upgradetx.StepRelease, upgradetx.StepRelease}
	deps := [][]uint32{nil, {1}, {1, 2}, {3}, nil, {5}, {6}, {4, 7}}
	model := &rollModel{children: make(map[string]upgradetx.Attempt), starts: make(map[string]int), holder: true, interval: time.Hour}
	for i, key := range participants {
		scope := plan.Scope
		if kinds[i] == upgradetx.StepBrokerCut || kinds[i] == upgradetx.StepAgentCut {
			scope = scopes[key]
		}
		grant := upgradetx.Grant{Handle: upgradetx.Handle{Key: key, TxID: testID(30 + i), CutID: testID(50 + i), Generation: 2, Revision: 1}, ParentTxID: plan.Parent.TxID, ParentSlot: "opaque-slot-" + key.DomainID, ClientNonce: testID(70 + i), Issuer: testOwner(i + 1), ActorFP: "fixture-actor", Mode: upgradetx.ModeHot, SourceRelease: "a", TargetRelease: "b", SourceSHA: testSHA(1), TargetSHA: testSHA(2), ScopeDigest: ScopeDigest(scope), CapabilityDigest: testSHA(3), ExpiresAt: plan.Deadline}
		step := upgradetx.Step{Ordinal: uint32(i + 1), Participant: key, Grant: grant, Kind: kinds[i], DependsOn: deps[i]}
		plan.Steps = append(plan.Steps, step)
		if isCut(step) {
			model.children[grant.Handle.TxID] = upgradetx.Attempt{Handle: grant.Handle, Direction: upgradetx.DirectionForward, SourceSHA: grant.SourceSHA, TargetSHA: grant.TargetSHA, SourceGeneration: 1, Phase: upgradetx.PhasePrepared, CutStage: upgradetx.CutNone, Outcome: upgradetx.OutcomePending, Scope: scope}
		}
	}
	model.plan = clonePlan(plan)
	grant := plan.Steps[len(plan.Steps)-1].Grant
	grant.Handle = plan.Parent
	grant.ScopeDigest = ScopeDigest(plan.Scope)
	model.record = upgradetx.Record{Version: upgradetx.RecordVersion, Key: parentKey, Revision: 1, Grant: grant, Forward: upgradetx.Attempt{Handle: plan.Parent, Direction: upgradetx.DirectionForward, SourceSHA: grant.SourceSHA, TargetSHA: grant.TargetSHA, SourceGeneration: 1, Phase: upgradetx.PhasePrepared, CutStage: upgradetx.CutNone, Outcome: upgradetx.OutcomePending, Scope: plan.Scope}}
	if err := validatePlan(plan); err != nil {
		t.Fatal(err)
	}
	return model, &rollPort{model: model}
}

func (m *rollModel) Load(key upgradetx.Key, txID string) (upgradetx.View, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.record.Key != key || m.record.Forward.Handle.TxID != txID {
		return upgradetx.View{}, refusal(upgradetx.CodeGrantUnknown, "parent not found", false)
	}
	record := m.record
	record.Actions = append([]upgradetx.Action(nil), record.Actions...)
	return upgradetx.View{Historical: record}, nil
}
func (m *rollModel) SealPlan(plan upgradetx.Plan) (upgradetx.Plan, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.sealed == nil {
		copy := clonePlan(plan)
		m.sealed = &copy
		m.log = append(m.log, "seal")
	}
	return clonePlan(*m.sealed), nil
}
func (m *rollModel) LoadPlan(parent upgradetx.Handle) (upgradetx.Plan, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.sealed == nil || !sameAttempt(m.sealed.Parent, parent) {
		return upgradetx.Plan{}, refusal(upgradetx.CodeGrantUnknown, "plan not found", false)
	}
	return clonePlan(*m.sealed), nil
}
func (m *rollModel) Apply(expected upgradetx.Handle, event upgradetx.Event) (upgradetx.Record, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.applyHook != nil {
		if err := m.applyHook(expected, event); err != nil {
			return upgradetx.Record{}, err
		}
	}
	if expected != m.record.Forward.Handle {
		return upgradetx.Record{}, refusal(upgradetx.CodeUnsettled, "stale parent CAS", true)
	}
	if m.applyNoWrite {
		return m.record, nil
	}
	index := -1
	for i, action := range m.record.Actions {
		if action.ID == event.Action.ID {
			index = i
		}
	}
	if index == -1 {
		m.record.Actions = append(m.record.Actions, event.Action)
	} else {
		m.record.Actions[index] = event.Action
	}
	m.record.Revision++
	m.record.Forward.Handle.Revision = m.record.Revision
	m.log = append(m.log, string(event.Action.State)+":"+event.Action.ID)
	return m.record, nil
}
func (m *rollModel) CompletePlan(expected upgradetx.Handle, observation upgradetx.Observation) (upgradetx.View, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if expected != m.record.Forward.Handle || m.holder {
		return upgradetx.View{}, refusal(upgradetx.CodeUnsettled, "parent completion CAS refused", true)
	}
	m.record.Forward = observation.Current
	m.record.Revision++
	m.record.Forward.Handle.Revision = m.record.Revision
	m.log = append(m.log, "complete-parent")
	return upgradetx.View{Historical: m.record, Observation: observation}, nil
}

func fixtureStamp(handle upgradetx.Handle, scope upgradetx.Scope, nonce string) upgradetx.Stamp {
	return upgradetx.Stamp{Attempt: handle, Producer: testOwner(1), Nonce: nonce, Sequence: 100, At: time.Now().UnixNano(), ScopeDigest: ScopeDigest(scope)}
}
func fixtureCensus(handle upgradetx.Handle, scope upgradetx.Scope, nonce string) upgradetx.Census {
	return upgradetx.Census{Stamp: fixtureStamp(handle, scope, nonce), Known: true, Complete: true, Resources: append([]upgradetx.Resource(nil), scope.Expected...), ExpectedDigest: digest(scope.Expected)}
}
func fixtureObservation(attempt upgradetx.Attempt, nonce string) upgradetx.Observation {
	census := fixtureCensus(attempt.Handle, attempt.Scope, nonce)
	running := attempt.SourceSHA
	if attempt.Phase == upgradetx.PhaseCompleted {
		running = attempt.TargetSHA
		kinds := []upgradetx.ReceiptKind{upgradetx.ReceiptControlReady, upgradetx.ReceiptResourcesMigrated, upgradetx.ReceiptOldGenerationReleased}
		receipts := []*upgradetx.Receipt{&attempt.Control, &attempt.Resources, &attempt.Released}
		for i := range receipts {
			*receipts[i] = upgradetx.Receipt{Kind: kinds[i], Stamp: census.Stamp, Known: true, Complete: true, RunningSHA: running, Census: census, EvidenceDigest: testSHA(90 + i)}
		}
	}
	return upgradetx.Observation{Nonce: nonce, At: time.Now().UnixNano(), Current: attempt, Census: census, OwnerKnown: true, RunningSHA: running, EffectiveSHA: running}
}
func (p *rollPort) Query(ctx context.Context, key upgradetx.Key, txID, nonce string) (upgradetx.View, error) {
	if err := ctx.Err(); err != nil {
		return upgradetx.View{}, err
	}
	m := p.model
	m.mu.Lock()
	defer m.mu.Unlock()
	for _, step := range m.plan.Steps {
		if step.Grant.Handle.TxID != txID || step.Participant != key || !isCut(step) {
			continue
		}
		attempt, exists := m.children[txID]
		if !exists {
			return upgradetx.View{}, refusal(upgradetx.CodeGrantUnknown, "child missing", false)
		}
		view := upgradetx.View{Historical: upgradetx.Record{Version: 2, Key: key, Grant: step.Grant, Forward: attempt}, Observation: fixtureObservation(attempt, nonce)}
		m.log = append(m.log, fmt.Sprintf("query:%d", step.Ordinal))
		if m.queryHook != nil {
			if err := m.queryHook(step, &view); err != nil {
				return view, err
			}
		}
		return view, nil
	}
	return upgradetx.View{}, refusal(upgradetx.CodeGrantUnknown, "child missing", false)
}
func (p *rollPort) Start(ctx context.Context, grant upgradetx.Grant, _ upgradeartifact.Receipt) (upgradetx.View, error) {
	m := p.model
	m.mu.Lock()
	defer m.mu.Unlock()
	m.startCalls++
	if ctx.Err() != nil {
		return upgradetx.View{}, ctx.Err()
	}
	if m.sealed == nil || !m.holder {
		return upgradetx.View{}, errors.New("effect before durable plan/reservation")
	}
	for _, step := range m.plan.Steps {
		if !reflect.DeepEqual(step.Grant, grant) {
			continue
		}
		intent := stepAction(m.plan, step)
		found := false
		for _, action := range m.record.Actions {
			if action.ID == intent.ID && action.State == upgradetx.ActionIntent {
				found = true
			}
		}
		if !found {
			return upgradetx.View{}, errors.New("dispatch before durable action intent")
		}
		m.starts[grant.Handle.TxID]++
		m.log = append(m.log, fmt.Sprintf("start:%d", step.Ordinal))
		attempt := m.children[grant.Handle.TxID]
		attempt.Phase = upgradetx.PhaseCompleted
		attempt.CutStage = upgradetx.CutReleased
		attempt.Outcome = upgradetx.OutcomeSuccess
		m.children[grant.Handle.TxID] = attempt
		if m.startHook != nil {
			return upgradetx.View{}, m.startHook(step)
		}
		return upgradetx.View{}, nil
	}
	return upgradetx.View{}, errors.New("unexpected new child grant")
}
func (p *rollPort) Reserve(ctx context.Context, plan upgradetx.Plan) (Reservation, error) {
	m := p.model
	m.mu.Lock()
	defer m.mu.Unlock()
	m.reserveCalls++
	if m.sealed == nil || !reflect.DeepEqual(plan, *m.sealed) {
		return nil, errors.New("reservation before immutable plan")
	}
	m.log = append(m.log, "reserve")
	return &rollReservation{model: m}, ctx.Err()
}
func (p *rollPort) Artifact(_ context.Context, grant upgradetx.Grant) (upgradeartifact.Receipt, error) {
	m := p.model
	m.mu.Lock()
	defer m.mu.Unlock()
	receipt := upgradeartifact.Receipt{Grant: grant, SourceSHA: grant.SourceSHA, TargetSHA: grant.TargetSHA, ManifestDigest: m.plan.TargetManifestDigest, CanonicalPath: "/fixture/canonical", SourcePath: "/fixture/source", TargetPath: "/fixture/target"}
	if m.artifactHook != nil {
		m.artifactHook(&receipt)
	}
	return receipt, nil
}
func fixtureBarrier(step upgradetx.Step, nonce string, kind upgradetx.ReceiptKind) upgradetx.Receipt {
	stamp := fixtureStamp(step.Grant.Handle, upgradetx.Scope{}, nonce)
	stamp.ScopeDigest = step.Grant.ScopeDigest
	return upgradetx.Receipt{Kind: kind, Stamp: stamp, Known: true, Complete: true, EvidenceDigest: testSHA(100 + int(step.Ordinal))}
}
func (p *rollPort) Converge(_ context.Context, step upgradetx.Step, nonce string) (upgradetx.Receipt, error) {
	m := p.model
	m.mu.Lock()
	defer m.mu.Unlock()
	m.log = append(m.log, fmt.Sprintf("converge:%d", step.Ordinal))
	receipt := fixtureBarrier(step, nonce, upgradetx.ReceiptControlReady)
	if m.receiptHook != nil {
		m.receiptHook(step, &receipt)
	}
	return receipt, nil
}
func (p *rollPort) ObserveParent(_ context.Context, plan upgradetx.Plan, nonce string) (upgradetx.Observation, error) {
	m := p.model
	m.mu.Lock()
	defer m.mu.Unlock()
	attempt := m.record.Forward
	attempt.Handle = plan.Parent
	attempt.Phase = upgradetx.PhaseCompleted
	attempt.CutStage = upgradetx.CutReleased
	attempt.Outcome = upgradetx.OutcomeSuccess
	observation := fixtureObservation(attempt, nonce)
	if m.parentHook != nil {
		m.parentHook(&observation)
	}
	return observation, nil
}
func (r *rollReservation) Check(_ context.Context, plan upgradetx.Plan, nonce string) (FleetProof, error) {
	m := r.model
	m.mu.Lock()
	defer m.mu.Unlock()
	proof := FleetProof{Parent: plan.Parent, Nonce: nonce, Known: true, Complete: true, Participants: append([]upgradetx.Key(nil), plan.Scope.Participants...), Census: fixtureCensus(plan.Parent, plan.Scope, nonce), HolderHeld: m.holder}
	for i, key := range []upgradetx.Key{plan.Steps[0].Participant, plan.Steps[1].Participant, plan.Steps[4].Participant, plan.Steps[4].Participant} {
		proof.Members = append(proof.Members, FleetMember{Participant: key, Owner: testOwner(i + 1), Release: "a", Generation: 1, Known: true})
	}
	if m.fleetHook != nil {
		m.fleetHook(&proof)
	}
	m.log = append(m.log, "fleet")
	return proof, nil
}
func (r *rollReservation) Release(_ context.Context, step upgradetx.Step, nonce string) (upgradetx.Receipt, error) {
	m := r.model
	m.mu.Lock()
	defer m.mu.Unlock()
	if step.Ordinal == uint32(len(m.plan.Steps)) {
		m.holder = false
		if m.releaseError {
			return upgradetx.Receipt{}, errors.New("release reply lost after holder CAS")
		}
	}
	m.log = append(m.log, fmt.Sprintf("release:%d", step.Ordinal))
	receipt := fixtureBarrier(step, nonce, upgradetx.ReceiptOldGenerationReleased)
	if m.receiptHook != nil {
		m.receiptHook(step, &receipt)
	}
	return receipt, nil
}
func (r *rollReservation) Renew(ctx context.Context, parent upgradetx.Handle) error {
	m := r.model
	m.mu.Lock()
	hook := m.renewHook
	m.log = append(m.log, "renew")
	matches := sameAttempt(parent, m.plan.Parent)
	m.mu.Unlock()
	if !matches {
		return errors.New("wrong holder")
	}
	if hook != nil {
		return hook(ctx)
	}
	return nil
}
func (r *rollReservation) RenewalInterval() time.Duration { return r.model.interval }
func (r *rollReservation) Stop() error {
	r.model.mu.Lock()
	defer r.model.mu.Unlock()
	r.model.stopCount++
	return nil
}

func runModel(t *testing.T, m *rollModel, p *rollPort) (upgradetx.View, error) {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	return Run(ctx, m, p, m.plan)
}
func requireUnsettled(t *testing.T, err error) {
	t.Helper()
	var txerr *upgradetx.Error
	if !errors.As(err, &txerr) || !txerr.Accepted || txerr.Code == upgradetx.CodeDeferred {
		t.Fatalf("expected accepted refusal, got %v", err)
	}
}

// origin: hot-update S8 plan §3.1 / §5.1 / §8.3 M09
func TestParentPlanRejectsIncompleteHierarchy(t *testing.T) {
	mutants := map[string]func(*upgradetx.Plan){
		"participant omitted": func(p *upgradetx.Plan) { p.Scope.Participants = p.Scope.Participants[1:] },
		"missing participant cut": func(p *upgradetx.Plan) {
			p.Scope.Participants = append(p.Scope.Participants, upgradetx.Key{DomainID: "offline", Component: upgradetx.ComponentAgent, SID: "s", ConfiguredNID: "n", ConfigPath: "/fixture/offline"})
			slices.SortFunc(p.Scope.Participants, keyOrder)
		},
		"random child": func(p *upgradetx.Plan) { p.Steps[1].Grant.ParentTxID = testID(999) },
		"wrong slot":   func(p *upgradetx.Plan) { p.Steps[1].Grant.ParentSlot = "another-host" },
		"slot borrowed across hosts": func(p *upgradetx.Plan) {
			for i := 4; i <= 6; i++ {
				p.Steps[i].Grant.ParentSlot = p.Steps[0].Grant.ParentSlot
			}
		},
		"future dependency":            func(p *upgradetx.Plan) { p.Steps[1].DependsOn = []uint32{3} },
		"host release before converge": func(p *upgradetx.Plan) { p.Steps[3].DependsOn = nil },
		"cluster release omits host":   func(p *upgradetx.Plan) { p.Steps[7].DependsOn = []uint32{4} },
		"missing sibling barrier":      func(p *upgradetx.Plan) { p.Steps[2].DependsOn = []uint32{1} },
		"duplicate grant":              func(p *upgradetx.Plan) { p.Steps[1].Grant.Handle.TxID = p.Steps[0].Grant.Handle.TxID },
		"renewed child ttl":            func(p *upgradetx.Plan) { p.Steps[1].Grant.ExpiresAt++ },
	}
	for name, mutate := range mutants {
		t.Run(name, func(t *testing.T) {
			m, _ := newRollModel(t)
			mutate(&m.plan)
			if err := validatePlan(m.plan); err == nil {
				t.Fatal("unsafe parent accepted")
			}
		})
	}
}

// origin: hot-update S8 plan §6.3 / §8.3 M18
func TestFullFleetReleaseReservation(t *testing.T) {
	for _, name := range []string{"non-voter third release", "agent unknown", "retained helper third release", "missing participant", "missing helper census", "third live generation", "holder lost", "late third release"} {
		t.Run(name, func(t *testing.T) {
			m, p := newRollModel(t)
			checks := 0
			m.fleetHook = func(f *FleetProof) {
				checks++
				switch name {
				case "non-voter third release":
					f.Members[2].Release = "c"
				case "agent unknown":
					f.Members[1].Known = false
				case "retained helper third release":
					f.Members[3].Release = "c"
				case "missing participant":
					f.Participants = f.Participants[:2]
				case "missing helper census":
					f.Census.Resources = f.Census.Resources[:len(f.Census.Resources)-1]
				case "third live generation":
					for i := 2; i <= 3; i++ {
						member := f.Members[2]
						member.Owner = testOwner(10 + i)
						member.Generation = uint64(i)
						f.Members = append(f.Members, member)
					}
				case "holder lost":
					f.HolderHeld = false
				case "late third release":
					if checks >= 2 {
						f.Members[2].Release = "c"
					}
				}
			}
			_, err := runModel(t, m, p)
			requireUnsettled(t, err)
			if len(m.starts) != 0 || !m.holder || m.stopCount != 1 {
				t.Fatalf("fleet refusal performed effects: starts=%v holder=%v stops=%d", m.starts, m.holder, m.stopCount)
			}
		})
	}
}

// origin: hot-update S8 plan §5.1 / §11.2 P11
func TestParentReleaseWaitsForEveryFreshChild(t *testing.T) {
	for _, name := range []string{"child unknown after completion", "home proof missing", "host release proof missing", "cluster release proof missing", "holder remains after release"} {
		t.Run(name, func(t *testing.T) {
			m, p := newRollModel(t)
			seen := 0
			m.queryHook = func(step upgradetx.Step, v *upgradetx.View) error {
				if name == "child unknown after completion" && step.Ordinal == 1 && v.Historical.Forward.Phase == upgradetx.PhaseCompleted {
					seen++
					if seen > 1 {
						v.Observation.OwnerKnown = false
					}
				}
				return nil
			}
			m.receiptHook = func(step upgradetx.Step, r *upgradetx.Receipt) {
				if (name == "home proof missing" && step.Ordinal == 3) || (name == "host release proof missing" && step.Ordinal == 4) || (name == "cluster release proof missing" && step.Ordinal == 8) {
					r.Known = false
				}
			}
			m.fleetHook = func(f *FleetProof) {
				if name == "holder remains after release" && !m.holder {
					f.HolderHeld = true
				}
			}
			_, err := runModel(t, m, p)
			requireUnsettled(t, err)
			if slices.Contains(m.log, "complete-parent") {
				t.Fatal("parent completed without independent release proof")
			}
			if name == "child unknown after completion" || name == "home proof missing" {
				if slices.Contains(m.log, "release:4") || slices.Contains(m.log, "release:8") {
					t.Fatal("reservation released before child/home completion")
				}
			}
		})
	}
}

// origin: hot-update S8 plan §2.2 / §11.2 P11
func TestParentPlanRejectsUnwiredPorts(t *testing.T) {
	m, p := newRollModel(t)
	if _, err := Run(t.Context(), contractstub.Store{}, p, m.plan); err == nil || !strings.Contains(err.Error(), "P02") {
		t.Fatalf("missing journal port: %v", err)
	}
	if _, err := Run(t.Context(), m, contractstub.AgentEndpoint{}, m.plan); err == nil || !strings.Contains(err.Error(), "P15/P19") {
		t.Fatalf("missing reservation port: %v", err)
	}
}
