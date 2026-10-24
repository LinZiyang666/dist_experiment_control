//go:build linux && hotupdate_integration

package hotupdate_test

import (
	"context"
	"crypto/sha256"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"reflect"
	"runtime"
	"slices"
	"testing"
	"time"

	"github.com/LinZiyang666/tether/internal/testharness"
	"github.com/LinZiyang666/tether/test/fullstackharness"
	c "github.com/LinZiyang666/tether/test/hotupdate/acceptance/contract"
)

// Generation is a resource-custody axis; Release is a fleet axis. Distinct
// owners of identical bytes must not collapse into one owner in either census.
// Event times are sequenced by one collector, not compared across host clocks.
// origin: hot-update S10 plan §5.2 / M06A/M06B/M06R/M06F
type fullstackOwner struct {
	Stamp   c.Stamp
	Release c.Digest
}

type fullstackOwnerEvent struct {
	Owner    fullstackOwner
	Resource string
	Event    string
	AtNS     int64
	Sequence uint64
}

type fullstackOwnerEffect struct {
	Owner                   fullstackOwner
	Resource, Operation     string
	AtNS                    int64
	Count                   int
	WriterAdmissionObserved bool
}

type fullstackLockProbe struct {
	AtNS             int64
	Acquired         bool
	Owner, Contender c.Process
	Device, Inode    uint64
}

type fullstackFDProof struct {
	Resource, OFD                             string
	Before, After                             c.Process
	OriginalFD, CurrentFD, OriginalReferences int
	KCMP                                      int
	OriginalPresent, CloseOnExec              bool
}

type fullstackOwnershipProof struct {
	Round                      int
	RunID                      string
	Required                   []c.Key
	Initial, Final             []fullstackOwner
	Events                     []fullstackOwnerEvent
	Effects                    []fullstackOwnerEffect
	StartNS, EndNS, ReleasedNS int64
	Known, Complete            bool
	LockProbes                 []fullstackLockProbe
	FDs                        []fullstackFDProof
	ChildPrivateFDs            []string
	ChildCensusKnown           bool
}

// origin: hot-update S10 plan §5.2 / §10.2 P24
func TestFullstackOwnershipIntervalsAreExclusive(t *testing.T) {
	t.Run("oracle", fullstackOwnershipControls)
	beforeG, beforeFD := runtime.NumGoroutine(), testharness.FDCount()
	for round := 0; round < 5; round++ {
		t.Run(fmt.Sprintf("round-%d", round), func(t *testing.T) { exerciseOwnershipCut(t, round) })
	}
	testharness.AssertNoGoroutineLeak(t, "fullstack ownership and same-PID lease x5", beforeG)
	testharness.AssertNoFDLeak(t, "fullstack ownership and same-PID lease x5", beforeFD, 0)
}

func exerciseOwnershipCut(t *testing.T, round int) {
	t.Helper()
	t.Run("lease", func(t *testing.T) {
		started := time.Now()
		// Reuse the S8 same-PID helper, which starts and joins through the
		// shared FIX-ORPHAN supervisor. Its independent contender observes
		// the original lease before exec, after commit and after release.
		exerciseTransactionLeaseExec(t)
		t.Logf("cost: lease startup+I/O+cut+settle+supervisor join=%s", time.Since(started))
	})
	t.Run("product", func(t *testing.T) {
		name := fmt.Sprintf("s10/p24/round-%d/ownership", round)
		if *fullstackLocalSupply != "" {
			if err := fullstackharness.VerifyAssemblyExecutable(t.Context(), filepath.Join(*fullstackLocalSupply, "assembly-host"), "host"); err != nil {
				t.Fatalf("SETUP-RED: NOT_RUN pending P25 source-pinned ownership fixture: %v", err)
			}
		}
		run := newFullstackRollout(t, round)
		defer run.close(t)
		_, err := run.fixture.Start(t.Context(), run.profile)
		transferMust(t, err)
		before := run.capture(t, "ownership-original").Evidence
		witnesses := fullstackRetainOwnershipFDs(t, before)
		defer func() {
			for _, witness := range witnesses {
				if err := witness.Witness.Close(); err != nil {
					t.Error(err)
				}
			}
		}()
		after := before
		for _, step := range run.profile.Steps {
			_, err = fullstackharness.ApplyFixtureStep(t.Context(), run.fixture, step.ID)
			transferMust(t, err)
			after = run.capture(t, "ownership-"+step.ID).Evidence
		}
		supplied, err := fullstackOwnershipFromRun(t.Context(), name, round, run, before, after)
		transferMust(t, err)
		for _, witness := range witnesses {
			index := slices.IndexFunc(supplied.FDs, func(fd fullstackFDProof) bool {
				return fd.Before == witness.Owner.Process && fd.OriginalFD == witness.FD && fd.Resource == witness.Resource
			})
			if index < 0 {
				t.Fatal("ownership omitted an original kernel FD witness")
			}
			fd := supplied.FDs[index]
			sample, ok := fullstackLatestSample(after, fd.After.Key)
			if !ok {
				t.Fatal("ownership FD has no current owner")
			}
			_, err := witness.Witness.Check(t.Context(), sample.Stamp, fd.CurrentFD)
			transferMust(t, err)
		}
		transferMust(t, fullstackJudgeOwnership(supplied))
		_, err = run.fixture.Drain(t.Context(), fullstackRolloutNonce())
		transferMust(t, err)
	})
}

type fullstackOwnerKey struct {
	Process c.Process
	Cut     string
	Gen     uint64
}

func fullstackOwnerID(owner fullstackOwner) fullstackOwnerKey {
	return fullstackOwnerKey{owner.Stamp.Process, owner.Stamp.CutID, owner.Stamp.Generation}
}

func fullstackOwnerValid(owner fullstackOwner, required []c.Key) bool {
	s := owner.Stamp
	return slices.Contains(required, s.Process.Key) && s.Process.BootID != "" && s.Process.PIDNS != "" &&
		s.Process.PID > 0 && s.Process.StartTicks > 0 && s.Process.InstanceID != "" &&
		s.CutID != "" && s.Generation > 0 && owner.Release != (c.Digest{})
}

// Replay the entire activation/retirement ledger. A periodic census would
// miss a short third generation, and checking SHA alone would miss two active
// copies of the same release. Effects are joined to the interval containing
// their observed occurrence, so a claimed retirement cannot erase a late write.
func fullstackJudgeOwnership(e fullstackOwnershipProof) error {
	if !e.Known || !e.Complete || len(e.Required) < 2 || len(e.Initial) != len(e.Required) ||
		len(e.Final) != len(e.Required) || len(e.Events) == 0 || len(e.Effects) == 0 || e.StartNS <= 0 ||
		e.EndNS <= e.StartNS || e.ReleasedNS < e.EndNS {
		return fmt.Errorf("full-fleet ownership census is missing or unknown")
	}
	population := make(map[c.Key]bool)
	fleet := make(map[fullstackOwnerKey]fullstackOwner)
	for _, key := range e.Required {
		if population[key] || (key.Role != "agent" && key.Role != "broker") {
			return fmt.Errorf("duplicate or unknown managed runtime")
		}
		population[key] = true
	}
	seenInitial := make(map[c.Key]bool)
	for _, owner := range e.Initial {
		if !fullstackOwnerValid(owner, e.Required) || seenInitial[owner.Stamp.Process.Key] {
			return fmt.Errorf("initial census omitted or duplicated an agent/nonvoter")
		}
		seenInitial[owner.Stamp.Process.Key] = true
		fleet[fullstackOwnerID(owner)] = owner
	}
	if err := fullstackTwoReleases(fleet); err != nil {
		return err
	}
	type interval struct {
		owner      fullstackOwner
		resource   string
		start, end int64
	}
	var intervals []interval
	retained := make(map[string]map[fullstackOwnerKey]fullstackOwner)
	active := make(map[string]int)
	lastNS, lastSequence := e.StartNS, uint64(0)
	for _, event := range e.Events {
		id := fullstackOwnerID(event.Owner)
		if !fullstackOwnerValid(event.Owner, e.Required) || event.Resource == "" || event.AtNS < lastNS ||
			event.AtNS > e.EndNS || event.Sequence != lastSequence+1 {
			return fmt.Errorf("ownership event gap, unknown owner or unordered interval")
		}
		lastNS, lastSequence = event.AtNS, event.Sequence
		if old, ok := fleet[id]; ok && old.Release != event.Owner.Release {
			return fmt.Errorf("one runtime cut claims multiple releases")
		}
		switch event.Event {
		case "fleet-enter":
			if _, exists := fleet[id]; exists {
				return fmt.Errorf("runtime entered fleet twice")
			}
			fleet[id] = event.Owner
		case "fleet-retire":
			if _, exists := fleet[id]; !exists {
				return fmt.Errorf("unobserved fleet owner retired")
			}
			for _, owners := range retained {
				if _, exists := owners[id]; exists {
					return fmt.Errorf("fleet retired before its last resource")
				}
			}
			delete(fleet, id)
		case "retain":
			if _, exists := fleet[id]; !exists {
				return fmt.Errorf("resource retained by an unknown fleet owner")
			}
			if retained[event.Resource] == nil {
				retained[event.Resource] = make(map[fullstackOwnerKey]fullstackOwner)
			}
			if _, exists := retained[event.Resource][id]; exists {
				return fmt.Errorf("duplicate resource retention")
			}
			retained[event.Resource][id] = event.Owner
			if len(retained[event.Resource]) > 2 {
				return fmt.Errorf("M06R: transient third resource generation")
			}
		case "activate":
			if _, retained := retained[event.Resource][id]; !retained {
				return fmt.Errorf("activation without observed retention")
			}
			if _, exists := active[event.Resource]; exists {
				return fmt.Errorf("M06A/M06B: overlapping active resource owners, including same SHA")
			}
			active[event.Resource] = len(intervals)
			intervals = append(intervals, interval{event.Owner, event.Resource, event.AtNS, e.EndNS + 1})
		case "deactivate":
			index, exists := active[event.Resource]
			if !exists || fullstackOwnerID(intervals[index].owner) != id {
				return fmt.Errorf("deactivation does not match the active owner")
			}
			intervals[index].end = event.AtNS
			delete(active, event.Resource)
		case "retire":
			if index, exists := active[event.Resource]; exists && fullstackOwnerID(intervals[index].owner) == id {
				return fmt.Errorf("resource retired while still active")
			}
			if _, exists := retained[event.Resource][id]; !exists {
				return fmt.Errorf("unobserved resource retired")
			}
			delete(retained[event.Resource], id)
		default:
			return fmt.Errorf("unknown authority event")
		}
		if err := fullstackTwoReleases(fleet); err != nil {
			return err
		}
	}
	seenFinal := make(map[fullstackOwnerKey]bool)
	finalPopulation := make(map[c.Key]bool)
	for _, owner := range e.Final {
		id := fullstackOwnerID(owner)
		key := owner.Stamp.Process.Key
		if !fullstackOwnerValid(owner, e.Required) || seenFinal[id] || finalPopulation[key] || fleet[id] != owner {
			return fmt.Errorf("final census differs from the full activation ledger")
		}
		seenFinal[id] = true
		finalPopulation[key] = true
		initial := e.Initial[slices.IndexFunc(e.Initial, func(o fullstackOwner) bool { return o.Stamp.Process.Key == key })]
		if owner.Stamp.Process != initial.Stamp.Process || owner.Stamp.Generation != initial.Stamp.Generation+1 ||
			owner.Stamp.CutID == initial.Stamp.CutID || owner.Release == initial.Release {
			return fmt.Errorf("runtime did not perform a same-PID cut to distinct executing bytes")
		}
	}
	if len(fleet) != len(seenFinal) {
		return fmt.Errorf("old service image did not retire")
	}
	operations := make(map[string]bool)
	writer := false
	for _, effect := range e.Effects {
		if effect.Operation == "" || operations[effect.Operation] || effect.Count != 1 {
			return fmt.Errorf("M11D: business operation has duplicate effects")
		}
		operations[effect.Operation] = true
		matches := 0
		for _, interval := range intervals {
			if effect.Owner == interval.owner && effect.Resource == interval.resource && effect.AtNS >= interval.start && effect.AtNS < interval.end {
				matches++
			}
		}
		if matches != 1 {
			return fmt.Errorf("effect did not occur inside exactly one actual owner interval")
		}
		writer = writer || effect.WriterAdmissionObserved
	}
	if !writer {
		return fmt.Errorf("M11D: no actual SQL writer admission; an early store-lock refusal is insufficient")
	}
	return fullstackJudgePrivateCustody(e)
}

func fullstackTwoReleases(fleet map[fullstackOwnerKey]fullstackOwner) error {
	releases := make(map[c.Digest]bool)
	for _, owner := range fleet {
		releases[owner.Release] = true
	}
	if len(releases) > 2 {
		return fmt.Errorf("M06F: third fleet release, including agents and nonvoters")
	}
	return nil
}

// OFD evidence must name the original FD, not merely some surviving duplicate.
// The child census is independent of the service's CLOEXEC flags: a spawn path
// may accidentally clear one flag after the service census has been collected.
func fullstackJudgePrivateCustody(e fullstackOwnershipProof) error {
	if len(e.LockProbes) < 3 || len(e.FDs) == 0 || !e.ChildCensusKnown || len(e.ChildPrivateFDs) != 0 {
		return fmt.Errorf("M11K: missing custody census or inherited child-private descriptor")
	}
	first := e.LockProbes[0]
	if first.AtNS > e.StartNS || first.Device == 0 || first.Inode == 0 {
		return fmt.Errorf("M11L: lease contender missed the start of the cut")
	}
	last := int64(0)
	for i, probe := range e.LockProbes {
		if probe.AtNS <= last || probe.Owner.PID <= 0 || probe.Contender.PID <= 0 || probe.Owner == probe.Contender ||
			probe.Device != first.Device || probe.Inode != first.Inode ||
			(i < len(e.LockProbes)-1 && (probe.Acquired || probe.AtNS >= e.ReleasedNS)) ||
			(i == len(e.LockProbes)-1 && (!probe.Acquired || probe.AtNS <= e.ReleasedNS)) {
			return fmt.Errorf("M11L: domain lease released before current responsibility retired")
		}
		last = probe.AtNS
	}
	for _, fd := range e.FDs {
		if fd.Resource == "" || fd.OFD == "" || fd.Before.PID <= 0 || fd.Before != fd.After ||
			fd.OriginalFD < 3 || fd.CurrentFD < 3 || !fd.OriginalPresent || fd.OriginalReferences != 1 || fd.KCMP != 0 || !fd.CloseOnExec {
			return fmt.Errorf("M11U/K: original descriptor closed/reopened, duplicate retained or CLOEXEC lost")
		}
	}
	return nil
}

func fullstackOwnershipControl() fullstackOwnershipProof {
	owner := func(role string, pid int, gen uint64, release byte) fullstackOwner {
		key := c.Key{Role: role, Host: "host", Unit: role, SID: "lab", NID: role, Domain: "domain"}
		return fullstackOwner{Stamp: c.Stamp{Process: c.Process{Key: key, BootID: "boot", PIDNS: "pidns", InstanceID: role, PID: pid, StartTicks: 10},
			CutID: fmt.Sprintf("cut-%d", gen), Generation: gen}, Release: c.Digest{release}}
	}
	a, b := owner("agent", 100, 1, 1), owner("broker", 200, 1, 1)
	e := fullstackOwnershipProof{Required: []c.Key{a.Stamp.Process.Key, b.Stamp.Process.Key}, Initial: []fullstackOwner{a, b},
		StartNS: 10, EndNS: 80, ReleasedNS: 90, Known: true, Complete: true, ChildCensusKnown: true}
	for i, o := range []fullstackOwner{a, b} {
		resource := o.Stamp.Process.Key.Role + "/writer"
		e.Events = append(e.Events, fullstackOwnerEvent{Owner: o, Resource: resource, Event: "retain", AtNS: int64(10 + i*10), Sequence: uint64(i*2 + 1)},
			fullstackOwnerEvent{Owner: o, Resource: resource, Event: "activate", AtNS: int64(11 + i*10), Sequence: uint64(i*2 + 2)})
		e.Effects = append(e.Effects, fullstackOwnerEffect{Owner: o, Resource: resource, Operation: resource + "/before", AtNS: 22, Count: 1, WriterAdmissionObserved: i == 1})
	}
	for _, old := range e.Initial {
		current := old
		current.Stamp.Generation, current.Stamp.CutID, current.Release = 2, "cut-2", c.Digest{2}
		e.Final = append(e.Final, current)
		resource := old.Stamp.Process.Key.Role + "/writer"
		fullstackAppendOwnerEvent(&e, current, resource, "fleet-enter")
		fullstackAppendOwnerEvent(&e, current, resource, "retain")
		fullstackAppendOwnerEvent(&e, old, resource, "deactivate")
		fullstackAppendOwnerEvent(&e, current, resource, "activate")
		fullstackAppendOwnerEvent(&e, old, resource, "retire")
		fullstackAppendOwnerEvent(&e, old, resource, "fleet-retire")
		e.Effects = append(e.Effects, fullstackOwnerEffect{Owner: current, Resource: resource, Operation: resource + "/after", AtNS: 60, Count: 1, WriterAdmissionObserved: old.Stamp.Process.Key.Role == "broker"})
	}
	contender := c.Process{PID: 300, StartTicks: 30}
	for i, at := range []int64{10, 50, 100} {
		e.LockProbes = append(e.LockProbes, fullstackLockProbe{AtNS: at, Acquired: i == 2, Owner: a.Stamp.Process, Contender: contender, Device: 1, Inode: 2})
	}
	e.FDs = []fullstackFDProof{{Resource: "original", OFD: "kcmp-witness", Before: a.Stamp.Process, After: a.Stamp.Process,
		OriginalFD: 7, CurrentFD: 8, OriginalReferences: 1, OriginalPresent: true, CloseOnExec: true}}
	return e
}

// origin: hot-update S10 plan §8 / M06A/M06B/M06R/M06F/M11D/M11L/M11U/M11K
func fullstackOwnershipControls(t *testing.T) {
	if err := fullstackJudgeOwnership(fullstackOwnershipControl()); err != nil {
		t.Fatal(err)
	}
	for name, mutate := range map[string]func(*fullstackOwnershipProof){
		"M06A": func(e *fullstackOwnershipProof) { fullstackDuplicateOwner(e, 0) },
		"M06B": func(e *fullstackOwnershipProof) { fullstackDuplicateOwner(e, 1) },
		"M06R": func(e *fullstackOwnershipProof) {
			for generation := uint64(3); generation <= 4; generation++ {
				owner := e.Final[0]
				owner.Stamp.Generation, owner.Stamp.CutID = generation, fmt.Sprintf("cut-%d", generation)
				fullstackAppendOwnerEvent(e, owner, "agent/writer", "fleet-enter")
				fullstackAppendOwnerEvent(e, owner, "agent/writer", "retain")
			}
		},
		"M06F": func(e *fullstackOwnershipProof) {
			for i := byte(3); i <= 4; i++ {
				owner := e.Final[0]
				owner.Release, owner.Stamp.Generation, owner.Stamp.CutID = c.Digest{i}, uint64(i), fmt.Sprintf("cut-%d", i)
				fullstackAppendOwnerEvent(e, owner, "agent/writer", "fleet-enter")
			}
		},
		"M11D":                 func(e *fullstackOwnershipProof) { e.Effects[1].Count++ },
		"M11L":                 func(e *fullstackOwnershipProof) { e.LockProbes[1].Acquired = true },
		"M11U":                 func(e *fullstackOwnershipProof) { e.FDs[0].OriginalPresent = false },
		"M11K":                 func(e *fullstackOwnershipProof) { e.ChildPrivateFDs = []string{"custody-memfd"} },
		"unknown-fleet":        func(e *fullstackOwnershipProof) { e.Known = false },
		"missing-agent":        func(e *fullstackOwnershipProof) { e.Initial = e.Initial[1:] },
		"event-gap":            func(e *fullstackOwnershipProof) { e.Events[1].Sequence++ },
		"effect-before-active": func(e *fullstackOwnershipProof) { e.Effects[0].AtNS = 10 },
		"writer-never-entered": func(e *fullstackOwnershipProof) {
			for i := range e.Effects {
				e.Effects[i].WriterAdmissionObserved = false
			}
		},
		"wrong-ofd":       func(e *fullstackOwnershipProof) { e.FDs[0].KCMP = 1 },
		"extra-dup":       func(e *fullstackOwnershipProof) { e.FDs[0].OriginalReferences++ },
		"cloexec-cleared": func(e *fullstackOwnershipProof) { e.FDs[0].CloseOnExec = false },
	} {
		t.Run(name, func(t *testing.T) {
			e := fullstackOwnershipControl()
			mutate(&e)
			if err := fullstackJudgeOwnership(e); err == nil {
				t.Fatalf("%s: invalid ownership proof accepted", name)
			}
		})
	}
}

func fullstackAppendOwnerEvent(e *fullstackOwnershipProof, owner fullstackOwner, resource, event string) {
	last := e.Events[len(e.Events)-1]
	e.Events = append(e.Events, fullstackOwnerEvent{Owner: owner, Resource: resource, Event: event, AtNS: last.AtNS + 1, Sequence: last.Sequence + 1})
}

func fullstackDuplicateOwner(e *fullstackOwnershipProof, index int) {
	owner := e.Final[index]
	owner.Stamp.Process.PID++ // Same SHA and generation, different real owner.
	resource := owner.Stamp.Process.Key.Role + "/writer"
	fullstackAppendOwnerEvent(e, owner, resource, "fleet-enter")
	fullstackAppendOwnerEvent(e, owner, resource, "retain")
	fullstackAppendOwnerEvent(e, owner, resource, "activate")
}

// origin: S10 internal review rv3-assembly F2
func fullstackOwnershipFromRun(ctx context.Context, name string, round int, run *fullstackRollout, before, after c.Evidence) (fullstackOwnershipProof, error) {
	var supplied fullstackOwnershipProof
	if run == nil || run.fixture == nil || !reflect.DeepEqual(before.Profile, run.profile) || !reflect.DeepEqual(after.Profile, run.profile) {
		return supplied, fmt.Errorf("ownership requires the current supervised product run")
	}
	if len(after.Samples) == 0 {
		return supplied, fmt.Errorf("ownership lacks live product observations")
	}
	nonce := after.Samples[len(after.Samples)-1].Stamp.QueryNonce
	file := filepath.Join(run.profile.EvidenceRoot, fmt.Sprintf("ownership-%x.json", sha256.Sum256([]byte(nonce))))
	data, err := os.ReadFile(file)
	if err != nil {
		return supplied, err
	}
	if nonce == "" || !slices.ContainsFunc(after.Raw, func(ref c.RawRef) bool {
		return ref.Path == file && ref.SHA == sha256.Sum256(data) && ref.Bytes == int64(len(data))
	}) {
		return supplied, fmt.Errorf("ownership is absent from the challenged private observation archive")
	}
	if err := fullstackDecode(data, reflect.ValueOf(&supplied).Elem()); err != nil {
		return supplied, err
	}
	required := []c.Key{}
	for _, key := range run.profile.Required {
		if key.Role == "agent" || key.Role == "broker" {
			required = append(required, key)
		}
	}
	if supplied.Round != round || supplied.RunID != name || !reflect.DeepEqual(supplied.Required, required) {
		return supplied, fmt.Errorf("ownership changed the presealed run or fleet")
	}
	for _, owner := range supplied.Final {
		sample, ok := fullstackLatestSample(after, owner.Stamp.Process.Key)
		original, present := fullstackLatestSample(before, owner.Stamp.Process.Key)
		if !ok || !present || !sample.Complete || sample.Stamp.QueryNonce != nonce || owner.Stamp != sample.Stamp ||
			owner.Release != sample.ExeSHA || original.Stamp.Process != sample.Stamp.Process || original.Stamp.Generation >= sample.Stamp.Generation {
			return supplied, fmt.Errorf("ownership replayed an owner instead of observing this run's cut")
		}
		if err := fullstackOwnershipExecuting(ctx, owner); err != nil {
			return supplied, err
		}
		process, err := fullstackharness.ReadKernelProcess(owner.Stamp.Process.Key, owner.Stamp.Process.PID, owner.Stamp.Process.InstanceID)
		if err != nil || process != owner.Stamp.Process {
			return supplied, fmt.Errorf("ownership process is no longer the live product owner")
		}
	}
	// Private writer/FD rows must be in the product's challenged raw archive;
	// the separate lease helper cannot lend its process identity to this proof.
	return supplied, fullstackJudgeOwnership(supplied)
}

// Retain the original OFDs before the first cut; a reopened endpoint afterwards
// cannot manufacture an ownership witness. Every duplicate is closed at Join.
// origin: S10 internal review rv3-assembly F2
type fullstackOwnershipFD struct {
	Owner    c.Stamp
	FD       int
	Resource string
	Witness  *fullstackharness.OFDWitness
}

func fullstackRetainOwnershipFDs(t *testing.T, before c.Evidence) []fullstackOwnershipFD {
	t.Helper()
	result := []fullstackOwnershipFD{}
	t.Cleanup(func() {
		for _, row := range result {
			if err := row.Witness.Close(); err != nil {
				t.Error(err)
			}
		}
	})
	for _, key := range before.Profile.Required {
		if key.Role != "agent" && key.Role != "broker" {
			continue
		}
		sample, ok := fullstackLatestSample(before, key)
		if !ok {
			t.Fatal("ownership lacks a pre-cut product sample")
		}
		for _, resource := range sample.Resources {
			if resource.FD < 3 || resource.Domain != "exec" && resource.Domain != "flow" && resource.Domain != "broker_store" {
				continue
			}
			witness, err := fullstackharness.NewOFDWitness(sample.Stamp, resource.FD)
			if err != nil {
				t.Fatalf("SETUP-RED: NOT_RUN pending P04 kernel OFD custody: %v", err)
			}
			result = append(result, fullstackOwnershipFD{sample.Stamp, resource.FD, resource.ID, witness})
		}
	}
	if len(result) == 0 {
		t.Fatal("ownership has no live original business/store FDs")
	}
	return result
}

func fullstackOwnershipExecuting(ctx context.Context, owner fullstackOwner) error {
	file, err := os.Open(fmt.Sprintf("/proc/%d/exe", owner.Stamp.Process.PID))
	if err != nil {
		return err
	}
	hash := sha256.New()
	_, readErr := io.Copy(hash, file)
	closeErr := file.Close()
	if readErr != nil || closeErr != nil || !slices.Equal(hash.Sum(nil), owner.Release[:]) || ctx.Err() != nil {
		return fmt.Errorf("ownership running image differs from the observed product")
	}
	return nil
}
