// Package acceptance judges supplied S10 evidence without IO or product imports.
// Pure verdicts do not certify the provenance of a collector's raw bytes.
package acceptance

import (
	"fmt"
	"sort"

	"github.com/LinZiyang666/tether/test/hotupdate/acceptance/contract"
)

// These are the shared leak helper's frozen limits, not machine-dependent knobs.
// Pure judges cannot import testharness (which brings IO and product helpers).
const resourceGoroutineTolerance int64 = 2
const resourceExerciseRounds = 5

type resourceImage struct {
	samples []contract.Sample
}

// JudgeResources keeps parent, live-image and final retirement evidence separate.
// Samples start with a sealed background baseline. Each cycle adds a nonempty
// logical resource ID, records real IO, then retires it before returning to that
// baseline. Metadata may accumulate only under its original retention clock.
// The final natural-drain census must leave only fixed costs and legally
// retained metadata. No caller-supplied phase flag
// can relabel a leak as a settled floor or reset a failed image at the next exec.
// origin: hot-update S10 plan §4.1–§4.3 / P03
func JudgeResources(e contract.Evidence) (contract.Result, error) {
	result := contract.Result{Verdict: "GREEN", Failures: []string{}, Missing: []string{}, Inputs: []contract.RawRef{}}
	images, keys := resourceImages(e, &result)
	for _, event := range e.Intervals {
		addResourceInput(event.Raw, &result)
	}
	hasHarness := false
	for _, key := range keys {
		chain := images[key]
		if key.Role == "fixture" {
			hasHarness = true
		}
		for i, image := range chain {
			judgeResourceImage(e, image, i == len(chain)-1, &result)
			if i > 0 {
				judgeResourceTransition(chain[i-1], image, &result)
			}
		}
	}
	if !hasHarness {
		result.Missing = append(result.Missing, "harness axis: no fixture process resource census")
	}
	judgeReleasedResources(e, &result)
	sort.Strings(result.Failures)
	sort.Strings(result.Missing)
	if len(result.Failures) > 0 {
		result.Verdict, result.RC = "ASSERT-FAIL", 1
	} else if len(result.Missing) > 0 {
		result.Verdict, result.RC = "INCOMPLETE", 4
	}
	return result, nil
}

var _ contract.JudgeFunc = JudgeResources

func resourceImages(e contract.Evidence, result *contract.Result) (map[contract.Key][]resourceImage, []contract.Key) {
	images := make(map[contract.Key][]resourceImage)
	var keys []contract.Key
	required := make(map[contract.Key]bool)
	for _, key := range e.Profile.Required {
		if required[key] {
			result.Failures = append(result.Failures, "duplicate required resource runtime")
		}
		required[key] = true
	}
	if len(required) == 0 {
		result.Missing = append(result.Missing, "resource population is empty")
	}
	// Input order is collector arrival order, not a cross-host clock order.
	// Group only contiguous images per complete Key; returning to an old cut is
	// an ABA defect even if the executable digest is a legitimate rollback.
	for _, s := range e.Samples {
		key := s.Stamp.Process.Key
		if !required[key] && key.Role != "fixture" {
			result.Failures = append(result.Failures, "sample belongs to an unregistered runtime")
		}
		chain := images[key]
		if len(chain) == 0 {
			keys = append(keys, key)
		}
		if len(chain) == 0 || !sameResourceImage(chain[len(chain)-1].samples[0], s) {
			for _, old := range chain {
				if sameResourceImage(old.samples[0], s) {
					result.Failures = append(result.Failures, "resource samples return to a retired image")
				}
			}
			chain = append(chain, resourceImage{})
		}
		last := len(chain) - 1
		chain[last].samples = append(chain[last].samples, s)
		images[key] = chain
		if !s.Complete {
			result.Missing = append(result.Missing, resourceLabel(s)+": incomplete resource census")
		}
		if !resourceStampValid(s.Stamp) || s.ExeSHA == (contract.Digest{}) || s.Goroutines < 0 || s.FDs < 0 ||
			(key.Role != "nats" && key.Role != "caddy" && s.Goroutines == 0) {
			result.Failures = append(result.Failures, resourceLabel(s)+": invalid observed identity or counters")
		}
		addResourceInput(s.Raw, result)
		for _, problem := range resourceProblems(s) {
			result.Failures = append(result.Failures, resourceLabel(s)+": "+problem)
		}
	}
	for _, key := range e.Profile.Required {
		if len(images[key]) == 0 {
			result.Missing = append(result.Missing, fmt.Sprintf("%s/%s/%s: missing observed runtime", key.Role, key.Host, key.Unit))
		}
	}
	return images, keys
}

func resourceStampValid(s contract.Stamp) bool {
	p := s.Process
	return resourceRole(p.Key.Role) && p.Key.Host != "" && p.Key.Unit != "" && p.Key.Domain != "" &&
		p.BootID != "" && p.PIDNS != "" && p.InstanceID != "" && p.PID > 0 && p.StartTicks > 0 &&
		s.Generation > 0 && s.Revision > 0 && s.Sequence > 0 && s.AtBootNS > 0 &&
		s.TxID != "" && s.CutID != "" && s.QueryNonce != "" && s.ScopeDigest != "" &&
		(s.Direction == "forward" || s.Direction == "reverse")
}

func resourceRole(role string) bool {
	switch role {
	case "agent", "broker", "nats", "caddy", "ctl", "fixture", "support":
		return true
	default:
		return false
	}
}

func sameResourceImage(a, b contract.Sample) bool {
	x, y := a.Stamp, b.Stamp
	return x.Process == y.Process && x.Generation == y.Generation && x.CutID == y.CutID &&
		x.TxID == y.TxID && x.Direction == y.Direction && x.ScopeDigest == y.ScopeDigest && a.ExeSHA == b.ExeSHA
}

func judgeResourceImage(e contract.Evidence, image resourceImage, final bool, result *contract.Result) {
	samples := image.samples
	base := samples[0]
	label := resourceLabel(base)
	rounds := 0
	pending := make(map[string]bool)
	known := make(map[string]bool)
	for _, r := range base.Resources {
		known[r.ID] = true
	}
	previousFloor := base
	nonces := make(map[string]bool)
	for i, s := range samples {
		if nonces[s.Stamp.QueryNonce] || (i > 0 && (s.Stamp.Sequence <= samples[i-1].Stamp.Sequence ||
			s.Stamp.AtBootNS <= samples[i-1].Stamp.AtBootNS || s.Stamp.Revision < samples[i-1].Stamp.Revision)) {
			result.Failures = append(result.Failures, resourceLabel(s)+": replayed or nonmonotonic resource sample")
		}
		nonces[s.Stamp.QueryNonce] = true
		if i == 0 {
			continue
		}
		judgeRetentionTransition(e, samples[i-1], s, result)
		for _, r := range s.Resources {
			if !known[r.ID] && !retainedResource(r) && r.Kind != "no-reexec-fence" {
				pending[r.ID] = true
			}
		}
		for _, r := range s.Resources {
			if retainedResource(r) && !known[r.ID] && !pending[r.ID] {
				result.Missing = append(result.Missing, resourceLabel(s)+": retained resource lacks an observed lifecycle origin")
			}
		}
		if !final || i != len(samples)-1 || rounds < resourceExerciseRounds || len(pending) != 0 {
			background, current := resourceBag(base.Resources, false, false), resourceBag(s.Resources, false, false)
			for r, count := range background {
				if current[r] < count {
					result.Failures = append(result.Failures, resourceLabel(s)+": original background reference disappeared or changed before final drain")
				}
			}
		}
		floor := sameResourceBag(base.Resources, s.Resources, false, false) &&
			sameResourceIDs(base.Consumers, s.Consumers) && sameResourceIDs(base.Temps, s.Temps) &&
			sameResourceIDs(base.Locks, s.Locks) && sameResourceIDs(base.OldImages, s.OldImages)
		if floor {
			// origin: hot-update S10 plan §4.1 / P03
			if s.Goroutines-base.Goroutines > resourceGoroutineTolerance {
				result.Failures = append(result.Failures, resourceLabel(s)+": same-image goroutine floor exceeds baseline +2")
			}
			if s.FDs != base.FDs {
				result.Failures = append(result.Failures, resourceLabel(s)+": same-image FD floor differs from baseline")
			}
			if len(pending) > 0 {
				for id := range pending {
					if !resourceCycleProven(e, previousFloor, s, id) {
						result.Missing = append(result.Missing, label+": cycle lacks IO and retirement for "+id)
					}
					known[id] = true
				}
				rounds++
				pending = make(map[string]bool)
				previousFloor = s
			}
		} else if i == len(samples)-1 && final && rounds >= resourceExerciseRounds && len(pending) == 0 {
			// The final natural drain is allowed to retire original long-cohort
			// references. It cannot erase an unfinished exercise or leak verdict.
			judgeFinalResourceCensus(base, s, result)
		}
	}
	role := base.Stamp.Process.Key.Role
	if role != "nats" && role != "caddy" && rounds < resourceExerciseRounds {
		result.Missing = append(result.Missing, label+": fewer than five same-image create/IO/settle cycles")
	}
	if len(pending) > 0 {
		result.Failures = append(result.Failures, label+": exercise resources never returned to the sealed background census")
	}
	last := samples[len(samples)-1]
	if final {
		judgeFinalResourceCensus(base, last, result)
	} else if !sameResourceBag(base.Resources, last.Resources, false, false) {
		result.Failures = append(result.Failures, label+": image left before its background census returned")
	}
}

func resourceCycleProven(e contract.Evidence, before, after contract.Sample, id string) bool {
	var effects, retirements []int64
	for _, event := range e.Intervals {
		if event.ResourceID != id || !sameResourceOwner(event.Stamp, after.Stamp) ||
			event.AtNS <= before.Stamp.AtBootNS || event.AtNS > after.Stamp.AtBootNS || !resourceRawValid(event.Raw) {
			continue
		}
		switch event.Event {
		case "read", "write", "wait", "accept", "primitive":
			effects = append(effects, event.AtNS)
		case "retire":
			retirements = append(retirements, event.AtNS)
		}
	}
	for _, effect := range effects {
		for _, retirement := range retirements {
			if effect < retirement {
				return true
			}
		}
	}
	return false
}

func judgeResourceTransition(old, next resourceImage, result *contract.Result) {
	a, b := old.samples[len(old.samples)-1], next.samples[0]
	role := b.Stamp.Process.Key.Role
	if b.Stamp.Generation <= a.Stamp.Generation || b.Stamp.CutID == a.Stamp.CutID ||
		b.Stamp.Process.BootID != a.Stamp.Process.BootID || b.Stamp.Process.PIDNS != a.Stamp.Process.PIDNS ||
		b.Stamp.AtBootNS <= a.Stamp.AtBootNS ||
		(role != "nats" && role != "caddy" && (a.Stamp.Process != b.Stamp.Process || b.Stamp.AtBootNS <= a.Stamp.AtBootNS)) {
		result.Failures = append(result.Failures, resourceLabel(b)+": generation transition lost original process or cut identity")
	}
	if !sameResourceBag(a.Resources, b.Resources, true, true) || !sameResourceIDs(a.Consumers, b.Consumers) ||
		!sameResourceIDs(a.Temps, b.Temps) || !sameResourceIDs(a.Locks, b.Locks) {
		result.Failures = append(result.Failures, resourceLabel(b)+": original background responsibility changed across exec")
	}
	if len(b.OldImages) != 0 {
		result.Failures = append(result.Failures, resourceLabel(b)+": successor retains old executable owners")
	}
	// Only runtime costs reset at exec. Original leases and terminal receipts
	// survive it and cannot be dropped while comparing just physical resources.
	judgeMetadataRetention(a, b, result)
}

func judgeFinalResourceCensus(base, last contract.Sample, result *contract.Result) {
	for _, r := range last.Resources {
		if r.State == "pending" {
			result.Failures = append(result.Failures, resourceLabel(last)+": final drain retains pending settlement responsibility "+r.ID)
		}
		if !fixedResource(r) && !retainedResource(r) && r.Kind != "no-reexec-fence" {
			result.Failures = append(result.Failures, resourceLabel(last)+": final drain retains live resource "+r.ID)
		}
	}
	if len(last.Consumers) != 0 || len(last.Temps) != 0 || len(last.OldImages) != 0 {
		result.Failures = append(result.Failures, resourceLabel(last)+": final consumer, temp or old-image set is nonempty")
	}
	var permanentLocks []string
	var fixedBefore, fixedAfter []contract.Resource
	for _, r := range base.Resources {
		if fixedResource(r) {
			fixedBefore = append(fixedBefore, r)
			if r.Kind == "lock" {
				permanentLocks = append(permanentLocks, r.ID)
			}
		}
	}
	for _, r := range last.Resources {
		if fixedResource(r) {
			fixedAfter = append(fixedAfter, r)
		}
	}
	if !sameResourceIDs(permanentLocks, last.Locks) || !sameResourceBag(fixedBefore, fixedAfter, false, false) {
		result.Failures = append(result.Failures, resourceLabel(last)+": final census changed fixed observer or permanent store-lock ownership")
	}
	if last.Goroutines-base.Goroutines > resourceGoroutineTolerance {
		result.Failures = append(result.Failures, resourceLabel(last)+": final same-image goroutine floor exceeds baseline +2")
	}
}

func judgeRetentionTransition(e contract.Evidence, before, after contract.Sample, result *contract.Result) {
	judgeMetadataRetention(before, after, result)
	for _, previous := range before.Resources {
		found := false
		for _, current := range after.Resources {
			if previous.ID == current.ID && previous.Kind == current.Kind && previous.Domain == current.Domain {
				found = true
			}
		}
		if found || previous.Kind == "no-reexec-fence" {
			continue
		}
		if !resourceRetirementProven(e, before, after, previous.ID) {
			result.Missing = append(result.Missing, resourceLabel(after)+": disappeared resource lacks product retirement or sweep witness")
		}
	}
}

func judgeMetadataRetention(before, after contract.Sample, result *contract.Result) {
	for _, previous := range before.Resources {
		if resourceClass(previous.Kind) != "metadata" || previous.Kind == "no-reexec-fence" {
			continue
		}
		found := false
		for _, current := range after.Resources {
			if previous.ID == current.ID && previous.Kind == current.Kind && previous.Domain == current.Domain {
				found = true
				if previous.ReleaseAfterNS != current.ReleaseAfterNS {
					result.Failures = append(result.Failures, resourceLabel(after)+": original retention deadline changed")
				}
			}
		}
		if !found && retainedResource(previous) && (previous.ReleaseAfterNS == 0 || after.Stamp.AtBootNS < previous.ReleaseAfterNS) {
			result.Failures = append(result.Failures, resourceLabel(after)+": retained metadata collected before original expiry")
		}
	}
}

func resourceRetirementProven(e contract.Evidence, before, after contract.Sample, id string) bool {
	for _, event := range e.Intervals {
		if event.ResourceID == id && event.Event == "retire" && sameResourceOwner(event.Stamp, after.Stamp) &&
			event.AtNS > before.Stamp.AtBootNS && event.AtNS <= after.Stamp.AtBootNS && resourceRawValid(event.Raw) {
			for _, r := range before.Resources {
				if r.ID == id && retainedResource(r) && (r.ReleaseAfterNS == 0 || event.AtNS < r.ReleaseAfterNS) {
					return false
				}
			}
			return true
		}
	}
	return false
}

func judgeReleasedResources(e contract.Evidence, result *contract.Result) {
	for _, view := range e.Views {
		r := view.Released
		if !r.Known || !r.Complete {
			continue // P01 owns completion coverage; a missing receipt proves no retirement.
		}
		addResourceInput(r.Evidence, result)
		for _, s := range e.Samples {
			if s.Stamp.Process.Key != r.Stamp.Process.Key || s.Stamp.Process.BootID != r.Stamp.Process.BootID || s.Stamp.AtBootNS < r.Stamp.AtBootNS {
				continue
			}
			for _, id := range r.ResourceIDs {
				for _, held := range s.Resources {
					if held.ID == id && held.Owner.Generation < r.Stamp.Generation {
						result.Failures = append(result.Failures, resourceLabel(s)+": released receipt still has an old owner reference")
					}
				}
			}
		}
	}
}

func resourceRawValid(raw contract.RawRef) bool {
	return raw.Path != "" && raw.Schema != "" && raw.Bytes > 0 && raw.SHA != (contract.Digest{})
}

func addResourceInput(raw contract.RawRef, result *contract.Result) {
	if !resourceRawValid(raw) {
		result.Missing = append(result.Missing, "resource observation lacks complete raw reference")
		return
	}
	for _, known := range result.Inputs {
		if known == raw {
			return
		}
		if known.Path == raw.Path && known != raw {
			result.Failures = append(result.Failures, "immutable resource raw reference changed")
		}
	}
	result.Inputs = append(result.Inputs, raw)
}
