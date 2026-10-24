// plan.go validates the sealed population and the seven-row rollout manifest.
// A failed or offline runtime remains required; a smaller discovered roster
// cannot silently redefine success. Planning does not perform any host action.
// origin: hot-update S10 plan §2.2 / §3.1 / §10.2 / P01
package acceptance

import (
	"errors"
	"fmt"
	"slices"

	c "github.com/LinZiyang666/tether/test/hotupdate/acceptance/contract"
)

var _ c.PlanFunc = Plan

// The two mixed rows contain separately directed plans. These barriers require
// every infra reverse owner to retire before any tether forward action starts.
var rolloutRows = [][]string{
	{"agent", "broker", "nats", "caddy"},
	{"caddy", "nats", "broker", "agent"},
	{"agent", "broker", "nats", "caddy"},
	{"caddy", "nats", "agent", "broker"},
	{"agent", "broker", "nats", "caddy"},
	{"caddy", "nats", "agent", "broker"},
	{"nats", "caddy"},
}

// Plan returns a dependency-ordered copy of Profile.Steps. The manifest is an
// input sealed by the fixture, not a plan reconstructed from a surviving fleet.
// Artifact references are keyed by (role, ID); roles may share mechanism labels.
func Plan(p c.Profile) ([]c.Step, error) {
	if _, err := c.EncodeJSON("profile", p); err != nil {
		return nil, errors.New("invalid fullstack profile")
	}
	if p.EndNS <= p.StartNS || p.ProbeRate != 10 || len(p.Steps) == 0 ||
		!validRaw(p.CommandBindings) || p.BudgetDigest == (c.Digest{}) || p.WorkloadDigest == (c.Digest{}) {
		return nil, errors.New("fullstack profile lacks a sealed rollout window")
	}
	population := make(map[c.Key]bool)
	counts := make(map[string]int)
	sids, nids, domains, units := map[string]bool{}, map[string]bool{}, map[string]bool{}, map[string]bool{}
	for _, key := range p.Required {
		population[key] = true
		counts[key.Role]++
		if key.Role == "agent" {
			domain, unit := key.Host+"\x00"+key.Domain, key.Host+"\x00"+key.Unit
			if key.SID == "" || key.NID == "" || sids[key.SID] || nids[key.NID] || domains[domain] || units[unit] {
				return nil, errors.New("fullstack agents must have independent identities and installations")
			}
			sids[key.SID], nids[key.NID], domains[domain], units[unit] = true, true, true, true
		}
	}
	// origin: hot-update S10 plan §3.1 / TestFullstackPlanRequiresEveryRuntime
	if counts["agent"] != 4 || counts["nats"] == 0 || counts["caddy"] == 0 ||
		(p.Topology == "single" && counts["broker"] != 1) || (p.Topology == "quorum" && counts["broker"] < 3) {
		return nil, errors.New("fullstack profile is missing a required runtime")
	}
	artifacts := make(map[string]c.Artifact)
	for _, artifact := range p.Artifacts {
		id := artifact.Role + "\x00" + artifact.ID
		if _, exists := artifacts[id]; exists || artifact.Binary == (c.Digest{}) {
			return nil, errors.New("fullstack artifact identity is ambiguous or empty")
		}
		artifacts[id] = artifact
	}
	byID := make(map[string]c.Step)
	groups := make(map[string][]string)
	for _, step := range p.Steps {
		if _, exists := byID[step.ID]; exists || !population[step.Target] || step.DeadlineNS <= p.StartNS || step.DeadlineNS > p.EndNS {
			return nil, errors.New("fullstack step has an invalid identity, target or deadline")
		}
		row := slices.Index([]string{"R1", "R2", "R3", "R4", "R5", "R6", "R7"}, step.ParentID)
		if row < 0 || !slices.Contains(rolloutRows[row], step.Target.Role) {
			return nil, errors.New("fullstack step is outside the frozen rollout")
		}
		reverse := row == 1 || ((row == 3 || row == 5) && (step.Target.Role == "nats" || step.Target.Role == "caddy"))
		direction, cohort, leg := "forward", "C0", "leg2"
		if reverse {
			direction = "reverse"
		}
		if row == 0 || row == 3 {
			leg = "leg1"
		}
		if row >= 3 {
			cohort = "C1"
		}
		if step.Direction != direction || step.Cohort != cohort || step.Leg != leg ||
			(reverse && step.Action != "rollback") || (!reverse && step.Action != "upgrade" && step.Action != "reload") {
			return nil, errors.New("fullstack component direction or transfer leg is incorrect")
		}
		byID[step.ID] = step
		group := step.ParentID + "/" + step.Target.Role
		groups[group] = append(groups[group], step.ID)
	}
	ordered := make([]c.Step, 0, len(p.Steps))
	ancestors := make(map[string]map[string]bool)
	for len(ordered) != len(p.Steps) {
		progress := false
		for _, step := range p.Steps {
			if ancestors[step.ID] != nil {
				continue
			}
			ready, seen := true, make(map[string]bool)
			for _, dep := range step.DependsOn {
				if _, exists := byID[dep]; !exists || dep == step.ID || seen[dep] {
					return nil, errors.New("fullstack dependency is unknown, repeated or self-referential")
				}
				seen[dep] = true
				ready = ready && ancestors[dep] != nil
			}
			if !ready {
				continue
			}
			closure := make(map[string]bool)
			for dep := range seen {
				closure[dep] = true
				for parent := range ancestors[dep] {
					closure[parent] = true
				}
			}
			ancestors[step.ID] = closure
			step.DependsOn = slices.Clone(step.DependsOn)
			ordered = append(ordered, step)
			progress = true
		}
		if !progress {
			return nil, errors.New("fullstack dependencies contain a cycle")
		}
	}
	previous := []string{}
	for row, roles := range rolloutRows {
		for _, role := range roles {
			ids := groups[fmt.Sprintf("R%d/%s", row+1, role)]
			seen := make(map[c.Key]bool)
			for _, id := range ids {
				key := byID[id].Target
				if seen[key] {
					return nil, errors.New("fullstack row repeats a runtime")
				}
				seen[key] = true
				for _, dep := range previous {
					if !ancestors[id][dep] {
						return nil, errors.New("fullstack row lacks a component retirement barrier")
					}
				}
			}
			// origin: hot-update S10 plan §2.2 / TestFullstackPlanRequiresEveryRuntime
			if len(seen) != counts[role] {
				return nil, errors.New("fullstack row omits a required runtime")
			}
			previous = ids
		}
	}
	for key := range population {
		if !slices.Contains(rolloutRows[0], key.Role) {
			continue // ctl, fixture and support are observed, not counted as tether successors.
		}
		if err := validateArtifactChain(key, ordered, artifacts); err != nil {
			return nil, err
		}
	}
	return ordered, nil
}

func validateArtifactChain(key c.Key, steps []c.Step, artifacts map[string]c.Artifact) error {
	var chain []c.Artifact
	for _, step := range steps {
		if step.Target != key {
			continue
		}
		from, fromOK := artifacts[key.Role+"\x00"+step.From]
		to, toOK := artifacts[key.Role+"\x00"+step.To]
		if !fromOK || !toOK || from.Binary == to.Binary || from.Epoch != to.Epoch ||
			!slices.Contains(from.ReadEpochs, to.Epoch) || !slices.Contains(to.ReadEpochs, from.Epoch) {
			return errors.New("fullstack step has a no-op or unreadable artifact transition")
		}
		if len(chain) == 0 {
			chain = append(chain, from)
		} else if chain[len(chain)-1].ID != from.ID {
			return errors.New("fullstack artifact chain is discontinuous")
		}
		chain = append(chain, to)
	}
	if key.Role == "agent" || key.Role == "broker" {
		if len(chain) != 7 || chain[2].Binary != chain[0].Binary {
			return errors.New("fullstack chain lacks the complete current rollback")
		}
		novel := map[c.Digest]bool{chain[0].Binary: true}
		for _, index := range []int{1, 3, 4, 5, 6} {
			// origin: hot-update S10 plan §2.2 / TestFullstackPlanRequiresEveryRuntime
			if novel[chain[index].Binary] {
				return errors.New("fullstack chain counts an ABA image as a novel successor")
			}
			novel[chain[index].Binary] = true
		}
	} else {
		if len(chain) != 8 {
			return errors.New("fullstack infra chain lacks the explicit final pin")
		}
		for index, artifact := range chain {
			if artifact.Binary != chain[index%2].Binary {
				return errors.New("fullstack infra chain differs from the two-version catalog")
			}
		}
	}
	return nil
}
