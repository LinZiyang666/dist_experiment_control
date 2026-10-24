// plan_test.go attacks the sealed rollout and current-completion predicates.
// Synthetic, schema-valid observations test the judge; they are not receipts
// for P18's real commands or P24's real transactions.
// origin: hot-update S10 plan §2.2 / §5.2 / §10.2 / P01
package acceptance

import (
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"reflect"
	"slices"
	"strings"
	"testing"

	c "github.com/LinZiyang666/tether/test/hotupdate/acceptance/contract"
)

// origin: hot-update S10 plan §3.1 / §10.2 / P01
func TestFullstackPlanRequiresEveryRuntime(t *testing.T) {
	p := rolloutProfile()
	before := cloneProfile(t, p)
	ordered, err := Plan(p)
	if err != nil || len(ordered) != 44 || !reflect.DeepEqual(p, before) {
		t.Fatalf("sealed seven-row plan: steps=%d err=%v mutated=%v", len(ordered), err, !reflect.DeepEqual(p, before))
	}
	ordered[4].DependsOn[0] = "changed-return-storage"
	if !reflect.DeepEqual(p, before) {
		t.Fatal("plan aliases the sealed input")
	}
	t.Run("quorum includes every peer", func(t *testing.T) {
		q := rolloutProfile()
		q.Topology = "quorum"
		for i := 1; i < 4; i++ {
			key := q.Required[4]
			key.Host, key.Unit, key.Domain, key.NID = fmt.Sprintf("broker-%d", i), fmt.Sprintf("broker-%d.service", i), fmt.Sprintf("broker-domain-%d", i), fmt.Sprintf("b%d", i)
			q.Required = append(q.Required, key)
		}
		q.Steps = rolloutSteps(q)
		steps, err := Plan(q)
		if err != nil || len(steps) != 62 {
			t.Fatalf("all voters and non-voters: %d %v", len(steps), err)
		}
		q.Required = slices.Delete(q.Required, 8, 10)
		q.Steps = rolloutSteps(q)
		if _, err := Plan(q); err == nil {
			t.Fatal("quorum profile accepted fewer than three brokers")
		}
	})
	cases := []struct {
		name   string
		change func(*c.Profile)
	}{
		{"missing agent", func(p *c.Profile) { p.Required = p.Required[1:]; p.Steps = rolloutSteps(*p) }},
		{"missing broker", func(p *c.Profile) { p.Required = slices.Delete(p.Required, 4, 5); p.Steps = rolloutSteps(*p) }},
		{"missing nats", func(p *c.Profile) { p.Required = slices.Delete(p.Required, 5, 6); p.Steps = rolloutSteps(*p) }},
		{"missing caddy", func(p *c.Profile) { p.Required = slices.Delete(p.Required, 6, 7); p.Steps = rolloutSteps(*p) }},
		{"missing runtime in one row", func(p *c.Profile) {
			removed := p.Steps[0].ID
			p.Steps = p.Steps[1:]
			for i := range p.Steps {
				p.Steps[i].DependsOn = slices.DeleteFunc(p.Steps[i].DependsOn, func(id string) bool { return id == removed })
			}
		}},
		{"empty plan", func(p *c.Profile) { p.Steps = nil }},
		{"shared agent installation", func(p *c.Profile) { p.Required[1].Domain = p.Required[0].Domain; p.Steps = rolloutSteps(*p) }},
		{"shared agent sid", func(p *c.Profile) { p.Required[1].SID = p.Required[0].SID; p.Steps = rolloutSteps(*p) }},
		{"duplicate step", func(p *c.Profile) { p.Steps = append(p.Steps, p.Steps[0]) }},
		{"unknown target", func(p *c.Profile) { p.Steps[0].Target.UID++ }},
		{"wrong mixed direction", func(p *c.Profile) {
			for i := range p.Steps {
				if p.Steps[i].ParentID == "R4" && p.Steps[i].Target.Role == "nats" {
					p.Steps[i].Direction = "forward"
					return
				}
			}
		}},
		{"wrong receiver leg", func(p *c.Profile) { p.Steps[7].Leg = "leg1" }},
		{"missing retirement dependency", func(p *c.Profile) { p.Steps[4].DependsOn = nil }},
		{"dependency cycle", func(p *c.Profile) { p.Steps[0].DependsOn = []string{p.Steps[4].ID} }},
		{"unknown dependency", func(p *c.Profile) { p.Steps[0].DependsOn = []string{"absent"} }},
		{"repeated dependency", func(p *c.Profile) { p.Steps[4].DependsOn = append(p.Steps[4].DependsOn, p.Steps[4].DependsOn[0]) }},
		{"deadline outside sealed window", func(p *c.Profile) { p.Steps[0].DeadlineNS = p.EndNS + 1 }},
		{"probe rate reduced", func(p *c.Profile) { p.ProbeRate = 9 }},
		{"unsealed workload", func(p *c.Profile) { p.WorkloadDigest = c.Digest{} }},
		{"unsealed budget", func(p *c.Profile) { p.BudgetDigest = c.Digest{} }},
		{"empty command binding", func(p *c.Profile) { p.CommandBindings.Bytes = 0 }},
		{"artifact source discontinuity", func(p *c.Profile) { p.Steps[7].From = "A" }},
		{"artifact wrong role", func(p *c.Profile) { p.Artifacts[0].Role = "ctl" }},
		{"same sha no op", func(p *c.Profile) { p.Artifacts[1].Binary = p.Artifacts[0].Binary }},
		{"aba counted as novel", func(p *c.Profile) { p.Artifacts[3].Binary = p.Artifacts[1].Binary }},
		{"incomplete reverse target", func(p *c.Profile) {
			for i := range p.Steps {
				if p.Steps[i].ParentID == "R2" && p.Steps[i].Target.Role == "agent" {
					p.Steps[i].To = "T2"
					return
				}
			}
		}},
		{"infra third version", func(p *c.Profile) {
			extra := p.Artifacts[len(p.Artifacts)-1]
			extra.ID = "C"
			extra.Binary = sha256.Sum256([]byte("third-version"))
			p.Artifacts = append(p.Artifacts, extra)
			p.Steps[len(p.Steps)-1].To = "C"
		}},
		{"explicit final pin omitted", func(p *c.Profile) { p.Steps = p.Steps[:len(p.Steps)-2] }},
		{"reader epoch absent", func(p *c.Profile) { p.Artifacts[1].ReadEpochs = []uint64{2} }},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			bad := cloneProfile(t, p)
			tc.change(&bad)
			if _, err := Plan(bad); err == nil {
				t.Fatal("invalid sealed plan accepted")
			}
		})
	}
	// A scrambled input is legal only when its dependencies still prove the same
	// complete barriers. The planner must not assume slice order establishes them.
	scrambled := cloneProfile(t, p)
	slices.Reverse(scrambled.Steps)
	got, err := Plan(scrambled)
	if err != nil {
		t.Fatalf("dependency order: %v", err)
	}
	seen := make(map[string]bool)
	for _, step := range got {
		for _, dep := range step.DependsOn {
			if !seen[dep] {
				t.Fatal("dependency sorted after its action")
			}
		}
		seen[step.ID] = true
	}
	if len(seen) != len(p.Steps) {
		t.Fatal("scrambling lost a planned action")
	}
}

// origin: hot-update S10 plan §5.2 / §10.2 / P01
func TestFullstackCompletionRequiresCurrentStamp(t *testing.T) {
	e := rolloutEvidence(t)
	r, err := Judge(e)
	if err != nil || r.RC != 0 || r.Verdict != "GREEN" || len(r.Inputs) != len(e.Raw) {
		t.Fatalf("current seven-row completion: %+v %v", r, err)
	}
	before := cloneEvidence(t, e)
	_, _ = Judge(e)
	if !reflect.DeepEqual(e, before) {
		t.Fatal("judge modifies archived evidence")
	}
	t.Run("sequence remains monotonic across cuts", func(t *testing.T) {
		v := e.Views[0]
		var o transactionObservation
		var h transactionHistory
		if json.Unmarshal(v.Current, &o) != nil || json.Unmarshal(v.Historical, &h) != nil {
			t.Fatal("invalid fixture projection")
		}
		previous := v.Stamp
		previous.Generation, previous.CutID, previous.TxID = 0, "prior-cut", "prior-tx"
		previous.Sequence++
		if freshView(v, o, h, previous) {
			t.Fatal("same-process sequence regressed across exec")
		}
		previous.Sequence = 1
		previous.AtBootNS++
		if freshView(v, o, h, previous) {
			t.Fatal("same-process boot clock regressed across exec")
		}
	})
	cases := []struct {
		name   string
		change func(*c.Evidence)
	}{
		{"historical completed current ready", func(e *c.Evidence) {
			editCurrent(t, &e.Views[0], func(o *transactionObservation) {
				o.Current.Phase, o.Current.CutStage, o.Current.Outcome = "ready", "none", "pending"
			})
			e.Views[0].Phase, e.Views[0].Outcome = "ready", "pending"
		}},
		{"historical completed current absent", func(e *c.Evidence) {
			editCurrent(t, &e.Views[0], func(o *transactionObservation) {
				o.OwnerKnown = false
				o.Census.Known = false
				o.Census.Complete = false
			})
		}},
		{"completion before dependency retired", func(e *c.Evidence) { e.Views[0], e.Views[4] = e.Views[4], e.Views[0] }},
		{"wrong receipt nonce", func(e *c.Evidence) { e.Views[0].Control.Stamp.QueryNonce = "old-query" }},
		{"wrong raw nonce", func(e *c.Evidence) {
			editCurrent(t, &e.Views[0], func(o *transactionObservation) { o.Nonce = "replayed-query" })
		}},
		{"reverse reuses forward stamp", func(e *c.Evidence) { e.Views[7].Released.Stamp.Direction = "forward" }},
		{"wrong producer pid", func(e *c.Evidence) { e.Views[0].Resources.Stamp.Process.PID++ }},
		{"wrong producer namespace", func(e *c.Evidence) { e.Views[0].Resources.Stamp.Process.PIDNS = "other-namespace" }},
		{"wrong revision", func(e *c.Evidence) { e.Views[0].Released.Stamp.Revision++ }},
		{"wrong generation", func(e *c.Evidence) { e.Views[0].Control.Stamp.Generation++ }},
		{"wrong transaction", func(e *c.Evidence) { e.Views[0].Control.Stamp.TxID = "other-tx" }},
		{"wrong cut", func(e *c.Evidence) { e.Views[0].Control.Stamp.CutID = "other-cut" }},
		{"wrong scope", func(e *c.Evidence) { e.Views[0].Resources.Stamp.ScopeDigest = "other-scope" }},
		{"future receipt sequence", func(e *c.Evidence) { e.Views[0].Control.Stamp.Sequence = e.Views[0].Stamp.Sequence + 1 }},
		{"release before control", func(e *c.Evidence) { e.Views[0].Released.Stamp.Sequence = 1 }},
		{"nonce replay", func(e *c.Evidence) { e.Views = append(e.Views[:1], append([]c.View{e.Views[0]}, e.Views[1:]...)...) }},
		{"unknown projected receipt", func(e *c.Evidence) { e.Views[0].Resources.Known = false }},
		{"missing release", func(e *c.Evidence) { e.Views[0].Released.Complete = false }},
		{"unknown raw receipt census", func(e *c.Evidence) {
			editCurrent(t, &e.Views[0], func(o *transactionObservation) { o.Current.Control.Census.Known = false })
		}},
		{"raw receipt disagrees with projection", func(e *c.Evidence) {
			editCurrent(t, &e.Views[0], func(o *transactionObservation) { o.Current.Resources.Stamp.Nonce = "old-query" })
		}},
		{"raw current scope loses resource", func(e *c.Evidence) {
			editCurrent(t, &e.Views[0], func(o *transactionObservation) { o.Census.Resources = nil })
		}},
		{"projected expected set loses resource", func(e *c.Evidence) { e.Views[0].Resources.ResourceIDs = nil }},
		{"old owner still lives", func(e *c.Evidence) {
			editCurrent(t, &e.Views[0], func(o *transactionObservation) { o.Census.OldOwners = []transactionOwner{o.Census.Stamp.Producer} })
		}},
		{"illegal success before release", func(e *c.Evidence) {
			editCurrent(t, &e.Views[0], func(o *transactionObservation) { o.Current.CutStage = "active" })
		}},
		{"wrong executed sha", func(e *c.Evidence) { e.Samples[0].ExeSHA = sha256.Sum256([]byte("wrong-executable")) }},
		{"wrong reverse recorded prev", func(e *c.Evidence) {
			editHistory(t, &e.Views[7], func(h map[string]any) { h["record"].(map[string]any)["recorded_previous_sha"] = "other-prev" })
		}},
		{"missing runtime views", func(e *c.Evidence) {
			key := e.Profile.Required[3]
			e.Views = slices.DeleteFunc(e.Views, func(v c.View) bool { return v.Stamp.Process.Key == key })
		}},
		{"missing support census", func(e *c.Evidence) { e.Samples = e.Samples[:len(e.Samples)-1] }},
		{"unknown fleet sample", func(e *c.Evidence) {
			extra := e.Samples[0]
			extra.Stamp.Process.Key.UID++
			e.Samples = append(e.Samples, extra)
		}},
		{"missing independent executable sample", func(e *c.Evidence) { e.Samples = e.Samples[1:] }},
		{"unknown independent executable sample", func(e *c.Evidence) { e.Samples[0].Complete = false }},
		{"missing raw archive", func(e *c.Evidence) { e.Raw = nil }},
		{"missing receipt raw", func(e *c.Evidence) {
			e.Raw = slices.DeleteFunc(e.Raw, func(r c.RawRef) bool { return r == e.Views[0].Control.Evidence })
		}},
		{"unplanned runtime", func(e *c.Evidence) { e.Views[0].Stamp.Process.Key.UID++ }},
		{"last success does not erase defer", func(e *c.Evidence) { e.Views[0].Deferred = []string{"upgrade_deferred"} }},
		{"omitted ordered reason still fails", func(e *c.Evidence) { e.Views[0].Code = "legacy_session_busy" }},
		{"historical journal reason still fails", func(e *c.Evidence) {
			editHistory(t, &e.Views[0], func(h map[string]any) { h["record"].(map[string]any)["reason"] = "upgrade_deferred" })
		}},
		{"interrupting grant cannot sign hot completion", func(e *c.Evidence) {
			editHistory(t, &e.Views[0], func(h map[string]any) {
				h["record"].(map[string]any)["grant"].(map[string]any)["mode"] = "legacy_interrupting"
			})
		}},
		{"broker refusal before later success", func(e *c.Evidence) {
			e.Attempts = []c.Attempt{{SlotID: "slot", CallID: "call", Index: 1, Phase: "result", StartedNS: 1, FinishedNS: 2, Sent: true, Completed: true, RC: 75, Code: "infra_not_ready", Raw: e.Raw[0]}, {SlotID: "slot", CallID: "call", Index: 2, Phase: "result", StartedNS: 2, FinishedNS: 3, Sent: true, Completed: true, Raw: e.Raw[0]}}
		}},
		{"unknown result before later success", func(e *c.Evidence) { e.Attempts = []c.Attempt{{Phase: "result", Raw: e.Raw[0]}} }},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			bad := cloneEvidence(t, e)
			tc.change(&bad)
			r, err := Judge(bad)
			if err != nil {
				t.Fatalf("control must reach the predicate, not schema/setup rejection: %v", err)
			}
			if r.RC == 0 {
				t.Fatal("invalid current completion accepted")
			}
		})
	}
	t.Run("invalid schema is setup red", func(t *testing.T) {
		bad := cloneEvidence(t, e)
		bad.Views[0].Current = json.RawMessage(`{"command":"touch marker"}`)
		r, err := Judge(bad)
		if err == nil || r.RC != 2 {
			t.Fatalf("invalid schema: %+v %v", r, err)
		}
	})
	t.Run("refusal dominates missing coverage", func(t *testing.T) {
		bad := cloneEvidence(t, e)
		bad.Views[0].Code = "upgrade_deferred"
		bad.Samples = nil
		r, err := Judge(bad)
		if err != nil || r.RC != 1 || len(r.Failures) == 0 || len(r.Missing) == 0 {
			t.Fatalf("native assertion precedence: %+v %v", r, err)
		}
	})
}

func rolloutProfile() c.Profile {
	p := c.Profile{ID: "synthetic-seven-row", Topology: "single", Regime: "quiet", Tier: "hermetic", EvidenceRoot: "raw", ProbeRate: 10, StartNS: 0, EndNS: 1_000_000,
		CommandBindings: fixtureRaw("bindings", "command_bindings"), BudgetDigest: sha256.Sum256([]byte("budget")), WorkloadDigest: sha256.Sum256([]byte("workload"))}
	for i := 0; i < 4; i++ {
		p.Required = append(p.Required, c.Key{Role: "agent", Host: fmt.Sprintf("agent-host-%d", i/2), Unit: fmt.Sprintf("agent-%d.service", i), SID: fmt.Sprintf("sid-%d", i), NID: fmt.Sprintf("a%d", i), Domain: fmt.Sprintf("agent-domain-%d", i), UID: uint32(1000 + i)})
	}
	for _, role := range []string{"broker", "nats", "caddy", "support"} {
		p.Required = append(p.Required, c.Key{Role: role, Host: "broker-host", Unit: role + ".service", NID: role + "-0", Domain: role + "-domain", UID: 2000})
	}
	for _, role := range []string{"agent", "broker", "nats", "caddy"} {
		ids := []string{"A", "B"}
		if role == "agent" || role == "broker" {
			ids = []string{"T0", "T1", "T2", "T3", "T4", "T5"}
		}
		for _, id := range ids {
			d := sha256.Sum256([]byte(role + id))
			p.Artifacts = append(p.Artifacts, c.Artifact{ID: id, Role: role, Release: id, SourceCommit: "synthetic", Toolchain: "synthetic", ImageID: "synthetic", Tree: d, Flags: d, Archive: d, Binary: d, Vendor: d, CopyManifest: d, Epoch: 1, ReadEpochs: []uint64{1}, Domains: []string{"control"}})
		}
	}
	p.Steps = rolloutSteps(p)
	return p
}

func rolloutSteps(p c.Profile) []c.Step {
	roles := [][]string{{"agent", "broker", "nats", "caddy"}, {"caddy", "nats", "broker", "agent"}, {"agent", "broker", "nats", "caddy"}, {"caddy", "nats", "agent", "broker"}, {"agent", "broker", "nats", "caddy"}, {"caddy", "nats", "agent", "broker"}, {"nats", "caddy"}}
	previous, steps := []string{}, []c.Step{}
	current := make(map[c.Key]string)
	for row, order := range roles {
		for _, role := range order {
			group := []string{}
			for i, key := range p.Required {
				if key.Role != role {
					continue
				}
				from := current[key]
				if from == "" {
					from = "A"
					if role == "agent" || role == "broker" {
						from = "T0"
					}
				}
				to := []string{"T1", "T0", "T2", "T3", "T4", "T5", "T5"}[row]
				direction, action := "forward", "upgrade"
				if role == "nats" || role == "caddy" {
					to = []string{"B", "A", "B", "A", "B", "A", "B"}[row]
				}
				if row == 1 || ((row == 3 || row == 5) && (role == "nats" || role == "caddy")) {
					direction, action = "reverse", "rollback"
				}
				cohort, leg := "C0", "leg2"
				if row >= 3 {
					cohort = "C1"
				}
				if row == 0 || row == 3 {
					leg = "leg1"
				}
				id := fmt.Sprintf("R%d.%s.%d", row+1, role, i)
				steps = append(steps, c.Step{ID: id, ParentID: fmt.Sprintf("R%d", row+1), Action: action, Direction: direction, Cohort: cohort, Leg: leg, Target: key, From: from, To: to, DependsOn: slices.Clone(previous), DeadlineNS: p.EndNS - 100 + int64(len(steps))})
				group = append(group, id)
				current[key] = to
			}
			previous = group
		}
	}
	return steps
}

func rolloutEvidence(t *testing.T) c.Evidence {
	t.Helper()
	e := c.Evidence{Profile: rolloutProfile()}
	generation := make(map[c.Key]uint64)
	for i, step := range e.Profile.Steps {
		generation[step.Target]++
		process := c.Process{Key: step.Target, BootID: "boot", PIDNS: "pidns", InstanceID: keyLabel(step.Target), PID: 100 + int(step.Target.UID), StartTicks: 1000}
		if step.Target.Role == "nats" || step.Target.Role == "caddy" {
			process.PID += int(generation[step.Target])
		}
		stamp := c.Stamp{Process: process, TxID: "tx-" + step.ID, CutID: "cut-" + step.ID, Direction: step.Direction, QueryNonce: "query-" + step.ID, ScopeDigest: "scope-" + step.ID, Generation: generation[step.Target], Revision: uint64(i + 1), Sequence: generation[step.Target]*10 + 4, AtBootNS: int64(100 + i)}
		handle := transactionHandle{transactionKey{step.Target.Domain, step.Target.Role, step.Target.SID, step.Target.NID, step.Target.UID}, stamp.TxID, stamp.CutID, stamp.Generation, stamp.Revision}
		owner := transactionOwner{process.BootID, process.PID, process.StartTicks, process.InstanceID}
		upStamp := transactionStamp{handle, owner, stamp.QueryNonce, stamp.Sequence, stamp.AtBootNS, stamp.ScopeDigest}
		resource := transactionResource{"control", "resource-" + step.Target.Role, "origin-" + step.Target.Role, owner, stamp.Generation, "live", "current"}
		census := transactionCensus{Stamp: upStamp, Known: true, Complete: true, Resources: []transactionResource{resource}, ExpectedDigest: stamp.ScopeDigest}
		from, to := stepArtifacts(e.Profile, step)
		attempt := transactionAttempt{Handle: handle, Direction: step.Direction, SourceSHA: digestText(from.Binary), TargetSHA: digestText(to.Binary), SourceGeneration: stamp.Generation - 1, Phase: "completed", CutStage: "released", Outcome: "success"}
		attempt.Scope.ID, attempt.Scope.Participants, attempt.Scope.Domains, attempt.Scope.Expected = stamp.ScopeDigest, []transactionKey{handle.Key}, []string{"control"}, []transactionResource{resource}
		v := c.View{Stamp: stamp, Phase: "completed", Outcome: "success", Raw: fixtureRaw("view-"+step.ID, "s8_view")}
		projected := []*c.Receipt{&v.Control, &v.Resources, &v.Released}
		actual := []*transactionReceipt{&attempt.Control, &attempt.Resources, &attempt.Released}
		for j, kind := range []string{"control_ready", "resources_migrated", "old_generation_released"} {
			receiptStamp := stamp
			receiptStamp.Sequence = stamp.Sequence - 3 + uint64(j)
			receiptStamp.QueryNonce = fmt.Sprintf("seal-%s-%d", step.ID, j)
			raw := fixtureRaw(kind+"-"+step.ID, "s8_receipt")
			*projected[j] = c.Receipt{Kind: kind, Stamp: receiptStamp, Known: true, Complete: true, RunningSHA: to.Binary, ResourceIDs: []string{resource.ID}, Evidence: raw}
			actualStamp := upStamp
			actualStamp.Sequence = receiptStamp.Sequence
			actualStamp.Nonce = receiptStamp.QueryNonce
			actualCensus := census
			actualCensus.Stamp = actualStamp
			*actual[j] = transactionReceipt{kind, actualStamp, true, true, digestText(to.Binary), actualCensus, digestText(raw.SHA)}
			e.Raw = append(e.Raw, raw)
		}
		o := transactionObservation{stamp.QueryNonce, stamp.AtBootNS, attempt, census, true, digestText(to.Binary), digestText(to.Binary)}
		v.Current = jsonBytes(t, o)
		v.Historical = historyBytes(t, step, attempt, owner)
		if step.Direction == "reverse" {
			v.Phase, v.Outcome = "rolled_back", "rolled_back"
		}
		sample := c.Sample{Stamp: stamp, ExeSHA: to.Binary, Goroutines: 10, FDs: 8, Complete: true, Raw: fixtureRaw("sample-"+step.ID, "sample")}
		e.Raw = append(e.Raw, v.Raw, sample.Raw)
		e.Views = append(e.Views, v)
		e.Samples = append(e.Samples, sample)
	}
	key := e.Profile.Required[7]
	ref := fixtureRaw("support-sample", "sample")
	e.Raw = append(e.Raw, ref)
	e.Samples = append(e.Samples, c.Sample{Stamp: c.Stamp{Process: c.Process{Key: key, BootID: "boot", PIDNS: "pidns", InstanceID: "support", PID: 500, StartTicks: 1000}, Direction: "forward"}, Complete: true, Raw: ref})
	return e
}

func historyBytes(t *testing.T, step c.Step, a transactionAttempt, owner transactionOwner) []byte {
	t.Helper()
	var schema struct {
		Defs map[string]struct {
			Const json.RawMessage `json:"const"`
		} `json:"$defs"`
	}
	if err := json.Unmarshal(c.Schema(), &schema); err != nil {
		t.Fatal(err)
	}
	forward, reverse := any(a), any(schema.Defs["s8_attempt_empty"].Const)
	if step.Direction == "reverse" {
		forward, reverse = reverse, forward
	}
	grant := map[string]any{"handle": a.Handle, "parent_tx_id": step.ParentID, "parent_slot": step.ID, "client_nonce": "prepare-" + step.ID, "issuer": owner, "actor_fp": "actor", "mode": "hot", "source_release": step.From, "target_release": step.To, "source_sha": a.SourceSHA, "target_sha": a.TargetSHA, "scope_digest": a.Scope.ID, "capability_digest": "capabilities", "expires_at": 1000000}
	record := map[string]any{"version": 1, "key": a.Handle.Key, "revision": a.Handle.Revision, "grant": grant, "forward": forward, "reverse": reverse, "actions": []any{}, "reason": "", "effective_sha": a.TargetSHA, "recorded_previous_sha": a.TargetSHA}
	metadata := map[string]any{"tx_id": a.Handle.TxID, "reported_revision": a.Handle.Revision, "window": "open", "pins": []any{}, "archived_at": 0}
	return jsonBytes(t, map[string]any{"record": record, "metadata": metadata})
}

func fixtureRaw(id, schema string) c.RawRef {
	return c.RawRef{Path: "raw/" + id + ".json", SHA: sha256.Sum256([]byte(id)), Bytes: 1, Schema: schema}
}
func jsonBytes(t *testing.T, value any) []byte {
	t.Helper()
	data, err := json.Marshal(value)
	if err != nil {
		t.Fatal(err)
	}
	return data
}
func cloneProfile(t *testing.T, p c.Profile) c.Profile {
	t.Helper()
	copy := p
	copy.Required = slices.Clone(p.Required)
	copy.Steps = slices.Clone(p.Steps)
	for i := range copy.Steps {
		copy.Steps[i].DependsOn = slices.Clone(p.Steps[i].DependsOn)
	}
	copy.Artifacts = slices.Clone(p.Artifacts)
	for i := range copy.Artifacts {
		copy.Artifacts[i].Domains = slices.Clone(p.Artifacts[i].Domains)
		copy.Artifacts[i].ReadEpochs = slices.Clone(p.Artifacts[i].ReadEpochs)
	}
	return copy
}
func cloneEvidence(t *testing.T, e c.Evidence) c.Evidence {
	t.Helper()
	copy := e
	copy.Profile = cloneProfile(t, e.Profile)
	copy.Views = slices.Clone(e.Views)
	for i := range copy.Views {
		copy.Views[i].Historical = slices.Clone(e.Views[i].Historical)
		copy.Views[i].Current = slices.Clone(e.Views[i].Current)
		copy.Views[i].Deferred = slices.Clone(e.Views[i].Deferred)
		copy.Views[i].Control.ResourceIDs = slices.Clone(e.Views[i].Control.ResourceIDs)
		copy.Views[i].Resources.ResourceIDs = slices.Clone(e.Views[i].Resources.ResourceIDs)
		copy.Views[i].Released.ResourceIDs = slices.Clone(e.Views[i].Released.ResourceIDs)
	}
	copy.Samples = slices.Clone(e.Samples)
	for i := range copy.Samples {
		copy.Samples[i].Resources = slices.Clone(e.Samples[i].Resources)
		copy.Samples[i].Consumers = slices.Clone(e.Samples[i].Consumers)
		copy.Samples[i].Temps = slices.Clone(e.Samples[i].Temps)
		copy.Samples[i].Locks = slices.Clone(e.Samples[i].Locks)
		copy.Samples[i].OldImages = slices.Clone(e.Samples[i].OldImages)
	}
	copy.Slots = slices.Clone(e.Slots)
	copy.Attempts = slices.Clone(e.Attempts)
	copy.Intervals = slices.Clone(e.Intervals)
	copy.Raw = slices.Clone(e.Raw)
	return copy
}
func editCurrent(t *testing.T, v *c.View, fn func(*transactionObservation)) {
	t.Helper()
	var o transactionObservation
	if err := json.Unmarshal(v.Current, &o); err != nil {
		t.Fatal(err)
	}
	fn(&o)
	v.Current = jsonBytes(t, o)
}
func editHistory(t *testing.T, v *c.View, fn func(map[string]any)) {
	t.Helper()
	var h map[string]any
	decoder := json.NewDecoder(strings.NewReader(string(v.Historical)))
	decoder.UseNumber()
	if err := decoder.Decode(&h); err != nil {
		t.Fatal(err)
	}
	fn(h)
	v.Historical = jsonBytes(t, h)
}
