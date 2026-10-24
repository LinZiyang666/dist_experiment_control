// judge.go checks rollout completion against current, digest-bound observations.
// This is the P01 predicate, not a replacement for the workload, budget, resource
// or compatibility predicates. It never runs commands or opens evidence paths.
// origin: hot-update S10 plan §5.2 / §10.2 / P01
package acceptance

import (
	"encoding/hex"
	"encoding/json"
	"errors"
	"slices"

	c "github.com/LinZiyang666/tether/test/hotupdate/acceptance/contract"
)

var _ c.JudgeFunc = Judge

// These typed projections are used only after the complete P00 schema validates
// Historical and Current. The original bytes stay in View and its raw archive;
// the projection cannot accept unknown fields or certify a partial upstream DTO.
type transactionKey struct {
	Domain string `json:"domain_id"`
	Role   string `json:"component"`
	SID    string `json:"sid"`
	NID    string `json:"configured_nid"`
	UID    uint32 `json:"uid"`
}
type transactionHandle struct {
	Key        transactionKey `json:"key"`
	TxID       string         `json:"tx_id"`
	CutID      string         `json:"cut_id"`
	Generation uint64         `json:"generation"`
	Revision   uint64         `json:"revision"`
}
type transactionOwner struct {
	BootID     string `json:"boot_id"`
	PID        int    `json:"pid"`
	StartTicks uint64 `json:"start_ticks"`
	InstanceID string `json:"instance_id"`
}
type transactionStamp struct {
	Attempt  transactionHandle `json:"attempt"`
	Producer transactionOwner  `json:"producer"`
	Nonce    string            `json:"nonce"`
	Sequence uint64            `json:"sequence"`
	At       int64             `json:"at"`
	Scope    string            `json:"scope_digest"`
}
type transactionResource struct {
	Domain     string           `json:"domain"`
	ID         string           `json:"id"`
	Origin     string           `json:"origin"`
	Owner      transactionOwner `json:"owner"`
	Generation uint64           `json:"generation"`
	State      string           `json:"state"`
	Custody    string           `json:"custody"`
}
type transactionCensus struct {
	Stamp          transactionStamp      `json:"stamp"`
	Known          bool                  `json:"known"`
	Complete       bool                  `json:"complete"`
	Resources      []transactionResource `json:"resources"`
	ExpectedDigest string                `json:"expected_digest"`
	Admitting      uint64                `json:"admitting"`
	OldOwners      []transactionOwner    `json:"old_owners"`
}
type transactionReceipt struct {
	Kind           string            `json:"kind"`
	Stamp          transactionStamp  `json:"stamp"`
	Known          bool              `json:"known"`
	Complete       bool              `json:"complete"`
	RunningSHA     string            `json:"running_sha"`
	Census         transactionCensus `json:"census"`
	EvidenceDigest string            `json:"evidence_digest"`
}
type transactionAttempt struct {
	Handle           transactionHandle `json:"handle"`
	Direction        string            `json:"direction"`
	SourceSHA        string            `json:"source_sha"`
	TargetSHA        string            `json:"target_sha"`
	SourceGeneration uint64            `json:"source_generation"`
	Phase            string            `json:"phase"`
	CutStage         string            `json:"cut_stage"`
	Outcome          string            `json:"outcome"`
	Scope            struct {
		ID           string                `json:"id"`
		Participants []transactionKey      `json:"participants"`
		Domains      []string              `json:"domains"`
		Expected     []transactionResource `json:"expected"`
	} `json:"scope"`
	Control   transactionReceipt `json:"control"`
	Resources transactionReceipt `json:"resources"`
	Released  transactionReceipt `json:"released"`
}
type transactionObservation struct {
	Nonce        string             `json:"nonce"`
	At           int64              `json:"at"`
	Current      transactionAttempt `json:"current"`
	Census       transactionCensus  `json:"census"`
	OwnerKnown   bool               `json:"owner_known"`
	RunningSHA   string             `json:"running_sha"`
	EffectiveSHA string             `json:"effective_sha"`
}
type transactionHistory struct {
	Record struct {
		Key      transactionKey `json:"key"`
		Revision uint64         `json:"revision"`
		Grant    struct {
			Mode string `json:"mode"`
		} `json:"grant"`
		Reason              string             `json:"reason"`
		RecordedPreviousSHA string             `json:"recorded_previous_sha"`
		Forward             transactionAttempt `json:"forward"`
		Reverse             transactionAttempt `json:"reverse"`
	} `json:"record"`
	Metadata struct {
		TxID             string `json:"tx_id"`
		ReportedRevision uint64 `json:"reported_revision"`
	} `json:"metadata"`
}

// Judge distinguishes contradictory evidence from absent coverage. A schema or
// manifest error is SETUP-RED with an error; missing observations are INCOMPLETE.
// Supplied failures always win over a later completed query or missing coverage.
func Judge(e c.Evidence) (c.Result, error) {
	r := c.Result{Verdict: "SETUP-RED", RC: 2, Inputs: slices.Clone(e.Raw)}
	if _, err := c.EncodeJSON("evidence", e); err != nil {
		return r, errors.New("invalid fullstack evidence")
	}
	steps, err := Plan(e.Profile)
	if err != nil {
		return r, err
	}
	byKey := make(map[c.Key][]c.Step)
	for _, step := range steps {
		byKey[step.Target] = append(byKey[step.Target], step)
	}
	raw := make(map[c.RawRef]bool)
	for _, ref := range e.Raw {
		if !validRaw(ref) || raw[ref] {
			return r, errors.New("invalid or repeated fullstack raw reference")
		}
		raw[ref] = true
	}
	if len(raw) == 0 {
		r.Missing = append(r.Missing, "raw archive")
	}
	progress := make(map[c.Key]int)
	completed := make(map[string]bool)
	last := make(map[c.Key]c.Stamp)
	nonces, cuts := make(map[string]bool), make(map[string]bool)
	for _, v := range e.Views {
		key := v.Stamp.Process.Key
		index := progress[key]
		if index >= len(byKey[key]) {
			r.Failures = append(r.Failures, "unplanned or replayed runtime view")
			continue
		}
		step := byKey[key][index]
		var current transactionObservation
		var history transactionHistory
		if json.Unmarshal(v.Current, &current) != nil || json.Unmarshal(v.Historical, &history) != nil {
			return r, errors.New("invalid fullstack transaction projection")
		}
		// All observations participate in the refusal union, including a refusal
		// followed by a successful query. No error namespace is rewritten here.
		// Unknown, cancelled and automatic rollback outcomes also remain failures:
		// later completion cannot restore certainty lost earlier in the same cut.
		// Normal pending observations and explicit reverse success remain legal.
		// origin: S10 internal review rv-harness F4
		if len(v.Deferred) != 0 || v.Code != "" || history.Record.Reason != "" || history.Record.Grant.Mode != "hot" ||
			(current.Current.Outcome != "pending" && current.Current.Outcome != "success") {
			r.Failures = append(r.Failures, "runtime action reported a non-success outcome or an error")
		}
		if !raw[v.Raw] {
			r.Missing = append(r.Missing, "runtime view raw")
		}
		if !legalTransactionState(current.Current) || !projectedPhase(v, current.Current) {
			r.Failures = append(r.Failures, "transaction phase, cut stage and outcome disagree")
			continue
		}
		if !freshView(v, current, history, last[key]) || nonces[v.Stamp.QueryNonce] {
			r.Failures = append(r.Failures, "runtime view has a stale or mismatched current stamp")
			continue
		}
		nonces[v.Stamp.QueryNonce] = true
		last[key] = v.Stamp
		if current.Current.Phase != "completed" {
			continue
		}
		from, to := stepArtifacts(e.Profile, step)
		if current.Current.Direction != step.Direction || v.Stamp.Direction != step.Direction ||
			current.Current.SourceSHA != digestText(from.Binary) || current.Current.TargetSHA != digestText(to.Binary) ||
			current.RunningSHA != digestText(to.Binary) || current.EffectiveSHA != current.RunningSHA {
			r.Failures = append(r.Failures, "current completion does not reach the planned component target")
			continue
		}
		if step.Direction == "reverse" && history.Record.RecordedPreviousSHA != current.Current.TargetSHA {
			r.Failures = append(r.Failures, "reverse completion does not reach recorded previous bytes")
			continue
		}
		cutKey := keyLabel(key) + "\x00" + v.Stamp.CutID
		if cuts[cutKey] {
			r.Failures = append(r.Failures, "completion reuses an earlier cut")
			continue
		}
		before := len(r.Failures) + len(r.Missing)
		// A correct final vector cannot repair an action that ran before its
		// predecessor retired. Views retain observation order from the driver;
		// cross-host BOOTTIME values are never subtracted or globally sorted.
		// origin: hot-update S10 plan §2.2 / §5.2 / P01
		for _, dependency := range step.DependsOn {
			if !completed[dependency] {
				r.Failures = append(r.Failures, "completion precedes a required retirement barrier")
			}
		}
		judgeCompletion(&r, e, v, current, to.Binary, raw)
		if len(r.Failures)+len(r.Missing) == before {
			cuts[cutKey] = true
			progress[key]++
			completed[step.ID] = true
		}
	}
	for _, key := range e.Profile.Required {
		if progress[key] != len(byKey[key]) {
			r.Missing = append(r.Missing, "required runtime rollout incomplete")
		}
		found := false
		for _, sample := range e.Samples {
			if sample.Stamp.Process.Key == key && sample.Complete && raw[sample.Raw] {
				found = true
			}
		}
		if !found {
			r.Missing = append(r.Missing, "required runtime census")
		}
	}
	for _, sample := range e.Samples {
		// A collector cannot shrink the observed fleet by ignoring an unknown
		// runtime, even when every planned runtime also has a complete sample.
		// origin: hot-update S10 plan §5.2 / P01
		if !slices.Contains(e.Profile.Required, sample.Stamp.Process.Key) {
			r.Failures = append(r.Failures, "sample belongs to an unplanned runtime")
		}
	}
	// Every attempt is retained: the final success cannot erase a broker refusal
	// or unknown result earlier in the same logical operation. P02 owns latency.
	for _, attempt := range e.Attempts {
		if attempt.RC != 0 || attempt.Code != "" {
			r.Failures = append(r.Failures, "control attempt reported a failure")
		}
		if !attempt.Completed || !attempt.Sent || !raw[attempt.Raw] {
			r.Missing = append(r.Missing, "control attempt completion")
		}
	}
	r.Verdict, r.RC = "GREEN", 0
	if len(r.Missing) != 0 {
		r.Verdict, r.RC = "INCOMPLETE", 4
	}
	if len(r.Failures) != 0 {
		r.Verdict, r.RC = "ASSERT-FAIL", 1
	}
	return r, nil
}

func freshView(v c.View, o transactionObservation, h transactionHistory, previous c.Stamp) bool {
	s := v.Stamp
	if s.TxID == "" || s.CutID == "" || s.QueryNonce == "" || s.ScopeDigest == "" ||
		s.Generation == 0 || s.Revision == 0 || s.Sequence == 0 || o.Nonce != s.QueryNonce || o.At != s.AtBootNS ||
		!matchesTransactionStamp(o.Census.Stamp, s) || !o.OwnerKnown || !o.Census.Known || !o.Census.Complete ||
		!matchesHandle(o.Current.Handle, s) || o.Current.Direction != s.Direction || !matchesKey(h.Record.Key, s.Process.Key) ||
		h.Record.Revision != s.Revision || h.Metadata.ReportedRevision != s.Revision || h.Metadata.TxID != s.TxID {
		return false
	}
	if previous == (c.Stamp{}) {
		return true
	}
	if (s.Process.Key.Role == "agent" || s.Process.Key.Role == "broker") && s.Process != previous.Process {
		return false // same-PID exec cannot be replaced by a new daemon identity.
	}
	if s.TxID == previous.TxID && s.CutID == previous.CutID {
		return s.Generation == previous.Generation && s.Revision >= previous.Revision && s.Sequence > previous.Sequence && s.AtBootNS >= previous.AtBootNS
	}
	// Same-PID exec preserves the producer's monotonic sequence and boot clock.
	// A newly started infra process has its own sequence; its generation still
	// advances, so the old process cannot supply the new cut's completion.
	// origin: hot-update S10 plan §5.2 / TestFullstackCompletionRequiresCurrentStamp
	return s.Generation > previous.Generation && s.Revision >= previous.Revision &&
		(s.Process != previous.Process || (s.Sequence > previous.Sequence && s.AtBootNS >= previous.AtBootNS))
}

func judgeCompletion(r *c.Result, e c.Evidence, v c.View, o transactionObservation, target c.Digest, raw map[c.RawRef]bool) {
	expected, ok := resourceIdentities(o.Current.Scope.Expected)
	if !ok || o.Current.Scope.ID == "" || len(o.Current.Scope.Domains) == 0 ||
		!slices.Contains(o.Current.Scope.Participants, o.Current.Handle.Key) || o.Census.ExpectedDigest == "" ||
		!sameResources(expected, o.Census.Resources) || !settledCensus(o.Current, o.Census) || len(o.Census.OldOwners) != 0 {
		r.Failures = append(r.Failures, "current census does not close the sealed scope")
	}
	kinds := []string{"control_ready", "resources_migrated", "old_generation_released"}
	projected := []c.Receipt{v.Control, v.Resources, v.Released}
	actual := []transactionReceipt{o.Current.Control, o.Current.Resources, o.Current.Released}
	for i, receipt := range projected {
		upstream := actual[i]
		// origin: hot-update S10 plan §5.2 / TestFullstackCompletionRequiresCurrentStamp
		if !historicalReceipt(receipt.Stamp, v.Stamp) || !matchesTransactionStamp(upstream.Stamp, receipt.Stamp) ||
			!matchesTransactionStamp(upstream.Census.Stamp, receipt.Stamp) || receipt.Kind != kinds[i] || upstream.Kind != kinds[i] ||
			receipt.RunningSHA != target || upstream.RunningSHA != digestText(target) ||
			upstream.Census.ExpectedDigest != o.Census.ExpectedDigest || !sameResources(expected, upstream.Census.Resources) ||
			!sameTransactionResourceIDs(upstream.Census.Resources, receipt.ResourceIDs) || upstream.EvidenceDigest == "" ||
			!validCensusResources(o.Current, upstream.Census) || (i > 0 && !settledCensus(o.Current, upstream.Census)) {
			r.Failures = append(r.Failures, "completion receipt has a mismatched current stamp or scope")
		}
		if !receipt.Known || !receipt.Complete || !upstream.Known || !upstream.Complete || !upstream.Census.Known || !upstream.Census.Complete || !raw[receipt.Evidence] {
			r.Missing = append(r.Missing, "current completion receipt")
		}
	}
	if !completionBarrier(o) {
		r.Failures = append(r.Failures, "old owner release precedes the completion barrier")
	}
	// origin: hot-update S10 plan §5.2 / TestFullstackCompletionRequiresCurrentStamp
	verified := false
	for _, sample := range e.Samples {
		if sameQuery(sample.Stamp, v.Stamp) && sample.Complete && sample.ExeSHA == target && raw[sample.Raw] {
			verified = true
		}
	}
	if !verified {
		r.Missing = append(r.Missing, "independent current executable proof")
	}
}

// Historical seals retain their own nonce and journal revision. A fresh query
// must follow every seal without rewriting any archived field to look current.
// origin: S10 internal review rv-honesty F3
func historicalReceipt(a, b c.Stamp) bool {
	revision, nonce := a.Revision, a.QueryNonce
	a.Revision, a.QueryNonce = b.Revision, b.QueryNonce
	return revision > 0 && revision <= b.Revision && nonce != "" && nonce != b.QueryNonce && sameQuery(a, b)
}

func sameQuery(a, b c.Stamp) bool {
	aSeq, aAt := a.Sequence, a.AtBootNS
	a.Sequence, a.AtBootNS = b.Sequence, b.AtBootNS
	return a == b && aSeq > 0 && aSeq <= b.Sequence && aAt > 0 && aAt <= b.AtBootNS
}

func matchesKey(a transactionKey, b c.Key) bool {
	return a.Role == b.Role && a.Domain == b.Domain && a.SID == b.SID && a.NID == b.NID && a.UID == b.UID
}

func matchesHandle(a transactionHandle, b c.Stamp) bool {
	return matchesKey(a.Key, b.Process.Key) && a.TxID == b.TxID && a.CutID == b.CutID && a.Generation == b.Generation && a.Revision == b.Revision
}

func matchesTransactionStamp(a transactionStamp, b c.Stamp) bool {
	owner := transactionOwner{b.Process.BootID, b.Process.PID, b.Process.StartTicks, b.Process.InstanceID}
	return matchesHandle(a.Attempt, b) && a.Producer == owner && a.Nonce == b.QueryNonce && a.Scope == b.ScopeDigest && a.Sequence == b.Sequence && a.At == b.AtBootNS
}

func legalTransactionState(a transactionAttempt) bool {
	// origin: hot-update S10 plan §5.2 / TestFullstackCompletionRequiresCurrentStamp
	stage := a.CutStage
	switch a.Phase {
	case "prepared", "staged", "ready":
		return stage == "none" && a.Outcome == "pending"
	case "freezing":
		return stage == "pristine" && a.Outcome == "pending"
	case "activating":
		return slices.Contains([]string{"pristine", "partial_active", "active"}, stage) && a.Outcome == "pending"
	case "serving_new":
		return slices.Contains([]string{"partial_active", "active"}, stage) && a.Outcome == "pending"
	case "migrating":
		return slices.Contains([]string{"active", "retiring"}, stage) && a.Outcome == "pending"
	case "completed":
		return stage == "released" && a.Outcome == "success"
	case "deferred", "rejected":
		return stage == "none" && a.Outcome == a.Phase
	case "rollback_pending", "rollback_blocked":
		return a.Direction == "forward" && slices.Contains([]string{"partial_active", "active", "retiring"}, stage) && a.Outcome == "pending"
	case "rolled_back":
		return a.Direction == "forward" && stage == "released" && a.Outcome == "rolled_back"
	case "recovery_required", "abandoned":
		return a.Outcome == "unknown"
	case "cancelled":
		return slices.Contains([]string{"none", "pristine"}, stage) && a.Outcome == "cancelled"
	default:
		return false
	}
}

func projectedPhase(v c.View, a transactionAttempt) bool {
	// Explicit reverse completion has a presentation alias. An automatic forward
	// rollback retains its native outcome and enters the failure union above.
	// origin: S10 internal review rv-harness F4
	if v.Phase == "rolled_back" && v.Outcome == "rolled_back" && a.Direction == "reverse" {
		return a.Phase == "completed" && a.CutStage == "released" && a.Outcome == "success"
	}
	return v.Phase == a.Phase && v.Outcome == a.Outcome
}

// Identity includes origin, but ownership is checked separately: migration may
// change custody without erasing an original or late-admitted resource.
// origin: S10 internal review rv-honesty F2/F3
func resourceIdentities(resources []transactionResource) (map[string]bool, bool) {
	set := make(map[string]bool)
	for _, resource := range resources {
		key := resourceIdentity(resource)
		if resource.Domain == "" || resource.ID == "" || resource.Origin == "" || set[key] {
			return nil, false
		}
		set[key] = true
	}
	return set, true // An explicitly complete empty scope is legal for a capable idle runtime.
}

func resourceIdentity(r transactionResource) string {
	return r.Domain + "\x00" + r.ID + "\x00" + r.Origin
}

func sameResources(expected map[string]bool, resources []transactionResource) bool {
	actual, ok := resourceIdentities(resources)
	if !ok {
		return false
	}
	for key := range expected {
		if !actual[key] {
			return false
		}
	}
	return true
}

// A census must prove actual custody, not just a matching list of IDs or SHA.
// OldOwners is an independent responsibility set, never a substitute for the
// generation attached to each live row. Inapplicability must precede the cut.
// origin: S10 internal review rv-honesty F2
func validCensusResources(a transactionAttempt, census transactionCensus) bool {
	if _, ok := resourceIdentities(census.Resources); !ok {
		return false
	}
	for _, r := range census.Resources {
		if !slices.Contains(a.Scope.Domains, r.Domain) || r.State == "" || r.Custody == "" {
			return false
		}
		if r.State == "not_applicable" || r.Custody == "not_applicable" {
			if r.State != "not_applicable" || r.Custody != "not_applicable" || r.Owner != (transactionOwner{}) || r.Generation != 0 {
				return false
			}
			found := false
			for _, expected := range a.Scope.Expected {
				if resourceIdentity(expected) == resourceIdentity(r) && expected.State == "not_applicable" && expected.Custody == "not_applicable" {
					found = true
				}
			}
			if !found {
				return false
			}
		} else if !validTransactionOwner(r.Owner) || r.Generation == 0 {
			return false
		}
	}
	owners := make(map[transactionOwner]bool)
	for _, owner := range census.OldOwners {
		if !validTransactionOwner(owner) || owners[owner] {
			return false
		}
		owners[owner] = true
	}
	return true
}

func validTransactionOwner(owner transactionOwner) bool {
	return owner.BootID != "" && owner.PID > 0 && owner.StartTicks > 0 && owner.InstanceID != ""
}

// origin: S10 internal review rv-honesty F2
func settledCensus(a transactionAttempt, census transactionCensus) bool {
	if !census.Known || !census.Complete || census.Admitting != 0 || !validCensusResources(a, census) {
		return false
	}
	for _, r := range census.Resources {
		if r.State == "not_applicable" {
			continue
		}
		if r.State == "terminal" && r.Custody == "released" {
			continue
		}
		if r.Generation != a.Handle.Generation || slices.Contains(census.OldOwners, r.Owner) {
			return false
		}
	}
	return true
}

// Later censuses may include new admissions, but cannot lose identities sealed
// earlier. Sequence and time order both survive the historical/fresh boundary.
// origin: S10 internal review rv-honesty F3
func completionBarrier(o transactionObservation) bool {
	last := o.Current.Released
	if len(last.Census.OldOwners) != 0 {
		return false
	}
	for _, earlier := range []transactionReceipt{o.Current.Control, o.Current.Resources} {
		ids, ok := resourceIdentities(earlier.Census.Resources)
		if !ok || !sameResources(ids, last.Census.Resources) || earlier.Stamp.Sequence > last.Stamp.Sequence || earlier.Stamp.At > last.Stamp.At {
			return false
		}
	}
	ids, ok := resourceIdentities(last.Census.Resources)
	return ok && sameResources(ids, o.Census.Resources) && last.Stamp.Sequence <= o.Census.Stamp.Sequence && last.Stamp.At <= o.Census.Stamp.At
}

func sameTransactionResourceIDs(resources []transactionResource, ids []string) bool {
	want := make([]string, len(resources))
	for i, resource := range resources {
		want[i] = resource.ID
	}
	want, got := slices.Clone(want), slices.Clone(ids)
	slices.Sort(want)
	slices.Sort(got)
	return slices.Equal(want, got)
}

func validRaw(ref c.RawRef) bool {
	return ref.Path != "" && ref.SHA != (c.Digest{}) && ref.Bytes > 0 && ref.Schema != ""
}
func digestText(d c.Digest) string { return hex.EncodeToString(d[:]) }
func keyLabel(k c.Key) string {
	data, _ := c.EncodeJSON("key", k)
	return string(data)
}
func stepArtifacts(p c.Profile, step c.Step) (c.Artifact, c.Artifact) {
	var from, to c.Artifact
	for _, artifact := range p.Artifacts {
		if artifact.Role == step.Target.Role {
			if artifact.ID == step.From {
				from = artifact
			}
			if artifact.ID == step.To {
				to = artifact
			}
		}
	}
	return from, to
}
