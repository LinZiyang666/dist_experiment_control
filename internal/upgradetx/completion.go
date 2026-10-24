package upgradetx

import (
	"path/filepath"
	"reflect"
	"slices"
	"strconv"
	"strings"
)

// Completed checks a historical completion against a separately obtained live
// observation. The observer must bind Nonce to the caller's query challenge;
// this pure function cannot perform a probe or establish that a clock is live.
// In particular, effective disk selection is never running-image evidence.
// origin: hot-update S8 plan §4.2 / §9.1
func Completed(attempt Attempt, observation Observation) error {
	if !validAttempt(attempt) || attempt.Phase != PhaseCompleted || !completionReceipts(attempt) {
		return unsettled("completion requires a released attempt and three independent receipts")
	}
	current := observation.Current
	// Direction and both SHAs are part of the attempt, even though a receipt
	// stamp carries only its handle. Comparing the handle alone would let a
	// caller reinterpret the same forward proof as a reverse observation.
	if !sameAttempt(attempt, current) || current.Handle != attempt.Handle ||
		current.Phase != PhaseCompleted || current.CutStage != CutReleased || current.Outcome != OutcomeSuccess {
		return unsettled("observation does not identify the completed attempt")
	}
	// origin: hot-update S8 plan §4.2 / M02, M15
	if !observation.OwnerKnown || observation.RunningSHA != attempt.TargetSHA ||
		observation.EffectiveSHA != attempt.TargetSHA || !hexString(observation.Nonce, 32) {
		return unsettled("current running image and owner are unproven")
	}
	census := observation.Census
	// The historical seal and the live query have different nonces. Inside a
	// single observation, however, the census must have exactly the query's
	// nonce, time and current revision: stitching two queries is not a proof.
	if !validCensus(attempt, census, true) || census.Stamp.Attempt != attempt.Handle ||
		census.Stamp.Nonce != observation.Nonce || census.Stamp.At != observation.At ||
		census.Stamp.Producer != attempt.Released.Stamp.Producer ||
		census.Stamp.ScopeDigest != attempt.Released.Stamp.ScopeDigest ||
		census.ExpectedDigest != attempt.Released.Census.ExpectedDigest ||
		census.Stamp.Sequence < attempt.Released.Stamp.Sequence || len(census.OldOwners) != 0 {
		return unsettled("current census does not close the admission and ownership barrier")
	}
	for _, receipt := range []Receipt{attempt.Control, attempt.Resources, attempt.Released} {
		if observation.Nonce == receipt.Stamp.Nonce || observation.At < receipt.Stamp.At {
			return unsettled("historical evidence cannot answer a fresh observation")
		}
	}
	// New admissions after completion may belong to this generation. They cannot
	// erase any original or late identity already witnessed at the final seal.
	if !containsResources(census.Resources, attempt.Released.Census.Resources) {
		return unsettled("current census lost a sealed resource identity")
	}
	return nil
}

func unsettled(detail string) error {
	return &Error{Code: CodeUnsettled, Detail: detail, Accepted: true}
}

// This is the journal half of completion, shared with Transition. It must not
// manufacture an Observation from serialized receipts: doing that would make
// a saved running SHA prove itself after a cold start or disk-only replacement.
// ScopeDigest and ExpectedDigest bind the original scope and identities using
// the shared canonical encoding. Every receipt keeps that commitment while its
// observed census can grow to include admissions after the cut.
func completionReceipts(a Attempt) bool {
	for _, pair := range []struct {
		receipt Receipt
		kind    ReceiptKind
	}{{a.Control, ReceiptControlReady}, {a.Resources, ReceiptResourcesMigrated}, {a.Released, ReceiptOldGenerationReleased}} {
		if !validReceipt(a, pair.receipt, pair.kind) {
			return false
		}
	}
	// The final seal subsumes earlier observations, including resources admitted
	// after the original cut. Arrival order of the three messages is immaterial.
	last := a.Released
	for _, earlier := range []Receipt{a.Control, a.Resources} {
		if earlier.Stamp.Producer != last.Stamp.Producer || earlier.Stamp.ScopeDigest != last.Stamp.ScopeDigest ||
			earlier.Census.ExpectedDigest != last.Census.ExpectedDigest || earlier.Stamp.Sequence > last.Stamp.Sequence ||
			earlier.Stamp.At > last.Stamp.At || !resourcesProgress(earlier.Census.Resources, last.Census.Resources) {
			return false
		}
	}
	return true
}

func validReceipt(a Attempt, receipt Receipt, kind ReceiptKind) bool {
	// Slot kind is supplied by the caller, not inferred from the stored value.
	// Otherwise a Resources receipt placed in Control would validate itself.
	// The census stamp must be identical, including producer and sequence;
	// partial equality would permit an unrelated empty census to sign release.
	if receipt.Kind != kind || !receipt.Known || !receipt.Complete || receipt.RunningSHA != a.TargetSHA ||
		!hexString(receipt.EvidenceDigest, 64) || receipt.Stamp != receipt.Census.Stamp {
		return false
	}
	settled := kind == ReceiptResourcesMigrated || kind == ReceiptOldGenerationReleased
	return validCensus(a, receipt.Census, settled) &&
		(kind != ReceiptOldGenerationReleased || len(receipt.Census.OldOwners) == 0)
}

func validCensus(a Attempt, census Census, settled bool) bool {
	s := census.Stamp
	scopeDigest, scopeErr := ScopeDigest(a.Scope)
	expectedDigest, expectedErr := ExpectedDigest(a.Scope)
	// Receipt revisions can precede later journal writes, but cannot come from
	// a future revision or an unknown cut. Store.Apply separately CASes the
	// caller's expected handle; accepting older evidence does not authorize an
	// old callback to write to the current transaction.
	if scopeErr != nil || expectedErr != nil || s.ScopeDigest != scopeDigest || census.ExpectedDigest != expectedDigest ||
		!census.Known || !census.Complete || !sameCut(s.Attempt, a.Handle) || s.Attempt.Revision == 0 ||
		s.Attempt.Revision > a.Handle.Revision || !validOwner(s.Producer) || !hexString(s.Nonce, 32) ||
		s.Sequence == 0 || s.At <= 0 || !hexString(s.ScopeDigest, 64) || !hexString(census.ExpectedDigest, 64) ||
		(settled && census.Admitting != 0) || !validResources(census.Resources, a.Scope.Domains) ||
		!CensusCovers(a.Scope, census) {
		return false
	}
	owners := make(map[Owner]bool, len(census.OldOwners))
	for _, owner := range census.OldOwners {
		if !validOwner(owner) || owners[owner] {
			return false
		}
		owners[owner] = true
	}
	if settled {
		// Control can be ready while old admissions are still settling. The
		// Resources and Released proofs cannot: every live item must already
		// belong to the target generation. PID equality across exec is expected
		// and therefore cannot replace this generation comparison.
		for _, resource := range census.Resources {
			// Terminal evidence has a distinct custody disposition. A terminal
			// worker with retained execution rights still occupies a generation.
			if resource.State == "not_applicable" && resource.Custody == "not_applicable" {
				found := false
				for _, expected := range a.Scope.Expected {
					if resourceIdentity(resource) == resourceIdentity(expected) && expected.State == "not_applicable" {
						found = true
					}
				}
				if !found {
					return false
				}
				continue
			}
			if ResourceReleased(resource) {
				continue
			}
			if !OrchestrationKey(a.Handle.Key) && (resource.Generation != a.Handle.Generation || resource.Owner != census.Stamp.Producer || owners[resource.Owner]) {
				return false
			}
		}
	}
	return true
}

// Resource identity excludes changing ownership/state. These fields must be
// checked as facts, but must not permit the original set to shrink on migration.
func resourceIdentity(r Resource) string {
	return string(r.Domain) + "\x00" + r.ID + "\x00" + r.Origin
}

func containsResources(got, expected []Resource) bool {
	// Extra identities are intentional: requests admitted after the first
	// snapshot must survive into the final seal and the later live query.
	// validResources rejects duplicate rows before this subset check is used.
	identities := make(map[string]bool, len(got))
	for _, resource := range got {
		identities[resourceIdentity(resource)] = true
	}
	for _, resource := range expected {
		if !identities[resourceIdentity(resource)] {
			return false
		}
	}
	return true
}

func validResources(resources []Resource, domains []Domain) bool {
	seen := make(map[string]bool, len(resources))
	for _, r := range resources {
		if !slices.Contains(domains, r.Domain) || !boundedText(r.ID, 4096) || !boundedText(r.Origin, 4096) ||
			!boundedText(r.State, 128) || !boundedText(r.Custody, 128) || seen[string(r.Domain)+"\x00"+r.ID] {
			return false
		}
		seen[string(r.Domain)+"\x00"+r.ID] = true
		// Inapplicability is explicit, with no pretend owner/generation. A live
		// row cannot be converted to this disposition during completion; that
		// separate comparison against the original scope is in validCensus.
		if r.State == "not_applicable" || r.Custody == "not_applicable" {
			if r.State != "not_applicable" || r.Custody != "not_applicable" || r.Owner != (Owner{}) || r.Generation != 0 {
				return false
			}
		} else if !validOwner(r.Owner) || r.Generation == 0 || !slices.Contains([]string{"source", "target", "runtime", "retained", "current", "owned", "active", "durable", "released"}, r.Custody) || (r.Custody == "released" || r.Custody == "durable") && r.State != "terminal" {
			return false
		}
	}
	return true
}

func validAttempt(a Attempt) bool {
	// Generations describe ownership, not ABI epochs or release versions. An
	// increasing ownership step excludes wraparound. A safely cancelled
	// candidate consumes an allocation without becoming a source owner.
	// origin: S8 internal review rv-fault F4
	return validHandle(a.Handle) && (a.Direction == DirectionForward || a.Direction == DirectionReverse) &&
		hexString(a.SourceSHA, 64) && hexString(a.TargetSHA, 64) && a.SourceGeneration > 0 &&
		a.SourceGeneration < a.Handle.Generation &&
		validScope(a.Scope) && (slices.Contains(a.Scope.Participants, a.Handle.Key) || OrchestrationKey(a.Handle.Key)) && validJointState(a)
}

func sameAttempt(a, b Attempt) bool {
	return sameCut(a.Handle, b.Handle) && a.Direction == b.Direction && a.SourceSHA == b.SourceSHA &&
		a.TargetSHA == b.TargetSHA && a.SourceGeneration == b.SourceGeneration && reflect.DeepEqual(a.Scope, b.Scope)
}

func sameCut(a, b Handle) bool {
	return a.Key == b.Key && a.TxID == b.TxID && a.CutID == b.CutID && a.Generation == b.Generation
}

func validHandle(h Handle) bool {
	return validKey(h.Key) && hexString(h.TxID, 32) && hexString(h.CutID, 32) && h.Generation > 0 && h.Revision > 0
}

func validKey(key Key) bool {
	// Root UID is a legitimate namespace, not an unknown identity. Agent keys
	// require SID/configured NID; broker keys require the stable config path.
	// Path aliases and installation registration are resolved by upgradelock;
	// this validator only rejects noncanonical lexical paths and wrong roles.
	// Infrastructure registration is independent of S8 execution capability.
	// The same role-bearing key must survive Transition and all three receipts;
	// broadening the role alone must not accept an agent's SID as an infra key.
	// origin: hot-update S8 plan §2.1 / §4.2; S9 plan §8 / T44
	if key.Component == ComponentNATS || key.Component == ComponentCaddy {
		return boundedText(key.DomainID, 256) && key.SID == "" && boundedText(key.ConfiguredNID, 256) &&
			boundedText(key.ConfigPath, 4096) && filepath.IsAbs(key.ConfigPath) && filepath.Clean(key.ConfigPath) == key.ConfigPath
	}
	if !boundedText(key.DomainID, 256) || (key.Component != ComponentAgent && key.Component != ComponentBroker) || key.Component == ComponentBroker && key.SID != "" {
		return false
	}
	if key.Component == ComponentAgent && (!boundedText(key.SID, 128) || !boundedText(key.ConfiguredNID, 128)) {
		return false
	}
	if key.Component == ComponentBroker && !boundedText(key.ConfigPath, 4096) {
		return false
	}
	return (key.SID == "" || boundedText(key.SID, 128)) && (key.ConfiguredNID == "" || boundedText(key.ConfiguredNID, 128)) &&
		(key.ConfigPath == "" || (boundedText(key.ConfigPath, 4096) && filepath.IsAbs(key.ConfigPath) && filepath.Clean(key.ConfigPath) == key.ConfigPath))
}

func validOwner(owner Owner) bool {
	// Boot IDs may use the platform UUID spelling. Do not require the random
	// transaction-ID encoding here; PID/start ticks/instance are still all
	// mandatory so a recycled PID alone cannot serve as ownership evidence.
	return boundedText(owner.BootID, 128) && owner.PID > 0 && owner.StartTicks > 0 && boundedText(owner.InstanceID, 128)
}

func keyOrder(key Key) string {
	// NUL framing is unambiguous because every key field excludes controls.
	// Include UID and config path: co-located installations are not one actor.
	return strings.Join([]string{key.DomainID, string(key.Component), key.SID, key.ConfiguredNID, key.ConfigPath, strconv.FormatUint(uint64(key.UID), 10)}, "\x00")
}

func validScope(scope Scope) bool {
	if !hexString(scope.ID, 32) || len(scope.Participants) == 0 {
		return false
	}
	for i, participant := range scope.Participants {
		if !validKey(participant) || (i > 0 && keyOrder(scope.Participants[i-1]) >= keyOrder(participant)) {
			return false
		}
	}
	// origin: hot-update S8 plan §2.1 / §4.2 / M06
	// Even an inapplicable domain must be explicit in the sealed scope. Its
	// collector supplies not_applicable evidence; absence is never a zero count.
	domains := []Domain{DomainAuthority, DomainBrokerRuntime, DomainBrokerStore, DomainControl, DomainExec,
		DomainFlow, DomainPTY, DomainSS, DomainTransferTracker, DomainTransferWorker, DomainUpgrade}
	if !slices.Equal(scope.Domains, domains) || !validResources(scope.Expected, scope.Domains) {
		return false
	}
	applicable := map[Domain]bool{}
	excluded := map[Domain]bool{}
	for _, r := range scope.Expected {
		if ResourceNotApplicable(r) {
			excluded[r.Domain] = true
		} else {
			applicable[r.Domain] = true
		}
		if excluded[r.Domain] && applicable[r.Domain] {
			return false
		}
	}
	for i := 1; i < len(scope.Expected); i++ {
		if resourceIdentity(scope.Expected[i-1]) >= resourceIdentity(scope.Expected[i]) {
			return false
		}
	}
	return true
}

func hexString(value string, length int) bool {
	// Exact lowercase encodings prevent alternate spellings of one identity
	// from bypassing duplicate detection or changing a sealed comparison.
	if len(value) != length {
		return false
	}
	for _, c := range value {
		if (c < '0' || c > '9') && (c < 'a' || c > 'f') {
			return false
		}
	}
	return true
}

func boundedText(value string, limit int) bool {
	if value == "" || len(value) > limit || strings.TrimSpace(value) != value {
		return false
	}
	for _, c := range value {
		if c < ' ' || c == '\x7f' {
			return false
		}
	}
	return true
}
