// Package upgradecompat owns pure upgrade admission checks. Callers supply
// observations from the real decoders, configuration parsers and reservation
// owners; these checks never collect evidence or authorize a side effect.
// origin: hot-update S8 plan §6 / §7 / P06
package upgradecompat

import (
	"slices"

	"github.com/LinZiyang666/tether/internal/upgradetx"
)

// Reader binds reader declarations to executable bytes. Epoch is the image ABI
// epoch, independent of both process Generation and Release. An older reader
// without ReadEpochs can read only its own explicitly declared epoch.
type Reader struct {
	// A pathname or release tag can describe different bytes after publication.
	// The adapter must bind these declarations to the probed executable SHA.
	SHA256    string
	Release   string
	Component upgradetx.Component
	// Image epochs describe decoder compatibility, not runtime lineage. Keeping
	// them separate prevents a generation-one image from certifying ABI support.
	Epoch      uint64
	ReadEpochs []uint64
	// Domains include retained state even when no live business object remains.
	// Features certify the callable S8 transaction and authority adapters.
	Domains  []string
	Features []string
}

// Evidence belongs to one observation at one admission gate. Known=false is
// uncertainty, even when Complete or a surrounding resource count is zero.
type Evidence struct {
	// Sharing the collector stamp binds every validator to the same cut and
	// nonce. A successful validator from an earlier gate is not transferable.
	Stamp upgradetx.Stamp
	Stage upgradetx.ActionKind
	// A role's not_applicable result must itself be known and complete; missing
	// observations cannot manufacture the evidence for an idle role.
	Known    bool
	Complete bool
}

// Preflight contains evidence for a hot cut only. Explicit first migration uses
// CheckLegacy and the legacy issuer's durable dispatch path, never hot receipts.
// RequiredDomains comes from the actual final descriptor union, including
// retained terminal/GC state, rather than only currently active business objects.
// Started is attested by the durable Store record, never a client flag. Once
// accepted, recovery attaches to that attempt after its unused-grant expiry;
// it does not acquire fresh Start authority by preparing an equivalent grant.
type Preflight struct {
	// The Store owns grant authenticity and current-handle CAS. This layer
	// checks that the capability and resource evidence uses that exact grant.
	Grant     upgradetx.Grant
	Stamp     upgradetx.Stamp
	Stage     upgradetx.ActionKind
	Now       int64
	Started   bool
	Source    Reader
	Candidate Reader
	Previous  Reader
	// The journal's recorded previous SHA survives a newer canonical selector.
	// Reading today's disk pathname instead would silently change rollback.
	RecordedPreviousSHA string
	RequiredDomains     []string
	Checks              map[string]Evidence
	// Fleet releases and component resource generations are independent limits.
	// Neither a matching release nor an empty resource list proves the other.
	Fleet  FleetCensus
	Census upgradetx.Census
	// ExpectedDigest is the sealed original resource set supplied by its owner.
	// Stamp equality alone cannot detect an omitted resource in a later census.
	ExpectedDigest   string
	SourceGeneration uint64
	TargetGeneration uint64
}

// RequiredChecks returns a fresh list so a caller cannot weaken future gates
// by modifying a shared slice. Each name denotes actual adapter verification;
// declarations alone cannot supply decoder_closure or configuration evidence.
func RequiredChecks() []string {
	return []string{
		"artifact_provenance", "manifest_schema", "decoder_closure",
		"configuration", "authority_writers", "execution", "descriptors",
		"capacity", "state_directory",
	}
}

// CheckEpochs checks one export/read direction, including distance separately
// from the explicit reader set. A generous reader declaration cannot authorize
// crossing two ABI epochs, and uint64 subtraction must never wrap.
// origin: hot-update S8 plan §6.1 / TestImageEpochBidirectionalReaders
func CheckEpochs(exporter, reader Reader) error {
	if exporter.Epoch == 0 || reader.Epoch == 0 {
		return reject("image_epoch_unknown")
	}
	seen := make(map[uint64]bool, len(reader.ReadEpochs))
	// Malformed declarations are a refusal, not permission to infer the reader
	// set. In particular, zero never represents an all-epochs wildcard.
	for _, epoch := range reader.ReadEpochs {
		if epoch == 0 || seen[epoch] {
			return reject("image_epochs_invalid")
		}
		seen[epoch] = true
	}
	low, high := exporter.Epoch, reader.Epoch
	if low > high {
		low, high = high, low
	}
	if high-low > 1 {
		return reject("image_epoch_distance")
	}
	readable := seen[exporter.Epoch]
	// Legacy absence is weaker than an explicit adjacent reader declaration.
	// It preserves the existing same-epoch mechanism without inventing N-1.
	if len(reader.ReadEpochs) == 0 {
		readable = exporter.Epoch == reader.Epoch
	}
	if !readable {
		return reject("image_epoch_unreadable")
	}
	return nil
}

// CheckReaders requires the source, candidate and recorded previous bytes to
// read each other's images. Checking only candidate<-source leaves a successful
// forward cut with no usable current-state rollback. The required S8 adjunct
// and authority capabilities apply even to a fully observed idle runtime.
// origin: hot-update S8 plan §6.1 / TestImageEpochBidirectionalReaders
func CheckReaders(source, candidate, previous Reader, required []string) error {
	// The union belongs to the actual image exporter. Duplicate/empty tokens
	// could hide an incomplete collector and must not be silently normalized.
	if !uniqueTokens(required) {
		return reject("required_domains_invalid")
	}
	readers := []Reader{source, candidate, previous}
	for _, reader := range readers {
		if !validSHA(reader.SHA256) || reader.Release == "" {
			return reject("reader_identity_invalid")
		}
		if (reader.Component != upgradetx.ComponentAgent && reader.Component != upgradetx.ComponentBroker) || reader.Component != source.Component {
			return reject("reader_role_mismatch")
		}
		if !uniqueTokens(reader.Domains) || !uniqueTokens(reader.Features) {
			return reject("reader_declarations_invalid")
		}
		// Existing S7 transfer support does not imply an S8 journal, authority
		// writer or lock adjunct reader. Each new responsibility is explicit.
		for _, feature := range []string{"upgrade-tx-v1", "authority-incarnation-v1", "upgrade-cut-v1"} {
			if !slices.Contains(reader.Features, feature) {
				return reject("reader_capability_missing")
			}
		}
		// The adjunct remains required when business state is wholly idle: its
		// transaction identity and inherited reservation still cross the exec.
		if !slices.Contains(reader.Domains, "upgrade-cut-v1") {
			return reject("reader_domain_missing")
		}
		for _, domain := range required {
			if !slices.Contains(reader.Domains, domain) {
				return reject("reader_domain_missing")
			}
		}
	}
	// All pairs include the previous artifact as both exporter and reader.
	// Candidate<-source alone cannot prove a reverse export from new code.
	for _, exporter := range readers {
		for _, reader := range readers {
			if err := CheckEpochs(exporter, reader); err != nil {
				return err
			}
		}
	}
	return nil
}

// CheckPreflight is deliberately stateless. Invoke it before disk selection,
// before freeze, and before final dispatch with a newly sampled Stamp. A prior
// success cannot replace evidence for a domain admitted after the first gate.
// Stage uses the frozen action vocabulary instead of another enum family.
// origin: hot-update S8 plan §6.1 / TestUpgradePreflightRequiresFreshEvidence
func CheckPreflight(p Preflight) error {
	// A non-hot first migration has a different dispatch and completion proof.
	// Passing its grant through this path would advertise false continuity.
	if p.Grant.Mode != upgradetx.ModeHot {
		return reject("hot_mode_required")
	}
	if p.Stage != upgradetx.ActionSelect && p.Stage != upgradetx.ActionFreeze && p.Stage != upgradetx.ActionDispatch {
		return reject("preflight_stage_invalid")
	}
	if p.Grant.Handle.Key.Component != upgradetx.ComponentAgent && p.Grant.Handle.Key.Component != upgradetx.ComponentBroker {
		return &upgradetx.Error{Code: upgradetx.CodeUnsupportedComponent, Detail: "preflight_component_unsupported"}
	}
	// Expiry prevents unused authorization from starting, while an accepted
	// attempt keeps its identity when the driver returns to finish recovery.
	if p.Now <= 0 || (!p.Started && p.Grant.ExpiresAt <= p.Now) {
		return &upgradetx.Error{Code: upgradetx.CodeGrantExpired, Detail: "preflight_grant_expired"}
	}
	// Exact attempt and scope matching fences late samples from transaction A
	// after B has become current. A fresh timestamp cannot cure wrong identity.
	if !validStamp(p.Stamp, p.Now) || p.Stamp.Attempt != p.Grant.Handle || p.Stamp.ScopeDigest != p.Grant.ScopeDigest {
		return reject("preflight_identity_mismatch")
	}
	if p.Source.Component != p.Grant.Handle.Key.Component || p.Source.SHA256 != p.Grant.SourceSHA || p.Candidate.SHA256 != p.Grant.TargetSHA || p.Previous.SHA256 != p.RecordedPreviousSHA || p.Source.Release != p.Grant.SourceRelease || p.Candidate.Release != p.Grant.TargetRelease {
		return reject("preflight_artifact_mismatch")
	}
	if err := CheckReaders(p.Source, p.Candidate, p.Previous, p.RequiredDomains); err != nil {
		return err
	}
	names := RequiredChecks()
	// The proof vocabulary is closed here; a misspelled name must not become
	// an ignored validator that a caller mistakenly believes was enforced.
	for name := range p.Checks {
		if !slices.Contains(names, name) {
			return reject("preflight_evidence_unexpected")
		}
	}
	// These are actual adapter results, including nonempty decoder exercises
	// and both configuration parsers. Advertising a token cannot create them.
	for _, name := range names {
		proof, ok := p.Checks[name]
		if !ok || !proof.Known || !proof.Complete {
			return deferUpgrade("preflight_evidence_incomplete")
		}
		if proof.Stage != p.Stage || proof.Stamp != p.Stamp {
			return reject("preflight_evidence_stale")
		}
	}
	// Reserve-then-read is enforced by the reservation owner. Its sample and
	// the final resource seal must belong to this same observation barrier.
	if p.Fleet.Stamp != p.Stamp || p.Census.Stamp != p.Stamp || !validSHA(p.ExpectedDigest) || p.Census.ExpectedDigest != p.ExpectedDigest {
		return reject("preflight_census_stale")
	}
	if p.TargetGeneration != p.Grant.Handle.Generation {
		return reject("preflight_generation_mismatch")
	}
	if err := CheckFleet(p.Fleet, p.Candidate.Release); err != nil {
		return err
	}
	return CheckGenerations(p.Census, p.SourceGeneration, p.TargetGeneration)
}

// LegacyConsumer describes an actual consumer of the old upgrade queue. A
// probe of one broker cannot vouch for the next queue delivery's broker.
type LegacyConsumer struct {
	ID      string
	Capable bool
}

// LegacyPolicy is shared by old upgrade, new tx, cluster child and ReExecOnly
// adapters. The consumer list comes from the fixed complete managed roster;
// Issuer and RouteIssuer identify the single durable journal and dispatch route.
type LegacyPolicy struct {
	Component     upgradetx.Component
	Mode          upgradetx.Mode
	SourceCapable bool
	TargetCapable bool
	Busy          bool
	// These facts come from observations and an actual admission fence. A
	// broker cannot set them from the absence of rows in an old agent reply.
	CensusKnown       bool
	Quiescent         bool
	FenceHeld         bool
	ConsumersKnown    bool
	ConsumersComplete bool
	// Enumeration is fixed under the managed reservation; it includes every
	// queue consumer, not just the leader or the broker answering one probe.
	ExpectedConsumers []string
	Consumers         []LegacyConsumer
	Issuer            string
	RouteIssuer       string
}

// CheckLegacy permits no implicit interrupting mode. Capable->legacy is always
// refused; a legacy idle observation without an admission fence is deferred.
// Explicit migration is agent-only and grants no hot completion or causal proof.
// origin: hot-update S8 plan §7.2 / TestLegacyUpgradePolicy
func CheckLegacy(p LegacyPolicy) error {
	if p.Component != upgradetx.ComponentAgent && p.Component != upgradetx.ComponentBroker {
		return &upgradetx.Error{Code: upgradetx.CodeUnsupportedComponent, Detail: "legacy_component_unsupported"}
	}
	if p.Mode != "" && p.Mode != upgradetx.ModeHot && p.Mode != upgradetx.ModeLegacyInterrupting {
		return reject("legacy_mode_invalid")
	}
	// Losing v2 authority and deduplication readers cannot be waived by an
	// interrupting flag. Reinstalling legacy code is a separate cold operation.
	if p.SourceCapable {
		if !p.TargetCapable {
			return reject("capable_to_legacy")
		}
		if p.Mode == upgradetx.ModeLegacyInterrupting {
			return reject("legacy_source_required")
		}
		return nil
	}
	// A missing field on the old wire invokes this same safe default. Explicit
	// busy evidence rejects before an uncertain idle observation can defer.
	if p.Mode != upgradetx.ModeLegacyInterrupting {
		if p.Busy {
			return reject("legacy_busy")
		}
		if !p.CensusKnown || !p.Quiescent || !p.FenceHeld {
			return deferUpgrade("legacy_quiescence_unproved")
		}
		return nil
	}
	// The broker's first migration is an explicit stop/replace/start operation.
	// Only the agent's legacy forward can use this single-dispatch admission.
	if p.Component != upgradetx.ComponentAgent || !p.TargetCapable {
		return reject("legacy_target_unsupported")
	}
	if !p.ConsumersKnown || !p.ConsumersComplete || len(p.Consumers) == 0 || len(p.ExpectedConsumers) == 0 {
		return reject("legacy_consumers_unknown")
	}
	if !uniqueTokens(p.ExpectedConsumers) {
		return reject("legacy_consumer_unsupported")
	}
	seen := make(map[string]bool, len(p.Consumers))
	for _, consumer := range p.Consumers {
		if consumer.ID == "" || seen[consumer.ID] || !consumer.Capable || !slices.Contains(p.ExpectedConsumers, consumer.ID) {
			return reject("legacy_consumer_unsupported")
		}
		seen[consumer.ID] = true
	}
	// Completeness flags do not excuse dropping an unavailable consumer from
	// the immutable snapshot. That consumer can still receive the next command.
	if len(seen) != len(p.ExpectedConsumers) {
		return reject("legacy_consumers_unknown")
	}
	// Recovery must query the original issuer's durable dispatch intent.
	// Switching to another capable broker does not transfer that causal record.
	if p.Issuer == "" || p.Issuer != p.RouteIssuer || !seen[p.Issuer] {
		return reject("legacy_issuer_route_mismatch")
	}
	return nil
}

func validStamp(stamp upgradetx.Stamp, now int64) bool {
	return stamp.At > 0 && stamp.At <= now && stamp.Sequence > 0 && stamp.Attempt.Generation > 0 && stamp.Attempt.Revision > 0 && validHex(stamp.Attempt.TxID, 32) && validHex(stamp.Attempt.CutID, 32) && validHex(stamp.Nonce, 32) && validSHA(stamp.ScopeDigest) && validOwner(stamp.Producer)
}

func validOwner(owner upgradetx.Owner) bool {
	return owner.BootID != "" && owner.PID > 0 && owner.StartTicks > 0 && owner.InstanceID != ""
}

func validSHA(value string) bool { return validHex(value, 64) }

func validHex(value string, length int) bool {
	if len(value) != length {
		return false
	}
	for _, digit := range value {
		if (digit < '0' || digit > '9') && (digit < 'a' || digit > 'f') {
			return false
		}
	}
	return true
}

func uniqueTokens(tokens []string) bool {
	seen := make(map[string]bool, len(tokens))
	for _, token := range tokens {
		if token == "" || seen[token] {
			return false
		}
		seen[token] = true
	}
	return true
}

func reject(reason string) error {
	// Diagnostics remain static policy reasons, never executable declarations
	// or resource payloads. Public exit projection belongs to the wire owner.
	return &upgradetx.Error{Code: upgradetx.CodeRejected, Detail: reason}
}

func deferUpgrade(reason string) error {
	// This operation has issued no side effect. An endpoint resuming an accepted
	// transaction must project the error using its durable execution record.
	return &upgradetx.Error{Code: upgradetx.CodeDeferred, Detail: reason}
}
