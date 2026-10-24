// Package upgradetx defines the shared journal, collector and engine evidence
// contract. Revision is a write CAS; Key/TxID/CutID/Generation identify a cut.
// Expected commits the original set, while every census may add late identities.
// Terminal durable/released rows have no execution custody; retained terminals
// still occupy a generation. Inapplicability is an explicit ownerless scope row.
// Current exports retain the scope namespace and original identities while
// including admissions and current custody; they need not equal bootstrap bytes.
// origin: S8 internal review rv-fault F5
// origin: S8 internal review rv-txn F04 F05 F07 F08 F09 F10 F11 F12
package upgradetx

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"reflect"
	"slices"
	"sort"
)

// ValidateKey is role aware; an agent may omit its configuration path. A present
// path remains part of the namespace and must be absolute and canonical.
func ValidateKey(key Key) error {
	if !validKey(key) {
		return &Error{Code: CodeRejected, Detail: "invalid component identity"}
	}
	return nil
}

// SameCut deliberately excludes the journal CAS revision.
func SameCut(a, b Handle) bool { return sameCut(a, b) }

// ValidateRecord is used before writes and after decoding. It validates both
// directions together; validating only Current could hide an invalid forward.
func ValidateRecord(record Record) error {
	if !validRecord(record) {
		return &Error{Code: CodeStateCorrupt, Detail: "invalid upgrade execution record", Accepted: true}
	}
	return nil
}

// CanonicalScope has one encoding across grants, receipts and disk. Empty sets
// encode as [], participants sort by identity fields and UID, and duplicates
// are rejected rather than removed. Applicable idle domains need no fake row.
func CanonicalScope(scope Scope) (Scope, error) {
	scope.Participants = append([]Key{}, scope.Participants...)
	scope.Domains = append([]Domain{}, scope.Domains...)
	scope.Expected = append([]Resource{}, scope.Expected...)
	sort.Slice(scope.Participants, func(i, j int) bool { return keyOrder(scope.Participants[i]) < keyOrder(scope.Participants[j]) })
	slices.Sort(scope.Domains)
	sort.Slice(scope.Expected, func(i, j int) bool { return resourceIdentity(scope.Expected[i]) < resourceIdentity(scope.Expected[j]) })
	if len(scope.Participants) > 4096 || len(scope.Expected) > 4096 || !validScope(scope) {
		return Scope{}, &Error{Code: CodeRejected, Detail: "invalid original scope"}
	}
	return scope, nil
}

// ScopeDigest and ExpectedDigest use the same canonical scope at every seam.
func ScopeDigest(scope Scope) (string, error) {
	s, err := CanonicalScope(scope)
	if err != nil {
		return "", err
	}
	return evidenceDigest(s), nil
}
func ExpectedDigest(scope Scope) (string, error) {
	s, err := CanonicalScope(scope)
	if err != nil {
		return "", err
	}
	return evidenceDigest(s.Expected), nil
}
func evidenceDigest(v any) string {
	b, _ := json.Marshal(v)
	sum := sha256.Sum256(b)
	return hex.EncodeToString(sum[:])
}

// ResourceReleased means no remaining execution or retained worker custody.
// Durable is the original collector spelling; released is the journal spelling.
func ResourceReleased(r Resource) bool {
	return r.State == "terminal" && (r.Custody == "released" || r.Custody == "durable")
}
func ResourceNotApplicable(r Resource) bool {
	return r.State == "not_applicable" && r.Custody == "not_applicable" && r.Owner == (Owner{}) && r.Generation == 0
}
func ValidateResource(r Resource) error {
	domains := []Domain{DomainAuthority, DomainBrokerRuntime, DomainBrokerStore, DomainControl, DomainExec, DomainFlow, DomainPTY, DomainSS, DomainTransferTracker, DomainTransferWorker, DomainUpgrade}
	if !validResources([]Resource{r}, domains) {
		return unsettled("invalid resource disposition")
	}
	return nil
}

// CensusCovers requires the complete original identity set, permits admissions,
// and forbids changing an applicable identity into an exclusion (or vice versa).
func CensusCovers(scope Scope, census Census) bool {
	if !validResources(census.Resources, scope.Domains) || !containsResources(census.Resources, scope.Expected) {
		return false
	}
	expected := make(map[string]Resource, len(scope.Expected))
	excluded := make(map[Domain]bool)
	for _, row := range scope.Expected {
		expected[resourceIdentity(row)] = row
		if ResourceNotApplicable(row) {
			excluded[row.Domain] = true
		}
	}
	for _, r := range census.Resources {
		e, found := expected[resourceIdentity(r)]
		if excluded[r.Domain] && !ResourceNotApplicable(r) || found && ResourceNotApplicable(r) != ResourceNotApplicable(e) || !found && ResourceNotApplicable(r) {
			return false
		}
	}
	return true
}

// SameResources compares sets of facts, including custody and origin. Empty
// runtime and decoded slices may differ in allocation but never in meaning.
// Duplicates cannot cancel a missing identity on either side.
// origin: S8 internal review rv-txn F09 F10
func SameResources(a, b []Resource) bool {
	if len(a) != len(b) {
		return false
	}
	rows := make(map[string]Resource, len(a))
	for _, r := range a {
		id := string(r.Domain) + "\x00" + r.ID
		if _, found := rows[id]; found {
			return false
		}
		rows[id] = r
	}
	for _, r := range b {
		id := string(r.Domain) + "\x00" + r.ID
		if old, found := rows[id]; !found || old != r {
			return false
		}
		delete(rows, id)
	}
	return len(rows) == 0
}

// SameCensus preserves the entire stamped observation while comparing its
// collections as sets. It is not a substitute for validating either census.
func SameCensus(a, b Census) bool {
	if a.Stamp != b.Stamp || a.Known != b.Known || a.Complete != b.Complete || a.ExpectedDigest != b.ExpectedDigest || a.Admitting != b.Admitting || len(a.OldOwners) != len(b.OldOwners) || !SameResources(a.Resources, b.Resources) {
		return false
	}
	owners := make(map[Owner]bool, len(a.OldOwners))
	for _, o := range a.OldOwners {
		if owners[o] {
			return false
		}
		owners[o] = true
	}
	for _, o := range b.OldOwners {
		if !owners[o] {
			return false
		}
		delete(owners, o)
	}
	return len(owners) == 0
}

// ValidateCensus and ValidateReceipt share disposition and commitment checks
// with Transition. Producer authentication and query freshness remain external.
func ValidateCensus(a Attempt, c Census, settled bool) error {
	if !validCensus(a, c, settled) {
		return unsettled("invalid census evidence")
	}
	return nil
}
func ValidateReceipt(a Attempt, r Receipt, kind ReceiptKind) error {
	if !validReceipt(a, r, kind) {
		return unsettled("invalid receipt evidence")
	}
	return nil
}

// ValidateDeath separates the dead subject from its independent live producer.
// The observer must prove all retained original and late custody terminal; a
// reaped PID alone, reused identity, future stamp or another cut never suffices.
func ValidateDeath(a Attempt, owner Owner, d DeathProof, now, maxAge int64) error {
	c := d.Custody
	if !d.Reaped || !d.NoOwnedFDs || d.Owner != owner || d.Stamp.Producer == owner ||
		d.Stamp.Attempt != a.Handle || d.Stamp != c.Stamp || d.Stamp.At > now || now-d.Stamp.At > maxAge ||
		!validCensus(a, c, false) || c.Admitting != 0 || len(c.OldOwners) != 0 {
		return unsettled("owner death or custody is unproven")
	}
	for _, r := range c.Resources {
		if !ResourceReleased(r) && !ResourceNotApplicable(r) {
			return unsettled("dead owner retains custody")
		}
	}
	for _, receipt := range []Receipt{a.Control, a.Resources, a.Released} {
		if !reflect.DeepEqual(receipt, Receipt{}) && !containsResources(c.Resources, receipt.Census.Resources) {
			return unsettled("death census lost late custody")
		}
	}
	return nil
}

// RecordForwardRelease is a CAS payload for salvage after reverse preparation.
// It cannot replace another direction's receipt or authorize reverse effects.
func RecordForwardRelease(record Record, receipt Receipt) (Record, error) {
	next := cloneRecord(record)
	if emptyAttempt(next.Forward) || emptyAttempt(next.Reverse) || next.Reverse.CutStage != CutNone && next.Reverse.CutStage != CutPristine ||
		receipt.Kind != ReceiptOldGenerationReleased {
		return Record{}, unsettled("forward retirement is outside salvage")
	}
	if err := applyReceipt(&next.Forward, receipt); err != nil {
		return Record{}, err
	}
	next.Revision++
	next.Reverse.Handle.Revision = next.Revision
	if !validRecord(next) {
		return Record{}, unsettled("invalid forward retirement")
	}
	return next, nil
}

// ValidateAttempt is the joint state predicate used by disk lifecycle checks.
func ValidateAttempt(a Attempt) error {
	if !validAttempt(a) {
		return unsettled("invalid attempt state")
	}
	return nil
}

// ValidateExportScope applies the census superset contract to an exported cut.
// Participants/domains remain authorization; Expected is current resource state.
// origin: S8 internal review rv-fault F5
func ValidateExportScope(original, exported Scope) error {
	a, err := CanonicalScope(original)
	if err != nil {
		return err
	}
	b, err := CanonicalScope(exported)
	if err != nil {
		return err
	}
	if a.ID != b.ID || !reflect.DeepEqual(a.Participants, b.Participants) || !reflect.DeepEqual(a.Domains, b.Domains) ||
		!CensusCovers(a, Census{Resources: b.Expected}) {
		return unsettled("current export lost scope authority or custody")
	}
	return nil
}
