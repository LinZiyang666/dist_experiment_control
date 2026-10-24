package upgradecut

import (
	"bytes"
	"encoding/json"
	"errors"
	"io"
	"path/filepath"
	"unicode/utf8"

	"github.com/LinZiyang666/tether/internal/upgradetx"
)

// The adjunct is metadata in a separate sealed memfd, never an extension of
// agentimage.Bundle or the nine closed broker private kinds. Its version and
// required bit travel with the payload so a reader cannot skip an unknown kind
// merely because a producer labels it optional. P17 owns memfd seal checks and
// the comparison with the actual image, descriptor table and authority census.
// origin: hot-update S8 plan §6.1 / TestPrivateAdjunctCodecClosure
type adjunctEnvelope struct {
	Kind     string `json:"kind"`
	Required bool   `json:"required"`
	State    State  `json:"state"`
}

// These are local allocation ceilings, below the existing sealed-state cap.
// Bounded bytes alone do not bound work spent validating a nested collection.
const (
	maxAdjunctBytes  = 1 << 20
	maxEntries       = 4096
	maxFDIndex       = (1 << 14) - 1
	maxIdentityBytes = 4096
)

// Encode refuses an unsealed or ambiguous scope rather than sorting it here:
// silently changing the producer's order would change the already sealed scope.
// Neither errors nor the public handoff bundle should contain these bytes.
func Encode(state State) ([]byte, error) {
	if err := validateState(state); err != nil {
		return nil, err
	}
	data, err := json.Marshal(adjunctEnvelope{Kind: Version, Required: true, State: state})
	if err != nil || len(data) > maxAdjunctBytes {
		return nil, invalidAdjunct()
	}
	return data, nil
}

// Decode checks the whole closed projection before callers may Take anything.
// Exact re-encoding rejects duplicate and missing fields (including false/zero
// decisions), case-folded names, null collections and alternate encodings that
// encoding/json otherwise accepts. DisallowUnknownFields also covers every
// nested owner, key, authority and expected resource.
// origin: hot-update S8 plan §2.2 / §6.1 / TestPrivateAdjunctCodecClosure
func Decode(data []byte) (State, error) {
	if len(data) == 0 || len(data) > maxAdjunctBytes || !utf8.Valid(data) {
		return State{}, invalidAdjunct()
	}
	var envelope adjunctEnvelope
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.DisallowUnknownFields()
	if decoder.Decode(&envelope) != nil || !errors.Is(decoder.Decode(new(any)), io.EOF) {
		return State{}, invalidAdjunct()
	}
	// An unknown optional kind still represents a different, unreadable cut.
	if envelope.Kind != Version || !envelope.Required {
		return State{}, invalidAdjunct()
	}
	canonical, err := Encode(envelope.State)
	if err != nil || !bytes.Equal(data, canonical) {
		return State{}, invalidAdjunct()
	}
	return envelope.State, nil
}

func invalidAdjunct() error {
	return &upgradetx.Error{Code: upgradetx.CodeRejected, Detail: "invalid upgrade adjunct"}
}

// The wire/disk validators cannot stand in for this boundary. In particular an
// in-memory State passed directly to Take must satisfy the same invariants as
// one returned by Decode. Expected running SHA/current authority comparisons
// require the runtime's facts and remain P17's pre-Take responsibility.
// origin: hot-update S8 plan §5.1 / §6.1
func validateState(state State) error {
	handle := state.Attempt
	if !validKey(handle.Key) || !hexWidth(handle.TxID, 32) || !hexWidth(handle.CutID, 32) ||
		handle.Generation == 0 || handle.Revision == 0 || !validOwner(state.Owner) ||
		!hexWidth(state.SourceSHA, 64) || !hexWidth(state.TargetSHA, 64) ||
		(state.ParentToken != "" && !hexWidth(state.ParentToken, 32)) || state.LockFD > maxFDIndex {
		return invalidAdjunct()
	}
	if !validScope(state.Scope, handle.Key) || state.Authorities == nil || len(state.Authorities) > maxEntries {
		return invalidAdjunct()
	}
	previous := ""
	for _, authority := range state.Authorities {
		if !validText(authority.SID) || authority.SID <= previous || !hexWidth(authority.Incarnation, 64) {
			return invalidAdjunct()
		}
		previous = authority.SID
	}
	// An agent can carry only its own creation identity. Brokers may have zero
	// sessions, but their explicitly empty map is compared with the real census
	// by P17 after OpenCurrent; this codec must not open a second store to check it.
	if handle.Key.Component == upgradetx.ComponentAgent &&
		(len(state.Authorities) != 1 || state.Authorities[0].SID != handle.Key.SID) {
		return invalidAdjunct()
	}
	// Participant SIDs already known to this scope cannot disappear from the
	// map. Additional broker sessions are allowed: participant != session roster.
	for _, participant := range state.Scope.Participants {
		if participant.SID != "" && !hasAuthority(state.Authorities, participant.SID) {
			return invalidAdjunct()
		}
	}
	return nil
}

func validScope(scope upgradetx.Scope, key upgradetx.Key) bool {
	if !hexWidth(scope.ID, 32) || len(scope.Participants) == 0 || len(scope.Participants) > maxEntries ||
		scope.Domains == nil || len(scope.Domains) > 11 || scope.Expected == nil || len(scope.Expected) > maxEntries {
		return false
	}
	found := false
	previous := ""
	for _, participant := range scope.Participants {
		if !validKey(participant) {
			return false
		}
		encoded, _ := json.Marshal(participant)
		identity := string(encoded)
		if identity <= previous {
			return false
		}
		previous = identity
		found = found || participant == key
	}
	if !found {
		return false
	}
	previous = ""
	for _, domain := range scope.Domains {
		if !validDomain(domain) || string(domain) <= previous {
			return false
		}
		previous = string(domain)
	}
	// Upgrade custody and authority are required even when business domains
	// are idle. Actual role-specific image domains are unioned by P17; resources
	// cannot smuggle in an unadvertised domain here.
	// origin: hot-update S8 plan §4.2 / §6.1
	if !hasDomain(scope.Domains, upgradetx.DomainControl) ||
		!hasDomain(scope.Domains, upgradetx.DomainAuthority) || !hasDomain(scope.Domains, upgradetx.DomainUpgrade) {
		return false
	}
	previous = ""
	for _, resource := range scope.Expected {
		identity := string(resource.Domain) + "\x00" + resource.ID
		if !hasDomain(scope.Domains, resource.Domain) || !validText(resource.ID) ||
			!validText(resource.Origin) || !validOwner(resource.Owner) || resource.Generation == 0 ||
			!validText(resource.State) || !validText(resource.Custody) || identity <= previous {
			return false
		}
		previous = identity
	}
	return true
}

func validKey(key upgradetx.Key) bool {
	if !validText(key.DomainID) || (key.SID != "" && !validText(key.SID)) ||
		(key.ConfiguredNID != "" && !validText(key.ConfiguredNID)) ||
		(key.ConfigPath != "" && (!validText(key.ConfigPath) || !filepath.IsAbs(key.ConfigPath) || filepath.Clean(key.ConfigPath) != key.ConfigPath)) {
		return false
	}
	// Reserved infrastructure components have no S8 runtime to import a cut.
	return (key.Component == upgradetx.ComponentAgent && key.SID != "" && key.ConfiguredNID != "") ||
		(key.Component == upgradetx.ComponentBroker && key.ConfigPath != "")
}

func validOwner(owner upgradetx.Owner) bool {
	// Linux boot IDs are canonical UUIDs; transaction/instance IDs are random
	// 128-bit hex. Retaining all four fields prevents PID-reuse adoption.
	boot := owner.BootID
	if len(boot) == 36 {
		if boot[8] != '-' || boot[13] != '-' || boot[18] != '-' || boot[23] != '-' {
			return false
		}
		boot = boot[:8] + boot[9:13] + boot[14:18] + boot[19:23] + boot[24:]
	}
	return hexWidth(boot, 32) && owner.PID > 0 && uint64(owner.PID) <= 1<<31-1 &&
		owner.StartTicks != 0 && hexWidth(owner.InstanceID, 32)
}

func validText(value string) bool {
	if len(value) == 0 || len(value) > maxIdentityBytes || !utf8.ValidString(value) {
		return false
	}
	for _, char := range value {
		if char < 0x20 || char == 0x7f {
			return false
		}
	}
	return true
}

func hexWidth(value string, width int) bool {
	if len(value) != width {
		return false
	}
	for _, char := range value {
		if (char < '0' || char > '9') && (char < 'a' || char > 'f') {
			return false
		}
	}
	return true
}

func hasAuthority(authorities []Authority, sid string) bool {
	for _, authority := range authorities {
		if authority.SID == sid {
			return true
		}
	}
	return false
}

func hasDomain(domains []upgradetx.Domain, domain upgradetx.Domain) bool {
	for _, member := range domains {
		if member == domain {
			return true
		}
	}
	return false
}

func validDomain(domain upgradetx.Domain) bool {
	return domain == upgradetx.DomainControl || domain == upgradetx.DomainExec || domain == upgradetx.DomainPTY ||
		domain == upgradetx.DomainFlow || domain == upgradetx.DomainSS || domain == upgradetx.DomainTransferWorker ||
		domain == upgradetx.DomainTransferTracker || domain == upgradetx.DomainBrokerStore ||
		domain == upgradetx.DomainBrokerRuntime || domain == upgradetx.DomainAuthority || domain == upgradetx.DomainUpgrade
}
