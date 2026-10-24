// Package upgradelegacy owns the deliberately limited legacy migration proof.
// A legacy observation never supplies the three receipts needed for a hot cut.
// origin: hot-update S8 plan §3.2 / §7.2 / P12
package upgradelegacy

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"io"
	"strings"
	"unicode/utf8"

	"github.com/LinZiyang666/tether/internal/upgradetx"
)

const maxProofBytes = 16 * 1024

// EncodeProof encodes only the frozen DTO. The sole issuer journal must include
// these bytes in its checksummed, atomic action write, never in a second file.
func EncodeProof(proof upgradetx.LegacyUpgradeProof) ([]byte, error) {
	if err := validateProof(proof); err != nil {
		return nil, err
	}
	data, err := json.Marshal(proof)
	if err != nil || len(data) > maxProofBytes {
		return nil, corrupt("legacy proof cannot be encoded")
	}
	return data, nil
}

// DecodeProof fails closed on duplicate fields as well as unknown fields. A
// checksum alone cannot resolve two different interpretations of issuer/actor
// identity, and a malformed proof must never become permission to dispatch.
// origin: hot-update S8 plan §2.2 / §3.1
func DecodeProof(data []byte) (upgradetx.LegacyUpgradeProof, error) {
	var proof upgradetx.LegacyUpgradeProof
	if len(data) == 0 || len(data) > maxProofBytes {
		return proof, corrupt("legacy proof size is invalid")
	}
	tokens := json.NewDecoder(bytes.NewReader(data))
	if err := uniqueValue(tokens, 0); err != nil {
		return proof, corrupt("legacy proof JSON is ambiguous")
	}
	if _, err := tokens.Token(); err != io.EOF {
		return proof, corrupt("legacy proof has trailing data")
	}
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&proof); err != nil {
		return upgradetx.LegacyUpgradeProof{}, corrupt("legacy proof schema is invalid")
	}
	if err := validateProof(proof); err != nil {
		return upgradetx.LegacyUpgradeProof{}, err
	}
	return proof, nil
}

func uniqueValue(decoder *json.Decoder, depth int) error {
	if depth > 16 {
		return corrupt("legacy proof nesting is invalid")
	}
	token, err := decoder.Token()
	if err != nil {
		return err
	}
	delim, nested := token.(json.Delim)
	if !nested {
		return nil
	}
	if delim != '{' && delim != '[' {
		return corrupt("legacy proof delimiter is invalid")
	}
	seen := make(map[string]bool)
	for decoder.More() {
		if delim == '{' {
			field, err := decoder.Token()
			if err != nil {
				return err
			}
			name, ok := field.(string)
			// encoding/json accepts case-insensitive aliases. Frozen tags do
			// not: otherwise "state" and "State" become an unseen duplicate.
			if !ok || name != strings.ToLower(name) || seen[name] {
				return corrupt("legacy proof field is repeated")
			}
			seen[name] = true
		}
		if err := uniqueValue(decoder, depth+1); err != nil {
			return err
		}
	}
	_, err = decoder.Token()
	return err
}

// origin: hot-update S8 plan §2.1 / §3.1 / §7.2 / P12
// Missing bindings survive neither decoding nor digest creation. Observations
// may lack old-protocol identity, but such a gap remains explicitly noncausal.
func validateProof(proof upgradetx.LegacyUpgradeProof) error {
	if !validHex(proof.ParentTxID, 32) || !validHex(proof.GrantNonce, 32) ||
		!validHex(proof.DispatchActionID, 32) || !validOwner(proof.Issuer) ||
		!validTarget(proof.TargetKey) || !validText(proof.TargetInstance, 128) ||
		!validHex(proof.SourceSHA, 64) || !validHex(proof.CandidateSHA, 64) ||
		proof.SourceSHA == proof.CandidateSHA {
		return corrupt("legacy proof binding is invalid")
	}
	if proof.LegacyUpgradeID != "" && !validText(proof.LegacyUpgradeID, 128) {
		return corrupt("legacy upgrade identity is invalid")
	}
	if proof.ObservedSHA != "" && !validHex(proof.ObservedSHA, 64) {
		return corrupt("legacy observed image is invalid")
	}
	if proof.ObservedNonce != "" && !validHex(proof.ObservedNonce, 32) {
		return corrupt("legacy observed nonce is invalid")
	}
	switch proof.State {
	case "attempted":
		if proof.Causal || proof.LegacyUpgradeID != "" || proof.ObservedSHA != "" || proof.ObservedNonce != "" {
			return corrupt("legacy intent contains an observation")
		}
	case "observed", "unknown":
		// No old UpgradeID is required for an explicitly uncorrelated fact.
	default:
		return corrupt("legacy proof state is invalid")
	}
	// origin: hot-update S8 plan §3.2 / §7.2 / P12
	// A timestamp, registration or SHA does not manufacture a causal receipt.
	if proof.Causal && (proof.State != "observed" || proof.LegacyUpgradeID == "" ||
		proof.ObservedNonce == "" || proof.ObservedSHA != proof.CandidateSHA) {
		return corrupt("legacy causal receipt is incomplete")
	}
	return nil
}

func intentProof(proof upgradetx.LegacyUpgradeProof) upgradetx.LegacyUpgradeProof {
	proof.LegacyUpgradeID, proof.ObservedSHA, proof.ObservedNonce = "", "", ""
	proof.State, proof.Causal = "attempted", false
	return proof
}

func proofDigest(proof upgradetx.LegacyUpgradeProof) (string, error) {
	data, err := EncodeProof(proof)
	if err != nil {
		return "", err
	}
	digest := sha256.Sum256(data)
	return hex.EncodeToString(digest[:]), nil
}

func validateObservation(saved, observed upgradetx.LegacyUpgradeProof, nonce string) error {
	if err := validateProof(observed); err != nil {
		return err
	}
	// origin: hot-update S8 plan §7.2 / P12
	// Route changes may only lose availability, never claim another issuer's
	// dispatch. Compare every immutable field, including the action identity.
	if intentProof(saved) != intentProof(observed) || observed.ObservedNonce != nonce ||
		(observed.State != "observed" && observed.State != "unknown") {
		return corrupt("legacy observation does not match the dispatch")
	}
	if saved.Causal && (!observed.Causal || saved.LegacyUpgradeID != observed.LegacyUpgradeID) {
		return corrupt("legacy observation changed its causal identity")
	}
	return nil
}

func validHex(value string, length int) bool {
	if len(value) != length {
		return false
	}
	for _, char := range value {
		if (char < '0' || char > '9') && (char < 'a' || char > 'f') {
			return false
		}
	}
	return true
}

func validText(value string, limit int) bool {
	if value == "" || len(value) > limit || strings.TrimSpace(value) != value || !utf8.ValidString(value) {
		return false
	}
	for _, char := range value {
		if char < 32 || char == 127 {
			return false
		}
	}
	return true
}

func validOwner(owner upgradetx.Owner) bool {
	return validText(owner.BootID, 128) && owner.PID > 0 && owner.StartTicks > 0 && validText(owner.InstanceID, 128)
}

func validTarget(key upgradetx.Key) bool {
	return key.Component == upgradetx.ComponentAgent && validText(key.DomainID, 512) &&
		validText(key.SID, 128) && validText(key.ConfiguredNID, 128) &&
		(key.ConfigPath == "" || validText(key.ConfigPath, 1024))
}

func corrupt(detail string) error {
	return &upgradetx.Error{Code: upgradetx.CodeStateCorrupt, Detail: detail}
}
