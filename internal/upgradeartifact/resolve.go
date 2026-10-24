// resolve.go follows the component's durable selector across ordinary cold boot.
// origin: hot-update S8 plan §3.1 / §5.2 / P05
package upgradeartifact

import (
	"os"
	"path/filepath"
	"strings"

	"github.com/LinZiyang666/tether/internal/upgradetx"
)

// Pins gives the journal owner the two executable pins and their exact
// publication manifest. Only that owner decides when the rollback window closes
// and when these pins can be retired; the artifact package never performs GC.
func Pins(receipt Receipt) []string {
	return []string{receipt.SourcePath, receipt.TargetPath,
		filepath.Join(filepath.Dir(receipt.SourcePath), manifestName(receipt.ManifestDigest))}
}

// Resolve consumes the already loaded authoritative journal view. Pins include
// the publication manifest so path authority survives a cold start without
// guessing from os.Executable(), a newest disk slot, or an environment override.
func Resolve(key upgradetx.Key, view upgradetx.View) (string, error) {
	if err := validateKey(key); err != nil {
		return "", err
	}
	if err := validateSelector(key, view); err != nil {
		return "", err
	}
	var selected string
	for _, pin := range view.Metadata.Pins {
		if !cleanAbsolute(pin) || !strings.HasPrefix(filepath.Base(pin), "manifest-") {
			continue
		}
		manifest, err := readManifest(pin, key)
		if err != nil {
			return "", err
		}
		receipt := manifest.Receipt
		receipt.ManifestDigest = strings.TrimSuffix(strings.TrimPrefix(filepath.Base(pin), "manifest-"), ".json")
		if !selectorPublication(receipt, view.Historical) {
			continue // Unrelated historical publications do not authorize this selector.
		}
		// Accepted cold-start state remains usable after Prepare's TTL expires.
		// This checks immutable publication evidence, never restores Start rights.
		if err := verifyReceipt(receipt, receipt.Grant); err != nil {
			return "", err
		}
		var path string
		switch view.Historical.EffectiveSHA {
		case receipt.SourceSHA:
			path = receipt.SourcePath
		case receipt.TargetSHA:
			path = receipt.TargetPath
		default:
			return "", artifactError(upgradetx.CodeStateCorrupt, "effective selector is outside its authorized SHA pair")
		}
		if !containsPin(view.Metadata.Pins, path) || (selected != "" && selected != path) {
			return "", artifactError(upgradetx.CodeStateCorrupt, "effective selector pin is missing or ambiguous")
		}
		selected = path
	}
	if selected == "" {
		return "", artifactError(upgradetx.CodeStateCorrupt, "effective selector publication is missing")
	}
	return selected, nil
}

// ResolveCanonical is the boot adapter for the explicitly registered canonical
// entry. A genuinely absent journal may use it; a corrupt or pending journal
// must go through Resolve and cannot silently start the newest disk bytes.
func ResolveCanonical(canonical string, key upgradetx.Key, view *upgradetx.View) (string, error) {
	if err := validateKey(key); err != nil {
		return "", err
	}
	if !cleanAbsolute(canonical) {
		return "", artifactError(upgradetx.CodeRejected, "canonical boot path is invalid")
	}
	resolved, err := filepath.EvalSymlinks(canonical)
	if err != nil || resolved != canonical {
		return "", artifactError(upgradetx.CodeRejected, "canonical boot alias is not registered")
	}
	// A nil view means the boot adapter proved the unique journal absent.
	// Unknown transaction IDs and corrupt reads must pass a nonnil view.
	if view == nil {
		info, err := os.Lstat(canonical)
		if err != nil || !info.Mode().IsRegular() || info.Mode().Perm()&0111 == 0 || info.Mode().Perm()&0022 != 0 || !trustedOwner(info, key) {
			return "", artifactError(upgradetx.CodeRejected, "canonical boot executable is not installation-controlled")
		}
		return canonical, nil
	}
	path, err := Resolve(key, *view)
	if err != nil {
		return "", err
	}
	// Cross-check the registration supplied by the boot adapter with publication.
	for _, pin := range view.Metadata.Pins {
		if strings.HasPrefix(filepath.Base(pin), "manifest-") {
			manifest, err := readManifest(pin, key)
			if err != nil {
				return "", err
			}
			if selectorPublication(manifest.Receipt, view.Historical) && manifest.Receipt.CanonicalPath != canonical {
				return "", artifactError(upgradetx.CodeStateCorrupt, "publication belongs to another canonical installation")
			}
		}
	}
	return path, nil
}

func validateSelector(key upgradetx.Key, view upgradetx.View) error {
	record := view.Historical
	// origin: S8 internal review rv3-leaves F10
	// Pins authorize bytes, not execution custody. An interrupted activation
	// must recover its exact cut before ordinary canonical boot can use them.
	if !upgradetx.ColdSelectionSafe(record) {
		return artifactError(upgradetx.CodeUnsettled, "pending selector or activation requires current-cut recovery")
	}
	if view.Code != "" || record.Version != upgradetx.RecordVersion || record.Key != key || record.Grant.Handle.Key != key ||
		record.Revision == 0 || !validSHA(record.EffectiveSHA) || view.Metadata.TxID != record.Grant.Handle.TxID ||
		len(view.Metadata.Pins) == 0 || len(view.Metadata.Pins) > 256 {
		return artifactError(upgradetx.CodeStateCorrupt, "effective selector journal identity or state is invalid")
	}
	current := view.Observation.Current.Handle
	if (view.Observation.EffectiveSHA != "" && view.Observation.EffectiveSHA != record.EffectiveSHA) ||
		(current.TxID != "" && current != record.Grant.Handle && current != record.Forward.Handle && current != record.Reverse.Handle) {
		return artifactError(upgradetx.CodeStateCorrupt, "historical selector differs from the current transaction")
	}
	return validateGrantShape(record.Grant)
}

// An accepted reverse may reuse the original pinned images without asking root
// to publish them again. Its own recorded attempt binds the opposite SHA pair
// and recorded prev. This read authorization never allows Verify/Start to adopt
// an old publication under a new grant merely because its SHA bytes are equal.
// origin: hot-update S8 plan §4.1 / §5.2 / §8.1
func selectorPublication(receipt Receipt, record upgradetx.Record) bool {
	if receipt.Grant == record.Grant {
		return true
	}
	if receipt.Grant.Handle.Key != record.Key || receipt.SourceSHA != record.RecordedPreviousSHA ||
		receipt.SourceSHA != record.Grant.TargetSHA || receipt.TargetSHA != record.Grant.SourceSHA {
		return false
	}
	for _, attempt := range []upgradetx.Attempt{record.Forward, record.Reverse} {
		if attempt.Direction == upgradetx.DirectionReverse && sameAttemptIdentity(attempt.Handle, record.Grant.Handle) &&
			attempt.SourceSHA == record.Grant.SourceSHA && attempt.TargetSHA == record.Grant.TargetSHA {
			return true
		}
	}
	return false
}

func sameAttemptIdentity(actual, expected upgradetx.Handle) bool {
	// Apply advances the attempt revision; the inert original grant is immutable.
	return actual.Key == expected.Key && actual.TxID == expected.TxID && actual.CutID == expected.CutID && actual.Generation == expected.Generation
}

func containsPin(pins []string, path string) bool {
	for _, pin := range pins {
		if pin == path {
			return true
		}
	}
	return false
}
