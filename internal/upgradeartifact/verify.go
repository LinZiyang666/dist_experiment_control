// verify.go binds the actual immutable bytes to the complete original grant.
// origin: hot-update S8 plan §3.1 / §3.2 / §5.2 / P05
package upgradeartifact

import (
	"bytes"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"io"
	"os"
	"path/filepath"
	"time"

	"github.com/LinZiyang666/tether/internal/upgradetx"
)

const maxManifestBytes = 64 << 10

// This disk DTO is separate from execution state. The receipt digest is empty
// inside its own hashed envelope to avoid a self-referential checksum.
type publicationManifest struct {
	Version        uint32  `json:"version"`
	Receipt        Receipt `json:"receipt"`
	SourceManifest string  `json:"source_manifest"`
	TargetManifest string  `json:"target_manifest"`
}

// Verify checks an unexpired authorization, the persisted publication and both
// executable files. Matching SHA pairs never replace exact grant equality.
func Verify(receipt Receipt, grant upgradetx.Grant) error {
	if err := validateGrant(grant); err != nil {
		return err
	}
	return verifyReceipt(receipt, grant)
}

func verifyReceipt(receipt Receipt, grant upgradetx.Grant) error {
	// origin: hot-update S8 plan §5.2 / M16
	if receipt.Grant != grant || receipt.SourceSHA != grant.SourceSHA || receipt.TargetSHA != grant.TargetSHA || !validSHA(receipt.ManifestDigest) {
		return artifactError(upgradetx.CodeRejected, "artifact receipt belongs to another authorization")
	}
	directory := filepath.Dir(receipt.SourcePath)
	location := Location{CanonicalPath: receipt.CanonicalPath, Directory: directory}
	if receipt.SourcePath != filepath.Join(directory, grant.SourceSHA) || receipt.TargetPath != filepath.Join(directory, grant.TargetSHA) {
		return artifactError(upgradetx.CodeRejected, "artifact receipt paths are outside their SHA slots")
	}
	if err := validateLocation(location, grant.Handle.Key); err != nil {
		return err
	}
	manifest, err := readManifest(filepath.Join(directory, manifestName(receipt.ManifestDigest)), grant.Handle.Key)
	if err != nil {
		return err
	}
	expected := receipt
	expected.ManifestDigest = ""
	if manifest.Receipt != expected || manifest.Version != 1 || !validSHA(manifest.SourceManifest) || !validSHA(manifest.TargetManifest) {
		return artifactError(upgradetx.CodeRejected, "publication manifest does not match the artifact receipt")
	}
	for _, image := range []struct{ path, sha string }{{receipt.SourcePath, grant.SourceSHA}, {receipt.TargetPath, grant.TargetSHA}} {
		if err := verifyImage(image.path, image.sha, grant.Handle.Key); err != nil {
			return err
		}
	}
	return nil
}

func readManifest(path string, key upgradetx.Key) (publicationManifest, error) {
	var manifest publicationManifest
	file, err := openImmutable(path, key, 0444)
	if err != nil {
		return manifest, err
	}
	defer func() { _ = file.Close() }()
	data, err := io.ReadAll(io.LimitReader(file, maxManifestBytes+1))
	if err != nil || len(data) > maxManifestBytes || filepath.Base(path) != manifestName(digest(data)) {
		return manifest, artifactError(upgradetx.CodeStateCorrupt, "publication manifest checksum or size is invalid")
	}
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&manifest); err != nil {
		return manifest, artifactError(upgradetx.CodeStateCorrupt, "publication manifest is invalid")
	}
	// Exact writer encoding also rejects duplicate keys and trailing JSON, which
	// DisallowUnknownFields alone would silently accept or leave unread.
	canonical, err := json.Marshal(manifest)
	if err != nil || !bytes.Equal(canonical, data) || manifest.Version != 1 || manifest.Receipt.ManifestDigest != "" {
		return manifest, artifactError(upgradetx.CodeStateCorrupt, "publication manifest encoding or version is invalid")
	}
	return manifest, nil
}

func verifyImage(path, expectedSHA string, key upgradetx.Key) error {
	file, err := openImmutable(path, key, 0555)
	if err != nil {
		return err
	}
	defer func() { _ = file.Close() }()
	hash := sha256.New()
	n, err := io.Copy(hash, io.LimitReader(file, maxArtifactBytes+1))
	if err != nil || n == 0 || n > maxArtifactBytes || hex.EncodeToString(hash.Sum(nil)) != expectedSHA {
		return artifactError(upgradetx.CodeRejected, "immutable artifact SHA readback failed")
	}
	return nil
}

func openImmutable(path string, key upgradetx.Key, mode os.FileMode) (*os.File, error) {
	before, err := os.Lstat(path)
	if err != nil || !before.Mode().IsRegular() || before.Mode().Perm() != mode || !trustedOwner(before, key) {
		return nil, artifactError(upgradetx.CodeRejected, "artifact is missing or is not an immutable owner-controlled file")
	}
	file, err := os.Open(path)
	if err != nil {
		return nil, artifactError(upgradetx.CodeRejected, "immutable artifact cannot be opened")
	}
	after, err := file.Stat()
	if err != nil || !os.SameFile(before, after) || after.Mode().Perm() != mode || !trustedOwner(after, key) {
		_ = file.Close()
		return nil, artifactError(upgradetx.CodeRejected, "immutable artifact changed while being opened")
	}
	return file, nil
}

func validateGrant(grant upgradetx.Grant) error {
	if err := validateGrantShape(grant); err != nil {
		return err
	}
	if grant.ExpiresAt <= time.Now().UnixNano() {
		return artifactError(upgradetx.CodeGrantExpired, "publication authorization has expired")
	}
	return nil
}

func validateGrantShape(grant upgradetx.Grant) error {
	key := grant.Handle.Key
	if err := validateKey(key); err != nil {
		return err
	}
	if !validID(grant.Handle.TxID) || !validID(grant.Handle.CutID) || grant.Handle.Generation == 0 || grant.Handle.Revision == 0 ||
		!validSHA(grant.SourceSHA) || !validSHA(grant.TargetSHA) || !validSHA(grant.ScopeDigest) || !validSHA(grant.CapabilityDigest) ||
		grant.ClientNonce == "" || len(grant.ClientNonce) > 128 || grant.ActorFP == "" || len(grant.ActorFP) > 128 ||
		grant.SourceRelease == "" || len(grant.SourceRelease) > 128 || grant.TargetRelease == "" || len(grant.TargetRelease) > 128 ||
		grant.Issuer.BootID == "" || len(grant.Issuer.BootID) > 128 || grant.Issuer.PID <= 0 || grant.Issuer.StartTicks == 0 ||
		grant.Issuer.InstanceID == "" || len(grant.Issuer.InstanceID) > 128 || grant.ExpiresAt <= 0 || grant.Mode != upgradetx.ModeHot {
		return artifactError(upgradetx.CodeRejected, "publication authorization identity or proofs are invalid")
	}
	if (grant.ParentTxID == "") != (grant.ParentSlot == "") ||
		(grant.ParentTxID != "" && (!validID(grant.ParentTxID) || len(grant.ParentSlot) > 128)) {
		return artifactError(upgradetx.CodeRejected, "publication parent authorization is invalid")
	}
	return nil
}

// origin: S8 internal review rv-txn F07
func validateKey(key upgradetx.Key) error {
	if key.Component != upgradetx.ComponentAgent && key.Component != upgradetx.ComponentBroker {
		return artifactError(upgradetx.CodeUnsupportedComponent, "artifact publication requires an agent or broker")
	}
	return upgradetx.ValidateKey(key)
}

func validID(value string) bool  { return len(value) == 32 && lowerHex(value) }
func validSHA(value string) bool { return len(value) == 64 && lowerHex(value) }
func lowerHex(value string) bool {
	for _, c := range value {
		if (c < '0' || c > '9') && (c < 'a' || c > 'f') {
			return false
		}
	}
	return true
}

func digest(data []byte) string {
	sum := sha256.Sum256(data)
	return hex.EncodeToString(sum[:])
}

func randomID() string               { return rand.Text() }
func manifestName(sha string) string { return "manifest-" + sha + ".json" }
func artifactError(code, detail string) error {
	return &upgradetx.Error{Code: code, Detail: detail, Accepted: false}
}
