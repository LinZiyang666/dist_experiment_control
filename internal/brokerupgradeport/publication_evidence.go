// Publication evidence keeps verified immutable slots outside the maintenance hold.
// origin: hot-update S8 plan §5.2 / §6.1; S8 internal review upgrade-maketest F5
package brokerupgradeport

import (
	"maps"
	"path/filepath"
	"time"

	"github.com/LinZiyang666/tether/internal/upgradeartifact"
	"github.com/LinZiyang666/tether/internal/upgradetx"
)

type publicationEvidence struct {
	receipt upgradeartifact.Receipt
	files   map[string]evidenceStamp
}

// verifyPublication keeps Verify's provenance, owner, mode, path and content
// checks. A hit requires the complete receipt and grant, every path component,
// and unchanged inode/change-time/size/mode/owner for both slots and manifest.
// A chmod/write/rename or manifest replacement forces full verification. Reads
// may change atime and unrelated journal writes may change directory times;
// neither changes those bytes or their installation authority.
func (p *brokerRuntime) verifyPublication(artifact upgradeartifact.Receipt, grant upgradetx.Grant, started bool) error {
	return verifyPublicationUsing(p, artifact, grant, started, upgradeartifact.Verify)
}

// The local verifier dependency lets tests count real Verify calls; production
// always supplies upgradeartifact.Verify, with no global replacement hook.
func verifyPublicationUsing(p *brokerRuntime, artifact upgradeartifact.Receipt, grant upgradetx.Grant, started bool, verify upgradeartifact.VerifyFunc) error {
	before, statErr := publicationFiles(artifact)
	if statErr != nil {
		// unsupported platforms retain full Verify at every gate
		return verify(artifact, grant)
	}
	p.mu.Lock()
	cached := p.publication
	p.mu.Unlock()
	if artifact.Grant == grant && cached.receipt == artifact && maps.Equal(cached.files, before) &&
		(started || grant.ExpiresAt > time.Now().UnixNano()) {
		return nil
	}
	if err := verify(artifact, grant); err != nil {
		return err
	}
	after, err := publicationFiles(artifact)
	if err != nil || !maps.Equal(before, after) {
		return reject(ReasonArtifactMismatch)
	}
	p.mu.Lock()
	p.publication = publicationEvidence{artifact, after}
	p.mu.Unlock()
	return nil
}

// Track every ancestor by identity and permissions, so replacing a parent
// with a symlink cannot borrow an earlier verification of its destination.
func publicationFiles(artifact upgradeartifact.Receipt) (map[string]evidenceStamp, error) {
	files := make(map[string]evidenceStamp)
	manifest := filepath.Join(filepath.Dir(artifact.SourcePath), "manifest-"+artifact.ManifestDigest+".json")
	for _, path := range []string{artifact.CanonicalPath, artifact.SourcePath, artifact.TargetPath, manifest} {
		if !filepath.IsAbs(path) || filepath.Clean(path) != path {
			return nil, reject(ReasonArtifactMismatch)
		}
		for {
			if _, exists := files[path]; exists {
				break
			}
			stamp, err := readEvidenceStamp(path)
			if err != nil {
				return nil, err
			}
			files[path] = stamp
			parent := filepath.Dir(path)
			if parent == path {
				break
			}
			path = parent
		}
	}
	return files, nil
}
