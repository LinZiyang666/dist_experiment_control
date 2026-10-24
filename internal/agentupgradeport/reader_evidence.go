// Reader evidence is reusable only for the exact verified publication. The
// final gate still checks current owners, required domains and the final image;
// unchanged immutable bytes do not need another hash or supervised execution.
// origin: hot-update S8 plan §5.2 / §6.1 / §10; S8 internal review FREEZE-WINDOW R6
package agentupgradeport

import (
	"maps"
	"path/filepath"
	"slices"
	"time"

	"github.com/LinZiyang666/tether/internal/agentimage"
	"github.com/LinZiyang666/tether/internal/upgradeartifact"
	"github.com/LinZiyang666/tether/internal/upgradeport"
	"github.com/LinZiyang666/tether/internal/upgradetx"
)

type readerEvidence struct {
	release      string
	capabilities agentimage.Capabilities
}

type publicationEvidence struct {
	receipt upgradeartifact.Receipt
	files   map[string]evidenceStamp
}

// candidateReader is called only after targetPath has verified the bytes
// against this grant. Its digest, rather than the pathname, binds declarations.
// A lying or failed probe cannot populate the cache for the expected digest.
func (p *runtimePort) candidateReader(path, sha string) (string, agentimage.Capabilities, error) {
	p.mu.Lock()
	cached := p.probed
	p.mu.Unlock()
	if sha != "" && cached.capabilities.SHA256 == sha {
		return cached.release, cloneCapabilities(cached.capabilities), nil
	}
	release, capabilities, err := p.host.Probe(path)
	if err == nil && capabilities.SHA256 == sha {
		p.mu.Lock()
		p.probed = readerEvidence{release, cloneCapabilities(capabilities)}
		p.mu.Unlock()
	}
	return release, capabilities, err
}

func cloneCapabilities(c agentimage.Capabilities) agentimage.Capabilities {
	c.Domains = slices.Clone(c.Domains)
	c.ReadEpochs = slices.Clone(c.ReadEpochs)
	c.Features = slices.Clone(c.Features)
	return c
}

// verifyPublication keeps Verify's provenance, owner, mode, path and content
// checks. A hit requires the complete receipt and grant, every path component,
// and unchanged inode/change-time/size/mode/owner for both slots and manifest.
// A chmod/write/rename or manifest replacement forces full verification. Reads
// may change atime and unrelated journal writes may change directory times;
// neither changes those bytes or their installation authority.
func (p *runtimePort) verifyPublication(artifact upgradeartifact.Receipt, grant upgradetx.Grant, started bool) error {
	return verifyPublicationUsing(p, artifact, grant, started, upgradeartifact.Verify)
}

// The local verifier dependency lets tests count real Verify calls; production
// always supplies upgradeartifact.Verify, with no global replacement hook.
func verifyPublicationUsing(p *runtimePort, artifact upgradeartifact.Receipt, grant upgradetx.Grant, started bool, verify upgradeartifact.VerifyFunc) error {
	before, statErr := publicationFiles(artifact)
	p.mu.Lock()
	cached := p.publication
	p.mu.Unlock()
	if statErr == nil && artifact.Grant == grant && cached.receipt == artifact && maps.Equal(cached.files, before) &&
		(started || grant.ExpiresAt > time.Now().UnixNano()) {
		return nil
	}
	if err := verify(artifact, grant); err != nil {
		return err
	}
	if statErr == nil {
		after, err := publicationFiles(artifact)
		if err != nil || !maps.Equal(before, after) {
			return reject(ReasonArtifactMismatch)
		}
		p.mu.Lock()
		p.publication = publicationEvidence{artifact, after}
		p.mu.Unlock()
	} // unsupported platforms retain full Verify at every gate
	return nil
}

// Track every ancestor by identity and permissions, so replacing a parent
// with a symlink cannot borrow an earlier verification of its destination.
func publicationFiles(artifact upgradeartifact.Receipt) (map[string]evidenceStamp, error) {
	files := make(map[string]evidenceStamp)
	manifest := filepath.Join(filepath.Dir(artifact.SourcePath), "manifest-"+artifact.ManifestDigest+".json")
	for _, path := range []string{artifact.CanonicalPath, artifact.SourcePath, artifact.TargetPath, manifest} {
		if !filepath.IsAbs(path) || filepath.Clean(path) != path {
			return nil, agentimage.ErrRunningImageDiffers
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

// targetPath binds the artifact to this grant. Forward Start supplies the
// publication receipt; a reverse resolves the recorded previous pin, never a
// pathname of matching bytes from another publication.
func (p *runtimePort) targetPath(grant upgradetx.Grant, artifact upgradeartifact.Receipt, record upgradetx.View, started bool) (string, error) {
	if artifact != (upgradeartifact.Receipt{}) {
		if err := p.verifyPublication(artifact, grant, started); err != nil {
			// An accepted grant may expire between gates; only the exact unchanged
			// publication this adapter verified before acceptance keeps its pins.
			return "", reject(ReasonArtifactMismatch)
		}
		return artifact.TargetPath, nil
	}
	// A reverse grant is prepared before its acceptance: an automatic one
	// shares the forward TxID, a manual one inherits Current's pins at Start.
	pins := record.Metadata.Pins
	if !started {
		view, err := p.host.Journal.Load(p.host.Key, grant.Handle.TxID)
		if err != nil {
			view, err = p.host.Journal.Load(p.host.Key, "")
		}
		if err != nil {
			return "", deferral(ReasonArtifactUnavailable)
		}
		pins = view.Metadata.Pins
	}
	for _, pin := range pins {
		if filepath.IsAbs(pin) && filepath.Base(pin) == grant.TargetSHA {
			if sha, err := agentimage.SHA256OfFile(pin); err == nil && sha == grant.TargetSHA {
				return pin, nil
			}
		}
	}
	return "", deferral(ReasonArtifactUnavailable)
}

// VerifyTargetReader rechecks the exact immutable publication at final prepare.
// The preflight already checked grant authority and hashed both slots. Reusing
// that proof requires unchanged path identities and content metadata; the final
// re-export still runs every retained-domain/reader check. An unbound adapter
// or a reverse pin without publication evidence keeps the full SHA check.
// origin: hot-update S8 plan §6.1 / §10; S8 internal review FREEZE-WINDOW R6
func VerifyTargetReader(runtime upgradeport.RuntimePort, target string, reader agentimage.Capabilities) error {
	if p, ok := runtime.(*runtimePort); ok && p != nil {
		p.mu.Lock()
		artifact := p.publication.receipt
		p.mu.Unlock()
		if artifact.TargetPath == target && reader.SHA256 != "" && artifact.TargetSHA == reader.SHA256 {
			return p.verifyPublication(artifact, artifact.Grant, true)
		}
	}
	return agentimage.VerifyCapabilitySHA(target, reader)
}
