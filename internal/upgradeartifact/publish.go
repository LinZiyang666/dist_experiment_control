// publish.go owns immutable artifact publication, never the daemon journal.
// origin: hot-update S8 plan §3.2 / §5.2 / P05
package upgradeartifact

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"os"
	"path/filepath"
	"syscall"

	"github.com/LinZiyang666/tether/internal/upgradelock"
	"github.com/LinZiyang666/tether/internal/upgradetx"
)

const maxArtifactBytes = 256 << 20

// Location comes from trusted installation registration, not the executing
// slot, an environment variable, or the caller's grant file.
type Location struct {
	CanonicalPath string
	Directory     string
}

// PublicationPort supplies the integration-owned authority facts that the
// frozen Grant intentionally does not carry. Authorize must check the original
// stored grant, local peer/issuer boot+PID+ticks, running source SHA and parent
// slot. CheckImage must check the real manifest and decoder against the grant's
// capability digest. Reserve uses the host reservation, borrowing its original
// OFD for a registered parent rather than taking a second flock.
// No method writes execution state or starts a service.
type PublicationPort interface {
	Authorize(context.Context, upgradetx.Grant) (Location, error)
	Reserve(context.Context, upgradetx.Grant) (upgradelock.Lease, error)
	CheckImage(context.Context, string, string, string, string) (string, error)
}

type publicationPortKey struct{}

// WithPublicationPort binds an explicitly constructed local authority to one
// stage invocation. There is no global hook or implicit filesystem fallback.
func WithPublicationPort(ctx context.Context, port PublicationPort) context.Context {
	return context.WithValue(ctx, publicationPortKey{}, port)
}

// Publish publishes both SHA slots and a grant-bound manifest. Returning a
// receipt proves readback and durability, not acceptance or Start. A failed
// publication may leave immutable objects; it never deletes journal or pins.
func Publish(ctx context.Context, grant upgradetx.Grant, source, target io.Reader) (receipt Receipt, err error) {
	return publishWithReceipt(ctx, grant, source, target, (*os.File).Sync, true)
}

// The local file operation is an explicit dependency of the publication
// algorithm, like the existing binaryupgrade durability boundary. The public
// entry always supplies real Sync; no environment or context can replace it.
func publish(ctx context.Context, grant upgradetx.Grant, source, target io.Reader, syncFile func(*os.File) error) (receipt Receipt, err error) {
	return publishWithReceipt(ctx, grant, source, target, syncFile, false)
}

func publishWithReceipt(ctx context.Context, grant upgradetx.Grant, source, target io.Reader, syncFile func(*os.File) error, register bool) (receipt Receipt, err error) {
	if ctx == nil {
		return Receipt{}, artifactError(upgradetx.CodeRejected, "missing publication context")
	}
	if err := validateGrant(grant); err != nil {
		return Receipt{}, err
	}
	port, ok := ctx.Value(publicationPortKey{}).(PublicationPort)
	if !ok || port == nil {
		return Receipt{}, artifactError(upgradetx.CodeDeferred, "trusted publication authority is unavailable")
	}
	location, err := port.Authorize(ctx, grant)
	if err != nil {
		return Receipt{}, err
	}
	lease, err := port.Reserve(ctx, grant)
	if err != nil {
		return Receipt{}, err
	}
	if lease == nil {
		return Receipt{}, artifactError(upgradetx.CodeRejected, "publication reservation is missing")
	}
	defer func() {
		if closeErr := lease.Close(); closeErr != nil && err == nil {
			receipt, err = Receipt{}, artifactError(upgradetx.CodeUnsettled, "publication reservation release failed")
		}
	}()
	if lease.DomainID() != grant.Handle.Key.DomainID {
		return Receipt{}, artifactError(upgradetx.CodeRejected, "publication reservation belongs to another domain")
	}
	if err := validateLocation(location, grant.Handle.Key); err != nil {
		return Receipt{}, err
	}
	root, err := os.OpenRoot(location.Directory)
	if err != nil {
		return Receipt{}, artifactError(upgradetx.CodeDeferred, "artifact directory is unavailable")
	}
	defer func() { _ = root.Close() }()
	sourcePath := filepath.Join(location.Directory, grant.SourceSHA)
	targetPath := filepath.Join(location.Directory, grant.TargetSHA)
	sourceManifest, targetManifest, err := publishImages(ctx, root, port, grant, source, target, syncFile)
	if err != nil {
		return Receipt{}, err
	}
	if !validSHA(sourceManifest) || !validSHA(targetManifest) {
		return Receipt{}, artifactError(upgradetx.CodeRejected, "image decoder manifest proof is missing")
	}
	// Revalidate after slow readers/probes while the reservation is still held.
	// Expiry, issuer death or a changed parent slot cannot acquire a late receipt.
	// origin: hot-update S8 plan §3.1 / §5.2
	after, err := port.Authorize(ctx, grant)
	if err != nil {
		return Receipt{}, err
	}
	if after != location {
		return Receipt{}, artifactError(upgradetx.CodeRejected, "installation registration changed during publication")
	}
	if err := validateGrant(grant); err != nil {
		return Receipt{}, err
	}
	if err := ctx.Err(); err != nil {
		return Receipt{}, err
	}
	receipt = Receipt{Grant: grant, CanonicalPath: location.CanonicalPath, SourcePath: sourcePath,
		TargetPath: targetPath, SourceSHA: grant.SourceSHA, TargetSHA: grant.TargetSHA}
	manifest := publicationManifest{Version: 1, Receipt: receipt, SourceManifest: sourceManifest, TargetManifest: targetManifest}
	data, err := json.Marshal(manifest)
	if err != nil {
		return Receipt{}, artifactError(upgradetx.CodeRejected, "publication manifest cannot be encoded")
	}
	receipt.ManifestDigest = digest(data)
	if err := publishObject(ctx, root, manifestName(receipt.ManifestDigest), data, syncFile); err != nil {
		return Receipt{}, err
	}
	if register {
		// Receipt persistence ends with ReadPublication's full manifest and
		// two-image SHA verification. Repeating Verify immediately before it
		// reads both complete binaries again without adding a final-boundary
		// check. Keep the single verification after the last directory Sync.
		// origin: hot-update S8 plan §3.2 / §5.2 / S8 internal review ROOT-STAGE-LATENCY F2
		if err := publishReceipt(ctx, receipt, syncFile); err != nil {
			return Receipt{}, err
		}
	} else if err := Verify(receipt, grant); err != nil {
		return Receipt{}, err
	}
	// ReadPublication also serves accepted historical grants, so it deliberately
	// ignores TTL. A new publication must still be unexpired after slow Syncs.
	// origin: hot-update S8 plan §3.1 / §5.2 / S8 internal review ROOT-STAGE-LATENCY F2
	if err := validateGrant(grant); err != nil {
		return Receipt{}, err
	}
	return receipt, nil
}

// Independent images may overlap their copy, sync, SHA readback and probe.
// Only an explicitly concurrent port opts in: older ports can hold mutable
// decoder state, so the frozen PublicationPort keeps its serial contract.
// Both checks join before authority is revalidated or the lease/root closes,
// including on refusal and cancellation. An inert SHA slot may survive a
// failed sibling check; neither a manifest nor a receipt may survive it.
// origin: hot-update S8 plan §5.2 / S8 internal review PREPARE-LATENCY R-10
func publishImages(ctx context.Context, root *os.Root, port PublicationPort, grant upgradetx.Grant, source, target io.Reader, syncFile func(*os.File) error) (string, string, error) {
	check := func(reader io.Reader, sha, release string) (string, error) {
		if err := publishImage(ctx, root, reader, sha, grant.Handle.Key, syncFile); err != nil {
			return "", err
		}
		return port.CheckImage(ctx, filepath.Join(root.Name(), sha), sha, release, grant.CapabilityDigest)
	}
	var sourceManifest, targetManifest string
	var sourceErr, targetErr error
	if parallel, ok := port.(interface{ ParallelImages() bool }); ok && parallel.ParallelImages() {
		done := make(chan struct{})
		go func() {
			defer close(done)
			sourceManifest, sourceErr = check(source, grant.SourceSHA, grant.SourceRelease)
		}()
		targetManifest, targetErr = check(target, grant.TargetSHA, grant.TargetRelease)
		<-done
	} else {
		// Preserve the original ordering for ports that did not opt in:
		// both durable slots exist before either decoder is consulted.
		if err := publishImage(ctx, root, source, grant.SourceSHA, grant.Handle.Key, syncFile); err != nil {
			return "", "", err
		}
		if err := publishImage(ctx, root, target, grant.TargetSHA, grant.Handle.Key, syncFile); err != nil {
			return "", "", err
		}
		sourceManifest, sourceErr = port.CheckImage(ctx, filepath.Join(root.Name(), grant.SourceSHA), grant.SourceSHA, grant.SourceRelease, grant.CapabilityDigest)
		if sourceErr == nil {
			targetManifest, targetErr = port.CheckImage(ctx, filepath.Join(root.Name(), grant.TargetSHA), grant.TargetSHA, grant.TargetRelease, grant.CapabilityDigest)
		}
	}
	if sourceErr != nil {
		return "", "", sourceErr
	}
	return sourceManifest, targetManifest, targetErr
}

func publishImage(ctx context.Context, root *os.Root, reader io.Reader, expectedSHA string, key upgradetx.Key, syncFile func(*os.File) error) error {
	if reader == nil {
		return artifactError(upgradetx.CodeRejected, "artifact reader is missing")
	}
	file, name, err := temporaryFile(root)
	if err != nil {
		return err
	}
	defer func() { _ = file.Close(); _ = root.Remove(name) }()
	hash := sha256.New()
	n, err := io.Copy(io.MultiWriter(file, hash), io.LimitReader(contextReader{ctx, reader}, maxArtifactBytes+1))
	if err != nil || n == 0 || n > maxArtifactBytes || hex.EncodeToString(hash.Sum(nil)) != expectedSHA {
		return artifactError(upgradetx.CodeRejected, "artifact bytes do not match the authorized SHA or size")
	}
	if err := finishObject(root, file, name, expectedSHA, 0555, syncFile); err != nil {
		return err
	}
	return verifyImage(filepath.Join(root.Name(), expectedSHA), expectedSHA, key)
}

type contextReader struct {
	ctx    context.Context
	reader io.Reader
}

func (r contextReader) Read(data []byte) (int, error) {
	if err := r.ctx.Err(); err != nil {
		return 0, err
	}
	return r.reader.Read(data)
}

func publishObject(ctx context.Context, root *os.Root, name string, data []byte, syncFile func(*os.File) error) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	file, temporary, err := temporaryFile(root)
	if err != nil {
		return err
	}
	defer func() { _ = file.Close(); _ = root.Remove(temporary) }()
	if _, err := file.Write(data); err != nil {
		return artifactError(upgradetx.CodeDeferred, "artifact manifest write failed")
	}
	return finishObject(root, file, temporary, name, 0444, syncFile)
}

func temporaryFile(root *os.Root) (*os.File, string, error) {
	for range 8 {
		name := ".stage-" + randomID()
		file, err := root.OpenFile(name, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0600)
		if err == nil {
			return file, name, nil
		}
		if !errors.Is(err, os.ErrExist) {
			break
		}
	}
	return nil, "", artifactError(upgradetx.CodeDeferred, "artifact temporary file cannot be created")
}

func finishObject(root *os.Root, file *os.File, temporary, name string, mode os.FileMode, syncFile func(*os.File) error) error {
	if err := file.Chmod(mode); err != nil {
		return artifactError(upgradetx.CodeDeferred, "artifact permissions cannot be sealed")
	}
	if err := syncFile(file); err != nil {
		return artifactError(upgradetx.CodeDeferred, "artifact file sync failed")
	}
	if err := file.Close(); err != nil {
		return artifactError(upgradetx.CodeDeferred, "artifact file close failed")
	}
	// Link is a no-replace publication: even a concurrent writer cannot replace
	// a pinned executable inode. Existing names are checked by final readback.
	// origin: hot-update S8 plan §3.2 / §5.2
	if err := root.Link(temporary, name); err != nil && !errors.Is(err, os.ErrExist) {
		return artifactError(upgradetx.CodeDeferred, "immutable artifact publication failed")
	}
	directory, err := root.Open(".")
	if err != nil {
		return artifactError(upgradetx.CodeDeferred, "artifact directory cannot be opened for sync")
	}
	defer func() { _ = directory.Close() }()
	if err := syncFile(directory); err != nil {
		return artifactError(upgradetx.CodeDeferred, "artifact directory sync failed")
	}
	return nil
}

func validateLocation(location Location, key upgradetx.Key) error {
	if !cleanAbsolute(location.CanonicalPath) || !cleanAbsolute(location.Directory) || location.Directory == filepath.Dir(location.CanonicalPath) {
		return artifactError(upgradetx.CodeRejected, "artifact installation paths are invalid")
	}
	canonical, err := filepath.EvalSymlinks(location.CanonicalPath)
	if err != nil || canonical != location.CanonicalPath {
		return artifactError(upgradetx.CodeRejected, "canonical installation alias is not registered")
	}
	directory, err := filepath.EvalSymlinks(location.Directory)
	if err != nil || directory != location.Directory {
		return artifactError(upgradetx.CodeRejected, "artifact directory alias is not registered")
	}
	info, err := os.Lstat(location.Directory)
	if err != nil || !info.IsDir() || info.Mode().Perm()&0022 != 0 || !trustedOwner(info, key) {
		return artifactError(upgradetx.CodeRejected, "artifact directory is not controlled by the installation owner")
	}
	return nil
}

func trustedOwner(info os.FileInfo, key upgradetx.Key) bool {
	stat, ok := info.Sys().(*syscall.Stat_t)
	return ok && (stat.Uid == 0 || stat.Uid == key.UID)
}

func cleanAbsolute(path string) bool { return filepath.IsAbs(path) && filepath.Clean(path) == path }
