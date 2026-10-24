package infraharness

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"runtime"

	"github.com/LinZiyang666/tether/internal/controlbudget"
	"github.com/LinZiyang666/tether/internal/infraartifact"
	c "github.com/LinZiyang666/tether/internal/infracontract"
)

// StockSource shares P26's offline inputs across the tagged suites. An explicit
// suite override remains authoritative, including when it names broken supply;
// falling back in that case would hide a stale or incomplete candidate.
// origin: hot-update S9 plan §9.1 / §12 P26; S9 INFRA-SUITE-SUPPLIES
func StockSource(repository, override string) string {
	if override != "" {
		return override
	}
	if shared := os.Getenv("S9_ARTIFACT_DIR"); shared != "" {
		return shared
	}
	if repository == "" {
		_, file, _, ok := runtime.Caller(0)
		if !ok || !filepath.IsAbs(file) {
			return "" // Missing source identity must not select the host's cwd.
		}
		repository = filepath.Clean(filepath.Join(filepath.Dir(file), "../.."))
	}
	return filepath.Join(repository, "test/simcluster/vendor/infra")
}

// StockArchiveSource reads the genuine pre-supplied official releases. P01 and
// P26 use different checksum filenames; neither renaming nor extraction changes
// their bytes. No test downloads a release or substitutes a source-built image.
type StockArchiveSource string

func (root StockArchiveSource) Open(e infraartifact.Entry, d c.Deadline, stop c.Stop) (io.ReadCloser, io.ReadCloser, error) {
	if _, err := controlbudget.Remaining(d); err != nil {
		return nil, nil, err
	}
	select {
	case <-stop:
		return nil, nil, context.Canceled
	default:
	}
	if !privateSupplyPath(string(root)) {
		return nil, nil, errors.New("infra harness: canonical offline supply directory required")
	}
	component := "nats-server"
	if e.Kind == c.CaddyBinary {
		component = "caddy"
	}
	// NATS uses SHA256SUMS for both versions: select the version-bound P26/P01
	// name first so two adjacent releases never share the wrong checksum list.
	names := []string{component + "-" + e.Version + "-checksums.txt",
		fmt.Sprintf("%d-%s.checksums", e.Kind, e.Version), filepath.Base(e.ChecksumURL)}
	archive, err := openSupplyFile(string(root), e.ArchiveName)
	if err != nil {
		return nil, nil, err
	}
	for _, name := range names {
		sums, err := openSupplyFile(string(root), name)
		if errors.Is(err, os.ErrNotExist) {
			continue
		}
		// Return partial opens to the catalog, which owns their close on failure.
		return archive, sums, err
	}
	return archive, nil, os.ErrNotExist
}

func privateSupplyPath(path string) bool {
	return filepath.IsAbs(path) && filepath.Clean(path) == path && path != "/" && path != os.TempDir()
}

func openSupplyFile(root, name string) (*os.File, error) {
	if name == "." || filepath.Base(name) != name {
		return nil, errors.New("infra harness: invalid supply member")
	}
	path := filepath.Join(root, name)
	info, err := os.Lstat(path)
	if err != nil {
		return nil, err
	}
	if !info.Mode().IsRegular() {
		return nil, errors.New("infra harness: supply member must be regular")
	}
	return os.Open(path)
}

// StockArtifact is a private fixture input receipt, never a running-image or
// retirement receipt. The consumer still observes the actual process separately.
type StockArtifact struct {
	Artifact c.Artifact
	Binary   string
}

// PrepareStockArtifact verifies the official archive/list/member through the
// product catalog, then publishes fresh private executable bytes and a manifest.
// It requires pre-supplied inputs and performs no process or network operations.
// origin: hot-update S9 plan §3.1 / §9.1 / T55 / T60–T67
func PrepareStockArtifact(root, source string, kind c.ArtifactKind, version string, d c.Deadline, stop c.Stop) (StockArtifact, error) {
	var result StockArtifact
	if !privateSupplyPath(root) {
		return result, errors.New("infra harness: private output root required")
	}
	info, err := os.Lstat(root)
	if err != nil || !info.IsDir() {
		return result, errors.New("infra harness: output root must be a directory")
	}
	catalog, err := infraartifact.New(infraartifact.Config{Source: StockArchiveSource(source)})
	if err != nil {
		return result, err
	}
	artifact, err := catalog.Resolve(kind, version, "linux", "amd64")
	if err != nil {
		return result, err
	}
	verified, err := catalog.ReadVerified(artifact, d, stop)
	if err != nil {
		return result, fmt.Errorf("SETUP-RED pending S9 P26: pre-supplied official %d/%s archive and checksum list required: %w", kind, version, err)
	}
	// A fresh child directory prevents a reused test root from overwriting an
	// executable which might still be running or from following a caller link.
	directory, err := os.MkdirTemp(root, "stock-"+version+"-")
	if err != nil {
		return result, err
	}
	complete := false
	defer func() {
		if !complete {
			_ = os.RemoveAll(directory)
		}
	}()
	path := filepath.Join(directory, "binary")
	if err := os.WriteFile(path, verified.Bytes(), 0500); err != nil {
		return result, err
	}
	if err := VerifyArtifact(root, path, artifact.BinarySHA256, false); err != nil {
		return result, err
	}
	result = StockArtifact{Artifact: artifact, Binary: path}
	body, err := json.Marshal(result)
	if err != nil {
		return StockArtifact{}, err
	}
	if err := os.WriteFile(filepath.Join(directory, "manifest.json"), body, 0600); err != nil {
		return StockArtifact{}, err
	}
	complete = true
	return result, nil
}
