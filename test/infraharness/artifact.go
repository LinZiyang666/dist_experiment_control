package infraharness

import (
	"crypto/sha256"
	"debug/buildinfo"
	"errors"
	"io"
	"os"
	"path/filepath"
	"strings"

	"github.com/LinZiyang666/tether/internal/infracontract"
)

// VerifyArtifact consumes a pre-supplied binary, never downloading or building
// a historical release. A requested race binary must carry inner -race=true;
// the enclosing go test's flags alone cannot establish that fact.
// origin: hot-update S9 plan §9.1 / §10.2 / T58
func VerifyArtifact(root, binary string, expected infracontract.Digest, requireRace bool) error {
	if !filepath.IsAbs(root) || filepath.Clean(root) != root || root == "/" ||
		root == os.TempDir() || !filepath.IsAbs(binary) || expected == (infracontract.Digest{}) {
		return errors.New("infra harness: private artifact and digest required")
	}
	resolvedRoot, err := filepath.EvalSymlinks(root)
	if err != nil {
		return err
	}
	// A private-looking symlink must not turn the fixture boundary into the
	// entire host filesystem before the executable is opened.
	if resolvedRoot == "/" {
		return errors.New("infra harness: artifact root resolves to host filesystem")
	}
	resolved, err := filepath.EvalSymlinks(binary)
	if err != nil {
		return err
	}
	rel, err := filepath.Rel(resolvedRoot, resolved)
	if err != nil || rel == ".." || strings.HasPrefix(rel, ".."+string(filepath.Separator)) {
		return errors.New("infra harness: artifact outside private fixture")
	}
	f, err := os.Open(resolved)
	if err != nil {
		return err
	}
	defer func() { _ = f.Close() }()
	stat, err := f.Stat()
	if err != nil {
		return err
	}
	if !stat.Mode().IsRegular() || stat.Mode().Perm()&0111 == 0 {
		return errors.New("infra harness: artifact is not executable")
	}
	hash := sha256.New()
	if _, err := io.Copy(hash, f); err != nil {
		return err
	}
	if infracontract.Digest(hash.Sum(nil)) != expected {
		return errors.New("infra harness: artifact digest mismatch")
	}
	if requireRace {
		info, err := buildinfo.Read(f)
		if err != nil {
			return err
		}
		for _, setting := range info.Settings {
			if setting.Key == "-race" && setting.Value == "true" {
				return nil
			}
		}
		return errors.New("infra harness: artifact lacks race instrumentation")
	}
	return nil
}
