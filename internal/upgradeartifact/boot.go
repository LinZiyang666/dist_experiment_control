// boot.go is the canonical cold-boot selector: before daemon initialization a
// registered component finds its own registration and journal, and runs the
// image its durable effective selector names. It never infers the installation
// from the running slot or an environment variable, and it never treats the
// newest canonical bytes as current when a journal says otherwise.
// origin: hot-update S8 plan §5.2 / §11.2 P19 (canonical selector)
package upgradeartifact

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"io"
	"os"
	"path/filepath"
	"strings"
	"syscall"

	"github.com/LinZiyang666/tether/internal/upgradelock"
	"github.com/LinZiyang666/tether/internal/upgradetx"
)

const maxRegistrationBytes = 1 << 20

// RegisteredKey finds the deployment registration that names the configured
// component in its control directory and re-registers it in this process, so
// later lock/publication calls resolve the same domain. want carries every
// configured identity field except DomainID. An absent directory or no
// matching participant means "not registered": the component then runs its
// canonical bytes with no S8 transaction capability. Two matches, a malformed
// registration or a registration whose recomputed ID differs refuse boot,
// because each would let one component address two journals.
// origin: hot-update S8 plan §3.1 / §5.1 / §5.2
func RegisteredKey(controlDir string, want upgradetx.Key) (upgradetx.Key, upgradelock.Registration, bool, error) {
	var none upgradelock.Registration
	if !cleanAbsolute(controlDir) || want.DomainID != "" {
		return upgradetx.Key{}, none, false, artifactError(upgradetx.CodeRejected, "registration lookup needs a canonical control directory")
	}
	entries, err := os.ReadDir(controlDir)
	if os.IsNotExist(err) {
		return upgradetx.Key{}, none, false, nil
	}
	if err != nil || len(entries) > 4096 {
		return upgradetx.Key{}, none, false, artifactError(upgradetx.CodeStateCorrupt, "registration directory cannot be enumerated")
	}
	var found upgradetx.Key
	var registration upgradelock.Registration
	for _, entry := range entries {
		id, ok := strings.CutSuffix(entry.Name(), ".domain")
		if !ok {
			continue
		}
		reg, err := readRegistration(filepath.Join(controlDir, entry.Name()))
		if err != nil {
			return upgradetx.Key{}, none, false, err
		}
		for _, participant := range reg.Participants {
			key := participant.Key
			candidate := key
			candidate.DomainID = ""
			if candidate != want {
				continue
			}
			if found != (upgradetx.Key{}) || key.DomainID != id {
				return upgradetx.Key{}, none, false, artifactError(upgradetx.CodeStateCorrupt, "component is registered more than once")
			}
			found, registration = key, reg
		}
	}
	if found == (upgradetx.Key{}) {
		return upgradetx.Key{}, none, false, nil
	}
	id, err := upgradelock.RegisterDomain(registration)
	if err != nil {
		return upgradetx.Key{}, none, false, err
	}
	if id != found.DomainID {
		return upgradetx.Key{}, none, false, artifactError(upgradetx.CodeStateCorrupt, "registration identity no longer matches the installation")
	}
	return found, registration, true, nil
}

func readRegistration(path string) (upgradelock.Registration, error) {
	var reg upgradelock.Registration
	file, err := os.OpenFile(path, os.O_RDONLY|syscall.O_NOFOLLOW|syscall.O_NONBLOCK, 0)
	if err != nil {
		return reg, artifactError(upgradetx.CodeStateCorrupt, "registration is unavailable")
	}
	defer func() { _ = file.Close() }()
	info, err := file.Stat()
	if err != nil || !info.Mode().IsRegular() || info.Size() <= 0 || info.Size() > maxRegistrationBytes {
		return reg, artifactError(upgradetx.CodeStateCorrupt, "registration is not a bounded regular file")
	}
	data, err := io.ReadAll(io.LimitReader(file, maxRegistrationBytes+1))
	if err != nil || json.Unmarshal(data, &reg) != nil {
		return reg, artifactError(upgradetx.CodeStateCorrupt, "registration cannot be decoded")
	}
	return reg, nil
}

// SelectEffective decides the ordinary cold-boot image. view is nil exactly
// when the caller proved that no transaction was ever accepted for the key;
// then the registered canonical entry runs. Otherwise the durable selector
// decides, through Resolve's pins and publication checks; an unresolved,
// pending or corrupt selector refuses boot instead of falling back to the
// newest canonical bytes (a readonly canonical can be a newer dispatcher).
// exec is true when the running image is not the selected one.
// origin: hot-update S8 plan §5.2 / IL13 / DB09
func SelectEffective(canonical string, key upgradetx.Key, view *upgradetx.View, runningSHA string) (path, sha string, exec bool, err error) {
	if !validSHA(runningSHA) {
		return "", "", false, artifactError(upgradetx.CodeRejected, "running image identity is unknown")
	}
	path, err = ResolveCanonical(canonical, key, view)
	if err != nil {
		return "", "", false, err
	}
	if view == nil {
		// No accepted transaction: canonical IS the effective image. Its
		// digest is read once here and compared, so a canonical replaced by
		// an installer is exec'ed rather than silently mixed with old code.
		sha, err = fileSHA(path)
		if err != nil {
			return "", "", false, err
		}
	} else {
		sha = view.Historical.EffectiveSHA
		if filepath.Base(path) != sha {
			return "", "", false, artifactError(upgradetx.CodeStateCorrupt, "effective selector path is not its SHA slot")
		}
		if err := verifyImage(path, sha, key); err != nil {
			return "", "", false, err
		}
	}
	return path, sha, sha != runningSHA, nil
}

// FileSHA streams the digest of an executable without buffering it. It is the
// running-image witness for /proc/self/exe; a pathname is never a witness.
func FileSHA(path string) (string, error) { return fileSHA(path) }

func fileSHA(path string) (string, error) {
	file, err := os.Open(path)
	if err != nil {
		return "", artifactError(upgradetx.CodeRejected, "image is unavailable")
	}
	defer func() { _ = file.Close() }()
	hash := sha256.New()
	if n, err := io.Copy(hash, io.LimitReader(file, maxArtifactBytes+1)); err != nil || n == 0 || n > maxArtifactBytes {
		return "", artifactError(upgradetx.CodeRejected, "image size or read failed")
	}
	return hex.EncodeToString(hash.Sum(nil)), nil
}
