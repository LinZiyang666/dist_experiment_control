package infraroot

import (
	"crypto/sha256"
	"errors"
	"os"

	"github.com/LinZiyang666/tether/internal/infraartifact"
	c "github.com/LinZiyang666/tether/internal/infracontract"
	"github.com/LinZiyang666/tether/internal/infrahost"
	"github.com/LinZiyang666/tether/internal/infralaunch"
)

// Initial selectors bind the actual supported old binaries. Bootstrap never
// executes an unverified candidate or invents an archive hash from binary bytes.
func (i *installation) initialState(d c.Deadline) (_ infrahost.HostState, resultErr error) {
	var state infrahost.HostState
	catalog, err := infraartifact.New(infraartifact.Config{})
	if err != nil {
		return state, err
	}
	for _, item := range []struct {
		kind     c.ArtifactKind
		path     string
		versions []string
	}{
		{c.NATSBinary, "/usr/local/bin/nats-server", []string{"2.14.5", "2.14.6"}},
		{c.CaddyBinary, "/usr/local/bin/caddy", []string{"2.7.5", "2.7.6"}},
	} {
		if err := installedBudget(d, nil); err != nil {
			return state, err
		}
		data, err := i.files.Read(item.path, 256<<20)
		if err != nil {
			return state, err
		}
		digest := sha256.Sum256(data)
		var artifact c.Artifact
		for _, version := range item.versions {
			candidate, err := catalog.Resolve(item.kind, version, "linux", "amd64")
			if err == nil && candidate.BinarySHA256 == digest {
				artifact = candidate
			}
		}
		if artifact == (c.Artifact{}) {
			return state, infrahost.ErrArtifact
		}
		path := stateDirectory + "/binary-" + digestName(digest)
		old, err := i.files.Read(path, 256<<20)
		if err == nil && sha256.Sum256(old) != digest {
			return state, infrahost.ErrArtifact
		}
		if err != nil && !errors.Is(err, os.ErrNotExist) {
			return state, err
		}
		if errors.Is(err, os.ErrNotExist) {
			if err := i.files.Replace(path, data, 0755, uint32(os.Getegid())); err != nil {
				return state, err
			}
		}
		state.Slots[item.kind].Active = artifact
	}
	directory, err := i.serviceDirectory()
	if err != nil {
		return state, err
	}
	defer func() { resultErr = errors.Join(resultErr, directory.Close()) }()
	locks, err := i.files.Directory(stateDirectory+"/locks", false, 0, 0)
	if err != nil {
		return state, err
	}
	defer func() { resultErr = errors.Join(resultErr, locks.Close()) }()
	lock, err := infralaunch.AcquireStore(directory, locks)
	if err != nil {
		return state, err
	}
	return state, lock.Close()
}
