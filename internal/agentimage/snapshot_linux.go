//go:build linux

package agentimage

import (
	"errors"
	"os"

	"github.com/LinZiyang666/tether/internal/handoff"
	"github.com/LinZiyang666/tether/internal/sealedstate"
	"golang.org/x/sys/unix"
)

// Domain exporters replace and close their last snapshot during Export. The
// composed cut needs independent sealed-file ownership until its compatibility
// check and Prepare both succeed; a rejected candidate must retain the old cut.
// Endpoint route state is also sealed even though the tunnel exporter lists
// only its root file in Owned. Retain every state resource, counting each once
// in the composed byte bound, while raw/replay originals remain borrowed.
// origin: hot-update S5 plan §5.3 / T33
func retainDomainSnapshot(snapshot *sealedstate.Snapshot) (_ *sealedstate.Snapshot, retErr error) {
	retained := &sealedstate.Snapshot{}
	defer func() {
		// The domain snapshot is an export artifact: after copying its sealed
		// ownership, release that artifact rather than retain two root FDs.
		// Borrowed endpoint/business files still belong to the domain owner.
		retErr = errors.Join(retErr, snapshot.Close())
		if retErr != nil {
			retErr = errors.Join(retErr, retained.Close())
		}
	}()
	owned := make(map[*os.File]bool, len(snapshot.Owned))
	for _, file := range snapshot.Owned {
		owned[file] = true
	}
	duplicates := make(map[*os.File]*os.File)
	for _, resource := range snapshot.Resources {
		copyResource := handoff.Resource{ID: resource.ID, Kind: resource.Kind, Data: resource.Data, Files: make(map[string]*os.File, len(resource.Files))}
		for name, file := range resource.Files {
			if !owned[file] && (resource.Kind != "sealed-state-v1" || name != "state") {
				copyResource.Files[name] = file
				continue
			}
			if file == nil {
				return nil, sealedstate.ErrInvalid
			}
			duplicate := duplicates[file]
			if duplicate == nil {
				raw, err := file.SyscallConn()
				if err != nil {
					return nil, sealedstate.ErrInvalid
				}
				fd, dupErr := -1, error(nil)
				if err := raw.Control(func(original uintptr) {
					fd, dupErr = unix.FcntlInt(original, unix.F_DUPFD_CLOEXEC, 3)
				}); err != nil || dupErr != nil {
					return nil, sealedstate.ErrInvalid
				}
				duplicate = os.NewFile(uintptr(fd), "image-sealed-state")
				duplicates[file] = duplicate
				retained.Owned = append(retained.Owned, duplicate)
			}
			copyResource.Files[name] = duplicate
		}
		retained.Resources = append(retained.Resources, copyResource)
	}
	return retained, nil
}
