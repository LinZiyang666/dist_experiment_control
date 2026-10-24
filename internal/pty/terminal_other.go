//go:build !linux

package pty

import (
	"errors"
	"os"

	"github.com/LinZiyang666/tether/internal/workload"
)

var errTerminalUnsupported = errors.New("pty: terminal image ownership requires Linux")

// Legacy sessions keep the portable owner; refusing image ownership must not
// close their descriptors or attempt Linux replay/deadline setup.
// origin: hot-update S4 plan §7.3 / lead L6
func takeTerminalMaster(*os.File, workload.Record) (Master, TerminalIdentity, error) {
	return nil, TerminalIdentity{}, errTerminalUnsupported
}

func RestoreMaster(*os.File, TerminalIdentity, workload.Record) (Master, error) {
	return nil, errTerminalUnsupported
}

func CheckMaster(*os.File, TerminalIdentity, workload.Record) error { return errTerminalUnsupported }

// Legacy allocation/start remain the identity authority on portable systems;
// no image is accepted and no Linux process or replay API is involved.
func legacyTerminalVerified(s *Session) bool {
	return s.ready && s.draining && s.owner != nil
}
