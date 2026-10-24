//go:build linux

package infraroot

import (
	"golang.org/x/sys/unix"
	"os"
)

// origin: hot-update S9 plan §10.2 / T83 — activation stays in the pinned wants directory.
func linkInstalledSocket(wants *os.File) error {
	return unix.Symlinkat("../tether-infra-helper.socket", int(wants.Fd()), "tether-infra-helper.socket")
}
