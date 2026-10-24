//go:build linux

package infraupgrade

import (
	"os"
	"syscall"
)

// The journal reader must not follow a symlink or block on a substituted FIFO.
// origin: S9 internal review GATE-CONVERGE-sliminfra F3
func openTransactionJournal(path string) (*os.File, error) {
	return os.OpenFile(path, os.O_RDONLY|syscall.O_NOFOLLOW|syscall.O_NONBLOCK, 0)
}
