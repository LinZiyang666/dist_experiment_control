//go:build !linux

package rawsocket

import (
	"os"
)

// Reliable image ownership depends on Linux nonblocking descriptor semantics.
// Darwin keeps its legacy path; cross-compilation is not an image capability.
// origin: hot-update S5 plan §7.4
func (s *Socket) ExportFile() (*os.File, error) {
	return nil, ErrUnsupported
}

func validateImageFile(f *os.File, want Fingerprint) error { return ErrUnsupported }
