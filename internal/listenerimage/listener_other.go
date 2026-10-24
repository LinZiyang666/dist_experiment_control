//go:build !linux

package listenerimage

import (
	"net"
	"os"
	"time"

	"github.com/LinZiyang666/tether/internal/controlbudget"
)

// Unsupported cuts refuse before taking ownership, closing a native listener,
// removing a Unix pathname or changing any shared descriptor flags.
func Adopt(Descriptor, *os.File) (Keeper, error) { return nil, ErrUnsupported }

func captureListener(net.Listener, string) (*os.File, Descriptor, error) {
	return nil, Descriptor{}, ErrUnsupported
}

func validateListener(Descriptor, *os.File) error { return ErrUnsupported }
func protectUnixPath(net.Listener)                {}
func unlinkListenerPath(Descriptor) error         { return ErrUnsupported }
func listenerAddr(Descriptor) net.Addr            { return nil }
func listenerRemaining(controlbudget.Deadline) (time.Duration, error) {
	return 0, ErrUnsupported
}
