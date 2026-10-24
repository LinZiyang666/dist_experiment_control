//go:build !linux

package terminalio

import (
	"errors"
	"os"
)

// Darwin keeps the legacy terminal backend. This adapter cannot advertise a
// proc-fd/poller contract on platforms where it has not been implemented.
// origin: hot-update S4 plan §7.3 / lead L6
var errUnsupportedPlatform = errors.New("terminalio: reliable terminal I/O requires Linux")

type platformIO struct{}

func open(_, _ *os.File) (*IO, error) { return nil, errUnsupportedPlatform }
func (*platformIO) enterRaw() error   { return errUnsupportedPlatform }
func (*platformIO) source() Source    { return nil }
func (*platformIO) sink() Sink        { return nil }
func (*platformIO) restore() error    { return nil } // Open cannot acquire resources here.
func (*platformIO) close() error      { return nil } // A zero IO owns no descriptors.
