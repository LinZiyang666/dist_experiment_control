// Package terminalio adapts local terminal files to cancellable protocol I/O.
package terminalio

import (
	"fmt"
	"os"
)

// Source and Sink are structural interfaces shared with execclient without a
// dependency on its protocol loop. Cancel must wake an in-flight operation.
// origin: hot-update S4 plan §2.3
// The platform owner implements regular files and /dev/null as well as TTY
// and FIFO adapters; unsupported kinds fall back before create (lead L5).
type Source interface {
	Read([]byte) (int, error)
	Cancel() error
}

type Sink interface {
	Write([]byte) (int, error)
	Cancel() error
}

// ErrUnsupportedStdio identifies a stream that cannot safely be cancelled.
// The CLI can select its legacy path before creating a remote task; this is
// not a configuration error or permission to fall back after create.
// origin: hot-update S4 plan lead L5 / T43
type ErrUnsupportedStdio struct {
	Stream string
	Kind   string
}

func (e *ErrUnsupportedStdio) Error() string {
	return fmt.Sprintf("terminalio: %s is a %s; cancellable I/O is unsupported", e.Stream, e.Kind)
}

// IO owns local descriptor copies, two bounded workers and terminal restoration,
// never the caller's files. The caller must keep those files open during Open.
// Cancel is permanent for its direction; Close cancels and joins both workers.
type IO struct{ local platformIO }

// Open validates both streams before starting workers or changing termios.
func Open(in, out *os.File) (*IO, error) { return open(in, out) }

// EnterRaw changes a TTY input to raw mode, leaving non-TTY input alone.
// Repeated calls do not replace the original saved terminal state.
func (i *IO) EnterRaw() error { return i.local.enterRaw() }

// Restore restores the saved terminal state, and is safe to call repeatedly.
func (i *IO) Restore() error { return i.local.restore() }

// Close joins I/O before restoring termios and releasing owned descriptors.
// It is idempotent and never closes the original stdin or stdout.
func (i *IO) Close() error { return i.local.close() }

func (i *IO) Source() Source { return i.local.source() }

func (i *IO) Sink() Sink { return i.local.sink() }
