//go:build !linux

package installfs

import (
	"os"

	c "github.com/LinZiyang666/tether/internal/infracontract"
)

// origin: hot-update S9 plan §10.2 / T83 — never emulate privileged namespace custody on another OS.
func Open(string, uint32) (*Files, error)                               { return nil, c.ErrUnsupported }
func (*Files) Close() error                                             { return c.ErrUnsupported }
func (*Files) Directory(string, bool, uint32, uint32) (*os.File, error) { return nil, c.ErrUnsupported }
func (*Files) Open(string, uint32) (*os.File, error)                    { return nil, c.ErrUnsupported }
func (*Files) Read(string, int64) ([]byte, error)                       { return nil, c.ErrUnsupported }
func (*Files) Decode(string, any) error                                 { return c.ErrUnsupported }
func (*Files) Replace(string, []byte, uint32, uint32) error             { return c.ErrUnsupported }
func (*Files) Encode(string, any, uint32, uint32) error                 { return c.ErrUnsupported }
func (*Files) Lock(string) (func(), error)                              { return nil, c.ErrUnsupported }
func (*Files) ServiceDirectory(string, uint32) (*os.File, error)        { return nil, c.ErrUnsupported }

// Duplicate retains the caller-supplied open file description with CLOEXEC custody.
// origin: hot-update S9 plan §10.2 / T83 — no substitute descriptor may become stage custody.
func Duplicate(*os.File) (*os.File, error) { return nil, c.ErrUnsupported }
