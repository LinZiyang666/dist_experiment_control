// Package rawsocket owns the original nonblocking business socket across image cuts.
// Carrier lifetimes must never become authority to close this descriptor.
// origin: hot-update S5 plan §2.1 / §3.2
package rawsocket

import (
	"errors"
	"net/netip"
	"os"
)

var (
	ErrNotImplemented      = errors.New("rawsocket: not implemented (S5 P1)")
	ErrImageNotImplemented = errors.New("rawsocket: not implemented (S5 P6)")
	ErrUnsupported         = errors.New("rawsocket: platform unsupported")
	ErrBusy                = errors.New("rawsocket: owner busy")
	ErrClosed              = errors.New("rawsocket: owner closed")
	ErrIdentity            = errors.New("rawsocket: descriptor identity mismatch")
)

type FileSource interface {
	TakeFile(id, name string) (*os.File, error)
}
type Fingerprint struct {
	Dev  uint64
	Ino  uint64
	Type uint32
}
type ConnectState uint8

const (
	ConnectConnecting ConnectState = 1
	ConnectConnected  ConnectState = 2
	ConnectFailed     ConnectState = 3
)

// Tuple is private state: it must never be copied to a public manifest.
type Tuple struct {
	Local  netip.AddrPort
	Remote netip.AddrPort
}
type Socket struct{ state *socketState }
