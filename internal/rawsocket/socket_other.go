//go:build !linux

package rawsocket

import (
	"net"
	"os"
	"syscall"
)

// Portable callers remain on legacy TCP; an unsupported cut never grants an owner.
func Adopt(f *os.File, want Fingerprint) (*Socket, error)    { return nil, ErrUnsupported }
func ValidateFile(f *os.File, want Fingerprint) error        { return ErrUnsupported }
func readSocket(raw syscall.RawConn, p []byte) (int, error)  { return 0, ErrUnsupported }
func writeSocket(raw syscall.RawConn, p []byte) (int, error) { return 0, ErrUnsupported }
func shutdownSocket(raw syscall.RawConn, both bool) error    { return ErrUnsupported }
func BootNow() (int64, error)                                { return 0, ErrUnsupported }

func TakeTCP(conn *net.TCPConn) (*Socket, error) { return nil, ErrUnsupported }
