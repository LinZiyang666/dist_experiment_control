//go:build !linux

package rawsocket

import (
	"net/netip"
	"time"
)

func Connect(addr netip.AddrPort, deadline time.Time, stop <-chan struct{}, beforeConnect func(Tuple) error) (*Socket, error) {
	return nil, ErrUnsupported
}
func (s *Socket) CompleteConnect(deadline time.Time, stop <-chan struct{}) error {
	return ErrUnsupported
}
