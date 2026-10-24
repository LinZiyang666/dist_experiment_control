//go:build !linux

package sealedstate

import (
	"os"

	"github.com/LinZiyang666/tether/internal/rawsocket"
)

// Refuse before allocating or taking resources: Darwin's ordinary SS/flow
// path stays legacy and cannot produce an empty successful Linux image.
// origin: hot-update S5 plan §7.4
func New(id string, data []byte) (*Snapshot, error) {
	return nil, ErrUnsupported
}

// origin: hot-update S5 plan §7.4
func Read(src rawsocket.FileSource, id, name string, limit int64) ([]byte, *os.File, error) {
	return nil, nil, ErrUnsupported
}
