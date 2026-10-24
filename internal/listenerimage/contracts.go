// Package listenerimage separates a permanent listener keeper from each Accept view.
// origin: hot-update S6 plan §2.1 / §12.1 P00
package listenerimage

import (
	"net"
	"os"

	"github.com/LinZiyang666/tether/internal/controlbudget"
	"github.com/LinZiyang666/tether/internal/handoff"
)

type Descriptor struct {
	ID            string `json:"id"`
	Network       string `json:"network"`
	Address       string `json:"address"`
	Path          string `json:"path"`
	Device        uint64 `json:"device"`
	Inode         uint64 `json:"inode"`
	Cookie        uint64 `json:"cookie"`
	UnixPathInode uint64 `json:"unix_path_inode"`
	Family        int    `json:"family"`
	SockType      int    `json:"sock_type"`
	Nonblock      bool   `json:"nonblock"`
	CloseOnExec   bool   `json:"close_on_exec"`
}

// Closing an Accept view never closes the keeper's original listening OFD.
type Keeper interface {
	Descriptor() Descriptor
	View() net.Listener
	Pause(controlbudget.Deadline) error
	Export() (Descriptor, handoff.Resource, error)
	Resume() error
	Detach() (*os.File, error)
	Close() error
}
type AdoptFunc = func(Descriptor, *os.File) (Keeper, error)
type CaptureFunc = func(net.Listener, string) (Keeper, error)
