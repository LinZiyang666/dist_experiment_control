//go:build !linux

package infralaunch

import (
	c "github.com/LinZiyang666/tether/internal/infracontract"
	"os"
)

func (*Prepared) Files() ([]*os.File, error) { return nil, c.ErrUnsupported }
func AdoptPrepared(c.HostRequest, []*os.File, c.Deadline) (*Prepared, error) {
	return nil, c.ErrUnsupported
}
func (*Prepared) Successor() (c.Process, error) { return c.Process{}, c.ErrUnsupported }
func RecoverStoreLock(*os.File, *os.File, c.Process, c.Deadline) (*Lock, error) {
	return nil, c.ErrUnsupported
}

func StoreWithLock(*os.File, *os.File, c.Component) (c.Store, error) {
	return c.Store{}, c.ErrUnsupported
}
func AdoptACMEWriter(*os.File, *os.File, *os.File) (*Lock, error) { return nil, c.ErrUnsupported }

func RecoverACMEWriter(*os.File, *os.File, c.Process, c.Deadline) (*Lock, error) {
	return nil, c.ErrUnsupported
}
