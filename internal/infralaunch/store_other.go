//go:build !linux

package infralaunch

import (
	"net"
	"os"

	"github.com/LinZiyang666/tether/internal/infracontract"
)

type Lock struct{}

func supported() error { return infracontract.ErrUnsupported }
func StoreIdentity(*os.File) (infracontract.Store, error) {
	return infracontract.Store{}, infracontract.ErrUnsupported
}
func AcquireStore(*os.File, *os.File) (*Lock, error) { return nil, infracontract.ErrUnsupported }
func AdoptStoreLock(*os.File, *os.File, *os.File) (*Lock, error) {
	return nil, infracontract.ErrUnsupported
}
func (*Lock) Identity() infracontract.Store { return infracontract.Store{} }
func (*Lock) Close() error                  { return nil }
func prepare(infracontract.HostRequest, Selection) (*Prepared, error) {
	return nil, infracontract.ErrUnsupported
}
func (*Prepared) verify() error                                { return infracontract.ErrUnsupported }
func (*Prepared) duplicate() (*Prepared, error)                { return nil, infracontract.ErrUnsupported }
func executableIdentity(*os.File, infracontract.Process) error { return infracontract.ErrUnsupported }
func CurrentProcess(string) (infracontract.Process, error) {
	return infracontract.Process{}, infracontract.ErrUnsupported
}
func execPrepared(*Prepared, infracontract.Deadline, infracontract.Stop) error {
	return infracontract.ErrUnsupported
}
func Send(*net.UnixConn, *Prepared, infracontract.Deadline, infracontract.Stop) error {
	return infracontract.ErrUnsupported
}
func Receive(*net.UnixConn, infracontract.Component, infracontract.Deadline, infracontract.Stop) (*Prepared, error) {
	return nil, infracontract.ErrUnsupported
}
