//go:build !linux

package infrahost

import "github.com/LinZiyang666/tether/internal/infracontract"

// Keep the cold installer/CLI buildable on other platforms without suggesting
// that a compiled adapter can perform the Linux pidfd/systemd hot-cut contract.
// origin: hot-update S9 plan lead LQ1 / T05
type ProcessPin struct{}

type CensusSequence struct{}

func (*CensusSequence) Observe(infracontract.Census) (infracontract.Census, error) {
	return infracontract.Census{}, infracontract.ErrUnsupported
}

func ReadProcess(int32, infracontract.Deadline) (infracontract.Process, error) {
	return infracontract.Process{}, infracontract.ErrUnsupported
}
func PinProcess(infracontract.Process, infracontract.Deadline) (*ProcessPin, error) {
	return nil, infracontract.ErrUnsupported
}
func (*ProcessPin) Close() error { return nil }
func (*ProcessPin) Signal(infracontract.HostOp, infracontract.Deadline, infracontract.Stop) error {
	return infracontract.ErrUnsupported
}
func (*ProcessPin) Exited(infracontract.Deadline) (bool, error) {
	return false, infracontract.ErrUnsupported
}
func VerifyRetired([]infracontract.Process, infracontract.Census, bool) error {
	return infracontract.ErrUnsupported
}
func NewIdentityBackend(CensusScope) (infracontract.Host, error) {
	return nil, infracontract.ErrUnsupported
}
