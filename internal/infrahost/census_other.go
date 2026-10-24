//go:build !linux

package infrahost

import "github.com/LinZiyang666/tether/internal/infracontract"

// Same scope shape on every OS; none of its fields grants a fallback capability.
// origin: hot-update S9 plan lead LQ1 / T06
type CensusScope struct {
	Domain     infracontract.ID
	Generation uint64
	Cgroups    []string
	Before     []infracontract.Process
	Addresses  []string
}

func ReadCensus(CensusScope, infracontract.Deadline) (infracontract.Census, error) {
	return infracontract.Census{}, infracontract.ErrUnsupported
}
func ReadSocket(int, infracontract.Process, infracontract.Deadline) (infracontract.Socket, error) {
	return infracontract.Socket{}, infracontract.ErrUnsupported
}
func VerifyIngressProof(infracontract.Socket, infracontract.Socket, infracontract.Process, infracontract.Census) error {
	return infracontract.ErrUnsupported
}
func VerifyAttachmentChange(infracontract.Attachment, infracontract.Attachment, infracontract.Socket, infracontract.Census) error {
	return infracontract.ErrUnsupported
}
