//go:build !linux

// Kernel identity and OFD evidence are unsupported here. Returning zeros would
// let a portable build falsely certify a Linux acceptance contract.
// origin: hot-update S10 plan §11.2 / P04
package fullstackharness

import (
	"context"
	"errors"

	"github.com/LinZiyang666/tether/test/hotupdate/acceptance/contract"
)

var (
	ErrIdentityChanged   = errors.New("fullstack: process or executing image changed")
	ErrKernelUnsupported = errors.New("fullstack: kernel witness unavailable")
)

type SampleSource func(context.Context, contract.Key, string) (contract.Sample, error)
type StampReader func(context.Context) (contract.Stamp, error)
type ExecutableIdentity struct {
	Device uint64          `json:"device"`
	Inode  uint64          `json:"inode"`
	SHA    contract.Digest `json:"sha"`
}
type ExecutableRead struct {
	Before, After     contract.Stamp
	Original, Current ExecutableIdentity
}
type OFDProof struct {
	Owner                            contract.Stamp
	OriginalFD, CurrentFD, WitnessFD int
	Flags, SocketCookie              uint64
}
type OFDWitness struct{}

func ReadKernelProcess(contract.Key, int, string) (contract.Process, error) {
	return contract.Process{}, ErrKernelUnsupported
}
func ReadExecutable(context.Context, contract.Stamp, StampReader) (*ExecutableRead, error) {
	return nil, ErrKernelUnsupported
}
func (*ExecutableRead) VerifyTarget(contract.Digest) error          { return ErrKernelUnsupported }
func (*ExecutableRead) Close() error                                { return ErrKernelUnsupported }
func NewSnapshotter(contract.Profile) (contract.Snapshotter, error) { return nil, ErrKernelUnsupported }
func BindSnapshotSource(contract.Snapshotter, contract.Key, SampleSource) error {
	return ErrKernelUnsupported
}
func NewOFDWitness(contract.Stamp, int) (*OFDWitness, error) { return nil, ErrKernelUnsupported }
func ReadDescriptorFlags(int, int) (uint64, error)           { return 0, ErrKernelUnsupported }
func (*OFDWitness) Check(context.Context, contract.Stamp, int) (OFDProof, error) {
	return OFDProof{}, ErrKernelUnsupported
}
func (*OFDWitness) Close() error { return ErrKernelUnsupported }

var _ contract.ReaderFactory = NewSnapshotter
