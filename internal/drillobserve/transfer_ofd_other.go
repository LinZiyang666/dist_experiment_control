//go:build !linux

package drillobserve

import (
	"os"
)

// An unsupported kernel supplies no OFD evidence. It cannot fabricate a
// witness from stat identity or pretend to own a sealed custody descriptor.
// origin: hot-update S7 plan §10.4 / T47
type TransferOFDWitness struct{}

func CaptureTransferOFD(*os.File, any) (*TransferOFDWitness, error) {
	return nil, ErrTransferCustody
}
func (*TransferOFDWitness) Receipt() TransferOFDReceipt    { return TransferOFDReceipt{} }
func (*TransferOFDWitness) Compare(int, uint64, int) error { return ErrTransferCustody }
func (*TransferOFDWitness) Send(int) error                 { return ErrTransferCustody }
func (*TransferOFDWitness) Close() error                   { return ErrTransferCustody }
func ReceiveTransferOFD(int) (*TransferOFDWitness, error)  { return nil, ErrTransferCustody }
func TransferProcessIdentity(uint64, string) (TransferProcess, error) {
	return TransferProcess{}, ErrTransferCustody
}

func poisonTransferCustody() {}

func PublishTransferOFD(*TransferOFDWitness) error { return ErrTransferCustody }

func transferNow() (int64, error) { return 0, ErrTransferCustody }

func transferRunningReader() (string, error) { return "", ErrTransferCustody }

func TransferExecDescriptors() ([]int, error) {
	if Hook == nil {
		return nil, nil
	}
	return nil, ErrTransferCustody
}
