//go:build !linux

package drillobserve

// Unsupported kernels cannot supply authenticated inherited observation FDs.
// origin: S8 internal review activation-and-route-migration F1
func ExecDescriptors() ([]int, error) {
	if Hook == nil {
		return nil, nil
	}
	return nil, ErrTransferCustody
}
