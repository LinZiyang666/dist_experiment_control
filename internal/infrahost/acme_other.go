//go:build !linux

package infrahost

import (
	"os"

	"github.com/LinZiyang666/tether/internal/infracontract"
)

// Unsupported storage migration fails before touching a selector, fence or key.
// Cold installation remains available; a portable build is not a hot-cut proof.
// origin: hot-update S9 plan lead LQ1 / §5.3
func acmePlatform() error { return infracontract.ErrUnsupported }

func acmeDuplicateDirectory(*os.File, int) (*os.File, error) {
	return nil, infracontract.ErrUnsupported
}
func acmeOpenDirectory(*os.File, string) (*os.File, error) {
	return nil, infracontract.ErrUnsupported
}
func acmeLock(*os.File, string, infracontract.Deadline, infracontract.Stop, bool) (*os.File, error) {
	return nil, infracontract.ErrUnsupported
}
func (*ACMEStorage) readState(bool) (acmeState, error) {
	return acmeState{}, infracontract.ErrUnsupported
}
func (*ACMEStorage) writeState(acmeState) error { return infracontract.ErrUnsupported }
func (*ACMEStorage) current(acmeState) (*os.File, error) {
	return nil, infracontract.ErrUnsupported
}
func acmeRandomSlot(string) (string, error) { return "", infracontract.ErrUnsupported }
func (*ACMEStorage) copyTree(*os.File, string, bool, infracontract.Deadline, infracontract.Stop) (infracontract.Digest, error) {
	return infracontract.Digest{}, infracontract.ErrUnsupported
}
func acmeRemoveTree(*os.File, string) error { return infracontract.ErrUnsupported }
