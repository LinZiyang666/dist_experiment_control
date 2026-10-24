//go:build !linux

package infrahost

import (
	"os"

	"github.com/LinZiyang666/tether/internal/infracontract"
)

// Hot root effects require the frozen Linux host profile. Cold installation
// and other-platform builds remain available without claiming that capability.
// origin: hot-update S9 plan lead LQ1 / T10
func openPrivateDirectory(string) (*os.File, error) { return nil, infracontract.ErrUnsupported }
func openPrivateFile(*os.File, string, int, os.FileMode) (*os.File, error) {
	return nil, infracontract.ErrUnsupported
}
func lockJournal(*os.File, infracontract.Deadline, infracontract.Stop) (func(), error) {
	return nil, infracontract.ErrUnsupported
}
func lockJournalAt(*os.File, infracontract.Deadline, infracontract.Stop, hostClock) (func(), error) {
	return nil, infracontract.ErrUnsupported
}
func replacePrivateFile(*os.File, string, []byte) error { return infracontract.ErrUnsupported }
func copyArtifact(*os.File, *os.File, infracontract.Artifact, infracontract.Deadline, infracontract.Stop) error {
	return infracontract.ErrUnsupported
}
func verifyArtifact(*os.File, infracontract.Artifact, infracontract.Deadline, infracontract.Stop) error {
	return infracontract.ErrUnsupported
}
