//go:build !linux

package upgradelock

import "github.com/LinZiyang666/tether/internal/upgradetx"

// A portable stat proves the inode, not the OFD holding its reservation. Until
// the platform supplies an equivalent custody proof, hot adoption must refuse.
// Local registration, acquisition and journal guards still work on Darwin.
// origin: hot-update S8 plan §5.1
func requireHeldOFD(uintptr) error {
	return refusal(upgradetx.CodeRejected, "inherited reservation proof is unsupported on this platform")
}
