//go:build !linux

package infraharness

import "github.com/LinZiyang666/tether/internal/infracontract"

// Ordinary non-Linux commands have no fatal-parent cleanup guarantee. Infra
// callers must receive unsupported before spawning rather than a bare fallback.
func supervisionSupported() error          { return infracontract.ErrUnsupported }
func ReadIdentity(int) (Identity, error)   { return Identity{}, infracontract.ErrUnsupported }
func ProcessExists(Identity) (bool, error) { return false, infracontract.ErrUnsupported }
func killIdentity(Identity) error          { return infracontract.ErrUnsupported }
