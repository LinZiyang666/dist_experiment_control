//go:build linux

package drillobserve

import "github.com/LinZiyang666/tether/internal/observationexec"

// ExecDescriptors returns the authenticated startup custody originals for the
// handoff executor's exact allowlist. Both observation protocols own these
// descriptors; neither a descriptor number nor an environment override grants
// inheritance. Re-reading the sealed configuration detects close/reuse before
// the executor opens its inheritance window and restores flags on failure.
// origin: S8 internal review activation-and-route-migration F1
func ExecDescriptors() ([]int, error) {
	if active == nil || active.broken.Load() {
		return nil, nil
	}
	if err := observationexec.Validate(active.fd, active.channel, active.identity); err != nil {
		return nil, ErrTransferCustody
	}
	return []int{active.fd, active.channel}, nil
}

// Bind once during authenticated startup. The executor borrows the same sealed
// authority without importing the observer or installing replaceable callbacks.
// origin: S8 internal review activation-and-route-migration F1
func init() {
	if active != nil {
		if err := observationexec.Register(active.fd, active.channel, active.identity, &active.broken); err != nil {
			poisonCustody(active, 18)
		}
	}
}
