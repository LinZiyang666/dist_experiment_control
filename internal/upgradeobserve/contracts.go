// Package upgradeobserve binds resource admission to completion evidence.
// The registry sequences admissions before effects; an empty snapshot is not
// proof unless every expected domain is known and complete under the same gate.
// origin: hot-update S8 plan §2.2 / §4.2 / P00
package upgradeobserve

import "github.com/LinZiyang666/tether/internal/upgradetx"

type Registry interface {
	Begin(resource upgradetx.Resource) (uint64, error)
	Finish(sequence uint64, resource upgradetx.Resource) error
	Snapshot(stamp upgradetx.Stamp) (upgradetx.Census, error)
	Seal(attempt upgradetx.Attempt, nonce string) (upgradetx.Receipt, error)
}
