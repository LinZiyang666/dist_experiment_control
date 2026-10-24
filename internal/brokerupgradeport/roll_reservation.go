package brokerupgradeport

import (
	"context"
	"github.com/LinZiyang666/tether/internal/upgradehost"
	"github.com/LinZiyang666/tether/internal/upgradestore"
	"github.com/LinZiyang666/tether/internal/upgradetx"
)

// Shared parent bindings belong to the registered domain, including agents.
// The original comments and implementation moved intact to upgradehost.
// origin: S8 internal review SIGNED-ROLL-FIXTURES F3; S8 plan §5.1
func RollKey(k upgradetx.Key) upgradetx.Key { return upgradehost.RollKey(k) }
func RollJournal(k upgradetx.Key, o upgradetx.Owner, h bool, r ...string) (*upgradestore.Store, error) {
	return upgradehost.RollJournal(k, o, h, r...)
}
func RollSlot(p upgradetx.Grant, k upgradetx.Key, r ...string) string {
	return upgradehost.RollSlot(p, k, r...)
}
func ParentDeadline(k upgradetx.Key, o upgradetx.Owner, p, s string) (int64, error) {
	return upgradehost.ParentDeadline(k, o, p, s)
}
func BindRollParent(k upgradetx.Key, o upgradetx.Owner, p upgradetx.Grant, s upgradetx.Scope, r ...string) error {
	return upgradehost.BindRollParent(k, o, p, s, r...)
}
func HoldRoll(ctx context.Context, k upgradetx.Key, p string) error {
	return upgradehost.HoldRoll(ctx, k, p)
}
func ReleaseRoll(k upgradetx.Key) error { return upgradehost.ReleaseRoll(k) }
