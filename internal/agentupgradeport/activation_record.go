package agentupgradeport

import (
	"context"
	"github.com/LinZiyang666/tether/internal/upgradebudget"
	"github.com/LinZiyang666/tether/internal/upgradecut"

	"github.com/LinZiyang666/tether/internal/upgradetx"
)

type activationRecordKey struct{}

// WithActivationRecord carries the accepted journal identity to the early
// wire reply. A reverse can receive a new TxID before exec destroys its final
// reply; the requester must retain that issued grant before it can wait.
// The notice remains accepted/unsettled, never a completion receipt.
// origin: hot-update S8 plan §3.1 / §7.1 / §8.1; S8 internal review upgrade-maketest F8
func WithActivationRecord(ctx context.Context, record upgradetx.Record) context.Context {
	return context.WithValue(ctx, activationRecordKey{}, record)
}

// CaptureWatchdog preserves the accepted cut's immutable recovery authority.
// Its grant already carries the original absolute handover deadline; adding a
// second window at import silently postpones recovery after exec. The reverse
// may execute for one handover window after that original deadline.
// origin: S8 internal review activation-and-route-migration F10; S8 plan §3.3
func CaptureWatchdog(record upgradetx.Record, cut upgradecut.State) (upgradetx.Fence, int64, upgradetx.ReverseRequest) {
	handle := current(record).Handle
	handle.Revision = record.Revision
	deadline := record.Grant.ExpiresAt
	fence := upgradetx.Fence{Current: handle, Owner: cut.Owner, RunningSHA: cut.TargetSHA,
		PreviousSHA: record.RecordedPreviousSHA, Incarnation: cut.Authorities[0].Incarnation}
	reverse := upgradetx.ReverseRequest{ClientNonce: newID(), ActorFP: record.Grant.ActorFP,
		HistoricalTxID: handle.TxID, TargetSHA: record.RecordedPreviousSHA, ExpiresAt: deadline + int64(upgradebudget.ComponentHandover)}
	return fence, deadline, reverse
}

// IsReverseCut recognizes only the verified imported attempt in its accepted
// journal. Its old forward deadline cannot authorize another automatic reverse
// while this return image is completing; a failed reverse retains recovery
// evidence and requires explicit current-owner recovery instead of a loop.
// origin: S8 internal review activation-and-route-migration F10; S8 plan §8.1
func IsReverseCut(journal upgradetx.Store, cut upgradecut.State) bool {
	if journal == nil {
		return false
	}
	view, err := journal.Load(cut.Attempt.Key, cut.Attempt.TxID)
	a := current(view.Historical)
	return err == nil && view.Historical.Key == cut.Attempt.Key && a.Direction == upgradetx.DirectionReverse && upgradetx.SameCut(a.Handle, cut.Attempt)
}
