package xfertrack

import (
	x "github.com/LinZiyang666/tether/internal/xfercontract"
	"testing"
)

// A no-effect result can carry a partial/full synced tmp prefix, but cannot
// claim directory durability or bytes beyond the original sealed source.
// origin: S7 internal review rv-terminal F2 / rv-recovery F03
func TestTransferClosedPrefixIsBoundedByOriginalSource(t *testing.T) {
	for _, fault := range []string{"negative", "oversized", "synced_parent", "partial", "full"} {
		t.Run(fault, func(t *testing.T) {
			f := newFixture(t, "b", "push")
			f.admit(t)
			grant := f.reserve(t)
			out := x.ReceiverOutcome{Grant: grant, Outcome: x.OutcomeNotEntered, Primitive: x.PrimitiveLink, Code: "dst_exists", Bytes: f.intent.Size}
			switch fault {
			case "negative":
				out.Bytes = -1
			case "oversized":
				out.Bytes++
			case "synced_parent":
				out.ParentSynced = true
			case "partial":
				out.Bytes--
			}
			receipt, err := f.owner.Finish(f.permit.Identity, out, f.clock.deadline())
			if fault == "partial" || fault == "full" {
				if err != nil || receipt.Kind != "failed" || receipt.Bytes != out.Bytes {
					t.Fatalf("valid synced prefix refused: %+v %v", receipt, err)
				}
				return
			}
			if err == nil || f.ledger.row(f.permit.Identity).CanonicalTerminal != nil {
				t.Fatalf("invalid prefix became a terminal: %+v %v", out, err)
			}
		})
	}
}
