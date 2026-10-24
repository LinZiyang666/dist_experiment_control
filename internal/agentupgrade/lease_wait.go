package agentupgrade

import (
	"context"
	"log/slog"
	"sync/atomic"
	"time"
)

type LeaseWaitPort struct {
	Terminal *atomic.Bool
	Until    *atomic.Int64
	Refusals *atomic.Int32
	Logger   *slog.Logger
	SID, NID string
}

// AwaitLeaseRefusal waits out the delay owed for an unusable lease
// verdict, returning false if the run context ended first.
//
// The wait lives HERE, between sessions, rather than in the reconnect handler
// that observed the verdict: by the time this runs the contested session has
// already been torn down, so nothing is serving a name this process does not
// hold while it waits (external review F3). Zero when nothing is owed, which is
// every ordinary rebuild.
// A package-level function taking *Agent, not a method: the structural-budget
// ratchet pins this type's method count exactly.
func AwaitLeaseRefusal(ctx context.Context, p LeaseWaitPort) bool {
	if p.Terminal.Load() {
		p.Logger.Error("agent: lease refusal budget exhausted; staying disconnected until restart",
			"sid", p.SID, "configured_nid", p.NID,
			"refusals", p.Refusals.Load())
		<-ctx.Done()
		return false
	}
	until := p.Until.Swap(0)
	if until == 0 {
		return true
	}
	wait := time.Until(time.Unix(0, until))
	if wait <= 0 {
		return true
	}
	p.Logger.Info("agent: waiting before competing for a name again",
		"sid", p.SID, "configured_nid", p.NID, "wait", wait.String())
	select {
	case <-ctx.Done():
		return false
	case <-time.After(wait):
		return true
	}
}
