package xferserve

import (
	x "github.com/LinZiyang666/tether/internal/xfercontract"
)

// SettleEagerly runs the finalizer for an entry the moment its exact history
// receipt is returned, instead of waiting for the next watchdog pass. Both
// owners keep one tier-B slot per session until settlement, so without this a
// user's next push/pull in the same session is refused for up to a pass after
// the previous one already completed. The finalizer remains the only owner of
// cleanup; a failure here is retried by the regular pass.
// origin: hot-update S7 plan §5.2 / §5.3 — S7 production activation
func SettleEagerly(c x.Controller, f x.FinalizerPort) x.Controller {
	if c == nil || f == nil {
		return c
	}
	return eager{Controller: c, finalizer: f}
}

type eager struct {
	x.Controller
	finalizer x.FinalizerPort
}

func (e eager) Handle(req x.Request, who x.Principal, d x.Deadline) (x.Response, error) {
	resp, err := e.Controller.Handle(req, who, d)
	if err == nil && resp.Receipt != nil && (req.Op == x.OpTerminal || req.Op == x.OpFinalize) {
		_, _ = e.finalizer.Advance(req.Identity, d)
	}
	return resp, err
}
