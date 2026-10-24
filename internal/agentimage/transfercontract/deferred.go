package transfercontract

import "fmt"

// ReasonTransfersInFlight is shared with the composition so the adapter can
// report the same refusal without importing the image's runtime owners.
// origin: S7 internal review IMAGE-LEAF F1
const ReasonTransfersInFlight = "transfers_in_flight"

// DeferredError says the cut was refused and nothing was frozen: the
// upgrade should be deferred with Reason, and Detail says which object.
type DeferredError struct {
	Reason string
	Detail string
	Cause  error
}

func (e *DeferredError) Error() string {
	if e.Cause != nil {
		return fmt.Sprintf("agent image: deferred (%s): %s: %v", e.Reason, e.Detail, e.Cause)
	}
	return fmt.Sprintf("agent image: deferred (%s): %s", e.Reason, e.Detail)
}

func (e *DeferredError) Unwrap() error { return e.Cause }
