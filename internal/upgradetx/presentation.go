package upgradetx

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
)

// Presentation stays with transaction values so the CLI does not accumulate
// formatting/classification code or pull runtime dependencies into image leaves.
// origin: S10 internal review ARCH-REDS-2 F3

// ExitCode classifies an operation error. Querying a valid unknown View has no
// error and returns zero; Wait produces upgrade_unsettled for that same state.
// origin: hot-update S8 plan §7.1 / §11.2 P18
func ExitCode(err error) int {
	if err == nil {
		return 0
	}
	var failure *Error
	if !errors.As(err, &failure) {
		return 74
	}
	switch failure.Code {
	case "not_owner", CodeSessionIncarnationMismatch:
		return 77
	case CodeDeferred, "try_again":
		if failure.Accepted {
			return 74
		}
		return 75
	case CodeRejected, CodeUnsupportedComponent, CodeWindowClosed, CodeGrantUnknown, CodeGrantExpired, "bad_request", "legacy_busy":
		return 64
	case CodeUnsettled, CodeStateCorrupt, CodeNotWired:
		return 74
	default:
		// Unregistered failures must not become a retryable new transaction.
		return 74
	}
}

// Render writes the private actor/admin View unchanged in JSON. Text keeps
// historical execution separate from the live census and never promotes a
// pathname SHA, registration, or zero resource count to completion.
// origin: hot-update S8 plan §2 / §7.1 / P18
func Render(w io.Writer, view View, asJSON bool) error {
	if asJSON {
		return json.NewEncoder(w).Encode(view)
	}
	forward := view.Historical.Forward
	current := view.Observation.Current
	_, err := fmt.Fprintf(w, "Historical: tx_id=%s phase=%s outcome=%s revision=%d window=%s\n"+
		"Observation: nonce=%s at=%d tx_id=%s cut_id=%s generation=%d phase=%s outcome=%s owner_known=%t running_sha=%s effective_sha=%s\n"+
		"Census: known=%t complete=%t sequence=%d admitting=%d resources=%d old_owners=%d\n"+
		"Receipts: control_ready=%t resources_migrated=%t old_generation_released=%t\n",
		view.Historical.Grant.Handle.TxID, forward.Phase, forward.Outcome, view.Historical.Revision, view.Metadata.Window,
		view.Observation.Nonce, view.Observation.At, current.Handle.TxID, current.Handle.CutID, current.Handle.Generation,
		current.Phase, current.Outcome, view.Observation.OwnerKnown, view.Observation.RunningSHA, view.Observation.EffectiveSHA,
		view.Observation.Census.Known, view.Observation.Census.Complete, view.Observation.Census.Stamp.Sequence, view.Observation.Census.Admitting,
		len(view.Observation.Census.Resources), len(view.Observation.Census.OldOwners),
		current.Control.Known && current.Control.Complete, current.Resources.Known && current.Resources.Complete, current.Released.Known && current.Released.Complete)
	if err != nil {
		return err
	}
	if view.Historical.Grant.Mode == ModeLegacyInterrupting || current.Outcome == OutcomeLegacyInterrupting {
		if _, err := fmt.Fprintln(w, "Mode: legacy_interrupting non_hot"); err != nil {
			return err
		}
	}
	if view.Code != "" || view.Detail != "" || view.Historical.Reason != "" {
		_, err = fmt.Fprintf(w, "Reason: code=%s detail=%q historical=%q\n", view.Code, view.Detail, view.Historical.Reason)
	}
	return err
}
