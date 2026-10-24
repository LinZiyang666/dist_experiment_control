package upgradecli

import (
	"context"
	"io"
	"time"

	"github.com/LinZiyang666/tether/internal/upgradeport"
	"github.com/LinZiyang666/tether/internal/upgradetx"
)

// Transaction verbs. Each addresses exactly the original TxID; none can choose
// a new execution ID or reissue Start. They are independent sibling commands
// (`node upgrade-status <nid>`), never children of `node upgrade <nid>`, so
// every legal NID — including "status", "wait" and "rollback" — stays
// addressable by `node upgrade`.
// origin: hot-update S8 plan §7.1 / DC06 / §11.2 P19
const (
	VerbStatus   = "status"
	VerbWait     = "wait"
	VerbRollback = "rollback"
	VerbCancel   = "cancel"
	VerbArchive  = "archive"
)

// VerbRequest is one --tx-id operation. Bind rejects a recalled grant that
// names another component than the command addressed (wrong SID/NID/role).
type VerbRequest struct {
	Verb, Home, TxID, ActorFP string
	Bind                      func(upgradetx.Key) error
	Endpoint                  upgradeport.Endpoint
	Timeout                   time.Duration
	JSON                      bool
	Out                       io.Writer
}

// RunVerb executes one verb. Query renders whatever the target observed,
// including unknown ownership, and succeeds (rc0); Wait turns that same state
// into upgrade_unsettled (74). Mutations always act on a FRESH query of the
// original transaction and render the target's reply; rollback then waits for
// the reverse goal, so its rc reflects the requested target, not the RPC.
// origin: hot-update S8 plan §7.1 / §8.1 / §11.2 P19
func RunVerb(ctx context.Context, r VerbRequest) error {
	grant, err := RecallGrant(r.Home, r.TxID)
	if err != nil {
		return err
	}
	key := grant.Handle.Key
	if r.ActorFP == "" {
		// The local admin socket is authorized by its Unix permissions; the
		// reverse still names the original actor rather than an invented one.
		r.ActorFP = grant.ActorFP
	}
	if r.Bind != nil {
		if err := r.Bind(key); err != nil {
			return err
		}
	}
	client := Client{Endpoint: r.Endpoint, Completed: upgradetx.Completed}
	switch r.Verb {
	case VerbStatus:
		view, err := client.Query(ctx, key, r.TxID)
		if err != nil {
			return err
		}
		return Render(r.Out, view, r.JSON)
	case VerbWait:
		view, err := client.WaitRecorded(ctx, key, r.TxID, r.Timeout)
		return renderOutcome(r.Out, view, r.JSON, err)
	case VerbRollback:
		return rollbackVerb(ctx, client, key, r)
	case VerbCancel, VerbArchive:
		view, err := client.Query(ctx, key, r.TxID)
		if err != nil {
			return err
		}
		handle, err := CurrentHandle(view)
		if err != nil {
			return err
		}
		if r.Verb == VerbCancel {
			view, err = r.Endpoint.Cancel(ctx, handle)
		} else {
			view, err = r.Endpoint.Archive(ctx, handle)
		}
		return renderOutcome(r.Out, view, r.JSON, err)
	default:
		return rejected("unknown transaction verb")
	}
}

func rollbackVerb(ctx context.Context, client Client, key upgradetx.Key, r VerbRequest) error {
	view, err := client.Query(ctx, key, r.TxID)
	if err != nil {
		return err
	}
	fence, reverse, err := ReverseFor(view, r.ActorFP, time.Now())
	if err != nil {
		return err
	}
	reply, err := r.Endpoint.Rollback(ctx, fence, reverse)
	if err != nil && !acceptedReverseReply(reply, err, fence, reverse) {
		return renderOutcome(r.Out, reply, r.JSON, err)
	}
	// The reverse may be a new transaction (manual reverse of a completed
	// execution) or continue the original one. Record its grant before
	// waiting so a lost wait can still be queried by the actor later.
	next := reply.Historical.Grant
	if next.Handle.Key != key || !hexValue(next.Handle.TxID, 32) {
		return &upgradetx.Error{Code: upgradetx.CodeUnsettled, Accepted: true, Detail: "rollback reply does not name a reverse transaction"}
	}
	if next.Handle.TxID != r.TxID {
		if err := RememberGrant(r.Home, next); err != nil {
			return err
		}
	}
	final, err := client.Wait(ctx, key, next.Handle.TxID, r.Timeout, next.Handle.TxID != r.TxID)
	return renderOutcome(r.Out, final, r.JSON, err)
}

// renderOutcome shows the last view even when the verb failed, so the
// operator sees the historical/observation split behind a nonzero rc.
func renderOutcome(out io.Writer, view upgradetx.View, asJSON bool, err error) error {
	if view.Historical.Grant.Handle.TxID != "" {
		if renderErr := Render(out, view, asJSON); renderErr != nil && err == nil {
			err = renderErr
		}
	}
	return err
}
