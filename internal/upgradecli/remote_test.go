package upgradecli

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/LinZiyang666/tether/internal/upgradeartifact"
	"github.com/LinZiyang666/tether/internal/upgradetx"
	"github.com/LinZiyang666/tether/internal/upgradewire"
)

// remoteKey is a structurally valid agent key the strict codec accepts.
func remoteKey() upgradetx.Key {
	return upgradetx.Key{DomainID: strings.Repeat("d", 32), Component: upgradetx.ComponentAgent, SID: "lab", ConfiguredNID: "status", UID: 1000}
}

func remoteHandle() upgradetx.Handle {
	return upgradetx.Handle{Key: remoteKey(), TxID: strings.Repeat("a", 32), CutID: strings.Repeat("b", 32), Generation: 2, Revision: 3}
}

func remoteGrant() upgradetx.Grant {
	owner := upgradetx.Owner{BootID: "boot", PID: 7, StartTicks: 9, InstanceID: strings.Repeat("e", 32)}
	return upgradetx.Grant{Handle: remoteHandle(), Issuer: owner, ActorFP: "SHA256:actor", ClientNonce: strings.Repeat("c", 32),
		Mode: upgradetx.ModeHot, SourceRelease: "v1", TargetRelease: "v2", SourceSHA: strings.Repeat("1", 64), TargetSHA: strings.Repeat("2", 64),
		ScopeDigest: strings.Repeat("3", 64), CapabilityDigest: strings.Repeat("4", 64), ExpiresAt: 1}
}

func remoteReceipt(g upgradetx.Grant) upgradeartifact.Receipt {
	return upgradeartifact.Receipt{Grant: g, CanonicalPath: "/opt/tether/tether", SourcePath: "/var/lib/tether-upgrade/artifacts/" + g.SourceSHA,
		TargetPath: "/var/lib/tether-upgrade/artifacts/" + g.TargetSHA, SourceSHA: g.SourceSHA, TargetSHA: g.TargetSHA, ManifestDigest: strings.Repeat("5", 64)}
}

// remoteCalls invokes every Endpoint operation with valid frozen envelopes.
func remoteCalls(r Remote) map[string]func() error {
	ctx := context.Background() // ctx-root: synchronous classification test.
	h, g := remoteHandle(), remoteGrant()
	fence := upgradetx.Fence{Current: h, Owner: g.Issuer, RunningSHA: g.TargetSHA, PreviousSHA: g.SourceSHA, Incarnation: strings.Repeat("6", 64)}
	reverse := upgradetx.ReverseRequest{ClientNonce: "nonce", ActorFP: g.ActorFP, HistoricalTxID: h.TxID, TargetSHA: g.SourceSHA, ExpiresAt: 1,
		Scope: upgradetx.Scope{ID: "scope", Participants: []upgradetx.Key{remoteKey()}, Domains: []upgradetx.Domain{upgradetx.DomainExec}}}
	prepare := upgradetx.PrepareRequest{ClientNonce: g.ClientNonce, ActorFP: g.ActorFP, Mode: upgradetx.ModeHot, SourceRelease: "v1", TargetRelease: "v2",
		SourceSHA: g.SourceSHA, TargetSHA: g.TargetSHA, CapabilityDigest: g.CapabilityDigest, ExpiresAt: 1, Scope: reverse.Scope}
	return map[string]func() error{
		upgradewire.OpPrepare:     func() error { _, err := r.Prepare(ctx, remoteKey(), prepare); return err },
		upgradewire.OpStart:       func() error { _, err := r.Start(ctx, g, remoteReceipt(g)); return err },
		upgradewire.OpQuery:       func() error { _, err := r.Query(ctx, remoteKey(), h.TxID, strings.Repeat("f", 32)); return err },
		upgradewire.OpRollback:    func() error { _, err := r.Rollback(ctx, fence, reverse); return err },
		upgradewire.OpCancel:      func() error { _, err := r.Cancel(ctx, h); return err },
		upgradewire.OpArchive:     func() error { _, err := r.Archive(ctx, h); return err },
		upgradewire.OpCloseWindow: func() error { _, err := r.CloseWindow(ctx, h); return err },
	}
}

// A mutation whose reply was lost may already be accepted: it must surface as
// accepted upgrade_unsettled (74), never as a retryable deferral that invites
// a second Start. Only provable non-delivery is a pre-acceptance deferral.
// origin: hot-update S8 plan §3.1 / §7.1 / §11.2 P19
func TestRemoteLostMutationReplyIsAcceptedUnsettled(t *testing.T) {
	sent := map[string]bool{}
	lost := Remote{Exchange: func(_ context.Context, r upgradewire.Request) (upgradewire.Response, error) {
		sent[r.Op] = true
		return upgradewire.Response{}, errors.New("reply inbox closed")
	}}
	for op, call := range remoteCalls(lost) {
		err := call()
		var failure *upgradetx.Error
		if !errors.As(err, &failure) || !sent[op] {
			t.Fatalf("%s: lost reply was not classified: %v", op, err)
		}
		mutation := op != upgradewire.OpPrepare && op != upgradewire.OpQuery
		switch {
		case mutation && (failure.Code != upgradetx.CodeUnsettled || !failure.Accepted || ExitCode(err) != 74):
			t.Fatalf("%s: lost mutation reply invited a retry: %+v rc=%d", op, failure, ExitCode(err))
		case op == upgradewire.OpPrepare && (failure.Code != upgradetx.CodeDeferred || failure.Accepted || ExitCode(err) != 75):
			t.Fatalf("prepare: lost inert reply must be a same-nonce retry: %+v", failure)
		case op == upgradewire.OpQuery && (failure.Code != upgradetx.CodeUnsettled || failure.Accepted):
			t.Fatalf("query: lost reply must not invent acceptance: %+v", failure)
		}
	}
	notSent := Remote{Exchange: func(context.Context, upgradewire.Request) (upgradewire.Response, error) {
		return upgradewire.Response{}, &NotSentError{Err: errors.New("no responders")}
	}}
	for op, call := range remoteCalls(notSent) {
		if err := call(); ExitCode(err) != 75 {
			t.Fatalf("%s: provably undelivered request must be retryable 75, got %v rc=%d", op, err, ExitCode(err))
		}
	}
}

// The target's own classification is relayed, never upgraded: a deferral it
// returned precedes acceptance; an unregistered code cannot pass as success
// or as a retryable class.
func TestRemoteRelaysTargetClassification(t *testing.T) {
	for _, tc := range []struct {
		code string
		rc   int
	}{
		{upgradetx.CodeDeferred, 75}, {upgradetx.CodeRejected, 64}, {upgradetx.CodeGrantExpired, 64},
		{upgradetx.CodeSessionIncarnationMismatch, 77}, {"not_owner", 77}, {"bad_request", 64}, {"try_again", 75},
		{"unregistered_future_code", 74}, {upgradetx.CodeUnsettled, 74},
	} {
		r := Remote{Exchange: func(context.Context, upgradewire.Request) (upgradewire.Response, error) {
			return upgradewire.Response{Code: tc.code, Error: "detail"}, nil
		}}
		for op, call := range remoteCalls(r) {
			err := call()
			if err == nil || ExitCode(err) != tc.rc {
				t.Fatalf("%s/%s: rc=%d err=%v, want %d", tc.code, op, ExitCode(err), err, tc.rc)
			}
			// An unregistered code is never relayed as-is: a later classifier
			// could map it to a retryable class. It becomes upgrade_unsettled,
			// accepted for every operation that may have had an effect.
			var failure *upgradetx.Error
			if tc.code == "unregistered_future_code" && (!errors.As(err, &failure) || failure.Code != upgradetx.CodeUnsettled ||
				failure.Accepted != (op != upgradewire.OpPrepare && op != upgradewire.OpQuery)) {
				t.Fatalf("%s: unregistered target code was relayed verbatim: %+v", op, err)
			}
		}
	}
}

// A malformed envelope is the caller's error and never reaches the transport;
// the strict codec runs before the exchange, not only at the receiver.
func TestRemoteRefusesMalformedEnvelopeBeforeSending(t *testing.T) {
	calls := 0
	r := Remote{Exchange: func(context.Context, upgradewire.Request) (upgradewire.Response, error) {
		calls++
		return upgradewire.Response{}, nil
	}}
	bad := remoteHandle()
	bad.TxID = "short"
	ctx := context.Background() // ctx-root: synchronous classification test.
	if _, err := r.Cancel(ctx, bad); ExitCode(err) != 64 || calls != 0 {
		t.Fatalf("malformed cancel reached the transport: calls=%d err=%v", calls, err)
	}
	if _, err := r.Query(ctx, remoteKey(), remoteHandle().TxID, "not-a-nonce"); ExitCode(err) != 64 || calls != 0 {
		t.Fatalf("malformed query reached the transport: calls=%d err=%v", calls, err)
	}
	if _, err := (Remote{}).Query(ctx, remoteKey(), remoteHandle().TxID, strings.Repeat("f", 32)); err == nil {
		t.Fatal("an unwired transport answered a query")
	}
}

// Prepare's reply must bind exactly the request: a grant for another key,
// actor, nonce or SHA pair is not this request's authorization (M16).
func TestRemotePrepareRequiresBoundGrant(t *testing.T) {
	for _, defect := range []string{"none", "key", "actor", "nonce", "target", "capability", "txid"} {
		t.Run(defect, func(t *testing.T) {
			r := Remote{Exchange: func(_ context.Context, req upgradewire.Request) (upgradewire.Response, error) {
				g := remoteGrant()
				switch defect {
				case "key":
					g.Handle.Key.ConfiguredNID = "other"
				case "actor":
					g.ActorFP = "SHA256:other"
				case "nonce":
					g.ClientNonce = strings.Repeat("9", 32)
				case "target":
					g.TargetSHA = strings.Repeat("8", 64)
				case "capability":
					g.CapabilityDigest = strings.Repeat("7", 64)
				case "txid":
					g.Handle.TxID = ""
				}
				return upgradewire.Response{View: upgradetx.View{Historical: upgradetx.Record{Key: req.Key, Grant: g}}}, nil
			}}
			err := remoteCalls(r)[upgradewire.OpPrepare]()
			if (defect == "none") != (err == nil) {
				t.Fatalf("prepare binding defect %s: %v", defect, err)
			}
			if defect != "none" && ExitCode(err) != 74 {
				t.Fatalf("unbound prepare reply rc=%d", ExitCode(err))
			}
		})
	}
}

// origin: S8 internal review cluster-roll F1
func TestRemoteRetainsAcceptedTransactionRefusals(t *testing.T) {
	endpoint := Remote{Exchange: func(context.Context, upgradewire.Request) (upgradewire.Response, error) {
		return upgradewire.Response{Code: upgradetx.CodeUnsettled, Error: "broker_activation_dispatched"}, nil
	}}
	_, err := endpoint.Start(t.Context(), remoteGrant(), remoteReceipt(remoteGrant()))
	var accepted *upgradetx.Error
	if !errors.As(err, &accepted) || !accepted.Accepted || ExitCode(err) != 74 {
		t.Fatalf("armed cut lost accepted status: %v", err)
	}
}
