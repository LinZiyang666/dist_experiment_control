package execclient

// Real: NATS protocol messages and the public Run entrypoint.
// Stubbed: the broker's pre-forward refusal of a create (CallID 0, the
// "oversize / malformed" boundary architecture §22.4 item 2 declares).
// This file does not claim broker or agent behaviour beyond that reply shape.

import (
	"context"
	"errors"
	"io"
	"testing"
	"time"

	"github.com/LinZiyang666/tether/internal/proto"
)

// origin: hot-update S1 internal review, ctl lane (verifier). The broker
// refuses an over-1 MiB create with process_request_too_large and CallID 0
// (exec.go handleProcessResumeReq, "malformed/oversize have no reliably
// parseable correlation"). receive() drops every reply whose CallID does not
// match, and call() treats the resulting attempt timeout as "resend". So a
// create the broker positively refused is resent every 500 ms for the whole
// --timeout (10 min by default) and then reported as CreateSent + ctx
// deadline, which exec.go turns into 74 "the remote task keeps running …
// NOT killed — use `tether kill`" for a task nothing ever admitted.
// error_hints.go maps process_request_too_large to 64 and says "the same
// request is the same size next time" — an entry the client can never reach.
//
// nats.go's own ErrMaxPayload masks this only while the server's max_payload
// is the 1 MiB default; usage §5 tells operators to raise it to ≥16 MiB for
// tier-A transfers, and natsconf carries it through as a passthrough.
func TestRunSurfacesCallIDZeroCreateRefusalInsteadOfResendingUntilTimeout(t *testing.T) {
	f := newClientFixture(t)
	run := f.start(t, 2*time.Second, io.Discard, io.Discard)
	create := f.negotiate(t)
	sendClientReply(t, create, proto.ProcessResumeReply{Version: proto.ProcessResumeVersion, CallID: 0, Kind: "error",
		Code: proto.CodeProcessTooLarge, Error: "process request too large"})
	// Count how many more copies of the refused create the client publishes.
	resent := 0
	deadline := time.Now().Add(1500 * time.Millisecond)
	for time.Now().Before(deadline) {
		msg, err := f.sub.NextMsg(time.Until(deadline))
		if err != nil {
			break
		}
		if msg.Subject == f.subject {
			resent++
		}
	}
	result, err := run.finish(t)
	var pe *ProtocolError
	if resent > 1 || !errors.As(err, &pe) || pe.Code != proto.CodeProcessTooLarge {
		t.Fatalf("a create the broker refused with CallID 0 was resent %d time(s) and Run returned %v (CreateSent=%v): the refusal is invisible to the caller and the command hangs until --timeout, then exits 74 for a task nothing admitted",
			resent, err, result.CreateSent)
	}
	if errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("Run reached the caller's deadline instead of surfacing the refusal: %v", err)
	}
}
