package execclient

import (
	"encoding/json"
	"errors"
	"io"
	"testing"
	"time"

	"github.com/LinZiyang666/tether/internal/proto"
)

// A retryable typed refusal (node_offline, agent_no_responders, ...) may be
// retried inside the admission deadline, but exhaustion must surface the last
// refusal so the CLI keeps its class and hint (node_offline -> 64 + "start the
// agent"), instead of a generic "context deadline exceeded" (75) that tells an
// operator to retry a node that is simply down.
// origin: hot-update S6 internal review rv-ctl
func TestAdmissionExhaustionKeepsLastTypedRefusal(t *testing.T) {
	f := newClientFixture(t)
	cfg := admissionConfig(t, 800*time.Millisecond)
	r := startAdmission(t, f, cfg, proto.ExecReq{Argv: []string{"/bin/true"}}, io.Discard)
	refusals := 0
	for {
		select {
		case <-r.done:
		default:
			msg, err := f.sub.NextMsg(50 * time.Millisecond)
			if err != nil {
				continue
			}
			ex := clientExchange{msg: msg}
			if decodeErr := json.Unmarshal(msg.Data, &ex.req); decodeErr != nil {
				t.Fatal(decodeErr)
			}
			refusals++
			sendClientReply(t, ex, proto.ProcessResumeReply{Version: 1, CallID: ex.req.CallID, Kind: "error",
				Code: proto.CodeNodeOffline, Error: "node_offline: status=OFFLINE"})
			continue
		}
		break
	}
	_, err := r.finish(t)
	if refusals < 2 {
		t.Fatalf("typed refusal was not retried inside admission: refusals=%d err=%v", refusals, err)
	}
	var pe *ProtocolError
	if !errors.As(err, &pe) || pe.Code != proto.CodeNodeOffline {
		t.Fatalf("exhaustion replaced the last node_offline refusal with %v after %d refusals", err, refusals)
	}
}
