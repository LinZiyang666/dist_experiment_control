package broker

// Real: NATS forwarding/replies and current SQLite session/member/node ACLs.
// Stubbed: the forwarded-command observer represents the agent boundary.
// Not covered: agent scope/MAC/epoch validation or real PTY side effects (P8).

import (
	"encoding/json"
	"fmt"
	"reflect"
	"runtime"
	"testing"
	"time"

	"github.com/LinZiyang666/tether/internal/auth"
	"github.com/LinZiyang666/tether/internal/proto"
	"github.com/LinZiyang666/tether/internal/session"
	"github.com/LinZiyang666/tether/internal/testharness"
	"github.com/nats-io/nats.go"
)

// origin: hot-update S4 plan §2.1 / T27. Every operation must recheck
// current authority, even when its handle and private reply were valid before
// revocation. Five independent route lifecycles also exercise NATS teardown.
func TestProcessResumePTYAuthorizationMatrix(t *testing.T) {
	before := runtime.NumGoroutine()
	beforeFD := testharness.FDCount()
	for round := 0; round < 5; round++ {
		t.Run(fmt.Sprintf("round-%d", round), func(t *testing.T) {
			exercisePTYAuthorizationMatrix(t)
		})
	}
	testharness.AssertNoGoroutineLeak(t, "PTY authorization routes", before)
	// All fixture connections and embedded servers have finished cleanup;
	// the runtime's lazily initialized poller may retain its two descriptors.
	testharness.AssertNoFDLeak(t, "PTY authorization routes", beforeFD, 2)
}

func exercisePTYAuthorizationMatrix(t *testing.T) {
	t.Helper()
	f := newProcessRouteFixture(t)
	member := freshUserActor(t)
	fp, err := auth.FingerprintFromActor(member)
	if err != nil {
		t.Fatal(err)
	}
	if err := session.AddMember(f.b.cfg.DB, "lab", fp, session.RoleMember, session.ViaPin, time.Now().UTC()); err != nil {
		t.Fatal(err)
	}
	// Membership of another session cannot grant access to this session or
	// to a same-named node there. Both scopes have a real ONLINE node.
	seedOwnedSession(t, f.b, member, "elsewhere", "node", "ONLINE")
	seedOwnedSession(t, f.b, freshUserActor(t), "foreign", "node", "ONLINE")
	subject := proto.SubjCmdBy("lab", member, "node", proto.ProcessResumeVerb)
	reply := auth.InboxPrefixFor(member) + ".pty-matrix"
	update := func(query string, args ...any) {
		t.Helper()
		if _, err := f.b.cfg.DB.Exec(query, args...); err != nil {
			t.Fatal(err)
		}
	}
	setStatus := func(status string) {
		update(`UPDATE nodes SET status=? WHERE sid='lab' AND nid='node'`, status)
	}

	for _, op := range []string{"probe", "pty-probe", "create", "resume", "ack", "terminal-ack", "input", "resize", "signal", "keepalive", "hangup"} {
		t.Run(op, func(t *testing.T) {
			body := ptyRouteBody(t, op)
			// Check the entire raw overlay, including future fields, the opaque
			// handle and the Run actor forgery. Only the top-level actor is the
			// broker's PTY responsibility; the agent owns the Run stamp.
			assertPTYForward(t, f, subject, reply, body, fp)
			setStatus("STALE")
			if op == "probe" || op == "pty-probe" {
				assertPTYRefusal(t, f.invoke(t, subject, reply, body), reply, "node_offline", "node_offline: status=STALE")
			} else {
				assertPTYForward(t, f, subject, reply, body, fp)
			}
			setStatus("OFFLINE")
			assertPTYRefusal(t, f.invoke(t, subject, reply, body), reply, "node_offline", "node_offline: status=OFFLINE")
			setStatus("ONLINE")

			for _, denied := range []struct{ sid, nid, code string }{
				{"missing", "node", "session_not_found_or_deleting"},
				{"foreign", "node", "not_a_member"},
				{"lab", "absent", "node_not_found"},
			} {
				badSubject := proto.SubjCmdBy(denied.sid, member, denied.nid, proto.ProcessResumeVerb)
				assertPTYRefusal(t, f.invoke(t, badSubject, reply, body), reply, denied.code, denied.code)
			}
			// Reuse the same subject and body after each authority change.
			// A cached positive admission must not survive either deletion.
			update(`UPDATE sessions SET state='DELETING' WHERE sid='lab'`)
			assertPTYRefusal(t, f.invoke(t, subject, reply, body), reply, "session_not_found_or_deleting", "session_not_found_or_deleting")
			update(`UPDATE sessions SET state='ACTIVE' WHERE sid='lab'`)
			update(`UPDATE nodes SET nid='moved' WHERE sid='lab' AND nid='node'`)
			assertPTYRefusal(t, f.invoke(t, subject, reply, body), reply, "node_not_found", "node_not_found")
			update(`UPDATE nodes SET nid='node' WHERE sid='lab' AND nid='moved'`)
			update(`DELETE FROM members WHERE sid='lab' AND pubkey_fp=?`, fp)
			assertPTYRefusal(t, f.invoke(t, subject, reply, body), reply, "not_a_member", "not_a_member")

			// Private-reply validation precedes even the membership error.
			// The observer's ordered marker proves there were no earlier
			// forwards or broker-authored publications on the victim inbox.
			for _, badReply := range []string{
				"", auth.InboxPrefixFor(member), auth.InboxPrefixFor(member) + "evil.x",
				auth.InboxPrefixFor(member) + ".*", auth.InboxPrefixFor(member) + ".>",
				auth.InboxPrefixFor(member) + "..x", auth.InboxPrefixFor(f.actor) + ".victim",
				"_INBOX.legacy", proto.SubjCmdForwarded("lab", "node", proto.ProcessResumeVerb),
			} {
				for _, data := range [][]byte{body, []byte("{")} {
					if messages := f.invoke(t, subject, badReply, data); len(messages) != 0 {
						t.Fatalf("unsafe reply caused publication: reply=%q messages=%v", badReply, messages)
					}
				}
			}
			if err := session.AddMember(f.b.cfg.DB, "lab", fp, session.RoleMember, session.ViaPin, time.Now().UTC()); err != nil {
				t.Fatal(err)
			}
			assertPTYForward(t, f, subject, reply, body, fp)
		})
	}
	for _, op := range []string{"", "frobnicate", "exec", "PTY-PROBE", "future-pty-control"} {
		t.Run("unknown-"+op, func(t *testing.T) {
			body := ptyRouteBody(t, op)
			assertPTYRefusal(t, f.invoke(t, subject, reply, body), reply, proto.CodeProcessUnsupportedOp, "unsupported process operation")
			setStatus("STALE")
			// Unknown operations retain strict node admission, which runs
			// before the operation whitelist and its unsupported reply.
			assertPTYRefusal(t, f.invoke(t, subject, reply, body), reply, "node_offline", "node_offline: status=STALE")
			setStatus("ONLINE")
		})
	}
}

func ptyRouteBody(t *testing.T, op string) []byte {
	t.Helper()
	var raw map[string]json.RawMessage
	if err := json.Unmarshal(processRouteBody(t, op), &raw); err != nil {
		t.Fatal(err)
	}
	delete(raw, "exec")
	raw["kind"] = json.RawMessage(`"pty"`)
	raw["Actor_FP"] = json.RawMessage(`"SHA256:forged-case-variant"`)
	raw["future_field"] = json.RawMessage(`{"nested":[1,"opaque",{"preserve":true}]}`)
	raw["run"] = json.RawMessage(`{"argv":["/bin/cat"],"env":{"KEY":"value"},"actor_fp":"SHA256:forged-run","ACTOR_FP":"SHA256:forged-run-case","future_run_field":["preserve"]}`)
	raw["input"] = json.RawMessage(`"AAMEDf8="`)
	raw["input_seq"] = json.RawMessage(`23`)
	raw["input_fin"] = json.RawMessage(`true`)
	raw["resize"] = json.RawMessage(`{"cols":123,"rows":47}`)
	raw["signal"] = json.RawMessage(`15`)
	raw["control_seq"] = json.RawMessage(`17`)
	// Deliberately combine payloads: the broker reads only the envelope.
	// Agent mutual-exclusion validation must receive these values intact.
	body, err := json.Marshal(raw)
	if err != nil {
		t.Fatal(err)
	}
	return body
}

func assertPTYForward(t *testing.T, f *processRouteFixture, subject, reply string, body []byte, fp string) {
	t.Helper()
	messages := f.invoke(t, subject, reply, body)
	forwarded := processForwarded(messages)
	if len(forwarded) != 1 || forwarded[0].Subject != proto.SubjCmdForwarded("lab", "node", proto.ProcessResumeVerb) || forwarded[0].Reply != reply {
		t.Fatalf("expected exactly one PTY forward on the original route/reply: %v", messages)
	}
	for _, msg := range messages {
		if msg.Subject == reply {
			t.Fatalf("broker both forwarded and refused the PTY request: %s", msg.Data)
		}
	}
	var got, want map[string]any
	if err := json.Unmarshal(body, &want); err != nil {
		t.Fatal(err)
	}
	if err := json.Unmarshal(forwarded[0].Data, &got); err != nil {
		t.Fatal(err)
	}
	delete(want, "Actor_FP")
	want["actor_fp"] = fp
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("PTY raw overlay changed opaque fields or trusted actor: got=%s want=%v", forwarded[0].Data, want)
	}
}

func assertPTYRefusal(t *testing.T, messages []*nats.Msg, reply, code, reason string) {
	t.Helper()
	// Exactly one private error also excludes forwarded-command side effects
	// and audit publications. A refusal alone without this fence is weaker.
	if len(messages) != 1 || messages[0].Subject != reply {
		t.Fatalf("refusal must publish only one private reply, with zero forwards: %v", messages)
	}
	var response proto.ProcessResumeReply
	if err := json.Unmarshal(messages[0].Data, &response); err != nil {
		t.Fatal(err)
	}
	if response.Version != proto.ProcessResumeVersion || response.Kind != "error" || response.Code != code || response.Error != reason || response.CallID != 179 {
		t.Fatalf("wrong correlated PTY refusal: got=%+v want code=%s reason=%s call_id=179", response, code, reason)
	}
}
