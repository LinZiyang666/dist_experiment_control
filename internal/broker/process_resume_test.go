package broker

// Real: NATS requests/replies, SQLite admission and auth_callout-issued ACLs.
// Stubbed: the agent is an observing forwarded-command subscription; no process
// runtime, output replay or complete hot update is claimed by this file.
// origin: docs/reviews/hot-update-plan.md §3.1, reliable Exec authorization.

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"regexp"
	"strings"
	"testing"
	"time"

	"github.com/LinZiyang666/tether/internal/agentprov"
	"github.com/LinZiyang666/tether/internal/auth"
	"github.com/LinZiyang666/tether/internal/cli"
	"github.com/LinZiyang666/tether/internal/proto"
	"github.com/LinZiyang666/tether/internal/session"
	natsserver "github.com/nats-io/nats-server/v2/server"
	natstest "github.com/nats-io/nats-server/v2/test"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nkeys"
)

const processRouteBudget = 5 * time.Second

func processBadCRC(t *testing.T, actor string) string {
	t.Helper()
	last := byte('A')
	if actor[len(actor)-1] == last {
		last = 'B'
	}
	bad := actor[:len(actor)-1] + string(last)
	if err := proto.ValidateActorToken(bad); err != nil {
		t.Fatalf("CRC fixture failed earlier shape validation: %v", err)
	}
	if _, err := auth.FingerprintFromActor(bad); err == nil {
		t.Fatal("CRC fixture still decodes")
	}
	return bad
}

func TestPrivateProcessReplyBindsAuthenticatedActor(t *testing.T) {
	actor, other := freshUserActor(t), freshUserActor(t)
	prefix := auth.InboxPrefixFor(actor)
	for _, suffix := range []string{".abc123", ".NUID01.4", ".client_a.request-9"} {
		if !privateProcessReply(actor, prefix+suffix) {
			t.Fatalf("private literal reply rejected: %q", prefix+suffix)
		}
	}
	for _, reply := range []string{
		"", prefix, prefix + ".", prefix + "evil.x", prefix + "..x", prefix + ".x.",
		prefix + ".*", prefix + ".>", prefix + ".x*y", prefix + ".x>y",
		prefix + ".x y", prefix + ".x\t", prefix + ".x\n", prefix + ".x\r",
		prefix + ".x\x00", prefix + ".x\x7f", prefix + ".é",
		auth.InboxPrefixFor(other) + ".nonce", "_INBOX.legacy", proto.SubjCmdForwarded("lab", "node", "exec"),
	} {
		if privateProcessReply(actor, reply) {
			t.Errorf("untrusted reply admitted: %q", reply)
		}
	}
	badCRC := processBadCRC(t, actor)
	if privateProcessReply(badCRC, auth.InboxPrefixFor(badCRC)+".looks-private") {
		t.Fatal("actor shape was mistaken for authenticated public key")
	}
	fp, err := auth.FingerprintFromActor(actor)
	if err != nil {
		t.Fatal(err)
	}
	if privateProcessReply(fp, auth.InboxPrefixFor(fp)+".wrong-hash-input") {
		t.Fatal("ActorFP accepted in place of the public actor key")
	}
}

type processRouteFixture struct {
	b     *Broker
	nc    *nats.Conn
	actor string
}

func newProcessRouteFixture(t *testing.T) *processRouteFixture {
	t.Helper()
	nc, err := nats.Connect(startNATS(t))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(nc.Close)
	b := &Broker{cfg: Config{DB: openDB(t), Logger: silentLogger(), Now: time.Now}}
	b.nc.Store(nc)
	actor := freshUserActor(t)
	seedOwnedSession(t, b, actor, "lab", "node", "ONLINE")
	return &processRouteFixture{b, nc, actor}
}

// invoke uses a real bound Msg so even error replies take the production
// Respond path. Only the subject/reply under attack are replaced: malformed
// subjects need not match the production subscription to exercise its guard.
// The ordered marker follows the handler's return on the same connection;
// observing it proves all earlier publishes reached the tap, without a sleep.
func (f *processRouteFixture) invoke(t *testing.T, subject, reply string, data []byte) []*nats.Msg {
	t.Helper()
	tap, err := f.nc.SubscribeSync(">")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = tap.Unsubscribe() }()
	driver, marker := f.nc.NewInbox(), f.nc.NewInbox()
	handled := make(chan struct{})
	sub, err := f.nc.Subscribe(driver, func(msg *nats.Msg) {
		defer close(handled)
		copy := *msg
		copy.Subject, copy.Reply = subject, reply
		handleProcessResumeReq(f.b, f.nc, &copy)
	})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = sub.Unsubscribe() }()
	if err := f.nc.FlushTimeout(processRouteBudget); err != nil {
		t.Fatal(err)
	}
	if err := f.nc.Publish(driver, data); err != nil {
		t.Fatal(err)
	}
	select {
	case <-handled:
	case <-time.After(processRouteBudget):
		t.Fatal("process-resume handler did not return")
	}
	if err := f.nc.Publish(marker, nil); err != nil {
		t.Fatal(err)
	}
	var observed []*nats.Msg
	for {
		msg, err := tap.NextMsg(processRouteBudget)
		if err != nil {
			t.Fatal(err)
		}
		if msg.Subject == marker {
			return observed
		}
		if msg.Subject != driver {
			observed = append(observed, msg)
		}
	}
}

func processRouteBody(t *testing.T, op string) []byte {
	t.Helper()
	body, err := json.Marshal(proto.ProcessResumeReq{
		Version: proto.ProcessResumeVersion, CallID: 179, Op: op, ClientLineage: "client-lineage",
		ActorFP: "SHA256:forged-top-level", AttachEpoch: 9,
		Handle: &proto.ProcessHandle{RequestID: "stable-request", Lease: proto.ProcessLease{
			Version: 1, LeaseID: "agent-issued-lease", TargetInstance: strings.Repeat("a", 26),
			BootID: "original-boot", SID: "lab", NID: "node", ActorFP: "SHA256:lease-owner",
			ClientLineage: "client-lineage", IssuedBootNS: 11, ExpiresBootNS: 99, MAC: "opaque-agent-MAC",
		}},
		Exec: &proto.ExecReq{Argv: []string{"/bin/echo", "intent"}, Env: map[string]string{"KEY": "value"},
			Cwd: "/tmp", Stdin: []byte("input"), Safe: true},
		StdoutACK: 13, StderrACK: 7, StdoutFIN: true, TerminalID: "terminal-receipt",
	})
	if err != nil {
		t.Fatal(err)
	}
	return body
}

func processForwarded(messages []*nats.Msg) []*nats.Msg {
	var forwarded []*nats.Msg
	for _, msg := range messages {
		if strings.HasSuffix(msg.Subject, ".req.forwarded") {
			forwarded = append(forwarded, msg)
		}
	}
	return forwarded
}

func TestProcessResumeForwardStampsActorAndPreservesHandle(t *testing.T) {
	f := newProcessRouteFixture(t)
	fp, err := auth.FingerprintFromActor(f.actor)
	if err != nil {
		t.Fatal(err)
	}
	for _, op := range []string{"probe", "create", "resume", "ack", "terminal-ack"} {
		t.Run(op, func(t *testing.T) {
			body := processRouteBody(t, op)
			reply := auth.InboxPrefixFor(f.actor) + ".request.1"
			messages := f.invoke(t, proto.SubjCmdBy("lab", f.actor, "node", proto.ProcessResumeVerb), reply, body)
			forwarded := processForwarded(messages)
			if len(forwarded) != 1 {
				t.Fatalf("expected exactly one forwarded request; got %d, messages=%v", len(forwarded), messages)
			}
			msg := forwarded[0]
			if msg.Subject != proto.SubjCmdForwarded("lab", "node", proto.ProcessResumeVerb) || msg.Reply != reply {
				t.Fatalf("wrong route or reply: %+v", msg)
			}
			var got, want proto.ProcessResumeReq
			if err := json.Unmarshal(msg.Data, &got); err != nil {
				t.Fatal(err)
			}
			if err := json.Unmarshal(body, &want); err != nil {
				t.Fatal(err)
			}
			want.ActorFP = fp
			want.Exec.ActorFP = fp
			wantBody, _ := json.Marshal(want)
			gotBody, _ := json.Marshal(got)
			if !bytes.Equal(gotBody, wantBody) {
				t.Fatalf("trusted attribution or opaque lease/intent changed: got=%s want=%s", gotBody, wantBody)
			}
		})
	}
}

func TestProcessResumeRejectsUnsafeReplyBeforeEveryError(t *testing.T) {
	f := newProcessRouteFixture(t)
	prefix := auth.InboxPrefixFor(f.actor)
	canary := proto.SubjectPrefix + ".cluster.process-reply-canary"
	for _, reply := range []string{"", prefix, prefix + "evil.x", prefix + ".*", prefix + ".>", prefix + "..x", prefix + ".x\n", auth.InboxPrefixFor(freshUserActor(t)) + ".victim", "_INBOX.legacy", canary} {
		for _, body := range [][]byte{processRouteBody(t, "probe"), []byte("{")} {
			messages := f.invoke(t, proto.SubjCmdBy("lab", f.actor, "node", proto.ProcessResumeVerb), reply, body)
			if len(messages) != 0 {
				t.Fatalf("untrusted reply caused a publish before validation: reply=%q messages=%v", reply, messages)
			}
		}
	}
	// A real typed error on an authorized inbox proves the negative table did
	// not merely exercise a broken responder that cannot publish any reply.
	reply := prefix + ".valid-error"
	messages := f.invoke(t, proto.SubjCmdBy("lab", f.actor, "node", proto.ProcessResumeVerb), reply, []byte("{"))
	if len(messages) != 1 || messages[0].Subject != reply {
		t.Fatalf("safe malformed request did not return one error: %v", messages)
	}
	var response proto.ProcessResumeReply
	if err := json.Unmarshal(messages[0].Data, &response); err != nil || response.Kind != "error" || response.Error == "" {
		t.Fatalf("invalid error reply: %+v decode=%v", response, err)
	}
	// An ACL refusal must not respond before applying the very same reply
	// guard. The valid-member cases above would miss that ordering regression.
	fp, err := auth.FingerprintFromActor(f.actor)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := f.b.cfg.DB.Exec(`DELETE FROM members WHERE sid=? AND pubkey_fp=?`, "lab", fp); err != nil {
		t.Fatal(err)
	}
	if messages := f.invoke(t, proto.SubjCmdBy("lab", f.actor, "node", proto.ProcessResumeVerb), canary, processRouteBody(t, "probe")); len(messages) != 0 {
		t.Fatalf("membership refusal injected a broker response into canary: %v", messages)
	}
}

func TestProcessResumeMalformedSubjectCannotPublish(t *testing.T) {
	f := newProcessRouteFixture(t)
	base := proto.SubjCmdBy("lab", f.actor, "node", proto.ProcessResumeVerb)
	badCRC := processBadCRC(t, f.actor)
	for _, subject := range []string{
		base + ".extra", strings.TrimSuffix(base, ".req"),
		proto.SubjCmdBy("BAD", f.actor, "node", proto.ProcessResumeVerb),
		proto.SubjCmdBy("lab", f.actor, "BAD", proto.ProcessResumeVerb),
		proto.SubjCmdBy("lab", f.actor, "node", ""),
		proto.SubjCmdBy("lab", f.actor, "node", "exec"),
		proto.SubjCmdBy("lab", "not-an-actor", "node", proto.ProcessResumeVerb),
		proto.SubjCmdBy("lab", badCRC, "node", proto.ProcessResumeVerb),
	} {
		replyActor := f.actor
		if strings.Contains(subject, badCRC) {
			replyActor = badCRC // Its apparent prefix matches; only CRC can refuse.
		}
		messages := f.invoke(t, subject, auth.InboxPrefixFor(replyActor)+".malformed", processRouteBody(t, "probe"))
		if len(messages) != 0 {
			t.Fatalf("malformed subject published data: subject=%q messages=%v", subject, messages)
		}
	}
}

func TestProcessResumeUnsupportedProtocolNeverUsesLegacyExec(t *testing.T) {
	f := newProcessRouteFixture(t)
	subject := proto.SubjCmdBy("lab", f.actor, "node", proto.ProcessResumeVerb)
	reply := auth.InboxPrefixFor(f.actor) + ".unsupported"
	legacy, err := json.Marshal(proto.ExecReq{Argv: []string{"/bin/echo", "must-not-run"}})
	if err != nil {
		t.Fatal(err)
	}
	for _, body := range [][]byte{
		legacy,
		[]byte(`{"version":2,"op":"create"}`),
		[]byte(`{"version":1,"op":"exec"}`),
		[]byte(`{"version":1,"op":""}`),
	} {
		messages := f.invoke(t, subject, reply, body)
		if len(messages) != 1 || messages[0].Subject != reply {
			t.Fatalf("unsupported protocol was forwarded, dropped or retried as legacy: %v", messages)
		}
		var response proto.ProcessResumeReply
		if err := json.Unmarshal(messages[0].Data, &response); err != nil || response.Kind != "error" || response.Error == "" {
			t.Fatalf("unsupported protocol did not return a typed refusal: %+v %v", response, err)
		}
	}
	if got := processForwarded(f.invoke(t, subject, reply, processRouteBody(t, "probe"))); len(got) != 1 {
		t.Fatal("positive reliable protocol was also refused")
	}
}

func TestProcessResumeErrorPreservesCallID(t *testing.T) {
	// S1: every refusal also carries the stable wire code the client
	// classifies by; the prose stays as the pre-Code peers' fallback.
	wantCode := map[string]string{"membership": "not_a_member", "version": proto.CodeProcessUnsupported, "operation": proto.CodeProcessUnsupportedOp}
	for _, cause := range []string{"membership", "version", "operation"} {
		t.Run(cause, func(t *testing.T) {
			f := newProcessRouteFixture(t)
			var req proto.ProcessResumeReq
			if err := json.Unmarshal(processRouteBody(t, "probe"), &req); err != nil {
				t.Fatal(err)
			}
			switch cause {
			case "membership":
				fp, err := auth.FingerprintFromActor(f.actor)
				if err != nil {
					t.Fatal(err)
				}
				if _, err := f.b.cfg.DB.Exec(`DELETE FROM members WHERE sid=? AND pubkey_fp=?`, "lab", fp); err != nil {
					t.Fatal(err)
				}
			case "version":
				req.Version++
			case "operation":
				req.Op = "unknown-operation"
			}
			body, err := json.Marshal(req)
			if err != nil {
				t.Fatal(err)
			}
			reply := auth.InboxPrefixFor(f.actor) + ".correlation"
			messages := f.invoke(t, proto.SubjCmdBy("lab", f.actor, "node", proto.ProcessResumeVerb), reply, body)
			if len(messages) != 1 || messages[0].Subject != reply {
				t.Fatalf("expected one safe error response: %v", messages)
			}
			var response proto.ProcessResumeReply
			if err := json.Unmarshal(messages[0].Data, &response); err != nil || response.Kind != "error" || response.Error == "" || response.CallID != req.CallID || req.CallID == 0 {
				t.Fatalf("error response lost its request correlation: response=%+v requestCall=%d decode=%v", response, req.CallID, err)
			}
			if response.Code != wantCode[cause] {
				t.Fatalf("refusal code = %q, want %q (%+v)", response.Code, wantCode[cause], response)
			}
		})
	}
}

// origin: docs/reviews/hot-update-reliable-exec-plan.md §2.2 — the broker is
// a pass-through for fields it does not know: a client newer than this
// broker must reach a newer agent intact, and only the broker's actor stamp
// may survive, whatever case variant the client sent.
func TestProcessResumeForwardPreservesUnknownFieldsAndStripsForgedActor(t *testing.T) {
	f := newProcessRouteFixture(t)
	var req map[string]any
	if err := json.Unmarshal(processRouteBody(t, "create"), &req); err != nil {
		t.Fatal(err)
	}
	req["future_field"] = map[string]any{"nested": []any{1.0, "two"}}
	req["Actor_FP"] = "SHA256:forged"
	if exec, ok := req["exec"].(map[string]any); ok {
		exec["ACTOR_fp"] = "SHA256:forged-exec"
		exec["future_exec_field"] = true
	}
	body, err := json.Marshal(req)
	if err != nil {
		t.Fatal(err)
	}
	reply := auth.InboxPrefixFor(f.actor) + ".passthrough"
	forwarded := processForwarded(f.invoke(t, proto.SubjCmdBy("lab", f.actor, "node", proto.ProcessResumeVerb), reply, body))
	if len(forwarded) != 1 {
		t.Fatalf("expected one forwarded request: %d", len(forwarded))
	}
	var got map[string]any
	if err := json.Unmarshal(forwarded[0].Data, &got); err != nil {
		t.Fatal(err)
	}
	fp, err := auth.FingerprintFromActor(f.actor)
	if err != nil {
		t.Fatal(err)
	}
	if got["actor_fp"] != fp || got["Actor_FP"] != nil || got["future_field"].(map[string]any)["nested"].([]any)[1] != "two" {
		t.Fatalf("top level after forward: %s", forwarded[0].Data)
	}
	exec := got["exec"].(map[string]any)
	if kept, _ := exec["future_exec_field"].(bool); exec["actor_fp"] != fp || exec["ACTOR_fp"] != nil || !kept {
		t.Fatalf("exec after forward: %s", forwarded[0].Data)
	}
	if body := string(forwarded[0].Data); strings.Contains(body, "Actor_FP") || strings.Contains(body, "ACTOR_fp") {
		t.Fatalf("forged actor variants survived: %s", body)
	}
}

// origin: docs/reviews/hot-update-reliable-exec-plan.md §2.3 — the refusal
// order is the live contract: an oversize body is refused with CallID 0 (it
// cannot be read), a malformed body likewise, and a member refusal comes
// BEFORE the version check so an unauthorized caller learns nothing about
// which protocol versions this broker speaks.
func TestProcessResumeRefusalOrderAndUnreadableCallID(t *testing.T) {
	f := newProcessRouteFixture(t)
	reply := auth.InboxPrefixFor(f.actor) + ".order"
	subject := proto.SubjCmdBy("lab", f.actor, "node", proto.ProcessResumeVerb)
	decode := func(t *testing.T, msgs []*nats.Msg) proto.ProcessResumeReply {
		t.Helper()
		if len(msgs) != 1 || msgs[0].Subject != reply {
			t.Fatalf("expected one refusal on the private reply: %v", msgs)
		}
		var r proto.ProcessResumeReply
		if err := json.Unmarshal(msgs[0].Data, &r); err != nil {
			t.Fatal(err)
		}
		return r
	}
	// (An oversize body never reaches the handler over real NATS: the server
	// enforces max_payload first; procforward's own tests cover the limit.)
	if r := decode(t, f.invoke(t, subject, reply, []byte(`[1,2]`))); r.Code != proto.CodeProcessBadRequest || r.CallID != 0 {
		t.Fatalf("malformed: %+v", r)
	}
	if r := decode(t, f.invoke(t, subject, reply, []byte(`{"call_id":"x","op":"probe"}`))); r.Code != proto.CodeProcessBadRequest || r.CallID != 0 {
		t.Fatalf("bad call_id: %+v", r)
	}
	// Revoked member + wrong version: the ACL refusal wins.
	fp, err := auth.FingerprintFromActor(f.actor)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := f.b.cfg.DB.Exec(`DELETE FROM members WHERE sid=? AND pubkey_fp=?`, "lab", fp); err != nil {
		t.Fatal(err)
	}
	if r := decode(t, f.invoke(t, subject, reply, []byte(`{"version":99,"call_id":5,"op":"probe"}`))); r.Code != "not_a_member" || r.CallID != 5 {
		t.Fatalf("ACL must precede the version check: %+v", r)
	}
}

func TestProcessResumeRechecksCurrentSessionMembership(t *testing.T) {
	f := newProcessRouteFixture(t)
	member := freshUserActor(t)
	fp, err := auth.FingerprintFromActor(member)
	if err != nil {
		t.Fatal(err)
	}
	if err := session.AddMember(f.b.cfg.DB, "lab", fp, session.RoleMember, session.ViaPin, time.Now().UTC()); err != nil {
		t.Fatal(err)
	}
	subject, reply := proto.SubjCmdBy("lab", member, "node", proto.ProcessResumeVerb), auth.InboxPrefixFor(member)+".current"
	if got := processForwarded(f.invoke(t, subject, reply, processRouteBody(t, "probe"))); len(got) != 1 {
		t.Fatal("member positive never reached forward")
	}
	if _, err := f.b.cfg.DB.Exec(`DELETE FROM members WHERE sid=? AND pubkey_fp=?`, "lab", fp); err != nil {
		t.Fatal(err)
	}
	// Remaining a member of a different SID must not grant this one.
	if _, err := session.Create(f.b.cfg.DB, "other", "other", fp, "pin-hash", time.Now().UTC()); err != nil {
		t.Fatal(err)
	}
	for _, op := range []string{"probe", "create", "resume", "ack", "terminal-ack"} {
		messages := f.invoke(t, subject, reply, processRouteBody(t, op))
		if len(processForwarded(messages)) != 0 {
			t.Fatalf("removed member was still forwarded: op=%s", op)
		}
		found := false
		for _, message := range messages {
			if message.Subject == reply {
				var response proto.ProcessResumeReply
				if err := json.Unmarshal(message.Data, &response); err != nil || response.Kind != "error" || !strings.Contains(response.Error, "not_a_member") {
					t.Fatalf("wrong member refusal: %+v %v", response, err)
				}
				found = true
			}
		}
		if !found {
			t.Fatal("member denial never replied to the validated inbox")
		}
	}
	if _, err := f.b.cfg.DB.Exec(`UPDATE sessions SET state='DELETING' WHERE sid='lab'`); err != nil {
		t.Fatal(err)
	}
	ownerSubject := proto.SubjCmdBy("lab", f.actor, "node", proto.ProcessResumeVerb)
	if got := processForwarded(f.invoke(t, ownerSubject, auth.InboxPrefixFor(f.actor)+".deleting", processRouteBody(t, "resume"))); len(got) != 0 {
		t.Fatal("owner resumed a deleting session")
	}
}

func TestProcessResumeClusterRoutingUsesLeader(t *testing.T) {
	pattern := proto.SubjectPrefix + ".s.*.cmd.by.*.node.*." + proto.ProcessResumeVerb + ".req"
	if !isBroadcastClusterSubject(pattern) {
		t.Fatal("reliable control would be queue-delivered to a silent follower")
	}
	f := newProcessRouteFixture(t)
	f.b.clusterMode = true
	f.b.cl = &clusterRuntime{node: newSingleVoterNode(t, "process-leader")}
	subject := proto.SubjCmdBy("lab", f.actor, "node", proto.ProcessResumeVerb)
	reply := auth.InboxPrefixFor(f.actor) + ".leader"
	if got := processForwarded(f.invoke(t, subject, reply, processRouteBody(t, "probe"))); len(got) != 1 {
		t.Fatal("real elected leader did not forward")
	}
	f.b.cl = &clusterRuntime{node: nonLeaderNode(t, "process-follower")}
	// Make an accidental ACL access return a wire error. A correct follower
	// must never consult this closed DB or emit even an error reply.
	if err := f.b.cfg.DB.Close(); err != nil {
		t.Fatal(err)
	}
	if messages := f.invoke(t, subject, reply, processRouteBody(t, "probe")); len(messages) != 0 {
		t.Fatalf("follower processed or replied: %v", messages)
	}
}

func TestProcessResumeAuthCalloutPinsActorAndRechecksMembership(t *testing.T) {
	t.Setenv(cli.DevNoAuthEnv, "")
	owner, err := cli.EnsureIdentity(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	member, err := cli.EnsureIdentity(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	agentID, err := cli.EnsureAgentIdentity(t.TempDir(), "lab")
	if err != nil {
		t.Fatal(err)
	}
	brokerSeed, err := auth.GenerateUserSeed()
	if err != nil {
		t.Fatal(err)
	}
	brokerPub, err := auth.PublicKeyFromSeed(brokerSeed)
	if err != nil {
		t.Fatal(err)
	}
	account, err := nkeys.CreateAccount()
	if err != nil {
		t.Fatal(err)
	}
	defer account.Wipe()
	accountPub, err := account.PublicKey()
	if err != nil {
		t.Fatal(err)
	}
	accountSeed, err := account.Seed()
	if err != nil {
		t.Fatal(err)
	}
	permissions := auth.PermissionsForBroker()
	opts := natstest.DefaultTestOptions
	opts.Port = -1
	opts.Nkeys = []*natsserver.NkeyUser{{Nkey: brokerPub, Permissions: &natsserver.Permissions{
		Publish:   &natsserver.SubjectPermission{Allow: permissions.Pub.Allow, Deny: permissions.Pub.Deny},
		Subscribe: &natsserver.SubjectPermission{Allow: permissions.Sub.Allow, Deny: permissions.Sub.Deny},
	}}}
	opts.AuthCallout = &natsserver.AuthCallout{Issuer: accountPub, Account: "$G", AuthUsers: []string{brokerPub}}
	server := natstest.RunServer(&opts)
	t.Cleanup(func() { server.Shutdown(); server.WaitForShutdown() })
	db := openDB(t)
	seed := &Broker{cfg: Config{DB: db}}
	seedOwnedSession(t, seed, owner.PublicKey, "lab", "node", "ONLINE")
	if err := session.AddMember(db, "lab", member.Fingerprint, session.RoleMember, session.ViaPin, time.Now().UTC()); err != nil {
		t.Fatal(err)
	}
	if err := agentprov.Provision(db, "lab", "node", agentID.Fingerprint, time.Now().UTC()); err != nil {
		t.Fatal(err)
	}
	ready := make(chan struct{})
	b, err := New(Config{NATSURL: server.ClientURL(), DB: db, Logger: silentLogger(), Now: time.Now,
		ReconcileInterval: time.Hour, StaleAfter: time.Hour, OfflineAfter: 2 * time.Hour, ReadyCh: ready,
		AuthCallout: &AuthCalloutConfig{BrokerNkeySeed: brokerSeed, AccountSeed: append([]byte(nil), accountSeed...)}})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(t.Context())
	done := make(chan error, 1)
	go func() { done <- b.Run(ctx) }()
	t.Cleanup(func() {
		cancel()
		select {
		case err := <-done:
			if err != nil && !errors.Is(err, context.Canceled) {
				t.Errorf("broker teardown: %v", err)
			}
		case <-time.After(2 * processRouteBudget):
			t.Error("broker did not finish teardown")
		}
	})
	select {
	case <-ready:
	case <-time.After(processRouteBudget):
		t.Fatal("real broker did not install its subscriptions")
	}
	permissionErrors := make(chan error, 16)
	ctl, err := cli.ConnectNATSWithNkey(server.ClientURL(), member, nats.Name(cli.CtlNameForSession("lab")),
		nats.ErrorHandler(func(_ *nats.Conn, _ *nats.Subscription, err error) { permissionErrors <- err }))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(ctl.Close)
	agentConn, err := cli.ConnectNATSWithNkey(server.ClientURL(), agentID, nats.Name(cli.AgentName("lab", "node")))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(agentConn.Close)
	forwardSubject := proto.SubjCmdForwarded("lab", "node", proto.ProcessResumeVerb)
	forward, err := agentConn.SubscribeSync(forwardSubject)
	if err != nil {
		t.Fatal(err)
	}
	if err := agentConn.FlushTimeout(processRouteBudget); err != nil {
		t.Fatal(err)
	}
	reply := ctl.NewInbox()
	if !strings.HasPrefix(reply, auth.InboxPrefixFor(member.PublicKey)+".") {
		t.Fatal("fixture downgraded to legacy inbox")
	}
	replies, err := ctl.SubscribeSync(reply)
	if err != nil {
		t.Fatal(err)
	}
	if err := ctl.FlushTimeout(processRouteBudget); err != nil {
		t.Fatal(err)
	}
	subject := proto.SubjCmdBy("lab", member.PublicKey, "node", proto.ProcessResumeVerb)
	if err := ctl.PublishRequest(subject, reply, processRouteBody(t, "probe")); err != nil {
		t.Fatal(err)
	}
	message, err := forward.NextMsg(processRouteBudget)
	if err != nil {
		t.Fatalf("actual auth_callout member did not reach agent permission tree: %v", err)
	}
	var forwarded proto.ProcessResumeReq
	if err := json.Unmarshal(message.Data, &forwarded); err != nil || forwarded.ActorFP != member.Fingerprint {
		t.Fatalf("subject identity was not trusted: %+v %v", forwarded, err)
	}
	if err := message.Respond([]byte(`{"version":1,"kind":"probe"}`)); err != nil {
		t.Fatal(err)
	}
	if _, err := replies.NextMsg(processRouteBudget); err != nil {
		t.Fatalf("agent could not answer member's private inbox: %v", err)
	}
	for _, forbidden := range []string{proto.SubjCmdBy("lab", owner.PublicKey, "node", proto.ProcessResumeVerb), forwardSubject} {
		if err := ctl.PublishRequest(forbidden, reply, processRouteBody(t, "create")); err != nil {
			t.Fatal(err)
		}
		select {
		case err := <-permissionErrors:
			if !errors.Is(err, nats.ErrPermissionViolation) {
				t.Fatalf("expected actual NATS permission denial: %v", err)
			}
		case <-time.After(processRouteBudget):
			t.Fatal("JWT did not reject actor/forwarded spoof")
		}
	}
	if _, err := db.Exec(`DELETE FROM members WHERE sid=? AND pubkey_fp=?`, "lab", member.Fingerprint); err != nil {
		t.Fatal(err)
	}
	// Keep the original authenticated connection and its still-valid JWT.
	// CONNECT-time authorization cannot substitute for current request ACL.
	for _, op := range []string{"create", "resume", "ack", "terminal-ack"} {
		if err := ctl.PublishRequest(subject, reply, processRouteBody(t, op)); err != nil {
			t.Fatal(err)
		}
		msg, err := replies.NextMsg(processRouteBudget)
		if err != nil {
			t.Fatal(err)
		}
		var response proto.ProcessResumeReply
		if err := json.Unmarshal(msg.Data, &response); err != nil || response.Kind != "error" || !strings.Contains(response.Error, "not_a_member") {
			t.Fatalf("old JWT bypassed CURRENT membership: %+v %v", response, err)
		}
	}
	// A marker from the same broker publisher fences all possible earlier
	// forwards; unauthorized requests must not have reached the agent observer.
	marker := []byte("process-auth-forward-fence")
	if err := b.nc.Load().Publish(forwardSubject, marker); err != nil {
		t.Fatal(err)
	}
	if msg, err := forward.NextMsg(processRouteBudget); err != nil || !bytes.Equal(msg.Data, marker) {
		t.Fatalf("unauthorized request reached the agent: message=%v err=%v", msg, err)
	}
}

func FuzzPrivateProcessReply(f *testing.F) {
	seed, err := auth.GenerateUserSeed()
	if err != nil {
		f.Fatal(err)
	}
	actor, err := auth.PublicKeyFromSeed(seed)
	if err != nil {
		f.Fatal(err)
	}
	prefix := auth.InboxPrefixFor(actor)
	for _, reply := range []string{prefix + ".NUID.1", prefix + ".a_b-c", prefix, prefix + ".*", prefix + ".x\x00", prefix + "evil.x", "_TINBOX.foreign.actor.x", "_INBOX.legacy", prefix + "." + strings.Repeat("x", 513)} {
		f.Add(actor, reply)
	}
	f.Add(strings.Repeat("U", 56), "_TINBOX.bad.crc.x")
	// Regex is the independent public grammar, rather than a copy of the
	// production parser's per-token byte loop. Complete equality also detects
	// a parser that refuses every input and would satisfy safety-only checks.
	literal := regexp.MustCompile(`^[A-Za-z0-9_-]+(\.[A-Za-z0-9_-]+)*$`)
	f.Fuzz(func(t *testing.T, actor, reply string) {
		_, keyErr := auth.FingerprintFromActor(actor)
		ownPrefix := auth.InboxPrefixFor(actor) + "."
		suffix, matching := strings.CutPrefix(reply, ownPrefix)
		want := keyErr == nil && len(reply) <= 512 && matching && literal.MatchString(suffix)
		if got := privateProcessReply(actor, reply); got != want {
			t.Fatalf("private reply grammar mismatch: actor=%q reply=%q got=%v want=%v", actor, reply, got, want)
		}
	})
}

// origin: docs/reviews/hot-update-reliable-exec-plan.md §2.4 (gotcha #90) —
// a STALE node (heartbeat gap during a NATS or broker restart) must not turn
// an in-flight exec into node_offline: attach ops are forwarded, while a
// probe (a NEW task) and every op against an OFFLINE node are still refused.
func TestProcessResumeAttachOpsTolerateStaleNodeButNotOffline(t *testing.T) {
	f := newProcessRouteFixture(t)
	reply := auth.InboxPrefixFor(f.actor) + ".stale"
	subject := proto.SubjCmdBy("lab", f.actor, "node", proto.ProcessResumeVerb)
	setStatus := func(t *testing.T, status string) {
		t.Helper()
		if _, err := f.b.cfg.DB.Exec(`UPDATE nodes SET status=? WHERE sid=? AND nid=?`, status, "lab", "node"); err != nil {
			t.Fatal(err)
		}
	}
	refusal := func(t *testing.T, msgs []*nats.Msg) string {
		t.Helper()
		if len(msgs) != 1 || msgs[0].Subject != reply {
			t.Fatalf("expected exactly one refusal on the private reply: %v", msgs)
		}
		var r proto.ProcessResumeReply
		if err := json.Unmarshal(msgs[0].Data, &r); err != nil {
			t.Fatal(err)
		}
		return r.Code + "/" + r.Error
	}
	setStatus(t, "STALE")
	for _, op := range []string{"create", "resume", "ack", "terminal-ack"} {
		if fwd := processForwarded(f.invoke(t, subject, reply, processRouteBody(t, op))); len(fwd) != 1 {
			t.Fatalf("%s against a STALE node was not forwarded", op)
		}
	}
	if got := refusal(t, f.invoke(t, subject, reply, processRouteBody(t, "probe"))); got != "node_offline/node_offline: status=STALE" {
		t.Fatalf("probe against a STALE node: %s", got)
	}
	// An UNKNOWN op takes the strict spec too (architecture §22.4 item 4):
	// against a STALE node it is refused for the node, not for the op — the
	// admission check precedes the op check. Routing unknown ops to the
	// attach spec (`default:` on the switch) would answer
	// unsupported_process_operation here instead (S1 review protocol-V3).
	if got := refusal(t, f.invoke(t, subject, reply, processRouteBody(t, "frobnicate"))); got != "node_offline/node_offline: status=STALE" {
		t.Fatalf("unknown op against a STALE node: %s (want the strict node check)", got)
	}
	setStatus(t, "OFFLINE")
	for _, op := range []string{"probe", "create", "resume", "ack", "terminal-ack", "frobnicate"} {
		if got := refusal(t, f.invoke(t, subject, reply, processRouteBody(t, op))); got != "node_offline/node_offline: status=OFFLINE" {
			t.Fatalf("%s against an OFFLINE node: %s", op, got)
		}
	}
	setStatus(t, "ONLINE")
	if got := refusal(t, f.invoke(t, subject, reply, processRouteBody(t, "frobnicate"))); !strings.HasPrefix(got, proto.CodeProcessUnsupportedOp+"/") {
		t.Fatalf("unknown op against an ONLINE node: %s (want unsupported_process_operation)", got)
	}
}
