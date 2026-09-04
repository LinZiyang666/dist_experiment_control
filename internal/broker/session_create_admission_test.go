package broker

import (
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/LinZiyang666/tether/internal/auth"
	"github.com/LinZiyang666/tether/internal/cluster"
	"github.com/LinZiyang666/tether/internal/proto"
	"github.com/LinZiyang666/tether/internal/session"
	"github.com/LinZiyang666/tether/internal/testharness"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nkeys"
)

// realActor mints a genuine user nkey: handleSessionCreate validates the actor token
// before anything else, so a made-up string would exercise the wrong refusal.
func realActor(t *testing.T) string {
	t.Helper()
	kp, err := nkeys.CreateUser()
	if err != nil {
		t.Fatal(err)
	}
	pub, err := kp.PublicKey()
	if err != nil {
		t.Fatal(err)
	}
	kp.Wipe()
	return pub
}

// session_create_admission_test.go — who may create a session.
//
// origin: prerelease audit round 2.
//
// handleSessionCreate had NO admission control: a syntactically valid user nkey was the
// entire test. The control plane is reachable at wss://<broker>:443 by design, so a
// stranger could name a session, become its owner, and from there mint the
// activated-member permission template — and, with the PIN they had just chosen, the
// AGENT template as well. That is the three-step chain behind the `_INBOX` disclosure,
// and it is also why "an activated member is an authorized principal" was never a valid
// premise anywhere else in this audit.

// admissionBroker is a broker on a real bus — the reply path (replyJSON →
// respondBytes → the live conn) is what these tests read, so a fake would test
// something else.
func admissionBroker(t *testing.T) (*Broker, *nats.Conn) {
	t.Helper()
	nc, err := nats.Connect(testharness.StartNATS(t))
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	t.Cleanup(nc.Close)
	b := &Broker{}
	b.cfg.DB = testharness.OpenDB(t)
	b.cfg.Logger = silentLogger()
	b.cfg.Now = time.Now
	b.nc.Store(nc)
	return b, nc
}

// createAs drives the real handler on the real subject for one actor and returns the
// decoded reply, over the real bus.
func createAs(t *testing.T, b *Broker, nc *nats.Conn, actor, name, pin string) proto.SessionCreateResp {
	t.Helper()
	body, err := json.Marshal(proto.SessionCreateReq{Name: name, PIN: pin})
	if err != nil {
		t.Fatal(err)
	}
	// THE HANDLER MUST BE GIVEN A MESSAGE IT ACTUALLY RECEIVED. A hand-built *nats.Msg
	// carries no connection, so msg.Respond fails with "invalid connection" and the reply
	// this test reads never leaves — which looks exactly like a handler that returned
	// early. Publish it, catch it on a broker-side subscription, hand THAT to the handler.
	subj := proto.SubjCtrlBy(actor, "session.create.req")
	catch, err := nc.SubscribeSync(subj)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = catch.Unsubscribe() }()
	reply := nc.NewInbox()
	sub, err := nc.SubscribeSync(reply)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = sub.Unsubscribe() }()
	if err := nc.Flush(); err != nil {
		t.Fatal(err)
	}
	if err := nc.PublishRequest(subj, reply, body); err != nil {
		t.Fatal(err)
	}
	if err := nc.Flush(); err != nil {
		t.Fatal(err)
	}
	req, err := catch.NextMsg(3 * time.Second)
	if err != nil {
		t.Fatalf("the request never arrived: %v", err)
	}
	b.handleSessionCreate(req)
	msg, err := sub.NextMsg(3 * time.Second)
	if err != nil {
		t.Fatalf("no reply from handleSessionCreate: %v", err)
	}
	var got proto.SessionCreateResp
	if err := json.Unmarshal(msg.Data, &got); err != nil {
		t.Fatal(err)
	}
	return got
}

// origin: prerelease audit round 2.
//
// A STRANGER MAY NOT CREATE A SESSION. This is the root of the three-step chain: create
// a session → become its owner → mint the activated-member template → provision an agent
// against the PIN you just chose → mint the AGENT template.
func TestAnUnadmittedIdentityCannotCreateASession(t *testing.T) {
	b, nc := admissionBroker(t)
	stranger := realActor(t)

	resp := createAs(t, b, nc, stranger, "mine", "1234")
	if resp.Error == "" {
		t.Fatal("a stranger created a session.\n\n" +
			"Owning a session mints the activated-member template, and the PIN they just chose " +
			"mints the agent template — so with no admission control here, both of those " +
			"templates are reachable by anybody on the public control plane, and every argument " +
			"in this audit that treats an activated member as authorized is void.")
	}
	// THE CODE IS A FIELD, not a prefix on the sentence — origin: increment 2 internal
	// review, five lanes. It used to be buried in Error, so cmd/tether passed the whole
	// sentence to the exit-class table, missed, and exited 70 — which docs/usage.md §9.13
	// defines as "back off and retry" for a refusal that is permanent until a human acts.
	if resp.Code != "not_allowed" {
		t.Fatalf("refusal code = %q, want not_allowed (the CLI classifies the exit code from this "+
			"field; an unclassified refusal exits 70, which automation is told to retry)", resp.Code)
	}
	if !strings.Contains(resp.Error, "session-allow") {
		t.Errorf("the refusal (%q) does not tell the operator how to admit the identity; a "+
			"policy refusal with no remedy reads as a bug", resp.Error)
	}
	var n int
	if err := b.cfg.DB.QueryRow(`SELECT COUNT(*) FROM sessions`).Scan(&n); err != nil {
		t.Fatal(err)
	}
	if n != 0 {
		t.Fatalf("%d session row(s) were written despite the refusal", n)
	}
}

// origin: prerelease audit round 2.
//
// THE POSITIVE CONTROL, without which the test above is satisfied by a handler that
// refuses everything.
func TestAnAdmittedIdentityStillCreatesSessions(t *testing.T) {
	b, nc := admissionBroker(t)
	actor := realActor(t)
	fp, err := auth.FingerprintFromActor(actor)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := session.AllowCreator(b.cfg.DB, fp, "test", "", time.Now()); err != nil {
		t.Fatal(err)
	}

	resp := createAs(t, b, nc, actor, "lab", "1234")
	if resp.Error != "" {
		t.Fatalf("an ADMITTED identity was refused: %q", resp.Error)
	}
	if resp.SID == "" {
		t.Fatal("no sid in the reply")
	}
}

// origin: prerelease audit round 2.
//
// THE REFUSAL COMES BEFORE argon2. HashPIN costs the broker 0.15-0.3s of its single
// serialized callout goroutine and 64 MiB, and THIS call site was never charged against
// the process-wide ceiling internal/authcallout added for exactly that reason — so
// before admission control an unadmitted caller could burn broker CPU by the request,
// for free, anonymously.
//
// Asserted by TIME rather than by reading the source: what matters is that the work does
// not happen, and a reordering that keeps the refusal but moves it after the hash would
// pass any structural check.
func TestAnUnadmittedSessionCreateDoesNotSpendArgon2(t *testing.T) {
	b, nc := admissionBroker(t)

	// Baseline: what one real argon2 hash costs on this machine.
	t0 := time.Now()
	if _, err := auth.HashPIN("1234"); err != nil {
		t.Fatal(err)
	}
	oneHash := time.Since(t0)
	if oneHash < 10*time.Millisecond {
		t.Skipf("argon2 costs only %v here; this timing discriminator has no headroom", oneHash)
	}

	start := time.Now()
	for i := 0; i < 5; i++ {
		createAs(t, b, nc, realActor(t), "x", "1234")
	}
	spent := time.Since(start)
	if spent > 2*oneHash {
		t.Fatalf("five refused session creates took %v, i.e. more than two argon2 hashes (%v each).\n\n"+
			"The refusal must come BEFORE HashPIN: this call site is not charged against the "+
			"process-wide argon2 ceiling, so an unadmitted caller who reaches the hash can burn "+
			"the broker's single serialized callout goroutine by the request.", spent, oneHash)
	}
}

// origin: prerelease audit round 2.
//
// AN UNREADABLE POLICY TABLE MUST REFUSE, not be mistaken for an empty one. A broker
// that cannot read its own allow-list has no basis to admit anybody.
func TestAnUnreadableAllowListRefusesRatherThanAdmits(t *testing.T) {
	b, nc := admissionBroker(t)
	if _, err := b.cfg.DB.Exec(`DROP TABLE session_creators`); err != nil {
		t.Fatal(err)
	}
	resp := createAs(t, b, nc, realActor(t), "lab", "1234")
	if resp.Error == "" {
		t.Fatal("a broker that cannot read its allow-list created the session anyway")
	}
	if !strings.Contains(resp.Error, "store_error") {
		t.Errorf("refusal = %q; a read failure must be reported as a store error, not as a "+
			"policy decision the operator will try to fix by admitting a fingerprint", resp.Error)
	}
}

// origin: prerelease audit increment 2 internal review — reported independently by
// adminsock-cli/L10-F1, repo-invariants/F1, n1-quadrants/L3-F1, raft-op/F1,
// ops-upgrade/L16-F2 and admission-enforcement/L9-F2.
//
// AN ADMISSION CHANGE MUST NOT BE PROPOSED WHILE ANY VOTER WOULD POISON-SKIP IT.
//
// OpSessionCreatorSet is replicated. A voter whose binary predates it does NOT fail-stop:
// decodeCommand skips the entry, advancing applied_index without running the SQL. The
// leader's session_creators row appears, that voter's does not, and nothing anywhere
// reports a problem. For this particular table the divergence is a security one in both
// directions — an allow that does not take on one replica, or a revocation that does not
// — and which replica a ctl reaches is decided by a NATS queue group.
//
// The repository already had this gate for the v0.4.2 phase-fluidity ops; this op shipped
// without it. That is why the check is now a shared function rather than a
// single-purpose one with an op's name in every line.
func TestAnAdmissionWriteIsRefusedWhileAnyVoterCannotApplyIt(t *testing.T) {
	all := []cluster.ServerInfo{
		{NodeID: "self", Voter: true},
		{NodeID: "peer-a", Voter: true},
		{NodeID: "peer-b", Voter: true},
	}
	// THE PRODUCTION CAPABILITY, not a copy of it. An earlier draft of this test built its
	// own opCapability literal here; mutating the production one to read the wrong health
	// field left every assertion below green, because they were asserting against the
	// duplicate. A guard must hold the thing it guards.
	check := func(cfg []cluster.ServerInfo, health map[string]proto.ClusterHealthResp) error {
		return checkVotersSupportOps("session-allow SHA256:x", cfg, "self", health, sessionCreatorCapability)
	}

	// N=1: only self applies the op, so no divergence is possible and admission must work
	// on a single broker with no health map at all.
	if err := check(all[:1], nil); err != nil {
		t.Fatalf("a single-broker deployment must be able to admit a fingerprint: %v", err)
	}
	// Every non-self voter advertises the capability — allowed.
	ok := map[string]proto.ClusterHealthResp{
		"peer-a": {SessionCreatorOps: true}, "peer-b": {SessionCreatorOps: true},
	}
	if err := check(all, ok); err != nil {
		t.Fatalf("an all-upgraded cluster must be able to admit a fingerprint: %v", err)
	}
	// One voter predates the op — refuse.
	old := map[string]proto.ClusterHealthResp{
		"peer-a": {SessionCreatorOps: true}, "peer-b": {SessionCreatorOps: false},
	}
	if err := check(all, old); err == nil {
		t.Fatal("a voter without the admission op must be refused: it would poison-skip the write, " +
			"and `tether admin session-allow` would report success while that broker keeps refusing " +
			"the fingerprint")
	}
	// A voter is unreachable — fail CLOSED. "I could not ask" is not "it is fine"; the
	// same distinction the destructive-op gate needed.
	if err := check(all, map[string]proto.ClusterHealthResp{"peer-a": {SessionCreatorOps: true}}); err == nil {
		t.Fatal("an unreachable voter must be refused (fail-closed)")
	}
	// THE GATE READS ITS OWN CAPABILITY BIT, not any capability bit. Without this row a
	// gate wired to the wrong field — PhaseFluidityOps, say, which every current broker
	// also sets — passes every case above.
	wrongBit := map[string]proto.ClusterHealthResp{
		"peer-a": {PhaseFluidityOps: true, SessionCreatorOps: true},
		"peer-b": {PhaseFluidityOps: true, SessionCreatorOps: false},
	}
	if err := check(all, wrongBit); err == nil {
		t.Fatal("a voter advertising OTHER capabilities but not this one must still be refused")
	}
	// A NONVOTER is ignored: only voters apply a replicated op.
	withLearner := append(append([]cluster.ServerInfo{}, all...),
		cluster.ServerInfo{NodeID: "learner", Voter: false})
	if err := check(withLearner, ok); err != nil {
		t.Fatalf("a nonvoter must not gate the write: %v", err)
	}
}

// origin: increment 2 internal review, raft-op/F1.
//
// WHAT THIS PROVES, EXACTLY: that OpSessionCreatorSet is in the FSM's known-ops registry
// and that the capability accessor agrees with it. That catches the realistic mistake —
// adding an op to the allowed-ops map and forgetting the registry, which would make every
// upgraded broker advertise "I cannot apply this" and refuse admission changes cluster-wide.
//
// WHAT IT DOES NOT PROVE, stated so nobody reads more into it: it cannot detect an
// accessor hard-coded to `return true`, because the registry lookup it compares against
// is also true. No test in this package can — knownOps is a package-level map in
// internal/cluster with no seam. The protection against that is that the accessor is two
// lines long and sits directly under the one it copies.
func TestTheAdmissionCapabilityAgreesWithTheOpRegistry(t *testing.T) {
	if got, want := cluster.HasSessionCreatorOps(), cluster.KnownOpsForDocs()[cluster.OpSessionCreatorSet]; got != want {
		t.Fatalf("HasSessionCreatorOps() = %v but the known-ops registry says %v — the capability "+
			"this broker advertises has come loose from the vocabulary its FSM actually has", got, want)
	}
	if !cluster.KnownOpsForDocs()[cluster.OpSessionCreatorSet] {
		t.Fatal("OpSessionCreatorSet is not in the FSM's known-ops registry, so every voter would " +
			"poison-skip it and the mixed-version gate would refuse every admission change")
	}
}
