package broker

import (
	"context"
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/nats-io/nats.go"

	"github.com/LinZiyang666/tether/internal/proto"
)

// origin: simcluster-speed 0a (drill 67 CONTROL(after): twelve too_many_in_flight refusals against a
// healthy cluster, all waiting on one abandoned upload).

// TestClaimAbandonedPushOnlyBeforeCommit pins the tracker rule the finalize handler leans on: a push
// creator may end its own transfer ONLY while it is tier B and no commit has been forwarded. The rows
// are the ways the claim can be wrong — a pull entry, a committed push, an already-finalized push, a
// tier-A push (external review F2: no commit phase, so `committed` is false for its whole life and the
// receiving agent has owned the terminal since the push.req), and a miss — each with the reason the
// handler words its reply from.
func TestClaimAbandonedPushOnlyBeforeCommit(t *testing.T) {
	tr := newTransferTracker()
	put := func(id, verb string) *transferEntry {
		e := &transferEntry{transferID: id, sid: "lab", nid: "lab-1", verb: verb, tier: "b", bucket: "xfer-" + id}
		if code := tr.put(e); code != "" {
			t.Fatalf("put %s: %s", id, code)
		}
		return e
	}
	put("push-fresh", "push")
	put("push-committed", "push")
	put("pull", "pull")
	// A tier-A push exactly as handlePushReq tracks it: tier "a", no bucket, never committed.
	if code := tr.put(&transferEntry{transferID: "push-tier-a", sid: "lab", nid: "lab-1", verb: "push", tier: "a"}); code != "" {
		t.Fatalf("put push-tier-a: %s", code)
	}
	fin := put("push-finalized", "push")
	if _, ok := tr.claimFinalize(fin); !ok {
		t.Fatal("seed claimFinalize failed")
	}
	if !tr.markCommitted(tr.get("push-committed")) {
		t.Fatal("markCommitted on a live push must succeed")
	}
	if tr.markCommitted(tr.get("push-finalized")) {
		t.Fatal("markCommitted on a finalized entry must be refused — the watchdog already owns it")
	}
	if tr.markCommitted(tr.get("absent")) {
		t.Fatal("markCommitted on a miss must be refused")
	}

	cases := []struct {
		id   string
		want abandonRefusal // the state as read under the lock (R6-F3: the caller must not re-read it racily)
	}{
		{"push-fresh", abandonClaimed},
		{"push-committed", abandonCommitted},
		{"pull", abandonNotPush},
		{"push-tier-a", abandonTierA},
		{"push-finalized", abandonFinalized},
		{"absent", abandonNoEntry},
	}
	for _, c := range cases {
		e, got := tr.claimAbandonedPush(tr.get(c.id))
		if got != c.want {
			t.Fatalf("claimAbandonedPush(%s) = %v, want %v", c.id, got, c.want)
		}
		if got != abandonClaimed && got != abandonNoEntry && e.finalized && c.id != "push-finalized" {
			t.Fatalf("claimAbandonedPush(%s) refused with %v but marked the entry finalized", c.id, got)
		}
	}
	// A successful claim is a claim: the second call sees finalized=true.
	if _, again := tr.claimAbandonedPush(tr.get("push-fresh")); again != abandonFinalized {
		t.Fatalf("claimAbandonedPush after it succeeded once = %v, want abandonFinalized", again)
	}
	// The refused tier-A push is untouched: still tracked, still claimable by its real terminal.
	if e := tr.get("push-tier-a"); e == nil || e.finalized {
		t.Fatalf("tier-A push after the refused abandon: entry=%v — it must remain for the agent's ev.transfer", e)
	}
	if _, ok := tr.claimFinalize(tr.get("push-tier-a")); !ok {
		t.Fatal("the agent's terminal (claimFinalize) must still be able to claim the tier-A push")
	}
}

// TestInFlightRefusalTextNamesTheBlocker: the per-bucket refusal must say WHICH transfer holds the
// bucket and must not advise a fresh transfer id (that is refused identically). The cap-shaped refusal
// keeps the old wording, where a fresh id is genuinely irrelevant to the cause.
func TestInFlightRefusalTextNamesTheBlocker(t *testing.T) {
	tr := newTransferTracker()
	first := &transferEntry{transferID: "held", sid: "lab", verb: "push", tier: "b", bucket: "xfer-lab"}
	if code, _ := tr.putWithBlocker(first); code != "" {
		t.Fatalf("first put refused: %s", code)
	}
	second := &transferEntry{transferID: "next", sid: "lab", verb: "push", tier: "b", bucket: "xfer-lab"}
	code, blocker := tr.putWithBlocker(second)
	if code != "too_many_in_flight" || blocker != "held" {
		t.Fatalf("second put: code=%q blocker=%q, want too_many_in_flight/held", code, blocker)
	}
	text := inFlightRefusalText("next", code, blocker)
	if !strings.Contains(text, "held") || !strings.Contains(text, "in flight in this session's bucket") {
		t.Fatalf("bucket refusal must name the holder: %s", text)
	}
	if strings.Contains(text, "use a fresh transfer id") {
		t.Fatalf("bucket refusal must not advise a fresh id: %s", text)
	}
	// The cap-shaped refusal (no blocker) keeps the generic advice.
	generic := inFlightRefusalText("x", "too_many_in_flight", "")
	if !strings.Contains(generic, "retry shortly") {
		t.Fatalf("cap refusal lost its advice: %s", generic)
	}
	// Once the holder is gone the bucket admits the next transfer — the property the finalize exists for.
	tr.remove(first)
	if code, _ := tr.putWithBlocker(second); code != "" {
		t.Fatalf("bucket still refused after the holder left: %s", code)
	}
}

// TestPushCreatorFinalizeFreesTheBucketBeforeCommit drives handleFinalizeReq over real NATS: a push
// creator's finalize{failed} before commit is accepted, removes the entry, and frees the session's
// bucket for the next tier-B transfer; after markCommitted the same finalize is refused verb_mismatch
// (the agent's ev.transfer owns the terminal now); a creator finalize{complete} for a push is refused
// regardless. Deleting the push branch in handleFinalizeReq turns the first arm red and only it.
func TestPushCreatorFinalizeFreesTheBucketBeforeCommit(t *testing.T) {
	url := startNATS(t)
	nc, err := nats.Connect(url)
	if err != nil {
		t.Fatal(err)
	}
	defer nc.Close()

	actor := freshUserActor(t)
	b := &Broker{cfg: Config{DB: openDB(t), Logger: silentLogger(), Now: time.Now}, transfers: newTransferTracker()}
	b.nc.Store(nc)
	seedD8TransferMemberNode(t, b, actor, "lab", "lab-1")

	finalize := func(tid string, fin proto.TransferFinalize) proto.TransferFinalizeResp {
		t.Helper()
		subj := proto.SubjCtrlTransferFinalize(actor, "lab", tid)
		sub, err := nc.Subscribe(subj, b.handleFinalizeReq)
		if err != nil {
			t.Fatal(err)
		}
		defer func() { _ = sub.Unsubscribe() }()
		if err := nc.Flush(); err != nil {
			t.Fatal(err)
		}
		body, _ := json.Marshal(fin)
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()
		msg, err := nc.RequestWithContext(ctx, subj, body)
		if err != nil {
			t.Fatalf("finalize %s: %v", tid, err)
		}
		var fr proto.TransferFinalizeResp
		if err := json.Unmarshal(msg.Data, &fr); err != nil {
			t.Fatal(err)
		}
		return fr
	}
	newPush := func(tid string) *transferEntry {
		e := &transferEntry{transferID: tid, sid: "lab", nid: "lab-1", actor: actor, verb: "push", tier: "b",
			bucket: proto.XferBucketName("lab"), path: "/tmp/x", size: 12, startedAt: time.Now()}
		if code, blocker := b.transfers.putWithBlocker(e); code != "" {
			t.Fatalf("put %s refused: %s (blocker %s)", tid, code, blocker)
		}
		return e
	}

	// Arm 1: abandoned before commit → accepted, entry gone, bucket free.
	newPush("abandoned")
	fr := finalize("abandoned", proto.TransferFinalize{Kind: "failed", TransferID: "abandoned", Tier: "b", Code: "object_put_failed", Error: "no responders"})
	if !fr.OK {
		t.Fatalf("pre-commit push finalize{failed} refused: code=%s err=%s", fr.Code, fr.Error)
	}
	if b.transfers.get("abandoned") != nil {
		t.Fatal("abandoned entry still in the tracker after finalize")
	}
	newPush("next") // the bucket must admit the session's next transfer at once

	// Arm 2: after commit the creator may not end it.
	if !b.transfers.markCommitted(b.transfers.get("next")) {
		t.Fatal("markCommitted failed")
	}
	fr = finalize("next", proto.TransferFinalize{Kind: "failed", TransferID: "next", Tier: "b", Code: "object_put_failed"})
	if fr.OK || fr.Code != "verb_mismatch" {
		t.Fatalf("post-commit push finalize must be refused verb_mismatch, got ok=%v code=%s", fr.OK, fr.Code)
	}
	if b.transfers.get("next") == nil {
		t.Fatal("a refused finalize must leave the committed entry in place")
	}

	// Arm 3: a creator can never vouch a push COMPLETED.
	newPush2 := newPush
	b.transfers.remove(b.transfers.get("next"))
	newPush2("claimed-complete")
	fr = finalize("claimed-complete", proto.TransferFinalize{Kind: "complete", TransferID: "claimed-complete", Tier: "b", Bytes: 12})
	if fr.OK || fr.Code != "verb_mismatch" {
		t.Fatalf("push finalize{complete} from the creator must be refused verb_mismatch, got ok=%v code=%s", fr.OK, fr.Code)
	}
	if b.transfers.get("claimed-complete") == nil {
		t.Fatal("a refused complete must leave the entry in place")
	}
	b.transfers.remove(b.transfers.get("claimed-complete"))

	// Arm 4 (external review F2): a tier-A push, tracked exactly as handlePushReq tracks it (tier "a",
	// no bucket, committed never set because tier A has no commit phase), with its watchdog armed. The
	// creator's finalize{failed} must be refused whatever Tier the body claims, the entry and its
	// watchdog must survive, and the agent's real terminal must still be the one that ends it.
	tierA := &transferEntry{transferID: "tier-a-receiving", sid: "lab", nid: "lab-1", actor: actor, verb: "push", tier: "a",
		path: "/tmp/dest", size: 12, startedAt: time.Now()}
	if code, blocker := b.transfers.putWithBlocker(tierA); code != "" {
		t.Fatalf("put tier-a refused: %s (blocker %s)", code, blocker)
	}
	wdCtx, wdCancel := context.WithCancel(context.Background())
	defer wdCancel()
	tierA.cancel = wdCancel // stands in for startTransferWatchdog's cancel: a finalize that fires it kills the watchdog
	for _, bodyTier := range []string{"a", "b", ""} {
		fr = finalize("tier-a-receiving", proto.TransferFinalize{Kind: "failed", TransferID: "tier-a-receiving", Tier: bodyTier, Code: "io_error"})
		if fr.OK || fr.Code != "verb_mismatch" {
			t.Fatalf("body Tier=%q: creator finalize{failed} on a tier-A push must be refused verb_mismatch, got ok=%v code=%s err=%s", bodyTier, fr.OK, fr.Code, fr.Error)
		}
		if !strings.Contains(fr.Error, "tier-a") {
			t.Fatalf("body Tier=%q: the refusal must say why (tier-a), got %q", bodyTier, fr.Error)
		}
		if got := b.transfers.get("tier-a-receiving"); got == nil || got.finalized {
			t.Fatalf("body Tier=%q: the tier-A entry must stay tracked and unclaimed for the agent's terminal, got %+v", bodyTier, got)
		}
		if wdCtx.Err() != nil {
			t.Fatalf("body Tier=%q: the refused finalize cancelled the transfer watchdog", bodyTier)
		}
	}
	// The receiving agent's terminal is unaffected: it claims exactly once.
	if _, ok := b.transfers.claimFinalize(b.transfers.get("tier-a-receiving")); !ok {
		t.Fatal("the agent's terminal could not claim the tier-A push after the refused abandon")
	}
	if _, ok := b.transfers.claimFinalize(b.transfers.get("tier-a-receiving")); ok {
		t.Fatal("a second terminal claim must be refused — exactly one terminal per transfer")
	}
}

// origin: simcluster-speed internal review round 1 R4-F2. The handoff itself is the invariant: a
// push-commit that the broker FORWARDS marks the entry committed under the tracker lock BEFORE the
// forward, so a creator finalize{failed} arriving afterwards is refused (the agent's ev.transfer owns the
// terminal). Without this test, deleting the markCommitted call in handlePushCommitReq left every test
// green — the previous test exercised markCommitted directly, never the handler that must call it.
func TestPushCommitHandoffMarksTheEntryCommittedBeforeForwarding(t *testing.T) {
	url := startNATS(t)
	nc, err := nats.Connect(url)
	if err != nil {
		t.Fatal(err)
	}
	defer nc.Close()
	actor := freshUserActor(t)
	b := &Broker{cfg: Config{DB: openDB(t), Logger: silentLogger(), Now: time.Now}, transfers: newTransferTracker()}
	b.nc.Store(nc)
	seedD8TransferMemberNode(t, b, actor, "lab", "lab-1")
	e := &transferEntry{transferID: "t1", sid: "lab", nid: "lab-1", actor: actor, verb: "push", tier: "b",
		bucket: proto.XferBucketName("lab"), path: "/tmp/x", size: 12, startedAt: time.Now()}
	if code, _ := b.transfers.putWithBlocker(e); code != "" {
		t.Fatalf("put refused: %s", code)
	}
	// The broker's commit handler on the ctl-facing subject; a fake agent answers the forwarded copy so
	// the request completes the way a real one does.
	commitSubj := proto.SubjCmdBy("lab", actor, "lab-1", "push-commit")
	hsub, err := nc.Subscribe(commitSubj, func(msg *nats.Msg) { b.handlePushCommitReq(nc, msg) })
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = hsub.Unsubscribe() }()
	forwarded := 0
	asub, err := nc.Subscribe(proto.SubjCmdForwarded("lab", "lab-1", "push-commit"), func(msg *nats.Msg) {
		forwarded++
		_ = msg.Respond([]byte(`{"ok":true}`))
	})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = asub.Unsubscribe() }()
	if err := nc.Flush(); err != nil {
		t.Fatal(err)
	}
	body, _ := json.Marshal(proto.TransferCommitReq{TransferID: "t1"})
	if _, err := nc.Request(commitSubj, body, 3*time.Second); err != nil {
		t.Fatalf("push-commit round trip: %v", err)
	}
	if forwarded != 1 {
		t.Fatalf("the commit must be forwarded exactly once, got %d", forwarded)
	}
	// The handoff happened: the creator can no longer abandon it, and the tracker says why.
	if _, why := b.transfers.claimAbandonedPush(b.transfers.get("t1")); why != abandonCommitted {
		t.Fatalf("after a forwarded commit claimAbandonedPush must refuse with abandonCommitted, got %v", why)
	}
	fsub, err := nc.Subscribe(proto.SubjCtrlTransferFinalize(actor, "lab", "t1"), b.handleFinalizeReq)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = fsub.Unsubscribe() }()
	if err := nc.Flush(); err != nil {
		t.Fatal(err)
	}
	fb, _ := json.Marshal(proto.TransferFinalize{Kind: "failed", TransferID: "t1", Tier: "b", Code: "object_put_failed"})
	msg, err := nc.Request(proto.SubjCtrlTransferFinalize(actor, "lab", "t1"), fb, 3*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	var fr proto.TransferFinalizeResp
	if err := json.Unmarshal(msg.Data, &fr); err != nil {
		t.Fatal(err)
	}
	if fr.OK || fr.Code != "verb_mismatch" || !strings.Contains(fr.Error, "already committed") {
		t.Fatalf("a creator finalize{failed} after the commit handoff must be refused as already committed, got ok=%v code=%s err=%s", fr.OK, fr.Code, fr.Error)
	}
	if b.transfers.get("t1") == nil {
		t.Fatal("the committed entry must survive the refused finalize")
	}
}
