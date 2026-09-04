package broker

import (
	"database/sql"
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/LinZiyang666/tether/internal/adminsock"
	"github.com/LinZiyang666/tether/internal/cluster"
	"github.com/LinZiyang666/tether/internal/proto"
	"github.com/LinZiyang666/tether/internal/testharness"
	"github.com/nats-io/nats.go"
)

// alert_ack_validation_test.go — what the cluster ACK responder accepts.
//
// origin: prerelease audit broker-cluster-ops/L3-F2.
//
// The responder checked only that dedup_key was non-empty. What sits downstream of it
// is a raft Propose: whatever arrives is replicated to every broker and replayed on
// every restore, forever. The manual-alert admin path has validated its own text since
// it was written; this path, which is reachable over NATS rather than over the admin
// socket, had not.
//
// The test drives the REAL responder over a REAL bus, because the defect was that a
// message reached the forward at all — a unit test of validAlertText would have passed
// throughout.
func TestAlertAckValidatesTheDedupKeyBeforeForwarding(t *testing.T) {
	url := testharness.StartNATS(t)
	nc, err := nats.Connect(url)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(nc.Close)

	// A forwarder pointed at a bus with NO leader responder. If a request gets past
	// validation it fails HERE, with a different message — which is exactly the
	// discriminator this test needs: "refused before forwarding" versus "forwarded and
	// the forward failed".
	fwd := NewForwarder(nc, 250*time.Millisecond)
	sub, err := SubscribeAlertAck(nc, fwd, testharness.SilentLog())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = sub.Unsubscribe() })
	if err := nc.Flush(); err != nil {
		t.Fatal(err)
	}

	ack := func(t *testing.T, key string) string {
		t.Helper()
		body, _ := json.Marshal(proto.AlertAckReq{DedupKey: key})
		reply, rerr := nc.Request(proto.SubjCtrlAlertAck("SHA256:operator"), body, 10*time.Second)
		if rerr != nil {
			t.Fatalf("no reply from the ack responder: %v", rerr)
		}
		return string(reply.Data)
	}

	cases := []struct {
		name   string
		key    string
		reason string
	}{
		{"over-length", strings.Repeat("k", maxAlertDedupKeyLen+1), "too long"},
		{"NUL byte", "quorum\x00lost", "NUL"},
		// NOT invalid UTF-8: encoding/json replaces malformed bytes with U+FFFD while
		// decoding, so by the time validAlertText sees the string it is already valid.
		// That arm of the validator is unreachable through this path and asserting it
		// here would be asserting something the JSON decoder does.
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := ack(t, tc.key)
			if !strings.Contains(got, tc.reason) {
				t.Fatalf("reply %q does not report the %s problem.\n\n"+
					"An unvalidated key is not merely ugly: the responder forwards it into a raft "+
					"Propose, so it is replicated to every broker and replayed on every restore. "+
					"The only reply that means 'this was refused before it could become durable' is "+
					"one that names what was wrong with it.", got, tc.name)
			}
		})
	}

	// POSITIVE CONTROL. A well-formed key must NOT be refused by validation — it has to
	// get far enough to fail at the forward. Without this the test above is satisfied by
	// a responder that refuses everything.
	got := ack(t, "quorum_lost:ctl-2")
	for _, banned := range []string{"too long", "NUL"} {
		if strings.Contains(got, banned) {
			t.Fatalf("a well-formed dedup key was refused by validation: %q", got)
		}
	}
	if !strings.HasPrefix(got, "error:") {
		t.Fatalf("expected the well-formed key to reach the forward and fail there (no leader on "+
			"this bus), got %q — if it succeeded, this test is no longer discriminating between "+
			"'refused by validation' and 'forwarded'", got)
	}
}

// TestAlertAckRefusesAKeyWithNoAlertBehindIt is the leader-side half of L3-F2.
//
// Validation stops malformed keys; it does not stop well-formed ones that name nothing.
// Acking a key with no alert behind it wrote a permanent alert_acks row for an event
// that never happened, and alert_acks is replicated and replayed on restore, so nothing
// ever removed it. The check belongs on the LEADER, in the Propose closure, because
// that is the only place with the committed view.
func TestAlertAckRefusesAKeyWithNoAlertBehindIt(t *testing.T) {
	n, _ := d7SingleNode(t, "ctl-1")
	arm, ok := writeVerbs[VerbAlertAck]
	if !ok {
		t.Fatal("SELF-CHECK FAILED: VerbAlertAck has no forward arm; this guard is testing nothing")
	}
	now := func() time.Time { return time.Now().UTC() }
	ack := func(key string) error {
		payload, merr := json.Marshal(AlertAckPayload{DedupKey: key, AckedBy: "SHA256:operator"})
		if merr != nil {
			t.Fatal(merr)
		}
		return arm(n, now, forwardEnvelope{Verb: VerbAlertAck, Payload: payload}, forwardDeps{})
	}

	if err := ack("no-such-alert"); err == nil {
		t.Fatal("the leader accepted an ack for a dedup_key with no alert behind it.\n\n" +
			"That commits a row into alert_acks, which is replicated to every broker and replayed on " +
			"every restore. Nothing removes it, so a typo — or anyone who can publish on the ack " +
			"subject — leaves a permanent record of acknowledging something that never happened.")
	}

	// POSITIVE CONTROL: a key that DOES name an alert must be accepted, or the check
	// above is satisfied by a leader that refuses every ack.
	key := cluster.DedupKeyNode(cluster.AlertKindBelowQuorum, "ctl-2")
	if perr := n.Propose(func(_ *sql.DB) (*cluster.Command, error) {
		return cluster.PlanAlertRaise("alert-seed", cluster.AlertKindBelowQuorum, cluster.AlertSeveritySevere, key, "seeded", now())
	}); perr != nil {
		t.Fatalf("seed an alert: %v", perr)
	}
	if err := ack(key); err != nil {
		t.Fatalf("the leader refused an ack for an alert that EXISTS: %v", err)
	}
}

// origin: prerelease audit round 2, G-6.
//
// AN ALERT THIS PRODUCT CAN RAISE MUST BE ACKABLE.
//
// The dedup-key cap (256) was added to the ACK responder only, while the RAISE path
// validated its `--label` against maxAlertTextLen (4096) and then MINTED the key from
// it. Every label between those two numbers therefore produced an alert that showed up
// in `cluster alerts ls` and could never be acknowledged — a permanent banner with no
// operator action that clears it, which is the state alerts exist to avoid.
//
// The bound is asserted against WHAT THE OTHER END MINTS, not against a literal: a cap
// is only right relative to the values the product can actually produce.
func TestEveryDedupKeyTheRaisePathCanMintIsAckable(t *testing.T) {
	// The longest label the raise path admits, and the key it mints from it.
	longest := strings.Repeat("x", maxAlertTextLen)
	if err := validAlertText("--label", longest, maxAlertTextLen); err != nil {
		t.Fatalf("premise broken: the longest legal label is not legal (%v)", err)
	}
	widest := cluster.DedupKeyNode(cluster.AlertKindManual, longest)
	if len(widest) <= maxAlertDedupKeyLen {
		t.Skip("the label cap is now below the key cap; the gap this test guards cannot exist")
	}

	// handleAlertRaise must refuse it — and refuse it BEFORE any raft Propose, which a
	// nil admin proves: reaching b.admin.now() would panic.
	b := &clusterAdminBackend{}
	resp := b.handleAlertRaise(adminsock.Request{
		AlertKind: cluster.AlertKindManual, AlertSeverity: cluster.AlertSeveritySevere,
		AlertMessage: "disk is on fire", AlertLabel: longest,
	})
	if resp.OK || resp.Code != adminsock.CodeBadRequest {
		t.Fatalf("a manual alert was accepted under a dedup_key the ack responder refuses "+
			"(ok=%v code=%q).\n\n"+
			"It appears in `cluster alerts ls`, and every `alert ack` for it is rejected as a bad "+
			"request — a permanent banner with no operator action that clears it.", resp.OK, resp.Code)
	}
	if !strings.Contains(resp.Error, "--label") {
		t.Errorf("the refusal (%q) does not name --label, which is the input the operator controls",
			resp.Error)
	}

	// And the CLEAR path must agree with both: it takes the same value, and accepting a
	// key wider than anything that can be raised means the three surfaces handling one
	// value disagree about what it is.
	//
	// A key just past the ack cap and well INSIDE the text cap — the whole gap lives
	// there. `widest` above is longer than maxAlertTextLen too, so it would be refused
	// as text no matter what this cap said, and asserting on it proves nothing.
	overAck := strings.Repeat("x", maxAlertDedupKeyLen+1)
	if len(overAck) >= maxAlertTextLen {
		t.Fatal("premise broken: the two caps no longer leave a gap to test")
	}
	cleared := b.handleAlertClear(adminsock.Request{AlertDedupKey: overAck})
	if cleared.Code != adminsock.CodeBadRequest {
		t.Errorf("`alert clear` accepted a dedup_key (%d bytes) that can neither be raised nor "+
			"acked; code=%q.\n\n"+
			"Three surfaces handle this one value and they must agree what it is.",
			len(overAck), cleared.Code)
	}
}
