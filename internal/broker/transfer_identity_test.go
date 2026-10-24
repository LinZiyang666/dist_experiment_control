// origin: hot-update S7 plan §5.1 / §11.4 / P31; use the real leaf and locked record snapshots.
package broker

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/LinZiyang666/tether/internal/proto"
	"github.com/LinZiyang666/tether/internal/testharness"
	"github.com/LinZiyang666/tether/internal/xfertrack"
	"github.com/nats-io/nats.go"
)

// Real NATS handlers and SQLite authorization; tracker removal/insertion controls the interleaving.
// This exercises continuation ownership, without transferring bytes or provisioning JetStream.
// origin: simcluster-speed external review round 3 R3-F1
func TestTransferContinuationDoesNotClaimAReusedID(t *testing.T) {
	for _, op := range []string{"push-finalize", "pull-finalize", "push-commit"} {
		t.Run(op, func(t *testing.T) {
			nc, err := nats.Connect(startNATS(t))
			if err != nil {
				t.Fatal(err)
			}
			defer nc.Close()
			actor := freshUserActor(t)
			db := openDB(t)
			b := &Broker{cfg: Config{DB: db, Logger: silentLogger(), Now: time.Now}, transfers: xfertrack.NewLegacyTracker()}
			b.nc.Store(nc)
			seedD8TransferMemberNode(t, b, actor, "lab", "lab-1")
			const id = "reused-id"
			verb := "push"
			if op == "pull-finalize" {
				verb = "pull"
			}
			old := xfertrack.NewLegacyEntry(xfertrack.LegacyRecord{TransferID: id, SID: "lab", NID: "lab-1", Actor: actor, Verb: verb, Tier: "b", StartedAt: time.Now()})
			if code := putLegacyTransfer(b.transfers, old); code != "" {
				t.Fatal(code)
			}
			subj := proto.SubjCtrlTransferFinalize(actor, "lab", id)
			handler := b.handleFinalizeReq
			body, err := json.Marshal(proto.TransferFinalize{TransferID: id, Kind: "failed", Tier: "b"})
			if op == "push-commit" {
				subj = proto.SubjCmdBy("lab", actor, "lab-1", "push-commit")
				handler = func(m *nats.Msg) { b.handlePushCommitReq(nc, m) }
				body, err = json.Marshal(proto.TransferCommitReq{TransferID: id})
			}
			if err != nil {
				t.Fatal(err)
			}
			sub, err := nc.Subscribe(subj, handler)
			if err != nil {
				t.Fatal(err)
			}
			defer func() { _ = sub.Unsubscribe() }()
			forwarded, err := nc.SubscribeSync(proto.SubjCmdForwarded("lab", "lab-1", "push-commit"))
			if err != nil {
				t.Fatal(err)
			}
			defer func() { _ = forwarded.Unsubscribe() }()
			if err := nc.Flush(); err != nil {
				t.Fatal(err)
			}

			// Occupy the only DB connection. WaitCount proves the handler has read its preview
			// and entered transferGate; no scheduler delay or production test hook is needed.
			db.SetMaxOpenConns(1)
			held, err := db.Conn(context.Background())
			if err != nil {
				t.Fatal(err)
			}
			defer func() { _ = held.Close() }()
			waiting := db.Stats().WaitCount
			type result struct {
				msg *nats.Msg
				err error
			}
			done := make(chan result, 1)
			go func() {
				m, e := nc.Request(subj, body, 10*time.Second)
				done <- result{m, e}
			}()
			if !testharness.WaitFor(t, 5*time.Second, time.Millisecond, func() bool { return db.Stats().WaitCount > waiting }) {
				t.Fatal("continuation did not reach authorization after reading its preview")
			}
			b.transfers.Remove(old)
			replacement := xfertrack.NewLegacyEntry(xfertrack.LegacyRecord{TransferID: id, SID: "other", NID: "other-1", Actor: freshUserActor(t), Verb: "push", Tier: "b", StartedAt: time.Now()})
			if code := putLegacyTransfer(b.transfers, replacement); code != "" {
				t.Fatal(code)
			}
			if err := held.Close(); err != nil {
				t.Fatal(err)
			}
			got := <-done
			if got.err != nil {
				t.Fatal(got.err)
			}
			var resp proto.TransferFinalizeResp
			if err := json.Unmarshal(got.msg.Data, &resp); err != nil {
				t.Fatal(err)
			}
			if op == "push-commit" && (resp.OK || resp.Code != "transfer_unknown") {
				t.Fatalf("stale commit response = %+v, want transfer_unknown", resp)
			}
			if b.transfers.Get(id) != replacement || b.transfers.Record(replacement).Finalized || b.transfers.Record(replacement).Committed {
				t.Fatalf("%s authorized against an old entry changed another session's replacement", op)
			}
			if err := nc.Flush(); err != nil {
				t.Fatal(err)
			}
			if queued, _, err := forwarded.Pending(); err != nil || queued != 0 {
				t.Fatalf("stale commit was forwarded: pending=%d err=%v", queued, err)
			}
		})
	}
}

// Same owner and identical fields are insufficient: a new entry is a new lifetime, including for
// stale watchdog/cleanup callbacks which have already been released from their wait. The remove row
// is the main process's follow-through: every production remove follows a claim on the same entry,
// and binding it to the entry too leaves put (refuses a live id) and get (the preview) as the only
// by-id tracker paths — the argument "a claimed entry cannot be replaced" no longer has to be made.
func TestTransferClaimsRejectReplacedEntries(t *testing.T) {
	for _, op := range []string{"finalize", "commit", "abandon", "remove"} {
		t.Run(op, func(t *testing.T) {
			tr := xfertrack.NewLegacyTracker()
			old := xfertrack.NewLegacyEntry(xfertrack.LegacyRecord{TransferID: "reused", SID: "lab", Actor: "same-actor", Verb: "push", Tier: "b"})
			if code := putLegacyTransfer(tr, old); code != "" {
				t.Fatal(code)
			}
			tr.Remove(old)
			current := xfertrack.NewLegacyEntry(tr.Record(old))
			if code := putLegacyTransfer(tr, current); code != "" {
				t.Fatal(code)
			}
			claim := func(e *xfertrack.LegacyEntry) bool {
				switch op {
				case "finalize":
					_, ok := tr.ClaimFinalize(e)
					return ok
				case "commit":
					return tr.MarkCommitted(e)
				case "abandon":
					_, why := tr.ClaimAbandonedPush(e)
					return why == xfertrack.LegacyClaimed
				case "remove":
					tr.Remove(e)
					return tr.Get(tr.Record(e).TransferID) == nil
				default:
					t.Fatalf("unknown operation %q", op)
					return false
				}
			}
			if claim(old) || tr.Record(current).Finalized || tr.Record(current).Committed || tr.Get(tr.Record(current).TransferID) != current {
				t.Fatal("stale entry changed the replacement")
			}
			if !claim(current) {
				t.Fatal("the current entry must remain claimable")
			}
		})
	}
}
