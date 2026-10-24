// Real client settlement loop; the sink models
// one landed rename with a transient parent Sync error, then healthy storage.
package xferclient

import (
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/LinZiyang666/tether/internal/controlbudget"
	"github.com/LinZiyang666/tether/internal/handoff"
	x "github.com/LinZiyang666/tether/internal/xfercontract"
	"github.com/nats-io/nats.go/jetstream"
)

type recoverySink struct {
	attempts, effects int
	out               x.ReceiverOutcome
}

func (*recoverySink) WriteAt(p []byte, _ int64) (int, error) { return len(p), nil }
func (*recoverySink) Sync() error                            { return nil }
func (s *recoverySink) Commit(g x.CommitGrant, _ x.Deadline) x.ReceiverOutcome {
	s.attempts++
	if s.effects == 0 {
		s.effects++
		s.out = x.ReceiverOutcome{Grant: g, Outcome: x.OutcomeLanded, Primitive: x.PrimitiveRename, Bytes: 3, Code: x.CodeDurabilityUnknown}
	} else {
		s.out.ParentSynced = true
		s.out.Code = ""
	}
	return s.out
}
func (s *recoverySink) Freeze(x.Deadline) (x.AtomicState, []handoff.Resource, error) {
	return x.AtomicState{DurableOffset: 3, PrimitiveOutcome: s.out}, nil, nil
}
func (s *recoverySink) Stop(d x.Deadline) error {
	if !s.out.ParentSynced {
		s.Commit(s.out.Grant, d)
	}
	return nil
}

type recoveryTransport struct{ sent []x.ReceiverOutcome }

func (r *recoveryTransport) Request(q x.Request, _ x.Deadline) (x.Response, x.Attempt, error) {
	if q.Outcome == nil {
		return x.Response{}, x.Attempt{}, errors.New("test expected original outcome")
	}
	r.sent = append(r.sent, *q.Outcome)
	if !q.Outcome.ParentSynced {
		return x.Response{Code: x.CodeDurabilityUnknown}, x.Attempt{}, nil
	}
	return x.Response{Receipt: &x.TerminalReceipt{TerminalID: q.Outcome.Grant.TerminalID, Kind: "complete", CanonicalDigest: strings.Repeat("b", 64), ReqID: strings.Repeat("c", 64), HistoryStream: "history-session", HistorySeq: 2, Bytes: 3, SHA256: strings.Repeat("a", 64)}}, x.Attempt{}, nil
}
func (*recoveryTransport) OpenData(x.Identity, x.Deadline) (jetstream.JetStream, error) {
	return nil, errors.New("unexpected data IO")
}
func (*recoveryTransport) CloseData(x.Identity, x.Deadline) error { return nil }
func recoveryDeadline(t *testing.T) x.Deadline {
	t.Helper()
	d, e := controlbudget.After(time.Second)
	if e != nil {
		t.Fatal(e)
	}
	return d
}

// origin: S7 internal review rv-recovery F04
func TestRecoveryPullRetriesDirectorySyncBeforeTerminal(t *testing.T) {
	id := x.Identity{SID: "session", NID: "node", ActorFP: "actor", AgentInstance: "agent", TransferID: "transfer", EntryNonce: "entry", IntentDigest: strings.Repeat("d", 64)}
	g := x.CommitGrant{Identity: id, GrantNonce: "grant", ReceiverInstance: "receiver", TerminalID: terminalSlot(id)}
	sink := &recoverySink{}
	transport := &recoveryTransport{}
	c := &Client{cfg: Config{Base: x.Base{Clock: func() (string, int64, error) { n, e := controlbudget.Now(); return "boot", n, e }, UTC: time.Now}, Intent: x.Intent{Verb: "pull", Tier: "a", Size: 3, SHA256: strings.Repeat("a", 64)}, Destination: sink, Transport: transport}, identity: id, grant: &g, bootID: "boot", byteStop: make(chan struct{}), spec: &x.ObjectSpec{Identity: id, Size: 3, SHA256: strings.Repeat("a", 64)}, checkpoint: x.SinkCheckpoint{DurableOffset: 3}}
	c.deadline = recoveryDeadline(t)
	first := sink.Commit(g, c.deadline)
	first.SHA256 = c.spec.SHA256
	c.outcome = &first
	for n := 0; n < 3; n++ {
		_ = c.pull(recoveryDeadline(t))
	}
	if c.receipt == nil || !c.outcome.ParentSynced || sink.effects != 1 {
		t.Fatalf("landed outcome never retried parent Sync: settlement_attempts=%d sink_attempts=%d rename_effects=%d parent_synced=%v receipt=%v", len(transport.sent), sink.attempts, sink.effects, c.outcome.ParentSynced, c.receipt)
	}
}

type alteredSettlementSink struct {
	recoverySink
	alter func(*x.ReceiverOutcome)
}

func (s *alteredSettlementSink) Commit(g x.CommitGrant, d x.Deadline) x.ReceiverOutcome {
	next := s.recoverySink.Commit(g, d)
	s.alter(&next)
	return next
}

// Settlement may advance directory/unlink evidence, but cannot change the
// already landed grant, primitive, bytes or digest, or resurrect cleanup.
// origin: S7 internal review rv-recovery F04
func TestTransferPullSettlementRejectsChangedLandedEvidence(t *testing.T) {
	for _, defect := range []string{"grant", "missing_grant", "primitive", "outcome", "bytes", "sha", "cleanup", "parent"} {
		t.Run(defect, func(t *testing.T) {
			id := x.Identity{SID: "session", NID: "node", ActorFP: "actor", AgentInstance: "agent", TransferID: "transfer", EntryNonce: "entry", IntentDigest: strings.Repeat("d", 64)}
			grant := x.CommitGrant{Identity: id, GrantNonce: "grant", ReceiverInstance: "receiver", TerminalID: terminalSlot(id)}
			prior := x.ReceiverOutcome{Grant: grant, Outcome: x.OutcomeLanded, Primitive: x.PrimitiveRename, Bytes: 3, SHA256: strings.Repeat("a", 64), Code: x.CodeDurabilityUnknown}
			if defect == "parent" {
				prior.ParentSynced, prior.TmpCleanupPending = true, true
			}
			sink := &alteredSettlementSink{recoverySink: recoverySink{effects: 1, out: prior}, alter: func(next *x.ReceiverOutcome) {
				switch defect {
				case "grant":
					next.Grant.GrantNonce = "other"
				case "primitive":
					next.Primitive = x.PrimitiveLink
				case "outcome":
					next.Outcome = x.OutcomeNotEntered
				case "bytes":
					next.Bytes++
				case "sha":
					next.SHA256 = strings.Repeat("b", 64)
				case "cleanup":
					next.TmpCleanupPending = true
				case "parent":
					next.ParentSynced = false
				}
			}}
			transport := &recoveryTransport{}
			c := &Client{cfg: Config{Base: x.Base{Clock: func() (string, int64, error) { n, err := controlbudget.Now(); return "boot", n, err }, UTC: time.Now}, Destination: sink, Transport: transport}, identity: id, grant: &grant, bootID: "boot", outcome: &prior}
			if defect == "missing_grant" {
				c.grant = nil
			}
			if err := c.settle(recoveryDeadline(t)); ErrorCode(err) != x.CodeTerminalConflict {
				t.Fatalf("changed landed evidence was accepted: %v", err)
			}
			if len(transport.sent) != 0 || c.receipt != nil || *c.outcome != prior {
				t.Fatal("conflicting settlement replaced retained evidence or submitted a terminal")
			}
		})
	}
}
