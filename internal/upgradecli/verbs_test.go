package upgradecli

import (
	"bytes"
	"context"
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/LinZiyang666/tether/internal/upgradetx"
	"github.com/LinZiyang666/tether/internal/upgradetx/contractstub"
)

// verbEndpoint answers Query from a mutable view and records mutations. Every
// method it does not script stays P00's fail-closed stub.
type verbEndpoint struct {
	contractstub.AgentEndpoint
	view            upgradetx.View
	queries         []string
	rollbackFence   upgradetx.Fence
	rollbackReverse upgradetx.ReverseRequest
	rollbackReply   upgradetx.View
	cancelled       []upgradetx.Handle
}

func (e *verbEndpoint) Query(_ context.Context, key upgradetx.Key, txID, nonce string) (upgradetx.View, error) {
	e.queries = append(e.queries, txID)
	v := e.view
	if txID != v.Historical.Grant.Handle.TxID && e.rollbackReply.Historical.Grant.Handle.TxID == txID {
		v = e.rollbackReply
	}
	v.Observation.Nonce, v.Observation.At = nonce, time.Now().UnixNano()
	return v, nil
}

func (e *verbEndpoint) Rollback(_ context.Context, fence upgradetx.Fence, reverse upgradetx.ReverseRequest) (upgradetx.View, error) {
	e.rollbackFence, e.rollbackReverse = fence, reverse
	return e.rollbackReply, nil
}

func (e *verbEndpoint) Cancel(_ context.Context, h upgradetx.Handle) (upgradetx.View, error) {
	e.cancelled = append(e.cancelled, h)
	return e.view, nil
}

func verbRequest(t *testing.T, e *verbEndpoint, verb string) (VerbRequest, *bytes.Buffer) {
	t.Helper()
	home := t.TempDir()
	if err := RememberGrant(home, e.view.Historical.Grant); err != nil {
		t.Fatal(err)
	}
	var out bytes.Buffer
	return VerbRequest{Verb: verb, Home: home, TxID: e.view.Historical.Grant.Handle.TxID, ActorFP: "SHA256:actor",
		Bind: nodeBinding("lab", "status"), Endpoint: e, Timeout: 200 * time.Millisecond, JSON: true, Out: &out}, &out
}

// A status query of an accepted transaction whose current owner became
// unknown is a successful read (rc0) that renders the unknown observation;
// wait on the same state is upgrade_unsettled (74). The NID literally named
// "status" is addressed like any other node.
// origin: hot-update S8 plan §7.1 / §11.2 P19 (query unknown=0, wait unknown=74)
func TestTransactionVerbsSplitUnknownQueryFromWait(t *testing.T) {
	view := reverseView()
	view.Observation.OwnerKnown = false
	view.Observation.Current.Outcome = upgradetx.OutcomeUnknown
	view.Code = upgradetx.CodeUnsettled
	e := &verbEndpoint{view: view}
	ctx := context.Background() // ctx-root: synchronous CLI policy test.
	status, out := verbRequest(t, e, VerbStatus)
	if err := RunVerb(ctx, status); err != nil || ExitCode(err) != 0 {
		t.Fatalf("unknown query must succeed with rc0: %v", err)
	}
	var rendered upgradetx.View
	if err := json.Unmarshal(out.Bytes(), &rendered); err != nil || rendered.Observation.OwnerKnown || rendered.Code != upgradetx.CodeUnsettled || len(rendered.Observation.Nonce) != 32 {
		t.Fatalf("status did not render the unknown observation verbatim: %v %s", err, out.String())
	}
	wait, _ := verbRequest(t, e, VerbWait)
	if err := RunVerb(ctx, wait); ExitCode(err) != 74 || !strings.Contains(err.Error(), upgradetx.CodeUnsettled) {
		t.Fatalf("unknown wait must be bounded upgrade_unsettled rc74: %v", err)
	}
	other, _ := verbRequest(t, e, VerbStatus)
	other.Bind = nodeBinding("lab", "another-node")
	if err := RunVerb(ctx, other); ExitCode(err) != 64 {
		t.Fatalf("a grant for node status was queried as another node: %v", err)
	}
}

// A manual rollback fences the fresh current attempt, records the new reverse
// transaction before waiting, and succeeds only when the reverse goal is
// proven; it never reuses the forward transaction's identity (M07/M12).
// origin: hot-update S8 plan §4.1 / §8.1 / §11.2 P19
func TestRollbackVerbRecordsTheNewReverseTransaction(t *testing.T) {
	view := reverseView()
	reverseGrant := view.Historical.Grant
	reverseGrant.Handle.TxID, reverseGrant.Handle.CutID, reverseGrant.Handle.Generation = strings.Repeat("7", 32), strings.Repeat("8", 32), 3
	reverseAttempt := upgradetx.Attempt{Handle: reverseGrant.Handle, Direction: upgradetx.DirectionReverse, Phase: upgradetx.PhaseActivating, Outcome: upgradetx.OutcomePending}
	reply := upgradetx.View{Historical: upgradetx.Record{Key: reverseGrant.Handle.Key, Revision: reverseGrant.Handle.Revision, Grant: reverseGrant, Reverse: reverseAttempt},
		Observation: upgradetx.Observation{OwnerKnown: true, Current: reverseAttempt}}
	e := &verbEndpoint{view: view, rollbackReply: reply}
	ctx := context.Background() // ctx-root: synchronous CLI policy test.
	r, _ := verbRequest(t, e, VerbRollback)
	err := RunVerb(ctx, r)
	if ExitCode(err) != 74 {
		t.Fatalf("an unfinished reverse must not report its goal reached: %v", err)
	}
	if e.rollbackFence.Current != view.Observation.Current.Handle || e.rollbackReverse.HistoricalTxID != view.Historical.Grant.Handle.TxID ||
		e.rollbackReverse.TargetSHA != view.Historical.RecordedPreviousSHA {
		t.Fatalf("rollback was not fenced on the fresh current attempt: %+v %+v", e.rollbackFence, e.rollbackReverse)
	}
	if got, err := RecallGrant(r.Home, reverseGrant.Handle.TxID); err != nil || got != reverseGrant {
		t.Fatalf("the new reverse transaction was not recorded for later queries: %v", err)
	}
	if e.queries[len(e.queries)-1] != reverseGrant.Handle.TxID {
		t.Fatalf("rollback waited on the forward ID instead of the reverse: %v", e.queries)
	}
}

// Cancel/archive act on the CAS handle of a fresh query, never a remembered one.
func TestCancelVerbUsesTheFreshCurrentHandle(t *testing.T) {
	view := reverseView()
	e := &verbEndpoint{view: view}
	ctx := context.Background() // ctx-root: synchronous CLI policy test.
	r, _ := verbRequest(t, e, VerbCancel)
	if err := RunVerb(ctx, r); err != nil || len(e.cancelled) != 1 || e.cancelled[0] != view.Observation.Current.Handle {
		t.Fatalf("cancel did not use the fresh current handle: %v %+v", err, e.cancelled)
	}
	stale := reverseView()
	stale.Observation.Current.Handle.Revision++
	e = &verbEndpoint{view: stale}
	r, _ = verbRequest(t, e, VerbCancel)
	if err := RunVerb(ctx, r); ExitCode(err) != 74 || len(e.cancelled) != 0 {
		t.Fatalf("cancel acted on a handle that disagrees with the record: %v", err)
	}
}
