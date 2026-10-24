package brokerupgrade

import (
	"bytes"
	"fmt"
	"runtime"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/LinZiyang666/tether/internal/proto"
	"github.com/LinZiyang666/tether/internal/testharness"
	"github.com/LinZiyang666/tether/internal/upgradebudget"
	"github.com/LinZiyang666/tether/internal/upgradetx"
	"github.com/LinZiyang666/tether/internal/upgradewire"
	"github.com/nats-io/nats.go"
)

// A query can reach the old forwarded subscriber after Start's early reply
// but before exec removes it. Losing that read must not spend the 30-second
// mutation-forward window while an original inline transfer has only 30 seconds.
// Real: admitted broker route and NATS request/reply. Simulated: a subscriber
// that loses the first query, then answers the identical read. The real binary
// continuation/victim suites independently exercise the actual same-PID cut.
// origin: S7 internal review CONTINUATION-VICTIM F1
func TestAgentQueryRecoversLostCutReplyWithinOriginalBudget(t *testing.T) {
	g, fd := runtime.NumGoroutine(), testharness.FDCount()
	for round := 0; round < 5; round++ {
		t.Run(fmt.Sprint(round), func(t *testing.T) { exerciseLostAgentQuery(t) })
	}
	testharness.AssertNoGoroutineLeak(t, "lost agent query", g)
	testharness.AssertNoFDLeak(t, "lost agent query", fd, 0)
}

func exerciseLostAgentQuery(t *testing.T) {
	t.Helper()
	f := newAgentRouteFixture(t)
	f.advertise(t, proto.CapUpgradeTXV1)
	url := testharness.StartNATS(t)
	broker, err := nats.Connect(url)
	if err != nil {
		t.Fatal(err)
	}
	defer broker.Close()
	agent, err := nats.Connect(url)
	if err != nil {
		t.Fatal(err)
	}
	defer agent.Close()
	body, err := upgradewire.EncodeRequest(upgradewire.Request{Op: upgradewire.OpQuery, Key: f.key,
		TxID: strings.Repeat("1", 32), QueryNonce: strings.Repeat("2", 32)})
	if err != nil {
		t.Fatal(err)
	}
	var sent atomic.Int32
	var first []byte
	_, err = agent.Subscribe(proto.SubjCmdForwarded("lab", "node", SignedTransactionOp), func(msg *nats.Msg) {
		if sent.Add(1) == 1 {
			first = bytes.Clone(msg.Data)
			return // The old process received this query and lost its reply to exec.
		}
		if !bytes.Equal(first, msg.Data) {
			t.Error("query recovery replaced the original read envelope")
		}
		if err := agent.Publish(msg.Reply, f.reply); err != nil {
			t.Error(err)
		}
	})
	if err != nil || agent.FlushTimeout(time.Second) != nil {
		t.Fatal("query subscriber not ready", err)
	}
	var forwarder AgentForwarder
	results := make(chan forwardResult, 1)
	started := time.Now()
	forwarder.Forward(true, "", "", f.forwardRoute(body, upgradebudget.RequestLatency,
		NATSRequest(broker)), collect(t, results), nil)
	result := <-results
	if result.response.Code != "" || result.response.View.Historical.Key != f.key || sent.Load() != 2 {
		t.Fatalf("lost cut query consumed its window instead of recovering the same read: code=%q sends=%d", result.response.Code, sent.Load())
	}
	if elapsed := result.at.Sub(started); elapsed >= upgradebudget.RequestLatency {
		t.Fatalf("recovery exceeded the original query budget: %s", elapsed)
	}
}

// Retrying an observation cannot renew its deadline or become permission to
// repeat an unsafe mutation. An authoritative refusal or malformed response
// must also remain visible instead of being hidden behind a later success.
// origin: S7 internal review CONTINUATION-VICTIM F1
func TestAgentQueryRecoveryKeepsDeadlineAndMutationBoundary(t *testing.T) {
	g, fd := runtime.NumGoroutine(), testharness.FDCount()
	for round := 0; round < 5; round++ {
		t.Run(fmt.Sprint(round), func(t *testing.T) { exerciseAgentQueryBoundaries(t) })
	}
	testharness.AssertNoGoroutineLeak(t, "agent query boundaries", g)
	testharness.AssertNoFDLeak(t, "agent query boundaries", fd, 0)
}

func exerciseAgentQueryBoundaries(t *testing.T) {
	t.Helper()
	for _, kind := range []string{"expired-query", "control-cap", "spent-admission", "mutation", "refused-query", "malformed-query", "unsubscribed-query"} {
		t.Run(kind, func(t *testing.T) {
			f := newAgentRouteFixture(t)
			f.advertise(t, proto.CapUpgradeTXV1)
			body, err := upgradewire.EncodeRequest(upgradewire.Request{Op: upgradewire.OpQuery, Key: f.key,
				TxID: strings.Repeat("1", 32), QueryNonce: strings.Repeat("2", 32)})
			if err != nil {
				t.Fatal(err)
			}
			want := upgradetx.CodeDeferred
			if kind == "mutation" {
				body, want = f.cancel(t), upgradetx.CodeUnsettled
			}
			var forwarder AgentForwarder
			results := make(chan forwardResult, 1)
			calls := 0
			const budget = 20 * time.Millisecond
			routeBudget := budget
			if kind == "control-cap" || kind == "spent-admission" {
				routeBudget = 2 * upgradebudget.RequestLatency
			}
			started := time.Now()
			route := f.forwardRoute(body, routeBudget, func(_ string, _ []byte, remaining time.Duration) ([]byte, error) {
				calls++
				if remaining <= 0 || remaining > min(routeBudget, upgradebudget.RequestLatency) {
					t.Errorf("query replaced the original remaining budget: %s", remaining)
				}
				if calls > 1 && kind != "control-cap" {
					return f.reply, nil // A forbidden replay would conceal the first result.
				}
				switch kind {
				case "expired-query", "control-cap":
					time.Sleep(remaining)
					return nil, nats.ErrTimeout
				case "mutation":
					return nil, nats.ErrTimeout
				case "spent-admission":
					return f.reply, nil
				case "refused-query":
					refused, err := upgradewire.EncodeResponse(upgradewire.Response{Code: upgradetx.CodeDeferred, Error: "target is not ready"})
					return refused, err
				case "malformed-query":
					return []byte("invalid transaction response"), nil
				case "unsubscribed-query":
					return nil, nats.ErrNoResponders
				default:
					panic("unknown query boundary")
				}
			})
			if kind == "spent-admission" {
				route.Received = time.Now().Add(-upgradebudget.RequestLatency)
			}
			forwarder.Forward(true, "", "", route, collect(t, results), nil)
			result := <-results
			if kind == "spent-admission" {
				if calls != 0 {
					t.Fatalf("time already spent in admission was renewed: calls=%d", calls)
				}
			} else if kind == "control-cap" {
				if elapsed := result.at.Sub(started); elapsed < upgradebudget.RequestLatency || elapsed >= upgradebudget.RequestLatency+time.Second {
					t.Fatalf("query inherited or renewed the mutation budget: %s", elapsed)
				}
			} else if calls != 1 {
				t.Fatalf("%s replayed after the first result: calls=%d", kind, calls)
			}
			if result.response.Code != want {
				t.Fatalf("%s renewed or concealed the first result: code=%q calls=%d", kind, result.response.Code, calls)
			}
		})
	}
}
