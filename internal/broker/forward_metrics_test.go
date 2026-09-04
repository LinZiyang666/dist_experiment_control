package broker

import (
	"encoding/json"
	"errors"
	"go/ast"
	"go/parser"
	"go/printer"
	"go/token"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/LinZiyang666/tether/internal/authcallout"
	"github.com/LinZiyang666/tether/internal/brokermetrics"
	"github.com/LinZiyang666/tether/internal/cluster"
	"github.com/LinZiyang666/tether/internal/proto"
	"github.com/LinZiyang666/tether/internal/schema"
	"github.com/nats-io/nats.go"
)

// readSourceFile reads a file from THIS package's directory. The test binary's working directory
// is the package directory, so a bare filename is correct.
func readSourceFile(name string) (string, error) {
	b, err := os.ReadFile(name)
	return string(b), err
}

// forward_metrics_test.go (batch B, B2) — the forward-outcome counter.
//
// WHY THIS COUNTER EXISTS
//
// Two production forward paths discard their error on purpose:
// alert_forward.go's disk-pressure signal and topology_reconcile.go's bus-nkey self-report both
// write `_ = fwd.Forward(...)`, because both are level-triggered and a dropped message self-heals
// on the next tick. That retry policy is right. Its observability is not: a broker whose forwards
// fail on EVERY tick is indistinguishable, from the outside, from a broker with nothing to say.
//
// The counter changes nothing about the retry — it only makes the difference visible.

func TestCountForwardClassifiesTheThreeOutcomes(t *testing.T) {
	b := &Broker{}
	b.countForward("sessioncreate", nil)
	b.countForward("sessioncreate", cluster.ErrForwardNotLeader)
	b.countForward("sessioncreate", errors.New("some typed business rejection"))
	b.countForward("noderegister", nil)

	got := b.forwards.snapshot()
	want := map[string]int64{
		"sessioncreate/ok":         1,
		"sessioncreate/not_leader": 1,
		"sessioncreate/error":      1,
		"noderegister/ok":          1,
	}
	if len(got) != len(want) {
		t.Fatalf("snapshot has %d key(s), want %d: %v", len(got), len(want), got)
	}
	for k, v := range want {
		if got[k] != v {
			t.Errorf("%s = %d, want %d (full: %v)", k, got[k], v, got)
		}
	}
}

// TestCountForwardKeepsNotLeaderSeparateFromError is the assertion that makes the counter useful
// rather than decorative. not_leader is the routine leadership race every caller retries; error is
// a rejection retrying will not fix. An operator reading a rising rate needs to know which, and
// folding them together is the easy "simplification" that removes the whole signal.
func TestCountForwardKeepsNotLeaderSeparateFromError(t *testing.T) {
	b := &Broker{}
	// The sentinel, and a WRAPPED sentinel — proposeOrForward's leader branch returns the bare
	// one while the forward branch can return it wrapped, and both must classify the same.
	b.countForward("portfreealloc", cluster.ErrForwardNotLeader)
	b.countForward("portfreealloc", errors.Join(errors.New("forward:"), cluster.ErrForwardNotLeader))
	b.countForward("portfreealloc", errors.New("unrelated"))

	got := b.forwards.snapshot()
	if got["portfreealloc/not_leader"] != 2 {
		t.Errorf("wrapped ErrForwardNotLeader is not classified as not_leader: %v", got)
	}
	if got["portfreealloc/error"] != 1 {
		t.Errorf("a non-leadership error must be `error`, not `not_leader`: %v", got)
	}
	if got["portfreealloc/not_leader"] == got["portfreealloc/error"]+2 && len(got) == 1 {
		t.Error("the two outcomes collapsed into one key — the counter can no longer tell election " +
			"churn from a real rejection, which is the only reason it exists")
	}
}

// TestForwardCountersZeroValueIsUsable pins the constraint that kept this out of every existing
// test fixture: the ~126 package-internal &Broker{} literals never call New() or Run(), so a
// counter needing construction would panic in most of this package's tests.
func TestForwardCountersZeroValueIsUsable(t *testing.T) {
	var b Broker // deliberately not &Broker{...}: the barest possible value
	if got := b.forwards.snapshot(); got != nil {
		t.Errorf("a never-incremented counter must snapshot to nil (so the metric series is "+
			"OMITTED in single mode rather than emitted as an empty set), got %v", got)
	}
	b.countForward("provision", nil)
	if got := b.forwards.snapshot(); got["provision/ok"] != 1 {
		t.Fatalf("the zero value is not usable: %v", got)
	}
}

// TestForwardOutcomesRenderStably pins two things the exposition format needs: the series is a
// COUNTER (so rate() applies) and the label lines are sorted (so the output is byte-stable across
// scrapes and a golden test is possible at all).
func TestForwardOutcomesRenderStably(t *testing.T) {
	var sb strings.Builder
	brokermetrics.Render(&sb, brokermetrics.Snapshot{
		ClusterMode: true,
		ForwardOutcomes: map[string]int64{
			"sessioncreate/ok":        7,
			"noderegister/not_leader": 2,
			"portfreealloc/error":     1,
			"alertsignal/not_leader":  9,
		},
	})
	out := sb.String()

	if !strings.Contains(out, "# TYPE tether_broker_raft_forward_total counter") {
		t.Error("the series must be TYPE counter — a scraper will not apply rate()/increase() to a gauge")
	}
	for _, want := range []string{
		`tether_broker_raft_forward_total{verb="alertsignal",outcome="not_leader"} 9`,
		`tether_broker_raft_forward_total{verb="noderegister",outcome="not_leader"} 2`,
		`tether_broker_raft_forward_total{verb="portfreealloc",outcome="error"} 1`,
		`tether_broker_raft_forward_total{verb="sessioncreate",outcome="ok"} 7`,
	} {
		if !strings.Contains(out, want) {
			t.Errorf("missing series line:\n  %s\ngot:\n%s", want, out)
		}
	}
	// Sorted: alertsignal < noderegister < portfreealloc < sessioncreate.
	idx := func(s string) int { return strings.Index(out, s) }
	if idx("alertsignal") >= idx("noderegister") ||
		idx("noderegister") >= idx("portfreealloc") ||
		idx("portfreealloc") >= idx("sessioncreate") {
		t.Error("label lines are not sorted — map iteration order leaked into the exposition, so " +
			"the output differs between scrapes and no golden test can pin it")
	}

	// And the series must be ABSENT (not emitted empty) when nothing was forwarded: an empty
	// labelled counter is a series a scraper would chart as a flat zero, implying "we tried and
	// none failed", which is false in single mode where nothing is attempted at all.
	var single strings.Builder
	brokermetrics.Render(&single, brokermetrics.Snapshot{})
	if strings.Contains(single.String(), "tether_broker_raft_forward_total") {
		t.Error("the forward series is emitted in single mode, where no forward is ever attempted")
	}
}

// TestEverySilentForwardSenderIsCounted replaces a source-text tripwire that this counter's
// external review proved was false assurance.
//
// The old test grep'd for the literal `_ = fwd.Forward(VerbAlertSignal` and passed as long as that
// string was present. It never observed a counter — and the counter was in fact never incremented
// for that path, because counting lived inside proposeOrForward and the alert sink calls
// Forward directly. testing-standards §S1: a structural check does not substitute for the
// behavioural one, and here it actively concealed its absence.
//
// Every sender below discards or swallows its forward error by design (a level-triggered re-assert
// or a client retry self-heals). That design is exactly why the outcome must be observable, and it
// is why each case drives the REAL sender rather than calling Forward itself: a test that called
// Forward directly would pass even if the sender were rewired to something uncounted.
func TestEverySilentForwardSenderIsCounted(t *testing.T) {
	// No apply responder is subscribed, so every forward times out into the retriable
	// not_leader sentinel — the outcome an operator most needs to see repeating.
	cases := []struct {
		name string
		verb string
		// drive runs the production sender against a broker whose forwarder is fwd.
		drive func(t *testing.T, b *Broker, fwd *Forwarder, nc *nats.Conn)
	}{
		{
			name: "disk-pressure alert signal (discards its error; re-asserts every tick)",
			verb: VerbAlertSignal,
			drive: func(t *testing.T, b *Broker, fwd *Forwarder, nc *nats.Conn) {
				b.AttachAlertSink(fwd)
				b.alertSink(true)
			},
		},
		{
			name: "alert ack (error goes to the ctl reply, never to the counter before this fix)",
			verb: VerbAlertAck,
			drive: func(t *testing.T, b *Broker, fwd *Forwarder, nc *nats.Conn) {
				sub, err := SubscribeAlertAck(nc, fwd, nil)
				if err != nil {
					t.Fatal(err)
				}
				t.Cleanup(func() { _ = sub.Unsubscribe() })
				body, _ := json.Marshal(proto.AlertAckReq{DedupKey: "disk_pressure:brk-a"})
				// The responder replies "error: …"; we only care that the attempt was tallied.
				_, _ = nc.Request(proto.SubjCtrlAlertAck("UACTOR"), body, 3*time.Second)
			},
		},
		{
			name: "transfer-audit record (async, retried, then dropped)",
			verb: VerbTransferAudit,
			drive: func(t *testing.T, b *Broker, fwd *Forwarder, nc *nats.Conn) {
				b.AttachTransferAuditSink(fwd)
				b.emitTransferAudit(schema.AuditTransfer{
					V: 1, Kind: "transfer", Verb: "start", Ts: time.Now(),
					Session: "lab", Node: "lab-1", TransferID: "xfer-1",
				})
				b.WaitTransferAudit()
			},
		},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			nc, err := nats.Connect(startNATS(t))
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(nc.Close)

			b := &Broker{selfID: "counted-node", cfg: Config{Logger: silentLogger(), Now: time.Now}}
			// A short timeout keeps each case fast; b.newForwarder is the PRODUCTION constructor,
			// so this also exercises the observer wiring rather than a test-only substitute.
			fwd := b.newForwarder(nc, 150*time.Millisecond)
			c.drive(t, b, fwd, nc)

			got := b.forwards.snapshot()
			key := c.verb + "/not_leader"
			if got[key] == 0 {
				t.Fatalf("%s produced no %q observation: %v.\n"+
					"This sender bypasses proposeOrForward, so it is counted only if the outcome is "+
					"recorded at the Forwarder boundary. Without it, an operator watching a broker "+
					"whose forwards fail on EVERY tick sees no series at all — the exact "+
					"indistinguishability tether_broker_raft_forward_total exists to remove.",
					c.verb, key, got)
			}
			// And it must be the retriable class, not collapsed into `error`: they drive opposite
			// operator responses (wait for an election vs. go read the logs).
			if got[c.verb+"/error"] != 0 {
				t.Errorf("a timed-out forward was classified as a permanent error: %v", got)
			}
		})
	}
}

// TestLeaderLocalProvisionAndJoinAttemptsAreCounted covers the two direct Propose branches that
// bypass both Forward's network boundary and Broker.proposeOrForward. The network-only sender tests
// above stayed green while these leader-local attempts produced no denominator at all.
func TestLeaderLocalProvisionAndJoinAttemptsAreCounted(t *testing.T) {
	n, _ := d7SingleNode(t, "forward-metric-leader")
	b := &Broker{}
	fwd := &Forwarder{observe: b.countForward}

	// Missing sessions make both Plans deterministically reject. The business outcome is not this
	// test's concern; a rejected authoritative attempt must still contribute exactly one `error`.
	// The seams now REQUIRE a budgeted verifier (external review M-1) and refuse without
	// one, so this metrics test supplies a real handler's. The Plans still reject on the
	// missing session before Argon2 runs, which is what keeps the attempt counted.
	verifyPIN := (&authcallout.Handler{Now: time.Now}).VerifyPINWithBudget
	_ = NewProvisionSeam(n, fwd, verifyPIN)(
		"missing-provision-session", "node-1", "SHA256:fp", "bad-pin", time.Now(),
	)
	_ = NewJoinSeam(n, fwd, verifyPIN)(
		"missing-join-session", "SHA256:fp", "bad-pin", time.Now(),
	)

	got := b.forwards.snapshot()
	for _, verb := range []string{VerbProvision, VerbJoin} {
		total := got[verb+"/ok"] + got[verb+"/not_leader"] + got[verb+"/error"]
		if total != 1 {
			t.Errorf("leader-local %s recorded %d outcomes, want exactly 1: %v", verb, total, got)
		}
	}
}

// TestEveryBrokerOwnedForwarderIsObserved is the structural half — and unlike the test it replaces,
// it does not stand in for a behavioural check, it covers what one cannot see: a NEW production
// forwarder built with the bare constructor would be silently uncounted, and no existing sender's
// test would notice, because each sender only exercises the forwarder it was handed.
func TestEveryBrokerOwnedForwarderIsObserved(t *testing.T) {
	fset := token.NewFileSet()
	var offenders []string
	for _, file := range []string{"broker.go", "clusterwrite.go"} {
		f, err := parser.ParseFile(fset, file, nil, 0)
		if err != nil {
			t.Fatal(err)
		}
		ast.Inspect(f, func(n ast.Node) bool {
			as, ok := n.(*ast.AssignStmt)
			if !ok {
				return true
			}
			assignsForwarder := false
			for _, lhs := range as.Lhs {
				if src := exprString(fset, lhs); strings.HasSuffix(src, "forwarder") || src == "fwd" {
					assignsForwarder = true
				}
			}
			if !assignsForwarder {
				return true
			}
			for _, rhs := range as.Rhs {
				src := exprString(fset, rhs)
				if strings.Contains(src, "NewForwarder") && !strings.Contains(src, "b.newForwarder") {
					offenders = append(offenders, fset.Position(as.Pos()).String()+": "+src)
				}
			}
			return true
		})
	}
	if len(offenders) > 0 {
		t.Errorf("production builds a Forwarder with the bare constructor: %v.\n"+
			"Use b.newForwarder, which installs the outcome observer. A bare Forwarder counts "+
			"nothing, and every sender that receives it goes dark in the metric without any test "+
			"failing.", offenders)
	}

	// Non-vacuity: the walk must actually find the assignments it is filtering.
	found := 0
	for _, file := range []string{"broker.go", "clusterwrite.go"} {
		src, err := readSourceFile(file)
		if err != nil {
			t.Fatal(err)
		}
		found += strings.Count(src, "b.newForwarder(")
	}
	if found < 2 {
		t.Fatalf("found only %d b.newForwarder call(s) in the two wiring files; the scan is not "+
			"looking at the production construction sites", found)
	}
}

func exprString(fset *token.FileSet, e ast.Expr) string {
	var sb strings.Builder
	if err := printer.Fprint(&sb, fset, e); err != nil {
		return ""
	}
	return sb.String()
}
