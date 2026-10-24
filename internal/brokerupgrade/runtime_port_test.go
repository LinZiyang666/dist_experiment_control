package brokerupgrade

import (
	"encoding/json"
	"errors"
	"fmt"
	"runtime"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/LinZiyang666/tether/internal/proto"
	"github.com/LinZiyang666/tether/internal/testharness"
	"github.com/LinZiyang666/tether/internal/upgradeartifact"
	"github.com/LinZiyang666/tether/internal/upgradeport"
	"github.com/LinZiyang666/tether/internal/upgradetx"
	"github.com/nats-io/nats.go"
)

func refusedWith(err error, code, detail string, accepted bool) bool {
	var typed *upgradetx.Error
	return errors.As(err, &typed) && typed.Code == code && typed.Detail == detail && typed.Accepted == accepted
}

// origin: S8 FIX-DESCRIBE (P19 request R4). Binding a registered broker's
// journal must not let it accept a cut it cannot carry out: until the broker
// exporter exists every cut step refuses before acceptance (75, nothing
// journaled), retirement is never certified, describe states the missing
// census instead of a partial scope, and observation is a truthful unknown
// census, never an empty broker a query could read as complete. The
// production adapter binds through Bind/GateRuntime with describe visible.
func TestBrokerRuntimeRefusesEveryCutStepBeforeAcceptance(t *testing.T) {
	f := newEngineFixture()
	owner := f.artifact.Grant.Issuer
	running := f.artifact.Grant.SourceSHA
	port := NewRuntime(RuntimeHost{Key: f.artifact.Grant.Handle.Key, Journal: f.store,
		Owner: func() (upgradetx.Owner, error) { return owner, nil }, RunningSHA: func() (string, error) { return running, nil }})
	if _, err := port.Preflight(t.Context(), f.artifact.Grant, f.artifact); !refusedWith(err, upgradetx.CodeDeferred, ReasonBrokerCutUnavailable, false) {
		t.Fatalf("preflight admitted an unexportable cut: %v", err)
	}
	if _, err := port.Freeze(f.artifact.Grant.Handle); !refusedWith(err, upgradetx.CodeDeferred, ReasonBrokerCutUnavailable, false) {
		t.Fatalf("freeze claimed custody: %v", err)
	}
	if _, err := port.ExportCurrent(f.artifact.Grant.Handle); !refusedWith(err, upgradetx.CodeDeferred, ReasonBrokerCutUnavailable, false) {
		t.Fatalf("export claimed custody: %v", err)
	}
	if _, err := port.Activate(upgradeport.Cut{Attempt: f.artifact.Grant.Handle}); !refusedWith(err, upgradetx.CodeDeferred, ReasonBrokerCutUnavailable, false) {
		t.Fatalf("activate claimed an effect: %v", err)
	}
	if _, err := port.Retire(f.artifact.Grant.Handle); !refusedWith(err, upgradetx.CodeUnsettled, "old generation release is unproven", true) {
		t.Fatalf("retirement certified without an imported cut: %v", err)
	}
	if _, err := port.(upgradeport.RuntimeDescriber).DescribeRuntime(strings.Repeat("e", 32)); !refusedWith(err, upgradetx.CodeDeferred, ReasonDomainCensus, false) {
		t.Fatalf("broker described a scope it cannot enumerate: %v", err)
	}
	stamp := upgradetx.Stamp{Attempt: f.artifact.Grant.Handle, Nonce: strings.Repeat("c", 32)}
	o, err := port.Observe(stamp)
	if err != nil || !o.OwnerKnown || o.RunningSHA != running || o.Nonce != stamp.Nonce || o.Census.Known || o.Census.Complete ||
		o.Census.Stamp.Producer != owner || o.Census.Stamp.Nonce != stamp.Nonce || o.At <= 0 {
		t.Fatalf("observation is not a truthful unknown census: %+v err=%v", o, err)
	}
	broken := NewRuntime(RuntimeHost{Key: f.artifact.Grant.Handle.Key, Journal: f.store})
	if _, err := broken.Observe(stamp); err == nil {
		t.Fatal("an adapter without owner/running facts observed")
	}
	// Start through the production binding: refused before acceptance.
	endpoint, key, err := Bind(&Dependencies{Key: f.artifact.Grant.Handle.Key, Store: f.store, Runtime: port, Registry: UnwiredRegistry()}, func() error { return nil })
	if err != nil || key != f.artifact.Grant.Handle.Key {
		t.Fatalf("registered broker could not bind: %v", err)
	}
	if _, err := endpoint.Start(t.Context(), f.artifact.Grant, upgradeartifact.Receipt{Grant: f.artifact.Grant, SourceSHA: f.artifact.Grant.SourceSHA, TargetSHA: f.artifact.Grant.TargetSHA}); !refusedWith(err, upgradetx.CodeDeferred, ReasonBrokerCutUnavailable, false) || f.store.starts != 0 {
		t.Fatalf("a broker cut was accepted: starts=%d err=%v", f.store.starts, err)
	}
	describer, ok := endpoint.(upgradeport.Describer)
	if !ok {
		t.Fatal("the broker endpoint cannot describe")
	}
	if _, err := describer.Describe(t.Context(), upgradetx.Key{Component: upgradetx.ComponentBroker}, strings.Repeat("e", 32)); !refusedWith(err, upgradetx.CodeDeferred, ReasonDomainCensus, false) {
		t.Fatalf("describe lost the runtime's reason behind the role gate: %v", err)
	}
	if _, err := describer.Describe(t.Context(), upgradetx.Key{Component: upgradetx.ComponentAgent, SID: "s", ConfiguredNID: "n"}, strings.Repeat("e", 32)); err == nil {
		t.Fatal("a broker described itself for an agent selector")
	}
	registry := UnwiredRegistry()
	if _, err := registry.Begin(upgradetx.Resource{}); err == nil {
		t.Fatal("an unwired registry admitted a resource")
	}
	if _, err := registry.Seal(upgradetx.Attempt{}, strings.Repeat("e", 32)); err == nil {
		t.Fatal("an unwired registry sealed a receipt")
	}
}

// origin: hot-update S8 plan §4.2 / M05; S8 FIX-DESCRIBE. A nonce-bound
// heartbeat request is relayed as a claim probe to exactly the node its
// subject names, and the node's own answer is the reply. The liveness handler
// always runs first and unchanged; a plain heartbeat, a malformed nonce or a
// heartbeat without a reply inbox never probes anything.
func TestControlProbeRelayAnswersOnlyTheRequestingAgent(t *testing.T) {
	g, fd := runtime.NumGoroutine(), testharness.FDCount()
	for round := 0; round < 5; round++ {
		t.Run(fmt.Sprintf("round-%d", round), func(t *testing.T) { exerciseControlProbeRelay(t) })
	}
	testharness.AssertNoGoroutineLeak(t, "control probe relay", g)
	testharness.AssertNoFDLeak(t, "control probe relay", fd, 0)
}

func exerciseControlProbeRelay(t *testing.T) {
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
	var heartbeats, probesA, probesB atomic.Int64
	sub, err := broker.Subscribe(proto.SubjectPrefix+".ctrl.s.*.node.*.heartbeat", WithControlProbe(broker, func(*nats.Msg) { heartbeats.Add(1) }))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = sub.Unsubscribe() }()
	answer := func(count *atomic.Int64, instance string) nats.MsgHandler {
		return func(m *nats.Msg) {
			count.Add(1)
			var query struct {
				Nonce string `json:"query_nonce"`
			}
			_ = json.Unmarshal(m.Data, &query)
			body, _ := json.Marshal(map[string]any{"instance_id": instance, "query_nonce": query.Nonce, "session_generation": 7})
			_ = m.Respond(body)
		}
	}
	for _, s := range []struct {
		nid      string
		count    *atomic.Int64
		instance string
	}{{"node-a", &probesA, "instance-a"}, {"node-b", &probesB, "instance-b"}} {
		probe, err := agent.Subscribe(proto.SubjCmdForwarded("lab", s.nid, proto.ClaimProbeVerb), answer(s.count, s.instance))
		if err != nil {
			t.Fatal(err)
		}
		defer func() { _ = probe.Unsubscribe() }()
	}
	if err := errors.Join(broker.Flush(), agent.Flush()); err != nil {
		t.Fatal(err)
	}
	nonce := strings.Repeat("a", 32)
	body, _ := json.Marshal(proto.HeartbeatPayload{Ts: time.Now(), ProxyBound: true, ControlProbeNonce: nonce})
	reply, err := agent.Request(proto.SubjNodeHeartbeat("lab", "node-a"), body, 5*time.Second)
	if err != nil {
		t.Fatalf("control probe was not relayed: %v", err)
	}
	var proof struct {
		Instance   string `json:"instance_id"`
		Nonce      string `json:"query_nonce"`
		Generation uint64 `json:"session_generation"`
	}
	if json.Unmarshal(reply.Data, &proof) != nil || proof.Instance != "instance-a" || proof.Nonce != nonce || proof.Generation != 7 {
		t.Fatalf("relay answered with another node's or a stale proof: %s", reply.Data)
	}
	if heartbeats.Load() != 1 || probesA.Load() != 1 || probesB.Load() != 0 {
		t.Fatalf("relay probed the wrong node or skipped liveness: heartbeats=%d a=%d b=%d", heartbeats.Load(), probesA.Load(), probesB.Load())
	}
	plain, _ := json.Marshal(proto.HeartbeatPayload{Ts: time.Now()})
	if err := agent.Publish(proto.SubjNodeHeartbeat("lab", "node-a"), plain); err != nil {
		t.Fatal(err)
	}
	if err := agent.Publish(proto.SubjNodeHeartbeat("lab", "node-a"), body); err != nil { // nonce but no reply inbox
		t.Fatal(err)
	}
	malformed, _ := json.Marshal(proto.HeartbeatPayload{Ts: time.Now(), ControlProbeNonce: "not-a-nonce"})
	if _, err := agent.Request(proto.SubjNodeHeartbeat("lab", "node-a"), malformed, 300*time.Millisecond); err == nil {
		t.Fatal("a malformed nonce was relayed")
	}
	if !testharness.WaitFor(t, 5*time.Second, 5*time.Millisecond, func() bool { return heartbeats.Load() == 4 }) {
		t.Fatalf("liveness handler skipped a heartbeat: %d", heartbeats.Load())
	}
	if err := errors.Join(broker.Flush(), agent.Flush()); err != nil {
		t.Fatal(err)
	}
	if probesA.Load() != 1 || probesB.Load() != 0 {
		t.Fatalf("a heartbeat without a valid nonce and inbox probed an agent: a=%d b=%d", probesA.Load(), probesB.Load())
	}
}
