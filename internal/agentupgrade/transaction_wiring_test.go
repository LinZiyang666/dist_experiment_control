package agentupgrade

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"runtime"
	"slices"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/LinZiyang666/tether/internal/agentupgradeport"
	"github.com/LinZiyang666/tether/internal/brokerupgrade"
	"github.com/LinZiyang666/tether/internal/proto"
	"github.com/LinZiyang666/tether/internal/testharness"
	"github.com/LinZiyang666/tether/internal/upgradecut"
	"github.com/LinZiyang666/tether/internal/upgradeport"
	"github.com/LinZiyang666/tether/internal/upgradetx"
	"github.com/LinZiyang666/tether/internal/upgradetx/contractstub"
	"github.com/nats-io/nats.go"
)

// origin: hot-update S8 plan §6.1 / §7.2; S8 FIX-DESCRIBE. The broker forwards
// upgrade-tx only to an agent whose latest register advertised it, so the
// token follows the bound endpoint exactly; proxy-v1 is always declared.
func TestRegisterAdvertisesTransactionsOnlyWithABoundEndpoint(t *testing.T) {
	if got := agentupgradeport.RegisterCapabilities(nil); !slices.Equal(got, []string{proto.CapProxyV1}) {
		t.Fatalf("unbound agent advertised %v", got)
	}
	var none upgradeport.Endpoint
	if got := agentupgradeport.RegisterCapabilities(none); slices.Contains(got, proto.CapUpgradeTXV1) {
		t.Fatal("a nil endpoint advertised transactions")
	}
	bound := New(contractstub.Store{}, contractstub.Runtime{}, contractstub.Registry{})
	if got := agentupgradeport.RegisterCapabilities(bound); !slices.Equal(got, []string{proto.CapProxyV1, proto.CapUpgradeTXV1}) {
		t.Fatalf("bound agent advertised %v", got)
	}
}

// activatingRuntime reports a process image about to be replaced.
type activatingRuntime struct {
	contractstub.Runtime
	activating bool
}

func (r activatingRuntime) Activating() bool { return r.activating }

// origin: hot-update S8 plan §3.1 / §7.1; S8 FIX-DESCRIBE (real-cut finding).
// A request reaching an agent between a Start's early reply and its exec used
// to wait behind that Start and be lost to the exec, while the broker held it
// (and everything queued behind it) for its 30 s forward timeout. ReplyWire
// answers it at once with a deferral that accepted nothing, without handing it
// to the engine; an agent that is not activating serves it normally.
func TestReplyWireRefusesRequestsWhileActivating(t *testing.T) {
	nc, err := nats.Connect(testharness.StartNATS(t))
	if err != nil {
		t.Fatal(err)
	}
	defer nc.Close()
	for _, activating := range []bool{true, false} {
		handled := false
		ports := &Dependencies{Runtime: activatingRuntime{activating: activating},
			HandleWire: func(context.Context, upgradeport.Endpoint, upgradetx.Key, []byte) ([]byte, error) {
				handled = true
				return []byte(`{"code":"ok"}`), nil
			}}
		inbox := nats.NewInbox()
		sub, err := nc.SubscribeSync(inbox)
		if err != nil {
			t.Fatal(err)
		}
		ReplyWire(t.Context(), ports, New(contractstub.Store{}, contractstub.Runtime{}, contractstub.Registry{}), nc, &nats.Msg{Reply: inbox, Data: []byte(`{}`)})
		msg, err := sub.NextMsg(2 * time.Second)
		_ = sub.Unsubscribe()
		if err != nil {
			t.Fatalf("activating=%v: no reply: %v", activating, err)
		}
		var reply struct{ Code, Error string }
		_ = json.Unmarshal(msg.Data, &reply)
		if activating && (handled || reply.Code != upgradetx.CodeDeferred || !strings.HasPrefix(reply.Error, "target_activating")) {
			t.Fatalf("a request during activation reached the engine or was not refused at once: handled=%v reply=%s", handled, msg.Data)
		}
		if !activating && (!handled || string(msg.Data) != `{"code":"ok"}`) {
			t.Fatalf("an idle agent refused a request: handled=%v reply=%s", handled, msg.Data)
		}
	}
}

type describingRuntime struct {
	contractstub.Runtime
	answer func(nonce string) (upgradeport.Description, error)
}

func (r describingRuntime) DescribeRuntime(nonce string) (upgradeport.Description, error) {
	return r.answer(nonce)
}

// origin: hot-update S8 plan §2.1 / §3.1; S8 FIX-DESCRIBE (P19 request R1).
// The engine relays only the runtime's own description of exactly the
// selected component and nonce; an undescribable runtime is not wired; a
// typed runtime refusal keeps its public reason; and a describe waits behind
// a running operation instead of interleaving it (cancellable while waiting).
func TestDescribeServesOnlyTheSelectedComponent(t *testing.T) {
	key := upgradetx.Key{DomainID: strings.Repeat("d", 32), Component: upgradetx.ComponentAgent, SID: "lab", ConfiguredNID: "node", UID: 1000}
	selector := upgradetx.Key{Component: upgradetx.ComponentAgent, SID: "lab", ConfiguredNID: "node"}
	nonce := strings.Repeat("e", 32)
	honest := func(n string) (upgradeport.Description, error) {
		return upgradeport.Description{Key: key, Nonce: n}, nil
	}
	served := New(contractstub.Store{}, describingRuntime{answer: honest}, contractstub.Registry{}).(*engine)
	if d, err := served.Describe(t.Context(), selector, nonce); err != nil || d.Key != key || d.Nonce != nonce {
		t.Fatalf("selected component not described: %+v %v", d, err)
	}
	other := selector
	other.ConfiguredNID = "other"
	// Every mismatch refuses with the identity-free public reason, which the
	// engine's closed-vocabulary projection must keep (S8 FIX-DESCRIBE).
	unavailable := func(err error) bool {
		var refusal *upgradetx.Error
		return errors.As(err, &refusal) && refusal.Code == upgradetx.CodeDeferred && refusal.Detail == "target_description_unavailable"
	}
	if _, err := served.Describe(t.Context(), other, nonce); !unavailable(err) {
		t.Fatalf("another node's selector was answered: %v", err)
	}
	for name, answer := range map[string]func(string) (upgradeport.Description, error){
		"stale_nonce": func(string) (upgradeport.Description, error) {
			return upgradeport.Description{Key: key, Nonce: strings.Repeat("0", 32)}, nil
		},
		"broker_key": func(n string) (upgradeport.Description, error) {
			return upgradeport.Description{Key: upgradetx.Key{Component: upgradetx.ComponentBroker}, Nonce: n}, nil
		},
		"no_domain_key": func(n string) (upgradeport.Description, error) {
			return upgradeport.Description{Key: selector, Nonce: n}, nil
		},
	} {
		e := New(contractstub.Store{}, describingRuntime{answer: answer}, contractstub.Registry{}).(*engine)
		if _, err := e.Describe(t.Context(), selector, nonce); !unavailable(err) {
			t.Fatalf("%s: description relayed: %v", name, err)
		}
	}
	census := New(contractstub.Store{}, describingRuntime{answer: func(string) (upgradeport.Description, error) {
		return upgradeport.Description{}, &upgradetx.Error{Code: upgradetx.CodeDeferred, Detail: "domain_census_unavailable"}
	}}, contractstub.Registry{}).(*engine)
	var typed *upgradetx.Error
	if _, err := census.Describe(t.Context(), selector, nonce); !errors.As(err, &typed) || typed.Code != upgradetx.CodeDeferred || typed.Detail != "domain_census_unavailable" {
		t.Fatalf("runtime reason lost: %v", err)
	}
	plain := New(contractstub.Store{}, contractstub.Runtime{}, contractstub.Registry{}).(*engine)
	if _, err := plain.Describe(t.Context(), selector, nonce); !errors.As(err, &typed) || typed.Code != upgradetx.CodeNotWired {
		t.Fatalf("undescribable runtime answered: %v", err)
	}
	served.gate <- struct{}{} // another operation holds the sequencer
	ctx, cancel := context.WithTimeout(t.Context(), 50*time.Millisecond)
	defer cancel()
	if _, err := served.Describe(ctx, selector, nonce); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("describe interleaved a running operation: %v", err)
	}
	served.leave()
}

// origin: hot-update S8 plan §4.2 / M05; S8 FIX-DESCRIBE. With no injected
// probe the production control proof is the agent's own heartbeat with a
// nonce, relayed by the broker as a claim probe through the real forwarded
// subscriber (brokerupgrade.WithControlProbe). A wrong instance, a missing
// relay or a missing heartbeat source is unproven. Readiness requires that
// proof only when a carried cut must be completed: a registered agent without
// a cut stays ready under a broker that cannot relay (N-1).
func TestControlProbeTraversesTheBrokerRelay(t *testing.T) {
	g, fd := runtime.NumGoroutine(), testharness.FDCount()
	for round := 0; round < 5; round++ {
		t.Run(fmt.Sprintf("round-%d", round), func(t *testing.T) { exerciseControlProbeRelay(t) })
	}
	testharness.AssertNoGoroutineLeak(t, "agent control probe relay", g)
	testharness.AssertNoFDLeak(t, "agent control probe relay", fd, 0)
}

func exerciseControlProbeRelay(t *testing.T) {
	url := testharness.StartNATS(t)
	agentNC, err := nats.Connect(url)
	if err != nil {
		t.Fatal(err)
	}
	defer agentNC.Close()
	var current atomic.Pointer[nats.Conn]
	var generation atomic.Uint64
	current.Store(agentNC)
	generation.Store(3)
	instance := "instance-under-test"
	var answering atomic.Value
	answering.Store(instance)
	claim, err := agentNC.Subscribe(proto.SubjCmdForwarded("lab", "node", proto.ClaimProbeVerb), func(m *nats.Msg) {
		var query ControlProof
		_ = json.Unmarshal(m.Data, &query)
		body, _ := json.Marshal(ControlProof{InstanceID: answering.Load().(string), Nonce: query.Nonce, Generation: generation.Load()})
		_ = m.Respond(body)
	})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = claim.Unsubscribe() }()
	port := ControlPort{InstanceID: instance, SID: "lab", NID: "node", Current: &current, Generation: &generation, Dependencies: &Dependencies{},
		Heartbeat: func() []byte {
			body, _ := json.Marshal(proto.HeartbeatPayload{Ts: time.Now(), ProxyBound: true})
			return body
		}}
	// No relay subscribed: an older broker cannot prove control.
	if err := ProbeControl(t.Context(), port, agentNC, Nonce()); err == nil {
		t.Fatal("control proven without the broker relay")
	}
	var ready atomic.Pointer[nats.Conn]
	if err := FinishControlReady(t.Context(), ReadyPort{Control: port, Ready: &ready}, agentNC); err != nil || ready.Load() != agentNC {
		t.Fatalf("registered agent without a cut lost readiness under an older broker: %v", err)
	}
	carried := port
	carried.Dependencies = &Dependencies{Cut: &upgradecut.State{}}
	ready.Store(nil)
	if err := FinishControlReady(t.Context(), ReadyPort{Control: carried, Ready: &ready}, agentNC); err == nil || ready.Load() != nil {
		t.Fatal("a carried cut became ready without an external control proof")
	}
	brokerNC, err := nats.Connect(url)
	if err != nil {
		t.Fatal(err)
	}
	defer brokerNC.Close()
	var proxyBound atomic.Bool
	relay, err := brokerNC.Subscribe(proto.SubjectPrefix+".ctrl.s.*.node.*.heartbeat", brokerupgrade.WithControlProbe(brokerNC, func(m *nats.Msg) {
		var hb proto.HeartbeatPayload
		_ = json.Unmarshal(m.Data, &hb)
		proxyBound.Store(hb.ProxyBound)
	}))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = relay.Unsubscribe() }()
	if err := brokerNC.Flush(); err != nil {
		t.Fatal(err)
	}
	if err := ProbeControl(t.Context(), port, agentNC, Nonce()); err != nil || !proxyBound.Load() {
		t.Fatalf("relayed control probe failed or reset liveness facts: err=%v proxy_bound=%v", err, proxyBound.Load())
	}
	// With control proven, an unprovable completion (here: no engine can Boot
	// the cut) stays journaled for recovery while the session keeps serving;
	// it never ends the session (that exited the agent, rc 70).
	ready.Store(nil)
	if err := FinishControlReady(t.Context(), ReadyPort{Control: carried, Ready: &ready}, agentNC); err != nil || ready.Load() != agentNC {
		t.Fatalf("an unprovable completion ended a proven control-ready session: %v", err)
	}
	answering.Store("another-instance")
	if err := ProbeControl(t.Context(), port, agentNC, Nonce()); err == nil {
		t.Fatal("another instance's answer proved this agent's control")
	}
	answering.Store(instance)
	noSource := port
	noSource.Heartbeat = nil
	var typed *upgradetx.Error
	if err := ProbeControl(t.Context(), noSource, agentNC, Nonce()); !errors.As(err, &typed) || typed.Code != upgradetx.CodeNotWired {
		t.Fatalf("probe without a heartbeat source was not refused: %v", err)
	}
}
