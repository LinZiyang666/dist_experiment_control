package execclient

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"runtime"
	"testing"
	"time"

	"github.com/LinZiyang666/tether/internal/controlbudget"
	"github.com/LinZiyang666/tether/internal/proto"
	"github.com/LinZiyang666/tether/internal/testharness"
	"github.com/nats-io/nats.go"
)

// A real server 503 during a modern blackout must not become permission to
// execute a legacy task, even if a later probe is delivered but not answered.
// The existing old-broker and old-agent binary tests keep both fallback doors.
// origin: hot-update S6 plan §8.2 / T31 / FIX-BLACKOUT
func TestAdmissionNoRespondersRequiresLiveLegacyPeer(t *testing.T) {
	beforeG, beforeFD := runtime.NumGoroutine(), testharness.FDCount()
	for round := 0; round < 5; round++ {
		t.Run(fmt.Sprint(round), func(t *testing.T) { exerciseProbeNoResponderBlackout(t) })
	}
	testharness.AssertNoGoroutineLeak(t, "probe no-responder blackout", beforeG)
	testharness.AssertNoFDLeak(t, "probe no-responder blackout", beforeFD, 0)
}

func exerciseProbeNoResponderBlackout(t *testing.T) {
	t.Helper()
	for _, mode := range []string{"absent-control", "503-then-silence", "live-control-then-silence"} {
		t.Run(mode, func(t *testing.T) {
			f := newClientFixture(t)
			discovered := make(chan struct{}, 1)
			modernInstalled := make(chan struct{})
			if mode == "live-control-then-silence" {
				body, err := json.Marshal(proto.NodeListResp{Nodes: []proto.NodeListEntry{{NID: "node", Status: "ONLINE", ReleaseVersion: "v0.7.0"}}})
				if err != nil {
					t.Fatal(err)
				}
				if _, err := f.responder.Subscribe(proto.SubjCtrlNodeList(f.actor, "lab"), func(msg *nats.Msg) {
					select {
					case discovered <- struct{}{}:
					default:
					}
					// The discovery response must follow the restored subscription.
					// Otherwise a fast second 503 legitimately proves the old-broker
					// shape before the test goroutine installs its silent modern peer.
					// origin: hot-update S6 plan §8.2 / T31 / TESTFIX
					select {
					case <-modernInstalled:
						_ = msg.Respond(body)
					case <-t.Context().Done():
					}
				}); err != nil {
					t.Fatal(err)
				}
			}
			if err := f.sub.Unsubscribe(); err != nil {
				t.Fatal(err)
			}
			if err := f.responder.FlushTimeout(time.Second); err != nil {
				t.Fatal(err)
			}
			cfg := admissionConfig(t, 5*time.Second)
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			done := make(chan error, 1)
			go func() {
				_, err := ProbeWithin(ctx, f.nc, f.subject, proto.ProcessResumeReq{Version: 1, Op: "probe", ClientLineage: "blackout"}, cfg)
				done <- err
			}()
			joined := false
			defer func() {
				if !joined {
					cancel()
					<-done
				}
			}()
			began := time.Now()
			// Observe the completed server 503 before changing the subscription; a
			// sleep alone would not establish which transport refusal the client saw.
			until := time.NewTimer(time.Second)
			defer until.Stop()
			observed := false
			for !observed {
				for _, a := range cfg.Observer.(*admissionObserver).snapshot() {
					observed = observed || a.Phase == controlbudget.PhaseProbe && a.Class == "no_responders"
				}
				if !observed {
					select {
					case <-until.C:
						t.Fatal("missing real server 503")
					case <-time.After(time.Millisecond):
					}
				}
			}
			restore := func() {
				sub, err := f.responder.SubscribeSync(proto.SubjCmdBy("lab", f.actor, "node", "*"))
				if err != nil {
					t.Fatal(err)
				}
				f.sub = sub
				if err := f.responder.FlushTimeout(time.Second); err != nil {
					t.Fatal(err)
				}
			}
			if mode == "503-then-silence" {
				restore()
			}
			faultLength := 2100 * time.Millisecond
			if mode == "live-control-then-silence" {
				select {
				case <-discovered:
				case err := <-done:
					joined = true
					t.Fatalf("absence authorized fallback before a live peer read: %v", err)
				case <-time.After(3 * time.Second):
					t.Fatal("missing authenticated node discovery")
				}
				restore()
				close(modernInstalled)
				// One fresh 503 and a live read cannot survive a subsequent
				// timeout once the modern subscription has returned.
				faultLength = 3100 * time.Millisecond
			}
			// sleep-fixture: the measured control fault lasts longer than the old
			// two-second downgrade threshold, within the unchanged five-second budget.
			fault := time.NewTimer(time.Until(began.Add(faultLength)))
			defer fault.Stop()
			select {
			case err := <-done:
				joined = true
				t.Fatalf("modern absence ended negotiation before recovery: %v", err)
			case <-fault.C:
			}
			if mode == "absent-control" {
				restore()
			}
			probe := f.next(t, "probe")
			capability := proto.ProcessResumeReply{Version: 1, CallID: probe.req.CallID, Kind: "capability", TargetInstance: "original", Lease: &proto.ProcessLease{Version: 1, TargetInstance: "original", SID: "lab", NID: "node", ActorFP: f.fp, ClientLineage: "blackout"}}
			sendClientReply(t, probe, capability)
			err := <-done
			joined = true
			if err != nil {
				t.Fatalf("recovered modern probe: %v", err)
			}
			if time.Since(began) > 5*time.Second {
				t.Fatal("probe renewed admission budget")
			}
			// No create or legacy message was sent by this capability-only API.
			for {
				msg, err := f.sub.NextMsg(20 * time.Millisecond)
				if errors.Is(err, nats.ErrTimeout) {
					break
				}
				if err != nil {
					t.Fatal(err)
				}
				var req proto.ProcessResumeReq
				if json.Unmarshal(msg.Data, &req) != nil || req.Op != "probe" || req.CallID != probe.req.CallID {
					t.Fatal("negotiation changed operation or correlation")
				}
			}
		})
	}
}

// origin: hot-update S6 plan §8.2 / T31 / FIX-BLACKOUT
func TestAdmissionLegacyProofCannotOutliveBudget(t *testing.T) {
	beforeG, beforeFD := runtime.NumGoroutine(), testharness.FDCount()
	for round := 0; round < 5; round++ {
		t.Run(fmt.Sprint(round), func(t *testing.T) { exerciseLegacyProofDeadline(t) })
	}
	testharness.AssertNoGoroutineLeak(t, "legacy proof deadline", beforeG)
	testharness.AssertNoFDLeak(t, "legacy proof deadline", beforeFD, 0)
}

type legacyProofBarrier struct {
	controlbudget.Observer
	entered, release chan struct{}
}

func (o *legacyProofBarrier) Record(a controlbudget.Attempt) error {
	close(o.entered)
	<-o.release
	return o.Observer.Record(a)
}

func exerciseLegacyProofDeadline(t *testing.T) {
	t.Helper()
	for _, phase := range []string{"before-publish", "after-reply", "final-503"} {
		t.Run(phase, func(t *testing.T) {
			f := newClientFixture(t)
			requests := make(chan struct{}, 4)
			version := "v0.6.0"
			if phase == "final-503" {
				version = "v0.7.0"
			}
			body, err := json.Marshal(proto.NodeListResp{Nodes: []proto.NodeListEntry{{NID: "node", Status: "ONLINE", ReleaseVersion: version}}})
			if err != nil {
				t.Fatal(err)
			}
			if _, err := f.responder.Subscribe(proto.SubjCtrlNodeList(f.actor, "lab"), func(msg *nats.Msg) {
				requests <- struct{}{}
				_ = msg.Respond(body)
			}); err != nil {
				t.Fatal(err)
			}
			if err := f.responder.FlushTimeout(time.Second); err != nil {
				t.Fatal(err)
			}
			cfg := admissionConfig(t, 150*time.Millisecond)
			if phase == "final-503" {
				if err := f.sub.Unsubscribe(); err != nil {
					t.Fatal(err)
				}
				if err := f.responder.FlushTimeout(time.Second); err != nil {
					t.Fatal(err)
				}
				// The discovery at two seconds succeeds, but pacing the next
				// actual server 503 consumes the rest of this same deadline.
				cfg = admissionConfig(t, 2400*time.Millisecond)
				_, err := ProbeWithin(t.Context(), f.nc, f.subject, proto.ProcessResumeReq{Version: 1, Op: "probe", ClientLineage: "late-503"}, cfg)
				if !controlbudget.IsExpired(err) || errors.Is(err, ErrUnsupported) {
					t.Fatalf("expired 503 authorized fallback: %v", err)
				}
				proved := false
				for _, a := range cfg.Observer.(*admissionObserver).snapshot() {
					proved = proved || a.Phase == controlbudget.PhaseDiscovery && a.Class == "success"
				}
				if !proved {
					t.Fatal("late-503 control did not establish live peer evidence")
				}
				return
			}
			if phase == "before-publish" {
				cfg.Observer.(*admissionObserver).beginDelay = 200 * time.Millisecond
				old, err := legacyNodeProof(t.Context(), &transport{nc: f.nc, subject: f.subject}, cfg)
				if old || !controlbudget.IsExpired(err) {
					t.Fatalf("observer delay renewed legacy budget: old=%t err=%v", old, err)
				}
				if err := f.nc.FlushTimeout(time.Second); err != nil {
					t.Fatal(err)
				}
				if len(requests) != 0 {
					t.Fatal("legacy discovery was published after admission expired")
				}
				return
			}
			barrier := &legacyProofBarrier{Observer: cfg.Observer, entered: make(chan struct{}), release: make(chan struct{})}
			cfg.Observer = barrier
			done := make(chan error, 1)
			go func() {
				old, err := legacyNodeProof(t.Context(), &transport{nc: f.nc, subject: f.subject}, cfg)
				if old || !controlbudget.IsExpired(err) {
					err = fmt.Errorf("late legacy reply authorized fallback: old=%t err=%v", old, err)
				} else {
					err = nil
				}
				done <- err
			}()
			released := false
			defer func() {
				if !released {
					close(barrier.release)
					<-done
				}
			}()
			select {
			case <-barrier.entered:
			case <-time.After(time.Second):
				t.Fatal("legacy response did not reach the observer barrier")
			}
			if len(requests) != 1 {
				t.Fatal("deadline test did not receive the real old-peer response")
			}
			// sleep-fixture: hold a real received response past its original deadline.
			if left, err := controlbudget.Remaining(cfg.Deadline); err == nil {
				time.Sleep(left + time.Millisecond)
			}
			close(barrier.release)
			released = true
			if err := <-done; err != nil {
				t.Fatal(err)
			}
		})
	}
}
