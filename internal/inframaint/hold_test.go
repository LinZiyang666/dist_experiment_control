package inframaint

import (
	"errors"
	"math"
	"runtime"
	"sync"
	"testing"
	"time"

	"github.com/LinZiyang666/tether/internal/infracontract"
	"github.com/LinZiyang666/tether/internal/testharness"
	"github.com/LinZiyang666/tether/internal/upgradebudget"
)

func testView(f *routeFixture, a infracontract.Attachment) View {
	return View{Attachment: a, HomeDigest: f.state.HomeDigest, AuthorityRevision: f.state.AuthorityRevision, PolicyRevision: f.state.PolicyRevision, Known: true, Healthy: true, Registered: true}
}

func installedHold(t *testing.T, f *routeFixture, p infracontract.RouteProof) *Hold {
	t.Helper()
	h, err := NewHold(Challenge{Domain: f.plan.Domain, Tx: f.plan.Tx, Participant: p.Participant, Direction: p.Direction, Nonce: p.Nonce, BootID: "observer-boot", StartedNS: f.start})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := h.Install(p, "route-key", f.pub, testView(f, p.Old), "observer-boot", f.start, f.clock.Load()); err != nil {
		t.Fatal(err)
	}
	return h
}

// origin: hot-update S9 plan §4.6 / T37 / M18
func TestInfraRouteHoldExpiresWithoutRenewal(t *testing.T) {
	t.Run("producer_retries_keep_absolute_expiry", func(t *testing.T) {
		f := newRouteFixture(t)
		p := f.prepare(t)
		if p.RemainingNS != uint64(upgradebudget.BrokerMaintenanceTTL) {
			t.Fatal("initial grant differs from SSOT")
		}
		f.clock.Store(f.start + int64(3*time.Second))
		retry := f.prepare(t)
		if retry.Nonce != p.Nonce || retry.RemainingNS != uint64(upgradebudget.BrokerMaintenanceTTL-3*time.Second) {
			t.Fatal("Prepare retry renewed deadline")
		}
		f.clock.Store(f.start + int64(5*time.Second))
		m := f.observeNew()
		bound, err := f.routes.Bind(f.plan, m, infracontract.RouteOutbound, f.deadline)
		if err != nil {
			t.Fatal(err)
		}
		if bound[0].RemainingNS != uint64(upgradebudget.BrokerMaintenanceTTL-5*time.Second) {
			t.Fatal("Bind renewed deadline")
		}
		f.clock.Store(f.start + int64(upgradebudget.BrokerMaintenanceTTL))
		if _, err := f.routes.Bind(f.plan, m, infracontract.RouteOutbound, f.deadline); !errors.Is(err, ErrExpired) {
			t.Fatal("expired Bind signed")
		}
	})
	t.Run("observer_RTT_and_new_generation_cannot_renew", func(t *testing.T) {
		f := newRouteFixture(t)
		p := f.prepare(t)
		f.clock.Store(f.start + int64(time.Second))
		h := installedHold(t, f, p)
		v := testView(f, p.Old)
		if !h.Protects(v, "observer-boot", f.start+int64(7*time.Second)) {
			t.Fatal("valid old owner unprotected")
		}
		pending := Pending{Participant: p.Participant, Generation: f.new.Generation, Target: p.Target}
		if err := h.BeginDial(pending, v, "observer-boot", f.clock.Load()); err != nil {
			t.Fatal(err)
		}
		f.clock.Store(f.start + int64(5*time.Second))
		m := f.observeNew()
		bound, err := f.routes.Bind(f.plan, m, infracontract.RouteOutbound, f.deadline)
		if err != nil {
			t.Fatal(err)
		}
		v.Attachment = f.new
		remaining, err := h.Install(bound[0], "route-key", f.pub, v, "observer-boot", f.clock.Load(), f.clock.Load())
		if err != nil || remaining != uint64(3*time.Second) {
			t.Fatalf("valid five-second Bind: remaining=%d err=%v", remaining, err)
		}
		if h.Protects(testView(f, p.Old), "observer-boot", f.clock.Load()) || !h.Protects(v, "observer-boot", f.clock.Load()) {
			t.Fatal("Bind did not replace exact protected owner")
		}
		// Even a correctly signed later full-TTL reply cannot extend t0+8s.
		renew := bound[0]
		renew.RemainingNS = uint64(upgradebudget.BrokerMaintenanceTTL)
		renew = f.sign(t, renew)
		if _, err := h.Install(renew, "route-key", f.pub, v, "observer-boot", f.start+int64(7*time.Second), f.start+int64(7*time.Second)); err != nil {
			t.Fatal(err)
		}
		if h.Protects(v, "observer-boot", f.start+int64(upgradebudget.BrokerMaintenanceTTL)) || h.Protects(v, "observer-boot", f.start+int64(60*time.Second)) {
			t.Fatal("expired bridge still shields ordinary reaper")
		}
		if _, err := h.Install(renew, "route-key", f.pub, v, "observer-boot", f.start+int64(60*time.Second), f.start+int64(60*time.Second)); err == nil {
			t.Fatal("expired bridge resurrected")
		}
	})
	t.Run("late_initial_reply_and_clock_failure", func(t *testing.T) {
		f := newRouteFixture(t)
		p := f.prepare(t)
		h, err := NewHold(Challenge{Domain: f.plan.Domain, Tx: f.plan.Tx, Participant: p.Participant, Direction: p.Direction, Nonce: p.Nonce, BootID: "observer-boot", StartedNS: f.start})
		if err != nil {
			t.Fatal(err)
		}
		if _, err := h.Install(p, "route-key", f.pub, testView(f, p.Old), "observer-boot", f.start, f.start+int64(upgradebudget.BrokerMaintenanceTTL)); !errors.Is(err, ErrExpired) {
			t.Fatal("late first reply restarted budget")
		}
		f = newRouteFixture(t)
		f.source.onRead = func() { f.clock.Add(int64(upgradebudget.BrokerMaintenanceTTL)) }
		if _, err := f.routes.Prepare(f.plan, f.checkpoint, infracontract.RouteOutbound, f.deadline); !errors.Is(err, ErrExpired) {
			t.Fatal("slow observation got fresh TTL")
		}
		f = newRouteFixture(t)
		f.routes.cfg.Sign = func([]byte) ([]byte, error) {
			f.clock.Add(int64(upgradebudget.BrokerMaintenanceTTL))
			return make([]byte, 64), nil
		}
		if _, err := f.routes.Prepare(f.plan, f.checkpoint, infracontract.RouteOutbound, f.deadline); !errors.Is(err, ErrExpired) {
			t.Fatal("slow signer returned expired proof")
		}
		f = newRouteFixture(t)
		f.clock.Store(math.MaxInt64)
		if _, err := f.routes.Prepare(f.plan, f.checkpoint, infracontract.RouteOutbound, f.deadline); !errors.Is(err, ErrInvalid) {
			t.Fatal("clock overflow accepted")
		}
	})
	t.Run("authority_and_unknown_source_end_protection", func(t *testing.T) {
		for _, tc := range []struct {
			name string
			edit func(*View)
		}{
			{"home", func(v *View) { v.HomeDigest[0]++ }}, {"authority", func(v *View) { v.AuthorityRevision++ }}, {"policy", func(v *View) { v.PolicyRevision++ }}, {"revoked", func(v *View) { v.Revoked = true }}, {"unknown", func(v *View) { v.Known = false }},
		} {
			t.Run(tc.name, func(t *testing.T) {
				f := newRouteFixture(t)
				p := f.prepare(t)
				h := installedHold(t, f, p)
				v := testView(f, p.Old)
				tc.edit(&v)
				if h.Protects(v, "observer-boot", f.start+1) {
					t.Fatal("changed authority protected")
				}
				if h.Protects(testView(f, p.Old), "observer-boot", f.start+2) {
					t.Fatal("old snapshot resurrected hold")
				}
			})
		}
	})
	t.Run("pending_is_one_real_generation", func(t *testing.T) {
		f := newRouteFixture(t)
		p := f.prepare(t)
		h := installedHold(t, f, p)
		v := testView(f, p.Old)
		pending := Pending{Participant: p.Participant, Generation: f.new.Generation, Target: p.Target}
		if h.ProtectsPending(pending, v, "observer-boot", f.start) {
			t.Fatal("unstarted dial protected")
		}
		if err := h.BeginDial(pending, v, "observer-boot", f.start); err != nil {
			t.Fatal(err)
		}
		if !h.ProtectsPending(pending, v, "observer-boot", f.start+1) {
			t.Fatal("authorized real dial unprotected")
		}
		for _, edit := range []func(*Pending){func(p *Pending) { p.Generation++ }, func(p *Pending) { p.Participant[0]++ }, func(p *Pending) { p.Target.URL = "wss://other.example" }, func(p *Pending) { p.Target.Ingress.PID++ }} {
			bad := pending
			edit(&bad)
			if h.ProtectsPending(bad, v, "observer-boot", f.start+1) || h.BeginDial(bad, v, "observer-boot", f.start+1) == nil {
				t.Fatal("another worker/target inherited pending permission")
			}
		}
		if h.Protects(testView(f, f.new), "observer-boot", f.start+1) {
			t.Fatal("pending intent signed completed attachment")
		}
		if h.ProtectsPending(pending, v, "observer-boot", f.start+int64(upgradebudget.BrokerMaintenanceTTL)) {
			t.Fatal("late dial retained maintenance")
		}
	})
	t.Run("bind_needs_prepare_and_exact_observation", func(t *testing.T) {
		f := newRouteFixture(t)
		p := f.prepare(t)
		m := f.observeNew()
		proofs, err := f.routes.Bind(f.plan, m, infracontract.RouteOutbound, f.deadline)
		if err != nil {
			t.Fatal(err)
		}
		bound := proofs[0]
		for _, edit := range []func(*View){func(v *View) { v.Attachment.Raw.Cookie++ }, func(v *View) { v.Attachment.HealthGeneration++ }, func(v *View) { v.Attachment.Ingress.ExeInode++ }, func(v *View) { v.HomeDigest[0]++ }, func(v *View) { v.Registered = false }, func(v *View) { v.Healthy = false }} {
			h := installedHold(t, f, p)
			v := testView(f, p.Old)
			pending := Pending{Participant: p.Participant, Generation: bound.New.Generation, Target: p.Target}
			if err := h.BeginDial(pending, v, "observer-boot", f.start); err != nil {
				t.Fatal(err)
			}
			v.Attachment = f.new
			edit(&v)
			if _, err := h.Install(bound, "route-key", f.pub, v, "observer-boot", f.start, f.start+1); err == nil {
				t.Fatal("Bind ignored raw/health/home observation")
			}
		}
		h := installedHold(t, f, p)
		if _, err := h.Install(bound, "route-key", f.pub, testView(f, f.new), "observer-boot", f.start, f.start+1); err == nil {
			t.Fatal("Bind without actual dial accepted")
		}
	})
	t.Run("deadline_clamps_and_stable_return", func(t *testing.T) {
		f := newRouteFixture(t)
		f.prepare(t)
		f.clock.Store(f.start + int64(time.Second))
		short := infracontract.Deadline(f.start + int64(4*time.Second))
		proofs, err := f.routes.Prepare(f.plan, f.checkpoint, infracontract.RouteOutbound, short)
		if err != nil || proofs[0].RemainingNS != uint64(3*time.Second) {
			t.Fatal("shorter deadline ignored")
		}
		f.clock.Store(f.start + int64(4*time.Second))
		if _, err := f.routes.Prepare(f.plan, f.checkpoint, infracontract.RouteOutbound, f.deadline); !errors.Is(err, ErrExpired) {
			t.Fatal("retry enlarged shortened deadline")
		}
		f = newRouteFixture(t)
		p := f.prepare(t)
		m := f.observeNew()
		if _, err := f.routes.Bind(f.plan, m, infracontract.RouteOutbound, f.deadline); err != nil {
			t.Fatal(err)
		}
		f.source.states[0].Target = infracontract.RouteTarget{URL: p.Old.URL, NodeID: "broker-a", Component: infracontract.Caddy, Ingress: p.Old.Ingress}
		f.source.states[0].Nonce = infracontract.ID{30}
		f.source.states[0].Stable = false
		if _, err := f.routes.Prepare(f.plan, f.checkpoint, infracontract.RouteReturn, f.deadline); err == nil {
			t.Fatal("automatic return renewed an unstable hop")
		}
		f.source.states[0].Stable = true
		returned, err := f.routes.Prepare(f.plan, f.checkpoint, infracontract.RouteReturn, f.deadline)
		if err != nil || returned[0].Direction != infracontract.RouteReturn || returned[0].Old != f.new {
			t.Fatal("genuine stable return rejected")
		}
	})
	t.Run("prepare_retry_rechecks_home", func(t *testing.T) {
		f := newRouteFixture(t)
		f.prepare(t)
		f.source.states[0].AuthorityRevision++
		if _, err := f.routes.Prepare(f.plan, f.checkpoint, infracontract.RouteOutbound, f.deadline); err == nil {
			t.Fatal("stale Prepare retry ignored home CAS")
		}
	})
	t.Run("new_step_requires_fresh_challenge", func(t *testing.T) {
		f := newRouteFixture(t)
		f.prepare(t)
		f.plan.Step++
		if _, err := f.routes.Prepare(f.plan, f.checkpoint, infracontract.RouteOutbound, f.deadline); !errors.Is(err, ErrTransition) {
			t.Fatal("another hop reused original nonce")
		}
		f.source.states[0].Nonce[0]++
		if _, err := f.routes.Prepare(f.plan, f.checkpoint, infracontract.RouteOutbound, f.deadline); !errors.Is(err, ErrTransition) {
			t.Fatal("fresh nonce bypassed unfinished prior move")
		}
		f.plan.Step--
		f.source.states[0].Nonce = f.state.Nonce
		if _, err := f.routes.Bind(f.plan, f.observeNew(), infracontract.RouteOutbound, f.deadline); err != nil {
			t.Fatal(err)
		}
		f.plan.Step++
		if _, err := f.routes.Prepare(f.plan, f.checkpoint, infracontract.RouteOutbound, f.deadline); !errors.Is(err, ErrTransition) {
			t.Fatal("completed move reused original nonce in another hop")
		}
		f.source.states[0].Nonce[0]++
		if _, err := f.routes.Prepare(f.plan, f.checkpoint, infracontract.RouteOutbound, f.deadline); err != nil {
			t.Fatal("independent stable step rejected", err)
		}
	})
	t.Run("boot_revoke_release_are_irreversible", func(t *testing.T) {
		for _, action := range []func(*Hold, View, int64){func(h *Hold, v View, n int64) { h.Protects(v, "new-boot", n) }, func(h *Hold, v View, n int64) { h.Protects(v, "observer-boot", n-1) }, func(h *Hold, _ View, _ int64) { h.Release() }} {
			f := newRouteFixture(t)
			p := f.prepare(t)
			h := installedHold(t, f, p)
			v := testView(f, p.Old)
			action(h, v, f.start)
			if h.Protects(v, "observer-boot", f.start+1) {
				t.Fatal("terminated hold resurrected")
			}
		}
	})
	t.Run("challenge_identity_and_first_clock_map", func(t *testing.T) {
		if _, err := NewHold(Challenge{}); err == nil {
			t.Fatal("missing challenge accepted")
		}
		f := newRouteFixture(t)
		p := f.prepare(t)
		c := Challenge{Domain: f.plan.Domain, Tx: f.plan.Tx, Participant: p.Participant, Direction: p.Direction, Nonce: p.Nonce, BootID: "observer-boot", StartedNS: f.start}
		for _, edit := range []func(*infracontract.RouteProof){func(p *infracontract.RouteProof) { p.Tx[0]++ }, func(p *infracontract.RouteProof) { p.Nonce[0]++ }, func(p *infracontract.RouteProof) { p.Direction = infracontract.RouteReturn }} {
			h, err := NewHold(c)
			if err != nil {
				t.Fatal(err)
			}
			bad := p
			edit(&bad)
			bad = f.sign(t, bad)
			if _, err := h.Install(bad, "route-key", f.pub, testView(f, p.Old), "observer-boot", f.start, f.start+1); !errors.Is(err, ErrIdentity) {
				t.Fatal("unrequested proof accepted")
			}
		}
		for _, sent := range []int64{f.start - 1, f.start + 1, f.start + 3} {
			h, _ := NewHold(c)
			if _, err := h.Install(p, "route-key", f.pub, testView(f, p.Old), "observer-boot", sent, f.start+2); !errors.Is(err, ErrInvalid) {
				t.Fatal("first reply changed challenge t0")
			}
		}
	})
}

// origin: hot-update S9 plan §12 P16 / T36 / T37
func TestInfraMaintenanceLifecycle(t *testing.T) {
	beforeG := runtime.NumGoroutine()
	beforeFD := testharness.FDCount()
	for round := 0; round < 5; round++ {
		exerciseMaintenance(t)
	}
	testharness.AssertNoGoroutineLeak(t, "infra maintenance", beforeG)
	testharness.AssertNoFDLeak(t, "infra maintenance", beforeFD, 0)
}

func exerciseMaintenance(t *testing.T) {
	t.Helper()
	f := newRouteFixture(t)
	p := f.prepare(t)
	h := installedHold(t, f, p)
	v := testView(f, p.Old)
	pending := Pending{Participant: p.Participant, Generation: f.new.Generation, Target: p.Target}
	if err := h.BeginDial(pending, v, "observer-boot", f.start); err != nil {
		t.Fatal(err)
	}
	var wg sync.WaitGroup
	for worker := 0; worker < 8; worker++ {
		wg.Go(func() {
			for range 10 {
				if !h.ProtectsPending(pending, v, "observer-boot", f.start+1) {
					t.Error("concurrent pending check failed")
				}
			}
		})
	}
	wg.Wait()
	f.clock.Store(f.start + int64(time.Second))
	proofs, err := f.routes.Bind(f.plan, f.observeNew(), infracontract.RouteOutbound, f.deadline)
	if err != nil {
		t.Fatal(err)
	}
	v.Attachment = f.new
	if _, err := h.Install(proofs[0], "route-key", f.pub, v, "observer-boot", f.clock.Load(), f.clock.Load()); err != nil {
		t.Fatal(err)
	}
	for worker := 0; worker < 8; worker++ {
		wg.Go(func() {
			for range 10 {
				if !h.Protects(v, "observer-boot", f.start+int64(2*time.Second)) {
					t.Error("concurrent bound check failed")
				}
			}
		})
	}
	wg.Wait()
	h.Release()
	if h.Protects(v, "observer-boot", f.start+int64(2*time.Second)) {
		t.Fatal("released hold protects")
	}
	f.routes.Forget(f.plan.Tx)
	if len(f.routes.prepared) != 0 {
		t.Fatal("route settlement retained entries")
	}
}
