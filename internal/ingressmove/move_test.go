package ingressmove

import (
	"errors"
	"reflect"
	"testing"
	"time"

	"github.com/LinZiyang666/tether/internal/controlbudget"
	c "github.com/LinZiyang666/tether/internal/infracontract"
	"github.com/LinZiyang666/tether/internal/upgradebudget"
)

// origin: hot-update S9 plan §2.3 / §4.5 / §4.6 / T35 / M17
func TestIngressReturnRequiresEveryAttachment(t *testing.T) {
	t.Run("both_ledgers_all_connections_and_live_business", func(t *testing.T) {
		f, cp := outboundFixture(t)
		proofs := f.prepares(c.RouteReturn, f.returned)
		mustInstall(t, f, cp, proofs[0])
		if _, err := f.registry.Return(cp, deadline(t), nil); err == nil {
			t.Fatal("return reused outbound Installed ACK for dataNC")
		}
		for _, conn := range f.connections {
			if conn.attachCalls != 1 {
				t.Fatal("partial return prepare had physical effect")
			}
		}
		mustInstall(t, f, cp, proofs[1])
		move := mustMove(t, f, cp, c.RouteReturn)
		if err := f.registry.Release(cp, deadline(t)); err == nil {
			t.Fatal("Attached substituted for Returned Bind ACK")
		}
		if _, err := f.registry.Bind(cp, binds(proofs, move)[:1], deadline(t)); err == nil {
			t.Fatal("missing dataNC return proof accepted")
		}
		mustBind(t, f, cp, proofs, move)
		for _, direction := range []c.RouteDirection{c.RouteOutbound, c.RouteReturn} {
			ledger, err := f.registry.Ledger(direction)
			if err != nil || len(ledger.Installed) != 2 || len(ledger.Attached) != 2 || len(ledger.Bound) != 2 {
				t.Fatal("missing leg-specific receipt")
			}
			wantGeneration := uint64(2)
			if direction == c.RouteReturn {
				wantGeneration = 3
			}
			for _, a := range ledger.Bound {
				if a.Generation != wantGeneration {
					t.Fatal("old leg's generation inherited")
				}
			}
			ledger.Bound[0].CID = 0
			fresh, _ := f.registry.Ledger(direction)
			if fresh.Bound[0].CID == 0 {
				t.Fatal("caller altered registry receipts")
			}
		}
		f.witness.pending = 1 // An ordinary HTTP/late-Upgrade responsibility.
		if err := f.registry.Release(cp, deadline(t)); err == nil {
			t.Fatal("old socket responsibility ignored at release")
		}
		f.witness.pending = 0
		if err := f.registry.Release(cp, deadline(t)); err != nil {
			t.Fatal(err)
		}
		ledger, _ := f.registry.Ledger(c.RouteReturn)
		if len(ledger.Bound) != 2 {
			t.Fatal("release erased return receipts")
		}
		for _, conn := range f.connections {
			if conn.current.Generation != 3 || conn.current.URL != f.returned.URL {
				t.Fatal("release changed live transport")
			}
		}
		f.plan.Tx[0]++
		f.plan.Step++
		newCP := f.seal(t)
		if newCP.Cohort == cp.Cohort {
			t.Fatal("next hop reused cohort identity")
		}
		if _, err := f.registry.Move(newCP, deadline(t), nil); err == nil {
			t.Fatal("next hop inherited previous hop ACKs")
		}
	})
	t.Run("physical_change_cannot_be_replaced_by_pool_or_forwarder", func(t *testing.T) {
		for _, mode := range []string{"pool_only", "same_cid", "old_generation", "wrong_ingress", "forwarded", "unchanged"} {
			t.Run(mode, func(t *testing.T) {
				f := newCohortFixture(t, "control", "data")
				cp := f.seal(t)
				proofs := f.prepares(c.RouteOutbound, f.away)
				for _, p := range proofs {
					mustInstall(t, f, cp, p)
				}
				f.connections[1].attachMode = mode
				if _, err := f.registry.Move(cp, deadline(t), nil); err == nil {
					t.Fatal("nonphysical/foreign attachment certified")
				}
			})
		}
	})
	t.Run("prepare_and_bind_field_binding", func(t *testing.T) {
		for _, field := range []string{"Purpose", "Direction", "Target", "Version", "Tx", "Participant", "Old", "HomeDigest", "AuthorityRevision", "PolicyRevision", "RemainingNS", "Nonce", "KeyID", "Signature"} {
			t.Run("prepare_"+field, func(t *testing.T) {
				f := newCohortFixture(t, "data")
				cp := f.seal(t)
				p := f.prepares(c.RouteOutbound, f.away)[0]
				reflect.ValueOf(&p).Elem().FieldByName(field).SetZero()
				if err := f.registry.Install(cp, p, deadline(t)); err == nil {
					t.Fatal("unbound Prepare field accepted")
				}
				if f.connections[0].installCalls != 0 {
					t.Fatal("invalid Prepare reached NC")
				}
			})
			t.Run("bind_"+field, func(t *testing.T) {
				f := newCohortFixture(t, "data")
				cp := f.seal(t)
				proofs := f.prepares(c.RouteOutbound, f.away)
				mustInstall(t, f, cp, proofs[0])
				move := mustMove(t, f, cp, c.RouteOutbound)
				ps := binds(proofs, move)
				reflect.ValueOf(&ps[0]).Elem().FieldByName(field).SetZero()
				if _, err := f.registry.Bind(cp, ps, deadline(t)); err == nil {
					t.Fatal("unbound Bind field accepted")
				}
				if f.connections[0].bindCalls != 0 {
					t.Fatal("invalid Bind reached NC")
				}
			})
		}
		f := newCohortFixture(t, "data")
		cp := f.seal(t)
		p := f.prepares(c.RouteOutbound, f.away)[0]
		p.New = p.Old
		if err := f.registry.Install(cp, p, deadline(t)); err == nil {
			t.Fatal("Prepare invented a future cid")
		}
	})
	t.Run("forwarded_installed_and_returned_ack", func(t *testing.T) {
		f := newCohortFixture(t, "control", "data")
		cp := f.seal(t)
		proofs := f.prepares(c.RouteOutbound, f.away)
		f.connections[1].installReply = &f.connections[0].current
		if err := f.registry.Install(cp, proofs[1], deadline(t)); err == nil {
			t.Fatal("forwarded control ACK signed dataNC installation")
		}
		f.connections[1].installReply = nil
		for _, p := range proofs {
			mustInstall(t, f, cp, p)
		}
		move := mustMove(t, f, cp, c.RouteOutbound)
		duplicate := binds(proofs, move)
		duplicate[1] = duplicate[0]
		if _, err := f.registry.Bind(cp, duplicate, deadline(t)); err == nil {
			t.Fatal("duplicate ACK replaced missing participant")
		}
		mustBind(t, f, cp, proofs, move)
		proofs = f.prepares(c.RouteReturn, f.returned)
		for _, p := range proofs {
			mustInstall(t, f, cp, p)
		}
		move = mustMove(t, f, cp, c.RouteReturn)
		f.connections[1].bindReply = &f.connections[0].current
		if _, err := f.registry.Bind(cp, binds(proofs, move), deadline(t)); err == nil {
			t.Fatal("forwarded return ACK signed independent dataNC")
		}
		if err := f.registry.Release(cp, deadline(t)); err == nil {
			t.Fatal("missing Returned ACK released cohort")
		}
	})
	t.Run("lost_attachment_reply_queries_original_generation", func(t *testing.T) {
		f := newCohortFixture(t, "data")
		cp := f.seal(t)
		proofs := f.prepares(c.RouteOutbound, f.away)
		mustInstall(t, f, cp, proofs[0])
		f.connections[0].attachErr = errors.New("attachment reply cut after effect")
		if _, err := f.registry.Move(cp, deadline(t), nil); err == nil {
			t.Fatal("lost attachment reply pretended success")
		}
		generation := f.connections[0].current.Generation
		f.connections[0].attachErr = nil
		move := mustMove(t, f, cp, c.RouteOutbound)
		if f.connections[0].attachCalls != 1 || move.Attachments[0].Generation != generation {
			t.Fatal("lost reply minted a second generation")
		}
		f.connections[0].bindErr = errors.New("Bind reply cut after receipt")
		if _, err := f.registry.Bind(cp, binds(proofs, move), deadline(t)); err == nil {
			t.Fatal("lost Bind reply pretended success")
		}
		f.connections[0].bindErr = nil
		mustBind(t, f, cp, proofs, move)
		if f.connections[0].attachCalls != 1 {
			t.Fatal("Bind retry reconnected transport")
		}
	})
	t.Run("signed_intent_is_verified_and_copied", func(t *testing.T) {
		f := newCohortFixture(t, "data")
		cp := f.seal(t)
		p := f.prepares(c.RouteOutbound, f.away)[0]
		bad := cloneProof(p)
		bad.Signature[0]++
		if err := f.registry.Install(cp, bad, deadline(t)); err == nil {
			t.Fatal("invalid signed intent accepted")
		}
		mustInstall(t, f, cp, p)
		copy := cloneProof(p)
		p.Signature[0]++
		mustInstall(t, f, cp, copy)
	})
	t.Run("restricted_target_and_prepare_limits", func(t *testing.T) {
		defects := []struct {
			name   string
			mutate func(*c.RouteProof)
		}{
			{"credentials_in_url", func(p *c.RouteProof) { p.Target.URL = "nats://user:pass@127.0.0.1:4223" }},
			{"missing_host", func(p *c.RouteProof) { p.Target.URL = "nats:///path" }},
			{"unknown_scheme", func(p *c.RouteProof) { p.Target.URL = "https://127.0.0.1:4223" }},
			{"url_query", func(p *c.RouteProof) { p.Target.URL += "?token=private" }},
			{"url_fragment", func(p *c.RouteProof) { p.Target.URL += "#private" }},
			{"unknown_node", func(p *c.RouteProof) { p.Target.NodeID = "" }},
			{"unknown_component", func(p *c.RouteProof) { p.Target.Component = 99 }},
			{"wrong_component", func(p *c.RouteProof) { p.Target.Component = c.Caddy }},
			{"unknown_ingress", func(p *c.RouteProof) { p.Target.Ingress.BootID = "" }},
			{"excess_hold", func(p *c.RouteProof) { p.RemainingNS = uint64(upgradebudget.BrokerMaintenanceTTL) + 1 }},
			{"short_signature", func(p *c.RouteProof) { p.Signature = p.Signature[:1] }},
		}
		for _, defect := range defects {
			t.Run(defect.name, func(t *testing.T) {
				f := newCohortFixture(t, "data")
				cp := f.seal(t)
				p := f.prepares(c.RouteOutbound, f.away)[0]
				defect.mutate(&p)
				if err := f.registry.Install(cp, p, deadline(t)); err == nil {
					t.Fatal("unsupported target or renewed hold accepted")
				}
			})
		}
	})
	t.Run("unknown_bind_participant_and_new_identity", func(t *testing.T) {
		f := newCohortFixture(t, "data")
		cp := f.seal(t)
		proofs := f.prepares(c.RouteOutbound, f.away)
		mustInstall(t, f, cp, proofs[0])
		move := mustMove(t, f, cp, c.RouteOutbound)
		ps := binds(proofs, move)
		ps[0].New = c.Attachment{}
		if _, err := f.registry.Bind(cp, ps, deadline(t)); err == nil {
			t.Fatal("zero New accepted in Bind")
		}
		ps = binds(proofs, move)
		extra := cloneProof(ps[0])
		extra.Participant = c.ID{99}
		ps = append(ps, extra)
		if _, err := f.registry.Bind(cp, ps, deadline(t)); err == nil {
			t.Fatal("unknown extra participant accepted in Bind")
		}
		ps = binds(proofs, move)
		ps[0].Old.CID++
		if _, err := f.registry.Bind(cp, ps, deadline(t)); err == nil {
			t.Fatal("nonzero Old tamper escaped exact attachment binding")
		}
	})
	t.Run("physical_witness_and_return_census_are_required", func(t *testing.T) {
		f := newCohortFixture(t, "data")
		cp := f.seal(t)
		proofs := f.prepares(c.RouteOutbound, f.away)
		mustInstall(t, f, cp, proofs[0])
		f.witness.defect = "verify_failure"
		if _, err := f.registry.Move(cp, deadline(t), nil); err == nil {
			t.Fatal("NC supplied its own physical witness")
		}
		f.witness.defect = ""
		move := mustMove(t, f, cp, c.RouteOutbound)
		mustBind(t, f, cp, proofs, move)
		proofs = f.prepares(c.RouteReturn, f.returned)
		mustInstall(t, f, cp, proofs[0])
		move = mustMove(t, f, cp, c.RouteReturn)
		f.witness.defect = "incomplete"
		if _, err := f.registry.Bind(cp, binds(proofs, move), deadline(t)); err == nil {
			t.Fatal("incomplete return census certified")
		}
		f.witness.defect = ""
		if err := f.registry.Release(cp, deadline(t)); err == nil {
			t.Fatal("failed return census inherited old Bind authority")
		}
		mustBind(t, f, cp, proofs, move)
	})
	t.Run("one_move_budget_covers_the_complete_cohort", func(t *testing.T) {
		f := newCohortFixture(t, "control", "data")
		cp := f.seal(t)
		proofs := f.prepares(c.RouteOutbound, f.away)
		for _, p := range proofs {
			mustInstall(t, f, cp, p)
		}
		long, err := controlbudget.After(3 * upgradebudget.RequestLatency)
		if err != nil {
			t.Fatal(err)
		}
		upper, err := controlbudget.After(upgradebudget.RequestLatency + upgradebudget.RequestLatency/10)
		if err != nil {
			t.Fatal(err)
		}
		if _, err = f.registry.Move(cp, long, nil); err != nil {
			t.Fatal(err)
		}
		for _, conn := range f.connections {
			if conn.attachDeadline > upper {
				t.Fatal("transport received more than the complete cohort move budget")
			}
		}
		f.registry.mu.Lock()
		f.registry.active.moveDeadline = 1
		f.registry.mu.Unlock()
		if _, err = f.registry.Move(cp, long, nil); err == nil {
			t.Fatal("lost-reply query renewed the original move deadline")
		}
	})
	t.Run("expired_prepare_retry_never_renews_hold", func(t *testing.T) {
		f := newCohortFixture(t, "data")
		cp := f.seal(t)
		p := f.prepares(c.RouteOutbound, f.away)[0]
		p.RemainingNS = uint64(25 * time.Millisecond)
		f.connections[0].installErr = errors.New("Installed reply cut")
		if err := f.registry.Install(cp, p, deadline(t)); err == nil {
			t.Fatal("lost reply accepted")
		}
		f.registry.mu.Lock()
		expires := f.registry.participants[p.Participant].expires
		f.registry.mu.Unlock()
		waitDeadline(t, expires)
		f.connections[0].installErr = nil
		if err := f.registry.Install(cp, p, deadline(t)); err == nil {
			t.Fatal("retry renewed expired prepare hold")
		}
		if f.connections[0].installCalls != 1 {
			t.Fatal("expired nonce resent")
		}
	})
	t.Run("installation_time_consumes_original_hold", func(t *testing.T) {
		f := newCohortFixture(t, "data")
		cp := f.seal(t)
		p := f.prepares(c.RouteOutbound, f.away)[0]
		p.RemainingNS = uint64(25 * time.Millisecond)
		f.connections[0].installHook = func(d c.Deadline) { waitDeadline(t, d) }
		if err := f.registry.Install(cp, p, deadline(t)); err == nil {
			t.Fatal("late Installed reply renewed hold")
		}
	})
	t.Run("stop_busy_stale_and_census_failures", func(t *testing.T) {
		f := newCohortFixture(t, "data")
		cp := f.seal(t)
		p := f.prepares(c.RouteOutbound, f.away)[0]
		mustInstall(t, f, cp, p)
		stop := make(chan struct{})
		close(stop)
		if _, err := f.registry.Move(cp, deadline(t), stop); err == nil {
			t.Fatal("canceled operation changed transport")
		}
		if f.connections[0].attachCalls != 0 {
			t.Fatal("canceled operation had attachment effect")
		}
		if _, err := f.registry.Return(cp, deadline(t), nil); err == nil {
			t.Fatal("unprepared return accepted")
		}
		bad := cp
		bad.Proof[0]++
		if _, err := f.registry.Move(bad, deadline(t), nil); err == nil {
			t.Fatal("tampered checkpoint accepted")
		}
		f.registry.mu.Lock()
		f.registry.busy = true
		f.registry.mu.Unlock()
		if _, err := f.registry.Move(cp, deadline(t), nil); err == nil {
			t.Fatal("concurrent movement admitted")
		}
		f.registry.mu.Lock()
		f.registry.busy = false
		f.registry.mu.Unlock()
		move := mustMove(t, f, cp, c.RouteOutbound)
		f.witness.defect = "incomplete"
		if _, err := f.registry.Bind(cp, binds([]c.RouteProof{p}, move), deadline(t)); err == nil {
			t.Fatal("partial census certified Bind")
		}
		returnProof := f.prepares(c.RouteReturn, f.returned)[0]
		if err := f.registry.Install(cp, returnProof, deadline(t)); err == nil {
			t.Fatal("failed Bind census authorized return")
		}
		f.witness.defect = ""
		mustBind(t, f, cp, []c.RouteProof{p}, move)
	})
}

func outboundFixture(t *testing.T) (*cohortFixture, c.Checkpoint) {
	t.Helper()
	f := newCohortFixture(t, "control", "data")
	cp := f.seal(t)
	proofs := f.prepares(c.RouteOutbound, f.away)
	for _, p := range proofs {
		mustInstall(t, f, cp, p)
	}
	move := mustMove(t, f, cp, c.RouteOutbound)
	mustBind(t, f, cp, proofs, move)
	return f, cp
}
