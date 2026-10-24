package ingressmove

import (
	"crypto/ed25519"
	"runtime"
	"testing"
	"time"

	c "github.com/LinZiyang666/tether/internal/infracontract"
	"github.com/LinZiyang666/tether/internal/testharness"
)

// Five real embedded-NATS rounds prove NC CONNECT/PONG, distinct raw OFDs,
// same-server changed cid on return, independent dataNC, lost reply query and
// joined socket teardown. P02's Caddy accepted-PID producer and P14's blocked
// DNS/WSS cancellation remain independent pending integration responsibilities.
// origin: hot-update S9 plan §4.5 / §9.5 / §12 P15 / T35 / M17
func TestIngressMoveLifecycle(t *testing.T) {
	if runtime.GOOS != "linux" {
		t.Skip("Linux sandbox socket census unsupported on this platform")
	}
	beforeG, beforeFD := runtime.NumGoroutine(), testharness.FDCount()
	for round := 0; round < 5; round++ {
		exerciseMove(t)
	}
	testharness.AssertNoGoroutineLeak(t, "ingress move", beforeG)
	testharness.AssertNoFDLeak(t, "ingress move", beforeFD, 0)
}

func exerciseMove(t *testing.T) {
	t.Helper()
	s := newSandbox(t)
	defer s.close()
	plan := c.Plan{Tx: c.ID{61}, Domain: c.ID{62}, Step: 1, Component: c.NATS, Action: c.Upgrade, PolicyRevision: 7, Before: s.process}
	r, err := New(s, func(p c.Plan, proof c.RouteProof, _ c.Deadline) error {
		if p != plan {
			return ErrIdentity
		}
		unsigned := cloneProof(proof)
		unsigned.Signature = nil
		data, e := c.EncodeRouteProof(unsigned)
		if e != nil {
			return e
		}
		if !ed25519.Verify(s.key.Public().(ed25519.PublicKey), data, proof.Signature) {
			return ErrIdentity
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	for i, role := range []string{"control", "data"} {
		p := &sandboxConnection{sandbox: s, id: c.ID{byte(i + 1)}}
		if err = p.connect(s.servers[0].ClientURL(), deadline(t)); err != nil {
			t.Fatal(err)
		}
		s.connections = append(s.connections, p)
		a, e := r.Admit(p.id, role, deadline(t))
		if e != nil {
			t.Fatal(e)
		}
		if err = r.Register(a, p, deadline(t)); err != nil {
			t.Fatal(err)
		}
	}
	// This is the actual nats.go SetServerPool seam: removing the current URL
	// changes ConnectedUrl without reconnecting the raw TCP socket or cid.
	conn := s.connections[1]
	original, err := conn.Observe(deadline(t))
	if err != nil {
		t.Fatal(err)
	}
	if err = conn.nc.SetServerPool([]string{s.servers[1].ClientURL()}); err != nil {
		t.Fatal(err)
	}
	poolOnly, err := conn.Observe(deadline(t))
	if err != nil {
		t.Fatal(err)
	}
	if poolOnly.URL == original.URL || poolOnly.CID != original.CID || poolOnly.Raw != original.Raw {
		t.Fatal("server-pool negative did not hit unchanged physical attachment")
	}
	if s.Verify(poolOnly, c.RouteTarget{}, deadline(t)) == nil {
		t.Fatal("independent server census accepted logical pool change")
	}
	if _, err = r.Seal(plan, deadline(t)); err == nil {
		t.Fatal("actual pool-only update certified migration")
	}
	if err = conn.nc.SetServerPool([]string{s.servers[0].ClientURL()}); err != nil {
		t.Fatal(err)
	}
	restored, err := conn.Observe(deadline(t))
	if err != nil || restored != original {
		t.Fatal("pool-only negative failed to restore original attachment")
	}
	cp, err := r.Seal(plan, deadline(t))
	if err != nil {
		t.Fatal(err)
	}
	var outbound, returned []c.RouteProof
	for _, direction := range []c.RouteDirection{c.RouteOutbound, c.RouteReturn} {
		targetServer := s.servers[1]
		if direction == c.RouteReturn {
			targetServer = s.servers[0]
		}
		target := c.RouteTarget{URL: targetServer.ClientURL(), NodeID: targetServer.ID(), Component: c.NATS, Ingress: s.process}
		var proofs []c.RouteProof
		for _, conn := range s.connections {
			a, e := conn.Observe(deadline(t))
			if e != nil {
				t.Fatal(e)
			}
			p := prepare(plan, a, direction, target)
			s.sign(t, &p)
			proofs = append(proofs, p)
			if err = r.Install(cp, p, deadline(t)); err != nil {
				t.Fatal(err)
			}
		}
		var move c.MoveResult
		if direction == c.RouteOutbound {
			s.connections[1].lostReply = true
			if _, err = r.Move(cp, deadline(t), nil); err == nil {
				t.Fatal("lost actual attachment reply certified success")
			}
			move, err = r.Move(cp, deadline(t), nil)
		} else {
			move, err = r.Return(cp, deadline(t), nil)
		}
		if err != nil {
			t.Fatal(err)
		}
		ps := binds(proofs, move)
		for i := range ps {
			s.sign(t, &ps[i])
		}
		census, e := r.Bind(cp, ps, deadline(t))
		if e != nil {
			t.Fatal(e)
		}
		if census.OldOwnerPending != 0 || census.LiveBusiness != 2 {
			t.Fatal("real socket cohort responsibility mismatch")
		}
		if direction == c.RouteOutbound {
			outbound = ps
		} else {
			returned = ps
		}
		if direction == c.RouteOutbound && r.Release(cp, deadline(t)) == nil {
			t.Fatal("outbound ACKs certified completed return")
		}
	}
	for i, conn := range s.connections {
		if conn.attachCalls != 2 || returned[i].New.ServerID != outbound[i].Old.ServerID || returned[i].New.CID == outbound[i].Old.CID || returned[i].New.Raw.Cookie == outbound[i].Old.Raw.Cookie {
			t.Fatal("return did not establish a new original-server socket/cid")
		}
	}
	if err = r.Release(cp, deadline(t)); err != nil {
		t.Fatal(err)
	}
	// Live traffic after registry release proves that registry cleanup has no
	// authority over even the NC itself, much less a business data owner.
	for _, conn := range s.connections {
		if _, e := conn.Observe(deadline(t)); e != nil {
			t.Fatal(e)
		}
	}
	for _, conn := range s.connections {
		conn.close()
	}
	for _, ns := range s.servers {
		testharness.WaitFor(t, 2*time.Second, time.Millisecond, func() bool { return ns.NumClients() == 0 })
	}
	for _, conn := range s.connections {
		if err = r.Remove(conn.id, conn.generation, deadline(t)); err != nil {
			t.Fatal(err)
		}
	}
	// FD/leak assertions in the Test body run after explicit shutdown, before
	// t.Cleanup. The deferred idempotent close only protects fatal paths.
	s.close()
}
