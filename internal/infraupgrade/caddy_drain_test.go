package infraupgrade

import (
	"errors"
	"testing"

	c "github.com/LinZiyang666/tether/internal/infracontract"
)

// This sandbox owner checks the active ingress state, separately from census.
// Production binds this port to the root's rules and responsibility seal.
func (a caddyTestHost) VerifyCaddyIngress(p c.Plan, old, target c.Process, census c.Census, _ c.Deadline, _ c.Stop) error {
	h := a.h
	if p != h.plan || !caddyPresent(census, target) || census.OldOwnerPending != 0 ||
		(old == p.Before && (target != h.warm || !h.redirect)) ||
		(old == h.warm && (target != h.main || h.redirect)) || h.fault == "ingress_unsealed" {
		return errors.New("infra_inventory_incomplete")
	}
	return nil
}

// origin: S9 internal review rv-infra F07
func TestCaddySealedListenersReachGracefulStop(t *testing.T) {
	for _, action := range []c.Action{c.Upgrade, c.Rollback, c.Reload} {
		h := newCaddyFixture(t, action)
		h.censusHook = func(census *c.Census) {
			for _, owner := range census.Processes {
				census.Sockets = append(census.Sockets, c.Socket{Owner: owner, NetNS: 1, Device: 1, Inode: uint64(owner.PID), Cookie: uint64(owner.PID), Local: "0.0.0.0:443", Remote: "0.0.0.0:0"})
			}
		}
		status, err := h.driver.Advance(h.plan, h.deadline(), nil)
		if err != nil || status.Phase != c.Completed || h.warmLive || (action != c.Reload && (h.oldLive || h.applied[c.SignalGraceful] != 1)) {
			t.Fatalf("sealed listener prevented retirement for %v: %v", action, err)
		}
	}
}

// origin: S9 internal review rv-infra F07
func TestCaddyDrainRequiresIngressSealAndFullResponsibility(t *testing.T) {
	for _, fault := range []string{"ingress_unsealed", "accepted_TLS", "idle_HTTP", "slow_HTTP", "late_Upgrade", "backlog", "conntrack", "unknown"} {
		t.Run(fault, func(t *testing.T) {
			h := newCaddyFixture(t, c.Upgrade)
			h.fault = fault
			h.censusHook = func(census *c.Census) {
				if !h.moved || !h.oldLive {
					return
				}
				socket := c.Socket{Owner: h.plan.Before, NetNS: 1, Device: 1, Inode: 8, Cookie: 9, Local: "0.0.0.0:443", Remote: "0.0.0.0:0"}
				switch fault {
				case "ingress_unsealed":
				case "accepted_TLS", "idle_HTTP", "slow_HTTP", "late_Upgrade":
					socket.Remote = "127.0.0.1:9000"
				case "backlog", "conntrack":
					census.OldOwnerPending = 1
				case "unknown":
					socket.Remote = ""
				}
				census.Sockets = append(census.Sockets, socket)
			}
			status, err := h.driver.Advance(h.plan, h.deadline(), nil)
			if err == nil || status.Phase == c.Completed || h.applied[c.SignalGraceful] != 0 || !h.oldLive || !h.warmLive {
				t.Fatalf("unsealed or pending ingress was stopped: %v %v", status.Phase, err)
			}
		})
	}
}
