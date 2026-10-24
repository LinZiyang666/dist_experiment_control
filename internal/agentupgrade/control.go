// Control completion traverses the actual forwarded subscriber with a fresh
// nonce, instance and session generation before attempting a durable commit.
// origin: hot-update S8 plan §4.2 / M05 / M15 / §12.1 P14.
package agentupgrade

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	mrand "math/rand/v2"
	"strings"
	"sync/atomic"
	"time"

	"github.com/LinZiyang666/tether/internal/agentboot"
	"github.com/LinZiyang666/tether/internal/agentupgradeport"
	"github.com/LinZiyang666/tether/internal/binaryupgrade"
	"github.com/LinZiyang666/tether/internal/drillobserve"
	"github.com/LinZiyang666/tether/internal/proto"
	"github.com/LinZiyang666/tether/internal/upgradebudget"
	"github.com/LinZiyang666/tether/internal/upgradeport"
	"github.com/LinZiyang666/tether/internal/upgradetx"

	"github.com/nats-io/nats.go"
)

type ControlProof = agentupgradeport.ControlProof

type ControlPort struct {
	InstanceID, SID, NID string
	Current              *atomic.Pointer[nats.Conn]
	Generation           *atomic.Uint64
	Dependencies         *Dependencies
	Anonymous            bool
	// Heartbeat renders this agent's ordinary heartbeat. The probe adds its
	// nonce and the broker relays it as a claim probe through the real
	// forwarded subscriber (brokerupgrade.WithControlProbe). It must be the
	// full heartbeat, because the broker also records liveness and proxy
	// readiness from it.
	// origin: hot-update S8 plan §4.2 / M05; S8 FIX-DESCRIBE
	Heartbeat func() []byte
}

// The forwarded wildcard is installed by session, so this probe traverses a
// real NATS request and the actual command subscriber. A nonce, instance and
// session-generation echo exclude a cached reply or a retired connection.
// origin: hot-update S8 plan §4.2 / M05 / M15.
func ProbeControl(ctx context.Context, p ControlPort, nc *nats.Conn, nonce string) error {
	if nc == nil || p.Current.Load() != nc || len(nonce) != 32 {
		return errors.New("agent: control probe has no current connection or nonce")
	}
	generation := p.Generation.Load()
	body, _ := json.Marshal(map[string]string{"query_nonce": nonce})
	probeCtx, cancel := context.WithTimeout(ctx, upgradebudget.RequestLatency)
	defer cancel()
	if err := nc.FlushWithContext(probeCtx); err != nil {
		return err
	}
	var reply *nats.Msg
	var err error
	switch ports := p.Dependencies; {
	case ports != nil && ports.ControlProbe != nil:
		reply, err = ports.ControlProbe(probeCtx, nonce)
	case p.Anonymous:
		// Anonymous ordinary-wire fixtures have no publish ACL. This path cannot
		// produce an S8 receipt: no transaction endpoint or registry is installed.
		reply, err = nc.RequestWithContext(probeCtx, proto.SubjCmdForwarded(p.SID, p.NID, proto.ClaimProbeVerb), body)
	case ports != nil && p.Heartbeat != nil:
		// The production external probe: the broker relays this nonce-bound
		// heartbeat as a claim probe through the real forwarded subscriber.
		// An old broker never answers, so the probe times out unproven.
		// origin: hot-update S8 plan §4.2 / M05; S8 FIX-DESCRIBE
		reply, err = agentupgradeport.HeartbeatProbe(probeCtx, nc, p.SID, p.NID, p.Heartbeat(), nonce)
	default:
		return &upgradetx.Error{Code: upgradetx.CodeNotWired, Detail: "external control probe is unavailable"}
	}
	if err != nil {
		return err
	}
	if reply == nil {
		return errors.New("agent: control probe returned no evidence")
	}
	var proof ControlProof
	if json.Unmarshal(reply.Data, &proof) != nil || proof.InstanceID != p.InstanceID || proof.Nonce != nonce || proof.Generation != generation || p.Generation.Load() != generation || p.Current.Load() != nc {
		return errors.New("agent: control probe did not prove the current subscription")
	}
	return nil
}

type ReadyPort struct {
	Control          ControlPort
	Endpoint         upgradeport.Endpoint
	Captured         *binaryupgrade.Marker
	Legacy           LegacyPort
	Ready            *atomic.Pointer[nats.Conn]
	RefreshAdmission func()
	Image            *agentboot.Image
}

func FinishControlReady(ctx context.Context, p ReadyPort, nc *nats.Conn) error {
	// origin: S8 FIX-DESCRIBE (P19 request R3). A registered agent with no
	// carried cut has no S8 attempt to complete; requiring the external probe
	// there would end every session of a registered agent under an older
	// broker that cannot relay it. Only a cut, a pending legacy marker or an
	// anonymous fixture still proves control before readiness.
	carried := p.Control.Dependencies != nil && p.Control.Dependencies.Cut != nil
	if !carried && !p.Control.Anonymous && (p.Captured == nil || p.Captured.State != binaryupgrade.StatePending) {
		// Ordinary wire against old brokers needs no S8 proof. The observer port
		// below still refuses without an installed external probe.
		// NATS requires a deadline even when Run's parent is unbounded. Reuse
		// the control request budget; an earlier caller deadline still wins.
		// origin: hot-update S8 plan §4.2 / P14 integration maintenance regression.
		flushCtx, cancel := context.WithTimeout(ctx, upgradebudget.RequestLatency)
		defer cancel()
		if err := nc.FlushWithContext(flushCtx); err != nil {
			return err
		}
	} else if err := ProbeControl(ctx, p.Control, nc, Nonce()); err != nil {
		return err
	}
	p.Ready.Store(nc)
	if ports := p.Control.Dependencies; ports != nil {
		if ports.Cut != nil {
			view, err := Boot(ctx, p.Endpoint, ports.Cut.Attempt)
			// origin: hot-update S8 plan §5.1 / M08; S8 FIX-AGENTWIRE. The
			// component slot is released only after its completion is durable;
			// an unresolved cut keeps the inherited reservation for recovery.
			if err == nil && current(view.Historical).Phase == upgradetx.PhaseCompleted && view.Historical.Grant.ParentTxID == "" && ports.leaseMu != nil {
				_, release := agentupgradeport.ParentLeaseAccess(&ports.Lease, ports.leaseMu)
				_ = release()
			}
			// origin: S8 FIX-DESCRIBE (real-cut finding; P14/P23 liveness note).
			// Control is already proven and the imported business is serving.
			// An unprovable completion is journaled (recovery_required) and
			// keeps its reservation; ending the session here exited the agent
			// (rc 70) and turned an evidence gap into a business outage. The
			// next session's control-ready retries Boot; a query reports it.
			if err != nil && p.Legacy.Logger != nil {
				p.Legacy.Logger.Error("agent: imported upgrade cut completion is unproven; serving and retaining the reservation for recovery")
			}
			return nil
		}
		return nil
	}
	reported, _ := LegacyReport(p.Legacy, p.Captured)
	CommitLegacyReport(p.Legacy, reported, p.Captured)
	p.RefreshAdmission()
	if p.Image != nil {
		p.Image.MarkReady()
		if drillobserve.Hook != nil {
			drillobserve.Hook(drillobserve.Event{Kind: "mark-ready", Domain: "agent"})
		}
	}
	return nil
}

func DefaultDuration(value, fallback time.Duration) time.Duration {
	if value == 0 {
		return fallback
	}
	return value
}

// SystemEventPort keeps workload and maintenance ownership in the agent while
// the control leaf decides which authenticated event may reach those owners.
// origin: hot-update S8 internal review FIX-AGENTSLIM.
type SystemEventPort struct {
	SID, NID   string
	Logger     *slog.Logger
	Refresh    chan<- struct{}
	Invalidate func()
	Revoke     func(string)
	Reap       func()
	Cancel     func()
}

// HandleSystemEvent dispatches the three event families without owning their
// daemon, workload or discovery lifetimes.
// origin: hot-update S8 internal review FIX-AGENTSLIM.
func HandleSystemEvent(data []byte, p SystemEventPort) {
	var ev struct {
		Type string `json:"type"`
		SID  string `json:"sid"`
		NID  string `json:"nid"`
	}
	if err := json.Unmarshal(data, &ev); err != nil {
		return
	}
	if strings.HasPrefix(ev.Type, "nats_topology_") {
		// Events are only a wake-up hint: refreshRosterOnce still verifies the account-signed
		// roster and its monotone generation. Coalesce a burst from multiple brokers.
		select {
		case p.Refresh <- struct{}{}:
		default:
		}
		return
	}
	if ev.Type == "session_destroyed" && ev.SID == p.SID {
		p.Invalidate()
		// C-10 exec half (hot-update S1): the session that authorized every
		// task here is gone. Kill and tombstone the managed workloads now —
		// a client's late resume must not revive them — but keep the daemon
		// up (see revokeSessionWorkloads for why exiting is the wrong move);
		// the register refusal path retires it. The event carries no NID.
		p.Logger.Info("agent: session destroyed", "sid", ev.SID)
		p.Revoke("session_destroyed")
		return
	}
	if ev.Type != "agent_evicted" {
		return
	}
	if ev.SID != p.SID || ev.NID != p.NID {
		return
	}
	p.Invalidate()
	p.Logger.Info("agent: evicted by admin", "sid", ev.SID, "nid", ev.NID)
	// #26: reap this node's managed OS children BEFORE the daemon self-exits. On a
	// bare setsid-nohup host there is no systemd cgroup to reap them, so an evicted
	// agent's exec/run children would otherwise linger in the host process table.
	p.Reap()
	p.Cancel()
}

// JitterDuration spreads retries after a shared failover.
// origin: hot-update S8 internal review FIX-AGENTSLIM.
// jitterDur returns a full-jitter sleep in (0, d] to de-synchronize a fleet
// retrying after a shared home failover.
func JitterDuration(d time.Duration) time.Duration {
	if d <= 0 {
		return 0
	}
	return time.Duration(mrand.Int64N(int64(d)) + 1)
}
