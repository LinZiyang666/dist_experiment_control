// Concrete bootstrap and transaction request ports keep the Agent owner thin.
// Missing dependencies refuse work and never advertise hot upgrade capability.
// origin: hot-update S8 plan §2.2 / §3.3 / §11.2 P14.
package agentupgrade

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"sync"
	"time"

	"github.com/LinZiyang666/tether/internal/agentupgradeport"
	"github.com/LinZiyang666/tether/internal/upgradecut"
	"github.com/LinZiyang666/tether/internal/upgradelock"
	"github.com/LinZiyang666/tether/internal/upgradeobserve"
	"github.com/LinZiyang666/tether/internal/upgradeport"
	"github.com/LinZiyang666/tether/internal/upgradetx"
	"github.com/LinZiyang666/tether/internal/upgradewire"

	"github.com/nats-io/nats.go"
)

// Dependencies is the S8-side wiring port. The bootstrap supplies the
// canonical store and validated adjunct; the wire owner supplies its strict
// decoder. Missing ports confer no capability and cannot authorize an effect.
// origin: hot-update S8 plan §2.2 / §11.2 P14.
type Dependencies struct {
	Key              upgradetx.Key
	Store            upgradetx.Store
	Runtime          upgradeport.RuntimePort
	Registry         upgradeobserve.Registry
	HandleWire       func(context.Context, upgradeport.Endpoint, upgradetx.Key, []byte) ([]byte, error)
	ControlProbe     func(context.Context, string) (*nats.Msg, error)
	Cut              *upgradecut.State
	WatchdogFence    upgradetx.Fence
	WatchdogDeadline int64
	WatchdogReverse  upgradetx.ReverseRequest
	// Lease is the adopted domain reservation once activation committed: the
	// commit owner closes it only after the cut's completion is durable.
	// origin: hot-update S8 plan §5.1 / M08; S8 FIX-AGENTWIRE
	Lease upgradelock.Lease
	// Parent couriers may observe and release one host concurrently.
	// origin: S8 internal review SIGNED-ROLL-FIXTURES F3; S8 plan §5.1
	leaseMu *sync.Mutex
	// Open is the owner-late journal opener. The journal's owner (the grant
	// issuer Preflight checks) is derived from the lineage instance ID that
	// agent.New mints or inherits, so the canonical boot in cmd can name the
	// registered Key and journal but cannot open the store itself. The agent
	// calls it once, before a carried cut is verified against that journal.
	// origin: hot-update S8 plan §3.1 / §5.1; S8 FIX-DESCRIBE (P19 request R3)
	Open func(owner upgradetx.Owner) (upgradetx.Store, error)
}

// AcquireCurrentLease binds lease access to this dependency's configured key.
// origin: S8 internal review TAGGED-SUITE-SWEEP-A F4 / hot-update S8 plan §5.1
func AcquireCurrentLease(ctx context.Context, ports *Dependencies, key upgradetx.Key) (upgradelock.Lease, error) {
	if ports == nil || key != ports.Key {
		return nil, &upgradetx.Error{Code: upgradetx.CodeDeferred, Detail: agentupgradeport.ReasonDomainLeaseHeld}
	}
	return agentupgradeport.AcquireRuntimeLease(ctx, key, ports.Store, ports.Cut, &ports.Lease, ports.leaseMu)
}

// Bind clones the validated adjunct before restore, preserving its authority
// and reverse scope independently of caller-owned slices and maps.
func Bind(ports *Dependencies, sid, nid string, uid uint32) (*Dependencies, upgradeport.Endpoint, string, error) {
	if ports == nil {
		return nil, nil, "", nil
	}
	if ports.Key.Component != upgradetx.ComponentAgent || ports.Key.SID != sid || ports.Key.ConfiguredNID != nid || ports.Key.UID != uid {
		return nil, nil, "", &upgradetx.Error{Code: upgradetx.CodeRejected, Detail: "agent upgrade key does not match its configured identity"}
	}
	if ports.Store == nil || ports.Runtime == nil || ports.Registry == nil {
		return nil, nil, "", &upgradetx.Error{Code: upgradetx.CodeNotWired, Detail: "agent transaction adapters are unavailable"}
	}
	copy := *ports
	copy.leaseMu = &sync.Mutex{}
	if copy.HandleWire == nil {
		copy.HandleWire = HandleWire
	}
	incarnation := ""
	if ports.Cut != nil {
		data, err := upgradecut.Encode(*ports.Cut)
		if err != nil {
			return nil, nil, "", err
		}
		cut, err := upgradecut.Decode(data)
		if err != nil || cut.Attempt.Key != ports.Key {
			return nil, nil, "", &upgradetx.Error{Code: upgradetx.CodeRejected, Detail: "agent upgrade cut does not match its configured identity"}
		}
		copy.Cut = &cut
		copy.WatchdogReverse.Scope = cut.Scope
		incarnation = cut.Authorities[0].Incarnation
	}
	agentupgradeport.BindInheritedLease(copy.Runtime, &copy.Lease, copy.leaseMu)
	return &copy, New(copy.Store, copy.Runtime, copy.Registry), incarnation, nil
}

func activatingRefusal(ports *Dependencies) []byte {
	if ports == nil {
		return nil
	}
	return agentupgradeport.ActivatingRefusal(ports.Runtime)
}

// ReplyWire runs under a joined request whose context and endpoint were captured
// before dispatch. A rebuilt session cannot lend its authority to a queued call.
// The strict decoder belongs to P13. Its absence is a refusal, never a permissive
// json.Unmarshal fallback. Broker admission already authenticates the actor;
// this adapter additionally binds every operation to the configured component.
// origin: hot-update S8 plan §2.2 / §7.2 / §11.2 P14.
func ReplyWire(ctx context.Context, ports *Dependencies, endpoint upgradeport.Endpoint, nc *nats.Conn, msg *nats.Msg) {
	// origin: S8 FIX-DESCRIBE — an activating Start answers before its exec.
	ctx, reply := agentupgradeport.ReplyOnce(ctx, nc, msg.Reply)
	// A request reaching a process about to exec is refused at once, never
	// queued behind the activating Start (agentupgradeport.ActivatingRefusal).
	if refusal := activatingRefusal(ports); refusal != nil {
		reply(refusal)
		return
	}
	var body []byte
	var err error
	if ports == nil || ports.HandleWire == nil || endpoint == nil || ctx == nil {
		err = &upgradetx.Error{Code: upgradetx.CodeNotWired, Detail: "agent transaction adapters are unavailable"}
	} else {
		if isRollForwarded(msg.Data) {
			body, err = replyRoll(ctx, ports, endpoint, msg.Data)
		} else {
			body, err = ports.HandleWire(ctx, endpoint, ports.Key, msg.Data)
		}
	}
	if err == nil && len(body) == 0 {
		err = errors.New("agent: wire adapter returned no evidence")
	}
	if err != nil {
		code := upgradetx.CodeUnsettled
		var typed *upgradetx.Error
		if errors.As(err, &typed) {
			code = typed.Code
		}
		body, _ = json.Marshal(map[string]string{"code": code, "error": "agent transaction evidence is unresolved"})
	}
	reply(body)
}

// ArmWatchdog is joined by Run's cancellation funnel, using the original
// absolute deadline and fence instead of looking up whichever Tx is Current.
// P17 captures the fence and absolute deadline from the accepted journal
// while adopting this adjunct. A late timer never loads another Current.
// origin: hot-update S8 plan §3.3 / M11.
func ArmWatchdog(ctx context.Context, ports *Dependencies, endpoint upgradeport.Endpoint, incarnation string, joined *sync.WaitGroup, logger *slog.Logger) bool {
	if ports == nil {
		return false
	}
	if ports.Cut == nil {
		return true
	}
	if agentupgradeport.IsReverseCut(ports.Store, *ports.Cut) {
		return true
	}
	fence, reverse, deadline := ports.WatchdogFence, ports.WatchdogReverse, ports.WatchdogDeadline
	if !upgradetx.SameCut(fence.Current, ports.Cut.Attempt) || fence.Owner != ports.Cut.Owner || fence.RunningSHA != ports.Cut.TargetSHA || fence.Incarnation != incarnation {
		logger.Error("agent: transaction watchdog fence does not match the inherited cut")
		return true
	}
	joined.Add(1)
	go func() {
		defer joined.Done()
		if _, err := Watchdog(ctx, endpoint, fence, deadline, reverse); err != nil && ctx.Err() == nil {
			logger.Error("agent: transaction watchdog retained unresolved evidence")
		}
	}()
	return true
}

// A running S8 image never falls back to a v1 prev rename or an old snapshot.
// The endpoint independently verifies this captured cut against current running
// ownership, exports current state and creates an independent reverse attempt.
// origin: hot-update S8 plan §3.3 / §8.1 / M11 / M12.
func RecoverCurrent(ctx context.Context, ports *Dependencies, endpoint upgradeport.Endpoint, logger *slog.Logger) {
	if ports == nil || ports.Cut == nil || !upgradetx.SameCut(ports.WatchdogFence.Current, ports.Cut.Attempt) {
		return
	}
	if _, err := Watchdog(ctx, endpoint, ports.WatchdogFence, time.Now().UnixNano(), ports.WatchdogReverse); err != nil {
		logger.Error("agent: current transaction recovery retained unresolved evidence")
	}
}

// HandleWire uses P13's strict, authenticated forwarded envelope and response
// projection. The caller selects the canonical endpoint and configured key.
// origin: hot-update S8 plan §2.2 / §7.2 / §11.2 P14.
func HandleWire(ctx context.Context, endpoint upgradeport.Endpoint, key upgradetx.Key, body []byte) ([]byte, error) {
	return upgradewire.EncodeResponse(upgradewire.HandleForwarded(ctx, endpoint, key, body))
}
