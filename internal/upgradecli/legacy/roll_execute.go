// The historical executor remains an explicit legacy adapter. The command's
// production entry now drives target-issued S8 transactions and a sealed plan.
package legacy

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"io"
	"time"

	"github.com/LinZiyang666/tether/internal/cli"
	"github.com/LinZiyang666/tether/internal/clusterupgrade"
	"github.com/LinZiyang666/tether/internal/proto"
)

type ExecutionKeeper interface {
	Stop()
	Lost() bool
	LostErr(string) error
}
type ExecutionPorts struct {
	IO     RollIO
	Health func(context.Context, []string) ([]proto.ClusterHealthResp, error)
	Halt   func(string, string, error) error
	Notify func(string, string, map[string]any)
	Keeper func(context.Context, func(context.Context) (bool, error), io.Writer) ExecutionKeeper
	Broker func(context.Context, clusterupgrade.Step, clusterupgrade.PlanScope, []string, string, string, string, string) error
	Agent  func(context.Context, clusterupgrade.Step, clusterupgrade.PlanScope, string, string, string, string, io.Writer) error
}

func ExecuteRoll(ctx context.Context, out io.Writer, plan clusterupgrade.Plan, toVersion, expectSHA, webhook string, ports ExecutionPorts) error {
	rio := ports.IO
	// origin: hot-update S6 plan §7.1 / T40
	// The holder is random authority, not the lock timestamp or host identity.
	holderBytes := make([]byte, 16)
	if _, err := rand.Read(holderBytes); err != nil {
		return err
	}
	holder := hex.EncodeToString(holderBytes)
	// Choose legacy bytes before acquiring any lock; an old leader cannot verify
	// an extension even when the first target broker has already been upgraded.
	// origin: hot-update S6 internal review rv-cluster-8
	var initialVoters []string
	for _, step := range plan.Steps {
		initialVoters = append(initialVoters, step.NodeID)
	}
	initial, err := ports.Health(ctx, initialVoters)
	if err != nil {
		return err
	}
	if cli.LegacyBrokerRoll(initial) {
		holder = ""
	}
	signedScope := string(plan.Scope)
	if holder == "" {
		signedScope = ""
	}
	scope := plan.Scope
	if scope == "" {
		scope = clusterupgrade.ScopeWholeHost
	}
	var voters []string
	for _, step := range plan.Steps {
		voters = append(voters, step.NodeID)
	}
	ports.Notify(webhook, "start", map[string]any{"to_version": toVersion, "hosts": plan.Upgrades()})
	// External-review B2: take a cluster-scoped roll lock (idempotent UPSERT in replicated cluster_meta) so a
	// concurrent `cluster join`/`cluster retire` cannot cross the rolling restart. The marker is replicated
	// state, so it survives the leadership transfers the roll itself triggers. We release it ONLY on clean
	// completion — a HALT/cancel deliberately LEAVES the MARKER held so membership stays blocked while the
	// cluster sits in a partial-roll state.
	//
	// R7b: the lock is now LEASED. The paragraph above described a lock that a HALT left held forever —
	// "fix and re-run" is only a self-heal while `plan.Upgrades() > 0`, and back then an agentless host had
	// AtTarget structurally false, so the `plan.Upgrades() == 0` self-heal branch was UNREACHABLE there.
	// (P3 has since fixed the root cause: an agentless host now reaches AtTarget on the broker alone, so
	// that branch is reachable again. The lease remains the general answer — it covers every HALT, not just
	// this one.) A HALT used to fence join/retire permanently. The lease closes that: this process keeps
	// renewing for as long as it is driving, and when it stops (HALT, kill -9, snapped SSH) the reaper
	// on the leader releases the lock
	// within one reap interval after LockLeaseTTL. `tether cluster unlock` is the impatient operator's out.
	//
	// WHAT A RE-RUN ACTUALLY MEETS — origin: prerelease audit round 2, H-2. The first
	// paragraph above used to end "a re-run re-acquires (UPSERT) and eventually releases, so
	// the lock self-heals via the documented fix-and-re-run recovery", and that stopped being
	// true when L3-F3 made the acquire refuse a marker it does not own: a re-run was then
	// REFUSED, for LockLeaseTTL plus a reap interval, with a message telling the operator to
	// re-run. The acquire is lease-aware now, and the deferred expire below hands the lease
	// back the moment this process gives up — so the re-run really is admitted immediately,
	// and so is the emergency `--to-version <older>` rollback.
	{
		leader, lerr := RollLeader(ctx, rio)
		if lerr != nil {
			return ports.Halt(webhook, "", lerr)
		}
		lresp, lrerr := rio.Trigger(ctx, &proto.ClusterUpgradeReq{Op: "acquire-lock", TargetNode: leader, Holder: holder, Scope: signedScope})
		if lrerr != nil {
			return ports.Halt(webhook, leader, fmt.Errorf("acquire upgrade lock: %w", lrerr))
		}
		if !lresp.OK {
			return ports.Halt(webhook, leader, fmt.Errorf("acquire upgrade lock refused: %s %s", lresp.Code, lresp.Error))
		}
	}
	// Renew for the whole roll. defer Stop() covers EVERY exit below — each `return haltUpgrade(...)` is a
	// path on which the operator walks away and the lock must decay on its own.
	keeper := ports.Keeper(ctx, func(rctx context.Context) (bool, error) { return RenewRollLease(rctx, rio, holder) }, out)
	// SAY SO ON THE WAY OUT. origin: prerelease audit round 2, H-2.
	//
	// Stopping the renewals is what eventually expires the lease, but "eventually"
	// is LockLeaseTTL — 15 minutes during which the re-run this function's own HALT
	// message instructs the operator to perform was REFUSED, and so was the
	// emergency rollback. An orchestrator that is walking away can simply say so,
	// and then the next roll is admitted at once.
	//
	// The MARKER is untouched: membership stays fenced across the partial roll.
	// Only the roll-versus-roll exclusion is given up, which is the one this
	// process is no longer entitled to hold.
	//
	// Best-effort and idempotent — a pre-existing broker answers "unknown op" and
	// the lease simply times out as it did before. `released` is set on the clean
	// path, where the whole lock is dropped and there is nothing left to expire.
	released := false
	defer func() {
		keeper.Stop()
		if !released {
			ExpireRollLease(ctx, rio, out, holder)
		}
	}()

	canaryChecked := false
	for _, s := range plan.Steps {
		if s.Kind != clusterupgrade.StepUpgrade {
			continue
		}
		stepCtx, cancelStep := context.WithTimeout(ctx, 3*time.Minute)
		cutBytes := make([]byte, 16)
		if _, err := rand.Read(cutBytes); err != nil {
			cancelStep()
			return err
		}
		err := ports.Broker(stepCtx, s, scope, voters, toVersion, expectSHA, holder, hex.EncodeToString(cutBytes))
		cancelStep()
		if err != nil {
			return ports.Halt(webhook, s.NodeID, err)
		}

		if err := ports.Agent(ctx, s, scope, signedScope, toVersion, expectSHA, holder, out); err != nil {
			return ports.Halt(webhook, s.NodeID, err)
		}

		scopeLabel := "broker+agent"
		if scope == clusterupgrade.ScopeBrokerDaemon || s.BrokerOnly {
			scopeLabel = "broker daemon"
		}
		_, _ = fmt.Fprintf(out, "  ✓ %s (%s) at %s\n", s.NodeID, scopeLabel, toVersion)
		ports.Notify(webhook, "host_done", map[string]any{"node": s.NodeID, "version": toVersion})
		if !canaryChecked {
			canaryChecked = true
			if err := CanaryCommandVersion(rio, s.NodeID, out); err != nil {
				return ports.Halt(webhook, s.NodeID, err)
			}
		}
	}
	// R8a P1 — the upgrade verb's rc-semantics gate. Every host now reports the target
	// version, which is a CONTROL-plane fact. A rolling upgrade restarts brokers
	// underneath live reverse tunnels, so it can finish with an expose whose agent never
	// re-confirmed its home — the data plane down while the roll prints success. Ask the
	// LEADER for the agent-confirmed verdict before declaring completion, holding the roll
	// lock throughout (the keeper is still renewing), and refuse rc=0 if it does not come.
	//
	// This mirrors the keeper.Lost() precedent immediately below: "every host reached the
	// target" has never been sufficient for rc=0 in this function.
	if err := WaitHomesConverged(ctx, rio, out); err != nil {
		keeper.Stop()
		releaseErr := ReleaseRollLock(ctx, rio, out, holder)
		released = releaseErr == nil
		ports.Notify(webhook, "halt", map[string]any{"to_version": toVersion, "error": err.Error()})
		return err
	}
	// R7b: stop renewing BEFORE releasing. Not for safety (a renewal cannot re-create a released lock — its
	// statements are gated on the marker existing), but so the release is never racing a renewal that would
	// otherwise land microseconds after it and make the log confusing to read.
	keeper.Stop()
	// External-review B2: clean completion → release the cluster-scoped roll lock so membership resumes.
	if err := ReleaseRollLock(ctx, rio, out, holder); err != nil {
		return err
	}
	released = true
	// R7b: a roll that lost its lock partway through was NOT serialized against other membership changes,
	// even though every host reached the target. That is not an rc=0 outcome.
	if keeper.Lost() {
		ports.Notify(webhook, "halt", map[string]any{"to_version": toVersion, "error": "upgrade lock lost mid-roll"})
		return keeper.LostErr("cluster upgrade rolled every host but LOST its roll lock partway through")
	}
	_, _ = fmt.Fprintln(out, "rolling upgrade complete.")
	ports.Notify(webhook, "complete", map[string]any{"to_version": toVersion})
	return nil
}
