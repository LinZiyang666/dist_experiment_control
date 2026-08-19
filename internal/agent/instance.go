package agent

import (
	"context"
	"crypto/rand"
	"encoding/base32"
	"encoding/json"
	"fmt"
	"os"
	"strings"
	"sync/atomic"
	"time"

	"github.com/LinZiyang666/tether/internal/proto"
	"github.com/nats-io/nats.go"
)

// Instance identity for cloned-credential deployments.
//
// THE PROBLEM. A tether agent can be baked into a VM/container image and that
// image launched many times at once. Every clone presents the same credential
// and the same agent.yaml nid, so the broker cannot tell "my agent reconnected"
// from "a second process arrived" — and every clone executes every command
// addressed to that name.
//
// WHY NOT A MACHINE FINGERPRINT. Deriving identity from the machine is the
// obvious answer and it is wrong here, because production will contain every
// conceivable virtualization form and each fingerprint has a form that defeats
// it. Two are already falsified on the live fleet: /etc/machine-id is EMPTY in
// the reference pod, and boot_id is per-KERNEL, so every container on one host
// shares it and it does not change across an agent restart either. MAC, IP and
// hostname are shared or absent in as many shapes. So this reads NOTHING about
// the machine: the id is pure crypto/rand.
//
// WHY IT IS NEVER WRITTEN TO DISK. Persisting it would put it inside the image,
// and every clone would then carry the SAME id — recreating the exact ambiguity
// it exists to remove. Not persisting is also what makes the display name
// self-healing: an agent always starts from its agent.yaml basename, so an
// instance that was suffixed re-competes for the basename on its next restart,
// and a device that only ever runs one agent therefore keeps its own name
// permanently.
//
// This matters more than it first looks on the reference deployment, where
// ~/.tether turns out to be a SHARED NFS mount: both instances see one
// state.json at one inode. Any on-disk identity there would not merely be
// copied, it would be actively overwritten by the other process.
//
// WHY IT SURVIVES exec. `node upgrade` re-executes the binary in place
// (syscall.Exec), and so do both rollback paths. A per-process id would make
// the post-exec image a NEW instance that then asks for a lease while its own
// pre-exec self still holds one — i.e. upgrading a node would rename it. So the
// id is a PROCESS LINEAGE, passed through the environment across exec. The
// environment is not on disk and is not part of an image, so the safety
// property above is preserved verbatim.
//
// KNOWN, UNSUPPORTED CASE: a warm clone (RAM snapshot, Firecracker, CRIU,
// container commit) reproduces the environment byte-for-byte and therefore
// reproduces the id.
//
// This is a HARD LIMITATION, not something detected downstream. An earlier
// version of this comment claimed detection; external review F14/Q5 was right
// that the claim is false and dangerous. Two live processes presenting one
// instance id are read by the broker as ONE instance reconnecting — that is
// precisely what the id means — so both are granted the same name and both
// subscribe to it, which is the fan-out this whole mechanism exists to remove.
// Nothing downstream distinguishes them, because by construction there is
// nothing to distinguish.
//
// Deployments that clone a RUNNING process image are therefore out of scope.
// Cloning a stopped image (the case this feature is for) is unaffected: a fresh
// boot has no inherited environment and mints its own id.
//
// NOT A SECURITY BOUNDARY. Every clone shares one nkey and one fingerprint, and
// the CONNECT name is client-controlled. The instance id is a routing and
// correctness token; it is never isolation and never attestation.

const (
	// instanceIDEnv carries the lineage across syscall.Exec. It is read at
	// startup and injected at every exec site.
	instanceIDEnv = "TETHER_INSTANCE_ID"
	// routingNIDEnv carries the currently-adopted routing name across
	// syscall.Exec, so an upgrade of a leased instance re-registers under the
	// name it already holds instead of re-contesting for the basename.
	routingNIDEnv = "TETHER_ROUTING_NID"
)

// instanceIDLen is the character length of an instance id: 16 crypto/rand bytes
// rendered as unpadded lowercase base32.
const instanceIDLen = 26

// lowerBase32 renders bytes with the standard base32 alphabet lowercased and
// without padding, giving [0-9a-z] — subject-safe, and the same character class
// the lowercase ULIDs in internal/proc already use for pids.
var lowerBase32 = base32.NewEncoding("abcdefghijklmnopqrstuvwxyz234567").WithPadding(base32.NoPadding)

// mintInstanceID returns this process's instance id, adopting an inherited
// lineage when one is present and generating a fresh one otherwise.
//
// The inherited value is validated before use: it arrives through the
// environment, which any parent process can set.
func mintInstanceID() (string, error) {
	// NO PROCESS-WIDE CACHE. A round-2 revision memoized this so a boot-time
	// caller and Agent.New would agree; that made every Agent constructed in one
	// process share an id, and two instances sharing an id read to the broker as
	// one instance reconnecting — the fan-out this increment exists to remove.
	// The boot shim no longer needs an id at all (external review F7), so the
	// question is moot and the simple reading stands.
	id, _, err := mintInstanceIDLocked()
	return id, err
}

func mintInstanceIDLocked() (string, bool, error) {
	v := strings.TrimSpace(os.Getenv(instanceIDEnv))
	// CONSUME the variable: once its value is in memory it must not remain in
	// this process's environment, because managed children inherit os.Environ()
	// by default and several spawn paths deliberately leave cmd.Env nil to stay
	// byte-identical to pre-feature behaviour (remote-fs-resilience pins that).
	//
	// Unsetting here is what makes the guarantee total instead of per-call-site:
	// a child cannot inherit what the parent no longer has, so a spawn path
	// added later cannot forget the rule. Without it, a user command that starts
	// an agent (`tether run node -- bash`, then a daemon in that shell) would
	// inherit this process's lineage and the broker would read the two of them
	// as one instance reconnecting — the clone fan-out, restored through the
	// environment.
	//
	// The value is re-injected explicitly at the two syscall.Exec sites (see
	// execEnv), which is the only place it is legitimately handed forward.
	_ = os.Unsetenv(instanceIDEnv)
	if v != "" && len(v) == instanceIDLen && isLowerBase32(v) {
		return v, true, nil
	}
	// A malformed inherited value is discarded rather than propagated: a bad id
	// would be rejected by the broker on every register, and silently running
	// with one is worse than starting a fresh lineage.
	id, err := newInstanceID()
	return id, false, err
}

func newInstanceID() (string, error) {
	var b [16]byte
	if _, err := rand.Read(b[:]); err != nil {
		return "", fmt.Errorf("agent: mint instance id: %w", err)
	}
	return lowerBase32.EncodeToString(b[:]), nil
}

func isLowerBase32(s string) bool {
	for i := 0; i < len(s); i++ {
		c := s[i]
		if (c >= 'a' && c <= 'z') || (c >= '0' && c <= '9') {
			continue
		}
		return false
	}
	return true
}

// execLineage carries what the two syscall.Exec sites must hand forward.
//
// It is PROCESS-level state rather than a field on Agent because one of the two
// sites (realExec in upgrade_state.go) is a package-level function with no
// receiver, and because the thing being modelled genuinely is per-process: a
// process has exactly one instance lineage. Tests never traverse the exec path,
// so a shared value cannot leak between concurrently-constructed Agents there.
var execLineage struct {
	instanceID atomic.Pointer[string]
	routingNID atomic.Pointer[string]
}

// setExecLineage records what a subsequent re-exec must carry forward.
func setExecLineage(instanceID, routingNID string) {
	if instanceID != "" {
		execLineage.instanceID.Store(&instanceID)
	}
	if routingNID != "" {
		execLineage.routingNID.Store(&routingNID)
	}
}

// execEnv returns the environment for a syscall.Exec of this binary: the
// current environment with the instance lineage attached, so the re-executed
// image continues the SAME instance instead of arriving as a stranger and
// contesting the name its own predecessor still holds.
//
// Without this, `node upgrade` would rename every node it upgraded.
func execEnv() []string {
	out := stripInstanceEnv(os.Environ())
	// Fall back to the raw environment when no Agent has recorded a lineage yet.
	//
	// The boot-time upgrade check runs BEFORE Agent.New — a rollback there
	// re-execs from a process that never called mintInstanceID, so execLineage
	// is empty while the inherited variables are still in os.Environ(). Without
	// this fallback the rollback would strip the lineage and never re-add it,
	// and the recovered agent would arrive as a stranger contesting its own
	// name.
	inst := loadedLineage(execLineage.instanceID.Load(), instanceIDEnv)
	route := loadedLineage(execLineage.routingNID.Load(), routingNIDEnv)
	if inst != "" {
		out = append(out, instanceIDEnv+"="+inst)
	}
	if route != "" {
		out = append(out, routingNIDEnv+"="+route)
	}
	return out
}

// loadedLineage prefers a recorded lineage value and falls back to the ambient
// environment, which is the only source available before Agent.New has run.
func loadedLineage(recorded *string, env string) string {
	if recorded != nil && *recorded != "" {
		return *recorded
	}
	return strings.TrimSpace(os.Getenv(env))
}

// nidOf returns the name this agent currently routes under — the lease name
// when one has been adopted, otherwise the agent.yaml basename.
//
// It is a package-level function taking *Agent, not a method, because the
// structural-budget ratchet pins internal/agent.Agent's method count exactly;
// growing a type's surface is precisely what that gate exists to make
// deliberate.
//
// USE THIS FOR ROUTING (subjects, register, heartbeat, events, the tunnel
// REGISTER line). Do NOT use it where the operator's CONFIGURED identity is
// meant: the upgrade marker's commit proof, agent.yaml validation, the
// agent_evicted matcher and the auth-failure hint all deliberately read
// cfg.NID, because a lease name is transient and comparing against it would
// strand a marker or miss an eviction.
func nidOf(a *Agent) string {
	if p := a.routingNID.Load(); p != nil && *p != "" {
		return *p
	}
	return a.cfg.NID
}

// replyClaimProbe answers a lease claim probe with this process's instance id.
//
// The id is what lets the broker distinguish "someone else holds this name"
// from "the thing answering is the very process that is re-registering" — the
// case that would otherwise rename a device on every network blip, because
// nats.go replays subscriptions before it invokes the reconnect handler.
//
// A package-level function, not a method: the structural-budget ratchet pins
// internal/agent.Agent's method count exactly.
func replyClaimProbe(a *Agent, nc *nats.Conn, msg *nats.Msg) {
	if msg.Reply == "" {
		return
	}
	payload, err := json.Marshal(proto.ClaimProbeResp{InstanceID: a.instanceID})
	if err != nil {
		return
	}
	_ = nc.Publish(msg.Reply, payload)
}

// previousNIDOnce returns the name adopted away from, and clears it, so it
// rides exactly one register.
//
// If that register fails the value is gone, and the agent falls back to the
// broker's own belt: a nid with no process history never issues an orphan kill.
// Losing a hint is acceptable; killing live work is not.
func previousNIDOnce(a *Agent) string {
	if p := a.previousNID.Swap(nil); p != nil {
		return *p
	}
	return ""
}

// replayPortsUnlessLeased re-establishes reverse-TCP proxies for any
// port_tokens still in state.json — architecture F.6: an agent restart has the
// tunnel client rebuild its proxies from state.json and re-present the token,
// which the broker reconciles against the token_hash already in
// port_allocations. No-op when the state store or the adapter is absent.
//
// It is skipped entirely when this instance is running under an assigned lease
// name.
//
// A leased instance must NOT replay what it found there: that file belongs to
// whoever holds the basename, and its tokens are valid bearer credentials for
// THAT node's allocations — replaying them dials the incumbent's tunnel and
// takes over its public port, because the tunnel server's session map is keyed
// on the public port alone and the install is an unconditional replace.
//
// The skip is MEMORY-ONLY; nothing is written back. On the reference deployment
// ~/.tether is a SHARED NFS mount (both instances see one inode), so "discard
// and persist" would not prune a private copy — it would destroy the basename
// holder's live state.
//
// It must never fire for a basename-granted agent: the broker leaves ports it
// was not re-presented ALLOCATED and the offline reaper never fires for an
// online node, so a wrong skip would black-hole the port permanently.
func replayPortsUnlessLeased(a *Agent) {
	if nidOf(a) != a.cfg.NID {
		a.cfg.Logger.Info("agent: running under an assigned lease name; not replaying inherited port state",
			"sid", a.cfg.SID, "basename", a.cfg.NID, "routing_nid", nidOf(a))
		return
	}
	a.replayPortsFromState()
}

// leasedInstanceRefusesProxy reports whether this agent must ignore a proxy
// directive because it is running under an assigned lease name.
//
// A LEASED instance never serves as a proxy egress. The broker already
// withholds the directive (nodeParticipatesInProxy folds !LeasedNID), so this
// is the belt: an OLD broker that has not learned the rule would still push
// one, and acting on it would give an ephemeral instance a public egress port
// whose disappearance breaks every subscriber — while the /sub render, which
// gates on proxy_capable, filters it out so nobody would ever use it. It also
// keeps the shared proxy footprint in state.json untouched.
func leasedInstanceRefusesProxy(a *Agent, d *proto.ProxyDirective) bool {
	if d == nil || nidOf(a) == a.cfg.NID {
		return false
	}
	a.cfg.Logger.Info("agent: ignoring a proxy directive while running under an assigned lease name",
		"sid", a.cfg.SID, "basename", a.cfg.NID, "routing_nid", nidOf(a))
	return true
}

// applyLeaseVerdict handles the broker's lease verdict on a register reply and
// reports whether this session must be rebuilt.
//
// ANY non-nil lease terminates the register. Falling through would subscribe on
// the contested basename and replay state.json — exactly what the verdict
// exists to prevent — so the two arms differ only in what they log:
//
//   - an ACCEPTABLE assigned name is adopted, and the session rebuilds under it;
//   - anything else (an EMPTY name, which is the broker's refusal after it
//     PROVED a different live process holds this one; or a malformed/foreign
//     name) is refused, and the session rebuilds under the name it already has.
//
// ADOPTION IS A FULL SESSION REBUILD, never an in-place rename: nats.go runs
// with MaxReconnects(-1) and replays subscriptions on their ORIGINAL subjects
// while re-sending the stored CONNECT name, so mutating the name under a live
// connection would revert on the first network blip — leaving the agent
// publishing under one name and subscribed under another.
//
// The caller returns before its subscribe step, so a refused instance NEVER
// installs a subscription on the basename's forwarded subject: there is no
// window in which two instances both receive the same command.
//
// An OLD broker never sends a lease, and a nil verdict MUST read as legacy mode
// (no rebuild, no retry) — that is what keeps new-agent × old-broker
// byte-identical to today instead of a crash loop.
//
// The rebuild flags are set before returning because the teardown defer derives
// its intent from them: a rebuild that left them unset is torn down as a
// SHUTDOWN, after which a wedged close escalates to os.Exit instead of
// reconnecting — the gotcha #72 shape.
//
// A package-level function taking *Agent: the structural-budget ratchet pins
// this type's method count exactly, and extracting it keeps session() inside
// the maintainability-index gate.
func applyLeaseVerdict(a *Agent, lease *proto.NodeLease) bool {
	if lease == nil {
		resetLeaseRefusals(a)
		return false
	}
	if acceptableLeaseName(a, lease.AssignedNID) {
		presented := nidOf(a)
		adoptRoutingNID(a, lease.AssignedNID)
		a.rebuilding.Store(true)
		a.rebuildRequested.Store(true)
		a.cfg.Logger.Info("agent: name contested, rebuilding session under the assigned lease name",
			"sid", a.cfg.SID, "presented_nid", presented, "assigned_nid", lease.AssignedNID)
		return true
	}
	if lease.AssignedNID == "" {
		a.cfg.Logger.Warn("agent: the broker refused this name — another live process holds it "+
			"and no lease name could be issued (basename too long, or the instance ceiling reached)",
			"sid", a.cfg.SID, "presented_nid", nidOf(a), "basename", lease.Basename)
	} else {
		a.cfg.Logger.Warn("agent: refusing an unacceptable lease name from the broker",
			"sid", a.cfg.SID, "basename", a.cfg.NID, "offered_nid", lease.AssignedNID)
	}
	refusals, wait, giveUp := recordLeaseRefusal(a)
	a.cfg.Logger.Warn("agent: unusable lease verdict counted",
		"sid", a.cfg.SID, "configured_nid", a.cfg.NID,
		"consecutive_refusals", refusals, "backoff", wait.String(), "giving_up", giveUp)
	a.rebuilding.Store(true)
	a.rebuildRequested.Store(true)
	return true
}

// requestLeaseRebuild adopts a broker-assigned lease name and asks the session
// to be rebuilt under it.
//
// It goes through the SAME rebuild protocol every other rebuild path uses —
// CAS `rebuilding`, set `rebuildRequested`, then hand the teardown to the
// session finalizer. Skipping that is not cosmetic: the teardown defer computes
// its intent from `rebuildRequested`, so a rebuild that did not set it is torn
// down as a SHUTDOWN, and a wedged close then escalates to os.Exit instead of
// reconnecting (the gotcha #72 shape).
//
// Returns false when a rebuild is already in flight — the caller must then do
// nothing, because the in-flight rebuild will re-register anyway.
func requestLeaseRebuild(a *Agent, assigned string) {
	if !a.rebuilding.CompareAndSwap(false, true) {
		return
	}
	a.rebuildRequested.Store(true)
	// An EMPTY assigned name means "retire this session and re-compete for the
	// configured name" — the refusal path (external review F3). There is no
	// rename to adopt, but the teardown below is exactly what is needed: the
	// replayed subscriptions of a name this process does not hold must not
	// survive the verdict that said so.
	if assigned == "" {
		a.cfg.Logger.Info("agent: retiring the contested session and re-competing for the configured name",
			"sid", a.cfg.SID, "configured_nid", a.cfg.NID)
	} else {
		adoptRoutingNID(a, assigned)
		a.cfg.Logger.Info("agent: name contested; rebuilding session under the assigned lease name",
			"sid", a.cfg.SID, "basename", a.cfg.NID, "assigned_nid", assigned)
	}
	if fin := a.loadSessionFinalizer(); fin != nil {
		fin.Do(teardownRebuild)
		return
	}
	a.sessCancelMu.Lock()
	cancel := a.sessCancel
	a.sessCancelMu.Unlock()
	if cancel != nil {
		cancel()
		return
	}
	// Nothing to tear down yet. Leaving the flags latched would disarm the NEXT
	// session, so unwind them exactly as fireRedial does.
	a.rebuilding.Store(false)
	a.rebuildRequested.Store(false)
}

// acceptableLeaseName reports whether a broker-supplied lease name is one this
// agent may adopt.
//
// The name arrives over the network and becomes this agent's CONNECT name and
// every subject token it publishes on, so it is validated before use — the
// sibling path that restores a name from the environment already validates, and
// the LESS trusted source must not be the unchecked one. Two conditions:
// it must be a syntactically legal nid (four subject parsers re-validate it),
// and it must be a lease OF THIS AGENT'S OWN BASENAME — a broker has no business
// moving an agent onto an unrelated device's name.
func acceptableLeaseName(a *Agent, assigned string) bool {
	if assigned == "" || assigned == nidOf(a) {
		return false
	}
	if proto.ValidateNID(assigned) != nil {
		return false
	}
	// A DIRECT LEASE OF THE CONFIGURED NAME, nothing else.
	//
	// The earlier rule compared against proto.BasenameOf(a.cfg.NID) so that the
	// agent would agree with a broker that folded `gpu-02` into the `gpu`
	// family. Agreeing was the wrong repair: the two sides then agreed on a name
	// in the WRONG credential family, and the disagreement simply moved to auth
	// — `gpu-03` is not covered by gpu-02's provisioning row, so the reconnect
	// is denied (external review F2). The broker no longer folds; the configured
	// name is an opaque root on both sides, and `gpu-02`'s lease is
	// `gpu-02-02`.
	base, _, leased := proto.SplitLeaseName(assigned)
	if !leased || base != a.cfg.NID {
		return false
	}
	// BOUND THE ADOPTIONS. Every adoption tears down and rebuilds the session,
	// so a broker that keeps assigning a different name each time would spin the
	// session loop as fast as the network allows — measured at ~700 rebuilds per
	// second, which is unbounded goroutine and fd churn dressed up as progress.
	//
	// After the cap the agent stays on the name it has and says so. That is the
	// safe direction: it is still a functioning node, and the suffix is never
	// persisted, so a restart re-competes cleanly.
	return a.leaseAdoptions.Load() < maxLeaseAdoptions
}

// maxLeaseAdoptions caps how many times one agent process will accept a new
// routing name. Three is enough for any legitimate sequence (contest, adopt,
// and one more if the assigned name was itself taken in the meantime) and small
// enough that a misbehaving or flapping broker cannot spin the session loop.
const maxLeaseAdoptions = 3

// leaseRefusalBackoff returns how long to wait before re-competing after a
// verdict this process cannot use, and whether the agent should stop competing
// and stay put.
//
// external review F17. A refusal retires the session (that is the F3 fix — the
// replayed subscriptions of a name this process does not hold must not survive
// the verdict), and the natural consequence is a re-connect. With nothing
// damping it, an instance meeting a full suffix space re-competes at connect
// speed forever: a full auth_callout round per attempt, from every clone in the
// image, aimed at the one broker that just told them all there is no room.
//
// The shape is exponential-with-a-floor and then a STOP. Stopping is the honest
// end state: the answer is not going to change until an operator raises
// MaxInstancesPerBasename or something else exits, and an agent that keeps
// asking is a load generator, not a retry.
func leaseRefusalBackoff(refusals int32) (wait time.Duration, giveUp bool) {
	switch {
	case refusals <= 0:
		return 0, false
	case refusals >= maxLeaseRefusals:
		return 0, true
	default:
		// 2s, 4s, 8s, … capped — the same shape as the register backoff, so the
		// two do not beat against each other.
		wait = time.Duration(1<<uint(refusals-1)) * 2 * time.Second
		if wait > leaseRefusalBackoffMax {
			wait = leaseRefusalBackoffMax
		}
		return wait, false
	}
}

// recordLeaseRefusal advances the ONE refusal budget shared by initial
// register and reconnect-register. It records the next-session policy only;
// callers retire or return from the current session immediately.
func recordLeaseRefusal(a *Agent) (refusals int32, wait time.Duration, giveUp bool) {
	refusals = a.leaseRefusals.Add(1)
	wait, giveUp = leaseRefusalBackoff(refusals)
	if giveUp {
		a.leaseRefusalTerminal.Store(true)
		a.leaseRefusalUntil.Store(0)
		return refusals, wait, true
	}
	if wait > 0 {
		a.leaseRefusalUntil.Store(time.Now().Add(wait).UnixNano())
	}
	return refusals, wait, false
}

func resetLeaseRefusals(a *Agent) {
	a.leaseRefusals.Store(0)
	a.leaseRefusalUntil.Store(0)
	a.leaseRefusalTerminal.Store(false)
}

// maxLeaseRefusals is how many unusable verdicts this process accepts before it
// stops competing. Small: the condition it models (no free suffix, or a broker
// offering names from another family) does not clear on its own.
const maxLeaseRefusals = 5

// leaseRefusalBackoffMax caps the wait so an operator freeing a slot is not
// waiting minutes for anyone to notice.
const leaseRefusalBackoffMax = 60 * time.Second

// dropLease returns this agent to its configured basename.
//
// Called when the adopted name proves unusable — most importantly when auth
// DENIES it, which happens against a broker that predates the suffix fallback
// (a rollback, or one un-upgraded member of the cluster-wide auth_callout queue
// group). connectNATS treats an auth denial as FATAL, so without this the agent
// would re-present the same rejected name on every retry until Run returned and
// the process exited — turning a broker rollback into a fleet-wide agent kill
// instead of a degrade.
func dropLease(a *Agent) {
	if nidOf(a) == a.cfg.NID {
		return
	}
	base := a.cfg.NID
	a.routingNID.Store(&base)
	setExecLineage(a.instanceID, base)
	if s, ok := a.cfg.ExposeAdapter.(nidSetter); ok {
		s.SetNID(base)
	}
	// Back on the basename, so the on-disk state is ours again. Symmetric with
	// the detach in resumeRoutingNID — without it the agent keeps a store that
	// drops every write and returns nothing, and the device silently loses port
	// persistence for the rest of its life.
	if a.stateStore != nil {
		a.stateStore.reattach()
	}
	a.cfg.Logger.Warn("agent: dropping the assigned lease name and falling back to the configured basename",
		"sid", a.cfg.SID, "basename", base)
}

// nidSetter is implemented by an ExposeAdapter whose data-plane identity can be
// retargeted after construction. The production TunnelExposeAdapter does; the
// in-process test adapter need not.
type nidSetter interface{ SetNID(nid string) }

// adoptRoutingNID switches the routing name. It is only ever called between
// sessions — see Agent.routingNID for why an in-place rename inside a live
// connection would silently revert.
//
// It also retargets the tunnel data plane. The control plane alone is not
// enough: the tunnel REGISTER line carries its own copy of the nid, fixed when
// the adapter was constructed (before agent.New), and tunnelTokenLookup
// compares it against the allocation row. An instance that adopted a lease name
// but kept dialling as the basename would present a name that matches the
// INCUMBENT's row and evict it — the exact defect this increment exists to
// close.
func adoptRoutingNID(a *Agent, nid string) {
	a.leaseAdoptions.Add(1)
	// Remember what we were called, for exactly one register: the broker needs
	// it to recognise this agent's OWN running processes, whose rows are filed
	// under the old name. Without it they reach the orphan arm and the broker
	// orders the operator's work killed.
	prev := nidOf(a)
	a.previousNID.Store(&prev)
	resumeRoutingNID(a, nid)
}

// resumeRoutingNID re-enters a lease this PROCESS LINEAGE already held, without
// claiming any history under another name.
//
// It is the re-exec path: `node upgrade` and the rollback paths replace the
// image with syscall.Exec, and the successor inherits the routing name through
// the environment. Everything about the running agent's identity has to be
// restored — the routing name, the lineage it will pass on again, the expose
// adapter, the detached state store — but two things must NOT happen:
//
//  1. NO previousNID. That field says "rows filed under this other name are
//     mine, carried across a rename". A re-exec'd image never registered under
//     any other name, and at this point in Agent.New nidOf(a) is still the
//     BASENAME — so adopting here would tell the broker that the basename's
//     rows belong to this leased instance. They belong to the SIBLING holding
//     the basename: upgrading one instance of a cloned image would close every
//     RUNNING row of another live instance, and once that instance started
//     anything new, its own pre-existing processes would read as orphans and be
//     SIGKILLed. Upgrading device B would destroy work on device A.
//
//  2. NO adoption count. The cap exists so a flapping broker cannot spin the
//     session loop by renaming an agent repeatedly. A re-exec is not a
//     broker-driven rename, and spending a slot here would let three ordinary
//     upgrades exhaust an agent's ability to accept a real lease.
func resumeRoutingNID(a *Agent, nid string) {
	a.routingNID.Store(&nid)
	setExecLineage(a.instanceID, nid)
	if s, ok := a.cfg.ExposeAdapter.(nidSetter); ok {
		s.SetNID(nid)
	}
	// Detach the on-disk state: it belongs to whoever holds the basename, and on
	// a shared home it is the same file. See stateStore.detached — this is the
	// one place that decides it, so no call site has to remember the rule.
	if a.stateStore != nil {
		a.stateStore.detach()
	}
}

// stripInstanceEnv removes the lineage variables from an environment.
//
// This is applied to the environment handed to MANAGED CHILD PROCESSES. Without
// it, a user command that itself starts a tether agent — `tether run node --
// bash`, then launching an agent in that shell — would inherit this agent's
// lineage, and the broker would read the two of them as one instance
// reconnecting and hand them a single name. That is the fan-out defect,
// restored through the environment.
func stripInstanceEnv(env []string) []string {
	out := make([]string, 0, len(env))
	for _, kv := range env {
		if strings.HasPrefix(kv, instanceIDEnv+"=") || strings.HasPrefix(kv, routingNIDEnv+"=") {
			continue
		}
		out = append(out, kv)
	}
	return out
}

// releaseLeaseName tells the broker this process is stopping cleanly and is
// giving its node name back.
//
// WHY THIS EXISTS. Adjudication has to separate two situations that are
// indistinguishable from the broker's side: two clones launched together (the
// second must be suffixed) and one agent restarting quickly (it must keep its
// name). Both present a warm heartbeat, a recently granted holder, and — for
// the few hundred milliseconds before the server reaps the dead process's
// subscription — live interest that nobody answers. The departing process is
// the only party that knows which situation this is, so it says so.
//
// STRICTLY BEST-EFFORT. A crash, a kill -9, a severed link or a wedged teardown
// sends nothing, and adjudication still converges through the grant window, the
// interest probe and the heartbeat clock. Nothing may be built that DEPENDS on
// this arriving. That is also why it is a bare Publish with a short Flush rather
// than a request/reply: a shutdown path must not be able to hang on the network,
// and the caller is already inside a bounded teardown budget.
func releaseLeaseName(a *Agent, nc *nats.Conn) {
	if a == nil || nc == nil || a.instanceID == "" || nc.IsClosed() {
		return
	}
	// ProtoVersion and NID are NOT optional decoration: handleRegister rejects a
	// body whose NID does not match the subject, and rejects a proto epoch that
	// is not exactly this one, both BEFORE any lease logic runs. A farewell that
	// omits them is dropped as nid_mismatch and the name is never released.
	//
	// RosterRefreshOnly is what makes this INERT on a broker that predates the
	// feature, and it is load-bearing.
	//
	// "Additive and omitempty" only means an old broker IGNORES the new key —
	// not that it ignores the message. Without this flag a pre-feature broker
	// decodes the farewell as an ordinary register carrying an EMPTY snapshot,
	// and runs it: node.Register's ON CONFLICT wipes release_version/boot_id,
	// clears proxy_capable, and stamps status=ONLINE with a FRESH heartbeat for
	// a process that is exiting — then reconcileOnRegister sees no local
	// processes and closes every RUNNING row this node has, after which the
	// surviving children come back as pids with no rows, i.e. orphans, and get
	// SIGKILLed. A courtesy message would have destroyed the operator's work.
	//
	// That is not a hypothetical rollback story: this feature exists because the
	// agent is BAKED INTO AN IMAGE, so "brokers first, then agents" is the one
	// ordering these users cannot guarantee.
	//
	// A pre-feature broker short-circuits RosterRefreshOnly as a pure read
	// before touching anything. A current broker checks ReleasingName ahead of
	// that short-circuit, so the flag costs the farewell nothing here.
	body, err := json.Marshal(proto.NodeRegisterReq{
		ProtoVersion:      proto.ProtoVersion,
		NID:               nidOf(a),
		InstanceID:        a.instanceID,
		ReleasingName:     true,
		RosterRefreshOnly: true,
	})
	if err != nil {
		return
	}
	// nidOf, not cfg.NID: an agent that adopted a lease must release the name it
	// actually holds, not the one it was configured with.
	if err := nc.Publish(proto.SubjNodeRegister(a.cfg.SID, nidOf(a)), body); err != nil {
		return
	}
	// Bounded: give the farewell a moment to leave the socket, then stop caring.
	_ = nc.FlushTimeout(leaseReleaseFlush)
}

// leaseReleaseFlush bounds the farewell's time on the shutdown path. It buys the
// publish a chance to reach the wire without letting a stopping agent wait on a
// network that may already be gone.
const leaseReleaseFlush = 250 * time.Millisecond

// awaitLeaseRefusalBackoff waits out the delay owed for an unusable lease
// verdict, returning false if the run context ended first.
//
// The wait lives HERE, between sessions, rather than in the reconnect handler
// that observed the verdict: by the time this runs the contested session has
// already been torn down, so nothing is serving a name this process does not
// hold while it waits (external review F3). Zero when nothing is owed, which is
// every ordinary rebuild.
// A package-level function taking *Agent, not a method: the structural-budget
// ratchet pins this type's method count exactly.
func awaitLeaseRefusalBackoff(ctx context.Context, a *Agent) bool {
	if a.leaseRefusalTerminal.Load() {
		a.cfg.Logger.Error("agent: lease refusal budget exhausted; staying disconnected until restart",
			"sid", a.cfg.SID, "configured_nid", a.cfg.NID,
			"refusals", a.leaseRefusals.Load())
		<-ctx.Done()
		return false
	}
	until := a.leaseRefusalUntil.Swap(0)
	if until == 0 {
		return true
	}
	wait := time.Until(time.Unix(0, until))
	if wait <= 0 {
		return true
	}
	a.cfg.Logger.Info("agent: waiting before competing for a name again",
		"sid", a.cfg.SID, "configured_nid", a.cfg.NID, "wait", wait.String())
	select {
	case <-ctx.Done():
		return false
	case <-time.After(wait):
		return true
	}
}
