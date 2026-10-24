// transfer_home.go is the D8a (§9) BUILD-AND-PROVE mechanism file for distributing file
// transfer over the cluster: the tier-B replica target seam (xferTargetReplicas) and — added
// in later steps — the home-routing gate and the home-gated orphan reaper filter.
//
// Everything here is INERT in production (cutover=D9): serve.go never calls AttachXferReplicas
// nor the home seam, so xferTargetReplicas() returns jsstream.ReplicasSingle (byte-identical
// R=1 bucket) and the transfer handlers run on whichever broker received the request, exactly
// as today.
//
// EXCLUDED from the TestD8ProductionWiresNoCluster guard scan: this is where the
// `b.xferReplicasFn =` write lives (the guard bans that token from the SCANNED production
// files so the seam can never be wired there).
package broker

import (
	"encoding/json"
	"errors"
	"log/slog"
	"strings"
	"time"

	"github.com/LinZiyang666/tether/internal/brokerimage"
	"github.com/LinZiyang666/tether/internal/cluster"
	"github.com/LinZiyang666/tether/internal/controlbudget"
	"github.com/LinZiyang666/tether/internal/jsstream"
	"github.com/LinZiyang666/tether/internal/proto"
	"github.com/LinZiyang666/tether/internal/schema"
	"github.com/LinZiyang666/tether/internal/xferbucket"
	"github.com/LinZiyang666/tether/internal/xfercontract"
	"github.com/LinZiyang666/tether/internal/xfercontrol"
	"github.com/LinZiyang666/tether/internal/xferforward"
	"github.com/LinZiyang666/tether/internal/xferserve"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

// transferOriginGate borrows the current broker identity for original-owner selection.
func transferOriginGate(b *Broker, origin xfercontract.Origin) bool {
	return b != nil && xferserve.OriginGate(b.clusterMode, b.selfID, origin)
}

// transferNodeGate reads current bindings; the continuation leaf owns liveness policy.
func transferNodeGate(b *Broker, sid, nid string, accepted bool) string {
	return xferserve.NodeGate(b.read().SQL(), b.cfg.Logger, sid, nid, accepted)
}

// xferTargetReplicas returns the replica factor a freshly-created or reconciled OBJ_xfer
// bucket should target. In production (xferReplicasFn==nil, build-and-prove) it is
// jsstream.ReplicasSingle (R=1, byte-identical to the prior literal). When the harness has
// attached the cluster seam it is ReplicasFor(NumVoters) so a completed tier-B object lands
// on a quorum of brokers and survives a home-broker kill at N>=3 (§9 EXIT gate). Reading the
// nil seam is the only production-visible change; this method is called from the scanned
// ensureXferBucket call sites (b.xferTargetReplicas() is not a banned token).
func (b *Broker) xferTargetReplicas() int {
	if b.xferReplicasFn != nil {
		return b.xferReplicasFn()
	}
	return jsstream.ReplicasSingle
}

// AttachXferReplicas wires the D8a tier-B replica seam (TEST/HARNESS ONLY). fn typically
// closes over a cluster.Node: func() int { nv, _ := node.NumVoters(); return
// jsstream.ReplicasFor(nv) }. Call before Run. Production never calls it.
func (b *Broker) AttachXferReplicas(fn func() int) { b.xferReplicasFn = fn }

// transferHomeGate decides whether THIS broker should START a transfer for (sid,nid). In
// production (selfID=="", build-and-prove) it ALWAYS proceeds — the request path is
// byte-identical to today. In clustered mode only the agent's HOME broker proceeds: every
// transfer subject is a plain broadcast subscription (broker.go), so in a routed NATS every
// broker receives every push/pull request — without this gate N brokers would each ensure a
// bucket, register a tracker entry, forward to the agent and emit audit (N-way fan-out, §9).
// A non-home broker returns false; the START handlers then return SILENTLY (no reply) so the
// home broker's answer is the only one ctl sees. Home unresolved/ineligible also returns
// false: no broker can match an absent binding, so every broker stays silent and ctl times
// out + retries until the D6 binding converges (the home_catching_up posture; emitting an
// error from "some" broker is impossible without consensus, §9 D8).
//
// Only the START handlers (push.req / pull.req) consult this gate. The continuation/terminal
// handlers (push-commit / ev.transfer / finalize) are already routed by TRACKER PRESENCE —
// the broker that started the transfer is the only one holding its entry (transfers.get /
// claimFinalize return nothing elsewhere), so a transfer's whole lifecycle stays on its
// origin broker even across a mid-transfer rehome, and per-invocation transfer_ids keep two
// brokers from ever binding the same object (no epoch-fence needed).
func (b *Broker) transferHomeGate(sid, nid string) bool {
	if b.selfID == "" {
		return true // production: inert, byte-identical
	}
	home := b.resolveHomeForAgent(sid, nid)
	if home == nil {
		return false // unresolved/ineligible: stay silent, ctl retries
	}
	return home.NodeID == b.selfID
}

// homeOwnsXferBucket decides whether THIS broker may reap orphan objects from the shared,
// replicated OBJ_xfer-<sid> bucket during boot reconciliation (§9 D8). In production
// (selfID=="", build-and-prove) it is always true — a single broker owns every bucket, so
// the boot reaper's reap-everything behavior is byte-identical. In clustered mode the bucket
// is REPLICATED across brokers, so reaping every object (as the single-broker logic does,
// since the in-memory tracker is empty after boot) would delete a LIVE in-flight object that
// ANOTHER broker — the actual home of its target node — is mid-transferring (its tracker, not
// ours, protects it). To stay safe without per-object attribution (the object name is the
// transfer_id, which does not encode the target nid), we reap a bucket ONLY when self is the
// home of EVERY node currently bound to the session: then no other broker transfers into it,
// and self's empty post-boot tracker means none of self's own objects are resumable (the
// receiver-finalization invariant). A mixed-home or other-homed session is skipped here; a
// leader-driven cross-home GC is D9.
func (b *Broker) homeOwnsXferBucket(sid string) bool {
	if b.selfID == "" {
		return true // production: single broker owns every bucket
	}
	// Collect the session's node ids FIRST, then close the rows BEFORE resolving each home
	// via a nested query: the cluster DB is SetMaxOpenConns(1), so a resolveHomeForAgent
	// issued while `rows` still holds the single connection would DEADLOCK (same hazard
	// homeForRegister guards against).
	rows, err := b.read().Query(`SELECT nid FROM nodes WHERE sid=?`, sid)
	if err != nil {
		return false
	}
	var nids []string
	for rows.Next() {
		var nid string
		if err := rows.Scan(&nid); err != nil {
			_ = rows.Close()
			return false
		}
		nids = append(nids, nid)
	}
	cerr := rows.Err()
	_ = rows.Close()
	if cerr != nil || len(nids) == 0 {
		return false // no bound nodes (or scan error): cannot attribute — skip conservatively
	}
	for _, nid := range nids {
		home := b.resolveHomeForAgent(sid, nid)
		if home == nil || home.NodeID != b.selfID {
			return false // some node is unresolved or homed elsewhere: not ours to reap
		}
	}
	return true
}

// xferBucketOrphanedEverywhere reports whether NO broker can EVER reap the OBJ_xfer-<sid> bucket:
// the session has zero bound nodes, or its bound nodes resolve to no single common home (an unresolved
// node, or two distinct homes = a split-home session). Since homeOwnsXferBucket requires self to be the
// home of EVERY bound node, in each of those cases it returns false on EVERY broker and the bucket is
// immortal (the #58 split-home residual / racknerd small-disk-fill class). A bucket whose bound nodes
// all share ONE home elsewhere is NOT orphaned-everywhere — that broker will reap it. Read-only, used
// only for the N-6 unreapable-bucket gauge; callers gate on cluster mode (selfID!=""). Same collect-
// then-close discipline as homeOwnsXferBucket (SetMaxOpenConns(1)). A read error returns false —
// observability must not fabricate a defect it cannot confirm.
func (b *Broker) xferBucketOrphanedEverywhere(sid string) bool {
	rows, err := b.read().Query(`SELECT nid FROM nodes WHERE sid=?`, sid)
	if err != nil {
		return false
	}
	var nids []string
	for rows.Next() {
		var nid string
		if err := rows.Scan(&nid); err != nil {
			_ = rows.Close()
			return false
		}
		nids = append(nids, nid)
	}
	cerr := rows.Err()
	_ = rows.Close()
	if cerr != nil {
		return false
	}
	if len(nids) == 0 {
		return true // zero-node session bucket: no broker is its home, ever
	}
	commonHome := ""
	for _, nid := range nids {
		home := b.resolveHomeForAgent(sid, nid)
		if home == nil {
			return true // an unresolved node: homeOwnsXferBucket fails on every broker
		}
		if commonHome == "" {
			commonHome = home.NodeID
		} else if home.NodeID != commonHome {
			return true // split home: no single broker owns all bound nodes
		}
	}
	return false // all bound nodes share one home — that broker will reap this bucket
}

// isBroadcastClusterSubject reports whether a subject must stay a plain (broadcast)
// Subscribe even in cluster mode (round-3 BLOCKER). The FILE-TRANSFER subjects are
// broadcast + per-broker in-memory tracker presence + a home-keyed START gate (D8 /
// transfer_home.go): the HOME broker (the tracker holder) must see every push/pull/commit/
// finalize/transfer-event message, so they MUST NOT be queue-grouped (a queue group would
// hand each to a random member with no tracker entry → the transfer hangs). The home gate +
// tracker-presence collapse the broadcast fan-out to one answering broker, so broadcast here
// does not reintroduce the double-reply the queue group fixed for the leader-forwarded writes.
func isBroadcastClusterSubject(subj string) bool {
	// The original owner may be a follower after a rehome. A queue subscriber
	// cannot use its empty local registry as evidence that the transfer is gone.
	// Keep the operation set closed: a wildcard operation subscription would
	// bypass this classifier in both cold and restored business wiring.
	// origin: hot-update S7 plan §6.3 / T39
	tokens := strings.Split(subj, ".")
	if len(tokens) >= 4 && tokens[len(tokens)-4] == "transfer" && tokens[len(tokens)-3] != "" && tokens[len(tokens)-1] == "req" {
		for _, op := range []string{"probe", "permit", "status", "source", "reserve", "terminal", "cancel", "census"} {
			if tokens[len(tokens)-2] == op {
				return true
			}
		}
	}
	for _, leaf := range []string{
		".push.req", ".pull.req", ".push-commit.req",
		".transfer.*.complete", ".transfer.*.failed", ".transfer.*.finalize.req",
		// External-review F3: expose is leader-local (leak-once token) → broadcast +
		// leader-only handler, so it always reaches the leader (never a random follower).
		".expose.req",
		// Correctness-sensitive ingress that reads authoritative session/node/port state
		// before publishing to an agent or freeing a port must run on the leader's view.
		".run.req", ".exec.req", ".kill.req", ".expose-rm.req", ".process-resume.req",
		// C5: proxy CONTROL writes are leader-only (the leak-once subscriber token is minted on the
		// leader + returned in the reply; forwardReply carries no token). sub.list + status stay a
		// queue-group RODB read (any broker).
		".proxy.set.req", ".proxy.sub.create.req", ".proxy.sub.revoke.req",
	} {
		if strings.HasSuffix(subj, leaf) {
			return true
		}
	}
	return false
}

// TransferClusterForwardConfig resolves collaborators for this binding only.
// The caller must join the returned owner before closing its store or replacing
// the coordinator; no global callback table can borrow a previous backend.
// origin: hot-update S7 plan §6.3 / §8.2 / T39
type TransferClusterForwardConfig struct {
	Node    *cluster.Node
	Origin  xfercontract.Origin
	Buckets xfercontract.BucketPort
	Census  func(xfercontract.Origin, controlbudget.Deadline) (xfercontract.LedgerCensus, error)
	Probe   func(controlbudget.Deadline) (xfercontract.ProbeResult, error)
	// Logger receives reply-egress failures (respondLogged); nil discards them.
	Logger *slog.Logger
}

// TransferClusterForward owns callback admission as well as its subscriptions.
// A failed join retains the fenced owner for a later retry or explicit resume.
type TransferClusterForward struct {
	Extra *xferserve.ClusterService
	users brokerimage.StoreAdmission
	subs  []*nats.Subscription
	nc    *nats.Conn
}

// SubscribeTransferClusterForward installs exact plain subscriptions. It is
// used alongside SubscribeClusterApply; the latter excludes these RPCs before
// leader dispatch. Follower origins answer census/probe, and only the current
// leader can enter BucketPort.Apply. That port enforces predecessor retirement
// even if leadership changed while a previous UPDATE remained sent-unknown.
// origin: hot-update S7 plan §2.1 / §6.3 / T39
func SubscribeTransferClusterForward(nc *nats.Conn, cfg TransferClusterForwardConfig) (*TransferClusterForward, error) {
	if nc == nil || cfg.Node == nil || cfg.Origin.BrokerID == "" || cfg.Origin.BrokerID != cfg.Node.SelfID() {
		return nil, errors.New("broker: transfer cluster forwarding identity is missing")
	}
	owner := &TransferClusterForward{nc: nc}
	for _, verb := range []string{VerbTransferBucketConfig, VerbTransferCensus, VerbTransferProbe} {
		sub, err := nc.Subscribe(proto.SubjClusterApply(verb), func(msg *nats.Msg) {
			if msg.Reply == "" || !owner.users.Begin() {
				return
			}
			defer owner.users.End()
			var env forwardEnvelope
			var req transferForwardRequest
			if json.Unmarshal(msg.Data, &env) != nil || env.Verb != verb || json.Unmarshal(env.Payload, &req) != nil {
				return
			}
			if verb == VerbTransferBucketConfig {
				if !cfg.Node.IsLeader() {
					return
				}
			} else if req.Origin.BrokerID != cfg.Origin.BrokerID {
				return // nonowners cannot report unknown, recovering or absence
			}
			reply := dispatchTransferForward(cfg, env, req)
			raw, err := json.Marshal(reply)
			if err == nil {
				// The one broker reply egress (h1 A2): an oversize seal/census
				// reply becomes a typed reply_too_large refusal the forwarding
				// client decodes, never the silent timeout a bare Respond leaves.
				// origin: docs/reviews/h1-plan.md workstream A2; S7 lead integration
				respondLogged(cfg.Logger, nc, msg, raw)
			}
		})
		if err != nil {
			for _, installed := range owner.subs {
				_ = installed.Unsubscribe()
			}
			// Return the partial owner: a callback may already have borrowed the
			// backend before the later SUB failed. The caller still must join it.
			return owner, err
		}
		owner.subs = append(owner.subs, sub)
	}
	return owner, nil
}

// JoinTransferClusterForward closes future admission and observes callback
// completion. It does not unsubscribe on failure: the caller still owns this
// backend and may resume after its refused cut. No per-call waiter is spawned.
// origin: hot-update S7 plan §8.2 / T39
func JoinTransferClusterForward(owner *TransferClusterForward, deadline controlbudget.Deadline) error {
	if owner == nil {
		return errors.New("broker: transfer forwarding join owner is missing")
	}
	return errors.Join(owner.Extra.Join(deadline), owner.users.Fence(deadline))
}

func ResumeTransferClusterForward(owner *TransferClusterForward) {
	if owner != nil {
		owner.Extra.Resume()
		owner.users.Resume()
	}
}

func StopTransferClusterForward(owner *TransferClusterForward, deadline controlbudget.Deadline) error {
	if err := JoinTransferClusterForward(owner, deadline); err != nil {
		return err
	}
	if err := owner.Extra.Stop(deadline); err != nil {
		return err
	}
	var err error
	for _, sub := range owner.subs {
		err = errors.Join(err, sub.Unsubscribe())
	}
	left, remainingErr := controlbudget.Remaining(deadline)
	if remainingErr != nil {
		return errors.Join(err, remainingErr)
	}
	return errors.Join(err, owner.nc.FlushTimeout(left))
}

type transferForwardRequest = xferforward.Request
type transferForwardReply = xferforward.Reply
type transferCensusProof = xferforward.CensusProof

const transferForwardBudget = xferforward.Budget

func isTransferForwardVerb(verb string) bool { return xferforward.IsVerb(verb) }
func dispatchTransferForward(cfg TransferClusterForwardConfig, env forwardEnvelope, req transferForwardRequest) transferForwardReply {
	return xferforward.Dispatch(xferforward.Config{Origin: cfg.Origin, Buckets: cfg.Buckets, Census: cfg.Census, Probe: cfg.Probe}, xferforward.Envelope{ReqID: env.ReqID, Verb: env.Verb, Payload: env.Payload}, req)
}
func transferForwardClient(f *Forwarder) *xferforward.Client {
	if f == nil {
		return nil
	}
	return &xferforward.Client{NC: f.nc, Timeout: f.timeout, Limiter: f.limiter, Observe: f.observeOutcome}
}
func transferForwardRefusal(err error) error {
	var refusal *xferforward.Refusal
	if errors.As(err, &refusal) {
		return &ForwardBusinessError{Kind: refusal.Kind, Msg: refusal.Msg}
	}
	return err
}
func forwardTransferRPC(f *Forwarder, req transferForwardRequest, deadline controlbudget.Deadline) error {
	_, err := xferforward.RPC(transferForwardClient(f), VerbTransferCensus, req, deadline)
	return transferForwardRefusal(err)
}
func ForwardTransferBucketConfig(f *Forwarder, patch xfercontract.ConfigPatch, deadline controlbudget.Deadline) (xfercontract.BucketSeal, error) {
	seal, err := xferforward.Apply(transferForwardClient(f), patch, deadline)
	return seal, transferForwardRefusal(err)
}

type TransferOwnerResolver struct{ Forwarder *Forwarder }

func (r TransferOwnerResolver) Census(origin xfercontract.Origin, deadline controlbudget.Deadline) (xfercontract.LedgerCensus, error) {
	census, err := (xferforward.OwnerResolver{Client: transferForwardClient(r.Forwarder)}).Census(origin, deadline)
	return census, transferForwardRefusal(err)
}
func ProbeTransferBroker(f *Forwarder, origin xfercontract.Origin, deadline controlbudget.Deadline) (xfercontract.ProbeResult, error) {
	probe, err := xferforward.Probe(transferForwardClient(f), origin, deadline)
	return probe, transferForwardRefusal(err)
}

// attachTransferAuditSinkWith is the testable core of AttachTransferAuditSink: it wires the
// sink to an arbitrary forward function (a fake in unit tests; fwd.Forward in production-harness
// wiring). The forward runs in a tracked goroutine (transferAuditWG, drained by
// WaitTransferAudit) so emitTransferAudit never blocks the NATS handler (OQ9-A); a transient
// ErrForwardNotLeader is retried (the derived reqID dedups a double-commit), a permanent error
// returns at once, and the loop ALWAYS terminates (bounded attempts×backoff) — not a leak.
func (b *Broker) attachTransferAuditSinkWith(forward func(payload []byte) error) {
	// Capture this binding's admission owner. A late completion must never
	// decrement the replacement runtime's borrower count after a cut.
	// origin: hot-update S7 plan §8.2 / T39
	auditOwner := b.cl
	// R16 #57: expose the SYNCHRONOUS forward so finalizeStrandedXfers can confirm a commit before deleting
	// the durable ledger (a fire-and-forget emit would lose the evidence in the leaderless post-crash window).
	b.transferAuditForwardSync = func(payload []byte) error {
		if auditOwner != nil {
			if !auditOwner.transferAuditUsers.Begin() {
				return controlbudget.ErrExpired
			}
			defer auditOwner.transferAuditUsers.End()
		}
		return forward(payload)
	}
	runForward := func(payload []byte, rec schema.AuditTransfer, onCommitted func()) {
		for attempt := 0; attempt < transferAuditForwardAttempts; attempt++ {
			ferr := forward(payload)
			if ferr == nil {
				// External review F1: the record is now COMMITTED — this, not "the goroutine started",
				// is when a terminal path may drop its #57 in-flight ledger.
				if onCommitted != nil {
					onCommitted()
				}
				return
			}
			if !errors.Is(ferr, cluster.ErrForwardNotLeader) {
				b.cfg.Logger.Warn("d8: transfer audit forward failed (best-effort)",
					"err", ferr, "tid", rec.TransferID, "kind", rec.Kind)
				return
			}
			time.Sleep(transferAuditForwardBackoff) // transient: retry (reqID dedups a double-commit)
		}
		b.cfg.Logger.Warn("d8: transfer audit forward gave up after retries (best-effort)",
			"tid", rec.TransferID, "kind", rec.Kind)
	}
	b.transferAuditSink = func(rec schema.AuditTransfer, onCommitted func()) {
		payload, err := json.Marshal(rec)
		if err != nil {
			return
		}
		// D9 round-1 MAJOR: during the ordered shutdown forward SYNCHRONOUSLY in the NATS
		// handler so nc.Drain (which waits for handler callbacks) drains this audit too — the
		// async goroutine could otherwise be spawned AFTER WaitTransferAudit returned and lost
		// on Drain. Steady state stays async (OQ9-A: never block the agent-forward handler).
		//
		// audit M7 / xx-concurrency F1: the {check draining + WaitGroup.Add(1)} MUST be
		// indivisible w.r.t. clusterShutdownOrdered's {set draining}, else a callback that read
		// draining==false could be preempted, the shutdown could set draining + WaitTransferAudit
		// (counter 0, returns), and the callback could THEN Add(1)+spawn an un-waited goroutine
		// (leak + lost audit). transferAuditMu guards both sides; a bare atomic cannot.
		b.transferAuditMu.Lock()
		// Count admission before either the synchronous drain branch or Add.
		// Joining only the async WG misses a borrower arriving after draining.
		// Refusal never calls onCommitted: the retained ledger still owns replay.
		// origin: hot-update S7 plan §8.2 / T39
		if auditOwner != nil && !auditOwner.transferAuditUsers.Begin() {
			b.transferAuditMu.Unlock()
			return
		}
		finish := func() {
			if auditOwner != nil {
				auditOwner.transferAuditUsers.End()
			}
		}
		if b.transferAuditDraining.Load() {
			b.transferAuditMu.Unlock()
			defer finish()
			runForward(payload, rec, onCommitted)
			return
		}
		b.transferAuditWG.Add(1)
		b.transferAuditMu.Unlock()
		go func() {
			defer b.transferAuditWG.Done()
			defer finish()
			runForward(payload, rec, onCommitted)
		}()
	}
}

const (
	// These broker-only RPCs share the apply envelope, but never create a
	// Command/OpType or a SQL migration. Config writes belong to the leader's
	// single coordinator; census/probe belong to the explicitly addressed owner.
	// origin: hot-update S7 plan §2.1 / §6.3 / T39
	VerbTransferBucketConfig = "transfer-bucket-config"
	VerbTransferCensus       = "transfer-census"
	VerbTransferProbe        = "transfer-probe"
)

// TransferCode preserves deliberate public refusals across transfer forwarding.
// origin: hot-update S7 plan lead LQ3 / T39
func (e *ForwardBusinessError) TransferCode() string { return e.Kind }

// isLeaderUnavailable reports whether err is the transient "no leader committed this write
// right now" signal (cluster.ErrForwardNotLeader — returned by proposeOrForward on a
// leadership race / election). Handlers in broker.go (which deliberately does NOT import
// internal/cluster — L-2) use this to reply proto.CodeLeaderUnavailable so the client retries
// instead of treating a routine raft failover as a permanent rejection (audit M2 /
// write-forward F3).
func isLeaderUnavailable(err error) bool {
	return errors.Is(err, cluster.ErrForwardNotLeader)
}

// Legacy ensure/raise/converge and publisher observation borrow the very same
// port as continuation. Missing installation is recovery, never a second writer.
// origin: hot-update S7 plan §2.1 / T05
func brokerTransferConfigJS(b *Broker) jetstream.JetStream {
	if current := b.transferRuntime.Load(); current != nil {
		return xferbucket.Bind(brokerJS(b), brokerTransferBuckets(b))
	}
	return xferbucket.Guard(brokerJS(b))
}

// Every outer caller selects today's leader before entering a local port. The
// factory's raw coordinator must never make a follower a full-config writer.
// origin: hot-update S7 plan §2.1 / T05
func brokerTransferBuckets(b *Broker) xfercontract.BucketPort {
	router := &TransferBucketRouter{}
	if current := b.transferRuntime.Load(); current != nil {
		router.Local = current.Buckets
	}
	if b.clusterMode {
		if b.cl == nil || b.cl.node == nil {
			router.Local = nil
			return router
		}
		router.Node, router.Forwarder = b.cl.node, b.cl.forwarder
	}
	return router
}

// TransferBucketRouter owns no configuration snapshots. Only a leader's local
// coordinator can write, and its authority must prove predecessor retirement.
// The outer factory installs one router shared by legacy and current callers.
// origin: hot-update S7 plan §2.1 / T05
type TransferBucketRouter struct {
	Local     xfercontract.BucketPort
	Node      *cluster.Node
	Forwarder *Forwarder
}

func (r *TransferBucketRouter) Apply(p xfercontract.ConfigPatch, d controlbudget.Deadline) (xfercontract.BucketSeal, error) {
	if r.Node != nil && !r.Node.IsLeader() {
		return ForwardTransferBucketConfig(r.Forwarder, p, d)
	}
	if r.Local == nil {
		return xfercontract.BucketSeal{}, &xferbucket.Refusal{Code: xfercontract.CodeOwnerRecovering, Detail: "leader configuration authority is not installed"}
	}
	return r.Local.Apply(p, d)
}
func (r *TransferBucketRouter) Freeze(d controlbudget.Deadline) (xfercontract.ConfigWriterState, error) {
	if r.Local == nil {
		return xfercontract.ConfigWriterState{}, &xferbucket.Refusal{Code: xfercontract.CodeOwnerRecovering, Detail: "configuration retirement authority is not installed"}
	}
	return r.Local.Freeze(d)
}
func (r *TransferBucketRouter) Stop(d controlbudget.Deadline) error {
	if r.Local == nil {
		return nil
	}
	return r.Local.Stop(d)
}

// InstallTransferClusterForward retains the real callback owner even if a later
// subscription failed. JoinStoreUsers must see that partial borrower before SQL
// closes. Installation belongs to the cold/current factory, behind its fence.
// origin: hot-update S7 plan §8.2 / T39
func InstallTransferClusterForward(b *Broker, nc *nats.Conn, cfg TransferClusterForwardConfig) error {
	if b == nil || b.cl == nil || b.cl.transferRPC != nil || b.cl.node != cfg.Node {
		return errors.New("broker: transfer forwarding owner already installed or invalid")
	}
	owner, err := SubscribeTransferClusterForward(nc, cfg)
	b.cl.transferRPC = owner
	return err
}

// brokerTransferVersion declares configured capability independently of boot
// readiness. A graph still starting must never advertise cap0 and send the
// first capable invocation into the one-shot legacy path. The probe remains
// responsible for proving readiness before creation.
// origin: hot-update S7 plan §9 / INTEG-D capability compatibility
func brokerTransferVersion(b *Broker) string {
	if brokerTransferFactory(b) == nil && b.transferRuntime.Load() == nil {
		return ""
	}
	return xfercontract.Version
}

// recordTransferAgent keeps what a NORMAL register advertised: farewell and
// roster refresh return before this call, so neither can install capability.
// An unknown version is recorded as cap0, never as a guessed newer protocol.
// The reply acknowledges acceptance explicitly, independent of release names.
// origin: hot-update S7 plan §6.2 / §8.4 / §9
func recordTransferAgent(b *Broker, sid, nid string, req proto.NodeRegisterReq) string {
	version := req.TransferVersion
	if version != xfercontract.Version {
		version = ""
	}
	b.transferAgents.Record(sid, nid, req.InstanceID, version)
	if version == "" || b.transferRuntime.Load() == nil {
		return ""
	}
	return xfercontract.Version
}

// origin: hot-update S7 plan §6.3 / INTEG-G current runtime binding
func handleBrokerTransferCarrier(b *Broker, msg *nats.Msg, sid, actor, fp, nid, verb string) bool {
	body, handled := xfercontrol.HandleCarrier(msg, sid, actor, fp, nid, verb, privateProcessReply,
		func(origin xfercontract.Origin) bool { return transferOriginGate(b, origin) }, carrierRuntime(b.transferRuntime.Load()))
	if body != nil {
		b.respondBytes(msg, body)
	}
	return handled
}

// origin: hot-update S7 plan §6.3 / INTEG-G current runtime binding
func handleAgentTransferRequest(b *Broker, reg xfercontrol.Registration, msg *nats.Msg) {
	wire := xfercontrol.HandleAgentRequest(reg, msg, func(origin xfercontract.Origin) bool { return transferOriginGate(b, origin) }, b.transferAgents.Lookup, carrierRuntime(b.transferRuntime.Load()))
	if wire != nil {
		b.replyJSON(msg, wire)
	}
}

// origin: hot-update S7 plan §6.3 / INTEG-G current runtime binding
func resolveTransferStatusOrigin(b *Broker, current *BrokerTransfers, req xfercontract.Request, d controlbudget.Deadline) (xfercontract.Request, bool) {
	// Settled entries can retire their ledger row while their result is retained.
	// The durable permit directory resolves their full identity on single brokers
	// too; a ledger-only absence fed a partial identity to the exact tracker lookup.
	// origin: S7 internal review MAKETEST-TRANSFER F7; S7 plan §6.1 / T28
	if current != nil && current.Assembly != nil && req.Origin == (xfercontract.Origin{}) && req.Op == xfercontract.OpStatus {
		return xferserve.ResolveOrigin(current.Assembly.Owners, req, d)
	}
	return xfercontrol.ResolveStatusOrigin(b.clusterMode || b.selfID != "", carrierRuntime(current), req, d)
}

// origin: hot-update S7 plan §6.3 / INTEG-G current runtime binding
func attachTransferEvidence(current *BrokerTransfers, req xfercontract.Request, resp xfercontract.Response, wire any, d controlbudget.Deadline) any {
	return xfercontrol.AttachEvidence(carrierRuntime(current), req, resp, wire, d)
}

func carrierRuntime(current *BrokerTransfers) *xfercontrol.CarrierRuntime {
	if current == nil {
		return nil
	}
	return &xfercontrol.CarrierRuntime{Origin: current.Origin, Bindings: current.Bindings, Controller: current.Controller, Ledger: current.Ledger}
}

func brokerTransferVerifier(b *Broker) xfercontract.SettlementVerifier {
	return xferserve.CurrentSettlement(func() xfercontract.SettlementVerifier {
		if g := b.transferRuntime.Load(); g != nil && g.Assembly != nil {
			return g.Assembly.Reconnect
		}
		return b.cfg.AuthCallout.Settlement
	})
}
