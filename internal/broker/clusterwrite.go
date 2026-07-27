package broker

// clusterwrite.go — D9 step 3: the authoritative-write router. The whole point of the
// epic is that in cluster mode the FSM (not the broker) is the single writer, so the
// broker must route every authoritative control-state write through raft instead of
// `b.cfg.DB.Exec`. In single mode none of this runs — the broker keeps calling the
// direct mutators byte-identically (the D2 N=1 equivalence floor).
//
// The mechanism is exactly the D4 forward wire generalized: leader runs the Plan
// locally via Propose (leader-authoritative — the Plan reads the leader's committed
// view); a follower forwards the request to the leader over cluster.apply.<verb> so
// the leader runs the Plan. This is the same shape as D4's NewProvisionSeam /
// NewJoinSeam; D9 extends it to the session/port/home control verbs.

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/LinZiyang666/tether/internal/cluster"
	"github.com/LinZiyang666/tether/internal/clusternodes"
	"github.com/LinZiyang666/tether/internal/jsstream"
	"github.com/LinZiyang666/tether/internal/node"
	"github.com/LinZiyang666/tether/internal/port"
	"github.com/LinZiyang666/tether/internal/proc"
	"github.com/LinZiyang666/tether/internal/proto"
	"github.com/LinZiyang666/tether/internal/session"
	"github.com/LinZiyang666/tether/internal/tunnel"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

// ctlQueueGroup is the shared NATS queue group the ctl-command + event subscriptions join
// in CLUSTER mode, so each command/event is handled by EXACTLY ONE broker (which then
// forwards/proposes through raft) instead of every broker handling it (the broadcast
// dual-handling that races replies). Single mode uses a plain Subscribe (byte-equivalent).
const ctlQueueGroup = "tether-broker-ctl"

type clusterTunnelClosePayload = tunnel.SessionInfo

// isBroadcastClusterSubject reports whether a subject must stay a plain (broadcast)
// Subscribe even in cluster mode (round-3 BLOCKER). The FILE-TRANSFER subjects are
// broadcast + per-broker in-memory tracker presence + a home-keyed START gate (D8 /
// transfer_home.go): the HOME broker (the tracker holder) must see every push/pull/commit/
// finalize/transfer-event message, so they MUST NOT be queue-grouped (a queue group would
// hand each to a random member with no tracker entry → the transfer hangs). The home gate +
// tracker-presence collapse the broadcast fan-out to one answering broker, so broadcast here
// does not reintroduce the double-reply the queue group fixed for the leader-forwarded writes.
func isBroadcastClusterSubject(subj string) bool {
	for _, leaf := range []string{
		".push.req", ".pull.req", ".push-commit.req",
		".transfer.*.complete", ".transfer.*.failed", ".transfer.*.finalize.req",
		// External-review F3: expose is leader-local (leak-once token) → broadcast +
		// leader-only handler, so it always reaches the leader (never a random follower).
		".expose.req",
		// Correctness-sensitive ingress that reads authoritative session/node/port state
		// before publishing to an agent or freeing a port must run on the leader's view.
		".run.req", ".exec.req", ".kill.req", ".expose-rm.req",
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

func (b *Broker) isClusterFollower() bool {
	return b.clusterMode && b.cl != nil && b.cl.node != nil && !b.cl.node.IsLeader()
}

// clusterRuntime holds the cluster-mode state. It is built by Run when clusterMode is
// true and is nil in single mode. Keeping it a broker-package type lets broker.go hold
// a single `cl *clusterRuntime` field without importing internal/cluster directly.
type clusterRuntime struct {
	// node is the raft state layer (owns the sole WAL DB writer).
	node *cluster.Node
	// forwarder is the D4 follower→leader cluster.apply.<verb> client.
	forwarder *Forwarder
	// subs are the cluster responders (ClusterApply / ClusterHealth / AlertLs /
	// AlertAck), unsubscribed in the ordered shutdown (step 5).
	subs []*nats.Subscription
	// auditPub + alertRec are the leader-gated loops; they poll IsLeader() so the
	// goroutine count is constant across leadership flaps.
	// auditPub is written once by wireClusterLate and read afterwards by, among
	// others, the /metrics HTTP goroutine (metrics_wire.go). Batch-A review M6:
	// that read was unsynchronised against the write — a real data race with a
	// seconds-wide window (wireClusterLate does JetStream probing and several
	// reconciles between broker start and this assignment), which a 15s scrape
	// interval can land inside. atomic.Pointer keeps the read lock-free.
	auditPub atomic.Pointer[AuditPublisher]
	alertRec *AlertReconciler
	// cancel stops the leader-gated loops (a child of Run's ctx); loopDone receives one
	// signal per loop on exit so the ordered shutdown can JOIN them (not guess via defer).
	cancel func()
	loops  *loopSet
	// admin is the D7 membership orchestrator wired into the adminsock cluster backend;
	// kept here so the d9_integration harness can drive `cluster add` (AddNode) directly.
	admin *ClusterAdmin
	// fsArm tracks the online force-single sustained-quorum-loss dwell + arm token. Shared between the
	// (non-leader-gated) observe tick that feeds it and the adminsock backend handlers that read it.
	fsArm *forceSingleArm
	// topoSelf (C3) is this broker's latest topology-reconcile self-report (applied/observed/reason),
	// published by the per-broker reconcile loop and read by the status/health responders. atomic so
	// the responder (other goroutine) reads it race-free; nil until the first reconcile pass.
	topoSelf atomic.Pointer[topoSelfReport]
}

// topoSelfReport is one broker's reconcile state, read by the status/health responders (C3 §2.7).
type topoSelfReport struct {
	Applied  uint64
	Observed uint64
	Reason   string
}

// ClusterAdminForTest exposes the membership orchestrator for the d9_integration harness
// (driving a join in a 2-broker forward-path drill). TEST-ONLY, like ClusterStateForTest.
func (b *Broker) ClusterAdminForTest() *ClusterAdmin {
	if b.cl == nil {
		return nil
	}
	return b.cl.admin
}

// clusterShutdownOrdered runs the explicit D9 cluster shutdown sequence (step 5) — NOT a
// LIFO-defer guess. Order is load-bearing: drain the async transfer-audit forwards WHILE
// the NATS conn is still live (a publish-after-Drain would SILENTLY DROP an audit record
// — a loss the leak gate cannot catch), THEN stop + join the leader-gated loops, THEN
// unsubscribe the responders. The caller invokes this on ctx.Done() BEFORE returning, so
// the surviving defers (nc.Drain, then node.Shutdown) run after — in that order.
func (b *Broker) clusterShutdownOrdered() {
	if b.cl == nil {
		return
	}
	// 1. Flip the transfer-audit sink to SYNCHRONOUS (round-1 MAJOR), THEN drain the
	// already-spawned async forwards. After this, any transfer event arriving during the
	// remaining shutdown forwards inline in its NATS handler — so nc.Drain (a later defer)
	// waits for it instead of it racing in a goroutine spawned after this Wait returned.
	// audit M7 / xx-concurrency F1: set draining UNDER transferAuditMu so the sink's
	// {check draining + WaitGroup.Add} is indivisible w.r.t. this set — no Add can land
	// after WaitTransferAudit observes a zero counter.
	b.transferAuditMu.Lock()
	b.transferAuditDraining.Store(true)
	b.transferAuditMu.Unlock()
	b.WaitTransferAudit()
	// 2. Stop + join the leader-gated loops (bounded; they poll ctx between iterations).
	if b.cl.cancel != nil {
		b.cl.cancel()
	}
	if b.cl.loops != nil {
		b.cl.loops.Join(10*time.Second, b.cfg.Logger)
	}
	// 3. Unsubscribe the responders (stop accepting new forwarded writes / RPCs).
	for _, s := range b.cl.subs {
		_ = s.Unsubscribe()
	}
}

// wireClusterEarly loads this node's STABLE tunnel cert from the secrets dir and
// attaches the D6 home seam (selfID + cert). It MUST run before Run creates the tunnel
// server (so newTunnelServer picks NewServerWithCert with the stable cert and the
// advertised cert_fp matches cluster_nodes.cert_fp).
func (b *Broker) wireClusterEarly() error {
	cert, gotFP, err := b.loadStableTunnelCert()
	if err != nil {
		return err
	}
	self, err := clusternodes.LookupByNodeID(b.cl.node.RODB(), b.cl.node.SelfID())
	if err != nil {
		return fmt.Errorf("broker: read seeded cert_fp for self %q: %w", b.cl.node.SelfID(), err)
	}
	if !tunnelCertMatchesPinned(gotFP, self, b.cfg.Now()) {
		return tunnelCertPinMismatchError(gotFP, self, b.cl.node.SelfID(), b.cfg.ClusterSecretsDir)
	}
	b.AttachClusterSeam(b.cl.node.SelfID(), cert)
	return nil
}

// tunnelCertPinMismatchError composes the wireClusterEarly fail-closed error (R11 P12/DOC-23). It is
// a named helper so the RECOVERY WORDING is unit-testable and refactor-proof. Critically, this path
// runs BEFORE the admin socket exists (the daemon exits here), so the remedy must be a FILE-level
// restore — it must NOT point at `tether cluster rotate-tunnel-cert`, which dials the admin socket
// that is never up in this bricked state (the old text offered that command and dead-ended the
// operator).
func tunnelCertPinMismatchError(gotFP string, self *clusternodes.HomeNode, selfID, secretsDir string) error {
	return fmt.Errorf("broker: on-disk tunnel cert fingerprint %q matches neither the pinned "+
		"cluster_nodes.cert_fp %q nor an unexpired cert_fp_prev %q for node %q — agents pin against those and "+
		"would reject EVERY dial (silent data-plane outage). This fail-closes BEFORE the admin socket is up, "+
		"so the fix is a FILE-level restore, NOT a CLI command: put the PREVIOUS %s + %s back under %s (the "+
		"pinned cert/key) so the on-disk fingerprint matches the pin again, then restart the broker",
		gotFP, self.CertFP, self.CertFPPrev, selfID, secretTunnelCert, secretTunnelKey, secretsDir)
}

func (b *Broker) loadStableTunnelCert() (*tls.Certificate, string, error) {
	cert, fp, err := loadStableTunnelCertFrom(b.cfg.ClusterSecretsDir)
	return cert, fp, err
}

func loadStableTunnelCertFrom(dir string) (*tls.Certificate, string, error) {
	certPEM, err := os.ReadFile(filepath.Join(dir, secretTunnelCert))
	if err != nil {
		return nil, "", fmt.Errorf("broker: read stable tunnel cert: %w", err)
	}
	keyPEM, err := os.ReadFile(filepath.Join(dir, secretTunnelKey))
	if err != nil {
		return nil, "", fmt.Errorf("broker: read stable tunnel key: %w", err)
	}
	cert, err := tunnel.LoadServerCert(string(certPEM), string(keyPEM))
	if err != nil {
		return nil, "", fmt.Errorf("broker: load stable tunnel cert: %w", err)
	}
	leaf, err := x509.ParseCertificate(cert.Certificate[0])
	if err != nil {
		return nil, "", fmt.Errorf("broker: parse stable tunnel cert leaf: %w", err)
	}
	return cert, tunnel.CertFingerprint(leaf), nil
}

func loadOptionalStableTunnelCert(dir string) (*tls.Certificate, bool, error) {
	certPath := filepath.Join(dir, secretTunnelCert)
	keyPath := filepath.Join(dir, secretTunnelKey)
	_, certErr := os.Stat(certPath)
	_, keyErr := os.Stat(keyPath)
	if errors.Is(certErr, os.ErrNotExist) && errors.Is(keyErr, os.ErrNotExist) {
		return nil, false, nil
	}
	if certErr != nil || keyErr != nil {
		return nil, false, fmt.Errorf("broker: stable tunnel certificate pair is incomplete: cert=%v key=%v", certErr, keyErr)
	}
	cert, _, err := loadStableTunnelCertFrom(dir)
	if err != nil {
		return nil, false, err
	}
	return cert, true, nil
}

func (b *Broker) prepareTunnelCertRotate(newFP string) (func(), error) {
	srv := b.tunnelSrv.Load()
	if srv == nil {
		return nil, fmt.Errorf("live tunnel server is not running on this broker")
	}
	cert, gotFP, err := b.loadStableTunnelCert()
	if err != nil {
		return nil, err
	}
	if gotFP != newFP {
		return nil, fmt.Errorf("on-disk tunnel cert fingerprint %q does not match requested --cert-fp %q", gotFP, newFP)
	}
	return func() {
		b.tunnelCert = cert
		srv.SetCertificate(cert)
	}, nil
}

func tunnelCertMatchesPinned(gotFP string, self *clusternodes.HomeNode, now time.Time) bool {
	if self == nil || gotFP == "" {
		return false
	}
	if gotFP == self.CertFP {
		return true
	}
	return self.CertFPPrev != "" &&
		gotFP == self.CertFPPrev &&
		self.CertValid != nil &&
		now.Before(*self.CertValid)
}

// wireClusterLate attaches the D8 write/forward sinks, subscribes the cluster
// responders, and starts the leader-gated loops. It runs AFTER the NATS connect + JS
// probe (the loops need b.js). The loops are tied to ctx (Run's context) so a cancel
// stops them; step 5 adds the explicit ordered shutdown (drain forwards before
// nc.Drain, unsubscribe responders, node.Shutdown).
func (b *Broker) wireClusterLate(ctx context.Context, nc *nats.Conn) error {
	node := b.cl.node
	// The forwarder was built in Run (before installAuthCallout) so the PIN seams could use
	// it; reuse it here (a second one would be a redundant client). Fall back if unset.
	fwd := b.cl.forwarder
	if fwd == nil {
		fwd = b.newForwarder(nc, b.cfg.ExposeForwardTimeout())
		b.cl.forwarder = fwd
	}

	// D8a/D8b seams: transfer audit + tier-B replicas + disk-pressure alerts route
	// through the leader (the sinks were nil in single mode = byte-identical).
	b.AttachTransferAuditSink(fwd)
	b.AttachXferReplicas(func() int {
		v, err := node.NumVoters()
		if err != nil || v <= 0 {
			return jsstream.ReplicasSingle
		}
		return jsstream.ReplicasFor(v)
	})
	b.AttachAlertSink(fwd)

	// C3-B2: a broker reports topology ONLY when it actually manages a live nats.conf. Otherwise the
	// health gate would mark every grown cluster DEGRADED forever (TopoReported=true, Observed=0,
	// never converging because the loop never runs). nil topoSelf ⇒ TopoReported=false (the documented
	// "inert ⇒ reports nothing" contract), so a cluster-mode misconfig fails loud, not silently wedges.
	var topoSelf func() *topoSelfReport
	if b.cfg.NatsConfPath != "" {
		topoSelf = b.topoSelfReport
	}

	// D4 write-forward responder + D8b health/alert responders. Each stays silent on a
	// follower (the leader answers); collected for ordered unsubscribe.
	subscribers := []func() (*nats.Subscription, error){
		func() (*nats.Subscription, error) { return SubscribeClusterApply(nc, node, b.cfg.Now) },
		func() (*nats.Subscription, error) { return b.subscribeTunnelClose(nc) },
		func() (*nats.Subscription, error) {
			return SubscribeClusterHealth(nc, node, b.cfg.DB, b.cfg.Now, topoSelf, b.jsUnavail.Load, b.cfg.ColocatedAgentNID, b.accountPubOrEmpty)
		},
		func() (*nats.Subscription, error) { return SubscribeClusterRosterPull(nc, b.manifestBytes) }, // G3 #17
		func() (*nats.Subscription, error) { return b.SubscribeClusterUpgradeTrigger(nc) },            // G5 #13 W2b
		func() (*nats.Subscription, error) { return b.SubscribeClusterGrowTrigger(nc) },               // G4 §B grow trigger
		func() (*nats.Subscription, error) {
			return SubscribeClusterCursor(nc, node, b.cfg.DB, b.cfg.Now, topoSelf, b.jsUnavail.Load, b.cfg.ColocatedAgentNID, b.accountPubOrEmpty)
		},
		// R8a P1: the broker-owned _INBOX agents publish their APPLIED home acks to.
		// Without it the home-delivery pass has no ACTUAL half to compare against and
		// would degrade into an un-acked re-delivery loop, so this subscribe failing
		// must fail cluster wiring loudly (it is in the same fail-hard list).
		func() (*nats.Subscription, error) { return b.subscribeHomeAcks(nc) },
		func() (*nats.Subscription, error) { return SubscribeAlertLs(nc, b.cfg.DB) },
		func() (*nats.Subscription, error) { return SubscribeAlertAck(nc, fwd) },
	}
	for _, sub := range subscribers {
		s, err := sub()
		if err != nil {
			return fmt.Errorf("broker: cluster responder subscribe: %w", err)
		}
		b.cl.subs = append(b.cl.subs, s)
	}

	// D5 audit publisher (re-derivable post-commit publish + JS replica reconfig) +
	// D8b alert reconciler (leader-gated raise/clear). Both leader-gated internally.
	pub := NewAuditPublisher(AuditPublisherConfig{
		Node: node, JS: b.js, Now: b.cfg.Now, Logger: b.cfg.Logger,
		ListSIDs: func(context.Context) ([]string, error) {
			return listSessionsByState(b.cfg.DB, "ACTIVE")
		},
		XferState: func(ctx context.Context, sid string, target int) (jsstream.StreamReplicaState, error) {
			return XferReplicaState(ctx, b.js, sid, target)
		},
		// audit M5: a no-stream publish error is a permanent loss only when the session is no
		// longer ACTIVE (rm'd / DELETING — its history stream was deleted by finalizeSessionRm);
		// an ACTIVE session's missing stream is transient (the reconcile loop ensures it) → R-22.
		SessionExists: func(sid string) (bool, error) {
			return session.IsActive(b.cfg.DB, sid)
		},
	})
	// B6 OPS#2: an alert webhook poster, iff a URL is configured. A bad URL fails wiring loudly
	// (the operator set it on purpose). Off-by-default: unset URL → nil poster → nil Webhook seam.
	var webhookPost func(WebhookEvent)
	var poster *webhookPoster
	if b.cfg.AlertWebhookURL != "" {
		p, werr := newWebhookPoster(b.cfg.AlertWebhookURL, b.cfg.Logger)
		if werr != nil {
			return fmt.Errorf("alert webhook: %w", werr)
		}
		poster = p
		webhookPost = p.Post
	}
	// B6 OPS#4: wrap Observe so each reconciler tick also refreshes the cached stream replica
	// posture for the /metrics gauge (no extra JS call — reuses the report the reconciler fetches).
	observeAndCache := func(ctx context.Context) (ReplicaReport, error) {
		rep, err := pub.ObserveReplicas(ctx)
		if err == nil {
			b.cacheReplicaSnapshot(rep)
		}
		return rep, err
	}
	rec := NewAlertReconciler(AlertReconcilerConfig{
		Node: node, DB: b.cfg.DB, Propose: node.Propose, Now: b.cfg.Now, Logger: b.cfg.Logger,
		Observe: observeAndCache, Webhook: webhookPost, LeaderID: func() string { _, id := node.LeaderWithID(); return id },
		SelfID:           b.selfID,                              // #93/H13: distinguishes a same-node lease blip from a genuine handoff when re-baselining the webhook
		SetJSUnavailable: func(v bool) { b.jsUnavail.Store(v) }, // G7b #20③: leader-observed sustained JS-503 → health self-report
	})
	b.cl.auditPub.Store(pub)
	b.cl.alertRec = rec
	// D7 membership orchestrator — constructed in cluster mode regardless of the admin
	// socket (the adminsock backend, wired later in Run, uses it iff the socket is set).
	b.cl.admin = NewClusterAdmin(node, b.cfg.Logger)
	b.cl.admin.prepareTunnelCertRotate = b.prepareTunnelCertRotate
	// B5 OPS#9: self-row capacity probes (disk statfs + port band) for `cluster status`.
	b.cl.admin.SetCapacityProbes(b.cfg.StoreDir, b.cfg.PortBandLow, b.cfg.PortBandHigh)
	// G2 #20: the live nats.conf path so status can raise the DATA-PLANE-DEGRADED banner when
	// force-single left the survivor conf clustered (JetStream 503).
	b.cl.admin.SetNatsConfPath(b.cfg.NatsConfPath)
	b.cl.admin.SetJSUnavailFn(b.jsUnavail.Load) // External-review m2: socket status surfaces runtime JS-503 too
	// B7 DOC#5 + C6 BD6: the generic leader-side event emitter the drain uses for `expose_rehomed`
	// (back-compat) + the C6 home_reassign_* / rehome_stalled lifecycle events.
	b.cl.admin.emitEvent = b.pubSysEvent
	// C6 建议5: the `cluster status --homes` aggregate (leader-agnostic RODB read).
	b.cl.admin.homesReport = b.buildHomesReport
	// §17 row 3: give the admin the broker-only cursor scatter-gather so `cluster status`
	// reports REAL per-peer reachability + applied-lag (not a blanket self-report).
	b.cl.admin.healthPoll = func() map[string]proto.ClusterHealthResp {
		return pollClusterHealth(b.nc.Load(), proto.SubjClusterCursor, observePollWindow)
	}
	// External-review F1: give the admin the read-only JS replica observation so `cluster
	// status` renders the REAL stream actual (not a synthesized actual==target).
	b.cl.admin.streamObserve = func() (ReplicaReport, error) {
		return pub.ObserveReplicas(context.Background())
	}
	b.cl.admin.topoSelf = topoSelf                    // C3: authoritative self-row topology report for status (nil ⇒ inert, B2)
	b.cl.admin.accountPubSelf = b.accountPubOrEmpty   // batch B / B4: the ACCT.NK comparison baseline (see clusteradmin.go)
	b.cl.admin.caughtUpFn = b.clusterCaughtUp         // C4: operation-controller catch-up probe
	b.cl.admin.streamsReadyFn = b.clusterStreamsReady // C4: operation-controller stream-readiness probe
	b.cl.admin.jsPlaceableFn = b.clusterJSPlaceable   // G69 (#67 sub-face 4): join terminal placement gate
	// R8a P1: the drain/retire data-plane convergence oracle + the immediate delivery kick.
	// WITHOUT these two the drain would go back to returning rc=0 on an unconverged data
	// plane — TestWireClusterLateWiresHomeConvergence is the anti-half-wiring guard, and
	// TestDrainRefusesRcZeroWhenDataPlaneStale is its behavioural mutation test.
	b.cl.admin.homeAppliedFn = b.homeAppliedEpoch
	b.cl.admin.homeDeliverFn = b.deliverHomeNow

	// The loops run on a child ctx so the ordered shutdown can cancel + JOIN them; each
	// signals loopDone on exit. (Run's ctx cancel also reaches this child, so a hard
	// shutdown still stops them.)
	loopCtx, cancel := context.WithCancel(ctx)
	b.cl.cancel = cancel
	// Batch-A A5: the set counts itself. The previous hand-maintained
	// loopCount (4, or 5 with a webhook poster) had no test and no link to the
	// `go` statements below it; undercounting it would let the ordered shutdown
	// proceed to nc.Drain while a loop was still publishing.
	b.cl.loops = newLoopSet()
	b.cl.loops.Go(loopCtx, "audit-publisher", pub.Run)
	b.cl.loops.Go(loopCtx, "reconciler", rec.Run)
	// D9 §17 step 10b: the leader-gated observability poll (broker_down / raft_lag).
	b.cl.loops.Go(loopCtx, "observe", b.runObserveLoop)
	// C3: the per-broker (NOT leader-gated) NATS topology reconcile loop.
	b.cl.loops.Go(loopCtx, "topology-reconcile", b.runTopologyReconcileLoop)
	if poster != nil {
		b.cl.loops.Go(loopCtx, "alert-webhook", poster.Run)
	}
	return nil
}

// clusterCaughtUp is the production catch-up transport (external-review F1): it scatter-
// gathers the broker-only cursor probe and reports whether the JOINING node's self-reported
// AppliedIndex has reached the barrier. A node that does not answer yet is "not caught up"
// (false, no error) so the add gate keeps polling until maxWait.
func (b *Broker) clusterCaughtUp(nodeID string, barrier uint64) (bool, error) {
	resp := pollClusterHealth(b.nc.Load(), proto.SubjClusterCursor, observePollWindow)
	r, ok := resp[nodeID]
	if !ok {
		return false, nil
	}
	return r.AppliedIndex >= barrier, nil
}

// clusterStreamsReady is the production stream-readiness gate (external-review F1): retire is
// refused unless EVERY JS stream is at its target replica count (ReplicaReport.AllAtTarget is
// fail-closed — an incomplete observation reports NOT ready). nodeID is unused (the predicate
// is cluster-wide: a stream below target means retiring ANY node risks losing a replica).
func (b *Broker) clusterStreamsReady(string) (bool, error) {
	ap := b.cl.auditPub.Load()
	if ap == nil {
		// Fail-closed, consistent with AllAtTarget's own contract: an
		// unobservable cluster is NOT ready.
		return false, fmt.Errorf("broker: audit publisher not wired yet")
	}
	rep, err := ap.ObserveReplicas(context.Background())
	if err != nil {
		return false, err
	}
	return rep.AllAtTarget(), nil
}

// clusterJSPlaceProbeTimeout bounds the join-side placement probe. It is hygiene, not protection: the
// same inline tick already embeds a 2s healthPoll and an unbounded ObserveReplicas (topoConvergedForOp
// -> StatusReport -> streamObserve), so this cap does not keep the observe loop responsive and must not
// be described as if it did.
const clusterJSPlaceProbeTimeout = 3 * time.Second

// jsPlaceableFrom is #67 sub-face 4's predicate in PURE form, split out so it is testable without a
// broker or a JetStream connection.
//
// It has NO error return by design: every uncertainty folds into (false, detail). A seam that could
// return an error would let a probe that fails every tick take a path that skips the caller's deadline
// check — the precise shape of the hole that made gotcha #45 wedge the membership plane.
func jsPlaceableFrom(target int, st jsstream.StreamReplicaState, obsErr error) (bool, string) {
	if target <= 1 {
		// N=1 / force-single: there is nothing to place. Semantic, not an optimisation — a single-replica
		// asset needs no meta placement at all.
		return true, ""
	}
	if obsErr != nil {
		// G-11: the detail string is recorded via recordOpError, which is change-gated on the RENDERED
		// message — so folding the raw error in verbatim makes a JS meta election (which alternates
		// "context deadline exceeded" / "nats: no responders available") commit a raft write EVERY tick
		// and evict the join ladder's history at opTimelineCap. Classify into a STABLE category here and
		// leave the raw error to the logger.
		return false, "events stream not observable yet (" + jsObsCategory(obsErr) + ")"
	}
	if st.Configured < target {
		return false, fmt.Sprintf("events stream is configured at %d/%d replicas (the replica raise has not landed)",
			st.Configured, target)
	}
	if st.Assigned < target {
		return false, fmt.Sprintf("the JS meta has assigned %d/%d peers to the events stream",
			st.Assigned, target)
	}
	return true, ""
}

// jsObsCategory collapses an observation error into a small, STABLE set of words so the gate's detail
// string does not churn tick-to-tick (see the G-11 note above). The raw error still reaches the log.
func jsObsCategory(err error) string {
	switch {
	case err == nil:
		return ""
	case errors.Is(err, context.DeadlineExceeded):
		return "the JetStream API did not answer in time"
	case errors.Is(err, nats.ErrNoResponders):
		return "no JetStream responder"
	case errors.Is(err, jetstream.ErrStreamNotFound):
		return "the events stream does not exist yet"
	default:
		return "unavailable"
	}
}

// clusterJSPlaceable is the JOIN-side placement gate: can the JS meta host a NEW asset at the CURRENT
// target replica factor? Evidence is the canonical `events` stream, which ReconcileOnce raises FIRST and
// UNCONDITIONALLY on every pass, so it is always the first thing the meta had to place.
//
// It is deliberately NOT clusterStreamsReady (retire's predicate). That one asks a DATA-SAFETY question
// — "has every stream, including every multi-GiB OBJ_xfer bucket, CAUGHT UP" — and audit.go records in
// the product's own words that a lingering under-target OBJ_xfer blocks that gate forever. Making a grow
// wait for a full byte copy would be a false-block of a healthy cluster. Join asks a PLACEMENT-CAPABILITY
// question, and assignment answers it.
//
// SCOPE, stated honestly: ReplicasFor caps at 3, so a 3->4 grow leaves target=3 and the events stream
// TYPICALLY already has 3 assigned peers — none of them necessarily the joiner — so this returns true on
// the first tick. ("Typically" is deliberate: that is a steady-state assumption, not an invariant.) The
// gate covers target-INCREASING grows (1->2, 2->3), which is where #67 was measured. It does NOT prove
// the meta placed anything ON THE JOINER.
func (b *Broker) clusterJSPlaceable() (bool, string) {
	nv, err := b.cl.node.NumVoters()
	if err != nil {
		return false, "count voters: " + err.Error()
	}
	target := jsstream.ReplicasFor(nv)
	if target <= 1 {
		return true, "" // no JS call at all
	}
	ctx, cancel := context.WithTimeout(context.Background(), clusterJSPlaceProbeTimeout)
	defer cancel()
	// Cheap gate first: the events stream's CONFIGURED/ASSIGNED counts. It costs 2 round trips and
	// rejects the common "the replica raise has not landed yet" case without touching the meta.
	st, oerr := jsstream.CollectStreamState(ctx, b.js, jsstream.EventsStreamName, target)
	if ok, detail := jsPlaceableFrom(target, st, oerr); !ok {
		return false, detail
	}
	// EXTERNAL REVIEW F3: then MEASURE, do not infer. The counts above are a proxy for "a new R=N asset
	// is creatable", and the review showed a proxy can be satisfied by state that says nothing about
	// now — tether never peer-removes, so a retired member lingers in the assignment list. The canary is
	// an EMPTY stream created at the target factor and immediately deleted: it asks the meta the exact
	// question the CLI's contract promises an answer to, and because it carries no data it introduces
	// none of the byte-copy wait that gating on catch-up would.
	if err := jsstream.ProbeMetaCanPlace(ctx, b.js, target, b.cfg.Logger); err != nil {
		return false, "the JS meta refused to assign an empty memory-backed canary at the target replica " +
			"factor (this measures META ASSIGNMENT, not disk budget for file/object stores): " + err.Error()
	}
	return true, ""
}

// livenessDB returns the handle for LIVENESS-column writes — the local, non-replicated state
// (§3.5: Apply never writes it, rebuilt on failover).
//
// THE COLUMN SET IS THREE, NOT TWO (batch B, B3 — reconciled before the accessors were named).
// This godoc used to say "(last_heartbeat_at, status)". It was wrong: production writes a third,
// `proxy_ready`, through this same handle — node.SetProxyReady from broker.go:1469 and
// proxy.go:454. test/determinism/lint_skeleton_test.go's livenessColumnRe has always listed all
// three; the two SSOTs disagreed and the regex was the correct one. Naming an accessor whose
// contract was defined by a regex in a test that never runs against production would have
// promoted the ambiguity rather than resolved it, so it is resolved here:
//
//	last_heartbeat_at  — Heartbeat (broker.go:1441, clusterwrite.go:815)
//	status             — ReconcileStates (broker.go:1234, reconcile_passes.go:58)
//	proxy_ready        — SetProxyReady (broker.go:1469, proxy.go:454)
//
// EVERY CALLER IS NOW A LIVENESS WRITE. This paragraph used to name proc-GC as a current anomaly —
// a `processes` DELETE (replicated table) riding this handle — and to say moving it was tracked.
// It was moved in the same delta that wrote that sentence: reconcile_passes.go's proc-gc pass asks
// singleWriter(), which returns (nil, false) in cluster mode, so the restriction is structural
// rather than a comment plus a mode `if`. The stale warning is removed rather than left to be
// reasoned from (external review M2): an obsolete ownership claim in the one godoc a maintainer
// comes to for ownership questions is worse than none.
// In single mode that is the normal read+write handle; in cluster mode it is the FSM
// write pool (node.DB()), written directly (NOT through raft): liveness is high-frequency
// (heartbeats) and per-broker-local, so routing it through Propose would flood raft and
// is semantically wrong. MaxOpenConns(1) serializes these writes with Apply, and the
// columns are disjoint from the replicated set, so this is a safe deliberate exception
// to "the FSM is the single writer" — it applies only to the replicated columns.
func (b *Broker) livenessDB() *sql.DB {
	if b.clusterMode {
		return b.cl.node.DB()
	}
	return b.cfg.DB
}

// reaperMayDelete reports whether a boot/periodic orphan reaper may DELETE shared JetStream
// streams/buckets. In single mode (b.cl == nil) the local DB is the authority — always true. In
// cluster mode only a CAUGHT-UP LEADER has a local view that reflects the true cluster state; a fresh
// joiner / not-yet-caught-up follower would classify every cluster-wide stream as orphan and wipe live
// history + in-flight tier-B buckets (audit G — CRITICAL data loss).
//
// Caught-up is measured in the RAFT domain (RaftAppliedIndex vs CommitIndex), NOT the command domain.
// The SQLite command cursor AppliedIndex never advances on the leader-election LogNoop (or config
// entries), so a steady-state leader has SQLite-AppliedIndex == CommitIndex-1 PERMANENTLY — comparing it
// against CommitIndex is cross-domain and structurally never true, which silently disabled this gate on
// every cluster-mode boot (v0.4.4 review G-reaper-gate). RaftAppliedIndex advances on the noop too, so a
// caught-up leader reads RaftAppliedIndex == CommitIndex.
//
// External re-review (lock-reap-F2): the catch-up test is Node.CaughtUp(), which additionally
// requires CommitIndex()>0 — a bare RaftAppliedIndex()>=CommitIndex() is VACUOUSLY true on a
// never-elected node AND on a snapshot-carrying restart (applied=snapshot, volatile commit=0),
// which would let a pre-first-commit boot view classify orphans. The IsLeader() gate already
// blocks a booting non-leader, but the boot orphan reap runs from Run's boot path, so the
// commit>0 bound is a belt-and-suspenders that also matches the cluster-layer positive test.
//
// Raft-free (L-2): only the narrow Node accessors IsLeader/CaughtUp.
func (b *Broker) reaperMayDelete() bool {
	if b.cl == nil {
		return true
	}
	if b.cl.node == nil || !b.cl.node.IsLeader() {
		return false
	}
	return b.cl.node.CaughtUp()
}

// reaperCaughtUp is the LEADER-NEUTRAL sibling of reaperMayDelete, for a reaper whose
// blast radius is already partitioned by an independent HOME-authority gate (the xfer
// object reaper: homeOwnsXferBucket). It requires only that this broker's RAFT-domain
// view is current (RaftAppliedIndex >= CommitIndex) — which a caught-up FOLLOWER
// satisfies exactly as a leader does — and drops the IsLeader() requirement.
//
// WHY DROPPING IsLeader() IS SAFE HERE (and why it was a BUG to keep it — #58/P10):
// reaperMayDelete's leader requirement existed to guarantee a COMPLETE cluster view
// before deleting from the shared JS meta. But the xfer reaper never needs a complete
// cluster view: homeOwnsXferBucket(sid) already restricts it to buckets whose session
// is ENTIRELY homed to THIS broker, and home is a partition (each session has exactly
// one home). So "complete cluster view" was the wrong authority; "am I this bucket's
// home, on a current view" is the right one. Requiring BOTH leader AND home made a
// session homed to a NON-LEADER broker unreapable by ANY node — the leader failed
// homeOwnsXferBucket, the home failed IsLeader — so tier-B garbage was immortal on
// every cluster whose transfers weren't homed to the raft leader.
//
// The catch-up gate also closes the split-view race the home-partition argument needs, on
// a LIVE commit view: if a home reassignment X→Y is committed, X cannot both be caught up
// AND still see itself as home — before X applies the reassignment RaftAppliedIndex <
// CommitIndex (not caught up, no reap); after X applies it X sees home==Y (not own, no
// reap). CAVEAT (external re-review, CaughtUp HONEST LIMIT #2): on an ISLANDED X whose
// CommitIndex froze BEFORE the reassignment was learned, applied catches that frozen
// ceiling and CaughtUp() reads true on a stale view. The reap's destructive action is the
// write-path backstop there: a JS object delete fails once X's colocated JS meta loses
// quorum, so a stale-view reap cannot actually delete a live bucket. The commit>0 conjunct
// (see Node.CaughtUp) additionally suppresses the pre-first-commit boot/snapshot-restart
// window that a bare applied>=commit made vacuously true.
//
// Raft-free (L-2): only the narrow Node accessor CaughtUp.
func (b *Broker) reaperCaughtUp() bool {
	if b.cl == nil {
		return true
	}
	if b.cl.node == nil {
		return false
	}
	return b.cl.node.CaughtUp()
}

// proposeOrForward routes one authoritative write through raft. Leader ⇒ run the Plan
// locally (Propose / ProposeWithReqID for cross-retry idempotency via the 0011 ledger);
// follower ⇒ forward the request to the leader over the D4 wire (the leader's
// dispatchForward case for `verb` re-runs the Plan against its committed view). A nil
// runtime (single mode) is a programming error — callers gate on b.clusterMode first.
func (b *Broker) proposeOrForward(verb, reqID string, payload []byte, plan func(db *sql.DB) (*cluster.Command, error)) error {
	if b.cl.node.IsLeader() {
		var err error
		if reqID == "" {
			err = b.cl.node.Propose(plan)
		} else {
			err = b.cl.node.ProposeWithReqID(reqID, plan)
		}
		// audit M2 / write-forward F3: leadership can be LOST between the IsLeader() check and
		// raft.Apply, so Propose can return raft.ErrNotLeader / ErrLeadershipLost. Map it to the
		// SAME retriable sentinel the forward branch returns (cluster.ErrForwardNotLeader) so a
		// leadership race is UNIFORMLY transient across the leader-local and forward paths —
		// mirroring the PIN seam (NewProvisionSeam). Without this the raw raft error leaked out
		// as a terminal "store_error", which the agent's register loop treats as a PERMANENT
		// rejection and EXITS the process on a routine raft leader failover.
		if err != nil && cluster.IsNotLeader(err) {
			b.countForward(verb, cluster.ErrForwardNotLeader)
			return cluster.ErrForwardNotLeader
		}
		b.countForward(verb, err)
		return err
	}
	// NOT counted here: Forwarder.observe already tallies every Forward outcome at the network
	// boundary (external review B2). Counting again would double every follower forward while
	// leaving the five direct senders at zero — the worst of both.
	return b.cl.forwarder.Forward(verb, reqID, payload)
}

// forwardCounters holds the (verb, outcome) tallies surfaced as
// tether_broker_raft_forward_total. It is a separate type rather than two fields on Broker so the
// zero value is usable by the ~126 package-internal &Broker{} literals without them knowing it
// exists.
type forwardCounters struct {
	mu sync.Mutex
	n  map[string]int64
}

func (f *forwardCounters) add(key string) {
	f.mu.Lock()
	if f.n == nil {
		f.n = map[string]int64{}
	}
	f.n[key]++
	f.mu.Unlock()
}

func (f *forwardCounters) snapshot() map[string]int64 {
	f.mu.Lock()
	defer f.mu.Unlock()
	if len(f.n) == 0 {
		return nil
	}
	out := make(map[string]int64, len(f.n))
	for k, v := range f.n {
		out[k] = v
	}
	return out
}

// countForward classifies one authoritative write attempt.
//
// The three outcomes are NOT interchangeable and collapsing any two would remove the reason this
// counter exists: `not_leader` is the routine leadership race every caller retries, `error` is a
// typed business rejection that retrying will not fix, and `ok` is the denominator without which
// neither rate means anything. Several production paths (alert_forward.go's disk signal,
// cluster_health.go's alert ack, transfer_audit_forward.go, and the provision/join seams) discard
// the error entirely because a level-triggered re-assert or a client retry self-heals — this is
// what makes a forward that fails EVERY tick distinguishable from a broker with nothing to say,
// without changing that retry policy.
//
// TWO instrumentation boundaries (external review B2/R1): proposeOrForward's LEADER-LOCAL branch
// and Forwarder.observeOutcome. The latter covers every forward that crosses the wire and the
// provision/join seams' direct leader-local Propose branches, which bypass proposeOrForward. The
// previous arrangement counted only inside proposeOrForward, so the five direct senders above —
// including the disk signal this godoc cites — were never counted at all.
func (b *Broker) countForward(verb string, err error) {
	outcome := "ok"
	switch {
	case err == nil:
	case errors.Is(err, cluster.ErrForwardNotLeader), cluster.IsNotLeader(err):
		outcome = "not_leader"
	default:
		outcome = "error"
	}
	b.forwards.add(verb + "/" + outcome)
}

// createSession routes a session create (D9 §3, audit #9). Single mode: the direct
// mutator (byte-identical to pre-D9). Cluster mode: PlanCreate via Propose (leader) /
// forward (follower), then read the committed row back by SID (== name) for the
// authoritative leader-baked created_at. SID == name (name-keyed sessions), so no
// leader-baked id round-trip is needed.
func (b *Broker) createSession(sid, fp, pinHash string) (*session.Session, error) {
	if !b.clusterMode {
		return session.Create(b.cfg.DB, sid, sid, fp, pinHash, b.cfg.Now())
	}
	payload, err := json.Marshal(SessionCreatePayload{Name: sid, FP: fp, PinHash: pinHash})
	if err != nil {
		return nil, err
	}
	if err := b.proposeOrForward(VerbSessionCreate, "", payload, func(db *sql.DB) (*cluster.Command, error) {
		return session.PlanCreate(db, sid, sid, fp, pinHash, b.cfg.Now())
	}); err != nil {
		// NOT committed (leadership lost, forward failed) or a genuine ErrAlreadyExists (the name is
		// already taken) — surface it. A duplicate MUST still be rejected here: the D9 cluster tests
		// rely on it to prove the first create committed, and a same-owner retry is indistinguishable
		// from a fresh duplicate, so idempotency cannot live on this path.
		return nil, err
	}
	// COMMITTED. proposeOrForward returns nil only after the leader APPLIED the command (leader path,
	// Propose waits for local Apply) or the leader COMMITTED it (forward path returns on leader
	// commit). Read the authoritative row back for the leader-baked created_at, tolerating a
	// follower's Apply-lag (sessionReadBack*).
	if s, err := b.readCommittedSession(sid); err == nil {
		return s, nil
	}
	// Q4 (docs/reviews/r6-findings.md): the write is DURABLE (committed above) but this replica's
	// Apply still trails past the read-back window (R6 measured 1.37s under a partition). Reporting
	// that read-back timeout as a FAILURE was the "committed-but-reported-failed" defect: it returned
	// rc=70, and because the leader's PlanCreate then rejected every retry with already_exists, it
	// left `session create` structurally unable to ever go green (a poll_until loop stayed red). Since
	// the write really committed, report SUCCESS on the FIRST attempt with a best-effort session; the
	// authoritative created_at converges on the next read (`session ls`). This never manufactures a
	// false success — control only reaches here after proposeOrForward committed the write to raft.
	return &session.Session{
		SID: sid, Name: sid, OwnerPubkeyFP: fp,
		State: session.StateActive, CreatedAt: b.cfg.Now(),
	}, nil
}

// allocatePort routes an expose allocation (D9 audit #6). Single mode: the direct
// mutator. Cluster mode: LEADER-LOCAL — the leader runs PlanAllocate via Propose and
// captures the leak-once token + chosen port from the closure (read-back is impossible:
// only the token HASH is persisted). External-review F3: rather than bounce a follower with
// not_leader (which the queue group could deliver expose.req to, failing ~(N-1)/N of the
// time), the EXPOSE SUBJECT is BROADCAST + handleExposeReq is LEADER-ONLY — so allocatePort
// only runs on the leader (a stray non-leader call falls through to Propose's raft.ErrNotLeader,
// a benign election-race). The home is stamped AT allocate (PlanAllocate's homeBroker), so
// homeForExpose only READS the epoch.
func (b *Broker) allocatePort(sid, nid, name string, localPort, remotePort int, rebuildOff bool, onBroker, fp string) (*port.Allocation, error) {
	cfg := b.cfg.PortAllocCfg()
	if !b.clusterMode {
		// B4 --on-broker is cluster-only: a single broker has no roster to pin to. Reject
		// BEFORE the write. --no-rebuild is accepted (inert here — it writes rebuild_on_failure=0,
		// honest metadata a future `cluster init --from-existing` would carry forward).
		if onBroker != "" {
			return nil, errOnBrokerSingleMode
		}
		return port.Allocate(b.cfg.DB, sid, nid, name, localPort, remotePort, fp, rebuildOff, cfg)
	}
	// B4 --on-broker: validate the named node against the live roster with the SAME predicate
	// homeForExpose uses at delivery (Eligible()==VOTER && CertFP != ""), so no arbitrary string
	// reaches port_allocations.home_broker and a draining/non-voter/uncert-pinned target is
	// refused. Resolved on b.cfg.DB (= RODB in cluster mode) BEFORE Propose, exactly like the
	// default resolveHomeForAgent path below — a rejection returns before any raft append.
	homeBroker := ""
	if onBroker != "" {
		home, err := clusternodes.LookupByNodeID(b.cfg.DB, onBroker)
		if err != nil || !home.Eligible() || home.CertFP == "" {
			return nil, errOnBrokerUnknown
		}
		// D1 (Stage-C): also reject a node mid-drain. DrainNode raises the broker_draining marker
		// (step 1) BEFORE flipping phase->DRAINING (step 3), so during that window Eligible()
		// (phase==VOTER) alone would accept a draining target — and step-2 migrateExposes has
		// already run, so a fresh pin there strands the expose. The marker is the authoritative
		// "do not place here" signal (same predicate the reconciler uses). Fail closed: a read
		// error also rejects rather than risk pinning onto a draining node.
		if draining, derr := cluster.DrainingNodes(b.cfg.DB); derr != nil || slices.Contains(draining, onBroker) {
			return nil, errOnBrokerUnknown
		}
		homeBroker = onBroker
	} else if home := b.resolveHomeForAgent(sid, nid); home != nil {
		homeBroker = home.NodeID
	}
	var captured *port.Allocation
	if err := b.cl.node.Propose(func(db *sql.DB) (*cluster.Command, error) {
		a, cmd, e := port.PlanAllocate(db, sid, nid, name, localPort, remotePort, fp, homeBroker, rebuildOff, cfg)
		if e != nil {
			return nil, e
		}
		captured = a
		return cmd, nil
	}); err != nil {
		return nil, err
	}
	return captured, nil
}

// registerNode routes an agent register (D9 audit #1). Single mode: the direct mutator
// (identity + liveness in one tx). Cluster mode: PlanRegister via Propose/forward writes
// the IDENTITY columns through raft, then the LIVENESS columns (last_heartbeat_at, status)
// are written locally (§3.5: liveness is not replicated). The liveness write is best-effort
// — on a follower whose Apply hasn't yet landed the identity row, the next heartbeat
// re-asserts it (a missing-row Heartbeat error is logged, not fatal to register).
func (b *Broker) registerNode(in node.RegisterInput) error {
	if !b.clusterMode {
		return node.Register(b.cfg.DB, in, b.cfg.Now())
	}
	payload, err := json.Marshal(in)
	if err != nil {
		return err
	}
	if err := b.proposeOrForward(VerbNodeRegister, "", payload, func(db *sql.DB) (*cluster.Command, error) {
		return node.PlanRegister(db, in, b.cfg.Now())
	}); err != nil {
		return err
	}
	if err := node.Heartbeat(b.livenessDB(), in.SID, in.NID, b.cfg.Now()); err != nil {
		b.cfg.Logger.Debug("broker: register liveness write deferred (apply lag)", "sid", in.SID, "nid", in.NID, "err", err)
	}
	return nil
}

// recordProc routes a process-started record (D9 audit #4). Single mode: the direct
// mutator. Cluster mode: PlanInsert via Propose/forward. PlanInsert bakes INSERT OR IGNORE
// on the pid PRIMARY KEY, so a forwarder retry re-proposing the same pid at a new raft index
// is a deterministic no-op at Apply (NOT a PK-violation that would fail-stop panic the FSM);
// reqID="" because that OR IGNORE is the idempotency anchor, not the 0011 ledger (audit M8).
func (b *Broker) recordProc(p proc.Process) error {
	if !b.clusterMode {
		return proc.Insert(b.cfg.DB, p)
	}
	payload, err := json.Marshal(p)
	if err != nil {
		return err
	}
	return b.proposeOrForward(VerbProcInsert, "", payload, func(db *sql.DB) (*cluster.Command, error) {
		return proc.PlanInsert(db, p)
	})
}

// freePortAllocation routes an expose-rm free. All three paths (single-mode direct,
// leader-local Plan, forwarded Plan) take the SAME narrowed identity — see allocIdentity
// in cluster_forward.go for why the leader must not keep the wider struct.
func (b *Broker) freePortAllocation(a port.Allocation) error {
	id := allocIdentity(a)
	narrowed := id.allocation()
	if !b.clusterMode {
		return port.FreeAllocation(b.cfg.DB, narrowed, b.cfg.Now())
	}
	payload, err := json.Marshal(id)
	if err != nil {
		return err
	}
	return b.proposeOrForward(VerbPortFreeAllocation, "", payload, func(db *sql.DB) (*cluster.Command, error) {
		return port.PlanFreeAllocation(db, narrowed, b.cfg.Now())
	})
}

func allocationSessionInfo(a port.Allocation) tunnel.SessionInfo {
	return tunnel.SessionInfo{
		PublicPort: a.Port,
		SID:        a.SID,
		NID:        a.NID,
		TokenHash:  a.TokenHash,
		Epoch:      a.Epoch,
	}
}

func (b *Broker) closeTunnelProxyLocal(info tunnel.SessionInfo) {
	srv := b.tunnelSrv.Load()
	if srv == nil || info.PublicPort <= 0 {
		return
	}
	if ok := srv.CloseProxyIf(info); ok {
		b.cfg.Logger.Info("broker: tunnel proxy closed", "port", info.PublicPort, "sid", info.SID, "nid", info.NID)
	}
}

func (b *Broker) closeTunnelProxyEverywhere(a port.Allocation) {
	info := allocationSessionInfo(a)
	b.closeTunnelProxyLocal(info)
	if !b.clusterMode {
		return
	}
	body, err := json.Marshal(clusterTunnelClosePayload(info))
	if err != nil {
		b.cfg.Logger.Warn("broker: marshal tunnel close", "err", err, "port", info.PublicPort)
		return
	}
	if err := b.publishOnConn(proto.SubjClusterTunnelClose, body); err != nil {
		b.cfg.Logger.Warn("broker: broadcast tunnel close", "err", err, "port", info.PublicPort)
	}
}

func (b *Broker) subscribeTunnelClose(nc *nats.Conn) (*nats.Subscription, error) {
	return nc.Subscribe(proto.SubjClusterTunnelClose, func(msg *nats.Msg) {
		var p clusterTunnelClosePayload
		if err := json.Unmarshal(msg.Data, &p); err != nil {
			b.cfg.Logger.Warn("broker: tunnel close malformed", "err", err)
			return
		}
		b.closeTunnelProxyLocal(tunnel.SessionInfo(p))
	})
}

// revokePortAllocation routes an offline-node revoke. Same three-path narrowing as
// freePortAllocation; this one matters more, because its caller scans OFFLINE nodes and
// the row it selected may already have been reused by a later allocation — the identity
// fence is the only thing stopping a delayed revoke from hitting the wrong row.
func (b *Broker) revokePortAllocation(a port.Allocation, now time.Time) error {
	id := allocIdentity(a)
	narrowed := id.allocation()
	if !b.clusterMode {
		return port.RevokeAllocation(b.cfg.DB, narrowed, now)
	}
	payload, err := json.Marshal(id)
	if err != nil {
		return err
	}
	return b.proposeOrForward(VerbPortRevoke, "", payload, func(db *sql.DB) (*cluster.Command, error) {
		return port.PlanRevokeAllocation(db, narrowed, now)
	})
}

// markProcExited routes a process EXIT (D9 round-1 BLOCKER: exec.go's proc.exit + the
// reconcile missed-exit path). Single mode: the direct mutator. Cluster mode: PlanMarkExited
// via Propose/forward (the WHERE status='RUNNING' guard makes a forwarder retry idempotent).
func (b *Broker) markProcExited(pid string, exitCode int, when time.Time) error {
	if !b.clusterMode {
		return proc.MarkExited(b.cfg.DB, pid, exitCode, when)
	}
	payload, err := json.Marshal(ProcMarkExitedPayload{Pid: pid, ExitCode: exitCode, EndedAt: when})
	if err != nil {
		return err
	}
	return b.proposeOrForward(VerbProcMarkExited, "", payload, func(db *sql.DB) (*cluster.Command, error) {
		return proc.PlanMarkExited(db, pid, exitCode, when)
	})
}

// tombstoneSession routes a session rm's ACTIVE->DELETING transition (D9 round-1 BLOCKER,
// audit #11). Single mode: the direct mutator. Cluster mode: PlanTombstone via
// Propose/forward (the leader bakes deleting_at; ErrNotFound/ErrDeleting surface as typed
// forward errors so `session rm` reports the same codes as single mode).
func (b *Broker) tombstoneSession(sid string) error {
	if !b.clusterMode {
		return session.Tombstone(b.cfg.DB, sid, b.cfg.Now())
	}
	payload, err := json.Marshal(SessionMutatePayload{SID: sid})
	if err != nil {
		return err
	}
	return b.proposeOrForward(VerbSessionTombstone, "", payload, func(db *sql.DB) (*cluster.Command, error) {
		return session.PlanTombstone(db, sid, b.cfg.Now())
	})
}

// dropSession routes the H.3 finalize cascade (the 7-table DELETE of a DELETING session's
// rows after teardown; D9 round-1 BLOCKER). Single mode: dropSessionRows direct. Cluster
// mode: PlanHardDelete via Propose/forward (one FSM txn, same as the single-tx semantics;
// a no-op on an absent row so a retry / a re-run of resumeSessionRm is harmless).
func (b *Broker) dropSession(sid string) error {
	if !b.clusterMode {
		return dropSessionRows(b.cfg.DB, sid)
	}
	payload, err := json.Marshal(SessionMutatePayload{SID: sid})
	if err != nil {
		return err
	}
	return b.proposeOrForward(VerbSessionDrop, "", payload, func(db *sql.DB) (*cluster.Command, error) {
		return session.PlanHardDelete(db, sid)
	})
}

// evictNode routes an `admin evict` (D9 round-2 BLOCKER). Single mode: handled by the
// adminsock's direct tx (EvictWrite stays nil). Cluster mode: PlanEvict via Propose/forward
// (data-less; the DELETEs are no-ops on absent rows so a forwarder retry is harmless).
func (b *Broker) evictNode(sid, nid string) error {
	// audit write-forward F6: evictNode is the CLUSTER-MODE EvictWrite seam (the adminsock wires
	// it only in cluster mode; single mode leaves EvictWrite nil + does the direct tx). Guard the
	// !clusterMode case like every other proposeOrForward router so a wiring bug fails LOUD here
	// instead of nil-dereferencing b.cl.node inside proposeOrForward.
	if !b.clusterMode || b.cl == nil {
		return fmt.Errorf("broker: evictNode is cluster-mode only (single mode uses the adminsock direct tx)")
	}
	payload, err := json.Marshal(EvictPayload{SID: sid, NID: nid})
	if err != nil {
		return err
	}
	return b.proposeOrForward(VerbNodeEvict, "", payload, func(db *sql.DB) (*cluster.Command, error) {
		return node.PlanEvict(sid, nid)
	})
}

// sessionReadBack bounds how long readCommittedSession tries to fetch a just-committed session
// row back (for the authoritative leader-baked created_at) before giving up.
//
// A forward returns on the LEADER commit, but a follower's local Apply trails it. Under a real
// partition R6 measured that apply-lag at 1.37s (drill 96 canary2, docs/reviews/r6-findings.md
// Q4) — for which the original 1s bound (50 × 20ms) was too short. 3s (150 × 20ms) covers that
// worst case with ~2x margin while staying safely under the 5s ctl request deadline
// (cmd/tether/session.go), so the broker's reply still lands in time. Note this window is NOT the
// success criterion: createSession has ALREADY confirmed the raft commit before calling this, so a
// timeout here is NON-FATAL — createSession returns a best-effort success rather than failing.
const (
	sessionReadBackInterval = 20 * time.Millisecond
	sessionReadBackAttempts = 150 // 150 × 20ms = 3s
)

// readCommittedSession reads a session back by SID after a routed write, tolerating follower
// Apply-lag (see sessionReadBack* above). The leader path is immediate (Propose waits for local
// Apply), so the loop only ever spins on a follower. A timeout returns an error, but the sole
// caller (createSession) treats that as non-fatal — the write is already durably committed.
func (b *Broker) readCommittedSession(sid string) (*session.Session, error) {
	var last error
	for i := 0; i < sessionReadBackAttempts; i++ {
		s, err := session.Get(b.cfg.DB, sid)
		if err == nil {
			return s, nil
		}
		last = err
		time.Sleep(sessionReadBackInterval)
	}
	return nil, fmt.Errorf("broker: session %q not visible after commit (apply lag): %w", sid, last)
}
