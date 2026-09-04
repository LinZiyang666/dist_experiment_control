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
	// webhook is the optional alert-webhook poster, kept so RuntimeReport can publish its DELIVERY
	// OUTCOME separately from the loop's liveness (external review B2-4). nil when no URL is configured.
	//
	// atomic.Pointer for the same reason as auditPub above and NOT as boilerplate: wireClusterLate
	// assigns it while the adminsock goroutine may already be serving `admin runtime`, so a plain field
	// would be an unsynchronised read of a seconds-wide window.
	webhook atomic.Pointer[webhookPoster]
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
	// topoSelf (C3) is this broker's latest topology-reconcile self-report
	// (applied/observed/action/reason), published by the per-broker reconcile loop and read by the
	// status/health responders. atomic so the responder (other goroutine) reads it race-free; nil
	// until the first reconcile pass.
	topoSelf atomic.Pointer[topoSelfReport]
}

// topoSelfReport is one broker's reconcile state, read by the status/health responders (C3 §2.7).
type topoSelfReport struct {
	Applied  uint64
	Observed uint64
	// Action (batch C) is the natsconf.Action* the last pass returned — a CLOSED enum, unlike
	// Reason. It is what both status renderers classify on; Reason survives only as the operator's
	// detail text and as the mixed-version fallback for a broker that predates this field.
	Action string
	Reason string
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
// probe (the loops need brokerJS(b)). The loops are tied to ctx (Run's context) so a cancel
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
		func() (*nats.Subscription, error) {
			return SubscribeClusterApply(nc, node, b.cfg.Now, b.cfg.Logger,
				func(pin, hash string) error { return verifyPINOnLeader(b, pin, hash) })
		},
		func() (*nats.Subscription, error) { return b.subscribeTunnelClose(nc) },
		func() (*nats.Subscription, error) {
			return SubscribeClusterHealth(nc, node, b.cfg.DB, b.cfg.Now, topoSelf, b.jsUnavail.Load, b.cfg.ColocatedAgentNID, b.accountPubOrEmpty, b.cfg.Logger)
		},
		func() (*nats.Subscription, error) {
			return SubscribeClusterRosterPull(nc, b.manifestBytes, b.cfg.Logger)
		}, // G3 #17
		func() (*nats.Subscription, error) { return b.SubscribeClusterUpgradeTrigger(nc) }, // G5 #13 W2b
		func() (*nats.Subscription, error) { return b.SubscribeClusterGrowTrigger(nc) },    // G4 §B grow trigger
		func() (*nats.Subscription, error) {
			return SubscribeClusterCursor(nc, node, b.cfg.DB, b.cfg.Now, topoSelf, b.jsUnavail.Load, b.cfg.ColocatedAgentNID, b.accountPubOrEmpty, b.cfg.Logger)
		},
		// R8a P1: the broker-owned _INBOX agents publish their APPLIED home acks to.
		// Without it the home-delivery pass has no ACTUAL half to compare against and
		// would degrade into an un-acked re-delivery loop, so this subscribe failing
		// must fail cluster wiring loudly (it is in the same fail-hard list).
		func() (*nats.Subscription, error) { return b.subscribeHomeAcks(nc) },
		func() (*nats.Subscription, error) { return SubscribeAlertLs(nc, b.cfg.DB, b.cfg.Logger) },
		func() (*nats.Subscription, error) { return SubscribeAlertAck(nc, fwd, b.cfg.Logger) },
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
		Node: node, JS: brokerJS(b), Now: b.cfg.Now, Logger: b.cfg.Logger,
		// B7 per-iteration liveness. The closure resolves b.cl.loops lazily: the set is assigned before
		// the goroutine that will call this is started, so the goroutine-start edge publishes it.
		Beat: func() { b.cl.loops.Beat("audit-publisher") },
		ListSIDs: func(context.Context) ([]string, error) {
			return listSessionsByState(b.cfg.DB, "ACTIVE")
		},
		XferState: func(ctx context.Context, sid string, target int) (jsstream.StreamReplicaState, error) {
			return XferReplicaState(ctx, brokerJS(b), sid, target)
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
	// B6 OPS#4: wrap Observe so each reconciler tick also refreshes the cached stream replica posture —
	// the min-actual for the /metrics gauge AND the stream count that sizes the NEXT observation's
	// deadline (no extra JS call; it reuses the report the reconciler already fetches). This is the ONLY
	// writer of that cache, and it runs on the leader tick, which is why a follower's count stays 0.
	// This third ObserveReplicas call also needs its OWN bound: the reconciler passes its process-lifetime
	// loop context, so relying on the caller would let one stalled JS request wedge alert reconciliation
	// forever.
	observeAndCache := func(parent context.Context) (ReplicaReport, error) {
		ctx, cancel := context.WithTimeout(parent,
			clusterReplicaObserveBudget(b.observeStreamCountForBudget()))
		defer cancel()
		rep, err := pub.ObserveReplicas(ctx)
		// A failed per-stream collection may still carry the complete work-set count discovered first.
		// Cache it for the next budget, but cacheReplicaSnapshot never treats it as measured posture.
		b.cacheReplicaSnapshot(rep)
		return rep, err
	}
	rec := NewAlertReconciler(AlertReconcilerConfig{
		Node: node, DB: b.cfg.DB, Propose: node.Propose, Now: b.cfg.Now, Logger: b.cfg.Logger,
		Observe: observeAndCache, Webhook: webhookPost, LeaderID: func() string { _, id := node.LeaderWithID(); return id },
		SelfID:           b.selfID,                              // #93/H13: distinguishes a same-node lease blip from a genuine handoff when re-baselining the webhook
		SetJSUnavailable: func(v bool) { b.jsUnavail.Store(v) }, // G7b #20③: leader-observed sustained JS-503 → health self-report
		Beat:             func() { b.cl.loops.Beat("reconciler") },
	})
	b.cl.auditPub.Store(pub)
	b.cl.alertRec = rec
	// D7 membership orchestrator — constructed in cluster mode regardless of the admin
	// socket (the adminsock backend, wired later in Run, uses it iff the socket is set).
	b.cl.admin = NewClusterAdmin(node, b.cfg.Logger)
	// EXTERNAL review B1: the ONLINE force-single intent lives beside the raft sub-tree.
	b.cl.admin.dataDir = b.cfg.ClusterDataDir
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
		// B7: bounded, and the budget SCALES with the number of sessions the observation will walk.
		// This seam is reached from StatusReport, which the operation controller calls on every leader
		// tick (topoConvergedForOp), so an unbounded observation here stalls the whole convergence
		// ladder rather than just one status render — and a budget too small for the fleet turns a
		// healthy cluster into a reported replica deficit (internal review B7-02).
		ctx, cancel := context.WithTimeout(context.Background(),
			clusterReplicaObserveBudget(b.observeStreamCountForBudget()))
		defer cancel()
		return pub.ObserveReplicas(ctx)
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
	// B7: the four PERIODIC loops declare their cadence and beat once per iteration. The declaration is
	// what makes Iters diagnosable — "0 iterations against a 5s cadence" is dead, while the same 0 on
	// the event-driven poster below just means no alerts fired. Before this, all five reported only a
	// StartedAt, so a loop that returned on its first line was indistinguishable from a healthy one
	// (topology-reconcile does exactly that when NatsConfPath is empty).
	b.cl.loops.GoEvery(loopCtx, "audit-publisher", pub.cfg.Poll, pub.Run)
	b.cl.loops.GoEvery(loopCtx, "reconciler", rec.cfg.Poll, rec.Run)
	// D9 §17 step 10b: the leader-gated observability poll (broker_down / raft_lag).
	b.cl.loops.GoEvery(loopCtx, "observe", observeTickInterval, b.runObserveLoop)
	// C3: the per-broker (NOT leader-gated) NATS topology reconcile loop.
	b.cl.loops.GoEvery(loopCtx, "topology-reconcile", topoReconcileInterval, b.runTopologyReconcileLoop)
	if poster != nil {
		// No cadence: it wakes on an event, not a tick. Cadence 0 tells a status reader that Iters is
		// event-driven — it grows once per event DEQUEUED AND PROCESSED, so Iters == 0 means "nothing
		// arrived", never "the endpoint refused us". Whether the endpoint accepted is a separate
		// question, answered by the alert_webhook.accepted/rejected counters (external review B2-4).
		poster.beat = func() { b.cl.loops.Beat(webhookLoopName) }
		b.cl.webhook.Store(poster)
		b.cl.loops.Go(loopCtx, webhookLoopName, poster.Run)
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
// refused unless every JS stream would STILL be at its target replica count once the node being
// retired is gone. Fail-closed throughout — an incomplete observation reports NOT ready. nodeID is
// unused (the predicate is cluster-wide: a stream below target means retiring ANY node risks
// losing a replica).
//
// POST-REMOVAL TARGET, NOT CURRENT TARGET (prerelease audit broker-cluster-ops/#15). This used to
// ask AllAtTarget, i.e. "is every stream at the target implied by the CURRENT voter count". With a
// voter already down that question has no yes: 3 voters imply a target of 3, the dead one means an
// actual of 2, and retire is refused on every tick forever. Retiring a dead voter is the case
// cluster-runbook.md §2 documents and the case operators actually reach this gate for, and it was
// structurally impossible. Asking about the post-removal target is the question the runbook was
// already describing, and it stays fail-closed for the situations that should fail: two dead voters
// out of three still gives actual 1 against target 2, and is still refused.
func (b *Broker) clusterStreamsReady(nodeID string) (bool, error) {
	ap := b.cl.auditPub.Load()
	if ap == nil {
		// Fail-closed, consistent with AllAtTarget's own contract: an
		// unobservable cluster is NOT ready.
		return false, fmt.Errorf("broker: audit publisher not wired yet")
	}
	// B7: bounded, and safe to bound precisely BECAUSE the error path is already fail-closed — a
	// deadline exceeded returns (false, err), i.e. "not ready", which is the same answer an
	// unobservable cluster already gets two lines above. This is the second of the two unbounded
	// ObserveReplicas calls that shared the leader tick; missing it would have left the path
	// structurally unbounded while looking fixed.
	ctx, cancel := context.WithTimeout(context.Background(),
		clusterReplicaObserveBudget(b.observeStreamCountForBudget()))
	defer cancel()
	rep, err := ap.ObserveReplicas(ctx)
	if err != nil {
		return false, err
	}
	// The post-removal voter count. An unreadable count is NOT an excuse to relax the
	// gate — fall back to the observation's own target, which is the stricter question.
	nv, nerr := b.cl.node.NumVoters()
	if nerr != nil || nv <= 1 {
		return rep.AllAtTarget(), nil
	}
	// WHICH node is being retired matters, and this gate used to throw it away.
	//
	// origin: prerelease audit round 2, G-1. Asking "is Actual >= the post-removal
	// target" while ignoring nodeID counts a caught-up replica living ON the node about
	// to be removed toward the floor it is supposed to survive: three voters, one lagging
	// peer, a stream at Actual=2 passes a target of 2 and drops to 1 the instant the
	// retire completes. The comment claiming "no conservatism was traded away" was wrong.
	//
	// An unresolvable server name means we cannot attribute placement, so we fall back to
	// the STRICTER pre-removal question rather than guess.
	server := natsServerIDFor(b, nodeID)
	if server == "" {
		return rep.AllAtTarget(), nil
	}
	return replicaReportSurvivesRemoval(rep, server, jsstream.ReplicasFor(nv-1)), nil
}

// natsServerIDFor maps a raft node id to the NATS server name StreamInfo reports, via the
// replicated roster column that exists for exactly this bridge. Empty when unknown.
//
// A package-level function taking *Broker, not a method: the structural-budget ratchet
// pins this type's method count exactly.
func natsServerIDFor(b *Broker, nodeID string) string {
	if nodeID == "" {
		return ""
	}
	var s sql.NullString
	if err := b.read().SQL().QueryRow(
		`SELECT nats_server_id FROM cluster_nodes WHERE node_id=?`, nodeID).Scan(&s); err != nil {
		return ""
	}
	if !s.Valid {
		return ""
	}
	return s.String
}

// observeStreamCountForBudget is how many streams the next ObserveReplicas is expected to enumerate.
// It only sizes the deadline; nothing else reads it.
//
// Two lower bounds are combined: the PREVIOUS observation's stream count includes the events stream,
// every ACTIVE session's history stream AND every live `OBJ_xfer-*` stream; the current
// `events + one history per ACTIVE session` count sees session growth since that observation. The
// maximum retains the transfer coverage without ever regressing below the old live-session estimate.
//
// Only the leader writes the observation cache. A follower therefore uses the live-session estimate for
// its whole life, and a newly elected leader uses it until its first complete observation. A demoted
// leader retains its old count; maxing it with the live floor is conservative in either direction.
//
// Deliberately best-effort: a DB error keeps any cached observation (or yields the base budget if none
// exists). The read goes through b.read() so it cannot write, and it is one indexed COUNT against the
// local replica — never another round trip to the JS meta.
func (b *Broker) observeStreamCountForBudget() int {
	observed := b.observedStreamCount()
	var sessions int
	if err := b.read().QueryRow(`SELECT COUNT(*) FROM sessions WHERE state = 'ACTIVE'`).Scan(&sessions); err != nil {
		return observed
	}
	liveFloor := sessions + 1 // + the events stream, which ObserveReplicas always walks
	if observed > liveFloor {
		return observed
	}
	return liveFloor
}

// clusterJSPlaceProbeTimeout bounds the join-side placement probe.
//
// It USED to carry a caveat saying this cap "does not keep the observe loop responsive and must not be
// described as if it did", because the same inline tick also embedded a 2s healthPoll and TWO unbounded
// ObserveReplicas calls (one via topoConvergedForOp -> StatusReport -> streamObserve, one via
// streamsReady). B7 closed both, so the caveat is gone rather than merely stale: every JS observation
// on this tick now carries a deadline.
const clusterJSPlaceProbeTimeout = 3 * time.Second

// clusterReplicaObserveBudget bounds ONE JS replica observation, and it SCALES WITH THE WORK.
//
// A constant was wrong in shape, not merely in value (internal review B7-02). ObserveReplicas walks the
// events stream and then EVERY session's history stream, serially, two JS round trips each
// (audit_publisher.go's ObserveReplicas). A fixed 3s therefore covers a broker with three sessions and
// silently expires on one with thirty — and because a failed observation used to render as a MEASURED
// zero, expiry showed up to the operator as "stream replicas below target (0/N)" and DEGRADED on a
// perfectly healthy cluster. The zero-vs-unobserved half of that is fixed in clusterstatus.go; this is
// the half that stops the expiry from being routine in the first place.
//
// The base is the sibling JS probe's budget (same meta, same kind of work — not a second invented
// number); the per-STREAM term is what the sibling does not need because it queries one stream.
// The RELATION, not the numbers, is pinned by TestReplicaObserveBudgetScalesWithStreams.
//
// WHAT THE SCALING TERM COUNTS, AND WHY IT CHANGED (external review RB2 doubt 3)
// -----------------------------------------------------------------------------
// It used to count ACTIVE SESSIONS. That predicts the per-session history streams exactly — ListSIDs IS
// `sessions WHERE state='ACTIVE'` — but it counts the `OBJ_xfer-*` streams as ZERO, and ObserveReplicas
// enumerates every one of those too (via ListXferStreams). Those streams can OUTLIVE the session that
// created them, so a transfer-heavy or orphan-heavy broker was handed a budget sized for a fraction of
// the round trips it was about to make, and "routinely unobserved" was the predictable result.
//
// The scaling input is now the PREVIOUS observation's own stream count, which already includes events +
// history + xfer and costs nothing to obtain.
//
// TWO LIMITS OF THAT INPUT:
//
//	it lags an OBJ_xfer-only growth burst until the next observation enumerates the complete work set.
//	ObserveReplicas now does that enumeration before per-stream collection and returns StreamCount even
//	if collection times out, so the following pass is correctly sized without treating partial replica
//	state as observed. Session growth is covered immediately by the live floor above.
//
//	only the LEADER observes. cacheReplicaSnapshot is written from the alert reconciler's observe tick,
//	which is leader-only. A follower and a just-elected leader use the live session floor until they
//	have a complete cached observation; this still cannot see orphan transfer streams. A DEMOTED leader
//	keeps whatever count it last measured. That direction is conservative — an over-large budget is a
//	bound that stops binding, and only costs wall-clock when the JS meta is unresponsive.
//
// STILL NOT CHARACTERISED, deliberately: the 250ms per-stream coefficient. Sizing it properly needs a
// measured transfer-heavy fleet, and inventing a number would be exactly the "looks like
// characterisation" artefact this batch spent its budget deleting. What is fixed here is the structural
// mismatch (a term that ignored a whole class of streams), not the constant.
const (
	clusterReplicaObserveBase = clusterJSPlaceProbeTimeout
	// clusterReplicaObservePerStream is per STREAM enumerated, not per session (RB2 doubt 3). The name
	// used to say PerSession, which is how the xfer streams came to be uncounted: the term looked correct
	// for the quantity it was multiplied by.
	clusterReplicaObservePerStream = 250 * time.Millisecond
	// clusterReplicaObserveCeiling caps the derived budget. Without it a broker with a thousand stale
	// sessions would hand the leader tick a multi-minute deadline, which is the unbounded behaviour this
	// replaced wearing a different hat. At the cap the observation genuinely cannot finish and the
	// UNOBSERVED result is the honest answer.
	//
	// It binds at (30s - 3s) / 250ms = 108 streams — roughly 54 sessions each carrying one transfer.
	// Above that the scaling term stops mattering and every broker gets 30s, so on a genuinely
	// transfer-heavy fleet the old session-count term and the new stream count give the SAME answer.
	// What the fix buys is the range below the cap, which is where the observed "routinely unobserved"
	// brokers actually sat. Raising the cap is not obviously right (it lengthens a tick other work waits
	// on) and needs the same measured fleet the 250ms coefficient does.
	clusterReplicaObserveCeiling = 30 * time.Second
)

func clusterReplicaObserveBudget(streams int) time.Duration {
	if streams < 0 {
		streams = 0
	}
	d := clusterReplicaObserveBase + time.Duration(streams)*clusterReplicaObservePerStream
	if d > clusterReplicaObserveCeiling {
		return clusterReplicaObserveCeiling
	}
	return d
}

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
	st, oerr := jsstream.CollectStreamState(ctx, brokerJS(b), jsstream.EventsStreamName, target)
	if ok, detail := jsPlaceableFrom(target, st, oerr); !ok {
		return false, detail
	}
	// EXTERNAL REVIEW F3: then MEASURE, do not infer. The counts above are a proxy for "a new R=N asset
	// is creatable", and the review showed a proxy can be satisfied by state that says nothing about
	// now — tether never peer-removes, so a retired member lingers in the assignment list. The canary is
	// an EMPTY stream created at the target factor and immediately deleted: it asks the meta the exact
	// question the CLI's contract promises an answer to, and because it carries no data it introduces
	// none of the byte-copy wait that gating on catch-up would.
	if err := jsstream.ProbeMetaCanPlace(ctx, brokerJS(b), target, b.cfg.Logger); err != nil {
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
	// b.cl is nil until wireClusterLate runs, and a liveness write must never be
	// the thing that panics because it arrived a moment early (or because a test
	// exercises the clustered branch without the full cluster wired). The local
	// handle is the correct fallback: in single mode it IS the liveness handle,
	// and before wiring there is no replicated one to prefer.
	if b.clusterMode && b.cl != nil && b.cl.node != nil {
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
		return planAuthorizedSessionCreate(db, sid, fp, pinHash, b.cfg.Now())
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

// planAuthorizedSessionCreate is the single authoritative session-create planner.
// Both leader-local and forwarded writes decide admission from the leader's committed
// DB immediately before constructing the raft command. An origin-side check is only an
// early refusal: a creator can be revoked after that read and before this closure runs.
func planAuthorizedSessionCreate(
	db *sql.DB, name, fp, pinHash string, now time.Time,
) (*cluster.Command, error) {
	allowed, err := session.MayCreateSession(db, fp)
	if err != nil {
		return nil, fmt.Errorf("session create: read the creator allow-list: %w", err)
	}
	if !allowed {
		return nil, session.ErrNotAllowedToCreate
	}
	return session.PlanCreate(db, name, name, fp, pinHash, now)
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
func (b *Broker) registerNode(in node.RegisterInput, previousNID string, localProcesses []proto.LocalProcess) error {
	if !b.clusterMode {
		return node.Register(b.cfg.DB, in, b.cfg.Now())
	}
	// Commit the identity row and any process-row adoption in ONE existing
	// OpNodeRegister entry. Adding a new raft op would fork an unupgraded
	// replica during a same-proto roll; genericExec already permits this
	// additive statement shape.
	//
	// STILL THROUGH proposeOrForward, and that is load-bearing rather than
	// habit. handleRegister is leader-only, so the forward branch is
	// unreachable — but the LEADER branch carries the ErrNotLeader mapping, and
	// dropping it is a fleet-wide outage: leadership can be lost between
	// isClusterFollower() and raft.Apply, and the raw raft error then surfaces
	// as a terminal store_error, which the agent's register loop treats as a
	// PERMANENT rejection and exits the process on. One ordinary leader failover
	// would take every agent down with it. (See proposeOrForward's own comment —
	// audit M2 / write-forward F3 bought that mapping the hard way.)
	payload, err := json.Marshal(in)
	if err != nil {
		return err
	}
	if err := b.proposeOrForward(VerbNodeRegister, "", payload, func(db *sql.DB) (*cluster.Command, error) {
		return planRegisterWithRefiles(db, in, previousNID, localProcesses, b.cfg.Now())
	}); err != nil {
		return err
	}
	if err := node.Heartbeat(b.livenessDB(), in.SID, in.NID, b.cfg.Now()); err != nil {
		b.cfg.Logger.Debug("broker: register liveness write deferred (apply lag)", "sid", in.SID, "nid", in.NID, "err", err)
	}
	return nil
}

func planRegisterWithRefiles(db *sql.DB, in node.RegisterInput, previousNID string,
	localProcesses []proto.LocalProcess, now time.Time) (*cluster.Command, error) {
	cmd, err := node.PlanRegister(db, in, now)
	if err != nil {
		return nil, err
	}
	pids := make([]string, 0, len(localProcesses))
	for _, p := range localProcesses {
		pids = append(pids, p.PID)
	}
	moves, err := proc.PlanRefileStatements(in.SID, previousNID, in.NID, pids)
	if err != nil {
		return nil, err
	}
	cmd.Body = append(cmd.Body, moves...)
	return cmd, nil
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
//
// sid IS NOT OPTIONAL FOR ANY CALLER IN THIS REPO. Every call site has it in hand
// already: exec.go parses it out of the subject, reconcileOnRegister is called with it.
// It rides the forward payload so the LEADER applies the same fence the single-node
// writer does; a fence that only held on the mode the attacker did not happen to hit
// would not be one.
//
// TWO WRITERS, NAMED SEPARATELY — origin: prerelease audit round 2, L-F3. This used to
// say "see proc.MarkExited for what it fences", which names the SINGLE-mode function
// while the paragraph is about the cluster path. They are different functions with the
// same fence: proc.MarkExited (below, single) and proc.PlanMarkExited (via the leader,
// cluster). Pointing a reader at the one that is not on the path being described is how
// a fence gets audited once and believed twice.
//
// It is also not an unconditional guarantee, and the old wording read like one. What the
// code guarantees is that THIS function always sends the sid; that the leader USES it is
// a property of cluster_forward.go's VerbProcMarkExited handler, and the two are only
// held together by TestAForwardedProcExitCarriesTheSessionFence, which drives both.
func (b *Broker) markProcExited(pid, sid string, exitCode int, when time.Time) error {
	if !b.clusterMode {
		return proc.MarkExited(b.cfg.DB, pid, sid, exitCode, when)
	}
	payload, err := json.Marshal(ProcMarkExitedPayload{Pid: pid, Sid: sid, ExitCode: exitCode, EndedAt: when})
	if err != nil {
		return err
	}
	return b.proposeOrForward(VerbProcMarkExited, "", payload, func(db *sql.DB) (*cluster.Command, error) {
		return proc.PlanMarkExited(db, pid, sid, exitCode, when)
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

// setSessionCreator routes an admission-table write. Same shape and same reason as
// evictNode above: the adminsock wires it only in cluster mode, single mode leaves the
// seam nil and writes directly. origin: prerelease audit round 2.
//
// A package-level function taking *Broker, not a method: the structural-budget ratchet
// pins this type's method count exactly, and it caught this one on the way in.
func setSessionCreator(b *Broker, fp, addedBy, note string, allow bool) error {
	if !b.clusterMode || b.cl == nil {
		return fmt.Errorf("broker: setSessionCreator is cluster-mode only (single mode writes directly)")
	}
	// MIXED-VERSION GATE BEFORE THE PROPOSE. A voter whose binary predates
	// OpSessionCreatorSet does not fail-stop on it — decodeCommand poison-SKIPS it, so the
	// entry advances applied_index without running the SQL and that replica's admission
	// table silently diverges from the leader's. For a policy table that means an
	// operator's `session-allow` reports success while one broker keeps refusing the
	// fingerprint, or a `--remove` leaves it able to create sessions wherever a ctl lands.
	// origin: prerelease audit increment 2 internal review — six lanes.
	if b.cl.admin != nil {
		verb := "session-allow " + fp
		if !allow {
			verb = "session-allow --remove " + fp
		}
		if err := assertAllVotersSupportSessionCreatorOps(b.cl.admin, verb); err != nil {
			return err
		}
	}
	now := b.cfg.Now()
	payload, err := json.Marshal(SessionCreatorPayload{FP: fp, AddedBy: addedBy, Note: note, Allow: allow})
	if err != nil {
		return err
	}
	return b.proposeOrForward(VerbSessionCreatorSet, "", payload, func(*sql.DB) (*cluster.Command, error) {
		return session.PlanSetCreator(fp, addedBy, note, allow, now)
	})
}

// seedSessionCreators runs the ONE-SHOT upgrade backfill: every fingerprint that already
// owns a session is admitted to create sessions, so an existing deployment keeps working
// across the upgrade that introduced admission control. Returns how many were admitted.
//
// origin: prerelease audit increment 2 internal review. This was an INSERT…SELECT inside
// migration 0019; see that file for the three ways that was wrong. The properties this
// form has and that one did not:
//
//   - ONCE. The cluster_meta marker is written in the same transaction (single mode) or
//     the same raft entry (cluster mode) as the rows, so a later upgrade, a re-run
//     migration, or installing an older snapshot cannot re-derive the table and undo a
//     revocation.
//   - ONE WRITER. In cluster mode only the leader proposes, and every replica learns the
//     result through the log instead of computing its own from local state at its own
//     restart time.
//   - LEADERLESS IS NOT AN ERROR. A follower simply returns; the leader does it, and every
//     follower gets it replicated. A broker that boots while an election is in progress
//     tries again on its next boot, which is why the marker rather than a timer is what
//     stops it.
//
// A package-level function taking *Broker, not a method: the structural-budget ratchet
// pins this type's method count exactly.
func seedSessionCreators(b *Broker, now time.Time) (int, error) {
	db := b.read().SQL()
	seeded, err := session.CreatorsSeeded(db)
	if err != nil {
		return 0, err
	}
	if seeded {
		return 0, nil
	}
	fps, err := session.OwnersNeedingAdmission(db)
	if err != nil {
		return 0, err
	}
	if !b.clusterMode || b.cl == nil {
		return len(fps), session.SeedCreatorsLocally(db, fps, now)
	}
	// Cluster mode: the leader proposes, followers wait for the log. Not forwarded —
	// unlike an operator's `session-allow`, nothing is waiting on this and every broker
	// re-checks it on its own next boot, so a forward would only add a way for N brokers
	// to race the same backfill through the leader.
	if !b.cl.node.IsLeader() {
		return 0, nil
	}
	// The same mixed-version gate an operator's admission write goes through: a voter that
	// would poison-skip the op must not be handed a backfill either.
	if b.cl.admin != nil {
		if err := assertAllVotersSupportSessionCreatorOps(b.cl.admin, "session-creator upgrade backfill"); err != nil {
			return 0, err
		}
	}
	// The plan closure re-reads the marker under the leader's applyMu: between the check
	// above and this point another broker's proposal may have committed the backfill
	// already, and a nil command is Propose's documented no-op.
	if err := b.cl.node.Propose(func(planDB *sql.DB) (*cluster.Command, error) {
		already, cerr := session.CreatorsSeeded(planDB)
		if cerr != nil || already {
			return nil, cerr
		}
		return session.PlanSeedCreators(fps, now)
	}); err != nil {
		return 0, err
	}
	return len(fps), nil
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

// refileProc moves a RUNNING process row to a different node name after a lease
// adoption, so the agent's own long-running work stops being filed under the
// name it left.
//
// SINGLE MODE ONLY, DELIBERATELY. Every other mutator here has a raft twin
// because losing the write would lose correctness; this one is bookkeeping. The
// safety property — an agent's live processes are never killed after a rename —
// is carried by reconcileOnRegister's orphan rule (a pid with a row ANYWHERE is
// not an orphan), which needs no write at all. What re-filing buys on top is
// that `tether ps <lease-name>` lists the work that instance is actually
// running, instead of leaving it under the previous name forever.
//
// So in cluster mode this logs and moves on rather than growing a new raft verb,
// an FSM arm, a determinism ledger entry and an N-1 story for a display detail.
// The live fleet runs single mode; if that changes and the mis-filing becomes
// visible, the verb is a small, self-contained addition.
// A package-level function taking *Broker, not a method: the structural-budget
// ratchet pins this type's method count exactly, and growing a type's surface is
// the thing that gate exists to make deliberate. adjudicateLease is the same
// shape for the same reason.
func refileProc(b *Broker, sid, pid, from, to string) (bool, error) {
	if b.clusterMode {
		return false, fmt.Errorf("broker: late cluster process refile for %s/%s (%s -> %s): register transaction omitted the move",
			sid, pid, from, to)
	}
	return proc.Refile(b.cfg.DB, sid, pid, from, to)
}

// markNodeOfflineOnRelease takes a released lease row OFFLINE immediately,
// instead of waiting out OfflineAfter.
//
// A farewell is the holder stating that it has stopped. Leaving the row ONLINE
// for the full window means the name it just handed back still reads as
// occupied, so its own restart is issued the next suffix and the operator's
// addresses drift on every bounce (external review F11).
//
// Cluster mode routes through the same LOCAL status write the reconciler uses.
// Register/farewell handling is leader-only, so this immediately frees the
// allocator's authoritative leader view; follower-local liveness views age out
// through their ordinary sweep. Liveness is deliberately not replicated.
// It writes through livenessDB in BOTH modes, and that is not a shortcut:
// nodes.status is a liveness column, written locally on every node by the very
// same reconciler that ages a silent agent to OFFLINE. Routing it through raft
// would make this transition behave differently from every other liveness
// transition in the system.
//
// An earlier revision returned nil in cluster mode with a log line. external
// review caught that (TestClusterReleaseDoesNotLeaveTheLeaseRowOnline): the
// caller cannot tell "released" from "logged and skipped", so a clustered fleet
// kept the suffix drift F11 was supposed to remove while the code read as
// though it had been fixed. Reporting success for work not done is the one
// thing this must not do — the same lesson as refileProc, applied to the mode
// that actually needed the write.
func markNodeOfflineOnRelease(b *Broker, sid, nid string) error {
	return node.ReleaseLeaseRow(b.livenessDB(), sid, nid)
}
