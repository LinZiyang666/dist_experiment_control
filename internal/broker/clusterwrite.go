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
	"fmt"
	"os"
	"path/filepath"
	"strings"
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
	auditPub *AuditPublisher
	alertRec *AlertReconciler
	// cancel stops the leader-gated loops (a child of Run's ctx); loopDone receives one
	// signal per loop on exit so the ordered shutdown can JOIN them (not guess via defer).
	cancel   func()
	loopDone chan struct{}
	// admin is the D7 membership orchestrator wired into the adminsock cluster backend;
	// kept here so the d9_integration harness can drive `cluster add` (AddNode) directly.
	admin *ClusterAdmin
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
	for i := 0; i < cap(b.cl.loopDone); i++ {
		select {
		case <-b.cl.loopDone:
		case <-time.After(10 * time.Second):
			b.cfg.Logger.Warn("broker: cluster loop did not exit within 10s of shutdown")
		}
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
		return fmt.Errorf("broker: on-disk tunnel cert fingerprint %q matches neither the pinned "+
			"cluster_nodes.cert_fp %q nor an unexpired cert_fp_prev %q for node %q — agents pin against those and "+
			"would reject EVERY dial (silent data-plane outage). Restore the pinned cert or re-run "+
			"`tether cluster rotate-tunnel-cert`", gotFP, self.CertFP, self.CertFPPrev, b.cl.node.SelfID())
	}
	b.AttachClusterSeam(b.cl.node.SelfID(), cert)
	return nil
}

func (b *Broker) loadStableTunnelCert() (*tls.Certificate, string, error) {
	certPEM, err := os.ReadFile(filepath.Join(b.cfg.ClusterSecretsDir, secretTunnelCert))
	if err != nil {
		return nil, "", fmt.Errorf("broker: read stable tunnel cert: %w", err)
	}
	keyPEM, err := os.ReadFile(filepath.Join(b.cfg.ClusterSecretsDir, secretTunnelKey))
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

func (b *Broker) prepareTunnelCertRotate(newFP string) (func(), error) {
	if b.tunnelSrv == nil {
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
		b.tunnelSrv.SetCertificate(cert)
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
		fwd = NewForwarder(nc, b.cfg.ExposeForwardTimeout())
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

	// D4 write-forward responder + D8b health/alert responders. Each stays silent on a
	// follower (the leader answers); collected for ordered unsubscribe.
	subscribers := []func() (*nats.Subscription, error){
		func() (*nats.Subscription, error) { return SubscribeClusterApply(nc, node, b.cfg.Now) },
		func() (*nats.Subscription, error) { return b.subscribeTunnelClose(nc) },
		func() (*nats.Subscription, error) { return SubscribeClusterHealth(nc, node, b.cfg.DB, b.cfg.Now) },
		func() (*nats.Subscription, error) { return SubscribeClusterCursor(nc, node, b.cfg.DB, b.cfg.Now) },
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
	rec := NewAlertReconciler(AlertReconcilerConfig{
		Node: node, DB: b.cfg.DB, Propose: node.Propose, Now: b.cfg.Now, Logger: b.cfg.Logger,
		Observe: pub.ObserveReplicas,
	})
	b.cl.auditPub = pub
	b.cl.alertRec = rec
	// D7 membership orchestrator — constructed in cluster mode regardless of the admin
	// socket (the adminsock backend, wired later in Run, uses it iff the socket is set).
	b.cl.admin = NewClusterAdmin(node, b.cfg.Logger)
	b.cl.admin.prepareTunnelCertRotate = b.prepareTunnelCertRotate
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

	// The loops run on a child ctx so the ordered shutdown can cancel + JOIN them; each
	// signals loopDone on exit. (Run's ctx cancel also reaches this child, so a hard
	// shutdown still stops them.)
	loopCtx, cancel := context.WithCancel(ctx)
	b.cl.cancel = cancel
	b.cl.loopDone = make(chan struct{}, 3)
	go func() { defer func() { b.cl.loopDone <- struct{}{} }(); pub.Run(loopCtx) }()
	go func() { defer func() { b.cl.loopDone <- struct{}{} }(); rec.Run(loopCtx) }()
	// D9 §17 step 10b: the leader-gated observability poll (broker_down / raft_lag).
	go func() { defer func() { b.cl.loopDone <- struct{}{} }(); b.runObserveLoop(loopCtx) }()
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
	rep, err := b.cl.auditPub.ObserveReplicas(context.Background())
	if err != nil {
		return false, err
	}
	return rep.AllAtTarget(), nil
}

// livenessDB returns the handle for LIVENESS-column writes (last_heartbeat_at, status)
// — the local, non-replicated state (§3.5: Apply never writes it, rebuilt on failover).
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
			return cluster.ErrForwardNotLeader
		}
		return err
	}
	return b.cl.forwarder.Forward(verb, reqID, payload)
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
		return nil, err
	}
	return b.readCommittedSession(sid)
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
func (b *Broker) allocatePort(sid, nid, name string, localPort, remotePort int, fp string) (*port.Allocation, error) {
	cfg := b.cfg.PortAllocCfg()
	if !b.clusterMode {
		return port.Allocate(b.cfg.DB, sid, nid, name, localPort, remotePort, fp, cfg)
	}
	homeBroker := ""
	if home := b.resolveHomeForAgent(sid, nid); home != nil {
		homeBroker = home.NodeID
	}
	var captured *port.Allocation
	if err := b.cl.node.Propose(func(db *sql.DB) (*cluster.Command, error) {
		a, cmd, e := port.PlanAllocate(db, sid, nid, name, localPort, remotePort, fp, homeBroker, cfg)
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

func (b *Broker) freePortAllocation(a port.Allocation) error {
	if !b.clusterMode {
		return port.FreeAllocation(b.cfg.DB, a, b.cfg.Now())
	}
	payload, err := json.Marshal(PortFreeAllocationPayload{
		Port: a.Port, SID: a.SID, NID: a.NID, Name: a.Name, TokenHash: a.TokenHash,
	})
	if err != nil {
		return err
	}
	return b.proposeOrForward(VerbPortFreeAllocation, "", payload, func(db *sql.DB) (*cluster.Command, error) {
		return port.PlanFreeAllocation(db, a, b.cfg.Now())
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
	if b.tunnelSrv == nil || info.PublicPort <= 0 {
		return
	}
	if ok := b.tunnelSrv.CloseProxyIf(info); ok {
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

func (b *Broker) revokePortAllocation(a port.Allocation, now time.Time) error {
	if !b.clusterMode {
		return port.RevokeAllocation(b.cfg.DB, a, now)
	}
	payload, err := json.Marshal(PortFreeAllocationPayload{
		Port: a.Port, SID: a.SID, NID: a.NID, Name: a.Name, TokenHash: a.TokenHash,
	})
	if err != nil {
		return err
	}
	return b.proposeOrForward(VerbPortRevoke, "", payload, func(db *sql.DB) (*cluster.Command, error) {
		return port.PlanRevokeAllocation(db, a, now)
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

// readCommittedSession reads a session back by SID after a routed write, tolerating a
// brief follower Apply-lag: a forward returns on LEADER commit, but this replica's local
// Apply may trail by a few ms. The leader path is immediate (Propose waits for local
// Apply), so the loop only ever spins on a follower. Bounded so a genuine miss fails loud.
func (b *Broker) readCommittedSession(sid string) (*session.Session, error) {
	var last error
	for i := 0; i < 50; i++ {
		s, err := session.Get(b.cfg.DB, sid)
		if err == nil {
			return s, nil
		}
		last = err
		time.Sleep(20 * time.Millisecond)
	}
	return nil, fmt.Errorf("broker: session %q not visible after commit (apply lag): %w", sid, last)
}
