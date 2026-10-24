package broker

import (
	"cmp"
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"

	"github.com/LinZiyang666/tether/internal/authcallout"
	"github.com/LinZiyang666/tether/internal/brokercontrol"
	"github.com/LinZiyang666/tether/internal/cluster"
	"github.com/LinZiyang666/tether/internal/controlbudget"
	"github.com/LinZiyang666/tether/internal/jsstream"
	"github.com/LinZiyang666/tether/internal/schema"
	"github.com/LinZiyang666/tether/internal/xferaudit"
	"github.com/LinZiyang666/tether/internal/xferauth"
	"github.com/LinZiyang666/tether/internal/xfercontract"
	"github.com/LinZiyang666/tether/internal/xfercontrol"
	"github.com/LinZiyang666/tether/internal/xferledger"
	"github.com/LinZiyang666/tether/internal/xferserve"
	"github.com/LinZiyang666/tether/internal/xfertrack"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

// The cap0 ledger's disk/replay state machine lives in xferledger. The adapter
// supplies only the current broker backend; it never creates a second tracker.
// origin: hot-update S7 plan §5.1 / §12.2 / P13
type xferInflightRecord = xferledger.LegacyRecord
type ledgerSite = xferledger.LegacySite
type ledgerDisposition = xferledger.LegacyDisposition
type ledgerRowInputs = xferledger.LegacyRowInputs

const (
	xferStrandedSlack = xferledger.LegacyStrandedSlack
	ledgerSiteOutbox  = xferledger.LegacySiteOutbox
	ledgerSitePrimary = xferledger.LegacySitePrimary
	ledgerLeave       = xferledger.LegacyLeave
	ledgerReplay      = xferledger.LegacyReplay
	ledgerSynthesize  = xferledger.LegacySynthesize
)

func legacyTransferLedger(b *Broker, parents ...context.Context) *xferledger.Legacy {
	ctx := b.runCtx
	if len(parents) > 0 {
		ctx = parents[0]
	}
	var streamCommitted func(string, string) (bool, error)
	if js := brokerJS(b); js != nil {
		streamCommitted = func(sid, id string) (bool, error) { return jsstream.TransferTerminalCommitted(ctx, js, sid, id) }
	}
	return &xferledger.Legacy{Config: xferledger.LegacyConfig{
		ClusterDataDir: b.cfg.ClusterDataDir, DBPath: b.cfg.DBPath, Logger: b.cfg.Logger, Now: b.cfg.Now,
		Cluster: b.cl != nil, StreamCommitted: streamCommitted, Live: func(id string) bool { return b.transfers.Get(id) != nil },
		Delete: func(sid, id string) error { return b.deleteXferObject(ctx, sid, id) }, Committed: b.transferTerminalAlreadyCommitted, Commit: b.commitSyntheticTransferTerminal,
		Publish: func(subject string, data []byte, id string) error { return publishAuditDeduped(b, subject, data, id) }, Confirm: xferserve.LegacyConfirm(brokerJS(b)),
	}}
}
func (b *Broker) xferInflightDir() string       { return legacyTransferLedger(b).InflightDir() }
func (b *Broker) xferTerminalOutboxDir() string { return legacyTransferLedger(b).OutboxDir() }
func (b *Broker) writeLedgerRecord(dir string, rec xferInflightRecord) error {
	return legacyTransferLedger(b).WriteRecord(dir, rec)
}
func (b *Broker) stageXferInflightTerminal(id string, rec schema.AuditTransfer) (bool, bool) {
	return legacyTransferLedger(b).StageTerminal(id, rec)
}

func (b *Broker) removeXferInflight(id string) { legacyTransferLedger(b).Remove(id) }

func (b *Broker) replayStagedTerminal(ctx context.Context, rec xferInflightRecord) bool {
	return legacyTransferLedger(b, ctx).ReplayTerminal(rec)
}
func (b *Broker) ledgerProtectedObjects(now time.Time) (map[string]bool, map[string]bool, error) {
	return legacyTransferLedger(b).ProtectedObjects(now)
}
func xferLedgerSubdir(dir, db, sub string) string { return xferledger.LegacySubdir(dir, db, sub) }
func xferInflightFilename(id string) string       { return xferledger.LegacyFilename(id) }
func transferTimeoutFor(tier string, size int64) time.Duration {
	return xferledger.LegacyTimeout(tier, size)
}
func transferTierFloorFor(tier string) time.Duration { return xferledger.LegacyTierFloor(tier) }

func readXferInflight(path string) (xferInflightRecord, error) {
	return xferledger.ReadLegacyRecord(path)
}
func ledgerRowDisposition(in ledgerRowInputs) (ledgerDisposition, string) {
	return xferledger.LegacyRowDisposition(in)
}
func (b *Broker) finalizeStrandedXfers(ctx context.Context) (int, error) {
	// A live imported tracker protects objects from GC, but must still drive
	// its own history/cleanup responsibilities. Never run legacy synthesis on
	// a v1 grant merely because the original byte deadline has passed.
	// origin: hot-update S7 plan §5.3 / M11 / M13
	if b.cfg.TransferFactory != nil || b.transferRuntime.Load() != nil {
		return advanceBrokerTransfers(ctx, b)
	}
	return legacyTransferLedger(b, ctx).Finalize()
}

// writeXferInflight persists the in-flight ledger record for e (called right after transfers.put(), before
// the prepare is forwarded — a crash between put() and this loses only what a crash before put() already
// does). Best-effort + atomic (temp + rename): a failure only degrades #57 recovery, never the transfer.

func (b *Broker) writeXferInflight(e *transferEntry) {
	dir := b.xferInflightDir()
	if dir == "" {
		// No durable location at all — an in-memory test broker. The excuse that used
		// to be written here ("single-broker audit publish is best-effort anyway") was
		// FALSE and it is what kept #57 switched off in the shape it mattered most:
		// publishAudit goes to JetStream whenever brokerJS(b) != nil, and a single broker
		// supporting tier-B necessarily has JetStream. So a dangling start row on a
		// single broker is exactly as permanent as on a cluster — the audit stream
		// keeps a start that no complete or failed ever answers. Corrected by the
		// 2026-09-02 prerelease audit together with the xferInflightDir fallback.
		return
	}
	record := b.transfers.Record(e)
	if err := b.writeLedgerRecord(dir, xferInflightRecord{
		TransferID: record.TransferID, Session: record.SID, Node: record.NID,
		ActorNkey: record.Actor, ActorFp: record.ActorFP, Verb: record.Verb, Tier: record.Tier,
		Bucket: record.Bucket, Path: record.Path, StartedAt: record.StartedAt, Size: record.Size, Start: record.Start,
	}); err != nil {
		b.cfg.Logger.Warn("broker: install xfer-inflight ledger", "err", err)
	}
}

// The cap0 audit owner keeps content-derived IDs and the original retry bound.
func (b *Broker) commitSyntheticTransferTerminal(rec schema.AuditTransfer) bool {
	return (xferserve.LegacyAudit{Cluster: b.cl != nil, Forward: b.transferAuditForwardSync,
		Publish: func(subject string, data []byte, id string) error { return publishAuditDeduped(b, subject, data, id) },
		Logger:  b.cfg.Logger, Attempts: transferAuditForwardAttempts, Backoff: transferAuditForwardBackoff}).Commit(rec)
}

// transferTerminalAlreadyCommitted asks the replicated dedup ledger whether this exact terminal record
// has already been committed (external RE-REVIEW F1). A nil cluster runtime (single-broker mode) or an
// unreadable ledger answers "unknown" -> false, which degrades to the replay path: replaying is still
// idempotent at Apply, so an unknown answer is safe, never a double-count.

func (b *Broker) transferTerminalAlreadyCommitted(rec schema.AuditTransfer) (bool, error) {
	if b.cl == nil || b.cl.node == nil {
		return false, nil
	}
	reqID, err := xferaudit.TransferRecordReqID(rec)
	if err != nil {
		return false, err
	}
	return cluster.ReqIDCommitted(b.cl.node.RODB(), reqID)
}

// BrokerTransferFactory is the outer assembly seam, not a second leaf factory.
// It returns a sleeping tracker restored from the reconciled ledger/image and
// handlers made from the control owner's single registration descriptor. Leaf
// constructors keep their frozen xfercontract signatures.
// origin: hot-update S7 plan §8.1 / §12.2 / P22
type BrokerTransferFactory func(*Broker, jetstream.JetStream, controlbudget.Deadline) (*BrokerTransfers, error)

// BrokerTransferSubscription carries an already-authenticating control adapter.
// Cold and hot callers consume the same descriptors; continuation subscriptions
// must be broadcast so the original owner receives them after a home move.
type BrokerTransferSubscription = xfercontrol.Subscription

// BrokerTransfers contains current bindings only. The leaves own entry maps,
// watchdogs, canonical decisions, revision serializers and retirement state.
// Its fields are immutable after installation; replacement requires the image
// admission fence and joining the old owners, not assignment during a callback.
type BrokerTransfers struct {
	Assembly      *xferserve.Assembly
	Capacity      xfercontract.CapacityAccount
	Origin        xfercontract.Origin
	Bindings      xfercontract.Bindings
	Tracker       xfercontract.TrackerManager
	Ledger        xfercontract.LedgerPort
	Controller    xfercontract.Controller
	Finalizer     xfercontract.FinalizerPort
	Collector     xfercontract.Collector
	Buckets       xfercontract.BucketPort
	Subscriptions []BrokerTransferSubscription
	Settlement    xferauth.SessionBarrier
	Forward       *TransferClusterForward
	// Link delivers grants/receipts to the original agent; Release removes a
	// private state root of an in-memory broker after its owners joined.
	// origin: hot-update S7 plan §6.3 / P22 — S7 production activation
	Link      *xferserve.Link
	Release   func() error
	Decisions xfercontract.DecisionPort
}

// InstallBrokerTransfers publishes only a complete current graph. Missing
// census evidence is recovery, never an empty tracker that authorizes GC.
// The caller owns cleanup after any error, including a failed Bind.
// origin: hot-update S7 plan §5.3 / §8.2 / M11
func InstallBrokerTransfers(b *Broker, current *BrokerTransfers, deadline controlbudget.Deadline) error {
	if b.transferRuntime.Load() != nil {
		return fmt.Errorf("%s: current transfer graph is already installed", xfercontract.CodeOwnerRecovering)
	}

	if current == nil {
		return fmt.Errorf("%s: incomplete broker transfer graph", xfercontract.CodeOwnerRecovering)
	}
	if err := xfercontrol.ValidateSubscriptions(current.Subscriptions); err != nil {
		return err
	}
	if err := xfercontrol.BindRuntime(brokerTransferRuntime(current), current.Bindings, deadline); err != nil {
		return err
	}
	if !b.transferRuntime.CompareAndSwap(nil, current) {
		return fmt.Errorf("%s: current transfer graph is already installed", xfercontract.CodeOwnerRecovering)
	}
	return nil
}

// BrokerTransferSubscriptions is also the hot-wiring input. It returns a copy
// so subscription setup cannot mutate the factory's frozen registration table.
func BrokerTransferSubscriptions(b *Broker) []BrokerTransferSubscription {
	current := b.transferRuntime.Load()
	if current == nil {
		return nil
	}
	return xfercontrol.OrderSubscriptions(current.Subscriptions)
}

func startBrokerTransfers(ctx context.Context, b *Broker, nc *nats.Conn) error {
	deadline, err := transferPassDeadline(ctx)
	if err != nil {
		return err
	}
	current, err := installBrokerTransferGraph(b, nc, deadline)
	if current == nil || err != nil {
		return err
	}
	var installed []*nats.Subscription
	cleanup := func(cause error) error {
		for _, sub := range installed {
			cause = errors.Join(cause, sub.Unsubscribe())
		}
		return errors.Join(cause, uninstallBrokerTransfers(b, current, deadline))
	}
	for _, registration := range BrokerTransferSubscriptions(b) {
		// Plain SUB is deliberate even in N3. Queue assignment can send an
		// origin-bound request to a broker that must remain silent.
		// origin: hot-update S7 plan §6.3 / T39
		sub, err := nc.Subscribe(registration.Subject, brokerBusinessHandler(b, registration.Handler))
		if err != nil {
			return cleanup(err)
		}
		installed = append(installed, sub)
		trackBrokerSubscription(b, sub)
	}
	remaining, err := controlbudget.Remaining(deadline)
	if err != nil {
		return cleanup(err)
	}
	if err := nc.FlushTimeout(remaining); err != nil {
		return cleanup(err)
	}
	return nil
}

func brokerTransferFactory(b *Broker) BrokerTransferFactory {
	if b.cfg.TransferFactory == nil && brokerJS(b) != nil {
		// Production activation: every JetStream broker (tether serve, single
		// or cluster) installs the real continuation graph and its bucket
		// coordinator. Without it a capable ctl has no probe responder and
		// even cap0 tier-B provisioning refuses at the coordinator guard.
		// origin: hot-update S7 plan §8.2 / P22 — S7 production activation
		return ServeTransferFactory
	}
	return b.cfg.TransferFactory
}

// installBrokerTransferGraph assembles the graph from the durable ledger and
// installs it before any continuation subscription or destructive pass. Cold
// Run and image StartBusiness (rollback, restored activation) share it; the
// hot path subscribes through transferSubscriptions.
// origin: hot-update S7 plan §5.3 / §6.3 / §8.2 / P22 / P24
func installBrokerTransferGraph(b *Broker, nc *nats.Conn, deadline controlbudget.Deadline) (*BrokerTransfers, error) {
	factory := brokerTransferFactory(b)
	if factory == nil {
		return nil, nil
	}
	current, err := factory(b, brokerJS(b), deadline)
	if err == nil {
		err = InstallBrokerTransfers(b, current, deadline)
	}
	// The current ports and retained origin must exist before RPC callbacks.
	// Session removal reads the settlement barrier from the graph installed
	// above (sessionSettlementBarrier). Mirroring it into b.cfg here was an
	// unsynchronized write racing every session-rm handler, which Run has
	// already subscribed by this point.
	// origin: hot-update S7 lead integration — make test on the merged tree
	if err == nil && current.Settlement != nil {
		err = startBrokerTransferForward(b, nc, current)
	}
	if err != nil {
		return nil, errors.Join(err, uninstallBrokerTransfers(b, current, deadline))
	}
	if b.imageControl == nil && b.cfg.TransferFactory != nil && current.Settlement != nil {
		// An injected factory keeps its hot-subscription table even without
		// an image run. The default graph never does this: a cold non-image
		// Run would then root its disk monitor in a control generation that
		// Run's own cancellation does not reach.
		b.imageControl = newBrokerImageControl(b)
	}
	if b.imageControl != nil {
		b.imageControl.transferSubscriptions = func(*nats.Conn) []TransferImageSubscription {
			var subscriptions []TransferImageSubscription
			for _, spec := range brokerTransferWireSubscriptions(b) {
				subscriptions = append(subscriptions, TransferImageSubscription{Subject: spec.Subject, Handler: spec.Handler})
			}
			return subscriptions
		}
	}
	return current, nil
}

// uninstallBrokerTransfers joins every owner and only then publishes absence;
// a failed join keeps the graph installed as retained responsibility.
func uninstallBrokerTransfers(b *Broker, current *BrokerTransfers, deadline controlbudget.Deadline) error {
	if current == nil {
		return nil
	}
	if err := stopBrokerTransferGraph(current, deadline); err != nil {
		return err
	}
	b.transferRuntime.CompareAndSwap(current, nil)
	return nil
}

// suspendBrokerTransfers is the image cut's transfer join (JoinStoreUsers):
// the graph's readers and ledger writers stop before the store closes. It is
// also the managed terminal stop, so Run's own defer never stops it twice.
// origin: hot-update S7 plan §8.2 / P24 — managed slot transfer stop
func suspendBrokerTransfers(b *Broker, deadline controlbudget.Deadline) error {
	return uninstallBrokerTransfers(b, b.transferRuntime.Load(), deadline)
}

// resumeBrokerTransferGraph reinstalls the graph after a rollback or in the
// restored process; the same-boot durable origin re-adopts every retained row.
func resumeBrokerTransferGraph(b *Broker, nc *nats.Conn, deadline controlbudget.Deadline) error {
	if b.transferRuntime.Load() != nil {
		return nil // a failed suspension kept the original owners
	}
	_, err := installBrokerTransferGraph(b, nc, deadline)
	return err
}

// sessionSettlementBarrier reports the transfer settlement barrier that governs
// a session removal (settleSessionRm), and whether this broker is governed by
// one at all.
//
// LQ1 keeps a session DELETING with rc75 only while a real continuation
// transfer is unsettled. A broker with no continuation capability — no
// transfer factory configured, no transfer graph installed, no barrier
// injected — never issued a continuation Grant, so there is no retained
// responsibility a census could find, and removal must run the pre-S7
// history→OBJ→SQL cascade unchanged. The first P19 cut failed closed on a
// nil barrier instead, which made `session rm` (and the boot DELETING
// resumer) permanently rc75 on every broker the S7 factory is not wired into.
//
// A continuation-capable broker whose barrier is NOT installed (factory set
// but Run has not installed the graph yet, or a graph assembled without a
// settlement port) is different: it may hold responsibilities it cannot
// enumerate, so it still reports governed=true with a nil barrier and the
// caller fails closed with origin_census_unavailable.
//
// The installed graph is consulted first and the Config seam only without
// one: the graph is published atomically, while session-rm handlers are
// subscribed before Run installs it, so a Config field mirrored at install
// time would be an unsynchronized read for every concurrent handler.
// origin: hot-update S7 plan §6.4 / lead LQ1 / T31; S7 lead integration
func sessionSettlementBarrier(b *Broker) (xferauth.SessionBarrier, bool) {
	if graph := b.transferRuntime.Load(); graph != nil {
		return graph.Settlement, true
	}
	if b.cfg.TransferSettlement != nil {
		return b.cfg.TransferSettlement, true
	}
	// Before the default graph is installed, a retained durable root is
	// responsibility this broker cannot yet enumerate: removal fails closed.
	// origin: S7 production activation — default graph on every JS broker
	return nil, b.cfg.TransferFactory != nil || brokerJS(b) != nil && xferserve.RetainsState(b.cfg.ClusterDataDir, b.cfg.DBPath)
}

// Settlement uses the immutable live graph, never a Config write after Run.
// origin: hot-update S7 plan §6.4 / lead LQ1 / T31

func settleSessionDeadline(b *Broker, deadline controlbudget.Deadline, stop <-chan struct{}, sid string, abandon bool) error {
	barrier, governed := sessionSettlementBarrier(b)
	return xferauth.SettleSession(deadline, stop, barrier, governed, sid, abandon)
}

// Historical SID-only removal is retired in favor of guarded authority.
// The original settlement and retry rationale remains relevant to that owner.
// removeTombstonedSession drives phases ②③④ for handleSessionRm once the
// tombstone is authoritative (retry: the session was ALREADY DELETING).
//
// rc75 is LQ1's answer, and only while the barrier keeps the session: an
// unsettled continuation transfer, or a census that cannot prove there is
// none. Everything else keeps the pre-S7 contract:
//   - a broker with no continuation barrier answers a retry with
//     already_deleting and does nothing else, exactly as before S7;
//   - once the barrier has released the session, phase ②③ failures are
//     NON-fatal — the session stays DELETING, ctl gets OK, and the boot
//     reconciler finishes the job. A JetStream or
//     SQL hiccup after settlement is not a reason to tell the operator to
//     wait for a settlement that has already happened.
//
// A retry on a governed broker does run the barrier: that is the only way to
// know whether unsettled transfers still hold the session, and the documented
// LQ1 flow is to repeat the removal (with or without the explicit flag) once
// they retire. The cascade it then drives is the same idempotent one; it must
// answer OK so completing the documented retry does not become CLI exit 64.
// origin: hot-update S7 plan §6.4 / lead LQ1; S7 lead integration decisions 1 and 2
// Existing connections can retain cached JWT permissions. Notify workers
// immediately; their authority checks close byte/primitive admission before
// settling. Returning success still requires the durable barrier below.
// origin: hot-update S7 plan §6.4 / T31
// Audit shard 01 F7: derive from b.runCtx (broker shutdown
// context) instead of context.Background so a graceful
// shutdown mid-rm cancels the JS+SQLite cascade instead of
// running it against a half-torn-down broker. Falls back to
// Background when runCtx is nil (broker.New + finalize tests
// that bypass Run). The settlement shares this five-second budget.
// ctx-none: NATS handler before Run in isolated tests
// Phases ②③④. Failures here are NON-fatal to the rm — the
// session is in DELETING, ctl gets OK, and the boot reconciler
// will retry on next broker start. We still log loudly because
// it usually means the JS server is down.

// StopBrokerTransfers is the normal-stop seam for the current runtime owner.
// Keep the graph reachable on error: a failed join is retained responsibility,
// not a successful empty owner that permits a replacement or store close.
func StopBrokerTransfers(b *Broker, deadline controlbudget.Deadline) error {
	return stopBrokerTransferGraph(b.transferRuntime.Load(), deadline)
}

func stopBrokerTransfers(b *Broker) error {
	deadline, err := controlbudget.After(5 * time.Second)
	if err != nil {
		return err
	}
	return suspendBrokerTransfers(b, deadline)
}

func stopBrokerTransferGraph(current *BrokerTransfers, deadline controlbudget.Deadline) error {
	if current == nil {
		return nil
	}
	var result error
	if current.Forward != nil {
		result = errors.Join(result, StopTransferClusterForward(current.Forward, deadline))
	}
	result = errors.Join(result, xfercontrol.StopRuntime(brokerTransferRuntime(current), deadline))
	if result == nil && current.Release != nil {
		result = current.Release() // only after every owner joined
	}
	return result
}

// advanceBrokerTransfers is scheduling only. The finalizer decides exact ACK,
// object retirement and primary-before-outbox consumption under its serializer.
// origin: hot-update S7 plan §5.2 / §5.3 / P22
//
// The cap0 ledger (its own xfer-inflight/outbox roots, never a v1 grant) is scheduled beside the
// installed owner, after it. Every JetStream broker installs one, so skipping it stranded cap0 START
// rows, never replayed a staged cap0 terminal and kept those rows in the image-cut census for ever.
// A missing owner still refuses above, before any legacy synthesis.
// origin: hot-update S7 plan §5.1 / §5.3 / M13 (S7 integration oplegacy F2)
func advanceBrokerTransfers(ctx context.Context, b *Broker) (int, error) {
	current := b.transferRuntime.Load()
	if current == nil {
		return 0, fmt.Errorf("%s: transfer finalizer is not installed", xfercontract.CodeOwnerRecovering)
	}
	deadline, err := transferPassDeadline(ctx)
	if err != nil {
		return 0, err
	}
	n, err := xfercontrol.AdvanceRuntime(brokerTransferRuntime(current), deadline)
	legacy, lerr := legacyTransferLedger(b, ctx).Finalize()
	return n + legacy, errors.Join(err, lerr)
}

// NewBrokerTransferFactory assembles real leaf owners against the current
// backend. Environment ports must prove original-origin discovery, receiver
// retirement and config authority; constructors cannot infer them from SQL.
// origin: hot-update S7 plan §5 / §6.3 / §8.2 / P22
func NewBrokerTransferFactory(ports func(*Broker, jetstream.JetStream, controlbudget.Deadline) (xfercontrol.AssemblyConfig, error)) BrokerTransferFactory {
	return func(b *Broker, js jetstream.JetStream, d controlbudget.Deadline) (*BrokerTransfers, error) {
		cfg, err := ports(b, js, d)
		if err != nil {
			return nil, err
		}
		return assembleBrokerTransfers(b, js, cfg)
	}
}

func assembleBrokerTransfers(b *Broker, js jetstream.JetStream, cfg xfercontrol.AssemblyConfig, states ...*xfercontract.TrackerState) (*BrokerTransfers, error) {
	current := &BrokerTransfers{Origin: cfg.Origin, Buckets: cfg.Buckets, Capacity: cfg.Capacity}
	authority := xferauth.ControlAuthority{SQL: func() *sql.DB { return b.read().SQL() }, Ledger: func() xfercontract.LedgerPort { return current.Ledger }, NodeGate: func(sid, nid string, accepted bool) string { return transferNodeGate(b, sid, nid, accepted) }}
	if link, ok := cfg.Transport.(*xferserve.Link); ok {
		authority.SettlementRows = xferserve.SettlementRows(link, link.Rows)
	}
	cfg.JS, cfg.Authority = js, authority
	var state *xfercontract.TrackerState
	if len(states) > 0 {
		state = states[0]
	}
	runtime, err := xferserve.AssembleRuntime(cfg, state)
	if runtime != nil {
		current.Tracker, current.Ledger, current.Controller, current.Finalizer, current.Collector = runtime.Tracker, runtime.Ledger, runtime.Controller, runtime.Finalizer, runtime.Collector
		current.Settlement = runtime.Settlement
	}
	if err != nil {
		return current, err
	}
	current.Bindings = xfercontract.Bindings{JS: js, Authority: authority, Transport: cfg.Transport, Ledger: current.Ledger, OwnerGeneration: cfg.Origin.Generation}
	if state != nil {
		current.Bindings.OwnerGeneration = state.Generation + 1
	}
	current.Subscriptions = brokerTransferWireSubscriptions(b)
	return current, nil
}

// ServeTransferFactory is the production composition installed on every
// JetStream broker: durable origin/issuer, permit authority, the shared bucket
// coordinator (with this broker's shrink census), the broker→agent link and
// the receiver/object/census ports. A cluster controller writes config only
// through the leader router; the graph keeps the local coordinator.
// origin: hot-update S7 plan §5 / §6 / §8.2 / P22 — S7 production activation
func ServeTransferFactory(b *Broker, js jetstream.JetStream, d controlbudget.Deadline) (*BrokerTransfers, error) {
	return serveTransferFactoryState(b, js, d, nil)
}
func serveTransferFactoryState(b *Broker, js jetstream.JetStream, d controlbudget.Deadline, state *xfercontract.TrackerState) (*BrokerTransfers, error) {
	graph := func() *BrokerTransfers { return cmp.Or(b.transferRuntime.Load(), &BrokerTransfers{}) }
	ledger := func() xfercontract.LedgerPort { return graph().Ledger }
	host := xferserve.Host{ClusterDataDir: b.cfg.ClusterDataDir, DBPath: b.cfg.DBPath, BrokerID: cmp.Or(b.selfID, "single"),
		Conn: func() *nats.Conn { return js.Conn() }, Census: brokerBucketCensus{b}, Logger: b.cfg.Logger,
		Tracker: func() xfercontract.TrackerManager { return graph().Tracker }, Ledger: ledger, Settlement: func() xferserve.SettlementView {
			g := graph()
			return xferserve.SettlementView{Origin: g.Origin, Tracker: g.Tracker, Finalizer: g.Finalizer}
		},
		Rows: xferserve.RowsFrom(func() xfercontract.DecisionPort { return graph().Decisions }, ledger)}
	if b.cfg.AuthCallout != nil {
		host.AuthenticateAgent = func(sid, nid, pub string) error { return xferserve.AuthenticateAgent(b.read().SQL(), sid, nid, pub) }
	}
	if b.clusterMode && b.cl != nil {
		host.Peers = func(d controlbudget.Deadline) ([]xfercontract.Origin, error) {
			return xferserve.MemberOrigins(b.cl.node, d)
		}
		host.Leader = func() bool { return b.cl.node.VerifyLeaderRead(func(*sql.DB) error { return nil }) == nil }
		host.Probe = func(o xfercontract.Origin, d controlbudget.Deadline) (xfercontract.ProbeResult, error) {
			return ProbeTransferBroker(b.cl.forwarder, o, d)
		}
		host.Replicas = b.xferTargetReplicas()
		host.Router = func(local xfercontract.BucketPort) xfercontract.BucketPort {
			return &TransferBucketRouter{Local: local, Node: b.cl.node, Forwarder: b.cl.forwarder}
		}
		host.Remote = TransferOwnerResolver{Forwarder: b.cl.forwarder}
	}
	asm, err := xferserve.Assemble(host, js, d)
	if err != nil {
		return nil, err
	}
	current, err := assembleBrokerTransfers(b, js, asm.Config, state)
	if current == nil {
		return nil, errors.Join(err, asm.Release())
	}
	// Release runs only after every owner joined (stopBrokerTransferGraph).
	current.Buckets, current.Link, current.Release = asm.Local, asm.Link, asm.Release
	current.Assembly = &asm
	if err == nil {
		// The sleeping tracker is idle here; a later live view could be refused.
		current.Decisions, err = xfertrack.DecisionsForBinding(current.Tracker, current.Bindings.OwnerGeneration)
		current.Controller = xferserve.SettleEagerly(current.Controller, current.Finalizer)
	}
	return current, err
}

// serveTransferCarrier answers a capable continuation carried on a legacy
// command subject (prepare / push-commit); false keeps the legacy handler. A
// capable body never falls through to legacy byte work.
// origin: hot-update S7 plan §6.2 / §7 / INTEG-D carrier request
func serveTransferCarrier(b *Broker, msg *nats.Msg, sid, actor, nid, verb string) bool {
	// Explicitly composed INTEG-G graphs use their original directed carrier port.
	// origin: hot-update S7 plan §6.3 / T39
	if graph := b.transferRuntime.Load(); graph != nil && graph.Link == nil {
		return false
	}
	d, err := controlbudget.After(5 * time.Second)
	if err != nil {
		return false
	}
	reply, handled, silent := xferserve.Carrier(brokerTransferIngress(b), func(o xfercontract.Origin) bool { return transferOriginGate(b, o) }, actor, sid, nid, verb, msg.Data, d)
	if handled && !silent && reply != nil {
		b.respondBytes(msg, reply)
	}
	return handled
}

// serveAgentTransferRequest answers the agent-role verbs on the agent's own
// authenticated subtree; the principal instance is the retained entry's.
// origin: hot-update S7 plan §6.3 / T39 / INTEG-A trusted agent binding
func serveAgentTransferRequest(b *Broker, reg xfercontrol.Registration, msg *nats.Msg) {
	if reply := xferserve.DispatchAgent(brokerTransferIngress(b), func(o xfercontract.Origin) bool { return transferOriginGate(b, o) }, reg, msg); reply != nil {
		b.respondBytes(msg, reply)
	}
}

func brokerTransferIngress(b *Broker) xferserve.Graph {
	current := b.transferRuntime.Load()
	if current == nil {
		return xferserve.Graph{}
	}
	authority, _ := current.Bindings.Authority.(xferauth.ControlAuthority)
	rows := xferserve.RowsFrom(func() xfercontract.DecisionPort { return current.Decisions }, func() xfercontract.LedgerPort { return current.Ledger })
	return xferserve.Graph{Controller: current.Controller, Authority: authority, Tracker: current.Tracker, Rows: rows, Link: current.Link, Decisions: current.Decisions}
}

// brokerTransferWireSubscriptions resolves the installed graph at invocation,
// so the shared cold/hot descriptor never captures a pre-cut NC or tracker.
func brokerTransferWireSubscriptions(b *Broker) []BrokerTransferSubscription {
	var result []BrokerTransferSubscription
	for _, reg := range xfercontrol.Registrations() {
		if reg.LegacyCarrier || reg.ClusterVerb != "" {
			continue
		}
		result = append(result, BrokerTransferSubscription{Subject: reg.Subject, Handler: func(msg *nats.Msg) { handleBrokerTransferRequest(b, reg, msg) }})
	}
	return result
}

// Transfer census RPCs retain the original owner and only use current ports.
// Public census contains protection evidence, never private ledger/capsules.
func startBrokerTransferForward(b *Broker, nc *nats.Conn, current *BrokerTransfers) error {
	// A single broker has no cluster RPC borrower. A clustered broker with a
	// missing node must still fail installation instead of claiming readiness.
	// origin: hot-update S7 plan §6.3 / P22
	if !b.clusterMode && b.cl == nil {
		return nil
	}
	cfg := TransferClusterForwardConfig{Origin: current.Origin, Buckets: current.Buckets, Logger: b.cfg.Logger}
	if b.cl != nil {
		cfg.Node = b.cl.node
	}
	cfg.Census = func(origin xfercontract.Origin, d controlbudget.Deadline) (xfercontract.LedgerCensus, error) {
		graph := b.transferRuntime.Load()
		if graph == nil || graph.Origin != origin {
			return xfercontract.LedgerCensus{}, xferauth.ControlRefusal(xfercontract.CodeOwnerRecovering)
		}
		return xferserve.LocalCensus(graph.Origin, graph.Ledger, graph.Assembly, origin, d)
	}
	cfg.Probe = xferserve.ProbeOrigin(func() xfercontract.Origin { return cmp.Or(b.transferRuntime.Load(), &BrokerTransfers{}).Origin }, nc)
	var err error
	current.Forward, err = SubscribeTransferClusterForward(nc, cfg)
	if err == nil && current.Assembly != nil {
		current.Forward.Extra, err = current.Assembly.StartCluster(nc)
	}
	return err
}

// Projection borrows current leaves only; it transfers no ownership or state.
func brokerTransferRuntime(c *BrokerTransfers) xfercontrol.Runtime {
	return xfercontrol.Runtime{Tracker: c.Tracker, Ledger: c.Ledger, Controller: c.Controller, Finalizer: c.Finalizer, Collector: c.Collector, Buckets: c.Buckets, Settlement: c.Settlement}
}

// Managed image owners join through the cut; ordinary Run returns join here.
// origin: hot-update S7 plan §8.2 / P22
//
// A clean stop must hand Run's cancellation sentinel back untouched:
// errors.Join(context.Canceled, nil) is a *joinError that prints
// "context canceled" yet is != context.Canceled, which turned every ordinary
// shutdown of an embedded broker into "did not exit cleanly" (test/p13 proxy
// tunnel-drop stack, internal/broker Run tests). JoinCleanupError keeps both
// errors only when the transfer stop itself failed — the same T52 contract the
// other Run cleanup defers already follow.
// origin: hot-update S6 plan §4.2 / T52; S6-proxyfix make-test regression
// origin: hot-update S7 lead integration — make test on the merged tree (s7-p9k)
//
// managed is read when Run RETURNS, not when the defer is registered: Run
// flips it to true only after InstallCurrent hands every owner to the image
// slot, whose StopCurrent joins the graph (JoinStoreUsers →
// suspendBrokerTransfers). Capturing it by value meant the defer always saw
// false, so a managed Run stopped the graph behind the slot's back.
// origin: hot-update S7 plan §8.2 / P24 — managed slot transfer stop
func stopBrokerTransferRun(b *Broker, managed *bool, runErr *error) {
	if !*managed {
		*runErr = brokercontrol.JoinCleanupError(*runErr, stopBrokerTransfers(b))
	}
}

func reconcileBrokerTransfersOnBoot(ctx context.Context, b *Broker) {
	// R16 #57 (minor-2): eagerly finalize any transfer stranded by a PRIOR crash, so a broker that crash-loops
	// faster than XferReapInterval still closes dangling start rows (the periodic pass is anchored at
	// start+interval). M1 makes it safe when leaderless — the ledger is deleted only on a CONFIRMED commit.
	//
	// PLACEMENT IS LOAD-BEARING: this walks a ledger and, per stranded transfer, forwards to the leader with a
	// bounded retry (~500ms each when leaderless). It MUST run AFTER the admin socket is serving. Running it
	// earlier delays the socket, and the socket IS the joiner-readiness signal `tether cluster add` polls
	// (joinerBrokerUpLocal → OpClusterStatus): a joiner restarted mid-grow that is merely still finalizing
	// would be misread as "not up in cluster mode" and the grow would HALT at the start-joiner boundary.
	// Observed on the deploy tier (drill 42 returning-node re-grow) when this ran before the socket.
	//
	// NOT GATED ON clusterMode — origin: prerelease audit round 2, F-T7. It was, and
	// that made the crash-loop case this block exists for unreachable on exactly the
	// deployment #57 had just been extended to: batch F made synthesis work on a single
	// broker (commitSyntheticTransferTerminal's single-mode branch) and the ledger root
	// falls back to the DB's directory when no ClusterDataDir is set, so a lone broker
	// really does accumulate stranded rows — and then only the periodic pass, anchored at
	// start+XferReapInterval, would ever close them. A broker crash-looping faster than
	// that interval closed none, forever, which is the whole reason for finalizing here.
	//
	// finalizeStrandedXfers is a no-op when there is no ledger dir at all (an in-memory
	// test broker), so the added call costs nothing on the shapes that have nothing to
	// finalize.
	// file-transfer-plan §Object bucket lifecycle G.2 — reap leftover OBJ_xfer-* streams
	// from a previous crash. The current transfer graph and its census must be
	// installed first, including sleeping imported ownership (S7 §5.3 / M11).
	if n, rerr := b.reconcileXferObjects(ctx); rerr != nil {
		b.cfg.Logger.Warn("broker: OBJ_xfer boot reconcile", "err", rerr)
	} else if n > 0 {
		b.cfg.Logger.Info("broker: orphan xfer buckets reaped", "count", n)
	}
	if n, ferr := b.finalizeStrandedXfers(ctx); ferr != nil {
		b.cfg.Logger.Warn("broker: eager stranded-transfer finalize on boot", "err", ferr)
	} else if n > 0 {
		b.cfg.Logger.Info("broker: eagerly finalized stranded in-flight transfers on boot (#57)", "count", n)
	}

}

// The slot owns this projection in managed mode; cold Run's defer reads its
// managed latch by reference. The same image template is available before the
// restored process opens SQL or NATS.
// origin: hot-update S7 plan §8.2 / T36
func configureProductionTransferImage(b *Broker) {
	if b.cfg.ImageRun == nil {
		return
	}
	if b.imageControl == nil {
		b.imageControl = newBrokerImageControl(b)
	}
	if b.imageControl.transfers != nil {
		return
	}
	b.imageControl.transfers = xferserve.NewImageOwner(xferserve.ImageConfig{
		Capacity: func() xfercontract.CapacityAccount {
			if g := b.transferRuntime.Load(); g != nil {
				return g.Capacity
			}
			return nil
		},
		Current: func() (xfercontract.TrackerManager, xfercontract.BucketPort) {
			if g := b.transferRuntime.Load(); g != nil {
				return g.Tracker, g.Buckets
			}
			return nil, nil
		},
		Stop: func(d xfercontract.Deadline) error { return suspendBrokerTransfers(b, d) },
		Resume: func(d xfercontract.Deadline) error {
			g := b.transferRuntime.Load()
			if g == nil {
				return xferauth.ControlRefusal(xfercontract.CodeOwnerRecovering)
			}
			return g.Tracker.Bind(g.Bindings, d)
		},
		Rebuild: func(state *xfercontract.TrackerState, d xfercontract.Deadline) (xfercontract.TrackerManager, error) {
			g, err := serveTransferFactoryState(b, brokerJS(b), d, state)
			if err == nil {
				err = InstallBrokerTransfers(b, g, d)
			}
			if err == nil {
				err = startBrokerTransferForward(b, b.nc.Load(), g)
			}
			if err != nil {
				return nil, errors.Join(err, uninstallBrokerTransfers(b, g, d))
			}
			return g.Tracker, nil
		},
	})
	b.imageControl.transferSubscriptions = func(*nats.Conn) []TransferImageSubscription {
		var out []TransferImageSubscription
		for _, sub := range brokerTransferWireSubscriptions(b) {
			out = append(out, TransferImageSubscription{Subject: sub.Subject, Handler: sub.Handler})
		}
		return out
	}
}

// The loaded-config adapter lives outside authcallout to keep rendering and
// admission independent; only this broker composition joins them.
// origin: hot-update S9 plan §3.3 / P12 / T27
func brokerSystemPrincipals(b *Broker) func() []authcallout.SystemPrincipal {
	return xferserve.LoadedSystemPrincipals(b.cfg.NatsConfPath, b.cfg.Logger)
}
