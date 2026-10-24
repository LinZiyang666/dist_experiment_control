package broker

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"reflect"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/LinZiyang666/tether/internal/adminsock"
	"github.com/LinZiyang666/tether/internal/brokercontrol"
	"github.com/LinZiyang666/tether/internal/brokerimage"
	"github.com/LinZiyang666/tether/internal/brokermaint"
	"github.com/LinZiyang666/tether/internal/brokermetrics"
	"github.com/LinZiyang666/tether/internal/brokerupgrade"
	"github.com/LinZiyang666/tether/internal/brokerupgrade/brokerruntime"
	"github.com/LinZiyang666/tether/internal/cluster"
	"github.com/LinZiyang666/tether/internal/clustermanifest"
	"github.com/LinZiyang666/tether/internal/controlbudget"
	"github.com/LinZiyang666/tether/internal/handoff"
	"github.com/LinZiyang666/tether/internal/httplisten"
	"github.com/LinZiyang666/tether/internal/infraattach"
	"github.com/LinZiyang666/tether/internal/listenerimage"
	"github.com/LinZiyang666/tether/internal/proto"
	"github.com/LinZiyang666/tether/internal/storage"
	"github.com/LinZiyang666/tether/internal/subhttp"
	"github.com/LinZiyang666/tether/internal/tunnel"
	"github.com/LinZiyang666/tether/internal/workload"
	"github.com/LinZiyang666/tether/internal/xferbridge"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

// The daemon executes the privileged, already-staged artifact without sudo or
// systemctl. Same-PID exec keeps systemd Type=simple ownership and avoids the G5
// #23 clean-exit-not-restarted trap. Stage-C M4 still fences membership operations.
// S6 replaces the old cancel-Run/serve trampoline: unwinding Run would destroy
// the very listeners, raw sockets and flock OFD that the image must preserve.
// origin: hot-update S6 plan §3.2 / §4 / T01

// BrokerImageRun binds real producer implementations without importing the P00
// compile stubs. Capture reads current owners on every cut, including after a
// returned exec. InstallCurrent transfers ALL terminal cleanup to Slot only once
// cold startup has finished. Recover must reopen the current pathname and rebind
// authority/NC/Node before Thaw; a close-unknown owner must remain fenced.
//
// DrainArmed joins the DB-free reply sender, rather than assuming a timer proved
// delivery. WithExecLatch rechecks late revoke/role/maintenance/FD guards under
// the cut latch before invoking the supplied Prepared.Exec operation. These seams
// are required: a missing producer never produces a successful cut receipt.
// origin: hot-update S6 plan §2.1 / §12.1 P11
// All fields are wiring, not public image state or a second image ABI.
type BrokerImageRun = brokerimage.StoreRun[*Broker]

func validateBrokerImageRun(r *BrokerImageRun) error { return brokerimage.ValidateStoreRun(r) }

func brokerCutDeadline() (controlbudget.Deadline, error) { return brokerimage.NewStoreCutDeadline() }

func brokerReloadRequests(b *Broker) <-chan struct{} { return b.reloadSignal.Requests() }

// SetReloadTrigger preserves source compatibility without cancelling Run.
func (b *Broker) SetReloadTrigger(_ context.CancelFunc) {}

// RequestReExec coalesces cuts into the same live Run loop.
func (b *Broker) RequestReExec() { b.reloadSignal.Request() }

// ReExecRequested is an armed cut, not permission to exec after Run returns.
func (b *Broker) ReExecRequested() bool { return b.reloadSignal.Requested() }

// runBrokerImageReload is called synchronously by Run. Every failure after Freeze
// returns ownership through the same cut; after a store close attempt Recover
// must interpret the actual phases before deciding whether another open is safe.
// origin: hot-update S6 plan §3.2 / §4.2 / T01
// A reload that returns did not exec: an armed S8 cut is disarmed and settled
// (brokerupgrade.CutReturned). The delegation itself stays the one statement
// the broker-image contract gate mutates. origin: S8 BROKER-EXPORTER R-11
func runBrokerImageReload(b *Broker, shutdown brokerimage.StoreShutdown) (err error) {
	defer func() { err = brokerupgrade.CutReturned(b.cfg.Upgrade, err) }()
	return runBrokerImageReloadUsing(b, b.cfg.ImageRun, shutdown)
}

// The ACTIVE rollback selects its reader without replacing shared Config while
// imported authority callbacks still borrow it.
// origin: hot-update S6 plan §4.2 / T49
func runBrokerImageReloadUsing(b *Broker, image *BrokerImageRun, shutdown brokerimage.StoreShutdown) error {
	// Preserve the caller's reader choice (including ACTIVE rollback) while
	// checking the original admission at the actual irreversible exec latch,
	// reversible preparation and the final pre-close sample (GuardReloadRole).
	// origin: hot-update S6 plan §5 / internal review rv-cluster-7 / S6 rv-final F1
	// An S9 domain responsibility refuses before Fence and at every later sample.
	// origin: hot-update S9 plan §7.2 / M24 (image and infra overlap)
	if err := infraattach.ImageExclusive(b.infra); err != nil {
		return err
	}
	image, shutdown = brokerruntime.GuardReloadRole(image, shutdown, infraattach.ExclusiveRole(b.infra, func(stopped bool) error { return validateBrokerReloadRole(b, stopped) }))
	return brokerimage.ReloadCurrentStore(image, b, shutdown, brokerimage.StoreRunCallbacks[*Broker]{
		Deadline: b.reloadSignal.Deadline, Phase: setBrokerImagePhase, Resume: ResumeBrokerTransfers, Bind: bindBrokerImageHealth, Armed: notifyBrokerReloadArmed,
		// After the freeze, before the store closes: an armed S8 cut seals its
		// adjunct with the authority map of exactly this store (S8 BROKER-EXPORTER R-11).
		BeforeShutdown: func(b *Broker) error { return sealBrokerCut(b, validateBrokerReloadRole(b, false)) },
		Exec: func(prepared *handoff.Prepared, bundle brokerimage.Bundle, path string, env []string) error {
			if b.imageControl != nil && b.imageControl.cut != nil {
				return brokerimage.ExecCommit(b.imageControl.cut.Cut, prepared, func(final brokerimage.Bundle) error {
					if err := b.runCtx.Err(); err != nil {
						return err
					}
					if err := brokerimage.Validate(final); err != nil {
						return err
					}
					if err := validateBrokerReloadRole(b, true); err != nil {
						return err
					}
					emitBrokerImageEvent(b, "exec_enter", 0)
					return nil
				}, brokerupgrade.CutPath(b.cfg.Upgrade, path), bundle.Args, env)
			}
			return prepared.Exec(path, bundle.Args, env)
		},
	})
}

func brokerSealClosed(s storage.StoreSeal) bool { return brokerimage.StoreSealClosed(s) }

// brokerReloadBackend keeps single mode's local reload on the same guarded path
// as the signed remote trigger. Other cluster operations remain cluster-only.
type brokerReloadBackend struct {
	broker  *Broker
	cluster adminsock.ClusterAdminBackend
}

func (r *brokerReloadBackend) HandleCluster(req adminsock.Request) adminsock.Response {
	// origin: S8 internal review rv3-leaves F2
	return brokerupgrade.RouteLegacyReload(req, r.broker.read().SQL(), r.cluster)
}

// handleBrokerUpgradeReload validates the staged SHA even for an equal release.
// Only the SHA of the running executable proves an idempotent reload. Version
// spelling still uses SameRelease (goreleaser's leading v differs from builds).
// origin: hot-update S6 plan §5 / T01
func (b *clusterAdminBackend) handleBrokerUpgradeReload(req adminsock.Request) adminsock.Response {
	var node *cluster.Node
	if b.admin != nil {
		node = b.admin.Node
	}
	return brokerimage.HandleStoreReloadRequest(req, node, b.requestReExec)
}

// onDiskBinarySHA256 hashes the freshly staged pathname, which may have been
// replaced while this process still runs the previous inode.
func onDiskBinarySHA256() (string, error)  { return brokerimage.StagedArtifactSHA256() }
func runningBinarySHA256() (string, error) { return brokerimage.RunningArtifactSHA256() }

func notifyBrokerReloadArmed(b *Broker, err error) { b.reloadSignal.Notify(err) }

// origin: S6 internal review rv-final F4
func admitLocalBrokerMaintenance(b *Broker, deadline controlbudget.Deadline) error {
	if b.imageControl == nil {
		b.imageControl = newBrokerImageControl(b)
	}
	c := b.imageControl
	var node *cluster.Node
	if b.cl != nil {
		node = b.cl.node
	}
	db, err := brokerMaintenanceDB(b, node)
	if err != nil {
		return err
	}
	lease, err := brokercontrol.AdmitLocalMaintenance(brokercontrol.LocalMaintenanceConfig{DB: db, Node: node, NC: b.nc.Load(), Instance: c.instanceID, Generation: brokerruntime.ImageGeneration(brokerImageSnapshot(b)), Now: b.cfg.Now,
		Sign: maintenanceSign(b), Verify: maintenanceVerify,
		Collect: func(r brokermaint.Record, p maintenancePeer) (brokerimage.MaintenanceObserver, brokerimage.MaintenanceAck, error) {
			return collectMaintenanceObserver(b, r, p)
		}}, deadline)
	if err != nil {
		return fmt.Errorf("%w: %w", brokerimage.ErrMaintenanceObserverUnsupported, err)
	}
	c.localMaintenance = lease
	c.instanceID, c.bootID = lease.Record.InstanceID, lease.Record.IssuerBootID
	return nil
}

// Carry the actual admission sample through the asynchronous Run cut. A fresh
// follower sample alone would miss an intervening election or membership change.
// origin: hot-update S6 internal review rv-cluster-7
func captureBrokerReloadAdmission(b *Broker, req adminsock.Request, deadline controlbudget.Deadline) error {
	if b.cl == nil || b.cl.node == nil {
		return admitLocalBrokerMaintenance(b, deadline)
	}
	if err := brokerimage.CheckReloadRole(b.cl.node); err != nil {
		return err
	}
	voters, err := b.cl.node.NumVoters()
	if err != nil {
		return err
	}
	// The earlier voter-only rationale is preserved here with its missing cohort:
	// The local sole-voter entry has no remote mutation observers. Keep its D2
	// admission working; multi-voter cuts require the signed aggregate and holder.
	// Every affected agent needs an installed hold, including local sole-voter
	// cuts. Remote rolls retain their signed issuer aggregate and holder.
	// origin: S6 internal review rv-final F4
	if voters == 1 && req.CutID == "" {
		if err := admitLocalBrokerMaintenance(b, deadline); err != nil {
			return err
		}
	} else {
		if err := admitBrokerMaintenance(b, req, deadline); err != nil {
			if errors.Is(err, brokermaint.ErrExpired) {
				return fmt.Errorf("%s: %w", brokerimage.DetailMaintenanceExpired, err)
			}
			return err
		}
	}
	receipt, err := cluster.ReadImageRole(b.cl.node)
	if err != nil {
		return err
	}
	m := &b.cl.homeMaintenance
	m.mu.Lock()
	m.admission, m.admitted = receipt, true
	m.mu.Unlock()
	return nil
}

type brokerBusinessSubscription struct {
	subj    string
	handler nats.MsgHandler
}

// One subscription table serves both cold startup and the current restored graph.
// origin: hot-update S6 plan §6.3 / rv-cluster-2
func brokerBusinessSubscriptions(b *Broker, nc *nats.Conn) []brokerBusinessSubscription {
	subscriptions := []brokerBusinessSubscription{
		{proto.SubjectPrefix + ".ctrl.by.*.session.create.req", b.handleSessionCreate},
		{proto.SubjectPrefix + ".ctrl.by.*.session.list.req", b.handleSessionList},
		{proto.SubjectPrefix + ".ctrl.by.*.session.*.rm.req", b.handleSessionRm},
		// P4 control plane.
		{proto.SubjectPrefix + ".s.*.cmd.by.*.node.*.exec.req",
			func(msg *nats.Msg) { b.handleExecReq(nc, msg) }},
		{proto.SubjectPrefix + ".s.*.cmd.by.*.node.*.process-resume.req",
			func(msg *nats.Msg) { handleProcessResumeReq(b, nc, msg) }},
		{proto.SubjectPrefix + ".ctrl.by.*.s.*.ps.req", b.handlePsReq},
		{proto.SubjectPrefix + ".ctrl.by.*.s.*.node.list.req", b.handleNodeListReq},
		{proto.SubjectPrefix + ".s.*.ev.node.*.proc.*.started", b.handleProcEvent},
		{proto.SubjectPrefix + ".s.*.ev.node.*.proc.*.exit", b.handleProcEvent},
		// P5 PTY control plane.
		{proto.SubjectPrefix + ".s.*.cmd.by.*.node.*.run.req",
			func(msg *nats.Msg) { b.handleRunReq(nc, msg) }},
		{proto.SubjectPrefix + ".s.*.cmd.by.*.node.*.kill.req",
			func(msg *nats.Msg) { b.handleKillReq(nc, msg) }},
		{proto.SubjectPrefix + ".s.*.pty.*.failed", b.handlePtyFailed},
		// P6 data-plane control (expose / expose-rm).
		{proto.SubjectPrefix + ".s.*.cmd.by.*.node.*.expose.req",
			func(msg *nats.Msg) { b.handleExposeReq(nc, msg) }},
		{proto.SubjectPrefix + ".s.*.cmd.by.*.node.*.expose-rm.req",
			func(msg *nats.Msg) { b.handleExposeRmReq(nc, msg) }},
		// P10 J.4 — `tether node upgrade <nid>`; S8 transaction hop shares its admission.
		{proto.SubjectPrefix + ".s.*.cmd.by.*.node.*.upgrade.req",
			func(msg *nats.Msg) { b.handleUpgradeReq(nc, msg) }},
		{proto.SubjectPrefix + ".s.*.cmd.by.*.node.*.upgrade-tx.req", func(msg *nats.Msg) { handleUpgradeTxReq(b, nc, msg) }},
		// P11 file transfer (file-transfer-plan v0.2.0).
		{proto.SubjectPrefix + ".s.*.cmd.by.*.node.*.push.req",
			func(msg *nats.Msg) { b.handlePushReq(nc, msg) }},
		{proto.SubjectPrefix + ".s.*.cmd.by.*.node.*.pull.req",
			func(msg *nats.Msg) { b.handlePullReq(nc, msg) }},
		{proto.SubjectPrefix + ".s.*.cmd.by.*.node.*.push-commit.req",
			func(msg *nats.Msg) { b.handlePushCommitReq(nc, msg) }},
		{proto.SubjectPrefix + ".s.*.ev.node.*.transfer.*.complete", b.handleEvTransfer},
		{proto.SubjectPrefix + ".s.*.ev.node.*.transfer.*.failed", b.handleEvTransfer},
		{proto.SubjectPrefix + ".ctrl.by.*.s.*.transfer.*.finalize.req", b.handleFinalizeReq},
		{proto.SubjectPrefix + ".ctrl.by.*.s.*.caps.req", b.handleCapsReq},
		// P13 proxy subscription control plane.
		{proto.SubjectPrefix + ".ctrl.by.*.s.*.proxy.set.req",
			func(msg *nats.Msg) { b.handleProxySet(nc, msg) }},
		{proto.SubjectPrefix + ".ctrl.by.*.s.*.proxy.status.req", b.handleProxyStatus},
		// Mega-audit MAJ-2: register the CONCRETE create/revoke/list leaves, NOT the wildcard
		// `.proxy.sub.*.req`. The wildcard never matched isBroadcastClusterSubject's per-leaf
		// HasSuffix check, so it was queue-grouped and the leader-only create/revoke landed on a
		// silent follower ~(N-1)/N of the time → ctl timeout. create/revoke → broadcast+leader-only
		// (the handler's follower gate collapses the fan-out); list → queue-grouped RODB read.
		{proto.SubjectPrefix + ".ctrl.by.*.s.*.proxy.sub.create.req",
			func(msg *nats.Msg) { b.handleProxySub(nc, msg) }},
		{proto.SubjectPrefix + ".ctrl.by.*.s.*.proxy.sub.revoke.req",
			func(msg *nats.Msg) { b.handleProxySub(nc, msg) }},
		{proto.SubjectPrefix + ".ctrl.by.*.s.*.proxy.sub.list.req",
			func(msg *nats.Msg) { b.handleProxySub(nc, msg) }},
		{proto.SubjectPrefix + ".s.*.ev.node.*.proxy.*", b.handleProxyReadyEvent},
	}
	// origin: hot-update S9 plan §7.1 / WIRE-2 R14 — exact owner, never a queue.
	if b.selfID != "" {
		subscriptions = append(subscriptions, brokerBusinessSubscription{proto.SubjClusterInfra(b.selfID), infraattach.InfraHandler(b.infra, b.selfID, b.accountPubOrEmpty, b.cfg.Now, infraattach.ContextStop(b.runCtx))})
	}
	if b.imageControl != nil && b.imageControl.transferSubscriptions != nil {
		for _, spec := range b.imageControl.transferSubscriptions(nc) {
			subscriptions = append(subscriptions, brokerBusinessSubscription{spec.Subject, spec.Handler})
		}
	}
	return subscriptions
}

// TransferImageSubscription is the outer adapter of P16's one registration
// descriptor. Both cold and hot paths consume it through the same table above;
// the factory receives the current NC, rather than capturing the retired one.
// origin: hot-update S7 plan §6.3 / §8.2 / P24
type TransferImageSubscription struct {
	Subject string
	Handler nats.MsgHandler
}

// ConfigureBrokerTransferImage is called by transfer startup before cold scans
// or restored activation. The factory can be sleeping; activation installs its
// imported replacement before opening store/control or starting subscriptions.
func ConfigureBrokerTransferImage(b *Broker, owner brokerimage.TransferOwner, subscriptions func(*nats.Conn) []TransferImageSubscription) error {
	if b == nil || owner == nil || subscriptions == nil {
		return brokerimage.ErrImage
	}
	// origin: hot-update S7 plan §8.2 / P24. Typed nils cannot own a cut.
	value := reflect.ValueOf(owner)
	if value.Kind() != reflect.Pointer || value.IsNil() {
		return brokerimage.ErrImage
	}
	if b.imageControl == nil {
		b.imageControl = newBrokerImageControl(b)
	}
	if b.imageControl.transfers != nil {
		return brokerimage.ErrActive
	}
	b.imageControl.transfers, b.imageControl.transferSubscriptions = owner, subscriptions
	return nil
}

// NewImageRun installs the real cut/import adapters without opening a store.
// serve calls this after pure validation; a restored launch enters Run with no
// cold DB or Node. Closures resolve the current control generation on each cut.
// origin: hot-update S6 plan §1.2 / §4.1 / rv-cluster-2 / rv-fd-7
func NewImageRun(args []string, boot *brokerimage.BootImage) (*BrokerImageRun, error) {
	if len(args) < 2 {
		return nil, brokerimage.ErrImage
	}
	r := &BrokerImageRun{Restored: boot != nil}
	path, err := os.Executable()
	if err != nil {
		return nil, err
	}
	r.CandidatePath = strings.TrimSuffix(path, " (deleted)")
	// Redispatch may execute .prev while retaining the canonical sealed argv.
	// Later cuts still stage at the original daemon pathname, not .prev.prev.
	// origin: hot-update S6 plan §4.2 / T49
	if boot != nil && filepath.IsAbs(args[0]) {
		r.CandidatePath = args[0]
	}
	r.Slot, err = brokerimage.NewRuntimeSlot(func(store brokerimage.StoreOwner, _ controlbudget.Deadline) error {
		if closer, ok := store.(io.Closer); ok {
			return closer.Close()
		}
		return brokerimage.StopClusterStore(store)
	}, func(server tunnel.ServerImported, _ controlbudget.Deadline) error {
		if server != nil && server.Server() != nil {
			server.Server().Close()
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	var owner *Broker
	r.PrepareNodeShutdown = func(node *cluster.Node, deadline controlbudget.Deadline) error {
		if owner == nil || owner.imageControl == nil {
			return errors.New("broker: current role owner missing")
		}
		return brokerruntime.PrepareNodeShutdown(node, owner.imageControl.role, owner.runCtx.Err(), deadline)
	}
	r.Capture = func(b *Broker, deadline controlbudget.Deadline) (brokerimage.Config, error) {
		owner = b
		return captureBrokerImage(b, deadline)
	}
	r.Preflight = func(b *Broker, deadline controlbudget.Deadline) error {
		// An armed S8 cut probes its target slot and published source slot.
		candidate, previous, expected := brokerupgrade.CutReaders(b.cfg.Upgrade, r.CandidatePath, r.CandidatePath+".prev")
		return preflightBrokerImage(b, r, args, deadline, candidate, previous, expected)
	}
	r.Freeze = func(cfg brokerimage.Config) (brokerimage.Cut, error) {
		c, ok := cfg.Control.(*brokerImageControl)
		if !ok {
			return nil, errors.New("broker: current control owner missing")
		}
		if err := c.broker.runCtx.Err(); err != nil {
			return nil, err
		}
		emitBrokerImageEvent(c.broker, "freeze_begin", 0)
		cut, err := brokerimage.Freeze(cfg)
		if cut == nil {
			return nil, err
		}
		wrapped, wrapErr := xferbridge.WrapCut(cut, func() (xferbridge.Projection, error) { return projectBrokerTransferCut(c) })
		if wrapErr != nil {
			return cut, errors.Join(err, wrapErr)
		}
		c.cut = wrapped
		bindErr := tunnel.SetServerImageRevoke(cfg.Server, func(ids []string, revoke func() error) error { return brokerimage.RevokeResources(cut, ids, revoke) })
		return wrapped, errors.Join(err, bindErr, brokerimage.BindRuntime(cut, r.Slot))
	}
	r.InstallCurrent = func(b *Broker) error {
		owner = b
		if boot != nil {
			err := activateRestoredBrokerImage(b, r, boot, args)
			if err != nil {
				return rollbackRestoredBrokerImage(b, r, boot, err)
			}
			return nil
		}
		if _, err := r.Capture(b, 1); err != nil {
			return err
		}
		if b.imageControl.transfers != nil {
			if err := brokerimage.InstallTransferOwner(r.Slot, b.imageControl.transfers); err != nil {
				return err
			}
		}
		return r.Slot.Install(b.imageControl.store, b.imageControl, &brokerLiveServer{server: b.tunnelSrv.Load()})
	}
	r.Recover = func(b *Broker, cut brokerimage.Cut, seal storage.StoreSeal, deadline controlbudget.Deadline) error {
		if !brokerimage.StoreSealClosed(seal) {
			return errors.New("broker: store_close_unknown")
		}
		// StopTransport must join the initiating DB-free reply before a new
		// control generation can replace any handler's DB/NC pointers.
		if err := cut.Thaw(deadline); err != nil {
			return err
		}
		if err := tunnel.SetServerImageRevoke(b.tunnelSrv.Load(), nil); err != nil {
			return err
		}
		setBrokerImagePhase(b, proto.BrokerImagePhaseServing)
		return nil
	}
	r.DrainArmed = func(deadline controlbudget.Deadline) error {
		if owner == nil || owner.imageControl == nil {
			return errors.New("broker: armed reply owner missing")
		}
		return brokerruntime.DrainArmed(owner.admin, owner.nc.Load(), deadline)
	}
	r.WithExecLatch = func(exec func() error) error {
		// The final cut latch performs the same stop check again. Run remains
		// alive to consume cancellation and StopCurrent after a returned cut.
		// origin: hot-update S6 plan §4.2 / rv-fd-6
		if owner == nil {
			return errors.New("broker: current Run missing")
		}
		if err := owner.runCtx.Err(); err != nil {
			return err
		}
		return exec()
	}
	r.RunRestored = func(b *Broker) error { return runBrokerImageLoop(b.runCtx, b) }
	return r, nil
}

// Capture binds the current store and identity without constructing a second
// owner during the cut. The control adapter carries the original boot lineage.
// origin: hot-update S6 plan §2.2 / §3.2 / T01
func captureBrokerImage(b *Broker, deadline controlbudget.Deadline) (brokerimage.Config, error) {
	if b.imageControl == nil {
		b.imageControl = newBrokerImageControl(b)
	}
	c := b.imageControl
	var node *cluster.Node
	if b.clusterMode {
		node = b.cl.node
	}
	if c.instanceID == "" {
		var err error
		if c.instanceID, c.bootID, err = brokerruntime.ImageIdentity(node); err != nil {
			return brokerimage.Config{}, err
		}
	}
	if c.store == nil {
		c.store = b.cfg.SingleStore
		if b.clusterMode && b.cl != nil {
			var err error
			c.store, err = brokerimage.NewClusterStore(b.cl.node, c.lock, brokerProductionImageConfig(b, b.cl.node.SelfID()))
			if err != nil {
				return brokerimage.Config{}, err
			}
		}
	}
	if c.store == nil {
		return brokerimage.Config{}, errors.New("broker: current store owner missing")
	}
	mode, err := brokerruntime.ImageMode(b.clusterMode, node)
	if err != nil {
		return brokerimage.Config{}, err
	}
	return brokerimage.Config{Mode: mode, Store: c.store, Control: c, Server: b.tunnelSrv.Load(),
		Transfers: c.transfers,
		Listeners: c.keepers, Deadline: deadline, FreezeServer: tunnel.FreezeServer, ImportServer: tunnel.ImportServer,
		NewControl: func(store brokerimage.StoreOwner, _ controlbudget.Deadline) (brokerimage.ControlOwner, error) {
			return replaceBrokerImageControl(b, store, c)
		}}, nil
}

// Each generation has its own control roots. They can be canceled and joined
// without canceling Run or the data-plane owner; reopening replaces this object.
// origin: hot-update S6 plan §1.2 / §6.3
// This is wiring only: the durable projection is brokerimage.ControlState.
// Both readers are freshly probed against today's schema. Rollback binds its
// target to the previous SHA carried by the validated incoming image; it cannot
// accept an arbitrary file replaced at that pathname.
// origin: hot-update S6 plan §4.2 / T49
func preflightBrokerImage(b *Broker, r *BrokerImageRun, args []string, deadline controlbudget.Deadline, candidatePath, previousPath, expectedPrevious string) error {
	if err := b.runCtx.Err(); err != nil {
		return err
	}
	cfg, err := r.Capture(b, deadline)
	if err != nil {
		return err
	}
	stamp, err := cfg.Store.Stamp()
	if err != nil {
		return err
	}
	c := b.imageControl
	candidate, previous, configDigest, err := brokerruntime.PreflightImages(args, stamp, cfg.Transfers != nil, candidatePath, previousPath, expectedPrevious, deadline)
	if err != nil {
		return err
	}
	c.args, c.candidate, c.previous, c.configDigest = append([]string{}, args...), candidate, previous, configDigest
	c.cutID, err = brokerImageID()
	// ACTIVE rollback has a new image cut identity while retaining the original
	// fixed maintenance proof; reusing its image ID would disguise a stale cut.
	// origin: S6 internal review SINGLEACK F4 / plan §4.2 / T49
	if c.localMaintenance.Record.CutID != "" && b.reloadSignal.Requested() {
		c.cutID = c.localMaintenance.Record.CutID
	}
	if err != nil {
		return err
	}
	c.exporter, err = runningBinarySHA256()
	if err != nil {
		return err
	}
	if b.clusterMode {
		if err := brokerimage.CheckReloadRole(b.cl.node); err != nil {
			return err
		}
		c.role, err = cluster.ReadImageRole(b.cl.node)
		if err != nil {
			return err
		}
		m := &b.cl.homeMaintenance
		m.mu.Lock()
		if m.cutRecord.CutID != "" {
			c.cutID = m.cutRecord.CutID
		}
		// An ACTIVE rollback is a new cut of the current owner, with the
		// original fixed lease. Ordinary armed reloads retain their admission.
		if !b.reloadSignal.Requested() {
			m.admission, m.admitted = c.role, true
		}
		m.mu.Unlock()
	}
	return err
}

type brokerImageControl struct {
	diskDone                                                               <-chan struct{}
	broker                                                                 *Broker
	store                                                                  brokerimage.StoreOwner
	lock                                                                   *cluster.DataDirLock
	root                                                                   context.Context
	cancel                                                                 context.CancelFunc
	auth, apply                                                            *brokerAuthOwner
	subs                                                                   []*nats.Subscription
	joins                                                                  []<-chan struct{}
	http                                                                   []httplisten.ImageOwner
	keepers                                                                []listenerimage.Keeper
	users                                                                  brokerimage.StoreAdmission
	fenced, stopped                                                        bool
	adminDetached                                                          bool
	cut                                                                    *xferbridge.Cut
	args                                                                   []string
	cutID, instanceID, bootID, configDigest, exporter, candidate, previous string
	role                                                                   storage.RoleReceipt
	imported                                                               tunnel.ServerImported
	history                                                                *HomeMaintenanceHistory
	localMaintenance                                                       brokermaint.LocalLease
	transfers                                                              brokerimage.TransferOwner
	transferSubscriptions                                                  func(*nats.Conn) []TransferImageSubscription
}

func newBrokerImageControl(b *Broker) *brokerImageControl {
	root, cancel := context.WithCancel(context.Background()) // ctx-root: current broker control generation.
	return &brokerImageControl{broker: b, root: root, cancel: cancel}
}

func brokerImageID() (string, error) { return brokercontrol.MaintenanceNonce() }

func brokerProductionImageConfig(b *Broker, self string) cluster.ProductionConfig {
	sec := b.cfg.ClusterSecretsDir
	return brokerruntime.ProductionImageConfig(self, b.cfg.ClusterRaftAddr, b.cfg.Logger, func() (string, string) { return b.cfg.DBPath, b.cfg.ClusterDataDir },
		filepath.Join(sec, secretClusterCA), filepath.Join(sec, secretRouteCert), filepath.Join(sec, secretRouteKey))
}

func replaceBrokerImageControl(b *Broker, store brokerimage.StoreOwner, old *brokerImageControl) (*brokerImageControl, error) {
	c := newBrokerImageControl(b)
	c.store, c.keepers = store, old.keepers
	c.instanceID, c.bootID, c.cutID = old.instanceID, old.bootID, old.cutID
	c.args, c.configDigest, c.exporter, c.candidate, c.previous = old.args, old.configDigest, old.exporter, old.candidate, old.previous
	c.imported, c.adminDetached = old.imported, old.adminDetached
	c.history, c.localMaintenance = old.history, old.localMaintenance
	c.transfers, c.transferSubscriptions = old.transfers, old.transferSubscriptions
	var err error
	var currentDB *sql.DB
	if b.clusterMode {
		node, nodeErr := brokerimage.ClusterStoreNode(store)
		err = nodeErr
		if node != nil {
			b.cl = &clusterRuntime{node: node, fsArm: newForceSingleArm()}
			currentDB = node.RODB()
		}
	} else {
		currentDB, err = brokerimage.SingleStoreDB(store)
		if err == nil {
			err = currentDB.QueryRow("SELECT generation FROM proxy_meta WHERE id=1").Scan(&b.proxyGen)
		}
	}
	// The joined old generation no longer borrows this config. Install the
	// current pool as part of its replacement configuration in one place.
	cfg := b.cfg
	cfg.DB = currentDB
	b.cfg = cfg
	if err == nil {
		c.localMaintenance, err = brokercontrol.BindLocalMaintenance(currentDB, c.localMaintenance)
	}
	if b.clusterMode && err == nil {
		err = b.wireClusterEarly()
		if err == nil && c.history != nil {
			// Old control loops have joined. Replace their historical projection
			// together with the new Node/cache, including on returned exec.
			b.proxyDwell = sync.Map{}
			b.autoRebalanceArm = autoRebalanceArm{}
			err = RestoreHomeMaintenanceHistory(b, *c.history)
		}
	}
	// Install partial ownership before inspecting its constructor error.
	b.imageControl = c
	return c, err
}

func (c *brokerImageControl) FenceTransfers(d controlbudget.Deadline) (uint32, error) {
	return FenceBrokerTransfers(c.broker, d)
}
func (c *brokerImageControl) ResumeTransfers() error { ResumeBrokerTransfers(c.broker); return nil }
func (c *brokerImageControl) Fence(d controlbudget.Deadline) error {
	c.fenced = true
	c.cancel()
	b := c.broker
	b.transferAuditMu.Lock()
	b.transferAuditDraining.Store(true)
	b.transferAuditMu.Unlock()
	if b.cl != nil && b.cl.cancel != nil {
		b.cl.cancel()
	}
	var result error
	if b.admin != nil {
		result = errors.Join(result, b.admin.FenceImage(d))
	}
	for _, owner := range c.http {
		result = errors.Join(result, owner.Fence(d))
	}
	// Unsubscribe stops future dispatch; the subscription close callback and
	// borrower counter together join both queued and already-admitted handlers.
	for _, sub := range c.subs {
		result = errors.Join(result, sub.Unsubscribe())
	}
	if b.cl != nil {
		// The initiating remote reload owns its reply until Armed. It is
		// DB-free after admission and joined separately by StopTransport.
		joins, err := brokerruntime.UnsubscribeJoined(b.cl.subs, proto.SubjCtrlClusterUpgradeWildcard)
		c.joins, result = append(c.joins, joins...), errors.Join(result, err)
	}
	// The S9 auth-only queue leaves before the S6 owner refuses callbacks (P22).
	result = errors.Join(result, b.infra.Retire(d), stopBrokerImageAuth(c.auth, d), stopBrokerImageAuth(c.apply, d))
	return result
}
func (c *brokerImageControl) JoinStoreUsers(d controlbudget.Deadline) error {
	if err := c.users.Fence(d); err != nil {
		return err
	}
	for _, done := range c.joins {
		if err := brokerruntime.JoinDone(d, done); err != nil {
			return err
		}
	}
	// Closed callbacks above prove retired handlers have joined. Keep only the
	// live reply owner so a later reversible cut never unsubscribes stale handles.
	// origin: S6 internal review rv-final F2
	if c.broker.cl != nil {
		c.broker.cl.subs = brokerruntime.LiveSubscriptions(c.broker.cl.subs)
	}
	if c.broker.admin != nil {
		if err := c.broker.admin.JoinImage(d); err != nil {
			return err
		}
		// Relinquish the old Accept view while it is paused. Thaw resumes the
		// keeper before business construction, when Export is no longer legal.
		// The old server still owns its DB-free initiating reply until Armed.
		// origin: hot-update S6 plan §6.1 / T19
		if !c.adminDetached {
			if _, err := c.broker.admin.DetachImage(); err != nil {
				return err
			}
			c.adminDetached = true
		}
	}
	for _, owner := range c.http {
		if err := owner.Join(d); err != nil {
			return err
		}
	}
	if err := JoinBrokerStoreLoops(c.broker, d); err != nil {
		return err
	}
	if c.broker.clusterMode {
		// Capture identities while SQLite is still open, after all mutating
		// loops joined. Export runs after actual store closure and cannot query.
		// origin: hot-update S6 plan §2.2 / §7.2 / T43
		history, err := ExportHomeMaintenanceHistory(c.broker)
		if err != nil {
			return err
		}
		c.history = &history
	}
	// The continuation graph reads SQL and writes its ledger: it is joined here
	// and StartBusiness reinstalls it from the durable ledger (same origin).
	if err := suspendBrokerTransfers(c.broker, d); err != nil {
		return err
	}
	if err := JoinTransferAudit(c.broker, d); err != nil {
		return err
	}
	if c.broker.cl != nil && c.broker.cl.transferRPC != nil {
		if err := JoinTransferClusterForward(c.broker.cl.transferRPC, d); err != nil {
			return err
		}
	}
	if c.diskDone != nil {
		return brokerruntime.JoinDone(d, c.diskDone)
	}
	return nil
}
func (c *brokerImageControl) StopTransport(d controlbudget.Deadline) error {
	if c.stopped {
		return nil
	}
	if nc := c.broker.nc.Load(); nc != nil {
		if err := brokerruntime.StopTransport(nc, d); err != nil {
			return err
		}
	}
	c.broker.nc.Store(nil)
	c.stopped = true
	return nil
}
func (c *brokerImageControl) StartAuth(d controlbudget.Deadline) error {
	b := c.broker
	if c.root.Err() != nil {
		// A reversible fence canceled this control generation's readers. Thaw
		// starts a fresh root after their real join, leaving Run's stop intact.
		// origin: hot-update S6 plan §4.2 / rv-fd-6
		c.root, c.cancel = context.WithCancel(context.Background()) // ctx-root: thawed control generation.
	}
	if b.nc.Load() == nil {
		// The restored graph dials through the cold S9 descriptor (hot entry, T48).
		if err := brokerInfraConnect(b, d); err != nil {
			return err
		}
	}
	js, err := b.infra.BindJetStream(jetstream.New(b.nc.Load()))
	if err != nil {
		return err
	}
	if rebind := b.clusterMode && (b.cl.forwarder == nil || c.fenced); rebind && b.infra.Configured() {
		// S9 auth-first (T48): the apply responder needs a connected NC, so the configured graph
		// binds only the forwarder object the callout checks; StartBusiness subscribes the
		// responder through wireClusterLate exactly as cold Run does.
		b.cl.forwarder, c.apply = b.newForwarder(b.nc.Load(), b.cfg.ExposeForwardTimeout()), nil
	} else if rebind {
		c.apply, err = rebindBrokerImageAuthority(b, b.nc.Load(), d)
		if err != nil {
			return err
		}
	}
	if b.cfg.AuthCallout != nil {
		c.auth, err = startBrokerImageAuth(b, b.nc.Load(), d)
		if err != nil {
			return err
		}
	} else {
		// Anonymous deployments have no callout blackout. This event denotes
		// transport readiness only; configured callout requires its real probe.
		emitBrokerImageEvent(b, "auth_usable", 0)
	}
	// Rebind the client without IO before transfer owners resume; they must not
	// capture the retired NC while full-ready still awaits the external witness.
	// origin: S6 internal review rv-final F3 / S7 plan §8.2
	setBrokerJS(b, js)
	if b.clusterMode {
		// Auth is usable first. Await the sole voter's election before exposing
		// its inherited server to reconnects: premature home_catching_up DENY
		// spends another backoff after the store is already ready.
		// origin: hot-update S6 plan §5 / T37
		return brokerruntime.WaitSoleVoter(b.cl.node, d)
	}
	return nil
}
func (c *brokerImageControl) StartBusiness(d controlbudget.Deadline) error {
	b := c.broker
	// Auth owns a proved callout before JS is consulted. A failed witness keeps
	// that owner usable but cannot publish business/full-ready.
	// origin: S6 internal review rv-final F3
	// Read the existing external service. Hot activation must never run cold
	// Ensure/Reconcile routines that can recreate streams or reap live objects.
	// origin: hot-update S6 plan §9.2 / T51
	if err := brokercontrol.WitnessExistingJS(c.root, brokerJS(b), d); err != nil {
		return err
	}
	c.subs, c.joins = nil, nil
	if c.fenced {
		// Same-store failed Freeze resumes existing handlers; a reopened store
		// has a new owner and builds every DB-bearing handler below.
		c.fenced = false
		c.users.Resume()
	}
	if b.clusterMode {
		if err := brokerimage.ResumeClusterStore(c.store); err != nil {
			return err
		}
		if err := b.wireClusterLate(c.root, b.nc.Load()); err != nil {
			return err
		}
	}
	if err := resumeBrokerTransferGraph(b, b.nc.Load(), d); err != nil {
		return err
	}
	if err := subscribeBrokerImageBusiness(b, b.nc.Load()); err != nil {
		return err
	}
	if err := wakeBrokerImageAgents(b, b.nc.Load()); err != nil {
		return err
	}
	if err := startBrokerImageListeners(b, d); err != nil {
		return err
	}
	left, err := brokerAuthRemaining(d)
	if err != nil {
		return err
	}
	if err := b.nc.Load().FlushTimeout(left); err != nil {
		return err
	}
	if err := finishBrokerImageMaintenance(b, brokerimage.RestoredStore(b.cfg.ImageRun) && !b.reloadSignal.Requested()); err != nil {
		return err
	}
	b.transferAuditDraining.Store(false)
	b.startDiskMonitor(c.root)
	setBrokerImagePhase(b, proto.BrokerImagePhaseServing)
	bindBrokerImageHealth(b)
	emitBrokerImageEvent(b, "control_ready", 0)
	return nil
}

// Rebuilt subscriptions missed beats published in the cut (brokerruntime.WakeAgents asks the
// actual agents for their current heartbeat). origin: hot-update S6 plan §8.1 / T50 / EXITS-2-3
func wakeBrokerImageAgents(b *Broker, nc *nats.Conn) error {
	return brokerruntime.WakeAgents(b.read().SQL(), nc)
}

func (c *brokerImageControl) StopCurrent(d controlbudget.Deadline) error {
	var result error
	if !c.fenced {
		result = c.Fence(d)
	}
	result = errors.Join(result, c.JoinStoreUsers(d))
	if c.broker.admin != nil {
		result = errors.Join(result, c.broker.admin.Close())
	}
	for _, owner := range c.http {
		result = errors.Join(result, owner.Stop(d))
	}
	for _, keeper := range c.keepers {
		result = errors.Join(result, keeper.Close())
	}
	if c.broker.nc.Load() != nil {
		result = errors.Join(result, c.StopTransport(d))
	}
	return errors.Join(result, c.broker.infra.Close(d))
}

// Both shapes sit behind S9's synchronous admission fence (rv-business F2).
func brokerBusinessHandler(b *Broker, handler nats.MsgHandler) nats.MsgHandler {
	if b.imageControl == nil {
		return infraattach.Admit(b.infra, handler)
	}
	c := b.imageControl
	return infraattach.Admit(b.infra, func(msg *nats.Msg) {
		if !c.users.Begin() {
			return
		}
		defer c.users.End()
		handler(msg)
	})
}
func trackBrokerSubscription(b *Broker, sub *nats.Subscription) {
	if b.imageControl == nil {
		return
	}
	infraattach.TrackSubscription(sub, &b.imageControl.subs, &b.imageControl.joins)
}
func subscribeBrokerImageBusiness(b *Broker, nc *nats.Conn) error {
	subjects := append([]brokerBusinessSubscription{
		{proto.SubjectPrefix + ".ctrl.s.*.node.*.register.req", b.handleRegister},
		// origin: S8 FIX-DESCRIBE — the restored graph relays control probes too.
		{proto.SubjectPrefix + ".ctrl.s.*.node.*.heartbeat", brokerupgrade.WithControlProbe(nc, b.handleHeartbeat)},
	}, brokerBusinessSubscriptions(b, nc)...)
	for _, spec := range subjects {
		var sub *nats.Subscription
		var err error
		if infraattach.QueueBusiness(b.clusterMode, spec.subj, isBroadcastClusterSubject(spec.subj)) {
			sub, err = nc.QueueSubscribe(spec.subj, ctlQueueGroup, brokerBusinessHandler(b, spec.handler))
		} else {
			sub, err = nc.Subscribe(spec.subj, brokerBusinessHandler(b, spec.handler))
		}
		if err != nil {
			return err
		}
		trackBrokerSubscription(b, sub)
	}
	return nil
}

// brokerLiveServer gives the initial graph the same terminal projection as a
// restored server. Pristine operations are illegal on this already-live owner.
type brokerLiveServer struct{ server *tunnel.Server }

func (s *brokerLiveServer) Server() *tunnel.Server              { return s.server }
func (*brokerLiveServer) Activate(controlbudget.Deadline) error { return brokerimage.ErrActive }
func (*brokerLiveServer) DetachPristine() (tunnel.Salvage, error) {
	return tunnel.Salvage{}, brokerimage.ErrActive
}

// projectBrokerTransferCut reads the current control generation at each final
// export. The sealed codec and resource provenance live in the leaf bridge.
// origin: hot-update S7 plan §8.2 / P24
// An armed S8 cut's sealed adjunct travels as Extra (S8 BROKER-EXPORTER R-11).
func projectBrokerTransferCut(c *brokerImageControl) (xferbridge.Projection, error) {
	p := xferbridge.Projection{Generation: brokerImageSnapshot(c.broker).Generation + 1,
		InstanceID: c.instanceID, BootID: c.bootID, ExporterSHA256: c.exporter, CandidateSHA256: c.candidate,
		PreviousSHA256: c.previous, ConfigDigest: c.configDigest, CutID: c.cutID, Args: c.args,
		Maintenance: []brokermaint.LocalLease{}, Extra: brokerupgrade.CutResources(c.broker.cfg.Upgrade)}
	if c.history != nil {
		var err error
		p.HomeHistory, err = json.Marshal(c.history)
		if err != nil {
			return p, err
		}
		if c.history.Cache != nil {
			for _, observation := range c.history.Cache.Observations {
				p.Maintenance = append(p.Maintenance, observation.Lease)
			}
			sort.Slice(p.Maintenance, func(i, j int) bool { return p.Maintenance[i].Record.Home < p.Maintenance[j].Record.Home })
		}
	}
	if c.localMaintenance.Record.CutID != "" {
		p.Maintenance = append(p.Maintenance, c.localMaintenance)
	}
	return p, nil
}

func brokerImageHTTPHandler(b *Broker, id string) (http.Handler, error) {
	switch id {
	case "broker.sub-http":
		return subhttp.Handler(subhttp.Config{DB: b.read().SQL(), PublicHost: b.publicHostFor(), Logger: b.cfg.Logger,
			ClusterMode: b.clusterMode, Vendable: b.proxyVendable}), nil
	case "broker.metrics":
		return brokermetrics.Handler(b.metricsSnapshot, b.metricsReady), nil
	case "broker.manifest":
		return clustermanifest.Handler(b.manifestBytes), nil
	default:
		return nil, errors.New("broker: unknown HTTP listener owner")
	}
}

func startBrokerOwnedHTTP(b *Broker, ln net.Listener, id string, d controlbudget.Deadline) error {
	keeper, err := listenerimage.Capture(ln, id)
	if err != nil {
		return errors.Join(err, ln.Close())
	}
	b.imageControl.keepers = append(b.imageControl.keepers, keeper)
	return startBrokerImageHTTP(b, keeper, id, d)
}

// startBrokerImageHTTP serves one kept listener with the current owner's handler; the cold
// start and the restored graph share it so their HTTP ownership cannot diverge.
func startBrokerImageHTTP(b *Broker, keeper listenerimage.Keeper, id string, d controlbudget.Deadline) error {
	handler, err := brokerImageHTTPHandler(b, id)
	if err != nil {
		return err
	}
	owner, err := httplisten.StartImage(keeper, handler, id, d)
	if owner != nil {
		b.imageControl.http = append(b.imageControl.http, owner)
	}
	return err
}

func brokerImageAdminBackend(b *Broker) adminsock.Backend {
	// The restored graph serves the same S8 endpoint as cold Run (origin: S8 plan §2.2 / P15).
	backend := infraattach.BindAdmin(b.infra, adminsock.Backend{DB: b.read().SQL(), Now: b.cfg.Now, Logger: b.cfg.Logger, UpgradeEndpoint: brokercontrol.BindAdminUpgrade(b.upgradeEndpoint), UpgradeKey: b.upgradeKey,
		AuditTail: b.adminAuditTail, EventsTail: b.adminEventsTail, PubAgentEvicted: b.pubAgentEvicted, RuntimeSnapshot: b.runtimeSnapshot})
	if b.clusterMode {
		backend.Cluster = NewClusterAdminBackend(b.cl.admin, b.clusterCaughtUp, b.clusterStreamsReady,
			b.adminAuditTail, func() ([]string, error) { return listSessionsByState(b.read().SQL(), "ACTIVE") }, b.accountPubOrEmpty)
		if cab, ok := backend.Cluster.(*clusterAdminBackend); ok {
			cab.rebalanceProxy, cab.fsArm, cab.requestReExec = b.rebalanceProxyHomes, b.cl.fsArm, b.RequestReExec
		}
		backend.EvictWrite, backend.ClusterMode = b.evictNode, true
		backend.SessionCreatorWrite = func(fp, addedBy, note string, allow bool) error {
			return setSessionCreator(b, fp, addedBy, note, allow)
		}
	}
	backend.Cluster = &brokerReloadBackend{broker: b, cluster: backend.Cluster}
	b.setClusterAdminHandle(backend.Cluster.HandleCluster)
	return backend
}

func startBrokerImageListeners(b *Broker, d controlbudget.Deadline) error {
	c := b.imageControl
	c.http = nil
	for _, keeper := range c.keepers {
		id := keeper.Descriptor().ID
		if id == "admin.listener" {
			if b.admin != nil && !c.adminDetached {
				if _, err := b.admin.DetachImage(); err != nil {
					return err
				}
			}
			b.admin = adminsock.New(b.cfg.AdminSocketPath, brokerImageAdminBackend(b))
			c.adminDetached = false
			if err := b.admin.StartInherited(keeper, d); err != nil {
				return err
			}
			continue
		}
		if err := startBrokerImageHTTP(b, keeper, id, d); err != nil {
			return err
		}
	}
	return nil
}

func activateRestoredBrokerImage(b *Broker, r *BrokerImageRun, boot *brokerimage.BootImage, args []string) error {
	if boot == nil {
		return brokerimage.ErrImage
	}
	bundle := boot.Bundle()
	c := newBrokerImageControl(b)
	if b.imageControl != nil {
		c.transfers, c.transferSubscriptions = b.imageControl.transfers, b.imageControl.transferSubscriptions
	}
	b.imageControl = c
	c.args, c.instanceID, c.bootID = args, bundle.InstanceID, bundle.BootID
	c.configDigest = bundle.ConfigDigest
	if b.cfg.StableTunnelCertDir != "" {
		cert, present, err := loadOptionalStableTunnelCert(b.cfg.StableTunnelCertDir)
		if err != nil {
			return err
		}
		if present {
			b.tunnelCert = cert
		}
	}
	budget := &workload.ReplayBudget{Limit: bundle.Tunnel.ReplayLimit}
	tunnelConfig, err := brokerTunnelImportConfig(b, bundle.Tunnel, budget)
	if err != nil {
		return err
	}
	deadline, err := brokerCutDeadline()
	if err != nil {
		return err
	}
	var files map[string]*os.File
	cfg := brokerimage.Config{Mode: bundle.Mode, Deadline: deadline, ImportServer: tunnel.ImportServer, TunnelConfig: tunnelConfig, Transfers: c.transfers,
		NewStore: func(stamp storage.StoreStamp, file *os.File, d controlbudget.Deadline) (brokerimage.StoreOwner, error) {
			if b.clusterMode {
				return brokerimage.NewClusterStoreForImage(brokerProductionImageConfig(b, bundle.InstanceID), stamp, file, d)
			}
			return brokerimage.NewSingleStoreForImage(stamp, file, d)
		}, NewControl: func(store brokerimage.StoreOwner, _ controlbudget.Deadline) (brokerimage.ControlOwner, error) {
			current, err := replaceBrokerImageControl(b, store, c)
			if err != nil {
				return current, err
			}
			for _, desc := range bundle.Listeners {
				keeper, err := listenerimage.Adopt(desc, files[desc.ID])
				if keeper != nil {
					current.keepers = append(current.keepers, keeper)
				}
				if err != nil {
					return current, err
				}
			}
			return current, nil
		}}
	image, err := boot.Import(cfg)
	if err != nil {
		return err
	}
	if transfer, _ := brokerimage.CurrentTransferOwner(image); transfer != nil {
		current, ok := transfer.(brokerimage.TransferOwner)
		if !ok {
			return brokerimage.ErrImage
		}
		c.transfers = current
	}
	private, err := brokerimage.ImportedPrivateState(image)
	if err != nil {
		return err
	}
	c.cutID = private.Runtime.CutID
	c.localMaintenance = brokerruntime.LocalMaintenance(bundle.Maintenance)
	if len(private.Control.HomeHistory) != 0 {
		var history HomeMaintenanceHistory
		if err := json.Unmarshal(private.Control.HomeHistory, &history); err != nil {
			return brokerimage.ErrImage
		}
		c.history = &history
	}
	if err := InitializeBrokerImageStatus(b, bundle.Generation, c.cutID); err != nil {
		return err
	}
	_, _, sleepingServer, _ := brokerimage.CurrentOwners(image)
	c.imported = sleepingServer
	if sleepingServer != nil {
		b.tunnelSrv.Store(sleepingServer.Server())
	}
	// Read the sleeping owners before Activate takes its constructor latch.
	// The resulting borrows are adopted only by the ACTIVE control constructor.
	files = brokerimage.ImportedListenerFiles(image)
	b.reconcilers = newReconcileRegistry(b.cfg.Logger, b.reconcileLeaderGate)
	b.reconcilers.now = b.cfg.Now
	b.registerCoreReconcilePasses()
	emitBrokerImageEvent(b, "boot_claim", 0)
	if err := image.Activate(r.Slot, deadline); err != nil {
		return err
	}
	_, _, server, active := brokerimage.CurrentOwners(image)
	if !active || server == nil {
		return brokerimage.ErrActive
	}
	b.tunnelSrv.Store(server.Server())
	b.reconcilers.start(b.cfg.Now())
	if err := finishBrokerImageMaintenance(b, false); err != nil {
		return err
	}
	if b.cfg.ReadyCh != nil {
		close(b.cfg.ReadyCh)
	}
	return nil
}

// An ACTIVE import failure cannot return into Run's terminal cleanup before the
// current graph has had a chance to reach its SHA-bound previous reader. This
// is a fresh cut of today's disk/raw offsets, never Repack of the initial cut.
// origin: hot-update S6 plan §4.2 / T49
func rollbackRestoredBrokerImage(b *Broker, r *BrokerImageRun, boot *brokerimage.BootImage, cause error) error {
	store, control, server, active := brokerimage.CurrentOwners(boot.Current())
	if !active || store == nil || control == nil || server == nil || b.runCtx.Err() != nil {
		return cause
	}
	current, ok := control.(*brokerImageControl)
	if !ok {
		return cause
	}
	// The constructor already installed these exact current owners. Rewriting
	// their published fields here races live runtime observations during rollback.
	// origin: S6 internal review SINGLEACK F5 / plan §4.2 / T49
	b.tunnelSrv.Store(server.Server())
	// Probe both actual readers using the canonical staged pathname, then select
	// the previous reader for this cut. The source remains the current artifact.
	rollback := *r
	rollback.CandidatePath = r.CandidatePath + ".prev"
	rollback.Preflight = func(owner *Broker, d controlbudget.Deadline) error {
		return preflightBrokerImage(owner, r, current.args, d, rollback.CandidatePath, rollback.CandidatePath, boot.Bundle().PreviousSHA256)
	}
	err := runBrokerImageReloadUsing(b, &rollback, brokerNodeShutdown(b, rollback.PrepareNodeShutdown))
	return errors.Join(cause, err)
}

// brokerNodeShutdown reads the CURRENT cluster runtime at each step: a returned
// or rolled-back cut may already have replaced the Node this reload started on.
func brokerNodeShutdown(b *Broker, prepare brokerimage.PrepareImageShutdownFunc) brokerimage.StoreShutdown {
	return brokerruntime.NodeShutdown(func() *cluster.Node {
		if b.cl == nil {
			return nil
		}
		return b.cl.node
	}, prepare)
}

// The restored loop has no cold migration, reconcile or restart-event path.
// It uses the same Run-local shutdown/exec action as the cold loop. The image is
// ACTIVE and serving here, so an inherited S8 transaction settles exactly its own
// captured handle through the endpoint's sequencer (origin: S8 plan §3.3 / P10→P15).
func runBrokerImageLoop(ctx context.Context, b *Broker) error {
	brokerupgrade.ResumeImported(ctx, b.upgradeEndpoint, b.cfg.Upgrade, b.cfg.Logger)
	granularity := b.reconcilers.granularity()
	if granularity <= 0 {
		granularity = b.cfg.ReconcileInterval
	}
	ticker := time.NewTicker(granularity)
	defer ticker.Stop()
	defer unbindBrokerImageHealth(b)
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-brokerReloadRequests(b):
			err := runBrokerImageReload(b, brokerNodeShutdown(b, brokerimage.StoreShutdownPrepare(b.cfg.ImageRun)))
			notifyBrokerReloadArmed(b, err)
			b.reloadSignal.Clear()
			if err != nil {
				b.cfg.Logger.Error("broker: image reload returned", "err", err)
			}
		case <-ticker.C:
			b.reconcilers.runDue(ctx, b.cfg.Now())
		}
	}
}
