// Store reload glue lives beside the single adapter to keep Broker below its
// frozen structural cap; this file contains no Broker/command dependency.
// origin: hot-update S6 plan §3.2 / §9.3 / §12.1 P11
package brokerimage

import (
	"errors"
	"fmt"
	"github.com/LinZiyang666/tether/internal/adminsock"
	"github.com/LinZiyang666/tether/internal/brokerupgrade"
	"github.com/LinZiyang666/tether/internal/cluster"
	"github.com/LinZiyang666/tether/internal/controlbudget"
	"github.com/LinZiyang666/tether/internal/handoff"
	"github.com/LinZiyang666/tether/internal/proto"
	"github.com/LinZiyang666/tether/internal/rawsocket"
	"github.com/LinZiyang666/tether/internal/storage"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

// StoreRun holds the current-store cut adapters. It stays independent of Broker:
// callbacks bind process roots while this leaf enforces close/export/exec order.
// origin: hot-update S6 plan §3.2 / §12.1 P11
type StoreRun[Owner any] struct {
	Restored bool
	// RunRestored owns current control roots and handles ACTIVE rollback before
	// returning; a return is terminal StopCurrent, never a cold-start fallback.
	RunRestored         func(Owner) error
	Slot                RuntimeSlot
	Capture             func(Owner, controlbudget.Deadline) (Config, error)
	Freeze              FreezeFunc
	Preflight           func(Owner, controlbudget.Deadline) error
	PrepareNodeShutdown PrepareImageShutdownFunc
	InstallCurrent      func(Owner) error
	Recover             func(Owner, Cut, storage.StoreSeal, controlbudget.Deadline) error
	DrainArmed          func(controlbudget.Deadline) error
	WithExecLatch       func(func() error) error
	CandidatePath       string
	Environment         []string
}

func ValidateStoreRun[Owner any](r *StoreRun[Owner]) error {
	if r == nil || r.Slot == nil || r.Capture == nil || r.Freeze == nil || r.Preflight == nil ||
		r.PrepareNodeShutdown == nil || r.InstallCurrent == nil || r.Recover == nil ||
		r.DrainArmed == nil || r.WithExecLatch == nil || r.CandidatePath == "" {
		return fmt.Errorf("broker: broker image owners are not installed")
	}
	return nil
}

// Missing image wiring must reach ValidateStoreRun's refusal inside the live
// Run loop. Reading the callback before that guard would panic and unwind Run.
// origin: S7 internal review GATE-CONVERGE-layering F6 / TestBrokerRunLeakCycles
func StoreShutdownPrepare[Owner any](r *StoreRun[Owner]) PrepareImageShutdownFunc {
	if r == nil {
		return nil
	}
	return r.PrepareNodeShutdown
}

// StoreRunCallbacks bind process observations and actual Run-local Exec; these
// callbacks never replace the frozen StoreOwner/Cut/RuntimeSlot signatures.
type StoreRunCallbacks[Owner any] struct {
	Deadline func() (controlbudget.Deadline, error)
	Phase    func(Owner, string)
	Resume   func(Owner)
	Bind     func(Owner)
	Armed    func(Owner, error)
	Exec     func(*handoff.Prepared, Bundle, string, []string) error
	// Admission failure is reversible until an actual shutdown starts. Keep
	// role/maintenance validation ahead of the store-closing boundary.
	// origin: hot-update S6 plan §5 / T38 / internal review rv-cluster-7
	BeforeShutdown func(Owner) error
}

// StoreShutdown separates reversible validation/preparation from the actual
// close entry. A Prepare error cannot certify that any close was attempted;
// once Close is entered even an error requires known closed phases to reopen.
// origin: S6 internal review rv-final F1
type StoreShutdown struct {
	Prepare      func(controlbudget.Deadline) error
	Close        func() error
	AfterPrepare func() error
}

// ReloadCurrentStore closes the current store before sealing any image. A
// failed close uses phase-aware recovery; a returned exec closes only its copies
// before the current pathname is reopened. No database payload is restored.
// origin: hot-update S6 plan §3.2 / §4.2 / T01
func ReloadCurrentStore[Owner any](r *StoreRun[Owner], b Owner, shutdown StoreShutdown, callbacks StoreRunCallbacks[Owner]) error {
	if err := validateStoreRunCallbacks(callbacks); err != nil {
		return err
	}
	// A preflight/freeze refusal restores transfer admission without stopping live
	// transfers. Successful exec never reaches this cleanup in the old image.
	restored := true
	defer func() {
		if restored {
			callbacks.Resume(b)
		}
	}()
	cfg, deadline, err := captureStoreCut(r, b)
	if err != nil {
		return err
	}
	callbacks.Phase(b, proto.BrokerImagePhasePreparing)
	cut, err := r.Freeze(cfg)
	if err != nil {
		restored, err = recoverFreezeRefusal(b, cut, deadline, callbacks, err)
		return err
	}
	if cut == nil {
		callbacks.Phase(b, proto.BrokerImagePhaseFailed)
		return fmt.Errorf("broker: freeze returned no cut")
	}
	callbacks.Phase(b, proto.BrokerImagePhaseFrozen)
	closed := false
	var seal storage.StoreSeal
	var prepared *handoff.Prepared
	defer func() {
		if prepared != nil {
			_ = prepared.Close()
		}
	}()
	recoverCurrent := func(cause error) error {
		// Partial Prepare and later pre-exec failures own copies too. Reclaim those
		// aliases before current-store reopen rather than after recovery's census.
		// origin: hot-update S6 plan §3.2 / §4.2 / T02
		if prepared != nil {
			cause = errors.Join(cause, prepared.Close())
			prepared = nil
		}
		recovered, result := recoverStoreCut(r, b, cut, seal, closed, deadline, callbacks, cause)
		if !recovered {
			restored = false
		}
		return result
	}
	// A partial Node shutdown is already a potentially closed store. It must use
	// the phase-aware recovery path, never a blind pre-close Thaw.
	if err := prepareStoreShutdown(b, shutdown, deadline, callbacks); err != nil {
		return recoverCurrent(err)
	}
	if err := MarkStoreClosing(cut); err != nil {
		return recoverCurrent(err)
	}
	closed = true
	seal, err = closeStoreShutdown(cfg.Store, shutdown)
	if err != nil {
		return recoverCurrent(err)
	}
	seal, err = cfg.Store.CloseForImage(deadline)
	if err != nil {
		if seal.Stamp.Identity.Path == "" {
			seal = unknownStoreCloseSeal(cfg.Store)
		}
		return recoverCurrent(err)
	}
	if !StoreSealClosed(seal) {
		return recoverCurrent(fmt.Errorf("broker: %s", DetailStoreCloseUnknown))
	}
	bundle, resources, err := cut.Export(seal)
	if err != nil {
		return recoverCurrent(fmt.Errorf("brokerimage: export cut: %w", err))
	}
	prepared, err = cut.Prepare(bundle, resources)
	if err != nil {
		return recoverCurrent(fmt.Errorf("brokerimage: prepare cut: %w", err))
	}
	if prepared == nil {
		return recoverCurrent(fmt.Errorf("broker: prepare returned no image"))
	}
	// Only the final closed-store Prepared qualifies as armed. The handler now
	// has its DB-free reply; DrainArmed waits for its actual write/Flush receipt.
	// origin: hot-update S6 plan §3.2 / T01
	callbacks.Armed(b, nil)
	if err := r.DrainArmed(deadline); err != nil {
		return recoverCurrent(err)
	}
	if err := cfg.Control.StopTransport(deadline); err != nil {
		return recoverCurrent(err)
	}
	// The latch owner may regenerate a dirty Prepared before permitting this call.
	// It must not invoke the closure until all final guards are true.
	env := r.Environment
	if env == nil {
		env = os.Environ()
	}
	for {
		calls := 0
		var executionErr error
		err = r.WithExecLatch(func() error {
			calls++
			if calls != 1 {
				return fmt.Errorf("broker: exec commit invoked more than once")
			}
			now, clockErr := rawsocket.BootNow()
			if clockErr != nil {
				return clockErr
			}
			if int64(deadline) <= now {
				return fmt.Errorf("brokerimage: final exec deadline expired")
			}
			executionErr = callbacks.Exec(prepared, bundle, r.CandidatePath, handoff.CleanEnvironment(env))
			return executionErr
		})
		if calls == 0 && err == nil {
			err = fmt.Errorf("broker: exec commit did not invoke exec")
		}
		err = errors.Join(err, executionErr)
		if !errors.Is(err, ErrCutChanged) {
			break
		}
		// An ACKed hard revoke closed the old Prepared under the same cut latch.
		// Re-export the still-closed current store, including its latest fences,
		// instead of reopening or trying an obsolete resource graph. Every retry
		// retains the original absolute deadline and joined transport safe point.
		// origin: hot-update S6 plan §6.2 / T25
		if closeErr := prepared.Close(); closeErr != nil {
			return recoverCurrent(errors.Join(err, closeErr))
		}
		prepared = nil
		var nextResources []handoff.Resource
		bundle, nextResources, err = cut.Export(seal)
		if err != nil {
			return recoverCurrent(err)
		}
		prepared, err = cut.Prepare(bundle, nextResources)
		if err != nil {
			return recoverCurrent(err)
		}
		if prepared == nil {
			return recoverCurrent(errors.New("broker: prepare returned no image"))
		}
	}
	// Release aliases BEFORE reopening; the original lock/raw/listeners remain
	// owned by the cut. Closing only a later defer leaves stale aliases in census.
	err = errors.Join(err, prepared.Close())
	prepared = nil
	return recoverCurrent(err)
}

// Preflight consumes its own budget while business continues. Capture starts
// the absolute cut budget shared by freeze, final export, exec and recovery.
// origin: hot-update S6 plan §3.1 / §3.2 / T06 / T01
func captureStoreCut[Owner any](r *StoreRun[Owner], b Owner) (Config, controlbudget.Deadline, error) {
	if err := ValidateStoreRun(r); err != nil {
		return Config{}, 0, err
	}
	deadline, err := NewStoreCutDeadline()
	if err != nil {
		return Config{}, 0, err
	}
	if err := r.Preflight(b, deadline); err != nil {
		return Config{}, 0, fmt.Errorf("brokerimage: preflight cut: %w", err)
	}
	// origin: hot-update S6 internal review rv-ctl-11
	// Preflight runs before the cut; candidate smoke must not spend the freeze
	// budget while original business still runs. Start one new absolute budget
	// for capture/freeze and retain it through export, exec and recovery.
	deadline, err = NewStoreCutDeadline()
	if err != nil {
		return Config{}, 0, err
	}
	cfg, err := r.Capture(b, deadline)
	if err != nil {
		return Config{}, 0, err
	}
	if cfg.Store == nil || cfg.Control == nil {
		return Config{}, 0, fmt.Errorf("broker: current image owners are missing")
	}
	cfg.Deadline = deadline
	return cfg, deadline, nil
}

// Recovery follows the actual store boundary: an open cut can thaw in place,
// while a potentially closed writer requires the owner's phase-aware reopen.
// origin: hot-update S6 plan §3.2 / §4.2 / T02 / T10
func recoverStoreCut[Owner any](r *StoreRun[Owner], b Owner, cut Cut, seal storage.StoreSeal, closed bool, deadline controlbudget.Deadline, callbacks StoreRunCallbacks[Owner], cause error) (bool, error) {
	var recovery error
	if closed {
		callbacks.Phase(b, proto.BrokerImagePhaseRollingBack)
		recovery = r.Recover(b, cut, seal, deadline)
	} else {
		recovery = cut.Thaw(deadline)
	}
	if recovery != nil {
		callbacks.Phase(b, proto.BrokerImagePhaseFailed)
		return false, errors.Join(cause, recovery)
	}
	callbacks.Phase(b, proto.BrokerImagePhaseControlReady)
	callbacks.Bind(b)
	return true, cause
}

// A partial freeze still owns its cut. Failed thaw keeps admission fenced;
// a successful refusal leaves the original store and transfers usable.
// origin: hot-update S6 plan §3.2 / §4.2 / T01
func recoverFreezeRefusal[Owner any](b Owner, cut Cut, deadline controlbudget.Deadline, callbacks StoreRunCallbacks[Owner], err error) (bool, error) {
	if cut != nil {
		if thawErr := cut.Thaw(deadline); thawErr != nil {
			callbacks.Phase(b, proto.BrokerImagePhaseFailed)
			return false, errors.Join(fmt.Errorf("brokerimage: freeze cut: %w", err), thawErr)
		}
	}
	callbacks.Phase(b, proto.BrokerImagePhaseControlReady)
	return true, fmt.Errorf("brokerimage: freeze cut: %w", err)
}

// Unknown close phases cannot certify an open graph or authorize another writer.
func unknownStoreCloseSeal(store StoreOwner) storage.StoreSeal {
	stamp, _ := store.Stamp()
	return storage.StoreSeal{Stamp: stamp, Writer: storage.CloseUnknown, ReadOnly: storage.CloseUnknown,
		Bolt: storage.CloseUnknown, Transport: storage.CloseUnknown, Raft: storage.CloseUnknown}
}

// NewStoreCutDeadline establishes the single absolute store-cut budget.
func NewStoreCutDeadline() (controlbudget.Deadline, error) {
	now, err := rawsocket.BootNow()
	if err != nil {
		return 0, err
	}
	return controlbudget.Deadline(now + int64(storeCutBudget)), nil
}

const storeCutBudget = 5 * time.Second

// StagedArtifactSHA256 hashes the installed pathname, not the running inode.
// RunningArtifactSHA256 is the independent completion witness on Linux.
// origin: hot-update S6 plan §5 / §9.1
// StoreAdmission covers work before it has a durable/tracker owner. Fence joins
// existing users while refusing new ones. A failed cut resumes the same counter,
// rather than clearing live work to manufacture an empty census.
// origin: hot-update S6 plan §3.2 / §5 / T45
type StoreAdmission struct {
	mu      sync.Mutex
	fenced  bool
	users   uint32
	changed chan struct{}
}

func (g *StoreAdmission) Begin() bool {
	g.mu.Lock()
	defer g.mu.Unlock()
	if g.fenced {
		return false
	}
	g.users++
	return true
}
func (g *StoreAdmission) End() {
	g.mu.Lock()
	defer g.mu.Unlock()
	g.users--
	if g.changed != nil {
		close(g.changed)
		g.changed = nil
	}
}
func (g *StoreAdmission) Resume() {
	g.mu.Lock()
	g.fenced = false
	g.mu.Unlock()
}
func (g *StoreAdmission) Fence(deadline controlbudget.Deadline) error {
	return JoinStoreUsers(deadline, func() (bool, <-chan struct{}) {
		g.mu.Lock()
		defer g.mu.Unlock()
		g.fenced = true
		if g.users == 0 {
			return true, nil
		}
		if g.changed == nil {
			g.changed = make(chan struct{})
		}
		return false, g.changed
	})
}

// JoinStoreUsers observes actual completion using a condition broadcast. Unlike
// a token channel it can retry after a refused cut without losing a receipt.
// The predicate owns its mutex; the blocking wait holds no owner table lock.
func JoinStoreUsers(deadline controlbudget.Deadline, predicate func() (bool, <-chan struct{})) error {
	for {
		left, err := controlbudget.Remaining(deadline)
		if err != nil {
			return err
		}
		done, changed := predicate()
		if done {
			return nil
		}
		timer := time.NewTimer(left)
		select {
		case <-changed:
			// Go 1.26 timers cannot deliver a stale value after Stop returns.
			timer.Stop()
		case <-timer.C:
			return fmt.Errorf("brokerimage: store users join deadline expired: %w", controlbudget.ErrExpired)
		}
	}
}

// StoreReloadSignal coalesces requests without cancelling the process root. The
// armed receipt is sent only after a final closed-store Prepared exists.
// origin: hot-update S6 plan §3.2 / §4 / T01
type StoreReloadSignal struct {
	mu       sync.Mutex
	pending  atomic.Bool
	requests chan struct{}
	armed    chan error
	deadline controlbudget.Deadline
}

func (s *StoreReloadSignal) Requests() <-chan struct{} {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.requests == nil {
		s.requests = make(chan struct{}, 1)
	}
	return s.requests
}
func (s *StoreReloadSignal) Request() {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.requests == nil {
		s.requests = make(chan struct{}, 1)
	}
	if s.pending.CompareAndSwap(false, true) {
		s.requests <- struct{}{}
	}
}
func (s *StoreReloadSignal) Requested() bool { return s.pending.Load() }
func (s *StoreReloadSignal) Clear() {
	s.mu.Lock()
	s.pending.Store(false)
	s.deadline = 0
	s.mu.Unlock()
}
func (s *StoreReloadSignal) Notify(err error) {
	s.mu.Lock()
	ack := s.armed
	s.armed = nil
	s.mu.Unlock()
	if ack != nil {
		ack <- err
	}
}
func (s *StoreReloadSignal) Arm(deadline controlbudget.Deadline) error {
	if _, err := controlbudget.Remaining(deadline); err != nil {
		return fmt.Errorf("brokerimage: reload arming deadline expired: %w", err)
	}

	ack := make(chan error, 1)
	s.mu.Lock()
	if s.pending.Load() {
		s.mu.Unlock()
		return fmt.Errorf("brokerimage: reload cut is already pending")
	}
	s.armed = ack
	s.deadline = deadline
	s.mu.Unlock()
	s.Request()
	left, err := controlbudget.Remaining(deadline)
	if err != nil {
		return fmt.Errorf("brokerimage: reload arming deadline expired: %w", err)
	}

	timer := time.NewTimer(left)
	defer timer.Stop()
	select {
	case err := <-ack:
		return err
	case <-timer.C:
		return fmt.Errorf("%s", DetailControlResultUnknown)
	}
}

// ValidateInitialStore proves a lock owner exists before any cold constructor.
// CloseStoreOwner is only startup/terminal cleanup, never an image detach.
func ValidateInitialStore[Owner any](r *StoreRun[Owner], owner Owner) (StoreOwner, error) {
	deadline, err := NewStoreCutDeadline()
	if err != nil {
		return nil, err
	}
	cfg, err := r.Capture(owner, deadline)
	if err != nil {
		return nil, err
	}
	if cfg.Store == nil {
		return nil, fmt.Errorf("brokerimage: initial store owner is missing")
	}
	lock, err := cfg.Store.LockResource()
	if err != nil {
		return cfg.Store, err
	}
	if len(lock.Files) != 1 {
		return cfg.Store, fmt.Errorf("brokerimage: initial lock owner is missing")
	}
	return cfg.Store, nil
}
func CloseStoreOwner(owner StoreOwner) error {
	if closer, ok := owner.(io.Closer); ok {
		return closer.Close()
	}
	return fmt.Errorf("brokerimage: store terminal closer is missing")
}

// TransferLedgerCensus never quarantines a row to make unknown work disappear.
// Record parsing stays with the transfer owner and returns only the public ID.
func TransferLedgerCensus(dirs []string, ids map[string]struct{}, read func(string) (string, error)) error {
	for _, dir := range dirs {
		if dir == "" {
			continue
		}
		entries, err := os.ReadDir(dir)
		if os.IsNotExist(err) {
			continue
		}
		if err != nil {
			return err
		}
		for _, entry := range entries {
			if strings.Contains(entry.Name(), ".json.corrupt") {
				return fmt.Errorf("brokerimage: unresolved transfer ledger row")
			}
			if !strings.HasSuffix(entry.Name(), ".json") {
				continue
			}
			id, err := read(filepath.Join(dir, entry.Name()))
			if err != nil {
				return err
			}
			ids[id] = struct{}{}
		}
	}
	return nil
}

// RuntimeStatus is the one value producer for local and remote observers. It
// measures the running inode and retains imported generation/cut across phases.
// origin: hot-update S6 plan §5 / §9.1
type RuntimeStatus struct {
	mu     sync.RWMutex
	status proto.BrokerImageStatus
}

func (s *RuntimeStatus) Snapshot() *proto.BrokerImageStatus {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if s.status.Phase == "" {
		return nil
	}
	value := s.status
	return &value
}
func (s *RuntimeStatus) SetPhase(phase, mode string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.status.RunningSHA256 == "" {
		if sha, err := RunningArtifactSHA256(); err == nil {
			s.status.RunningSHA256 = sha
		}
	}
	if s.status.Generation == 0 {
		s.status.Generation = 1
	}
	s.status.Phase, s.status.Mode = phase, mode
}
func (s *RuntimeStatus) Initialize(generation uint64, cutID string) error {
	if generation == 0 {
		return fmt.Errorf("brokerimage: image generation is zero")
	}
	sha, err := RunningArtifactSHA256()
	if err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.status.Generation != 0 {
		return fmt.Errorf("brokerimage: image boot lineage already initialized")
	}
	s.status = proto.BrokerImageStatus{Generation: generation, CutID: cutID, RunningSHA256: sha, Phase: proto.BrokerImagePhaseServing}
	return nil
}

// LocalStoreReload binds admission to one current owner. It is shared by the
// single local admin surface and authenticated remote cluster trigger.
// origin: hot-update S6 plan §5 / §12.1 P11
type LocalStoreReload[Owner any] struct {
	Image   *StoreRun[Owner]
	Owner   Owner
	Node    *cluster.Node
	Cluster adminsock.ClusterAdminBackend
	Gate    *sync.Mutex
	Signal  *StoreReloadSignal
	Fence   func(Owner, controlbudget.Deadline) (uint32, error)
	Resume  func(Owner)
	Status  func(Owner) *proto.BrokerImageStatus
	// Admission captures the live role under Gate, before Run can freeze.
	Admission func(Owner, adminsock.Request, controlbudget.Deadline) error
}

func (r *LocalStoreReload[Owner]) HandleCluster(req adminsock.Request) adminsock.Response {
	if req.Op != adminsock.OpBrokerUpgradeReload {
		if r.Cluster != nil {
			return r.Cluster.HandleCluster(req)
		}
		return adminsock.Response{Op: req.Op, Code: adminsock.CodeClusterNotEnabled, Error: "cluster mode is not enabled"}
	}
	if err := ValidateStoreRun(r.Image); err != nil {
		return adminsock.Response{Op: req.Op, Code: adminsock.CodeUpgradeDeferred, Error: err.Error(), BrokerImage: r.Status(r.Owner)}
	}
	(*r.Gate).Lock()
	defer (*r.Gate).Unlock()
	if r.Signal.Requested() {
		return adminsock.Response{Op: req.Op, Code: adminsock.CodeUpgradeDeferred, Error: "broker image cut is already armed", BrokerImage: r.Status(r.Owner)}
	}
	refuse := func(code, message string) adminsock.Response {
		r.Resume(r.Owner)
		return adminsock.Response{Op: req.Op, Code: code, Error: message, BrokerImage: r.Status(r.Owner)}
	}
	// origin: hot-update S6 internal review rv-ctl-11 / T45
	// Creation handlers have no durable tracker yet. Join briefly so their
	// busy refusal reaches the caller before its five-second RPC expires.
	deadline, err := controlbudget.After(100 * time.Millisecond)
	if err != nil {
		return adminsock.Response{Op: req.Op, Code: adminsock.CodeStoreError, Error: err.Error()}
	}
	count, err := r.Fence(r.Owner, deadline)
	if err != nil {
		if controlbudget.IsExpired(err) {
			return refuse(adminsock.CodeUpgradeDeferred, "transfers_in_flight")
		}
		return refuse(adminsock.CodeStoreError, "cannot establish transfer census")
	}
	if count != 0 {
		return refuse(adminsock.CodeUpgradeDeferred, "transfers_in_flight")
	}
	// origin: S8 internal review rv3-leaves F2
	// An old reload cannot establish grant/reader/authority through callbacks.
	if policy := brokerupgrade.LegacyReloadResponse(req, nil); !policy.OK {
		return refuse(policy.Code, policy.Error)
	}
	// Role checks and the arm use their own bounded admission after the join.
	deadline, err = NewStoreCutDeadline()
	if err != nil {
		return refuse(adminsock.CodeStoreError, err.Error())
	}
	running, _ := RunningArtifactSHA256()
	if r.Admission != nil && !ReloadAlreadyRunning(req.ToVersion, req.ExpectSHA256, running) {
		if err := r.Admission(r.Owner, req, deadline); err != nil {
			code := adminsock.CodeBadRequest
			if errors.Is(err, ErrReloadLeader) {
				code = adminsock.CodeIsLeader
			}
			return refuse(code, err.Error())
		}
	}
	var armErr error
	resp := HandleStoreReloadRequest(req, r.Node, func() { armErr = r.Signal.Arm(deadline) })
	if armErr != nil {
		// Run retains ownership of an in-progress cut after an uncertain reply.
		// The caller must query state; this is not permission to send another cut.
		resp.OK, resp.Reloading = false, false
		resp.Code, resp.Error = adminsock.CodeStoreError, armErr.Error()
	}
	if !resp.Reloading && !r.Signal.Requested() {
		r.Resume(r.Owner)
	}
	resp.BrokerImage = r.Status(r.Owner)
	if resp.Reloading {
		resp.NextGeneration = resp.BrokerImage.Generation + 1
	}
	return resp
}

// origin: hot-update S6 plan §5 / T01
func HandleStoreReloadRequest(req adminsock.Request, node *cluster.Node, request func()) adminsock.Response {
	op := req.Op
	if req.ExpectSHA256 == "" {
		return adminsock.Response{Op: op, Code: adminsock.CodeBadRequest, Error: "reload requires expect_sha256"}
	}
	got, err := StagedArtifactSHA256()
	if err != nil {
		return adminsock.Response{Op: op, Code: adminsock.CodeStoreError, Error: "cannot hash staged executable"}
	}
	if !strings.EqualFold(got, req.ExpectSHA256) {
		return adminsock.Response{Op: op, Code: adminsock.CodeShaMismatch, Error: "staged executable sha256 does not match the expected image"}
	}
	// origin: S8 internal review rv3-leaves F2
	// Guard the historical cluster backend as well as local/signed adapters.
	if policy := brokerupgrade.LegacyReloadResponse(req, nil); !policy.OK {
		return policy
	}
	running, err := RunningArtifactSHA256()
	if err != nil {
		return adminsock.Response{Op: op, Code: adminsock.CodeStoreError, Error: "cannot hash running executable"}
	}
	if ReloadAlreadyRunning(req.ToVersion, req.ExpectSHA256, running) {
		return adminsock.Response{Op: op, OK: true, AlreadyAtVersion: true}
	}
	if node != nil {
		if err := CheckReloadRole(node); err != nil {
			code := adminsock.CodeStoreError
			if errors.Is(err, ErrReloadLeader) {
				code = adminsock.CodeIsLeader
			}
			return adminsock.Response{Op: op, Code: code, Error: err.Error()}
		}
		ops, err := cluster.NonTerminalOperations(node.RODB())
		if err != nil {
			return adminsock.Response{Op: op, Code: adminsock.CodeStoreError, Error: "cannot read cluster operation census"}
		}
		if len(ops) != 0 {
			return adminsock.Response{Op: op, Code: adminsock.CodeUpgradeDeferred, Error: "cluster operations are in flight"}
		}
	}
	if request == nil {
		return adminsock.Response{Op: op, Code: adminsock.CodeClusterNotEnabled, Error: "broker image reload is not wired"}
	}
	// This only queues Run. DrainArmed proves the actual Unix/NATS reply sender
	// completed before transport stop; no sleep is used as a delivery receipt.
	request()
	return adminsock.Response{Op: op, OK: true, Reloading: true}
}

func ReloadAlreadyRunning(version, expected, running string) bool {
	return version != "" && proto.SameRelease(proto.ReleaseVersion, version) && strings.EqualFold(expected, running)
}

var ErrReloadLeader = brokerupgrade.ErrLegacyReloadLeader

func CheckReloadRole(n *cluster.Node) error {
	return brokerupgrade.CheckLegacyReloadRole(n)
}

// Deadline retains the request's absolute budget through the Run cut. A direct
// legacy arming call has no prior budget and establishes one at Run admission.
func (s *StoreReloadSignal) Deadline() (controlbudget.Deadline, error) {
	s.mu.Lock()
	deadline := s.deadline
	s.mu.Unlock()
	if deadline == 0 {
		return NewStoreCutDeadline()
	}
	return deadline, nil
}

// ColdCleanup retains startup cleanup until InstallCurrent transfers terminal
// ownership to RuntimeSlot. Once managed, captured cold roots cannot stop a new
// generation installed after an exec error.
// origin: hot-update S6 plan §4.2 / T02
func ColdCleanup(managed *bool, stop func() error) {
	if !*managed {
		_ = stop()
	}
}

// ColdCleanupAction applies the same current-owner fence to cleanup operations
// without an error result, such as the tunnel server's terminal Close.
// origin: hot-update S6 plan §4.2 / T02
func ColdCleanupAction(managed *bool, stop func()) {
	if !*managed {
		stop()
	}
}

// RestoredStore is set only by validated broker Role dispatch, never argv/hints.
// RunRestoredStore activates the imported current graph and its process roots;
// it must never enter the cold constructor/migration/reconcile path.
// origin: hot-update S6 plan §4.1 / §6.3 / §12.1 P11
func RestoredStore[Owner any](r *StoreRun[Owner]) bool { return r != nil && r.Restored }
func RunRestoredStore[Owner any](r *StoreRun[Owner], owner Owner) error {
	if err := ValidateStoreRun(r); err != nil {
		return err
	}
	if r.RunRestored == nil {
		return fmt.Errorf("brokerimage: restored control roots are not installed")
	}
	if err := r.InstallCurrent(owner); err != nil {
		return err
	}
	return r.RunRestored(owner)
}

// prepareStoreShutdown keeps reversible admission guards ahead of the writer's
// closing marker. Its caller retains phase-aware recovery for every refusal.
// origin: hot-update S6 plan §3.2 / §4.2; S10 LINT-L4 responsibility split
func prepareStoreShutdown[Owner any](b Owner, shutdown StoreShutdown, deadline controlbudget.Deadline, callbacks StoreRunCallbacks[Owner]) error {
	var err error
	if callbacks.BeforeShutdown != nil {
		if err := callbacks.BeforeShutdown(b); err != nil {
			return err
		}
	}
	// Both admission checks and an early PrepareNodeShutdown refusal leave the
	// original writer untouched. Thaw owns their reversible fences.
	// origin: S6 internal review rv-final F1
	if shutdown.Prepare != nil {
		err = shutdown.Prepare(deadline)
	}
	if err == nil && shutdown.AfterPrepare != nil {
		err = shutdown.AfterPrepare()
	}
	if err != nil {
		return err
	}
	return nil
}

// closeStoreShutdown preserves actual cluster close phases on a partial stop.
// Unknown phases must never be mistaken for an untouched writer at recovery.
// origin: hot-update S6 plan §3.2 / §4.2; S10 LINT-L4 responsibility split
func closeStoreShutdown(store StoreOwner, shutdown StoreShutdown) (storage.StoreSeal, error) {
	// A shutdown failure has no closed receipt. Preserve real cluster phases
	// when available, otherwise report unknown rather than all-CloseOpen zeros.
	// origin: hot-update S6 plan §3.2 / §4.2; internal review rv-writer-6
	seal := unknownStoreCloseSeal(store)
	if shutdown.Close != nil {
		if err := shutdown.Close(); err != nil {
			if node, nodeErr := ClusterStoreNode(store); nodeErr == nil {
				seal, _ = cluster.ClosedImageStore(node)
			}
			return seal, err
		}
	}
	return seal, nil
}

// validateStoreRunCallbacks refuses incomplete wiring before cut admission.
// origin: hot-update S6 plan §3.2; S10 LINT-L4 responsibility split
func validateStoreRunCallbacks[Owner any](callbacks StoreRunCallbacks[Owner]) error {
	if callbacks.Deadline == nil || callbacks.Phase == nil || callbacks.Resume == nil || callbacks.Bind == nil || callbacks.Armed == nil || callbacks.Exec == nil {
		return fmt.Errorf("brokerimage: store Run callbacks are incomplete")
	}
	return nil
}
