// Package infraroot composes the privileged infrastructure leaves. Business
// readiness and transaction drivers stay outside this executable's dependency
// closure; a complete binding table is a prerequisite, not a capability receipt.
// origin: hot-update S9 plan §2.1 / §6 / §10.3 / T91
package infraroot

import (
	"crypto/sha256"
	"errors"
	"io"
	"net"
	"os"
	"reflect"
	"runtime"
	"sync"
	"time"

	"github.com/LinZiyang666/tether/internal/controlbudget"
	c "github.com/LinZiyang666/tether/internal/infracontract"
	"github.com/LinZiyang666/tether/internal/infrahost"
	"github.com/LinZiyang666/tether/internal/infralaunch"
	"github.com/LinZiyang666/tether/internal/infrapriv"
	"github.com/LinZiyang666/tether/internal/ingressswitch"
)

// Dependencies are local, trusted composition inputs, never decoded from IPC.
// Their record providers resolve immutable digests under the current domain;
// caller paths, argv and business dependencies have no place in this boundary.
// The journal and worker-limit directories must already exist after bootstrap.
type Dependencies struct {
	Policy       infrapriv.Policy
	JournalPath  string
	Scope        infrahost.CensusScope
	Source       infrahost.ArtifactSource
	Facts        infrahost.HostFacts
	Manager      infrahost.UnitManager
	UnitState    infrahost.UnitStateStore
	UnitPolicies []infrahost.UnitPolicy
	Ingress      ingressswitch.Backend
	Changes      ingressswitch.Records
	Evidence     ingressswitch.Evidence
	Storage      infrahost.ACMEConfig
	// StorageRevision resolves the durable FINAL selector, not a warm copy.
	StorageRevision func(c.HostRequest, c.Deadline) (uint64, error)
	Launch          infralaunch.Bindings
	ResolvePolicy   func(c.Digest, c.Deadline, c.Stop) (infrapriv.Policy, error)
	PersistPolicy   func(infrapriv.Policy, infrapriv.Policy, c.Deadline, c.Stop) error
	Files           infrapriv.FileExchange
	Bootstrap       infrahost.BootstrapSetup
}

// Root owns its worker and all leaf resources created during composition.
// Sources, record providers, supplied directory FDs and WorkerLimit are borrowed.
// Closing the worker joins admitted calls before releasing custody or pidfds.
type Root struct {
	host       c.Host
	journal    *infrahost.Journal
	policy     *infrapriv.PolicyStore
	authority  *peerAuthority
	launch     *infralaunch.Backend
	storage    *infrahost.ACMEStorage
	files      infrapriv.FileExchange
	bindings   c.HostBindings
	once       sync.Once
	closeErr   error
	mu         sync.Mutex
	closed     bool
	serving    sync.WaitGroup
	stop       chan struct{}
	bootstrap  infrahost.BootstrapDependencies
	initialize func(c.Deadline, c.Stop) (infrahost.BootstrapReceipt, error)
	owned      []io.Closer
}

func supported() error {
	if runtime.GOOS != "linux" || runtime.GOARCH != "amd64" {
		return c.ErrUnsupported
	}
	return nil
}

func missing(v any) bool {
	if v == nil {
		return true
	}
	r := reflect.ValueOf(v)
	switch r.Kind() {
	case reflect.Pointer, reflect.Func, reflect.Map, reflect.Chan, reflect.Slice, reflect.Interface:
		return r.IsNil()
	default:
		return false
	}
}

// New creates every backend itself. It accepts no HostBindings from a caller:
// swapping a real effect owner for a compile stub cannot reach NewWorker.
// Construction does not start a unit, change nft rules, or advertise readiness.
// origin: hot-update S9 plan §2.1 / §10.3 / T91
func New(d Dependencies) (_ *Root, resultErr error) {
	if err := supported(); err != nil {
		return nil, err
	}
	if d.JournalPath == "" || d.Scope.Domain != d.Policy.Domain || missing(d.Source) || missing(d.Facts) ||
		missing(d.Manager) || missing(d.UnitState) || len(d.UnitPolicies) == 0 || missing(d.Ingress) ||
		missing(d.Changes) || missing(d.Evidence) || d.StorageRevision == nil ||
		d.ResolvePolicy == nil || d.PersistPolicy == nil || missing(d.Files) {
		return nil, c.ErrIncomplete
	}
	p, err := infrapriv.NewPolicyStore(d.Policy)
	if err != nil {
		return nil, err
	}
	r := &Root{policy: p, files: d.Files, stop: make(chan struct{}), authority: &peerAuthority{policy: p, peers: make(map[c.Digest]c.Peer)}}
	defer func() {
		if resultErr != nil {
			_ = r.Close()
		}
	}()
	r.journal, err = infrahost.OpenJournal(d.JournalPath, d.Policy.HostBootID, d.Policy.Domain, r.authority)
	if err != nil {
		return nil, err
	}
	physical, err := infrahost.NewIdentityBackend(d.Scope)
	if err != nil {
		return nil, err
	}
	r.bootstrap = infrahost.BootstrapDependencies{Manager: d.Manager, Setup: d.Bootstrap, Physical: physical, Policies: d.UnitPolicies}
	workerBackend, err := infrahost.NewBackend(r.journal, d.Source, d.Facts)
	if err != nil {
		return nil, err
	}
	r.storage, err = infrahost.NewACMEStorage(d.Storage)
	if err != nil {
		return nil, err
	}
	units, err := infrahost.NewUnitHost(infrahost.UnitDependencies{
		Manager: d.Manager, State: d.UnitState, Physical: physical, Policies: d.UnitPolicies,
		Retire: retireWarm,
	})
	if err != nil {
		return nil, err
	}
	unitStorage := &storageUnits{units, r.storage, d.StorageRevision, d.UnitState}
	ingress, err := ingressswitch.New(d.Ingress, d.Changes, d.Evidence)
	if err != nil {
		return nil, err
	}
	r.launch, err = infralaunch.NewBackend(d.Launch)
	if err != nil {
		return nil, err
	}
	policy := &infrapriv.PolicyBackend{Store: p, Resolve: d.ResolvePolicy, Persist: d.PersistPolicy}
	// Each slot names precisely one owner. Per-operation guards prevent a
	// miswired slot from falling through a different owner's permissive handler.
	// origin: hot-update S9 plan §10.3 / T91
	r.bindings = c.HostBindings{
		c.Inspect: physical, c.Stage: workerBackend, c.FenceStart: unitStorage,
		c.Promote: workerBackend, c.SignalLDM: physical, c.SignalStop: physical,
		c.SignalGraceful: physical, c.StartWarm: unitStorage, c.SwitchIngress: ingress,
		c.FinalizeStorage: r.storage, c.ReleaseStart: unitStorage, c.RetireWarm: unitStorage,
		c.RecoverHost: workerBackend, c.RotatePolicy: policy, c.UpdateSupport: workerBackend,
		c.LaunchClaim: r.launch, c.LaunchAck: r.launch,
		c.VerifyIngress: &ingressReader{ingress, d.Facts},
	}
	for op := c.Inspect; op <= c.VerifyIngress; op++ {
		if missing(r.bindings[op]) {
			return nil, c.ErrContractStub
		}
		r.bindings[op] = &operation{op, r.bindings[op], r.journal, d.Facts}
	}
	r.host, err = infrahost.NewWorker(r.bindings)
	if err != nil {
		return nil, err
	}
	return r, nil
}

type operation struct {
	op      c.HostOp
	backend c.Host
	journal *infrahost.Journal
	facts   infrahost.HostFacts
}

func (h *operation) HostJournal() *infrahost.Journal                 { return h.journal }
func (h *operation) Inspect(id c.ID, d c.Deadline) (c.Census, error) { return h.backend.Inspect(id, d) }
func (h *operation) Do(r c.HostRequest, d c.Deadline, stop c.Stop) (c.HostReceipt, error) {
	if r.Op != h.op {
		return c.HostReceipt{}, infrahost.Refuse(c.ErrUnsupported)
	}
	state, _, err := h.journal.Read(d)
	if err != nil {
		return c.HostReceipt{}, infrahost.Refuse(err)
	}
	// P03 authenticates the envelope; physical/config/census digests still need
	// root-owned resolution at the actual effect boundary for EVERY backend.
	// origin: hot-update S9 plan §6.2 / T91
	if err := h.facts.Validate(r, state, d, stop); err != nil {
		return c.HostReceipt{}, infrahost.Refuse(err)
	}
	return h.backend.Do(r, d, stop)
}

// RetireWarm cannot rewrite the signed request into SignalGraceful or stop a
// unit by name. P05 has checked the current zero-responsibility return receipt;
// P02 pins exactly that incarnation again for the final graceful signal.
// origin: hot-update S9 plan §10.3 / T91
func retireWarm(r c.HostRequest, d c.Deadline, stop c.Stop) (c.HostReceipt, error) {
	if r.Op != c.RetireWarm || r.Component != c.Caddy {
		return c.HostReceipt{}, c.ErrUnsupported
	}
	pin, err := infrahost.PinProcess(r.Expected, d)
	if err != nil {
		return c.HostReceipt{}, err
	}
	defer func() { _ = pin.Close() }()
	if err = pin.Signal(c.SignalGraceful, d, stop); err != nil {
		return c.HostReceipt{}, err
	}
	return c.HostReceipt{Applied: true, Current: r.Expected}, nil
}

type storageUnits struct {
	units    c.Host
	storage  *infrahost.ACMEStorage
	revision func(c.HostRequest, c.Deadline) (uint64, error)
	state    infrahost.UnitStateStore
}

func (h *storageUnits) Inspect(id c.ID, d c.Deadline) (c.Census, error) {
	return h.units.Inspect(id, d)
}
func (h *storageUnits) Do(r c.HostRequest, d c.Deadline, stop c.Stop) (c.HostReceipt, error) {
	if r.Component == c.Caddy {
		switch r.Op {
		case c.FenceStart:
			// Persist the ACME fence before any Restart=always launch can race the
			// unit retrofit; a later error leaves both release paths fail closed.
			if _, err := infrahost.FenceACME(h.storage, r, d, stop); err != nil {
				return c.HostReceipt{}, err
			}
		case c.ReleaseStart:
			// Validate P05's still-closed permit before opening P09's writer
			// fence. Opening storage first would let an invalid unit-state CAS
			// expose a writer even though ReleaseStart subsequently refused.
			before, err := h.state.Load(r.Component, d)
			if err != nil {
				return c.HostReceipt{}, err
			}
			if _, err := infrahost.ReleaseStartup(before, r); err != nil {
				return c.HostReceipt{}, err
			}
			revision, err := h.revision(r, d)
			if err != nil {
				return c.HostReceipt{}, err
			}
			if _, err = infrahost.ReleaseACMERequest(h.storage, r, revision, d, stop); err != nil {
				return c.HostReceipt{}, err
			}
		case c.Inspect, c.Stage, c.Promote, c.SignalLDM, c.SignalStop, c.SignalGraceful,
			c.StartWarm, c.SwitchIngress, c.FinalizeStorage, c.RetireWarm, c.RecoverHost,
			c.RotatePolicy, c.UpdateSupport, c.LaunchClaim, c.LaunchAck, c.VerifyIngress:
			// These operations do not change the ACME startup fence.
		}
	}
	return h.units.Do(r, d, stop)
}

func (r *Root) Close() error {
	r.once.Do(func() {
		r.mu.Lock()
		r.closed = true
		close(r.stop)
		r.mu.Unlock()
		if r.host != nil {
			closer := r.host.(io.Closer) //nolint:forcetypeassert // New sets host only to the concrete Worker, which always implements io.Closer.
			r.closeErr = errors.Join(r.closeErr, closer.Close())
		}
		// Socket readers can still be in P03 admission while Host has no active
		// call. Join them too before releasing the authority and leaf resources.
		r.serving.Wait()
		if r.launch != nil {
			r.closeErr = errors.Join(r.closeErr, r.launch.Close())
		}
		if r.storage != nil {
			r.closeErr = errors.Join(r.closeErr, r.storage.Close())
		}
		if r.journal != nil {
			r.closeErr = errors.Join(r.closeErr, r.journal.Close())
		}
		for _, owner := range r.owned {
			r.closeErr = errors.Join(r.closeErr, owner.Close())
		}
	})
	return r.closeErr
}

// peerAuthority is the adapter between P03's kernel peer and P04's short CAS.
// A signature from another local process cannot bypass socket admission merely
// because it possesses a key of the same UID. Entries live for one connection.
type peerAuthority struct {
	mu     sync.Mutex
	policy *infrapriv.PolicyStore
	peers  map[c.Digest]c.Peer
}

func requestDigest(r c.HostRequest) (c.Digest, error) {
	data, err := c.EncodeRequest(r)
	if err != nil {
		return c.Digest{}, err
	}
	return sha256.Sum256(data), nil
}

func (a *peerAuthority) Authorize(r c.HostRequest, d c.Deadline) (c.Peer, error) {
	if _, err := controlbudget.Remaining(d); err != nil {
		return c.Peer{}, err
	}
	digest, err := requestDigest(r)
	if err != nil {
		return c.Peer{}, err
	}
	a.mu.Lock()
	peer, present := a.peers[digest]
	a.mu.Unlock()
	if !present {
		return c.Peer{}, infrapriv.ErrPermission
	}
	if err := a.policy.Authorize(r, peer); err != nil {
		return c.Peer{}, err
	}
	return peer, nil
}

type requestFiles struct {
	root       *Root
	digest     c.Digest
	registered bool
	transfer   *infralaunch.Prepared
	claim      c.HostRequest
}

func (f *requestFiles) Accept(r c.HostRequest, peer c.Peer, files []*os.File, d c.Deadline, stop c.Stop) error {
	if f.registered {
		return infrapriv.ErrBusy
	}
	if err := f.root.policy.Authorize(r, peer); err != nil {
		return err
	}
	digest, err := requestDigest(r)
	if err != nil {
		return err
	}
	f.root.authority.mu.Lock()
	if _, exists := f.root.authority.peers[digest]; exists {
		f.root.authority.mu.Unlock()
		return infrapriv.ErrBusy
	}
	f.root.authority.peers[digest] = peer
	f.root.authority.mu.Unlock()
	f.digest, f.registered = digest, true
	return f.root.files.Accept(r, peer, files, d, stop)
}

func (f *requestFiles) Reply(r c.HostRequest, receipt c.HostReceipt, peer c.Peer, d c.Deadline, stop c.Stop) ([]*os.File, error) {
	if !f.registered {
		return nil, infrapriv.ErrPermission
	}
	if r.Op == c.LaunchClaim {
		transfer, err := f.root.launch.Transfer(r)
		if err != nil {
			return nil, err
		}
		f.transfer, f.claim = transfer, r
		return transfer.Files()
	}
	return f.root.files.Reply(r, receipt, peer, d, stop)
}

// The read-only offer resolver runs only after P03 authenticates the launch
// query and binds its process lifetime to SO_PEERCRED. Claim still uses the
// ordinary signed HostRequest and common journal CAS.
// origin: S9 internal review rv-infra F03
func (f *requestFiles) Offer(q infrapriv.LaunchQuery, peer c.Peer, d c.Deadline, stop c.Stop) (c.HostRequest, error) {
	offers, ok := f.root.files.(infrapriv.LaunchOffers)
	if !ok {
		return c.HostRequest{}, c.ErrIncomplete
	}
	return offers.Offer(q, peer, d, stop)
}

func (f *requestFiles) close() {
	if f.transfer != nil {
		_ = f.transfer.Close()
	}
	if f.registered {
		f.root.authority.mu.Lock()
		delete(f.root.authority.peers, f.digest)
		f.root.authority.mu.Unlock()
	}
}

// ServeConn is synchronous: the shared IPC server owns closing the accepted
// socket and interrupting blocked I/O, while Close joins the shared host worker.
// The limit is a root-owned permanent OFD limiter shared by Accept=yes processes.
func (r *Root) ServeConn(conn *net.UnixConn, limit *infrapriv.WorkerLimit, d c.Deadline, stop c.Stop) error {
	r.mu.Lock()
	if r.closed {
		r.mu.Unlock()
		if conn != nil {
			_ = conn.Close()
		}
		return infrahost.ErrStopped
	}
	r.serving.Add(1)
	r.mu.Unlock()
	defer r.serving.Done()
	cancel, done, joined := make(chan struct{}), make(chan struct{}), make(chan struct{})
	go func() {
		defer close(joined)
		select {
		case <-r.stop:
		case <-stop:
		case <-done:
			return
		}
		close(cancel)
	}()
	defer func() { close(done); <-joined }()
	files := &requestFiles{root: r}
	defer files.close()
	server, err := infrapriv.NewServer(r.policy, r.host, limit, files)
	if err != nil {
		if conn != nil {
			_ = conn.Close()
		}
		return err
	}
	err = server.ServeConn(conn, d, cancel)
	if files.transfer != nil {
		// Accept=yes gives each request its own process. Keep this worker's
		// last reference after the reply until another worker durably observes
		// Ack, or the original bounded custody interval ends in fenced recovery.
		// origin: S9 internal review rv-infra F03 / F04
		ackErr := awaitLaunchAck(r.journal, files.claim, d, cancel)
		if err == nil {
			err = ackErr
		}
	}
	return err
}

func awaitLaunchAck(journal *infrahost.Journal, claim c.HostRequest, d c.Deadline, stop c.Stop) error {
	query := claim
	query.Op = c.LaunchAck
	for {
		select {
		case <-stop:
			return infrahost.ErrStopped
		default:
		}
		if _, err := controlbudget.Remaining(d); err != nil {
			return err
		}
		event, err := journal.OperationRecord(query, d)
		if err == nil && event.Phase == infrahost.EffectApplied && event.Request.Payload == claim.Payload && event.Request.Sequence > claim.Sequence {
			return nil
		}
		timer := time.NewTimer(5 * time.Millisecond)
		select {
		case <-stop:
			timer.Stop()
			return infrahost.ErrStopped
		case <-timer.C:
		}
	}
}

// Bootstrap calls the real management-only P05 state machine. It cannot be
// inferred from a constructed worker or performed through a socket HostOp.
func (r *Root) Bootstrap(d c.Deadline, stop c.Stop) (infrahost.BootstrapReceipt, error) {
	if r.initialize != nil {
		return r.initialize(d, stop)
	}
	if missing(r.bootstrap.Setup) {
		return infrahost.BootstrapReceipt{}, c.ErrIncomplete
	}
	return infrahost.Bootstrap(r.bootstrap, d, stop)
}

// OpenInstalled opens only an explicitly enabled installation. Fresh bootstrap
// has its own root-only constructor, so no missing policy/journal can prevent
// reaching initialization, and no worker can silently initialize authority.
// It refuses before effects unless all installed root record providers are
// available, preventing partial bindings from advertising hot-update capability.
// origin: hot-update S9 plan §2.1 / §6.4 / T91
// origin: S9 internal review rv-infra F01
func OpenInstalled() (*Root, error) { return openInstalled(false) }

// OpenBootstrap constructs the root-only management initializer without opening
// a runtime journal that explicit fresh initialization has yet to create.
func OpenBootstrap() (*Root, error) { return openInstalled(true) }
