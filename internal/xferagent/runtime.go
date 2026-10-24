// Package xferagent composes the production agent-side continuation worker:
// the agent→broker transport on the agent's authenticated subtree, dedicated
// data attachments, the sender/receiver/courier factories and the broker→agent
// link handler. It is a leaf below internal/agent: the agent supplies getters
// for its CURRENT session, so a control-session rebuild replaces only the
// attachment while the worker and its actors keep their lifetime.
// origin: hot-update S7 plan §4 / §8.1 / P20 / P21
package xferagent

import (
	"errors"
	"log/slog"
	"sync"
	"time"

	"github.com/LinZiyang666/tether/internal/controlbudget"
	"github.com/LinZiyang666/tether/internal/drillobserve"
	"github.com/LinZiyang666/tether/internal/procrequest"
	"github.com/LinZiyang666/tether/internal/xfercapacity"
	x "github.com/LinZiyang666/tether/internal/xfercontract"
	"github.com/nats-io/nats.go/jetstream"
	"github.com/nats-io/nkeys"
)

// Host is the agent's current runtime view. Every getter resolves the session
// at call time; nothing here captures a *nats.Conn for the worker's lifetime.
type Host struct {
	SID       string
	NID       func() string
	Instance  func() string
	JetStream func() jetstream.JetStream
	Roots     []string
	Mode      int
	Logger    *slog.Logger
	// Admit reserves first creation against the agent's image cut (nil
	// admits); its release runs when the prepare callback returns.
	Admit func() (func(), bool)
}

// Runtime is the per-process composition owner shared by every transfer entry.
// The worker is bound after construction so link controls can reach it.
type Runtime struct {
	host     Host
	boot     string
	capacity *xfercapacity.Account
	receiver nkeys.KeyPair // ephemeral settlement key when the agent has no nkey

	mu       sync.Mutex
	worker   x.WorkerManager
	intents  map[x.Key]x.Intent
	data     map[x.Key]*attachment
	sources  map[x.Key]x.Request // last source seal per entry, replayed for reconcile
	canons   map[x.Key]x.CanonicalRecord
	revoked  map[string]bool // sid tombstones observed by the agent
	versions func() string
	image    imageOwner
	// Tier A pull senders by entry, created or imported here, so a prepare
	// (and its replay) can return the sender's retained copy. An entry is
	// forgotten when its sender stops; it never outlives that owner.
	// origin: hot-update S7 plan §8.1 / FIX-INLINE
	inline map[x.Key]*liveSender
}

// New refuses on a platform without a boot identity: a worker whose deadlines
// cannot be compared across a same-PID exec must not advertise transfer-v1.
func New(h Host) (*Runtime, error) {
	if h.SID == "" || h.NID == nil || h.Instance == nil || h.JetStream == nil {
		return nil, errors.New("xferagent: incomplete agent host")
	}
	boot, err := procrequest.CurrentBootID()
	if err != nil || boot == "" {
		return nil, errors.Join(errors.New("xferagent: boot identity unavailable"), err)
	}
	key, err := nkeys.CreateUser()
	if err != nil {
		return nil, err
	}
	if h.Logger == nil {
		h.Logger = slog.Default()
	}
	return &Runtime{host: h, boot: boot, capacity: xfercapacity.New("xfer-agent-"), receiver: key,
		intents: map[x.Key]x.Intent{}, data: map[x.Key]*attachment{}, sources: map[x.Key]x.Request{},
		canons: map[x.Key]x.CanonicalRecord{}, revoked: map[string]bool{}}, nil
}

// WorkerConfig returns the frozen P00 worker configuration with the real
// ports. Identity is completed by the agent's handler owner.
func (r *Runtime) WorkerConfig() x.WorkerConfig {
	return x.WorkerConfig{Base: r.base(), Identity: x.Identity{SID: r.host.SID}, Authority: r, Transport: r,
		MakeSender: r.makeSender, MakeReceiver: r.makeReceiver, MakeCourier: r.makeCourier}
}

// Bind records the worker built from WorkerConfig (or imported across an image
// cut). version reports the handler's current advertised capability.
func (r *Runtime) Bind(worker x.WorkerManager, version func() string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.worker, r.versions = worker, version
}

func (r *Runtime) boundWorker() (x.WorkerManager, string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	version := ""
	if r.versions != nil {
		version = r.versions()
	}
	return r.worker, version
}

func (r *Runtime) base() x.Base {
	boot := r.boot
	return x.Base{Clock: func() (string, int64, error) {
		now, err := controlbudget.Now()
		return boot, now, err
	}, UTC: time.Now, Capacity: r.capacity, Observe: transferObserver(r)}
}

// Leaves share the actual process generation, including constructor receipts.
// Permit origin generations describe routing authority, not this image reader.
// origin: S7 internal review INTEG-O F5
func transferObserver(r *Runtime) func(x.Observation) {
	if drillobserve.Hook == nil {
		return nil
	}
	return func(event x.Observation) {
		r.mu.Lock()
		generation := max(r.image.generation, uint64(1))
		r.mu.Unlock()
		event.Generation = 0
		drillobserve.ProcessTransferObserver[x.Observation](generation, "agent")(event)
	}
}

// Check is the agent half of authority. The broker authorizes every
// operation; the agent additionally refuses continuation for a session whose
// deletion it has observed, so a revoked owner cannot keep writing bytes.
// origin: hot-update S7 plan §6.4
func (r *Runtime) Check(id x.Identity, access x.AccessKind, _ x.Deadline) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.revoked[id.SID] && access != x.AccessSettle && access != x.AccessStatus {
		return errors.New(x.CodeSessionDeleting)
	}
	return nil
}

// Revoke records a session tombstone observed on the agent's own event intake.
func (r *Runtime) Revoke(sid string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.revoked[sid] = true
}

func (r *Runtime) recordIntent(id x.Identity, intent x.Intent) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.intents[x.Key{SID: id.SID, TransferID: id.TransferID}] = intent
}

func (r *Runtime) intent(id x.Identity) (x.Intent, bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	intent, ok := r.intents[x.Key{SID: id.SID, TransferID: id.TransferID}]
	return intent, ok
}
