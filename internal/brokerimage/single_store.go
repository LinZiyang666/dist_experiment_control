package brokerimage

import (
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"errors"
	"fmt"
	"github.com/LinZiyang666/tether/internal/cluster"
	"github.com/LinZiyang666/tether/internal/controlbudget"
	"github.com/LinZiyang666/tether/internal/handoff"
	"github.com/LinZiyang666/tether/internal/rawsocket"
	"github.com/LinZiyang666/tether/internal/storage"
	"io"
	"net/url"
	"os"
	"runtime"
	"strings"
	"sync"
)

// SingleStoreLock is the method projection of P04's original DataDirLock. It
// does not open, flock, unlock or duplicate anything: those operations have one
// owner, shared with cluster and offline tools. In particular the S5 release
// closure is unsuitable here because it explicitly LOCK_UNs the inherited OFD.
// origin: hot-update S6 plan §3.1 / §12.1 P11
type SingleStoreLock interface {
	Export() (handoff.Resource, error)
	Release() error
}

// SingleStoreSupport binds the P03/P04 implementation at startup. Resolve proves
// canonical DB/DataDir/lock binding before either the lock or SQLite is opened.
// Close returns actual driver/checkpoint phases; an error is never converted to
// an anticipated closed receipt. Quiesce and Resume are paired so a partial
// fence can be reversed before auth or business uses the original pool again.
// The adapter adds no storage→cluster import.
// origin: hot-update S6 plan §2.1 / §3.2 / §12.1 P11
type SingleStoreSupport struct {
	Resolve       func(string, string) (storage.DBIdentity, error)
	AcquireLock   func(string) (SingleStoreLock, error)
	OpenCold      func(SingleStoreConfig) (*sql.DB, storage.StoreStamp, error)
	ReadStamp     func(*sql.DB, storage.DBIdentity) (storage.StoreStamp, error)
	Quiesce       func(*sql.DB, controlbudget.Deadline) error
	Resume        func(*sql.DB) error
	CloseForImage func(*sql.DB, storage.StoreStamp, storage.DriverRegistry, controlbudget.Deadline) (storage.StoreSeal, error)
	OpenCurrent   storage.OpenCurrentFunc
}

var singleStoreSupport struct {
	sync.RWMutex
	support *SingleStoreSupport
}

// InstallSingleStoreSupport is a one-time binding, performed before cold serve
// opens a single store. A second binding could change lock domains or swap driver
// registries under a live owner, so it is refused rather than hot-reconfigured.
func InstallSingleStoreSupport(s SingleStoreSupport) error {
	if s.Resolve == nil || s.AcquireLock == nil || s.OpenCold == nil || s.ReadStamp == nil ||
		s.Quiesce == nil || s.Resume == nil || s.CloseForImage == nil || s.OpenCurrent == nil {
		return fmt.Errorf("brokerimage: single store adapters are incomplete")
	}
	singleStoreSupport.Lock()
	defer singleStoreSupport.Unlock()
	if singleStoreSupport.support != nil {
		return fmt.Errorf("brokerimage: single store adapters already installed")
	}
	singleStoreSupport.support = &s
	return nil
}

// OpenSingleStore acquires the canonical lock BEFORE cold Open. A nonnil partial
// DB remains owned by the returned owner even on error: callers must close it,
// and must not retry by opening another pool. Missing P03/P04 support is explicit.
func OpenSingleStore(cfg SingleStoreConfig) (StoreOwner, *sql.DB, error) {
	singleStoreSupport.RLock()
	support := singleStoreSupport.support
	singleStoreSupport.RUnlock()
	if support == nil {
		return nil, nil, fmt.Errorf("brokerimage: single store support not installed (pending S6 P03/P04)")
	}
	if cfg.Registry == nil {
		return nil, nil, fmt.Errorf("brokerimage: single store driver registry is required")
	}
	// In-memory cold stores have no file or flock to inherit. Keep the advertised
	// ephemeral mode, but refuse any image operation before fencing or closing it.
	// origin: hot-update S6 plan §3.1; internal review rv-writer-8/12 and rv-fd-9
	if singleMemoryStore(cfg.DBPath) {
		db, _, err := support.OpenCold(cfg)
		owner := &singleStoreOwner{support: support, registry: cfg.Registry, db: db,
			stamp:           storage.StoreStamp{Identity: storage.DBIdentity{Path: cfg.DBPath}},
			imageIneligible: errors.New("brokerimage: in-memory store is image-ineligible")}
		return owner, db, err
	}
	identity, err := support.Resolve(cfg.DBPath, cfg.DataDir)
	if err != nil {
		return nil, nil, err
	}
	cfg.DBPath, cfg.DataDir = identity.Path, identity.DataDir
	lock, err := support.AcquireLock(identity.DataDir)
	if lock == nil {
		if err == nil {
			err = fmt.Errorf("brokerimage: single store lock owner is missing")
		}
		return nil, nil, err
	}
	owner := &singleStoreOwner{support: support, registry: cfg.Registry, lock: lock, stamp: storage.StoreStamp{Identity: identity}}
	if err != nil {
		return owner, nil, err
	}
	db, stamp, err := support.OpenCold(cfg)
	owner.db = db
	// Even a failed cold constructor retains the canonical identity. A zero
	// image stamp must never erase the lock domain needed by terminal cleanup.
	// origin: hot-update S6 plan §3.2; internal review rv-writer-7
	if stamp.Identity.Path != "" {
		owner.stamp = stamp
	}
	if err != nil || db == nil {
		if err == nil {
			err = fmt.Errorf("brokerimage: single store cold open returned no DB")
		}
		if db == nil {
			err = errors.Join(err, lock.Release())
			owner.lock = nil
		}
		return owner, db, err
	}
	return owner, db, nil
}

func singleMemoryStore(dsn string) bool {
	if dsn == ":memory:" {
		return true
	}
	u, err := url.Parse(dsn)
	return err == nil && u.Scheme == "file" &&
		(u.Opaque == ":memory:" || u.Path == ":memory:" || u.Query().Get("mode") == "memory")
}

type singleStoreOwner struct {
	mu              sync.Mutex
	support         *SingleStoreSupport
	registry        storage.DriverRegistry
	lock            SingleStoreLock
	db              *sql.DB
	stamp           storage.StoreStamp
	seal            storage.StoreSeal
	closeAttempted  bool
	closed          bool
	imageIneligible error
}

func (s *singleStoreOwner) Quiesce(deadline controlbudget.Deadline) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.db == nil || s.closeAttempted {
		return fmt.Errorf("brokerimage: single store is not open")
	}
	if _, err := singleImageStamp(s); err != nil {
		return err
	}
	return s.support.Quiesce(s.db, deadline)
}

// resumeSingleStore cannot lift admission after a partial close, even when its
// caller still holds the old pool pointer. Quiesce/Resume are a paired adapter.
// origin: hot-update S6 plan §4.2; internal review rv-writer-4
func resumeSingleStore(s *singleStoreOwner) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.db == nil || s.closeAttempted || s.support.Resume == nil {
		return errors.New("brokerimage: single store admission cannot resume")
	}
	return s.support.Resume(s.db)
}

func (s *singleStoreOwner) Stamp() (storage.StoreStamp, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return s.stamp, nil
	}
	if s.db == nil || s.closeAttempted {
		return storage.StoreStamp{}, fmt.Errorf("brokerimage: single store stamp is unknown")
	}
	return singleImageStamp(s)
}

// Image eligibility is stricter than cold compatibility. An N-1 binary may
// serve a database carrying newer migrations, but cannot certify it for hot
// reopen. Platform refusal belongs here, never at the cold constructor.
// The caller holds s.mu, binding the identity and pool to one owner generation.
// origin: hot-update S6 plan §3.1 / §3.3; internal review rv-writer-7/8
func singleImageStamp(s *singleStoreOwner) (storage.StoreStamp, error) {
	if runtime.GOOS != "linux" {
		return storage.StoreStamp{}, handoff.ErrUnsupported
	}
	if s.imageIneligible != nil {
		return storage.StoreStamp{}, s.imageIneligible
	}
	stamp, err := s.support.ReadStamp(s.db, s.stamp.Identity)
	if err == nil {
		s.stamp = stamp
	}
	return stamp, err
}

func (s *singleStoreOwner) LockResource() (handoff.Resource, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.lock == nil {
		return handoff.Resource{}, fmt.Errorf("brokerimage: single store lock was transferred or released")
	}
	return s.lock.Export()
}

func (s *singleStoreOwner) CloseForImage(deadline controlbudget.Deadline) (storage.StoreSeal, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return s.seal, nil
	}
	if s.db == nil {
		return storage.StoreSeal{}, fmt.Errorf("brokerimage: single store DB owner is missing")
	}
	if _, err := singleImageStamp(s); err != nil {
		return storage.StoreSeal{}, err
	}
	// Mark attempted before entering a potentially partial close. Reopen cannot
	// mistake a returned close error for a still pristine pool.
	// origin: hot-update S6 plan §3.2 / T10
	s.closeAttempted = true
	seal, err := s.support.CloseForImage(s.db, s.stamp, s.registry, deadline)
	s.seal = seal
	if err != nil {
		return seal, err
	}
	if !StoreSealClosed(seal) {
		return seal, fmt.Errorf("brokerimage: %s", DetailStoreCloseUnknown)
	}
	writers, readers := s.registry.Connections(seal.Stamp.Identity)
	if writers != 0 || readers != 0 {
		return seal, fmt.Errorf("brokerimage: single store drivers remain open")
	}
	s.closed, s.db, s.stamp = true, nil, seal.Stamp
	return seal, nil
}

func (s *singleStoreOwner) ReopenCurrent(stamp storage.StoreStamp, deadline controlbudget.Deadline) (StoreOwner, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	now, err := rawsocket.BootNow()
	if err != nil {
		return nil, err
	}
	if int64(deadline) <= now {
		return nil, fmt.Errorf("brokerimage: single store reopen deadline expired")
	}
	// Retain the original OFD across Exec error; NEVER reacquire/reflock. An
	// uncertain pool close forbids even an attempted current-store constructor.
	// origin: hot-update S6 plan §3.2 / §4.2 / T10
	if !s.closed || s.lock == nil || stamp.Identity != s.stamp.Identity {
		return nil, fmt.Errorf("brokerimage: single store is not known closed with its original lock")
	}
	if err := s.registry.AssertExclusive(stamp.Identity); err != nil {
		return nil, err
	}
	db, err := s.support.OpenCurrent(storage.OpenImageConfig{Stamp: stamp, Registry: s.registry})
	next := &singleStoreOwner{support: s.support, registry: s.registry, lock: s.lock, db: db, stamp: stamp}
	if db != nil {
		// Even a partial constructor owns the new pool. Move the lock so the old
		// owner's later Close cannot unlock underneath that partial writer.
		s.lock = nil
		return next, err
	}
	if err == nil {
		err = fmt.Errorf("brokerimage: current store open returned no DB")
	}
	return nil, err
}

// Close is the terminal-owner extension consumed by RuntimeSlot.StopCurrent.
// Image close retains the lock; terminal stop releases it only after a proven
// driver-zero receipt. Repeated Close on a superseded owner cannot unlock next.
func (s *singleStoreOwner) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	// Ordinary stop requires a real closed pool and driver-zero receipt, not an
	// image schema/checkpoint stamp. Otherwise newer migrations, other journals,
	// in-memory stores and Darwin strand cold owners during terminal cleanup.
	// Retain the interlock on an uncertain Close to protect that writer.
	// origin: hot-update S6 plan §3.2 / T02; internal review rv-writer-7/8/12
	if s.db != nil && !s.closed {
		s.closeAttempted = true
		if err := s.db.Close(); err != nil {
			return err
		}
		writers, readers := s.registry.Connections(s.stamp.Identity)
		if writers != 0 || readers != 0 {
			return errors.New("brokerimage: single store drivers remain open at stop")
		}
		s.closed, s.db = true, nil
	}
	if s.lock == nil {
		return nil
	}
	if err := s.lock.Release(); err != nil {
		return err
	}
	s.lock = nil
	return nil
}

// CurrentStoreOwners keeps health observers bound to current Node ownership.
// Replacement removes old keys so stopped Brokers cannot be retained by a
// process-global responder registry.
// origin: hot-update S6 plan §5 / §9.1
type CurrentStoreOwners[Key comparable, Owner comparable] struct {
	mu     sync.Mutex
	owners map[Key]Owner
}

func (r *CurrentStoreOwners[Key, Owner]) Bind(key Key, owner Owner) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.owners == nil {
		r.owners = make(map[Key]Owner)
	}
	for key, value := range r.owners {
		if value == owner {
			delete(r.owners, key)
		}
	}
	r.owners[key] = owner
}
func (r *CurrentStoreOwners[Key, Owner]) Remove(owner Owner) {
	r.mu.Lock()
	defer r.mu.Unlock()
	for key, value := range r.owners {
		if value == owner {
			delete(r.owners, key)
		}
	}
}
func (r *CurrentStoreOwners[Key, Owner]) Load(key Key) (Owner, bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	owner, ok := r.owners[key]
	return owner, ok
}

// Closed receipts share one journal-aware predicate with the cut and codec.
// DELETE reports not-applicable (-1,-1,-1); successful WAL TRUNCATE reports
// (0,0,0). Neither a partial close nor an incomplete checkpoint permits reopen.
// origin: hot-update S6 plan §3.2 / T11; internal review rv-writer-3
func StoreSealClosed(s storage.StoreSeal) bool {
	if s.Writer != storage.CloseClosed || s.ReadOnly != storage.CloseClosed ||
		s.Bolt != storage.CloseClosed || s.Transport != storage.CloseClosed || s.Raft != storage.CloseClosed ||
		s.DriverWriters != 0 || s.DriverReaders != 0 || s.ActiveRPCs != 0 || s.ActiveBackups != 0 {
		return false
	}
	switch s.Stamp.Journal {
	case "wal":
		return s.CheckpointBusy == 0 && s.CheckpointLog == 0 && s.CheckpointDone == 0
	case "delete":
		return s.CheckpointBusy == -1 && s.CheckpointLog == -1 && s.CheckpointDone == -1
	default:
		return false
	}
}

// Artifact preflight distinguishes installed pathname from running inode.
func StagedArtifactSHA256() (string, error) {
	exe, err := os.Executable()
	if err != nil {
		return "", err
	}
	return artifactSHA256(strings.TrimSuffix(exe, " (deleted)"))
}

func RunningArtifactSHA256() (string, error) {
	if runtime.GOOS != "linux" {
		return "", handoff.ErrUnsupported
	}
	runningArtifact.mu.Lock()
	defer runningArtifact.mu.Unlock()
	if runningArtifact.sha == "" {
		sha, err := artifactSHA256("/proc/self/exe")
		if err != nil {
			return "", err
		}
		runningArtifact.sha = sha
	}
	return runningArtifact.sha, nil
}

// Linux denies writes to the mapped executable (ETXTBSY); replacing its staged
// pathname cannot change /proc/self/exe. Exec replaces this entire cache with
// the new Go image. Only the mapped witness is cached: staged/probed artifacts
// still receive independent before/after hashes to bind each reader proof.
// origin: hot-update S6 plan §3.1 / T01 / T37
var runningArtifact struct {
	mu  sync.Mutex
	sha string
}

func artifactSHA256(path string) (string, error) {
	f, err := os.Open(path)
	if err != nil {
		return "", err
	}
	defer func() { _ = f.Close() }()
	h := sha256.New()
	if _, err := io.Copy(h, f); err != nil {
		return "", err
	}
	return hex.EncodeToString(h.Sum(nil)), nil
}

// InstallColdSingleStoreSupport binds cold Open and current-store reopen to the
// same canonical lock domain. The exportable lock lends its original OFD; only
// terminal Release unlocks it. An explicit binding installed earlier still takes
// precedence, so startup cannot swap registries under an existing owner.
// origin: hot-update S6 plan §3.1 / §12.1 P22
func InstallColdSingleStoreSupport() error {
	coldSingleSupport.Do(func() {
		singleStoreSupport.RLock()
		installed := singleStoreSupport.support != nil
		singleStoreSupport.RUnlock()
		if installed {
			return
		}
		coldSingleSupport.err = InstallSingleStoreSupport(SingleStoreSupport{
			Resolve: storage.CanonicalStoreIdentity,
			AcquireLock: func(dir string) (SingleStoreLock, error) {
				return cluster.AcquireImageDataDirLock(dir)
			},
			OpenCold: func(cfg SingleStoreConfig) (*sql.DB, storage.StoreStamp, error) {
				db, err := storage.Open(cfg.DBPath)
				if err != nil {
					return db, storage.StoreStamp{}, err
				}
				if singleMemoryStore(cfg.DBPath) {
					return db, storage.StoreStamp{}, nil
				}
				identity, err := storage.CanonicalStoreIdentity(cfg.DBPath, cfg.DataDir)
				if err != nil {
					return db, storage.StoreStamp{}, err
				}
				return db, storage.StoreStamp{Identity: identity}, nil
			},
			ReadStamp: storage.ReadStoreStamp,
			Quiesce: func(_ *sql.DB, deadline controlbudget.Deadline) error {
				// Single mode has no autonomous store producers. ControlOwner
				// fences its admission and joins the actual SQL borrowers next;
				// Quiesce must not close their pool before that join completes.
				return cutDeadline(deadline)
			},
			// Cold Quiesce above cannot fence any SQL work; confirm the same pool
			// remains usable when a refused cut returns to its original owner.
			Resume:        func(db *sql.DB) error { return db.Ping() },
			CloseForImage: closeColdSingleStore,
			OpenCurrent:   storage.OpenCurrent,
		})
	})
	return coldSingleSupport.err
}

var coldSingleSupport struct {
	sync.Once
	err error
}

// SingleStoreDB returns the current pool to the control constructor. Keeping
// this projection here prevents wiring from opening a second writer to recover.
// origin: hot-update S6 plan §3.3 / rv-cluster-2
func SingleStoreDB(owner StoreOwner) (*sql.DB, error) {
	s, ok := owner.(*singleStoreOwner)
	if !ok {
		return nil, fmt.Errorf("brokerimage: expected single store owner")
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed || s.db == nil {
		return nil, fmt.Errorf("brokerimage: single store is not open")
	}
	return s.db, nil
}

// NewSingleStoreForImage adopts the original OFD before OpenCurrent. A partial
// pool remains reachable through the nonnil owner even when construction fails.
// origin: hot-update S6 plan §3.3 / §4.2 / rv-cluster-2
func NewSingleStoreForImage(stamp storage.StoreStamp, file *os.File, deadline controlbudget.Deadline) (StoreOwner, error) {
	if err := cutDeadline(deadline); err != nil {
		return nil, err
	}
	if err := InstallColdSingleStoreSupport(); err != nil {
		return nil, err
	}
	lock, err := cluster.AdoptDataDirLock(stamp.Identity.DataDir, file)
	if err != nil {
		return nil, err
	}
	singleStoreSupport.RLock()
	support := singleStoreSupport.support
	singleStoreSupport.RUnlock()
	owner := &singleStoreOwner{support: support, registry: storage.DefaultDriverRegistry(), lock: lock, stamp: stamp}
	owner.db, err = support.OpenCurrent(storage.OpenImageConfig{Stamp: stamp, Registry: owner.registry})
	return owner, err
}

// Cold terminal cleanup runs after Run's handlers and loops have joined. Read
// the final SQL stamp rather than reusing the one captured before broker.New's
// cold generation write. Checkpoint precedes Close and only actual driver-zero
// observations can authorize releasing the interlock.
// origin: hot-update S6 plan §3.2 / §4.2 / T02
func closeColdSingleStore(db *sql.DB, stamp storage.StoreStamp, registry storage.DriverRegistry, deadline controlbudget.Deadline) (storage.StoreSeal, error) {
	seal := storage.StoreSeal{Stamp: stamp, Writer: storage.CloseUnknown, ReadOnly: storage.CloseUnknown,
		Bolt: storage.CloseClosed, Transport: storage.CloseClosed, Raft: storage.CloseClosed}
	if !deadlineAlive(deadline) {
		return seal, fmt.Errorf("brokerimage: single store close deadline expired")
	}
	current, stampErr := storage.ReadStoreStamp(db, stamp.Identity)
	if stampErr == nil {
		seal.Stamp = current
	}
	var checkpointErr error
	seal.CheckpointBusy, seal.CheckpointLog, seal.CheckpointDone, checkpointErr = storage.CheckpointForImage(db)
	closeErr := db.Close()
	seal.DriverWriters, seal.DriverReaders = registry.Connections(stamp.Identity)
	if closeErr == nil && seal.DriverWriters == 0 && seal.DriverReaders == 0 {
		seal.Writer, seal.ReadOnly = storage.CloseClosed, storage.CloseClosed
	}
	return seal, errors.Join(stampErr, checkpointErr, closeErr)
}

// CloseColdStore is used only until Run installs its current slot. Once managed,
// terminal cleanup belongs to that slot, including owners replaced after Exec.
// origin: hot-update S6 plan §4.2 / T02
func CloseColdStore(owner StoreOwner, managed bool) error {
	if owner == nil || managed {
		return nil
	}
	return CloseStoreOwner(owner)
}
