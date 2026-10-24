// marker.go owns the durable executable transaction independently of agent connections.
// origin: hot-update S4 plan §10.
package binaryupgrade

import (
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"syscall"
	"time"

	"github.com/LinZiyang666/tether/internal/upgradetx"
)

// Store keeps the durability hook with the transaction that owns its writes.
// A nil hook performs real fsync; tests may inject the same failures as the agent.
type Store struct{ BeforeSync func(kind, path string) error }

const (
	StatePending        = "pending"
	StateCommitted      = "committed"
	StateRolledBack     = "rolled_back"
	StateRollbackFailed = "rollback_failed"

	// upgradeMarkerName lives NEXT TO the binary (same writable dir the
	// installer already requires for its tmp file and atomic rename).
	MarkerName = ".tether-upgrade.json"
)

// upgradeMarker is the on-disk record of an in-flight (or just-finished)
// upgrade. Written atomically (tmp + rename) next to the binary.
//
// TargetSID/TargetNID/UpgradeID (external review F1): the marker lives next
// to a binary that MAY be shared by several agent processes on one host
// (requirements: one machine can run multiple agents). These fields pin the
// upgrade to the single instance `node upgrade <nid>` targeted — commit and
// register-report refuse a marker whose target is not this agent, so a
// co-hosted sibling can neither claim nor clear another instance's upgrade.
// A shared binary is ONE UPGRADE DOMAIN: while a marker is pending, sibling
// installs are refused (upgrade_in_progress) until it commits or rolls back.
type Marker struct {
	State       string    `json:"state"`
	PrevSHA     string    `json:"prev_sha"`
	NewSHA      string    `json:"new_sha"`
	PrevVersion string    `json:"prev_version"`
	NewVersion  string    `json:"new_version"`
	Deadline    time.Time `json:"deadline"`
	BootCount   int       `json:"boot_count"`
	BootBudget  int       `json:"boot_budget"`
	Detail      string    `json:"detail,omitempty"`
	TargetSID   string    `json:"target_sid,omitempty"`
	TargetNID   string    `json:"target_nid,omitempty"`
	UpgradeID   string    `json:"upgrade_id,omitempty"`

	// TargetInstance is the instance LINEAGE the upgrade was started by.
	//
	// (TargetSID, TargetNID) alone stopped discriminating the moment one image
	// could run twice: TargetNID is the agent.yaml basename — deliberately so,
	// because a lease name is transient and comparing against it would strand
	// the marker — and every instance of that image shares it. Without this
	// field the F1 sibling guard gives the SAME verdict to every clone, so the
	// basename holder reports another instance's rollback against a nid that
	// was never upgraded, and ERASES the marker of the instance that actually
	// rolled back.
	//
	// The value survives the re-exec through the environment (see instance.go),
	// which is what makes it usable as the identity of an upgrade in flight.
	// Empty on any marker written before this existed, which correctly falls
	// back to the (sid, nid) comparison.
	TargetInstance string `json:"target_instance,omitempty"`

	// Hot-update S3 (architecture §22.6), all additive: an older binary reads
	// the marker with plain json.Unmarshal and ignores them. Generation is the
	// image generation the staged binary will import (the exporter's +1);
	// ResourcePhase is where the carried resources are — one of the
	// markerPhase* values below, never the bundle (the marker is a plain file
	// next to the binary; the bundle carries the registry key and lives only
	// in the sealed handoff memfd). ImportedResources and FreezeWindowMS are
	// stamped by the new image after its thaw: the calibration sample of the
	// ≤500 ms local-freeze budget (upgradebudget.LocalFreezeWindow).
	Generation        uint64 `json:"generation,omitempty"`
	ResourcePhase     string `json:"resource_phase,omitempty"`
	ImportedResources int    `json:"imported_resources,omitempty"`
	FreezeWindowMS    int64  `json:"freeze_window_ms,omitempty"`
	// origin: hot-update S4 plan §5.2. Reader declarations apply only to
	// these exact bytes; absent declarations on an S3 marker require a probe.
	CandidateImageABI     int      `json:"candidate_image_abi,omitempty"`
	CandidateImageDomains []string `json:"candidate_image_domains,omitempty"`
	CandidateImageSHA256  string   `json:"candidate_image_sha256,omitempty"`
	PrevImageABI          int      `json:"prev_image_abi,omitempty"`
	PrevImageDomains      []string `json:"prev_image_domains,omitempty"`
	PrevImageSHA256       string   `json:"prev_image_sha256,omitempty"`
}

// Marker resource phases (upgradeMarker.ResourcePhase). The one naming
// source for the marker, the logs the drills grep and architecture §22.6.
const (
	PhaseStaged          = "staged"           // install wrote pending; the cut follows
	PhaseActive          = "active"           // the new image thawed what it imported
	PhaseRedispatched    = "redispatched"     // the bundle went back to the previous binary
	PhaseThawed          = "thawed"           // the exec failed; the old image thawed in place
	PhaseDeferred        = "deferred"         // the cut deferred; the disk was rolled back
	PhaseLost            = "lost"             // a crash window: the carried tasks are lost
	PhaseRollbackBlocked = "rollback_blocked" // the watchdog cannot reach a safe point; retrying
)

func MarkerPath(exePath string) string {
	return filepath.Join(filepath.Dir(exePath), MarkerName)
}

func PrevPath(exePath string) string { return exePath + ".prev" }

// upgradeLockPath is the CROSS-PROCESS mutual exclusion for every
// read-modify-write of the marker/prev/dst triple (external review F1: the
// in-process mutexes cannot serialize two agent processes sharing one
// binary). flock, not O_EXCL: released automatically when a process dies.
func LockPath(exePath string) string {
	return filepath.Join(filepath.Dir(exePath), ".tether-upgrade.lock")
}

// withUpgradeFileLock runs fn holding the host-wide upgrade flock. blocking
// decides contention behavior: the install pipeline uses non-blocking (a
// loser replies upgrade_in_progress instead of queueing behind a ~35s
// download), the short marker transitions (boot / commit / watchdog /
// exec-failure recovery) block — their critical sections are milliseconds.
// Returns errUpgradeLockBusy when non-blocking and the lock is held.
func WithFileLock(exePath string, blocking bool, fn func() error) error {
	f, err := os.OpenFile(LockPath(exePath), os.O_CREATE|os.O_RDWR, 0o644)
	if err != nil {
		return fmt.Errorf("open upgrade lock: %w", err)
	}
	defer func() { _ = f.Close() }()
	how := syscall.LOCK_EX
	if !blocking {
		how |= syscall.LOCK_NB
	}
	var lockErr error
	for {
		lockErr = syscall.Flock(int(f.Fd()), how)
		if !errors.Is(lockErr, syscall.EINTR) {
			break
		}
	}
	if lockErr != nil {
		if !blocking && (errors.Is(lockErr, syscall.EWOULDBLOCK) || errors.Is(lockErr, syscall.EAGAIN)) {
			return ErrLockBusy
		}
		return fmt.Errorf("flock upgrade lock: %w", lockErr)
	}
	defer func() { _ = syscall.Flock(int(f.Fd()), syscall.LOCK_UN) }()
	return fn()
}

// errUpgradeLockBusy: another PROCESS holds the host-wide upgrade lock.
var ErrLockBusy = errors.New("upgrade: host-wide upgrade lock is held by another process")

// syncFile fsyncs an open file (data + metadata, including a chmod done on
// the same fd). Part of the F6 ordered-durability protocol: rename gives
// crash ATOMICITY, not persistence — without fsync of file and directory a
// power cut can surface a truncated dst or a lost marker after reboot.
func (s Store) SyncFile(f *os.File) error {
	if s.BeforeSync != nil {
		return s.BeforeSync("file", f.Name())
	}
	return f.Sync()
}

// syncDirE fsyncs a directory, making the rename/link/remove entries inside
// it durable. Error-returning: the install pipeline treats a failed sync as
// install_failed (nothing irreversible has happened yet).
func (s Store) SyncDirE(dir string) error {
	if s.BeforeSync != nil {
		return s.BeforeSync("dir", dir)
	}
	d, err := os.Open(dir)
	if err != nil {
		return err
	}
	serr := d.Sync()
	cerr := d.Close()
	if serr != nil {
		return serr
	}
	return cerr
}

// syncDir is the best-effort variant for the rollback/terminal paths, where
// refusing to proceed on a sync error would recreate the stuck states this
// machinery exists to end. Loud, not fatal.
func (s Store) SyncDir(dir string) {
	if err := s.SyncDirE(dir); err != nil {
		slog.Default().Warn("upgrade: directory fsync failed (durability of the last transition is not guaranteed)",
			"dir", dir, "err", err)
	}
}

// readUpgradeMarker returns (nil, nil) when no marker exists, and (nil, err)
// when the file exists but does not decode — corrupt markers are treated as
// idle by every caller (loudly, never a panic and never a block: a corrupt
// marker must not brick the upgrade path).
func ReadMarker(path string) (*Marker, error) {
	raw, err := os.ReadFile(path)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil, nil
		}
		return nil, err
	}
	// The v2 journal keeps the v1 fields as a read-only projection beside its
	// version/entries. Every v1 writer in this package (boot rollback, bootstrap
	// redispatch, watchdog, install compensation) reads through here, so the
	// projection must refuse rather than authorize a v1 effect on a v2 attempt.
	// origin: hot-update S8 plan §3.3 / §11.2 P17 / M11
	var m struct {
		Marker
		Version uint32          `json:"version"`
		Schema  json.RawMessage `json:"schema_version"`
		Entries json.RawMessage `json:"entries"`
	}
	if err := json.Unmarshal(raw, &m); err != nil {
		return nil, fmt.Errorf("upgrade marker %s is corrupt: %w", path, err)
	}
	if m.Version > 1 || len(m.Schema) != 0 || len(m.Entries) != 0 {
		return nil, ErrTransactionJournal
	}
	return &m.Marker, nil
}

// ErrTransactionJournal: the marker path holds an S8 journal. Only its sole v2
// writer may act on it; v1 callers treat it like any unreadable marker.
var ErrTransactionJournal = &upgradetx.Error{Code: upgradetx.CodeUnsettled, Detail: "transaction journal requires its sole v2 writer"}

func (s Store) WriteMarker(path string, m *Marker) error {
	raw, err := json.MarshalIndent(m, "", "  ")
	if err != nil {
		return err
	}
	tmp, err := os.CreateTemp(filepath.Dir(path), ".tether-upgrade-marker-*")
	if err != nil {
		return err
	}
	name := tmp.Name()
	_, werr := tmp.Write(raw)
	// fsync before the rename (internal review S13): without it, a power cut
	// after the rename can leave a zero-length marker on filesystems without
	// auto_da_alloc-style heuristics — which reads as corrupt→idle and
	// silently disarms the rollback machinery.
	serr := s.SyncFile(tmp)
	cerr := tmp.Close()
	if werr != nil || serr != nil || cerr != nil {
		_ = os.Remove(name)
		return errors.Join(werr, serr, cerr)
	}
	if err := os.Rename(name, path); err != nil {
		_ = os.Remove(name)
		return err
	}
	// Directory fsync (external review F6): the rename above is atomic but
	// not durable until the directory entry itself is on stable storage.
	return s.SyncDirE(filepath.Dir(path))
}

// bootAction is decideBoot's verdict. Deliberately a string type, not an iota
// enum (keeps it out of the enum-switch-default gate's family table and makes
// test failures self-describing).
type BootAction string

const (
	// bootProceed: no marker / terminal marker — a perfectly ordinary boot.
	BootProceed BootAction = "proceed"
	// bootContinuePending: we ARE the staged new binary, inside budget and
	// deadline. Caller persists boot_count+1 and lets Run arm the watchdog.
	BootContinuePending BootAction = "continue_pending"
	// bootRollback: pending but the boot budget or deadline is exhausted —
	// restore the prev slot and exec into it.
	BootRollback BootAction = "rollback"
	// bootConvergeRolledBack: we are the PREV binary while the marker still
	// says pending — a rollback (or a failed install before the flip) was
	// interrupted before the marker caught up. Converge the record.
	BootConvergeRolledBack BootAction = "converge_rolled_back"
	// bootMarkOrphan: the running binary matches NEITHER sha (someone swapped
	// the binary by hand mid-upgrade). Record rollback_failed and run with
	// what's on disk — anything else would fight the operator.
	BootMarkOrphan BootAction = "mark_orphan"
)

// decideBoot is the pure boot-time decision (plan §3.1, table-driven-tested in
// upgrade_state_test.go). It does no IO: the caller supplies the marker, the
// sha of the binary it is running from, and the clock.
func DecideBoot(m *Marker, selfSHA string, now time.Time) BootAction {
	if m == nil || m.State != StatePending {
		return BootProceed
	}
	if selfSHA == m.NewSHA {
		if m.BootCount >= m.BootBudget || !now.Before(m.Deadline) {
			return BootRollback
		}
		return BootContinuePending
	}
	if selfSHA == m.PrevSHA {
		return BootConvergeRolledBack
	}
	return BootMarkOrphan
}
