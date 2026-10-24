package infrahost

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"os"
	"time"

	"github.com/LinZiyang666/tether/internal/controlbudget"
	"github.com/LinZiyang666/tether/internal/infracontract"
)

// ACMEConfig is assembled from root policy, never from a caller's path. Root and
// Initial are already approved directory descriptors; NewACMEStorage duplicates
// them, so their owners can close them after construction. Exit must observe the
// exact Expected incarnation and every writer of the selected filesystem tree.
// Its complete census contains only remaining writers, not all host processes.
// origin: hot-update S9 plan §5.3 / §6.2 / T20
type ACMEConfig struct {
	Root, Initial *os.File
	UID, GID      int
	Exit          func(infracontract.Process, infracontract.Deadline, infracontract.Stop) (infracontract.Census, error)
}

// ACMEStorage owns filesystem migration only. The P04 worker authorizes the
// request, writes the common intent/result journal, and binds Do at
// FinalizeStorage; P05 owns FenceStart/ReleaseStart and calls the helpers below.
// An ACME lock is distinct from the common journal lock, which cannot span I/O.
type ACMEStorage struct {
	config        ACMEConfig
	root, initial *os.File
	syncFile      func(*os.File) error
	copyStep      func() // An in-package failure-window probe; production leaves it nil.
}

var _ infracontract.Host = (*ACMEStorage)(nil)

var (
	errACMEBusy     = errors.New("infra_busy")
	errACMENotReady = errors.New("infra_not_ready")
	errACMEInternal = errors.New("infra_internal")
	errACMEBudget   = errors.New("infra_budget_exceeded")
)

// The selector and startup barrier are one atomic durable record. In particular,
// a committed directory is not a permit: Fenced remains true until P05 commits
// ReleaseStart. Pending survives interrupted bounded copy actions; a later action
// verifies and reuses complete files without treating a partial tree as current.
// origin: hot-update S9 plan §2.2 / §5.3 / T21
type acmeState struct {
	Version       uint16
	Purpose       string
	Revision      uint64
	Tx, Domain    infracontract.ID
	Step          uint32
	Expected      infracontract.Process
	UID, GID      int
	Fenced        bool
	Finalized     bool
	Current       string
	Pending       string
	Garbage       string
	TreeDigest    infracontract.Digest
	RequestDigest infracontract.Digest
	Payload       infracontract.Digest
}

// ACMEView is a private filesystem reference, not a public status payload. Warm
// views have directory/file modes 0500/0400 and must be consumed with automatic
// HTTPS disabled by P08. Same-UID code belongs to the trusted product domain.
type ACMEView struct {
	Directory *os.File
	Revision  uint64
	Digest    infracontract.Digest
	slot      string
}

// ACMEWriter keeps the permanent writer-lock OFD alive for the whole stock
// invocation. P06 must inherit Lease through exec, retaining its last reference
// until exec acknowledgment; closing a parent is not an ownership transfer.
type ACMEWriter struct {
	ACMEView
	Lease *os.File
}

// NewACMEStorage refuses unsupported platforms before creating any record. It
// makes no claim about systemd/nft availability; P05/P07 check those separately.
func NewACMEStorage(config ACMEConfig) (*ACMEStorage, error) {
	if err := acmePlatform(); err != nil {
		return nil, err
	}
	// Linux reserves the all-ones UID/GID as "leave unchanged" in chown. Reject
	// it and larger integers before syscall truncation can select the wrong owner.
	// origin: hot-update S9 plan §5.3 / T20
	if config.Root == nil || config.Initial == nil || config.Exit == nil || config.UID < 0 || config.GID < 0 || uint64(config.UID) >= uint64(^uint32(0)) || uint64(config.GID) >= uint64(^uint32(0)) {
		return nil, infracontract.ErrIncomplete
	}
	root, err := acmeDuplicateDirectory(config.Root, os.Geteuid())
	if err != nil {
		return nil, err
	}
	initial, err := acmeDuplicateDirectory(config.Initial, -1)
	if err != nil {
		_ = root.Close()
		return nil, err
	}
	s := &ACMEStorage{config: config, root: root, initial: initial, syncFile: (*os.File).Sync}
	return s, nil
}

// Close releases only this backend's descriptors, not views or invocation leases
// already handed to another owner. The caller must join its worker before Close.
func (s *ACMEStorage) Close() error {
	return errors.Join(s.root.Close(), s.initial.Close())
}

func acmeCheck(deadline infracontract.Deadline, stop infracontract.Stop) error {
	select {
	case <-stop:
		return errACMEBudget
	default:
	}
	if _, err := controlbudget.Remaining(deadline); err != nil {
		return errACMEBudget
	}
	return nil
}

func acmeRequest(r infracontract.HostRequest) (infracontract.Digest, error) {
	if r.Component != infracontract.Caddy {
		return infracontract.Digest{}, infracontract.ErrUnsupported
	}
	b, err := infracontract.EncodeRequest(r)
	if err != nil {
		return infracontract.Digest{}, infracontract.ErrIncomplete
	}
	return sha256.Sum256(b), nil
}

func acmeMatches(state acmeState, r infracontract.HostRequest) bool {
	return state.Tx == r.Tx && state.Domain == r.Domain && state.Step == r.Step && state.Expected == r.Expected
}

// FenceACME persists the storage startup barrier before an old writer can exit.
// P05 must call this under its authorized common-journal intent. RestartSec,
// elapsed time and controller lifetime never release this record.
// origin: hot-update S9 plan §5.3 / T21
func FenceACME(s *ACMEStorage, r infracontract.HostRequest, d infracontract.Deadline, stop infracontract.Stop) (uint64, error) {
	if _, err := acmeRequest(r); err != nil || r.Op != infracontract.FenceStart {
		return 0, infracontract.ErrIncomplete
	}
	lock, err := acmeLock(s.root, "transaction.lock", d, stop, true)
	if err != nil {
		return 0, err
	}
	// Closing the lease descriptor is the unlock. Nothing is written through it,
	// and close(2) releases the descriptor and its OFD lock even when it reports
	// an error, so that error carries nothing a caller could act on.
	defer func() { _ = lock.Close() }()
	state, err := s.readState(true)
	if err != nil {
		return 0, err
	}
	if state.Fenced && state.Tx != (infracontract.ID{}) {
		if acmeMatches(state, r) {
			return state.Revision, nil
		}
		return 0, errACMEBusy
	}
	state.Revision++
	state.Tx, state.Domain, state.Step, state.Expected = r.Tx, r.Domain, r.Step, r.Expected
	state.Fenced, state.Finalized = true, false
	state.RequestDigest, state.Payload = infracontract.Digest{}, infracontract.Digest{}
	if err := s.writeState(state); err != nil {
		return 0, err
	}
	return state.Revision, nil
}

// WarmACME copies the selected current tree while the old writer can still renew.
// It never changes the selector and never copies CertMagic's advisory locks.
func WarmACME(s *ACMEStorage, d infracontract.Deadline, stop infracontract.Stop) (ACMEView, error) {
	lock, err := acmeLock(s.root, "transaction.lock", d, stop, true)
	if err != nil {
		return ACMEView{}, err
	}
	defer func() { _ = lock.Close() }() // Unlock; see FenceACME.
	state, err := s.readState(true)
	if err != nil {
		return ACMEView{}, err
	}
	source, err := s.current(state)
	if err != nil {
		return ACMEView{}, err
	}
	defer func() { _ = source.Close() }() // A read-only directory cursor; no write can be lost.
	slot, err := acmeRandomSlot("warm-")
	if err != nil {
		return ACMEView{}, err
	}
	digest, err := s.copyTree(source, slot, true, d, stop)
	if err != nil {
		_ = acmeRemoveTree(s.root, slot)
		return ACMEView{}, err
	}
	view, err := acmeOpenDirectory(s.root, slot)
	if err != nil {
		_ = acmeRemoveTree(s.root, slot)
		return ACMEView{}, err
	}
	return ACMEView{Directory: view, Revision: state.Revision, Digest: digest, slot: slot}, nil
}

// RetireACMEWarm is called only after P10/P02 prove the warm invocation has no
// responsibility. The opaque slot cannot designate a selected writer directory.
func RetireACMEWarm(s *ACMEStorage, view ACMEView) error {
	if !acmeSlot(view.slot, "warm-") {
		return infracontract.ErrIncomplete
	}
	return acmeRemoveTree(s.root, view.slot)
}

// Do implements only the P09 binding. It does not independently grant policy or
// domain authority: the authorized P04 worker is its sole production caller.
func (s *ACMEStorage) Do(r infracontract.HostRequest, d infracontract.Deadline, stop infracontract.Stop) (infracontract.HostReceipt, error) {
	if r.Op != infracontract.FinalizeStorage {
		return infracontract.HostReceipt{}, infracontract.ErrUnsupported
	}
	requestDigest, err := acmeRequest(r)
	if err != nil {
		return infracontract.HostReceipt{}, err
	}
	lock, err := acmeLock(s.root, "transaction.lock", d, stop, true)
	if err != nil {
		return infracontract.HostReceipt{}, err
	}
	defer func() { _ = lock.Close() }() // Unlock; see FenceACME.
	state, err := s.readState(false)
	if err != nil {
		return infracontract.HostReceipt{}, err
	}
	// origin: hot-update S9 plan §5.3 / T20 / T21
	if !state.Fenced || !acmeMatches(state, r) {
		return infracontract.HostReceipt{}, errACMENotReady
	}
	if state.Finalized {
		if state.RequestDigest != requestDigest {
			return infracontract.HostReceipt{}, errACMEBusy
		}
		return s.finalReceipt(state, r, d, stop)
	}
	// A complete, empty writer census is independent evidence, not an age-based
	// stale-lock heuristic. The lifetime lock separately catches a live new writer.
	// origin: hot-update S9 plan §5.3 / T20
	census, err := s.config.Exit(r.Expected, d, stop)
	if err != nil {
		return infracontract.HostReceipt{}, errACMENotReady
	}
	if !census.Complete || census.Digest == (infracontract.Digest{}) || len(census.Processes) != 0 || census.OldOwnerPending != 0 {
		return infracontract.HostReceipt{}, infracontract.ErrIncomplete
	}
	writer, err := acmeLock(s.root, "writer.lock", d, stop, false)
	if err != nil {
		return infracontract.HostReceipt{}, err
	}
	// The writer lease only proves no new writer is alive during the copy; its
	// close is the release and has nothing to report. See FenceACME.
	defer func() { _ = writer.Close() }()
	source, err := s.current(state)
	if err != nil {
		return infracontract.HostReceipt{}, err
	}
	defer func() { _ = source.Close() }() // A read-only directory cursor; no write can be lost.
	if state.Pending == "" {
		state.Pending, err = acmeRandomSlot("tree-")
		if err != nil {
			return infracontract.HostReceipt{}, err
		}
		state.Payload, state.RequestDigest = r.Payload, requestDigest
		if err := s.writeState(state); err != nil {
			return infracontract.HostReceipt{}, err
		}
	} else if state.Payload != r.Payload {
		return infracontract.HostReceipt{}, errACMEBusy
	} else if state.RequestDigest != requestDigest {
		// A new authorized sequence gives the next bounded action its own budget;
		// it is not a replay that extends an expired request. The tree reference,
		// transaction, domain, step and old incarnation remain fixed by the fence.
		// origin: hot-update S9 plan §2.2 / §6.1 / T21
		state.RequestDigest = requestDigest
		if err := s.writeState(state); err != nil {
			return infracontract.HostReceipt{}, err
		}
	}
	digest, err := s.copyTree(source, state.Pending, false, d, stop)
	if err != nil {
		return infracontract.HostReceipt{}, err
	}
	if err := acmeCheck(d, stop); err != nil {
		return infracontract.HostReceipt{}, err
	}
	// The old selector stays intact on every pre-commit failure. Both the tree and
	// its ownership are durable before this CAS; the fence remains closed afterward.
	// origin: hot-update S9 plan §5.3 / T20 / T21
	state.Garbage, state.Current, state.Pending = state.Current, state.Pending, ""
	state.TreeDigest, state.Finalized = digest, true
	state.Revision++
	if err := s.writeState(state); err != nil {
		return infracontract.HostReceipt{}, err
	}
	return s.finalReceipt(state, r, d, stop)
}

func (s *ACMEStorage) finalReceipt(state acmeState, r infracontract.HostRequest, d infracontract.Deadline, stop infracontract.Stop) (infracontract.HostReceipt, error) {
	if err := acmeCheck(d, stop); err != nil {
		return infracontract.HostReceipt{}, err
	}
	if state.Garbage != "" {
		if err := acmeRemoveTree(s.root, state.Garbage); err != nil {
			return infracontract.HostReceipt{}, err
		}
		state.Garbage = ""
		if err := s.writeState(state); err != nil {
			return infracontract.HostReceipt{}, err
		}
	}
	b, _ := json.Marshal(state)
	return infracontract.HostReceipt{Tx: r.Tx, Sequence: r.Sequence, Revision: state.Revision,
		Op: r.Op, Applied: true, RequestDigest: state.RequestDigest, Journal: sha256.Sum256(b)}, nil
}

// Inspect cannot sign a host or process census. P40 binds the real P02 observer
// at Inspect, rather than accepting an empty ACME-only census as host readiness.
func (s *ACMEStorage) Inspect(infracontract.ID, infracontract.Deadline) (infracontract.Census, error) {
	return infracontract.Census{}, infracontract.ErrUnsupported
}

// ReleaseACMEStart is a revision CAS, called after the common FinalizeStorage
// result is durable. A retry with a stale revision cannot release a later fence.
// origin: hot-update S9 plan §5.3 / T21
func ReleaseACMEStart(s *ACMEStorage, tx, domain infracontract.ID, revision uint64, d infracontract.Deadline, stop infracontract.Stop) (uint64, error) {
	return releaseACMEStart(s, tx, domain, revision, nil, d, stop)
}

// ReleaseACMERequest additionally binds the common owner's release to the exact
// finalized writer, step and payload, including when querying a committed release.
// origin: S9 internal review rv-infra F09
func ReleaseACMERequest(s *ACMEStorage, r infracontract.HostRequest, revision uint64, d infracontract.Deadline, stop infracontract.Stop) (uint64, error) {
	if r.Op != infracontract.ReleaseStart || r.Component != infracontract.Caddy {
		return 0, errACMENotReady
	}
	return releaseACMEStart(s, r.Tx, r.Domain, revision, &r, d, stop)
}

func releaseACMEStart(s *ACMEStorage, tx, domain infracontract.ID, revision uint64, request *infracontract.HostRequest, d infracontract.Deadline, stop infracontract.Stop) (uint64, error) {
	lock, err := acmeLock(s.root, "transaction.lock", d, stop, true)
	if err != nil {
		return 0, err
	}
	defer func() { _ = lock.Close() }() // Unlock; see FenceACME.
	state, err := s.readState(false)
	if err != nil {
		return 0, err
	}
	if state.Tx != tx || state.Domain != domain || !state.Finalized || state.Pending != "" || state.Garbage != "" {
		return 0, errACMENotReady
	}
	if request != nil && (!acmeMatches(state, *request) || state.Payload != request.Payload) {
		return 0, errACMENotReady
	}
	// The durable selector also records release progress. Only the exact FINAL
	// revision can query its committed release after worker death or a failed
	// unit CAS; a newer fence or another transaction is never released by replay.
	// origin: S9 internal review rv-infra F09
	if !state.Fenced && revision != 0 && state.Revision == revision+1 {
		return state.Revision, nil
	}
	if !state.Fenced || state.Revision != revision {
		return 0, errACMENotReady
	}
	state.Fenced = false
	state.Revision++
	if err := s.writeState(state); err != nil {
		return 0, err
	}
	return state.Revision, nil
}

// ClaimACMEWriter waits within the launch budget. The barrier is re-read from
// disk under the transaction lock, so a dead/replaced helper and an automatic
// systemd restart cannot reuse an in-memory "ready" flag.
// origin: hot-update S9 plan §5.3 / T21
func ClaimACMEWriter(s *ACMEStorage, tx, domain infracontract.ID, revision uint64, d infracontract.Deadline, stop infracontract.Stop) (ACMEWriter, error) {
	for {
		writer, err := acmeClaimWriter(s, tx, domain, revision, d, stop)
		if err == nil || !errors.Is(err, errACMENotReady) {
			return writer, err
		}
		if err := acmeCheck(d, stop); err != nil {
			return ACMEWriter{}, err
		}
		timer := time.NewTimer(10 * time.Millisecond)
		select {
		case <-stop:
			timer.Stop()
			return ACMEWriter{}, errACMEBudget
		case <-timer.C:
		}
	}
}

func acmeClaimWriter(s *ACMEStorage, tx, domain infracontract.ID, revision uint64, d infracontract.Deadline, stop infracontract.Stop) (ACMEWriter, error) {
	lock, err := acmeLock(s.root, "transaction.lock", d, stop, false)
	if errors.Is(err, errACMEBusy) {
		return ACMEWriter{}, errACMENotReady
	}
	if err != nil {
		return ACMEWriter{}, err
	}
	defer func() { _ = lock.Close() }() // Unlock; see FenceACME.
	state, err := s.readState(false)
	if err != nil {
		return ACMEWriter{}, err
	}
	if state.Fenced || !state.Finalized || state.Tx != tx || state.Domain != domain || state.Revision != revision {
		return ACMEWriter{}, errACMENotReady
	}
	lease, err := acmeLock(s.root, "writer.lock", d, stop, false)
	if err != nil {
		return ACMEWriter{}, err
	}
	view, err := s.current(state)
	if err != nil {
		_ = lease.Close()
		return ACMEWriter{}, err
	}
	return ACMEWriter{ACMEView: ACMEView{Directory: view, Revision: state.Revision, Digest: state.TreeDigest}, Lease: lease}, nil
}

func acmeSlot(name, prefix string) bool {
	if len(name) != len(prefix)+32 || name[:len(prefix)] != prefix {
		return false
	}
	b, err := hex.DecodeString(name[len(prefix):])
	return err == nil && hex.EncodeToString(b) == name[len(prefix):]
}

// ACMEFinalRevision resolves the exact durable FINAL result, including the
// already committed release revision, for the common unit/storage reconciler.
// origin: S9 internal review rv-infra F09
func ACMEFinalRevision(s *ACMEStorage, r infracontract.HostRequest, d infracontract.Deadline) (_ uint64, resultErr error) {
	lock, err := acmeLock(s.root, "transaction.lock", d, nil, true)
	if err != nil {
		return 0, err
	}
	defer func() { resultErr = errors.Join(resultErr, lock.Close()) }()
	state, err := s.readState(false)
	if err != nil {
		return 0, err
	}
	if !acmeMatches(state, r) || !state.Finalized || state.Payload != r.Payload || state.Pending != "" || state.Garbage != "" {
		return 0, errACMENotReady
	}
	if !state.Fenced {
		return state.Revision - 1, nil
	}
	return state.Revision, nil
}

// CurrentACMEView is the read-only launch offer. It grants no writer lease and
// cannot release a fence. ClaimACMEWriter rechecks the same selector revision.
func CurrentACMEView(s *ACMEStorage, tx, domain infracontract.ID, d infracontract.Deadline) (ACMEView, error) {
	lock, err := acmeLock(s.root, "transaction.lock", d, nil, true)
	if err != nil {
		return ACMEView{}, err
	}
	state, err := s.readState(false)
	if err != nil {
		return ACMEView{}, errors.Join(err, lock.Close())
	}
	if state.Tx != tx || state.Domain != domain || state.Fenced || !state.Finalized {
		return ACMEView{}, errors.Join(errACMENotReady, lock.Close())
	}
	directory, err := s.current(state)
	err = errors.Join(err, lock.Close())
	if err != nil {
		if directory != nil {
			err = errors.Join(err, directory.Close())
		}
		return ACMEView{}, err
	}
	return ACMEView{Directory: directory, Revision: state.Revision, Digest: state.TreeDigest}, nil
}
