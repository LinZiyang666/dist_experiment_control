//go:build linux || darwin

package transferpath

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/LinZiyang666/tether/internal/controlbudget"
	"github.com/LinZiyang666/tether/internal/handoff"
	// The transport-free half of xfercontract, with identical identities: the
	// filesystem leaf must not reach NATS through the contract's port file.
	// origin: hot-update S7 plan §11.1 / P00 — NATS-free value split
	xfercontract "github.com/LinZiyang666/tether/internal/xfercontract/xfervalue"
	"golang.org/x/sys/unix"
)

// Atomic owns the original tmp/parent descriptions, rather than paths that an
// importer could reopen. active covers an entire IO or commit critical section;
// frozen and stopping close admission before a lifecycle caller starts joining.
// A timed-out join retains this owner and every descriptor until the IO returns.
// origin: hot-update S7 plan §3 / T10 / T11
type Atomic struct {
	mu          sync.Mutex
	changed     chan struct{}
	active      bool
	frozen      bool
	stopping    bool
	closed      bool
	imported    bool
	state       xfercontract.AtomicState
	tmp         *os.File
	parent      *os.File
	source      *os.File
	parentPath  string
	written     int64
	needsRepair bool
	repairErr   error
	closeErr    error
	ops         atomicOperations
	observe     func(xfercontract.Observation)
	eventSeq    uint64
	// objectBound/objectVersion name the single object incarnation whose Grant
	// may authorize this receiver's primitive. The frozen AtomicState has no
	// field for it, so the owner that knows the transfer identity declares it.
	// origin: hot-update S7 plan §8.1 tier A/B / INTEG-D Tier A grant regression
	objectBound   bool
	objectVersion string
}

// inlineAtomicMaxBytes mirrors proto.XferTierAMaxBytes (8 MiB). transferpath
// cannot import proto without a transport closure; a test pins equality.
// The per-owner syscall table lets tests fail AFTER a real primitive has
// landed. Replacing the whole receiver with a fake would miss that boundary.
type atomicOperations struct {
	write    func(*os.File, []byte, int64) (int, error)
	sync     func(*os.File) error
	truncate func(*os.File, int64) error
	close    func(*os.File) error
	rename   func(int, string, int, string) error
	link     func(int, string, int, string, int) error
	unlink   func(int, string, int) error
}

func defaultAtomicOperations() atomicOperations {
	return atomicOperations{
		write: (*os.File).WriteAt, sync: (*os.File).Sync,
		truncate: (*os.File).Truncate, close: (*os.File).Close,
		rename: unix.Renameat, link: unix.Linkat, unlink: unix.Unlinkat,
	}
}

var _ AtomicReceiver = (*Atomic)(nil)
var _ ImportFactory = ImportAtomic

// NewAtomicReceiver uses the established traversal, exclusive WRONLY tmp and
// pinned-parent guards. The suffix is a private tmp nonce, not a business ID.
func NewAtomicReceiver(vp *ValidatedPath, randSuffix string, force bool) (AtomicReceiver, error) {
	if vp == nil || !atomicToken(randSuffix) || len(randSuffix) > 100 {
		return nil, pathErr("io_error", "invalid atomic receiver path or tmp nonce")
	}
	f, pending, err := OpenForWriteAtomic(vp, randSuffix)
	if err != nil {
		return nil, err
	}
	// Transfer the existing descriptor, without dup or a pathname reopen. The
	// legacy pending object must no longer close the descriptor now owned here.
	r := &Atomic{
		changed: make(chan struct{}), tmp: f,
		parent:     os.NewFile(uintptr(pending.dirFD), pending.parent),
		parentPath: pending.parent, ops: defaultAtomicOperations(),
		state: xfercontract.AtomicState{
			Phase: xfercontract.PhaseReceiving, TmpName: pending.tmpName,
			Destination: pending.dstName, Force: force,
			Tmp:    xfercontract.ResourceRef{ResourceID: "xfer-" + randSuffix + "-tmp", Name: xfercontract.TmpFileName, Kind: xfercontract.ResourceTmp},
			Parent: xfercontract.ResourceRef{ResourceID: "xfer-" + randSuffix + "-parent", Name: xfercontract.ParentFileName, Kind: xfercontract.ResourceParent},
		},
	}
	pending.done = true
	return r, nil
}

func atomicToken(s string) bool {
	if len(s) == 0 || len(s) > 128 {
		return false
	}
	for _, c := range s {
		switch {
		case c >= 'a' && c <= 'z', c >= 'A' && c <= 'Z', c >= '0' && c <= '9', c == '-', c == '_', c == '.':
		default:
			return false
		}
	}
	return s != "." && s != ".."
}

func atomicBasename(s string) bool {
	return s != "" && s != "." && s != ".." && filepath.Base(s) == s && !strings.ContainsAny(s, "/\\\x00")
}

// SetAtomicObserver binds the outer owner's custody adapter before IO. Events
// contain references and outcomes, never paths or file content. P28 supplies
// the independent kernel witness; fingerprint equality is not an OFD oracle.
func SetAtomicObserver(receiver AtomicReceiver, observe func(xfercontract.Observation)) error {
	r, ok := receiver.(*Atomic)
	if !ok || r == nil {
		return errors.New("transferpath: foreign atomic receiver")
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.active {
		return pathErr(xfercontract.CodeCommitInflight, "atomic IO is in flight")
	}
	r.observe = observe
	if r.imported {
		r.emit(xfercontract.ObserveImport, nil)
	}
	return nil
}

func (r *Atomic) emit(kind xfercontract.ObservationKind, err error) {
	if r.observe == nil {
		return
	}
	r.eventSeq++
	now, _ := controlbudget.Now()
	var errno unix.Errno
	_ = errors.As(err, &errno)
	g, o := r.state.CommitGrant, r.state.PrimitiveOutcome
	r.observe(xfercontract.Observation{
		Kind: kind, PID: os.Getpid(), EventSeq: r.eventSeq, BoottimeNS: now,
		TransferID: g.Identity.TransferID, EntryNonce: g.Identity.EntryNonce,
		ObjectVersion: g.Identity.ObjectVersion, GrantNonce: g.GrantNonce,
		Phase: r.state.Phase, DurableOffset: r.state.DurableOffset,
		Primitive: o.Primitive, Outcome: o.Outcome, Errno: int32(errno),
		ParentSynced: o.ParentSynced, DirectorySynced: o.ParentSynced,
		FileSynced:      kind == xfercontract.ObserveSynced,
		AdmissionClosed: kind == xfercontract.ObserveFreeze || kind == xfercontract.ObservePrimitiveEnter || kind == xfercontract.ObservePrimitiveExit,
		Tmp:             r.state.Tmp, Parent: r.state.Parent, Source: r.state.Source,
	})
}

func (r *Atomic) beginIO() error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.closed || r.stopping {
		return pathErr(xfercontract.CodeCanceled, "atomic receiver gate is closed")
	}
	if r.frozen || r.active {
		return pathErr(xfercontract.CodeOwnerRecovering, "atomic receiver is quiescing")
	}
	if r.tmp == nil || r.state.PrimitiveOutcome.Outcome != "" {
		return pathErr("io_error", "atomic receiver no longer accepts bytes")
	}
	r.active = true
	return nil
}

func (r *Atomic) endIO() {
	r.mu.Lock()
	r.active = false
	close(r.changed)
	r.changed = make(chan struct{})
	r.mu.Unlock()
}

// WriteAt only extends the current contiguous prefix. A short write advances
// by the kernel's n even when err is nonnil; no requested-length checkpoint is
// manufactured. The receiver actor owns hash/seq confirmation after Sync.
// origin: hot-update S7 plan §3 / T10
func (r *Atomic) WriteAt(p []byte, off int64) (int, error) {
	if err := r.beginIO(); err != nil {
		return 0, err
	}
	defer r.endIO()
	if off != r.written || off < 0 || int64(len(p)) > int64(^uint64(0)>>1)-off {
		return 0, pathErr("io_error", "write does not extend the atomic prefix")
	}
	// An inline-bound receiver accepts only a Tier A sized prefix; refusing
	// before the syscall keeps a large file from ever reaching its tmp.
	// origin: hot-update S7 plan §8.1 / INTEG-D Tier A grant regression
	if r.objectBound && r.objectVersion == "" && off+int64(len(p)) > inlineAtomicMaxBytes {
		return 0, pathErr(xfercontract.CodePayloadUnsupported, "inline receiver exceeds the tier A ceiling")
	}
	n, err := r.ops.write(r.tmp, p, off)
	if n > 0 {
		r.written += int64(n)
		r.needsRepair = true
	}
	return n, err
}

func (r *Atomic) Sync() error {
	if err := r.beginIO(); err != nil {
		return err
	}
	defer r.endIO()
	if err := r.ops.sync(r.tmp); err != nil {
		return err
	}
	r.state.DurableOffset = r.written
	r.needsRepair = false
	r.emit(xfercontract.ObserveSynced, nil)
	return nil
}

// waitIdle is called with mu held and returns with it held. Waiting on the
// completion channel spends the original BOOTTIME budget, including contention.
func (r *Atomic) waitIdle(deadline controlbudget.Deadline) error {
	for r.active {
		remaining, err := controlbudget.Remaining(deadline)
		if err != nil {
			return err
		}
		changed := r.changed
		timer := time.NewTimer(remaining)
		r.mu.Unlock()
		select {
		case <-changed:
		case <-timer.C:
		}
		timer.Stop()
		r.mu.Lock()
	}
	_, err := controlbudget.Remaining(deadline)
	return err
}

// Freeze repairs only on the OLD owner. The repair remains an owned active IO
// if its filesystem blocks; timing out never exports a partial prefix or drops
// that goroutine/FD responsibility. A subsequent Thaw joins before reopening.
// origin: hot-update S7 plan §3 / T10
func (r *Atomic) Freeze(deadline controlbudget.Deadline) (xfercontract.AtomicState, []handoff.Resource, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if _, err := controlbudget.Remaining(deadline); err != nil {
		return xfercontract.AtomicState{}, nil, err
	}
	r.frozen = true
	if err := r.waitIdle(deadline); err != nil {
		return xfercontract.AtomicState{}, nil, err
	}
	if r.needsRepair && r.tmp != nil {
		r.active = true
		go func() {
			err := r.ops.truncate(r.tmp, r.state.DurableOffset)
			if err == nil {
				r.written = r.state.DurableOffset
				err = r.ops.sync(r.tmp)
			}
			r.repairErr = err
			if err == nil {
				r.needsRepair = false
			}
			r.endIO()
		}()
		if err := r.waitIdle(deadline); err != nil {
			return xfercontract.AtomicState{}, nil, err
		}
	}
	if r.repairErr != nil {
		return xfercontract.AtomicState{}, nil, r.repairErr
	}
	if r.closeErr != nil && (r.tmp != nil || r.parent != nil || r.source != nil) {
		// AtomicState has no separate consumed-close evidence beside a pending
		// directory/unlink error. Keep the current owner rather than allowing
		// import to lose that independent uncertainty during settlement.
		return xfercontract.AtomicState{}, nil, pathErr(xfercontract.CodeReconcileRequired, "atomic close evidence cannot yet be exported")
	}
	resources, err := r.snapshotResources()
	if err != nil {
		return xfercontract.AtomicState{}, nil, err
	}
	r.emit(xfercontract.ObserveFreeze, nil)
	return r.state, resources, nil
}

// ThawAtomic is the exec-failure path. It cannot reopen a canceled gate or
// abandon an IO still owned by a timed-out Freeze.
func ThawAtomic(receiver AtomicReceiver, deadline controlbudget.Deadline) error {
	r, ok := receiver.(*Atomic)
	if !ok || r == nil {
		return errors.New("transferpath: foreign atomic receiver")
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if err := r.waitIdle(deadline); err != nil {
		return err
	}
	if r.stopping || r.closed {
		return pathErr(xfercontract.CodeCanceled, "atomic receiver gate is closed")
	}
	r.frozen, r.repairErr = false, nil
	return nil
}

// grantAdmissible separates Tier A from Tier B. A receiver whose owner declared
// its object accepts only that exact version; an inline binding additionally
// needs a Tier A sized prefix. Without a declaration the receiver keeps the
// original Tier B rule: an empty version never authorizes a primitive, so a
// forgotten binding fails closed instead of letting "" bypass the fixed NUID.
// Called with mu held.
// origin: hot-update S7 plan §8.1 (Tier B requires the permit’s fixed, nonempty NUID) / INTEG-D
func (r *Atomic) grantAdmissible(g xfercontract.CommitGrant) bool {
	if !validAtomicGrant(g) {
		return false
	}
	version := g.Identity.ObjectVersion
	if !r.objectBound {
		return version != ""
	}
	return version == r.objectVersion && (version != "" || r.written <= inlineAtomicMaxBytes)
}

// BindObjectVersion declares the object incarnation of this receiver's
// transfer: the permit's fixed Tier B NUID, or "" for the Tier A inline path.
// The declaration is immutable; rebinding the same value is idempotent, so a
// reentered owner may repeat it. An imported receiver must be bound again by
// the owner that knows the identity (the frozen AtomicState has no such field);
// one imported with a retained Grant is already bound to that Grant's version.
// origin: hot-update S7 plan §8.1 / INTEG-D Tier A grant regression
func (r *Atomic) BindObjectVersion(version string) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if version != "" && !atomicToken(version) {
		return pathErr(xfercontract.CodeIntentConflict, "invalid atomic object version")
	}
	if r.objectBound {
		if r.objectVersion != version {
			return pathErr(xfercontract.CodeObjectVersionMismatch, "atomic receiver is bound to another object version")
		}
		return nil
	}
	if r.state.CommitGrant != (xfercontract.CommitGrant{}) && r.state.CommitGrant.Identity.ObjectVersion != version {
		return pathErr(xfercontract.CodeObjectVersionMismatch, "atomic receiver already holds another object's grant")
	}
	if version == "" && r.written > inlineAtomicMaxBytes {
		return pathErr(xfercontract.CodePayloadUnsupported, "atomic prefix exceeds the tier A ceiling")
	}
	r.objectBound, r.objectVersion = true, version
	return nil
}

// Commit serializes the primitive and its outcome with Freeze/Stop. Once a
// primitive returned (including errno), retries return that decision. A landed
// link with failed unlink, or failed parent Sync, retries only settlement.
// origin: hot-update S7 plan §3–§4 / T11
func (r *Atomic) Commit(grant xfercontract.CommitGrant, deadline controlbudget.Deadline) xfercontract.ReceiverOutcome {
	r.mu.Lock()
	if r.active {
		r.mu.Unlock()
		return xfercontract.ReceiverOutcome{Grant: grant, Outcome: xfercontract.OutcomeInflight, Code: xfercontract.CodeCommitInflight}
	}
	if r.state.CommitGrant != (xfercontract.CommitGrant{}) && r.state.CommitGrant != grant {
		r.mu.Unlock()
		return xfercontract.ReceiverOutcome{Grant: grant, Code: xfercontract.CodeIntentConflict, Error: "commit grant changed"}
	}
	if r.state.PrimitiveOutcome.Outcome != "" && ((!r.state.DirSyncPending && !r.state.UnlinkPending) || r.stopping || r.closed || r.frozen) {
		out := r.state.PrimitiveOutcome
		r.mu.Unlock()
		return out
	}
	if r.stopping || r.closed || r.frozen || !r.grantAdmissible(grant) {
		code := xfercontract.CodeOwnerRecovering
		outcome := xfercontract.PrimitiveOutcome("")
		if r.stopping || r.closed {
			code = xfercontract.CodeCanceled
			outcome = xfercontract.OutcomeNotEntered
		} else if !r.grantAdmissible(grant) {
			code = xfercontract.CodeIntentConflict
		}
		r.mu.Unlock()
		// Quiescing or an invalid Grant is unresolved, not the irreversible
		// closed-gate proof on which the tracker could safely decide failed.
		return xfercontract.ReceiverOutcome{Grant: grant, Outcome: outcome, Code: code, Error: "commit admission refused"}
	}
	if _, err := controlbudget.Remaining(deadline); err != nil {
		if r.state.PrimitiveOutcome.Outcome == xfercontract.OutcomeLanded {
			// A settlement attempt expiring cannot revoke a known filesystem
			// effect. Preserve the original outcome, even after Stop/Freeze.
			out := r.state.PrimitiveOutcome
			r.mu.Unlock()
			return out
		}
		r.stopping = true
		r.state.CommitGrant = grant
		r.state.Phase = xfercontract.PhaseDecided
		r.state.PrimitiveOutcome = xfercontract.ReceiverOutcome{Grant: grant, Outcome: xfercontract.OutcomeNotEntered, Code: xfercontract.CodeBudgetExceeded, Error: err.Error()}
		out := r.state.PrimitiveOutcome
		r.mu.Unlock()
		return out
	}
	r.active = true
	r.mu.Unlock()
	defer r.endIO()
	if r.state.PrimitiveOutcome.Outcome == "" {
		r.state.CommitGrant = grant
		r.enterPrimitive(deadline)
	}
	if r.state.PrimitiveOutcome.Outcome == xfercontract.OutcomeLanded {
		r.settleLanded()
	}
	return r.state.PrimitiveOutcome
}

func (r *Atomic) enterPrimitive(deadline controlbudget.Deadline) {
	o := xfercontract.ReceiverOutcome{Grant: r.state.CommitGrant, Outcome: xfercontract.OutcomeNotEntered, Bytes: r.state.DurableOffset}
	if r.state.Force {
		o.Primitive = xfercontract.PrimitiveRename
	} else {
		o.Primitive = xfercontract.PrimitiveLink
	}
	r.state.PrimitiveOutcome = o
	r.state.Phase = xfercontract.PhaseDecided
	err := r.checkCommitPaths()
	if err == nil && (r.needsRepair || r.written != r.state.DurableOffset) {
		err = pathErr("io_error", "atomic prefix has not been synced")
	}
	if err == nil {
		if _, budgetErr := controlbudget.Remaining(deadline); budgetErr != nil {
			err = pathErr(xfercontract.CodeBudgetExceeded, "atomic commit deadline expired before primitive")
		}
	}
	if err != nil {
		r.state.PrimitiveOutcome.Code, r.state.PrimitiveOutcome.Error = atomicErrorCode(err), err.Error()
		return
	}
	dirFD, _ := atomicDescriptor(r.parent)
	r.state.PrimitiveOutcome.Outcome = xfercontract.OutcomeInflight
	r.emit(xfercontract.ObservePrimitiveEnter, nil)
	if r.state.Force {
		err = r.ops.rename(dirFD, r.state.TmpName, dirFD, r.state.Destination)
	} else {
		err = r.ops.link(dirFD, r.state.TmpName, dirFD, r.state.Destination, 0)
	}
	if err != nil {
		r.state.PrimitiveOutcome.Outcome = xfercontract.OutcomeNotEntered
		r.state.PrimitiveOutcome.Code, r.state.PrimitiveOutcome.Error = "io_error", err.Error()
		if errors.Is(err, unix.EEXIST) {
			r.state.PrimitiveOutcome.Code = "dst_exists"
		}
	} else {
		// Record landed BEFORE any cleanup. Failure of a later syscall cannot
		// turn this link into an unattempted primitive on import or retry.
		r.state.PrimitiveOutcome.Outcome = xfercontract.OutcomeLanded
		r.state.DirSyncPending = true
		r.state.UnlinkPending = !r.state.Force
	}
	r.emit(xfercontract.ObservePrimitiveExit, err)
}

func atomicErrorCode(err error) string {
	var pve *PathValidationError
	if errors.As(err, &pve) {
		return pve.Code
	}
	return "io_error"
}

func (r *Atomic) settleLanded() {
	o := &r.state.PrimitiveOutcome
	var cleanupErr, syncErr error
	if r.state.UnlinkPending {
		dirFD, err := atomicDescriptor(r.parent)
		if err == nil {
			err = r.ops.unlink(dirFD, r.state.TmpName, 0)
		}
		if err == nil || errors.Is(err, unix.ENOENT) {
			r.state.UnlinkPending = false
			r.state.DirSyncPending = true
			o.ParentSynced = false
		} else {
			cleanupErr = err
		}
	}
	if r.state.DirSyncPending {
		syncErr = r.ops.sync(r.parent)
		if syncErr == nil {
			r.state.DirSyncPending, o.ParentSynced = false, true
		}
		r.emit(xfercontract.ObserveDirectorySync, syncErr)
	}
	o.TmpCleanupPending = r.state.UnlinkPending
	// A tmp FD is no longer needed after rename, or successful unlink. Keep
	// the parent through durability/cleanup uncertainty, never reopen it.
	if !r.state.UnlinkPending {
		r.closeFile(&r.tmp)
	}
	r.closeFile(&r.source)
	if !r.state.UnlinkPending && !r.state.DirSyncPending {
		r.closeFile(&r.parent)
	}
	o.Code, o.Error = "", ""
	if syncErr != nil {
		o.Code, o.Error = xfercontract.CodeDurabilityUnknown, syncErr.Error()
	} else if err := errors.Join(cleanupErr, r.closeErr); err != nil {
		o.Code, o.Error = xfercontract.CodeReconcileRequired, err.Error()
	}
	r.state.Phase = xfercontract.PhaseTerminalPending
	if r.state.UnlinkPending || r.state.DirSyncPending {
		r.state.Phase = xfercontract.PhaseCleanupPending
	}
}

func (r *Atomic) closeFile(file **os.File) {
	if *file != nil {
		// os.File.Close consumes its descriptor even when it reports an error.
		// Retrying that close risks closing a recycled descriptor. Preserve the
		// error separately from the vanished data-plane resource.
		r.closeErr = errors.Join(r.closeErr, r.ops.close(*file))
		*file = nil
	}
}

// Stop wins admission irreversibly before joining. A late Grant cannot race a
// cancellation into rename. If rename already won, Stop retains that outcome
// and attempts only its directory/cleanup responsibility.
// origin: hot-update S7 plan §3–§4 / T11
func (r *Atomic) Stop(deadline controlbudget.Deadline) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.stopping = true
	if err := r.waitIdle(deadline); err != nil {
		return err
	}
	if r.closed {
		return r.closeErr
	}
	r.active = true
	go func() {
		if r.state.PrimitiveOutcome.Outcome == xfercontract.OutcomeLanded {
			if r.state.DirSyncPending || r.state.UnlinkPending {
				r.settleLanded()
			}
		} else {
			if r.state.PrimitiveOutcome.Outcome == "" {
				r.state.PrimitiveOutcome = xfercontract.ReceiverOutcome{Outcome: xfercontract.OutcomeNotEntered, Code: xfercontract.CodeCanceled, Bytes: r.state.DurableOffset}
			}
			if r.tmp != nil && r.parent != nil {
				fd, err := atomicDescriptor(r.parent)
				if err == nil {
					err = r.ops.unlink(fd, r.state.TmpName, 0)
				}
				if err != nil && !errors.Is(err, unix.ENOENT) {
					r.state.UnlinkPending = true
					r.state.PrimitiveOutcome.TmpCleanupPending = true
					r.state.Phase = xfercontract.PhaseCleanupPending
					r.endIO()
					return
				}
			}
			r.state.UnlinkPending = false
			r.state.PrimitiveOutcome.TmpCleanupPending = false
			r.closeFile(&r.tmp)
			r.closeFile(&r.source)
			r.closeFile(&r.parent)
			r.state.Phase = xfercontract.PhaseTerminalPending
		}
		r.mu.Lock()
		r.closed = r.tmp == nil && r.parent == nil && r.source == nil
		r.mu.Unlock()
		r.endIO()
	}()
	if err := r.waitIdle(deadline); err != nil {
		return err
	}
	if r.state.DirSyncPending || r.state.UnlinkPending {
		return pathErr(xfercontract.CodeReconcileRequired, "atomic cleanup remains pending")
	}
	return r.closeErr
}

// DiscardAtomic closes the old owner's originals after a duplicate-based
// import, without unlinking the tmp that now belongs to the imported owner.
// Exec failure instead uses ThawAtomic and keeps these originals.
func DiscardAtomic(receiver AtomicReceiver, deadline controlbudget.Deadline) error {
	r, ok := receiver.(*Atomic)
	if !ok || r == nil {
		return errors.New("transferpath: foreign atomic receiver")
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if !r.frozen {
		return errors.New("transferpath: discard requires a frozen atomic receiver")
	}
	if err := r.waitIdle(deadline); err != nil {
		return err
	}
	r.closeFile(&r.tmp)
	r.closeFile(&r.parent)
	r.closeFile(&r.source)
	r.closed, r.stopping = true, true
	return r.closeErr
}

func (r *Atomic) snapshotResources() ([]handoff.Resource, error) {
	var resources []handoff.Resource
	for _, item := range []struct {
		file     *os.File
		ref      *xfercontract.ResourceRef
		identity *xfercontract.FileIdentity
	}{{r.parent, &r.state.Parent, &r.state.ParentIdentity}, {r.tmp, &r.state.Tmp, &r.state.TmpIdentity}, {r.source, &r.state.Source, &r.state.SourceIdentity}} {
		if item.file == nil {
			*item.ref, *item.identity = xfercontract.ResourceRef{}, xfercontract.FileIdentity{}
			continue
		}
		identity, size, err := atomicIdentity(item.file)
		if err != nil {
			return nil, err
		}
		if item.file == r.tmp && size != r.state.DurableOffset {
			return nil, errors.New("transferpath: tmp size differs from synced prefix")
		}
		*item.identity = identity
		resources = append(resources, handoff.Resource{ID: item.ref.ResourceID, Kind: item.ref.Kind, Files: map[string]*os.File{item.ref.Name: item.file}})
	}
	return resources, nil
}

// These checks never Seek, repair descriptor flags or reopen inherited files.
func atomicDescriptor(f *os.File) (int, error) {
	if f == nil {
		return -1, errors.New("transferpath: missing atomic descriptor")
	}
	raw, err := f.SyscallConn()
	if err != nil {
		return -1, err
	}
	fd := -1
	err = raw.Control(func(value uintptr) { fd = int(value) })
	return fd, err
}

func atomicIdentity(f *os.File) (xfercontract.FileIdentity, int64, error) {
	fd, err := atomicDescriptor(f)
	if err != nil {
		return xfercontract.FileIdentity{}, 0, err
	}
	var st unix.Stat_t
	if err := unix.Fstat(fd, &st); err != nil {
		return xfercontract.FileIdentity{}, 0, err
	}
	flags, err := unix.FcntlInt(uintptr(fd), unix.F_GETFL, 0)
	if err != nil {
		return xfercontract.FileIdentity{}, 0, err
	}
	cloexec, err := unix.FcntlInt(uintptr(fd), unix.F_GETFD, 0)
	if err != nil {
		return xfercontract.FileIdentity{}, 0, err
	}
	if cloexec&unix.FD_CLOEXEC == 0 {
		return xfercontract.FileIdentity{}, 0, fmt.Errorf("transferpath: descriptor is not CLOEXEC")
	}
	return xfercontract.FileIdentity{Dev: uint64(st.Dev), Ino: st.Ino, Mode: uint32(st.Mode), Flags: int32(flags)}, st.Size, nil
}

func (r *Atomic) checkCommitPaths() error {
	if r.tmp == nil || r.parent == nil || !atomicBasename(r.state.TmpName) || !atomicBasename(r.state.Destination) {
		return pathErr("io_error", "atomic commit resources are incomplete")
	}
	fd, err := atomicDescriptor(r.parent)
	if err != nil {
		return err
	}
	if same, err := dirFDStillNamesPath(fd, r.parentPath); err != nil || !same {
		return pathErr("path_race", "atomic parent changed before commit")
	}
	identity, size, err := atomicIdentity(r.tmp)
	if err != nil || size != r.state.DurableOffset {
		return pathErr("path_race", "atomic tmp differs from synced prefix")
	}
	var tmp unix.Stat_t
	if err := unix.Fstatat(fd, r.state.TmpName, &tmp, unix.AT_SYMLINK_NOFOLLOW); err != nil || uint64(tmp.Dev) != identity.Dev || tmp.Ino != identity.Ino {
		return pathErr("path_race", "atomic tmp name no longer names the original file")
	}
	var dst unix.Stat_t
	err = unix.Fstatat(fd, r.state.Destination, &dst, unix.AT_SYMLINK_NOFOLLOW)
	if err == nil {
		if !unixModeIsRegular(uint32(dst.Mode)) {
			return pathErr("not_a_regular_file", "atomic destination is not a regular file")
		}
		if r.state.Force {
			// Apply only ordinary permission bits to the owned FD immediately
			// before commit, so the temporary upload never becomes writable by
			// another user during transfer. No pathname chmod follows rename.
			tmpFD, _ := atomicDescriptor(r.tmp)
			_ = unix.Fchmod(tmpFD, uint32(dst.Mode)&0o777)
		}
	} else if !errors.Is(err, unix.ENOENT) {
		return err
	}
	return nil
}
