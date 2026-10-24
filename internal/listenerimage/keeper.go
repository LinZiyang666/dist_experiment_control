package listenerimage

import (
	"encoding/json"
	"errors"
	"net"
	"os"
	"sync"
	"time"

	"github.com/LinZiyang666/tether/internal/controlbudget"
	"github.com/LinZiyang666/tether/internal/handoff"
)

var (
	ErrIdentity    = errors.New("listenerimage: invalid listener identity")
	ErrBusy        = errors.New("listenerimage: Accept view is active")
	ErrClosed      = errors.New("listenerimage: keeper closed")
	ErrDeadline    = errors.New("listenerimage: pause deadline expired")
	ErrUnsupported = errors.New("listenerimage: unsupported platform")
)

// The caller's cut latch protects exported borrows against Resume/Close; this
// leaf only serializes local owner operations. In particular it cannot revoke
// Prepared or salvage duplicates that the bundle owner registered elsewhere.
// A late hard revoke must enumerate those aliases before ExecCommit, rather
// than pretending that closing this keeper alone destroys a LISTEN OFD.
// origin: hot-update S6 plan §4.1 / §6.2
//
// There is no background goroutine per listener. The socket remains under
// netpoll while a view is live, and stopping a view closes its duplicate to
// wake Accept. Context cancellation must never own a keeper: the inherited
// application sockets outlive the old control root and its cancellation.
// Handler joins belong to the HTTP/admin/tunnel owners above this leaf; an
// Accept join proves admission stopped, not that accepted requests finished.
//
// Owner operations acquire k.mu before touching the current view. Accept never
// needs k.mu, so Pause can wait under that mutex without blocking the Accept
// completion it is joining. Registry operations only acquire their own lock
// beneath k.mu and never enter a keeper, preventing an inverse lock order.

// A keeper holds the original LISTEN OFD independently of netpoll's short-lived
// view. Closing a view wakes Accept by closing only its registered duplicate;
// no shutdown, rebind or flag repair is allowed on the permanent descriptor.
// origin: hot-update S6 plan §6.1 / T18
type keeper struct {
	mu   sync.Mutex
	desc Descriptor
	file *os.File
	view *acceptView
}

// The registry prevents two local terminal owners from claiming aliases of the
// same socket. Prepared duplicates remain borrowed resources, not new keepers.
// Exec starts with an empty registry; pristine Detach releases the old claim.
// origin: hot-update S6 plan §4.1 / §6.1 / T18
var keeperOwners = struct {
	sync.Mutex
	bySocket map[socketIdentity]*keeper
}{bySocket: make(map[socketIdentity]*keeper)}

type socketIdentity struct {
	device uint64
	inode  uint64
	cookie uint64
}

// Capture transfers a native TCP/Unix listener after establishing an independent
// keeper and view. On failure before the transfer, the caller still owns ln.
func Capture(ln net.Listener, id string) (Keeper, error) {
	file, desc, err := captureListener(ln, id)
	if err != nil {
		return nil, err
	}
	k, err := claimKeeper(desc, file)
	if err != nil {
		_ = file.Close()
		return nil, err
	}
	if err := k.Resume(); err != nil {
		// Failed capture must not unlink the caller's still-owned Unix socket.
		_, _ = k.Detach()
		_ = file.Close()
		return nil, err
	}
	// The source's automatic Unix unlink must be disabled before its Close
	// completes the transfer. Delay that change until the replacement is ready
	// so earlier capture failures preserve the caller's original close policy.
	protectUnixPath(ln)
	// Even a close error leaves the established keeper owned by the caller. A
	// nonnil result must be registered before inspecting its error upstream.
	return k, ln.Close()
}

func claimKeeper(desc Descriptor, file *os.File) (*keeper, error) {
	identity := listenerIdentity(desc)
	keeperOwners.Lock()
	defer keeperOwners.Unlock()
	if keeperOwners.bySocket[identity] != nil {
		return nil, ErrIdentity
	}
	// Resource IDs identify image rows, not OFDs. Two different IDs naming the
	// same dev/inode/cookie still represent conflicting terminal ownership.
	k := &keeper{desc: desc, file: file}
	keeperOwners.bySocket[identity] = k
	return k, nil
}

func listenerIdentity(desc Descriptor) socketIdentity {
	return socketIdentity{device: desc.Device, inode: desc.Inode, cookie: desc.Cookie}
}

func releaseKeeper(k *keeper) {
	keeperOwners.Lock()
	defer keeperOwners.Unlock()
	delete(keeperOwners.bySocket, listenerIdentity(k.desc))
}

// The descriptor is immutable after validation. Reading it after Detach is
// useful for salvage receipts and cannot accidentally regain file ownership.
func (k *keeper) Descriptor() Descriptor { return k.desc }

func (k *keeper) View() net.Listener {
	k.mu.Lock()
	defer k.mu.Unlock()
	if k.view == nil {
		// An imported keeper is intentionally dormant. A closed placeholder
		// satisfies net.Listener without allocating an untracked socket alias;
		// callers must fetch the new view after the explicit Resume boundary.
		return &acceptView{addr: listenerAddr(k.desc), stopped: true, done: closedViewDone()}
	}
	return k.view
}

func closedViewDone() chan struct{} {
	done := make(chan struct{})
	close(done)
	return done
}

// Pause joins every Accept that entered the old view, including accepted sockets
// racing the fence. Only those unreturned sockets are closed; application-owned
// connections and the kernel backlog survive for the next view.
// origin: hot-update S6 plan §6.1 / T18
func (k *keeper) Pause(deadline controlbudget.Deadline) error {
	// Reject invalid/expired deadlines before stopping a live generation. An
	// upstream refused cut can continue serving through that exact same view.
	// Read the clock after taking the owner lock: time queued behind another
	// owner operation also consumes the caller's original absolute budget.
	k.mu.Lock()
	defer k.mu.Unlock()
	_, err := listenerRemaining(deadline)
	if err != nil {
		return err
	}
	if k.file == nil {
		return ErrClosed
	}
	if k.view == nil {
		return nil
	}
	if err := k.view.stop(); err != nil {
		return err
	}
	// Closing a native view is synchronous; recompute the absolute remainder
	// afterwards so the close work cannot silently restart the pause budget.
	remaining, err := listenerRemaining(deadline)
	if err != nil {
		return err
	}
	timer := time.NewTimer(remaining)
	defer timer.Stop()
	select {
	case <-k.view.done:
		return nil
	case <-timer.C:
		// A timed-out join retains the stopped view and original file. Resume
		// refuses until completion; retrying Pause never drops that owner.
		return ErrDeadline
	}
}

// Export borrows only the permanent descriptor. handoff.Prepare owns its own
// duplicate; the caller must keep this keeper paused for the borrow's lifetime.
func (k *keeper) Export() (Descriptor, handoff.Resource, error) {
	k.mu.Lock()
	defer k.mu.Unlock()
	if k.file == nil {
		return Descriptor{}, handoff.Resource{}, ErrClosed
	}
	if !viewJoined(k.view) {
		return Descriptor{}, handoff.Resource{}, ErrBusy
	}
	if err := validateListener(k.desc, k.file); err != nil {
		return Descriptor{}, handoff.Resource{}, err
	}
	// Descriptor JSON contains public socket identity only. This leaf carries
	// no token, authentication state or opaque protocol buffers in its resource.
	data, err := json.Marshal(k.desc)
	if err != nil {
		return Descriptor{}, handoff.Resource{}, err
	}
	return k.desc, handoff.Resource{ID: k.desc.ID, Kind: "listener", Data: data,
		Files: map[string]*os.File{"listener": k.file}}, nil
}

func (k *keeper) Resume() error {
	k.mu.Lock()
	defer k.mu.Unlock()
	if k.file == nil {
		return ErrClosed
	}
	if k.view != nil {
		k.view.mu.Lock()
		stopped := k.view.stopped
		k.view.mu.Unlock()
		if !stopped {
			// Idempotent resume must not create a second netpoll duplicate or
			// replace a view which is already admitting on the current socket.
			return nil
		}
		if !viewJoined(k.view) {
			return ErrBusy
		}
	}
	// Validate before FileListener: netpoll is allowed to set O_NONBLOCK, but
	// an invalid inherited OFD must be refused rather than repaired by it.
	if err := validateListener(k.desc, k.file); err != nil {
		return err
	}
	ln, err := net.FileListener(k.file)
	if err != nil {
		return err
	}
	// FileListener owns a new duplicate, never k.file. Its sole terminal owner
	// is acceptView; preserve the Unix pathname when that duplicate is closed.
	protectUnixPath(ln)
	k.view = &acceptView{ln: ln, addr: ln.Addr(), done: make(chan struct{})}
	return nil
}

func viewJoined(view *acceptView) bool {
	if view == nil {
		return true
	}
	select {
	case <-view.done:
		// Completion publishes closeErr through this channel. A failed Close
		// is not proof that the duplicate was released: keep export, adoption
		// transfer and replacement views fenced until terminal cleanup.
		return view.closeErr == nil
	default:
		return false
	}
}

// Detach relinquishes a pristine paused owner without shutdown or Unix unlink.
// The returned descriptor is the only remaining registered local socket alias.
func (k *keeper) Detach() (*os.File, error) {
	k.mu.Lock()
	defer k.mu.Unlock()
	if k.file == nil {
		return nil, ErrClosed
	}
	if !viewJoined(k.view) {
		return nil, ErrBusy
	}
	file := k.file
	// Release the registry claim only when the old view has no active Accept
	// or live wrapper FD. The returned file can then be adopted once locally.
	k.file = nil
	releaseKeeper(k)
	return file, nil
}

// Close is the normal terminal owner's cleanup. A replaced pathname belongs to
// somebody else, so only a matching original Unix socket inode may be removed.
func (k *keeper) Close() error {
	k.mu.Lock()
	defer k.mu.Unlock()
	if k.file == nil {
		// Detached old owners must be harmless when captured cleanup eventually
		// runs: the new owner alone may close the transferred FD or unlink.
		return nil
	}
	var viewErr error
	if k.view != nil {
		viewErr = k.view.Close()
	}
	fileErr := k.file.Close()
	k.file = nil
	releaseKeeper(k)
	return errors.Join(viewErr, fileErr, unlinkListenerPath(k.desc))
}

// acceptView owns precisely one net.FileListener duplicate and no keeper FD.
// Its completion channel is closed only after all entered Accept calls leave.
type acceptView struct {
	mu            sync.Mutex
	once          sync.Once
	ln            net.Listener
	addr          net.Addr
	active        int
	stopped       bool
	closeFinished bool
	done          chan struct{}
	closeErr      error
}

func (v *acceptView) Accept() (net.Conn, error) {
	v.mu.Lock()
	if v.stopped {
		v.mu.Unlock()
		return nil, net.ErrClosed
	}
	v.active++
	// Count before leaving the admission lock. A concurrent stop either sees
	// this Accept and joins it, or prevents it from reaching the kernel at all.
	v.mu.Unlock()
	conn, err := v.ln.Accept()
	v.mu.Lock()
	if v.stopped {
		// A socket returned after the admission fence was never delivered to
		// the application. Close it before decrementing the joined count.
		v.mu.Unlock()
		if conn != nil {
			_ = conn.Close()
		}
		conn, err = nil, net.ErrClosed
		v.mu.Lock()
	}
	v.active--
	if v.stopped && v.closeFinished && v.active == 0 {
		close(v.done)
	}
	v.mu.Unlock()
	return conn, err
}

func (v *acceptView) stop() error {
	// once serializes physical Close and its returned error even when callers
	// concurrently invoke View.Close, Pause and the keeper's normal Close.
	v.once.Do(func() {
		v.mu.Lock()
		if v.stopped {
			v.mu.Unlock()
			return
		}
		v.stopped = true
		v.mu.Unlock()
		v.closeErr = v.ln.Close()
		// Export/Resume also observe done without calling stop. It must prove
		// both Accept completion and actual view-FD close completion, otherwise
		// a concurrent View.Close could leave an unregistered wrapper at the cut.
		v.mu.Lock()
		v.closeFinished = true
		if v.active == 0 {
			close(v.done)
		}
		v.mu.Unlock()
	})
	return v.closeErr
}

func (v *acceptView) Close() error {
	err := v.stop()
	<-v.done
	return err
}

func (v *acceptView) Addr() net.Addr { return v.addr }
