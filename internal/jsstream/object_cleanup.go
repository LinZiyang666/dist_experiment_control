package jsstream

import (
	"context"
	"errors"
	"sort"
	"sync"
	"time"

	"github.com/LinZiyang666/tether/internal/controlbudget"
	"github.com/LinZiyang666/tether/internal/xfercontract"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"github.com/nats-io/nuid"
)

// The limit is per connection, not per reader: duplicate commit deliveries or
// simultaneous transfers must not add their four-message windows together.
// Dirty connections and unresolved consumer deletes outlive their last reader.
// origin: hot-update S7 plan §3 / T07
var objectWindows = struct {
	sync.Mutex
	byConn map[*nats.Conn]*objectWindow
	owners map[string]*objectWindow
}{byConn: make(map[*nats.Conn]*objectWindow), owners: make(map[string]*objectWindow)}

type objectWindow struct {
	gate           chan struct{}
	refs           int
	identity       string
	needsBarrier   bool // guarded by objectWindows; gate serializes the actual barrier
	deletes        map[string]ConsumerDelete
	cleanupRunning bool
	cleanupDone    chan struct{}
	nc             *nats.Conn // retained until the last deletion is confirmed
	management     jetstream.JetStream
	lifecycle      chan struct{}
	paused         bool
	cleanupCancel  context.CancelFunc
}

// ConsumerDelete is a server responsibility, never a socket or SDK handle.
// Keep it in the sealed transfer image, including duties with no live reader.
// origin: S7 internal review rv-jsnats F3
type ConsumerDelete = xfercontract.ConsumerDelete

// ConsumerCleanupState carries one independent cleanup owner across an exec.
// Socket barriers cannot cross exec: Freeze requires those to be proven first.
type ConsumerCleanupState = xfercontract.ConsumerCleanupState

// ConsumerCleanupOwner survives the reader and its physical data connection.
// Hosts retain this handle when replacing an attachment; Bind explicitly grants
// use of a current management connection in the same account/JetStream domain.
type ConsumerCleanupOwner struct {
	window *objectWindow
}

const objectCleanupTimeout = 2 * time.Second
const objectWindowLimit = 1024

func retainObjectWindow(js jetstream.JetStream) (*objectWindow, error) {
	objectWindows.Lock()
	defer objectWindows.Unlock()
	nc := js.Conn()
	if nc.IsClosed() {
		return nil, nats.ErrConnectionClosed
	}
	// Bound all retained readers and consumer cleanup names, including many
	// readers on one socket. Cleanup can temporarily add one name per reader.
	// origin: hot-update S7 plan §3 / T07
	if objectReservationsLocked() >= objectWindowLimit {
		return nil, errors.New("transfer_capacity_exceeded: reader cleanup reservations")
	}
	w := objectWindows.byConn[nc]
	if w == nil {
		// Capacity exhaustion must refuse Open, never evict another socket's dirty guard.
		// origin: hot-update S7 plan §3 / T07
		if len(objectWindows.owners) >= objectWindowLimit {
			return nil, errors.New("transfer_capacity_exceeded: reader connection windows")
		}
		w = newObjectWindow(nuid.Next())
		w.nc, w.management = nc, js
		objectWindows.byConn[nc] = w
		objectWindows.owners[w.identity] = w
	}
	if w.paused {
		return nil, errors.New("transfer object: consumer cleanup is frozen")
	}
	w.refs++
	return w, nil
}

func newObjectWindow(identity string) *objectWindow {
	done := make(chan struct{})
	close(done)
	return &objectWindow{gate: make(chan struct{}, 1), identity: identity,
		deletes: make(map[string]ConsumerDelete), lifecycle: make(chan struct{}, 1), cleanupDone: done}
}

func objectReservationsLocked() int {
	reservations := 0
	for _, w := range objectWindows.owners {
		reservations += w.refs + len(w.deletes)
	}
	return reservations
}

// The live-socket index may disappear at physical close, but the independent
// owner index keeps both the old connection and its reservation until proof.
// origin: S7 internal review rv-jsnats F3
func retireObjectWindowLocked(w *objectWindow) {
	if w.nc != nil && w.nc.IsClosed() {
		w.needsBarrier = false
		delete(objectWindows.byConn, w.nc)
	}
	if w.refs == 0 && !w.needsBarrier && len(w.deletes) == 0 && !w.cleanupRunning && !w.paused {
		if objectWindows.byConn[w.nc] == w {
			delete(objectWindows.byConn, w.nc)
		}
		if objectWindows.owners[w.identity] == w {
			delete(objectWindows.owners, w.identity)
		}
		w.nc, w.management = nil, nil
	}
}

func releaseObjectWindow(nc *nats.Conn, w *objectWindow) {
	objectWindows.Lock()
	defer objectWindows.Unlock()
	w.refs--
	if w.needsBarrier || len(w.deletes) != 0 {
		startObjectCleanupLocked(nc, w)
	}
	retireObjectWindowLocked(w)
}

func objectWindowDirty(w *objectWindow) bool {
	objectWindows.Lock()
	defer objectWindows.Unlock()
	return w.needsBarrier
}

func setObjectWindowDirty(w *objectWindow, dirty bool) {
	objectWindows.Lock()
	w.needsBarrier = dirty
	objectWindows.Unlock()
}

// The gate is held from Fetch until consumption or UNSUB/PONG. A successful
// barrier clears only work ordered before its PING; no new Fetch overlaps it.
func drainObjectWindow(ctx context.Context, nc *nats.Conn, w *objectWindow) error {
	if !objectWindowDirty(w) {
		return nil
	}
	if err := nc.FlushWithContext(ctx); err != nil {
		return err
	}
	setObjectWindowDirty(w, false)
	return nil
}

func deleteObjectConsumer(ctx context.Context, js jetstream.JetStream, cleanup ConsumerDelete) error {
	// A 404 in a different account or a recreated stream proves nothing about
	// the original consumer. Validate the incarnation through this binding before
	// accepting consumer-not-found or issuing a destructive request.
	// origin: S7 internal review rv-jsnats F3
	stream, err := js.Stream(ctx, cleanup.Stream)
	if err != nil {
		return err
	}
	if stream.CachedInfo().Created.UTC().Format(time.RFC3339Nano) != cleanup.StreamCreated {
		return errors.New("transfer_object_version_mismatch: consumer cleanup stream incarnation changed")
	}
	err = js.DeleteConsumer(ctx, cleanup.Stream, cleanup.Consumer)
	if errors.Is(err, jetstream.ErrConsumerNotFound) {
		return nil
	}
	return err
}

func deferObjectConsumer(nc *nats.Conn, w *objectWindow, cleanup ConsumerDelete) {
	objectWindows.Lock()
	w.deletes[cleanup.Stream+"/"+cleanup.Consumer] = cleanup
	startObjectCleanupLocked(nc, w)
	objectWindows.Unlock()
}

func startObjectCleanupLocked(nc *nats.Conn, w *objectWindow) {
	if w.cleanupRunning || w.paused {
		return
	}
	w.cleanupRunning = true
	w.cleanupDone = make(chan struct{})
	// The independent lifecycle owner retains deletion after this attempt loop joins.
	// ctx-root: the connection window owns cleanup until proven drained or physically closed.
	root, cancel := context.WithCancel(context.Background())
	w.cleanupCancel = cancel
	go runObjectCleanup(root, nc, w)
}

// One owner per socket retains uncertain drains and deletes. Every attempt is
// bounded; physical Close retires the socket barrier and joins this owner even
// when no reader remains to call Close again. A live socket keeps retrying.
// The joined socket worker leaves deletion in the rebindable owner index;
// physical close never counts as a consumer-deletion receipt.
// origin: hot-update S7 plan §3 / T07
func runObjectCleanup(root context.Context, nc *nats.Conn, w *objectWindow) {
	for {
		objectWindows.Lock()
		retireObjectWindowLocked(w)
		js := w.management
		if root.Err() != nil || js == nil || js.Conn().IsClosed() || (!w.needsBarrier && len(w.deletes) == 0) {
			w.cleanupRunning = false
			// Socket closure proves its byte drain, not a server consumer delete.
			// Pending names remain owned for a later management binding/image.
			retireObjectWindowLocked(w)
			w.cleanupCancel()
			w.cleanupCancel = nil
			close(w.cleanupDone)
			// A timed-out Bind/Freeze rolls back its pause. The canceled attempt
			// still joins here before the original owner may start another loop.
			if root.Err() != nil && !w.paused && w.management != nil && !w.management.Conn().IsClosed() &&
				(w.needsBarrier || len(w.deletes) != 0) {
				startObjectCleanupLocked(w.nc, w)
			}
			objectWindows.Unlock()
			return
		}
		pending := make([]ConsumerDelete, 0, len(w.deletes))
		for _, cleanup := range w.deletes {
			pending = append(pending, cleanup)
		}
		objectWindows.Unlock()
		if nc != nil && !nc.IsClosed() {
			ctx, cancel := context.WithTimeout(root, objectCleanupTimeout)
			select {
			case w.gate <- struct{}{}:
				_ = drainObjectWindow(ctx, nc, w)
				<-w.gate
			case <-ctx.Done():
			}
			cancel()
		}
		for _, cleanup := range pending {
			ctx, cancel := context.WithTimeout(root, objectCleanupTimeout)
			err := deleteObjectConsumer(ctx, js, cleanup)
			cancel()
			if err == nil {
				objectWindows.Lock()
				delete(w.deletes, cleanup.Stream+"/"+cleanup.Consumer)
				objectWindows.Unlock()
			}
			if root.Err() != nil || js.Conn().IsClosed() {
				break
			}
		}
		// Pace unavailable cleanup without spinning or extending an individual attempt.
		timer := time.NewTimer(100 * time.Millisecond)
		select {
		case <-timer.C:
		case <-root.Done():
		}
		timer.Stop()
	}
}

// ConsumerCleanupFor finds the duty even after physical Close removed the
// live-socket index. Nil means this attachment has no remaining responsibility.
func ConsumerCleanupFor(nc *nats.Conn) *ConsumerCleanupOwner {
	objectWindows.Lock()
	defer objectWindows.Unlock()
	for _, w := range objectWindows.owners {
		if w.nc == nc {
			return &ConsumerCleanupOwner{window: w}
		}
	}
	return nil
}

// ConsumerCleanupOwners is the process cut's census, including refs-zero duties
// on closed attachments. Stop/join readers before freezing these owners; export
// every returned state into sealed storage before exec, and Bind on rollback.
func ConsumerCleanupOwners() []*ConsumerCleanupOwner {
	objectWindows.Lock()
	defer objectWindows.Unlock()
	owners := make([]*ConsumerCleanupOwner, 0, len(objectWindows.owners))
	for _, w := range objectWindows.owners {
		owners = append(owners, &ConsumerCleanupOwner{window: w})
	}
	sort.Slice(owners, func(i, j int) bool { return owners[i].window.identity < owners[j].window.identity })
	return owners
}

func cleanupDeadlineContext(deadline controlbudget.Deadline) (context.Context, context.CancelFunc, error) {
	// ctx-none: the lifecycle port receives an absolute deadline, not a caller context.
	return readerDeadlineContext(context.Background(), deadline)
}

// pauseObjectCleanup joins all requests on the old management binding before a
// replacement or export. The lifecycle permit serializes these transitions;
// objectWindows protects state only and is never held over network IO or joins.
func pauseObjectCleanup(ctx context.Context, w *objectWindow) error {
	objectWindows.Lock()
	w.paused = true
	if w.cleanupCancel != nil {
		w.cleanupCancel()
	}
	done := w.cleanupDone
	objectWindows.Unlock()
	select {
	case <-done:
		return nil
	case <-ctx.Done():
		objectWindows.Lock()
		w.paused = false
		startObjectCleanupLocked(w.nc, w)
		objectWindows.Unlock()
		return ctx.Err()
	}
}

// Bind authorizes cleanup through the current management connection. It never
// changes the data socket's UNSUB/PONG barrier or treats a new socket as proof.
// origin: S7 internal review rv-jsnats F3
func (o *ConsumerCleanupOwner) Bind(js jetstream.JetStream, deadline controlbudget.Deadline) error {
	if js == nil || js.Conn() == nil || js.Conn().IsClosed() {
		return nats.ErrConnectionClosed
	}
	ctx, cancel, err := cleanupDeadlineContext(deadline)
	if err != nil {
		return err
	}
	defer cancel()
	w := o.window
	select {
	case w.lifecycle <- struct{}{}:
		defer func() { <-w.lifecycle }()
	case <-ctx.Done():
		return ctx.Err()
	}
	if err := pauseObjectCleanup(ctx, w); err != nil {
		return err
	}
	objectWindows.Lock()
	w.management, w.paused = js, false
	startObjectCleanupLocked(w.nc, w)
	objectWindows.Unlock()
	return nil
}

// Freeze keeps the old owner and reservation intact. Failed cuts can Bind it
// again; successful exec imports the state before accepting any new readers.
// origin: S7 internal review rv-jsnats F3
func (o *ConsumerCleanupOwner) Freeze(deadline controlbudget.Deadline) (ConsumerCleanupState, error) {
	ctx, cancel, err := cleanupDeadlineContext(deadline)
	if err != nil {
		return ConsumerCleanupState{}, err
	}
	defer cancel()
	w := o.window
	select {
	case w.lifecycle <- struct{}{}:
		defer func() { <-w.lifecycle }()
	case <-ctx.Done():
		return ConsumerCleanupState{}, ctx.Err()
	}
	objectWindows.Lock()
	if w.refs != 0 {
		objectWindows.Unlock()
		return ConsumerCleanupState{}, errors.New("transfer object: cleanup export requires joined readers")
	}
	// Close admission under the same lock as retainObjectWindow, before join.
	w.paused = true
	objectWindows.Unlock()
	if err := pauseObjectCleanup(ctx, w); err != nil {
		return ConsumerCleanupState{}, err
	}
	objectWindows.Lock()
	defer objectWindows.Unlock()
	retireObjectWindowLocked(w)
	if w.needsBarrier {
		w.paused = false
		startObjectCleanupLocked(w.nc, w)
		return ConsumerCleanupState{}, errors.New("transfer object: cleanup export requires socket drain")
	}
	return objectCleanupStateLocked(w), nil
}

func objectCleanupStateLocked(w *objectWindow) ConsumerCleanupState {
	state := ConsumerCleanupState{OwnerID: w.identity, Deletes: make([]ConsumerDelete, 0, len(w.deletes))}
	for _, cleanup := range w.deletes {
		state.Deletes = append(state.Deletes, cleanup)
	}
	sort.Slice(state.Deletes, func(i, j int) bool {
		a, b := state.Deletes[i], state.Deletes[j]
		return a.Stream+"/"+a.Consumer < b.Stream+"/"+b.Consumer
	})
	return state
}

// ImportConsumerCleanup is inert until Bind. Importing an identical frozen
// owner in the current process reuses its custody (pristine rollback); a live
// owner or conflicting image cannot overwrite current deletion progress.
// origin: S7 internal review rv-jsnats F3
func ImportConsumerCleanup(state ConsumerCleanupState) (*ConsumerCleanupOwner, error) {
	if !objectChunkToken(state.OwnerID) || len(state.OwnerID) > 128 || len(state.Deletes) > objectWindowLimit {
		return nil, errors.New("transfer object: invalid cleanup image")
	}
	w := newObjectWindow(state.OwnerID)
	w.paused = true
	for _, cleanup := range state.Deletes {
		created, err := time.Parse(time.RFC3339Nano, cleanup.StreamCreated)
		if len(cleanup.Stream) > 255 || len(cleanup.Stream) < 5 || cleanup.Stream[:4] != "OBJ_" ||
			!validTransferBucket(cleanup.Stream[4:]) || !objectChunkToken(cleanup.Consumer) || len(cleanup.Consumer) > 128 ||
			err != nil || created.IsZero() || created.UTC().Format(time.RFC3339Nano) != cleanup.StreamCreated {
			return nil, errors.New("transfer object: invalid cleanup identity")
		}
		key := cleanup.Stream + "/" + cleanup.Consumer
		if _, exists := w.deletes[key]; exists {
			return nil, errors.New("transfer object: duplicate cleanup identity")
		}
		w.deletes[key] = cleanup
	}
	objectWindows.Lock()
	defer objectWindows.Unlock()
	if current := objectWindows.owners[state.OwnerID]; current != nil {
		if !current.paused || current.cleanupRunning || current.refs != 0 || current.needsBarrier || len(current.deletes) != len(w.deletes) {
			return nil, errors.New("transfer object: cleanup image conflicts with current owner")
		}
		for key, cleanup := range w.deletes {
			if current.deletes[key] != cleanup {
				return nil, errors.New("transfer object: cleanup image conflicts with current owner")
			}
		}
		return &ConsumerCleanupOwner{window: current}, nil
	}
	if len(objectWindows.owners) >= objectWindowLimit || objectReservationsLocked()+len(w.deletes) > objectWindowLimit {
		return nil, errors.New("transfer_capacity_exceeded: consumer cleanup import")
	}
	objectWindows.owners[w.identity] = w
	return &ConsumerCleanupOwner{window: w}, nil
}

// Wait joins the current attempt loop and proves that every reservation was
// released. An unavailable binding returns pending, never fake cleanup success.
func (o *ConsumerCleanupOwner) Wait(deadline controlbudget.Deadline) error {
	ctx, cancel, err := cleanupDeadlineContext(deadline)
	if err != nil {
		return err
	}
	defer cancel()
	objectWindows.Lock()
	done := o.window.cleanupDone
	objectWindows.Unlock()
	select {
	case <-done:
	case <-ctx.Done():
		return ctx.Err()
	}
	objectWindows.Lock()
	defer objectWindows.Unlock()
	if o.window.cleanupRunning || o.window.needsBarrier || len(o.window.deletes) != 0 {
		return errors.New("transfer object: consumer cleanup pending")
	}
	return nil
}

// ID is the immutable custody key used by a host's private cleanup group.
// Reading it cannot freeze another host's live reader or change its binding.
// origin: S7 internal review rv-jsnats F3 / s7-p9jsn2 Requests 1–2
func (o *ConsumerCleanupOwner) ID() string { return o.window.identity }
