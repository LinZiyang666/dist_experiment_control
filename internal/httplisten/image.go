package httplisten

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/http"
	"sync"
	"time"

	"github.com/LinZiyang666/tether/internal/controlbudget"
	"github.com/LinZiyang666/tether/internal/listenerimage"
	"github.com/LinZiyang666/tether/internal/rawsocket"
)

var (
	ErrImageDeadline = errors.New("httplisten: image deadline expired")
	ErrImageBusy     = errors.New("httplisten: HTTP generation has not joined")
	ErrImageStopped  = errors.New("httplisten: HTTP owner stopped")
	ErrImageHijacked = errors.New("httplisten: untracked hijacked HTTP work")
)

// serverOwner counts both handler bodies and accepted connections. Serve and
// Shutdown can return while user code still holds a database connection; only
// the body's deferred departure and ConnStateClosed constitute join receipts.
// op serializes generation changes, while mu never covers socket close or join.
// origin: hot-update S6 plan §6.1 / T20
type serverOwner struct {
	op        sync.Mutex
	mu        sync.Mutex
	keeper    listenerimage.Keeper
	handler   http.Handler
	name      string
	base      context.Context
	srv       *http.Server
	cancel    context.CancelFunc
	changed   chan struct{}
	conns     map[net.Conn]http.ConnState
	handlers  int
	hijacked  bool
	fenced    bool
	stopped   bool
	serveDone bool
	serveErr  error
	fenceErr  error
}

var _ StartImageFunc = StartImage
var _ ImageOwner = (*serverOwner)(nil)

// StartImage borrows the keeper until Stop, which is its normal terminal close.
// Fence closes only the current Accept view and accepted HTTP connections. A
// new image builds its handler from the current runtime before calling here;
// Resume reuses this handler only for a thaw of this same owner.
// origin: hot-update S6 plan §6.1 / §12.1 P09
func StartImage(k listenerimage.Keeper, handler http.Handler, name string, deadline controlbudget.Deadline) (ImageOwner, error) {
	if k == nil || handler == nil || name == "" {
		return nil, errors.New("httplisten: invalid image configuration")
	}
	if _, err := imageRemaining(deadline); err != nil {
		return nil, err
	}
	if err := k.Resume(); err != nil {
		return nil, err
	}
	base := context.Background() // ctx-root: each image owns its HTTP control generation.
	o := newServerOwner(base, handler, name)
	o.keeper = k
	o.start(k.View())
	return o, nil
}

func newServerOwner(base context.Context, handler http.Handler, name string) *serverOwner {
	return &serverOwner{base: base, handler: handler, name: name, changed: make(chan struct{})}
}

// start is called with no live old generation. The root cancels request SQL,
// not the permanent listener, so a control cut cannot destroy the LISTEN OFD.
func (o *serverOwner) start(ln net.Listener) {
	root, cancel := context.WithCancel(o.base)
	o.mu.Lock()
	o.cancel = cancel
	o.conns = make(map[net.Conn]http.ConnState)
	o.fenced, o.serveDone, o.hijacked = false, false, false
	o.serveErr, o.fenceErr = nil, nil
	o.srv = &http.Server{Handler: http.HandlerFunc(o.serveHTTP), ReadHeaderTimeout: 5 * time.Second,
		BaseContext: func(net.Listener) context.Context { return root }, ConnState: o.connState}
	srv := o.srv
	o.mu.Unlock()
	go func() {
		err := srv.Serve(ln)
		o.mu.Lock()
		if err != nil && !errors.Is(err, http.ErrServerClosed) && (!o.fenced || !errors.Is(err, net.ErrClosed)) {
			o.serveErr = fmt.Errorf("%s: serve: %w", o.name, err)
		}
		o.serveDone = true
		o.signal()
		o.mu.Unlock()
	}()
}

func (o *serverOwner) signal() { close(o.changed); o.changed = make(chan struct{}) }

func (o *serverOwner) serveHTTP(w http.ResponseWriter, r *http.Request) {
	o.mu.Lock()
	// Admission and the count increment share the fence lock. A request on an
	// existing keepalive socket can never enter business code after the cut.
	// origin: hot-update S6 plan §6.1 / T20
	o.handlers++
	fenced := o.fenced
	o.mu.Unlock()
	defer func() { o.mu.Lock(); o.handlers--; o.signal(); o.mu.Unlock() }()
	if fenced {
		w.Header().Set("Connection", "close")
		http.Error(w, "broker upgrading", http.StatusServiceUnavailable)
		return
	}
	o.handler.ServeHTTP(w, r)
}

func (o *serverOwner) connState(conn net.Conn, state http.ConnState) {
	o.mu.Lock()
	defer o.mu.Unlock()
	if state == http.StateClosed {
		delete(o.conns, conn)
	} else {
		o.conns[conn] = state
	}
	// These surfaces do not hijack. If a future handler does, its detached
	// work cannot be proved joined by net/http, so image sealing must refuse.
	if state == http.StateHijacked {
		o.hijacked = true
	}
	o.signal()
}

func (o *serverOwner) Fence(deadline controlbudget.Deadline) error {
	o.op.Lock()
	defer o.op.Unlock()
	return o.fence(deadline)
}

func (o *serverOwner) fence(deadline controlbudget.Deadline) error {
	if _, err := imageRemaining(deadline); err != nil {
		return err
	}
	o.mu.Lock()
	if o.stopped {
		o.mu.Unlock()
		return ErrImageStopped
	}
	o.fenced = true
	srv, cancel := o.srv, o.cancel
	o.signal()
	o.mu.Unlock()
	cancel()
	srv.SetKeepAlivesEnabled(false)
	pauseErr := o.keeper.Pause(deadline)
	closeErr := srv.Close()
	o.closeConnections()
	o.mu.Lock()
	o.fenceErr = errors.Join(pauseErr, closeErr)
	err := o.fenceErr
	o.mu.Unlock()
	return err
}

func (o *serverOwner) closeConnections() {
	o.mu.Lock()
	conns := make([]net.Conn, 0, len(o.conns))
	for conn := range o.conns {
		conns = append(conns, conn)
	}
	o.mu.Unlock()
	for _, conn := range conns {
		_ = conn.Close()
	}
}

func (o *serverOwner) joined() bool { return o.serveDone && o.handlers == 0 && len(o.conns) == 0 }

func (o *serverOwner) Join(deadline controlbudget.Deadline) error {
	for {
		remaining, err := imageRemaining(deadline)
		if err != nil {
			return err
		}
		o.mu.Lock()
		if !o.fenced {
			o.mu.Unlock()
			return ErrImageBusy
		}
		if o.hijacked {
			o.mu.Unlock()
			return ErrImageHijacked
		}
		if o.joined() {
			err := errors.Join(o.fenceErr, o.serveErr)
			o.mu.Unlock()
			return err
		}
		changed := o.changed
		o.mu.Unlock()
		// A short clock recheck also accounts for suspend: Go timers use a
		// different monotonic clock from the cut's absolute CLOCK_BOOTTIME.
		timer := time.NewTimer(min(remaining, 25*time.Millisecond))
		select {
		case <-changed:
		case <-timer.C:
		}
		timer.Stop()
	}
}

func (o *serverOwner) Resume() error {
	o.op.Lock()
	defer o.op.Unlock()
	o.mu.Lock()
	if o.stopped {
		o.mu.Unlock()
		return ErrImageStopped
	}
	if !o.fenced {
		o.mu.Unlock()
		return nil
	}
	if !o.joined() || o.hijacked {
		o.mu.Unlock()
		return ErrImageBusy
	}
	o.mu.Unlock()
	if err := o.keeper.Resume(); err != nil {
		return err
	}
	o.start(o.keeper.View())
	return nil
}

func (o *serverOwner) Stop(deadline controlbudget.Deadline) error {
	o.op.Lock()
	defer o.op.Unlock()
	o.mu.Lock()
	stopped := o.stopped
	alreadyJoined := o.fenced && o.joined() && o.fenceErr == nil
	o.mu.Unlock()
	if stopped {
		return o.Join(deadline)
	}
	if _, err := imageRemaining(deadline); err != nil {
		return err
	}
	// A pristine detached generation already proved its view and handlers
	// joined. Re-fencing its relinquished keeper would mistake ErrClosed for
	// a failed drain; its harmless Close must not touch the new owner.
	// origin: hot-update S6 plan §6.1 / T20
	var fenceErr error
	if !alreadyJoined {
		fenceErr = o.fence(deadline)
	}
	joinErr := o.Join(deadline)
	// Even a timed-out handler remains counted. Normal shutdown releases its
	// listener once, and returns the failed join rather than claiming a seal.
	closeErr := o.keeper.Close()
	o.mu.Lock()
	o.stopped = true
	o.mu.Unlock()
	return errors.Join(fenceErr, joinErr, closeErr)
}

func imageRemaining(deadline controlbudget.Deadline) (time.Duration, error) {
	now, err := rawsocket.BootNow()
	if err != nil {
		return 0, err
	}
	if deadline <= 0 || int64(deadline) <= now {
		return 0, ErrImageDeadline
	}
	return time.Duration(int64(deadline) - now), nil
}

// serveTracked preserves the cold API's graceful drain while joining the
// cancellation callback and tracking handler/connection departure. A failed
// drain is observable; returning from Serve is never a substitute for it.
func serveTracked(ctx context.Context, ln net.Listener, handler http.Handler, name string) error {
	o := newServerOwner(ctx, handler, name)
	o.start(ln)
	callbackDone := make(chan struct{})
	var shutdownErr error
	stop := context.AfterFunc(ctx, func() {
		defer close(callbackDone)
		o.mu.Lock()
		o.fenced = true
		o.signal()
		o.mu.Unlock()
		shCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), shutdownGrace)
		defer cancel()
		shutdownErr = o.srv.Shutdown(shCtx)
		if shutdownErr != nil {
			_ = o.srv.Close()
		}
	})
	for {
		o.mu.Lock()
		done, changed := o.serveDone, o.changed
		o.mu.Unlock()
		if done {
			break
		}
		<-changed
	}
	if !stop() {
		<-callbackDone
	}
	o.cancel()
	_ = o.srv.Close()
	o.closeConnections()
	joinCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), shutdownGrace)
	defer cancel()
	for {
		o.mu.Lock()
		joined, changed, serveErr := o.joined(), o.changed, o.serveErr
		o.mu.Unlock()
		if joined {
			return errors.Join(serveErr, shutdownErr)
		}
		select {
		case <-changed:
		case <-joinCtx.Done():
			return errors.Join(serveErr, shutdownErr, joinCtx.Err())
		}
	}
}
