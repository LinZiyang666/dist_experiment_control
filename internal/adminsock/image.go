package adminsock

import (
	"context"
	"encoding/json"
	"errors"
	"net"
	"os"
	"strings"
	"time"

	"github.com/LinZiyang666/tether/internal/controlbudget"
	"github.com/LinZiyang666/tether/internal/listenerimage"
	"github.com/LinZiyang666/tether/internal/rawsocket"
	"github.com/LinZiyang666/tether/internal/upgradebudget"
	"github.com/LinZiyang666/tether/internal/upgradetx"
	"github.com/LinZiyang666/tether/internal/upgradewire"
)

// A reading connection has no store admission yet. A dispatched business
// handler owns its commit/reply until completion. Reload owns only its armed
// reply once the cut begins; the broker must not access SQL after that boundary.
// origin: hot-update S6 plan §6.1 / §12.1 P10
type adminHandler struct {
	dispatched bool
	reload     bool
}

// ImageKeeper lends the permanent listener to the broker's cut graph. Neither
// the caller's context nor a closed Accept view owns its terminal lifetime.
// origin: hot-update S6 plan §1.2 / §6.1 / rv-fd-6
func ImageKeeper(s *Server) listenerimage.Keeper {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.keeper
}

// StartOwned leaves terminal stop with RuntimeSlot; Run cancellation is checked
// by the cut driver before exec and never closes an exported Unix pathname.
// origin: hot-update S6 plan §1.2 / rv-fd-6
func StartOwned(s *Server) error {
	return s.Start(context.Background()) // ctx-root: explicit broker terminal owner.
}

// StartInherited accepts only the supplied keeper; cold Start's filesystem
// probe, stale unlink, chmod and bind are forbidden here. The descriptor binds
// this server to the original Unix route before ownership can transfer.
// origin: hot-update S6 plan §6.1 / T19
func (s *Server) StartInherited(keeper listenerimage.Keeper, deadline controlbudget.Deadline) error {
	s.ownerMu.Lock()
	defer s.ownerMu.Unlock()
	if _, err := adminImageRemaining(deadline); err != nil {
		return err
	}
	if keeper == nil {
		return errors.New("adminsock: missing inherited keeper")
	}
	desc := keeper.Descriptor()
	if desc.Network != "unix" || desc.Path != s.path || desc.Address != s.path {
		return errors.New("adminsock: inherited listener identity mismatch")
	}
	s.mu.Lock()
	unavailable := s.closed || len(s.handlers) != 0 || (s.listener != nil && !s.fenced) || (s.keeper != nil && s.keeper != keeper)
	if s.acceptDone != nil {
		select {
		case <-s.acceptDone:
		default:
			unavailable = true
		}
	}
	if s.watcherDone != nil {
		select {
		case <-s.watcherDone:
		default:
			unavailable = true
		}
	}
	s.mu.Unlock()
	if unavailable {
		return errors.New("adminsock: server already started or closed")
	}
	if err := keeper.Resume(); err != nil {
		return err
	}
	// ctx-root: inherited admin control owner; cancellation never owns the keeper.
	s.startOwner(context.Background(), keeper.View(), keeper, nil)
	return nil
}

func (s *Server) startOwner(ctx context.Context, ln net.Listener, keeper listenerimage.Keeper, info os.FileInfo) {
	s.mu.Lock()
	s.listener, s.keeper, s.pathInfo = ln, keeper, info
	s.fenced = false
	s.reloadReplyErr = nil
	s.handlers = make(map[net.Conn]*adminHandler)
	s.changed = make(chan struct{})
	s.acceptDone = make(chan struct{})
	s.watcherDone = make(chan struct{})
	s.controlCtx = ctx
	s.readCtx, s.cancelReads = context.WithCancel(ctx)
	done, watcherDone := s.acceptDone, s.watcherDone
	s.mu.Unlock()
	// Pass the listener as a parameter so acceptLoop never reads
	// s.listener concurrently with Close (which sets it to nil). The
	// loop owns its own reference; Close still triggers exit by
	// closing the listener (Accept returns ErrClosed).
	go s.acceptLoop(ln, done)
	// Audit shard 01 F6: the inner ctx-watcher goroutine used to
	// leak when shutdown was initiated by an explicit Close()
	// (which the broker does via defer b.admin.Close()) — accept
	// returns ErrClosed cleanly, but the watcher sat forever
	// waiting on <-ctx.Done. Close a `done` channel on loop exit
	// so the watcher exits via select too.
	// Explicit Close and image Fence also wake this watcher through the Accept
	// join; cancellation of the old control root cannot close a detached keeper.
	go func() {
		defer close(watcherDone)
		select {
		case <-ctx.Done():
			_ = s.Close()
		case <-done:
		}
	}()
}

func (s *Server) notifyLocked() {
	if s.changed != nil {
		close(s.changed)
	}
	s.changed = make(chan struct{})
}

func (s *Server) readContext() context.Context {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.readCtx != nil {
		return s.readCtx
	}
	// ctx-none: direct dispatch callers have no running control owner or caller context.
	return context.Background()
}

// Fence closes only the Accept view and unadmitted sockets. Reads with a
// cancellation contract are canceled; admitted writes retain their commit and
// reply path. A failed join retains every owner for a later retry or normal stop.
// origin: hot-update S6 plan §6.1 / T19
func (s *Server) FenceImage(deadline controlbudget.Deadline) error {
	s.ownerMu.Lock()
	defer s.ownerMu.Unlock()
	if _, err := adminImageRemaining(deadline); err != nil {
		return err
	}
	s.mu.Lock()
	if s.closed || s.keeper == nil {
		s.mu.Unlock()
		return errors.New("adminsock: no current image owner")
	}
	s.fenced = true
	s.cancelReads()
	keeper := s.keeper
	idle := make([]net.Conn, 0, len(s.handlers))
	for conn, handler := range s.handlers {
		if !handler.dispatched {
			idle = append(idle, conn)
		}
	}
	s.notifyLocked()
	s.mu.Unlock()
	var err error
	for _, conn := range idle {
		err = errors.Join(err, conn.Close())
	}
	return errors.Join(err, keeper.Pause(deadline))
}

// Join is a real accepted-handler receipt, including reply writes and the
// Accept/watcher roots. The initiating reload is excluded to avoid self-join;
// JoinImageReplies is the separate DB-free reply barrier before Prepared.Exec.
// origin: hot-update S6 plan §6.1 / T19
func (s *Server) JoinImage(deadline controlbudget.Deadline) error {
	return joinAdminImage(s, deadline, false)
}

// JoinImageReplies waits for the armed reload reply after the backend returned.
// It must run from the cut driver, never from the initiating reload handler.
// origin: hot-update S6 plan §6.1 / §12.1 P10
func JoinImageReplies(s *Server, deadline controlbudget.Deadline) error {
	return joinAdminImage(s, deadline, true)
}

func joinAdminImage(s *Server, deadline controlbudget.Deadline, replies bool) error {
	for {
		remaining, err := adminImageRemaining(deadline)
		if err != nil {
			return err
		}
		s.mu.Lock()
		if !s.fenced {
			s.mu.Unlock()
			return errors.New("adminsock: image admission is not fenced")
		}
		pending := s.acceptDone
		if pending != nil {
			select {
			case <-pending:
				pending = nil
			default:
			}
		}
		if pending == nil && s.watcherDone != nil {
			select {
			case <-s.watcherDone:
			default:
				pending = s.watcherDone
			}
		}
		active := 0
		for _, handler := range s.handlers {
			if replies || !handler.reload {
				active++
			}
		}
		if pending == nil && active == 0 {
			var replyErr error
			if replies {
				replyErr = s.reloadReplyErr
			}
			s.mu.Unlock()
			return replyErr
		}
		if pending == nil {
			pending = s.changed
		}
		s.mu.Unlock()
		timer := time.NewTimer(remaining)
		select {
		case <-pending:
			timer.Stop()
		case <-timer.C:
			return errors.New("adminsock: image join deadline expired")
		}
	}
}

// Detach leaves the original LISTEN OFD and pathname with the caller. Late
// cleanup on this retired server has no authority over the replacement keeper.
// The only allowed outstanding handler is the initiating DB-free reload reply.
// origin: hot-update S6 plan §6.1 / T19
func (s *Server) DetachImage() (listenerimage.Keeper, error) {
	s.ownerMu.Lock()
	defer s.ownerMu.Unlock()
	s.mu.Lock()
	defer s.mu.Unlock()
	if !s.fenced || s.closed || s.keeper == nil {
		return nil, errors.New("adminsock: image owner is not detachable")
	}
	for _, handler := range s.handlers {
		if !handler.reload {
			return nil, errors.New("adminsock: accepted handlers are not joined")
		}
	}
	if s.acceptDone != nil {
		select {
		case <-s.acceptDone:
		default:
			return nil, errors.New("adminsock: Accept is not joined")
		}
	}
	if s.watcherDone != nil {
		select {
		case <-s.watcherDone:
		default:
			return nil, errors.New("adminsock: watcher is not joined")
		}
	}
	keeper := s.keeper
	if _, _, err := keeper.Export(); err != nil {
		return nil, err
	}
	s.keeper, s.listener, s.pathInfo = nil, nil, nil
	return keeper, nil
}

func adminImageRemaining(deadline controlbudget.Deadline) (time.Duration, error) {
	// BootNow already supplies Linux CLOCK_BOOTTIME with an unsupported-platform
	// twin. Using the existing clock avoids a wall-time conversion or a second
	// syscall implementation in this control owner.
	now, err := rawsocket.BootNow()
	if err != nil {
		return 0, err
	}
	if deadline <= 0 || int64(deadline) <= now {
		return 0, errors.New("adminsock: image deadline expired")
	}
	return time.Duration(int64(deadline) - now), nil
}

// decodeAdminRequest closes duplicate-op smuggling before selecting either
// decoder. Legacy verbs keep their old additive fields; transaction verbs have
// an exact, independent per-operation field set.
// origin: hot-update S8 plan §2.2 / §7.1 / P13
func decodeAdminRequest(body []byte) (Request, error) {
	if err := upgradewire.CheckJSON(body); err != nil {
		return Request{}, err
	}
	var request Request
	if err := json.Unmarshal(body, &request); err != nil {
		return Request{}, err
	}
	if strings.HasPrefix(request.Op, "upgrade.") {
		tx, err := upgradewire.DecodeRequest(body, upgradetx.ComponentBroker)
		return Request{Op: request.Op, Upgrade: &tx}, err
	}
	return request, nil
}

// A cut-initiating transaction retains the same DB-free reply barrier as S6
// reload. It cannot wait for its own connection while draining admin handlers.
// origin: hot-update S8 plan §4.2 / P13
func reloadRequest(request Request) bool {
	return request.Op == OpBrokerUpgradeReload || request.Op == upgradewire.OpStart || request.Op == upgradewire.OpRollback
}

func dispatchUpgrade(s *Server, request Request) Response {
	if request.Upgrade == nil || request.Op != request.Upgrade.Op {
		response := upgradewire.ErrorResponse(upgradeAdminError())
		return Response{Upgrade: &response}
	}
	body, err := upgradewire.EncodeRequest(*request.Upgrade)
	if err != nil {
		response := upgradewire.ErrorResponse(err)
		return Response{Upgrade: &response}
	}
	// The process control owner outlives Fence's read cancellation. Accepted
	// Start/Rollback retain commit+reply custody while the store cut is drained;
	// querying or preparing remains a cancellable read operation.
	// origin: hot-update S8 plan §4.2 / P13
	ctx := s.readContext()
	if reloadRequest(request) {
		s.mu.Lock()
		owner := s.controlCtx
		s.mu.Unlock()
		if owner != nil {
			ctx = owner
		}
	}
	deadline, err := controlbudget.After(upgradebudget.RequestLatency)
	if err != nil {
		response := upgradewire.ErrorResponse(err)
		return Response{Upgrade: &response}
	}
	if s.backend.UpgradeEndpoint == nil {
		response := upgradewire.Handle(ctx, nil, body, upgradewire.Access{Key: s.backend.UpgradeKey, Administrator: true})
		return Response{Upgrade: &response}
	}
	response := s.backend.UpgradeEndpoint(body, upgradewire.Access{Key: s.backend.UpgradeKey, Administrator: true}, deadline, ctx.Done())
	return Response{Upgrade: &response}
}

// Error replies use the same envelope as the request, including while intake
// is fenced. A transaction client must not mistake a legacy-shaped refusal
// for an undecodable/lost accepted result.
// origin: hot-update S8 plan §2.2 / §4.2 / P13
func invalidAdminResponse(op, detail string) Response {
	if strings.HasPrefix(op, "upgrade.") {
		response := upgradewire.ErrorResponse(&upgradetx.Error{Code: upgradetx.CodeRejected, Detail: detail})
		return Response{Upgrade: &response}
	}
	return Response{Op: op, Code: CodeBadRequest, Error: detail}
}
func fencedAdminResponse(request Request) Response {
	if request.Upgrade != nil {
		response := upgradewire.ErrorResponse(&upgradetx.Error{Code: upgradetx.CodeDeferred, Detail: "admin admission is fenced"})
		return Response{Upgrade: &response}
	}
	return Response{Op: request.Op, Code: CodeUpgradeDeferred, Error: "admin admission is fenced"}
}
