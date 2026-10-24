package ssproxy

import (
	"bytes"
	"context"
	"encoding/hex"
	"errors"
	"net"
	"net/netip"
	"os"
	"runtime"
	"sort"
	"syscall"
	"time"

	"github.com/LinZiyang666/tether/internal/drillobserve"
	"github.com/LinZiyang666/tether/internal/handoff"
	"github.com/LinZiyang666/tether/internal/rawsocket"
	"github.com/LinZiyang666/tether/internal/sealedstate"
	"golang.org/x/sys/unix"
)

type frozenState struct {
	server   *Server
	released bool
	// image.go owns the sealed cut separately. Runtime wrappers own borrowed
	// business descriptors, so releasing a cut never closes raw originals.
}

// lockServerUntil includes guard contention in the caller's one absolute cut
// budget. No detached goroutine can acquire this lock after the caller returns.
func lockServerUntil(s *Server, deadline time.Time) error {
	if deadline.IsZero() {
		return ErrStateUnrepresentable
	}
	for !s.mu.TryLock() {
		left := time.Until(deadline)
		if left <= 0 {
			return os.ErrDeadlineExceeded
		}
		timer := time.NewTimer(min(left, time.Millisecond))
		<-timer.C
	}
	if !time.Now().Before(deadline) {
		s.mu.Unlock()
		return os.ErrDeadlineExceeded
	}
	return nil
}

// Freeze pauses admission and joins the actor's outstanding IO commit. It
// never closes a business FD to obtain a safe point, and never waits for ACK.
// origin: hot-update S5 plan §5.3 / §6.1 / T22
func (s *Server) Freeze(deadline time.Time) (*Frozen, error) {
	if runtime.GOOS != "linux" {
		return nil, rawsocket.ErrUnsupported
	}
	if err := lockServerUntil(s, deadline); err != nil {
		return nil, err
	}
	st := ensureImageState(s)
	if s.closed {
		s.mu.Unlock()
		return nil, ErrStopped
	}
	if st.cut != nil || st.frozen {
		s.mu.Unlock()
		return nil, ErrStateUnrepresentable
	}
	// Resolver/dial state with no owned raw descriptor cannot be represented.
	for _, a := range st.actors {
		if a.dnsInFlight {
			s.mu.Unlock()
			return nil, ErrStateUnrepresentable
		}
	}
	st.frozen = true
	signalStateLocked(st)
	for st.workers != 0 || st.acceptRunning {
		changed := st.changed
		s.mu.Unlock()
		timer := time.NewTimer(max(0, time.Until(deadline)))
		select {
		case <-changed:
			timer.Stop()
		case <-timer.C:
			s.mu.Lock()
			st.frozen = false
			signalStateLocked(st)
			resumeWorkersLocked(s)
			s.mu.Unlock()
			return nil, os.ErrDeadlineExceeded
		}
		if err := lockServerUntil(s, deadline); err != nil {
			// Every critical section is bounded; rollback must not leave a frozen owner.
			s.mu.Lock()
			st.frozen = false
			signalStateLocked(st)
			resumeWorkersLocked(s)
			s.mu.Unlock()
			return nil, err
		}
	}
	f := &frozenState{server: s}
	st.cut = f
	s.mu.Unlock()
	return &Frozen{state: f}, nil
}

func resumeWorkersLocked(s *Server) {
	st := s.imageState
	if s.closed || st.frozen {
		return
	}
	if s.ln != nil && !st.acceptRunning {
		s.acceptExited = false
		st.acceptRunning = true
		s.wg.Add(1)
		go s.acceptLoop(s.ln)
	}
	for _, a := range st.actors {
		// A held raw wrapper belongs to the SS safe point. Release it before
		// starting the sole actor; keeping it held would reject every resumed IO.
		// origin: hot-update S5 plan §5.3 / T27
		for _, sock := range []actorSocket{a.client, a.upstream} {
			if raw, ok := sock.(*rawsocket.Socket); ok {
				_ = rawsocket.Release(raw)
			}
		}
		startActorLocked(s, a)
	}
}

func (f *Frozen) Thaw() error {
	if f == nil || f.state == nil {
		return ErrStateUnrepresentable
	}
	fs := f.state
	s := fs.server
	s.mu.Lock()
	if fs.released || s.imageState.cut != fs {
		s.mu.Unlock()
		return ErrStateUnrepresentable
	}
	fs.released = true
	st := s.imageState
	st.cut = nil
	st.frozen = false
	// An exec failure does not make the old grant fresh. Raw owners can resume
	// only after a current grant, and a revoked actor is never re-authorized.
	// origin: hot-update S5 plan §6.5 / T22
	if st.authority != AuthorityPhaseRevoked {
		st.authority = AuthorityPhasePending
	}
	for _, a := range st.actors {
		if !a.state.flag("revoked") {
			a.state.put("authority_phase", uint64(AuthorityPhasePending))
		}
	}
	exportFiles := releaseExportFilesLocked(s)
	signalStateLocked(st)
	resumeWorkersLocked(s)
	var revoked []*ssActor
	for _, a := range st.actors {
		if a.state.flag("revoked") || SSPhase(a.state.number("phase")) == SSPhaseTerminal {
			revoked = append(revoked, a)
		}
	}
	s.mu.Unlock()
	cleanupActors(s, revoked)
	for _, file := range exportFiles {
		_ = file.Close()
	}
	// P10 owns sealed cuts separately from the borrowed runtime descriptors.
	// Release outside s.mu: Export holds the image lock before SnapshotState.
	// origin: hot-update S5 plan §5.3 / T27
	return closeSSImage(f)
}

func (f *Frozen) Discard() error {
	if f == nil || f.state == nil {
		return ErrStateUnrepresentable
	}
	fs := f.state
	s := fs.server
	s.mu.Lock()
	if fs.released {
		s.mu.Unlock()
		return ErrStateUnrepresentable
	}
	fs.released = true
	s.closed = true
	releaseImportedStateLocked(s)
	s.imageState.cut = nil
	var actors []*ssActor
	for _, a := range s.imageState.actors {
		actors = append(actors, a)
	}
	ln := s.ln
	listenerFile := s.imageState.listenerFile
	s.imageState.listenerFile = nil
	if s.stopCancel != nil {
		s.stopCancel()
	}
	signalStateLocked(s.imageState)
	s.mu.Unlock()
	// Ownership transfer drops these references without SHUT_RDWR. The new
	// image owns duplicates of the same OFDs and must keep them usable.
	for _, a := range actors {
		closeActor(a)
	}
	if ln != nil {
		_ = ln.Close()
	}
	if listenerFile != nil {
		_ = listenerFile.Close()
	}
	// origin: hot-update S5 plan §5.3 / T27
	return closeSSImage(f)
}

func (s *Server) ActivateRaw() error {
	s.mu.Lock()
	st := ensureImageState(s)
	if s.closed {
		s.mu.Unlock()
		return ErrStopped
	}
	if st.cut != nil {
		s.mu.Unlock()
		return ErrStateUnrepresentable
	}
	releaseImportedStateLocked(s)
	st.proxyImported = false
	st.frozen = false
	if st.authority != AuthorityPhaseRevoked {
		st.authority = AuthorityPhasePending
	}
	if s.stopCtx == nil {
		// ctx-root: restored SS server owns its own stop cancellation.
		s.stopCtx, s.stopCancel = context.WithCancel(context.Background())
	}
	var revoked []*ssActor
	for _, a := range st.actors {
		if a.state.flag("revoked") || SSPhase(a.state.number("phase")) == SSPhaseTerminal {
			revoked = append(revoked, a)
		}
	}
	signalStateLocked(st)
	resumeWorkersLocked(s)
	s.mu.Unlock()
	cleanupActors(s, revoked)
	return nil
}

func markRevokedLocked(a *ssActor) bool {
	if a.state.flag("revoked") {
		return false
	}
	a.state.put("revoked", 1)
	a.state.put("cleanup_pending", 1)
	a.state.put("phase", uint64(SSPhaseRevoked))
	a.state.put("authority_phase", uint64(AuthorityPhaseRevoked))
	// origin: hot-update S5 plan §6.5 / T26. The irreversible barrier precedes shutdown.
	if drillobserve.Hook != nil {
		drillobserve.Hook(observedActor(a, "ss-revoke-mark"))
	}
	return true
}

// shutdownSocket invalidates the kernel OFD, including Prepared/export dup FDs.
// Closing one Go wrapper would leave a frozen victim usable after exec.
// syscall.Shutdown/Fstat/Dup here are portable Unix calls, not Linux-only ABI.
// origin: hot-update S5 plan §6.5 / T21
func shutdownSocket(sock actorSocket) {
	if sock == nil {
		return
	}
	if raw, ok := sock.(*rawsocket.Socket); ok {
		_ = raw.Shutdown()
		return
	}
	if tc, ok := sock.(*net.TCPConn); ok {
		raw, err := tc.SyscallConn()
		if err == nil {
			_ = raw.Control(func(fd uintptr) { _ = syscall.Shutdown(int(fd), syscall.SHUT_RDWR) })
		}
	}
}
func closeActor(a *ssActor) {
	if a.client != nil {
		_ = a.client.Close()
	}
	if a.upstream != nil {
		_ = a.upstream.Close()
	}
	for _, f := range a.files {
		_ = f.Close()
	}
	// Shutdown/close has reached every original wrapper before zero is observed.
	if drillobserve.Hook != nil {
		e := observedActor(a, "ss-closed")
		e.Values[7] = 1
		drillobserve.Hook(e)
	}
}
func retireActorLocked(s *Server, a *ssActor) {
	delete(s.imageState.actors, stateID(a.state.bytes("id")))
	for tuple, b := range s.imageState.bindings {
		if b.connID == stateID(a.state.bytes("id")) {
			delete(s.imageState.bindings, tuple)
		}
	}
	delete(s.allConns, a.clientConn)
	delete(s.allConns, a.upstreamConn)
	id := string(a.state.bytes("authorized_key_id"))
	set := s.keyConns[id]
	delete(set, a.clientConn)
	delete(set, a.upstreamConn)
	if len(set) == 0 {
		delete(s.keyConns, id)
	}
	if a.material != nil {
		delete(a.material.live, string(a.state.bytes("rx_salt")))
		if a.state.flag("tx_salt_reserved") {
			delete(a.material.live, string(a.state.bytes("tx_salt")))
		}
	}
}

func cleanupActors(s *Server, actors []*ssActor) {
	for _, a := range actors {
		s.mu.Lock()
		client, upstream := a.client, a.upstream
		fn := s.imageState.revoke
		id := stateID(a.state.bytes("flow_id"))
		pending := a.state.flag("cleanup_pending")
		if a.cleanupRunning {
			s.mu.Unlock()
			shutdownSocket(client)
			shutdownSocket(upstream)
			continue
		}
		a.cleanupRunning = true
		s.mu.Unlock()
		shutdownSocket(client)
		shutdownSocket(upstream)
		// Callbacks run on the API caller after dropping every SS guard. Actor
		// workers never invoke a callback that could Stop and join themselves.
		if pending && fn != nil && id != ([16]byte{}) {
			fn(id, "revoked")
		}
		s.mu.Lock()
		a.cleanupRunning = false
		if !pending || id == ([16]byte{}) || fn != nil {
			a.state.put("cleanup_pending", 0)
		}
		stopped := !a.running
		retire := stopped && !s.imageState.frozen && !a.state.flag("cleanup_pending")
		if retire {
			retireActorLocked(s, a)
		}
		signalStateLocked(s.imageState)
		s.mu.Unlock()
		if retire {
			closeActor(a)
		}
	}
}
func (s *Server) RevokeAll(reason string) error {
	s.mu.Lock()
	st := ensureImageState(s)
	st.authority = AuthorityPhaseRevoked
	s.keys = nil
	var actors []*ssActor
	for _, a := range st.actors {
		if markRevokedLocked(a) {
			actors = append(actors, a)
		}
	}
	signalStateLocked(st)
	s.mu.Unlock()
	cleanupActors(s, actors)
	return nil
}

func descriptorFile(v interface {
	SyscallConn() (syscall.RawConn, error)
}) (*os.File, rawsocket.Fingerprint, error) {
	raw, err := v.SyscallConn()
	if err != nil {
		return nil, rawsocket.Fingerprint{}, err
	}
	var fd int
	var stat syscall.Stat_t
	var inner error
	err = raw.Control(func(n uintptr) {
		if inner = syscall.Fstat(int(n), &stat); inner != nil {
			return
		}
		// Dup preserves O_NONBLOCK; unlike TCPConn.File it cannot clear that flag
		// on an OFD still owned by a live Go netpoll wrapper.
		syscall.ForkLock.RLock()
		fd, inner = syscall.Dup(int(n))
		if inner == nil {
			syscall.CloseOnExec(fd)
		}
		syscall.ForkLock.RUnlock()
	})
	if err != nil {
		return nil, rawsocket.Fingerprint{}, err
	}
	if inner != nil {
		return nil, rawsocket.Fingerprint{}, inner
	}
	return os.NewFile(uintptr(fd), "ss-owned"), rawsocket.Fingerprint{Dev: uint64(stat.Dev), Ino: uint64(stat.Ino), Type: uint32(stat.Mode) & syscall.S_IFMT}, nil
}
func fileFingerprint(f *os.File) (rawsocket.Fingerprint, error) {
	raw, err := f.SyscallConn()
	if err != nil {
		return rawsocket.Fingerprint{}, err
	}
	var stat syscall.Stat_t
	var inner error
	err = raw.Control(func(fd uintptr) { inner = syscall.Fstat(int(fd), &stat) })
	if err != nil {
		return rawsocket.Fingerprint{}, err
	}
	if inner != nil {
		return rawsocket.Fingerprint{}, inner
	}
	return rawsocket.Fingerprint{Dev: uint64(stat.Dev), Ino: uint64(stat.Ino), Type: uint32(stat.Mode) & syscall.S_IFMT}, nil
}
func socketFile(a *ssActor, name string, sock actorSocket) (*os.File, rawsocket.Fingerprint, error) {
	if f := a.files[name]; f != nil {
		fp, err := fileFingerprint(f)
		return f, fp, err
	}
	if raw, ok := sock.(*rawsocket.Socket); ok {
		// Actor IO is already joined under the SS freeze guard. Hold the raw
		// owner too before borrowing; its original file is never an owned dup.
		// origin: hot-update S5 plan §5.3 / §6.1 / T27
		if err := rawsocket.Hold(raw); err != nil {
			return nil, rawsocket.Fingerprint{}, err
		}
		f, err := rawsocket.BorrowHeldFile(raw)
		return f, raw.Fingerprint(), err
	}
	tc, ok := sock.(*net.TCPConn)
	if !ok {
		return nil, rawsocket.Fingerprint{}, ErrStateUnrepresentable
	}
	f, fp, err := descriptorFile(tc)
	if err == nil {
		a.files[name] = f
	}
	return f, fp, err
}
func putFingerprint(p privateRecord, prefix string, fp rawsocket.Fingerprint) {
	p.put(prefix+"_dev", fp.Dev)
	p.put(prefix+"_ino", fp.Ino)
	p.put(prefix+"_type", uint64(fp.Type))
}
func getFingerprint(p privateRecord, prefix string) rawsocket.Fingerprint {
	return rawsocket.Fingerprint{Dev: p.number(prefix + "_dev"), Ino: p.number(prefix + "_ino"), Type: uint32(p.number(prefix + "_type"))}
}
func resourceID(prefix string, id []byte) string { return prefix + "." + hex.EncodeToString(id) }

// SnapshotState borrows business files; only the sealing owner may place
// secret records into a memfd. No private bytes enter Resource.Data.
func (f *Frozen) SnapshotState() (sealedstate.CutState, error) {
	if f == nil || f.state == nil {
		return sealedstate.CutState{}, ErrStateUnrepresentable
	}
	fs := f.state
	s := fs.server
	s.mu.Lock()
	defer s.mu.Unlock()
	st := s.imageState
	if fs.released || !st.frozen || st.workers != 0 || st.acceptRunning {
		return sealedstate.CutState{}, ErrStateUnrepresentable
	}
	if st.id == ([16]byte{}) {
		id, err := newStateID()
		if err != nil {
			return sealedstate.CutState{}, err
		}
		st.id = id
	}
	out := sealedstate.CutState{}
	server := newPrivateRecord(sealedstate.KindSSServer)
	server.putBytes("id", st.id[:])
	server.put("authority_phase", uint64(st.authority))
	if st.authFailed {
		server.put("authority_phase", uint64(AuthorityPhasePending))
	}
	if s.allowPrivate {
		server.put("allow_private", 1)
	}
	server.put("generation", uint64(st.generation))
	server.put("epoch", uint64(st.epoch))
	server.put("applied_revision", st.revision)
	server.putBytes("keyset_digest", st.digest[:])
	if st.authority == AuthorityPhaseRevoked {
		server.put("revoked", 1)
	}
	if s.ln != nil {
		if st.listenerFile == nil {
			tc, ok := s.ln.(*net.TCPListener)
			if !ok {
				return out, ErrStateUnrepresentable
			}
			file, _, err := descriptorFile(tc)
			if err != nil {
				return out, err
			}
			st.listenerFile = file
		}
		fp, err := fileFingerprint(st.listenerFile)
		if err != nil {
			return out, err
		}
		ref := resourceID("ss-listener", st.id[:])
		server.putBytes("listener_ref", []byte(ref))
		putFingerprint(server, "listener", fp)
		server.putBytes("listener_tuple", []byte(s.ln.Addr().String()))
		out.Resources = append(out.Resources, handoff.Resource{ID: ref, Kind: "ss-listener", Files: map[string]*os.File{"listener": st.listenerFile}})
	}
	server.putBytes("keys", encodeKeyEntries(s.keys))
	actorIDs := make([][16]byte, 0, len(st.actors))
	for id := range st.actors {
		actorIDs = append(actorIDs, id)
	}
	sort.Slice(actorIDs, func(i, j int) bool { return bytes.Compare(actorIDs[i][:], actorIDs[j][:]) < 0 })
	var ids []byte
	for _, id := range actorIDs {
		a := st.actors[id]
		p := a.state
		if a.dnsInFlight {
			return out, ErrStateUnrepresentable
		}
		ref := resourceID("ss-conn", id[:])
		files := make(map[string]*os.File)
		for _, part := range []struct {
			name string
			sock actorSocket
		}{{"loopback", a.client}, {"upstream", a.upstream}} {
			if part.sock == nil {
				continue
			}
			file, fp, err := socketFile(a, part.name, part.sock)
			if err != nil {
				return out, err
			}
			files[part.name] = file
			p.putBytes(part.name+"_ref", []byte(ref))
			putFingerprint(p, part.name, fp)
			if tc, ok := part.sock.(*net.TCPConn); ok {
				p.putBytes(part.name+"_tuple", []byte(tc.LocalAddr().String()+"\n"+tc.RemoteAddr().String()))
			}
		}
		if len(files) > 0 {
			out.Resources = append(out.Resources, handoff.Resource{ID: ref, Kind: "ss-conn", Files: files})
		}
		out.Records = append(out.Records, p.clone())
		ids = append(ids, id[:]...)
	}
	server.putBytes("conn_ids", ids)
	materials, materialIDs := snapshotMaterials(s)
	out.Records = append(out.Records, materials...)
	server.putBytes("material_ids", materialIDs)
	var bindingIDs []byte
	for _, b := range st.bindings {
		p := newPrivateRecord(sealedstate.KindBinding)
		p.putBytes("id", b.id[:])
		p.putBytes("flow_id", b.id[:])
		p.putBytes("conn_id", b.connID[:])
		p.put("deadline_boot_ns", uint64(b.deadline))
		if b.consumed {
			p.put("consumed", 1)
		}
		p.putBytes("tuple", []byte(b.tuple.Local.String()+"\n"+b.tuple.Remote.String()))
		out.Records = append(out.Records, p.clone())
		bindingIDs = append(bindingIDs, b.id[:]...)
	}
	server.putBytes("binding_ids", bindingIDs)
	authority, timer, err := snapshotSSControl(s, server)
	if err != nil {
		return sealedstate.CutState{}, err
	}
	out.Records = append([]sealedstate.Record{server.clone(), authority, timer}, out.Records...)
	return out, nil
}

func parseTuple(b []byte) (rawsocket.Tuple, error) {
	parts := bytes.Split(b, []byte{'\n'})
	if len(parts) != 2 {
		return rawsocket.Tuple{}, ErrStateUnrepresentable
	}
	local, e1 := netip.ParseAddrPort(string(parts[0]))
	remote, e2 := netip.ParseAddrPort(string(parts[1]))
	if e1 != nil || e2 != nil {
		return rawsocket.Tuple{}, ErrStateUnrepresentable
	}
	return rawsocket.Tuple{Local: local, Remote: remote}, nil
}

// String deliberately reveals only an opaque type. fmt on the owner must not
// recursively dump keys, nonce state or target buffers into a public log.
func (s *Server) String() string   { return "ssproxy.Server(redacted)" }
func (f *Frozen) String() string   { return "ssproxy.Frozen(redacted)" }
func (s *Server) GoString() string { return s.String() }
func (f *Frozen) GoString() string { return f.String() }

// validateSocketFile checks the private tuple and flags without changing them
// and without reading SO_ERROR. Descriptor identity alone cannot distinguish
// an approved target from a substituted live socket on import.
// origin: hot-update S5 plan §5.1 / §6.1
func validateSocketFile(f *os.File, want []byte, listener, connecting bool) error {
	raw, err := f.SyscallConn()
	if err != nil {
		return ErrStateUnrepresentable
	}
	var failure error
	err = raw.Control(func(fd uintptr) {
		flags, e := unix.FcntlInt(fd, unix.F_GETFL, 0)
		if e != nil || flags&unix.O_NONBLOCK == 0 {
			failure = ErrStateUnrepresentable
			return
		}
		flags, e = unix.FcntlInt(fd, unix.F_GETFD, 0)
		if e != nil || flags&unix.FD_CLOEXEC == 0 {
			failure = ErrStateUnrepresentable
			return
		}
		typ, e := syscall.GetsockoptInt(int(fd), syscall.SOL_SOCKET, syscall.SO_TYPE)
		if e != nil || typ != syscall.SOCK_STREAM {
			failure = ErrStateUnrepresentable
			return
		}
		listening, e := syscall.GetsockoptInt(int(fd), syscall.SOL_SOCKET, syscall.SO_ACCEPTCONN)
		if e != nil || (listening != 0) != listener {
			failure = ErrStateUnrepresentable
			return
		}
		local, e := syscall.Getsockname(int(fd))
		if e != nil {
			failure = ErrStateUnrepresentable
			return
		}
		actual, e := socketAddress(local)
		if e != nil {
			failure = e
			return
		}
		if listener {
			expected, e := netip.ParseAddrPort(string(want))
			if e != nil || expected != actual {
				failure = ErrStateUnrepresentable
			}
			return
		}
		tuple, e := parseTuple(want)
		if e != nil || tuple.Local != actual {
			failure = ErrStateUnrepresentable
			return
		}
		remote, e := syscall.Getpeername(int(fd))
		if e != nil {
			if !connecting || !errors.Is(e, syscall.ENOTCONN) {
				failure = ErrStateUnrepresentable
			}
			return
		}
		peer, e := socketAddress(remote)
		if e != nil || peer != tuple.Remote {
			failure = ErrStateUnrepresentable
		}
	})
	if err != nil {
		return ErrStateUnrepresentable
	}
	return failure
}
func socketAddress(addr syscall.Sockaddr) (netip.AddrPort, error) {
	switch a := addr.(type) {
	case *syscall.SockaddrInet4:
		return netip.AddrPortFrom(netip.AddrFrom4(a.Addr), uint16(a.Port)), nil
	case *syscall.SockaddrInet6:
		if a.ZoneId != 0 {
			return netip.AddrPort{}, ErrStateUnrepresentable
		}
		return netip.AddrPortFrom(netip.AddrFrom16(a.Addr).Unmap(), uint16(a.Port)), nil
	default:
		return netip.AddrPort{}, ErrStateUnrepresentable
	}
}

// netpoll sockets need a duplicate to lend an os.File without File.Fd changing
// the original OFD's flags. These cut export references are reclaimed on Thaw;
// restored raw owners already expose a borrowed file and never enter this map.
func releaseExportFilesLocked(s *Server) []*os.File {
	var files []*os.File
	for _, a := range s.imageState.actors {
		for _, f := range a.files {
			files = append(files, f)
		}
		a.files = make(map[string]*os.File)
	}
	if f := s.imageState.listenerFile; f != nil {
		files = append(files, f)
		s.imageState.listenerFile = nil
	}
	return files
}
