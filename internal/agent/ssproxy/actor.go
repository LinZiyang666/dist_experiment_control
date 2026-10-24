package ssproxy

import (
	"context"
	"crypto/cipher"
	"crypto/rand"
	"crypto/sha256"
	"errors"
	"io"
	"net"
	"net/netip"
	"os"
	"runtime"
	"strconv"
	"syscall"
	"time"

	"github.com/LinZiyang666/tether/internal/drillobserve"
	"github.com/LinZiyang666/tether/internal/handoff"
	"github.com/LinZiyang666/tether/internal/procrequest"
	"github.com/LinZiyang666/tether/internal/rawsocket"
	"github.com/LinZiyang666/tether/internal/sealedstate"
	"golang.org/x/crypto/chacha20poly1305"
)

const actorPollInterval = 10 * time.Millisecond

type netConn = net.Conn

type actorSocket interface {
	io.ReadWriteCloser
	SetReadDeadline(time.Time) error
	SetWriteDeadline(time.Time) error
	CloseWrite() error
}

type ssActor struct {
	state          privateRecord
	client         actorSocket
	upstream       actorSocket
	clientConn     net.Conn
	upstreamConn   net.Conn
	material       *materialState
	rx             cipher.AEAD
	tx             cipher.AEAD
	running        bool
	cleanupRunning bool
	dnsInFlight    bool
	files          map[string]*os.File
}

type flowBinding struct {
	id       [16]byte
	connID   [16]byte
	tuple    rawsocket.Tuple
	consumed bool
	deadline int64
}

type serverImageState struct {
	proxy               *ProxyState // copied authority/timer metadata, never scheduler handles
	proxyImported       bool
	id                  [16]byte
	actors              map[[16]byte]*ssActor
	materials           map[[32]byte]*materialState
	bindings            map[rawsocket.Tuple]*flowBinding
	revoke              func([16]byte, string)
	random              io.Reader
	authority           AuthorityPhase
	generation, epoch   int64
	revision            uint64
	digest              [32]byte
	granted, authFailed bool
	frozen              bool
	acceptRunning       bool
	workers             int
	changed             chan struct{}
	cut                 *frozenState
	listenerFile        *os.File
	listenerRef         string
	listenerTuple       []byte
	listenerFingerprint rawsocket.Fingerprint
	// A pristine import retains the exact sealed cut and taken business files
	// until activation commits, so a refusal can transfer the same closure.
	// origin: hot-update S5 plan §5.3 / §5.4 / T34
	importedResources []handoff.Resource
}

func newStateID() ([16]byte, error) { var id [16]byte; _, err := rand.Read(id[:]); return id, err }
func ensureImageState(s *Server) *serverImageState {
	if s.imageState == nil {
		s.imageState = &serverImageState{actors: make(map[[16]byte]*ssActor), materials: make(map[[32]byte]*materialState), bindings: make(map[rawsocket.Tuple]*flowBinding), authority: AuthorityPhaseAuthorized, changed: make(chan struct{})}
	}
	if s.allConns == nil {
		s.allConns = make(map[net.Conn]struct{})
	}
	if s.keyConns == nil {
		s.keyConns = make(map[string]map[net.Conn]struct{})
	}
	return s.imageState
}
func signalStateLocked(st *serverImageState) { close(st.changed); st.changed = make(chan struct{}) }
func timeoutError(err error) bool            { var e net.Error; return errors.As(err, &e) && e.Timeout() }

// Boot-relative deadlines survive same-PID exec. Legacy Darwin never exports
// this state and keeps a local wall deadline because BOOTTIME is Linux-only.
func actorNow() (int64, error) {
	if runtime.GOOS != "linux" {
		return time.Now().UnixNano(), nil
	}
	return procrequest.BootNow()
}

func newActor() (*ssActor, error) {
	id, err := newStateID()
	if err != nil {
		return nil, err
	}
	now, err := actorNow()
	if err != nil {
		return nil, err
	}
	a := &ssActor{state: newPrivateRecord(sealedstate.KindSSActor), files: make(map[string]*os.File)}
	p := a.state
	p.putBytes("id", id[:])
	p.putBytes("conn_id", id[:])
	p.put("phase", uint64(SSPhaseSalt))
	p.put("authority_phase", uint64(AuthorityPhaseAuthorized))
	p.put("connect_state", uint64(rawsocket.ConnectConnecting))
	p.put("handshake_deadline_boot_ns", uint64(now+int64(handshakeTimeout)))
	p.putBytes("rx_length_cipher", make([]byte, 2+tagSize))
	return a, nil
}

func acceptActorLocked(s *Server, c net.Conn) (*ssActor, error) {
	st := ensureImageState(s)
	if s.closed {
		return nil, ErrStopped
	}
	if len(st.actors) >= MaxActors {
		return nil, ErrCapacity
	}
	tc, ok := c.(*net.TCPConn)
	if !ok {
		return nil, ErrStateUnrepresentable
	}
	a, err := newActor()
	if err != nil {
		return nil, err
	}
	a.client, a.clientConn = tc, c
	if st.revoke != nil {
		local, ok1 := c.RemoteAddr().(*net.TCPAddr)
		remote, ok2 := c.LocalAddr().(*net.TCPAddr)
		if !ok1 || !ok2 {
			return nil, ErrStateUnrepresentable
		}
		tuple := rawsocket.Tuple{Local: local.AddrPort(), Remote: remote.AddrPort()}
		b := st.bindings[tuple]
		now, err := actorNow()
		if err != nil || b == nil || b.consumed || b.deadline <= now {
			return nil, ErrStateUnrepresentable
		}
		b.consumed = true
		b.connID = stateID(a.state.bytes("id"))
		a.state.putBytes("flow_id", b.id[:])
	}
	st.actors[stateID(a.state.bytes("id"))] = a
	if drillobserve.Hook != nil {
		drillobserve.Hook(observedActor(a, "ss-binding"))
	}
	s.allConns[c] = struct{}{}
	return a, nil
}

func startActorLocked(s *Server, a *ssActor) {
	if a.running || a.state.flag("revoked") || SSPhase(a.state.number("phase")) == SSPhaseTerminal {
		return
	}
	a.running = true
	s.imageState.workers++
	s.wg.Add(1)
	go runActor(s, a)
}

func runActor(s *Server, a *ssActor) {
	defer func() {
		// Release the actor's join responsibility before an expiry callback can
		// re-enter Stop. The pending record remains until cleanup acknowledges it.
		s.wg.Done()
		s.mu.Lock()
		pending := a.state.flag("cleanup_pending") && a.state.flag("fired")
		s.mu.Unlock()
		if pending {
			cleanupActors(s, []*ssActor{a})
		}
	}()
	defer func() {
		s.mu.Lock()
		a.running = false
		s.imageState.workers--
		signalStateLocked(s.imageState)
		terminal := a.state.flag("revoked") || SSPhase(a.state.number("phase")) == SSPhaseTerminal
		retire := terminal && !s.imageState.frozen && !a.state.flag("cleanup_pending")
		if retire {
			retireActorLocked(s, a)
		} else if !terminal && !s.imageState.frozen && !s.closed {
			startActorLocked(s, a)
		}
		s.mu.Unlock()
		if retire {
			closeActor(a)
		}
	}()
	for {
		s.mu.Lock()
		st := s.imageState
		if s.closed || st.frozen || a.state.flag("revoked") {
			s.mu.Unlock()
			return
		}
		remaining, expired := actorAuthorityDeadline(a)
		if expired {
			markRevokedLocked(a)
			a.state.put("fired", 1)
			s.mu.Unlock()
			return
		}
		if st.authority != AuthorityPhaseAuthorized {
			changed := st.changed
			s.mu.Unlock()
			if remaining == 0 {
				<-changed
			} else {
				timer := time.NewTimer(remaining)
				select {
				case <-changed:
					timer.Stop()
				case <-timer.C:
				}
			}
			continue
		}
		err := actorTurnLocked(s, a)
		if errors.Is(err, ErrAuthorityPending) {
			s.mu.Unlock()
			continue
		}
		if err != nil {
			a.state.put("phase", uint64(SSPhaseTerminal))
			s.mu.Unlock()
			return
		}
		if drillobserve.Hook != nil {
			drillobserve.Hook(observedActor(a, "ss-state"))
		}
		done := a.state.flag("rx_close_write") && a.state.flag("tx_close_write")
		if done {
			a.state.put("phase", uint64(SSPhaseTerminal))
		}
		s.mu.Unlock()
		if done {
			return
		}
	}
}

// IO calls release the guard, then commit n before interpreting errors. The
// single actor has at most one outstanding operation; Freeze joins that owner.
func actorIOLocked(s *Server, a *ssActor, sock actorSocket, write bool, p []byte) (int, error) {
	if a.state.flag("revoked") || s.imageState.authority != AuthorityPhaseAuthorized {
		return 0, ErrAuthorityPending
	}
	if s.imageState.frozen {
		return 0, nil
	}
	deadline := time.Now().Add(actorPollInterval)
	if write {
		_ = sock.SetWriteDeadline(deadline)
	} else {
		_ = sock.SetReadDeadline(deadline)
	}
	var observation drillobserve.Event
	if drillobserve.Hook != nil && write && sock == a.client && a.state.number("tx_salt_wire_cursor") == saltSize {
		observation = observedActor(a, "ss-write")
		observation.Digest = sha256.Sum256(a.state.bytes("tx_subkey"))
		observation.Nonce = observedPayloadNonce(a.state.bytes("tx_nonce"), a.state.flag("tx_nonce_exhausted"))
		observation.Values = [16]uint64{uint64(len(p)), 0, 0, 0, 1, a.state.number("tx_cipher_write_cursor"), uint64(len(a.state.bytes("tx_cipher_pending")))}
	}
	s.mu.Unlock()
	var n int
	var err error
	if write {
		n, err = sock.Write(p)
	} else {
		n, err = sock.Read(p)
	}
	// origin: hot-update S5 plan §6.1 / T26. Observe before timeout normalization.
	if drillobserve.Hook != nil && observation.Kind != "" && n >= 0 && n <= len(p) {
		observation.Values[1] = uint64(n)
		observation.Values[2] = uint64(len(p) - n)
		if err != nil {
			observation.Values[3] = 1
		}
		observation.Data = p[n:]
		drillobserve.Hook(observation)
	}
	s.mu.Lock()
	if n < 0 || n > len(p) {
		return 0, io.ErrShortWrite
	}
	if timeoutError(err) {
		err = nil
	}
	return n, err
}

func actorTurnLocked(s *Server, a *ssActor) error {
	p := a.state
	phase := SSPhase(p.number("phase"))
	switch phase {
	case SSPhaseSalt, SSPhaseLength, SSPhasePayload, SSPhaseTarget:
		now, err := actorNow()
		if err != nil {
			return err
		}
		if p.number("target_parser_phase") != 4 && uint64(now) >= p.number("handshake_deadline_boot_ns") {
			return errors.New("ssproxy: handshake deadline")
		}
	case SSPhaseConnecting, SSPhaseRelay, SSPhaseDraining:
	case SSPhaseRevoked, SSPhaseTerminal:
		return ErrStopped
	default:
		return ErrStateUnrepresentable
	}
	if phase == SSPhaseConnecting {
		return connectActorLocked(s, a)
	}
	if a.upstream != nil {
		if err := actorTransmitLocked(s, a); err != nil {
			return err
		}
		if s.imageState.frozen || p.flag("revoked") {
			return nil
		}
		clear := p.bytes("rx_clear_pending")
		cursor := p.number("rx_clear_cursor")
		if cursor < uint64(len(clear)) {
			n, err := actorIOLocked(s, a, a.upstream, true, clear[cursor:])
			p.put("rx_clear_cursor", cursor+uint64(n))
			p.put("rx_applied", p.number("rx_applied")+uint64(n))
			if err != nil {
				return err
			}
			if cursor+uint64(n) < uint64(len(clear)) {
				return nil
			}
		}
		p.putBytes("rx_clear_pending", nil)
		p.put("rx_clear_cursor", 0)
		// Propagate a genuine read EOF while retaining the reverse stream.
		// A full close here would truncate a delayed target response.
		if p.flag("rx_eof") {
			if !p.flag("rx_close_write") {
				_ = a.upstream.CloseWrite()
				p.put("rx_close_write", 1)
			}
			return nil
		}
	}
	if s.imageState.frozen || p.flag("revoked") {
		return nil
	}
	return readActorFrameLocked(s, a)
}

func readActorFrameLocked(s *Server, a *ssActor) error {
	p := a.state
	var buffer, cursor string
	if p.number("rx_salt_cursor") < saltSize {
		buffer, cursor = "rx_salt", "rx_salt_cursor"
	} else if !p.flag("rx_length_opened") {
		buffer, cursor = "rx_length_cipher", "rx_length_cursor"
	} else {
		buffer, cursor = "rx_payload_cipher", "rx_payload_cursor"
	}
	b := p.bytes(buffer)
	off := p.number(cursor)
	if off == uint64(len(b)) {
		return consumeActorFrameLocked(s, a)
	}
	n, err := actorIOLocked(s, a, a.client, false, b[off:])
	p.put(cursor, off+uint64(n))
	if a.state.flag("revoked") {
		return nil
	}
	if off+uint64(n) == uint64(len(b)) {
		if stepErr := consumeActorFrameLocked(s, a); stepErr != nil {
			return stepErr
		}
	}
	if errors.Is(err, io.EOF) {
		// EOF is valid only at an empty frame boundary, never through a tag.
		if p.number("rx_salt_cursor") != saltSize || p.number("rx_length_cursor") != 0 || p.flag("rx_length_opened") {
			return io.ErrUnexpectedEOF
		}
		p.put("rx_eof", 1)
		return nil
	}
	return err
}

func connectActorLocked(s *Server, a *ssActor) error {
	p := a.state
	host, port := actorTarget(p)
	if s.imageState.revoke == nil && !p.flag("dns_decided") {
		return connectLegacyActorLocked(s, a, net.JoinHostPort(host, port))
	}
	if !p.flag("dns_decided") {
		a.dnsInFlight = true
		allowPrivate := s.allowPrivate
		ctx, cancel := context.WithTimeout(s.stopCtx, dialTimeout)
		s.mu.Unlock()
		addrs, err := net.DefaultResolver.LookupNetIP(ctx, "ip", host)
		cancel()
		s.mu.Lock()
		a.dnsInFlight = false
		if err != nil {
			p.putBytes("dns_error", []byte("resolution_failed"))
			return errors.New("ssproxy: resolution failed")
		}
		if len(addrs) == 0 || len(addrs) > 128 {
			return ErrCapacity
		}
		for _, ip := range addrs {
			if !allowPrivate && blockedIP(net.IP(ip.AsSlice())) {
				// origin: hot-update S5 plan §6.6 / T26
				s.logger.Warn("ssproxy: blocked destination", "reason", "destination_policy_denied")
				return errors.New("ssproxy: destination policy denied")
			}
		}
		var candidates []byte
		for _, ip := range addrs {
			v := ip.Unmap().As16()
			candidates = append(candidates, v[:]...)
		}
		p.putBytes("resolved_ip_candidates", candidates)
		p.put("dns_decided", 1)
		p.put("policy_approved", 1)
	}
	if s.imageState.frozen || p.flag("revoked") {
		return nil
	}
	now, err := actorNow()
	if err != nil {
		return err
	}
	if p.number("connect_deadline_boot_ns") == 0 {
		p.put("connect_deadline_boot_ns", uint64(now+int64(dialTimeout)))
	}
	left := time.Duration(int64(p.number("connect_deadline_boot_ns")) - now)
	if left <= 0 {
		return errors.New("ssproxy: connect deadline")
	}
	ips := p.bytes("resolved_ip_candidates")
	index := int(p.number("candidate_index"))
	if index >= len(ips)/16 {
		return errors.New("ssproxy: connect failed")
	}
	var ip16 [16]byte
	copy(ip16[:], ips[index*16:])
	ip := netip.AddrFrom16(ip16).Unmap()
	// A portable legacy dial has no exportable in-flight socket. Freeze rejects
	// it while outstanding; Linux flow owners use the raw connector contract.
	if s.imageState.revoke == nil {
		ctx, cancel := context.WithTimeout(s.stopCtx, left)
		a.dnsInFlight = true
		s.mu.Unlock()
		remote, err := (&net.Dialer{}).DialContext(ctx, "tcp", net.JoinHostPort(ip.String(), port))
		cancel()
		s.mu.Lock()
		a.dnsInFlight = false
		if err != nil {
			p.put("candidate_index", uint64(index+1))
			// origin: hot-update S5 plan §6.1 / T17
			return nil //nolint:nilerr // The failed candidate is saved; nil schedules the next actor step rather than terminating the connection.
		}
		// origin: hot-update S5 plan §6.1 / T17
		upstream, ok := remote.(*net.TCPConn)
		if !ok {
			_ = remote.Close()
			return ErrStateUnrepresentable
		}
		a.upstream = upstream
		a.upstreamConn = remote
	} else if a.upstream == nil {
		pn, _ := strconv.ParseUint(port, 10, 16)
		var tuple rawsocket.Tuple
		a.dnsInFlight = true
		s.mu.Unlock()
		remote, err := rawsocket.Connect(netip.AddrPortFrom(ip, uint16(pn)), time.Now().Add(left), s.stopCtx.Done(), func(t rawsocket.Tuple) error { tuple = t; return nil })
		s.mu.Lock()
		a.dnsInFlight = false
		if err == nil {
			// Routing chooses the concrete source during Connect, including
			// EINPROGRESS. The pre-SYN callback only reserved a wildcard port;
			// import must compare the identity of the original bound socket.
			// origin: hot-update S5 internal review rv-ss-1
			_, _, tuple, _, _ = rawsocket.ConnectReceipt(remote)
			p.putBytes("upstream_tuple", []byte(tuple.Local.String()+"\n"+tuple.Remote.String()))
		}
		if err != nil {
			return err
		}
		a.upstream = remote
	}
	if p.flag("revoked") || s.closed {
		shutdownSocket(a.upstream)
		return ErrStopped
	}
	if s.imageState.frozen {
		return nil
	}
	if raw, ok := a.upstream.(*rawsocket.Socket); ok {
		s.mu.Unlock()
		err := raw.CompleteConnect(time.Now().Add(min(left, actorPollInterval)), s.stopCtx.Done())
		s.mu.Lock()
		if err != nil {
			if timeoutError(err) {
				return nil
			}
			recordConnectFailure(p, err)
			return errors.New("ssproxy: connect failed")
		}
	}
	p.put("connect_state", uint64(rawsocket.ConnectConnected))
	p.put("connect_so_error_consumed", 1)
	if a.upstreamConn != nil {
		s.allConns[a.upstreamConn] = struct{}{}
		id := string(p.bytes("authorized_key_id"))
		if s.keyConns[id] == nil {
			s.keyConns[id] = make(map[net.Conn]struct{})
		}
		s.keyConns[id][a.upstreamConn] = struct{}{}
	}
	p.put("phase", uint64(SSPhaseRelay))
	return nil
}

// SO_ERROR is a consumed kernel receipt; importing must not probe it again and
// mistake the cleared slot for a successful connect. Keep wrapped errno values.
// origin: hot-update S5 plan §6.1 / T17
func recordConnectFailure(p privateRecord, err error) {
	var errno syscall.Errno
	_ = errors.As(err, &errno)
	p.put("connect_so_error", uint64(errno))
	p.put("connect_state", uint64(rawsocket.ConnectFailed))
	p.put("connect_so_error_consumed", 1)
}

func actorTransmitLocked(s *Server, a *ssActor) error {
	p := a.state
	if !p.flag("tx_salt_reserved") {
		salt, err := responseSaltLocked(s, a.material)
		if err != nil {
			return err
		}
		sk, err := sessionSubkey(a.material.master, salt)
		if err != nil {
			return err
		}
		a.tx, err = chacha20poly1305.New(sk)
		if err != nil {
			return err
		}
		a.tx = observeCipher(a.tx, sk, stateID(p.bytes("id")))
		p.putBytes("tx_salt", salt)
		p.putBytes("tx_subkey", sk)
		p.put("tx_salt_reserved", 1)
	}
	if cursor := p.number("tx_salt_wire_cursor"); cursor < saltSize {
		n, err := actorIOLocked(s, a, a.client, true, p.bytes("tx_salt")[cursor:])
		p.put("tx_salt_wire_cursor", cursor+uint64(n))
		return err
	}
	if cursor := p.number("tx_cipher_write_cursor"); cursor < uint64(len(p.bytes("tx_cipher_pending"))) {
		n, err := actorIOLocked(s, a, a.client, true, p.bytes("tx_cipher_pending")[cursor:])
		p.put("tx_cipher_write_cursor", cursor+uint64(n))
		p.put("tx_applied", p.number("tx_applied")+uint64(n))
		return err
	}
	p.putBytes("tx_cipher_pending", nil)
	p.put("tx_cipher_write_cursor", 0)
	p.putBytes("tx_plain_pending", nil)
	p.putBytes("tx_length_cipher", nil)
	p.putBytes("tx_payload_cipher", nil)
	p.put("tx_length_sealed", 0)
	p.put("tx_payload_sealed", 0)
	if p.flag("tx_eof") {
		if !p.flag("tx_close_write") {
			_ = a.client.CloseWrite()
			p.put("tx_close_write", 1)
		}
		return nil
	}
	b := make([]byte, maxChunk)
	n, err := actorIOLocked(s, a, a.upstream, false, b)
	p.putBytes("upstream_raw_read_pending", b[:n])
	if errors.Is(err, io.EOF) {
		p.put("tx_eof", 1)
		err = nil
	}
	if err != nil {
		p.putBytes("upstream_raw_read_error", []byte("io_failure"))
	}
	if n > 0 {
		if p.flag("revoked") {
			return nil
		}
		p.putBytes("tx_plain_pending", b[:n])
		p.putBytes("upstream_raw_read_pending", nil)
		if sealErr := sealActorPending(a); sealErr != nil {
			return sealErr
		}
	}
	return err
}

// restoreCutState performs pure semantic validation before touching any socket.
// On every failure its resource return preserves the full caller-owned salvage.
func restoreCutState(cut sealedstate.CutState) (*Server, []handoff.Resource, error) {
	s, err := restorePrivateRecords(cut.Records)
	if err != nil {
		return nil, cut.Resources, err
	}
	fail := func(err error) (*Server, []handoff.Resource, error) { return nil, cut.Resources, err }
	st := s.imageState
	resources := make(map[string]handoff.Resource)
	for _, r := range cut.Resources {
		if _, ok := resources[r.ID]; ok {
			return fail(ErrStateUnrepresentable)
		}
		resources[r.ID] = r
	}
	// Validate every descriptor before creating wrappers; validation must not
	// consume SO_ERROR on a connecting upstream. Failed sockets legitimately have
	// no peer either: their sealed consumed errno is the authoritative decision.
	// origin: hot-update S5 internal review rv-ss-3
	for _, a := range st.actors {
		for _, name := range []string{"loopback", "upstream"} {
			ref := string(a.state.bytes(name + "_ref"))
			if ref == "" {
				continue
			}
			file := resources[ref].Files[name]
			if file == nil {
				return fail(ErrStateUnrepresentable)
			}
			if err := validateSocketFile(file, a.state.bytes(name+"_tuple"), false, name == "upstream" && a.state.number("connect_state") != uint64(rawsocket.ConnectConnected)); err != nil {
				return fail(err)
			}
			if err := rawsocket.ValidateFile(file, getFingerprint(a.state, name)); err != nil {
				return fail(err)
			}
		}
	}
	var listener *os.File
	if st.listenerRef != "" {
		listener = resources[st.listenerRef].Files["listener"]
		if listener == nil {
			return fail(ErrStateUnrepresentable)
		}
		if err := validateSocketFile(listener, st.listenerTuple, true, false); err != nil {
			return fail(err)
		}
		fp, err := fileFingerprint(listener)
		if err != nil || fp != st.listenerFingerprint {
			return fail(ErrStateUnrepresentable)
		}
	}
	// Raw Adopt takes files, but failed adoption must not close successfully
	// adopted earlier actors. The full resource list remains the salvage owner.
	for _, a := range st.actors {
		for _, name := range []string{"loopback", "upstream"} {
			ref := string(a.state.bytes(name + "_ref"))
			if ref == "" {
				continue
			}
			sock, err := rawsocket.Adopt(resources[ref].Files[name], getFingerprint(a.state, name))
			if err != nil {
				return fail(err)
			}
			if name == "loopback" {
				// No imported descriptor may perform IO before local activation.
				// origin: hot-update S5 plan §7.1 / T27
				if err := rawsocket.Hold(sock); err != nil {
					return fail(err)
				}
				a.client = sock
			} else {
				// Adopt may observe only the current kernel connection shape, not
				// the destructive SO_ERROR receipt already consumed by this actor.
				// Preserve that private decision and keep the imported wrapper held.
				// origin: hot-update S5 plan §6.1 / T17 / T27
				tuple, err := parseTuple(a.state.bytes("upstream_tuple"))
				if err != nil {
					return fail(err)
				}
				now, err := actorNow()
				if err != nil {
					return fail(err)
				}
				deadline := time.Now().Add(time.Duration(int64(a.state.number("connect_deadline_boot_ns")) - now))
				if err := rawsocket.RestoreConnectReceipt(sock, rawsocket.ConnectState(a.state.number("connect_state")), a.state.flag("connect_so_error_consumed"), a.state.number("connect_so_error"), tuple, deadline); err != nil {
					return fail(err)
				}
				a.upstream = sock
			}
		}
	}
	if listener != nil {
		ln, err := net.FileListener(listener)
		if err != nil {
			return fail(errors.New("ssproxy: listener restore failed"))
		}
		tcp, ok := ln.(*net.TCPListener)
		if !ok {
			_ = ln.Close()
			return fail(ErrStateUnrepresentable)
		}
		// origin: hot-update S5 plan §5.3 / T27
		addr, ok := tcp.Addr().(*net.TCPAddr)
		if !ok {
			_ = tcp.Close()
			return fail(ErrStateUnrepresentable)
		}
		s.ln = tcp
		s.localPort = addr.Port
		st.listenerFile = listener
	}
	// ctx-root: imported SS actors remain frozen until local activation.
	s.stopCtx, s.stopCancel = context.WithCancel(context.Background())
	// Success transfers every business file to the frozen runtime; salvage is
	// exclusively the failure path and must not retain a second Close owner.
	// origin: hot-update S5 plan §5.3 / T27
	return s, nil, nil
}
