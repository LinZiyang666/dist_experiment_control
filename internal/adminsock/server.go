package adminsock

import (
	"bufio"
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"os"
	"path/filepath"
	"sync"
	"syscall"
	"time"

	"github.com/LinZiyang666/tether/internal/proto"
)

// Backend is everything the server needs from the broker. Decoupled
// so the broker can wire its own DB / JS / pub functions without
// adminsock importing the broker package (which would cycle).
type Backend struct {
	// DB is the broker's SQLite handle. Used by sessions / nodes /
	// evict.
	DB *sql.DB

	// AuditTail returns the last n audit entries for sid in time
	// order (oldest → newest). Implemented by the broker against
	// JetStream history-<sid>; nil = audit endpoint disabled
	// (returns "audit_unavailable").
	AuditTail func(ctx context.Context, sid string, n int) ([]AuditEntry, error)

	// EventsTail (#30) returns the last n messages of the H.1 `events` stream, filtered to those
	// newer than `since` (0 = no time bound) and of type `kind` ("" = all kinds), in time order
	// (oldest → newest). Implemented by the broker against JetStream; nil = events endpoint disabled
	// (returns "events_unavailable"). The stream is secret-free by construction of its producers.
	EventsTail func(ctx context.Context, n int, since time.Duration, kind string) ([]AuditEntry, bool, error)

	// PubAgentEvicted broadcasts sys.events{type:agent_evicted, sid,
	// nid} so a live agent subscribing to sys.events can self-exit
	// within the architecture P9 "1s 内下线" budget. nil = broker
	// did not wire a publisher; evict still deletes rows but the
	// agent only notices on next reconnect.
	PubAgentEvicted func(sid, nid string)

	// EvictWrite, when non-nil (the D9 cutover wires it in cluster mode), routes the evict's
	// row deletes through raft (Propose PlanEvict on the leader / forward on a follower)
	// instead of the direct tx on DB (which is the READ-ONLY FSM handle in cluster mode).
	// nil ⇒ single mode: handleEvict does the direct tx (byte-identical to pre-D9).
	EvictWrite func(sid, nid string) error

	// ClusterMode makes a nil EvictWrite FAIL CLOSED instead of falling through to the direct tx
	// (batch B, B3). It is the exact counterpart of authcallout.Handler.ClusterMode; see that field
	// for the full argument. The short version: "EvictWrite != nil" was doing double duty as both
	// the seam and the mode flag, so a clustered broker that failed to wire the seam silently got
	// the single-mode write path against the read-only FSM handle.
	//
	// The consequence differs from authcallout's in ONE way that matters. Here the immediate
	// symptom is loud — the operator invoked the evict and gets the error straight back on the
	// socket. What is NOT loud is the second failure mode: if this package is ever handed the FSM
	// WRITE pool instead of the read-only one, the direct tx SUCCEEDS and deletes
	// agent_provisioning/nodes rows OUTSIDE raft. The agent is then evicted on one broker and
	// still provisioned on the others, with no error anywhere.
	//
	// Zero value = single mode = today's behaviour, so no existing Backend literal changes meaning.
	ClusterMode bool

	// Now is the time source used for reply timestamps; nil →
	// time.Now.
	Now func() time.Time

	// Cluster handles the D7 cluster admin verbs (add/remove/drain/
	// transfer/status/rotate-cert). nil (production until the D9
	// cutover; the build-and-prove harness sets it) → those verbs
	// reply "cluster mode not enabled". adminsock stays a leaf: the
	// broker provides an adapter that translates to the wire types.
	Cluster ClusterAdminBackend

	// RuntimeSnapshot (R13) returns this process's live runtime introspection
	// (goroutines/threads/fds/rss/uptime + per-reconciler last-tick) for OpRuntime.
	// The broker builds it from runtime.NumGoroutine() + the R7 registry; adminsock
	// stays a leaf and never imports runtime-internal state. nil → OpRuntime replies
	// "runtime introspection unavailable" (a Backend that did not wire it).
	RuntimeSnapshot func() *RuntimeReport

	// Logger receives info/warn lines about accepts and dispatch
	// failures; nil → discard.
	Logger *slog.Logger
}

// Server owns the listening Unix socket. One per broker process.
// The socket file is created with 0600 mode under a 0700 parent
// directory; only the broker's effective uid (or root) can connect
// — the architecture P9 acceptance test for "non-tether user →
// permission denied" is satisfied by these filesystem permissions
// (the OS rejects the connect attempt with EACCES before any user
// code runs).
type Server struct {
	path    string
	backend Backend

	mu       sync.Mutex
	listener net.Listener
	closed   bool
}

// New returns a Server ready to call Start. path is the absolute
// path of the Unix socket file. Backend is the wire-up to broker
// state.
func New(path string, backend Backend) *Server {
	return &Server{path: path, backend: backend}
}

// Start binds the listener (creating the parent dir 0700 if absent),
// chmods the socket to 0600, then launches the accept loop in a
// goroutine. It returns once the listener is bound; the goroutine
// runs until ctx is canceled or the server is Closed. Concurrent
// Start calls are not allowed.
func (s *Server) Start(ctx context.Context) error {
	parent := filepath.Dir(s.path)
	if err := os.MkdirAll(parent, 0o700); err != nil {
		return fmt.Errorf("adminsock: mkdir parent: %w", err)
	}
	// Audit shard 02 F5: verify the parent dir is owned by us
	// before chmod-ing it. A malicious local user could otherwise
	// pre-create /var/run/tether owned by THEIR uid, mode 0700;
	// chmod 0700 of an already-0700 dir succeeds for the owner
	// (us) — but the dir is still theirs, and they can swap a
	// symlink for admin.sock. Refuse if uid doesn't match.
	if fi, err := os.Lstat(parent); err == nil {
		if st, ok := fi.Sys().(*syscall.Stat_t); ok {
			if uid := os.Geteuid(); uid != int(st.Uid) {
				return fmt.Errorf("adminsock: parent %s owned by uid=%d, broker uid=%d",
					parent, st.Uid, uid)
			}
		}
	}
	// MkdirAll is a no-op when the parent already exists; defensively
	// re-chmod every Start so an operator-pre-created /var/run/tether
	// at 0755 doesn't silently weaken the architecture P9 invariant
	// of "parent dir 0700, socket 0600". Failure to harden is fatal:
	// continuing would leave the socket exposed to any local user.
	if err := os.Chmod(parent, 0o700); err != nil {
		return fmt.Errorf("adminsock: chmod parent 0700: %w", err)
	}
	// Stale socket from a previous crashed broker would block bind.
	// `unix` listeners can't reuse the path. Two failure modes share
	// this dirent shape: a TRULY stale path (no listener) and a path
	// owned by a STILL-RUNNING broker. Unlinking the latter would
	// silently steal the path from the live process — we'd succeed
	// here while leaving the original broker bound to a now-orphaned
	// inode and unreachable via the documented admin path. So probe
	// with a short Dial first: connectable → return active-socket
	// error; not connectable → safe to unlink and re-bind.
	if fi, err := os.Lstat(s.path); err == nil && fi.Mode()&os.ModeSocket != 0 {
		if alive := isAdminSocketAlive(s.path); alive {
			return fmt.Errorf("adminsock: active socket already exists at %s", s.path)
		}
		_ = os.Remove(s.path)
	}
	ln, err := net.Listen("unix", s.path)
	if err != nil {
		return fmt.Errorf("adminsock: listen: %w", err)
	}
	if err := os.Chmod(s.path, 0o600); err != nil {
		_ = ln.Close()
		return fmt.Errorf("adminsock: chmod 0600: %w", err)
	}
	s.mu.Lock()
	s.listener = ln
	s.mu.Unlock()

	// Pass the listener as a parameter so acceptLoop never reads
	// s.listener concurrently with Close (which sets it to nil). The
	// loop owns its own reference; Close still triggers exit by
	// closing the listener (Accept returns ErrClosed).
	go s.acceptLoop(ctx, ln)
	return nil
}

// isAdminSocketAlive returns true iff a Dial against path succeeds
// within a short window — i.e. another broker is currently bound to
// it. Connection-refused / ENOENT / timeout are all "not alive,
// safe to reclaim". The dial doesn't write anything, so the live
// broker (if any) just sees an immediate disconnect.
func isAdminSocketAlive(path string) bool {
	conn, err := net.DialTimeout("unix", path, 200*time.Millisecond)
	if err != nil {
		return false
	}
	_ = conn.Close()
	return true
}

// Close unbinds the listener and removes the socket file. Safe to
// call multiple times.
func (s *Server) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return nil
	}
	s.closed = true
	var err error
	if s.listener != nil {
		err = s.listener.Close()
		s.listener = nil
	}
	_ = os.Remove(s.path)
	return err
}

func (s *Server) acceptLoop(ctx context.Context, ln net.Listener) {
	// Audit shard 01 F6: the inner ctx-watcher goroutine used to
	// leak when shutdown was initiated by an explicit Close()
	// (which the broker does via defer b.admin.Close()) — accept
	// returns ErrClosed cleanly, but the watcher sat forever
	// waiting on <-ctx.Done. Close a `done` channel on loop exit
	// so the watcher exits via select too.
	done := make(chan struct{})
	go func() {
		select {
		case <-ctx.Done():
			_ = s.Close()
		case <-done:
		}
	}()
	defer close(done)
	for {
		conn, err := ln.Accept()
		if err != nil {
			if errors.Is(err, net.ErrClosed) {
				return
			}
			s.warn("adminsock: accept", "err", err)
			return
		}
		go s.handle(conn)
	}
}

func (s *Server) handle(conn net.Conn) {
	defer func() { _ = conn.Close() }()
	_ = conn.SetReadDeadline(s.now().Add(5 * time.Second))

	br := bufio.NewReader(conn)
	line, err := br.ReadBytes('\n')
	if err != nil {
		s.warn("adminsock: read", "err", err)
		return
	}
	var req Request
	if err := json.Unmarshal(line, &req); err != nil {
		s.writeErr(conn, "", fmt.Sprintf("json_parse: %v", err))
		return
	}
	resp := s.dispatch(req)
	body, err := json.Marshal(&resp)
	if err != nil {
		s.warn("adminsock: marshal resp", "err", err)
		return
	}
	body = append(body, '\n')
	_ = conn.SetWriteDeadline(s.now().Add(5 * time.Second))
	if _, err := conn.Write(body); err != nil {
		s.warn("adminsock: write", "err", err)
	}
}

func (s *Server) writeErr(conn net.Conn, op, msg string) {
	body, _ := json.Marshal(Response{Op: op, Error: msg})
	body = append(body, '\n')
	_ = conn.SetWriteDeadline(s.now().Add(5 * time.Second))
	_, _ = conn.Write(body)
}

func (s *Server) dispatch(req Request) Response {
	switch req.Op {
	case OpSessions:
		return s.handleSessions()
	case OpNodes:
		return s.handleNodes()
	case OpAudit:
		return s.handleAudit(req)
	case OpEvents:
		return s.handleEvents(req)
	case OpEvict:
		return s.handleEvict(req)
	case OpRuntime:
		return s.handleRuntime()
	default:
		if clusterOps[req.Op] {
			if s.backend.Cluster == nil {
				return Response{Op: req.Op, Error: "cluster mode not enabled", Code: CodeClusterNotEnabled}
			}
			return s.backend.Cluster.HandleCluster(req)
		}
		return Response{Op: req.Op, Error: "unknown op: " + req.Op, Code: CodeBadRequest}
	}
}

func (s *Server) handleSessions() Response {
	rows, err := s.backend.DB.Query(`
		SELECT sid, name, owner_pubkey_fp, state, created_at
		FROM sessions
		ORDER BY sid
	`)
	if err != nil {
		return Response{Op: OpSessions, Error: err.Error()}
	}
	defer func() { _ = rows.Close() }()
	var out []SessionEntry
	for rows.Next() {
		var e SessionEntry
		if err := rows.Scan(&e.SID, &e.Name, &e.OwnerFP, &e.State, &e.CreatedAt); err != nil {
			return Response{Op: OpSessions, Error: err.Error()}
		}
		out = append(out, e)
	}
	if err := rows.Err(); err != nil {
		return Response{Op: OpSessions, Error: err.Error()}
	}
	return Response{Op: OpSessions, Sessions: out, OK: true}
}

func (s *Server) handleNodes() Response {
	rows, err := s.backend.DB.Query(`
		SELECT sid, nid, status, last_heartbeat_at, boot_id, release_version, proto_version
		FROM nodes
		ORDER BY sid, nid
	`)
	if err != nil {
		return Response{Op: OpNodes, Error: err.Error()}
	}
	defer func() { _ = rows.Close() }()
	var out []NodeEntry
	for rows.Next() {
		var e NodeEntry
		var hb sql.NullTime
		var boot, rel sql.NullString
		var pv sql.NullInt64
		if err := rows.Scan(&e.SID, &e.NID, &e.Status, &hb, &boot, &rel, &pv); err != nil {
			return Response{Op: OpNodes, Error: err.Error()}
		}
		if hb.Valid {
			e.LastHeartbeatAt = hb.Time
		}
		e.BootID = boot.String
		e.ReleaseVersion = rel.String
		e.ProtoVersion = int(pv.Int64)
		out = append(out, e)
	}
	if err := rows.Err(); err != nil {
		return Response{Op: OpNodes, Error: err.Error()}
	}
	return Response{Op: OpNodes, Nodes: out, OK: true}
}

func (s *Server) handleRuntime() Response {
	if s.backend.RuntimeSnapshot == nil {
		return Response{Op: OpRuntime, Error: "runtime introspection unavailable", Code: CodeBadRequest}
	}
	rep := s.backend.RuntimeSnapshot()
	if rep == nil {
		return Response{Op: OpRuntime, Error: "runtime introspection unavailable", Code: CodeBadRequest}
	}
	return Response{Op: OpRuntime, OK: true, Runtime: rep}
}

func (s *Server) handleAudit(req Request) Response {
	if req.SID == "" {
		return Response{Op: OpAudit, Error: "sid required"}
	}
	if req.N <= 0 {
		req.N = 50
	}
	if s.backend.AuditTail == nil {
		return Response{Op: OpAudit, Error: "audit_unavailable: broker has no JetStream"}
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	entries, err := s.backend.AuditTail(ctx, req.SID, req.N)
	if err != nil {
		return Response{Op: OpAudit, Error: err.Error()}
	}
	return Response{Op: OpAudit, Audit: entries, OK: true}
}

func (s *Server) handleEvents(req Request) Response {
	if req.N <= 0 {
		req.N = 50
	}
	var since time.Duration
	if req.Since != "" {
		d, err := time.ParseDuration(req.Since)
		if err != nil {
			return Response{Op: OpEvents, Error: "bad since duration: " + err.Error(), Code: CodeBadRequest}
		}
		if d < 0 {
			return Response{Op: OpEvents, Error: "since must not be negative", Code: CodeBadRequest}
		}
		since = d
	}
	if s.backend.EventsTail == nil {
		return Response{Op: OpEvents, Error: "events_unavailable: broker has no JetStream"}
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	entries, truncated, err := s.backend.EventsTail(ctx, req.N, since, req.EventKind)
	if err != nil {
		return Response{Op: OpEvents, Error: err.Error()}
	}
	return Response{Op: OpEvents, Events: entries, Truncated: truncated, OK: true}
}

// rowExists reports whether the 1-column existence query returns a row (used by the
// cluster-mode evict to report whether a row was actually removed). Read on the RODB handle.
func (s *Server) rowExists(query string, args ...any) bool {
	var one int
	return s.backend.DB.QueryRow(query, args...).Scan(&one) == nil
}

func (s *Server) handleEvict(req Request) Response {
	if req.SID == "" || req.NID == "" {
		return Response{Op: OpEvict, Error: "sid and nid required"}
	}
	// REFUSE TO "EVICT" A LEASE NAME (external review F10).
	//
	// Evict means REVOKE A CREDENTIAL: it deletes the agent_provisioning binding
	// so the agent can no longer authenticate, and broadcasts agent_evicted so a
	// live one disconnects. A broker-assigned lease name has neither half. It
	// owns no binding — it is admitted through its BASENAME's fingerprint — and
	// the eviction broadcast is matched by agents on their CONFIGURED name, so a
	// running leased instance never hears it.
	//
	// Running it anyway did the damage without the effect: node/process/port
	// rows deleted, `ok` reported, the instance still running and still able to
	// come back through the suffix fallback. An operator following §5.18's
	// OFFLINE-cleanup advice would silently destroy live bookkeeping.
	//
	// So: refuse, and say which name to use instead. Revoking the whole clone
	// family is `evict <basename>` — a real, coherent operation. Stopping one
	// instance is a job for the host that runs it, not for credential
	// revocation, and the CLI now says so rather than implying otherwise.
	if base, _, leased := proto.SplitLeaseName(req.NID); leased &&
		!s.rowExists(`SELECT 1 FROM agent_provisioning WHERE sid=? AND nid=?`, req.SID, req.NID) {
		return Response{Op: OpEvict, Code: CodeBadRequest, Error: fmt.Sprintf(
			"%q is a broker-assigned lease name, not a credential: it owns no provisioning row to "+
				"revoke, and the eviction broadcast is matched on an agent's CONFIGURED name so a "+
				"running instance would not hear it. To revoke the whole clone family use "+
				"`evict %s`; to stop one instance, stop it on the host that runs it.",
			req.NID, base)}
	}
	// D9 round-2 BLOCKER: in cluster mode the direct tx below writes the READ-ONLY FSM
	// handle and fails ("readonly database"). Route through raft via the EvictWrite seam
	// (Propose PlanEvict / forward). Pre-query existence on the read handle so the result
	// still reports whether a row was actually removed.
	if s.backend.EvictWrite != nil {
		provExisted := s.rowExists(`SELECT 1 FROM agent_provisioning WHERE sid=? AND nid=?`, req.SID, req.NID)
		nodeExisted := s.rowExists(`SELECT 1 FROM nodes WHERE sid=? AND nid=?`, req.SID, req.NID)
		if err := s.backend.EvictWrite(req.SID, req.NID); err != nil {
			return Response{Op: OpEvict, Error: err.Error()}
		}
		broadcasted := false
		if (provExisted || nodeExisted) && s.backend.PubAgentEvicted != nil {
			s.backend.PubAgentEvicted(req.SID, req.NID)
			broadcasted = true
		}
		return Response{Op: OpEvict, OK: true, Evict: &EvictResult{
			SID: req.SID, NID: req.NID,
			NodeRowDeleted: nodeExisted, AgentProvDeleted: provExisted, BroadcastedEvicted: broadcasted,
		}}
	}
	// FAIL CLOSED (batch B, B3): clustered, but the seam above is absent. Falling through to the
	// direct tx would write the read-only FSM handle — or, with a write handle, delete rows outside
	// raft. See Backend.ClusterMode.
	//
	// Unlike authcallout's twin, this message is DETAILED on purpose. That reply goes out over a
	// root-owned unix socket to the operator who typed the command; there is no unauthenticated
	// reader to disclose anything to, so the wire IS the operator's log and hiding the cause would
	// just make a broker bug look like a missing agent.
	if s.backend.ClusterMode {
		if s.backend.Logger != nil {
			s.backend.Logger.Error("adminsock: EvictWrite seam is not wired in cluster mode — "+
				"refusing rather than writing the read-only FSM handle directly",
				"seam", "EvictWrite", "sid", req.SID, "nid", req.NID)
		}
		return Response{Op: OpEvict, Code: CodeStoreError, Error: "evict is unavailable: this " +
			"broker is clustered but its raft evict path is not wired (broker wiring bug, not a " +
			"bad request) — refusing rather than writing un-replicated rows"}
	}
	tx, err := s.backend.DB.Begin()
	if err != nil {
		return Response{Op: OpEvict, Error: err.Error()}
	}
	defer func() { _ = tx.Rollback() }()

	res1, err := tx.Exec(
		`DELETE FROM agent_provisioning WHERE sid = ? AND nid = ?`,
		req.SID, req.NID,
	)
	if err != nil {
		return Response{Op: OpEvict, Error: "delete agent_provisioning: " + err.Error()}
	}
	provN, _ := res1.RowsAffected()

	res2, err := tx.Exec(
		`DELETE FROM nodes WHERE sid = ? AND nid = ?`,
		req.SID, req.NID,
	)
	if err != nil {
		return Response{Op: OpEvict, Error: "delete nodes: " + err.Error()}
	}
	nodeN, _ := res2.RowsAffected()

	if err := tx.Commit(); err != nil {
		return Response{Op: OpEvict, Error: "commit: " + err.Error()}
	}

	broadcasted := false
	if (provN > 0 || nodeN > 0) && s.backend.PubAgentEvicted != nil {
		s.backend.PubAgentEvicted(req.SID, req.NID)
		broadcasted = true
	}
	return Response{
		Op: OpEvict,
		OK: true,
		Evict: &EvictResult{
			SID:                req.SID,
			NID:                req.NID,
			NodeRowDeleted:     nodeN > 0,
			AgentProvDeleted:   provN > 0,
			BroadcastedEvicted: broadcasted,
		},
	}
}

func (s *Server) now() time.Time {
	if s.backend.Now != nil {
		return s.backend.Now()
	}
	return time.Now()
}

func (s *Server) warn(msg string, kv ...any) {
	if s.backend.Logger != nil {
		s.backend.Logger.Warn(msg, kv...)
	}
}
