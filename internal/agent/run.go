// Implementation of the P5 PTY `run` flow on the agent side. The wire
// shape and ordering live in architecture C.5 / C.5.1.
//
// One handler call drives one full run lifecycle:
//
//	handleRunForwarded
//	  ├── allocate PTY (Cols/Rows from RunReq)
//	  ├── reply RunChunk{Kind:ready, PID, Cols, Rows}
//	  ├── subscribe pty.<pid>.attach (sync, with 3s deadline)
//	  │     attach not received → reply RunChunk{Kind:failed, Reason:attach_timeout}
//	  │                          + pub PtyFailedEvent on pty.<pid>.failed
//	  │                          + close PTY → return
//	  ├── pty.Start with the (potentially updated) cols/rows from attach
//	  ├── put session into a.procs (kill verb looks here)
//	  ├── reply RunChunk{Kind:started}
//	  ├── subscribe pty.<pid>.in     → write bytes to PTY master
//	  ├── subscribe pty.<pid>.resize → ioctl(TIOCSWINSZ) on master
//	  ├── pump master → publish chunks on pty.<pid>.out (4KB or 50ms)
//	  ├── Wait → reply RunChunk{Kind:exit, ExitCode}
//	  └── prune from a.procs, close PTY, unsubscribe
package agent

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"strings"
	"syscall"
	"time"

	"github.com/LinZiyang666/tether/internal/proc"
	"github.com/LinZiyang666/tether/internal/proto"
	"github.com/LinZiyang666/tether/internal/pty"
	"github.com/LinZiyang666/tether/internal/spawnsafe"
	"github.com/nats-io/nats.go"
)

// defaultAttachDeadline is how long the agent waits between publishing
// RunChunk{Kind:ready} and giving up because no attach arrived. P5
// spec pinned this at 3s (architecture C.5.1) which proved too tight
// in production over public NATS WSS — single-trip 200-400ms RTTs
// from a remote ctl + TLS overhead can routinely eat 1-2s before the
// ctl's pty.<pid>.attach pub lands.
//
// Default raised to 15s. Operators on lower-RTT LAN deployments can
// tune via TETHER_AGENT_ATTACH_DEADLINE (e.g. "5s", "30s") to trade
// faster orphan-PTY cleanup for less retry friction.
const defaultAttachDeadline = 15 * time.Second

// attachDeadline returns the active deadline, honoring the env var
// override at startup. Read on each PTY alloc so live-reload-style
// SIGHUP'd unit edits work without a binary restart.
func attachDeadline() time.Duration {
	if v := os.Getenv("TETHER_AGENT_ATTACH_DEADLINE"); v != "" {
		if d, err := time.ParseDuration(v); err == nil && d > 0 {
			return d
		}
	}
	return defaultAttachDeadline
}

// runChunkSize / runFlushInterval bound the master→.out pump. 4KB or
// 50ms whichever comes first — see architecture C.5.2.
const (
	runChunkSize     = 4 * 1024
	runFlushInterval = 50 * time.Millisecond
)

// handleRunForwarded executes the full run lifecycle. msg.Reply is the
// ctl's _INBOX where we publish RunChunk lifecycle events; PTY byte
// streams travel on a separate `pty.<pid>.*` subject family.
func (a *Agent) handleRunForwarded(nc *nats.Conn, msg *nats.Msg) {
	if msg.Reply == "" {
		a.cfg.Logger.Warn("agent: run.req.forwarded without Reply inbox")
		return
	}
	var req proto.RunReq
	if err := json.Unmarshal(msg.Data, &req); err != nil {
		a.replyRunChunk(nc, msg.Reply, proto.RunChunk{
			Kind: "failed", Reason: "json_parse", PID: "",
		})
		return
	}
	if len(req.Argv) == 0 {
		a.replyRunChunk(nc, msg.Reply, proto.RunChunk{
			Kind: "failed", Reason: "argv_required",
		})
		return
	}
	cols, rows := normalizedSize(req.Cols, req.Rows)

	pid := proc.NewPID()
	a.cfg.Logger.Info("agent: run", "pid", pid, "argv", req.Argv, "cols", cols, "rows", rows)

	sess, err := pty.Allocate(cols, rows)
	if err != nil {
		// origin: line-2 §12 Y2, completing the split the unclassifiedCodeAllowlist entry named.
		// pty.Allocate wraps cpty.Open, and its failures divide cleanly by errno into causes with
		// OPPOSITE remedies:
		//
		//   a resource limit  fds (EMFILE/ENFILE), the pty count itself (ENOSPC), or kernel memory
		//                    (ENOMEM/EAGAIN). Self-healing — the next attempt after some sessions close
		//                    will succeed. Retry is correct. The exact list, and why ENOSPC is the one
		//                    that matters most, is at ptyTransientErrnos.
		//   anything else    /dev/ptmx is absent, not permitted, or the container forbids it. That is
		//                    a property of the HOST, not of this instant; retrying is guaranteed to
		//                    fail again and the operator has to change something.
		//
		// One code for both told automation to retry a missing /dev/ptmx forever — which A1 had
		// classified 75 on the strength of the common cause, and external review M2 reverted to
		// unclassified precisely because no single retry semantics fits. Two codes fit two semantics.
		// Written as two literal branches rather than a `reason` variable on purpose: the wire-code
		// coverage gate resolves codes statically, and a variable here would become an "unresolved
		// site" needing an entry in unresolvedCodeSites. An exemption that can be avoided by writing
		// the code out is an exemption that should not exist.
		if ptyFailureIsTransient(err) {
			a.cfg.Logger.Warn("agent: pty allocate (a resource limit is exhausted — fds or the pty count)",
				"err", err, "pid", pid)
			a.replyRunChunk(nc, msg.Reply, proto.RunChunk{
				Kind: "failed", PID: pid, Reason: "pty_alloc_failed",
			})
			a.pubPtyFailed(nc, pid, "pty_alloc_failed", err.Error())
			return
		}
		a.cfg.Logger.Warn("agent: pty allocate (host cannot provide a PTY)", "err", err, "pid", pid)
		a.replyRunChunk(nc, msg.Reply, proto.RunChunk{
			Kind: "failed", PID: pid, Reason: "pty_unavailable",
		})
		a.pubPtyFailed(nc, pid, "pty_unavailable", err.Error())
		return
	}

	// Step 3 of C.5.1: subscribe to attach BEFORE replying ready, then
	// reply ready, then block on the sub. Race condition fixed here:
	// previously SubscribeSync(attach_subj) lived inside waitForAttach
	// — called AFTER replying ready. ctl receives ready and immediately
	// pubs attach; if the agent goroutine hadn't reached SubscribeSync
	// yet (CI runner under load), the attach pub landed at the NATS
	// server with no matching subscriber and was discarded. Result:
	// 15s wait → attach_timeout, even though ctl did its job correctly.
	attachSubj := proto.SubjPtyAttach(a.cfg.SID, pid)
	attachSub, err := nc.SubscribeSync(attachSubj)
	if err != nil {
		a.cfg.Logger.Warn("agent: SubscribeSync attach", "err", err, "pid", pid)
		_ = sess.Close()
		// origin: line-2 §12 Y2. This used to report pty_alloc_failed, which was a lie about WHICH
		// thing failed and, worse, a lie with a retry policy attached: the PTY allocated fine (we are
		// past pty.Allocate and closing the session on the way out) — it is the NATS subscription that
		// did not come up. Sharing one code made those two indistinguishable to a monitor, so a host
		// that can NEVER allocate a PTY (no /dev/ptmx, container restriction) got the same
		// retry-forever treatment as a NATS hiccup that really does clear on its own.
		a.replyRunChunk(nc, msg.Reply, proto.RunChunk{
			Kind: "failed", PID: pid, Reason: "attach_subscribe_failed",
		})
		return
	}
	defer func() { _ = attachSub.Unsubscribe() }()
	// Flush so the SUB protocol frame has reached the NATS server
	// before ctl can possibly send attach. Same logic as broker
	// ReadyCh in test/p10.
	_ = nc.FlushTimeout(200 * time.Millisecond)

	a.replyRunChunk(nc, msg.Reply, proto.RunChunk{
		Kind: "ready", PID: pid, Cols: cols, Rows: rows,
	})

	attachCols, attachRows, ok := a.waitForAttachOnSub(attachSub, pid)
	if !ok {
		a.cfg.Logger.Info("agent: attach_timeout", "pid", pid)
		_ = sess.Close()
		a.replyRunChunk(nc, msg.Reply, proto.RunChunk{
			Kind: "failed", PID: pid, Reason: "attach_timeout",
		})
		a.pubPtyFailed(nc, pid, "attach_timeout", "")
		return
	}
	if attachCols > 0 && attachRows > 0 {
		_ = sess.SetSize(attachCols, attachRows)
	}

	// Now exec. Failures here are exec_failed (not attach_timeout) —
	// keep them distinguishable so the audit kind is meaningful.
	//
	// Build the child env on top of os.Environ() so the child
	// inherits PATH / HOME / locale defaults from the agent unit;
	// req.Env entries override those; finally TERM is force-defaulted
	// to xterm-256color if neither layer set it (systemd --user
	// units run with no TERM, so curses apps like tmux otherwise
	// fail with "terminal does not support clear").
	childEnv := mergeChildEnv(req.Env)
	childCwd := req.Cwd
	resolvedPath := ""
	// Hung-fs-safe resolution: pre-resolve argv[0] against a PATH sanitized of
	// wedged network mounts so fork+exec cannot D-hang on a dead $PATH dir. A
	// fail-fast (argv[0]/cwd on a dead mount, or not found) surfaces as
	// RunChunk{failed, Reason:remote_fs_*} rather than an indefinite black screen.
	// lookupPATH = agent process PATH (what LookPath would walk — review F2).
	d, ferr := a.spawnPolicy.Prepare(req.Argv, req.Cwd, os.Getenv("PATH"), childEnv, req.Safe)
	if ferr != nil {
		reason := remoteFSFailReason(ferr)
		_ = sess.Close()
		a.replyRunChunk(nc, msg.Reply, proto.RunChunk{Kind: "failed", PID: pid, Reason: reason})
		a.pubPtyFailed(nc, pid, reason, ferr.Error())
		return
	}
	if d.Active {
		resolvedPath = d.Path // always self-resolve on a hangable machine (bypass LookPath)
		if d.Outage {
			childEnv, childCwd = d.Env, d.Cwd
			// run warnings go to agent.log only — injecting a banner into the
			// raw PTY stream would corrupt vim/tmux (§3.H).
			a.cfg.Logger.Warn("agent: run in remote-fs-safe mode", "pid", pid, "detail", d.Warn)
		}
		// else healthy-hangable: keep legacy childEnv/childCwd (byte-identical).
	}
	startFn := func() error { return sess.Start(req.Argv, childEnv, childCwd, resolvedPath) }
	var startErr error
	if d.Active {
		// Bound only the execve start window (not the interactive session
		// lifetime) so a D-state hang is abandoned, not a healthy long shell.
		startErr = a.spawnPolicy.RunStartWithCleanup(
			startFn,
			a.spawnTimeout(),
			func() { _ = sess.Close() },
			func(err error) {
				// If the timer won just after Start returned nil, Start did not
				// observe the later Close and therefore did not self-reap.
				if err == nil {
					sess.KillAndWaitAfterAbandonedStart()
				}
			},
		)
	} else {
		startErr = startFn()
	}
	if startErr != nil {
		reason := "exec_failed"
		if errors.Is(startErr, spawnsafe.ErrSpawnTimeout) || errors.Is(startErr, spawnsafe.ErrTooManyWedged) {
			reason = remoteFSFailReason(startErr)
		}
		a.cfg.Logger.Warn("agent: pty start", "err", startErr, "pid", pid)
		_ = sess.Close()
		a.replyRunChunk(nc, msg.Reply, proto.RunChunk{
			Kind: "failed", PID: pid, Reason: reason,
		})
		a.pubPtyFailed(nc, pid, reason, startErr.Error())
		return
	}

	// Capture the G.1 PID-reuse triple at fork time. start_time_ticks
	// is /proc/<os_pid>/stat field 22 — the kernel-stamped boot tick
	// when this process started. agent echoes it on every register
	// snapshot; broker compares against the value persisted at
	// proc.Insert time.
	startTicks, _ := readStartTimeTicks(sess.OSPID())
	rec := &procRec{
		sess:           sess,
		osPID:          sess.OSPID(),
		startTimeTicks: startTicks,
		startedAt:      time.Now().UTC(),
		// h1 D2: arm the ctl-liveness reaper ONLY when the ctl advertised
		// keepalives (0 = pre-h1 ctl = never reap). lastKA seeds at spawn so
		// the first window measures from the run's start, not the epoch.
		kaGrace: kaGraceFor(req.KAIntervalMS),
		lastKA:  time.Now(),
	}
	a.registerProc(pid, rec)
	// Hook into the shared process state machine: `tether run`
	// processes must show up in `tether ps` and produce normal
	// audit.proc{kind:start,exit} records, the same as `tether exec`.
	// Failures BEFORE this point (attach_timeout / pty_alloc_failed /
	// exec_failed) deliberately do NOT pub proc.started — no child
	// was started, so no SQLite row should exist. Those surface via
	// PtyFailedEvent → audit.proc{kind:reason} instead.
	// h1 C1: through the courier — see handleExecForwarded's note.
	a.courier.enqueueStarted(pid, req.Argv, req.ActorFP,
		readBootID(), startTicks, rec.startedAt)
	a.replyRunChunk(nc, msg.Reply, proto.RunChunk{Kind: "started", PID: pid})

	// .in and .resize are SESSION-scoped wildcard subscriptions installed once per NATS
	// session (startPtyIntake), not per-run subscriptions on the spawn conn.
	//
	// origin: prerelease audit agent-run/L3-F2. They used to be subscribed HERE, on the
	// conn captured when the child was spawned. A session rebuild — which rebuildOntoVoter
	// performs for ordinary events like a drain or a roster change — closes that conn, and
	// the interactive session went deaf: keystrokes and window resizes stopped arriving,
	// while `.out` kept streaming, so the user saw a live terminal that ignored them.
	// pty.<pid>.out is a publisher (master→bus) and resolves its conn per flush.
	//
	// The reviewer's minimal alternative — hang up the session when the ctl link drops —
	// was rejected: it trades "invisible" for "killed". Every rebuildOntoVoter would then
	// SIGHUP the foreground job, and a long non-tmux task would simply die.
	//
	// This is the same shape startCtlLiveness already uses for `pty.*.ka`, and the ACL
	// (internal/auth/permissions.go) already grants the wildcard.

	outSubj := proto.SubjPtyOut(a.cfg.SID, pid)
	pumpDone := make(chan struct{})
	go func() {
		a.pumpMasterToBus(nc, sess.Master, outSubj)
		close(pumpDone)
	}()

	exitCode, _ := sess.Wait()
	// h1 D3: latch waitDone the INSTANT Wait returns, under procsMu — from
	// here to unregisterProc the child's pgid is dead and can be RECYCLED,
	// and the liveness reaper must never SIGHUP a recycled pgid. Checked (not
	// cmd.ProcessState — reading that races Wait's own write) by
	// shouldReapRun before any Hangup.
	a.procsMu.Lock()
	if lrec, ok := a.procs[pid]; ok {
		lrec.waitDone = true
	}
	a.procsMu.Unlock()
	// Wait returning means the child closed its end of the slave;
	// reading from the master will hit EOF imminently. Wait for the
	// pump to drain so the very last bytes (e.g. "$ exit\r\n") aren't
	// dropped.
	select {
	case <-pumpDone:
	case <-time.After(200 * time.Millisecond):
	}

	// No per-run .in/.resize subscriptions to tear down: they are session-scoped now
	// (startPtyIntake) and the session finalizer owns them. unregisterProc is what stops
	// this pid from being routed — ptySessionFor returns nil the moment the record goes.
	a.unregisterProc(pid)
	_ = sess.Close()

	// Symmetric with the started enqueue above: the courier delivers
	// ev.proc.exit (ACKed, current-conn) so the broker transcribes the row to
	// EXITED + writes audit.proc{kind:exit}. Enqueued BEFORE the lifecycle
	// chunk so delivery is already in flight when ctl reacts to exit.
	a.courier.enqueueExit(pid, exitCode)
	// The exit lifecycle chunk rides the CURRENT conn — see liveConn (exec.go), which
	// now carries this reasoning for both verbs. It used to live here, applied to this
	// one publish, while exec's chunks all rode a captured conn (prerelease audit
	// agent-exec/L3-F1).
	a.replyRunChunk(liveConn(a, nc), msg.Reply, proto.RunChunk{
		Kind: "exit", PID: pid, ExitCode: exitCode,
	})
	a.cfg.Logger.Info("agent: run exit", "pid", pid, "code", exitCode)
}

// waitForAttachOnSub blocks for at most attachDeadline waiting for
// the ctl's PtyAttachEvent on a PRE-ESTABLISHED sub. Returns (cols,
// rows, true) on success; (0, 0, false) on timeout. The caller owns
// sub.Unsubscribe.
//
// The sub MUST be created (and ideally Flushed) BEFORE the agent
// publishes the ready chunk back to ctl — otherwise ctl can race
// past us and fire attach into the void. See dispatchRun.
func (a *Agent) waitForAttachOnSub(sub *nats.Subscription, pid string) (int, int, bool) {
	ctx, cancel := context.WithTimeout(context.Background(), attachDeadline())
	defer cancel()
	msg, err := sub.NextMsgWithContext(ctx)
	if err != nil {
		_ = pid // pid kept in signature for log-context parity
		return 0, 0, false
	}
	var ev proto.PtyAttachEvent
	if err := json.Unmarshal(msg.Data, &ev); err != nil {
		// Malformed attach is treated as timeout — ctl violated the
		// protocol; safest is to give up and let the operator retry.
		return 0, 0, false
	}
	return ev.Cols, ev.Rows, true
}

// pumpMasterToBus reads the PTY master and publishes byte chunks to
// outSubj. Returns when EOF is reached (child closed its end). 4KB or
// 50ms whichever first — see architecture C.5.2.
func (a *Agent) pumpMasterToBus(nc *nats.Conn, master io.Reader, outSubj string) {
	buf := make([]byte, runChunkSize)
	pending := make([]byte, 0, runChunkSize)
	flushTimer := time.NewTimer(runFlushInterval)
	defer flushTimer.Stop()

	// resetTimer drains the channel if a tick is already queued and
	// arms a fresh runFlushInterval. MUST be called every time the
	// timer fires (even when pending is empty) so the periodic flush
	// keeps ticking. Otherwise time.NewTimer fires once and stops,
	// and any future PTY output that doesn't fill 4KB sits in
	// `pending` until the child closes (operator pain: tmux echo
	// only appears in batches at session end).
	resetTimer := func() {
		if !flushTimer.Stop() {
			select {
			case <-flushTimer.C:
			default:
			}
		}
		flushTimer.Reset(runFlushInterval)
	}

	flush := func() {
		if len(pending) == 0 {
			resetTimer()
			return
		}
		// Make a copy because nats.Publish may queue and reuse our buf.
		out := make([]byte, len(pending))
		copy(out, pending)
		// PER FLUSH, not the conn captured at spawn.
		//
		// origin: prerelease audit round 2, I-F2. L3-F2 moved the INPUT direction to a
		// session-scoped subscription and left the OUTPUT direction publishing on the
		// spawn conn, while a comment at the call site claimed this pump "resolves its
		// conn per flush". After a session rebuild that conn is closed, so the terminal
		// went silent in the direction the user actually watches — and the comment said
		// otherwise.
		live := liveConn(a, nc)
		_ = live.Publish(outSubj, out)
		// Force the NATS client to push this chunk to the socket
		// NOW. Without this, nc.Publish only queues into an in-
		// process bufio.Writer; on a high-latency WSS link the
		// internal flusher's batching can stall PTY echo for
		// seconds (operator pain: typing into `tether run -- cat`
		// shows nothing until the child closes). A small Flush
		// timeout means: best-effort push, but never block the
		// pump on a wedged connection.
		_ = live.FlushTimeout(50 * time.Millisecond)
		pending = pending[:0]
		resetTimer()
	}

	readCh := make(chan readResult, 1)
	go func() {
		for {
			n, err := master.Read(buf)
			data := append([]byte(nil), buf[:n]...)
			readCh <- readResult{data: data, err: err}
			if err != nil {
				return
			}
		}
	}()

	for {
		select {
		case r := <-readCh:
			if len(r.data) > 0 {
				pending = append(pending, r.data...)
				if len(pending) >= runChunkSize {
					flush()
				}
			}
			if r.err != nil {
				flush()
				return
			}
		case <-flushTimer.C:
			flush()
		}
	}
}

type readResult struct {
	data []byte
	err  error
}

// handleKillForwarded is the Ctrl-C path: ctl sends KillReq{PID, Signal},
// agent looks up the live PTY session and sends the signal to its
// process group. Reply is a small KillResp ack.
func (a *Agent) handleKillForwarded(nc *nats.Conn, msg *nats.Msg) {
	var req proto.KillReq
	if err := json.Unmarshal(msg.Data, &req); err != nil {
		a.replyKill(msg, proto.KillResp{Code: "json_parse", Error: err.Error()})
		return
	}
	if req.PID == "" {
		a.replyKill(msg, proto.KillResp{Code: "pid_required"})
		return
	}
	if req.Signal == 0 {
		req.Signal = 2 // SIGINT default
	}
	sess, ok := a.lookupProc(req.PID)
	if !ok {
		a.replyKill(msg, proto.KillResp{Code: "pid_unknown"})
		return
	}
	if err := sess.sess.Signal(syscall.Signal(req.Signal)); err != nil {
		a.replyKill(msg, proto.KillResp{Code: "signal_failed", Error: err.Error()})
		return
	}
	a.replyKill(msg, proto.KillResp{OK: true})
}

func (a *Agent) replyKill(msg *nats.Msg, resp proto.KillResp) {
	if msg.Reply == "" {
		return
	}
	body, _ := json.Marshal(resp)
	_ = msg.Respond(body)
}

func (a *Agent) replyRunChunk(nc *nats.Conn, replyTo string, c proto.RunChunk) {
	if replyTo == "" {
		return
	}
	body, err := json.Marshal(c)
	if err != nil {
		a.cfg.Logger.Warn("agent: marshal RunChunk", "err", err)
		return
	}
	if err := nc.Publish(replyTo, body); err != nil {
		a.cfg.Logger.Warn("agent: pub RunChunk", "err", err, "kind", c.Kind)
	}
}

func (a *Agent) pubPtyFailed(nc *nats.Conn, pid, reason, detail string) {
	body, err := json.Marshal(proto.PtyFailedEvent{
		PID: pid, Reason: reason, Detail: detail,
	})
	if err != nil {
		return
	}
	if err := nc.Publish(proto.SubjPtyFailed(a.cfg.SID, pid), body); err != nil {
		a.cfg.Logger.Warn("agent: pub pty.failed", "err", err)
	}
}

// registerExecChild / unregisterExecChild track a live synchronous `tether exec`
// OS child so an admin EVICT can reap it (#26). p may be nil (a start that never
// produced a process) — recorded as a no-op guard so callers need not nil-check.
func (a *Agent) registerExecChild(pid string, p *os.Process) {
	if p == nil {
		return
	}
	a.execChildrenMu.Lock()
	a.execChildren[pid] = p
	a.execChildrenMu.Unlock()
}

func (a *Agent) unregisterExecChild(pid string) {
	a.execChildrenMu.Lock()
	delete(a.execChildren, pid)
	a.execChildrenMu.Unlock()
}

// reapManagedChildren SIGKILLs every process this agent still manages — the
// synchronous `tether exec` children AND the interactive `run` PTY sessions. It
// is the EVICT contract (#26): once the operator has evicted this node, the
// daemon self-exits, and on a bare setsid-nohup host (no systemd cgroup) nothing
// else would reap the managed children — they would linger in the host process
// table. Each exec child is started Setpgid, so a single kill(-pgid) reaps the
// child AND any subtree it forked; the PTY session kills its own Setsid group.
// Best-effort: a child that already exited (ESRCH) is fine.
func (a *Agent) reapManagedChildren() {
	a.execChildrenMu.Lock()
	execs := make([]*os.Process, 0, len(a.execChildren))
	for _, p := range a.execChildren {
		execs = append(execs, p)
	}
	a.execChildrenMu.Unlock()
	for _, p := range execs {
		// Setpgid => pgid == child pid; kill the whole group (child + descendants).
		if err := syscall.Kill(-p.Pid, syscall.SIGKILL); err != nil {
			_ = p.Kill() // fallback: the group is gone / never formed → target the pid directly
		}
	}

	a.procsMu.Lock()
	ptys := make([]*procRec, 0, len(a.procs))
	for _, r := range a.procs {
		ptys = append(ptys, r)
	}
	a.procsMu.Unlock()
	for _, r := range ptys {
		_ = r.sess.Signal(syscall.SIGKILL) // the PTY session signals its own Setsid process group
	}
}

func (a *Agent) registerProc(pid string, rec *procRec) {
	a.procsMu.Lock()
	defer a.procsMu.Unlock()
	a.procs[pid] = rec
}

func (a *Agent) unregisterProc(pid string) {
	a.procsMu.Lock()
	defer a.procsMu.Unlock()
	delete(a.procs, pid)
}

func (a *Agent) lookupProc(pid string) (*procRec, bool) {
	a.procsMu.Lock()
	defer a.procsMu.Unlock()
	r, ok := a.procs[pid]
	return r, ok
}

// normalizedSize returns sane defaults for cols/rows when ctl forgot.
// 80x24 is the conventional dumb-terminal default.
func normalizedSize(cols, rows int) (int, int) {
	if cols <= 0 {
		cols = 80
	}
	if rows <= 0 {
		rows = 24
	}
	return cols, rows
}

// ptyFailureIsTransient decides which of the two PTY-failure wire codes a pty.Allocate error earns.
//
// origin: line-2 external review M18 / PC-3. This predicate used to be written inline in handleRunReq,
// which made the DECISION untestable while leaving the two code literals testable — and the review
// measured the consequence: replacing the condition with `if false` (the transient branch permanently
// unreachable) left ./internal/agent/ and ./cmd/tether/ both green. The wire-code coverage gate only
// asserts that each code literal EXISTS somewhere, so it could not see that one of them had become
// dead. Extracting the predicate is what gives the criterion itself a test
// (TestPTYFailureTransientClassification).
//
// The two code literals stay written out at the two call sites, unchanged: the coverage gate resolves
// codes statically and a `reason` variable there would need an unresolvedCodeSites exemption, which is an
// exemption avoidable by writing the code out.
//
// EMFILE / ENFILE  the process or the host is out of file descriptors. Self-healing — the next attempt
//
//	after some sessions close will succeed, so pty_alloc_failed is retryable.
//
// anything else    /dev/ptmx is absent, not permitted, or the container forbids it. A property of the
//
//	HOST, not of this instant; pty_unavailable tells automation to stop retrying.
//
// ptyTransientErrnos are the allocation failures that clear on their own as sessions close.
//
// ENOSPC is the one that matters most and was missing. origin: line-2 external review M17 / PC-2 / F2 / D5.
// Opening /dev/ptmx goes ptmx_open -> devpts_new_index, and when the devpts instance is at its limit
// (/proc/sys/kernel/pty/max, minus pty_reserve for non-root, or a `max=` mount option) that returns
// -ENOSPC, not EMFILE. So PTY EXHAUSTION -- the single most likely transient PTY failure on a busy host,
// and the exact thing pty_alloc_failed exists to name -- was landing in the terminal branch and telling
// automation to stop retrying something that clears in seconds.
//
// Measured, not inferred: in a privileged container with devpts mounted `newinstance,max=1`, the second
// open returns `no space left on device` with EMFILE=false ENFILE=false ENOSPC=true. This host's
// /proc/sys/kernel/pty/max is 4096, so an ordinary busy broker can reach it.
//
// ENOMEM and EAGAIN are included on the same principle: both are resource-pressure errnos that a later
// attempt can win. The list is a variable while the two Reason literals at the call site stay written out
// -- the wire-code coverage gate resolves codes statically, and an errno list is not a code.
var ptyTransientErrnos = []syscall.Errno{
	syscall.EMFILE, // this process is out of file descriptors
	syscall.ENFILE, // the host's file table is full
	syscall.ENOSPC, // devpts index exhaustion -- the pty limit, see above
	syscall.ENOMEM, // kernel allocation pressure
	syscall.EAGAIN, // resource temporarily unavailable
}

func ptyFailureIsTransient(err error) bool {
	for _, e := range ptyTransientErrnos {
		if errors.Is(err, e) {
			return true
		}
	}
	return false
}

// ---- h1 D: ctl-liveness contract for interactive runs -----------------------
// origin: docs/reviews/h1-plan.md workstream D (2026-08-04 incident, zombie
// class b: `tether run tmux attach` + a killed terminal window = a process
// that lives forever because nobody ever hangs its PTY up — contrast sshd,
// where client death SIGHUPs the session).

// ctlLivenessTick is a var (not const) ONLY so the package tests can shrink
// the reaper cadence; production never mutates it.
var ctlLivenessTick = 5 * time.Second

const (
	// kaGraceFloor: the reap window never drops below 3 minutes (h1 plan Q3).
	// A laptop lid-close under 3min resumes its keepalives unharmed; longer
	// suspends hang the session up — sshd-with-ClientAliveInterval semantics,
	// documented in usage.md. For tmux the cost is a reattach.
	kaGraceFloor = 3 * time.Minute
	// ctlProbeTimeout bounds the reaper's round-trip proof (h1 plan
	// critique-4 BLOCKER): nc.Status() stays CONNECTED for minutes on a
	// silent partition (nats.go only notices via socket error or its 2min
	// ping cycle), so before ANY reap the conn must prove it can round-trip.
	ctlProbeTimeout = 2 * time.Second
)

// ctlConnProbe is the reaper's round-trip proof, indirected through a var so
// the tests can pin it INDEPENDENTLY of the `IsConnected()` guard.
//
// That independence is the whole point (internal review): the two guards are
// defense-in-depth, and against an embedded NATS server they cover for each
// other — shutting the server down flips nats.go to reconnecting, so
// IsConnected() alone already stops the reap and a deleted probe stays
// invisible. The state where ONLY the probe saves us — the socket is up,
// nats.go still says CONNECTED, and nothing can round-trip (a blackholed
// path; nats.go needs its 2min ping cycle to notice) — is the production
// hazard and cannot be produced with an in-process server. Injecting the
// probe is how that hazard gets a real test instead of a hopeful comment.
var ctlConnProbe = func(nc *nats.Conn) error { return nc.FlushTimeout(ctlProbeTimeout) }

// kaGraceFor derives the reap window from a ctl-advertised keepalive
// interval: 0 = not advertised = never reap; otherwise the interval clamps to
// [1s, 60s] and the grace is max(6×interval, kaGraceFloor) — six missed beats
// or three minutes, whichever is longer.
func kaGraceFor(intervalMS int) time.Duration {
	if intervalMS <= 0 {
		return 0
	}
	iv := time.Duration(intervalMS) * time.Millisecond
	if iv < time.Second {
		iv = time.Second
	}
	if iv > time.Minute {
		iv = time.Minute
	}
	if g := 6 * iv; g > kaGraceFloor {
		return g
	}
	return kaGraceFloor
}

// reapVerdict is shouldReapRun's decision for one run on one reaper tick.
type reapVerdict int

const (
	reapNone    reapVerdict = iota // healthy / not armed / already handled
	reapConfirm                    // expired once with a proven conn — arm the second strike
	reapNow                        // second probe-backed strike — hang it up
)

// shouldReapRun is the PURE reap decision (table-tested): a run is reaped
// only when (armed) ∧ (not already reaped) ∧ (child not already exited —
// recycled-pgid guard) ∧ (keepalive silence exceeds grace) ∧ (the agent's
// conn was verifiably healthy on THIS tick) ∧ (this is the second
// consecutive such tick). Anything less reaps healthy sessions: an
// unhealthy-conn tick proves only that the AGENT is deaf, not that the ctl
// is dead.
func shouldReapRun(now time.Time, rec *procRec, connProven bool) reapVerdict {
	if rec.kaGrace <= 0 || rec.reaped || rec.waitDone {
		return reapNone
	}
	if now.Sub(rec.lastKA) <= rec.kaGrace {
		return reapNone
	}
	if !connProven {
		return reapNone
	}
	if !rec.probeConfirmed {
		return reapConfirm
	}
	return reapNow
}

// ctlLivenessReaper is the per-session watchdog goroutine (h1 D3): every tick
// it inspects armed runs and hangs up those whose ctl has been silent past
// grace — with the false-positive ladder above. Started in session() beside
// the other per-session loops; exits with runCtx.
// ctx-root: per-session background loop.
func (a *Agent) ctlLivenessReaper(ctx context.Context) {
	t := time.NewTicker(ctlLivenessTick)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
		}
		nc := a.ncBox.Load()
		if nc == nil || !nc.IsConnected() {
			// Unobservable silence NEVER counts against the ctl: while the
			// agent itself is deaf, every stamp is pushed forward so the
			// window restarts once the conn heals.
			a.restampProcKA()
			continue
		}
		now := time.Now()
		a.procsMu.Lock()
		anyExpired := false
		for _, rec := range a.procs {
			if rec.kaGrace > 0 && !rec.reaped && !rec.waitDone && now.Sub(rec.lastKA) > rec.kaGrace {
				anyExpired = true
			}
		}
		a.procsMu.Unlock()
		if !anyExpired {
			continue
		}
		// Round-trip proof BEFORE acting (never under procsMu — Flush blocks).
		// A failed probe means the conn is lying about its health: restamp,
		// don't reap.
		if err := ctlConnProbe(nc); err != nil {
			a.restampProcKA()
			continue
		}
		var hangups []*procRec
		a.procsMu.Lock()
		for pid, rec := range a.procs {
			switch shouldReapRun(now, rec, true) {
			case reapNone:
				// Healthy / unarmed / already handled — nothing to do. Listed
				// explicitly (never a `default:`) so a future verdict cannot
				// silently inherit the no-op branch: see
				// test/determinism/enum_switch_default_test.go.
			case reapConfirm:
				rec.probeConfirmed = true
			case reapNow:
				rec.reaped = true
				hangups = append(hangups, rec)
				a.cfg.Logger.Info("agent: hanging up interactive run — ctl keepalive silent past grace",
					"pid", pid, "grace", rec.kaGrace, "silent_for", now.Sub(rec.lastKA))
			}
		}
		a.procsMu.Unlock()
		for _, rec := range hangups {
			// Outside procsMu: Hangup takes the session mutex and kills a
			// process group. After SIGHUP the run handler's Wait returns and
			// the NORMAL exit path (courier exit event, rc=-1 for a
			// signal-death per pty.Wait) clears the ps row — no special state.
			rec.sess.Hangup()
		}
	}
}

// startSessionIntakes wires every SESSION-scoped subscription in one place: the PTY
// keystroke/resize intake and the ctl-liveness keepalive intake.
//
// They are grouped because they share the one property that matters — a subscription
// established per RUN, on the conn captured when that run started, goes deaf the moment
// the session is rebuilt, and a rebuild is an ordinary consequence of a drain or a
// roster change. Keeping them together is also what keeps session() itself inside the
// maintainability gate: two call sites there cost more than one.
//
// A package-level function taking *Agent: the structural-budget ratchet pins this
// type's method count exactly.
func startSessionIntakes(runCtx context.Context, a *Agent, nc *nats.Conn, fin *sessionFinalizer) error {
	if err := startPtyIntake(a, nc, fin); err != nil {
		return err
	}
	return a.startCtlLiveness(runCtx, nc, fin)
}

// startPtyIntake wires the SESSION-scoped `.in` and `.resize` intake: one wildcard
// subscription each, routed to the live PTY by pid through a.procs.
//
// SESSION-SCOPED FOR THE SAME REASON THE KEEPALIVE INTAKE IS: per-run wiring on a
// captured conn goes deaf the moment the session is rebuilt, and a rebuild is an
// ordinary consequence of a drain or a roster change.
//
// origin: prerelease audit agent-run/L3-F2 — see the note at the spawn site for what
// per-run subscriptions on a captured conn cost an interactive session.
//
// A pid with no record is silently ignored: the run has already exited, and answering
// would mean writing to a closed master.
//
// A package-level function taking *Agent, not a method: the structural-budget ratchet
// pins this type's method count exactly.
func startPtyIntake(a *Agent, nc *nats.Conn, fin *sessionFinalizer) error {
	pidOf := func(subject, suffix string) string {
		parts := strings.Split(subject, ".")
		if len(parts) < 2 || parts[len(parts)-1] != suffix {
			return ""
		}
		return parts[len(parts)-2]
	}
	writeIn := func(m *nats.Msg) {
		// ctx-none: nats.go MsgHandler has no ctx.
		// WriteInput, not sess.Master.Write — origin: prerelease audit round 2,
		// I-F5. ptySessionFor releases procsMu before returning, so reading the
		// Master field here was unsynchronized against Close()'s `s.Master = nil`.
		if sess := ptySessionFor(a, pidOf(m.Subject, "in")); sess != nil {
			_, _ = sess.WriteInput(m.Data)
		}
	}
	// NODE-SCOPED FIRST — origin: prerelease audit round 2, I-F6. This is the
	// subscription that carries keystrokes on an upgraded fleet, and the server delivers
	// it only to the agent it is addressed to.
	subInNode, err := nc.Subscribe(
		proto.SubjectPrefix+".s."+a.cfg.SID+".node."+nidOf(a)+".pty.*.in", writeIn)
	if err != nil {
		return fmt.Errorf("agent: subscribe node-scoped pty intake: %w", err)
	}
	fin.addBoundedCleanup(func() { _ = subInNode.Unsubscribe() })

	// THE LEGACY SESSION-SCOPED FORM, for a ctl older than the node-scoped subject.
	// It is the fan-out I-F6 reports, kept only for the N-1 window.
	//
	// A message that carries PtyNodeHeader is DROPPED here: its sender also published a
	// node-scoped copy, which the subscription above has already delivered. Without that
	// check an upgraded ctl talking to an upgraded agent would write every keystroke
	// twice, and a keystroke stream cannot be de-duplicated after the fact.
	subIn, err := nc.Subscribe(
		proto.SubjectPrefix+".s."+a.cfg.SID+".pty.*.in",
		func(m *nats.Msg) {
			if m.Header.Get(proto.PtyNodeHeader) != "" {
				return
			}
			writeIn(m)
		},
	)
	if err != nil {
		return fmt.Errorf("agent: subscribe pty intake: %w", err)
	}
	fin.addBoundedCleanup(func() { _ = subIn.Unsubscribe() })

	doResize := func(m *nats.Msg) {
		// ctx-none: nats.go MsgHandler has no ctx.
		sess := ptySessionFor(a, pidOf(m.Subject, "resize"))
		if sess == nil {
			return
		}
		var ev proto.PtyResizeEvent
		if err := json.Unmarshal(m.Data, &ev); err != nil {
			return
		}
		_ = sess.SetSize(ev.Cols, ev.Rows)
	}
	subResizeNode, err := nc.Subscribe(
		proto.SubjectPrefix+".s."+a.cfg.SID+".node."+nidOf(a)+".pty.*.resize", doResize)
	if err != nil {
		return fmt.Errorf("agent: subscribe node-scoped pty resize: %w", err)
	}
	fin.addBoundedCleanup(func() { _ = subResizeNode.Unsubscribe() })

	// Legacy, same reasoning and same header drop as `.in` above. A resize IS idempotent,
	// so a duplicate would be harmless here — the check is kept anyway so the two intakes
	// have one shape, and so a reader does not have to work out which of them is allowed
	// to double-deliver.
	subResize, err := nc.Subscribe(
		proto.SubjectPrefix+".s."+a.cfg.SID+".pty.*.resize",
		func(m *nats.Msg) {
			if m.Header.Get(proto.PtyNodeHeader) != "" {
				return
			}
			doResize(m)
		},
	)
	if err != nil {
		return fmt.Errorf("agent: subscribe pty resize: %w", err)
	}
	fin.addBoundedCleanup(func() { _ = subResize.Unsubscribe() })
	return nil
}

// ptySessionFor returns the live PTY for pid, or nil when the run has ended.
//
// A package-level function taking *Agent, not a method: the structural-budget ratchet
// pins this type's method count exactly, and it caught this one on the way in.
func ptySessionFor(a *Agent, pid string) *pty.Session {
	if pid == "" {
		return nil
	}
	a.procsMu.Lock()
	defer a.procsMu.Unlock()
	if rec, ok := a.procs[pid]; ok && !rec.waitDone {
		return rec.sess
	}
	return nil
}

// startCtlLiveness wires the h1 D intake + reaper for one session: ONE
// session-scoped wildcard subscription (never a per-run sub on the spawn
// conn — per-run wiring on a captured conn is exactly the trap behind zombie
// class a), the deaf-window restamp, and the reaper goroutine. Bounded
// teardown matches subFwd's: Unsubscribe takes nc.mu, so it goes through the
// finalizer rather than a plain defer (origin: internal review CT-1 / #72).
func (a *Agent) startCtlLiveness(runCtx context.Context, nc *nats.Conn, fin *sessionFinalizer) error {
	subKA, err := nc.Subscribe(
		proto.SubjectPrefix+".s."+a.cfg.SID+".pty.*.ka",
		func(msg *nats.Msg) {
			// ctx-none: nats.go MsgHandler has no ctx.
			// Subject: <prefix>.s.<sid>.pty.<pid>.ka — pid is token n-2.
			parts := strings.Split(msg.Subject, ".")
			if len(parts) < 2 || parts[len(parts)-1] != "ka" {
				return
			}
			a.touchProcKA(parts[len(parts)-2])
		},
	)
	if err != nil {
		return fmt.Errorf("agent: subscribe pty keepalive: %w", err)
	}
	fin.addBoundedCleanup(func() { _ = subKA.Unsubscribe() })
	// A session (re)build reopens the intake after a deaf window — restamp so
	// the silence that accumulated while we could not HEAR keepalives never
	// counts against the ctl.
	a.restampProcKA()
	go a.ctlLivenessReaper(runCtx)
	return nil
}

// touchProcKA stamps a keepalive receipt for pid (the `.ka` subscription
// handler). A beat also disarms a pending second strike.
func (a *Agent) touchProcKA(pid string) {
	a.procsMu.Lock()
	if rec, ok := a.procs[pid]; ok {
		rec.lastKA = time.Now()
		rec.probeConfirmed = false
	}
	a.procsMu.Unlock()
}

// restampProcKA pushes every armed run's keepalive stamp to now — called
// whenever keepalive silence stops being attributable to the ctl (conn
// unhealthy tick, failed probe, reconnect, fresh `.ka` subscription after a
// session rebuild).
func (a *Agent) restampProcKA() {
	now := time.Now()
	a.procsMu.Lock()
	for _, rec := range a.procs {
		if rec.kaGrace > 0 && !rec.reaped {
			rec.lastKA = now
			rec.probeConfirmed = false
		}
	}
	a.procsMu.Unlock()
}
