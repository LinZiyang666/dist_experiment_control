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
	"io"
	"os"
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
	}
	a.registerProc(pid, rec)
	// Hook into the shared process state machine: `tether run`
	// processes must show up in `tether ps` and produce normal
	// audit.proc{kind:start,exit} records, the same as `tether exec`.
	// Failures BEFORE this point (attach_timeout / pty_alloc_failed /
	// exec_failed) deliberately do NOT pub proc.started — no child
	// was started, so no SQLite row should exist. Those surface via
	// PtyFailedEvent → audit.proc{kind:reason} instead.
	a.pubProcStartedWithTriple(nc, pid, req.Argv, req.ActorFP,
		readBootID(), startTicks, rec.startedAt)
	a.replyRunChunk(nc, msg.Reply, proto.RunChunk{Kind: "started", PID: pid})

	// Two long-running subscriptions for the lifetime of the child:
	//   .in     → write to master
	//   .resize → ioctl on master
	// pty.<pid>.out is a publisher (master→bus).
	subIn, err := nc.Subscribe(proto.SubjPtyIn(a.cfg.SID, pid), func(m *nats.Msg) {
		_, _ = sess.Master.Write(m.Data)
	})
	if err != nil {
		a.cfg.Logger.Warn("agent: subscribe pty.in", "err", err, "pid", pid)
	}
	subResize, err := nc.Subscribe(proto.SubjPtyResize(a.cfg.SID, pid), func(m *nats.Msg) {
		var ev proto.PtyResizeEvent
		if err := json.Unmarshal(m.Data, &ev); err != nil {
			return
		}
		_ = sess.SetSize(ev.Cols, ev.Rows)
	})
	if err != nil {
		a.cfg.Logger.Warn("agent: subscribe pty.resize", "err", err, "pid", pid)
	}

	outSubj := proto.SubjPtyOut(a.cfg.SID, pid)
	pumpDone := make(chan struct{})
	go func() {
		a.pumpMasterToBus(nc, sess.Master, outSubj)
		close(pumpDone)
	}()

	exitCode, _ := sess.Wait()
	// Wait returning means the child closed its end of the slave;
	// reading from the master will hit EOF imminently. Wait for the
	// pump to drain so the very last bytes (e.g. "$ exit\r\n") aren't
	// dropped.
	select {
	case <-pumpDone:
	case <-time.After(200 * time.Millisecond):
	}

	if subIn != nil {
		_ = subIn.Unsubscribe()
	}
	if subResize != nil {
		_ = subResize.Unsubscribe()
	}
	a.unregisterProc(pid)
	_ = sess.Close()

	// Symmetric with pubProcStarted above: emit ev.proc.exit so the
	// broker's existing handleProcEvent transcribes the SQLite row to
	// EXITED + writes audit.proc{kind:exit}. Done BEFORE the lifecycle
	// chunk so the row is updated by the time ctl reacts to exit.
	a.pubProcExit(nc, pid, exitCode)
	a.replyRunChunk(nc, msg.Reply, proto.RunChunk{
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
		_ = nc.Publish(outSubj, out)
		// Force the NATS client to push this chunk to the socket
		// NOW. Without this, nc.Publish only queues into an in-
		// process bufio.Writer; on a high-latency WSS link the
		// internal flusher's batching can stall PTY echo for
		// seconds (operator pain: typing into `tether run -- cat`
		// shows nothing until the child closes). A small Flush
		// timeout means: best-effort push, but never block the
		// pump on a wedged connection.
		_ = nc.FlushTimeout(50 * time.Millisecond)
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
