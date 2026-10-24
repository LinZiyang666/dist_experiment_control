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
	"sort"
	"strings"
	"syscall"
	"time"

	"github.com/LinZiyang666/tether/internal/proc"
	"github.com/LinZiyang666/tether/internal/procrequest"
	"github.com/LinZiyang666/tether/internal/proto"
	"github.com/LinZiyang666/tether/internal/pty"
	"github.com/LinZiyang666/tether/internal/ptysession"
	"github.com/LinZiyang666/tether/internal/spawnsafe"
	"github.com/LinZiyang666/tether/internal/workload"
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

// handleRunForwarded executes the full run lifecycle. msg.Reply is the
// ctl's _INBOX where we publish RunChunk lifecycle events; PTY byte
// streams travel on a separate `pty.<pid>.*` subject family.
func (a *Agent) handleRunForwarded(nc *nats.Conn, msg *nats.Msg) {
	if msg.Reply == "" {
		a.cfg.Logger.Warn("agent: run.req.forwarded without Reply inbox")
		return
	}
	// origin: hot-update S4 plan §2.2. Legacy must obey the same pending
	// rollback gate as reliable create; falling back cannot bypass it.
	if err := checkPTYAdmission(a); err != nil {
		// origin: hot-update S4 plan §9.2 / N1-REFUSAL. v0.6.0 knows try_again as 75; keep the upgrade reason in detail.
		a.replyRunChunk(nc, msg.Reply, proto.RunChunk{Kind: "failed", Reason: "try_again: agent_upgrading: image switch in progress; retry"})
		return
	}
	// origin: hot-update S3 plan §4.0. Hold admission for the whole handler.
	release, admitted := a.gate.admit(gateRun)
	if !admitted {
		// origin: hot-update S4 plan §9.2 / N1-REFUSAL. v0.6.0 knows try_again as 75; keep the upgrade reason in detail.
		a.replyRunChunk(nc, msg.Reply, proto.RunChunk{Kind: "failed", Reason: "try_again: agent_upgrading: image switch in progress; retry"})
		return
	}
	defer release()

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

	if reason, err := startAllocatedRunPTY(a, pid, &req, sess); err != nil {
		a.replyRunChunk(nc, msg.Reply, proto.RunChunk{Kind: "failed", PID: pid, Reason: reason})
		a.pubPtyFailed(nc, pid, reason, err.Error())
		return
	}
	row, _ := a.workloads.Lookup(pid)
	// h1 D2: arm only when ctl advertised keepalives; seed the first
	// grace window at spawn, never at the epoch.
	rec := &procRec{sess: sess, osPID: row.OSPID, startTimeTicks: row.StartTimeTicks,
		startedAt: row.StartedAt, kaGrace: kaGraceFor(req.KAIntervalMS), lastKA: time.Now()}
	master := sess.Master // capture before an intake owner can explicitly close the session
	rec.input = newLegacyRunInput(a, pid, sess, nc, msg.Reply)
	a.registerProc(pid, rec)
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
		a.pumpMasterToBus(nc, master, outSubj)
		close(pumpDone)
	}()

	// origin: hot-update S4 plan §3.3 / §7.1. Descendants may retain the
	// slave after the leader exits. Drain the real source before releasing
	// the reap hold; no timer is evidence that the last slave has closed.
	<-pumpDone
	rec.input.Stop()
	_ = a.workloads.AllowReap(pid)
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

	// No per-run .in/.resize subscriptions to tear down: they are session-scoped now
	// (startPtyIntake) and the session finalizer owns them. unregisterProc is what stops
	// this pid from being routed — legacyPTYInputFor returns nil the moment the record goes.
	_ = sess.Close()

	// Symmetric with the started enqueue above: the courier delivers
	// ev.proc.exit (ACKed, current-conn) so the broker transcribes the row to
	// EXITED + writes audit.proc{kind:exit}. Enqueued BEFORE the lifecycle
	// chunk so delivery is already in flight when ctl reacts to exit.
	a.courier.enqueueExit(pid, exitCode)
	a.unregisterProc(pid)
	_ = a.workloads.Forget(pid)
	// The exit lifecycle chunk rides the CURRENT conn — see liveConn (exec.go), which
	// now carries this reasoning for both verbs. It used to live here, applied to this
	// one publish, while exec's chunks all rode a captured conn (prerelease audit
	// agent-exec/L3-F1).
	a.replyRunChunk(liveConn(a, nc), msg.Reply, proto.RunChunk{
		Kind: "exit", PID: pid, ExitCode: exitCode,
	})
	a.cfg.Logger.Info("agent: run exit", "pid", pid, "code", exitCode)
}

func startRunPTY(a *Agent, pid string, req *proto.RunReq) (*pty.Session, string, error) {
	cols, rows := normalizedSize(req.Cols, req.Rows)
	sess, err := pty.Allocate(cols, rows)
	if err != nil {
		if ptyFailureIsTransient(err) {
			return nil, "pty_alloc_failed", err
		}
		return nil, "pty_unavailable", err
	}
	if reason, err := startAllocatedRunPTY(a, pid, req, sess); err != nil {
		return nil, reason, err
	}
	return sess, "", nil
}

// startAllocatedRunPTY also accepts the legacy master allocated before ready.
// Both paths therefore share safe spawn, courier ordering and draining adoption.
// origin: hot-update S4 plan §7.1
func startAllocatedRunPTY(a *Agent, pid string, req *proto.RunReq, sess *pty.Session) (string, error) {
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
		_ = sess.Close()
		return remoteFSFailReason(ferr), ferr
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
		return reason, startErr
	}

	// Capture the G.1 PID-reuse triple at fork time. start_time_ticks
	// is /proc/<os_pid>/stat field 22 — the kernel-stamped boot tick
	// when this process started. Every register echoes it, and the broker
	// compares it against the value persisted at proc.Insert time.
	// Capture before adoption, while this session still owns the child.
	startTicks, _ := readStartTimeTicks(sess.OSPID())
	id := workload.Identity{ID: pid, OSPID: sess.OSPID(), BootID: readBootID(),
		StartTimeTicks: startTicks, StartedAt: time.Now().UTC()}
	// Hook into the shared process state machine: `tether run`
	// processes must show up in `tether ps` and produce normal
	// audit.proc{kind:start,exit} records, the same as `tether exec`.
	// Failures BEFORE this point (attach_timeout / pty_alloc_failed /
	// exec_failed) deliberately do NOT pub proc.started — no child
	// was started, so no SQLite row should exist. Those surface via
	// PtyFailedEvent → audit.proc{kind:reason} instead.
	// h1 C1: through the courier — see handleExecForwarded's note.
	// C-5 (hot-update S1 review P5): enqueued BEFORE adoption, like
	// startExecPipes — a register snapshot taken between an adoption and
	// its started enqueue would list the PTY child running with no pending
	// started, and a broker without its row would orphan-kill it. The
	// child already exists here, so a failed adoption below closes the row
	// it just opened with a -1 exit instead of leaving it RUNNING.
	a.courier.enqueueStarted(pid, req.Argv, req.ActorFP, id.BootID, startTicks, id.StartedAt)
	// origin: hot-update S4 plan §3.3 / §7.1. The table must hold the leader
	// until the last slave ends; a leader exit alone cannot delimit PTY output.
	if err := sess.AdoptDraining(&a.workloads, id); err != nil {
		_ = sess.Signal(syscall.SIGKILL)
		_, _ = sess.Wait() // failed adoption leaves the session as the sole waiter
		_ = sess.Close()
		a.courier.enqueueExit(pid, -1)
		return "exec_failed", err
	}
	return "", nil
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

// Publish against the current connection for every flush; the PTY owner batches the bytes.
// origin: hot-update S5 plan §10 / AGENT-SLIM-S5.
func (a *Agent) pumpMasterToBus(nc *nats.Conn, master io.Reader, outSubj string) {
	pty.PumpLegacyOutput(master, func(out []byte) {
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
	})
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
	err := signalManagedProcess(a, req.PID, syscall.Signal(req.Signal))
	if errors.Is(err, workload.ErrUnknown) {
		a.replyKill(msg, proto.KillResp{Code: "pid_unknown"})
		return
	}
	if err != nil {
		a.replyKill(msg, proto.KillResp{Code: "signal_failed", Error: err.Error()})
		return
	}
	a.replyKill(msg, proto.KillResp{OK: true})
}

// signalManagedProcess is shared by the public kill verb and register's orphan
// decisions. Both address a stable task owner, never a detached OS PID list.
// Sessions created by legacy in-process callers may not yet have a manager;
// their Session remains the signal owner. Normal agent PTYs delegate internally.
func signalManagedProcess(a *Agent, pid string, sig syscall.Signal) error {
	if rec, ok := a.lookupProc(pid); ok {
		return rec.sess.Signal(sig)
	}
	return a.workloads.Signal(pid, sig)
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

// execChildIdentity captures the OS identity of a just-spawned `tether exec`
// child once, so the proc.started event, the ownership table row and the
// reliable runtime's Started hook all describe the same (pid, boot, ticks).
// The ownership table (#26) is what lets an admin EVICT reap the child and
// keeps the wait result for reconnect snapshots until the courier takes over;
// startExecPipes adopts with this identity right after enqueueing started.
func execChildIdentity(pid string, p *os.Process) workload.Identity {
	ticks, _ := readStartTimeTicks(p.Pid)
	return workload.Identity{
		ID: pid, OSPID: p.Pid, BootID: readBootID(), StartTimeTicks: ticks,
		StartedAt: time.Now().UTC(),
	}
}

func (a *Agent) unregisterExecChild(pid string) {
	_ = a.workloads.Forget(pid) // unsuccessful starts never entered the table
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
	_ = a.workloads.SignalAll(syscall.SIGKILL)

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

// localProcessSnapshot forms a union with terminal records taking precedence.
// Both child owners enqueue exit BEFORE retiring their live record. Reading
// owners before the courier prevents a missing-pid gap; the overlap is collapsed
// here so the broker never receives two states for the same task in one request.
// h1 C4: register is the courier's exit replay channel, carrying the REAL code
// instead of G.1's missed-exit -1 after a transport outage.
//
// C-5 (hot-update S1): pending started events are merged too. startExecPipes
// enqueues started BEFORE adopting, so between the two courier reads here the
// windows are: pid adopted after owners were read → the courier still holds
// the started → the row is added running+unacked; started ACKed between the
// reads → the table row exists without the flag and the broker already has
// its row → G.1's accepted arm. The reverse (adopted before enqueued) cannot
// happen. Over-reporting StartedUnacked only ever means "do not kill".
func localProcessSnapshot(a *Agent) []proto.LocalProcess {
	byID := make(map[string]proto.LocalProcess)
	a.procsMu.Lock()
	for pid, rec := range a.procs {
		byID[pid] = proto.LocalProcess{PID: pid, State: "running",
			StartedAt: rec.startedAt, StartTimeTicks: rec.startTimeTicks}
	}
	a.procsMu.Unlock()
	for _, row := range a.workloads.Snapshot() {
		p := proto.LocalProcess{PID: row.ID, State: "running",
			StartedAt: row.StartedAt, StartTimeTicks: row.StartTimeTicks}
		if row.Exit != nil {
			rc := row.Exit.Code
			endedAt := row.Exit.EndedAt
			p.State, p.RC, p.EndedAt = "exited", &rc, &endedAt
		}
		byID[p.PID] = p
	}
	for _, p := range a.courier.pendingStartedSnapshot() {
		if existing, ok := byID[p.PID]; ok {
			if existing.State == "running" {
				existing.StartedUnacked = true
				byID[p.PID] = existing
			}
			continue
		}
		byID[p.PID] = p
	}
	for _, p := range a.courier.pendingExitSnapshot() {
		byID[p.PID] = p
	}
	rows := make([]proto.LocalProcess, 0, len(byID))
	for _, p := range byID {
		rows = append(rows, p)
	}
	sort.Slice(rows, func(i, j int) bool { return rows[i].PID < rows[j].PID })
	return rows
}

func normalizedSize(cols, rows int) (int, int) { return pty.NormalizedSize(cols, rows) }

var ptyTransientErrnos = pty.TransientErrnos

func ptyFailureIsTransient(err error) bool { return pty.FailureIsTransient(err) }

// ---- h1 D: ctl-liveness contract for interactive runs -----------------------
// origin: docs/reviews/h1-plan.md workstream D (2026-08-04 incident, zombie
// class b: `tether run tmux attach` + a killed terminal window = a process
// that lives forever because nobody ever hangs its PTY up — contrast sshd,
// where client death SIGHUPs the session).

// ctlLivenessTick is a var (not const) ONLY so the package tests can shrink
// the reaper cadence; production never mutates it.
var ctlLivenessTick = 5 * time.Second

const (
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

func kaGraceFor(intervalMS int) time.Duration { return pty.KeepaliveGraceFor(intervalMS) }

// reapVerdict is shouldReapRun's decision for one run on one reaper tick.
type reapVerdict int

const (
	reapNone    reapVerdict = iota // healthy / not armed / already handled
	reapConfirm                    // expired once with a proven conn — arm the second strike
	reapNow                        // second probe-backed strike — hang it up
)

func shouldReapRun(now time.Time, rec *procRec, connProven bool) reapVerdict {
	return reapVerdict(pty.ShouldReapLegacy(now, rec.kaGrace, rec.lastKA, rec.reaped, rec.waitDone, connProven, rec.probeConfirmed))
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
		// Unobservable silence NEVER counts against the ctl: while the
		// agent itself is deaf, stamps move forward so the legacy window
		// restarts once the connection heals. The round-trip probe also
		// detects a connected socket that is silently blackholed.
		if !provePTYHealth(a, nc) {
			a.restampProcKA()
			continue
		}
		now := time.Now()
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
	for _, suffix := range []string{".pty.*.in", ".pty.*.resize"} {
		if err := subscribeLegacyPTY(a, nc, fin, suffix); err != nil {
			return err
		}
	}
	return nil
}

// ctx-none: nats.go MsgHandler has no ctx.
// The queue writer uses WriteInput, not sess.Master.Write — origin:
// prerelease audit round 2, I-F5. Releasing procsMu before reading
// Master used to race Close()'s `s.Master = nil`. The callback now
// only copies into a bounded queue (hot-update S4 plan §7.1 / T42).
// NODE-SCOPED FIRST — origin: prerelease audit round 2, I-F6. This is the
// subscription that carries keystrokes on an upgraded fleet, and the server delivers
// it only to the agent it is addressed to.
// THE LEGACY SESSION-SCOPED FORM, for a ctl older than the node-scoped subject.
// It is the fan-out I-F6 reports, kept only for the N-1 window.
//
// A message that carries PtyNodeHeader is DROPPED here: its sender also published a
// node-scoped copy, which the subscription above has already delivered. Without that
// check an upgraded ctl talking to an upgraded agent would write every keystroke
// twice, and a keystroke stream cannot be de-duplicated after the fact.
// ctx-none: nats.go MsgHandler has no ctx.
// Legacy, same reasoning and same header drop as `.in` above. A resize IS idempotent,
// so a duplicate would be harmless here — the check is kept anyway so the two intakes
// have one shape, and so a reader does not have to work out which of them is allowed
// to double-deliver.
// origin: hot-update S4 plan §7.1 / T42. One subscription shape owns the
// N-1 header filter for input, resize and KA so none can drift on its own.
func subscribeLegacyPTY(a *Agent, nc *nats.Conn, fin *sessionFinalizer, suffix string) error {
	op := suffix[strings.LastIndexByte(suffix, '.')+1:]
	base := proto.SubjectPrefix + ".s." + a.cfg.SID
	for _, route := range []string{".node." + nidOf(a), ""} {
		nodeScoped := route != ""
		sub, err := nc.Subscribe(base+route+suffix, func(m *nats.Msg) {
			if !nodeScoped && m.Header.Get(proto.PtyNodeHeader) != "" {
				return
			}
			parts := strings.Split(m.Subject, ".")
			q := legacyPTYInputFor(a, parts[len(parts)-2])
			if q == nil {
				return
			}
			ev := pty.LegacyInputEvent{Keepalive: op == "ka"}
			switch op {
			case "in":
				ev.Data = m.Data
			case "resize":
				ev.Resize = &proto.PtyResizeEvent{}
				if json.Unmarshal(m.Data, ev.Resize) != nil {
					return
				}
			}
			q.Enqueue(ev)
		})
		if err != nil {
			return fmt.Errorf("agent: subscribe pty %s intake: %w", op, err)
		}
		fin.addBoundedCleanup(func() { _ = sub.Unsubscribe() })
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
	if err := subscribeLegacyPTY(a, nc, fin, ".pty.*.ka"); err != nil {
		return err
	}
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

type legacyPTYInput = pty.LegacyInput

const legacyPTYQueueEvents = pty.LegacyInputEventLimit

// legacyPTYInputFor returns the legacy input owner for pid, or nil when the run has ended.
//
// A package-level function taking *Agent, not a method: the structural-budget ratchet
// pins this type's method count exactly, and it caught this one on the way in.
func legacyPTYInputFor(a *Agent, pid string) *legacyPTYInput {
	a.procsMu.Lock()
	defer a.procsMu.Unlock()
	if rec := a.procs[pid]; rec != nil && !rec.waitDone {
		return rec.input
	}
	return nil
}

// provePTYHealth is deliberately independent of the legacy wall-clock grace:
// imported reliable actors keep absolute deadlines and decide whether a fresh
// proof counts as either of their two strikes. Never restamp them on reconnect.
// origin: hot-update S4 plan §6.1
func provePTYHealth(a *Agent, nc *nats.Conn) bool {
	generation := a.ptyHealthGeneration.Load()
	healthy := nc != nil && nc.IsConnected()
	var reconnects uint64
	if healthy {
		reconnects = nc.Stats().Reconnects
		healthy = ctlConnProbe(nc) == nil
	}
	healthy = healthy && a.ncBox.Load() == nc && nc.IsConnected() &&
		nc.Stats().Reconnects == reconnects && a.ptyHealthGeneration.Load() == generation
	if a.processRuntime != nil {
		now, err := procrequest.BootNow()
		a.processRuntime.PTYHealth(ptysession.HealthProof{Generation: generation, AtBootNS: now, Healthy: healthy && err == nil,
			Verifier: ptyHealthConnectionProof{agent: a, conn: nc, reconnects: reconnects}})
	}
	return healthy
}

// A successful probe can wait behind a terminal control transaction. Its
// captured generation alone cannot authorize a destructive second strike.
// Recheck the same connection and reconnect counter once the actor holds its
// token; no network round trip or agent method is added here.
// origin: hot-update S4 internal review rv-auth RV-AUTH-3
type ptyHealthConnectionProof struct {
	agent      *Agent
	conn       *nats.Conn
	reconnects uint64
}

func (p ptyHealthConnectionProof) Current(generation uint64) bool {
	return p.conn != nil && p.agent.ncBox.Load() == p.conn && p.conn.IsConnected() &&
		p.conn.Stats().Reconnects == p.reconnects && p.agent.ptyHealthGeneration.Load() == generation
}

func newLegacyRunInput(a *Agent, pid string, sess *pty.Session, nc *nats.Conn, inbox string) *legacyPTYInput {
	return pty.NewLegacyInput(sess, func() { a.touchProcKA(pid) }, func() {
		// origin: hot-update S4 plan §7.1 / T42. Overflow is a failed running
		// task, never a claim that spawn failed or silently discarded bytes.
		a.cfg.Logger.Warn("agent: legacy PTY input overflow", "pid", pid, "detail", "pty input buffer full")
		a.replyRunChunk(liveConn(a, nc), inbox, proto.RunChunk{Kind: "failed", PID: pid, Reason: "pty_input_overflow"})
		sess.Hangup()
		// Overflow is an explicit abort. Even if the leader already exited
		// and its Manager signal guard refuses HUP, close the input source
		// to discharge the writer's blocked-I/O responsibility.
		_ = sess.Close()
	})
}
