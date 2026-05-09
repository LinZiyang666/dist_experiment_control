// Implementation of the P5 PTY `run` flow on the agent side. The wire
// shape and ordering live in architecture C.5 / C.5.1.
//
// One handler call drives one full run lifecycle:
//
//   handleRunForwarded
//     ├── allocate PTY (Cols/Rows from RunReq)
//     ├── reply RunChunk{Kind:ready, PID, Cols, Rows}
//     ├── subscribe pty.<pid>.attach (sync, with 3s deadline)
//     │     attach not received → reply RunChunk{Kind:failed, Reason:attach_timeout}
//     │                          + pub PtyFailedEvent on pty.<pid>.failed
//     │                          + close PTY → return
//     ├── pty.Start with the (potentially updated) cols/rows from attach
//     ├── put session into a.procs (kill verb looks here)
//     ├── reply RunChunk{Kind:started}
//     ├── subscribe pty.<pid>.in     → write bytes to PTY master
//     ├── subscribe pty.<pid>.resize → ioctl(TIOCSWINSZ) on master
//     ├── pump master → publish chunks on pty.<pid>.out (4KB or 50ms)
//     ├── Wait → reply RunChunk{Kind:exit, ExitCode}
//     └── prune from a.procs, close PTY, unsubscribe
package agent

import (
	"context"
	"encoding/json"
	"io"
	"syscall"
	"time"

	"github.com/LinZiyang666/tether/internal/proc"
	"github.com/LinZiyang666/tether/internal/proto"
	"github.com/LinZiyang666/tether/internal/pty"
	"github.com/nats-io/nats.go"
)

// attachDeadline is how long the agent waits between publishing
// RunChunk{Kind:ready} and giving up because no attach arrived. P5 spec
// pins this at 3s (architecture C.5.1).
const attachDeadline = 3 * time.Second

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
		a.cfg.Logger.Warn("agent: pty allocate", "err", err, "pid", pid)
		a.replyRunChunk(nc, msg.Reply, proto.RunChunk{
			Kind: "failed", PID: pid, Reason: "pty_alloc_failed",
		})
		a.pubPtyFailed(nc, pid, "pty_alloc_failed", err.Error())
		return
	}

	// Step 3 of C.5.1: announce ready and BLOCK until ctl says "I'm
	// subscribed; here is my real terminal size; you may exec".
	a.replyRunChunk(nc, msg.Reply, proto.RunChunk{
		Kind: "ready", PID: pid, Cols: cols, Rows: rows,
	})

	attachCols, attachRows, ok := a.waitForAttach(nc, pid)
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
	if err := sess.Start(req.Argv, envSliceFromMap(req.Env), req.Cwd); err != nil {
		a.cfg.Logger.Warn("agent: pty start", "err", err, "pid", pid)
		_ = sess.Close()
		a.replyRunChunk(nc, msg.Reply, proto.RunChunk{
			Kind: "failed", PID: pid, Reason: "exec_failed",
		})
		a.pubPtyFailed(nc, pid, "exec_failed", err.Error())
		return
	}

	a.registerProc(pid, sess)
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

	a.replyRunChunk(nc, msg.Reply, proto.RunChunk{
		Kind: "exit", PID: pid, ExitCode: exitCode,
	})
	a.cfg.Logger.Info("agent: run exit", "pid", pid, "code", exitCode)
}

// waitForAttach blocks for at most attachDeadline waiting for the ctl's
// PtyAttachEvent. Returns (cols, rows, true) on success; (0, 0, false)
// on timeout. Uses SubscribeSync so we can apply a hard deadline; the
// subscription is unsubscribed before return either way.
func (a *Agent) waitForAttach(nc *nats.Conn, pid string) (int, int, bool) {
	subj := proto.SubjPtyAttach(a.cfg.SID, pid)
	sub, err := nc.SubscribeSync(subj)
	if err != nil {
		a.cfg.Logger.Warn("agent: SubscribeSync attach", "err", err, "pid", pid)
		return 0, 0, false
	}
	defer func() { _ = sub.Unsubscribe() }()

	ctx, cancel := context.WithTimeout(context.Background(), attachDeadline)
	defer cancel()
	msg, err := sub.NextMsgWithContext(ctx)
	if err != nil {
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

	flush := func() {
		if len(pending) == 0 {
			return
		}
		// Make a copy because nats.Publish may queue and reuse our buf.
		out := make([]byte, len(pending))
		copy(out, pending)
		_ = nc.Publish(outSubj, out)
		pending = pending[:0]
		// Reset the flush timer for the next batch.
		if !flushTimer.Stop() {
			select {
			case <-flushTimer.C:
			default:
			}
		}
		flushTimer.Reset(runFlushInterval)
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
	if err := sess.Signal(syscall.Signal(req.Signal)); err != nil {
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

func (a *Agent) registerProc(pid string, sess *pty.Session) {
	a.procsMu.Lock()
	defer a.procsMu.Unlock()
	a.procs[pid] = sess
}

func (a *Agent) unregisterProc(pid string) {
	a.procsMu.Lock()
	defer a.procsMu.Unlock()
	delete(a.procs, pid)
}

func (a *Agent) lookupProc(pid string) (*pty.Session, bool) {
	a.procsMu.Lock()
	defer a.procsMu.Unlock()
	s, ok := a.procs[pid]
	return s, ok
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

