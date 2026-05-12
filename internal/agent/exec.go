package agent

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"strings"
	"sync"
	"time"

	"github.com/LinZiyang666/tether/internal/proc"
	"github.com/LinZiyang666/tether/internal/proto"
	"github.com/nats-io/nats.go"
)

// streamChunkSize bounds how much output is shipped in a single ExecChunk
// publish. NATS default max message size is 1MiB; 4KiB keeps the stream
// fine-grained enough that progress bars / partial lines surface
// promptly without spamming.
const streamChunkSize = 4 * 1024

// dispatchForwarded routes a `cmd.node.<N>.<verb>.req.forwarded` message
// to the right verb handler. Subject layout:
//
//   tether.v1.s.<sid>.cmd.node.<nid>.<verb>.req.forwarded
//
// (10 tokens; same as the cmd.by.* tree minus the actor segment).
func (a *Agent) dispatchForwarded(nc *nats.Conn, msg *nats.Msg) {
	// Audit shard 01 F5: a forwarded msg that arrives mid-shutdown
	// would otherwise spawn a goroutine that publishes onto a
	// draining nats.Conn. Drop early if runCtx is gone — the
	// caller already told subFwd.Unsubscribe to stop new
	// dispatches; this catches the in-flight race.
	if a.runCtx != nil {
		if err := a.runCtx.Err(); err != nil {
			return
		}
	}
	parts := strings.Split(msg.Subject, ".")
	if len(parts) != 10 || parts[8] != "req" || parts[9] != "forwarded" {
		a.cfg.Logger.Warn("agent: forwarded subject malformed", "subject", msg.Subject)
		return
	}
	verb := parts[7]
	// Each verb handler is dispatched in its own goroutine so a
	// long-running run (which blocks in pty.Wait) doesn't head-of-line
	// block subsequent kill.req.forwarded / exec.req.forwarded
	// messages on the same single NATS subscription. Without this,
	// `tether run sleep 60` + Ctrl-C would never deliver the kill
	// signal because the dispatch goroutine is still inside Wait.
	switch verb {
	case "exec":
		go a.handleExecForwarded(nc, msg)
	case "run":
		go a.handleRunForwarded(nc, msg)
	case "kill":
		go a.handleKillForwarded(nc, msg)
	case "expose":
		go a.handleExposeForwarded(nc, msg)
	case "expose-rm":
		go a.handleExposeRmForwarded(nc, msg)
	case "upgrade":
		go a.handleUpgradeForwarded(nc, msg)
	default:
		a.cfg.Logger.Warn("agent: unknown forwarded verb", "verb", verb)
	}
}

// handleExecForwarded runs an ExecReq and streams ExecChunks back on the
// request's reply inbox. See proto.ExecChunk for the wire shape.
func (a *Agent) handleExecForwarded(nc *nats.Conn, msg *nats.Msg) {
	if msg.Reply == "" {
		a.cfg.Logger.Warn("agent: exec.req.forwarded without Reply inbox")
		return
	}
	var req proto.ExecReq
	if err := json.Unmarshal(msg.Data, &req); err != nil {
		a.replyChunk(nc, msg.Reply, proto.ExecChunk{
			Kind: "error", Error: "json_parse: " + err.Error(),
		})
		return
	}
	if len(req.Argv) == 0 {
		a.replyChunk(nc, msg.Reply, proto.ExecChunk{
			Kind: "error", Error: "argv required",
		})
		return
	}

	pid := proc.NewPID()
	a.cfg.Logger.Info("agent: exec", "pid", pid, "argv", req.Argv)
	a.replyChunk(nc, msg.Reply, proto.ExecChunk{Kind: "started", PID: pid})
	a.pubProcStarted(nc, pid, req.Argv, req.ActorFP)

	exitCode, err := a.runChild(nc, msg.Reply, &req)
	a.pubProcExit(nc, pid, exitCode)

	if err != nil {
		a.replyChunk(nc, msg.Reply, proto.ExecChunk{
			Kind: "error", PID: pid, Error: err.Error(),
		})
		return
	}
	a.replyChunk(nc, msg.Reply, proto.ExecChunk{
		Kind: "exit", PID: pid, ExitCode: exitCode,
	})
}

// runChild spawns the child process, streams stdout/stderr to replyTo,
// and returns the exit code. A non-nil error is returned ONLY for setup
// failures (pipe / Start), not for child exit codes — those are the
// returned exitCode value and the caller pubs an "exit" chunk.
func (a *Agent) runChild(nc *nats.Conn, replyTo string, req *proto.ExecReq) (int, error) {
	cmd := exec.Command(req.Argv[0], req.Argv[1:]...)
	if req.Cwd != "" {
		cmd.Dir = req.Cwd
	}
	if len(req.Env) > 0 {
		cmd.Env = envSliceFromMap(req.Env)
	}
	if len(req.Stdin) > 0 {
		cmd.Stdin = bytes.NewReader(req.Stdin)
	}

	stdoutPipe, err := cmd.StdoutPipe()
	if err != nil {
		return -1, err
	}
	stderrPipe, err := cmd.StderrPipe()
	if err != nil {
		return -1, err
	}

	if err := cmd.Start(); err != nil {
		return -1, err
	}

	var wg sync.WaitGroup
	wg.Add(2)
	go a.streamPipe(&wg, nc, stdoutPipe, replyTo, "stdout")
	go a.streamPipe(&wg, nc, stderrPipe, replyTo, "stderr")
	wg.Wait()

	if err := cmd.Wait(); err != nil {
		var exitErr *exec.ExitError
		if errors.As(err, &exitErr) {
			// ExitCode() returns -1 when the child was terminated by a
			// signal rather than a clean exit. Stream a stderr note so
			// the operator can tell e.g. "my own `pkill -f <pattern>`
			// caught the exec child's shell" apart from "agent crashed",
			// but keep the legacy wire shape (exit chunk with ExitCode=-1)
			// so existing ctl tooling that special-cases negative exit
			// codes keeps working.
			if exitErr.ExitCode() < 0 {
				note := []byte(fmt.Sprintf(
					"\n[tether agent] child terminated by signal (%s) — usually external pkill / SIGTERM matched the shell's argv\n",
					exitErr.String()))
				a.replyChunk(nc, replyTo, proto.ExecChunk{Kind: "stderr", Data: note})
			}
			return exitErr.ExitCode(), nil
		}
		return -1, err
	}
	return 0, nil
}

func (a *Agent) streamPipe(wg *sync.WaitGroup, nc *nats.Conn, r io.ReadCloser, replyTo, kind string) {
	defer wg.Done()
	buf := make([]byte, streamChunkSize)
	for {
		n, err := r.Read(buf)
		if n > 0 {
			data := make([]byte, n)
			copy(data, buf[:n])
			a.replyChunk(nc, replyTo, proto.ExecChunk{Kind: kind, Data: data})
		}
		if err != nil {
			return // io.EOF or pipe closed = child finished
		}
	}
}

func (a *Agent) replyChunk(nc *nats.Conn, replyTo string, c proto.ExecChunk) {
	if replyTo == "" {
		return
	}
	payload, err := json.Marshal(c)
	if err != nil {
		// Programming error — our own types should always marshal.
		a.cfg.Logger.Warn("agent: marshal ExecChunk", "err", err, "kind", c.Kind)
		return
	}
	if err := nc.Publish(replyTo, payload); err != nil {
		a.cfg.Logger.Warn("agent: reply chunk pub", "err", err, "kind", c.Kind)
	}
}

func (a *Agent) pubProcStarted(nc *nats.Conn, pid string, argv []string, actorFP string) {
	a.pubProcStartedWithTriple(nc, pid, argv, actorFP, "", 0, time.Now().UTC())
}

// pubProcStartedWithTriple is the full-info variant: PTY children
// can include the (boot_id, start_time_ticks) pair captured at fork
// time so the broker persists them in processes.boot_id /
// .start_time_ticks for the next G.1 reconcile to verify against.
// Exec children call pubProcStarted (which leaves both empty) — they
// have a sync lifecycle and no agent-side persistence path that would
// need the verification.
func (a *Agent) pubProcStartedWithTriple(
	nc *nats.Conn,
	pid string,
	argv []string,
	actorFP string,
	bootID string,
	startTimeTicks int64,
	startedAt time.Time,
) {
	payload, err := json.Marshal(proto.ProcStartedEvent{
		PID:            pid,
		Argv:           argv,
		StartedAt:      startedAt,
		StartedByFP:    actorFP,
		BootID:         bootID,
		StartTimeTicks: startTimeTicks,
	})
	if err != nil {
		return
	}
	subj := proto.SubjEvProc(a.cfg.SID, a.cfg.NID, pid, "started")
	if err := nc.Publish(subj, payload); err != nil {
		a.cfg.Logger.Warn("agent: pub proc.started", "err", err)
	}
}

func (a *Agent) pubProcExit(nc *nats.Conn, pid string, exitCode int) {
	payload, err := json.Marshal(proto.ProcExitEvent{
		PID:      pid,
		ExitCode: exitCode,
		EndedAt:  time.Now().UTC(),
	})
	if err != nil {
		return
	}
	subj := proto.SubjEvProc(a.cfg.SID, a.cfg.NID, pid, "exit")
	if err := nc.Publish(subj, payload); err != nil {
		a.cfg.Logger.Warn("agent: pub proc.exit", "err", err)
	}
}

func envSliceFromMap(m map[string]string) []string {
	if len(m) == 0 {
		return nil
	}
	out := make([]string, 0, len(m))
	for k, v := range m {
		out = append(out, k+"="+v)
	}
	return out
}

// mergeChildEnv builds a child env array starting from os.Environ()
// (so PATH / HOME / locale come for free) and overlays the override
// map on top — last-wins semantics, matching os/exec docs. Always
// guarantees TERM is set so curses programs (tmux, vim, htop, less)
// don't fail with "terminal does not support clear" under systemd
// --user where TERM is unset by default.
//
// Used by `tether run` for PTY children. `tether exec` non-PTY
// children may want a stricter env (no inherited TERM since stdout
// isn't a tty); they keep using envSliceFromMap directly.
func mergeChildEnv(override map[string]string) []string {
	base := os.Environ()
	out := make([]string, 0, len(base)+len(override)+1)
	out = append(out, base...)
	hasTerm := false
	for _, e := range base {
		if strings.HasPrefix(e, "TERM=") {
			hasTerm = true
			break
		}
	}
	for k, v := range override {
		out = append(out, k+"="+v)
		if k == "TERM" {
			hasTerm = true
		}
	}
	if !hasTerm {
		out = append(out, "TERM=xterm-256color")
	}
	return out
}
