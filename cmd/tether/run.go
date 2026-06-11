package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/LinZiyang666/tether/internal/cli"
	"github.com/LinZiyang666/tether/internal/proto"
	"github.com/nats-io/nats.go"
	"github.com/spf13/cobra"
	"golang.org/x/term"
)

// newRunCmd implements `tether run` — the P5 PTY-mode interactive
// remote command. Architecture C.5 / C.5.1 walks through the wire flow;
// the ctl-side responsibilities here are:
//
//  1. allocate a streaming inbox, publish the run.req with current
//     terminal cols/rows;
//  2. wait for RunChunk{Kind:ready,PID,Cols,Rows} on the inbox;
//  3. subscribe pty.<PID>.out BEFORE publishing pty.<PID>.attach (this is
//     the whole point of the two-phase handshake — agent only fork+execs
//     once it sees the attach, so first byte of stdout can't be lost);
//  4. flip the local terminal to raw mode (so individual keys, Ctrl
//     sequences, etc. don't get cooked by the kernel before reaching the
//     remote PTY) — restored on exit;
//  5. run three pumps for the lifetime of the child:
//     local stdin  → pty.<PID>.in
//     SIGWINCH     → pty.<PID>.resize  (with current cols/rows)
//     local Ctrl-C → cmd.by.<A>.node.<N>.kill.req {SIGINT}
//     and one passive listener:
//     pty.<PID>.out → local stdout
//  6. on RunChunk{Kind:exit} restore terminal and exit with the same code.
func newRunCmd() *cobra.Command {
	var (
		natsURL string
		home    string
		cwd     string
		safe    bool
	)
	cmd := &cobra.Command{
		Use:   "run <node> -- <argv...>",
		Short: "Run an argv on a node interactively (allocates a PTY)",
		Long: `tether run — interactive PTY remote command (P5).

Use for shells, vim, htop, anything that needs a terminal. The local
terminal goes into raw mode so keys, arrow sequences, Ctrl-C, etc. flow
through to the remote process group untouched. Resize is propagated.

The two-phase attach handshake (architecture C.5.1) ensures the very
first byte of remote output is not dropped; on the agent side an attach
deadline (default 15s) guarantees no orphan PTYs if ctl drops
mid-handshake.

Flags must come BEFORE the node argument (flag parsing stops at the first
positional). Use --safe to survive a hung network filesystem on the node:
    tether run --safe gpu-01 -- bash
A trailing '--safe' is sent to the remote command, not parsed here.
`,
		Args: cobra.MinimumNArgs(2),
		RunE: func(cmd *cobra.Command, args []string) error {
			sid := cli.ReadCurrentSession(home)
			if sid == "" {
				return fmt.Errorf("no active session — run `tether login -s <sid>` first")
			}
			natsURL = cli.ResolveNATSURLFromHome(natsURL, cmd.Flags().Changed("nats-url"), home)
			nid := args[0]
			argv := args[1:]
			// Strip a single leading `--`. See the matching comment
			// in cmd/tether/exec.go for the rationale.
			if len(argv) > 0 && argv[0] == "--" {
				argv = argv[1:]
			}

			id, err := cli.EnsureIdentity(home)
			if err != nil {
				return err
			}
			nc, err := cli.ConnectNATSWithNkey(natsURL, id, nats.Name(cli.CtlNameForSession(sid)))
			if err != nil {
				return connectError("run", natsURL, err)
			}
			defer nc.Close()

			cols, rows := terminalSize()
			// Forward TERM (and a couple of related vars) so curses
			// apps on the agent (tmux, vim, htop, less) know what
			// terminal capabilities to use. Without this they fall
			// back to TERM=dumb / unset and refuse to start with
			// "open terminal failed: terminal does not support clear".
			env := map[string]string{}
			for _, k := range []string{"TERM", "COLORTERM", "LANG", "LC_ALL"} {
				if v := os.Getenv(k); v != "" {
					env[k] = v
				}
			}
			body, err := json.Marshal(proto.RunReq{
				Argv: argv, Cwd: cwd, Cols: cols, Rows: rows, Env: env, Safe: safe,
			})
			if err != nil {
				return err
			}

			inbox := nc.NewInbox()
			sub, err := nc.SubscribeSync(inbox)
			if err != nil {
				return err
			}
			defer func() { _ = sub.Unsubscribe() }()

			runSubj := proto.SubjCmdBy(sid, id.PublicKey, nid, "run")
			if err := nc.PublishRequest(runSubj, inbox, body); err != nil {
				return fmt.Errorf("run: publish: %w", err)
			}

			// Wait for ready / failed.
			// ctl waits for the agent's RunChunk{Kind:ready} reply
			// before publishing pty.<pid>.attach. Has to outlast both
			// the agent-side attachDeadline (default 15s, override
			// TETHER_AGENT_ATTACH_DEADLINE) AND the round-trip cost
			// of the run.req → ready reply over WSS. 20s gives slack
			// without wedging on a truly dead broker. Override:
			// TETHER_RUN_READY_TIMEOUT.
			readyTimeout := 20 * time.Second
			if v := os.Getenv("TETHER_RUN_READY_TIMEOUT"); v != "" {
				if d, err := time.ParseDuration(v); err == nil && d > 0 {
					readyTimeout = d
				}
			}
			readyCtx, cancelReady := context.WithTimeout(cmd.Context(), readyTimeout)
			defer cancelReady()
			msg, err := sub.NextMsgWithContext(readyCtx)
			if err != nil {
				return fmt.Errorf("run: waiting for ready: %w", err)
			}
			var first proto.RunChunk
			if err := json.Unmarshal(msg.Data, &first); err != nil {
				return fmt.Errorf("run: malformed first chunk: %w", err)
			}
			switch first.Kind {
			case "ready":
				// happy path — fall through.
			case "failed":
				return runFailureMessage(first.Reason)
			default:
				return fmt.Errorf("run: unexpected first chunk kind=%q (broker version skew?)", first.Kind)
			}

			pid := first.PID

			// Step 3: subscribe to .out BEFORE telling the agent to exec.
			outCh := make(chan []byte, 64)
			outSub, err := nc.Subscribe(proto.SubjPtyOut(sid, pid), func(m *nats.Msg) {
				// copy: NATS reuses buffers between callbacks
				b := make([]byte, len(m.Data))
				copy(b, m.Data)
				select {
				case outCh <- b:
				default:
					// Drop on local backpressure rather than wedge the
					// NATS dispatcher. With 64-deep + tiny terminals this
					// only fires under pathological output rates.
				}
			})
			if err != nil {
				return fmt.Errorf("run: subscribe pty.out: %w", err)
			}
			defer func() { _ = outSub.Unsubscribe() }()

			// Step 4: raw mode on local terminal IF stdin is a tty.
			restore, isTTY := enterRawMode()
			defer restore()

			// Step 5b: tell the agent we are subscribed and ready.
			attachBody, _ := json.Marshal(proto.PtyAttachEvent{Cols: cols, Rows: rows})
			if err := nc.Publish(proto.SubjPtyAttach(sid, pid), attachBody); err != nil {
				return fmt.Errorf("run: publish attach: %w", err)
			}

			// Background pumps. ctxRun goes down on exit / failure.
			ctxRun, cancelRun := context.WithCancel(cmd.Context())
			defer cancelRun()

			// lifecycleCh: agent's RunChunk events (started/exit/failed)
			// land here from the inbox-drainer below; the watchdog also
			// posts a synthetic `failed` when the heartbeat goes dark.
			// Declared up here so the watchdog can be started before the
			// inbox-drainer (no causal dependency; just ordering).
			lifecycleCh := make(chan proto.RunChunk, 4)

			// Agent-liveness watchdog: without this, an agent that
			// goes silent mid-run (machine offline / network partition)
			// leaves the main select loop below waiting forever for an
			// `exit` chunk that never comes, with the local terminal
			// stuck in raw mode. Subscribe to the agent's existing
			// per-node heartbeat (agent publishes every ~5s; see
			// internal/agent/agent.go:heartbeatLoop) and arm a timer
			// AFTER the first HB arrives. The arm-after-first delay
			// degrades gracefully on a broker whose JWT perms don't yet
			// grant heartbeat-sub to ctl (older release) — we just stay
			// unarmed and behave as before, no false positives.
			startRunWatchdog(ctxRun, nc, sid, nid, lifecycleCh)

			// stdin → pty.<pid>.in
			if isTTY {
				go pumpStdinToBus(ctxRun, nc, proto.SubjPtyIn(sid, pid), os.Stdin)
			}

			// SIGWINCH → pty.<pid>.resize
			winchCh := make(chan os.Signal, 1)
			signal.Notify(winchCh, syscall.SIGWINCH)
			defer signal.Stop(winchCh)
			go pumpWinchToBus(ctxRun, nc, proto.SubjPtyResize(sid, pid), winchCh)

			// Local Ctrl-C → kill.req {SIGINT}. We can't observe Ctrl-C as
			// a SIGINT signal here because raw mode delivers ^C as a byte
			// to stdin (so it's already in the .in pump). The dual capture
			// would double-deliver. Instead, capture SIGINT only when stdin
			// is NOT a tty (where the kernel still cooks ^C).
			if !isTTY {
				sigCh := make(chan os.Signal, 1)
				signal.Notify(sigCh, syscall.SIGINT)
				defer signal.Stop(sigCh)
				go func() {
					select {
					case <-ctxRun.Done():
						return
					case <-sigCh:
						_ = sendKill(nc, sid, id.PublicKey, nid, pid, int(syscall.SIGINT))
					}
				}()
			}

			// Foreground: drain .out → stdout AND wait for exit on the inbox.
			out := cmd.OutOrStdout()
			go func() {
				for {
					m, err := sub.NextMsgWithContext(ctxRun)
					if err != nil {
						return
					}
					var c proto.RunChunk
					if err := json.Unmarshal(m.Data, &c); err == nil {
						lifecycleCh <- c
					}
				}
			}()

			for {
				select {
				case b := <-outCh:
					_, _ = out.Write(b)
				case c := <-lifecycleCh:
					switch c.Kind {
					case "started":
						// optional log; nothing to do.
					case "exit":
						// Drain any final bytes the kernel may have buffered.
						drainPending(outCh, out, 100*time.Millisecond)
						restore()
						if c.ExitCode != 0 {
							os.Exit(c.ExitCode)
						}
						return nil
					case "failed":
						drainPending(outCh, out, 50*time.Millisecond)
						restore()
						return fmt.Errorf("run: %s", c.Reason)
					}
				case <-cmd.Context().Done():
					restore()
					return cmd.Context().Err()
				}
			}
		},
	}
	cmd.Flags().StringVar(&natsURL, "nats-url", "nats://127.0.0.1:4222", "NATS server URL")
	cmd.Flags().StringVar(&home, "home", cli.DefaultHome(), "tether home dir")
	cmd.Flags().StringVar(&cwd, "cwd", "", "working directory on the agent (default: agent's)")
	// NOTE: SetInterspersed(false) means --safe must PRECEDE the node arg:
	// `tether run --safe gpu-01 -- bash`. A trailing `--safe` is sent to the
	// remote argv, not parsed here.
	cmd.Flags().BoolVar(&safe, "safe", false,
		"hung-mount-safe spawn: resolve argv[0] against a PATH sanitized of unresponsive network mounts; fail fast instead of hanging")
	// Audit shard 04 F12: same as exec — stop cobra parsing the
	// remote command's flags as ours.
	cmd.Flags().SetInterspersed(false)
	cmd.ValidArgsFunction = func(c *cobra.Command, args []string, toComplete string) ([]string, cobra.ShellCompDirective) {
		if len(args) > 0 {
			return nil, cobra.ShellCompDirectiveNoFileComp
		}
		cctx := cli.NewCompletionContext(home, natsURL, c.Flags().Changed("nats-url"))
		t := cli.NewCompletionTransport(home, natsURL, c.Flags().Changed("nats-url"))
		defer t.Close()
		return cli.CompleteOnlineNodes(t, cctx, toComplete)
	}
	return cmd
}

func terminalSize() (cols, rows int) {
	if w, h, err := term.GetSize(int(os.Stdout.Fd())); err == nil {
		return w, h
	}
	return 80, 24
}

// enterRawMode flips the local terminal to raw mode IF stdin is a tty.
// Returns a restore function (always safe to call, even on non-tty) and
// the isTTY bool the caller uses to decide whether to enable the stdin
// pump and the SIGINT path.
func enterRawMode() (restore func(), isTTY bool) {
	fd := int(os.Stdin.Fd())
	if !term.IsTerminal(fd) {
		return func() {}, false
	}
	old, err := term.MakeRaw(fd)
	if err != nil {
		return func() {}, false
	}
	return func() { _ = term.Restore(fd, old) }, true
}

func pumpStdinToBus(ctx context.Context, nc *nats.Conn, subj string, src io.Reader) {
	buf := make([]byte, 4*1024)
	for {
		// Bounded read with cancel: best-effort, since os.Stdin doesn't
		// honor ctx natively. On ctx.Done the goroutine will exit on the
		// next read or on the parent process exit.
		select {
		case <-ctx.Done():
			return
		default:
		}
		n, err := src.Read(buf)
		if n > 0 {
			b := make([]byte, n)
			copy(b, buf[:n])
			_ = nc.Publish(subj, b)
			// Force the keystroke to the wire instead of letting
			// nats.go's internal flusher batch it. On a high-RTT
			// WSS link the operator otherwise types into a black
			// hole until the buffer fills naturally.
			_ = nc.FlushTimeout(50 * time.Millisecond)
		}
		if err != nil {
			return
		}
	}
}

func pumpWinchToBus(ctx context.Context, nc *nats.Conn, subj string, ch <-chan os.Signal) {
	for {
		select {
		case <-ctx.Done():
			return
		case <-ch:
			cols, rows := terminalSize()
			body, _ := json.Marshal(proto.PtyResizeEvent{Cols: cols, Rows: rows})
			_ = nc.Publish(subj, body)
		}
	}
}

// runWatchdogTimeout returns the no-heartbeat threshold for the run
// watchdog. Default 15s = 3 × agent's HeartbeatInterval (5s) so a
// single missed beat doesn't trip; override via env for flaky links
// (TETHER_RUN_LIVENESS_TIMEOUT=30s) or kill-switch (=0).
func runWatchdogTimeout() time.Duration {
	const defaultTimeout = 15 * time.Second
	v := os.Getenv("TETHER_RUN_LIVENESS_TIMEOUT")
	if v == "" {
		return defaultTimeout
	}
	d, err := time.ParseDuration(v)
	if err != nil || d < 0 {
		return defaultTimeout
	}
	return d
}

// startRunWatchdog wires up the heartbeat-based liveness check for an
// in-progress `tether run`. Behavior:
//
//   - Subscribe to ctrl.s.<sid>.node.<nid>.heartbeat. If the subscribe
//     itself errors (no JWT perm on a stale broker, transport down),
//     log a one-line warning and return — never wedge the run because
//     the watchdog couldn't arm.
//   - The watchdog ARMS only after the first heartbeat is observed.
//     This is the graceful-degradation hinge: if no HB ever arrives
//     (old broker, weird auth, dev mode without auth_callout) we
//     simply behave like before this feature existed.
//   - Once armed, missing a heartbeat for runWatchdogTimeout pushes a
//     synthetic `failed` RunChunk into lifecycleCh, which the main
//     select loop already knows how to handle (drain → restore →
//     return). No new exit path needed.
//   - Timeout of 0 disables the watchdog entirely (TETHER_RUN_LIVENESS_TIMEOUT=0).
func startRunWatchdog(ctx context.Context, nc *nats.Conn, sid, nid string, lifecycleCh chan<- proto.RunChunk) {
	timeout := runWatchdogTimeout()
	if timeout == 0 {
		return
	}
	hbCh := make(chan struct{}, 8)
	sub, err := nc.Subscribe(proto.SubjNodeHeartbeat(sid, nid), func(m *nats.Msg) {
		select {
		case hbCh <- struct{}{}:
		default:
		}
	})
	if err != nil {
		fmt.Fprintf(os.Stderr, "warning: run liveness watchdog disabled: %v\n", err)
		return
	}
	go func() {
		defer func() { _ = sub.Unsubscribe() }()
		// Wait for first HB before arming. If ctx cancels (clean
		// exit / signal) before any HB arrives, just leave silently.
		select {
		case <-hbCh:
		case <-ctx.Done():
			return
		}
		timer := time.NewTimer(timeout)
		defer timer.Stop()
		for {
			select {
			case <-hbCh:
				if !timer.Stop() {
					select {
					case <-timer.C:
					default:
					}
				}
				timer.Reset(timeout)
			case <-timer.C:
				// Non-blocking send: lifecycleCh has buffer 4 and the
				// main loop is the only reader. A buffer-full case
				// here would mean the main loop has already received
				// another terminal event (exit / failed) and is about
				// to return — drop the synthetic failed in that case.
				select {
				case lifecycleCh <- proto.RunChunk{
					Kind:   "failed",
					Reason: fmt.Sprintf("agent unreachable: no heartbeat for %s", timeout),
				}:
				default:
				}
				return
			case <-ctx.Done():
				return
			}
		}
	}()
}

func sendKill(nc *nats.Conn, sid, actor, nid, pid string, sig int) error {
	body, _ := json.Marshal(proto.KillReq{PID: pid, Signal: sig})
	subj := proto.SubjCmdBy(sid, actor, nid, "kill")
	_, err := nc.Request(subj, body, 2*time.Second)
	return err
}

// drainPending greedily drains buffered .out bytes for up to maxWait so
// the very last lines (e.g. "$ exit\r\n") aren't visually clipped when
// the agent's exit chunk races past them.
func drainPending(outCh <-chan []byte, w io.Writer, maxWait time.Duration) {
	deadline := time.NewTimer(maxWait)
	defer deadline.Stop()
	for {
		select {
		case b := <-outCh:
			_, _ = w.Write(b)
		case <-deadline.C:
			return
		default:
			// nothing pending right now; give NATS a moment in case more
			// is in flight, then return.
			select {
			case b := <-outCh:
				_, _ = w.Write(b)
			case <-time.After(20 * time.Millisecond):
				return
			case <-deadline.C:
				return
			}
		}
	}
}
