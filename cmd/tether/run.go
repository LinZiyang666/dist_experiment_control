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
//        local stdin  → pty.<PID>.in
//        SIGWINCH     → pty.<PID>.resize  (with current cols/rows)
//        local Ctrl-C → cmd.by.<A>.node.<N>.kill.req {SIGINT}
//     and one passive listener:
//        pty.<PID>.out → local stdout
//  6. on RunChunk{Kind:exit} restore terminal and exit with the same code.
func newRunCmd() *cobra.Command {
	var (
		natsURL string
		home    string
		cwd     string
	)
	cmd := &cobra.Command{
		Use:   "run <node> -- <argv...>",
		Short: "Run an argv on a node interactively (allocates a PTY)",
		Long: `tether run — interactive PTY remote command (P5).

Use for shells, vim, htop, anything that needs a terminal. The local
terminal goes into raw mode so keys, arrow sequences, Ctrl-C, etc. flow
through to the remote process group untouched. Resize is propagated.

The two-phase attach handshake (architecture C.5.1) ensures the very
first byte of remote output is not dropped; on the agent side a 3s
attach deadline guarantees no orphan PTYs if ctl drops mid-handshake.
`,
		Args: cobra.MinimumNArgs(2),
		RunE: func(cmd *cobra.Command, args []string) error {
			sid := cli.ReadCurrentSession(home)
			if sid == "" {
				return fmt.Errorf("no active session — run `tether login -s <sid>` first")
			}
			nid := args[0]
			argv := args[1:]

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
			body, err := json.Marshal(proto.RunReq{
				Argv: argv, Cwd: cwd, Cols: cols, Rows: rows,
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

			// Wait for ready / failed (5s — matches agent attachDeadline + slack).
			readyCtx, cancelReady := context.WithTimeout(cmd.Context(), 5*time.Second)
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
			lifecycleCh := make(chan proto.RunChunk, 4)
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

