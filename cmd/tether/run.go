package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"os/signal"
	"strings"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/LinZiyang666/tether/internal/cli"
	"github.com/LinZiyang666/tether/internal/execclient"
	"github.com/LinZiyang666/tether/internal/proto"
	"github.com/LinZiyang666/tether/internal/terminalio"
	"github.com/LinZiyang666/tether/internal/upgradebudget"
	"github.com/nats-io/nats.go"
	"github.com/spf13/cobra"
	"golang.org/x/term"
)

// newRunCmd first probes the reliable PTY protocol, opens cancellable local
// I/O and enters raw mode before create. Legacy run remains the pre-create
// compatibility path. The reliable transport loop belongs to execclient.
// origin: hot-update S4 plan §7.2 / T49
//
// The legacy path implements the P5 PTY-mode interactive
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
//     raw Ctrl-C  → the remote line discipline as byte 0x03
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
		Long: `tether run — interactive PTY remote command.

Use for shells, vim, htop, anything that needs a terminal. The local
terminal goes into raw mode so keys, arrow sequences, Ctrl-C, etc. flow
through to the remote process group untouched. Resize is propagated.

On compatible Linux agents, reliable input and output survive hot updates
and reconnect using the original task. Older endpoints use the legacy
two-phase attach handshake (architecture C.5.1). External SIGINT/SIGTERM
cancel locally; raw Ctrl-C bytes reach the remote terminal unchanged.

Flags must come BEFORE the node argument (flag parsing stops at the first
positional). Use --safe to survive a hung network filesystem on the node:
    tether run --safe gpu-01 -- bash
A trailing '--safe' is sent to the remote command, not parsed here.
`,
		Args: cobra.MinimumNArgs(2),
		RunE: func(cmd *cobra.Command, args []string) (resultErr error) {
			admission, err := beginCtlAdmission(cmd)
			if err != nil {
				return err
			}
			defer finishCtlAdmission(admission, &resultErr)
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
			nc, err := connectCtl(cmd, "run", home, natsURL, id, nats.Name(cli.CtlNameForSession(sid)))
			if err != nil {
				return err
			}
			defer nc.Close()

			ackAlerts, _ := cmd.Flags().GetBool("ack-alerts")
			if gerr := gateDestructiveWithin(cmd, nc, id.PublicKey, ackAlerts); gerr != nil { // D8b §10.4
				return gerr
			}

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
			spec := proto.RunReq{
				Argv: argv, Cwd: cwd, Cols: cols, Rows: rows, Env: env, Safe: safe,
				// h1 D2: advertise the ctl-liveness keepalive (5s beat). An
				// old agent ignores the field and never reaps — safe skew.
				KAIntervalMS: int(runKeepaliveInterval / time.Millisecond),
			}
			legacy, reliableErr := tryReliableRun(cmd, nc, sid, id.PublicKey, nid, spec)
			if !legacy {
				return reliableErr
			}

			body, err := json.Marshal(spec)
			if err != nil {
				return err
			}

			inbox := nc.NewInbox()
			sub, err := nc.SubscribeSync(inbox)
			if err != nil {
				return err
			}
			defer func() { _ = sub.Unsubscribe() }()

			// Legacy admission also consumes the original budget; no new 20s
			// window may start after CONNECT and reliable protocol negotiation.
			// origin: hot-update S6 plan §8.2 / T30
			readyCtx, cancelReady, err := ctlAdmissionContext(cmd.Context(), admission.deadline)
			if err != nil {
				return err
			}
			defer cancelReady()
			runSubj := proto.SubjCmdBy(sid, id.PublicKey, nid, "run")
			if err := nc.PublishRequest(runSubj, inbox, body); err != nil {
				return fmt.Errorf("run: publish: %w", err)
			}

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

			// Arm the watchdog before raw mode: its optional setup warning must
			// not become a byte in the interactive terminal stream.
			// origin: hot-update S4 plan §7.3 / T39; PRODUCT-R4 stderr audit
			// Step 4: raw mode on local terminal IF stdin is a tty.
			restore, isTTY := enterRawMode()
			defer restore()

			// Step 5b: tell the agent we are subscribed and ready.
			attachBody, _ := json.Marshal(proto.PtyAttachEvent{Cols: cols, Rows: rows})
			if err := nc.Publish(proto.SubjPtyAttach(sid, pid), attachBody); err != nil {
				return fmt.Errorf("run: publish attach: %w", err)
			}

			// stdin → pty.<pid>.in, on BOTH the node-scoped subject and the legacy
			// session-scoped one. origin: prerelease audit round 2, I-F6.
			//
			// The legacy copy carries proto.PtyNodeHeader, which an upgraded agent uses
			// to drop it (it has already taken the node-scoped copy) and which an OLD
			// agent has never heard of and ignores. That is what makes publishing to both
			// safe: without the header a new ctl + new agent would write every keystroke
			// twice, and with only the node-scoped subject a new ctl could not drive an
			// agent that has not been upgraded yet.
			if isTTY {
				go pumpStdinToBus(ctxRun, nc,
					proto.SubjPtyInNode(sid, nid, pid), proto.SubjPtyIn(sid, pid), os.Stdin)
			}

			// h1 D2: keepalive beat → pty.<pid>.ka. Runs regardless of isTTY
			// (a piped-stdin run dies with its ctl just the same). If this
			// ctl's JWT predates the .ka grant (old broker minted it), the
			// server -ERRs every publish — the pump stops on the first such
			// violation (see kaPubDenied) instead of spamming an error into
			// a raw-mode terminal every 5s.
			go pumpKeepaliveToBus(ctxRun, nc, proto.SubjPtyKeepalive(sid, pid))

			// SIGWINCH → pty.<pid>.resize
			winchCh := make(chan os.Signal, 1)
			signal.Notify(winchCh, syscall.SIGWINCH)
			defer signal.Stop(winchCh)
			go pumpWinchToBus(ctxRun, nc,
				proto.SubjPtyResizeNode(sid, nid, pid), proto.SubjPtyResize(sid, pid), winchCh)

			// External SIGINT/SIGTERM cancel through main's root NotifyContext.
			// A second SIGINT handler races that cancellation and double-delivers
			// a cooked Ctrl-C; remote interrupt semantics belong to raw input.
			// origin: hot-update S4 plan §7.3 / lead L3

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

			admissionDone := readyCtx.Done()
			for {
				select {
				case <-admissionDone:
					restore()
					return &ExitError{Class: exitUnsettled, Err: fmt.Errorf("run: admission expired after %s; the sent task result is unknown", upgradebudget.RequestLatency)}
				case b := <-outCh:
					_, _ = out.Write(b)
				case c := <-lifecycleCh:
					switch c.Kind {
					case "started":
						// Only true start releases the admission deadline. Raw I/O retains
						// the original user context for the rest of the task's lifetime.
						// origin: hot-update S6 plan §8.2 / T30
						cancelReady()
						admissionDone = nil
					case "exit":
						// Drain any final bytes the kernel may have buffered.
						drainPending(outCh, out, 100*time.Millisecond)
						restore()
						admission.settled = true
						finishCtlAdmission(admission, &resultErr)
						if resultErr != nil {
							return resultErr
						}
						if c.ExitCode != 0 {
							os.Exit(c.ExitCode)
						}
						return nil
					case "failed":
						drainPending(outCh, out, 50*time.Millisecond)
						restore()
						return runFailureMessage(c.Reason)
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
	cmd.Flags().Bool("ack-alerts", false, "proceed despite an active severe cluster alert (quorum_lost / force_single_active)")
	cmd.Flags().StringVar(&cwd, "cwd", "", "working directory on the agent (default: agent's)")
	// NOTE: SetInterspersed(false) means --safe must PRECEDE the node arg:
	// `tether run --safe gpu-01 -- bash`. A trailing `--safe` is sent to the
	// remote argv, not parsed here.
	cmd.Flags().BoolVar(&safe, "safe", false,
		"hung-mount-safe spawn: re-probe mount health (discarding cached verdicts), resolve argv[0] against a PATH sanitized of unresponsive network mounts, and fail fast instead of hanging")
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
	// File.Fd may clear O_NONBLOCK on a pollable inherited descriptor.
	// Reliable run reopens its own OFD and must preserve the original flags.
	// origin: hot-update S4 plan §7.3 / T49
	cols, rows = 80, 24
	raw, err := os.Stdout.SyscallConn()
	if err != nil {
		return cols, rows
	}
	_ = raw.Control(func(fd uintptr) {
		if w, h, err := term.GetSize(int(fd)); err == nil {
			cols, rows = w, h
		}
	})
	return cols, rows
}

// enterRawMode flips the local terminal to raw mode IF stdin is a tty.
// Returns a restore function (always safe to call, even on non-tty) and
// the isTTY bool the caller uses to decide whether to enable the stdin
// pump. External signals always cancel through the root context.
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

// publishPtyBoth sends one pty payload on the node-scoped subject and, for an agent that
// predates it, on the legacy session-scoped one — the legacy copy marked with
// proto.PtyNodeHeader so an upgraded agent drops it instead of acting on it twice.
// origin: prerelease audit round 2, I-F6.
func publishPtyBoth(nc *nats.Conn, nodeSubj, legacySubj string, payload []byte) {
	_ = nc.Publish(nodeSubj, payload)
	_ = nc.PublishMsg(&nats.Msg{
		Subject: legacySubj,
		Header:  nats.Header{proto.PtyNodeHeader: []string{"1"}},
		Data:    payload,
	})
}

func pumpStdinToBus(ctx context.Context, nc *nats.Conn, nodeSubj, legacySubj string, src io.Reader) {
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
			publishPtyBoth(nc, nodeSubj, legacySubj, b)
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

// runKeepaliveInterval is the h1 D2 ctl-liveness beat: an empty publish every
// 5s on pty.<pid>.ka for the run's lifetime. The agent's grace is
// max(6×interval, 3min), so a suspended/killed ctl hangs the session up like
// sshd with ClientAliveInterval would.
const runKeepaliveInterval = 5 * time.Second

// kaPubDenied flips when the broker rejects a keepalive publish with a
// permissions violation (a v0.4.7-minted JWT has no .ka grant). Set by the
// async ErrorHandler in connectCtl; every keepalive pump stops on it — the
// alternative is nats.go's default handler printing the -ERR into a RAW-MODE
// terminal every 5s for the whole session (h1 plan critique-4).
var kaPubDenied atomic.Bool

// pumpKeepaliveToBus publishes the liveness beat until the run context ends
// or the broker denies the subject (old-broker skew).
func pumpKeepaliveToBus(ctx context.Context, nc *nats.Conn, subj string) {
	t := time.NewTicker(runKeepaliveInterval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			if kaPubDenied.Load() {
				return
			}
			_ = nc.Publish(subj, nil)
		}
	}
}

func pumpWinchToBus(ctx context.Context, nc *nats.Conn, nodeSubj, legacySubj string, ch <-chan os.Signal) {
	for {
		select {
		case <-ctx.Done():
			return
		case <-ch:
			cols, rows := terminalSize()
			body, _ := json.Marshal(proto.PtyResizeEvent{Cols: cols, Rows: rows})
			publishPtyBoth(nc, nodeSubj, legacySubj, body)
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

// These structural interfaces keep CLI ordering independently testable while
// the protocol loop and descriptor ownership remain in their respective packages.
// origin: hot-update S4 plan §7.2 / T49
type runPTYClient interface {
	Run(context.Context, proto.RunReq, execclient.Source, execclient.Sink, <-chan execclient.ControlEvent) (execclient.Result, error)
	Close() error
}

type runTerminal interface {
	EnterRaw() error
	Restore() error
	Close() error
	Source() terminalio.Source
	Sink() terminalio.Sink
}

type reliableRunDeps struct {
	probe  func(context.Context) (runPTYClient, error)
	open   func() (runTerminal, error)
	events func(context.Context) (<-chan execclient.ControlEvent, func())
	exit   func(execclient.Result)
}

func tryReliableRun(cmd *cobra.Command, nc *nats.Conn, sid, actor, nid string, spec proto.RunReq) (bool, error) {
	var closeErr error
	legacy, err := runReliableBranch(cmd.Context(), nid, spec, cmd.ErrOrStderr(), reliableRunDeps{
		probe: func(ctx context.Context) (runPTYClient, error) {
			return execclient.ProbePTYWithin(ctx, nc, proto.SubjCmdBy(sid, actor, nid, proto.ProcessResumeVerb), ctlAdmissionConfig(cmd))
		},
		open:   func() (runTerminal, error) { return terminalio.Open(os.Stdin, os.Stdout) },
		events: runResizeEvents,
		exit: func(result execclient.Result) {
			a, _ := cmd.Context().Value(ctlAdmissionKey{}).(*ctlAdmission)
			if a != nil {
				a.settled = true
			}
			finishCtlAdmission(a, &closeErr)
			if closeErr != nil {
				return
			} // surfaced below before any remote-status exit
			exitWithRemoteStatus(result)
		},
	})
	return legacy, errors.Join(err, closeErr)
}

// Only Probe and Open may request legacy fallback. Run has crossed the create
// boundary, even when its returned error happens to wrap ErrUnsupported.
// Cleanup returns through runReliablePTY before any exit callback can bypass
// Go's deferred terminal restoration.
// origin: hot-update S4 plan §2.2 / §7.2 / lead L5 / T49
func runReliableBranch(ctx context.Context, nid string, spec proto.RunReq, stderr io.Writer, deps reliableRunDeps) (bool, error) {
	result, legacy, err := runReliablePTY(ctx, spec, stderr, deps)
	if legacy {
		return true, nil
	}
	if err != nil || result.InputAbort != "" || result.StdoutAbort != "" || result.StderrAbort != "" || result.TerminalError != "" {
		return false, classifyReliableRunFailure(nid, result, err)
	}
	deps.exit(result)
	return false, nil
}

func runReliablePTY(ctx context.Context, spec proto.RunReq, stderr io.Writer, deps reliableRunDeps) (result execclient.Result, legacy bool, err error) {
	client, err := deps.probe(ctx)
	if errors.Is(err, execclient.ErrUnsupported) {
		return result, true, nil
	}
	if err != nil {
		return result, false, err
	}
	defer func() { _ = client.Close() }()

	terminal, err := deps.open()
	// origin: hot-update S4 plan lead L5 — terminalio names the offending stream and
	// its kind; the notice repeats them, and the legacy path is chosen before create.
	var unsupported *terminalio.ErrUnsupportedStdio
	if errors.As(err, &unsupported) {
		_, _ = fmt.Fprintf(stderr, "tether: %s is a %s; using legacy run (no hot-update continuity)\n", unsupported.Stream, unsupported.Kind)
		return result, true, nil
	}
	if err != nil {
		return result, false, err
	}
	defer func() {
		// Restore even if entering raw mode failed partway. Close owns only
		// reopened descriptors; failures must not turn a remote rc0 into success.
		// origin: hot-update S4 plan §7.3 / T49
		restoreErr := terminal.Restore()
		closeErr := terminal.Close()
		err = errors.Join(err, restoreErr, closeErr)
	}()
	if err := terminal.EnterRaw(); err != nil {
		return result, false, err
	}
	events, stopEvents := deps.events(ctx)
	defer stopEvents() // join the local resize adapter before restoring the terminal
	result, err = client.Run(ctx, spec, terminal.Source(), terminal.Sink(), events)
	return result, false, err
}

// Run shares exec's established phase taxonomy, but an unknown PTY refusal
// cannot imply retry-safe exit 70. Input responsibility is reported even for
// rc0, with both the applied and accepted byte offsets preserved.
// origin: hot-update S4 plan §2.2 / T49
func classifyReliableRunFailure(nid string, result execclient.Result, err error) error {
	var settled *execclient.SettledError
	if errors.As(err, &settled) {
		result = settled.Result
	}
	var permission *execclient.ProtocolError
	if errors.As(err, &permission) && brokerCodeExitClass(permission.Code) == exitNoPerm {
		return &ExitError{Class: exitNoPerm, Code: permission.Code, Err: fmt.Errorf("run: %s", permission.Message)}
	}
	if result.InputAbort != "" {
		abort := strings.TrimPrefix(result.InputAbort, "pty input aborted: ")
		message := fmt.Sprintf("run: pty input aborted: %s; applied=%d accepted=%d; remote exit=%d signal=%d (task %s on %s)",
			abort, result.InputACK, result.InputAccepted, result.ExitCode, result.Signal, result.Handle.RequestID, nid)
		return &ExitError{Class: exitUnsettled, Err: errors.New(message)}
	}
	if err == nil {
		err = &execclient.SettledError{Result: result}
	}
	// Result.CreateSent is also evidence when the peer lacks a typed phase.
	// Copy a ProtocolError rather than mutating an error retained by the client.
	var pe *execclient.ProtocolError
	// origin: hot-update S4 plan §2.2 / T38
	// The PTY client marks only a definitive first-create admission refusal
	// as pre-admission. Recovery-pending uses process_recovering; genuine busy
	// is final even after a lost reply.
	if errors.As(err, &pe) && pe.Code == proto.CodeProcessBusy && !pe.AfterCreate {
		return &ExitError{Class: exitTransient, Code: pe.Code, Err: fmt.Errorf("run: %s: %s", pe.Code, pe.Message)}
	}
	if errors.As(err, &pe) && result.CreateSent && !pe.AfterCreate {
		copyError := *pe
		copyError.AfterCreate = true
		err = &copyError
	}
	classified := classifyReliableFailure(nid, upgradebudget.PTYRecovery, result, err)
	var exitErr *ExitError
	if errors.As(classified, &exitErr) {
		class := exitErr.Class
		if class == exitInternal {
			class = exitUnsettled
		}
		message := strings.Replace(classified.Error(), "exec:", "run:", 1)
		// Run has no --timeout flag: its recovery window is fixed by the
		// protocol, so exec's timeout guidance would name an unusable option.
		// origin: hot-update S4 plan §7.2 / T49
		message = strings.Replace(message, "gave up waiting after --timeout ", "recovery window expired after ", 1)
		return &ExitError{Class: class, Code: exitErr.Code, Err: errors.New(message)}
	}
	return &ExitError{Class: exitUnsettled, Err: fmt.Errorf("run: %w", classified)}
}

// SIGWINCH is the only signal owned here. External SIGINT/SIGTERM belong to
// main's root NotifyContext; raw 0x03 remains ordinary input to the protocol.
// The single-slot event channel bounds a blocked transport's resize backlog.
// origin: hot-update S4 plan §7.3 / lead L3
func runResizeEvents(ctx context.Context) (<-chan execclient.ControlEvent, func()) {
	ctx, cancel := context.WithCancel(ctx)
	winch := make(chan os.Signal, 1)
	signal.Notify(winch, syscall.SIGWINCH)
	events := make(chan execclient.ControlEvent, 1)
	done := make(chan struct{})
	go func() {
		defer close(done)
		defer close(events)
		for {
			select {
			case <-ctx.Done():
				return
			case <-winch:
				cols, rows := terminalSize()
				event := execclient.ControlEvent{Op: "resize", Cols: uint16(cols), Rows: uint16(rows)}
				select {
				case events <- event:
				case <-ctx.Done():
					return
				}
			}
		}
	}()
	return events, func() {
		signal.Stop(winch)
		cancel()
		<-done
	}
}
