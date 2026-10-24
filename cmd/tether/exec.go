package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"strings"
	"time"

	"github.com/LinZiyang666/tether/internal/cli"
	"github.com/LinZiyang666/tether/internal/controlbudget"
	"github.com/LinZiyang666/tether/internal/execclient"
	"github.com/LinZiyang666/tether/internal/proto"
	"github.com/nats-io/nats.go"
	"github.com/spf13/cobra"
)

func newExecCmd() *cobra.Command {
	var (
		natsURL   string
		home      string
		cwd       string
		timeout   time.Duration
		safe      bool
		readStdin bool
	)
	cmd := &cobra.Command{
		Use:   "exec <node> <argv...>",
		Short: "Run an argv on a node, stream stdout/stderr, propagate exit code",
		Long: `tether exec — non-interactive remote command.

The command runs on <node> in the active session (TETHER_SESSION env or
current_session file). stdout/stderr stream back as they arrive; the
exit code of the remote process becomes the local exit code.

For interactive commands (vim, htop, progress bars with cursor moves)
use 'tether run' — it allocates a PTY on the agent and pumps raw bytes
both ways.

Flags must come BEFORE the node argument (flag parsing stops at the first
positional). Use --safe to survive a hung network filesystem on the node:
    tether exec --safe gpu-01 -- whoami
A trailing '--safe' (e.g. 'tether exec gpu-01 --safe whoami') is sent to the
remote command, not parsed here, and the safe-spawn lifeline silently no-ops.
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
			nid, argv := args[0], args[1:]
			// SetInterspersed(false) eats `--` as a literal positional
			// (cobra doesn't strip it). `tether exec gpu-01 -- echo`
			// then reaches the agent as argv ["--", "echo"] and exec
			// fails with `executable file not found: --`. Operators
			// reach for `--` because every other CLI uses it to
			// separate flags from sub-argv; honor that habit by
			// stripping a single leading `--`.
			if len(argv) > 0 && argv[0] == "--" {
				argv = argv[1:]
			}

			// --stdin is read BEFORE connecting: a body over the request limit
			// is a usage error the user can fix locally, not a round trip.
			var stdin []byte
			if readStdin {
				if stdin, err = readExecStdin(cmd.InOrStdin()); err != nil {
					return err
				}
			}
			spec := proto.ExecReq{Argv: argv, Cwd: cwd, Safe: safe, Stdin: stdin}

			id, err := cli.EnsureIdentity(home)
			if err != nil {
				return err
			}
			nc, err := connectCtl(cmd, "exec", home, natsURL, id, nats.Name(cli.CtlNameForSession(sid)))
			if err != nil {
				return err
			}
			defer nc.Close()

			ctx, cancel := context.WithTimeout(cmd.Context(), timeout)
			defer cancel()
			result, reliableErr := execclient.RunWithin(ctx, nc,
				proto.SubjCmdBy(sid, id.PublicKey, nid, proto.ProcessResumeVerb),
				spec, cmd.OutOrStdout(), cmd.ErrOrStderr(), ctlAdmissionConfig(cmd))
			if !errors.Is(reliableErr, execclient.ErrUnsupported) || result.CreateSent {
				if reliableErr == nil {
					// Run returns only after both outputs and the terminal ACK
					// have settled. Drain before propagating a nonzero code.
					_ = nc.Drain()
					admission.settled = true
					finishCtlAdmission(admission, &resultErr)
					if resultErr != nil {
						return resultErr
					}
					exitWithRemoteStatus(result)
					return nil
				}
				var settled *execclient.SettledError
				if errors.As(reliableErr, &settled) {
					_ = nc.Drain() // the task settled on the agent; drain like the clean path
				}
				return classifyReliableFailure(nid, timeout, result, reliableErr)
			}

			return runLegacyExec(ctx, cmd, nc, sid, nid, id.PublicKey, spec, timeout, admission)
		},
	}
	cmd.Flags().StringVar(&natsURL, "nats-url", "nats://127.0.0.1:4222", "NATS server URL")
	cmd.Flags().StringVar(&home, "home", cli.DefaultHome(), "tether home dir")
	cmd.Flags().StringVar(&cwd, "cwd", "", "working directory on the agent (default: agent's)")
	cmd.Flags().DurationVar(&timeout, "timeout", 10*time.Minute, "max time to wait for output / exit")
	// NOTE: SetInterspersed(false) below means flags must PRECEDE the node arg:
	// `tether exec --safe gpu-01 -- whoami`. A trailing `--safe` is sent to the
	// remote argv, not parsed here.
	cmd.Flags().BoolVar(&safe, "safe", false,
		"hung-mount-safe spawn: re-probe mount health (discarding cached verdicts), resolve argv[0] against a PATH sanitized of unresponsive network mounts, and fail fast instead of hanging")
	cmd.Flags().BoolVar(&readStdin, "stdin", false,
		"read local stdin to EOF (max 512 KiB) and hand it to the remote command as its one-shot stdin; without it the remote stdin is /dev/null")
	// Audit shard 04 F12: without this, `tether exec node1 ls -la`
	// fails with "unknown shorthand flag 'l'" because cobra parses
	// the remote command's flags as ours. SetInterspersed(false)
	// stops flag parsing at the first positional, treating the rest
	// as the remote argv even when it contains -flag-shaped tokens.
	cmd.Flags().SetInterspersed(false)
	cmd.ValidArgsFunction = func(c *cobra.Command, args []string, toComplete string) ([]string, cobra.ShellCompDirective) {
		// First positional is the node id; subsequent positionals are
		// the remote argv (no useful local completion). Cobra calls us
		// after flag parsing, so home/natsURL closure vars are populated.
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

// Legacy streaming starts only after reliable negotiation proves unsupported.
// It sends once and preserves the unknown-result boundary until true start.
// origin: hot-update S6 plan §8.2 / T31 / T36
func runLegacyExec(ctx context.Context, cmd *cobra.Command, nc *nats.Conn, sid, nid, actor string, spec proto.ExecReq, timeout time.Duration, admission *ctlAdmission) (resultErr error) {
	body, err := json.Marshal(spec)
	if err != nil {
		return err
	}

	// Streaming reply: subscribe to a fresh inbox, then publish
	// the request with that inbox as Reply. Read chunks until we
	// see "exit" (success path) or "error" (broker / agent reject).
	inbox := nc.NewInbox()
	sub, err := nc.SubscribeSync(inbox)
	if err != nil {
		return err
	}
	defer func() { _ = sub.Unsubscribe() }()

	// origin: hot-update S6 plan §8.2 / T30 / T36
	admitCtx, cancelAdmission, err := ctlAdmissionContext(ctx, admission.deadline)
	if err != nil {
		return err
	}
	defer cancelAdmission()
	subject := proto.SubjCmdBy(sid, actor, nid, "exec")
	if err := nc.PublishRequest(subject, inbox, body); err != nil {
		if errors.Is(err, nats.ErrMaxPayload) {
			// Deterministic: the same argv/--stdin is the same size next time.
			return &ExitError{Class: exitUsage, Err: fmt.Errorf("exec: publish: %w: the command line plus --stdin does not fit in one request", err)}
		}
		return fmt.Errorf("exec: publish: %w", err)
	}

	out, errOut := cmd.OutOrStdout(), cmd.ErrOrStderr()

	receiveCtx := admitCtx
	for {
		msg, err := sub.NextMsgWithContext(receiveCtx)
		if err != nil {
			// origin: hot-update S6 internal review rv-ctl-9 / T36
			// A published legacy task has no stable receipt to query or replay.
			return legacyExecReceiveFailure(receiveCtx == admitCtx, timeout, err)
		}
		var chunk proto.ExecChunk
		if err := json.Unmarshal(msg.Data, &chunk); err != nil {
			return fmt.Errorf("exec: malformed chunk: %w", err)
		}
		switch chunk.Kind {
		case "started":
			// True start ends admission; output/settlement retain the user timeout.
			cancelAdmission()
			receiveCtx = ctx
		case "stdout":
			_, _ = out.Write(chunk.Data)
		case "stderr":
			_, _ = errOut.Write(chunk.Data)
		case "exit":
			// Propagate remote exit code as our own. cobra's RunE
			// returning nil keeps exit 0; for non-zero we use os.Exit
			// here so we don't print cobra's own error wrapper.
			//
			// Audit shard 04 F9: a negative ExitCode from the agent
			// means the child was signal-killed (Go's exec package
			// returns -1 for that). os.Exit(-1) wraps to 255 with
			// no signal information; print a stderr hint and exit
			// 128 (the closest analog to ssh's 128+sig — we don't
			// have the actual signal value yet, that's a wire
			// proto enhancement deferred to v2).
			admission.settled = true
			finishCtlAdmission(admission, &resultErr)
			if resultErr != nil {
				return resultErr
			}
			if code := chunk.ExitCode; code != 0 {
				cancelAdmission()
				if code < 0 {
					_, _ = fmt.Fprintln(errOut, "tether exec: remote process terminated by signal (no exit code)")
					code = 128
				}
				//nolint:gocritic // exitAfterDefer: the only deferred call, cancelAdmission, runs explicitly above.
				os.Exit(code)
			}
			_ = nc.Drain()
			return nil
		case "error":
			return execFailureMessage(chunk.Error)
		default:
			// Forward-compat: ignore unknown chunk kinds.
		}
	}
}

// legacyExecReceiveFailure preserves the unsafe sent boundary even before the
// legacy "started" chunk. It distinguishes admission from the user's task wait.
// origin: hot-update S6 internal review rv-ctl-9 / plan §8.2 / T36
func legacyExecReceiveFailure(admitting bool, timeout time.Duration, err error) error {
	detail := "receive failed"
	switch {
	case admitting && errors.Is(err, context.DeadlineExceeded):
		detail = "admission expired after 5s"
	case errors.Is(err, context.DeadlineExceeded):
		detail = fmt.Sprintf("gave up waiting after --timeout %s", timeout)
	case errors.Is(err, context.Canceled):
		detail = "interrupted"
	}
	return &ExitError{Class: exitUnsettled, Code: proto.CodeAgentNoResponders, Err: fmt.Errorf("exec: control_result_unknown: %s; the sent task result is unknown and a rerun may execute it again: %w", detail, err)}
}

// execStdinLimit bounds --stdin. The create request must stay under the
// broker's 1 MiB body limit (which is also NATS' default max_payload and the
// registry's intent-hash limit) after base64 expansion (×4/3) plus the
// handle/argv envelope: 512 KiB of raw stdin is 699 KiB on the wire.
const execStdinLimit = 512 << 10

// readExecStdin reads the caller's stdin for --stdin, refusing more than the
// limit as a usage error before any connection is made.
func readExecStdin(r io.Reader) ([]byte, error) {
	data, err := io.ReadAll(io.LimitReader(r, execStdinLimit+1))
	if err != nil {
		return nil, fmt.Errorf("exec: read --stdin: %w", err)
	}
	if len(data) > execStdinLimit {
		return nil, &ExitError{Class: exitUsage, Err: fmt.Errorf("exec: --stdin exceeds %d bytes; pipe the data to the remote command instead", execStdinLimit)}
	}
	return data, nil
}

// exitWithRemoteStatus passes the remote process's own status through, as the
// legacy path always has: 128+sig for a signal death, 128 for an unknown
// status, otherwise the exit code itself. Not an ExitError: these values are
// the remote program's, outside this taxonomy.
func exitWithRemoteStatus(result execclient.Result) {
	if result.Signal != 0 {
		os.Exit(128 + result.Signal)
	}
	if result.ExitCode < 0 {
		os.Exit(128)
	}
	if result.ExitCode != 0 {
		os.Exit(result.ExitCode)
	}
}

// classifyReliableFailure turns a failed reliable exec into the exit class
// docs/usage.md §9.13.2 promises. The rule is by PHASE, not by error text:
//
//   - before the create request reached the transport the command never ran,
//     so a transport error is the retryable transient it always was (75/69)
//     and a broker refusal keeps its own class (77/64/…);
//   - once create was sent the task may exist and may have run — a lost create
//     reply followed by --timeout leaves no PID behind but a child on the
//     agent — so everything except a permission refusal is exitUnsettled (74,
//     terminal): a rerun would execute the command again;
//   - a task that SETTLED with an unclean result (a stream aborted after
//     delivering bytes, or an agent-recorded setup/wait fault) is 74 too,
//     unless the agent named a spawn-side failure and the child never ran
//     (remote_fs_spawn_timeout and friends keep their own retryable class:
//     retrying a command that never started is safe);
//   - a refusal that BY CONSTRUCTION means "no task was admitted for this
//     request" keeps its own class even after create was sent: the agent's
//     admission table refusing the create (process_capacity, only ever the
//     create's own reply, and only when no record exists for the handle) and
//     the broker refusing the body as too large. Presenting those as
//     "accepted, result unknown" would tell automation never to retry a
//     command that never started (S1 internal review C2).
//
// There is no per-task kill verb: an abandoned task runs to its own end, or
// dies with its session (`tether session rm`), or is stopped on the node.
// The guidance printed below names those and nothing else
// (TestExecFailureGuidanceNamesExistingCommands).
//
// origin: docs/reviews/hot-update-reliable-exec-plan.md §1.1 (S1 exit-code taxonomy).
func classifyReliableFailure(nid string, timeout time.Duration, result execclient.Result, err error) error {
	var settled *execclient.SettledError
	if errors.As(err, &settled) {
		r := settled.Result
		// Accepted PTY input retains delivery responsibility even when no
		// output or running phase was observed; an abort cannot mean no spawn.
		// origin: hot-update S4 plan §2.2 / T49
		if !r.Executed && r.StdoutAbort == "" && r.StderrAbort == "" && r.InputAbort == "" {
			// The child never ran: the agent's own spawn-side code decides.
			return execFailureMessage(r.TerminalError)
		}
		return &ExitError{Class: exitUnsettled, Err: fmt.Errorf("exec: %s; remote exit=%d signal=%d (task %s on %s)",
			settled.Error(), r.ExitCode, r.Signal, r.Handle.RequestID, nid)}
	}
	var pe *execclient.ProtocolError
	if errors.As(err, &pe) {
		// The class lookup key: the wire code, or for a peer that predates
		// ProcessResumeReply.Code the prose prefix of its message. Only a real
		// wire code is carried structurally on the ExitError (S1 review C7).
		lookup := pe.Code
		if lookup == "" {
			lookup = pe.Message
			if i := strings.IndexByte(lookup, ':'); i >= 0 {
				lookup = strings.TrimSpace(lookup[:i])
			}
		}
		class := brokerDetailExitClass(lookup, pe.Message)
		if !pe.AfterCreate || class == exitNoPerm || createNeverAdmitted[pe.Code] {
			// A refusal before acceptance keeps its own class; a revoked member
			// is refused as such even mid-stream — never disguised as 74; a
			// refusal that means "nothing was admitted" is retry-safe. The
			// class comes from the CODE — the message of a typed refusal is
			// prose ("process request: …") whose prefix keys nothing.
			msg := pe.Message
			if hint := brokerCodeHints[lookup]; hint != "" {
				msg = hint + " (" + msg + ")"
			}
			return &ExitError{Class: class, Code: pe.Code, Err: fmt.Errorf("exec: %s", msg)}
		}
		return &ExitError{Class: exitUnsettled, Code: pe.Code, Err: fmt.Errorf(
			"exec: %s — the remote task was accepted and its result is unknown (task %s on %s)", pe.Message, result.Handle.RequestID, nid)}
	}
	if !result.CreateSent {
		if errors.Is(err, context.DeadlineExceeded) || errors.Is(err, context.Canceled) {
			return &ExitError{Class: exitTransient, Err: fmt.Errorf("exec: %w before the command was accepted", err)}
		}
		if errors.Is(err, nats.ErrMaxPayload) {
			// The create body is over the server's max_payload: the same
			// argv/--stdin is the same size next time (S1 review C5).
			return &ExitError{Class: exitUsage, Err: fmt.Errorf("exec: %w: the command line plus --stdin does not fit in one request", err)}
		}
		return execFailureMessage(err.Error())
	}
	switch {
	// origin: hot-update S6 internal review rv-ctl-9
	case errors.Is(err, controlbudget.ErrExpired):
		return &ExitError{Class: exitUnsettled, Code: proto.CodeAgentNoResponders, Err: fmt.Errorf(
			"exec: control_result_unknown: admission expired after 5s; %s", execAbandonedGuidance(nid, result.Handle.RequestID))}
	case errors.Is(err, context.DeadlineExceeded):
		return &ExitError{Class: exitUnsettled, Err: fmt.Errorf(
			"exec: gave up waiting after --timeout %s; %s", timeout, execAbandonedGuidance(nid, result.Handle.RequestID))}
	case errors.Is(err, context.Canceled):
		return &ExitError{Class: exitUnsettled, Err: fmt.Errorf(
			"exec: interrupted (Ctrl-C/SIGTERM); %s", execAbandonedGuidance(nid, result.Handle.RequestID))}
	}
	return &ExitError{Class: exitUnsettled, Err: fmt.Errorf(
		"exec: %v — the remote task was accepted and its result is unknown (task %s on %s)", err, result.Handle.RequestID, nid)}
}

// createNeverAdmitted names the refusal codes that, arriving after the create
// was sent, still prove no task was admitted for this request, so the
// command is safe to rerun and keeps its table class instead of 74.
var createNeverAdmitted = map[string]bool{
	proto.CodeProcessCapacity: true, // Reserve refuses capacity only when no record exists for the handle
	proto.CodeProcessTooLarge: true, // the broker never forwarded the body
}

// execAbandonedGuidance is the one sentence every abandoned-task exit ends
// with. It names only things that exist: there is no per-task kill verb.
func execAbandonedGuidance(nid, requestID string) string {
	return fmt.Sprintf("the remote task keeps running on %s (task %s) and was NOT killed — a rerun re-executes it; "+
		"it ends on its own, with `tether session rm <sid>` (every task of the session, on every node), or by stopping the process on the node",
		nid, requestID)
}
