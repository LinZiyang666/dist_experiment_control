package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"time"

	"github.com/LinZiyang666/tether/internal/cli"
	"github.com/LinZiyang666/tether/internal/proto"
	"github.com/nats-io/nats.go"
	"github.com/spf13/cobra"
)

func newExecCmd() *cobra.Command {
	var (
		natsURL string
		home    string
		cwd     string
		timeout time.Duration
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

			id, err := cli.EnsureIdentity(home)
			if err != nil {
				return err
			}
			nc, err := cli.ConnectNATSWithNkey(natsURL, id, nats.Name(cli.CtlNameForSession(sid)))
			if err != nil {
				return connectError("exec", natsURL, err)
			}
			defer nc.Close()

			body, err := json.Marshal(proto.ExecReq{Argv: argv, Cwd: cwd})
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

			subject := proto.SubjCmdBy(sid, id.PublicKey, nid, "exec")
			if err := nc.PublishRequest(subject, inbox, body); err != nil {
				return fmt.Errorf("exec: publish: %w", err)
			}

			ctx, cancel := context.WithTimeout(cmd.Context(), timeout)
			defer cancel()

			out := cmd.OutOrStdout()
			errOut := cmd.ErrOrStderr()

			for {
				msg, err := sub.NextMsgWithContext(ctx)
				if err != nil {
					if errors.Is(err, context.DeadlineExceeded) {
						return fmt.Errorf("exec: timed out after %s waiting for chunk", timeout)
					}
					return fmt.Errorf("exec: receive: %w", err)
				}
				var chunk proto.ExecChunk
				if err := json.Unmarshal(msg.Data, &chunk); err != nil {
					return fmt.Errorf("exec: malformed chunk: %w", err)
				}
				switch chunk.Kind {
				case "started":
					// Optional. Some users want to know the assigned PID.
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
					if chunk.ExitCode < 0 {
						_, _ = fmt.Fprintln(errOut,
							"tether exec: remote process terminated by signal (no exit code)")
						os.Exit(128)
					}
					if chunk.ExitCode != 0 {
						os.Exit(chunk.ExitCode)
					}
					_ = nc.Drain()
					return nil
				case "error":
					return fmt.Errorf("exec: %s", chunk.Error)
				default:
					// Forward-compat: ignore unknown chunk kinds.
				}
			}
		},
	}
	cmd.Flags().StringVar(&natsURL, "nats-url", "nats://127.0.0.1:4222", "NATS server URL")
	cmd.Flags().StringVar(&home, "home", cli.DefaultHome(), "tether home dir")
	cmd.Flags().StringVar(&cwd, "cwd", "", "working directory on the agent (default: agent's)")
	cmd.Flags().DurationVar(&timeout, "timeout", 10*time.Minute, "max time to wait for output / exit")
	// Audit shard 04 F12: without this, `tether exec node1 ls -la`
	// fails with "unknown shorthand flag 'l'" because cobra parses
	// the remote command's flags as ours. SetInterspersed(false)
	// stops flag parsing at the first positional, treating the rest
	// as the remote argv even when it contains -flag-shaped tokens.
	cmd.Flags().SetInterspersed(false)
	return cmd
}
